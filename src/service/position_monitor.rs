//! Take-profit / stop-loss monitor.
//!
//! The live-only monitor polls open positions and sends guarded FOK exits.

use std::sync::Arc;

use anyhow::Result;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

use crate::config::TpSlConfig;
use crate::models::{OrderType, PlannedOrder, Side, VenueId};
use crate::service::execution_circuit_breaker::ExecutionCircuitBreaker;
use crate::service::midprice::MidpriceSource;
use crate::service::order_gateway::{OrderGateway, OrderReceipt, OrderSubmitError};
use crate::service::position_store::{OpenPosition, PositionStore};
use crate::utils;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitReason {
    TakeProfit,
    StopLoss,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExitOutcome {
    NoTrigger,
    Rejected(OrderSubmitError),
    Filled(OrderReceipt),
}

pub fn check_exit(pos: &OpenPosition, midprice: f64) -> Option<ExitReason> {
    let pnl = pos.pnl_pct(midprice);
    if pnl >= pos.take_profit_pct {
        Some(ExitReason::TakeProfit)
    } else if pnl <= -pos.stop_loss_pct {
        Some(ExitReason::StopLoss)
    } else {
        None
    }
}

/// Spawns the live monitor. Any uncertain or halted execution terminates the
/// task so that no further exit evaluation can submit an order.
pub fn spawn(
    cfg: TpSlConfig,
    positions: Arc<PositionStore>,
    gateway: Arc<dyn OrderGateway>,
    breaker: Arc<ExecutionCircuitBreaker>,
    midprice: Arc<dyn MidpriceSource>,
    price_buffer: f64,
) -> tokio::task::JoinHandle<Result<()>> {
    tokio::spawn(async move {
        if !cfg.enabled {
            info!("TP/SL monitor disabled in config — exiting task");
            return Ok(());
        }
        let mut ticker = interval(Duration::from_secs(cfg.poll_interval_secs.max(1)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            for pos in positions.snapshot() {
                if let Err(error) = monitor_once(
                    &pos,
                    &positions,
                    gateway.as_ref(),
                    breaker.as_ref(),
                    midprice.as_ref(),
                    price_buffer,
                )
                .await
                {
                    let order_error = error.downcast_ref::<OrderSubmitError>();
                    let fatal = order_error.is_some_and(|order_error| {
                        matches!(
                            order_error,
                            OrderSubmitError::Uncertain { .. } | OrderSubmitError::Halted { .. }
                        )
                    });
                    if fatal {
                        if let Some(instruction) =
                            order_error.and_then(OrderSubmitError::operator_instruction)
                        {
                            error!(
                                token = %pos.token_id,
                                instruction,
                                "TP/SL halt marker persistence failed; monitor stopping"
                            );
                        } else {
                            error!(token = %pos.token_id, "TP/SL execution halted; monitor stopping");
                        }
                        return Err(error);
                    }
                    warn!(token = %pos.token_id, "TP/SL tick failed before order submission");
                }
            }
        }
    })
}

async fn monitor_once(
    pos: &OpenPosition,
    positions: &PositionStore,
    gateway: &dyn OrderGateway,
    breaker: &ExecutionCircuitBreaker,
    midprice: &dyn MidpriceSource,
    price_buffer: f64,
) -> Result<ExitOutcome> {
    let mid = midprice.midprice(&pos.token_id).await?;
    let pnl = pos.pnl_pct(mid);
    debug!(
        token = %pos.token_id,
        mid,
        entry = pos.entry_price,
        pnl_pct = pnl,
        "tp/sl tick"
    );

    let Some(reason) = check_exit(pos, mid) else {
        return Ok(ExitOutcome::NoTrigger);
    };
    info!(
        token = %pos.token_id,
        slug = %pos.slug,
        ?reason,
        pnl_pct = pnl,
        midprice = mid,
        "TP/SL triggered — submitting FOK exit"
    );

    let planned = exit_plan(pos, mid, price_buffer);
    match breaker.submit_fok(gateway, &planned).await {
        Ok(receipt) => {
            positions.close(&pos.token_id);
            Ok(ExitOutcome::Filled(receipt))
        }
        Err(error @ (OrderSubmitError::Preflight { .. } | OrderSubmitError::Rejected { .. })) => {
            Ok(ExitOutcome::Rejected(error))
        }
        Err(error) => Err(anyhow::Error::new(error)),
    }
}

fn exit_plan(pos: &OpenPosition, midprice: f64, price_buffer: f64) -> PlannedOrder {
    let side = pos.side.flip();
    let limit_price = match side {
        Side::Sell => utils::clamp_price(midprice - price_buffer),
        Side::Buy => utils::clamp_price(midprice + price_buffer),
    };
    PlannedOrder {
        venue: VenueId::Polymarket,
        token_id: pos.token_id.clone(),
        neg_risk: pos.neg_risk,
        side,
        shares: pos.shares,
        limit_price,
        usd_notional: pos.shares * limit_price,
        order_type: OrderType::Fok,
        source_trade_hash: None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use chrono::Utc;
    use parking_lot::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;
    use crate::config::AppConfig;
    use crate::service::execution_circuit_breaker::ExecutionCircuitBreaker;
    use crate::service::execution_ledger::ExecutionLedger;
    use crate::service::market_cache::MarketCache;
    use crate::service::order_executor::test_support;
    use crate::service::order_gateway::{
        OrderErrorCode, OrderGateway, OrderReceipt, OrderSubmitError,
    };
    use crate::service::risk_guard::RiskGuard;

    fn pos(entry: f64, tp: f64, sl: f64, side: Side) -> OpenPosition {
        OpenPosition {
            token_id: "t".into(),
            slug: "s".into(),
            category: None,
            tags: vec![],
            neg_risk: false,
            side,
            entry_price: entry,
            shares: 100.0,
            usd_notional: 100.0 * entry,
            take_profit_pct: tp,
            stop_loss_pct: sl,
            opened_at: Utc::now(),
        }
    }

    #[test]
    fn take_profit_triggers() {
        let p = pos(0.50, 30.0, 20.0, Side::Buy);
        assert_eq!(check_exit(&p, 0.70), Some(ExitReason::TakeProfit));
    }

    #[test]
    fn stop_loss_triggers() {
        let p = pos(0.50, 30.0, 20.0, Side::Buy);
        assert_eq!(check_exit(&p, 0.39), Some(ExitReason::StopLoss));
    }

    #[test]
    fn flat_range_does_not_trigger() {
        let p = pos(0.50, 30.0, 20.0, Side::Buy);
        assert_eq!(check_exit(&p, 0.55), None);
    }

    #[test]
    fn sell_inverted_pnl_logic() {
        let p = pos(0.50, 30.0, 20.0, Side::Sell);
        assert_eq!(check_exit(&p, 0.30), Some(ExitReason::TakeProfit));
        assert_eq!(check_exit(&p, 0.65), Some(ExitReason::StopLoss));
    }

    #[test]
    fn exit_outcome_debug_never_contains_the_complete_order_id() {
        let order_id = "EXIT_OUTCOME_ORDER_ID_SENTINEL_1234567890";
        let outcome = ExitOutcome::Filled(OrderReceipt {
            order_id: order_id.to_owned(),
            filled_shares_micros: 100_000_000,
            filled_usd_micros: 70_000_000,
        });

        assert!(!format!("{outcome:?}").contains(order_id));
    }

    #[tokio::test]
    async fn matched_sell_exit_closes_position_with_fok_for_open_shares() {
        let positions = PositionStore::new();
        let position = pos(0.50, 30.0, 20.0, Side::Buy);
        positions.open(position.clone());
        let dir = tempfile::tempdir().unwrap();
        let breaker = test_breaker(dir.path().join("execution-halt.json"));
        let gateway = Arc::new(FakeGateway::returning(Ok(OrderReceipt {
            order_id: "order-public-fixture".into(),
            filled_shares_micros: 100_000_000,
            filled_usd_micros: 70_000_000,
        })));

        let outcome = monitor_once(
            &position,
            &positions,
            gateway.as_ref(),
            breaker.as_ref(),
            &FixedMidprice(0.70),
            0.005,
        )
        .await
        .unwrap();

        assert!(matches!(outcome, ExitOutcome::Filled(_)));
        assert!(positions.get("t").is_none());
        let planned = gateway.plans.lock().pop().unwrap();
        assert_eq!(planned.side, Side::Sell);
        assert_eq!(planned.order_type, OrderType::Fok);
        assert_eq!(planned.shares, position.shares);
    }

    #[tokio::test]
    async fn rejected_exit_keeps_position_and_breaker_open() {
        let positions = PositionStore::new();
        let position = pos(0.50, 30.0, 20.0, Side::Buy);
        positions.open(position.clone());
        let dir = tempfile::tempdir().unwrap();
        let breaker = test_breaker(dir.path().join("execution-halt.json"));
        let gateway = Arc::new(FakeGateway::returning(Err(OrderSubmitError::Rejected {
            http_status: Some(409),
            code: OrderErrorCode::HttpRejected,
        })));

        let outcome = monitor_once(
            &position,
            &positions,
            gateway.as_ref(),
            breaker.as_ref(),
            &FixedMidprice(0.70),
            0.005,
        )
        .await
        .unwrap();

        assert!(matches!(outcome, ExitOutcome::Rejected(_)));
        assert!(positions.get("t").is_some());
        assert!(!breaker.is_halted());
    }

    #[tokio::test]
    async fn uncertain_exit_keeps_position_persists_halt_and_blocks_later_entry() {
        let positions = PositionStore::new();
        let position = pos(0.50, 30.0, 20.0, Side::Buy);
        positions.open(position.clone());
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("execution-halt.json");
        let breaker = test_breaker(marker.clone());
        let gateway_fake = Arc::new(FakeGateway::returning(Err(OrderSubmitError::Uncertain {
            code: OrderErrorCode::PostTransport,
        })));
        let gateway: Arc<dyn OrderGateway> = gateway_fake.clone();

        let result = monitor_once(
            &position,
            &positions,
            gateway.as_ref(),
            breaker.as_ref(),
            &FixedMidprice(0.70),
            0.005,
        )
        .await;

        assert!(result.is_err());
        assert!(positions.get("t").is_some());
        assert!(breaker.is_halted());
        assert!(marker.is_file());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut cfg: AppConfig = serde_json::from_str(include_str!("../../config.json")).unwrap();
        cfg.site.gamma_api_base = format!("http://{}", listener.local_addr().unwrap());
        cfg.site.clob_api_base = "http://127.0.0.1:9".into();
        cfg.bot.enable_trading = true;
        cfg.bot.mock_trading = false;
        cfg.trading.execution_halt_path = marker;
        let markets = MarketCache::new(reqwest::Client::new(), cfg.site.gamma_api_base.clone());
        let server = tokio::spawn(serve_fixture_market(listener));
        let executor = test_support::new_with_live_components(
            cfg.clone(),
            RiskGuard::new(cfg.risk.clone()),
            markets,
            Arc::clone(&positions),
            Some(gateway.clone()),
            Some(breaker.clone()),
        );

        let error = executor.execute(&entry_trade()).await.unwrap_err();
        server.await.unwrap();
        assert!(matches!(
            error.downcast_ref::<OrderSubmitError>(),
            Some(OrderSubmitError::Halted { .. })
        ));
        assert!(positions.get("12345").is_none());
        assert_eq!(positions.len(), 1);
        assert_eq!(gateway_fake.calls(), 1);
    }

    fn entry_trade() -> crate::models::WhaleTrade {
        crate::models::WhaleTrade {
            venue: VenueId::Polymarket,
            maker: "fixture-maker".into(),
            side: Side::Buy,
            token_id: "12345".into(),
            shares: 200.0,
            price: 0.5,
            usd_notional: 100.0,
            tx_hash: Some("fixture-entry".into()),
            block_number: Some(1),
            observed_at: Utc::now(),
        }
    }

    async fn serve_fixture_market(listener: TcpListener) {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0u8; 1024];
        let _ = socket.read(&mut request).await.unwrap();
        let body = r#"[{"slug":"fixture-market","question":"Fixture market","closed":false,"clobTokenIds":"[\"12345\",\"67890\"]","category":"Politics","tags":["election"]}]"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    }

    struct FixedMidprice(f64);

    #[async_trait]
    impl MidpriceSource for FixedMidprice {
        async fn midprice(&self, _token_id: &str) -> Result<f64> {
            Ok(self.0)
        }
    }

    fn test_breaker(path: std::path::PathBuf) -> Arc<ExecutionCircuitBreaker> {
        let ledger = Arc::new(
            ExecutionLedger::open_live(path.parent().unwrap().join("execution-ledger.jsonl"))
                .unwrap(),
        );
        ExecutionCircuitBreaker::new_live(ledger, path).unwrap()
    }

    struct FakeGateway {
        result: Result<OrderReceipt, OrderSubmitError>,
        calls: AtomicUsize,
        plans: Mutex<Vec<PlannedOrder>>,
    }

    impl FakeGateway {
        fn returning(result: Result<OrderReceipt, OrderSubmitError>) -> Self {
            Self {
                result,
                calls: AtomicUsize::new(0),
                plans: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl OrderGateway for FakeGateway {
        async fn submit_fok(
            &self,
            planned: &PlannedOrder,
        ) -> Result<OrderReceipt, OrderSubmitError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.plans.lock().push(planned.clone());
            self.result.clone()
        }
    }
}
