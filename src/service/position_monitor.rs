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
use crate::service::execution_ledger::{IntentId, IntentPurpose, OrderSide, PositionClose, Venue};
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
    InvalidMidpoint,
    Rejected(OrderSubmitError),
    Filled(OrderReceipt),
}

pub fn check_exit(pos: &OpenPosition, midprice: f64) -> Option<ExitReason> {
    let pnl = pos.pnl_pct(midprice);
    if pnl >= pos.take_profit_pct() {
        Some(ExitReason::TakeProfit)
    } else if pnl <= -pos.stop_loss_pct() {
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
    let token_id = pos.token_id.to_string();
    let mid = midprice.midprice(&token_id).await?;
    if !mid.is_finite() || mid <= 0.0 || mid >= 1.0 {
        return Ok(ExitOutcome::InvalidMidpoint);
    }
    let pnl = pos.pnl_pct(mid);
    debug!(
        token = %pos.token_id,
        mid,
        entry = pos.entry_price(),
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
    match breaker
        .submit_fok(
            gateway,
            &planned,
            IntentPurpose::Exit {
                position_id: pos.position_id,
            },
            |receipt| apply_filled_close(pos, positions, receipt).map_err(|_| ()),
        )
        .await
    {
        Ok(receipt) => Ok(ExitOutcome::Filled(receipt)),
        Err(error @ (OrderSubmitError::Preflight { .. } | OrderSubmitError::Rejected { .. })) => {
            Ok(ExitOutcome::Rejected(error))
        }
        Err(error) => Err(anyhow::Error::new(error)),
    }
}

fn apply_filled_close(
    pos: &OpenPosition,
    positions: &PositionStore,
    receipt: &OrderReceipt,
) -> Result<()> {
    if receipt.filled_shares_micros != pos.shares_micros || receipt.filled_usd_micros == 0 {
        return Err(anyhow::anyhow!(
            "confirmed exit receipt has conflicting amounts"
        ));
    }
    let order_id = receipt.order_id.clone();
    let closing_intent_id = if positions.is_paper() {
        IntentId(uuid::Uuid::new_v4())
    } else {
        positions
            .pending_exit_intent(&order_id, pos.position_id)
            .ok_or_else(|| anyhow::anyhow!("confirmed exit has no journaled intent"))?
    };
    positions.apply_close(PositionClose {
        position_id: pos.position_id,
        closing_intent_id,
        closing_order_id: order_id,
        shares_micros: receipt.filled_shares_micros,
        usd_micros: receipt.filled_usd_micros,
        closed_at: chrono::Utc::now(),
    })?;
    Ok(())
}

fn exit_plan(pos: &OpenPosition, midprice: f64, price_buffer: f64) -> PlannedOrder {
    let side = match pos.side {
        OrderSide::Buy => Side::Sell,
        OrderSide::Sell => Side::Buy,
    };
    let limit_price = match side {
        Side::Sell => utils::clamp_price(midprice - price_buffer),
        Side::Buy => utils::clamp_price(midprice + price_buffer),
    };
    PlannedOrder {
        venue: match pos.venue {
            Venue::PolymarketClob => VenueId::Polymarket,
        },
        token_id: pos.token_id.to_string(),
        neg_risk: pos.neg_risk,
        side,
        shares: pos.shares(),
        limit_price,
        usd_notional: pos.shares() * limit_price,
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
    use crate::service::execution_ledger::{
        ExecutionLedger, IntentId, IntentPurpose, LedgerPayload, MatchedAmounts, OrderId,
        OrderSide, OrderType as LedgerOrderType, PositionId, PositionSeed, PreparedIntent, TokenId,
        Venue, ORDER_PROTOCOL_VERSION,
    };
    use crate::service::market_cache::MarketCache;
    use crate::service::order_executor::test_support;
    use crate::service::order_gateway::{
        OrderErrorCode, OrderGateway, OrderReceipt, OrderSubmitError, PrePostJournal,
    };
    use crate::service::risk_guard::RiskGuard;

    fn pos(entry: f64, tp: f64, sl: f64, side: Side) -> OpenPosition {
        OpenPosition {
            position_id: PositionId(uuid::Uuid::from_u128(1)),
            opening_intent_id: IntentId(uuid::Uuid::from_u128(1)),
            opening_order_id: OrderId::from_hex(format!("0x{}", "11".repeat(32))).unwrap(),
            venue: Venue::PolymarketClob,
            token_id: TokenId::from_decimal("100").unwrap(),
            slug: "s".into(),
            category: String::new(),
            tags: vec![],
            neg_risk: false,
            side: match side {
                Side::Buy => OrderSide::Buy,
                Side::Sell => OrderSide::Sell,
            },
            shares_micros: 100_000_000,
            usd_notional_micros: (100.0 * entry * 1_000_000.0) as u128,
            take_profit_bps: (tp * 100.0) as u32,
            stop_loss_bps: (sl * 100.0) as u32,
            opened_at: Utc::now(),
        }
    }

    #[test]
    fn tp_sl_snapshot_after_reopen_uses_exact_durable_position() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let position = pos(0.50, 30.0, 20.0, Side::Buy);
        {
            let ledger = Arc::new(ExecutionLedger::open_live(&path).unwrap());
            let store = PositionStore::from_ledger(Arc::clone(&ledger)).unwrap();
            ledger
                .append(
                    position.opening_intent_id,
                    LedgerPayload::IntentPrepared(PreparedIntent {
                        order_id: position.opening_order_id.clone(),
                        protocol_version: ORDER_PROTOCOL_VERSION,
                        venue: position.venue,
                        token_id: position.token_id,
                        neg_risk: position.neg_risk,
                        side: position.side,
                        order_type: LedgerOrderType::Fok,
                        expected_maker_micros: position.usd_notional_micros,
                        expected_taker_micros: position.shares_micros,
                        source_hash: None,
                        purpose: IntentPurpose::Entry(PositionSeed {
                            slug: position.slug.clone(),
                            category: position.category.clone(),
                            tags: position.tags.clone(),
                            take_profit_bps: position.take_profit_bps,
                            stop_loss_bps: position.stop_loss_bps,
                        }),
                    }),
                )
                .unwrap();
            ledger
                .append(position.opening_intent_id, LedgerPayload::SubmitStarted)
                .unwrap();
            ledger
                .append(
                    position.opening_intent_id,
                    LedgerPayload::RemoteMatched(MatchedAmounts {
                        shares_micros: position.shares_micros,
                        usd_micros: position.usd_notional_micros,
                    }),
                )
                .unwrap();
            store.apply_open(position.clone()).unwrap();
            ledger
                .append(
                    position.opening_intent_id,
                    LedgerPayload::SubmissionCommitted,
                )
                .unwrap();
        }

        let ledger = Arc::new(ExecutionLedger::open_live(&path).unwrap());
        let reopened = PositionStore::from_ledger(ledger).unwrap();
        let actual = reopened.snapshot().pop().unwrap();

        assert_eq!(actual, position);
        assert_eq!(check_exit(&actual, 0.70), Some(ExitReason::TakeProfit));
        assert_eq!(exit_plan(&actual, 0.70, 0.005).shares, 100.0);
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
        let order_id = OrderId::from_hex(format!("0x{}", "30".repeat(32))).unwrap();
        let outcome = ExitOutcome::Filled(OrderReceipt {
            order_id: order_id.clone(),
            filled_shares_micros: 100_000_000,
            filled_usd_micros: 70_000_000,
        });

        assert!(!format!("{outcome:?}").contains(order_id.as_str()));
    }

    #[tokio::test]
    async fn matched_sell_exit_closes_position_with_fok_for_open_shares() {
        let positions = PositionStore::new_paper();
        let position = pos(0.50, 30.0, 20.0, Side::Buy);
        positions.apply_open(position.clone()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let breaker = test_breaker(dir.path().join("execution-halt.json"));
        let gateway = Arc::new(FakeGateway::returning(Ok(OrderReceipt {
            order_id: OrderId::from_hex(format!("0x{}", "31".repeat(32))).unwrap(),
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
        assert!(positions.get_by_token(&position.token_id).is_none());
        let planned = gateway.plans.lock().pop().unwrap();
        assert_eq!(planned.side, Side::Sell);
        assert_eq!(planned.order_type, OrderType::Fok);
        assert_eq!(planned.shares, position.shares());
    }

    #[tokio::test]
    async fn accepted_exit_fill_position_apply_failure_stops_monitor_before_second_gateway_call() {
        let positions = PositionStore::new_paper();
        let position = pos(0.50, 30.0, 20.0, Side::Buy);
        positions.apply_open(position.clone()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("execution-halt.json");
        let breaker = test_breaker(marker.clone());
        let order_id = OrderId::from_hex(format!("0x{}", "32".repeat(32))).unwrap();
        let gateway = Arc::new(ApplyCloseBeforeReceiptGateway {
            positions: Arc::clone(&positions),
            close: PositionClose {
                position_id: position.position_id,
                closing_intent_id: IntentId(uuid::Uuid::from_u128(32)),
                closing_order_id: order_id.clone(),
                shares_micros: position.shares_micros,
                usd_micros: 70_000_000,
                closed_at: Utc::now() - chrono::Duration::days(1),
            },
            receipt: OrderReceipt {
                order_id: order_id.clone(),
                filled_shares_micros: position.shares_micros,
                filled_usd_micros: 70_000_000,
            },
            calls: AtomicUsize::new(0),
            plans: Mutex::new(Vec::new()),
        });
        let gateway_trait: Arc<dyn OrderGateway> = gateway.clone();
        let mut cfg: AppConfig = serde_json::from_str(include_str!("../../config.json")).unwrap();
        cfg.tp_sl.enabled = true;
        cfg.tp_sl.poll_interval_secs = 1;

        let mut handle = spawn(
            cfg.tp_sl,
            Arc::clone(&positions),
            gateway_trait,
            Arc::clone(&breaker),
            Arc::new(FixedMidprice(0.70)),
            0.005,
        );
        let completion =
            tokio::time::timeout(std::time::Duration::from_millis(500), &mut handle).await;
        if completion.is_err() {
            handle.abort();
        }
        let error = completion
            .expect("post-fill position failure must stop the monitor")
            .expect("monitor task must join")
            .unwrap_err();

        assert!(matches!(
            error.downcast_ref::<OrderSubmitError>(),
            Some(OrderSubmitError::Halted {
                code: OrderErrorCode::ExecutionHalted
            })
        ));
        assert!(breaker.is_halted());
        assert!(marker.is_file());
        assert_eq!(gateway.calls(), 1);

        let planned = gateway.plans.lock()[0].clone();
        assert!(matches!(
            breaker
                .submit_fok(
                    gateway.as_ref(),
                    &planned,
                    IntentPurpose::Exit {
                        position_id: position.position_id,
                    },
                    |_receipt| Ok(()),
                )
                .await,
            Err(OrderSubmitError::Halted { .. })
        ));
        assert_eq!(gateway.calls(), 1);
    }

    #[tokio::test]
    async fn rejected_exit_keeps_position_and_breaker_open() {
        let positions = PositionStore::new_paper();
        let position = pos(0.50, 30.0, 20.0, Side::Buy);
        positions.apply_open(position.clone()).unwrap();
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
        assert!(positions.get_by_token(&position.token_id).is_some());
        assert!(!breaker.is_halted());
    }

    #[tokio::test]
    async fn invalid_midpoints_are_typed_no_submit_outcomes() {
        let positions = PositionStore::new_paper();
        let position = pos(0.50, 30.0, 20.0, Side::Buy);
        positions.apply_open(position.clone()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let breaker = test_breaker(dir.path().join("execution-halt.json"));
        let gateway = Arc::new(FakeGateway::returning(Ok(OrderReceipt {
            order_id: OrderId::from_hex(format!("0x{}", "33".repeat(32))).unwrap(),
            filled_shares_micros: position.shares_micros,
            filled_usd_micros: 70_000_000,
        })));

        for invalid in [
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            -0.01,
            0.0,
            1.0,
            1.01,
        ] {
            let outcome = monitor_once(
                &position,
                &positions,
                gateway.as_ref(),
                breaker.as_ref(),
                &FixedMidprice(invalid),
                0.005,
            )
            .await
            .unwrap();

            assert!(matches!(outcome, ExitOutcome::InvalidMidpoint));
        }

        assert_eq!(gateway.calls(), 0);
        assert!(positions.get_by_id(&position.position_id).is_some());
        assert!(!breaker.is_halted());
    }

    #[tokio::test]
    async fn uncertain_exit_keeps_position_persists_halt_and_blocks_later_entry() {
        let positions = PositionStore::new_paper();
        let position = pos(0.50, 30.0, 20.0, Side::Buy);
        positions.apply_open(position.clone()).unwrap();
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
        assert!(positions.get_by_token(&position.token_id).is_some());
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
        assert!(positions
            .get_by_token(&TokenId::from_decimal("12345").unwrap())
            .is_none());
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
            _journal: &dyn PrePostJournal,
        ) -> Result<OrderReceipt, OrderSubmitError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.plans.lock().push(planned.clone());
            self.result.clone()
        }
    }

    struct ApplyCloseBeforeReceiptGateway {
        positions: Arc<PositionStore>,
        close: PositionClose,
        receipt: OrderReceipt,
        calls: AtomicUsize,
        plans: Mutex<Vec<PlannedOrder>>,
    }

    impl ApplyCloseBeforeReceiptGateway {
        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl OrderGateway for ApplyCloseBeforeReceiptGateway {
        async fn submit_fok(
            &self,
            planned: &PlannedOrder,
            _journal: &dyn PrePostJournal,
        ) -> Result<OrderReceipt, OrderSubmitError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.plans.lock().push(planned.clone());
            self.positions.apply_close(self.close.clone()).unwrap();
            Ok(self.receipt.clone())
        }
    }
}
