//! Copy-trading bot — phase-2 offline/non-live implementation.
//!
//! Architecture (matches §2 of the technical brief):
//!
//! 1. **Ingestion**: Polygon WebSocket `eth_subscribe`/`logs` filtered to the
//!    configured CTF Exchange contracts, topic-0 = `OrderFilled`, and the
//!    watched whale address packed as topic-2 (maker). Server-side filtering
//!    means the bot is woken only when the whale actually transacts.
//! 2. **Parse**: decode raw logs into [`WhaleTrade`] via [`service::parse`].
//! 3. **Eligibility**: resolve market metadata via Gamma, then check against
//!    the operator's allow/block lists ([`service::eligibility`]).
//! 4. **Sizing**: apply the configured copy strategy ([`service::strategy`]).
//! 5. **Exposure caps**: per-category and per-tag open-USD limits enforced
//!    via [`service::position_store`].
//! 6. **Risk**: fast in-memory check, optional book/depth check
//!    ([`service::risk_guard`]).
//! 7. **Execute**: the official Rust V2 SDK builds, signs, L2-authenticates,
//!    and posts true FOK CTF orders through [`service::order_executor`] and
//!    [`service::clob_sdk_orders`].
//! 8. **TP/SL**: live-only background monitor polls midprice for every open
//!    position and submits a guarded FOK exit when P&L crosses the configured
//!    thresholds ([`service::position_monitor`]).
//!
//! Safety: `enable_trading=false` OR `mock_trading=true` keeps the executor
//! in strict paper mode — the full pipeline records plans without SDK
//! initialization, signing, any CLOB request (including midpoint), or TP/SL
//! midpoint polling.

use crate::config::AppConfig;
use crate::service::{
    execution_circuit_breaker::ExecutionCircuitBreaker,
    market_cache::MarketCache,
    midprice::{ClobMidpriceSource, MidpriceSource},
    onchain::{spawn_subscription, LogFilter, RawLog},
    order_executor::{ExecutionOutcome, OrderExecutor},
    order_gateway::{order_id_hint, OrderGateway, OrderReceipt, OrderSubmitError},
    parse::{decode_whale_trade, order_filled_topic},
    position_monitor,
    risk_guard::RiskGuard,
};
use anyhow::{anyhow, Result};
use reqwest::Client;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

const LOG_CHANNEL_CAPACITY: usize = 256;

pub async fn run(cfg: AppConfig) -> Result<()> {
    let whale = cfg
        .bot
        .wallets_to_track
        .first()
        .cloned()
        .ok_or_else(|| anyhow!("config.bot.wallets_to_track is empty"))?;

    info!(
        whale = %whale,
        enable_trading = cfg.bot.enable_trading,
        mock_trading = cfg.bot.mock_trading,
        strict_allowlist = cfg.filters.is_strict(),
        tp_sl_enabled = cfg.tp_sl.enabled,
        "starting copy-trading bot"
    );

    let http = Client::builder()
        .user_agent("polymarket-toolkits/0.1")
        .build()?;
    let risk = RiskGuard::new(cfg.risk.clone());
    let markets = MarketCache::new(http.clone(), cfg.site.gamma_api_base.clone());
    let executor = OrderExecutor::new(cfg.clone(), Arc::clone(&risk), Arc::clone(&markets)).await?;
    let positions = executor.positions();

    let mut tp_sl_monitor = None;
    if let Some((gateway, breaker)) = live_tp_sl_components(&cfg, &executor) {
        let midprice: Arc<dyn MidpriceSource> = Arc::new(ClobMidpriceSource::new(
            http.clone(),
            cfg.site.clob_api_base.clone(),
        ));
        tp_sl_monitor = Some(position_monitor::spawn(
            cfg.tp_sl.clone(),
            Arc::clone(&positions),
            gateway,
            breaker,
            midprice,
            cfg.trading.price_buffer,
        ));
        info!(
            poll_interval_secs = cfg.tp_sl.poll_interval_secs,
            "TP/SL monitor spawned"
        );
    } else if cfg.tp_sl.enabled {
        info!("TP/SL CLOB monitoring inactive in strict dry-run");
    }

    let filter = build_filter(&cfg, &whale)?;
    let (tx, mut rx) = mpsc::channel::<RawLog>(LOG_CHANNEL_CAPACITY);
    let _sub = spawn_subscription(cfg.site.polygon_ws_url.clone(), filter, tx);

    let mut shutdown = Box::pin(tokio::signal::ctrl_c());

    loop {
        tokio::select! {
            biased;
            monitor_result = supervise_tp_sl_monitor(&mut tp_sl_monitor) => {
                error!("TP/SL monitor terminated; copy bot stopping");
                return monitor_result;
            }
            _ = &mut shutdown => {
                info!(open_positions = positions.len(), "shutdown signal received");
                return Ok(());
            }
            maybe_log = rx.recv() => {
                let Some(log) = maybe_log else {
                    warn!("on-chain subscription channel closed");
                    return Ok(());
                };
                if let Err(error) = handle_log(&executor, &whale, &log).await {
                    let fatal = error
                        .downcast_ref::<OrderSubmitError>()
                        .is_some_and(|order_error| matches!(
                            order_error,
                            OrderSubmitError::Uncertain { .. } | OrderSubmitError::Halted { .. }
                        ));
                    if fatal {
                        error!(tx = %log.tx_hash, "copy execution halted; bot stopping");
                        return Err(error);
                    }
                    error!(tx = %log.tx_hash, "handle_log failed before uncertain submission");
                }
            }
        }
    }
}

async fn supervise_tp_sl_monitor(
    monitor: &mut Option<tokio::task::JoinHandle<Result<()>>>,
) -> Result<()> {
    let Some(handle) = monitor.as_mut() else {
        return std::future::pending().await;
    };
    match handle.await {
        Ok(Ok(())) => Err(anyhow!("TP/SL monitor stopped unexpectedly")),
        Ok(Err(error)) => Err(error),
        Err(_) => Err(anyhow!("TP/SL monitor task terminated unexpectedly")),
    }
}

async fn handle_log(executor: &OrderExecutor, whale: &str, log: &RawLog) -> Result<()> {
    let Some(trade) = decode_whale_trade(log, whale)? else {
        return Ok(());
    };
    info!(
        token = %trade.token_id,
        side = ?trade.side,
        shares = trade.shares,
        usd = trade.usd_notional,
        tx = %log.tx_hash,
        "whale trade detected"
    );
    match executor.execute(&trade).await? {
        ExecutionOutcome::Skipped(r) => info!(?r, "execution skipped"),
        ExecutionOutcome::DryRunPlanned(planned) => info!(
            token = %planned.token_id,
            shares = planned.shares,
            usd = planned.usd_notional,
            price = planned.limit_price,
            "dry-run order planned (without a signature or submission)"
        ),
        ExecutionOutcome::NotSubmitted(error) => log_not_submitted(&error),
        ExecutionOutcome::Filled(receipt) => log_filled(&receipt),
    }
    Ok(())
}

fn build_filter(cfg: &AppConfig, whale: &str) -> Result<LogFilter> {
    let exchanges = vec![
        cfg.exchange.ctf_exchange_address.to_lowercase(),
        cfg.exchange.neg_risk_exchange_address.to_lowercase(),
    ];
    let topic0 = format!("0x{}", hex::encode(order_filled_topic().as_slice()));
    let whale_topic = pad_address_to_topic(whale)?;
    Ok(LogFilter {
        address: exchanges,
        topics: vec![Some(vec![topic0]), None, Some(vec![whale_topic])],
    })
}

fn pad_address_to_topic(addr: &str) -> Result<String> {
    let trimmed = addr.trim().trim_start_matches("0x").to_lowercase();
    if trimmed.len() != 40 {
        return Err(anyhow!("address must be 20 bytes / 40 hex chars"));
    }
    Ok(format!("0x{}{}", "0".repeat(24), trimmed))
}

fn live_tp_sl_components(
    cfg: &AppConfig,
    executor: &OrderExecutor,
) -> Option<(Arc<dyn OrderGateway>, Arc<ExecutionCircuitBreaker>)> {
    if !cfg.tp_sl.enabled || !cfg.live_trading_allowed() {
        return None;
    }
    executor.live_order_components()
}

fn log_not_submitted(error: &OrderSubmitError) {
    match error {
        OrderSubmitError::Preflight { code, .. } => {
            info!(code = ?code, "order not submitted during preflight");
        }
        OrderSubmitError::Rejected { http_status, code } => {
            info!(code = ?code, http_status = ?http_status, "order rejected by CLOB");
        }
        OrderSubmitError::Uncertain { .. } | OrderSubmitError::Halted { .. } => {}
    }
}

fn log_filled(receipt: &OrderReceipt) {
    info!(
        order_id_hint = %order_id_hint(&receipt.order_id),
        shares = receipt.filled_shares(),
        usd = receipt.filled_usd(),
        "order fully matched"
    );
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use chrono::Utc;

    use super::*;
    use crate::models::{OrderType, PlannedOrder, Side, VenueId};
    use crate::service::execution_ledger::{
        ExecutionLedger, IntentId, OrderId, OrderSide, PositionId, TokenId, Venue,
    };
    use crate::service::order_gateway::OrderErrorCode;
    use crate::service::position_store::{OpenPosition, PositionStore};

    #[tokio::test]
    async fn strict_dry_run_never_exposes_tp_sl_live_components() {
        let mut cfg: AppConfig = serde_json::from_str(include_str!("../../config.json")).unwrap();
        cfg.credentials.private_key.clear();
        cfg.credentials.funder_address.clear();
        cfg.credentials.signature_type = None;
        cfg.credentials.api_key = None;
        cfg.credentials.api_secret = None;
        cfg.credentials.api_passphrase = None;
        cfg.tp_sl.enabled = true;
        cfg.bot.enable_trading = false;
        cfg.bot.mock_trading = true;
        cfg.site.clob_api_base = "http://127.0.0.1:9".into();
        let risk = RiskGuard::new(cfg.risk.clone());
        let markets = MarketCache::new(Client::new(), cfg.site.gamma_api_base.clone());
        let executor = OrderExecutor::new(cfg.clone(), risk, markets)
            .await
            .unwrap();

        assert!(live_tp_sl_components(&cfg, &executor).is_none());
    }

    #[tokio::test]
    async fn marker_persist_failure_is_fatal_to_copy_supervisor_without_retry() {
        let mut cfg: AppConfig = serde_json::from_str(include_str!("../../config.json")).unwrap();
        cfg.tp_sl.enabled = true;
        cfg.tp_sl.poll_interval_secs = 1;
        let positions = PositionStore::new_paper();
        let position = OpenPosition {
            position_id: PositionId(uuid::Uuid::from_u128(700)),
            opening_intent_id: IntentId(uuid::Uuid::from_u128(700)),
            opening_order_id: OrderId::from_hex(format!("0x{}", "70".repeat(32))).unwrap(),
            venue: Venue::PolymarketClob,
            token_id: TokenId::from_decimal("700").unwrap(),
            slug: "fatal-exit-market".to_owned(),
            category: String::new(),
            tags: Vec::new(),
            neg_risk: false,
            side: OrderSide::Buy,
            shares_micros: 100_000_000,
            usd_notional_micros: 50_000_000,
            take_profit_bps: 3_000,
            stop_loss_bps: 2_000,
            opened_at: Utc::now(),
        };
        positions.apply_open(position.clone()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("DYNAMIC_FILESYSTEM_ERROR_SENTINEL");
        let ledger = Arc::new(
            ExecutionLedger::open_live(dir.path().join("execution-ledger.jsonl")).unwrap(),
        );
        let breaker = ExecutionCircuitBreaker::new_live(ledger, marker.clone()).unwrap();
        std::fs::create_dir(&marker).unwrap();
        let gateway = Arc::new(UncertainGateway::default());
        let gateway_trait: Arc<dyn OrderGateway> = gateway.clone();

        let mut monitor = Some(position_monitor::spawn(
            cfg.tp_sl,
            Arc::clone(&positions),
            gateway_trait,
            Arc::clone(&breaker),
            Arc::new(FixedMidprice(0.70)),
            0.005,
        ));
        let completion = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            supervise_tp_sl_monitor(&mut monitor),
        )
        .await
        .expect("fatal monitor termination must reach its supervisor");

        assert!(breaker.is_halted());
        assert!(positions.get_by_id(&position.position_id).is_some());
        assert_eq!(gateway.calls(), 1);
        let retry = breaker
            .submit_fok(
                gateway.as_ref(),
                &PlannedOrder {
                    venue: VenueId::Polymarket,
                    token_id: position.token_id.to_string(),
                    neg_risk: false,
                    side: Side::Sell,
                    shares: 100.0,
                    limit_price: 0.695,
                    usd_notional: 69.5,
                    order_type: OrderType::Fok,
                    source_trade_hash: None,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(retry, OrderSubmitError::Halted { .. }));
        assert_eq!(gateway.calls(), 1);

        let rendered = format!("{completion:?}").to_lowercase();
        assert!(rendered.contains("do not restart until manual reconciliation"));
        assert!(!rendered.contains("dynamic_filesystem_error_sentinel"));
    }

    #[test]
    fn redacts_short_order_ids_without_disclosing_them() {
        assert_eq!(order_id_hint("short"), "[redacted]");
    }

    #[test]
    fn redacts_normal_ascii_order_ids_with_prefix_and_suffix() {
        assert_eq!(order_id_hint("order-public-fixture"), "orde…ture");
    }

    #[test]
    fn redacts_multibyte_order_ids_on_character_boundaries() {
        assert_eq!(
            order_id_hint("订单编号交易确认成功回执"),
            "订单编号…成功回执"
        );
    }

    struct FixedMidprice(f64);

    #[async_trait]
    impl MidpriceSource for FixedMidprice {
        async fn midprice(&self, _token_id: &str) -> Result<f64> {
            Ok(self.0)
        }
    }

    #[derive(Default)]
    struct UncertainGateway {
        calls: AtomicUsize,
    }

    impl UncertainGateway {
        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl OrderGateway for UncertainGateway {
        async fn submit_fok(
            &self,
            _planned: &PlannedOrder,
        ) -> std::result::Result<OrderReceipt, OrderSubmitError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(OrderSubmitError::Uncertain {
                code: OrderErrorCode::PostTransport,
            })
        }
    }
}
