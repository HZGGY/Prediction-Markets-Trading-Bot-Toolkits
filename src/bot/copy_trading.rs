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
    market_cache::MarketCache,
    midprice::{ClobMidpriceSource, MidpriceSource},
    onchain::{spawn_subscription, LogFilter, RawLog},
    order_executor::{ExecutionOutcome, LiveExecutionRuntime, OrderExecutor},
    order_gateway::{order_id_hint, OrderReceipt, OrderSubmitError},
    parse::{decode_whale_trade, order_filled_topic},
    position_monitor,
    risk_guard::RiskGuard,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use reqwest::Client;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

const LOG_CHANNEL_CAPACITY: usize = 256;

pub async fn run(cfg: AppConfig) -> Result<()> {
    run_with_factory(cfg, &ProductionRuntimeFactory).await
}

async fn run_with_factory(cfg: AppConfig, factory: &dyn CopyRuntimeFactory) -> Result<()> {
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

    let risk = RiskGuard::new(cfg.risk.clone());
    let mut runtime = initialize_runtime(&cfg, &whale, risk, factory).await?;

    let mut shutdown = Box::pin(tokio::signal::ctrl_c());

    loop {
        tokio::select! {
            biased;
            monitor_result = supervise_tp_sl_monitor(&mut runtime.tp_sl_monitor) => {
                error!("TP/SL monitor terminated; copy bot stopping");
                return monitor_result;
            }
            _ = &mut shutdown => {
                info!(open_positions = runtime.positions.len(), "shutdown signal received");
                return Ok(());
            }
            maybe_log = runtime.rx.recv() => {
                let Some(log) = maybe_log else {
                    warn!("on-chain subscription channel closed");
                    return Ok(());
                };
                if let Err(error) = handle_log(&runtime.executor, &whale, &log).await {
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

enum RuntimeHttp {
    Production(Client),
    #[cfg(test)]
    Inert,
}

#[async_trait]
trait CopyRuntimeFactory: Send + Sync {
    async fn executor(&self, cfg: AppConfig, risk: Arc<RiskGuard>) -> Result<OrderExecutor>;
    fn http(&self) -> Result<RuntimeHttp>;
    fn attach_gamma(
        &self,
        executor: OrderExecutor,
        http: &RuntimeHttp,
        cfg: &AppConfig,
    ) -> OrderExecutor;
    fn midpoint(&self, http: &RuntimeHttp, cfg: &AppConfig) -> Arc<dyn MidpriceSource>;
    fn polygon(
        &self,
        cfg: &AppConfig,
        filter: LogFilter,
        tx: mpsc::Sender<RawLog>,
    ) -> Box<dyn Send>;
}

struct ProductionRuntimeFactory;

#[async_trait]
impl CopyRuntimeFactory for ProductionRuntimeFactory {
    async fn executor(&self, cfg: AppConfig, risk: Arc<RiskGuard>) -> Result<OrderExecutor> {
        OrderExecutor::new(cfg, risk).await
    }

    fn http(&self) -> Result<RuntimeHttp> {
        Ok(RuntimeHttp::Production(
            Client::builder()
                .user_agent("polymarket-toolkits/0.1")
                .build()?,
        ))
    }

    fn attach_gamma(
        &self,
        executor: OrderExecutor,
        http: &RuntimeHttp,
        cfg: &AppConfig,
    ) -> OrderExecutor {
        #[cfg(not(test))]
        let RuntimeHttp::Production(http) = http;
        #[cfg(test)]
        let http = match http {
            RuntimeHttp::Production(http) => http,
            RuntimeHttp::Inert => panic!("production factory received inert HTTP"),
        };
        executor.with_markets(MarketCache::new(
            http.clone(),
            cfg.site.gamma_api_base.clone(),
        ))
    }

    fn midpoint(&self, http: &RuntimeHttp, cfg: &AppConfig) -> Arc<dyn MidpriceSource> {
        #[cfg(not(test))]
        let RuntimeHttp::Production(http) = http;
        #[cfg(test)]
        let http = match http {
            RuntimeHttp::Production(http) => http,
            RuntimeHttp::Inert => panic!("production factory received inert HTTP"),
        };
        Arc::new(ClobMidpriceSource::new(
            http.clone(),
            cfg.site.clob_api_base.clone(),
        ))
    }

    fn polygon(
        &self,
        cfg: &AppConfig,
        filter: LogFilter,
        tx: mpsc::Sender<RawLog>,
    ) -> Box<dyn Send> {
        Box::new(spawn_subscription(
            cfg.site.polygon_ws_url.clone(),
            filter,
            tx,
        ))
    }
}

struct InitializedRuntime {
    executor: OrderExecutor,
    positions: Arc<crate::service::position_store::PositionStore>,
    tp_sl_monitor: Option<tokio::task::JoinHandle<Result<()>>>,
    rx: mpsc::Receiver<RawLog>,
    _subscription: Box<dyn Send>,
}

async fn initialize_runtime(
    cfg: &AppConfig,
    whale: &str,
    risk: Arc<RiskGuard>,
    factory: &dyn CopyRuntimeFactory,
) -> Result<InitializedRuntime> {
    let executor = factory.executor(cfg.clone(), risk).await?;
    let http = factory.http()?;
    let executor = factory.attach_gamma(executor, &http, cfg);
    let positions = executor.positions();

    let mut tp_sl_monitor = None;
    if let Some(runtime) = live_tp_sl_components(cfg, &executor) {
        tp_sl_monitor = Some(position_monitor::spawn(
            cfg.tp_sl.clone(),
            Arc::clone(&runtime.positions),
            Arc::clone(&runtime.gateway),
            Arc::clone(&runtime.breaker),
            factory.midpoint(&http, cfg),
            cfg.trading.price_buffer,
        ));
        info!(
            poll_interval_secs = cfg.tp_sl.poll_interval_secs,
            "TP/SL monitor spawned"
        );
    } else if cfg.tp_sl.enabled {
        info!("TP/SL CLOB monitoring inactive in strict dry-run");
    }

    let filter = build_filter(cfg, whale)?;
    let (tx, rx) = mpsc::channel::<RawLog>(LOG_CHANNEL_CAPACITY);
    let subscription = factory.polygon(cfg, filter, tx);
    Ok(InitializedRuntime {
        executor,
        positions,
        tp_sl_monitor,
        rx,
        _subscription: subscription,
    })
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
) -> Option<Arc<LiveExecutionRuntime>> {
    if !cfg.tp_sl.enabled || !cfg.live_trading_allowed() {
        return None;
    }
    executor.live_runtime()
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
        order_id_hint = %order_id_hint(receipt.order_id.as_str()),
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
        AcknowledgeReason, ExecutionLedger, IntentId, IntentPurpose, LedgerPayload, OrderId,
        OrderSide, OrderType as LedgerOrderType, PositionId, PositionSeed, PreparedIntent, TokenId,
        Venue, ORDER_PROTOCOL_VERSION,
    };
    use crate::service::order_executor::LiveGatewayFactory;
    use crate::service::position_store::{OpenPosition, PositionStore};
    use crate::service::{
        execution_circuit_breaker::ExecutionCircuitBreaker,
        order_gateway::{OrderErrorCode, OrderGateway, PrePostJournal},
    };

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
        let factory = InertRuntimeFactory::default();
        let mut runtime = initialize_runtime(
            &cfg,
            "0x1111111111111111111111111111111111111111",
            RiskGuard::new(cfg.risk.clone()),
            &factory,
        )
        .await
        .unwrap();

        assert!(live_tp_sl_components(&cfg, &runtime.executor).is_none());
        assert_eq!(factory.counts(), ComponentCounts::new(0, 0, 1, 1, 1));
        runtime.tp_sl_monitor.take();
    }

    #[tokio::test]
    async fn live_tp_sl_receives_the_executor_shared_runtime_components() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg: AppConfig = serde_json::from_str(include_str!("../../config.json")).unwrap();
        cfg.bot.enable_trading = true;
        cfg.bot.mock_trading = false;
        cfg.tp_sl.enabled = true;
        cfg.trading.execution_ledger_path = dir.path().join("execution-ledger.jsonl");
        cfg.trading.execution_halt_path = dir.path().join("execution-halt.json");
        cfg.credentials.private_key =
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".to_owned();
        cfg.credentials.funder_address = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266".to_owned();
        cfg.credentials.signature_type = Some(0);
        cfg.credentials.api_key = Some("00000000-0000-0000-0000-000000000000".to_owned());
        cfg.credentials.api_secret =
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_owned());
        cfg.credentials.api_passphrase = Some("fixture-passphrase".to_owned());

        let factory = InertRuntimeFactory::default();
        let mut initialized = initialize_runtime(
            &cfg,
            "0x1111111111111111111111111111111111111111",
            RiskGuard::new(cfg.risk.clone()),
            &factory,
        )
        .await
        .unwrap();
        let runtime = live_tp_sl_components(&cfg, &initialized.executor)
            .expect("enabled live TP/SL must use the shared live runtime");
        let (gateway, breaker) = initialized
            .executor
            .live_order_components()
            .expect("live executor must expose its shared components internally");

        assert!(Arc::ptr_eq(
            &initialized.executor.positions(),
            &runtime.positions
        ));
        assert!(Arc::ptr_eq(&gateway, &runtime.gateway));
        assert!(Arc::ptr_eq(&breaker, &runtime.breaker));
        assert!(Arc::ptr_eq(
            &runtime.ledger,
            &runtime.positions.live_ledger().unwrap()
        ));
        assert!(Arc::ptr_eq(&runtime.ledger, &runtime.breaker.ledger()));
        assert_eq!(runtime.ledger.projection().sequence, 0);
        assert_eq!(factory.counts(), ComponentCounts::new(1, 1, 1, 1, 1));
        initialized
            .tp_sl_monitor
            .take()
            .expect("live TP/SL must have a monitor")
            .abort();
    }

    #[tokio::test]
    async fn failed_live_preflight_constructs_no_network_capable_components() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = live_cfg(dir.path());
        cfg.credentials.api_secret = Some("not URL-safe base64".to_owned());

        assert_failed_initialization_has_zero_component_construction(
            cfg,
            "order preflight failed at Initialization (MissingCredentials)",
        )
        .await;
    }

    #[tokio::test]
    async fn failed_ledger_snapshot_and_marker_startup_construct_no_network_capable_components() {
        let ledger_dir = tempfile::tempdir().unwrap();
        let ledger_cfg = live_cfg(ledger_dir.path());
        std::fs::write(&ledger_cfg.trading.execution_ledger_path, b"{\n").unwrap();
        assert_failed_initialization_has_zero_component_construction(
            ledger_cfg,
            "live startup blocked (code=ledger_corrupt): preserve and inspect the ledger; do not edit it or retry startup",
        )
        .await;

        let snapshot_dir = tempfile::tempdir().unwrap();
        let snapshot_cfg = live_cfg(snapshot_dir.path());
        std::fs::write(&snapshot_cfg.trading.execution_ledger_path, b"").unwrap();
        std::fs::write(
            snapshot_cfg
                .trading
                .execution_ledger_path
                .with_extension("jsonl.active.json"),
            br#"{"schema_version":1,"sequence":1,"head_hash":"0000000000000000000000000000000000000000000000000000000000000000","active_intent":null}"#,
        )
        .unwrap();
        assert_failed_initialization_has_zero_component_construction(
            snapshot_cfg,
            "live startup blocked (code=snapshot_inconsistent): inspect the snapshot and ledger; do not overwrite either file",
        )
        .await;

        let marker_dir = tempfile::tempdir().unwrap();
        let marker_cfg = live_cfg(marker_dir.path());
        drop(ExecutionLedger::open_live(&marker_cfg.trading.execution_ledger_path).unwrap());
        std::fs::write(&marker_cfg.trading.execution_halt_path, b"fixture marker").unwrap();
        assert_failed_initialization_has_zero_component_construction(
            marker_cfg,
            "live startup blocked (code=orphan_marker): preserve and inspect the orphan or incompatible halt marker; do not delete it",
        )
        .await;
    }

    #[tokio::test]
    async fn active_and_cleanup_startup_states_have_exact_zero_component_diagnostics() {
        let active_dir = tempfile::tempdir().unwrap();
        let active_cfg = live_cfg(active_dir.path());
        {
            let ledger =
                ExecutionLedger::open_live(&active_cfg.trading.execution_ledger_path).unwrap();
            append_prepared_entry(&ledger, IntentId(uuid::Uuid::from_u128(801)));
        }
        assert_failed_initialization_has_zero_component_construction(
            active_cfg,
            "live startup blocked (code=active_unresolved): manual recovery and reconciliation are required before restart",
        )
        .await;

        let cleanup_dir = tempfile::tempdir().unwrap();
        let cleanup_cfg = live_cfg(cleanup_dir.path());
        let cleanup_intent = IntentId(uuid::Uuid::from_u128(802));
        {
            let ledger =
                ExecutionLedger::open_live(&cleanup_cfg.trading.execution_ledger_path).unwrap();
            append_prepared_entry(&ledger, cleanup_intent);
            ledger
                .append(
                    cleanup_intent,
                    LedgerPayload::Acknowledged {
                        reason: AcknowledgeReason::NotSent,
                    },
                )
                .unwrap();
        }
        std::fs::write(
            &cleanup_cfg.trading.execution_halt_path,
            b"fixture cleanup marker",
        )
        .unwrap();
        assert_failed_initialization_has_zero_component_construction(
            cleanup_cfg,
            "live startup blocked (code=cleanup_pending): resume the bounded halt-marker cleanup acknowledgement; do not re-reconcile or delete the marker directly",
        )
        .await;
    }

    #[tokio::test]
    async fn unsupported_snapshot_schema_has_exact_zero_component_diagnostic() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = live_cfg(dir.path());
        std::fs::write(&cfg.trading.execution_ledger_path, b"").unwrap();
        std::fs::write(
            cfg.trading.execution_ledger_path.with_extension("jsonl.active.json"),
            br#"{"schema_version":2,"sequence":0,"head_hash":"0000000000000000000000000000000000000000000000000000000000000000","active_intent":null}"#,
        )
        .unwrap();

        assert_failed_initialization_has_zero_component_construction(
            cfg,
            "live startup blocked (code=snapshot_inconsistent): inspect the snapshot and ledger; do not overwrite either file",
        )
        .await;
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
                IntentPurpose::Exit {
                    position_id: position.position_id,
                },
                |_receipt| Ok(()),
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

    #[derive(Debug, Eq, PartialEq)]
    struct ComponentCounts {
        gateway: usize,
        midpoint: usize,
        http: usize,
        gamma: usize,
        polygon: usize,
    }

    impl ComponentCounts {
        const fn new(
            gateway: usize,
            midpoint: usize,
            http: usize,
            gamma: usize,
            polygon: usize,
        ) -> Self {
            Self {
                gateway,
                midpoint,
                http,
                gamma,
                polygon,
            }
        }
    }

    #[derive(Default)]
    struct InertRuntimeFactory {
        gateway: AtomicUsize,
        midpoint: AtomicUsize,
        http: AtomicUsize,
        gamma: AtomicUsize,
        polygon: AtomicUsize,
    }

    impl InertRuntimeFactory {
        fn counts(&self) -> ComponentCounts {
            ComponentCounts::new(
                self.gateway.load(Ordering::SeqCst),
                self.midpoint.load(Ordering::SeqCst),
                self.http.load(Ordering::SeqCst),
                self.gamma.load(Ordering::SeqCst),
                self.polygon.load(Ordering::SeqCst),
            )
        }
    }

    async fn assert_failed_initialization_has_zero_component_construction(
        cfg: AppConfig,
        expected: &str,
    ) {
        let factory = InertRuntimeFactory::default();
        let result = initialize_runtime(
            &cfg,
            "0x1111111111111111111111111111111111111111",
            RiskGuard::new(cfg.risk.clone()),
            &factory,
        )
        .await;

        let error = match result {
            Ok(_) => panic!("fixture must block live initialization"),
            Err(error) => error,
        };
        assert_eq!(format!("{error}"), expected);
        assert_eq!(factory.counts(), ComponentCounts::new(0, 0, 0, 0, 0));
    }

    fn append_prepared_entry(ledger: &ExecutionLedger, intent_id: IntentId) {
        ledger
            .append(
                intent_id,
                LedgerPayload::IntentPrepared(PreparedIntent {
                    order_id: OrderId::from_hex(format!("0x{}", "80".repeat(32))).unwrap(),
                    protocol_version: ORDER_PROTOCOL_VERSION,
                    venue: Venue::PolymarketClob,
                    token_id: TokenId::from_decimal("12345").unwrap(),
                    neg_risk: false,
                    side: OrderSide::Buy,
                    order_type: LedgerOrderType::Fok,
                    expected_maker_micros: 19_500_000,
                    expected_taker_micros: 39_000_000,
                    source_hash: None,
                    purpose: IntentPurpose::Entry(PositionSeed {
                        slug: "fixture-market".into(),
                        category: "Politics".into(),
                        tags: vec!["election".into()],
                        take_profit_bps: 4_000,
                        stop_loss_bps: 2_500,
                    }),
                }),
            )
            .unwrap();
    }

    fn live_cfg(dir: &std::path::Path) -> AppConfig {
        let mut cfg: AppConfig = serde_json::from_str(include_str!("../../config.json")).unwrap();
        cfg.bot.enable_trading = true;
        cfg.bot.mock_trading = false;
        cfg.tp_sl.enabled = true;
        cfg.trading.execution_ledger_path = dir.join("execution-ledger.jsonl");
        cfg.trading.execution_halt_path = dir.join("execution-halt.json");
        cfg.credentials.private_key =
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".to_owned();
        cfg.credentials.funder_address = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266".to_owned();
        cfg.credentials.signature_type = Some(0);
        cfg.credentials.api_key = Some("00000000-0000-0000-0000-000000000000".to_owned());
        cfg.credentials.api_secret =
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_owned());
        cfg.credentials.api_passphrase = Some("fixture-passphrase".to_owned());
        cfg
    }

    #[async_trait]
    impl LiveGatewayFactory for InertRuntimeFactory {
        async fn build(
            &self,
            _cfg: &AppConfig,
        ) -> std::result::Result<Arc<dyn OrderGateway>, OrderSubmitError> {
            self.gateway.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(InertGateway))
        }
    }

    #[async_trait]
    impl CopyRuntimeFactory for InertRuntimeFactory {
        async fn executor(&self, cfg: AppConfig, risk: Arc<RiskGuard>) -> Result<OrderExecutor> {
            OrderExecutor::new_with_test_gateway_factory(cfg, risk, self).await
        }

        fn http(&self) -> Result<RuntimeHttp> {
            self.http.fetch_add(1, Ordering::SeqCst);
            Ok(RuntimeHttp::Inert)
        }

        fn attach_gamma(
            &self,
            executor: OrderExecutor,
            _http: &RuntimeHttp,
            _cfg: &AppConfig,
        ) -> OrderExecutor {
            self.gamma.fetch_add(1, Ordering::SeqCst);
            executor
        }

        fn midpoint(&self, _http: &RuntimeHttp, _cfg: &AppConfig) -> Arc<dyn MidpriceSource> {
            self.midpoint.fetch_add(1, Ordering::SeqCst);
            Arc::new(FixedMidprice(0.50))
        }

        fn polygon(
            &self,
            _cfg: &AppConfig,
            _filter: LogFilter,
            _tx: mpsc::Sender<RawLog>,
        ) -> Box<dyn Send> {
            self.polygon.fetch_add(1, Ordering::SeqCst);
            Box::new(())
        }
    }

    struct InertGateway;

    #[async_trait]
    impl OrderGateway for InertGateway {
        async fn submit_fok(
            &self,
            _planned: &PlannedOrder,
            _journal: &dyn PrePostJournal,
        ) -> std::result::Result<OrderReceipt, OrderSubmitError> {
            panic!("inert startup gateway cannot submit an order")
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
            _journal: &dyn PrePostJournal,
        ) -> std::result::Result<OrderReceipt, OrderSubmitError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(OrderSubmitError::Uncertain {
                code: OrderErrorCode::PostTransport,
            })
        }
    }
}
