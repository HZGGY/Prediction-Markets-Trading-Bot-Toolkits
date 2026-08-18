//! High-level execution surface: glue between metadata → eligibility →
//! sizing → exposure → risk → signing → POST → position recording.
//!
//! Every bot routes its decided trades through [`OrderExecutor::execute`].
//! Safety flags are checked here, so individual bots never accidentally
//! bypass `enable_trading` or `mock_trading`.

use std::sync::Arc;

use crate::config::AppConfig;
use crate::models::{OrderType, PlannedOrder, Side, WhaleTrade};
use crate::service::clob_sdk_orders::SdkOrderGateway;
use crate::service::eligibility::{self, Eligibility};
use crate::service::execution_circuit_breaker::ExecutionCircuitBreaker;
use crate::service::market_cache::{MarketCache, MarketInfo};
use crate::service::order_gateway::{OrderGateway, OrderReceipt, OrderSubmitError};
use crate::service::position_store::{OpenPosition, PositionStore};
use crate::service::risk_guard::{BlockReason, RiskCheck, RiskGuard};
use crate::service::strategy;
use crate::utils;
use anyhow::{anyhow, Result};
use chrono::Utc;
use tracing::{info, warn};

pub struct OrderExecutor {
    cfg: AppConfig,
    gateway: Option<Arc<dyn OrderGateway>>,
    breaker: Option<Arc<ExecutionCircuitBreaker>>,
    risk: Arc<RiskGuard>,
    markets: Arc<MarketCache>,
    positions: Arc<PositionStore>,
}

#[derive(Debug, Clone)]
pub enum ExecutionOutcome {
    Skipped(SkipReason),
    /// Paper-only plan produced without credentials; no signature or POST.
    DryRunPlanned(PlannedOrder),
    NotSubmitted(OrderSubmitError),
    Filled(OrderReceipt),
}

#[derive(Debug, Clone)]
pub enum SkipReason {
    BelowSizing,
    MarketMetadataUnavailable,
    Ineligible(Eligibility),
    ExposureCategoryCap {
        category: String,
        cap: f64,
        current: f64,
        want: f64,
    },
    ExposureTagCap {
        tag: String,
        cap: f64,
        current: f64,
        want: f64,
    },
    RiskBlocked(BlockReason),
    TradingDisabled,
    AlreadyOpen,
    /// Whale closed/reduced their position. We delegate exits to the TP/SL
    /// monitor rather than mirroring the unwind, so this is a no-op by design.
    WhaleExitIgnored,
}

impl OrderExecutor {
    pub async fn new(
        cfg: AppConfig,
        risk: Arc<RiskGuard>,
        markets: Arc<MarketCache>,
        positions: Arc<PositionStore>,
    ) -> Result<Self> {
        if !cfg.live_trading_allowed() {
            return Ok(Self::new_with_live_components(
                cfg, risk, markets, positions, None, None,
            ));
        }
        let breaker = ExecutionCircuitBreaker::new_live(cfg.trading.execution_halt_path.clone())?;
        let gateway: Arc<dyn OrderGateway> = Arc::new(SdkOrderGateway::new(&cfg).await?);
        Ok(Self::new_with_live_components(
            cfg,
            risk,
            markets,
            positions,
            Some(gateway),
            Some(breaker),
        ))
    }

    fn new_with_live_components(
        cfg: AppConfig,
        risk: Arc<RiskGuard>,
        markets: Arc<MarketCache>,
        positions: Arc<PositionStore>,
        gateway: Option<Arc<dyn OrderGateway>>,
        breaker: Option<Arc<ExecutionCircuitBreaker>>,
    ) -> Self {
        Self {
            cfg,
            gateway,
            breaker,
            risk,
            markets,
            positions,
        }
    }

    pub fn live_order_components(
        &self,
    ) -> Option<(Arc<dyn OrderGateway>, Arc<ExecutionCircuitBreaker>)> {
        Some((
            self.gateway.as_ref()?.clone(),
            self.breaker.as_ref()?.clone(),
        ))
    }

    pub fn positions(&self) -> Arc<PositionStore> {
        Arc::clone(&self.positions)
    }

    pub async fn execute(&self, trade: &WhaleTrade) -> Result<ExecutionOutcome> {
        // 0a. Whale sells are not mirrored — TP/SL is the configured exit path.
        if trade.side == Side::Sell {
            return Ok(ExecutionOutcome::Skipped(SkipReason::WhaleExitIgnored));
        }
        // 0b. Don't pyramid into a market we're already long.
        if self.positions.get(&trade.token_id).is_some() {
            return Ok(ExecutionOutcome::Skipped(SkipReason::AlreadyOpen));
        }

        // 1. Resolve market metadata for category/tags/closed lookup.
        let market = match self.markets.by_token_id(&trade.token_id).await {
            Ok(m) => m,
            Err(_) => {
                warn!(token = %trade.token_id, "market metadata lookup failed");
                return Ok(ExecutionOutcome::Skipped(
                    SkipReason::MarketMetadataUnavailable,
                ));
            }
        };

        // 2. Eligibility (allowlist / blocklist / closed).
        let eligibility = eligibility::check(&self.cfg.filters, &market);
        if eligibility != Eligibility::Allowed {
            info!(
                slug = %market.slug,
                category = ?market.category,
                ?eligibility,
                "market not eligible"
            );
            return Ok(ExecutionOutcome::Skipped(SkipReason::Ineligible(
                eligibility,
            )));
        }

        // 3. Sizing.
        let sizing = strategy::size_for_trade(&self.cfg.strategy, trade);
        if let Some(r) = sizing.skipped {
            info!(?r, "sizing skipped trade");
            return Ok(ExecutionOutcome::Skipped(SkipReason::BelowSizing));
        }

        // 4. Per-category / per-tag exposure caps (entries only).
        if trade.side == Side::Buy {
            if let Some(skip) = self.check_exposure(&market, sizing.copy_usd) {
                info!(?skip, "exposure cap blocked entry");
                return Ok(ExecutionOutcome::Skipped(skip));
            }
        }

        // 5. Risk guard.
        match self.risk.check_fast(trade) {
            RiskCheck::Allow => {}
            RiskCheck::FetchBook => {
                // Caller (or a follow-up) should pull the book and call
                // `risk.check_with_book`. For now we proceed but flag the
                // pre-trade book fetch as the next integration point.
            }
            RiskCheck::Block(reason) => {
                warn!(?reason, "risk guard blocked trade");
                return Ok(ExecutionOutcome::Skipped(SkipReason::RiskBlocked(reason)));
            }
        }

        // 6. Build the order.
        let limit_price = limit_price_for(trade, &self.cfg);
        let shares = utils::usd_to_shares(sizing.copy_usd, limit_price);
        let planned = PlannedOrder {
            venue: trade.venue,
            token_id: trade.token_id.clone(),
            neg_risk: market.neg_risk,
            side: trade.side,
            shares,
            limit_price,
            usd_notional: sizing.copy_usd,
            order_type: OrderType::Fok,
            source_trade_hash: trade.tx_hash.clone(),
        };

        if !self.cfg.live_trading_allowed() {
            info!(
                token = %planned.token_id,
                side = ?planned.side,
                shares = planned.shares,
                price = planned.limit_price,
                "dry-run: order planned but not signed or submitted"
            );
            self.record_open_from_plan(&market, &planned);
            return Ok(ExecutionOutcome::DryRunPlanned(planned));
        }

        let gateway = self
            .gateway
            .as_ref()
            .ok_or_else(|| anyhow!("live gateway unavailable"))?;
        let breaker = self
            .breaker
            .as_ref()
            .ok_or_else(|| anyhow!("live breaker unavailable"))?;
        match breaker.submit_fok(gateway.as_ref(), &planned).await {
            Ok(receipt) => {
                self.record_open_from_receipt(&market, &planned, &receipt)?;
                Ok(ExecutionOutcome::Filled(receipt))
            }
            Err(
                error @ (OrderSubmitError::Preflight { .. } | OrderSubmitError::Rejected { .. }),
            ) => Ok(ExecutionOutcome::NotSubmitted(error)),
            Err(error) => Err(anyhow::Error::new(error)),
        }
    }

    fn check_exposure(&self, market: &MarketInfo, want_usd: f64) -> Option<SkipReason> {
        let filters = &self.cfg.filters;
        if let Some(cat) = market.category.as_ref() {
            if let Some(&cap) = filters
                .per_category_max_open_usd
                .iter()
                .find(|(k, _)| ci_eq(k, cat))
                .map(|(_, v)| v)
            {
                let current = self.positions.open_usd_by_category(cat);
                if current + want_usd > cap {
                    return Some(SkipReason::ExposureCategoryCap {
                        category: cat.clone(),
                        cap,
                        current,
                        want: want_usd,
                    });
                }
            }
        }
        for tag in &market.tags {
            if let Some(&cap) = filters
                .per_tag_max_open_usd
                .iter()
                .find(|(k, _)| ci_eq(k, tag))
                .map(|(_, v)| v)
            {
                let current = self.positions.open_usd_by_tag(tag);
                if current + want_usd > cap {
                    return Some(SkipReason::ExposureTagCap {
                        tag: tag.clone(),
                        cap,
                        current,
                        want: want_usd,
                    });
                }
            }
        }
        None
    }

    fn record_open_from_plan(&self, market: &MarketInfo, planned: &PlannedOrder) {
        // Only entries open positions; exits remove them in the monitor.
        if planned.side != Side::Buy {
            return;
        }
        let (tp_pct, sl_pct) = self.tp_sl_for(market);
        let pos = OpenPosition {
            token_id: planned.token_id.clone(),
            slug: market.slug.clone(),
            category: market.category.clone(),
            tags: market.tags.clone(),
            neg_risk: market.neg_risk,
            side: planned.side,
            entry_price: planned.limit_price,
            shares: planned.shares,
            usd_notional: planned.usd_notional,
            take_profit_pct: tp_pct,
            stop_loss_pct: sl_pct,
            opened_at: Utc::now(),
        };
        self.positions.open(pos);
    }

    fn record_open_from_receipt(
        &self,
        market: &MarketInfo,
        planned: &PlannedOrder,
        receipt: &OrderReceipt,
    ) -> Result<()> {
        if planned.side != Side::Buy {
            return Ok(());
        }
        let shares = receipt.filled_shares();
        let usd_notional = receipt.filled_usd();
        if shares <= 0.0 || usd_notional <= 0.0 {
            return Err(anyhow!("confirmed receipt contains zero fill"));
        }
        let entry_price = usd_notional / shares;
        if !entry_price.is_finite() || entry_price <= 0.0 {
            return Err(anyhow!("confirmed receipt cannot form an entry price"));
        }
        let (tp_pct, sl_pct) = self.tp_sl_for(market);
        self.positions.open(OpenPosition {
            token_id: planned.token_id.clone(),
            slug: market.slug.clone(),
            category: market.category.clone(),
            tags: market.tags.clone(),
            neg_risk: market.neg_risk,
            side: planned.side,
            entry_price,
            shares,
            usd_notional,
            take_profit_pct: tp_pct,
            stop_loss_pct: sl_pct,
            opened_at: Utc::now(),
        });
        Ok(())
    }

    fn tp_sl_for(&self, market: &MarketInfo) -> (f64, f64) {
        let tp_default = self.cfg.tp_sl.default_take_profit_pct;
        let sl_default = self.cfg.tp_sl.default_stop_loss_pct;
        if let Some(cat) = market.category.as_ref() {
            let tp = self
                .cfg
                .tp_sl
                .per_category_tp_pct
                .iter()
                .find(|(k, _)| ci_eq(k, cat))
                .map(|(_, v)| *v)
                .unwrap_or(tp_default);
            let sl = self
                .cfg
                .tp_sl
                .per_category_sl_pct
                .iter()
                .find(|(k, _)| ci_eq(k, cat))
                .map(|(_, v)| *v)
                .unwrap_or(sl_default);
            (tp, sl)
        } else {
            (tp_default, sl_default)
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Arc;

    use crate::config::AppConfig;
    use crate::service::execution_circuit_breaker::ExecutionCircuitBreaker;
    use crate::service::market_cache::MarketCache;
    use crate::service::order_gateway::OrderGateway;
    use crate::service::position_store::PositionStore;
    use crate::service::risk_guard::RiskGuard;

    use super::OrderExecutor;

    pub(crate) fn new_with_live_components(
        cfg: AppConfig,
        risk: Arc<RiskGuard>,
        markets: Arc<MarketCache>,
        positions: Arc<PositionStore>,
        gateway: Option<Arc<dyn OrderGateway>>,
        breaker: Option<Arc<ExecutionCircuitBreaker>>,
    ) -> OrderExecutor {
        OrderExecutor::new_with_live_components(cfg, risk, markets, positions, gateway, breaker)
    }
}

fn limit_price_for(trade: &WhaleTrade, cfg: &AppConfig) -> f64 {
    let buf = cfg.trading.price_buffer;
    let raw = match trade.side {
        Side::Buy => trade.price + buf,
        Side::Sell => trade.price - buf,
    };
    utils::clamp_price(raw)
}

fn ci_eq(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.chars()
            .zip(b.chars())
            .all(|(x, y)| x.eq_ignore_ascii_case(&y))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Side, VenueId};
    use crate::service::execution_circuit_breaker::ExecutionCircuitBreaker;
    use crate::service::onchain::RawLog;
    use crate::service::order_gateway::{
        OrderErrorCode, OrderGateway, OrderReceipt, OrderStage, OrderSubmitError,
    };
    use crate::service::parse::{decode_whale_trade, order_filled_topic};
    use crate::service::position_store::PositionStore;
    use crate::service::risk_guard::RiskGuard;
    use async_trait::async_trait;
    use chrono::Utc;
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    const MAKER: &str = "0x1111111111111111111111111111111111111111";

    fn u256_word(value: u128) -> String {
        format!("{value:064x}")
    }

    fn address_topic(address: &str) -> String {
        format!("0x{}{}", "0".repeat(24), address.trim_start_matches("0x"))
    }

    fn order_filled_fixture() -> RawLog {
        RawLog {
            address: "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e".into(),
            topics: vec![
                format!("0x{}", hex::encode(order_filled_topic().as_slice())),
                format!("0x{}", "11".repeat(32)),
                address_topic(MAKER),
                address_topic("0x2222222222222222222222222222222222222222"),
            ],
            // makerAssetId=0 (USDC), takerAssetId=12345 (outcome token),
            // makerAmountFilled=$100, takerAmountFilled=200 shares, fee=0.
            data: format!(
                "0x{}{}{}{}{}",
                u256_word(0),
                u256_word(12_345),
                u256_word(100_000_000),
                u256_word(200_000_000),
                u256_word(0),
            ),
            tx_hash: "0xreplay-fixture".into(),
            block_number: 123,
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

    #[tokio::test]
    async fn order_filled_replay_without_credentials_records_paper_position() {
        let trade = decode_whale_trade(&order_filled_fixture(), MAKER)
            .unwrap()
            .expect("fixture must decode for the tracked maker");
        assert_eq!(trade.venue, VenueId::Polymarket);
        assert_eq!(trade.side, Side::Buy);
        assert_eq!(trade.token_id, "12345");
        assert!((trade.shares - 200.0).abs() < 1e-9);
        assert!((trade.usd_notional - 100.0).abs() < 1e-9);
        assert!((trade.price - 0.5).abs() < 1e-9);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(serve_fixture_market(listener));

        let mut cfg: AppConfig = serde_json::from_str(include_str!("../../config.json")).unwrap();
        cfg.site.gamma_api_base = base;
        cfg.tp_sl.enabled = false;

        let risk = RiskGuard::new(cfg.risk.clone());
        let markets = MarketCache::new(reqwest::Client::new(), cfg.site.gamma_api_base.clone());
        let positions = PositionStore::new();
        let executor = OrderExecutor::new(cfg, risk, markets, Arc::clone(&positions))
            .await
            .unwrap();

        let outcome = executor.execute(&trade).await.unwrap();
        server.await.unwrap();

        match outcome {
            ExecutionOutcome::DryRunPlanned(planned) => {
                assert_eq!(planned.token_id, "12345");
                assert_eq!(planned.side, Side::Buy);
                assert_eq!(planned.usd_notional, 20.0);
                assert_eq!(planned.limit_price, 0.505);
            }
            other => panic!("paper dry-run must produce a plan: {other:?}"),
        }
        let position = positions
            .get("12345")
            .expect("paper dry-run should record the planned entry");
        assert!((position.usd_notional - 20.0).abs() < 1e-9);
        assert!((position.entry_price - 0.505).abs() < 1e-9);
        assert_eq!(position.shares, 39.0);
    }

    #[tokio::test]
    async fn strict_dry_run_skips_live_components_and_records_planned_position() {
        let (cfg, markets, server) = fixture_runtime(false).await;
        let positions = PositionStore::new();
        let executor = OrderExecutor::new(
            cfg,
            RiskGuard::new(test_cfg()),
            markets,
            Arc::clone(&positions),
        )
        .await
        .unwrap();

        assert!(executor.live_order_components().is_none());
        assert!(matches!(
            executor.execute(&fixture_trade()).await.unwrap(),
            ExecutionOutcome::DryRunPlanned(_)
        ));
        server.await.unwrap();
        assert!(positions.get("12345").is_some());
    }

    #[tokio::test]
    async fn matched_live_receipt_records_actual_filled_amounts() {
        let (mut cfg, markets, server) = fixture_runtime(true).await;
        let halt_dir = tempfile::tempdir().unwrap();
        cfg.trading.execution_halt_path = halt_dir.path().join("execution-halt.json");
        let positions = PositionStore::new();
        let gateway = Arc::new(FakeGateway::returning(Ok(OrderReceipt {
            order_id: "order-public-fixture".into(),
            filled_shares_micros: 39_000_000,
            filled_usd_micros: 19_500_000,
        })));
        let breaker =
            ExecutionCircuitBreaker::new_live(cfg.trading.execution_halt_path.clone()).unwrap();
        let executor = OrderExecutor::new_with_live_components(
            cfg,
            RiskGuard::new(test_cfg()),
            markets,
            Arc::clone(&positions),
            Some(gateway.clone()),
            Some(breaker),
        );

        assert!(matches!(
            executor.execute(&fixture_trade()).await.unwrap(),
            ExecutionOutcome::Filled(_)
        ));
        server.await.unwrap();
        let position = positions.get("12345").unwrap();
        assert_eq!(position.shares, 39.0);
        assert_eq!(position.usd_notional, 19.5);
        assert_eq!(position.entry_price, 0.5);
        assert_eq!(gateway.calls(), 1);
    }

    #[tokio::test]
    async fn preflight_and_rejected_entries_are_not_submitted_or_recorded() {
        for result in [
            Err(OrderSubmitError::Preflight {
                stage: OrderStage::Build,
                code: OrderErrorCode::InvalidPrice,
            }),
            Err(OrderSubmitError::Rejected {
                http_status: Some(409),
                code: OrderErrorCode::HttpRejected,
            }),
        ] {
            let (mut cfg, markets, server) = fixture_runtime(true).await;
            let halt_dir = tempfile::tempdir().unwrap();
            cfg.trading.execution_halt_path = halt_dir.path().join("execution-halt.json");
            let positions = PositionStore::new();
            let gateway = Arc::new(FakeGateway::returning(result));
            let breaker =
                ExecutionCircuitBreaker::new_live(cfg.trading.execution_halt_path.clone()).unwrap();
            let executor = OrderExecutor::new_with_live_components(
                cfg,
                RiskGuard::new(test_cfg()),
                markets,
                Arc::clone(&positions),
                Some(gateway),
                Some(breaker),
            );

            assert!(matches!(
                executor.execute(&fixture_trade()).await.unwrap(),
                ExecutionOutcome::NotSubmitted(_)
            ));
            server.await.unwrap();
            assert!(positions.is_empty());
        }
    }

    #[tokio::test]
    async fn uncertain_entry_halts_persists_and_never_retries() {
        let (mut cfg, markets, server) = fixture_runtime(true).await;
        let halt_dir = tempfile::tempdir().unwrap();
        cfg.trading.execution_halt_path = halt_dir.path().join("execution-halt.json");
        let marker = cfg.trading.execution_halt_path.clone();
        let positions = PositionStore::new();
        let gateway = Arc::new(FakeGateway::returning(Err(OrderSubmitError::Uncertain {
            code: OrderErrorCode::PostTimeout,
        })));
        let breaker = ExecutionCircuitBreaker::new_live(marker.clone()).unwrap();
        let executor = OrderExecutor::new_with_live_components(
            cfg,
            RiskGuard::new(test_cfg()),
            markets,
            Arc::clone(&positions),
            Some(gateway.clone()),
            Some(breaker),
        );

        assert!(executor.execute(&fixture_trade()).await.is_err());
        server.await.unwrap();
        assert!(positions.is_empty());
        assert!(marker.is_file());
        assert_eq!(gateway.calls(), 1);
        assert!(executor.execute(&fixture_trade()).await.is_err());
        assert_eq!(gateway.calls(), 1);
    }

    fn test_cfg() -> crate::config::RiskConfig {
        serde_json::from_str::<AppConfig>(include_str!("../../config.json"))
            .unwrap()
            .risk
    }

    fn fixture_trade() -> WhaleTrade {
        WhaleTrade {
            venue: VenueId::Polymarket,
            maker: MAKER.into(),
            side: Side::Buy,
            token_id: "12345".into(),
            shares: 200.0,
            price: 0.5,
            usd_notional: 100.0,
            tx_hash: Some("fixture-trade".into()),
            block_number: Some(1),
            observed_at: Utc::now(),
        }
    }

    async fn fixture_runtime(
        live: bool,
    ) -> (AppConfig, Arc<MarketCache>, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut cfg: AppConfig = serde_json::from_str(include_str!("../../config.json")).unwrap();
        cfg.site.gamma_api_base = format!("http://{}", listener.local_addr().unwrap());
        cfg.site.clob_api_base = "http://127.0.0.1:9".into();
        cfg.bot.enable_trading = live;
        cfg.bot.mock_trading = !live;
        let markets = MarketCache::new(reqwest::Client::new(), cfg.site.gamma_api_base.clone());
        (cfg, markets, tokio::spawn(serve_fixture_market(listener)))
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
