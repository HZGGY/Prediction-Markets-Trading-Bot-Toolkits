//! Copy-trading bot — production-ready.
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
//! 7. **Execute**: build EIP-712 signed CTF order, post via L2 auth
//!    ([`service::clob`], [`service::order_executor`]).
//! 8. **TP/SL**: live-only background monitor polls midprice for every open
//!    position and submits a guarded FOK exit when P&L crosses the configured
//!    thresholds ([`service::position_monitor`]).
//!
//! Safety: `enable_trading=false` OR `mock_trading=true` keeps the executor
//! in dry-run mode — the full pipeline runs but plans are recorded without
//! SDK initialization, signing, CLOB requests, or TP/SL midpoint polling.

use crate::config::AppConfig;
use crate::service::{
    execution_circuit_breaker::ExecutionCircuitBreaker,
    market_cache::MarketCache,
    midprice::{ClobMidpriceSource, MidpriceSource},
    onchain::{spawn_subscription, LogFilter, RawLog},
    order_executor::{ExecutionOutcome, OrderExecutor},
    order_gateway::{OrderGateway, OrderReceipt, OrderSubmitError},
    parse::{decode_whale_trade, order_filled_topic},
    position_monitor,
    position_store::PositionStore,
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
    let positions = PositionStore::new();

    let executor = OrderExecutor::new(
        cfg.clone(),
        Arc::clone(&risk),
        Arc::clone(&markets),
        Arc::clone(&positions),
    )
    .await?;

    if let Some((gateway, breaker)) = live_tp_sl_components(&cfg, &executor) {
        let midprice: Arc<dyn MidpriceSource> = Arc::new(ClobMidpriceSource::new(
            http.clone(),
            cfg.site.clob_api_base.clone(),
        ));
        position_monitor::spawn(
            cfg.tp_sl.clone(),
            Arc::clone(&positions),
            gateway,
            breaker,
            midprice,
            cfg.trading.price_buffer,
        );
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
            "dry-run order planned (not signed or submitted)"
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
        order_id_hint = %redact_order_id(&receipt.order_id),
        shares = receipt.filled_shares(),
        usd = receipt.filled_usd(),
        "order fully matched"
    );
}

fn redact_order_id(order_id: &str) -> String {
    const VISIBLE: usize = 4;
    let characters = order_id.chars().collect::<Vec<_>>();
    if characters.len() <= VISIBLE * 2 {
        return "[redacted]".into();
    }
    format!(
        "{}…{}",
        characters[..VISIBLE].iter().collect::<String>(),
        characters[characters.len() - VISIBLE..]
            .iter()
            .collect::<String>()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn strict_dry_run_never_exposes_tp_sl_live_components() {
        let mut cfg: AppConfig = serde_json::from_str(include_str!("../../config.json")).unwrap();
        cfg.tp_sl.enabled = true;
        cfg.bot.enable_trading = false;
        cfg.bot.mock_trading = true;
        cfg.site.clob_api_base = "http://127.0.0.1:9".into();
        let risk = RiskGuard::new(cfg.risk.clone());
        let markets = MarketCache::new(Client::new(), cfg.site.gamma_api_base.clone());
        let executor = OrderExecutor::new(cfg.clone(), risk, markets, PositionStore::new())
            .await
            .unwrap();

        assert!(live_tp_sl_components(&cfg, &executor).is_none());
    }

    #[test]
    fn redacts_short_order_ids_without_disclosing_them() {
        assert_eq!(redact_order_id("short"), "[redacted]");
    }

    #[test]
    fn redacts_normal_ascii_order_ids_with_prefix_and_suffix() {
        assert_eq!(redact_order_id("order-public-fixture"), "orde…ture");
    }

    #[test]
    fn redacts_multibyte_order_ids_on_character_boundaries() {
        assert_eq!(
            redact_order_id("订单编号交易确认成功回执"),
            "订单编号…成功回执"
        );
    }
}
