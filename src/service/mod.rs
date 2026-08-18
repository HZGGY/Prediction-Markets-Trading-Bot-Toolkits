//! Shared services consumed by every bot:
//!
//! - [`strategy`]         — copy-sizing strategies (percentage / fixed / adaptive)
//! - [`risk_guard`]       — circuit breaker + depth check
//! - [`market_cache`]     — slug ↔ CLOB token-id ↔ category/tags resolution
//! - [`onchain`]          — Polygon WebSocket subscription
//! - [`parse`]            — ABI log decoding for Polymarket exchange events
//! - [`clob_sdk_orders`]  — official Rust V2 SDK FOK order build/sign/L2/POST
//! - [`order_executor`]   — applies sizing, eligibility, exposure, risk, and dispatch
//! - [`eligibility`]      — allowlist/blocklist filter for slugs, categories, tags
//! - [`position_store`]   — open-position tracking + exposure totals
//! - [`midprice`]         — `/midpoint` HTTP client (trait, swappable)
//! - [`position_monitor`] — TP/SL polling loop, submits guarded FOK exits

pub mod clob_auth;
pub mod clob_sdk_orders;
pub mod eligibility;
pub mod execution_circuit_breaker;
pub mod market_cache;
pub mod midprice;
pub mod onchain;
pub mod order_executor;
pub mod order_gateway;
pub mod parse;
pub mod position_monitor;
pub mod position_store;
pub mod risk_guard;
pub mod strategy;
