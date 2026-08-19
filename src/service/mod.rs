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
//!
//! The production order-submission seam is intentionally internal. A
//! downstream crate cannot replace the durable pre-POST journal or extract the
//! raw live gateway from an executor:
//!
//! ```compile_fail
//! use polymarket_toolkits::{
//!     config::AppConfig,
//!     models::PlannedOrder,
//!     service::{
//!         clob_sdk_orders::SdkOrderGateway,
//!         order_gateway::{
//!             OrderGateway, OrderSubmitError, PrePostJournal, PreparedOrderIdentity,
//!         },
//!     },
//! };
//!
//! struct NoOpJournal;
//!
//! impl PrePostJournal for NoOpJournal {
//!     fn before_post(
//!         &self,
//!         _identity: &PreparedOrderIdentity,
//!     ) -> Result<(), OrderSubmitError> {
//!         Ok(())
//!     }
//! }
//!
//! async fn bypass_with_no_op_journal(gateway: &dyn OrderGateway, planned: &PlannedOrder) {
//!     let _ = gateway.submit_fok(planned, &NoOpJournal).await;
//! }
//!
//! async fn construct_official_gateway(cfg: &AppConfig) {
//!     let _ = SdkOrderGateway::new(cfg).await;
//! }
//! ```
//!
//! ```compile_fail
//! use polymarket_toolkits::service::order_gateway::OrderGateway;
//!
//! fn obtain_raw_submission_trait(gateway: &dyn OrderGateway) {
//!     let _ = gateway;
//! }
//! ```
//!
//! ```compile_fail
//! use polymarket_toolkits::{
//!     config::AppConfig,
//!     service::clob_sdk_orders::SdkOrderGateway,
//! };
//!
//! async fn construct_official_gateway(cfg: &AppConfig) {
//!     let _ = SdkOrderGateway::new(cfg).await;
//! }
//! ```
//!
//! ```compile_fail
//! use polymarket_toolkits::service::order_executor::OrderExecutor;
//!
//! fn extract_raw_live_submission_components(executor: &OrderExecutor) {
//!     let _ = executor.live_order_components();
//! }
//! ```

pub mod clob_auth;
pub mod clob_sdk_orders;
pub mod eligibility;
pub mod execution_circuit_breaker;
pub mod execution_ledger;
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
