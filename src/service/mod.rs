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
//!
//! Durable ledger mutation is internal orchestration state. Downstream crates
//! cannot forge reconciliation, recovery, acknowledgement, or cleanup events:
//!
//! ```compile_fail,E0624
//! use polymarket_toolkits::service::execution_ledger::{
//!     ExecutionLedger, IntentId, LedgerPayload, MatchedAmounts,
//! };
//!
//! fn append_reconciled_match(
//!     ledger: &ExecutionLedger,
//!     intent_id: IntentId,
//!     amounts: MatchedAmounts,
//! ) {
//!     let _ = ledger.append(intent_id, LedgerPayload::ReconciledMatched(amounts));
//! }
//! ```
//!
//! ```compile_fail,E0624
//! use polymarket_toolkits::service::execution_ledger::{
//!     ExecutionLedger, IntentId, LedgerPayload, TerminalNoFillStatus,
//! };
//!
//! fn append_reconciled_no_fill(
//!     ledger: &ExecutionLedger,
//!     intent_id: IntentId,
//!     status: TerminalNoFillStatus,
//! ) {
//!     let _ = ledger.append(intent_id, LedgerPayload::ReconciledNoFill { status });
//! }
//! ```
//!
//! ```compile_fail,E0624
//! use polymarket_toolkits::service::execution_ledger::{
//!     EventId, ExecutionLedger, IntentId, LedgerPayload,
//! };
//!
//! fn append_recovery_applied(
//!     ledger: &ExecutionLedger,
//!     intent_id: IntentId,
//!     position_event_id: EventId,
//! ) {
//!     let _ = ledger.append(intent_id, LedgerPayload::RecoveryApplied { position_event_id });
//! }
//! ```
//!
//! ```compile_fail,E0624
//! use polymarket_toolkits::service::execution_ledger::{
//!     AcknowledgeReason, ExecutionLedger, IntentId, LedgerPayload,
//! };
//!
//! fn append_acknowledged(
//!     ledger: &ExecutionLedger,
//!     intent_id: IntentId,
//!     reason: AcknowledgeReason,
//! ) {
//!     let _ = ledger.append(intent_id, LedgerPayload::Acknowledged { reason });
//! }
//! ```
//!
//! ```compile_fail,E0624
//! use polymarket_toolkits::service::execution_ledger::{
//!     ExecutionLedger, IntentId, LedgerPayload,
//! };
//!
//! fn append_cleanup_completed(ledger: &ExecutionLedger, intent_id: IntentId) {
//!     let _ = ledger.append(intent_id, LedgerPayload::HaltMarkerCleanupCompleted);
//! }
//! ```
//!
//! A durable [`position_store::PositionStore`] is likewise constructed and
//! mutated only by internal executor/recovery orchestration:
//!
//! ```compile_fail,E0624
//! use std::sync::Arc;
//! use polymarket_toolkits::service::{
//!     execution_ledger::ExecutionLedger,
//!     position_store::PositionStore,
//! };
//!
//! fn construct_durable_store(ledger: Arc<ExecutionLedger>) {
//!     let _ = PositionStore::from_ledger(ledger);
//! }
//! ```
//!
//! ```compile_fail,E0624
//! use polymarket_toolkits::service::position_store::{OpenPosition, PositionStore};
//!
//! fn mutate_durable_open(store: &PositionStore, position: OpenPosition) {
//!     let _ = store.apply_open(position);
//! }
//! ```
//!
//! ```compile_fail,E0624
//! use polymarket_toolkits::service::{
//!     execution_ledger::PositionClose,
//!     position_store::PositionStore,
//! };
//!
//! fn mutate_durable_close(store: &PositionStore, close: PositionClose) {
//!     let _ = store.apply_close(close);
//! }
//! ```
//!
//! Recovery is likewise an internal exact-only capability. Downstream crates
//! cannot obtain its neutral trait or construct the official SDK adapter:
//!
//! ```compile_fail
//! use polymarket_toolkits::service::recovery_gateway::RecoveryGateway;
//!
//! fn obtain_raw_recovery_trait(gateway: &dyn RecoveryGateway) {
//!     let _ = gateway;
//! }
//! ```
//!
//! ```compile_fail
//! use polymarket_toolkits::{
//!     config::AppConfig,
//!     service::clob_sdk_recovery::SdkRecoveryGateway,
//! };
//!
//! async fn construct_recovery_gateway(cfg: &AppConfig) {
//!     let _ = SdkRecoveryGateway::new(cfg).await;
//! }
//! ```

pub mod clob_auth;
pub mod clob_sdk_orders;
pub(crate) mod clob_sdk_recovery;
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
pub(crate) mod recovery_gateway;
pub(crate) mod recovery_service;
pub mod risk_guard;
pub mod strategy;
