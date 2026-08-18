# Official SDK Order Execution Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the custom production CLOB order builder, EIP-712 signer, L2 HMAC, and order POST path with the official Rust V2 SDK while preserving strict no-CLOB dry-run behavior and failing closed on every uncertain post state.

**Architecture:** Introduce SDK-neutral order contracts, put all official SDK business types in `clob_sdk_orders.rs`, and place a persistent execution circuit breaker between both live callers and the gateway. `OrderExecutor` and the TP/SL monitor share the same gateway and breaker; only an exact fully matched FOK receipt updates positions. The former `clob.rs` backend is deleted after all callers and tests migrate, leaving no runtime backend switch.

**Tech Stack:** Rust 2021, Tokio, `async-trait`, Serde/Serde JSON, `tempfile`, Chrono, Thiserror, official `polymarket_client_sdk_v2 = 0.6.0`, Alloy 1.x types used by that SDK, existing Reqwest/Tracing test conventions.

**Spec:** `docs/superpowers/specs/2026-08-18-official-sdk-order-migration-design.md`

## Global Constraints

- This plan implements phase 2 of the confirmed three-phase official SDK migration.
- Use `polymarket_client_sdk_v2 = 0.6.0` as the only production implementation of order build, EIP-712 signing, L2 authentication, and `POST /order`.
- Support Polygon chain ID `137`, EOA `signature_type = 0`, and require funder to equal signer.
- Preserve true FOK behavior; do not map phase-2 orders to SDK FAK, GTC, or GTD.
- Strict dry-run performs no CLOB HTTP request, creates no authenticated SDK client, builds no SDK order, and signs nothing.
- Positions change only for `success = true`, exact `Matched`, non-empty order ID, and exact expected making/taking amounts.
- Never retry an uncertain post. Timeout, disconnect, malformed success response, non-final successful status, and response amount ambiguity halt all entries and exits.
- Persist uncertainty to `trading.execution_halt_path`, default `execution-halt.json`; never clear it automatically.
- Never log or persist private keys, complete API keys, API secrets, passphrases, signatures, signed payloads, L2 headers, raw response bodies, or raw SDK errors.
- Keep committed configurations at `enable_trading = false` and `mock_trading = true`.
- All behavioral tests are offline or use `127.0.0.1`; do not call a real CLOB, Gamma, Polygon, or credential endpoint.
- Do not run authentication commands, do not enter real credentials, and do not submit a real order.
- Do not run whole-repository formatting; format only intentionally changed Rust files and preserve the known unrelated formatting baseline.
- Do not push, merge, or delete the branch without the user's explicit choice after final verification.

---

### Task 1: Rename the internal order intent from misleading FAK to FOK

**Files:**
- Modify/Test: `src/models.rs`
- Modify: `src/service/clob.rs`
- Modify: `src/service/order_executor.rs`
- Modify: `src/service/position_monitor.rs`

**Interfaces:**
- Consumes: existing `OrderType::Fak`, whose serializer currently emits wire value `FOK`.
- Produces: `OrderType::Fok`, preserving the current wire behavior before the old backend is removed.

- [ ] **Step 1: Write the failing FOK naming test**

Add to `src/models.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_two_order_intent_is_named_fok() {
        let order_type = OrderType::Fok;
        assert!(matches!(order_type, OrderType::Fok));
    }
}
```

- [ ] **Step 2: Run the focused test and verify RED**

```powershell
cargo test --offline --locked models::tests::phase_two_order_intent_is_named_fok
```

Expected: compile failure because `OrderType::Fok` does not exist.

- [ ] **Step 3: Rename only the internal variant and references**

Change the enum declaration to:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    /// Fill-or-kill; must map to official SDK `OrderType::FOK`.
    Fok,
    /// Good-till-date — retained for non-phase-2 stubs only.
    Gtd,
    /// Good-till-cancel — retained for non-phase-2 stubs only.
    Gtc,
}
```

Replace every `OrderType::Fak` reference with `OrderType::Fok`. In the temporary legacy serializer, keep:

```rust
OrderType::Fok => "FOK",
```

Do not change it to `FAK`.

- [ ] **Step 4: Run naming and existing protocol regressions and verify GREEN**

```powershell
cargo test --offline --locked models::tests::phase_two_order_intent_is_named_fok
cargo test --offline --locked service::clob::tests
```

Expected: both commands PASS and the legacy fixed order still serializes `FOK`.

- [ ] **Step 5: Commit Task 1**

```powershell
git add -- src/models.rs src/service/clob.rs src/service/order_executor.rs src/service/position_monitor.rs
git commit -m "refactor: name fill-or-kill orders explicitly"
```

---

### Task 2: Add the persistent execution-halt path to public configuration

**Files:**
- Modify/Test: `src/config.rs`
- Modify: `config.json`
- Modify: `config.dryrun-public.json`

**Interfaces:**
- Consumes: existing `TradingConfig`.
- Produces: `TradingConfig::execution_halt_path: PathBuf`, defaulting to `execution-halt.json`.

- [ ] **Step 1: Write failing default and committed-config tests**

Add to the existing config tests:

```rust
#[test]
fn execution_halt_path_defaults_when_omitted() {
    let mut value: serde_json::Value =
        serde_json::from_str(include_str!("../config.json")).unwrap();
    value["trading"].as_object_mut().unwrap().remove("execution_halt_path");
    let cfg: AppConfig = serde_json::from_value(value).unwrap();
    assert_eq!(cfg.trading.execution_halt_path, PathBuf::from("execution-halt.json"));
}

#[test]
fn committed_configs_pin_safe_halt_path_and_trading_flags() {
    for raw in [
        include_str!("../config.json"),
        include_str!("../config.dryrun-public.json"),
    ] {
        let cfg: AppConfig = serde_json::from_str(raw).unwrap();
        assert_eq!(cfg.trading.execution_halt_path, PathBuf::from("execution-halt.json"));
        assert!(!cfg.bot.enable_trading);
        assert!(cfg.bot.mock_trading);
    }
}
```

- [ ] **Step 2: Run the focused tests and verify RED**

```powershell
cargo test --offline --locked config::tests::execution_halt_path_defaults_when_omitted
cargo test --offline --locked config::tests::committed_configs_pin_safe_halt_path_and_trading_flags
```

Expected: compile failure because `execution_halt_path` is absent.

- [ ] **Step 3: Add the field, default, and explicit committed values**

Import `PathBuf` and extend `TradingConfig`:

```rust
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingConfig {
    pub rate_limit: u32,
    pub rate_window_secs: u64,
    pub poll_interval_secs: u64,
    pub price_buffer: f64,
    pub fee_rate_bps: u32,
    pub order_expiration_secs: u64,
    #[serde(default = "default_execution_halt_path")]
    pub execution_halt_path: PathBuf,
}

fn default_execution_halt_path() -> PathBuf {
    PathBuf::from("execution-halt.json")
}
```

Add this property to each JSON `trading` object without changing any other value:

```json
"execution_halt_path": "execution-halt.json"
```

- [ ] **Step 4: Run all config tests and verify GREEN**

```powershell
cargo test --offline --locked config::tests
```

Expected: all config tests PASS.

- [ ] **Step 5: Commit Task 2**

```powershell
git add -- src/config.rs config.json config.dryrun-public.json
git commit -m "feat: configure persistent execution halt marker"
```

---

### Task 3: Define SDK-neutral order gateway contracts and exact receipt units

**Files:**
- Create/Test: `src/service/order_gateway.rs`
- Modify: `src/service/mod.rs`

**Interfaces:**
- Consumes: `PlannedOrder` and internal `Side`.
- Produces: `OrderGateway::submit_fok`, `OrderReceipt`, `OrderStage`, `OrderErrorCode`, and `OrderSubmitError`.

- [ ] **Step 1: Create failing tests for exact units and sanitized errors**

Create `src/service/order_gateway.rs` with only this test module first:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receipt_converts_micro_units_only_at_position_boundary() {
        let receipt = OrderReceipt {
            order_id: "0xabc".to_owned(),
            filled_shares_micros: 12_345_678,
            filled_usd_micros: 6_172_839,
        };
        assert!((receipt.filled_shares() - 12.345_678).abs() < 1e-12);
        assert!((receipt.filled_usd() - 6.172_839).abs() < 1e-12);
    }

    #[test]
    fn rendered_errors_are_stable_and_contain_no_dynamic_secret() {
        let sentinel = "SERVER_BODY_SECRET_SENTINEL";
        let error = OrderSubmitError::Rejected {
            http_status: Some(429),
            code: OrderErrorCode::HttpRejected,
        };
        let rendered = format!("{error:?} {error}");
        assert!(rendered.contains("HttpRejected"));
        assert!(!rendered.contains(sentinel));
    }
}
```

- [ ] **Step 2: Run the module and verify RED**

```powershell
cargo test --offline --locked service::order_gateway::tests
```

Expected: compile failure because the module and contract types do not exist.

- [ ] **Step 3: Implement the complete neutral contract**

Add `pub mod order_gateway;` to `src/service/mod.rs`, then implement:

```rust
use async_trait::async_trait;
use thiserror::Error;

use crate::models::PlannedOrder;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderReceipt {
    pub order_id: String,
    pub filled_shares_micros: u128,
    pub filled_usd_micros: u128,
}

impl OrderReceipt {
    pub fn filled_shares(&self) -> f64 {
        self.filled_shares_micros as f64 / 1_000_000.0
    }

    pub fn filled_usd(&self) -> f64 {
        self.filled_usd_micros as f64 / 1_000_000.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStage {
    Initialization,
    Metadata,
    Build,
    Sign,
    Post,
    Response,
    CircuitBreaker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderErrorCode {
    InvalidHost,
    InvalidChain,
    UnsupportedSignatureType,
    FunderMismatch,
    MissingCredentials,
    InvalidTokenId,
    MetadataLookupFailed,
    NegRiskMismatch,
    InvalidTickSize,
    InvalidPrice,
    InvalidSize,
    UnsupportedProtocolVersion,
    AmountConversion,
    SdkBuild,
    SdkSign,
    HttpRejected,
    ServerRejected,
    PostTimeout,
    PostTransport,
    MalformedResponse,
    NonFinalStatus,
    EmptyOrderId,
    AmountMismatch,
    HaltMarkerPresent,
    HaltMarkerIo,
    ExecutionHalted,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum OrderSubmitError {
    #[error("order preflight failed at {stage:?} ({code:?})")]
    Preflight { stage: OrderStage, code: OrderErrorCode },
    #[error("order rejected with status {http_status:?} ({code:?})")]
    Rejected { http_status: Option<u16>, code: OrderErrorCode },
    #[error("order result uncertain ({code:?})")]
    Uncertain { code: OrderErrorCode },
    #[error("order execution halted ({code:?})")]
    Halted { code: OrderErrorCode },
}

impl OrderSubmitError {
    pub fn code(&self) -> OrderErrorCode {
        match self {
            Self::Preflight { code, .. }
            | Self::Rejected { code, .. }
            | Self::Uncertain { code }
            | Self::Halted { code } => *code,
        }
    }

    pub fn is_uncertain(&self) -> bool {
        matches!(self, Self::Uncertain { .. })
    }
}

#[async_trait]
pub trait OrderGateway: Send + Sync {
    async fn submit_fok(
        &self,
        planned: &PlannedOrder,
    ) -> Result<OrderReceipt, OrderSubmitError>;
}
```

Do not import an SDK type into this file.

- [ ] **Step 4: Run the module and verify GREEN**

```powershell
cargo test --offline --locked service::order_gateway::tests
```

Expected: both tests PASS.

- [ ] **Step 5: Commit Task 3**

```powershell
git add -- src/service/order_gateway.rs src/service/mod.rs
git commit -m "feat: define neutral order gateway contracts"
```

---

### Task 4: Add the shared in-memory and persistent execution circuit breaker

**Files:**
- Create/Test: `src/service/execution_circuit_breaker.rs`
- Modify: `src/service/mod.rs`

**Interfaces:**
- Consumes: `OrderGateway`, `OrderSubmitError`, `PlannedOrder`, and configured marker path.
- Produces: `ExecutionCircuitBreaker::new_live`, `check`, `halt_uncertain`, and guarded `submit_fok`.

- [ ] **Step 1: Write failing marker and guarded-submission tests**

Add tests using a `FakeGateway` with an `AtomicUsize` call counter. The wished-for API is:

```rust
#[tokio::test]
async fn uncertainty_persists_marker_and_blocks_every_later_submission() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("execution-halt.json");
    let breaker = ExecutionCircuitBreaker::new_live(path.clone()).unwrap();
    let gateway = FakeGateway::returning(Err(OrderSubmitError::Uncertain {
        code: OrderErrorCode::PostTimeout,
    }));
    let planned = fixture_planned_order();

    let first = breaker.submit_fok(&gateway, &planned).await.unwrap_err();
    assert!(first.is_uncertain());
    assert!(path.is_file());
    assert_eq!(gateway.calls(), 1);

    let second = breaker.submit_fok(&gateway, &planned).await.unwrap_err();
    assert!(matches!(second, OrderSubmitError::Halted { .. }));
    assert_eq!(gateway.calls(), 1);

    let marker: ExecutionHaltMarker =
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(marker.schema_version, 1);
    assert_eq!(marker.reason_code, "PostTimeout");
    assert_eq!(marker.token_id, planned.token_id);
}

#[test]
fn existing_marker_blocks_live_startup() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("execution-halt.json");
    std::fs::write(&path, b"{}").unwrap();
    assert!(matches!(
        ExecutionCircuitBreaker::new_live(path),
        Err(OrderSubmitError::Halted {
            code: OrderErrorCode::HaltMarkerPresent
        })
    ));
}

#[test]
fn persist_failure_leaves_memory_halted() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("execution-halt.json");
    let breaker = ExecutionCircuitBreaker::new_live(target).unwrap();
    let result = breaker.halt_uncertain_with(
        &fixture_planned_order(),
        OrderErrorCode::PostTransport,
        |_temp, _target| Err(std::io::Error::other("simulated persist failure")),
    );
    assert!(matches!(result, Err(OrderSubmitError::Halted { .. })));
    assert!(breaker.is_halted());
}
```

Implement `FakeGateway` in the same test module with exactly one configurable cloned result and an atomic call counter.

```rust
struct FakeGateway {
    result: Result<OrderReceipt, OrderSubmitError>,
    calls: AtomicUsize,
    delay: Duration,
}

impl FakeGateway {
    fn returning(result: Result<OrderReceipt, OrderSubmitError>) -> Self {
        Self { result, calls: AtomicUsize::new(0), delay: Duration::ZERO }
    }

    fn returning_after(
        result: Result<OrderReceipt, OrderSubmitError>,
        delay: Duration,
    ) -> Self {
        Self { result, calls: AtomicUsize::new(0), delay }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl OrderGateway for FakeGateway {
    async fn submit_fok(
        &self,
        _planned: &PlannedOrder,
    ) -> Result<OrderReceipt, OrderSubmitError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        self.result.clone()
    }
}

fn fixture_planned_order() -> PlannedOrder {
    PlannedOrder {
        venue: VenueId::Polymarket,
        token_id: "12345".to_owned(),
        neg_risk: false,
        side: Side::Buy,
        shares: 39.0,
        limit_price: 0.505,
        usd_notional: 20.0,
        order_type: OrderType::Fok,
        source_trade_hash: None,
    }
}
```

Add this concurrency regression; without a submission mutex it makes two gateway calls and fails:

```rust
#[tokio::test]
async fn concurrent_submission_waits_and_never_posts_after_first_uncertainty() {
    let dir = tempfile::tempdir().unwrap();
    let breaker = ExecutionCircuitBreaker::new_live(
        dir.path().join("execution-halt.json"),
    ).unwrap();
    let gateway = FakeGateway::returning_after(
        Err(OrderSubmitError::Uncertain {
            code: OrderErrorCode::PostTransport,
        }),
        Duration::from_millis(20),
    );
    let planned = fixture_planned_order();
    let (first, second) = tokio::join!(
        breaker.submit_fok(&gateway, &planned),
        breaker.submit_fok(&gateway, &planned),
    );
    assert!(first.is_err());
    assert!(second.is_err());
    assert_eq!(gateway.calls(), 1);
}
```

- [ ] **Step 2: Run the breaker tests and verify RED**

```powershell
cargo test --offline --locked service::execution_circuit_breaker::tests
```

Expected: compile failure because the breaker module does not exist.

- [ ] **Step 3: Implement the marker schema and startup checks**

Add `pub mod execution_circuit_breaker;` and create:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionHaltMarker {
    pub schema_version: u32,
    pub halted_at: DateTime<Utc>,
    pub reason_code: String,
    pub stage: String,
    pub token_id: String,
    pub side: String,
    pub order_id_hint: Option<String>,
}

pub struct ExecutionCircuitBreaker {
    halted: AtomicBool,
    path: PathBuf,
    submit_lock: tokio::sync::Mutex<()>,
}

impl ExecutionCircuitBreaker {
    pub fn new_live(path: PathBuf) -> Result<Arc<Self>, OrderSubmitError> {
        if path.exists() {
            return Err(OrderSubmitError::Halted {
                code: OrderErrorCode::HaltMarkerPresent,
            });
        }
        let parent = path.parent().filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        if !parent.is_dir()
            || tempfile::Builder::new()
                .prefix(".execution-halt-probe-")
                .tempfile_in(parent)
                .is_err()
        {
            return Err(OrderSubmitError::Preflight {
                stage: OrderStage::Initialization,
                code: OrderErrorCode::HaltMarkerIo,
            });
        }
        Ok(Arc::new(Self {
            halted: AtomicBool::new(false),
            path,
            submit_lock: tokio::sync::Mutex::new(()),
        }))
    }

    pub fn is_halted(&self) -> bool {
        self.halted.load(Ordering::Acquire)
    }

    pub fn check(&self) -> Result<(), OrderSubmitError> {
        if self.is_halted() {
            Err(OrderSubmitError::Halted {
                code: OrderErrorCode::ExecutionHalted,
            })
        } else {
            Ok(())
        }
    }
}
```

- [ ] **Step 4: Implement atomic halt persistence and guarded submission**

`halt_uncertain` must set memory first, then atomically persist without including any payload or credential:

```rust
pub fn halt_uncertain(
    &self,
    planned: &PlannedOrder,
    reason: OrderErrorCode,
) -> Result<(), OrderSubmitError> {
    self.halt_uncertain_with(planned, reason, |temp, target| {
        temp.persist(target)
            .map(|_| ())
            .map_err(|error| error.error)
    })
}

fn halt_uncertain_with<F>(
    &self,
    planned: &PlannedOrder,
    reason: OrderErrorCode,
    persist: F,
) -> Result<(), OrderSubmitError>
where
    F: FnOnce(tempfile::NamedTempFile, &Path) -> std::io::Result<()>,
{
    self.halted.store(true, Ordering::Release);
    let marker = ExecutionHaltMarker {
        schema_version: 1,
        halted_at: Utc::now(),
        reason_code: format!("{reason:?}"),
        stage: "post_or_response".to_owned(),
        token_id: planned.token_id.clone(),
        side: format!("{:?}", planned.side).to_uppercase(),
        order_id_hint: None,
    };
    let parent = self.path.parent().filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut temp = tempfile::Builder::new()
        .prefix(".execution-halt-")
        .tempfile_in(parent)
        .map_err(|_| OrderSubmitError::Halted { code: OrderErrorCode::HaltMarkerIo })?;
    serde_json::to_writer_pretty(temp.as_file_mut(), &marker)
        .map_err(|_| OrderSubmitError::Halted { code: OrderErrorCode::HaltMarkerIo })?;
    temp.as_file_mut().flush()
        .map_err(|_| OrderSubmitError::Halted { code: OrderErrorCode::HaltMarkerIo })?;
    temp.as_file().sync_all()
        .map_err(|_| OrderSubmitError::Halted { code: OrderErrorCode::HaltMarkerIo })?;
    persist(temp, &self.path)
        .map_err(|_| OrderSubmitError::Halted { code: OrderErrorCode::HaltMarkerIo })
}

pub async fn submit_fok(
    &self,
    gateway: &dyn OrderGateway,
    planned: &PlannedOrder,
) -> Result<OrderReceipt, OrderSubmitError> {
    let _submission_guard = self.submit_lock.lock().await;
    self.check()?;
    match gateway.submit_fok(planned).await {
        Err(error @ OrderSubmitError::Uncertain { code }) => {
            self.halt_uncertain(planned, code)?;
            Err(error)
        }
        result => result,
    }
}
```

Do not add a marker-clearing function.

- [ ] **Step 5: Run breaker and gateway tests and verify GREEN**

```powershell
cargo test --offline --locked service::execution_circuit_breaker::tests
cargo test --offline --locked service::order_gateway::tests
```

Expected: all tests PASS; uncertainty calls the fake once, writes one marker, and blocks the second call.

- [ ] **Step 6: Commit Task 4**

```powershell
git add -- src/service/execution_circuit_breaker.rs src/service/mod.rs
git commit -m "feat: persist uncertain order execution halts"
```

---

### Task 5: Build and sign FOK orders through an isolated official SDK adapter

**Files:**
- Create/Test: `src/service/clob_sdk_orders.rs`
- Modify: `src/service/mod.rs`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`

**Interfaces:**
- Consumes: `AppConfig`, official SDK credentials already present in config, and neutral `PlannedOrder`.
- Produces: `SdkOrderGateway::new`, private `prepare_fok`, tick alignment, V2 amount extraction, and signed SDK orders; no POST implementation yet.

- [ ] **Step 1: Add failing pure mapping tests**

Create `clob_sdk_orders.rs` with tests for:

```rust
#[test]
fn buy_rounds_down_and_sell_rounds_up_without_worsening_limit() {
    let tick = dec!(0.01);
    assert_eq!(align_price(dec!(0.505), tick, Side::Buy).unwrap(), dec!(0.50));
    assert_eq!(align_price(dec!(0.505), tick, Side::Sell).unwrap(), dec!(0.51));
    assert_eq!(align_price(dec!(0.50), tick, Side::Buy).unwrap(), dec!(0.50));
    assert_eq!(align_price(dec!(0.50), tick, Side::Sell).unwrap(), dec!(0.50));
}

#[test]
fn invalid_tick_price_size_and_token_are_preflight_errors() {
    assert!(matches!(
        align_price(dec!(0.5), Decimal::ZERO, Side::Buy),
        Err(OrderSubmitError::Preflight { code: OrderErrorCode::InvalidTickSize, .. })
    ));
    assert!(decimal_from_f64(f64::NAN, OrderErrorCode::InvalidPrice).is_err());
    assert!(parse_token_id("not-a-u256").is_err());
}

#[test]
fn side_aware_amount_mapping_is_exact() {
    assert_eq!(map_amounts(Side::Buy, 20_000_000, 40_000_000), (40_000_000, 20_000_000));
    assert_eq!(map_amounts(Side::Sell, 40_000_000, 20_000_000), (40_000_000, 20_000_000));
}
```

Here `map_amounts(side, making_micros, taking_micros)` returns `(shares_micros, usd_micros)`.

- [ ] **Step 2: Run mapping tests and verify RED**

```powershell
cargo test --offline --locked service::clob_sdk_orders::tests::buy_rounds_down_and_sell_rounds_up_without_worsening_limit
```

Expected: compile failure because the adapter helpers do not exist.

- [ ] **Step 3: Implement pure conversion and alignment helpers**

Use SDK-reexported `Decimal` and `U256`; do not use `f64` for tick arithmetic:

```rust
fn preflight(stage: OrderStage, code: OrderErrorCode) -> OrderSubmitError {
    OrderSubmitError::Preflight { stage, code }
}

fn decimal_from_f64(value: f64, code: OrderErrorCode) -> Result<Decimal, OrderSubmitError> {
    if !value.is_finite() {
        return Err(preflight(OrderStage::Build, code));
    }
    Decimal::from_str(&value.to_string()).map_err(|_| preflight(OrderStage::Build, code))
}

fn parse_token_id(value: &str) -> Result<U256, OrderSubmitError> {
    U256::from_str(value).map_err(|_| preflight(OrderStage::Build, OrderErrorCode::InvalidTokenId))
}

fn align_price(price: Decimal, tick: Decimal, side: Side) -> Result<Decimal, OrderSubmitError> {
    if tick <= Decimal::ZERO {
        return Err(preflight(OrderStage::Metadata, OrderErrorCode::InvalidTickSize));
    }
    let remainder = price % tick;
    let aligned = if remainder.is_zero() {
        price
    } else {
        match side {
            Side::Buy => price - remainder,
            Side::Sell => price + (tick - remainder),
        }
    };
    if aligned <= Decimal::ZERO || aligned >= Decimal::ONE {
        return Err(preflight(OrderStage::Build, OrderErrorCode::InvalidPrice));
    }
    Ok(aligned.normalize())
}

fn decimal_to_micros(value: Decimal) -> Result<u128, OrderSubmitError> {
    let mut value = value.normalize();
    if value.is_sign_negative() || value.scale() > 6 {
        return Err(preflight(OrderStage::Response, OrderErrorCode::AmountConversion));
    }
    value.rescale(6);
    value.mantissa().try_into()
        .map_err(|_| preflight(OrderStage::Response, OrderErrorCode::AmountConversion))
}

fn u256_micros_to_decimal(value: U256) -> Result<Decimal, OrderSubmitError> {
    let raw: u128 = value.try_into()
        .map_err(|_| preflight(OrderStage::Build, OrderErrorCode::AmountConversion))?;
    let raw: i128 = raw.try_into()
        .map_err(|_| preflight(OrderStage::Build, OrderErrorCode::AmountConversion))?;
    Ok(Decimal::from_i128_with_scale(raw, 6).normalize())
}

fn map_amounts(side: Side, making: u128, taking: u128) -> (u128, u128) {
    match side {
        Side::Buy => (taking, making),
        Side::Sell => (making, taking),
    }
}
```

The helpers must not log their input on error.

- [ ] **Step 4: Add failing loopback build/sign test**

Use the public Hardhat fixture key already used in `clob_auth.rs`, UUID nil credentials, and a local TCP server that returns, in order:

```text
GET /tick-size?token_id=12345 -> {"minimum_tick_size":"0.01"}
GET /neg-risk?token_id=12345 -> {"neg_risk":false}
GET /version -> {"version":2}
```

The test must call `prepare_fok` and assert:

```rust
assert_eq!(prepared.signed.order_type, SdkOrderType::FOK);
assert_eq!(prepared.signed.payload.version(), 2);
assert_eq!(prepared.signed.order().tokenId, U256::from(12_345));
assert_eq!(prepared.signed.order().side, SdkSide::Buy as u8);
assert_eq!(prepared.expected_making, dec!(19.5));
assert_eq!(prepared.expected_taking, dec!(39));
assert_eq!(captured_paths, vec!["/tick-size", "/neg-risk", "/version"]);
assert_eq!(l1_auth_request_count, 0);
```

Use a plan with `shares = 39.0`, `limit_price = 0.505`; BUY alignment makes the SDK order price `0.50` and expected USD `19.5`.

Add a second test where loopback returns `{"neg_risk":true}` for a `planned.neg_risk = false` order. Assert `NegRiskMismatch` and that `/version` is never requested.

- [ ] **Step 5: Implement authenticated client construction with supplied credentials only**

Define the adapter and private test seam:

```rust
type AuthenticatedClient = Client<Authenticated<Normal>>;

pub struct SdkOrderGateway {
    client: AuthenticatedClient,
    signer: LocalSigner,
    post_timeout: Duration,
}

struct PreparedOrder {
    signed: SdkSignedOrder,
    expected_making: Decimal,
    expected_taking: Decimal,
    side: Side,
}

impl SdkOrderGateway {
    pub async fn new(cfg: &AppConfig) -> Result<Self, OrderSubmitError> {
        if cfg.site.clob_api_base != OFFICIAL_CLOB_V2_HOST {
            return Err(preflight(OrderStage::Initialization, OrderErrorCode::InvalidHost));
        }
        Self::new_with_host(cfg, &cfg.site.clob_api_base, Duration::from_secs(15)).await
    }

    async fn new_with_host(
        cfg: &AppConfig,
        host: &str,
        post_timeout: Duration,
    ) -> Result<Self, OrderSubmitError> {
        if cfg.exchange.chain_id != POLYGON {
            return Err(preflight(OrderStage::Initialization, OrderErrorCode::InvalidChain));
        }
        if cfg.credentials.signature_type != Some(0) {
            return Err(preflight(OrderStage::Initialization, OrderErrorCode::UnsupportedSignatureType));
        }
        let signer = LocalSigner::from_str(cfg.credentials.private_key.trim())
            .map_err(|_| preflight(OrderStage::Initialization, OrderErrorCode::MissingCredentials))?
            .with_chain_id(Some(POLYGON));
        let funder = Address::from_str(&cfg.credentials.funder_address)
            .map_err(|_| preflight(OrderStage::Initialization, OrderErrorCode::FunderMismatch))?;
        if signer.address() != funder {
            return Err(preflight(OrderStage::Initialization, OrderErrorCode::FunderMismatch));
        }
        let key = cfg.credentials.api_key.as_deref()
            .ok_or_else(|| preflight(OrderStage::Initialization, OrderErrorCode::MissingCredentials))?;
        let key = Uuid::parse_str(key)
            .map_err(|_| preflight(OrderStage::Initialization, OrderErrorCode::MissingCredentials))?;
        let secret = cfg.credentials.api_secret.clone()
            .filter(|v| !v.trim().is_empty())
            .ok_or_else(|| preflight(OrderStage::Initialization, OrderErrorCode::MissingCredentials))?;
        let passphrase = cfg.credentials.api_passphrase.clone()
            .filter(|v| !v.trim().is_empty())
            .ok_or_else(|| preflight(OrderStage::Initialization, OrderErrorCode::MissingCredentials))?;
        let credentials = Credentials::new(key, secret, passphrase);
        let client = Client::new(host, SdkConfig::default())
            .map_err(|_| preflight(OrderStage::Initialization, OrderErrorCode::SdkBuild))?
            .authentication_builder(&signer)
            .credentials(credentials)
            .authenticate()
            .await
            .map_err(|_| preflight(OrderStage::Initialization, OrderErrorCode::SdkBuild))?;
        Ok(Self { client, signer, post_timeout })
    }
}
```

Only tests may call `new_with_host`; production calls `new` and therefore enforces the exact official host. Authentication with supplied `Credentials` must not call Create, Derive, or Create-or-Derive.

- [ ] **Step 6: Implement explicit metadata, build, and sign stages**

`prepare_fok` must:

```rust
async fn prepare_fok(&self, planned: &PlannedOrder) -> Result<PreparedOrder, OrderSubmitError> {
    if planned.order_type != OrderType::Fok {
        return Err(preflight(OrderStage::Build, OrderErrorCode::SdkBuild));
    }
    let token_id = parse_token_id(&planned.token_id)?;
    let tick = self.client.tick_size(token_id).await
        .map_err(|_| preflight(OrderStage::Metadata, OrderErrorCode::MetadataLookupFailed))?
        .minimum_tick_size
        .as_decimal();
    let sdk_neg_risk = self.client.neg_risk(token_id).await
        .map_err(|_| preflight(OrderStage::Metadata, OrderErrorCode::MetadataLookupFailed))?
        .neg_risk;
    if sdk_neg_risk != planned.neg_risk {
        return Err(preflight(OrderStage::Metadata, OrderErrorCode::NegRiskMismatch));
    }
    let price = align_price(
        decimal_from_f64(planned.limit_price, OrderErrorCode::InvalidPrice)?,
        tick,
        planned.side,
    )?;
    let size = decimal_from_f64(planned.shares, OrderErrorCode::InvalidSize)?;
    let sdk_side = match planned.side { Side::Buy => SdkSide::Buy, Side::Sell => SdkSide::Sell };
    let signable = self.client.limit_order()
        .token_id(token_id)
        .side(sdk_side)
        .price(price)
        .size(size)
        .order_type(SdkOrderType::FOK)
        .build()
        .await
        .map_err(|_| preflight(OrderStage::Build, OrderErrorCode::SdkBuild))?;
    if signable.payload.version() != 2 {
        return Err(preflight(OrderStage::Build, OrderErrorCode::UnsupportedProtocolVersion));
    }
    let order = signable.payload.as_v2()
        .ok_or_else(|| preflight(OrderStage::Build, OrderErrorCode::UnsupportedProtocolVersion))?;
    let expected_making = u256_micros_to_decimal(order.makerAmount)?;
    let expected_taking = u256_micros_to_decimal(order.takerAmount)?;
    let signed = self.client.sign(&self.signer, signable).await
        .map_err(|_| preflight(OrderStage::Sign, OrderErrorCode::SdkSign))?;
    Ok(PreparedOrder { signed, expected_making, expected_taking, side: planned.side })
}
```

Do not call `build_sign_and_post`.

- [ ] **Step 7: Add SDK signature recovery to the loopback test**

Add a dev-only alias compatible with the SDK's Alloy generation:

```toml
[dev-dependencies]
alloy-sol-types-v1 = { package = "alloy-sol-types", version = "=1.6.1" }
```

In the test, obtain the V2 exchange from `polymarket_client_sdk_v2::contract_config(POLYGON, false)`, construct domain name `Polymarket CTF Exchange`, version `2`, chain 137, and recover the fixture signer from the EIP-712 prehash using `alloy_sol_types_v1::SolStruct`. Assert the recovered address equals `signer.address()`. Never print the signature.

```rust
use alloy_sol_types_v1::{SolStruct as _, eip712_domain};
use polymarket_client_sdk_v2::clob::types::OrderSignature;
use polymarket_client_sdk_v2::contract_config;

let exchange = contract_config(POLYGON, false)
    .unwrap()
    .exchange_v2
    .unwrap();
let domain = eip712_domain! {
    name: "Polymarket CTF Exchange",
    version: "2",
    chain_id: POLYGON,
    verifying_contract: exchange,
};
let digest = prepared.signed.order().eip712_signing_hash(&domain);
let signature = match &prepared.signed.signature {
    OrderSignature::Ecdsa(signature) => signature,
    OrderSignature::Wrapped(_) => panic!("EOA order must use ECDSA"),
    _ => panic!("unsupported future signature type for EOA test"),
};
assert_eq!(
    signature.recover_address_from_prehash(&digest).unwrap(),
    signer.address()
);
```

- [ ] **Step 8: Run adapter preparation tests and verify GREEN**

```powershell
cargo test --offline --locked service::clob_sdk_orders::tests::buy_rounds_down_and_sell_rounds_up_without_worsening_limit
cargo test --offline --locked service::clob_sdk_orders::tests::official_sdk_builds_and_signs_v2_eoa_fok_on_loopback
cargo test --offline --locked service::clob_sdk_orders::tests::neg_risk_mismatch_stops_before_build
```

Expected: all tests PASS, no L1 request occurs, and the captured hosts are loopback only.

- [ ] **Step 9: Commit Task 5**

```powershell
git add -- Cargo.toml Cargo.lock src/service/clob_sdk_orders.rs src/service/mod.rs
git commit -m "feat: prepare FOK orders with official SDK"
```

---

### Task 6: Post once, classify SDK responses, and return exact neutral receipts

**Files:**
- Modify/Test: `src/service/clob_sdk_orders.rs`

**Interfaces:**
- Consumes: `PreparedOrder` from Task 5 and official SDK `PostOrderResponse`/`Error`.
- Produces: the only production `OrderGateway` implementation, with one explicit post attempt and sanitized classification.

- [ ] **Step 1: Write failing response-classification unit tests**

Create a pure `classify_response` seam and tests covering this exact matrix:

```rust
fn response(
    success: bool,
    status: OrderStatusType,
    order_id: &str,
    making_amount: Decimal,
    taking_amount: Decimal,
) -> PostOrderResponse {
    PostOrderResponse::builder()
        .success(success)
        .status(status)
        .order_id(order_id)
        .making_amount(making_amount)
        .taking_amount(taking_amount)
        .build()
}

#[test]
fn exact_matched_buy_returns_actual_side_aware_receipt() {
    let response = response(true, OrderStatusType::Matched, "0xabc", dec!(19.5), dec!(39));
    let receipt = classify_response(
        response,
        dec!(19.5),
        dec!(39),
        Side::Buy,
    ).unwrap();
    assert_eq!(receipt.filled_shares_micros, 39_000_000);
    assert_eq!(receipt.filled_usd_micros, 19_500_000);
}

#[test]
fn success_false_is_rejected_but_successful_non_final_or_mismatch_is_uncertain() {
    let rejected = classify_response(
        response(false, OrderStatusType::Canceled, "", Decimal::ZERO, Decimal::ZERO),
        dec!(19.5), dec!(39), Side::Buy,
    ).unwrap_err();
    assert!(matches!(rejected, OrderSubmitError::Rejected { code: OrderErrorCode::ServerRejected, .. }));

    for response in [
        response(true, OrderStatusType::Live, "0xabc", dec!(19.5), dec!(39)),
        response(true, OrderStatusType::Matched, "", dec!(19.5), dec!(39)),
        response(true, OrderStatusType::Matched, "0xabc", dec!(19.4), dec!(39)),
    ] {
        assert!(matches!(
            classify_response(response, dec!(19.5), dec!(39), Side::Buy),
            Err(OrderSubmitError::Uncertain { .. })
        ));
    }
}
```

Also cover `Delayed`, `Unmatched`, `Unknown`, zero amounts, SELL mapping, excess decimal precision, and micro-unit overflow.

- [ ] **Step 2: Run classification tests and verify RED**

```powershell
cargo test --offline --locked service::clob_sdk_orders::tests::exact_matched_buy_returns_actual_side_aware_receipt
```

Expected: compile failure because `classify_response` does not exist.

- [ ] **Step 3: Implement fail-closed response classification**

Implement in this order so `success = false` never becomes an uncertainty merely because its amounts are empty:

```rust
fn classify_response(
    response: PostOrderResponse,
    expected_making: Decimal,
    expected_taking: Decimal,
    side: Side,
) -> Result<OrderReceipt, OrderSubmitError> {
    if !response.success {
        return Err(OrderSubmitError::Rejected {
            http_status: None,
            code: OrderErrorCode::ServerRejected,
        });
    }
    if response.order_id.trim().is_empty() {
        return Err(OrderSubmitError::Uncertain { code: OrderErrorCode::EmptyOrderId });
    }
    if response.status != OrderStatusType::Matched {
        return Err(OrderSubmitError::Uncertain { code: OrderErrorCode::NonFinalStatus });
    }
    if response.making_amount <= Decimal::ZERO
        || response.taking_amount <= Decimal::ZERO
        || response.making_amount != expected_making
        || response.taking_amount != expected_taking
    {
        return Err(OrderSubmitError::Uncertain { code: OrderErrorCode::AmountMismatch });
    }
    let making = decimal_to_micros(response.making_amount)
        .map_err(|_| OrderSubmitError::Uncertain { code: OrderErrorCode::AmountConversion })?;
    let taking = decimal_to_micros(response.taking_amount)
        .map_err(|_| OrderSubmitError::Uncertain { code: OrderErrorCode::AmountConversion })?;
    let (filled_shares_micros, filled_usd_micros) = map_amounts(side, making, taking);
    Ok(OrderReceipt {
        order_id: response.order_id,
        filled_shares_micros,
        filled_usd_micros,
    })
}
```

Because `OrderStatusType` is non-exhaustive, any future successful non-`Matched` value follows the uncertainty branch.

- [ ] **Step 4: Write failing loopback post/error/redaction tests**

Use a scripted loopback server that serves Task 5's three metadata/version responses and then one `/order` response. Add tests for:

```text
200 exact Matched -> receipt, one POST
200 success=false -> Rejected, one POST
400/409/429/500 with SERVER_BODY_SECRET_SENTINEL -> Rejected with status, sentinel absent
200 malformed/truncated JSON -> Uncertain(MalformedResponse), one POST
disconnect after request bytes -> Uncertain(PostTransport), one POST
server accepts request but withholds response beyond injected 25ms -> Uncertain(PostTimeout), one POST
200 success=true Live/Delayed/amount mismatch -> Uncertain, one POST
```

For every test, capture requests and assert there is exactly one `POST /order`, `orderType` is `FOK`, `owner` is UUID nil, `POLY_ADDRESS` is the fixture signer, and L1 `/auth/api-key` and `/auth/derive-api-key` counts are zero. Sentinel assertions must use only rendered local errors; never print the raw body.

- [ ] **Step 5: Implement one-shot post classification and the trait**

Inspect status without formatting or attaching the SDK error:

```rust
fn classify_post_error(error: &SdkError) -> OrderSubmitError {
    if let Some(status) = error.downcast_ref::<SdkStatus>() {
        return OrderSubmitError::Rejected {
            http_status: Some(status.status_code.as_u16()),
            code: OrderErrorCode::HttpRejected,
        };
    }
    let code = match error.downcast_ref::<reqwest::Error>() {
        Some(source) if source.is_decode() => OrderErrorCode::MalformedResponse,
        _ => OrderErrorCode::PostTransport,
    };
    OrderSubmitError::Uncertain { code }
}

#[async_trait]
impl OrderGateway for SdkOrderGateway {
    async fn submit_fok(
        &self,
        planned: &PlannedOrder,
    ) -> Result<OrderReceipt, OrderSubmitError> {
        let prepared = self.prepare_fok(planned).await?;
        let response = tokio::time::timeout(
            self.post_timeout,
            self.client.post_order(prepared.signed),
        )
        .await
        .map_err(|_| OrderSubmitError::Uncertain { code: OrderErrorCode::PostTimeout })?
        .map_err(|error| classify_post_error(&error))?;
        classify_response(
            response,
            prepared.expected_making,
            prepared.expected_taking,
            prepared.side,
        )
    }
}
```

Do not retry after version invalidation or any other error. Never call `build_sign_and_post`.

- [ ] **Step 6: Run the full SDK adapter module and verify GREEN**

```powershell
cargo test --offline --locked service::clob_sdk_orders::tests
```

Expected: all pure and loopback tests PASS; each uncertain case has exactly one POST and no response-body sentinel appears in output.

- [ ] **Step 7: Commit Task 6**

```powershell
git add -- src/service/clob_sdk_orders.rs
git commit -m "feat: submit and classify official SDK FOK orders"
```

---

### Task 7: Migrate OrderExecutor and strict dry-run to the neutral live runtime

**Files:**
- Modify/Test: `src/service/order_executor.rs`

**Interfaces:**
- Consumes: `SdkOrderGateway`, `ExecutionCircuitBreaker`, `OrderGateway`, and `OrderReceipt`.
- Produces: async `OrderExecutor::new`, `live_order_components`, `ExecutionOutcome::NotSubmitted`, and `ExecutionOutcome::Filled`.

- [ ] **Step 1: Write failing fake-gateway execution tests**

Add a test-only constructor:

```rust
fn new_with_live_components(
    cfg: AppConfig,
    risk: Arc<RiskGuard>,
    markets: Arc<MarketCache>,
    positions: Arc<PositionStore>,
    gateway: Option<Arc<dyn OrderGateway>>,
    breaker: Option<Arc<ExecutionCircuitBreaker>>,
) -> Self
```

Write tests asserting:

```rust
// Strict dry-run: CLOB host is an unreachable loopback address, credentials are empty,
// constructor succeeds, gateway is never called, and planned position is recorded.
assert!(executor.live_order_components().is_none());
assert!(matches!(outcome, ExecutionOutcome::DryRunPlanned(_)));

// Confirmed live receipt uses actual values, not planned values.
assert_eq!(position.shares, 39.0);
assert_eq!(position.usd_notional, 19.5);
assert_eq!(position.entry_price, 0.5);

// Preflight and Rejected produce NotSubmitted and no position.
assert!(matches!(outcome, ExecutionOutcome::NotSubmitted(_)));

// Uncertain writes the marker, returns an error, opens no position, and a second
// execution never calls the gateway.
```

The fake receipt for the actual-accounting test is `39_000_000` shares and `19_500_000` USD. The share count remains a complete FOK fill, while the actual USD differs from the strategy's original `20.0` estimate because the SDK-aligned BUY price is `0.50` rather than the planned `0.505` limit.

- [ ] **Step 2: Run executor tests and verify RED**

```powershell
cargo test --offline --locked service::order_executor::tests
```

Expected: compile failures for the new constructor/outcome API and at least one behavioral failure because live positions still use planned amounts.

- [ ] **Step 3: Replace `ClobClient` fields and outcomes**

Use these fields and outcome variants:

```rust
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
    DryRunPlanned(PlannedOrder),
    NotSubmitted(OrderSubmitError),
    Filled(OrderReceipt),
}
```

Delete `DryRun(SignedOrder)` and `Submitted { signed, ... }`.

- [ ] **Step 4: Make construction strict and asynchronous**

```rust
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
    let breaker = ExecutionCircuitBreaker::new_live(
        cfg.trading.execution_halt_path.clone(),
    )?;
    let gateway: Arc<dyn OrderGateway> = Arc::new(SdkOrderGateway::new(&cfg).await?);
    Ok(Self::new_with_live_components(
        cfg, risk, markets, positions, Some(gateway), Some(breaker),
    ))
}

pub fn live_order_components(
    &self,
) -> Option<(Arc<dyn OrderGateway>, Arc<ExecutionCircuitBreaker>)> {
    Some((self.gateway.as_ref()?.clone(), self.breaker.as_ref()?.clone()))
}
```

Live initialization errors must propagate; never fall back to dry-run.

- [ ] **Step 5: Route execution and accounting through the guarded gateway**

Every phase-2 plan uses `OrderType::Fok`. After planning:

```rust
if !self.cfg.live_trading_allowed() {
    self.record_open_from_plan(&market, &planned);
    return Ok(ExecutionOutcome::DryRunPlanned(planned));
}
let gateway = self.gateway.as_ref().ok_or_else(|| anyhow!("live gateway unavailable"))?;
let breaker = self.breaker.as_ref().ok_or_else(|| anyhow!("live breaker unavailable"))?;
match breaker.submit_fok(gateway.as_ref(), &planned).await {
    Ok(receipt) => {
        self.record_open_from_receipt(&market, &planned, &receipt);
        Ok(ExecutionOutcome::Filled(receipt))
    }
    Err(error @ (OrderSubmitError::Preflight { .. } | OrderSubmitError::Rejected { .. })) => {
        Ok(ExecutionOutcome::NotSubmitted(error))
    }
    Err(error) => Err(anyhow::Error::new(error)),
}
```

Keep paper accounting in `record_open_from_plan`. Live accounting must set:

```rust
let shares = receipt.filled_shares();
let usd_notional = receipt.filled_usd();
let entry_price = usd_notional / shares;
```

Reject zero values before creating an `OpenPosition`; a zero receipt should already be impossible after Task 6.

- [ ] **Step 6: Run executor tests and verify GREEN**

```powershell
cargo test --offline --locked service::order_executor::tests
```

Expected: dry-run replay still passes without credentials/signing; live fake receipts use actual amounts; reject/preflight/uncertain cases never open a position.

- [ ] **Step 7: Commit Task 7**

```powershell
git add -- src/service/order_executor.rs
git commit -m "feat: route entries through guarded SDK gateway"
```

---

### Task 8: Migrate TP/SL exits and bot wiring to the same gateway and breaker

**Files:**
- Modify/Test: `src/service/position_monitor.rs`
- Modify/Test: `src/bot/copy_trading.rs`

**Interfaces:**
- Consumes: `OrderExecutor::live_order_components`, shared `OrderGateway`, shared breaker, and live-only midpoint source.
- Produces: live-only TP/SL spawn and exact FOK exit outcomes with no local close on rejected/uncertain orders.

- [ ] **Step 1: Write failing TP/SL fake-gateway tests**

Expose a testable single-tick result:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
enum ExitOutcome {
    NoTrigger,
    Rejected(OrderSubmitError),
    Filled(OrderReceipt),
}
```

Add async tests:

```rust
// Exact matched SELL receipt closes the position.
assert!(matches!(outcome, ExitOutcome::Filled(_)));
assert!(positions.get("t").is_none());

// Rejected exit keeps the position and breaker open.
assert!(matches!(outcome, ExitOutcome::Rejected(_)));
assert!(positions.get("t").is_some());
assert!(!breaker.is_halted());

// Uncertain exit keeps the position, writes marker, and blocks a later entry fake.
assert!(result.is_err());
assert!(positions.get("t").is_some());
assert!(breaker.is_halted());
```

Assert the submitted exit plan is `Side::Sell`, `OrderType::Fok`, and exactly the open position's shares.

- [ ] **Step 2: Run TP/SL tests and verify RED**

```powershell
cargo test --offline --locked service::position_monitor::tests
```

Expected: compile failures because the monitor still requires `ClobClient` and closes on any successful POST wrapper result.

- [ ] **Step 3: Replace the monitor's live dependencies and submission logic**

Change `spawn` and `monitor_once` to consume:

```rust
gateway: Arc<dyn OrderGateway>,
breaker: Arc<ExecutionCircuitBreaker>,
midprice: Arc<dyn MidpriceSource>,
price_buffer: f64,
```

Remove `live_trading` and `order_expiration_secs`. Submit through:

```rust
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
```

No dry-run monitor branch exists. An uncertain exit never closes or reduces the local position.

In the spawned monitor, ordinary midpoint errors and explicit rejections may continue to the next tick, but an execution uncertainty/halt must terminate the monitor task:

```rust
if let Err(error) = monitor_once(
    &pos,
    &positions,
    gateway.as_ref(),
    breaker.as_ref(),
    midprice.as_ref(),
    price_buffer,
).await {
    let fatal = error
        .downcast_ref::<OrderSubmitError>()
        .is_some_and(|order_error| matches!(
            order_error,
            OrderSubmitError::Uncertain { .. } | OrderSubmitError::Halted { .. }
        ));
    if fatal {
        error!(token = %pos.token_id, "TP/SL execution halted; monitor stopping");
        return;
    }
    warn!(token = %pos.token_id, "TP/SL tick failed before order submission");
}
```

Do not format the full error in either log branch.

- [ ] **Step 4: Write failing bot wiring test/helper**

Extract this pure decision and test it:

```rust
fn live_tp_sl_components(
    cfg: &AppConfig,
    executor: &OrderExecutor,
) -> Option<(Arc<dyn OrderGateway>, Arc<ExecutionCircuitBreaker>)> {
    if !cfg.tp_sl.enabled || !cfg.live_trading_allowed() {
        return None;
    }
    executor.live_order_components()
}
```

The test must assert strict dry-run returns `None` even when TP/SL is enabled. This proves copy startup will not construct `ClobMidpriceSource` in dry-run.

- [ ] **Step 5: Update bot construction, spawning, and outcome logs**

Await constructor:

```rust
let executor = OrderExecutor::new(
    cfg.clone(),
    Arc::clone(&risk),
    Arc::clone(&markets),
    Arc::clone(&positions),
).await?;
```

Only inside `if let Some((gateway, breaker)) = live_tp_sl_components(...)` construct `ClobMidpriceSource` and call `position_monitor::spawn`. In dry-run, log that TP/SL CLOB monitoring is inactive; do not warn about missing credentials.

Update `handle_log` to match only:

```rust
ExecutionOutcome::Skipped(reason)
ExecutionOutcome::DryRunPlanned(planned)
ExecutionOutcome::NotSubmitted(error)
ExecutionOutcome::Filled(receipt)
```

Log `error.code()` and optional safe status by pattern matching; do not use `?error` on SDK-originated data. Redact the receipt order ID before logging with a local prefix/suffix helper.

Change the main receive loop so an uncertain/halted order error terminates copy execution after the breaker has persisted its marker; non-order errors retain the existing logged-and-continue behavior:

```rust
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
```

Do not log the full `anyhow::Error`, because it may originate in an unrelated HTTP source.

- [ ] **Step 6: Run monitor, bot, and replay tests and verify GREEN**

```powershell
cargo test --offline --locked service::position_monitor::tests
cargo test --offline --locked service::order_executor::tests
cargo test --offline --locked bot::copy_trading
```

Expected: all tests PASS; dry-run has no CLOB midpoint/SDK path and entries/exits share the same breaker in live fixtures.

- [ ] **Step 7: Commit Task 8**

```powershell
git add -- src/service/position_monitor.rs src/bot/copy_trading.rs
git commit -m "feat: share guarded SDK execution with TP SL"
```

---

### Task 9: Remove the custom production backend and document the phase-2 boundary

**Files:**
- Delete: `src/service/clob.rs`
- Modify: `src/service/mod.rs`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `README.md`
- Modify: `README.zh-CN.md`
- Modify: `config.yaml.example`

**Interfaces:**
- Consumes: all migrated callers and official SDK loopback coverage from Tasks 5–8.
- Produces: one production order backend, corrected user documentation, and no runtime manual signing/HMAC/POST entry point.

- [ ] **Step 1: Prove all production callers have left the old backend**

```powershell
rg -n "ClobClient|SignedOrder|OrderPostBody|build_signed_order|l2_headers|hmac_sha256|serialize_order_request|service::clob" src --glob '!src/service/clob.rs'
```

Expected: no matches. If a match remains, migrate that exact caller to `OrderGateway` before deleting anything.

- [ ] **Step 2: Delete the backend and remove its module export**

Delete `src/service/clob.rs` and remove `pub mod clob;`. Update service/bot module comments to describe official SDK FOK execution and strict paper mode.

Audit direct dependencies after deletion:

```powershell
rg -n "base64::|alloy_primitives_v1|alloy_dyn_abi" src
```

If the command has no matches, remove direct `base64`, `alloy-primitives-v1`, and `alloy-dyn-abi` entries. Keep `alloy-primitives = 0.8` and `alloy-sol-types = 0.8` because `parse.rs` uses them. Keep the exact Alloy signer pins required by the already-verified SDK dependency graph.

- [ ] **Step 3: Run compile and old-backend absence checks**

```powershell
cargo test --offline --locked --no-run
rg -n "pub mod clob;|ClobClient|build_signed_order|l2_headers|hmac_sha256_base64url|serialize_order_request" src
```

Expected: compile succeeds and the absence scan has no matches. `clob_auth` and `clob_sdk_orders` names are allowed.

- [ ] **Step 4: Update English and Chinese documentation**

Document these exact facts in both README files:

- local order execution now uses official Rust V2 SDK 0.6 for build/sign/L2/POST;
- copy entries and TP/SL exits are true FOK;
- dry-run does not sign and does not call any CLOB endpoint, including midpoint;
- only exact fully matched responses update local positions;
- uncertainty writes `execution-halt.json`, blocks all later entries/exits, and is never retried;
- the marker must not be deleted before external reconciliation;
- phase 2 used loopback only and still does not authorize live trading;
- balance, allowance, reconciliation, cancellation, in-flight journaling, and controlled real-endpoint validation remain phase 3.

Replace the local status paragraphs that still say “existing raw V2 order path.” Correct copy-trading-specific `FAK/GTD` claims to `FOK`; do not rewrite unrelated strategy marketing that genuinely describes FAK.

Update `config.yaml.example` comments from “phase 1” to “current EOA-only SDK phases” without adding any credential value.

- [ ] **Step 5: Run documentation and safety scans**

```powershell
rg -n "existing raw V2 order path|现有 V2 原始订单路径|dry-run.*signed|空跑.*签名" README.md README.zh-CN.md src
rg -n '"enable_trading"\s*:\s*true|"mock_trading"\s*:\s*false|api_key:\s*"[^"]+"|api_secret:\s*"[^"]+"|api_passphrase:\s*"[^"]+"' config.json config.dryrun-public.json config.yaml.example
```

Expected: obsolete local status claims and permissive/credential values have no matches.

- [ ] **Step 6: Commit Task 9**

```powershell
git add -- Cargo.toml Cargo.lock src/service/clob.rs src/service/mod.rs README.md README.zh-CN.md config.yaml.example
git commit -m "refactor: remove custom CLOB order backend"
```

---

### Task 10: Run final offline verification, independent review, and durable project recording

**Files:**
- Modify: `docs/superpowers/plans/2026-08-18-official-sdk-order-migration.md` (checkboxes and actual verification results only)
- Update outside Git: `C:\Users\Haozi\Documents\记忆库\20-Prediction-Markets-Trading-Bot-Toolkits.md`
- Update outside Git: `C:\Users\Haozi\Documents\记忆库\05-项目索引.md`

**Interfaces:**
- Consumes: complete phase-2 branch.
- Produces: verified branch evidence, credential-free Obsidian status, and a user choice for branch completion.

- [ ] **Step 1: Format only changed Rust files**

List changed Rust files, inspect the exact paths, then run `rustfmt --edition 2021` only on those files. The expected set is:

```powershell
rustfmt --edition 2021 src/models.rs src/config.rs src/service/order_gateway.rs src/service/execution_circuit_breaker.rs src/service/clob_sdk_orders.rs src/service/order_executor.rs src/service/position_monitor.rs src/service/mod.rs src/bot/copy_trading.rs
```

Do not format untouched files.

- [ ] **Step 2: Run focused safety-critical tests**

```powershell
cargo test --offline --locked service::order_gateway::tests
cargo test --offline --locked service::execution_circuit_breaker::tests
cargo test --offline --locked service::clob_sdk_orders::tests
cargo test --offline --locked service::order_executor::tests
cargo test --offline --locked service::position_monitor::tests
```

Expected: all PASS with zero failures and no public network access.

- [ ] **Step 3: Run the full locked offline suite and release gates**

```powershell
cargo test --all-targets --offline --locked
cargo build --release --offline --locked
cargo clippy --all-targets --offline --locked -- -D warnings
```

Expected: all commands PASS.

- [ ] **Step 4: Record the repository-wide formatting baseline**

```powershell
cargo fmt --check
```

If it fails only on the known untouched files, record the exact unchanged baseline and do not run whole-repository formatting. If a changed file appears, format only that file and rerun its focused tests.

- [ ] **Step 5: Run old-backend, retry, host, and secret scans**

```powershell
rg -n "ClobClient|build_signed_order|build_sign_and_post|l2_headers|hmac_sha256_base64url|serialize_order_request" src
rg -n "retry|will retry|create_or_derive|create-or-derive" src/service/clob_sdk_orders.rs src/service/order_executor.rs src/service/position_monitor.rs
rg -n 'https?://(?!127\.0\.0\.1|localhost)' src/service/clob_sdk_orders.rs
rg -n '"enable_trading"\s*:\s*true|"mock_trading"\s*:\s*false|api_key:\s*"[^"]+"|api_secret:\s*"[^"]+"|api_passphrase:\s*"[^"]+"' config.json config.dryrun-public.json config.yaml.example
git grep -n "SERVER_BODY_SECRET_SENTINEL"
```

Interpretation:

- old-backend scan: no matches;
- retry scan: only assertions/documentation saying no retry, never executable retry logic;
- host scan: production official-host constant and test loopback literals only; PowerShell `rg` may not support lookahead, so if the third command errors, run `rg -n "https?://" src/service/clob_sdk_orders.rs` and inspect every match manually;
- config scan: no matches;
- sentinel scan: test and implementation-plan literals only, never production output.

- [ ] **Step 6: Inspect dependency and diff state**

```powershell
cargo tree --offline --locked -i polymarket_client_sdk_v2
git diff --check
git status --short --branch
git log --oneline --decorate -12
```

Expected: official SDK is the sole order protocol implementation, diff check is clean, and only intentional plan-result/Obsidian work remains.

- [ ] **Step 7: Perform independent code review and address findings**

Use `superpowers:requesting-code-review` after all tests pass. Review specifically for:

```text
dry-run accidentally constructs SDK or midpoint client
any route around the shared breaker
position mutation before exact Matched receipt
post ambiguity classified as rejection
automatic repost/retry
marker persistence after rather than before in-memory halt
raw SDK/body/signature/credential leakage
remaining runtime references to custom clob.rs
```

For each accepted finding, add a failing regression test, implement the smallest fix, rerun focused and full gates, and commit the fix separately.

- [ ] **Step 8: Update Obsidian without credentials**

Record only:

- design and plan paths/commits;
- official SDK version and completed implementation commit list;
- final test count plus build/Clippy results;
- strict dry-run behavior;
- exact FOK success rule and persistent halt behavior;
- confirmation that no real auth/order endpoint or real credential was used;
- phase 3 remains required before any live-trading evaluation.

Never store fixture secrets, complete API keys, private keys, signatures, signed bodies, raw response bodies, or raw terminal output.

- [ ] **Step 9: Commit the completed plan record**

```powershell
git add -- docs/superpowers/plans/2026-08-18-official-sdk-order-migration.md
git commit -m "docs: close official SDK order migration plan"
```

- [ ] **Step 10: Use the finishing-development-branch workflow**

Invoke `superpowers:finishing-a-development-branch` and offer only its supported local merge, PR, keep-branch, or discard choices. Do not merge, push, delete, or discard without explicit user selection.

---

## Plan Self-Review Checklist

- [x] Every approved design requirement maps to a task: FOK naming (Task 1), halt config (Task 2), neutral contracts (Task 3), persistent shared breaker (Task 4), SDK build/sign (Task 5), SDK POST/classification (Task 6), entry accounting (Task 7), TP/SL and dry-run wiring (Task 8), old backend removal/docs (Task 9), verification/review/memory (Task 10).
- [x] SDK business types occur only in `clob_sdk_orders.rs`; neutral callers use `OrderGateway`, `OrderReceipt`, and typed sanitized errors.
- [x] Strict dry-run exits constructor setup before SDK authentication and before midpoint creation.
- [x] Supplied API credentials are used directly; phase 2 never calls Create, Derive, or Create-or-Derive.
- [x] All phase-2 orders map to SDK `OrderType::FOK`; no production FAK/GTD/GTC mapping remains in the copy/TP-SL path.
- [x] BUY prices round down and SELL prices round up to the SDK-reported tick.
- [x] Planned neg-risk must match the SDK response before build/sign/post.
- [x] Explicit build, sign, and post stages are used; `build_sign_and_post` is forbidden.
- [x] HTTP status errors are sanitized rejection; post transport/timeout/malformed success is uncertainty.
- [x] Only exact positive `Matched` making/taking values produce a receipt and position mutation.
- [x] Every uncertain post gets one attempt, no position mutation, in-memory halt first, atomic marker persistence, and global entry/exit blocking; the shared submission lock prevents an entry/exit race from posting a second order after the first uncertainty.
- [x] Marker persistence failure remains halted in memory and returns a fatal sanitized error; marker clearing is absent.
- [x] The process-crash gap before marker persistence remains documented as phase 3 work, so phase 2 does not authorize live trading.
- [x] All new network tests bind only to loopback and final gates run `--offline --locked`.
- [x] No placeholder instructions or undefined cross-task interfaces remain.
