# Phase 3A Durable Execution Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the post-before-marker crash gap, rebuild exact live positions from durable integer events, and provide exact-order, human-confirmed reconciliation/cancellation/recovery without contacting a public endpoint during implementation or acceptance.

**Architecture:** Add one append-only, hash-chained JSONL `ExecutionLedger` with an atomically replaced active-intent snapshot and exclusive process lock. The ledger owns the live order-lifecycle and position projection; strict paper mode keeps an isolated in-memory store. A narrow pre-POST journal callback makes the official SDK expose the proven V2 order hash, synchronizes `IntentPrepared` and `SubmitStarted`, and only then permits one POST. SDK-neutral recovery contracts allow exact-order read and single-order cancel only; `RecoveryService` enforces fresh, ledger-head-bound human challenges for apply, cancel, and acknowledge.

**Tech Stack:** Rust 2021, Tokio, Serde/Serde JSON, `sha2`, `uuid`, `fs2`, `tempfile`, Chrono, `async-trait`, Thiserror, official `polymarket_client_sdk_v2 = 0.6.0`, Alloy EIP-712 primitives, Clap, offline loopback tests.

**Spec:** `docs/superpowers/specs/2026-08-18-phase-3a-durable-recovery-design.md`

## Global Constraints

- Phase 3A is offline/loopback engineering acceptance only. It does not authorize live trading or any public CLOB, Gamma, Polygon, authentication, signing-broadcast, or order call.
- Keep later gates explicit and out of scope: Phase 3B covers pUSD/buying-power and allowance capability, Phase 3C covers separately authorized controlled real-endpoint acceptance, and Phase 3D covers a separately designed isolated-wallet micro-value live evaluation.
- The load-bearing first gate is the exact V2 order ID. Do not wire the ledger into the production submission path until Task 1 proves the local ID equals the official V2 EIP-712 `hashOrder` result.
- The official V2 contract fixes domain name `Polymarket CTF Exchange`, domain version `2`, and excludes the signature from `_createStructHash`. Keep the proof linked to the official `ctf-exchange-v2` sources.
- `IntentPrepared` must be durable before `SubmitStarted`; `SubmitStarted` must be flushed and synchronized immediately before the only POST invocation.
- Never retry or repost an uncertain submission. Startup and recovery inspection are local-only and perform no network operation.
- The JSONL ledger is authoritative. The active snapshot is only a synchronized safety mirror; missing or mismatched active state fails closed.
- Live position identity is `position_id` plus opening/closing order IDs, never token ID alone. All durable amounts are integer micro-units.
- Only exact, positive, fully matched FOK evidence may produce `PositionOpened` or `PositionClosed`; recovered matches additionally require explicit `apply` and then separate `acknowledge`.
- Cancellation is limited to one exact active ledger order. No neutral interface, CLI path, or runtime wrapper may expose batch, market, or account-wide cancellation.
- `prepare-cancel` performs a fresh exact query. `cancel` appends `CancelStarted` before one DELETE, records the sanitized response, and always follows it with exact reconciliation.
- A 404, timeout, disconnect, malformed response, unknown status, partial fill, field mismatch, or trade mismatch remains uncertain and cannot be acknowledged.
- Confirmation challenges bind action, intent ID, exact order ID, sequence, and ledger head hash. Any append invalidates the challenge. There is no `--force`, `--yes`, environment bypass, or hidden marker-clearing operation.
- Local-only `inspect`, `apply`, and `acknowledge` load public config and ledger only. Network `reconcile`, `prepare-cancel`, and `cancel` explicitly load credentials for that one invocation.
- Never log or persist a private key, complete API key, API secret, passphrase, signature, signed payload, HMAC/L2 header, raw request/response body, raw SDK error, or full order ID except explicit local `inspect --show-order-id` output.
- Strict paper mode returns before ledger path validation/open, lock acquisition, credentials, SDK construction, signing, CLOB midpoint construction, or recovery gateway construction.
- Keep committed configs at `enable_trading = false` and `mock_trading = true`.
- Use changed-file rustfmt only. Do not run whole-repository formatting, authentication commands, public-endpoint tests, or real order operations.
- Each task ends in a focused commit. Do not push, merge, or delete the branch without a separate explicit user choice.

---

### Task 1: Prove and expose the exact official V2 order ID

**Files:**
- Modify: `vendor/polymarket_client_sdk_v2/src/clob/types/mod.rs`
- Modify/Test: `src/service/clob_sdk_orders.rs`
- Reference: `vendor/polymarket_client_sdk_v2/src/clob/client.rs`

**Interfaces:**
- Produces: `SignedOrder::v2_order_hash(&Eip712Domain) -> Result<B256>` in the existing vendored SDK patch.
- Consumes: the same `OrderV2::eip712_signing_hash` and V2 domain already used by SDK signing.
- Invariant: the returned order ID depends on every identity-bearing V2 field and the EIP-712 domain, but not on credentials or signature bytes.

- [ ] **Step 1: Add a failing SDK helper test with an independent fixed vector**

In the application adapter test module, construct an SDK `OrderV2` and an independently declared Alloy 1.x mirror with these exact public values: Polygon chain `137`, verifying contract `0xE111180000d2663C0091e4f400237545B87B996B`, salt `1`, maker/signer `0x1111111111111111111111111111111111111111`, token `12345`, maker amount `19500000`, taker amount `39000000`, BUY `0`, EOA `0`, timestamp `1700000000000`, zero metadata, and zero builder. Assert the independently precomputed digest:

```rust
#[test]
fn v2_order_hash_matches_official_contract_algorithm() {
    let order = fixed_sdk_v2_order();
    let domain = eip712_domain! {
        name: "Polymarket CTF Exchange",
        version: "2",
        chain_id: 137,
        verifying_contract: address!("E111180000d2663C0091e4f400237545B87B996B"),
    };
    let expected = independent_v2_order().eip712_signing_hash(&domain);
    let actual = order.eip712_signing_hash(&domain);

    assert_eq!(actual, expected);
    assert_eq!(actual, b256!("dee0837cae29a8c41bd52f1f614e7e163739ff5ae52343da8f0501189c02e020"));
}
```

Keep the fixed field values beside the literal in the test. The digest was independently checked through the existing Alloy 1.6 `SolStruct::eip712_signing_hash` seam; the production helper must use the vendored SDK order type. Do not derive the hard-coded literal from the helper under test. A second async adapter test prepares a real SDK-signed loopback order, verifies `signed.v2_order_hash(domain) == signed.order().eip712_signing_hash(domain)`, changes only `signed.signature`, and verifies the helper result is unchanged.

- [ ] **Step 2: Run the focused test and verify RED**

```powershell
cargo test --offline --locked service::clob_sdk_orders::tests::v2_order_hash_matches_official_contract_algorithm
```

Expected: compile failure because `SignedOrder::v2_order_hash` does not exist.

- [ ] **Step 3: Add the narrow audited helper**

Add only this public helper; do not copy order serialization into application code:

```rust
impl SignedOrder {
    /// Returns the exact CTF Exchange V2 EIP-712 order hash used as the order ID.
    pub fn v2_order_hash(&self, domain: &Eip712Domain) -> Result<B256> {
        match &self.payload {
            OrderPayload::V2(payload) => Ok(payload.order.eip712_signing_hash(domain)),
            OrderPayload::V1(_) => Err(Error::validation(
                "V2 order hash requested for a V1 payload".to_owned(),
            )),
        }
    }
}
```

Import the same `Eip712Domain`, `SolStruct`, and `B256` types already used by `client.rs`. Document the official proof sources in the test:

```text
https://github.com/Polymarket/ctf-exchange-v2/blob/main/src/exchange/mixins/Hashing.sol
https://github.com/Polymarket/ctf-exchange-v2/blob/main/src/exchange/libraries/Structs.sol
```

- [ ] **Step 4: Add field-mutation and application-adapter proof tests**

Create a table-driven test that mutates `salt`, `maker`, `signer`, `tokenId`, `makerAmount`, `takerAmount`, `side`, `signatureType`, `timestamp`, `metadata`, and `builder`, asserting every mutation changes the hash. In `clob_sdk_orders.rs`, assert normal and neg-risk domains use `contract_config(POLYGON, neg_risk).exchange_v2` and that the formatted ID is exactly `0x` plus 64 lowercase hex characters.

```rust
fn exact_v2_order_id(signed: &SdkSignedOrder, neg_risk: bool) -> Result<String, OrderSubmitError> {
    let exchange = contract_config(POLYGON, neg_risk)
        .and_then(|config| config.exchange_v2)
        .ok_or_else(|| preflight(OrderStage::Sign, OrderErrorCode::ExactOrderIdUnavailable))?;
    let domain = eip712_domain! {
        name: "Polymarket CTF Exchange",
        version: "2",
        chain_id: POLYGON,
        verifying_contract: exchange,
    };
    Ok(format!("{:#x}", signed.v2_order_hash(&domain).map_err(|_| {
        preflight(OrderStage::Sign, OrderErrorCode::ExactOrderIdUnavailable)
    })?))
}
```

- [ ] **Step 5: Run the proof gate and verify GREEN**

```powershell
cargo test --offline --locked service::clob_sdk_orders::tests::v2_order_hash
cargo test --offline --locked service::clob_sdk_orders::tests::v2_order_id
```

Expected: all tests PASS. If the independent vector cannot be proven, stop implementation here, leave the runtime unwired, and report Phase 3A incomplete.

- [ ] **Step 6: Commit Task 1**

```powershell
git add -- vendor/polymarket_client_sdk_v2/src/clob/types/mod.rs src/service/clob_sdk_orders.rs
git commit -m "feat: prove exact v2 order identity"
```

---

### Task 2: Add durable execution configuration and direct dependencies

**Files:**
- Modify/Test: `Cargo.toml`
- Modify/Test: `Cargo.lock`
- Modify/Test: `src/config.rs`
- Modify: `config.json`
- Modify: `config.dryrun-public.json`

**Interfaces:**
- Produces: `TradingConfig::execution_ledger_path: PathBuf`, default `execution-ledger.jsonl`.
- Derives: `<ledger>.active.json` and `<ledger>.lock` in the same directory; these are not separately configurable.

- [ ] **Step 1: Write failing default and committed-config tests**

```rust
#[test]
fn execution_ledger_path_defaults_when_omitted() {
    let mut value: serde_json::Value = serde_json::from_str(include_str!("../config.json")).unwrap();
    value["trading"].as_object_mut().unwrap().remove("execution_ledger_path");
    let cfg: AppConfig = serde_json::from_value(value).unwrap();
    assert_eq!(cfg.trading.execution_ledger_path, PathBuf::from("execution-ledger.jsonl"));
}

#[test]
fn committed_configs_pin_safe_ledger_and_trading_flags() {
    for raw in [include_str!("../config.json"), include_str!("../config.dryrun-public.json")] {
        let cfg: AppConfig = serde_json::from_str(raw).unwrap();
        assert_eq!(cfg.trading.execution_ledger_path, PathBuf::from("execution-ledger.jsonl"));
        assert!(!cfg.bot.enable_trading);
        assert!(cfg.bot.mock_trading);
    }
}
```

- [ ] **Step 2: Run the focused tests and verify RED**

```powershell
cargo test --offline --locked config::tests::execution_ledger_path
cargo test --offline --locked config::tests::committed_configs_pin_safe_ledger
```

Expected: compile failure because the field is absent.

- [ ] **Step 3: Add the field and dependencies**

```toml
sha2 = "0.10"
uuid = { version = "1", features = ["v4", "serde"] }
fs2 = "0.4"
```

```rust
#[serde(default = "default_execution_ledger_path")]
pub execution_ledger_path: PathBuf,

fn default_execution_ledger_path() -> PathBuf {
    PathBuf::from("execution-ledger.jsonl")
}
```

Add `"execution_ledger_path": "execution-ledger.jsonl"` to both committed public configs without changing safe flags. Generate the lockfile offline.

- [ ] **Step 4: Verify GREEN and dependency availability**

```powershell
cargo check --offline
cargo test --offline --locked config::tests::execution_ledger_path
cargo test --offline --locked config::tests::committed_configs_pin_safe_ledger
```

Expected: PASS and no network access.

- [ ] **Step 5: Commit Task 2**

```powershell
git add -- Cargo.toml Cargo.lock src/config.rs config.json config.dryrun-public.json
git commit -m "feat: configure durable execution ledger"
```

---

### Task 3: Define the closed ledger schema and replay state machine

**Files:**
- Create/Test: `src/service/execution_ledger/mod.rs`
- Create/Test: `src/service/execution_ledger/model.rs`
- Create/Test: `src/service/execution_ledger/projection.rs`
- Modify: `src/service/mod.rs`

**Interfaces:**
- Produces: `IntentId`, `EventId`, `OrderId`, `PositionId`, `EventHash`, `LedgerEvent`, `LedgerPayload`, `LedgerProjection`, `LedgerError`.
- Invariant: all serialized identities and amounts are exact, stable, versioned, and secret-free.

- [ ] **Step 1: Write failing schema and transition tests**

Cover sequence start, legal entry/exit chains, rejected/no-fill, `NotSent`, all reconciliation classes, recovery application, cancellation, acknowledgement, unknown kind/schema rejection, duplicate event idempotency, conflicting duplicates, and one active intent at a time.

```rust
#[test]
fn matched_entry_requires_position_before_commit() {
    let mut projection = LedgerProjection::default();
    projection.apply(fixture_event(1, LedgerPayload::IntentPrepared(fixture_entry()))).unwrap();
    projection.apply(fixture_event(2, LedgerPayload::SubmitStarted)).unwrap();
    projection.apply(fixture_event(3, LedgerPayload::RemoteMatched(fixture_match()))).unwrap();

    let error = projection
        .apply(fixture_event(4, LedgerPayload::SubmissionCommitted))
        .unwrap_err();
    assert_eq!(error.code(), LedgerErrorCode::IllegalTransition);
}
```

- [ ] **Step 2: Run the module test and verify RED**

```powershell
cargo test --offline --locked service::execution_ledger
```

Expected: compile failure because the module does not exist.

- [ ] **Step 3: Add exact identity and payload types**

Use transparent newtypes and a closed tagged enum:

```rust
pub const LEDGER_SCHEMA_VERSION: u32 = 1;
pub const ZERO_EVENT_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IntentId(pub Uuid);

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OrderId(String);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum LedgerPayload {
    IntentPrepared(PreparedIntent),
    SubmitStarted,
    RemoteMatched(MatchedAmounts),
    RemoteRejected { code: RemoteRejectCode },
    RemoteUncertain { code: UncertainCode },
    SubmissionCommitted,
    SubmissionCommittedNoFill,
    PositionOpened(DurablePosition),
    PositionClosed(PositionClose),
    ReconciliationStarted,
    ReconciledMatched(MatchedAmounts),
    ReconciledNoFill { status: TerminalNoFillStatus },
    ReconciledLive,
    ReconciledPending,
    ReconciledUncertain { code: ReconcileUncertainCode },
    CancelStarted,
    CancelResponseObserved { result: CancelResponseClass },
    RecoveryApplied { position_event_id: EventId },
    Acknowledged { reason: AcknowledgeReason },
}
```

`PreparedIntent` contains exact order ID, protocol version `2`, venue, token ID, neg-risk, side, FOK, expected maker/taker micro-units, optional sanitized source hash, and `IntentPurpose::{Entry(PositionSeed), Exit { position_id }}`. `DurablePosition` contains `position_id = PositionId(opening_intent.0)`, opening intent/order IDs, metadata, integer shares/USD, integer TP/SL basis points, and timestamps.

- [ ] **Step 4: Implement the pure projection**

```rust
#[derive(Debug, Default)]
pub struct LedgerProjection {
    pub sequence: u64,
    pub head_hash: EventHash,
    pub active: Option<ActiveIntent>,
    pub positions: HashMap<PositionId, DurablePosition>,
    pub event_ids: HashMap<EventId, EventHash>,
}

impl LedgerProjection {
    pub fn validate_and_apply(&mut self, event: &LedgerEvent) -> Result<ApplyOutcome, LedgerError> {
        self.validate_envelope(event)?;
        self.validate_transition(event)?;
        self.apply_payload(event)?;
        self.sequence = event.sequence;
        self.head_hash = event.event_hash.clone();
        self.event_ids.insert(event.event_id, event.event_hash.clone());
        Ok(ApplyOutcome::Applied)
    }
}
```

An identical repeated `event_id` and identical hash returns `AlreadyApplied`; the same ID with different content is `IdempotencyConflict`. `Acknowledged` is the only event that clears `active`.

- [ ] **Step 5: Manually redact errors and verify schema stability**

`LedgerError::{Debug,Display}` must emit only stable code and configured labels. Add a JSON golden test for every event discriminant and sentinel tests proving no secret-shaped field exists in serialized events.

- [ ] **Step 6: Run tests and verify GREEN**

```powershell
cargo test --offline --locked service::execution_ledger::model
cargo test --offline --locked service::execution_ledger::projection
```

Expected: PASS.

- [ ] **Step 7: Commit Task 3**

```powershell
git add -- src/service/execution_ledger src/service/mod.rs
git commit -m "feat: define durable execution state machine"
```

---

### Task 4: Implement append-only storage, hash-chain validation, and exclusive locking

**Files:**
- Create/Test: `src/service/execution_ledger/storage.rs`
- Modify/Test: `src/service/execution_ledger/mod.rs`
- Modify/Test: `src/service/execution_ledger/model.rs`

**Interfaces:**
- Produces: `ExecutionLedger::open_live`, `ExecutionLedger::append`, `ExecutionLedger::projection`.
- Owns: one `fs2` exclusive lock for the ledger lifetime and one in-process mutex for append ordering.

- [ ] **Step 1: Write failing storage tests**

Cover first append/reopen, multi-event replay, deterministic hashes, duplicate append, conflicting duplicate, skipped/duplicate sequence, broken previous/current hash, invalid JSON, unknown schema/kind, missing final newline, lock contention, unwritable parent, and symlink/reparse rejection where supported.

```rust
#[test]
fn truncated_tail_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("execution-ledger.jsonl");
    std::fs::write(&path, br#"{"schema_version":1}"#).unwrap();
    let error = ExecutionLedger::open_live(&path).unwrap_err();
    assert_eq!(error.code(), LedgerErrorCode::TruncatedTail);
}
```

- [ ] **Step 2: Run and verify RED**

```powershell
cargo test --offline --locked service::execution_ledger::storage
```

Expected: compile failure because storage is absent.

- [ ] **Step 3: Implement canonical event hashing**

Serialize a fixed-field struct, never a map:

```rust
#[derive(Serialize)]
struct HashMaterial<'a> {
    schema_version: u32,
    sequence: u64,
    event_id: EventId,
    intent_id: IntentId,
    recorded_at: &'a DateTime<Utc>,
    payload: &'a LedgerPayload,
    previous_hash: &'a EventHash,
}

fn calculate_event_hash(material: &HashMaterial<'_>) -> Result<EventHash, LedgerError> {
    let bytes = serde_json::to_vec(material).map_err(|_| LedgerError::serialization())?;
    Ok(EventHash::from_bytes(Sha256::digest(bytes)))
}
```

- [ ] **Step 4: Implement path containment and exclusive open**

Derive snapshot and lock names from the configured file name. Reject the ledger, lock, snapshot, parent, or existing target when `symlink_metadata` reports a symlink; on Windows also reject `FILE_ATTRIBUTE_REPARSE_POINT` using `std::os::windows::fs::MetadataExt::file_attributes()` and constant `0x400`. Canonicalize the parent and assert every derived target parent equals it before opening.

```rust
let lock_file = OpenOptions::new().create(true).read(true).write(true).open(&paths.lock)?;
lock_file.try_lock_exclusive().map_err(|_| LedgerError::locked())?;
```

- [ ] **Step 5: Implement replay and synchronous append protocol**

While holding the mutex and process lock: build the next event, validate against a cloned projection, append JSON plus newline, `flush`, `sync_all`, then apply to the live projection. If any write/sync/apply step fails, set an in-memory fatal flag and reject every later append.

```rust
pub fn append(&self, intent: IntentId, payload: LedgerPayload) -> Result<AppendOutcome, LedgerError> {
    let mut state = self.state.lock();
    state.ensure_healthy()?;
    let event = state.build_next(intent, payload)?;
    state.projection.validate_next(&event)?;
    serde_json::to_writer(&mut state.file, &event).map_err(|_| state.fail_write())?;
    state.file.write_all(b"\n").map_err(|_| state.fail_write())?;
    state.file.flush().map_err(|_| state.fail_flush())?;
    state.file.sync_all().map_err(|_| state.fail_sync())?;
    state.projection.validate_and_apply(&event)?;
    Ok(AppendOutcome::Appended(event))
}
```

- [ ] **Step 6: Add injectable durability failures and verify GREEN**

Introduce a private `DurabilityOps` trait used only at I/O seams so tests can fail append, flush, sync, persist, and directory sync without changing public API. Verify every injected failure leaves the instance fatal and replay either sees the previous complete event or safely rejects the file.

```powershell
cargo test --offline --locked service::execution_ledger::storage
```

Expected: PASS.

- [ ] **Step 7: Commit Task 4**

```powershell
git add -- src/service/execution_ledger
git commit -m "feat: persist hash chained execution ledger"
```

---

### Task 5: Add the atomic active snapshot and compatibility halt ordering

**Files:**
- Create/Test: `src/service/execution_ledger/snapshot.rs`
- Modify/Test: `src/service/execution_ledger/storage.rs`
- Modify/Test: `src/service/execution_circuit_breaker.rs`

**Interfaces:**
- Produces: `ActiveSnapshot { schema_version, sequence, head_hash, active_intent }`.
- Invariant: live open accepts absent snapshot only when replay has no active intent; any mismatch fails closed.

- [ ] **Step 1: Write failing snapshot tests**

Cover create, replace, no-active absence, active absence, malformed data, sequence/hash/intent mismatch, temp persist failure, directory sync failure, and leftover compatibility marker after an already acknowledged ledger.

- [ ] **Step 2: Verify RED**

```powershell
cargo test --offline --locked service::execution_ledger::snapshot
```

Expected: compile failure because the snapshot module is absent.

- [ ] **Step 3: Implement same-directory atomic replacement**

```rust
fn persist_snapshot(paths: &LedgerPaths, snapshot: &ActiveSnapshot) -> Result<(), LedgerError> {
    let mut temp = tempfile::Builder::new()
        .prefix(".execution-active-")
        .tempfile_in(&paths.parent)
        .map_err(|_| LedgerError::snapshot_write())?;
    serde_json::to_writer(temp.as_file_mut(), snapshot)
        .map_err(|_| LedgerError::snapshot_write())?;
    temp.as_file_mut().flush().map_err(|_| LedgerError::snapshot_flush())?;
    temp.as_file().sync_all().map_err(|_| LedgerError::snapshot_sync())?;
    temp.persist(&paths.snapshot).map_err(|_| LedgerError::snapshot_persist())?;
    sync_parent_supported(&paths.parent)?;
    Ok(())
}
```

Update the snapshot after each append that changes active state. Do not silently regenerate a missing or conflicting active snapshot during live open.

- [ ] **Step 4: Make the ledger authoritative over marker deletion**

Change `ExecutionCircuitBreaker::new_live` to accept `Arc<ExecutionLedger>`. It halts when the ledger has an active intent regardless of marker state. A marker with no active intent remains a compatibility halt that only idempotent `acknowledge` cleanup may remove.

- [ ] **Step 5: Run focused tests and verify GREEN**

```powershell
cargo test --offline --locked service::execution_ledger::snapshot
cargo test --offline --locked service::execution_circuit_breaker
```

Expected: PASS, including “manual marker deletion cannot bypass active ledger intent.”

- [ ] **Step 6: Commit Task 5**

```powershell
git add -- src/service/execution_ledger src/service/execution_circuit_breaker.rs
git commit -m "feat: mirror active execution state atomically"
```

---

### Task 6: Rebuild live positions from the ledger while isolating paper positions

**Files:**
- Modify/Test: `src/service/position_store.rs`
- Modify/Test: `src/service/order_executor.rs`
- Modify/Test: `src/service/position_monitor.rs`

**Interfaces:**
- Produces: `PositionStore::new_paper()` and `PositionStore::from_ledger(Arc<ExecutionLedger>)`.
- Produces: exact `apply_open`, `apply_close`, `get_by_token`, `get_by_id`, `snapshot`, category/tag exposure.

- [ ] **Step 1: Write failing durable-position tests**

Cover restart rebuild, exact micro-unit preservation, duplicate apply no-op, conflicting duplicate fatal, named-position-only close, uncertain/rejected no mutation, one-position-per-token entry conflict, and TP/SL snapshot after reopen.

```rust
#[test]
fn reopened_store_preserves_exact_micro_units() {
    let fixture = live_ledger_with_open_position(12_345_678, 6_172_839);
    let reopened = PositionStore::from_ledger(fixture.reopen()).unwrap();
    let position = reopened.snapshot().pop().unwrap();
    assert_eq!(position.shares_micros, 12_345_678);
    assert_eq!(position.usd_notional_micros, 6_172_839);
}
```

- [ ] **Step 2: Verify RED**

```powershell
cargo test --offline --locked service::position_store
```

Expected: compile failures for the new constructors and integer fields.

- [ ] **Step 3: Replace the volatile-only shape with explicit backends**

```rust
enum PositionBackend {
    Paper(RwLock<HashMap<PositionId, OpenPosition>>),
    Live(Arc<ExecutionLedger>),
}

pub struct PositionStore {
    backend: PositionBackend,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OpenPosition {
    pub position_id: PositionId,
    pub opening_intent_id: IntentId,
    pub opening_order_id: OrderId,
    pub token_id: String,
    pub shares_micros: u128,
    pub usd_notional_micros: u128,
    pub take_profit_bps: u32,
    pub stop_loss_bps: u32,
}
```

Retain slug/category/tags/neg-risk/side/timestamps. Provide `shares()`, `usd_notional()`, `entry_price()`, and `pnl_pct()` conversion methods at presentation/risk boundaries.

- [ ] **Step 4: Enforce append-before-projection semantics**

For live apply, append `PositionOpened`/`PositionClosed`; the ledger projection changes only after sync. For paper apply, mutate only the isolated map and never open the ledger. Return `PositionApply::{Applied, AlreadyApplied}`; conflicting duplicates return typed fatal errors.

- [ ] **Step 5: Update callers and verify GREEN**

Update field access in executor/monitor and all fixtures. An uncertain exit must retain the open position; only a durable `PositionClosed` removes it.

```powershell
cargo test --offline --locked service::position_store
cargo test --offline --locked service::position_monitor
cargo test --offline --locked service::order_executor
```

Expected: PASS.

- [ ] **Step 6: Commit Task 6**

```powershell
git add -- src/service/position_store.rs src/service/order_executor.rs src/service/position_monitor.rs
git commit -m "feat: rebuild live positions from ledger"
```

---

### Task 7: Insert a durable pre-POST journal seam into the official SDK gateway

**Files:**
- Modify/Test: `src/service/order_gateway.rs`
- Modify/Test: `src/service/clob_sdk_orders.rs`
- Modify/Test: `src/service/execution_circuit_breaker.rs`

**Interfaces:**
- Produces: SDK-neutral `PreparedOrderIdentity` and `PrePostJournal`.
- Changes: `OrderGateway::submit_fok(planned, journal)`; the adapter must invoke `journal.before_post` exactly once after signing/ID proof and immediately before POST.

- [ ] **Step 1: Write failing ordering and response-ID tests**

Loopback tests must prove: no POST when journal append fails; `IntentPrepared` precedes `SubmitStarted`; `SubmitStarted` sync completes before server accepts bytes; exactly one POST; response order ID mismatch is uncertain; HTTP 5xx is uncertain; only a deterministic pre-acceptance client rejection or parsed `success=false` is rejected/no-fill; preflight before journal creates no event.

- [ ] **Step 2: Verify RED**

```powershell
cargo test --offline --locked service::clob_sdk_orders::tests::journal
cargo test --offline --locked service::clob_sdk_orders::tests::response_order_id_mismatch
```

Expected: compile failure because the journal contract is absent.

- [ ] **Step 3: Add the neutral journal contract**

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedOrderIdentity {
    pub order_id: OrderId,
    pub protocol_version: u8,
    pub token_id: String,
    pub neg_risk: bool,
    pub side: Side,
    pub expected_making_micros: u128,
    pub expected_taking_micros: u128,
}

pub trait PrePostJournal: Send + Sync {
    fn before_post(&self, identity: &PreparedOrderIdentity) -> Result<(), OrderSubmitError>;
}

#[async_trait]
pub trait OrderGateway: Send + Sync {
    async fn submit_fok(
        &self,
        planned: &PlannedOrder,
        journal: &dyn PrePostJournal,
    ) -> Result<OrderReceipt, OrderSubmitError>;
}
```

- [ ] **Step 4: Compute identity during preparation and journal immediately before POST**

Extend private `PreparedOrder` with `identity`. Its `Debug` remains redacted. In `submit_fok`, perform all metadata/build/sign work, call `journal.before_post(&prepared.identity)?`, then invoke the single timeout-wrapped `post_order`. Compare `PostOrderResponse.order_id` byte-for-byte with the prepared ID before accepting a match. Reclassify HTTP 5xx, timeout, disconnect, decode failure, and ambiguous status as uncertain; reserve rejected/no-fill for a deterministic client rejection or a successfully parsed `success=false` response. Add `ExactOrderIdUnavailable` and `ResponseOrderIdMismatch` stable codes.

- [ ] **Step 5: Implement the ledger-backed journal**

The breaker creates a journal with one pre-generated `IntentId` and `IntentPurpose`. `before_post` appends and syncs `IntentPrepared`, then appends and syncs `SubmitStarted`. A second callback invocation is fatal. No API returns from `SubmitStarted` to `submit_fok` after restart.

- [ ] **Step 6: Run focused tests and verify GREEN**

```powershell
cargo test --offline --locked service::clob_sdk_orders::tests
cargo test --offline --locked service::execution_circuit_breaker::tests::submit_started
```

Expected: PASS, one loopback POST, zero retries.

- [ ] **Step 7: Commit Task 7**

```powershell
git add -- src/service/order_gateway.rs src/service/clob_sdk_orders.rs src/service/execution_circuit_breaker.rs
git commit -m "feat: journal intent before order post"
```

---

### Task 8: Journal normal entry and exit completion with crash-injection coverage

**Files:**
- Modify/Test: `src/service/execution_circuit_breaker.rs`
- Modify/Test: `src/service/order_executor.rs`
- Modify/Test: `src/service/position_monitor.rs`
- Create/Test: `tests/execution_crash_matrix.rs`

**Interfaces:**
- Produces: `ExecutionIntent { intent_id, planned, purpose }` and `ExecutionResult` coordinated by the breaker.
- Invariant: entries and exits share the same ledger, submission mutex, gateway, breaker, and position projection.

- [ ] **Step 1: Write failing normal-state and crash-matrix tests**

Inject interruption/failure immediately before and after prepared append, submit-start append, POST invocation, remote evidence append, position append, terminal append, and snapshot replace. Reopen after every point and assert exact committed positions or a safe active halt; assert no reopen performs network or repost.

- [ ] **Step 2: Verify RED**

```powershell
cargo test --offline --locked --test execution_crash_matrix
```

Expected: compile failure because coordinated execution is absent.

- [ ] **Step 3: Coordinate terminal evidence and position events**

```rust
pub async fn execute_fok(
    &self,
    gateway: &dyn OrderGateway,
    positions: &PositionStore,
    intent: ExecutionIntent,
) -> Result<OrderReceipt, OrderSubmitError> {
    let _guard = self.submit_lock.lock().await;
    self.check()?;
    let journal = LedgerSubmissionJournal::new(self.ledger.clone(), &intent);
    match gateway.submit_fok(&intent.planned, &journal).await {
        Ok(receipt) => {
            self.ledger.append(intent.intent_id, LedgerPayload::RemoteMatched(receipt.amounts()))?;
            positions.apply_match(&intent, &receipt)?;
            self.ledger.append(intent.intent_id, LedgerPayload::SubmissionCommitted)?;
            Ok(receipt)
        }
        Err(error @ OrderSubmitError::Rejected { .. }) if journal.started() => {
            self.ledger.append(intent.intent_id, LedgerPayload::RemoteRejected { code: error.reject_code() })?;
            self.ledger.append(intent.intent_id, LedgerPayload::SubmissionCommittedNoFill)?;
            Err(error)
        }
        Err(error) => self.persist_uncertain_if_started(intent.intent_id, error),
    }
}
```

If any persistence fails after POST may have started, mark the breaker fatal, persist the compatibility marker when possible, return `Halted`, and never mutate positions or call the gateway again.

- [ ] **Step 4: Wire entry and TP/SL exit purpose**

Entry purpose carries `PositionSeed`; exit purpose carries the exact `position_id`. TP/SL takes a durable snapshot and closes only that ID. A rejected order commits no-fill; uncertainty leaves entry unapplied or the exit position open.

- [ ] **Step 5: Verify matrix and regressions GREEN**

```powershell
cargo test --offline --locked --test execution_crash_matrix
cargo test --offline --locked service::order_executor
cargo test --offline --locked service::position_monitor
```

Expected: PASS and every request counter remains `0` on reopen.

- [ ] **Step 6: Commit Task 8**

```powershell
git add -- src/service/execution_circuit_breaker.rs src/service/order_executor.rs src/service/position_monitor.rs tests/execution_crash_matrix.rs
git commit -m "feat: commit journaled entries and exits"
```

---

### Task 9: Add exact-order recovery contracts and the SDK read adapter

**Files:**
- Create/Test: `src/service/recovery_gateway.rs`
- Create/Test: `src/service/clob_sdk_recovery.rs`
- Modify: `src/service/mod.rs`

**Interfaces:**
- Produces: exact-only `RecoveryGateway`, `RemoteOrderEvidence`, `CancelAttemptEvidence`, `RecoveryError`.
- Adapter calls: `client.order(exact_id)`, then exactly one `client.trades(id=trade_id)` request per associated trade; `client.cancel_order(exact_id)` only.

- [ ] **Step 1: Write failing neutral-interface compile tests**

Assert a fake implements only exact query/cancel. Add source/API tests showing no batch, market, cancel-all, arbitrary search, or heuristic identity method exists.

```rust
#[async_trait]
pub trait RecoveryGateway: Send + Sync {
    async fn reconcile_exact(
        &self,
        expected: &PreparedOrderIdentity,
    ) -> Result<RemoteOrderEvidence, RecoveryError>;

    async fn cancel_exact(
        &self,
        order_id: &OrderId,
    ) -> Result<CancelAttemptEvidence, RecoveryError>;
}
```

- [ ] **Step 2: Verify RED**

```powershell
cargo test --offline --locked service::recovery_gateway
```

Expected: compile failure because the modules do not exist.

- [ ] **Step 3: Define sanitized evidence**

```rust
pub enum RemoteOrderEvidence {
    Matched { making_micros: u128, taking_micros: u128, trade_ids: Vec<TradeId> },
    NoFill { status: TerminalNoFillStatus },
    Live,
    Pending,
    Uncertain { code: ReconcileUncertainCode },
}

pub enum CancelAttemptEvidence {
    Canceled,
    NotCanceled,
    Uncertain { code: CancelUncertainCode },
}
```

Manually redact `Debug` and `Display`; never attach raw SDK errors or body text.

- [ ] **Step 4: Implement exact SDK classification**

Validate response `id`, token, side, `original_size`, exact expected maker/taker amounts derived by side, FOK type, and associated trades. Query each listed trade by exact trade ID and require association to the exact order as taker or maker, compatible asset/side, successful final status, exact aggregate amount, and no continuation cursor; pagination or extra ambiguous evidence is uncertain rather than followed automatically. Classify live/unmatched as `Live`, delayed as `Pending`, canceled with zero matched as `NoFill`, matched only when full and consistent, and every 404/unknown/partial/mismatch/timeout/transport/decode case as sanitized `Uncertain`.

- [ ] **Step 5: Add loopback matrix**

Cover matched BUY/SELL, canceled zero-fill, live, pending, 404, partial, wrong ID/token/side/type, amount/trade mismatch, unknown status, timeout, disconnect, malformed response, unexpected HTTP class, raw-body sentinel redaction, no L1 key create/derive request, exact request counts, and zero retries.

```powershell
cargo test --offline --locked service::clob_sdk_recovery
```

Expected: PASS; all servers bind only `127.0.0.1`.

- [ ] **Step 6: Commit Task 9**

```powershell
git add -- src/service/recovery_gateway.rs src/service/clob_sdk_recovery.rs src/service/mod.rs
git commit -m "feat: reconcile exact ledger orders"
```

---

### Task 10: Implement human-bound recovery inspect, reconcile, apply, and acknowledge

**Files:**
- Create/Test: `src/service/recovery_service.rs`
- Modify/Test: `src/service/execution_ledger/projection.rs`
- Modify/Test: `src/service/position_store.rs`
- Modify: `src/service/mod.rs`

**Interfaces:**
- Produces: `RecoveryService::{inspect, reconcile, prepare_apply, apply, prepare_acknowledge, acknowledge}`.
- Produces: `ConfirmationChallenge` bound to action, intent, order ID, sequence, and head hash.

- [ ] **Step 1: Write failing service tests**

Cover local `NotSent`, all remote classifications, matched requires apply, apply requires fresh challenge, stale challenge rejected before mutation/network, duplicate apply no-op, conflict fatal, acknowledge allowlist, marker cleanup ordering, marker-deletion bypass prevention, and no automatic resume.

- [ ] **Step 2: Verify RED**

```powershell
cargo test --offline --locked service::recovery_service
```

Expected: compile failure because the service is absent.

- [ ] **Step 3: Implement deterministic challenge binding**

```rust
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ChallengeMaterial<'a> {
    action: RecoveryAction,
    intent_id: IntentId,
    order_id: &'a OrderId,
    sequence: u64,
    head_hash: &'a EventHash,
}

fn challenge(material: &ChallengeMaterial<'_>) -> ConfirmationChallenge {
    let bytes = serde_json::to_vec(material).expect("fixed challenge schema serializes");
    ConfirmationChallenge(hex::encode(Sha256::digest(bytes)))
}
```

The challenge is an accident guard, not local-user authentication. Redact it from tracing.

- [ ] **Step 4: Implement inspect and reconcile**

`inspect` replays local state only and shows hints unless `show_order_id`. It also prints the currently valid apply or acknowledge challenge when that action is allowed. `reconcile` appends `ReconciliationStarted`, invokes exactly one adapter reconciliation sequence, then appends exactly one classification event and prints the challenge enabled by that new head (`apply` for a match, `acknowledge` for proven no-fill). No classification mutates positions or clears active state.

- [ ] **Step 5: Implement apply and acknowledge ordering**

`apply` accepts only latest exact `ReconciledMatched`, validates current head-bound challenge, appends the named position event, then `RecoveryApplied`, and prints the new head-bound acknowledge challenge. `acknowledge` accepts only local `NotSent`, `ReconciledNoFill`, or matched plus applied; it appends `Acknowledged`, updates snapshot to no active intent, removes the compatibility marker, and syncs the parent where supported. Repeated cleanup after an already acknowledged ledger is idempotent.

- [ ] **Step 6: Run tests and verify GREEN**

```powershell
cargo test --offline --locked service::recovery_service
cargo test --offline --locked service::position_store
```

Expected: PASS.

- [ ] **Step 7: Commit Task 10**

```powershell
git add -- src/service/recovery_service.rs src/service/execution_ledger/projection.rs src/service/position_store.rs src/service/mod.rs
git commit -m "feat: require human confirmed recovery"
```

---

### Task 11: Add fresh-challenge exact single-order cancellation

**Files:**
- Modify/Test: `src/service/recovery_service.rs`
- Modify/Test: `src/service/clob_sdk_recovery.rs`
- Modify/Test: `src/service/recovery_gateway.rs`

**Interfaces:**
- Produces: `RecoveryService::prepare_cancel` and `RecoveryService::cancel`.
- Invariant: exactly one ledger-owned order ID, one DELETE, and mandatory post-DELETE exact reconciliation.

- [ ] **Step 1: Write failing cancellation tests**

Cover only-active-ID acceptance, arbitrary-ID rejection, fresh query required, stale challenge before network, `CancelStarted` synced before DELETE, canceled/not-canceled both re-query, disconnect after DELETE no retry, matched-after-cancel routes to apply, canceled-zero-fill routes to acknowledge, and compile/source absence of broad cancel.

- [ ] **Step 2: Verify RED**

```powershell
cargo test --offline --locked service::recovery_service::tests::cancel
cargo test --offline --locked service::clob_sdk_recovery::tests::cancel
```

Expected: failures because cancellation orchestration is absent.

- [ ] **Step 3: Implement prepare-cancel**

Perform a fresh `reconcile_exact` against the ledger identity. Issue a `Cancel` challenge only when exact evidence is `Live` and the current sequence/head still matches. Pending/delayed and every uncertain class remain halted without a challenge.

- [ ] **Step 4: Implement cancel ordering**

Validate challenge/head, append and sync `CancelStarted`, call `cancel_exact` once, append `CancelResponseObserved`, then call `reconcile_exact` once and append its classification. The cancel response alone never makes the intent terminal. Any DELETE or follow-up uncertainty remains active.

- [ ] **Step 5: Verify GREEN and exact request counts**

```powershell
cargo test --offline --locked service::recovery_service::tests::cancel
cargo test --offline --locked service::clob_sdk_recovery::tests::cancel
```

Expected: PASS; request counters prove no retries and no broad endpoint.

- [ ] **Step 6: Commit Task 11**

```powershell
git add -- src/service/recovery_service.rs src/service/clob_sdk_recovery.rs src/service/recovery_gateway.rs
git commit -m "feat: cancel one exact recovery order"
```

---

### Task 12: Add the recovery CLI with strict credential isolation

**Files:**
- Modify/Test: `src/main.rs`
- Create/Test: `src/recovery_cli.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Produces exactly:
  - `recovery inspect [--intent <id>] [--show-order-id]`
  - `recovery reconcile --intent <id>`
  - `recovery prepare-cancel --intent <id>`
  - `recovery cancel --intent <id> --confirm <challenge>`
  - `recovery apply --intent <id> --confirm <challenge>`
  - `recovery acknowledge --intent <id> --confirm <challenge>`

- [ ] **Step 1: Write failing parsing and privilege tests**

Assert exact command parsing, no `force`/`yes`/broad-cancel option, local commands ignore malformed credentials and secret env overrides, network commands require credentials, full ID appears only with `--show-order-id`, and no raw-body sentinel reaches output.

- [ ] **Step 2: Verify RED**

```powershell
cargo test --offline --locked --bin polymarket-toolkits recovery
```

Expected: parsing failure because `recovery` is absent.

- [ ] **Step 3: Add closed Clap enums**

```rust
#[derive(Subcommand, Debug)]
enum RecoveryCommand {
    Inspect { #[arg(long)] intent: Option<Uuid>, #[arg(long)] show_order_id: bool },
    Reconcile { #[arg(long)] intent: Uuid },
    PrepareCancel { #[arg(long)] intent: Uuid },
    Cancel { #[arg(long)] intent: Uuid, #[arg(long)] confirm: String },
    Apply { #[arg(long)] intent: Uuid, #[arg(long)] confirm: String },
    Acknowledge { #[arg(long)] intent: Uuid, #[arg(long)] confirm: String },
}
```

- [ ] **Step 4: Split config loading by command capability**

```rust
fn command_needs_credentials(command: &Option<Command>, cfg: &AppConfig) -> bool {
    match command {
        Some(Command::Auth { .. }) => true,
        Some(Command::Recovery { command }) => matches!(
            command,
            RecoveryCommand::Reconcile { .. }
                | RecoveryCommand::PrepareCancel { .. }
                | RecoveryCommand::Cancel { .. }
        ),
        Some(Command::Run { .. }) | Some(Command::Tui) | None => cfg.live_trading_allowed(),
    }
}
```

Local recovery construction receives no gateway factory. Network command construction receives one exact-operation gateway only after credentials load.

- [ ] **Step 5: Add stable redacted output and verify GREEN**

```powershell
cargo test --offline --locked --bin polymarket-toolkits recovery
cargo test --offline --locked --bin polymarket-toolkits strict_run
```

Expected: PASS.

- [ ] **Step 6: Commit Task 12**

```powershell
git add -- src/main.rs src/recovery_cli.rs src/lib.rs
git commit -m "feat: expose explicit recovery commands"
```

---

### Task 13: Wire strict paper and fail-closed live initialization

**Files:**
- Modify/Test: `src/bot/copy_trading.rs`
- Modify/Test: `src/service/order_executor.rs`
- Modify/Test: `src/service/position_monitor.rs`
- Modify/Test: `src/main.rs`

**Interfaces:**
- Paper: in-memory positions only, no ledger/CLOB credentials/SDK/midpoint.
- Live initialization order: validate account/host -> open/lock/replay ledger -> rebuild positions -> verify snapshot -> inspect marker -> build gateway/breaker -> expose shared components.

- [ ] **Step 1: Write failing initialization-order tests**

Use malformed/unwritable ledger paths plus malformed credential files to prove strict paper exits before touching either. Use fake live components to prove unresolved/mismatched ledger blocks before gateway or midpoint construction. Assert executor and TP/SL share pointer-equal ledger, breaker, gateway, and position store.

- [ ] **Step 2: Verify RED**

```powershell
cargo test --offline --locked strict_paper_bypasses_live_ledger
cargo test --offline --locked live_initialization_fails_closed
```

Expected: failures until construction is reordered.

- [ ] **Step 3: Introduce one shared live runtime bundle**

```rust
pub struct LiveExecutionRuntime {
    pub ledger: Arc<ExecutionLedger>,
    pub positions: Arc<PositionStore>,
    pub gateway: Arc<dyn OrderGateway>,
    pub breaker: Arc<ExecutionCircuitBreaker>,
}
```

Construct it once in `OrderExecutor::new` only after `cfg.live_trading_allowed()`. Return paper executor immediately otherwise. Pass clones from the bundle to TP/SL; do not reopen the ledger.

- [ ] **Step 4: Prove startup is local and non-healing**

Add request-count fakes showing live startup with active intent makes zero network calls, emits a static operator instruction, and does not recreate/overwrite a conflicting snapshot or remove a marker.

- [ ] **Step 5: Run runtime regressions and verify GREEN**

```powershell
cargo test --offline --locked strict_paper_bypasses_live_ledger
cargo test --offline --locked live_initialization_fails_closed
cargo test --offline --locked bot::copy_trading
cargo test --offline --locked service::position_monitor
```

Expected: PASS.

- [ ] **Step 6: Commit Task 13**

```powershell
git add -- src/bot/copy_trading.rs src/service/order_executor.rs src/service/position_monitor.rs src/main.rs
git commit -m "feat: wire fail closed durable runtime"
```

---

### Task 14: Document the operator workflow and run all Phase 3A gates

**Files:**
- Modify: `README.md`
- Modify: `README.zh-CN.md`
- Modify: `docs/superpowers/plans/2026-08-18-phase-3a-durable-recovery.md`
- Modify: task-relevant Obsidian project note only after implementation verification

**Interfaces:**
- Documents: ledger path, exact manual sequence, marker prohibition, local/network privilege split, Phase 3A non-live boundary, and remaining Phase 3B–3D gates.

- [x] **Step 1: Add matching English and Chinese recovery sections**

Document the exact flow:

```text
inspect -> reconcile -> [prepare-cancel -> cancel -> reconcile]
                    \-> apply -> acknowledge
```

State explicitly: never delete `execution-halt.json` to resume; a recovered match requires `apply` then `acknowledge`; 404/partial/unknown cannot be acknowledged; Phase 3A used only offline and loopback acceptance and does not authorize live use.

- [x] **Step 2: Run focused and full offline tests**

```powershell
cargo test --offline --locked service::execution_ledger
cargo test --offline --locked service::clob_sdk_orders
cargo test --offline --locked service::clob_sdk_recovery
cargo test --offline --locked service::recovery_service
cargo test --offline --locked execution_crash_matrix -- --test-threads=1
cargo test --all-targets --offline --locked
```

Expected: all PASS, no public endpoint access.

- [x] **Step 3: Run build, lint, and changed-file formatting gates**

```powershell
cargo build --release --offline --locked
cargo clippy --all-targets --offline --locked -- -D warnings -A clippy::field_reassign_with_default
rustfmt --edition 2021 --check --config skip_children=true src/service/mod.rs src/service/execution_ledger/storage.rs src/service/position_store.rs src/bot/copy_trading.rs
git diff --check
```

If repository-wide `cargo fmt -- --check` reports known unrelated baseline differences, run `rustfmt` only on changed Rust files with the repository toolchain and record the unchanged baseline separately; do not reformat unrelated files.

- [x] **Step 4: Run safety scans**

```powershell
rg -n "PRIVATE KEY|BEGIN PRIVATE|api_secret|api_passphrase|POLY_SIGNATURE|POLY_PASSPHRASE" . --glob "!target/**" --glob "!config.yaml" --glob "!*.example"
rg -n "retry|backoff|post_orders|cancel_orders|cancel_all_orders|cancel_market_orders|cancel-all|cancel-market" src tests
rg -n "raw response|response body|SdkError|error_msg" src/service
rg -n '"enable_trading"\s*:\s*true|"mock_trading"\s*:\s*false' config.json config.dryrun-public.json
rg -n "https://clob-v2.polymarket.com|wss://|gamma-api" tests src --glob "*test*"
```

Expected: only intentional sanitized fields, SDK adapter references, negative tests, docs, and production host guards appear. No broad-cancel call is reachable from application recovery code; no committed live config exists.

- [ ] **Step 5: Perform independent specification and code review**

Use `superpowers:requesting-code-review`. Review specifically:

- exact V2 order-ID proof and domain selection;
- fsync-before-POST ordering and one-POST invariant;
- replay/hash/snapshot/lock correctness;
- durable position idempotency;
- startup zero-network behavior;
- stale challenge handling;
- single-order cancellation and mandatory re-query;
- credential and output privilege boundaries;
- absence of automatic healing/resume.

Address every High/Critical issue, rerun affected gates, and preserve the review result.

- [ ] **Step 6: Update Obsidian with safe project facts only**

Record architecture, implementation commit IDs, verified test counts, review outcome, and remaining Phase 3B–3D boundary. Do not store ledger contents, complete order IDs, credentials, signatures, bodies, or raw terminal output.

- [x] **Step 7: Commit Task 14**

```powershell
git add -- README.md README.zh-CN.md docs/superpowers/plans/2026-08-18-phase-3a-durable-recovery.md
git commit -m "docs: complete phase 3a recovery guidance"
```

---

## Completion Checklist

- [x] Official contract algorithm and independent fixed vector prove the exact V2 order ID.
- [x] Exact ID and synchronized `SubmitStarted` exist before POST bytes may leave.
- [x] No normal, restart, or recovery path retries or reposts an uncertain order.
- [x] JSONL replay, hash chain, lock, active snapshot, and failure injection all fail closed.
- [x] Live positions rebuild exactly from integer ledger events; paper positions remain isolated.
- [x] Only exact full FOK evidence can create a position event.
- [x] Recovery apply and acknowledge are separate, fresh-challenge human actions.
- [x] Cancellation is one exact order, one DELETE, and mandatory exact re-query.
- [x] Local commands do not load credentials; network commands have narrow explicit authority.
- [x] Marker deletion cannot bypass an active ledger intent.
- [x] All acceptance tests are offline or loopback and use no real credentials.
- [ ] English/Chinese docs and safe Obsidian project memory state that Phase 3A is not live authorization.
