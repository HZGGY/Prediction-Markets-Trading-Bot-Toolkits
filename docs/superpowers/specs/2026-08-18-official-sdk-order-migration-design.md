# Official SDK Order Execution Migration Design

## Status

- Date: 2026-08-18
- Phase: Official Rust V2 SDK migration, phase 2 of 3
- State: Approved design, awaiting implementation plan
- Safety boundary: offline and loopback validation only; no real credentials, no real CLOB requests, no order broadcast, and no real funds

## Goal

Replace the production order-construction, EIP-712 signing, L2 HMAC, and `POST /order` path with `polymarket_client_sdk_v2 = 0.6.0`, while preserving the existing copy-trading strategy, risk controls, paper-trading behavior, and EOA-only account model.

The migration must fail closed. A live order may update a position only after the official SDK returns an explicit, internally consistent, fully matched FOK result. Any post-send result whose final state cannot be proven stops all new entries and TP/SL exits and persists that halt across process restarts.

This phase does not make the project production-ready. Balance and allowance checks, order reconciliation, cancellation, recovery, and controlled real-endpoint validation remain phase 3 work.

## Context and Current Problem

Phase 1 introduced the official SDK authentication foundation, pinned the compatible dependency graph, and added explicit Create/Derive API credential commands. The current production order path in `src/service/clob.rs` still manually performs all of the following:

- CLOB V2 order construction;
- EIP-712 digest construction and EOA signing;
- L2 HMAC header generation;
- JSON wire serialization;
- HTTP `POST /order` submission and response interpretation.

That code was useful as an offline compatibility bridge, but maintaining a second protocol implementation creates drift risk as the official SDK evolves. Phase 2 removes the self-built implementation from production and makes the official SDK the single order-protocol implementation.

## Confirmed Decisions

- Adopt a new isolated official-SDK adapter behind an internal, SDK-neutral `OrderGateway` interface.
- Do not retain a custom/SDK dual backend or runtime backend switch.
- Keep the current execution behavior as true FOK (`Fill or Kill`). The misleading internal `Fak` name will be renamed to `Fok`; phase 2 does not adopt partial-fill FAK semantics.
- Strict dry-run performs no CLOB access, creates no authenticated SDK client, builds no SDK order, and signs nothing. It produces only a `PlannedOrder` and a paper position.
- Entry and TP/SL exit orders use the same gateway and the same shared execution circuit breaker.
- Positions change only after an explicit SDK success that proves a complete FOK fill. Position amounts come from the actual `making_amount` and `taking_amount` response fields, not from the original estimate.
- A post-send timeout, connection interruption, malformed success response, status ambiguity, amount mismatch, or non-final success state is `Uncertain`; it is never automatically retried.
- `Uncertain` globally halts both new entries and TP/SL exits in memory and through a persistent marker file.
- All tests are offline or use a local loopback server. They must not use real private keys, real API credentials, real CLOB endpoints, or real orders.

## Scope

### In Scope

- Introduce an SDK-neutral order gateway contract and receipt/error types.
- Add an official SDK 0.6 order adapter for authenticated EOA accounts on Polygon chain ID 137.
- Move live limit-order build, signing, L2 authentication, and posting to the official SDK.
- Align limit prices to the SDK-reported market tick without worsening the configured execution limit.
- Verify SDK market metadata, including `neg_risk`, before sending.
- Require exact, complete FOK response confirmation before changing positions.
- Add a shared in-memory and persistent execution circuit breaker.
- Route copy-trading entries and TP/SL exits through the same gateway and breaker.
- Make strict dry-run independent of credentials and CLOB availability.
- Remove the self-built production order signing, HMAC, JSON payload, and POST implementation.
- Retain only frozen protocol vectors that still provide useful migration-regression evidence; they must no longer be callable production code.
- Update configuration, documentation, tests, and Obsidian project status after implementation is complete.

### Out of Scope

- No real CLOB authentication, market-data lookup, or order request.
- No real private key, API key, secret, or passphrase is entered or stored for testing.
- No pUSD balance, allowance, approval, wrapping, or funding operation.
- No order-status reconciliation, open-order query, cancellation, replacement, or automatic recovery.
- No automatic retry of an uncertain post.
- No automatic clearing of the execution halt marker.
- No proxy, Safe, or POLY_1271 support; phase 2 remains EOA `signature_type = 0` with signer equal to funder.
- No partial-fill FAK policy.
- No strategy, sizing, allowlist, TP/SL threshold, or wallet-tracking behavior changes.
- No automatic enabling of `enable_trading` or disabling of `mock_trading`.
- No remote push and no unrelated whole-repository formatting.

## Architecture

```text
copy strategy / TP-SL monitor
          |
          v
     PlannedOrder
          |
          v
    OrderExecutor
          |
          +---- strict dry-run ----> paper position only
          |
          +---- live gates ----> ExecutionCircuitBreaker
                                  |
                                  v
                             OrderGateway
                                  |
                                  v
                           SdkOrderGateway
                                  |
                                  v
                    polymarket_client_sdk_v2 0.6.0
```

### Module Boundaries

`src/service/order_gateway.rs` owns SDK-neutral contracts:

- `OrderGateway`;
- `OrderReceipt`;
- `OrderSubmitError` and sanitized stage/code fields;
- amount conversion helpers used at the position boundary.

`src/service/clob_sdk_orders.rs` owns all official SDK order behavior:

- authenticated SDK client construction from the already-loaded configuration;
- EOA/chain/host validation;
- tick-size and neg-risk lookup;
- limit-order builder mapping;
- explicit SDK build, sign, and post stages;
- SDK response classification and conversion into neutral receipts/errors.

`src/service/execution_circuit_breaker.rs` owns:

- shared in-memory halted state;
- atomic persistent halt-marker creation;
- startup marker detection;
- sanitized halt reason metadata;
- a fail-closed live-startup writability check.

`src/service/order_executor.rs` continues to own strategy-facing execution orchestration and paper-position behavior, but no longer constructs or signs protocol orders.

`src/service/position_monitor.rs` continues to decide when a TP/SL condition is met, but submits exits through the same `OrderGateway` and checks the same `ExecutionCircuitBreaker` as entries.

The production signing, HMAC, wire serialization, and `POST /order` implementation is removed from `src/service/clob.rs`. If fixed-vector helpers remain for migration-oracle tests, they are test-only and cannot be selected by runtime configuration.

## Internal Contracts

The exact Rust syntax may be adjusted during TDD for object safety and existing async conventions, but the semantic contract is fixed:

```rust
#[async_trait]
pub trait OrderGateway: Send + Sync {
    async fn submit_fok(
        &self,
        planned: &PlannedOrder,
    ) -> Result<OrderReceipt, OrderSubmitError>;
}

pub struct OrderReceipt {
    pub order_id: String,
    pub filled_shares_micros: u128,
    pub filled_usd_micros: u128,
}

pub enum OrderSubmitError {
    Preflight {
        stage: OrderStage,
        code: OrderErrorCode,
    },
    Rejected {
        http_status: Option<u16>,
        code: OrderErrorCode,
    },
    Uncertain {
        code: OrderErrorCode,
    },
    Halted {
        code: OrderErrorCode,
    },
}
```

Neutral receipts use integer micro-units rather than SDK `Decimal` or binary floating-point. SDK decimals are validated as finite, non-negative, and exactly convertible to six decimal places before receipt creation. Conversion to the existing position store's `f64` representation occurs only at the position boundary. A conversion failure after posting is `Uncertain`, because the order may have executed even though the local process cannot represent the result safely.

`OrderSubmitError`'s `Display`, `Debug`, source chain, and tracing fields must be sanitized. Callers can branch on category and stable code without receiving response bodies, credentials, signed payloads, or raw SDK debug output.

## Configuration and Initialization

Add `trading.execution_halt_path` to public configuration. Its default is `execution-halt.json`, resolved consistently relative to the process/config policy already used by the application. Tests inject a temporary absolute path.

### Strict Dry-Run

When `enable_trading = false` or `mock_trading = true` selects strict dry-run behavior:

- `OrderExecutor` receives no live gateway;
- no authenticated SDK client is constructed;
- no tick-size, neg-risk, midpoint, or other CLOB endpoint is called;
- no EIP-712 order is built or signed;
- a valid plan creates only the existing paper position;
- TP/SL does not start a CLOB midpoint or exit-order monitor.

Missing private keys or API credentials must therefore not produce a CLOB initialization warning in strict dry-run.

### Live Initialization

`OrderExecutor::new` becomes asynchronous so live initialization can create the official authenticated client. Before the bot accepts any live execution work, initialization must:

1. confirm `enable_trading = true` and `mock_trading = false`;
2. require a private key plus existing API key, secret, and passphrase; it must not create or derive credentials automatically;
3. require exact official host `https://clob-v2.polymarket.com`;
4. require Polygon chain ID 137, EOA signature type 0, and funder equal to signer;
5. resolve the halt-marker path;
6. fail closed if a marker already exists;
7. verify that the marker's parent directory exists and is writable before enabling submission;
8. create one shared circuit breaker and one shared SDK-backed gateway for entries and exits.

Live initialization failure leaves execution disabled. Authentication errors must not silently fall back to dry-run.

## Official SDK Order Mapping

For each `PlannedOrder`, `SdkOrderGateway` maps neutral values to an official SDK limit-order builder:

```text
token_id       -> U256 token id
side           -> SDK Side::Buy or Side::Sell
price          -> rust_decimal::Decimal
size           -> rust_decimal::Decimal
order_type     -> SDK OrderType::FOK
signature type -> EOA
chain          -> Polygon 137
```

The adapter must call the public SDK market helpers for the planned token:

- `tick_size(token_id)`;
- `neg_risk(token_id)`.

The SDK-reported `neg_risk` value must equal the market metadata carried by `PlannedOrder`. A mismatch is a `Preflight` error and no order is built, signed, or sent.

### Tick Alignment

The configured limit must be aligned to the SDK-reported tick without worsening execution:

- BUY: round down to the nearest tick;
- SELL: round up to the nearest tick.

Both the original and aligned prices may be logged as non-secret numeric fields. The aligned price must remain within the valid CLOB price interval and must not become zero or otherwise violate SDK/order constraints. Invalid tick values, an invalid aligned price, zero size, an unrepresentable decimal, or an order made too small by alignment is `Preflight`; no signing or send occurs.

The adapter derives the expected making/taking amounts from the SDK-built order payload. These exact values are retained locally for comparison with the post response.

## Build, Sign, and Post Stages

The adapter performs three explicit SDK stages and does not use a combined `build_sign_and_post` convenience call:

1. `build` — create the SDK signable FOK limit order after metadata and tick checks;
2. `sign` — sign the built order with the configured EOA signer;
3. `post_order` — submit the signed order using the authenticated SDK client.

Explicit staging is required for safe error classification:

- metadata, validation, decimal conversion, build, or sign failure is `Preflight`; the adapter knows no order was posted;
- an SDK HTTP status response that proves the server rejected the request is `Rejected`;
- an SDK post error without a definitive HTTP rejection, including timeout or connection interruption, is `Uncertain` because the request may have reached the server.

No error path automatically rebuilds, re-signs, or reposts the order. Version, tick, nonce, and authentication errors are surfaced for operator action rather than retried.

## Response Classification and Position Accounting

A post result is a confirmed success only if all conditions hold:

- the SDK response has `success = true`;
- status is exactly `Matched`;
- `order_id` is non-empty;
- `making_amount` and `taking_amount` are positive and exactly equal to the amounts expected from the SDK-built FOK order;
- both amounts convert exactly into the neutral micro-unit receipt.

For a BUY, the receipt maps the actual matched response amounts to purchased shares and spent USD according to the SDK order side. For a SELL, it maps sold shares and received USD. The adapter must not assume that making is always USD or that taking is always shares; mapping is side-aware.

Only this receipt allows `OrderExecutor` or `PositionMonitor` to change the position store. The existing strategy estimate may be logged for comparison but is not the accounting source of truth.

Response outcomes are classified as follows:

| SDK/result condition | Classification | Position update | Circuit breaker |
| --- | --- | --- | --- |
| `success = true`, `Matched`, non-empty ID, exact full amounts | Success | Yes, actual amounts | Remains open |
| HTTP rejection, including 4xx/409/429/5xx with a definitive response | `Rejected` | No | Remains open |
| HTTP 200 with `success = false` | `Rejected` | No | Remains open |
| FOK canceled with explicit unsuccessful response | `Rejected` | No | Remains open |
| Timeout, disconnect, or post transport ambiguity | `Uncertain` | No | Halt globally |
| Malformed HTTP 200/success payload | `Uncertain` | No | Halt globally |
| `success = true` with `Live`, `Delayed`, unknown, or any non-`Matched` status | `Uncertain` | No | Halt globally |
| `success = true` but zero/partial/mismatched amounts or empty order ID | `Uncertain` | No | Halt globally |
| Exact receipt conversion failure after posting | `Uncertain` | No | Halt globally |

The SDK's complete `OrderStatusType` is matched exhaustively where possible. Any new or unrecognized successful status fails closed as `Uncertain`.

## Execution Circuit Breaker

The breaker is shared by copy entries and TP/SL exits. It provides an in-memory halt flag for immediate process-wide blocking and a JSON marker for restart persistence.

The marker contains no secrets and no full signed order. Its schema includes only:

```json
{
  "schema_version": 1,
  "halted_at": "RFC3339 timestamp",
  "reason_code": "stable sanitized code",
  "stage": "post_or_response",
  "token_id": "public token id",
  "side": "BUY or SELL",
  "order_id_hint": "optional redacted prefix/suffix"
}
```

On `Uncertain`, the caller must:

1. atomically set the in-memory halted state so concurrent submissions are rejected;
2. atomically write and sync a same-directory temporary marker;
3. persist/rename it to `execution_halt_path`;
4. stop the affected execution loop with a sanitized fatal error.

All later entry or exit attempts return `Halted` before SDK build/sign/post. A marker found at startup blocks live initialization.

The marker is never cleared automatically in phase 2. An operator must reconcile the potentially submitted order through phase 3 tooling or the official interface before manually removing the marker. Documentation must warn that deletion without reconciliation risks duplicate or contradictory orders.

If marker persistence fails after an uncertain post, the process remains halted in memory and terminates with a fatal sanitized error instructing the operator not to restart until manual reconciliation. Live startup's parent-directory writability check reduces this risk but does not eliminate power-loss or filesystem failure windows.

Phase 2 intentionally does not add a write-ahead in-flight order journal. Therefore a process or machine crash after bytes are sent but before an uncertainty marker is persisted remains an unresolved recovery gap. This is an explicit reason phase 2 completion does not authorize live trading; persistent in-flight tracking and reconciliation belong to phase 3.

## Entry Data Flow

### Dry-Run Entry

```text
whale fill
  -> strategy filters and risk sizing
  -> PlannedOrder(FOK intent)
  -> dry-run OrderExecutor
  -> paper position from planned values
  -> no SDK, no signature, no CLOB
```

### Live Entry

```text
whale fill
  -> strategy filters and risk sizing
  -> PlannedOrder(FOK intent)
  -> breaker check
  -> SDK metadata and tick validation
  -> SDK build
  -> SDK sign
  -> SDK post_order
  -> classify response
     -> confirmed full Matched: position from actual receipt
     -> Rejected: no position, continue according to existing loop policy
     -> Uncertain: no position, persist global halt, stop execution
```

## TP/SL Exit Data Flow

TP/SL monitoring is a live-only CLOB feature in phase 2. When enabled in a valid live runtime:

```text
open confirmed position
  -> midpoint monitor
  -> TP or SL condition
  -> PlannedOrder SELL/FOK
  -> shared breaker check
  -> shared SDK gateway
  -> classify response
     -> confirmed full Matched: close/reduce using actual receipt
     -> Rejected: keep position open
     -> Uncertain: keep local position unchanged, persist global halt, stop exits and entries
```

A rejected FOK exit leaves the position open for a future strategy evaluation, but the rejected order itself is not automatically retried inside the submission call. An uncertain exit must never mark the position closed.

## Error Handling and Redaction

Logs may contain only operationally safe fields:

- stage and stable error code;
- HTTP status when available;
- HTTP method and endpoint path without query secrets;
- token ID, side, original/aligned price, and public amount;
- redacted order ID prefix/suffix;
- a coarse SDK error kind selected by local code.

Logs, `Display`, `Debug`, marker files, tests, and Obsidian must not contain:

- private keys;
- API secret or passphrase;
- complete API key;
- `POLY_SIGNATURE` or raw EIP-712 signature;
- complete signed order JSON;
- L2 headers or HMAC input/output;
- raw response body;
- raw SDK error debug output when it can embed request/response data.

HTTP 4xx, 409, 429, and 5xx errors are tested with sentinel secrets in response bodies; those sentinels must not appear in rendered errors or captured logs.

## Removal and Compatibility Policy

There is one production order backend after phase 2: `SdkOrderGateway`.

- No runtime flag can select the old manual backend.
- No live caller can construct `SignedOrder`, compute L2 HMAC, or issue its own `POST /order`.
- Internal `OrderType::Fak` is renamed to `Fok`, and all production serialization is delegated to the SDK.
- Manual EIP-712/HMAC vectors may survive only in `#[cfg(test)]` fixtures or clearly isolated test modules when they prove that migration has not changed accepted EOA/FOK protocol semantics.
- Dead manual request structs, serializers, header builders, and posting methods are deleted once equivalent SDK tests pass.

## Testing Strategy

All behavior is implemented with RED -> GREEN TDD. The default test suite must work with `--offline --locked` and must never resolve a public hostname.

### Neutral Contract and Mapping Tests

- BUY and SELL side mapping.
- `Fok` is the only phase 2 execution type; no accidental FAK mapping.
- BUY tick alignment rounds down and SELL rounds up.
- Already-aligned prices are unchanged.
- Invalid/zero tick, invalid price, zero size, precision overflow, and too-small aligned orders fail before signing.
- Standard and neg-risk metadata match and mismatch behavior.
- Side-aware making/taking amount mapping.
- Decimal-to-micro-unit exact conversion and rejection of excess precision/overflow.
- Error `Display`/`Debug` and receipt output contain no sentinel secrets.

### OrderExecutor Tests with a Fake Gateway

- Strict dry-run has no gateway calls and still records the expected paper position.
- Live initialization requires a gateway and breaker.
- Confirmed full receipt updates positions from actual, not planned, amounts.
- `Preflight` and `Rejected` do not update positions and do not halt the breaker.
- `Uncertain` does not update positions, persists a marker, and blocks every subsequent entry.
- Existing marker blocks live initialization.
- Marker persistence failure leaves the in-memory breaker halted and returns a fatal error.
- A halted path is checked before every submission.
- No uncertain order is automatically retried.

### Official SDK Loopback Tests

Use a local loopback HTTP server and public fixture keys only. Supply fixture API credentials directly so authenticated client creation performs no L1 Create/Derive call.

- SDK requests protocol/tick/neg-risk metadata from the expected local endpoints.
- SDK builder emits V2 EOA order fields, Polygon chain ID 137, signer/funder identity, FOK order type, and the expected standard or neg-risk exchange.
- Signature recovery matches the public fixture signer.
- SDK L2 request authentication and `POST /order` reach the expected loopback endpoint.
- Supplied credentials cause zero L1 API-key requests.
- A matched response with exact amounts produces the neutral receipt.
- A response with `success = false` is `Rejected`.
- Definitive 4xx/409/429/5xx responses are `Rejected` and never expose body sentinels.
- Loopback disconnect, timeout, truncated/malformed 200 JSON, and malformed successful fields are `Uncertain`.
- `success = true` plus non-`Matched`, empty ID, partial, zero, or mismatched amounts is `Uncertain`.
- Each uncertain case produces one post attempt, one halt transition, and no retry.

### TP/SL Tests

- Confirmed full SELL receipt closes or reduces the position using actual amounts.
- Rejected exit leaves the position unchanged and breaker open.
- Uncertain exit leaves the position unchanged, persists the global halt, and blocks both exits and new entries.
- Strict dry-run does not start CLOB midpoint or TP/SL exit networking.

### Configuration and Startup Tests

- `trading.execution_halt_path` defaults to `execution-halt.json` and accepts a test override.
- Existing public configs remain `enable_trading = false` and `mock_trading = true`.
- Strict dry-run starts without private key/API credentials and creates no SDK client.
- Live startup rejects non-official host, wrong chain, non-EOA signature type, signer/funder mismatch, missing credentials, unwritable marker parent, and existing marker.
- Authentication is performed only from supplied credentials; live initialization never creates or derives them.

### Regression and Completion Gates

- `cargo test --all-targets --offline --locked`
- `cargo build --release --offline --locked`
- `cargo clippy --all-targets --offline --locked -- -D warnings`
- targeted formatting check for changed Rust files, plus a recorded whole-repository `cargo fmt --check` result without formatting unrelated files
- `git diff --check`
- scan tracked changes and captured logs for private-key/API/signature/body sentinels
- confirm public configs still disable real trading
- confirm no test requested a non-loopback network address
- confirm old production HMAC/signing/POST entry points are absent and no dual-backend switch exists

## Documentation Deliverables

- Update the English and Chinese README files with the strict dry-run boundary, official SDK order path, FOK semantics, halt marker, and remaining live-trading restrictions.
- Update configuration examples with `execution_halt_path` and safe operator guidance.
- Document that a halt marker requires external order reconciliation before manual removal.
- Record implementation commits, validation counts, and remaining phase 3 gates in the Obsidian project note, without storing any credential or raw private output.

## Acceptance Criteria

- All live order build, EIP-712 signing, L2 authentication, and posting use `polymarket_client_sdk_v2 = 0.6.0` through one `SdkOrderGateway`.
- No production code path can select or invoke the former manual order backend.
- Strict dry-run creates no SDK client, performs no CLOB call, and signs nothing.
- Both entries and TP/SL exits use FOK and share one breaker.
- Positions update only from an exact, fully matched SDK receipt using actual response amounts.
- Definitive rejection never changes positions and does not halt the runtime.
- Every ambiguous post/result state changes no position, is never retried, atomically persists the global halt when possible, and blocks future entries and exits.
- A persistent marker blocks live startup and is never automatically cleared.
- All new tests run offline or against loopback and all regression/completion gates pass.
- No real CLOB endpoint, real credential, real signature, or real order is used.
- Completion of phase 2 is explicitly documented as insufficient authorization for live trading because phase 3 reconciliation and account-capability work is still absent.

## Deferred Phase 3 Work

Phase 3 must address the operational gaps required before any controlled real-endpoint or real-funds evaluation:

- pUSD balance and allowance read-only checks;
- open-order and order-status queries;
- uncertain-order reconciliation;
- persistent in-flight intent/submission journaling;
- cancellation and idempotent recovery policy;
- safe operator tooling for inspecting and clearing a halt;
- no-funds real authentication and deterministic rejection tests, only after separate explicit authorization;
- low-balance isolated wallet limits and human confirmation before any later micro-value live test.

## References

- Official Rust V2 SDK repository: https://github.com/Polymarket/rs-clob-client-v2
- Official SDK version used by this project: `polymarket_client_sdk_v2 = 0.6.0`
- Phase 1 design: `docs/superpowers/specs/2026-08-17-official-sdk-auth-foundation-design.md`
- Phase 1 implementation plan: `docs/superpowers/plans/2026-08-17-official-sdk-auth-foundation.md`
