# Phase 3A Durable Execution Recovery Design

**Date:** 2026-08-18

**Status:** Approved section-by-section in chat; pending written-spec review
**Scope:** Offline/loopback durable execution ledger, reconstructable positions, exact-order reconciliation, single-order cancellation, and manual recovery acknowledgement

## Summary

Phase 2 moved Polymarket FOK execution to the official Rust V2 SDK, made uncertain POST outcomes fail closed, and persisted an `execution-halt.json` marker. It intentionally left a crash window: the process can terminate after POST bytes leave the machine but before the uncertainty marker is persisted. It also keeps positions only in memory, so every restart loses local position state.

Phase 3A closes those local durability gaps without using a real account or authorizing live trading. It introduces one append-only execution ledger as the durable source of truth for order lifecycle and position events, an atomically replaced active-intent snapshot, exact-order read-only reconciliation, narrowly scoped single-order cancellation, and explicit human-controlled recovery commands.

The implementation and acceptance work for this phase uses unit tests and loopback servers only. It does not call a public CLOB, Gamma, Polygon, authentication, signing-broadcast, or order endpoint. pUSD balance and allowance checks, real no-funds endpoint validation, and micro-value live trading remain separate phases.

## Approved Decisions

The user approved these binding decisions:

1. Phase 3 is decomposed; Phase 3A addresses durable recovery first.
2. Recovery is fail closed and fully human-unlocked. No reconciliation result automatically resumes trading.
3. An exact recovered FOK fill changes local positions only after explicit human confirmation.
4. Recovered position changes are idempotent and persisted atomically through the same execution ledger.
5. Cancellation is limited to a single exact order already present in the ledger and confirmed remotely as cancellable.
6. A cancellation response is never sufficient evidence by itself; the order must be queried again.
7. Storage uses an append-only JSONL event ledger plus an atomic active snapshot, not SQLite and not a single overwritten JSON state file.
8. The execution ledger is also the durable source for live positions; the in-memory position view is rebuilt from ledger events.

## Goals

- Persist a unique order intent and deterministic V2 order identifier before any order POST can begin.
- Ensure a process or machine crash at every submission boundary leaves enough local evidence to prevent an automatic repost.
- Rebuild open positions exactly from durable integer-micro-unit events after restart.
- Reconcile only by a proven exact order identifier and directly associated trades.
- Keep every missing, conflicting, partial, malformed, or unavailable result halted.
- Permit only human-confirmed, idempotent recovery of exact full FOK fills.
- Permit only human-confirmed cancellation of one known ledger order.
- Preserve a permanent, integrity-checked audit trail without storing credentials, signatures, or raw protocol payloads.
- Make direct deletion of `execution-halt.json` insufficient to bypass an unresolved ledger state.

## Non-Goals

- pUSD balance, available buying power, or allowance checks.
- Token approval, pUSD wrapping, deposits, withdrawals, or any on-chain transaction.
- Real CLOB, Gamma, Polygon, L1 authentication, L2 authentication, signing-broadcast, or order calls during implementation or acceptance.
- Automatic reconciliation, automatic cancellation, automatic position recovery, or automatic halt clearing.
- Market-wide cancellation, account-wide cancellation, order replacement, or order retry.
- Multiple concurrently submitted orders for one account.
- Multiple processes sharing one account ledger.
- Proxy, Safe, deposit-wallet, or POLY_1271 support; Phase 3A remains EOA-only.
- Real-funds or micro-value live testing.
- Ledger compaction or historical event deletion.

## Current System Gaps

### Crash gap

`ExecutionCircuitBreaker::submit_fok` currently calls the gateway and writes the halt marker only after the gateway returns `Uncertain`. A crash after the POST begins but before that write can leave no durable indication that an order may exist remotely.

### Volatile positions

`PositionStore` is an in-memory `HashMap`. Entry fills insert a position and TP/SL fills remove one, but no position event survives process restart.

### Insufficient recovery identity

The POST response returns the remote order ID, but that response may be lost. Safe crash recovery therefore requires proving the exact remote V2 order identifier before POST. Matching by token, side, price, size, or time is heuristic and forbidden.

### Two independent halt sources

Phase 2 has an `execution-halt.json` marker but no durable order state. Phase 3A must make the ledger authoritative while retaining the marker as a compatibility and operator-warning mirror.

## Architecture

### `ExecutionLedger`

`ExecutionLedger` is the only durable source of truth. It owns:

- append-only event persistence;
- sequence and hash-chain validation;
- exclusive process locking;
- active-intent snapshot derivation and atomic replacement;
- replay into an immutable projection;
- idempotency validation;
- safe startup refusal on any integrity or durability error.

The ledger file defaults to `execution-ledger.jsonl` through `trading.execution_ledger_path`. The following sibling paths are derived, not independently configured:

- `<ledger>.active.json` — atomically replaced active-intent snapshot;
- `<ledger>.lock` — exclusive live-process lock;
- temporary files created in the same directory for atomic replacement.

The JSONL ledger is authoritative. The active snapshot is an acceleration and safety mirror. Any mismatch between replayed ledger state and the snapshot halts startup; the program never silently rebuilds or overwrites a conflicting snapshot in live mode.

### `LedgerPositionStore`

`LedgerPositionStore` replaces the volatile store in live mode. It:

- rebuilds the current position map from accepted `PositionOpened` and `PositionClosed` events;
- exposes the existing read operations needed by risk checks and TP/SL;
- appends a position event before mutating the in-memory projection;
- rejects duplicate or conflicting events for the same `intent_id` and `order_id`;
- stores all quantities as integer micro-units and converts to floating point only at existing strategy/display boundaries.

Strict paper mode keeps the current isolated in-memory behavior. It does not open the live ledger, load credentials, create an SDK client, or contact a CLOB midpoint endpoint.

### `RecoveryGateway`

The neutral recovery interface exposes only exact-order operations:

```rust
#[async_trait]
pub trait RecoveryGateway: Send + Sync {
    async fn reconcile_exact(
        &self,
        order_id: &OrderId,
    ) -> Result<RemoteOrderEvidence, RecoveryError>;

    async fn cancel_exact(
        &self,
        order_id: &OrderId,
    ) -> Result<CancelAttemptEvidence, RecoveryError>;
}
```

`RemoteOrderEvidence`, `CancelAttemptEvidence`, `RecoveryError`, and all status enums are SDK-neutral and contain no raw response body or dynamic server error text. There is no market-cancel or cancel-all method in this trait.

### `SdkRecoveryGateway`

`SdkRecoveryGateway` is the only adapter from official SDK order/trade/cancel types to neutral evidence. It:

- uses the exact official V2 host in production;
- uses an injected loopback host only in tests;
- queries by exact order ID;
- queries directly associated trades when amounts are required;
- validates order ID, token, side, original amounts, FOK type, and trade association;
- performs at most one request per planned operation step and never retries automatically;
- maps every timeout, transport error, malformed response, unknown status, mismatch, or partial result to a sanitized uncertain result.

### `RecoveryService`

`RecoveryService` implements the operator workflow:

```text
inspect -> reconcile -> [prepare-cancel -> cancel -> reconcile]
                    \-> apply -> acknowledge
```

It validates state transitions against the ledger head, creates confirmation challenges, appends local recovery events, invokes the exact-order gateway only for explicit network commands, and never clears a halt implicitly.

### `ExecutionCircuitBreaker`

The breaker and the ledger share the existing submission mutex. Live submission is permitted only when:

- ledger validation succeeds;
- the exclusive lock is held;
- no active unresolved intent exists;
- no incompatible halt marker exists;
- the active snapshot matches the ledger projection.

An unresolved ledger state blocks execution even if a user manually deletes `execution-halt.json`.

## Ledger Model

### Event envelope

Every physical line is one complete JSON event with a terminating newline. The fixed envelope is:

```text
schema_version
sequence
event_id
intent_id
recorded_at
kind
payload
previous_hash
event_hash
```

- `schema_version` starts at `1`.
- `sequence` begins at `1` and increases by exactly one.
- `event_id` is globally unique and makes append retries idempotent.
- `intent_id` is a UUID generated once before order preparation.
- `recorded_at` is UTC with sub-second precision and is informational, never used as identity.
- `kind` is a closed versioned event discriminant.
- `payload` is a tagged schema with fixed fields for that event kind.
- `previous_hash` is zero for the first event and the preceding `event_hash` thereafter.
- `event_hash` is SHA-256 over a canonical serialization of all preceding envelope fields.

The hash chain detects accidental corruption and unexpected rewriting; it is not claimed to resist an attacker who controls both the application and files.

### Durable order identity

The immutable order identity stores:

- `intent_id`;
- exact V2 `order_id`/order hash;
- venue and protocol version;
- token ID;
- neg-risk flag;
- BUY or SELL side;
- FOK order type;
- expected making and taking amounts in micro-units;
- strategy source reference as an optional sanitized hash;
- whether the intent is an entry or exit;
- for exits, the exact position identity being closed.

It does not store a private key, API credential, passphrase, HMAC, EIP-712 signature, full signed request, raw request body, raw response body, or dynamic server message.

### Position identity

A durable position includes:

- stable `position_id` derived from the opening `intent_id`;
- opening order ID;
- token, slug, category, tags, neg-risk, and side;
- entry shares and USD notional in micro-units;
- entry price derived from those integer amounts;
- configured TP/SL percentages;
- opening timestamp;
- optional closing order ID and closing timestamp only after `PositionClosed`.

Token ID alone is not an idempotency key. Phase 3A may retain the current one-position-per-token strategy rule, but persistence and recovery use `position_id` plus opening/closing order IDs.

### Event kinds

Phase 3A defines these event families:

**Submission**

- `IntentPrepared`
- `SubmitStarted`
- `RemoteMatched`
- `RemoteRejected`
- `RemoteUncertain`
- `SubmissionCommitted`
- `SubmissionCommittedNoFill`

**Positions**

- `PositionOpened`
- `PositionClosed`

**Recovery**

- `ReconciliationStarted`
- `ReconciledMatched`
- `ReconciledNoFill`
- `ReconciledLive`
- `ReconciledPending`
- `ReconciledUncertain`
- `CancelStarted`
- `CancelResponseObserved`
- `RecoveryApplied`
- `Acknowledged`

Event payloads use stable enums and integer units. Unknown enum values or unknown schema versions halt replay.

## Durability and Integrity Rules

### Append protocol

For every event:

1. Hold the single-writer ledger mutex and exclusive process lock.
2. Validate the transition against the current projection.
3. Serialize one complete event in memory.
4. Append the event plus newline.
5. Flush the file.
6. Call `sync_all`.
7. Apply the event to the in-memory projection.
8. Atomically replace and synchronize the active snapshot when active state changes.

If any step fails, execution remains halted in memory and the process returns a sanitized fatal error. The code never reports a durable transition that did not complete.

### Startup replay

Startup opens the ledger without following a symlink/reparse target outside the intended directory, acquires the exclusive lock, then validates every event. These conditions are fatal and fail closed:

- missing newline or truncated final event;
- invalid JSON;
- unsupported schema;
- duplicate or skipped sequence;
- duplicate event ID with different content;
- broken previous hash or event hash;
- illegal state transition;
- duplicate/conflicting position event;
- active-snapshot mismatch;
- lock acquisition failure;
- unwritable parent or failed durability probe.

There is no automatic truncation, repair, compaction, or event deletion in Phase 3A.

### Active snapshot replacement

The snapshot contains only the current unresolved intent, ledger sequence, and ledger head hash. It is written to a same-directory temporary file, synchronized, atomically persisted over the target, and followed by the strongest available parent-directory synchronization for the platform.

A missing snapshot with an active ledger intent, or a snapshot that names a different head, is a halt condition. An absent snapshot is valid only when replay proves there is no active intent.

## Pre-POST Order-ID Proof Gate

Safe recovery depends on knowing the same order identifier the CLOB uses before POST. Phase 3A therefore has a load-bearing acceptance gate:

1. Derive the V2 order ID from the exact signed-order payload using the official algorithm and correct V2 EIP-712 domain.
2. Use official SDK public types when sufficient.
3. If the SDK lacks a public helper, add only a narrow audited helper to the existing vendored patch; do not duplicate the full order protocol in application code.
4. Validate the result against an official public vector or upstream fixture and independent local tests.
5. Verify that changing every identity-bearing V2 field changes the derived ID.
6. Verify that neither API credentials nor the signature itself are required to store the ID.

If the exact identifier cannot be independently proven, the ledger framework may be implemented and tested, but it must not be wired into a real submission path and Phase 3A must be reported incomplete for crash recovery.

## State Machine

### Normal entry or exit

```text
IntentPrepared
  -> SubmitStarted
  -> RemoteMatched
  -> PositionOpened | PositionClosed
  -> SubmissionCommitted
```

or:

```text
IntentPrepared
  -> SubmitStarted
  -> RemoteRejected
  -> SubmissionCommittedNoFill
```

Rules:

- Preflight failure before `IntentPrepared` creates no order event.
- `IntentPrepared` is durable before `SubmitStarted`.
- `SubmitStarted` is durably synchronized immediately before any POST bytes may be sent.
- There is no code path from `SubmitStarted` back to a second POST.
- A direct exact matched response is recorded before the position event.
- The position event is recorded before a committed terminal event.
- If persistence fails after POST may have begun, the process halts and exits; it does not mutate positions or retry.

### Crash recovery

On restart:

- `IntentPrepared` without `SubmitStarted` proves the local process did not begin POST under the enforced invariant. It is classified `NotSent`, but human acknowledgement is still required.
- `SubmitStarted`, `RemoteUncertain`, or any nonterminal later state is unresolved and blocks all bots.
- The program does not contact the network at startup. It instructs the operator to run an explicit recovery command.

### Reconciliation classification

| Evidence | Classification | Position action | May acknowledge? |
| --- | --- | --- | --- |
| Exact ID, exact positive full FOK fill, all amounts and associated trades consistent | `ReconciledMatched` | Human-confirmed `apply` required | Only after `RecoveryApplied` |
| Exact ID, explicit terminal canceled/invalid/rejected state, zero matched amount | `ReconciledNoFill` | None | Yes, with confirmation |
| Exact ID, still live | `ReconciledLive` | None | No |
| Exact ID, pending/delayed | `ReconciledPending` | None | No |
| 404/not found without stronger evidence | `ReconciledUncertain` | None | No |
| Partial fill, amount mismatch, wrong side/token/type, conflicting trades | `ReconciledUncertain` | None | No |
| Timeout, transport failure, malformed response, unknown status | `ReconciledUncertain` | None | No |

The reconciler may combine exact order and associated trade reads, but it may not infer identity from token, price, size, timestamp, or proximity.

### Human-confirmed position recovery

`recovery apply` is accepted only when:

- the latest state is `ReconciledMatched`;
- the challenge binds to the exact intent, action, and current ledger head;
- no position event already exists for the order;
- entry recovery does not conflict with another open position for the strategy's one-position-per-token rule;
- exit recovery names the exact durable position being closed;
- actual positive integer amounts are available and match the reconciled evidence.

The command appends `PositionOpened` or `PositionClosed`, synchronizes it, then appends `RecoveryApplied`. Re-running after a completed event is an idempotent no-op with a stable already-applied result. A conflicting duplicate is fatal.

### Single-order cancellation

Cancellation has its own in-flight state because DELETE can also become uncertain:

1. `prepare-cancel` performs a fresh exact-order query.
2. It permits a challenge only for the ledger's exact active order in a remotely cancellable state.
3. `cancel` validates that the ledger head has not changed.
4. It appends and synchronizes `CancelStarted` before sending DELETE.
5. It sends exactly one single-order cancellation request.
6. It appends `CancelResponseObserved` with sanitized canceled/not-canceled classification.
7. It immediately performs exact reconciliation.

The cancellation response is never terminal evidence. If DELETE or the follow-up query is uncertain, the system remains halted. Pending/delayed orders that the venue says cannot be canceled remain halted.

### Acknowledgement and halt clearing

`recovery acknowledge` is allowed only for:

- locally proven `NotSent`;
- `ReconciledNoFill`;
- `ReconciledMatched` followed by `RecoveryApplied`.

It is not allowed for live, pending, partial, mismatched, missing, unavailable, or unknown evidence.

The clearing order is deliberately fail closed:

1. Append and synchronize `Acknowledged`.
2. Atomically update the active snapshot to no active intent.
3. Remove the compatibility `execution-halt.json` marker.
4. Synchronize the marker directory where supported.

A crash between these steps can leave a harmless extra marker. Re-running `acknowledge` completes cleanup idempotently. Removing the marker manually never resolves an active ledger intent.

## Operator CLI

The new command group is:

```text
recovery inspect [--intent <id>] [--show-order-id]
recovery reconcile --intent <id>
recovery prepare-cancel --intent <id>
recovery cancel --intent <id> --confirm <challenge>
recovery apply --intent <id> --confirm <challenge>
recovery acknowledge --intent <id> --confirm <challenge>
```

### Local-only commands

`inspect`, `apply`, and `acknowledge` load public configuration and the local ledger only. They do not load the private-key/API-credential source and cannot construct an SDK client.

### Explicit network commands

`reconcile`, `prepare-cancel`, and `cancel` require credential loading. Running one of these commands is explicit authorization for only the named operation. No command authorizes a bot run, another API call, or a future retry.

During Phase 3A implementation and acceptance, these paths are exercised only against injected loopback servers with public fixtures. No production command is run against a public endpoint.

### Confirmation challenges

A confirmation challenge binds:

- action name;
- intent ID;
- exact order ID;
- current sequence and ledger head hash.

It is intended to prevent accidental operation, not to authenticate an attacker with local machine access. Any ledger state change invalidates the challenge. There is no `--force`, generic `--yes`, environment bypass, or hidden marker-clearing API.

### Output policy

- Tracing logs always use an order-ID hint.
- Normal `inspect` uses an order-ID hint.
- Full order ID is printed only to local command output after explicit `--show-order-id`.
- Stable error/status codes and static operator instructions are shown.
- Raw SDK errors, server bodies, signatures, credentials, and signed payloads are never shown.

## Interaction With Existing Runtime

### Strict paper mode

Strict paper mode returns before opening the live ledger, validating its directory, loading credentials, constructing an SDK gateway, or constructing a CLOB midpoint source. Paper positions remain isolated from the live ledger.

### Live-mode initialization

Before any live component is returned, initialization must:

1. validate the official host, EOA account, chain, and credentials as Phase 2 does;
2. open and exclusively lock the execution ledger;
3. replay and validate the complete ledger;
4. rebuild positions;
5. compare the active snapshot;
6. inspect the compatibility halt marker;
7. refuse startup if any unresolved or inconsistent state exists.

Phase 3A completion still does not authorize live use because account capability and controlled real-endpoint acceptance remain absent.

### Entry and TP/SL

Entries and exits use the same ledger, submission lock, gateway, breaker, and position projection. TP/SL cannot create a parallel ledger or bypass journaled submission. An uncertain exit preserves the durable open position.

## Error Model

New errors are stable typed categories, including:

- ledger unavailable, locked, corrupt, unsupported, or unsynchronized;
- snapshot missing or mismatched;
- illegal transition or idempotency conflict;
- exact order ID unavailable or unproven;
- reconciliation transport, timeout, malformed, not found, nonfinal, partial, or mismatch;
- cancellation not allowed, stale challenge, uncertain cancellation, or post-cancel mismatch;
- recovery not applicable, already applied, or position conflict;
- acknowledgement not allowed or halt cleanup incomplete.

Display and Debug implementations are manually redacted. Dynamic filesystem paths are reduced to a safe configured label in operator errors; raw server content is never attached.

## Security and Privacy

- No secret field is serializable into a ledger event.
- Signed SDK objects remain private adapter details and have redacted Debug.
- Event hashes never include credentials or signatures.
- The ledger stores trading history and is therefore sensitive even without credentials; it uses restrictive file creation where supported and inherits only the intended directory ACL on Windows.
- Ledger, active snapshot, and lock targets must remain in the configured ledger directory and must not follow an unexpected link/reparse target.
- Obsidian records only architecture, commit IDs, test counts, and safety status; it never stores a ledger, full order ID, credential, signature, body, or terminal transcript.
- Config examples remain `enable_trading = false` and `mock_trading = true` with no credential values.

## Testing Strategy

### Ledger tests

- first append and reopen;
- ordered multi-event replay;
- event/hash-chain determinism;
- duplicate retry with identical content;
- duplicate event ID with conflicting content;
- skipped/duplicate sequence;
- broken previous/current hash;
- invalid JSON, unknown schema, unknown event kind, and truncated tail;
- active-snapshot create, replace, absence, and mismatch;
- exclusive lock contention;
- unwritable path and injected flush, sync, persist, and directory-sync failures;
- symlink/reparse path rejection where testable on the host.

### Position tests

- rebuild entries and exits after restart;
- exact micro-unit preservation;
- duplicate apply is a no-op;
- conflicting duplicate is fatal;
- exit closes only the named durable position;
- uncertain/rejected orders never change positions;
- TP/SL sees the rebuilt position set.

### Crash-injection matrix

Tests interrupt or inject persistence failure immediately before and after:

- `IntentPrepared` append;
- `SubmitStarted` append;
- POST invocation;
- remote response evidence append;
- position event append;
- committed terminal append;
- cancel-start append;
- cancel response;
- recovery-apply append;
- acknowledgement append;
- active snapshot replacement;
- halt-marker removal.

Every restart must either reconstruct the exact committed position state or remain safely halted. No case may repost automatically.

### Recovery adapter loopback tests

- exact full matched BUY and SELL;
- explicit zero-fill rejected/canceled/invalid;
- live and pending;
- not found;
- partial fill;
- wrong ID/token/side/type;
- amount or associated-trade mismatch;
- unknown status;
- timeout, disconnect, malformed response, and unexpected HTTP class;
- exactly one request per operation step and no retries;
- no L1 create/derive API-key request;
- no raw-body sentinel in any rendered output.

### Cancellation loopback tests

- only the active ledger order is accepted;
- arbitrary IDs, market cancel, and cancel-all are unavailable;
- stale challenge is rejected before network;
- `CancelStarted` is durable before DELETE;
- canceled and not-canceled responses both trigger re-query;
- disconnect after DELETE remains uncertain without retry;
- post-cancel matched evidence routes to human position recovery;
- post-cancel zero-fill canceled evidence routes to human acknowledgement.

### CLI and credential-isolation tests

- local commands ignore malformed credential files and secret environment overrides;
- explicit network commands require the credential source;
- command parsing has no force/broad-cancel path;
- full order ID appears only with explicit local display flag;
- all normal logs and errors remain redacted.

### Final gates

- focused module tests;
- `cargo test --all-targets --offline --locked`;
- `cargo build --release --offline --locked`;
- `cargo clippy --all-targets --offline --locked -- -D warnings`;
- rustfmt on changed Rust files only;
- `git diff --check`;
- secret, retry, broad-cancel, raw SDK error/body, and live-config scans;
- independent specification and code review focused on crash ordering, idempotency, and privilege boundaries.

## Acceptance Criteria

- The exact V2 order ID is proven and durably stored before POST begins.
- `SubmitStarted` is synchronized before any POST bytes can be sent.
- No restart path automatically posts, retries, cancels, applies a position, or clears a halt.
- Ledger replay reconstructs exact durable positions.
- Every corruption, persistence failure, stale challenge, partial result, or ambiguity fails closed.
- Only an exact full FOK fill can lead to a position event.
- A recovered fill requires human confirmation and is idempotent.
- Cancellation targets only one exact known order, uses a fresh confirmation, and is followed by reconciliation.
- Acknowledgement is human-confirmed and limited to proven safe terminal states.
- Deleting the compatibility halt marker cannot bypass an active ledger intent.
- Strict paper mode remains credential-free, signing-free, and CLOB-free.
- No public endpoint or real credential is used during Phase 3A implementation or acceptance.
- Documentation explicitly states that Phase 3A does not authorize live trading.

## Deferred Phases

### Phase 3B — account capability

- pUSD balance and available buying power;
- standard and neg-risk exchange allowances;
- conditional-token allowance for exits;
- account/funder/signature-type consistency;
- reservation by existing open orders.

### Phase 3C — controlled real endpoint acceptance

After separate explicit authorization:

- no-funds real authentication;
- read-only balance/order queries;
- deterministic rejection tests that do not create an order;
- geoblock and account-capability confirmation;
- operational credential handling review.

### Phase 3D — isolated micro-value live evaluation

After a separate design and explicit human authorization:

- low-balance isolated EOA;
- hard notional and daily limits;
- human confirmation for each test order;
- monitored submit/reconcile/cancel exercise;
- rollback and incident procedure.

## Documentation and Memory

Implementation must update the English and Chinese README files with:

- the ledger path and operator commands;
- the exact manual recovery sequence;
- the prohibition on deleting the halt marker directly;
- the fact that Phase 3A is offline/loopback acceptance and not live authorization;
- the remaining Phase 3B–3D gates.

Obsidian may record the approved design path, implementation commits, test counts, review outcome, and remaining phase boundary. It must not contain ledger contents, full order identifiers, credentials, signatures, request/response bodies, or raw terminal output.

## References

- Official Rust V2 SDK: https://github.com/Polymarket/rs-clob-client-v2
- Official order management documentation: https://docs.polymarket.com/trading/manage-orders
- Official order lifecycle documentation: https://docs.polymarket.com/concepts/order-lifecycle
- Official user-order API reference: https://docs.polymarket.com/api-reference/trade/get-user-orders
- Phase 2 design: `docs/superpowers/specs/2026-08-18-official-sdk-order-migration-design.md`
- Phase 2 implementation plan: `docs/superpowers/plans/2026-08-18-official-sdk-order-migration.md`
