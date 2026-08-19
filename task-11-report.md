# Phase 3A Task 11 — Exact recovery cancellation

## Scope

Implemented the human-confirmed cancellation path for exactly one active
ledger-owned recovery order. The work remained offline: tests use only
temporary files and loopback fakes; no credentials, public endpoints,
signatures, or orders were used.

## Inherited-work audit

This task started from an uncommitted inherited partial diff. Its initial
cancel test baseline had two passing `prepare_cancel` tests, one invalid
cleanup-pending fixture, and an intentionally incomplete `cancel` method.
The original RED state is not claimed. The inherited cleanup fixture was
corrected and the new full-flow tests were first observed failing at the
placeholder `NotApplicable` implementation.

## Delivered behavior

- `prepare_cancel` makes one fresh exact reconciliation and produces a
  `Cancel` challenge only for exact `Live` evidence.
- Arbitrary, historical, and cleanup-pending IDs fail locally without a
  gateway request. Pending and uncertain fresh evidence produce no challenge.
- `cancel` validates the active owner, action, sequence, and ledger head before
  appending an event or calling the gateway.
- A valid cancellation persists `CancelStarted`, calls only `cancel_exact` for
  the prepared order ID, persists exactly one sanitized
  `CancelResponseObserved`, then performs one exact follow-up reconciliation.
- Canceled, not-canceled, and typed uncertain DELETE observations are durable.
  DELETE and follow-up transport errors remain fail-closed and are classified
  as sanitized uncertainty; neither path retries.
- Follow-up matched evidence offers only manual apply; zero-fill offers only
  manual acknowledgement; live, pending, and uncertain evidence offers no
  automatic action. No path auto-applies, acknowledges, resumes, or clears a
  halt.
- The operation lock is async-aware. Concurrent synchronous recovery mutation
  fails closed with `operation_busy` rather than blocking a Tokio worker.

## Coverage

Service tests cover fresh exact Live challenge, pending/uncertain refusal,
stale challenges before ledger/network changes, arbitrary/history/cleanup
rejection, exact order/request counts, `CancelStarted` before DELETE, all
DELETE response classes, all follow-up classification classes, event ordering,
redaction, replay of uncertain cancellation state, and fail-closed concurrency.

Existing SDK loopback tests additionally prove one `DELETE /order` with one
exact `orderID`, no `orderIDs`, no cancel-all/market endpoint, conservative
response parsing, and timeout/disconnect/malformed-response no-retry behavior.

## Verification

- `cargo test --all-targets --offline --locked` — 286 library tests and 7
  binary tests passed.
- `cargo test --doc --offline --locked` — 7 doctests passed.
- `cargo check --all-targets --offline --locked` — passed.
- `cargo build --release --offline --locked --quiet` — passed.
- `rustfmt --edition 2021 --check` on all three changed Rust files — passed.
- `git diff --check` — passed.
- Strict Clippy was run. The only remaining failure is the pre-existing,
  unmodified `clippy::field_reassign_with_default` at
  `src/service/clob_sdk_orders.rs:255`. With only that one lint suppressed,
  `cargo clippy --all-targets --offline --locked -- -D warnings
  -A clippy::field_reassign_with_default` passed; Task 11 introduced no
  Clippy findings.

## Handoff

The branch is intentionally unmerged and unpushed, ready for independent
review. No production network operation occurred.
