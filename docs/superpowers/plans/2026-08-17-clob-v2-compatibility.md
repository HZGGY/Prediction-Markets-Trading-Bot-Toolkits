# CLOB V2 Compatibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将本地 Rust CLOB 客户端迁移到 Polymarket CLOB V2 的 EOA（`signature_type=0`）订单签名与 JSON 请求格式，并用离线确定性测试证明签名字段和安全门正确。

**Architecture:** 保留现有 `OrderExecutor` 的交易安全门，只替换 `src/service/clob.rs` 的协议层。`config.rs` 负责显式解析账户签名类型；`clob.rs` 负责 V2 EIP-712 类型、domain、签名和 wire body；测试使用固定账户/固定 salt/固定 timestamp，不进行 HTTP 或链上调用。`1/2/3` 可解析但第一阶段在 EOA-only client 初始化时拒绝。

**Tech Stack:** Rust 2021, `alloy-sol-types` EIP-712, `alloy-signer-local`, `serde`, `tokio`, Cargo offline tests.

## Global Constraints

- 只支持第一阶段 EOA `signature_type=0`；`1/2/3` 只解析并明确拒绝，不实现代理/Safe/POLY_1271 链上验证。
- V2 exchange domain 使用 chain `137`、V2 exchange 地址、domain version `"2"`；CLOB API 认证 domain 不在本计划中改动。
- V2 signed order 不包含 `taker`、`expiration`、`nonce`、`feeRateBps`；`expiration` 只作为 POST wire body 的外层订单生命周期字段。
- Gamma `negRisk` 必须贯穿计划订单和纸面持仓；standard 与 neg-risk 市场分别使用各自的 V2 verifying contract。
- `timestamp` 使用 Unix epoch milliseconds；`metadata` 与 `builder` 第一阶段固定为 `bytes32` 零值。
- 不读取、写入、请求用户真实私钥、API key、secret、passphrase；不向 CLOB API 发起订单请求。
- 默认 `enable_trading=false`、`mock_trading=true`，现有 dry-run 行为不变。
- 所有生产代码行为先有失败测试，并观察到预期 RED 后再实现。

---

### Task 1: Add explicit signature type and V2 exchange configuration

**Files:**
- Modify: `src/config.rs` (`Credentials`, `CredentialsFile`, `AppConfig::load`)
- Modify: `src/service/clob.rs` (`SignatureType` parsing and client initialization)
- Modify: `config.json` (V2 exchange addresses and domain version)
- Modify: `config.yaml.example` (explicit `signature_type: 0` example)
- Test: `src/config.rs` tests and `src/service/clob.rs` tests

**Interfaces:**
- Produces `Credentials.signature_type: Option<u8>` for config loading.
- Produces `SignatureType::from_u8(value: u8) -> Result<SignatureType>` with mappings `0=Eoa`, `1=PolyProxy`, `2=PolyGnosisSafe`, `3=Poly1271`; unknown values return an error.
- Produces `SignatureType::is_supported_for_eoa_phase() -> bool`, true only for `Eoa`.
- `ClobClient::new` consumes the explicit value and rejects missing/unsupported signature types before constructing a live-capable signer.

- [ ] **Step 1: Write failing tests for explicit signature parsing**

```rust
#[test]
fn parses_all_known_polymarket_signature_types() {
    assert_eq!(SignatureType::from_u8(0).unwrap(), SignatureType::Eoa);
    assert_eq!(SignatureType::from_u8(1).unwrap(), SignatureType::PolyProxy);
    assert_eq!(SignatureType::from_u8(2).unwrap(), SignatureType::PolyGnosisSafe);
    assert_eq!(SignatureType::from_u8(3).unwrap(), SignatureType::Poly1271);
}

#[test]
fn rejects_unknown_signature_type() {
    let error = SignatureType::from_u8(9).unwrap_err();
    assert!(error.to_string().contains("unsupported signature type"));
}
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `cargo test --offline service::clob::tests::parses_all_known_polymarket_signature_types service::clob::tests::rejects_unknown_signature_type`

Expected: FAIL because `Poly1271` and `from_u8` do not yet exist.

- [ ] **Step 3: Implement minimal parsing and config plumbing**

Add `Poly1271`, `from_u8`, and `is_supported_for_eoa_phase`. Add `signature_type: Option<u8>` to the deserialized credentials shape and preserve `None` for the no-credentials dry-run path. In `ClobClient::new`, require `Some(0)` for a signer client and return an error containing the numeric type for `1/2/3` or missing live credentials; do not infer the value from `funder_address == signer.address()`.

Update public exchange config to the official V2 addresses and `domain_version: "2"`; update the credentials example with `signature_type: 0` without adding any real credential.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run: `cargo test --offline service::clob::tests service::config::tests`

Expected: PASS, with existing no-credential dry-run tests still constructing `AppConfig` successfully.

- [ ] **Step 5: Commit the task**

```text
git add src/config.rs src/service/clob.rs config.json config.yaml.example
git commit -m "feat: make CLOB account signature type explicit"
```

### Task 2: Replace the V1 EIP-712 order with the V2 signed order

**Files:**
- Modify: `src/service/clob.rs` (`sol! Order`, `SignedOrder`, `build_signed_order`)
- Test: `src/service/clob.rs` unit tests

**Interfaces:**
- `OrderV2` contains exactly: `salt`, `maker`, `signer`, `tokenId`, `makerAmount`, `takerAmount`, `side: uint8`, `signatureType: uint8`, `timestamp`, `metadata`, `builder`.
- `SignedOrder` serializes the V2 wire fields with `expiration` outside the signed order and keeps `signature` as a hex string.
- `build_signed_order` continues to accept `PlannedOrder`, `OrderType`, and expiration seconds so existing executor and TP/SL call sites remain unchanged.

- [ ] **Step 1: Write a failing test for V2 signed JSON shape**

```rust
#[test]
fn v2_signed_order_json_excludes_v1_fields() {
    let signed = fixture_v2_signed_order();
    let json = serde_json::to_value(OrderPostBody {
        order: signed,
        owner: "owner".into(),
        order_type: "GTC".into(),
    }).unwrap();
    let order = json.get("order").unwrap().as_object().unwrap();
    for legacy in ["taker", "nonce", "feeRateBps"] {
        assert!(!order.contains_key(legacy), "legacy field remained: {legacy}");
    }
    for v2 in ["timestamp", "metadata", "builder", "expiration"] {
        assert!(order.contains_key(v2), "V2 wire field missing: {v2}");
    }
}
```

- [ ] **Step 2: Run the test and verify RED**

Run: `cargo test --offline service::clob::tests::v2_signed_order_json_excludes_v1_fields`

Expected: FAIL because the current V1 `SignedOrder` still serializes `taker`, `nonce`, and `feeRateBps` and lacks V2 fields.

- [ ] **Step 3: Implement the V2 typed struct and wire model**

Replace the `sol!` struct with the exact V2 field list and `uint8` types for `side` and `signatureType`. Add `timestamp`, `metadata`, and `builder` to `SignedOrder`; keep `expiration` as the outer wire-only field. Use `B256::ZERO` for metadata/builder and compute timestamp in milliseconds. Keep BUY/SELL amount conversion unchanged except for comments referring to pUSD-compatible six-decimal collateral.

- [ ] **Step 4: Run the focused serialization test and verify GREEN**

Run: `cargo test --offline service::clob::tests::v2_signed_order_json_excludes_v1_fields`

Expected: PASS, with JSON containing `expiration`, `timestamp`, `metadata`, and `builder` and no V1-only fields.

- [ ] **Step 5: Commit the task**

```text
git add src/service/clob.rs
git commit -m "feat: model Polymarket CLOB v2 orders"
```

### Task 3: Add deterministic EIP-712 digest and signature fixtures

**Files:**
- Modify: `src/service/clob.rs` (test helpers and signing implementation)
- Test: `src/service/clob.rs` unit tests

**Interfaces:**
- Adds a private deterministic helper used only by tests to build an order with fixed salt and timestamp.
- Production `build_signed_order` uses V2 exchange config, current millisecond timestamp, zero metadata/builder, and EOA signature type `0`.

- [ ] **Step 1: Write a failing deterministic digest/signature test**

```rust
#[test]
fn v2_fixed_order_matches_known_digest_and_signature() {
    let (digest, signature) = sign_v2_fixture_with_fixed_salt_and_timestamp();
    assert_eq!(hex::encode(digest), FIXTURE_DIGEST_HEX);
    assert_eq!(hex::encode(signature.as_bytes()), FIXTURE_SIGNATURE_HEX);
}
```

The fixture implementation must define `FIXTURE_DIGEST_HEX` and `FIXTURE_SIGNATURE_HEX` as checked-in hexadecimal constants, use a fixed 32-byte private key only inside test code, a fixed salt, fixed timestamp, the official V2 exchange address, `domain_version="2"`, zero metadata and zero builder. The constants must come from an independent reference calculation before they are placed in the test, not be copied from the production helper.

- [ ] **Step 2: Run the test and verify RED**

Run: `cargo test --offline service::clob::tests::v2_fixed_order_matches_known_digest_and_signature`

Expected: FAIL because the current code signs the V1 type/domain and cannot produce the V2 fixture.

- [ ] **Step 3: Implement V2 domain and digest generation**

Use `eip712_domain!` with `domain_name`, `domain_version`, chain `137`, and the selected V2 exchange verifying contract. Call the V2 `Order` EIP-712 signing hash and sign that digest through the existing `PrivateKeySigner`. Do not include wire-only expiration in the signed struct.

- [ ] **Step 4: Run the deterministic fixture and mutation tests**

Run: `cargo test --offline service::clob::tests::v2_`

Expected: PASS for the fixed digest/signature and for tests proving that changing domain version, exchange address, timestamp, or metadata changes the digest.

- [ ] **Step 5: Commit the task**

```text
git add src/service/clob.rs
git commit -m "test: lock CLOB v2 EIP712 signature fixtures"
```

### Task 4: Centralize V2 POST serialization and preserve dry-run safety

**Files:**
- Modify: `src/service/clob.rs` (`OrderPostBody`, `post_order`)
- Test: `src/service/clob.rs` unit tests and existing order executor replay tests

**Interfaces:**
- Produces a private `serialize_order_request(signed: SignedOrder, funder: Address, order_type: OrderType) -> Result<String>` helper used by `post_order` and unit tests.
- `post_order` still posts to `/order` only when called through the existing live gate; it serializes a V2 order body and retains `owner` and `orderType`.
- `owner` is the L2 API Key. `l2_headers` emits `POLY_ADDRESS`, `POLY_SIGNATURE`, `POLY_TIMESTAMP`, `POLY_API_KEY`, and `POLY_PASSPHRASE`; its HMAC decodes the URL-safe Base64 API Secret and returns padded URL-safe Base64. No builder HMAC headers are added.

- [ ] **Step 1: Write a failing test for the centralized request serializer**

```rust
#[test]
fn serializes_v2_request_with_owner_and_order_type() {
    let body = serialize_order_request(fixture_v2_signed_order(), Address::from([0x11; 20]), OrderType::Gtc).unwrap();
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["owner"], "api-key-fixture");
    assert_eq!(json["orderType"], "GTC");
    assert!(json["order"]["timestamp"].is_string());
    assert!(json["order"]["expiration"].is_string());
    assert!(json["order"].get("nonce").is_none());
    assert!(json["order"].get("feeRateBps").is_none());
}
```

- [ ] **Step 2: Run the test and verify RED**

Run: `cargo test --offline service::clob::tests::serializes_v2_request_with_owner_and_order_type`

Expected: FAIL because the serializer helper does not yet exist.

- [ ] **Step 3: Implement the serializer and route `post_order` through it**

Construct `OrderPostBody` with the API Key as `owner`, serialize it once, and use the same exact string for the L2 HMAC prehash and HTTP request body. Decode the API Secret as URL-safe Base64 and retain output padding, matching the official V2 client. Keep no-credential dry-run behavior as `DryRunPlanned`; no test helper may call `reqwest` or the CLOB endpoint.

- [ ] **Step 4: Run focused tests and existing replay tests**

Run: `cargo test --offline service::clob::tests service::order_executor::tests`

Expected: PASS; the V2 wire shape is stable and dry-run replay still records paper positions without credentials.

- [ ] **Step 4a: Verify standard/neg-risk exchange routing**

Run: `cargo test --offline neg_risk`

Expected: PASS; Gamma metadata selects different standard and neg-risk V2 verifying contracts, and the property survives through position recording for exit signing.

- [ ] **Step 5: Commit the task**

```text
git add src/service/clob.rs
git commit -m "feat: centralize CLOB v2 order serialization"
```

### Task 5: Update documentation and run the complete verification gate

**Files:**
- Modify: `README.md` or `README.zh-CN.md` (only the credential/configuration section that currently describes V1 assumptions)
- Modify: `config.yaml.example` if Task 1 did not already include the final V2 notes
- Modify: `docs/superpowers/specs/2026-08-17-clob-v2-compatibility-design.md` only if implementation decisions change the approved scope
- Modify: `docs/superpowers/plans/2026-08-17-clob-v2-compatibility.md` to mark completed steps
- Modify: Obsidian `20-Prediction-Markets-Trading-Bot-Toolkits.md` after verification with a concise stable result; never include secrets

- [ ] **Step 1: Write/update the user-facing V2 safety notes**

Document that `signature_type: 0` is the only supported live account type in this phase, that `pUSD`/funding and API credential setup are separate prerequisites, and that default flags remain dry-run.

- [ ] **Step 2: Run formatting check without rewriting unrelated files**

Run: `cargo fmt --check`

Expected: report any repository-existing formatting differences; do not run full-format rewriting unless the diff is limited to touched files.

- [ ] **Step 3: Run all offline tests**

Run: `cargo test --offline`

Expected: all existing tests plus the new V2 tests pass with zero failures.

- [ ] **Step 4: Run release build and strict Clippy**

Run: `cargo build --release --offline`

Expected: exit code 0.

Run: `cargo clippy --all-targets -- -D warnings`

Expected: exit code 0 and no warnings.

- [ ] **Step 5: Verify no live network or secret mutation occurred**

Run: `git diff --stat`, `git diff --check`, and `git status --short`.

Confirm the diff contains no credentials, no `enable_trading=true` in default configs, no HTTP test pointed at Polymarket, and no order broadcast command.

- [ ] **Step 6: Commit the documentation and verification record**

```text
git add README.md README.zh-CN.md config.yaml.example docs/superpowers/plans/2026-08-17-clob-v2-compatibility.md
git commit -m "docs: record CLOB v2 compatibility verification"
```
