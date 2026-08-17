# Official SDK Authentication Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Polymarket's official Rust V2 SDK as an isolated L1 authentication adapter, expose explicit create/derive API-key commands, and atomically update an existing credentials YAML without exposing secrets or touching the existing order path.

**Architecture:** Keep `src/service/clob.rs` unchanged in this phase. Add `src/service/clob_auth.rs` as the only module that imports official SDK types, keep credential persistence in `src/config.rs`, and let `src/main.rs` orchestrate explicit auth commands. Production authentication is restricted to the exact official V2 host; tests inject a loopback host and never access the public internet.

**Tech Stack:** Rust 1.97.1, Tokio, Clap 4, Serde YAML, `tempfile`, `polymarket_client_sdk_v2` 0.6 with CLOB support, existing Anyhow/Tracing test conventions.

## Global Constraints

- This plan implements only phase 1 of the confirmed three-phase official SDK migration.
- Do not replace or modify the existing order construction, EIP-712 order signing, L2 HMAC, POST, position, TP/SL, dry-run, or risk paths in `src/service/clob.rs`.
- Support EOA accounts only: `signature_type=0`, and funder must equal the signer address.
- Production L1 authentication must reject every host except exact `https://clob-v2.polymarket.com`.
- `create-api-key` and `derive-api-key` are explicit operations; never fall back from one to the other.
- Never print or log a complete private key, API key, API secret, or passphrase.
- The credentials target must already exist; never create a new file containing a private key and never create a backup containing credentials.
- Keep `enable_trading=false` and `mock_trading=true` in committed configurations.
- Do not execute the new auth commands against the real CLOB during implementation or verification.
- Network access during implementation is limited to downloading the approved Cargo dependencies after explicit user permission; all behavioral tests use loopback only and all final verification commands run `--offline`.
- Do not run whole-repository formatting; preserve the known pre-existing `cargo fmt --check` baseline and format only files intentionally changed by this plan.

---

### Task 1: Pin official SDK dependencies and correct the V2 host contract

**Files:**
- Modify: `Cargo.toml`
- Modify: `config.json`
- Modify: `config.dryrun-public.json`
- Modify/Test: `src/config.rs`

**Interfaces:**
- Consumes: existing `AppConfig`, `config.json`, and `config.dryrun-public.json`.
- Produces: `OFFICIAL_CLOB_V2_HOST: &str` and a committed configuration invariant used by the auth adapter in Task 3.

- [ ] **Step 1: Add a failing committed-config invariant test**

Append this test module to `src/config.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn committed_configs_use_official_v2_host_and_remain_locked() {
        for raw in [
            include_str!("../config.json"),
            include_str!("../config.dryrun-public.json"),
        ] {
            let cfg: AppConfig = serde_json::from_str(raw).unwrap();
            assert_eq!(
                cfg.site.clob_api_base,
                "https://clob-v2.polymarket.com"
            );
            assert!(!cfg.bot.enable_trading);
            assert!(cfg.bot.mock_trading);
            assert!(cfg.credentials.api_key.is_none());
            assert!(cfg.credentials.api_secret.is_none());
            assert!(cfg.credentials.api_passphrase.is_none());
        }
    }
}
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```powershell
cargo test --offline config::tests::committed_configs_use_official_v2_host_and_remain_locked
```

Expected: FAIL because both committed configs still contain `https://clob.polymarket.com`.

- [ ] **Step 3: Update the V2 host and add the two approved dependencies**

Change only `site.clob_api_base` in both JSON files:

```json
"clob_api_base": "https://clob-v2.polymarket.com"
```

Add the shared production constant near `SiteConfig`:

```rust
pub const OFFICIAL_CLOB_V2_HOST: &str = "https://clob-v2.polymarket.com";
```

Add to `Cargo.toml`:

```toml
# Official Polymarket V2 SDK — phase 1 uses only L1 CLOB authentication.
polymarket_client_sdk_v2 = { version = "0.6", default-features = false, features = ["clob"] }

# Same-directory atomic persistence for the gitignored credentials YAML.
tempfile = "3"
```

Do not alter `clob_wss_url`, order code, or trading flags.

- [ ] **Step 4: Fetch dependencies only after explicit network approval**

Run with the tool's network escalation and explain that this contacts Cargo registries only:

```powershell
cargo fetch
```

Do not run an auth command and do not contact a Polymarket endpoint.

- [ ] **Step 5: Run the focused test offline and verify GREEN**

Run:

```powershell
cargo test --offline config::tests::committed_configs_use_official_v2_host_and_remain_locked
```

Expected: PASS.

- [ ] **Step 6: Commit Task 1**

```powershell
git add -- Cargo.toml config.json config.dryrun-public.json src/config.rs
git commit -m "build: add official Polymarket V2 SDK"
```

---

### Task 2: Add validated atomic credential persistence

**Files:**
- Modify/Test: `src/config.rs`

**Interfaces:**
- Consumes: an existing credentials YAML path and `ApiCredentialUpdate<'_>`.
- Produces: `pub fn persist_api_credentials(path: &Path, update: ApiCredentialUpdate<'_>) -> Result<()>`.

- [ ] **Step 1: Write a failing success-path persistence test**

Add a test that uses the complete wished-for API; do not add the production type or function yet:

```rust
#[test]
fn persists_api_credentials_without_changing_account_or_unknown_fields() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.yaml");
    let before = r#"bot:
  private_key: fixture-private-key
  funder_address: 0x1111111111111111111111111111111111111111
  signature_type: 0
  custom_field: keep-me
top_level_custom: 42
"#;
    std::fs::write(&path, before).unwrap();

    persist_api_credentials(
        &path,
        ApiCredentialUpdate {
            api_key: "00000000-0000-0000-0000-000000000000",
            api_secret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            api_passphrase: "fixture-passphrase",
        },
    )
    .unwrap();

    let value: serde_yaml::Value =
        serde_yaml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(value["bot"]["private_key"], "fixture-private-key");
    assert_eq!(value["bot"]["custom_field"], "keep-me");
    assert_eq!(value["top_level_custom"], 42);
    assert_eq!(
        value["bot"]["api_key"],
        "00000000-0000-0000-0000-000000000000"
    );
    assert_eq!(
        value["bot"]["api_secret"],
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
    );
    assert_eq!(value["bot"]["api_passphrase"], "fixture-passphrase");
}
```

- [ ] **Step 2: Run the focused test and verify RED**

```powershell
cargo test --offline config::tests::persists_api_credentials_without_changing_account_or_unknown_fields
```

Expected: compile failure because `persist_api_credentials` does not exist.

- [ ] **Step 3: Implement only the success path**

Add these imports and helpers in `src/config.rs`:

```rust
use anyhow::{anyhow, Context, Result};
use serde_yaml::{Mapping, Value};
use std::io::Write as _;

pub struct ApiCredentialUpdate<'a> {
    pub api_key: &'a str,
    pub api_secret: &'a str,
    pub api_passphrase: &'a str,
}

fn yaml_key(name: &str) -> Value {
    Value::String(name.to_owned())
}

fn required_mapping<'a>(mapping: &'a mut Mapping, key: &str) -> Result<&'a mut Mapping> {
    mapping
        .get_mut(&yaml_key(key))
        .and_then(Value::as_mapping_mut)
        .ok_or_else(|| anyhow!("credentials YAML must contain a '{key}' mapping"))
}
```

Implement the public entry point for a valid existing file. Do not add the explicit missing-file check, empty-field validation, or injectable persist helper yet; those behaviors get their own RED cycles below:

```rust
pub fn persist_api_credentials(path: &Path, update: ApiCredentialUpdate<'_>) -> Result<()> {
    let original = std::fs::read(path)
        .with_context(|| format!("reading credentials from {}", path.display()))?;
    let mut root: Value = serde_yaml::from_slice(&original).context("parsing config.yaml")?;
    let root_mapping = root
        .as_mapping_mut()
        .ok_or_else(|| anyhow!("credentials YAML root must be a mapping"))?;
    let bot = required_mapping(root_mapping, "bot")?;

    for required in ["private_key", "funder_address", "signature_type"] {
        if !bot.contains_key(&yaml_key(required)) {
            return Err(anyhow!("credentials YAML is missing bot.{required}"));
        }
    }
    bot.insert(yaml_key("api_key"), Value::String(update.api_key.to_owned()));
    bot.insert(
        yaml_key("api_secret"),
        Value::String(update.api_secret.to_owned()),
    );
    bot.insert(
        yaml_key("api_passphrase"),
        Value::String(update.api_passphrase.to_owned()),
    );

    let rendered = serde_yaml::to_string(&root).context("serializing config.yaml")?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("credentials path has no parent directory"))?;
    let mut temp = tempfile::Builder::new()
        .prefix(".polymarket-credentials-")
        .tempfile_in(parent)
        .context("creating temporary credentials file")?;
    temp.as_file()
        .set_permissions(std::fs::metadata(path)?.permissions())?;
    temp.write_all(rendered.as_bytes())?;
    temp.flush()?;
    temp.as_file().sync_all()?;
    temp.persist(path)
        .map_err(|error| anyhow!(error.error))?;
    Ok(())
}
```

- [ ] **Step 4: Run the focused success test and verify GREEN**

```powershell
cargo test --offline config::tests::persists_api_credentials_without_changing_account_or_unknown_fields
```

Expected: PASS.

- [ ] **Step 5: Write a failing missing-file test**

```rust
#[test]
fn refuses_to_create_a_missing_credentials_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("missing.yaml");
    let result = persist_api_credentials(
        &path,
        ApiCredentialUpdate {
            api_key: "key",
            api_secret: "secret",
            api_passphrase: "passphrase",
        },
    );
    assert!(result.unwrap_err().to_string().contains("must already exist"));
    assert!(!path.exists());
}
```

- [ ] **Step 6: Run the missing-file test and verify RED**

```powershell
cargo test --offline config::tests::refuses_to_create_a_missing_credentials_file
```

Expected: FAIL because the current read error does not provide the required precondition message.

- [ ] **Step 7: Add the explicit existing-file precondition and verify GREEN**

Add this at the start of `persist_api_credentials`:

```rust
if !path.is_file() {
    return Err(anyhow!(
        "credentials file must already exist: {}",
        path.display()
    ));
}
```

Run the focused test again. Expected: PASS.

- [ ] **Step 8: Write a failing empty-field rollback test**

```rust

#[test]
fn rejects_empty_api_fields_without_changing_original_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.yaml");
    let before = b"bot:\n  private_key: key\n  funder_address: addr\n  signature_type: 0\n";
    std::fs::write(&path, before).unwrap();
    let result = persist_api_credentials(
        &path,
        ApiCredentialUpdate {
            api_key: "",
            api_secret: "secret",
            api_passphrase: "passphrase",
        },
    );
    assert!(result.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), before);
}
```

- [ ] **Step 9: Run the empty-field test and verify RED**

```powershell
cargo test --offline config::tests::rejects_empty_api_fields_without_changing_original_bytes
```

Expected: FAIL because the success-path implementation writes an empty API key.

- [ ] **Step 10: Add non-empty validation and verify GREEN**

Add before reading the file:

```rust
for (name, value) in [
    ("api_key", update.api_key),
    ("api_secret", update.api_secret),
    ("api_passphrase", update.api_passphrase),
] {
    if value.trim().is_empty() {
        return Err(anyhow!("{name} must not be empty"));
    }
}
```

Run the focused test again. Expected: PASS.

- [ ] **Step 11: Write a failing persist-failure rollback test**

This test introduces the wished-for internal injection seam, which does not exist yet:

```rust

#[test]
fn simulated_persist_failure_keeps_original_and_cleans_tempfile() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.yaml");
    let before = b"bot:\n  private_key: key\n  funder_address: addr\n  signature_type: 0\n";
    std::fs::write(&path, before).unwrap();
    let result = persist_api_credentials_with(
        &path,
        ApiCredentialUpdate {
            api_key: "key",
            api_secret: "secret",
            api_passphrase: "passphrase",
        },
        |_temp, _target| Err(anyhow!("simulated persist failure")),
    );
    assert!(result.unwrap_err().to_string().contains("simulated persist failure"));
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
}
```

- [ ] **Step 12: Run the persist-failure test and verify RED**

```powershell
cargo test --offline config::tests::simulated_persist_failure_keeps_original_and_cleans_tempfile
```

Expected: compile failure because `persist_api_credentials_with` does not exist.

- [ ] **Step 13: Extract the injectable persist seam and verify GREEN**

Make the public function delegate to the private helper without changing successful behavior:

```rust
pub fn persist_api_credentials(path: &Path, update: ApiCredentialUpdate<'_>) -> Result<()> {
    persist_api_credentials_with(path, update, |temp, target| {
        temp.persist(target)
            .map(|_| ())
            .map_err(|error| anyhow!(error.error))
    })
}

fn persist_api_credentials_with<F>(
    path: &Path,
    update: ApiCredentialUpdate<'_>,
    persist: F,
) -> Result<()>
where
    F: FnOnce(tempfile::NamedTempFile, &Path) -> Result<()>,
{
    for (name, value) in [
        ("api_key", update.api_key),
        ("api_secret", update.api_secret),
        ("api_passphrase", update.api_passphrase),
    ] {
        if value.trim().is_empty() {
            return Err(anyhow!("{name} must not be empty"));
        }
    }
    if !path.is_file() {
        return Err(anyhow!(
            "credentials file must already exist: {}",
            path.display()
        ));
    }

    let original = std::fs::read(path)
        .with_context(|| format!("reading credentials from {}", path.display()))?;
    let mut root: Value = serde_yaml::from_slice(&original).context("parsing config.yaml")?;
    let root_mapping = root
        .as_mapping_mut()
        .ok_or_else(|| anyhow!("credentials YAML root must be a mapping"))?;
    let bot = required_mapping(root_mapping, "bot")?;
    for required in ["private_key", "funder_address", "signature_type"] {
        if !bot.contains_key(&yaml_key(required)) {
            return Err(anyhow!("credentials YAML is missing bot.{required}"));
        }
    }
    bot.insert(yaml_key("api_key"), Value::String(update.api_key.to_owned()));
    bot.insert(
        yaml_key("api_secret"),
        Value::String(update.api_secret.to_owned()),
    );
    bot.insert(
        yaml_key("api_passphrase"),
        Value::String(update.api_passphrase.to_owned()),
    );

    let rendered = serde_yaml::to_string(&root).context("serializing config.yaml")?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("credentials path has no parent directory"))?;
    let mut temp = tempfile::Builder::new()
        .prefix(".polymarket-credentials-")
        .tempfile_in(parent)
        .context("creating temporary credentials file")?;
    temp.as_file()
        .set_permissions(std::fs::metadata(path)?.permissions())?;
    temp.write_all(rendered.as_bytes())?;
    temp.flush()?;
    temp.as_file().sync_all()?;
    persist(temp, path)?;
    Ok(())
}
```

- [ ] **Step 14: Run all config tests and verify GREEN**

```powershell
cargo test --offline config::tests
```

Expected: all config tests PASS.

- [ ] **Step 15: Commit Task 2**

```powershell
git add -- src/config.rs
git commit -m "feat: atomically persist CLOB API credentials"
```

---

### Task 3: Add the isolated official SDK L1 authentication adapter

**Files:**
- Create/Test: `src/service/clob_auth.rs`
- Modify: `src/service/mod.rs`

**Interfaces:**
- Consumes: `AppConfig`, `OFFICIAL_CLOB_V2_HOST`, official SDK `LocalSigner`, `Client`, and `Credentials`.
- Produces: `ApiKeyAction`, `AuthRequest`, `ensure_official_v2_host`, and `obtain_api_credentials`.

- [ ] **Step 1: Write a failing exact-host validation test**

Create `src/service/clob_auth.rs` with tests that describe the public API before implementation:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_auth_accepts_only_exact_official_v2_host() {
        assert!(ensure_official_v2_host(OFFICIAL_CLOB_V2_HOST).is_ok());
        for rejected in [
            "http://clob-v2.polymarket.com",
            "https://clob.polymarket.com",
            "https://clob-v2.polymarket.com.evil.example",
            "https://clob-v2.polymarket.com/extra",
        ] {
            assert!(ensure_official_v2_host(rejected).is_err(), "accepted {rejected}");
        }
    }

}
```

Use the same public test-only private keys already present in `src/service/clob.rs`; never copy a user key into tests.

- [ ] **Step 2: Register the module and verify RED**

Add to `src/service/mod.rs`:

```rust
pub mod clob_auth;
```

Run:

```powershell
cargo test --offline service::clob_auth::tests::production_auth_accepts_only_exact_official_v2_host
```

Expected: compile failure because the wished-for API is not implemented.

- [ ] **Step 3: Implement only the public types and exact host validation**

Use only official SDK types inside this module:

```rust
use anyhow::{anyhow, Result};

use crate::config::OFFICIAL_CLOB_V2_HOST;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKeyAction {
    Create,
    Derive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthRequest {
    pub action: ApiKeyAction,
    pub nonce: Option<u32>,
}

pub fn ensure_official_v2_host(host: &str) -> Result<()> {
    if host == OFFICIAL_CLOB_V2_HOST {
        Ok(())
    } else {
        Err(anyhow!("L1 authentication is restricted to the official CLOB V2 host"))
    }
}
```

- [ ] **Step 4: Run the exact-host test and verify GREEN**

```powershell
cargo test --offline service::clob_auth::tests::production_auth_accepts_only_exact_official_v2_host
```

Expected: PASS without opening a socket.

- [ ] **Step 5: Write failing loopback endpoint/header tests**

First add this validation test, then add the loopback fixture and endpoint tests below. They intentionally refer to functions that do not exist yet:

```rust
fn fixture_config() -> AppConfig {
    let mut cfg: AppConfig = serde_json::from_str(include_str!("../../config.json")).unwrap();
    cfg.credentials.private_key = PUBLIC_HARDHAT_KEY.to_owned();
    cfg.credentials.funder_address =
        "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266".to_owned();
    cfg.credentials.signature_type = Some(0);
    cfg
}

#[tokio::test]
async fn rejects_non_eoa_before_any_request() {
    let mut cfg = fixture_config();
    cfg.credentials.signature_type = Some(2);
    let error = obtain_api_credentials(
        &cfg,
        AuthRequest {
            action: ApiKeyAction::Create,
            nonce: None,
        },
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("EOA"));
}
```

Add this loopback fixture to the test module:

```rust
use std::collections::HashMap;
use std::str::FromStr as _;

use polymarket_client_sdk_v2::auth::{Credentials, LocalSigner, Signer as _, Uuid};
use polymarket_client_sdk_v2::clob::{Client, Config as SdkConfig};
use polymarket_client_sdk_v2::{AMOY, POLYGON};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;

const PUBLIC_HARDHAT_KEY: &str =
    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const CREDENTIAL_RESPONSE: &str = r#"{
  "apiKey":"00000000-0000-0000-0000-000000000000",
  "secret":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
  "passphrase":"fixture-passphrase"
}"#;

#[derive(Debug)]
struct CapturedRequest {
    request_line: String,
    headers: HashMap<String, String>,
}

async fn spawn_scripted_server(
    responses: Vec<(&'static str, &'static str)>,
) -> (String, tokio::task::JoinHandle<Vec<CapturedRequest>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let mut captured = Vec::with_capacity(responses.len());
        for (status, body) in responses {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = vec![0_u8; 16 * 1024];
            let count = stream.read(&mut buffer).await.unwrap();
            let raw = String::from_utf8(buffer[..count].to_vec()).unwrap();
            let mut lines = raw.split("\r\n");
            let request_line = lines.next().unwrap().to_owned();
            let headers = lines
                .take_while(|line| !line.is_empty())
                .filter_map(|line| line.split_once(':'))
                .map(|(name, value)| {
                    (name.to_ascii_lowercase(), value.trim().to_owned())
                })
                .collect();
            captured.push(CapturedRequest {
                request_line,
                headers,
            });

            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();
        }
        captured
    });
    (format!("http://{address}"), handle)
}

fn hardhat_signer(chain_id: u64) -> LocalSigner {
    LocalSigner::from_str(PUBLIC_HARDHAT_KEY)
        .unwrap()
        .with_chain_id(Some(chain_id))
}

#[tokio::test]
async fn create_uses_only_post_api_key_with_l1_headers_and_nonce() {
    let (host, server) =
        spawn_scripted_server(vec![("200 OK", CREDENTIAL_RESPONSE)]).await;
    let client = Client::new(&host, SdkConfig::default()).unwrap();
    let signer = hardhat_signer(POLYGON);

    let credentials = obtain_with_client(
        client,
        &signer,
        AuthRequest {
            action: ApiKeyAction::Create,
            nonce: Some(23),
        },
    )
    .await
    .unwrap();
    let captured = server.await.unwrap();

    assert_eq!(credentials.key(), Uuid::nil());
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].request_line, "POST /auth/api-key HTTP/1.1");
    assert_eq!(captured[0].headers["poly_nonce"], "23");
    assert_eq!(
        captured[0].headers["poly_address"],
        "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"
    );
    assert!(!captured[0].headers["poly_signature"].is_empty());
    assert!(!captured[0].headers["poly_timestamp"].is_empty());
}

#[tokio::test]
async fn derive_uses_only_get_derive_api_key_with_l1_headers_and_nonce() {
    let (host, server) =
        spawn_scripted_server(vec![("200 OK", CREDENTIAL_RESPONSE)]).await;
    let client = Client::new(&host, SdkConfig::default()).unwrap();
    let signer = hardhat_signer(POLYGON);

    obtain_with_client(
        client,
        &signer,
        AuthRequest {
            action: ApiKeyAction::Derive,
            nonce: Some(23),
        },
    )
    .await
    .unwrap();
    let captured = server.await.unwrap();

    assert_eq!(captured.len(), 1);
    assert_eq!(
        captured[0].request_line,
        "GET /auth/derive-api-key HTTP/1.1"
    );
    assert_eq!(captured[0].headers["poly_nonce"], "23");
}

#[tokio::test]
async fn create_status_error_does_not_fall_back_to_derive() {
    let leaked_body = r#"{"error":"fixture-secret-must-not-leak"}"#;
    let (host, server) = spawn_scripted_server(vec![("409 Conflict", leaked_body)]).await;
    let client = Client::new(&host, SdkConfig::default()).unwrap();
    let signer = hardhat_signer(POLYGON);

    let error = obtain_with_client(
        client,
        &signer,
        AuthRequest {
            action: ApiKeyAction::Create,
            nonce: None,
        },
    )
    .await
    .unwrap_err()
    .to_string();
    let captured = server.await.unwrap();

    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].request_line, "POST /auth/api-key HTTP/1.1");
    assert!(!error.contains("fixture-secret-must-not-leak"));
}

#[tokio::test]
async fn official_l1_signature_vector_is_preserved_through_the_sdk_request() {
    let (host, server) = spawn_scripted_server(vec![
        ("200 OK", "10000000"),
        ("200 OK", CREDENTIAL_RESPONSE),
    ])
    .await;
    let sdk_config = SdkConfig::builder().use_server_time(true).build();
    let client = Client::new(&host, sdk_config).unwrap();
    let signer = hardhat_signer(AMOY);

    obtain_with_client(
        client,
        &signer,
        AuthRequest {
            action: ApiKeyAction::Create,
            nonce: Some(23),
        },
    )
    .await
    .unwrap();
    let captured = server.await.unwrap();

    assert_eq!(captured.len(), 2);
    assert_eq!(captured[0].request_line, "GET /time HTTP/1.1");
    assert_eq!(captured[1].headers["poly_timestamp"], "10000000");
    assert_eq!(
        captured[1].headers["poly_signature"],
        "0xf62319a987514da40e57e2f4d7529f7bac38f0355bd88bb5adbb3768d80de6c1682518e0af677d5260366425f4361e7b70c25ae232aff0ab2331e2b164a1aedc1b"
    );
}

#[test]
fn empty_sdk_credential_fields_are_rejected() {
    let empty_secret = Credentials::new(
        Uuid::nil(),
        String::new(),
        "fixture-passphrase".to_owned(),
    );
    assert!(validate_credentials(&empty_secret).is_err());
}
```

Before writing production changes for these tests, state which dispatch arm or missing helper will make each test fail.

- [ ] **Step 6: Run each endpoint test and verify RED**

```powershell
cargo test --offline service::clob_auth::tests::create_uses_only_post_api_key_with_l1_headers_and_nonce
cargo test --offline service::clob_auth::tests::derive_uses_only_get_derive_api_key_with_l1_headers_and_nonce
cargo test --offline service::clob_auth::tests::create_status_error_does_not_fall_back_to_derive
cargo test --offline service::clob_auth::tests::official_l1_signature_vector_is_preserved_through_the_sdk_request
cargo test --offline service::clob_auth::tests::rejects_non_eoa_before_any_request
cargo test --offline service::clob_auth::tests::empty_sdk_credential_fields_are_rejected
```

Expected: each test fails for the missing loopback client injection or incorrect dispatch behavior, not for external DNS/network access.

- [ ] **Step 7: Implement signer validation, explicit SDK dispatch, sanitized errors, and returned-credential validation**

Add these imports:

```rust
use std::str::FromStr as _;

use anyhow::Context as _;
use polymarket_client_sdk_v2::auth::{
    Credentials, ExposeSecret as _, LocalSigner, Signer as _,
};
use polymarket_client_sdk_v2::clob::{Client, Config as SdkConfig};
use polymarket_client_sdk_v2::types::Address;
use polymarket_client_sdk_v2::POLYGON;

use crate::config::AppConfig;
```

Implement validation before any production request:

```rust
pub async fn obtain_api_credentials(
    cfg: &AppConfig,
    request: AuthRequest,
) -> Result<Credentials> {
    ensure_official_v2_host(&cfg.site.clob_api_base)?;
    if cfg.exchange.chain_id != POLYGON {
        return Err(anyhow!("L1 authentication requires Polygon chain id 137"));
    }
    if cfg.credentials.signature_type != Some(0) {
        return Err(anyhow!("L1 authentication phase supports EOA signature_type=0 only"));
    }

    let signer = LocalSigner::from_str(cfg.credentials.private_key.trim())
        .context("loading EOA signer")?
        .with_chain_id(Some(POLYGON));
    let funder = Address::from_str(&cfg.credentials.funder_address)
        .context("parsing EOA funder address")?;
    if signer.address() != funder {
        return Err(anyhow!("EOA funder_address must match the signer address"));
    }

    let client = Client::new(&cfg.site.clob_api_base, SdkConfig::default())?;
    obtain_with_client(client, &signer, request).await
}
```

Implement explicit dispatch and sanitized errors. Do not attach the SDK error as an anyhow source or format the complete SDK error, because an SDK status error can contain the server response body:

```rust
async fn obtain_with_client<S: polymarket_client_sdk_v2::auth::Signer>(
    client: Client,
    signer: &S,
    request: AuthRequest,
) -> Result<Credentials> {
    let result = match request.action {
        ApiKeyAction::Create => client.create_api_key(signer, request.nonce).await,
        ApiKeyAction::Derive => client.derive_api_key(signer, request.nonce).await,
    };
    let credentials = result.map_err(|error| {
        let (method, path) = match request.action {
            ApiKeyAction::Create => ("POST", "/auth/api-key"),
            ApiKeyAction::Derive => ("GET", "/auth/derive-api-key"),
        };
        anyhow!("CLOB L1 {method} {path} failed ({:?})", error.kind())
    })?;
    validate_credentials(&credentials)?;
    Ok(credentials)
}
```

Validate all returned fields:

```rust
use polymarket_client_sdk_v2::auth::ExposeSecret as _;

fn validate_credentials(credentials: &Credentials) -> Result<()> {
    if credentials.key().to_string().trim().is_empty() {
        return Err(anyhow!("CLOB returned an empty API key"));
    }
    if credentials.secret().expose_secret().trim().is_empty() {
        return Err(anyhow!("CLOB returned an empty API secret"));
    }
    if credentials.passphrase().expose_secret().trim().is_empty() {
        return Err(anyhow!("CLOB returned an empty API passphrase"));
    }
    Ok(())
}
```

Call `validate_credentials(&credentials)?` after either explicit SDK method. Do not format `credentials` with `Debug` in any error.

- [ ] **Step 8: Run the complete adapter test module and verify GREEN**

```powershell
cargo test --offline service::clob_auth::tests
```

Expected: all adapter tests PASS using loopback only.

- [ ] **Step 9: Commit Task 3**

```powershell
git add -- src/service/clob_auth.rs src/service/mod.rs
git commit -m "feat: add official SDK L1 auth adapter"
```

---

### Task 4: Add explicit auth CLI commands with redacted output

**Files:**
- Modify/Test: `src/main.rs`

**Interfaces:**
- Consumes: `obtain_api_credentials`, `persist_api_credentials`, global `--config`, and global `--credentials`.
- Produces: `auth create-api-key [--nonce N]` and `auth derive-api-key [--nonce N]`.

- [ ] **Step 1: Write failing Clap parser tests**

Derive `PartialEq, Eq` for auth command types and add tests in `src/main.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_create_api_key_with_nonce() {
        let cli = Cli::try_parse_from([
            "polymarket-toolkits",
            "auth",
            "create-api-key",
            "--nonce",
            "23",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Auth {
                command: AuthCommand::CreateApiKey { nonce: Some(23) }
            })
        ));
    }

    #[test]
    fn parses_derive_api_key_with_default_nonce() {
        let cli = Cli::try_parse_from([
            "polymarket-toolkits",
            "auth",
            "derive-api-key",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Auth {
                command: AuthCommand::DeriveApiKey { nonce: None }
            })
        ));
    }
}
```

- [ ] **Step 2: Run parser tests and verify RED**

```powershell
cargo test --offline --bin polymarket-toolkits parses_create_api_key_with_nonce
cargo test --offline --bin polymarket-toolkits parses_derive_api_key_with_default_nonce
```

Expected: compile failure because `Command::Auth` and `AuthCommand` do not exist.

- [ ] **Step 3: Implement nested auth command types**

Add:

```rust
#[derive(Subcommand, Debug, PartialEq, Eq)]
enum AuthCommand {
    /// Create a new CLOB API key. This contacts the official V2 host but never places an order.
    CreateApiKey {
        #[arg(long)]
        nonce: Option<u32>,
    },
    /// Derive an existing CLOB API key. This contacts the official V2 host but never places an order.
    DeriveApiKey {
        #[arg(long)]
        nonce: Option<u32>,
    },
}
```

Extend `Command`:

```rust
/// Create or derive CLOB L2 API credentials using explicit L1 authentication.
Auth {
    #[command(subcommand)]
    command: AuthCommand,
},
```

- [ ] **Step 4: Run parser tests and verify GREEN**

Run both focused commands from Step 2. Expected: PASS.

- [ ] **Step 5: Write a failing redaction test**

```rust
#[test]
fn api_key_summary_never_contains_the_complete_key() {
    let key = "12345678-1234-1234-1234-123456789abc";
    let summary = redact_api_key(key);
    assert!(!summary.contains(key));
    assert!(summary.starts_with("1234"));
    assert!(summary.ends_with("9abc"));
}
```

Run:

```powershell
cargo test --offline --bin polymarket-toolkits api_key_summary_never_contains_the_complete_key
```

Expected: compile failure because `redact_api_key` does not exist.

- [ ] **Step 6: Implement only API-key redaction and verify GREEN**

```rust
fn redact_api_key(key: &str) -> String {
    if key.len() <= 8 {
        return "<redacted>".to_owned();
    }
    format!("{}…{}", &key[..4], &key[key.len() - 4..])
}
```

Run the focused redaction test again. Expected: PASS.

- [ ] **Step 7: Write a failing pre-network credentials-file test**

Add this test using a committed-config fixture but a nonexistent credentials path:

```rust
#[tokio::test]
async fn auth_refuses_missing_credentials_file_before_signing() {
    let cfg: AppConfig = serde_json::from_str(include_str!("../config.json")).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("config.yaml");
    let error = run_auth(
        &cfg,
        &missing,
        AuthCommand::CreateApiKey { nonce: None },
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("must already exist"));
}
```

Run:

```powershell
cargo test --offline --bin polymarket-toolkits auth_refuses_missing_credentials_file_before_signing
```

Expected: compile failure because `run_auth` does not exist. The test must never open a socket.

- [ ] **Step 8: Implement auth command orchestration**

Import only the needed secret accessor in `main.rs`:

```rust
use polymarket_client_sdk_v2::auth::ExposeSecret as _;
use polymarket_toolkits::{
    bot::{self, BotKind},
    config::{persist_api_credentials, ApiCredentialUpdate, AppConfig},
    service::clob_auth::{obtain_api_credentials, ApiKeyAction, AuthRequest},
    ui,
};
```

Add `run_auth`; the credentials-file precondition must be its first executable branch:

```rust
async fn run_auth(
    cfg: &AppConfig,
    credentials_path: &std::path::Path,
    command: AuthCommand,
) -> Result<()> {
    if !credentials_path.is_file() {
        return Err(anyhow::anyhow!(
            "credentials file must already exist; copy config.yaml.example to {} first",
            credentials_path.display()
        ));
    }

    let request = match command {
        AuthCommand::CreateApiKey { nonce } => AuthRequest {
            action: ApiKeyAction::Create,
            nonce,
        },
        AuthCommand::DeriveApiKey { nonce } => AuthRequest {
            action: ApiKeyAction::Derive,
            nonce,
        },
    };

    let credentials = obtain_api_credentials(cfg, request)
        .await
        .context("obtaining CLOB API credentials")?;
    let api_key = credentials.key().to_string();
    persist_api_credentials(
        credentials_path,
        ApiCredentialUpdate {
            api_key: &api_key,
            api_secret: credentials.secret().expose_secret(),
            api_passphrase: credentials.passphrase().expose_secret(),
        },
    )
    .context("persisting CLOB API credentials")?;

    info!(
        signer = %cfg.credentials.funder_address,
        api_key = %redact_api_key(&api_key),
        credentials_path = %credentials_path.display(),
        "CLOB API credentials saved; trading remains disabled unless separately enabled"
    );
    Ok(())
}
```

Clone `cli.credentials` before moving `cli.command`, then add the match arm:

```rust
Some(Command::Auth { command }) => run_auth(&cfg, &credentials_path, command).await,
```

Do not invoke auth from TUI startup, bot startup, or config loading.

- [ ] **Step 9: Run CLI tests and inspect help without executing auth**

```powershell
cargo test --offline --bin polymarket-toolkits
cargo run --offline -- --help
cargo run --offline -- auth --help
cargo run --offline -- auth create-api-key --help
cargo run --offline -- auth derive-api-key --help
```

Expected: tests PASS; help text states that auth contacts the official V2 host and never places an order. Do not run either command without `--help`.

- [ ] **Step 10: Commit Task 4**

```powershell
git add -- src/main.rs
git commit -m "feat: add explicit CLOB API credential commands"
```

---

### Task 5: Document phase-1 behavior and keep safety defaults locked

**Files:**
- Modify: `README.md`
- Modify: `README.zh-CN.md`
- Modify: `config.yaml.example`
- Modify: `docs/superpowers/plans/2026-08-17-official-sdk-auth-foundation.md` (check completed boxes only)

**Interfaces:**
- Consumes: the final CLI syntax and persistence behavior from Tasks 1–4.
- Produces: user-facing instructions that do not encourage real execution during this phase.

- [ ] **Step 1: Add English and Chinese phase-1 usage notes**

Document exactly these commands as future manual operations, without running them:

```powershell
.\target\release\polymarket-toolkits.exe `
  --config .\config.json `
  --credentials .\config.yaml `
  auth create-api-key

.\target\release\polymarket-toolkits.exe `
  --config .\config.json `
  --credentials .\config.yaml `
  auth derive-api-key
```

State that:

- the command contacts only `https://clob-v2.polymarket.com`;
- create and derive never silently fall back;
- the existing YAML is atomically updated;
- output is redacted;
- credentials do not enable trading;
- this development phase did not execute either command.

- [ ] **Step 2: Update `config.yaml.example` comments**

Replace the statement that automatic L1 credential creation is not implemented with:

```yaml
  # Copy this example to config.yaml and fill the EOA fields before running
  # `auth create-api-key` or `auth derive-api-key`. The auth command updates
  # only these three API fields and never enables trading.
  # api_key: ""
  # api_secret: ""      # URL-safe Base64 value returned by the CLOB API.
  # api_passphrase: ""
```

- [ ] **Step 3: Run documentation and safety scans**

```powershell
rg -n "clob\.polymarket\.com|create-or-derive|create_or_derive" README.md README.zh-CN.md config.json config.dryrun-public.json config.yaml.example src/main.rs src/service/clob_auth.rs
rg -n '"enable_trading"\s*:\s*true|"mock_trading"\s*:\s*false|api_key:\s*"[^\"]+"|api_secret:\s*"[^\"]+"|api_passphrase:\s*"[^\"]+"' config.json config.dryrun-public.json config.yaml.example
```

Expected: old HTTP host and silent fallback references are absent from the intended V2 HTTP/auth documentation; safety scan has no matches. The existing WebSocket hostname containing `clob.polymarket.com` is allowed and must not be changed in this phase.

- [ ] **Step 4: Commit Task 5**

```powershell
git add -- README.md README.zh-CN.md config.yaml.example docs/superpowers/plans/2026-08-17-official-sdk-auth-foundation.md
git commit -m "docs: explain official SDK credential flow"
```

---

### Task 6: Run final offline verification and update Obsidian

**Files:**
- Update outside Git: `C:\Users\Haozi\Documents\记忆库\20-Prediction-Markets-Trading-Bot-Toolkits.md`
- Update outside Git: `C:\Users\Haozi\Documents\记忆库\05-项目索引.md`

**Interfaces:**
- Consumes: the complete phase-1 branch.
- Produces: verification evidence, a clean feature branch, and a credential-free durable project record.

- [ ] **Step 1: Format only intentionally changed Rust files**

Run targeted rustfmt through Cargo's configured edition without formatting untouched files:

```powershell
rustfmt --edition 2021 src/config.rs src/service/clob_auth.rs src/service/mod.rs src/main.rs
```

Then rerun focused tests changed by formatting.

- [ ] **Step 2: Run the full offline test suite**

```powershell
cargo test --offline
```

Expected: all prior 47 tests plus the new config, adapter, persistence, and CLI tests PASS with zero failures.

- [ ] **Step 3: Run release and lint gates offline**

```powershell
cargo build --release --offline
cargo clippy --all-targets --offline -- -D warnings
```

Expected: both PASS.

- [ ] **Step 4: Record the repository-wide formatting baseline**

```powershell
cargo fmt --check
```

Expected: if it still fails only on known pre-existing unrelated formatting differences, record that fact and do not run whole-repository formatting. If it identifies a newly changed file, format that file only and rerun its tests.

- [ ] **Step 5: Run final diff, branch, and secret safety checks**

```powershell
git diff --check
git status --short --branch
git log --oneline --decorate -6
rg -n '"enable_trading"\s*:\s*true|"mock_trading"\s*:\s*false|api_key:\s*"[^\"]+"|api_secret:\s*"[^\"]+"|api_passphrase:\s*"[^\"]+"' config.json config.dryrun-public.json config.yaml.example
```

Expected: no diff-check errors, no real credentials, no permissive trading flags, and no uncommitted production changes after the final documentation commit.

- [ ] **Step 6: Update Obsidian without storing credentials**

In `20-Prediction-Markets-Trading-Bot-Toolkits.md`, record only:

- official SDK version and phase-1 commit(s);
- new CLI names;
- corrected V2 HTTP host;
- test/release/Clippy counts and results;
- confirmation that no real auth command ran and no credential was created;
- phase 2 remains SDK order-path migration.

Update the project-index row to the same stable status. Never include fixture secrets, user credentials, private keys, complete API keys, or raw HTTP output.

- [ ] **Step 7: Use the finishing-development-branch workflow**

After fresh verification, offer exactly the supported local merge / PR / keep-branch options. Do not merge, push, or delete the branch without the user's explicit selection.

---

## Plan Self-Review Checklist

- [ ] Every in-scope design requirement maps to a task: dependency/host (Task 1), atomic persistence (Task 2), SDK L1 adapter (Task 3), explicit CLI/redaction (Task 4), docs (Task 5), verification/memory (Task 6).
- [ ] No task changes the existing order path or trading safety gates.
- [ ] All new behavior starts with a failing test and records RED before GREEN.
- [ ] SDK types remain confined to `clob_auth.rs` except the minimal `ExposeSecret` accessor used at the CLI persistence boundary.
- [ ] The official fixed L1 signature vector is exercised through a loopback SDK request with fixed server time and nonce.
- [ ] SDK status errors are sanitized without attaching or formatting the server response body.
- [ ] The CLI rejects a missing credentials file before constructing a signer or request.
- [ ] Production host restriction and loopback-only tests are both explicit.
- [ ] Complete credentials never appear in logs, docs, Git, Obsidian, or final output.
- [ ] No placeholder instructions remain.
