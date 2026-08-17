//! Configuration loading and types.
//!
//! Two-file split:
//! - `config.json` — public settings (committed)
//! - `config.yaml` — credentials (gitignored, must never be committed)

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use serde_yaml::{Mapping, Value};
use std::io::Write as _;
use std::path::Path;

pub const OFFICIAL_CLOB_V2_HOST: &str = "https://clob-v2.polymarket.com";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub bot: BotConfig,
    pub site: SiteConfig,
    pub strategy: StrategyConfig,
    pub trading: TradingConfig,
    pub risk: RiskConfig,
    pub exchange: ExchangeConfig,

    /// Market eligibility filter (allowlist + blocklist by category, tag, slug).
    #[serde(default)]
    pub filters: FiltersConfig,

    /// Take-profit / stop-loss configuration.
    #[serde(default)]
    pub tp_sl: TpSlConfig,

    /// Loaded from `config.yaml`. Not serialised back out.
    #[serde(skip)]
    pub credentials: Credentials,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BotConfig {
    pub wallets_to_track: Vec<String>,

    /// When `false`, decisions are computed but no orders are sent.
    #[serde(default)]
    pub enable_trading: bool,

    /// When `true`, every order path returns early with a log line.
    /// Independent of `enable_trading` — both must be permissive for live trades.
    #[serde(default = "default_true")]
    pub mock_trading: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiteConfig {
    pub gamma_api_base: String,
    pub data_api_base: String,
    pub clob_api_base: String,
    pub clob_wss_url: String,
    pub polygon_rpc_url: String,
    pub polygon_ws_url: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum CopyStrategy {
    Percentage,
    Fixed,
    Adaptive,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
    pub copy_strategy: CopyStrategy,

    /// For PERCENTAGE: percent of whale notional (e.g. 20.0 = 20%).
    /// For FIXED: USD per copy leg.
    /// For ADAPTIVE: ignored (uses adaptive_* below).
    pub copy_size: f64,

    pub trade_multiplier: f64,
    pub min_order_size_usd: f64,
    pub max_order_size_usd: f64,
    pub min_whale_shares_to_copy: f64,
    pub adaptive_threshold_usd: f64,
    pub adaptive_min_percent: f64,
    pub adaptive_max_percent: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingConfig {
    /// Max API requests per `rate_window_secs`.
    pub rate_limit: u32,
    pub rate_window_secs: u64,
    pub poll_interval_secs: u64,

    /// Slippage tolerance applied to the order limit price (fractional, e.g. 0.005 = 0.5¢ at $1 scale).
    pub price_buffer: f64,

    pub fee_rate_bps: u32,
    pub order_expiration_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    pub large_trade_shares: f64,
    pub consecutive_trigger: u32,
    pub sequence_window_secs: u64,
    pub min_depth_usd: f64,
    pub trip_duration_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeConfig {
    pub ctf_exchange_address: String,
    pub neg_risk_exchange_address: String,
    pub chain_id: u64,
    pub domain_name: String,
    pub domain_version: String,
}

/// Market eligibility filter.
///
/// Behavior:
/// 1. Block lists always win — anything matching is skipped.
/// 2. If *any* allow list is non-empty, the market must match at least one
///    allow rule for ANY of (slug / category / tag) to be eligible.
/// 3. If all allow lists are empty, the bot falls back to allow-everything-
///    minus-blocklist semantics (so users who haven't curated yet aren't
///    stranded with zero trades).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FiltersConfig {
    #[serde(default)]
    pub slug_allow: Vec<String>,
    #[serde(default)]
    pub slug_block: Vec<String>,
    #[serde(default)]
    pub categories_allow: Vec<String>,
    #[serde(default)]
    pub categories_block: Vec<String>,
    #[serde(default)]
    pub tags_allow: Vec<String>,
    #[serde(default)]
    pub tags_block: Vec<String>,

    /// Per-category cap on simultaneously open USD notional.
    /// e.g. `{ "Politics": 500, "Sports": 300 }`.
    #[serde(default)]
    pub per_category_max_open_usd: std::collections::HashMap<String, f64>,

    /// Per-tag cap on simultaneously open USD notional.
    #[serde(default)]
    pub per_tag_max_open_usd: std::collections::HashMap<String, f64>,
}

impl FiltersConfig {
    /// True when at least one allow list is populated — strict allowlist mode.
    pub fn is_strict(&self) -> bool {
        !self.slug_allow.is_empty()
            || !self.categories_allow.is_empty()
            || !self.tags_allow.is_empty()
    }
}

/// Take-profit / stop-loss config (percent of entry price).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TpSlConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_tp_pct")]
    pub default_take_profit_pct: f64,
    #[serde(default = "default_sl_pct")]
    pub default_stop_loss_pct: f64,
    #[serde(default)]
    pub per_category_tp_pct: std::collections::HashMap<String, f64>,
    #[serde(default)]
    pub per_category_sl_pct: std::collections::HashMap<String, f64>,
    #[serde(default = "default_monitor_secs")]
    pub poll_interval_secs: u64,
}

impl Default for TpSlConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_take_profit_pct: 50.0,
            default_stop_loss_pct: 30.0,
            per_category_tp_pct: Default::default(),
            per_category_sl_pct: Default::default(),
            poll_interval_secs: 15,
        }
    }
}

fn default_tp_pct() -> f64 {
    50.0
}
fn default_sl_pct() -> f64 {
    30.0
}
fn default_monitor_secs() -> u64 {
    15
}

#[derive(Debug, Clone, Default)]
pub struct Credentials {
    pub private_key: String,
    pub funder_address: String,
    pub signature_type: Option<u8>,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub api_passphrase: Option<String>,
}

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
        .get_mut(yaml_key(key))
        .and_then(Value::as_mapping_mut)
        .ok_or_else(|| anyhow!("credentials YAML must contain a '{key}' mapping"))
}

#[derive(Debug, Deserialize)]
struct CredentialsFile {
    bot: CredentialsBot,
}

#[derive(Debug, Deserialize)]
struct CredentialsBot {
    private_key: String,
    funder_address: String,
    signature_type: Option<u8>,
    api_key: Option<String>,
    api_secret: Option<String>,
    api_passphrase: Option<String>,
}

impl AppConfig {
    pub fn load(config_path: &Path, credentials_path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(config_path)
            .with_context(|| format!("reading public config from {}", config_path.display()))?;
        let mut cfg: AppConfig = serde_json::from_str(&raw).context("parsing config.json")?;

        // Credentials file is optional — without it we can still run in mock mode.
        if credentials_path.exists() {
            let raw = std::fs::read_to_string(credentials_path).with_context(|| {
                format!("reading credentials from {}", credentials_path.display())
            })?;
            let parsed: CredentialsFile =
                serde_yaml::from_str(&raw).context("parsing config.yaml")?;
            cfg.credentials = Credentials {
                private_key: parsed.bot.private_key,
                funder_address: parsed.bot.funder_address,
                signature_type: parsed.bot.signature_type,
                api_key: parsed.bot.api_key,
                api_secret: parsed.bot.api_secret,
                api_passphrase: parsed.bot.api_passphrase,
            };
        }

        // Environment-variable overrides (handy for CI / Docker).
        if let Ok(key) = std::env::var("PM_PRIVATE_KEY") {
            cfg.credentials.private_key = key;
        }
        if let Ok(addr) = std::env::var("PM_FUNDER_ADDRESS") {
            cfg.credentials.funder_address = addr;
        }

        Ok(cfg)
    }

    pub fn live_trading_allowed(&self) -> bool {
        self.bot.enable_trading && !self.bot.mock_trading
    }
}

fn normalized_hex(value: &str) -> &str {
    let trimmed = value.trim();
    trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed)
}

/// Refuse to persist API credentials when environment overrides changed the
/// account that is stored in the target YAML file.
pub fn ensure_credentials_file_account_matches(path: &Path, effective: &Credentials) -> Result<()> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading credentials from {}", path.display()))?;
    let stored: CredentialsFile = serde_yaml::from_str(&raw).context("parsing config.yaml")?;
    let account_matches = normalized_hex(&stored.bot.private_key)
        .eq_ignore_ascii_case(normalized_hex(&effective.private_key))
        && normalized_hex(&stored.bot.funder_address)
            .eq_ignore_ascii_case(normalized_hex(&effective.funder_address))
        && stored.bot.signature_type == effective.signature_type;

    if !account_matches {
        return Err(anyhow!(
            "credentials file account does not match the effective account; remove PM_PRIVATE_KEY/PM_FUNDER_ADDRESS overrides or update the YAML before authentication"
        ));
    }
    Ok(())
}

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
        if !bot.contains_key(yaml_key(required)) {
            return Err(anyhow!("credentials YAML is missing bot.{required}"));
        }
    }
    bot.insert(
        yaml_key("api_key"),
        Value::String(update.api_key.to_owned()),
    );
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

fn default_true() -> bool {
    true
}

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
            assert_eq!(cfg.site.clob_api_base, "https://clob-v2.polymarket.com");
            assert!(!cfg.bot.enable_trading);
            assert!(cfg.bot.mock_trading);
            assert!(cfg.credentials.api_key.is_none());
            assert!(cfg.credentials.api_secret.is_none());
            assert!(cfg.credentials.api_passphrase.is_none());
        }
    }

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
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must already exist"));
        assert!(!path.exists());
    }

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
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("simulated persist failure"));
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }
}
