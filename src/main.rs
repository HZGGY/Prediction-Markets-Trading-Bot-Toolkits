use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use polymarket_client_sdk_v2::auth::ExposeSecret as _;
use polymarket_toolkits::{
    bot::{self, BotKind},
    config::{
        ensure_credentials_file_account_matches, persist_api_credentials, ApiCredentialUpdate,
        AppConfig,
    },
    service::clob_auth::{obtain_api_credentials, ApiKeyAction, AuthRequest},
    ui,
};
use std::path::PathBuf;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "polymarket-toolkits")]
#[command(about = "Multi-venue prediction-market trading toolkit.", long_about = None)]
struct Cli {
    /// Path to public config (JSON).
    #[arg(long, default_value = "config.json")]
    config: PathBuf,

    /// Path to credentials file (YAML).
    #[arg(long, default_value = "config.yaml")]
    credentials: PathBuf,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Launch the interactive TUI to select a bot. (Default if no subcommand.)
    Tui,
    /// Run a specific bot headlessly (no TUI).
    Run {
        /// Which bot to run.
        #[arg(value_enum)]
        bot: BotKindArg,
    },
    /// Create or derive CLOB L2 API credentials using explicit L1 authentication.
    Auth {
        #[command(subcommand)]
        command: AuthCommand,
    },
}

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

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
enum BotKindArg {
    CopyTrading,
    BtcArb,
    CrossArb,
    DirectionalArb,
    SpreadFarming,
    Sports,
    ResolutionSniper,
    OrderbookImbalance,
    MarketMaking,
    WhaleSignal,
}

impl From<BotKindArg> for BotKind {
    fn from(b: BotKindArg) -> Self {
        match b {
            BotKindArg::CopyTrading => BotKind::CopyTrading,
            BotKindArg::BtcArb => BotKind::BtcArb,
            BotKindArg::CrossArb => BotKind::CrossArb,
            BotKindArg::DirectionalArb => BotKind::DirectionalArb,
            BotKindArg::SpreadFarming => BotKind::SpreadFarming,
            BotKindArg::Sports => BotKind::Sports,
            BotKindArg::ResolutionSniper => BotKind::ResolutionSniper,
            BotKindArg::OrderbookImbalance => BotKind::OrderbookImbalance,
            BotKindArg::MarketMaking => BotKind::MarketMaking,
            BotKindArg::WhaleSignal => BotKind::WhaleSignal,
        }
    }
}

fn redact_api_key(key: &str) -> String {
    if key.len() <= 8 {
        return "<redacted>".to_owned();
    }
    format!("{}…{}", &key[..4], &key[key.len() - 4..])
}

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
    ensure_credentials_file_account_matches(credentials_path, &cfg.credentials)?;

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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("polymarket_toolkits=info,info")),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();
    let cfg = AppConfig::load(&cli.config, &cli.credentials).context("loading configuration")?;
    let credentials_path = cli.credentials.clone();

    info!(
        wallets = cfg.bot.wallets_to_track.len(),
        enable_trading = cfg.bot.enable_trading,
        mock_trading = cfg.bot.mock_trading,
        "configuration loaded"
    );

    match cli.command {
        Some(Command::Run { bot: kind }) => bot::run(kind.into(), cfg).await,
        Some(Command::Auth { command }) => run_auth(&cfg, &credentials_path, command).await,
        Some(Command::Tui) | None => ui::run(cfg).await,
    }
}

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
        let cli = Cli::try_parse_from(["polymarket-toolkits", "auth", "derive-api-key"]).unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Auth {
                command: AuthCommand::DeriveApiKey { nonce: None }
            })
        ));
    }

    #[test]
    fn api_key_summary_never_contains_the_complete_key() {
        let key = "12345678-1234-1234-1234-123456789abc";
        let summary = redact_api_key(key);
        assert!(!summary.contains(key));
        assert!(summary.starts_with("1234"));
        assert!(summary.ends_with("9abc"));
    }

    #[tokio::test]
    async fn auth_refuses_missing_credentials_file_before_signing() {
        let cfg: AppConfig = serde_json::from_str(include_str!("../config.json")).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("config.yaml");
        let error = run_auth(&cfg, &missing, AuthCommand::CreateApiKey { nonce: None })
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("must already exist"));
    }

    #[tokio::test]
    async fn auth_refuses_yaml_account_mismatch_before_signing_or_network() {
        let mut cfg: AppConfig = serde_json::from_str(include_str!("../config.json")).unwrap();
        cfg.credentials.private_key = "effective-private-key".to_owned();
        cfg.credentials.funder_address = "0x1111111111111111111111111111111111111111".to_owned();
        cfg.credentials.signature_type = Some(0);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(
            &path,
            r#"bot:
  private_key: disk-private-key
  funder_address: 0x1111111111111111111111111111111111111111
  signature_type: 0
"#,
        )
        .unwrap();

        let error = run_auth(&cfg, &path, AuthCommand::CreateApiKey { nonce: None })
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("does not match the effective account"));
        assert!(!error.contains("effective-private-key"));
        assert!(!error.contains("disk-private-key"));
    }
}
