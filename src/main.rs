use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use polymarket_client_sdk_v2::auth::ExposeSecret as _;
use polymarket_toolkits::{
    bot::{self, BotKind},
    config::{
        ensure_credentials_file_account_matches, persist_api_credentials, ApiCredentialUpdate,
        AppConfig,
    },
    recovery_cli::{self, RecoveryCommand},
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
    /// Inspect and explicitly recover one durable execution intent.
    Recovery {
        #[command(subcommand)]
        command: RecoveryCommand,
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

fn load_config_for_command(cli: &Cli) -> Result<AppConfig> {
    let recovery_command = is_recovery_command(&cli.command);
    let mut cfg = AppConfig::load_public(&cli.config).map_err(|error| {
        if recovery_command {
            anyhow::anyhow!(
                "recovery failed code=public_config_unavailable instruction=check_recovery_public_config"
            )
        } else {
            error
        }
    })?;
    if command_needs_credentials(&cli.command, &cfg) {
        cfg.load_credentials(&cli.credentials).map_err(|error| {
            if recovery_command {
                anyhow::anyhow!(
                    "recovery failed code=credentials_unavailable instruction=check_recovery_credentials"
                )
            } else {
                error
            }
        })?;
    }
    Ok(cfg)
}

fn is_recovery_command(command: &Option<Command>) -> bool {
    matches!(command, Some(Command::Recovery { .. }))
}

fn load_config_for_operator(cli: &Cli) -> Result<AppConfig> {
    let config = load_config_for_command(cli);
    if is_recovery_command(&cli.command) {
        config
    } else {
        config.context("loading configuration")
    }
}

fn command_needs_credentials(command: &Option<Command>, cfg: &AppConfig) -> bool {
    match command {
        Some(Command::Auth { .. }) => true,
        Some(Command::Recovery { command }) => recovery_cli::command_needs_credentials(command),
        Some(Command::Run { .. }) | Some(Command::Tui) | None => cfg.live_trading_allowed(),
    }
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
    let cfg = load_config_for_operator(&cli)?;
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
        Some(Command::Recovery { command }) => recovery_cli::run(&cfg, command).await,
        Some(Command::Tui) | None => ui::run(cfg).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{ffi::OsString, sync::Mutex};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvVarGuard {
        name: &'static str,
        previous: Option<OsString>,
    }

    impl EnvVarGuard {
        fn set(name: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(name);
            std::env::set_var(name, value);
            Self { name, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(previous) = self.previous.take() {
                std::env::set_var(self.name, previous);
            } else {
                std::env::remove_var(self.name);
            }
        }
    }

    fn load_config_at_main_seam(cli: &Cli) -> Result<AppConfig> {
        load_config_for_operator(cli)
    }

    #[test]
    fn local_recovery_public_config_failure_has_only_a_static_operator_message() {
        let path_sentinel = "RECOVERY_PUBLIC_CONFIG_PATH_SENTINEL";
        let cli = Cli::try_parse_from([
            "polymarket-toolkits",
            "--config",
            path_sentinel,
            "recovery",
            "inspect",
        ])
        .unwrap();

        let output = load_config_at_main_seam(&cli).unwrap_err().to_string();

        assert_eq!(
            output,
            "recovery failed code=public_config_unavailable instruction=check_recovery_public_config"
        );
        assert!(!output.contains(path_sentinel));
        assert!(!output.contains("The system cannot find"));
    }

    #[test]
    fn network_recovery_credentials_failure_has_only_a_static_operator_message() {
        let directory = tempfile::tempdir().unwrap();
        let config_path = directory.path().join("public.json");
        let credentials_sentinel = "RECOVERY_CREDENTIALS_PATH_SENTINEL";
        std::fs::write(&config_path, include_str!("../config.json")).unwrap();
        let cli = Cli::try_parse_from([
            "polymarket-toolkits".to_owned(),
            "--config".to_owned(),
            config_path.to_string_lossy().into_owned(),
            "--credentials".to_owned(),
            credentials_sentinel.to_owned(),
            "recovery".to_owned(),
            "reconcile".to_owned(),
            "--intent".to_owned(),
            "123e4567-e89b-12d3-a456-426614174000".to_owned(),
        ])
        .unwrap();

        let output = load_config_at_main_seam(&cli).unwrap_err().to_string();

        assert_eq!(
            output,
            "recovery failed code=credentials_unavailable instruction=check_recovery_credentials"
        );
        assert!(!output.contains(credentials_sentinel));
        assert!(!output.contains("The system cannot find"));
    }

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
    fn parses_recovery_inspect_with_explicit_local_order_id_display() {
        let cli = Cli::try_parse_from([
            "polymarket-toolkits",
            "recovery",
            "inspect",
            "--intent",
            "123e4567-e89b-12d3-a456-426614174000",
            "--show-order-id",
        ])
        .unwrap();

        assert!(matches!(
            cli.command,
            Some(Command::Recovery {
                command: RecoveryCommand::Inspect {
                    intent: Some(_),
                    show_order_id: true,
                }
            })
        ));
    }

    #[test]
    fn recovery_parser_exposes_only_the_six_explicit_operations() {
        let intent = "123e4567-e89b-12d3-a456-426614174000";
        for command in [
            vec!["recovery", "inspect"],
            vec!["recovery", "reconcile", "--intent", intent],
            vec!["recovery", "prepare-cancel", "--intent", intent],
            vec![
                "recovery",
                "cancel",
                "--intent",
                intent,
                "--confirm",
                "challenge",
            ],
            vec![
                "recovery",
                "apply",
                "--intent",
                intent,
                "--confirm",
                "challenge",
            ],
            vec![
                "recovery",
                "acknowledge",
                "--intent",
                intent,
                "--confirm",
                "challenge",
            ],
        ] {
            let mut args = vec!["polymarket-toolkits"];
            args.extend(command);
            assert!(Cli::try_parse_from(args).is_ok());
        }

        for rejected in [
            vec!["recovery", "inspect", "--force"],
            vec!["recovery", "cancel", "--yes"],
            vec!["recovery", "cancel-all"],
            vec!["recovery", "cancel", "--market", "market-id"],
            vec!["recovery", "retry", "--intent", intent],
        ] {
            let mut args = vec!["polymarket-toolkits"];
            args.extend(rejected);
            assert!(Cli::try_parse_from(args).is_err());
        }
    }

    #[test]
    fn local_recovery_commands_ignore_malformed_credentials_and_secret_environment_overrides() {
        let _env_lock = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
        let _private_key = EnvVarGuard::set("PM_PRIVATE_KEY", "RECOVERY_PRIVATE_KEY_SENTINEL");
        let _funder = EnvVarGuard::set(
            "PM_FUNDER_ADDRESS",
            "0x9999999999999999999999999999999999999999",
        );
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("public.json");
        let credentials_path = dir.path().join("malformed-credentials.yaml");
        std::fs::write(&config_path, include_str!("../config.json")).unwrap();
        std::fs::write(&credentials_path, "bot: [malformed").unwrap();

        for command in [
            vec!["recovery", "inspect"],
            vec![
                "recovery",
                "apply",
                "--intent",
                "123e4567-e89b-12d3-a456-426614174000",
                "--confirm",
                "challenge",
            ],
            vec![
                "recovery",
                "acknowledge",
                "--intent",
                "123e4567-e89b-12d3-a456-426614174000",
                "--confirm",
                "challenge",
            ],
        ] {
            let mut args = vec![
                "polymarket-toolkits".to_owned(),
                "--config".to_owned(),
                config_path.to_string_lossy().into_owned(),
                "--credentials".to_owned(),
                credentials_path.to_string_lossy().into_owned(),
            ];
            args.extend(command.into_iter().map(str::to_owned));
            let cli = Cli::try_parse_from(args).unwrap();
            let cfg = load_config_at_main_seam(&cli)
                .unwrap_or_else(|_| panic!("local recovery must not read credentials"));
            assert!(!command_needs_credentials(&cli.command, &cfg));
            assert!(cfg.credentials.private_key.is_empty());
            assert!(cfg.credentials.funder_address.is_empty());
            assert_eq!(cfg.credentials.api_key, None);
            assert_eq!(cfg.credentials.api_secret, None);
            assert_eq!(cfg.credentials.api_passphrase, None);
        }
    }

    #[test]
    fn network_recovery_commands_require_the_credential_source() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("public.json");
        let credentials_path = dir.path().join("malformed-credentials.yaml");
        std::fs::write(&config_path, include_str!("../config.json")).unwrap();
        std::fs::write(&credentials_path, "bot: [malformed").unwrap();

        for command in [
            vec![
                "recovery",
                "reconcile",
                "--intent",
                "123e4567-e89b-12d3-a456-426614174000",
            ],
            vec![
                "recovery",
                "prepare-cancel",
                "--intent",
                "123e4567-e89b-12d3-a456-426614174000",
            ],
            vec![
                "recovery",
                "cancel",
                "--intent",
                "123e4567-e89b-12d3-a456-426614174000",
                "--confirm",
                "challenge",
            ],
        ] {
            let mut args = vec![
                "polymarket-toolkits".to_owned(),
                "--config".to_owned(),
                config_path.to_string_lossy().into_owned(),
                "--credentials".to_owned(),
                credentials_path.to_string_lossy().into_owned(),
            ];
            args.extend(command.into_iter().map(str::to_owned));
            let cli = Cli::try_parse_from(args).unwrap();
            assert!(load_config_at_main_seam(&cli).is_err());
        }
    }

    #[test]
    fn api_key_summary_never_contains_the_complete_key() {
        let key = "12345678-1234-1234-1234-123456789abc";
        let summary = redact_api_key(key);
        assert!(!summary.contains(key));
        assert!(summary.starts_with("1234"));
        assert!(summary.ends_with("9abc"));
    }

    #[test]
    fn strict_run_ignores_malformed_credentials_and_secret_environment_overrides() {
        let _env_lock = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
        let private_key_sentinel = "STRICT_RUN_PRIVATE_KEY_SENTINEL";
        let funder_sentinel = "0x9999999999999999999999999999999999999999";
        let _private_key = EnvVarGuard::set("PM_PRIVATE_KEY", private_key_sentinel);
        let _funder = EnvVarGuard::set("PM_FUNDER_ADDRESS", funder_sentinel);

        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("strict-paper.json");
        let credentials_path = dir.path().join("malformed-credentials.yaml");
        std::fs::write(&config_path, include_str!("../config.json")).unwrap();
        std::fs::write(&credentials_path, "bot: [malformed").unwrap();
        let cli = Cli::try_parse_from(vec![
            "polymarket-toolkits".to_owned(),
            "--config".to_owned(),
            config_path.to_string_lossy().into_owned(),
            "--credentials".to_owned(),
            credentials_path.to_string_lossy().into_owned(),
            "run".to_owned(),
            "copy-trading".to_owned(),
        ])
        .unwrap();

        let cfg = load_config_at_main_seam(&cli)
            .unwrap_or_else(|_| panic!("strict Run must not read or parse the credential source"));

        assert!(!cfg.live_trading_allowed());
        assert!(cfg.credentials.private_key.is_empty());
        assert!(cfg.credentials.funder_address.is_empty());
        assert_eq!(cfg.credentials.signature_type, None);
        assert_eq!(cfg.credentials.api_key, None);
        assert_eq!(cfg.credentials.api_secret, None);
        assert_eq!(cfg.credentials.api_passphrase, None);
        assert_ne!(cfg.credentials.private_key, private_key_sentinel);
        assert_ne!(cfg.credentials.funder_address, funder_sentinel);
    }

    #[test]
    fn auth_command_still_requires_parsing_the_credential_source() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("public.json");
        let credentials_path = dir.path().join("malformed-credentials.yaml");
        std::fs::write(&config_path, include_str!("../config.json")).unwrap();
        std::fs::write(&credentials_path, "bot: [malformed").unwrap();
        let cli = Cli::try_parse_from(vec![
            "polymarket-toolkits".to_owned(),
            "--config".to_owned(),
            config_path.to_string_lossy().into_owned(),
            "--credentials".to_owned(),
            credentials_path.to_string_lossy().into_owned(),
            "auth".to_owned(),
            "derive-api-key".to_owned(),
        ])
        .unwrap();

        assert!(load_config_at_main_seam(&cli).is_err());
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
