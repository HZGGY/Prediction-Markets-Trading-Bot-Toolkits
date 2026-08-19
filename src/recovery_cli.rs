//! Explicit operator recovery commands.
//!
//! Local commands never receive a recovery gateway or credential-bearing
//! configuration. Network commands construct only the exact-order gateway.

use std::{path::PathBuf, sync::Arc};

use anyhow::{anyhow, Result};
use clap::Subcommand;
use uuid::Uuid;

use crate::{
    config::AppConfig,
    service::{
        execution_ledger::{ExecutionLedger, IntentId},
        position_store::PositionStore,
        recovery_gateway::RecoveryGateway,
        recovery_service::{
            RecoveryAcknowledgeStatus, RecoveryApplyStatus, RecoveryInspection, RecoveryService,
            RecoveryServiceError,
        },
    },
};

/// Closed recovery operator surface. No aliases or bypass flags are accepted.
#[derive(Subcommand, Debug)]
pub enum RecoveryCommand {
    /// Inspect one durable intent using local state only.
    Inspect {
        #[arg(long)]
        intent: Option<Uuid>,
        #[arg(long)]
        show_order_id: bool,
    },
    /// Reconcile exactly one ledger-owned order.
    Reconcile {
        #[arg(long)]
        intent: Uuid,
    },
    /// Reconcile one order and issue a cancellation challenge when it is live.
    PrepareCancel {
        #[arg(long)]
        intent: Uuid,
    },
    /// Cancel one exact ledger-owned order with a fresh challenge.
    Cancel {
        #[arg(long)]
        intent: Uuid,
        #[arg(long)]
        confirm: String,
    },
    /// Apply a recovered position locally with a fresh challenge.
    Apply {
        #[arg(long)]
        intent: Uuid,
        #[arg(long)]
        confirm: String,
    },
    /// Acknowledge a safe terminal recovery state locally with a fresh challenge.
    Acknowledge {
        #[arg(long)]
        intent: Uuid,
        #[arg(long)]
        confirm: String,
    },
}

/// The only configuration local recovery commands can receive.
struct LocalRecoveryConfig {
    execution_ledger_path: PathBuf,
    execution_halt_path: PathBuf,
}

impl From<&AppConfig> for LocalRecoveryConfig {
    fn from(cfg: &AppConfig) -> Self {
        Self {
            execution_ledger_path: cfg.trading.execution_ledger_path.clone(),
            execution_halt_path: cfg.trading.execution_halt_path.clone(),
        }
    }
}

pub fn command_needs_credentials(command: &RecoveryCommand) -> bool {
    matches!(
        command,
        RecoveryCommand::Reconcile { .. }
            | RecoveryCommand::PrepareCancel { .. }
            | RecoveryCommand::Cancel { .. }
    )
}

/// Execute one recovery command. Opening the ledger is intentionally lock-bearing
/// for every command, including local inspection.
pub async fn run(cfg: &AppConfig, command: RecoveryCommand) -> Result<()> {
    if command_needs_credentials(&command) {
        run_network(cfg, command).await
    } else {
        run_local_command(LocalRecoveryConfig::from(cfg), command)
    }
}

fn open_local_service(
    cfg: &LocalRecoveryConfig,
) -> Result<(Arc<ExecutionLedger>, RecoveryService)> {
    let ledger = Arc::new(
        ExecutionLedger::open_live(&cfg.execution_ledger_path)
            .map_err(|error| anyhow!("recovery failed code={}", error.code()))?,
    );
    let positions = PositionStore::from_ledger(Arc::clone(&ledger))
        .map_err(|_| anyhow!("recovery failed code=position"))?;
    let service = RecoveryService::local(
        Arc::clone(&ledger),
        positions,
        cfg.execution_halt_path.clone(),
    );

    Ok((ledger, service))
}

fn run_local_command(cfg: LocalRecoveryConfig, command: RecoveryCommand) -> Result<()> {
    let (ledger, service) = open_local_service(&cfg)?;

    run_local(&service, ledger.as_ref(), command)
}

async fn run_network(cfg: &AppConfig, command: RecoveryCommand) -> Result<()> {
    let local = LocalRecoveryConfig::from(cfg);
    let (_, service) = open_local_service(&local)?;
    let gateway = crate::service::clob_sdk_recovery::SdkRecoveryGateway::new(cfg)
        .await
        .map_err(|_| anyhow!("recovery failed code=gateway_initialization"))?;

    run_network_with_gateway(&service, &gateway, command).await
}

#[cfg(test)]
async fn run_network_with_injected_gateway(
    cfg: &AppConfig,
    command: RecoveryCommand,
    gateway: &dyn RecoveryGateway,
) -> Result<()> {
    let local = LocalRecoveryConfig::from(cfg);
    let (_, service) = open_local_service(&local)?;

    run_network_with_gateway(&service, gateway, command).await
}

fn run_local(
    service: &RecoveryService,
    ledger: &ExecutionLedger,
    command: RecoveryCommand,
) -> Result<()> {
    match command {
        RecoveryCommand::Inspect {
            intent,
            show_order_id,
        } => {
            let Some(intent) = inspect_intent(ledger, intent)? else {
                println!("recovery status=no_active_intent");
                return Ok(());
            };
            let inspection = service
                .inspect(intent, show_order_id)
                .map_err(service_error)?;
            print_inspection(&inspection, show_order_id);
            Ok(())
        }
        RecoveryCommand::Apply { intent, confirm } => {
            let result = service
                .apply(IntentId(intent), &confirm)
                .map_err(service_error)?;
            let status = match result.status {
                RecoveryApplyStatus::Applied => "applied",
                RecoveryApplyStatus::AlreadyApplied => "already_applied",
            };
            let challenge = result
                .acknowledge_challenge
                .as_ref()
                .map_or("none", |challenge| challenge.as_str());
            println!("recovery status={status} next_action=acknowledge confirm={challenge}");
            Ok(())
        }
        RecoveryCommand::Acknowledge { intent, confirm } => {
            let status = match service
                .acknowledge(IntentId(intent), &confirm)
                .map_err(service_error)?
            {
                RecoveryAcknowledgeStatus::Acknowledged => "acknowledged",
                RecoveryAcknowledgeStatus::AlreadyAcknowledged => "already_acknowledged",
            };
            println!("recovery status={status}");
            Ok(())
        }
        _ => Err(anyhow!("recovery failed code=unsupported_command")),
    }
}

async fn run_network_with_gateway(
    service: &RecoveryService,
    gateway: &dyn RecoveryGateway,
    command: RecoveryCommand,
) -> Result<()> {
    match command {
        RecoveryCommand::Reconcile { intent } => {
            let inspection = service
                .reconcile(gateway, IntentId(intent))
                .await
                .map_err(service_error)?;
            print_inspection(&inspection, false);
            Ok(())
        }
        RecoveryCommand::PrepareCancel { intent } => {
            let challenge = service
                .prepare_cancel(gateway, IntentId(intent))
                .await
                .map_err(service_error)?;
            println!(
                "recovery status=prepared_cancel confirm={}",
                challenge.as_str()
            );
            Ok(())
        }
        RecoveryCommand::Cancel { intent, confirm } => {
            let inspection = service
                .cancel(gateway, IntentId(intent), &confirm)
                .await
                .map_err(service_error)?;
            print_inspection(&inspection, false);
            Ok(())
        }
        _ => Err(anyhow!("recovery failed code=unsupported_command")),
    }
}

fn inspect_intent(ledger: &ExecutionLedger, requested: Option<Uuid>) -> Result<Option<IntentId>> {
    let projection = ledger.projection();
    Ok(requested
        .map(IntentId)
        .or_else(|| projection.active.as_ref().map(|active| active.intent_id))
        .or_else(|| {
            projection
                .cleanup_pending
                .as_ref()
                .map(|pending| pending.intent_id)
        }))
}

fn print_inspection(inspection: &RecoveryInspection, show_order_id: bool) {
    println!("{}", render_inspection(inspection, show_order_id));
}

fn render_inspection(inspection: &RecoveryInspection, show_order_id: bool) -> String {
    let action = inspection.action.map_or("none", |action| match action {
        crate::service::recovery_service::RecoveryAction::Apply => "apply",
        crate::service::recovery_service::RecoveryAction::Acknowledge => "acknowledge",
        crate::service::recovery_service::RecoveryAction::Cancel => "cancel",
    });
    let hint = inspection.order_id_hint.as_deref().unwrap_or("none");
    let order_id = if show_order_id {
        inspection
            .order_id
            .as_ref()
            .map_or("none", |order_id| order_id.as_str())
    } else {
        "redacted"
    };
    let challenge = inspection
        .challenge
        .as_ref()
        .map_or("none", |challenge| challenge.as_str());
    format!(
        "recovery status=inspected intent_id={} action={action} order_id_hint={hint} order_id={order_id} confirm={challenge}",
        inspection.intent_id.0
    )
}

fn service_error(error: RecoveryServiceError) -> anyhow::Error {
    anyhow!("recovery failed code={}", error.code())
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::{Ipv4Addr, TcpListener, TcpStream},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    use async_trait::async_trait;

    use super::*;
    use crate::service::{
        execution_ledger::{
            ExecutionLedger, IntentId, IntentPurpose, LedgerPayload, OrderId, OrderSide, OrderType,
            PositionSeed, PreparedIntent, TokenId, Venue, ORDER_PROTOCOL_VERSION,
        },
        recovery_gateway::{RecoveryError, RemoteOrderEvidence},
        recovery_service::RecoveryInspection,
    };

    struct LoopbackExactGateway {
        address: std::net::SocketAddr,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl RecoveryGateway for LoopbackExactGateway {
        async fn reconcile_exact(
            &self,
            _expected: &crate::service::order_gateway::PreparedOrderIdentity,
        ) -> Result<RemoteOrderEvidence, RecoveryError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut stream =
                TcpStream::connect(self.address).map_err(|_| RecoveryError::Initialization)?;
            stream
                .write_all(b"GET /exact-recovery-order HTTP/1.1\r\n\r\n")
                .map_err(|_| RecoveryError::Initialization)?;
            Ok(RemoteOrderEvidence::Live)
        }

        async fn cancel_exact(
            &self,
            _order_id: &OrderId,
        ) -> Result<crate::service::recovery_gateway::CancelAttemptEvidence, RecoveryError>
        {
            Err(RecoveryError::Initialization)
        }
    }

    fn test_config(dir: &tempfile::TempDir) -> AppConfig {
        let mut cfg: AppConfig = serde_json::from_str(include_str!("../config.json")).unwrap();
        cfg.trading.execution_ledger_path = dir.path().join("ledger.jsonl");
        cfg.trading.execution_halt_path = dir.path().join("halt.marker");
        cfg
    }

    #[test]
    fn local_recovery_config_keeps_only_the_public_ledger_paths() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(&dir);

        let local = LocalRecoveryConfig::from(&cfg);

        assert_eq!(
            local.execution_ledger_path,
            cfg.trading.execution_ledger_path
        );
        assert_eq!(local.execution_halt_path, cfg.trading.execution_halt_path);
    }

    fn prepared_intent() -> PreparedIntent {
        PreparedIntent {
            order_id: OrderId::from_hex(format!("0x{}", "11".repeat(32))).unwrap(),
            protocol_version: ORDER_PROTOCOL_VERSION,
            venue: Venue::PolymarketClob,
            token_id: TokenId::from_decimal("12345").unwrap(),
            neg_risk: false,
            side: OrderSide::Buy,
            order_type: OrderType::Fok,
            expected_maker_micros: 2_000_000,
            expected_taker_micros: 4_000_000,
            source_hash: None,
            purpose: IntentPurpose::Entry(PositionSeed {
                slug: "question".into(),
                category: "politics".into(),
                tags: vec!["us".into()],
                take_profit_bps: 500,
                stop_loss_bps: 300,
            }),
        }
    }

    #[test]
    fn inspection_output_reveals_the_full_order_id_only_with_the_explicit_flag() {
        let full_order_id = format!("0x{}", "a".repeat(64));
        let order_id = OrderId::from_hex(full_order_id.clone()).unwrap();
        let inspection = RecoveryInspection {
            intent_id: IntentId(uuid::Uuid::nil()),
            action: None,
            challenge: None,
            order_id: Some(order_id.clone()),
            order_id_hint: Some(order_id.to_string()),
        };
        let raw_body_sentinel = "RECOVERY_RAW_BODY_SENTINEL";

        let default_output = render_inspection(&inspection, false);
        let explicit_output = render_inspection(&inspection, true);

        assert!(!default_output.contains(&full_order_id));
        assert!(explicit_output.contains(&full_order_id));
        assert!(!default_output.contains(raw_body_sentinel));
        assert!(!explicit_output.contains(raw_body_sentinel));
        assert!(!service_error(RecoveryServiceError::GatewayFailed)
            .to_string()
            .contains(raw_body_sentinel));
    }

    #[test]
    fn inspect_without_an_intent_selects_only_the_current_active_intent() {
        let dir = tempfile::tempdir().unwrap();
        let ledger = ExecutionLedger::open_live(dir.path().join("ledger.jsonl")).unwrap();
        let explicit = uuid::Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap();

        assert_eq!(
            inspect_intent(&ledger, Some(explicit)).unwrap(),
            Some(IntentId(explicit))
        );
        assert_eq!(inspect_intent(&ledger, None).unwrap(), None);
    }

    #[tokio::test]
    async fn local_inspect_opens_the_lock_bearing_ledger_without_credentials_or_sdk_construction() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg: AppConfig = serde_json::from_str(include_str!("../config.json")).unwrap();
        cfg.trading.execution_ledger_path = dir.path().join("ledger.jsonl");
        cfg.trading.execution_halt_path = dir.path().join("halt.marker");
        cfg.site.clob_api_base = "http://127.0.0.1:1".to_owned();

        run(
            &cfg,
            RecoveryCommand::Inspect {
                intent: None,
                show_order_id: false,
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn injected_loopback_gateway_handles_one_exact_reconcile_without_a_production_host() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(&dir);
        let intent = IntentId(uuid::Uuid::from_u128(12));
        let ledger = ExecutionLedger::open_live(&cfg.trading.execution_ledger_path).unwrap();
        ledger
            .append(intent, LedgerPayload::IntentPrepared(prepared_intent()))
            .unwrap();
        ledger.append(intent, LedgerPayload::SubmitStarted).unwrap();
        drop(ledger);

        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let fixture = std::thread::spawn(move || {
            let (mut stream, peer) = listener.accept().unwrap();
            assert!(peer.ip().is_loopback());
            let mut request = String::new();
            stream.read_to_string(&mut request).unwrap();
            assert_eq!(request, "GET /exact-recovery-order HTTP/1.1\r\n\r\n");
        });
        let gateway = Arc::new(LoopbackExactGateway {
            address,
            calls: AtomicUsize::new(0),
        });

        run_network_with_injected_gateway(
            &cfg,
            RecoveryCommand::Reconcile { intent: intent.0 },
            gateway.as_ref(),
        )
        .await
        .unwrap();

        fixture.join().unwrap();
        assert_eq!(gateway.calls.load(Ordering::SeqCst), 1);
    }
}
