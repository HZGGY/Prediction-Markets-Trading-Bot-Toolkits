#![allow(
    dead_code,
    reason = "Task 10 keeps the recovery service crate-private until Task 12 wires the explicit operator CLI"
)]

use std::{
    fmt, fs, io,
    path::{Path, PathBuf},
    sync::Arc,
};

use chrono::Utc;
use parking_lot::Mutex;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::service::{
    execution_ledger::{
        ActiveIntent, ActiveIntentState, EventHash, ExecutionLedger, IntentId, LedgerPayload,
        MatchedAmounts, OrderId, PositionClose, PositionId,
    },
    order_gateway::PreparedOrderIdentity,
    position_store::{OpenPosition, PositionStore},
    recovery_gateway::{RecoveryGateway, RemoteOrderEvidence},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecoveryAction {
    Apply,
    Acknowledge,
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct ConfirmationChallenge(String);

impl ConfirmationChallenge {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ConfirmationChallenge {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[redacted-confirmation-challenge]")
    }
}

impl fmt::Debug for ConfirmationChallenge {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ConfirmationChallenge({self})")
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct ChallengeMaterial<'a> {
    action: RecoveryAction,
    intent_id: IntentId,
    order_id: &'a OrderId,
    sequence: u64,
    head_hash: &'a EventHash,
}

fn challenge(
    action: RecoveryAction,
    active: &ActiveIntent,
    sequence: u64,
    head_hash: &EventHash,
) -> ConfirmationChallenge {
    challenge_for(
        action,
        active.intent_id,
        &active.prepared.order_id,
        sequence,
        head_hash,
    )
}

fn challenge_for(
    action: RecoveryAction,
    intent_id: IntentId,
    order_id: &OrderId,
    sequence: u64,
    head_hash: &EventHash,
) -> ConfirmationChallenge {
    let material = ChallengeMaterial {
        action,
        intent_id,
        order_id,
        sequence,
        head_hash,
    };
    let bytes = serde_json::to_vec(&material).expect("fixed challenge schema serializes");
    ConfirmationChallenge(hex::encode(Sha256::digest(bytes)))
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct RecoveryInspection {
    pub(crate) intent_id: IntentId,
    pub(crate) action: Option<RecoveryAction>,
    pub(crate) challenge: Option<ConfirmationChallenge>,
    pub(crate) order_id: Option<OrderId>,
    pub(crate) order_id_hint: Option<String>,
}

impl fmt::Display for RecoveryInspection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "recovery_inspection(intent=[redacted-intent-id], action={:?}, order_id_hint={:?}, challenge=[redacted])",
            self.action, self.order_id_hint
        )
    }
}

impl fmt::Debug for RecoveryInspection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

pub(crate) struct RecoveryService {
    ledger: Arc<ExecutionLedger>,
    positions: Arc<PositionStore>,
    halt_marker: PathBuf,
    cleanup: Arc<dyn HaltMarkerCleanup>,
    operation: Mutex<()>,
}

trait HaltMarkerCleanup: Send + Sync {
    fn remove_and_sync(&self, marker: &Path) -> io::Result<()>;
}

struct SystemHaltMarkerCleanup;

impl HaltMarkerCleanup for SystemHaltMarkerCleanup {
    fn remove_and_sync(&self, marker: &Path) -> io::Result<()> {
        if marker.exists() {
            fs::remove_file(marker)?;
        }
        sync_marker_parent(marker)
    }
}

#[cfg(unix)]
fn sync_marker_parent(marker: &Path) -> io::Result<()> {
    std::fs::File::open(marker.parent().unwrap_or_else(|| Path::new(".")))?.sync_all()
}

#[cfg(not(unix))]
fn sync_marker_parent(_: &Path) -> io::Result<()> {
    Ok(())
}

impl RecoveryService {
    pub(crate) fn local(
        ledger: Arc<ExecutionLedger>,
        positions: Arc<PositionStore>,
        halt_marker: PathBuf,
    ) -> Self {
        Self {
            ledger,
            positions,
            halt_marker,
            cleanup: Arc::new(SystemHaltMarkerCleanup),
            operation: Mutex::new(()),
        }
    }

    #[cfg(test)]
    fn local_with_cleanup_failure_for_test(
        ledger: Arc<ExecutionLedger>,
        positions: Arc<PositionStore>,
        halt_marker: PathBuf,
    ) -> Self {
        Self {
            ledger,
            positions,
            halt_marker,
            cleanup: Arc::new(FailOnceCleanup::default()),
            operation: Mutex::new(()),
        }
    }

    pub(crate) fn inspect(
        &self,
        intent_id: IntentId,
        show_order_id: bool,
    ) -> Result<RecoveryInspection, RecoveryServiceError> {
        let projection = self.ledger.projection();
        let active = projection
            .active
            .as_ref()
            .filter(|active| active.intent_id == intent_id)
            .ok_or(RecoveryServiceError::NotApplicable)?;
        let action = available_action(active);
        Ok(RecoveryInspection {
            intent_id,
            action,
            challenge: action.map(|action| {
                challenge(action, active, projection.sequence, &projection.head_hash)
            }),
            order_id: show_order_id.then(|| active.prepared.order_id.clone()),
            order_id_hint: Some(active.prepared.order_id.to_string()),
        })
    }

    pub(crate) async fn reconcile(
        &self,
        gateway: &dyn RecoveryGateway,
        intent_id: IntentId,
    ) -> Result<RecoveryInspection, RecoveryServiceError> {
        let expected = {
            let projection = self.ledger.projection();
            let active = projection
                .active
                .as_ref()
                .filter(|active| active.intent_id == intent_id)
                .ok_or(RecoveryServiceError::NotApplicable)?;
            prepared_identity(&active.prepared)
        };
        self.ledger
            .append(intent_id, LedgerPayload::ReconciliationStarted)
            .map_err(RecoveryServiceError::ledger)?;
        let evidence = gateway
            .reconcile_exact(&expected)
            .await
            .map_err(|_| RecoveryServiceError::GatewayFailed)?;
        let payload = reconcile_payload(evidence, &expected);
        self.ledger
            .append(intent_id, payload)
            .map_err(RecoveryServiceError::ledger)?;
        self.inspect(intent_id, false)
    }

    pub(crate) fn prepare_apply(
        &self,
        intent_id: IntentId,
    ) -> Result<ConfirmationChallenge, RecoveryServiceError> {
        let projection = self.ledger.projection();
        let active = active_for(&projection.active, intent_id)?;
        if !matches!(
            active.state,
            ActiveIntentState::ReconciledMatched | ActiveIntentState::RecoveryPositionRecorded
        ) {
            return Err(RecoveryServiceError::NotApplicable);
        }
        Ok(challenge(
            RecoveryAction::Apply,
            active,
            projection.sequence,
            &projection.head_hash,
        ))
    }

    pub(crate) fn apply(
        &self,
        intent_id: IntentId,
        confirmation: &str,
    ) -> Result<RecoveryApplyResult, RecoveryServiceError> {
        let _operation = self.operation.lock();
        let projection = self.ledger.projection();
        let active = active_for(&projection.active, intent_id)?;
        if active.state == ActiveIntentState::RecoveryApplied {
            return Ok(RecoveryApplyResult {
                status: RecoveryApplyStatus::AlreadyApplied,
                acknowledge_challenge: None,
            });
        }
        if !matches!(
            active.state,
            ActiveIntentState::ReconciledMatched | ActiveIntentState::RecoveryPositionRecorded
        ) {
            return Err(RecoveryServiceError::NotApplicable);
        }
        validate_challenge(
            confirmation,
            RecoveryAction::Apply,
            active,
            projection.sequence,
            &projection.head_hash,
        )?;

        let position_event_id = if active.state == ActiveIntentState::ReconciledMatched {
            self.apply_position(active)?;
            self.ledger
                .projection()
                .active
                .and_then(|active| active.position_event_id)
                .ok_or(RecoveryServiceError::Position)?
        } else {
            if !matches!(
                active.evidence,
                crate::service::execution_ledger::ActiveEvidence::ReconciledMatched(_)
            ) {
                return Err(RecoveryServiceError::NotApplicable);
            }
            active
                .position_event_id
                .ok_or(RecoveryServiceError::Position)?
        };
        self.ledger
            .append(
                intent_id,
                LedgerPayload::RecoveryApplied { position_event_id },
            )
            .map_err(RecoveryServiceError::ledger)?;
        Ok(RecoveryApplyResult {
            status: RecoveryApplyStatus::Applied,
            acknowledge_challenge: Some(self.prepare_acknowledge(intent_id)?),
        })
    }

    pub(crate) fn prepare_acknowledge(
        &self,
        intent_id: IntentId,
    ) -> Result<ConfirmationChallenge, RecoveryServiceError> {
        let projection = self.ledger.projection();
        match projection.active.as_ref() {
            Some(active) if active.intent_id == intent_id => {
                acknowledge_reason(active)?;
                Ok(challenge(
                    RecoveryAction::Acknowledge,
                    active,
                    projection.sequence,
                    &projection.head_hash,
                ))
            }
            Some(_) => Err(RecoveryServiceError::NotApplicable),
            None => {
                let order_id = projection
                    .intent_orders
                    .get(&intent_id)
                    .cloned()
                    .ok_or(RecoveryServiceError::NotApplicable)?;
                Ok(challenge_for(
                    RecoveryAction::Acknowledge,
                    intent_id,
                    &order_id,
                    projection.sequence,
                    &projection.head_hash,
                ))
            }
        }
    }

    pub(crate) fn acknowledge(
        &self,
        intent_id: IntentId,
        confirmation: &str,
    ) -> Result<RecoveryAcknowledgeStatus, RecoveryServiceError> {
        let _operation = self.operation.lock();
        let projection = self.ledger.projection();
        let status = match projection.active.as_ref() {
            Some(active) if active.intent_id == intent_id => {
                let reason = acknowledge_reason(active)?;
                validate_challenge(
                    confirmation,
                    RecoveryAction::Acknowledge,
                    active,
                    projection.sequence,
                    &projection.head_hash,
                )?;
                self.ledger
                    .append(intent_id, LedgerPayload::Acknowledged { reason })
                    .map_err(RecoveryServiceError::ledger)?;
                RecoveryAcknowledgeStatus::Acknowledged
            }
            Some(_) => return Err(RecoveryServiceError::NotApplicable),
            None => {
                let order_id = projection
                    .intent_orders
                    .get(&intent_id)
                    .cloned()
                    .ok_or(RecoveryServiceError::NotApplicable)?;
                let expected = challenge_for(
                    RecoveryAction::Acknowledge,
                    intent_id,
                    &order_id,
                    projection.sequence,
                    &projection.head_hash,
                );
                if confirmation != expected.as_str() {
                    return Err(RecoveryServiceError::StaleChallenge);
                }
                RecoveryAcknowledgeStatus::AlreadyAcknowledged
            }
        };
        self.cleanup
            .remove_and_sync(&self.halt_marker)
            .map_err(|_| RecoveryServiceError::HaltCleanupIncomplete)?;
        Ok(status)
    }

    fn apply_position(&self, active: &ActiveIntent) -> Result<(), RecoveryServiceError> {
        match &active.prepared.purpose {
            crate::service::execution_ledger::IntentPurpose::Entry(_) => {
                self.positions
                    .apply_open(self.position_for_recovery(active)?)
                    .map_err(|_| RecoveryServiceError::Position)?;
            }
            crate::service::execution_ledger::IntentPurpose::Exit { position_id } => {
                let amounts = reconciled_amounts(active)?;
                self.positions
                    .apply_close(PositionClose {
                        position_id: *position_id,
                        closing_intent_id: active.intent_id,
                        closing_order_id: active.prepared.order_id.clone(),
                        shares_micros: amounts.shares_micros,
                        usd_micros: amounts.usd_micros,
                        closed_at: Utc::now(),
                    })
                    .map_err(|_| RecoveryServiceError::Position)?;
            }
        }
        Ok(())
    }

    fn position_for_recovery(
        &self,
        active: &ActiveIntent,
    ) -> Result<OpenPosition, RecoveryServiceError> {
        let crate::service::execution_ledger::IntentPurpose::Entry(seed) = &active.prepared.purpose
        else {
            return Err(RecoveryServiceError::Position);
        };
        let amounts = reconciled_amounts(active)?;
        Ok(OpenPosition {
            position_id: PositionId(active.intent_id.0),
            opening_intent_id: active.intent_id,
            opening_order_id: active.prepared.order_id.clone(),
            venue: active.prepared.venue,
            token_id: active.prepared.token_id,
            slug: seed.slug.clone(),
            category: seed.category.clone(),
            tags: seed.tags.clone(),
            neg_risk: active.prepared.neg_risk,
            side: active.prepared.side,
            shares_micros: amounts.shares_micros,
            usd_notional_micros: amounts.usd_micros,
            take_profit_bps: seed.take_profit_bps,
            stop_loss_bps: seed.stop_loss_bps,
            opened_at: Utc::now(),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryApplyStatus {
    Applied,
    AlreadyApplied,
}

#[derive(Debug)]
pub(crate) struct RecoveryApplyResult {
    pub(crate) status: RecoveryApplyStatus,
    pub(crate) acknowledge_challenge: Option<ConfirmationChallenge>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryAcknowledgeStatus {
    Acknowledged,
    AlreadyAcknowledged,
}

fn active_for(
    active: &Option<ActiveIntent>,
    intent_id: IntentId,
) -> Result<&ActiveIntent, RecoveryServiceError> {
    active
        .as_ref()
        .filter(|active| active.intent_id == intent_id)
        .ok_or(RecoveryServiceError::NotApplicable)
}

fn reconciled_amounts(active: &ActiveIntent) -> Result<MatchedAmounts, RecoveryServiceError> {
    match active.evidence {
        crate::service::execution_ledger::ActiveEvidence::ReconciledMatched(amounts) => Ok(amounts),
        _ => Err(RecoveryServiceError::NotApplicable),
    }
}

fn acknowledge_reason(
    active: &ActiveIntent,
) -> Result<crate::service::execution_ledger::AcknowledgeReason, RecoveryServiceError> {
    match active.state {
        ActiveIntentState::NotSent => {
            Ok(crate::service::execution_ledger::AcknowledgeReason::NotSent)
        }
        ActiveIntentState::ReconciledNoFill => {
            Ok(crate::service::execution_ledger::AcknowledgeReason::ReconciledNoFill)
        }
        ActiveIntentState::RecoveryApplied => {
            Ok(crate::service::execution_ledger::AcknowledgeReason::RecoveryApplied)
        }
        _ => Err(RecoveryServiceError::NotApplicable),
    }
}

fn validate_challenge(
    confirmation: &str,
    action: RecoveryAction,
    active: &ActiveIntent,
    sequence: u64,
    head_hash: &EventHash,
) -> Result<(), RecoveryServiceError> {
    let expected = challenge(action, active, sequence, head_hash);
    (confirmation == expected.as_str())
        .then_some(())
        .ok_or(RecoveryServiceError::StaleChallenge)
}

fn prepared_identity(
    prepared: &crate::service::execution_ledger::PreparedIntent,
) -> PreparedOrderIdentity {
    PreparedOrderIdentity {
        order_id: prepared.order_id.clone(),
        protocol_version: prepared.protocol_version,
        venue: prepared.venue,
        token_id: prepared.token_id,
        neg_risk: prepared.neg_risk,
        side: prepared.side,
        order_type: prepared.order_type,
        expected_maker_micros: prepared.expected_maker_micros,
        expected_taker_micros: prepared.expected_taker_micros,
    }
}

fn reconcile_payload(
    evidence: RemoteOrderEvidence,
    expected: &PreparedOrderIdentity,
) -> LedgerPayload {
    match evidence {
        RemoteOrderEvidence::Matched {
            making_micros,
            taking_micros,
            ..
        } => LedgerPayload::ReconciledMatched(MatchedAmounts {
            shares_micros: match expected.side {
                crate::service::execution_ledger::OrderSide::Buy => taking_micros,
                crate::service::execution_ledger::OrderSide::Sell => making_micros,
            },
            usd_micros: match expected.side {
                crate::service::execution_ledger::OrderSide::Buy => making_micros,
                crate::service::execution_ledger::OrderSide::Sell => taking_micros,
            },
        }),
        RemoteOrderEvidence::NoFill { status } => LedgerPayload::ReconciledNoFill { status },
        RemoteOrderEvidence::Live => LedgerPayload::ReconciledLive,
        RemoteOrderEvidence::Pending => LedgerPayload::ReconciledPending,
        RemoteOrderEvidence::Uncertain { code } => LedgerPayload::ReconciledUncertain { code },
    }
}

fn available_action(active: &ActiveIntent) -> Option<RecoveryAction> {
    match active.state {
        ActiveIntentState::ReconciledMatched | ActiveIntentState::RecoveryPositionRecorded => {
            Some(RecoveryAction::Apply)
        }
        ActiveIntentState::NotSent
        | ActiveIntentState::ReconciledNoFill
        | ActiveIntentState::RecoveryApplied => Some(RecoveryAction::Acknowledge),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryServiceError {
    NotApplicable,
    StaleChallenge,
    GatewayFailed,
    Ledger,
    Position,
    HaltCleanupIncomplete,
}

impl RecoveryServiceError {
    fn ledger(_: crate::service::execution_ledger::LedgerError) -> Self {
        Self::Ledger
    }

    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::NotApplicable => "not_applicable",
            Self::StaleChallenge => "stale_challenge",
            Self::GatewayFailed => "gateway_failed",
            Self::Ledger => "ledger",
            Self::Position => "position",
            Self::HaltCleanupIncomplete => "halt_cleanup_incomplete",
        }
    }
}

impl fmt::Display for RecoveryServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "recovery_service_error(code={})", self.code())
    }
}

impl std::error::Error for RecoveryServiceError {}

#[cfg(test)]
#[derive(Default)]
struct FailOnceCleanup(std::sync::atomic::AtomicBool);

#[cfg(test)]
impl HaltMarkerCleanup for FailOnceCleanup {
    fn remove_and_sync(&self, marker: &Path) -> io::Result<()> {
        if !self.0.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return Err(io::Error::other("injected marker cleanup failure"));
        }
        SystemHaltMarkerCleanup.remove_and_sync(marker)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    use crate::service::{
        execution_ledger::{
            ActiveIntentState, EventHash, ExecutionLedger, IntentId, IntentPurpose, LedgerPayload,
            OrderId, OrderSide, OrderType, PositionSeed, PreparedIntent, ReconcileUncertainCode,
            TerminalNoFillStatus, TokenId, Venue, ORDER_PROTOCOL_VERSION,
        },
        order_gateway::PreparedOrderIdentity,
        position_store::{OpenPosition, PositionStore},
        recovery_gateway::{
            CancelAttemptEvidence, RecoveryError, RecoveryGateway, RemoteOrderEvidence,
        },
    };
    use async_trait::async_trait;

    use super::{
        challenge, challenge_for, ConfirmationChallenge, RecoveryAction, RecoveryService,
        RecoveryServiceError,
    };

    fn order_id(byte: u8) -> OrderId {
        OrderId::from_hex(format!("0x{}", format!("{byte:02x}").repeat(32))).unwrap()
    }

    fn prepared() -> PreparedIntent {
        PreparedIntent {
            order_id: order_id(0x11),
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

    struct CountingGateway {
        calls: AtomicUsize,
        result: Result<RemoteOrderEvidence, RecoveryError>,
    }

    #[async_trait]
    impl RecoveryGateway for CountingGateway {
        async fn reconcile_exact(
            &self,
            _expected: &PreparedOrderIdentity,
        ) -> Result<RemoteOrderEvidence, RecoveryError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.result.clone()
        }

        async fn cancel_exact(
            &self,
            _order_id: &OrderId,
        ) -> Result<CancelAttemptEvidence, RecoveryError> {
            unreachable!("Task 10 must not cancel")
        }
    }

    fn active_service() -> (
        tempfile::TempDir,
        Arc<ExecutionLedger>,
        Arc<PositionStore>,
        IntentId,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let ledger = Arc::new(
            ExecutionLedger::open_live(dir.path().join("execution-ledger.jsonl")).unwrap(),
        );
        let intent_id = IntentId(uuid::Uuid::from_u128(1));
        ledger
            .append(intent_id, LedgerPayload::IntentPrepared(prepared()))
            .unwrap();
        ledger
            .append(intent_id, LedgerPayload::SubmitStarted)
            .unwrap();
        let positions = PositionStore::from_ledger(Arc::clone(&ledger)).unwrap();
        (dir, ledger, positions, intent_id)
    }

    fn reconciled_matched_service() -> (
        tempfile::TempDir,
        Arc<ExecutionLedger>,
        Arc<PositionStore>,
        IntentId,
        RecoveryService,
    ) {
        let (dir, ledger, positions, intent_id) = active_service();
        ledger
            .append(intent_id, LedgerPayload::ReconciliationStarted)
            .unwrap();
        ledger
            .append(
                intent_id,
                LedgerPayload::ReconciledMatched(
                    crate::service::execution_ledger::MatchedAmounts {
                        shares_micros: 4_000_000,
                        usd_micros: 2_000_000,
                    },
                ),
            )
            .unwrap();
        let service = RecoveryService::local(
            Arc::clone(&ledger),
            Arc::clone(&positions),
            dir.path().join("execution-halt.json"),
        );
        (dir, ledger, positions, intent_id, service)
    }

    #[test]
    fn inspect_is_local_only_and_offers_only_the_fresh_notsent_acknowledge_challenge() {
        let dir = tempfile::tempdir().unwrap();
        let ledger = Arc::new(
            ExecutionLedger::open_live(dir.path().join("execution-ledger.jsonl")).unwrap(),
        );
        let intent_id = IntentId(uuid::Uuid::from_u128(1));
        ledger
            .append(intent_id, LedgerPayload::IntentPrepared(prepared()))
            .unwrap();
        let positions = PositionStore::from_ledger(Arc::clone(&ledger)).unwrap();
        let service =
            RecoveryService::local(ledger, positions, dir.path().join("execution-halt.json"));

        let inspection = service.inspect(intent_id, false).unwrap();
        let explicit = service.inspect(intent_id, true).unwrap();

        assert_eq!(inspection.intent_id, intent_id);
        assert_eq!(inspection.action, Some(RecoveryAction::Acknowledge));
        assert!(inspection.order_id.is_none());
        assert!(inspection.order_id_hint.is_some());
        assert!(inspection.challenge.is_some());
        assert_eq!(
            explicit.order_id.as_ref().unwrap().as_str(),
            prepared().order_id.as_str()
        );
        let rendered = format!("{inspection:?} {inspection}");
        assert!(!rendered.contains(prepared().order_id.as_str()));
        assert!(!rendered.contains(inspection.challenge.as_ref().unwrap().as_str()));
    }

    #[test]
    fn challenge_uses_the_fixed_lowercase_sha256_schema_and_binds_every_identity_field() {
        let intent = IntentId(uuid::Uuid::from_u128(1));
        let order = order_id(0x11);
        let head = EventHash::from_bytes([0x22; 32]);
        let acknowledgement = challenge_for(RecoveryAction::Acknowledge, intent, &order, 7, &head);

        assert_eq!(
            acknowledgement.as_str(),
            "b6a651a96cdc416ac6677600f700795ea3574f3d3b1e91be0d943d5f2ff0687e"
        );
        assert_eq!(acknowledgement.as_str().len(), 64);
        assert!(acknowledgement
            .as_str()
            .bytes()
            .all(|value| value.is_ascii_digit() || (b'a'..=b'f').contains(&value)));
        assert_ne!(
            acknowledgement,
            challenge_for(RecoveryAction::Apply, intent, &order, 7, &head)
        );
        assert_ne!(
            acknowledgement,
            challenge_for(RecoveryAction::Acknowledge, intent, &order, 8, &head)
        );
    }

    #[test]
    fn local_inspection_never_resumes_or_clears_an_unresolved_halt() {
        let (dir, ledger, positions, intent_id) = active_service();
        let marker = dir.path().join("execution-halt.json");
        fs::write(&marker, b"legacy halt").unwrap();
        let service = RecoveryService::local(Arc::clone(&ledger), positions, marker.clone());
        let before = ledger.projection();

        service.inspect(intent_id, false).unwrap();

        assert_eq!(ledger.projection().sequence, before.sequence);
        assert!(ledger.projection().active.is_some());
        assert!(marker.exists());
    }

    #[tokio::test]
    async fn reconcile_durably_starts_once_calls_exact_gateway_once_and_classifies_every_remote_result(
    ) {
        let cases = [
            (
                RemoteOrderEvidence::Matched {
                    making_micros: 2_000_000,
                    taking_micros: 4_000_000,
                    trade_ids: vec![],
                },
                ActiveIntentState::ReconciledMatched,
                Some(RecoveryAction::Apply),
            ),
            (
                RemoteOrderEvidence::NoFill {
                    status: TerminalNoFillStatus::Canceled,
                },
                ActiveIntentState::ReconciledNoFill,
                Some(RecoveryAction::Acknowledge),
            ),
            (
                RemoteOrderEvidence::Live,
                ActiveIntentState::ReconciledLive,
                None,
            ),
            (
                RemoteOrderEvidence::Pending,
                ActiveIntentState::ReconciledPending,
                None,
            ),
            (
                RemoteOrderEvidence::Uncertain {
                    code: ReconcileUncertainCode::Timeout,
                },
                ActiveIntentState::ReconciledUncertain,
                None,
            ),
        ];

        for (remote, expected_state, expected_action) in cases {
            let (dir, ledger, positions, intent_id) = active_service();
            let service = RecoveryService::local(
                Arc::clone(&ledger),
                Arc::clone(&positions),
                dir.path().join("execution-halt.json"),
            );
            let gateway = CountingGateway {
                calls: AtomicUsize::new(0),
                result: Ok(remote),
            };

            let outcome = service.reconcile(&gateway, intent_id).await.unwrap();

            assert_eq!(gateway.calls.load(Ordering::SeqCst), 1);
            assert_eq!(ledger.projection().event_count, 4);
            assert_eq!(ledger.projection().active.unwrap().state, expected_state);
            assert!(positions.is_empty());
            assert_eq!(outcome.action, expected_action);
        }
    }

    #[tokio::test]
    async fn reconcile_gateway_failure_keeps_the_durable_started_halt_and_never_classifies() {
        let (dir, ledger, positions, intent_id) = active_service();
        let service = RecoveryService::local(
            Arc::clone(&ledger),
            positions,
            dir.path().join("execution-halt.json"),
        );
        let gateway = CountingGateway {
            calls: AtomicUsize::new(0),
            result: Err(RecoveryError::Initialization),
        };

        let error = service.reconcile(&gateway, intent_id).await.unwrap_err();

        assert_eq!(error.code(), "gateway_failed");
        assert_eq!(gateway.calls.load(Ordering::SeqCst), 1);
        assert_eq!(ledger.projection().event_count, 3);
        assert_eq!(
            ledger.projection().active.unwrap().state,
            ActiveIntentState::ReconciliationStarted
        );
    }

    #[test]
    fn apply_requires_a_current_exact_challenge_before_any_position_or_ledger_mutation() {
        let (_dir, ledger, positions, intent_id, service) = reconciled_matched_service();
        let projection = ledger.projection();
        let active = projection.active.unwrap();
        let mut wrong_intent = active.clone();
        wrong_intent.intent_id = IntentId(uuid::Uuid::from_u128(99));
        let mut wrong_order = active.clone();
        wrong_order.prepared.order_id = order_id(0x99);
        let mismatches = [
            challenge(
                RecoveryAction::Acknowledge,
                &active,
                projection.sequence,
                &projection.head_hash,
            ),
            challenge(
                RecoveryAction::Apply,
                &wrong_intent,
                projection.sequence,
                &projection.head_hash,
            ),
            challenge(
                RecoveryAction::Apply,
                &wrong_order,
                projection.sequence,
                &projection.head_hash,
            ),
            challenge(
                RecoveryAction::Apply,
                &active,
                projection.sequence + 1,
                &projection.head_hash,
            ),
            challenge(
                RecoveryAction::Apply,
                &active,
                projection.sequence,
                &EventHash::from_bytes([0x33; 32]),
            ),
            ConfirmationChallenge("00".repeat(32)),
        ];

        for confirmation in mismatches {
            assert_eq!(
                service.apply(intent_id, confirmation.as_str()).unwrap_err(),
                RecoveryServiceError::StaleChallenge
            );
            assert_eq!(ledger.projection().sequence, projection.sequence);
            assert!(positions.is_empty());
        }
    }

    #[test]
    fn apply_appends_the_exact_position_then_recovery_applied_and_yields_a_new_acknowledge_challenge(
    ) {
        let (_dir, ledger, positions, intent_id, service) = reconciled_matched_service();
        let confirmation = service.prepare_apply(intent_id).unwrap();

        let outcome = service.apply(intent_id, confirmation.as_str()).unwrap();

        assert!(outcome.acknowledge_challenge.is_some());
        assert_eq!(positions.len(), 1);
        let active = ledger.projection().active.unwrap();
        assert_eq!(active.state, ActiveIntentState::RecoveryApplied);
        assert!(active.position_event_id.is_some());
        assert_eq!(ledger.projection().event_count, 6);
    }

    #[test]
    fn duplicate_apply_is_an_idempotent_noop_after_recovery_applied() {
        let (_dir, ledger, positions, intent_id, service) = reconciled_matched_service();
        let confirmation = service.prepare_apply(intent_id).unwrap();
        service.apply(intent_id, confirmation.as_str()).unwrap();
        let sequence = ledger.projection().sequence;

        let duplicate = service.apply(intent_id, confirmation.as_str()).unwrap();

        assert_eq!(duplicate.status, super::RecoveryApplyStatus::AlreadyApplied);
        assert!(duplicate.acknowledge_challenge.is_none());
        assert_eq!(ledger.projection().sequence, sequence);
        assert_eq!(positions.len(), 1);
    }

    #[test]
    fn apply_resumes_a_crash_after_the_position_event_without_a_duplicate_position_mutation() {
        let (_dir, ledger, positions, intent_id, service) = reconciled_matched_service();
        let first_confirmation = service.prepare_apply(intent_id).unwrap();
        let projection = ledger.projection();
        let active = projection.active.unwrap();
        let position = service.position_for_recovery(&active).unwrap();
        positions.apply_open(position).unwrap();
        assert_eq!(
            ledger.projection().active.unwrap().state,
            ActiveIntentState::RecoveryPositionRecorded
        );

        assert_eq!(
            service
                .apply(intent_id, first_confirmation.as_str())
                .unwrap_err(),
            RecoveryServiceError::StaleChallenge
        );
        let recovery_confirmation = service.prepare_apply(intent_id).unwrap();
        let outcome = service
            .apply(intent_id, recovery_confirmation.as_str())
            .unwrap();

        assert!(outcome.acknowledge_challenge.is_some());
        assert_eq!(positions.len(), 1);
        assert_eq!(ledger.projection().event_count, 6);
        assert_eq!(
            ledger.projection().active.unwrap().state,
            ActiveIntentState::RecoveryApplied
        );
    }

    #[test]
    fn apply_closes_only_the_exact_durable_exit_position() {
        let dir = tempfile::tempdir().unwrap();
        let ledger = Arc::new(
            ExecutionLedger::open_live(dir.path().join("execution-ledger.jsonl")).unwrap(),
        );
        let positions = PositionStore::from_ledger(Arc::clone(&ledger)).unwrap();
        let opening_intent = IntentId(uuid::Uuid::from_u128(20));
        let open = OpenPosition {
            position_id: crate::service::execution_ledger::PositionId(opening_intent.0),
            opening_intent_id: opening_intent,
            opening_order_id: order_id(0x20),
            venue: Venue::PolymarketClob,
            token_id: TokenId::from_decimal("12345").unwrap(),
            slug: "question".into(),
            category: "politics".into(),
            tags: vec!["us".into()],
            neg_risk: false,
            side: OrderSide::Buy,
            shares_micros: 4_000_000,
            usd_notional_micros: 2_000_000,
            take_profit_bps: 500,
            stop_loss_bps: 300,
            opened_at: chrono::Utc::now(),
        };
        let mut open_prepared = prepared();
        open_prepared.order_id = open.opening_order_id.clone();
        ledger
            .append(opening_intent, LedgerPayload::IntentPrepared(open_prepared))
            .unwrap();
        ledger
            .append(opening_intent, LedgerPayload::SubmitStarted)
            .unwrap();
        ledger
            .append(
                opening_intent,
                LedgerPayload::RemoteMatched(crate::service::execution_ledger::MatchedAmounts {
                    shares_micros: open.shares_micros,
                    usd_micros: open.usd_notional_micros,
                }),
            )
            .unwrap();
        positions.apply_open(open.clone()).unwrap();
        ledger
            .append(opening_intent, LedgerPayload::SubmissionCommitted)
            .unwrap();

        let exit_intent = IntentId(uuid::Uuid::from_u128(21));
        ledger
            .append(
                exit_intent,
                LedgerPayload::IntentPrepared(PreparedIntent {
                    order_id: order_id(0x21),
                    protocol_version: ORDER_PROTOCOL_VERSION,
                    venue: open.venue,
                    token_id: open.token_id,
                    neg_risk: open.neg_risk,
                    side: OrderSide::Sell,
                    order_type: OrderType::Fok,
                    expected_maker_micros: open.shares_micros,
                    expected_taker_micros: open.usd_notional_micros,
                    source_hash: None,
                    purpose: IntentPurpose::Exit {
                        position_id: open.position_id,
                    },
                }),
            )
            .unwrap();
        ledger
            .append(exit_intent, LedgerPayload::SubmitStarted)
            .unwrap();
        ledger
            .append(exit_intent, LedgerPayload::ReconciliationStarted)
            .unwrap();
        ledger
            .append(
                exit_intent,
                LedgerPayload::ReconciledMatched(
                    crate::service::execution_ledger::MatchedAmounts {
                        shares_micros: open.shares_micros,
                        usd_micros: open.usd_notional_micros,
                    },
                ),
            )
            .unwrap();
        let service = RecoveryService::local(
            Arc::clone(&ledger),
            Arc::clone(&positions),
            dir.path().join("execution-halt.json"),
        );

        let confirmation = service.prepare_apply(exit_intent).unwrap();
        service.apply(exit_intent, confirmation.as_str()).unwrap();

        assert!(positions.is_empty());
        assert_eq!(
            ledger.projection().active.unwrap().state,
            ActiveIntentState::RecoveryApplied
        );
    }

    #[test]
    fn acknowledge_is_limited_to_notsent_nofill_or_applied_and_marker_deletion_never_changes_an_active_ledger(
    ) {
        let cases = [
            (LedgerPayload::ReconciledLive, false),
            (LedgerPayload::ReconciledPending, false),
            (
                LedgerPayload::ReconciledUncertain {
                    code: ReconcileUncertainCode::NotFound,
                },
                false,
            ),
            (
                LedgerPayload::ReconciledNoFill {
                    status: TerminalNoFillStatus::Rejected,
                },
                true,
            ),
        ];
        for (classification, allowed) in cases {
            let (dir, ledger, positions, intent_id) = active_service();
            ledger
                .append(intent_id, LedgerPayload::ReconciliationStarted)
                .unwrap();
            ledger.append(intent_id, classification).unwrap();
            let marker = dir.path().join("execution-halt.json");
            fs::write(&marker, b"legacy halt").unwrap();
            let service = RecoveryService::local(Arc::clone(&ledger), positions, marker.clone());

            let prepared_acknowledgement = service.prepare_acknowledge(intent_id);
            assert_eq!(prepared_acknowledgement.is_ok(), allowed);
            if allowed {
                service
                    .acknowledge(intent_id, prepared_acknowledgement.unwrap().as_str())
                    .unwrap();
                assert!(ledger.projection().active.is_none());
                assert!(!marker.exists());
            } else {
                fs::remove_file(&marker).unwrap();
                assert!(ledger.projection().active.is_some());
                assert!(service.prepare_acknowledge(intent_id).is_err());
            }
        }
    }

    #[test]
    fn acknowledge_publishes_the_clear_before_marker_cleanup_and_retries_cleanup_idempotently() {
        let dir = tempfile::tempdir().unwrap();
        let ledger = Arc::new(
            ExecutionLedger::open_live(dir.path().join("execution-ledger.jsonl")).unwrap(),
        );
        let intent_id = IntentId(uuid::Uuid::from_u128(7));
        ledger
            .append(intent_id, LedgerPayload::IntentPrepared(prepared()))
            .unwrap();
        let positions = PositionStore::from_ledger(Arc::clone(&ledger)).unwrap();
        let marker = dir.path().join("execution-halt.json");
        fs::write(&marker, b"legacy halt").unwrap();
        let service = RecoveryService::local_with_cleanup_failure_for_test(
            Arc::clone(&ledger),
            positions,
            marker.clone(),
        );
        let confirmation = service.prepare_acknowledge(intent_id).unwrap();

        assert_eq!(
            service
                .acknowledge(intent_id, "not-a-challenge")
                .unwrap_err(),
            RecoveryServiceError::StaleChallenge
        );
        assert!(ledger.projection().active.is_some());
        assert!(marker.exists());

        assert_eq!(
            service
                .acknowledge(intent_id, confirmation.as_str())
                .unwrap_err(),
            RecoveryServiceError::HaltCleanupIncomplete
        );
        assert!(ledger.projection().active.is_none());
        assert!(marker.exists());

        let retry = service.prepare_acknowledge(intent_id).unwrap();
        service.acknowledge(intent_id, retry.as_str()).unwrap();
        assert!(ledger.projection().active.is_none());
        assert!(!marker.exists());

        let repeat = service.prepare_acknowledge(intent_id).unwrap();
        service.acknowledge(intent_id, repeat.as_str()).unwrap();
        assert!(!marker.exists());
    }
}
