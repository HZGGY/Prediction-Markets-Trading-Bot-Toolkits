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
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::service::{
    execution_ledger::{
        ActiveIntent, ActiveIntentState, CancelResponseClass, EventHash, ExecutionLedger, IntentId,
        LedgerPayload, MatchedAmounts, OrderId, PositionClose, PositionId,
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
    Cancel,
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
    #[cfg(test)]
    fail_cleanup_completion_append: std::sync::atomic::AtomicBool,
}

trait HaltMarkerCleanup: Send + Sync {
    fn remove_and_sync(&self, marker: &Path) -> io::Result<()>;
}

struct SystemHaltMarkerCleanup;

impl HaltMarkerCleanup for SystemHaltMarkerCleanup {
    fn remove_and_sync(&self, marker: &Path) -> io::Result<()> {
        #[cfg(unix)]
        let parent = open_marker_parent(marker)?;
        #[cfg(not(unix))]
        validate_marker_parent(marker)?;
        if marker.exists() {
            fs::remove_file(marker)?;
        }
        #[cfg(unix)]
        {
            return sync_marker_parent(parent);
        }
        #[cfg(not(unix))]
        {
            sync_marker_parent(marker)
        }
    }
}

fn open_marker_parent(marker: &Path) -> io::Result<std::fs::File> {
    let parent = marker.parent().unwrap_or_else(|| Path::new("."));
    validate_marker_parent(marker)?;
    std::fs::File::open(parent)
}

fn validate_marker_parent(marker: &Path) -> io::Result<()> {
    let parent = marker.parent().unwrap_or_else(|| Path::new("."));
    parent.is_dir().then_some(()).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "halt marker parent is not a directory",
        )
    })
}

#[cfg(unix)]
fn sync_marker_parent(parent: std::fs::File) -> io::Result<()> {
    parent.sync_all()
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
            #[cfg(test)]
            fail_cleanup_completion_append: std::sync::atomic::AtomicBool::new(false),
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
            fail_cleanup_completion_append: std::sync::atomic::AtomicBool::new(false),
        }
    }

    #[cfg(test)]
    fn local_with_post_remove_sync_failure_for_test(
        ledger: Arc<ExecutionLedger>,
        positions: Arc<PositionStore>,
        halt_marker: PathBuf,
    ) -> Self {
        Self {
            ledger,
            positions,
            halt_marker,
            cleanup: Arc::new(RemoveThenFailOnceCleanup::default()),
            operation: Mutex::new(()),
            fail_cleanup_completion_append: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub(crate) fn inspect(
        &self,
        intent_id: IntentId,
        show_order_id: bool,
    ) -> Result<RecoveryInspection, RecoveryServiceError> {
        let projection = self.ledger.projection();
        if let Some(active) = projection
            .active
            .as_ref()
            .filter(|active| active.intent_id == intent_id)
        {
            let action = available_action(active);
            return Ok(RecoveryInspection {
                intent_id,
                action,
                challenge: action.map(|action| {
                    challenge(action, active, projection.sequence, &projection.head_hash)
                }),
                order_id: show_order_id.then(|| active.prepared.order_id.clone()),
                order_id_hint: Some(active.prepared.order_id.to_string()),
            });
        }
        let pending = cleanup_for(&projection, intent_id)?;
        Ok(RecoveryInspection {
            intent_id,
            action: Some(RecoveryAction::Acknowledge),
            challenge: Some(challenge_for(
                RecoveryAction::Acknowledge,
                intent_id,
                &pending.order_id,
                projection.sequence,
                &projection.head_hash,
            )),
            order_id: show_order_id.then(|| pending.order_id.clone()),
            order_id_hint: Some(pending.order_id.to_string()),
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

    pub(crate) async fn prepare_cancel(
        &self,
        gateway: &dyn RecoveryGateway,
        intent_id: IntentId,
    ) -> Result<ConfirmationChallenge, RecoveryServiceError> {
        let _operation = self.operation.lock().await;
        let expected = {
            let projection = self.ledger.projection();
            prepared_identity(&active_for(&projection.active, intent_id)?.prepared)
        };
        self.ledger
            .append(intent_id, LedgerPayload::ReconciliationStarted)
            .map_err(RecoveryServiceError::ledger)?;
        let evidence = gateway
            .reconcile_exact(&expected)
            .await
            .map_err(|_| RecoveryServiceError::GatewayFailed)?;
        self.ledger
            .append(intent_id, reconcile_payload(evidence, &expected))
            .map_err(RecoveryServiceError::ledger)?;
        let projection = self.ledger.projection();
        let active = active_for(&projection.active, intent_id)?;
        if active.state != ActiveIntentState::ReconciledLive {
            return Err(RecoveryServiceError::NotApplicable);
        }
        Ok(challenge(
            RecoveryAction::Cancel,
            active,
            projection.sequence,
            &projection.head_hash,
        ))
    }

    pub(crate) async fn cancel(
        &self,
        gateway: &dyn RecoveryGateway,
        intent_id: IntentId,
        confirmation: &str,
    ) -> Result<RecoveryInspection, RecoveryServiceError> {
        let _operation = self.operation.lock().await;
        let projection = self.ledger.projection();
        let active = active_for(&projection.active, intent_id)?;
        if active.state != ActiveIntentState::ReconciledLive {
            return Err(RecoveryServiceError::NotApplicable);
        }
        validate_challenge(
            confirmation,
            RecoveryAction::Cancel,
            active,
            projection.sequence,
            &projection.head_hash,
        )?;
        let expected = prepared_identity(&active.prepared);
        let order_id = active.prepared.order_id.clone();
        self.ledger
            .append(intent_id, LedgerPayload::CancelStarted)
            .map_err(RecoveryServiceError::ledger)?;
        let cancel_result = match gateway.cancel_exact(&order_id).await {
            Ok(result) => result,
            Err(_) => crate::service::recovery_gateway::CancelAttemptEvidence::Uncertain {
                code: crate::service::recovery_gateway::CancelUncertainCode::Transport,
            },
        };
        self.ledger
            .append(
                intent_id,
                LedgerPayload::CancelResponseObserved {
                    result: match cancel_result {
                        crate::service::recovery_gateway::CancelAttemptEvidence::Canceled => {
                            CancelResponseClass::Canceled
                        }
                        crate::service::recovery_gateway::CancelAttemptEvidence::NotCanceled => {
                            CancelResponseClass::NotCanceled
                        }
                        crate::service::recovery_gateway::CancelAttemptEvidence::Uncertain {
                            code,
                        } => CancelResponseClass::Uncertain { code },
                    },
                },
            )
            .map_err(RecoveryServiceError::ledger)?;
        self.ledger
            .append(intent_id, LedgerPayload::ReconciliationStarted)
            .map_err(RecoveryServiceError::ledger)?;
        let evidence = match gateway.reconcile_exact(&expected).await {
            Ok(evidence) => evidence,
            Err(_) => RemoteOrderEvidence::Uncertain {
                code: crate::service::execution_ledger::ReconcileUncertainCode::Transport,
            },
        };
        self.ledger
            .append(intent_id, reconcile_payload(evidence, &expected))
            .map_err(RecoveryServiceError::ledger)?;
        self.inspect(intent_id, false)
    }

    #[cfg(test)]
    fn fail_next_cleanup_completion_append(&self) {
        self.fail_cleanup_completion_append
            .store(true, std::sync::atomic::Ordering::SeqCst);
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
        let _operation = self
            .operation
            .try_lock()
            .map_err(|_| RecoveryServiceError::OperationBusy)?;
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

        let position_event_id = if let Some(position_event_id) = active.position_event_id {
            self.validate_recorded_position(active, position_event_id)?;
            position_event_id
        } else if active.state == ActiveIntentState::ReconciledMatched {
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
                let pending = cleanup_for(&projection, intent_id)?;
                Ok(challenge_for(
                    RecoveryAction::Acknowledge,
                    intent_id,
                    &pending.order_id,
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
        let _operation = self
            .operation
            .try_lock()
            .map_err(|_| RecoveryServiceError::OperationBusy)?;
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
                let pending = cleanup_for(&projection, intent_id)?;
                let expected = challenge_for(
                    RecoveryAction::Acknowledge,
                    intent_id,
                    &pending.order_id,
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
        #[cfg(test)]
        if self
            .fail_cleanup_completion_append
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(RecoveryServiceError::Ledger);
        }
        self.ledger
            .append(intent_id, LedgerPayload::HaltMarkerCleanupCompleted)
            .map_err(RecoveryServiceError::ledger)?;
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

    fn validate_recorded_position(
        &self,
        active: &ActiveIntent,
        position_event_id: crate::service::execution_ledger::EventId,
    ) -> Result<(), RecoveryServiceError> {
        let amounts = reconciled_amounts(active)?;
        let event = self
            .ledger
            .event(position_event_id)
            .ok_or(RecoveryServiceError::Position)?;
        if event.intent_id != active.intent_id {
            return Err(RecoveryServiceError::Position);
        }
        let projection = self.ledger.projection();
        match (&active.prepared.purpose, event.payload) {
            (
                crate::service::execution_ledger::IntentPurpose::Entry(seed),
                LedgerPayload::PositionOpened(position),
            ) => {
                let exact = position.position_id
                    == crate::service::execution_ledger::PositionId(active.intent_id.0)
                    && position.opening_intent_id == active.intent_id
                    && position.opening_order_id == active.prepared.order_id
                    && position.venue == active.prepared.venue
                    && position.token_id == active.prepared.token_id
                    && position.slug == seed.slug
                    && position.category == seed.category
                    && position.tags == seed.tags
                    && position.neg_risk == active.prepared.neg_risk
                    && position.side == active.prepared.side
                    && position.entry_shares_micros == amounts.shares_micros
                    && position.entry_usd_micros == amounts.usd_micros
                    && projection.positions.get(&position.position_id) == Some(&position);
                exact.then_some(()).ok_or(RecoveryServiceError::Position)
            }
            (
                crate::service::execution_ledger::IntentPurpose::Exit { position_id },
                LedgerPayload::PositionClosed(close),
            ) => {
                let exact = close.position_id == *position_id
                    && close.closing_intent_id == active.intent_id
                    && close.closing_order_id == active.prepared.order_id
                    && close.shares_micros == amounts.shares_micros
                    && close.usd_micros == amounts.usd_micros
                    && projection
                        .positions
                        .get(position_id)
                        .is_some_and(|position| {
                            position.closing_intent_id == Some(active.intent_id)
                                && position.closing_order_id.as_ref()
                                    == Some(&active.prepared.order_id)
                                && position.closing_shares_micros == Some(amounts.shares_micros)
                                && position.closing_usd_micros == Some(amounts.usd_micros)
                                && position.closed_at == Some(close.closed_at)
                        });
                exact.then_some(()).ok_or(RecoveryServiceError::Position)
            }
            _ => Err(RecoveryServiceError::Position),
        }
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

fn cleanup_for(
    projection: &crate::service::execution_ledger::LedgerProjectionSnapshot,
    intent_id: IntentId,
) -> Result<&crate::service::execution_ledger::CleanupPending, RecoveryServiceError> {
    projection
        .cleanup_pending
        .as_ref()
        .filter(|pending| pending.intent_id == intent_id)
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
    OperationBusy,
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
            Self::OperationBusy => "operation_busy",
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
#[derive(Default)]
struct RemoveThenFailOnceCleanup(std::sync::atomic::AtomicBool);

#[cfg(test)]
impl HaltMarkerCleanup for RemoveThenFailOnceCleanup {
    fn remove_and_sync(&self, marker: &Path) -> io::Result<()> {
        if !self.0.swap(true, std::sync::atomic::Ordering::SeqCst) {
            validate_marker_parent(marker)?;
            if marker.exists() {
                fs::remove_file(marker)?;
            }
            return Err(io::Error::other("injected post-remove parent-sync failure"));
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
            Arc, Mutex,
        },
    };

    use crate::service::{
        execution_ledger::{
            ActiveEvidence, ActiveIntentState, EventHash, ExecutionLedger, IntentId, IntentPurpose,
            LedgerPayload, OrderId, OrderSide, OrderType, PositionSeed, PreparedIntent,
            ReconcileUncertainCode, TerminalNoFillStatus, TokenId, Venue, ORDER_PROTOCOL_VERSION,
        },
        order_gateway::PreparedOrderIdentity,
        position_store::{OpenPosition, PositionStore},
        recovery_gateway::{
            CancelAttemptEvidence, CancelUncertainCode, RecoveryError, RecoveryGateway,
            RemoteOrderEvidence, TradeId,
        },
    };
    use async_trait::async_trait;

    use super::{
        challenge, challenge_for, prepared_identity, ConfirmationChallenge, RecoveryAction,
        RecoveryService, RecoveryServiceError,
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

    struct ScriptedCancelGateway {
        ledger: Arc<ExecutionLedger>,
        reconcile_results: Mutex<Vec<Result<RemoteOrderEvidence, RecoveryError>>>,
        cancel_results: Mutex<Vec<Result<CancelAttemptEvidence, RecoveryError>>>,
        reconcile_calls: AtomicUsize,
        cancel_calls: AtomicUsize,
        cancel_saw_started: AtomicUsize,
        identities: Mutex<Vec<PreparedOrderIdentity>>,
        canceled_orders: Mutex<Vec<OrderId>>,
    }

    impl ScriptedCancelGateway {
        fn new(
            ledger: Arc<ExecutionLedger>,
            reconcile_results: Vec<RemoteOrderEvidence>,
            cancel_result: CancelAttemptEvidence,
        ) -> Self {
            Self {
                ledger,
                reconcile_results: Mutex::new(reconcile_results.into_iter().map(Ok).collect()),
                cancel_results: Mutex::new(vec![Ok(cancel_result)]),
                reconcile_calls: AtomicUsize::new(0),
                cancel_calls: AtomicUsize::new(0),
                cancel_saw_started: AtomicUsize::new(0),
                identities: Mutex::new(Vec::new()),
                canceled_orders: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl RecoveryGateway for ScriptedCancelGateway {
        async fn reconcile_exact(
            &self,
            expected: &PreparedOrderIdentity,
        ) -> Result<RemoteOrderEvidence, RecoveryError> {
            self.reconcile_calls.fetch_add(1, Ordering::SeqCst);
            self.identities.lock().unwrap().push(expected.clone());
            self.reconcile_results.lock().unwrap().remove(0)
        }

        async fn cancel_exact(
            &self,
            order_id: &OrderId,
        ) -> Result<CancelAttemptEvidence, RecoveryError> {
            self.cancel_calls.fetch_add(1, Ordering::SeqCst);
            self.canceled_orders.lock().unwrap().push(order_id.clone());
            if self.ledger.projection().active.unwrap().state == ActiveIntentState::CancelStarted {
                self.cancel_saw_started.store(1, Ordering::SeqCst);
            }
            self.cancel_results.lock().unwrap().remove(0)
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

    #[test]
    fn normally_committed_history_cannot_authorize_unrelated_marker_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("execution-halt.json");
        let ledger = Arc::new(
            ExecutionLedger::open_live(dir.path().join("execution-ledger.jsonl")).unwrap(),
        );
        let intent_id = IntentId(uuid::Uuid::from_u128(91));
        ledger
            .append(intent_id, LedgerPayload::IntentPrepared(prepared()))
            .unwrap();
        ledger
            .append(intent_id, LedgerPayload::SubmitStarted)
            .unwrap();
        ledger
            .append(
                intent_id,
                LedgerPayload::RemoteRejected {
                    code: crate::service::execution_ledger::RemoteRejectCode::ServerRejected,
                },
            )
            .unwrap();
        ledger
            .append(intent_id, LedgerPayload::SubmissionCommittedNoFill)
            .unwrap();
        fs::write(&marker, b"unrelated compatibility marker").unwrap();
        let positions = PositionStore::from_ledger(Arc::clone(&ledger)).unwrap();
        let service = RecoveryService::local(ledger, positions, marker.clone());

        assert_eq!(
            service.prepare_acknowledge(intent_id).unwrap_err(),
            RecoveryServiceError::NotApplicable
        );
        assert_eq!(
            service.inspect(intent_id, false).unwrap_err(),
            RecoveryServiceError::NotApplicable
        );
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
            Arc::clone(&positions),
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

    #[tokio::test]
    async fn prepare_cancel_freshly_reconciles_only_the_exact_active_identity_and_challenges_live()
    {
        let (dir, ledger, positions, intent_id) = active_service();
        let service = RecoveryService::local(
            Arc::clone(&ledger),
            Arc::clone(&positions),
            dir.path().join("execution-halt.json"),
        );
        let gateway = ScriptedCancelGateway::new(
            Arc::clone(&ledger),
            vec![RemoteOrderEvidence::Live],
            CancelAttemptEvidence::NotCanceled,
        );

        let confirmation = service.prepare_cancel(&gateway, intent_id).await.unwrap();

        assert_eq!(gateway.reconcile_calls.load(Ordering::SeqCst), 1);
        assert_eq!(gateway.cancel_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            gateway.identities.lock().unwrap().as_slice(),
            &[prepared_identity(&prepared())]
        );
        assert_eq!(
            ledger.projection().active.unwrap().state,
            ActiveIntentState::ReconciledLive
        );
        assert_eq!(
            confirmation,
            challenge(
                RecoveryAction::Cancel,
                &ledger.projection().active.unwrap(),
                ledger.projection().sequence,
                &ledger.projection().head_hash,
            )
        );
        assert!(positions.is_empty());
    }

    #[tokio::test]
    async fn prepare_cancel_refuses_pending_or_uncertain_without_a_challenge() {
        let cases = [
            (
                RemoteOrderEvidence::Pending,
                ActiveIntentState::ReconciledPending,
            ),
            (
                RemoteOrderEvidence::Uncertain {
                    code: ReconcileUncertainCode::Timeout,
                },
                ActiveIntentState::ReconciledUncertain,
            ),
        ];

        for (evidence, expected_state) in cases {
            let (dir, ledger, positions, intent_id) = active_service();
            let service = RecoveryService::local(
                Arc::clone(&ledger),
                positions,
                dir.path().join("execution-halt.json"),
            );
            let gateway = ScriptedCancelGateway::new(
                Arc::clone(&ledger),
                vec![evidence],
                CancelAttemptEvidence::NotCanceled,
            );

            assert_eq!(
                service
                    .prepare_cancel(&gateway, intent_id)
                    .await
                    .unwrap_err(),
                RecoveryServiceError::NotApplicable
            );
            assert_eq!(gateway.reconcile_calls.load(Ordering::SeqCst), 1);
            assert_eq!(gateway.cancel_calls.load(Ordering::SeqCst), 0);
            assert_eq!(ledger.projection().active.unwrap().state, expected_state);
        }
    }

    #[tokio::test]
    async fn prepare_cancel_rejects_historical_or_cleanup_pending_intents_without_network() {
        let (dir, ledger, positions, intent_id) = active_service();
        let service = RecoveryService::local(
            Arc::clone(&ledger),
            Arc::clone(&positions),
            dir.path().join("execution-halt.json"),
        );
        let gateway = ScriptedCancelGateway::new(
            Arc::clone(&ledger),
            vec![RemoteOrderEvidence::Live],
            CancelAttemptEvidence::NotCanceled,
        );

        assert_eq!(
            service
                .prepare_cancel(&gateway, IntentId(uuid::Uuid::from_u128(999)))
                .await
                .unwrap_err(),
            RecoveryServiceError::NotApplicable
        );
        assert_eq!(gateway.reconcile_calls.load(Ordering::SeqCst), 0);

        ledger
            .append(intent_id, LedgerPayload::ReconciliationStarted)
            .unwrap();
        ledger
            .append(
                intent_id,
                LedgerPayload::ReconciledNoFill {
                    status: TerminalNoFillStatus::Canceled,
                },
            )
            .unwrap();
        let marker = dir.path().join("cleanup-marker.json");
        fs::write(&marker, b"halt").unwrap();
        let cleanup = RecoveryService::local_with_cleanup_failure_for_test(
            Arc::clone(&ledger),
            Arc::clone(&positions),
            marker,
        );
        let acknowledgement = cleanup.prepare_acknowledge(intent_id).unwrap();
        assert_eq!(
            cleanup
                .acknowledge(intent_id, acknowledgement.as_str())
                .unwrap_err(),
            RecoveryServiceError::HaltCleanupIncomplete
        );
        assert_eq!(
            cleanup
                .prepare_cancel(&gateway, intent_id)
                .await
                .unwrap_err(),
            RecoveryServiceError::NotApplicable
        );
        assert_eq!(gateway.reconcile_calls.load(Ordering::SeqCst), 0);

        let retry = cleanup.prepare_acknowledge(intent_id).unwrap();
        cleanup.acknowledge(intent_id, retry.as_str()).unwrap();
        assert!(ledger.projection().active.is_none());
        assert!(ledger.projection().cleanup_pending.is_none());
        assert_eq!(
            cleanup
                .prepare_cancel(&gateway, intent_id)
                .await
                .unwrap_err(),
            RecoveryServiceError::NotApplicable
        );
        assert_eq!(gateway.reconcile_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cancel_observes_each_definite_response_then_reconciles_the_exact_order_once() {
        for (response, result) in [
            (CancelAttemptEvidence::Canceled, "canceled"),
            (CancelAttemptEvidence::NotCanceled, "not_canceled"),
        ] {
            let (dir, ledger, positions, intent_id) = active_service();
            let service = RecoveryService::local(
                Arc::clone(&ledger),
                Arc::clone(&positions),
                dir.path().join("execution-halt.json"),
            );
            let gateway = ScriptedCancelGateway::new(
                Arc::clone(&ledger),
                vec![
                    RemoteOrderEvidence::Live,
                    RemoteOrderEvidence::NoFill {
                        status: TerminalNoFillStatus::Canceled,
                    },
                ],
                response,
            );

            let confirmation = service.prepare_cancel(&gateway, intent_id).await.unwrap();
            let inspection = service
                .cancel(&gateway, intent_id, confirmation.as_str())
                .await
                .unwrap();

            assert_eq!(gateway.reconcile_calls.load(Ordering::SeqCst), 2);
            assert_eq!(gateway.cancel_calls.load(Ordering::SeqCst), 1);
            assert_eq!(gateway.cancel_saw_started.load(Ordering::SeqCst), 1);
            assert_eq!(
                gateway.identities.lock().unwrap().as_slice(),
                &[
                    prepared_identity(&prepared()),
                    prepared_identity(&prepared())
                ]
            );
            assert_eq!(
                gateway.canceled_orders.lock().unwrap().as_slice(),
                &[prepared().order_id]
            );
            assert_eq!(ledger.projection().event_count, 8);
            assert_eq!(
                ledger.projection().active.unwrap().state,
                ActiveIntentState::ReconciledNoFill
            );
            assert_eq!(inspection.action, Some(RecoveryAction::Acknowledge));
            assert!(inspection.challenge.is_some());
            assert!(inspection.order_id.is_none());
            assert!(positions.is_empty());
            let journal = fs::read_to_string(dir.path().join("execution-ledger.jsonl")).unwrap();
            assert_eq!(journal.matches("cancel_response_observed").count(), 1);
            assert!(journal.contains(&format!(r#""result":"{result}""#)));
            let kinds = journal
                .lines()
                .map(|line| {
                    serde_json::from_str::<serde_json::Value>(line).unwrap()["kind"]
                        .as_str()
                        .unwrap()
                        .to_owned()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                kinds,
                [
                    "intent_prepared",
                    "submit_started",
                    "reconciliation_started",
                    "reconciled_live",
                    "cancel_started",
                    "cancel_response_observed",
                    "reconciliation_started",
                    "reconciled_no_fill",
                ]
            );
            let rendered = format!("{inspection:?} {inspection}");
            assert!(!rendered.contains(prepared().order_id.as_str()));
            assert!(!rendered.contains(inspection.challenge.as_ref().unwrap().as_str()));
        }
    }

    #[tokio::test]
    async fn cancel_follow_up_classification_never_applies_or_acknowledges_automatically() {
        let cases = [
            (
                RemoteOrderEvidence::Matched {
                    making_micros: 2_000_000,
                    taking_micros: 4_000_000,
                    trade_ids: vec![TradeId::from_exact("trade-1").unwrap()],
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
                RemoteOrderEvidence::NoFill {
                    status: TerminalNoFillStatus::Invalid,
                },
                ActiveIntentState::ReconciledNoFill,
                Some(RecoveryAction::Acknowledge),
            ),
            (
                RemoteOrderEvidence::NoFill {
                    status: TerminalNoFillStatus::Rejected,
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

        for (follow_up, expected_state, expected_action) in cases {
            let (dir, ledger, positions, intent_id) = active_service();
            let service = RecoveryService::local(
                Arc::clone(&ledger),
                Arc::clone(&positions),
                dir.path().join("execution-halt.json"),
            );
            let gateway = ScriptedCancelGateway::new(
                Arc::clone(&ledger),
                vec![RemoteOrderEvidence::Live, follow_up],
                CancelAttemptEvidence::Canceled,
            );

            let confirmation = service.prepare_cancel(&gateway, intent_id).await.unwrap();
            let inspection = service
                .cancel(&gateway, intent_id, confirmation.as_str())
                .await
                .unwrap();

            assert_eq!(gateway.reconcile_calls.load(Ordering::SeqCst), 2);
            assert_eq!(gateway.cancel_calls.load(Ordering::SeqCst), 1);
            assert_eq!(ledger.projection().active.unwrap().state, expected_state);
            assert_eq!(inspection.action, expected_action);
            assert!(positions.is_empty());
        }
    }

    #[tokio::test]
    async fn cancel_timeout_is_observed_then_reconciled_once_without_a_retry() {
        let (dir, ledger, positions, intent_id) = active_service();
        let service = RecoveryService::local(
            Arc::clone(&ledger),
            Arc::clone(&positions),
            dir.path().join("execution-halt.json"),
        );
        let gateway = ScriptedCancelGateway::new(
            Arc::clone(&ledger),
            vec![
                RemoteOrderEvidence::Live,
                RemoteOrderEvidence::Uncertain {
                    code: ReconcileUncertainCode::Transport,
                },
            ],
            CancelAttemptEvidence::Uncertain {
                code: CancelUncertainCode::Timeout,
            },
        );

        let confirmation = service.prepare_cancel(&gateway, intent_id).await.unwrap();
        let inspection = service
            .cancel(&gateway, intent_id, confirmation.as_str())
            .await
            .unwrap();

        assert_eq!(gateway.reconcile_calls.load(Ordering::SeqCst), 2);
        assert_eq!(gateway.cancel_calls.load(Ordering::SeqCst), 1);
        assert_eq!(ledger.projection().event_count, 8);
        assert_eq!(
            ledger.projection().active.unwrap().state,
            ActiveIntentState::ReconciledUncertain
        );
        assert_eq!(inspection.action, None);
        assert!(positions.is_empty());
        let journal = fs::read_to_string(dir.path().join("execution-ledger.jsonl")).unwrap();
        assert!(journal.contains(r#""kind":"cancel_response_observed""#));
        assert!(journal.contains(r#""result":{"uncertain":{"code":"timeout"}}"#));
        drop(service);
        drop(gateway);
        drop(positions);
        drop(ledger);
        let replayed =
            ExecutionLedger::open_live(dir.path().join("execution-ledger.jsonl")).unwrap();
        let active = replayed.projection().active.unwrap();
        assert_eq!(active.state, ActiveIntentState::ReconciledUncertain);
        assert_eq!(
            active.evidence,
            ActiveEvidence::ReconciledUncertain(ReconcileUncertainCode::Transport)
        );
    }

    #[tokio::test]
    async fn cancel_validates_every_challenge_binding_before_ledger_or_network() {
        let (dir, ledger, positions, intent_id) = active_service();
        let service = RecoveryService::local(
            Arc::clone(&ledger),
            positions,
            dir.path().join("execution-halt.json"),
        );
        let gateway = ScriptedCancelGateway::new(
            Arc::clone(&ledger),
            vec![RemoteOrderEvidence::Live, RemoteOrderEvidence::Live],
            CancelAttemptEvidence::Canceled,
        );
        let confirmation = service.prepare_cancel(&gateway, intent_id).await.unwrap();
        let projection = ledger.projection();
        let active = projection.active.unwrap();
        let mut wrong_intent = active.clone();
        wrong_intent.intent_id = IntentId(uuid::Uuid::from_u128(999));
        let mut wrong_order = active.clone();
        wrong_order.prepared.order_id = order_id(0x99);
        let mismatches = [
            challenge(
                RecoveryAction::Apply,
                &active,
                projection.sequence,
                &projection.head_hash,
            ),
            challenge(
                RecoveryAction::Cancel,
                &wrong_intent,
                projection.sequence,
                &projection.head_hash,
            ),
            challenge(
                RecoveryAction::Cancel,
                &wrong_order,
                projection.sequence,
                &projection.head_hash,
            ),
            challenge(
                RecoveryAction::Cancel,
                &active,
                projection.sequence + 1,
                &projection.head_hash,
            ),
            challenge(
                RecoveryAction::Cancel,
                &active,
                projection.sequence,
                &EventHash::from_bytes([0x99; 32]),
            ),
            ConfirmationChallenge("00".repeat(32)),
        ];

        for stale in mismatches {
            assert_eq!(
                service
                    .cancel(&gateway, intent_id, stale.as_str())
                    .await
                    .unwrap_err(),
                RecoveryServiceError::StaleChallenge
            );
            assert_eq!(ledger.projection().sequence, projection.sequence);
            assert_eq!(gateway.cancel_calls.load(Ordering::SeqCst), 0);
            assert_eq!(gateway.reconcile_calls.load(Ordering::SeqCst), 1);
        }
        assert_ne!(confirmation, ConfirmationChallenge("00".repeat(32)));
    }

    #[tokio::test]
    async fn synchronous_apply_fails_closed_while_an_async_recovery_operation_is_in_flight() {
        let (_dir, ledger, positions, intent_id, service) = reconciled_matched_service();
        let confirmation = service.prepare_apply(intent_id).unwrap();
        let sequence = ledger.projection().sequence;
        let _operation = service.operation.lock().await;

        assert_eq!(
            service.apply(intent_id, confirmation.as_str()).unwrap_err(),
            RecoveryServiceError::OperationBusy
        );
        assert_eq!(ledger.projection().sequence, sequence);
        assert!(positions.is_empty());
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
    fn apply_after_normal_entry_position_crash_reuses_the_retained_event_without_regenerating_it() {
        let (dir, ledger, positions, intent_id) = active_service();
        ledger
            .append(
                intent_id,
                LedgerPayload::RemoteMatched(crate::service::execution_ledger::MatchedAmounts {
                    shares_micros: 4_000_000,
                    usd_micros: 2_000_000,
                }),
            )
            .unwrap();
        let opened_at = chrono::Utc::now();
        positions
            .apply_open(OpenPosition {
                position_id: crate::service::execution_ledger::PositionId(intent_id.0),
                opening_intent_id: intent_id,
                opening_order_id: prepared().order_id,
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
                opened_at,
            })
            .unwrap();
        let retained = ledger
            .projection()
            .active
            .unwrap()
            .position_event_id
            .unwrap();
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
            positions.clone(),
            dir.path().join("execution-halt.json"),
        );

        let confirmation = service.prepare_apply(intent_id).unwrap();
        service.apply(intent_id, confirmation.as_str()).unwrap();

        assert_eq!(ledger.projection().event_count, 7);
        assert_eq!(
            ledger.projection().active.unwrap().position_event_id,
            Some(retained)
        );
        assert_eq!(
            positions
                .get_by_id(&crate::service::execution_ledger::PositionId(intent_id.0))
                .unwrap()
                .opened_at,
            opened_at
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
            .append(
                exit_intent,
                LedgerPayload::RemoteMatched(crate::service::execution_ledger::MatchedAmounts {
                    shares_micros: open.shares_micros,
                    usd_micros: open.usd_notional_micros,
                }),
            )
            .unwrap();
        let closed_at = chrono::Utc::now();
        positions
            .apply_close(crate::service::execution_ledger::PositionClose {
                position_id: open.position_id,
                closing_intent_id: exit_intent,
                closing_order_id: order_id(0x21),
                shares_micros: open.shares_micros,
                usd_micros: open.usd_notional_micros,
                closed_at,
            })
            .unwrap();
        let retained = ledger
            .projection()
            .active
            .unwrap()
            .position_event_id
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
        assert_eq!(
            ledger.projection().active.unwrap().position_event_id,
            Some(retained)
        );
        assert_eq!(ledger.projection().event_count, 12);
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
    fn acknowledge_publishes_the_clear_before_marker_cleanup_and_retries_only_while_pending() {
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

        assert_eq!(
            service.prepare_acknowledge(intent_id).unwrap_err(),
            RecoveryServiceError::NotApplicable
        );
        assert!(!marker.exists());
    }

    #[test]
    fn post_remove_parent_sync_failure_replays_as_cleanup_pending_and_retries_locally() {
        let dir = tempfile::tempdir().unwrap();
        let ledger_path = dir.path().join("execution-ledger.jsonl");
        let marker = dir.path().join("execution-halt.json");
        let intent_id = IntentId(uuid::Uuid::from_u128(70));
        let ledger = Arc::new(ExecutionLedger::open_live(&ledger_path).unwrap());
        ledger
            .append(intent_id, LedgerPayload::IntentPrepared(prepared()))
            .unwrap();
        let positions = PositionStore::from_ledger(Arc::clone(&ledger)).unwrap();
        fs::write(&marker, b"legacy halt").unwrap();
        let service = RecoveryService::local_with_post_remove_sync_failure_for_test(
            Arc::clone(&ledger),
            Arc::clone(&positions),
            marker.clone(),
        );
        let confirmation = service.prepare_acknowledge(intent_id).unwrap();

        assert_eq!(
            service
                .acknowledge(intent_id, confirmation.as_str())
                .unwrap_err(),
            RecoveryServiceError::HaltCleanupIncomplete
        );
        assert!(ledger.projection().active.is_none());
        assert!(ledger.projection().cleanup_pending.is_some());
        assert!(!marker.exists());
        assert!(matches!(
            crate::service::execution_circuit_breaker::ExecutionCircuitBreaker::new_live(
                Arc::clone(&ledger),
                marker.clone(),
            ),
            Err(crate::service::order_gateway::OrderSubmitError::Halted { .. })
        ));

        drop(service);
        drop(positions);
        drop(ledger);
        let restarted = Arc::new(ExecutionLedger::open_live(&ledger_path).unwrap());
        assert!(restarted.projection().cleanup_pending.is_some());
        let restarted_positions = PositionStore::from_ledger(Arc::clone(&restarted)).unwrap();
        let retry =
            RecoveryService::local(Arc::clone(&restarted), restarted_positions, marker.clone());
        let confirmation = retry.prepare_acknowledge(intent_id).unwrap();
        retry.acknowledge(intent_id, confirmation.as_str()).unwrap();
        assert!(restarted.projection().cleanup_pending.is_none());
        assert!(!marker.exists());
    }

    #[test]
    fn final_cleanup_completion_append_failure_leaves_the_durable_owner_halted_for_retry() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("execution-halt.json");
        let ledger = Arc::new(
            ExecutionLedger::open_live(dir.path().join("execution-ledger.jsonl")).unwrap(),
        );
        let intent_id = IntentId(uuid::Uuid::from_u128(71));
        ledger
            .append(intent_id, LedgerPayload::IntentPrepared(prepared()))
            .unwrap();
        let positions = PositionStore::from_ledger(Arc::clone(&ledger)).unwrap();
        fs::write(&marker, b"legacy halt").unwrap();
        let service = RecoveryService::local(Arc::clone(&ledger), positions, marker.clone());
        let confirmation = service.prepare_acknowledge(intent_id).unwrap();
        service.fail_next_cleanup_completion_append();

        assert_eq!(
            service
                .acknowledge(intent_id, confirmation.as_str())
                .unwrap_err(),
            RecoveryServiceError::Ledger
        );
        assert!(ledger.projection().active.is_none());
        assert!(ledger.projection().cleanup_pending.is_some());
        assert!(!marker.exists());
        assert!(matches!(
            crate::service::execution_circuit_breaker::ExecutionCircuitBreaker::new_live(
                Arc::clone(&ledger),
                marker.clone(),
            ),
            Err(crate::service::order_gateway::OrderSubmitError::Halted { .. })
        ));

        let retry = service.prepare_acknowledge(intent_id).unwrap();
        service.acknowledge(intent_id, retry.as_str()).unwrap();
        assert!(ledger.projection().cleanup_pending.is_none());
    }
}
