use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::model::*;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActiveIntentState {
    NotSent,
    SubmitStarted,
    RemoteMatched,
    RemoteRejected,
    RemoteUncertain,
    PositionRecorded,
    ReconciliationStarted,
    ReconciledMatched,
    ReconciledNoFill,
    ReconciledLive,
    ReconciledPending,
    ReconciledUncertain,
    CancelStarted,
    CancelResponseObserved,
    RecoveryPositionRecorded,
    RecoveryApplied,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActiveEvidence {
    None,
    RemoteMatched(MatchedAmounts),
    RemoteRejected(RemoteRejectCode),
    RemoteUncertain(UncertainCode),
    ReconciledMatched(MatchedAmounts),
    ReconciledNoFill(TerminalNoFillStatus),
    ReconciledLive,
    ReconciledPending,
    ReconciledUncertain(ReconcileUncertainCode),
    CancelResponseObserved(CancelResponseClass),
    RecoveryApplied(EventId),
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationOrigin {
    SubmitStarted,
    RemoteMatched,
    RemoteRejected,
    RemoteUncertain,
    PositionRecorded,
    ReconciledLive,
    ReconciledPending,
    ReconciledUncertain,
    CancelStarted,
    CancelResponseObserved,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DurableRemoteOutcome {
    Matched(MatchedAmounts),
    Rejected(RemoteRejectCode),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActiveIntent {
    pub intent_id: IntentId,
    pub prepared: PreparedIntent,
    pub state: ActiveIntentState,
    pub position_event_id: Option<EventId>,
    pub evidence: ActiveEvidence,
    pub reconciliation_origin: Option<ReconciliationOrigin>,
    pub durable_remote_outcome: Option<DurableRemoteOutcome>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplyOutcome {
    Applied,
    AlreadyApplied,
}

/// The projection deliberately is not cloneable: replay mutates only touched state.
///
/// ```compile_fail
/// use polymarket_toolkits::service::execution_ledger::LedgerProjection;
/// let projection = LedgerProjection::default();
/// let _copy = projection.clone();
/// ```
#[derive(Debug, Default)]
pub struct LedgerProjection {
    pub sequence: u64,
    pub head_hash: EventHash,
    pub active: Option<ActiveIntent>,
    pub positions: HashMap<PositionId, DurablePosition>,
    pub event_ids: HashMap<EventId, LedgerEvent>,
    pub intent_orders: HashMap<IntentId, OrderId>,
    pub order_intents: HashMap<OrderId, IntentId>,
}

/// Owned read state for orchestration that must not retain the ledger mutex.
///
/// Event and identity indexes remain internal to the live projection; exposing
/// only their count avoids cloning replay history for ordinary reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerProjectionSnapshot {
    pub sequence: u64,
    pub head_hash: EventHash,
    pub active: Option<ActiveIntent>,
    pub positions: HashMap<PositionId, DurablePosition>,
    pub event_count: usize,
}

pub(crate) struct StagedProjection {
    outcome: ApplyOutcome,
    changes: ProjectionChanges,
}

#[derive(Default)]
struct ProjectionChanges {
    active: ActiveChange,
    position: PositionChange,
    identity: Option<(IntentId, OrderId)>,
}

#[derive(Default)]
enum ActiveChange {
    #[default]
    Unchanged,
    Set(Box<ActiveIntent>),
    Clear,
}

#[derive(Default)]
enum PositionChange {
    #[default]
    Unchanged,
    Upsert(Box<DurablePosition>),
}

impl LedgerProjection {
    pub(crate) fn snapshot(&self) -> LedgerProjectionSnapshot {
        LedgerProjectionSnapshot {
            sequence: self.sequence,
            head_hash: self.head_hash.clone(),
            active: self.active.clone(),
            positions: self.positions.clone(),
            event_count: self.event_ids.len(),
        }
    }

    pub fn apply(&mut self, event: &LedgerEvent) -> Result<ApplyOutcome, LedgerError> {
        self.validate_and_apply(event)
    }

    pub fn validate_and_apply(&mut self, event: &LedgerEvent) -> Result<ApplyOutcome, LedgerError> {
        let staged = self.stage_next(event)?;
        let outcome = staged.outcome;
        self.publish_staged(event, staged);
        Ok(outcome)
    }

    pub(crate) fn stage_next(&self, event: &LedgerEvent) -> Result<StagedProjection, LedgerError> {
        if event.schema_version != LEDGER_SCHEMA_VERSION {
            return Err(LedgerError::new(LedgerErrorCode::UnsupportedSchema));
        }
        if let Some(existing_event) = self.event_ids.get(&event.event_id) {
            return if existing_event == event {
                Ok(StagedProjection {
                    outcome: ApplyOutcome::AlreadyApplied,
                    changes: ProjectionChanges::default(),
                })
            } else {
                Err(LedgerError::new(LedgerErrorCode::IdempotencyConflict))
            };
        }

        self.validate_envelope(event)?;
        let changes = self.stage_payload(event)?;
        Ok(StagedProjection {
            outcome: ApplyOutcome::Applied,
            changes,
        })
    }

    pub(crate) fn publish_staged(&mut self, event: &LedgerEvent, staged: StagedProjection) {
        if staged.outcome == ApplyOutcome::AlreadyApplied {
            return;
        }

        if let Some((intent_id, order_id)) = staged.changes.identity {
            self.intent_orders.insert(intent_id, order_id.clone());
            self.order_intents.insert(order_id, intent_id);
        }
        match staged.changes.position {
            PositionChange::Unchanged => {}
            PositionChange::Upsert(position) => {
                self.positions.insert(position.position_id, *position);
            }
        }
        match staged.changes.active {
            ActiveChange::Unchanged => {}
            ActiveChange::Set(active) => self.active = Some(*active),
            ActiveChange::Clear => self.active = None,
        }
        self.sequence = event.sequence;
        self.head_hash = event.event_hash.clone();
        self.event_ids.insert(event.event_id, event.clone());
    }

    fn validate_envelope(&self, event: &LedgerEvent) -> Result<(), LedgerError> {
        if event.schema_version != LEDGER_SCHEMA_VERSION {
            return Err(LedgerError::new(LedgerErrorCode::UnsupportedSchema));
        }
        let expected_sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| LedgerError::new(LedgerErrorCode::SequenceExhausted))?;
        if event.sequence != expected_sequence {
            return Err(LedgerError::new(LedgerErrorCode::SequenceMismatch));
        }
        if event.previous_hash != self.head_hash {
            return Err(LedgerError::new(LedgerErrorCode::PreviousHashMismatch));
        }
        Ok(())
    }

    fn stage_payload(&self, event: &LedgerEvent) -> Result<ProjectionChanges, LedgerError> {
        match &event.payload {
            LedgerPayload::IntentPrepared(prepared) => {
                self.stage_prepare(event.intent_id, prepared)
            }
            LedgerPayload::SubmitStarted => self.stage_transition(
                event.intent_id,
                ActiveIntentState::NotSent,
                ActiveIntentState::SubmitStarted,
            ),
            LedgerPayload::RemoteMatched(amounts) => {
                self.validate_match(event.intent_id, *amounts)?;
                let mut active = self.active_for(event.intent_id)?.clone();
                if active.state != ActiveIntentState::SubmitStarted {
                    return Err(LedgerError::new(LedgerErrorCode::IllegalTransition));
                }
                active.state = ActiveIntentState::RemoteMatched;
                active.evidence = ActiveEvidence::RemoteMatched(*amounts);
                active.durable_remote_outcome = Some(DurableRemoteOutcome::Matched(*amounts));
                Ok(Self::active_changes(active))
            }
            LedgerPayload::RemoteRejected { code } => {
                let mut active = self.active_for(event.intent_id)?.clone();
                if active.state != ActiveIntentState::SubmitStarted {
                    return Err(LedgerError::new(LedgerErrorCode::IllegalTransition));
                }
                active.state = ActiveIntentState::RemoteRejected;
                active.evidence = ActiveEvidence::RemoteRejected(*code);
                active.durable_remote_outcome = Some(DurableRemoteOutcome::Rejected(*code));
                Ok(Self::active_changes(active))
            }
            LedgerPayload::RemoteUncertain { code } => {
                let mut changes = self.stage_transition(
                    event.intent_id,
                    ActiveIntentState::SubmitStarted,
                    ActiveIntentState::RemoteUncertain,
                )?;
                let ActiveChange::Set(active) = &mut changes.active else {
                    unreachable!("transition always stages active state")
                };
                active.evidence = ActiveEvidence::RemoteUncertain(*code);
                Ok(changes)
            }
            LedgerPayload::SubmissionCommitted => {
                self.stage_clear_normal(event.intent_id, ActiveIntentState::PositionRecorded)
            }
            LedgerPayload::SubmissionCommittedNoFill => {
                self.stage_clear_normal(event.intent_id, ActiveIntentState::RemoteRejected)
            }
            LedgerPayload::PositionOpened(position) => self.stage_open_position(event, position),
            LedgerPayload::PositionClosed(close) => self.stage_close_position(event, close),
            LedgerPayload::ReconciliationStarted => {
                self.stage_start_reconciliation(event.intent_id)
            }
            LedgerPayload::ReconciledMatched(amounts) => {
                self.stage_classify_matched(event.intent_id, *amounts)
            }
            LedgerPayload::ReconciledNoFill { status } => self.stage_classify_without_position(
                event.intent_id,
                ActiveIntentState::ReconciledNoFill,
                ActiveEvidence::ReconciledNoFill(*status),
            ),
            LedgerPayload::ReconciledLive => self.stage_classify_without_position(
                event.intent_id,
                ActiveIntentState::ReconciledLive,
                ActiveEvidence::ReconciledLive,
            ),
            LedgerPayload::ReconciledPending => self.stage_classify_without_position(
                event.intent_id,
                ActiveIntentState::ReconciledPending,
                ActiveEvidence::ReconciledPending,
            ),
            LedgerPayload::ReconciledUncertain { code } => self.stage_classify_without_position(
                event.intent_id,
                ActiveIntentState::ReconciledUncertain,
                ActiveEvidence::ReconciledUncertain(*code),
            ),
            LedgerPayload::CancelStarted => self.stage_transition(
                event.intent_id,
                ActiveIntentState::ReconciledLive,
                ActiveIntentState::CancelStarted,
            ),
            LedgerPayload::CancelResponseObserved { result } => {
                let mut changes = self.stage_transition(
                    event.intent_id,
                    ActiveIntentState::CancelStarted,
                    ActiveIntentState::CancelResponseObserved,
                )?;
                let ActiveChange::Set(active) = &mut changes.active else {
                    unreachable!("transition always stages active state")
                };
                active.evidence = ActiveEvidence::CancelResponseObserved(*result);
                Ok(changes)
            }
            LedgerPayload::RecoveryApplied { position_event_id } => {
                self.stage_apply_recovery(event.intent_id, *position_event_id)
            }
            LedgerPayload::Acknowledged { reason } => {
                self.stage_acknowledge(event.intent_id, *reason)
            }
        }
    }

    fn stage_prepare(
        &self,
        intent_id: IntentId,
        prepared: &PreparedIntent,
    ) -> Result<ProjectionChanges, LedgerError> {
        if self.intent_orders.contains_key(&intent_id)
            || self.order_intents.contains_key(&prepared.order_id)
        {
            return Err(LedgerError::new(LedgerErrorCode::IdentityConflict));
        }
        if self.active.is_some()
            || prepared.protocol_version != ORDER_PROTOCOL_VERSION
            || prepared.order_type != OrderType::Fok
            || prepared.expected_maker_micros == 0
            || prepared.expected_taker_micros == 0
        {
            return Err(LedgerError::new(LedgerErrorCode::IllegalTransition));
        }

        match &prepared.purpose {
            IntentPurpose::Entry(_) => {
                if self
                    .positions
                    .values()
                    .any(|position| position.is_open() && position.token_id == prepared.token_id)
                {
                    return Err(LedgerError::new(LedgerErrorCode::PositionConflict));
                }
            }
            IntentPurpose::Exit { position_id } => {
                let position = self
                    .positions
                    .get(position_id)
                    .ok_or_else(|| LedgerError::new(LedgerErrorCode::IllegalTransition))?;
                let expected_shares = match prepared.side {
                    OrderSide::Buy => prepared.expected_taker_micros,
                    OrderSide::Sell => prepared.expected_maker_micros,
                };
                if !position.is_open()
                    || position.token_id != prepared.token_id
                    || position.venue != prepared.venue
                    || position.neg_risk != prepared.neg_risk
                    || position.side == prepared.side
                    || position.entry_shares_micros != expected_shares
                {
                    return Err(LedgerError::new(LedgerErrorCode::IllegalTransition));
                }
            }
        }

        Ok(ProjectionChanges {
            active: ActiveChange::Set(Box::new(ActiveIntent {
                intent_id,
                prepared: prepared.clone(),
                state: ActiveIntentState::NotSent,
                position_event_id: None,
                evidence: ActiveEvidence::None,
                reconciliation_origin: None,
                durable_remote_outcome: None,
            })),
            identity: Some((intent_id, prepared.order_id.clone())),
            ..ProjectionChanges::default()
        })
    }

    fn active_for(&self, intent_id: IntentId) -> Result<&ActiveIntent, LedgerError> {
        let active = self
            .active
            .as_ref()
            .ok_or_else(|| LedgerError::new(LedgerErrorCode::IllegalTransition))?;
        if active.intent_id != intent_id {
            return Err(LedgerError::new(LedgerErrorCode::IntentMismatch));
        }
        Ok(active)
    }

    fn stage_transition(
        &self,
        intent_id: IntentId,
        from: ActiveIntentState,
        to: ActiveIntentState,
    ) -> Result<ProjectionChanges, LedgerError> {
        let mut active = self.active_for(intent_id)?.clone();
        if active.state != from {
            return Err(LedgerError::new(LedgerErrorCode::IllegalTransition));
        }
        active.state = to;
        Ok(Self::active_changes(active))
    }

    fn stage_clear_normal(
        &self,
        intent_id: IntentId,
        expected: ActiveIntentState,
    ) -> Result<ProjectionChanges, LedgerError> {
        let active = self.active_for(intent_id)?;
        if active.state != expected {
            return Err(LedgerError::new(LedgerErrorCode::IllegalTransition));
        }
        Ok(ProjectionChanges {
            active: ActiveChange::Clear,
            ..ProjectionChanges::default()
        })
    }

    fn validate_match(
        &self,
        intent_id: IntentId,
        amounts: MatchedAmounts,
    ) -> Result<(), LedgerError> {
        let active = self
            .active
            .as_ref()
            .ok_or_else(|| LedgerError::new(LedgerErrorCode::IllegalTransition))?;
        if active.intent_id != intent_id {
            return Err(LedgerError::new(LedgerErrorCode::IntentMismatch));
        }
        let exact = match active.prepared.side {
            OrderSide::Buy => {
                amounts.usd_micros == active.prepared.expected_maker_micros
                    && amounts.shares_micros == active.prepared.expected_taker_micros
            }
            OrderSide::Sell => {
                amounts.shares_micros == active.prepared.expected_maker_micros
                    && amounts.usd_micros == active.prepared.expected_taker_micros
            }
        };
        if !amounts.is_positive() || !exact {
            return Err(LedgerError::new(LedgerErrorCode::IllegalTransition));
        }
        Ok(())
    }

    fn stage_open_position(
        &self,
        event: &LedgerEvent,
        position: &DurablePosition,
    ) -> Result<ProjectionChanges, LedgerError> {
        let mut active = self.active_for(event.intent_id)?.clone();
        if !matches!(
            active.state,
            ActiveIntentState::RemoteMatched | ActiveIntentState::ReconciledMatched
        ) {
            return Err(LedgerError::new(LedgerErrorCode::IllegalTransition));
        }
        let IntentPurpose::Entry(seed) = &active.prepared.purpose else {
            return Err(LedgerError::new(LedgerErrorCode::IllegalTransition));
        };
        let amounts = MatchedAmounts {
            shares_micros: position.entry_shares_micros,
            usd_micros: position.entry_usd_micros,
        };
        self.validate_match(event.intent_id, amounts)?;
        let exact = position.position_id == PositionId(event.intent_id.0)
            && position.opening_intent_id == event.intent_id
            && position.opening_order_id == active.prepared.order_id
            && position.venue == active.prepared.venue
            && position.token_id == active.prepared.token_id
            && position.slug == seed.slug
            && position.category == seed.category
            && position.tags == seed.tags
            && position.neg_risk == active.prepared.neg_risk
            && position.side == active.prepared.side
            && position.take_profit_bps == seed.take_profit_bps
            && position.stop_loss_bps == seed.stop_loss_bps
            && position.is_open();
        if !exact {
            return Err(LedgerError::new(LedgerErrorCode::PositionConflict));
        }
        if self.positions.contains_key(&position.position_id)
            || self
                .positions
                .values()
                .any(|existing| existing.is_open() && existing.token_id == position.token_id)
        {
            return Err(LedgerError::new(LedgerErrorCode::PositionConflict));
        }
        active.state = if active.state == ActiveIntentState::RemoteMatched {
            ActiveIntentState::PositionRecorded
        } else {
            ActiveIntentState::RecoveryPositionRecorded
        };
        active.position_event_id = Some(event.event_id);
        Ok(ProjectionChanges {
            active: ActiveChange::Set(Box::new(active)),
            position: PositionChange::Upsert(Box::new(position.clone())),
            ..ProjectionChanges::default()
        })
    }

    fn stage_close_position(
        &self,
        event: &LedgerEvent,
        close: &PositionClose,
    ) -> Result<ProjectionChanges, LedgerError> {
        let mut active = self.active_for(event.intent_id)?.clone();
        if !matches!(
            active.state,
            ActiveIntentState::RemoteMatched | ActiveIntentState::ReconciledMatched
        ) {
            return Err(LedgerError::new(LedgerErrorCode::IllegalTransition));
        }
        let IntentPurpose::Exit { position_id } = active.prepared.purpose else {
            return Err(LedgerError::new(LedgerErrorCode::IllegalTransition));
        };
        let amounts = MatchedAmounts {
            shares_micros: close.shares_micros,
            usd_micros: close.usd_micros,
        };
        self.validate_match(event.intent_id, amounts)?;
        if close.position_id != position_id
            || close.closing_intent_id != event.intent_id
            || close.closing_order_id != active.prepared.order_id
        {
            return Err(LedgerError::new(LedgerErrorCode::PositionConflict));
        }
        let mut position = self
            .positions
            .get(&position_id)
            .cloned()
            .ok_or_else(|| LedgerError::new(LedgerErrorCode::PositionConflict))?;
        if !position.is_open()
            || position.token_id != active.prepared.token_id
            || position.entry_shares_micros != close.shares_micros
        {
            return Err(LedgerError::new(LedgerErrorCode::PositionConflict));
        }
        position.closing_intent_id = Some(close.closing_intent_id);
        position.closing_order_id = Some(close.closing_order_id.clone());
        position.closing_shares_micros = Some(close.shares_micros);
        position.closing_usd_micros = Some(close.usd_micros);
        position.closed_at = Some(close.closed_at);
        active.state = if active.state == ActiveIntentState::RemoteMatched {
            ActiveIntentState::PositionRecorded
        } else {
            ActiveIntentState::RecoveryPositionRecorded
        };
        active.position_event_id = Some(event.event_id);
        Ok(ProjectionChanges {
            active: ActiveChange::Set(Box::new(active)),
            position: PositionChange::Upsert(Box::new(position)),
            ..ProjectionChanges::default()
        })
    }

    fn stage_start_reconciliation(
        &self,
        intent_id: IntentId,
    ) -> Result<ProjectionChanges, LedgerError> {
        let mut active = self.active_for(intent_id)?.clone();
        let origin = match active.state {
            ActiveIntentState::SubmitStarted => ReconciliationOrigin::SubmitStarted,
            ActiveIntentState::RemoteMatched => ReconciliationOrigin::RemoteMatched,
            ActiveIntentState::RemoteRejected => ReconciliationOrigin::RemoteRejected,
            ActiveIntentState::RemoteUncertain => ReconciliationOrigin::RemoteUncertain,
            ActiveIntentState::PositionRecorded => ReconciliationOrigin::PositionRecorded,
            ActiveIntentState::ReconciledLive => ReconciliationOrigin::ReconciledLive,
            ActiveIntentState::ReconciledPending => ReconciliationOrigin::ReconciledPending,
            ActiveIntentState::ReconciledUncertain => ReconciliationOrigin::ReconciledUncertain,
            ActiveIntentState::CancelStarted => ReconciliationOrigin::CancelStarted,
            ActiveIntentState::CancelResponseObserved => {
                ReconciliationOrigin::CancelResponseObserved
            }
            _ => return Err(LedgerError::new(LedgerErrorCode::IllegalTransition)),
        };
        active.state = ActiveIntentState::ReconciliationStarted;
        active.reconciliation_origin = Some(origin);
        Ok(Self::active_changes(active))
    }

    fn stage_classify_matched(
        &self,
        intent_id: IntentId,
        amounts: MatchedAmounts,
    ) -> Result<ProjectionChanges, LedgerError> {
        let active = self.active_for(intent_id)?;
        if active.state != ActiveIntentState::ReconciliationStarted {
            return Err(LedgerError::new(LedgerErrorCode::IllegalTransition));
        }
        if matches!(
            active.durable_remote_outcome,
            Some(DurableRemoteOutcome::Rejected(_))
        ) {
            return Err(LedgerError::new(LedgerErrorCode::EvidenceConflict));
        }
        self.validate_match(intent_id, amounts)?;
        let mut active = active.clone();
        active.state = ActiveIntentState::ReconciledMatched;
        active.evidence = ActiveEvidence::ReconciledMatched(amounts);
        Ok(Self::active_changes(active))
    }

    fn stage_classify_without_position(
        &self,
        intent_id: IntentId,
        classification: ActiveIntentState,
        evidence: ActiveEvidence,
    ) -> Result<ProjectionChanges, LedgerError> {
        let mut active = self.active_for(intent_id)?.clone();
        if active.state != ActiveIntentState::ReconciliationStarted
            || (active.position_event_id.is_some()
                && classification != ActiveIntentState::ReconciledUncertain)
        {
            return Err(LedgerError::new(LedgerErrorCode::IllegalTransition));
        }
        let conflicts = match active.durable_remote_outcome {
            Some(DurableRemoteOutcome::Matched(_)) => {
                classification != ActiveIntentState::ReconciledUncertain
            }
            Some(DurableRemoteOutcome::Rejected(_)) => !matches!(
                classification,
                ActiveIntentState::ReconciledNoFill | ActiveIntentState::ReconciledUncertain
            ),
            None => false,
        };
        if conflicts {
            return Err(LedgerError::new(LedgerErrorCode::EvidenceConflict));
        }
        active.state = classification;
        active.evidence = evidence;
        Ok(Self::active_changes(active))
    }

    fn stage_apply_recovery(
        &self,
        intent_id: IntentId,
        position_event_id: EventId,
    ) -> Result<ProjectionChanges, LedgerError> {
        let mut active = self.active_for(intent_id)?.clone();
        if !matches!(
            active.state,
            ActiveIntentState::RecoveryPositionRecorded | ActiveIntentState::ReconciledMatched
        ) || active.position_event_id != Some(position_event_id)
        {
            return Err(LedgerError::new(LedgerErrorCode::IllegalTransition));
        }
        active.state = ActiveIntentState::RecoveryApplied;
        active.evidence = ActiveEvidence::RecoveryApplied(position_event_id);
        Ok(Self::active_changes(active))
    }

    fn stage_acknowledge(
        &self,
        intent_id: IntentId,
        reason: AcknowledgeReason,
    ) -> Result<ProjectionChanges, LedgerError> {
        let active = self.active_for(intent_id)?;
        let allowed = matches!(
            (active.state, reason),
            (ActiveIntentState::NotSent, AcknowledgeReason::NotSent)
                | (
                    ActiveIntentState::ReconciledNoFill,
                    AcknowledgeReason::ReconciledNoFill
                )
                | (
                    ActiveIntentState::RecoveryApplied,
                    AcknowledgeReason::RecoveryApplied
                )
        );
        if !allowed {
            return Err(LedgerError::new(LedgerErrorCode::IllegalTransition));
        }
        Ok(ProjectionChanges {
            active: ActiveChange::Clear,
            ..ProjectionChanges::default()
        })
    }

    fn active_changes(active: ActiveIntent) -> ProjectionChanges {
        ProjectionChanges {
            active: ActiveChange::Set(Box::new(active)),
            ..ProjectionChanges::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use uuid::Uuid;

    use super::*;

    struct Fixtures {
        projection: LedgerProjection,
        next_event_id: u128,
        next_hash_byte: u8,
    }

    impl Fixtures {
        fn new() -> Self {
            Self {
                projection: LedgerProjection::default(),
                next_event_id: 1_000,
                next_hash_byte: 1,
            }
        }

        fn event(&mut self, intent_id: IntentId, payload: LedgerPayload) -> LedgerEvent {
            let sequence = self.projection.sequence + 1;
            let event = LedgerEvent {
                schema_version: LEDGER_SCHEMA_VERSION,
                sequence,
                event_id: EventId(Uuid::from_u128(self.next_event_id)),
                intent_id,
                recorded_at: timestamp(sequence as i64),
                payload,
                previous_hash: self.projection.head_hash.clone(),
                event_hash: EventHash::from_bytes([self.next_hash_byte; 32]),
            };
            self.next_event_id += 1;
            self.next_hash_byte += 1;
            event
        }

        fn apply(&mut self, intent_id: IntentId, payload: LedgerPayload) -> ApplyOutcome {
            let event = self.event(intent_id, payload);
            self.projection.validate_and_apply(&event).unwrap()
        }

        fn apply_entry_until(&mut self, intent_id: IntentId, terminal: EntryTerminal) -> EventId {
            self.apply(intent_id, LedgerPayload::IntentPrepared(entry_intent(0x11)));
            self.apply(intent_id, LedgerPayload::SubmitStarted);
            self.apply(intent_id, LedgerPayload::RemoteMatched(buy_match()));
            let position_event = self.event(
                intent_id,
                LedgerPayload::PositionOpened(entry_position(intent_id, 0x11)),
            );
            let position_event_id = position_event.event_id;
            self.projection.validate_and_apply(&position_event).unwrap();
            if terminal == EntryTerminal::Committed {
                self.apply(intent_id, LedgerPayload::SubmissionCommitted);
            }
            position_event_id
        }
    }

    #[derive(Clone, Copy, Eq, PartialEq)]
    enum EntryTerminal {
        PositionRecorded,
        Committed,
    }

    fn intent_id(value: u128) -> IntentId {
        IntentId(Uuid::from_u128(value))
    }

    fn order_id(byte: u8) -> OrderId {
        OrderId::from_hex(format!("0x{}", format!("{byte:02x}").repeat(32))).unwrap()
    }

    fn timestamp(offset_seconds: i64) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 18, 0, 0, 0).single().unwrap()
            + Duration::seconds(offset_seconds)
    }

    fn seed() -> PositionSeed {
        PositionSeed {
            slug: "will-example-pass".to_owned(),
            category: "testing".to_owned(),
            tags: vec!["offline".to_owned()],
            take_profit_bps: 1_250,
            stop_loss_bps: 750,
        }
    }

    fn entry_intent(order_byte: u8) -> PreparedIntent {
        PreparedIntent {
            order_id: order_id(order_byte),
            protocol_version: 2,
            venue: Venue::PolymarketClob,
            token_id: TokenId::from_decimal("12345678901234567890").unwrap(),
            neg_risk: false,
            side: OrderSide::Buy,
            order_type: OrderType::Fok,
            expected_maker_micros: 5_000_000,
            expected_taker_micros: 10_000_000,
            source_hash: None,
            purpose: IntentPurpose::Entry(seed()),
        }
    }

    fn exit_intent(position_id: PositionId, order_byte: u8) -> PreparedIntent {
        PreparedIntent {
            order_id: order_id(order_byte),
            protocol_version: 2,
            venue: Venue::PolymarketClob,
            token_id: TokenId::from_decimal("12345678901234567890").unwrap(),
            neg_risk: false,
            side: OrderSide::Sell,
            order_type: OrderType::Fok,
            expected_maker_micros: 10_000_000,
            expected_taker_micros: 6_000_000,
            source_hash: None,
            purpose: IntentPurpose::Exit { position_id },
        }
    }

    fn buy_match() -> MatchedAmounts {
        MatchedAmounts {
            shares_micros: 10_000_000,
            usd_micros: 5_000_000,
        }
    }

    fn sell_match() -> MatchedAmounts {
        MatchedAmounts {
            shares_micros: 10_000_000,
            usd_micros: 6_000_000,
        }
    }

    fn entry_position(opening_intent_id: IntentId, order_byte: u8) -> DurablePosition {
        DurablePosition {
            position_id: PositionId(opening_intent_id.0),
            opening_intent_id,
            opening_order_id: order_id(order_byte),
            venue: Venue::PolymarketClob,
            token_id: TokenId::from_decimal("12345678901234567890").unwrap(),
            slug: "will-example-pass".to_owned(),
            category: "testing".to_owned(),
            tags: vec!["offline".to_owned()],
            neg_risk: false,
            side: OrderSide::Buy,
            entry_shares_micros: 10_000_000,
            entry_usd_micros: 5_000_000,
            take_profit_bps: 1_250,
            stop_loss_bps: 750,
            opened_at: timestamp(4),
            closing_intent_id: None,
            closing_order_id: None,
            closing_shares_micros: None,
            closing_usd_micros: None,
            closed_at: None,
        }
    }

    fn close(
        position_id: PositionId,
        closing_intent_id: IntentId,
        order_byte: u8,
    ) -> PositionClose {
        PositionClose {
            position_id,
            closing_intent_id,
            closing_order_id: order_id(order_byte),
            shares_micros: 10_000_000,
            usd_micros: 6_000_000,
            closed_at: timestamp(10),
        }
    }

    #[test]
    fn sequence_starts_at_one_and_advances_without_gaps() {
        let mut fixtures = Fixtures::new();
        let intent = intent_id(1);
        let mut event = fixtures.event(intent, LedgerPayload::IntentPrepared(entry_intent(0x11)));
        event.sequence = 2;

        assert_eq!(
            fixtures.projection.apply(&event).unwrap_err().code(),
            LedgerErrorCode::SequenceMismatch
        );

        event.sequence = 1;
        fixtures.projection.apply(&event).unwrap();
        let mut skipped = fixtures.event(intent, LedgerPayload::SubmitStarted);
        skipped.sequence = 3;
        assert_eq!(
            fixtures.projection.apply(&skipped).unwrap_err().code(),
            LedgerErrorCode::SequenceMismatch
        );
    }

    #[test]
    fn exhausted_sequence_fails_closed_with_a_typed_error() {
        let mut fixtures = Fixtures::new();
        fixtures.projection.sequence = u64::MAX;
        let event = LedgerEvent {
            schema_version: LEDGER_SCHEMA_VERSION,
            sequence: 0,
            event_id: EventId(Uuid::from_u128(999)),
            intent_id: intent_id(1),
            recorded_at: timestamp(0),
            payload: LedgerPayload::IntentPrepared(entry_intent(0x11)),
            previous_hash: fixtures.projection.head_hash.clone(),
            event_hash: EventHash::from_bytes([0xaa; 32]),
        };

        assert_eq!(
            fixtures.projection.apply(&event).unwrap_err().code(),
            LedgerErrorCode::SequenceExhausted
        );
    }

    #[test]
    fn matched_entry_requires_position_before_commit() {
        let mut fixtures = Fixtures::new();
        let intent = intent_id(1);
        fixtures.apply(intent, LedgerPayload::IntentPrepared(entry_intent(0x11)));
        fixtures.apply(intent, LedgerPayload::SubmitStarted);
        fixtures.apply(intent, LedgerPayload::RemoteMatched(buy_match()));

        let event = fixtures.event(intent, LedgerPayload::SubmissionCommitted);
        assert_eq!(
            fixtures.projection.apply(&event).unwrap_err().code(),
            LedgerErrorCode::IllegalTransition
        );
    }

    #[test]
    fn normal_entry_and_exit_commits_mutate_positions_and_clear_active_state() {
        let mut fixtures = Fixtures::new();
        let entry = intent_id(1);
        fixtures.apply_entry_until(entry, EntryTerminal::Committed);
        assert!(fixtures.projection.active.is_none());
        assert!(fixtures.projection.positions[&PositionId(entry.0)].is_open());

        let exit = intent_id(2);
        let position_id = PositionId(entry.0);
        fixtures.apply(
            exit,
            LedgerPayload::IntentPrepared(exit_intent(position_id, 0x22)),
        );
        fixtures.apply(exit, LedgerPayload::SubmitStarted);
        fixtures.apply(exit, LedgerPayload::RemoteMatched(sell_match()));
        fixtures.apply(
            exit,
            LedgerPayload::PositionClosed(close(position_id, exit, 0x22)),
        );
        fixtures.apply(exit, LedgerPayload::SubmissionCommitted);

        assert!(fixtures.projection.active.is_none());
        let position = &fixtures.projection.positions[&position_id];
        assert!(!position.is_open());
        assert_eq!(position.closing_intent_id, Some(exit));
        assert_eq!(position.closing_order_id, Some(order_id(0x22)));
    }

    #[test]
    fn rejected_no_fill_commit_clears_normal_active_state() {
        let mut fixtures = Fixtures::new();
        let intent = intent_id(1);
        fixtures.apply(intent, LedgerPayload::IntentPrepared(entry_intent(0x11)));
        fixtures.apply(intent, LedgerPayload::SubmitStarted);
        fixtures.apply(
            intent,
            LedgerPayload::RemoteRejected {
                code: RemoteRejectCode::ServerRejected,
            },
        );
        fixtures.apply(intent, LedgerPayload::SubmissionCommittedNoFill);

        assert!(fixtures.projection.active.is_none());
        assert!(fixtures.projection.positions.is_empty());
    }

    #[test]
    fn not_sent_requires_explicit_acknowledgement_to_clear_active_state() {
        let mut fixtures = Fixtures::new();
        let intent = intent_id(1);
        fixtures.apply(intent, LedgerPayload::IntentPrepared(entry_intent(0x11)));
        assert_eq!(
            fixtures.projection.active.as_ref().unwrap().state,
            ActiveIntentState::NotSent
        );

        fixtures.apply(
            intent,
            LedgerPayload::Acknowledged {
                reason: AcknowledgeReason::NotSent,
            },
        );
        assert!(fixtures.projection.active.is_none());
    }

    #[test]
    fn reconciliation_records_every_closed_class_without_clearing_active_state() {
        let cases = [
            LedgerPayload::ReconciledMatched(buy_match()),
            LedgerPayload::ReconciledNoFill {
                status: TerminalNoFillStatus::Rejected,
            },
            LedgerPayload::ReconciledLive,
            LedgerPayload::ReconciledPending,
            LedgerPayload::ReconciledUncertain {
                code: ReconcileUncertainCode::PartialFill,
            },
        ];

        for classification in cases {
            let mut fixtures = Fixtures::new();
            let intent = intent_id(1);
            fixtures.apply(intent, LedgerPayload::IntentPrepared(entry_intent(0x11)));
            fixtures.apply(intent, LedgerPayload::SubmitStarted);
            fixtures.apply(
                intent,
                LedgerPayload::RemoteUncertain {
                    code: UncertainCode::Transport,
                },
            );
            fixtures.apply(intent, LedgerPayload::ReconciliationStarted);
            fixtures.apply(intent, classification);
            assert!(fixtures.projection.active.is_some());
        }
    }

    #[test]
    fn active_projection_retains_typed_safety_evidence_and_reconciliation_origin() {
        let mut fixtures = Fixtures::new();
        let intent = intent_id(1);
        fixtures.apply(intent, LedgerPayload::IntentPrepared(entry_intent(0x11)));
        fixtures.apply(intent, LedgerPayload::SubmitStarted);
        fixtures.apply(
            intent,
            LedgerPayload::RemoteUncertain {
                code: UncertainCode::MalformedResponse,
            },
        );
        assert_eq!(
            fixtures.projection.active.as_ref().unwrap().evidence,
            ActiveEvidence::RemoteUncertain(UncertainCode::MalformedResponse)
        );

        fixtures.apply(intent, LedgerPayload::ReconciliationStarted);
        assert_eq!(
            fixtures
                .projection
                .active
                .as_ref()
                .unwrap()
                .reconciliation_origin,
            Some(ReconciliationOrigin::RemoteUncertain)
        );
        fixtures.apply(
            intent,
            LedgerPayload::ReconciledUncertain {
                code: ReconcileUncertainCode::PartialFill,
            },
        );
        assert_eq!(
            fixtures.projection.active.as_ref().unwrap().evidence,
            ActiveEvidence::ReconciledUncertain(ReconcileUncertainCode::PartialFill)
        );

        let mut rejected = Fixtures::new();
        rejected.apply(intent, LedgerPayload::IntentPrepared(entry_intent(0x11)));
        rejected.apply(intent, LedgerPayload::SubmitStarted);
        rejected.apply(
            intent,
            LedgerPayload::RemoteRejected {
                code: RemoteRejectCode::HttpRejected,
            },
        );
        assert_eq!(
            rejected.projection.active.as_ref().unwrap().evidence,
            ActiveEvidence::RemoteRejected(RemoteRejectCode::HttpRejected)
        );
        rejected.apply(intent, LedgerPayload::ReconciliationStarted);
        rejected.apply(
            intent,
            LedgerPayload::ReconciledNoFill {
                status: TerminalNoFillStatus::Rejected,
            },
        );
        assert_eq!(
            rejected.projection.active.as_ref().unwrap().evidence,
            ActiveEvidence::ReconciledNoFill(TerminalNoFillStatus::Rejected)
        );

        let mut canceled = Fixtures::new();
        canceled.apply(intent, LedgerPayload::IntentPrepared(entry_intent(0x11)));
        canceled.apply(intent, LedgerPayload::SubmitStarted);
        canceled.apply(intent, LedgerPayload::ReconciliationStarted);
        canceled.apply(intent, LedgerPayload::ReconciledLive);
        canceled.apply(intent, LedgerPayload::CancelStarted);
        canceled.apply(
            intent,
            LedgerPayload::CancelResponseObserved {
                result: CancelResponseClass::NotCanceled,
            },
        );
        assert_eq!(
            canceled.projection.active.as_ref().unwrap().evidence,
            ActiveEvidence::CancelResponseObserved(CancelResponseClass::NotCanceled)
        );
    }

    #[test]
    fn durable_remote_match_cannot_reconcile_to_no_fill() {
        let mut fixtures = Fixtures::new();
        let intent = intent_id(1);
        fixtures.apply(intent, LedgerPayload::IntentPrepared(entry_intent(0x11)));
        fixtures.apply(intent, LedgerPayload::SubmitStarted);
        fixtures.apply(intent, LedgerPayload::RemoteMatched(buy_match()));
        fixtures.apply(intent, LedgerPayload::ReconciliationStarted);

        let no_fill = fixtures.event(
            intent,
            LedgerPayload::ReconciledNoFill {
                status: TerminalNoFillStatus::Canceled,
            },
        );
        assert_eq!(
            fixtures
                .projection
                .validate_and_apply(&no_fill)
                .unwrap_err()
                .code(),
            LedgerErrorCode::EvidenceConflict
        );
        assert!(fixtures.projection.active.is_some());
        assert!(fixtures.projection.positions.is_empty());
    }

    #[test]
    fn durable_remote_rejection_cannot_reconcile_to_match() {
        let mut fixtures = Fixtures::new();
        let intent = intent_id(1);
        fixtures.apply(intent, LedgerPayload::IntentPrepared(entry_intent(0x11)));
        fixtures.apply(intent, LedgerPayload::SubmitStarted);
        fixtures.apply(
            intent,
            LedgerPayload::RemoteRejected {
                code: RemoteRejectCode::ServerRejected,
            },
        );
        fixtures.apply(intent, LedgerPayload::ReconciliationStarted);

        let matched = fixtures.event(intent, LedgerPayload::ReconciledMatched(buy_match()));
        assert_eq!(
            fixtures
                .projection
                .validate_and_apply(&matched)
                .unwrap_err()
                .code(),
            LedgerErrorCode::EvidenceConflict
        );
        assert!(fixtures.projection.active.is_some());
        assert!(fixtures.projection.positions.is_empty());
    }

    #[test]
    fn recovered_match_requires_position_then_recovery_applied_then_acknowledgement() {
        let mut fixtures = Fixtures::new();
        let intent = intent_id(1);
        fixtures.apply(intent, LedgerPayload::IntentPrepared(entry_intent(0x11)));
        fixtures.apply(intent, LedgerPayload::SubmitStarted);
        fixtures.apply(
            intent,
            LedgerPayload::RemoteUncertain {
                code: UncertainCode::Timeout,
            },
        );
        fixtures.apply(intent, LedgerPayload::ReconciliationStarted);
        fixtures.apply(intent, LedgerPayload::ReconciledMatched(buy_match()));

        let premature = fixtures.event(
            intent,
            LedgerPayload::RecoveryApplied {
                position_event_id: EventId(Uuid::from_u128(999)),
            },
        );
        assert_eq!(
            fixtures.projection.apply(&premature).unwrap_err().code(),
            LedgerErrorCode::IllegalTransition
        );

        let position_event = fixtures.event(
            intent,
            LedgerPayload::PositionOpened(entry_position(intent, 0x11)),
        );
        let position_event_id = position_event.event_id;
        fixtures.projection.apply(&position_event).unwrap();
        fixtures.apply(intent, LedgerPayload::RecoveryApplied { position_event_id });
        assert!(fixtures.projection.active.is_some());
        fixtures.apply(
            intent,
            LedgerPayload::Acknowledged {
                reason: AcknowledgeReason::RecoveryApplied,
            },
        );

        assert!(fixtures.projection.active.is_none());
        assert!(fixtures.projection.positions[&PositionId(intent.0)].is_open());
    }

    #[test]
    fn recovered_no_fill_requires_acknowledgement_to_clear_active_state() {
        let mut fixtures = Fixtures::new();
        let intent = intent_id(1);
        fixtures.apply(intent, LedgerPayload::IntentPrepared(entry_intent(0x11)));
        fixtures.apply(intent, LedgerPayload::SubmitStarted);
        fixtures.apply(intent, LedgerPayload::ReconciliationStarted);
        fixtures.apply(
            intent,
            LedgerPayload::ReconciledNoFill {
                status: TerminalNoFillStatus::Canceled,
            },
        );

        assert!(fixtures.projection.active.is_some());
        fixtures.apply(
            intent,
            LedgerPayload::Acknowledged {
                reason: AcknowledgeReason::ReconciledNoFill,
            },
        );
        assert!(fixtures.projection.active.is_none());
    }

    #[test]
    fn rejected_response_interrupted_before_commit_can_reconcile_to_no_fill() {
        let mut fixtures = Fixtures::new();
        let intent = intent_id(1);
        fixtures.apply(intent, LedgerPayload::IntentPrepared(entry_intent(0x11)));
        fixtures.apply(intent, LedgerPayload::SubmitStarted);
        fixtures.apply(
            intent,
            LedgerPayload::RemoteRejected {
                code: RemoteRejectCode::HttpRejected,
            },
        );

        fixtures.apply(intent, LedgerPayload::ReconciliationStarted);
        fixtures.apply(
            intent,
            LedgerPayload::ReconciledNoFill {
                status: TerminalNoFillStatus::Rejected,
            },
        );
        fixtures.apply(
            intent,
            LedgerPayload::Acknowledged {
                reason: AcknowledgeReason::ReconciledNoFill,
            },
        );

        assert!(fixtures.projection.active.is_none());
        assert!(fixtures.projection.positions.is_empty());
    }

    #[test]
    fn cancellation_records_response_then_requires_fresh_reconciliation() {
        let mut fixtures = Fixtures::new();
        let intent = intent_id(1);
        fixtures.apply(intent, LedgerPayload::IntentPrepared(entry_intent(0x11)));
        fixtures.apply(intent, LedgerPayload::SubmitStarted);
        fixtures.apply(intent, LedgerPayload::ReconciliationStarted);
        fixtures.apply(intent, LedgerPayload::ReconciledLive);
        fixtures.apply(intent, LedgerPayload::CancelStarted);
        fixtures.apply(
            intent,
            LedgerPayload::CancelResponseObserved {
                result: CancelResponseClass::Canceled,
            },
        );

        let acknowledge = fixtures.event(
            intent,
            LedgerPayload::Acknowledged {
                reason: AcknowledgeReason::ReconciledNoFill,
            },
        );
        assert_eq!(
            fixtures.projection.apply(&acknowledge).unwrap_err().code(),
            LedgerErrorCode::IllegalTransition
        );

        fixtures.apply(intent, LedgerPayload::ReconciliationStarted);
        fixtures.apply(
            intent,
            LedgerPayload::ReconciledNoFill {
                status: TerminalNoFillStatus::Canceled,
            },
        );
        fixtures.apply(
            intent,
            LedgerPayload::Acknowledged {
                reason: AcknowledgeReason::ReconciledNoFill,
            },
        );
        assert!(fixtures.projection.active.is_none());
    }

    #[test]
    fn unsafe_reconciliation_classes_cannot_be_acknowledged() {
        let cases = [
            LedgerPayload::ReconciledLive,
            LedgerPayload::ReconciledPending,
            LedgerPayload::ReconciledUncertain {
                code: ReconcileUncertainCode::Mismatch,
            },
            LedgerPayload::ReconciledMatched(buy_match()),
        ];

        for classification in cases {
            let mut fixtures = Fixtures::new();
            let intent = intent_id(1);
            fixtures.apply(intent, LedgerPayload::IntentPrepared(entry_intent(0x11)));
            fixtures.apply(intent, LedgerPayload::SubmitStarted);
            fixtures.apply(intent, LedgerPayload::ReconciliationStarted);
            fixtures.apply(intent, classification);
            let acknowledge = fixtures.event(
                intent,
                LedgerPayload::Acknowledged {
                    reason: AcknowledgeReason::RecoveryApplied,
                },
            );
            assert_eq!(
                fixtures.projection.apply(&acknowledge).unwrap_err().code(),
                LedgerErrorCode::IllegalTransition
            );
        }
    }

    #[test]
    fn unknown_schema_version_is_rejected_before_transition_logic() {
        let mut fixtures = Fixtures::new();
        let intent = intent_id(1);
        let mut event = fixtures.event(intent, LedgerPayload::IntentPrepared(entry_intent(0x11)));
        event.schema_version = LEDGER_SCHEMA_VERSION + 1;

        assert_eq!(
            fixtures.projection.apply(&event).unwrap_err().code(),
            LedgerErrorCode::UnsupportedSchema
        );
    }

    #[test]
    fn identical_event_retry_is_idempotent_but_conflicting_retry_is_fatal() {
        let mut fixtures = Fixtures::new();
        let intent = intent_id(1);
        let event = fixtures.event(intent, LedgerPayload::IntentPrepared(entry_intent(0x11)));
        assert_eq!(
            fixtures.projection.apply(&event).unwrap(),
            ApplyOutcome::Applied
        );
        assert_eq!(
            fixtures.projection.apply(&event).unwrap(),
            ApplyOutcome::AlreadyApplied
        );

        let mut conflicting = event.clone();
        conflicting.event_hash = EventHash::from_bytes([0xee; 32]);
        assert_eq!(
            fixtures.projection.apply(&conflicting).unwrap_err().code(),
            LedgerErrorCode::IdempotencyConflict
        );

        let mut changed_payload_with_retained_hash = event.clone();
        changed_payload_with_retained_hash.payload = LedgerPayload::SubmitStarted;
        assert_eq!(
            fixtures
                .projection
                .apply(&changed_payload_with_retained_hash)
                .unwrap_err()
                .code(),
            LedgerErrorCode::IdempotencyConflict
        );

        let mut changed_schema_with_retained_hash = event;
        changed_schema_with_retained_hash.schema_version += 1;
        assert_eq!(
            fixtures
                .projection
                .apply(&changed_schema_with_retained_hash)
                .unwrap_err()
                .code(),
            LedgerErrorCode::UnsupportedSchema
        );
    }

    #[test]
    fn only_one_intent_can_be_active_at_a_time() {
        let mut fixtures = Fixtures::new();
        fixtures.apply(
            intent_id(1),
            LedgerPayload::IntentPrepared(entry_intent(0x11)),
        );
        let second = fixtures.event(
            intent_id(2),
            LedgerPayload::IntentPrepared(entry_intent(0x22)),
        );

        assert_eq!(
            fixtures.projection.apply(&second).unwrap_err().code(),
            LedgerErrorCode::IllegalTransition
        );
        assert_eq!(
            fixtures.projection.active.as_ref().unwrap().intent_id,
            intent_id(1)
        );
    }

    #[test]
    fn terminal_intent_id_cannot_be_rebound_to_another_order() {
        let mut fixtures = Fixtures::new();
        let intent = intent_id(1);
        fixtures.apply(intent, LedgerPayload::IntentPrepared(entry_intent(0x11)));
        fixtures.apply(intent, LedgerPayload::SubmitStarted);
        fixtures.apply(
            intent,
            LedgerPayload::RemoteRejected {
                code: RemoteRejectCode::ServerRejected,
            },
        );
        fixtures.apply(intent, LedgerPayload::SubmissionCommittedNoFill);

        let rebound = fixtures.event(intent, LedgerPayload::IntentPrepared(entry_intent(0x22)));
        assert_eq!(
            fixtures.projection.apply(&rebound).unwrap_err().code(),
            LedgerErrorCode::IdentityConflict
        );
    }

    #[test]
    fn terminal_order_id_cannot_be_rebound_to_another_intent() {
        let mut fixtures = Fixtures::new();
        let first = intent_id(1);
        fixtures.apply(first, LedgerPayload::IntentPrepared(entry_intent(0x11)));
        fixtures.apply(first, LedgerPayload::SubmitStarted);
        fixtures.apply(
            first,
            LedgerPayload::RemoteRejected {
                code: RemoteRejectCode::ServerRejected,
            },
        );
        fixtures.apply(first, LedgerPayload::SubmissionCommittedNoFill);

        let rebound = fixtures.event(
            intent_id(2),
            LedgerPayload::IntentPrepared(entry_intent(0x11)),
        );
        assert_eq!(
            fixtures.projection.apply(&rebound).unwrap_err().code(),
            LedgerErrorCode::IdentityConflict
        );
    }

    #[test]
    fn second_entry_for_an_open_token_is_rejected_during_preparation() {
        let mut fixtures = Fixtures::new();
        fixtures.apply_entry_until(intent_id(1), EntryTerminal::Committed);

        let second = fixtures.event(
            intent_id(2),
            LedgerPayload::IntentPrepared(entry_intent(0x22)),
        );
        assert_eq!(
            fixtures
                .projection
                .validate_and_apply(&second)
                .unwrap_err()
                .code(),
            LedgerErrorCode::PositionConflict
        );
    }

    #[test]
    fn exit_preparation_requires_the_opposing_side() {
        let mut fixtures = Fixtures::new();
        let opening = intent_id(1);
        fixtures.apply_entry_until(opening, EntryTerminal::Committed);
        let mut exit = exit_intent(PositionId(opening.0), 0x22);
        exit.side = OrderSide::Buy;
        exit.expected_maker_micros = 6_000_000;
        exit.expected_taker_micros = 10_000_000;

        let event = fixtures.event(intent_id(2), LedgerPayload::IntentPrepared(exit));
        assert_eq!(
            fixtures
                .projection
                .validate_and_apply(&event)
                .unwrap_err()
                .code(),
            LedgerErrorCode::IllegalTransition
        );
    }

    #[test]
    fn exit_preparation_requires_matching_neg_risk() {
        let mut fixtures = Fixtures::new();
        let opening = intent_id(1);
        fixtures.apply_entry_until(opening, EntryTerminal::Committed);
        let mut exit = exit_intent(PositionId(opening.0), 0x22);
        exit.neg_risk = true;

        let event = fixtures.event(intent_id(2), LedgerPayload::IntentPrepared(exit));
        assert_eq!(
            fixtures
                .projection
                .validate_and_apply(&event)
                .unwrap_err()
                .code(),
            LedgerErrorCode::IllegalTransition
        );
    }

    #[test]
    fn exit_preparation_requires_exact_full_close_shares() {
        let mut fixtures = Fixtures::new();
        let opening = intent_id(1);
        fixtures.apply_entry_until(opening, EntryTerminal::Committed);
        let mut exit = exit_intent(PositionId(opening.0), 0x22);
        exit.expected_maker_micros -= 1;

        let event = fixtures.event(intent_id(2), LedgerPayload::IntentPrepared(exit));
        assert_eq!(
            fixtures
                .projection
                .validate_and_apply(&event)
                .unwrap_err()
                .code(),
            LedgerErrorCode::IllegalTransition
        );
    }

    #[test]
    fn position_recorded_before_a_crash_can_be_reconciled_without_duplicate_mutation() {
        let mut fixtures = Fixtures::new();
        let intent = intent_id(1);
        let position_event_id = fixtures.apply_entry_until(intent, EntryTerminal::PositionRecorded);
        fixtures.apply(intent, LedgerPayload::ReconciliationStarted);
        fixtures.apply(intent, LedgerPayload::ReconciledMatched(buy_match()));
        fixtures.apply(intent, LedgerPayload::RecoveryApplied { position_event_id });
        fixtures.apply(
            intent,
            LedgerPayload::Acknowledged {
                reason: AcknowledgeReason::RecoveryApplied,
            },
        );

        assert!(fixtures.projection.active.is_none());
        assert_eq!(fixtures.projection.positions.len(), 1);
    }

    #[test]
    fn durable_entry_position_allows_only_uncertain_non_mutating_classification() {
        let mut fixtures = Fixtures::new();
        let intent = intent_id(1);
        let position_id = PositionId(intent.0);
        let position_event_id = fixtures.apply_entry_until(intent, EntryTerminal::PositionRecorded);
        let durable_position = fixtures.projection.positions[&position_id].clone();
        fixtures.apply(intent, LedgerPayload::ReconciliationStarted);

        for illegal in [
            LedgerPayload::ReconciledNoFill {
                status: TerminalNoFillStatus::Canceled,
            },
            LedgerPayload::ReconciledLive,
            LedgerPayload::ReconciledPending,
        ] {
            let event = fixtures.event(intent, illegal);
            assert_eq!(
                fixtures.projection.apply(&event).unwrap_err().code(),
                LedgerErrorCode::IllegalTransition
            );
            assert_eq!(
                fixtures.projection.positions[&position_id],
                durable_position
            );
        }

        fixtures.apply(
            intent,
            LedgerPayload::ReconciledUncertain {
                code: ReconcileUncertainCode::Timeout,
            },
        );

        let active = fixtures.projection.active.as_ref().unwrap();
        assert_eq!(active.state, ActiveIntentState::ReconciledUncertain);
        assert_eq!(active.position_event_id, Some(position_event_id));
        assert_eq!(
            active.evidence,
            ActiveEvidence::ReconciledUncertain(ReconcileUncertainCode::Timeout)
        );
        assert_eq!(
            fixtures.projection.positions[&position_id],
            durable_position
        );
    }

    #[test]
    fn durable_exit_position_allows_only_uncertain_non_mutating_classification() {
        let mut fixtures = Fixtures::new();
        let opening_intent = intent_id(1);
        let exit_intent_id = intent_id(2);
        let position_id = PositionId(opening_intent.0);
        fixtures.apply_entry_until(opening_intent, EntryTerminal::Committed);
        fixtures.apply(
            exit_intent_id,
            LedgerPayload::IntentPrepared(exit_intent(position_id, 0x22)),
        );
        fixtures.apply(exit_intent_id, LedgerPayload::SubmitStarted);
        fixtures.apply(exit_intent_id, LedgerPayload::RemoteMatched(sell_match()));
        let position_event = fixtures.event(
            exit_intent_id,
            LedgerPayload::PositionClosed(close(position_id, exit_intent_id, 0x22)),
        );
        let position_event_id = position_event.event_id;
        fixtures.projection.apply(&position_event).unwrap();
        let durable_position = fixtures.projection.positions[&position_id].clone();
        fixtures.apply(exit_intent_id, LedgerPayload::ReconciliationStarted);

        for illegal in [
            LedgerPayload::ReconciledNoFill {
                status: TerminalNoFillStatus::Rejected,
            },
            LedgerPayload::ReconciledLive,
            LedgerPayload::ReconciledPending,
        ] {
            let event = fixtures.event(exit_intent_id, illegal);
            assert_eq!(
                fixtures.projection.apply(&event).unwrap_err().code(),
                LedgerErrorCode::IllegalTransition
            );
            assert_eq!(
                fixtures.projection.positions[&position_id],
                durable_position
            );
        }

        fixtures.apply(
            exit_intent_id,
            LedgerPayload::ReconciledUncertain {
                code: ReconcileUncertainCode::Transport,
            },
        );

        let active = fixtures.projection.active.as_ref().unwrap();
        assert_eq!(active.state, ActiveIntentState::ReconciledUncertain);
        assert_eq!(active.position_event_id, Some(position_event_id));
        assert_eq!(
            active.evidence,
            ActiveEvidence::ReconciledUncertain(ReconcileUncertainCode::Transport)
        );
        assert_eq!(
            fixtures.projection.positions[&position_id],
            durable_position
        );
    }
}
