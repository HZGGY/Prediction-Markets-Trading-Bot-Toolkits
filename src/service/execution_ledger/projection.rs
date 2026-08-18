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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActiveIntent {
    pub intent_id: IntentId,
    pub prepared: PreparedIntent,
    pub state: ActiveIntentState,
    pub position_event_id: Option<EventId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplyOutcome {
    Applied,
    AlreadyApplied,
}

#[derive(Clone, Debug, Default)]
pub struct LedgerProjection {
    pub sequence: u64,
    pub head_hash: EventHash,
    pub active: Option<ActiveIntent>,
    pub positions: HashMap<PositionId, DurablePosition>,
    pub event_ids: HashMap<EventId, EventHash>,
}

impl LedgerProjection {
    pub fn apply(&mut self, event: &LedgerEvent) -> Result<ApplyOutcome, LedgerError> {
        self.validate_and_apply(event)
    }

    pub fn validate_and_apply(&mut self, event: &LedgerEvent) -> Result<ApplyOutcome, LedgerError> {
        if let Some(existing_hash) = self.event_ids.get(&event.event_id) {
            return if existing_hash == &event.event_hash {
                Ok(ApplyOutcome::AlreadyApplied)
            } else {
                Err(LedgerError::new(LedgerErrorCode::IdempotencyConflict))
            };
        }

        self.validate_envelope(event)?;
        let mut candidate = self.clone();
        candidate.apply_payload(event)?;
        candidate.sequence = event.sequence;
        candidate.head_hash = event.event_hash.clone();
        candidate
            .event_ids
            .insert(event.event_id, event.event_hash.clone());
        *self = candidate;
        Ok(ApplyOutcome::Applied)
    }

    fn validate_envelope(&self, event: &LedgerEvent) -> Result<(), LedgerError> {
        if event.schema_version != LEDGER_SCHEMA_VERSION {
            return Err(LedgerError::new(LedgerErrorCode::UnsupportedSchema));
        }
        if event.sequence != self.sequence + 1 {
            return Err(LedgerError::new(LedgerErrorCode::SequenceMismatch));
        }
        if event.previous_hash != self.head_hash {
            return Err(LedgerError::new(LedgerErrorCode::PreviousHashMismatch));
        }
        Ok(())
    }

    fn apply_payload(&mut self, event: &LedgerEvent) -> Result<(), LedgerError> {
        match &event.payload {
            LedgerPayload::IntentPrepared(prepared) => self.prepare(event.intent_id, prepared),
            LedgerPayload::SubmitStarted => self.transition(
                event.intent_id,
                ActiveIntentState::NotSent,
                ActiveIntentState::SubmitStarted,
            ),
            LedgerPayload::RemoteMatched(amounts) => {
                self.validate_match(event.intent_id, *amounts)?;
                self.transition(
                    event.intent_id,
                    ActiveIntentState::SubmitStarted,
                    ActiveIntentState::RemoteMatched,
                )
            }
            LedgerPayload::RemoteRejected { .. } => self.transition(
                event.intent_id,
                ActiveIntentState::SubmitStarted,
                ActiveIntentState::RemoteRejected,
            ),
            LedgerPayload::RemoteUncertain { .. } => self.transition(
                event.intent_id,
                ActiveIntentState::SubmitStarted,
                ActiveIntentState::RemoteUncertain,
            ),
            LedgerPayload::SubmissionCommitted => {
                self.clear_normal(event.intent_id, ActiveIntentState::PositionRecorded)
            }
            LedgerPayload::SubmissionCommittedNoFill => {
                self.clear_normal(event.intent_id, ActiveIntentState::RemoteRejected)
            }
            LedgerPayload::PositionOpened(position) => self.open_position(event, position),
            LedgerPayload::PositionClosed(close) => self.close_position(event, close),
            LedgerPayload::ReconciliationStarted => self.start_reconciliation(event.intent_id),
            LedgerPayload::ReconciledMatched(amounts) => {
                self.classify_matched(event.intent_id, *amounts)
            }
            LedgerPayload::ReconciledNoFill { .. } => {
                self.classify_without_position(event.intent_id, ActiveIntentState::ReconciledNoFill)
            }
            LedgerPayload::ReconciledLive => {
                self.classify_without_position(event.intent_id, ActiveIntentState::ReconciledLive)
            }
            LedgerPayload::ReconciledPending => self
                .classify_without_position(event.intent_id, ActiveIntentState::ReconciledPending),
            LedgerPayload::ReconciledUncertain { .. } => self
                .classify_without_position(event.intent_id, ActiveIntentState::ReconciledUncertain),
            LedgerPayload::CancelStarted => self.transition(
                event.intent_id,
                ActiveIntentState::ReconciledLive,
                ActiveIntentState::CancelStarted,
            ),
            LedgerPayload::CancelResponseObserved { .. } => self.transition(
                event.intent_id,
                ActiveIntentState::CancelStarted,
                ActiveIntentState::CancelResponseObserved,
            ),
            LedgerPayload::RecoveryApplied { position_event_id } => {
                self.apply_recovery(event.intent_id, *position_event_id)
            }
            LedgerPayload::Acknowledged { reason } => self.acknowledge(event.intent_id, *reason),
        }
    }

    fn prepare(
        &mut self,
        intent_id: IntentId,
        prepared: &PreparedIntent,
    ) -> Result<(), LedgerError> {
        if self.active.is_some()
            || prepared.protocol_version != ORDER_PROTOCOL_VERSION
            || prepared.order_type != OrderType::Fok
            || prepared.token_id.is_empty()
            || prepared.expected_maker_micros == 0
            || prepared.expected_taker_micros == 0
        {
            return Err(LedgerError::new(LedgerErrorCode::IllegalTransition));
        }

        if let IntentPurpose::Exit { position_id } = prepared.purpose {
            let position = self
                .positions
                .get(&position_id)
                .ok_or_else(|| LedgerError::new(LedgerErrorCode::IllegalTransition))?;
            if !position.is_open() || position.token_id != prepared.token_id {
                return Err(LedgerError::new(LedgerErrorCode::IllegalTransition));
            }
        }

        self.active = Some(ActiveIntent {
            intent_id,
            prepared: prepared.clone(),
            state: ActiveIntentState::NotSent,
            position_event_id: None,
        });
        Ok(())
    }

    fn active_mut(&mut self, intent_id: IntentId) -> Result<&mut ActiveIntent, LedgerError> {
        let active = self
            .active
            .as_mut()
            .ok_or_else(|| LedgerError::new(LedgerErrorCode::IllegalTransition))?;
        if active.intent_id != intent_id {
            return Err(LedgerError::new(LedgerErrorCode::IntentMismatch));
        }
        Ok(active)
    }

    fn transition(
        &mut self,
        intent_id: IntentId,
        from: ActiveIntentState,
        to: ActiveIntentState,
    ) -> Result<(), LedgerError> {
        let active = self.active_mut(intent_id)?;
        if active.state != from {
            return Err(LedgerError::new(LedgerErrorCode::IllegalTransition));
        }
        active.state = to;
        Ok(())
    }

    fn clear_normal(
        &mut self,
        intent_id: IntentId,
        expected: ActiveIntentState,
    ) -> Result<(), LedgerError> {
        let active = self.active_mut(intent_id)?;
        if active.state != expected {
            return Err(LedgerError::new(LedgerErrorCode::IllegalTransition));
        }
        self.active = None;
        Ok(())
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

    fn open_position(
        &mut self,
        event: &LedgerEvent,
        position: &DurablePosition,
    ) -> Result<(), LedgerError> {
        let active = self.active_mut(event.intent_id)?.clone();
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
        self.positions
            .insert(position.position_id, position.clone());
        let active = self.active_mut(event.intent_id)?;
        active.state = if active.state == ActiveIntentState::RemoteMatched {
            ActiveIntentState::PositionRecorded
        } else {
            ActiveIntentState::RecoveryPositionRecorded
        };
        active.position_event_id = Some(event.event_id);
        Ok(())
    }

    fn close_position(
        &mut self,
        event: &LedgerEvent,
        close: &PositionClose,
    ) -> Result<(), LedgerError> {
        let active = self.active_mut(event.intent_id)?.clone();
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
        let position = self
            .positions
            .get_mut(&position_id)
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
        let active = self.active_mut(event.intent_id)?;
        active.state = if active.state == ActiveIntentState::RemoteMatched {
            ActiveIntentState::PositionRecorded
        } else {
            ActiveIntentState::RecoveryPositionRecorded
        };
        active.position_event_id = Some(event.event_id);
        Ok(())
    }

    fn start_reconciliation(&mut self, intent_id: IntentId) -> Result<(), LedgerError> {
        let active = self.active_mut(intent_id)?;
        if !matches!(
            active.state,
            ActiveIntentState::SubmitStarted
                | ActiveIntentState::RemoteMatched
                | ActiveIntentState::RemoteRejected
                | ActiveIntentState::RemoteUncertain
                | ActiveIntentState::PositionRecorded
                | ActiveIntentState::ReconciledLive
                | ActiveIntentState::ReconciledPending
                | ActiveIntentState::ReconciledUncertain
                | ActiveIntentState::CancelStarted
                | ActiveIntentState::CancelResponseObserved
        ) {
            return Err(LedgerError::new(LedgerErrorCode::IllegalTransition));
        }
        active.state = ActiveIntentState::ReconciliationStarted;
        Ok(())
    }

    fn classify_matched(
        &mut self,
        intent_id: IntentId,
        amounts: MatchedAmounts,
    ) -> Result<(), LedgerError> {
        if self.active_mut(intent_id)?.state != ActiveIntentState::ReconciliationStarted {
            return Err(LedgerError::new(LedgerErrorCode::IllegalTransition));
        }
        self.validate_match(intent_id, amounts)?;
        self.active_mut(intent_id)?.state = ActiveIntentState::ReconciledMatched;
        Ok(())
    }

    fn classify_without_position(
        &mut self,
        intent_id: IntentId,
        classification: ActiveIntentState,
    ) -> Result<(), LedgerError> {
        let active = self.active_mut(intent_id)?;
        if active.state != ActiveIntentState::ReconciliationStarted
            || active.position_event_id.is_some()
        {
            return Err(LedgerError::new(LedgerErrorCode::IllegalTransition));
        }
        active.state = classification;
        Ok(())
    }

    fn apply_recovery(
        &mut self,
        intent_id: IntentId,
        position_event_id: EventId,
    ) -> Result<(), LedgerError> {
        let active = self.active_mut(intent_id)?;
        if !matches!(
            active.state,
            ActiveIntentState::RecoveryPositionRecorded | ActiveIntentState::ReconciledMatched
        ) || active.position_event_id != Some(position_event_id)
        {
            return Err(LedgerError::new(LedgerErrorCode::IllegalTransition));
        }
        active.state = ActiveIntentState::RecoveryApplied;
        Ok(())
    }

    fn acknowledge(
        &mut self,
        intent_id: IntentId,
        reason: AcknowledgeReason,
    ) -> Result<(), LedgerError> {
        let active = self.active_mut(intent_id)?;
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
        self.active = None;
        Ok(())
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
            token_id: "12345678901234567890".to_owned(),
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
            token_id: "12345678901234567890".to_owned(),
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
            token_id: "12345678901234567890".to_owned(),
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
}
