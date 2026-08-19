//! Paper-isolated and ledger-backed open-position views.

use crate::service::execution_ledger::{
    DurablePosition, ExecutionLedger, IntentId, IntentPurpose, LedgerError, LedgerErrorCode,
    LedgerPayload, OrderId, OrderSide, PositionClose, PositionId, TokenId, Venue,
};
use chrono::{DateTime, Utc};
use parking_lot::{Mutex, RwLock};
use std::{collections::HashMap, error::Error, fmt, sync::Arc};

const MICROS_PER_UNIT: f64 = 1_000_000.0;
const BPS_PER_PERCENT: f64 = 100.0;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OpenPosition {
    pub position_id: PositionId,
    pub opening_intent_id: IntentId,
    pub opening_order_id: OrderId,
    pub venue: Venue,
    pub token_id: TokenId,
    pub slug: String,
    pub category: String,
    pub tags: Vec<String>,
    pub neg_risk: bool,
    pub side: OrderSide,
    pub shares_micros: u128,
    pub usd_notional_micros: u128,
    pub take_profit_bps: u32,
    pub stop_loss_bps: u32,
    pub opened_at: DateTime<Utc>,
}

impl OpenPosition {
    pub fn shares(&self) -> f64 {
        self.shares_micros as f64 / MICROS_PER_UNIT
    }

    pub fn usd_notional(&self) -> f64 {
        self.usd_notional_micros as f64 / MICROS_PER_UNIT
    }

    pub fn entry_price(&self) -> f64 {
        if self.shares_micros == 0 {
            0.0
        } else {
            self.usd_notional_micros as f64 / self.shares_micros as f64
        }
    }

    pub fn take_profit_pct(&self) -> f64 {
        self.take_profit_bps as f64 / BPS_PER_PERCENT
    }

    pub fn stop_loss_pct(&self) -> f64 {
        self.stop_loss_bps as f64 / BPS_PER_PERCENT
    }

    /// Current unrealised P&L percentage given a midprice quote.
    pub fn pnl_pct(&self, midprice: f64) -> f64 {
        let entry_price = self.entry_price();
        if entry_price <= 0.0 {
            return 0.0;
        }
        let raw = (midprice - entry_price) / entry_price * 100.0;
        match self.side {
            OrderSide::Buy => raw,
            OrderSide::Sell => -raw,
        }
    }

    fn from_durable(position: &DurablePosition) -> Result<Self, PositionStoreError> {
        if !position.is_open()
            || position.entry_shares_micros == 0
            || position.entry_usd_micros == 0
        {
            return Err(PositionStoreError::new(
                PositionStoreErrorCode::PositionConflict,
            ));
        }
        Ok(Self {
            position_id: position.position_id,
            opening_intent_id: position.opening_intent_id,
            opening_order_id: position.opening_order_id.clone(),
            venue: position.venue,
            token_id: position.token_id,
            slug: position.slug.clone(),
            category: position.category.clone(),
            tags: position.tags.clone(),
            neg_risk: position.neg_risk,
            side: position.side,
            shares_micros: position.entry_shares_micros,
            usd_notional_micros: position.entry_usd_micros,
            take_profit_bps: position.take_profit_bps,
            stop_loss_bps: position.stop_loss_bps,
            opened_at: position.opened_at,
        })
    }

    fn to_durable(&self) -> DurablePosition {
        DurablePosition {
            position_id: self.position_id,
            opening_intent_id: self.opening_intent_id,
            opening_order_id: self.opening_order_id.clone(),
            venue: self.venue,
            token_id: self.token_id,
            slug: self.slug.clone(),
            category: self.category.clone(),
            tags: self.tags.clone(),
            neg_risk: self.neg_risk,
            side: self.side,
            entry_shares_micros: self.shares_micros,
            entry_usd_micros: self.usd_notional_micros,
            take_profit_bps: self.take_profit_bps,
            stop_loss_bps: self.stop_loss_bps,
            opened_at: self.opened_at,
            closing_intent_id: None,
            closing_order_id: None,
            closing_shares_micros: None,
            closing_usd_micros: None,
            closed_at: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PositionApply {
    Applied,
    AlreadyApplied,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PositionStoreErrorCode {
    IdempotencyConflict,
    PositionConflict,
    Ledger(LedgerErrorCode),
}

#[derive(Clone, Eq, PartialEq)]
pub struct PositionStoreError {
    code: PositionStoreErrorCode,
}

impl PositionStoreError {
    fn new(code: PositionStoreErrorCode) -> Self {
        Self { code }
    }

    pub fn code(&self) -> PositionStoreErrorCode {
        self.code
    }
}

impl From<LedgerError> for PositionStoreError {
    fn from(error: LedgerError) -> Self {
        let code = match error.code() {
            LedgerErrorCode::IdempotencyConflict => PositionStoreErrorCode::IdempotencyConflict,
            LedgerErrorCode::PositionConflict => PositionStoreErrorCode::PositionConflict,
            code => PositionStoreErrorCode::Ledger(code),
        };
        Self::new(code)
    }
}

impl fmt::Display for PositionStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "position_store_error(code={:?})", self.code)
    }
}

impl fmt::Debug for PositionStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl Error for PositionStoreError {}

enum PositionBackend {
    Paper(RwLock<HashMap<PositionId, DurablePosition>>),
    Live(Arc<ExecutionLedger>),
}

pub struct PositionStore {
    backend: PositionBackend,
    mutation: Mutex<()>,
}

impl PositionStore {
    pub fn new_paper() -> Arc<Self> {
        Arc::new(Self {
            backend: PositionBackend::Paper(RwLock::new(HashMap::new())),
            mutation: Mutex::new(()),
        })
    }

    pub fn from_ledger(ledger: Arc<ExecutionLedger>) -> Result<Arc<Self>, PositionStoreError> {
        for position in ledger.projection().positions.values() {
            if position.is_open() {
                OpenPosition::from_durable(position)?;
            }
        }
        Ok(Arc::new(Self {
            backend: PositionBackend::Live(ledger),
            mutation: Mutex::new(()),
        }))
    }

    #[cfg(test)]
    pub(crate) fn live_ledger(&self) -> Option<Arc<ExecutionLedger>> {
        match &self.backend {
            PositionBackend::Paper(_) => None,
            PositionBackend::Live(ledger) => Some(Arc::clone(ledger)),
        }
    }

    pub fn apply_open(&self, position: OpenPosition) -> Result<PositionApply, PositionStoreError> {
        let _guard = self.mutation.lock();
        match &self.backend {
            PositionBackend::Paper(positions) => {
                let mut positions = positions.write();
                match precheck_open(&positions, &position)? {
                    PositionApply::AlreadyApplied => Ok(PositionApply::AlreadyApplied),
                    PositionApply::Applied => {
                        positions.insert(position.position_id, position.to_durable());
                        Ok(PositionApply::Applied)
                    }
                }
            }
            PositionBackend::Live(ledger) => {
                match precheck_open(&ledger.projection().positions, &position)? {
                    PositionApply::AlreadyApplied => Ok(PositionApply::AlreadyApplied),
                    PositionApply::Applied => {
                        ledger.append(
                            position.opening_intent_id,
                            LedgerPayload::PositionOpened(position.to_durable()),
                        )?;
                        Ok(PositionApply::Applied)
                    }
                }
            }
        }
    }

    pub fn apply_close(&self, close: PositionClose) -> Result<PositionApply, PositionStoreError> {
        let _guard = self.mutation.lock();
        match &self.backend {
            PositionBackend::Paper(positions) => {
                let mut positions = positions.write();
                match precheck_close(&positions, &close)? {
                    PositionApply::AlreadyApplied => Ok(PositionApply::AlreadyApplied),
                    PositionApply::Applied => {
                        let position = positions
                            .get_mut(&close.position_id)
                            .expect("precheck requires the named position");
                        apply_close_fields(position, &close);
                        Ok(PositionApply::Applied)
                    }
                }
            }
            PositionBackend::Live(ledger) => {
                match precheck_close(&ledger.projection().positions, &close)? {
                    PositionApply::AlreadyApplied => Ok(PositionApply::AlreadyApplied),
                    PositionApply::Applied => {
                        ledger.append(
                            close.closing_intent_id,
                            LedgerPayload::PositionClosed(close),
                        )?;
                        Ok(PositionApply::Applied)
                    }
                }
            }
        }
    }

    pub fn snapshot(&self) -> Vec<OpenPosition> {
        let mut positions = self
            .durable_positions()
            .values()
            .filter(|position| position.is_open())
            .map(OpenPosition::from_durable)
            .collect::<Result<Vec<_>, _>>()
            .expect("validated position projections contain valid open positions");
        positions.sort_by_key(|position| (position.token_id, position.position_id.0.as_u128()));
        positions
    }

    pub fn get_by_token(&self, token_id: &TokenId) -> Option<OpenPosition> {
        self.snapshot()
            .into_iter()
            .find(|position| &position.token_id == token_id)
    }

    pub fn get_by_id(&self, position_id: &PositionId) -> Option<OpenPosition> {
        self.snapshot()
            .into_iter()
            .find(|position| &position.position_id == position_id)
    }

    pub fn len(&self) -> usize {
        self.snapshot().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn open_usd_by_category(&self, category: &str) -> f64 {
        self.snapshot()
            .iter()
            .filter(|position| ci_eq(&position.category, category))
            .map(OpenPosition::usd_notional)
            .sum()
    }

    pub fn open_usd_by_tag(&self, tag: &str) -> f64 {
        self.snapshot()
            .iter()
            .filter(|position| position.tags.iter().any(|value| ci_eq(value, tag)))
            .map(OpenPosition::usd_notional)
            .sum()
    }

    pub(crate) fn is_paper(&self) -> bool {
        matches!(self.backend, PositionBackend::Paper(_))
    }

    pub(crate) fn pending_entry_identity(
        &self,
        order_id: &OrderId,
    ) -> Option<(IntentId, PositionId)> {
        let PositionBackend::Live(ledger) = &self.backend else {
            return None;
        };
        let active = ledger.projection().active?;
        if active.prepared.order_id != *order_id
            || !matches!(active.prepared.purpose, IntentPurpose::Entry(_))
        {
            return None;
        }
        Some((active.intent_id, PositionId(active.intent_id.0)))
    }

    pub(crate) fn pending_exit_intent(
        &self,
        order_id: &OrderId,
        position_id: PositionId,
    ) -> Option<IntentId> {
        let PositionBackend::Live(ledger) = &self.backend else {
            return None;
        };
        let active = ledger.projection().active?;
        if active.prepared.order_id != *order_id
            || active.prepared.purpose != (IntentPurpose::Exit { position_id })
        {
            return None;
        }
        Some(active.intent_id)
    }

    fn durable_positions(&self) -> HashMap<PositionId, DurablePosition> {
        match &self.backend {
            PositionBackend::Paper(positions) => positions.read().clone(),
            PositionBackend::Live(ledger) => ledger.projection().positions,
        }
    }
}

fn precheck_open(
    positions: &HashMap<PositionId, DurablePosition>,
    requested: &OpenPosition,
) -> Result<PositionApply, PositionStoreError> {
    if requested.position_id != PositionId(requested.opening_intent_id.0)
        || requested.shares_micros == 0
        || requested.usd_notional_micros == 0
    {
        return Err(PositionStoreError::new(
            PositionStoreErrorCode::PositionConflict,
        ));
    }
    for existing in positions.values() {
        if existing.closing_intent_id == Some(requested.opening_intent_id)
            || existing.closing_order_id.as_ref() == Some(&requested.opening_order_id)
        {
            return Err(PositionStoreError::new(
                PositionStoreErrorCode::IdempotencyConflict,
            ));
        }
        if existing.position_id == requested.position_id
            || existing.opening_intent_id == requested.opening_intent_id
            || existing.opening_order_id == requested.opening_order_id
        {
            return if opening_matches(existing, requested) {
                Ok(PositionApply::AlreadyApplied)
            } else {
                Err(PositionStoreError::new(
                    PositionStoreErrorCode::IdempotencyConflict,
                ))
            };
        }
        if existing.is_open() && existing.token_id == requested.token_id {
            return Err(PositionStoreError::new(
                PositionStoreErrorCode::PositionConflict,
            ));
        }
    }
    Ok(PositionApply::Applied)
}

fn opening_matches(existing: &DurablePosition, requested: &OpenPosition) -> bool {
    existing.position_id == requested.position_id
        && existing.opening_intent_id == requested.opening_intent_id
        && existing.opening_order_id == requested.opening_order_id
        && existing.venue == requested.venue
        && existing.token_id == requested.token_id
        && existing.slug == requested.slug
        && existing.category == requested.category
        && existing.tags == requested.tags
        && existing.neg_risk == requested.neg_risk
        && existing.side == requested.side
        && existing.entry_shares_micros == requested.shares_micros
        && existing.entry_usd_micros == requested.usd_notional_micros
        && existing.take_profit_bps == requested.take_profit_bps
        && existing.stop_loss_bps == requested.stop_loss_bps
        && existing.opened_at == requested.opened_at
}

fn precheck_close(
    positions: &HashMap<PositionId, DurablePosition>,
    close: &PositionClose,
) -> Result<PositionApply, PositionStoreError> {
    if close.shares_micros == 0 || close.usd_micros == 0 {
        return Err(PositionStoreError::new(
            PositionStoreErrorCode::PositionConflict,
        ));
    }
    if positions.values().any(|position| {
        position.opening_intent_id == close.closing_intent_id
            || position.opening_order_id == close.closing_order_id
    }) {
        return Err(PositionStoreError::new(
            PositionStoreErrorCode::IdempotencyConflict,
        ));
    }
    if positions.values().any(|position| {
        position.position_id != close.position_id
            && (position.closing_intent_id == Some(close.closing_intent_id)
                || position.closing_order_id.as_ref() == Some(&close.closing_order_id))
    }) {
        return Err(PositionStoreError::new(
            PositionStoreErrorCode::IdempotencyConflict,
        ));
    }
    let position = positions
        .get(&close.position_id)
        .ok_or_else(|| PositionStoreError::new(PositionStoreErrorCode::PositionConflict))?;
    if !position.is_open() {
        return if close_matches(position, close) {
            Ok(PositionApply::AlreadyApplied)
        } else {
            Err(PositionStoreError::new(
                PositionStoreErrorCode::IdempotencyConflict,
            ))
        };
    }
    if position.entry_shares_micros != close.shares_micros {
        return Err(PositionStoreError::new(
            PositionStoreErrorCode::PositionConflict,
        ));
    }
    Ok(PositionApply::Applied)
}

fn close_matches(position: &DurablePosition, close: &PositionClose) -> bool {
    position.position_id == close.position_id
        && position.closing_intent_id == Some(close.closing_intent_id)
        && position.closing_order_id.as_ref() == Some(&close.closing_order_id)
        && position.closing_shares_micros == Some(close.shares_micros)
        && position.closing_usd_micros == Some(close.usd_micros)
        && position.closed_at == Some(close.closed_at)
}

fn apply_close_fields(position: &mut DurablePosition, close: &PositionClose) {
    position.closing_intent_id = Some(close.closing_intent_id);
    position.closing_order_id = Some(close.closing_order_id.clone());
    position.closing_shares_micros = Some(close.shares_micros);
    position.closing_usd_micros = Some(close.usd_micros);
    position.closed_at = Some(close.closed_at);
}

fn ci_eq(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.chars()
            .zip(b.chars())
            .all(|(x, y)| x.eq_ignore_ascii_case(&y))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::execution_ledger::{
        ExecutionLedger, IntentId, IntentPurpose, LedgerPayload, MatchedAmounts, OrderId,
        OrderSide, OrderType, PositionClose, PositionId, PositionSeed, PreparedIntent,
        RemoteRejectCode, TokenId, UncertainCode, Venue, ORDER_PROTOCOL_VERSION,
    };
    use chrono::{TimeZone, Utc};
    use uuid::Uuid;

    fn intent_id(value: u128) -> IntentId {
        IntentId(Uuid::from_u128(value))
    }

    fn position_id(value: u128) -> PositionId {
        PositionId(Uuid::from_u128(value))
    }

    fn order_id(byte: u8) -> OrderId {
        OrderId::from_hex(format!("0x{}", format!("{byte:02x}").repeat(32))).unwrap()
    }

    fn token_id(value: &str) -> TokenId {
        TokenId::from_decimal(value).unwrap()
    }

    fn opened_at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 18, 12, 34, 56)
            .single()
            .unwrap()
    }

    fn pos(
        value: u128,
        token: &str,
        slug: &str,
        cat: &str,
        tags: &[&str],
        shares_micros: u128,
        usd_micros: u128,
    ) -> OpenPosition {
        OpenPosition {
            position_id: position_id(value),
            opening_intent_id: intent_id(value),
            opening_order_id: order_id(value as u8),
            venue: Venue::PolymarketClob,
            token_id: token_id(token),
            slug: slug.into(),
            category: cat.into(),
            tags: tags.iter().map(|s| s.to_string()).collect(),
            neg_risk: false,
            side: OrderSide::Buy,
            shares_micros,
            usd_notional_micros: usd_micros,
            take_profit_bps: 5_000,
            stop_loss_bps: 3_000,
            opened_at: opened_at(),
        }
    }

    fn prepared_entry(position: &OpenPosition) -> PreparedIntent {
        PreparedIntent {
            order_id: position.opening_order_id.clone(),
            protocol_version: ORDER_PROTOCOL_VERSION,
            venue: position.venue,
            token_id: position.token_id,
            neg_risk: position.neg_risk,
            side: position.side,
            order_type: OrderType::Fok,
            expected_maker_micros: position.usd_notional_micros,
            expected_taker_micros: position.shares_micros,
            source_hash: None,
            purpose: IntentPurpose::Entry(PositionSeed {
                slug: position.slug.clone(),
                category: position.category.clone(),
                tags: position.tags.clone(),
                take_profit_bps: position.take_profit_bps,
                stop_loss_bps: position.stop_loss_bps,
            }),
        }
    }

    fn prepare_matched_entry(ledger: &ExecutionLedger, position: &OpenPosition) {
        ledger
            .append(
                position.opening_intent_id,
                LedgerPayload::IntentPrepared(prepared_entry(position)),
            )
            .unwrap();
        ledger
            .append(position.opening_intent_id, LedgerPayload::SubmitStarted)
            .unwrap();
        ledger
            .append(
                position.opening_intent_id,
                LedgerPayload::RemoteMatched(MatchedAmounts {
                    shares_micros: position.shares_micros,
                    usd_micros: position.usd_notional_micros,
                }),
            )
            .unwrap();
    }

    fn commit_entry(ledger: &ExecutionLedger, position: &OpenPosition) {
        ledger
            .append(
                position.opening_intent_id,
                LedgerPayload::SubmissionCommitted,
            )
            .unwrap();
    }

    fn prepare_matched_exit(
        ledger: &ExecutionLedger,
        position: &OpenPosition,
        close: &PositionClose,
    ) {
        ledger
            .append(
                close.closing_intent_id,
                LedgerPayload::IntentPrepared(PreparedIntent {
                    order_id: close.closing_order_id.clone(),
                    protocol_version: ORDER_PROTOCOL_VERSION,
                    venue: position.venue,
                    token_id: position.token_id,
                    neg_risk: position.neg_risk,
                    side: OrderSide::Sell,
                    order_type: OrderType::Fok,
                    expected_maker_micros: close.shares_micros,
                    expected_taker_micros: close.usd_micros,
                    source_hash: None,
                    purpose: IntentPurpose::Exit {
                        position_id: position.position_id,
                    },
                }),
            )
            .unwrap();
        ledger
            .append(close.closing_intent_id, LedgerPayload::SubmitStarted)
            .unwrap();
        ledger
            .append(
                close.closing_intent_id,
                LedgerPayload::RemoteMatched(MatchedAmounts {
                    shares_micros: close.shares_micros,
                    usd_micros: close.usd_micros,
                }),
            )
            .unwrap();
    }

    fn close(value: u128, position: &OpenPosition, usd_micros: u128) -> PositionClose {
        PositionClose {
            position_id: position.position_id,
            closing_intent_id: intent_id(value),
            closing_order_id: order_id(value as u8),
            shares_micros: position.shares_micros,
            usd_micros,
            closed_at: opened_at() + chrono::Duration::hours(1),
        }
    }

    #[test]
    fn live_store_rebuilds_open_positions_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let expected = pos(1, "12345", "a", "Politics", &["us"], 12_345_678, 6_172_839);
        {
            let ledger = Arc::new(ExecutionLedger::open_live(&path).unwrap());
            let store = PositionStore::from_ledger(Arc::clone(&ledger)).unwrap();
            prepare_matched_entry(&ledger, &expected);
            assert_eq!(
                store.apply_open(expected.clone()).unwrap(),
                PositionApply::Applied
            );
            commit_entry(&ledger, &expected);
        }

        let reopened_ledger = Arc::new(ExecutionLedger::open_live(&path).unwrap());
        let reopened = PositionStore::from_ledger(reopened_ledger).unwrap();

        assert_eq!(reopened.snapshot(), vec![expected]);
    }

    #[test]
    fn reopened_store_preserves_exact_integer_amounts_and_basis_points() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution-ledger.jsonl");
        let expected = pos(
            2,
            "123456789012345678901234567890",
            "exact",
            "Testing",
            &["durable", "offline"],
            18_014_398_509_481_987,
            9_007_199_254_740_993,
        );
        {
            let ledger = Arc::new(ExecutionLedger::open_live(&path).unwrap());
            let store = PositionStore::from_ledger(Arc::clone(&ledger)).unwrap();
            prepare_matched_entry(&ledger, &expected);
            store.apply_open(expected.clone()).unwrap();
            commit_entry(&ledger, &expected);
        }

        let ledger = Arc::new(ExecutionLedger::open_live(&path).unwrap());
        let actual = PositionStore::from_ledger(ledger)
            .unwrap()
            .snapshot()
            .pop()
            .unwrap();

        assert_eq!(actual.token_id, expected.token_id);
        assert_eq!(actual.shares_micros, 18_014_398_509_481_987);
        assert_eq!(actual.usd_notional_micros, 9_007_199_254_740_993);
        assert_eq!(actual.take_profit_bps, 5_000);
        assert_eq!(actual.stop_loss_bps, 3_000);
        assert_eq!(actual.opened_at, expected.opened_at);
    }

    #[test]
    fn exact_open_retry_is_a_noop_but_conflicting_content_is_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let ledger = Arc::new(
            ExecutionLedger::open_live(dir.path().join("execution-ledger.jsonl")).unwrap(),
        );
        let store = PositionStore::from_ledger(Arc::clone(&ledger)).unwrap();
        let position = pos(3, "300", "open", "Politics", &[], 10_000_000, 5_000_000);
        prepare_matched_entry(&ledger, &position);
        assert_eq!(
            store.apply_open(position.clone()).unwrap(),
            PositionApply::Applied
        );
        let sequence = ledger.projection().sequence;

        assert_eq!(
            store.apply_open(position.clone()).unwrap(),
            PositionApply::AlreadyApplied
        );
        assert_eq!(ledger.projection().sequence, sequence);

        let mut conflicting = position;
        conflicting.usd_notional_micros += 1;
        let error = store.apply_open(conflicting).unwrap_err();
        assert_eq!(error.code(), PositionStoreErrorCode::IdempotencyConflict);
        assert_eq!(ledger.projection().sequence, sequence);
    }

    #[test]
    fn exact_close_retry_is_a_noop_but_conflicting_content_is_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let ledger = Arc::new(
            ExecutionLedger::open_live(dir.path().join("execution-ledger.jsonl")).unwrap(),
        );
        let store = PositionStore::from_ledger(Arc::clone(&ledger)).unwrap();
        let position = pos(4, "400", "close", "Politics", &[], 10_000_000, 5_000_000);
        prepare_matched_entry(&ledger, &position);
        store.apply_open(position.clone()).unwrap();
        commit_entry(&ledger, &position);
        let close = close(40, &position, 6_000_000);
        prepare_matched_exit(&ledger, &position, &close);
        assert_eq!(
            store.apply_close(close.clone()).unwrap(),
            PositionApply::Applied
        );
        let sequence = ledger.projection().sequence;

        assert_eq!(
            store.apply_close(close.clone()).unwrap(),
            PositionApply::AlreadyApplied
        );
        assert_eq!(ledger.projection().sequence, sequence);

        let mut conflicting = close;
        conflicting.usd_micros += 1;
        let error = store.apply_close(conflicting).unwrap_err();
        assert_eq!(error.code(), PositionStoreErrorCode::IdempotencyConflict);
        assert_eq!(ledger.projection().sequence, sequence);
    }

    #[test]
    fn close_removes_only_the_named_position() {
        let store = PositionStore::new_paper();
        let first = pos(5, "500", "first", "Politics", &[], 10_000_000, 5_000_000);
        let second = pos(6, "600", "second", "Sports", &[], 20_000_000, 8_000_000);
        store.apply_open(first.clone()).unwrap();
        store.apply_open(second.clone()).unwrap();

        assert_eq!(
            store.apply_close(close(50, &first, 6_000_000)).unwrap(),
            PositionApply::Applied
        );
        assert!(store.get_by_id(&first.position_id).is_none());
        assert_eq!(store.get_by_id(&second.position_id), Some(second));
    }

    #[test]
    fn rejected_and_uncertain_evidence_never_mutate_live_positions() {
        for (value, outcome) in [
            (
                7,
                LedgerPayload::RemoteRejected {
                    code: RemoteRejectCode::HttpRejected,
                },
            ),
            (
                8,
                LedgerPayload::RemoteUncertain {
                    code: UncertainCode::Timeout,
                },
            ),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let ledger = Arc::new(
                ExecutionLedger::open_live(dir.path().join("execution-ledger.jsonl")).unwrap(),
            );
            let store = PositionStore::from_ledger(Arc::clone(&ledger)).unwrap();
            let position = pos(
                value,
                &format!("{value}00"),
                "none",
                "Testing",
                &[],
                1_000_000,
                500_000,
            );
            ledger
                .append(
                    position.opening_intent_id,
                    LedgerPayload::IntentPrepared(prepared_entry(&position)),
                )
                .unwrap();
            ledger
                .append(position.opening_intent_id, LedgerPayload::SubmitStarted)
                .unwrap();
            ledger.append(position.opening_intent_id, outcome).unwrap();

            assert!(store.snapshot().is_empty());
            assert!(store.get_by_token(&position.token_id).is_none());
        }
    }

    #[test]
    fn paper_backend_rejects_a_second_open_position_for_the_same_token() {
        let store = PositionStore::new_paper();
        let first = pos(9, "900", "first", "Politics", &[], 2_000_000, 1_000_000);
        let second = pos(10, "900", "second", "Politics", &[], 4_000_000, 2_000_000);
        store.apply_open(first.clone()).unwrap();

        let error = store.apply_open(second).unwrap_err();

        assert_eq!(error.code(), PositionStoreErrorCode::PositionConflict);
        assert_eq!(store.snapshot(), vec![first]);
    }

    #[test]
    fn paper_backend_rejects_open_and_close_identity_reuse_across_roles() {
        let store = PositionStore::new_paper();
        let first = pos(19, "1900", "first", "Testing", &[], 2_000_000, 1_000_000);
        store.apply_open(first.clone()).unwrap();

        let reused_opening_close = PositionClose {
            position_id: first.position_id,
            closing_intent_id: first.opening_intent_id,
            closing_order_id: first.opening_order_id.clone(),
            shares_micros: first.shares_micros,
            usd_micros: 1_100_000,
            closed_at: opened_at() + chrono::Duration::hours(1),
        };
        assert_eq!(
            store.apply_close(reused_opening_close).unwrap_err().code(),
            PositionStoreErrorCode::IdempotencyConflict
        );
        assert_eq!(store.get_by_id(&first.position_id), Some(first.clone()));

        let applied_close = close(20, &first, 1_100_000);
        store.apply_close(applied_close).unwrap();
        let reusing_close_identity =
            pos(20, "2000", "second", "Testing", &[], 3_000_000, 1_500_000);
        assert_eq!(
            store.apply_open(reusing_close_identity).unwrap_err().code(),
            PositionStoreErrorCode::IdempotencyConflict
        );
    }

    #[test]
    fn paper_backend_never_reads_or_mutates_a_live_ledger() {
        let dir = tempfile::tempdir().unwrap();
        let ledger = Arc::new(
            ExecutionLedger::open_live(dir.path().join("execution-ledger.jsonl")).unwrap(),
        );
        let paper = PositionStore::new_paper();
        let position = pos(11, "1100", "paper", "Testing", &[], 3_000_000, 1_500_000);

        paper.apply_open(position.clone()).unwrap();

        assert_eq!(paper.get_by_token(&position.token_id), Some(position));
        assert_eq!(ledger.projection().sequence, 0);
        assert!(ledger.projection().positions.is_empty());
        let live = PositionStore::from_ledger(ledger).unwrap();
        assert!(live.is_empty());
    }

    #[test]
    fn category_exposure_sums_only_same_category() {
        let s = PositionStore::new_paper();
        s.apply_open(pos(
            12,
            "1200",
            "a",
            "Politics",
            &[],
            200_000_000,
            100_000_000,
        ))
        .unwrap();
        s.apply_open(pos(
            13,
            "1300",
            "b",
            "Politics",
            &[],
            100_000_000,
            50_000_000,
        ))
        .unwrap();
        s.apply_open(pos(
            14,
            "1400",
            "c",
            "Crypto",
            &[],
            400_000_000,
            200_000_000,
        ))
        .unwrap();
        assert_eq!(s.open_usd_by_category("Politics"), 150.0);
        assert_eq!(s.open_usd_by_category("crypto"), 200.0);
    }

    #[test]
    fn tag_exposure_sums_across_multiple_tags() {
        let s = PositionStore::new_paper();
        s.apply_open(pos(
            15,
            "1500",
            "a",
            "Politics",
            &["election2024", "us"],
            200_000_000,
            100_000_000,
        ))
        .unwrap();
        s.apply_open(pos(
            16,
            "1600",
            "b",
            "Politics",
            &["us"],
            100_000_000,
            50_000_000,
        ))
        .unwrap();
        assert_eq!(s.open_usd_by_tag("us"), 150.0);
        assert_eq!(s.open_usd_by_tag("election2024"), 100.0);
    }

    #[test]
    fn pnl_pct_buy_long_direction() {
        let p = pos(17, "1700", "a", "X", &[], 200_000_000, 100_000_000);
        assert!((p.pnl_pct(0.75) - 50.0).abs() < 1e-9);
        assert!((p.pnl_pct(0.25) + 50.0).abs() < 1e-9);
    }

    #[test]
    fn pnl_pct_sell_short_direction() {
        let mut p = pos(18, "1800", "a", "X", &[], 200_000_000, 100_000_000);
        p.side = OrderSide::Sell;
        assert!((p.pnl_pct(0.25) - 50.0).abs() < 1e-9);
    }
}
