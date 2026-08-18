use std::{error::Error, fmt};

use chrono::{DateTime, Utc};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

pub const LEDGER_SCHEMA_VERSION: u32 = 1;
pub const ORDER_PROTOCOL_VERSION: u8 = 2;
pub const ZERO_EVENT_HASH: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";

mod decimal_u128 {
    use serde::{de, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &u128, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u128, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value
            .parse()
            .map_err(|_| de::Error::custom("invalid decimal integer"))
    }
}

mod optional_decimal_u128 {
    use serde::{de, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Option<u128>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(value) => serializer.serialize_some(&value.to_string()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<u128>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<String>::deserialize(deserializer)?
            .map(|value| {
                value
                    .parse()
                    .map_err(|_| de::Error::custom("invalid optional decimal integer"))
            })
            .transpose()
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct IntentId(pub Uuid);

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct EventId(pub Uuid);

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct PositionId(pub Uuid);

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct OrderId(String);

impl OrderId {
    pub fn from_hex(value: impl Into<String>) -> Option<Self> {
        let value = value.into();
        let valid = value.len() == 66
            && value.starts_with("0x")
            && value[2..]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
        valid.then_some(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for OrderId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl fmt::Debug for OrderId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "OrderId({}…{})",
            &self.0[..6],
            &self.0[self.0.len() - 4..]
        )
    }
}

impl Serialize for OrderId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for OrderId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_hex(value).ok_or_else(|| de::Error::custom("invalid order id"))
    }
}

#[derive(Clone, Default, Eq, Hash, PartialEq)]
pub struct EventHash([u8; 32]);

impl EventHash {
    pub fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let bytes = bytes.as_ref();
        assert_eq!(bytes.len(), 32, "event hashes require exactly 32 bytes");
        let mut hash = [0_u8; 32];
        hash.copy_from_slice(bytes);
        Self(hash)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for EventHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&hex::encode(self.0))
    }
}

impl fmt::Debug for EventHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "EventHash({self})")
    }
}

impl Serialize for EventHash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for EventHash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(de::Error::custom("invalid event hash"));
        }
        let bytes = hex::decode(value).map_err(|_| de::Error::custom("invalid event hash"))?;
        Ok(Self::from_bytes(bytes))
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Venue {
    PolymarketClob,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    Fok,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PositionSeed {
    pub slug: String,
    pub category: String,
    pub tags: Vec<String>,
    pub take_profit_bps: u32,
    pub stop_loss_bps: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "purpose", rename_all = "snake_case")]
pub enum IntentPurpose {
    Entry(PositionSeed),
    Exit { position_id: PositionId },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PreparedIntent {
    pub order_id: OrderId,
    pub protocol_version: u8,
    pub venue: Venue,
    pub token_id: String,
    pub neg_risk: bool,
    pub side: OrderSide,
    pub order_type: OrderType,
    #[serde(with = "decimal_u128")]
    pub expected_maker_micros: u128,
    #[serde(with = "decimal_u128")]
    pub expected_taker_micros: u128,
    pub source_hash: Option<EventHash>,
    pub purpose: IntentPurpose,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MatchedAmounts {
    #[serde(with = "decimal_u128")]
    pub shares_micros: u128,
    #[serde(with = "decimal_u128")]
    pub usd_micros: u128,
}

impl MatchedAmounts {
    pub fn is_positive(self) -> bool {
        self.shares_micros > 0 && self.usd_micros > 0
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DurablePosition {
    pub position_id: PositionId,
    pub opening_intent_id: IntentId,
    pub opening_order_id: OrderId,
    pub token_id: String,
    pub slug: String,
    pub category: String,
    pub tags: Vec<String>,
    pub neg_risk: bool,
    pub side: OrderSide,
    #[serde(with = "decimal_u128")]
    pub entry_shares_micros: u128,
    #[serde(with = "decimal_u128")]
    pub entry_usd_micros: u128,
    pub take_profit_bps: u32,
    pub stop_loss_bps: u32,
    pub opened_at: DateTime<Utc>,
    pub closing_intent_id: Option<IntentId>,
    pub closing_order_id: Option<OrderId>,
    #[serde(with = "optional_decimal_u128")]
    pub closing_shares_micros: Option<u128>,
    #[serde(with = "optional_decimal_u128")]
    pub closing_usd_micros: Option<u128>,
    pub closed_at: Option<DateTime<Utc>>,
}

impl DurablePosition {
    pub fn is_open(&self) -> bool {
        self.closing_intent_id.is_none()
            && self.closing_order_id.is_none()
            && self.closing_shares_micros.is_none()
            && self.closing_usd_micros.is_none()
            && self.closed_at.is_none()
    }

    pub fn entry_price_ratio(&self) -> (u128, u128) {
        (self.entry_usd_micros, self.entry_shares_micros)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PositionClose {
    pub position_id: PositionId,
    pub closing_intent_id: IntentId,
    pub closing_order_id: OrderId,
    #[serde(with = "decimal_u128")]
    pub shares_micros: u128,
    #[serde(with = "decimal_u128")]
    pub usd_micros: u128,
    pub closed_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteRejectCode {
    HttpRejected,
    ServerRejected,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UncertainCode {
    Timeout,
    Transport,
    MalformedResponse,
    NonFinalStatus,
    AmountMismatch,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalNoFillStatus {
    Canceled,
    Invalid,
    Rejected,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconcileUncertainCode {
    NotFound,
    PartialFill,
    Mismatch,
    Timeout,
    Transport,
    MalformedResponse,
    UnknownStatus,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelResponseClass {
    Canceled,
    NotCanceled,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AcknowledgeReason {
    NotSent,
    ReconciledNoFill,
    RecoveryApplied,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum LedgerPayload {
    IntentPrepared(PreparedIntent),
    SubmitStarted,
    RemoteMatched(MatchedAmounts),
    RemoteRejected { code: RemoteRejectCode },
    RemoteUncertain { code: UncertainCode },
    SubmissionCommitted,
    SubmissionCommittedNoFill,
    PositionOpened(DurablePosition),
    PositionClosed(PositionClose),
    ReconciliationStarted,
    ReconciledMatched(MatchedAmounts),
    ReconciledNoFill { status: TerminalNoFillStatus },
    ReconciledLive,
    ReconciledPending,
    ReconciledUncertain { code: ReconcileUncertainCode },
    CancelStarted,
    CancelResponseObserved { result: CancelResponseClass },
    RecoveryApplied { position_event_id: EventId },
    Acknowledged { reason: AcknowledgeReason },
}

impl LedgerPayload {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::IntentPrepared(_) => "intent_prepared",
            Self::SubmitStarted => "submit_started",
            Self::RemoteMatched(_) => "remote_matched",
            Self::RemoteRejected { .. } => "remote_rejected",
            Self::RemoteUncertain { .. } => "remote_uncertain",
            Self::SubmissionCommitted => "submission_committed",
            Self::SubmissionCommittedNoFill => "submission_committed_no_fill",
            Self::PositionOpened(_) => "position_opened",
            Self::PositionClosed(_) => "position_closed",
            Self::ReconciliationStarted => "reconciliation_started",
            Self::ReconciledMatched(_) => "reconciled_matched",
            Self::ReconciledNoFill { .. } => "reconciled_no_fill",
            Self::ReconciledLive => "reconciled_live",
            Self::ReconciledPending => "reconciled_pending",
            Self::ReconciledUncertain { .. } => "reconciled_uncertain",
            Self::CancelStarted => "cancel_started",
            Self::CancelResponseObserved { .. } => "cancel_response_observed",
            Self::RecoveryApplied { .. } => "recovery_applied",
            Self::Acknowledged { .. } => "acknowledged",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LedgerEvent {
    pub schema_version: u32,
    pub sequence: u64,
    pub event_id: EventId,
    pub intent_id: IntentId,
    pub recorded_at: DateTime<Utc>,
    #[serde(flatten)]
    pub payload: LedgerPayload,
    pub previous_hash: EventHash,
    pub event_hash: EventHash,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LedgerErrorCode {
    UnsupportedSchema,
    SequenceMismatch,
    PreviousHashMismatch,
    IdempotencyConflict,
    IntentMismatch,
    IllegalTransition,
    PositionConflict,
}

impl fmt::Display for LedgerErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::UnsupportedSchema => "unsupported_schema",
            Self::SequenceMismatch => "sequence_mismatch",
            Self::PreviousHashMismatch => "previous_hash_mismatch",
            Self::IdempotencyConflict => "idempotency_conflict",
            Self::IntentMismatch => "intent_mismatch",
            Self::IllegalTransition => "illegal_transition",
            Self::PositionConflict => "position_conflict",
        })
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct LedgerError {
    code: LedgerErrorCode,
    label: &'static str,
}

impl LedgerError {
    pub fn new(code: LedgerErrorCode) -> Self {
        Self::with_label(code, "execution_ledger")
    }

    pub fn with_label(code: LedgerErrorCode, label: &'static str) -> Self {
        Self { code, label }
    }

    pub fn code(&self) -> LedgerErrorCode {
        self.code
    }

    pub fn label(&self) -> &'static str {
        self.label
    }
}

impl fmt::Display for LedgerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ledger_error(code={},label={})",
            self.code, self.label
        )
    }
}

impl fmt::Debug for LedgerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl Error for LedgerError {}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use serde_json::{json, Value};
    use uuid::Uuid;

    use super::*;

    fn intent_id(value: u128) -> IntentId {
        IntentId(Uuid::from_u128(value))
    }

    fn event_id(value: u128) -> EventId {
        EventId(Uuid::from_u128(value))
    }

    fn order_id(byte: u8) -> OrderId {
        OrderId::from_hex(format!("0x{}", format!("{byte:02x}").repeat(32))).unwrap()
    }

    fn event_hash(byte: u8) -> EventHash {
        EventHash::from_bytes([byte; 32])
    }

    fn recorded_at() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 18, 12, 34, 56)
            .single()
            .unwrap()
    }

    fn position_seed() -> PositionSeed {
        PositionSeed {
            slug: "will-example-pass".to_owned(),
            category: "testing".to_owned(),
            tags: vec!["offline".to_owned(), "durable".to_owned()],
            take_profit_bps: 1_250,
            stop_loss_bps: 750,
        }
    }

    fn prepared_intent() -> PreparedIntent {
        PreparedIntent {
            order_id: order_id(0x11),
            protocol_version: 2,
            venue: Venue::PolymarketClob,
            token_id: "123456789012345678901234567890".to_owned(),
            neg_risk: true,
            side: OrderSide::Buy,
            order_type: OrderType::Fok,
            expected_maker_micros: 9_007_199_254_740_993,
            expected_taker_micros: 18_014_398_509_481_987,
            source_hash: Some(event_hash(0x22)),
            purpose: IntentPurpose::Entry(position_seed()),
        }
    }

    fn matched_amounts() -> MatchedAmounts {
        MatchedAmounts {
            shares_micros: 18_014_398_509_481_987,
            usd_micros: 9_007_199_254_740_993,
        }
    }

    fn durable_position() -> DurablePosition {
        DurablePosition {
            position_id: PositionId(intent_id(1).0),
            opening_intent_id: intent_id(1),
            opening_order_id: order_id(0x11),
            token_id: "123456789012345678901234567890".to_owned(),
            slug: "will-example-pass".to_owned(),
            category: "testing".to_owned(),
            tags: vec!["offline".to_owned(), "durable".to_owned()],
            neg_risk: true,
            side: OrderSide::Buy,
            entry_shares_micros: 18_014_398_509_481_987,
            entry_usd_micros: 9_007_199_254_740_993,
            take_profit_bps: 1_250,
            stop_loss_bps: 750,
            opened_at: recorded_at(),
            closing_intent_id: None,
            closing_order_id: None,
            closing_shares_micros: None,
            closing_usd_micros: None,
            closed_at: None,
        }
    }

    fn payloads() -> Vec<(&'static str, LedgerPayload)> {
        vec![
            (
                "intent_prepared",
                LedgerPayload::IntentPrepared(prepared_intent()),
            ),
            ("submit_started", LedgerPayload::SubmitStarted),
            (
                "remote_matched",
                LedgerPayload::RemoteMatched(matched_amounts()),
            ),
            (
                "remote_rejected",
                LedgerPayload::RemoteRejected {
                    code: RemoteRejectCode::ServerRejected,
                },
            ),
            (
                "remote_uncertain",
                LedgerPayload::RemoteUncertain {
                    code: UncertainCode::Transport,
                },
            ),
            ("submission_committed", LedgerPayload::SubmissionCommitted),
            (
                "submission_committed_no_fill",
                LedgerPayload::SubmissionCommittedNoFill,
            ),
            (
                "position_opened",
                LedgerPayload::PositionOpened(durable_position()),
            ),
            (
                "position_closed",
                LedgerPayload::PositionClosed(PositionClose {
                    position_id: PositionId(intent_id(1).0),
                    closing_intent_id: intent_id(2),
                    closing_order_id: order_id(0x33),
                    shares_micros: 18_014_398_509_481_987,
                    usd_micros: 10_000_000_000_000_000,
                    closed_at: recorded_at(),
                }),
            ),
            (
                "reconciliation_started",
                LedgerPayload::ReconciliationStarted,
            ),
            (
                "reconciled_matched",
                LedgerPayload::ReconciledMatched(matched_amounts()),
            ),
            (
                "reconciled_no_fill",
                LedgerPayload::ReconciledNoFill {
                    status: TerminalNoFillStatus::Canceled,
                },
            ),
            ("reconciled_live", LedgerPayload::ReconciledLive),
            ("reconciled_pending", LedgerPayload::ReconciledPending),
            (
                "reconciled_uncertain",
                LedgerPayload::ReconciledUncertain {
                    code: ReconcileUncertainCode::NotFound,
                },
            ),
            ("cancel_started", LedgerPayload::CancelStarted),
            (
                "cancel_response_observed",
                LedgerPayload::CancelResponseObserved {
                    result: CancelResponseClass::Canceled,
                },
            ),
            (
                "recovery_applied",
                LedgerPayload::RecoveryApplied {
                    position_event_id: event_id(9),
                },
            ),
            (
                "acknowledged",
                LedgerPayload::Acknowledged {
                    reason: AcknowledgeReason::RecoveryApplied,
                },
            ),
        ]
    }

    fn fixture_event(payload: LedgerPayload) -> LedgerEvent {
        LedgerEvent {
            schema_version: LEDGER_SCHEMA_VERSION,
            sequence: 1,
            event_id: event_id(10),
            intent_id: intent_id(1),
            recorded_at: recorded_at(),
            payload,
            previous_hash: EventHash::default(),
            event_hash: event_hash(0x44),
        }
    }

    #[test]
    fn every_event_discriminant_has_stable_snake_case_json() {
        let actual = payloads()
            .into_iter()
            .map(|(expected, payload)| {
                let value = serde_json::to_value(fixture_event(payload)).unwrap();
                assert_eq!(value["kind"], expected);
                let decoded: LedgerEvent = serde_json::from_value(value).unwrap();
                expected.to_owned() + ":" + decoded.payload.kind()
            })
            .collect::<Vec<_>>();

        assert_eq!(
            actual,
            [
                "intent_prepared:intent_prepared",
                "submit_started:submit_started",
                "remote_matched:remote_matched",
                "remote_rejected:remote_rejected",
                "remote_uncertain:remote_uncertain",
                "submission_committed:submission_committed",
                "submission_committed_no_fill:submission_committed_no_fill",
                "position_opened:position_opened",
                "position_closed:position_closed",
                "reconciliation_started:reconciliation_started",
                "reconciled_matched:reconciled_matched",
                "reconciled_no_fill:reconciled_no_fill",
                "reconciled_live:reconciled_live",
                "reconciled_pending:reconciled_pending",
                "reconciled_uncertain:reconciled_uncertain",
                "cancel_started:cancel_started",
                "cancel_response_observed:cancel_response_observed",
                "recovery_applied:recovery_applied",
                "acknowledged:acknowledged",
            ]
        );
    }

    #[test]
    fn identities_and_amounts_round_trip_exactly_beyond_f64_integer_range() {
        let event = fixture_event(LedgerPayload::IntentPrepared(prepared_intent()));
        let encoded = serde_json::to_string(&event).unwrap();
        let decoded: LedgerEvent = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded, event);
        assert!(encoded.contains("9007199254740993"));
        assert!(encoded.contains("18014398509481987"));
        assert!(encoded.contains("123456789012345678901234567890"));
        assert!(encoded.contains(&order_id(0x11).to_string()));
    }

    #[test]
    fn unknown_event_kind_is_rejected_by_the_closed_schema() {
        let mut value = serde_json::to_value(fixture_event(LedgerPayload::SubmitStarted)).unwrap();
        value["kind"] = json!("credential_rotated");

        assert!(serde_json::from_value::<LedgerEvent>(value).is_err());
    }

    #[test]
    fn serialized_events_have_no_secret_shaped_fields_or_values() {
        const FORBIDDEN_KEYS: &[&str] = &[
            "private_key",
            "api_key",
            "api_secret",
            "passphrase",
            "hmac",
            "signature",
            "signed_order",
            "request_body",
            "response_body",
            "server_message",
        ];
        const SENTINEL: &str = "SECRET_VALUE_SENTINEL";

        fn assert_secret_free(value: &Value) {
            match value {
                Value::Object(fields) => {
                    for (key, value) in fields {
                        assert!(
                            !FORBIDDEN_KEYS.contains(&key.as_str()),
                            "forbidden key: {key}"
                        );
                        assert_secret_free(value);
                    }
                }
                Value::Array(values) => values.iter().for_each(assert_secret_free),
                Value::String(value) => assert!(!value.contains(SENTINEL)),
                _ => {}
            }
        }

        for (_, payload) in payloads() {
            assert_secret_free(&serde_json::to_value(fixture_event(payload)).unwrap());
        }
    }

    #[test]
    fn ledger_errors_render_only_stable_code_and_configured_label() {
        let sentinel = "SECRET_DYNAMIC_PATH_OR_SERVER_BODY";
        let error = LedgerError::with_label(LedgerErrorCode::IllegalTransition, "live_ledger");
        let rendered = format!("{error:?} {error}");

        assert_eq!(
            rendered,
            "ledger_error(code=illegal_transition,label=live_ledger) ledger_error(code=illegal_transition,label=live_ledger)"
        );
        assert!(!rendered.contains(sentinel));
    }

    #[test]
    fn zero_hash_is_a_stable_lowercase_hex_identity() {
        assert_eq!(EventHash::default().to_string(), ZERO_EVENT_HASH);
        assert_eq!(
            serde_json::to_value(EventHash::default()).unwrap(),
            ZERO_EVENT_HASH
        );
    }
}
