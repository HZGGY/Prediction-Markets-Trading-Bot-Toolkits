use std::{collections::HashSet, error::Error, fmt, str::FromStr};

use alloy_primitives::U256;
use chrono::{DateTime, Utc};
use serde::{
    de::{self, MapAccess, SeqAccess, Visitor},
    ser::SerializeStruct,
    Deserialize, Deserializer, Serialize, Serializer,
};
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
        write!(
            formatter,
            "{}…{}",
            &self.0[..6],
            &self.0[self.0.len() - 4..]
        )
    }
}

impl fmt::Debug for OrderId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "OrderId({self})")
    }
}

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TokenId(U256);

impl TokenId {
    pub fn from_decimal(value: &str) -> Option<Self> {
        if value.is_empty() || (value.len() > 1 && value.starts_with('0')) {
            return None;
        }
        let parsed = U256::from_str(value).ok()?;
        (parsed.to_string() == value).then_some(Self(parsed))
    }

    pub fn as_u256(self) -> U256 {
        self.0
    }
}

impl fmt::Display for TokenId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl fmt::Debug for TokenId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "TokenId({self})")
    }
}

impl Serialize for TokenId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for TokenId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_decimal(&value).ok_or_else(|| de::Error::custom("invalid token id"))
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
#[serde(deny_unknown_fields)]
pub struct PositionSeed {
    pub slug: String,
    pub category: String,
    pub tags: Vec<String>,
    pub take_profit_bps: u32,
    pub stop_loss_bps: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "purpose", rename_all = "snake_case", deny_unknown_fields)]
pub enum IntentPurpose {
    Entry(PositionSeed),
    Exit { position_id: PositionId },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedIntent {
    pub order_id: OrderId,
    pub protocol_version: u8,
    pub venue: Venue,
    pub token_id: TokenId,
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct DurablePosition {
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
#[serde(deny_unknown_fields)]
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

#[derive(Clone, Debug, Eq, PartialEq)]
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

    fn payload_value(&self) -> Result<serde_json::Value, serde_json::Error> {
        use serde_json::Value;

        match self {
            Self::IntentPrepared(value) => serde_json::to_value(value),
            Self::SubmitStarted
            | Self::SubmissionCommitted
            | Self::SubmissionCommittedNoFill
            | Self::ReconciliationStarted
            | Self::ReconciledLive
            | Self::ReconciledPending
            | Self::CancelStarted => Ok(Value::Null),
            Self::RemoteMatched(value) | Self::ReconciledMatched(value) => {
                serde_json::to_value(value)
            }
            Self::RemoteRejected { code } => {
                serde_json::to_value(RemoteRejectedPayload { code: *code })
            }
            Self::RemoteUncertain { code } => {
                serde_json::to_value(RemoteUncertainPayload { code: *code })
            }
            Self::PositionOpened(value) => serde_json::to_value(value),
            Self::PositionClosed(value) => serde_json::to_value(value),
            Self::ReconciledNoFill { status } => {
                serde_json::to_value(ReconciledNoFillPayload { status: *status })
            }
            Self::ReconciledUncertain { code } => {
                serde_json::to_value(ReconciledUncertainPayload { code: *code })
            }
            Self::CancelResponseObserved { result } => {
                serde_json::to_value(CancelResponsePayload { result: *result })
            }
            Self::RecoveryApplied { position_event_id } => {
                serde_json::to_value(RecoveryAppliedPayload {
                    position_event_id: *position_event_id,
                })
            }
            Self::Acknowledged { reason } => {
                serde_json::to_value(AcknowledgedPayload { reason: *reason })
            }
        }
    }

    fn from_parts(kind: &str, payload: serde_json::Value) -> Result<Self, &'static str> {
        fn decode<T: serde::de::DeserializeOwned>(
            payload: serde_json::Value,
        ) -> Result<T, &'static str> {
            serde_json::from_value(payload).map_err(|_| "invalid ledger event payload")
        }

        fn unit(payload: serde_json::Value) -> Result<(), &'static str> {
            payload
                .is_null()
                .then_some(())
                .ok_or("unit event payload must be null")
        }

        Ok(match kind {
            "intent_prepared" => Self::IntentPrepared(decode(payload)?),
            "submit_started" => {
                unit(payload)?;
                Self::SubmitStarted
            }
            "remote_matched" => Self::RemoteMatched(decode(payload)?),
            "remote_rejected" => {
                let value: RemoteRejectedPayload = decode(payload)?;
                Self::RemoteRejected { code: value.code }
            }
            "remote_uncertain" => {
                let value: RemoteUncertainPayload = decode(payload)?;
                Self::RemoteUncertain { code: value.code }
            }
            "submission_committed" => {
                unit(payload)?;
                Self::SubmissionCommitted
            }
            "submission_committed_no_fill" => {
                unit(payload)?;
                Self::SubmissionCommittedNoFill
            }
            "position_opened" => Self::PositionOpened(decode(payload)?),
            "position_closed" => Self::PositionClosed(decode(payload)?),
            "reconciliation_started" => {
                unit(payload)?;
                Self::ReconciliationStarted
            }
            "reconciled_matched" => Self::ReconciledMatched(decode(payload)?),
            "reconciled_no_fill" => {
                let value: ReconciledNoFillPayload = decode(payload)?;
                Self::ReconciledNoFill {
                    status: value.status,
                }
            }
            "reconciled_live" => {
                unit(payload)?;
                Self::ReconciledLive
            }
            "reconciled_pending" => {
                unit(payload)?;
                Self::ReconciledPending
            }
            "reconciled_uncertain" => {
                let value: ReconciledUncertainPayload = decode(payload)?;
                Self::ReconciledUncertain { code: value.code }
            }
            "cancel_started" => {
                unit(payload)?;
                Self::CancelStarted
            }
            "cancel_response_observed" => {
                let value: CancelResponsePayload = decode(payload)?;
                Self::CancelResponseObserved {
                    result: value.result,
                }
            }
            "recovery_applied" => {
                let value: RecoveryAppliedPayload = decode(payload)?;
                Self::RecoveryApplied {
                    position_event_id: value.position_event_id,
                }
            }
            "acknowledged" => {
                let value: AcknowledgedPayload = decode(payload)?;
                Self::Acknowledged {
                    reason: value.reason,
                }
            }
            _ => return Err("unknown ledger event kind"),
        })
    }
}

struct UniqueJsonValue(serde_json::Value);

struct UniqueJsonValueVisitor;

impl<'de> Visitor<'de> for UniqueJsonValueVisitor {
    type Value = UniqueJsonValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("JSON without duplicate object fields")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(serde_json::Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(serde_json::Value::Number(value.into())))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(serde_json::Value::Number(value.into())))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .map(UniqueJsonValue)
            .ok_or_else(|| de::Error::custom("invalid JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(serde_json::Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(serde_json::Value::String(value)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(serde_json::Value::Null))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(serde_json::Value::Null))
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        UniqueJsonValue::deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element::<UniqueJsonValue>()? {
            values.push(value.0);
        }
        Ok(UniqueJsonValue(serde_json::Value::Array(values)))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut fields = serde_json::Map::new();
        let mut seen = HashSet::new();
        while let Some(key) = map.next_key::<String>()? {
            if !seen.insert(key.clone()) {
                return Err(de::Error::custom("duplicate JSON field"));
            }
            let value = map.next_value::<UniqueJsonValue>()?;
            fields.insert(key, value.0);
        }
        Ok(UniqueJsonValue(serde_json::Value::Object(fields)))
    }
}

impl<'de> Deserialize<'de> for UniqueJsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(UniqueJsonValueVisitor)
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RemoteRejectedPayload {
    code: RemoteRejectCode,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RemoteUncertainPayload {
    code: UncertainCode,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReconciledNoFillPayload {
    status: TerminalNoFillStatus,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReconciledUncertainPayload {
    code: ReconcileUncertainCode,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CancelResponsePayload {
    result: CancelResponseClass,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RecoveryAppliedPayload {
    position_event_id: EventId,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AcknowledgedPayload {
    reason: AcknowledgeReason,
}

impl Serialize for LedgerPayload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let payload = self.payload_value().map_err(serde::ser::Error::custom)?;
        let mut state = serializer.serialize_struct("LedgerPayload", 2)?;
        state.serialize_field("kind", self.kind())?;
        state.serialize_field("payload", &payload)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for LedgerPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            kind: String,
            payload: UniqueJsonValue,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::from_parts(&wire.kind, wire.payload.0).map_err(de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerEvent {
    pub schema_version: u32,
    pub sequence: u64,
    pub event_id: EventId,
    pub intent_id: IntentId,
    pub recorded_at: DateTime<Utc>,
    pub payload: LedgerPayload,
    pub previous_hash: EventHash,
    pub event_hash: EventHash,
}

impl Serialize for LedgerEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let payload = self
            .payload
            .payload_value()
            .map_err(serde::ser::Error::custom)?;
        let mut state = serializer.serialize_struct("LedgerEvent", 9)?;
        state.serialize_field("schema_version", &self.schema_version)?;
        state.serialize_field("sequence", &self.sequence)?;
        state.serialize_field("event_id", &self.event_id)?;
        state.serialize_field("intent_id", &self.intent_id)?;
        state.serialize_field("recorded_at", &self.recorded_at)?;
        state.serialize_field("kind", self.payload.kind())?;
        state.serialize_field("payload", &payload)?;
        state.serialize_field("previous_hash", &self.previous_hash)?;
        state.serialize_field("event_hash", &self.event_hash)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for LedgerEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            schema_version: u32,
            sequence: u64,
            event_id: EventId,
            intent_id: IntentId,
            recorded_at: DateTime<Utc>,
            kind: String,
            payload: UniqueJsonValue,
            previous_hash: EventHash,
            event_hash: EventHash,
        }

        let wire = Wire::deserialize(deserializer)?;
        let payload =
            LedgerPayload::from_parts(&wire.kind, wire.payload.0).map_err(de::Error::custom)?;
        Ok(Self {
            schema_version: wire.schema_version,
            sequence: wire.sequence,
            event_id: wire.event_id,
            intent_id: wire.intent_id,
            recorded_at: wire.recorded_at,
            payload,
            previous_hash: wire.previous_hash,
            event_hash: wire.event_hash,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LedgerErrorCode {
    Unavailable,
    UnsafePath,
    Locked,
    TruncatedTail,
    InvalidJson,
    UnsupportedEventKind,
    EventHashMismatch,
    SerializationFailed,
    AppendFailed,
    FlushFailed,
    SyncFailed,
    PersistFailed,
    DirectorySyncFailed,
    Fatal,
    UnsupportedSchema,
    SequenceExhausted,
    SequenceMismatch,
    PreviousHashMismatch,
    IdempotencyConflict,
    IdentityConflict,
    IntentMismatch,
    IllegalTransition,
    EvidenceConflict,
    PositionConflict,
}

impl fmt::Display for LedgerErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Unavailable => "unavailable",
            Self::UnsafePath => "unsafe_path",
            Self::Locked => "locked",
            Self::TruncatedTail => "truncated_tail",
            Self::InvalidJson => "invalid_json",
            Self::UnsupportedEventKind => "unsupported_event_kind",
            Self::EventHashMismatch => "event_hash_mismatch",
            Self::SerializationFailed => "serialization_failed",
            Self::AppendFailed => "append_failed",
            Self::FlushFailed => "flush_failed",
            Self::SyncFailed => "sync_failed",
            Self::PersistFailed => "persist_failed",
            Self::DirectorySyncFailed => "directory_sync_failed",
            Self::Fatal => "fatal",
            Self::UnsupportedSchema => "unsupported_schema",
            Self::SequenceExhausted => "sequence_exhausted",
            Self::SequenceMismatch => "sequence_mismatch",
            Self::PreviousHashMismatch => "previous_hash_mismatch",
            Self::IdempotencyConflict => "idempotency_conflict",
            Self::IdentityConflict => "identity_conflict",
            Self::IntentMismatch => "intent_mismatch",
            Self::IllegalTransition => "illegal_transition",
            Self::EvidenceConflict => "evidence_conflict",
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
            token_id: TokenId::from_decimal("123456789012345678901234567890").unwrap(),
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
            venue: Venue::PolymarketClob,
            token_id: TokenId::from_decimal("123456789012345678901234567890").unwrap(),
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
    fn all_nineteen_event_variants_match_complete_literal_json_goldens() {
        const GOLDENS: &str = r#"[
          {"schema_version":1,"sequence":1,"event_id":"00000000-0000-0000-0000-00000000000a","intent_id":"00000000-0000-0000-0000-000000000001","recorded_at":"2026-08-18T12:34:56Z","kind":"intent_prepared","payload":{"order_id":"0x1111111111111111111111111111111111111111111111111111111111111111","protocol_version":2,"venue":"polymarket_clob","token_id":"123456789012345678901234567890","neg_risk":true,"side":"buy","order_type":"fok","expected_maker_micros":"9007199254740993","expected_taker_micros":"18014398509481987","source_hash":"2222222222222222222222222222222222222222222222222222222222222222","purpose":{"purpose":"entry","slug":"will-example-pass","category":"testing","tags":["offline","durable"],"take_profit_bps":1250,"stop_loss_bps":750}},"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","event_hash":"4444444444444444444444444444444444444444444444444444444444444444"},
          {"schema_version":1,"sequence":1,"event_id":"00000000-0000-0000-0000-00000000000a","intent_id":"00000000-0000-0000-0000-000000000001","recorded_at":"2026-08-18T12:34:56Z","kind":"submit_started","payload":null,"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","event_hash":"4444444444444444444444444444444444444444444444444444444444444444"},
          {"schema_version":1,"sequence":1,"event_id":"00000000-0000-0000-0000-00000000000a","intent_id":"00000000-0000-0000-0000-000000000001","recorded_at":"2026-08-18T12:34:56Z","kind":"remote_matched","payload":{"shares_micros":"18014398509481987","usd_micros":"9007199254740993"},"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","event_hash":"4444444444444444444444444444444444444444444444444444444444444444"},
          {"schema_version":1,"sequence":1,"event_id":"00000000-0000-0000-0000-00000000000a","intent_id":"00000000-0000-0000-0000-000000000001","recorded_at":"2026-08-18T12:34:56Z","kind":"remote_rejected","payload":{"code":"server_rejected"},"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","event_hash":"4444444444444444444444444444444444444444444444444444444444444444"},
          {"schema_version":1,"sequence":1,"event_id":"00000000-0000-0000-0000-00000000000a","intent_id":"00000000-0000-0000-0000-000000000001","recorded_at":"2026-08-18T12:34:56Z","kind":"remote_uncertain","payload":{"code":"transport"},"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","event_hash":"4444444444444444444444444444444444444444444444444444444444444444"},
          {"schema_version":1,"sequence":1,"event_id":"00000000-0000-0000-0000-00000000000a","intent_id":"00000000-0000-0000-0000-000000000001","recorded_at":"2026-08-18T12:34:56Z","kind":"submission_committed","payload":null,"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","event_hash":"4444444444444444444444444444444444444444444444444444444444444444"},
          {"schema_version":1,"sequence":1,"event_id":"00000000-0000-0000-0000-00000000000a","intent_id":"00000000-0000-0000-0000-000000000001","recorded_at":"2026-08-18T12:34:56Z","kind":"submission_committed_no_fill","payload":null,"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","event_hash":"4444444444444444444444444444444444444444444444444444444444444444"},
          {"schema_version":1,"sequence":1,"event_id":"00000000-0000-0000-0000-00000000000a","intent_id":"00000000-0000-0000-0000-000000000001","recorded_at":"2026-08-18T12:34:56Z","kind":"position_opened","payload":{"position_id":"00000000-0000-0000-0000-000000000001","opening_intent_id":"00000000-0000-0000-0000-000000000001","opening_order_id":"0x1111111111111111111111111111111111111111111111111111111111111111","venue":"polymarket_clob","token_id":"123456789012345678901234567890","slug":"will-example-pass","category":"testing","tags":["offline","durable"],"neg_risk":true,"side":"buy","entry_shares_micros":"18014398509481987","entry_usd_micros":"9007199254740993","take_profit_bps":1250,"stop_loss_bps":750,"opened_at":"2026-08-18T12:34:56Z","closing_intent_id":null,"closing_order_id":null,"closing_shares_micros":null,"closing_usd_micros":null,"closed_at":null},"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","event_hash":"4444444444444444444444444444444444444444444444444444444444444444"},
          {"schema_version":1,"sequence":1,"event_id":"00000000-0000-0000-0000-00000000000a","intent_id":"00000000-0000-0000-0000-000000000001","recorded_at":"2026-08-18T12:34:56Z","kind":"position_closed","payload":{"position_id":"00000000-0000-0000-0000-000000000001","closing_intent_id":"00000000-0000-0000-0000-000000000002","closing_order_id":"0x3333333333333333333333333333333333333333333333333333333333333333","shares_micros":"18014398509481987","usd_micros":"10000000000000000","closed_at":"2026-08-18T12:34:56Z"},"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","event_hash":"4444444444444444444444444444444444444444444444444444444444444444"},
          {"schema_version":1,"sequence":1,"event_id":"00000000-0000-0000-0000-00000000000a","intent_id":"00000000-0000-0000-0000-000000000001","recorded_at":"2026-08-18T12:34:56Z","kind":"reconciliation_started","payload":null,"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","event_hash":"4444444444444444444444444444444444444444444444444444444444444444"},
          {"schema_version":1,"sequence":1,"event_id":"00000000-0000-0000-0000-00000000000a","intent_id":"00000000-0000-0000-0000-000000000001","recorded_at":"2026-08-18T12:34:56Z","kind":"reconciled_matched","payload":{"shares_micros":"18014398509481987","usd_micros":"9007199254740993"},"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","event_hash":"4444444444444444444444444444444444444444444444444444444444444444"},
          {"schema_version":1,"sequence":1,"event_id":"00000000-0000-0000-0000-00000000000a","intent_id":"00000000-0000-0000-0000-000000000001","recorded_at":"2026-08-18T12:34:56Z","kind":"reconciled_no_fill","payload":{"status":"canceled"},"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","event_hash":"4444444444444444444444444444444444444444444444444444444444444444"},
          {"schema_version":1,"sequence":1,"event_id":"00000000-0000-0000-0000-00000000000a","intent_id":"00000000-0000-0000-0000-000000000001","recorded_at":"2026-08-18T12:34:56Z","kind":"reconciled_live","payload":null,"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","event_hash":"4444444444444444444444444444444444444444444444444444444444444444"},
          {"schema_version":1,"sequence":1,"event_id":"00000000-0000-0000-0000-00000000000a","intent_id":"00000000-0000-0000-0000-000000000001","recorded_at":"2026-08-18T12:34:56Z","kind":"reconciled_pending","payload":null,"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","event_hash":"4444444444444444444444444444444444444444444444444444444444444444"},
          {"schema_version":1,"sequence":1,"event_id":"00000000-0000-0000-0000-00000000000a","intent_id":"00000000-0000-0000-0000-000000000001","recorded_at":"2026-08-18T12:34:56Z","kind":"reconciled_uncertain","payload":{"code":"not_found"},"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","event_hash":"4444444444444444444444444444444444444444444444444444444444444444"},
          {"schema_version":1,"sequence":1,"event_id":"00000000-0000-0000-0000-00000000000a","intent_id":"00000000-0000-0000-0000-000000000001","recorded_at":"2026-08-18T12:34:56Z","kind":"cancel_started","payload":null,"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","event_hash":"4444444444444444444444444444444444444444444444444444444444444444"},
          {"schema_version":1,"sequence":1,"event_id":"00000000-0000-0000-0000-00000000000a","intent_id":"00000000-0000-0000-0000-000000000001","recorded_at":"2026-08-18T12:34:56Z","kind":"cancel_response_observed","payload":{"result":"canceled"},"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","event_hash":"4444444444444444444444444444444444444444444444444444444444444444"},
          {"schema_version":1,"sequence":1,"event_id":"00000000-0000-0000-0000-00000000000a","intent_id":"00000000-0000-0000-0000-000000000001","recorded_at":"2026-08-18T12:34:56Z","kind":"recovery_applied","payload":{"position_event_id":"00000000-0000-0000-0000-000000000009"},"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","event_hash":"4444444444444444444444444444444444444444444444444444444444444444"},
          {"schema_version":1,"sequence":1,"event_id":"00000000-0000-0000-0000-00000000000a","intent_id":"00000000-0000-0000-0000-000000000001","recorded_at":"2026-08-18T12:34:56Z","kind":"acknowledged","payload":{"reason":"recovery_applied"},"previous_hash":"0000000000000000000000000000000000000000000000000000000000000000","event_hash":"4444444444444444444444444444444444444444444444444444444444444444"}
        ]"#;

        let expected: Value = serde_json::from_str(GOLDENS).unwrap();
        let actual = Value::Array(
            payloads()
                .into_iter()
                .map(|(_, payload)| serde_json::to_value(fixture_event(payload)).unwrap())
                .collect(),
        );
        assert_eq!(actual, expected);

        for event in actual.as_array().unwrap() {
            let decoded: LedgerEvent = serde_json::from_value(event.clone()).unwrap();
            assert_eq!(serde_json::to_value(decoded).unwrap(), *event);
        }
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
        assert!(encoded.contains(order_id(0x11).as_str()));
    }

    #[test]
    fn unknown_event_kind_is_rejected_by_the_closed_schema() {
        let mut value = serde_json::to_value(fixture_event(LedgerPayload::SubmitStarted)).unwrap();
        value["kind"] = json!("credential_rotated");

        assert!(serde_json::from_value::<LedgerEvent>(value).is_err());
    }

    #[test]
    fn unknown_fields_are_rejected_at_every_wire_boundary() {
        let event = fixture_event(LedgerPayload::IntentPrepared(prepared_intent()));

        let mut envelope = serde_json::to_value(&event).unwrap();
        envelope["unexpected_envelope"] = json!(true);
        assert!(serde_json::from_value::<LedgerEvent>(envelope).is_err());

        let mut payload = serde_json::to_value(fixture_event(LedgerPayload::RemoteRejected {
            code: RemoteRejectCode::ServerRejected,
        }))
        .unwrap();
        payload["payload"]["unexpected_payload"] = json!(true);
        assert!(serde_json::from_value::<LedgerEvent>(payload).is_err());

        let mut nested = serde_json::to_value(event).unwrap();
        nested["payload"]["purpose"]["unexpected_nested"] = json!(true);
        assert!(serde_json::from_value::<LedgerEvent>(nested).is_err());
    }

    #[test]
    fn raw_ledger_payload_rejects_duplicate_variant_payload_fields() {
        let raw = r#"{"kind":"remote_rejected","payload":{"code":"http_rejected","code":"server_rejected"}}"#;

        assert!(serde_json::from_str::<LedgerPayload>(raw).is_err());
    }

    #[test]
    fn raw_ledger_payload_rejects_duplicate_nested_payload_fields() {
        let raw = r#"{
            "kind":"intent_prepared",
            "payload":{
                "order_id":"0x1111111111111111111111111111111111111111111111111111111111111111",
                "protocol_version":2,
                "venue":"polymarket_clob",
                "token_id":"123",
                "neg_risk":false,
                "side":"buy",
                "order_type":"fok",
                "expected_maker_micros":"5",
                "expected_taker_micros":"10",
                "source_hash":null,
                "purpose":{
                    "purpose":"entry",
                    "slug":"first",
                    "slug":"second",
                    "category":"testing",
                    "tags":[],
                    "take_profit_bps":1250,
                    "stop_loss_bps":750
                }
            }
        }"#;

        assert!(serde_json::from_str::<LedgerPayload>(raw).is_err());
    }

    #[test]
    fn raw_ledger_event_rejects_duplicate_variant_payload_fields() {
        let raw = r#"{
            "schema_version":1,
            "sequence":1,
            "event_id":"00000000-0000-0000-0000-00000000000a",
            "intent_id":"00000000-0000-0000-0000-000000000001",
            "recorded_at":"2026-08-18T12:34:56Z",
            "kind":"remote_rejected",
            "payload":{"code":"http_rejected","code":"server_rejected"},
            "previous_hash":"0000000000000000000000000000000000000000000000000000000000000000",
            "event_hash":"4444444444444444444444444444444444444444444444444444444444444444"
        }"#;

        assert!(serde_json::from_str::<LedgerEvent>(raw).is_err());
    }

    #[test]
    fn raw_ledger_event_rejects_duplicate_nested_payload_fields() {
        let raw = r#"{
            "schema_version":1,
            "sequence":1,
            "event_id":"00000000-0000-0000-0000-00000000000a",
            "intent_id":"00000000-0000-0000-0000-000000000001",
            "recorded_at":"2026-08-18T12:34:56Z",
            "kind":"intent_prepared",
            "payload":{
                "order_id":"0x1111111111111111111111111111111111111111111111111111111111111111",
                "protocol_version":2,
                "venue":"polymarket_clob",
                "token_id":"123",
                "neg_risk":false,
                "side":"buy",
                "order_type":"fok",
                "expected_maker_micros":"5",
                "expected_taker_micros":"10",
                "source_hash":null,
                "purpose":{
                    "purpose":"entry",
                    "slug":"first",
                    "slug":"second",
                    "category":"testing",
                    "tags":[],
                    "take_profit_bps":1250,
                    "stop_loss_bps":750
                }
            },
            "previous_hash":"0000000000000000000000000000000000000000000000000000000000000000",
            "event_hash":"4444444444444444444444444444444444444444444444444444444444444444"
        }"#;

        assert!(serde_json::from_str::<LedgerEvent>(raw).is_err());
    }

    #[test]
    fn noncanonical_token_id_is_rejected_at_the_wire_boundary() {
        let mut value = serde_json::to_value(prepared_intent()).unwrap();
        value["token_id"] = json!("01");

        assert!(serde_json::from_value::<PreparedIntent>(value).is_err());

        let maximum =
            "115792089237316195423570985008687907853269984665640564039457584007913129639935";
        let token_id = TokenId::from_decimal(maximum).unwrap();
        assert_eq!(token_id.to_string(), maximum);
        assert_eq!(serde_json::to_value(token_id).unwrap(), maximum);
        assert!(TokenId::from_decimal(
            "115792089237316195423570985008687907853269984665640564039457584007913129639936"
        )
        .is_none());
    }

    #[test]
    fn order_id_default_display_is_redacted_but_explicit_access_is_exact() {
        let order_id = order_id(0x11);

        assert_eq!(order_id.as_str(), format!("0x{}", "11".repeat(32)));
        assert_eq!(order_id.to_string(), "0x1111…1111");
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

    #[test]
    fn durable_position_wire_identity_includes_venue() {
        let value = serde_json::to_value(durable_position()).unwrap();

        assert_eq!(value["venue"], "polymarket_clob");
    }
}
