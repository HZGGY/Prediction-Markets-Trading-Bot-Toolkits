#![allow(
    dead_code,
    reason = "Task 9 seals the recovery boundary before Task 10 wires its explicit operator service"
)]

use std::{error::Error, fmt};

use async_trait::async_trait;

use crate::service::{
    execution_ledger::{OrderId, ReconcileUncertainCode, TerminalNoFillStatus},
    order_gateway::PreparedOrderIdentity,
};

pub(crate) use crate::service::execution_ledger::CancelUncertainCode;

#[derive(Clone, Eq, Hash, PartialEq)]
pub(crate) struct TradeId(String);

impl TradeId {
    pub(crate) fn from_exact(value: impl Into<String>) -> Option<Self> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'));
        valid.then_some(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TradeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[redacted-trade-id]")
    }
}

impl fmt::Debug for TradeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "TradeId({self})")
    }
}

impl fmt::Display for CancelUncertainCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotFound => "not_found",
            Self::Timeout => "timeout",
            Self::Transport => "transport",
            Self::MalformedResponse => "malformed_response",
            Self::ResponseMismatch => "response_mismatch",
        })
    }
}

impl fmt::Debug for CancelUncertainCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "CancelUncertainCode({self})")
    }
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) enum RemoteOrderEvidence {
    Matched {
        making_micros: u128,
        taking_micros: u128,
        trade_ids: Vec<TradeId>,
    },
    NoFill {
        status: TerminalNoFillStatus,
    },
    Live,
    Pending,
    Uncertain {
        code: ReconcileUncertainCode,
    },
}

impl fmt::Display for RemoteOrderEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Matched {
                making_micros,
                taking_micros,
                trade_ids,
            } => write!(
                formatter,
                "matched(making_micros={making_micros}, taking_micros={taking_micros}, trade_count={})",
                trade_ids.len()
            ),
            Self::NoFill { status } => write!(formatter, "no_fill(status={status:?})"),
            Self::Live => formatter.write_str("live"),
            Self::Pending => formatter.write_str("pending"),
            Self::Uncertain { code } => write!(formatter, "uncertain(code={code:?})"),
        }
    }
}

impl fmt::Debug for RemoteOrderEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "RemoteOrderEvidence({self})")
    }
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) enum CancelAttemptEvidence {
    Canceled,
    NotCanceled,
    Uncertain { code: CancelUncertainCode },
}

impl fmt::Display for CancelAttemptEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Canceled => formatter.write_str("canceled"),
            Self::NotCanceled => formatter.write_str("not_canceled"),
            Self::Uncertain { code } => write!(formatter, "uncertain(code={code})"),
        }
    }
}

impl fmt::Debug for CancelAttemptEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "CancelAttemptEvidence({self})")
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum RecoveryError {
    Initialization,
}

impl fmt::Display for RecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("recovery gateway initialization failed")
    }
}

impl fmt::Debug for RecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RecoveryError(Initialization)")
    }
}

impl Error for RecoveryError {}

#[async_trait]
pub(crate) trait RecoveryGateway: Send + Sync {
    async fn reconcile_exact(
        &self,
        expected: &PreparedOrderIdentity,
    ) -> Result<RemoteOrderEvidence, RecoveryError>;

    async fn cancel_exact(
        &self,
        order_id: &OrderId,
    ) -> Result<CancelAttemptEvidence, RecoveryError>;
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use crate::service::{
        execution_ledger::{
            OrderId, OrderSide, OrderType, ReconcileUncertainCode, TerminalNoFillStatus, TokenId,
            Venue, ORDER_PROTOCOL_VERSION,
        },
        order_gateway::PreparedOrderIdentity,
    };

    use super::{
        CancelAttemptEvidence, CancelUncertainCode, RecoveryError, RecoveryGateway,
        RemoteOrderEvidence, TradeId,
    };

    struct ExactOnlyFake;

    #[async_trait]
    impl RecoveryGateway for ExactOnlyFake {
        async fn reconcile_exact(
            &self,
            _expected: &PreparedOrderIdentity,
        ) -> Result<RemoteOrderEvidence, RecoveryError> {
            Ok(RemoteOrderEvidence::Live)
        }

        async fn cancel_exact(
            &self,
            _order_id: &OrderId,
        ) -> Result<CancelAttemptEvidence, RecoveryError> {
            Ok(CancelAttemptEvidence::NotCanceled)
        }
    }

    fn expected_identity() -> PreparedOrderIdentity {
        PreparedOrderIdentity {
            order_id: OrderId::from_hex(format!("0x{}", "11".repeat(32))).unwrap(),
            protocol_version: ORDER_PROTOCOL_VERSION,
            venue: Venue::PolymarketClob,
            token_id: TokenId::from_decimal("12345").unwrap(),
            neg_risk: false,
            side: OrderSide::Buy,
            order_type: OrderType::Fok,
            expected_maker_micros: 20_000_000,
            expected_taker_micros: 40_000_000,
        }
    }

    #[tokio::test]
    async fn fake_needs_only_exact_identity_query_and_single_order_cancel() {
        let fake = ExactOnlyFake;
        assert_eq!(
            fake.reconcile_exact(&expected_identity()).await.unwrap(),
            RemoteOrderEvidence::Live
        );
        assert_eq!(
            fake.cancel_exact(&expected_identity().order_id)
                .await
                .unwrap(),
            CancelAttemptEvidence::NotCanceled
        );
    }

    #[test]
    fn evidence_uses_closed_ids_integer_amounts_and_ledger_statuses() {
        let trade_id = TradeId::from_exact("trade-123").unwrap();
        let evidence = RemoteOrderEvidence::Matched {
            making_micros: 20_000_000,
            taking_micros: 40_000_000,
            trade_ids: vec![trade_id.clone()],
        };
        let no_fill = RemoteOrderEvidence::NoFill {
            status: TerminalNoFillStatus::Canceled,
        };
        let uncertain = RemoteOrderEvidence::Uncertain {
            code: ReconcileUncertainCode::Mismatch,
        };
        let cancel_uncertain = CancelAttemptEvidence::Uncertain {
            code: CancelUncertainCode::Transport,
        };

        assert_eq!(trade_id.as_str(), "trade-123");
        assert!(matches!(
            evidence,
            RemoteOrderEvidence::Matched {
                making_micros: 20_000_000,
                taking_micros: 40_000_000,
                ..
            }
        ));
        assert_eq!(
            no_fill,
            RemoteOrderEvidence::NoFill {
                status: TerminalNoFillStatus::Canceled
            }
        );
        assert_eq!(
            uncertain,
            RemoteOrderEvidence::Uncertain {
                code: ReconcileUncertainCode::Mismatch
            }
        );
        assert_eq!(
            cancel_uncertain,
            CancelAttemptEvidence::Uncertain {
                code: CancelUncertainCode::Transport
            }
        );
    }

    #[test]
    fn debug_and_display_never_render_raw_dynamic_text_or_complete_ids() {
        let sentinel = "RAW_SDK_BODY_ERROR_SENTINEL";
        let trade_id = TradeId::from_exact("trade-RAW_SDK_BODY_ERROR_SENTINEL").unwrap();
        let evidence = RemoteOrderEvidence::Matched {
            making_micros: 20_000_000,
            taking_micros: 40_000_000,
            trade_ids: vec![trade_id],
        };
        let error = RecoveryError::Initialization;

        let rendered = format!("{evidence:?} {evidence} {error:?} {error}");
        assert!(!rendered.contains(sentinel));
    }
}
