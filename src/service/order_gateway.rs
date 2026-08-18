use std::{error::Error, fmt};

use async_trait::async_trait;

use crate::models::PlannedOrder;

pub const HALT_MARKER_IO_INSTRUCTION: &str =
    "do not restart until manual reconciliation is complete";

#[derive(Clone, PartialEq, Eq)]
pub struct OrderReceipt {
    pub order_id: String,
    pub filled_shares_micros: u128,
    pub filled_usd_micros: u128,
}

pub(crate) fn order_id_hint(order_id: &str) -> String {
    const VISIBLE: usize = 4;
    let characters = order_id.chars().collect::<Vec<_>>();
    if characters.len() <= VISIBLE * 2 {
        return "[redacted]".to_owned();
    }
    format!(
        "{}…{}",
        characters[..VISIBLE].iter().collect::<String>(),
        characters[characters.len() - VISIBLE..]
            .iter()
            .collect::<String>()
    )
}

impl fmt::Debug for OrderReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OrderReceipt")
            .field("order_id_hint", &order_id_hint(&self.order_id))
            .field("filled_shares_micros", &self.filled_shares_micros)
            .field("filled_usd_micros", &self.filled_usd_micros)
            .finish()
    }
}

impl OrderReceipt {
    pub fn filled_shares(&self) -> f64 {
        self.filled_shares_micros as f64 / 1_000_000.0
    }

    pub fn filled_usd(&self) -> f64 {
        self.filled_usd_micros as f64 / 1_000_000.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStage {
    Initialization,
    Metadata,
    Build,
    Sign,
    Post,
    Response,
    CircuitBreaker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderErrorCode {
    InvalidHost,
    InvalidChain,
    UnsupportedSignatureType,
    FunderMismatch,
    MissingCredentials,
    InvalidTokenId,
    MetadataLookupFailed,
    NegRiskMismatch,
    InvalidTickSize,
    InvalidPrice,
    InvalidSize,
    UnsupportedProtocolVersion,
    AmountConversion,
    SdkBuild,
    SdkSign,
    HttpRejected,
    ServerRejected,
    PostTimeout,
    PostTransport,
    MalformedResponse,
    NonFinalStatus,
    EmptyOrderId,
    AmountMismatch,
    HaltMarkerPresent,
    HaltMarkerIo,
    ExecutionHalted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderSubmitError {
    Preflight {
        stage: OrderStage,
        code: OrderErrorCode,
    },
    Rejected {
        http_status: Option<u16>,
        code: OrderErrorCode,
    },
    Uncertain {
        code: OrderErrorCode,
    },
    Halted {
        code: OrderErrorCode,
    },
}

impl OrderSubmitError {
    pub fn code(&self) -> OrderErrorCode {
        match self {
            Self::Preflight { code, .. }
            | Self::Rejected { code, .. }
            | Self::Uncertain { code }
            | Self::Halted { code } => *code,
        }
    }

    pub fn is_uncertain(&self) -> bool {
        matches!(self, Self::Uncertain { .. })
    }

    pub fn operator_instruction(&self) -> Option<&'static str> {
        matches!(
            self,
            Self::Halted {
                code: OrderErrorCode::HaltMarkerIo
            }
        )
        .then_some(HALT_MARKER_IO_INSTRUCTION)
    }
}

impl fmt::Display for OrderSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Preflight { stage, code } => {
                write!(formatter, "order preflight failed at {stage:?} ({code:?})")
            }
            Self::Rejected { http_status, code } => write!(
                formatter,
                "order rejected with status {http_status:?} ({code:?})"
            ),
            Self::Uncertain { code } => {
                write!(formatter, "order result uncertain ({code:?})")
            }
            Self::Halted {
                code: OrderErrorCode::HaltMarkerIo,
            } => write!(
                formatter,
                "order execution halted (HaltMarkerIo); {HALT_MARKER_IO_INSTRUCTION}"
            ),
            Self::Halted { code } => write!(formatter, "order execution halted ({code:?})"),
        }
    }
}

impl Error for OrderSubmitError {}

#[async_trait]
pub trait OrderGateway: Send + Sync {
    async fn submit_fok(&self, planned: &PlannedOrder) -> Result<OrderReceipt, OrderSubmitError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receipt_converts_micro_units_only_at_position_boundary() {
        let receipt = OrderReceipt {
            order_id: "0xabc".to_owned(),
            filled_shares_micros: 12_345_678,
            filled_usd_micros: 6_172_839,
        };
        assert!((receipt.filled_shares() - 12.345_678).abs() < 1e-12);
        assert!((receipt.filled_usd() - 6.172_839).abs() < 1e-12);
    }

    #[test]
    fn rendered_errors_are_stable_and_contain_no_dynamic_secret() {
        let sentinel = "SERVER_BODY_SECRET_SENTINEL";
        let error = OrderSubmitError::Rejected {
            http_status: Some(429),
            code: OrderErrorCode::HttpRejected,
        };
        let rendered = format!("{error:?} {error}");
        assert!(rendered.contains("HttpRejected"));
        assert!(!rendered.contains(sentinel));
    }

    #[test]
    fn receipt_debug_never_contains_the_complete_order_id() {
        let order_id = "ORDER_ID_DEBUG_SENTINEL_1234567890";
        let receipt = OrderReceipt {
            order_id: order_id.to_owned(),
            filled_shares_micros: 12_000_000,
            filled_usd_micros: 6_000_000,
        };

        let rendered = format!("{receipt:?}");

        assert!(!rendered.contains(order_id));
        assert!(rendered.contains("ORDE"));
        assert!(rendered.contains("7890"));
    }
}
