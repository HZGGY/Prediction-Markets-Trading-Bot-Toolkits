use async_trait::async_trait;
use thiserror::Error;

use crate::models::PlannedOrder;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderReceipt {
    pub order_id: String,
    pub filled_shares_micros: u128,
    pub filled_usd_micros: u128,
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

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum OrderSubmitError {
    #[error("order preflight failed at {stage:?} ({code:?})")]
    Preflight {
        stage: OrderStage,
        code: OrderErrorCode,
    },
    #[error("order rejected with status {http_status:?} ({code:?})")]
    Rejected {
        http_status: Option<u16>,
        code: OrderErrorCode,
    },
    #[error("order result uncertain ({code:?})")]
    Uncertain { code: OrderErrorCode },
    #[error("order execution halted ({code:?})")]
    Halted { code: OrderErrorCode },
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
}

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
}
