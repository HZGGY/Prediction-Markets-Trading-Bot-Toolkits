use std::borrow::Cow;
use std::fmt;
use std::str::FromStr as _;
use std::time::Duration;

use alloy_signer_local::PrivateKeySigner;
use async_trait::async_trait;
use polymarket_client_sdk_v2::auth::{
    state::Authenticated, Credentials, Normal, Signer as _, Uuid,
};
use polymarket_client_sdk_v2::clob::types::response::PostOrderResponse;
use polymarket_client_sdk_v2::clob::types::{
    Eip712Domain, OrderStatusType, OrderType as SdkOrderType, Side as SdkSide,
    SignedOrder as SdkSignedOrder,
};
use polymarket_client_sdk_v2::clob::{Client, Config as SdkConfig};
use polymarket_client_sdk_v2::error::EmptyResponse as SdkEmptyResponse;
use polymarket_client_sdk_v2::error::{Error as SdkError, Status as SdkStatus};
use polymarket_client_sdk_v2::types::{Address, Decimal, U256};
use polymarket_client_sdk_v2::{contract_config, POLYGON};

use crate::config::{AppConfig, OFFICIAL_CLOB_V2_HOST};
use crate::models::{OrderType, PlannedOrder, Side};
use crate::service::order_gateway::{
    OrderErrorCode, OrderGateway, OrderReceipt, OrderStage, OrderSubmitError,
};

type AuthenticatedClient = Client<Authenticated<Normal>>;

pub struct SdkOrderGateway {
    client: AuthenticatedClient,
    signer: PrivateKeySigner,
    post_timeout: Duration,
}

struct PreparedOrder {
    signed: SdkSignedOrder,
    expected_making: Decimal,
    expected_taking: Decimal,
    side: Side,
}

impl fmt::Debug for PreparedOrder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedOrder")
            .field("signed", &"<redacted>")
            .field("expected_making", &self.expected_making)
            .field("expected_taking", &self.expected_taking)
            .field("side", &self.side)
            .finish()
    }
}

impl SdkOrderGateway {
    pub async fn new(cfg: &AppConfig) -> Result<Self, OrderSubmitError> {
        if cfg.site.clob_api_base != OFFICIAL_CLOB_V2_HOST {
            return Err(preflight(
                OrderStage::Initialization,
                OrderErrorCode::InvalidHost,
            ));
        }
        Self::new_with_host(cfg, &cfg.site.clob_api_base, Duration::from_secs(15)).await
    }

    async fn new_with_host(
        cfg: &AppConfig,
        host: &str,
        post_timeout: Duration,
    ) -> Result<Self, OrderSubmitError> {
        if cfg.exchange.chain_id != POLYGON {
            return Err(preflight(
                OrderStage::Initialization,
                OrderErrorCode::InvalidChain,
            ));
        }
        if cfg.credentials.signature_type != Some(0) {
            return Err(preflight(
                OrderStage::Initialization,
                OrderErrorCode::UnsupportedSignatureType,
            ));
        }
        let signer = PrivateKeySigner::from_str(cfg.credentials.private_key.trim())
            .map_err(|_| {
                preflight(
                    OrderStage::Initialization,
                    OrderErrorCode::MissingCredentials,
                )
            })?
            .with_chain_id(Some(POLYGON));
        let funder = Address::from_str(&cfg.credentials.funder_address)
            .map_err(|_| preflight(OrderStage::Initialization, OrderErrorCode::FunderMismatch))?;
        if signer.address() != funder {
            return Err(preflight(
                OrderStage::Initialization,
                OrderErrorCode::FunderMismatch,
            ));
        }
        let key = cfg.credentials.api_key.as_deref().ok_or_else(|| {
            preflight(
                OrderStage::Initialization,
                OrderErrorCode::MissingCredentials,
            )
        })?;
        let key = Uuid::parse_str(key).map_err(|_| {
            preflight(
                OrderStage::Initialization,
                OrderErrorCode::MissingCredentials,
            )
        })?;
        let secret = cfg
            .credentials
            .api_secret
            .clone()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                preflight(
                    OrderStage::Initialization,
                    OrderErrorCode::MissingCredentials,
                )
            })?;
        let passphrase = cfg
            .credentials
            .api_passphrase
            .clone()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                preflight(
                    OrderStage::Initialization,
                    OrderErrorCode::MissingCredentials,
                )
            })?;
        let credentials = Credentials::new(key, secret, passphrase);
        let client = Client::new(host, SdkConfig::default())
            .map_err(|_| preflight(OrderStage::Initialization, OrderErrorCode::SdkBuild))?
            .authentication_builder(&signer)
            .credentials(credentials)
            .authenticate()
            .await
            .map_err(|_| preflight(OrderStage::Initialization, OrderErrorCode::SdkBuild))?;
        Ok(Self {
            client,
            signer,
            post_timeout,
        })
    }

    async fn prepare_fok(&self, planned: &PlannedOrder) -> Result<PreparedOrder, OrderSubmitError> {
        if planned.order_type != OrderType::Fok {
            return Err(preflight(OrderStage::Build, OrderErrorCode::SdkBuild));
        }
        let token_id = parse_token_id(&planned.token_id)?;
        let tick = self
            .client
            .tick_size(token_id)
            .await
            .map_err(|_| preflight(OrderStage::Metadata, OrderErrorCode::MetadataLookupFailed))?
            .minimum_tick_size
            .as_decimal();
        let sdk_neg_risk = self
            .client
            .neg_risk(token_id)
            .await
            .map_err(|_| preflight(OrderStage::Metadata, OrderErrorCode::MetadataLookupFailed))?
            .neg_risk;
        if sdk_neg_risk != planned.neg_risk {
            return Err(preflight(
                OrderStage::Metadata,
                OrderErrorCode::NegRiskMismatch,
            ));
        }
        let price = align_price(
            decimal_from_f64(planned.limit_price, OrderErrorCode::InvalidPrice)?,
            tick,
            planned.side,
        )?;
        let size = validated_size_from_f64(planned.shares)?;
        let sdk_side = match planned.side {
            Side::Buy => SdkSide::Buy,
            Side::Sell => SdkSide::Sell,
        };
        let signable = self
            .client
            .limit_order()
            .token_id(token_id)
            .side(sdk_side)
            .price(price)
            .size(size)
            .order_type(SdkOrderType::FOK)
            .build()
            .await
            .map_err(|_| preflight(OrderStage::Build, OrderErrorCode::SdkBuild))?;
        if signable.payload.version() != 2 {
            return Err(preflight(
                OrderStage::Build,
                OrderErrorCode::UnsupportedProtocolVersion,
            ));
        }
        let order = signable.payload.as_v2().ok_or_else(|| {
            preflight(
                OrderStage::Build,
                OrderErrorCode::UnsupportedProtocolVersion,
            )
        })?;
        let expected_making = u256_micros_to_decimal(order.makerAmount)?;
        let expected_taking = u256_micros_to_decimal(order.takerAmount)?;
        let signed = self
            .client
            .sign(&self.signer, signable)
            .await
            .map_err(|_| preflight(OrderStage::Sign, OrderErrorCode::SdkSign))?;
        Ok(PreparedOrder {
            signed,
            expected_making,
            expected_taking,
            side: planned.side,
        })
    }
}

fn preflight(stage: OrderStage, code: OrderErrorCode) -> OrderSubmitError {
    OrderSubmitError::Preflight { stage, code }
}

#[allow(
    dead_code,
    reason = "Task 1 proves the ID before later pre-POST wiring"
)]
fn exact_v2_order_id(signed: &SdkSignedOrder, neg_risk: bool) -> Result<String, OrderSubmitError> {
    let exchange = contract_config(POLYGON, neg_risk)
        .and_then(|config| config.exchange_v2)
        .ok_or_else(|| preflight(OrderStage::Sign, OrderErrorCode::ExactOrderIdUnavailable))?;
    let mut domain = Eip712Domain::default();
    domain.name = Some(Cow::Borrowed("Polymarket CTF Exchange"));
    domain.version = Some(Cow::Borrowed("2"));
    domain.chain_id = Some(U256::from(POLYGON));
    domain.verifying_contract = Some(exchange);
    Ok(format!(
        "{:#x}",
        signed.v2_order_hash(&domain).map_err(|_| {
            preflight(OrderStage::Sign, OrderErrorCode::ExactOrderIdUnavailable)
        })?
    ))
}

fn decimal_from_f64(value: f64, code: OrderErrorCode) -> Result<Decimal, OrderSubmitError> {
    if !value.is_finite() {
        return Err(preflight(OrderStage::Build, code));
    }
    Decimal::from_str(&value.to_string()).map_err(|_| preflight(OrderStage::Build, code))
}

fn validated_size_from_f64(value: f64) -> Result<Decimal, OrderSubmitError> {
    let size = decimal_from_f64(value, OrderErrorCode::InvalidSize)?.normalize();
    if size <= Decimal::ZERO || size.scale() > 2 {
        return Err(preflight(OrderStage::Build, OrderErrorCode::InvalidSize));
    }
    exact_decimal_to_micros(size, OrderStage::Build, OrderErrorCode::InvalidSize)?;
    Ok(size)
}

fn parse_token_id(value: &str) -> Result<U256, OrderSubmitError> {
    U256::from_str(value).map_err(|_| preflight(OrderStage::Build, OrderErrorCode::InvalidTokenId))
}

fn align_price(price: Decimal, tick: Decimal, side: Side) -> Result<Decimal, OrderSubmitError> {
    if tick <= Decimal::ZERO || tick >= Decimal::ONE {
        return Err(preflight(
            OrderStage::Metadata,
            OrderErrorCode::InvalidTickSize,
        ));
    }
    let remainder = price % tick;
    let aligned = if remainder.is_zero() {
        price
    } else {
        match side {
            Side::Buy => price - remainder,
            Side::Sell => price + (tick - remainder),
        }
    };
    if aligned < tick || aligned > Decimal::ONE - tick {
        return Err(preflight(OrderStage::Build, OrderErrorCode::InvalidPrice));
    }
    Ok(aligned.normalize())
}

fn decimal_to_micros(value: Decimal) -> Result<u128, OrderSubmitError> {
    exact_decimal_to_micros(
        value,
        OrderStage::Response,
        OrderErrorCode::AmountConversion,
    )
}

fn exact_decimal_to_micros(
    value: Decimal,
    stage: OrderStage,
    code: OrderErrorCode,
) -> Result<u128, OrderSubmitError> {
    let value = value.normalize();
    if value.is_sign_negative() || value.scale() > 6 {
        return Err(preflight(stage, code));
    }
    let factor = 10_i128
        .checked_pow(6 - value.scale())
        .ok_or_else(|| preflight(stage, code))?;
    let micros = value
        .mantissa()
        .checked_mul(factor)
        .ok_or_else(|| preflight(stage, code))?;
    let reconstructed =
        Decimal::try_from_i128_with_scale(micros, 6).map_err(|_| preflight(stage, code))?;
    if reconstructed != value {
        return Err(preflight(stage, code));
    }
    micros.try_into().map_err(|_| preflight(stage, code))
}

fn u256_micros_to_decimal(value: U256) -> Result<Decimal, OrderSubmitError> {
    let raw: u128 = value
        .try_into()
        .map_err(|_| preflight(OrderStage::Build, OrderErrorCode::AmountConversion))?;
    let raw: i128 = raw
        .try_into()
        .map_err(|_| preflight(OrderStage::Build, OrderErrorCode::AmountConversion))?;
    Decimal::try_from_i128_with_scale(raw, 6)
        .map(|value| value.normalize())
        .map_err(|_| preflight(OrderStage::Build, OrderErrorCode::AmountConversion))
}

fn map_amounts(side: Side, making: u128, taking: u128) -> (u128, u128) {
    match side {
        Side::Buy => (taking, making),
        Side::Sell => (making, taking),
    }
}

fn classify_response(
    response: PostOrderResponse,
    expected_making: Decimal,
    expected_taking: Decimal,
    side: Side,
) -> Result<OrderReceipt, OrderSubmitError> {
    if !response.success {
        return Err(OrderSubmitError::Rejected {
            http_status: None,
            code: OrderErrorCode::ServerRejected,
        });
    }
    if response.order_id.trim().is_empty() {
        return Err(OrderSubmitError::Uncertain {
            code: OrderErrorCode::EmptyOrderId,
        });
    }
    if response.status != OrderStatusType::Matched {
        return Err(OrderSubmitError::Uncertain {
            code: OrderErrorCode::NonFinalStatus,
        });
    }
    if response.making_amount <= Decimal::ZERO
        || response.taking_amount <= Decimal::ZERO
        || response.making_amount != expected_making
        || response.taking_amount != expected_taking
    {
        return Err(OrderSubmitError::Uncertain {
            code: OrderErrorCode::AmountMismatch,
        });
    }
    let making =
        decimal_to_micros(response.making_amount).map_err(|_| OrderSubmitError::Uncertain {
            code: OrderErrorCode::AmountConversion,
        })?;
    let taking =
        decimal_to_micros(response.taking_amount).map_err(|_| OrderSubmitError::Uncertain {
            code: OrderErrorCode::AmountConversion,
        })?;
    let (filled_shares_micros, filled_usd_micros) = map_amounts(side, making, taking);
    Ok(OrderReceipt {
        order_id: response.order_id,
        filled_shares_micros,
        filled_usd_micros,
    })
}

fn classify_post_error(error: &SdkError) -> OrderSubmitError {
    if error.downcast_ref::<SdkEmptyResponse>().is_some() {
        return OrderSubmitError::Uncertain {
            code: OrderErrorCode::MalformedResponse,
        };
    }
    if let Some(status) = error.downcast_ref::<SdkStatus>() {
        if status.status_code.is_client_error() || status.status_code.is_server_error() {
            return OrderSubmitError::Rejected {
                http_status: Some(status.status_code.as_u16()),
                code: OrderErrorCode::HttpRejected,
            };
        }
        return OrderSubmitError::Uncertain {
            code: OrderErrorCode::PostTransport,
        };
    }
    let malformed = error
        .downcast_ref::<reqwest::Error>()
        .is_some_and(reqwest::Error::is_decode)
        || sdk_error_has_json_source(error);
    let code = if malformed {
        OrderErrorCode::MalformedResponse
    } else {
        OrderErrorCode::PostTransport
    };
    OrderSubmitError::Uncertain { code }
}

fn sdk_error_has_json_source(error: &SdkError) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = error
        .inner()
        .map(|inner| inner as &(dyn std::error::Error + 'static));
    while let Some(current) = source {
        if current.downcast_ref::<serde_json::Error>().is_some() {
            return true;
        }
        source = current.source();
    }
    false
}

#[async_trait]
impl OrderGateway for SdkOrderGateway {
    async fn submit_fok(&self, planned: &PlannedOrder) -> Result<OrderReceipt, OrderSubmitError> {
        let prepared = self.prepare_fok(planned).await?;
        let response =
            tokio::time::timeout(self.post_timeout, self.client.post_order(prepared.signed))
                .await
                .map_err(|_| OrderSubmitError::Uncertain {
                    code: OrderErrorCode::PostTimeout,
                })?
                .map_err(|error| classify_post_error(&error))?;
        classify_response(
            response,
            prepared.expected_making,
            prepared.expected_taking,
            prepared.side,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;
    use std::time::Duration;

    use alloy_sol_types::SolStruct as _;
    use alloy_sol_types_v1::{eip712_domain, SolStruct as _};
    use polymarket_client_sdk_v2::clob::types::response::PostOrderResponse;
    use polymarket_client_sdk_v2::clob::types::{
        OrderSignature, OrderStatusType, OrderType as SdkOrderType, OrderV2, Side as SdkSide,
    };
    use polymarket_client_sdk_v2::types::{address, b256, Decimal, B256};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    use crate::config::AppConfig;
    use crate::models::{OrderType, PlannedOrder, VenueId};

    use super::*;

    const PUBLIC_HARDHAT_KEY: &str =
        "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    const FIXTURE_SIGNER: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";
    const SERVER_BODY_SECRET_SENTINEL: &str = "SERVER_BODY_SECRET_SENTINEL";
    const MATCHED_RESPONSE: &str = r#"{
        "error_msg":"",
        "makingAmount":"19.5",
        "takingAmount":"39",
        "orderID":"0xabc",
        "status":"MATCHED",
        "success":true
    }"#;

    struct SafeCapturedRequest {
        line: String,
        poly_address: Option<String>,
        order_type: Option<String>,
        owner: Option<String>,
    }

    enum OrderServerResponse {
        Http {
            status: &'static str,
            body: &'static str,
        },
        Redirect,
        Disconnect,
        Withhold(Duration),
    }

    macro_rules! dec {
        ($value:literal) => {
            Decimal::from_str(stringify!($value)).unwrap()
        };
    }

    mod independent_v2 {
        alloy_sol_types_v1::sol! {
            #![sol(alloy_sol_types = ::alloy_sol_types_v1)]
            struct Order {
                uint256 salt;
                address maker;
                address signer;
                uint256 tokenId;
                uint256 makerAmount;
                uint256 takerAmount;
                uint8 side;
                uint8 signatureType;
                uint256 timestamp;
                bytes32 metadata;
                bytes32 builder;
            }
        }
    }

    mod independent_v08 {
        alloy_sol_types::sol! {
            struct Order {
                uint256 salt;
                address maker;
                address signer;
                uint256 tokenId;
                uint256 makerAmount;
                uint256 takerAmount;
                uint8 side;
                uint8 signatureType;
                uint256 timestamp;
                bytes32 metadata;
                bytes32 builder;
            }
        }
    }

    fn response(
        success: bool,
        status: OrderStatusType,
        order_id: &str,
        making_amount: Decimal,
        taking_amount: Decimal,
    ) -> PostOrderResponse {
        PostOrderResponse::builder()
            .success(success)
            .status(status)
            .order_id(order_id)
            .making_amount(making_amount)
            .taking_amount(taking_amount)
            .build()
    }

    #[test]
    fn exact_matched_buy_returns_actual_side_aware_receipt() {
        let response = response(
            true,
            OrderStatusType::Matched,
            "0xabc",
            dec!(19.5),
            dec!(39),
        );

        let receipt = classify_response(response, dec!(19.5), dec!(39), Side::Buy).unwrap();

        assert_eq!(receipt.order_id, "0xabc");
        assert_eq!(receipt.filled_shares_micros, 39_000_000);
        assert_eq!(receipt.filled_usd_micros, 19_500_000);
    }

    #[test]
    fn success_false_is_rejected_before_empty_amount_checks() {
        let error = classify_response(
            response(
                false,
                OrderStatusType::Canceled,
                "",
                Decimal::ZERO,
                Decimal::ZERO,
            ),
            dec!(19.5),
            dec!(39),
            Side::Buy,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            OrderSubmitError::Rejected {
                http_status: None,
                code: OrderErrorCode::ServerRejected,
            }
        ));
    }

    #[test]
    fn successful_non_final_statuses_are_uncertain() {
        for status in [
            OrderStatusType::Live,
            OrderStatusType::Delayed,
            OrderStatusType::Unmatched,
            OrderStatusType::Unknown("FUTURE_STATUS".to_owned()),
        ] {
            assert!(matches!(
                classify_response(
                    response(true, status, "0xabc", dec!(19.5), dec!(39)),
                    dec!(19.5),
                    dec!(39),
                    Side::Buy,
                ),
                Err(OrderSubmitError::Uncertain {
                    code: OrderErrorCode::NonFinalStatus,
                })
            ));
        }
    }

    #[test]
    fn successful_matched_requires_nonempty_id_and_exact_positive_amounts() {
        for response in [
            response(true, OrderStatusType::Matched, "  ", dec!(19.5), dec!(39)),
            response(
                true,
                OrderStatusType::Matched,
                "0xabc",
                Decimal::ZERO,
                dec!(39),
            ),
            response(
                true,
                OrderStatusType::Matched,
                "0xabc",
                dec!(19.4),
                dec!(39),
            ),
        ] {
            assert!(matches!(
                classify_response(response, dec!(19.5), dec!(39), Side::Buy),
                Err(OrderSubmitError::Uncertain { .. })
            ));
        }
    }

    #[test]
    fn exact_matched_sell_returns_actual_side_aware_receipt() {
        let receipt = classify_response(
            response(
                true,
                OrderStatusType::Matched,
                "0xsell",
                dec!(39),
                dec!(19.5),
            ),
            dec!(39),
            dec!(19.5),
            Side::Sell,
        )
        .unwrap();

        assert_eq!(receipt.order_id, "0xsell");
        assert_eq!(receipt.filled_shares_micros, 39_000_000);
        assert_eq!(receipt.filled_usd_micros, 19_500_000);
    }

    #[test]
    fn exact_matched_unrepresentable_amounts_are_uncertain() {
        for amount in [dec!(0.0000001), Decimal::MAX] {
            assert!(matches!(
                classify_response(
                    response(true, OrderStatusType::Matched, "0xabc", amount, dec!(1)),
                    amount,
                    dec!(1),
                    Side::Buy,
                ),
                Err(OrderSubmitError::Uncertain {
                    code: OrderErrorCode::AmountConversion,
                })
            ));
        }
    }

    async fn spawn_scripted_server(
        script: Vec<(&'static str, &'static str)>,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let mut requests = Vec::with_capacity(script.len());
            for (expected_request, body) in script {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_request_line(&mut stream).await;
                assert_eq!(request, expected_request, "unexpected loopback request");
                requests.push(request);
                write_json_response(&mut stream, "200 OK", body).await;
            }

            if let Ok(Ok((mut stream, _))) =
                tokio::time::timeout(Duration::from_millis(100), listener.accept()).await
            {
                let request = read_request_line(&mut stream).await;
                panic!("unexpected extra loopback request: {request}");
            }
            requests
        });
        (format!("http://{address}"), handle)
    }

    async fn read_request_line(stream: &mut tokio::net::TcpStream) -> String {
        let mut buffer = vec![0_u8; 16 * 1024];
        let count = stream.read(&mut buffer).await.unwrap();
        let raw = String::from_utf8(buffer[..count].to_vec()).unwrap();
        let mut parts = raw.lines().next().unwrap().split_whitespace();
        let method = parts.next().unwrap();
        let target = parts.next().unwrap();
        assert_eq!(parts.next(), Some("HTTP/1.1"));
        assert_eq!(parts.next(), None);
        format!("{method} {target}")
    }

    async fn write_json_response(stream: &mut tokio::net::TcpStream, status: &str, body: &str) {
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.shutdown().await.unwrap();
    }

    async fn write_keep_alive_json_response(stream: &mut tokio::net::TcpStream, body: &str) {
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    }

    async fn read_safe_request(stream: &mut tokio::net::TcpStream) -> SafeCapturedRequest {
        let mut bytes = Vec::new();
        let header_end = loop {
            if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break index;
            }
            let mut chunk = [0_u8; 4096];
            let count = stream.read(&mut chunk).await.unwrap();
            assert!(count > 0, "loopback request ended before its headers");
            bytes.extend_from_slice(&chunk[..count]);
        };
        let headers = std::str::from_utf8(&bytes[..header_end])
            .expect("loopback request headers must be UTF-8")
            .to_owned();
        let content_length = headers
            .lines()
            .skip(1)
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        let request_end = header_end + 4 + content_length;
        while bytes.len() < request_end {
            let mut chunk = [0_u8; 4096];
            let count = stream.read(&mut chunk).await.unwrap();
            assert!(count > 0, "loopback request ended before its body");
            bytes.extend_from_slice(&chunk[..count]);
        }

        let mut request_parts = headers.lines().next().unwrap().split_whitespace();
        let method = request_parts.next().unwrap();
        let target = request_parts.next().unwrap();
        assert_eq!(request_parts.next(), Some("HTTP/1.1"));
        assert_eq!(request_parts.next(), None);
        let poly_address = headers.lines().skip(1).find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("poly_address")
                .then(|| value.trim().to_owned())
        });
        let (order_type, owner) = if content_length == 0 {
            (None, None)
        } else {
            let body: serde_json::Value =
                serde_json::from_slice(&bytes[header_end + 4..request_end])
                    .expect("loopback request body must be valid JSON");
            (
                body.get("orderType")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
                body.get("owner")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
            )
        };

        SafeCapturedRequest {
            line: format!("{method} {target}"),
            poly_address,
            order_type,
            owner,
        }
    }

    async fn spawn_order_server(
        order_response: OrderServerResponse,
    ) -> (String, tokio::task::JoinHandle<Vec<SafeCapturedRequest>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let metadata = [
                (
                    "GET /tick-size?token_id=12345",
                    r#"{"minimum_tick_size":"0.01"}"#,
                ),
                ("GET /neg-risk?token_id=12345", r#"{"neg_risk":false}"#),
            ];
            let mut requests = Vec::with_capacity(4);
            for (expected_line, body) in metadata {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_safe_request(&mut stream).await;
                assert_eq!(request.line, expected_line);
                requests.push(request);
                write_json_response(&mut stream, "200 OK", body).await;
            }

            let (mut stream, _) = listener.accept().await.unwrap();
            let version_request = read_safe_request(&mut stream).await;
            assert_eq!(version_request.line, "GET /version");
            requests.push(version_request);
            write_keep_alive_json_response(&mut stream, r#"{"version":2}"#).await;
            requests.push(read_safe_request(&mut stream).await);
            let allow_redirect_probe = matches!(order_response, OrderServerResponse::Redirect);
            match order_response {
                OrderServerResponse::Http { status, body } => {
                    write_json_response(&mut stream, status, body).await;
                }
                OrderServerResponse::Redirect => {
                    let response = concat!(
                        "HTTP/1.1 307 Temporary Redirect\r\n",
                        "Location: /redirect-target\r\n",
                        "Content-Length: 0\r\n",
                        "Connection: close\r\n\r\n"
                    );
                    stream.write_all(response.as_bytes()).await.unwrap();
                    stream.shutdown().await.unwrap();
                }
                OrderServerResponse::Disconnect => {
                    stream.shutdown().await.unwrap();
                }
                OrderServerResponse::Withhold(duration) => {
                    tokio::time::sleep(duration).await;
                    stream.shutdown().await.unwrap();
                }
            }

            if let Ok(Ok((mut stream, _))) =
                tokio::time::timeout(Duration::from_millis(100), listener.accept()).await
            {
                let request = read_safe_request(&mut stream).await;
                if allow_redirect_probe {
                    requests.push(request);
                    write_json_response(&mut stream, "200 OK", MATCHED_RESPONSE).await;
                } else {
                    panic!("unexpected extra loopback request: {}", request.line);
                }
            }
            requests
        });
        (format!("http://{address}"), handle)
    }

    fn assert_order_request_contract(requests: &[SafeCapturedRequest]) {
        let lines: Vec<&str> = requests
            .iter()
            .map(|request| request.line.as_str())
            .collect();
        assert_eq!(
            lines,
            vec![
                "GET /tick-size?token_id=12345",
                "GET /neg-risk?token_id=12345",
                "GET /version",
                "POST /order",
            ]
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.line == "POST /order")
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| {
                    request.line.contains(" /auth/api-key")
                        || request.line.contains(" /auth/derive-api-key")
                })
                .count(),
            0
        );
        let post = requests
            .iter()
            .find(|request| request.line == "POST /order")
            .unwrap();
        assert_eq!(post.order_type.as_deref(), Some("FOK"));
        assert_eq!(
            post.owner.as_deref(),
            Some("00000000-0000-0000-0000-000000000000")
        );
        assert_eq!(post.poly_address.as_deref(), Some(FIXTURE_SIGNER));
    }

    fn fixture_config() -> AppConfig {
        let mut cfg: AppConfig = serde_json::from_str(include_str!("../../config.json")).unwrap();
        cfg.credentials.private_key = PUBLIC_HARDHAT_KEY.to_owned();
        cfg.credentials.funder_address = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266".to_owned();
        cfg.credentials.signature_type = Some(0);
        cfg.credentials.api_key = Some("00000000-0000-0000-0000-000000000000".to_owned());
        cfg.credentials.api_secret =
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_owned());
        cfg.credentials.api_passphrase = Some("fixture-passphrase".to_owned());
        cfg
    }

    fn planned_order(neg_risk: bool) -> PlannedOrder {
        PlannedOrder {
            venue: VenueId::Polymarket,
            token_id: "12345".to_owned(),
            neg_risk,
            side: Side::Buy,
            shares: 39.0,
            limit_price: 0.505,
            usd_notional: 19.695,
            order_type: OrderType::Fok,
            source_trade_hash: None,
        }
    }

    fn fixed_sdk_v2_order() -> OrderV2 {
        let mut order = OrderV2::default();
        order.salt = U256::from(1);
        order.maker = address!("1111111111111111111111111111111111111111");
        order.signer = address!("1111111111111111111111111111111111111111");
        order.tokenId = U256::from(12_345);
        order.makerAmount = U256::from(19_500_000);
        order.takerAmount = U256::from(39_000_000);
        order.side = 0;
        order.signatureType = 0;
        order.timestamp = U256::from(1_700_000_000_000_u64);
        order.metadata = B256::ZERO;
        order.builder = B256::ZERO;
        order
    }

    fn independent_v2_order() -> independent_v2::Order {
        independent_v2::Order {
            salt: U256::from(1),
            maker: address!("1111111111111111111111111111111111111111"),
            signer: address!("1111111111111111111111111111111111111111"),
            tokenId: U256::from(12_345),
            makerAmount: U256::from(19_500_000),
            takerAmount: U256::from(39_000_000),
            side: 0,
            signatureType: 0,
            timestamp: U256::from(1_700_000_000_000_u64),
            metadata: B256::ZERO,
            builder: B256::ZERO,
        }
    }

    fn independent_v08_order() -> independent_v08::Order {
        independent_v08::Order {
            salt: alloy_primitives::U256::from(1),
            maker: alloy_primitives::address!("1111111111111111111111111111111111111111"),
            signer: alloy_primitives::address!("1111111111111111111111111111111111111111"),
            tokenId: alloy_primitives::U256::from(12_345),
            makerAmount: alloy_primitives::U256::from(19_500_000),
            takerAmount: alloy_primitives::U256::from(39_000_000),
            side: 0,
            signatureType: 0,
            timestamp: alloy_primitives::U256::from(1_700_000_000_000_u64),
            metadata: alloy_primitives::B256::ZERO,
            builder: alloy_primitives::B256::ZERO,
        }
    }

    // Official proof sources:
    // https://github.com/Polymarket/ctf-exchange-v2/blob/main/src/exchange/mixins/Hashing.sol
    // https://github.com/Polymarket/ctf-exchange-v2/blob/main/src/exchange/libraries/Structs.sol
    #[test]
    fn v2_order_hash_matches_official_contract_algorithm() {
        let order = fixed_sdk_v2_order();
        let domain = eip712_domain! {
            name: "Polymarket CTF Exchange",
            version: "2",
            chain_id: 137,
            verifying_contract: address!("E111180000d2663C0091e4f400237545B87B996B"),
        };
        let expected = independent_v2_order().eip712_signing_hash(&domain);
        let actual = order.eip712_signing_hash(&domain);
        let domain_v08 = alloy_sol_types::eip712_domain! {
            name: "Polymarket CTF Exchange",
            version: "2",
            chain_id: 137,
            verifying_contract: alloy_primitives::address!(
                "E111180000d2663C0091e4f400237545B87B996B"
            ),
        };
        let alloy_v08 = independent_v08_order().eip712_signing_hash(&domain_v08);

        assert_eq!(actual, expected);
        assert_eq!(actual.as_slice(), alloy_v08.as_slice());
        assert_eq!(
            actual,
            b256!("dee0837cae29a8c41bd52f1f614e7e163739ff5ae52343da8f0501189c02e020")
        );
        assert_eq!(
            alloy_v08,
            alloy_primitives::b256!(
                "dee0837cae29a8c41bd52f1f614e7e163739ff5ae52343da8f0501189c02e020"
            )
        );
    }

    #[test]
    fn v2_order_hash_changes_for_every_identity_field() {
        fn mutated(
            field: &'static str,
            mutate: impl FnOnce(&mut OrderV2),
        ) -> (&'static str, OrderV2) {
            let mut order = fixed_sdk_v2_order();
            mutate(&mut order);
            (field, order)
        }

        let domain = eip712_domain! {
            name: "Polymarket CTF Exchange",
            version: "2",
            chain_id: 137,
            verifying_contract: address!("E111180000d2663C0091e4f400237545B87B996B"),
        };
        let baseline = fixed_sdk_v2_order().eip712_signing_hash(&domain);
        let mutations = vec![
            mutated("salt", |order| order.salt = U256::from(2)),
            mutated("maker", |order| {
                order.maker = address!("2222222222222222222222222222222222222222")
            }),
            mutated("signer", |order| {
                order.signer = address!("3333333333333333333333333333333333333333")
            }),
            mutated("tokenId", |order| order.tokenId = U256::from(12_346)),
            mutated("makerAmount", |order| {
                order.makerAmount = U256::from(19_500_001)
            }),
            mutated("takerAmount", |order| {
                order.takerAmount = U256::from(39_000_001)
            }),
            mutated("side", |order| order.side = 1),
            mutated("signatureType", |order| order.signatureType = 1),
            mutated("timestamp", |order| {
                order.timestamp = U256::from(1_700_000_000_001_u64)
            }),
            mutated("metadata", |order| {
                order.metadata =
                    b256!("0000000000000000000000000000000000000000000000000000000000000001")
            }),
            mutated("builder", |order| {
                order.builder =
                    b256!("0000000000000000000000000000000000000000000000000000000000000002")
            }),
        ];

        for (field, order) in mutations {
            assert_ne!(
                order.eip712_signing_hash(&domain),
                baseline,
                "mutating {field} must change the V2 order hash"
            );
        }
    }

    #[tokio::test]
    async fn v2_order_hash_is_signature_independent_for_signed_loopback_order() {
        let (host, server) = spawn_scripted_server(vec![
            (
                "GET /tick-size?token_id=12345",
                r#"{"minimum_tick_size":"0.01"}"#,
            ),
            ("GET /neg-risk?token_id=12345", r#"{"neg_risk":false}"#),
            ("GET /version", r#"{"version":2}"#),
        ])
        .await;
        let cfg = fixture_config();
        let gateway = SdkOrderGateway::new_with_host(&cfg, &host, Duration::from_secs(1))
            .await
            .unwrap();
        let mut prepared = gateway.prepare_fok(&planned_order(false)).await.unwrap();
        let exchange = contract_config(POLYGON, false)
            .unwrap()
            .exchange_v2
            .unwrap();
        let domain = eip712_domain! {
            name: "Polymarket CTF Exchange",
            version: "2",
            chain_id: POLYGON,
            verifying_contract: exchange,
        };

        let expected = prepared.signed.order().eip712_signing_hash(&domain);
        let before = prepared.signed.v2_order_hash(&domain).unwrap();
        prepared.signed.signature = OrderSignature::Wrapped("0x00".to_owned());
        let after = prepared.signed.v2_order_hash(&domain).unwrap();
        let requests = server.await.unwrap();

        assert!(before == expected);
        assert!(after == expected);
        assert_eq!(
            requests,
            vec![
                "GET /tick-size?token_id=12345",
                "GET /neg-risk?token_id=12345",
                "GET /version",
            ]
        );
    }

    #[tokio::test]
    async fn v2_order_id_uses_configured_exchange_and_canonical_lowercase_hex() {
        let (host, server) = spawn_scripted_server(vec![
            (
                "GET /tick-size?token_id=12345",
                r#"{"minimum_tick_size":"0.01"}"#,
            ),
            ("GET /neg-risk?token_id=12345", r#"{"neg_risk":false}"#),
            ("GET /version", r#"{"version":2}"#),
        ])
        .await;
        let cfg = fixture_config();
        let gateway = SdkOrderGateway::new_with_host(&cfg, &host, Duration::from_secs(1))
            .await
            .unwrap();
        let prepared = gateway.prepare_fok(&planned_order(false)).await.unwrap();

        for neg_risk in [false, true] {
            let id = exact_v2_order_id(&prepared.signed, neg_risk).unwrap();
            let exchange = contract_config(POLYGON, neg_risk)
                .unwrap()
                .exchange_v2
                .unwrap();
            let domain = eip712_domain! {
                name: "Polymarket CTF Exchange",
                version: "2",
                chain_id: POLYGON,
                verifying_contract: exchange,
            };
            let expected = prepared.signed.order().eip712_signing_hash(&domain);
            let decoded = hex::decode(&id[2..]).unwrap();

            assert_eq!(id.len(), 66);
            assert!(id.starts_with("0x"));
            assert!(id[2..]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
            assert!(decoded.as_slice() == expected.as_slice());
        }

        assert_eq!(
            server.await.unwrap(),
            vec![
                "GET /tick-size?token_id=12345",
                "GET /neg-risk?token_id=12345",
                "GET /version",
            ]
        );
    }

    #[tokio::test]
    async fn exact_matched_post_returns_receipt_after_one_request() {
        let (host, server) = spawn_order_server(OrderServerResponse::Http {
            status: "200 OK",
            body: MATCHED_RESPONSE,
        })
        .await;
        let cfg = fixture_config();
        let gateway = SdkOrderGateway::new_with_host(&cfg, &host, Duration::from_secs(1))
            .await
            .unwrap();

        let receipt = gateway.submit_fok(&planned_order(false)).await.unwrap();
        let requests = server.await.unwrap();

        assert_eq!(receipt.order_id, "0xabc");
        assert_eq!(receipt.filled_shares_micros, 39_000_000);
        assert_eq!(receipt.filled_usd_micros, 19_500_000);
        assert_order_request_contract(&requests);
    }

    #[tokio::test]
    async fn success_false_post_is_rejected_after_one_request() {
        let (host, server) = spawn_order_server(OrderServerResponse::Http {
            status: "200 OK",
            body: r#"{
                "error_msg":"rejected",
                "makingAmount":"",
                "takingAmount":"",
                "orderID":"",
                "status":"CANCELED",
                "success":false
            }"#,
        })
        .await;
        let cfg = fixture_config();
        let gateway = SdkOrderGateway::new_with_host(&cfg, &host, Duration::from_secs(1))
            .await
            .unwrap();

        let error = gateway.submit_fok(&planned_order(false)).await.unwrap_err();
        let requests = server.await.unwrap();

        assert!(matches!(
            error,
            OrderSubmitError::Rejected {
                http_status: None,
                code: OrderErrorCode::ServerRejected,
            }
        ));
        assert_order_request_contract(&requests);
    }

    #[tokio::test]
    async fn definitive_http_statuses_are_rejected_without_rendering_body() {
        for (status_line, status_code) in [
            ("400 Bad Request", 400),
            ("409 Conflict", 409),
            ("429 Too Many Requests", 429),
            ("500 Internal Server Error", 500),
        ] {
            let (host, server) = spawn_order_server(OrderServerResponse::Http {
                status: status_line,
                body: SERVER_BODY_SECRET_SENTINEL,
            })
            .await;
            let cfg = fixture_config();
            let gateway = SdkOrderGateway::new_with_host(&cfg, &host, Duration::from_secs(1))
                .await
                .unwrap();

            let error = gateway.submit_fok(&planned_order(false)).await.unwrap_err();
            let requests = server.await.unwrap();
            let rendered = format!("{error:?} {error}");

            assert!(matches!(
                error,
                OrderSubmitError::Rejected {
                    http_status: Some(actual),
                    code: OrderErrorCode::HttpRejected,
                } if actual == status_code
            ));
            assert!(!rendered.contains(SERVER_BODY_SECRET_SENTINEL));
            assert_order_request_contract(&requests);
        }
    }

    #[test]
    fn only_client_and_server_status_classes_are_definitive_rejections() {
        use polymarket_client_sdk_v2::error::{Method, StatusCode};

        for status in [100_u16, 199, 300, 399, 600, 799] {
            let sdk_error = SdkError::status(
                StatusCode::from_u16(status).unwrap(),
                Method::POST,
                "/order".to_owned(),
                "STATUS_BODY_SECRET_SENTINEL",
            );

            assert_eq!(
                classify_post_error(&sdk_error),
                OrderSubmitError::Uncertain {
                    code: OrderErrorCode::PostTransport,
                },
                "status {status} does not prove rejection"
            );
        }

        for status in [400_u16, 499, 500, 599] {
            let sdk_error = SdkError::status(
                StatusCode::from_u16(status).unwrap(),
                Method::POST,
                "/order".to_owned(),
                "STATUS_BODY_SECRET_SENTINEL",
            );

            assert_eq!(
                classify_post_error(&sdk_error),
                OrderSubmitError::Rejected {
                    http_status: Some(status),
                    code: OrderErrorCode::HttpRejected,
                },
                "status {status} is a definitive rejection"
            );
        }
    }

    #[tokio::test]
    async fn malformed_success_response_is_uncertain_after_one_request() {
        let (host, server) = spawn_order_server(OrderServerResponse::Http {
            status: "200 OK",
            body: "{",
        })
        .await;
        let cfg = fixture_config();
        let gateway = SdkOrderGateway::new_with_host(&cfg, &host, Duration::from_secs(1))
            .await
            .unwrap();

        let error = gateway.submit_fok(&planned_order(false)).await.unwrap_err();
        let requests = server.await.unwrap();

        assert_eq!(
            error,
            OrderSubmitError::Uncertain {
                code: OrderErrorCode::MalformedResponse,
            }
        );
        assert_order_request_contract(&requests);
    }

    #[tokio::test]
    async fn successful_null_response_is_uncertain_without_fabricating_http_404() {
        let (host, server) = spawn_order_server(OrderServerResponse::Http {
            status: "200 OK",
            body: "null",
        })
        .await;
        let cfg = fixture_config();
        let gateway = SdkOrderGateway::new_with_host(&cfg, &host, Duration::from_secs(1))
            .await
            .unwrap();

        let result = gateway.submit_fok(&planned_order(false)).await;
        let requests = server.await.unwrap();
        let error = result.unwrap_err();

        assert_eq!(
            error,
            OrderSubmitError::Uncertain {
                code: OrderErrorCode::MalformedResponse,
            }
        );
        assert_order_request_contract(&requests);
    }

    #[tokio::test]
    async fn redirect_response_is_not_followed_or_replayed() {
        let (host, server) = spawn_order_server(OrderServerResponse::Redirect).await;
        let cfg = fixture_config();
        let gateway = SdkOrderGateway::new_with_host(&cfg, &host, Duration::from_secs(1))
            .await
            .unwrap();

        let result = gateway.submit_fok(&planned_order(false)).await;
        let requests = server.await.unwrap();
        let error = result.unwrap_err();

        assert_eq!(
            error,
            OrderSubmitError::Uncertain {
                code: OrderErrorCode::PostTransport,
            }
        );
        assert_order_request_contract(&requests);
    }

    #[tokio::test]
    async fn disconnect_after_post_bytes_is_uncertain_without_retry() {
        let (host, server) = spawn_order_server(OrderServerResponse::Disconnect).await;
        let cfg = fixture_config();
        let gateway = SdkOrderGateway::new_with_host(&cfg, &host, Duration::from_secs(1))
            .await
            .unwrap();

        let error = gateway.submit_fok(&planned_order(false)).await.unwrap_err();
        let requests = server.await.unwrap();

        assert!(matches!(
            error,
            OrderSubmitError::Uncertain {
                code: OrderErrorCode::PostTransport,
            }
        ));
        assert_order_request_contract(&requests);
    }

    #[tokio::test]
    async fn withheld_post_response_times_out_without_retry() {
        let (host, server) =
            spawn_order_server(OrderServerResponse::Withhold(Duration::from_millis(100))).await;
        let cfg = fixture_config();
        let gateway = SdkOrderGateway::new_with_host(&cfg, &host, Duration::from_millis(25))
            .await
            .unwrap();

        let error = gateway.submit_fok(&planned_order(false)).await.unwrap_err();
        let requests = server.await.unwrap();

        assert!(matches!(
            error,
            OrderSubmitError::Uncertain {
                code: OrderErrorCode::PostTimeout,
            }
        ));
        assert_order_request_contract(&requests);
    }

    #[tokio::test]
    async fn successful_nonfinal_or_mismatched_posts_are_uncertain_without_retry() {
        for body in [
            r#"{
                "error_msg":"",
                "makingAmount":"19.5",
                "takingAmount":"39",
                "orderID":"0xabc",
                "status":"LIVE",
                "success":true
            }"#,
            r#"{
                "error_msg":"",
                "makingAmount":"19.5",
                "takingAmount":"39",
                "orderID":"0xabc",
                "status":"DELAYED",
                "success":true
            }"#,
            r#"{
                "error_msg":"",
                "makingAmount":"19.4",
                "takingAmount":"39",
                "orderID":"0xabc",
                "status":"MATCHED",
                "success":true
            }"#,
        ] {
            let (host, server) = spawn_order_server(OrderServerResponse::Http {
                status: "200 OK",
                body,
            })
            .await;
            let cfg = fixture_config();
            let gateway = SdkOrderGateway::new_with_host(&cfg, &host, Duration::from_secs(1))
                .await
                .unwrap();

            let error = gateway.submit_fok(&planned_order(false)).await.unwrap_err();
            let requests = server.await.unwrap();

            assert!(matches!(error, OrderSubmitError::Uncertain { .. }));
            assert_order_request_contract(&requests);
        }
    }

    #[test]
    fn buy_rounds_down_and_sell_rounds_up_without_worsening_limit() {
        let tick = dec!(0.01);
        assert_eq!(
            align_price(dec!(0.505), tick, Side::Buy).unwrap(),
            dec!(0.50)
        );
        assert_eq!(
            align_price(dec!(0.505), tick, Side::Sell).unwrap(),
            dec!(0.51)
        );
        assert_eq!(
            align_price(dec!(0.50), tick, Side::Buy).unwrap(),
            dec!(0.50)
        );
        assert_eq!(
            align_price(dec!(0.50), tick, Side::Sell).unwrap(),
            dec!(0.50)
        );
    }

    #[test]
    fn tick_must_be_strictly_between_zero_and_one() {
        for tick in [Decimal::ZERO, Decimal::ONE, dec!(1.01)] {
            assert!(matches!(
                align_price(dec!(0.5), tick, Side::Buy),
                Err(OrderSubmitError::Preflight {
                    stage: OrderStage::Metadata,
                    code: OrderErrorCode::InvalidTickSize,
                })
            ));
        }
    }

    #[test]
    fn non_divisor_tick_rounding_remains_side_aware() {
        let tick = dec!(0.03);
        assert_eq!(
            align_price(dec!(0.50), tick, Side::Buy).unwrap(),
            dec!(0.48)
        );
        assert_eq!(
            align_price(dec!(0.50), tick, Side::Sell).unwrap(),
            dec!(0.51)
        );
    }

    #[test]
    fn aligned_price_must_stay_inside_inclusive_sdk_tick_bounds() {
        let cent = dec!(0.01);
        assert_eq!(align_price(dec!(0.005), cent, Side::Sell).unwrap(), cent);
        assert!(matches!(
            align_price(dec!(0.005), cent, Side::Buy),
            Err(OrderSubmitError::Preflight {
                stage: OrderStage::Build,
                code: OrderErrorCode::InvalidPrice,
            })
        ));
        assert_eq!(
            align_price(dec!(0.995), cent, Side::Buy).unwrap(),
            dec!(0.99)
        );
        assert!(matches!(
            align_price(dec!(0.995), cent, Side::Sell),
            Err(OrderSubmitError::Preflight {
                stage: OrderStage::Build,
                code: OrderErrorCode::InvalidPrice,
            })
        ));
        assert_eq!(align_price(cent, cent, Side::Buy).unwrap(), cent);
        assert_eq!(
            align_price(dec!(0.99), cent, Side::Sell).unwrap(),
            dec!(0.99)
        );

        let non_divisor_tick = dec!(0.03);
        assert_eq!(
            align_price(dec!(0.98), non_divisor_tick, Side::Buy).unwrap(),
            dec!(0.96)
        );
        assert!(matches!(
            align_price(dec!(0.98), non_divisor_tick, Side::Sell),
            Err(OrderSubmitError::Preflight {
                stage: OrderStage::Build,
                code: OrderErrorCode::InvalidPrice,
            })
        ));
    }

    #[test]
    fn invalid_tick_price_size_and_token_are_preflight_errors() {
        assert!(matches!(
            align_price(dec!(0.5), Decimal::ZERO, Side::Buy),
            Err(OrderSubmitError::Preflight {
                code: OrderErrorCode::InvalidTickSize,
                ..
            })
        ));
        assert!(decimal_from_f64(f64::NAN, OrderErrorCode::InvalidPrice).is_err());
        assert!(parse_token_id("not-a-u256").is_err());
    }

    #[test]
    fn side_aware_amount_mapping_is_exact() {
        assert_eq!(
            map_amounts(Side::Buy, 20_000_000, 40_000_000),
            (40_000_000, 20_000_000)
        );
        assert_eq!(
            map_amounts(Side::Sell, 40_000_000, 20_000_000),
            (40_000_000, 20_000_000)
        );
    }

    #[test]
    fn huge_decimal_to_micros_is_amount_conversion_error() {
        assert!(matches!(
            decimal_to_micros(Decimal::MAX),
            Err(OrderSubmitError::Preflight {
                stage: OrderStage::Response,
                code: OrderErrorCode::AmountConversion,
            })
        ));
    }

    #[test]
    fn decimal_incompatible_u256_is_amount_conversion_error_without_panic() {
        assert!(matches!(
            u256_micros_to_decimal(U256::from(1_u128 << 96)),
            Err(OrderSubmitError::Preflight {
                stage: OrderStage::Build,
                code: OrderErrorCode::AmountConversion,
            })
        ));
    }

    #[test]
    fn u256_over_i128_is_amount_conversion_error() {
        assert!(matches!(
            u256_micros_to_decimal(U256::MAX),
            Err(OrderSubmitError::Preflight {
                stage: OrderStage::Build,
                code: OrderErrorCode::AmountConversion,
            })
        ));
    }

    #[test]
    fn normal_micro_conversions_remain_exact() {
        assert_eq!(decimal_to_micros(dec!(19.5)).unwrap(), 19_500_000);
        assert_eq!(
            u256_micros_to_decimal(U256::from(19_500_000)).unwrap(),
            dec!(19.5)
        );
    }

    #[tokio::test]
    async fn invalid_size_boundaries_stop_before_sdk_build_or_sign() {
        for shares in [0.0, -1.0, 1.001, 1.000_000_1] {
            let (host, server) = spawn_scripted_server(vec![
                (
                    "GET /tick-size?token_id=12345",
                    r#"{"minimum_tick_size":"0.01"}"#,
                ),
                ("GET /neg-risk?token_id=12345", r#"{"neg_risk":false}"#),
            ])
            .await;
            let cfg = fixture_config();
            let gateway = SdkOrderGateway::new_with_host(&cfg, &host, Duration::from_secs(1))
                .await
                .unwrap();
            let mut planned = planned_order(false);
            planned.shares = shares;

            let error = gateway.prepare_fok(&planned).await.unwrap_err();
            let requests = server.await.unwrap();

            assert!(matches!(
                error,
                OrderSubmitError::Preflight {
                    stage: OrderStage::Build,
                    code: OrderErrorCode::InvalidSize,
                }
            ));
            assert_eq!(requests.len(), 2);
            assert!(!requests.iter().any(|line| line.contains(" /version")));
        }
    }

    #[tokio::test]
    async fn official_sdk_builds_and_signs_v2_eoa_fok_on_loopback() {
        let (host, server) = spawn_scripted_server(vec![
            (
                "GET /tick-size?token_id=12345",
                r#"{"minimum_tick_size":"0.01"}"#,
            ),
            ("GET /neg-risk?token_id=12345", r#"{"neg_risk":false}"#),
            ("GET /version", r#"{"version":2}"#),
        ])
        .await;
        let cfg = fixture_config();
        let gateway = SdkOrderGateway::new_with_host(&cfg, &host, Duration::from_secs(1))
            .await
            .unwrap();

        let prepared = gateway.prepare_fok(&planned_order(false)).await.unwrap();
        let requests = server.await.unwrap();
        let l1_auth_request_count = requests
            .iter()
            .filter(|line| line.contains(" /auth/"))
            .count();

        assert_eq!(prepared.signed.order_type, SdkOrderType::FOK);
        assert_eq!(prepared.signed.payload.version(), 2);
        assert_eq!(prepared.signed.order().tokenId, U256::from(12_345));
        assert_eq!(prepared.signed.order().side, SdkSide::Buy as u8);
        assert_eq!(prepared.expected_making, dec!(19.5));
        assert_eq!(prepared.expected_taking, dec!(39));
        assert_eq!(
            requests,
            vec![
                "GET /tick-size?token_id=12345",
                "GET /neg-risk?token_id=12345",
                "GET /version",
            ]
        );
        assert_eq!(l1_auth_request_count, 0);

        let exchange = contract_config(POLYGON, false)
            .unwrap()
            .exchange_v2
            .unwrap();
        let domain = eip712_domain! {
            name: "Polymarket CTF Exchange",
            version: "2",
            chain_id: POLYGON,
            verifying_contract: exchange,
        };
        let digest = prepared.signed.order().eip712_signing_hash(&domain);
        let signature = match &prepared.signed.signature {
            OrderSignature::Ecdsa(signature) => signature,
            OrderSignature::Wrapped(_) => panic!("EOA order must use ECDSA"),
            _ => panic!("unsupported future signature type for EOA test"),
        };
        assert_eq!(
            signature.recover_address_from_prehash(&digest).unwrap(),
            gateway.signer.address()
        );
    }

    #[tokio::test]
    async fn neg_risk_sdk_builds_and_signs_against_v2_exchange() {
        let (host, server) = spawn_scripted_server(vec![
            (
                "GET /tick-size?token_id=12345",
                r#"{"minimum_tick_size":"0.01"}"#,
            ),
            ("GET /neg-risk?token_id=12345", r#"{"neg_risk":true}"#),
            ("GET /version", r#"{"version":2}"#),
        ])
        .await;
        let cfg = fixture_config();
        let gateway = SdkOrderGateway::new_with_host(&cfg, &host, Duration::from_secs(1))
            .await
            .unwrap();

        let prepared = gateway.prepare_fok(&planned_order(true)).await.unwrap();
        let requests = server.await.unwrap();

        assert_eq!(prepared.signed.order_type, SdkOrderType::FOK);
        assert_eq!(prepared.signed.payload.version(), 2);
        assert_eq!(
            requests,
            vec![
                "GET /tick-size?token_id=12345",
                "GET /neg-risk?token_id=12345",
                "GET /version",
            ]
        );
        assert_eq!(
            requests
                .iter()
                .filter(|line| line.contains(" /auth/"))
                .count(),
            0
        );

        let exchange = contract_config(POLYGON, true).unwrap().exchange_v2.unwrap();
        let domain = eip712_domain! {
            name: "Polymarket CTF Exchange",
            version: "2",
            chain_id: POLYGON,
            verifying_contract: exchange,
        };
        let digest = prepared.signed.order().eip712_signing_hash(&domain);
        let signature = match &prepared.signed.signature {
            OrderSignature::Ecdsa(signature) => signature,
            OrderSignature::Wrapped(_) => panic!("EOA order must use ECDSA"),
            _ => panic!("unsupported future signature type for EOA test"),
        };
        assert_eq!(
            signature.recover_address_from_prehash(&digest).unwrap(),
            gateway.signer.address()
        );
    }

    #[tokio::test]
    async fn sell_sdk_order_preserves_side_and_exact_amount_mapping() {
        let (host, server) = spawn_scripted_server(vec![
            (
                "GET /tick-size?token_id=12345",
                r#"{"minimum_tick_size":"0.01"}"#,
            ),
            ("GET /neg-risk?token_id=12345", r#"{"neg_risk":false}"#),
            ("GET /version", r#"{"version":2}"#),
        ])
        .await;
        let cfg = fixture_config();
        let gateway = SdkOrderGateway::new_with_host(&cfg, &host, Duration::from_secs(1))
            .await
            .unwrap();
        let mut planned = planned_order(false);
        planned.side = Side::Sell;

        let prepared = gateway.prepare_fok(&planned).await.unwrap();
        let requests = server.await.unwrap();
        let order = prepared.signed.order();

        assert_eq!(order.side, SdkSide::Sell as u8);
        assert_eq!(order.makerAmount, U256::from(39_000_000));
        assert_eq!(order.takerAmount, U256::from(19_890_000));
        assert_eq!(prepared.expected_making, dec!(39));
        assert_eq!(prepared.expected_taking, dec!(19.89));
        assert_eq!(prepared.side, Side::Sell);
        assert_eq!(
            map_amounts(
                prepared.side,
                decimal_to_micros(prepared.expected_making).unwrap(),
                decimal_to_micros(prepared.expected_taking).unwrap(),
            ),
            (39_000_000, 19_890_000)
        );
        assert_eq!(
            requests,
            vec![
                "GET /tick-size?token_id=12345",
                "GET /neg-risk?token_id=12345",
                "GET /version",
            ]
        );
    }

    #[tokio::test]
    async fn production_new_rejects_nonofficial_host_before_network() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut cfg = fixture_config();
        cfg.site.clob_api_base = format!("http://{}", listener.local_addr().unwrap());

        let error = match SdkOrderGateway::new(&cfg).await {
            Err(error) => error,
            Ok(_) => panic!("nonofficial production host must be rejected"),
        };

        assert!(matches!(
            error,
            OrderSubmitError::Preflight {
                stage: OrderStage::Initialization,
                code: OrderErrorCode::InvalidHost,
            }
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn prepared_order_debug_redacts_signed_order() {
        let (host, server) = spawn_scripted_server(vec![
            (
                "GET /tick-size?token_id=12345",
                r#"{"minimum_tick_size":"0.01"}"#,
            ),
            ("GET /neg-risk?token_id=12345", r#"{"neg_risk":false}"#),
            ("GET /version", r#"{"version":2}"#),
        ])
        .await;
        let cfg = fixture_config();
        let gateway = SdkOrderGateway::new_with_host(&cfg, &host, Duration::from_secs(1))
            .await
            .unwrap();

        let prepared = gateway.prepare_fok(&planned_order(false)).await.unwrap();
        let debug = format!("{prepared:?}");
        server.await.unwrap();

        assert!(
            debug
                == "PreparedOrder { signed: \"<redacted>\", expected_making: 19.5, expected_taking: 39, side: Buy }"
        );
    }

    #[tokio::test]
    async fn neg_risk_mismatch_stops_before_build() {
        let (host, server) = spawn_scripted_server(vec![
            (
                "GET /tick-size?token_id=12345",
                r#"{"minimum_tick_size":"0.01"}"#,
            ),
            ("GET /neg-risk?token_id=12345", r#"{"neg_risk":true}"#),
        ])
        .await;
        let cfg = fixture_config();
        let gateway = SdkOrderGateway::new_with_host(&cfg, &host, Duration::from_secs(1))
            .await
            .unwrap();

        let error = gateway
            .prepare_fok(&planned_order(false))
            .await
            .unwrap_err();
        let requests = server.await.unwrap();

        assert!(matches!(
            error,
            OrderSubmitError::Preflight {
                stage: OrderStage::Metadata,
                code: OrderErrorCode::NegRiskMismatch,
            }
        ));
        assert_eq!(
            requests,
            vec![
                "GET /tick-size?token_id=12345",
                "GET /neg-risk?token_id=12345",
            ]
        );
        assert!(!requests.iter().any(|line| line.contains(" /version")));
        assert_eq!(
            requests
                .iter()
                .filter(|line| line.contains(" /auth/"))
                .count(),
            0
        );
    }
}
