#![allow(
    dead_code,
    reason = "Task 5 prepares the isolated adapter; later migration tasks wire submission"
)]

use std::fmt;
use std::str::FromStr as _;
use std::time::Duration;

use alloy_signer_local::PrivateKeySigner;
use polymarket_client_sdk_v2::auth::{
    state::Authenticated, Credentials, Normal, Signer as _, Uuid,
};
use polymarket_client_sdk_v2::clob::types::{
    OrderType as SdkOrderType, Side as SdkSide, SignedOrder as SdkSignedOrder,
};
use polymarket_client_sdk_v2::clob::{Client, Config as SdkConfig};
use polymarket_client_sdk_v2::types::{Address, Decimal, U256};
use polymarket_client_sdk_v2::POLYGON;

use crate::config::{AppConfig, OFFICIAL_CLOB_V2_HOST};
use crate::models::{OrderType, PlannedOrder, Side};
use crate::service::order_gateway::{OrderErrorCode, OrderStage, OrderSubmitError};

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

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;
    use std::time::Duration;

    use alloy_sol_types_v1::{eip712_domain, SolStruct as _};
    use polymarket_client_sdk_v2::clob::types::{
        OrderSignature, OrderType as SdkOrderType, Side as SdkSide,
    };
    use polymarket_client_sdk_v2::contract_config;
    use polymarket_client_sdk_v2::types::Decimal;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    use crate::config::AppConfig;
    use crate::models::{OrderType, PlannedOrder, VenueId};

    use super::*;

    const PUBLIC_HARDHAT_KEY: &str =
        "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

    macro_rules! dec {
        ($value:literal) => {
            Decimal::from_str(stringify!($value)).unwrap()
        };
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
