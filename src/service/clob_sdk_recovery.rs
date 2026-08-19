#![allow(
    dead_code,
    reason = "Task 9 supplies a sealed adapter that Task 10 will construct only for explicit recovery commands"
)]

use std::{str::FromStr as _, time::Duration};

use alloy_signer_local::PrivateKeySigner;
use async_trait::async_trait;
use polymarket_client_sdk_v2::{
    auth::{state::Authenticated, state::Unauthenticated, Credentials, Normal, Signer as _, Uuid},
    clob::{
        types::{
            request::TradesRequest,
            response::{CancelOrdersResponse, OpenOrderResponse, TradeResponse},
            OrderStatusType, OrderType as SdkOrderType, Side as SdkSide, TradeStatusType,
            TraderSide,
        },
        Client, Config as SdkConfig,
    },
    error::{EmptyResponse, Error as SdkError, Status as SdkStatus},
    types::{Address, Decimal},
    POLYGON,
};

use crate::{
    config::{AppConfig, OFFICIAL_CLOB_V2_HOST},
    service::{
        execution_ledger::{OrderSide, OrderType, ReconcileUncertainCode, Venue},
        order_gateway::PreparedOrderIdentity,
        recovery_gateway::{
            CancelAttemptEvidence, CancelUncertainCode, RecoveryError, RecoveryGateway,
            RemoteOrderEvidence, TradeId,
        },
    },
};

type AuthenticatedClient = Client<Authenticated<Normal>>;

pub(crate) struct SdkRecoveryGateway {
    client: AuthenticatedClient,
    request_timeout: Duration,
}

impl SdkRecoveryGateway {
    pub(crate) async fn new(cfg: &AppConfig) -> Result<Self, RecoveryError> {
        if cfg.site.clob_api_base != OFFICIAL_CLOB_V2_HOST {
            return Err(RecoveryError::Initialization);
        }
        let client = Client::new(OFFICIAL_CLOB_V2_HOST, SdkConfig::default())
            .map_err(|_| RecoveryError::Initialization)?;
        Self::authenticate(cfg, client, Duration::from_secs(15)).await
    }

    #[cfg(test)]
    async fn new_with_host(
        cfg: &AppConfig,
        host: &str,
        request_timeout: Duration,
    ) -> Result<Self, RecoveryError> {
        let client =
            Client::new(host, SdkConfig::default()).map_err(|_| RecoveryError::Initialization)?;
        Self::authenticate(cfg, client, request_timeout).await
    }

    async fn authenticate(
        cfg: &AppConfig,
        client: Client<Unauthenticated>,
        request_timeout: Duration,
    ) -> Result<Self, RecoveryError> {
        let signer = PrivateKeySigner::from_str(cfg.credentials.private_key.trim())
            .map_err(|_| RecoveryError::Initialization)?
            .with_chain_id(Some(POLYGON));
        let funder = Address::from_str(&cfg.credentials.funder_address)
            .map_err(|_| RecoveryError::Initialization)?;
        if signer.address() != funder
            || cfg.exchange.chain_id != POLYGON
            || cfg.credentials.signature_type != Some(0)
        {
            return Err(RecoveryError::Initialization);
        }
        let key = cfg
            .credentials
            .api_key
            .as_deref()
            .ok_or(RecoveryError::Initialization)
            .and_then(|value| Uuid::parse_str(value).map_err(|_| RecoveryError::Initialization))?;
        let secret = cfg
            .credentials
            .api_secret
            .clone()
            .filter(|value| !value.trim().is_empty())
            .ok_or(RecoveryError::Initialization)?;
        let passphrase = cfg
            .credentials
            .api_passphrase
            .clone()
            .filter(|value| !value.trim().is_empty())
            .ok_or(RecoveryError::Initialization)?;
        let client = client
            .authentication_builder(&signer)
            .credentials(Credentials::new(key, secret, passphrase))
            .authenticate()
            .await
            .map_err(|_| RecoveryError::Initialization)?;
        Ok(Self {
            client,
            request_timeout,
        })
    }

    async fn query_exact_trade(
        &self,
        expected: &PreparedOrderIdentity,
        trade_id: &TradeId,
    ) -> Result<(u128, u128), ReconcileUncertainCode> {
        let request = TradesRequest::builder().id(trade_id.as_str()).build();
        let page = tokio::time::timeout(self.request_timeout, self.client.trades(&request, None))
            .await
            .map_err(|_| ReconcileUncertainCode::Timeout)?
            .map_err(|error| reconcile_error_code(&error))?;
        if page.next_cursor != "LTE=" || page.count != 1 || page.data.len() != 1 {
            return Err(ReconcileUncertainCode::Mismatch);
        }
        classify_trade(expected, trade_id, &page.data[0])
    }
}

#[async_trait]
impl RecoveryGateway for SdkRecoveryGateway {
    async fn reconcile_exact(
        &self,
        expected: &PreparedOrderIdentity,
    ) -> Result<RemoteOrderEvidence, RecoveryError> {
        let response = match tokio::time::timeout(
            self.request_timeout,
            self.client.order(expected.order_id.as_str()),
        )
        .await
        {
            Err(_) => {
                return Ok(RemoteOrderEvidence::Uncertain {
                    code: ReconcileUncertainCode::Timeout,
                })
            }
            Ok(Err(error)) => {
                return Ok(RemoteOrderEvidence::Uncertain {
                    code: reconcile_error_code(&error),
                })
            }
            Ok(Ok(response)) => response,
        };
        let trade_ids = match classify_order(expected, &response) {
            Ok(OrderClassification::Matched(trade_ids)) => trade_ids,
            Ok(OrderClassification::NoFill(status)) => {
                return Ok(RemoteOrderEvidence::NoFill { status });
            }
            Ok(OrderClassification::Live) => return Ok(RemoteOrderEvidence::Live),
            Ok(OrderClassification::Pending) => return Ok(RemoteOrderEvidence::Pending),
            Err(code) => return Ok(RemoteOrderEvidence::Uncertain { code }),
        };
        let mut making_micros = 0_u128;
        let mut taking_micros = 0_u128;
        for trade_id in &trade_ids {
            let (making, taking) = match self.query_exact_trade(expected, trade_id).await {
                Ok(amounts) => amounts,
                Err(code) => return Ok(RemoteOrderEvidence::Uncertain { code }),
            };
            making_micros = match making_micros.checked_add(making) {
                Some(value) => value,
                None => {
                    return Ok(RemoteOrderEvidence::Uncertain {
                        code: ReconcileUncertainCode::Mismatch,
                    })
                }
            };
            taking_micros = match taking_micros.checked_add(taking) {
                Some(value) => value,
                None => {
                    return Ok(RemoteOrderEvidence::Uncertain {
                        code: ReconcileUncertainCode::Mismatch,
                    })
                }
            };
        }
        if making_micros != expected.expected_maker_micros
            || taking_micros != expected.expected_taker_micros
        {
            return Ok(RemoteOrderEvidence::Uncertain {
                code: ReconcileUncertainCode::Mismatch,
            });
        }
        Ok(RemoteOrderEvidence::Matched {
            making_micros,
            taking_micros,
            trade_ids,
        })
    }

    async fn cancel_exact(
        &self,
        order_id: &crate::service::execution_ledger::OrderId,
    ) -> Result<CancelAttemptEvidence, RecoveryError> {
        let response = match tokio::time::timeout(
            self.request_timeout,
            self.client.cancel_order(order_id.as_str()),
        )
        .await
        {
            Err(_) => {
                return Ok(CancelAttemptEvidence::Uncertain {
                    code: CancelUncertainCode::Timeout,
                });
            }
            Ok(Err(error)) => {
                return Ok(CancelAttemptEvidence::Uncertain {
                    code: cancel_error_code(&error),
                });
            }
            Ok(Ok(response)) => response,
        };
        Ok(classify_cancel_response(order_id.as_str(), &response))
    }
}

fn cancel_error_code(error: &SdkError) -> CancelUncertainCode {
    if error
        .downcast_ref::<SdkStatus>()
        .is_some_and(|status| status.status_code.as_u16() == 404)
    {
        CancelUncertainCode::NotFound
    } else if is_decode_error(error) {
        CancelUncertainCode::MalformedResponse
    } else {
        CancelUncertainCode::Transport
    }
}

fn is_decode_error(error: &SdkError) -> bool {
    if error.downcast_ref::<EmptyResponse>().is_some() {
        return true;
    }
    let mut source: Option<&(dyn std::error::Error + 'static)> = error
        .inner()
        .map(|value| value as &(dyn std::error::Error + 'static));
    while let Some(value) = source {
        if value.is::<serde_json::Error>()
            || value
                .downcast_ref::<reqwest::Error>()
                .is_some_and(reqwest::Error::is_decode)
        {
            return true;
        }
        source = value.source();
    }
    false
}

fn classify_cancel_response(
    order_id: &str,
    response: &CancelOrdersResponse,
) -> CancelAttemptEvidence {
    if response.canceled.len() == 1
        && response.canceled[0] == order_id
        && response.not_canceled.is_empty()
    {
        CancelAttemptEvidence::Canceled
    } else if response.canceled.is_empty()
        && response.not_canceled.len() == 1
        && response.not_canceled.contains_key(order_id)
    {
        CancelAttemptEvidence::NotCanceled
    } else {
        CancelAttemptEvidence::Uncertain {
            code: CancelUncertainCode::ResponseMismatch,
        }
    }
}

enum OrderClassification {
    Matched(Vec<TradeId>),
    NoFill(crate::service::execution_ledger::TerminalNoFillStatus),
    Live,
    Pending,
}

fn reconcile_error_code(error: &SdkError) -> ReconcileUncertainCode {
    if error
        .downcast_ref::<SdkStatus>()
        .is_some_and(|status| status.status_code.as_u16() == 404)
    {
        ReconcileUncertainCode::NotFound
    } else if is_decode_error(error) {
        ReconcileUncertainCode::MalformedResponse
    } else {
        ReconcileUncertainCode::Transport
    }
}

fn classify_order(
    expected: &PreparedOrderIdentity,
    response: &OpenOrderResponse,
) -> Result<OrderClassification, ReconcileUncertainCode> {
    let expected_side = match expected.side {
        OrderSide::Buy => SdkSide::Buy,
        OrderSide::Sell => SdkSide::Sell,
    };
    if expected.protocol_version != 2
        || expected.venue != Venue::PolymarketClob
        || expected.order_type != OrderType::Fok
        || response.id != expected.order_id.as_str()
        || response.asset_id != expected.token_id.as_u256()
        || response.side != expected_side
        || response.order_type != SdkOrderType::FOK
        || response.original_size <= Decimal::ZERO
        || response.price <= Decimal::ZERO
    {
        return Err(ReconcileUncertainCode::Mismatch);
    }
    let (making, taking) = order_amounts(expected.side, response.original_size, response.price)?;
    if making != expected.expected_maker_micros || taking != expected.expected_taker_micros {
        return Err(ReconcileUncertainCode::Mismatch);
    }
    let matched_micros = decimal_to_micros(response.size_matched)?;
    let original_micros = decimal_to_micros(response.original_size)?;
    if matched_micros > original_micros {
        return Err(ReconcileUncertainCode::Mismatch);
    }
    match response.status {
        OrderStatusType::Live | OrderStatusType::Unmatched => {
            if matched_micros == 0 && response.associate_trades.is_empty() {
                return Ok(OrderClassification::Live);
            }
            return Err(ReconcileUncertainCode::PartialFill);
        }
        OrderStatusType::Delayed => {
            if matched_micros == 0 && response.associate_trades.is_empty() {
                return Ok(OrderClassification::Pending);
            }
            return Err(ReconcileUncertainCode::PartialFill);
        }
        OrderStatusType::Canceled => {
            if matched_micros == 0 && response.associate_trades.is_empty() {
                return Ok(OrderClassification::NoFill(
                    crate::service::execution_ledger::TerminalNoFillStatus::Canceled,
                ));
            }
            return Err(ReconcileUncertainCode::PartialFill);
        }
        OrderStatusType::Matched => {}
        OrderStatusType::Unknown(_) => return Err(ReconcileUncertainCode::UnknownStatus),
        _ => return Err(ReconcileUncertainCode::UnknownStatus),
    }
    if matched_micros != original_micros {
        return Err(ReconcileUncertainCode::PartialFill);
    }
    let mut trade_ids = Vec::with_capacity(response.associate_trades.len());
    for value in &response.associate_trades {
        let trade_id =
            TradeId::from_exact(value.clone()).ok_or(ReconcileUncertainCode::Mismatch)?;
        if trade_ids.contains(&trade_id) {
            return Err(ReconcileUncertainCode::Mismatch);
        }
        trade_ids.push(trade_id);
    }
    if trade_ids.is_empty() {
        return Err(ReconcileUncertainCode::Mismatch);
    }
    Ok(OrderClassification::Matched(trade_ids))
}

fn order_amounts(
    side: OrderSide,
    shares: Decimal,
    price: Decimal,
) -> Result<(u128, u128), ReconcileUncertainCode> {
    let shares = decimal_to_micros(shares)?;
    let usd = decimal_to_micros(shares_to_decimal(shares)? * price)?;
    Ok(match side {
        OrderSide::Buy => (usd, shares),
        OrderSide::Sell => (shares, usd),
    })
}

fn shares_to_decimal(micros: u128) -> Result<Decimal, ReconcileUncertainCode> {
    let micros = i128::try_from(micros).map_err(|_| ReconcileUncertainCode::Mismatch)?;
    Decimal::try_from_i128_with_scale(micros, 6).map_err(|_| ReconcileUncertainCode::Mismatch)
}

fn classify_trade(
    expected: &PreparedOrderIdentity,
    trade_id: &TradeId,
    trade: &TradeResponse,
) -> Result<(u128, u128), ReconcileUncertainCode> {
    let expected_side = match expected.side {
        OrderSide::Buy => SdkSide::Buy,
        OrderSide::Sell => SdkSide::Sell,
    };
    if trade.id != trade_id.as_str()
        || trade.asset_id != expected.token_id.as_u256()
        || trade.side != expected_side
        || trade.status != TradeStatusType::Confirmed
    {
        return Err(ReconcileUncertainCode::Mismatch);
    }
    let exact_order_id = expected.order_id.as_str();
    let (size, price) = match &trade.trader_side {
        TraderSide::Taker
            if trade.taker_order_id == exact_order_id
                && !trade
                    .maker_orders
                    .iter()
                    .any(|maker| maker.order_id == exact_order_id) =>
        {
            (trade.size, trade.price)
        }
        TraderSide::Maker => {
            if trade.taker_order_id == exact_order_id {
                return Err(ReconcileUncertainCode::Mismatch);
            }
            let mut exact_makers = trade
                .maker_orders
                .iter()
                .filter(|maker| maker.order_id == exact_order_id);
            let maker = exact_makers
                .next()
                .ok_or(ReconcileUncertainCode::Mismatch)?;
            if exact_makers.next().is_some()
                || maker.asset_id != expected.token_id.as_u256()
                || maker.side != expected_side
                || maker.matched_amount <= Decimal::ZERO
            {
                return Err(ReconcileUncertainCode::Mismatch);
            }
            (maker.matched_amount, maker.price)
        }
        TraderSide::Taker | TraderSide::Unknown(_) | _ => {
            return Err(ReconcileUncertainCode::Mismatch);
        }
    };
    if size <= Decimal::ZERO || price <= Decimal::ZERO {
        return Err(ReconcileUncertainCode::Mismatch);
    }
    order_amounts(expected.side, size, price)
}

fn decimal_to_micros(value: Decimal) -> Result<u128, ReconcileUncertainCode> {
    let value = value.normalize();
    if value.is_sign_negative() || value.scale() > 6 {
        return Err(ReconcileUncertainCode::Mismatch);
    }
    let factor = 10_i128
        .checked_pow(6 - value.scale())
        .ok_or(ReconcileUncertainCode::Mismatch)?;
    let micros = value
        .mantissa()
        .checked_mul(factor)
        .ok_or(ReconcileUncertainCode::Mismatch)?;
    let reconstructed = Decimal::try_from_i128_with_scale(micros, 6)
        .map_err(|_| ReconcileUncertainCode::Mismatch)?;
    if reconstructed != value {
        return Err(ReconcileUncertainCode::Mismatch);
    }
    micros
        .try_into()
        .map_err(|_| ReconcileUncertainCode::Mismatch)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::TcpListener,
    };

    use crate::{
        config::AppConfig,
        service::{
            execution_ledger::{
                OrderId, OrderSide, OrderType, TokenId, Venue, ORDER_PROTOCOL_VERSION,
            },
            order_gateway::PreparedOrderIdentity,
            recovery_gateway::{RecoveryGateway as _, RemoteOrderEvidence, TradeId},
        },
    };

    use super::SdkRecoveryGateway;

    const PUBLIC_HARDHAT_KEY: &str =
        "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

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

    fn expected_buy() -> PreparedOrderIdentity {
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
    async fn production_constructor_rejects_loopback_before_any_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut cfg = fixture_config();
        cfg.site.clob_api_base = format!("http://{}", listener.local_addr().unwrap());

        let error = match SdkRecoveryGateway::new(&cfg).await {
            Err(error) => error,
            Ok(_) => panic!("production recovery gateway must reject a loopback host"),
        };

        assert_eq!(error, super::RecoveryError::Initialization);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn exact_matched_buy_queries_order_then_each_exact_trade_once() {
        let expected = expected_buy();
        let order_body = format!(
            r#"{{
                "id":"{}",
                "status":"MATCHED",
                "owner":"00000000-0000-0000-0000-000000000000",
                "maker_address":"0x0000000000000000000000000000000000000001",
                "market":"0x0000000000000000000000000000000000000000000000000000000000000001",
                "asset_id":"12345",
                "side":"BUY",
                "original_size":"40",
                "size_matched":"40",
                "price":"0.5",
                "associate_trades":["trade-1"],
                "outcome":"Yes",
                "created_at":1770000000,
                "expiration":"0",
                "order_type":"FOK"
            }}"#,
            expected.order_id.as_str()
        );
        let trade_body = format!(
            r#"{{
                "data":[{{
                    "id":"trade-1",
                    "taker_order_id":"{}",
                    "market":"0x0000000000000000000000000000000000000000000000000000000000000001",
                    "asset_id":"12345",
                    "side":"BUY",
                    "size":"40",
                    "fee_rate_bps":"0",
                    "price":"0.5",
                    "status":"CONFIRMED",
                    "match_time":"1770000000",
                    "last_update":"1770000001",
                    "outcome":"Yes",
                    "bucket_index":0,
                    "owner":"00000000-0000-0000-0000-000000000000",
                    "maker_address":"0x0000000000000000000000000000000000000001",
                    "maker_orders":[],
                    "transaction_hash":"0x0000000000000000000000000000000000000000000000000000000000000002",
                    "trader_side":"TAKER",
                    "error_msg":null
                }}],
                "next_cursor":"LTE=",
                "limit":1,
                "count":1
            }}"#,
            expected.order_id.as_str()
        );
        let exact_order_path = format!("GET /data/order/{}", expected.order_id.as_str());
        let (host, server) = spawn_scripted_server(vec![
            (exact_order_path.clone(), "200 OK".to_owned(), order_body),
            (
                "GET /data/trades?id=trade-1".to_owned(),
                "200 OK".to_owned(),
                trade_body,
            ),
        ])
        .await;
        let gateway =
            SdkRecoveryGateway::new_with_host(&fixture_config(), &host, Duration::from_secs(1))
                .await
                .unwrap();

        let evidence = gateway.reconcile_exact(&expected).await.unwrap();
        let requests = server.await.unwrap();

        assert_eq!(
            evidence,
            RemoteOrderEvidence::Matched {
                making_micros: 20_000_000,
                taking_micros: 40_000_000,
                trade_ids: vec![TradeId::from_exact("trade-1").unwrap()],
            }
        );
        assert_eq!(
            requests,
            vec![exact_order_path, "GET /data/trades?id=trade-1".to_owned()]
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.contains("/auth/api-key")
                    || request.contains("/auth/derive-api-key"))
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn cancel_exact_sends_one_delete_for_only_the_supplied_order_id() {
        let expected = expected_buy();
        let (host, server) = spawn_scripted_server(vec![(
            "DELETE /order".to_owned(),
            "200 OK".to_owned(),
            format!(
                r#"{{"canceled":["{}"],"not_canceled":{{}}}}"#,
                expected.order_id.as_str()
            ),
        )])
        .await;
        let gateway =
            SdkRecoveryGateway::new_with_host(&fixture_config(), &host, Duration::from_secs(1))
                .await
                .unwrap();

        let evidence = gateway.cancel_exact(&expected.order_id).await.unwrap();
        let requests = server.await.unwrap();

        assert_eq!(evidence, super::CancelAttemptEvidence::Canceled);
        assert_eq!(requests, vec!["DELETE /order".to_owned()]);
    }

    #[tokio::test]
    async fn cancel_exact_maps_not_found_to_sanitized_uncertain_without_retry() {
        let expected = expected_buy();
        let (host, server) = spawn_scripted_server(vec![(
            "DELETE /order".to_owned(),
            "404 Not Found".to_owned(),
            r#"{"error":"RAW_SDK_BODY_ERROR_SENTINEL"}"#.to_owned(),
        )])
        .await;
        let gateway =
            SdkRecoveryGateway::new_with_host(&fixture_config(), &host, Duration::from_secs(1))
                .await
                .unwrap();

        let evidence = gateway.cancel_exact(&expected.order_id).await.unwrap();
        let rendered = format!("{evidence:?} {evidence}");

        assert_eq!(
            evidence,
            super::CancelAttemptEvidence::Uncertain {
                code: super::CancelUncertainCode::NotFound,
            }
        );
        assert!(!rendered.contains("RAW_SDK_BODY_ERROR_SENTINEL"));
        assert_eq!(server.await.unwrap(), vec!["DELETE /order".to_owned()]);
    }

    #[tokio::test]
    async fn cancel_exact_transmits_only_the_exact_order_id_in_its_single_request() {
        let expected = expected_buy();
        let (host, server) = spawn_raw_response_server(format!(
            r#"{{"canceled":["{}"],"not_canceled":{{}}}}"#,
            expected.order_id.as_str()
        ))
        .await;
        let gateway =
            SdkRecoveryGateway::new_with_host(&fixture_config(), &host, Duration::from_secs(1))
                .await
                .unwrap();

        assert_eq!(
            gateway.cancel_exact(&expected.order_id).await.unwrap(),
            super::CancelAttemptEvidence::Canceled
        );
        let raw_request = server.await.unwrap();
        assert!(raw_request.starts_with("DELETE /order HTTP/1.1\r\n"));
        assert!(raw_request.contains(&format!(
            r#"{{"orderID":"{}"}}"#,
            expected.order_id.as_str()
        )));
        assert!(!raw_request.contains("orderIDs"));
        assert!(!raw_request.contains("cancel-all"));
        assert!(!raw_request.contains("cancel-market"));
    }

    #[tokio::test]
    async fn exact_order_statuses_are_classified_without_search_or_retry() {
        let expected = expected_buy();
        let exact_path = format!("GET /data/order/{}", expected.order_id.as_str());
        let cases = [
            ("LIVE", "0", "[]", RemoteOrderEvidence::Live),
            ("UNMATCHED", "0", "[]", RemoteOrderEvidence::Live),
            ("DELAYED", "0", "[]", RemoteOrderEvidence::Pending),
            (
                "CANCELED",
                "0",
                "[]",
                RemoteOrderEvidence::NoFill {
                    status: crate::service::execution_ledger::TerminalNoFillStatus::Canceled,
                },
            ),
        ];

        for (status, size_matched, trade_ids, want) in cases {
            let (host, server) = spawn_scripted_server(vec![(
                exact_path.clone(),
                "200 OK".to_owned(),
                order_body(&expected, status, size_matched, trade_ids),
            )])
            .await;
            let gateway =
                SdkRecoveryGateway::new_with_host(&fixture_config(), &host, Duration::from_secs(1))
                    .await
                    .unwrap();

            let evidence = gateway.reconcile_exact(&expected).await.unwrap();
            assert_eq!(evidence, want, "status {status}");
            assert_eq!(server.await.unwrap(), vec![exact_path.clone()]);
        }
    }

    #[tokio::test]
    async fn exact_order_not_found_is_sanitized_without_a_second_request() {
        let expected = expected_buy();
        let exact_path = format!("GET /data/order/{}", expected.order_id.as_str());
        let (host, server) = spawn_scripted_server(vec![(
            exact_path.clone(),
            "404 Not Found".to_owned(),
            r#"{"error":"RAW_SDK_BODY_ERROR_SENTINEL"}"#.to_owned(),
        )])
        .await;
        let gateway =
            SdkRecoveryGateway::new_with_host(&fixture_config(), &host, Duration::from_secs(1))
                .await
                .unwrap();

        let evidence = gateway.reconcile_exact(&expected).await.unwrap();
        let rendered = format!("{evidence:?} {evidence}");

        assert_eq!(
            evidence,
            RemoteOrderEvidence::Uncertain {
                code: crate::service::execution_ledger::ReconcileUncertainCode::NotFound,
            }
        );
        assert!(!rendered.contains("RAW_SDK_BODY_ERROR_SENTINEL"));
        assert_eq!(server.await.unwrap(), vec![exact_path]);
    }

    #[tokio::test]
    async fn exact_trade_not_found_is_sanitized_without_a_second_trade_request() {
        let expected = expected_buy();
        let exact_path = format!("GET /data/order/{}", expected.order_id.as_str());
        let (host, server) = spawn_scripted_server(vec![
            (
                exact_path.clone(),
                "200 OK".to_owned(),
                order_body(&expected, "MATCHED", "40", r#"["trade-1"]"#),
            ),
            (
                "GET /data/trades?id=trade-1".to_owned(),
                "404 Not Found".to_owned(),
                r#"{"error":"RAW_SDK_BODY_ERROR_SENTINEL"}"#.to_owned(),
            ),
        ])
        .await;
        let gateway =
            SdkRecoveryGateway::new_with_host(&fixture_config(), &host, Duration::from_secs(1))
                .await
                .unwrap();

        let evidence = gateway.reconcile_exact(&expected).await.unwrap();
        assert_eq!(
            evidence,
            RemoteOrderEvidence::Uncertain {
                code: crate::service::execution_ledger::ReconcileUncertainCode::NotFound,
            }
        );
        assert_eq!(
            server.await.unwrap(),
            vec![exact_path, "GET /data/trades?id=trade-1".to_owned()]
        );
    }

    #[tokio::test]
    async fn full_sell_with_exact_maker_trade_association_is_matched() {
        let mut expected = expected_buy();
        expected.side = OrderSide::Sell;
        expected.expected_maker_micros = 40_000_000;
        expected.expected_taker_micros = 20_000_000;
        let exact_path = format!("GET /data/order/{}", expected.order_id.as_str());
        let trade_body = format!(
            r#"{{"data":[{{"id":"trade-1","taker_order_id":"other-order","market":"0x0000000000000000000000000000000000000000000000000000000000000001","asset_id":"12345","side":"SELL","size":"40","fee_rate_bps":"0","price":"0.5","status":"CONFIRMED","match_time":"1770000000","last_update":"1770000001","outcome":"Yes","bucket_index":0,"owner":"00000000-0000-0000-0000-000000000000","maker_address":"0x0000000000000000000000000000000000000001","maker_orders":[{{"order_id":"{}","owner":"00000000-0000-0000-0000-000000000000","maker_address":"0x0000000000000000000000000000000000000001","matched_amount":"40","price":"0.5","fee_rate_bps":"0","asset_id":"12345","outcome":"Yes","side":"SELL"}}],"transaction_hash":"0x0000000000000000000000000000000000000000000000000000000000000002","trader_side":"MAKER","error_msg":null}}],"next_cursor":"LTE=","limit":1,"count":1}}"#,
            expected.order_id.as_str()
        );
        let (host, server) = spawn_scripted_server(vec![
            (
                exact_path.clone(),
                "200 OK".to_owned(),
                order_body(&expected, "MATCHED", "40", r#"["trade-1"]"#),
            ),
            (
                "GET /data/trades?id=trade-1".to_owned(),
                "200 OK".to_owned(),
                trade_body,
            ),
        ])
        .await;
        let gateway =
            SdkRecoveryGateway::new_with_host(&fixture_config(), &host, Duration::from_secs(1))
                .await
                .unwrap();

        assert_eq!(
            gateway.reconcile_exact(&expected).await.unwrap(),
            RemoteOrderEvidence::Matched {
                making_micros: 40_000_000,
                taking_micros: 20_000_000,
                trade_ids: vec![TradeId::from_exact("trade-1").unwrap()],
            }
        );
        assert_eq!(
            server.await.unwrap(),
            vec![exact_path, "GET /data/trades?id=trade-1".to_owned()]
        );
    }

    #[tokio::test]
    async fn partial_order_evidence_is_uncertain_and_never_followed() {
        let expected = expected_buy();
        let exact_path = format!("GET /data/order/{}", expected.order_id.as_str());
        let partial_order = order_body(&expected, "LIVE", "1", r#"["trade-1"]"#);
        let (host, server) = spawn_scripted_server(vec![(
            exact_path.clone(),
            "200 OK".to_owned(),
            partial_order,
        )])
        .await;
        let gateway =
            SdkRecoveryGateway::new_with_host(&fixture_config(), &host, Duration::from_secs(1))
                .await
                .unwrap();

        assert_eq!(
            gateway.reconcile_exact(&expected).await.unwrap(),
            RemoteOrderEvidence::Uncertain {
                code: crate::service::execution_ledger::ReconcileUncertainCode::PartialFill,
            }
        );
        assert_eq!(server.await.unwrap(), vec![exact_path]);
    }

    #[tokio::test]
    async fn unknown_or_mismatched_exact_order_is_uncertain_without_trade_search() {
        let expected = expected_buy();
        let exact_path = format!("GET /data/order/{}", expected.order_id.as_str());
        let base = order_body(&expected, "MATCHED", "40", r#"["trade-1"]"#);
        let cases = [
            (
                base.replacen(expected.order_id.as_str(), "wrong-order", 1),
                crate::service::execution_ledger::ReconcileUncertainCode::Mismatch,
            ),
            (
                base.replacen("\"asset_id\":\"12345\"", "\"asset_id\":\"999\"", 1),
                crate::service::execution_ledger::ReconcileUncertainCode::Mismatch,
            ),
            (
                base.replacen("\"side\":\"BUY\"", "\"side\":\"SELL\"", 1),
                crate::service::execution_ledger::ReconcileUncertainCode::Mismatch,
            ),
            (
                base.replacen("\"order_type\":\"FOK\"", "\"order_type\":\"GTC\"", 1),
                crate::service::execution_ledger::ReconcileUncertainCode::Mismatch,
            ),
            (
                base.replacen("\"original_size\":\"40\"", "\"original_size\":\"41\"", 1),
                crate::service::execution_ledger::ReconcileUncertainCode::Mismatch,
            ),
            (
                base.replacen("\"status\":\"MATCHED\"", "\"status\":\"FUTURE\"", 1),
                crate::service::execution_ledger::ReconcileUncertainCode::UnknownStatus,
            ),
        ];

        for (body, code) in cases {
            let (host, server) =
                spawn_scripted_server(vec![(exact_path.clone(), "200 OK".to_owned(), body)]).await;
            let gateway =
                SdkRecoveryGateway::new_with_host(&fixture_config(), &host, Duration::from_secs(1))
                    .await
                    .unwrap();

            assert_eq!(
                gateway.reconcile_exact(&expected).await.unwrap(),
                RemoteOrderEvidence::Uncertain { code }
            );
            assert_eq!(server.await.unwrap(), vec![exact_path.clone()]);
        }
    }

    #[tokio::test]
    async fn trade_cursor_is_ambiguous_and_is_never_followed() {
        let expected = expected_buy();
        let exact_path = format!("GET /data/order/{}", expected.order_id.as_str());
        let (host, server) = spawn_scripted_server(vec![
            (
                exact_path.clone(),
                "200 OK".to_owned(),
                order_body(&expected, "MATCHED", "40", r#"["trade-1"]"#),
            ),
            (
                "GET /data/trades?id=trade-1".to_owned(),
                "200 OK".to_owned(),
                taker_trade_page(&expected, "trade-1", "next-page"),
            ),
        ])
        .await;
        let gateway =
            SdkRecoveryGateway::new_with_host(&fixture_config(), &host, Duration::from_secs(1))
                .await
                .unwrap();

        assert_eq!(
            gateway.reconcile_exact(&expected).await.unwrap(),
            RemoteOrderEvidence::Uncertain {
                code: crate::service::execution_ledger::ReconcileUncertainCode::Mismatch,
            }
        );
        assert_eq!(
            server.await.unwrap(),
            vec![exact_path, "GET /data/trades?id=trade-1".to_owned()]
        );
    }

    #[tokio::test]
    async fn malformed_and_unexpected_http_order_responses_are_sanitized_once() {
        let expected = expected_buy();
        let exact_path = format!("GET /data/order/{}", expected.order_id.as_str());
        let cases = [
            (
                "200 OK".to_owned(),
                "{not-json".to_owned(),
                crate::service::execution_ledger::ReconcileUncertainCode::MalformedResponse,
            ),
            (
                "503 Service Unavailable".to_owned(),
                r#"{"error":"RAW_SDK_BODY_ERROR_SENTINEL"}"#.to_owned(),
                crate::service::execution_ledger::ReconcileUncertainCode::Transport,
            ),
        ];

        for (status, body, code) in cases {
            let (host, server) =
                spawn_scripted_server(vec![(exact_path.clone(), status, body)]).await;
            let gateway =
                SdkRecoveryGateway::new_with_host(&fixture_config(), &host, Duration::from_secs(1))
                    .await
                    .unwrap();
            let evidence = gateway.reconcile_exact(&expected).await.unwrap();

            assert_eq!(evidence, RemoteOrderEvidence::Uncertain { code });
            assert!(!format!("{evidence:?} {evidence}").contains("RAW_SDK_BODY_ERROR_SENTINEL"));
            assert_eq!(server.await.unwrap(), vec![exact_path.clone()]);
        }
    }

    #[tokio::test]
    async fn timeout_and_disconnect_are_uncertain_without_retry() {
        let expected = expected_buy();
        let exact_path = format!("GET /data/order/{}", expected.order_id.as_str());
        let cases = [
            (
                OneRequestServerMode::Disconnect,
                Duration::from_secs(1),
                crate::service::execution_ledger::ReconcileUncertainCode::Transport,
            ),
            (
                OneRequestServerMode::Withhold(Duration::from_millis(100)),
                Duration::from_millis(20),
                crate::service::execution_ledger::ReconcileUncertainCode::Timeout,
            ),
        ];

        for (mode, timeout, code) in cases {
            let (host, server) = spawn_one_request_server(mode).await;
            let gateway = SdkRecoveryGateway::new_with_host(&fixture_config(), &host, timeout)
                .await
                .unwrap();

            assert_eq!(
                gateway.reconcile_exact(&expected).await.unwrap(),
                RemoteOrderEvidence::Uncertain { code }
            );
            assert_eq!(server.await.unwrap(), vec![exact_path.clone()]);
        }
    }

    #[tokio::test]
    async fn contradictory_taker_or_maker_membership_is_mismatch_without_replay() {
        let expected = expected_buy();
        let exact_path = format!("GET /data/order/{}", expected.order_id.as_str());
        let maker = maker_order(&expected, expected.order_id.as_str(), "40");
        let cases = [
            (
                "taker_also_lists_exact_maker",
                taker_trade_page(&expected, "trade-1", "LTE=").replacen(
                    "\"maker_orders\":[]",
                    &format!("\"maker_orders\":[{maker}]"),
                    1,
                ),
            ),
            (
                "maker_also_names_exact_taker",
                maker_trade_page(
                    &expected,
                    "trade-1",
                    expected.order_id.as_str(),
                    &maker,
                    "LTE=",
                    1,
                ),
            ),
            (
                "maker_lists_exact_child_twice",
                maker_trade_page(
                    &expected,
                    "trade-1",
                    "another-order",
                    &format!("{maker},{maker}"),
                    "LTE=",
                    1,
                ),
            ),
        ];

        for (name, trade_body) in cases {
            let (host, server) = spawn_scripted_server(vec![
                (
                    exact_path.clone(),
                    "200 OK".to_owned(),
                    order_body(&expected, "MATCHED", "40", r#"["trade-1"]"#),
                ),
                (
                    "GET /data/trades?id=trade-1".to_owned(),
                    "200 OK".to_owned(),
                    trade_body,
                ),
            ])
            .await;
            let gateway =
                SdkRecoveryGateway::new_with_host(&fixture_config(), &host, Duration::from_secs(1))
                    .await
                    .unwrap();

            assert_eq!(
                gateway.reconcile_exact(&expected).await.unwrap(),
                RemoteOrderEvidence::Uncertain {
                    code: crate::service::execution_ledger::ReconcileUncertainCode::Mismatch,
                },
                "{name}"
            );
            assert_eq!(
                server.await.unwrap(),
                vec![exact_path.clone(), "GET /data/trades?id=trade-1".to_owned()],
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn successful_null_order_is_malformed_without_retry() {
        let expected = expected_buy();
        let exact_path = format!("GET /data/order/{}", expected.order_id.as_str());
        let (host, server) = spawn_scripted_server(vec![(
            exact_path.clone(),
            "200 OK".to_owned(),
            "null".to_owned(),
        )])
        .await;
        let gateway =
            SdkRecoveryGateway::new_with_host(&fixture_config(), &host, Duration::from_secs(1))
                .await
                .unwrap();

        assert_eq!(
            gateway.reconcile_exact(&expected).await.unwrap(),
            RemoteOrderEvidence::Uncertain {
                code: crate::service::execution_ledger::ReconcileUncertainCode::MalformedResponse,
            }
        );
        assert_eq!(server.await.unwrap(), vec![exact_path]);
    }

    #[tokio::test]
    async fn successful_null_trade_is_malformed_without_retry() {
        let expected = expected_buy();
        let exact_path = format!("GET /data/order/{}", expected.order_id.as_str());
        let (host, server) = spawn_scripted_server(vec![
            (
                exact_path.clone(),
                "200 OK".to_owned(),
                order_body(&expected, "MATCHED", "40", r#"["trade-1"]"#),
            ),
            (
                "GET /data/trades?id=trade-1".to_owned(),
                "200 OK".to_owned(),
                "null".to_owned(),
            ),
        ])
        .await;
        let gateway =
            SdkRecoveryGateway::new_with_host(&fixture_config(), &host, Duration::from_secs(1))
                .await
                .unwrap();

        assert_eq!(
            gateway.reconcile_exact(&expected).await.unwrap(),
            RemoteOrderEvidence::Uncertain {
                code: crate::service::execution_ledger::ReconcileUncertainCode::MalformedResponse,
            }
        );
        assert_eq!(
            server.await.unwrap(),
            vec![exact_path, "GET /data/trades?id=trade-1".to_owned()]
        );
    }

    #[tokio::test]
    async fn successful_null_cancel_is_malformed_without_retry() {
        let expected = expected_buy();
        let (host, server) = spawn_scripted_server(vec![(
            "DELETE /order".to_owned(),
            "200 OK".to_owned(),
            "null".to_owned(),
        )])
        .await;
        let gateway =
            SdkRecoveryGateway::new_with_host(&fixture_config(), &host, Duration::from_secs(1))
                .await
                .unwrap();

        assert_eq!(
            gateway.cancel_exact(&expected.order_id).await.unwrap(),
            super::CancelAttemptEvidence::Uncertain {
                code: super::CancelUncertainCode::MalformedResponse,
            }
        );
        assert_eq!(server.await.unwrap(), vec!["DELETE /order".to_owned()]);
    }

    #[tokio::test]
    async fn cancel_response_shapes_are_conservative_and_never_retried() {
        let expected = expected_buy();
        let cases = [
            (
                "exact_not_canceled",
                format!(
                    r#"{{"canceled":[],"not_canceled":{{"{}":"still open"}}}}"#,
                    expected.order_id.as_str()
                ),
                super::CancelAttemptEvidence::NotCanceled,
            ),
            (
                "mixed_exact_ids",
                format!(
                    r#"{{"canceled":["{}"],"not_canceled":{{"{}":"still open"}}}}"#,
                    expected.order_id.as_str(),
                    expected.order_id.as_str()
                ),
                super::CancelAttemptEvidence::Uncertain {
                    code: super::CancelUncertainCode::ResponseMismatch,
                },
            ),
            (
                "extra_canceled_id",
                format!(
                    r#"{{"canceled":["{}","other-order"],"not_canceled":{{}}}}"#,
                    expected.order_id.as_str()
                ),
                super::CancelAttemptEvidence::Uncertain {
                    code: super::CancelUncertainCode::ResponseMismatch,
                },
            ),
            (
                "extra_not_canceled_id",
                format!(
                    r#"{{"canceled":[],"not_canceled":{{"{}":"still open","other-order":"still open"}}}}"#,
                    expected.order_id.as_str()
                ),
                super::CancelAttemptEvidence::Uncertain {
                    code: super::CancelUncertainCode::ResponseMismatch,
                },
            ),
        ];

        for (name, body, want) in cases {
            let (host, server) = spawn_scripted_server(vec![(
                "DELETE /order".to_owned(),
                "200 OK".to_owned(),
                body,
            )])
            .await;
            let gateway =
                SdkRecoveryGateway::new_with_host(&fixture_config(), &host, Duration::from_secs(1))
                    .await
                    .unwrap();

            assert_eq!(
                gateway.cancel_exact(&expected.order_id).await.unwrap(),
                want,
                "{name}"
            );
            assert_eq!(
                server.await.unwrap(),
                vec!["DELETE /order".to_owned()],
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn cancel_malformed_transport_timeout_and_redirect_are_uncertain_once() {
        let expected = expected_buy();
        let cases = [
            (
                "malformed",
                "200 OK".to_owned(),
                "{not-json".to_owned(),
                super::CancelUncertainCode::MalformedResponse,
            ),
            (
                "unexpected_http",
                "503 Service Unavailable".to_owned(),
                r#"{"error":"RAW_SDK_BODY_ERROR_SENTINEL"}"#.to_owned(),
                super::CancelUncertainCode::Transport,
            ),
        ];
        for (name, status, body, code) in cases {
            let (host, server) =
                spawn_scripted_server(vec![("DELETE /order".to_owned(), status, body)]).await;
            let gateway =
                SdkRecoveryGateway::new_with_host(&fixture_config(), &host, Duration::from_secs(1))
                    .await
                    .unwrap();
            let evidence = gateway.cancel_exact(&expected.order_id).await.unwrap();

            assert_eq!(
                evidence,
                super::CancelAttemptEvidence::Uncertain { code },
                "{name}"
            );
            assert!(!format!("{evidence:?} {evidence}").contains("RAW_SDK_BODY_ERROR_SENTINEL"));
            assert_eq!(
                server.await.unwrap(),
                vec!["DELETE /order".to_owned()],
                "{name}"
            );
        }

        let cases = [
            (
                OneRequestServerMode::Disconnect,
                Duration::from_secs(1),
                super::CancelUncertainCode::Transport,
            ),
            (
                OneRequestServerMode::Withhold(Duration::from_millis(100)),
                Duration::from_millis(20),
                super::CancelUncertainCode::Timeout,
            ),
        ];
        for (mode, timeout, code) in cases {
            let (host, server) = spawn_one_request_server(mode).await;
            let gateway = SdkRecoveryGateway::new_with_host(&fixture_config(), &host, timeout)
                .await
                .unwrap();

            assert_eq!(
                gateway.cancel_exact(&expected.order_id).await.unwrap(),
                super::CancelAttemptEvidence::Uncertain { code }
            );
            assert_eq!(server.await.unwrap(), vec!["DELETE /order".to_owned()]);
        }
    }

    #[tokio::test]
    async fn recovery_redirect_is_not_followed_or_replayed() {
        let expected = expected_buy();
        let exact_path = format!("GET /data/order/{}", expected.order_id.as_str());
        let (host, server) = spawn_redirect_server(exact_path.clone()).await;
        let gateway =
            SdkRecoveryGateway::new_with_host(&fixture_config(), &host, Duration::from_secs(1))
                .await
                .unwrap();

        assert_eq!(
            gateway.reconcile_exact(&expected).await.unwrap(),
            RemoteOrderEvidence::Uncertain {
                code: crate::service::execution_ledger::ReconcileUncertainCode::Transport,
            }
        );
        assert_eq!(server.await.unwrap(), vec![exact_path]);
    }

    #[tokio::test]
    async fn distinct_exact_trades_are_each_queried_once_and_aggregate_exactly() {
        let expected = expected_buy();
        let exact_path = format!("GET /data/order/{}", expected.order_id.as_str());
        let (host, server) = spawn_scripted_server(vec![
            (
                exact_path.clone(),
                "200 OK".to_owned(),
                order_body(&expected, "MATCHED", "40", r#"["trade-1","trade-2"]"#),
            ),
            (
                "GET /data/trades?id=trade-1".to_owned(),
                "200 OK".to_owned(),
                trade_page(&taker_trade_record(&expected, "trade-1", "20"), "LTE=", 1),
            ),
            (
                "GET /data/trades?id=trade-2".to_owned(),
                "200 OK".to_owned(),
                trade_page(&taker_trade_record(&expected, "trade-2", "20"), "LTE=", 1),
            ),
        ])
        .await;
        let gateway =
            SdkRecoveryGateway::new_with_host(&fixture_config(), &host, Duration::from_secs(1))
                .await
                .unwrap();

        assert_eq!(
            gateway.reconcile_exact(&expected).await.unwrap(),
            RemoteOrderEvidence::Matched {
                making_micros: 20_000_000,
                taking_micros: 40_000_000,
                trade_ids: vec![
                    TradeId::from_exact("trade-1").unwrap(),
                    TradeId::from_exact("trade-2").unwrap(),
                ],
            }
        );
        assert_eq!(
            server.await.unwrap(),
            vec![
                exact_path,
                "GET /data/trades?id=trade-1".to_owned(),
                "GET /data/trades?id=trade-2".to_owned(),
            ]
        );
    }

    #[tokio::test]
    async fn duplicate_associations_and_ambiguous_trade_pages_stop_without_following() {
        let expected = expected_buy();
        let exact_path = format!("GET /data/order/{}", expected.order_id.as_str());
        let duplicate_association =
            order_body(&expected, "MATCHED", "40", r#"["trade-1","trade-1"]"#);
        let (host, server) = spawn_scripted_server(vec![(
            exact_path.clone(),
            "200 OK".to_owned(),
            duplicate_association,
        )])
        .await;
        let gateway =
            SdkRecoveryGateway::new_with_host(&fixture_config(), &host, Duration::from_secs(1))
                .await
                .unwrap();
        assert_eq!(
            gateway.reconcile_exact(&expected).await.unwrap(),
            RemoteOrderEvidence::Uncertain {
                code: crate::service::execution_ledger::ReconcileUncertainCode::Mismatch,
            }
        );
        assert_eq!(server.await.unwrap(), vec![exact_path.clone()]);

        let record = taker_trade_record(&expected, "trade-1", "40");
        let pages = [
            trade_page(&format!("{record},{record}"), "LTE=", 2),
            trade_page(&record, "LTE=", 2),
        ];
        for page in pages {
            let (host, server) = spawn_scripted_server(vec![
                (
                    exact_path.clone(),
                    "200 OK".to_owned(),
                    order_body(&expected, "MATCHED", "40", r#"["trade-1"]"#),
                ),
                (
                    "GET /data/trades?id=trade-1".to_owned(),
                    "200 OK".to_owned(),
                    page,
                ),
            ])
            .await;
            let gateway =
                SdkRecoveryGateway::new_with_host(&fixture_config(), &host, Duration::from_secs(1))
                    .await
                    .unwrap();
            assert_eq!(
                gateway.reconcile_exact(&expected).await.unwrap(),
                RemoteOrderEvidence::Uncertain {
                    code: crate::service::execution_ledger::ReconcileUncertainCode::Mismatch,
                }
            );
            assert_eq!(
                server.await.unwrap(),
                vec![exact_path.clone(), "GET /data/trades?id=trade-1".to_owned()]
            );
        }
    }

    #[tokio::test]
    async fn malformed_exact_trade_fields_and_relations_are_mismatch_without_search() {
        let expected = expected_buy();
        let exact_path = format!("GET /data/order/{}", expected.order_id.as_str());
        let base = taker_trade_page(&expected, "trade-1", "LTE=");
        let cases = [
            (
                "wrong_id",
                base.replacen("\"id\":\"trade-1\"", "\"id\":\"trade-other\"", 1),
            ),
            (
                "wrong_asset",
                base.replacen("\"asset_id\":\"12345\"", "\"asset_id\":\"999\"", 1),
            ),
            (
                "wrong_side",
                base.replacen("\"side\":\"BUY\"", "\"side\":\"SELL\"", 1),
            ),
            (
                "wrong_status",
                base.replacen("\"status\":\"CONFIRMED\"", "\"status\":\"MATCHED\"", 1),
            ),
            (
                "wrong_taker_relation",
                base.replacen(
                    &format!("\"taker_order_id\":\"{}\"", expected.order_id.as_str()),
                    "\"taker_order_id\":\"another-order\"",
                    1,
                ),
            ),
            (
                "amount_mismatch",
                base.replacen("\"size\":\"40\"", "\"size\":\"39\"", 1),
            ),
        ];

        for (name, trade_body) in cases {
            let (host, server) = spawn_scripted_server(vec![
                (
                    exact_path.clone(),
                    "200 OK".to_owned(),
                    order_body(&expected, "MATCHED", "40", r#"["trade-1"]"#),
                ),
                (
                    "GET /data/trades?id=trade-1".to_owned(),
                    "200 OK".to_owned(),
                    trade_body,
                ),
            ])
            .await;
            let gateway =
                SdkRecoveryGateway::new_with_host(&fixture_config(), &host, Duration::from_secs(1))
                    .await
                    .unwrap();
            assert_eq!(
                gateway.reconcile_exact(&expected).await.unwrap(),
                RemoteOrderEvidence::Uncertain {
                    code: crate::service::execution_ledger::ReconcileUncertainCode::Mismatch,
                },
                "{name}"
            );
            assert_eq!(
                server.await.unwrap(),
                vec![exact_path.clone(), "GET /data/trades?id=trade-1".to_owned()],
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn multiple_exact_trades_with_inexact_total_are_mismatch_without_replay() {
        let expected = expected_buy();
        let exact_path = format!("GET /data/order/{}", expected.order_id.as_str());
        let (host, server) = spawn_scripted_server(vec![
            (
                exact_path.clone(),
                "200 OK".to_owned(),
                order_body(&expected, "MATCHED", "40", r#"["trade-1","trade-2"]"#),
            ),
            (
                "GET /data/trades?id=trade-1".to_owned(),
                "200 OK".to_owned(),
                trade_page(&taker_trade_record(&expected, "trade-1", "20"), "LTE=", 1),
            ),
            (
                "GET /data/trades?id=trade-2".to_owned(),
                "200 OK".to_owned(),
                trade_page(&taker_trade_record(&expected, "trade-2", "19"), "LTE=", 1),
            ),
        ])
        .await;
        let gateway =
            SdkRecoveryGateway::new_with_host(&fixture_config(), &host, Duration::from_secs(1))
                .await
                .unwrap();

        assert_eq!(
            gateway.reconcile_exact(&expected).await.unwrap(),
            RemoteOrderEvidence::Uncertain {
                code: crate::service::execution_ledger::ReconcileUncertainCode::Mismatch,
            }
        );
        assert_eq!(
            server.await.unwrap(),
            vec![
                exact_path,
                "GET /data/trades?id=trade-1".to_owned(),
                "GET /data/trades?id=trade-2".to_owned(),
            ]
        );
    }

    fn taker_trade_page(
        expected: &PreparedOrderIdentity,
        trade_id: &str,
        next_cursor: &str,
    ) -> String {
        trade_page(
            &taker_trade_record(expected, trade_id, "40"),
            next_cursor,
            1,
        )
    }

    fn taker_trade_record(expected: &PreparedOrderIdentity, trade_id: &str, size: &str) -> String {
        let side = match expected.side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        };
        format!(
            r#"{{"id":"{trade_id}","taker_order_id":"{}","market":"0x0000000000000000000000000000000000000000000000000000000000000001","asset_id":"12345","side":"{side}","size":"{size}","fee_rate_bps":"0","price":"0.5","status":"CONFIRMED","match_time":"1770000000","last_update":"1770000001","outcome":"Yes","bucket_index":0,"owner":"00000000-0000-0000-0000-000000000000","maker_address":"0x0000000000000000000000000000000000000001","maker_orders":[],"transaction_hash":"0x0000000000000000000000000000000000000000000000000000000000000002","trader_side":"TAKER","error_msg":null}}"#,
            expected.order_id.as_str(),
        )
    }

    fn trade_page(records: &str, next_cursor: &str, count: usize) -> String {
        format!(r#"{{"data":[{records}],"next_cursor":"{next_cursor}","limit":1,"count":{count}}}"#)
    }

    fn maker_order(
        expected: &PreparedOrderIdentity,
        order_id: &str,
        matched_amount: &str,
    ) -> String {
        let side = match expected.side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        };
        format!(
            r#"{{"order_id":"{order_id}","owner":"00000000-0000-0000-0000-000000000000","maker_address":"0x0000000000000000000000000000000000000001","matched_amount":"{matched_amount}","price":"0.5","fee_rate_bps":"0","asset_id":"12345","outcome":"Yes","side":"{side}"}}"#
        )
    }

    fn maker_trade_page(
        expected: &PreparedOrderIdentity,
        trade_id: &str,
        taker_order_id: &str,
        maker_orders: &str,
        next_cursor: &str,
        count: usize,
    ) -> String {
        let side = match expected.side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        };
        format!(
            r#"{{"data":[{{"id":"{trade_id}","taker_order_id":"{taker_order_id}","market":"0x0000000000000000000000000000000000000000000000000000000000000001","asset_id":"12345","side":"{side}","size":"40","fee_rate_bps":"0","price":"0.5","status":"CONFIRMED","match_time":"1770000000","last_update":"1770000001","outcome":"Yes","bucket_index":0,"owner":"00000000-0000-0000-0000-000000000000","maker_address":"0x0000000000000000000000000000000000000001","maker_orders":[{maker_orders}],"transaction_hash":"0x0000000000000000000000000000000000000000000000000000000000000002","trader_side":"MAKER","error_msg":null}}],"next_cursor":"{next_cursor}","limit":1,"count":{count}}}"#
        )
    }

    fn order_body(
        expected: &PreparedOrderIdentity,
        status: &str,
        size_matched: &str,
        associate_trades: &str,
    ) -> String {
        format!(
            r#"{{"id":"{}","status":"{status}","owner":"00000000-0000-0000-0000-000000000000","maker_address":"0x0000000000000000000000000000000000000001","market":"0x0000000000000000000000000000000000000000000000000000000000000001","asset_id":"12345","side":"{}","original_size":"40","size_matched":"{size_matched}","price":"0.5","associate_trades":{associate_trades},"outcome":"Yes","created_at":1770000000,"expiration":"0","order_type":"FOK"}}"#,
            expected.order_id.as_str(),
            match expected.side {
                OrderSide::Buy => "BUY",
                OrderSide::Sell => "SELL",
            }
        )
    }

    async fn spawn_scripted_server(
        script: Vec<(String, String, String)>,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let mut requests = Vec::with_capacity(script.len());
            for (expected_request, status, body) in script {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_request_line(&mut stream).await;
                assert_eq!(request, expected_request, "unexpected loopback request");
                requests.push(request);
                write_json_response(&mut stream, &status, &body).await;
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

    #[derive(Clone, Copy)]
    enum OneRequestServerMode {
        Disconnect,
        Withhold(Duration),
    }

    async fn spawn_one_request_server(
        mode: OneRequestServerMode,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_request_line(&mut stream).await;
            if let OneRequestServerMode::Withhold(duration) = mode {
                tokio::time::sleep(duration).await;
            }
            vec![request]
        });
        (format!("http://{address}"), handle)
    }

    async fn spawn_raw_response_server(body: String) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let raw = read_raw_request(&mut stream).await;
            write_json_response(&mut stream, "200 OK", &body).await;
            raw
        });
        (format!("http://{address}"), handle)
    }

    async fn spawn_redirect_server(
        expected_request: String,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_request_line(&mut stream).await;
            assert_eq!(request, expected_request, "unexpected loopback request");
            let response = "HTTP/1.1 307 Temporary Redirect\r\nLocation: /redirect-target\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();
            if let Ok(Ok((mut extra, _))) =
                tokio::time::timeout(Duration::from_millis(100), listener.accept()).await
            {
                let extra = read_request_line(&mut extra).await;
                panic!("redirect was followed or replayed: {extra}");
            }
            vec![request]
        });
        (format!("http://{address}"), handle)
    }

    async fn read_request_line(stream: &mut tokio::net::TcpStream) -> String {
        let raw = read_raw_request(stream).await;
        let mut parts = raw.lines().next().unwrap().split_whitespace();
        let method = parts.next().unwrap();
        let target = parts.next().unwrap();
        assert_eq!(parts.next(), Some("HTTP/1.1"));
        assert_eq!(parts.next(), None);
        format!("{method} {target}")
    }

    async fn read_raw_request(stream: &mut tokio::net::TcpStream) -> String {
        let mut buffer = vec![0_u8; 16 * 1024];
        let count = stream.read(&mut buffer).await.unwrap();
        String::from_utf8(buffer[..count].to_vec()).unwrap()
    }

    async fn write_json_response(stream: &mut tokio::net::TcpStream, status: &str, body: &str) {
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.shutdown().await.unwrap();
    }
}
