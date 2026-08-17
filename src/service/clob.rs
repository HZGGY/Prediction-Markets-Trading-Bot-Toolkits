//! Polymarket CLOB v2 client — EIP-712 order signing + L2 HMAC auth + POST.
//!
//! The Polymarket exchange expects orders as EIP-712 typed data signed by the
//! maker EOA (or a delegated signer for proxy/Safe accounts) against the
//! `Polymarket CTF Exchange` domain on chain 137. The signed payload is then
//! posted to `clob.polymarket.com/order` with L2 HMAC headers derived from
//! per-account API credentials.
//!
//! Safety: every public entry-point honours `enable_trading` and
//! `mock_trading`. The hot path will not transmit a signed order until both
//! flags are explicitly permissive (see [`AppConfig::live_trading_allowed`]).

use crate::config::{AppConfig, ExchangeConfig};
use crate::models::{OrderType, PlannedOrder, Side};
use alloy_primitives::{Address, B256, U256};
use alloy_signer::Signer;
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{eip712_domain, sol, Eip712Domain, SolStruct};
use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::URL_SAFE, Engine as _};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

sol! {
    /// Polymarket CTF Exchange V2 order — EIP-712 typed data.
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SignatureType {
    Eoa = 0,
    PolyProxy = 1,
    PolyGnosisSafe = 2,
    Poly1271 = 3,
}

impl SignatureType {
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Eoa),
            1 => Ok(Self::PolyProxy),
            2 => Ok(Self::PolyGnosisSafe),
            3 => Ok(Self::Poly1271),
            _ => Err(anyhow!("unsupported signature type {value}")),
        }
    }

    pub fn is_supported_for_eoa_phase(self) -> bool {
        self == Self::Eoa
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SignedOrder {
    pub salt: String,
    pub maker: String,
    pub signer: String,
    #[serde(rename = "tokenId")]
    pub token_id: String,
    #[serde(rename = "makerAmount")]
    pub maker_amount: String,
    #[serde(rename = "takerAmount")]
    pub taker_amount: String,
    pub side: String,
    pub expiration: String,
    pub timestamp: String,
    pub metadata: String,
    pub builder: String,
    #[serde(rename = "signatureType")]
    pub signature_type: u8,
    pub signature: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct OrderPostBody {
    pub order: SignedOrder,
    pub owner: String,
    #[serde(rename = "orderType")]
    pub order_type: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderResponse {
    #[serde(default, rename = "orderID")]
    pub order_id: Option<String>,
    #[serde(default)]
    pub success: bool,
    #[serde(default, rename = "errorMsg")]
    pub error_msg: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct SigningContext {
    salt: U256,
    timestamp_ms: u64,
    metadata: B256,
    builder: B256,
}

pub struct ClobClient {
    http: Client,
    clob_base: String,
    signer: PrivateKeySigner,
    funder: Address,
    exchange: ExchangeConfig,
    signature_type: SignatureType,

    api_key: Option<String>,
    api_secret: Option<String>,
    api_passphrase: Option<String>,
}

impl ClobClient {
    pub fn new(cfg: &AppConfig) -> Result<Self> {
        let pk = parse_private_key(&cfg.credentials.private_key)
            .context("loading private key")?;
        let signer = PrivateKeySigner::from_bytes(&B256::from(pk))
            .context("creating EOA signer from private key")?;
        let funder = Address::from_str(&cfg.credentials.funder_address)
            .context("parsing funder address")?;

        let signature_type_value = cfg
            .credentials
            .signature_type
            .ok_or_else(|| anyhow!("missing signature_type; set 0 for an EOA account"))?;
        let signature_type = SignatureType::from_u8(signature_type_value)?;
        if !signature_type.is_supported_for_eoa_phase() {
            return Err(anyhow!(
                "signature type {signature_type_value} is not supported in the EOA-only phase"
            ));
        }
        if funder != signer.address() {
            return Err(anyhow!(
                "EOA funder_address must match the signer address"
            ));
        }

        Ok(Self {
            http: Client::builder()
                .user_agent("polymarket-toolkits/0.1")
                .build()?,
            clob_base: cfg.site.clob_api_base.clone(),
            signer,
            funder,
            exchange: cfg.exchange.clone(),
            signature_type,
            api_key: cfg.credentials.api_key.clone(),
            api_secret: cfg.credentials.api_secret.clone(),
            api_passphrase: cfg.credentials.api_passphrase.clone(),
        })
    }

    pub fn signer_address(&self) -> Address {
        self.signer.address()
    }

    /// Build, sign, and prepare an order without posting it. Useful for unit
    /// tests and dry-run logging.
    pub async fn build_signed_order(
        &self,
        planned: &PlannedOrder,
        order_type: OrderType,
        expiration_secs: u64,
    ) -> Result<SignedOrder> {
        self.build_signed_order_with_values(
            planned,
            order_type,
            expiration_secs,
            SigningContext {
                salt: U256::from(rand::random::<u128>()),
                timestamp_ms: chrono::Utc::now().timestamp_millis().max(0) as u64,
                metadata: B256::ZERO,
                builder: B256::ZERO,
            },
        )
        .await
    }

    async fn build_signed_order_with_values(
        &self,
        planned: &PlannedOrder,
        order_type: OrderType,
        expiration_secs: u64,
        context: SigningContext,
    ) -> Result<SignedOrder> {
        let token_id_u256 = U256::from_str(&planned.token_id)
            .map_err(|_| anyhow!("token_id must be a U256 decimal"))?;
        let (maker_amount, taker_amount) =
            usd_and_share_amounts(planned.shares, planned.limit_price, planned.side);

        let expiration = match order_type {
            OrderType::Gtc => 0u64,
            _ => (chrono::Utc::now().timestamp() as u64).saturating_add(expiration_secs),
        };

        let order = Order {
            salt: context.salt,
            maker: self.funder,
            signer: self.signer.address(),
            tokenId: token_id_u256,
            makerAmount: maker_amount,
            takerAmount: taker_amount,
            side: planned.side.as_u8(),
            signatureType: self.signature_type as u8,
            timestamp: U256::from(context.timestamp_ms),
            metadata: context.metadata,
            builder: context.builder,
        };

        let exchange_address = if planned.neg_risk {
            &self.exchange.neg_risk_exchange_address
        } else {
            &self.exchange.ctf_exchange_address
        };
        let verifying_contract = Address::from_str(exchange_address)?;
        let domain: Eip712Domain = eip712_domain! {
            name: self.exchange.domain_name.clone(),
            version: self.exchange.domain_version.clone(),
            chain_id: self.exchange.chain_id,
            verifying_contract: verifying_contract,
        };
        let digest: B256 = order.eip712_signing_hash(&domain);
        let sig = self
            .signer
            .sign_hash(&digest)
            .await
            .context("signing order digest")?;

        Ok(SignedOrder {
            salt: order.salt.to_string(),
            maker: format!("0x{:x}", order.maker),
            signer: format!("0x{:x}", order.signer),
            token_id: order.tokenId.to_string(),
            maker_amount: order.makerAmount.to_string(),
            taker_amount: order.takerAmount.to_string(),
            side: side_str(planned.side).to_string(),
            expiration: expiration.to_string(),
            timestamp: order.timestamp.to_string(),
            metadata: format!("0x{:x}", order.metadata),
            builder: format!("0x{:x}", order.builder),
            signature_type: self.signature_type as u8,
            signature: format!("0x{}", hex::encode(sig.as_bytes())),
        })
    }

    /// Post a fully signed order to `/order`. Requires L2 API credentials.
    pub async fn post_order(
        &self,
        signed: SignedOrder,
        order_type: OrderType,
    ) -> Result<OrderResponse> {
        let path = "/order";
        let api_key = self
            .api_key
            .as_deref()
            .ok_or_else(|| anyhow!("L2 auth missing api_key — run the L1 sign-in flow first"))?;
        let body_json = serialize_order_request(signed, api_key, order_type)?;
        let url = format!("{}{}", self.clob_base, path);

        let headers = self.l2_headers("POST", path, &body_json)?;
        let mut req = self.http.post(&url).body(body_json);
        for (k, v) in headers {
            req = req.header(k, v);
        }
        let resp = req.send().await?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            return Err(anyhow!(
                "CLOB rejected order (HTTP {}): {}",
                status.as_u16(),
                text
            ));
        }
        let parsed: OrderResponse =
            serde_json::from_str(&text).context("parsing CLOB response")?;
        Ok(parsed)
    }

    /// Build the L2 HMAC auth headers Polymarket expects on signed endpoints.
    fn l2_headers(
        &self,
        method: &str,
        path: &str,
        body: &str,
    ) -> Result<Vec<(&'static str, String)>> {
        let api_key = self
            .api_key
            .as_ref()
            .ok_or_else(|| anyhow!("L2 auth missing api_key — run the L1 sign-in flow first"))?;
        let api_secret = self
            .api_secret
            .as_ref()
            .ok_or_else(|| anyhow!("L2 auth missing api_secret"))?;
        let api_passphrase = self
            .api_passphrase
            .as_ref()
            .ok_or_else(|| anyhow!("L2 auth missing api_passphrase"))?;

        let ts = chrono::Utc::now().timestamp().to_string();
        let prehash = format!("{ts}{method}{path}{body}");
        let signature = hmac_sha256_base64url(api_secret, &prehash)?;

        Ok(vec![
            ("POLY_ADDRESS", format!("0x{:x}", self.signer.address())),
            ("POLY_SIGNATURE", signature),
            ("POLY_TIMESTAMP", ts),
            ("POLY_API_KEY", api_key.clone()),
            ("POLY_PASSPHRASE", api_passphrase.clone()),
            ("Content-Type", "application/json".into()),
        ])
    }
}

fn usd_and_share_amounts(shares: f64, price: f64, side: Side) -> (U256, U256) {
    // pUSD and CTF shares use 6-decimal units in Polymarket CLOB V2.
    let shares_units = (shares * 1_000_000.0) as u128;
    let usd_units = ((shares * price) * 1_000_000.0) as u128;
    match side {
        // BUY: maker pays pUSD, takes shares.
        Side::Buy => (U256::from(usd_units), U256::from(shares_units)),
        // SELL: maker gives shares, takes pUSD.
        Side::Sell => (U256::from(shares_units), U256::from(usd_units)),
    }
}

fn side_str(side: Side) -> &'static str {
    match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    }
}

fn order_type_str(t: OrderType) -> &'static str {
    match t {
        OrderType::Fak => "FOK",
        OrderType::Gtd => "GTD",
        OrderType::Gtc => "GTC",
    }
}

fn serialize_order_request(
    signed: SignedOrder,
    owner: &str,
    order_type: OrderType,
) -> Result<String> {
    let body = OrderPostBody {
        order: signed,
        owner: owner.to_string(),
        order_type: order_type_str(order_type).to_string(),
    };
    serde_json::to_string(&body).context("serializing CLOB V2 order request")
}

fn parse_private_key(raw: &str) -> Result<[u8; 32]> {
    let trimmed = raw.trim().trim_start_matches("0x");
    let bytes = hex::decode(trimmed).map_err(|_| anyhow!("private key not valid hex"))?;
    if bytes.len() != 32 {
        return Err(anyhow!(
            "private key must be 32 bytes (64 hex chars), got {} bytes",
            bytes.len()
        ));
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

/// Decode the URL-safe base64 API secret, then return the HMAC-SHA256 digest
/// as padded URL-safe base64, matching the official CLOB V2 clients.
fn hmac_sha256_base64url(secret: &str, data: &str) -> Result<String> {
    let key = URL_SAFE
        .decode(secret)
        .context("API secret must be URL-safe base64")?;
    let mac = hmac_sha256(&key, data.as_bytes());
    Ok(URL_SAFE.encode(mac))
}

// --- minimal HMAC-SHA256 ----------------------------------------------------
// Vendored to avoid pulling another crate solely for this. SHA-256 itself
// comes from the std-compatible `sha2`-style impl below.

fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut k = [0u8; BLOCK];
    if key.len() > BLOCK {
        let h = sha256(key);
        k[..32].copy_from_slice(&h);
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; BLOCK];
    let mut opad = [0x5cu8; BLOCK];
    for i in 0..BLOCK {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let inner = {
        let mut buf = Vec::with_capacity(BLOCK + msg.len());
        buf.extend_from_slice(&ipad);
        buf.extend_from_slice(msg);
        sha256(&buf)
    };
    let mut outer = Vec::with_capacity(BLOCK + 32);
    outer.extend_from_slice(&opad);
    outer.extend_from_slice(&inner);
    sha256(&outer)
}

fn sha256(input: &[u8]) -> [u8; 32] {
    // Minimal SHA-256 (FIPS-180-4) — small and dependency-free.
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1,
        0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
        0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
        0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
        0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
        0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
        0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
        0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
        0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
        0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
    ];
    let mut h = [
        0x6a09e667u32, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c,
        0x1f83d9ab, 0x5be0cd19,
    ];
    // Pre-processing
    let bit_len = (input.len() as u64) * 8;
    let mut padded = Vec::with_capacity(input.len() + 72);
    padded.extend_from_slice(input);
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in padded.chunks(64) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes(chunk[i * 4..i * 4 + 4].try_into().unwrap());
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let mut a = h[0]; let mut b = h[1]; let mut c = h[2]; let mut d = h[3];
        let mut e = h[4]; let mut f = h[5]; let mut g = h[6]; let mut hh = h[7];
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ (!e & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);
            hh = g; g = f; f = e; e = d.wrapping_add(temp1);
            d = c; c = b; b = a; a = temp1.wrapping_add(temp2);
        }
        h[0] = h[0].wrapping_add(a); h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c); h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e); h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g); h[7] = h[7].wrapping_add(hh);
    }
    let mut out = [0u8; 32];
    for i in 0..8 {
        out[i * 4..i * 4 + 4].copy_from_slice(&h[i].to_be_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::VenueId;

    fn fixture_config(signature_type: Option<u8>) -> AppConfig {
        let mut cfg: AppConfig = serde_json::from_str(include_str!("../../config.json")).unwrap();
        cfg.credentials.private_key =
            "0000000000000000000000000000000000000000000000000000000000000001".into();
        cfg.credentials.funder_address =
            "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf".into();
        cfg.credentials.signature_type = signature_type;
        cfg
    }

    fn fixture_signed_order() -> SignedOrder {
        SignedOrder {
            salt: "1".into(),
            maker: "0x1111111111111111111111111111111111111111".into(),
            signer: "0x1111111111111111111111111111111111111111".into(),
            token_id: "2".into(),
            maker_amount: "500000".into(),
            taker_amount: "1000000".into(),
            side: "BUY".into(),
            expiration: "0".into(),
            timestamp: "1713398400000".into(),
            metadata: format!("0x{}", "00".repeat(32)),
            builder: format!("0x{}", "00".repeat(32)),
            signature_type: 0,
            signature: "0x1234".into(),
        }
    }

    fn fixture_planned_order() -> PlannedOrder {
        PlannedOrder {
            venue: VenueId::Polymarket,
            token_id: "12345678901234567890".into(),
            neg_risk: false,
            side: Side::Buy,
            shares: 1.0,
            limit_price: 0.5,
            usd_notional: 0.5,
            order_type: OrderType::Gtc,
            source_trade_hash: None,
        }
    }

    fn fixture_signing_context(timestamp_ms: u64, metadata: B256) -> SigningContext {
        SigningContext {
            salt: U256::from(42u64),
            timestamp_ms,
            metadata,
            builder: B256::ZERO,
        }
    }

    #[test]
    fn parses_all_known_polymarket_signature_types() {
        assert_eq!(SignatureType::from_u8(0).unwrap(), SignatureType::Eoa);
        assert_eq!(
            SignatureType::from_u8(1).unwrap(),
            SignatureType::PolyProxy
        );
        assert_eq!(
            SignatureType::from_u8(2).unwrap(),
            SignatureType::PolyGnosisSafe
        );
        assert_eq!(
            SignatureType::from_u8(3).unwrap(),
            SignatureType::Poly1271
        );
    }

    #[test]
    fn rejects_unknown_signature_type() {
        let error = SignatureType::from_u8(9).unwrap_err();
        assert!(error.to_string().contains("unsupported signature type 9"));
    }

    #[test]
    fn only_eoa_is_supported_in_first_phase() {
        assert!(SignatureType::Eoa.is_supported_for_eoa_phase());
        assert!(!SignatureType::PolyProxy.is_supported_for_eoa_phase());
        assert!(!SignatureType::PolyGnosisSafe.is_supported_for_eoa_phase());
        assert!(!SignatureType::Poly1271.is_supported_for_eoa_phase());
    }

    #[test]
    fn rejects_missing_explicit_signature_type() {
        let error = match ClobClient::new(&fixture_config(None)) {
            Ok(_) => panic!("client unexpectedly inferred a signature type"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("missing signature_type"));
    }

    #[test]
    fn rejects_proxy_signature_type_before_signing() {
        let error = match ClobClient::new(&fixture_config(Some(1))) {
            Ok(_) => panic!("proxy signature type unexpectedly enabled"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("signature type 1 is not supported in the EOA-only phase"));
    }

    #[test]
    fn rejects_eoa_when_funder_differs_from_signer() {
        let mut cfg = fixture_config(Some(0));
        cfg.credentials.funder_address =
            "0x1111111111111111111111111111111111111111".into();
        let error = match ClobClient::new(&cfg) {
            Ok(_) => panic!("EOA client unexpectedly accepted a different funder"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("EOA funder_address must match the signer address"));
    }

    #[test]
    fn v2_signed_order_json_excludes_v1_fields() {
        let json = serde_json::to_value(fixture_signed_order()).unwrap();
        let order = json.as_object().unwrap();

        for legacy in ["taker", "nonce", "feeRateBps"] {
            assert!(!order.contains_key(legacy), "legacy field remained: {legacy}");
        }
        for v2 in ["timestamp", "metadata", "builder", "expiration"] {
            assert!(order.contains_key(v2), "V2 wire field missing: {v2}");
        }
    }

    #[test]
    fn v2_eip712_root_type_matches_exchange_contract() {
        assert_eq!(
            Order::eip712_root_type().as_ref(),
            "Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)"
        );
    }

    #[tokio::test]
    async fn v2_fixed_order_matches_known_digest_and_signature() {
        let client = ClobClient::new(&fixture_config(Some(0))).unwrap();
        let signed = client
            .build_signed_order_with_values(
                &fixture_planned_order(),
                OrderType::Gtc,
                60,
                fixture_signing_context(1_713_398_400_000u64, B256::ZERO),
            )
            .await
            .unwrap();

        assert_eq!(signed.salt, "42");
        assert_eq!(signed.timestamp, "1713398400000");
        assert_eq!(
            signed.signature,
            "0x49a3c751eb94c4e96efa077ef503eee0892bbe8df6790e3139fb528bdca214903dce18b94802352eb25cfd9644bb032a164fac51c686bac42831f90fd69d20411b"
        );

        let signer = PrivateKeySigner::from_bytes(&B256::from([
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 1,
        ]))
        .unwrap();
        let order = Order {
            salt: U256::from(42u64),
            maker: signer.address(),
            signer: signer.address(),
            tokenId: U256::from(12_345_678_901_234_567_890u128),
            makerAmount: U256::from(500_000u64),
            takerAmount: U256::from(1_000_000u64),
            side: 0,
            signatureType: 0,
            timestamp: U256::from(1_713_398_400_000u64),
            metadata: B256::ZERO,
            builder: B256::ZERO,
        };
        let domain = eip712_domain! {
            name: "Polymarket CTF Exchange".to_string(),
            version: "2".to_string(),
            chain_id: 137u64,
            verifying_contract: Address::from_str("0xE111180000d2663C0091e4f400237545B87B996B").unwrap(),
        };
        let digest = order.eip712_signing_hash(&domain);
        assert_eq!(
            format!("{digest:#x}"),
            "0xf50c40827be812ab26c5e9e3558946824a450ba302e8231a36f8316ac18c424e"
        );

        let signature =
            alloy_primitives::PrimitiveSignature::from_str(&signed.signature).unwrap();
        assert_eq!(
            signature.recover_address_from_prehash(&digest).unwrap(),
            signer.address()
        );
    }

    #[tokio::test]
    async fn v2_timestamp_changes_signature() {
        let client = ClobClient::new(&fixture_config(Some(0))).unwrap();
        let first = client
            .build_signed_order_with_values(
                &fixture_planned_order(),
                OrderType::Gtc,
                60,
                fixture_signing_context(1_713_398_400_000u64, B256::ZERO),
            )
            .await
            .unwrap();
        let second = client
            .build_signed_order_with_values(
                &fixture_planned_order(),
                OrderType::Gtc,
                60,
                fixture_signing_context(1_713_398_400_001u64, B256::ZERO),
            )
            .await
            .unwrap();

        assert_ne!(first.signature, second.signature);
    }

    #[tokio::test]
    async fn v2_domain_and_metadata_changes_signature() {
        let base_client = ClobClient::new(&fixture_config(Some(0))).unwrap();
        let base = base_client
            .build_signed_order_with_values(
                &fixture_planned_order(),
                OrderType::Gtc,
                60,
                fixture_signing_context(1_713_398_400_000u64, B256::ZERO),
            )
            .await
            .unwrap();

        let mut version_cfg = fixture_config(Some(0));
        version_cfg.exchange.domain_version = "1".into();
        let version_client = ClobClient::new(&version_cfg).unwrap();
        let changed_version = version_client
            .build_signed_order_with_values(
                &fixture_planned_order(),
                OrderType::Gtc,
                60,
                fixture_signing_context(1_713_398_400_000u64, B256::ZERO),
            )
            .await
            .unwrap();

        let mut contract_cfg = fixture_config(Some(0));
        contract_cfg.exchange.ctf_exchange_address =
            "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E".into();
        let contract_client = ClobClient::new(&contract_cfg).unwrap();
        let changed_contract = contract_client
            .build_signed_order_with_values(
                &fixture_planned_order(),
                OrderType::Gtc,
                60,
                fixture_signing_context(1_713_398_400_000u64, B256::ZERO),
            )
            .await
            .unwrap();

        let changed_metadata = base_client
            .build_signed_order_with_values(
                &fixture_planned_order(),
                OrderType::Gtc,
                60,
                fixture_signing_context(1_713_398_400_000u64, B256::from([1u8; 32])),
            )
            .await
            .unwrap();

        assert_ne!(base.signature, changed_version.signature);
        assert_ne!(base.signature, changed_contract.signature);
        assert_ne!(base.signature, changed_metadata.signature);
    }

    #[tokio::test]
    async fn v2_neg_risk_market_uses_neg_risk_exchange() {
        let client = ClobClient::new(&fixture_config(Some(0))).unwrap();
        let standard = client
            .build_signed_order_with_values(
                &fixture_planned_order(),
                OrderType::Gtc,
                60,
                fixture_signing_context(1_713_398_400_000u64, B256::ZERO),
            )
            .await
            .unwrap();
        let mut neg_risk_order = fixture_planned_order();
        neg_risk_order.neg_risk = true;
        let neg_risk = client
            .build_signed_order_with_values(
                &neg_risk_order,
                OrderType::Gtc,
                60,
                fixture_signing_context(1_713_398_400_000u64, B256::ZERO),
            )
            .await
            .unwrap();

        assert_ne!(standard.signature, neg_risk.signature);
    }

    #[test]
    fn serializes_v2_request_with_owner_and_order_type() {
        let body = serialize_order_request(
            fixture_signed_order(),
            "api-key-fixture",
            OrderType::Gtc,
        )
        .unwrap();
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();

        assert_eq!(json["owner"], "api-key-fixture");
        assert_eq!(json["orderType"], "GTC");
        assert!(json["order"]["timestamp"].is_string());
        assert!(json["order"]["expiration"].is_string());
        assert!(json["order"].get("nonce").is_none());
        assert!(json["order"].get("feeRateBps").is_none());
    }

    #[test]
    fn sha256_known_vectors() {
        // "abc" → ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
        let h = sha256(b"abc");
        assert_eq!(
            hex::encode(h),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn hmac_rfc4231_test_1() {
        // RFC 4231 §4.2 — HMAC-SHA-256 of "Hi There" with 20×0x0b key.
        let key = vec![0x0bu8; 20];
        let mac = hmac_sha256(&key, b"Hi There");
        assert_eq!(
            hex::encode(mac),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
    }

    #[test]
    fn l2_hmac_decodes_api_secret_and_keeps_base64_padding() {
        // Official clients treat the API secret as URL-safe base64 encoded.
        // "c2VjcmV0a2V5" decodes to the HMAC key "secretkey".
        let signature =
            hmac_sha256_base64url("c2VjcmV0a2V5", "1000POST/order{}").unwrap();

        assert_eq!(signature, "l_GD1L6lBLUTXQ4OwhciHGDBd2nw2iP7K2dGQkYW4ls=");
    }

    #[test]
    fn l2_hmac_rejects_invalid_api_secret_encoding() {
        let error = hmac_sha256_base64url("not base64!", "1000POST/order{}")
            .unwrap_err()
            .to_string();

        assert!(error.contains("API secret must be URL-safe base64"));
    }
}
