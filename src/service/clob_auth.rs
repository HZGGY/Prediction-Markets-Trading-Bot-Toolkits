use std::str::FromStr as _;

use anyhow::{anyhow, Context as _, Result};
use polymarket_client_sdk_v2::auth::{Credentials, ExposeSecret as _, LocalSigner, Signer as _};
use polymarket_client_sdk_v2::clob::{Client, Config as SdkConfig};
use polymarket_client_sdk_v2::error::{Status as SdkStatus, StatusCode as SdkStatusCode};
use polymarket_client_sdk_v2::types::Address;
use polymarket_client_sdk_v2::POLYGON;

use crate::config::{AppConfig, OFFICIAL_CLOB_V2_HOST};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKeyAction {
    Create,
    Derive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthRequest {
    pub action: ApiKeyAction,
    pub nonce: Option<u32>,
}

pub fn ensure_official_v2_host(host: &str) -> Result<()> {
    if host == OFFICIAL_CLOB_V2_HOST {
        Ok(())
    } else {
        Err(anyhow!(
            "L1 authentication is restricted to the official CLOB V2 host"
        ))
    }
}

pub async fn obtain_api_credentials(cfg: &AppConfig, request: AuthRequest) -> Result<Credentials> {
    ensure_official_v2_host(&cfg.site.clob_api_base)?;
    if cfg.exchange.chain_id != POLYGON {
        return Err(anyhow!("L1 authentication requires Polygon chain id 137"));
    }
    if cfg.credentials.signature_type != Some(0) {
        return Err(anyhow!(
            "L1 authentication phase supports EOA signature_type=0 only"
        ));
    }

    let signer = LocalSigner::from_str(cfg.credentials.private_key.trim())
        .context("loading EOA signer")?
        .with_chain_id(Some(POLYGON));
    let funder =
        Address::from_str(&cfg.credentials.funder_address).context("parsing EOA funder address")?;
    if signer.address() != funder {
        return Err(anyhow!("EOA funder_address must match the signer address"));
    }

    let client = Client::new(&cfg.site.clob_api_base, SdkConfig::default())?;
    obtain_with_client(client, &signer, request).await
}

async fn obtain_with_client<S: polymarket_client_sdk_v2::auth::Signer>(
    client: Client,
    signer: &S,
    request: AuthRequest,
) -> Result<Credentials> {
    let result = match request.action {
        ApiKeyAction::Create => client.create_api_key(signer, request.nonce).await,
        ApiKeyAction::Derive => client.derive_api_key(signer, request.nonce).await,
    };
    let credentials = result.map_err(|error| {
        let (method, path) = match request.action {
            ApiKeyAction::Create => ("POST", "/auth/api-key"),
            ApiKeyAction::Derive => ("GET", "/auth/derive-api-key"),
        };
        if let Some(status) = error.downcast_ref::<SdkStatus>() {
            let suggestion = if request.action == ApiKeyAction::Create
                && status.status_code == SdkStatusCode::CONFLICT
            {
                "; the API key may already exist—run `auth derive-api-key` explicitly"
            } else {
                ""
            };
            return anyhow!(
                "CLOB L1 {method} {path} failed with HTTP {}{suggestion}",
                status.status_code
            );
        }
        anyhow!("CLOB L1 {method} {path} failed ({:?})", error.kind())
    })?;
    validate_credentials(&credentials)?;
    Ok(credentials)
}

fn validate_credentials(credentials: &Credentials) -> Result<()> {
    if credentials.key().to_string().trim().is_empty() {
        return Err(anyhow!("CLOB returned an empty API key"));
    }
    if credentials.secret().expose_secret().trim().is_empty() {
        return Err(anyhow!("CLOB returned an empty API secret"));
    }
    if credentials.passphrase().expose_secret().trim().is_empty() {
        return Err(anyhow!("CLOB returned an empty API passphrase"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::str::FromStr as _;

    use polymarket_client_sdk_v2::auth::{Credentials, LocalSigner, Signer as _, Uuid};
    use polymarket_client_sdk_v2::clob::{Client, Config as SdkConfig};
    use polymarket_client_sdk_v2::{AMOY, POLYGON};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    use super::*;
    use crate::config::AppConfig;

    const PUBLIC_HARDHAT_KEY: &str =
        "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    const CREDENTIAL_RESPONSE: &str = r#"{
  "apiKey":"00000000-0000-0000-0000-000000000000",
  "secret":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
  "passphrase":"fixture-passphrase"
}"#;

    #[derive(Debug)]
    struct CapturedRequest {
        request_line: String,
        headers: HashMap<String, String>,
    }

    async fn spawn_scripted_server(
        responses: Vec<(&'static str, &'static str)>,
    ) -> (String, tokio::task::JoinHandle<Vec<CapturedRequest>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let mut captured = Vec::with_capacity(responses.len());
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut buffer = vec![0_u8; 16 * 1024];
                let count = stream.read(&mut buffer).await.unwrap();
                let raw = String::from_utf8(buffer[..count].to_vec()).unwrap();
                let mut lines = raw.split("\r\n");
                let request_line = lines.next().unwrap().to_owned();
                let headers = lines
                    .take_while(|line| !line.is_empty())
                    .filter_map(|line| line.split_once(':'))
                    .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
                    .collect();
                captured.push(CapturedRequest {
                    request_line,
                    headers,
                });

                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            }

            if let Ok(Ok((mut stream, _))) =
                tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept()).await
            {
                let mut buffer = vec![0_u8; 16 * 1024];
                let count = stream.read(&mut buffer).await.unwrap();
                let raw = String::from_utf8(buffer[..count].to_vec()).unwrap();
                let mut lines = raw.split("\r\n");
                let request_line = lines.next().unwrap().to_owned();
                let headers = lines
                    .take_while(|line| !line.is_empty())
                    .filter_map(|line| line.split_once(':'))
                    .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
                    .collect();
                captured.push(CapturedRequest {
                    request_line,
                    headers,
                });

                let body = r#"{"error":"unexpected extra request"}"#;
                let response = format!(
                    "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            }
            captured
        });
        (format!("http://{address}"), handle)
    }

    fn hardhat_signer(chain_id: u64) -> impl polymarket_client_sdk_v2::auth::Signer {
        LocalSigner::from_str(PUBLIC_HARDHAT_KEY)
            .unwrap()
            .with_chain_id(Some(chain_id))
    }

    fn fixture_config() -> AppConfig {
        let mut cfg: AppConfig = serde_json::from_str(include_str!("../../config.json")).unwrap();
        cfg.credentials.private_key = PUBLIC_HARDHAT_KEY.to_owned();
        cfg.credentials.funder_address = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266".to_owned();
        cfg.credentials.signature_type = Some(0);
        cfg
    }

    #[test]
    fn production_auth_accepts_only_exact_official_v2_host() {
        assert!(ensure_official_v2_host(OFFICIAL_CLOB_V2_HOST).is_ok());
        for rejected in [
            "http://clob-v2.polymarket.com",
            "https://clob.polymarket.com",
            "https://clob-v2.polymarket.com.evil.example",
            "https://clob-v2.polymarket.com/extra",
        ] {
            assert!(
                ensure_official_v2_host(rejected).is_err(),
                "accepted {rejected}"
            );
        }
    }

    #[tokio::test]
    async fn rejects_non_eoa_before_any_request() {
        let mut cfg = fixture_config();
        cfg.credentials.signature_type = Some(2);
        let error = obtain_api_credentials(
            &cfg,
            AuthRequest {
                action: ApiKeyAction::Create,
                nonce: None,
            },
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("EOA"));
    }

    #[tokio::test]
    async fn create_uses_only_post_api_key_with_l1_headers_and_nonce() {
        let (host, server) = spawn_scripted_server(vec![("200 OK", CREDENTIAL_RESPONSE)]).await;
        let client = Client::new(&host, SdkConfig::default()).unwrap();
        let signer = hardhat_signer(POLYGON);

        let credentials = obtain_with_client(
            client,
            &signer,
            AuthRequest {
                action: ApiKeyAction::Create,
                nonce: Some(23),
            },
        )
        .await
        .unwrap();
        let captured = server.await.unwrap();

        assert_eq!(credentials.key(), Uuid::nil());
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].request_line, "POST /auth/api-key HTTP/1.1");
        assert_eq!(captured[0].headers["poly_nonce"], "23");
        assert_eq!(
            captured[0].headers["poly_address"],
            "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"
        );
        assert!(!captured[0].headers["poly_signature"].is_empty());
        assert!(!captured[0].headers["poly_timestamp"].is_empty());
    }

    #[tokio::test]
    async fn derive_uses_only_get_derive_api_key_with_l1_headers_and_nonce() {
        let (host, server) = spawn_scripted_server(vec![("200 OK", CREDENTIAL_RESPONSE)]).await;
        let client = Client::new(&host, SdkConfig::default()).unwrap();
        let signer = hardhat_signer(POLYGON);

        obtain_with_client(
            client,
            &signer,
            AuthRequest {
                action: ApiKeyAction::Derive,
                nonce: Some(23),
            },
        )
        .await
        .unwrap();
        let captured = server.await.unwrap();

        assert_eq!(captured.len(), 1);
        assert_eq!(
            captured[0].request_line,
            "GET /auth/derive-api-key HTTP/1.1"
        );
        assert_eq!(captured[0].headers["poly_nonce"], "23");
    }

    #[tokio::test]
    async fn create_status_error_does_not_fall_back_to_derive() {
        let leaked_body = r#"{"error":"fixture-secret-must-not-leak"}"#;
        let (host, server) = spawn_scripted_server(vec![("409 Conflict", leaked_body)]).await;
        let client = Client::new(&host, SdkConfig::default()).unwrap();
        let signer = hardhat_signer(POLYGON);

        let error = obtain_with_client(
            client,
            &signer,
            AuthRequest {
                action: ApiKeyAction::Create,
                nonce: None,
            },
        )
        .await
        .unwrap_err()
        .to_string();
        let captured = server.await.unwrap();

        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].request_line, "POST /auth/api-key HTTP/1.1");
        assert!(error.contains("409 Conflict"));
        assert!(error.contains("derive-api-key"));
        assert!(!error.contains("fixture-secret-must-not-leak"));
    }

    #[tokio::test]
    async fn official_l1_signature_vector_is_preserved_through_the_sdk_request() {
        let (host, server) = spawn_scripted_server(vec![
            ("200 OK", "10000000"),
            ("200 OK", CREDENTIAL_RESPONSE),
        ])
        .await;
        let sdk_config = SdkConfig::builder().use_server_time(true).build();
        let client = Client::new(&host, sdk_config).unwrap();
        let signer = hardhat_signer(AMOY);

        obtain_with_client(
            client,
            &signer,
            AuthRequest {
                action: ApiKeyAction::Create,
                nonce: Some(23),
            },
        )
        .await
        .unwrap();
        let captured = server.await.unwrap();

        assert_eq!(captured.len(), 2);
        assert_eq!(captured[0].request_line, "GET /time HTTP/1.1");
        assert_eq!(captured[1].headers["poly_timestamp"], "10000000");
        assert_eq!(
            captured[1].headers["poly_signature"],
            "0xf62319a987514da40e57e2f4d7529f7bac38f0355bd88bb5adbb3768d80de6c1682518e0af677d5260366425f4361e7b70c25ae232aff0ab2331e2b164a1aedc1b"
        );
    }

    #[test]
    fn empty_sdk_credential_fields_are_rejected() {
        let empty_secret =
            Credentials::new(Uuid::nil(), String::new(), "fixture-passphrase".to_owned());
        assert!(validate_credentials(&empty_secret).is_err());

        let empty_passphrase =
            Credentials::new(Uuid::nil(), "fixture-secret".to_owned(), String::new());
        assert!(validate_credentials(&empty_passphrase).is_err());
    }
}
