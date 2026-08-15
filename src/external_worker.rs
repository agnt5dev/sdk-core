//! Shared bootstrap for workers running outside an AGNT5-managed data plane.
//!
//! Language bindings opt into this path with `AGNT5_API_KEY_FILE`, explicit
//! `AGNT5_EXTERNAL_WORKER=true`, or an API key without legacy runtime IDs and
//! endpoints. The core discovers immutable placement authority,
//! exchanges the long-lived bootstrap key for a short-lived workload token,
//! and refreshes that token without exposing endpoint or tenant wiring to each
//! language SDK.

use crate::error::{ErrorCode, Result, SdkError};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

const DEFAULT_CONTROL_PLANE_URL: &str = "https://api.agnt5.com";
const TOKEN_REFRESH_SKEW_SECONDS: i64 = 60;
pub const EXTERNAL_WORKER_PROTOCOL_PULL_V1: &str = "pull.v1";

#[derive(Clone, Debug)]
enum CredentialProvider {
    Inline(Arc<str>),
    File(Arc<Path>),
}

impl CredentialProvider {
    async fn load(&self) -> Result<String> {
        let value =
            match self {
                Self::Inline(value) => value.to_string(),
                Self::File(path) => tokio::fs::read_to_string(path).await.map_err(|error| {
                    SdkError::Configuration {
                        message: format!(
                            "failed to read AGNT5 API key file {}: {error}",
                            path.display()
                        ),
                        field: Some("AGNT5_API_KEY_FILE".to_string()),
                    }
                })?,
            };
        let value = value.trim().to_string();
        if value.is_empty() {
            return Err(SdkError::Configuration {
                message: "AGNT5 bootstrap credential is empty".to_string(),
                field: Some(
                    match self {
                        Self::Inline(_) => "AGNT5_API_KEY",
                        Self::File(_) => "AGNT5_API_KEY_FILE",
                    }
                    .to_string(),
                ),
            });
        }
        Ok(value)
    }
}

/// Configuration shared by every language binding using SDK core.
#[derive(Clone, Debug)]
pub struct ExternalWorkerBootstrapConfig {
    control_plane_url: Url,
    environment: Option<String>,
    credential: CredentialProvider,
}

impl ExternalWorkerBootstrapConfig {
    /// Resolve external-worker bootstrap configuration from environment.
    /// Returns `None` when neither credential source is configured, preserving
    /// the existing managed/local worker path.
    pub fn from_env() -> Result<Option<Self>> {
        Self::from_env_with_legacy_coordinates(false)
    }

    pub(crate) fn from_env_with_legacy_coordinates(
        programmatic_legacy_coordinates: bool,
    ) -> Result<Option<Self>> {
        let key_file = std::env::var("AGNT5_API_KEY_FILE")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let inline_key = std::env::var("AGNT5_API_KEY")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let explicitly_external = std::env::var("AGNT5_EXTERNAL_WORKER")
            .ok()
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes"
                )
            })
            .unwrap_or(false);
        let has_legacy_coordinates = [
            "AGNT5_COORDINATOR_ENDPOINT",
            "AGNT5_ENGINE_URL",
            "AGNT5_EE_ENDPOINT",
            "AGNT5_PROJECT_ID",
            "AGNT5_DEPLOYMENT_ID",
        ]
        .iter()
        .any(|name| {
            std::env::var(name)
                .ok()
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false)
        });
        let has_legacy_coordinates = has_legacy_coordinates || programmatic_legacy_coordinates;
        let external_requested = key_file.is_some()
            || explicitly_external
            || (inline_key.is_some() && !has_legacy_coordinates);
        if !external_requested {
            return Ok(None);
        }
        let credential = match (key_file, inline_key) {
            (Some(_), Some(_)) => {
                return Err(SdkError::Configuration {
                    message: "configure only one of AGNT5_API_KEY_FILE or AGNT5_API_KEY"
                        .to_string(),
                    field: Some("AGNT5_API_KEY_FILE".to_string()),
                })
            }
            (Some(path), None) => CredentialProvider::File(Arc::from(PathBuf::from(path))),
            (None, Some(value)) => CredentialProvider::Inline(Arc::from(value)),
            (None, None) if explicitly_external => {
                return Err(SdkError::Configuration {
                    message: "AGNT5_EXTERNAL_WORKER requires AGNT5_API_KEY_FILE or AGNT5_API_KEY"
                        .to_string(),
                    field: Some("AGNT5_API_KEY_FILE".to_string()),
                })
            }
            (None, None) => return Ok(None),
        };
        let control_plane_url = std::env::var("AGNT5_CONTROL_PLANE_URL")
            .unwrap_or_else(|_| DEFAULT_CONTROL_PLANE_URL.to_string());
        let environment = std::env::var("AGNT5_ENVIRONMENT")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        Ok(Some(Self::new(control_plane_url, environment, credential)?))
    }

    fn new(
        control_plane_url: String,
        environment: Option<String>,
        credential: CredentialProvider,
    ) -> Result<Self> {
        let control_plane_url = validated_endpoint(&control_plane_url, "control plane")?;
        Ok(Self {
            control_plane_url,
            environment,
            credential,
        })
    }

    #[cfg(test)]
    fn inline(
        control_plane_url: &str,
        environment: Option<&str>,
        credential: &str,
    ) -> Result<Self> {
        Self::new(
            control_plane_url.to_string(),
            environment.map(str::to_string),
            CredentialProvider::Inline(Arc::from(credential)),
        )
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct ExternalWorkerConnection {
    pub project_id: String,
    pub environment_id: String,
    pub deployment_id: String,
    pub worker_pool_id: String,
    pub placement: String,
    pub runtime_endpoint: String,
    pub protocol: String,
}

#[derive(Debug, Serialize)]
struct DiscoveryRequest<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    environment: Option<&'a str>,
}

#[derive(Debug, Serialize)]
struct TokenExchangeRequest<'a> {
    project_id: &'a str,
    environment_id: &'a str,
    deployment_id: &'a str,
    worker_pool_id: &'a str,
}

#[derive(Clone, Debug, Deserialize)]
struct TokenExchangeResponse {
    workload_token: String,
    token_type: String,
    expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
struct CachedToken {
    value: Arc<str>,
    expires_at: DateTime<Utc>,
}

/// Resolved external-worker placement plus a single-flight refreshable token.
#[derive(Debug)]
pub struct ExternalWorkerSession {
    config: ExternalWorkerBootstrapConfig,
    client: Client,
    connection: ExternalWorkerConnection,
    token: RwLock<CachedToken>,
    refresh: Mutex<()>,
}

impl ExternalWorkerSession {
    pub async fn connect(config: ExternalWorkerBootstrapConfig) -> Result<Arc<Self>> {
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|error| bootstrap_connection_error("build HTTP client", error))?;
        Self::connect_with_client(config, client).await
    }

    async fn connect_with_client(
        config: ExternalWorkerBootstrapConfig,
        client: Client,
    ) -> Result<Arc<Self>> {
        let credential = config.credential.load().await?;
        let connection = discover(&client, &config, &credential).await?;
        validate_connection(&connection)?;
        let token = exchange_token(&client, &config, &credential, &connection).await?;
        Ok(Arc::new(Self {
            config,
            client,
            connection,
            token: RwLock::new(token),
            refresh: Mutex::new(()),
        }))
    }

    pub fn connection(&self) -> &ExternalWorkerConnection {
        &self.connection
    }

    /// Return a valid workload token, refreshing under one mutex when it is
    /// within the refresh window. Concurrent poll slots reuse the result.
    pub async fn workload_token(&self) -> Result<String> {
        if let Some(value) = self.valid_cached_token().await {
            return Ok(value);
        }
        let _refresh = self.refresh.lock().await;
        if let Some(value) = self.valid_cached_token().await {
            return Ok(value);
        }
        let credential = self.config.credential.load().await?;
        let refreshed =
            exchange_token(&self.client, &self.config, &credential, &self.connection).await?;
        let value = refreshed.value.to_string();
        *self.token.write().await = refreshed;
        Ok(value)
    }

    async fn valid_cached_token(&self) -> Option<String> {
        let token = self.token.read().await;
        (token.expires_at > Utc::now() + ChronoDuration::seconds(TOKEN_REFRESH_SKEW_SECONDS))
            .then(|| token.value.to_string())
    }
}

async fn discover(
    client: &Client,
    config: &ExternalWorkerBootstrapConfig,
    credential: &str,
) -> Result<ExternalWorkerConnection> {
    let endpoint = config
        .control_plane_url
        .join("api/v1/worker-discovery")
        .map_err(|error| bootstrap_configuration_error("worker discovery URL", error))?;
    let response = client
        .post(endpoint)
        .header("X-API-KEY", credential)
        .json(&DiscoveryRequest {
            environment: config.environment.as_deref(),
        })
        .send()
        .await
        .map_err(|error| bootstrap_connection_error("worker discovery", error))?;
    decode_response(response, "worker discovery").await
}

async fn exchange_token(
    client: &Client,
    config: &ExternalWorkerBootstrapConfig,
    credential: &str,
    connection: &ExternalWorkerConnection,
) -> Result<CachedToken> {
    let endpoint = config
        .control_plane_url
        .join("api/v1/worker-token")
        .map_err(|error| bootstrap_configuration_error("worker token URL", error))?;
    let response = client
        .post(endpoint)
        .header("X-API-KEY", credential)
        .json(&TokenExchangeRequest {
            project_id: &connection.project_id,
            environment_id: &connection.environment_id,
            deployment_id: &connection.deployment_id,
            worker_pool_id: &connection.worker_pool_id,
        })
        .send()
        .await
        .map_err(|error| bootstrap_connection_error("worker token exchange", error))?;
    let response: TokenExchangeResponse =
        decode_response(response, "worker token exchange").await?;
    if response.workload_token.trim().is_empty()
        || !response.token_type.eq_ignore_ascii_case("bearer")
        || response.expires_at <= Utc::now()
    {
        return Err(SdkError::Configuration {
            message: "worker token exchange returned an invalid credential".to_string(),
            field: None,
        });
    }
    Ok(CachedToken {
        value: Arc::from(response.workload_token),
        expires_at: response.expires_at,
    })
}

async fn decode_response<T: for<'de> Deserialize<'de>>(
    response: reqwest::Response,
    operation: &str,
) -> Result<T> {
    let status = response.status();
    if !status.is_success() {
        let error_code = response
            .json::<serde_json::Value>()
            .await
            .ok()
            .and_then(|body| {
                body.get("error")
                    .and_then(|value| value.as_str())
                    .map(str::to_string)
            })
            .unwrap_or_else(|| "request_failed".to_string());
        let code = match status {
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => ErrorCode::InvalidConfiguration,
            StatusCode::TOO_MANY_REQUESTS => ErrorCode::ResourceExhausted,
            StatusCode::CONFLICT | StatusCode::SERVICE_UNAVAILABLE => ErrorCode::ServiceUnavailable,
            _ if status.is_server_error() => ErrorCode::ServiceUnavailable,
            _ => ErrorCode::InvalidInput,
        };
        return Err(SdkError::Connection {
            message: format!("{operation} failed with status {status}: {error_code}"),
            code,
            source: None,
        });
    }
    response
        .json::<T>()
        .await
        .map_err(|error| bootstrap_connection_error(&format!("decode {operation} response"), error))
}

fn validate_connection(connection: &ExternalWorkerConnection) -> Result<()> {
    if connection.project_id.is_empty()
        || connection.environment_id.is_empty()
        || connection.deployment_id.is_empty()
        || connection.worker_pool_id.is_empty()
    {
        return Err(SdkError::Configuration {
            message: "worker discovery omitted immutable placement authority".to_string(),
            field: None,
        });
    }
    if connection.protocol != EXTERNAL_WORKER_PROTOCOL_PULL_V1 {
        return Err(SdkError::Configuration {
            message: format!(
                "unsupported external worker protocol: {}",
                connection.protocol
            ),
            field: None,
        });
    }
    if !matches!(
        connection.placement.as_str(),
        "customer_docker" | "customer_kubernetes"
    ) {
        return Err(SdkError::Configuration {
            message: format!(
                "worker discovery returned non-external placement: {}",
                connection.placement
            ),
            field: None,
        });
    }
    validated_endpoint(&connection.runtime_endpoint, "runtime")?;
    Ok(())
}

fn validated_endpoint(raw: &str, name: &str) -> Result<Url> {
    let endpoint = Url::parse(raw).map_err(|error| bootstrap_configuration_error(name, error))?;
    let host = endpoint.host_str().ok_or_else(|| SdkError::Configuration {
        message: format!("{name} endpoint has no host"),
        field: None,
    })?;
    if !endpoint.username().is_empty() || endpoint.password().is_some() {
        return Err(SdkError::Configuration {
            message: format!("{name} endpoint must not contain user info"),
            field: None,
        });
    }
    match endpoint.scheme() {
        "https" => Ok(endpoint),
        "http" if is_loopback(host) => Ok(endpoint),
        "http" => Err(SdkError::Configuration {
            message: format!("{name} endpoint must use verified TLS outside local development"),
            field: None,
        }),
        _ => Err(SdkError::Configuration {
            message: format!("{name} endpoint must use https"),
            field: None,
        }),
    }
}

fn is_loopback(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .map(|address| address.is_loopback())
            .unwrap_or(false)
}

fn bootstrap_connection_error(
    operation: &str,
    error: impl std::error::Error + Send + Sync + 'static,
) -> SdkError {
    SdkError::Connection {
        message: format!("{operation} failed: {error}"),
        code: ErrorCode::ConnectionFailed,
        source: Some(Box::new(error)),
    }
}

fn bootstrap_configuration_error(field: &str, error: impl std::error::Error) -> SdkError {
    SdkError::Configuration {
        message: format!("invalid {field}: {error}"),
        field: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn connection(endpoint: &str) -> ExternalWorkerConnection {
        ExternalWorkerConnection {
            project_id: "project-1".to_string(),
            environment_id: "environment-1".to_string(),
            deployment_id: "deployment-1".to_string(),
            worker_pool_id: "external-deployment-1".to_string(),
            placement: "customer_docker".to_string(),
            runtime_endpoint: endpoint.to_string(),
            protocol: EXTERNAL_WORKER_PROTOCOL_PULL_V1.to_string(),
        }
    }

    #[test]
    fn requires_tls_outside_loopback() {
        assert!(validated_endpoint("https://runtime.agnt5.com", "runtime").is_ok());
        assert!(validated_endpoint("http://localhost:34182", "runtime").is_ok());
        assert!(validated_endpoint("http://127.0.0.1:34182", "runtime").is_ok());
        assert!(validated_endpoint("http://runtime.agnt5.com:34182", "runtime").is_err());
        assert!(validated_endpoint("https://user:password@runtime.agnt5.com", "runtime").is_err());
    }

    #[test]
    fn validates_immutable_pull_connection() {
        assert!(validate_connection(&connection("https://runtime.agnt5.com")).is_ok());
        let mut invalid = connection("https://runtime.agnt5.com");
        invalid.protocol = "push.v1".to_string();
        assert!(validate_connection(&invalid).is_err());
        invalid.protocol = EXTERNAL_WORKER_PROTOCOL_PULL_V1.to_string();
        invalid.deployment_id.clear();
        assert!(validate_connection(&invalid).is_err());
        let mut managed = connection("https://runtime.agnt5.com");
        managed.placement = "managed".to_string();
        assert!(validate_connection(&managed).is_err());
    }

    #[tokio::test]
    async fn inline_credential_is_trimmed() {
        let config = ExternalWorkerBootstrapConfig::inline(
            "http://localhost:34181",
            Some("production"),
            "  agnt5_sk_test  ",
        )
        .expect("config");
        assert_eq!(
            config.credential.load().await.expect("credential"),
            "agnt5_sk_test"
        );
        assert_eq!(config.environment.as_deref(), Some("production"));
    }

    #[tokio::test]
    async fn discovers_exchanges_and_refreshes_without_language_endpoint_config() {
        let soon = (Utc::now() + ChronoDuration::seconds(30)).to_rfc3339();
        let fresh = (Utc::now() + ChronoDuration::minutes(5)).to_rfc3339();
        let discovery = serde_json::json!({
            "project_id": "project-1",
            "environment_id": "environment-1",
            "deployment_id": "deployment-1",
            "worker_pool_id": "external-deployment-1",
            "placement": "customer_docker",
            "runtime_endpoint": "http://127.0.0.1:34182",
            "protocol": "pull.v1"
        })
        .to_string();
        let first_token = serde_json::json!({
            "workload_token": "first-token",
            "token_type": "Bearer",
            "expires_at": soon
        })
        .to_string();
        let refreshed_token = serde_json::json!({
            "workload_token": "refreshed-token",
            "token_type": "Bearer",
            "expires_at": fresh
        })
        .to_string();
        let (control_plane_url, server) =
            spawn_json_server(vec![discovery, first_token, refreshed_token]).await;
        let config = ExternalWorkerBootstrapConfig::inline(
            &control_plane_url,
            Some("production"),
            "agnt5_sk_test",
        )
        .expect("config");

        let session = ExternalWorkerSession::connect(config)
            .await
            .expect("external worker bootstrap");
        assert_eq!(session.connection().project_id, "project-1");
        assert_eq!(
            session.workload_token().await.expect("refresh token"),
            "refreshed-token"
        );
        assert_eq!(
            session.workload_token().await.expect("reuse token"),
            "refreshed-token"
        );

        let requests = server.await.expect("server task");
        assert_eq!(requests.len(), 3);
        assert!(requests[0].starts_with("POST /api/v1/worker-discovery "));
        assert!(requests[0]
            .to_ascii_lowercase()
            .contains("x-api-key: agnt5_sk_test"));
        assert!(requests[0].contains(r#"{"environment":"production"}"#));
        assert!(requests[1].starts_with("POST /api/v1/worker-token "));
        assert!(requests[1].contains(r#""worker_pool_id":"external-deployment-1""#));
        assert!(requests[2].starts_with("POST /api/v1/worker-token "));
    }

    async fn spawn_json_server(
        responses: Vec<String>,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("local address");
        let handle = tokio::spawn(async move {
            let mut requests = Vec::with_capacity(responses.len());
            for body in responses {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let request = read_http_request(&mut stream).await;
                requests.push(request);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream
                    .write_all(response.as_bytes())
                    .await
                    .expect("write response");
            }
            requests
        });
        (format!("http://{address}/"), handle)
    }

    async fn read_http_request(stream: &mut tokio::net::TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 1024];
        loop {
            let read = stream.read(&mut buffer).await.expect("read request");
            if read == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..read]);
            let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
                continue;
            };
            let headers = String::from_utf8_lossy(&bytes[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            if bytes.len() >= header_end + 4 + content_length {
                break;
            }
        }
        String::from_utf8(bytes).expect("UTF-8 request")
    }
}
