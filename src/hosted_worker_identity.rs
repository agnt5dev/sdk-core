//! Runtime credentials for workers that AGNT5 runs.
//!
//! A hosted worker pod has no service key. It proves who it is with the
//! Kubernetes service-account token projected into the pod, which the
//! workload broker exchanges for a short-lived runtime token. Kubelet rotates
//! the projected token, so every exchange reads the file again.

use crate::client::{spawn_token_refresh, SharedBearer, TokenGrant};
use crate::error::{ErrorCode, Result, SdkError};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{info, warn};

pub(crate) const WORKLOAD_BROKER_URL_ENV: &str = "AGNT5_WORKLOAD_BROKER_URL";
pub(crate) const WORKLOAD_TOKEN_FILE_ENV: &str = "AGNT5_WORKLOAD_TOKEN_FILE";
const TOKEN_EXCHANGE_PATH: &str = "/internal/workload/token";
const RUNTIME_AUDIENCE: &str = "agnt5-runtime";

// The broker is in-cluster, so a slow answer means it is unhealthy; fail fast
// and let the refresh loop try again rather than stall a worker's connect.
const BROKER_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const BROKER_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
// One quick retry rides out a broker restart or rollout.
const BROKER_RETRY_DELAY: Duration = Duration::from_millis(250);
const REFRESH_RETRY_DELAY: Duration = Duration::from_secs(5);

/// Where a hosted worker trades its pod identity for a runtime token.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HostedWorkloadIdentity {
    pub(crate) broker_url: String,
    pub(crate) token_file: PathBuf,
}

impl HostedWorkloadIdentity {
    /// Both variables are set by AGNT5's worker pod spec; with either missing
    /// the worker is not AGNT5-hosted.
    pub(crate) fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Option<Self> {
        let present = |name| lookup(name).filter(|value: &String| !value.trim().is_empty());
        let broker_url = present(WORKLOAD_BROKER_URL_ENV)?;
        let token_file = present(WORKLOAD_TOKEN_FILE_ENV)?;
        Some(Self {
            broker_url: broker_url.trim().trim_end_matches('/').to_string(),
            token_file: PathBuf::from(token_file.trim()),
        })
    }
}

#[derive(Serialize)]
struct TokenExchangeRequest<'a> {
    token: &'a str,
    audience: [&'a str; 1],
}

#[derive(Deserialize)]
struct TokenExchangeResponse {
    token: String,
    expires_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone)]
struct BrokerClient {
    http: reqwest::Client,
    identity: HostedWorkloadIdentity,
}

impl BrokerClient {
    fn new(identity: HostedWorkloadIdentity) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(BROKER_REQUEST_TIMEOUT)
            .connect_timeout(BROKER_CONNECT_TIMEOUT)
            .build()
            .map_err(|error| connection_error(format!("build workload broker client: {error}")))?;
        Ok(Self { http, identity })
    }

    /// Exchange the pod's current projected token for a runtime token.
    async fn exchange(&self) -> Result<TokenGrant> {
        let path = &self.identity.token_file;
        let projected = tokio::fs::read_to_string(path).await.map_err(|error| {
            connection_error(format!(
                "read workload token file {}: {error}",
                path.display()
            ))
        })?;
        let projected = projected.trim();
        if projected.is_empty() {
            return Err(connection_error(format!(
                "workload token file {} is empty",
                path.display()
            )));
        }
        let url = format!("{}{}", self.identity.broker_url, TOKEN_EXCHANGE_PATH);
        let body = TokenExchangeRequest {
            token: projected,
            audience: [RUNTIME_AUDIENCE],
        };
        let started = Instant::now();
        let mut attempt = 0;
        let (status, text) = loop {
            attempt += 1;
            let result = match self.http.post(&url).json(&body).send().await {
                Ok(response) => {
                    let status = response.status();
                    response.text().await.map(|text| (status, text))
                }
                Err(error) => Err(error),
            };
            let unavailable = match &result {
                Ok((status, _)) => matches!(status.as_u16(), 502..=504),
                Err(_) => true,
            };
            if unavailable && attempt == 1 {
                tokio::time::sleep(BROKER_RETRY_DELAY).await;
                continue;
            }
            break result.map_err(|error| {
                connection_error(format!(
                    "workload token exchange failed after {}ms: {error}",
                    started.elapsed().as_millis()
                ))
            })?;
        };
        if !status.is_success() {
            // The broker's error body names the refusal and never echoes the
            // token, so it is safe to log.
            return Err(connection_error(format!(
                "workload token exchange returned {status}: {}",
                truncate(&text, 200)
            )));
        }
        let response: TokenExchangeResponse = serde_json::from_str(&text).map_err(|error| {
            connection_error(format!("decode workload token exchange response: {error}"))
        })?;
        if response.token.is_empty() {
            return Err(connection_error(
                "workload token exchange returned an empty token",
            ));
        }
        Ok(TokenGrant {
            token: response.token,
            expires_at: response.expires_at,
        })
    }
}

/// Fetch the first runtime token and keep it fresh in the background.
///
/// A broker that is down at startup does not stop the worker: it connects
/// without a token and the refresh loop keeps trying, so the token appears as
/// soon as the broker answers.
async fn start(identity: HostedWorkloadIdentity, retry_after: Duration) -> Result<Hosted> {
    let broker = BrokerClient::new(identity)?;
    let token: SharedBearer = Arc::new(RwLock::new(None));
    let expires_at = match broker.exchange().await {
        Ok(grant) => match crate::client::bearer_metadata(&grant.token) {
            Ok(value) => {
                *token.write().unwrap_or_else(|p| p.into_inner()) = Some(value);
                info!("worker authenticated with its workload identity");
                Some(grant.expires_at)
            }
            Err(error) => {
                warn!("workload broker returned an unusable runtime token: {error}; connecting without one");
                None
            }
        },
        Err(error) => {
            warn!("workload broker unavailable: {error}; connecting without a runtime token");
            None
        }
    };
    let refresher = spawn_token_refresh(
        "workload",
        token.clone(),
        expires_at,
        retry_after,
        move || {
            let broker = broker.clone();
            async move { broker.exchange().await }
        },
    );
    Ok(Hosted { token, refresher })
}

struct Hosted {
    token: SharedBearer,
    refresher: tokio::task::JoinHandle<()>,
}

// One token per process: every runtime connection shares it, so reconnects
// don't call the broker and only one refresh loop runs.
static HOSTED: Mutex<Option<Hosted>> = Mutex::const_new(None);

/// The runtime token every worker connection should send. Empty until the
/// broker first answers; it never stops a worker from connecting.
pub(crate) async fn authorization(identity: HostedWorkloadIdentity) -> SharedBearer {
    let mut hosted = HOSTED.lock().await;
    if let Some(current) = hosted.as_ref() {
        // A refresh loop ends only with the Tokio runtime that ran it; a
        // binding that starts a new runtime needs a new loop.
        if !current.refresher.is_finished() {
            return current.token.clone();
        }
    }
    match start(identity, REFRESH_RETRY_DELAY).await {
        Ok(next) => {
            let token = next.token.clone();
            *hosted = Some(next);
            token
        }
        Err(error) => {
            warn!("workload identity unavailable: {error}; connecting without a runtime token");
            Arc::new(RwLock::new(None))
        }
    }
}

fn truncate(text: &str, max_chars: usize) -> String {
    let text = text.trim();
    match text.char_indices().nth(max_chars) {
        Some((index, _)) => format!("{}…", &text[..index]),
        None => text.to_string(),
    }
}

fn connection_error(message: impl Into<String>) -> SdkError {
    SdkError::Connection {
        message: message.into(),
        code: ErrorCode::ConnectionFailed,
        source: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    struct Received {
        method: String,
        path: String,
        content_type: String,
        body: serde_json::Value,
    }

    /// A broker that answers each request with the next response from
    /// `responses` and reports what it was sent.
    async fn stub_broker(
        responses: Vec<(u16, String)>,
    ) -> (String, tokio::sync::mpsc::UnboundedReceiver<Received>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let (head, request_body) = loop {
                    let mut chunk = [0; 4096];
                    let n = stream.read(&mut chunk).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&chunk[..n]);
                    let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") else {
                        continue;
                    };
                    let head = String::from_utf8_lossy(&bytes[..end]).to_string();
                    let length: usize = head
                        .lines()
                        .find_map(|line| {
                            let (k, v) = line.split_once(':')?;
                            k.eq_ignore_ascii_case("content-length")
                                .then(|| v.trim().parse().unwrap())
                        })
                        .unwrap_or(0);
                    if bytes.len() >= end + 4 + length {
                        break (head, bytes[end + 4..end + 4 + length].to_vec());
                    }
                };
                let mut request_line = head.lines().next().unwrap().split(' ');
                let header = |name: &str| {
                    head.lines()
                        .find_map(|line| {
                            let (k, v) = line.split_once(':')?;
                            k.eq_ignore_ascii_case(name).then(|| v.trim().to_string())
                        })
                        .unwrap_or_default()
                };
                let _ = tx.send(Received {
                    method: request_line.next().unwrap().to_string(),
                    path: request_line.next().unwrap().to_string(),
                    content_type: header("content-type"),
                    body: serde_json::from_slice(&request_body).unwrap(),
                });
                let response = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (url, rx)
    }

    fn grant(token: &str, expires_in: chrono::Duration) -> (u16, String) {
        let expires_at = (chrono::Utc::now() + expires_in).to_rfc3339();
        (
            200,
            serde_json::json!({ "token": token, "expires_at": expires_at }).to_string(),
        )
    }

    fn identity(url: &str, dir: &tempfile::TempDir, projected: &str) -> HostedWorkloadIdentity {
        let token_file = dir.path().join("token");
        std::fs::write(&token_file, projected).unwrap();
        HostedWorkloadIdentity {
            broker_url: url.to_string(),
            token_file,
        }
    }

    fn sent_bearer(token: &SharedBearer) -> Option<String> {
        token
            .read()
            .unwrap()
            .as_ref()
            .map(|value| value.to_str().unwrap().to_string())
    }

    #[test]
    fn hosted_identity_needs_both_the_broker_and_the_token_file() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |name: &str| {
                pairs
                    .iter()
                    .find(|(key, _)| *key == name)
                    .map(|(_, value)| value.to_string())
            }
        };
        assert_eq!(
            HostedWorkloadIdentity::from_lookup(env(&[
                (WORKLOAD_BROKER_URL_ENV, " http://broker:34189/ "),
                (WORKLOAD_TOKEN_FILE_ENV, "/run/agnt5-workload/token"),
            ])),
            Some(HostedWorkloadIdentity {
                broker_url: "http://broker:34189".into(),
                token_file: "/run/agnt5-workload/token".into(),
            })
        );
        assert_eq!(
            HostedWorkloadIdentity::from_lookup(env(&[(WORKLOAD_BROKER_URL_ENV, "http://b")])),
            None
        );
        assert_eq!(
            HostedWorkloadIdentity::from_lookup(env(&[(WORKLOAD_TOKEN_FILE_ENV, "/t")])),
            None
        );
        assert_eq!(
            HostedWorkloadIdentity::from_lookup(env(&[
                (WORKLOAD_BROKER_URL_ENV, "http://b"),
                (WORKLOAD_TOKEN_FILE_ENV, "  "),
            ])),
            None
        );
    }

    #[tokio::test]
    async fn exchange_sends_the_trimmed_pod_token_for_the_runtime_audience() {
        let (url, mut received) =
            stub_broker(vec![grant("runtime-jwt", chrono::Duration::minutes(10))]).await;
        let dir = tempfile::tempdir().unwrap();
        let broker = BrokerClient::new(identity(&url, &dir, "projected-1\n")).unwrap();

        let grant = broker.exchange().await.unwrap();

        assert_eq!(grant.token, "runtime-jwt");
        let request = received.recv().await.unwrap();
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/internal/workload/token");
        assert_eq!(request.content_type, "application/json");
        assert_eq!(
            request.body,
            serde_json::json!({ "token": "projected-1", "audience": ["agnt5-runtime"] })
        );
    }

    #[tokio::test]
    async fn exchange_retries_an_unavailable_broker_once() {
        let (url, mut received) = stub_broker(vec![
            (503, r#"{"error":"starting"}"#.into()),
            grant("runtime-jwt", chrono::Duration::minutes(10)),
        ])
        .await;
        let dir = tempfile::tempdir().unwrap();
        let broker = BrokerClient::new(identity(&url, &dir, "projected")).unwrap();

        assert_eq!(broker.exchange().await.unwrap().token, "runtime-jwt");
        assert!(received.recv().await.is_some());
        assert!(received.recv().await.is_some());
    }

    #[tokio::test]
    async fn exchange_does_not_retry_a_refusal() {
        let (url, _received) = stub_broker(vec![
            (403, r#"{"error":"pod is not a worker"}"#.into()),
            grant("runtime-jwt", chrono::Duration::minutes(10)),
        ])
        .await;
        let dir = tempfile::tempdir().unwrap();
        let broker = BrokerClient::new(identity(&url, &dir, "projected")).unwrap();

        let error = broker.exchange().await.unwrap_err().to_string();
        assert!(error.contains("403"), "{error}");
        assert!(error.contains("pod is not a worker"), "{error}");
    }

    #[tokio::test]
    async fn start_attaches_the_runtime_token_as_a_bearer() {
        let (url, _received) =
            stub_broker(vec![grant("runtime-jwt", chrono::Duration::minutes(10))]).await;
        let dir = tempfile::tempdir().unwrap();

        let hosted = start(identity(&url, &dir, "projected"), REFRESH_RETRY_DELAY)
            .await
            .unwrap();

        assert_eq!(
            sent_bearer(&hosted.token).as_deref(),
            Some("Bearer runtime-jwt")
        );
        let request = crate::client::authorize_request_for_test(&hosted.token);
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer runtime-jwt"
        );
        hosted.refresher.abort();
    }

    #[tokio::test]
    async fn refresh_reads_the_rotated_pod_token_again() {
        // Already inside the refresh margin, so the loop refreshes at once.
        let (url, mut received) = stub_broker(vec![
            grant("runtime-jwt-1", chrono::Duration::seconds(30)),
            grant("runtime-jwt-2", chrono::Duration::minutes(10)),
        ])
        .await;
        let dir = tempfile::tempdir().unwrap();
        let identity = identity(&url, &dir, "projected-1");
        let token_file = identity.token_file.clone();

        let hosted = start(identity, REFRESH_RETRY_DELAY).await.unwrap();
        assert_eq!(received.recv().await.unwrap().body["token"], "projected-1");
        // Kubelet rotates the projected token in place.
        std::fs::write(&token_file, "projected-2").unwrap();

        let refreshed = tokio::time::timeout(Duration::from_secs(10), received.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(refreshed.body["token"], "projected-2");
        tokio::time::timeout(Duration::from_secs(5), async {
            while sent_bearer(&hosted.token).as_deref() != Some("Bearer runtime-jwt-2") {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        hosted.refresher.abort();
    }

    #[tokio::test]
    async fn worker_starts_without_a_token_and_gains_one_when_the_broker_recovers() {
        let (url, _received) = stub_broker(vec![
            (503, r#"{"error":"down"}"#.into()),
            (503, r#"{"error":"down"}"#.into()),
            grant("runtime-jwt", chrono::Duration::minutes(10)),
        ])
        .await;
        let dir = tempfile::tempdir().unwrap();

        let hosted = start(identity(&url, &dir, "projected"), Duration::from_millis(50))
            .await
            .unwrap();
        assert_eq!(sent_bearer(&hosted.token), None);

        tokio::time::timeout(Duration::from_secs(5), async {
            while sent_bearer(&hosted.token).is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            sent_bearer(&hosted.token).as_deref(),
            Some("Bearer runtime-jwt")
        );
        hosted.refresher.abort();
    }
}
