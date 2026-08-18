//! Certificate-bound identity lifecycle for customer-hosted workers.
//!
//! This module is deliberately the only SDK-core component allowed to read the
//! bootstrap credential or persist private session material. Callers receive a
//! TLS configuration and a rotating bearer-token interceptor, never raw keys.

use crate::error::{ErrorCode, Result, SdkError};
use base64::Engine;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rcgen::{CertificateParams, KeyPair, PKCS_ECDSA_P256_SHA256};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock, Weak};
use std::time::Duration;
use tokio::sync::{Mutex, OnceCell};
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, ClientTlsConfig, Identity};
use tracing::{error, warn};

const DISCOVERY_PATH: &str = "/api/v1/worker-discovery";
const SESSION_OPEN_PATH: &str = "/api/v1/external-worker-sessions";
const TOKEN_REFRESH_PATH: &str = "/api/v1/external-worker-sessions/token";
const SESSION_RENEW_PATH: &str = "/api/v1/external-worker-sessions/renew";
const SESSION_FILE_NAME: &str = "worker-session.json";
const REFRESH_MARGIN: ChronoDuration = ChronoDuration::seconds(60);

static MANAGER: OnceCell<Arc<IdentityManager>> = OnceCell::const_new();
static SESSION_FILE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct Authority {
    project_id: String,
    environment_id: String,
    deployment_id: String,
    worker_pool_id: String,
    #[serde(default)]
    runtime_endpoint: String,
    #[serde(default)]
    protocol: String,
}

#[derive(Debug, Serialize)]
struct DiscoveryRequest<'a> {
    environment: &'a str,
}

#[derive(Debug, Serialize)]
struct OpenRequest<'a> {
    project_id: &'a str,
    environment_id: &'a str,
    deployment_id: &'a str,
    worker_pool_id: &'a str,
    csr_der_base64: &'a str,
}

#[derive(Debug, Serialize)]
struct RenewRequest<'a> {
    csr_der_base64: &'a str,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Session {
    session_id: String,
    project_id: String,
    environment_id: String,
    deployment_id: String,
    worker_pool_id: String,
    worker_id: String,
    spiffe_id: String,
    #[serde(default)]
    runtime_endpoint: String,
    certificate_der_base64: String,
    certificate_chain_der_base64: Vec<String>,
    trust_bundle_der_base64: Vec<String>,
    trust_bundle_version: String,
    certificate_expires_at: DateTime<Utc>,
    renew_after: DateTime<Utc>,
    workload_token: String,
    token_type: String,
    token_expires_at: DateTime<Utc>,
    #[serde(default)]
    private_key_pem: String,
}

#[derive(Debug, Deserialize)]
struct RefreshedToken {
    workload_token: String,
    token_type: String,
    token_expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdentityReadiness {
    Enrolling,
    Ready {
        certificate_expires_at: DateTime<Utc>,
        token_expires_at: DateTime<Utc>,
    },
    Degraded,
}

#[derive(Clone)]
pub struct ConnectionIdentity {
    pub endpoint: String,
    pub worker_id: String,
    pub tls: ClientTlsConfig,
    pub authorization: Arc<RwLock<Option<MetadataValue<tonic::metadata::Ascii>>>>,
}

struct Subscriber {
    session_id: String,
    token: Weak<RwLock<Option<MetadataValue<tonic::metadata::Ascii>>>>,
}

struct IdentityManager {
    identity_url: String,
    authority: Authority,
    session_path: PathBuf,
    session: RwLock<Session>,
    readiness: RwLock<IdentityReadiness>,
    subscribers: Mutex<Vec<Subscriber>>,
}

pub fn bootstrap_profile_enabled() -> bool {
    matches!(
        std::env::var("AGNT5_WORKER_IDENTITY_MODE")
            .unwrap_or_else(|_| "disabled".to_string())
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "bootstrap" | "required"
    )
}

pub async fn connection_identity() -> Result<ConnectionIdentity> {
    let manager = MANAGER
        .get_or_try_init(|| async { IdentityManager::initialize().await.map(Arc::new) })
        .await?
        .clone();
    manager.connection_identity().await
}

pub async fn readiness() -> Option<IdentityReadiness> {
    Some(MANAGER.get()?.readiness.read().ok()?.clone())
}

impl IdentityManager {
    async fn initialize() -> Result<Self> {
        let control_plane_url = normalized_url(
            "AGNT5_CONTROL_PLANE_URL",
            "https://api.agnt5.com",
            "control-plane URL",
        )?;
        let identity_url = normalized_url(
            "AGNT5_IDENTITY_MTLS_URL",
            &control_plane_url,
            "identity mTLS URL",
        )?;
        let key_path = required_env("AGNT5_API_KEY_FILE")?;
        let credential = std::fs::read_to_string(&key_path)
            .map_err(|e| connection_error(format!("read bootstrap credential {key_path}: {e}")))?
            .trim()
            .to_string();
        if credential.is_empty() {
            return Err(connection_error("bootstrap credential file is empty"));
        }
        let session_dir = PathBuf::from(required_env("AGNT5_WORKER_SESSION_DIR")?);
        ensure_private_directory(&session_dir)?;
        let authority = discover(&control_plane_url, &credential).await?;
        // Discovery is authoritative for runtime registration. Language
        // bindings consume these through the existing WorkerConfig path.
        std::env::set_var("AGNT5_PROJECT_ID", &authority.project_id);
        std::env::set_var("AGNT5_DEPLOYMENT_ID", &authority.deployment_id);
        std::env::set_var("AGNT5_WORKERPOOL_ID", &authority.worker_pool_id);
        std::env::set_var("AGNT5_WORKER_MODE", "pull");
        let session_path = session_dir.join(SESSION_FILE_NAME);
        let _guard = SESSION_FILE_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .await;
        let session = match load_session(&session_path, &authority) {
            Ok(session) => session,
            Err(_) => {
                let (key, csr) = new_key_and_csr()?;
                let mut session =
                    open_session(&control_plane_url, &credential, &authority, &csr).await?;
                session.private_key_pem = key;
                validate_session(&session, &authority)?;
                write_session(&session_path, &session)?;
                session
            }
        };
        let readiness = IdentityReadiness::Ready {
            certificate_expires_at: session.certificate_expires_at,
            token_expires_at: session.token_expires_at,
        };
        let manager = Self {
            identity_url,
            authority,
            session_path,
            session: RwLock::new(session),
            readiness: RwLock::new(readiness),
            subscribers: Mutex::new(Vec::new()),
        };
        Ok(manager)
    }

    async fn connection_identity(self: &Arc<Self>) -> Result<ConnectionIdentity> {
        let session = self
            .session
            .read()
            .map_err(|_| connection_error("worker identity state is unavailable"))?
            .clone();
        validate_session(&session, &self.authority)?;
        let authorization = Arc::new(RwLock::new(Some(bearer_metadata(&session.workload_token)?)));
        self.subscribers.lock().await.push(Subscriber {
            session_id: session.session_id.clone(),
            token: Arc::downgrade(&authorization),
        });
        let manager = self.clone();
        static MAINTENANCE_STARTED: OnceLock<()> = OnceLock::new();
        if MAINTENANCE_STARTED.set(()).is_ok() {
            tokio::spawn(async move { manager.maintain().await });
        }
        Ok(ConnectionIdentity {
            endpoint: session.runtime_endpoint.clone(),
            worker_id: session.worker_id.clone(),
            tls: tls_config(&session)?,
            authorization,
        })
    }

    async fn maintain(self: Arc<Self>) {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let session = match self.session.read() {
                Ok(value) => value.clone(),
                Err(_) => continue,
            };
            let now = Utc::now();
            let result = if now >= session.renew_after {
                self.renew(&session).await
            } else if now + REFRESH_MARGIN >= session.token_expires_at {
                self.refresh(&session).await
            } else {
                continue;
            };
            if let Err(e) = result {
                if let Ok(mut readiness) = self.readiness.write() {
                    *readiness = IdentityReadiness::Degraded;
                }
                error!("external worker identity maintenance failed: {e}");
            }
        }
    }

    async fn refresh(&self, current: &Session) -> Result<()> {
        let client = identity_http_client(current)?;
        let response = client
            .post(format!("{}{}", self.identity_url, TOKEN_REFRESH_PATH))
            .send()
            .await
            .map_err(|e| connection_error(format!("worker token refresh failed: {e}")))?;
        if !response.status().is_success() {
            return Err(connection_error(format!(
                "worker token refresh returned {}",
                response.status()
            )));
        }
        let refreshed: RefreshedToken = response
            .json()
            .await
            .map_err(|e| connection_error(format!("decode worker token refresh: {e}")))?;
        let value = bearer_metadata(&refreshed.workload_token)?;
        let mut next = current.clone();
        next.workload_token = refreshed.workload_token;
        next.token_type = refreshed.token_type;
        next.token_expires_at = refreshed.token_expires_at;
        write_session(&self.session_path, &next)?;
        *self
            .session
            .write()
            .map_err(|_| connection_error("worker identity state is unavailable"))? = next.clone();
        let mut subscribers = self.subscribers.lock().await;
        subscribers.retain(|subscriber| {
            if let Some(token) = subscriber.token.upgrade() {
                if subscriber.session_id == current.session_id {
                    if let Ok(mut stored) = token.write() {
                        *stored = Some(value.clone());
                    }
                }
                true
            } else {
                false
            }
        });
        self.mark_ready(&next);
        Ok(())
    }

    async fn renew(&self, current: &Session) -> Result<()> {
        let (private_key_pem, csr) = new_key_and_csr()?;
        let client = identity_http_client(current)?;
        let response = client
            .post(format!("{}{}", self.identity_url, SESSION_RENEW_PATH))
            .bearer_auth(&current.workload_token)
            .json(&RenewRequest {
                csr_der_base64: &csr,
            })
            .send()
            .await
            .map_err(|e| connection_error(format!("worker identity renewal failed: {e}")))?;
        if !response.status().is_success() {
            return Err(connection_error(format!(
                "worker identity renewal returned {}",
                response.status()
            )));
        }
        let mut next: Session = response
            .json()
            .await
            .map_err(|e| connection_error(format!("decode worker identity renewal: {e}")))?;
        next.private_key_pem = private_key_pem;
        if next.runtime_endpoint.trim().is_empty() {
            next.runtime_endpoint = current.runtime_endpoint.clone();
        }
        validate_session(&next, &self.authority)?;
        write_session(&self.session_path, &next)?;
        *self
            .session
            .write()
            .map_err(|_| connection_error("worker identity state is unavailable"))? = next.clone();
        self.mark_ready(&next);
        warn!("external worker identity rotated; the runtime will reconnect the worker channel");
        Ok(())
    }

    fn mark_ready(&self, session: &Session) {
        if let Ok(mut readiness) = self.readiness.write() {
            *readiness = IdentityReadiness::Ready {
                certificate_expires_at: session.certificate_expires_at,
                token_expires_at: session.token_expires_at,
            };
        }
    }
}

async fn discover(control_plane_url: &str, credential: &str) -> Result<Authority> {
    let environment = std::env::var("AGNT5_ENVIRONMENT").unwrap_or_default();
    let response = bootstrap_http_client()?
        .post(format!("{control_plane_url}{DISCOVERY_PATH}"))
        .header("X-API-KEY", credential)
        .json(&DiscoveryRequest {
            environment: &environment,
        })
        .send()
        .await
        .map_err(|e| connection_error(format!("external worker discovery failed: {e}")))?;
    if !response.status().is_success() {
        return Err(connection_error(format!(
            "external worker discovery returned {}",
            response.status()
        )));
    }
    let authority: Authority = response
        .json()
        .await
        .map_err(|e| connection_error(format!("decode external worker discovery: {e}")))?;
    if authority.protocol != "pull.v1" || authority.runtime_endpoint.trim().is_empty() {
        return Err(connection_error(
            "external worker discovery returned an unsupported target",
        ));
    }
    Ok(authority)
}

async fn open_session(
    control_plane_url: &str,
    credential: &str,
    authority: &Authority,
    csr: &str,
) -> Result<Session> {
    let response = bootstrap_http_client()?
        .post(format!("{control_plane_url}{SESSION_OPEN_PATH}"))
        .bearer_auth(credential)
        .json(&OpenRequest {
            project_id: &authority.project_id,
            environment_id: &authority.environment_id,
            deployment_id: &authority.deployment_id,
            worker_pool_id: &authority.worker_pool_id,
            csr_der_base64: csr,
        })
        .send()
        .await
        .map_err(|e| connection_error(format!("worker identity enrollment failed: {e}")))?;
    if !response.status().is_success() {
        return Err(connection_error(format!(
            "worker identity enrollment returned {}",
            response.status()
        )));
    }
    response
        .json()
        .await
        .map_err(|e| connection_error(format!("decode worker identity enrollment: {e}")))
}

fn bootstrap_http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| connection_error(format!("create worker enrollment client: {e}")))
}

fn identity_http_client(session: &Session) -> Result<reqwest::Client> {
    let identity = reqwest::Identity::from_pem(&identity_pem(session)?)
        .map_err(|e| connection_error(format!("load worker client identity: {e}")))?;
    let mut builder = reqwest::Client::builder()
        .identity(identity)
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none());
    for root in trust_bundle_pem(session)? {
        builder = builder.add_root_certificate(
            reqwest::Certificate::from_pem(root.as_bytes())
                .map_err(|e| connection_error(format!("load worker trust root: {e}")))?,
        );
    }
    builder
        .build()
        .map_err(|e| connection_error(format!("create worker mTLS client: {e}")))
}

fn tls_config(session: &Session) -> Result<ClientTlsConfig> {
    Ok(ClientTlsConfig::new()
        .identity(Identity::from_pem(
            certificate_chain_pem(session)?,
            session.private_key_pem.clone(),
        ))
        .ca_certificate(Certificate::from_pem(trust_bundle_pem(session)?.concat())))
}

fn identity_pem(session: &Session) -> Result<Vec<u8>> {
    let mut bytes = certificate_chain_pem(session)?.into_bytes();
    bytes.extend_from_slice(session.private_key_pem.as_bytes());
    Ok(bytes)
}

fn certificate_chain_pem(session: &Session) -> Result<String> {
    let mut values = Vec::with_capacity(1 + session.certificate_chain_der_base64.len());
    values.push(session.certificate_der_base64.as_str());
    values.extend(
        session
            .certificate_chain_der_base64
            .iter()
            .map(String::as_str),
    );
    der_values_to_pem("CERTIFICATE", values)
}

fn trust_bundle_pem(session: &Session) -> Result<Vec<String>> {
    session
        .trust_bundle_der_base64
        .iter()
        .map(|value| der_values_to_pem("CERTIFICATE", [value.as_str()]))
        .collect()
}

fn der_values_to_pem<'a>(label: &str, values: impl IntoIterator<Item = &'a str>) -> Result<String> {
    let mut result = String::new();
    for value in values {
        let der = base64::engine::general_purpose::STANDARD
            .decode(value.trim())
            .map_err(|_| connection_error("worker session contained invalid certificate data"))?;
        result.push_str(&pem::encode(&pem::Pem::new(label, der)));
    }
    Ok(result)
}

fn new_key_and_csr() -> Result<(String, String)> {
    let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)
        .map_err(|e| connection_error(format!("generate worker identity key: {e}")))?;
    let csr = CertificateParams::default()
        .serialize_request(&key)
        .map_err(|e| connection_error(format!("generate worker identity CSR: {e}")))?;
    Ok((
        key.serialize_pem(),
        base64::engine::general_purpose::STANDARD.encode(csr.der().as_ref()),
    ))
}

fn validate_session(session: &Session, authority: &Authority) -> Result<()> {
    if session.project_id != authority.project_id
        || session.environment_id != authority.environment_id
        || session.deployment_id != authority.deployment_id
        || session.worker_pool_id != authority.worker_pool_id
        || session.session_id.trim().is_empty()
        || session.worker_id.trim().is_empty()
        || session.private_key_pem.trim().is_empty()
        || session.workload_token.trim().is_empty()
        || session.certificate_der_base64.trim().is_empty()
        || session.trust_bundle_der_base64.is_empty()
        || session.certificate_expires_at <= Utc::now() + ChronoDuration::seconds(30)
        || session.token_expires_at <= Utc::now() + ChronoDuration::seconds(30)
    {
        return Err(connection_error(
            "stored worker identity is expired, incomplete, or outside discovery authority",
        ));
    }
    Ok(())
}

fn load_session(path: &Path, authority: &Authority) -> Result<Session> {
    ensure_private_session_file(path)?;
    let bytes = std::fs::read(path)
        .map_err(|e| connection_error(format!("load worker session {}: {e}", path.display())))?;
    let session: Session = serde_json::from_slice(&bytes)
        .map_err(|e| connection_error(format!("decode worker session: {e}")))?;
    validate_session(&session, authority)?;
    Ok(session)
}

fn write_session(path: &Path, session: &Session) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| connection_error("worker session path has no parent"))?;
    let temporary = parent.join(format!(".{SESSION_FILE_NAME}.{}.tmp", uuid::Uuid::new_v4()));
    let bytes = serde_json::to_vec(session)
        .map_err(|e| connection_error(format!("encode worker session: {e}")))?;
    write_private_file(&temporary, &bytes)?;
    if let Err(error) = std::fs::rename(&temporary, path) {
        let _ = std::fs::remove_file(&temporary);
        return Err(connection_error(format!(
            "replace worker session atomically: {error}"
        )));
    }
    sync_directory(parent)?;
    Ok(())
}

fn ensure_private_directory(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)
        .map_err(|e| connection_error(format!("create worker session directory: {e}")))?;
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|e| connection_error(format!("inspect worker session directory: {e}")))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(connection_error(
            "worker session directory must be a real directory, not a symlink",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .map_err(|e| connection_error(format!("secure worker session directory: {e}")))?;
    }
    Ok(())
}

fn write_private_file(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .map_err(|e| connection_error(format!("write worker session: {e}")))?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|e| connection_error(format!("flush worker session: {e}")))
}

fn ensure_private_session_file(path: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|e| connection_error(format!("inspect worker session {}: {e}", path.display())))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(connection_error(
            "worker session must be a regular file, not a symlink",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(connection_error(
                "worker session permissions must not allow group or other access",
            ));
        }
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    std::fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|e| connection_error(format!("flush worker session directory: {e}")))?;
    Ok(())
}

fn bearer_metadata(token: &str) -> Result<MetadataValue<tonic::metadata::Ascii>> {
    format!("Bearer {token}")
        .parse()
        .map_err(|_| connection_error("worker token cannot be encoded as authorization metadata"))
}

fn normalized_url(name: &str, default: &str, label: &str) -> Result<String> {
    let value = std::env::var(name).unwrap_or_else(|_| default.to_string());
    let value = value.trim().trim_end_matches('/').to_string();
    let parsed = reqwest::Url::parse(&value)
        .map_err(|e| connection_error(format!("invalid {label}: {e}")))?;
    if parsed.scheme() != "https"
        && !matches!(parsed.host_str(), Some("localhost" | "127.0.0.1" | "::1"))
    {
        return Err(connection_error(format!("{label} must use HTTPS")));
    }
    Ok(value)
}

fn required_env(name: &str) -> Result<String> {
    let value = std::env::var(name)
        .map_err(|_| connection_error(format!("{name} is required for bootstrap identity")))?;
    if value.trim().is_empty() {
        return Err(connection_error(format!(
            "{name} is required for bootstrap identity"
        )));
    }
    Ok(value.trim().to_string())
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

    fn authority() -> Authority {
        Authority {
            project_id: "project".into(),
            environment_id: "environment".into(),
            deployment_id: "deployment".into(),
            worker_pool_id: "pool".into(),
            runtime_endpoint: "https://runtime.example.com".into(),
            protocol: "pull.v1".into(),
        }
    }

    #[test]
    fn generated_csr_uses_profile_key_algorithm() {
        let (key, csr) = new_key_and_csr().expect("key and csr");
        assert!(key.contains("PRIVATE KEY"));
        assert!(!csr.is_empty());
    }

    #[test]
    fn session_validation_rejects_cross_authority_and_expiry() {
        let mut session = Session {
            session_id: "session".into(),
            project_id: "project".into(),
            environment_id: "environment".into(),
            deployment_id: "deployment".into(),
            worker_pool_id: "pool".into(),
            worker_id: "worker".into(),
            spiffe_id: "spiffe://example/workload/project/environment/deployment/worker".into(),
            runtime_endpoint: "https://runtime.example.com".into(),
            certificate_der_base64: "Y2VydA==".into(),
            certificate_chain_der_base64: vec![],
            trust_bundle_der_base64: vec!["cm9vdA==".into()],
            trust_bundle_version: "v1".into(),
            certificate_expires_at: Utc::now() + ChronoDuration::minutes(10),
            renew_after: Utc::now() + ChronoDuration::minutes(5),
            workload_token: "token".into(),
            token_type: "Bearer".into(),
            token_expires_at: Utc::now() + ChronoDuration::minutes(5),
            private_key_pem: "key".into(),
        };
        assert!(validate_session(&session, &authority()).is_ok());
        session.project_id = "other".into();
        assert!(validate_session(&session, &authority()).is_err());
    }
}
