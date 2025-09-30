//! Session implementation for the ADK.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::adk::runtime_client::RuntimeServiceClient;
use crate::error::{Result, SdkError};
use crate::pb::{
    runtime_service_request::Operation as RuntimeOperation,
    runtime_service_response::Result as RuntimeResult, RuntimeServiceRequest,
    SessionEventAppendRequest, SessionEventListRequest, SessionStateDeleteRequest,
    SessionStateGetRequest, SessionStateScope as PbSessionStateScope, SessionStateSetRequest,
};

/// Scope identifiers for session state entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionStateScope {
    Session,
    User,
    App,
    Temp,
}

impl SessionStateScope {
    pub fn as_str(self) -> &'static str {
        match self {
            SessionStateScope::Session => "session",
            SessionStateScope::User => "user",
            SessionStateScope::App => "app",
            SessionStateScope::Temp => "temp",
        }
    }
}

impl From<SessionStateScope> for PbSessionStateScope {
    fn from(scope: SessionStateScope) -> Self {
        match scope {
            SessionStateScope::Session => PbSessionStateScope::Session,
            SessionStateScope::User => PbSessionStateScope::User,
            SessionStateScope::App => PbSessionStateScope::App,
            SessionStateScope::Temp => PbSessionStateScope::Temp,
        }
    }
}

/// Persistent session handle exposed to callers.
#[derive(Clone)]
pub struct SessionHandle {
    id: String,
    tenant_id: String,
    backend: Arc<dyn SessionBackend>,
}

impl SessionHandle {
    pub fn new(
        session_id: impl Into<String>,
        tenant_id: impl Into<String>,
        backend: Arc<dyn SessionBackend>,
    ) -> Self {
        Self {
            id: session_id.into(),
            tenant_id: tenant_id.into(),
            backend,
        }
    }

    pub fn new_placeholder(session_id: impl Into<String>) -> Self {
        let backend: Arc<dyn SessionBackend> = Arc::new(InMemorySessionBackend::default());
        let tenant = std::env::var("AGNT5_TENANT_ID").unwrap_or_else(|_| "default".to_string());
        Self::new(session_id, tenant, backend)
    }

    pub fn new_runtime(
        session_id: impl Into<String>,
        tenant_id: impl Into<String>,
        client: Arc<RuntimeServiceClient>,
    ) -> Self {
        let backend: Arc<dyn SessionBackend> = Arc::new(RuntimeSessionBackend::new(client));
        Self::new(session_id, tenant_id, backend)
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    pub fn state(&self) -> SessionStateHandle {
        SessionStateHandle {
            session_id: self.id.clone(),
            tenant_id: self.tenant_id.clone(),
            backend: Arc::clone(&self.backend),
        }
    }

    pub fn history(&self, limit: Option<usize>) -> Result<Vec<SessionEvent>> {
        self.backend.fetch_history(&self.tenant_id, &self.id, limit)
    }

    pub fn events(&self, since: Option<u64>, limit: Option<usize>) -> Result<Vec<SessionEvent>> {
        self.backend
            .fetch_events(&self.tenant_id, &self.id, since, limit)
    }

    pub fn append_event(&self, event: SessionEvent) -> Result<()> {
        self.backend.append_event(&self.tenant_id, &self.id, event)
    }
}

/// Handle for working with session state.
#[derive(Clone)]
pub struct SessionStateHandle {
    session_id: String,
    tenant_id: String,
    backend: Arc<dyn SessionBackend>,
}

impl SessionStateHandle {
    pub fn get(&self, key: &str, scope: SessionStateScope) -> Result<Option<String>> {
        self.backend
            .state_get(&self.tenant_id, &self.session_id, key, scope)
    }

    pub fn set(&self, key: &str, value: String, scope: SessionStateScope) -> Result<()> {
        self.backend
            .state_set(&self.tenant_id, &self.session_id, key, value, scope)
    }

    pub fn delete(&self, key: &str, scope: SessionStateScope) -> Result<()> {
        self.backend
            .state_delete(&self.tenant_id, &self.session_id, key, scope)
    }
}

/// Lightweight representation of a session event.
#[derive(Debug, Clone, Default)]
pub struct SessionEvent {
    pub offset: u64,
    pub kind: String,
    pub timestamp_ms: i64,
    pub payload: Option<String>,
    pub metadata: HashMap<String, String>,
}

impl SessionEvent {
    pub fn new_placeholder(kind: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            metadata: HashMap::new(),
            ..Default::default()
        }
    }

    fn normalize(mut self, next_offset: u64) -> Self {
        if self.offset == 0 {
            self.offset = next_offset;
        }
        if self.timestamp_ms == 0 {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default();
            self.timestamp_ms = now.as_millis() as i64;
        }
        self
    }
}

/// Backend interface used by the session handle.
pub trait SessionBackend: Send + Sync {
    fn fetch_history(
        &self,
        tenant_id: &str,
        session_id: &str,
        limit: Option<usize>,
    ) -> Result<Vec<SessionEvent>>;

    fn fetch_events(
        &self,
        tenant_id: &str,
        session_id: &str,
        since: Option<u64>,
        limit: Option<usize>,
    ) -> Result<Vec<SessionEvent>>;

    fn append_event(&self, tenant_id: &str, session_id: &str, event: SessionEvent) -> Result<()>;

    fn state_get(
        &self,
        tenant_id: &str,
        session_id: &str,
        key: &str,
        scope: SessionStateScope,
    ) -> Result<Option<String>>;

    fn state_set(
        &self,
        tenant_id: &str,
        session_id: &str,
        key: &str,
        value: String,
        scope: SessionStateScope,
    ) -> Result<()>;

    fn state_delete(
        &self,
        tenant_id: &str,
        session_id: &str,
        key: &str,
        scope: SessionStateScope,
    ) -> Result<()>;
}

/// Simple in-memory backend used for development/testing.
#[derive(Default)]
pub struct InMemorySessionBackend {
    inner: Mutex<HashMap<String, SessionRecord>>,
}

#[derive(Default)]
struct SessionRecord {
    events: Vec<SessionEvent>,
    state: HashMap<String, HashMap<String, String>>, // scope -> (key -> value)
}

impl InMemorySessionBackend {
    fn with_session<F, T>(&self, session_id: &str, create: bool, f: F) -> Result<T>
    where
        F: FnOnce(&mut SessionRecord) -> T,
    {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| SdkError::Internal("Session backend mutex poisoned".to_string()))?;
        let record = if create {
            guard.entry(session_id.to_string()).or_default()
        } else {
            guard
                .get_mut(session_id)
                .ok_or_else(|| SdkError::Internal(format!("Session '{session_id}' not found")))?
        };
        Ok(f(record))
    }
}

impl SessionBackend for InMemorySessionBackend {
    fn fetch_history(
        &self,
        _tenant_id: &str,
        session_id: &str,
        limit: Option<usize>,
    ) -> Result<Vec<SessionEvent>> {
        self.with_session(session_id, false, |rec| {
            let total = rec.events.len();
            let start = limit.map(|limit| total.saturating_sub(limit)).unwrap_or(0);
            rec.events[start..].to_vec()
        })
    }

    fn fetch_events(
        &self,
        _tenant_id: &str,
        session_id: &str,
        since: Option<u64>,
        limit: Option<usize>,
    ) -> Result<Vec<SessionEvent>> {
        self.with_session(session_id, false, |rec| {
            let iter = rec.events.iter().filter(|event| match since {
                Some(offset) => event.offset > offset,
                None => true,
            });
            let mut results: Vec<_> = iter.cloned().collect();
            if let Some(limit) = limit {
                results.truncate(limit);
            }
            results
        })
    }

    fn append_event(&self, _tenant_id: &str, session_id: &str, event: SessionEvent) -> Result<()> {
        self.with_session(session_id, true, |rec| {
            let next_offset = rec.events.len() as u64;
            rec.events.push(event.normalize(next_offset));
        })
    }

    fn state_get(
        &self,
        _tenant_id: &str,
        session_id: &str,
        key: &str,
        scope: SessionStateScope,
    ) -> Result<Option<String>> {
        self.with_session(session_id, false, |rec| {
            rec.state
                .get(scope.as_str())
                .and_then(|bucket| bucket.get(key).cloned())
        })
    }

    fn state_set(
        &self,
        _tenant_id: &str,
        session_id: &str,
        key: &str,
        value: String,
        scope: SessionStateScope,
    ) -> Result<()> {
        self.with_session(session_id, true, |rec| {
            rec.state
                .entry(scope.as_str().to_string())
                .or_default()
                .insert(key.to_string(), value);
        })
    }

    fn state_delete(
        &self,
        _tenant_id: &str,
        session_id: &str,
        key: &str,
        scope: SessionStateScope,
    ) -> Result<()> {
        self.with_session(session_id, false, |rec| {
            if let Some(bucket) = rec.state.get_mut(scope.as_str()) {
                bucket.remove(key);
            }
        })
    }
}

struct RuntimeSessionBackend {
    client: Arc<RuntimeServiceClient>,
}

impl RuntimeSessionBackend {
    fn new(client: Arc<RuntimeServiceClient>) -> Self {
        Self { client }
    }
}

impl SessionBackend for RuntimeSessionBackend {
    fn fetch_history(
        &self,
        tenant_id: &str,
        session_id: &str,
        limit: Option<usize>,
    ) -> Result<Vec<SessionEvent>> {
        let request = RuntimeServiceRequest {
            request_id: String::new(),
            tenant_id: tenant_id.to_string(),
            session_id: session_id.to_string(),
            operation: Some(RuntimeOperation::SessionEventList(
                SessionEventListRequest {
                    since_offset: 0,
                    limit: limit.unwrap_or(0) as u32,
                },
            )),
        };
        block_on_events(Arc::clone(&self.client), request)
    }

    fn fetch_events(
        &self,
        tenant_id: &str,
        session_id: &str,
        since: Option<u64>,
        limit: Option<usize>,
    ) -> Result<Vec<SessionEvent>> {
        let request = RuntimeServiceRequest {
            request_id: String::new(),
            tenant_id: tenant_id.to_string(),
            session_id: session_id.to_string(),
            operation: Some(RuntimeOperation::SessionEventList(
                SessionEventListRequest {
                    since_offset: since.unwrap_or(0),
                    limit: limit.unwrap_or(0) as u32,
                },
            )),
        };
        block_on_events(Arc::clone(&self.client), request)
    }

    fn append_event(&self, tenant_id: &str, session_id: &str, event: SessionEvent) -> Result<()> {
        let request = RuntimeServiceRequest {
            request_id: String::new(),
            tenant_id: tenant_id.to_string(),
            session_id: session_id.to_string(),
            operation: Some(RuntimeOperation::SessionEventAppend(
                SessionEventAppendRequest {
                    kind: event.kind,
                    payload: event.payload.unwrap_or_default().into_bytes(),
                    metadata: event.metadata,
                },
            )),
        };
        block_on_unit(Arc::clone(&self.client), request)
    }

    fn state_get(
        &self,
        tenant_id: &str,
        session_id: &str,
        key: &str,
        scope: SessionStateScope,
    ) -> Result<Option<String>> {
        let request = RuntimeServiceRequest {
            request_id: String::new(),
            tenant_id: tenant_id.to_string(),
            session_id: session_id.to_string(),
            operation: Some(RuntimeOperation::SessionStateGet(SessionStateGetRequest {
                scope: PbSessionStateScope::from(scope) as i32,
                key: key.to_string(),
            })),
        };
        block_on_state_get(Arc::clone(&self.client), request)
    }

    fn state_set(
        &self,
        tenant_id: &str,
        session_id: &str,
        key: &str,
        value: String,
        scope: SessionStateScope,
    ) -> Result<()> {
        let request = RuntimeServiceRequest {
            request_id: String::new(),
            tenant_id: tenant_id.to_string(),
            session_id: session_id.to_string(),
            operation: Some(RuntimeOperation::SessionStateSet(SessionStateSetRequest {
                scope: PbSessionStateScope::from(scope) as i32,
                key: key.to_string(),
                value: value.into_bytes(),
                expected_version: String::new(),
                metadata: HashMap::new(),
            })),
        };
        block_on_unit(Arc::clone(&self.client), request)
    }

    fn state_delete(
        &self,
        tenant_id: &str,
        session_id: &str,
        key: &str,
        scope: SessionStateScope,
    ) -> Result<()> {
        let request = RuntimeServiceRequest {
            request_id: String::new(),
            tenant_id: tenant_id.to_string(),
            session_id: session_id.to_string(),
            operation: Some(RuntimeOperation::SessionStateDelete(
                SessionStateDeleteRequest {
                    scope: PbSessionStateScope::from(scope) as i32,
                    key: key.to_string(),
                    expected_version: String::new(),
                },
            )),
        };
        block_on_unit(Arc::clone(&self.client), request)
    }
}

fn block_on_events(
    client: Arc<RuntimeServiceClient>,
    request: RuntimeServiceRequest,
) -> Result<Vec<SessionEvent>> {
    block_on(async move {
        let response = client.request(request).await?;
        match response.result {
            Some(RuntimeResult::SessionEventList(res)) => Ok(res
                .events
                .into_iter()
                .map(|event| SessionEvent {
                    offset: event.offset,
                    kind: event.kind,
                    timestamp_ms: event
                        .timestamp
                        .as_ref()
                        .map(|ts| ts.seconds * 1000 + (ts.nanos as i64 / 1_000_000))
                        .unwrap_or_default(),
                    payload: String::from_utf8(event.payload).ok(),
                    metadata: event.metadata,
                })
                .collect()),
            _ => Ok(Vec::new()),
        }
    })
}

fn block_on_state_get(
    client: Arc<RuntimeServiceClient>,
    request: RuntimeServiceRequest,
) -> Result<Option<String>> {
    block_on(async move {
        let response = client.request(request).await?;
        match response.result {
            Some(RuntimeResult::SessionStateGet(res)) => {
                if res.found {
                    Ok(String::from_utf8(res.value).ok())
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    })
}

fn block_on_unit(client: Arc<RuntimeServiceClient>, request: RuntimeServiceRequest) -> Result<()> {
    block_on(async move {
        let _ = client.request(request).await?;
        Ok(())
    })
}

fn block_on<F, T>(future: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.block_on(future)
    } else {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| SdkError::Internal(format!("create runtime: {}", e)))?;
        runtime.block_on(future)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_as_str_matches_expected_values() {
        assert_eq!(SessionStateScope::Session.as_str(), "session");
        assert_eq!(SessionStateScope::User.as_str(), "user");
        assert_eq!(SessionStateScope::App.as_str(), "app");
        assert_eq!(SessionStateScope::Temp.as_str(), "temp");
    }

    #[test]
    fn in_memory_backend_persists_state_and_events() {
        let session = SessionHandle::new_placeholder("test-session");
        let state = session.state();

        state
            .set("counter", "1".into(), SessionStateScope::Session)
            .unwrap();
        assert_eq!(
            state
                .get("counter", SessionStateScope::Session)
                .unwrap()
                .as_deref(),
            Some("1")
        );

        session
            .append_event(SessionEvent::new_placeholder("message"))
            .unwrap();
        let events = session.events(None, None).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, "message");
        assert_eq!(events[0].offset, 0);
        assert!(events[0].timestamp_ms > 0);
    }

    #[test]
    fn event_history_honours_limits() {
        let session = SessionHandle::new_placeholder("test-session");
        for i in 0..5 {
            session
                .append_event(SessionEvent::new_placeholder(format!("event-{i}")))
                .unwrap();
        }

        let subset = session.history(Some(2)).unwrap();
        assert_eq!(subset.len(), 2);
        assert_eq!(subset[0].kind, "event-3");
        assert_eq!(subset[1].kind, "event-4");

        let since = session.events(Some(1), Some(2)).unwrap();
        assert_eq!(since.len(), 2);
        assert_eq!(since[0].offset, 2);
        assert_eq!(since[1].offset, 3);
    }
}
