use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use base64::Engine as _;
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    adk::runtime_client::RuntimeServiceClient,
    error::{Result, SdkError},
    pb::{ActivationKind, ActivationRecoveryPolicy, BeginActivationRequest, WorkerSuspension},
    runtime_adapter::ActivationDecision,
};

use super::config::ContextConfig;
use super::registry::{FunctionCall, InvocationContext};

#[derive(Debug, Clone)]
pub struct CoreContext {
    state: Arc<ContextState>,
}

#[derive(Debug)]
struct ContextState {
    client: Option<Arc<RuntimeServiceClient>>,
    config: ContextConfig,
    sleep_ordinal: AtomicU64,
    completed_steps: Mutex<serde_json::Map<String, Value>>,
}

impl ContextState {
    fn function_registry(&self) -> Arc<super::registry::FunctionRegistry> {
        Arc::clone(&self.config.function_registry)
    }
}

impl CoreContext {
    pub fn new(client: Option<Arc<RuntimeServiceClient>>, config: ContextConfig) -> Self {
        let completed_steps = completed_steps(&config.metadata);
        let state = ContextState {
            client,
            config,
            sleep_ordinal: AtomicU64::new(0),
            completed_steps: Mutex::new(completed_steps),
        };
        Self {
            state: Arc::new(state),
        }
    }

    pub fn with_runtime(client: Arc<RuntimeServiceClient>, config: ContextConfig) -> Self {
        Self::new(Some(client), config)
    }

    pub fn config(&self) -> &ContextConfig {
        &self.state.config
    }

    pub fn runtime_client(&self) -> Option<&Arc<RuntimeServiceClient>> {
        self.state.client.as_ref()
    }

    pub fn functions(&self) -> FunctionNamespace {
        FunctionNamespace::new(self.state.clone())
    }

    pub fn signals(&self) -> SignalNamespace {
        SignalNamespace::new(self.state.clone())
    }

    pub fn timers(&self) -> TimerNamespace {
        TimerNamespace::new(self.state.clone())
    }

    pub fn language_model(&self) -> LanguageModelNamespace {
        LanguageModelNamespace::new(self.state.clone())
    }
}

#[derive(Debug, Clone)]
pub struct FunctionNamespace {
    state: Arc<ContextState>,
}

impl FunctionNamespace {
    fn new(state: Arc<ContextState>) -> Self {
        Self { state }
    }

    pub async fn call(&self, request: FunctionCall) -> Result<FunctionHandle> {
        let registry = self.state.function_registry();
        let invocation_id = Uuid::new_v4().to_string();
        let invocation_ctx = InvocationContext::from(&self.state.config);
        let original_request = request.clone();

        match registry.invoke(request, invocation_ctx).await {
            Ok(output) => Ok(FunctionHandle::succeeded(
                original_request,
                invocation_id,
                output,
            )),
            Err(err) => match &err {
                SdkError::InvalidArgument { .. } => Err(err),
                _ => Ok(FunctionHandle::failed(
                    original_request,
                    invocation_id,
                    err.to_string(),
                )),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct FunctionHandle {
    result: Arc<FunctionResult>,
}

impl FunctionHandle {
    pub async fn result(&self) -> FunctionResult {
        (*self.result).clone()
    }

    pub fn status(&self) -> FunctionStatus {
        self.result.status
    }

    fn succeeded(request: FunctionCall, invocation_id: String, output: Value) -> Self {
        let result = FunctionResult {
            request,
            invocation_id,
            status: FunctionStatus::Succeeded,
            output: Some(output),
            error: None,
        };
        Self {
            result: Arc::new(result),
        }
    }

    fn failed(request: FunctionCall, invocation_id: String, error: String) -> Self {
        let result = FunctionResult {
            request,
            invocation_id,
            status: FunctionStatus::Failed,
            output: None,
            error: Some(error),
        };
        Self {
            result: Arc::new(result),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionStatus {
    Pending,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone)]
pub struct FunctionResult {
    pub request: FunctionCall,
    pub invocation_id: String,
    pub status: FunctionStatus,
    pub output: Option<Value>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SignalNamespace {
    state: Arc<ContextState>,
}

impl SignalNamespace {
    fn new(state: Arc<ContextState>) -> Self {
        Self { state }
    }

    pub async fn wait(&self, name: &str) -> Result<Value> {
        let _ = (&self.state, name);
        Err(SdkError::Unavailable {
            message: "Signal waiting not yet implemented".to_string(),
            service: None,
        })
    }

    pub async fn emit(&self, name: &str, payload: Value) -> Result<()> {
        let _ = (&self.state, name, &payload);
        Err(SdkError::Unavailable {
            message: "Signal emission not yet implemented".to_string(),
            service: None,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TimerNamespace {
    state: Arc<ContextState>,
}

impl TimerNamespace {
    fn new(state: Arc<ContextState>) -> Self {
        Self { state }
    }

    pub async fn sleep(&self, duration: Duration) -> Result<()> {
        if duration.is_zero() {
            return Ok(());
        }
        if self
            .state
            .config
            .metadata
            .get("durable_suspension_v1")
            .map(String::as_str)
            != Some("true")
        {
            return Err(activation_error(
                crate::error::ErrorCode::DurabilityUnavailable,
                "durable_suspension_v1 was not negotiated for this context",
                None,
                None,
            ));
        }

        let ordinal = self.state.sleep_ordinal.fetch_add(1, Ordering::Relaxed);
        let timer_key = format!("sleep:sleep_{ordinal}");
        if self
            .state
            .completed_steps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains_key(&timer_key)
        {
            return Ok(());
        }

        let delay_ms = duration_to_delay_ms(duration)?;
        let plan = durable_timer_plan(&self.state.config, timer_key.clone(), delay_ms)?;
        if self
            .state
            .config
            .metadata
            .get("timer_key")
            .is_some_and(|resumed| resumed == &timer_key)
        {
            let resumed_activation = self
                .state
                .config
                .metadata
                .get("activation_id")
                .map(String::as_str)
                .unwrap_or_default();
            if resumed_activation != plan.expected_activation_id {
                return Err(activation_error(
                    crate::error::ErrorCode::NondeterministicReplay,
                    "timer resume authority does not match the deterministic sleep activation",
                    (!resumed_activation.is_empty()).then(|| resumed_activation.to_string()),
                    None,
                ));
            }
            self.state
                .completed_steps
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(timer_key, Value::Null);
            return Ok(());
        }

        let client = self
            .state
            .config
            .timer_activation_client
            .as_ref()
            .ok_or_else(|| {
                activation_error(
                    crate::error::ErrorCode::DurabilityUnavailable,
                    "durable timer activation client is unavailable",
                    None,
                    None,
                )
            })?;
        let decision = client.begin_timer(plan.request).await?;
        let execution = match decision {
            ActivationDecision::Execute(receipt) => receipt,
            decision => return Err(timer_decision_error(decision)),
        };
        let completed_steps = self
            .state
            .completed_steps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let continuation = timer_continuation(&self.state.config.metadata, completed_steps)?;
        Err(SdkError::DurableSuspension {
            suspension: Box::new(WorkerSuspension {
                activation_id: execution.activation_id,
                attempt: execution.attempt,
                fence_token: execution.fence_token,
                timer_key,
                ready_at_ms: 0,
                input_digest: plan.input_digest,
                definition_digest: plan.definition_digest,
                continuation,
                delay_ms,
            }),
        })
    }
}

const ACTIVATION_IDENTITY_DOMAIN: &[u8] = b"agnt5.activation.identity.v1\0";
const ACTIVATION_DEFINITION_DOMAIN: &[u8] = b"agnt5.activation.definition.v1\0";
const DURABLE_ACTIVATION_V1: &[u8] = b"durable_activation_v1";

struct DurableTimerPlan {
    request: BeginActivationRequest,
    expected_activation_id: String,
    input_digest: Vec<u8>,
    definition_digest: Vec<u8>,
}

fn durable_timer_plan(
    config: &ContextConfig,
    timer_key: String,
    delay_ms: i64,
) -> Result<DurableTimerPlan> {
    let metadata = &config.metadata;
    let project_id = metadata
        .get("project_id")
        .filter(|value| !value.is_empty())
        .cloned()
        .unwrap_or_else(|| config.tenant_id.clone());
    let worker_session_id = metadata
        .get("worker_session_id")
        .or_else(|| metadata.get("worker_id"))
        .cloned()
        .unwrap_or_default();
    let run_authority = metadata
        .get("run_authority")
        .cloned()
        .or_else(|| config.invocation_id.clone())
        .unwrap_or_else(|| config.run_id.clone());
    let lease_authority = metadata
        .get("lease_authority")
        .or_else(|| metadata.get("lease_id"))
        .cloned()
        .unwrap_or_default();
    let component_name = metadata
        .get("component_name")
        .cloned()
        .unwrap_or_else(|| config.step_id.clone());
    let definition_version = metadata
        .get("activation_definition_version")
        .cloned()
        .unwrap_or_default();
    if project_id.is_empty()
        || config.run_id.is_empty()
        || worker_session_id.is_empty()
        || run_authority.is_empty()
        || lease_authority.is_empty()
        || component_name.is_empty()
        || definition_version.is_empty()
    {
        return Err(activation_error(
            crate::error::ErrorCode::DurabilityUnavailable,
            "durable timer requires project, run, worker-session, run, lease, and definition authority",
            None,
            None,
        ));
    }

    let canonical_input = serde_json::to_vec(&serde_json::json!([
        "object",
        [
            ["delay_ms", ["i64", delay_ms.to_string()]],
            ["timer_key", ["string", timer_key.clone()]],
        ]
    ]))?;
    let input_digest = Sha256::digest(canonical_input).to_vec();
    let artifact = decode_sha256(
        metadata
            .get("activation_artifact_sha256")
            .map(String::as_str)
            .unwrap_or_default(),
    )?;
    let canonical_config = metadata
        .get("activation_definition_config")
        .map(String::as_bytes)
        .unwrap_or(b"[\"object\",[]]");
    let parsed_config: Value = serde_json::from_slice(canonical_config).map_err(|error| {
        activation_error(
            crate::error::ErrorCode::InvalidInput,
            format!("activation definition config is not valid canonical JSON: {error}"),
            None,
            None,
        )
    })?;
    if serde_json::to_vec(&parsed_config)? != canonical_config {
        return Err(activation_error(
            crate::error::ErrorCode::InvalidInput,
            "activation definition config is not canonically encoded",
            None,
            None,
        ));
    }
    let mut definition_bytes = Vec::from(ACTIVATION_DEFINITION_DOMAIN);
    for part in [
        artifact.as_slice(),
        component_name.as_bytes(),
        definition_version.as_bytes(),
        DURABLE_ACTIVATION_V1,
        canonical_config,
    ] {
        push_frame(&mut definition_bytes, part);
    }
    let definition_digest = Sha256::digest(definition_bytes).to_vec();
    let parent_activation_id = metadata
        .get("parent_activation_id")
        .cloned()
        .unwrap_or_default();
    let expected_activation_id = activation_id(
        &project_id,
        &config.run_id,
        &parent_activation_id,
        ActivationKind::Timer,
        &timer_key,
    );
    let request = BeginActivationRequest {
        project_id,
        run_id: config.run_id.clone(),
        parent_activation_id,
        kind: ActivationKind::Timer as i32,
        stable_key: timer_key,
        input_digest: input_digest.clone(),
        definition_digest: definition_digest.clone(),
        recovery_policy: ActivationRecoveryPolicy::DurableSteps as i32,
        worker_session_id,
        run_authority: run_authority.into_bytes(),
        lease_authority: lease_authority.into_bytes(),
    };
    Ok(DurableTimerPlan {
        request,
        expected_activation_id,
        input_digest,
        definition_digest,
    })
}

fn duration_to_delay_ms(duration: Duration) -> Result<i64> {
    let nanos = duration.as_nanos();
    let millis = nanos.saturating_add(999_999) / 1_000_000;
    i64::try_from(millis).map_err(|_| SdkError::InvalidArgument {
        message: "timer duration exceeds maximum supported range".to_string(),
        argument: Some("duration".to_string()),
    })
}

fn activation_id(
    project_id: &str,
    run_id: &str,
    parent_activation_id: &str,
    kind: ActivationKind,
    stable_key: &str,
) -> String {
    let mut encoded = Vec::from(ACTIVATION_IDENTITY_DOMAIN);
    for part in [
        project_id.as_bytes(),
        run_id.as_bytes(),
        parent_activation_id.as_bytes(),
    ] {
        push_frame(&mut encoded, part);
    }
    encoded.extend_from_slice(&(kind as u32).to_be_bytes());
    push_frame(&mut encoded, stable_key.as_bytes());
    format!(
        "actv1_{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(encoded))
    )
}

fn push_frame(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(&(value.len() as u64).to_be_bytes());
    target.extend_from_slice(value);
}

fn decode_sha256(value: &str) -> Result<Vec<u8>> {
    if value.is_empty() {
        return Err(activation_error(
            crate::error::ErrorCode::DurabilityUnavailable,
            "activation artifact SHA-256 is unavailable",
            None,
            None,
        ));
    }
    let decoded = hex::decode(value)
        .ok()
        .filter(|bytes| bytes.len() == 32)
        .or_else(|| {
            [
                base64::engine::general_purpose::STANDARD.decode(value),
                base64::engine::general_purpose::STANDARD_NO_PAD.decode(value),
                base64::engine::general_purpose::URL_SAFE.decode(value),
                base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(value),
            ]
            .into_iter()
            .find_map(|decoded| decoded.ok().filter(|bytes| bytes.len() == 32))
        });
    decoded.ok_or_else(|| {
        activation_error(
            crate::error::ErrorCode::InvalidInput,
            "activation artifact SHA-256 must encode exactly 32 bytes",
            None,
            None,
        )
    })
}

fn completed_steps(metadata: &HashMap<String, String>) -> serde_json::Map<String, Value> {
    metadata
        .get("completed_steps")
        .and_then(|value| serde_json::from_str::<serde_json::Map<String, Value>>(value).ok())
        .unwrap_or_default()
}

fn timer_continuation(
    metadata: &HashMap<String, String>,
    completed_steps: serde_json::Map<String, Value>,
) -> Result<Vec<u8>> {
    let mut continuation = serde_json::Map::new();
    if !completed_steps.is_empty() {
        continuation.insert(
            "completed_steps".to_string(),
            Value::Object(completed_steps),
        );
    }
    for key in ["step_events", "workflow_state"] {
        if let Some(value) = metadata.get(key) {
            if let Ok(parsed) = serde_json::from_str(value) {
                continuation.insert(key.to_string(), parsed);
            }
        }
    }
    if let Some(value) = metadata.get("workflow_correlation_id") {
        continuation.insert(
            "workflow_correlation_id".to_string(),
            Value::String(value.clone()),
        );
    }
    serde_json::to_vec(&Value::Object(continuation)).map_err(Into::into)
}

fn timer_decision_error(decision: ActivationDecision) -> SdkError {
    match decision {
        ActivationDecision::Wait { activation_id, .. } => activation_error(
            crate::error::ErrorCode::ActivationContended,
            "timer activation is already executing",
            Some(activation_id),
            None,
        ),
        ActivationDecision::Conflict {
            activation_id,
            receipt,
        } => activation_error(
            crate::error::ErrorCode::NondeterministicReplay,
            receipt.message,
            Some(activation_id),
            None,
        ),
        ActivationDecision::Cancelled {
            activation_id,
            attempt,
            ..
        } => activation_error(
            crate::error::ErrorCode::ActivationCancelled,
            "timer activation was cancelled",
            Some(activation_id),
            Some(attempt),
        ),
        ActivationDecision::UnknownOutcome { activation_id, .. } => activation_error(
            crate::error::ErrorCode::UnknownOutcome,
            "timer activation has an unknown outcome",
            Some(activation_id),
            None,
        ),
        ActivationDecision::Replay(receipt) => activation_error(
            crate::error::ErrorCode::UnknownOutcome,
            "timer activation unexpectedly returned a terminal replay",
            Some(receipt.activation_id),
            Some(receipt.attempt),
        ),
        ActivationDecision::Execute(receipt) => activation_error(
            crate::error::ErrorCode::UnknownOutcome,
            "timer activation execution receipt was not consumed",
            Some(receipt.activation_id),
            Some(receipt.attempt),
        ),
    }
}

fn activation_error(
    code: crate::error::ErrorCode,
    message: impl Into<String>,
    activation_id: Option<String>,
    attempt: Option<u32>,
) -> SdkError {
    SdkError::Activation {
        message: message.into(),
        code,
        activation_id,
        attempt,
    }
}

#[derive(Debug, Clone)]
pub struct LanguageModelNamespace {
    state: Arc<ContextState>,
}

impl LanguageModelNamespace {
    fn new(state: Arc<ContextState>) -> Self {
        Self { state }
    }

    pub async fn generate(&self, _request: serde_json::Value) -> Result<Value> {
        let _ = &self.state;
        Err(SdkError::Unavailable {
            message: "Language model generation not yet implemented".to_string(),
            service: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use serde_json::json;

    use crate::context::config::ContextConfig;
    use crate::context::registry::{FunctionCall, FunctionRegistry};
    use crate::context::TimerActivationClient;
    use crate::error::{Result, SdkError};
    use crate::pb::{ActivationKind, BeginActivationRequest};
    use crate::runtime_adapter::{ActivationDecision, ActivationExecutionReceipt};

    use super::{activation_id, CoreContext, FunctionStatus};

    #[derive(Debug)]
    struct TimerActivationStub {
        requests: Mutex<Vec<BeginActivationRequest>>,
        decision: ActivationDecision,
    }

    #[async_trait::async_trait]
    impl TimerActivationClient for TimerActivationStub {
        async fn begin_timer(&self, request: BeginActivationRequest) -> Result<ActivationDecision> {
            self.requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(request);
            Ok(self.decision.clone())
        }
    }

    fn durable_timer_config() -> ContextConfig {
        ContextConfig::new("project-1", "session-1", "run-1", "workflow", 0)
            .with_invocation_id("run-1")
            .with_metadata("durable_suspension_v1", "true")
            .with_metadata("project_id", "project-1")
            .with_metadata("worker_session_id", "worker-1")
            .with_metadata("lease_authority", "lease-1")
            .with_metadata("component_name", "workflow")
            .with_metadata("activation_definition_version", "v1")
            .with_metadata("activation_definition_config", "[\"object\",[]]")
            .with_metadata("activation_artifact_sha256", "00".repeat(32))
    }

    #[test]
    fn context_stores_configuration() {
        let cfg =
            ContextConfig::new("tenant", "session", "run", "step", 0).with_invocation_id("invoke");
        let ctx = CoreContext::new(None, cfg.clone());

        assert_eq!(ctx.config().invocation_id, cfg.invocation_id);
        assert_eq!(ctx.config().tenant_id, cfg.tenant_id);
    }

    #[tokio::test]
    async fn timer_zero_duration_is_an_immediate_noop() {
        let ctx = CoreContext::new(None, ContextConfig::default());
        ctx.timers()
            .sleep(Duration::ZERO)
            .await
            .expect("zero timer");
    }

    #[tokio::test]
    async fn timer_sleep_returns_typed_durable_suspension() {
        let timer_key = "sleep:sleep_0";
        let expected_id = activation_id("project-1", "run-1", "", ActivationKind::Timer, timer_key);
        let stub = Arc::new(TimerActivationStub {
            requests: Mutex::new(Vec::new()),
            decision: ActivationDecision::Execute(ActivationExecutionReceipt {
                activation_id: expected_id.clone(),
                attempt: 1,
                fence_token: b"fence-1".to_vec(),
                accepted_journal_offset: 7,
            }),
        });
        let config = durable_timer_config().with_timer_activation_client(stub.clone());
        let ctx = CoreContext::new(None, config);

        let error = ctx
            .timers()
            .sleep(Duration::from_millis(2_500))
            .await
            .expect_err("timer must suspend");
        let SdkError::DurableSuspension { suspension } = error else {
            panic!("expected typed suspension");
        };
        assert_eq!(suspension.activation_id, expected_id);
        assert_eq!(
            suspension.activation_id,
            "actv1_D1ifjG2fzE7kuiPRcKCoqPqIOKFwcesV0ttRbcK3Og0"
        );
        assert_eq!(suspension.timer_key, timer_key);
        assert_eq!(suspension.delay_ms, 2_500);
        assert_eq!(suspension.fence_token, b"fence-1");
        assert_eq!(suspension.input_digest.len(), 32);
        assert_eq!(suspension.definition_digest.len(), 32);
        assert_eq!(
            hex::encode(&suspension.input_digest),
            "d1b146bf89e149e9df5ead1964ea34dc77c0b4b9e6b9d2dc8d7e2c58f0501ee7"
        );
        assert_eq!(
            hex::encode(&suspension.definition_digest),
            "18372aa11ce6f79e1e218982c1bc22976cfc28f868ac4a1c13811e4416e973a6"
        );

        let requests = stub
            .requests
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].kind, ActivationKind::Timer as i32);
        assert_eq!(requests[0].stable_key, timer_key);
        assert_eq!(requests[0].run_authority, b"run-1");
        assert_eq!(requests[0].lease_authority, b"lease-1");
    }

    #[tokio::test]
    async fn timer_resume_requires_the_matching_deterministic_activation() {
        let timer_key = "sleep:sleep_0";
        let expected_id = activation_id("project-1", "run-1", "", ActivationKind::Timer, timer_key);
        let config = durable_timer_config()
            .with_metadata("timer_key", timer_key)
            .with_metadata("activation_id", expected_id);
        let ctx = CoreContext::new(None, config);

        ctx.timers()
            .sleep(Duration::from_secs(1))
            .await
            .expect("matching resume");
    }

    #[tokio::test]
    async fn timer_replay_skips_completed_sleep_before_current_resume() {
        let second_key = "sleep:sleep_1";
        let second_id = activation_id("project-1", "run-1", "", ActivationKind::Timer, second_key);
        let config = durable_timer_config()
            .with_metadata("completed_steps", r#"{"sleep:sleep_0":null}"#)
            .with_metadata("timer_key", second_key)
            .with_metadata("activation_id", second_id);
        let ctx = CoreContext::new(None, config);

        ctx.timers()
            .sleep(Duration::from_secs(1))
            .await
            .expect("completed first sleep");
        ctx.timers()
            .sleep(Duration::from_secs(1))
            .await
            .expect("current second sleep");
    }

    #[tokio::test]
    async fn timer_continuation_includes_sleep_completed_during_replay() {
        let first_key = "sleep:sleep_0";
        let first_id = activation_id("project-1", "run-1", "", ActivationKind::Timer, first_key);
        let second_key = "sleep:sleep_1";
        let second_id = activation_id("project-1", "run-1", "", ActivationKind::Timer, second_key);
        let stub = Arc::new(TimerActivationStub {
            requests: Mutex::new(Vec::new()),
            decision: ActivationDecision::Execute(ActivationExecutionReceipt {
                activation_id: second_id,
                attempt: 1,
                fence_token: b"fence-2".to_vec(),
                accepted_journal_offset: 9,
            }),
        });
        let config = durable_timer_config()
            .with_metadata("timer_key", first_key)
            .with_metadata("activation_id", first_id)
            .with_timer_activation_client(stub);
        let ctx = CoreContext::new(None, config);

        ctx.timers()
            .sleep(Duration::from_secs(1))
            .await
            .expect("resume first sleep");
        let error = ctx
            .timers()
            .sleep(Duration::from_secs(1))
            .await
            .expect_err("second sleep must suspend");
        let SdkError::DurableSuspension { suspension } = error else {
            panic!("expected typed suspension");
        };
        let continuation: serde_json::Value =
            serde_json::from_slice(&suspension.continuation).expect("continuation json");
        assert_eq!(
            continuation["completed_steps"][first_key],
            serde_json::Value::Null
        );
    }

    #[tokio::test]
    async fn function_namespace_invokes_registered_function() {
        let registry = Arc::new(FunctionRegistry::new());
        registry.register("analytics", "process", |call, ctx| async move {
            assert_eq!(ctx.run_id, "run");
            assert!(call.metadata.get("corr_id").is_some());
            Ok(json!({
                "echo": call.payload,
            }))
        });

        let cfg = ContextConfig::new("tenant", "session", "run", "step", 0)
            .with_function_registry(Arc::clone(&registry));
        let ctx = CoreContext::new(None, cfg);
        let request = FunctionCall::new("analytics", "process", json!({"foo": "bar"}))
            .with_metadata("corr_id", "123");

        let handle = ctx.functions().call(request).await.expect("call succeeds");
        assert_eq!(handle.status(), FunctionStatus::Succeeded);

        let result = handle.result().await;
        assert_eq!(result.status, FunctionStatus::Succeeded);
        assert_eq!(result.output.unwrap()["echo"]["foo"], "bar");
        assert!(!result.invocation_id.is_empty());
    }

    #[tokio::test]
    async fn function_namespace_handles_handler_error() {
        let registry = Arc::new(FunctionRegistry::new());
        registry.register("svc", "fail", |_call, _ctx| async move {
            Err(crate::error::SdkError::Invocation {
                message: "boom".into(),
                function_name: None,
            })
        });

        let cfg = ContextConfig::new("tenant", "session", "run", "step", 0)
            .with_function_registry(Arc::clone(&registry));
        let ctx = CoreContext::new(None, cfg);
        let request = FunctionCall::new("svc", "fail", json!({}));

        let handle = ctx.functions().call(request).await.expect("call succeeds");
        let result = handle.result().await;
        assert_eq!(result.status, FunctionStatus::Failed);
        assert!(result.error.unwrap().contains("boom"));
    }

    #[tokio::test]
    async fn function_namespace_errors_when_unregistered() {
        let cfg = ContextConfig::new("tenant", "session", "run", "step", 0);
        let ctx = CoreContext::new(None, cfg);
        let request = FunctionCall::new("missing", "handler", json!({}));

        let err = ctx.functions().call(request).await;
        assert!(matches!(err, Err(SdkError::InvalidArgument { .. })));
    }
}
