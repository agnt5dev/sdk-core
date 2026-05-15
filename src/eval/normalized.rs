//! Normalized trace/session DTOs for eval contracts.
//!
//! Raw prompts, tool arguments, tool results, and model responses should be
//! referenced by hash/ref fields. The safe summary fields are intended for
//! reports, CI gates, and deterministic trace scorers.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const NORMALIZED_SESSION_SCHEMA: &str = "agnt5.eval.normalized_session.v1";
pub const NORMALIZED_SPAN_SCHEMA: &str = "agnt5.eval.normalized_span.v1";
pub const TRACE_ARTIFACT_MANIFEST_SCHEMA: &str = "agnt5.eval.trace_artifact_manifest.v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct NormalizedSession {
    pub schema_version: String,
    pub session_id: String,
    pub project_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
    pub root_run_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub started_at: i64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub ended_at: i64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub turns: Vec<NormalizedTurn>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub spans: Vec<NormalizedSpan>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<NormalizedToolCall>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub llm_calls: Vec<NormalizedLlmCall>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub handoffs: Vec<NormalizedHandoff>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<NormalizedTraceError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_output_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_output_hash: Option<String>,
    pub safe_summary: NormalizedSessionSummary,
    pub redaction_policy_snapshot: RedactionPolicySnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct NormalizedTurn {
    pub turn_index: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_hash: Option<String>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub started_at: i64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub ended_at: i64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes_safe: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct NormalizedSpan {
    pub schema_version: String,
    pub span_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_span_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub component_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub component_name: Option<String>,
    pub event_type: String,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub started_at: i64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub ended_at: i64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub duration_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message_sanitized: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes_safe: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct NormalizedToolCall {
    pub call_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub started_at: i64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub ended_at: i64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub duration_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message_sanitized: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes_safe: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct NormalizedLlmCall {
    pub call_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub started_at: i64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub ended_at: i64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub duration_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_hash: Option<String>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub input_tokens: i64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub output_tokens: i64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub total_tokens: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message_sanitized: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes_safe: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct NormalizedHandoff {
    pub handoff_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_component: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_component: Option<String>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub started_at: i64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub ended_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason_safe: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes_safe: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct NormalizedTraceError {
    pub error_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message_sanitized: Option<String>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub timestamp: i64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes_safe: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct NormalizedSessionSummary {
    pub span_count: usize,
    pub tool_call_count: usize,
    pub llm_call_count: usize,
    pub handoff_count: usize,
    pub error_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_types: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub component_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RedactionPolicySnapshot {
    pub schema_version: String,
    pub mode: String,
    pub raw_payloads_by_ref: bool,
    pub sanitized_fields_only: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocked_keys: Vec<String>,
}

impl Default for RedactionPolicySnapshot {
    fn default() -> Self {
        Self {
            schema_version: TRACE_ARTIFACT_MANIFEST_SCHEMA.to_string(),
            mode: "managed_default".to_string(),
            raw_payloads_by_ref: true,
            sanitized_fields_only: true,
            blocked_keys: vec![
                "input".into(),
                "output".into(),
                "prompt".into(),
                "messages".into(),
                "arguments".into(),
                "args".into(),
                "result".into(),
                "response".into(),
                "content".into(),
                "tool_result".into(),
                "tool_args".into(),
            ],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct TraceArtifactManifest {
    pub schema_version: String,
    pub project_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub experiment_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub experiment_run_item_id: Option<String>,
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    pub normalized_session_ref: String,
    pub normalized_session_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_trace_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_trace_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression: Option<String>,
    pub redaction_policy_snapshot: RedactionPolicySnapshot,
    pub counts: NormalizedSessionSummary,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub safe_summary: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub created_at: i64,
}

fn is_zero(value: &i64) -> bool {
    *value == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalized_session_serializes_refs_without_raw_payloads() {
        let session = NormalizedSession {
            schema_version: NORMALIZED_SESSION_SCHEMA.to_string(),
            session_id: "session_1".to_string(),
            project_id: "proj_1".to_string(),
            root_run_id: "run_1".to_string(),
            spans: vec![NormalizedSpan {
                schema_version: NORMALIZED_SPAN_SCHEMA.to_string(),
                span_id: "span_1".to_string(),
                event_type: "lm.call.completed".to_string(),
                input_ref: Some("s3://managed/input.json".to_string()),
                output_hash: Some("hmac:output".to_string()),
                ..Default::default()
            }],
            safe_summary: NormalizedSessionSummary {
                span_count: 1,
                ..Default::default()
            },
            redaction_policy_snapshot: RedactionPolicySnapshot::default(),
            ..Default::default()
        };

        let encoded = serde_json::to_string(&session).expect("serialize normalized session");
        assert!(encoded.contains(NORMALIZED_SESSION_SCHEMA));
        assert!(encoded.contains("s3://managed/input.json"));
        assert!(!encoded.contains("raw prompt"));
    }
}
