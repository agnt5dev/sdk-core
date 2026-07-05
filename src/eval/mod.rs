//! AGNT5 Evaluation Framework
//!
//! Provides scorers for evaluating AI component outputs:
//! - Deterministic scorers: `exact_match`, `contains`, `regex_match`,
//!   `json_valid`, `json_schema`, `numeric_range`, `levenshtein`.
//! - LLM-as-judge (`llm_judge`) for semantic evaluation; async-only,
//!   routed through the LM client rather than the sync fast path.
//! - Trace assertions for glassbox testing.

pub mod agent_trace_metrics;
pub mod builtin_scorer;
pub mod deterministic;
pub mod llm_judge;
pub mod normalized;
pub mod trace;
pub mod trace_eval_metrics;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Input to a scorer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScorerInput {
    /// The actual output from the component
    pub output: Value,
    /// Expected output (optional, for comparison scorers)
    pub expected: Option<Value>,
    /// Original input (optional, for context-aware scoring)
    pub input: Option<Value>,
    /// Event trace (optional, for glassbox scoring)
    pub trace: Option<Vec<TraceEvent>>,
}

impl ScorerInput {
    /// Create a new ScorerInput with just an output
    pub fn new(output: Value) -> Self {
        Self {
            output,
            expected: None,
            input: None,
            trace: None,
        }
    }

    /// Set the expected output for comparison
    pub fn with_expected(mut self, expected: Value) -> Self {
        self.expected = Some(expected);
        self
    }

    /// Set the original input for context-aware scoring
    pub fn with_input(mut self, input: Value) -> Self {
        self.input = Some(input);
        self
    }

    /// Set the event trace for glassbox scoring
    pub fn with_trace(mut self, trace: Vec<TraceEvent>) -> Self {
        self.trace = Some(trace);
        self
    }
}

/// Result from a scorer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScorerResult {
    /// Score between 0.0 and 1.0
    pub score: f64,
    /// Whether the score passes a threshold (optional)
    pub passed: Option<bool>,
    /// Categorical label (optional)
    pub label: Option<String>,
    /// Human-readable explanation (optional)
    pub explanation: Option<String>,
    /// Additional metadata (optional)
    pub metadata: Option<Value>,
}

impl ScorerResult {
    /// Create a new passing result
    pub fn pass() -> Self {
        Self {
            score: 1.0,
            passed: Some(true),
            label: Some("pass".into()),
            explanation: None,
            metadata: None,
        }
    }

    /// Create a new failing result
    pub fn fail() -> Self {
        Self {
            score: 0.0,
            passed: Some(false),
            label: Some("fail".into()),
            explanation: None,
            metadata: None,
        }
    }

    /// Create a result with a specific score
    pub fn with_score(score: f64) -> Self {
        Self {
            score: score.clamp(0.0, 1.0),
            passed: Some(score >= 0.5),
            label: None,
            explanation: None,
            metadata: None,
        }
    }

    /// Set whether this result passes
    pub fn passed(mut self, passed: bool) -> Self {
        self.passed = Some(passed);
        self
    }

    /// Set a label for this result
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Set an explanation for this result
    pub fn explanation(mut self, explanation: impl Into<String>) -> Self {
        self.explanation = Some(explanation.into());
        self
    }

    /// Set metadata for this result
    pub fn metadata(mut self, metadata: Value) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

/// Event from execution trace (for glassbox testing)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceEvent {
    /// Event type (e.g., "run.started", "lm.call.completed")
    pub event_type: String,
    /// Unique event identifier
    pub event_id: String,
    /// Correlation ID linking related events
    pub correlation_id: String,
    /// Parent correlation ID (for hierarchical events)
    pub parent_correlation_id: Option<String>,
    /// Timestamp in nanoseconds
    pub timestamp_ns: i64,
    /// Event-specific data
    pub data: Value,
    /// Optional name (e.g., step name, function name)
    pub name: Option<String>,
}

impl TraceEvent {
    /// Create a new trace event
    pub fn new(event_type: impl Into<String>, event_id: impl Into<String>) -> Self {
        Self {
            event_type: event_type.into(),
            event_id: event_id.into(),
            correlation_id: String::new(),
            parent_correlation_id: None,
            timestamp_ns: 0,
            data: Value::Object(Default::default()),
            name: None,
        }
    }

    /// Set the correlation ID
    pub fn correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = id.into();
        self
    }

    /// Set the parent correlation ID
    pub fn parent_correlation_id(mut self, id: impl Into<String>) -> Self {
        self.parent_correlation_id = Some(id.into());
        self
    }

    /// Set the timestamp
    pub fn timestamp_ns(mut self, ts: i64) -> Self {
        self.timestamp_ns = ts;
        self
    }

    /// Set the event data
    pub fn data(mut self, data: Value) -> Self {
        self.data = data;
        self
    }

    /// Set the name
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

// Re-export commonly used items
pub use deterministic::{
    contains, exact_match, json_schema, json_valid, levenshtein, numeric_range, regex_match,
    ContainsConfig, ExactMatchConfig, JsonSchemaConfig, LevenshteinConfig, NumericRangeConfig,
    RegexConfig,
};
pub use llm_judge::{llm_judge, LlmJudgeConfig};
pub use normalized::{
    NormalizedHandoff, NormalizedLlmCall, NormalizedSession, NormalizedSessionSummary,
    NormalizedSpan, NormalizedToolCall, NormalizedTraceError, RedactionPolicySnapshot,
    TraceArtifactManifest, NORMALIZED_SESSION_SCHEMA, NORMALIZED_SPAN_SCHEMA,
    TRACE_ARTIFACT_MANIFEST_SCHEMA, TRACE_EVAL_CONTEXT_SCHEMA,
};
pub use trace::{trace_score, TraceAssertion};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_scorer_input_builder() {
        let input = ScorerInput::new(json!("hello"))
            .with_expected(json!("hello"))
            .with_input(json!({"query": "test"}));

        assert_eq!(input.output, json!("hello"));
        assert_eq!(input.expected, Some(json!("hello")));
        assert_eq!(input.input, Some(json!({"query": "test"})));
        assert!(input.trace.is_none());
    }

    #[test]
    fn test_scorer_result_builder() {
        let result = ScorerResult::with_score(0.8)
            .passed(true)
            .label("good")
            .explanation("High similarity");

        assert_eq!(result.score, 0.8);
        assert_eq!(result.passed, Some(true));
        assert_eq!(result.label, Some("good".into()));
        assert_eq!(result.explanation, Some("High similarity".into()));
    }

    #[test]
    fn test_trace_event_builder() {
        let event = TraceEvent::new("lm.call.completed", "event-1")
            .correlation_id("corr-1")
            .timestamp_ns(1000000)
            .data(json!({"total_tokens": 500}))
            .name("chat");

        assert_eq!(event.event_type, "lm.call.completed");
        assert_eq!(event.event_id, "event-1");
        assert_eq!(event.correlation_id, "corr-1");
        assert_eq!(event.timestamp_ns, 1000000);
        assert_eq!(event.name, Some("chat".into()));
    }
}
