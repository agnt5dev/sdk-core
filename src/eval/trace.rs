//! Trace assertions for glassbox testing.
//!
//! Allows asserting properties of execution traces such as:
//! - Token usage limits
//! - LLM call counts
//! - Event sequences
//! - Step memoization
//! - Error detection
//! - Duration bounds

use super::{ScorerInput, ScorerResult, TraceEvent};
use serde::{Deserialize, Serialize};

/// Assertion types for trace evaluation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TraceAssertion {
    /// Assert total tokens used is at most `max`
    MaxTokens { max: u64 },
    /// Assert number of LLM calls is at most `max`
    MaxLmCalls { max: u32 },
    /// Assert events occur in the specified order (subsequence match)
    EventSequence { events: Vec<String> },
    /// Assert a specific step was memoized (retrieved from cache)
    StepMemoized { step_name: String },
    /// Assert no error events occurred
    NoErrors,
    /// Assert total duration is under `max_ms` milliseconds
    DurationUnder { max_ms: u64 },
    /// Assert a specific event type occurred at least `min` times
    EventCount { event_type: String, min: u32 },
    /// Assert a custom condition on the trace data
    Custom { name: String, description: String },
}

impl TraceAssertion {
    /// Create a MaxTokens assertion
    pub fn max_tokens(max: u64) -> Self {
        TraceAssertion::MaxTokens { max }
    }

    /// Create a MaxLmCalls assertion
    pub fn max_lm_calls(max: u32) -> Self {
        TraceAssertion::MaxLmCalls { max }
    }

    /// Create an EventSequence assertion
    pub fn event_sequence(events: Vec<String>) -> Self {
        TraceAssertion::EventSequence { events }
    }

    /// Create a StepMemoized assertion
    pub fn step_memoized(step_name: impl Into<String>) -> Self {
        TraceAssertion::StepMemoized {
            step_name: step_name.into(),
        }
    }

    /// Create a NoErrors assertion
    pub fn no_errors() -> Self {
        TraceAssertion::NoErrors
    }

    /// Create a DurationUnder assertion
    pub fn duration_under(max_ms: u64) -> Self {
        TraceAssertion::DurationUnder { max_ms }
    }

    /// Create an EventCount assertion
    pub fn event_count(event_type: impl Into<String>, min: u32) -> Self {
        TraceAssertion::EventCount {
            event_type: event_type.into(),
            min,
        }
    }

    /// Check this assertion against a trace
    pub fn check(&self, trace: &[TraceEvent]) -> AssertionResult {
        match self {
            TraceAssertion::MaxTokens { max } => check_max_tokens(trace, *max),
            TraceAssertion::MaxLmCalls { max } => check_max_lm_calls(trace, *max),
            TraceAssertion::EventSequence { events } => check_event_sequence(trace, events),
            TraceAssertion::StepMemoized { step_name } => check_step_memoized(trace, step_name),
            TraceAssertion::NoErrors => check_no_errors(trace),
            TraceAssertion::DurationUnder { max_ms } => check_duration_under(trace, *max_ms),
            TraceAssertion::EventCount { event_type, min } => {
                check_event_count(trace, event_type, *min)
            }
            TraceAssertion::Custom { name, description } => AssertionResult {
                name: name.clone(),
                passed: false,
                explanation: format!(
                    "Custom assertion '{}' must be evaluated externally",
                    description
                ),
            },
        }
    }
}

/// Result of checking a single assertion
#[derive(Debug, Clone)]
pub struct AssertionResult {
    /// Name of the assertion
    pub name: String,
    /// Whether the assertion passed
    pub passed: bool,
    /// Explanation of the result
    pub explanation: String,
}

fn check_max_tokens(trace: &[TraceEvent], max: u64) -> AssertionResult {
    let total: u64 = trace
        .iter()
        .filter(|e| e.event_type == "lm.call.completed")
        .filter_map(|e| e.data.get("total_tokens").and_then(|v| v.as_u64()))
        .sum();

    AssertionResult {
        name: format!("max_tokens({})", max),
        passed: total <= max,
        explanation: format!("Token usage: {} (max: {})", total, max),
    }
}

fn check_max_lm_calls(trace: &[TraceEvent], max: u32) -> AssertionResult {
    let count = trace
        .iter()
        .filter(|e| e.event_type == "lm.call.completed")
        .count() as u32;

    AssertionResult {
        name: format!("max_lm_calls({})", max),
        passed: count <= max,
        explanation: format!("LLM calls: {} (max: {})", count, max),
    }
}

fn check_event_sequence(trace: &[TraceEvent], events: &[String]) -> AssertionResult {
    let actual_types: Vec<&str> = trace.iter().map(|e| e.event_type.as_str()).collect();

    let mut j = 0;
    for expected in events {
        while j < actual_types.len() && actual_types[j] != expected {
            j += 1;
        }
        if j >= actual_types.len() {
            return AssertionResult {
                name: "event_sequence".into(),
                passed: false,
                explanation: format!("Missing event '{}' in sequence", expected),
            };
        }
        j += 1;
    }

    AssertionResult {
        name: "event_sequence".into(),
        passed: true,
        explanation: "All events found in expected order".into(),
    }
}

fn check_step_memoized(trace: &[TraceEvent], step_name: &str) -> AssertionResult {
    let memoized = trace
        .iter()
        .filter(|e| e.event_type == "workflow.step.completed")
        .filter(|e| e.name.as_deref() == Some(step_name))
        .any(|e| {
            e.data
                .get("is_memoized")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        });

    AssertionResult {
        name: format!("step_memoized({})", step_name),
        passed: memoized,
        explanation: if memoized {
            format!("Step '{}' was memoized", step_name)
        } else {
            format!("Step '{}' was NOT memoized", step_name)
        },
    }
}

fn check_no_errors(trace: &[TraceEvent]) -> AssertionResult {
    let error_types = [
        "run.failed",
        "workflow.step.failed",
        "agent.failed",
        "lm.call.failed",
        "function.failed",
    ];
    let errors: Vec<_> = trace
        .iter()
        .filter(|e| error_types.contains(&e.event_type.as_str()))
        .collect();

    AssertionResult {
        name: "no_errors".into(),
        passed: errors.is_empty(),
        explanation: if errors.is_empty() {
            "No error events found".into()
        } else {
            format!("Found {} error event(s)", errors.len())
        },
    }
}

fn check_duration_under(trace: &[TraceEvent], max_ms: u64) -> AssertionResult {
    let (first, last) = trace.iter().fold((i64::MAX, i64::MIN), |(min, max), e| {
        (min.min(e.timestamp_ns), max.max(e.timestamp_ns))
    });

    let duration_ms = if first == i64::MAX {
        0
    } else {
        (last - first) / 1_000_000
    };

    AssertionResult {
        name: format!("duration_under({}ms)", max_ms),
        passed: duration_ms <= max_ms as i64,
        explanation: format!("Duration: {}ms (max: {}ms)", duration_ms, max_ms),
    }
}

fn check_event_count(trace: &[TraceEvent], event_type: &str, min: u32) -> AssertionResult {
    let count = trace.iter().filter(|e| e.event_type == event_type).count() as u32;

    AssertionResult {
        name: format!("event_count({}, min={})", event_type, min),
        passed: count >= min,
        explanation: format!(
            "Event '{}' occurred {} times (min: {})",
            event_type, count, min
        ),
    }
}

/// Score a trace against multiple assertions.
///
/// # Arguments
/// * `input` - ScorerInput containing the trace to evaluate
/// * `assertions` - List of assertions to check
///
/// # Returns
/// ScorerResult with aggregate score (proportion passed)
pub fn trace_score(input: &ScorerInput, assertions: &[TraceAssertion]) -> ScorerResult {
    let trace = match &input.trace {
        Some(t) => t,
        None => {
            return ScorerResult {
                score: 0.0,
                passed: Some(false),
                label: Some("error".into()),
                explanation: Some("No trace provided for glassbox scoring".into()),
                metadata: None,
            }
        }
    };

    if assertions.is_empty() {
        return ScorerResult {
            score: 1.0,
            passed: Some(true),
            label: Some("pass".into()),
            explanation: Some("No assertions to check".into()),
            metadata: None,
        };
    }

    let results: Vec<AssertionResult> = assertions.iter().map(|a| a.check(trace)).collect();

    let passed_count = results.iter().filter(|r| r.passed).count();
    let score = passed_count as f64 / results.len() as f64;
    let all_passed = results.iter().all(|r| r.passed);

    let failed: Vec<_> = results.iter().filter(|r| !r.passed).collect();
    let explanation = if failed.is_empty() {
        "All assertions passed".into()
    } else {
        format!(
            "Failed assertions:\n{}",
            failed
                .iter()
                .map(|r| format!("- {}: {}", r.name, r.explanation))
                .collect::<Vec<_>>()
                .join("\n")
        )
    };

    ScorerResult {
        score,
        passed: Some(all_passed),
        label: Some(if all_passed {
            "pass".into()
        } else {
            "fail".into()
        }),
        explanation: Some(explanation),
        metadata: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn create_test_trace() -> Vec<TraceEvent> {
        vec![
            TraceEvent {
                event_type: "run.started".into(),
                event_id: "1".into(),
                correlation_id: "a".into(),
                parent_correlation_id: None,
                timestamp_ns: 1_000_000,
                data: json!({}),
                name: None,
            },
            TraceEvent {
                event_type: "lm.call.completed".into(),
                event_id: "2".into(),
                correlation_id: "a".into(),
                parent_correlation_id: None,
                timestamp_ns: 2_000_000,
                data: json!({"total_tokens": 500}),
                name: Some("chat".into()),
            },
            TraceEvent {
                event_type: "workflow.step.completed".into(),
                event_id: "3".into(),
                correlation_id: "a".into(),
                parent_correlation_id: None,
                timestamp_ns: 3_000_000,
                data: json!({"is_memoized": true}),
                name: Some("fetch_data".into()),
            },
            TraceEvent {
                event_type: "run.completed".into(),
                event_id: "4".into(),
                correlation_id: "a".into(),
                parent_correlation_id: None,
                timestamp_ns: 4_000_000,
                data: json!({}),
                name: None,
            },
        ]
    }

    #[test]
    fn test_max_tokens() {
        let trace = create_test_trace();

        // Should pass
        let result = TraceAssertion::max_tokens(1000).check(&trace);
        assert!(result.passed);

        // Should fail
        let result = TraceAssertion::max_tokens(100).check(&trace);
        assert!(!result.passed);
    }

    #[test]
    fn test_max_lm_calls() {
        let trace = create_test_trace();

        // Should pass
        let result = TraceAssertion::max_lm_calls(5).check(&trace);
        assert!(result.passed);

        // Should fail
        let result = TraceAssertion::max_lm_calls(0).check(&trace);
        assert!(!result.passed);
    }

    #[test]
    fn test_event_sequence() {
        let trace = create_test_trace();

        // Should pass - subsequence present
        let result = TraceAssertion::event_sequence(vec![
            "run.started".into(),
            "lm.call.completed".into(),
            "run.completed".into(),
        ])
        .check(&trace);
        assert!(result.passed);

        // Should fail - wrong order
        let result =
            TraceAssertion::event_sequence(vec!["run.completed".into(), "run.started".into()])
                .check(&trace);
        assert!(!result.passed);

        // Should fail - missing event
        let result =
            TraceAssertion::event_sequence(vec!["run.started".into(), "nonexistent".into()])
                .check(&trace);
        assert!(!result.passed);
    }

    #[test]
    fn test_step_memoized() {
        let trace = create_test_trace();

        // Should pass - step was memoized
        let result = TraceAssertion::step_memoized("fetch_data").check(&trace);
        assert!(result.passed);

        // Should fail - step not found
        let result = TraceAssertion::step_memoized("unknown_step").check(&trace);
        assert!(!result.passed);
    }

    #[test]
    fn test_no_errors() {
        let trace = create_test_trace();

        // Should pass - no errors
        let result = TraceAssertion::no_errors().check(&trace);
        assert!(result.passed);

        // Add an error event
        let mut trace_with_error = trace.clone();
        trace_with_error.push(TraceEvent {
            event_type: "run.failed".into(),
            event_id: "5".into(),
            correlation_id: "a".into(),
            parent_correlation_id: None,
            timestamp_ns: 5_000_000,
            data: json!({"error": "Something went wrong"}),
            name: None,
        });

        let result = TraceAssertion::no_errors().check(&trace_with_error);
        assert!(!result.passed);
    }

    #[test]
    fn test_duration_under() {
        let trace = create_test_trace();

        // Trace is 3ms (4_000_000 - 1_000_000 ns = 3ms)
        let result = TraceAssertion::duration_under(10).check(&trace);
        assert!(result.passed);

        let result = TraceAssertion::duration_under(1).check(&trace);
        assert!(!result.passed);
    }

    #[test]
    fn test_trace_score() {
        let trace = create_test_trace();
        let input = ScorerInput::new(json!("result")).with_trace(trace);

        let assertions = vec![
            TraceAssertion::max_tokens(1000),
            TraceAssertion::max_lm_calls(5),
            TraceAssertion::no_errors(),
        ];

        let result = trace_score(&input, &assertions);
        assert_eq!(result.score, 1.0);
        assert_eq!(result.passed, Some(true));

        // Add a failing assertion
        let assertions = vec![
            TraceAssertion::max_tokens(1000),
            TraceAssertion::max_lm_calls(0), // This will fail
            TraceAssertion::no_errors(),
        ];

        let result = trace_score(&input, &assertions);
        assert!((result.score - 0.666).abs() < 0.01); // 2/3 passed
        assert_eq!(result.passed, Some(false));
    }

    #[test]
    fn test_trace_score_no_trace() {
        let input = ScorerInput::new(json!("result"));
        let assertions = vec![TraceAssertion::no_errors()];

        let result = trace_score(&input, &assertions);
        assert_eq!(result.score, 0.0);
        assert_eq!(result.label, Some("error".into()));
    }

    #[test]
    fn test_trace_score_no_assertions() {
        let trace = create_test_trace();
        let input = ScorerInput::new(json!("result")).with_trace(trace);

        let result = trace_score(&input, &[]);
        assert_eq!(result.score, 1.0);
        assert_eq!(result.passed, Some(true));
    }
}
