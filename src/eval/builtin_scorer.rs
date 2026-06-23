//! Built-in scorer execution for worker-side interception.
//!
//! When the platform dispatches a scorer execution request, the worker can
//! handle known built-in scorers directly in Rust without crossing the FFI
//! boundary to the language SDK. Unknown scorers fall through to the language handler.

use super::deterministic::{
    contains, exact_match, json_schema, json_valid, levenshtein, numeric_range, regex_match,
    ContainsConfig, ExactMatchConfig, JsonSchemaConfig, LevenshteinConfig, NumericRangeConfig,
    RegexConfig,
};
use super::{ScorerInput, ScorerResult};
use serde_json::Value;

/// Names of all built-in scorers that can be handled in Rust.
pub const BUILTIN_SCORER_NAMES: &[&str] = &[
    "exact_match",
    "contains",
    "regex_match",
    "json_valid",
    "json_schema",
    "numeric_range",
    "levenshtein",
    "llm_judge",
    "agent_judge",
    "step_efficiency",
    "plan_quality",
    "plan_adherence",
];

/// Check if a scorer name is a known built-in scorer.
pub fn is_builtin_scorer(name: &str) -> bool {
    BUILTIN_SCORER_NAMES.contains(&name)
}

/// Check if a scorer name can be executed directly in Rust (without FFI).
///
/// `llm_judge` and `agent_judge` are NOT in the sync fast path — they are async
/// (call the LM client)
/// and must be invoked via [`super::llm_judge::llm_judge`] rather than through
/// this entry point.
pub fn can_execute_locally(name: &str) -> bool {
    matches!(
        name,
        "exact_match"
            | "contains"
            | "regex_match"
            | "json_valid"
            | "json_schema"
            | "numeric_range"
            | "levenshtein"
            | "step_efficiency"
            | "plan_quality"
            | "plan_adherence"
    )
}

/// Execute a built-in scorer given the raw input JSON from the platform.
///
/// The platform sends:
/// ```json
/// {
///   "output": <component output>,
///   "expected": <expected output>,
///   "input": <original input>,
///   "config": <scorer-specific config>
/// }
/// ```
///
/// Returns `Some(ScorerResult)` if handled, `None` if the scorer is unknown
/// or not implemented in the Rust fast path.
///
/// Known deterministic built-ins always return a scorer result. Bad request
/// payloads are surfaced as config/input errors instead of falling through to
/// component lookup as "not found".
pub fn execute(scorer_name: &str, input_data: &[u8]) -> Option<ScorerResult> {
    if !can_execute_locally(scorer_name) {
        return None;
    }

    // Parse the input JSON
    let input_json: Value = match serde_json::from_slice(input_data) {
        Ok(value) => value,
        Err(e) => {
            return Some(ScorerResult {
                score: 0.0,
                passed: Some(false),
                label: Some("input_error".into()),
                explanation: Some(format!("Invalid scorer input JSON: {}", e)),
                metadata: None,
            });
        }
    };

    let output = input_json.get("output").cloned().unwrap_or(Value::Null);
    let expected = input_json.get("expected").cloned();
    let input = input_json.get("input").cloned();
    let config = input_json.get("config").cloned().unwrap_or(Value::Null);

    let scorer_input = ScorerInput {
        output,
        expected,
        input,
        trace: None,
    };

    match scorer_name {
        "exact_match" => {
            let cfg: ExactMatchConfig = serde_json::from_value(config).unwrap_or_default();
            Some(exact_match(&scorer_input, &cfg))
        }
        "contains" => {
            // Don't unwrap_or_default — a malformed config there
            // would silently produce an empty pattern that matches
            // every input as "true". Surface the error instead.
            let cfg: ContainsConfig = match serde_json::from_value(config) {
                Ok(c) => c,
                Err(e) => {
                    return Some(ScorerResult {
                        score: 0.0,
                        passed: Some(false),
                        label: Some("config_error".into()),
                        explanation: Some(format!("Invalid contains config: {}", e)),
                        metadata: None,
                    });
                }
            };
            if cfg.pattern.is_empty() {
                return Some(ScorerResult {
                    score: 0.0,
                    passed: Some(false),
                    label: Some("config_error".into()),
                    explanation: Some("contains requires a non-empty `pattern`".into()),
                    metadata: None,
                });
            }
            Some(contains(&scorer_input, &cfg))
        }
        "regex_match" => {
            let cfg: RegexConfig = match serde_json::from_value(config) {
                Ok(c) => c,
                Err(e) => {
                    return Some(ScorerResult {
                        score: 0.0,
                        passed: Some(false),
                        label: Some("error".into()),
                        explanation: Some(format!("Invalid regex_match config: {}", e)),
                        metadata: None,
                    });
                }
            };
            Some(regex_match(&scorer_input, &cfg))
        }
        "json_valid" => Some(json_valid(&scorer_input)),
        "json_schema" => {
            let cfg: JsonSchemaConfig = match serde_json::from_value(config) {
                Ok(c) => c,
                Err(e) => {
                    return Some(ScorerResult {
                        score: 0.0,
                        passed: Some(false),
                        label: Some("config_error".into()),
                        explanation: Some(format!("json_schema requires `schema` in config: {e}")),
                        metadata: None,
                    });
                }
            };
            Some(json_schema(&scorer_input, &cfg))
        }
        "numeric_range" => {
            let cfg: NumericRangeConfig = serde_json::from_value(config).unwrap_or_default();
            Some(numeric_range(&scorer_input, &cfg))
        }
        "levenshtein" => {
            let cfg: LevenshteinConfig = serde_json::from_value(config).unwrap_or_default();
            Some(levenshtein(&scorer_input, &cfg))
        }
        "step_efficiency" => Some(super::trace_eval_metrics::step_efficiency(&input_json)),
        "plan_quality" => Some(super::trace_eval_metrics::plan_quality(&input_json)),
        "plan_adherence" => Some(super::trace_eval_metrics::plan_adherence(&input_json)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_exact_match_via_execute() {
        let input = json!({
            "output": "hello",
            "expected": "hello",
        });
        let result = execute("exact_match", input.to_string().as_bytes()).unwrap();
        assert_eq!(result.score, 1.0);
        assert_eq!(result.passed, Some(true));
    }

    #[test]
    fn test_exact_match_mismatch() {
        let input = json!({
            "output": "hello",
            "expected": "world",
        });
        let result = execute("exact_match", input.to_string().as_bytes()).unwrap();
        assert_eq!(result.score, 0.0);
        assert_eq!(result.passed, Some(false));
    }

    #[test]
    fn test_contains_via_execute() {
        let input = json!({
            "output": "hello world",
            "config": {"pattern": "world"},
        });
        let result = execute("contains", input.to_string().as_bytes()).unwrap();
        assert_eq!(result.score, 1.0);
    }

    #[test]
    fn test_contains_empty_pattern_returns_config_error() {
        // An empty pattern would match everything if we let it through —
        // surface a config_error like regex_match and json_schema do.
        let input = json!({
            "output": "hello world",
            "config": {"pattern": ""},
        });
        let result = execute("contains", input.to_string().as_bytes()).unwrap();
        assert_eq!(result.passed, Some(false));
        assert_eq!(result.label.as_deref(), Some("config_error"));
    }

    #[test]
    fn test_contains_malformed_config_returns_config_error() {
        // Wrong type for `pattern` used to fall back to
        // ContainsConfig::default() (empty pattern, matches everything).
        let input = json!({
            "output": "hello world",
            "config": {"pattern": 42},
        });
        let result = execute("contains", input.to_string().as_bytes()).unwrap();
        assert_eq!(result.passed, Some(false));
        assert_eq!(result.label.as_deref(), Some("config_error"));
    }

    #[test]
    fn test_json_valid_via_execute() {
        let input = json!({
            "output": r#"{"key": "value"}"#,
        });
        let result = execute("json_valid", input.to_string().as_bytes()).unwrap();
        assert_eq!(result.score, 1.0);
    }

    #[test]
    fn test_unknown_scorer_returns_none() {
        let input = json!({"output": "test"});
        assert!(execute("unknown_scorer", input.to_string().as_bytes()).is_none());
    }

    #[test]
    fn test_known_scorer_bad_input_returns_input_error() {
        let result = execute("exact_match", b"{not json").unwrap();
        assert_eq!(result.score, 0.0);
        assert_eq!(result.passed, Some(false));
        assert_eq!(result.label.as_deref(), Some("input_error"));
        assert!(result
            .explanation
            .as_deref()
            .unwrap_or_default()
            .contains("Invalid scorer input JSON"));
    }

    #[test]
    fn test_llm_judge_returns_none() {
        // llm_judge is built-in but not in the fast path
        let input = json!({"output": "test"});
        assert!(execute("llm_judge", input.to_string().as_bytes()).is_none());
    }

    #[test]
    fn test_agent_judge_returns_none() {
        // agent_judge is built-in but worker-executed, not in the Rust fast path
        let input = json!({"output": "test"});
        assert!(execute("agent_judge", input.to_string().as_bytes()).is_none());
    }

    #[test]
    fn test_trace_metrics_execute_locally_from_trace_eval_context() {
        let first_plan_step = 1;
        let second_plan_step = 2;
        let trace_eval_context = json!({
            "schema_version": "agnt5.eval.trace_eval_context.v1",
            "project_id": "project-1",
            "session_id": "session-1",
            "root_run_id": "root-runtime-run-1",
            "task": {"text_safe": "Find the refund policy and summarize the answer"},
            "plan": {
                "detected": true,
                "steps": [
                    {"index": first_plan_step, "text_safe": "Search refund policy docs", "expected_action": "tool_call"},
                    {"index": second_plan_step, "text_safe": "Summarize refund policy answer", "expected_action": "final_answer"}
                ]
            },
            "execution_steps": [
                {"index": 1, "kind": "llm_call", "role": "planning"},
                {"index": 2, "kind": "tool_call", "tool_name": "search_docs", "matches_plan_step": first_plan_step},
                {"index": 3, "kind": "tool_call", "tool_name": "search_docs"},
                {"index": 4, "kind": "llm_call", "role": "final_response", "matches_plan_step": second_plan_step}
            ],
            "features": {
                "execution_step_count": 4,
                "tool_call_count": 2,
                "unique_tool_call_count": 1,
                "llm_call_count": 2,
                "plan_steps_total": 2,
                "plan_steps_matched": 2,
                "off_path_steps": [3],
                "duplicate_tool_calls": [
                    {"tool_name": "search_docs", "step_ids": [2, 3], "reason": "same tool and same safe arguments/result signature"}
                ]
            },
            "evidence_refs": {"normalized_session_ref": "mem://session"}
        });
        let input = json!({
            "output": "test",
            "trace_eval_context": trace_eval_context,
        });

        for scorer_name in ["step_efficiency", "plan_quality", "plan_adherence"] {
            assert!(is_builtin_scorer(scorer_name));
            assert!(can_execute_locally(scorer_name));
            assert!(execute(scorer_name, input.to_string().as_bytes()).is_some());
        }

        let step_result = execute("step_efficiency", input.to_string().as_bytes()).unwrap();
        assert_eq!(step_result.passed, Some(false));
        assert_eq!(step_result.label.as_deref(), Some("needs_review"));
        let step_metadata = step_result.metadata.as_ref().unwrap();
        assert_eq!(step_metadata["actual_steps"], 3);
        assert_eq!(step_metadata["minimum_steps"], 2);
        assert_eq!(step_metadata["duplicate_tool_call_count"], 1);
        assert_eq!(step_metadata["off_path_step_count"], 1);

        let quality_result = execute("plan_quality", input.to_string().as_bytes()).unwrap();
        assert_eq!(quality_result.passed, Some(true));
        assert!(quality_result.score >= 0.8);
        assert!(quality_result
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("completeness_score"))
            .is_some());

        let adherence_result = execute("plan_adherence", input.to_string().as_bytes()).unwrap();
        assert_eq!(adherence_result.passed, Some(false));
        assert!(adherence_result.score < 0.8);
        assert_eq!(adherence_result.label.as_deref(), Some("partial_adherence"));
    }

    #[test]
    fn test_trace_metric_missing_context_returns_artifact_error() {
        let input = json!({"output": "test"});
        let result = execute("plan_quality", input.to_string().as_bytes()).unwrap();
        assert_eq!(result.score, 0.0);
        assert_eq!(result.passed, Some(false));
        assert_eq!(result.label.as_deref(), Some("artifact_error"));
    }

    #[test]
    fn test_trace_metric_invalid_config_returns_config_error() {
        let input = json!({
            "output": "test",
            "config": {"score_threshold": 1.5},
            "trace_eval_context": {
                "schema_version": "agnt5.eval.trace_eval_context.v1",
                "project_id": "project-1",
                "session_id": "session-1",
                "root_run_id": "root-runtime-run-1",
                "plan": {"detected": false},
                "features": {}
            }
        });
        let result = execute("plan_quality", input.to_string().as_bytes()).unwrap();
        assert_eq!(result.score, 0.0);
        assert_eq!(result.passed, Some(false));
        assert_eq!(result.label.as_deref(), Some("config_error"));
    }

    #[test]
    fn test_levenshtein_via_execute() {
        let input = json!({
            "output": "hello",
            "expected": "hallo",
        });
        let result = execute("levenshtein", input.to_string().as_bytes()).unwrap();
        assert!(result.score > 0.7);
    }

    #[test]
    fn test_json_schema_via_execute_valid() {
        let input = json!({
            "output": {"age": 42, "name": "Ada"},
            "config": {
                "schema": {
                    "type": "object",
                    "required": ["age", "name"],
                    "properties": {
                        "age": {"type": "integer", "minimum": 0},
                        "name": {"type": "string"}
                    }
                }
            }
        });
        let result = execute("json_schema", input.to_string().as_bytes()).unwrap();
        assert_eq!(result.score, 1.0);
        assert_eq!(result.passed, Some(true));
        assert_eq!(result.label.as_deref(), Some("valid"));
    }

    #[test]
    fn test_json_schema_via_execute_invalid() {
        let input = json!({
            "output": {"age": -1},
            "config": {
                "schema": {
                    "type": "object",
                    "required": ["age", "name"],
                    "properties": {
                        "age": {"type": "integer", "minimum": 0},
                        "name": {"type": "string"}
                    }
                }
            }
        });
        let result = execute("json_schema", input.to_string().as_bytes()).unwrap();
        assert_eq!(result.score, 0.0);
        assert_eq!(result.passed, Some(false));
        assert_eq!(result.label.as_deref(), Some("invalid"));
        // metadata.errors should be a non-empty array.
        let errs = result
            .metadata
            .as_ref()
            .and_then(|m| m.get("errors"))
            .and_then(|e| e.as_array())
            .expect("errors array present");
        assert!(!errs.is_empty());
    }

    #[test]
    fn test_json_schema_via_execute_string_output() {
        // Output is a JSON string — should be parsed before validation.
        let input = json!({
            "output": "{\"age\": 30}",
            "config": {
                "schema": {"type": "object", "properties": {"age": {"type": "integer"}}}
            }
        });
        let result = execute("json_schema", input.to_string().as_bytes()).unwrap();
        assert_eq!(result.score, 1.0);
    }

    #[test]
    fn test_numeric_range_via_execute() {
        // In range, inclusive default.
        let input = json!({
            "output": 5,
            "config": {"min": 1, "max": 10}
        });
        let r = execute("numeric_range", input.to_string().as_bytes()).unwrap();
        assert_eq!(r.score, 1.0);
        assert_eq!(r.label.as_deref(), Some("in_range"));

        // On the boundary, inclusive.
        let input = json!({"output": 10, "config": {"min": 1, "max": 10}});
        let r = execute("numeric_range", input.to_string().as_bytes()).unwrap();
        assert_eq!(r.score, 1.0);

        // On the boundary, exclusive.
        let input = json!({
            "output": 10,
            "config": {"min": 1, "max": 10, "inclusive": false}
        });
        let r = execute("numeric_range", input.to_string().as_bytes()).unwrap();
        assert_eq!(r.score, 0.0);

        // Out of range.
        let input = json!({"output": 11, "config": {"min": 1, "max": 10}});
        let r = execute("numeric_range", input.to_string().as_bytes()).unwrap();
        assert_eq!(r.score, 0.0);
        assert_eq!(r.label.as_deref(), Some("out_of_range"));

        // String numeric output.
        let input = json!({"output": "3.14", "config": {"min": 0, "max": 5}});
        let r = execute("numeric_range", input.to_string().as_bytes()).unwrap();
        assert_eq!(r.score, 1.0);

        // Non-numeric output → parse_error.
        let input = json!({"output": "not a number", "config": {"min": 0, "max": 5}});
        let r = execute("numeric_range", input.to_string().as_bytes()).unwrap();
        assert_eq!(r.score, 0.0);
        assert_eq!(r.label.as_deref(), Some("parse_error"));

        // Missing both bounds → config_error.
        let input = json!({"output": 1, "config": {}});
        let r = execute("numeric_range", input.to_string().as_bytes()).unwrap();
        assert_eq!(r.label.as_deref(), Some("config_error"));
    }

    #[test]
    fn test_regex_match_via_execute() {
        let input = json!({
            "output": "hello123",
            "config": {"pattern": "\\d+"},
        });
        let result = execute("regex_match", input.to_string().as_bytes()).unwrap();
        assert_eq!(result.score, 1.0);
    }
}
