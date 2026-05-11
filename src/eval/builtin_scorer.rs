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
];

/// Check if a scorer name is a known built-in scorer.
pub fn is_builtin_scorer(name: &str) -> bool {
    BUILTIN_SCORER_NAMES.contains(&name)
}

/// Check if a scorer name can be executed directly in Rust (without FFI).
///
/// `llm_judge` is the only built-in NOT in the sync fast path — it's async
/// (calls the LM client) and must be invoked via [`super::llm_judge::llm_judge`]
/// rather than through this entry point.
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
pub fn execute(scorer_name: &str, input_data: &[u8]) -> Option<ScorerResult> {
    if !can_execute_locally(scorer_name) {
        return None;
    }

    // Parse the input JSON
    let input_json: Value = serde_json::from_slice(input_data).ok()?;

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
            let cfg: ContainsConfig = serde_json::from_value(config).unwrap_or_default();
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
                        explanation: Some(format!(
                            "json_schema requires `schema` in config: {e}"
                        )),
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
    fn test_llm_judge_returns_none() {
        // llm_judge is built-in but not in the fast path
        let input = json!({"output": "test"});
        assert!(execute("llm_judge", input.to_string().as_bytes()).is_none());
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
