//! Deterministic scorers for evaluating outputs.
//!
//! These scorers provide fast, deterministic evaluation of outputs
//! using string comparison, pattern matching, and similarity metrics.

use super::{ScorerInput, ScorerResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Configuration for exact_match scorer
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExactMatchConfig {
    /// Whether to compare case-sensitively (default: true)
    pub case_sensitive: Option<bool>,
}

/// Check if output exactly matches expected value.
///
/// # Arguments
/// * `input` - ScorerInput with output and expected values
/// * `config` - Configuration for case sensitivity
///
/// # Returns
/// ScorerResult with score 1.0 for match, 0.0 for mismatch
pub fn exact_match(input: &ScorerInput, config: &ExactMatchConfig) -> ScorerResult {
    let case_sensitive = config.case_sensitive.unwrap_or(true);

    let output_str = value_to_string(&input.output);
    let expected_str = input
        .expected
        .as_ref()
        .map(value_to_string)
        .unwrap_or_default();

    let matches = if case_sensitive {
        output_str == expected_str
    } else {
        output_str.to_lowercase() == expected_str.to_lowercase()
    };

    ScorerResult {
        score: if matches { 1.0 } else { 0.0 },
        passed: Some(matches),
        label: Some(if matches {
            "match".into()
        } else {
            "mismatch".into()
        }),
        explanation: None,
        metadata: None,
    }
}

/// Configuration for contains scorer
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ContainsConfig {
    /// Pattern to search for
    pub pattern: String,
    /// Whether to search case-sensitively (default: true)
    pub case_sensitive: Option<bool>,
}

/// Check if output contains a specific pattern.
///
/// # Arguments
/// * `input` - ScorerInput with output value
/// * `config` - Configuration with pattern and case sensitivity
///
/// # Returns
/// ScorerResult with score 1.0 if found, 0.0 if not found
pub fn contains(input: &ScorerInput, config: &ContainsConfig) -> ScorerResult {
    let case_sensitive = config.case_sensitive.unwrap_or(true);
    let output_str = value_to_string(&input.output);

    let found = if case_sensitive {
        output_str.contains(&config.pattern)
    } else {
        output_str
            .to_lowercase()
            .contains(&config.pattern.to_lowercase())
    };

    ScorerResult {
        score: if found { 1.0 } else { 0.0 },
        passed: Some(found),
        label: Some(if found {
            "found".into()
        } else {
            "not_found".into()
        }),
        explanation: None,
        metadata: None,
    }
}

/// Check if output is valid JSON.
///
/// If the output is already a JSON Value (not a string), it's considered valid.
/// If it's a string, attempts to parse it as JSON.
///
/// # Arguments
/// * `input` - ScorerInput with output value
///
/// # Returns
/// ScorerResult with score 1.0 if valid, 0.0 if invalid
pub fn json_valid(input: &ScorerInput) -> ScorerResult {
    let valid = match &input.output {
        Value::String(s) => serde_json::from_str::<Value>(s).is_ok(),
        _ => true, // Already a Value, so it's valid JSON
    };

    ScorerResult {
        score: if valid { 1.0 } else { 0.0 },
        passed: Some(valid),
        label: Some(if valid {
            "valid".into()
        } else {
            "invalid".into()
        }),
        explanation: None,
        metadata: None,
    }
}

/// Configuration for regex_match scorer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegexConfig {
    /// Regex pattern to match
    pub pattern: String,
}

/// Check if output matches a regex pattern.
///
/// # Arguments
/// * `input` - ScorerInput with output value
/// * `config` - Configuration with regex pattern
///
/// # Returns
/// ScorerResult with score 1.0 if matches, 0.0 if not
pub fn regex_match(input: &ScorerInput, config: &RegexConfig) -> ScorerResult {
    let output_str = value_to_string(&input.output);

    match regex::Regex::new(&config.pattern) {
        Ok(re) => {
            let matches = re.is_match(&output_str);
            ScorerResult {
                score: if matches { 1.0 } else { 0.0 },
                passed: Some(matches),
                label: Some(if matches {
                    "match".into()
                } else {
                    "no_match".into()
                }),
                explanation: None,
                metadata: None,
            }
        }
        Err(e) => ScorerResult {
            score: 0.0,
            passed: Some(false),
            label: Some("error".into()),
            explanation: Some(format!("Invalid regex: {}", e)),
            metadata: None,
        },
    }
}

/// Configuration for levenshtein scorer
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LevenshteinConfig {
    /// Minimum similarity threshold (0.0-1.0, default: 0.0)
    pub threshold: Option<f64>,
}

/// Calculate Levenshtein similarity between output and expected.
///
/// Returns a similarity score between 0.0 and 1.0, where 1.0 means identical.
///
/// # Arguments
/// * `input` - ScorerInput with output and expected values
/// * `config` - Configuration with optional threshold
///
/// # Returns
/// ScorerResult with similarity score
pub fn levenshtein(input: &ScorerInput, config: &LevenshteinConfig) -> ScorerResult {
    let output_str = value_to_string(&input.output);
    let expected_str = input
        .expected
        .as_ref()
        .map(value_to_string)
        .unwrap_or_default();

    let distance = levenshtein_distance(&output_str, &expected_str);
    let max_len = output_str.len().max(expected_str.len());
    let similarity = if max_len == 0 {
        1.0
    } else {
        1.0 - (distance as f64 / max_len as f64)
    };

    let threshold = config.threshold.unwrap_or(0.0);
    let passed = similarity >= threshold;

    ScorerResult {
        score: similarity,
        passed: Some(passed),
        label: None,
        explanation: Some(format!(
            "Similarity: {:.2}%, Distance: {}",
            similarity * 100.0,
            distance
        )),
        metadata: None,
    }
}

/// Configuration for json_schema scorer.
///
/// Validates the output against a JSON Schema (Draft 2020-12 by default).
/// The output is parsed as JSON if it's a string; otherwise the JSON `Value`
/// is validated directly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonSchemaConfig {
    /// The JSON Schema to validate against. Required.
    pub schema: Value,
}

/// Check whether the output validates against a JSON Schema.
///
/// String outputs are parsed as JSON first; anything that isn't valid JSON
/// fails with `parse_error`. Validation failures include a one-line summary
/// of the first error in `explanation` and the full error list in
/// `metadata.errors`.
pub fn json_schema(input: &ScorerInput, config: &JsonSchemaConfig) -> ScorerResult {
    // Parse the output: strings get JSON-decoded, everything else is the
    // existing `Value` (the caller already gave us structured JSON).
    let parsed: Value = match &input.output {
        Value::String(s) => match serde_json::from_str(s) {
            Ok(v) => v,
            Err(e) => {
                return ScorerResult {
                    score: 0.0,
                    passed: Some(false),
                    label: Some("parse_error".into()),
                    explanation: Some(format!("output is not valid JSON: {e}")),
                    metadata: None,
                };
            }
        },
        v => v.clone(),
    };

    // Compile the schema. A bad schema is a config error, not a sample
    // failure — report it distinctly so users notice.
    let validator = match jsonschema::validator_for(&config.schema) {
        Ok(v) => v,
        Err(e) => {
            return ScorerResult {
                score: 0.0,
                passed: Some(false),
                label: Some("config_error".into()),
                explanation: Some(format!("invalid schema: {e}")),
                metadata: None,
            };
        }
    };

    let errors: Vec<String> = validator
        .iter_errors(&parsed)
        .map(|e| format!("{}: {}", e.instance_path, e))
        .collect();

    if errors.is_empty() {
        ScorerResult {
            score: 1.0,
            passed: Some(true),
            label: Some("valid".into()),
            explanation: None,
            metadata: None,
        }
    } else {
        ScorerResult {
            score: 0.0,
            passed: Some(false),
            label: Some("invalid".into()),
            explanation: Some(errors[0].clone()),
            metadata: Some(serde_json::json!({ "errors": errors })),
        }
    }
}

/// Configuration for numeric_range scorer.
///
/// At least one of `min` / `max` must be set. `inclusive` controls
/// whether the bounds themselves are accepted (default: true).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NumericRangeConfig {
    pub min: Option<f64>,
    pub max: Option<f64>,
    /// When true (default), `min ≤ x ≤ max`. When false, `min < x < max`.
    pub inclusive: Option<bool>,
}

/// Check whether the output's numeric value falls within `[min, max]`.
///
/// Accepts numbers directly or numeric strings (e.g. `"42"` or `"3.14"`).
/// Non-numeric output fails with `parse_error`. Returns 1.0 inside the
/// range, 0.0 outside.
pub fn numeric_range(input: &ScorerInput, config: &NumericRangeConfig) -> ScorerResult {
    if config.min.is_none() && config.max.is_none() {
        return ScorerResult {
            score: 0.0,
            passed: Some(false),
            label: Some("config_error".into()),
            explanation: Some("numeric_range requires at least one of `min` or `max`".into()),
            metadata: None,
        };
    }

    let value = match &input.output {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    };

    let Some(value) = value else {
        return ScorerResult {
            score: 0.0,
            passed: Some(false),
            label: Some("parse_error".into()),
            explanation: Some(format!(
                "output is not numeric: {}",
                serde_json::to_string(&input.output).unwrap_or_default()
            )),
            metadata: None,
        };
    };

    let inclusive = config.inclusive.unwrap_or(true);
    let above_min = match config.min {
        Some(min) if inclusive => value >= min,
        Some(min) => value > min,
        None => true,
    };
    let below_max = match config.max {
        Some(max) if inclusive => value <= max,
        Some(max) => value < max,
        None => true,
    };
    let in_range = above_min && below_max;

    ScorerResult {
        score: if in_range { 1.0 } else { 0.0 },
        passed: Some(in_range),
        label: Some(if in_range { "in_range" } else { "out_of_range" }.into()),
        explanation: Some(format!(
            "value={value}, min={:?}, max={:?}, inclusive={inclusive}",
            config.min, config.max
        )),
        metadata: None,
    }
}

/// Convert a JSON Value to a string for comparison
fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        _ => v.to_string(),
    }
}

/// Calculate Levenshtein edit distance between two strings
fn levenshtein_distance(a: &str, b: &str) -> usize {
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let m = a_chars.len();
    let n = b_chars.len();

    if m == 0 {
        return n;
    }
    if n == 0 {
        return m;
    }

    // Use two rows instead of full matrix for space efficiency
    let mut prev_row: Vec<usize> = (0..=n).collect();
    let mut curr_row: Vec<usize> = vec![0; n + 1];

    for i in 1..=m {
        curr_row[0] = i;
        for j in 1..=n {
            let cost = if a_chars[i - 1] == b_chars[j - 1] {
                0
            } else {
                1
            };
            curr_row[j] = (prev_row[j] + 1)
                .min(curr_row[j - 1] + 1)
                .min(prev_row[j - 1] + cost);
        }
        std::mem::swap(&mut prev_row, &mut curr_row);
    }

    prev_row[n]
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_exact_match() {
        // Exact match
        let input = ScorerInput::new(json!("hello")).with_expected(json!("hello"));
        let result = exact_match(&input, &ExactMatchConfig::default());
        assert_eq!(result.score, 1.0);
        assert_eq!(result.passed, Some(true));

        // Mismatch
        let input = ScorerInput::new(json!("hello")).with_expected(json!("world"));
        let result = exact_match(&input, &ExactMatchConfig::default());
        assert_eq!(result.score, 0.0);
        assert_eq!(result.passed, Some(false));

        // Case insensitive
        let input = ScorerInput::new(json!("HELLO")).with_expected(json!("hello"));
        let config = ExactMatchConfig {
            case_sensitive: Some(false),
        };
        let result = exact_match(&input, &config);
        assert_eq!(result.score, 1.0);
    }

    #[test]
    fn test_contains() {
        // Found
        let input = ScorerInput::new(json!("hello world"));
        let config = ContainsConfig {
            pattern: "world".into(),
            case_sensitive: Some(true),
        };
        let result = contains(&input, &config);
        assert_eq!(result.score, 1.0);

        // Not found
        let config = ContainsConfig {
            pattern: "foo".into(),
            case_sensitive: Some(true),
        };
        let result = contains(&input, &config);
        assert_eq!(result.score, 0.0);

        // Case insensitive
        let config = ContainsConfig {
            pattern: "WORLD".into(),
            case_sensitive: Some(false),
        };
        let result = contains(&input, &config);
        assert_eq!(result.score, 1.0);
    }

    #[test]
    fn test_json_valid() {
        // Valid JSON string
        let input = ScorerInput::new(json!(r#"{"key": "value"}"#));
        let result = json_valid(&input);
        assert_eq!(result.score, 1.0);

        // Invalid JSON string
        let input = ScorerInput::new(json!("{invalid}"));
        let result = json_valid(&input);
        assert_eq!(result.score, 0.0);

        // Already a JSON object
        let input = ScorerInput::new(json!({"key": "value"}));
        let result = json_valid(&input);
        assert_eq!(result.score, 1.0);
    }

    #[test]
    fn test_regex_match() {
        // Match
        let input = ScorerInput::new(json!("hello123world"));
        let config = RegexConfig {
            pattern: r"\d+".into(),
        };
        let result = regex_match(&input, &config);
        assert_eq!(result.score, 1.0);

        // No match
        let input = ScorerInput::new(json!("helloworld"));
        let result = regex_match(&input, &config);
        assert_eq!(result.score, 0.0);

        // Invalid regex
        let config = RegexConfig {
            pattern: r"[".into(),
        };
        let result = regex_match(&input, &config);
        assert_eq!(result.score, 0.0);
        assert_eq!(result.label, Some("error".into()));
    }

    #[test]
    fn test_levenshtein() {
        // Identical strings
        let input = ScorerInput::new(json!("hello")).with_expected(json!("hello"));
        let result = levenshtein(&input, &LevenshteinConfig::default());
        assert_eq!(result.score, 1.0);

        // Different strings
        let input = ScorerInput::new(json!("hello")).with_expected(json!("hallo"));
        let result = levenshtein(&input, &LevenshteinConfig::default());
        assert!(result.score > 0.7); // 4/5 = 0.8 similarity

        // With threshold
        let config = LevenshteinConfig {
            threshold: Some(0.9),
        };
        let result = levenshtein(&input, &config);
        assert_eq!(result.passed, Some(false)); // 0.8 < 0.9
    }

    #[test]
    fn test_json_schema_valid_object() {
        let schema = json!({
            "type": "object",
            "required": ["name"],
            "properties": {"name": {"type": "string"}}
        });
        let input = ScorerInput::new(json!({"name": "Ada"}));
        let result = json_schema(&input, &JsonSchemaConfig { schema });
        assert_eq!(result.score, 1.0);
        assert_eq!(result.label.as_deref(), Some("valid"));
    }

    #[test]
    fn test_json_schema_invalid_object() {
        let schema = json!({
            "type": "object",
            "required": ["name", "age"],
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer", "minimum": 0}
            }
        });
        let input = ScorerInput::new(json!({"name": "Ada", "age": -5}));
        let result = json_schema(&input, &JsonSchemaConfig { schema });
        assert_eq!(result.score, 0.0);
        assert!(result.explanation.is_some());
    }

    #[test]
    fn test_json_schema_string_parses_json() {
        let schema = json!({"type": "array", "items": {"type": "integer"}});
        let input = ScorerInput::new(json!("[1, 2, 3]"));
        let result = json_schema(&input, &JsonSchemaConfig { schema });
        assert_eq!(result.score, 1.0);
    }

    #[test]
    fn test_json_schema_invalid_schema_is_config_error() {
        // Schemas must be objects/booleans, not strings.
        let input = ScorerInput::new(json!({"x": 1}));
        let result = json_schema(
            &input,
            &JsonSchemaConfig {
                schema: json!("not a schema"),
            },
        );
        assert_eq!(result.label.as_deref(), Some("config_error"));
    }

    #[test]
    fn test_numeric_range_basics() {
        let cfg = NumericRangeConfig {
            min: Some(0.0),
            max: Some(10.0),
            inclusive: None,
        };
        assert_eq!(numeric_range(&ScorerInput::new(json!(5)), &cfg).score, 1.0);
        assert_eq!(numeric_range(&ScorerInput::new(json!(0)), &cfg).score, 1.0);
        assert_eq!(numeric_range(&ScorerInput::new(json!(10)), &cfg).score, 1.0);
        assert_eq!(numeric_range(&ScorerInput::new(json!(-1)), &cfg).score, 0.0);
        assert_eq!(numeric_range(&ScorerInput::new(json!(11)), &cfg).score, 0.0);
    }

    #[test]
    fn test_numeric_range_exclusive() {
        let cfg = NumericRangeConfig {
            min: Some(0.0),
            max: Some(10.0),
            inclusive: Some(false),
        };
        assert_eq!(numeric_range(&ScorerInput::new(json!(0)), &cfg).score, 0.0);
        assert_eq!(numeric_range(&ScorerInput::new(json!(10)), &cfg).score, 0.0);
        assert_eq!(numeric_range(&ScorerInput::new(json!(5)), &cfg).score, 1.0);
    }

    #[test]
    fn test_numeric_range_one_sided() {
        // Lower bound only.
        let cfg = NumericRangeConfig {
            min: Some(100.0),
            max: None,
            inclusive: None,
        };
        assert_eq!(numeric_range(&ScorerInput::new(json!(99)), &cfg).score, 0.0);
        assert_eq!(
            numeric_range(&ScorerInput::new(json!(100)), &cfg).score,
            1.0
        );
        assert_eq!(
            numeric_range(&ScorerInput::new(json!(1_000_000)), &cfg).score,
            1.0
        );

        // Upper bound only.
        let cfg = NumericRangeConfig {
            min: None,
            max: Some(0.5),
            inclusive: None,
        };
        assert_eq!(
            numeric_range(&ScorerInput::new(json!(0.4)), &cfg).score,
            1.0
        );
        assert_eq!(
            numeric_range(&ScorerInput::new(json!(0.5)), &cfg).score,
            1.0
        );
        assert_eq!(
            numeric_range(&ScorerInput::new(json!(0.6)), &cfg).score,
            0.0
        );
    }

    #[test]
    fn test_numeric_range_requires_a_bound() {
        let cfg = NumericRangeConfig::default();
        let r = numeric_range(&ScorerInput::new(json!(1)), &cfg);
        assert_eq!(r.label.as_deref(), Some("config_error"));
    }

    #[test]
    fn test_levenshtein_distance() {
        assert_eq!(levenshtein_distance("", ""), 0);
        assert_eq!(levenshtein_distance("hello", "hello"), 0);
        assert_eq!(levenshtein_distance("hello", ""), 5);
        assert_eq!(levenshtein_distance("", "hello"), 5);
        assert_eq!(levenshtein_distance("hello", "hallo"), 1);
        assert_eq!(levenshtein_distance("kitten", "sitting"), 3);
    }
}
