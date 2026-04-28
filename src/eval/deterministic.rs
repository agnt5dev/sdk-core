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
    fn test_levenshtein_distance() {
        assert_eq!(levenshtein_distance("", ""), 0);
        assert_eq!(levenshtein_distance("hello", "hello"), 0);
        assert_eq!(levenshtein_distance("hello", ""), 5);
        assert_eq!(levenshtein_distance("", "hello"), 5);
        assert_eq!(levenshtein_distance("hello", "hallo"), 1);
        assert_eq!(levenshtein_distance("kitten", "sitting"), 3);
    }
}
