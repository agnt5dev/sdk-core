//! LLM-as-judge scorer for semantic evaluation.
//!
//! Uses a language model to evaluate outputs based on custom criteria.

use super::{ScorerInput, ScorerResult};
use crate::lm::{GenerateRequest, LanguageModel, Message, MessageRole};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Configuration for LLM-as-judge scorer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmJudgeConfig {
    /// Evaluation criteria/rubric
    pub criteria: String,
    /// Custom system prompt (optional, uses default if not provided)
    pub system_prompt: Option<String>,
    /// Temperature for LLM (default: 0.0 for deterministic)
    pub temperature: Option<f32>,
    /// Whether to include the original input in the prompt
    pub include_input: Option<bool>,
}

impl LlmJudgeConfig {
    /// Create a new LLM judge configuration with the given criteria
    pub fn new(criteria: impl Into<String>) -> Self {
        Self {
            criteria: criteria.into(),
            system_prompt: None,
            temperature: None,
            include_input: None,
        }
    }

    /// Set a custom system prompt
    pub fn with_system_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.system_prompt = Some(prompt.into());
        self
    }

    /// Set the temperature (0.0 for deterministic)
    pub fn with_temperature(mut self, temp: f32) -> Self {
        self.temperature = Some(temp);
        self
    }

    /// Include the original input in the evaluation prompt
    pub fn include_input(mut self) -> Self {
        self.include_input = Some(true);
        self
    }
}

const DEFAULT_SYSTEM_PROMPT: &str = r#"You are an expert evaluator. Your task is to evaluate the given output based on the provided criteria.

Respond with a JSON object containing:
- "score": a number between 0.0 and 1.0
- "passed": boolean (true if score >= 0.7)
- "explanation": brief explanation of your evaluation

Respond ONLY with the JSON object, no other text."#;

/// Evaluate output using an LLM as judge.
///
/// # Arguments
/// * `lm` - Language model to use for evaluation
/// * `model` - Model identifier (e.g., "openai/gpt-4o-mini")
/// * `input` - ScorerInput with output to evaluate
/// * `config` - LlmJudgeConfig with criteria and settings
///
/// # Returns
/// ScorerResult with the LLM's evaluation
pub async fn llm_judge<M: LanguageModel>(
    lm: &M,
    model: &str,
    input: &ScorerInput,
    config: &LlmJudgeConfig,
) -> ScorerResult {
    let system_prompt = config
        .system_prompt
        .clone()
        .unwrap_or_else(|| DEFAULT_SYSTEM_PROMPT.to_string());

    let mut user_content = format!("## Evaluation Criteria\n{}\n\n", config.criteria);

    if config.include_input.unwrap_or(false) {
        if let Some(input_val) = &input.input {
            user_content.push_str(&format!(
                "## Input\n{}\n\n",
                serde_json::to_string_pretty(input_val).unwrap_or_default()
            ));
        }
    }

    user_content.push_str(&format!(
        "## Output to Evaluate\n{}\n\n",
        serde_json::to_string_pretty(&input.output).unwrap_or_default()
    ));

    if let Some(expected) = &input.expected {
        user_content.push_str(&format!(
            "## Expected Output (Reference)\n{}\n\n",
            serde_json::to_string_pretty(expected).unwrap_or_default()
        ));
    }

    user_content.push_str("Please evaluate the output and respond with a JSON object.");

    let mut request = GenerateRequest::new(model)
        .message(Message::new(MessageRole::System, system_prompt))
        .message(Message::new(MessageRole::User, user_content));

    if let Some(temp) = config.temperature {
        request = request.configure(|c| c.temperature = Some(temp));
    } else {
        // Default to 0.0 for deterministic evaluation
        request = request.configure(|c| c.temperature = Some(0.0));
    }

    match lm.generate(request).await {
        Ok(response) => parse_llm_judge_response(&response.text),
        Err(e) => ScorerResult {
            score: 0.0,
            passed: Some(false),
            label: Some("error".into()),
            explanation: Some(format!("LLM call failed: {}", e)),
            metadata: None,
        },
    }
}

/// Parse the LLM's JSON response into a ScorerResult
fn parse_llm_judge_response(content: &str) -> ScorerResult {
    let json_str = extract_json(content);

    match serde_json::from_str::<Value>(&json_str) {
        Ok(v) => {
            let score = v
                .get("score")
                .and_then(|s| s.as_f64())
                .unwrap_or(0.0)
                .clamp(0.0, 1.0);

            let passed = v
                .get("passed")
                .and_then(|p| p.as_bool())
                .unwrap_or(score >= 0.7);

            let explanation = v
                .get("explanation")
                .and_then(|e| e.as_str())
                .map(String::from);

            let label = v.get("label").and_then(|l| l.as_str()).map(String::from);

            ScorerResult {
                score,
                passed: Some(passed),
                label,
                explanation,
                metadata: Some(v),
            }
        }
        Err(_) => ScorerResult {
            score: 0.0,
            passed: Some(false),
            label: Some("parse_error".into()),
            explanation: Some(format!("Could not parse LLM response as JSON: {}", content)),
            metadata: None,
        },
    }
}

/// Extract JSON object from response text (handles markdown code blocks)
fn extract_json(s: &str) -> String {
    let s = s.trim();

    // Handle markdown code blocks
    if s.starts_with("```json") {
        if let Some(end) = s.rfind("```") {
            let start = s.find('\n').map(|i| i + 1).unwrap_or(7);
            if start < end {
                return s[start..end].trim().to_string();
            }
        }
    } else if s.starts_with("```") {
        if let Some(end) = s.rfind("```") {
            let start = s.find('\n').map(|i| i + 1).unwrap_or(3);
            if start < end {
                return s[start..end].trim().to_string();
            }
        }
    }

    // Try to find JSON object in response
    if let Some(start) = s.find('{') {
        if let Some(end) = s.rfind('}') {
            if end >= start {
                return s[start..=end].to_string();
            }
        }
    }

    s.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_json() {
        // Plain JSON
        assert_eq!(extract_json(r#"{"score": 0.8}"#), r#"{"score": 0.8}"#);

        // JSON with surrounding text
        assert_eq!(
            extract_json(r#"Here is my evaluation: {"score": 0.8} done"#),
            r#"{"score": 0.8}"#
        );

        // Markdown code block
        assert_eq!(
            extract_json("```json\n{\"score\": 0.8}\n```"),
            r#"{"score": 0.8}"#
        );

        // Generic code block
        assert_eq!(
            extract_json("```\n{\"score\": 0.8}\n```"),
            r#"{"score": 0.8}"#
        );
    }

    #[test]
    fn test_parse_llm_judge_response() {
        // Valid response
        let result = parse_llm_judge_response(
            r#"{"score": 0.85, "passed": true, "explanation": "Good output"}"#,
        );
        assert_eq!(result.score, 0.85);
        assert_eq!(result.passed, Some(true));
        assert_eq!(result.explanation, Some("Good output".into()));

        // Response without passed field (should infer from score)
        let result = parse_llm_judge_response(r#"{"score": 0.5, "explanation": "Mediocre"}"#);
        assert_eq!(result.score, 0.5);
        assert_eq!(result.passed, Some(false)); // 0.5 < 0.7

        // Invalid JSON
        let result = parse_llm_judge_response("not valid json");
        assert_eq!(result.score, 0.0);
        assert_eq!(result.label, Some("parse_error".into()));
    }

    #[test]
    fn test_llm_judge_config_builder() {
        let config = LlmJudgeConfig::new("Is the output helpful?")
            .with_temperature(0.2)
            .with_system_prompt("You are a strict evaluator.")
            .include_input();

        assert_eq!(config.criteria, "Is the output helpful?");
        assert_eq!(config.temperature, Some(0.2));
        assert_eq!(
            config.system_prompt,
            Some("You are a strict evaluator.".into())
        );
        assert_eq!(config.include_input, Some(true));
    }
}
