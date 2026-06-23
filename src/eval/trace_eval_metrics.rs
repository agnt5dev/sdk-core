//! Deterministic trace-eval metrics over `trace_eval_context`.

use super::normalized::TRACE_EVAL_CONTEXT_SCHEMA;
use super::ScorerResult;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TraceEvalContext {
    #[serde(default)]
    pub schema_version: String,
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub project_id: String,
    #[serde(default)]
    pub root_run_id: String,
    #[serde(default)]
    pub task: Option<TraceEvalTask>,
    #[serde(default)]
    pub plan: TraceEvalPlan,
    #[serde(default)]
    pub execution_steps: Vec<TraceEvalExecutionStep>,
    #[serde(default)]
    pub features: TraceEvalFeatures,
    #[serde(default)]
    pub evidence_refs: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TraceEvalTask {
    #[serde(default)]
    pub text_safe: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TraceEvalPlan {
    #[serde(default)]
    pub detected: bool,
    #[serde(default)]
    pub steps: Vec<TraceEvalPlanStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TraceEvalPlanStep {
    #[serde(default)]
    pub index: i64,
    #[serde(default)]
    pub text_safe: String,
    #[serde(default)]
    pub expected_action: String,
    #[serde(default)]
    pub expected_tool: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TraceEvalExecutionStep {
    #[serde(default)]
    pub index: i64,
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub role: String,
    #[serde(default)]
    pub matches_plan_step: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TraceEvalFeatures {
    #[serde(default)]
    pub unique_tool_call_count: i64,
    #[serde(default)]
    pub llm_call_count: i64,
    #[serde(default)]
    pub error_count: i64,
    #[serde(default)]
    pub duplicate_tool_calls: Vec<TraceEvalStepGroup>,
    #[serde(default)]
    pub plan_steps_total: i64,
    #[serde(default)]
    pub plan_steps_matched: i64,
    #[serde(default)]
    pub plan_steps_missing: Vec<TraceEvalMissingPlanStep>,
    #[serde(default)]
    pub off_path_steps: Vec<i64>,
    #[serde(default)]
    pub retry_groups: Vec<TraceEvalRetryGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TraceEvalStepGroup {
    #[serde(default)]
    pub tool_name: String,
    #[serde(default)]
    pub step_ids: Vec<i64>,
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TraceEvalRetryGroup {
    #[serde(default)]
    pub step_ids: Vec<i64>,
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TraceEvalMissingPlanStep {
    #[serde(default)]
    pub plan_step_index: i64,
    #[serde(default)]
    pub text_safe: String,
}

pub fn step_efficiency(input_json: &Value) -> ScorerResult {
    let trace_ctx = match trace_eval_context_from_input(input_json) {
        Ok(trace_ctx) => trace_ctx,
        Err(result) => return result,
    };
    let config = input_json.get("config").unwrap_or(&Value::Null);
    let threshold = match score_threshold(config, "step_efficiency", 0.8) {
        Ok(threshold) => threshold,
        Err(result) => return result,
    };
    let (configured_minimum, has_configured_minimum) = match minimum_steps(config) {
        Ok(value) => value,
        Err(result) => return result,
    };

    let actual_steps = step_efficiency_actual_steps(&trace_ctx);
    let (minimum_steps, minimum_steps_source) = step_efficiency_minimum_steps(
        &trace_ctx,
        configured_minimum,
        has_configured_minimum,
        actual_steps,
    );
    let excess_steps = (actual_steps - minimum_steps).max(0);
    let duplicate_tool_call_count = duplicate_tool_call_count(&trace_ctx);
    let off_path_step_count = trace_ctx.features.off_path_steps.len() as i64;
    let retry_extra_count = retry_extra_count(&trace_ctx);
    let error_count = trace_ctx.features.error_count.max(0);
    let penalty_units = excess_steps
        + duplicate_tool_call_count
        + off_path_step_count
        + retry_extra_count
        + error_count;

    let denominator = actual_steps + minimum_steps + penalty_units;
    let score = if denominator > 0 && penalty_units > 0 {
        1.0 - (penalty_units as f64 / denominator as f64)
    } else {
        1.0
    }
    .clamp(0.0, 1.0);
    let passed = score >= threshold;
    let label = if passed && score >= 0.95 {
        "efficient"
    } else if score >= 0.5 {
        "needs_review"
    } else {
        "inefficient"
    };

    ScorerResult {
        score,
        passed: Some(passed),
        label: Some(label.to_string()),
        explanation: Some(format!(
            "step efficiency score {}: actual_steps={}, minimum_steps={}, penalties={}",
            format_score(score),
            actual_steps,
            minimum_steps,
            penalty_units,
        )),
        metadata: Some(json!({
            "builtin": "step_efficiency",
            "trace_eval_context_schema": trace_ctx.schema_version,
            "threshold": threshold,
            "actual_steps": actual_steps,
            "minimum_steps": minimum_steps,
            "minimum_steps_source": minimum_steps_source,
            "excess_steps": excess_steps,
            "penalty_units": penalty_units,
            "duplicate_tool_call_count": duplicate_tool_call_count,
            "off_path_step_count": off_path_step_count,
            "retry_extra_count": retry_extra_count,
            "error_count": error_count,
            "plan_detected": trace_ctx.plan.detected,
            "plan_steps_total": trace_ctx.features.plan_steps_total,
            "plan_steps_matched": trace_ctx.features.plan_steps_matched,
            "duplicate_tool_calls": trace_ctx.features.duplicate_tool_calls,
            "off_path_steps": trace_ctx.features.off_path_steps,
            "retry_groups": trace_ctx.features.retry_groups,
            "evidence_refs": trace_ctx.evidence_refs,
        })),
    }
}

pub fn plan_adherence(input_json: &Value) -> ScorerResult {
    let trace_ctx = match trace_eval_context_from_input(input_json) {
        Ok(trace_ctx) => trace_ctx,
        Err(result) => return result,
    };
    let config = input_json.get("config").unwrap_or(&Value::Null);
    let threshold = match score_threshold(config, "plan_adherence", 0.8) {
        Ok(threshold) => threshold,
        Err(result) => return result,
    };

    let total_plan_steps = if trace_ctx.features.plan_steps_total > 0 {
        trace_ctx.features.plan_steps_total
    } else {
        trace_ctx.plan.steps.len() as i64
    };
    if !trace_ctx.plan.detected || total_plan_steps == 0 {
        let mut metadata = plan_base_metadata("plan_adherence", &trace_ctx, threshold);
        metadata.insert("reason".to_string(), json!("no_plan_detected"));
        return ScorerResult {
            score: 0.0,
            passed: Some(false),
            label: Some("no_plan".to_string()),
            explanation: Some(
                "plan adherence cannot be evaluated because no plan was detected".to_string(),
            ),
            metadata: Some(Value::Object(metadata)),
        };
    }

    let mut matched_plan_steps = if trace_ctx.features.plan_steps_matched > 0 {
        trace_ctx.features.plan_steps_matched
    } else {
        matched_step_count(&trace_ctx)
    };
    if matched_plan_steps > total_plan_steps {
        matched_plan_steps = total_plan_steps;
    }
    let mut missing_plan_steps = trace_ctx.features.plan_steps_missing.len() as i64;
    if missing_plan_steps == 0 && matched_plan_steps < total_plan_steps {
        missing_plan_steps = total_plan_steps - matched_plan_steps;
    }
    let off_path_step_count = trace_ctx.features.off_path_steps.len() as i64;
    let retry_extra_count = retry_extra_count(&trace_ctx);
    let error_count = trace_ctx.features.error_count.max(0);
    let deviation_count =
        missing_plan_steps + off_path_step_count + retry_extra_count + error_count;
    let denominator = total_plan_steps + off_path_step_count + retry_extra_count + error_count;
    let score = if denominator > 0 {
        matched_plan_steps as f64 / denominator as f64
    } else {
        0.0
    }
    .clamp(0.0, 1.0);
    let passed = score >= threshold;
    let label = plan_metric_label(score, passed, "adhered", "partial_adherence", "not_adhered");
    let mut metadata = plan_base_metadata("plan_adherence", &trace_ctx, threshold);
    metadata.insert("matched_plan_steps".to_string(), json!(matched_plan_steps));
    metadata.insert(
        "missing_plan_step_count".to_string(),
        json!(missing_plan_steps),
    );
    metadata.insert(
        "off_path_step_count".to_string(),
        json!(off_path_step_count),
    );
    metadata.insert("retry_extra_count".to_string(), json!(retry_extra_count));
    metadata.insert("error_count".to_string(), json!(error_count));
    metadata.insert("deviation_count".to_string(), json!(deviation_count));
    metadata.insert(
        "missing_plan_steps".to_string(),
        json!(trace_ctx.features.plan_steps_missing),
    );
    metadata.insert(
        "off_path_steps".to_string(),
        json!(trace_ctx.features.off_path_steps),
    );
    metadata.insert(
        "retry_groups".to_string(),
        json!(trace_ctx.features.retry_groups),
    );

    ScorerResult {
        score,
        passed: Some(passed),
        label: Some(label.to_string()),
        explanation: Some(format!(
            "plan adherence score {}: matched_steps={}/{}, deviations={}",
            format_score(score),
            matched_plan_steps,
            total_plan_steps,
            deviation_count,
        )),
        metadata: Some(Value::Object(metadata)),
    }
}

pub fn plan_quality(input_json: &Value) -> ScorerResult {
    let trace_ctx = match trace_eval_context_from_input(input_json) {
        Ok(trace_ctx) => trace_ctx,
        Err(result) => return result,
    };
    let config = input_json.get("config").unwrap_or(&Value::Null);
    let threshold = match score_threshold(config, "plan_quality", 0.8) {
        Ok(threshold) => threshold,
        Err(result) => return result,
    };

    if !trace_ctx.plan.detected || trace_ctx.plan.steps.is_empty() {
        let mut metadata = plan_base_metadata("plan_quality", &trace_ctx, threshold);
        metadata.insert("reason".to_string(), json!("no_plan_detected"));
        return ScorerResult {
            score: 0.0,
            passed: Some(false),
            label: Some("no_plan".to_string()),
            explanation: Some(
                "plan quality cannot be evaluated because no plan was detected".to_string(),
            ),
            metadata: Some(Value::Object(metadata)),
        };
    }

    let step_count_score = plan_quality_step_count_score(trace_ctx.plan.steps.len());
    let specificity_score = plan_quality_specificity_score(&trace_ctx.plan.steps);
    let structure_score = (step_count_score + specificity_score) / 2.0;
    let completeness_score = plan_quality_completeness_score(&trace_ctx.plan.steps);
    let order_score = plan_quality_order_score(&trace_ctx.plan.steps);
    let relevance_score = plan_quality_relevance_score(&trace_ctx);
    let score = ((0.25 * structure_score)
        + (0.30 * completeness_score)
        + (0.20 * order_score)
        + (0.25 * relevance_score))
        .clamp(0.0, 1.0);
    let passed = score >= threshold;
    let label = plan_metric_label(score, passed, "good_plan", "needs_review", "poor_plan");
    let mut metadata = plan_base_metadata("plan_quality", &trace_ctx, threshold);
    metadata.insert("structure_score".to_string(), json!(structure_score));
    metadata.insert("step_count_score".to_string(), json!(step_count_score));
    metadata.insert("specificity_score".to_string(), json!(specificity_score));
    metadata.insert("completeness_score".to_string(), json!(completeness_score));
    metadata.insert("order_score".to_string(), json!(order_score));
    metadata.insert("relevance_score".to_string(), json!(relevance_score));
    metadata.insert(
        "task_present".to_string(),
        json!(trace_ctx
            .task
            .as_ref()
            .is_some_and(|task| !task.text_safe.trim().is_empty())),
    );
    metadata.insert("plan_steps".to_string(), json!(trace_ctx.plan.steps));

    ScorerResult {
        score,
        passed: Some(passed),
        label: Some(label.to_string()),
        explanation: Some(format!(
            "plan quality score {}: structure={}, completeness={}, order={}, relevance={}",
            format_score(score),
            format_score(structure_score),
            format_score(completeness_score),
            format_score(order_score),
            format_score(relevance_score),
        )),
        metadata: Some(Value::Object(metadata)),
    }
}

fn trace_eval_context_from_input(input_json: &Value) -> Result<TraceEvalContext, ScorerResult> {
    let raw = input_json
        .get("trace_eval_context")
        .or_else(|| input_json.get("trace"));
    let Some(raw) = raw else {
        return Err(error_result(
            "artifact_error",
            "trace_eval_context is required for trace metric scoring",
        ));
    };
    let trace_ctx: TraceEvalContext = match serde_json::from_value(raw.clone()) {
        Ok(trace_ctx) => trace_ctx,
        Err(err) => {
            return Err(error_result(
                "artifact_error",
                format!("invalid trace_eval_context: {err}"),
            ));
        }
    };
    if let Err(message) = validate_trace_eval_context(&trace_ctx) {
        return Err(error_result("artifact_error", message));
    }
    Ok(trace_ctx)
}

fn validate_trace_eval_context(trace_ctx: &TraceEvalContext) -> Result<(), String> {
    if trace_ctx.schema_version != TRACE_EVAL_CONTEXT_SCHEMA {
        return Err(format!(
            "invalid trace eval context schema_version {:?}",
            trace_ctx.schema_version
        ));
    }
    if trace_ctx.project_id.trim().is_empty() {
        return Err("project_id is required".to_string());
    }
    if trace_ctx.session_id.trim().is_empty() {
        return Err("session_id is required".to_string());
    }
    if trace_ctx.root_run_id.trim().is_empty() {
        return Err("root_run_id is required".to_string());
    }
    for (idx, step) in trace_ctx.execution_steps.iter().enumerate() {
        if step.index <= 0 {
            return Err(format!("execution_steps[{idx}].index must be positive"));
        }
        if step.kind.trim().is_empty() {
            return Err(format!("execution_steps[{idx}].kind is required"));
        }
    }
    Ok(())
}

fn score_threshold(config: &Value, builtin: &str, default_value: f64) -> Result<f64, ScorerResult> {
    let threshold = number_config(config, "score_threshold")
        .or_else(|| number_config(config, "threshold"))
        .unwrap_or(default_value);
    if !(0.0..=1.0).contains(&threshold) {
        return Err(error_result(
            "config_error",
            format!("{builtin} score_threshold must be between 0 and 1"),
        ));
    }
    Ok(threshold)
}

fn minimum_steps(config: &Value) -> Result<(i64, bool), ScorerResult> {
    let raw = config
        .get("minimum_steps")
        .or_else(|| config.get("min_steps"));
    let Some(raw) = raw else {
        return Ok((0, false));
    };
    let Some(value) = raw.as_i64() else {
        return Err(error_result(
            "config_error",
            "step_efficiency minimum_steps must be a non-negative integer",
        ));
    };
    if value < 0 {
        return Err(error_result(
            "config_error",
            "step_efficiency minimum_steps must be a non-negative integer",
        ));
    }
    Ok((value, true))
}

fn number_config(config: &Value, key: &str) -> Option<f64> {
    config.get(key).and_then(Value::as_f64)
}

fn plan_base_metadata(
    builtin: &str,
    trace_ctx: &TraceEvalContext,
    threshold: f64,
) -> Map<String, Value> {
    let mut metadata = Map::new();
    metadata.insert("builtin".to_string(), json!(builtin));
    metadata.insert(
        "trace_eval_context_schema".to_string(),
        json!(trace_ctx.schema_version),
    );
    metadata.insert("threshold".to_string(), json!(threshold));
    metadata.insert("plan_detected".to_string(), json!(trace_ctx.plan.detected));
    metadata.insert(
        "plan_steps_total".to_string(),
        json!(trace_ctx.features.plan_steps_total),
    );
    metadata.insert(
        "plan_steps_matched".to_string(),
        json!(trace_ctx.features.plan_steps_matched),
    );
    metadata.insert("evidence_refs".to_string(), trace_ctx.evidence_refs.clone());
    metadata
}

fn matched_step_count(trace_ctx: &TraceEvalContext) -> i64 {
    let mut matched = BTreeSet::new();
    for step in &trace_ctx.execution_steps {
        if let Some(index) = step.matches_plan_step {
            matched.insert(index);
        }
    }
    matched.len() as i64
}

fn plan_quality_step_count_score(step_count: usize) -> f64 {
    match step_count {
        0 => 0.0,
        1 => 0.7,
        2..=6 => 1.0,
        7..=10 => 0.8,
        _ => 0.55,
    }
}

fn plan_quality_specificity_score(steps: &[TraceEvalPlanStep]) -> f64 {
    if steps.is_empty() {
        return 0.0;
    }
    let specific = steps
        .iter()
        .filter(|step| {
            meaningful_tokens(&step.text_safe).len() >= 2
                && !plan_step_looks_generic(&step.text_safe)
        })
        .count();
    specific as f64 / steps.len() as f64
}

fn plan_quality_completeness_score(steps: &[TraceEvalPlanStep]) -> f64 {
    if steps.is_empty() {
        return 0.0;
    }
    let mut has_work_step = false;
    let mut has_final_step = false;
    for step in steps {
        let action = step.expected_action.trim().to_lowercase();
        let text = step.text_safe.to_lowercase();
        if action == "tool_call"
            || action == "read_result"
            || text.contains("search")
            || text.contains("lookup")
            || text.contains("read")
            || text.contains("fetch")
        {
            has_work_step = true;
        }
        if action == "final_answer"
            || text.contains("answer")
            || text.contains("respond")
            || text.contains("summar")
        {
            has_final_step = true;
        }
    }
    let mut score: f64 = 0.0;
    if has_work_step {
        score += 0.45;
    }
    if has_final_step {
        score += 0.45;
    }
    if steps.len() >= 2 {
        score += 0.1;
    } else if has_work_step || has_final_step {
        score += 0.05;
    }
    score.clamp(0.0, 1.0)
}

fn plan_quality_order_score(steps: &[TraceEvalPlanStep]) -> f64 {
    if steps.is_empty() {
        return 0.0;
    }
    let mut score = 1.0_f64;
    let mut seen = BTreeSet::new();
    let mut final_step_index: Option<usize> = None;
    let mut first_work_step_index: Option<usize> = None;
    for (idx, step) in steps.iter().enumerate() {
        if step.index != 0 && step.index != (idx as i64 + 1) {
            score -= 0.2;
        }
        let key = meaningful_tokens(&step.text_safe).join(" ");
        if !key.is_empty() && !seen.insert(key) {
            score -= 0.25;
        }
        let action = step.expected_action.trim().to_lowercase();
        if action == "final_answer" && final_step_index.is_none() {
            final_step_index = Some(idx);
        }
        if (action == "tool_call" || action == "read_result") && first_work_step_index.is_none() {
            first_work_step_index = Some(idx);
        }
    }
    if let (Some(final_idx), Some(work_idx)) = (final_step_index, first_work_step_index) {
        if final_idx < work_idx {
            score -= 0.25;
        }
    }
    score.clamp(0.0, 1.0)
}

fn plan_quality_relevance_score(trace_ctx: &TraceEvalContext) -> f64 {
    let Some(task) = &trace_ctx.task else {
        return 0.75;
    };
    if task.text_safe.trim().is_empty() {
        return 0.75;
    }
    let task_tokens = token_set(meaningful_tokens(&task.text_safe));
    if task_tokens.is_empty() {
        return 0.75;
    }
    let plan_tokens = token_set(meaningful_tokens(&plan_text(&trace_ctx.plan.steps)));
    let overlap = task_tokens
        .iter()
        .filter(|token| plan_tokens.contains(*token))
        .count();
    let mut score = overlap as f64 / (task_tokens.len().min(5) as f64);
    if score == 0.0 && plan_quality_completeness_score(&trace_ctx.plan.steps) > 0.0 {
        score = 0.4;
    }
    score.clamp(0.0, 1.0)
}

fn plan_text(steps: &[TraceEvalPlanStep]) -> String {
    steps
        .iter()
        .map(|step| step.text_safe.as_str())
        .collect::<Vec<_>>()
        .join(" ")
}

fn token_set(tokens: Vec<String>) -> BTreeSet<String> {
    tokens.into_iter().collect()
}

fn meaningful_tokens(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .filter_map(|token| {
            let token = token.trim();
            if token.len() < 3 || is_trace_eval_stop_word(token) {
                None
            } else {
                Some(token.to_string())
            }
        })
        .collect()
}

fn plan_step_looks_generic(text: &str) -> bool {
    matches!(
        text.trim().to_lowercase().as_str(),
        "" | "do it" | "complete task" | "solve task" | "answer" | "respond" | "think" | "plan"
    )
}

fn step_efficiency_actual_steps(trace_ctx: &TraceEvalContext) -> i64 {
    trace_ctx
        .execution_steps
        .iter()
        .filter(|step| !(step.kind == "llm_call" && step.role == "planning"))
        .count() as i64
}

fn step_efficiency_minimum_steps(
    trace_ctx: &TraceEvalContext,
    configured_minimum: i64,
    has_configured_minimum: bool,
    actual_steps: i64,
) -> (i64, &'static str) {
    if has_configured_minimum {
        return (configured_minimum, "config");
    }
    if trace_ctx.features.plan_steps_total > 0 {
        return (trace_ctx.features.plan_steps_total, "plan");
    }
    let mut inferred =
        trace_ctx.features.unique_tool_call_count + step_efficiency_final_response_count(trace_ctx);
    if inferred == 0 && trace_ctx.features.llm_call_count > 0 {
        inferred = 1;
    }
    if inferred == 0 && actual_steps > 0 {
        inferred = 1;
    }
    (inferred, "inferred")
}

fn step_efficiency_final_response_count(trace_ctx: &TraceEvalContext) -> i64 {
    trace_ctx
        .execution_steps
        .iter()
        .filter(|step| step.kind == "llm_call" && step.role == "final_response")
        .count() as i64
}

fn duplicate_tool_call_count(trace_ctx: &TraceEvalContext) -> i64 {
    trace_ctx
        .features
        .duplicate_tool_calls
        .iter()
        .map(|group| group.step_ids.len().saturating_sub(1) as i64)
        .sum()
}

fn retry_extra_count(trace_ctx: &TraceEvalContext) -> i64 {
    trace_ctx
        .features
        .retry_groups
        .iter()
        .map(|group| group.step_ids.len().saturating_sub(1) as i64)
        .sum()
}

fn plan_metric_label<'a>(
    score: f64,
    passed: bool,
    pass_label: &'a str,
    review_label: &'a str,
    fail_label: &'a str,
) -> &'a str {
    if passed {
        pass_label
    } else if score >= 0.5 {
        review_label
    } else {
        fail_label
    }
}

fn error_result(label: &str, explanation: impl Into<String>) -> ScorerResult {
    ScorerResult {
        score: 0.0,
        passed: Some(false),
        label: Some(label.to_string()),
        explanation: Some(explanation.into()),
        metadata: None,
    }
}

fn format_score(value: f64) -> String {
    format!("{value:.3}")
}

fn is_trace_eval_stop_word(token: &str) -> bool {
    matches!(
        token,
        "about"
            | "after"
            | "again"
            | "also"
            | "and"
            | "answer"
            | "are"
            | "ask"
            | "for"
            | "from"
            | "into"
            | "the"
            | "then"
            | "this"
            | "that"
            | "task"
            | "their"
            | "them"
            | "they"
            | "with"
            | "will"
            | "you"
            | "your"
    )
}
