//! Deterministic agent trace metrics over `trace_eval_context`.

use super::trace_eval_metrics::{
    error_result, score_threshold, trace_eval_context_from_input, TraceEvalContext,
    TraceEvalExecutionStep,
};
use super::ScorerResult;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

pub fn tool_called(input_json: &Value) -> ScorerResult {
    let trace_ctx = match trace_eval_context_from_input(input_json) {
        Ok(trace_ctx) => trace_ctx,
        Err(result) => return result,
    };
    let config = input_json.get("config").unwrap_or(&Value::Null);
    let tool = match required_string(config, "tool", "tool_called") {
        Ok(tool) => tool,
        Err(result) => return result,
    };
    let actual_tools = tool_names(&trace_ctx);
    let matched = actual_tools.iter().any(|name| name == &tool);
    binary_result(
        "tool_called",
        &trace_ctx,
        config,
        matched,
        "called",
        "not_called",
        format!(
            "tool `{tool}` was {}called",
            if matched { "" } else { "not " }
        ),
        Some(json!({
            "expected_tool": tool,
            "actual_tools": actual_tools,
        })),
    )
}

pub fn tool_not_called(input_json: &Value) -> ScorerResult {
    let trace_ctx = match trace_eval_context_from_input(input_json) {
        Ok(trace_ctx) => trace_ctx,
        Err(result) => return result,
    };
    let config = input_json.get("config").unwrap_or(&Value::Null);
    let tool = match required_string(config, "tool", "tool_not_called") {
        Ok(tool) => tool,
        Err(result) => return result,
    };
    let actual_tools = tool_names(&trace_ctx);
    let matched = !actual_tools.iter().any(|name| name == &tool);
    binary_result(
        "tool_not_called",
        &trace_ctx,
        config,
        matched,
        "not_called",
        "called",
        format!(
            "tool `{tool}` was {}called",
            if matched { "not " } else { "" }
        ),
        Some(json!({
            "expected_absent_tool": tool,
            "actual_tools": actual_tools,
        })),
    )
}

pub fn tool_sequence(input_json: &Value) -> ScorerResult {
    tool_trajectory_with_mode(input_json, "tool_sequence", "in_order")
}

pub fn tool_sequence_in_order(input_json: &Value) -> ScorerResult {
    tool_trajectory_with_mode(input_json, "tool_sequence_in_order", "in_order")
}

pub fn tool_sequence_exact(input_json: &Value) -> ScorerResult {
    tool_trajectory_with_mode(input_json, "tool_sequence_exact", "exact")
}

pub fn tool_sequence_any_order(input_json: &Value) -> ScorerResult {
    tool_trajectory_with_mode(input_json, "tool_sequence_any_order", "any_order")
}

pub fn tool_trajectory(input_json: &Value) -> ScorerResult {
    let config = input_json.get("config").unwrap_or(&Value::Null);
    let mode = match trajectory_mode(config, "tool_trajectory", "exact") {
        Ok(mode) => mode,
        Err(result) => return result,
    };
    tool_trajectory_with_mode(input_json, "tool_trajectory", &mode)
}

pub fn tool_params_match(input_json: &Value) -> ScorerResult {
    let trace_ctx = match trace_eval_context_from_input(input_json) {
        Ok(trace_ctx) => trace_ctx,
        Err(result) => return result,
    };
    let config = input_json.get("config").unwrap_or(&Value::Null);
    let tool = match required_string(config, "tool", "tool_params_match") {
        Ok(tool) => tool,
        Err(result) => return result,
    };
    let expected = match required_object(config, "params", "tool_params_match") {
        Ok(params) => params,
        Err(result) => return result,
    };

    let mut candidate_count = 0;
    let mut matched_step_indexes = Vec::new();
    let mut mismatches = Vec::new();
    for step in tool_steps(&trace_ctx)
        .into_iter()
        .filter(|step| step.tool_name == tool)
    {
        candidate_count += 1;
        let safe_attrs = safe_tool_attributes(step);
        let missing_or_mismatched = params_mismatches(expected, &safe_attrs);
        if missing_or_mismatched.is_empty() {
            matched_step_indexes.push(step.index);
        } else {
            mismatches.push(json!({
                "step_index": step.index,
                "mismatched_keys": missing_or_mismatched,
            }));
        }
    }
    let matched = !matched_step_indexes.is_empty();
    binary_result(
        "tool_params_match",
        &trace_ctx,
        config,
        matched,
        "matched",
        "not_matched",
        if matched {
            format!("tool `{tool}` had a call matching the configured safe parameters")
        } else if candidate_count == 0 {
            format!("tool `{tool}` was not called")
        } else {
            format!("tool `{tool}` was called, but no call matched the configured safe parameters")
        },
        Some(json!({
            "tool": tool,
            "candidate_count": candidate_count,
            "matched_step_indexes": matched_step_indexes,
            "mismatches": mismatches,
            "supported_param_keys": supported_param_keys(),
        })),
    )
}

pub fn max_tool_calls(input_json: &Value) -> ScorerResult {
    max_count_metric(input_json, "max_tool_calls", "tool_calls", |trace_ctx| {
        if trace_ctx.features.tool_call_count > 0 {
            trace_ctx.features.tool_call_count
        } else {
            tool_steps(trace_ctx).len() as i64
        }
    })
}

pub fn max_llm_calls(input_json: &Value) -> ScorerResult {
    max_count_metric(input_json, "max_llm_calls", "llm_calls", |trace_ctx| {
        if trace_ctx.features.llm_call_count > 0 {
            trace_ctx.features.llm_call_count
        } else {
            trace_ctx
                .execution_steps
                .iter()
                .filter(|step| step.kind == "llm_call")
                .count() as i64
        }
    })
}

pub fn max_tokens(input_json: &Value) -> ScorerResult {
    max_count_metric(input_json, "max_tokens", "tokens", |trace_ctx| {
        if trace_ctx.features.total_tokens > 0 {
            trace_ctx.features.total_tokens
        } else {
            trace_ctx
                .execution_steps
                .iter()
                .map(|step| step.tokens)
                .sum()
        }
    })
}

pub fn duration_under(input_json: &Value) -> ScorerResult {
    let trace_ctx = match trace_eval_context_from_input(input_json) {
        Ok(trace_ctx) => trace_ctx,
        Err(result) => return result,
    };
    let config = input_json.get("config").unwrap_or(&Value::Null);
    let max_ms = match required_nonnegative_i64(config, "max_ms", "duration_under") {
        Ok(max_ms) => max_ms,
        Err(result) => return result,
    };
    let actual_ms = trace_duration_ms(&trace_ctx);
    let matched = actual_ms <= max_ms;
    binary_result(
        "duration_under",
        &trace_ctx,
        config,
        matched,
        "within_limit",
        "over_limit",
        format!("trace duration was {}ms (max: {}ms)", actual_ms, max_ms),
        Some(json!({
            "actual_ms": actual_ms,
            "max_ms": max_ms,
        })),
    )
}

pub fn no_errors(input_json: &Value) -> ScorerResult {
    let trace_ctx = match trace_eval_context_from_input(input_json) {
        Ok(trace_ctx) => trace_ctx,
        Err(result) => return result,
    };
    let config = input_json.get("config").unwrap_or(&Value::Null);
    let error_count = trace_error_count(&trace_ctx);
    let matched = error_count == 0;
    binary_result(
        "no_errors",
        &trace_ctx,
        config,
        matched,
        "no_errors",
        "has_errors",
        if matched {
            "trace contained no errors".to_string()
        } else {
            format!("trace contained {error_count} error(s)")
        },
        Some(json!({
            "error_count": error_count,
        })),
    )
}

pub fn tool_failure_recovered(input_json: &Value) -> ScorerResult {
    let trace_ctx = match trace_eval_context_from_input(input_json) {
        Ok(trace_ctx) => trace_ctx,
        Err(result) => return result,
    };
    let config = input_json.get("config").unwrap_or(&Value::Null);
    let tool_filter = optional_string(config, "tool");
    let error_code_filter =
        optional_string(config, "error_code").or_else(|| optional_string(config, "errorCode"));

    let failed_tools = tool_steps(&trace_ctx)
        .into_iter()
        .filter(|step| is_failed_tool_step(step))
        .filter(|step| {
            tool_filter
                .as_deref()
                .map_or(true, |tool| step.tool_name == tool || step.name == tool)
        })
        .filter(|step| {
            error_code_filter
                .as_deref()
                .map_or(true, |code| step.error_code == code)
        })
        .collect::<Vec<_>>();
    let first_failure_index = failed_tools
        .iter()
        .map(|step| step.index)
        .min()
        .unwrap_or_default();
    let recovered = first_failure_index > 0
        && trace_ctx
            .execution_steps
            .iter()
            .any(|step| step.index > first_failure_index && is_successful_progress_step(step));
    binary_result(
        "tool_failure_recovered",
        &trace_ctx,
        config,
        recovered,
        "recovered",
        "not_recovered",
        if recovered {
            "trace recovered after a tool failure".to_string()
        } else if failed_tools.is_empty() {
            "trace did not contain the configured failed tool call".to_string()
        } else {
            "trace contained a failed tool call, but no later successful progress".to_string()
        },
        Some(json!({
            "tool": tool_filter,
            "error_code": error_code_filter,
            "failed_tool_count": failed_tools.len(),
            "first_failure_index": first_failure_index,
        })),
    )
}

pub fn state_equals(input_json: &Value) -> ScorerResult {
    let config = input_json.get("config").unwrap_or(&Value::Null);
    let name = match required_string(config, "name", "state_equals") {
        Ok(name) => name,
        Err(result) => return result,
    };
    let Some(expected) = config.get("expected") else {
        return error_result("config_error", "state_equals requires `expected` in config");
    };
    let threshold = match score_threshold(config, "state_equals", 1.0) {
        Ok(threshold) => threshold,
        Err(result) => return result,
    };
    let Some(actual) = state_value(input_json, &name) else {
        return error_result(
            "artifact_error",
            format!("state_equals could not find state snapshot `{name}`"),
        );
    };
    let matched = actual == expected;
    let score = if matched { 1.0 } else { 0.0 };
    ScorerResult {
        score,
        passed: Some(score >= threshold),
        label: Some(if matched { "matched" } else { "not_matched" }.to_string()),
        explanation: Some(format!(
            "state `{name}` {} expected value",
            if matched { "matched" } else { "did not match" }
        )),
        metadata: Some(json!({
            "builtin": "state_equals",
            "state_name": name,
            "threshold": threshold,
            "actual": actual,
            "expected": expected,
        })),
    }
}

fn tool_trajectory_with_mode(input_json: &Value, builtin: &str, mode: &str) -> ScorerResult {
    let trace_ctx = match trace_eval_context_from_input(input_json) {
        Ok(trace_ctx) => trace_ctx,
        Err(result) => return result,
    };
    let config = input_json.get("config").unwrap_or(&Value::Null);
    let expected_tools = match required_string_vec(config, "tools", builtin) {
        Ok(tools) => tools,
        Err(result) => return result,
    };
    let mode = match normalize_trajectory_mode(mode) {
        Ok(mode) => mode,
        Err(result) => return result,
    };
    let actual_tools = tool_names(&trace_ctx);
    let matched = trajectory_matches(&actual_tools, &expected_tools, mode);
    binary_result(
        builtin,
        &trace_ctx,
        config,
        matched,
        "matched",
        "not_matched",
        format!(
            "tool trajectory {} expected {} sequence",
            if matched { "matched" } else { "did not match" },
            mode
        ),
        Some(json!({
            "mode": mode,
            "expected_tools": expected_tools,
            "actual_tools": actual_tools,
        })),
    )
}

fn max_count_metric(
    input_json: &Value,
    builtin: &str,
    count_label: &str,
    actual: impl Fn(&TraceEvalContext) -> i64,
) -> ScorerResult {
    let trace_ctx = match trace_eval_context_from_input(input_json) {
        Ok(trace_ctx) => trace_ctx,
        Err(result) => return result,
    };
    let config = input_json.get("config").unwrap_or(&Value::Null);
    let max = match required_nonnegative_i64(config, "max", builtin) {
        Ok(max) => max,
        Err(result) => return result,
    };
    let count = actual(&trace_ctx).max(0);
    let matched = count <= max;
    binary_result(
        builtin,
        &trace_ctx,
        config,
        matched,
        "within_limit",
        "over_limit",
        format!("{count_label} count was {count} (max: {max})"),
        Some(json!({
            "actual": count,
            "max": max,
        })),
    )
}

fn binary_result(
    builtin: &str,
    trace_ctx: &TraceEvalContext,
    config: &Value,
    matched: bool,
    pass_label: &str,
    fail_label: &str,
    explanation: String,
    extra_metadata: Option<Value>,
) -> ScorerResult {
    let threshold = match score_threshold(config, builtin, 1.0) {
        Ok(threshold) => threshold,
        Err(result) => return result,
    };
    let score = if matched { 1.0 } else { 0.0 };
    let mut metadata = base_metadata(builtin, trace_ctx, threshold);
    if let Some(Value::Object(extra)) = extra_metadata {
        metadata.extend(extra);
    }
    ScorerResult {
        score,
        passed: Some(score >= threshold),
        label: Some(if matched { pass_label } else { fail_label }.to_string()),
        explanation: Some(explanation),
        metadata: Some(Value::Object(metadata)),
    }
}

fn base_metadata(
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
    metadata.insert("session_id".to_string(), json!(trace_ctx.session_id));
    metadata.insert("root_run_id".to_string(), json!(trace_ctx.root_run_id));
    metadata.insert("evidence_refs".to_string(), trace_ctx.evidence_refs.clone());
    metadata
}

fn tool_steps(trace_ctx: &TraceEvalContext) -> Vec<&TraceEvalExecutionStep> {
    trace_ctx
        .execution_steps
        .iter()
        .filter(|step| step.kind == "tool_call")
        .collect()
}

fn tool_names(trace_ctx: &TraceEvalContext) -> Vec<String> {
    tool_steps(trace_ctx)
        .into_iter()
        .filter_map(|step| {
            let name = step.tool_name.trim();
            if name.is_empty() {
                None
            } else {
                Some(name.to_string())
            }
        })
        .collect()
}

fn trajectory_matches(actual: &[String], expected: &[String], mode: &str) -> bool {
    match mode {
        "exact" => actual == expected,
        "in_order" => trajectory_in_order(actual, expected),
        "any_order" => trajectory_any_order(actual, expected),
        _ => false,
    }
}

fn trajectory_in_order(actual: &[String], expected: &[String]) -> bool {
    if expected.is_empty() {
        return true;
    }
    let mut expected_index = 0;
    for name in actual {
        if name == &expected[expected_index] {
            expected_index += 1;
            if expected_index == expected.len() {
                return true;
            }
        }
    }
    false
}

fn trajectory_any_order(actual: &[String], expected: &[String]) -> bool {
    let mut remaining = BTreeMap::<&str, i64>::new();
    for name in actual {
        *remaining.entry(name.as_str()).or_default() += 1;
    }
    for name in expected {
        let Some(count) = remaining.get_mut(name.as_str()) else {
            return false;
        };
        if *count <= 0 {
            return false;
        }
        *count -= 1;
    }
    true
}

fn trajectory_mode(
    config: &Value,
    builtin: &str,
    default_value: &'static str,
) -> Result<String, ScorerResult> {
    let raw = config
        .get("mode")
        .or_else(|| config.get("pattern"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|mode| !mode.is_empty())
        .unwrap_or(default_value);
    normalize_trajectory_mode(raw)
        .map(str::to_string)
        .map_err(|_| {
            error_result(
                "config_error",
                format!("{builtin} mode must be exact, in_order, or any_order"),
            )
        })
}

fn normalize_trajectory_mode(mode: &str) -> Result<&str, ScorerResult> {
    match mode {
        "exact" => Ok("exact"),
        "in_order" => Ok("in_order"),
        "any_order" => Ok("any_order"),
        _ => Err(error_result(
            "config_error",
            "tool trajectory mode must be exact, in_order, or any_order",
        )),
    }
}

fn trace_duration_ms(trace_ctx: &TraceEvalContext) -> i64 {
    let mut first: Option<i64> = None;
    let mut last: Option<i64> = None;
    for step in &trace_ctx.execution_steps {
        if step.started_at > 0 {
            first = Some(first.map_or(step.started_at, |current| current.min(step.started_at)));
            last = Some(last.map_or(step.started_at, |current| current.max(step.started_at)));
        }
        if step.ended_at > 0 {
            last = Some(last.map_or(step.ended_at, |current| current.max(step.ended_at)));
        }
    }
    match (first, last) {
        (Some(first), Some(last)) if last >= first => last - first,
        _ => trace_ctx
            .execution_steps
            .iter()
            .map(|step| step.duration_ms.max(0))
            .sum(),
    }
}

fn trace_error_count(trace_ctx: &TraceEvalContext) -> i64 {
    if trace_ctx.features.error_count > 0 {
        return trace_ctx.features.error_count;
    }
    trace_ctx
        .execution_steps
        .iter()
        .filter(|step| {
            step.kind == "error"
                || !step.error_code.trim().is_empty()
                || step.status.eq_ignore_ascii_case("failed")
        })
        .count() as i64
}

fn is_failed_tool_step(step: &TraceEvalExecutionStep) -> bool {
    step.kind == "tool_call"
        && (step.status.eq_ignore_ascii_case("failed")
            || !step.error_code.trim().is_empty()
            || !step.error_safe.trim().is_empty())
}

fn is_successful_progress_step(step: &TraceEvalExecutionStep) -> bool {
    !matches!(step.kind.as_str(), "error" | "state")
        && !step.status.eq_ignore_ascii_case("failed")
        && step.error_code.trim().is_empty()
        && (step.status.eq_ignore_ascii_case("completed")
            || step.status.eq_ignore_ascii_case("success")
            || !step.output_ref.trim().is_empty()
            || !step.output_hash.trim().is_empty())
}

fn safe_tool_attributes(step: &TraceEvalExecutionStep) -> BTreeMap<&'static str, Value> {
    let mut attrs = BTreeMap::new();
    insert_non_empty(&mut attrs, "tool_name", &step.tool_name);
    insert_non_empty(&mut attrs, "name", &step.name);
    insert_non_empty(&mut attrs, "status", &step.status);
    insert_non_empty(&mut attrs, "summary_safe", &step.summary_safe);
    insert_non_empty(&mut attrs, "arguments_ref", &step.arguments_ref);
    insert_non_empty(&mut attrs, "arguments_hash", &step.arguments_hash);
    insert_non_empty(&mut attrs, "result_ref", &step.result_ref);
    insert_non_empty(&mut attrs, "result_hash", &step.result_hash);
    insert_non_empty(&mut attrs, "input_hash", &step.input_hash);
    insert_non_empty(&mut attrs, "output_hash", &step.output_hash);
    attrs.insert("duration_ms", json!(step.duration_ms));
    attrs
}

fn insert_non_empty(attrs: &mut BTreeMap<&'static str, Value>, key: &'static str, value: &str) {
    if !value.trim().is_empty() {
        attrs.insert(key, json!(value));
    }
}

fn params_mismatches(
    expected: &Map<String, Value>,
    actual: &BTreeMap<&'static str, Value>,
) -> Vec<String> {
    let mut missing_or_mismatched = Vec::new();
    for (key, expected_value) in expected {
        match actual.get(key.as_str()) {
            Some(actual_value) if actual_value == expected_value => {}
            _ => missing_or_mismatched.push(key.to_string()),
        }
    }
    missing_or_mismatched
}

fn supported_param_keys() -> Vec<&'static str> {
    vec![
        "tool_name",
        "name",
        "status",
        "summary_safe",
        "arguments_ref",
        "arguments_hash",
        "result_ref",
        "result_hash",
        "input_hash",
        "output_hash",
        "duration_ms",
    ]
}

fn required_string(config: &Value, key: &str, builtin: &str) -> Result<String, ScorerResult> {
    let Some(value) = config.get(key).and_then(Value::as_str).map(str::trim) else {
        return Err(error_result(
            "config_error",
            format!("{builtin} requires `{key}` in config"),
        ));
    };
    if value.is_empty() {
        return Err(error_result(
            "config_error",
            format!("{builtin} `{key}` must be a non-empty string"),
        ));
    }
    Ok(value.to_string())
}

fn optional_string(config: &Value, key: &str) -> Option<String> {
    config
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn required_string_vec(
    config: &Value,
    key: &str,
    builtin: &str,
) -> Result<Vec<String>, ScorerResult> {
    let Some(values) = config.get(key).and_then(Value::as_array) else {
        return Err(error_result(
            "config_error",
            format!("{builtin} requires `{key}` in config"),
        ));
    };
    if values.is_empty() {
        return Err(error_result(
            "config_error",
            format!("{builtin} `{key}` must be a non-empty array"),
        ));
    }
    let mut out = Vec::with_capacity(values.len());
    for (idx, value) in values.iter().enumerate() {
        let Some(name) = value.as_str().map(str::trim) else {
            return Err(error_result(
                "config_error",
                format!("{builtin} `{key}[{idx}]` must be a non-empty string"),
            ));
        };
        if name.is_empty() {
            return Err(error_result(
                "config_error",
                format!("{builtin} `{key}[{idx}]` must be a non-empty string"),
            ));
        }
        out.push(name.to_string());
    }
    Ok(out)
}

fn required_object<'a>(
    config: &'a Value,
    key: &str,
    builtin: &str,
) -> Result<&'a Map<String, Value>, ScorerResult> {
    let Some(value) = config.get(key).and_then(Value::as_object) else {
        return Err(error_result(
            "config_error",
            format!("{builtin} requires `{key}` object in config"),
        ));
    };
    if value.is_empty() {
        return Err(error_result(
            "config_error",
            format!("{builtin} `{key}` must be a non-empty object"),
        ));
    }
    Ok(value)
}

fn required_nonnegative_i64(config: &Value, key: &str, builtin: &str) -> Result<i64, ScorerResult> {
    let Some(value) = config.get(key).and_then(Value::as_i64) else {
        return Err(error_result(
            "config_error",
            format!("{builtin} requires non-negative `{key}` in config"),
        ));
    };
    if value < 0 {
        return Err(error_result(
            "config_error",
            format!("{builtin} `{key}` must be non-negative"),
        ));
    }
    Ok(value)
}

fn state_value<'a>(input_json: &'a Value, name: &str) -> Option<&'a Value> {
    for key in ["state", "states", "state_snapshots"] {
        let Some(container) = input_json.get(key) else {
            continue;
        };
        if let Some(value) = container.get(name) {
            return Some(value);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_input(config: Value) -> Value {
        json!({
            "output": {"answer": "done"},
            "config": config,
            "trace_eval_context": {
                "schema_version": "agnt5.eval.trace_eval_context.v1",
                "project_id": "project-1",
                "session_id": "session-1",
                "root_run_id": "run-1",
                "execution_steps": [
                    {"index": 1, "kind": "llm_call", "role": "planning", "tokens": 20, "started_at": 10, "ended_at": 20},
                    {"index": 2, "kind": "tool_call", "tool_name": "search", "arguments_hash": "args-a", "result_hash": "result-a", "status": "completed", "duration_ms": 5, "started_at": 20, "ended_at": 25},
                    {"index": 3, "kind": "tool_call", "tool_name": "lookup", "arguments_hash": "args-b", "status": "completed", "duration_ms": 7, "started_at": 25, "ended_at": 32},
                    {"index": 4, "kind": "llm_call", "role": "final_response", "tokens": 30, "started_at": 32, "ended_at": 40}
                ],
                "features": {
                    "execution_step_count": 4,
                    "tool_call_count": 2,
                    "unique_tool_call_count": 2,
                    "llm_call_count": 2,
                    "total_tokens": 50,
                    "error_count": 0
                },
                "evidence_refs": {"normalized_session_ref": "mem://session"}
            }
        })
    }

    fn recovery_input(config: Value) -> Value {
        json!({
            "output": {"answer": "done"},
            "config": config,
            "trace_eval_context": {
                "schema_version": "agnt5.eval.trace_eval_context.v1",
                "project_id": "project-1",
                "session_id": "session-1",
                "root_run_id": "run-1",
                "execution_steps": [
                    {"index": 1, "kind": "tool_call", "tool_name": "search", "status": "failed", "error_code": "SIMULATED_TIMEOUT", "started_at": 10, "ended_at": 12},
                    {"index": 2, "kind": "llm_call", "role": "replan", "status": "completed", "started_at": 12, "ended_at": 20},
                    {"index": 3, "kind": "tool_call", "tool_name": "lookup", "status": "completed", "result_hash": "result-b", "started_at": 20, "ended_at": 30}
                ],
                "features": {
                    "execution_step_count": 3,
                    "tool_call_count": 2,
                    "unique_tool_call_count": 2,
                    "llm_call_count": 1,
                    "total_tokens": 25,
                    "error_count": 1
                },
                "evidence_refs": {"normalized_session_ref": "mem://session"}
            }
        })
    }

    #[test]
    fn tool_trajectory_modes_match_python_helpers() {
        let exact = tool_sequence_exact(&sample_input(json!({"tools": ["search", "lookup"]})));
        assert_eq!(exact.passed, Some(true));

        let exact_missing = tool_sequence_exact(&sample_input(json!({"tools": ["search"]})));
        assert_eq!(exact_missing.passed, Some(false));

        let in_order =
            tool_sequence_in_order(&sample_input(json!({"tools": ["search", "lookup"]})));
        assert_eq!(in_order.passed, Some(true));

        let wrong_order =
            tool_sequence_in_order(&sample_input(json!({"tools": ["lookup", "search"]})));
        assert_eq!(wrong_order.passed, Some(false));

        let any_order =
            tool_sequence_any_order(&sample_input(json!({"tools": ["lookup", "search"]})));
        assert_eq!(any_order.passed, Some(true));
    }

    #[test]
    fn tool_called_and_params_match_use_safe_trace_context_fields() {
        let called = tool_called(&sample_input(json!({"tool": "search"})));
        assert_eq!(called.passed, Some(true));

        let not_called = tool_not_called(&sample_input(json!({"tool": "write_ticket"})));
        assert_eq!(not_called.passed, Some(true));

        let params = tool_params_match(&sample_input(json!({
            "tool": "search",
            "params": {"arguments_hash": "args-a", "status": "completed"}
        })));
        assert_eq!(params.passed, Some(true));

        let mismatch = tool_params_match(&sample_input(json!({
            "tool": "search",
            "params": {"arguments_hash": "wrong"}
        })));
        assert_eq!(mismatch.passed, Some(false));
    }

    #[test]
    fn process_limits_score_trace_features() {
        assert_eq!(
            max_tool_calls(&sample_input(json!({"max": 2}))).passed,
            Some(true)
        );
        assert_eq!(
            max_tool_calls(&sample_input(json!({"max": 1})))
                .label
                .as_deref(),
            Some("over_limit")
        );
        assert_eq!(
            max_llm_calls(&sample_input(json!({"max": 2}))).passed,
            Some(true)
        );
        assert_eq!(
            max_tokens(&sample_input(json!({"max": 40}))).passed,
            Some(false)
        );
        assert_eq!(
            duration_under(&sample_input(json!({"max_ms": 30}))).passed,
            Some(true)
        );
        assert_eq!(no_errors(&sample_input(json!({}))).passed, Some(true));
    }

    #[test]
    fn tool_failure_recovered_requires_later_successful_progress() {
        let recovered = tool_failure_recovered(&recovery_input(json!({
            "tool": "search",
            "error_code": "SIMULATED_TIMEOUT"
        })));
        assert_eq!(recovered.passed, Some(true));
        assert_eq!(recovered.label.as_deref(), Some("recovered"));

        let not_found = tool_failure_recovered(&recovery_input(json!({
            "tool": "write_ticket"
        })));
        assert_eq!(not_found.passed, Some(false));
        assert_eq!(not_found.label.as_deref(), Some("not_recovered"));
    }

    #[test]
    fn state_equals_uses_runtime_supplied_state_snapshots() {
        let input = json!({
            "config": {"name": "cart", "expected": {"items": 2}},
            "states": {"cart": {"items": 2}}
        });
        assert_eq!(state_equals(&input).passed, Some(true));
    }
}
