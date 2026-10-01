//! What a model accepts, decided from its id.
//!
//! Newer reasoning models reject sampling parameters (`temperature`, `top_p`)
//! with a 400 instead of ignoring them: OpenAI gpt-5 and later and the o-series,
//! and Claude models after Opus 4.6 / Sonnet 4.6 (AGNT5-1403, AGNT5-1456).
//! Each provider asks here rather than keeping its own model list.

/// Strip routing decoration so only the model name is left: a `provider/`
/// prefix, Bedrock region and vendor prefixes (`us.anthropic.`), a Vertex
/// `@version` suffix and a Bedrock `-v1:0` suffix.
fn bare_model(model: &str) -> &str {
    let mut name = model.trim();
    if let Some((_, rest)) = name.rsplit_once('/') {
        name = rest;
    }
    if let Some(idx) = name.find("anthropic.") {
        name = &name[idx + "anthropic.".len()..];
    }
    if let Some((head, _)) = name.split_once('@') {
        name = head;
    }
    if let Some(idx) = name.rfind("-v") {
        let suffix = &name[idx + 2..];
        if suffix.starts_with(|c: char| c.is_ascii_digit()) && suffix.contains(':') {
            name = &name[..idx];
        }
    }
    name
}

/// OpenAI reasoning models: gpt-5 and every later `gpt-N`, and the o-series
/// (o1, o3, o4, ...). They reject `temperature`/`top_p` and take
/// `max_completion_tokens` instead of `max_tokens`. gpt-4o and gpt-4.1 still
/// accept sampling parameters; gpt-oss is not a numbered gpt.
pub(crate) fn is_openai_reasoning_model(model: &str) -> bool {
    let name = bare_model(model);
    if let Some(rest) = name.strip_prefix("gpt-") {
        let major: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        return major.parse::<u32>().is_ok_and(|major| major >= 5);
    }
    if let Some(rest) = name.strip_prefix('o') {
        let digits = rest.chars().take_while(|c| c.is_ascii_digit()).count();
        return digits > 0 && matches!(rest[digits..].chars().next(), None | Some('-'));
    }
    false
}

/// Claude models that reject `temperature`, `top_p` and `top_k`: everything
/// after Opus 4.6 / Sonnet 4.6 / Haiku 4.5, including Fable. Only versions we
/// know accept sampling are allowed through, so a new or unrecognised Claude
/// model defaults to rejecting: dropping a sampling parameter is a quiet
/// degradation, sending one to a model that rejects it fails the call.
pub(crate) fn claude_rejects_sampling_params(model: &str) -> bool {
    let name = bare_model(model);
    let Some(rest) = name.strip_prefix("claude-") else {
        return false;
    };

    // Ids look like claude-3-5-sonnet-20241022, claude-sonnet-4-6,
    // claude-opus-4-20250514, claude-2.1 or claude-instant-1.2.
    let mut version: Vec<u32> = Vec::new();
    for token in rest.split(['-', '.']) {
        let is_version_part =
            !token.is_empty() && token.len() <= 2 && token.chars().all(|c| c.is_ascii_digit());
        if is_version_part {
            version.push(token.parse().unwrap_or(0));
            continue;
        }
        let known_family = matches!(token, "opus" | "sonnet" | "haiku" | "instant");
        if version.is_empty() && !known_family {
            return true;
        }
        if !version.is_empty() {
            break;
        }
    }

    match version.as_slice() {
        [] => true,
        [major] => *major > 4,
        [major, minor, ..] => (*major, *minor) > (4, 6),
    }
}

/// Default output token budget for a Claude model when the caller sets none.
/// `max_tokens` covers thinking plus the answer, and models that think by
/// default exhaust a small budget before answering.
pub(crate) fn claude_default_max_tokens(model: &str) -> u32 {
    if claude_rejects_sampling_params(model) {
        16_384
    } else {
        4_096
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openai_reasoning_models() {
        for model in [
            "gpt-5",
            "gpt-5-mini",
            "gpt-5.5",
            "gpt-6",
            "gpt-6-luna",
            "openai/gpt-6-luna",
            "gpt-7",
            "o1",
            "o1-preview",
            "o3-mini",
            "o4-mini",
        ] {
            assert!(
                is_openai_reasoning_model(model),
                "{model} should be reasoning"
            );
        }
        for model in [
            "gpt-4o",
            "gpt-4o-mini",
            "gpt-4.1",
            "gpt-4-turbo",
            "gpt-3.5-turbo",
            "gpt-oss-120b",
            "openai/gpt-oss-20b",
            "omni-moderation-latest",
            "claude-opus-5",
        ] {
            assert!(
                !is_openai_reasoning_model(model),
                "{model} should not be reasoning"
            );
        }
    }

    #[test]
    fn claude_models_that_reject_sampling() {
        for model in [
            "claude-opus-4-7",
            "claude-opus-4-8",
            "anthropic/claude-opus-5",
            "claude-opus-5-5",
            "claude-sonnet-5",
            "claude-sonnet-5-5",
            "claude-fable-5",
            "claude-fable-5-1",
            "claude-opus-4-7-20260115",
            "anthropic.claude-opus-4-7-v1:0",
            "us.anthropic.claude-sonnet-5-20260301-v1:0",
            "claude-opus-4-7@20260115",
            "claude-newfamily-1",
        ] {
            assert!(
                claude_rejects_sampling_params(model),
                "{model} should reject sampling"
            );
        }
    }

    #[test]
    fn claude_models_that_accept_sampling() {
        for model in [
            "claude-haiku-4-5",
            "claude-haiku-4-5-20251001",
            "claude-sonnet-4-6",
            "claude-opus-4-6",
            "claude-sonnet-4-5-20250929",
            "claude-opus-4-1",
            "claude-opus-4-20250514",
            "claude-sonnet-4-20250514",
            "claude-3-7-sonnet-20250219",
            "claude-3-5-haiku-20241022",
            "claude-3-opus-20240229",
            "claude-2.1",
            "claude-instant-1.2",
            "anthropic/claude-sonnet-4-6",
            "anthropic.claude-3-5-sonnet-20240620-v1:0",
            "us.anthropic.claude-haiku-4-5-20251001-v1:0",
            "claude-sonnet-4-5@20250929",
        ] {
            assert!(
                !claude_rejects_sampling_params(model),
                "{model} should accept sampling"
            );
        }
        assert!(!claude_rejects_sampling_params("gpt-6-luna"));
    }

    #[test]
    fn claude_max_tokens_default_depends_on_generation() {
        assert_eq!(claude_default_max_tokens("anthropic/claude-opus-5"), 16_384);
        assert_eq!(claude_default_max_tokens("claude-haiku-4-5"), 4_096);
    }
}
