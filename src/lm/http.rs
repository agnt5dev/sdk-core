//! Shared HTTP utilities for LM providers.
//!
//! Centralizes retry logic, error classification, and HTTP client construction
//! so all providers get production-grade resilience without duplicating code.

use std::time::Duration;

use anyhow::anyhow;
use rand::Rng;
use reqwest::Client;
use serde::Deserialize;

use crate::error::{Result as SdkResult, SdkError};

// ---------------------------------------------------------------------------
// Retry configuration
// ---------------------------------------------------------------------------

/// Configuration for automatic retries on transient LM API errors.
///
/// Retries use exponential backoff with jitter and respect `retry-after` headers.
/// Retryable status codes: 408, 429, 500, 502, 503, 504, 529.
#[derive(Clone, Debug)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (0 = no retries).
    pub max_retries: u32,
    /// Initial delay before the first retry.
    pub initial_delay: Duration,
    /// Maximum delay between retries (caps exponential growth).
    pub max_delay: Duration,
    /// Jitter factor (0.25 = ±25% random variation on each delay).
    pub jitter_factor: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 2,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(8),
            jitter_factor: 0.25,
        }
    }
}

impl RetryConfig {
    /// Build a RetryConfig from environment variables, falling back to defaults.
    ///
    /// Reads: `AGNT5_LM_MAX_RETRIES`, `AGNT5_LM_INITIAL_DELAY_MS`, `AGNT5_LM_MAX_DELAY_MS`.
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(v) = std::env::var("AGNT5_LM_MAX_RETRIES") {
            if let Ok(n) = v.parse::<u32>() {
                config.max_retries = n;
            }
        }
        if let Ok(v) = std::env::var("AGNT5_LM_INITIAL_DELAY_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                config.initial_delay = Duration::from_millis(ms);
            }
        }
        if let Ok(v) = std::env::var("AGNT5_LM_MAX_DELAY_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                config.max_delay = Duration::from_millis(ms);
            }
        }

        config
    }

    /// Create a config that disables retries entirely.
    pub fn disabled() -> Self {
        Self {
            max_retries: 0,
            ..Default::default()
        }
    }
}

// ---------------------------------------------------------------------------
// Error classification
// ---------------------------------------------------------------------------

/// Structured error information extracted from an HTTP error response.
#[derive(Clone, Debug)]
pub(crate) struct ApiErrorInfo {
    pub status: u16,
    pub message: String,
    pub request_id: Option<String>,
    pub retry_after: Option<Duration>,
}

/// Inspect an HTTP response and return it on success, or extract error details
/// on failure. This replaces the per-provider `ensure_success()` functions.
pub(crate) async fn classify_response(
    response: reqwest::Response,
    provider: &str,
) -> Result<reqwest::Response, ApiErrorInfo> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }

    // Extract headers before consuming the body.
    let retry_after = parse_retry_after(&response);
    let request_id = extract_request_id(&response);

    let body = response
        .text()
        .await
        .unwrap_or_else(|_| "<unable to read body>".to_string());

    let message = parse_error_message(&body, provider)
        .unwrap_or_else(|| format!("{provider} API error ({status}): {body}"));

    Err(ApiErrorInfo {
        status: status.as_u16(),
        message,
        request_id,
        retry_after,
    })
}

/// Convert an `ApiErrorInfo` into an `SdkError::LmApiError`.
pub(crate) fn to_sdk_error(info: &ApiErrorInfo, provider: &str) -> SdkError {
    SdkError::LmApiError {
        status: info.status,
        provider: provider.to_string(),
        message: info.message.clone(),
        request_id: info.request_id.clone(),
    }
}

/// Classify a `reqwest::Error` (transport-level) into an appropriate `SdkError`.
pub(crate) fn classify_reqwest_error(err: reqwest::Error, provider: &str) -> SdkError {
    if err.is_timeout() {
        SdkError::Timeout {
            message: format!("{provider} API request timed out: {err}"),
            operation: "lm_request".to_string(),
            duration_ms: None,
        }
    } else if err.is_connect() {
        SdkError::Connection {
            message: format!("{provider} API connection failed: {err}"),
            code: crate::error::ErrorCode::ConnectionFailed,
            source: Some(Box::new(err)),
        }
    } else {
        SdkError::Other(anyhow!("{provider} API request failed: {err}"))
    }
}

// ---------------------------------------------------------------------------
// Retry execution
// ---------------------------------------------------------------------------

/// Send an HTTP request with automatic retries on transient failures.
///
/// The `build_request` closure is called fresh on each attempt (important for
/// providers like Bedrock that compute per-request signatures).
///
/// When `per_request_timeout` is `Some`, it overrides the client-level timeout
/// for this specific request.
///
/// Retries only cover the initial HTTP request; once the response body begins
/// streaming, mid-stream errors are NOT retried.
pub(crate) async fn send_with_retry<F>(
    build_request: F,
    retry_config: &RetryConfig,
    provider: &str,
    per_request_timeout: Option<Duration>,
) -> SdkResult<reqwest::Response>
where
    F: Fn() -> reqwest::RequestBuilder,
{
    let mut last_error: Option<SdkError> = None;
    let mut last_retry_after: Option<Duration> = None;

    for attempt in 0..=retry_config.max_retries {
        // Backoff before retry attempts (skip for the first attempt).
        if attempt > 0 {
            let delay =
                calculate_retry_delay(retry_config, attempt, last_retry_after.take());
            tracing::warn!(
                provider = provider,
                attempt = attempt,
                max_retries = retry_config.max_retries,
                delay_ms = delay.as_millis() as u64,
                "Retrying LM API request",
            );
            tokio::time::sleep(delay).await;
        }

        // Build and send the request, applying per-request timeout if set.
        let mut req = build_request();
        if let Some(timeout) = per_request_timeout {
            req = req.timeout(timeout);
        }
        let response = match req.send().await {
            Ok(r) => r,
            Err(err) => {
                let sdk_err = classify_reqwest_error(err, provider);
                if sdk_err.is_retryable() && attempt < retry_config.max_retries {
                    last_error = Some(sdk_err);
                    continue;
                }
                return Err(sdk_err);
            }
        };

        // Check HTTP status.
        match classify_response(response, provider).await {
            Ok(r) => return Ok(r),
            Err(info) => {
                let sdk_err = to_sdk_error(&info, provider);
                if sdk_err.is_retryable() && attempt < retry_config.max_retries {
                    last_retry_after = info.retry_after;
                    last_error = Some(sdk_err);
                    continue;
                }
                return Err(sdk_err);
            }
        }
    }

    Err(last_error.unwrap_or_else(|| SdkError::Internal("retry loop exhausted".into())))
}

/// Calculate the delay before a retry attempt.
///
/// Uses exponential backoff with jitter, respecting `retry-after` from the server.
fn calculate_retry_delay(
    config: &RetryConfig,
    attempt: u32,
    retry_after: Option<Duration>,
) -> Duration {
    // Exponential backoff: initial * 2^(attempt-1), capped at max_delay.
    let exp_ms = config
        .initial_delay
        .as_millis()
        .saturating_mul(1u128 << (attempt - 1).min(10));
    let base = Duration::from_millis(exp_ms.min(config.max_delay.as_millis()) as u64);

    // Apply jitter: ±jitter_factor.
    let jitter = rand::rng().random_range(-config.jitter_factor..=config.jitter_factor);
    let jittered_ms = (base.as_millis() as f64 * (1.0 + jitter)).max(0.0) as u64;
    let delay = Duration::from_millis(jittered_ms);

    // Respect server retry-after: use the larger of our calculated delay and the server hint.
    match retry_after {
        Some(server_delay) => delay.max(server_delay),
        None => delay,
    }
}

// ---------------------------------------------------------------------------
// HTTP client builder
// ---------------------------------------------------------------------------

/// Build a production-grade reqwest HTTP client with connection pooling,
/// keepalive, and a separate connect timeout.
pub(crate) fn build_http_client(timeout: Duration) -> SdkResult<Client> {
    Client::builder()
        .timeout(timeout)
        .connect_timeout(Duration::from_secs(10))
        .pool_max_idle_per_host(20)
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_keepalive(Duration::from_secs(60))
        .build()
        .map_err(|err| SdkError::Other(anyhow!("failed to construct HTTP client: {err}")))
}

// ---------------------------------------------------------------------------
// Response metadata extraction
// ---------------------------------------------------------------------------

use super::interface::ResponseMetadata;

/// Extract metadata from HTTP response headers before the body is consumed.
/// Call this on the successful `reqwest::Response` returned by `send_with_retry`.
pub(crate) fn extract_metadata(response: &reqwest::Response) -> ResponseMetadata {
    let headers = response.headers();
    let status_code = Some(response.status().as_u16());

    let request_id = extract_request_id(response);

    let rate_limit_remaining = headers
        .get("x-ratelimit-remaining-requests")
        .or_else(|| headers.get("x-ratelimit-remaining"))
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u32>().ok());

    let rate_limit_reset = headers
        .get("x-ratelimit-reset-requests")
        .or_else(|| headers.get("x-ratelimit-reset"))
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            // Try parsing as seconds (float or integer)
            v.parse::<f64>().ok().map(|secs| Duration::from_secs_f64(secs))
        });

    ResponseMetadata {
        status_code,
        request_id,
        rate_limit_remaining,
        rate_limit_reset,
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Parse `retry-after` (seconds) or `retry-after-ms` (milliseconds) headers.
fn parse_retry_after(response: &reqwest::Response) -> Option<Duration> {
    // Anthropic uses `retry-after-ms`.
    if let Some(val) = response.headers().get("retry-after-ms") {
        if let Ok(s) = val.to_str() {
            if let Ok(ms) = s.trim().parse::<u64>() {
                return Some(Duration::from_millis(ms));
            }
        }
    }

    // Standard `retry-after` header (seconds or HTTP-date; we only parse seconds).
    if let Some(val) = response.headers().get("retry-after") {
        if let Ok(s) = val.to_str() {
            if let Ok(secs) = s.trim().parse::<u64>() {
                return Some(Duration::from_secs(secs));
            }
        }
    }

    None
}

/// Extract a request ID from common provider headers.
fn extract_request_id(response: &reqwest::Response) -> Option<String> {
    for header_name in &["x-request-id", "request-id", "x-amzn-requestid"] {
        if let Some(val) = response.headers().get(*header_name) {
            if let Ok(s) = val.to_str() {
                return Some(s.to_string());
            }
        }
    }
    None
}

/// Attempt to extract a human-readable error message from a JSON error body.
///
/// Supports two common formats:
/// - `{ "error": { "message": "..." } }` (OpenAI, Anthropic, Google)
/// - `{ "message": "..." }` (some providers)
fn parse_error_message(body: &str, _provider: &str) -> Option<String> {
    // Format: { "error": { "message": "..." } }
    #[derive(Deserialize)]
    struct NestedError {
        error: ErrorDetail,
    }
    #[derive(Deserialize)]
    struct ErrorDetail {
        message: String,
    }

    if let Ok(parsed) = serde_json::from_str::<NestedError>(body) {
        return Some(parsed.error.message);
    }

    // Format: { "message": "..." }
    #[derive(Deserialize)]
    struct FlatError {
        message: String,
    }

    if let Ok(parsed) = serde_json::from_str::<FlatError>(body) {
        return Some(parsed.message);
    }

    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_config_defaults() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 2);
        assert_eq!(config.initial_delay, Duration::from_millis(500));
        assert_eq!(config.max_delay, Duration::from_secs(8));
        assert!((config.jitter_factor - 0.25).abs() < f64::EPSILON);
    }

    #[test]
    fn test_retry_config_disabled() {
        let config = RetryConfig::disabled();
        assert_eq!(config.max_retries, 0);
    }

    #[test]
    fn test_retry_delay_exponential_growth() {
        let config = RetryConfig {
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(8),
            jitter_factor: 0.0, // No jitter for deterministic testing
            ..Default::default()
        };

        let d1 = calculate_retry_delay(&config, 1, None);
        let d2 = calculate_retry_delay(&config, 2, None);
        let d3 = calculate_retry_delay(&config, 3, None);

        assert_eq!(d1.as_millis(), 500); // 500 * 2^0
        assert_eq!(d2.as_millis(), 1000); // 500 * 2^1
        assert_eq!(d3.as_millis(), 2000); // 500 * 2^2
    }

    #[test]
    fn test_retry_delay_capped_at_max() {
        let config = RetryConfig {
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(2),
            jitter_factor: 0.0,
            ..Default::default()
        };

        let d5 = calculate_retry_delay(&config, 5, None);
        assert_eq!(d5.as_millis(), 2000); // Capped at max_delay
    }

    #[test]
    fn test_retry_delay_respects_retry_after() {
        let config = RetryConfig {
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(8),
            jitter_factor: 0.0,
            ..Default::default()
        };

        let server_hint = Some(Duration::from_secs(5));
        let delay = calculate_retry_delay(&config, 1, server_hint);

        // Server says 5s, our calculation says 500ms → use server's 5s.
        assert_eq!(delay.as_secs(), 5);
    }

    #[test]
    fn test_retry_delay_jitter_bounds() {
        let config = RetryConfig {
            initial_delay: Duration::from_millis(1000),
            max_delay: Duration::from_secs(8),
            jitter_factor: 0.25,
            ..Default::default()
        };

        // Run multiple iterations to check jitter stays within bounds.
        for _ in 0..100 {
            let delay = calculate_retry_delay(&config, 1, None);
            let ms = delay.as_millis();
            // 1000ms ± 25% → [750, 1250]
            assert!(ms >= 750, "delay {ms}ms below lower bound 750ms");
            assert!(ms <= 1250, "delay {ms}ms above upper bound 1250ms");
        }
    }

    #[test]
    fn test_parse_error_message_nested() {
        let body = r#"{"error": {"message": "Rate limit exceeded"}}"#;
        assert_eq!(
            parse_error_message(body, "openai"),
            Some("Rate limit exceeded".to_string())
        );
    }

    #[test]
    fn test_parse_error_message_flat() {
        let body = r#"{"message": "Unauthorized"}"#;
        assert_eq!(
            parse_error_message(body, "test"),
            Some("Unauthorized".to_string())
        );
    }

    #[test]
    fn test_parse_error_message_unparseable() {
        let body = "Internal Server Error";
        assert_eq!(parse_error_message(body, "test"), None);
    }
}
