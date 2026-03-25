use hmac::{Hmac, Mac};
use sha2::Sha256;

use super::types::Platform;

type HmacSha256 = Hmac<Sha256>;

/// Errors during webhook verification.
#[derive(Debug, thiserror::Error)]
pub enum WebhookError {
    #[error("missing required header: {0}")]
    MissingHeader(String),
    #[error("invalid signature")]
    InvalidSignature,
    #[error("timestamp too old (possible replay attack)")]
    TimestampTooOld,
    #[error("unsupported platform: {0}")]
    UnsupportedPlatform(String),
}

/// Verify an incoming webhook request for the given platform.
///
/// Each platform uses different signing mechanisms:
/// - Slack: HMAC-SHA256 with signing secret
/// - Discord: Ed25519 (TODO: Phase 3)
/// - Teams: RSA (TODO: Phase 4)
pub fn verify_webhook(
    platform: &Platform,
    secret: &str,
    headers: &std::collections::HashMap<String, String>,
    body: &[u8],
) -> Result<bool, WebhookError> {
    match platform {
        Platform::Slack => verify_slack(secret, headers, body),
        _ => Err(WebhookError::UnsupportedPlatform(platform.to_string())),
    }
}

/// Verify a Slack webhook request using HMAC-SHA256.
///
/// Slack sends:
/// - `x-slack-request-timestamp`: Unix timestamp of the request
/// - `x-slack-signature`: `v0=<hex HMAC-SHA256>`
///
/// The signed string is: `v0:{timestamp}:{body}`
fn verify_slack(
    signing_secret: &str,
    headers: &std::collections::HashMap<String, String>,
    body: &[u8],
) -> Result<bool, WebhookError> {
    let timestamp = headers
        .get("x-slack-request-timestamp")
        .ok_or_else(|| WebhookError::MissingHeader("x-slack-request-timestamp".into()))?;

    let signature = headers
        .get("x-slack-signature")
        .ok_or_else(|| WebhookError::MissingHeader("x-slack-signature".into()))?;

    // Check timestamp is within 5 minutes to prevent replay attacks
    if let Ok(ts) = timestamp.parse::<i64>() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        if (now - ts).abs() > 300 {
            return Err(WebhookError::TimestampTooOld);
        }
    }

    // Build the base string: v0:{timestamp}:{body}
    let body_str = std::str::from_utf8(body).unwrap_or("");
    let base_string = format!("v0:{}:{}", timestamp, body_str);

    // Compute HMAC-SHA256
    let mut mac =
        HmacSha256::new_from_slice(signing_secret.as_bytes()).expect("HMAC accepts any key size");
    mac.update(base_string.as_bytes());
    let result = mac.finalize();

    // Constant-time comparison to prevent timing attacks
    let expected = signature.strip_prefix("v0=").unwrap_or(signature);
    let computed_hex = hex::encode(result.into_bytes());
    Ok(expected.len() == computed_hex.len()
        && expected
            .bytes()
            .zip(computed_hex.bytes())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_verify_slack_valid() {
        let secret = "test_signing_secret";
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .to_string();
        let body = b"hello world";

        // Compute expected signature
        let base_string = format!("v0:{}:{}", timestamp, "hello world");
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(base_string.as_bytes());
        let expected_sig = format!("v0={}", hex::encode(mac.finalize().into_bytes()));

        let mut headers = HashMap::new();
        headers.insert("x-slack-request-timestamp".to_string(), timestamp);
        headers.insert("x-slack-signature".to_string(), expected_sig);

        let result = verify_webhook(&Platform::Slack, secret, &headers, body).unwrap();
        assert!(result);
    }

    #[test]
    fn test_verify_slack_invalid_signature() {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .to_string();

        let mut headers = HashMap::new();
        headers.insert("x-slack-request-timestamp".to_string(), timestamp);
        headers.insert(
            "x-slack-signature".to_string(),
            "v0=deadbeef".to_string(),
        );

        let result =
            verify_webhook(&Platform::Slack, "secret", &headers, b"body").unwrap();
        assert!(!result);
    }

    #[test]
    fn test_verify_slack_missing_header() {
        let headers = HashMap::new();
        let result = verify_webhook(&Platform::Slack, "secret", &headers, b"body");
        assert!(result.is_err());
    }
}
