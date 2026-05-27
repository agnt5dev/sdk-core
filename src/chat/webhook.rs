//! Backwards-compatible chat-flow webhook verifier.
//!
//! The canonical verifiers now live in [`crate::webhook`]; this module
//! preserves the platform-keyed `verify_webhook` API for the existing
//! chat handler. New integrations should use [`crate::webhook`]
//! directly.

use std::collections::HashMap;

use super::types::Platform;
pub use crate::webhook::WebhookError;
use crate::webhook::{SlackVerifier, Verifier};

/// Verify an incoming chat webhook for the given platform.
///
/// Returns `Ok(true)` on a valid signature, `Ok(false)` on a mismatch,
/// and `Err` on missing/malformed inputs.
pub fn verify_webhook(
    platform: &Platform,
    secret: &str,
    headers: &HashMap<String, String>,
    body: &[u8],
) -> Result<bool, WebhookError> {
    match platform {
        Platform::Slack => match SlackVerifier.verify(secret.as_bytes(), headers, body) {
            Ok(_) => Ok(true),
            Err(WebhookError::InvalidSignature) => Ok(false),
            Err(e) => Err(e),
        },
        _ => Err(WebhookError::UnsupportedScheme(platform.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;

    #[test]
    fn test_verify_slack_valid() {
        let secret = "test_signing_secret";
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .to_string();
        let body = b"hello world";

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
        headers.insert("x-slack-signature".to_string(), "v0=deadbeef".to_string());

        let result = verify_webhook(&Platform::Slack, "secret", &headers, b"body").unwrap();
        assert!(!result);
    }

    #[test]
    fn test_verify_slack_missing_header() {
        let headers = HashMap::new();
        let result = verify_webhook(&Platform::Slack, "secret", &headers, b"body");
        assert!(result.is_err());
    }
}
