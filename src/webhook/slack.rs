//! Slack Events API verifier — <https://api.slack.com/authentication/verifying-requests-from-slack>.
//!
//! Headers:
//!   - `x-slack-request-timestamp`: unix epoch seconds.
//!   - `x-slack-signature`: `v0=<hex(HMAC_SHA256(secret, "v0:{ts}:{body}"))>`.
//!
//! Slack does not ship a stable per-delivery id, so we surface the
//! event type (from `event.type` or `type` in the JSON payload) and
//! leave idempotency to the workflow.

use std::collections::HashMap;

use hmac::{Hmac, Mac};
use sha2::Sha256;

use super::{check_timestamp, ct_eq, Verified, Verifier, WebhookError};

type HmacSha256 = Hmac<Sha256>;

const TIMESTAMP_TOLERANCE_SECS: i64 = 5 * 60;

pub struct SlackVerifier;

impl Verifier for SlackVerifier {
    fn verify(
        &self,
        secret: &[u8],
        headers: &HashMap<String, String>,
        body: &[u8],
    ) -> Result<Verified, WebhookError> {
        let ts_str = headers
            .get("x-slack-request-timestamp")
            .ok_or_else(|| WebhookError::MissingHeader("x-slack-request-timestamp".into()))?;
        let signature = headers
            .get("x-slack-signature")
            .ok_or_else(|| WebhookError::MissingHeader("x-slack-signature".into()))?;

        let ts: i64 = ts_str.parse().map_err(|_| {
            WebhookError::MalformedHeader("x-slack-request-timestamp".into(), ts_str.clone())
        })?;
        check_timestamp(ts, TIMESTAMP_TOLERANCE_SECS)?;

        let body_str = std::str::from_utf8(body).unwrap_or("");
        let signed = format!("v0:{}:{}", ts, body_str);

        let mut mac =
            HmacSha256::new_from_slice(secret).map_err(|_| WebhookError::InvalidSignature)?;
        mac.update(signed.as_bytes());
        let expected = mac.finalize().into_bytes();

        let provided_hex = signature.strip_prefix("v0=").unwrap_or(signature);
        let provided = hex::decode(provided_hex).map_err(|_| WebhookError::InvalidSignature)?;
        if !ct_eq(&provided, &expected) {
            return Err(WebhookError::InvalidSignature);
        }

        let event_type = serde_json::from_slice::<serde_json::Value>(body)
            .ok()
            .and_then(|v| {
                v.get("event")
                    .and_then(|e| e.get("type"))
                    .and_then(|t| t.as_str().map(String::from))
                    .or_else(|| v.get("type").and_then(|t| t.as_str().map(String::from)))
            });

        Ok(Verified {
            idempotency_key: None,
            event_type,
            timestamp: Some(ts),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sign(secret: &[u8], ts: i64, body: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(secret).unwrap();
        mac.update(format!("v0:{}:{}", ts, std::str::from_utf8(body).unwrap()).as_bytes());
        format!("v0={}", hex::encode(mac.finalize().into_bytes()))
    }

    fn now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    #[test]
    fn accepts_valid_signature() {
        let secret = b"signing-secret";
        let body = br#"{"type":"event_callback","event":{"type":"app_mention"}}"#;
        let ts = now();
        let sig = sign(secret, ts, body);

        let mut headers = HashMap::new();
        headers.insert("x-slack-request-timestamp".into(), ts.to_string());
        headers.insert("x-slack-signature".into(), sig);

        let v = SlackVerifier.verify(secret, &headers, body).unwrap();
        assert_eq!(v.event_type.as_deref(), Some("app_mention"));
    }

    #[test]
    fn rejects_wrong_signature() {
        let ts = now();
        let mut headers = HashMap::new();
        headers.insert("x-slack-request-timestamp".into(), ts.to_string());
        headers.insert("x-slack-signature".into(), "v0=deadbeef".into());
        assert!(matches!(
            SlackVerifier.verify(b"s", &headers, b"body"),
            Err(WebhookError::InvalidSignature)
        ));
    }
}
