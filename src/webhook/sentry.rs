//! Sentry webhook verifier — predates Standard Webhooks.
//!
//! Sentry signs the raw body under HMAC-SHA256 with the integration's
//! client secret and ships the lowercase hex digest in
//! `sentry-hook-signature`. There is no timestamp in the signing
//! scheme; `request-id` is unique per delivery and serves as the
//! idempotency key for retried deliveries.

use std::collections::HashMap;

use hmac::{Hmac, Mac};
use sha2::Sha256;

use super::{ct_eq, Verified, Verifier, WebhookError};

type HmacSha256 = Hmac<Sha256>;

pub struct SentryVerifier;

impl Verifier for SentryVerifier {
    fn verify(
        &self,
        secret: &[u8],
        headers: &HashMap<String, String>,
        body: &[u8],
    ) -> Result<Verified, WebhookError> {
        let signature_hex = headers
            .get("sentry-hook-signature")
            .ok_or_else(|| WebhookError::MissingHeader("sentry-hook-signature".into()))?;

        let mut mac =
            HmacSha256::new_from_slice(secret).map_err(|_| WebhookError::InvalidSignature)?;
        mac.update(body);
        let expected = mac.finalize().into_bytes();

        let provided =
            hex::decode(signature_hex.as_str()).map_err(|_| WebhookError::InvalidSignature)?;
        if !ct_eq(&provided, &expected) {
            return Err(WebhookError::InvalidSignature);
        }

        // Sentry surfaces the resource type in a header but the action
        // ("created", "resolved", ...) is in the body. Compose
        // `sentry.{resource}.{action}` when both are known so the
        // gateway has a routing-friendly event type.
        let resource = headers.get("sentry-hook-resource").cloned();
        let action = serde_json::from_slice::<serde_json::Value>(body)
            .ok()
            .and_then(|v| v.get("action").and_then(|a| a.as_str().map(String::from)));
        let event_type = match (resource.as_deref(), action.as_deref()) {
            (Some(r), Some(a)) => Some(format!("sentry.{}.{}", r, a)),
            (Some(r), None) => Some(format!("sentry.{}", r)),
            _ => None,
        };

        // `sentry-hook-timestamp` is milliseconds since epoch.
        let timestamp = headers
            .get("sentry-hook-timestamp")
            .and_then(|s| s.parse::<i64>().ok())
            .map(|ms| ms / 1000);

        Ok(Verified {
            idempotency_key: headers.get("request-id").cloned(),
            event_type,
            timestamp,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sign(secret: &[u8], body: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(secret).unwrap();
        mac.update(body);
        hex::encode(mac.finalize().into_bytes())
    }

    #[test]
    fn accepts_valid_signature() {
        let secret = b"shh";
        let body = br#"{"action":"created","data":{"issue":{}}}"#;
        let sig = sign(secret, body);

        let mut headers = HashMap::new();
        headers.insert("sentry-hook-signature".into(), sig);
        headers.insert("sentry-hook-resource".into(), "issue".into());
        headers.insert("request-id".into(), "req_123".into());

        let v = SentryVerifier.verify(secret, &headers, body).unwrap();
        assert_eq!(v.idempotency_key.as_deref(), Some("req_123"));
        assert_eq!(v.event_type.as_deref(), Some("sentry.issue.created"));
    }

    #[test]
    fn rejects_wrong_signature() {
        let mut headers = HashMap::new();
        headers.insert(
            "sentry-hook-signature".into(),
            "00000000000000000000000000000000".into(),
        );
        assert!(matches!(
            SentryVerifier.verify(b"s", &headers, b"{}"),
            Err(WebhookError::InvalidSignature)
        ));
    }

    #[test]
    fn rejects_missing_header() {
        let headers = HashMap::new();
        assert!(matches!(
            SentryVerifier.verify(b"s", &headers, b"{}"),
            Err(WebhookError::MissingHeader(_))
        ));
    }
}
