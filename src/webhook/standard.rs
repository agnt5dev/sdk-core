//! Standard Webhooks verifier — <https://www.standardwebhooks.com/>.
//!
//! Headers:
//!   - `webhook-id`: stable per-delivery identifier, used as the
//!     idempotency key.
//!   - `webhook-timestamp`: unix epoch seconds when the message was
//!     sent. A 5-minute tolerance window guards against replays.
//!   - `webhook-signature`: space-separated list of `v1,<base64>`
//!     signatures. Multiple values support key rotation; any match
//!     passes.
//!
//! Signed string: `{id}.{timestamp}.{body}` under HMAC-SHA256.

use std::collections::HashMap;

use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use hmac::{Hmac, Mac};
use sha2::Sha256;

use super::{check_timestamp, ct_eq, Verified, Verifier, WebhookError};

type HmacSha256 = Hmac<Sha256>;

const TIMESTAMP_TOLERANCE_SECS: i64 = 5 * 60;

pub struct StandardVerifier;

impl Verifier for StandardVerifier {
    fn verify(
        &self,
        secret: &[u8],
        headers: &HashMap<String, String>,
        body: &[u8],
    ) -> Result<Verified, WebhookError> {
        let id = headers
            .get("webhook-id")
            .ok_or_else(|| WebhookError::MissingHeader("webhook-id".into()))?;
        let ts_str = headers
            .get("webhook-timestamp")
            .ok_or_else(|| WebhookError::MissingHeader("webhook-timestamp".into()))?;
        let sig_header = headers
            .get("webhook-signature")
            .ok_or_else(|| WebhookError::MissingHeader("webhook-signature".into()))?;

        let ts: i64 = ts_str.parse().map_err(|_| {
            WebhookError::MalformedHeader("webhook-timestamp".into(), ts_str.clone())
        })?;
        check_timestamp(ts, TIMESTAMP_TOLERANCE_SECS)?;

        let body_str = std::str::from_utf8(body).unwrap_or("");
        let signed = format!("{}.{}.{}", id, ts, body_str);

        let mut mac =
            HmacSha256::new_from_slice(secret).map_err(|_| WebhookError::InvalidSignature)?;
        mac.update(signed.as_bytes());
        let expected = mac.finalize().into_bytes();

        let mut matched = false;
        for part in sig_header.split(' ') {
            let Some(rest) = part.strip_prefix("v1,") else {
                continue;
            };
            let Ok(sig_bytes) = B64.decode(rest) else {
                continue;
            };
            if ct_eq(&sig_bytes, &expected) {
                matched = true;
                break;
            }
        }
        if !matched {
            return Err(WebhookError::InvalidSignature);
        }

        Ok(Verified {
            idempotency_key: Some(id.clone()),
            event_type: headers.get("webhook-event").cloned(),
            timestamp: Some(ts),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sign(secret: &[u8], id: &str, ts: i64, body: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(secret).unwrap();
        mac.update(format!("{}.{}.{}", id, ts, std::str::from_utf8(body).unwrap()).as_bytes());
        format!("v1,{}", B64.encode(mac.finalize().into_bytes()))
    }

    fn now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    #[test]
    fn accepts_valid_signature() {
        let secret = b"whsec_test";
        let body = br#"{"event":"user.created"}"#;
        let id = "msg_abc";
        let ts = now();
        let sig = sign(secret, id, ts, body);

        let mut headers = HashMap::new();
        headers.insert("webhook-id".into(), id.into());
        headers.insert("webhook-timestamp".into(), ts.to_string());
        headers.insert("webhook-signature".into(), sig);

        let v = StandardVerifier.verify(secret, &headers, body).unwrap();
        assert_eq!(v.idempotency_key.as_deref(), Some("msg_abc"));
        assert_eq!(v.timestamp, Some(ts));
    }

    #[test]
    fn accepts_one_of_multiple_signatures() {
        let secret = b"whsec_current";
        let other = b"whsec_old";
        let body = b"{}";
        let id = "msg_rot";
        let ts = now();

        let mut sig = sign(other, id, ts, body);
        sig.push(' ');
        sig.push_str(&sign(secret, id, ts, body));

        let mut headers = HashMap::new();
        headers.insert("webhook-id".into(), id.into());
        headers.insert("webhook-timestamp".into(), ts.to_string());
        headers.insert("webhook-signature".into(), sig);

        StandardVerifier.verify(secret, &headers, body).unwrap();
    }

    #[test]
    fn rejects_wrong_signature() {
        let body = b"{}";
        let id = "msg_x";
        let ts = now();
        let sig = sign(b"wrong", id, ts, body);

        let mut headers = HashMap::new();
        headers.insert("webhook-id".into(), id.into());
        headers.insert("webhook-timestamp".into(), ts.to_string());
        headers.insert("webhook-signature".into(), sig);

        assert!(matches!(
            StandardVerifier.verify(b"actual", &headers, body),
            Err(WebhookError::InvalidSignature)
        ));
    }

    #[test]
    fn rejects_old_timestamp() {
        let secret = b"whsec";
        let body = b"{}";
        let id = "msg_old";
        let ts = now() - 3600;
        let sig = sign(secret, id, ts, body);

        let mut headers = HashMap::new();
        headers.insert("webhook-id".into(), id.into());
        headers.insert("webhook-timestamp".into(), ts.to_string());
        headers.insert("webhook-signature".into(), sig);

        assert!(matches!(
            StandardVerifier.verify(secret, &headers, body),
            Err(WebhookError::TimestampOutOfTolerance)
        ));
    }

    #[test]
    fn rejects_missing_header() {
        let headers = HashMap::new();
        assert!(matches!(
            StandardVerifier.verify(b"s", &headers, b""),
            Err(WebhookError::MissingHeader(_))
        ));
    }
}
