//! Webhook verification for inbound integrations.
//!
//! Each external service that sends webhooks to AGNT5 (Sentry, Slack
//! Events API, generic Standard-Webhooks publishers, ...) gets a
//! [`Verifier`] implementation. The runtime gateway dispatches based on
//! the `{source}` path segment, picks the right verifier, and on success
//! envelopes the payload into a `run.queued` record. The [`Verified`]
//! handle carries an idempotency key the runtime stores on the run
//! metadata so workflows can dedupe.
//!
//! [Standard Webhooks](https://www.standardwebhooks.com/) is the
//! canonical scheme. Vendor verifiers exist for services whose signing
//! predates the spec and will not migrate.

use std::collections::HashMap;

mod sentry;
mod slack;
mod standard;

pub use sentry::SentryVerifier;
pub use slack::SlackVerifier;
pub use standard::StandardVerifier;

#[derive(Debug, thiserror::Error)]
pub enum WebhookError {
    #[error("missing required header: {0}")]
    MissingHeader(String),
    #[error("malformed header {0}: {1}")]
    MalformedHeader(String, String),
    #[error("invalid signature")]
    InvalidSignature,
    #[error("timestamp out of tolerance")]
    TimestampOutOfTolerance,
    #[error("unsupported scheme: {0}")]
    UnsupportedScheme(String),
}

/// Metadata extracted alongside a successful signature check. Callers
/// persist these so the workflow runtime can dedupe retried deliveries
/// and route by event type without re-parsing the body.
#[derive(Debug, Clone, Default)]
pub struct Verified {
    pub idempotency_key: Option<String>,
    pub event_type: Option<String>,
    pub timestamp: Option<i64>,
}

pub trait Verifier {
    fn verify(
        &self,
        secret: &[u8],
        headers: &HashMap<String, String>,
        body: &[u8],
    ) -> Result<Verified, WebhookError>;
}

pub(crate) fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

pub(crate) fn check_timestamp(ts: i64, tolerance_secs: i64) -> Result<(), WebhookError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    if (now - ts).abs() > tolerance_secs {
        return Err(WebhookError::TimestampOutOfTolerance);
    }
    Ok(())
}
