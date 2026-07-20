//! Deterministic context utilities.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Result, SdkError};

pub(crate) fn block_on_runtime<F, T>(future: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.block_on(future)
    } else {
        tokio::runtime::Runtime::new()
            .map_err(|err| SdkError::Internal(format!("create runtime: {err}")))?
            .block_on(future)
    }
}

#[derive(Debug, Clone, Default)]
pub struct DeterministicUtils;

impl DeterministicUtils {
    /// Return the deterministic now timestamp (placeholder).
    pub fn now(&self) -> Result<i64> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|dur| dur.as_millis() as i64)
            .map_err(|_| SdkError::Internal("system clock before UNIX epoch".to_string()))
    }

    /// Placeholder random helper.
    pub fn rand(&self) -> Result<f64> {
        Err(SdkError::Internal(
            "rand() not wired to deterministic runtime".to_string(),
        ))
    }
}
