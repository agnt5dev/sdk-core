//! Error types for the AGNT5 SDK Core

use thiserror::Error;

/// Indicates whether an error is retryable
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryHint {
    /// Error is transient and can be retried immediately
    Retryable,
    /// Error is transient but should be retried with backoff
    RetryableWithBackoff,
    /// Error is permanent and should not be retried
    NotRetryable,
}

/// Error codes for programmatic error handling
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    // Connection errors (retryable)
    ConnectionFailed,
    ConnectionTimeout,
    ServiceUnavailable,

    // Configuration errors (not retryable)
    InvalidConfiguration,
    MissingConfiguration,

    // Execution errors
    ExecutionFailed,
    ExecutionTimeout,
    ExecutionSuspended,

    // Validation errors (not retryable)
    InvalidInput,
    InvalidMessage,
    InvalidState,

    // Resource errors
    ResourceExhausted,
    QuotaExceeded,

    // Internal errors
    InternalError,
    NotImplemented,

    // Sandbox errors
    SandboxExecutionFailed,
    SandboxUnavailable,
    UnsupportedLanguage,
    UnsupportedOperation,
}

#[derive(Error, Debug)]
pub enum SdkError {
    // External crate errors with automatic conversion
    #[error("gRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("gRPC status error: {0}")]
    Status(#[from] tonic::Status),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Other error: {0}")]
    Other(#[from] anyhow::Error),

    // AGNT5-specific errors with codes and context
    #[error("Connection error: {message}")]
    Connection {
        message: String,
        code: ErrorCode,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    #[error("Worker registration failed: {message}")]
    Registration { message: String, code: ErrorCode },

    #[error("Worker registration redirected to {endpoint}: {message}")]
    RegistrationRedirect { endpoint: String, message: String },

    #[error("Invalid configuration: {message}")]
    Configuration {
        message: String,
        field: Option<String>,
    },

    #[error("Function invocation error: {message}")]
    Invocation {
        message: String,
        function_name: Option<String>,
    },

    #[error("State management error: {message}")]
    State { message: String, code: ErrorCode },

    #[error("Timeout error: {message}")]
    Timeout {
        message: String,
        operation: String,
        duration_ms: Option<u64>,
    },

    #[error("Invalid message: {message}")]
    InvalidMessage {
        message: String,
        field: Option<String>,
    },

    #[error("Execution suspended: {message}")]
    SuspendedExecution { message: String, reason: String },

    #[error("Replay error: {message}")]
    ReplayError {
        message: String,
        step_id: Option<String>,
    },

    #[error("Service call error: {message}")]
    ServiceCallError { message: String, service: String },

    #[error("Telemetry error: {0}")]
    TelemetryError(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Service unavailable: {message}")]
    Unavailable {
        message: String,
        service: Option<String>,
    },

    #[error("Invalid argument: {message}")]
    InvalidArgument {
        message: String,
        argument: Option<String>,
    },

    #[error("LM API error ({status}): {message}")]
    LmApiError {
        status: u16,
        provider: String,
        message: String,
        request_id: Option<String>,
    },

    #[error("Sandbox error ({operation}): {message}")]
    Sandbox {
        message: String,
        operation: String,
        code: ErrorCode,
    },
}

impl SdkError {
    /// Returns the error code for programmatic error handling
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::Transport(_) | Self::Status(_) => ErrorCode::ConnectionFailed,
            Self::Connection { code, .. } => *code,
            Self::Registration { code, .. } => *code,
            Self::RegistrationRedirect { .. } => ErrorCode::ConnectionFailed,
            Self::Configuration { .. } => ErrorCode::InvalidConfiguration,
            Self::Invocation { .. } => ErrorCode::ExecutionFailed,
            Self::State { code, .. } => *code,
            Self::Timeout { .. } => ErrorCode::ExecutionTimeout,
            Self::InvalidMessage { .. } => ErrorCode::InvalidMessage,
            Self::SuspendedExecution { .. } => ErrorCode::ExecutionSuspended,
            Self::ReplayError { .. } => ErrorCode::ExecutionFailed,
            Self::ServiceCallError { .. } => ErrorCode::ServiceUnavailable,
            Self::TelemetryError(_) => ErrorCode::InternalError,
            Self::Internal(_) => ErrorCode::InternalError,
            Self::Unavailable { .. } => ErrorCode::ServiceUnavailable,
            Self::InvalidArgument { .. } => ErrorCode::InvalidInput,
            Self::Serialization(_) => ErrorCode::InvalidInput,
            Self::Other(_) => ErrorCode::InternalError,
            Self::LmApiError { status, .. } => match *status {
                401 | 403 => ErrorCode::InvalidConfiguration,
                429 => ErrorCode::ResourceExhausted,
                408 => ErrorCode::ExecutionTimeout,
                500 | 502 | 503 | 504 | 529 => ErrorCode::ServiceUnavailable,
                _ => ErrorCode::InvalidInput,
            },
            Self::Sandbox { code, .. } => *code,
        }
    }

    /// Returns a hint about whether this error is retryable
    pub fn retry_hint(&self) -> RetryHint {
        match self {
            // Network errors are retryable with backoff
            Self::Transport(_) | Self::Status(_) => RetryHint::RetryableWithBackoff,

            Self::Connection { code, .. } => match code {
                ErrorCode::ConnectionTimeout | ErrorCode::ServiceUnavailable => {
                    RetryHint::RetryableWithBackoff
                }
                _ => RetryHint::NotRetryable,
            },
            Self::RegistrationRedirect { .. } => RetryHint::Retryable,

            // Service unavailable is retryable
            Self::Unavailable { .. } => RetryHint::RetryableWithBackoff,
            Self::ServiceCallError { .. } => RetryHint::RetryableWithBackoff,

            // Timeouts are retryable
            Self::Timeout { .. } => RetryHint::Retryable,

            // Resource exhaustion might be retryable after backoff
            Self::State {
                code: ErrorCode::ResourceExhausted,
                ..
            } => RetryHint::RetryableWithBackoff,

            // Configuration, validation, and internal errors are not retryable
            Self::Configuration { .. }
            | Self::InvalidMessage { .. }
            | Self::InvalidArgument { .. }
            | Self::Serialization(_)
            | Self::Internal(_)
            | Self::TelemetryError(_) => RetryHint::NotRetryable,

            // Execution errors depend on the specific error
            Self::Invocation { .. } | Self::ReplayError { .. } => RetryHint::NotRetryable,

            // Suspended execution is not an error to retry
            Self::SuspendedExecution { .. } => RetryHint::NotRetryable,

            // Registration errors are not retryable
            Self::Registration { .. } => RetryHint::NotRetryable,

            // Default for state errors
            Self::State { .. } => RetryHint::NotRetryable,

            // Unknown errors are not retryable by default
            Self::Other(_) => RetryHint::NotRetryable,

            // LM API errors: retry on transient HTTP status codes
            Self::LmApiError { status, .. } => match *status {
                408 | 429 | 500 | 502 | 503 | 504 | 529 => RetryHint::RetryableWithBackoff,
                _ => RetryHint::NotRetryable,
            },

            // Sandbox errors
            Self::Sandbox { code, .. } => match code {
                ErrorCode::SandboxUnavailable => RetryHint::RetryableWithBackoff,
                _ => RetryHint::NotRetryable,
            },
        }
    }

    /// Returns true if this error suggests the operation should be retried
    pub fn is_retryable(&self) -> bool {
        matches!(
            self.retry_hint(),
            RetryHint::Retryable | RetryHint::RetryableWithBackoff
        )
    }
}

pub type Result<T> = std::result::Result<T, SdkError>;
