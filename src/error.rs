//! Error types for the AGNT5 SDK Core

use thiserror::Error;

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
    
    // AGNT5-specific errors
    #[error("Connection error: {0}")]
    Connection(String),
    
    #[error("Worker registration failed: {0}")]
    Registration(String),
    
    #[error("Invalid configuration: {0}")]
    Configuration(String),
    
    #[error("Function invocation error: {0}")]
    Invocation(String),
    
    #[error("State management error: {0}")]
    State(String),
    
    #[error("Timeout error: {0}")]
    Timeout(String),
    
    #[error("Invalid message: {0}")]
    InvalidMessage(String),
    
    #[error("Execution suspended: {0}")]
    SuspendedExecution(String),
    
    #[error("Replay error: {0}")]
    ReplayError(String),
    
    #[error("Service call error: {0}")]
    ServiceCallError(String),
    
    #[error("Telemetry error: {0}")]
    TelemetryError(String),
    
    #[error("Internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, SdkError>;