use thiserror::Error;

#[derive(Error, Debug)]
pub enum SdkError {
    #[error("gRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    
    #[error("gRPC status error: {0}")]
    Status(#[from] tonic::Status),
    
    #[error("Connection error: {0}")]
    Connection(String),
    
    #[error("Registration error: {0}")]
    Registration(String),
    
    #[error("Worker error: {0}")]
    Worker(String),
    
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    
    #[error("Other error: {0}")]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, SdkError>;