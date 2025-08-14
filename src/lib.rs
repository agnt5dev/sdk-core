// AGNT5 SDK Core - Simple, focused foundation for worker coordination

pub mod worker;
pub mod client;
pub mod error;

// Re-export main types
pub use worker::Worker;
pub use client::WorkerCoordinatorClient;
pub use error::{SdkError, Result};

// Generated protobuf modules
pub mod pb {
    tonic::include_proto!("api.v1");
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}