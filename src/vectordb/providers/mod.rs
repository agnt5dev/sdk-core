// Vector database provider implementations
pub mod pgvector;
pub mod pinecone;
pub mod qdrant;

// Re-export provider types for convenience
pub use pgvector::PgVectorProvider;
pub use pinecone::PineconeProvider;
pub use qdrant::QdrantProvider;

// Note: Agnt5Provider (platform gateway proxy) was removed — it targeted
// the old Go gateway which no longer exists in the Rust runtime.
// Users should use direct providers (Qdrant, Pinecone, pgvector) instead.
