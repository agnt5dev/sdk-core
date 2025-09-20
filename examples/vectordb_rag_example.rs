// Example demonstrating Vector Database and RAG functionality in AGNT5 SDK-Core
use agnt5_sdk_core::llm::{
    models::EmbeddingsInput, Collection, DistanceMetric, DocumentProcessor, EmbeddingsRequest,
    LlmClient, RagConfig, RagPipeline, VectorDbRegistry,
};
use agnt5_sdk_core::{init_logging, init_telemetry};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging and telemetry (ignore errors if already initialized)
    let _ = init_logging();
    let _ = init_telemetry("vectordb_rag_example", "0.1.0");

    println!("🚀 AGNT5 Vector Database & RAG Example");

    // Create LLM client
    let llm_client = match LlmClient::new() {
        Ok(client) => Arc::new(client),
        Err(e) => {
            println!("❌ Failed to create LLM client: {}", e);
            println!("💡 Make sure to set API keys in environment variables:");
            println!("   OPENAI_API_KEY=your_openai_key");
            return Ok(());
        }
    };

    println!("✅ Created LLM client");

    // Create vector database registry
    let mut vector_registry = VectorDbRegistry::new();

    // Try to load vector databases from environment
    match vector_registry.load_from_environment().await {
        Ok(()) => {
            println!("✅ Loaded vector database providers");
            let providers = vector_registry.list_providers();
            println!("📋 Available vector DB providers: {:?}", providers);

            if let Some(vector_db) = vector_registry.get_default_provider() {
                println!("🗄️  Using vector database: {}", vector_db.provider_name());

                // Test basic vector database operations
                if let Err(e) = test_basic_vectordb_operations(&vector_db).await {
                    println!("⚠️  Basic vector DB operations failed: {}", e);
                } else {
                    println!("✅ Basic vector DB operations successful");
                }

                // Test RAG pipeline
                if let Err(e) = test_rag_pipeline(&llm_client, &vector_db).await {
                    println!("⚠️  RAG pipeline test failed: {}", e);
                } else {
                    println!("✅ RAG pipeline test successful");
                }
            }
        }
        Err(e) => {
            println!("⚠️  No vector databases available: {}", e);
            println!("💡 To test vector database functionality, set up:");
            println!("   QDRANT_URL=http://localhost:6333");
            println!("   or POSTGRES_URL=postgresql://user:password@localhost/database");

            // Still demonstrate the basic concepts without actual DB
            demonstrate_vectordb_concepts(&llm_client).await?;
        }
    }

    println!("✨ Example completed");
    Ok(())
}

async fn test_basic_vectordb_operations(
    vector_db: &Arc<dyn agnt5_sdk_core::llm::VectorDatabase>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 Testing Basic Vector DB Operations");

    // 1. Health check
    vector_db.health_check().await?;
    println!("✅ Health check passed");

    // 2. Create a test collection
    let collection = Collection::new("test_documents".to_string(), 1536) // OpenAI ada-002 dimension
        .with_distance_metric(DistanceMetric::Cosine)
        .with_description("Test collection for example".to_string());

    // Try to create collection (might fail if already exists)
    match vector_db.create_collection(&collection).await {
        Ok(()) => println!("✅ Created collection: {}", collection.name),
        Err(e) => println!("ℹ️  Collection creation: {} (might already exist)", e),
    }

    // 3. List collections
    let collections = vector_db.list_collections().await?;
    println!("📚 Available collections: {:?}", collections);

    Ok(())
}

async fn test_rag_pipeline(
    llm_client: &Arc<LlmClient>,
    vector_db: &Arc<dyn agnt5_sdk_core::llm::VectorDatabase>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🤖 Testing RAG Pipeline");

    // Get first available LLM provider
    let llm_providers = llm_client.list_providers();
    if llm_providers.is_empty() {
        return Err("No LLM providers available".into());
    }

    let llm_provider = &llm_providers[0];
    println!("🔗 Using LLM provider: {}", llm_provider);

    // Configure RAG pipeline
    let rag_config = RagConfig::new(
        "text-embedding-ada-002".to_string(),
        llm_provider.clone(),
        "gpt-3.5-turbo".to_string(),
        "test_documents".to_string(),
    )
    .with_num_results(3);

    let rag_pipeline = RagPipeline::new(
        llm_client.clone(),
        vector_db.clone(),
        rag_config.embedding_model.clone(),
        rag_config.llm_provider.clone(),
        rag_config.collection_name.clone(),
    );

    // Sample documents to ingest
    let documents = vec![
        ("AGNT5 is a platform for orchestration, evals, and monitoring for AI Workflows and Agents. The platform provides a foundation for workflows that survive failures, maintain state across restarts, and coordinate complex multi-step operations with exactly-once guarantees.", "agnt5_overview.md"),
        ("The AGNT5 platform uses a three-plane architecture for clear separation of concerns: Control Plane for strategic resource management, Orchestration Plane for real-time workflow execution, and Data Plane for code execution environments.", "agnt5_architecture.md"),
        ("AGNT5 uses event sourcing with Redpanda as the authoritative event log and CockroachDB for transactional state projections. This ensures reliable replay and recovery capabilities.", "agnt5_event_sourcing.md"),
        ("The SDK provides multi-language bindings with a Rust core implementation. This includes Python, TypeScript, Go, Java, and Kotlin SDKs with FFI bindings to the performant Rust core.", "agnt5_sdk.md"),
    ];

    // Ingest documents
    println!("📄 Ingesting {} documents...", documents.len());
    for (text, source) in documents {
        match rag_pipeline
            .ingest_document(text, Some(source.to_string()))
            .await
        {
            Ok(()) => println!("✅ Ingested: {}", source),
            Err(e) => println!("❌ Failed to ingest {}: {}", source, e),
        }
    }

    // Test queries
    let test_questions = vec![
        "What is AGNT5?",
        "How does the architecture work?",
        "What database technologies does AGNT5 use?",
        "What programming languages are supported?",
    ];

    println!("\n❓ Testing RAG queries:");
    for question in test_questions {
        match rag_pipeline.query(question, 3, "gpt-3.5-turbo").await {
            Ok(response) => {
                println!("\n🔍 Question: {}", question);
                println!("💬 Answer: {}", response.answer);
                println!("📚 Sources found: {}", response.sources.len());
                for (i, source) in response.sources.iter().enumerate() {
                    if let Some(metadata) = &source.metadata {
                        if let Some(source_name) = &metadata.source {
                            println!("   {}. {} (score: {:.3})", i + 1, source_name, source.score);
                        }
                    }
                }
            }
            Err(e) => println!("❌ Query failed for '{}': {}", question, e),
        }
    }

    Ok(())
}

async fn demonstrate_vectordb_concepts(
    llm_client: &Arc<LlmClient>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🎓 Demonstrating Vector Database Concepts (without actual DB)");

    let llm_providers = llm_client.list_providers();
    if llm_providers.is_empty() {
        println!("⚠️  No LLM providers available for embeddings demonstration");
        return Ok(());
    }

    let provider = &llm_providers[0];

    // 1. Generate embeddings for sample texts
    let sample_texts = vec![
        "AGNT5 is a workflow orchestration platform",
        "The platform uses event sourcing for reliability",
        "Multi-language SDKs are available",
        "The architecture has three planes: control, orchestration, and data",
    ];

    println!("🔢 Generating embeddings for sample texts:");
    for (i, text) in sample_texts.iter().enumerate() {
        let request = EmbeddingsRequest {
            model: "text-embedding-ada-002".to_string(),
            input: EmbeddingsInput::String(text.to_string()),
            encoding_format: None,
            dimensions: None,
            user: None,
        };

        match llm_client.embeddings(provider, request).await {
            Ok(response) => {
                if let Some(embedding) = response.first_embedding() {
                    println!(
                        "✅ Text {}: {} dimensions, first 5 values: {:?}",
                        i + 1,
                        embedding.len(),
                        &embedding[..5.min(embedding.len())]
                    );
                }
            }
            Err(e) => println!("❌ Failed to generate embedding for text {}: {}", i + 1, e),
        }
    }

    // 2. Document processing demonstration
    let document_processor = DocumentProcessor::new()
        .with_chunk_size(500)
        .with_overlap(100);

    let long_text = "AGNT5 is a comprehensive platform for orchestration, evaluation, and monitoring of AI workflows and agents. The platform provides a robust foundation for workflows that can survive failures, maintain state across restarts, and coordinate complex multi-step operations with exactly-once guarantees. \n\nThe architecture is built on three core principles: simplicity, reliability, and great developer experience. These tenets guide every decision in the platform's design and implementation. \n\nThe system uses a three-plane architecture that provides clear separation of concerns. The Control Plane handles strategic resource management and infrastructure provisioning. The Orchestration Plane manages real-time workflow execution and task coordination. The Data Plane provides code execution environments.";

    let chunks = document_processor.chunk_text(long_text, Some("demo_document.md".to_string()));

    println!("\n📄 Document chunking demonstration:");
    println!("Original text length: {} characters", long_text.len());
    println!("Generated {} chunks:", chunks.len());

    for (i, chunk) in chunks.iter().enumerate() {
        println!(
            "  Chunk {}: {} chars - \"{}...\"",
            i + 1,
            chunk.text.len(),
            chunk.text.chars().take(50).collect::<String>()
        );
    }

    Ok(())
}
