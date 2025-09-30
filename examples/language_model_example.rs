// Example demonstrating the new LanguageModel API

use agnt5_sdk_core::language_model::{
    GenerateOptions, LanguageModel, LanguageModelConfig, PromptInput,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("🤖 AGNT5 Language Model API Example");
    println!("===================================");

    // Create a basic configuration
    let config = LanguageModelConfig::new()
        .with_default_provider("openai")
        .with_default_model("gpt-3.5-turbo");

    // Initialize the language model
    let lm = match LanguageModel::with_config(config) {
        Ok(lm) => lm,
        Err(e) => {
            println!("❌ Failed to initialize LanguageModel: {}", e);
            println!("💡 Tip: Make sure you have LLM providers configured in your environment");
            println!("   For example: export OPENAI_API_KEY=your-key-here");
            return Ok(());
        }
    };

    println!("✅ LanguageModel initialized successfully");

    // List available providers
    let providers = lm.list_providers();
    println!("📋 Available providers: {:?}", providers);

    if providers.is_empty() {
        println!("⚠️  No providers available. Please configure at least one provider:");
        println!("   - OpenAI: export OPENAI_API_KEY=your-key");
        println!("   - Anthropic: export ANTHROPIC_API_KEY=your-key");
        println!("   - OpenRouter: export OPENROUTER_API_KEY=your-key");
        return Ok(());
    }

    // Example 1: Simple text generation
    println!("\n📝 Example 1: Simple text generation");
    println!("=====================================");

    let prompt = PromptInput::Text("What is the capital of France?".to_string());
    let options = GenerateOptions::default()
        .with_max_tokens(50)
        .with_temperature(0.7);

    match lm.generate(prompt, options).await {
        Ok(response) => {
            println!("✅ Response from {}:", response.model);
            println!("📄 Text: {}", response.text);
            if let Some(usage) = response.usage {
                println!(
                    "📊 Usage: {} prompt + {} completion = {} total tokens",
                    usage.prompt_tokens, usage.completion_tokens, usage.total_tokens
                );
            }
        }
        Err(e) => {
            println!("❌ Generation failed: {}", e);
        }
    }

    // Example 2: Chat-style conversation
    println!("\n💬 Example 2: Chat conversation");
    println!("================================");

    let chat_prompt = PromptInput::Messages(vec![
        agnt5_sdk_core::language_model::types::ChatMessage {
            role: "user".to_string(),
            content: "Hello! I'm learning about Rust programming.".to_string(),
        },
        agnt5_sdk_core::language_model::types::ChatMessage {
            role: "assistant".to_string(),
            content: "Hello! Rust is a great language to learn. What would you like to know?"
                .to_string(),
        },
        agnt5_sdk_core::language_model::types::ChatMessage {
            role: "user".to_string(),
            content: "What makes Rust special compared to other languages?".to_string(),
        },
    ]);

    let chat_options = GenerateOptions::default()
        .with_max_tokens(100)
        .with_temperature(0.8);

    match lm.generate(chat_prompt, chat_options).await {
        Ok(response) => {
            println!("✅ Chat response from {}:", response.model);
            println!("💭 Assistant: {}", response.text);
        }
        Err(e) => {
            println!("❌ Chat generation failed: {}", e);
        }
    }

    // Example 3: Streaming (simplified for demo)
    println!("\n🌊 Example 3: Streaming generation");
    println!("===================================");

    let stream_prompt = PromptInput::Text("Tell me a very short joke".to_string());
    let stream_options = GenerateOptions::default().with_max_tokens(50);

    match lm.stream(stream_prompt, stream_options).await {
        Ok(mut stream) => {
            use futures::StreamExt;
            println!("🎭 Joke: ");
            while let Some(chunk_result) = stream.next().await {
                match chunk_result {
                    Ok(chunk) => {
                        print!("{}", chunk.text);
                        if chunk.finished {
                            println!(
                                "\n✅ Stream finished with reason: {:?}",
                                chunk.finish_reason
                            );
                            break;
                        }
                    }
                    Err(e) => {
                        println!("\n❌ Stream error: {}", e);
                        break;
                    }
                }
            }
        }
        Err(e) => {
            println!("❌ Stream setup failed: {}", e);
        }
    }

    println!("\n🎉 Language Model API example completed!");
    println!("💡 This API provides a simplified interface to language models");
    println!("   with consistent behavior across all providers.");

    Ok(())
}
