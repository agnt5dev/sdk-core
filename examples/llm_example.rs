// Example demonstrating LLM integration in AGNT5 SDK-Core
use agnt5_sdk_core::llm::{ChatCompletionRequest, ChatMessage, ChatMessageContent, LlmClient};
use agnt5_sdk_core::{init_logging, init_telemetry};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging and telemetry (ignore errors if already initialized)
    let _ = init_logging();
    let _ = init_telemetry("llm_example", "0.1.0");

    println!("🚀 AGNT5 LLM Example");

    // Create LLM client (will load providers from environment)
    match LlmClient::new() {
        Ok(client) => {
            println!("✅ Created LLM client");

            // List available providers
            let providers = client.list_providers();
            println!("📋 Available providers: {:?}", providers);

            if !providers.is_empty() {
                // Create a simple chat completion request
                let request = ChatCompletionRequest {
                    model: "gpt-3.5-turbo".to_string(),
                    messages: vec![ChatMessage {
                        role: "user".to_string(),
                        content: Some(ChatMessageContent::String(
                            "Hello! How are you?".to_string(),
                        )),
                        name: None,
                        tool_calls: None,
                        tool_call_id: None,
                        refusal: None,
                    }],
                    temperature: Some(0.7),
                    max_tokens: Some(100),
                    stream: Some(false),
                    // ... other fields will use defaults
                    top_p: None,
                    n: None,
                    stop: None,
                    max_completion_tokens: None,
                    presence_penalty: None,
                    frequency_penalty: None,
                    tools: None,
                    tool_choice: None,
                    response_format: None,
                    reasoning: None,
                    logprobs: None,
                    top_logprobs: None,
                    seed: None,
                    user: None,
                };

                // Try to make a request with the first available provider
                let provider_name = &providers[0];
                println!("🤖 Making request to provider: {}", provider_name);

                match client.chat_completion(provider_name, request).await {
                    Ok(response) => {
                        println!("✅ Request successful!");
                        match response {
                            agnt5_sdk_core::llm::ChatCompletionResponse::NonStream(completion) => {
                                if let Some(choice) = completion.choices.first() {
                                    if let Some(ChatMessageContent::String(content)) =
                                        &choice.message.content
                                    {
                                        println!("💬 Response: {}", content);
                                    }
                                }
                                println!(
                                    "📊 Usage: {} prompt tokens, {} completion tokens",
                                    completion.usage.prompt_tokens,
                                    completion.usage.completion_tokens
                                );
                            }
                            agnt5_sdk_core::llm::ChatCompletionResponse::Stream(_) => {
                                println!("📡 Received streaming response (streaming not implemented yet)");
                            }
                        }
                    }
                    Err(e) => {
                        println!("❌ Request failed: {}", e);
                    }
                }
            } else {
                println!("⚠️  No providers available. Set API keys in environment variables:");
                println!("   OPENAI_API_KEY=your_openai_key");
                println!("   ANTHROPIC_API_KEY=your_anthropic_key");
            }
        }
        Err(e) => {
            println!("❌ Failed to create LLM client: {}", e);
            println!("💡 Make sure to set API keys in environment variables:");
            println!("   OPENAI_API_KEY=your_openai_key");
            println!("   ANTHROPIC_API_KEY=your_anthropic_key");
        }
    }

    println!("✨ Example completed");
    Ok(())
}
