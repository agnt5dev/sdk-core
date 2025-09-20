// Example demonstrating OpenRouter's unique features in AGNT5 SDK-Core
use agnt5_sdk_core::llm::{
    LlmClient, Provider,
    providers::openrouter::{OpenRouterProvider, ProviderPreferences, RouteStrategy},
    models::{ChatCompletionRequest, ChatMessage, ChatMessageContent},
    ProviderConfig, ProviderType
};
use agnt5_sdk_core::{init_logging, init_telemetry};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging and telemetry (ignore errors if already initialized)
    let _ = init_logging();
    let _ = init_telemetry("openrouter_example", "0.1.0");

    println!("🚀 AGNT5 OpenRouter Example - Showcasing Unique Features");

    // Check for API key
    let api_key = match std::env::var("OPENROUTER_API_KEY") {
        Ok(key) => key,
        Err(_) => {
            println!("❌ OPENROUTER_API_KEY not found in environment variables");
            println!("💡 Please set your OpenRouter API key:");
            println!("   export OPENROUTER_API_KEY=your_api_key_here");
            println!("   Get your API key at: https://openrouter.ai/keys");
            return Ok(());
        }
    };

    // Example 1: Create OpenRouter provider with native features
    println!("\n📊 Example 1: Creating OpenRouter provider with unique features");

    let config = ProviderConfig::new(
        "openrouter".to_string(),
        api_key.clone(),
        ProviderType::OpenRouter,
    );

    let openrouter = OpenRouterProvider::new(&config)
        .with_models(vec![
            "anthropic/claude-3-haiku".to_string(),
            "openai/gpt-3.5-turbo".to_string(),
            "meta-llama/llama-3.1-8b-instruct:free".to_string(),
        ])
        .with_route(RouteStrategy::Fallback)
        .with_provider_preferences(
            ProviderPreferences::new()
                .require(vec!["chat".to_string()])
        )
        .with_app_name("AGNT5 SDK Example".to_string())
        .with_referer("https://agnt5.ai".to_string());

    println!("✅ Created OpenRouter provider with:");
    println!("   - Multi-model fallback routing");
    println!("   - Provider preferences for chat models only");
    println!("   - App attribution for analytics");

    // Example 2: List available models with pricing
    println!("\n💰 Example 2: Exploring available models with pricing");
    match openrouter.list_models().await {
        Ok(models) => {
            println!("✅ Found {} available models", models.len());

            // Show a few interesting models
            println!("\n🔍 Sample models with pricing:");
            for model in models.iter().take(5) {
                println!("   📝 {}", model.name);
                println!("      ID: {}", model.id);
                if let Some(pricing) = &model.pricing {
                    if let Some(input) = &pricing.input {
                        println!("      Input: ${}/1M tokens", input);
                    }
                    if let Some(output) = &pricing.output {
                        println!("      Output: ${}/1M tokens", output);
                    }
                }
                if let Some(context) = model.context_length {
                    println!("      Context: {} tokens", context);
                }
                println!();
            }
        }
        Err(e) => println!("⚠️  Failed to list models: {}", e),
    }

    // Example 3: Check limits and quotas
    println!("\n📊 Example 3: Checking current limits and usage");
    match openrouter.get_limits().await {
        Ok(limits) => {
            println!("✅ Retrieved account limits:");
            if let Some(daily_limit) = limits.daily_limit {
                println!("   💳 Daily limit: ${:.4}", daily_limit);
            }
            if let Some(daily_used) = limits.daily_used {
                println!("   📈 Daily used: ${:.4}", daily_used);
            }
            if let Some(daily_remaining) = limits.daily_remaining {
                println!("   💰 Daily remaining: ${:.4}", daily_remaining);
            }
            if let Some(rate_limit) = limits.rate_limit {
                println!("   ⏱️  Rate limit: {} requests/minute", rate_limit);
            }
        }
        Err(e) => println!("⚠️  Failed to get limits: {}", e),
    }

    // Example 4: Chat completion with fallback routing
    println!("\n💬 Example 4: Chat completion with multi-model fallback");

    let messages = vec![
        ChatMessage {
            role: "user".to_string(),
            content: Some(ChatMessageContent::String(
                "Explain the benefits of using OpenRouter for AI applications in exactly 2 sentences.".to_string()
            )),
            name: None,
            tool_calls: None,
            tool_call_id: None,
            refusal: None,
        }
    ];

    let request = ChatCompletionRequest {
        model: "anthropic/claude-3-haiku".to_string(), // Primary model
        messages,
        max_tokens: Some(150),
        temperature: Some(0.7),
        stream: Some(false),
        user: Some("example_user".to_string()), // For abuse prevention
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
        top_p: None,
    };

    println!("🔄 Sending request with fallback routing...");
    match openrouter.chat_completion(request).await {
        Ok(response) => {
            if let agnt5_sdk_core::llm::models::ChatCompletionResponse::NonStream(completion) = response {
                println!("✅ Received response:");
                println!("   🤖 Model: {}", completion.model);
                if let Some(choice) = completion.choices.first() {
                    if let Some(ChatMessageContent::String(content)) = &choice.message.content {
                        println!("   💭 Response: {}", content);
                    }
                }
                let usage = completion.usage;
                println!("   📊 Token usage: {} prompt + {} completion = {} total",
                    usage.prompt_tokens,
                    usage.completion_tokens,
                    usage.total_tokens
                );
            }
        }
        Err(e) => println!("❌ Chat completion failed: {}", e),
    }

    // Example 5: Using LLM client registry (recommended approach)
    println!("\n🔗 Example 5: Using OpenRouter through LLM client registry");

    // Set environment variable for registry loading
    std::env::set_var("OPENROUTER_API_KEY", api_key);
    std::env::set_var("OPENROUTER_DEFAULT_MODELS", "anthropic/claude-3-haiku,openai/gpt-3.5-turbo");
    std::env::set_var("OPENROUTER_APP_NAME", "AGNT5 SDK");
    std::env::set_var("OPENROUTER_ROUTE", "fallback");

    match LlmClient::new() {
        Ok(client) => {
            let providers = client.list_providers();
            println!("✅ LLM client loaded with providers: {:?}", providers);

            if providers.contains(&"openrouter".to_string()) {
                println!("🎯 OpenRouter provider available through LLM client");

                let simple_request = ChatCompletionRequest {
                    model: "anthropic/claude-3-haiku".to_string(),
                    messages: vec![
                        ChatMessage {
                            role: "user".to_string(),
                            content: Some(ChatMessageContent::String(
                                "What makes OpenRouter unique for developers?".to_string()
                            )),
                            name: None,
                            tool_calls: None,
                            tool_call_id: None,
                            refusal: None,
                        }
                    ],
                    max_tokens: Some(100),
                    temperature: Some(0.8),
                    stream: Some(false),
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
                    top_p: None,
                    user: None,
                };

                match client.chat_completion("openrouter", simple_request).await {
                    Ok(response) => {
                        if let agnt5_sdk_core::llm::models::ChatCompletionResponse::NonStream(completion) = response {
                            println!("✅ LLM client response received:");
                            if let Some(choice) = completion.choices.first() {
                                if let Some(ChatMessageContent::String(content)) = &choice.message.content {
                                    println!("   💭 {}", content);
                                }
                            }
                        }
                    }
                    Err(e) => println!("❌ LLM client request failed: {}", e),
                }
            }
        }
        Err(e) => println!("⚠️  Failed to create LLM client: {}", e),
    }

    println!("\n✨ OpenRouter Example Completed!");
    println!("\n🔧 Key Features Demonstrated:");
    println!("   ✅ Multi-model fallback routing");
    println!("   ✅ Provider preferences and filtering");
    println!("   ✅ Cost tracking and quota monitoring");
    println!("   ✅ Rich model discovery with pricing");
    println!("   ✅ App attribution and analytics");
    println!("   ✅ Integration with AGNT5 LLM client");

    println!("\n💡 Environment Variables for Configuration:");
    println!("   OPENROUTER_API_KEY=your_api_key");
    println!("   OPENROUTER_DEFAULT_MODELS=model1,model2,model3");
    println!("   OPENROUTER_APP_NAME=YourAppName");
    println!("   OPENROUTER_REFERER=https://yoursite.com");
    println!("   OPENROUTER_ROUTE=fallback");

    Ok(())
}