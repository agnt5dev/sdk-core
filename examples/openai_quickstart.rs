//! Quickstart example for the embedded OpenAI language model implementation.
//! Requires `OPENAI_API_KEY` in the environment.

use std::env;

use agnt5_sdk_core::{
    generate, stream, GenerateRequest, GenerationConfig, Message, MessageRole, OpenAiProvider,
    StreamChunk,
};
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialise tracing once (ignore errors if already set up by caller).
    let _ = tracing_subscriber::fmt::try_init();

    // Build the provider from environment variables (OPENAI_API_KEY, etc.).
    let provider = OpenAiProvider::from_env()?;

    let model = env::var("OPENAI_MODEL").unwrap_or_else(|_| "openai/gpt-4o-mini".to_string());

    let request = GenerateRequest::new(model.clone())
        .system_prompt("You are a concise assistant that answers clearly.")
        .user_message("Summarise Rust's ownership model in two sentences.")
        .configure(|cfg| {
            cfg.temperature = Some(0.4);
            cfg.max_output_tokens = Some(120);
        });

    let response = generate(&provider, request.clone()).await?;
    println!("\n### Non-streaming response\n{}\n", response.text.trim());
    if let Some(usage) = response.usage.as_ref() {
        println!(
            "Usage: prompt {:?}, completion {:?}, total {:?}\n",
            usage.prompt_tokens, usage.completion_tokens, usage.total_tokens
        );
    }

    let stream_request = GenerateRequest::new(model)
        .system_prompt("Respond in haiku form.")
        .message(Message::new(MessageRole::User, "Explain borrowing."))
        .with_config(GenerationConfig {
            temperature: Some(0.6),
            top_p: None,
            max_output_tokens: Some(80),
        });

    let mut stream = stream(&provider, stream_request).await?;

    println!("### Streaming response\n");
    while let Some(chunk) = stream.next().await {
        match chunk? {
            StreamChunk::Delta { content } => {
                print!("{}", content);
            }
            StreamChunk::Completed(final_response) => {
                println!("\n\n---\nModel: {}", final_response.model);
                if let Some(usage) = final_response.usage {
                    println!(
                        "Usage: prompt {:?}, completion {:?}, total {:?}",
                        usage.prompt_tokens, usage.completion_tokens, usage.total_tokens
                    );
                }
            }
        }
    }

    Ok(())
}
