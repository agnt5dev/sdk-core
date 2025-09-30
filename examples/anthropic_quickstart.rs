//! Quickstart example for the embedded Anthropic language model implementation.
//! Requires `ANTHROPIC_API_KEY` in the environment.

use std::env;

use agnt5_sdk_core::{
    generate, stream, AnthropicProvider, GenerateRequest, GenerationConfig, Message, MessageRole,
    StreamChunk,
};
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let provider = AnthropicProvider::from_env()?;
    let model = env::var("ANTHROPIC_MODEL")
        .unwrap_or_else(|_| "anthropic/claude-3-haiku-20240307".to_string());

    let request = GenerateRequest::new(model.clone())
        .system_prompt("You are an expert technical writer. Answer succinctly.")
        .message(Message::new(
            MessageRole::User,
            "List two key properties of Rust's borrow checker.",
        ))
        .configure(|cfg| {
            cfg.temperature = Some(0.5);
            cfg.max_output_tokens = Some(200);
        });

    let response = generate(&provider, request.clone()).await?;
    println!("\n### Non-streaming response\n{}\n", response.text.trim());
    if let Some(usage) = response.usage.as_ref() {
        println!(
            "Usage: input {:?}, output {:?}, total {:?}\n",
            usage.prompt_tokens, usage.completion_tokens, usage.total_tokens
        );
    }

    let stream_request = GenerateRequest::new(model)
        .system_prompt("Answer in poetic form.")
        .user_message("Describe zero-cost abstractions.")
        .with_config(GenerationConfig {
            temperature: Some(0.7),
            top_p: None,
            max_output_tokens: Some(120),
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
                        "Usage: input {:?}, output {:?}, total {:?}",
                        usage.prompt_tokens, usage.completion_tokens, usage.total_tokens
                    );
                }
            }
        }
    }

    Ok(())
}
