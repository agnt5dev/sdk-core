//! Multi-platform chat SDK for connecting AGNT5 agents to chat platforms.
//!
//! This module provides the shared core for building chatbots that work across
//! Slack, Discord, Teams, Telegram, and other platforms. All platform-specific
//! logic lives here in Rust so language SDKs only need thin wrappers.

pub mod adapter;
pub mod message_buffer;
pub mod request_builder;
pub mod types;
pub mod webhook;

pub use adapter::{parse_event, ParseError};
pub use message_buffer::StreamingMessageBuffer;
pub use request_builder::PlatformRequest;
pub use types::{Attachment, ChatEvent, ChatMessage, ChatResponse, ChatUser, Platform};
pub use webhook::{verify_webhook, WebhookError};
