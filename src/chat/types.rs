use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Supported chat platforms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Platform {
    Slack,
    Discord,
    Teams,
    Telegram,
}

impl Platform {
    pub fn as_str(&self) -> &'static str {
        match self {
            Platform::Slack => "slack",
            Platform::Discord => "discord",
            Platform::Teams => "teams",
            Platform::Telegram => "telegram",
        }
    }
}

impl std::fmt::Display for Platform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for Platform {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "slack" => Ok(Platform::Slack),
            "discord" => Ok(Platform::Discord),
            "teams" => Ok(Platform::Teams),
            "telegram" => Ok(Platform::Telegram),
            _ => Err(format!("unknown platform: {}", s)),
        }
    }
}

/// A user on a chat platform.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatUser {
    /// Platform-specific user ID.
    pub id: String,
    /// Display name.
    pub name: String,
    /// Which platform this user is on.
    pub platform: Platform,
}

/// A file attachment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attachment {
    /// Filename.
    pub filename: String,
    /// MIME type (e.g. "image/png").
    pub mime_type: Option<String>,
    /// Public URL to download the file.
    pub url: Option<String>,
    /// File size in bytes.
    pub size_bytes: Option<u64>,
}

/// A normalized chat message from any platform.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    /// Platform-specific message ID.
    pub id: String,
    /// Which platform this message is from.
    pub platform: Platform,
    /// Channel/conversation ID.
    pub channel_id: String,
    /// Thread ID for threaded conversations (e.g. Slack thread_ts).
    pub thread_id: Option<String>,
    /// Who sent the message.
    pub author: ChatUser,
    /// Text content of the message.
    pub content: String,
    /// File attachments.
    pub attachments: Vec<Attachment>,
    /// Whether the bot was explicitly mentioned.
    pub is_mention: bool,
    /// Whether this is a direct message.
    pub is_dm: bool,
    /// Original platform payload for adapter-specific processing.
    pub raw_payload: Vec<u8>,
    /// Additional platform-specific metadata.
    pub metadata: HashMap<String, String>,
}

/// Normalized event from a chat platform.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChatEvent {
    /// A new message (may or may not mention the bot).
    Message(ChatMessage),
    /// Bot was explicitly mentioned.
    Mention(ChatMessage),
    /// An emoji reaction was added.
    Reaction {
        message_id: String,
        channel_id: String,
        emoji: String,
        user: ChatUser,
    },
    /// A slash command was invoked.
    SlashCommand {
        command: String,
        args: String,
        channel_id: String,
        user: ChatUser,
    },
    /// An interactive component action (button click, select, etc.).
    Action {
        action_id: String,
        value: Option<String>,
        channel_id: String,
        thread_id: Option<String>,
        user: ChatUser,
        /// Raw action payload for platform-specific processing.
        payload: Vec<u8>,
    },
    /// URL verification challenge (Slack sends this during webhook setup).
    UrlVerification { challenge: String },
}

impl ChatEvent {
    /// Get the channel ID for this event, if applicable.
    pub fn channel_id(&self) -> Option<&str> {
        match self {
            ChatEvent::Message(m) | ChatEvent::Mention(m) => Some(&m.channel_id),
            ChatEvent::Reaction { channel_id, .. } => Some(channel_id),
            ChatEvent::SlashCommand { channel_id, .. } => Some(channel_id),
            ChatEvent::Action { channel_id, .. } => Some(channel_id),
            ChatEvent::UrlVerification { .. } => None,
        }
    }

    /// Get the thread ID for this event, if applicable.
    pub fn thread_id(&self) -> Option<&str> {
        match self {
            ChatEvent::Message(m) | ChatEvent::Mention(m) => m.thread_id.as_deref(),
            ChatEvent::Action { thread_id, .. } => thread_id.as_deref(),
            _ => None,
        }
    }

    /// Get the user who triggered this event, if applicable.
    pub fn user(&self) -> Option<&ChatUser> {
        match self {
            ChatEvent::Message(m) | ChatEvent::Mention(m) => Some(&m.author),
            ChatEvent::Reaction { user, .. } => Some(user),
            ChatEvent::SlashCommand { user, .. } => Some(user),
            ChatEvent::Action { user, .. } => Some(user),
            ChatEvent::UrlVerification { .. } => None,
        }
    }
}

/// A response to send back to a chat platform.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatResponse {
    /// Text content to send.
    pub content: String,
    /// Thread ID to reply in (for threaded conversations).
    pub thread_id: Option<String>,
    /// File attachments to include.
    pub attachments: Vec<Attachment>,
    /// Whether this message should be ephemeral (visible only to the user).
    pub ephemeral: bool,
}

impl ChatResponse {
    /// Create a simple text response.
    pub fn text(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            thread_id: None,
            attachments: Vec::new(),
            ephemeral: false,
        }
    }

    /// Set the thread ID for a threaded reply.
    pub fn in_thread(mut self, thread_id: impl Into<String>) -> Self {
        self.thread_id = Some(thread_id.into());
        self
    }

    /// Mark this response as ephemeral.
    pub fn ephemeral(mut self) -> Self {
        self.ephemeral = true;
        self
    }
}
