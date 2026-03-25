use serde_json::Value;
use std::collections::HashMap;

use super::types::{Attachment, ChatEvent, ChatMessage, ChatUser, Platform};

/// Errors during event parsing.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("invalid JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),
    #[error("missing field: {0}")]
    MissingField(String),
    #[error("unsupported event type: {0}")]
    UnsupportedEvent(String),
    #[error("unsupported platform: {0}")]
    UnsupportedPlatform(String),
}

/// Parse a raw webhook body into a normalized ChatEvent.
pub fn parse_event(platform: &Platform, body: &[u8]) -> Result<ChatEvent, ParseError> {
    match platform {
        Platform::Slack => parse_slack_event(body),
        _ => Err(ParseError::UnsupportedPlatform(platform.to_string())),
    }
}

/// Parse a Slack Events API payload.
///
/// Slack sends different payload types:
/// - `url_verification`: Challenge during webhook setup
/// - `event_callback`: Actual events (message, app_mention, reaction_added, etc.)
fn parse_slack_event(body: &[u8]) -> Result<ChatEvent, ParseError> {
    let payload: Value = serde_json::from_slice(body)?;

    let event_type = payload["type"]
        .as_str()
        .ok_or_else(|| ParseError::MissingField("type".into()))?;

    match event_type {
        "url_verification" => {
            let challenge = payload["challenge"]
                .as_str()
                .ok_or_else(|| ParseError::MissingField("challenge".into()))?;
            Ok(ChatEvent::UrlVerification {
                challenge: challenge.to_string(),
            })
        }
        "event_callback" => {
            let event = &payload["event"];
            let inner_type = event["type"]
                .as_str()
                .ok_or_else(|| ParseError::MissingField("event.type".into()))?;

            match inner_type {
                "app_mention" => parse_slack_message(event, true),
                "message" => {
                    // Skip bot messages to avoid infinite loops
                    if event.get("bot_id").is_some() || event.get("subtype").is_some() {
                        return Err(ParseError::UnsupportedEvent(
                            "bot message or subtype (skipped)".into(),
                        ));
                    }
                    parse_slack_message(event, false)
                }
                "reaction_added" => parse_slack_reaction(event),
                _ => Err(ParseError::UnsupportedEvent(inner_type.to_string())),
            }
        }
        _ => Err(ParseError::UnsupportedEvent(event_type.to_string())),
    }
}

/// Parse a Slack message or app_mention event into a ChatMessage.
fn parse_slack_message(event: &Value, is_mention: bool) -> Result<ChatEvent, ParseError> {
    let user_id = event["user"]
        .as_str()
        .ok_or_else(|| ParseError::MissingField("event.user".into()))?;

    let text = event["text"].as_str().unwrap_or("");
    let channel = event["channel"]
        .as_str()
        .ok_or_else(|| ParseError::MissingField("event.channel".into()))?;
    let ts = event["ts"].as_str().unwrap_or("");
    let thread_ts = event["thread_ts"].as_str().map(|s| s.to_string());

    // Check if this is a DM (channel type "im")
    let channel_type = event["channel_type"].as_str().unwrap_or("");
    let is_dm = channel_type == "im";

    // Parse file attachments
    let attachments = parse_slack_files(event);

    let message = ChatMessage {
        id: ts.to_string(),
        platform: Platform::Slack,
        channel_id: channel.to_string(),
        thread_id: thread_ts,
        author: ChatUser {
            id: user_id.to_string(),
            name: user_id.to_string(), // Slack doesn't include display name in events
            platform: Platform::Slack,
        },
        content: text.to_string(),
        attachments,
        is_mention: is_mention || is_dm, // DMs are implicit mentions
        is_dm,
        raw_payload: serde_json::to_vec(event).unwrap_or_default(),
        metadata: HashMap::new(),
    };

    if is_mention || is_dm {
        Ok(ChatEvent::Mention(message))
    } else {
        Ok(ChatEvent::Message(message))
    }
}

/// Parse Slack file attachments from an event.
fn parse_slack_files(event: &Value) -> Vec<Attachment> {
    let Some(files) = event["files"].as_array() else {
        return Vec::new();
    };

    files
        .iter()
        .filter_map(|f| {
            let filename = f["name"].as_str()?.to_string();
            Some(Attachment {
                filename,
                mime_type: f["mimetype"].as_str().map(|s| s.to_string()),
                url: f["url_private"].as_str().map(|s| s.to_string()),
                size_bytes: f["size"].as_u64(),
            })
        })
        .collect()
}

/// Parse a Slack reaction_added event.
fn parse_slack_reaction(event: &Value) -> Result<ChatEvent, ParseError> {
    let user_id = event["user"]
        .as_str()
        .ok_or_else(|| ParseError::MissingField("event.user".into()))?;
    let reaction = event["reaction"]
        .as_str()
        .ok_or_else(|| ParseError::MissingField("event.reaction".into()))?;

    let item = &event["item"];
    let message_ts = item["ts"].as_str().unwrap_or("");
    let channel = item["channel"].as_str().unwrap_or("");

    Ok(ChatEvent::Reaction {
        message_id: message_ts.to_string(),
        channel_id: channel.to_string(),
        emoji: reaction.to_string(),
        user: ChatUser {
            id: user_id.to_string(),
            name: user_id.to_string(),
            platform: Platform::Slack,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_url_verification() {
        let body = r#"{"type":"url_verification","challenge":"test_challenge_123"}"#;
        let event = parse_event(&Platform::Slack, body.as_bytes()).unwrap();
        match event {
            ChatEvent::UrlVerification { challenge } => {
                assert_eq!(challenge, "test_challenge_123");
            }
            _ => panic!("expected UrlVerification"),
        }
    }

    #[test]
    fn test_parse_app_mention() {
        let body = r#"{
            "type": "event_callback",
            "event": {
                "type": "app_mention",
                "user": "U123",
                "text": "<@UBOT> hello",
                "channel": "C456",
                "ts": "1234567890.123456",
                "thread_ts": "1234567890.000000"
            }
        }"#;
        let event = parse_event(&Platform::Slack, body.as_bytes()).unwrap();
        match event {
            ChatEvent::Mention(msg) => {
                assert_eq!(msg.author.id, "U123");
                assert_eq!(msg.content, "<@UBOT> hello");
                assert_eq!(msg.channel_id, "C456");
                assert_eq!(msg.thread_id, Some("1234567890.000000".to_string()));
                assert!(msg.is_mention);
            }
            _ => panic!("expected Mention"),
        }
    }

    #[test]
    fn test_parse_message() {
        let body = r#"{
            "type": "event_callback",
            "event": {
                "type": "message",
                "user": "U123",
                "text": "hello",
                "channel": "C456",
                "ts": "1234567890.123456"
            }
        }"#;
        let event = parse_event(&Platform::Slack, body.as_bytes()).unwrap();
        match event {
            ChatEvent::Message(msg) => {
                assert_eq!(msg.content, "hello");
                assert!(!msg.is_mention);
            }
            _ => panic!("expected Message"),
        }
    }

    #[test]
    fn test_skip_bot_messages() {
        let body = r#"{
            "type": "event_callback",
            "event": {
                "type": "message",
                "bot_id": "B123",
                "text": "bot reply",
                "channel": "C456",
                "ts": "1234567890.123456"
            }
        }"#;
        let result = parse_event(&Platform::Slack, body.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_reaction() {
        let body = r#"{
            "type": "event_callback",
            "event": {
                "type": "reaction_added",
                "user": "U123",
                "reaction": "thumbsup",
                "item": {
                    "type": "message",
                    "channel": "C456",
                    "ts": "1234567890.123456"
                }
            }
        }"#;
        let event = parse_event(&Platform::Slack, body.as_bytes()).unwrap();
        match event {
            ChatEvent::Reaction {
                emoji,
                user,
                channel_id,
                ..
            } => {
                assert_eq!(emoji, "thumbsup");
                assert_eq!(user.id, "U123");
                assert_eq!(channel_id, "C456");
            }
            _ => panic!("expected Reaction"),
        }
    }

    #[test]
    fn test_parse_dm_is_mention() {
        let body = r#"{
            "type": "event_callback",
            "event": {
                "type": "message",
                "user": "U123",
                "text": "hello in DM",
                "channel": "D456",
                "channel_type": "im",
                "ts": "1234567890.123456"
            }
        }"#;
        let event = parse_event(&Platform::Slack, body.as_bytes()).unwrap();
        match event {
            ChatEvent::Mention(msg) => {
                assert!(msg.is_dm);
                assert!(msg.is_mention);
            }
            _ => panic!("expected Mention for DM"),
        }
    }
}
