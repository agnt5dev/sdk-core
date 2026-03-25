use serde_json::json;
use std::collections::HashMap;

/// An HTTP request spec built by Rust, executed by the language SDK's native HTTP client.
#[derive(Debug, Clone)]
pub struct PlatformRequest {
    pub url: String,
    pub method: String,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

const SLACK_API_BASE: &str = "https://slack.com/api";

/// Build a Slack `chat.postMessage` request.
pub fn slack_post_message(
    token: &str,
    channel: &str,
    text: &str,
    thread_ts: Option<&str>,
) -> PlatformRequest {
    let mut payload = json!({
        "channel": channel,
        "text": text,
    });

    if let Some(ts) = thread_ts {
        payload["thread_ts"] = json!(ts);
    }

    slack_request(token, "chat.postMessage", payload)
}

/// Build a Slack `chat.update` request (for streaming post-then-edit).
pub fn slack_update_message(
    token: &str,
    channel: &str,
    message_ts: &str,
    text: &str,
) -> PlatformRequest {
    let payload = json!({
        "channel": channel,
        "ts": message_ts,
        "text": text,
    });

    slack_request(token, "chat.update", payload)
}

/// Build a Slack `chat.postEphemeral` request.
pub fn slack_post_ephemeral(
    token: &str,
    channel: &str,
    user: &str,
    text: &str,
    thread_ts: Option<&str>,
) -> PlatformRequest {
    let mut payload = json!({
        "channel": channel,
        "user": user,
        "text": text,
    });

    if let Some(ts) = thread_ts {
        payload["thread_ts"] = json!(ts);
    }

    slack_request(token, "chat.postEphemeral", payload)
}

/// Build a Slack `reactions.add` request.
pub fn slack_add_reaction(
    token: &str,
    channel: &str,
    message_ts: &str,
    emoji: &str,
) -> PlatformRequest {
    let payload = json!({
        "channel": channel,
        "timestamp": message_ts,
        "name": emoji,
    });

    slack_request(token, "reactions.add", payload)
}

/// Helper to build a Slack API request.
fn slack_request(token: &str, method: &str, payload: serde_json::Value) -> PlatformRequest {
    let mut headers = HashMap::new();
    headers.insert(
        "Authorization".to_string(),
        format!("Bearer {}", token),
    );
    headers.insert("Content-Type".to_string(), "application/json".to_string());

    PlatformRequest {
        url: format!("{}/{}", SLACK_API_BASE, method),
        method: "POST".to_string(),
        headers,
        body: serde_json::to_vec(&payload).unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slack_post_message() {
        let req = slack_post_message("xoxb-token", "C123", "hello", None);
        assert_eq!(req.url, "https://slack.com/api/chat.postMessage");
        assert_eq!(req.method, "POST");
        assert_eq!(
            req.headers.get("Authorization").unwrap(),
            "Bearer xoxb-token"
        );

        let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
        assert_eq!(body["channel"], "C123");
        assert_eq!(body["text"], "hello");
        assert!(body.get("thread_ts").is_none());
    }

    #[test]
    fn test_slack_post_message_in_thread() {
        let req = slack_post_message("xoxb-token", "C123", "reply", Some("1234.5678"));
        let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
        assert_eq!(body["thread_ts"], "1234.5678");
    }

    #[test]
    fn test_slack_update_message() {
        let req = slack_update_message("xoxb-token", "C123", "1234.5678", "updated text");
        assert_eq!(req.url, "https://slack.com/api/chat.update");

        let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
        assert_eq!(body["ts"], "1234.5678");
        assert_eq!(body["text"], "updated text");
    }

    #[test]
    fn test_slack_post_ephemeral() {
        let req = slack_post_ephemeral("xoxb-token", "C123", "U456", "only you see this", None);
        assert_eq!(req.url, "https://slack.com/api/chat.postEphemeral");

        let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
        assert_eq!(body["user"], "U456");
    }

    #[test]
    fn test_slack_add_reaction() {
        let req = slack_add_reaction("xoxb-token", "C123", "1234.5678", "thumbsup");
        assert_eq!(req.url, "https://slack.com/api/reactions.add");

        let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
        assert_eq!(body["name"], "thumbsup");
    }
}
