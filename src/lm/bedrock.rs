use std::env;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::stream;
use hmac::{Hmac, Mac};
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::error::{Result as SdkResult, SdkError};

use super::interface::{
    generate as generate_via_model, stream as stream_via_model, GenerateRequest, GenerateResponse,
    LanguageModel, MessageRole, ResponseFormat, StreamChunk, StreamHandle, StreamRequest,
    TokenUsage, ToolChoice, ToolDefinition,
};

const SERVICE_NAME: &str = "bedrock";
const MODEL_PREFIX: &str = "bedrock";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_TOKENS: u32 = 1024;
const ANTHROPIC_VERSION: &str = "bedrock-2023-05-31";

#[derive(Clone, Debug)]
pub struct AwsCredentials {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
}

#[derive(Clone, Debug)]
pub struct BedrockConfig {
    pub credentials: AwsCredentials,
    pub default_region: Option<String>,
    pub timeout: Duration,
}

impl BedrockConfig {
    pub fn from_env() -> SdkResult<Self> {
        let access_key = env::var("AWS_ACCESS_KEY_ID").map_err(|_| {
            SdkError::Configuration {
                message: "AWS_ACCESS_KEY_ID must be set for Bedrock requests".to_string(),
                field: Some("AWS_ACCESS_KEY_ID".to_string()),
            }
        })?;

        let secret_key = env::var("AWS_SECRET_ACCESS_KEY").map_err(|_| {
            SdkError::Configuration {
                message: "AWS_SECRET_ACCESS_KEY must be set for Bedrock requests".to_string(),
                field: Some("AWS_SECRET_ACCESS_KEY".to_string()),
            }
        })?;

        let session_token = env::var("AWS_SESSION_TOKEN").ok();
        let default_region = env::var("AWS_REGION").ok();

        Ok(Self {
            credentials: AwsCredentials {
                access_key,
                secret_key,
                session_token,
            },
            default_region,
            timeout: DEFAULT_TIMEOUT,
        })
    }
}

#[derive(Clone)]
pub struct BedrockProvider {
    http: Client,
    config: BedrockConfig,
}

impl BedrockProvider {
    pub fn new(config: BedrockConfig) -> SdkResult<Self> {
        let http = Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|err| SdkError::Other(anyhow!("failed to construct HTTP client: {err}")))?;

        Ok(Self { http, config })
    }

    pub fn from_env() -> SdkResult<Self> {
        let config = BedrockConfig::from_env()?;
        Self::new(config)
    }

    pub async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse> {
        generate_via_model(self, request).await
    }

    pub async fn stream(&self, request: StreamRequest) -> SdkResult<StreamHandle> {
        stream_via_model(self, request).await
    }
}

#[async_trait]
impl LanguageModel for BedrockProvider {
    async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse> {
        validate_request(&request)?;
        let (region, model_id) =
            extract_region_and_model(&request.model, self.config.default_region.as_deref())?;

        let url = format!("https://bedrock-runtime.{region}.amazonaws.com/model/{model_id}/invoke");

        let payload = build_bedrock_payload(&request, &model_id)?;
        let body = serde_json::to_vec(&payload).map_err(|err| {
            SdkError::Other(anyhow!("failed to serialize Bedrock payload: {err}"))
        })?;

        let response = self
            .invoke_signed(&url, region, &body)
            .await?
            .json::<BedrockAnthropicResponse>()
            .await
            .map_err(|err| SdkError::Other(anyhow!("failed to parse Bedrock response: {err}")))?;

        response.into_generate_response(request.config.response_format.clone())
    }

    async fn stream(&self, request: StreamRequest) -> SdkResult<StreamHandle> {
        // Bedrock's streaming APIs require a different endpoint; for now, fall back to a
        // single-response stream.
        let response = self.generate(request).await?;
        let stream = stream::once(async move { Ok(StreamChunk::Completed(response)) });
        Ok(StreamHandle::new(Box::pin(stream)))
    }
}

fn validate_request(request: &GenerateRequest) -> SdkResult<()> {
    if request.messages.is_empty() {
        return Err(SdkError::Configuration {
            message: "at least one message is required for Bedrock requests".to_string(),
            field: None,
        });
    }
    if !request.tools.is_empty() || request.tool_choice.is_some() {
        return Err(SdkError::Configuration {
            message: "Bedrock provider does not yet support tool calls in this SDK".to_string(),
            field: None,
        });
    }
    Ok(())
}

fn extract_region_and_model<'a>(
    model: &'a str,
    default_region: Option<&'a str>,
) -> SdkResult<(&'a str, &'a str)> {
    let trimmed = model.trim();
    let rest = if let Some((prefix, rest)) = trimmed.split_once('/') {
        if prefix != MODEL_PREFIX {
            return Err(SdkError::Configuration {
                message: format!("Bedrock provider expects model ids prefixed with `{MODEL_PREFIX}/`; got `{prefix}`"),
                field: Some("model".to_string()),
            });
        }
        rest
    } else {
        return Err(SdkError::Configuration {
            message: format!("Bedrock model ids must be prefixed with `{MODEL_PREFIX}/`"),
            field: Some("model".to_string()),
        });
    };

    if let Some((region, model_id)) = rest.split_once('/') {
        if region.trim().is_empty() || model_id.trim().is_empty() {
            return Err(SdkError::Configuration {
                message: "Bedrock model id must be in the form `bedrock/<region>/<model>`".to_string(),
                field: Some("model".to_string()),
            });
        }
        Ok((region.trim(), model_id.trim()))
    } else if let Some(region) = default_region {
        Ok((region, rest.trim()))
    } else {
        Err(SdkError::Configuration {
            message: "Bedrock model id must include a region (bedrock/<region>/<model>)".to_string(),
            field: Some("model".to_string()),
        })
    }
}

fn build_bedrock_payload(request: &GenerateRequest, model_id: &str) -> SdkResult<Value> {
    if model_id.starts_with("anthropic.") {
        build_anthropic_payload(request)
    } else {
        Err(SdkError::Configuration {
            message: format!("Bedrock provider currently supports anthropic models only; got `{model_id}`"),
            field: Some("model".to_string()),
        })
    }
}

fn build_anthropic_payload(request: &GenerateRequest) -> SdkResult<Value> {
    let mut messages = Vec::new();

    for message in &request.messages {
        let role = match message.role {
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
            MessageRole::System => {
                // System messages handled separately
                continue;
            }
        };

        messages.push(json!({
            "role": role,
            "content": [
                {"type": "text", "text": message.content}
            ]
        }));
    }

    if messages.is_empty() {
        return Err(SdkError::Configuration {
            message: "Bedrock anthropic requests require at least one user or assistant message".to_string(),
            field: None,
        });
    }

    let max_tokens = request
        .config
        .max_output_tokens
        .unwrap_or(DEFAULT_MAX_TOKENS)
        .max(1);

    let mut payload = json!({
        "anthropic_version": ANTHROPIC_VERSION,
        "messages": messages,
        "max_tokens": max_tokens,
    });

    let mut system_blocks = Vec::new();

    if let Some(system_prompt) = &request.system_prompt {
        system_blocks.push(json!({"type": "text", "text": system_prompt}));
    }

    if let Some(instruction) = response_format_instruction(&request.config.response_format) {
        system_blocks.push(json!({"type": "text", "text": instruction}));
    }

    if !system_blocks.is_empty() {
        payload["system"] = json!(system_blocks);
    }

    let tools = convert_tools(&request.tools);
    if !tools.is_empty() {
        payload["tools"] = json!(tools);
    }

    if let Some(choice) = convert_tool_choice(request.tool_choice.as_ref()) {
        payload["tool_choice"] = choice;
    }

    if let Some(temp) = request.config.temperature {
        payload["temperature"] = json!(temp);
    }

    if let Some(top_p) = request.config.top_p {
        payload["top_p"] = json!(top_p);
    }

    Ok(payload)
}

impl BedrockProvider {
    async fn invoke_signed(
        &self,
        url: &str,
        region: &str,
        body: &[u8],
    ) -> SdkResult<reqwest::Response> {
        let url = Url::parse(url)
            .map_err(|err| SdkError::Other(anyhow!("invalid Bedrock URL: {err}")))?;
        // Take an owned copy of the host string so it doesn't borrow from `url`.
        let host = url
            .host_str()
            .ok_or_else(|| SdkError::Configuration {
                message: "Bedrock URL missing host".to_string(),
                field: Some("url".to_string()),
            })?
            .to_string();

        let timestamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = timestamp[..8].to_string();
        let payload_hash = hex::encode(Sha256::digest(body));

        let mut headers = vec![
            ("content-type".to_string(), "application/json".to_string()),
            ("host".to_string(), host.to_string()),
            ("x-amz-content-sha256".to_string(), payload_hash.clone()),
            ("x-amz-date".to_string(), timestamp.clone()),
        ];

        if let Some(token) = &self.config.credentials.session_token {
            headers.push(("x-amz-security-token".to_string(), token.clone()));
        }

        headers.sort_by(|a, b| a.0.cmp(&b.0));

        let canonical_headers = headers
            .iter()
            .map(|(k, v)| format!("{}:{}\n", k, v.trim()))
            .collect::<String>();
        let signed_headers = headers
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>()
            .join(";");

        let canonical_request = format!(
            "POST\n{}\n{}\n{}\n{}\n{}",
            url.path(),
            url.query().unwrap_or(""),
            canonical_headers,
            signed_headers,
            payload_hash,
        );

        let canonical_request_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));

        let credential_scope = format!("{}/{}/{}/aws4_request", date_stamp, region, SERVICE_NAME);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            timestamp, credential_scope, canonical_request_hash
        );

        let signing_key = derive_signing_key(
            &self.config.credentials.secret_key,
            &date_stamp,
            region,
            SERVICE_NAME,
        );
        let signature = hex::encode(hmac_sha256(&signing_key, &string_to_sign));

        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.config.credentials.access_key, credential_scope, signed_headers, signature
        );

        let url_string = url.to_string();
        let mut request_builder = self.http.post(url_string).body(body.to_vec());
        request_builder = request_builder
            .header("Content-Type", "application/json")
            .header("Host", host)
            .header("x-amz-date", &timestamp)
            .header("x-amz-content-sha256", &payload_hash)
            .header("Authorization", authorization);

        if let Some(token) = &self.config.credentials.session_token {
            request_builder = request_builder.header("x-amz-security-token", token);
        }

        request_builder
            .send()
            .await
            .map_err(|err| SdkError::Other(anyhow!("Bedrock request failed: {err}")))
    }
}

fn derive_signing_key(secret_key: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_secret = format!("AWS4{}", secret_key);
    let k_date = hmac_sha256(k_secret.as_bytes(), date);
    let k_region = hmac_sha256(&k_date, region);
    let k_service = hmac_sha256(&k_region, service);
    hmac_sha256(&k_service, "aws4_request")
}

fn hmac_sha256(key: impl AsRef<[u8]>, data: &str) -> Vec<u8> {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(key.as_ref()).expect("HMAC can take key of any size");
    mac.update(data.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

#[derive(Deserialize)]
struct BedrockAnthropicResponse {
    id: Option<String>,
    model: Option<String>,
    stop_reason: Option<String>,
    usage: Option<BedrockUsage>,
    content: Vec<BedrockContentBlock>,
}

impl BedrockAnthropicResponse {
    fn into_generate_response(
        self,
        response_format: ResponseFormat,
    ) -> SdkResult<GenerateResponse> {
        let text = self
            .content
            .into_iter()
            .filter_map(|block| block.text)
            .collect::<Vec<_>>()
            .join("");

        let object = match response_format {
            ResponseFormat::Text => None,
            ResponseFormat::Json => Some(parse_json_value(&text)?),
            ResponseFormat::JsonSchema(_) => Some(parse_json_value(&text)?),
        };

        Ok(GenerateResponse {
            id: self.id.unwrap_or_default(),
            model: self.model.unwrap_or_default(),
            created: None,
            finish_reason: self.stop_reason,
            usage: self.usage.and_then(|usage| usage.into_token_usage()),
            text,
            tool_calls: None,  // Bedrock tool calls not yet supported
            object,
            raw: None,
        })
    }
}

#[derive(Deserialize)]
struct BedrockContentBlock {
    #[serde(rename = "type")]
    #[allow(unused)]
    block_type: String,
    text: Option<String>,
}

#[derive(Deserialize)]
struct BedrockUsage {
    input_tokens: Option<u32>,
    output_tokens: Option<u32>,
}

impl BedrockUsage {
    fn into_token_usage(self) -> Option<TokenUsage> {
        let total = match (self.input_tokens, self.output_tokens) {
            (Some(input), Some(output)) => Some(input + output),
            _ => None,
        };
        Some(TokenUsage {
            prompt_tokens: self.input_tokens,
            completion_tokens: self.output_tokens,
            total_tokens: total,
        })
    }
}

fn parse_json_value(text: &str) -> SdkResult<Value> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(SdkError::Other(anyhow!(
            "expected JSON response but model returned empty content"
        )));
    }

    serde_json::from_str(trimmed)
        .map_err(|err| SdkError::Other(anyhow!("failed to parse JSON response: {err}")))
}

fn response_format_instruction(format: &ResponseFormat) -> Option<String> {
    match format {
        ResponseFormat::Text => None,
        ResponseFormat::Json => Some("Please respond with a valid JSON object.".to_string()),
        ResponseFormat::JsonSchema(schema) => {
            let schema_text = serde_json::to_string_pretty(&schema.schema)
                .unwrap_or_else(|_| schema.schema.to_string());
            Some(format!(
                "Respond with a JSON object matching the following schema (strict={}):\n{}",
                schema.strict, schema_text
            ))
        }
    }
}

fn convert_tools(tools: &[ToolDefinition]) -> Vec<Value> {
    tools
        .iter()
        .map(|tool| {
            let schema = tool.parameters.clone().unwrap_or_else(|| {
                json!({
                    "type": "object",
                    "properties": {},
                })
            });

            json!({
                "name": tool.name,
                "description": tool.description,
                "input_schema": schema,
            })
        })
        .collect()
}

fn convert_tool_choice(choice: Option<&ToolChoice>) -> Option<Value> {
    match choice {
        None => None,
        Some(ToolChoice::Auto) => Some(json!({"type": "auto"})),
        Some(ToolChoice::None) => Some(json!({"type": "none"})),
        Some(ToolChoice::Required) => Some(json!({"type": "any"})), // Bedrock uses "any" for required
        Some(ToolChoice::Tool { name }) => Some(json!({"type": "tool", "name": name})),
    }
}
