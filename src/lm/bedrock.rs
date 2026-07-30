use std::collections::{BTreeMap, HashMap};
use std::env;
use std::pin::Pin;
use std::time::Duration;

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use futures::StreamExt;
use hmac::{Hmac, Mac};
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::error::{Result as SdkResult, SdkError};

use super::http;
use super::interface::{
    generate as generate_via_model, stream as stream_via_model, ContentBlockType, GenerateRequest,
    GenerateResponse, LanguageModel, MessageRole, ResponseFormat, StreamChunk, StreamHandle,
    StreamRequest, TokenUsage, ToolCall, ToolChoice, ToolDefinition,
};

const SERVICE_NAME: &str = "bedrock";
const MODEL_PREFIX: &str = "bedrock";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_TOKENS: u32 = 1024;
const ANTHROPIC_VERSION: &str = "bedrock-2023-05-31";
const MAX_EVENT_MESSAGE_SIZE: usize = 32 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BedrockModelFamily {
    Anthropic,
    MetaLlama,
    AmazonTitan,
    Cohere,
    Mistral,
}

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
    pub retry_config: http::RetryConfig,
}

impl BedrockConfig {
    pub fn from_env() -> SdkResult<Self> {
        let access_key = env::var("AWS_ACCESS_KEY_ID").map_err(|_| SdkError::Configuration {
            message: "AWS_ACCESS_KEY_ID must be set for Bedrock requests".to_string(),
            field: Some("AWS_ACCESS_KEY_ID".to_string()),
        })?;

        let secret_key =
            env::var("AWS_SECRET_ACCESS_KEY").map_err(|_| SdkError::Configuration {
                message: "AWS_SECRET_ACCESS_KEY must be set for Bedrock requests".to_string(),
                field: Some("AWS_SECRET_ACCESS_KEY".to_string()),
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
            retry_config: http::RetryConfig::from_env(),
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
        let http = http::build_http_client(config.timeout)?;

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
        let (region, model_id) =
            extract_region_and_model(&request.model, self.config.default_region.as_deref())?;
        let family = model_family(model_id)?;
        validate_request(&request, family)?;

        let url = format!("https://bedrock-runtime.{region}.amazonaws.com/model/{model_id}/invoke");

        let payload = build_bedrock_payload(&request, model_id, family, false)?;
        let body = serde_json::to_vec(&payload).map_err(|err| {
            SdkError::Other(anyhow!("failed to serialize Bedrock payload: {err}"))
        })?;

        let response = self.invoke_signed(&url, region, &body, false).await?;
        let response = checked_response_bytes(response).await?;

        parse_bedrock_response(
            family,
            model_id,
            response.as_ref(),
            request.config.response_format.clone(),
        )
    }

    async fn stream(&self, request: StreamRequest) -> SdkResult<StreamHandle> {
        let (region, model_id) =
            extract_region_and_model(&request.model, self.config.default_region.as_deref())?;
        let family = model_family(model_id)?;
        validate_request(&request, family)?;

        let url = format!(
            "https://bedrock-runtime.{region}.amazonaws.com/model/{model_id}/invoke-with-response-stream"
        );
        let payload = build_bedrock_payload(&request, model_id, family, true)?;
        let body = serde_json::to_vec(&payload).map_err(|err| {
            SdkError::Other(anyhow!(
                "failed to serialize Bedrock streaming payload: {err}"
            ))
        })?;
        let response = self.invoke_signed(&url, region, &body, true).await?;
        let response = ensure_success(response).await?;
        let stream = build_bedrock_stream(
            response,
            family,
            model_id.to_string(),
            request.config.response_format.clone(),
        );
        Ok(StreamHandle::new(stream))
    }
}

fn validate_request(request: &GenerateRequest, family: BedrockModelFamily) -> SdkResult<()> {
    if request.messages.is_empty() {
        return Err(SdkError::Configuration {
            message: "at least one message is required for Bedrock requests".to_string(),
            field: None,
        });
    }
    if !request
        .messages
        .iter()
        .any(|message| message.role != MessageRole::System)
    {
        return Err(SdkError::Configuration {
            message: "Bedrock requests require at least one user or assistant message".to_string(),
            field: Some("messages".to_string()),
        });
    }
    if family != BedrockModelFamily::Anthropic
        && (!request.tools.is_empty() || request.tool_choice.is_some())
    {
        return Err(SdkError::Configuration {
            message: "Bedrock native tool calls are currently supported for Anthropic models only"
                .to_string(),
            field: Some("tools".to_string()),
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
                message: format!(
                    "Bedrock provider expects model ids prefixed with `{MODEL_PREFIX}/`; got `{prefix}`"
                ),
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
                message: "Bedrock model id must be in the form `bedrock/<region>/<model>`"
                    .to_string(),
                field: Some("model".to_string()),
            });
        }
        Ok((region.trim(), model_id.trim()))
    } else if let Some(region) = default_region {
        Ok((region, rest.trim()))
    } else {
        Err(SdkError::Configuration {
            message: "Bedrock model id must include a region (bedrock/<region>/<model>)"
                .to_string(),
            field: Some("model".to_string()),
        })
    }
}

fn model_family(model_id: &str) -> SdkResult<BedrockModelFamily> {
    if model_id.starts_with("anthropic.") {
        Ok(BedrockModelFamily::Anthropic)
    } else if model_id.starts_with("meta.llama") {
        Ok(BedrockModelFamily::MetaLlama)
    } else if model_id.starts_with("amazon.titan") {
        Ok(BedrockModelFamily::AmazonTitan)
    } else if model_id.starts_with("cohere.") {
        Ok(BedrockModelFamily::Cohere)
    } else if model_id.starts_with("mistral.") {
        Ok(BedrockModelFamily::Mistral)
    } else {
        Err(SdkError::Configuration {
            message: format!(
                "unsupported Bedrock model family for `{model_id}`; supported prefixes are \
                 anthropic., meta.llama, amazon.titan, cohere., and mistral."
            ),
            field: Some("model".to_string()),
        })
    }
}

fn build_bedrock_payload(
    request: &GenerateRequest,
    model_id: &str,
    family: BedrockModelFamily,
    streaming: bool,
) -> SdkResult<Value> {
    match family {
        BedrockModelFamily::Anthropic => build_anthropic_payload(request),
        BedrockModelFamily::MetaLlama => Ok(build_meta_llama_payload(request)),
        BedrockModelFamily::AmazonTitan => Ok(build_amazon_titan_payload(request)),
        BedrockModelFamily::Cohere => {
            if is_cohere_command_r(model_id) {
                Ok(build_cohere_command_r_payload(request))
            } else {
                Ok(build_cohere_payload(request, streaming))
            }
        }
        BedrockModelFamily::Mistral => {
            if is_mistral_chat(model_id) {
                Ok(build_mistral_chat_payload(request))
            } else {
                Ok(build_mistral_payload(request))
            }
        }
    }
}

fn build_anthropic_payload(request: &GenerateRequest) -> SdkResult<Value> {
    let mut messages = Vec::new();

    for message in &request.messages {
        if let Some(tool_call_id) = &message.tool_call_id {
            messages.push(json!({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": tool_call_id,
                    "content": message.content,
                }]
            }));
            continue;
        }

        let role = match message.role {
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
            MessageRole::System => {
                // System messages handled separately
                continue;
            }
        };

        let mut content = Vec::new();
        if !message.content.is_empty() {
            content.push(json!({"type": "text", "text": message.content}));
        }
        if let Some(tool_calls) = &message.tool_calls {
            for tool_call in tool_calls {
                let input =
                    serde_json::from_str(&tool_call.arguments).unwrap_or_else(|_| json!({}));
                content.push(json!({
                    "type": "tool_use",
                    "id": tool_call.id,
                    "name": tool_call.name,
                    "input": input,
                }));
            }
        }

        messages.push(json!({
            "role": role,
            "content": content,
        }));
    }

    if messages.is_empty() {
        return Err(SdkError::Configuration {
            message: "Bedrock anthropic requests require at least one user or assistant message"
                .to_string(),
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

    for instruction in system_instructions(request) {
        system_blocks.push(json!({"type": "text", "text": instruction}));
    }

    if let Some(instruction) = response_format_instruction(&request.config.response_format) {
        system_blocks.push(json!({"type": "text", "text": instruction}));
    }

    if !system_blocks.is_empty() {
        payload["system"] = json!(system_blocks);
    }

    let tools = if matches!(request.tool_choice, Some(ToolChoice::None)) {
        Vec::new()
    } else {
        convert_tools(&request.tools)
    };
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

fn build_meta_llama_payload(request: &GenerateRequest) -> Value {
    let max_tokens = request
        .config
        .max_output_tokens
        .unwrap_or(DEFAULT_MAX_TOKENS)
        .max(1);
    let mut payload = json!({
        "prompt": format_meta_llama_prompt(request),
        "max_gen_len": max_tokens,
    });
    if let Some(temperature) = request.config.temperature {
        payload["temperature"] = json!(temperature);
    }
    if let Some(top_p) = request.config.top_p {
        payload["top_p"] = json!(top_p);
    }
    payload
}

fn build_amazon_titan_payload(request: &GenerateRequest) -> Value {
    let mut generation_config = json!({
        "maxTokenCount": request
            .config
            .max_output_tokens
            .unwrap_or(DEFAULT_MAX_TOKENS),
    });
    if let Some(temperature) = request.config.temperature {
        generation_config["temperature"] = json!(temperature);
    }
    if let Some(top_p) = request.config.top_p {
        generation_config["topP"] = json!(top_p);
    }

    json!({
        "inputText": format_plain_conversation(request, "Bot"),
        "textGenerationConfig": generation_config,
    })
}

fn build_cohere_payload(request: &GenerateRequest, streaming: bool) -> Value {
    let mut payload = json!({
        "prompt": format_plain_conversation(request, "Assistant"),
        "max_tokens": request
            .config
            .max_output_tokens
            .unwrap_or(DEFAULT_MAX_TOKENS),
        "stream": streaming,
        "num_generations": 1,
    });
    if let Some(temperature) = request.config.temperature {
        payload["temperature"] = json!(temperature);
    }
    if let Some(top_p) = request.config.top_p {
        payload["p"] = json!(top_p);
    }
    payload
}

fn build_cohere_command_r_payload(request: &GenerateRequest) -> Value {
    let messages = request
        .messages
        .iter()
        .filter(|message| message.role != MessageRole::System)
        .collect::<Vec<_>>();
    let (current, history) = messages
        .split_last()
        .expect("validated Bedrock requests include a message");
    let mut payload = json!({
        "message": current.content,
        "chat_history": history
            .iter()
            .map(|message| {
                json!({
                    "role": match message.role {
                        MessageRole::Assistant => "CHATBOT",
                        MessageRole::User | MessageRole::System => "USER",
                    },
                    "message": message.content,
                })
            })
            .collect::<Vec<_>>(),
        "max_tokens": request
            .config
            .max_output_tokens
            .unwrap_or(DEFAULT_MAX_TOKENS),
    });
    let preamble = system_instructions(request).join("\n\n");
    if !preamble.is_empty() {
        payload["preamble"] = json!(preamble);
    }
    if let Some(temperature) = request.config.temperature {
        payload["temperature"] = json!(temperature);
    }
    if let Some(top_p) = request.config.top_p {
        payload["p"] = json!(top_p);
    }
    payload
}

fn build_mistral_payload(request: &GenerateRequest) -> Value {
    let mut payload = json!({
        "prompt": format_mistral_prompt(request),
        "max_tokens": request
            .config
            .max_output_tokens
            .unwrap_or(DEFAULT_MAX_TOKENS),
    });
    if let Some(temperature) = request.config.temperature {
        payload["temperature"] = json!(temperature);
    }
    if let Some(top_p) = request.config.top_p {
        payload["top_p"] = json!(top_p);
    }
    payload
}

fn build_mistral_chat_payload(request: &GenerateRequest) -> Value {
    let mut messages = Vec::new();
    let system = system_instructions(request).join("\n\n");
    if !system.is_empty() {
        messages.push(json!({"role": "system", "content": system}));
    }
    messages.extend(
        request
            .messages
            .iter()
            .filter(|message| message.role != MessageRole::System)
            .map(|message| {
                json!({
                    "role": match message.role {
                        MessageRole::User => "user",
                        MessageRole::Assistant => "assistant",
                        MessageRole::System => "system",
                    },
                    "content": message.content,
                })
            }),
    );

    let mut payload = json!({
        "messages": messages,
        "max_tokens": request
            .config
            .max_output_tokens
            .unwrap_or(DEFAULT_MAX_TOKENS),
    });
    if let Some(temperature) = request.config.temperature {
        payload["temperature"] = json!(temperature);
    }
    if let Some(top_p) = request.config.top_p {
        payload["top_p"] = json!(top_p);
    }
    payload
}

fn is_cohere_command_r(model_id: &str) -> bool {
    model_id.starts_with("cohere.command-r")
}

fn is_mistral_chat(model_id: &str) -> bool {
    model_id.contains("mistral-large-2407")
}

fn system_instructions(request: &GenerateRequest) -> Vec<String> {
    let mut instructions = Vec::new();
    if let Some(system_prompt) = &request.system_prompt {
        if !system_prompt.trim().is_empty() {
            instructions.push(system_prompt.clone());
        }
    }
    instructions.extend(
        request
            .messages
            .iter()
            .filter(|message| message.role == MessageRole::System)
            .map(|message| message.content.clone())
            .filter(|content| !content.trim().is_empty()),
    );
    if let Some(instruction) = response_format_instruction(&request.config.response_format) {
        instructions.push(instruction);
    }
    instructions
}

fn format_meta_llama_prompt(request: &GenerateRequest) -> String {
    let mut prompt = String::from("<|begin_of_text|>");
    let system = system_instructions(request).join("\n\n");
    if !system.is_empty() {
        prompt.push_str("<|start_header_id|>system<|end_header_id|>\n\n");
        prompt.push_str(&system);
        prompt.push_str("<|eot_id|>");
    }

    for message in request
        .messages
        .iter()
        .filter(|message| message.role != MessageRole::System)
    {
        let role = match message.role {
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
            MessageRole::System => continue,
        };
        prompt.push_str("<|start_header_id|>");
        prompt.push_str(role);
        prompt.push_str("<|end_header_id|>\n\n");
        prompt.push_str(&message.content);
        prompt.push_str("<|eot_id|>");
    }
    prompt.push_str("<|start_header_id|>assistant<|end_header_id|>\n\n");
    prompt
}

fn format_mistral_prompt(request: &GenerateRequest) -> String {
    let mut prompt = String::from("<s>");
    let mut system = system_instructions(request).join("\n\n");
    for message in request
        .messages
        .iter()
        .filter(|message| message.role != MessageRole::System)
    {
        match message.role {
            MessageRole::User => {
                prompt.push_str("[INST] ");
                if !system.is_empty() {
                    prompt.push_str(&system);
                    prompt.push_str("\n\n");
                    system.clear();
                }
                prompt.push_str(&message.content);
                prompt.push_str(" [/INST]");
            }
            MessageRole::Assistant => {
                prompt.push(' ');
                prompt.push_str(&message.content);
                prompt.push_str("</s>");
            }
            MessageRole::System => {}
        }
    }
    prompt
}

fn format_plain_conversation(request: &GenerateRequest, assistant_label: &str) -> String {
    let mut prompt = String::new();
    let system = system_instructions(request).join("\n\n");
    if !system.is_empty() {
        prompt.push_str("System: ");
        prompt.push_str(&system);
        prompt.push('\n');
    }
    for message in request
        .messages
        .iter()
        .filter(|message| message.role != MessageRole::System)
    {
        match message.role {
            MessageRole::User => prompt.push_str("User: "),
            MessageRole::Assistant => {
                prompt.push_str(assistant_label);
                prompt.push_str(": ");
            }
            MessageRole::System => continue,
        }
        prompt.push_str(&message.content);
        prompt.push('\n');
    }
    prompt.push_str(assistant_label);
    prompt.push(':');
    prompt
}

impl BedrockProvider {
    async fn invoke_signed(
        &self,
        url: &str,
        region: &str,
        body: &[u8],
        streaming: bool,
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

        let accept = if streaming {
            "application/vnd.amazon.eventstream"
        } else {
            "application/json"
        };
        let mut headers = vec![
            ("accept".to_string(), accept.to_string()),
            ("content-type".to_string(), "application/json".to_string()),
            ("host".to_string(), host.to_string()),
            ("x-amz-content-sha256".to_string(), payload_hash.clone()),
            ("x-amz-date".to_string(), timestamp.clone()),
        ];
        if streaming {
            headers.push((
                "x-amzn-bedrock-accept".to_string(),
                "application/json".to_string(),
            ));
        }

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
            .header("Accept", accept)
            .header("Content-Type", "application/json")
            .header("Host", host)
            .header("x-amz-date", &timestamp)
            .header("x-amz-content-sha256", &payload_hash)
            .header("Authorization", authorization);
        if streaming {
            request_builder = request_builder.header("x-amzn-bedrock-accept", "application/json");
        }

        if let Some(token) = &self.config.credentials.session_token {
            request_builder = request_builder.header("x-amz-security-token", token);
        }

        request_builder
            .send()
            .await
            .map_err(|err| SdkError::Other(anyhow!("Bedrock request failed: {err}")))
    }
}

async fn ensure_success(response: reqwest::Response) -> SdkResult<reqwest::Response> {
    if response.status().is_success() {
        return Ok(response);
    }

    let status = response.status();
    let body = response
        .text()
        .await
        .unwrap_or_else(|_| "<unreadable response body>".to_string());
    Err(SdkError::Other(anyhow!(
        "Bedrock request failed with HTTP {status}: {body}"
    )))
}

async fn checked_response_bytes(response: reqwest::Response) -> SdkResult<bytes::Bytes> {
    ensure_success(response)
        .await?
        .bytes()
        .await
        .map_err(|err| SdkError::Other(anyhow!("failed to read Bedrock response: {err}")))
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
        let mut text_parts = Vec::new();
        let mut tool_calls = Vec::new();
        for block in self.content {
            match block.block_type.as_str() {
                "text" => {
                    if let Some(text) = block.text {
                        text_parts.push(text);
                    }
                }
                "tool_use" => {
                    if let (Some(id), Some(name), Some(input)) = (block.id, block.name, block.input)
                    {
                        tool_calls.push(ToolCall {
                            id,
                            name,
                            arguments: input.to_string(),
                        });
                    }
                }
                _ => {}
            }
        }
        let text = text_parts.join("");
        let object = response_object(&text, &response_format, !tool_calls.is_empty())?;

        Ok(GenerateResponse {
            id: self.id.unwrap_or_default(),
            model: self.model.unwrap_or_default(),
            created: None,
            finish_reason: self.stop_reason,
            usage: self.usage.and_then(|usage| usage.into_token_usage()),
            text,
            tool_calls: if tool_calls.is_empty() {
                None
            } else {
                Some(tool_calls)
            },
            object,
            raw: None,
            metadata: None,
        })
    }
}

#[derive(Deserialize)]
struct BedrockContentBlock {
    #[serde(rename = "type")]
    block_type: String,
    text: Option<String>,
    id: Option<String>,
    name: Option<String>,
    input: Option<Value>,
}

#[derive(Deserialize)]
struct MetaLlamaResponse {
    generation: String,
    prompt_token_count: Option<u32>,
    generation_token_count: Option<u32>,
    stop_reason: Option<String>,
}

#[derive(Deserialize)]
struct AmazonTitanResponse {
    #[serde(rename = "inputTextTokenCount")]
    input_text_token_count: Option<u32>,
    results: Vec<AmazonTitanResult>,
}

#[derive(Deserialize)]
struct AmazonTitanResult {
    #[serde(rename = "tokenCount")]
    token_count: Option<u32>,
    #[serde(rename = "outputText")]
    output_text: String,
    #[serde(rename = "completionReason")]
    completion_reason: Option<String>,
}

#[derive(Deserialize)]
struct CohereResponse {
    id: Option<String>,
    generations: Vec<CohereGeneration>,
}

#[derive(Deserialize)]
struct CohereGeneration {
    id: Option<String>,
    text: String,
    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct CohereCommandRResponse {
    response_id: Option<String>,
    generation_id: Option<String>,
    text: String,
    finish_reason: Option<String>,
    meta: Option<CohereMeta>,
}

#[derive(Deserialize)]
struct CohereMeta {
    billed_units: Option<CohereBilledUnits>,
}

#[derive(Deserialize)]
struct CohereBilledUnits {
    input_tokens: Option<u32>,
    output_tokens: Option<u32>,
}

#[derive(Deserialize)]
struct MistralResponse {
    outputs: Vec<MistralOutput>,
}

#[derive(Deserialize)]
struct MistralOutput {
    text: String,
    stop_reason: Option<String>,
}

#[derive(Deserialize)]
struct MistralChatResponse {
    choices: Vec<MistralChatChoice>,
}

#[derive(Deserialize)]
struct MistralChatChoice {
    message: MistralChatMessage,
    stop_reason: Option<String>,
}

#[derive(Deserialize)]
struct MistralChatMessage {
    content: Option<String>,
}

fn parse_bedrock_response(
    family: BedrockModelFamily,
    model_id: &str,
    body: &[u8],
    response_format: ResponseFormat,
) -> SdkResult<GenerateResponse> {
    match family {
        BedrockModelFamily::Anthropic => {
            let response: BedrockAnthropicResponse =
                serde_json::from_slice(body).map_err(|err| {
                    SdkError::Other(anyhow!("failed to parse Bedrock Anthropic response: {err}"))
                })?;
            response.into_generate_response(response_format)
        }
        BedrockModelFamily::MetaLlama => {
            let response: MetaLlamaResponse = serde_json::from_slice(body).map_err(|err| {
                SdkError::Other(anyhow!(
                    "failed to parse Bedrock Meta Llama response: {err}"
                ))
            })?;
            let usage = token_usage(response.prompt_token_count, response.generation_token_count);
            generic_generate_response(
                model_id,
                response.generation,
                response.stop_reason,
                usage,
                response_format,
            )
        }
        BedrockModelFamily::AmazonTitan => {
            let response: AmazonTitanResponse = serde_json::from_slice(body).map_err(|err| {
                SdkError::Other(anyhow!(
                    "failed to parse Bedrock Amazon Titan response: {err}"
                ))
            })?;
            let result = response.results.into_iter().next().ok_or_else(|| {
                SdkError::Other(anyhow!("Bedrock Amazon Titan response had no results"))
            })?;
            let usage = token_usage(response.input_text_token_count, result.token_count);
            generic_generate_response(
                model_id,
                result.output_text,
                result.completion_reason,
                usage,
                response_format,
            )
        }
        BedrockModelFamily::Cohere => {
            if is_cohere_command_r(model_id) {
                let response: CohereCommandRResponse =
                    serde_json::from_slice(body).map_err(|err| {
                        SdkError::Other(anyhow!(
                            "failed to parse Bedrock Cohere Command R response: {err}"
                        ))
                    })?;
                let usage = response.meta.and_then(|meta| {
                    meta.billed_units
                        .and_then(|units| token_usage(units.input_tokens, units.output_tokens))
                });
                let mut generated = generic_generate_response(
                    model_id,
                    response.text,
                    response.finish_reason,
                    usage,
                    response_format,
                )?;
                generated.id = response
                    .generation_id
                    .or(response.response_id)
                    .unwrap_or_default();
                Ok(generated)
            } else {
                let response: CohereResponse = serde_json::from_slice(body).map_err(|err| {
                    SdkError::Other(anyhow!("failed to parse Bedrock Cohere response: {err}"))
                })?;
                let generation = response.generations.into_iter().next().ok_or_else(|| {
                    SdkError::Other(anyhow!("Bedrock Cohere response had no generations"))
                })?;
                let mut generated = generic_generate_response(
                    model_id,
                    generation.text,
                    generation.finish_reason,
                    None,
                    response_format,
                )?;
                generated.id = generation.id.or(response.id).unwrap_or_default();
                Ok(generated)
            }
        }
        BedrockModelFamily::Mistral => {
            if is_mistral_chat(model_id) {
                let response: MistralChatResponse =
                    serde_json::from_slice(body).map_err(|err| {
                        SdkError::Other(anyhow!(
                            "failed to parse Bedrock Mistral chat response: {err}"
                        ))
                    })?;
                let choice = response.choices.into_iter().next().ok_or_else(|| {
                    SdkError::Other(anyhow!("Bedrock Mistral response had no choices"))
                })?;
                generic_generate_response(
                    model_id,
                    choice.message.content.unwrap_or_default(),
                    choice.stop_reason,
                    None,
                    response_format,
                )
            } else {
                let response: MistralResponse = serde_json::from_slice(body).map_err(|err| {
                    SdkError::Other(anyhow!("failed to parse Bedrock Mistral response: {err}"))
                })?;
                let output = response.outputs.into_iter().next().ok_or_else(|| {
                    SdkError::Other(anyhow!("Bedrock Mistral response had no outputs"))
                })?;
                generic_generate_response(
                    model_id,
                    output.text,
                    output.stop_reason,
                    None,
                    response_format,
                )
            }
        }
    }
}

fn generic_generate_response(
    model: &str,
    text: String,
    finish_reason: Option<String>,
    usage: Option<TokenUsage>,
    response_format: ResponseFormat,
) -> SdkResult<GenerateResponse> {
    let object = response_object(&text, &response_format, false)?;
    Ok(GenerateResponse {
        id: String::new(),
        model: model.to_string(),
        created: None,
        text,
        usage,
        finish_reason,
        tool_calls: None,
        object,
        raw: None,
        metadata: None,
    })
}

fn token_usage(prompt_tokens: Option<u32>, completion_tokens: Option<u32>) -> Option<TokenUsage> {
    if prompt_tokens.is_none() && completion_tokens.is_none() {
        return None;
    }
    Some(TokenUsage {
        prompt_tokens,
        completion_tokens,
        total_tokens: match (prompt_tokens, completion_tokens) {
            (Some(prompt), Some(completion)) => Some(prompt + completion),
            _ => None,
        },
        cached_tokens: None,
        cache_creation_tokens: None,
    })
}

fn response_object(
    text: &str,
    response_format: &ResponseFormat,
    has_tool_calls: bool,
) -> SdkResult<Option<Value>> {
    match response_format {
        ResponseFormat::Text => Ok(None),
        ResponseFormat::Json | ResponseFormat::JsonSchema(_) => {
            if text.trim().is_empty() && has_tool_calls {
                Ok(None)
            } else {
                Ok(Some(parse_json_value(text)?))
            }
        }
    }
}

fn build_bedrock_stream(
    response: reqwest::Response,
    family: BedrockModelFamily,
    model: String,
    response_format: ResponseFormat,
) -> Pin<Box<dyn futures::Stream<Item = SdkResult<StreamChunk>> + Send>> {
    let bytes_stream = response.bytes_stream();
    let stream = try_stream! {
        futures::pin_mut!(bytes_stream);
        let mut decoder = AwsEventStreamDecoder::default();
        let mut partial = BedrockPartialResponse::new(model);
        let mut aggregate = String::new();
        let mut content_block_started = false;

        while let Some(chunk) = bytes_stream.next().await {
            let chunk = chunk.map_err(|err| {
                SdkError::Other(anyhow!("error reading Bedrock event stream: {err}"))
            })?;

            for event in decoder.ingest(chunk.as_ref())? {
                let message_type = event.headers.get(":message-type").map(String::as_str);
                let event_type = event.headers.get(":event-type").map(String::as_str);
                if message_type == Some("exception") || event_type != Some("chunk") {
                    let detail = String::from_utf8_lossy(&event.payload);
                    Err(SdkError::Other(anyhow!(
                        "Bedrock stream returned {}: {detail}",
                        event_type.unwrap_or("unknown event")
                    )))?;
                }

                for text in partial.absorb_payload(family, &event.payload)? {
                    if text.is_empty() {
                        continue;
                    }
                    if !content_block_started {
                        yield StreamChunk::ContentBlockStart {
                            index: 0,
                            block_type: ContentBlockType::Text,
                        };
                        content_block_started = true;
                    }
                    aggregate.push_str(&text);
                    yield StreamChunk::Delta {
                        content: text,
                        index: 0,
                        block_type: ContentBlockType::Text,
                    };
                }
            }
        }

        decoder.finish()?;
        if content_block_started {
            yield StreamChunk::ContentBlockStop { index: 0 };
        }
        let response = partial.into_generate_response(aggregate, response_format)?;
        yield StreamChunk::Completed(response);
    };

    Box::pin(stream)
}

#[derive(Default)]
struct AwsEventStreamDecoder {
    buffer: Vec<u8>,
}

struct AwsEventStreamMessage {
    headers: HashMap<String, String>,
    payload: Vec<u8>,
}

impl AwsEventStreamDecoder {
    fn ingest(&mut self, chunk: &[u8]) -> SdkResult<Vec<AwsEventStreamMessage>> {
        self.buffer.extend_from_slice(chunk);
        let mut messages = Vec::new();

        loop {
            if self.buffer.len() < 12 {
                break;
            }

            let total_len = read_be_u32(&self.buffer[0..4]) as usize;
            let headers_len = read_be_u32(&self.buffer[4..8]) as usize;
            if !(16..=MAX_EVENT_MESSAGE_SIZE).contains(&total_len) {
                return Err(SdkError::Other(anyhow!(
                    "invalid AWS event-stream message length {total_len}"
                )));
            }
            if headers_len > total_len - 16 {
                return Err(SdkError::Other(anyhow!(
                    "invalid AWS event-stream headers length {headers_len}"
                )));
            }
            if self.buffer.len() < total_len {
                break;
            }

            let message = &self.buffer[..total_len];
            let expected_prelude_crc = read_be_u32(&message[8..12]);
            let actual_prelude_crc = crc32fast::hash(&message[..8]);
            if expected_prelude_crc != actual_prelude_crc {
                return Err(SdkError::Other(anyhow!(
                    "AWS event-stream prelude CRC mismatch"
                )));
            }

            let expected_message_crc = read_be_u32(&message[total_len - 4..total_len]);
            let actual_message_crc = crc32fast::hash(&message[..total_len - 4]);
            if expected_message_crc != actual_message_crc {
                return Err(SdkError::Other(anyhow!(
                    "AWS event-stream message CRC mismatch"
                )));
            }

            let headers = parse_event_headers(&message[12..12 + headers_len])?;
            let payload = message[12 + headers_len..total_len - 4].to_vec();
            messages.push(AwsEventStreamMessage { headers, payload });
            self.buffer.drain(..total_len);
        }

        Ok(messages)
    }

    fn finish(&self) -> SdkResult<()> {
        if self.buffer.is_empty() {
            Ok(())
        } else {
            Err(SdkError::Other(anyhow!(
                "Bedrock event stream ended with {} truncated bytes",
                self.buffer.len()
            )))
        }
    }
}

fn parse_event_headers(bytes: &[u8]) -> SdkResult<HashMap<String, String>> {
    let mut headers = HashMap::new();
    let mut offset = 0;
    while offset < bytes.len() {
        let name_len = bytes[offset] as usize;
        offset += 1;
        if offset + name_len + 1 > bytes.len() {
            return Err(SdkError::Other(anyhow!(
                "truncated AWS event-stream header name"
            )));
        }
        let name = std::str::from_utf8(&bytes[offset..offset + name_len])
            .map_err(|err| SdkError::Other(anyhow!("invalid event header name: {err}")))?
            .to_string();
        offset += name_len;
        let value_type = bytes[offset];
        offset += 1;

        let value = match value_type {
            0 | 1 => None,
            2 => {
                take_header_bytes(bytes, &mut offset, 1)?;
                None
            }
            3 => {
                take_header_bytes(bytes, &mut offset, 2)?;
                None
            }
            4 => {
                take_header_bytes(bytes, &mut offset, 4)?;
                None
            }
            5 | 8 => {
                take_header_bytes(bytes, &mut offset, 8)?;
                None
            }
            6 | 7 => {
                let length_bytes = take_header_bytes(bytes, &mut offset, 2)?;
                let value_len = u16::from_be_bytes([length_bytes[0], length_bytes[1]]) as usize;
                let value_bytes = take_header_bytes(bytes, &mut offset, value_len)?;
                if value_type == 7 {
                    Some(
                        std::str::from_utf8(value_bytes)
                            .map_err(|err| {
                                SdkError::Other(anyhow!("invalid event header value: {err}"))
                            })?
                            .to_string(),
                    )
                } else {
                    None
                }
            }
            9 => {
                take_header_bytes(bytes, &mut offset, 16)?;
                None
            }
            other => {
                return Err(SdkError::Other(anyhow!(
                    "unsupported AWS event-stream header type {other}"
                )));
            }
        };
        if let Some(value) = value {
            headers.insert(name, value);
        }
    }
    Ok(headers)
}

fn take_header_bytes<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> SdkResult<&'a [u8]> {
    if *offset + len > bytes.len() {
        return Err(SdkError::Other(anyhow!(
            "truncated AWS event-stream header value"
        )));
    }
    let value = &bytes[*offset..*offset + len];
    *offset += len;
    Ok(value)
}

fn read_be_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

#[derive(Default)]
struct PartialBedrockToolCall {
    id: Option<String>,
    name: Option<String>,
    initial_input: Option<Value>,
    arguments: String,
}

struct BedrockPartialResponse {
    id: Option<String>,
    model: String,
    finish_reason: Option<String>,
    prompt_tokens: Option<u32>,
    completion_tokens: Option<u32>,
    cache_creation_tokens: Option<u32>,
    cached_tokens: Option<u32>,
    tool_calls: BTreeMap<u32, PartialBedrockToolCall>,
}

impl BedrockPartialResponse {
    fn new(model: String) -> Self {
        Self {
            id: None,
            model,
            finish_reason: None,
            prompt_tokens: None,
            completion_tokens: None,
            cache_creation_tokens: None,
            cached_tokens: None,
            tool_calls: BTreeMap::new(),
        }
    }

    fn absorb_payload(
        &mut self,
        family: BedrockModelFamily,
        payload: &[u8],
    ) -> SdkResult<Vec<String>> {
        let value: Value = serde_json::from_slice(payload).map_err(|err| {
            SdkError::Other(anyhow!("failed to parse Bedrock stream payload: {err}"))
        })?;
        let mut text = Vec::new();

        match family {
            BedrockModelFamily::Anthropic => self.absorb_anthropic_event(&value, &mut text),
            BedrockModelFamily::MetaLlama => {
                push_json_text(&value, "generation", &mut text);
                self.prompt_tokens =
                    json_u32(value.get("prompt_token_count")).or(self.prompt_tokens);
                self.completion_tokens =
                    json_u32(value.get("generation_token_count")).or(self.completion_tokens);
                self.finish_reason =
                    json_string(value.get("stop_reason")).or(self.finish_reason.take());
            }
            BedrockModelFamily::AmazonTitan => {
                push_json_text(&value, "outputText", &mut text);
                self.prompt_tokens =
                    json_u32(value.get("inputTextTokenCount")).or(self.prompt_tokens);
                self.completion_tokens =
                    json_u32(value.get("totalOutputTextTokenCount")).or(self.completion_tokens);
                self.finish_reason =
                    json_string(value.get("completionReason")).or(self.finish_reason.take());
            }
            BedrockModelFamily::Cohere => {
                if let Some(generation) = value
                    .get("generations")
                    .and_then(Value::as_array)
                    .and_then(|items| items.first())
                {
                    push_json_text(generation, "text", &mut text);
                    self.id = json_string(generation.get("id"))
                        .or_else(|| json_string(value.get("id")))
                        .or(self.id.take());
                    self.finish_reason =
                        json_string(generation.get("finish_reason")).or(self.finish_reason.take());
                } else {
                    push_json_text(&value, "text", &mut text);
                    self.id = json_string(value.get("generation_id"))
                        .or_else(|| json_string(value.get("response_id")))
                        .or(self.id.take());
                    self.finish_reason =
                        json_string(value.get("finish_reason")).or(self.finish_reason.take());
                    if let Some(units) = value.get("meta").and_then(|meta| meta.get("billed_units"))
                    {
                        self.prompt_tokens =
                            json_u32(units.get("input_tokens")).or(self.prompt_tokens);
                        self.completion_tokens =
                            json_u32(units.get("output_tokens")).or(self.completion_tokens);
                    }
                }
            }
            BedrockModelFamily::Mistral => {
                if let Some(output) = value
                    .get("outputs")
                    .and_then(Value::as_array)
                    .and_then(|items| items.first())
                {
                    push_json_text(output, "text", &mut text);
                    self.finish_reason =
                        json_string(output.get("stop_reason")).or(self.finish_reason.take());
                } else if let Some(choice) = value
                    .get("choices")
                    .and_then(Value::as_array)
                    .and_then(|items| items.first())
                {
                    if let Some(delta) = choice.get("delta").or_else(|| choice.get("message")) {
                        push_json_text(delta, "content", &mut text);
                    }
                    self.finish_reason =
                        json_string(choice.get("stop_reason")).or(self.finish_reason.take());
                }
            }
        }

        Ok(text)
    }

    fn absorb_anthropic_event(&mut self, value: &Value, text: &mut Vec<String>) {
        match value.get("type").and_then(Value::as_str) {
            Some("message_start") => {
                if let Some(message) = value.get("message") {
                    self.id = json_string(message.get("id")).or(self.id.take());
                    if let Some(model) = json_string(message.get("model")) {
                        self.model = model;
                    }
                    if let Some(usage) = message.get("usage") {
                        self.update_anthropic_usage(usage);
                    }
                }
            }
            Some("content_block_start") => {
                let index = json_u32(value.get("index")).unwrap_or(0);
                if let Some(block) = value.get("content_block") {
                    match block.get("type").and_then(Value::as_str) {
                        Some("text") => push_json_text(block, "text", text),
                        Some("tool_use") => {
                            let tool_call = self.tool_calls.entry(index).or_default();
                            tool_call.id = json_string(block.get("id"));
                            tool_call.name = json_string(block.get("name"));
                            tool_call.initial_input = block.get("input").cloned();
                        }
                        _ => {}
                    }
                }
            }
            Some("content_block_delta") => {
                let index = json_u32(value.get("index")).unwrap_or(0);
                if let Some(delta) = value.get("delta") {
                    match delta.get("type").and_then(Value::as_str) {
                        Some("text_delta") => push_json_text(delta, "text", text),
                        Some("input_json_delta") => {
                            if let Some(fragment) =
                                delta.get("partial_json").and_then(Value::as_str)
                            {
                                self.tool_calls
                                    .entry(index)
                                    .or_default()
                                    .arguments
                                    .push_str(fragment);
                            }
                        }
                        _ => {}
                    }
                }
            }
            Some("message_delta") => {
                if let Some(delta) = value.get("delta") {
                    self.finish_reason =
                        json_string(delta.get("stop_reason")).or(self.finish_reason.take());
                }
                if let Some(usage) = value.get("usage") {
                    self.update_anthropic_usage(usage);
                }
            }
            _ => {}
        }
    }

    fn update_anthropic_usage(&mut self, usage: &Value) {
        self.prompt_tokens = json_u32(usage.get("input_tokens")).or(self.prompt_tokens);
        self.completion_tokens = json_u32(usage.get("output_tokens")).or(self.completion_tokens);
        self.cache_creation_tokens =
            json_u32(usage.get("cache_creation_input_tokens")).or(self.cache_creation_tokens);
        self.cached_tokens = json_u32(usage.get("cache_read_input_tokens")).or(self.cached_tokens);
    }

    fn into_generate_response(
        self,
        text: String,
        response_format: ResponseFormat,
    ) -> SdkResult<GenerateResponse> {
        let tool_calls = self
            .tool_calls
            .into_values()
            .filter_map(|partial| {
                let id = partial.id?;
                let name = partial.name?;
                let arguments = if partial.arguments.is_empty() {
                    partial
                        .initial_input
                        .unwrap_or_else(|| json!({}))
                        .to_string()
                } else {
                    partial.arguments
                };
                Some(ToolCall {
                    id,
                    name,
                    arguments,
                })
            })
            .collect::<Vec<_>>();
        let object = response_object(&text, &response_format, !tool_calls.is_empty())?;
        let prompt_tokens = self.prompt_tokens.map(|input| {
            input + self.cache_creation_tokens.unwrap_or(0) + self.cached_tokens.unwrap_or(0)
        });
        let usage = if prompt_tokens.is_none() && self.completion_tokens.is_none() {
            None
        } else {
            Some(TokenUsage {
                prompt_tokens,
                completion_tokens: self.completion_tokens,
                total_tokens: match (prompt_tokens, self.completion_tokens) {
                    (Some(prompt), Some(completion)) => Some(prompt + completion),
                    _ => None,
                },
                cached_tokens: self.cached_tokens,
                cache_creation_tokens: self.cache_creation_tokens,
            })
        };

        Ok(GenerateResponse {
            id: self.id.unwrap_or_default(),
            model: self.model,
            created: None,
            text,
            usage,
            finish_reason: self.finish_reason,
            tool_calls: if tool_calls.is_empty() {
                None
            } else {
                Some(tool_calls)
            },
            object,
            raw: None,
            metadata: None,
        })
    }
}

fn json_u32(value: Option<&Value>) -> Option<u32> {
    value
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
}

fn json_string(value: Option<&Value>) -> Option<String> {
    value.and_then(Value::as_str).map(ToString::to_string)
}

fn push_json_text(value: &Value, field: &str, output: &mut Vec<String>) {
    if let Some(text) = value.get(field).and_then(Value::as_str) {
        if !text.is_empty() {
            output.push(text.to_string());
        }
    }
}

#[derive(Deserialize)]
struct BedrockUsage {
    input_tokens: Option<u32>,
    output_tokens: Option<u32>,
    #[serde(default)]
    cache_creation_input_tokens: Option<u32>,
    #[serde(default)]
    cache_read_input_tokens: Option<u32>,
}

impl BedrockUsage {
    fn into_token_usage(self) -> Option<TokenUsage> {
        let cache_creation = self.cache_creation_input_tokens;
        let cache_read = self.cache_read_input_tokens;
        // Bedrock (Anthropic invoke format) reports `input_tokens` excluding
        // cache reads and writes. Normalize to the OpenAI convention where
        // `prompt_tokens` counts all input tokens, keeping `cached_tokens` a
        // subset of `prompt_tokens`.
        let prompt_tokens = self
            .input_tokens
            .map(|input| input + cache_creation.unwrap_or(0) + cache_read.unwrap_or(0));
        let total = match (prompt_tokens, self.output_tokens) {
            (Some(prompt), Some(output)) => Some(prompt + output),
            _ => None,
        };
        Some(TokenUsage {
            prompt_tokens,
            completion_tokens: self.output_tokens,
            total_tokens: total,
            cached_tokens: cache_read,
            cache_creation_tokens: cache_creation,
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

            let mut value = json!({
                "name": tool.name,
                "input_schema": schema,
            });
            if let Some(description) = &tool.description {
                value["description"] = json!(description);
            }
            value
        })
        .collect()
}

fn convert_tool_choice(choice: Option<&ToolChoice>) -> Option<Value> {
    match choice {
        None => None,
        Some(ToolChoice::Auto) => Some(json!({"type": "auto"})),
        // Bedrock's Anthropic Messages schema accepts auto, any, and tool.
        // Omitting tool_choice preserves the default when callers disable tools.
        Some(ToolChoice::None) => None,
        Some(ToolChoice::Required) => Some(json!({"type": "any"})), // Bedrock uses "any" for required
        Some(ToolChoice::Tool { name }) => Some(json!({"type": "tool", "name": name})),
    }
}

#[cfg(test)]
mod tests {
    use super::super::interface::Message;
    use super::*;

    fn request() -> GenerateRequest {
        GenerateRequest::new("bedrock/us-west-2/anthropic.claude-3-5-sonnet-20241022-v2:0")
            .system_prompt("Be concise.")
            .user_message("Hello")
            .configure(|config| {
                config.temperature = Some(0.2);
                config.top_p = Some(0.8);
                config.max_output_tokens = Some(256);
            })
    }

    #[test]
    fn routes_supported_model_families() {
        assert_eq!(
            model_family("anthropic.claude-3-sonnet-v1:0").unwrap(),
            BedrockModelFamily::Anthropic
        );
        assert_eq!(
            model_family("meta.llama3-70b-instruct-v1:0").unwrap(),
            BedrockModelFamily::MetaLlama
        );
        assert_eq!(
            model_family("amazon.titan-text-express-v1").unwrap(),
            BedrockModelFamily::AmazonTitan
        );
        assert_eq!(
            model_family("cohere.command-text-v14").unwrap(),
            BedrockModelFamily::Cohere
        );
        assert_eq!(
            model_family("mistral.mistral-7b-instruct-v0:2").unwrap(),
            BedrockModelFamily::Mistral
        );
        assert!(model_family("ai21.j2-ultra-v1").is_err());
    }

    #[test]
    fn builds_native_payloads_for_each_model_family() {
        let request = request();

        let llama = build_bedrock_payload(
            &request,
            "meta.llama3-70b-instruct-v1:0",
            BedrockModelFamily::MetaLlama,
            false,
        )
        .unwrap();
        assert_eq!(llama["max_gen_len"], 256);
        assert!(llama["prompt"]
            .as_str()
            .unwrap()
            .contains("<|start_header_id|>user<|end_header_id|>"));

        let titan = build_bedrock_payload(
            &request,
            "amazon.titan-text-express-v1",
            BedrockModelFamily::AmazonTitan,
            false,
        )
        .unwrap();
        assert_eq!(titan["textGenerationConfig"]["maxTokenCount"], 256);
        assert!(titan["inputText"].as_str().unwrap().contains("User: Hello"));

        let cohere = build_bedrock_payload(
            &request,
            "cohere.command-text-v14",
            BedrockModelFamily::Cohere,
            true,
        )
        .unwrap();
        assert_eq!(cohere["max_tokens"], 256);
        assert_eq!(cohere["stream"], true);
        assert!((cohere["p"].as_f64().unwrap() - 0.8).abs() < 1e-6);

        let cohere_command_r = build_bedrock_payload(
            &request,
            "cohere.command-r-v1:0",
            BedrockModelFamily::Cohere,
            true,
        )
        .unwrap();
        assert_eq!(cohere_command_r["message"], "Hello");
        assert_eq!(cohere_command_r["preamble"], "Be concise.");
        assert!(cohere_command_r.get("stream").is_none());

        let mistral = build_bedrock_payload(
            &request,
            "mistral.mistral-7b-instruct-v0:2",
            BedrockModelFamily::Mistral,
            false,
        )
        .unwrap();
        assert_eq!(mistral["max_tokens"], 256);
        assert!(mistral["prompt"].as_str().unwrap().contains("[INST]"));

        let mistral_chat = build_bedrock_payload(
            &request,
            "mistral.mistral-large-2407-v1:0",
            BedrockModelFamily::Mistral,
            false,
        )
        .unwrap();
        assert_eq!(mistral_chat["messages"][0]["role"], "system");
        assert_eq!(mistral_chat["messages"][1]["role"], "user");
        assert!(mistral_chat.get("prompt").is_none());
    }

    #[test]
    fn anthropic_payload_preserves_tool_calls_and_results() {
        let tool_call = ToolCall {
            id: "toolu_123".to_string(),
            name: "lookup_weather".to_string(),
            arguments: r#"{"city":"Seattle"}"#.to_string(),
        };
        let request = request()
            .add_tool(
                ToolDefinition::new("lookup_weather")
                    .description("Look up weather")
                    .parameters(json!({
                        "type": "object",
                        "properties": {"city": {"type": "string"}}
                    })),
            )
            .tool_choice(Some(ToolChoice::Auto))
            .message(Message::assistant_with_tool_calls("", vec![tool_call]))
            .message(Message::tool_result("toolu_123", r#"{"temperature":62}"#));

        validate_request(&request, BedrockModelFamily::Anthropic).unwrap();
        let payload = build_anthropic_payload(&request).unwrap();

        assert_eq!(payload["tools"][0]["name"], "lookup_weather");
        assert_eq!(payload["tool_choice"]["type"], "auto");
        assert_eq!(payload["messages"][1]["content"][0]["type"], "tool_use");
        assert_eq!(
            payload["messages"][2]["content"][0]["tool_use_id"],
            "toolu_123"
        );
    }

    #[test]
    fn non_anthropic_models_reject_tool_requests() {
        let request = request().add_tool(ToolDefinition::new("lookup_weather"));

        assert!(validate_request(&request, BedrockModelFamily::MetaLlama).is_err());
        assert!(validate_request(&request, BedrockModelFamily::AmazonTitan).is_err());
        assert!(validate_request(&request, BedrockModelFamily::Cohere).is_err());
        assert!(validate_request(&request, BedrockModelFamily::Mistral).is_err());
    }

    #[test]
    fn parses_anthropic_tool_call_response() {
        let body = json!({
            "id": "msg_bdrk_1",
            "model": "claude-3-sonnet",
            "stop_reason": "tool_use",
            "usage": {"input_tokens": 20, "output_tokens": 8},
            "content": [{
                "type": "tool_use",
                "id": "toolu_bdrk_1",
                "name": "lookup_weather",
                "input": {"city": "Seattle"}
            }]
        });

        let response = parse_bedrock_response(
            BedrockModelFamily::Anthropic,
            "anthropic.claude-3-sonnet-v1:0",
            &serde_json::to_vec(&body).unwrap(),
            ResponseFormat::Text,
        )
        .unwrap();
        let tool_calls = response.tool_calls.unwrap();

        assert_eq!(response.finish_reason.as_deref(), Some("tool_use"));
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].id, "toolu_bdrk_1");
        assert_eq!(tool_calls[0].name, "lookup_weather");
        assert_eq!(tool_calls[0].arguments, r#"{"city":"Seattle"}"#);
    }

    #[test]
    fn parses_native_responses_for_each_model_family() {
        let cases = [
            (
                BedrockModelFamily::MetaLlama,
                "meta.llama3-70b-instruct-v1:0",
                json!({
                    "generation": "Llama answer",
                    "prompt_token_count": 4,
                    "generation_token_count": 2,
                    "stop_reason": "stop"
                }),
                "Llama answer",
            ),
            (
                BedrockModelFamily::AmazonTitan,
                "amazon.titan-text-express-v1",
                json!({
                    "inputTextTokenCount": 4,
                    "results": [{
                        "tokenCount": 2,
                        "outputText": "Titan answer",
                        "completionReason": "FINISHED"
                    }]
                }),
                "Titan answer",
            ),
            (
                BedrockModelFamily::Cohere,
                "cohere.command-text-v14",
                json!({
                    "id": "cohere-request",
                    "generations": [{
                        "id": "cohere-generation",
                        "text": "Cohere answer",
                        "finish_reason": "COMPLETE"
                    }]
                }),
                "Cohere answer",
            ),
            (
                BedrockModelFamily::Cohere,
                "cohere.command-r-v1:0",
                json!({
                    "response_id": "cohere-response",
                    "generation_id": "cohere-generation",
                    "text": "Cohere Command R answer",
                    "finish_reason": "COMPLETE",
                    "meta": {
                        "billed_units": {
                            "input_tokens": 4,
                            "output_tokens": 2
                        }
                    }
                }),
                "Cohere Command R answer",
            ),
            (
                BedrockModelFamily::Mistral,
                "mistral.mistral-7b-instruct-v0:2",
                json!({
                    "outputs": [{
                        "text": "Mistral answer",
                        "stop_reason": "stop"
                    }]
                }),
                "Mistral answer",
            ),
            (
                BedrockModelFamily::Mistral,
                "mistral.mistral-large-2407-v1:0",
                json!({
                    "choices": [{
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": "Mistral chat answer"
                        },
                        "stop_reason": "stop"
                    }]
                }),
                "Mistral chat answer",
            ),
        ];

        for (family, model, body, expected) in cases {
            let response = parse_bedrock_response(
                family,
                model,
                &serde_json::to_vec(&body).unwrap(),
                ResponseFormat::Text,
            )
            .unwrap();
            assert_eq!(response.text, expected);
        }
    }

    #[test]
    fn event_stream_decoder_handles_split_frames_and_validates_crc() {
        let frame = encode_event_stream_message(
            &[
                (":message-type", "event"),
                (":event-type", "chunk"),
                (":content-type", "application/json"),
            ],
            br#"{"generation":"hello"}"#,
        );
        let split = frame.len() / 2;
        let mut decoder = AwsEventStreamDecoder::default();

        assert!(decoder.ingest(&frame[..split]).unwrap().is_empty());
        let messages = decoder.ingest(&frame[split..]).unwrap();
        decoder.finish().unwrap();

        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages[0].headers.get(":event-type").map(String::as_str),
            Some("chunk")
        );
        assert_eq!(messages[0].payload, br#"{"generation":"hello"}"#);

        let mut corrupt = frame;
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0xff;
        assert!(AwsEventStreamDecoder::default().ingest(&corrupt).is_err());
    }

    #[test]
    fn anthropic_stream_accumulates_text_tool_calls_and_usage() {
        let events = [
            json!({
                "type": "message_start",
                "message": {
                    "id": "msg_stream_1",
                    "model": "claude-3-sonnet",
                    "usage": {"input_tokens": 15, "output_tokens": 0}
                }
            }),
            json!({
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": "Checking "}
            }),
            json!({
                "type": "content_block_start",
                "index": 1,
                "content_block": {
                    "type": "tool_use",
                    "id": "toolu_stream_1",
                    "name": "lookup_weather",
                    "input": {}
                }
            }),
            json!({
                "type": "content_block_delta",
                "index": 1,
                "delta": {
                    "type": "input_json_delta",
                    "partial_json": "{\"city\":\"Seattle\"}"
                }
            }),
            json!({
                "type": "message_delta",
                "delta": {"stop_reason": "tool_use"},
                "usage": {"output_tokens": 9}
            }),
        ];
        let mut partial = BedrockPartialResponse::new("anthropic.claude-3-sonnet-v1:0".to_string());
        let mut text = String::new();
        for event in events {
            text.push_str(
                &partial
                    .absorb_payload(
                        BedrockModelFamily::Anthropic,
                        &serde_json::to_vec(&event).unwrap(),
                    )
                    .unwrap()
                    .join(""),
            );
        }

        let response = partial
            .into_generate_response(text, ResponseFormat::Text)
            .unwrap();
        let tool_calls = response.tool_calls.unwrap();

        assert_eq!(response.text, "Checking ");
        assert_eq!(response.finish_reason.as_deref(), Some("tool_use"));
        assert_eq!(response.usage.unwrap().total_tokens, Some(24));
        assert_eq!(tool_calls[0].id, "toolu_stream_1");
        assert_eq!(tool_calls[0].arguments, r#"{"city":"Seattle"}"#);
    }

    #[tokio::test]
    async fn bedrock_stream_emits_incremental_deltas_and_completed_response() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let first = encode_event_stream_message(
            &[
                (":message-type", "event"),
                (":event-type", "chunk"),
                (":content-type", "application/json"),
            ],
            br#"{"generation":"Hello ","prompt_token_count":3,"generation_token_count":1}"#,
        );
        let second = encode_event_stream_message(
            &[
                (":message-type", "event"),
                (":event-type", "chunk"),
                (":content-type", "application/json"),
            ],
            br#"{"generation":"world","generation_token_count":2,"stop_reason":"stop"}"#,
        );
        let mut body = first;
        body.extend_from_slice(&second);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request_bytes = [0_u8; 1024];
            let _ = socket.read(&mut request_bytes).await.unwrap();
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/vnd.amazon.eventstream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(headers.as_bytes()).await.unwrap();
            let split = body.len() / 3;
            socket.write_all(&body[..split]).await.unwrap();
            tokio::task::yield_now().await;
            socket.write_all(&body[split..]).await.unwrap();
        });

        let response = reqwest::Client::new()
            .get(format!("http://{address}"))
            .send()
            .await
            .unwrap();
        let mut stream = build_bedrock_stream(
            response,
            BedrockModelFamily::MetaLlama,
            "meta.llama3-70b-instruct-v1:0".to_string(),
            ResponseFormat::Text,
        );
        let mut deltas = Vec::new();
        let mut completed = None;
        while let Some(item) = stream.next().await {
            match item.unwrap() {
                StreamChunk::Delta { content, .. } => deltas.push(content),
                StreamChunk::Completed(response) => completed = Some(response),
                StreamChunk::ContentBlockStart { .. } | StreamChunk::ContentBlockStop { .. } => {}
            }
        }
        server.await.unwrap();

        assert_eq!(deltas, vec!["Hello ", "world"]);
        let completed = completed.expect("stream should emit a completed response");
        assert_eq!(completed.text, "Hello world");
        assert_eq!(completed.finish_reason.as_deref(), Some("stop"));
        assert_eq!(completed.usage.unwrap().total_tokens, Some(5));
    }

    fn encode_event_stream_message(headers: &[(&str, &str)], payload: &[u8]) -> Vec<u8> {
        let mut encoded_headers = Vec::new();
        for (name, value) in headers {
            encoded_headers.push(name.len() as u8);
            encoded_headers.extend_from_slice(name.as_bytes());
            encoded_headers.push(7);
            encoded_headers.extend_from_slice(&(value.len() as u16).to_be_bytes());
            encoded_headers.extend_from_slice(value.as_bytes());
        }

        let total_len = 16 + encoded_headers.len() + payload.len();
        let mut message = Vec::with_capacity(total_len);
        message.extend_from_slice(&(total_len as u32).to_be_bytes());
        message.extend_from_slice(&(encoded_headers.len() as u32).to_be_bytes());
        message.extend_from_slice(&crc32fast::hash(&message[..8]).to_be_bytes());
        message.extend_from_slice(&encoded_headers);
        message.extend_from_slice(payload);
        message.extend_from_slice(&crc32fast::hash(&message).to_be_bytes());
        message
    }
}
