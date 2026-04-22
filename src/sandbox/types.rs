//! Canonical types for AGNT5 sandbox operations.
//!
//! These Rust types are the source of truth for the sandbox API contract.
//! Language SDK clients (Python, TypeScript) conform to these definitions.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ── Enums ───────────────────────────────────────────────────────

/// Languages supported for sandboxed code execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Language {
    Python,
    Javascript,
    Bash,
}

impl std::fmt::Display for Language {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Language::Python => write!(f, "python"),
            Language::Javascript => write!(f, "javascript"),
            Language::Bash => write!(f, "bash"),
        }
    }
}

/// Identifies which kind of backend is executing sandbox operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SandboxBackendKind {
    /// Embedded Wasmtime + WASI execution (Level 2). No infrastructure required.
    Wasm,
    /// HTTP client to an external sandbox provider (Level 4/5).
    Remote,
}

impl std::fmt::Display for SandboxBackendKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SandboxBackendKind::Wasm => write!(f, "wasm"),
            SandboxBackendKind::Remote => write!(f, "remote"),
        }
    }
}

// ── Capabilities ────────────────────────────────────────────────

/// Describes what a sandbox backend supports. Callers should check capabilities
/// before invoking operations to fail early rather than hitting runtime errors.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxCapabilities {
    /// Languages supported by `execute_code`.
    pub languages: Vec<Language>,
    /// Whether `run_command` / `run_command_stream` are available.
    pub supports_commands: bool,
    /// Whether `run_command_stream` produces streaming output.
    pub supports_streaming: bool,
    /// Whether git operations (clone, status, commit, push) are available.
    pub supports_git: bool,
    /// Whether `get_preview_url` is available.
    pub supports_preview_url: bool,
    /// Whether snapshot/checkpoint operations are available.
    pub supports_snapshots: bool,
    /// Maximum execution time in milliseconds (0 = unlimited).
    pub max_execution_time_ms: u64,
    /// Maximum memory in bytes (0 = unlimited).
    pub max_memory_bytes: u64,
    /// Whether the sandbox has outbound network access.
    pub has_network_access: bool,
}

// ── Request types ───────────────────────────────────────────────

/// Request to execute code in a sandbox.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteCodeRequest {
    /// Source code to execute.
    pub code: String,
    /// Programming language of the code.
    pub language: Language,
    /// Execution timeout in milliseconds. Defaults to 30000.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Environment variables to set during execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<HashMap<String, String>>,
    /// Working directory for execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_dir: Option<String>,
}

/// Request to run a shell command in a sandbox (RemoteSandbox only).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunCommandRequest {
    /// Command to run.
    pub command: String,
    /// Command arguments.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,
    /// Working directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
    /// Environment variables.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<HashMap<String, String>>,
    /// Execution timeout in milliseconds. Defaults to 30000.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

/// Request to write a file in the sandbox workspace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteFileRequest {
    /// File path (relative to workspace root or absolute within sandbox).
    pub path: String,
    /// File content as raw bytes.
    #[serde(with = "base64_bytes")]
    pub content: Vec<u8>,
    /// File mode/permissions. Defaults to 0o644.
    #[serde(default = "default_file_mode")]
    pub mode: u32,
}

/// Request to clone a git repository (RemoteSandbox only).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitCloneRequest {
    /// Repository URL.
    pub url: String,
    /// Branch to checkout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// Target directory for clone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_dir: Option<String>,
    /// Shallow clone depth (0 = full clone).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth: Option<u32>,
    /// Auth username (HTTPS).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// Auth password or token (HTTPS).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    /// Base64-encoded SSH private key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssh_key: Option<String>,
}

/// Request to create a git commit (RemoteSandbox only).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitCommitRequest {
    /// Commit message.
    pub message: String,
    /// Repository path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Specific files to stage and commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files: Option<Vec<String>>,
    /// Stage all changes before commit.
    #[serde(default)]
    pub all: bool,
    /// Author string ("Name <email>").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
}

/// Request to push commits to remote (RemoteSandbox only).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitPushRequest {
    /// Repository path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Remote name. Defaults to "origin".
    #[serde(default = "default_remote")]
    pub remote: String,
    /// Branch to push.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// Force push.
    #[serde(default)]
    pub force: bool,
    /// Auth username (HTTPS).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// Auth password or token (HTTPS).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    /// Base64-encoded SSH private key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssh_key: Option<String>,
}

// ── Result types ────────────────────────────────────────────────

/// Result of code execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteCodeResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub execution_time_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Result of running a shell command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunCommandResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub execution_time_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Result of writing a file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteFileResult {
    pub success: bool,
    pub path: String,
    #[serde(default)]
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Result of reading a file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadFileResult {
    pub path: String,
    #[serde(with = "base64_bytes")]
    pub content: Vec<u8>,
    pub size: u64,
    #[serde(default)]
    pub mode: u32,
    #[serde(default)]
    pub is_dir: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Information about a single file or directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub name: String,
    pub path: String,
    pub size: u64,
    pub mode: u32,
    pub is_dir: bool,
    /// Last modified time as Unix milliseconds.
    pub mod_time: i64,
}

/// Result of listing files in a directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListFilesResult {
    pub path: String,
    pub files: Vec<FileInfo>,
    pub total: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Event from streaming command execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEvent {
    /// Event type: "stdout", "stderr", "exit", "error".
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: String,
    /// Timestamp as Unix milliseconds.
    pub time: i64,
}

/// Result of a sandbox health check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxHealthResult {
    pub status: String,
    pub sandbox_id: String,
    #[serde(default)]
    pub uptime_ms: u64,
    pub backend_kind: SandboxBackendKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Result of git clone.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitCloneResult {
    pub success: bool,
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub branch: String,
    #[serde(default)]
    pub commit_sha: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Status of a file in git.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitStatusFile {
    pub path: String,
    /// Status: modified, added, deleted, untracked, renamed.
    pub status: String,
    pub staged: bool,
}

/// Result of git status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitStatusResult {
    pub branch: String,
    pub commit_sha: String,
    pub is_clean: bool,
    #[serde(default)]
    pub ahead: i32,
    #[serde(default)]
    pub behind: i32,
    #[serde(default)]
    pub files: Vec<GitStatusFile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Result of git commit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitCommitResult {
    pub success: bool,
    #[serde(default)]
    pub commit_sha: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Result of git push.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitPushResult {
    pub success: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Summary info about a sandbox instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxInfo {
    pub sandbox_id: String,
    pub status: String,
    pub backend_kind: SandboxBackendKind,
}

// ── Defaults ────────────────────────────────────────────────────

fn default_timeout_ms() -> u64 {
    30_000
}

fn default_file_mode() -> u32 {
    0o644
}

fn default_remote() -> String {
    "origin".to_string()
}

// ── Base64 serde helper ─────────────────────────────────────────

/// Serde helper for serializing `Vec<u8>` as base64 strings in JSON.
mod base64_bytes {
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
        serializer.serialize_str(&encoded)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        use base64::Engine;
        let s = String::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD
            .decode(&s)
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_language_display() {
        assert_eq!(Language::Python.to_string(), "python");
        assert_eq!(Language::Javascript.to_string(), "javascript");
        assert_eq!(Language::Bash.to_string(), "bash");
    }

    #[test]
    fn test_language_serde_roundtrip() {
        let lang = Language::Javascript;
        let json = serde_json::to_string(&lang).unwrap();
        assert_eq!(json, "\"javascript\"");
        let parsed: Language = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, lang);
    }

    #[test]
    fn test_backend_kind_serde() {
        let kind = SandboxBackendKind::Wasm;
        let json = serde_json::to_string(&kind).unwrap();
        assert_eq!(json, "\"wasm\"");
    }

    #[test]
    fn test_execute_code_request_defaults() {
        let json = r#"{"code": "print(1)", "language": "python"}"#;
        let req: ExecuteCodeRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.timeout_ms, 30_000);
        assert!(req.env.is_none());
        assert!(req.work_dir.is_none());
    }

    #[test]
    fn test_execute_code_result_serde() {
        let result = ExecuteCodeResult {
            stdout: "hello".to_string(),
            stderr: String::new(),
            exit_code: 0,
            execution_time_ms: 42,
            error: None,
        };
        let json = serde_json::to_string(&result).unwrap();
        let parsed: ExecuteCodeResult = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.stdout, "hello");
        assert_eq!(parsed.exit_code, 0);
    }

    #[test]
    fn test_write_file_request_default_mode() {
        let json = r#"{"path": "/test.txt", "content": "aGVsbG8="}"#;
        let req: WriteFileRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.mode, 0o644);
        assert_eq!(req.content, b"hello");
    }

    #[test]
    fn test_capabilities_serde() {
        let caps = SandboxCapabilities {
            languages: vec![Language::Javascript],
            supports_commands: false,
            supports_streaming: false,
            supports_git: false,
            supports_preview_url: false,
            supports_snapshots: false,
            max_execution_time_ms: 30_000,
            max_memory_bytes: 256 * 1024 * 1024,
            has_network_access: false,
        };
        let json = serde_json::to_string(&caps).unwrap();
        let parsed: SandboxCapabilities = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.languages, vec![Language::Javascript]);
        assert!(!parsed.supports_commands);
    }
}
