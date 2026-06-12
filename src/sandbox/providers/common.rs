//! Shared helpers for managed sandbox provider implementations.
//!
//! Most external providers (E2B, Daytona, Modal, Northflank, Vercel, Together)
//! execute code by running an interpreter as a shell command inside the
//! sandbox. These helpers centralize the language → command mapping and shell
//! quoting so every provider behaves identically.

use crate::error::{ErrorCode, SdkError};
use crate::sandbox::types::{ExecuteCodeRequest, Language};

/// Interpreter argv for executing `req.code` inside a POSIX sandbox.
///
/// Returns `(program, args)` where the code is passed as a single argument,
/// avoiding shell-injection concerns when the provider accepts an argv array.
pub(crate) fn interpreter_argv(req: &ExecuteCodeRequest) -> (String, Vec<String>) {
    match req.language {
        Language::Python => (
            "python3".to_string(),
            vec!["-c".to_string(), req.code.clone()],
        ),
        Language::Javascript => ("node".to_string(), vec!["-e".to_string(), req.code.clone()]),
        Language::Bash => ("bash".to_string(), vec!["-c".to_string(), req.code.clone()]),
    }
}

/// Interpreter invocation as a single shell command line, for providers whose
/// exec API takes one command string run through `sh -c` (e.g. Daytona's
/// toolbox process API).
pub(crate) fn interpreter_command_line(req: &ExecuteCodeRequest) -> String {
    let (program, args) = interpreter_argv(req);
    let mut line = program;
    for arg in args {
        line.push(' ');
        line.push_str(&shell_single_quote(&arg));
    }
    line
}

/// Quote a string for safe interpolation into a POSIX shell command line.
pub(crate) fn shell_single_quote(s: &str) -> String {
    // 'foo'"'"'bar' pattern: close quote, escaped literal quote, reopen.
    format!("'{}'", s.replace('\'', "'\"'\"'"))
}

/// Parse directory listings in the shared `type|size|mode|mtime|path` line
/// format (one entry per line, `|`-separated):
///
/// - type: `d` for directory, anything else for file
/// - size: bytes
/// - mode: octal permissions (e.g. `644`)
/// - mtime: Unix seconds, fractional part allowed
/// - path: absolute path (may itself contain `|`-free text only)
///
/// This is the output of GNU `find -printf '%y|%s|%m|%T@|%p\n'` and of the
/// equivalent Python `os.walk` snippets used by providers without a native
/// listing API.
pub(crate) fn parse_listing_output(stdout: &str) -> Vec<crate::sandbox::types::FileInfo> {
    let mut files = Vec::new();
    for line in stdout.lines() {
        let parts: Vec<&str> = line.splitn(5, '|').collect();
        if parts.len() != 5 {
            continue;
        }
        let path = parts[4].to_string();
        let name = path.rsplit('/').next().unwrap_or(path.as_str()).to_string();
        files.push(crate::sandbox::types::FileInfo {
            name,
            size: parts[1].parse().unwrap_or(0),
            mode: u32::from_str_radix(parts[2], 8).unwrap_or(0),
            is_dir: parts[0] == "d",
            mod_time: parts[3]
                .parse::<f64>()
                .map(|t| (t * 1000.0) as i64)
                .unwrap_or(0),
            path,
        });
    }
    files
}

/// Standard error for operations a provider's API does not expose.
pub(crate) fn unsupported(provider: &str, operation: &str) -> SdkError {
    SdkError::Sandbox {
        message: format!("{} does not support this operation", provider),
        operation: operation.to_string(),
        code: ErrorCode::UnsupportedOperation,
    }
}

/// Map an HTTP error status from a provider API to a sandbox error.
pub(crate) fn provider_http_error(
    provider: &str,
    operation: &str,
    status: u16,
    body: &str,
) -> SdkError {
    let code = match status {
        401 | 403 => ErrorCode::InvalidConfiguration,
        404 => ErrorCode::SandboxUnavailable,
        408 | 504 => ErrorCode::ExecutionTimeout,
        429 | 502 | 503 => ErrorCode::SandboxUnavailable,
        _ => ErrorCode::SandboxExecutionFailed,
    };
    SdkError::Sandbox {
        message: format!("{} API error (HTTP {}): {}", provider, status, body),
        operation: operation.to_string(),
        code,
    }
}

/// Map a reqwest transport error to a sandbox error.
pub(crate) fn provider_transport_error(
    provider: &str,
    operation: &str,
    err: reqwest::Error,
) -> SdkError {
    let code = if err.is_timeout() {
        ErrorCode::ExecutionTimeout
    } else {
        ErrorCode::SandboxUnavailable
    };
    SdkError::Sandbox {
        message: format!("{} request failed: {}", provider, err),
        operation: operation.to_string(),
        code,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(language: Language, code: &str) -> ExecuteCodeRequest {
        ExecuteCodeRequest {
            code: code.to_string(),
            language,
            timeout_ms: 30_000,
            env: None,
            work_dir: None,
        }
    }

    #[test]
    fn test_interpreter_argv() {
        let (prog, args) = interpreter_argv(&req(Language::Python, "print(1)"));
        assert_eq!(prog, "python3");
        assert_eq!(args, vec!["-c", "print(1)"]);

        let (prog, args) = interpreter_argv(&req(Language::Javascript, "console.log(1)"));
        assert_eq!(prog, "node");
        assert_eq!(args, vec!["-e", "console.log(1)"]);

        let (prog, args) = interpreter_argv(&req(Language::Bash, "echo hi"));
        assert_eq!(prog, "bash");
        assert_eq!(args, vec!["-c", "echo hi"]);
    }

    #[test]
    fn test_shell_single_quote() {
        assert_eq!(shell_single_quote("plain"), "'plain'");
        assert_eq!(shell_single_quote("it's"), "'it'\"'\"'s'");
    }

    #[test]
    fn test_interpreter_command_line_quotes_code() {
        let line = interpreter_command_line(&req(Language::Python, "print('hi')"));
        assert_eq!(line, "python3 '-c' 'print('\"'\"'hi'\"'\"')'");
    }

    #[test]
    fn test_parse_listing_output() {
        let stdout = "f|42|644|1718200000.5|/workspace/test.txt\nd|4096|755|1718200001.0|/workspace/src\nbogus line\n";
        let files = parse_listing_output(stdout);
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].name, "test.txt");
        assert_eq!(files[0].size, 42);
        assert_eq!(files[0].mode, 0o644);
        assert!(!files[0].is_dir);
        assert_eq!(files[0].mod_time, 1718200000500);
        assert!(files[1].is_dir);
        assert_eq!(files[1].name, "src");
    }
}
