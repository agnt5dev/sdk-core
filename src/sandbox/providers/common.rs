//! Shared helpers for managed sandbox provider implementations.
//!
//! Most external providers (E2B, Daytona, Modal, Northflank, Vercel, Together)
//! execute code by running an interpreter as a shell command inside the
//! sandbox. These helpers centralize the language → command mapping and shell
//! quoting so every provider behaves identically.

use crate::error::{ErrorCode, Result, SdkError};
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
}
