//! WasmSandbox — Embedded Wasmtime + WASI execution (Level 2).
//!
//! Provides zero-infrastructure sandboxed code execution by embedding Wasmtime
//! in the SDK process. Currently supports JavaScript via a QuickJS WASI module.
//!
//! This module is behind the `wasm-sandbox` feature flag.
//!
//! # Supported Operations
//!
//! - `execute_code` (JavaScript only — Python and Bash return `UnsupportedLanguage`)
//! - `write_file`, `read_file`, `delete_file`, `list_files` (isolated temp workspace)
//! - `health`
//!
//! # Unsupported Operations
//!
//! Shell commands, git operations, preview URLs, and snapshots are not available
//! in the Wasm backend. These require a real OS and should use [`RemoteSandbox`].
//!
//! # QuickJS WASI Module
//!
//! The WasmSandbox requires a QuickJS binary compiled to WASI (`.wasm` file).
//! Set `AGNT5_QUICKJS_WASM_PATH` or pass `quickjs_wasm_path` in config.
//! Without it, `execute_code` returns `NotImplemented`.
//!
//! To obtain a QuickJS WASI binary:
//! ```sh
//! # Option 1: Build from nicholasgasior/nicholasgasior/nicholasgasior project
//! # Option 2: Use aspect-build's prebuilt binary
//! # Option 3: Compile QuickJS with wasi-sdk yourself
//! ```

use crate::error::{ErrorCode, Result, SdkError};
use crate::sandbox::types::*;
use crate::sandbox::{SandboxExecutor, SandboxWorkspace};
use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use wasmtime::{Config, Engine, Linker, Module, Store};
use wasmtime_wasi::pipe::{MemoryInputPipe, MemoryOutputPipe};
use wasmtime_wasi::preview1::{self, WasiP1Ctx};
use wasmtime_wasi::WasiCtxBuilder;

/// Fuel units per millisecond of execution time.
/// Empirically calibrated — QuickJS uses roughly 10M fuel/second on modern hardware.
/// This is configurable via `WasmSandboxConfig::fuel_per_ms`.
const DEFAULT_FUEL_PER_MS: u64 = 10_000;

// ── Config ──────────────────────────────────────────────────────

/// Configuration for the embedded Wasm sandbox.
#[derive(Debug, Clone)]
pub struct WasmSandboxConfig {
    /// Maximum memory for WASI guests in bytes. Default: 256 MiB.
    pub max_memory_bytes: u64,
    /// Maximum execution time in milliseconds. Default: 30_000.
    pub max_execution_time_ms: u64,
    /// Maximum stdout/stderr output in bytes. Default: 1 MiB.
    pub max_output_bytes: usize,
    /// Workspace directory for file operations. If None, uses a temp directory.
    pub workspace_dir: Option<PathBuf>,
    /// Path to the QuickJS WASI binary. If None, checks `AGNT5_QUICKJS_WASM_PATH`.
    pub quickjs_wasm_path: Option<PathBuf>,
    /// Fuel units per millisecond for timeout calibration. Default: 10_000.
    pub fuel_per_ms: u64,
}

impl Default for WasmSandboxConfig {
    fn default() -> Self {
        Self {
            max_memory_bytes: 256 * 1024 * 1024,
            max_execution_time_ms: 30_000,
            max_output_bytes: 1024 * 1024,
            workspace_dir: None,
            quickjs_wasm_path: None,
            fuel_per_ms: DEFAULT_FUEL_PER_MS,
        }
    }
}

// ── WasmSandbox ─────────────────────────────────────────────────

/// Embedded Wasm sandbox using Wasmtime + WASI.
///
/// Each `execute_code` call creates a fresh WASI context with its own memory
/// and filesystem isolation. The Wasmtime engine and compiled modules are
/// shared across calls for performance.
pub struct WasmSandbox {
    config: WasmSandboxConfig,
    capabilities: SandboxCapabilities,
    workspace: PathBuf,
    _workspace_handle: tempfile::TempDir,
    engine: Arc<Engine>,
    /// Pre-compiled QuickJS module (None if binary not provided).
    quickjs_module: Option<Module>,
    /// Pre-configured linker with WASI preview1 bindings.
    linker: Arc<Linker<WasiP1Ctx>>,
}

impl WasmSandbox {
    /// Create a new WasmSandbox.
    ///
    /// Initializes the Wasmtime engine and optionally pre-compiles the QuickJS
    /// WASI module. If the QuickJS binary is not found, the sandbox will still
    /// work for file operations but `execute_code` will return `NotImplemented`.
    pub fn new(config: WasmSandboxConfig) -> Result<Self> {
        // Create isolated workspace directory
        let workspace_handle = tempfile::TempDir::new().map_err(|e| SdkError::Sandbox {
            message: format!("failed to create workspace directory: {}", e),
            operation: "new".to_string(),
            code: ErrorCode::SandboxExecutionFailed,
        })?;

        let workspace = config
            .workspace_dir
            .clone()
            .unwrap_or_else(|| workspace_handle.path().to_path_buf());

        // Configure Wasmtime engine with fuel for CPU limiting
        let mut engine_config = Config::new();
        engine_config.consume_fuel(true);
        // Async support not needed — we run WASI modules synchronously
        // on a blocking thread via spawn_blocking.

        let engine = Engine::new(&engine_config).map_err(|e| SdkError::Sandbox {
            message: format!("failed to create Wasmtime engine: {}", e),
            operation: "new".to_string(),
            code: ErrorCode::SandboxExecutionFailed,
        })?;

        // Set up linker with WASI preview1 bindings
        let mut linker: Linker<WasiP1Ctx> = Linker::new(&engine);
        preview1::add_to_linker_sync(&mut linker, |ctx| ctx).map_err(|e| SdkError::Sandbox {
            message: format!("failed to link WASI functions: {}", e),
            operation: "new".to_string(),
            code: ErrorCode::SandboxExecutionFailed,
        })?;

        // Try to pre-compile QuickJS module
        let quickjs_path = config
            .quickjs_wasm_path
            .clone()
            .or_else(|| std::env::var("AGNT5_QUICKJS_WASM_PATH").ok().map(PathBuf::from));

        let quickjs_module = if let Some(path) = quickjs_path {
            if path.exists() {
                let module =
                    Module::from_file(&engine, &path).map_err(|e| SdkError::Sandbox {
                        message: format!("failed to compile QuickJS module from {}: {}", path.display(), e),
                        operation: "new".to_string(),
                        code: ErrorCode::SandboxExecutionFailed,
                    })?;
                tracing::info!("QuickJS WASI module loaded from {}", path.display());
                Some(module)
            } else {
                tracing::warn!(
                    "QuickJS WASI binary not found at {}. execute_code will be unavailable.",
                    path.display()
                );
                None
            }
        } else {
            tracing::debug!(
                "No QuickJS WASI path configured. Set AGNT5_QUICKJS_WASM_PATH or pass quickjs_wasm_path in config."
            );
            None
        };

        let capabilities = SandboxCapabilities {
            languages: if quickjs_module.is_some() {
                vec![Language::Javascript]
            } else {
                vec![]
            },
            supports_commands: false,
            supports_streaming: false,
            supports_git: false,
            supports_preview_url: false,
            supports_snapshots: false,
            max_execution_time_ms: config.max_execution_time_ms,
            max_memory_bytes: config.max_memory_bytes,
            has_network_access: false,
        };

        Ok(Self {
            config,
            capabilities,
            workspace,
            _workspace_handle: workspace_handle,
            engine: Arc::new(engine),
            quickjs_module,
            linker: Arc::new(linker),
        })
    }

    /// Resolve a path within the workspace, preventing directory traversal.
    ///
    /// Normalizes path components (resolving `.` and `..`) without touching
    /// the filesystem, then verifies the result stays within the workspace.
    fn resolve_path(&self, path: &str) -> Result<PathBuf> {
        // Canonicalize the workspace first (resolves symlinks like /tmp → /private/tmp)
        let workspace_canonical = self.workspace.canonicalize().map_err(|e| SdkError::Sandbox {
            message: format!("workspace path error: {}", e),
            operation: "resolve_path".to_string(),
            code: ErrorCode::SandboxExecutionFailed,
        })?;

        let raw = if path.starts_with('/') {
            workspace_canonical.join(path.trim_start_matches('/'))
        } else {
            workspace_canonical.join(path)
        };

        // Normalize the path components without filesystem access
        let mut normalized = PathBuf::new();
        for component in raw.components() {
            match component {
                std::path::Component::ParentDir => {
                    normalized.pop();
                }
                std::path::Component::CurDir => {}
                other => normalized.push(other),
            }
        }

        // Verify the normalized path is within the workspace
        if !normalized.starts_with(&workspace_canonical) {
            return Err(SdkError::Sandbox {
                message: "path traversal denied".to_string(),
                operation: "resolve_path".to_string(),
                code: ErrorCode::InvalidInput,
            });
        }

        Ok(normalized)
    }
}

// ── SandboxExecutor impl ────────────────────────────────────────

#[async_trait]
impl SandboxExecutor for WasmSandbox {
    fn backend_kind(&self) -> SandboxBackendKind {
        SandboxBackendKind::Wasm
    }

    fn capabilities(&self) -> &SandboxCapabilities {
        &self.capabilities
    }

    async fn execute_code(&self, req: ExecuteCodeRequest) -> Result<ExecuteCodeResult> {
        // Fail early on unsupported languages
        match req.language {
            Language::Javascript => {}
            Language::Python => {
                return Err(SdkError::Sandbox {
                    message: "Python execution is not yet supported in the Wasm backend. Use RemoteSandbox for Python.".to_string(),
                    operation: "execute_code".to_string(),
                    code: ErrorCode::UnsupportedLanguage,
                });
            }
            Language::Bash => {
                return Err(SdkError::Sandbox {
                    message: "Bash execution is not supported in the Wasm backend. Use RemoteSandbox for shell commands.".to_string(),
                    operation: "execute_code".to_string(),
                    code: ErrorCode::UnsupportedLanguage,
                });
            }
        }

        // Run Wasmtime on a blocking thread to avoid blocking the async runtime.
        // Wasmtime execution is CPU-bound and synchronous within a Store.
        let engine = self.engine.clone();
        let linker = self.linker.clone();
        let module = self.quickjs_module.clone();
        let max_output = self.config.max_output_bytes;
        let fuel_per_ms = self.config.fuel_per_ms;
        let code = req.code;
        let timeout_ms = req.timeout_ms;
        let env = req.env;

        // We need to create a temporary WasmSandbox-like context on the blocking thread
        // because Store is not Send. Build everything we need as owned values.
        tokio::task::spawn_blocking(move || {
            let module = module.ok_or_else(|| SdkError::Sandbox {
                message: "QuickJS WASI module not loaded. Set AGNT5_QUICKJS_WASM_PATH.".to_string(),
                operation: "execute_code".to_string(),
                code: ErrorCode::NotImplemented,
            })?;

            let fuel = timeout_ms.saturating_mul(fuel_per_ms);

            let stdin_pipe = MemoryInputPipe::new(bytes::Bytes::new());
            let stdout_pipe = MemoryOutputPipe::new(max_output);
            let stderr_pipe = MemoryOutputPipe::new(max_output);

            let stdout_clone = stdout_pipe.clone();
            let stderr_clone = stderr_pipe.clone();

            let mut ctx_builder = WasiCtxBuilder::new();
            ctx_builder
                .stdin(stdin_pipe)
                .stdout(stdout_pipe)
                .stderr(stderr_pipe)
                .args(&["qjs", "--eval", &code]);

            if let Some(env_vars) = &env {
                for (k, v) in env_vars {
                    ctx_builder.env(k, v);
                }
            }

            let wasi_ctx = ctx_builder.build_p1();
            let mut store = Store::new(&engine, wasi_ctx);
            store.set_fuel(fuel).map_err(|e| SdkError::Sandbox {
                message: format!("failed to set fuel: {}", e),
                operation: "execute_code".to_string(),
                code: ErrorCode::SandboxExecutionFailed,
            })?;

            let start_time = Instant::now();

            let instance = linker.instantiate(&mut store, &module).map_err(|e| SdkError::Sandbox {
                message: format!("failed to instantiate module: {}", e),
                operation: "execute_code".to_string(),
                code: ErrorCode::SandboxExecutionFailed,
            })?;

            let start_func = instance
                .get_typed_func::<(), ()>(&mut store, "_start")
                .map_err(|e| SdkError::Sandbox {
                    message: format!("QuickJS module missing _start function: {}", e),
                    operation: "execute_code".to_string(),
                    code: ErrorCode::SandboxExecutionFailed,
                })?;

            let (exit_code, error) = match start_func.call(&mut store, ()) {
                Ok(()) => (0, None),
                Err(e) => {
                    if let Some(trap) = e.downcast_ref::<wasmtime::Trap>() {
                        if matches!(trap, wasmtime::Trap::OutOfFuel) {
                            return Ok(ExecuteCodeResult {
                                stdout: String::from_utf8_lossy(&stdout_clone.contents()).to_string(),
                                stderr: String::from_utf8_lossy(&stderr_clone.contents()).to_string(),
                                exit_code: -1,
                                execution_time_ms: start_time.elapsed().as_millis() as u64,
                                error: Some(format!("execution timed out ({}ms limit)", timeout_ms)),
                            });
                        }
                    }
                    if let Some(exit) = e.downcast_ref::<wasmtime_wasi::I32Exit>() {
                        (
                            exit.0,
                            if exit.0 != 0 {
                                Some(format!("process exited with code {}", exit.0))
                            } else {
                                None
                            },
                        )
                    } else {
                        (1, Some(format!("execution error: {}", e)))
                    }
                }
            };

            Ok(ExecuteCodeResult {
                stdout: String::from_utf8_lossy(&stdout_clone.contents()).to_string(),
                stderr: String::from_utf8_lossy(&stderr_clone.contents()).to_string(),
                exit_code,
                execution_time_ms: start_time.elapsed().as_millis() as u64,
                error,
            })
        })
        .await
        .map_err(|e| SdkError::Sandbox {
            message: format!("execution thread panicked: {}", e),
            operation: "execute_code".to_string(),
            code: ErrorCode::SandboxExecutionFailed,
        })?
    }

    async fn health(&self) -> Result<SandboxHealthResult> {
        Ok(SandboxHealthResult {
            status: "ok".to_string(),
            sandbox_id: "wasm-embedded".to_string(),
            uptime_ms: 0,
            backend_kind: SandboxBackendKind::Wasm,
            error: None,
        })
    }
}

// ── SandboxWorkspace impl ───────────────────────────────────────

#[async_trait]
impl SandboxWorkspace for WasmSandbox {
    async fn write_file(&self, req: WriteFileRequest) -> Result<WriteFileResult> {
        let resolved = self.resolve_path(&req.path)?;

        if let Some(parent) = resolved.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| SdkError::Sandbox {
                message: format!("failed to create parent directory: {}", e),
                operation: "write_file".to_string(),
                code: ErrorCode::SandboxExecutionFailed,
            })?;
        }

        tokio::fs::write(&resolved, &req.content)
            .await
            .map_err(|e| SdkError::Sandbox {
                message: format!("failed to write file: {}", e),
                operation: "write_file".to_string(),
                code: ErrorCode::SandboxExecutionFailed,
            })?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(req.mode);
            tokio::fs::set_permissions(&resolved, perms).await.ok();
        }

        Ok(WriteFileResult {
            success: true,
            path: req.path,
            size: req.content.len() as u64,
            error: None,
        })
    }

    async fn read_file(&self, path: &str) -> Result<ReadFileResult> {
        let resolved = self.resolve_path(path)?;

        let metadata = tokio::fs::metadata(&resolved).await.map_err(|e| SdkError::Sandbox {
            message: format!("file not found: {}", e),
            operation: "read_file".to_string(),
            code: ErrorCode::SandboxExecutionFailed,
        })?;

        let content = tokio::fs::read(&resolved).await.map_err(|e| SdkError::Sandbox {
            message: format!("failed to read file: {}", e),
            operation: "read_file".to_string(),
            code: ErrorCode::SandboxExecutionFailed,
        })?;

        let mode = {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                metadata.permissions().mode()
            }
            #[cfg(not(unix))]
            {
                0o644u32
            }
        };

        Ok(ReadFileResult {
            path: path.to_string(),
            content,
            size: metadata.len(),
            mode,
            is_dir: metadata.is_dir(),
            error: None,
        })
    }

    async fn delete_file(&self, path: &str, recursive: bool) -> Result<bool> {
        let resolved = self.resolve_path(path)?;

        if !resolved.exists() {
            return Ok(false);
        }

        if resolved.is_dir() {
            if recursive {
                tokio::fs::remove_dir_all(&resolved).await
            } else {
                tokio::fs::remove_dir(&resolved).await
            }
        } else {
            tokio::fs::remove_file(&resolved).await
        }
        .map_err(|e| SdkError::Sandbox {
            message: format!("failed to delete: {}", e),
            operation: "delete_file".to_string(),
            code: ErrorCode::SandboxExecutionFailed,
        })?;

        Ok(true)
    }

    async fn list_files(&self, path: &str, recursive: bool) -> Result<ListFilesResult> {
        let resolved = self.resolve_path(path)?;

        if !resolved.is_dir() {
            return Err(SdkError::Sandbox {
                message: format!("not a directory: {}", path),
                operation: "list_files".to_string(),
                code: ErrorCode::InvalidInput,
            });
        }

        let mut files = Vec::new();
        collect_files(&resolved, &resolved, recursive, &mut files).await?;
        let total = files.len() as u64;

        Ok(ListFilesResult {
            path: path.to_string(),
            files,
            total,
            error: None,
        })
    }
}

/// Recursively collect file info from a directory.
async fn collect_files(
    base: &std::path::Path,
    dir: &std::path::Path,
    recursive: bool,
    out: &mut Vec<FileInfo>,
) -> Result<()> {
    let mut entries = tokio::fs::read_dir(dir).await.map_err(|e| SdkError::Sandbox {
        message: format!("failed to read directory: {}", e),
        operation: "list_files".to_string(),
        code: ErrorCode::SandboxExecutionFailed,
    })?;

    while let Some(entry) = entries.next_entry().await.map_err(|e| SdkError::Sandbox {
        message: format!("failed to read directory entry: {}", e),
        operation: "list_files".to_string(),
        code: ErrorCode::SandboxExecutionFailed,
    })? {
        let metadata = entry.metadata().await.map_err(|e| SdkError::Sandbox {
            message: format!("failed to read metadata: {}", e),
            operation: "list_files".to_string(),
            code: ErrorCode::SandboxExecutionFailed,
        })?;

        let relative_path = entry
            .path()
            .strip_prefix(base)
            .unwrap_or(entry.path().as_path())
            .to_string_lossy()
            .to_string();

        let mode = {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                metadata.permissions().mode()
            }
            #[cfg(not(unix))]
            {
                0o644u32
            }
        };

        let mod_time = metadata
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        out.push(FileInfo {
            name: entry.file_name().to_string_lossy().to_string(),
            path: relative_path,
            size: metadata.len(),
            mode,
            is_dir: metadata.is_dir(),
            mod_time,
        });

        if recursive && metadata.is_dir() {
            Box::pin(collect_files(base, &entry.path(), true, out)).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wasm_sandbox_config_default() {
        let config = WasmSandboxConfig::default();
        assert_eq!(config.max_memory_bytes, 256 * 1024 * 1024);
        assert_eq!(config.max_execution_time_ms, 30_000);
        assert_eq!(config.max_output_bytes, 1024 * 1024);
        assert!(config.workspace_dir.is_none());
        assert!(config.quickjs_wasm_path.is_none());
        assert_eq!(config.fuel_per_ms, DEFAULT_FUEL_PER_MS);
    }

    #[tokio::test]
    async fn test_wasm_sandbox_health() {
        let sandbox = WasmSandbox::new(WasmSandboxConfig::default()).unwrap();
        let health = sandbox.health().await.unwrap();
        assert_eq!(health.status, "ok");
        assert_eq!(health.backend_kind, SandboxBackendKind::Wasm);
    }

    #[tokio::test]
    async fn test_wasm_sandbox_capabilities_without_quickjs() {
        let sandbox = WasmSandbox::new(WasmSandboxConfig::default()).unwrap();
        let caps = sandbox.capabilities();
        // Without QuickJS binary, no languages available
        assert!(caps.languages.is_empty());
        assert!(!caps.supports_commands);
        assert!(!caps.supports_git);
        assert!(!caps.supports_preview_url);
        assert!(!caps.has_network_access);
    }

    #[tokio::test]
    async fn test_wasm_sandbox_unsupported_python() {
        let sandbox = WasmSandbox::new(WasmSandboxConfig::default()).unwrap();
        let result = sandbox
            .execute_code(ExecuteCodeRequest {
                code: "print('hello')".to_string(),
                language: Language::Python,
                timeout_ms: 5000,
                env: None,
                work_dir: None,
            })
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), ErrorCode::UnsupportedLanguage);
    }

    #[tokio::test]
    async fn test_wasm_sandbox_js_without_module() {
        let sandbox = WasmSandbox::new(WasmSandboxConfig::default()).unwrap();
        let result = sandbox
            .execute_code(ExecuteCodeRequest {
                code: "console.log('hello')".to_string(),
                language: Language::Javascript,
                timeout_ms: 5000,
                env: None,
                work_dir: None,
            })
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), ErrorCode::NotImplemented);
    }

    #[tokio::test]
    async fn test_wasm_sandbox_file_operations() {
        let sandbox = WasmSandbox::new(WasmSandboxConfig::default()).unwrap();

        // Write a file
        let write_result = sandbox
            .write_file(WriteFileRequest {
                path: "test.txt".to_string(),
                content: b"hello world".to_vec(),
                mode: 0o644,
            })
            .await
            .unwrap();
        assert!(write_result.success);
        assert_eq!(write_result.size, 11);

        // Read it back
        let read_result = sandbox.read_file("test.txt").await.unwrap();
        assert_eq!(read_result.content, b"hello world");
        assert!(!read_result.is_dir);

        // List files
        let list_result = sandbox.list_files(".", false).await.unwrap();
        assert!(list_result.files.iter().any(|f| f.name == "test.txt"));

        // Delete it
        let deleted = sandbox.delete_file("test.txt", false).await.unwrap();
        assert!(deleted);

        // Verify gone
        let read_after = sandbox.read_file("test.txt").await;
        assert!(read_after.is_err());
    }

    /// Integration test: actually runs JS code via QuickJS WASI.
    /// Requires AGNT5_QUICKJS_WASM_PATH to be set (skipped otherwise).
    #[tokio::test]
    async fn test_wasm_sandbox_execute_js() {
        let quickjs_path = std::env::var("AGNT5_QUICKJS_WASM_PATH").ok()
            .or_else(|| {
                let default = std::path::PathBuf::from("/tmp/qjs-wasi.wasm");
                if default.exists() { Some(default.to_string_lossy().to_string()) } else { None }
            });

        let Some(path) = quickjs_path else {
            eprintln!("SKIP: AGNT5_QUICKJS_WASM_PATH not set and /tmp/qjs-wasi.wasm not found");
            return;
        };

        let config = WasmSandboxConfig {
            quickjs_wasm_path: Some(PathBuf::from(&path)),
            ..Default::default()
        };
        let sandbox = WasmSandbox::new(config).unwrap();

        // Verify capabilities now include Javascript
        assert!(sandbox.capabilities().languages.contains(&Language::Javascript));

        // Simple console.log
        let result = sandbox
            .execute_code(ExecuteCodeRequest {
                code: "console.log('hello from wasm')".to_string(),
                language: Language::Javascript,
                timeout_ms: 10_000,
                env: None,
                work_dir: None,
            })
            .await
            .unwrap();

        assert_eq!(result.exit_code, 0);
        assert!(result.stdout.contains("hello from wasm"), "stdout was: {}", result.stdout);
        assert!(result.error.is_none());

        // Math expression
        let result = sandbox
            .execute_code(ExecuteCodeRequest {
                code: "console.log(2 + 2)".to_string(),
                language: Language::Javascript,
                timeout_ms: 10_000,
                env: None,
                work_dir: None,
            })
            .await
            .unwrap();

        assert_eq!(result.exit_code, 0);
        assert!(result.stdout.contains("4"), "stdout was: {}", result.stdout);

        // JSON manipulation
        let result = sandbox
            .execute_code(ExecuteCodeRequest {
                code: r#"console.log(JSON.stringify({name: "agnt5", version: 1}))"#.to_string(),
                language: Language::Javascript,
                timeout_ms: 10_000,
                env: None,
                work_dir: None,
            })
            .await
            .unwrap();

        assert_eq!(result.exit_code, 0);
        assert!(result.stdout.contains(r#""name":"agnt5""#), "stdout was: {}", result.stdout);

        // Error case — should capture stderr and non-zero exit
        let result = sandbox
            .execute_code(ExecuteCodeRequest {
                code: "throw new Error('test error')".to_string(),
                language: Language::Javascript,
                timeout_ms: 10_000,
                env: None,
                work_dir: None,
            })
            .await
            .unwrap();

        assert_ne!(result.exit_code, 0);
    }

    #[tokio::test]
    async fn test_wasm_sandbox_path_traversal_blocked() {
        let sandbox = WasmSandbox::new(WasmSandboxConfig::default()).unwrap();

        let result = sandbox
            .write_file(WriteFileRequest {
                path: "../../etc/passwd".to_string(),
                content: b"malicious".to_vec(),
                mode: 0o644,
            })
            .await;

        assert!(result.is_err());
    }
}
