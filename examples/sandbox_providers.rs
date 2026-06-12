//! Smoke-test template for the managed sandbox provider integrations.
//!
//! Detects providers from environment variables, then walks each one through
//! the full lifecycle: create → health → execute code (every supported
//! language) → file write/read/list/delete → destroy.
//!
//! # Usage
//!
//! ```sh
//! # Configure one or more providers:
//! export E2B_API_KEY=e2b_...                # E2B
//! export DAYTONA_API_KEY=dtn_...            # Daytona
//! export VERCEL_TOKEN=... \
//!        VERCEL_TEAM_ID=team_... \
//!        VERCEL_PROJECT_ID=prj_...          # Vercel (or just VERCEL_OIDC_TOKEN)
//! export NORTHFLANK_API_TOKEN=... \
//!        NORTHFLANK_PROJECT_ID=...          # Northflank
//! export TOGETHER_API_KEY=...               # Together Code Interpreter
//! export MODAL_TOKEN_ID=ak-... \
//!        MODAL_TOKEN_SECRET=as-...          # Modal
//!
//! # Test everything that's configured:
//! cargo run --example sandbox_providers
//!
//! # Or a single provider:
//! cargo run --example sandbox_providers e2b
//! ```
//!
//! Sandbox creation is billed by the provider — each run provisions one
//! sandbox per tested provider and destroys it afterwards (Together sessions
//! cannot be destroyed and expire on their own after 60 minutes).
//!
//! Notes:
//! - Daytona and Northflank creation can take 1–3 minutes (image pull).
//! - Provider templates/images can be overridden with `E2B_TEMPLATE`,
//!   `NORTHFLANK_SANDBOX_IMAGE`, or per-provider defaults documented in
//!   `src/sandbox/providers/`.

use agnt5_sdk_core::error::ErrorCode;
use agnt5_sdk_core::sandbox::{
    CreateSandboxOptions, ExecuteCodeRequest, Language, SandboxBackend, SandboxProvider,
    SandboxRegistry, WriteFileRequest,
};
use std::sync::Arc;
use std::time::Instant;

const TEST_DIR: &str = "/tmp/agnt5_smoke";
const TEST_FILE: &str = "/tmp/agnt5_smoke/hello.txt";
const TEST_CONTENT: &[u8] = b"hello from the AGNT5 sandbox smoke test\n";

#[tokio::main]
async fn main() {
    let filter = std::env::args().nth(1);

    let mut registry = SandboxRegistry::new();
    if let Err(e) = registry.load_providers_from_environment() {
        eprintln!("provider configuration error: {}", e);
        std::process::exit(1);
    }

    let mut names: Vec<String> = registry
        .list_providers()
        .into_iter()
        .map(str::to_string)
        .collect();
    names.sort();

    if let Some(filter) = &filter {
        names.retain(|n| n == filter);
        if names.is_empty() {
            eprintln!(
                "provider '{}' is not configured (check its env vars)",
                filter
            );
            std::process::exit(1);
        }
    }

    if names.is_empty() {
        eprintln!("No sandbox providers configured. Set one or more of:");
        eprintln!("  E2B_API_KEY");
        eprintln!("  DAYTONA_API_KEY");
        eprintln!("  VERCEL_OIDC_TOKEN (or VERCEL_TOKEN + VERCEL_TEAM_ID + VERCEL_PROJECT_ID)");
        eprintln!("  NORTHFLANK_API_TOKEN + NORTHFLANK_PROJECT_ID");
        eprintln!("  TOGETHER_API_KEY");
        eprintln!("  MODAL_TOKEN_ID + MODAL_TOKEN_SECRET");
        std::process::exit(1);
    }

    println!("=== AGNT5 sandbox provider smoke test ===");
    println!("providers under test: {}\n", names.join(", "));

    let mut all_ok = true;
    for name in &names {
        let provider = registry.get_provider(name).expect("provider is registered");
        let ok = run_suite(name, provider).await;
        all_ok &= ok;
    }

    std::process::exit(if all_ok { 0 } else { 1 });
}

/// Outcome of a single smoke-test step.
enum StepResult {
    Pass(String),
    Skip(String),
    Fail(String),
}

struct Suite {
    passed: u32,
    skipped: u32,
    failed: u32,
}

impl Suite {
    fn new() -> Self {
        Self {
            passed: 0,
            skipped: 0,
            failed: 0,
        }
    }

    fn record(&mut self, step: &str, started: Instant, result: StepResult) {
        let elapsed = started.elapsed();
        match result {
            StepResult::Pass(detail) => {
                self.passed += 1;
                println!("  {:<28} ok    ({:.1?}) {}", step, elapsed, detail);
            }
            StepResult::Skip(reason) => {
                self.skipped += 1;
                println!("  {:<28} skip  — {}", step, reason);
            }
            StepResult::Fail(error) => {
                self.failed += 1;
                println!("  {:<28} FAIL  ({:.1?}) {}", step, elapsed, error);
            }
        }
    }
}

/// Convert an operation result into a step result, treating "the provider
/// doesn't support this" as a skip rather than a failure.
fn step_result<T>(
    result: agnt5_sdk_core::error::Result<T>,
    detail: impl FnOnce(T) -> String,
) -> StepResult {
    match result {
        Ok(value) => StepResult::Pass(detail(value)),
        Err(e)
            if matches!(
                e.code(),
                ErrorCode::UnsupportedOperation | ErrorCode::UnsupportedLanguage
            ) =>
        {
            StepResult::Skip(format!("{}", e))
        }
        Err(e) => StepResult::Fail(format!("{}", e)),
    }
}

async fn run_suite(name: &str, provider: Arc<dyn SandboxProvider>) -> bool {
    println!("--- {} ---", name);
    let mut suite = Suite::new();

    // 1. Create. Everything else depends on this, so bail out on failure.
    let started = Instant::now();
    let sandbox: Arc<dyn SandboxBackend> = match provider
        .create_sandbox(CreateSandboxOptions {
            timeout_secs: Some(300),
            ..Default::default()
        })
        .await
    {
        Ok(sandbox) => {
            suite.record("create_sandbox", started, StepResult::Pass(String::new()));
            sandbox
        }
        Err(e) => {
            suite.record(
                "create_sandbox",
                started,
                StepResult::Fail(format!("{}", e)),
            );
            println!(
                "  summary: {} passed, {} skipped, {} FAILED — aborting suite\n",
                suite.passed, suite.skipped, suite.failed
            );
            return false;
        }
    };

    // 2. Health.
    let started = Instant::now();
    let health = sandbox.health().await;
    let sandbox_id = health
        .as_ref()
        .map(|h| h.sandbox_id.clone())
        .unwrap_or_default();
    suite.record(
        "health",
        started,
        step_result(health, |h| {
            format!("status={} id={}", h.status, h.sandbox_id)
        }),
    );

    // 3. Code execution in every language the backend claims to support.
    let snippets = [
        (Language::Python, "print('py:' + str(6 * 7))", "py:42"),
        (Language::Javascript, "console.log('js:' + 6 * 7)", "js:42"),
        (Language::Bash, "echo bash:$((6 * 7))", "bash:42"),
    ];
    let supported = sandbox.capabilities().languages.clone();
    for (language, code, expected) in snippets {
        let step = format!("execute_code[{}]", language);
        let started = Instant::now();
        if !supported.contains(&language) {
            suite.record(
                &step,
                started,
                StepResult::Skip("not in capabilities".into()),
            );
            continue;
        }
        let result = sandbox
            .execute_code(ExecuteCodeRequest {
                code: code.to_string(),
                language,
                timeout_ms: 60_000,
                env: None,
                work_dir: None,
            })
            .await;
        let outcome = step_result(result, |r| {
            format!("exit={} stdout={:?}", r.exit_code, r.stdout.trim())
        });
        // Verify the output actually round-tripped, not just that the call
        // succeeded.
        let outcome = match outcome {
            StepResult::Pass(detail) if !detail.contains(expected) => StepResult::Fail(format!(
                "expected stdout to contain {:?}; got {}",
                expected, detail
            )),
            other => other,
        };
        suite.record(&step, started, outcome);
    }

    // 4. Workspace file round-trip.
    let started = Instant::now();
    let result = sandbox
        .write_file(WriteFileRequest {
            path: TEST_FILE.to_string(),
            content: TEST_CONTENT.to_vec(),
            mode: 0o644,
        })
        .await;
    suite.record(
        "write_file",
        started,
        step_result(result, |r| format!("{} ({} bytes)", r.path, r.size)),
    );

    let started = Instant::now();
    let result = sandbox.read_file(TEST_FILE).await;
    let outcome = match result {
        Ok(r) if r.content == TEST_CONTENT => {
            StepResult::Pass(format!("round-trip verified ({} bytes)", r.size))
        }
        Ok(r) => StepResult::Fail(format!(
            "content mismatch: wrote {} bytes, read {} bytes",
            TEST_CONTENT.len(),
            r.content.len()
        )),
        Err(e) => step_result(Err::<(), _>(e), |_| String::new()),
    };
    suite.record("read_file", started, outcome);

    let started = Instant::now();
    let result = sandbox.list_files(TEST_DIR, false).await;
    suite.record(
        "list_files",
        started,
        step_result(result, |r| format!("{} entries in {}", r.total, r.path)),
    );

    let started = Instant::now();
    let result = sandbox.delete_file(TEST_DIR, true).await;
    suite.record(
        "delete_file",
        started,
        step_result(result, |ok| format!("deleted={}", ok)),
    );

    // 5. Control plane: list + destroy.
    let started = Instant::now();
    let result = provider.list_sandboxes().await;
    suite.record(
        "list_sandboxes",
        started,
        step_result(result, |s| format!("{} visible", s.len())),
    );

    let started = Instant::now();
    if sandbox_id.is_empty() {
        suite.record(
            "destroy_sandbox",
            started,
            StepResult::Skip("no sandbox id from health".into()),
        );
    } else {
        let result = provider.destroy_sandbox(&sandbox_id).await;
        suite.record(
            "destroy_sandbox",
            started,
            step_result(result, |ok| format!("destroyed={}", ok)),
        );
    }

    println!(
        "  summary: {} passed, {} skipped, {} failed\n",
        suite.passed, suite.skipped, suite.failed
    );
    suite.failed == 0
}
