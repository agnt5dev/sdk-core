//! Cross-language golden parity tests for the builtin scorer fast path.
//!
//! Reads `test-fixtures/eval/builtin_goldens.json` (shared across
//! Rust / Python / TypeScript) and asserts each row produces the same
//! `(score, passed)` here as it does in the other two SDKs. Rows that
//! include a `label` field also enforce label equality.
//!
//! If a row breaks here but passes elsewhere, the Rust builtin has
//! drifted from the canonical behaviour — fix Rust, not the fixture.

use agnt5_sdk_core::eval::builtin_scorer::execute;
use serde_json::Value;

#[derive(Debug, serde::Deserialize)]
struct Goldens {
    cases: Vec<GoldenCase>,
}

#[derive(Debug, serde::Deserialize)]
struct GoldenCase {
    name: String,
    scorer: String,
    input: Value,
    expect: ExpectedResult,
}

#[derive(Debug, serde::Deserialize)]
struct ExpectedResult {
    score: f64,
    passed: bool,
    #[serde(default)]
    label: Option<String>,
}

fn load_goldens() -> Goldens {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("test-fixtures")
        .join("eval")
        .join("builtin_goldens.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("parse goldens JSON")
}

#[test]
fn builtin_scorers_match_cross_language_goldens() {
    let goldens = load_goldens();
    let mut failures = Vec::new();
    for case in &goldens.cases {
        let payload = serde_json::to_string(&case.input).unwrap();
        let result = match execute(&case.scorer, payload.as_bytes()) {
            Some(r) => r,
            None => {
                failures.push(format!(
                    "[{}] scorer {:?} returned None from execute()",
                    case.name, case.scorer
                ));
                continue;
            }
        };
        if (result.score - case.expect.score).abs() > 1e-9 {
            failures.push(format!(
                "[{}] score mismatch: got {}, expected {}",
                case.name, result.score, case.expect.score
            ));
        }
        match (result.passed, Some(case.expect.passed)) {
            (Some(got), Some(want)) if got != want => {
                failures.push(format!(
                    "[{}] passed mismatch: got {}, expected {}",
                    case.name, got, want
                ));
            }
            (None, _) => {
                failures.push(format!(
                    "[{}] passed is None; goldens require a concrete bool",
                    case.name
                ));
            }
            _ => {}
        }
        if let Some(expected_label) = &case.expect.label {
            match &result.label {
                Some(got) if got != expected_label => {
                    failures.push(format!(
                        "[{}] label mismatch: got {:?}, expected {:?}",
                        case.name, got, expected_label
                    ));
                }
                None => {
                    failures.push(format!(
                        "[{}] label missing: expected {:?}",
                        case.name, expected_label
                    ));
                }
                _ => {}
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} golden case(s) failed:\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}
