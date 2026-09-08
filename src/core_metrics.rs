//! Worker observations for the core-metrics contract. No scheduling decisions,
//! journal writes, or run identifiers in metric labels live here.
use opentelemetry::{global, metrics::Histogram, KeyValue};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

struct Clock {
    monotonic: Instant,
    unix_ms: f64,
    incarnation: String,
}

fn clock() -> &'static Clock {
    static CLOCK: OnceLock<Clock> = OnceLock::new();
    CLOCK.get_or_init(|| Clock {
        monotonic: Instant::now(),
        unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64()
            * 1000.0,
        incarnation: uuid::Uuid::new_v4().to_string(),
    })
}

/// A shared native/language monotonic clock anchored once for cross-process
/// correlation. Durations never depend on subsequent wall-clock adjustments.
pub fn now_ms() -> f64 {
    clock().unix_ms + clock().monotonic.elapsed().as_secs_f64() * 1000.0
}

#[derive(Clone)]
struct Execution {
    worker_id: String,
    generation: u64,
}

fn runs() -> &'static Mutex<HashMap<String, Execution>> {
    static RUNS: OnceLock<Mutex<HashMap<String, Execution>>> = OnceLock::new();
    RUNS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn emit(worker_id: &str, mut observation: Value) {
    observation["schema_version"] = json!(1);
    observation["worker_id"] = json!(worker_id);
    observation["instance_id"] = json!(format!("{}:{}", clock().incarnation, worker_id));
    tracing::info!(target: "agnt5.core_metrics", "AGNT5_CORE_METRIC {}", observation);
}

pub(crate) fn configured(worker_id: &str, max_slots: usize) {
    emit(
        worker_id,
        json!({"event": "configured", "at_ms": now_ms(), "max_slots": max_slots}),
    );
}

pub(crate) fn worker_event(
    event: &'static str,
    worker_id: &str,
    run_id: &str,
    slot_id: usize,
    generation: u64,
    at_ms: f64,
) {
    // Lifetime is bounded by the actual active pull slots, including unwind.
    if event == "claimed" {
        runs()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                run_id.to_owned(),
                Execution {
                    worker_id: worker_id.to_owned(),
                    generation,
                },
            );
    }
    emit(
        worker_id,
        json!({"event": event, "at_ms": at_ms, "run_id": run_id, "slot_id": slot_id,
        "execution_id": format!("{worker_id}:{generation}")}),
    );
    if event == "released" {
        let mut runs = runs()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if runs
            .get(run_id)
            .is_some_and(|entry| entry.worker_id == worker_id && entry.generation == generation)
        {
            runs.remove(run_id);
        }
    }
}

pub(crate) fn acknowledged(run_id: &str) {
    let execution = runs()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(run_id)
        .cloned();
    if let Some(execution) = execution {
        emit(
            &execution.worker_id,
            json!({"event": "acknowledged", "run_id": run_id, "at_ms": now_ms(),
            "execution_id": format!("{}:{}", execution.worker_id, execution.generation)}),
        );
    }
}

fn timing_observation(
    run_id: &str,
    phase: &str,
    started_ms: f64,
    ended_ms: f64,
    outcome: &str,
) -> Option<Value> {
    if !matches!(phase, "business" | "begin" | "complete" | "fail")
        || !matches!(
            outcome,
            "success"
                | "error"
                | "cancelled"
                | "replay"
                | "execute"
                | "wait"
                | "conflict"
                | "unknown"
        )
        || !started_ms.is_finite()
        || !ended_ms.is_finite()
        || ended_ms < started_ms
    {
        return None;
    }
    Some(
        json!({"event": if phase == "business" { "business" } else { "activation_rpc" },
        "run_id": run_id, "operation": phase, "outcome": outcome,
        "at_ms": ended_ms, "start_ms": started_ms, "end_ms": ended_ms,
        "elapsed_ms": ended_ms - started_ms}),
    )
}

/// Includes native RPC retries/backoff; cancellation is recorded on drop.
pub(crate) struct RpcTimer {
    run_id: String,
    operation: &'static str,
    started_ms: f64,
    outcome: &'static str,
}

impl RpcTimer {
    pub(crate) fn new(run_id: &str, operation: &'static str) -> Self {
        Self {
            run_id: run_id.to_owned(),
            operation,
            started_ms: now_ms(),
            outcome: "cancelled",
        }
    }

    pub(crate) fn outcome(&mut self, outcome: &'static str) {
        self.outcome = outcome;
    }
}

impl Drop for RpcTimer {
    fn drop(&mut self) {
        record_timing(&self.run_id, self.operation, self.started_ms, self.outcome);
    }
}

/// Called by language bindings after a measured body or activation operation.
/// Telemetry never changes the operation's result or durable retry identity.
pub fn record_timing(run_id: &str, phase: &str, started_ms: f64, outcome: &str) {
    let Some(mut observation) = timing_observation(run_id, phase, started_ms, now_ms(), outcome)
    else {
        return;
    };
    let execution = runs()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(run_id)
        .cloned();
    let Some(execution) = execution else {
        return;
    };
    let worker_id = execution.worker_id;
    observation["execution_id"] = json!(format!("{worker_id}:{}", execution.generation));
    static BODY: OnceLock<Histogram<f64>> = OnceLock::new();
    static ACK: OnceLock<Histogram<f64>> = OnceLock::new();
    let (instrument, name) = if phase == "business" {
        (&BODY, "agnt5.worker.activation.business.duration_ms")
    } else {
        (&ACK, "agnt5.worker.activation.acknowledgment.duration_ms")
    };
    let histogram = instrument.get_or_init(|| {
        global::meter("agnt5-sdk-core")
            .f64_histogram(name)
            .with_unit("ms")
            .with_boundaries(vec![
                1., 5., 10., 20., 50., 100., 250., 500., 1000., 5000., 30000., 60000.,
            ])
            .build()
    });
    histogram.record(
        observation["elapsed_ms"].as_f64().unwrap_or_default(),
        &[
            KeyValue::new("operation", phase.to_owned()),
            KeyValue::new("result", outcome.to_owned()),
            KeyValue::new("agnt5.worker.id", worker_id.clone()),
        ],
    );
    emit(&worker_id, observation);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_actual_body_interval_and_separate_acknowledgment() {
        let body = timing_observation("run", "business", 100., 1200., "success").unwrap();
        assert_eq!(body["elapsed_ms"], 1100.);
        assert_eq!(body["event"], "business");
        let ack = timing_observation("run", "complete", 1200., 1250., "error").unwrap();
        assert_eq!(ack["event"], "activation_rpc");
        assert_eq!(ack["outcome"], "error");
    }

    #[test]
    fn invalid_measurements_do_not_emit_or_affect_execution() {
        assert!(timing_observation("run", "business", f64::NAN, 2., "success").is_none());
        assert!(timing_observation("run", "business", 3., 2., "success").is_none());
        assert!(timing_observation("run", "arbitrary_label", 1., 2., "success").is_none());
    }

    #[test]
    fn stale_release_does_not_erase_new_execution() {
        worker_event("claimed", "worker", "generation-test", 0, 10, 1.);
        worker_event("claimed", "worker", "generation-test", 1, 11, 2.);
        worker_event("released", "worker", "generation-test", 0, 10, 3.);
        assert_eq!(
            runs()
                .lock()
                .unwrap()
                .get("generation-test")
                .unwrap()
                .generation,
            11
        );
        worker_event("released", "worker", "generation-test", 1, 11, 4.);
        assert!(!runs().lock().unwrap().contains_key("generation-test"));
    }
}
