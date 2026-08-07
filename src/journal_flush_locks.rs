//! Per-run serialization of durable journal writes.
//!
//! # Why per-run and not global
//!
//! Durable checkpoints (`run.started`, `step.completed`, `run.completed`, state
//! snapshots) must not overtake transient events already drained for **the same
//! run** — that ordering is what makes replay correct. Ordering *between*
//! different runs carries no such requirement.
//!
//! This used to be enforced with a single process-wide mutex held across the
//! engine round-trip. That made every durable event in the worker serialize
//! behind every other one: with N in-flight invocations each emitting k durable
//! events, a worker performed N*k strictly sequential network calls, so slots
//! stayed occupied for minutes at near-zero CPU while dispatch saw the worker as
//! full (AGNT5-953).
//!
//! Locking per run keeps the ordering guarantee exactly where it is needed and
//! lets independent runs write concurrently.
//!
//! # Deadlock safety
//!
//! The checkpoint path takes **one** run lock and never acquires a second while
//! holding it. The flush task may hold several at once and always acquires them
//! in sorted order. A task holding a single lock and never requesting another
//! cannot participate in a cycle, so the two paths cannot deadlock.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::{Mutex as TokioMutex, OwnedMutexGuard};

/// Above this many tracked runs, `lock_run` opportunistically drops entries no
/// task is holding or waiting on. Runs are normally retired explicitly via
/// [`JournalFlushLocks::retire_run`] when they reach a terminal event; this is
/// only a backstop for runs that end without one (crash, cancellation).
const GC_THRESHOLD: usize = 1024;

/// Registry of per-run flush locks.
#[derive(Debug, Default)]
pub struct JournalFlushLocks {
    locks: Mutex<HashMap<String, Arc<TokioMutex<()>>>>,
}

impl JournalFlushLocks {
    pub fn new() -> Self {
        Self::default()
    }

    fn handle(&self, run_id: &str) -> Arc<TokioMutex<()>> {
        let mut locks = match self.locks.lock() {
            Ok(locks) => locks,
            // A poisoned registry must not take down event emission; fall back
            // to a detached lock, which degrades to "no cross-task ordering for
            // this run" rather than a panic.
            Err(poisoned) => poisoned.into_inner(),
        };

        if locks.len() > GC_THRESHOLD {
            // Strong count 1 means only the map holds it: no guard is alive and
            // no task is parked waiting for it.
            locks.retain(|_, handle| Arc::strong_count(handle) > 1);
        }

        locks
            .entry(run_id.to_string())
            .or_insert_with(|| Arc::new(TokioMutex::new(())))
            .clone()
    }

    /// Acquire the flush lock for a single run.
    ///
    /// Callers must not acquire a second run lock while holding this guard —
    /// see the deadlock note in the module docs.
    pub async fn lock_run(&self, run_id: &str) -> OwnedMutexGuard<()> {
        self.handle(run_id).lock_owned().await
    }

    /// Acquire flush locks for several runs at once, in a deterministic order.
    ///
    /// Used by the flush task, which drains a batch spanning runs. Sorting the
    /// ids gives every multi-lock caller the same acquisition order.
    pub async fn lock_runs(&self, run_ids: &[String]) -> Vec<OwnedMutexGuard<()>> {
        let mut ordered: Vec<&String> = run_ids.iter().collect();
        ordered.sort_unstable();
        ordered.dedup();

        let mut guards = Vec::with_capacity(ordered.len());
        for run_id in ordered {
            guards.push(self.handle(run_id).lock_owned().await);
        }
        guards
    }

    /// Forget a run's lock once it has reached a terminal event.
    pub fn retire_run(&self, run_id: &str) {
        let mut locks = match self.locks.lock() {
            Ok(locks) => locks,
            Err(poisoned) => poisoned.into_inner(),
        };
        // Only retire when nothing else references it. A guard or a waiter
        // still holding the Arc means another task is mid-write for this run.
        if let Some(handle) = locks.get(run_id) {
            if Arc::strong_count(handle) == 1 {
                locks.remove(run_id);
            }
        }
    }

    /// Number of runs currently tracked. Test/diagnostic use.
    pub fn tracked_runs(&self) -> usize {
        self.locks.lock().map(|l| l.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Two different runs must be able to hold their locks simultaneously —
    /// this is the property the global mutex did not provide.
    #[tokio::test]
    async fn different_runs_do_not_block_each_other() {
        let locks = Arc::new(JournalFlushLocks::new());
        let a = locks.lock_run("run-a").await;

        // Would hang forever under a global lock.
        let b = tokio::time::timeout(Duration::from_secs(1), locks.lock_run("run-b")).await;
        assert!(b.is_ok(), "a second run must not wait on the first");
        drop(a);
    }

    /// The same run must still serialize, or a checkpoint could overtake
    /// earlier events for that run.
    #[tokio::test]
    async fn same_run_serializes() {
        let locks = Arc::new(JournalFlushLocks::new());
        let held = locks.lock_run("run-a").await;

        let contended =
            tokio::time::timeout(Duration::from_millis(200), locks.lock_run("run-a")).await;
        assert!(contended.is_err(), "same run must wait");

        drop(held);
        let acquired = tokio::time::timeout(Duration::from_secs(1), locks.lock_run("run-a")).await;
        assert!(acquired.is_ok(), "lock must be available once released");
    }

    /// Writers on one run must observe each other's ordering.
    #[tokio::test]
    async fn same_run_writes_are_ordered() {
        let locks = Arc::new(JournalFlushLocks::new());
        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        for _ in 0..16 {
            let locks = locks.clone();
            let counter = counter.clone();
            handles.push(tokio::spawn(async move {
                let _g = locks.lock_run("run-a").await;
                let entered = counter.fetch_add(1, Ordering::SeqCst);
                // If the lock did not hold, another task would bump the counter
                // inside this window.
                tokio::time::sleep(Duration::from_millis(1)).await;
                assert_eq!(
                    counter.load(Ordering::SeqCst),
                    entered + 1,
                    "another writer entered the same run's critical section"
                );
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }
    }

    /// The flush task takes many locks at once; sorted acquisition plus
    /// single-lock checkpoint holders means no cycle is possible.
    #[tokio::test]
    async fn multi_run_acquisition_completes() {
        let locks = Arc::new(JournalFlushLocks::new());
        let runs: Vec<String> = (0..8).map(|i| format!("run-{i}")).collect();

        let flusher = {
            let locks = locks.clone();
            let runs = runs.clone();
            tokio::spawn(async move { locks.lock_runs(&runs).await })
        };
        let checkpoint = {
            let locks = locks.clone();
            tokio::spawn(async move {
                let _g = locks.lock_run("run-5").await;
            })
        };

        let both = tokio::time::timeout(Duration::from_secs(5), async {
            let guards = flusher.await.unwrap();
            drop(guards);
            checkpoint.await.unwrap();
        })
        .await;
        assert!(both.is_ok(), "flush task and checkpoint deadlocked");
    }

    #[tokio::test]
    async fn retire_frees_idle_runs_only() {
        let locks = JournalFlushLocks::new();
        let guard = locks.lock_run("run-a").await;
        locks.retire_run("run-a");
        assert_eq!(locks.tracked_runs(), 1, "held run must not be retired");
        drop(guard);
        locks.retire_run("run-a");
        assert_eq!(locks.tracked_runs(), 0, "idle run should be retired");
    }
}
