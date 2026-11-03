//! One ordered writer per run, shared by flush barriers and acknowledged writes.
use super::{JournalQueueConfig, Result, SdkError, Worker};
use prost::Message;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

// Keep combined messages below the ordinary gRPC receive limit. A larger
// individual record retains the existing single-Append behavior.
const MAX_BATCH_BYTES: usize = 1_048_576;

pub(super) struct CheckpointWrite {
    pub record: crate::pb::Record,
    pub timeout_ms: u64,
    pub sequence: i64,
    pub batch_size: usize,
    pub batchable: bool,
    pub compatibility: HashMap<String, String>,
    pub queued_at: std::time::Instant,
}

impl CheckpointWrite {
    fn compatible(&self, next: &Self) -> bool {
        self.batchable
            && next.batchable
            && self.timeout_ms == next.timeout_ms
            && self.record.project_id == next.record.project_id
            && self.compatibility == next.compatibility
    }
}

struct PendingCheckpoint {
    write: CheckpointWrite,
    reply: oneshot::Sender<std::result::Result<(), Arc<SdkError>>>,
    _permit: OwnedSemaphorePermit,
}

enum Entry {
    Barrier(oneshot::Sender<RunFlushGuard>),
    Checkpoint(Box<PendingCheckpoint>),
}

#[derive(Default)]
struct QueueState {
    entries: VecDeque<Entry>,
    running: bool,
    worker: Option<Arc<Worker>>,
}

#[derive(Default)]
struct RunQueue(Mutex<QueueState>);

impl RunQueue {
    fn enqueue(self: &Arc<Self>, entry: Entry, worker: Option<&Worker>) {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if state.worker.is_none() {
            state.worker = worker.map(|worker| Arc::new(worker.clone()));
        }
        state.entries.push_back(entry);
        if !state.running {
            state.running = true;
            tokio::spawn(self.clone().run());
        }
    }

    async fn run(self: Arc<Self>) {
        loop {
            let entry = {
                let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
                match state.entries.pop_front() {
                    Some(entry) => entry,
                    None => {
                        state.running = false;
                        return;
                    }
                }
            };
            match entry {
                Entry::Barrier(reply) => {
                    let (release, released) = oneshot::channel();
                    // Dropping a cancelled receiver also drops the guard. The
                    // next operation cannot run until the granted guard drops.
                    let _ = reply.send(RunFlushGuard { _release: release });
                    let _ = released.await;
                }
                Entry::Checkpoint(first) => {
                    if first.reply.is_closed() {
                        continue;
                    }
                    let mut bytes = first.write.record.encoded_len();
                    let mut batch = vec![first];
                    {
                        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
                        while batch.len() < batch[0].write.batch_size.max(1) {
                            let Some(Entry::Checkpoint(next)) = state.entries.front() else {
                                break;
                            };
                            if next.reply.is_closed() {
                                state.entries.pop_front();
                                continue;
                            }
                            let next_bytes = next.write.record.encoded_len();
                            if !batch[0].write.compatible(&next.write)
                                || bytes.saturating_add(next_bytes) > MAX_BATCH_BYTES
                            {
                                break;
                            }
                            bytes += next_bytes;
                            if let Some(Entry::Checkpoint(next)) = state.entries.pop_front() {
                                batch.push(next);
                            }
                        }
                    }
                    let worker = self
                        .0
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .worker
                        .as_ref()
                        .expect("checkpoint queue has no worker")
                        .clone();
                    let writes: Vec<_> = batch.iter().map(|pending| &pending.write).collect();
                    // Persistence belongs to the writer, rather than to any
                    // one waiter: cancellation cannot abandon an in-flight batch.
                    let result = worker
                        .persist_checkpoint_writes(&writes)
                        .await
                        .map_err(Arc::new);
                    if batch.len() == 1 {
                        let _ = batch.pop().unwrap().reply.send(result);
                    } else {
                        for pending in batch {
                            let _ = pending.reply.send(result.clone());
                        }
                    }
                }
            }
        }
    }
}

pub(super) struct RunFlushGuard {
    _release: oneshot::Sender<()>,
}

#[derive(Clone)]
pub(super) struct RunFlushLocks {
    runs: Arc<Mutex<HashMap<String, Weak<RunQueue>>>>,
    capacity: Arc<Semaphore>,
}

impl Default for RunFlushLocks {
    fn default() -> Self {
        Self {
            runs: Default::default(),
            capacity: Arc::new(Semaphore::new(
                JournalQueueConfig::from_env().max_size.max(1),
            )),
        }
    }
}

impl RunFlushLocks {
    #[cfg(test)]
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            runs: Default::default(),
            capacity: Arc::new(Semaphore::new(capacity)),
        }
    }

    fn queue(&self, run_id: &str) -> Arc<RunQueue> {
        let mut runs = self.runs.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(queue) = runs.get(run_id).and_then(Weak::upgrade) {
            return queue;
        }
        if runs.len() >= 4096 {
            runs.retain(|_, queue| queue.strong_count() > 0);
        }
        let queue = Arc::new(RunQueue::default());
        runs.insert(run_id.to_string(), Arc::downgrade(&queue));
        queue
    }

    pub async fn lock_run(&self, run_id: &str) -> RunFlushGuard {
        let (reply, granted) = oneshot::channel();
        self.queue(run_id).enqueue(Entry::Barrier(reply), None);
        granted
            .await
            .expect("run writer stopped before granting flush barrier")
    }

    pub async fn lock_runs<I: IntoIterator<Item = String>>(
        &self,
        run_ids: I,
    ) -> Vec<RunFlushGuard> {
        let mut run_ids: Vec<_> = run_ids.into_iter().collect();
        run_ids.sort_unstable();
        run_ids.dedup();
        let mut guards = Vec::with_capacity(run_ids.len());
        for run_id in run_ids {
            guards.push(self.lock_run(&run_id).await);
        }
        guards
    }

    pub async fn checkpoint(&self, worker: &Worker, write: CheckpointWrite) -> Result<()> {
        let permit =
            self.capacity
                .clone()
                .try_acquire_owned()
                .map_err(|_| SdkError::Connection {
                    message: "acknowledged checkpoint queue is full".into(),
                    code: crate::error::ErrorCode::ResourceExhausted,
                    source: None,
                })?;
        let (reply, ack) = oneshot::channel();
        self.queue(&write.record.run_id).enqueue(
            Entry::Checkpoint(Box::new(PendingCheckpoint {
                write,
                reply,
                _permit: permit,
            })),
            Some(worker),
        );
        match ack.await.map_err(|_| {
            SdkError::Internal(
                "checkpoint writer stopped before acknowledgment; persistence outcome is unknown"
                    .into(),
            )
        })? {
            Ok(()) => Ok(()),
            Err(error) => Err(Arc::try_unwrap(error).unwrap_or_else(copy_error)),
        }
    }
}

#[derive(Debug)]
struct SharedFailure(Arc<SdkError>);
impl std::fmt::Display for SharedFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for SharedFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

// Engine append errors have structured status/timeout fields. Retain these for
// every waiter, including status details used to distinguish permanent rejects.
fn copy_error(error: Arc<SdkError>) -> SdkError {
    match error.as_ref() {
        SdkError::Timeout {
            message,
            operation,
            duration_ms,
        } => SdkError::Timeout {
            message: message.clone(),
            operation: operation.clone(),
            duration_ms: *duration_ms,
        },
        SdkError::Connection {
            message,
            code,
            source,
        } => SdkError::Connection {
            message: message.clone(),
            code: *code,
            source: source.as_ref().map(|source| {
                if let Some(status) = source.downcast_ref::<tonic::Status>() {
                    Box::new(status.clone()) as Box<dyn std::error::Error + Send + Sync>
                } else {
                    Box::new(SharedFailure(error.clone()))
                }
            }),
        },
        SdkError::Internal(message) => SdkError::Internal(message.clone()),
        SdkError::InvalidArgument { message, argument } => SdkError::InvalidArgument {
            message: message.clone(),
            argument: argument.clone(),
        },
        SdkError::Configuration { message, field } => SdkError::Configuration {
            message: message.clone(),
            field: field.clone(),
        },
        SdkError::Status(status) => SdkError::Status(status.clone()),
        _ => SdkError::Connection {
            message: error.to_string(),
            code: error.code(),
            source: Some(Box::new(SharedFailure(error))),
        },
    }
}
