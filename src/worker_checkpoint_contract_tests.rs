//! Exercise acknowledged lifecycle writes against the current Engine service.
use super::*;
use crate::pb::{AppendRequest, AppendResponse, Record};
use std::convert::Infallible;
use std::task::{Context, Poll};
use tonic::codegen::{BoxFuture, Service};

struct PendingAppend {
    records: Vec<Record>,
    ack: tokio::sync::oneshot::Sender<std::result::Result<(), tonic::Status>>,
}

#[derive(Clone)]
struct GatedEngine {
    calls: tokio::sync::mpsc::UnboundedSender<PendingAppend>,
    persisted: Arc<std::sync::Mutex<Vec<Record>>>,
}

impl GatedEngine {
    async fn persist(&self, records: Vec<Record>) -> std::result::Result<Vec<u64>, tonic::Status> {
        let (ack, response) = tokio::sync::oneshot::channel();
        self.calls
            .send(PendingAppend {
                records: records.clone(),
                ack,
            })
            .map_err(|_| tonic::Status::cancelled("test closed"))?;
        let outcome = response
            .await
            .map_err(|_| tonic::Status::cancelled("ack dropped"))?;
        let lost_ack = outcome
            .as_ref()
            .is_err_and(|status| status.message() == "lost acknowledgment after commit");
        if !lost_ack {
            outcome.as_ref().map_err(Clone::clone)?;
        }
        let mut persisted = self.persisted.lock().unwrap();
        let offsets = (persisted.len() + 1..=persisted.len() + records.len())
            .map(|offset| offset as u64)
            .collect();
        persisted.extend(records);
        if lost_ack {
            return Err(outcome.unwrap_err());
        }
        Ok(offsets)
    }
}

impl tonic::server::NamedService for GatedEngine {
    const NAME: &'static str = "api.v1.EngineService";
}

impl tonic::server::UnaryService<AppendRequest> for GatedEngine {
    type Response = AppendResponse;
    type Future = BoxFuture<tonic::Response<AppendResponse>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<AppendRequest>) -> Self::Future {
        let engine = self.clone();
        Box::pin(async move {
            let record = request.into_inner().record.unwrap();
            let offsets = engine.persist(vec![record]).await?;
            Ok(tonic::Response::new(AppendResponse {
                offset: offsets[0],
                timestamp_ns: 1,
            }))
        })
    }
}

impl tonic::server::UnaryService<crate::pb::AppendBatchRequest> for GatedEngine {
    type Response = crate::pb::AppendBatchResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<crate::pb::AppendBatchRequest>) -> Self::Future {
        let engine = self.clone();
        Box::pin(async move {
            let records = request.into_inner().records;
            let malformed = records
                .iter()
                .any(|record| record.metadata.contains_key("malformed_ack"));
            let mut offsets = engine.persist(records).await?;
            if malformed {
                offsets.truncate(1);
            }
            Ok(tonic::Response::new(crate::pb::AppendBatchResponse {
                written_count: offsets.len() as i32,
                offsets,
            }))
        })
    }
}

impl Service<http::Request<tonic::body::Body>> for GatedEngine {
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let service = self.clone();
        Box::pin(async move {
            match request.uri().path() {
                "/api.v1.EngineService/Append" => {
                    let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::<
                        AppendResponse,
                        AppendRequest,
                    >::default());
                    Ok(grpc.unary(service, request).await)
                }
                "/api.v1.EngineService/AppendBatch" => {
                    let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::<
                        crate::pb::AppendBatchResponse,
                        crate::pb::AppendBatchRequest,
                    >::default());
                    Ok(grpc.unary(service, request).await)
                }
                _ => Ok(tonic::Status::unimplemented("test RPC").into_http()),
            }
        })
    }
}

async fn gated_worker() -> (
    Worker,
    tokio::sync::mpsc::UnboundedReceiver<PendingAppend>,
    Arc<std::sync::Mutex<Vec<Record>>>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (calls, receiver) = tokio::sync::mpsc::unbounded_channel();
    let persisted = Arc::new(std::sync::Mutex::new(Vec::new()));
    let engine = GatedEngine {
        calls,
        persisted: persisted.clone(),
    };
    let incoming = async_stream::stream! {
        loop { yield listener.accept().await.map(|(socket, _)| socket); }
    };
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(engine)
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    let worker = Worker::new(
        WorkerConfig {
            service_name: "checkpoint-batching".into(),
            service_version: "test".into(),
            service_type: "standalone".into(),
            worker_id: "worker".into(),
            coordinator_endpoint: endpoint.clone(),
            ee_endpoint: "http://127.0.0.1:1".into(),
            engine_endpoint: Some(endpoint),
            max_retries: 0,
            max_concurrency: Some(32),
        },
        vec![],
        HashMap::from([
            ("project_id".into(), "project".into()),
            ("dispatch_mode".into(), "pull".into()),
        ]),
    );
    (worker, receiver, persisted, server)
}

fn checkpoint(worker: &Worker, run: &str, index: i64) -> tokio::task::JoinHandle<Result<()>> {
    let worker = worker.clone();
    let run = run.to_string();
    tokio::spawn(async move {
        worker
            .emit_checkpoint_sync(
                run,
                "function.started".into(),
                b"{}".to_vec(),
                index,
                HashMap::from([("cid".into(), index.to_string())]),
                index,
                5000,
            )
            .await
    })
}

async fn next_append(
    calls: &mut tokio::sync::mpsc::UnboundedReceiver<PendingAppend>,
) -> PendingAppend {
    tokio::time::timeout(Duration::from_secs(5), calls.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn concurrent_checkpoints_share_ack_roundtrip() {
    let (worker, mut calls, persisted, server) = gated_worker().await;
    let first = checkpoint(&worker, "run", 0);
    let first_call = next_append(&mut calls).await;
    let remaining: Vec<_> = (1..11).map(|i| checkpoint(&worker, "run", i)).collect();
    tokio::task::yield_now().await;
    assert!(!first.is_finished());
    assert!(remaining.iter().all(|task| !task.is_finished()));
    assert!(calls.try_recv().is_err(), "same-run writes stay ordered");
    first_call.ack.send(Ok(())).unwrap();
    first.await.unwrap().unwrap();
    let batch = next_append(&mut calls).await;
    assert_eq!(
        batch.records.len(),
        10,
        "waiting starts should share one durable batch"
    );
    assert!(
        remaining.iter().all(|task| !task.is_finished()),
        "no caller may pass its own ACK"
    );
    batch.ack.send(Ok(())).unwrap();
    for task in remaining {
        task.await.unwrap().unwrap();
    }
    assert_eq!(persisted.lock().unwrap().len(), 11);
    server.abort();
}

#[tokio::test]
async fn checkpoint_batch_cannot_cross_a_flush_barrier() {
    let (worker, mut calls, _, server) = gated_worker().await;
    let first = checkpoint(&worker, "run", 0);
    let first_call = next_append(&mut calls).await;
    let second = checkpoint(&worker, "run", 1);
    tokio::task::yield_now().await;
    let locks = worker.journal_flush_locks.clone();
    let barrier = tokio::spawn(async move { locks.lock_run("run").await });
    tokio::task::yield_now().await;
    let third = checkpoint(&worker, "run", 2);
    tokio::task::yield_now().await;
    first_call.ack.send(Ok(())).unwrap();
    first.await.unwrap().unwrap();
    let second_call = next_append(&mut calls).await;
    assert_eq!(second_call.records.len(), 1);
    assert_eq!(second_call.records[0].correlation_id, "1");
    second_call.ack.send(Ok(())).unwrap();
    second.await.unwrap().unwrap();
    let guard = barrier.await.unwrap();
    assert!(
        calls.try_recv().is_err(),
        "later checkpoint overtook the flush barrier"
    );
    drop(guard);
    let third_call = next_append(&mut calls).await;
    assert_eq!(third_call.records[0].correlation_id, "2");
    third_call.ack.send(Ok(())).unwrap();
    third.await.unwrap().unwrap();
    server.abort();
}

#[tokio::test]
async fn cancelled_checkpoint_settles_before_later_writes() {
    let (worker, mut calls, persisted, server) = gated_worker().await;
    let first = checkpoint(&worker, "run", 0);
    let first_call = next_append(&mut calls).await;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let later = checkpoint(&worker, "run", 1);
    tokio::task::yield_now().await;
    assert!(calls.try_recv().is_err());
    first_call.ack.send(Ok(())).unwrap();
    let later_call = next_append(&mut calls).await;
    later_call.ack.send(Ok(())).unwrap();
    later.await.unwrap().unwrap();
    assert_eq!(
        persisted
            .lock()
            .unwrap()
            .iter()
            .map(|r| r.correlation_id.as_str())
            .collect::<Vec<_>>(),
        ["0", "1"]
    );
    server.abort();
}

#[tokio::test]
async fn cancelled_queued_checkpoint_is_not_sent() {
    let (worker, mut calls, persisted, server) = gated_worker().await;
    let first = checkpoint(&worker, "run", 0);
    let first_call = next_append(&mut calls).await;
    let cancelled = checkpoint(&worker, "run", 1);
    tokio::task::yield_now().await;
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());
    let later = checkpoint(&worker, "run", 2);
    tokio::task::yield_now().await;
    first_call.ack.send(Ok(())).unwrap();
    first.await.unwrap().unwrap();
    let call = next_append(&mut calls).await;
    assert_eq!(call.records.len(), 1);
    assert_eq!(call.records[0].correlation_id, "2");
    call.ack.send(Ok(())).unwrap();
    later.await.unwrap().unwrap();
    assert_eq!(persisted.lock().unwrap().len(), 2);
    server.abort();
}

#[tokio::test]
async fn other_runs_progress_while_checkpoint_ack_is_pending() {
    let (worker, mut calls, _, server) = gated_worker().await;
    let first = checkpoint(&worker, "run-a", 0);
    let first_call = next_append(&mut calls).await;
    let second = checkpoint(&worker, "run-b", 1);
    let second_call = next_append(&mut calls).await;
    assert_eq!(second_call.records[0].run_id, "run-b");
    second_call.ack.send(Ok(())).unwrap();
    second.await.unwrap().unwrap();
    assert!(!first.is_finished());
    first_call.ack.send(Ok(())).unwrap();
    first.await.unwrap().unwrap();
    server.abort();
}

#[tokio::test]
async fn rejected_batch_reports_status_to_every_waiter() {
    let (worker, mut calls, persisted, server) = gated_worker().await;
    let first = checkpoint(&worker, "run", 0);
    let first_call = next_append(&mut calls).await;
    let tasks: Vec<_> = (1..4).map(|i| checkpoint(&worker, "run", i)).collect();
    tokio::task::yield_now().await;
    first_call.ack.send(Ok(())).unwrap();
    first.await.unwrap().unwrap();
    let batch = next_append(&mut calls).await;
    assert_eq!(batch.records.len(), 3);
    batch
        .ack
        .send(Err(tonic::Status::failed_precondition(
            "stale execution authority",
        )))
        .unwrap();
    for task in tasks {
        let error = task.await.unwrap().unwrap_err();
        let SdkError::Connection {
            source: Some(source),
            ..
        } = error
        else {
            panic!("lost Engine error type");
        };
        assert_eq!(
            source.downcast_ref::<tonic::Status>().unwrap().code(),
            tonic::Code::FailedPrecondition
        );
    }
    assert_eq!(persisted.lock().unwrap().len(), 1);
    server.abort();
}

#[tokio::test]
async fn queued_prefix_is_acknowledged_before_checkpoint() {
    let (worker, mut calls, persisted, server) = gated_worker().await;
    let mut older =
        JournalEventMessage::new("run".into(), "function.started".into(), b"{}".to_vec());
    older.correlation_id = "older".into();
    worker.journal_queue.push(older).unwrap();
    let later = checkpoint(&worker, "run", 1);
    let prefix = next_append(&mut calls).await;
    assert_eq!(prefix.records[0].correlation_id, "older");
    assert!(!later.is_finished());
    assert!(calls.try_recv().is_err());
    prefix.ack.send(Ok(())).unwrap();
    let checkpoint_call = next_append(&mut calls).await;
    assert_eq!(checkpoint_call.records[0].correlation_id, "1");
    checkpoint_call.ack.send(Ok(())).unwrap();
    later.await.unwrap().unwrap();
    assert_eq!(persisted.lock().unwrap().len(), 2);
    server.abort();
}

#[tokio::test]
async fn checkpoint_timeout_fails_all_waiters_and_blocks_later_boundaries() {
    let (worker, mut calls, _, server) = gated_worker().await;
    let first = checkpoint(&worker, "run", 0);
    let first_call = next_append(&mut calls).await;
    let mut tasks = Vec::new();
    for index in 1..4 {
        let worker = worker.clone();
        tasks.push(tokio::spawn(async move {
            worker
                .emit_checkpoint_sync(
                    "run".into(),
                    "function.started".into(),
                    b"{}".to_vec(),
                    index,
                    HashMap::from([("cid".into(), index.to_string())]),
                    index,
                    20,
                )
                .await
        }));
    }
    tokio::task::yield_now().await;
    first_call.ack.send(Ok(())).unwrap();
    first.await.unwrap().unwrap();
    let batch = next_append(&mut calls).await;
    assert_eq!(batch.records.len(), 3);
    for task in tasks {
        assert!(matches!(
            task.await.unwrap().unwrap_err(),
            SdkError::Timeout { .. }
        ));
    }
    assert!(checkpoint(&worker, "run", 4).await.unwrap().is_err());
    assert!(worker
        .flush_run_events_before_durable_write("run")
        .await
        .is_err());
    assert!(worker
        .emit_checkpoint_batch(vec![(
            "run".into(),
            "run.completed".into(),
            b"{}".to_vec(),
            5,
            HashMap::new(),
            5
        )])
        .await
        .is_err());
    assert!(calls.try_recv().is_err());
    drop(batch);
    server.abort();
}

#[tokio::test]
async fn checkpoints_with_different_authority_are_not_combined() {
    let (worker, mut calls, _, server) = gated_worker().await;
    let first = checkpoint(&worker, "run", 0);
    let first_call = next_append(&mut calls).await;
    let mut tasks = Vec::new();
    for authority in ["lease-a", "lease-b"] {
        let worker = worker.clone();
        tasks.push(tokio::spawn(async move {
            worker
                .emit_checkpoint_sync(
                    "run".into(),
                    "function.started".into(),
                    b"{}".to_vec(),
                    1,
                    HashMap::from([("lease_id".into(), authority.into())]),
                    1,
                    5000,
                )
                .await
        }));
    }
    tokio::task::yield_now().await;
    first_call.ack.send(Ok(())).unwrap();
    first.await.unwrap().unwrap();
    for authority in ["lease-a", "lease-b"] {
        let call = next_append(&mut calls).await;
        assert_eq!(call.records.len(), 1);
        assert_eq!(call.records[0].metadata.get("lease_id").unwrap(), authority);
        call.ack.send(Ok(())).unwrap();
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    server.abort();
}

#[tokio::test]
async fn malformed_batch_ack_never_releases_waiters_or_later_writes() {
    let (worker, mut calls, persisted, server) = gated_worker().await;
    let first = checkpoint(&worker, "run", 0);
    let first_call = next_append(&mut calls).await;
    let mut tasks = Vec::new();
    for index in 1..4 {
        let worker = worker.clone();
        tasks.push(tokio::spawn(async move {
            worker
                .emit_checkpoint_sync(
                    "run".into(),
                    "function.started".into(),
                    b"{}".to_vec(),
                    index,
                    HashMap::from([("malformed_ack".into(), "true".into())]),
                    index,
                    5000,
                )
                .await
        }));
    }
    tokio::task::yield_now().await;
    first_call.ack.send(Ok(())).unwrap();
    first.await.unwrap().unwrap();
    let batch = next_append(&mut calls).await;
    assert_eq!(batch.records.len(), 3);
    batch.ack.send(Ok(())).unwrap();
    for task in tasks {
        let error = task.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("cardinality mismatch"));
    }
    assert_eq!(
        persisted.lock().unwrap().len(),
        4,
        "bad ACK does not establish that storage rejected the write"
    );
    assert!(worker
        .flush_run_events_before_durable_write("run")
        .await
        .is_err());
    server.abort();
}

#[tokio::test]
async fn cancelled_waiter_does_not_abandon_shared_batch() {
    let (worker, mut calls, persisted, server) = gated_worker().await;
    let first = checkpoint(&worker, "run", 0);
    let first_call = next_append(&mut calls).await;
    let cancelled = checkpoint(&worker, "run", 1);
    let survivor = checkpoint(&worker, "run", 2);
    tokio::task::yield_now().await;
    first_call.ack.send(Ok(())).unwrap();
    first.await.unwrap().unwrap();
    let batch = next_append(&mut calls).await;
    assert_eq!(batch.records.len(), 2);
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());
    let later = checkpoint(&worker, "run", 3);
    tokio::task::yield_now().await;
    assert!(!survivor.is_finished());
    assert!(calls.try_recv().is_err());
    batch.ack.send(Ok(())).unwrap();
    survivor.await.unwrap().unwrap();
    let final_call = next_append(&mut calls).await;
    final_call.ack.send(Ok(())).unwrap();
    later.await.unwrap().unwrap();
    assert_eq!(persisted.lock().unwrap().len(), 4);
    server.abort();
}

#[tokio::test]
async fn checkpoint_batches_obey_the_configured_record_limit() {
    let (mut worker, mut calls, _, server) = gated_worker().await;
    worker.journal_queue = JournalEventQueue::new(JournalQueueConfig {
        batch_size: 2,
        ..Default::default()
    });
    let first = checkpoint(&worker, "run", 0);
    let first_call = next_append(&mut calls).await;
    let tasks: Vec<_> = (1..6).map(|i| checkpoint(&worker, "run", i)).collect();
    tokio::task::yield_now().await;
    first_call.ack.send(Ok(())).unwrap();
    first.await.unwrap().unwrap();
    for expected in [2, 2, 1] {
        let batch = next_append(&mut calls).await;
        assert_eq!(batch.records.len(), expected);
        batch.ack.send(Ok(())).unwrap();
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    server.abort();
}

#[tokio::test]
async fn prefix_flush_rejection_never_acknowledges_the_waiting_checkpoint() {
    let (worker, mut calls, persisted, server) = gated_worker().await;
    worker
        .journal_queue
        .push(JournalEventMessage::new(
            "run".into(),
            "function.started".into(),
            b"{}".to_vec(),
        ))
        .unwrap();
    let first = checkpoint(&worker, "run", 1);
    let prefix = next_append(&mut calls).await;
    let second = checkpoint(&worker, "run", 2);
    tokio::task::yield_now().await;
    prefix
        .ack
        .send(Err(tonic::Status::failed_precondition("prefix rejected")))
        .unwrap();
    assert!(first
        .await
        .unwrap()
        .unwrap_err()
        .to_string()
        .contains("prefix rejected"));
    // A known rejection contains no uncertain write. The later independent
    // checkpoint may proceed, but cannot be acknowledged before its own RPC.
    let later = next_append(&mut calls).await;
    assert!(!second.is_finished());
    later.ack.send(Ok(())).unwrap();
    second.await.unwrap().unwrap();
    assert_eq!(persisted.lock().unwrap().len(), 1);
    server.abort();
}

#[tokio::test]
async fn lost_batch_ack_retries_identical_records_before_later_writes() {
    let (worker, mut calls, persisted, server) = gated_worker().await;
    let first = checkpoint(&worker, "run", 0);
    let first_call = next_append(&mut calls).await;
    let tasks: Vec<_> = (1..4).map(|i| checkpoint(&worker, "run", i)).collect();
    tokio::task::yield_now().await;
    first_call.ack.send(Ok(())).unwrap();
    first.await.unwrap().unwrap();
    let batch = next_append(&mut calls).await;
    let original = batch.records.clone();
    batch
        .ack
        .send(Err(tonic::Status::unavailable(
            "lost acknowledgment after commit",
        )))
        .unwrap();
    let later = checkpoint(&worker, "run", 4);
    let retry = next_append(&mut calls).await;
    assert_eq!(
        retry.records, original,
        "retry must retain every record's identity and payload"
    );
    assert!(tasks.iter().all(|task| !task.is_finished()));
    assert!(!later.is_finished());
    assert_eq!(
        persisted.lock().unwrap().len(),
        4,
        "first attempt committed despite its lost ACK"
    );
    retry.ack.send(Ok(())).unwrap();
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    let final_call = next_append(&mut calls).await;
    assert_eq!(final_call.records.len(), 1);
    assert_eq!(final_call.records[0].correlation_id, "4");
    final_call.ack.send(Ok(())).unwrap();
    later.await.unwrap().unwrap();
    // Lifecycle record deduplication belongs to the engine. This transport
    // test deliberately records both attempts; it does not promise exactly-once effects.
    server.abort();
}

#[tokio::test]
async fn full_checkpoint_queue_rejects_without_dropping_accepted_writes() {
    let (mut worker, mut calls, persisted, server) = gated_worker().await;
    worker.journal_flush_locks = RunFlushLocks::with_capacity(2);
    let first = checkpoint(&worker, "run", 0);
    let first_call = next_append(&mut calls).await;
    let second = checkpoint(&worker, "run", 1);
    tokio::task::yield_now().await;
    let overflow = checkpoint(&worker, "run", 2).await.unwrap().unwrap_err();
    assert_eq!(overflow.code(), crate::error::ErrorCode::ResourceExhausted);
    assert!(!first.is_finished());
    assert!(!second.is_finished());
    first_call.ack.send(Ok(())).unwrap();
    first.await.unwrap().unwrap();
    let second_call = next_append(&mut calls).await;
    second_call.ack.send(Ok(())).unwrap();
    second.await.unwrap().unwrap();
    let fresh = checkpoint(&worker, "other-run", 3);
    let fresh_call = next_append(&mut calls).await;
    fresh_call.ack.send(Ok(())).unwrap();
    fresh.await.unwrap().unwrap();
    assert_eq!(persisted.lock().unwrap().len(), 3);
    server.abort();
}

#[tokio::test]
async fn large_records_remain_individual_and_keep_their_payloads() {
    let (worker, mut calls, persisted, server) = gated_worker().await;
    let first = checkpoint(&worker, "run", 0);
    let first_call = next_append(&mut calls).await;
    let payload = serde_json::to_vec(&serde_json::json!({"payload": "x".repeat(600_000)})).unwrap();
    let mut tasks = Vec::new();
    for index in 1..4 {
        let worker = worker.clone();
        let payload = payload.clone();
        tasks.push(tokio::spawn(async move {
            worker
                .emit_checkpoint_sync(
                    "run".into(),
                    "function.started".into(),
                    payload,
                    index,
                    HashMap::new(),
                    index,
                    5000,
                )
                .await
        }));
    }
    tokio::task::yield_now().await;
    first_call.ack.send(Ok(())).unwrap();
    first.await.unwrap().unwrap();
    for _ in 0..3 {
        let call = next_append(&mut calls).await;
        assert_eq!(call.records.len(), 1);
        assert_eq!(call.records[0].data, payload);
        call.ack.send(Ok(())).unwrap();
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    assert_eq!(persisted.lock().unwrap().len(), 4);
    server.abort();
}

#[tokio::test]
async fn run_completion_keeps_its_own_ack_barrier_after_function_batch() {
    let (worker, mut calls, persisted, server) = gated_worker().await;
    let first = checkpoint(&worker, "run", 0);
    let first_call = next_append(&mut calls).await;
    let tasks: Vec<_> = (1..3).map(|i| checkpoint(&worker, "run", i)).collect();
    let terminal_worker = worker.clone();
    let terminal = tokio::spawn(async move {
        terminal_worker
            .emit_checkpoint_sync(
                "run".into(),
                "run.completed".into(),
                b"{}".to_vec(),
                3,
                HashMap::new(),
                3,
                5000,
            )
            .await
    });
    tokio::task::yield_now().await;
    first_call.ack.send(Ok(())).unwrap();
    first.await.unwrap().unwrap();
    let batch = next_append(&mut calls).await;
    assert_eq!(batch.records.len(), 2);
    assert!(!terminal.is_finished());
    batch.ack.send(Ok(())).unwrap();
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    let completion = next_append(&mut calls).await;
    assert_eq!(completion.records.len(), 1);
    assert_eq!(completion.records[0].event_type, "run.completed");
    assert!(!terminal.is_finished());
    completion.ack.send(Ok(())).unwrap();
    terminal.await.unwrap().unwrap();
    assert_eq!(
        persisted.lock().unwrap().last().unwrap().event_type,
        "run.completed"
    );
    server.abort();
}

#[tokio::test]
async fn acknowledged_checkpoint_preserves_subtenant_and_event_identity() {
    let (worker, mut calls, _, server) = gated_worker().await;
    let task = tokio::spawn(async move {
        worker
            .emit_checkpoint_sync(
                "run".into(),
                "function.started".into(),
                b"{\"event_id\":\"event\"}".to_vec(),
                7,
                HashMap::from([
                    ("tenant_id".into(), "customer".into()),
                    ("correlation_id".into(), "child".into()),
                    ("parent_correlation_id".into(), "parent".into()),
                    ("lease_id".into(), "lease".into()),
                ]),
                123,
                5000,
            )
            .await
    });
    let call = next_append(&mut calls).await;
    let record = &call.records[0];
    assert_eq!(record.project_id, "project");
    assert_eq!(
        record.metadata.get("tenant_id").map(String::as_str),
        Some("customer")
    );
    assert_eq!(
        record.metadata.get("lease_id").map(String::as_str),
        Some("lease")
    );
    assert_eq!(record.correlation_id, "child");
    assert_eq!(record.parent_event_id, "parent");
    assert_eq!(record.timestamp_ns, 123);
    assert_eq!(record.data, b"{\"event_id\":\"event\"}");
    call.ack.send(Ok(())).unwrap();
    task.await.unwrap().unwrap();
    server.abort();
}

#[tokio::test]
async fn uncertain_push_checkpoint_stops_lease_renewal_without_masking_timeout() {
    let (mut worker, mut calls, _, server) = gated_worker().await;
    worker
        .metadata
        .insert("dispatch_mode".into(), "push".into());
    let client = WorkerCoordinatorClient::connect(worker.config.coordinator_endpoint.clone())
        .await
        .unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    worker
        .lease_renewal_stops
        .lock()
        .unwrap()
        .insert("run".into(), stop);
    let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cancelled_by_hook = cancelled.clone();
    worker.set_cancel_hook(move |_| {
        cancelled_by_hook.store(true, std::sync::atomic::Ordering::SeqCst);
    });
    let renewal = spawn_active_lease_renewal(
        client,
        ActiveLeaseAuthority {
            worker_id: "worker".into(),
            project_id: "project".into(),
            deployment_id: "deployment".into(),
            run_id: "run".into(),
            lease_id: "lease".into(),
            attempt: 0,
            lease_timeout_ms: 60_000,
            lease_expires_at_ms: current_time_ms() + 60_000,
            session: ActiveLeaseSession::Push,
        },
        worker.revoked_executions.clone(),
        worker.cancel_tokens.clone(),
        worker.cancel_hook.clone(),
        stopped,
    )
    .unwrap();
    let emitter = worker.clone();
    let checkpoint = tokio::spawn(async move {
        emitter
            .emit_checkpoint_sync(
                "run:child".into(),
                "function.started".into(),
                b"{}".to_vec(),
                1,
                HashMap::new(),
                1,
                20,
            )
            .await
    });
    let pending = next_append(&mut calls).await;
    assert!(matches!(
        checkpoint.await.unwrap().unwrap_err(),
        SdkError::Timeout { .. }
    ));
    assert!(
        !cancelled.load(std::sync::atomic::Ordering::SeqCst),
        "original checkpoint failure must reach its waiter"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), renewal)
            .await
            .is_ok(),
        "revoked push execution must stop renewing so its lease can expire for recovery"
    );
    assert!(!worker
        .lease_renewal_stops
        .lock()
        .unwrap()
        .contains_key("run"));
    drop(pending);
    server.abort();
}

#[derive(Clone, Default)]
struct RecordingEngine(Arc<std::sync::Mutex<Vec<Record>>>);

impl tonic::server::NamedService for RecordingEngine {
    const NAME: &'static str = "api.v1.EngineService";
}

impl tonic::server::UnaryService<AppendRequest> for RecordingEngine {
    type Response = AppendResponse;
    type Future = BoxFuture<tonic::Response<AppendResponse>, tonic::Status>;

    fn call(&mut self, request: tonic::Request<AppendRequest>) -> Self::Future {
        let records = self.0.clone();
        Box::pin(async move {
            let record = request
                .into_inner()
                .record
                .ok_or_else(|| tonic::Status::invalid_argument("missing record"))?;
            let mut records = records.lock().unwrap();
            records.push(record);
            Ok(tonic::Response::new(AppendResponse {
                offset: records.len() as u64,
                timestamp_ns: 1,
            }))
        })
    }
}

impl Service<http::Request<tonic::body::Body>> for RecordingEngine {
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let service = self.clone();
        Box::pin(async move {
            if request.uri().path() != "/api.v1.EngineService/Append" {
                return Ok(
                    tonic::Status::unimplemented("only current Engine Append is supported")
                        .into_http(),
                );
            }
            let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
            Ok(grpc.unary(service, request).await)
        })
    }
}

#[tokio::test]
async fn lifecycle_checkpoints_use_current_engine_when_endpoint_is_derived() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let engine = RecordingEngine::default();
    let records = engine.0.clone();
    let incoming = async_stream::stream! {
        loop {
            yield listener.accept().await.map(|(socket, _)| socket);
        }
    };
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(engine)
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    let config = WorkerConfig {
        service_name: "checkpoint-contract".into(),
        service_version: "test".into(),
        service_type: "standalone".into(),
        worker_id: "worker".into(),
        coordinator_endpoint: endpoint.clone(),
        ee_endpoint: "http://127.0.0.1:1".into(), // A deprecated fallback cannot succeed.
        max_retries: 0,
        engine_endpoint: resolve_engine_endpoint(None, &endpoint),
        max_concurrency: Some(1),
    };
    let worker = Worker::new(
        config,
        vec![],
        HashMap::from([("project_id".into(), "project".into())]),
    );
    let events = [
        "run.started",
        "workflow.step.started",
        "workflow.step.completed",
        "run.completed",
    ];
    tokio::time::timeout(Duration::from_secs(5), async {
        for (sequence, event) in events.iter().enumerate() {
            worker
                .emit_checkpoint_sync(
                    "run".into(),
                    (*event).into(),
                    b"{}".to_vec(),
                    sequence as i64,
                    HashMap::new(),
                    1,
                    1000,
                )
                .await
                .unwrap();
        }
    })
    .await
    .unwrap();
    server.abort();
    let records = records.lock().unwrap();
    assert_eq!(
        records
            .iter()
            .map(|record| record.event_type.as_str())
            .collect::<Vec<_>>(),
        events
    );
    assert!(records
        .iter()
        .all(|record| record.run_id == "run" && record.project_id == "project"));
}
