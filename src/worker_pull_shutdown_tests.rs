//! Drive real pull slots and connection teardown through a local unary Engine
//! transport. Handler and completion barriers make shutdown races deterministic.
use super::*;
use crate::pb::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Semaphore;
use tonic::codegen::{Body, BoxFuture, Service, StdError};

struct RuntimeFixture {
    polls: AtomicUsize,
    register_entered: Semaphore,
    register_release: Semaphore,
    poll_entered: Semaphore,
    handler_entered: Semaphore,
    handler_release: Semaphore,
    completion_entered: Semaphore,
    completion_release: Semaphore,
    completions: std::sync::Mutex<Vec<CompleteJobRequest>>,
}

impl RuntimeFixture {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            polls: AtomicUsize::new(0),
            register_entered: Semaphore::new(0),
            register_release: Semaphore::new(1),
            poll_entered: Semaphore::new(0),
            handler_entered: Semaphore::new(0),
            handler_release: Semaphore::new(0),
            completion_entered: Semaphore::new(0),
            completion_release: Semaphore::new(0),
            completions: std::sync::Mutex::new(Vec::new()),
        })
    }

    async fn register(&self, _: RegisterWorkerSessionRequest) -> RegisterWorkerSessionResponse {
        self.register_entered.add_permits(1);
        self.register_release.acquire().await.unwrap().forget();
        RegisterWorkerSessionResponse {
            worker_session_id: "shutdown-test-session".into(),
            expires_at_ms: current_time_ms() + 300_000,
            ..Default::default()
        }
    }

    async fn poll(&self, request: PollJobRequest) -> PollJobResponse {
        assert_eq!(request.worker_session_id, "shutdown-test-session");
        self.poll_entered.add_permits(1);
        if self.polls.fetch_add(1, Ordering::SeqCst) == 0 {
            PollJobResponse {
                job: Some(JobAssignment {
                    job_id: "shutdown-test-run".into(),
                    run_id: "shutdown-test-run".into(),
                    component_name: "drain".into(),
                    component_type: ComponentType::Function as i32,
                    lease_id: "shutdown-test-lease".into(),
                    lease_expires_at_ms: current_time_ms() + 300_000,
                    attempt: 3,
                    input_data: b"{}".to_vec(),
                    metadata: HashMap::from([
                        ("project_id".into(), "shutdown-project".into()),
                        ("deployment_id".into(), "shutdown-deployment".into()),
                    ]),
                    ..Default::default()
                }),
                ..Default::default()
            }
        } else {
            std::future::pending().await
        }
    }

    async fn capacity(&self, _: ReportWorkerCapacityRequest) -> ReportWorkerCapacityResponse {
        ReportWorkerCapacityResponse::default()
    }

    async fn renew(&self, _: RenewJobLeaseRequest) -> RenewJobLeaseResponse {
        RenewJobLeaseResponse {
            renewed: true,
            lease_expires_at_ms: current_time_ms() + 300_000,
            ..Default::default()
        }
    }

    async fn complete(&self, request: CompleteJobRequest) -> CompleteJobResponse {
        self.completions.lock().unwrap().push(request);
        self.completion_entered.add_permits(1);
        self.completion_release.acquire().await.unwrap().forget();
        CompleteJobResponse {
            acknowledged: true,
            ..Default::default()
        }
    }

    async fn handle(&self, message: RuntimeMessage) -> Result<Option<ServiceMessage>> {
        let Some(runtime_message::MessageData::DispatchComponent(request)) = message.message_data
        else {
            panic!("expected dispatched pull job");
        };
        assert_eq!(request.invocation_id, "shutdown-test-run");
        assert_eq!(request.metadata["dispatch_mode"], "pull");
        self.handler_entered.add_permits(1);
        self.handler_release.acquire().await.unwrap().forget();
        Ok(Some(ServiceMessage {
            message_type: Some(service_message::MessageType::FunctionResponse(
                DispatchComponentResponse {
                    invocation_id: request.invocation_id,
                    success: true,
                    event_type: "run.completed".into(),
                    result: Some(dispatch_component_response::Result::OutputData(
                        b"drained".to_vec(),
                    )),
                    ..Default::default()
                },
            )),
            ..Default::default()
        }))
    }
}

#[derive(Clone)]
struct FixtureEngine(Arc<RuntimeFixture>);

impl tonic::server::NamedService for FixtureEngine {
    const NAME: &'static str = "api.v1.EngineService";
}

impl<B> Service<http::Request<B>> for FixtureEngine
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        macro_rules! unary {
            ($request:ty, $response:ty, $method:ident) => {{
                struct Method(Arc<RuntimeFixture>);
                impl tonic::server::UnaryService<$request> for Method {
                    type Response = $response;
                    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                    fn call(&mut self, request: tonic::Request<$request>) -> Self::Future {
                        let fixture = self.0.clone();
                        Box::pin(async move {
                            Ok(tonic::Response::new(
                                fixture.$method(request.into_inner()).await,
                            ))
                        })
                    }
                }
                let method = Method(self.0.clone());
                Box::pin(async move {
                    let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                    Ok(grpc.unary(method, request).await)
                })
            }};
        }
        match request.uri().path() {
            "/api.v1.EngineService/RegisterWorkerSession" => unary!(
                RegisterWorkerSessionRequest,
                RegisterWorkerSessionResponse,
                register
            ),
            "/api.v1.EngineService/PollJob" => unary!(PollJobRequest, PollJobResponse, poll),
            "/api.v1.EngineService/ReportWorkerCapacity" => unary!(
                ReportWorkerCapacityRequest,
                ReportWorkerCapacityResponse,
                capacity
            ),
            "/api.v1.EngineService/RenewJobLease" => {
                unary!(RenewJobLeaseRequest, RenewJobLeaseResponse, renew)
            }
            "/api.v1.EngineService/CompleteJob" => {
                unary!(CompleteJobRequest, CompleteJobResponse, complete)
            }
            _ => Box::pin(async {
                Ok(http::Response::builder()
                    .status(200)
                    .header("grpc-status", "12")
                    .header("content-type", "application/grpc")
                    .body(tonic::body::Body::empty())
                    .unwrap())
            }),
        }
    }
}

async fn fixture_worker() -> (Arc<RuntimeFixture>, Worker, tokio::task::JoinHandle<()>) {
    let fixture = RuntimeFixture::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let incoming = async_stream::stream! {
        loop { yield listener.accept().await.map(|(socket, _)| socket); }
    };
    let service = FixtureEngine(fixture.clone());
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    let mut config = WorkerConfig::new("shutdown-test".into(), "v1".into(), "function".into());
    config.coordinator_endpoint = endpoint.clone();
    config.ee_endpoint = endpoint.clone();
    config.engine_endpoint = Some(endpoint);
    config.max_concurrency = Some(1);
    let worker = Worker::new(
        config,
        vec![],
        HashMap::from([
            ("project_id".into(), "shutdown-project".into()),
            ("deployment_id".into(), "shutdown-deployment".into()),
        ]),
    );
    (fixture, worker, server)
}

async fn graceful_pull_drain(outer_connection: bool) {
    let (fixture, worker, server) = fixture_worker().await;
    let handler_fixture = fixture.clone();
    let handler = move |message, _| {
        let fixture = handler_fixture.clone();
        async move { fixture.handle(message).await }
    };
    let (shutdown, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let (poll_shutdown, poll_shutdown_rx) = tokio::sync::watch::channel(false);
    let mut task = if outer_connection {
        tokio::spawn(async move {
            worker
                .try_connect_and_run(handler, shutdown_rx, false, None)
                .await
                .unwrap();
        })
    } else {
        worker.spawn_parked_poll_task(
            flume::unbounded().0,
            handler,
            poll_shutdown_rx,
            1,
            Arc::new(AtomicUsize::new(0)),
            vec![],
            vec![],
        )
    };
    tokio::time::timeout(Duration::from_secs(2), fixture.handler_entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    if outer_connection {
        shutdown.send(()).unwrap();
    } else {
        poll_shutdown.send(true).unwrap();
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut task)
            .await
            .is_err(),
        "graceful shutdown dropped an accepted pull handler before CompleteJob"
    );
    fixture.handler_release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), fixture.completion_entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut task)
            .await
            .is_err(),
        "graceful shutdown returned before the fenced completion acknowledgement"
    );
    fixture.completion_release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    let completions = fixture.completions.lock().unwrap();
    assert_eq!(completions.len(), 1);
    assert_eq!(completions[0].worker_session_id, "shutdown-test-session");
    assert_eq!(completions[0].lease_id, "shutdown-test-lease");
    assert_eq!(completions[0].attempt, Some(3));
    assert_eq!(completions[0].output_data, b"drained");
    assert_eq!(
        fixture.polls.load(Ordering::SeqCst),
        1,
        "shutdown must stop new polls"
    );
    server.abort();
}

#[tokio::test]
async fn parked_pull_supervisor_drains_accepted_handler_and_completion_on_shutdown() {
    graceful_pull_drain(false).await;
}

#[tokio::test]
async fn pull_connection_drains_accepted_handler_and_completion_on_shutdown() {
    graceful_pull_drain(true).await;
}

#[tokio::test]
async fn idle_pull_shutdown_does_not_wait_for_long_poll_deadline() {
    let (fixture, worker, server) = fixture_worker().await;
    fixture.polls.store(1, Ordering::SeqCst); // Keep the first real request parked.
    let (shutdown, rx) = tokio::sync::watch::channel(false);
    let handler = |_, _| async { panic!("idle poll must not dispatch work") };
    let task = worker.spawn_parked_poll_task(
        flume::unbounded().0,
        handler,
        rx,
        1,
        Arc::new(AtomicUsize::new(0)),
        vec![],
        vec![],
    );
    tokio::time::timeout(Duration::from_secs(2), fixture.poll_entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_millis(200), task)
        .await
        .expect("idle shutdown must cancel the outstanding 30-second poll")
        .unwrap();
    assert_eq!(fixture.polls.load(Ordering::SeqCst), 2);
    assert!(fixture.completions.lock().unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn locally_ready_poll_reply_wins_a_simultaneous_shutdown() {
    let (stop, mut rx) = tokio::sync::watch::channel(false);
    stop.send(true).unwrap();
    let response =
        wait_for_poll_or_shutdown(std::future::ready("accepted assignment"), &mut rx).await;
    assert_eq!(response, Some("accepted assignment"));
}

#[tokio::test]
async fn pull_drain_deadline_cancels_language_work_without_completing_its_lease() {
    let (fixture, worker, server) = fixture_worker().await;
    let cancellations = Arc::new(AtomicUsize::new(0));
    let observed = cancellations.clone();
    worker.set_cancel_hook(move |run_id| {
        assert_eq!(run_id, "shutdown-test-run");
        observed.fetch_add(1, Ordering::SeqCst);
    });
    let handler_fixture = fixture.clone();
    let handler = move |message, _| {
        let fixture = handler_fixture.clone();
        async move { fixture.handle(message).await }
    };
    let (shutdown, rx) = tokio::sync::watch::channel(false);
    let task = worker.spawn_parked_poll_task(
        flume::unbounded().0,
        handler,
        rx,
        1,
        Arc::new(AtomicUsize::new(0)),
        vec![],
        vec![],
    );
    tokio::time::timeout(Duration::from_secs(2), fixture.handler_entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    tokio::time::pause();
    shutdown.send(true).unwrap();
    tokio::task::yield_now().await;
    tokio::time::advance(PULL_SHUTDOWN_DRAIN_TIMEOUT - Duration::from_secs(1)).await;
    assert!(!task.is_finished());
    assert_eq!(cancellations.load(Ordering::SeqCst), 0);
    tokio::time::advance(Duration::from_secs(1)).await;
    task.await.unwrap();
    assert_eq!(cancellations.load(Ordering::SeqCst), 1);
    assert!(fixture.completions.lock().unwrap().is_empty());
    assert!(execution_is_revoked(
        &worker.revoked_executions,
        "shutdown-test-run"
    ));
    assert_eq!(
        worker
            .pending_lease_ids
            .lock()
            .unwrap()
            .get("shutdown-test-run")
            .map(String::as_str),
        Some("shutdown-test-lease")
    );
    server.abort();
}

#[cfg(unix)]
#[tokio::test]
async fn sigterm_child_runs_the_real_worker_shutdown_path() {
    if std::env::var_os("AGNT5_TEST_PULL_SIGTERM_CHILD").is_none() {
        return;
    }
    use tokio::io::AsyncBufReadExt;
    let (fixture, worker, server) = fixture_worker().await;
    let handler_fixture = fixture.clone();
    let task = tokio::spawn(async move {
        worker
            .run(move |message, _| {
                let fixture = handler_fixture.clone();
                async move { fixture.handle(message).await }
            })
            .await
            .unwrap();
    });
    tokio::time::timeout(Duration::from_secs(5), fixture.handler_entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    println!("PULL_SIGTERM_READY");
    std::io::Write::flush(&mut std::io::stdout()).unwrap();
    let mut input = tokio::io::BufReader::new(tokio::io::stdin());
    let mut command = String::new();
    tokio::time::timeout(Duration::from_secs(5), input.read_line(&mut command))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.trim(), "finish");
    assert!(
        !task.is_finished(),
        "SIGTERM must keep accepted work alive while draining"
    );
    fixture.handler_release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), fixture.completion_entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    fixture.completion_release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fixture.completions.lock().unwrap().len(), 1);
    assert_eq!(fixture.polls.load(Ordering::SeqCst), 1);
    server.abort();
}

#[cfg(unix)]
#[tokio::test]
async fn unix_sigterm_drains_accepted_pull_work_in_a_child_process() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "worker::pull_shutdown_tests::sigterm_child_runs_the_real_worker_shutdown_path",
            "--nocapture",
        ])
        .env("AGNT5_TEST_PULL_SIGTERM_CHILD", "1")
        .env("AGNT5_WORKER_MODE", "pull")
        .env("OTEL_SDK_DISABLED", "true")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut output = tokio::io::BufReader::new(child.stdout.take().unwrap()).lines();
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let line = output
                .next_line()
                .await
                .unwrap()
                .expect("child exited before signal listener was ready");
            if line == "PULL_SIGTERM_READY" {
                break;
            }
        }
    })
    .await
    .unwrap();
    let pid = child.id().unwrap().to_string();
    let status = tokio::process::Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .await
        .unwrap();
    assert!(status.success());
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"finish\n")
        .await
        .unwrap();
    let status = tokio::time::timeout(Duration::from_secs(8), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(
        status.success(),
        "SIGTERM child must finish its fenced pull completion: {status}"
    );
}

#[tokio::test]
async fn shutdown_during_pull_registration_never_starts_a_poll() {
    let (fixture, worker, server) = fixture_worker().await;
    fixture.register_release.acquire().await.unwrap().forget();
    let (shutdown, rx) = tokio::sync::broadcast::channel(1);
    let task = tokio::spawn(async move {
        worker
            .try_connect_and_run(
                |_, _| async { panic!("shutdown registration must not dispatch") },
                rx,
                false,
                None,
            )
            .await
            .unwrap();
    });
    tokio::time::timeout(Duration::from_secs(2), fixture.register_entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    shutdown.send(()).unwrap();
    tokio::time::timeout(Duration::from_millis(200), task)
        .await
        .expect("registration must be cancelled promptly")
        .unwrap();
    fixture.register_release.add_permits(1);
    assert_eq!(fixture.polls.load(Ordering::SeqCst), 0);
    server.abort();
}

#[tokio::test]
async fn shutdown_queued_before_pull_spawn_never_starts_a_poll() {
    let (fixture, worker, server) = fixture_worker().await;
    let (shutdown, rx) = tokio::sync::broadcast::channel(1);
    shutdown.send(()).unwrap();
    let task = tokio::spawn(async move {
        worker
            .try_connect_and_run(
                |_, _| async { panic!("already stopped worker must not dispatch") },
                rx,
                false,
                None,
            )
            .await
            .unwrap();
    });
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fixture.polls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.register_entered.available_permits(), 0);
    server.abort();
}
