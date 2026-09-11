//! AGNT5-1002: exercise lifecycle writes against only the current Engine service.
use super::*;
use crate::pb::{AppendRequest, AppendResponse, Record};
use std::convert::Infallible;
use std::task::{Context, Poll};
use tonic::codegen::{BoxFuture, Service};

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
