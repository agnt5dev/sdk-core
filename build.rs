fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use protobuf-src's bundled protoc and includes
    std::env::set_var("PROTOC", protobuf_src::protoc());
    std::env::set_var("PROTOC_INCLUDE", protobuf_src::include());

    tonic_prost_build::configure()
        .build_server(false)
        .compile_protos(
            &[
                "proto/api/v1/worker_coordinator.proto",
                "proto/api/v1/execution_engine.proto",
                "proto/api/v1/engine.proto",
                "proto/api/v1/common.proto",
            ],
            &["proto"],
        )?;

    // Modal sandbox provider: vendored gRPC contract (Modal has no REST API).
    tonic_prost_build::configure()
        .build_server(false)
        .compile_protos(&["proto/modal/api.proto"], &["proto/modal"])?;
    Ok(())
}
