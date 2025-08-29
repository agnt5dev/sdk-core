fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(false)
        .compile_protos(
            &[
                "../../protos/proto/api/v1/worker_coordinator.proto",
                "../../protos/proto/api/v1/common.proto",
            ],
            &["../../protos/proto"],
        )?;
    Ok(())
}