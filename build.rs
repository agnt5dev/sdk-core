fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile protocol buffers
    let proto_root = "../../protos/proto";
    
    tonic_build::configure()
        .build_server(false)  // We're only a client for now
        .build_client(true)
        .compile_protos(
            &[
                "api/v1/worker_coordinator.proto",
                "api/v1/common.proto",
            ],
            &[proto_root],
        )?;

    println!("cargo:rerun-if-changed={}/api/v1/", proto_root);
    Ok(())
}