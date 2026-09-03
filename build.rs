fn main() -> Result<(), Box<dyn std::error::Error>> {
    configure_protoc();

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

    Ok(())
}

/// Pick the `protoc` used to compile the runtime protos.
///
/// An explicit `PROTOC` in the environment always wins, so CI and downstream
/// builds can point at a prebuilt binary. Otherwise the `vendored-protoc`
/// feature (on by default) supplies one through `protobuf-src`, which builds
/// libprotobuf from source and adds several minutes to a cold build.
fn configure_protoc() {
    println!("cargo:rerun-if-env-changed=PROTOC");
    println!("cargo:rerun-if-env-changed=PROTOC_INCLUDE");

    if std::env::var_os("PROTOC").is_some_and(|value| !value.is_empty()) {
        return;
    }

    #[cfg(feature = "vendored-protoc")]
    {
        std::env::set_var("PROTOC", protobuf_src::protoc());
        std::env::set_var("PROTOC_INCLUDE", protobuf_src::include());
    }

    #[cfg(not(feature = "vendored-protoc"))]
    {
        panic!(
            "agnt5-sdk-core was built without the `vendored-protoc` feature and PROTOC is not set. \
             Install protoc and export PROTOC=/path/to/protoc, or enable the `vendored-protoc` feature."
        );
    }
}
