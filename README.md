# AGNT5 SDK Core

[![CI](https://github.com/agnt5dev/sdk-core/actions/workflows/ci.yml/badge.svg)](https://github.com/agnt5dev/sdk-core/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

`agnt5-sdk-core` is the shared Rust foundation for the AGNT5 language SDKs. It
provides the runtime-facing primitives used by the Python, TypeScript, and Go
SDKs, including:

- worker registration, dispatch, and lifecycle management;
- durable workflow checkpoints and event delivery;
- language-model and embedding provider abstractions;
- built-in evaluation scorers;
- telemetry and structured logging;
- MCP client support; and
- vendor-neutral sandbox contracts.

Most application developers should use an AGNT5 language SDK rather than this
crate directly:

- [Python SDK](https://github.com/agnt5dev/sdk-python)
- [TypeScript SDK](https://github.com/agnt5dev/sdk-typescript)
- [Go SDK](https://github.com/agnt5dev/sdk-go)

## Installation

Add the crate to a Rust project:

```toml
[dependencies]
agnt5-sdk-core = "0.1.1"
```

The default build contains the portable SDK foundation. Optional capabilities
can be enabled with Cargo features:

```toml
[dependencies]
agnt5-sdk-core = { version = "0.1.1", features = ["libsql-memory"] }
```

| Feature | Purpose |
| --- | --- |
| `libsql-memory` | Embedded libSQL-backed vector memory |
| `wasm-sandbox` | Embedded Wasmtime sandbox execution |

## Repository boundaries

This repository owns the language-neutral SDK runtime contracts and their Rust
implementation. Cross-SDK conformance specifications and shared fixtures live
under [`conformance/`](conformance/) and [`test-fixtures/`](test-fixtures/).

Vendor-specific sandbox implementations are maintained separately in
[`agnt5dev/sdk-integrations`](https://github.com/agnt5dev/sdk-integrations).
Keeping those integrations outside core prevents vendor credentials, endpoints,
and protocols from becoming part of the base SDK dependency graph.

## Development

The project requires a stable Rust toolchain. Protocol Buffer tooling is built
as part of the crate, so a separate system `protoc` installation is not
required.

Run the same checks used by CI:

```bash
cargo fmt --check
cargo test --locked
cargo package --locked
```

Changes to worker, checkpoint, event, streaming, or serialization behavior
should include a corresponding conformance contract or fixture.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidance. To report a
security issue, follow [SECURITY.md](SECURITY.md).

## License

Licensed under the [Apache License 2.0](LICENSE).
