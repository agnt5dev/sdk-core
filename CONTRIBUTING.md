# Contributing

Thank you for contributing to the AGNT5 SDKs.

Open an issue before making a large or compatibility-affecting change. Keep
pull requests focused, add tests for observable behavior, and update public
documentation when an API changes.

By contributing, you agree that your contribution is licensed under the
Apache License 2.0 included in this repository.

## Development

Run the narrow Rust checks before opening a pull request:

```bash
cargo fmt --check
cargo test
```

Changes to worker, event, checkpoint, streaming, or serialization behavior
must include or update a language-neutral contract under `conformance/`.
