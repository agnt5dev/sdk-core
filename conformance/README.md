# SDK conformance contracts

This directory contains the language-neutral behavior contracts shared by the
AGNT5 Python, TypeScript, and Go SDKs. Each YAML file describes observable
inputs, outputs, lifecycle states, and event-order requirements for one SDK
capability.

The contracts are specifications, not an implementation-specific test suite.
Each SDK is responsible for running the same cases through its public API and
reporting compatible results.

## Contract structure

Contracts live under [`contracts/`](contracts/). A contract groups cases for a
single component or capability and identifies the expected coverage level:

- `shape` validates request and response structure;
- `behavior` validates observable SDK behavior;
- `integration` validates behavior against external services; and
- `resilience` validates recovery and failure handling.

See [`contracts/README.md`](contracts/README.md) for the schema and authoring
guidance.

## Making contract changes

When changing a shared SDK behavior:

1. Update or add the language-neutral contract first.
2. Run the case against every supported language SDK.
3. Fix implementation differences instead of adding language-specific skips.
4. Merge the contract only when its required coverage level is satisfied.

Harnesses that provision runtimes, invoke deployed workers, or aggregate CI
results belong with the environment that operates them. They should consume
these contracts without making this repository depend on a particular SDK
language.
