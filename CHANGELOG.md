# Changelog

## 0.3.0

### Changed

- Workers now default to pull assignment when `AGNT5_WORKER_MODE` is unset or empty. Set `AGNT5_WORKER_MODE=push` to retain push dispatch. Registration reports the resolved mode consistently.

### Added

- Optional `display_parent_correlation_id` in the durable activation wire contract, allowing agent iteration display ancestry without changing activation identity or replay keys. Deploy compatible runtime readers before enabling SDK writers.
- Shared conformance contracts for worker assignment, display ancestry, and current Engine checkpoint transport.

### Verified

- A real gRPC regression test proves that worker run and step checkpoints use current `EngineService.Append` with the derived runtime endpoint. Missing or invalid current Engine configuration fails startup instead of selecting the deprecated checkpoint RPC.

### Upgrade

- This is a minor release because the worker assignment default changes. Deployments relying on implicit push must set `AGNT5_WORKER_MODE=push` before upgrading.
- Python and TypeScript consumers must adopt the new core version in their own releases; Go implements the shared contracts independently.
