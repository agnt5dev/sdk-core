# Changelog

## 0.3.4 - 2026-09-24

### Fixed

- Treat the gpt-6 family as OpenAI reasoning models, like gpt-5 and the o-series: no `temperature` or `top_p` in the request and `max_completion_tokens` instead of `max_tokens`. gpt-6 rejects both with a 400, so every Python call to `gpt-6-luna` failed (AGNT5-1301).

## 0.3.3 - 2026-09-22

### Fixed

- Reconnect pull-only workers after certificate rotation, including rebuilding cached engine channels; token refresh on the same certificate does not restart the worker.

- Keep discovered project/deployment authority and pull mode on the connection instead of rewriting process environment variables; reject reuse of a cached certificate manager for a different authority.

- Propagate the certificate-assigned worker ID into pull execution, lifecycle checkpoints, and activation authority. This fixes `authority_stale` failures when opt-in mTLS workers execute tasks; bearer workers keep their configured ID.

## 0.3.2 - 2026-09-22

### Added

- Opt-in external-worker mTLS with durable authentication selection and certificate-bound workload tokens. Existing bearer workers retain their default behavior.
- Persist renewal request identity and replacement key before sending a request, so a lost response can be recovered after restart without enrolling another worker.
- Trust runtime and control-plane server certificates through system roots or `AGNT5_WORKER_SERVER_CA_FILE`, independently of the workload certificate issuer.

### Upgrade

- Commission the policy-aware control plane, recoverable renewal endpoint, and dedicated runtime mTLS listener before enabling `AGNT5_WORKER_MTLS_ENABLED=true`. Mount a private persistent `AGNT5_WORKER_SESSION_DIR`. Once selected, mTLS remains pinned across restart and cannot silently downgrade to bearer authentication.

## 0.3.1 - 2026-09-13

### Fixed

- Drain accepted pull assignments through their handler and completion acknowledgement on intentional shutdown (AGNT5-1129), within one shared 25-second budget.
- Route Unix SIGTERM through graceful shutdown, stop idle polls promptly, and retain existing lease fencing and expiry when a grant outcome is ambiguous.
- Stop pending registration and poll setup when shutdown has already been requested; cancel remaining handlers and renewal tasks if the drain deadline expires.

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
