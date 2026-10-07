# Changelog

## 0.3.9 - 2026-10-07

### Maintenance

- Remove an unused telemetry environment setting from the shutdown test. Runtime behavior is unchanged from 0.3.8, including fenced failure responses for escaped worker handler errors.

## 0.3.8 - 2026-10-06

### Fixed

- Return a failure response when a worker handler returns an error, including a Python `BaseException` that escapes an executor. Previously the worker logged the error and dropped the response, leaving the invocation waiting for its lease to expire. Push and pull workers now preserve the invocation identity, attempt, and lease in the failure response. Cancellation, revocation, and durable suspension keep their existing behavior.

### Upgrade

- Python and TypeScript native bindings must adopt core 0.3.8 in their own releases to include this fix.

## 0.3.7 - 2026-10-02

### Fixed

- Drop `temperature` and `top_p` (with a one-time warning) for Claude models that reject them: Opus 4.7 and later, Sonnet 5, Opus 5 and Fable, including Bedrock and Vertex ids. A Python `Agent` with default settings failed with a 400 on all of them. New or unrecognised Claude models are treated as rejecting.
- Raise the default Anthropic `max_tokens` from 1024 to 16384 for those models (4096 for older ones), since thinking counts toward it; Opus 5 answers were cut off mid-sentence. The Anthropic and Bedrock request timeout default rises from 30 s to 10 minutes to match the OpenAI provider.
- OpenAI reasoning models are now every `gpt-N` with N ≥ 5 plus the o-series, from one shared rule.
- Bedrock cross-region inference profile ids (`us.anthropic.…`, `eu.`, `apac.`, `global.`) route to their model family instead of failing as unsupported.
- Claude Haiku accepts sampling parameters only up to 4.5 (Opus and Sonnet up to 4.6).

### Added

- `ReasoningEffort::None` and `ReasoningEffort::Low`. gpt-6 accepts `none`/`low`/`medium`/`high` and rejects `minimal`; gpt-5 keeps `minimal`. Gemini maps `none` to `minimal`.

## 0.3.6 - 2026-10-01

### Fixed

- Export trace spans with the worker resource (`service.name=agnt5-worker`, `service.version`, `agnt5.*`). The span filter did not forward the provider resource to the OTLP exporter, so Python and TypeScript traces had an empty resource and never appeared in trace listing. The filter now also forwards `force_flush` and `shutdown_with_timeout`.
- Treat the gpt-6 family as OpenAI reasoning models, like gpt-5 and the o-series: no `temperature` or `top_p` in the request and `max_completion_tokens` instead of `max_tokens`. gpt-6 rejects both with a 400, so every Python call to `gpt-6-luna` failed.

## 0.3.5 - 2026-10-01

### Fixed

- Populate the parsed structured `object` for non-streaming OpenAI responses when JSON or JSON-schema output is requested. Previously only the streaming path and other providers parsed it, so Python `structured_output` and TypeScript `structuredOutput` were empty for OpenAI models. Invalid JSON is logged and leaves `object` unset; the raw text is still returned (AGNT5-1371, AGNT5-1416).

## 0.3.4 - 2026-09-25

- Add the bounded structured assertions built-in shared by native SDKs and runtime.
- Validate online recipes against parsed evidence requirements before activation.

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
