# Request logging and durable ordering

Ordinary request logs are best-effort observations. Their live-stream copy
must not become a prerequisite for an activation or checkpoint append.
Implementations may hand that copy to a bounded, nonblocking transport and
drop it when the transport is unavailable or full. Preserve the request's
project, run, correlation IDs and source timestamp when forwarding it.
The separate application telemetry exporter keeps its existing behavior.

This rule applies only to SSE-only `log` and `log.*` events on requests that
did not explicitly opt into streaming. Explicit streaming requests retain
their ordered event-prefix flush before subsequent durable writes. Other
streaming events, such as output deltas, retain their existing ordering.

An explicitly durable record must still wait for its durable ACK, including
a record named `log.*`. Activation start/result ACKs, same-run ordering,
unknown-outcome fencing and replay semantics must not depend on the health
of ordinary best-effort logging. An unavailable log stream for one run must
not prevent another run from progressing.

Internal SDK DEBUG diagnostics are opt-in and should be filtered before
creating telemetry or stream work. This does not change application log
levels or prevent explicit internal diagnostic configuration.

The SDK-core checkpoint contract tests exercise bounded and disconnected
log transport, explicitly streamed prefixes, durable log records and
independent runs against a local Engine RPC server with gated ACKs. Python
unit tests cover source-level diagnostic filtering and application logging
compatibility. These are local behavioral tests; they do not certify
production latency or resilience across process restarts.
