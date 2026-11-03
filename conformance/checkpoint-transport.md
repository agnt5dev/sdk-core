# Engine checkpoint transport and ordering

Coverage: transport integration against an isolated gRPC service.

1. Configure a combined coordinator/Engine listener without an explicit Engine
   endpoint. Derive the Engine endpoint from that coordinator listener.
2. Persist run start, step start, step completion, and run completion through
   `api.v1.EngineService/Append`; require acknowledgement for each write.
3. A listener exposing only the current service must suffice. Never choose the
   deprecated ExecutionEngine checkpoint RPC because Engine configuration is
   absent.
4. Preserve project/run identity and event order across these writes.
5. Reject an explicitly unusable Engine configuration with an actionable error.

The sdk-core test
`lifecycle_checkpoints_use_current_engine_when_endpoint_is_derived` drives the
same worker lifecycle method used by Python and TypeScript, with the deprecated
endpoint set to an unreachable address. The `engine_endpoint` unit tests cover
resolution and invalid/missing configuration. Go remains pure Go and unchanged;
its own transport tests and cross-SDK deployed checks are separate evidence.

## Concurrent acknowledged writes

Compatible function, agent, tool-call, model and sandbox lifecycle records
already waiting within one run may share `AppendBatch`. A single record is sent
immediately through `Append`; there is no timer to collect a larger group.
Every caller waits until its own record is acknowledged, including pull mode.

The per-run writer orders checkpoints together with periodic flushes, activation
flushes, explicit checkpoint batches and completion flushes. A checkpoint cannot
overtake an older queued or in-flight event prefix. A group cannot cross an
explicit flush barrier. Unrelated runs retain independent writers.

Records keep their payload, timestamp, correlation and parent IDs, project,
subtenant and authority metadata. Records with different request metadata
(except correlation IDs) or RPC timeout budgets are not combined. Run terminal
events, workflow control events and approvals remain individual writes.

Groups use the configured journal batch count and at most 1 MiB of combined
record data. A larger individual record retains the single-record path. The
acknowledged-write queue uses `AGNT5_JOURNAL_QUEUE_SIZE` (default 5000) as a
separate bound; overflow returns `ResourceExhausted` without dropping accepted
writes. RPC timeout budgets retain their existing meaning: they start at RPC
dispatch and do not include time waiting in the ordering queue.

Cancellation can withdraw a request still waiting in the queue. Once selected
for persistence, a group settles independently of individual waiters. Remaining
callers receive only the outcome of their own group. Rejections retain their
structured status, and retries retain identical records. Timeout, malformed ACK
or another uncertain persistence failure blocks later writes and activation
flushes for that execution until normal run recovery. No timeout implies success.
Push lease renewal stops when an uncertain execution is blocked, so a suppressed
response cannot leave its lease renewing indefinitely. Checkpoint waiters retain
their original error instead of being cancelled by the language hook.

The gated `worker::checkpoint_contract_tests` exercise the real worker interface
and gRPC transport. They cover shared ACKs, prefix and explicit-barrier ordering,
caller cancellation, error fan-out, lost-ACK retries, bounds and metadata.

`native_checkpoint_probe.py` measures native transport with controlled local
ACK latency. Its server acknowledges both RPC types but is not durable storage.
Real-engine workflow and restart evidence must be recorded separately.
