# AGNT5 SDK Core (Rust)

The foundation Rust implementation that powers all AGNT5 language SDKs. Handles gRPC transport, event persistence, worker lifecycle, and the FFI boundary.

## Integration boundary

SDK Core owns vendor-neutral sandbox contracts, the generic remote sandbox
backend, and the optional embedded WASM backend. Vendor-specific sandbox
clients are maintained in
[`agnt5dev/sdk-integrations`](https://github.com/agnt5dev/sdk-integrations).
Core does not compile vendor credentials, endpoints, or vendored protocols.

## Event Persistence Architecture

All events flow from SDK workers directly to the Execution Engine (EE). There are exactly **3 paths**, each serving a distinct purpose. All other components (language SDKs, built-in scorers, flush task) must use one of these.

### Path 1: Synchronous Checkpoint (`emit_checkpoint_sync`)

**For:** Lifecycle events that affect workflow state and must be durably persisted before the caller proceeds.

| Property | Value |
|----------|-------|
| RPC | `WriteCheckpoint` (unary) on `ExecutionEngineService` |
| Destination | EE direct |
| Batching | None (one event per RPC) |
| Ack | Blocks until EE response |
| Error handling | Clears EE client for reconnect; returns error to caller |

**Event types:** `run.started`, `run.completed`, `run.failed`, `workflow.step.started`, `workflow.step.completed`, `workflow.paused`, `approval.requested`, `approval.resolved`

**Callers:**
- Language SDKs (Python, TypeScript, Go) via `emit_checkpoint_sync_blocking`
- Built-in scorer fast path in the dispatch loop

**Key behavior:** Before sending `run.completed` or `run.failed`, pre-flushes any pending SSE-only events for that run via `drain_run_events()`. This ensures streaming deltas arrive before the terminal event closes the SSE listener.

### Path 2: Batched Journal Write (Flush Task → `WriteJournalEventsBatch`)

**For:** Boundary/observability events that should be durably persisted but don't need synchronous acknowledgment.

| Property | Value |
|----------|-------|
| RPC | `WriteJournalEventsBatch` (unary) on `ExecutionEngineService` |
| Destination | EE direct |
| Batching | Up to 100 events per 50ms tick |
| Ack | Batch response with per-event error reporting |
| Error handling | Re-queues failed events; clears EE client for reconnect |

**Event types:** `lm.call.started`, `lm.call.completed`, `tool.call.started`, `tool.call.completed`, `agent.iteration.started`, `agent.iteration.completed`, and any other non-SSE-only event queued via `queue_event`

**Flow:**
1. Language SDK calls `worker.queue_event(JournalEventMessage)` (or legacy wrappers `queue_checkpoint`/`queue_delta`)
2. Event is pushed to `JournalEventQueue` (in-memory `VecDeque`, cap 5000)
3. `spawn_journal_flush_task` drains batch every 50ms
4. Boundary events (`is_sse_only == false`) are collected into a `WriteJournalEventsBatchRequest`
5. Sent to EE which persists to journal + publishes to SSE (Redis/Centrifuge)

### Path 3: EventStream (Flush Task → Client-Streaming)

**For:** Transient SSE-only events that need real-time delivery but no durable persistence.

| Property | Value |
|----------|-------|
| RPC | `EventStream` (client-streaming) on `ExecutionEngineService` |
| Destination | EE direct |
| Batching | Individual events, sent as available |
| Ack | None (fire-and-forget) |
| Error handling | Falls back to dispatch stream (WC) if EventStream unavailable |

**Event types:** `output.delta`, `output.start`, `output.stop`, `lm.stream.delta`, `lm.message.delta`, `lm.thinking.delta`, `lm.tool_call.*`, `progress.*`, `log`, `log.info`, `log.warn`, `log.error`

**Gating:** SSE-only events are silently dropped for non-streaming runs (`is_streaming == false`). The `streaming_runs` map tracks which runs have active SSE listeners.

**Flow:**
1. Language SDK calls `worker.queue_event(JournalEventMessage)` with SSE-only event type
2. Event auto-classified as SSE-only by `JournalEventMessage::is_sse_only_event_type()`
3. Flush task sends via `EventStream` channel
4. If EventStream unavailable, falls back to dispatch stream wrapped as `DispatchComponentResponse`

### What Does NOT Go Through These Paths

- **Function responses** (`DispatchComponentResponse` with terminal `event_type`): These go on the WC bidirectional dispatch stream. They are function return values, not journal events.
- **CompleteJob** (polled jobs): The WC writes journal entries server-side in the `CompleteJob` RPC handler. The SDK has no control over this persistence path.
- **Heartbeats**: Go on the dispatch stream. Not journal events.

### Event Classification

Events are classified by `JournalEventMessage::is_sse_only_event_type()` in `journal_queue.rs`:

```
SSE-only (transient):       output.*, lm.stream.*, lm.message.*, lm.thinking.*,
                            lm.tool_call.*, progress.*, log*

Boundary (persisted):       Everything else (workflow.*, agent.*, lm.call.*,
                            tool.call.*, approval.*, etc.)
```

The inverse helper `is_checkpoint_event_type()` returns true for boundary events. Language SDKs use this to decide between `emit_checkpoint_sync` (lifecycle) and `queue_event` (observability).

### Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `AGNT5_JOURNAL_QUEUE_SIZE` | 5000 | Max buffered events in queue |
| `AGNT5_JOURNAL_BATCH_SIZE` | 100 | Events per flush tick |
| `AGNT5_JOURNAL_FLUSH_INTERVAL_MS` | 50 | Flush interval in ms |

## Key Files

| File | Purpose |
|------|---------|
| `src/worker.rs` | Worker lifecycle, dispatch loop, flush task, `emit_checkpoint_sync`, built-in scorer fast path, long-poll task, `CompleteJob` dispatch |
| `src/journal_queue.rs` | `JournalEventQueue`, `JournalEventMessage`, event classification, metrics |
| `src/client.rs` | `WorkerCoordinatorClient`, `complete_job()`, `poll_job()`, `create_ee_event_stream()` |
| `src/eval/builtin_scorer.rs` | Fast-path Rust scorers (`exact_match`, `contains`, `regex_match`, `json_valid`, `levenshtein`) |
| `src/lib.rs` | Module declarations, public re-exports, `pb` generated code |

## Architecture Diagram

```
Language SDK (Python/TypeScript/Go)
    │
    ├── Lifecycle events ──► emit_checkpoint_sync() ──► WriteCheckpoint RPC ──► EE
    │   (run.started, run.completed,                     (unary, sync ack)
    │    step.*, approval.*)
    │
    └── Observability + streaming events ──► queue_event() ──► JournalEventQueue
                                                                    │
                                                   spawn_journal_flush_task (50ms tick)
                                                                    │
                                              ┌─────────────────────┴─────────────────────┐
                                              │                                           │
                                      Boundary events                            SSE-only events
                                      (is_sse_only=false)                        (is_sse_only=true)
                                              │                                           │
                                  WriteJournalEventsBatch RPC              EventStream (client-streaming)
                                         to EE                                     to EE
                                              │                                           │
                                    ┌─────────┴─────────┐                    ┌────────────┴────────────┐
                                    │                    │                    │                         │
                              Journal persist      SSE publish         SSE publish              (no persist)
                              (PostgreSQL)      (Redis/Centrifuge)   (Redis/Centrifuge)
```
