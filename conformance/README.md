# AGNT5 SDK conformance

This directory holds capability contracts that must stay aligned across Python,
TypeScript, and Go. Contracts are not split by language. A case is green only
when every SDK satisfies the same request, output, lifecycle, and event
requirements.

Result artifacts label each case as `shape`, `behavior`, `integration`, or
`resilience`. Shape coverage is useful but does not make an integration or
capability complete.

## Run Against A Live Kitchen Sink

Start the runtime and the target kitchen-sink worker, then run:

```bash
python3 sdk/e2e/conformance/run_kitchen_sink_contract.py \
  --contract sdk/e2e/conformance/contracts/ks_analyze_text.yaml \
  --gateway-url "${AGNT5_GATEWAY_URL:-http://localhost:34183}" \
  --api-key "$AGNT5_API_KEY" \
  --deployment-id "$AGNT5_DEPLOYMENT_ID"
```

The runner invokes each component through the gateway, checks terminal status
and output/error shape, then checks ordered journal event subsequences for cases
that declare `expect.event_order`.

Use `--skip-events` only while debugging a runtime without event-history
support. Parity runs should keep event checks enabled.

Run one component contract across all three SDKs, or generate the complete
component-by-SDK matrix:

```bash
just sdk-e2e-parity contracts/ks_hitl.yaml
just sdk-e2e-parity-all
```

The full command writes `matrix.md`, `matrix.json`, and the per-contract SDK
results under `.artifacts/sdk-e2e/full-parity-<timestamp>/`.

The shared `ks_batch.yaml` group covers raw and SDK-envelope inputs, complete
and partial-failure aggregation, status with and without a matching batch,
cancellation of active child runs, and aggregate result envelopes. These are
behavior contracts: they execute real child runs through each kitchen sink.

## Expansion Order

Add each capability as a shared contract first, observe the failures across all
three SDKs, and fix the SDK/runtime behavior before promoting it to the required
gate. Do not add language skips to make the matrix green.

1. Lifecycle: persistent state and sessions, isolation, HITL rejection, stale
   resume, and mixed completed/running batch cancellation races.
2. Streaming behavior and performance: ordered payload fidelity, first-delta
   latency, incremental delivery, burst throughput, failure propagation, and
   exactly-once terminal behavior.
3. Streaming resilience: reconnect/resume, client disconnect, cancellation,
   slow consumers, backpressure, and duplicate terminal protection.
4. Resilience: worker replacement, worker/runtime restart during queued,
   running, streaming, and paused work, lease expiry, and coordinator failover.
5. Triggers and integrations: real cron firing, webhook/event ingress, MCP
   transport and tool calls, provider sandboxes, and live LLM adapters.
6. Deployment compatibility: built/published SDK artifacts, managed local-k8s
   deployments, and supported SDK/runtime version combinations.
7. Scale and operations: concurrency, overload, noisy-neighbor isolation,
   readiness, telemetry, backup/restore, and rollback evidence.

The deterministic PM2 matrix runs `shape` and `behavior` contracts. Integration
and resilience lanes must provision their own prerequisites and still produce
the same component-by-SDK matrix.

## Validate The Harness

```bash
python3 -m unittest discover -s sdk/e2e/conformance -p '*_test.py'
```
