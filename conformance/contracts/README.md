# Cross-SDK parity contracts

Contracts are grouped by capability, not implementation language. Every case
in a group is expected to run against Python, TypeScript, and Go unless its
`sdks` field explicitly narrows the matrix.

Every contract also declares what a green result proves:

- `shape`: deterministic request/response compatibility only.
- `behavior`: real SDK behavior with deterministic local dependencies.
- `integration`: a real service, transport, provider, scheduler, or ingress path.
- `resilience`: behavior across restarts, failover, duplicate delivery, or load.

The local parity gate must not skip an SDK. Provider-backed integration and
resilience contracts run in separate lanes with their prerequisites created by
the harness. A shape contract must never be reported as integration coverage.

```yaml
version: 1
group: functions
coverage: behavior
cases:
  - id: analyze_success
    component: ks_analyze_text
    sdks: [python, typescript, go]
    input: {text: hello durable sdk}
    expect:
      status: success
      events:
        required: [run.started, function.started, function.completed, run.completed]
        ordered: [run.started, function.started, function.completed, run.completed]
        min_counts: {}
```

The evaluator reports one result per SDK and compares the same event/output
contract. SDK-specific component aliases belong in an adapter manifest, never
in separate language contracts.

Recommended groups are `functions`, `workflows`, `streaming`, `state`,
`agents`, `tools`, and `hitl`.
