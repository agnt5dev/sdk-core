# Durable activation display ancestry (AGNT5-1118)

Coverage: wire shape plus behavior. This additive reader contract is independent
of SDK language; writers that implement it must satisfy the same invariants.
TypeScript is the first language writer in this change. Other writers may omit
this field without losing their existing durable behavior.

- `BeginActivationRequest.display_parent_correlation_id` is string field 15.
- An absent/empty hint retains historical parent behavior and canonical bytes.
- A supplied hint identifies the owning agent iteration for model/tool display.
- `parent_activation_id` remains the durable owner; the hint must not affect
  activation IDs, input/definition digests, or replay decisions.
- Each concurrently executing agent and each later iteration carries its own
  hint, including streamed model calls and tools.
- The runtime retains the first accepted hint through completion, failure,
  retries, suspension, cancellation, unknown outcomes, snapshots, and rebuilds.
- Readers use valid display ancestry for trees and dataset subtree capture.
- Hints longer than 256 UTF-8 bytes are rejected by the runtime.

Executable checks: sdk-core `activation_display_parent`; TypeScript
`agent-display-parent.test.ts`; runtime `activation::tests::display_parent`;
Studio `timeline-display-parent.test.ts` and `use-processed-events.test.ts`.
These checks establish wire shape and isolated behavior, not deployed parity.
Runtime readers must roll out before SDK writers. Rollback readers must also
understand field-bearing canonical journal records.
