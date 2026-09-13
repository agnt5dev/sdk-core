# Worker assignment mode (AGNT5-1100)

User-approved default: pull. Explicit push remains supported.

- Absent or empty AGNT5_WORKER_MODE resolves to pull before native startup,
  registration, and async assignment polling.
- Explicit worker options override environment values; existing pull aliases
  retain their precedence. Explicit push selects coordinator-stream dispatch.
- An explicitly configured environment push value remains push.
- Registration sends the resolved typed mode. The coordinator honors that mode
  before legacy metadata; missing mode and metadata resolve to pull.
- Control-plane queries interpret missing/empty metadata as pull.
- With no worker-mode variable, an async submission must execute via pull and
  complete. Both languages backed by sdk-core and pure-Go workers must comply.
- SDK documentation and next minor release notes must call out the new default.

Regression coverage: sdk-core worker_mode tests; Python worker_pull_options;
TypeScript worker.test; Go worker mode and pull transport tests; coordinator
registration mode tests. Deployed async-submission parity is a separate gate.


## Graceful pull shutdown (AGNT5-1129)

- Ctrl+C and Unix SIGTERM enter the same intentional worker shutdown path.
- Stop issuing polls and cancel unresolved idle polls promptly. If a poll reply
  and shutdown are both ready locally, consume the reply and drain its assignment.
- Already received pull assignments keep their handlers, lease renewals, lifecycle
  flushing, and fenced CompleteJob retries alive until acknowledgement.
- One 25-second budget bounds the entire pull drain, including completion work;
  it is not multiplied by the number of slots. This leaves cleanup time inside
  the operator's default 30-second interval after preStop. Push drain is unchanged.
- At the deadline, invoke cooperative language cancellation and stop owned tasks,
  including lease renewal RPCs. Do not guess a release or accelerate lease expiry.
- A grant committed remotely while its poll reply is being cancelled remains an
  ambiguous outcome. Its original runtime lease and fencing rules still apply.
- Supersession and uncertain transport disconnection do not imply graceful
  shutdown or authorize a new lease outcome.

Regression coverage: `worker::pull_shutdown_tests` exercises a local tonic Engine,
accepted handler and completion-ack barriers, idle polls, simultaneous ready reply,
a paused-clock deadline, and SIGTERM sent only to a dedicated child test process.
