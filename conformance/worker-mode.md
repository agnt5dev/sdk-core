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
