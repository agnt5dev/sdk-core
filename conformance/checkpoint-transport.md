# Current Engine checkpoint transport (AGNT5-1002)

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
