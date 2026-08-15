# External worker bootstrap v1

This language-neutral contract defines how an SDK worker running outside an
AGNT5-managed data plane obtains placement and authenticates to the runtime.
Python and TypeScript inherit the Rust core implementation; Go implements the
same observable behavior natively.

## Activation

The external path is selected when `AGNT5_API_KEY_FILE` is set, when
`AGNT5_EXTERNAL_WORKER=true`, or when `AGNT5_API_KEY` is present without legacy
runtime routing coordinates. Existing workers with explicit coordinator,
engine, project, or deployment coordinates remain on the managed/local path.

Configuring both key sources fails closed. `AGNT5_EXTERNAL_WORKER=true` without
one key source also fails closed. File credentials are trimmed when read and
must never be added to logs, runtime metadata, or gRPC request headers.

## Discovery and exchange

1. `POST /api/v1/worker-discovery` sends the optional `AGNT5_ENVIRONMENT`
   selector and the bootstrap credential in `X-API-KEY`.
2. A successful response supplies immutable project, environment, deployment,
   worker-pool, placement, runtime endpoint, and protocol authority.
3. The SDK accepts only `customer_docker` or `customer_kubernetes`, `pull.v1`,
   and verified HTTPS endpoints outside explicit loopback development.
4. `POST /api/v1/worker-token` exchanges the same bootstrap credential and the
   discovered authority for a short-lived bearer token.
5. Runtime gRPC calls carry only that bearer token. The SDK refreshes it once
   within 60 seconds of expiry and shares the refreshed result across slots.

## Resilience cases

| Case | Required behavior |
| --- | --- |
| Bootstrap redirect | Reject; credentials are never forwarded to redirects. |
| Missing or changed authority | Reject before runtime registration. |
| Expired token | Refresh before the next authenticated RPC. |
| Concurrent refresh | Perform one exchange and reuse its result. |
| Transport interruption | Retry discovery/connection without exposing the bootstrap key. |
| Queued work after reconnect | Produce exactly one terminal completion. |
| Revoked bootstrap key | Reject the next discovery or refresh; an issued token follows its bounded lifetime. |

The isolated parent-repository Hybrid Docker smoke exercises all three SDKs,
the real authenticated runtime listener, a controllable worker-transport fault,
reconnect, revocation, and exactly-once terminal completion.
