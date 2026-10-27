# Persisted diagnostic access for qualification

The current live gate is unpassed. A read-only sixteen-minute tail probe loses
coverage after about seven minutes with close code `1006`, without a heartbeat
timeout or an observed resource failure. Two sockets do not make the provider
session independent: a live API probe creates three tails, including a rate-one
filter, and receives the same session ID and expiry each time. Both connections
can therefore fail together. Existing failed reports remain failed.

Cloudflare's persisted Workers Logs query API is the next diagnostic source to
evaluate. The current account credential returns HTTP `403`, code `10000`.
The documented permission for its query endpoint is **Workers Observability
Write**. The probe below uses `dry: true`, an aggregate count, an exact disposable
Worker filter and a one-hour time range; it does not create a saved query or
retrieve log rows. A successful access probe is not diagnostic coverage and
cannot pass the gate.

Create a token with only that permission on the existing test account, then save
it locally as `observability-token` in the ignored directory
`examples/cloudflare-serverless/dist/qualification/gate-mvnf29dvfe41`, with mode
`0600`. Do not paste the token into chat, command arguments, tracked config or
reports. The permission permits account log queries; the implementation limits
its requests to the disposable test Worker. Run from the example directory:

```sh
node qualification/persisted-diagnostic-probe.mjs \
  dist/qualification/gate-mvnf29dvfe41/gate.json
```

The checked-in probe validates test scope, file permissions, response size and
the query window. It persists only status, fixed numeric error codes, field
names and the exact test scope. Raw rows and credentials remain unpersisted.

After access works, qualify the backend before using it: verify unsampled
invocation logging for all eight roles, bounded time-window queries and
pagination, ingestion delay, resource/outcome classification, invocation-count
reconciliation, late errors, retention and independent artifact verification.
Missing or incomplete coverage must still fail before consuming recovery
actors. The existing latency, workload, resource and cold-recovery thresholds
remain unchanged. No persisted-log backend is implemented or claimed complete
by this access probe.

References: [Workers Query Builder](https://developers.cloudflare.com/workers/observability/query-builder/)
and [telemetry query API](https://developers.cloudflare.com/api/resources/workers/subresources/observability/subresources/telemetry/methods/query/).
