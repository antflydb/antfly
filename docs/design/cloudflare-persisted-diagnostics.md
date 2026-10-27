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
remain unchanged. The access probe itself does not establish completeness.

## Bounded collector and independent evidence verification

`qualification/persisted-diagnostics.mjs` now implements a read-only collector
for one exact disposable Worker and a window of at most sixty seconds. It
checks a count aggregate before and after cursor pagination, rejects incomplete
queries, changing counts, repeated event/invocation identities and unknown
schemas/outcomes, and requires both query ABR level one and ingestion
`sampleInterval` one. Query sampling and ingestion sampling are separate checks.
Pagination uses the documented last-event `$metadata.id` cursor with
`offsetDirection: next`.

Each response is limited to 4 MiB; each window is limited to 64 requests,
64 MiB of responses and 20,000 events. Requests execute sequentially. Provider
text and HTTP context never enter the report: event and invocation identities
are SHA-256 digests, while log text passes through the existing metric/error
allowlist. Resource signals, truncation and abandoned native I/O remain fatal
observations rather than being hidden by a successful HTTP response.

After the access probe succeeds, use a historical, settled window to exercise
all eight roles of one pair:

```sh
node qualification/collect-persisted-diagnostics.mjs \
  dist/qualification/gate-mvnf29dvfe41/gate.json 3 FROM_MS TO_MS
```

`TO_MS` must be at least sixty seconds old. The command repeats the same window
after at least thirty seconds, requires matching sanitized event-set digests,
and writes sixteen private artifacts in a new ignored directory. The independent
`verifyPersistedProbe` reader checks exact role/path scope, file lengths and
hashes, event-set hashes, duplicate identities, both observations and recomputed
resource/gap counters. Total artifact bytes are bounded to 256 MiB. Truncated,
substituted or missing files fail verification. Existing failed gate evidence
is never repaired or overwritten.

This collector is implemented and covered by adversarial local fixtures, but
has **not been verified against live API responses**: the requested token is
still absent. Provider field conventions are strict hypotheses until the live
probe confirms them. The CLI always reports `qualification: false`, and the
gate evaluator does not accept these artifacts as a substitute for live tails.
Stable query counts only prove consistency of the stored result set; they do
not prove the platform stored every invocation.

Promotion still requires live schema validation, deployment/version binding,
unsampled invocation configuration on all eight roles, independent expected
invocation accounting (including background Queue/alarm work), ingestion and
retention measurements, settled-window scheduling with boundary deduplication,
and late-error reconciliation across the entire measured execution. A quiet
role with zero stored events is not a coverage proof. None of those requirements
can be replaced by two matching query results or a newly permissive token.

References: [Workers Query Builder](https://developers.cloudflare.com/workers/observability/query-builder/)
and [telemetry query API](https://developers.cloudflare.com/api/resources/workers/subresources/observability/subresources/telemetry/methods/query/).
