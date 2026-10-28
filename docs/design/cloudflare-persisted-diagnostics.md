# Persisted diagnostic access for qualification

The current live gate is unpassed. A read-only sixteen-minute tail probe loses
coverage after about seven minutes with close code `1006`, without a heartbeat
timeout or an observed resource failure. Two sockets do not make the provider
session independent: a live API probe creates three tails, including a rate-one
filter, and receives the same session ID and expiry each time. Both connections
can therefore fail together. Existing failed reports remain failed.

Cloudflare's persisted Workers Logs query API is the next diagnostic source to
evaluate. The original Wrangler credential returns HTTP `403`, code `10000`;
the subsequently supplied observability token passes the access probe with
HTTP `200`. Authorization is no longer the diagnostic blocker.
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

This collector is covered by adversarial fixtures and selected live API checks.
The CLI always reports `qualification: false`, and the gate evaluator does not
accept these artifacts as a substitute for live tails.
Stable query counts only prove consistency of the stored result set; they do
not prove the platform stored every invocation.

Promotion still requires live schema validation, deployment/version binding,
unsampled invocation configuration on all eight roles, independent expected
invocation accounting (including background Queue/alarm work), ingestion and
retention measurements, settled-window scheduling with boundary deduplication,
and late-error reconciliation across the entire measured execution. A quiet
role with zero stored events is not a coverage proof. None of those requirements
can be replaced by two matching query results or a newly permissive token.

## Live access and schema checks

The supplied private token succeeds against the scoped query endpoint. No
additional permissions are needed for these probes. Live responses expose
three compatibility defects in the initial offline collector:

- `view` belongs at the request root. Placing it inside `parameters` is silently
  ignored: an events request returns calculations instead of log rows.
- Actual custom console logs use metadata type `cf-worker`, alongside invocation
  type `cf-worker-event`. The decoder retains `cf-worker-log` compatibility and
  rejects unknown types.
- Serialized metric objects must not be treated as exception text. Combining
  `cpuTimeMs` and a later memory-limit field name in serialized JSON falsely
  matches a broad CPU-error recognizer. Only the provider's actual error field
  becomes an exception; structured logs retain their existing sanitizer. The
  initial probe's 134 apparent CPU failures were parser false positives, not
  source Worker resource failures. That failed probe stays preserved.

The API also returns an average sample interval of zero for zero matched rows.
The collector accepts that only with exact zero counts, an independently empty
events page and a repeated zero count. This establishes an empty stored set,
not unsampled delivery or workload coverage.

On historical window `1793122393713..1793122453713`, two separated baseline-writer
queries reconcile 639 events / 145 invocations, with identical sanitized event
hashes and zero observed resource failures, terminal invocation errors or gaps. All eight current script-settings
responses confirm logging, invocation logs and persistence enabled, with both
root and log head-sampling rates equal to one.

Nevertheless, this historical window reports query ABR level one but ingestion
sample intervals above one: baseline builder `1.100558659217877`, candidate app
`1.0895522388059702`, writer `1.0833333333333333`, and builder
`1.3333333333333333`. The candidate reader has zero matched events. These are
not authorization failures. The collector correctly rejects the sampled
population with `DiagnosticIngestionSampled`; configured rates alone cannot
prove complete delivery. Cloudflare's [query API](https://developers.cloudflare.com/api/resources/workers/subresources/observability/subresources/telemetry/methods/query/)
documents ingestion sampling separately from query ABR. The cause of the
historical sampling is not established by current settings; no account quota,
billing, sampling policy, Worker deployment or acceptance threshold was changed.

The next coverage prerequisite is a controlled active window with verified
unsampled delivery and independent invocation accounting, or a durable capture
path that meets the same requirements. The existing sampled history cannot
establish the missing events retroactively. Eight-role whole-execution
qualification remains incomplete; all reserved cold epochs remain unused.

References: [Workers Query Builder](https://developers.cloudflare.com/workers/observability/query-builder/)
and [telemetry query API](https://developers.cloudflare.com/api/resources/workers/subresources/observability/subresources/telemetry/methods/query/).


## Physical attempts and invocation accounting

The common driver now records each physical HTTP attempt, including admission
retries and transport failures, in a bounded 20,000-entry ledger. A unique base36-encoded UUID
query parameter identifies each attempt without changing the frozen control's
source or request body. Reports retain timestamps, status, deployment UUID and a
SHA-256 digest of the normalized response Ray ID; they retain no raw Ray IDs,
HTTP URLs, headers, credentials or document bodies. Overflow is a sticky failure.
The declared request-identity policy applies equally to both sides.

Persisted window schema two adds hashed attempt and Ray identities, deployment
UUID and an allowlisted invocation type. The independent reader continues to
verify original schema-one artifacts. Accounting checks four exact role scopes,
contiguous declared windows, deployment versions, duplicate/conflicting event
identities, every acknowledged request and its Ray ID, and unfinished attempts.
Counts must come from a separately scoped, unsampled platform counter with the
same Worker and time range. Empty logs, missing counters, estimated counts and
ambiguous transport failures cannot silently establish coverage. These checks
remain diagnostic-only; the gate evaluator has no new acceptance path.

The fresh workload command is:

```sh
node qualification/run-coverage-probe.mjs   dist/qualification/gate-mvnf29dvfe41/gate.json 3 --seconds=600
```

It requires a sealed, deployed, paused pair with unused recovery epochs, resumes
only that pair, captures eight auxiliary live streams, drives the existing
fixed-arrival workload, and pauses both sides in cleanup. It audits one interior
minute twice after settlement. It explicitly does not claim whole-execution
coverage or exact body/receipt verification. Each invocation writes a distinct
private report directory; subsequent versions also snapshot selected diagnostic
driver sources. Live socket gaps remain recorded even when this diagnostic
experiment continues collecting load. This behavior does not change the strict
gate's immediate cancellation rule.

Independent counter exploration uses Workers `workersInvocationsAdaptive` for
public invocations and Durable Objects
`durableObjectsInvocationsAdaptiveGroups` for private RPC invocations. Historical
Worker and writer aggregates both report sampling above one. Some private-role
rows are unsampled, but that cannot establish an exact population for all roles
or account for Queue/cron/alarm invocations through the wrong dataset. See
[Durable Object metrics](https://developers.cloudflare.com/durable-objects/observability/metrics-and-analytics/).
The dedicated observability token works; more token permission does not repair
sampled data.

A durable export is the next transport candidate, not an automatic coverage
proof. An isolated Tail Worker should sanitize before persistence, write bounded
immutable batches to an observability-only R2 prefix/bucket, retain terminal
outcomes after `waitUntil`, and independently verify object hashes and settled
execution windows. Keep its storage and CPU outside the database's admission
path; instrument the same producer roles on both sides. Never use timestamps as
unique invocation IDs. HTTP identities can use the ledger; private/background
identities and independent exact totals still need a verified source. Missing
IDs, dropped batches, unknown versions, exporter failures, sampling or missing
expected invocations must fail closed.

Cloudflare documents a Tail Worker invocation after producer execution, but its
[handler schema](https://developers.cloudflare.com/workers/runtime-apis/handlers/tail/)
does not promise unique private-RPC identities or an at-least-once export
contract. R2 durability only protects batches that reach R2; it cannot recover
an invocation never delivered to the exporter. Validate those boundaries with a
small canary and independently counted injected failures before connecting a
new transport to gate evaluation. Keep the frozen control, original failures,
thresholds, and all recovery epochs intact until that prerequisite succeeds.


The fresh source confirms URL redaction replaces raw UUID query values with
`REDACTED`. The driver now encodes the same 128-bit UUID as a fixed-length,
lowercase base36 value with a non-hex prefix; this avoids the documented hex and
mixed-case base64 redaction shapes without requesting unredacted headers. The
decoder validates range and canonical encoding before producing the same hashed
UUID identity. Old failed UUID-tagged reports remain unchanged. See Cloudflare's
[redaction rules](https://developers.cloudflare.com/workers/runtime-apis/handlers/tail/#tailrequest).
The live source also returns console events with the same metadata event ID
and timestamp in two distinct invocations, with distinct payloads. Schema three
therefore hashes the tuple `(requestId, metadata.id)` as the event identity,
retaining both events and their resource signals. Repetition of that tuple
remains fatal; it never deduplicates divergent rows or decreases expected counts.
The verifier preserves schema-one and schema-two compatibility. Pagination still
uses the documented raw metadata cursor and fails closed if a repeated or
incomplete page cannot reconcile the exact population. This identity correction
does not repair ingestion sampling.

## Fresh native protocol window

The October 27 native deployment produces stable, unsampled persisted windows
for all eight roles during one interior minute. Acknowledged HTTP attempt/Ray
identities reconcile when window membership explicitly uses response completion.
`reconcileInvocations` retains request-start membership as its default; reports
record the selected basis. This is not an arbitrary clock tolerance and does
not forgive missing or unexpected identities.

`independent-counters.mjs` now captures the exact disposable Worker scopes and
bounded minute twice, checking integer totals, sample intervals and settlement.
It deliberately fails on empty adaptive results and on DO/Worker population
mismatches. Durable Object RPC totals need a separate namespace-scoped dataset;
Worker totals alone cannot cover mixed builder invocations. Confidence sample
sizes corroborate two active DO scopes, but quiet-role and whole-execution
coverage are still unproven. The collector cannot pass the qualification gate.

See [the live diagnostic](cloudflare-native-live.md) for immutable report IDs,
latencies, the preserved setup-cutoff failure and the validated idempotent seed
resume. Metrics sanitization now retains bounded publication progress, native
WAL plan spans, numeric CPU/wall times and deployment UUIDs; it still discards
request bodies, headers, credentials and arbitrary strings.
