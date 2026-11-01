# Platform baselines, request attribution and memory isolation

This follows the independent-client work for #1278 / draft PR #1320. It measures
transport, owner RPC, local storage and R2 before assigning the application's
second-scale tails to an inherent provider floor. No numeric gate policy,
approved hosted control module, durable write contract or memory ceiling changes.

## What is implemented

The authenticated primitive fixture uses two Workers, one SQLite-backed Durable
Object namespace and one R2 bucket per fresh stack. All operations have fixed
keys, bounded input and a fixed 1 KiB response payload with bounded diagnostic metadata; it accepts no arbitrary SQL, URLs,
object names, memory sizes or credentials. Operations are app echo, service echo,
owner echo, owner storage GET, owner storage PUT plus sync, R2 GET and conditional
R2 PUT, including R2 calls from the owner isolate holding the synthetic memory. Successful writes are read back outside their timed population. Owner
storage uses the platform API without an application SQL schema. This fixture
isolates the provider's storage from the Antfly engine; it does not replace it.

Each profile records an unloaded serial population, then fixed arrivals at
16 and 64 requests/s. The external Node transport owns its connection pool;
client overload, late dispatch, failed bodies, changed app/private versions,
owner reconstruction and journal errors prevent a clean result. No workload
request retries. Intents are synced before each timed population; terminals and
partial case results remain on disk. Timing residuals subtract only durations
from the same request. They include scheduling/routing/body transfer and must
not be called pure network time. The local monitor is not a production memory
profiler and does not measure total isolate memory.

The three independent configurations are:

| Profile | Stored value | Synthetic WASM linear memory | Retained bytes |
| --- | ---: | ---: | ---: |
| Primitives | 1 KiB | 0 | 0 |
| Larger storage | 256 KiB | 0 | 0 |
| Memory pressure | 256 KiB | 32 MiB | 8 MiB |

These are bounded pressure factors, not Antfly native-module execution. Fresh
runtimes prevent earlier allocations from contaminating later profiles. A
separate fresh-owner echo population uses unique actor IDs and no storage or
synthetic pressure. It proves new actor construction, not a new physical Worker
isolate or a data-cold R2 read. The fixed seed is hot after setup; object-store
cache misses require a separate cold-object experiment and cannot be inferred
from this result. TLS setup is included only when the client actually makes a
new connection; ordinary populations reuse a pool.

## Complete inline phase attribution

`QUALIFICATION_PHASE_PROFILE=true` adds diagnostic response attribution to the
current candidate's HTTP and resident-read/grouped-write paths. It is off by
default. Native code, execution limits and acknowledgment behavior are unchanged.
The owner returns bounded plain numeric metadata through the existing RPC; the
app removes it from the response body and emits the private diagnostic header.
Cohorts and write groups retain all caller trace IDs. Internal metadata is
stripped even when the flag is disabled. Payloads, object keys, account secrets,
request handles and arbitrary exception messages are excluded.

The independent client can require owner profiles for a profiled target. Missing,
truncated, error-bearing or mismatched profiles fail attribution. Activation
fields describe configured flags; physical calls/cache-hit metrics establish
what actually happened. Worker timers are advanced by I/O: zero spans are not
zero CPU. Profiling in production remains necessary to measure CPU.

`analyze-request-phases.mjs` reports per-request client, HTTP, RPC, owner,
admission, native planning/finalization, dependency-wave and physical-I/O
statistics. It never subtracts independent percentiles or timestamps from two
isolates. Nested spans are not additive. Shared cohort/group spans are not
independent CPU observations for every member. Inline attribution supplements
canonical/native service evidence; it does not close resource coverage.

The existing actual-workerd/native experiment accepts `--phase-profile` and
retains profiles with every measured read/write and reconstruction sample.
The first run covers all 128 measured mixed requests without missing/invalid
profiles. With 20 ms injected binding delay it records roughly 191 ms read p95
and 272 ms write p95. These are local diagnostic results, not hosted forecasts.
Its exact saved bundles serve as the local before-change control for the
batch-window experiment; they are not the approved hosted `c44896ac8c` control.

## Queueing change

Mutation groups retain the existing two-millisecond coalescing window. With
`MUTATION_ELAPSED_BATCH_WINDOW=true`, an opt-in queued group counts time it already waited behind the previous durable commit,
so handoff does not add another unconditional timer. The callback stays in
its submitting RPC context. Queue/byte/record bounds, fairness, keyed receipts
and durability-before-response remain unchanged. A blocked-commit test proves
that later groups skip the expired window and no caller is answered early.
The flag defaults off pending isolated paired evidence. This is a small
scheduling change, not a twofold latency claim.

## Reproduction and operation

Run the primitive/pressure matrix locally:

```sh
node qualification/run-platform-baseline-local.mjs
node --test test/platform-baseline-workerd.test.mjs
```

Do not run other local performance experiments or builds concurrently when
making comparisons. Preserve incomplete/failed populations. Local SQLite/R2
bindings cannot establish a Cloudflare latency floor or rule out a hosted
`SQLITE_NOMEM`. A successful pressure matrix narrows reproduction conditions;
it does not identify the historical root cause.

For a fresh hosted diagnostic, first prepare the reviewable manifests:

```sh
node qualification/platform-baseline-lifecycle.mjs prepare ACCOUNT_ID primitives
node qualification/platform-baseline-lifecycle.mjs prepare ACCOUNT_ID large
node qualification/platform-baseline-lifecycle.mjs prepare ACCOUNT_ID pressure
```

On clean committed source, each emitted run supports `seal`, `deploy`,
`upload-token`, `measure`, then `cleanup`, with the manifest path as its argument.
Only newly generated disposable app tokens are installed. Existing account
credentials stay with official local Wrangler authentication. Runtime/config
hashes and every source/bundle version are retained. A measured manifest cannot
be replayed. Hosted runs from the local machine are explicitly external-client
diagnostics, not GitHub-hosted independent qualification.

All stacks request Western North America placement. Hints are best effort;
observed app colo does not prove owner or R2 geography. Compare against another
placement only with fresh runtime identities and storage names, preserving both
populations. The existing read-only Antfly placement probe remains available
for matched application stacks. Do not probe an actor before a physical-cold
population: probing warms it.

Cleanup first replaces the public gateway with a sealed drain-only worker,
removes the private Worker/namespace, waits for quiescence, drains the exact
owned bucket through awaited bounded pages, then removes the gateway and bucket.
Its new disposable token is removed last; reports and hashes remain local.
The fixture creates no Queues, cron jobs or log export destinations.

## Interpretation and next decisions

A low-load baseline is an empirical envelope for the tested placement, connection
state, payload and day. Minimum observed latency is not an invariant lower bound;
p95 under load depends on queueing. Expand samples and client locations before
setting a product SLO. Keep the original 1,000/3,000/5,000 ms gate policy intact.

If Antfly exceeds the matched owner/storage baseline, use the inline profiles to
choose among read/write queue isolation, fewer serial RPC/dependency waves,
bootstrap/layout changes and existing cache/affinity/group-commit activation.
Do not add speculative caches when activation/hit evidence is absent. A durable
R2 write cannot acknowledge before its required commits under the current
contract. A faster provider-local log with asynchronous R2 checkpointing would
be an explicit, separately qualified durability/authority protocol, including
S3/GCS ownership and recovery semantics.

`SQLITE_NOMEM` remains open until a stable reproduction and actual fix are
validated. Response failures, recovered log errors, total platform outcomes and
native/canonical windows must all be considered. Synthetic linear/retained-byte
counts exclude JS objects, serialization, compiled code and provider storage
memory. Passing this fixture does not constitute a memory-error or full-gate pass.


The hosted execution captures best-effort sanitized Tail for both Workers and
rejects observed resource errors. Tail may be incomplete; a clean fixture is
not a full resource-window pass. Unloaded, offered-load and fresh-owner
populations remain separate. The response profile validator accepts the
existing camel-case RPC span labels; invalid/omitted labels remain explicit
in the dropped count, and rejected profile headers are retained for inspection.
