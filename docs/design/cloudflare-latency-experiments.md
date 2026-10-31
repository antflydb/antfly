# Native latency experiments

These experiments extend the object-native design without changing acceptance
thresholds, resource caps or the approved `c44896ac8c` control. New runtime
behaviors are opt-in. Local injected-delay results characterize dependency
depth, not Cloudflare placement, billing or production SLOs.

The proposed next implementation order is in
[Cloudflare runtime architecture steps](cloudflare-runtime-target.md).

## Experiments and controls

The baseline native modules are copied from implementation `80615f8acf` before
editing and SHA-256 bound in reports. `qualification/latency-matrix.mjs` compares
that module, the current module with inline bodies disabled, and the current
module with them enabled. It checks exact bodies, updates, deletion and counts
at 512 and 4,096 documents. Other current changes are disabled in those cases.

`qualification/local-worker-latency.mjs` exercises actual workerd with separate
app/writer/reader/builder Workers, four independent tenant domains, native R2
coordination, exact bodies and bounded pools. Its variants include the previous
native modules, all new behaviors disabled, each behavior separately and the
combination. A request-owned R2 wrapper injects a declared delay. It reports the
complete measured population, including initial instantiation; small samples
and run order can produce cold-start outliers. Results are not gate evidence.

`qualification/parallel-io-latency.mjs` compares serial execution with the
existing bounded four-way upload executor using actual native publication.
Both modes perform 63 operations / 46 PUTs and validate results; it does not
increase the production concurrency limit.

At 20 ms injected delay, the 512-document native point-read comparison removes
one GET: immutable-object-cache-cold p95 falls from about 223 to 196 ms, warm p95 from 173 to 148 ms.
At 4,096 documents warm p95 falls from about 169 to 149 ms. Retained fixture
payload grows about 27–29%; this is not a measured total-isolate-memory result.

The four-tenant workerd experiment at 20 ms records current-disabled versus
combined read p95 of about 206/185 ms and write p95 of 241/208 ms. At 60 ms it
records about 526/462 ms for reads and 599/527 ms for writes. Separate warm
medians also improve for optimistic writes. Initial instantiation causes large
outliers in some individual variants; no isolated p95 ratio is a production
speedup claim. The fused-only stable-checkpoint case does not establish a win.

The existing parallel executor takes about 822–831 ms versus 1,328–1,365 ms
serially under 20 ms injected delay, with identical operation counts. This
confirms an already implemented optimization rather than a new deployment gain.

## Fresh query execution before obsolete proof drain

`FUSED_READS=true` publishes a freshly authenticated, pinned operation view to
its submitting request while obsolete speculative proof I/O finishes. It can
start native lookup at that point. Each caller registers its own promise
continuation; the cohort owner only resolves it. This preserves originating
Worker I/O context for concurrent cohort members.

Both lookup and metadata observation must settle before responding. Admission,
original deadlines and error paths remain owned until all started I/O drains.
Metrics emission is deferred until that join. The view is never reused across
requests and speculative bytes cannot replace fresh native authority. Actual
workerd tests cover direct reads, tenant isolation, physical reopen and caller
contexts, including this mode.

## Native facts with bounded inline point bodies

`INLINE_POINT_BODIES=true` allows eligible facts records to carry a body copy
inside the authenticated COW leaf. Eligible keys are at most 256 bytes and
simple bodies at most 1,536 bytes. Stored/enrichment bodies with dependencies
retain the existing external representation. Existing standalone and packed
body artifacts remain durable and reachable for ordinary readers, scheduling
and collectors; this experiment trades extra tree/storage bytes for a point-read
GET. It does not remove their retention requirements.

Facts metadata version 11 uses `AFDFACTB` roots. Roots without inline records
continue emitting version 10 / `AFDFACTA`. The bounded `AFDINL01` record wrapper
contains a legacy fact plus exact body bytes. Decoders validate lengths, the
body envelope digest, source LSN and namespace/root provenance. Wrappers are
padded to at least 185 bytes, above every legacy record length, so a legacy
digest beginning with the wrapper magic cannot be misclassified. A point reader
copies bytes while its cursor still owns the leaf; general fact decoders never
return borrowed page memory. Audit and pending indexes retain the compact legacy
fact instead of duplicating inline bodies across every scheduling tree.

Deploy compatible readers/builders/collectors before enabling new writes.
Disabling the flag stops new inline copies but retained version-11 generations
still require new readers. Existing version-3 through version-10 facts remain
supported. Native tests check corrupt digests, invalid lengths and allocation
ownership. Workerd collection/reindex/restart/storage-loss tests exercise inline
facts as well as ordinary formats.

## Optimistic WAL preparation with authoritative final validation

`OPTIMISTIC_WAL=true` uses an already committed inline capsule as a preparation
hint. The native commit still performs fresh recovery/fencing, validates the
expected WAL ETag and bounded source dependencies, then publishes WAL HEAD and
derived-work admission atomically. A stale capsule cannot acknowledge a write.
Pins must already be live; preparation cannot synthesize or renew protection.
Hints own their bytes, expire and remain bounded. Replays return through fresh
admission rather than treating a retained receipt as authority.

The shared object-store native `commit_wal` action accepts at most four distinct
source dependencies, each at most 8 KiB and 16 KiB total. Absent records must
remain absent; present encoded values must match exactly. Invalid or stale
inputs cannot publish a WAL/receipt root. Cached preparation conflicts discard
the hint and retry under the original operation identities, deadline and I/O
budgets. Only recognized missing-object errors from speculative preparation
select a fresh retry; corruption/resource failures remain failures.

Local tests cover changed/absent dependencies, byte ownership/expiry, receipt
replay, a skipped second snapshot, and storage loss/restart. Native owner-fencing
and ambiguous-commit tests remain required.

## Native compaction classification

`NATIVE_COMPACTION_PLAN=true` first runs bounded native source validation with
`compaction_probe_only`. A paged no-op returns through its existing snapshot
path without acquiring a publication lease. A flat writing case signals
`CompactionNeedsPublicationLease` before acquiring a writer lease; the host then
acquires a native lease and uses staged metadata with atomic finalization.
Source protection is re-established under that lease.

Both passes share one original deadline and aggregate native operation/transfer
budgets. Source/manifest bytes already fetched can use the immutable cache.
Compaction does not feed publication throughput adaptation. Keyed compactions
retain their existing durable receipt path, and unkeyed transport failures are
still never blindly replayed. Workerd tests verify the actual head-changing and
paged no-op paths, lease release, exact results and platform-storage loss.

## Cloudflare comparison

Every experiment declares feature flags in the sealed runtime profile. Worker
placement is separately recorded when configured. Local runs cannot establish
physical placement; the Cloudflare experiment must inspect actual placement
status/header evidence. Smart Placement can require multiple-location traffic
and up to fifteen minutes of analysis; a configured mode alone establishes no
colocation or improvement. See [Cloudflare placement](https://developers.cloudflare.com/workers/configuration/placement/).

Use new paired corpus identities while preserving prior reports/data and the
approved control bundles. Deploy all compatible native roles before enabling
inline writes. Seed the declared workload, run short diagnostic comparisons,
then pause delivery and retain independently verified reports. No reserved cold
epochs are consumed by these experiments. Gate qualification still requires
complete diagnostics, exact verification and all unchanged hour/cold populations.

## Validation before deployment

The final modules pass 269 Node and 25 actual-workerd tests. The full native
suite passes 661 tests (six skipped); the final codec guard additionally passes
all 17 focused facts tests. The deployed full module SHA-256 is
`c0f96a553ebdeb2be64c94d1cdbb3aa6bd753c8666282a029aa56af68550e8ce`.
Earlier local experiment reports retain their actual intermediate module hashes;
they are not rebound to this build.

## Wall deadline headroom

The first Cloudflare seed exposed successful publication slices dominated by
20–25 seconds of storage I/O, followed by deadline cancellations. Compute-only
feedback can immediately regrow a recovered prefix into that deadline again.
The follow-up host guard keeps the native deadline at 30 seconds and uses a
22.5-second wall headroom target alongside compute, transfer, operation and
working-set guards. Growth additionally requires wall use below 80% of that
target. Fixed network wait still does not count as compute. Local regression
cases cover slow successful slices, preventing immediate regrowth, and recovery
when wall headroom returns. The first deployed experiment predates this guard;
its reports must not be attributed to the follow-up implementation.

A repeat of the point experiment on the final codec module at 512 documents
records no-inline versus inline p95 of 224/207 ms with the immutable cache
cleared, and 176/158 ms warm. The GET reduction and retained-payload tradeoff
remain the same as the earlier intermediate-module experiment. The final codec
repeat at 4,096 documents records warm p95 174/155 ms and cache-cleared p95
220/198 ms, with about 27% additional retained payload.

The first fresh 512-document-per-tenant Cloudflare preparation was interrupted
after about eighteen minutes once the control stalled at publication budget one
with repeated cancellation. The candidate had completed two tenants and begun
the third. This is failed preparation evidence, not a completed latency trial;
it cannot attribute the difference to inline bodies because locality differs
and both profiles record cancellations. Neither limits nor the gate population
were changed. The diagnostic driver additionally accepts explicit 64/128-document
corpora, records their size and remains `qualification:false`; strict gate runs
continue requiring 512 documents per tenant. Smaller diagnostic corpora isolate
steady-state/placement experiments from the bulk-preparation blocker.

## Remaining bulk-publication boundary

The control's budget-one cancellation shows that lowering a record prefix alone
cannot guarantee a useful slice fits the fixed deadline under slow storage. It
does not prove whether minimum-unit work, storage locality or a particular
artifact phase dominates; retain per-phase I/O evidence before choosing a fix.
The durable long-term extension is a native resumable publication plan: persist
validated source/prefix identities and content-addressed intermediate outputs,
bound each build/upload slice, upload independent immutable pages in bounded
waves, and atomically publish HEAD only after the whole planned prefix is ready.
Reacquire and revalidate fenced ownership/source protection after reconstruction;
never expose a partially built prefix or retry an ambiguous unkeyed mutation.
The wall-headroom guard reduces regrowth into deadlines but does not implement
that minimum-unit split or establish a Cloudflare throughput improvement.

## Completed Cloudflare diagnostics

All three timed comparisons use four tenants with **128 documents each**, the
same combined candidate (`daca7145ad`, full module `c0f96a55…`) and unchanged
approved `c44896ac8c` control. Each side offers 72 reads, 24 writes and eight
unkeyed compactions over three minutes. All read/write populations complete and
final tenant counts are checked. These are end-to-end client p95 values, not
CPU measurements. The later wall-headroom guard is locally tested but was not
part of these deployed bundles.

| Candidate placement | Control read/write p95 (ms) | Candidate read/write p95 (ms) | Compaction connection resets, control/candidate |
| --- | --- | --- | --- |
| Default | 5,993 / 7,796 | 1,291 / 4,851 | 0 / 0 |
| Smart | 7,305 / 10,267 | 1,670 / 4,960 | 1 / 2 |
| Explicit `aws:us-west-2` | 6,133 / 7,628 | 1,349 / 5,041 | 3 / 4 |

The default run records no client logical errors. The other runs retain the
unreplayed `ECONNRESET` compaction failures and failed round markers. A completed
read/write latency population does not make those workloads successful.
Production maintenance should use the existing keyed compaction receipt/status
path; an ambiguous unkeyed transport outcome is never blindly replayed.

Smart's app status reaches `SUCCESS`, while private roles report
`INSUFFICIENT_INVOCATIONS`. Its recorded response placement headers are all local
(SJC, PDX and one LAX), establishing no remote placement benefit. Explicit region
placement records 161 `remote-SEA` and two `local-SEA` responses, proving that
relocation took effect. It does not prove proximity to R2. Neither mode improves
on the observed default p95 pair. These small, ordered runs and changing control
latencies do not establish a statistically stable placement effect. Placement
only affects fetch handlers, not the Durable Object RPC work.

The control predates other implementation improvements, so the large
candidate/control gap does not isolate the five new experiments. Local matrices
supply the individual experiment comparisons. The cloud runs show that the
combined deployment still misses the 1,000 ms read and 3,000 ms write targets,
even on the smaller diagnostic corpus.

No experiment qualifies the gate. All sixteen persisted observations per timed
run settle with matching repeated hashes, but independent invocation counts and
transport/acknowledgement/version reconciliation remain incomplete. Live capture
also has gaps. No resource failure is observed in available live journals or
client responses; incomplete coverage cannot establish an exhaustive zero.
At measurement completion, all owned tail sessions were removed, all six stacks
were paused/sealed, the
approved control bundles are unchanged, and all 300 reserved cold epochs remain
unused. The 512-document preparation failure and prior 108-artifact gate evidence
remain preserved separately from this experiment's content-addressed manifest.

The immediate next performance work is the native resumable publication boundary
above and a native read plan that overlaps immutable query inputs with fresh
authority validation. The scheduling experiment alone does not implement that
full plan. Keep the inline-body tradeoff opt-in until bulk publication and the
unchanged full-size gate are qualified; placement is not a substitute for
reducing dependent storage waves.


## Post-test resource retirement

Following the already approved post-test cleanup, all six disposable stacks were
removed. Account API inventory verifies absence of their 24 Workers, 18 original
Durable Object namespaces, 12 Queues/DLQs and six R2 buckets. No other
`antfly-1278-` test Workers, buckets or queues were found. The six local disposable
API-token files were removed. Local reports and frozen bundle provenance remain
preserved; the old cold reservations were never used. Future qualification must
provision fresh stacks rather than reuse deleted run manifests.

Cleanup uses a standalone authenticated R2-only Worker, stops producers before
emptying storage, bounds work to 1,000 deleted objects/eight list steps per
invocation with a two-second continuation headroom check, and requires a final
empty invocation before bucket deletion. Pending storage calls can outlast the
headroom check; it does not cancel or falsely acknowledge them. Bounded retries
handle lost drain acknowledgements without claiming exact deletion counts.
The adapter may return fewer list entries than requested. All 273 Node tests
pass, including scope/auth rejection, short-page batching, failed deletion,
ambiguous response handling and bounded retry/page limits.

[R2 retained storage](https://developers.cloudflare.com/r2/pricing/) can remain
billable while compute is paused. Account-wide subscription charges and usage
already incurred are separate from deletion; see
[Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/).
The cleanup verifies resource absence, not a zero account invoice.
