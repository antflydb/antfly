# Native latency experiments

These experiments extend the object-native design without changing acceptance
thresholds, resource caps or the approved `c44896ac8c` control. New runtime
behaviors are opt-in. Local injected-delay results characterize dependency
depth, not Cloudflare placement, billing or production SLOs.

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
remain the same as the earlier intermediate-module experiment.
