# Separate Vector Store: Experiment Log (September 2026)

> Relocated verbatim from `zig/VECTOR_STORE.md` (lines 5-85 and 280-3480 at commit 271838a195) on 2026-09-16 during the documentation cleanup, with the completed pre-refinement 1M repeat qualification (lines 3838-3864) appended below. This is a historical implementation log kept for context; the living design is [`VECTOR_STORE.md`](../../../zig/VECTOR_STORE.md). Durable decisions from this log were folded into that document before the move.
>
> Relative links were re-based to this file's new location; no other text changed. Links into `.benchmark-results/` point at the untracked local results directory at the repo root, as they did before the move.

## September 14: scoped default promotion

New local, single-shard standalone tables now select `vector_store` when the
create request omits `storage`, provided HA and replication are disabled.
Explicit `{"storage":{"dense_embeddings":"primary_lsm"}}` retains LSM source
ownership. An explicitly supplied empty storage object also selects
`primary_lsm`; omission selects the deployment's creation policy. Unsupported
deployments retain `primary_lsm` on omission and reject explicit `vector_store`.
Catalog persistence records the resolved choice before provisioning. Public and
internal forwarding preserve omission until the authoritative creation boundary.
Existing tables, including older catalog records without a storage field, keep
`primary_lsm`; DB open does not reinterpret them using today's creation default.
Snapshot/backup and split operations on vector-store tables currently reject
with `VectorStoreLifecycleUnsupported`. Tables needing those operations must
explicitly select `primary_lsm` until source-reference closure is supported.

The completed [50K/1M ABBA qualification](../../../.benchmark-results/vector-store-owner-admission-20260914/RESULTS.md)
supports this scoped promotion. Both 1M pairs reduced disk by about 43% and
observed peak RSS by about 28%, with peak QPS up 5.8–7.3% and mixed QPS up
10.2–19.1%. This is an overall tradeoff, not a win on every measurement: one
50K peak-QPS arm regressed 22.5%, 1M churn took longer, and source GC settled
in 149–155 seconds versus 33 seconds for LSM ownership. All eight arms passed
workload, restart and reclamation gates. Keep these costs visible in follow-up
work; broader deployment admission needs its own lifecycle qualification.

Promotion includes the measured read settings, so ordinary launches obtain the
qualified implementation without benchmark environment setup:

| Default behavior | Retained qualification override |
| --- | --- |
| Float32 source and ANN payload encoding for fresh stores | `ANTFLY_HBC_VECTOR_BLOCK_ENCODING=float16` selects the residual-backed experiment |
| Direct ANN member bindings | `ANTFLY_SOURCE_VECTOR_MEMBER_BINDINGS=0` disables |
| Exact mapped reads | `ANTFLY_EXPERIMENT_EXACT_MAPPED=0` disables |
| Reduction-based query packing | `ANTFLY_EXPERIMENT_QUERY_PACKING=lanes` restores the prior path |
| One bounded vector-read helper | `ANTFLY_EXPERIMENT_VECTOR_READ_SINGLE_HELPER=0` disables |
| Batched source reads and positional batches | `ANTFLY_SOURCE_VECTOR_BATCH_READS=0`, `ANTFLY_SOURCE_VECTOR_POSITIONAL_BATCH_READS=0` disable |
| Shared immutable source catalogs | `ANTFLY_SOURCE_VECTOR_SHARED_CATALOG=0` disables |
| Replay-aware matrix loads | `ANTFLY_SOURCE_VECTOR_REPLAY_READS=0` disables |

Existing source stores retain their persisted encoding, including float16;
opening a source store does not migrate its encoding. Derived ANN serving
projections retain the existing preferred-encoding rebuild policy. The
qualification runner also defaults to float32, matching the completed ABBA arms
and ordinary fresh-table launches.

These read settings apply wherever their existing capability checks permit,
including applicable LSM-serving paths; the ownership comparisons used the same
settings in both arms. They do not change a persisted format or metric. Other
experimental GC, admission, cache and read policies remain off. Historical
entries below describe the defaults at the time of each experiment.

Promotion fault injection also exposed a cache rollback gap: incremental
inventory advanced to a candidate before read-view allocation, but an allocation
failure there did not discard it. The next retry subtracted old WAL contributions
from the candidate cache and raised `InvalidVectorInventory`. Both installation
and collection now discard candidate inventory on every subsequent failure,
including read-view preparation. The allocation-failure test explicitly enables
published reads and checks foreground reads, retry and repeated reopen.

[Promotion validation](../../../.benchmark-results/vector-store-default-20260914/README.md)
passed 67 source tests, 43 native tests (two skipped), the standalone catalog
suite, request-forwarding/policy and quantizer checks, and 11 public API tests
with no experiment flags. The API gate covers omitted and explicit ownership,
multiple models, updates/deletes, restart, last-index removal/rebuild, enrichment,
full-text preservation and explicit-LSM backup/restore. Supported restore requests
explicitly retain primary ownership rather than applying fresh-table policy.
OpenAPI/generated-doc checks pass. These are correctness checks for promotion;
the performance evidence remains the completed ABBA comparison above.

The PR-review follow-up aligns fresh-store encoding with that float32
qualification and closes an admission race in the optional query-snapshot path.
A query now rechecks portable runtime activation after acquiring apply-shared
and the catalog lease, so publication cannot leave a queued query searching a
runtime whose activation is pending. The regression closes admission while each
of three public query adapters waits for apply, in both snapshot modes.
[Review-fix validation](../../../.benchmark-results/vector-store-pr-review-fixes-20260914/README.md)
passed that regression, 68 source tests, 43 native tests (two skipped), seven
owner tests, 13 benchmark-tool tests, and 11 API checks with vector/HBC overrides
cleared. A separate float16 override/reopen check also passed. No new throughput
measurement is claimed.

## September 13: compiled-owner maintenance and fresh ownership comparison

The post-merge 1M GC screen stopped before its first arm qualified: automatic
source verification never completed. An idle reproduction recorded zero GC
steps for three minutes. The compiled storage-owner open path omitted the
shared resident-worker startup call after installing the DB at its final address.
It now registers that maintenance after successful owner configuration, using
the same stop/join lifecycle as resident cache entries. A compiled-owner regression
reopens a source-backed table and requires automatic liveness verification without
foreground traffic or an explicit maintenance drain; it fails on the old path
and passes with the registration restored, with no test leaks.

The incomplete screen's roughly 920 peak QPS was measured without functioning
background source verification and must not be used as promotion evidence.
The [fresh ownership comparison](../../../.benchmark-results/vector-store-promotion-20260913/README.md)
requires working automatic GC and matching query results across saved-1M restarts
before timing fresh primary_lsm/vector_store tables in AB/BA order at 50K and 1M.
Both modes use the established common ANN/read settings. New GC-policy, adaptive
read, and native-only snapshot experiments remain disabled pending independent
fresh-workload qualification. No creation default is changed.

## September 13: GC policy implementation and qualification

The [GC policy experiment](../../../.benchmark-results/vector-store-gc-policy-20260913/README.md)
implements detached reader preparation/retirement, explicit receipt scheduling,
cost-based reclamation, and adaptive read dispatch as independent opt-in treatments.
It preserves metadata validation and source/primary recovery fences. Full-copy
input preparation and sorting move outside DB apply with immutable reader
validation; publication binds the latest protected WAL tail. Reclamation exposes
deferred bytes and persists its maximum-age scheduling deadline across restart.

Background receipt reuse is a bounded retention hint, not permission to delete:
ANN-only retirement may wait for the mandatory five-minute full verification.
Startup and explicit collection still establish complete liveness. This avoids
hashing the ANN WAL during background receipt checks. The previous zero dispatch
counters came from leaving the detailed profiling flag disabled; diagnostic arms
now enable it consistently, while clean throughput arms leave it disabled.

Qualification is in progress. Source/native tests pass. The DB lifecycle gate
exposed a test assumption about immediate GC admission while a recovery source
session remained active; it now drives later wakeups and verifies completion.
Pressure cleanup also now respects the detached reader-validation reservation.
No ownership default or experimental-policy default is promoted here.

After merging `origin/main`, the saved-1M gate exposed a schema-open failure
before query timing: the fixture's ASCH V12 runtime schema and the current V13
encoding represented the same logical epoch, but immutable-version validation
compared raw bytes. Format-only retries now compare complete decoded runtime
schemas and preserve the existing active/history bytes; changed runtime or
public validation contracts remain rejected at the same logical version.
This uses the schema decoder's existing supported formats, without adding
vector-store format compatibility. The failed arm is retained as recovery
evidence and is excluded from performance results.

The next saved-fixture check opened successfully and matched the expected first
hit set, but exposed missing dense telemetry in the compiled local-query provider.
That provider now honors `profile: true` through the DB's captured profiled search
and shares the public dense-profile mapping with in-process reads. Query identity
still comes from the result's read lease. Diagnostic qualification requires the
dense profile and adaptive decision counters; absent telemetry is a gate failure.

## September 13 UTC: remaining GC costs investigated

The [follow-up exploration](../../../.benchmark-results/vector-store-gc-followup-exploration-20260913/README.md)
uses the same R32 binary for a receipt A/B/B/A and a separate sampled 1M churn run.
No runtime behavior or ownership default changed during this exploration.

The publication stall is now attributed: both samples put the dominant work in
`PreparedPublication.openReaders`, especially index/key CRC validation. The loader
also validates newly acquired blocks twice. Return a retained validated block/reader
pair and prepare immutable reader catalogs outside both source and DB apply locks.
Bind the latest protected WAL suffix and commit under a short final fence; retire
old owners/files afterward. Merely unlocking SourceLock while blocking a writer
that holds DB apply does not resolve foreground stalls. Preserve descriptor,
metadata and payload integrity checks and ambiguous-publication recovery.

Existing receipt reuse halves observed marking from 30.02 million rows / about
10 seconds to 15.01 million rows / about 5 seconds, but does not consistently
improve settled QPS or physical footprint. Receipt enablement is coupled to the
explicit GC-step environment setting, and receipt hits do not advance the
verification deadline. Introduce explicit open-time policy and proof-check
scheduling; use validated primary/ANN/source authority identities, including
orphan preparations and model/index lifecycle, rather than repeatedly hashing
WAL files under the lock. Do not treat a reduced scan count as a measured memory win.

Separate periodic verification from expensive copying: small churn still causes
two full copies (6.144 GB read, 6.384 GB written). Choose reclamation by benefit,
copy cost, disk pressure and maximum garbage age, then qualify independent segment
retirement. The existing selective experiment cannot replace part of a physical
base. Keep deferred bytes visible and require eventual reclamation. Wider reads
remain a cold/warm tradeoff; wire source-mediated helper counters before designing
adaptive dispatch based on observed cost and available concurrency.

The sampled churn run reclaimed to one million live payloads with zero pending/
unreferenced bytes and passed restart hit-set checks. Its maximum GC publication
was 922 ms. Sampling also coincided with a 4.23-second foreground outlier; use
these profiles for attribution, not as a new unsampled latency benchmark.

## September 13 UTC: background table-owned GC qualification

The [background-GC candidate](../../../.benchmark-results/vector-store-background-gc-20260913/README.md)
removes ordinary source collection from synchronous writable open. Durable
source/primary recovery and source generation installation still finish before
serving. Uncommitted preparations and obsolete versions remain safe to retain
until the stable DB maintenance owner completes a pinned liveness scan.

Table-owned stores now use snapshot mark turns capped at 16,384 rows and 2 ms,
with 25% scan duty, outside source/apply locks. These are cooperative bounds.
Collection copy turns are capped at 8 MiB, including explicit sync collection.
Debt schedules subsequent verification with a 30-second periodic fallback;
it never certifies liveness. Setup, planning and publication retain their
existing fences and can exceed a scan turn's time budget. Standalone Store
collectors retain their existing policy, and the table creation ownership
default remains primary_lsm.

All 63 source tests and 12 focused DB/repair tests pass without leaks. The added
DB regression covers zero ANN indexes, uncommitted preparations, updates/deletes,
an old reader, cancellation during partial marking, and two reopens. Four fresh
50K lifecycle arms pass with no consistent readiness/peak-throughput direction.
Saved-index 1M activation improves from 12.7–12.9 seconds to 3.6–4.7 seconds with
matching hit sets. Maximum all-live GC locked intervals fall from about four
seconds to 43 ms. Cold latency is essentially unchanged by scheduling alone.

The repeated read-policy screen finds a useful separate tradeoff: wider read
dispatch raises cold QPS from 56 to 80 and lowers p99 from 49–51 ms to about
27 ms. Mean cold vector loading falls from 9.6 to 4.0 ms. Changing mapped reads
to positional reads adds little; wider dispatch does not improve already warm
loading. A two-second training-query warmup lowers held-out cold p99 to 18–19 ms,
but activation plus the first 1,000 queries takes longer. This is an experimental
request/time bound, not an automatic product policy or a memory cap.

Background GC is not yet a uniform performance win. Initial settled 1M throughput
is roughly 2–5% lower, with a retained warm-pass latency outlier whose cause is
unresolved. Repeated verification can retain an approximately 80 MB mark workspace.
The large churn probe reclaims to one million payloads with no pending/orphan
bytes and passes restart, but full copies read 6.1 GB, publication holds the
source lock for about 0.9 seconds, and foreground maximum latency reaches 0.99 s.
Next targets are reuse of authenticated unchanged-authority collection receipts
and staging publication before its short commit fence. The ownership default
remains unchanged. Final c1/10/20/30 A/B/B/A checks pass, but wider dispatch is
0.3–3.9% lower in settled QPS. Prefer investigating a cold-work policy with narrow
warm dispatch to assuming one fixed ceiling is best. All qualification stages
and frozen-source/binary checks are complete; detailed results are in the bundle.

## September 13 UTC: GC liveness and scan cache admission

The [GC qualification](../../../.benchmark-results/vector-store-gc-liveness-20260913/README.md)
implements two changes identified by the R30 memory attribution. GC opens its
primary read snapshot with transient block-cache admission, preserving its
version fence while avoiding one-pass scan pollution. The all-live fast path
resolves source metadata in its pinned generation instead of reading and
checksumming every payload. It still rejects missing/tombstoned identities,
wrong dimensions or revisions, reference chains, and float16 payloads lacking
an exact residual. Immutable reader open validates index/key metadata and bounds;
exact consumption and GC copying retain payload and residual checksums. An
all-live GC pass is a liveness check, not a full data-integrity scrub.

All 63 source ownership/recovery tests and 42 native store tests pass with no
leaks; two opt-in microbenchmarks are skipped. New regressions cover cache
admission with a concurrent delete, retained old source generations, and corrupt
float32/residual payloads that GC retains but reads and copies reject. A fixture
abort-after-commit error was corrected; its failed run is preserved in the bundle.
No configuration or scheduling default changes accompany these two fixes.
The saved dirty 1M A/B/B/A passed all eight restarts, sixteen query windows and
1,000-query hit-set checks. Resident activation falls from 33.1–36.7 seconds to
12.0–12.8 seconds (64–65% faster). Primary block-cache residency falls from
939.5 MB to 143.3 MB; 90-second idle physical footprint falls from 1.10–1.22 GB
to 271–278 MB. The all-live source still retains exactly one million payloads,
with no unreferenced/pending bytes. Second restarts remain about nine seconds.
Settled throughput varies with order; c1 p99 is 5–8% higher in these observations,
so there is no consistent query-speed or tail-latency win.

A separate clean-index cold/warm A/B/B/A exposes the prewarming tradeoff. Cold
serial p99 rises from 18–22 ms to 49 ms, and cold QPS falls from 97–111 to 56.
Warm results are much closer and vary with order. Activation plus the first
1,000 queries still finishes 29–34% sooner, but individual cold queries are
slower. The full results retain all per-query latencies; these are process-cold
starts with uncontrolled OS cache state on the available host.

Keep liveness GC separate from payload warming. Next, move ordinary collection
out of synchronous writable open into bounded snapshot maintenance: the candidate
still performs two roughly four-second full mark passes while opening/activating.
Durable source/primary WAL recovery and memory admission must remain active.
Separately compare cold positional batch reads and read concurrency against this
bundle's mapped exact reads/single-helper policy, and measure explicit bounded
ANN warmup if needed. Those are follow-up targets, not implemented wins. Keep the
creation default unchanged until the cold-latency/readiness tradeoff is qualified
with intended shipping settings and the remaining mixed-workload comparisons.

## September 13 UTC: detached repair input implementation

[Implementation and qualification receipts](../../../.benchmark-results/vector-store-detached-repair-20260913/README.md)
now cover a native repair input handle that retains immutable metadata/source
generations and owns leaf, ancestor, artifact-family and transform state.
Metadata resolution, exact batched reads and transforms run outside the shared
apply/catalog/index locks. Sorted directory misses preserve overlay/delta and
tombstone precedence; generation-scoped member bindings avoid repeated resolution.
Captured native inputs never consult current primary data on a miss. Publication
still rejects changed epochs and index incarnations through the maintenance WAL.
Deferred capture is distinct from a required synchronous path, so it does not
request an exclusive fence merely to discover foreground pressure.

Fifteen focused repair/recovery tests (zero leaks), ten directory tests and
thirteen qualification-validator tests pass. All four saved-index repair arms
and eight fresh same-binary ownership arms passed, including updates/deletes,
enrichment, restart recall and reclamation. Capture under index ownership falls
from 5.3–5.5 ms to 3.6–3.8 microseconds. Total detached input still takes about
5.4 ms, mostly metadata plus source-location resolution; repair-traffic p99 does
not show a consistent improvement. Publication retains epoch/incarnation checks.

At 50K, vector-store readiness is 21–28% faster, peak QPS 8–16% higher and disk
about 49% lower. At 1M, readiness is 14–20% faster and disk about 43% lower;
peak QPS is +7.7% / -2.4%, while saturated mixed query throughput is 1.5–4.2%
lower. Sampled peak physical footprint improves 40–45% at 1M, but idle retained
footprint is 824–1,207 MiB versus 254–258 MiB. Process-cold resident activation
still takes 33–37 seconds versus about 3 seconds. Serial cold query tails alone
do not expose this activation cost. Two pairs on a shared host are observed
ranges, not confidence intervals; the complete concurrency and enrichment data
are retained in the linked receipts.

The next concrete targets are GC scan behavior. Its primary read snapshot admits
one-pass scan blocks into the regular LSM cache: an isolated 90-second idle check
retains 940 MB versus 149 MB in that cache. Its final liveness verification still
calls `source.get` for every live digest, reading/checksumming payloads after the
metadata-only inventory scan. Nearly the entire source mapping becomes resident.
An 8 MiB GC output-step A/B/B/A leaves startup unchanged because it does not bound
the roughly 15 million mark visits. These are identified costs, not fixes shipped
in this candidate.

Proceed with snapshot-preserving transient cache admission, metadata-only
liveness validation with checksums retained on consumption/copy, and bounded
background marking. Recheck missing/tombstoned references, snapshot/concurrent
write protection and cold activation/memory, then measure paced 1M mixed traffic.
Repair's remaining first-touch metadata/location work needs finer attribution
before a format redesign. Keep the ownership default unchanged until these
regressions are addressed and intended shipping settings are qualified. This
comparison uses an explicit float32/native-read/member-binding/packing bundle on
fresh standalone single-shard tables; it does not qualify every default setting
or deployment mode. Old catalogs must retain absent-field semantics.

## September 13 UTC: repair cost and default-promotion review

The [follow-up review](../../../.benchmark-results/vector-store-repair-cost-review-20260913/README.md)
uses an instrumentation-only R28 snapshot and separate uninstrumented paced
comparisons; qualified runtime code and defaults are unchanged.

Across 9,815 vector rows during 1M repair-under-traffic, metadata lookup consumes
350.7 ms (60% of matrix loading), native lookup/read 221.6 ms (38%), and transforms
3.1 ms (under 1%). There are no primary-payload fallback rows. The activated-idle
follow-up shows the same dominant stages, with 171.1 ms metadata and 205.7 ms
native reads across 9,571 rows. These timers include scheduling/page faults and
checksumming; they are not measurements of device I/O alone.

The metadata batch API loops over version-aware native point reads, and the
matrix reader resolves artifact keys and source digests one row at a time. The
next shape is a repair input handle retaining immutable ANN metadata and source
generations, plus owned leaf/ancestor and transform state. Resolve/read/transform
outside the shared apply lease. Reuse generation-scoped member bindings first,
then a genuinely batched immutable metadata-miss path and bounded exact reads.
The cache starts empty on generation replacement, so improving only cache hits
would not solve first-touch costs. Preserve source/model/index version identity,
tombstone precedence and publication validation; never switch to current primary
data after releasing the captured fence. Count deferred and discarded attempts,
and avoid exclusive fallback admission when capture was merely deferred.

At matched 1,000 write rows/s on identical saved 50K files, R28 query QPS is
1.16% / 1.42% lower than R27, with query p99 approximately unchanged. All four
arms complete the same 20,100 rows without errors and retain approximately
0.98485 recall. This narrows the saturated mixed-workload concern at one offered
rate; it does not establish the result near saturation or compare ownership.

A status-only idle diagnostic initially retains dirty payloads after 45 seconds,
with correct hit sets. Activating the resident serving runtime first takes
6.127 seconds for the initial query; all 114 repairs then publish within about
two seconds idle and the full 1,000-query validation is clean. The initial
incomplete-clean result remains preserved. Default qualification should separate
status readiness, resident activation and first-query latency.

The remaining promotion work is a fresh same-binary `primary_lsm`/`vector_store`
comparison using the intended shipping settings at 50K and 1M, including matched
write rates, cold-query tails and phase-aligned memory/reclamation accounting.
The old ownership comparison's 50K peak-QPS and 1M cold-tail penalties need
remeasurement; recent maintenance-only comparisons cannot establish that they
persist or are fixed. The R28 RSS outlier appears during churn/enrichment, with
only one physical-footprint ledger sample. It does not establish a heap leak or
a memory win. Judge sustained non-reclaimable/retained memory and availability
alongside RSS. A scoped creation default can be justified by balanced metrics;
it does not require every noisy timing cell to improve. Existing catalogs and
the local-single-shard deployment restriction must retain their meaning.

## September 12: prepared posting replacement qualification

Implementation and receipts are under
[`vector-store-prepared-refresh-20260912`](../../../.benchmark-results/vector-store-prepared-refresh-20260912/README.md).
One dirty leaf's transformed vectors and required ancestor metadata are copied
under the index mutation owner and a short shared DB lease. The shared lease
keeps primary/source fallback reads coherent and admits queries. An owned
preparation object then recomputes centroids/radii, quantizes and encodes the
replacement values without DB, catalog or index mutation locks.

The exclusive publication step validates the index incarnation and mutation
epoch before installing prepared values through the existing maintenance WAL
capture and publication protocol. It preserves source coverage and requires a
subsequent clean verification sweep. Updates, deletes, aborts, another mutation
and same-name index replacement invalidate a preparation. Scratch reservations
cover its lifetime and are released on rejection or completion. Existing fair
admission and bounded idle bursts remain; query-pressure pages yield after one
publication. The shared input copy and durable publication still have lock costs.
Experimental row-delta payloads and flagged structural repair retain their prior
path. This changes neither persistent formats nor payload ownership defaults.

All fourteen focused/recovery checks pass, including byte-identical results
versus synchronous repair for L2, cosine and inner product; update/delete/abort
and equal-epoch incarnation rejection; scratch denial/cleanup; empty payload
removal; old readers and WAL/restart recovery. The pinned service binary uses
the v2 source snapshot; a later test-only fixture adjustment is retained as a
separate patch. R27 is the control. All four saved-index 1M arms, four fresh 50K
service arms, four normal enrichment arms and four reclamation gates pass.

On the same saved dirty 1M index, repair-phase query p99 improves from
29.83 to 24.06 ms and from 27.38 to 22.18 ms (about 19% in both run orders).
Query counts are nearly unchanged, and the sampled debt clears slightly later.
All sixteen settled concurrency windows preserve 1,000 query hit sets, with zero
sampled stale payloads in final profiles. Settled QPS has no regression in either
pair, but the second pair is nearly neutral. Initial cold responses still reach
about six seconds in every arm.

Successful preparations spend 5.6–5.9 ms on average copying inputs under shared
admission, about 0.06–0.07 ms building without locks, and 0.53–0.57 ms publishing.
Publication p99 is 4.6–4.8 ms and its maximum is 14.15 ms. Capture reaches
49.64 ms. These counters exclude discarded preparations and cover payload-only
leaf repairs. Input copying remains the dominant repair stage; a queued writer
can still wait behind its shared lease.

Fresh 50K peak QPS changes by -4.4% / +4.5%, and readiness is nearly neutral
within each pair. Mixed query throughput is lower by 14.8% / 3.0% while write
throughput rises 1.3% / 3.8%; query p99 remains about 208 ms. Fixed-count churn
and peak RSS change direction, with a large RSS peak in the second candidate.
These measurements support the repair-tail improvement, not a general throughput
or memory improvement. Restart preserves per-arm recall and all lifecycle gates
pass. Residual mixed-workload admission, cold responses and physical-memory
variation remain investigation targets. A future input-copy optimization needs
revision-pinned source/fallback snapshots; releasing the current locks without
that ownership would be unsafe. Populated same-name index recreation remains
outside the final focused incarnation test's qualification.

## September 12: resumable posting refresh qualification

The follow-up is recorded in
[`vector-store-posting-refresh-20260912`](../../../.benchmark-results/vector-store-posting-refresh-20260912/README.md).
A saved 1M post-churn index reproduced dirty quantized payloads after readiness;
existing idle maintenance cleared the sampled debt between 32.6 and 37.7 seconds.
Deferred rounds were treated as idle and backed off for 30 seconds. A continuous
query stream can also prevent the old 25 ms quiet interval from occurring.

The candidate retains a metadata scan cursor, bounds idle pages to 128 node IDs
and eight repairs, and uses read transactions for clean prefixes. A completed
clean mutation epoch makes repeated polling constant-cost. Changes behind the
cursor and repaired pages require a complete clean verification sweep. Deferred
or partial rounds remain pending and retry promptly. The first candidate's
nonblocking apply-lock probe still starved during 45 seconds at concurrency 10.
The revised candidate scans under a catalog lifetime lease, then requests fair
writer admission only when it finds actual debt. Admission cancels after 50 ms;
a mutating page rechecks current state and repairs at most one posting per index
under query pressure before yielding. The admission limit does not bound payload
rebuild time. Existing serving and publication fences remain in force.
Flagged structural repair retains its separate existing idle policy.

Twelve focused checks and four additional recovery checks pass, covering cursor
resumption, mutation rechecking, foreground deferral, native WAL source
coverage/reopen, and exact fallback. The work also fixes false payload debt when
quantization is disabled; that failure reproduced on the pinned control first.
All four fresh 50K service arms, four normal enrichment arms and four reclamation
gates pass, including updates/deletes and cold/warm restart recall.

On the saved dirty 1M index, candidates clear the sampled stale posting after
13.6/14.0 seconds during sustained concurrency-10 traffic. Controls remain dirty
through the entire 45-second stream and clear at about 63.5 seconds after idle
gaps begin. The candidate's repair-phase p99 rises by 7–9 ms. Both comparisons
use vector-store ownership and the same R26 streaming implementation; this is a
maintenance comparison, not an ownership comparison.

Fresh 50K peak QPS changes by +1.5% / −1.5%, with readiness around 12.7–13.0 s.
The saved dirty-input comparison has lower settled concurrency-20 QPS in both
candidate arms. A second 1M comparison starting every arm from identical repaired
files changes that to +1.8% / −8.0%; concurrency-30 QPS remains lower by
17.5% / 1.7%. All 32 windows across the two saved-index comparisons preserve
1,000 query hit sets, with zero stale payloads in final sampled profiles. These
results qualify maintenance progress and correctness, not a universal throughput
win. The common-file run logs no repair/checkpoint work during measurement.

A small-table restart timing difference reverses on identical files; HTTP traces
show roughly one-second visibility-status steps, consistent with the existing
status refresh cadence, while logged writer opens remain tens of milliseconds.
This does not demonstrate slower vector loading. The next repair-tail target is
preparing a version-checked immutable replacement outside the DB apply fence,
then validating and publishing briefly under it. This is not implemented here.
Residual store-phase write latency and physical-memory demand still need separate
profiling. Ownership defaults and deployment qualification are unchanged.

## September 12: full-service streaming qualification

The completed comparison is recorded under
[`vector-store-streaming-service-20260912`](../../../.benchmark-results/vector-store-streaming-service-20260912/README.md).
Two pinned binaries differ only in the vector-block implementation files. All
four normal enrichment arms, eight alternating fresh 50K/1M service arms and
reclamation hooks passed. The candidate build passes 102 store/recovery tests.
The table ownership default is unchanged.

At 50K, readiness and QPS were broadly neutral. At 1M, readiness improved from
270.1/309.2 s in controls to 246.5/263.8 s in candidates (8.7%/14.7%). Maximum
delta staging fell from 4.15/4.62 s to 0.56/0.54 s. Largest ordinary write batches
fell from 4.98/9.00 s to 0.91/2.33 s; remaining store-phase work is not covered by
the vector merge budget. Live peak RSS was 11–15% lower in both 1M pairs, while
ledger-backed memory-demand estimates were mixed. Final disk usage was similar.

Fresh peak QPS changed direction across pairs, and fresh concurrency-10 QPS was
about 5% lower in both candidate arms. A follow-up held the saved data and ANN
index identical across binaries: all sixteen warmed concurrency windows passed
with identical hit sets for 1,000 queries. Candidates had no QPS regression in
that comparison. A runtime QPS penalty was not reproduced on the common clean
index; fresh-index/lifecycle/host variability remains relevant, and these results
do not establish a universal QPS speedup.

The follow-up also identified dirty quantized postings after churn in both
controls and one candidate. Queries safely fall back to exact vectors, but the
extra work remains a maintenance target. The first saved input failed a strict
zero-stale-payload probe before timing; the qualified follow-up uses a clean
snapshot with that check retained. Refreshing dirty postings without withdrawing
healthy query serving, and profiling residual store-phase latency and memory
demand, are the next concrete investigations. All receipts and failed attempts
remain in the worktree; these results do not qualify distributed deployment or
change the payload-ownership default.

## September 12: streaming merges and bounded checkpoint work

Implemented receipts, source snapshots and runners are in
[`vector-store-streaming-merge-20260911`](../../../.benchmark-results/vector-store-streaming-merge-20260911/README.md).
Delta checkpoints now use the same streaming merge machinery as base compaction:
one metadata cursor per selected run, newest-version selection, then validation
and copying of the winning payload. Compatible encoding preserves the existing
bytes, quantization bounds and exact residuals without requantization. Partial
merges retain tombstones. Existing publication fences and old-reader leases apply.

Streaming directly over interleaved mmap payloads regressed cold performance.
Bounded positional read windows recovered locality and reduced resident payload
pages. Read-ahead uses a 4 MiB target across inputs plus one shared oversized
vector/residual pair; cached offsets are bound to the retained inode identity.
Output continues through the existing streaming block writer.

Ordinary scheduled merges exceeding 256 MiB of delta input plus incoming WAL
now select a bounded newest suffix. Large delta backlogs also activate this
selection before the generation limit. The policy applies during ingestion and
after updates to an established base, respects explicit append-only mode, and
uses the existing memory and durability settings. This automatic policy does
not change the table's payload-ownership default or enable background/cache flags.
The hard manifest limit retains a defensive full-merge fallback.

At 1M × 768 dimensions, paired buffered-streaming runs reduced source ingest
plus final checkpoint from 51.8 s to 34.5–36.8 s, and peak process RSS from
3,633 MiB to 685 MiB with identical logical write volume. Isolating bounded
selection then reduced the largest write batch from 5.77–6.26 s to 0.86–1.00 s
and WAL-plus-checkpoint output from 12.322 to 10.752 GiB. The tradeoff was
roughly 160 MiB more peak RSS and more final-consolidation work: total time was
37.9–40.3 s versus 36.8–39.0 s with buffered streaming alone. These are separate
sequential source-store comparisons, not end-to-end ANN readiness or QPS results.

A final control/candidate and candidate/control update comparison over the same
retained 1M base reduced the full update pass plus checkpoint from 50.0/59.8 s
to 26.2/27.1 s, and the largest write batch from 5.64/10.03 s to 0.90/0.92 s.
Logical writes fell from 16.579 to 7.922 GiB. The control already includes
buffered streaming; this isolates the bounded policy and the corrected online
merge boundary. More segment files remain (2,816 versus 1,024) for later
maintenance. All updated and original references are checked after reopen.

Full stable-tip consolidation still takes about 8–9 seconds. The ordinary merge
input budget is not a universal latency guarantee. The 50K × 3072 check also
showed extra early-merge writes and a small ingestion regression; receipt tables
retain those results. Full service churn/query qualification remains necessary
before claiming improvement across all workload metrics.

The final implementation passes 60 source-store and 42 native-store tests (two
optional benchmarks skipped), covering recovery, concurrent WAL tails, old
leases, updates/deletes, winner corruption, exact encoded residuals and merge
budget boundaries. The real-file update harness retains and verifies both old
and updated artifact references after reopen; without primary-owner GC it is
a source retention test, not a reclamation benchmark.

## September 11: source payload costs and scratch admission

The follow-up implementation and receipts live under
[`vector-store-source-costs-20260911`](../../../.benchmark-results/vector-store-source-costs-20260911/README.md).
Ordinary search scratch now admits growth before allocation. Bounded reranking
checks the captured source capability before growing its decode matrix; a
supported source generation is pinned for the later score pass. Posting-local
bounds retain their complete-shell refinement. This addresses the ungated
memory issue described below without reducing the candidate set.

Source inventory, directory hints, ANN marking and GC planning read admitted
metadata through `sourceIdentityAt`. Payload reads and copying still validate
checksums, and artifact reconstruction still validates the complete key/header/
vector digest. Runtime SHA-256 dispatch enables optional instructions in baseline
binaries, while already accelerated compilation targets keep std's kernel.

The new real-file `vector-payload-bench` and independent checkpoint/cache
switches are documented in that receipt directory. Those switches remain off.
Qualification includes forward/reverse source-store arms at 50K and 1M; restart
must be assessed together with first-read latency because metadata-only reopen
no longer preloads all payload pages. Tiering and background scheduling are
experiments, not assumed improvements. Final service and workload results are
recorded with their exact executable and settings.

The final service build passed 32 ungated query windows at 50K/1M, preserving
all 1,000 reference hit sets in each window. At 50K, observed retained search
memory fell from 181 MB to 82 MB and allocation rejections fell from two to
zero. Source-only 1M reopen fell from 6.87 s to 0.10 s; the first full scan now
pays the deferred cold I/O. Runtime SHA measured about 6x over the forced
portable kernel on ARM, with SHA-NI correctness checked under emulation; this
is not an end-to-end speedup on already accelerated native builds.

Retaining checkpoint output in cache passed fresh 50K ABBA and normal
enrichment/update/restart checks. Ready times were 15.94/20.07 s in control
versus 13.60/12.32 s with retention, while mixed query throughput varied.
Tiered merging increased writes at 1M and remains experimental. Warm 1M query
profiles attribute approximately 4.6 ms to leaf scoring and 0.4 ms to rerank
loading; at concurrency 30, scan admission adds about 30 ms in both builds.
The memory/recovery fixes stand independently of cache or checkpoint policy.
Further performance work should measure scoring and admission together, and
qualify cache/append-only combinations through full 1M churn before promotion.

The delta-streaming follow-up is implemented in the September 12 entry above.
Its buffered execution and bounded merge policy were measured separately from
these earlier cache/background experiments.

## September 11: snapshot synchronization, query packing and build-quality experiments

The next qualification is isolated under
[`vector-store-snapshot-packing-20260911`](../../../.benchmark-results/vector-store-snapshot-packing-20260911/).
The new settings remain experimental and off by default:

- `ANTFLY_EXPERIMENT_QUERY_SNAPSHOT=1` captures a catalog lease, identity
  visibility generation, primary metadata read transaction, ANN generation and
  exact float32 source generation at the same source sequence. The apply read
  fence is released after capture; catalog and generation leases survive until
  result materialization finishes. Admission currently requires an all-visible,
  non-TTL document index with no chunk or multi-source result shaping, an
  authenticated native tree or flat directory, and a plain score-only request. Unsupported or lagging cases
  retain the existing synchronization. An incomplete snapshot releases the
  bundle and retries through the existing DB repair path.
- `ANTFLY_EXPERIMENT_QUERY_SNAPSHOT_NATIVE_ONLY=1`, together with the snapshot
  flag, omits the full primary metadata snapshot. A short primary probe verifies
  the committed sequence while the apply fence is held. ANN metadata and exact
  vectors come from the retained native generations. Any need for primary data
  aborts that attempt and retries the whole query through the ordinary DB path;
  it must never open a current-tip primary transaction after releasing the
  fence. The public single-dense query envelope uses the same eligibility gate.
- `ANTFLY_EXPERIMENT_QUERY_PACKING=reduce` replaces scalar lane extraction with
  vector shifts and a reduction into the four query bit planes. `mask` provides
  an independent bit-cast packing variant. Float quantization, bit order,
  scalar tails, exact reranking and the persistent format are unchanged.
- `ANTFLY_EXPERIMENT_CONFIGURED_BULK_BUILD=1` lets empty-index bulk ingestion
  honor the persisted `bulk_build_algo` choice. It retains the existing batch,
  replay and publication boundary. This isolates bootstrap quality; it does not
  implement a whole-table matrix build or imply that changing the first batch
  alone will improve a mature incremental index.

Identity visibility summaries now load once during DB open, before query
admission. Ordinary mutations continue to publish them through the existing
apply fence. Apply-lock reader admission uses an atomic reader count and a
writer-closed bit. Synchronous waiters use an epoch and native wakeups;
cooperative/cancellable I/O callers retain their owner's bounded wait protocol.
Profiled DB entrypoints now account for foreground query activity like normal
queries, so profiling does not silently alter that admission policy.

Correctness checks cover old ANN membership/metadata after publication, a
query bundle retaining primary metadata and exact source after replacement,
identity visibility across restart/deletion, and cancellation/fairness in the
apply lock. The native-only tests also force a missing source value with exact
reranking and verify a whole-query primary fallback. Packing parity includes
every eight-lane bit mask and dimensional tails. A table with retained identity
tombstones continues through the ordinary path; this snapshot experiment does
not yet provide an immutable visibility mask for general delete-heavy queries.
Qualification results are recorded in the experiment directory; a kernel speedup
must not be reported as a whole-query speedup.

### Qualification result

The frozen v4 runtime passed seven independent or combined retained-index
comparisons: 174 service windows at 50K/1M, with all 1,000 reference query hit
sets preserved in each window. Clean AB/BA and separate instrumented windows
ran on the available host. Both arms used the existing aggregate-admission gate,
float32 source ownership, exact mapped views, member bindings and single-helper
policy. Two order reversals do not establish confidence bounds.

The final 1M combined comparison is against the earlier frozen R22 binary:

| Concurrency | QPS before → after | QPS change | p99 change | Server CPU/query before → after |
|---|---:|---:|---:|---:|
| 1 | 106 → 114 | +7.9% | +1.6% | 18.44 → 16.77 ms |
| 10 | 630 → 790 | +25.5% | -43.2% | 12.19 → 9.56 ms |
| 30 | 361 → 803 | +122.7% | -79.9% | 30.40 → 9.38 ms |

These are medians of paired ratios; absolute QPS/CPU columns are arm medians.
At C30, median p99 falls from about 213 ms to 43 ms. This is a substantial
concurrency improvement, not a tenfold whole-query improvement.

Independent controls locate the gains:

- Synchronization/identity changes alone improve C30 QPS by 102% at 50K and
  130% at 1M. Lower concurrency is mixed: 1M/C10 is -4.6% in that pair despite
  lower CPU/query. Do not infer a universal throughput gain from C30.
- Reduction packing improves QPS by 7–11% at 50K and 13–18% at 1M across
  C1/2/4/10/20/30. The 50K/C30 p99 result is mixed. The kernel probe is 1.56–1.81x
  faster for preparation plus a one-row score; that is not a service speedup.
- Native-only snapshots versus ordinary synchronization at 1M add 4–6% QPS
  and reduce p99 by 31–38% at C10–C30. C1 is -2.4%. At 50K, native-only versus
  eager-primary snapshots is essentially flat in QPS at C10/C30. The earlier
  eager-primary 50K snapshot experiment regressed C10 QPS by 8.7%.
- Bootstrap and full-topology quality probes did not establish a large
  reduction in scored vectors at comparable recall. At 256 probes, recursive
  bootstrap reaches 95.15% recall with 22,953 rows/query; full global k-means
  reaches 95.655% with 24,622 rows/query and adds about 71 seconds of rebuild
  work. Hierarchical recall gains consume more scored rows. Complete curves
  and measured recall-threshold crossings are retained; no automatic global
  rebuild or 1M whole-matrix builder is promoted.

The ungated series exposed a search-memory admission failure in both an old
control and a new candidate. An exact-rerank allocation of about 49 KB failed
with 194 MB charged against a 179 MB search slice, while aggregate memory was
well below its limit. The gated comparisons had no query errors; the observed
post-window search usage stayed around 103 MiB or less at 50K and 48 MiB or less
at 1M. These observations are not peak process-memory qualification.

A concrete remaining issue is eager decode-matrix growth for the full bounded
candidate shell before the float32 source declines the float16-only capability.
The subsequent exact pass uses bounded batches but retains the larger matrix.
A source-sequence-bound capability check before scratch growth, plus admission
for ordinary scratch growth, is the next memory fix. It is not part of this
measured binary. The aggregate gate is a matched qualification condition, not
proof that the ungated issue is fixed.

All new experimental settings remain off. This work qualifies warm retained-index
queries and bounded build-quality probes; it does not qualify fresh ingestion,
cold paging, mixed churn, distributed lifecycle paths, or a default promotion.
The implementation, frozen binaries, test receipts, failure stacks, raw windows
and full tables are in the linked experiment directory.

## September 11: unified fetching and exact views (not promoted)

`ANTFLY_EXPERIMENT_UNIFIED_VECTOR_FETCH=1` adds a synchronous, transaction-owned
metadata resolver to the HBC exact rerank callback. Within a bounded wave of at
most 256 members, it resolves member-binding hits and only the missing artifact
metadata before issuing one native payload batch. Original output positions
travel with the requests, so physical sorting preserves result order without
searching the candidate IDs again. This capability requires a sequence-matched
float32 generation with member bindings; other cases retain the existing path.
Missing/invalid native values retain authoritative fallback. The read arena is
released before that fallback acquires its own scratch. Float16 projection and
residual reuse are unchanged.

Independent `ANTFLY_EXPERIMENT_EXACT_MAPPED=1` uses existing generation-leased
exact views for float32 block requests, including CRC validation. It still uses
the selected helper policy and retains existing scratch allocation, isolating
payload access from a scratch-pool redesign. This is a mapped-view experiment,
not a resident-page guarantee or a new user-space cache. New profile counters
separate mapped requests/bytes from positional reads; a mapped request can cause
page faults and must not be interpreted as zero physical device I/O. Helper
submission-to-start delay is now measured separately from worker drain time.

The new lifecycle checks exercise mixed binding hits/misses in one batch,
reordered output, updates/deletes, older reader generations and restart. Exact
view tests reject corrupt bytes and clear stale results when requests are reused.
Both settings remain off by default; durable format and ownership are unchanged.

The frozen source, build, replay matrix and independent retained-index service
comparisons are under
[`vector-store-unified-fetch-20260911`](../../../.benchmark-results/vector-store-unified-fetch-20260911/).
Native tests passed (38, with two gated benchmarks skipped), as did four focused
index/lifecycle tests and the complete real-file replay matrix. Independent
service comparisons preserve all 1,000 settled query hit sets. Both settings
remain off by default. Mapped access is the clearer warm-read improvement;
unified batching consolidates mixed batches but is not a consistent throughput
win. These are retained-index comparisons, not fresh ingestion qualification.

Clean mapped/copy service changes below are medians of two paired ratios, AB
and BA. C20/C30 are a separate extension using the same common settings. All
outliers are retained; these small samples do not establish confidence bounds.

| Scale | Metric | C1 | C2 | C4 | C10 | C20 | C30 |
|---|---|---:|---:|---:|---:|---:|---:|
| 50K | QPS | +4.2% | +6.5% | +8.1% | +19.2% | +21.2% | +34.1% |
| 50K | p99 | +5.8% | +0.7% | -6.5% | -14.7% | -21.4% | -23.1% |
| 1M | QPS | +24.3% | +4.0% | +5.8% | +8.8% | +8.2% | +8.6% |
| 1M | p99 | -34.3% | -0.8% | -9.9% | -11.1% | -1.1% | -8.2% |

C1's 1M control varies substantially, making the exact +24.3% magnitude weak.
The 1M inline warm-fetch microbenchmark improves 48.19 → 3.39 microseconds per
completed batch at C10 (14.2x), but excludes identity resolution, ANN scoring,
HTTP and cold storage. Mapped process RSS grows while measured footprint remains
similar; this is not memory-pressure qualification. Source ownership and durable
format are unchanged. See the experiment's [report](../../../.benchmark-results/vector-store-unified-fetch-20260911/README.md)
for raw-pair analysis, CPU/query changes, gates and limitations.

## September 11: remaining bottlenecks and larger design opportunities

The current 1M mapped C10 profile spends 5.823 ms scoring leaves, 1.247 ms routing,
3.816 ms waiting for scan admission and 0.518 ms loading exact vectors within
12.647 ms dense search. It scores approximately 243,535 vectors across 2,048
leaves, reading 23.379 MB of codes to return 100 results. Eliminating the remaining
exact-load interval alone yields only a 1.043x fixed-stage speedup; it cannot
produce a 10x complete-query improvement. Queueing can respond nonlinearly.

The concrete remaining targets are:

1. **Complete query snapshots and the apply fence.**
   `DB.beginDenseSearchAccess` still selects the apply shared lock whenever an
   external loader exists. The lock spans search and lies outside the dense
   profile timer. `ApplyRwLock.lockShared` spins and then polls with zero-duration
   sleeps; sampled query stacks repeatedly wait here. Pin catalog/index lifetime,
   identity/visibility, authoritative metadata, source payload and ANN generations
   as one consistent query bundle before allowing eligible external-payload reads
   to avoid the fence. Keep fallback synchronization for unsupported paths.
   Generation-pinned vector files alone do not prove this safe. Event-driven
   waits remain useful for the paths that need the fence.
2. **Recall per scored vector and initial build quality.**
   A 10x reduction in candidate work requires about 24K candidates at the same
   recall; earlier 20K-candidate runs reached only about 83%, versus about 99.35%
   with 241K. Better partition training or representative routing must improve
   that curve. The normal insertion path bulk-builds only an empty index;
   bulk-ingest explicitly selects recursive construction, and later batches
   incrementally insert/split. Test a bounded snapshot-fed initial build using
   existing builders, including catch-up and atomic publication. Do not turn this
   into an unbounded larger batch or assume every bootstrap path has this limit.
3. **Query preparation and block scoring.**
   `quantizeQueryPlanes` vectorizes float arithmetic but packs extracted lanes
   with four serial shift/OR updates per dimension. Distinct 2,048 × 768D origins
   imply about 6.29 million packing updates per query. SIMD bit-transpose packing
   is a specific parity-testable experiment. The AArch64 scorer also performs
   four horizontal popcount reductions per pair of code words. Benchmark the
   complete preparation/scoring/candidate pipeline; same-origin preparation
   reuse and SIMD scoring already exist.
4. **Useful concurrency rather than more admitted work.**
   Clean mapped 1M throughput is 610–614 QPS at C10, 361–366 at C20 and 314–331
   at C30; CPU/query rises from about 12.3 to 30–32 ms. At 50K, throughput also
   declines after C10. Reduce scan work and broad synchronization before raising
   worker limits. Existing aggregate admission holds a driver slot even while
   waiting for scan capacity; a phase-aware policy needs independent qualification.
5. **Durable work proportional to mutations.**
   Shared origins, per-leaf revision debt and durable row-reference translation
   could avoid repeated checkpoint encoding. They exist experimentally; an earlier
   integrated row-store run wrote 3.8–4.0x checkpoint bytes and regressed queries.
   Later fixes need fresh ingestion/churn/restart qualification. Remaining split
   loading was already reduced to 22–23 seconds within roughly 280-second readiness;
   another exact-load-only rewrite cannot make total readiness 10x faster.

CPU samples identify active functions and wait call chains, not CPU percentages.
High-concurrency sampling substantially perturbs throughput. There is also a
profiling-path mismatch: the normal DB wrapper registers foreground-query activity,
but the profiled dense wrapper omits that accounting. Fix that parity and instrument
the actual provisioned endpoint before assigning all HTTP-minus-dense time to one
component. Separate normal-request DB-wrapper diagnostics confirm apply-lock waits without
the CPU sampler: at 1M/C30, mean acquisition is 36.006 ms and p99 is 196.575 ms,
versus 1.642 / 13.749 ms at C10. Diagnostic throughput matches clean windows.
At 50K/C30, identity-generation capture also averages 5.215 ms; absent an in-memory
visibility summary, it reads metadata through the LSM and its snapshot locks.
The query bundle should carry a correctly published identity token as well as
payload generations. These last-1,000-wrapper diagnostics use logging and are
kept separate from clean A/B timing; stage p99 values cannot be added.

Storage has a different ceiling: 1M × 768 × float32 is 3.072 GB of authoritative
payload alone, versus roughly 4.1 GB total in the earlier qualified store. Remaining
metadata elimination cannot give 10x total disk savings at unchanged encoding.
Memory, cold reads, enrichment/full text, multi-index use and mixed updates/deletes
still require fresh qualification for these new read experiments. No complete-query
10x improvement is established. Detailed evidence and rejection gates are in
[BOTTLENECKS.md](../../../.benchmark-results/vector-store-unified-fetch-20260911/BOTTLENECKS.md).

## September 11: fetch-batch dispatch investigation (not promoted)

The next experiment replays complete, real-file float32 fetch batches through
production exact reads, including CRC checks. It recovered the previous trace
by matching retained immutable source copies and validating every logged range:
128 batches per scale, 7,303 requests at 50K and 9,120 at 1M. Payload file hashes
and explicit remapping are preserved. Identity resolution and ANN scoring are
outside this replay; a controlled one-miss split tests the extra batch barrier.

The expanded warm replay found that one optional helper reduced serial batch
latency by 12.6% at 50K and 27.8% at 1M. Inline reads used substantially less CPU
and reduced time per completed batch at ten concurrent callers by 6.1% / 7.5%,
but increased serial time by 21.0% / 4.0%. Splitting off one miss did not produce
a consistent large penalty. The shorter first replay showed larger concurrent
inline gains. Neither result establishes service throughput or justifies a new
file format or callback contract.

`ANTFLY_EXPERIMENT_VECTOR_READ_INLINE=1` drains positional payload batches in the
caller; `ANTFLY_EXPERIMENT_VECTOR_READ_SINGLE_HELPER=1` permits one helper per
batch. Inline takes precedence. Defaults retain the existing bounded workers and
all existing global admission/lifetime/cancellation rules. Separate
`ANTFLY_EXPERIMENT_VECTOR_READ_PROFILE=1` runs expose exact-read physical batches,
requests, admitted helpers, denied launches, dispatch/caller/join times and summed
helper wall time. Summed helper wall time overlaps other work and is not CPU time.
Logical binding batches, mixed batches and maximum observed binding-array bytes
are also visible in query profiles; helper pressure is exported in metrics.

Three clean replay rounds reverse order; the fourth attribution round is excluded
from medians. Process CPU is measured separately. Descriptor-local `F_NOCACHE`
experiments are included, with no claim that device caches or existing filesystem
pages were cold. The replay helper limit was 16; the service's existing limit
was 28 on this host. All 84 service windows and four additional serial-profile
windows passed retained hit-set gates. Clean timing and attribution were separate;
all timing outliers remain included. These are retained-index query comparisons
against the existing vector-store path, not fresh ingestion or LSM ownership.

| Workload | Candidate | C1 QPS | C10 QPS | C10 p99 | C10 CPU/query |
|---|---|---:|---:|---:|---:|
| 50K | Bindings + single | +16.9% | +21.6% | -15.0% | -20.3% |
| 1M | Bindings + inline | -19.9% | +38.6% | -41.5% | -26.8% |

The combined 50K candidate improved C10 throughput in all four pairs and C1 in
both pairs. At 1M, inline plus bindings improved C10 in three pairs, regressed in
the fourth, and regressed C1 in both pairs. Dimensions also differ between these
workloads; these results do not establish a table-size crossover.

Combined C10 profiling cut mean 1M vector-loading time from 3.865 to 0.921 ms.
The focused C1 diagnostic instead measured 4.577 / 4.244 ms for the candidate
versus 1.901 / 1.877 ms for the control, despite under 0.4% binding misses and
similar ANN leaf-scoring time. Candidate caller reads accounted for 4.397 /
4.055 ms. The serial tradeoff is therefore in the payload stage; binding/order
and dispatch both differ in this comparison, so kernel I/O versus scheduling is
not individually attributed. Some captured HTTP tails also substantially exceed
the dense-search timer; their remaining time is not yet assigned to a component.

Keep the settings off by default. A shared nonblocking helper budget is one
component of the next candidate, not a complete new design: experimental
aggregate caller/helper admission already exists. The architecture review below
refines this direction. Preserve measurable batch-size/dimension/read-latency
effects; do not hard-code a table-size threshold from two datasets.
Native and focused index regressions passed. Fresh ingestion/churn/readiness and
matched LSM-ownership qualification remain necessary before promotion.

Artifacts: [`vector-store-fetch-batches-20260911`](../../../.benchmark-results/vector-store-fetch-batches-20260911/README.md).

## September 11: long-term read-path review after dispatch experiments

Retain one authoritative source payload and generation-safe indirection. The
recommended next shape is **one bounded resolve → fetch → score pipeline per
rerank wave**, with execution policy beneath it. This is a design direction,
not a claim that its performance is already qualified. Changing worker counts
alone has repeatedly exchanged concurrent throughput for serial latency.

The code review establishes several useful boundaries:

- `ResourceManager.tryAcquireDenseReadTask` already limits helpers globally.
  `ANTFLY_EXPERIMENT_AGGREGATE_ADMISSION` additionally shares capacity with query
  drivers, acquired before generation/scan admission and retained across both
  query phases. Helpers cannot wait or bypass queued drivers. This is admitted
  work accounting, not a measurement of executing CPU: drivers also retain
  their slot while waiting for bandwidth or payload reads. Earlier recovery-v2
  diagnostics improved C30 throughput by 11.8% / 3.9% at 50K / 1M while reducing
  C1 by 12.7% / 5.5%. Those older workloads are not matched R21 comparisons.
- `runPositionalReadBatchProfiled` uses the existing shared I/O thread pool;
  it does not create OS threads per vector. It does create a group and attempt
  up to seven helper submissions per batch, with one atomic claim per request.
  A new persistent executor would need to beat this implementation, not merely
  duplicate its persistent threads.
- Member-binding hits are currently read and scored before HBC resolves misses
  through metadata and the ordinary exact loader. R21's serial 1M candidate
  performed about 5.3 physical batches for 4.0 logical binding batches/query.
  The controlled singleton-miss replay did not show a consistent large penalty;
  consolidation is a cleaner execution boundary, not yet a proven large win.
- `exactVectorBlockDistance` already scores aligned float32 bytes directly.
  The ordinary exact read still performs `pread` into scratch and validates the
  payload. `viewExact` can borrow mapped bytes under a generation lease, and
  the projection path already has bounded clean-page borrowing. Neither proves
  that direct mapping or another cache improves float32 service performance.

The proposed pipeline has these responsibilities:

1. **Resolve without fetching.** Use the existing index incarnation/member ID
   binding first; resolve only misses through authoritative artifact metadata.
   Carry each result's original position and retained source location in one
   bounded request array. Preserve the ANN/source sequence fence, missing-value
   behavior, encoding/dimension checks and float16 projection/residual reuse.
   Fetch hits and resolved misses together within the existing rerank wave;
   do not eagerly fetch later waves that the exact stopping proof may exclude.
   Explicit positions also avoid the current repeated member-ID output search.
2. **Borrow verified hot bytes or issue bounded reads.** A borrowed value must
   retain its immutable generation and any cache lease through scoring. Keep
   CRC validation and the existing authoritative fallback. Test mapped views
   and bounded cache borrowing separately: direct mmap can fault synchronously,
   while a user cache adds memory and may amplify reads. Do not turn the entire
   source into a second resident serving copy. Preserve bounded scratch for
   misses, cross-page values, residuals and unsupported view alignment.
3. **Make optional parallelism proportional to remaining work.** Reuse the
   current nonblocking helper admission and caller-progress guarantee. Test a
   small local cap and a minimum useful byte/request budget before attempting
   adaptive control or another executor. Under load, spare helpers should yield
   to query drivers; at low concurrency they may overlap slow reads. Keep
   scan-bandwidth, scratch-memory and helper limits explicit: a CPU-count cap
   alone does not describe memory bandwidth or useful in-flight I/O. Any future
   phase-aware policy must preserve admission ordering and fairness. Cancellation
   must drain issued work before releasing request buffers or source leases.

Keep file layout and durable identity independent of this execution policy.
An artifact-version directory shared across indexes remains a plausible later
replacement for the disposable member cache. A parent docid alone still cannot
identify different chunks, models or versions. Updates publish new versions,
deletes change current visibility, and old-reader leases delay reclamation;
dropping all ANN indexes must preserve source ownership. A new directory must
earn its publication, recovery and memory cost: R21's serial candidate already
had fewer than 0.4% binding misses, yet payload loading remained slow.

Do not prioritize broad read coalescing from the current traces. Even allowing
64 KiB gaps reduced modeled spans by only 1.63% / 0.20% while adding 8.3% / 1.8%
bytes at 50K / 1M. This is bounded trace geometry, not measured device I/O, and
does not rule out a different physical arrangement. Source placement must also
serve multiple independently partitioned indexes without repeated full copies.

The next discriminating experiments, in order, are:

1. Hold bindings, physical order, query working-set cadence and encoding fixed;
   compare zero, one and original helpers at C1, C2, C4 and C10. Independently
   include the existing aggregate-admission setting. Sample helper start delay,
   per-read wall versus thread CPU, faults and process footprint; retain clean
   timing windows without that instrumentation. The combined R21 C1 comparison
   changed binding order as well as dispatch and cannot isolate those causes.
2. Add logical-wave IDs, result positions and binding hit/miss membership to
   bounded replay traces. Compare unified resolution with the split path at
   identical physical requests/order and dispatch. Preserve early termination,
   updates/deletes, old readers, restart and corruption checks.
3. Use those same requests to compare exact copied reads with existing mapped
   views, then bounded borrowing if reuse justifies admission. Measure bytes,
   CRC/scoring time, CPU, latency and memory under pressure; do not equate
   `F_NOCACHE` replay with cold-device qualification. Measure chunked work claims
   or a reusable executor only if dispatch remains material after these changes.
4. Combine only independently useful changes, then repeat fresh ingestion,
   churn/reclamation, readiness/restart and matched LSM ownership qualification.
   No table-size heuristic, new durable format or default promotion follows
   from the current two datasets. Remaining HTTP tails also need timing outside
   dense search before attributing them to this payload pipeline.

This review changes documentation only. It adds no benchmark result or runtime
behavior beyond the recorded R21 experiments.

## September 11: existing-member source bindings (not promoted)

`ANTFLY_SOURCE_VECTOR_MEMBER_BINDINGS=1` connects the existing index-scoped ANN
member identity to a compact source row under the retained ANN/source generation.
A hit precedes ANN metadata loading and skips artifact-key construction, reference
lookup and source digest lookup. It retains the ordinary bounded payload reads,
CRC checks and exact scoring. This experiment currently accelerates float32;
float16 keeps its projection/residual reuse path. WAL payloads and misses use the
existing authoritative resolution. No persisted format or ownership default changes.

A parent DocOrdinal alone cannot identify multiple chunks or model artifacts.
The current ordinal-to-vector maps are also mutable and cover ordinary document
embeddings, so borrowing those maps from an old query would be unsafe. This
implementation uses the already assigned ANN member ID and the index's existing
capture incarnation, inside one table's immutable vector generation. It allocates
no new document IDs. Artifact identity/version is checked by the normal reference
resolution before admitting a binding. Recreated indexes get new incarnations;
new generations and restart start empty. Old query leases retain their original
source files and bindings across updates, deletes and compaction.

Each binding is 40 bytes: index incarnation, member ID, source reader/row, and
logical source sequence/revision. It reconstructs scoring metadata directly from
the validated immutable row; it stores neither payload bytes nor a duplicated
full location/digest. Four-way buckets allocate lazily in 256 independent stripes.
A busy stripe declines immediately. Arrays are admitted to
`dense_source_payload_state` and can be reclaimed independently. Capacity follows
immutable reader row counts, capped at 1,048,576 entries (40 MiB per live
generation), shared across that table's indexes. There is no corpus scan at
publication and no attempt to copy bindings to replacement generations.

This is a disposable member-to-row acceleration layer, not yet a persistent
ordinal/radix directory co-located in AFVD. It tests whether bypassing the entire
identity lookup chain is valuable before adding durable format and lifecycle
complexity. A working set larger than capacity can still miss; publication can
also make it cold. The microbenchmark and retained 50K/1M comparisons must expose
those costs, memory residency, hit rate and query latency. Explicit profile
counters are `hbc_rerank_member_binding_hits` and
`hbc_rerank_member_binding_misses`.

The corrected metadata microbenchmark completed four forward/reverse rounds on
64-shard, current-format source/reference files. For a repeated 10K-request
working set, median full lookup versus binding-hit latency was 556 ns versus
15 ns at 50K, and 1,385 ns versus 18 ns at 1M. It forces the complete location
value to escape optimization. These are in-memory metadata measurements, not
payload-I/O or query-throughput results. The original smoke fixture's recursive
owner comparison and offset-only consumption were rejected before qualification.

All eight retained-topology C10 arms passed activation and identical-hit-set
gates. Despite removing over 99% of rerank metadata fetches, median paired peak
throughput changed by **−4.6% at 50K and −7.8% at 1M**; p99 latency increased
21.1% and 56.3%. Profiled vector-loading time fell 22.2%/35.5% in the 50K pairs;
at 1M it changed +0.7%/−42.7%. Physical vector-read counts and bytes stayed
essentially constant. Profile phases and clean timing phases have different
host/scheduling conditions; these figures do not identify a single cause.
Measured peak process footprint increased 16.5–22.3 MiB at 50K and 19.5–62.4 MiB
at 1M. This is not a qualified peak-throughput improvement.

The cache is therefore off by default. All eight serial (C1) diagnostic arms
also passed activation and identical-hit-set gates. At 50K, throughput improved
40.3% / 18.0%; at 1M it changed −17.8% / +24.2%. In the separate 1M profile
phases, artifact-read time fell 62.8% / 66.1%, and dense-search time fell
23.7% / 22.8%. Serial results therefore show a useful fetch-path benefit but
also substantial timing variability; concurrency alone is not a proven cause.

Before selecting another optimization, instrument logical versus physical batch
counts, mixed hits/misses, worker admission/dispatch/join, read CPU/wall time and
binding residency. Replay complete real fetch batches in microbenchmarks using
real payload files, dimensions and CRC checks, with warm/cold and serial/concurrent
conditions. Isolate preserving one payload batch for hits and misses and inline
versus bounded worker dispatch. The current early scoring callback can issue hits
before the fallback batch; this is a structural difference, not an established
cause. Use shorter interleaved A/B blocks on the available host to reduce time
drift, then repeat end-to-end qualification. A persistent document/artifact
directory or a new payload layout should follow this evidence.

Post-timing lifecycle copies passed two rounds of 2,000 updates/deletes/restores
at both scales. Each of 1,000-query pre-restart, cold-restart and warm-restart
checks measured the same recall: 0.98541 at 50K and 0.99060 at 1M. The first
lifecycle harness attempt incorrectly treated search width as returned hit count;
it made no mutations and remains preserved separately. Correctness suites passed
57 source tests, 36 native tests and four focused manager tests, without failures
or leaks; the gated microbenchmark was also run explicitly and passed.

The exact-read path now participates in the bounded location trace, correcting
the previous diagnostic's float32 coverage gap. Traced runs remain separate from
clean timing. Scripts, frozen source, tests and results live under
[`vector-store-member-bindings-20260911`](../../../.benchmark-results/vector-store-member-bindings-20260911/README.md).

## September 11: reference lookup and physical locality

The stable 50K diagnostic reproduced the query throughput gap: vector-store
ownership was 10.6% / 15.1% slower than LSM ownership on retained, settled indexes.
All hit sets stayed unchanged within each ownership topology. The original cache
treatment was inactive: it allocated 8,922,208 bytes but recorded zero hits and
misses. `Store.snapshot` cloned a published native view without rebinding its
table-owned `reference_location_cache`. Native catalog clones intentionally omit
that pointer. The fix binds it explicitly at the source snapshot ownership
boundary, matching the writer-snapshot path and preserving generation/shard
validation. The regression now exercises both paths through WAL checkpoint,
source relocation, and old-reader access, with float32 and exact float16 payloads.

Artifact references already live in the ANN serving artifact records. The source
store holds the one full payload copy. The first fresh 1M comparison measured
about 3.19 GB of full serving vectors under LSM ownership versus 150 MB of serving
metadata/references under vector-store ownership, plus 3.19 GB in the shared source
files. Source ownership also removes the full primary embedding values. The
extra digest lookup is an implementation cost, not a required consequence of
eliminating duplicate payloads.

The independent locality treatment resolves identities first, then orders complete
payload-read requests by retained source owner, file, and byte offset. Each request
carries its result position, destination buffer, value, and error. Both projection
and exact reads participate. Authoritative float32 reranking uses the exact-read
API; the compact float16 path uses projection and residual reads. No persistent offsets or extra vector copies
are introduced. `ANTFLY_SOURCE_VECTOR_PHYSICAL_RERANK_ORDER=1` enables it, and
`antfly_dense_physically_ordered_{batches,requests}_total` metrics prove activation.
It is off by default pending measurement. Focused validation passed 57 source
ownership/recovery tests and 34 native storage tests, including reordered reads
with a failing destination and successful siblings.

A possible next format would co-locate an optional physical-location hint with
the ANN artifact reference (or the already-read ANN vector metadata). Keep the
artifact version/model identity and digest authoritative. A hint may contain a
source segment generation, shard, offset, encoding/length, and integrity binding;
it must validate against the retained source snapshot. If compaction relocates a
payload, an older reader keeps its leased files, while a newer reader rejects a
stale hint and resolves the digest. Build or refresh hints incrementally; a full
payload-directory traversal on every publication would recreate ingestion costs.
Different ANN indexes can carry different small hints while sharing the same
model/version payload. Persistent memory addresses, unchecked offsets, and a
second full serving payload corpus are not part of this proposal.

The qualification completed all 18 arms, isolating cache-only, ordering-only,
and their combination on the same binary at 50K and 1M, with forward/reverse
order, activation checks, and unchanged hit sets. Median paired throughput
changes versus shared control were −51.7% / −11.9% for cache-only, +11.4% / −15.5%
for ordering-only, and −5.3% / −27.1% for both (50K / 1M). Neither option is promoted.
The 50K LSM controls in this follow-up were slower than shared control, reversing
the earlier ownership diagnostic; preserve both observations. Separate profile
phases do not explain the full unprofiled throughput changes, so no single causal
attribution follows from these ratios. Results and frozen inputs are retained under
[`vector-store-reference-locality-20260911`](../../../.benchmark-results/vector-store-reference-locality-20260911/README.md).

## File-format investigation: locate first, then fetch

The ownership comparison is not a comparison between an SSTable vector read and
an optimized vector-file read. With native ANN storage enabled, **both modes
already use AFVBLK serving files**. LSM ownership gives an ANN artifact key a
full serving vector. Shared ownership gives it an authenticated 32-byte digest,
then resolves that digest in the retained source catalog before reading the
payload. The latter saves a full serving copy and primary payload storage, but
adds metadata work. Payload access uses retained descriptors and bounded `pread`
workers in both modes. A recorded physical-read counter counts these application
reads, not SSD operations: warm reads can be served by the OS cache.

Current AFVBLK V4 has an 88-byte index entry, separate key and payload arenas,
and bounded streaming publication. Float32 rows use four bytes per component.
Float16 uses two-byte components plus a separate exact residual: ordinarily 13
bits per component and a four-byte exception count, with an exception path for
other IEEE-754 values. Thus ordinary full exact storage is about 29 bits per
component, before metadata/exceptions, not a 50% reduction. Reading only the
candidate plane saves bandwidth; reconstructing exact values adds residual I/O
and decoding. The current ownership/locality experiment fixes encoding at
float32, so it cannot establish a float16 format win. An environment change on
an existing database is not a fresh encoding comparison.

The identity prototype should start with the existing DocID machinery.
`db/doc_identity.zig` already supplies namespace-scoped document ordinals,
canonical identities and generation-aware document visibility. Dense indexes
already map ordinals to ANN vector IDs, and the native vector directory carries
member metadata. The missing connection is from those identities to a particular
source artifact version and its leased location. A new parallel document-ID
allocator is not the intended design.

Conceptually, use `(identity namespace, DocOrdinal, artifact slot, artifact
version)` to identify a source payload. The artifact slot distinguishes embedding
families/models and derived chunk/extraction members; document visibility alone
does not distinguish old/new embeddings or several embeddings on one document.
This tuple describes logical identity, not a requirement to repeat every field
in every row. Bind the existing ANN member metadata to that source identity when
publishing a generation, and retain or resolve its location under the same
snapshot. Keep artifact integrity and preparation/retry validation. Reuse the
existing namespace/handoff rules and explicitly qualify ordinal remapping during
shard movement. The prototype should remove the current member-metadata → string
artifact key → digest → source lookup round trip, while preserving versioning and
zero-index source ownership.

The most useful format alternatives have different costs:

| Alternative | Intended saving | Cost and constraint |
|---|---|---|
| Physical hint beside an ANN reference | Avoid repeated source digest lookup on a valid hint | Small per-index metadata; validate identity and generation; fall back after relocation. Incremental refresh only. |
| Existing document/artifact identity plus snapshot-owned location directory | Direct array/radix lookup shared by every index, independent of physical placement | Reuse namespace/ordinal lifecycle; distinguish artifact slots and versions; bounded copy-on-write directory pages; preserve preparation/retry semantics and old-reader identity/integrity. |
| Homogeneous dimension/encoding chunks with ordinal addressing | Smaller per-row metadata and predictable offsets | Mixed models/dimensions need separate chunk classes; variable residuals still need offsets; avoid tiny-chunk fragmentation. |
| Source-owned spatial packing and selective adjacent coalescing | Fewer scattered reads without another vector copy | Source placement cannot depend on one index's changing topology; extra bytes and rewrite debt can exceed saved reads. |
| Compact candidate plane with exact residual completion | Fetch fewer bytes for candidates safely excluded by score bounds | Bound checks, decode CPU, and residual reads; preserve exact reconstruction and the existing recall policy. |

A handle derived from existing document/artifact identity is an alternative to putting a full physical locator in every
posting, not an additional mandatory format layer. The source location directory
would be shared across indexes and updated by changed pages when compaction
moves payloads. Old readers keep their directory and segment leases. Updates
allocate a new artifact-version identity; deletes remove current visibility,
and reclamation waits for all current references and older leases. Dropping the
last ANN index must not delete source objects. Artifact/model identity remains
in the authoritative envelope; equal dimensions do not imply equal embeddings.
Handle namespaces must also survive snapshot restore and shard movement without
collisions; those lifecycle paths need explicit qualification before enabling a
new handle format outside fresh standalone tables. Local warm-read measurements
do not establish the best chunk size for cold or remote object storage.

Avoid a blanket page-alignment or page-cache rewrite. Earlier source-only packing
screens and borrowed-page experiments in `VECTORDBBENCH_FINDINGS.md` measured
limited span reduction and extra fetched bytes. Posting-local float16 copies
previously improved locality considerably but restored a large second plane;
that is a space/throughput option, not single-copy consolidation. The next format
must earn its complexity with actual request traces, bounded memory and write
amplification, and fresh encoding comparisons. Reference-cache and request-order
measurements are kept separate from these unimplemented alternatives.

At 1M the clean profiled phases also report 5.17–6.55 ms of candidate-scan
admission time, 7.68–10.26 ms of leaf scoring, and 2.93–6.39 ms of artifact-read
work per query. The scan bandwidth cap is about 128 MiB, with approximately
23.5 MB reserved per active scan and a measured peak of five active queries at
C10. At 50K this gate did not queue requests. Native descriptor-admission wait
counters remained zero. Do not confuse the small general admission timer with
`hbc_scan_admission_wait_ns` or describe all of these delays as vector I/O.

`ANTFLY_EXPERIMENT_PHASE_ADMISSION` already supports releasing scan bandwidth
before acquiring a separate rerank lane; this comparison held it off. Prior
experiments qualified useful high-concurrency tradeoffs but observed C1 costs.
A location-format change will not automatically fix scan admission or scoring.
Keep phase/aggregate admission, exact scan-byte accounting, and scoring work
independently measurable alongside any source-format prototype.

The follow-up completed eight separate CPU-sampled cache arms with unchanged
hit sets and proven cache activity. The large 50K clean-QPS regression did not
repeat at the same magnitude under sampling; visible metadata work fell, while
payload I/O and ANN scoring remained substantial. This does not prove either a
cache benefit or a cache-lock explanation for the original regression.

A new location-layout estimate did **not** qualify: the existing trace hook only
covers the compact projection API, and this float32 workload uses exact reads.
No live trace rows were emitted. The analyzer rejected that empty capture. Before
changing chunk layout, extend the hook to exact reads and require observed,
complete, checksum-verified batches. Preserve the earlier float16 packing screens
as separate evidence. Detailed results and excluded attempts are in
[`attribution/README.md`](../../../.benchmark-results/vector-store-reference-locality-20260911/attribution/README.md).

The next bounded prototype should separate source-reference resolution time from
payload I/O and full request dispatch/response time, then compare generation-bound
co-located hints with a compact stable-handle location directory. Keep cache-only,
physical sorting, phase admission, and any encoding/layout rewrite independent.
Qualify relocation, old readers, updates/deletes, restart, multiple model/index
identities and zero-index source retention before measuring the winning shape.

## September 11: fresh source-ownership comparison

The admission/replay candidate passed fresh `primary_lsm` / `vector_store`
comparisons in A/B and B/A order at both 50K and 1M, plus four enrichment arms.
Every scale arm passed churn and unchanged before/cold/warm restart recall;
vector-store reclamation checks passed. One frozen binary serves both ownership
modes, with float32 encoding, native ANN storage, batch/positional reads, shared
catalogs, replay reads, durability, concurrency, and memory admission held equal.
Append-only segments, selective GC, and location caching remain off.

| Median paired vector-store change versus primary LSM | 50K | 1M |
|---|---:|---:|
| Readiness time | −10.3% | −33.7% |
| Peak query throughput (C10 in every arm) | −13.4% | +25.7% |
| C30 query throughput | −1.5% | +1.1% |
| Mixed query throughput | +5.2% | +25.7% |
| Fixed-count churn time | +8.7% | −1.3% |
| Total logical disk after restart | −49.2% | −42.4% |
| Sampled process logical writes | −17.0% | −21.9% |
| Process peak physical footprint | −44.6% | −55.7% |

These are medians of within-pair ratios, not ratios of medians. The available
host was shared; no competing diagnostic or build ran during timed arms. At 1M,
readiness was 579.118 → 277.309 seconds in A/B and 347.151 → 294.118 seconds
in B/A. Peak QPS was 299.246 → 432.166 and 365.066 → 390.471. The direction
repeats, but the effect size varies substantially. Mixed QPS improved 54.8% in
the first pair and fell 3.4% in the second. Memory and tails are also mixed:
whole-process peak physical footprint fell, while the brief reopened serial-query
phase showed greater mapped residency/footprint and sparse resource samples;
1M cold-restart query p99 increased 24.7% by the median paired ratio. A process
footprint reduction is not a claim that every query-phase memory metric improved.

The 50K peak regression is under separate investigation using the same saved
index topology with location caching off/on. Serial post-restart profiles load
roughly the same number of rerank vectors but show additional artifact-read time
under source ownership: about 0.71–0.72 versus 0.51–0.52 ms at 50K, and 1.31
versus 0.91 ms in the first 1M pair. Reference resolution adds a source-key lookup;
physical requests are ordered by ANN artifact keys rather than source payload
locations. These are candidate costs, not yet a causal attribution of concurrent
peak throughput. The completed locality diagnostic above does not justify a default change.

Receipts, all results, and the isolated diagnostic are preserved in
[`vector-store-ownership-compare-20260911`](../../../.benchmark-results/vector-store-ownership-compare-20260911/README.md).

## September 10: admission, replay, and routing-cache follow-up

The next candidate addresses the remaining admission and loading costs, together
with correctness failures exposed by stricter recovery qualification. The previous
50K candidate failed its final restart check. The replacement has passed twelve
50K arms, six 1M loading arms, and a separate full 1M workload/recovery check.
These are not fresh primary-LSM ownership comparisons, so table defaults remain
unchanged.

* Resource reservation, batch-reservation, and observer identity ledgers bound
  deleted hash-table slots with allocation-free rehashing after capacity/8
  successful removals. This prevents accumulated tombstones from turning a missing
  identity lookup into a capacity-sized scan under the shared admission mutex.
  Split workspaces cache their configured capacity and update residency accounting
  only when allocated capacity changes, removing repeated manager calls per vector.
* `ANTFLY_SOURCE_VECTOR_REPLAY_READS=1` lets ANN split/refresh matrices combine a
  native generation certified at the capture's exact base sequence with the latest
  captured mutations. Updated payloads override the base; tombstones never fall
  through to old vectors. Metadata/model identity and dimensions must match. An
  older/newer native generation or dirty projection falls back to authoritative
  artifact reads. Captured vectors already belong to the replay window, so this
  adds no second retained payload corpus and does not weaken query readiness.
* External-vector updates obtain their previous centroid contribution from the
  certified native base, never from the already-updated primary artifact. Missing
  previous versions, repeated captures, or dirty centroid origins force a complete
  recomputation. External update batches coalesce final leaf work so recomputing
  against the committed source batch does not double-apply a later update. Deferred
  ancestor work resolves surviving leaves' final parents after splits/merges.
  Deltas are used only when membership stays unchanged throughout the batch:
  a later removal can recompute the mean and already include an earlier update.
  Membership-changing batches reconstruct final affected centroids once; leaves
  that have been deleted or converted to internal nodes are skipped.
* Quantized-cache replacement suppressed during publication now invalidates the
  previous entry. Previously, a pinned internal node could retain old routing
  scores when child count stayed constant, changing results when evicted or
  reopened. A deterministic 512-vector update test reproduces the failure and
  checks cached values, cache eviction, checkpoint byte preservation, and restart.
  Cache reads also check the transaction's publication epoch before and after
  retaining a node, quantized payload, vector, or metadata entry. An old snapshot
  cannot consume a newer cache entry or refill current vector residency from old
  values. Readers still reuse current caches; older readers fall back to their
  leased storage without forcing the whole query to restart. Unbound cursors do
  not read or fill the vector cache. A second regression covers old node/vector/
  quantized reads, and a deterministic interleaving checks publication during
  cache acquisition. Native search admission carries the epoch sampled before
  retaining its generation into the query transaction; delayed admission cannot
  pair an old generation with a newly sampled cache epoch. A regression reproduces
  the old-generation read returning the newly published vector before this fix.
* Deferred quantized rebuilds now publish payload freshness in the same transaction
  as the rebuilt payload and update the cached node state. A focused regression
  showed that completed rebuilds previously left `payload_dirty` set. Startup
  validation then cleared 128 such records in the failed 50K control, changing
  which leaves used quantized scoring at unchanged sequence 814. Missing payloads
  remain dirty, and allocation/source errors propagate instead of certifying an
  unsuccessful rebuild.
* Posting-WAL patch preparation skips byte-identical packed/scoring values using
  the base already resolved for encoding. These values create neither a copy-only
  patch nor a live overlay that unnecessarily disables the native scan plane.
  Coverage still advances durably, and unchanged generation leases remain valid.

The harness now repeats the same serial recall pass immediately before shutdown
and after cold/warm reopen. It rejects a change greater than one reported precision
unit (0.0001), keeping churn effects separate from restart effects. This caught a
50K control changing from 0.9590 to 0.9704 on unchanged source data and stopped
the queued 1M runs. The cache regression above subsequently narrowed the failure
to eviction alone. Three arms of the next candidate passed, but its last control
changed from 0.9589 to 0.9691. The deferred-freshness regression above explains
a further recovery-dependent change in serving behavior. The replacement candidate passed
this gate in all twelve 50K arms before its larger comparisons. Earlier R10 pre-churn versus post-restart recall differences
cannot by themselves be attributed to restart.

Frozen sources, binaries, runners, and results are retained under
[`vector-store-admission-epoch-20260910`](../../../.benchmark-results/vector-store-admission-epoch-20260910/README.md).
The comparison isolates replay reads, then append-only segments and selective GC
with matched GC limits. Fresh 1M load comparisons isolate the identity-ledger fix
from the complete candidate in forward and reverse order; load-only runs do not
qualify query tails, churn, or restart.

The admission-epoch candidate has now passed all four 50K replay-read arms,
including unchanged before/cold/warm recall, workload checks, and reclamation.
Replay eliminated primary-store matrix fallbacks in these arms. Initial split
vector loading dropped from 0.938/0.906 seconds to 0.162/0.356 seconds. Median
paired mixed QPS improved 19.1% and C30 QPS improved 5.5%, but readiness was
mixed (14.81 → 14.32 seconds in A/B; 14.40 → 17.01 in B/A). Profile p99 and
read-only memory also regressed. The full measurements and receipts are in the
candidate directory above. All twelve 50K arms subsequently passed. Append-only
segments reduced sampled logical writes 15.5% but reduced mixed QPS 12.2%;
selective GC reduced writes another 23.0% against its append-only control but
reduced mixed QPS 8.5% and increased fixed-churn time 6.3%. Both remain
experimental and off in the 1M loading candidate. Fresh 1M loading comparisons are complete:

| 1M measurement | Prior A/B | Complete A/B | Complete B/A | Prior B/A |
|---|---:|---:|---:|---:|
| Readiness (s) | 523.179 | 286.749 | 279.243 | 694.012 |
| Synchronization (s) | 278.131 | 108.271 | 104.273 | 447.321 |
| Split vector loading (s) | 137.881 | 22.277 | 22.946 | 269.885 |
| Primary matrix fallback vectors | 1,399,904 | 0 | 0 | 1,408,390 |

The ledger-only treatment reached readiness in 326.577/286.556 seconds, with
42.296/38.783 seconds of split-vector loading. This isolates a substantial
admission bookkeeping cost before replay reads remove the primary fallback.
The complete candidate improved readiness another 12.2%/2.6% over ledger-only.
All six loads passed. These compare vector-store implementations, not storage
ownership defaults. A separate fresh 1M query/churn/restart qualification also passed, with identical
0.9906 recall before/cold/warm restart and no workload errors. A supplemental
reclamation clone retained exactly 1M payloads / 3.072 GB of payload bytes and
zero unreferenced bytes, with post-GC queries passing. The clone was ready after
16.06 seconds and settled after 17.07 seconds. Peak sampled RSS did not improve:
6.39/6.88 GiB for complete versus 6.28/6.68 GiB for prior in the loading pairs;
this includes mapped-file residency. The configured 4 GiB admission budget is
not a hard RSS ceiling. The remaining 22–23 seconds of split loading, routing
work, and startup inventory/marking are still measured optimization opportunities.

## September 10: fresh checkpoint-admission comparison

The source-lock changes work, but the complete candidate is not ready for
promotion. Fresh `vector_store` tables were run sequentially in A/B and B/A
order against the original frozen vector-store executable, with matched data,
encoding, ANN settings, durability, batching and concurrency.

| Loading measurement | Original control | Complete candidate |
|---|---:|---:|
| 50K readiness, A/B | 14.635 s | 16.851 s |
| 50K readiness, B/A | 14.179 s | 14.559 s |
| 1M readiness, clean B/A | 404.782 s | 649.376 s |
| 1M insertion, clean B/A | 358.279 s | 288.091 s |
| 1M synchronization, clean B/A | 46.504 s | 361.285 s |
| 1M split-vector loading, clean B/A | 90.344 s | 184.237 s |

Readiness regressed by 8.9% across the two 50K pairs and 60.4% in the clean 1M
pair. Faster insertion did not compensate for the synchronization tail. The
first 1M control reached readiness in 302.405 seconds; its candidate paused at
875K indexed vectors and was sampled after 479 seconds of arm elapsed time.
That candidate eventually reached readiness in 655.003 seconds, but is excluded
from clean paired ratios. The unsampled second candidate also paused, at 762.5K
indexed vectors. These load-only runs do not qualify 1M query/churn/restart behavior.

Both 1M candidates copied zero catalog bytes and recorded only 1.86/1.75 ms of
total source read-lock waiting. Their longest final source checkpoint commits
were 0.389/0.388 ms, while construction and reader preparation ran outside that
commit. The earlier 34.6-second source-mutex wait is gone in these observations.
The remaining delay appears around resource admission: the diagnostic sample
places a maintenance thread in `ResourceManager.issueIdentityLocked`, with the
ANN worker waiting in `bulkSplitVectorWorkspaceAdmit -> ResourceManager.sliceStats`.

Reservation-identity bookkeeping is the next target. In a separate probe after
timing completed, a heavily churned hash map with 128 live entries and capacity
262,144 took 1.348 seconds for 10,000 absent-key lookups, versus 0.033 ms in a
fresh map at the same capacity. Rehashing cost 0.136 ms and restored fast lookups.
The identity allocator checks absent, monotonically issued IDs under a global
mutex. This supports investigating deleted-entry accumulation and bounding
ledger lookup/admission work; the live table's occupancy was not captured, so
the probe is not proof of the sole cause or an end-to-end improvement estimate.

The separate 50K comparison isolated the new read/checkpoint path in the same
binary, with batching and shared catalogs enabled in both arms. It passed all
four lifecycle/error/reclamation gates: median paired readiness improved 1.3%,
mixed query throughput 4.0%, and mixed p99 8.7%, while mixed write throughput fell
2.5%. Concurrency-30 throughput fell 7.5% and p99 increased 6.3%. Restart recall
varied between arms, so restart latencies are not a matched-recall comparison.
All 10 API cases and four enrichment arms passed; small enrichment readiness
varied in both directions, while semantic throughput improved 20–24%.

Full results, frozen source, executable, validation and diagnostic evidence are
saved in
[`vector-store-checkpoint-admission-20260910`](../../../.benchmark-results/vector-store-checkpoint-admission-20260910/README.md).
No defaults were changed. Fix and measure resource-admission bookkeeping before
considering promotion or broader performance qualification.

## September 10: vector-loading root causes under qualification

The current experimental implementation separates read publication from source
writer exclusion. Enabling positional or snapshot source reads now maintains a
reference-counted immutable read view. Acquiring it takes only a short publication
mutex; point and batched payload reads allocate no lease metadata and never take
the source writer mutex, including when recording completion. ANN callers that
need an owned native snapshot clone it after releasing publication exclusion.
Read counters and active-session counts are atomic. Sessions still retain the
source before taking their primary snapshot; a read view is selected after the
primary artifact reference, preserving version visibility and GC protection.
GC fences its zero-session check with a session-start epoch across primary
snapshot acquisition. If a session starts in that window, marking defers even
if the session has already retired. This preserves the old-reader boundary
without putting session admission behind the source writer mutex. External
poisoning fences both the writer and published-reader paths.

WAL checkpoints and stable-tip base construction reserve one generation, seal the
committed WAL cut, then build immutable files outside the source writer mutex.
Ordinary preparations may append during that build, subject to the bounded WAL
admission window. Final preparation briefly fences new writers while constructing
the replacement manifest and reader view outside the mutex. The fenced commit
preserves later WAL extents without copying their payloads, publishes CURRENT,
then swaps the prepared read view. Old leases retain their blocks and WAL nodes.
GC and other checkpoint builders defer while that generation is reserved.
Admission waits use an epoch notification; checkpoint success and failure both
wake writers. Staged-file cleanup remains armed before publication and is disarmed
after an ambiguous CURRENT result so recovery can determine which files won.

The ANN compaction caller also releases its build mutex before source
checkpointing. It first pins the sealed ANN generation and exact WAL prefix,
then checkpoints the source and stages ANN shards in the existing optimistic
publication section. Concurrent ANN captures can append; the original
generation reservation, suffix validation, and final publication fence remain
in force. Releasing only the inner source lock would leave this outer stall.

Deterministic tests pause both checkpoint construction and publication, read while
holding the writer mutex, append after the checkpoint cut, force WAL admission
waiting, and reopen twice. Native tests include concurrent updates and tombstones,
stale prepared publication rejection, and sealed/unsealed WAL suffixes. Source
tests cover pre-publication failure, ambiguous CURRENT, and read-view allocation
failure. A GC regression opens an old primary reader and deletes its artifact
between the zero-session check and GC snapshot acquisition, verifying deferral
and eventual reclamation in both read modes. A caller regression verifies ANN
capture admission is open at the source checkpoint boundary. Checkpoint benchmark
events separate staging, publication preparation,
and commit time. Fresh loading qualification found the regression described above;
table ownership and experiment defaults remain unchanged.

Implementation validation passes 57 source tests, 34 native-store tests, the
ANN admission regression, and five DB lifecycle tests in each of default,
positional, and snapshot-read modes. These include independent models, stale
chunk deletion, source-hash reuse, restart without an ANN index, and rebuilding
after the last consumer is dropped. The final source and validation receipts
are preserved in
[`vector-store-checkpoint-admission-20260910`](../../../.benchmark-results/vector-store-checkpoint-admission-20260910/README.md).
Its release build and fresh runtime comparison completed as described above. The preceding
published-checkpoint experiment's completed API/enrichment checks and partial
scale run predate the final ANN admission and GC epoch corrections.

Source batching alone is not a demonstrated readiness improvement. In the first
fresh 1M pair, the serialized control reached readiness in 395.9 seconds and the
batched candidate in 457.6 seconds. Split-vector loading accounted for 99.4 and
169.4 seconds respectively. The candidate recorded approximately 24 seconds
inside source reads and 24 seconds waiting for the source lock; these source
counters cover its initial server lifetime, including subsequent workload steps.
They must not be subtracted from the initial-loading timer as an exact partition.
The second 1M pair also regressed: 547.1 seconds batched versus 377.5 seconds
control. That candidate includes two short stack samples and is not an
uninstrumented paired observation. The small enrichment update test was about 150 ms (7%) slower in both pairs,
which is insufficient evidence to dismiss the difference as fluctuation.

The loading path contains more than vector I/O. ANN metadata selects artifact
keys, primary values select immutable artifact versions, and source reads then
validate, decode and copy those versions. Two inefficiencies have been corrected
in the working implementation:

- Prepared source payloads now use a transaction-owned contiguous hash index.
  This replaces repeated full scans of the preparation list and suppresses
  identical preparations within a transaction. The identity still includes the
  complete artifact key, model namespace, source envelope and vector bytes.
  Old versions remain addressable independently. Index growth releases its old
  allocation; payload ownership ends with the transaction. An isolated 12,500-
  preparation, 20,000-read test measured 123 ms scanning versus 0.32 ms indexed;
  this is not an end-to-end readiness estimate.
- Write-transaction batch reads resolve their pending overlay first, capture
  mutable values and pin immutable generations together, then perform block I/O
  outside the LSM writer lock. The previous path held that lock across reads and
  decompression, explicitly disabling existing bounded parallel point reads.
  Returned values remain owned by the write transaction after the temporary
  probe retires. This improvement applies to both storage modes.

Stack samples during a later 1M candidate caught primary reference reads in the
serialized block-decoding path. A later sample attributed 29 split-loading
samples to primary-reference reads and 26 to source lookup, of which 24 were in
CRC validation. These are short diagnostic samples, not whole-run CPU percentages;
the sampled arm is identified in the benchmark evidence. Hot-buffer CRC testing
measured about 10 GiB/s, suggesting mapped-page access rather than checksum
arithmetic as the next hypothesis to test; the sample alone cannot distinguish
CPU work from page faults.

The opt-in `ANTFLY_SOURCE_VECTOR_POSITIONAL_BATCH_READS=1` path acquires a short
immutable source lease, releases the publication mutex, and performs bounded positional
reads directly into the caller's float32 batch. Both native CRC and the complete
artifact SHA identity remain mandatory. Float16 decodes from the same immutable
read view; retaining that view requires no optional allocation. Leases retain WAL versions and block readers
through all I/O, and callbacks run after reads complete. Counters record positional
bytes, batches, and admission fallbacks. This experiment requires source batching;
the `positional_reads` comparison preset enables batching in both arms.

Positional reads now also require shared immutable segment and manifest catalogs.
Previously, every bounded read lease copied metadata for the whole segment set;
the interrupted 1M candidate had copied 5.15 GB by 475K indexed vectors. Sharing
is established at open and before successor publication, so reader acquisition
retains catalog ownership instead of traversing all segments. WAL versions and
old segment files remain pinned until leases retire. Regression tests verify zero
additional catalog-copy bytes across repeated positional reads and old lease
validity across WAL updates, checkpoint publication, and source destruction.
The isolated `positional_reads` preset holds sharing enabled in both arms.

The fresh comparison is preserved in
[`vector-store-shared-read-leases-20260910`](../../../.benchmark-results/vector-store-shared-read-leases-20260910/README.md).
Storage tests use the newly frozen source. Timing uses the preceding frozen
ReleaseFast executable with its explicit sharing switch enabled, which executes
the same ownership path now required automatically. The control is the original
frozen executable, with fresh vector-store tables in A/B and B/A order at each
scale. These load-only runs measure readiness and loading costs; they do not
establish full query/churn/recovery qualification. The prior interrupted run and
its 342.4-second control (103.1 seconds of split-vector loading) remain preserved.

The completed sharing-only 50K pairs eliminated catalog-copy bytes but regressed
measured readiness by 9.8% despite reducing split-vector loading by 21–23%. Its
first 1M pair reached readiness in 513.6 seconds versus 349.7 seconds control;
split-vector loading was 153.8 versus 96.8 seconds. One interval spent 34.6 seconds
waiting for the source mutex, and the candidate accumulated 38.9 seconds of source
lock waiting overall. This is why immutable read publication and staged checkpoints
are necessary beyond catalog sharing. The reverse-order run was stopped at the
user's request to implement those improvements first; it is not a completed
comparison or evidence of a readiness improvement.

End-to-end stress exposed an existing I/O assumption that became reachable from
write-transaction reads: range futures required a concurrent worker and could
fail indexing with `ConcurrencyUnavailable`. They now use optional asynchronous
execution, which runs on the caller when worker capacity is exhausted. A regression
test exercises zero worker capacity, successful reads, cancellation, and read
errors. All 34 native storage I/O tests pass.

The batched payload path uses at most 32 vectors and approximately 128 KiB of
scratch (at least one vector for larger dimensions). It can borrow caller scratch
when optional allocation is denied. Callbacks run after the source lock retires.
Stage counters distinguish ANN metadata lookup, primary-reference lookup, source
lock wait, locked source work, and total payload consumption.

Qualification is preserved under `.benchmark-results/` in
`vector-store-batch-reads-20260910`, `vector-store-prepared-index-20260910`, and
`vector-store-unlocked-reads-20260910`. The unlocked-read comparison uses the
prepared-index binary as its control, with batching enabled in both arms. Core
source/recovery tests, preparation allocation-failure tests and the wider LSM
suite pass; end-to-end performance qualification is still pending. The first
unlocked comparison hit query memory admission failures in its control; its retry
hit the worker-saturation failure during candidate churn. Both failed arms are
preserved. The query-error checker now includes client framework logs, and the
stricter audit passes all eight original table-mode arms and all eight batching
arms. A positional-read diagnostic also reproduced query memory pressure during
enrichment; its reservation owner is still under investigation. The corrected
worker fallback and positional-read comparison are frozen in
`vector-store-bounded-loads-20260910`, with 1M gated on 50K lifecycle checks. Neither table
ownership defaults nor the opt-in batching default have changed.

The memory investigation also reproduced an admission defect in a focused test:
weighted reclamation could leave unused shares with empty cache owners and reject
a query while another owner retained enough idle scratch. A bounded second pass
redistributes those shares after a productive first pass. It retains the registry
identity fence, skips busy owners, and does not raise limits; all 67 resource-manager
tests pass. End-to-end confirmation is pending. `--load-only` on the qualification
harness isolates initial ingestion and readiness when diagnosing loading; it
deliberately omits the full qualification receipt and cannot satisfy a scale gate.

Status: experimental implementation, fresh table-mode comparison completed, 2026-09-10. The table setting,
reference-based source payload path, recovery/reader guards, and initial
reclamation/accounting are implemented in this worktree. Recovery qualification
passed; the 1M performance tradeoffs keep the settings opt-in.

Posting-local projection duplication is a separate policy: new posting builds
now default to no-copy (September 7, 2026). Set
`ANTFLY_EXPERIMENT_POSTING_LOCAL_PROJECTIONS=1` for the locality-on opt-in.
Existing planes remain readable without an eager rewrite. This does not change
the table-level source-ownership setting or exact-score requirements. Frozen
comparison catalogs and archive locations are in `benchmark-baselines/README.md`.

## Fresh current table-mode comparison

The [September 10 comparison](../../../.benchmark-results/vector-store-current-compare-20260910/RESULTS.md)
uses frozen commit `e4db011603e2bf701aaecba6f580d99076b4635f`, one ReleaseFast
binary and fresh standalone tables in A/B then B/A order on the available host.
Only table source ownership changes. Float32 encoding, ANN settings, durability,
batch size, concurrency and the 4 GiB process budget stay fixed; optional source
experiments are unset. The 81 storage tests, five public API cases, eight scale
arms, four source reclamation checks and four enrichment arms passed. There were
no query errors. The binary, harness, client and dataset metadata were verified
after completion. No defaults changed.

| Median paired vector-store/LSM change | 50K | 1M |
| --- | ---: | ---: |
| Initial readiness time | +6.2% | +46.4% |
| Peak query throughput | +5.0% | +8.6% |
| Mixed query throughput | -0.9% | +18.1% |
| Mixed writes/s | +2.2% | +10.7% |
| Mixed query p99 | +12.9% | -12.8% |
| Total logical disk | -49.7% | -42.6% |
| Mixed sampled physical footprint | -43.0% | -69.9% |
| Fixed-count churn logical write I/O | -24.1% | -23.6% |

Throughput varies with run order. At 1M, source readiness takes 434/463 seconds
versus 322/293 seconds for primary LSM. Fixed-count source churn takes 125/25
seconds versus 28/41 seconds for primary LSM; an average obscures this tail.
The slow restore waits 84.5 seconds for index synchronization, with 84.0 seconds
in one worker's quantization loading timer and only 38 milliseconds in its
quantization computation. The timer includes leaf-vector or internal-child reads
and their waits; it does not establish raw source I/O as the cause.

At 50K, live recall is about 98.3–98.4%, while cold restart recall is about
95.8–96.1% in both modes. Post-restart latency is therefore not consistently a
comparison at matched recall. The small enrichment workload also retains a
readiness cost: initial readiness increases about 20%, updated readiness about
10%, with 21% less disk. These results establish storage savings, not a universal
loading or latency improvement.

The [loader investigation](../../../.benchmark-results/vector-store-current-compare-20260910/SYNC_WAIT.md)
identifies a concrete fallback cost: rejection of a whole immutable generation
can send ANN leaf refreshes through primary artifact reads, individual source
locks, serialized-envelope reconstruction and subsequent decoding. The next
target is bounded direct reference batches into ANN scratch, preserving exact
version identity, transaction visibility and reader protection. Separate leaf
and internal loading timers and source lock accounting must distinguish an
actual fix from an outlier that simply did not recur.

## Sparse fallback and bounded planning follow-up

Sparse batching now applies only when no denser reclaimable segment was selected.
Empty segments do not suppress sparse progress; the existing copy-byte and row
caps remain. This avoids adding cold-segment copying to useful dense collections.

`ANTFLY_SOURCE_VECTOR_INCREMENTAL_PLANNING=1` computes segment liveness summaries
on the pinned marking cut, using the existing scan row/time budget. With unlocked
marking, the density scan runs outside the source mutex and before DB apply.
Summaries cost 24 bytes per physical segment and are included in admission.
Resurrected owners invalidate summaries after the scanner rejoins; no retirement
can use stale classifications. All-live verification skips density work unless a
post-cut preparation prevents the fast path. Final copy planning uses direct
reader indices, but copy-set construction and sorting still run under the lock.

The qualification ledger (`.benchmark-results/vector-store-planning-targets-v2/RESULTS.md`)
records eight fresh 50K arms, separately comparing the planner and sparse policy
in A/B then B/A order. Source/native tests in default and combined modes, 13 cache
tests, 10 API checks, eight reclamation checks and four independent inventories
passed. Neither source setting is promoted; this follow-up has not run at 1M.

| 50K comparison | Ready time | Mixed writes/s | Mixed p99 | Fixed churn time | Sampled physical footprint |
| --- | ---: | ---: | ---: | ---: | ---: |
| Bounded planner versus compact bitmap control | -3.9% | -0.1% | +2.9% | -3.0% | +20.5% |
| Sparse-only batches versus matched debt scheduler | -7.0% | -3.6% | +2.0% | +1.0% | +32.6% |

These are median paired observations on the available host. Planner churn
intervals reduce locked planning about 94% and total source-lock time about 34%.
Density visits contribute to marking counters in the treatment; marking plus
planning falls about 16.5%, with different layouts and copy volumes between arms.
Sparse-only fallback still allows extra copying during foreground work when only
sparse garbage exists. Separate foreground and idle copy budgets remain future
work. Copy-set construction and sorting also remain locked.

A matched-input cleanup check (`.benchmark-results/vector-store-planning-targets-v2/matched-planning/RESULTS.md`)
holds 64 MiB sparse batching constant and changes only bounded planning. Four
clones of the same saved database retain exactly 50,000 payloads and copy exactly
15,490,108 bytes in two collections. Median paired source-lock time falls 16.9%,
but marking plus planning rises 5.5%; settling remains about 8.44 seconds. This
confirms less locked work, without a cleanup-throughput win on that layout.

Nine query rejections at concurrency 30 occurred across three fresh arms: planner
pair-1 control and both sparse pair-2 arms. Successful lifecycle gates do not make
those runs error-free performance qualifications. The memory/foreground tradeoffs
and query rejections gate default promotion, combined foreground qualification,
and the next 1M run. Earlier results below describe the prior sparse policy and
remain preserved.

The first current-integration control exposed a separate cold-restart cost in
ANN metadata admission. CLOCK removal already swap-compacts its slots, but
insertion still scanned the full array for holes. That obsolete scan is removed
in the common control/candidate baseline. The sampled control and its cold
restart result remain diagnostic evidence, excluded from the fresh comparison.
A separate same-data cache replay (`.benchmark-results/vector-store-planning-targets-v2/cache-replay/RESULTS.md`)
compares the old and corrected binaries in ABBA order: mean query latency falls
about 65% with identical hits, scores and distances. Each process receives ten
warmup queries followed by 100 measured queries; this is not an OS-cache-flushed
cold benchmark. This cache gain is separate from the source-store treatments.

## Bitmap locator and sparse reclamation follow-up

Two focused opt-in changes now target the preceding experiments' costs.
`ANTFLY_SOURCE_VECTOR_BITMAP_LOCATOR=1` adds a snapshot-local locator to bitmap
marking. Its slots contain physical addresses; every probe verifies the complete
digest against pinned segment metadata. Construction yields within the existing
scan budget, and copy subsets share the completed locator and its source cut.
The existing lookup serves reads while construction is incomplete. At one million
physical rows the slot array is 16 MiB, in addition to bitmap and WAL-fallback
storage. `mark_bitmap_bytes` excludes locator bytes; total source heap includes
them, and scan counters include locator construction work.

`ANTFLY_SOURCE_VECTOR_SPARSE_GC_COPY_BYTES=67108864` allows one verified mark to
select several low-density garbage segments. It targets at most 64 MiB of selected
copy work and 65,536 additional sparse live rows, while preserving the existing
selection of denser garbage and permitting one oversized segment for progress.
Copy steps retain their independent byte budget. Planning admission failure
releases the cut for retry; old snapshots and post-cut writes retain their
existing protection. Neither setting changes durable formats or defaults.

The qualification ledger (`.benchmark-results/vector-store-marking-targets/README.md`)
tracks separate locator-versus-bitmap, compact-shape-versus-hash, and sparse-batch
comparisons. All twelve fresh 50K workload/reclamation arms and six independent
candidate inventories passed. Median paired changes are:

| 50K comparison | Ready time | Mixed writes/s | Mixed p99 | Fixed churn time | Mixed physical footprint |
| --- | ---: | ---: | ---: | ---: | ---: |
| Locator versus prior bitmap | +5.2% | -1.3% | +0.5% | +3.5% | -8.8% |
| Compact bitmap shape versus hash marking | +1.5% | -0.6% | +1.8% | +1.7% | -22.0% |
| Sparse batches versus matched debt scheduler | -5.7% | +0.5% | -0.8% | +1.0% | +30.5% |

The compact shape reduces measured marking and planning time in both hash-control
pairs, but does not yet improve end-to-end churn consistently. Sparse batching's
foreground changes are small; churn logical write I/O rises 5.5%, and sampled
physical footprint rises in both pairs (4.8% and 56.2%). The unbatched debt controls
need 407/356 seconds to reclaim after churn, versus 2.4/2.5 seconds for the batched
candidates. These runs finish with different layouts.

A separate matched-layout ABBA check (`.benchmark-results/vector-store-marking-targets/matched-sparse/RESULTS.md`)
clones the same saved sparse database for every arm and changes only the batch
target. Controls take 336/338 seconds and 56 collections; batching takes 8.8/9.8
seconds and two collections. Mark rows fall from 11.56 million to 461,014, with
exactly the same 15,490,108 payload bytes written. Post-GC serving and retained
counts pass in every arm. This isolates a substantial sparse-cleanup benefit on
that layout; it does not establish a universally optimal batch size.

The compact locator shape also passed all four 1M workload/reclamation checks
and both independent inventories. Readiness improves 15.9%/12.8%, but the mixed
and churn results reverse between pairs: writes +25.7%/-36.4%, p99 -21.8%/+95.3%,
and churn time -6.0%/+67.1%. Median paired changes are -14.3% readiness time,
-5.4% writes, +36.8% p99, +30.6% churn, +21.2% sampled physical footprint, and
+2.4% churn logical write I/O. This does not qualify a default. Both control
churn source-counter intervals are incomplete, so no paired marking-time ratio
is claimed at 1M. Candidate planning takes 2.1/6.5 seconds, including a 4.7-second
restore planning interval that writes only 309,724 payload bytes. Metadata
planning under the source lock remains a concrete profiling target; these
counters do not prove the cause of the entire latency regression.

Sparse batching also passed four separate 1M arms and both independent inventories.
Median paired changes are -2.4% readiness time, +5.7% mixed writes, -14.0% p99,
+29.8% churn time, -18.2% sampled physical footprint, and +6.8% churn logical write
I/O. Mixed writes change -13.9%/+25.3%, p99 +16.3%/-44.3%, and churn +75.7%/-16.1%
between pairs, so the medians are not a consistent foreground win. In the first
pair, batching copies 44.4 MB in six churn steps versus 6.25 MB in one control
step, with 3.6 versus 0.38 seconds of planning. The second pair's full source
counter intervals are unavailable. All four ordinary 1M idle drains settle in
15–19 seconds; the sparse-layout outlier is specifically demonstrated by the
matched 50K diagnostic.

Keep both settings opt-in. These results motivated sparse batching only
when no denser reclaimable work is selected, plus bounded metadata planning with
snapshot-safe segment liveness summaries. The follow-up implementation and its
separate qualification are recorded above. Separate foreground and idle copy
budgets remain unimplemented; combined foreground use is not yet qualified. All twenty timed 50K/1M arms, twenty
reclamation checks, ten independent inventories, and four matched-layout runs
passed; the frozen runtime and harness identities remain pinned in the ledger.

## Cost-recovery experiments

Four further opt-in treatments are implemented:
WAL-prefix/append-delta inventory updates, obsolete-debt background scheduling,
segment bitmap marking with a bounded WAL fallback, and a provisional small-table
inventory cutoff. None changes defaults. The cutoff is a candidate to measure,
not an established optimal crossover. Explicit collection, full sync, payload
ownership, old-reader leases, WAL bounds and memory admission remain intact.
See the implementation and qualification ledger (`.benchmark-results/vector-store-cost-recovery/README.md`)
for settings, safety boundaries, validation and the independent A/B matrix.

The isolated 50K ABBA runs passed all 16 workload/reclamation gates and eight
independent full-inventory audits. Against the prior eager shared-catalog,
incremental-inventory, unlocked-scan candidate, median paired changes were:

| Treatment | Ready time | Mixed writes/s | Mixed p99 | Fixed churn time |
| --- | ---: | ---: | ---: | ---: |
| Append/checkpoint delta inventory | +18.6% | -19.6% | +15.5% | +12.4% |
| Debt-based background scheduling | -21.2% | -9.8% | -3.9% | -0.9% |
| Segment bitmap marking | -8.8% | -2.6% | +3.7% | +5.2% |
| Provisional small-table cutoff | +13.6% | +0.2% | +2.0% | -1.6% |

Delta inventory eliminates eligible WAL retraversal but adds foreground map
work; inventory and preparation time increased in both pairs. Debt scheduling
avoids initial marking in both 50K candidates, but one later sparse-GC drain
took 365 seconds and 57 collections, revisiting 8.85 million mark rows. Garbage
declined throughout and the debt policy did not defer that drain: the existing
collector repeatedly chooses one sparse segment per full mark. The other debt
candidate drained in 2.54 seconds. These are unequal-layout lifecycle results,
not matched GC microbenchmarks.

Bitmap marking reduced sampled mixed physical footprint 6.5% at 50K, with the
foreground tradeoffs above. The cutoff did not establish an optimal threshold.

Debt scheduling and bitmap marking also completed separate 1M ABBA runs. All
eight workloads/reclamation checks and four independent inventories passed,
with the source limit held at 384 MiB. Median paired changes were:

| Treatment at 1M | Ready time | Mixed writes/s | Mixed p99 | Fixed churn time | Mixed physical footprint |
| --- | ---: | ---: | ---: | ---: | ---: |
| Debt-based background scheduling | -1.1% | +5.1% | -3.5% | -14.3% | +2.1% |
| Segment bitmap marking | +11.2% | -7.2% | +29.3% | +10.4% | +5.3% |

Debt scheduling improves churn in both pairs (15.6% and 13.1%), but mixed writes
and latency change direction between pairs. Initial mark rows are
control/candidate 6.03M/0.80M and 5.24M/5.50M: the large reduction in the first
pair does not repeat. All four debt runs reclaim in 17–21 seconds. The 50K
sparse-GC outlier remains relevant even though it does not recur at 1M.

Bitmap marking reduces the reachability representation to about 191–193 KiB
of sampled bitmap/offset storage, with zero WAL-fallback entries in these
ingestion samples. Other source allocations remain: maximum sampled source
heap is control/candidate 378.8/306.1 MiB and 372.1/365.4 MiB. These samples can
miss brief peaks. Mixed RSS falls 12.9% overall, while physical footprint changes
direction between pairs; the smaller marking representation is not a uniform
whole-process memory improvement. Ready time, mixed writes, p99 and churn all
regress in both pairs. The first pair spends 15.97 seconds marking 4.02M churn
rows versus the control's 2.02 seconds for 6.05M rows. The second candidate also
spends 15.10 seconds marking; its control's complete source interval is
unavailable. Compact marking needs faster location lookup before promotion.

Keep all four settings opt-in. Do not combine the foreground-cost regressions
or infer a new default from these comparisons against the previous candidate.
The locator and bounded sparse-reclamation follow-up is recorded above. Applying
queued WAL deltas at installation without eager foreground map updates remains
a separate hypothesis. The cutoff still needs a matched-dimension size sweep
before selecting a crossover; neither is an implemented or qualified new win.
Full per-arm results and limitations are in the
measurement report (`.benchmark-results/vector-store-cost-recovery/RESULTS.md`).

## Lifecycle fixes and isolated qualification

The completed qualification is recorded under
`.benchmark-results/vector-lifecycle-qualification/`. It first addresses two
failures seen in control arms, before measuring individual source-store changes.

The saved posting-WAL mismatch was an encoding error in AFQD V2, rather than
evidence of an incorrect sequence fence. Its L2 directory reader expanded an
omitted centroid-dot-product field to 75 zero floats. The next patch expected
the original 2,408-byte protobuf (CRC `8b77cabb`); directory reconstruction
produced 2,711 bytes (CRC `57cf4f95`). AFQD V3 records omission explicitly while
keeping the physical zero plane available for search. Current recovery applies
patches directly with strict base length/CRC and replacement validation.

These native formats are new in this PR, so only their latest layouts are
supported: quantized directory V3, vector block V4, vector manifest V6, posting
segment V4, posting WAL V5, and posting checkpoint V4. The obsolete readers,
conditional older-format writers, omitted-field recovery shim, and V2 fixture
have been removed. Earlier experimental stores must be recreated. Regression
coverage now writes the current directory format, reconstructs the exact L2
protobuf bytes through buffered/streamed/cold reads, applies the strict patch,
and rejects an expanded-zero base. Format tests reject obsolete and future
versions. Existing pre-PR HBC metadata/LSM migration remains supported; it is
separate from this unreleased native format history.

Generated-coverage initial builds can also outlive completion of the canonical
serving generation. These intents have no catalog-admission marker, so the
previous marker-only completion check could leave redundant shadow work queued.
The implementation now checks their durable source-replay boundary together
with current configuration, replica/root identity, replay checkpoint, complete
coverage counters, and physical artifact cardinality. It attaches the canonical
worker, discards only an inactive candidate, then removes repair debt. Continued
query access during activation separately requires a verified resident
predecessor; an unvalidated replacement cannot inherit that exception.
The frozen source passed 57 source/native recovery checks and 27 selected
lifecycle checks, including the then-present saved patch fixture, repeated full-checkpoint
tail opens, inactive-candidate retirement, invalid replacement rejection, and
rollback/restart. Two existing tests also reproduced failures before these
fixes because they inspected a certificate while its background checkpoint was
still publishing; they now await that boundary. These checks alone do not
constitute performance qualification.

Because this is a shared worktree with concurrent implementation work, this
qualification uses the source copy in `source/` and its `frozen-source.json`
manifest, a separately frozen VectorDBBench client, and dataset checksums.
The binary is built from that copy and pinned for every measurement arm.
The latest-format-only cleanup happened afterward. Those frozen inputs and
receipts remain unchanged: the timings below describe the pinned benchmark
binary, not a new measurement of the cleanup. The cleanup passed current-format checks, 56 source-payload/native-store tests,
and five checkpoint integration tests, including strict patch rejection and
repeated reopen with a concurrent WAL tail.

The measurement order is adaptive cache, append-only segments, then selective
GC with append-only mode and the same GC budget in both arms. Ownership and
snapshot reads retain individual controls. Group commit stays off. Fresh 50K
tables run sequential A/B and B/A on the available host. Receipts include
fixed-count update/delete/restore counters, preparation lock waits, directory
writes, segment counts, cache residency, and whole-process sampled logical and
physical I/O. Process I/O spans primary storage and journals as well as vector
storage; physical writeback and final sampling gaps are reported as limitations.
Only a subset that passes recovery and explains the mixed-workload tradeoff
advances to repeated 1M qualification.

All six isolated/subset 50K comparisons passed: 24 of 24 fresh arms completed
lifecycle qualification. These compare refinements on `vector_store` tables;
they are not a new `primary_lsm` versus `vector_store` table-mode comparison.
The following are median paired candidate/control ratios (smaller is better
for bytes, duration, and latency; larger for throughput):

| Treatment | Process logical writes | Fixed churn duration | Mixed writes/s | Mixed query QPS | Mixed p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Adaptive cache | 1.068 | 0.886 | 0.891 | 1.115 | 0.994 |
| Append-only segments | 0.865 | 0.987 | 0.983 | 1.034 | 1.001 |
| Selective GC, matched append-only control | 0.716 | 1.058 | 1.036 | 0.976 | 1.035 |
| Ownership indexing | 0.941 | 0.963 | 0.959 | 1.029 | 0.892 |
| Snapshot reads | 0.995 | 1.111 | 0.897 | 1.070 | 1.011 |
| Append-only plus selective GC, matched GC budget | 0.550 | 0.836 | 1.158 | 0.953 | 0.809 |

The selected 1M subset is **append-only segments plus selective GC**, with an
8 MiB GC step budget in both modes and enrichment batch size 64. All other
experimental flags remain off. Its two paired process-write ratios are
0.584/0.515; mixed writes/s 1.123/1.192; mixed p99 0.869/0.750; and fixed churn
0.713/0.960. Both pairs therefore retain reduced rewriting without the earlier
mixed write/churn slowdown. Mixed QPS is about 5% lower, source disk grows
0.78%, and segment count rises from 128 to 649–650. Peak physical footprint
falls in both pairs (0.756/0.904), while mixed RSS rises (1.431/1.074), so memory
remains an explicit scale-up question. All arms reclaim to exactly 50,000
payloads after restart; both candidates have already done so before restart.

Ownership indexing cuts marking from about 1.5 s to 0.33 s, but its write and
peak-memory results disagree between pairs. It remains separately measurable
and has not been added to the selected subset without a combination test.
Adaptive cache does not reduce final cache residency at 50K and worsens the
query-concurrency curve; snapshot reads reduce source lock wait but lose write
throughput in both pairs. Neither joins the bulk-serving candidate. Medians in
the table do not establish repeatability where individual pairs disagree.

The selected subset completed **four 1M arms in A/B and B/A order**, all passing
lifecycle gates with live/cold/warm recall 0.9899–0.9903. It is recovery-qualified,
but **not promoted as a balanced 1M performance default**:

| 1M candidate/control ratio | A/B | B/A |
| --- | ---: | ---: |
| Ready time | 0.753 | 0.901 |
| Whole-process logical writes | 0.666 | 0.660 |
| Concurrency-30 QPS | 1.453 | 1.156 |
| Concurrency-30 p99 | 0.602 | 0.834 |
| Mixed queries/s | 1.364 | 0.846 |
| Mixed writes/s | 1.294 | 0.811 |
| Mixed query p99 | 0.630 | 1.004 |
| Fixed 10K churn duration | 2.433 | 1.733 |
| Peak physical footprint | 1.052 | 1.007 |
| Mixed RSS | 1.315 | 0.796 |
| Final total disk | 1.016 | 1.042 |

Controls wrote 30.63/30.81 GB of sampled logical process I/O, versus
20.40/20.32 GB for candidates. Readiness and concurrency-30 queries improve in
both pairs, but mixed throughput changes direction and changed-vector churn
regresses. The churn timer includes the final index-sync fence and status
observation. Candidate churn took 121.69/96.02 s versus 50.01/55.42 s in controls.
The first candidate spent 52.7 s marking and wrote 352.3 MB of directories while
copying only 6.7 MB during GC. Faster copy completion permits more full marks
and directory publications; those costs now need to be bounded or coalesced.
Ownership indexing is promising for marking, but its combination has not been
qualified and is not silently enabled.

Both controls also stalled in the first single-client query window (p99
21.1/29.0 s versus candidate 13.12/61.38 ms). This is a repeatable initial-window
observation, not a general 100× query-throughput claim. The second candidate
had a long final publication interval before readiness, with transient zero
published counts while shared-vector projection maintenance completed. It
subsequently became fully ready without repair debt. All original timings and
receipts are retained, including these delays.

The pinned runs are under
`.benchmark-results/vector-lifecycle-qualification/qualified-1m/`. Cold restart
denotes a fresh server process, not an OS page-cache purge. Source preparation
lock wait does not include all outer DB serialization. One candidate's final
before-restart source counters and one control's after-restart counters were
unavailable; null counters and invalid per-phase deltas are excluded from
aggregation, while process I/O and request timing remain available.
All four supplemental reclamation checks passed on cloned databases: exactly
1,000,000 retained payloads / 3,072,000,000 payload bytes, zero pending collection,
zero unreferenced bytes, and successful serving plus complete publication after
GC. Controls settled in 107.08/117.55 s and candidates in 34.15/35.68 s including
process startup and validation. These observations prove eventual cleanup;
they do not replace the timed end snapshots. The final receipts are under
`reclamation-1m-final/` within the qualification root. Full per-pair evidence and accounting limits
are in `.benchmark-results/vector-lifecycle-qualification/RESULTS.md`.

Source counters in a best-effort status response can come from a read-only
fallback with zero volatile activity counters. The completed measurements
verify prepared-payload counts and bounded fixed-churn counter deltas; process
I/O sampling supplies an independent total across server lifetimes. A transient
zero status counter must not be interpreted as zero write cost.

## Decision and next work after 1M qualification

The experiments support keeping table-owned source payloads, shared ANN payload
ownership, and the lifecycle fixes. The append-only/selective-GC combination
has a repeatable write-I/O benefit, but its 1M churn regression prevents promoting
it as the balanced default. It remains an opt-in experiment. Adaptive cache and
snapshot reads stay out of the candidate; group commit stays off until payload
preparations can overlap outside the outer DB lock.

Proceed with a narrower GC experiment:

1. Bound marking work across collection steps and test the existing ownership
   index in combination. Its isolated 50K marking reduction is promising, but
   does not establish an end-to-end or 1M win. Preserve primary commit ownership,
   stale-completion checks, tombstones, and leases held by old readers.
2. Coalesce directory publication or make it incremental so a small collection
   step does not repeatedly rewrite the whole directory. Preserve durable
   publication ordering and crash reconstruction before retiring segments.
3. Measure the same fixed-count update/delete/restore workload first at 50K,
   then 1M. Attribute mutation time, final index-sync time, status-read time,
   outer DB lock wait, marking, and directory I/O separately. Match GC budgets
   and start from fresh tables on the available host.
4. Repeat A/B and B/A for the resulting subset. Promote only after the churn
   regression is removed while retaining the write reduction and passing
   readiness, query-tail, recall, memory, restart, and reclamation gates.

The implementation of this follow-up is now under qualification in
`.benchmark-results/vector-bounded-gc/`. `ANTFLY_SOURCE_VECTOR_MARK_STEP_ROWS`
limits primary ownership, ANN ownership, and exact inventory verification to
16,384 rows per maintenance turn in the candidate. A retained primary snapshot
and ANN generation define the mark; preparations after the cut are protected
in the WAL suffix, including retries of previously orphaned payloads. The scan
can also advance under writer WAL pressure. Shutdown cancels its primary read
transaction before destroying the backend. A retry seen first in the WAL tail
and later by the mark is counted once. Segment selection, sorting, copy,
and publication remain separate work; the row limit is not a wall-clock limit
on the whole collector. Marking and planning have distinct counters.

`ANTFLY_SOURCE_VECTOR_COALESCE_DIRECTORY=1` updates location hints only for
retired/replacement segments and persists a directory snapshot after at least
max(4,096, current entries / 4) changed hints. Explicit source checkpoints use
the same coalescing rule. CURRENT and immutable segments remain authoritative;
missing/stale hints fall back to them, and old readers validate hints against
their own manifests. Recovery ordering and retirement fences are unchanged.

The fixed-count churn harness retains its final full-index mutation batch.
Existing server batch profiles separate sync wait from client mutation/request
time; status reads are timed independently. Profile row counts and final-fence
counts must match the exact workload. An empty public batch is not used as a
fence because it can return without routing to a shard. Source status adds DB batch lock
wait, marking step count/maximum rows/maximum duration, planning time, and
directory publication/deferral counters. Whole-process I/O remains the total
write-accounting cross-check.

Initial qualification stopped on the reversed candidate: replay caught up, but
active-count metadata was 50,685 for exactly 50,000 unique posting members.
The saved index has no duplicate members or incorrect leaf mappings. A focused
regression reproduced a source-capture cache gap: local HBC transactions finish
before the outer source transaction publishes its immutable serving generation.
Search cache borrowing and admission must remain isolated for that whole interval,
including durable posting publication and rollback. The correction has local/shared
cache and delayed-reader coverage, and is pinned separately for fresh qualification.
The original failed arm remains preserved; a passing focused test does not qualify
the larger workload by itself.

Selective collection must make progress even when every nonempty segment is
below the garbage-density threshold. Empty segments may be retired with a pass,
but their presence must not suppress selecting the best segment containing real
garbage. A saved 50K reclamation failure exposed that starvation case. The fix
has a regression with an empty segment, a one-eighth-obsolete segment, bounded
marking, and an old reader lease. Partial-GC live/orphan counters use the entire
mark, including untouched cold segments; copied bytes are a separate quantity.
`revisions/selective-progress/` pins these corrections for fresh qualification.

The cache-corrected 50K comparisons covered the combined bounded GC/directory/
ownership candidate and isolated ownership with bounded GC/directory in both
arms. Ownership alone added no clear fixed-churn benefit. After the selective-GC
correction, a fresh combined 50K ABBA and all four matching reclamation checks
passed. Fixed-count churn improved 25.8% and mixed write throughput 30.2% by
median paired change; churn-envelope process logical writes increased 11.8%
and mixed query throughput fell 8.6%. Directory bytes written fell 72.8%.
The maximum mark step was 16,384 rows (18–29 ms), versus roughly 800K rows
(264–392 ms) in controls; this does not bound planning/publication. Only the
first pair has valid paired source-counter envelopes, so repeated write-cost
comparisons use process-sampled I/O.

Fresh 1M ABBA subsequently passed all four workload/lifecycle arms with the same
corrected binary, gated by matching 50K lifecycle and reclamation passes. Both
scales use fresh sequential tables, matched 8 MiB copy budgets, float32 source
payloads, and the same ANN settings. Adaptive cache, snapshot reads, and group
commit stay off. All four exact 1M reclamation checks also passed on clones: 35.3, 51.6,
47.8, and 33.4 seconds in recorded arm order. Each reached exactly one million
live payloads (3,072,000,000 raw bytes), zero orphan/pending bytes, and healthy
queries before and after GC.

At 1M, fixed-count churn fell from 81.2 to 62.8 seconds in the first pair and
420.9 to 46.6 seconds in the reversed pair. Churn-envelope process logical writes
fell 51–57% in both pairs. This addresses a substantial part of the maintenance
cost, but mixed write throughput fell 23–69%, mixed query throughput regressed,
and initial ready time worsened in both pairs. Cold and warm restart tails varied
substantially between pairs. Keep the options experimental: the subset does not
yet retain mixed-workload performance. Shared-host contention was observed; the
repeated measurements remain useful but do not establish exact causal percentages
for every latency or footprint change.

One candidate mark step still took 1.13 seconds at the 16,384-row limit (the other
arm's maximum was 34.1 ms). The next implementation should add an elapsed-time
budget and scan immutable snapshots outside the foreground source-writer lock,
retaining primary/ANN/source generation leases and merging the protected tail
under a brief lock. Planning needs its own bound. Then reduce conservative
post-cut reappends by protecting an already-durable payload without copying it
merely because marking has not visited it yet. Retry, abandoned collection,
old-reader, and restart tests must continue to prove those fences. These are
next steps, not implemented changes in this qualified binary.

Exact live-payload reclamation does not imply physical single copy: conservative
post-cut preparation can reappend an already-live digest before marking reaches
it. Whole-process disk/WAL accounting retains that cost. Both 1M controls lack
valid complete source-counter envelopes; use process-sampled I/O for repeated
write comparisons and label partial source-counter attribution separately.
See `.benchmark-results/vector-bounded-gc/revisions/selective-progress/RESULTS.md`
for per-pair results, preserved failures, and pinned source/harness receipts.

## Foreground marking follow-up

The elapsed-time/unlocked-scan follow-up is implemented under
`.benchmark-results/vector-foreground-gc/`. Its pinned build passed five API
lifecycle checks and all four fresh combined 50K A/B and B/A arms.
`ANTFLY_SOURCE_VECTOR_MARK_STEP_US` adds a cooperative elapsed budget to the row
cap; the proposed candidate uses 2,000 microseconds and 16,384 rows, whichever
is reached first. One backend operation or scheduling stall can exceed the
budget, but no subsequent row starts after it has expired. It is not a hard
real-time guarantee.

`ANTFLY_SOURCE_VECTOR_MARK_OUTSIDE_LOCK=1` scans retained primary, ANN, and source
snapshots outside both the source writer lock and DB apply lock. The scanner owns
its live map, cursor, and captured ANN scopes. Writers maintain a separate
protected WAL tail and record concurrent preparations; scan discoveries and those
preparations are reconciled under the source lock once per turn. A retry of a
version already scanned is counted once. No other collector or checkpoint can
advance or retire an in-flight scan. Cancellation waits for the scanner before
releasing snapshots, and ambiguous primary outcomes still prevent publication.

Setup, planning, copying, and durable publication retain their existing locks and
fences. This change does not yet bound planning or remove conservative payload
reappends. New counters separate outside-lock scan time, merge time/maximum,
elapsed-budget yields, and concurrent-scan deferrals from existing marking and
planning totals. The budget and unlocked mode can be measured independently as
`mark_time` and `mark_unlocked`; `foreground_gc` combines them against the previous
bounded-GC/coalescing/ownership baseline.

The source recovery suite now has 27 passing tests, including paused scans with
concurrent writes/deletes/retries, cancellation, ambiguous outcomes, poison
fencing, lagging ANN generations, old readers, restart, and elapsed-budget exact
verification. The 32 native-store tests also pass on the pinned source.

Both combined and unlocked-only 50K A/B and B/A passed all four timed arms and
all four exact-reclamation clones. Neither treatment is a performance promotion:

| Median paired change | 2 ms + unlocked | Unlocked only |
|---|---:|---:|
| Initial readiness time | +29.6% | +60.9% |
| Fixed-count churn time | -0.6% | -8.6% |
| Mixed writes/sec | -11.7% | +38.7% |
| Mixed queries/sec | -2.0% | -23.8% |
| Mixed p99 | +15.3% | +9.8% |
| Churn logical write bytes | +13.2% | +10.1% |

Combined DB batch lock wait fell 97.5%, but overall performance did not improve.
Unlocked-only also increased mixed RSS 53.2%, despite essentially unchanged final
disk use. One control's source-counter snapshot was missing; complete paired
source-counter comparisons for unlocked-only are invalid, while workload timings
and process I/O remain available. All eight GC clones reached exactly 50,000 unique
payloads, zero pending/orphan bytes, and healthy complete ANN serving. Candidate
reclamation ranged from 6 to 89 seconds, so passing inventory checks does not imply
uniform reclamation latency.

No new 1M run was started: neither 50K subset meets the intended performance gate.
The options remain experimental. The next boundary is worker scheduling: an active
scan currently pays a fixed 100 ms sleep between passes. Separate progress from
scan quantum with cooperative yielding and explicit background CPU budgeting,
then measure it independently. Remaining locked setup/planning/copy/publication
and conservative reappends during scanning also need isolated profiling. Any
concurrent deduplication optimization must retain the referenced source locations
through GC publication and preserve prepare-before-primary durability; an
unprotected membership lookup is insufficient. Exact receipts and per-arm numbers
are in `.benchmark-results/vector-foreground-gc/RESULTS.md`.

## Active scan progress and protected reuse experiment

The [2026-09-08 correctness and design review](../../../zig/VECTOR_STORE_REVIEW.md) records
the remaining audit boundaries and larger opportunities after these measurements.

The three structural follow-ups are now implemented under
`.benchmark-results/vector-structural/`: `ANTFLY_SOURCE_VECTOR_INDEPENDENT_SCAN=1`
separates incomplete scan turns from DB metadata maintenance,
`ANTFLY_SOURCE_VECTOR_SHARED_CATALOG=1` shares immutable segment and manifest
arrays across source leases/WAL successors, and
`ANTFLY_SOURCE_VECTOR_INCREMENTAL_INVENTORY=1` maintains rebuildable physical
occurrence counts across changed segments and WAL membership. They remain
independent experimental controls. Inventory counts handle duplicate physical
locations and never replace primary/ANN ownership authority; they add resident
metadata. All 35 source tests pass with switches off and with all three plus
ownership indexing enabled; 32 native tests and five public API lifecycle checks
pass on the pinned release. All 12 fresh 50K timed arms and 12 reclamation clones
pass. Two inventory candidates also reopen with incremental inventory and
checkpoint inventory restoration disabled: full scans independently recover
50,000 unique payloads / 307,200,000 raw bytes before collection publication.

The independent A/B and B/A comparisons do not make all three clear wins:

| Treatment | Median paired result | Decision |
|---|---|---|
| Independent scan | Readiness +8.1%, mixed queries +0.1%, mixed writes -1.9% | Keep experimental; avoided apply visits did not produce an overall improvement. |
| Shared catalog | C1 queries +20.4%, mixed writes +4.1%, churn time -7.8%, mixed RSS +22.2% | Strongest next candidate; attribute the repeatable RSS increase before promotion. |
| Incremental inventory | Publication-stage time -51.9%, mixed writes +6.7%, mixed queries -3.5% | Keep experimental; fewer segment visits cost more total inventory bookkeeping. |

Shared-catalog readiness is outlier-sensitive. Inventory readiness and RSS change
direction across pairs; its roughly 93% reduction in segment rows excludes WAL
traversal and map work. Inventory candidate reclamation settles in 2.93/2.90 s
versus 7.19/9.53 s for controls, with different starting layouts/debt. These are
lifecycle results, not identical-input GC microbenchmarks. No defaults change.
Profile shared-catalog residency and inventory WAL/map costs, then qualify a
selected subset against the stronger locked row-bounded baseline before 1M.
See the complete measurement record (`.benchmark-results/vector-structural/RESULTS.md`).

The memory investigation is recorded under
`.benchmark-results/vector-structural-refined/`. Identical-data memory-map
diagnostics did not reproduce a retained-catalog heap or mapping increase, so
shared-catalog ownership stays unchanged. Original phase-matched process samples
show mixed physical footprint +6.6% versus RSS +22.2%; fresh qualification must
continue reporting both. A separate WAL-membership prototype passed four 50K
arms, four reclamation checks, and two independent inventory audits, but did not
establish an overall performance win. That prototype remains in its frozen
experiment snapshot and is excluded from the current implementation.


The subsequent source publication memory fix (`.benchmark-results/vector-source-memory-fix/README.md`)
reproduces the suspected OOM as a source-budget rejection of a second WAL-sized
allocation during GC preparation. Full and selective GC now prepare readers and
inventory before publication, reuse the committed WAL suffix, and preserve the
current generation on pre-publication failure. Scratch admission and a
budget-derived WAL bound protect foreground progress; ambiguous durable writes
still require recovery. The controlled memory test now succeeds, and validation
covers allocation failures, old readers, retry, update/delete and repeated restart.
That revision passes fresh 50K ABBA and its lifecycle checks, but the second
1M candidate logs one maintenance OOM and is excluded. A same-binary diagnostic
identifies a separate 77,594,648-byte mark-map allocation with 332,745,026 live
bytes against the 402,653,184-byte source slice. The
mark-workspace follow-up (`.benchmark-results/vector-source-memory-admission/README.md`)
admits the map and source leases against the resident ANN snapshot before map
allocation. Rejected setup releases its temporary snapshots and retries later;
backing allocation and I/O failures still propagate. Its regression verifies
repeated deferral without retained memory, reclamation after pressure clears,
old readers and repeated reopen. Validation passes 42 source checks in each
configuration, 33 native checks, 256 publication allocation-failure positions,
and both five-case public API suites, with no test leaks. Fresh sequential 50K
and 1M ABBA passes all eight workloads and reclamation checks and all four
independent candidate inventories. No OOM/poison/retry error invalidates an arm.
The memory limits are unchanged; earlier failed arms and diagnostic timings
remain excluded.

The optional catalog/eager-inventory/unlocked-scan combination remains a
performance tradeoff against the fixed locked baseline. At 1M, median paired
mixed queries improve 9.5%, writes 7.4%, p99 14.5%, and C1 queries 7.8%; those
directions repeat in both pairs. Readiness is slower in both pairs (+9.8% paired median).
Mixed RSS falls 3.4%, while physical footprint varies by pair (+1.5% median).
At 50K, mixed writes fall 11.2% and p99 rises 13.2%, so no defaults are promoted.
Candidate maximum sampled source heap still reaches 372–374 MiB of the 384 MiB
slice; the fix provides admission and progress, not elimination of resident
memory demand. Maximum sampled source WAL is 48.05 MiB in all four 1M arms.
These periodic samples are not exact peaks. The second 1M control lacks a
complete churn-stage source-counter interval; its lock/inventory breakdown is
unavailable, not zero. See the
qualified results (`.benchmark-results/vector-source-memory-admission/RESULTS.md`).
Both arms contain the fixes: this is not a pre-fix/post-fix timing comparison,
a comparison against `primary_lsm`, or qualification of other deployment modes.
The frozen source/harness/binary hashes verify. Qualification excludes unrelated
concurrent changes in the live worktree; the final repository-wide and task-file
diff checks pass.

The completed isolated deferred-inventory 50K ABBA passes all four timed and
reclamation arms and both independent full inventories, but it is not selected:
median paired mixed writes fall 25.6%, churn time rises 12.9%, and mixed physical
footprint rises 15.8% despite RSS falling 13.7%. The completed comparison under
`.benchmark-results/vector-structural-selected/` combines shared catalogs with
**eager** incremental inventory against the earlier locked baseline; lazy
inventory is disabled. It uses the same recovery-qualified binary and a separate
frozen harness, preserving prior measurements. Combined 50K passes all four
timed/reclamation arms and both independent inventory audits. Median paired mixed
writes improve 3.3%, churn time 2.1%, and mixed p99 4.0%; query and
physical-footprint directions vary by pair.

All four 1M attempts finished lifecycle checks, including both independent
candidate inventories, but **1M performance qualification is incomplete**.
The last control encountered three out-of-memory errors and source-store
poisoning during ingestion; the client retried four HTTP 500s and exited
successfully. That control is excluded, leaving three clean arms and only one
complete clean pair. In that pair mixed queries improve 7.5%, writes 7.4%, p99
13.6%, and fixed churn time 5.4%; mixed RSS rises 27.0% and physical footprint
6.4%. These are single-pair observations, not a repeated scale win or evidence
for promotion.

The benchmark now rejects write errors/retries even when the client exits zero,
and the 1M gate rechecks historical 50K logs. Four regression tests pass; an audit
of all twelve timed arms in this revision finds only the last 1M control affected.
Its failed-run data and original receipts are preserved. Source heap rose during
collection before the errors, but the failing allocation is not identified.
Next, reproduce memory admission/collection scratch pressure with allocation
evidence, fix it while preserving publication and recovery fences, then rerun
fresh 1M ABBA. No defaults change. See the
full results (`.benchmark-results/vector-structural-selected/RESULTS.md`) and
memory-failure evidence (`.benchmark-results/vector-structural-selected/MEMORY_FAILURE.md`).

The current refinement is under `.benchmark-results/vector-structural-recovery/`.
`ANTFLY_SOURCE_VECTOR_LAZY_INVENTORY=1` lets an incremental-inventory store use
the totals from a validated checkpoint receipt without eagerly constructing its
occurrence map. Segment installation constructs the map before using it for
physical accounting. Missing, corrupt, or stale receipts retain full
reconstruction; preparation counters, ownership authority, payload validation,
and publication failure fences remain unchanged. All 37 source checks pass with
defaults and with shared catalogs, incremental/lazy inventory, ownership indexing,
and checkpoint receipts enabled. Investigation of the API timeout found that
both default and explicit settings already enable native ANN storage. A simulated
low-space test reproduced capacity-deferred backfill and exposed competing
in-place startup reconstruction, which failed by publishing a chunk at sequence
zero. Durable generation-repair ownership now excludes that reconstruction;
independent rebuild chunks retain their existing capture coverage. Five focused
checks pass, including restart and both model generations. The saved failed
database, capacity simulation, source patch and qualification receipts are in
the recovery record (`.benchmark-results/vector-structural-recovery/README.md`).
The pinned release passes both five-case public API suites, saved-database
recovery with update/delete and two further restarts, and both controlled
capacity/restart/resume cases without another write. The isolated and combined
50K comparisons and incomplete 1M qualification are recorded above. Per-arm disk checks include the native safety
reserve; host quietness is not required. No defaults change solely because these
switches are implemented.

The implementation and measurements are under `.benchmark-results/vector-progress-dedup/`.
`ANTFLY_SOURCE_VECTOR_SCAN_DUTY_PERCENT=50` replaces the fixed active-scan pause
with a pause equal to the preceding scan's wall duration (minimum 100 microseconds).
This policy applies only to unlocked marking; idle maintenance and planned copying
retain their existing intervals. It cooperatively limits wall-time duty rather
than promising a hard CPU quota or real-time deadline.

`ANTFLY_SOURCE_VECTOR_RESCUE_REAPPENDS=1` lets preparation protect an existing
durable payload in the current mark. The writer records protection separately;
the scanner rejoins before those entries are added to its live set and before any
copy plan is finalized. Verification iterators are invalidated before that map
can grow. This permits reuse through GC publication without another payload WAL
append. Once copying is planned, the normal append fallback remains. Cancellation
retains the original durable authority, and ambiguous primary outcomes still
fence publication. A mark containing newly protected payloads absent from the cut's live set cannot
certify its old primary epoch as fully live: some preparations may be abandoned,
so a later mark must revisit reachability. Already-live retries preserve both
verification progress and the original checkpoint proof.

New cumulative and maximum timings distinguish source lock holds, setup, planning,
copying, and publication; setup includes checkpoints, while publication includes
directory refresh, inventory, receipts, and reclamation. Avoided payload/byte and
requested-duty-pause counters expose the cost tradeoff. Nested stage totals must
not be added to the overall lock-hold total.

Scheduling, protected reuse, and their combination were measured independently
against the same 2 ms unlocked-marking baseline. All 12 timed 50K arms and all 12
exact-reclamation clones passed. The measured release passed 61 distinct storage
checks and five API lifecycle tests. Final review added a planning-allocation
failure cleanup guard and retry/reopen regression; the final source suite passes
30 checks with ownership indexing off and on, alongside the earlier 32 native
checks. The guard's separate post-measurement receipt preserves the measured binary.

Protected reuse alone reduced churn logical write I/O by 21.6%, but increased
mixed RSS by 30.9% and reduced C1 query throughput by 20.2%. The combination
completed post-workload reclamation in 8.18/8.20 seconds versus 271.37/69.77 seconds
for its controls, while readiness was 14.3% slower and C1 throughput 25.8% lower
(median paired ratios). Physical layouts and starting reclamation debt differ;
these are lifecycle comparisons rather than identical-state GC microbenchmarks.
Remaining source lock work is dominated by setup and publication, warranting
finer checkpoint/directory/inventory profiling. No performance winner is promoted;
the stronger row-bounded baseline comparison and new 1M runs remain gated.
Per-arm measurements and limitations are in
`.benchmark-results/vector-progress-dedup/RESULTS.md`. All settings remain experimental.

## Second experiment round: six independent controls

The six follow-up experiments are implemented behind process environment
controls. They are **not new production defaults**. The shared table setting,
reference identity, exact float32 reconstruction, and prepare-before-primary
commit protocol remain the same. Results for this round live under
`.benchmark-results/vector-next-experiments/` in this worktree.

| Experiment | Candidate control | What is being measured |
| --- | --- | --- |
| Append-only payload segments | `ANTFLY_SOURCE_VECTOR_APPEND_ONLY=1` | Seal source WALs without ordinary payload merging; use a separate digest-to-generation/shard directory. |
| Selective segment collection | `ANTFLY_SOURCE_VECTOR_SELECTIVE_GC=1` | Rewrite garbage-bearing segments while retaining cold segments byte-for-byte. Requires append-only mode. |
| Committed ownership replay | `ANTFLY_SOURCE_VECTOR_OWNERSHIP_INDEX=1` | Maintain a compact owner index in primary transactions and replay it through the existing primary WAL. |
| Shorter preparation exclusion and group commit | `ANTFLY_SOURCE_VECTOR_GROUP_COMMIT=1` | Decode outside the source mutex and combine waiting preparations into one durable append. |
| Artifact reads through retained snapshots | `ANTFLY_SOURCE_VECTOR_SNAPSHOT_READS=1` | Acquire a source lease after selecting a primary reference; fetch and reconstruct outside the writer mutex. |
| Adaptive location cache | `ANTFLY_SOURCE_VECTOR_ADAPTIVE_CACHE=1` | Allocate cache stripes on demand, admit repeated accesses, and release arrays through the resource governor. Requires nonzero location-cache entries. |

`SOURCE_DIRECTORY` is a checksummed, disposable metadata checkpoint containing
44-byte digest/location records, independent of vector payload files. Its
current implementation retains a hash directory in source-accounted memory,
updates it from newly sealed block metadata, and rewrites only directory bytes
at stable checkpoints. It is not a memory-free directory or a disk-resident LSM
index. Every hint must identify a segment retained by the requesting snapshot;
the reader checks the full key. Missing, corrupted, stale, or contended hints
fall back to normal lookup. Directory bytes, lookup counts, and metadata write
cost are exposed separately. Append-only chains use manifest V6 above the old
64-generation admission bound; older binaries reject the new version. The
experimental chain still has a 4096-generation safety bound, at which native
merging applies.

Selective collection first seals the source WAL and marks durable owners. It
selects segments with at least 25% dead payload bytes, or the best remaining
segment when none reaches that threshold, and copies their live contents using
the existing incremental byte budget. Other segments remain in CURRENT.
Post-cut preparations stay in the preserved WAL suffix; old query leases retain
removed files. This separates old stable runs from new update runs by age and
reclaimability; it does **not** yet predict document update frequency or maintain
separate hot/cold admission buffers. Existing physical-base layouts use full-base
collection because partial base deletion would violate the manifest contract.

The ownership experiment deliberately reuses primary transaction replay rather
than creating another independently retained event log. Successful artifact
writes and deletes update an owner-key digest to a 36-byte payload-digest/dimensions
record in the same transaction. A failed ownership mutation fences that
transaction's commit. An ownership epoch must match the primary reference epoch
before collection can scan the narrow owner prefix instead of the entire
primary store. Missing or incomplete coverage falls back to the full scan,
including after a disabled interval; fresh tables provide the intended A/B
configuration. This removes unrelated primary rows from marking, but still
visits all current ownership entries. Its WAL and compaction costs remain in
primary storage accounting. Durable catalog artifact scopes include quarantined
indexes and let GC ignore lagging serving references belonging only to dropped
scopes. Scope retirement is a shared correctness rule in every arm, including
when the optional ownership index is disabled after an earlier collection. Primary source references survive dropping the last ANN index.

Group commit adds no batching timer and does not acknowledge preparation before
fsync. Requests already waiting behind an append can share the next append,
bounded by 32 requests and 256 items (an individual larger request remains
indivisible). Primary transactions continue to commit independently afterward.
The current outer DB writer serialization may limit actual grouping; compare
`prepare_requests` with `prepare_batches`, rather than assuming a source-only
concurrency test predicts enrichment throughput. Independent decoding uses a
separate resource reservation. WAL encoding and publication still require
source writer exclusion.

Snapshot reads acquire a lease per payload after selecting its primary value.
Lease metadata uses a transient allocator instead of the caller's transaction
arena; shared source allocations serialize accounting so the last reader may
release a WAL node concurrently with successor preparation.
They do not pin a source view before the primary snapshot, which could miss a
concurrent committed reference. The experiment trades a shorter writer hold
for cloning snapshot metadata on each read; it does not yet amortize that cost
across a batch or add a caller-owned artifact reconstruction API. The adaptive
cache uses separate reservations and thread-safe backing allocation, bypasses
contended stripes, and treats eviction as a normal lookup miss.

The comparison definitions are centralized in
`scripts/vector_store_experiment_settings.py`. All next-round arms hold the
64-item enrichment batch cap constant. Selective-GC controls also enable
append-only storage and incremental GC; ownership controls enable incremental
GC; adaptive-cache controls allocate the same maximum number of entries eagerly.
This avoids attributing required dependencies to the experiment itself. The
`next_combined` arm tests their interaction. Each comparison uses fresh tables,
A/B then B/A ordering on the available host, pinned binaries and frozen scripts.
A quiet host is not required. Query failures disqualify a comparison and retain
admission-state diagnostics; independent experiments can still finish.

Validation so far: 24 source-store tests and 31 native segment tests passed with all six controls enabled,
including over-64-generation reopen, stale directory hints, selective collection
with old leases and post-cut updates/deletes, group-commit acknowledgement,
cache reclamation, and existing recovery tests. Seven DB tests passed on the revised allocator/scope implementation, including
stale completion fencing and dropping/rebuilding the last index. An existing
managed-admission test needed to await asynchronous durable checkpoint
publication rather than assume that starting finalization completed it. The
final executable also passed five public API tests covering multiple models,
updates/deletes, restart, and last-index drop/rebuild.

### Measured second-round results

All 28 small-workload arms completed: six individual experiments plus their
combination, with two alternating pairs each. Each arm used 4,000 documents,
128-dimensional deterministic enrichment, an updated embedding version,
full-text/semantic queries, and restart. Six comparisons passed their query
availability gates. The group-commit comparison failed because its second
**control** arm returned one `index_rebuilding` 503 after reporting readiness.
This is an unresolved generation-recovery lifecycle issue, also reproduced in
a preliminary append-only arm; the failed measurements are retained.

| Experiment | Observation | Decision |
| --- | --- | --- |
| Payload segments | Median paired initial/update readiness ratios 0.77/0.90; semantic throughput about unchanged; RSS ratio 1.06. | Promising, but checkpoint timing and individual readiness pairs vary. Keep experimental. |
| Selective GC | GC copied 242 KB and 100 bytes versus 2.54/5.08 MB in controls; checkpoint/directory writes offset some savings; update readiness ratio 1.27. | Measure total write amplification and retained segment count, not GC bytes alone. |
| Ownership index | Marking took 24/18 ms versus 41/25 ms; update readiness ratio 1.26. | Narrower marking works, but transactional maintenance has a cost. |
| Group commit | Both candidate arms had 979 requests and 979 durable batches. | Outer DB serialization prevented grouping; availability comparison also failed. |
| Snapshot reads | Retained reads worked; query QPS ratios were 5.97 and 0.98 in the two pairs. | The large first-pair result is not a repeatable speedup. Batch leases before claiming a performance benefit. |
| Adaptive cache | 9,312 bytes versus 8,922,208 eager bytes with zero isolated-arm cache hits; combined arms grew and served 68K/75K hits. | Demand-based allocation works. Keep pressure/reclamation tests and account for peak growth. |
| All six | Four small arms passed; update readiness ratio 1.13, semantic QPS ratio 0.89, RSS ratio 0.96. | Do not enable the combination by default. |

Ratios are candidate/control; lower readiness and memory ratios are favorable.
These are small shared-host measurements with only two pairs, not production
qualification. Detailed per-arm timings, counters, failed-query diagnostics,
and executable/script hashes are retained in
`RESULTS.md` (`.benchmark-results/vector-next-experiments/RESULTS.md`) and
`small/summary.json` (`.benchmark-results/vector-next-experiments/small/summary.json`).
The combined 50K diagnostic is complete under `scale/`; 1M qualification
remains gated on recovery/availability failures and the observed regressions.

### 50K combined diagnostic: restart gate failed

Both candidates and the first control completed successfully. The final
control failed cold-restart validation: the auxiliary enrichment table's
`semantic` ANN index reported `PostingPatchBaseMismatch` while replaying a
quantized posting patch, required rebuild, and entered quarantine. The main
50K vector index still answered its cold queries, but the harness rejected the
run because the other table had a native lifecycle failure. This is separate
from the small readiness 503. No 1M run followed this failed gate.

Only the first pair supplies a complete comparison:

| 50K / 1536D / float32 | Control | All-six candidate |
| --- | ---: | ---: |
| Initial readiness | 21.38 s | 22.20 s |
| Peak query throughput | 2,213/s | 2,352/s |
| Mixed query throughput | 169.8/s | 127.1/s |
| Mixed write throughput | 1,203 rows/s | 822 rows/s |
| Fixed churn, 10,000 row operations | 13.97 s | 17.58 s |
| Read-only peak RSS | 1.623 GB | 1.487 GB |
| Mixed peak RSS | 1.457 GB | 1.543 GB |
| Reported source writes before restart | 963.5 MB | 716.2 MB |
| Total disk after restart | 464.6 MB | 442.0 MB |
| Source-store disk including directory | 315.8 MB | 318.2 MB |

The write-byte sum includes source WAL, checkpoint, GC output and directory
counters, but excludes primary ownership/journal writes and subsequent restart
work. Total disk includes both stores and journals. Mixed phases completed
different write counts, and much of the disk difference was ANN state rather
than source payload storage. The successful second candidate ran faster than
the first during mixed traffic; its failed control prevents treating that as a
second qualified pair. Both successful candidates retained exactly 50,000
main-table payloads after updates, deletes, restoration, and restart.

Keep the controls opt-in. Fix and regress the ANN restart mismatch and the
ready-to-repair availability transition before 1M or promotion. Full metrics
and failed gates are preserved in
`scale/Performance1536D50K-comparison.json` (`.benchmark-results/vector-next-experiments/scale/Performance1536D50K-comparison.json`)
(`qualified: false`) and the experiment report (`.benchmark-results/vector-next-experiments/RESULTS.md`).

## Implemented ownership paths

Create a fresh standalone table with:

```json
{"num_shards": 1, "storage": {"dense_embeddings": "vector_store"}}
```

Omitting `storage` now selects `vector_store` for the qualified local standalone
deployment described above. Explicit `primary_lsm` remains available. The setting is persisted in the table catalog
and primary store, reported by table status, and immutable after creation.
Existing populated roots cannot be switched in place. Local single-shard LSM
tables are the initial supported deployment. Replication/HA, split and snapshot
paths are gated until they can carry the independent source store correctly.

`storage/artifact_payload.zig` provides the common transactional boundary.
`DocStore` converts dense artifact writes into references and reconstructs their
original envelope on reads, including cursors and multi-get. Documents, sparse
vectors and other artifact payloads retain their existing representation.

`storage/vector_payload_store.zig` owns `source-vectors/` at DB scope and reuses
the native shared vector-block/WAL implementation. References contain the
original source envelope, dimensions, and a SHA-256 identity over the logical
artifact key plus the complete versioned artifact. Different models, embedding
names, source hashes and dimensions remain distinct. Reusing an identical
artifact on retry reuses the payload identity; physical compaction does not
change the reference. Source payloads remain exact float32.

Preparation synchronously appends source payloads before the primary transaction
can commit references. Failed/ambiguous source appends fence that source owner
until reopen. A primary commit with an unresolved outcome prevents collection
until recovery. Transactions register reader ownership before taking primary
snapshots; cursors retain that ownership even after their parent transaction
ends. Collection first synchronizes primary storage and defers while any old
reader/preparation is active. This also protects the older *durable* primary
version when a newer in-memory commit has not reached disk.

The collector runs at writer reopen and explicit full DB sync, and is
available as `DB.collectSourceVectorGarbage`. It scans committed references and
publishes a replacement generation, reclaiming obsolete versions and uncommitted
preparations. Its scan, allocation and write costs count toward the experiment.
Its default is synchronous. The optional incremental collector described below
bounds payload copying per pass; marking and final publication still serialize
source access and need further work before general rollout. Table-owned embedding namespaces and their chunk
parents survive ANN index removal. Ordinary admissions with existing artifacts
bootstrap coverage through the existing generation-repair path.

The experimental mode now shares source payloads with ANN serving generations.
`source-vectors/` owns the lossless vectors. `indexes/vector-blocks/` is a compact
version map: its WAL and immutable blocks contain logical artifact keys,
dimensions, source sequence/revision, and a 32-byte immutable payload digest.
They no longer contain another corpus of exact vectors. ANN-specific RaBitQ
codes, centroids and optional compact posting projection planes remain derived
index data and still count in total disk/memory measurements.

Each serving generation pins an immutable source snapshot. Point and batched
reads follow the version map directly into the source snapshot's WAL or native
blocks, without a primary-LSM lookup, artifact-envelope reconstruction, or the
source writer mutex. Positional reads retain their bounded concurrency; mmap
views and persisted residual hints retain their existing fast paths. Source
checkpoints and collection can replace files while old query leases retain
their original bytes. A full collection refreshes the manager's source snapshot
so obsolete generations retire as their last reader exits.

Reference-map construction reads physical primary references. Mutation
certification captures all requested references in a single atomic probe
multi-get; this preserves a coherent primary tip without cloning and sorting
the entire memtable. Base construction scans references without fetching vector
payloads. The source store uses float32 or float16 plus lossless residuals,
matching the configured vector encoding; comparisons must keep encoding fixed.

The collector retains current primary references plus the **latest** committed
ANN reference per logical key, not every superseded entry still present in ANN
deltas. Old in-flight queries retain their independent source generation. The
collector remains quiescent for primary transactions and synchronizes primary
storage before collecting. It can conservatively retain source versions named
by a still-persisted ANN map after index retirement; online scope-aware cleanup
remains a rollout improvement.

This is a fresh-table experiment. Pre-consolidation experimental tables with a
payload-bearing ANN vector map fail closed with
`VectorStoreReferenceFormatRequired`; create fresh tables for comparisons.
Older binaries reject the new reference block/WAL/manifest tags. No in-place
migration or downgrade is implied by the table setting.

Table `storage_status.source_vectors` reports the latest owner observation of
source WAL/block sizes, allocator
residency, retained payloads, last-collection live/unreferenced payload bytes,
checkpoint/collection I/O, resolve demand, and reader/ambiguous-commit deferrals.
`prepare_batches`, `preparation_ns`, `durable_append_ns`, and `checkpoint_ns`
separate source preparation and sync cost from end-to-end enrichment readiness.
Cumulative counters reset on reopen; retained sizes are reconstructed. Collection
read bytes count decoded vector payloads, and collection write bytes count staged
block files; these are logical work counters, not physical device I/O or page
fault counts. Checkpoint reads count the consumed WAL prefix. Heap bytes exclude
mmap pages and request-owned reconstruction buffers. Use process
footprint/RSS as well as these owner counters. Source preparation currently adds
a synchronous durability barrier even when the primary uses asynchronous write
acknowledgement; retain the same client durability policy and include this cost.

`scripts/vector_store_disk_accounting.py` inventories **every** data-root file,
including primary storage, separate journals, source vectors, ANN serving
vectors and other indexes. Embedded transaction/replay records remain included
in primary totals; this inventory does not assign individual SSTable keys to
journal categories. It reports logical and allocated disk bytes separately.

Run the controlled driver after correctness gates pass:

```sh
python3 scripts/run_vector_store_ab.py .benchmark-results/vector-source-ab \
  --binary /absolute/path/to/antfly --pairs 2 --include-1m
```

It uses fresh tables sequentially, alternates A/B then B/A, pins binary and measurement-script hashes,
uses identical float32 serving encoding and load/query settings, and stops on
any failed arm before scaling up. The underlying qualification harness accepts
`--dense-embeddings primary_lsm|vector_store`, prepares an isolated client copy,
and verifies the table's effective setting. Each arm includes existing mixed
same-vector updates and cold/warm process restart passes. Storage-mode
qualification runs in both arms version-changing updates, deletes and restoration
of the original ground truth before restart queries. It also measures a separate
4,000-document managed enrichment/full-text table using a deterministic local
embedding provider. Both phases fence their final real batch at `full_index`;
an empty batch is not relied upon to establish readiness. Model inference speed
and semantic quality are outside this deterministic-provider measurement.
Restart is a process cache boundary, not proof that the OS page cache is cold.

Validation completed in this worktree:

- 11 source payload tests: immutable model identities, physical primary references,
  old readers/cursors, failed/ambiguous preparation without primary publication,
  both outcomes of a lost primary acknowledgement, retry, restart, read-only missing authority, and orphan/version collection.
- 4 DB integration tests: persisted mode without ANN indexes, last-consumer
  deletion and rebuild, unchanged enrichment source-hash reuse, and stale chunk
  embedding deletion.
- DocStore regression suite: 672 passed, 1 skipped, no failures or leaks.
- Runtime error ABI: 7 tests passed, including transport of storage-mode gates.
- Public API: external model/update/delete/restart tests passed in both modes;
  generated chunk-embedding/restart tests passed in both modes; 3 invalid-mode
  and deployment-gate cases passed.
- Native-HBC enrichment: both storage modes completed two 4,000-document
  versions plus 100 full-text and 100 semantic queries. Source-mode restart
  reclaimed the first version, leaving 4,000 payloads (2,048,000 bytes).
- Three status tests pass, including callback publication while the DB apply
  lock is held; the source counter survives cached-status cloning and restart.

A 50K source-mode diagnostic loaded all 50,000 OpenAI vectors, but exposed a
full ANN checkpoint failure with float32 serving encoding: beginning an optional
float16 projection session returned `Unsupported` and prevented publication.
Full checkpoint construction now handles unavailable projections consistently
with the existing delta path. The lifecycle regression verifies publication,
search, required-projection debt and propagation of corruption errors. Recovery
of the retained 50K database succeeded with the corrected binary: all 50,000
documents became query-visible and the full checkpoint published. That restart
also exposed missing cached source counters after startup writer retirement;
status publication now carries owner counters into the cache; five public API
cases and three focused status regressions pass with this fix.

The 50K diagnostic then completed two rounds of 2,000 vector updates, 1,000
deletes and 2,000 restores. Before collection it retained 54,000 payloads
(331,776,000 bytes). Reopen reclaimed 4,000 obsolete payloads (24,576,000 bytes),
leaving exactly 50,000 live payloads (307,200,000 bytes). A 20-query recall@100
sample was 0.9855 before churn and 0.9850 after churn/restart; each cold/warm pair
agreed. This small Debug-build sample is diagnostic evidence, not a performance
qualification or a claim that the recall difference is statistically significant.

The 4,000-document native-HBC enrichment phase exposed a regression in the new
cached accounting callback, reproduced in both storage modes. A stack sample
showed an enrichment activity callback trying to acquire the DB apply read lock
through LSM-statistics collection. Source counters now travel independently in
runtime status and use only the source owner's nonblocking snapshot; the callback
cannot reacquire the DB lock or fabricate unavailable LSM samples as zeros. A
regression test invokes this callback while holding the DB apply lock; all three
focused status tests pass. The complete 4,000-document workload now passes in
both modes, including source-mode restart reclamation. The harness also
waits for index provisioning before timing ingestion and retains partial receipts
when a phase fails.

Host idleness is not a prerequisite for useful comparisons. Run fresh tables
sequentially in repeated A/B and B/A order on the available host, retain every
arm, and report paired ratios and run-to-run ranges. Record concurrent host
load and process memory alongside the results. Debugger-assisted runs and runs
with checkpoint failures are diagnostic evidence, not final qualification.

The focused driver additionally samples RSS, records all files after orderly
stop and restart, pins binary/workload hashes, and runs 1,000 queries per
workload by default:

```sh
python3 scripts/run_vector_store_enrichment_ab.py .benchmark-results/vector-enrichment-ab \
  --binary /absolute/path/to/antfly --pairs 2 --encoding float32
```

Consolidation diagnostics found that source preparation, including durable
appends, took less than one second across 8,000 generated embeddings in one
4K run. This does not support attributing its tens-of-seconds readiness gap to
source fsync alone. A stack sample exposed repeated primary-memtable snapshot
cloning/sorting during certification and replay cleanup. Certification now
uses the atomic reference multi-get described above; replay retirement scans
only its append-only lanes before applying the existing atomic deletion batch.

Repeated runs also exposed an L2 quantizer append bug: a directory reader can
materialize omitted centroid-dot products as zeros, and an append extended all
required arrays while leaving that unused array at its old count (observed:
93 entries for 144 vectors). Checkpoint validation then repeatedly rejected the
leaf. Appends now discard the unused L2 array; recovered delta construction
omits that optional plane without changing distance data. A regression test
publishes and reads the resulting larger checkpoint. Final comparisons must
use this fix in both modes.

Native shadow repairs must inherit the table's source-store binding before
opening their index. Without it, rebuilding can silently recreate a payload-
bearing ANN base. Query scratch capacity must likewise follow the resolved
source encoding, not the 32-byte map encoding. The repair regression inspects
the persisted candidate map, and source-reader tests cover both float32 and
float16 blocks. Enrichment readiness requires complete index readiness and
finished backfill in addition to row counts. Public status can still precede
a background repair transition: Debug runs observed transient `IndexRebuilding`
responses even after these checks. Timed 503s are recorded without retry and
excluded from successful throughput; any such error disqualifies an arm. The
focused driver retains all arms, while the larger qualification gates scale-up.

### Consolidated source-store measurements (2026-09-06)

Six fresh pairs, alternating A/B and B/A, ran sequentially on the available
host with the same ReleaseFast binary, float32 encoding, 4,000 documents,
128 dimensions, two complete embedding versions, and 1,000 queries of each
kind per arm. The deterministic local provider isolates storage/enrichment
work from model inference. All 24,000 timed queries succeeded and all twelve
arms restarted successfully. These are small-workload qualification results,
not evidence for 50K/1M or an all-metric improvement.

| Measurement (median across six arms per mode) | primary_lsm | vector_store |
| --- | ---: | ---: |
| Initial enrichment ready | 2.518 s | 2.840 s |
| Updated embeddings ready | 2.869 s | 2.970 s |
| Full-text successful queries/sec | 2,478 | 2,602 |
| Semantic successful queries/sec | 1,836 | 1,777 |
| Semantic successful-query p99 | 0.784 ms | 0.798 ms |
| Total logical disk after stop/restart | 10.858 MB | 8.335 MB |
| Sampled peak RSS | 238.84 MiB | 238.53 MiB |
| Restart ready | 0.108 s | 0.214 s |

Median **paired** source/primary ratios were 1.127 for initial readiness,
1.127 for updated readiness, 0.942 for semantic throughput, 1.022 for full-text
throughput, 1.080 for semantic p99, 0.777 for total disk, 0.999 for RSS, and
1.976 for restart. Ratios of medians differ from medians of paired ratios.
Initial paired ratios ranged 0.493–1.330 and update ratios 0.551–1.171; the
first baseline was an outlier and remains included. The consistent result is
lower disk usage, with modest write/query overhead and a slower small-table
restart. Cache-inclusive RSS showed no clear improvement at this scale.

Receipts: `/private/tmp/vector-consolidated-enrichment-release-ab/comparison.json`
and `/private/tmp/vector-consolidated-enrichment-release-confirm/comparison.json`.
Both pin binary SHA-256
`26cd3e42fd8d167a57784fb2c83b2366a9e1580f567db48a8ae38ac56b4d5362`
and workload SHA-256
`9f086856e184850ef8673010da97b466cd7ec957b127a966e198f6756506d397`.
Do not compare these absolute ReleaseFast times with the earlier Debug
diagnostic to claim a speedup attributable solely to consolidation. The valid
ownership comparison is between modes within the same binary and workload.

The first 50K pair subsequently passed the live load, read-only curve, mixed
writes, churn, and enrichment phases. Initial readiness was 15.472 s for primary
LSM and 14.282 s for source mode; maximum measured read-only throughput was
436 versus 392 queries/sec. Total logical disk before restart was 860.7 MB
versus 516.3 MB. Source mode then aborted during restart, so these remain
**unqualified diagnostics**, and later pairs/1M were gated. The crash exposed
a status-cache merge that shallow-copied owned index error strings and arrays
into a replacement snapshot. The merge now transfers deeply cloned entries;
a regression releases the previous snapshot before reading the replacement.
The focused synthetic-status suite passes 12 tests without leaks.

Subsequent changes require new binary qualification: source bootstrap runs
are merged once when initial ANN indexing settles (ordinary preparation keeps
its append path), and float16 cold projection sessions open pinned source files
while retaining the ANN owner's I/O admission. Source checkpoint counters include
the bootstrap merge's reads, writes, and elapsed time. A native cold-read test
uses an empty reference map over sixteen source shards to verify routing and
admission. Earlier float32 receipts do not exercise the float16 cold path.
Collection also verifies every selected digest before skipping a rewrite when
the retained inventory is entirely live. A repeated-collection test asserts
that neither the manifest generation nor written-byte counter advances. This
avoids charging every clean restart for a whole-corpus rewrite. The copied 50K
failure fixture survived three Debug restarts and sixty vector queries after
the cache-ownership fix, with 50,000 live payloads (307.2 MB) after reclamation.
The final Debug binary also passed all five public API cases and three further
restarts of the reclaimed fixture (8.76, 3.45, and 3.56 seconds), with sixty
successful vector queries. Every restart retained all 50,000 payloads and
reported zero collection bytes read or written. These timings are recovery
checks, not an A/B performance comparison.

### Final consolidated binary: repeated enrichment comparison

Four additional fresh float32 pairs (A/B, B/A, A/B, B/A) used the final
ReleaseFast binary, including the bootstrap, cold-reader, status-ownership and
clean-collection fixes. All eight arms restarted and all 16,000 timed queries
succeeded. Data, dimensions, query count and workload hash match the earlier
small comparison; these results qualify this workload only.

| Measurement (median across four arms per mode) | primary_lsm | vector_store |
| --- | ---: | ---: |
| Initial enrichment ready | 2.519 s | 2.975 s |
| Updated embeddings ready | 2.894 s | 3.162 s |
| Full-text successful queries/sec | 2,750 | 2,756 |
| Semantic successful queries/sec | 1,955 | 1,918 |
| Semantic successful-query p99 | 0.740 ms | 0.765 ms |
| Total logical disk after stop/restart | 10.560 MB | 8.628 MB |
| Sampled peak RSS | 235.47 MiB | 250.07 MiB |
| Restart ready | 0.110 s | 0.212 s |

Median paired source/primary ratios: initial readiness 1.179, updates 1.080,
semantic throughput 0.972, full-text throughput 0.989, semantic p99 1.051,
disk 0.797, RSS 1.070, restart 1.933. The first baseline query arm was slow
(228 semantic queries/sec); it remains in the results. The repeated medians
show a disk benefit with remaining readiness, memory and restart costs. They
do not establish a uniform performance win or isolate individual fixes against
the older Debug measurement.

Receipt: `.benchmark-results/vector-store-history/results/vector-consolidated-final-enrichment/comparison.json`.
Binary SHA-256:
`12d8284ef004117eba065b5e70d38883a813ad8ea3b9df7d5d68bc0221e03bb2`.
Workload SHA-256:
`9f086856e184850ef8673010da97b466cd7ec957b127a966e198f6756506d397`.

The same final binary also passed two float16 pairs (four fresh arms, 8,000
timed queries, all restarts). Median paired source/primary ratios were 0.926
for initial readiness, 1.121 for updates, 0.943 for semantic throughput, 1.011
for full-text throughput, 1.096 for semantic p99, 0.611 for total disk, 1.010
for sampled RSS and 1.936 for restart. Median total disk was 12.410 MB versus
7.557 MB; semantic throughput was 2,030 versus 1,914 queries/sec. This exercises
float16 configuration without treating two pairs as precise estimates. Receipt:
`.benchmark-results/vector-store-history/results/vector-consolidated-final-enrichment-f16/comparison.json`.

## Refinements suggested by the measurements

All five refinements now have implementations for qualification. Metadata-only
reads are used by primary freshness checks, enrichment freshness checks, dense
counter updates during publication, and key-only artifact/delete scans. The
other four experiments are independently configurable on a fresh table's
server process:

| Experiment | Environment setting | Control |
| --- | --- | --- |
| Metadata consumers | `ANTFLY_SOURCE_VECTOR_METADATA_ONLY=1` (default) | `0`: resolve full payload too |
| Source location hints | `ANTFLY_SOURCE_VECTOR_LOCATION_CACHE_ENTRIES=65536` | absent/0 |
| Segment sizing by bytes | `ANTFLY_SOURCE_VECTOR_TARGET_SEGMENT_BYTES=8388608` | absent/0: fixed 128 shards |
| Incremental collection and checkpoint receipts | `ANTFLY_SOURCE_VECTOR_GC_STEP_BYTES=8388608` | absent/0: synchronous collection |
| Dense artifact publication batches | `ANTFLY_ENRICHMENT_ARTIFACT_BATCH_ITEMS=64` | absent/1: one artifact per transaction |

The metadata control forces full payload reconstruction at the metadata API;
physical key-only scans remain enabled in both arms. The comparison runners
accept `--refinement`: `run_vector_store_enrichment_ab.py` supports each of the
five settings and `combined`, while `run_vector_store_ab.py` supports
`combined` at 50K and optionally 1M. These compare control/candidate settings
on fresh `vector_store` tables with one binary and record every arm
environment. Without this option they retain primary/source A/B behavior.

The reusable experiment controllers and scale summarizer now live under
`scripts/` in this worktree:

- `scripts/run_vector_store_refinement_experiments.py --binary /path/to/antfly`
  runs all five controls separately and then together; select a Python with the
  benchmark dependencies using `--python`.
- `scripts/run_vector_store_refinement_qualification.py --include-1m` builds
  ReleaseFast, freezes the harness, runs lifecycle API tests, repeats GC and
  combined enrichment pairs, then qualifies the combined 50K/1M configuration.
  It accepts `--binary` to skip building and explicit VectorDBBench paths.
- `scripts/summarize_vector_store_refinement_scale.py /path/to/scale` reports
  completed arms, recall, latency, throughput, disk, RSS and paired ratios.

New controller runs retain scripts, logs, receipts and data under the worktree's
`.benchmark-results/` by default. Completed earlier comparison summaries and
receipts are preserved in `.benchmark-results/vector-store-history/results/`;
historical one-off diagnostics and source-edit helpers are archived separately
in `.benchmark-results/vector-store-history/scripts/`. These ignored result
directories survive reboot but are not part of git history.

Location hints contain a digest, generation, shard and validated block location;
they never own vector bytes or pointers to a retired reader. Contended cache
locks are bypassed. Segment sizing preserves mixed model dimensions, grows at
stable source publication and can shrink during collection. Publication batches
apply to plain and chunked dense embeddings; the same source-version fencing
also protects single and multimodal dense completion. Guarding is unconditional
in both table storage modes. Batch sizing changes commit granularity but keeps
the existing source preparation and primary commit durability contract.

Collection starts with a quiescent primary mark plus durable ANN ownership.
On the supported local LSM backend it synchronizes the primary WAL, including
its index, without flushing the primary memtable. This is the required recovery
boundary; forcing scalar documents into SSTables is unrelated to reclaiming
source vectors and measurably changes query performance.
It then copies a bounded number of payload bytes per maintenance pass. New
preparations remain in the WAL suffix; retrying an old orphan re-appends that
identity after the cut, so it cannot be lost at publication. Post-cut primary
readers and old ANN leases retain their selected values. WAL pressure makes
writers help finish the reserved generation before ordinary checkpointing.
The initial mark and final manifest publication are still synchronous; the
byte budget is not a bound on wall time, metadata work, or a single vector.

`SOURCE_CHECKPOINT` is a checksummed optimization receipt, never an ownership
authority. Inventory reuse requires the same source manifest and recovered WAL
tip. Skipping a liveness mark also requires the same primary reference epoch
and a digest of ANN CURRENT/WAL contents. Reference writes and deletes advance
the epoch atomically with primary commit; aborted transactions do not. Missing,
stale or corrupt receipts fall back to reconstruction. A partial copy can be
abandoned before CURRENT publication; an ambiguous publication fences the owner
and preserves output files for recovery. Post-cut orphan preparations require
a later mark rather than receiving an all-live certificate.

Accounting exposes location-cache hits/misses/bytes, current source shards,
collection steps/pending bytes/mark time, receipt hits/inventory restores and
receipt bytes written in addition to WAL, payload, compaction and heap totals.
Source heap accounting includes the collector's mark map and retained input
state. Logical read bytes include abandoned copy work; output counters describe
completed staged blocks. Disk inventories also include temporary files. Writable
startup reclaims recognized abandoned vector-store temporary outputs before
starting any builders; read-only opens preserve them. ANN WAL hashing is additional
metadata I/O and must remain included in end-to-end timings.

Focused validation so far: 19 source ownership/recovery tests, 15 DB tests
with controls, and 13 DB tests with cache, adaptive segments, incremental
collection and batching enabled passed without leaks. These include delayed
plain/chunked completion after updates and deletes, ambiguous collection
publication, delete-only ambiguous commits, retries during collection, old
readers and restart. Delayed-provider coverage also includes deletion followed
by recreation with identical document bytes, checking that the old provider
result cannot be reused as the recreated document's embedding. The collection durability regression test verifies both
primary WAL sync calls, preserves the mutable primary entries without creating
a run, abandons the backend, and recovers the current scalar document and exact
embedding. The corrected release binary also passed all five public API tests
in 17.41 seconds. Its subsequent performance qualification failed the query
availability gate described below. Measurements above and the completed 50K/1M
scale qualification below use the pinned **pre-refinement** binary.

### First refinement experiments

All six experiments completed two alternating pairs on the shared host: fresh
4,000-row, 128-dimensional float32 tables, two embedding versions and 2,000
timed queries per arm. All 24 arms, 48,000 timed queries and restart checks
passed. Each control/candidate pair used the same ReleaseFast binary, SHA-256
`701827edc4c129e30b5bd9aaefe44bb2f4005761c374357ebfb630fb88343d1c`.
Receipts: `.benchmark-results/vector-store-history/results/vector-refinement-experiments.json` and
`.benchmark-results/vector-store-history/results/vector-refinement-{metadata,cache,segments,gc,batch,combined}/comparison.json`.

These are medians of the two paired candidate/control ratios, not confidence
intervals. Lower is better for time, disk and RSS; higher for query throughput.

| Enabled change | Initial ready | Updated ready | Semantic QPS | Full-text QPS | Restart | Disk | Peak sampled RSS |
|---|---:|---:|---:|---:|---:|---:|---:|
| Metadata reads | 0.949 | 0.990 | 0.981 | 0.981 | 1.032 | 1.021 | 1.002 |
| Location cache | 0.871 | 0.967 | 0.980 | 0.990 | 1.236 | 0.982 | 1.006 |
| Byte-sized segments | 0.881 | 1.198 | 0.974 | 1.028 | 0.510 | 1.043 | 0.994 |
| Incremental GC, before WAL-only fix | 1.273 | 0.985 | 0.792 | 0.801 | 0.509 | 1.102 | 1.104 |
| Publication batching | 0.760 | 0.514 | 0.991 | 1.088 | 1.002 | 0.986 | 0.965 |
| Combined, before WAL-only fix | 0.943 | 0.820 | 0.830 | 0.838 | 0.505 | 1.072 | 1.181 |

Batching reduced source preparation calls from 7,802 to 979–980 for 8,000
payloads. Its update-readiness ratios were 0.481 and 0.546, a repeatable benefit
in this workload. The configured limit is 64, but provider windows supplied
about eight completed artifacts per actual publication batch.

Metadata-only reads reduced reconstructed artifact counts from 24,941–26,144
to 18,815–19,178, without a clear timing win. Segment sizing reduced the source
shard count from 128 to 16 and roughly halved restart time, while updated
readiness regressed in both pairs. The cache-only arms allocated about 8.9 MB
but recorded **zero cache lookups**: the small corpus used the hot WAL path.
Those timings cannot establish a cache benefit. Combined arms exercised the
cache after collection moved payloads into blocks, recording 78,930–89,460 hits
and 90 misses; larger-table qualification is needed to judge the tradeoff.

The first incremental-GC experiment reclaimed the old version (8,000 retained
payloads became 4,000), used restart inventory receipts and halved restart time,
but reduced both query throughputs by about 20%. Inspection found that the
collector's primary `sync(true)` also flushed scalar documents out of the
memtable. It now uses the supported LSM's WAL-only durability boundary before
marking. The GC and combined results above deliberately retain that failed
prototype measurement; follow-up runs must use the corrected binary. Collection
also moves vectors from the hot WAL into immutable blocks, so the corrected
durability call alone does not prove all query overhead is removed.

### Corrected collector qualification: availability gate failed

The corrected ReleaseFast binary includes WAL-only primary synchronization and
the document-incarnation fence. SHA-256:
`185dc8f81f77d8b6b77f0389e4eb2766a6aed047791931bc93bc28671645a677`.
The GC and combined reruns each completed two alternating small-table pairs.
The GC control returned two `503 index_rebuilding` responses among its first
pair's 1,000 semantic queries; the combined control returned one among its
second pair's 1,000 semantic queries. Both comparisons therefore have
`qualified: false`. All candidate timed queries succeeded, but this does not
qualify their paired throughput ratios against failing controls.

The saved public status had reported complete readiness, no active backfill,
and complete source coverage before timing. Server logs subsequently show an
index repair/rebuild overlapping the failed queries. The cause of that later
lifecycle transition still needs isolation; a timing delay or ignoring 503s
would not fix the public availability contract. Throughput also varied widely
between these arms. No corrected-GC query-speed improvement is claimed.

The enrichment comparison driver originally saved the failed qualification
flag but exited with status zero. It now exits unsuccessfully after preserving
the complete comparison when any arm has timed-query failures. Replaying the
actual failed GC receipt through its summarization/gate code confirms rejection;
the earlier successful batching receipt remains accepted. The already-started
scale continuation was stopped after discovering this missing exit-status gate.
**The combined refinements have no completed corrected 50K/1M qualification.**

The corrected small runs, server logs, and interrupted scale root are preserved
under `.benchmark-results/vector-store-history/runs/`; phase receipts and
correctness logs are in `results/`, and the pinned executable is in
`binaries/antfly-refinements-qualified`. The initial comparisons above remain
useful experiments, with batching the clearest measured benefit. Keep the
optional physical/maintenance settings gated while resolving the availability
failure and completing their scale qualification.

The ownership boundary is the intended long-term shape: primary artifact
metadata and committed stable references, a table-owned exact-payload store,
and ANN-owned version maps and derived search structures. The current physical
layout and maintenance policy are experimental. Lower disk usage alone does
not establish that their query, memory, write and restart tradeoffs are optimal.

Exact source versions are immutable objects. A useful target to evaluate is
sealed vector segments with a compact relocatable location directory: ordinary
publication appends objects and updates small metadata, while collection moves
live objects only when reclaiming worthwhile space. This could avoid merging
wide immutable payloads solely to reduce sorted-run lookup fan-out. Reusing the
current vector-block engine is appropriate for the reference implementation;
whether to specialize its source maintenance requires measured compaction I/O
and query lookup evidence. Stable artifact identities remain authoritative,
and ANN generations may retain validated physical hints into pinned segments.

The following records the motivation and acceptance criteria for each change.
Implementation alone does not establish a performance win; qualify each flag
separately and then together so effects remain attributable.

1. **Separate artifact metadata reads from payload reads.** Transparent `get`
   resolution preserves compatibility, but can hide unnecessary payload work.
   `storedOrPendingEmbeddingSourceHash` previously called the resolved store read
   and reconstructs the entire embedding to inspect a source hash already in
   the committed reference envelope. Introduce a common metadata-only artifact
   read in both modes, then audit freshness checks, version comparisons, delete
   bookkeeping and replay. Request exact values only where the consumer needs
   them. Preserve snapshot semantics, source fencing and corruption reporting;
   a metadata read does not certify that missing source bytes are healthy.
   At the end of the first final 50K source churn sequence, the owner reported
   409,560 resolved artifacts (2.516 GB reconstructed) for 54,000 prepared
   payloads. Direct shared ANN reads bypass this reconstruction counter. Some
   reconstruction is required, but this is enough volume to warrant a call-site
   audit before assuming the remaining cost lies only in physical storage.
2. **Reduce reference-resolution work without changing authority.** The ordinary
   reference path currently locates the ANN version, authenticates its digest,
   then locates that digest in the pinned source generation. Float16 residual
   hints already bypass some repeated lookup. Profile the remaining path and
   extend generation-checked physical hints or bounded location caching where
   useful. Batch resolved reads by physical block. A physical offset is a hint,
   not the durable artifact identity: generation mismatch must resolve through
   the authoritative stable reference. Do not introduce an unbounded per-vector
   heap directory or recreate a full serving payload corpus to win query time.
3. **Make segment sizing follow payload bytes.** Fixed shard/file overhead is
   significant for small tables. Consider adaptive segment targets and blocks
   with compatible dimensions/encoding, retaining shared table ownership.
   Model/version identity remains explicit, and separate indexes may consume
   the same artifact version. Physical grouping must not require one store or
   tiny file per model/index, nor impose one embedding dimension on the table.
   Reuse the existing block/WAL engine while measuring metadata residency,
   files touched per query and checkpoint write amplification.
4. **Make reclamation incremental and recoverable.** Clean collection now
   avoids rewriting live bytes, but still discovers reachability by scanning.
   A durable liveness checkpoint plus replay of committed ownership changes
   could bound clean startup work; background collection could select segments
   by obsolete-byte density. Its recovery proof must include unresolved primary
   commits, durable ANN lag, retired index scopes and old reader generations.
   Counters alone are not authority. Bound retained bytes and collector work,
   and preserve the existing conservative collector as a verification path.
5. **Batch publication where counters justify it.** Preparation currently owns
   a mutex through deduplication, encoding, successor allocation and durable
   append, and may checkpoint a full WAL before appending. Measure queue wait,
   preparation, append and checkpoint time separately before changing this.
   Group commit and preparation outside publication exclusion may reduce small
   batch costs, but every primary reference still requires its payload to be
   durable first. Existing measurements do not identify fsync as the dominant
   cause of the earlier readiness gap.

   The final float32 enrichment receipts each record 7,802 preparation calls
   for 8,000 payloads, 0.434–0.577 seconds of preparation and 0.405–0.536
   seconds of durable append time across both versions. This makes batching
   relevant to the remaining small gap even though it cannot explain the old
   tens-of-seconds delay. Preparation calls are not a measured count of fsync
   syscalls. Consider coalescing completed enrichment results before entering
   serialized primary application, with per-artifact freshness validation and
   bounded wait time; a group-commit queue cannot combine callers that an outer
   lock already serializes through the entire prepare/commit operation.

Prioritize metadata-only reads and vector-loading profiling after the 50K
comparison, then test restart/reclamation, segment sizing and publication
batching with 1M and churn.
Keep derived ANN projections where they earn their measured latency benefit;
consolidating exact source ownership does not require eliminating every useful
derived representation. Keep the table option experimental until these costs
and the unsupported deployment lifecycle paths are qualified.

The first completed final-binary 50K pair provides a more specific profiling
lead. In the 1,000-query post-restart profile, mean server search time was
2.384 ms for primary LSM and 2.762 ms for source mode. Mean rerank vector-load
time was 0.660 versus 0.978 ms, including artifact-read time of 0.475 versus
0.733 ms. Exact vector counts were close (268.1 versus 270.4 per query), as
was recall (0.98302 versus 0.98351). This points to vector loading as a useful
optimization target; the aggregate timers do not isolate digest lookup from
positional reads, cache effects or scheduling. Logical positional-read counts
also differed (257.3 versus 270.4 per query), so it is not evidence that the
entire gap is lookup CPU. Receipts are the two `public-query-profile.json`
files under `.benchmark-results/vector-store-history/results/vector-consolidated-final-scale/Performance1536D50K-1-*`.

Both final-binary 50K pairs subsequently completed all phases with no reported
mixed-workload errors and successful cold/warm restart checks. Medians across
the two arms per mode were:

| 50K measurement | primary_lsm | vector_store |
| --- | ---: | ---: |
| Initial readiness | 15.382 s | 14.359 s |
| Peak read-only throughput | 442.1 queries/s | 405.7 queries/s |
| Post-restart profile p99 | 5.183 ms | 6.312 ms |
| Live recall@100 | 0.98325 | 0.98330 |
| Total logical disk after restart | 877.8 MB | 474.8 MB |
| Read-only sampled peak RSS | 1.829 GB | 1.565 GB |
| Mixed successful query throughput | 177.7 queries/s | 152.3 queries/s |
| Mixed completed write rate | 1,400.8 rows/s | 1,211.5 rows/s |

The median paired disk ratio was 0.541, read-only throughput 0.919 and profile
p99 1.234. Mixed write-rate paired ratios varied substantially (0.581 and
1.268); report them rather than treating the median as a precise regression.
Mixed source runs wrote 28,800 and 44,300 rows versus 49,500 and 34,900 in
primary mode, so their lower mixed RSS is not an equal-work memory comparison.
Host-wide wired-memory changes also make the sampled demand proxy unsuitable
for claiming a precise process-memory improvement here. This is successful
correctness qualification with explicit performance tradeoffs. The pinned
binary has proceeded to 1M; it does not include the five new refinements.
Compact receipt: `.benchmark-results/vector-store-history/results/vector-consolidated-final-scale/50k-comparison.json`.

## Completed pre-refinement 1M repeat qualification

Both A/B and B/A pairs completed with the same pinned consolidated binary used
for the final 50K qualification. All eight scale arms (four 50K, four 1M)
exited successfully, including mixed churn and cold/warm restart checks. The
following are medians of the two 1M arms per mode; host idleness was not required.

| Measurement, 1M rows / 768 dimensions / float32 | `primary_lsm` | `vector_store` |
| --- | ---: | ---: |
| Initial ready | 322.99 s | 326.20 s |
| Peak query throughput | 444.83/s | 442.72/s |
| Live recall | 0.9900 | 0.9899 |
| Total disk after restart | 7.588 GB | 4.490 GB |
| Sampled read-only phase peak RSS | 8.044 GB | 6.845 GB |
| Mixed query throughput | 111.45/s | 122.14/s |
| Mixed write throughput | 566.76 rows/s | 626.03 rows/s |
| Post-restart profile p99 | 91.55 ms | 83.40 ms |

This establishes a repeated disk reduction of about 41% at comparable recall
and aggregate query throughput, before the five new refinements. Two pairs do
not establish small timing differences precisely; report the individual arms
and retain host-load variation. RSS includes resident mapped pages and is not
identical to heap allocation or the configured resource budget. Mixed phases
run for equal time rather than equal numbers of writes.

Receipt: `.benchmark-results/vector-store-history/results/vector-consolidated-final-scale/1m-comparison.json`;
full provenance and exit status: `ab-runs.json` in the same directory.
