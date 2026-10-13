# Hacker News deployment qualification

Status: publication and serving qualification follow-up to merged PR #1066, 2026-10-11.

The next milestone is reproducible evidence that `hn.antfly.io` can serve useful
search over the historical archive while keeping recent data fresh. Engine
capabilities are implemented within their documented bounds; small fixtures do
not establish archive performance or deployment readiness.

## Archive release gate

Use `examples/hackernews/archive-qualification.json` and
`archive_qualification.py`. The default invocation prints a local plan;
`--execute` verifies the live GCS object generations, reserves a run allowance,
and launches one temporary same-region pod through `regional.py`. Results retain
the source inventory, binary digest/revision, admitted pod, raw samples, logs,
and a verdict. Registration plus publication is included in the initial build-time gate.
Index construction failure fails the gate before query latency
can be qualified. An interrupted run also consumes its reservation.

The existing immutable export contains 47,717,307 live story/comment rows in 334
raw BigQuery Parquet files, 21,195,813,507 compressed bytes. It came from public
HN snapshot `1791556039163`, export job `antfly_hn_full_2b1f7524d047` in
`antfly-dev-01`. Its maximum row group contains 145,186 rows. Reuse this export;
new exports need their own pinned snapshot, dry-run and maximum bytes billed.
Do not silently replace a rejected raw export with normalized data and describe
it as raw Parquet qualification.

Initial deployment targets, measured in `us-central1` with 8 CPU, 48 GiB RAM and
10 GiB ephemeral storage:

| Gate | Target |
|---|---:|
| First highlighted search after clearing application disk cache | <= 10 seconds |
| Warm highlighted search p95, at least 20 samples per term | <= 750 ms |
| Eight simultaneous highlighted searches, slowest request | <= 2 seconds |
| First highlighted search after daemon restart | <= 2 seconds |
| Indexed metadata filter p95, at least 20 samples per predicate | <= 2 seconds |
| Cursor pages beyond position 4096, p95 and restart replay | <= 2 seconds |
| Initial text + five metadata index publication | <= 45 minutes |
| Observed construction process peak RSS | <= 85% of requested RAM |

Run two empty-cache/restart cycles, including 40 cursor pages of up to 128 hits
and replay of the final page after restart. Require more than 4096 distinct hits. Test common, selective, rare and broad terms
(`database`, `postgres`, `sqlite`, `antfly`, `AI`, `the`). Zero hits on a rare
term are valid. Check ranked IDs/scores across repeats and cycles, exact row
counts, projected highlight spans, equality/range/conjunction filters against
residual evaluation, empty filters and retained pagination across restart.
This matrix qualifies one simultaneous burst and a small term set; it does not
establish sustained-load p95, relevance quality or a multi-table service SLO.

The harness SQL count precedes the first text search and can warm Parquet
metadata. “Cold” here means empty application disk cache after publication, not
an uninitialized machine or a brand-new source. Record this distinction with
results. Peak RSS is the Antfly process high-water mark, not all pod processes.

Reserve $20 for each of at most six attempts, including failed/disconnected
attempts, and $25 conservatively for prior work: $145 planned reservations inside
the approved $150 incremental ceiling. One pod's maximum 3,400-second lifetime
bounds compute; no persistent volume, service or new cluster is created. The
current undiscounted resource calculation is about $0.56 per attempt, with a $10
allowance for storage/operations/other overhead. Rates are recorded in the config
from [GKE pricing](https://cloud.google.com/kubernetes-engine/pricing) and
[GCS pricing](https://cloud.google.com/storage/pricing). Reservations are local
admission controls, not an instantaneous billing cap. Reconcile actual charges
before additional attempts; do not reset the shared ledger to obtain retries.

Failed derived artifacts can be deleted after the owned pod has stopped and no
reader remains. Successful artifacts should be retained for analysis until the
bucket's eight-day lifecycle removes them. Source data belongs to the earlier
qualification and must not be deleted by this runner. No long-lived deployment
may depend on this expiring bucket.

## October 10 measured blockers

The [recorded results](../../examples/hackernews/archive-results-20261010.json)
contain the initial attempts over the same raw archive. Merged main rejected a large
Parquet page with `ParquetRowGroupTooLarge`. The decoder fix eliminates redundant
per-column row-reference arrays while retaining the existing materialization
bounds; 132 Parquet and 31 lake SQL tests pass.

The fixed rerun advanced past that failure and stopped with
`SqlProgramLimitExceeded` after 195.3 seconds, using 174 MiB peak RSS. Static
inspection points to archive-wide build replay inheriting the SQL spill
manager’s 1 GiB limit; the error does not identify the exact limit site. Neither
attempt reached SQL count or search benchmarks. Both pods and their uniquely
scoped derived artifacts were deleted, preserving source objects.

The bounded construction below replaces whole-archive replay and reduction
with per-file work and durable, non-serving checkpoints. Spill limits remain
enforced. The optimized archive run must still prove complete construction and
the query matrix; raising a spill limit alone would not establish archive
scalability. The two initial allowances were consumed;
the user requested the bounded-construction follow-up and further benchmarks.
Retry extensions are recorded in the same ledger without changing spend limits
or resetting prior reservations. The first extension admitted a full-archive
rerun and a separately labelled 142,577-row raw-file search diagnostic. The
archive rerun failed after 841.6 seconds with `SqlProgramLimitExceeded`, at
267.5 MiB peak RSS. A constrained-capacity regression reproduced the predicate
builder exhausting a shared 64-file spill manager; predicate runs are now
capped at four. Two further retry allowances fit the unchanged $150 ceiling.
Runtime admission diagnostics distinguish file capacity from spill bytes;
the shared error alone does not establish the exact cloud failure site.

The single-file diagnostic failed at its first `Timeout` after 640.2 seconds,
with 260.4 MiB peak RSS. The engine queues a fresh fenced retry, but the original
harness aborted at the transient readiness state. The harness now records
`Timeout`/`DeadlineExceeded` observations and waits for checkpoint recovery
under its original absolute deadline. It still fails permanent admission
errors immediately. Those two owned pods and 15,701 derived objects were deleted;
the unchanged source copy is retained only for the next diagnostic attempt.

The fifth attempt used the predicate-capacity fix against the full archive.
Registration took 20.5 seconds, then construction stopped with `Unauthorized`
after 670.2 seconds, at 218.9 MiB peak RSS. Cached bearer expiration is consistent
with this failure, but the original expiry was not recorded and attribution is
unconfirmed. All five pods and 22,946 derived objects are removed. At that point the ledger
retained $125 in reservations; the final $20 diagnostic fit the $150 ceiling.

The regional harness now forces a fresh bearer once per pod and admits it only
when its expiry exceeds the remaining hard pod deadline by 30 seconds. It reuses
that token across cycles, avoiding credential-identity changes that invalidate
publication reuse. Reports contain expiry evidence only. This is a bounded
qualification credential flow; a durable service still needs workload identity
and refresh support. The live admission check and 22 harness tests pass.

The sixth attempt ran the separately labelled raw-file diagnostic on the
merged-main Linux `fast` binary (`f973688f6e`, SHA-256 recorded with the harness
hashes). Registration took 619.9 ms. It stopped at the original 2,700-second
build deadline with 350 MiB peak process RSS, after 38 engine `Timeout` events
and generation 39. It never reached SQL count or search measurements. Retry
generations shared five completed metadata child-tree reference sets; an
observation found 16 ordered roots referencing only those five sets. The text
index remained incomplete. All six pods and 50,069 failed derived objects are
removed, along with the generation-fenced diagnostic source copy. The original
334-object export is preserved. All six allowances total $145 in reservations,
not measured charges; reconcile billing before any additional attempts.

Separate native-client probes in the existing pod verified complete GCS uploads
and media reads for 24 KiB, 1 MiB and 8 MiB bodies. Upload samples were 105/94/253
ms; verified media-read samples were 62/76/223 ms. These are transfer diagnostics,
not search benchmarks. Their traffic overlaps pod-wide construction network
counters. Basic large-body transfers worked, but those probes did not exercise the
TLS plaintext-buffer boundary subsequently identified below. Concurrent artifact collection also remained in its mark
phase with no eligible deletions; that trace is retained without attributing the
construction error to maintenance.

Publication and checkpoint phases now have failure diagnostics, with more
specific text stages for source rows, ordinal pages, segment encoding/packs and
statistics. They preserve the original error and omit source text and credentials.
This instrumentation was added after the measured binary, and a subsequent focused production-builder attempt located the failure. The 160 lake regressions, 22 harness tests and
a keep-alive TLS upload regression pass. Archive latency remains unqualified.

## Bounded, resumable construction

Replay now admits at most one source file into its existing 1 GiB local spill
budget. Advancing to another file releases the prior run. Whole-source consumers
stream ordinarily; a single file exceeding replay admission uses the ordinary
streaming path. Replay is an optimization, while completed index contributions
are the durable recovery unit. Physical references, schema/null contracts,
delete handling and cancellation remain attached to the pinned source.

Fused metadata builders sort and publish each changed file before resetting
its local runs. A partially built ordered root carries zero fingerprints for
unfinished files, so retry cannot mistake missing rows for completed coverage.
Text construction updates document-frequency statistics per file with bounded
spill and page I/O, instead of reducing summaries for the entire archive in one
job. Its checkpoint contains a consistent statistics root and file manifests;
old contributions remain referenced until replacement completes.

The catalog stores a separate, non-serving checkpoint directory. Each complete
file commits through the renewing full-definition CAS. Ambiguous checkpoint
replies terminate the attempt; recovery rereads authority under a fresh lease
and reuses completed contributions. Source, definition, credentials, storage or
incarnation changes invalidate reuse. Only complete construction passes the
existing final source/lease checks and publishes a queryable generation.

Both vacuum traversal paths mark checkpoint roots. The collection cutoff also
protects the checkpoint's upload generation and later attempts. Replacement,
successful publication and DROP release checkpoint ownership through metadata.
These protections prevent vacuum from deleting an interrupted build's reusable
inputs without exposing its partial rows to query readers.

Memory, spill, file-count and per-file page I/O admission remain enforced. A
single pathological file can still exceed an index builder's limits; this change
removes archive-wide accumulation and does not establish a bound for arbitrary
file sizes. Full-archive construction and query performance must be measured on
the optimized binary before deployment readiness is claimed.

## Current/history correctness gate

Exercise the existing saved logical `hackernews` source with an explicit,
nonoverlapping creation-time cutoff. Current-only is the default; all-time search
uses the saved union, with the documented explicit RRF ranking contract. Current
and historical workers have independent state and warehouses. Archive WAL
backlog is a publication suffix; it is not a year-long in-memory current tier.

Before enabling continuous ingestion, prove old-record edits, deletes,
out-of-order parents, retries and restart reconciliation in the deployed cloud
path. Route old-record changes to the historical writer, or use keyed overlay
changes/tombstones; apply precedence before filtering and ranking. Publish lag,
last successful reconciliation, oldest unprocessed change, failed enrichment,
index readiness and maintenance failures as operator-visible status. Cutoff
migration must prove no missing/duplicate rows; atomic all-time cutover needs a
coordinated descriptor. A BigQuery historical snapshot alone cannot establish
current HN freshness or moderation visibility.

## Public deployment gate

Use dedicated durable same-region storage and workload identity configured
through `../colony/infra`. Record retention, reader-safe compaction/vacuum,
resource limits and recovery checks. Deploy a reviewable service configuration
and validate restart and ownership changes before exposing traffic.

The initial UI needs highlighted ranked results, links to HN, date/author/type/
points filters, relevance/date ordering and stable pagination. Choose defaults
for recent search and expose an all-time switch. Benchmark the actual saved
source and ranking used by the UI, including historical edits/deletes; a
single-table archive verdict does not pass this gate.

Sustained traffic should be tested separately with an explicit request rate,
duration and concurrency, recording p50/p95/p99, errors, RSS, CPU and provider
I/O. Rate limits and readiness must keep an index build or a cold replacement
owner from advertising a healthy serving endpoint prematurely.

## Engine work driven by evidence

Prioritize measured failures in raw Parquet page/materialization admission,
resumable index construction, cold warming, checkpoint reuse and historical
update/compaction costs. Full-archive facets require distributed reduction/spill
if a composed aggregation exceeds its current 100,000-row / 64 MiB bound. Do
not enable exact archive-wide facets by silently truncating.

Broader graph operators, serializable read-write lake transactions and new
catalog providers remain platform work. Semantic search and similar-discussion
features follow a relevance evaluation; they are not prerequisites for shipping
useful full-text HN search.


## TLS construction timeout resolution

A native production-builder diagnostic reproduced the timeout in text
`segment_statistics`, in a GCS page PUT, after 84 artifacts (7,660,290 bytes).
The TLS compatibility writer reset its plaintext buffer after one encryption
pass even when framing and authentication tags left an unencrypted tail.
The HTTP peer waited for the missing Content-Length bytes. Flush now consumes
only encrypted bytes and continues until empty; engine timeouts are unchanged.
An exact-capacity HTTP request reproduces the failure before this fix and passes
afterward with TLS 1.2 and TLS 1.3 on reused connections. All seven focused HTTPS
tests pass.

The first fixed optimized GCS diagnostic reached 1,995 artifacts (139,903,056
bytes) without an engine timeout before its local 300-second bound. Its artifacts
were generation-deleted after the local process stopped. The source is one
unchanged raw BigQuery file (142,577 rows, 61,797,036 bytes). The diagnostic uses
the production publication builder with a body-only schema, testing allocator,
local source and real GCS artifacts; it does not exercise the coordinator or
public serving API. Filesystem publication also passed in a debug correctness
run. These results do not qualify search or full-archive performance.

The budget is now fully reserved: the previous $145 plus a $5 local GCS diagnostic
allowance. The harness includes that allowance when admitting subsequent runs.
Billing export usage predates the final jobs, so no reservation has been released.
Further regional qualification requires actual-charge reconciliation or an
explicit budget extension. Required serving-path measurements remain cold/warm,
indexed filters, concurrency, deep cursors and restart recovery, followed by the
complete 47,717,307-row archive.

The subsequent 600-second completion check also hit its local bound before
publication: 2,265 artifacts, 157,341,017 bytes, no engine timeout logged. A
one-second stack sample placed remaining work in document-frequency statistics
reading pages through GCS. Complete GCS publication remains unverified; this
sample does not attribute all elapsed time or qualify regional performance.
All 2,265 artifacts were generation-deleted after the builder stopped; the
original archive source was preserved.


## October 11 follow-up sequence

Work starts from `origin/main` after PR #1066, in
`.worktrees/hackernews-publication-followup` on
`feat/hackernews-publication-followup`. The initial base is
`26edb20271407c60e257a583453a711947b4a9ca`. Changes and diagnostics belong to
that checkout; the main checkout contains independent work.

The updated detailed billing export is captured in
[`archive-spend-20261011.json`](../../examples/hackernews/archive-spend-20261011.json).
The six identified qualification pods cost $0.927106 gross. All HN-labelled
benchmark pods since October 9 cost $7.996654 gross, including those six and
other HN work. The shared import bucket has $0.277859 in exported charges;
this is bucket-wide, and its export only reaches October 10 at 23:00 UTC.
Cloud-program credits cover these posted charges. Project networking is shared
and the local October 11 storage diagnostics are not fully exported yet, so
these figures do not establish final all-in HN spend. Account for gross charges
rather than treating promotional credits as unlimited free usage.

The prior $150 figure was conservative reservations, not actual exhausted
spend. Preserve attempt IDs and original reservations. First add an explicit
reconciliation workflow to the admission guard: evidence identifies each run,
settles its exported compute charges, retains allowances for unexported storage,
requests and networking, and records released amounts as an append-only audit.
A reconciliation must not double-count shared bucket costs, erase failed runs,
refund pending costs, or increase the approved $150 ceiling. Extending the retry
limit should use reconciled exposure plus new reservations; creating a fresh
ledger to bypass the existing one is not a valid recovery path.

Then qualify one unchanged 142,577-row raw BigQuery file through the actual
coordinator and public API in the source bucket's region, `us-central1`.
Use a new owned artifact namespace and record the current optimized Linux
binary digest, admitted resources, immutable source generation, stage timings,
process memory/CPU and provider I/O. Keep the existing build deadline and
HTTP timeouts. The earlier laptop-to-GCS run sampled statistics page reads;
it does not establish same-region latency or a need to change engine behavior.
If the regional run is still slow, measure summary publication, document-frequency
reduction and final publication separately before changing the algorithm.
Avoid repeated uploads followed by immediate remote rereads where bounded
local streaming/spill can supply the same authenticated statistics; preserve
replacement/removal semantics, cancellation, budgets and fenced checkpoints.

After complete publication, verify SQL count, ranked results, highlights,
metadata filters, sort/pagination and source provenance. Measure cold/warm,
concurrent, deep-cursor and post-restart public queries with the existing
correctness and latency gates. A one-file verdict must remain separately
labelled; it cannot pass the full-archive gate. Only then build and qualify the
complete 334-file / 47,717,307-row archive, including interrupted recovery and
bounded resource use. Durable HN deployment follows those gates.

The existing `antfly-dev` cluster is already Autopilot. The measured pods used
regular general-purpose capacity, requesting 8 vCPU, 48 GiB RAM and 10 GiB
storage, about $0.59/hour at the recorded region's list prices. Keep that
profile for the first comparable regional check; evaluate smaller requests
as a separately labelled profile with measured headroom, rather than changing
resources and attributing all improvement to engine changes. Spot can reduce
compute cost, but eviction loses the current harness's pod-local catalog and
state directory. Use regular capacity for reproducible timing runs. Before
using Spot for resumable construction, persist authoritative catalog/checkpoint
ownership and prove recovery in a replacement pod; remote derived artifacts
alone are not sufficient.


The follow-up now implements reviewed compute reconciliation in
`archive_budget.py`. The six deleted pods release $59.072894 of excess reserved
compute while retaining $10 per run for noncompute, all earlier/local holds and
the original history. Recorded exposure is $90.927106. The reconciled follow-up
plan permits two new $20 reservations, for at most $130.927106 held or recorded,
under the unchanged $150 ceiling. No pending storage/networking hold was refunded.
The cursor workload now uses `AI`, with 4,110 actual analyzed matches in the
unchanged one-file partition. The previous `the` selection was invalid because
the standard analyzer removes it; physical substring counts cannot establish
indexed cursor coverage. The primary query has 856 matches and cannot qualify
the 4,097-hit depth gate by itself.

The follow-up profile enables `qualify_composed_sources`. It inserts two native
current rows under a numeric `hn_id` key and verifies union, replacement overlay,
saved-source lookup and retained source cursors after mutations and daemon
restart, against the unchanged historical Parquet source. This is a qualification
assertion, not measured evidence until the actual public-API run passes. It does
not establish a running Firebase feed, replacement-pod recovery or serving SLO.


## First regional publication result and corrections

[`publication-baseline-20261011.json`](../../examples/hackernews/publication-baseline-20261011.json)
records the first completed regional one-file publication. The unchanged
142,577-row file and all declared text/metadata indexes published, and SQL count
passed. The qualification verdict failed: cold highlighted search took
16,102 ms against the 10,000 ms gate; warm primary p95 was 323 ms. Publication
plus queries before failure took 1,107.78 seconds; the failed harness did not
retain an isolated publication duration, so that duration remains unknown.
The owned pod was deleted. Full-archive qualification and deployment have not
passed.

Cold search received 55,652,318 network bytes with the stored-source projection
disabled. The next profile explicitly enables `store_source` on the archive text
index, retaining the complete declared relational row while indexing only body.
Both highlights and returned HN metadata can use the authenticated sidecar. The
v2 stored-source recipe fences older body-only projections without changing
source-free recipe identity or the segment format. This increases sidecar storage and must be
measured; it is not yet a qualified latency improvement. The serving candidate
uses the same policy.

The next [registration probe](../../examples/hackernews/publication-registration-20261011.json)
stopped before publication because the public full-text allow-list omitted
`store_source`. Its pod was deleted and its artifact prefix was empty. Public
create-table/create-index admission and response projection now preserve the
boolean option. The regression also rejects non-boolean values. A native stored
source test deletes the Parquet object and verifies body plus returned metadata
remain available from the authenticated segment. These are local contract
checks, not regional latency measurements.

The latest billing export still omits today's two pods. The
[closed-pod evidence](../../examples/hackernews/archive-compute-bounds-20261011.json)
replaces their excess compute reservations with two full hours each at their
admitted resources, preserving $10 per run for pending noncompute. This is an
explicit upper estimate, not posted spend. Exposure is $113.301782 before another
attempt; a new $20 reservation fits the unchanged $150 ceiling. Original run
history and allowances remain, and posted gross billing supersedes these bounds,
including an overrun. The failed registration attempt measured no search latency.

The real deployment target is an organization-owned Antfly Cloud instance.
Colony PR #762 consolidates the optional standalone storage proposal; it is
disabled by default. Its initial 20 GiB disk is not a Cloud storage requirement,
and its example service-account grant does not authorize a managed tenant. The
application renderer can omit separately provisioned prerequisites with
`--foundation-exists`, but is not the managed provisioning path. Qualify the
released runtime, actual tenant identity, credential refresh, replacement-pod
recovery and authenticated managed API before public serving. See
[deployment guidance](../../examples/hackernews/DEPLOYMENT.md). A full-archive attempt must budget GCS operations and retain partial
catalog/checkpoint state; do not multiply a one-file timing into a proven bound
or discard an interrupted build's catalog after its pod deadline.

The composed history/current check failed with `unsupported_exact_sort` on a
numeric native-table key filter. Exact native filter lowering now translates
unambiguous JSON pointers and declared numeric/boolean equality. Positional
array paths, literal dotted components and integer equality outside f64's exact
range remain unresolved rather than producing approximate sorted results.
Native indexed range predicates also translate safe pointer paths.

Sorted page publication now admits batches of at most eight immutable uploads.
All children finish before roots can be returned or owners released, including
failure and cancellation. Memory admission is synchronized across workers;
the page format, attempt scope and publication fences stay intact. Default
32 KiB builders retain their page identities. Initial bulk ordered trees and
text-statistics trees now target 256 KiB pages to reduce object counts, while
keeping the existing 1 MiB page ceiling. A 1,000-row regression compares complete
forward/reverse and bounded reads, verifies more than a fourfold reduction in
page writes, and rejects invalid targets before reading or uploading. Neither
batching nor this fixture establishes full archive throughput. Repeated remote reads during statistics reduction and full
archive object-request costs remain measurement targets.

The harness keeps the last nonempty deep page for restart replay, rejects
initial duplicate IDs, preserves completed filter/publication measurements on
failure, and continues baseline restart checks after a composed-source failure.
Required composed coverage still fails the overall gate when unavailable.


## Retained native cache loss

A retained native generation's local directory is a cache, not a completeness
proof. Resume validates the local manifest and sealed file identities under the
publication guard before acquiring the local reader lease. Missing or corrupt
local metadata, or a missing/replaced sealed file, can instead open the same
authenticated remote generation. Table identity, original range, capability
expiry, manifest identity, chunk scopes, lengths and checksums remain enforced.
The engine does not capture current rows to repair an old cursor. The current
repository opens remote pages without copying a complete generation back to disk.
Legacy repositories can materialize a missing generation, but do not overwrite
an invalid existing cache.

Malformed or absent cache manifests also must not prevent new snapshot admission.
Admission discards these invalid local roots only after obtaining their exclusive
per-cut lease under the parent guard. Active local readers prevent that cleanup;
the request fails its existing admission bound instead of deleting their files.
Remote reader leases do not depend on the discarded local directory.

The recovery regression primes the vector reader, restarts the live database,
and then preserves the local directory while removing its manifest, corrupting
its metadata or removing a sealed source-vector file. Each case must return the
original row and text match from remote storage without recreating missing files.
Removing remote commit authority must fail closed while fresh live reads still
return the newer row. The public HTTP matrix also exercises partial cache loss
before complete cache loss, sorted native filters and cursor restart.

## Merged #1063 follow-up qualification

PR #1063 adds term-directed admission, batched statistics, bounded 64-segment /
32 MiB text windows, within-file small-run compaction, final-winner hydration and
bounded planning warmup. PR #1081 must qualify those paths together with its
compressed opening metadata before the next runtime release. Earlier timings
in this document identify earlier binaries and do not qualify the combined code.

Measure empty serving caches with optional planning warmup disabled, default
readiness warmup after restart, and repeated queries separately. Record warmup
attempts/pages/time outside query latency, provider requests/bytes, nested phase
times and child-process peak RSS. Profiling currently selects a different planner;
use health counters for the primary timing matrix and profile a separate diagnostic
run. Both engines must use the same logical HN source, prebuilt indexes and native
host execution. Keep OS-cache, HTTP, analyzer, thread-policy and shared-host limits
explicit. Repeat at least two cycles and twenty warm samples.

Qualify historical/current union and the saved replacement overlay with real sample
rows in a native current table, alongside exact filters, highlights, cursors,
nonmatching replacement suppression and mutation/restart retention. Include a
fragmented publication above the 64-segment window boundary. A bounded current
fixture and local filesystem source are diagnostic evidence, not full-archive or
Antfly Cloud acceptance.

Use the results to prioritize remaining work: publication-wide best-first traversal
if routing or irrelevant posting reads dominate; cross-file compaction if small
historical files/segments dominate; broader bounded execution if composed or pending
write paths remain disproportionately expensive. These changes require the ownership,
GC and conservative bound contracts in `zig/NATIVE_LAKE_PERFORMANCE_FOLLOWUP.md`.

Release handoff follows the verified PR, not the benchmark binary: human-approved CI
on the final PR revision, merge, immutable runtime release/image digest, development
runtime-catalog selection, and an Antfly Cloud instance. Verify the actual tenant's
credential scope and refresh, both HN tables and saved source, replacement recovery,
publication and the regional query matrix through the managed API. Keep public DNS
routing disabled until those gates pass. Reconcile the existing shared cloud ledger
before additional jobs; reuse the retained archive export and preserve prior spend
and reservations. No standalone claim or engine is required for this Cloud path.

The combined local matrix exposed two follow-up implementation gaps. Planning
warmup must read declarations from an authenticated publication directory when
inline declarations are absent, and must not memoize an unavailable lifecycle
authority during startup as a completed warmup. Explicit leading descending score
ordering can carry a competitive cutoff across windows while retaining equal-score
contenders; ascending score and backward cursor pages cannot use that minimum.
Count-only requests retain exact execution.

Composed five-hit requests previously fetched/highlighted 128 hits from each source.
Start each source with the requested page and one lookahead, bounded by 128. Grow
fully suppressed overlay batches geometrically to preserve archive traversal;
count, aggregation and graph consumers retain the larger streaming batches. Cursor
identities, source authority and total normalization must remain unchanged. Qualify
this with the same 1,000-row current fixture and mutation/restart cursor checks.

Final local evidence is in
`examples/hackernews/combined-main-search-qualification-20261012.json`. The raw
empty-cache matrix measured 32.25–78.91 ms first-query, 7.87–9.03 ms warm p95,
and 29.61–38.56 ms restart latency. Readiness-enabled runs measured 30.74–49.13 ms
first-query latency and now prove positive warmup (42 planning pages before query).
Both physical layouts preserve exact HN IDs/scores and enumerate 4,110 cursor
matches with filters, highlights and mutable source cursors verified after restart.

Provider work is the more stable comparison: raw empty-cache reads remain
248 requests / 25.04 MB versus the earlier 295 / 43.31 MB. The composed page-size
fix reduces measured leaf hydration and provider work, but full composed requests
remain much slower than single-table queries. Profile retained native snapshot
capture, binding and response/control work before selecting another optimization.
The SQL ordered body projection still hit its statement work limit; direct Parquet
fixture setup is outside query timing and does not qualify that SQL shape. Keep
these findings separate from the completed local keyword/cursor correctness gates.

The follow-up composed profile found repeated directory enumeration. Native
capability creation and generation publication now leave expiry collection to
the supervised owner. Persist namespace discovery before the capability so a
failed physical capture remains collectible after restart or drop. Filesystem
prefix listing starts at the appropriate subtree, preserves partial-prefix and
pagination semantics, and skips symbolic links as the original bucket walk did.
Use a bounded-work regression with unrelated generations, and compare live
current, retained current, union and saved-overlay requests after rebuilding.

Restart validation also exposed field-order assumptions in lazy routing:
canonical routing names can differ from a physical segment's writer order.
Binding must match names and verify the same field statistics and section kinds,
without relaxing incarnation, source or checksum validation. Catalog fixtures
must use distinct relational index names within their shared namespace.

The catalog HTTP rerun traced the remaining `InvalidSegment` to masked WAL
composition allocating through a query facade's locked allocator. Compose
through the pinned inventory owner's allocator, whose lifetime covers shared
physical readers and private deletion cells. Preserve the sparse read capability
and its cancellation checks. Destroy dependent owner objects before releasing
the base pin that owns their allocator. Keep the immutable-sharing allocator
guard; exercise this path with restart, nullable WAL rows and private masks.

Bounded composed pages can report `hits.total.relation = "gte"` until the
remaining source relation is visited. Exact overlay cardinality is a separate
`count: true` request. Qualification must test the bounded cursor and exact count
independently, without forcing every first page to scan its archive.

The final optimized follow-up passes 84 local Python tests and 28 subtests,
including four filesystem/S3 fixture cursor recovery cases. All eight catalog
HTTP cases pass after the ownership fix. The broader lake gate passes 237 other
regressions plus the corrected incremental stored-row assertion, without leaks.
The final raw and 67-file layouts retain exact IDs/scores, filter behavior,
highlights, 4,110-match cursor enumeration and mutable-source restart visibility.
Highlighted first searches measured 76.13 ms raw and 106.48 ms fragmented;
post-restart searches measured 29.49–35.71 ms. Eight-repeat composed runs measured
union warm p95 60.25–65.18 ms and saved-overlay warm p95 67.54–73.56 ms. A matched
before/after diagnostic records the large reduction from removing foreground
collection and bucket-wide enumeration. These shared-host sample measurements
do not establish full-archive performance or a managed Cloud SLO.

## Follow-up after #1081: publication traversal and cross-file serving runs

Keep the canonical per-file native inventory as the incremental rebuild authority.
An optional authenticated `compacted` root is the serving generation. It carries
merged native runs, their own term-directory envelopes, and complete physical
member spans referencing the original file manifests. It preserves producer IDs,
global statistics, deleted row holes, typed filtering, highlights and public
pagination. Source Parquet files are unchanged. Source replacement/removal retires
the affected merged unit; unchanged units and copy-on-write directory paths are
reused. Retained readers and GC protect both canonical and serving generations.

The initial policy merges adjacent complete one-run files in groups of at most
eight, with a 16 MiB input target, a 32 MiB output cap and a 64 MiB merge workspace.
Larger/multi-run files retain their existing within-file policy. Workspace denial
falls back to the original runs. Metadata version 11 and reader protocol 36 fence
writers/readers that do not understand member mappings; historical v9/v10 roots
remain decodable. Upgrade readers before publishing the new format.

Publication-wide best-first traversal uses authenticated term-page reductions to
prioritize promising segments. It skips only bounds strictly below the current
competitive score, keeps equal bounds for ID ties, and reports a lower-bound total
when pruning. Exact counts, backward/non-score ordering, unsupported query proofs,
and exhausted optional planning retain the exact existing traversal. The initial
query coordinator accepts single terms and single-token analyzed matches with
explicit leading `_score DESC`; broader query shapes remain supported through
that fallback. Frontier memory and object reads are bounded and cancellation is
checked even on cache hits. Health counters distinguish completed best-first
queries, admitted segments and pruned nodes from the profiling planner.

The reported SQLSTATE 54000 for `ORDER BY created_at ... LIMIT 1000` was a response
budget mismatch: HTTP SQL defaults to 128 rows unless its request `limit` is set.
Local probes with `limit: 1000` return 1,000 rows in both directions, including body
projection and `created_at IS NOT NULL`. The HN qualifier now seeds current rows
through Antfly SQL for both filesystem and remote sources, sets its explicit row
budget, and bounds that fixture to the supported 4,096-row response maximum. It
no longer substitutes a local PyArrow projection for this SQL path.

Development checks on the 142,577-row, 67-file sample produce a serving generation
with **9 runs**, while retaining the 67-file canonical inventory. The debug runtime
preserves the previously recorded top HN IDs/scores and passes exact filters,
highlights, union/overlay composition, SQL seeding, pagination and restart checks.
Debug timings are correctness evidence; optimized matched timings must be recorded
separately before treating them as a performance comparison.


### Optimized 1,024-file local comparison, October 13

[Recorded evidence](../../examples/hackernews/lake-scalability-qualification-20261013.json)
compares the retained optimized #1081 executable with the follow-up implementation
on the same 142,577 logical rows, re-encoded into 1,024 Snappy files. Each executable
ran two application-cache-cleared/restart cycles, 20 warm samples and 10 cursor
continuations. This preserves the canonical 1,024 runs and serves **128 compacted
runs**, exercising publication-wide traversal above the 64-run threshold.

| Measurement | Retained #1081 qualifier | Follow-up |
|---|---:|---:|
| First highlighted `database` search | 327–407 ms | 136–240 ms |
| Warm highlighted search p95 | 77–92 ms | 51–55 ms |
| First highlighted search after restart | 172–183 ms | 95–102 ms |
| Provider object reads during first search | 1,461 | 405 |
| Provider bytes during first search | 50.85 MB | 24.33 MB |

Both follow-up cycles visit 58 serving segments and prune 70 term-directory nodes.
Top-five public row IDs and exact floating-point scores match both baseline cycles;
highlights, exact filter comparison, empty filters, cursor continuation and restart
checks pass. Priority candidates are admitted in windows of up to eight, and
highlight locations follow the admitted snapshot's canonical segment order.
The full lake gate passes 155 local and 242 integration tests with no leaks; the
final optimized runtime also passes seven native gateway tests and nine subtests.

Cursor p95 and metadata-filter timings are mixed across cycles: cursor p95 is
178–224 ms versus 213–218 ms on the baseline; some filter cases vary or regress.
No general filter/cursor speedup is claimed. OS file caches were not flushed,
readiness warming remains enabled, and other local builds shared the development
host. These small normalized-data runs establish correctness and reduced query
I/O, not full-archive, raw BigQuery encoding or Antfly Cloud performance.

At PR #1117, the 1,024-file **cold composed search returned HTTP 422
`query_candidate_budget_exceeded` on both executables**. The composed stream
requires exact leaf totals, while large ranked pages can return lower-bound totals.
This is separate from SQL fixture seeding: Antfly SQL successfully supplies the
1,000 recent rows with an explicit 1,000-row HTTP budget before that diagnostic
fails. The smaller 67-file development composition check passes, but it does not
qualify this larger cold composed workload. Fixing that leaf-count contract and
full-archive/cloud qualification were the next follow-ups. No cloud job or deployment
was launched for this comparison.

### Composed cardinality and release handoff follow-up

A bounded ordered page and an exact relation count are separate contracts. The
composed executor now accepts a full requested ordered leaf window whose total
relation is `gte`. It keeps the exact total unknown and returns a conservative
visible-prefix lower bound. It does not add page-local lower bounds, because leaf
adapters can report either full-relation or remaining-relation counts. A short
window with an unknown lower-bound total still fails closed. An exact short or
empty terminal window proves exhaustion; full and remaining exact counts are
validated against the retained position. Explicit union `count: true` still
requires exact leaf counts, while overlay counts stream through visibility.
Replacement lookups retain their exact, bounded completeness requirement.

Retained source continuations use version 3 and pin catalog/schema/policy identity
independently of cardinality. Version 2 cursors remain readable: their existing
exact total establishes that the identity was already bound. Refills reauthorize
exhausted sources and verify immutable serving tokens. Unknown cardinality cannot
allow a drop/recreate or schema/policy change to silently rebind a cursor. The
merge loop checks exhaustion again after refill before indexing a possibly empty
terminal batch.

The regional runner now forwards `benchmark_composed` and
`composed_current_rows`. The archive acceptance profile requires cold and warm
measurements for both the historical/current union and the saved replacement
overlay, in addition to existing suppression and retained-cursor checks. Results
bind each public hit's table, ID and score, so equal IDs across sources do not hide
ordering changes. The 1,000-row current fixture is bounded diagnostic coverage;
it does not represent a year's worth of current HN data. Percentiles use nearest
rank consistently. Missing measurements, a different fixture size or exceeded
latency limits fail the gate.

The full-archive follow-up profile must explicitly match the existing shared
ledger before execution. A dry run reports remaining run slots, unreserved budget
and whether another reservation can be admitted. Updating a profile cannot reset
the retained ledger or create retry capacity. New billing and cloud-preflight
records remain local; reconcile posted gross compute and retain unexported
storage/network allowances before admitting another worker. Promotion credits do
not reduce budget accounting.

After local qualification, merge the fix and run the human-approved CI gate on
its final revision. Publish an immutable native runtime release and verify the
release/image digest includes this merge and #1117. Select that release in the
Colony development catalog, preserving the existing rollback reference and
leaving auto-upgrade disabled. The actual deploy uses an organization-owned
Antfly Cloud instance, with its tenant identity and credential-refresh path; a
standalone benchmark pod or retained 20 GiB claim does not qualify that path.
Verify restricted object access, both HN tables and saved overlay, publication,
cursor retention and replacement-owner recovery through the managed API. Reuse
the pinned 334-object, 47,717,307-row export after verifying its live generations.
Run the regional latency matrix only after budget admission is restored through
reviewed reconciliation or a new explicit ceiling. Public DNS remains gated on
managed deployment qualification. This follow-up does not create a release,
promote the development catalog or deploy a tenant.
