# Native serverless reuse audit for Cloudflare

The Cloudflare profile uses the existing serverless storage and execution
protocols. Finishing this profile does not require replacing them with a new
segment architecture. This audit separates implemented native behavior from
host integration and workload limits.

| Capability | Existing native implementation | Hosted integration |
| --- | --- | --- |
| Incremental document state | `build/document_facts_builder.zig` updates authenticated facts and pending-work trees; the builder loads touched rows when remaining vector/sparse projections are unchanged. | Used directly, including metadata-only changes. |
| Search artifact reuse | `build/builder.zig` compares projections and reuses unchanged text, sparse, and vector artifacts. | Used directly. Opt-in paged text/vector/sparse roots replace touched projections without reading unrelated bodies. Flat generations retain their legacy full-view boundary until migrated. |
| Publication continuation | Builder publication commits a WAL watermark; subsequent passes resume from it. | Queue dispatch, bounded input prefixes, and adaptive batch admission. Ordinary updates checkpoint between publications; the hosted reindex job checkpoints a frozen scan and WAL catchup separately. |
| Compaction | `build/compactor.zig` rebuilds the pinned published view, preserves document facts, reuses artifacts, fences publication, and handles retry/no-op. | Now exposed as `compact` and an optional scheduled Queue job, using host I/O and bounded preflight admission. It preserves the publication's graph families. |
| Enrichment continuation | `enrichment/worker.zig` scans indexed pending work under a finite cycle and persists CAS-protected stage progress. Source pinning and conditional WAL append reject stale model results. | Used directly. The embedding adapter now splits a document into dimension-bounded provider windows. Only a complete document is appended. |
| Retention and collection | Native read pins, work leases, retirement floors, manifest formats, and page validators define authority and reachability. | The durable hosted collector orchestrates these native rules over R2 and coordinated metadata. |

## Antfly-owned metadata profile

The object-native R2 coordinator is now implemented as a separate opt-in profile.
It shares Antfly's ordered page engine and commit protocol with Lite, persists
frontiers and metadata directly in R2, and supports explicit frozen migration
from the SQLite compatibility profile. See the
[backend design](antfly-object-storage-backend.md) and
[current qualification evidence](cloudflare-qualification.md#antfly-native-r2-coordinator-2026-10-24).
The [optimized R2 path](cloudflare-performance.md) adds one-RPC read views,
whole-operation mutation commits, body packs, bounded warm caches and tenant
routing; its live fixture records 124 ms median warm reads before namespace
replacement and 132 ms afterward.
The SQLite pressure measurements below describe the compatibility profile;
its recovered memory error was not root-caused. The new profile removes
application SQL/KV queries. The ANN recall limitation remains independent.

## Adapter changes

The native compactor previously instantiated a threaded fallback type even
when a caller supplied I/O. It now uses that fallback only for native callers
without an I/O token; freestanding callers must supply host I/O. Existing
native calls keep their behavior. Compaction no longer adds an implicit graph
when the source publication has no graph family, which keeps the admitted
hosted artifact set compatible with collection.

Hosted requests now use the existing `WorkingSetAllocator`, with a default
20 MiB requested-allocation limit and an observed peak. ABI transfer buffers
remain owned by the WASM allocator; the object transport distinguishes that
allocator from the request's working allocator. Responses are copied out
before the temporary allocator wrapper goes away. This is allocation admission,
not a measurement of all WASM or JS memory.

`compact` uses native work-lease fencing, publication lineage, document facts,
and reader protection. Defaults admit at most 1,000 documents and 4 MiB of
manifest-declared artifacts. Those are coarse preflight bounds; transitive body
reads and build amplification remain subject to allocation, I/O, and deadline
limits. An empty unpublished database is a no-op. Failed whole-pass attempts
can be retried; compaction has no new intra-build checkpoint format.

The embedding adapter uses the native pending-work cycle and source guards.
Hosted checkpoints now retain completed provider windows across invocation
boundaries and Worker eviction. Only the fully assembled document reaches the
native conditional derived append. See the scale follow-up below for the
scratch cache's authority, retention, and admission boundaries.

## Measurement and remaining boundaries

`LOG_METRICS=true` logs numeric operation measurements: outcome, operation and
inference counts, transferred bytes, elapsed and I/O-wait time, native working
allocation peak, WASM linear-memory high watermark, combined lane memory, and
retained JS cache charge. It does not log document bodies, tokens, or model
inputs. Observation failures do not change operation outcomes.

A backwards clock still makes lease checks fail closed. The host now marks
that instance unusable so the Worker can replace it; an invalid in-memory clock
guard cannot permanently wedge a reusable lane. Durable fencing and deadlines
remain in the coordinator. A deterministic rollback/reopen test preserves the
original mutation receipt and reads the published document on fresh compute.

`qualification/benchmark.mjs` runs the actual WASM module over the R2 contract
fixture at increasing dataset sizes. It records publication, cold/warm reads,
compaction, object amplification, memory measurements, and explicit admission
failures. This is a local characterization tool; in-memory fixture storage is
outside the measured WASM budget. It does not measure Cloudflare latency, total
isolate memory, or production throughput.

The remaining build boundaries include definition changes and graph/external
base migrations, plus production capacity, ANN recall and failure qualification.
The opt-in paged projection and resumable reindex paths are described below.
Paged text avoids whole-index rebuilds for ordinary WAL updates;
embedding checkpoints avoid restarting all completed model windows. Joining
separate mailbox state into ranked search remains an application/query
extension. These changes reuse native persistence rather than replacing it.

## Earlier flat-format capacity observations

The same WASM module completed publication, accepted-visibility checks,
cold/warm reads, and native compaction for 100, 1,000, and 5,000 synthetic
documents. Documents contain a short ID-specific prefix and repeated body
text. The 5,000-document compaction run explicitly raised its preflight to
5,000; the default remains 1,000.

| Documents | Publication peak requested allocations | Publication peak linear memory | Compaction peak requested allocations | Compaction peak linear memory |
| --- | ---: | ---: | ---: | ---: |
| 100 | 312,382 B | 4,587,520 B | 120,535 B | 4,259,840 B |
| 1,000 | 1,308,270 B | 8,912,896 B | 1,029,705 B | 6,684,672 B |
| 5,000 | 6,532,286 B | 25,231,360 B | 4,824,209 B | 15,990,784 B |

Two host boundaries were reached before any new serving format was needed:

- The original 1 MiB PUT cap rejected a growing flat artifact. The existing
  R2 adapter already accepts an explicit write limit; the hosted default is now
  4 MiB, with an 8 MiB configuration ceiling. The control run stopped at
  published LSN 3,168 with `ResponseTooLarge`; all 3,200 accepted documents
  remained visible through the bounded overlay.
- A long-lived instance with no recycling exhausted 32 MiB linear memory at
  published LSN 4,368, despite a peak requested working set of 5,712,390 B.
  Its 4,480 accepted documents remained visible through facts plus WAL.
  Recycling between requests at 24 MiB completed all 5,000 documents with two
  instance replacements and preserved the immutable cache.

The existing publication-prefix setting also materially changes amplification.
At 5,000 documents, a 64-record prefix reduced R2 fixture GET bytes from
259,672,081 to 68,308,149, PUT count from 10,640 to 6,658, and WASM/host transferred
bytes from 888,485,212 to 236,024,165. Both runs completed. This does not remove
whole-view rebuild costs or qualify a 64-record prefix for every deployed
latency profile. The host retains its conservative 16-record default and
durable adaptive reduction.

These results bound this synthetic local workload. They do not establish a
general document-count capacity, real R2 timing, total isolate memory, or
long-term storage/cost targets. Failed admission and successful instance
replacement are recorded explicitly in generated reports.

## Deployed follow-up

A second disposable live run on 2026-10-24 UTC passed a 300-document atomic
batch, publication, concurrent writers, native compaction, collection, retained
retry receipts, and a Workers AI embedding through Gateway. It used a
64-record publication ceiling and spanned Worker versions
`8c1d7565-11d4-4f2a-b5ec-be307721acbf` and
`b837a288-cabe-4988-b230-c8f9803deb2a`. Lookup median/p95 were 107/364 ms in this
small fixture. Captured successful publication operations took up to 23 s;
the successful compaction sample took 1.1 s. These observations are not SLOs.

A fresh incarnation then passed a real 48-chunk document through two ordered
Workers AI/Gateway embedding windows, native compaction, and fenced vector
search. Model and dimensions were `@cf/baai/bge-small-en-v1.5` and 384.
Worker version was `665a85ce-3be8-439e-bccf-ed17b47d5b19`. The captured enrichment
sample reported 970,259 B peak requested allocations, 6,225,920 B linear memory,
two inference calls, and about 4 s elapsed time. Total isolate memory remains
unmeasured. This deployed model fixture does not qualify the local 5,000-row
case against real R2 latency or production concurrency.

Validation passed 553 native serverless tests and 28 owner tests (six skipped,
no leaks), 12 portable native contracts, 29 WASM/host tests with both Binaryen
132 packaging paths, and the persisted workerd suite, including forced lane
recycling, compaction admission, scheduled compaction, model-profile checks,
and collection after compaction.
Both live incarnations were drained, and the disposable Worker, maintenance
Queue, dead-letter Queue, and R2 bucket were deleted. Generated reports and
private deployment files remain ignored under `dist/qualification/`.

## Paged text and embedding scale follow-up

Changed flat text indexes can now merge normalized projections and postings,
remapping document ordinals without retokenizing untouched text or fetching
untouched bodies. Native facts remain authoritative; an old packed body
snapshot can be reused as a cache. Insert/delete projection comparison now
checks actual vector/sparse presence, so a text-only document no longer forces
a full rebuild merely because its document identity appeared or disappeared.
Real vector/sparse projection changes retain the existing full-view path.

For larger text collections, opt-in `PAGED_TEXT=true` stores text artifact
metadata version 2 roots over the existing authenticated page tree. One tree
maps document IDs to normalized text; another maps escaped term/document keys
to term frequency. Prefix compression reduces repeated term and ID bytes.
Only touched document projections and posting paths change during normal WAL
publication. Manifest publication and its watermark remain the commit point.
The normal collector traces both trees and honors retained generations and
read pins. Text-only paged compaction validates roots and returns a no-op.
It does not reconstruct a corpus body snapshot. Mixed vector/sparse builds
retain their admitted whole-view path.

A flat generation can convert during an admitted publication. All readers and
collectors must understand version 2 before the flag is enabled. Normal WAL
publication preserves the paged format once present. A large initial flat
conversion or definition migration is still a bounded whole pass. This is not
a resumable full-corpus reindex implementation.

The local actual-WASM/R2-contract sweep completed these cases using 64-record
publication prefixes, without raising the 20 MiB native working allocation,
32 MiB lane, 4 MiB PUT, or 1,024-operation limits:

| Documents | Publication peak requested allocations | Publication peak linear memory | Cold common-term query requested allocations | Three cold point edits: GET bytes |
| --- | ---: | ---: | ---: | ---: |
| 1,000 | 865,754 B | 6,291,456 B | 327,851 B | 466,876 B |
| 10,000 | 4,082,302 B | 13,565,952 B | 1,331,162 B | 902,733 B |
| 50,000 | 4,209,995 B | 14,155,776 B | 3,845,193 B | 1,281,535 B |
| 100,000 | 4,229,762 B | 14,221,312 B | 8,018,078 B | 1,243,600 B |

Every case also passed cold reopen, exact count, delete, lookup, replacement
search, and no-rebuild compaction. No lane recycling was needed. The corpus
uses ID-specific numeric terms plus repeated common terms. At 50,000 rows,
ingestion/publication issued 77,293 fixture PUTs and read 360,751,237 bytes;
this characterizes amplification, not a storage-price or throughput promise.
A separate 1,000-row run with 4 KiB unrelated body padding passed, with peak
publication working allocations 2,818,030 B and 536,416 GET bytes for three
cold edits. The large-body regression also edits/deletes two rows across more
than 3 MiB of unrelated bodies while keeping publication reads below 384 KiB.

Paged queries retain native all/any/prefix/phrase scoring and filtering. They
scan matching postings and rank candidates under working memory, I/O, and
cancellation bounds. Common-term candidate state can still grow with corpus
size; paged storage alone does not implement WAND/top-k pruning or unbounded
query capacity. Broad phrase queries additionally fetch candidate projections.

Embedding checkpoints store verified binary float windows in a disposable R2
namespace. A coordinator pointer binds the checksum, exact input digest,
dimensions, model/revision, and gateway identity. Payloads precede pointers;
missing payloads recompute, while checksum/shape failures reject reuse. The
native source lease and conditional derived WAL append still decide whether a
completed result can commit. Final documents inline vectors and never refer
to scratch objects. A crash after a model call but before checkpoint commit
may repeat billing; there is no exactly-once provider-call claim.

Default hosted passes admit one provider window and re-enqueue on yield.
At most 4,096 chunks/8 MiB raw vectors are admitted before the tighter final
WAL/JSON/working-memory limits. The 600-chunk, four-dimensional WASM regression
needed ten model calls and nine yields, replacing the host on every pass and
preserving chunk order. Separate tests reject corrupt cache bytes, recompute
missing payloads, and reject a stale model result after a concurrent source
write. Current/previous UTC-day cache buckets are retained; bounded sweeps
prune coordinator pointers and R2 scratch independently using persisted
cursors. This cache is computation reuse rather than a new data authority.

The deployed scale trial also makes object latency visible: a sampled successful
64-record publication used 136 object operations and took 24.4 s. Per-document
body uploads are still serialized through the host ABI; packing or bounded
parallel transport is a distinct throughput improvement requiring its own
format/collector or I/O contract work. Local memory results do not remove this
real-service limit. The live runner waits for Queue-owned watermarks rather
than issuing competing maintenance calls on every batch.

Live metrics exposed redundant parallel Queue deliveries contending for the
same native work lease. The reference configuration now limits maintenance
consumer concurrency to one for its single physical database. Fencing remains
native, and interactive reads/mutations retain their own lane. This removes
retry amplification; it does not parallelize the serialized R2 body uploads.

The disposable live paged-text follow-up completed 1,024 documents through real
R2, Durable Objects, and Queues, followed by point replacement/deletion, exact
counts, an old generation's ranked cursor, and no-rebuild compaction. Serving
spanned deployments `47dc4c04-d6d7-41af-b85a-35b7584c1a35` and
`c5a57177-2775-45fd-96f8-467526f6e197`. A subsequent mark/sweep completed and
preserved the expected 1,023 rows and replacement search result.

The first live mark attempt reported `SQLITE_NOMEM` after 591 completed tasks;
diagnostics showed 612 persisted tasks, 21 pending, with maximum task JSON size
378 bytes. Replacement compute resumed that frontier. The HTTP adapter now returns `503` with `Retry-After` for this platform
SQLite pressure error, allowing bounded retry against the same durable job.
SQLite mutation cursors
are now explicitly exhausted before the next statement, and the resumed pass
completed after deployment replacement and cursor-lifecycle hardening. The
workerd suite additionally passed 2,048 body marks plus 16 frontier nodes with
a mid-run restart. This does not establish the precise cause of the transient
host SQLite error or substitute for a long-lived Durable Object soak test.

The live follow-up was fully drained. Its generated Worker, maintenance Queue,
dead-letter Queue, and empty R2 bucket were deleted. Only ignored reports and
private local qualification configuration remain.

Final native validation passed 555 serverless tests and 28 owner tests in
debug mode (six skipped, no leaks), including the corrected artifact-retention
checks. Thirteen portable native contracts and 37 WASM/host tests also passed.
Both Binaryen 132 packaging paths were checked. Persisted workerd validation
covers paged reopen, model-window resume, scratch expiry, the 2,048-body mark
frontier, and retryable SQLite-pressure responses. License headers, Apache
product boundaries, and formatting checks passed.


## Paged projections, resumable reindex and transport batches

The hosted profile now supports opt-in `PAGED_PROJECTIONS=true`. Sparse indexes
use native copy-on-write document and term/document trees. Dense indexes use
bounded hash-prefix projection shards and the existing native clustering and
quantized vector codec, with routing records and independently authenticated
cluster payloads. Hash sharding changes clustering topology: score parity under
exhaustive routing and self-neighbor checks do not establish ANN recall for
production embedding distributions. Centroid routing remains a scan bounded by
the original request budgets.

The native reindex job scans a frozen document-facts source and checkpoints its
partial paged roots. It then catches up through accepted WAL prefixes and uses
the existing fenced manifest/HEAD publication. The collector lowers its proposed
retirement floor to the active source and traces checkpoint roots, so GC between
steps cannot reclaim the job's input or output. The small progress-pointer CAS
compares the live HEAD ETag in the same coordinator transaction; providers that
cannot perform a guarded write reject this capability. Job expiration, stale
messages, cancellation, failed checkpoints and ambiguous HEAD responses preserve
the last durable outcome. Lineage acknowledgement is paginated independently of
corpus size. Definition changes, graph families and external bases fail closed.

Body, page and cluster uploads now use the existing object host ABI's bounded
batch operation: at most eight immutable objects, four uploads in flight,
original per-object and aggregate byte admission, and joined in-flight writes
on failure. This does not parallelize mutable authority updates.

A local native WASM/R2 contract run using 384-dimensional deterministic vectors
and sparse projections passed ingestion, cold text/vector/sparse queries, edits,
deletes and no-rebuild compaction at both 1,000 and 10,000 rows. The 10,000-row
publication peak was 4,086,747 B of tracked native working memory and 15,597,568 B
of linear memory. The cold point-update stage read 1,166,563 B in total; the
largest publication used 780 object operations. Fixture storage and total isolate
memory are outside these measurements. This run used 16-record publication
prefixes (input mutation batches selected to fit the JSON request limit).

Reports remain ignored under `examples/cloudflare-serverless/dist/qualification/`.
The benchmark accepts `--paged-projections --dimensions=384`, `--reindex` for a
cold-resumed rebuild, and `--recall-queries=N` to report approximate recall@10
against a brute-force cosine reference at explicit probe counts. Recall results
are measurements, not assertions of a universal quality target.

## Extended SQLite investigation

The original live error remains unreproduced. The new lightweight qualification
Worker runs the actual journal against SQLite, holding the actor warm and
repeating mark/cleanup cycles. It deliberately excludes native WASM, R2 and
inference. Legacy mode skips mutation-cursor exhaustion; drained mode exhausts
every cursor. The live 32-cycle run completed 670,080 statements and 131,072
body marks per mode (1,340,160 statements total). Legacy mode left 404,736
mutation cursors undrained. Both modes completed with a peak database size of
6,688,768 B and no `SQLITE_NOMEM`. Database size does not measure native or total
isolate memory, and sequential A/B timing is not a controlled performance result.

The ignored evidence is
`antfly-1278-mvmnbmzg-f1070a/soak-mvmrl0c2-889302-report.json`; the lightweight
Worker version was `2792c1a7-fe00-48f8-aa23-4eb680bacaf1`. The synthetic actor
state was drained and the disposable Worker was deleted after qualification.

[workerd's SQL cursor implementation](https://github.com/cloudflare/workerd/blob/main/src/workerd/api/sql.c%2B%2B)
retains query state until exhaustion or cursor destruction, so draining ignored
mutation cursors is a justified lifecycle fix.
[workerd's SQLite wrapper](https://github.com/cloudflare/workerd/blob/main/src/workerd/util/sqlite.c%2B%2B)
also has database/process memory admission. Neither fact identifies the cause of
the earlier production-host error. Keep cursor draining, bounded frontier work,
retryable 503 responses and the new non-writing pressure diagnostics; collect
those diagnostics if the error recurs under combined application load.

The extended 10,000-row run also completed a rebuild with a fresh WASM instance
for every step: peak native working memory 3,898,084 B, peak linear memory
15,204,352 B, and at most 818 object operations per step. The whole local rebuild
took 158.5 seconds; initial ingestion/publication took 227.3 seconds. These
wall times include a shared development machine and in-memory R2-contract
fixture, not deployed storage latency.

The five held-out random-vector queries exposed a material ANN limitation:
mean recall@10 at 10,000 rows was 0.18 with eight requested probes and 0.66 with
32. At 1,000 rows the corresponding means were 0.78 and 0.96. These are
small synthetic samples and do not establish recall equivalence with flat
global clustering.
Keep the paged vector profile opt-in. Higher probe/effort settings trade more
cluster reads for recall; production rollout needs distribution-specific recall
and latency targets, and a globally trained or hierarchical routing design if
those targets cannot fit the request budget. The current shard design bounds
updates and rebuild memory; it does not finish that ANN-quality work. A follow-up
on the merged build measured mean raw-cosine recall@10 of 0.84 with
`search_effort: 1`. That visits all routing clusters but retains native quantized
candidate selection and integer score/tie ordering; it is not a brute-force
exact-query mode. This five-query sample does not identify how much of the
remaining difference comes from candidate selection versus score ties. The follow-up implementation
now preserves exact distances inside integer score ties, keeps equal-score
candidates eligible, and bypasses both candidate caps and approximate stopping
at full effort. Paged searches rerank every vector in the selected local clusters.
Ten held-out queries after this change measured mean recall@10 of 1.00 at both
1,000 and 10,000 rows with full effort, against an independently computed
brute-force cosine reference. At 10,000 rows, eight and 32 probes measured 0.22
and 0.79; at 1,000 rows they measured 0.85 and 1.00. The benchmark asserts full-
effort equivalence. This establishes a working exact control for this synthetic
fixture, not a recall or latency SLA for arbitrary distributions. Global route
selection remains the low-probe recall limit.

Native regression validation passed 558 serverless tests (six skipped), 28
shared engine tests and ten runtime ownership tests without failures or leaks.
Focused page/ranking/reindex tests passed with the native testing allocator.
The workerd restart suite also completed collection while retaining a partial
reindex whose source had fallen behind the live head's parent, then resumed
and published its concurrent WAL changes. Host fault tests cover checkpoint
write failure and lost HEAD acknowledgement. Guarded metadata writes reject
stale HEAD ETags in real coordinator transactions and unsupported providers
fail closed.

The merged-tree workerd run passed pressure injection through both collection
RPCs and ordinary object RPCs. Native transport/rate-limit errors return 503
with `Retry-After`, preserving retry of idempotent writes when the original
SQLite message has been translated by the WASM ABI. Coordinator diagnostics
cover both RPC paths. The merged tree also passed all 45 host tests, 14 focused
native page/ranking/reindex tests with no leaks, the guarded-provider rejection
contract, 13 portable native contracts, the storage test audit, license headers
and Apache product boundaries. Both Binaryen 132 packaging paths built valid
WASM; the final merged artifact was checked in real workerd.


## Tenant runtime isolation and global routing follow-up

The R2 profile now runs reads and background steps through tenant runtime
actors, with module-wide bounded compact/full WASM pools. Native WAL/receipt
pages are prepared before entering the metadata writer transaction; the final
HEAD ETag check still fences concurrent writers and collection epoch changes.
Background admission coalesces duplicate triggers and continuation phases.
Implementation, memory accounting and rollout are described in the
[performance design](cloudflare-performance.md#bounded-tenant-runtimes-and-prepared-wal-commits).

Dense paged format-2 roots now carry immutable global coarse centers and exact
document-to-cell membership. Updates, multi-projection deletes, old-generation
reads and collector discovery have regression coverage. This extends bounded
incremental routing; it does not establish a universal ANN recall target or
remove the need to rebuild when the corpus distribution changes. The benchmark
records routing shape and physical GET bytes alongside recall so extra probe
work is visible rather than counted as a free quality improvement.
