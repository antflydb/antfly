# Hacker News deployment qualification

Status: qualification follow-up to merged PR #1025, 2026-10-10.

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
