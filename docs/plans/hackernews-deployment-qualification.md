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

Reserve $20 for each of at most four attempts, including failed/disconnected
attempts, and $25 conservatively for prior work: $105 planned reservations inside
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
contain two attempts over the same raw archive. Merged main rejected a large
Parquet page with `ParquetRowGroupTooLarge`. The decoder fix eliminates redundant
per-column row-reference arrays while retaining the existing materialization
bounds; 132 Parquet and 31 lake SQL tests pass.

The fixed rerun advanced past that failure and stopped with
`SqlProgramLimitExceeded` after 195.3 seconds, using 174 MiB peak RSS. Static
inspection points to archive-wide build replay inheriting the SQL spill
manager’s 1 GiB limit; the error does not identify the exact limit site. Neither
attempt reached SQL count or search benchmarks. Both pods and their uniquely
scoped derived artifacts were deleted, preserving source objects.

Replace whole-archive replay with bounded file/cohort windows shared across
index consumers. Release a window after its consumers finish, persist completed
contributions for resumable construction, and publish only after complete
coverage and source/lease validation. Bound local disk independently of archive
size and expose progress and the exhausted resource in failures. Raising the
spill limit alone does not establish archive scalability. Repeat construction
and the query matrix after this work. The two initial allowances were consumed;
the user requested the bounded-construction follow-up and further benchmarks.
A reviewed extension records two additional attempts in the same ledger without
changing spend limits or resetting prior reservations.

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
