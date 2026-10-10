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

Reserve $20 for each of at most two attempts, including failed/disconnected
attempts, and $25 conservatively for prior work: $65 planned reservations inside
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
