# Hacker News example

This example runs historical Hacker News search with GCS-backed Parquet and
Antfly index artifacts using a local standalone process. It uses the existing
`colony-import-sources-antfly-dev-01` bucket in `us-central1`; all objects expire
after eight days. A durable deployment needs separate managed storage.

## Export a bounded sample

`export.sql` selects the latest 10,000 live stories/comments within September
2025, not a complete month. Change the export prefix before another export and
dry-run the SELECT first. On October 7 the public source had 49,949,001 rows and
19,754,132,371 logical bytes. LIMIT bounds output, not the BigQuery scan cost.

```sh
bq --project_id=antfly-dev-01 --location=US query \
  --use_legacy_sql=false --maximum_bytes_billed=21474836480 \
  --label=purpose:hn_lake_poc < examples/hackernews/export.sql
```

The raw BigQuery export uses standard Snappy Parquet with DataPageV1, dictionary
encoding and plain fallback pages. Its dictionary headers use the deprecated
`PLAIN_DICTIONARY` label; Antfly now accepts that label as well as `PLAIN`.
The untouched export passed this check after the compatibility fix.

`normalize.py` is optional: it decodes HTML/entities and writes DataPageV2
Parquet. This converter iterates batches and writes bounded row groups instead of
materializing the whole export. Keep `hn_id` for HN links. `parent_id` identifies the
immediate parent, which may be another comment; `ingest.py` resolves root stories through its durable on-disk
ancestry graph, including parents arriving after their children.

## Run the check

Build a standalone binary with the allocator, Parquet, filter and cache fixes.
The GCS allocator fix is [PR #1009](https://github.com/antflydb/antfly/pull/1009).
Without it, parallel GCS metadata reads can crash because transport responses
are allocated and freed with different allocators.

```sh
python3 examples/hackernews/poc.py \
  --binary /path/to/antfly \
  --state /private/tmp/hackernews-state \
  --prefix hn-poc/20261007 \
  --require-filters
```

Use a fresh local state directory when changing sources, artifact namespaces or
index declarations. The checker starts a
loopback-only process, creates `hn_archive_poc`, waits for publication, checks
10,000 SQL rows, ranked search, projected highlights, snapshot pagination, exact
HN-ID filtering with score ordering, and score/cursor stability after restart.
It declares relational indexes on `hn_id`, `created_at`, `item_type`, `author`,
and `points`. Type/author equality, date/points ranges and their conjunction are
checked against residual evaluation, then checked again after restart. Repeated
and four concurrent highlighted searches are also validated.
It stops the process and writes `report.json` into the state directory.

The checker obtains a short-lived token from the current gcloud identity and
passes it only through the child environment. Configuration contains
`${secret:HN_GCS_BEARER}` rather than a credential. This authentication is for the
smoke check; a service needs its own credential flow. Source and artifact
connections have separate prefix allowlists and capabilities (`lake_read` and
`storage.primary`). Existing human IAM grants are used. Index artifacts are in
`<prefix>/indexes` by default; use `--artifact-prefix` to isolate another run
without copying the source. Catalog state and the configured 1 GiB cache are local.
Use `--text-only` for the projected-scan baseline and `--cold-cache` to stop after
publication/count, remove only that state's cache, and start before measuring.

## October 7 results

`qualification.json` records observations from a local Debug build against GCS.

| 10,000-row sample | First highlighted query | Repeat | After restart | Exact ordered filter |
|---|---:|---:|---:|---|
| Normalized, allocator fix only | 26.4 s | 17.8 s | 27.5 s | Rejected; unordered timed out |
| Same normalized file, follow-up fixes | 22.3 s | 468 ms | 20.0 s | Passed |
| Untouched BigQuery export, follow-up fixes | 13.7 s | 452 ms | 15.3 s | Passed |
| Untouched export, cold/cache fixes, two empty-cache runs | 7.6–10.2 s | 379–400 ms | 418–432 ms | Passed |

Search previously cached inventory metadata but never attached the Parquet range
cache for hydration. Attaching it removed repeated remote reads on warm queries.
The subsequent cold/cache fixes remove an index-wide dictionary diagnostic scan
from range-backed reader admission, admit readers in bounded parallel jobs, and
fetch complete immutable artifacts with one bounded, checksum-verified GET.
Metadata hints overlap independent required header reads.

A clock-domain mismatch prevented disk-cache writes on macOS: capacity probes
use the native monotonic clock, while admission used Io's awake clock. Cache
admission now uses the probe's clock. Both new runs persisted 82 cache files
(about 5 MiB) and reused them after a real process restart. Pgwire normalizes its
awake-clock deadline before native catalog/storage boundaries; the six prior
cancellation cases now pass without longer timeouts.

The two new runs copied catalog data into a new state directory with no cache
files before startup. The harness SQL count precedes text search, so Parquet
metadata can already be warm. The text index was cold. Older first-query timings
followed publication. Readiness timing on an existing publication is not
index-build timing. Empty-cache queries still pay required remote I/O. These
results do not establish full-archive capacity or concurrent production latency.

A separate raw-export check also passed the previously timed-out unordered
HN-ID filter in 481 ms.

The example declares five HN metadata indexes. For flat structured predicates,
the lake planner compares a projected scan, an exact index range, intersections
of indexes, and a selective index followed by residual evaluation. It estimates
work from authenticated range cardinalities, projected Parquet bytes/rows, and
whether index metadata is resident. These are relative work estimates rather
than calibrated latency predictions. No user hint is required. Missing inventory
row counts remain unknown; missing projected chunk statistics use the full file
length as an upper estimate.

Index candidates borrow the current query's authorized artifact store and leased
publication. Losing candidates release their metadata handles without reading
result rows. A partial candidate set retains the full predicate for exact
ranking/counts; it is allowed only within the engine's configured exact candidate
budget. Sorting, cursor pagination, exclusions, and vector filters require
exact sets. Broad exact scans and indexes support match sets above 100,000 rows;
full-archive qualification still needs larger partitions. Nested paths retain the existing residual-filter path.

## Same-region qualification

Build the current main standalone binary for Linux, then use the existing dev
cluster in `us-central1`, matching the source bucket:

```sh
cd zig
zig build antfly -Dtarget=x86_64-linux-musl -Doptimize=fast -Dmetal=false -j2
cd ..
python3 examples/hackernews/regional.py \
  --binary zig/zig-out/bin/antfly \
  --revision "$(git rev-parse HEAD)" \
  --run-prefix hn-poc/NEW-UNIQUE-RUN \
  --output /private/tmp/hackernews-regional-NEW-UNIQUE-RUN
```

This creates one temporary pod in the existing `default` namespace, with two
CPUs, 8 GiB memory, and no mounted service-account token. It copies the executable
and harness into ephemeral storage, benchmarks text-only and metadata-indexed
tables on the same worker, and deletes only its own pod afterward. A 45-minute
pod deadline bounds a disconnected run. It creates no service or persistent
volume and changes no shared workloads, bucket policy, or IAM.

The runner streams a fresh short-lived token through encrypted `kubectl exec`
stdin into the harness; it is kept only in memory and the Antfly child environment.
The pod manifest, configuration and reports contain no token. This is test
credential handling; a durable service needs a dedicated workload identity.

Each mode runs two empty-cache cycles, five warm searches per cycle, four
concurrent searches, metadata filter/reference comparisons, and restart checks.
`regional.json` records the source revision, binary SHA-256, resolved container
image, node, admitted resources and raw per-cycle reports. Linux profiles record
Antfly CPU time and pod network byte deltas for serial queries. These include
TLS/DNS and background/control traffic; concurrent intervals overlap and are
not per-query byte counts. Results qualify the 10k sample only. Earlier local
Debug timings use a different platform and optimization level.

### October 8 results

[regional-qualification.json](regional-qualification.json) records two runs per
mode on one `us-central1` worker, using main `d2e9138f3f` and a Linux `fast` build.
All five metadata indexes published, with exact filter totals/ranked pages
matching residual evaluation before and after restart. HN IDs and scores also
matched across both table modes and cycles. The temporary pod was deleted.

| 10,000-row raw export | Empty-cache highlighted search | Warm median | After restart | Four concurrent searches |
|---|---:|---:|---:|---:|
| Text only | 4.37–4.94 s | 152–161 ms | 283–340 ms | 167–214 ms |
| Text + five metadata indexes | 3.86–4.17 s | 151–152 ms | 199–234 ms | 142–179 ms |

First publication took 5.5 seconds for text only and 48.8 seconds with the five
metadata indexes. Later cycles reused publication. Individual indexed metadata
filters took 213–241 ms warm; their conjunction took 753 ms. The text-only
projected scans took 141–159 ms on this small cached sample. These indexes
provide scalable predicate evaluation; the 10k check does not demonstrate a
filtering latency win. These measurements precede the automatic predicate planner
and shared query-scoped index setup.

Cold searches received 5.7–7.4 MB and used 0.65–1.06 seconds of aggregate CPU.
Their reported execution time was 1.2–1.7 seconds, leaving 2.6–3.2 seconds outside
that timer. Warm searches received about 18.5 KB; retrieval/ranking/hydration
took only 4–9 ms of a roughly 150 ms HTTP request. These counters, together with
the current query path, suggest source/publication preparation and repeated
remote validation are the next latency targets. They do not identify individual
GCS requests. Index setup should share the authorized store and pinned snapshot
within a request; reusable clients and validation caches must preserve credential,
source-version and cancellation boundaries.

Cache directories were removed before process startup for each cold cycle.
One indexed cycle had one background-created file before its first query.
Post-restart reads reused the persisted cache and received 33–97 KB for text
only and about 38 KB with metadata indexes, rather than downloading megabytes.

### Automatic predicate planner results

[planner-qualification.json](planner-qualification.json) records the follow-up
on `10e36b97f1`, using the same raw export, GKE node, Linux `fast` optimization,
and pod resources. Each mode ran twice with five warm samples per filter in
each cycle. The table pools those ten samples per filter:

| Indexed filter | Before planner | After planner |
|---|---:|---:|
| Item type equality | 220 ms | 144 ms |
| Author equality | 240 ms | 156 ms |
| Points range | 217 ms | 156 ms |
| Creation-time range | 223 ms | 141 ms |
| Four-field conjunction | 753 ms | 142 ms |

These timing measurements predate the sorting/pagination and missing-statistics
review fixes; the recorded binary revision remains the basis for this comparison.
The conjunction improved about 5.3×; its reported native execution median fell
from 612 ms to 11 ms. All filter totals and ranked pages matched the residual
oracle before and after restart. HN IDs and scores were identical across both
modes, all cycles, and the before/after binaries. The temporary pod and its
local Kubernetes credential directory were removed.

The indexed table's unfiltered highlighted search took 3.92–4.36 seconds cold,
146–152 ms warm, and 203–216 ms after restart. Most warm request time still sits
outside the native execution timer. This fix removes repeated predicate setup;
remote source/publication preparation remains a latency target. Cache was empty
before startup; one cycle per mode had one background-created cache file at
its first search. These are 10k-row measurements, not archive capacity results.

## Remaining deployment work

- Reduce remote source/publication preparation, preserving snapshot and cancellation guarantees.
- Qualify larger partitions against build/corpus limits and indexed filtering.
- Define durable buckets and service identities through Colony's infra workflow.
- Stream backfills, resolve parent stories, and reconcile edits/deletions.
- Build Recent/Historical routing and the public search interface.


## Million-row qualification

`export-scale.sql` exports the newest million live story/comment rows between
January and September 2025. Use a fresh prefix and a BigQuery dry run before
execution. October 8's export processed 19,753,352,019 bytes, produced exactly
1,000,000 rows, and wrote 449,870,756 compressed bytes. The job was capped at
21,474,836,480 billed bytes; LIMIT does not reduce the scan.

The runner accepts explicit scale, build and resource budgets:

```sh
python3 examples/hackernews/regional.py \
  --binary /path/to/linux-antfly --revision EXACT-SOURCE-REVISION \
  --source-prefix hn-poc/20261008-scale --expected-rows 1000000 \
  --run-prefix hn-poc/NEW-UNIQUE-SCALE-RUN --output /tmp/hn-scale-results \
  --build-timeout 1800 --lifetime 7200 --cpu 4 --memory 16Gi --disk 16Gi
```

Reports include readiness time, server peak RSS during construction, construction
CPU/network deltas, cold/warm/restart timings, filter-oracle comparisons,
pagination and concurrency. RSS is the Antfly process high-water mark, not a
sum of all pod processes; network counters include control/background traffic.
Use identical resources and source when comparing binaries. Increasing the build
budget does not change the existing 15-second unordered-filter regression budget.

## Durable historical ingestion

Antfly serves search from GCS Parquet/Iceberg data and its own indexes. The
Python example ingestor uses Antfly Lite: one `ingestion.aflite` file holds the
item map, retry queues, ancestry key ranges, progress checkpoints, and PyIceberg
writer catalog. The embedded `antfly-embedded` Python binding drives native synced
batches and bounded key scans. There is no SQLite database or SQL catalog.
The Lite writer runs outside the public search request path and needs a
persistent volume plus stable-snapshot backups.

Build `zig build capi` and set `ANTFLY_LIBRARY` to the built shared library
when running from this source checkout. Published embedded platform wheels bundle
the library. Use a dedicated Iceberg warehouse and a persistent state volume. The ingestor
uses synced atomic Lite batches, streams Parquet input batches,
deduplicates by HN ID, checkpoints source content hashes and offsets, and retains
retry work before advancing its API cursor. Export full records (including
`dead` and `deleted`) for a durable backfill; the qualification SQL intentionally
contains only live rows. Its retained item map preserves tombstone ancestry.

```sh
uv run --project examples/hackernews python examples/hackernews/ingest.py \
  --state /data/hackernews --warehouse gs://hackernews-archive-antfly-dev-01/items \
  backfill /data/imports/part-*.parquet
uv run --project examples/hackernews python examples/hackernews/ingest.py \
  --state /data/hackernews --warehouse gs://hackernews-archive-antfly-dev-01/items \
  run
```

Polls fetch new IDs, the official HN API's recent updates and a bounded rolling
reconciliation sweep. [`updates.json`](https://github.com/HackerNews/API) is a
recent-changes feed, not a durable log; the sweep catches missed moderation and
edits after downtime. Catch-up time depends on the sweep budget and archive size.
Null responses remain queued for retry. Missing ancestors are fetched in bounded
batches; unresolved parents/cycles produce a null `root_story_id`, never an
invented root. Complete live records can clear earlier moderation flags.

Affected months are replaced using bounded Parquet writes and one Iceberg
transaction. Publication creates immutable metadata and uses a generation-match
CAS for `metadata/version-hint.text`. A persisted publication journal recovers
failed/lost pointer writes. Attach Antfly to the **warehouse directory**, with
`format: iceberg`, to follow commits; an explicit metadata-file attachment pins
that one metadata file. Existing native index reconciliation detects a changed
snapshot and publishes matching sidecars; queries cannot reuse a mismatched
publication. Declare `object_mutability: immutable` for this writer's data objects: each
Parquet file has a new immutable URI, so Antfly can reuse authenticated data-file
version proofs while it still reads the fresh Iceberg commit pointer. The pointer
changes; existing data files do not.

Archive publication defaults to hourly, separately from one-minute API polling.
This initial implementation replaces changed months and retains old snapshot
objects, so it has write/storage amplification. It is suitable for a batched
historical tier; a high-frequency whole-archive service needs file-level upserts,
compaction and a reviewed retention policy. Recent native-table ingestion and
the public UI remain separate work. No automatic snapshot deletion is included.

The single Lite state file must live on a persistent volume owned by
one writer. Take a streamed, consistent off-volume checkpoint after publication:

```sh
uv run --project examples/hackernews python examples/hackernews/ingest.py \
  --state /data/hackernews --warehouse gs://hackernews-archive-antfly-dev-01/items \
  --backup-root gs://hackernews-state-antfly-dev-01/ingestion backup
```

`restore` uses the same arguments and requires an empty state directory. It
checks file digests/Lite integrity and refuses an older checkpoint if the
archive has advanced: reconcile the current catalog and source state before
recovery in that case. Backups are explicit, not automatically scheduled.
Native serving requires Parquet Iceberg field IDs; the writer emits those IDs
from the Iceberg schema on every batch. GCS uses application default credentials; GKE uses the dedicated ingestor
Workload Identity. Keep credentials out of state, image layers and reports.

Run local correctness tests with the existing lake/iceberg E2E environment:

```sh
uv run --project zig/e2e/antfly --extra lake --extra iceberg pytest -q examples/hackernews/tests
```

## Native managed or REST catalog

The native table's `base_source.catalog` selects either `{"type":"managed"}`
or an external REST catalog. Configuration and durability tradeoffs are described
in [Native Iceberg catalog authorities](../../docs/design/lake-catalogs.md).
Generate a complete HN table definition with
`configure_catalog.py --warehouse gs://BUCKET/hackernews --source-connection hn_source --mode managed`.
For REST, use `--mode rest --rest-connection hn_catalog --rest-uri https://catalog.example.com`
and optionally set `--rest-namespace`, `--rest-name`, and `--rest-warehouse`.
POST the resulting JSON to `/db/v1/tables/hackernews`.

Create the Antfly table first with explicit HN document columns,
`write_policy: iceberg_writer`, a table-root URI and an `auto` schema fingerprint.
The first native catalog initialization finalizes that fingerprint.

```sh
uv run --project examples/hackernews python examples/hackernews/ingest.py \
  --state /private/tmp/hackernews-writer \
  --warehouse gs://YOUR-DURABLE-BUCKET/hackernews \
  --native-endpoint http://localhost:8080/db/v1 \
  --native-table hackernews run
```

Set `ANTFLY_API_KEY` in the worker environment when API authentication is enabled.
Both modes use PyIceberg to produce bounded Parquet files and manifests, then
commit through Antfly. The worker retains exact pending requests in Antfly Lite
before sending them. A lost response is recovered before another plan is built.
Native mode does not write `version-hint.text`; the selected native catalog is
the sole authority. Backup and restore compare its immutable metadata location
and table UUID, and reject a checkpoint after the archive advances.

Catalog commitment and searchable text/predicate publication are separate
recoverable steps. Native commits wake the supervised publication worker. The
`--native-rows` mode below additionally uses Antfly's WAL-to-Parquet writer;
additional vendor-specific CDC adapters remain extensions of the ingestion design.

Native catalog wire qualification (local S3 protocol fixture and independent
PyIceberg REST authority, with real manifests/Parquet and daemon restarts):

```sh
ANTFLY_NATIVE_BINARY=/path/to/antfly ANTFLY_LIBRARY=/path/to/libantfly.dylib \
  uv run --project examples/hackernews pytest -q examples/hackernews/tests/test_catalog_http.py
```

Native row ingestion is available with `--native-rows --native-endpoint
http://localhost:8080/db/v1 --native-table hackernews`. For a continuously
running producer, also set `--publish-interval 60`; each pass sends a bounded
transaction instead of replacing affected months. Antfly handles the durable
WAL, Parquet and Iceberg equality deletes, the configured managed or REST
catalog commit, and automatic text/predicate index publication. The local
Antfly Lite state keeps the exact pending request and per-item changes across
restarts. Acknowledged rows are removed from that queue only if no newer local
version has replaced them.

The native table must use the generated writable catalog binding and the node
must configure an independent artifact storage lane. The first publication
initializes an absent catalog with the HN schema. The producer persists one
source epoch and chains opaque checkpoints. Switching a pending request to a
different endpoint/table is rejected. Durable acceptance is distinct from
search visibility; use catalog coverage and index readiness to observe progress.

## Native recent search and maintenance

Each worker targets one logical table. Separate current/history workers can use
the fixed creation-time cohorts described below; rolling cutoff migration is
not automatic. The recent overlay below is bounded pending publication work.
JSON source composition, SQL/vector visibility and automated maintenance are described in
[Composed query sources and recent/archive visibility](../../docs/plans/composed-query-sources.md).

`--native-rows` sends complete transactions to Antfly's durable lake WAL. Once a
baseline text/predicate index is published, native text search also includes the
accepted WAL suffix while Parquet/catalog/index publication catches up. Stable
keys hide replaced/deleted archive rows before ranking. The overlay survives
restart; direct SQL SELECT can opt into accepted WAL visibility. Search cursors
expire explicitly if their archive/WAL cut changes.

Use the table admin maintenance endpoint for bounded jobs. Dry runs are the
default; reuse an operation ID to resume the exact operation after interruption.
Compaction commits automatically wake searchable publication.

```sh
ANTFLY_URL=http://localhost:8080/db/v1
curl -fsS -X POST "$ANTFLY_URL/tables/hackernews/lake/maintenance" \
  -H 'Content-Type: application/json' \
  -d '{"action":"compact","operation_id":"hn-compact-001","dry_run":false}'

curl -fsS -X POST "$ANTFLY_URL/tables/hackernews/lake/maintenance" \
  -H 'Content-Type: application/json' \
  -d '{"action":"wal_gc","operation_id":"hn-wal-gc-001","dry_run":false,"max_deleted":128}'

curl -fsS -X POST "$ANTFLY_URL/tables/hackernews/lake/maintenance" \
  -H 'Content-Type: application/json' \
  -d '{"action":"vacuum","operation_id":"hn-vacuum-plan-001"}'
```

File vacuum requires an explicit native-owned storage lifecycle and retention
agreement for external readers before setting `exclusive_ownership: true` and
`dry_run: false`. Antfly pins its own active readers and deletes only unreachable
files with native ownership proofs. External/unmarked files remain untouched.
The example bucket's eight-day lifecycle is independent of those pins, so it is
unsuitable for durable deployment. See the
[design's maintenance contracts](../../docs/plans/lake-ingestion-and-publication.md#compaction-and-garbage-collection)
for bounds, snapshot retention and restart recovery.


## Composed current and historical search

The global query endpoint can combine two existing tables in one result set:

```sh
curl -fsS -X POST "$ANTFLY_URL/query" -H 'Content-Type: application/json' -d '{
  "source":{"union":[{"table":"hackernews_current"},{"table":"hackernews_history"}]},
  "source_ranking":"rrf",
  "full_text_search":{"match":"distributed databases","field":"body"},
  "limit":20
}'
```

Union preserves both copies of overlapping records and includes `_table` provenance.
Use disjoint creation-time cohorts, routing edits and deletes to the owning table,
or explicitly configure `source.overlay` with retained latest rows/tombstones in
the changes table. Run separate ingestion workers with separate state directories and immutable
`--created-before CUTOVER_EPOCH` (history) / `--created-after CUTOVER_EPOCH` (current)
boundaries, together with `--native-rows`, separate `--native-table` values and
separate warehouse roots. `configure_catalog.py --table-id` gives each source its
own identity. Both workers retain normalized source rows so partial moderation
responses preserve original creation time. Missing creation time fails until
reconciled. Cohort changes on a used state directory are rejected; the worker
does not provision a rolling date boundary or migrate rows automatically.
The HTTP fixtures exercise an independent historical/current pair and keyed
precedence, including a newer nonmatching edit and a retained deleted record.

RRF uses independent visible source ranks, not shared BM25 statistics. Union and
keyed overlays stream bounded 128-hit source pages; result pages allow up to 4096
hits without a fixed archive-position horizon. Large overlay totals are lower
bounds unless `count: true` explicitly streams an exact count. Pass
`next_source_cursor` as `source_cursor` on the next request. Lake cuts retain the
same archive publication and accepted WAL images across publication and restart
for the configured cursor retention period; authorization and incarnation are always rechecked. Read the
[composition contracts](../../docs/plans/composed-query-sources.md#implemented-contracts)
before exposing all-time search over a full archive.

Save the logical source once in the native Antfly catalog:

```sh
curl -fsS -X POST "$ANTFLY_URL/sources/hackernews" \
  -H 'Content-Type: application/json' -d '{
    "source":{"union":[{"table":"hackernews_current"},{"table":"hackernews_history"}]}
  }'
curl -fsS -X POST "$ANTFLY_URL/query" -H 'Content-Type: application/json' -d '{
  "source":{"saved":"hackernews"},"source_ranking":"rrf",
  "full_text_search":{"match":"distributed databases","field":"body"},"limit":20
}'
```

Saved definitions are immutable; drop/recreate to change an expression. Reads need
permission on the saved source and each underlying table. For overlapping edits,
save an overlay instead and retain tombstones in its changes table.

Direct HTTP SQL SELECT can use `"lake_visibility":"accepted"` to include the
bounded durable WAL suffix after restart; committed visibility remains the default.
Search can use `"lake_read":{"visibility":"published"}` to explicitly select a
published archive, or add a `through` receipt from `/lake/changes` and `wait_ms`
to require minimum coverage. Accepted vector queries use coherent recent native
dense/sparse segments; managed text embeddings are computed by a durable retrying background job before Parquet
publication. Check `{"action":"enrichment_status"}` on the maintenance endpoint
for the completed WAL cut or retry/error state.

Set the Iceberg string property `antfly.maintenance.policy` to a JSON policy such
as `{"enabled":true,"compact":true,"wal_gc":true,"vacuum":false}` to enable
bounded supervised maintenance. Query durable policy/progress with:

```sh
curl -fsS -X POST "$ANTFLY_URL/tables/hackernews/lake/maintenance" \
  -H 'Content-Type: application/json' -d '{"action":"status"}'
```

Scheduling resumes saved operation requests after restart. It retains the existing
job limits and ownership/reader protections; automatic REST-catalog vacuum remains
disabled because external metadata writers need a separate retirement protocol.

The generated full-text index consumes the row schema so its declared sortable
columns have native doc values. Searching with `field: "body"` still restricts
text matching to that field. A field-specific text index consumes only that field
and cannot sort by an unrelated column merely because a predicate index exists.


### Chunk enrichment and native current-table cursors

The archive and accepted-WAL builders also accept managed embedding indexes with
`template` and `chunker` configurations. They use Antfly's native multimodal and
chunking providers and store independently addressable chunk vectors, original
source units and offsets in the published checkpoint. HN's text fixture remains
text-only; it does not require a paid media model. Sparse indexes support text
chunks; media embeddings require a compatible dense provider.

A mutable native current table can participate in the same saved union/overlay
source. Ordered native queries return a `native2:` `remote_snapshot`; echo it with
`_sort` as `search_after`/`search_before`. Composed continuation stores each native
leaf capability automatically. Later writes/deletes and a daemon restart preserve
that generation until its configured expiry (five minutes by default). Both `primary_lsm` and `vector_store`
source-vector ownership are supported on filesystem-managed LSM owners. Missing
authoritative artifacts, expiry or catalog/recipe changes produce a conflict.
`tests/test_native_cursor_http.py` qualifies mutations and restart against the real
standalone daemon for both storage settings. Full historical HN deployment and
archive-scale latency still need operational qualification.


### Native snapshot portability qualification

Native ordered queries publish their complete retained document/vector generation
through the artifact provider before admitting a cursor. Replacement owners can
restore missing local cuts from filesystem, S3 or GCS artifacts while preserving
the original query view. Missing authoritative data returns 409. Local cut files
are a read cache, and credentials can rotate without changing native cursor scope.
Set `lake_indexes.query_cursors.retention_ms` (default 300000, maximum 3600000),
`max_native_cuts` (default 64) and `max_native_retained_bytes` (default 64 GiB).
Shared immutable local extents count once; immutable remote chunks are reused
across captures. Retention does not renew on pagination.

The native HTTP fixture also supports opt-in qualification through a real GCS
bucket. Each test uses a fresh prefix and removes its artifacts afterward:

```sh
ANTFLY_NATIVE_BINARY=/path/to/antfly \
ANTFLY_NATIVE_CURSOR_GCS_BUCKET=colony-import-sources-antfly-dev-01 \
  .venv/bin/python -m pytest -q tests/test_native_cursor_http.py
```

It uses the caller's existing `gcloud` credentials. The token is passed to the
daemon through its environment and is never written to configuration or results.

`export-full.sql` prepares full-archive search qualification in `antfly-dev-01`.
The authorized incremental budget is $150; the dry run estimated 19,372,806,880
bytes. The pinned October 9 export completed with 47,717,307 live story/comment
rows in 334 Parquet objects (21,195,813,507 compressed bytes). Full-archive index
and latency qualification and an HN public deployment are still outstanding.
For a full archive, use `regional.py --modes indexed --cycles 1` to give the
metadata-index build its own bounded pod lifetime. Run `--modes text-only` with
a separate artifact prefix afterward. The default runs both modes in one pod;
its lifetime must cover both builds and their queries. Single-mode reports mark
`cross_mode_comparison` false, so they do not establish agreement between modes.
The first durable generation's transfer latency and automatic background warming
must be qualified separately from ordinary unordered search latency.
