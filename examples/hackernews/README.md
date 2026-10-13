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
Compaction commits automatically wake searchable publication. Compaction now
resumes large files across bounded turns; repeat the same request until
`complete: true`. `scanned_rows` counts physical input progress, including deleted
rows. `conflicted: true` means the parent changed and no rewrite was published;
start a fresh operation ID so it rereads the current parent.

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
the original query view. Version-2 capabilities retain the original ordered range
cover; continuation uses one current carrier and separately reads each original
range once after a split or merge. Version-1 capabilities without the cover require
a new query. Routing and two-origin storage tests cover this contract; real
multi-node topology-change qualification remains outstanding.
Missing authoritative data returns 409. Local cut files
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
The first regional full-archive attempt registered the source in 51.7 seconds,
then repeatedly hit `PoolExhaustedForHost` during publication; both temporary
pods were stopped. HTTP/1 request admission now waits for released capacity with
the original request deadline and cancellation, instead of failing immediately
when lake coverage submits more checks than the per-host pool limit. Full-archive
qualification must be repeated with that change before reporting capacity or latency.
For a full archive, use `regional.py --modes indexed --cycles 1` to give the
metadata-index build its own bounded pod lifetime. Run `--modes text-only` with
a separate artifact prefix afterward. The default runs both modes in one pod;
its lifetime must cover both builds and their queries. Single-mode reports mark
`cross_mode_comparison` false, so they do not establish agreement between modes.
Use `--cursor-retention-ms 3600000` when archive-scale filter checks need more
than five minutes before the retained restart-pagination check. Reports include
the configured retention window.
The first durable generation's transfer latency and automatic background warming
must be qualified separately from ordinary unordered search latency.

## Full-archive deployment qualification

The merged engine is qualified separately from the public HN deployment. See
[deployment gates](../../docs/plans/hackernews-deployment-qualification.md) for
performance targets, source freshness and durable deployment requirements.

Capture only authenticated Parquet footers, including immutable object versions:

```sh
python examples/hackernews/archive_inventory.py \
  --prefix hn-poc/20261009-full-portability-2b1f7524d047/archive \
  --output /private/tmp/hn-archive-inventory.json
```

Build merged main for Linux using `-Dtarget=x86_64-linux-musl -Doptimize=fast
-Dmetal=false -Dstrip=true`. Print the bounded plan before executing:

```sh
python examples/hackernews/archive_qualification.py \
  --inventory /private/tmp/hn-archive-inventory.json \
  --binary /path/to/linux/antfly --revision FULL_SOURCE_REVISION \
  --ledger /private/tmp/hn-archive-budget.json \
  --output /private/tmp/hn-archive-attempt-01
```

Add `--execute` to verify the live inventory and start the temporary regional
pod. Use the same ledger for retries; failed/interrupted runs consume their
reservation. Results include the raw logs and a failing verdict when construction
or latency gates fail. Never interpret an index-build failure as successful
archive qualification. The configuration targets the pinned 47,717,307-row
export; an expired or changed source requires a newly reviewed inventory/config.
The temporary bucket is unsuitable for the public service's durable storage.

### October 10 full-archive results

[archive-results-20261010.json](archive-results-20261010.json) records the pinned
47,717,307-row raw export and the merged-main Linux `fast` attempt. Registration
took 49.6 seconds, then publication failed with `ParquetRowGroupTooLarge` before
SQL counts or search timings could be measured. The temporary pod and its 36
small derived objects were deleted; the source export was preserved.

The follow-up decoder separates column-vector ownership from physical row
references. Streaming cursors create references once per bounded output batch
instead of retaining a complete reference array for each column page. Existing
materialization limits remain in force. A 12-column, 145,186-row regression
covers cached/uncached reads and correct references across 4,096-row batches;
one column's measured allocation peak was 4,065,504 bytes within its combined
5,592,404-byte payload/structure allowance. The 132 focused Parquet and 31 lake
SQL tests passed.

The fixed optimized rerun passed the original decoder failure but stopped during
construction with `SqlProgramLimitExceeded` after 195.3 seconds, with 174 MiB
peak RSS. Registration took 41.9 seconds. Static inspection points to the
archive-wide replay capture inheriting SQL’s 1 GiB spill limit; the runtime
error does not identify the exact limit site. The second pod and 78 derived
objects were deleted. No search latency was measured in either attempt.

The follow-up now bounds replay and metadata sorts by file, updates text
statistics by file, and saves complete file contributions in a separate
non-serving catalog checkpoint. Retries use a fresh lease and retain completed
work; both vacuum paths protect checkpoint roots. The local replay optimization
falls back to streaming for an oversized file. Per-file index admission remains
enforced. Optimized archive construction and query benchmarks are being rerun.

The original two attempts consumed $65 in conservative allowances including
prior work. The follow-up now permits four additional attempts, up to $145 of
allowances within the unchanged $150 ceiling. One is a separately labelled
142,577-row raw-file search diagnostic; full-archive gates remain unchanged.
`--extend-run-limit` records each extension in the existing ledger; it cannot
reset failed reservations or change spend limits.

The bounded archive attempt ran 841.6 seconds before another
`SqlProgramLimitExceeded`, at 267.5 MiB peak RSS. A shared spill-capacity
regression exposed the predicate builder retaining too many runs alongside
fused sorts; that builder now retains four runs. Its synchronized admission
diagnostics identify spill file and byte peaks on the next attempt. The
142,577-row diagnostic stopped at its first `Timeout` after 640.2 seconds,
before searches. The harness now records transient timeout observations and
waits for queued checkpoint recovery under the original build deadline.
Permanent admission errors still fail immediately. Neither result establishes
archive latency.

The fifth full-archive attempt used the predicate-capacity fix, but stopped at
`Unauthorized` after 670.2 seconds (218.9 MiB peak RSS), before searches.
Cached credential expiration is plausible; original expiry evidence is unavailable.
The regional harness now forces one fresh token per pod, checks its lifetime
against the remaining pod deadline, and reuses it across cycles. Only expiry
metadata is persisted. All five pods and 22,946 failed derived objects are
removed; $125 remains reserved in the shared ledger. The final reserved attempt
is a separately labelled one-file search diagnostic, not archive qualification.

The final raw-file diagnostic stopped at the original 45-minute build deadline,
after 38 engine `Timeout` events, with 350 MiB peak process RSS. It reached
generation 39 and reused all five metadata child-tree sets across retries; the
text index stayed incomplete. SQL count and search timings were never reached.
The six owned pods, 50,069 failed derived objects and temporary source copy are
removed; the original full export is preserved. All six allowances total $145
in reservations, not measured billing.

Native-client probes in the same pod verified full GCS uploads and media reads
up to 8 MiB, narrowing the failure investigation. Their traffic overlaps build
network counters and does not qualify search latency. New publication/checkpoint
and text-stage diagnostics preserve errors without logging source text or
credentials. Those diagnostics were not in the measured binary. The timeout was subsequently reproduced and fixed as described below;
reconcile actual cloud charges before further regional attempts. The 160 lake regressions,
22 harness tests and keep-alive TLS upload regression pass.


A focused production builder subsequently reproduced `Timeout` in
`segment_statistics` while uploading a GCS page. The TLS writer discarded a
plaintext tail when record framing filled the ciphertext buffer. Flush now
consumes only encrypted bytes and repeats until empty, without increasing engine
HTTP timeouts. An exact-buffer regression fails before the fix and passes after
it with TLS 1.2 and TLS 1.3; all seven focused HTTPS tests pass.

The fixed native optimized diagnostic reached 1,995 GCS artifacts (139,903,056
bytes) without an engine timeout before its local 300-second bound. Those
artifacts were generation-deleted after stopping the reader. This body-only,
testing-allocator production-builder diagnostic uses unchanged raw Parquet and
real GCS artifacts, but excludes the serving API and public search measurements.
The shared ledger now reserves the complete $150 ceiling: $145 for earlier work
and six pod attempts, plus $5 for local GCS diagnostics. Local diagnostic
reservations count against the same harness budget. Billing exports are still
too stale to release allowances. Full-archive and search latency remain
unqualified.

The subsequent 600-second completion check also hit its local bound before
publication: 2,265 artifacts, 157,341,017 bytes, no engine timeout logged. A
one-second stack sample placed remaining work in document-frequency statistics
reading pages through GCS. Complete GCS publication remains unverified; this
sample does not attribute all elapsed time or qualify regional performance.
All 2,265 artifacts were generation-deleted after the builder stopped; the
original archive source was preserved.


### Reconciled follow-up admission

Completed pod compute can be reconciled from a reviewed detailed billing export
without erasing original reservations or failed attempts:

```sh
python examples/hackernews/archive_budget.py \
  --ledger /private/tmp/hn-archive-budget.json \
  --evidence examples/hackernews/archive-reconciliation-20261011.json
```

Evidence must identify a deleted pod, the matching ledger run, closure and export
coverage timestamps, gross CPU/memory/ephemeral-storage charges, and the retained
noncompute allowance. Promotion credits do not reduce exposure. Repeated evidence
is idempotent, costs cannot decrease, and unsettled local diagnostics still count.
The October 11 reconciliation releases $59.072894 in excess compute reservations,
leaving $90.927106 held or recorded, including $60 for the six runs' unexported or
shared noncompute costs, $25 for earlier work and $5 for local diagnostics.

Use `archive-qualification-followup.json` with the same reconciled ledger and
`--extend-run-limit` for two additional attempts within the original $150 ceiling.
Its plan includes prior reconciled exposure; it must not be admitted against a
fresh ledger. Original attempts and extension records remain auditable.
The deep cursor workload uses `AI`, which the standard analyzer retains and
which matches 4,110 rows in the qualification partition. This exceeds the
4,097-hit gate; stopwords such as `the` cannot establish that coverage. Restart
checks replay the last nonempty deep page.

The follow-up also requires real public-API checks across historical and current
tables: union visibility, suppression of a historical match by a nonmatching
current replacement, saved-source visibility, and replay of a captured composed
cursor after current rows change and the daemon restarts. These are separate
correctness requirements; a successful single-table search cannot substitute
for them. The current rows are synthetic qualification data, not a live HN feed.

The stored-source and larger bulk-page run at engine revision `9663fe3bdc`
completed two same-region cycles on the unchanged 142,577-row partition. Initial
publication took 424.38 seconds; reopening the same publication took 0.2 seconds.
Exact SQL counts, indexed/residual filter agreement, concurrent searches,
historical/current replacement semantics, and restart/cursor correctness passed.
Restart searches took 703–712 ms. Cold highlighted queries still took 15.9–18.0
seconds, and early deep cursor pages exceeded the 2-second p95 gate. This is a
**failed** one-file performance qualification; see
`publication-stored-source-20261012.json`. The owned pod and all 7,429 derived
objects were removed with generation conditions; source data was preserved.

The harness exposes health metrics only on the daemon's loopback host at the
query port plus one. For selected serial phases it waits for the existing
five-second metrics refresh before and after each measured query, outside the
reported latency, and records process-level phase/cache-counter deltas. Nested
phase durations can overlap; background traffic and concurrent queries prevent
these counters from being a per-query provider trace.

The final optimized Linux binary also passed both native storage profiles through
the public HTTP recovery tests. An incomplete local cut directory now validates
as a cache miss and opens the same authenticated remote generation; loss of
remote authority still fails closed. Malformed manifests, missing sealed files,
and active-reader admission are covered by engine units. Exact validation layers
and binary provenance are in `native-recovery-qualification-20261012.json`.

New seekable text publications can bundle their opening metadata pages into one
authenticated object per segment. The bundle stays below 1 MiB and below half of
the original segment size; oversized or small segments keep the existing range
layout. The physical owner charges retained bundle bytes to its publication
allocator and reports them to residency accounting. Under memory pressure, reads
fall back to verified range leases. Segment-native offsets, page CRC checks,
scoring, legacy directories and cursor identities stay unchanged. This adds a
bounded duplicate of opening pages to object storage in exchange for fewer cold
GETs. Existing publications remain readable; rebuilding produces bundled layout.
The native regression opens all declared field navigation with exactly two
provider reads (directory and bundle), checks exact scores and the full byte
round-trip, rejects a substituted range identity, and tests residency denial.
Regional performance of this change must still pass the qualification gates.

The metadata bundle binary at revision `89ccd33a2f` passed both public native
recovery profiles and completed two regional cycles. Cold searches improved to
12.1–13.9 seconds but still missed the 10-second gate; early cursor pages also
missed their latency gate. Restart search took roughly half a second. See
`publication-metadata-qualification-20261012.json`; this remains failed
performance evidence. Phase counters placed most residual cold time in ranking
and initial deep-page time in hydration.

Lake text candidate identities can now come directly from the authenticated
publication's ordinal-to-physical-row map. This avoids fetching stored-key pages
for every matching candidate before final sorting and preserves canonical public
IDs, delete-hole rank selection, scores and cursor ties. Legacy/recent/enriched
paths retain stored-key projection. Final stored-source hydration advises a
four-document lookahead through the current snapshot's existing bounded source
queue; required reads retain their integrity and cancellation checks.

Budget review can adjust future run deposits using
`archive_budget.py --replan-future-reservation --ledger <ledger> --evidence <review>`
with `kind: reviewed_future_run_reservation`, the same project, a new deposit and
resource limits. The deposit must cover two whole hours at those resource limits
plus the unchanged noncompute allowance, and fit the existing spend ceiling.
The review is hash-audited, changes only future deposits, preserves original run
reservations and costs, and constrains both planning and direct reservation.
It does not establish actual billed spend.

Regional benchmark jobs run under a detached, singleflight supervisor. Launches
and status checks use short control-plane calls; a lost launch reply cannot start
a duplicate job, and stdout disconnection cannot terminate the benchmark. The
worker records terminal status and output on its owned ephemeral filesystem.
Credential material is sent only through bounded stdin pipes and is absent from
job descriptors, logs and completion records. Results are copied after confirmed
completion, with bounded retries; the pod still has a hard lifetime and cleanup
checks its ownership and incarnation.

`archive_qualification.py --resume-from <failed-run>` admits at most one
replacement of an unsettled attempt that failed specifically at the Kubernetes
exec transport. It requires confirmed deletion, the same configuration, inventory
and engine binary, and no recorded engine failure. Original run/deposit records
stay intact; an append-only resume admission bounds the aggregate lifetime of
both pods to two hours, including scheduling, copy and termination allowance.
It cannot add another reservation, refund exposure, bypass a settled attempt or
repeat a consumed replacement. The failed original evidence remains available.
Repeated launch/poll failures also retain an atomic `*-transport-failure.json`
record before the controller exits, even when it cannot copy the worker log.
It contains classified reasons and pod/job identity, without raw stderr, command
arguments or credentials. Resume verifies terminal transport evidence against
the retained job descriptor and pod incarnation, and records its digest in the
existing reservation audit. Recovered errors and unclassified failures cannot
admit a replacement. Older attempts without structured records retain their
original connection-reset log admission.

The ordinal/lookahead binary completed local public recovery checks for both
storage profiles. Its first regional cold search measured 7.0 seconds and early
continuation pages roughly 0.8–1.3 seconds, but the control-plane stream reset
while returning results. These partial observations are not a passing verdict.
A complete supervised two-cycle run is required before readiness.

The optimized ordinal/lookahead binary at revision `048692502a` completed both
regional cycles against an unchanged 142,577-row BigQuery Parquet partition. Both
cycles passed every configured correctness and performance gate with stored-source
highlights and empty caches before the first search. Cold queries took 6.35–7.82
seconds, warm p95 was 279–280 ms, restart search was 395–426 ms, and continuation
cursor p95 was 1.20–1.22 seconds across all 4,110 matching rows. Filter p95 stayed
below 484 ms; concurrent requests stayed below 527 ms. Historical/current union,
replacement suppression, saved visibility and mutable composed cursor restart
checks passed. Initial publication took 449.62 seconds plus 1.64 seconds of
registration; reopening the same publication took 0.22 seconds.

See `publication-ordinal-qualification-20261012.json` for binary and harness
provenance, gate thresholds, sample summaries and retained-report hashes. The
first attempt lost its Kubernetes output stream and supplied only partial evidence;
the detached supervised replacement supplied the complete passing verdict. The
owned worker was deleted and derived publication artifacts were retained. Initial
128-hit cursor pages took 1.97–2.28 seconds; the cursor gate measures continuation
p95 and restart. One first `postgres` request took 2.32 seconds, while its workload
p95 passed at 292 ms. This qualifies one partition, not the full archive or an
Antfly Cloud deployment. The same binary also passed both public filesystem native
recovery profiles. Final PR CI still requires human approval of the final revision.

## Local search comparison before cloud qualification

Use the same unchanged Parquet partition for both engines, prebuild both text
indexes, and measure after builds finish. Prefer native binaries of the same
architecture on the same host. CPU translation, background test runs, thread
limits and OS file-cache state materially affect cold measurements. Antfly's
`--cold-cache` clears only the benchmark state's serving cache; it does not flush
the OS cache. DuckDB's first sample starts in a fresh process with an uncontrolled
OS cache. Subsequent terms follow the first term in that process.

Install the optional pinned DuckDB dependency with `uv sync --group comparison`
from this directory. Put one partition in `<work>/source/archive/part.parquet`.
The following commands run from the repository root with the HN Python environment
active; set `ANTFLY_BINARY` to an optimized native build for this host.

```sh
python examples/hackernews/search_comparison.py \
  --source <work>/source/archive/part.parquet \
  --database <work>/duckdb-fts.duckdb --output <work>/duckdb-fts.json \
  --expected-rows 142577 --threads 4 --repeats 20 --cycles 2 --build

python examples/hackernews/poc.py --binary "$ANTFLY_BINARY" \
  --filesystem-source <work>/source --state <work>/antfly \
  --expected-rows 142577 --store-source --require-filters --cold-cache \
  --repeats 20 --workload-terms database postgres sqlite AI --no-highlights

python examples/hackernews/search_comparison.py --mode scan \
  --source <work>/source/archive/part.parquet \
  --database <work>/unused.duckdb --output <work>/duckdb-scan.json \
  --expected-rows 142577 --threads 4 --repeats 20 --cycles 2
```

Replace `<work>` with a real directory before executing. Run Antfly twice against
the same state for reopening/restart coverage. Use a fresh state to measure a new
publication layout: unchanged-file reuse preserves older sidecars. Omit
`--no-highlights` for the public-search workload, and add `--profile` to capture
execution diagnostics. `--query-term AI` changes the first query to a broader
workload. Filesystem mode obtains no cloud credentials and publishes locally.

Both ranked tests search `body`, return five hits and project the same metadata
columns. DuckDB uses its default Porter stemmer and English stopwords; analyzer,
field-length encoding and corpus semantics can produce different scores and
orders, so it is a performance baseline, not an exact ranking oracle. DuckDB
performs no HTTP response or highlighting work. Each engine's repeated ordered
results are checked for stability. The DuckDB database records the source SHA-256
and row count and rejects reuse with a different input. Installation, importing
data and building/checkpointing the index are excluded from query timings.

For matched x86 container runs, build `Dockerfile.search-comparison` with
`--platform linux/amd64`, mount this script at `/comparison.py`, the Parquet
partition read-only, and a writable result directory. Give the DuckDB container
`--cpus 4 --memory 4g`; Antfly's `poc.py --docker` defaults to those limits and
removes its uniquely named container when finished. Running x86 on ARM remains
a translated-host diagnostic. The native-host comparison is preferable.

`search_comparison.py --mode remote-scan` accepts an HTTPS Parquet URL and
`--bearer-stdin`. It uses a temporary in-memory HTTP secret, bounds credentials
on stdin, and excludes credentials/source URLs from the report. This workload
performs an **unranked substring scan** and has no text index. Remote experiments
need their own transfer/job bounds and budget admission; the local comparison
requires no cloud resources.

New native publications can compress opening navigation and batch small opening
objects into authenticated cohorts. Original immutable identities remain the
cache keys; required range reads remain available when memory admission is denied.
The directory codec, decoded-size limits, per-member proofs and GC/reuse retention
are described in `docs/plans/lake-ingestion-and-publication.md`. Compare provider
request and byte counters as well as wall-clock latency. Shared-host timing alone
is not a production latency guarantee.

The native ARM64 local comparison on the verified 142,577-row partition measured:

| Ranked `database`, five metadata hits, no highlights | First query | Warm p95 |
|---|---:|---:|
| Antfly, two reset-cache processes | 104.8–129.7 ms | 6.54–6.75 ms |
| DuckDB 1.5.6, two fresh processes | 26.15–30.54 ms | 10.24–10.31 ms |

Antfly restart searches took 56.5–78.5 ms. A separate highlighted native run
measured 178.83 ms cold, 8.87 ms warm p95 and 18.53 ms after restart. Its 4,110-hit
cursor continuation p95 was 38.85 ms, and history/current composition checks
passed. The matched x86 old/new publication comparison preserved exact top HN
IDs and scores while reducing provider reads from 397 to 314 (20.91%) and bytes
from 53,487,307 to 46,458,677 (13.14%). These are provider counters, not an HTTP
request trace. Native physical layouts can differ from the x86 layouts.

See `local-search-comparison-20261012.json` for provenance, samples and limits.
The host had unrelated concurrent work; these timings are diagnostic rather than
a production guarantee. OS file caches were not flushed. Antfly cleared its
serving cache before each restart; startup created one cache file, and measured
cold queries recorded zero disk-cache hits. DuckDB uses four threads; Antfly uses
its default server policy and includes HTTP overhead. Neither ranked comparison
highlights text, and scoring/tokenization are not identical between engines.
The local results isolate substantial remote-I/O overhead in the earlier GCS
measurements; they do not establish the new layout's regional or full-archive
latency. Antfly still reads roughly 43 MB locally for five hits, so reducing cold
work further remains useful. `--build-only` can prebuild and verify a publication
without taking query timing samples while builds are active.

A small separate DuckDB HTTPS scan diagnostic on the same unchanged export took
3.58 seconds for its first `database` substring query and 236 ms for its repeat;
`AI` took 206–209 ms after that initial access. This was unranked, used a native
ARM client outside the storage region and included only two samples per term.
It is recorded separately in the evidence and is not an indexed BM25 comparison
or a production latency qualification.

### Qualification after #1063

Rebuild publications with the merged routing/statistics formats before comparing
against the earlier results. Use `--build-only` first, and run query measurements
after the qualification compilers finish. Repeat each scenario twice on the same
source. Keep `--profile` out of the timed matrix: profiled requests currently use
the legacy planner, so enabling it changes the execution being measured. Process
health metrics still report provider requests, bytes and nested query phases.

```sh
uv run --project examples/hackernews python examples/hackernews/poc.py \
  --binary /absolute/path/to/antfly \
  --filesystem-source /absolute/path/to/source-root \
  --state /private/tmp/hn-combined-state --expected-rows 142577 \
  --store-source --require-filters --build-timeout 1200 --build-only

# Empty serving caches: optional readiness planning prefetch is disabled.
uv run --project examples/hackernews python examples/hackernews/poc.py \
  --binary /absolute/path/to/antfly \
  --filesystem-source /absolute/path/to/source-root \
  --state /private/tmp/hn-combined-state --expected-rows 142577 \
  --store-source --require-filters --cold-cache --disable-navigation-warming \
  --no-highlights --repeats 20 --concurrency 4 \
  --workload-terms database postgres sqlite AI

# Default readiness warmup, highlights, deep cursors and historical/current reads.
uv run --project examples/hackernews python examples/hackernews/poc.py \
  --binary /absolute/path/to/antfly \
  --filesystem-source /absolute/path/to/source-root \
  --state /private/tmp/hn-combined-state --expected-rows 142577 \
  --store-source --require-filters --cold-cache --repeats 20 \
  --cursor-pages 40 --cursor-term AI \
  --qualify-composed-sources --benchmark-composed --composed-current-rows 1000
```

`lake_indexes.navigation_warming.enabled` defaults to true. Disabling it skips
only optional planning prefetch; source proofs, publication validation and required
reads remain authoritative. A disabled run asserts that no warming attempt was
recorded. Clearing disk entries before startup with warming enabled does not
establish empty RAM planning caches. OS caches are not flushed in either scenario.

Restart timing runs before cursor continuation, preventing a continuation from
warming the historical search first. Union and saved-overlay benchmarks each use
an independent cache-cleared restart, followed by repeated queries. Their native
current fixture copies the sample's most recent rows; it is a bounded composition
diagnostic, not a live feed or a full year of current HN. The existing two-row
fixture separately checks nonmatching replacement suppression and mutable cursor
retention. Process peak RSS covers the complete child lifetime, including any
publication work, and must not be interpreted as query-only memory.

Use common and selective terms plus a fragmented source containing more than 64
segments to qualify bounded windows. Cross-source-file compaction and global
best-first traversal remain separate changes. Select them based on measured
navigation, segment-opening and posting work rather than treating the earlier
43 MB cold-read figure as the combined implementation's result.

Flat local `archive/*.parquet` inputs are staged under the qualification state's
filesystem object-store layout with `AFOBJ001` envelopes. The envelopes contain
payload length and SHA-256; Parquet payloads remain unchanged. Input files are not
modified, and a reused state rejects changed or extra staged objects. An existing
native filesystem bucket layout can be supplied directly. This setup happens
before publication and timing.

Local composition fixtures read the most recent sample rows directly from Parquet
outside timed requests. The attempted SQL seed (`SELECT` including `body`, ordered
by creation time with a 1,000-row limit) exceeded the statement work limit on this
sample. That diagnostic remains a separate SQL qualification finding; successful
composition does not imply that SQL shape has been qualified.

The combined implementation is recorded in
[`combined-main-search-qualification-20261012.json`](combined-main-search-qualification-20261012.json).
On the raw 142,577-row sample, final empty-serving-cache searches took
32.25–78.91 ms, warm p95 7.87–9.03 ms, and restart searches 29.61–38.56 ms.
After default readiness warmup, first searches took 30.74–49.13 ms; 42 planning
pages warmed before the query, with about 0.92 MB of cumulative setup reads.
Empty-cache queries still performed 248 provider reads / 25.04 MB. This is less
cold work than the earlier 295 reads / 43.31 MB, but remains a substantial target
for further navigation/posting improvements.

The matched DuckDB diagnostic ranged from 31.28 to 134.76 ms first-query latency
and 11.42–21.23 ms warm p95 across its two processes. Shared-host and OS-cache
variation prevents attributing every timing change to implementation changes.
Both Antfly layouts preserve exact top IDs/scores through all measured cycles.
The fragmented layout remains slower in some final runs; do not extrapolate these
small fixtures to full-archive throughput or a service SLO.

Small composed pages now fetch the requested page plus one lookahead, up to 128,
and grow geometrically when an overlay batch is completely suppressed. This cuts
historical leaf hydration from about 112 ms to 2.5 ms in the five-hit fixture and
reduces its cold provider reads from 474 to 244. End-to-end composition remains
roughly 0.6–0.9 seconds on the raw fixture, and the fragmented saved-overlay warm
p95 reached 1.01 seconds. Remaining time lies outside the measured historical leaf
query; native retained-snapshot capture and binding need separate profiling.

A later controlled profile found foreground cursor collection and filesystem
prefix listings traversing unrelated generations. Removing inline collection and
confining listings to their prefix subtree cut retained-current warm requests
from 739–1,151 ms to 53–56 ms, union requests from 742–808 ms to 64–95 ms, and
saved-overlay requests from 743–813 ms to 68–89 ms. The fixture and exact result
IDs/scores are unchanged. These are four warm repetitions on a shared native ARM64
host, without an OS cache flush; the report includes every sample and labels the
small-sample p95. They do not qualify full-archive throughput or a Cloud SLO.

Expiry collection runs under the existing maintenance owner. Cursor creation
persists discovery before the capability so interrupted physical captures remain
collectible after restart or drop. WAL overlays use the pinned inventory owner's
allocator and preserve sparse read capabilities and cancellation. Bounded
composed pages can return a `gte` total; request `count: true` separately for exact
overlay cardinality.
No end-to-end composed speedup is claimed. Count, aggregation and graph consumers
retain their larger streaming batches, and cursor identity/authority checks remain.

The follow-up to #1081 removes the SQL sampling workaround: current-table fixture
rows now come from Antfly SQL for both local and remote sources. The statement's
`LIMIT` and the HTTP request's `limit` are separate budgets; the latter must admit
the requested fixture size (default 1,000, maximum 4,096). Rows without timestamps
are excluded before ordering. Earlier recorded SQLSTATE 54000 observations used
the default 128-row HTTP budget and do not establish an ordered-projection scan
limit.

Native-text metadata 11/reader protocol 36 can retain a compacted serving root
alongside the original per-file rebuild inventory. Small adjacent one-run files
merge in bounded units without changing their source Parquet, row identities,
delete-hole maps or retained generations. The 67-file development fixture serves
9 native runs. Publication-wide score traversal uses authenticated term envelopes
for supported descending-score terms; ties remain eligible and pruned totals are
lower bounds. Exact counts and other query shapes keep the exact fallback. Upgrade
readers before publishing this format. See the deployment qualification plan for
bounds and separate correctness/performance evidence.
