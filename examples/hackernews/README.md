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
Parquet. This converter materializes the sample in memory; larger backfills need
a streaming converter. Keep `hn_id` for HN links. `parent_id` identifies the
immediate parent, which may be another comment; root-story resolution is future
ingestion work.

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

Use a fresh local state directory when changing sources. The checker starts a
loopback-only process, creates `hn_archive_poc`, waits for publication, checks
10,000 SQL rows, ranked search, projected highlights, snapshot pagination, exact
HN-ID filtering with score ordering, and score/cursor stability after restart.
It stops the process and writes `report.json` into the state directory.

The checker obtains a short-lived token from the current gcloud identity and
passes it only through the child environment. Configuration contains
`${secret:HN_GCS_BEARER}` rather than a credential. This authentication is for the
smoke check; a service needs its own credential flow. Source and artifact
connections have separate prefix allowlists and capabilities (`lake_read` and
`storage.primary`). Existing human IAM grants are used. Index artifacts are in
`<prefix>/indexes`; catalog state and the configured 1 GiB cache are local.

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

Tables with declared relational indexes evaluate supported structured predicates
against indexed physical candidates before ranking, including match sets above
100,000 rows. This example currently declares only its full-text index: its
flat structured predicates scan projected columns against the pinned,
delete-aware lake snapshot and collect up to 100,000 matching IDs. Full-archive
qualification needs relational indexes on the HN metadata fields and larger
partitions. Nested paths retain the existing residual-filter path.

## Remaining deployment work

- Measure remaining cold remote I/O in the deployment region and under concurrency.
- Qualify larger partitions against build/corpus limits and indexed filtering.
- Define durable buckets and service identities through Colony's infra workflow.
- Stream backfills, resolve parent stories, and reconcile edits/deletions.
- Build Recent/Historical routing and the public search interface.
