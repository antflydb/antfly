# SQL extraction: external lake SQL and native lake publication log

> Summary of a verbatim log. Full text: [docs/design/sql/history/lake-integration-log.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/sql/history/lake-integration-log.md). Originally relocated verbatim from `docs/sql-extraction.md` (lines 168–731, 4363–4394, 5263–5292, 5902–5929) at commit 3ce5aff4ea on 2026-10-12; durable decisions were folded into [SQL extraction](../extraction.md) before the move.

This log records the implementation of read-only external lake tables (Parquet prefixes and Iceberg tables attached via `base_source`) queryable through typed-row APIs and SQL over HTTP/pgwire, followed by native lake index publication (aggregates, ordered, text, sparse, dense) and cold-read/persistent-cache work. Counts and test totals describe the work at the time, not current guarantees.

## Decisions

- **Schema inference at creation** — reads Parquet footers or the selected Iceberg schema without decoding rows; Parquet combines all listed files, marks absent/nullable fields optional and rejects incompatible types; Iceberg uses field IDs, types and requiredness. Discovered columns and fingerprint are persisted before catalog publication so Describe/Execute use stable types. New fields do not silently expand the catalog; omitted fingerprints default to `auto`.
- **Supported types** — flat booleans, integers, floats, UTF-8 strings, timestamps, and decimals as precision-preserving strings (exact decoding up to precision 38, trailing zeros retained, string semantics; float casts explicit). Timestamp filters compare exact signed epoch nanoseconds. Nested, binary and unsupported logical types fail explicitly.
- **Credentials** — references resolve through configured `external_io` connections with `lake.read` capability and enforce bucket/prefix scope. Retained publications store a store locator and named connection, never credentials; reopening rejects a connection redirected to another endpoint, root, bucket or prefix.
- **Snapshot pinning** — each SQL statement pins source inventories and object versions; physical row identities are opaque versioned `lake1:` IDs, stable within a snapshot; clients must not parse or synthesize them.
- **Pruning stays conservative** — footer min/max pruning only for simple integer/string/boolean comparisons; Iceberg partition pruning by spec ID and source field IDs; unknown specs, transforms, missing statistics and decimal/timestamp annotations remain residual. Exact unfiltered `COUNT(*)` uses footer row counts only with no deletes or cursor constraints.
- **Bounded execution and spill** — blocking sorts, grouped aggregates, hash-join builds and window partitions spill through a shared statement owner; spill records preserve exact numeric tags, SQL/JSON null distinction and row order with checksum validation. Native batch interface is internal, not Apache Arrow.
- **One shared native admission scheduler** — expression stripes, Parquet decoding, speculative prefetch and spill writers share at most eight async tasks and 64 MiB reserved across statements, using the existing I/O runtime rather than per-operator pools. Required work runs inline under saturation; speculative prefetch yields.
- **Runtime filters** — nonrecursive equijoins install Bloom/range evidence into a direct probe scan, skipped on preserved outer-join probe sides and computed keys; the exact hash join and ON residual still decide every match.
- **Read-only attachments** — reject native write constraints, relational schema indexes, defaults, generated columns, TTL, SQL mutations, serializable range-proof requests, row policies/filters; existing native tables cannot acquire or remove an attachment via schema update.
- **Publication metadata out of the catalog record** — native metadata protocol 23 stores checksum, length and immutable artifact reference for the publication directory (bounded to 16 MiB) instead of embedding declarations in the 256 KiB catalog record; legacy inline catalogs remain readable. Cached payloads never replace fresh provider evidence.
- **Build ownership** — five-minute renewable lease with separate heartbeat; a lost or ambiguous metadata CAS is terminal and never replayed; publication goes through an exact table-definition CAS; 24-hour build deadline.
- **Incremental aggregate rebuilds** — exact mergeable recipes persist per-file contributions (at most 16,384 entries); changed-source builds rescan only new/changed files. Deletion-bearing sources, floating keys/extrema and non-associative floating reductions use a complete scan rather than inherit contributions under a weaker proof.
- **Published vs. queryable are separate states** — queryable only after current source proof and reader-contract verification; remote `CREATE INDEX` publication is asynchronous with explicit readiness.
- **Reader authority in native metadata, independent of definition CAS** — one renewable publication lease per process/session; retirement atomically closes reader admission; GC marks all descendants before its upload census, supports dry run and resumes the exact retirement cut. Deleting old attempt namespaces alone is unsafe. A protocol upgrade alone cannot prove old readers have drained, so legacy generations stay protected.
- **Random upload namespace per table incarnation** — collection cannot cross incarnations sharing a store and table number; store bindings are recorded at attempt admission so failed builds and early DROP leave enumerable cleanup obligations.
- **Backups preserve rebuild obligations, not publication authority** — restore creates an unpublished external table and rebuilds against its authorized source.
- **Persistent cache is optional** — missing paths, owner contention, startup failure and resource pressure must not become source authority or readiness failures. Failed disk-tier init retries after a 30-second backoff without any request sleeping. Cache contents never replace current source or authorization proof.
- **Opt-in stored source for highlighting** — `{"type":"full_text","field":"body","store_source":true}` (default false) retains only the indexed source projection so eligible single text queries highlight without Parquet; older unattested artifacts fall back to Parquet until rebuilt.
- **Ordered search continuation** — pages carry `remote_snapshot`; replay must echo it and a changed publication fails closed. The token grants no access and retains no artifacts.
- **Constant-time Iceberg coverage** requires an explicit immutable-object proof, not a snapshot label or TTL.
- Later protocol steps (protocol 24/25 reader contracts, protocol-25 paged contributions, protocol 26 keyed contribution pages and group-key-partitioned aggregate roots) are specified in [Remote table serving](../../../../zig/REMOTE_TABLE_SERVING.md#keyed-contribution-state-grouped-partitions-and-cold-load-admission); see also [bounded publication reuse](../../../../zig/REMOTE_TABLE_SERVING.md#bounded-publication-reuse-and-collection-progress) and [immutable scan plans](../../../../zig/REMOTE_TABLE_SERVING.md#immutable-scan-plans-and-aggregate-reduction-trees).

## Rejected approaches / caveats

- **Range planning sorted before grouping by compatibility** — interleaved object versions, codecs or decoded-column identities separated mergeable ranges; fixed by grouping the full compatibility class first (regression: eight reads to four; zero-gap policy keeps all eight).
- **Four 256 KiB GETs for a fully requested 1 MiB text pack** — replaced by one GET without increasing transferred bytes; partial cold reads keep 256 KiB units.
- **Partitioned spilled joins** can increase writes and first-row latency while reducing random reads, so microbenchmarks are not a general throughput guarantee.
- **Phase timings are not exclusive** — index acquisition nests inside ranking/highlighting and hydration inside search/delivery; do not sum them. Provider cache counters do not count all GCS HTTP attempts, metadata requests or retries.
- Local restart fixtures do not explain the previously observed deployment's missing cache files or quantify live GCS latency.

## Key limits and measurements

| Item | Value |
| --- | --- |
| Shared API SQL range cache | 64 MiB, at most 4,096 entries |
| Prefetch lookahead | at most four concurrent reads, 32 MiB per batch |
| Examined rows per stream | 100,000,000 |
| SQL statement defaults | 10,000,000 scanned rows, 65,536 scan pages, 256 MiB retained bytes |
| Embedded SQL JSON request limit | 64 MiB (67,108,864 bytes); oversized returns SQLSTATE `54000` |
| Prepare/execute working memory | 256 MiB each; sessions admit 4,096 staged mutations, 256 MiB staging |
| Spill quota | 1 GiB live, 64 open files per statement; `spill_root` default `/tmp` |
| Native execution chunk | 4,096-row upper bound |
| Recipes per index / directory ceiling | 64 / 4,096 declarations |
| Text corpus cache | 256 MiB shared heap; encoded corpus limit 512 MiB |
| Native per-request heap | 256 MiB |
| `lake_indexes.artifact_gc` defaults | every 30 seconds, 4096 deletes, 262144 marked, 512 MiB root reads |

- Cursor memory fixture: 131,072 and 1,048,576 integer rows in 65,536-row groups under a 32 MiB tracking allocator peaked at 7,081,703 and 7,081,817 bytes (input growth adds metadata, not retained rows).
- Window column-block spill: about 20x lower elapsed time across three samples and about 32% fewer physical reads for repeated, compressed 8 KiB payloads (local decoding/spill only). Raw data: [`native-lake-column-pipeline-refinements.json`](../../../../zig/bench/baselines/native-lake-column-pipeline-refinements.json), [`native-sql-refinements.json`](../../../../zig/bench/baselines/native-sql-refinements.json), [`native-lake-execution-refinements.json`](../../../../zig/bench/baselines/native-lake-execution-refinements.json).
- Projected-page restart regression: four cold provider requests, zero after reopening the disk cache with the provider unavailable.
- Final validation at the time: 35 focused cache/reader tests, 164 lake integration tests and all 503 lake-native tests passed.

## Deployment qualification procedure

Run the same highlighted query against one pinned publication cold, wait for queued writes to drain (check `write_errors`, `last_write_error`, `writes_dropped`), restart against the same persistent `lake_cache.root`, repeat, then repeat with an empty cache directory. Compare per-run deltas of `antfly_lake_cache_provider_reads_total`, `antfly_lake_cache_provider_bytes_total`, disk hits and phase timings on an idle node; inspect `disk_unavailable`, `disk_init_attempts` and `disk_init_failures` first. Configure `lake_cache.root` explicitly when the node has no local storage base directory.

## Open work noted in the log

- Richer statistics-driven costing, runtime filters through derived/computed scan mappings, more parallel aggregate/join pipelines.
- Shared detached dense generation cache and streamed contiguous native-file mappings.
- Public graph searches and search aggregations are not routed through the native remote adapters.
