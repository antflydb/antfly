# Native lake performance: implementation and qualification log (October 2026)

> Summary of a verbatim log. Full text: [zig/NATIVE_LAKE_PERFORMANCE_FOLLOWUP.md@2a456b1](https://github.com/antflydb/antfly/blob/2a456b1ad9dbe17872ba1e42cbe80349e7486ea5/zig/NATIVE_LAKE_PERFORMANCE_FOLLOWUP.md). Current contracts were folded into [Native index execution contracts](../../../../zig/LAKES.md#native-index-execution-contracts) and [Cloud generations and retention evidence](../../../../zig/REMOTE_TABLE_SERVING.md#cloud-generations-and-retention-evidence) before the file was removed; deferred work moved to [Lake ingestion and publication](../../../plans/lake-ingestion-and-publication.md#deferred-native-lake-qualification).

The log covers native lake execution work from 2026-10-07 to 2026-10-26, built on PR #1006 and continued through #1017, #1046, #1051, #1058, #1063, #1112 and #1117, reader protocols 33 through 89. Test counts, E2E timings and fixture sizes describe each qualification at the time, not current guarantees.

## Decisions

- **Exactness over speed.** Every optimization preserves exact scores, f32 addition order, ties and cursor semantics, and keeps an exact fallback. Optional budgets are optimization budgets, never result limits. Ranked totals become `gte` lower bounds when competitive pruning skips matches; exact counts stay on the authoritative path.
- **Logical tie order.** Equal scores tie on immutable file identity and original file rank, never on physical packed offsets, so repacking, parallel scoring and accepted-WAL composition cannot change winners.
- **Global statistics for subsets.** Selected-file and windowed admission always score with the complete pinned publication's N, document frequencies and field lengths.
- **Signed i128 nanosecond datetimes** shared by SQL, native sort, cursors and histograms; RFC3339 strings are not treated as chronological.
- **Adaptive membership** probes reached candidates at 64 work units per probe, capped by the full-membership estimate, then switches once to complete compressed membership; a candidate-local answer is never cached as complete.
- **Separate CPU and I/O admission** (eight CPU, 32 I/O tasks within 64 MiB), following DuckDB's asynchronous I/O lesson that remote request and CPU concurrency need independent controls.
- **Cross-source-file serving packs** (protocol 38, superseding the original requirements recorded in the log) separate canonical per-file maintenance from packed serving runs with authenticated source spans; packs grew to four eight-way tiers and paged spans in protocol 42.
- **Cloud retention certificates are opt-in.** GCS bucket retention protects versions, not live names, so certificates require exact generation-pinned references; S3 COMPLIANCE and GCS Locked retention are the only accepted evidence; `qualified_retention` defaults to false.
- **Public ID v3** (`lake3:`, protocol 51) separates snapshot and file digests so snapshot changes do not rewrite public file keys.
- **Planner statistics are advisory** and never prune, limit rows or prove uniqueness.

## Key measurements

These are local kernel or fixture measurements, not archive throughput:

| Measurement | Result |
| --- | --- |
| Bitmap seeks, one million probes into 50 million ordinals (ReleaseFast) | 383.5 ms to 9.7 ms |
| Shared top-K heap vs linear replacement, k = 10,000 | 881.7 ms to 1.84 ms |
| Join partial aggregation, 32 repeated rows per key (Debug, 12,000 rows) | 1.477 s to 0.079 s median |
| Plain `_id` sort, 16,384 rows / eight files | 16,384 to 21 decorations; warm median 1,293 ms to 24 ms |
| Point lineage charge, 65,536 ranges | 1,070,237 to 349,069 bytes |
| Ordering-key extraction kernel | 262 ms to 8.2 ms |
| Direct metadata probes, limit one | warm median 806 ms to 26 ms |

## Rejected or superseded approaches

- Treating a root digest alone as proof that retained artifacts survive; certificates need backend witnesses.
- Using GCS bucket retention or versioning checks as immutable-name evidence.
- Fixed per-file manifest arrays in publication roots; replaced by paged catalogs and COW trees.
- A hard-coded 64-segment shortcut for proved segment subsets; replaced by byte- and count-bounded cursors.
- Inferring fragmentation from file count in E2E fixtures; fixtures now set the admission window explicitly.
