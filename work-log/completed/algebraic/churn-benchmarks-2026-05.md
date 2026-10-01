# Algebraic Churn Benchmark Follow-Up (2026-05)

> Relocated verbatim from `zig/ALGEBRAIC.md` (lines 1988–2075 at commit 271838a195) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`ALGEBRAIC.md`](../../../zig/ALGEBRAIC.md), specifically the "Runtime Optimization" section. Durable decisions from this log were folded into that document (rewritten as undated prose) before the move.

The first May 17, 2026 small bulk-ingest smoke showed `docfact`, `pathfact`, and
`path_lookup` dominate sidecar bytes while `minmax` and `sym` remained small.
The overwrite path now coalesces fact/path updates by skipping the pre-delete
for overwritten upserts, diffing old and new `docfact` lookup rows, diffing old
and new `path_lookup` rows, and only rewriting `docfact`/`pathfact` payload rows
when their encoded payloads changed. Aggregate, adaptive, path-profile, and join
deltas still run through their exact old-minus/new-plus maintenance paths when
the projected dependency changes. A later pass made that skip explicit for
unchanged profile rows, unchanged path-promotion facts, and unchanged docfact
adaptive/direct tensors. On the same small LSM bulk-ingest smoke, adaptive churn
dropped from about 1.8s to about 0.1s after row coalescing, then to about 0.03s
after adaptive/profile skip gating for the isolated LSM adaptive case. The
combined smoke archive's max algebraic churn dropped from about 1.8s to about
0.08s. After routing non-append bulk ingest through the cursor-capable write
batch, a May 17, 2026 focused LSM smoke with 100 docs, 8 churn ops, batch size
50, adaptive profile, and `--algebraic-bulk-ingest` measured 24.981ms algebraic
update time versus 6.237ms full-text update time, with correctness checks
passing. A follow-up 120-doc focused comparison with 64 customers and 16
products measured direct LSM build/churn at about 278ms/157ms and bulk LSM
build/churn at about 20ms/33ms. Adaptive constrained terms now choose between
lazy point lookups for tiny bucket sets and preloaded child metric row maps for
larger bucket sets; the same bulk smoke moved constrained terms from about
0.70ms to about 0.50ms while preserving checksums. Configured and adaptive
materialized tensors now stay on the normal tensor-law mutation path inside
cursor-capable bulk write batches. The fact-only append-only fast path remains
available for schema/fact ingestion, but preaggregated materialization
accumulation is guarded off until it has the same production correctness
evidence as the tensor-law path. This keeps LSM direct sorted ingest for bulk
batches without treating repeated aggregate-row mutations as a separate
append-only fold.
Path-promotion dictionary maintenance now batches promoted lexicon/posting
changes during bulk maintenance. Standalone bulk batches rebuild each dirty FST
once before commit; DB-level bulk-ingest sessions carry dirty promoted
dictionaries across every flushed coalescer batch and rebuild each one once at
session finish before the primary store publishes the final sorted run. Non-bulk
writes keep immediate rebuild semantics. Primary document writes inside an
external DB bulk-ingest session now also open their LSM write batch with
`BatchOptions{ .mode = .bulk_ingest }`, which lets the primary store use the
same direct sorted-ingest fast path as algebraic/dense sidecars instead of only
benefiting from the elevated active-session flush threshold. Algebraic benchmark
dataset rows expose `algebraic_lsm_flushes`,
`algebraic_lsm_flush_output_runs`, `algebraic_lsm_sorted_ingest_runs`,
`algebraic_lsm_sorted_ingest_bytes`, and
`algebraic_lsm_write_pressure_compactions`; `storage_bench summary` rolls those into
`performance_evidence_summary` and supports
`--min-lsm-sorted-ingest-runs` so archived LSM bulk runs can prove the direct
ingest path stayed active. Path-promotion FST rebuild counts are also rolled up
and bounded with `--max-path-dictionary-fst-rebuild-count` so archives can catch
accidental per-row rebuild regressions. LSM flushes and write-pressure
compactions can be bounded with `--max-lsm-flushes` and
`--max-lsm-write-pressure-compactions` for archives that should stay on direct
sorted ingest. These LSM thresholds are part of the algebraic sidecar contract:
they prove that algebraic LSM-backed sidecars are not regressing into normal
flushes or pressure compactions under algebraic workloads. They are not intended
to certify generic LSM performance; standalone compaction policy, WAL, block
cache, and read-amplification questions belong in storage-specific LSM
benchmarks. The summary also reports
`unclassified_algebraic_comparisons`, adds a `correctness_record` flag to query
summaries, and `--require-performance-evidence` requires unclassified
comparisons to remain zero so new algebraic benchmark comparisons cannot bypass
correctness classification. The wide-key benchmark issues the public
multi-field `terms.fields` shape over the same canonical group axes as its
configured materializations, so it is now an exact comparison instead of a
deliberately classified shape mismatch. The shared DB matrix forwards LSM tuning through `--analytics-arg`, including
`--lsm-flush-threshold`, `--lsm-flush-threshold-bytes`,
`--lsm-bulk-ingest-flush-threshold-multiplier`,
`--lsm-bulk-ingest-flush-threshold-bytes-multiplier`, `--lsm-direct-bulk-ingest`,
and the compaction/level-target options. The `dataset_lsm_config` benchmark event records these values so
archive summaries can be tied back to the exact LSM finish-session policy under
test. The May 18, 2026 adaptive churn microbench showed the ready-spec cache
reduced repeated maintenance-plan builds substantially, but the materialized
adaptive LSM churn case remained dominated by lower-level sidecar mutation cost;
larger archive work should keep that as an open optimization target rather than
treating the cache as a complete churn fix. The algebraic benchmark and
shared DB matrix expose LSM bulk finish controls for publish-only,
flush-on-finish, compact-on-finish, deferred-L0 targets, and bounded foreground
compaction steps/bytes/time so archives can compare publish latency against
maintenance debt. External DB bulk finish now publishes the primary LSM session
before forcing managed-index catch-up to `full_index`, so algebraic sidecar rows
can fold the final coalesced documents and survive a durable LSM reopen at the
user-visible finish boundary. The shared DB matrix also forwards the
selected LSM bulk-ingest flags to the cold/warm read stage. Cold-read archives
should measure reopen behavior over the same direct sorted-ingest sidecar layout
as the scale, adaptive, and churn stages when
`--analytics-arg=--algebraic-bulk-ingest`, rather than forcing a normal non-bulk
build that creates unrelated flushes. MIN/MAX support compaction and per-row
path-promotion FST rebuilds are no longer the primary bottlenecks in this
workload.
