# Native vector locality baselines

No-copy is the new posting-build default in this worktree as of September 7,
2026. This is a deliberate disk/locality tradeoff, **not** a claim that it wins
all metrics. It does not change table-level source-vector ownership, public
score semantics, search effort, or the experimental routing/admission flags.

- Unset `ANTFLY_EXPERIMENT_POSTING_LOCAL_PROJECTIONS`, or set it to `0`, to
  omit the duplicate posting-local projection plane from new builds.
- Set it to `1` to retain the locality-on comparison/opt-in configuration.
- Existing immutable planes remain readable. The switch does not eagerly
  rewrite/reclaim old generations; use fresh data for layout/disk comparisons.
- Subgroup experiments may borrow projections for training without retaining
  a duplicate plane. Those experiments remain off by default.

## Preserved evidence

The catalogs alongside this file record original commands, flags, input hashes,
individual qualified results, medians, and the failed-arm receipts. Their
archives contain the executed binary, raw result JSON, reports, logs, memory
series, and historical helpers whose original hashes still match. They exclude
mutable database directories, datasets, models, and the source checkout.
Changed/missing historical helper bytes are explicitly identified, not replaced
with today's source and represented as the old measurement implementation.

| Catalog | Purpose | Qualified arms |
| --- | --- | --- |
| `pr593-locality-matched-20260906.json` | Primary same-binary 50K/1M locality-on versus no-copy reference | 50K: 2 on + 2 off; 1M: 1 on + 2 off |
| `pr593-locality-fast-50k-20260906.json` | Earlier strongest repeated locality-on 50K reference; also retains its slow pre-admission-fix no-copy control | 50K: 2 on + 2 off |

The primary matrix preserves the **failed second 1M locality-on arm**. Its
120-second mixed catch-up failure must not be averaged into qualified results.
The older fast 50K binary must not be substituted into only one arm of a new
layout A/B. Neither reference is an isolated-host promise or the performance of
the subsequently modified tip.

Archives are under `.benchmark-assets/baselines/` with SHA-256 and relative paths
in each catalog. They are local, Git-ignored binary artifacts, not uploaded
backups. Preserve them separately from disposable benchmark outputs. The
original data roots remain untouched for same-data diagnostics; those roots are
not included in the archives. The JSON catalogs and this README are intended
for version control. Do not overwrite an existing catalog/archive to refresh a
baseline; create a new named baseline.

## Baseline numbers

Primary matrix, medians of qualified arms (the 1M on column is one run):

| Metric | 50K on | 50K off | 1M on | 1M off |
| --- | ---: | ---: | ---: | ---: |
| Ready seconds | 17.974 | 23.007 | 352.240 | 299.901 |
| C30 QPS | 2,372.633 | 1,729.279 | 893.044 | 873.905 |
| C30 p95 ms | 50.222 | 49.052 | 87.139 | 67.437 |
| Recall | 98.570% | 98.380% | 99.280% | 99.055% |
| Allocated disk GB | 0.6997 | 0.3752 | 5.9202 | 3.8219 |
| Load/query peak RSS GB | 1.509 | 1.501 | 6.109 | 6.097 |
| Mixed peak RSS GB | 2.548 | 1.916 | 6.902 | 6.412 |

The older locality-on 50K reference is 17.061 seconds, 2,597.792 C30 QPS,
36.419 ms p95, 98.535% recall, 0.6990 GB disk, and 1.487 GB load/query RSS.
Do not attach those throughput/tail values to the newer no-copy layout.

## Measuring the next candidate

`scripts/run_posting_locality_ab.py` explicitly sets locality in each arm,
independent of the product default. Use fresh roots, one pinned ReleaseFast
binary, both orders, batch 100, unchanged effort/boundary rerank, and the
load/query/mixed/restart gates. `run_projection_locality_ab.py` holds locality
on explicitly when isolating source ownership; it is a different experiment.

Use `scripts/run_dense_recovery_query_ab.py` for same-data query attribution,
not fresh-load or disk qualification. Profiled diagnostic QPS and official
VectorDBBench QPS are separate measurements. Recheck recall within one
percentage point, p95/p99, physical reads and bytes, governed memory, RSS and
physical footprint. Mixed workloads need both equal-duration capacity and
equal-offered-write-rate comparisons before attributing interference.

`scripts/preserve_vdbbench_baseline.py ROOT --archive NEW.tar.gz --catalog
NEW.json` freezes a completed evidence root without overwriting another
baseline. It rejects changed executables and evidence races, excludes failed
arms from qualified summaries, and preserves their original receipts/logs.
