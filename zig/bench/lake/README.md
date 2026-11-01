# Lake archive qualification

`tools/benchmark_lake_archive.py` runs real Parquet queries and publication through
a dedicated Antfly server. It records every sample, verifies answers before
accepting timings, and keeps setup, restart, query and refresh work distinguishable.
Use the same collector and frozen inputs when comparing server builds.

From the repository root, with the E2E Python dependencies installed:

```sh
uv run --project zig/e2e/antfly python zig/tools/benchmark_lake_archive.py run \
  --binary zig/zig-out/bin/antfly \
  --output-dir .benchmark-results/lake/reference \
  --rows 131072 --files 8 --row-group 4096 --samples 3
```

Build the server in the worktree first. Use ReleaseSafe or ReleaseFast for
performance decisions and record the build configuration alongside the report;
Debug runs qualify functionality and identify work amplification. The collector
fingerprints the actual binary, collector, source files and workload rather than
inferring the binary version from the checkout. Full reports and generated data
belong in the ignored `.benchmark-results/` directory, not in Git.

Generated workloads include scans/aggregates, an indexed ID lookup, a broad
categorical filter, uniform and skewed joins, and an exact numeric text sort.
Independent arithmetic oracles check SQL results; the text case checks projected
logical IDs. `--join-build-rows` controls join fan-out. Inspect EXPLAIN's spill
counters before describing a run as a spilled-join qualification: small build
relations need not spill. `--case id_lookup --case ordered_text` selects a subset.
Fixture construction streams one `--row-group` at a time, including large
variable-width payloads, instead of retaining the archive in Python memory. The
generated numeric-sort case omits `schema.index_sort` and serves as a fallback
control. To qualify native sorted execution, use an imported plan that declares
physical ordering, then require positive ordered work in its sort profile.

The generated refresh changes every `--refresh-every`th row across all files.
New immutable files and a manifest chunk become visible through one atomic head
replacement. Readers cannot observe a partly rewritten fixture. The runner waits
for a new queryable text publication and independently verifies both changed-row
counts and SQL revision totals. Reports separate fixture encoding/commit time
from publication/readiness/verification time. The refresh recipe (rows, files, row groups, payload bytes, interval and generation)
is part of workload identity. Comparison also verifies the actual rewritten-file
hash and changed-row count. Imported or remote source data is never refreshed by
this tool; its report explicitly records no refresh workload. Earlier version-2
reports cannot qualify a version-3 comparison.

## Existing Parquet and remote sources

Supply an archive-specific version-1 JSON plan and one or more raw Parquet files:

```sh
uv run --project zig/e2e/antfly python zig/tools/benchmark_lake_archive.py run \
  --binary zig/zig-out/bin/antfly \
  --plan zig/bench/lake/archive-plan.example.json \
  --parquet /path/to/archive.parquet \
  --output-dir .benchmark-results/lake/archive-reference --samples 3
```

The import copies bytes into an isolated FsStore fixture, validates the copied
fingerprint before publishing, and overrides only the source URI. Originals are
not modified. Customize the plan's schema, row identity, indexes and cases for
the archive. Query statements use the dedicated `archive_qualification` table.
Plans can include exact `expected_rows` (HTTP SQL string cells) or `expected_ids`
for projected text results. Public IDs, scores, totals, highlights and SQL types
participate in answer comparison. Opaque snapshot/continuation token presence is
checked; token bytes contain changing authority/expiry. First-page timings do
not qualify cursor replay. The runner keeps normalized answers and mismatch
artifacts for inspection. An `identity_field` can replace physical IDs
only when it names the source's declared single-column row identity and is
projected in every hit. Without an independent expected answer, the report
labels the case as checking repeatability alone.

Without `--parquet`, a plan can reference an existing Parquet manifest or frozen
Iceberg source URI. Supply a `source_fingerprint` for comparisons; the report
labels it as user-declared, rather than claiming to have verified remote bytes.
Use `--server-config /path/to/qualification-config.json` for existing native
provider connections and secret references. The runner fingerprints that file
without copying its contents into the report; the managed server uses isolated
data/replica paths. Configuration must be suitable for a qualification server. Live cloud
query/retention qualification is separate from the filesystem fixture run.

Qualification report version 3 binds the executable, effective configuration
(including a generated default), collector and harness to file identities before
starting the server. It also hashes relevant runtime environment settings without
reporting their values. Checks around every restart and
measurement reject replacements and edits; final content checks run before a
report can be marked complete. Executables stay at their original resolved path
so executable-relative resources continue to work. Provenance is collected before
startup, and server/session cleanup also covers initialization errors,
interruptions, and failed report finalization.

Plans accept indexes embedded in `definition.indexes` and additional `indexes`.
Identical declarations are created once; conflicting definitions fail validation.
Initial readiness waits for the union plus `schema.relational_indexes`, recording
their published revisions. `identity_field` selects both the logical-ID oracle
and normalized answer identity, and must match the declared single-column row
identity. Omit it to retain the default `id` oracle and physical hit identities.

## Cache and measurement contracts

Each case/sample restarts the process, measures the first query, then immediately
measures the same query warm. A restart retains durable data, persistent cache
and OS page cache. Startup navigation can warm before public readiness. These
samples are named `process_restarted`, never empty-cache or network-cold results.
Restart duration, new-process sampled RSS and counters accumulated since boot
are recorded separately from first-query latency.

RSS is sampled for the server and its descendants at 50 ms intervals, with
explicit availability/errors. It is not an exact per-request allocation peak.
The collector retains raw sampled memory and process counters so their coverage
can be assessed alongside query timing. SQL cases also retain a separate `EXPLAIN (ANALYZE, FORMAT JSON)` execution with
its authoritative allocation/spill/coverage fields. Missing counters remain
unknown; they do not become zero. This diagnostic execution happens after that
case's timed samples.

Prometheus snapshots retain lake serving-cache counters and their process-interval
deltas. These can include asynchronous background work and do not cover all
SQL/provider or physical disk I/O. In particular, zero serving-cache provider
reads does not establish zero filesystem I/O. Reports explicitly leave remote
transport qualification false. Add provider-side traces when qualifying actual
cloud request counts and transferred bytes.

Refresh reports count newly retained immutable artifact files/bytes and their
ratio to rewritten Parquet bytes. This is an observed retained-artifact ratio,
not total disk writes: temporary spills and files reclaimed before observation
are excluded. The compressed-input denominator also depends on data entropy.
Do not extrapolate this ratio into cloud write amplification without the
corresponding provider trace.

## Comparisons and evidence

Run a candidate against identical inputs, then compare complete reports:

```sh
uv run --project zig/e2e/antfly python zig/tools/benchmark_lake_archive.py compare \
  --baseline .benchmark-results/lake/reference/report.json \
  --candidate .benchmark-results/lake/candidate/report.json \
  --output .benchmark-results/lake/comparison.json
```

Comparison requires complete version-3 provenance and rejects different source
identity/assurance, workload, collector, effective configuration, harness, runtime
environment, platform, cache contract, case coverage or answers. It rejects incomplete runs
and duplicate samples. Fewer than three samples cannot qualify timing. Ratios
from separate runs are observational; alternate build order and repeat on a
quiet machine before attributing a change to the implementation. Reports preserve
all raw samples instead of retaining only a favorable median.

## Native probes

Build the native probes through the supported product graph (from `zig/`):

```sh
ZIG_GLOBAL_CACHE_DIR=.zig-cache/global ~/bin/zig build lake-native-bench-build -Doptimize=fast
```

This regenerates options, SDK metadata and dependencies after `make gc`; no
ignored build snapshot or generated source path is required. The individual
`lake-typed-merge-bench-build`, `lake-spill-residency-bench-build` and
`lake-source-capture-bench-build` steps install their corresponding executables
in `zig-out/bin`. Steps without `-build` run the probes. Historical observations
preserve their original commands; these targets supersede their manual
cache-path build recipes for current-source reproduction.

Run comparisons only after task-owned builds, tests and cleanup finish. Alternate
both variants at least three times; keep other host activity and OS caches
explicitly uncontrolled. The fixed-cap control uses the same candidate binary,
not an old runtime, and isolates the former byte/root policy:

```sh
ANTFLY_MERGE_ROWS=65536 ANTFLY_MERGE_WORKSPACE=4194304 ANTFLY_MERGE_QUOTA=268435456 \
  ANTFLY_LEGACY_RESIDENCY_CAPS=1 zig-out/bin/lake-spill-residency-bench
ANTFLY_MERGE_ROWS=65536 ANTFLY_MERGE_WORKSPACE=4194304 ANTFLY_MERGE_QUOTA=268435456 \
  zig-out/bin/lake-spill-residency-bench
ANTFLY_CAPTURE_REPEATS=65536 ANTFLY_MATERIALIZED_CAPTURE=1 \
  zig-out/bin/lake-source-capture-bench
ANTFLY_CAPTURE_REPEATS=65536 zig-out/bin/lake-source-capture-bench
```

Set `ANTFLY_CAPTURE_ENGLISH=1` for English stemming; otherwise the capture probe
uses the simple analyzer. Both variants must match every token's term, position
and original byte-span digest, and the complete encoded offset SHA-256. The analyzer fingerprint is precomputed. Capture
timing includes analysis, callback validation, encoding and temporary teardown;
input creation, process startup, encoded-output verification and final Capture
teardown are excluded. Tracked peak includes the complete capture operation,
including its retained result, but excludes the immutable input. The callback
hash represents a consumer, not complete postings construction or publication.

The residency probe checks every sorted key, row ordinal, primitive value,
SQL NULL and tagged string under identical quotas for none/Snappy compression.
`CAPACITY` times the full leased drain with its value oracles; `TYPED_MERGE` times
add, finish, drain and sort teardown. Manager construction and process startup
are excluded. Payload-read counts and physical read bytes are distinct counters.
These kernel comparisons make no HTTP, cloud, empty-cache or archive-scale claim.

## Historical measurements

One-off comparison scripts, experiments and frozen measurement reports are kept
in [Git history](https://github.com/antflydb/antfly/blob/c2d0572d52f6dfceb108c8ae59b517c131bed62f/zig/bench/lake/README.md). They describe the
source revisions and machines measured at the time, rather than current
performance. Recover the exact report and script from that revision when needed.
New raw reports, logs and generated fixtures belong in ignored
`.benchmark-results/lake/`; include a concise result and reproduction command in
the PR. Keep reusable harnesses here only when they have a supported build target
or are exercised by the qualification suite.
