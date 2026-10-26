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

The first Debug filesystem run at 16,384 rows showed why this gate matters: the
numeric text-sort profile examined all 16,384 candidates to select 20, and the
refresh retained substantial new artifact data. Those observations identify
places for further CPU/producer profiling. They do not establish an archive-scale
speedup or justify a cache-limit increase. The 142,577-row HN fixture is not
bundled; the raw-Parquet import path supports qualifying it when available.


A repeated [declared-ordering configuration probe](declared-ordering-observation.json)
used the same frozen 16,384-row, eight-file source and binary, with three samples
per cache mode. Adding `schema.index_sort` (`id ASC, _id ASC`) preserved every
answer, but both configurations examined 16,384 candidates for 20 results. Warm
medians were 949 ms without the declaration and 980 ms with it; these local Debug
numbers establish no speedup. The declared case reported
`missing_segment_index_sort`. This tests the existing configuration path, not an
implementation of the missing encoder/compactor support. The
[native physical-ordering design](../../../docs/plans/lake-row-identity.md#qualification-integrity-and-native-physical-ordering)
now assigns sorted logical coordinates through the existing authenticated maps;
the probe above predates that implementation.

The [native implementation comparison](native-ordering-observation.json) uses
identical verified Parquet bytes and exact answers, with three samples per cache
mode. On 16,384 rows across eight files, ordered top-20 decorates 21 candidates
and probes 37 ordered rows, compared with 16,384 decorated candidates before.
These counters exclude membership construction, planning and other query I/O.
Observed warm medians were 962 ms before and 67 ms after. Native compilation and
lifecycle validation overlapped some samples, so this is evidence of reduced
sort work, not a controlled throughput or timing-attribution claim.

A separate 2,048-row fixture with a declared unindexed 2,048-byte payload retained
71,135,726 new artifact bytes on refresh before the schema fix and 10,386,662
after (about 85% less). Both runs verified all 205 changed rows and SQL revision
results. This is retained immutable artifact growth, excluding temporary spills,
reclaimed artifacts and provider write traces. Each report pins its actual
executable independently of the checkout revision. The checked-in evidence keeps
every timing/sort-work sample, memory observations and input/provenance hashes.

The [version-3 compaction comparison](native-compaction-v3-observation.json)
uses complete provenance and the same frozen 16,384-row source. Its declared
`id ASC, _id ASC` top-20 case decorates 21 candidates with 23 ordered probes;
the reference decorates all 16,384. Across three samples per cache mode, observed
warm medians were 966 ms before and 33 ms after. First-query-after-restart medians
were 1,015 ms before, 1,219 ms with intermediate JSON source maps and 935 ms with
compact dictionary/range pages. Startup, retained caches and OS cache still
contribute to these observations; they are not empty-cache measurements or
controlled timing attribution.

A separate generated fallback control verifies all 1,639 changed rows and the
same rewritten-source hash. Refresh retains 70,811,934 new immutable artifact
bytes versus 114,465,543 before (about 38% less). Its sort still decorates every
matching row, and its query timings show no speedup. The evidence retains every
timing/sort sample, sampled memory, full-report digests and comparison decisions.
The compact-page regression independently round-trips 16,384 interleaved
coordinates; a 64-range page falls from 34,009 JSON bytes to 2,020 compact bytes.
These observations support less sorting work and metadata amplification, with
archive-scale throughput and cloud transport left for separate qualification.

Tests:

```sh
python3 -B zig/tools/test_benchmark_lake_archive.py
uv run --project zig/e2e/antfly pytest -q zig/e2e/antfly/test_lake_archive_qualification.py
```

The protocol-83 refinement shares one authenticated source dictionary per serving
pack and retains compact range views in query memory. Its maximum-interleaving
regression covers 65,536 ranges and eight 2,048-byte source names: encoded metadata
falls from 6,523,136 to 1,070,237 bytes, fitting the existing 4 MiB admission cap.
A separate ordered-merge regression checks all 16,384 original coordinates and
initializes compact query identities below 1.2 MiB, under a 2 MiB allocation limit.
An admission retry regression verifies that splitting rejected packs cannot add
statistics or ownership for their discarded merged candidate. Historical JSON and
page-local source dictionaries remain readable.

Native merge provenance now streams in output order, eliminating the additional
per-row inverse-map lookup and permutation sort. Exact single-term membership can
remain deferred for unopened sorted runs; the original four-segment regression
materialized only the first probed segment while retaining an exact total of 32.
The further refinement below eliminates that bitmap too. Unsupported filters
retain the ordinary bitmap path; sparse deletion masks can use the exact-count
refinement described below. These are bounded-work and
memory checks, independent of machine timing.

The [shared-navigation version-3 comparison](native-shared-navigation-v3-observation.json)
repeats the declared-order workload on the same verified 16,384-row Parquet source.
Every answer agrees and sort work remains 21 decorated candidates / 23 probes.
Across three samples per cache mode, first-query-after-restart medians fall from
922 ms to 215 ms; warm medians remain about 32 ms. Sampled server RSS is lower in
all three restarted samples. Reference measurements overlapped native builds;
these local Debug timings and RSS observations do not isolate implementation
cost or qualify archive throughput. The evidence retains all samples, provenance
and full-report digests alongside the deterministic admission and memory checks.

The [prefix and bounded-membership comparison](native-prefix-navigation-v3-observation.json)
qualifies physical `(id, revision, _id)` with both complete-tuple queries and public
`(id, _id)` prefix queries, plus a partial categorical-term query. All exact answers
agree on the same frozen 16,384-row, eight-file Parquet source. Across three samples
per cache mode, the prefix case falls from 16,384 decorated candidates to 21, with
22 ordered probes. The complete-tuple control also decorates 21; its probes fall
from 23 to 22. These counters cover sort work, not all planning or membership I/O.

Observed prefix medians fall from 1,161 ms to 192 ms after restart and from 995 ms
to 23 ms warm. Complete-tuple and partial-term restarted medians remain about
192–193 ms; warm medians fall from about 31/29 ms to 23/21 ms. Baseline measurements
overlap final compilation. The evidence pins both binaries, the actual executable
source commits separately from collector checkout, the complete workload, all 36
samples, input hashes and full-report digests. These local Debug observations do
not isolate timing attribution or qualify archive/cloud throughput.

The deterministic regressions establish the memory/work reductions separately:

- Exact all/none membership uses implicit ranges and no bitmap. Partial single-term
  membership probes authenticated postings chunks in arbitrary order without
  reading positions or materializing a segment bitmap. Two 4,096-row segments retain
  exact total 6,144 under backward, forward, repeated and cross-segment probes, with
  allocation-failure coverage. Fallback enumeration and unsupported filters retain
  complete bitmap execution.
- Pending lineage retains 263,022 bytes of native ranges and shared descriptors for
  16,384 ranges/two sources, versus 3,276,800 bytes of full range structures alone.
  This is retained data, not peak RSS or spare arena capacity. Merge workspace
  admission additionally charges actual pending arena capacity.
- The same 16,384 ranges submit two ownership additions and two deletions, one per
  source, rather than 16,384 of each. The final authenticated ownership tree keeps
  the same keys and values.

The [bidirectional ordering and public-tie comparison](native-bidirectional-ties-v3-observation.json)
uses physical `(revision ASC, id ASC, _id ASC)`, with a constant revision across
16,384 rows in eight real Parquet files. Complete primary tuples run in both
uniform directions; revision-only queries create one wide primary tie. All 48
samples agree with independent expected application IDs. The wide-tie oracle
computes the public file digests from the declared source/file names, then uses
the corresponding known row range; it does not derive expected order from either
executor.

| Query | Prior decorated candidates | Refined decorated candidates | Refined ordered probes |
|---|---:|---:|---:|
| Complete tuple, forward | 21 | 21 | 22 |
| Complete tuple, reverse | 16,384 | 21 | 22 |
| Wide primary tie, forward | 32,767 | 21 | 23 |
| Wide primary tie, reverse | 16,384 | 21 | 23 |

Observed warm medians are about 1,191 ms before and 24 ms after for the reverse
complete tuple, and 2,227 ms before and 28 ms after for the forward wide tie.
The forward complete-tuple control remains about 25–26 ms. These are local Debug
observations; the caveats below apply.

Uniform reversal reuses native traversal. Large primary groups use existing
publication-bound public file/row navigation and the retained index's by-ID tree,
with dynamic complete-order evidence and bounded fallback. The public `_id`
tie-breaker stays ascending; `search_before` traverses that order backwards.
No additional persisted index or metadata version is required. Optional public
navigation has a 4,096-row and 16 MiB read allowance, bounded tiles and a 32 MiB
required-read reserve. Small/sparse or unsupported groups retain native execution.

A deterministic 1,600-match regression separately drains buffered heads/EOF at
zero probe allowance and injects ordered-producer failure after 64 matches.
Both paths decorate exactly 1,600 candidates, retain the exact total and return
the same 20 public-ID-ordered rows. The collector retains completed top-K work
and a compressed seen set rather than replaying it on exhaustive fallback.

Sparse tombstones preserve lazy point membership: two 4,096-row segments have
raw term total 6,144; deleting one matching and one nonmatching row gives exact
live total 6,143 with no materialized bitmap after arbitrary-order point probes.
Fallback enumeration visits all 6,143 live matches. Match-all counts support any
validated mask; partial terms use at most 4,096 deleted point probes covering no
more than one sixteenth of a segment. Dense masks and unsupported filters use
ordinary bitmap execution. Allocation-failure sweeps cover both cases.

The evidence pins actual executable source revisions and full-report hashes,
retains all timing/memory/sort samples and declares its measurement limits.
These sequential local Debug observations establish exact results and lower
measured sort work; they do not isolate timing attribution or qualify empty-cache,
cloud-transport or full-archive throughput. Full Parquet and real Iceberg lifecycle
tests additionally exercise 64-row ties, both primary directions, forward/backward
pagination, external rewrites, retained vector generations, deletion and restart.

The [independent public-ID comparison](native-public-id-v3-observation.json)
adds plain `_id ASC` to the same frozen source and physical-order controls.
All 48 samples agree with independently derived application IDs. Plain ID sorting
falls from 16,384 decorated candidates to 21, with 21 ordered probes. Complete
forward/reverse tuples and the wide forward tie remain at 21 decorations. The
observed warm ID median falls from 1,293 ms to 24 ms; restarted median falls from
1,491 ms to 211 ms. These local Debug timings remain observational, with the same
cache and transport limits described above.

Independent native regressions show that an inclusive winning-ID cutoff scans
one row from a 64-row range, and that batching 64 exact ID lookups charges 196,760
page-read bytes versus 12,592,640 for scalar seeks. Both traversal directions,
exclusive starting cursors, duplicate/missing IDs, optional quota exhaustion,
required-read reserves and allocation-failure cleanup are covered. The complete
public-ID endpoint applies only to an equal primary group; better groups retain
all identities.

The comparison also pins an executable upgrade check. A declared string hidden
by the prior numeric-only dynamic-template projection returns zero matches with
the old executable, then one after automatic local repair and another restart.
Relational external schemas disallow dynamic templates, so their existing Parquet
publication is an unchanged control. A separate native planner regression verifies
that affected whole-document text bindings change while selected-field text and
vector bindings remain stable. Reader protocol 84 fences this semantic revision;
native text metadata encoding remains v13.


The [endpoint/lineage observation](native-endpoint-lineage-v3-observation.json)
retains 48 exact-answer samples on the same eight-file, 16,384-row source, with
one-segment/one-byte admission windows to exercise native endpoint traversal.
All four cases retain 21 decorations. Authenticated endpoint tests visit the best
of 128 ranges first and stop at primary dominance in both directions and backward
pagination. Point lineage over 65,536 ranges charges 349,069 bytes versus
1,070,237 for the complete map. Complete-capture refreshes reuse 256 of 257 keys;
missing keys decode by payload block within each fragment. Admission ownership is
released before point/inverse-map regression reads, including allocation-failure
coverage. The final executable is pinned to source `e0bf3799de` and its hash.

Warm medians are 28–30 ms versus 24–26 ms in the baseline; restarted complete-tuple
queries improve while wide/public-ID controls regress. Runs overlapped native
and E2E validation, so these observations establish exact results and bounded
work, without attributing an end-to-end latency or cloud throughput improvement.


The [lineage-lease / probe / binary-refresh observation](native-leases-probes-v3-observation.json)
retains the next fixed-source comparison: all 48 complete-answer checks and
independent expected application IDs match, and all four cases retain 21
decorated candidates. The final executable is frozen from clean source
`4abddacd7f`; the baseline executable is separately pinned to `26622434b8`.
Collector workspace revisions are recorded independently of executable sources.

Deterministic checks show 128 nearby lineage lookups acquiring five pages;
an eight-page lease limit bounds resident raw bytes to 512 KiB. A scattered
128-ordinal tile acquires 16 pages while preserving caller order and duplicates.
Warm hits still check deadline, cancellation and current lease hooks. A separate
4,096-point CPU limit prevents page reuse from postponing inverse-map fallback
indefinitely. For partial terms, 256 interleaved checks over four segments open
four probes and hit the cache 252 times, using 4,096 bytes. Twelve-segment eviction
and a one-byte admission limit preserve exact bitmap fallback under the eight-slot,
2-MiB budget. A two-integer refresh tuple occupies 59 binary bytes versus 248 JSON
bytes; mixed historical JSON/new binary refresh reuses 256 unchanged keys while
gathering one changed row. This is a serialized-row fixture, not an archive-wide
storage ratio.

Warm medians are 27.6–29.9 ms versus 27.1–29.0 ms; restarted medians are
153.7–215.1 ms versus 154.5–213.5 ms. These local Debug runs overlap native/E2E
validation, so the refinement claims bounded work and compact state without
attributing an end-to-end latency improvement or qualifying cloud throughput.
The complete integration run passes 514 server and 237 embedded contracts. Final
focused suites pass 70 embedded top-K contracts (one declared skip), 32 server
top-K contracts and 15 fragment/codec contracts; lineage/frontier checks pass
88 server and 26 embedded contracts. No failures or leaks remain in these suites.
The evidence distinguishes the broad run launched during refinement from the
final focused checks.


All 34 E2Es pass on the final frozen executable in 853.68 seconds: archive and
schema qualification, full Parquet/real Iceberg lifecycles, and ordered logical
identity rewrite/refresh for both formats. Eighteen archive tooling tests,
control catalog/generated error checks, the 1,013-source embedded boundary,
the 2,251-source Apache product boundary, formatting and diff checks pass.
The latest `origin/main` (`71aa19359a`) is included; repeated fetch/merge checks
report up to date. Repository cache cleanup completed with active-output
protection; only its idle uv-prune child was interrupted.

The [compound/null/cohort observation](native-compound-null-cohorts-v3-observation.json)
retains all 48 exact-answer control samples over the same eight-file, 16,384-row
Parquet source. Every control retains 21 decorated candidates. Independent
integer and timestamp E2Es verify 44 additional feature samples over 4,096 rows,
including Boolean filters, NULL boundaries, returned cursor replay in both
directions, exact zero-limit counts, and execution after restart. Ordered feature
queries retain 21–22 candidates. These tests also guard named-source isolation
and composed profile ownership.

Native checks cover bounded short-circuit probes, bitmap fallback under pressure,
nullable spill compaction, unchanged general coverage summaries, mixed-layout
best-first traversal, and metadata-only legacy directory migration. Reader
protocol 88 / text metadata v16 fence the new writers. The field-level
`nulls_ordered` option preserves the `missing_rejected` default. The candidate
includes the main merge; concurrent local Debug timings do not isolate latency
attribution or qualify cloud transport, empty-cache searches, or archive throughput.


The [interval/probe/capture observation](native-interval-probes-capture-v3-observation.json)
records protocol 89 / text metadata v17 qualification and the final clean-source
executable. Forty layouts publish with a 52-byte aggregate under the 256-byte
limit. Deep cursor checks exclude 255–256 obsolete ranges, including three whole
subtrees, while retaining an overlapping interval and public-ID ties. Candidate
metadata membership uses seven exact point checks across two segment bases
without materializing a segment bitmap.

The documented 8,193-row, 129-block, 16-fragment ordering scenario now performs
zero payload decodes, versus the previous documented 144. Compact keys are
captured during verification under the existing private memory/spill quotas;
extra narrow key writes replace repeated wide payload reads. The same four
real-Parquet controls preserve all 48 normalized baseline/candidate answers and
21 decorations per query. The record retains publication intervals and timings
without attributing a latency gain under concurrent Debug validation. Real cloud
transport and full-archive execution remain separate qualifications.


All 37 E2Es pass on the final executable frozen from clean source `80fe9777a5`
in 849.51 seconds, including real Parquet/Iceberg lifecycles and ordered logical
identity reuse through rewrites, updates, deletes and restart. Forty-eight new
integer/timestamp feature samples retain 21–22 ordered candidates out of 4,096
rows. Eighteen archive tooling tests, stable control/error catalog checks, format
checks and embedded/Apache boundary audits (1,014 / 2,486 sources) pass. Main
`9d152a4c78` is merged; repeated final fetch/merge checks report up to date.


### Ordering batches and merge provenance

[`native-batch-kernels-v3-observation.json`](native-batch-kernels-v3-observation.json)
pins clean production source `b9e51ad91b`, the frozen executable and reports.
The same eight Parquet files / 16,384 rows pass 36 samples per binary, preserving
complete normalized answers and independently expected IDs across six cases.
All samples decorate 21 candidates. Ordinary term plans scan 22 rows; metadata
plans prefetch at most 16 candidates and scan 33 rather than 22 in these controls.
Warm median ratios range from 0.945 to 0.992. Sequential local Debug runs and
concurrent validation do not establish causal latency or archive throughput.

A 16,384-row / eight-key / 32-column comparison retains identical checksums:
repeated row adaptation takes 262,053,000 ns, versus 8,169,000 ns with one batch
binding and typed kernels. This is a kernel observation, not publication speed.
A 64-candidate metadata mask uses one batch callback and no point callbacks.
The 8,192-row alternating 256-row-run merge emits 32 provenance ranges instead
of 8,192 point callbacks and removes the extra typed ordered-plan pass. Fully
interleaved inputs still need individual ranges; source-only merges retain the
allocation-free fallback traversal. Captured ordering still decodes zero payload
blocks in the existing 8,193-row / 129-block / 16-fragment regression.

Qualification passes 343 native test executions with two declared skips,
18 archive tooling tests, compiled error identity and control/error catalog
checks, and 29 real Parquet/Iceberg E2Es without skips. The native count includes
the compiled error-identity test. Main `0c753dc705` is merged. Reader protocol 89
and existing durable key formats remain; the internal compiled runtime ABI is 65.


### Proven coordinate probes and demand-aware prefetch

The [direct-probe qualification](native-direct-probes-v1-observation.json) pins
clean production source `850290dfaa`. Matching authenticated row maps and file
dictionaries admit metadata probes in original index coordinates; incompatible
layouts retain translation. Unloaded lazy inventories retain unknown scan costs,
so a missing eager file list cannot force full materialization with a zero-cost
estimate. Batch costs amortize internal reverse-tree navigation without relaxing
execution quotas. Dense ordered prefetch follows remaining page demand; sparse
predicates and unresolved public ties keep bounded larger batches.

Across identical eight-file / 16,384-row controls, all 36 limit-20 and 12 limit-one
samples per binary preserve complete answers and independent expected IDs.
Metadata scans fall from 33 to 22 at limit 20 and from 17 to 3 at limit one.
Local Debug warm medians are about 805 to 404 ms and 806 to 26 ms respectively.
Ordinary limit-20 timings vary by roughly 3–6% in the other direction; concurrent
validation and sequential runs do not qualify causal latency or archive throughput.
The tiny-page diagnostic observes three direct probes, zero translations and no
complete materialization. Limit-20 diagnostics retain a complete-membership
fallback; further elimination and its triggering consumer are not qualified here.

Capture encodes identical tuples directly from live typed columns using reusable
scratch: 4,096 rows allocate about 2.95 MB cumulative per-row arena capacity versus
208 bytes retained by one arena. This is an allocation/kernel observation, not
process peak memory or publication throughput. Final qualification passes 146
focused native tests (one declared skip), 18 tooling tests, and 29 real
Parquet/Iceberg E2Es without skips, including tiny-page scan bounds, rewrites,
updates, deletes, NULL/date cursors, pgwire and restart. Main `0c753dc705` is merged.

### Proven winner scoring and membership conversion costs

The [proven-page qualification](native-proven-pages-v1-observation.json) pins
production source `2436a157e2`. Sorted winner scoring binds selected native IDs,
the original score expression and the result to one leased text snapshot. It
preserves scoring semantics while avoiding repeated metadata and visibility
checks already proved during page selection. Full-membership estimates include
conversion work: matching compressed blocks for shared coordinates, matching rows
for translated layouts, and row conversion for a source scan alternative. This
separates distinct-ID ranges from compact categorical predicates. Existing hard
read-byte, scratch, tile and cancellation limits still enforce exact fallback.

Across identical eight-file / 16,384-row inputs, all 36 limit-20 and 12 limit-one
samples per binary preserve complete normalized answers, including scores, and
independent expected IDs. The separate 18-query page-size diagnostic covers
limits 1, 4, 8, 12, 16, 20, 24, 32 and 64 in process-restarted and warm states.
Complete materializations fall from 12 to zero. Every candidate query probes
exactly `limit + 1` metadata rows, translates zero rows, and scans `limit + 2`
ordered candidates. Neither winner rescoring nor the former 2,312-unit crossover
causes full materialization in these controls. The representation-aware budget
is 1,050,888 units for this distinct-ID range; execution quotas remain unchanged.

Local Debug warm limit-20 medians fall from 403.58 to 31.82 ms forward and from
402.73 to 32.67 ms reverse. Limit-one controls vary by about 3–7%; other limit-20
warm controls range from roughly 3–8% faster. Sequential measurements
overlap other validation and do not establish causal latency, empty-cache
behavior, cloud transport or full-archive throughput. Process-restarted queries
retain persistent and OS caches and may benefit from startup navigation warming.

Final validation passes 154 focused native test executions (one declared skip,
no failures or leaks), 18 archive tooling tests, full `make generate`, catalog
and formatting checks, and 29 unique real Parquet/Iceberg E2E cases without skips.
The nine external rewrite/Iceberg/pgwire cases passed on the final binary; all
20 archive cases were rerun after limiting materialization diagnostics to dense
queries. Selective unmatched prefixes retain their exact fallback. Main
`b0b39178d6` is merged and the final fetch/merge check is up to date. Canonical
connection capabilities are `lake.read` and `lake.write`; only these dotted names
authorize lake connection use. Bare read/write grant no lake authority.


### Predicate seeks and global ordered pages

The [predicate-sort qualification](native-predicate-sort-seeks-v1-observation.json)
pins clean production source `a717fcaaed` and its binary hash. Append compaction
intersects semantic NULL-completeness witnesses. Native sorted streams binary-seek
required predicate ranges through constant/equality prefixes. Independent ordered
metadata providers retain lazy membership, and publication-bound whole-filter
proofs avoid redundant residual evaluation. The global coordinator completes the
winning primary tie before applying the public-ID order, with bounded fallback
when completeness cannot be proved. Providers decline archive-only ordering when
accepted recent rows fall outside their coverage.

All 36 samples per binary use identical eight-file / 16,384-row inputs,
configuration, collector and environment hashes. Every selected row, source,
score and sort tuple matches baseline and independent expected IDs. Warm local
Debug medians are:

| Limit-20 case | Baseline | Candidate |
|---|---:|---:|
| Metadata order, dense range | 1,385.82 ms | 43.91 ms |
| Metadata order, tail range | 706.84 ms | 43.36 ms |
| Metadata order, reverse range | 737.14 ms | 44.56 ms |
| Native physical order, dense range | 32.10 ms | 29.96 ms |
| Native physical order, tail range | 757.80 ms | 29.75 ms |
| Native physical order, reverse range | 781.65 ms | 29.28 ms |

Complete-membership materialization diagnostics fall from 30 to zero across the
36 queries. Metadata cursors emit 21 ordered row references; native streams visit
22 ordered candidates and decorate 21. Binary-probe work is outside these scan
counters. The global coordinator does not aggregate child decoration counters;
its default zero is recorded as unavailable coverage, not zero work. Early top-k
termination reports a valid lower-bound total; explicit exact counts remain exact.
These sequential local Debug observations do not qualify causal latency,
empty-cache behavior, cloud transport or full-archive throughput. Restarted
processes retain persistent and OS caches and may warm navigation during startup.

Validation passes 122 focused native tests (one declared skip; no failures or
leaks), 18 tooling tests and 28 unique real Parquet/Iceberg E2Es without skips on
the frozen binary. Coverage includes limits 1/12/64, exact counts, residual
filters, NULL/date ordering, forced tiny windows with 64-row public-ID ties and
forward/backward cursors, snapshots, schema IDs, partitions, deletes, accepted
writes and restart. Main `d8344ac981` is merged.

### Ordered demand and primary boundary proofs

The [ordered-demand observation](native-ordered-demand-v1-observation.json) records
three samples per cache mode on one independently written 8,192-row Parquet file.
Only the first or last 64 rows match the respective text query, and each group
shares one metadata sort value. The merged baseline cannot prove a primary
boundary from a later text match and falls back. The candidate uses the last
authenticated key in each processed page and stops after 84 ordered references.
It also attempts global ordered top-k before enumerating metadata selections,
bounds optional pack selection, and retains abandoned attempt work in profiles.
Required finite row selections carry conservative ceilings through lazy Boolean
membership, preventing tiny child pages from walking unrelated ordered rows.

| Limit-20 case | Baseline warm | Candidate warm |
|---|---:|---:|
| Correlated matches, forward | 318.23 ms | 50.44 ms |
| Correlated matches, reverse | 324.57 ms | 54.30 ms |
| Native-compatible ordering control | 48.02 ms | 40.09 ms |

All 18 compared pages preserve public IDs, projected sources and sort tuples
exactly. Correlated-path scores differ by one f32 ULP; native-control scores match
exactly. Explicit exact counts are tested separately from early top-k lower-bound
totals. The observation pins executable/input/configuration/runner hashes and
individual samples. These sequential local Debug runs do not establish causal
latency, empty-cache behavior, cloud performance or full-archive throughput.
Restarted processes retain persistent/OS caches and readiness warming.

The correlated fixture, residual filtering, exact count, forward/backward
pagination, restart and exhausted-tie fallback are reproduced by
`test_ordered_metadata_boundary_handles_correlated_text_and_reports_fallback`
in `test_lake_archive_qualification.py`. Baseline fallback profiles omit attempted
ordered work; the candidate's child and reference counters must not be compared
to those default zeroes as if both had equal coverage.

Validation passes 241 focused native tests (one declared benchmark skip; no
failures or leaks), 18 benchmark-tooling tests, and all 29 selected real
Parquet/Iceberg E2Es without skips. Main `9d553bc925` is merged; its integrated
production build and three real-file rechecks pass, including the strengthened
source/score/sort-tuple assertions, Parquet pagination and Iceberg pagination.
