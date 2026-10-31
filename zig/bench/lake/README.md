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


### Ordered execution refinements

The [refinement observation](native-ordered-execution-refinements-v1-observation.json)
qualifies the review fixes with three samples per case and cache mode. Eligible
native physical ordering keeps the selective postings path, and the shared native
cost rule avoids speculative global demand when a small text posting list is
cheaper. On the 8,192-row forward and backward fixtures, every final sample
examines the 64 matching candidates with zero speculative ordered visits; the
reviewed branch visited 1,024 unrelated ordered references before fallback.

Identity-only metadata pulls retain row references and one final boundary key.
The 256-row wide-key fixture has a nonconstant 131 KB unused suffix: it now
finishes through the bounded ordered path with 129 references. A separate native
allocation test fits 20 references with 96 KiB suffixes and their boundary into a
512 KiB page budget. Sparse supported text predicates use batched exact probes;
dense and unsupported shapes retain bitmap membership. Selected-pack waves use
four shared scheduler I/O lanes at most and join before releasing query state.
When every final winner shares a stored-source pack, one retained outer pin
allows covered projection delivery with validated native ordinals.

| Case | Reviewed warm median | Final warm median |
|---|---:|---:|
| Selective native ordering, forward | 286.75 ms | 18.29 ms |
| Selective native ordering, reverse | 293.19 ms | 18.11 ms |
| Wide unused ordered suffix | 2,247.41 ms | 648.80 ms |

The existing correlated-boundary cases retain their 84-reference/64-candidate
bound; their warm medians are 46.92/48.79 ms, versus 50.44/54.30 ms on the preceding
boundary build. All 36 paired pages preserve IDs, projected sources, sort tuples,
scores and other hit fields exactly. Exact and lower-bound total pairs remain
visible separately in the evidence. An intermediate boundary-only fix took
2.73 seconds in its single warm wide-key sample; retaining the proven winning
source for final projection addressed that additional delivery work.

The evidence pins binaries, sources, configurations, runners, every timing/work
sample and full-answer digests. These are sequential local Debug observations
on a shared machine, with build activity overlapping some baseline samples.
They do not establish causal latency, empty-cache performance, cloud transport
or full-archive throughput. Restarts retain persistent and OS caches and startup
navigation warming.

Validation on the frozen merged build passes 247 focused native tests (one
declared benchmark skip; no failures or leaks), 18 benchmark-tooling tests and
31 real Parquet/Iceberg E2Es without skips. New real-file regressions fail against
the reviewed binary and pass before and after restart on the candidate. Main
`2f47731030` is merged, with a successful integrated production build and the full
real-file suite. Formatting, syntax, license, Apache boundary and diff checks pass.


### Metadata-order costing and multi-pack delivery

The [delivery and costing observation](native-ordered-delivery-and-cost-v1-observation.json)
extends the shared single-term cost rule to metadata orders different from the
native physical order. The bucket index remains present in both builds, and the
Parquet, configuration, queries and runner are identical. Each selective query
now examines 64 text candidates with zero speculative metadata references; the
reviewed forward/reverse cases visited 1,024 unrelated references. A broad-filter
E2E also verifies the decision survives the downstream collector: it fails on the
reviewed binary at 2,304 references and passes after the complete handoff fix.

| Case | Reviewed warm median | Final warm median |
|---|---:|---:|
| Metadata ordering, anti-correlated forward | 298.82 ms | 20.16 ms |
| Metadata ordering, anti-correlated reverse | 303.48 ms | 19.68 ms |
| Metadata ordering, aligned forward | 51.02 ms | 19.61 ms |
| Native ordering control, forward | 18.29 ms | 20.26 ms |
| Native ordering control, reverse | 18.11 ms | 19.72 ms |
| Wide-key ordered control | 648.80 ms | 696.82 ms |

There are three samples per case and cache mode. These are sequential local
Debug observations on a shared machine; candidate samples overlap build/E2E
activity. They do not establish causal latency, empty-cache performance or cloud
throughput. All 36 paired pages preserve IDs, sources, sort tuples and every
non-score hit field. Metadata-order scores differ by at most 2.4e-7; native and
wide-key controls match exactly. Exact and lower-bound totals remain visible per
sample. The control timing increases are retained alongside the demonstrated
metadata work reduction; their work bounds remain unchanged.

Bitmap admission now evicts older pack states and retries while protecting the
current prepared patterns, then remembers an admission decline for that attempt.
Allocation-failure tests also verify that request-owned winner locators survive
result retirement without partial publication. Final ordered and ordinary text
pages use those locators to group covered native projection into joined waves of
at most four packs. Native ordinals never outlive their pin; stored coverage,
key validation and physical fallback remain enforced.

An independent Parquet E2E uses 32 incompressible 1 MiB stored payloads to create
multiple serving packs while projecting a small 24-hit page. With the original
Parquet object unavailable, it verifies at least two native hydration batches and
zero physical hydration calls across field/score ordering, projected/hidden
highlight fields and restart. Counter assertions wait for the health endpoint's
asynchronous metrics refresh. This proves the delivery path and eliminated
physical reads, without a separate multi-pack latency or throughput claim.


### Typed source columns and Boolean driver costing

The [source-column and Boolean-cost observation](native-source-columns-and-boolean-cost-v1-observation.json)
qualifies two gaps found in the delivery implementation. Sparse required Boolean
queries now use a conservative prepared candidate ceiling when choosing an
optional ordered driver. Exact membership and counts remain authoritative. All
18 Boolean samples examine 64 text candidates with zero speculative ordered
references, versus 2,304 references on the anti-correlated reviewed cases.

Stored-source recipe v3 persists independently compressed typed source columns
instead of a monolithic JSON body. A five-byte marker replaces the ordinary body;
there is no second persisted JSON copy. Native projection and hidden highlight
fields use four request-owned field cursors, each retaining one decoded chunk.
Append and sorted compaction preserve the columns through deletion and ordinal
remapping. Whole-document readers explicitly reconstruct JSON. A nested path
still reads its containing top-level JSON column.

| Case | Reviewed warm median | Final warm median |
|---|---:|---:|
| Boolean metadata order, anti-correlated forward | 336.09 ms | 27.46 ms |
| Boolean metadata order, anti-correlated reverse | 340.01 ms | 26.35 ms |
| Boolean metadata order, aligned forward | 44.64 ms | 26.61 ms |
| Small projection, 1 MiB unrequested payload per row | 391.57 ms | 33.93 ms |
| Same projection with physical source delivery | 26.58 ms | 29.83 ms |
| Wide-key ordered control | 696.82 ms | 545.97 ms |
| Native-order control, forward | 20.26 ms | 22.77 ms |
| Native-order control, reverse | 19.72 ms | 22.72 ms |

Each case/cache mode has three samples. All 48 paired pages preserve complete
hit objects exactly, including scores; total-count contracts are recorded
separately. Definition, Parquet, configuration and requests match within every
pair. The physical-delivery control intentionally changes `store_source` and
therefore the pack layout. The wide-key control retains its 129-reference bound;
native-order controls retain 64 candidates and zero ordered references.

These are sequential local Debug observations on a shared machine, with E2E
activity overlapping candidate measurements. The small native controls' timing
increases are retained alongside the demonstrated work reduction and projection
gains. No causal latency, empty-cache, cloud transport or full-archive throughput
claim follows. Restarts retain persistent and OS caches and readiness warming.

The production build succeeds with Zig 0.17.0. All 256 targeted native/owner tests
pass with no leaks and one declared benchmark skip; all 34 selected real
Parquet/Iceberg E2Es pass without skips. Source-column tests include exhaustive
allocation failures, sparse/deleted rows, both compaction orders, nested paths,
exact integers above 2^53 and explicit whole-source reconstruction. Main
`55d97ce423` is merged, with the canonical job projection ABI 84 and no duplicate
facade. The additional release-only merge preserves the frozen executable hash.
Benchmark tooling, formatting/lint, syntax, license, Apache boundary and diff
checks also pass.


### Source batching and indexed chunk navigation

The [source-batch observation](native-source-batches-and-chunk-navigation-v1-observation.json)
qualifies the wider-projection regression and the native source refinements.
On the same 1,024-row Parquet fixture, adding a fifth small integer field to a
24-hit page adds only 264 response bytes. The previous row-major hydration loop
reopened its four cached columns for each row. The new loop processes a requested
column across at most 64 ordinal-sorted winners before moving to the next column,
preserving public rank and key validation with the same four-cursor capacity.

| Case | Reviewed warm median | Final warm median |
|---|---:|---:|
| Four projected fields | 52.35 ms | 17.23 ms |
| Five projected fields | 93.25 ms | 17.63 ms |
| Boolean metadata order, anti-correlated forward | 27.46 ms | 21.07 ms |
| Boolean metadata order, anti-correlated reverse | 26.35 ms | 20.92 ms |
| Boolean metadata order, aligned forward | 26.61 ms | 20.91 ms |
| Small projection, 1 MiB unrequested payload per row | 33.93 ms | 30.90 ms |
| Same projection with physical source delivery | 29.83 ms | 26.49 ms |
| Wide-key ordered control | 545.97 ms | 517.79 ms |
| Native-order control, forward | 22.77 ms | 20.17 ms |
| Native-order control, reverse | 22.72 ms | 20.50 ms |

Projection variants have five alternating warm samples after both variants are
primed. Other case/cache modes have three samples. All 58 paired pages preserve
complete hit objects exactly, including scores, with identical candidate and
ordered-reference counts. Definitions, inputs, configuration and requests match
within every pair. The physical-source control intentionally changes
`store_source` and therefore pack layout. Earlier review repetition measured
55.52/84.02 ms for four/five fields; the retained original run above measured
52.35/93.25 ms. This remains a local Debug observation on a shared machine, with
E2E activity overlapping candidate measurements; it does not establish causal
latency, empty-cache, cloud or archive-scale throughput.

Recipe v4 source-column directories carry authenticated chunk row ranges. A
cold lookup into 32 sparse one-row chunks decodes only the selected chunk, and a
sparse gap performs no additional decode. Reusable cursors validate record offsets
once per chunk and use binary value lookup; append and sorted compaction share
them. Legacy formats, byte bounds, oversized values, malformed row ranges and
exhaustive allocation failures retain independent coverage.

Native text input now carries typed source without a temporary JSON body. Exact,
allocation-free AJV1 sizing shares encoder tags and preserves byte admission and
segment splitting. Tests retain a 256 KiB source payload while asserting no
stored JSON input and adequate source-size accounting. Highlight preparation
plans explicit/static source dependencies once, retains aliases, falls back
conservatively for unknown mappings, and reuses covered projected values. These
work reductions have structural qualification; no isolated ingestion RSS or
highlight latency gain is claimed.

The production build succeeds with Zig 0.17.0 and main `ef25cb9405` merged.
All 260 targeted native/owner checks and nine stored-column checks pass, with one
declared benchmark skip and no leaks. All 35 selected real Parquet/Iceberg E2Es
pass without skips. The new independent Parquet case crosses 64-row batch and
chunk boundaries with five projected fields, nullable exact integers above 2^53,
forward/backward cursor pages and hidden highlights before/after restart, while
the original Parquet object is unavailable. Benchmark tooling, formatting/lint,
syntax, license, Apache boundary and whitespace checks pass.

### Compressed ordered navigation and page occupancy

The [compressed-navigation observation](native-compressed-ordered-navigation-v1-observation.json)
compares identical Parquet files, index definitions, configuration and requests.
Native ordered publications now enable shared-prefix encoding. Both sorted
publication and incremental packing account for compressed occupancy, while
retaining the expanded-page hard cap and reserving final packed-child locators.
Decoded caches retain one prefix per page; identity scans consume borrowed key
views and copy only a required final boundary. Complete-key mutation and visitor
consumers retain their contiguous interface.

| Case | Reviewed median | Final median |
|---|---:|---:|
| 131 KB unused ordered suffix, warm | 508.02 ms | 79.02 ms |
| Same query, process restarted | 881.25 ms | 136.51 ms |
| Native order, forward, warm | 19.17 ms | 18.38 ms |
| Native order, reverse, warm | 19.99 ms | 17.85 ms |
| Native order, forward, process restarted | 112.61 ms | 137.56 ms |
| Native order, reverse, process restarted | 109.76 ms | 128.37 ms |
| Four projected fields, warm | 17.28 ms | 17.56 ms |
| Five projected fields, warm | 17.55 ms | 17.96 ms |

All 28 paired pages preserve complete hits and scores exactly, with unchanged
candidate, ordered-reference, selected and cursor-rejection counts. Wide/native
cases retain three samples per case/cache mode; projections retain five
alternating warm samples after both variants are primed. Reports pin source,
executable, Parquet, configuration, definition and runner hashes. The wide input
has 256 rows: `id=i`, `bucket=i//128`, `wide="w"*131000+str(i).zfill(4)`, and
`body="control quasar"`. Its bucket index retains the complete `bucket, wide, id`
tuple in both variants. This comparison does not remove the unused suffix from
the index definition.

A real publication regression scans forty 131,000-byte keys using 525,052 bytes
of navigation reads and 212,264 bytes of peak result allocation under a 256 KiB
limit, with zero cursor key materialization. Tests cover all key-view split
positions, cross-page forward/reverse/window/rank seeks, mutation preservation,
compressed sibling repair at the expanded hard cap, scoped cache reuse and
exhaustive allocation failures. Repeating an identical pagination request makes
zero additional provider reads; logical read-budget charges remain distinct
from physical cache misses.

These are sequential local Debug measurements on a shared machine. The final
pairs have no overlapping build or E2E work from this task. Native restart
controls were slower in this run, and the small projection changes are retained
alongside the long-key improvement. Restarts retain OS/persistent caches and
readiness warming. These observations do not qualify isolated causal latency,
empty-cache behavior, real cloud transport or archive-scale throughput.

The Zig 0.17.0 production build succeeds. All 263 targeted lake checks and 98
page/graph checks pass, with one declared benchmark skip and zero failures or
leaks. All 35 selected real Parquet/Iceberg E2Es pass without skips. Benchmark
tooling's 18 tests, formatting, license, Apache boundary (2,542 source files) and
whitespace checks pass. Latest fetched main `ef25cb9405` is already merged.

### Compact rank, count and membership probes

Read-only metadata probes now retain compact navigation leases through rank,
point/lower-bound batches, membership, range counts and sorted-range zippers.
Seek bounds share a page's prefix after one prefix check. Recursive range counts
retain normalized bounds while their ancestor leases remain live, and zipper
frames refresh bounds after their range producer advances. Batched results own
only returned complete keys/values; mutation and visitor interfaces stay intact.
Navigation I/O admission no longer reserves an unused expanded-key buffer.

The production-code kernel comparison uses 64 keys of 131,000 bytes, a fully
warmed decoded cache and three alternating before/after samples in Zig 0.17.0
Debug. Rank/count kernels run 256 operations per sample; batches run 32 operations
with eight requested keys. All answers match, every allocation is released, and
all measured loops make zero additional backing-store reads.

| Kernel | Reviewed median per operation | Final median per operation |
|---|---:|---:|
| Exact rank | 4.29 ms | 1.95 ms |
| Unbounded count | 26.0 us | 1.2 us |
| Bounded count | 19.04 ms | 7.30 ms |
| Eight exact ranks | 35.64 ms | 24.12 ms |
| Eight owned point lookups | 39.65 ms | 28.32 ms |
| Eight membership probes | 33.34 ms | 21.81 ms |

Warm rank/count/membership probes allocate zero key buffers. Rank removes
2,227,952 temporary allocated bytes per operation; bounded count removes
3,276,400 bytes. Eight point lookups reduce allocated bytes from 9,567,032 to
1,048,392, retaining the required owned result keys and values.

The earlier native-order restart slowdown did not reproduce with the same frozen
executables. In the repeat, original/reviewed forward restart medians were
127.24/109.09 ms and reverse medians were 129.02/103.30 ms. The earlier contrary
observation remains above. Three sequential samples on a shared Debug host do
not establish a causal startup regression or improvement; no startup policy was
changed on that evidence. Restarts retain OS/persistent caches and readiness
warming. Kernel improvements are distinct from whole-query gains.

To repeat the kernel comparison from this worktree:

```sh
mkdir -p .benchmark-results/lake/compact-metadata-probes
git show 3b1675bd94:zig/pkg/antfly/src/serverless/graph_segment/page_tree.zig > .benchmark-results/lake/compact-metadata-probes/before.zig
~/bin/zig test -lc --cache-dir zig/.zig-cache --global-cache-dir zig/.zig-cache/global \
  --test-filter compact_metadata_production_comparison --dep before --dep after \
  -Mroot=zig/bench/lake/compact_metadata_probe.zig \
  -Mbefore=.benchmark-results/lake/compact-metadata-probes/before.zig \
  -Mafter=zig/pkg/antfly/src/serverless/graph_segment/page_tree.zig
```

The [compact-probe observation](native-compact-metadata-probes-v1-observation.json)
retains raw kernel samples, source/executable/input hashes and all query medians.
Across 24 primary paired real Parquet pages, complete hits (including scores,
sources and sort keys), totals and work counters match exactly:

| Query | Cache mode | Reviewed median | Final median |
|---|---|---:|---:|
| Wide unused suffix | Warm | 79.30 ms | 76.96 ms |
| Wide unused suffix | Process restarted | 127.30 ms | 141.01 ms |
| Wide predicate bounds | Warm | 193.78 ms | 184.72 ms |
| Wide predicate bounds | Process restarted | 214.75 ms | 239.25 ms |
| Native forward | Warm | 18.96 ms | 17.94 ms |
| Native forward | Process restarted | 109.09 ms | 135.22 ms |
| Native reverse | Warm | 18.86 ms | 17.67 ms |
| Native reverse | Process restarted | 103.30 ms | 130.60 ms |

The final binary includes the main integration, and these sequential samples do
not isolate its changes from compact probes or host variation. The slower
restart observations prompted an additional traced control in reverse binary
order. Reviewed/final restart medians then measured 123.59/124.25 ms forward and
137.51/129.35 ms reverse. Those 12 diagnostic pairs also preserve complete hits,
totals and work. Every traced query is one successful HTTP request with no retry;
collector times are lower in the final binary, while total first-query time
varies outside the collector. These opposing observations leave startup latency
unqualified and do not justify a startup-policy change. All results are retained.

After merging main `031742c340`, the production CLI build, 364 native checks and
all 35 real Parquet/Iceberg E2Es pass, with one declared native benchmark skip,
zero E2E skips and zero native leaks/failures. The wide Parquet regression now
also checks long predicate bounds in both directions before and after restart.
Benchmark tooling's 19 tests and storage-audit tooling's 24 tests pass using the
pinned compiler and worktree caches. Formatting, license and Apache-boundary
checks pass. No build or E2E work from this task overlaps measured query runs.


### Projection-pruned covering delivery

Covering gathers now use a physical-coordinate tree with fixed-size key witnesses.
They no longer copy complete wide tuple keys through reverse lookup, forward
lookup and the result. The identity stripe checks the requested physical row and
its domain-separated key hash. Incremental publication updates this map together
with forward/reverse roots, preserving retained roots and cover dependencies.

Each covering batch publishes two objects: an authenticated directory and a pack
of independently authenticated native SQL column stripes. SQL windows read their
projection and predicate dependencies; ranked gathers read only requested fields.
The shared scheduler runs at most four joined block reads after exact selected-byte
admission. Decoded leases, cancellation, workspace limits, upload-scope retention
and both GC paths include these stripes. Metadata versions 11/12 and reader
protocol 90 fence workers that cannot interpret this publication layout.

The native structural regression gathers 32 rows with 131,000-byte tuple keys
under a 256 KiB workspace cap, peaking at **25,980 bytes**. A separate 16-row cover
pack contains **2,098,000 bytes**; its directory plus the requested identity/scalar
ranges total **2,427 bytes** of read admission. The unused wide value stripe is not read or
decoded. These are bounded component measurements, not total query-memory or
before/after transport measurements. Allocation-failure cases, wrong coordinates,
wrong key hashes, prepared-reference mismatches, a selected stripe larger than
1 MiB, and retained-root reads after incremental replacement are checked.

The [covering-projection observation](native-covering-projection-v1-observation.json)
records hashes, raw timing/profile samples, complete-hit comparison digests and
all narrow controls. The real Parquet fixture has 256 rows with a 131 KB unused
relational-key column, native `id` ordering and `store_source: false`. Each query
returns 128 `id`/`bucket` projections. Three restarts each yield one first query
and one warm query per executable. The narrow control removes the wide key.

| Query | Cache mode | Reviewed median | Final median |
|---|---|---:|---:|
| Wide covering keys | Warm | 792.85 ms | 54.14 ms |
| Wide covering keys | Process restarted | 1,502.52 ms | 76.98 ms |
| Narrow-key control | Warm | 51.70 ms | 48.49 ms |
| Narrow-key control | Process restarted | 63.44 ms | 62.41 ms |

All 12 paired pages preserve complete hits, including scores, sources and sort
keys, as well as totals and collector work counts (129 candidates/scanned rows,
128 selected rows and zero cursor rejects). Wide runs execute reviewed then final;
narrow controls execute final then reviewed. These are sequential local Debug
observations on a shared host. No build, E2E or cache cleanup from this task overlaps
measured queries; unrelated worktree compilations were observed. The final binary
also includes main integration. Restarts retain persistent/OS caches and readiness
warming. These measurements do not isolate causal, Release, empty-cache, cloud or
archive-scale latency. The ordinary narrow control shows no observed regression;
the large wide-key result agrees with the direct allocation and read reductions.

To reproduce with immutable executables built from the recorded revisions:

```sh
zig/e2e/antfly/.venv/bin/python -B zig/bench/lake/covering_projection.py before-wide /path/to/reviewed-antfly 2b954e55f401ce3b3914404741609c661dfe7f6f
zig/e2e/antfly/.venv/bin/python -B zig/bench/lake/covering_projection.py after-wide /path/to/final-antfly 8bc363e89a5c6776ebbcc6e8be216cfa6fb390d4
COVER_CONTROL_NARROW=1 zig/e2e/antfly/.venv/bin/python -B zig/bench/lake/covering_projection.py after-narrow /path/to/final-antfly 8bc363e89a5c6776ebbcc6e8be216cfa6fb390d4
COVER_CONTROL_NARROW=1 zig/e2e/antfly/.venv/bin/python -B zig/bench/lake/covering_projection.py before-narrow /path/to/reviewed-antfly 2b954e55f401ce3b3914404741609c661dfe7f6f
```

The checked-in E2E removes the imported Parquet object during HTTP and SQL queries,
checks both ordering directions, restores the object for replica readiness, then
repeats after restart. Full-text source storage is disabled, so duplicate source
rows cannot hide a fallback. The reviewed executable fails this fixture; the new
covering path passes. The production CLI and all 366 targeted native checks pass
with one declared benchmark skip, zero failures and zero leaks. Benchmark tooling's
19 tests, formatting, license, Apache boundary (2,546 sources) and whitespace checks
pass. Main `e8423151c59b` is merged; the final fetch adds only the HN
qualification document after executable freezing, with no production-source change.

All **36 selected real Parquet/Iceberg E2Es pass without skips** against the final
frozen executable, including the new wide covering regression. No runtime source
changed in the final main merge.


### Shared range waves and field-major result delivery

Navigation and covering delivery now share one authenticated range-wave planner.
Navigation retains its 1 MiB range cap and bounded gap policy. Covering reads use
at most 4 MiB per range, merge exact adjacency only, and process at most 16 units
per wave. Requested unit digests and immutable raw-cache keys remain independent
of transport grouping. Cold ranges use joined shared I/O tasks; warm decoded
stripes retain their existing leases without refetching evicted raw bytes or
acquiring the same decoded entry twice. Uncached single-unit buffers can transfer
ownership without another large copy.

The native regression reads three adjacent required stripes in **one pack-range
request**, with a maximum range of **1,689 bytes**, excluding the unused 2 MiB
value stripe. A directory fetch is separate from this pack-range count. Decoded
warm stripes make **zero provider reads even with raw and physical residency
disabled**. The shared navigation tests also verify corrupt data, range caps,
duplicates, allocation failures and gap-byte admission before provider I/O.

Stored-source hydration now traverses each field across the returned selection,
retaining four cursors while checking cancellation in 64-position tiles. A compact
permutation preserves result rank; metadata scales with returned hits. The native
256-row, eight-field comparison reduces both column opens and decoded chunks from
**32 to 8**, with identical exact integer values and no legacy whole-row decodes.
It also checks identity failures, cancellation and allocation faults. This is a
measured reduction in component work, not a claim of fourfold query speedup.

The real Parquet regression now uses 256 rows, nine projected fields and 192-hit
pages, with nullable integers beyond 2^53, both cursor directions, hidden highlights,
restart and the original Parquet object unavailable during queries.

Reproduce broad cases and narrow controls with the checked-in runner:

```sh
DELIVERY_CASE=source zig/e2e/antfly/.venv/bin/python -B zig/bench/lake/delivery_tiles.py LABEL /path/to/immutable-antfly SOURCE_COMMIT
DELIVERY_CASE=cover zig/e2e/antfly/.venv/bin/python -B zig/bench/lake/delivery_tiles.py LABEL /path/to/immutable-antfly SOURCE_COMMIT
DELIVERY_CASE=source DELIVERY_NARROW=1 zig/e2e/antfly/.venv/bin/python -B zig/bench/lake/delivery_tiles.py LABEL /path/to/immutable-antfly SOURCE_COMMIT
DELIVERY_CASE=cover DELIVERY_NARROW=1 zig/e2e/antfly/.venv/bin/python -B zig/bench/lake/delivery_tiles.py LABEL /path/to/immutable-antfly SOURCE_COMMIT
```

Each case uses the same 256-row Parquet data, native ID ordering, a 256-hit result
and three restarts with first/warm queries. Broad cases project ten scalar fields;
narrow controls project two. Source cases store native source columns; cover cases
disable text source storage and use an ordered index with included scalar fields.
Restart samples retain OS/persistent caches and readiness warming. Runs are
sequential local Debug observations on a shared host; no build, E2E or cache cleanup
from this task overlaps measured queries. Main integration and host variation are
explicit comparison qualifications. No isolated causal, Release, empty-cache,
real-cloud or archive-scale latency claim is made.


The [range/field delivery observation](native-range-field-delivery-v1-observation.json)
records all 26 reports, binary/source/input hashes, raw timing/profile samples and
78 exact paired-page comparisons (156 responses). Complete hits, scores, sources,
sort keys, totals and collector work counts match in every pair. The primary
comparison builds the reviewed implementation plus the same main revision
(`f67134b93c3b`) against the final implementation (`4fa6ccbd8bf3`). Both include main
`a507489e93d8`; the control build temporarily restores the six changed production
modules to the merged control revision, records their hashes, and restores final
sources and the installed executable afterward. Its build metadata consequently
reports the later HEAD with a dirty source overlay; the observation records the
actual source overrides explicitly.

| Query | Same-main control warm median | Final warm median | Control restarted median | Final restarted median |
|---|---:|---:|---:|---:|
| Ten-field stored source | 69.49 ms | 68.63 ms | 82.16 ms | 82.55 ms |
| Ten-field covering projection | 83.37 ms | 84.53 ms | 100.60 ms | 101.39 ms |
| Two-field stored-source control | 55.58 ms | 55.50 ms | 68.42 ms | 68.80 ms |
| Two-field covering control | 75.40 ms | 75.85 ms | 90.20 ms | 92.19 ms |

These whole-query medians are close. The structural gains above are qualified;
no additional large end-to-end latency gain is established. Earlier comparisons
against the pre-main reviewed binary, their slower observations and a reversed
broad-cover run are retained rather than selected away. A duplicate warm decoded
residency lookup was replaced with retained leases before final qualification.

Final validation: the production CLI builds; 368 targeted native checks pass with
one declared benchmark skip, zero failures and zero leaks. The benchmark tooling's
19 checks, Apache boundary (2,555 sources), license, formatting and whitespace
checks pass. All 36 selected real Parquet/Iceberg E2Es pass without skips against
the final frozen executable (286.39 seconds), including the expanded nine-field
projection regression. Production source and installed executable hashes match
the frozen final binary; main `a507489e93d8` is merged.

### Projection-independent caching, source prefetch and phrase costing

Required successful decoded stripes now share one immutable, interpretation- and
scope-bound cache entry across query workspace limits. Every reader checks the
retained footprint against its own allowance. Optional cached size-decline values
retain their policy key; failed required loads publish no value. The native
regression acquires the same value under three allowances with **one successful
decode**, rejects a smaller warm allowance, and retries a failed cold decode under
a larger limit. Covering queries alternate projections and 256/128/512 KiB limits
with raw and physical cache residency disabled: successful decoded entry count
stays constant and the later queries make **zero additional pack-range reads**.

Stored-source cursors use their authenticated chunk directory to hint only chunks
containing upcoming selected rows. A chunk transition examines at most **64
selection entries**, schedules at most **four range hints** and accounts at most
**1 MiB of compressed hint bytes**. A binary jump skips repeated selected IDs in
a chunk; a range exceeding the remaining allowance stops lookahead. The existing
query-owned native source queue retains its physical-block granularity, shared
scheduler admission, deduplication and cancellation/join ownership. Hint byte
counts are distinct from physical traffic and total query memory. Legacy and
oversized chunks retain ordinary required reads. The native test verifies four
exact small ranges (84 bytes), one 716,827-byte range in the larger-value case,
64-probe sparse termination, unchanged decoded cursor state, zero demand reads
during hinting and allocation-failure cleanup.

Phrase driver costing unions document-frequency ceilings within each alternative
position, then takes the minimum across required positions. Unknown statistics
retain the full collection ceiling. Positional matching, scoring and cursor proof
continue through the normal consumers. The native example bounds 10,000 documents
at 20 candidates for a rare term position, 50 for its alternatives, and 10,000
when statistics are unknown. The new real-Parquet phrase case verifies both sort
directions and restart, requiring at most 64 candidates/scanned entries. The
source E2E uses incompressible projected values spanning actual native transport
blocks, plus nullable exact integers, hidden highlights and an unavailable original
Parquet object.

Reproduce the comparisons with separately frozen executables:

```sh
zig/e2e/antfly/.venv/bin/python -B zig/bench/lake/policy_prefetch.py source LABEL /path/to/immutable-antfly SOURCE_COMMIT
zig/e2e/antfly/.venv/bin/python -B zig/bench/lake/policy_prefetch.py cover LABEL /path/to/immutable-antfly SOURCE_COMMIT
zig/e2e/antfly/.venv/bin/python -B zig/bench/lake/policy_prefetch.py phrase LABEL /path/to/immutable-antfly SOURCE_COMMIT
```

Source and cover cases use 256 rows and alternate two-field narrow and ten-field
covering/eleven-field source projections, then repeat both after each of three
restarts. Source includes deterministic incompressible 4 KiB notes. The phrase
case uses 8,192 rows, 64 phrase matches, a broad metadata predicate and 20 returned
hits; it exercises both favorable and opposing metadata prefixes. Every report
records exact hits, totals, profiles, HTTP attempts and source/executable/runner/
input hashes. Timers include application retries, if any. Restarts retain OS and
persistent caches and readiness warming. These local Debug observations do not
qualify Release, empty-cache, real-cloud or archive-scale performance.


The [policy/prefetch/phrase observation](native-policy-prefetch-phrase-v1-observation.json)
records same-main control `c532409d1c86` and final `04c923a48b6f` binaries, both
including main `ff2c66bd7ba6`. The control uses recorded reversible production-source
overrides; its build metadata reports the later HEAD and dirty overlay. Final
production source and installed executable hashes match the frozen final binary.

Initial paired local medians (milliseconds; query order is preserved per restart):

| Case | Query | Control | Final |
|---|---|---:|---:|
| source | narrow | 79.98 | 82.69 |
| source | broad | 82.60 | 79.19 |
| source | narrow_repeat | 46.58 | 45.89 |
| source | broad_repeat | 77.08 | 78.07 |
| cover | narrow | 76.01 | 76.78 |
| cover | broad | 71.97 | 71.61 |
| cover | narrow_repeat | 62.48 | 62.92 |
| cover | broad_repeat | 68.86 | 69.23 |
| phrase | pulsar_asc | 149.34 | 150.26 |
| phrase | quasar_desc | 30.37 | 31.12 |
| phrase | quasar_asc | 27.24 | 27.96 |
| phrase | pulsar_desc | 28.36 | 27.70 |

All 36 initial paired pages preserve complete hits, including source, scores and
sort keys, as well as totals. All 72 measured responses use one successful HTTP
request without application retry. Mixed projection/source timings are close;
they do not qualify an additional large end-to-end latency gain. The analyzed
`match_phrase` controls already choose postings in both versions, so those runs
do not demonstrate the ordered phrase costing change. All controls, including
slower observations, are retained.

The CLI builds; 270 affected native checks pass with one declared benchmark skip,
zero failures and zero leaks. The final source-lookahead owner revalidation passes
144 checks. All 37 selected real Parquet/Iceberg E2Es pass without skips in 297.76
seconds against the final frozen executable, including the incompressible source
fixture. The benchmark tooling's 19 tests, Apache boundary (2,555 sources), license,
formatting and whitespace checks pass. Real-provider qualification remains deferred.


The explicit-term public phrase form (`terms: ["control", term]`) exercises the
ordered cost decision. Reproduce that additional comparison with:

```sh
EXPLICIT_PHRASE_TERMS=1 zig/e2e/antfly/.venv/bin/python -B zig/bench/lake/policy_prefetch.py phrase LABEL /path/to/immutable-antfly SOURCE_COMMIT
```

Both explicit-term and analyzed forms pass the targeted real-Parquet regression
in both directions before and after restart. The explicit form preserves the same
64 text candidates while removing the abandoned metadata prefix: opposing cases
scan **1,024 → 0** ordered entries; favorable cases scan **84 → 0**.

| Explicit phrase | Control median | Final median | Reversed-order control | Reversed-order final |
|---|---:|---:|---:|---:|
| pulsar_asc | 467.36 ms | 135.92 ms | 441.07 ms | 129.15 ms |
| quasar_desc | 328.46 ms | 36.38 ms | 311.72 ms | 34.81 ms |
| quasar_asc | 57.41 ms | 31.54 ms | 53.77 ms | 30.73 ms |
| pulsar_desc | 59.42 ms | 32.42 ms | 55.13 ms | 30.47 ms |

The primary explicit comparison runs control then final; the diagnostic comparison
reverses that binary order. Each retains three process restarts and all four query
positions. No build, E2E or cache cleanup from this task overlaps any measured run.
This qualifies an observed improvement for the explicit-phrase fixture; the mixed
projection/source controls remain roughly flat. The final runner adds an optional
phrase-form selector and report annotation; the exact initial runner is retained
at commit `edd4b58d59be` with its historical report hashes.

Across all ten reports, **60 paired pages / 120 responses preserve complete hits,
source, scores and sort keys**. All 120 queries use one successful HTTP request
without application retry. Totals are identical in 48 pairs; in the 12 favorable
explicit-phrase pairs the valid lower bound `{value: 64, relation: gte}` becomes
the exact total `{value: 64, relation: exact}`. Every total independently satisfies
the fixture oracle. The supplemental phrase E2E passes in 8.18 seconds; the other
36 real-file cases retain their successful full-suite qualification. All initial,
slower and reversed-order observations remain recorded. Real-provider qualification
continues to be deferred.

### Streamed hidden highlights and covering-directory preparation

Hidden highlights now consume authenticated source values field by field across
all selected rows under each pack pin. The hydrator retains four field cursors;
the consumer retains row-type context and finished snippets in amortized
accumulators. Borrowed bodies are released after each callback. Matching keeps a
per-value cancellation checkpoint, and final field order follows explicit
highlight options. Projected-source highlighting still uses the shared matcher.

Covering preparation shares authenticated decoded-directory leases with direct
loads and submits cold directory misses through joined I/O waves of at most four
jobs. Each directory has a 1 MiB decoded allowance. The caller admits directory
wire bytes before preparation and selected stripe bytes before payload reads.
Prepared leases remain alive until the wave is consumed. Cancellation, rejected
budgets and provider errors join all tasks and release their leases/reservations.

Native checks exercise 256 rows and eight streamed source fields with eight
column opens and eight decodes, matching whole-selection projection. Directory
checks observe one concurrent read when I/O scheduling is saturated and four
with four admitted lanes; warm preparation and payload loading perform zero
provider reads with both raw caches disabled. These are structural checks, not
real-cloud latency measurements. Shared tests compare whole-row and streamed
highlight output for aliases, arrays and explicit/default field selection, and
exercise allocation failures.

The focused runner uses independently written Parquet, eight approximately
1 KiB text columns, 256 ranked hits, a narrow hidden-field control and a fully
projected control. Run immutable executables in both orders from the repository
root; reports retain individual samples, phase counters, input and executable
hashes, and complete hit digests:

```bash
zig/e2e/antfly/.venv/bin/python -B zig/bench/lake/hidden_highlights.py before-primary /path/to/reviewed-antfly BEFORE_SOURCE_COMMIT
zig/e2e/antfly/.venv/bin/python -B zig/bench/lake/hidden_highlights.py after-primary /path/to/final-antfly AFTER_SOURCE_COMMIT
zig/e2e/antfly/.venv/bin/python -B zig/bench/lake/hidden_highlights.py after-reverse /path/to/final-antfly AFTER_SOURCE_COMMIT
zig/e2e/antfly/.venv/bin/python -B zig/bench/lake/hidden_highlights.py before-reverse /path/to/reviewed-antfly BEFORE_SOURCE_COMMIT
```

The final executable includes the requested main merges. The comparison retains
that provenance and does not claim a strictly isolated same-main source control.
Local Debug timings do not qualify Release, real-cloud or archive-scale latency.

The [hidden-highlight observation](native-hidden-highlights-v1-observation.json)
records reviewed runtime `04c923a48b6f` (also the production source of the reviewed
`d6b8d93` executable) and final runtime `afcde2f5f55f`, including main
`073dfddf4866`. Their immutable executable SHA-256 values are retained in every
run. Three warm samples per query shape were measured in each binary order;
all 36 responses preserve complete hits and exact totals of 256. Query and
Parquet hashes also match across all runs.

Warm end-to-end medians, milliseconds:

| Query shape | Before, primary | After, primary | Before, reverse | After, reverse |
|---|---:|---:|---:|---:|
| Eight hidden highlight fields | 914.67 | 287.50 | 918.31 | 291.76 |
| Eight projected highlight fields | 271.13 | 274.73 | 276.56 | 253.71 |
| One hidden highlight field | 123.33 | 125.67 | 122.49 | 104.19 |

The wide hidden highlight phase falls from 838.47 to 201.73 ms in the primary
order and 829.46 to 225.33 ms in reverse order. The projected control's highlight
phase is 147.53 to 148.21 ms and 146.39 to 152.61 ms; the narrow hidden control is
55.64 to 55.95 ms and 56.40 to 56.12 ms. All slower controls and individual samples
are retained. The approximately 3.2x end-to-end improvement is specific to the
wide hidden-field fixture; lower wall times in the reverse-order controls do
not establish a general search improvement.

The final merged runtime builds with Zig 0.17.0. Its 70 targeted native checks
pass with zero failures, skips or leaks. All 30 real Parquet archive E2Es and all
16 real Iceberg/SQL E2Es pass on the same frozen executable. Wide hidden
highlights are compared with fully projected output in both directions, after
restart, and with the original Parquet object unavailable. Subsequent changes
record benchmark evidence and documentation only.

### Cancellable dense highlight kernels and prepared routing (v1)

`highlight_kernels.py LABEL IMMUTABLE_BINARY SOURCE_COMMIT` measures a real
Parquet fixture at 2, 8, 32, 64 and 128 KiB with sparse/dense matches, one
returned hit and at most three snippets. Its second cohort returns 32 dense
32-KiB bodies with hidden/projected fields and 250-ms/1-ms request budgets.
`hidden_highlights.py` remains the separate eight-field and narrow control.
Both runners use three samples per point and run in old/new, then new/old order.

[native-highlight-kernels-v1-observation.json](native-highlight-kernels-v1-observation.json)
retains all 204 measured HTTP samples, complete-result/total digests, input and
runner hashes, executable hashes, sources, deadline bodies and slower controls.
The runtime source is `9efb6f766d665c6eeac865829db5bcebbc7c57b5`, frozen as
`95fed771fd16d872da973990ef8242d13850f777ca64e1c05d7998025a2369d1`.
The control is runtime `afcde2f5f55f542a96bcda3eb16ac3fc44660df0`, executable
`27293d06f21235e7a58c57fd0e3dd8f3734d6fad22fc82d606fa10938e160a16`.
Main `65a8336d33ad4aecf0687b0d4e5c7620ba8bc6f3` was merged without conflicts;
its advance changes only the Hacker News example and `.gitignore`, leaving
runtime/build/test inputs unchanged. Benchmark controls were committed at
`4b86119e38485335f6dc2b71e10704be31820be3` before measurement.

Dense-field median HTTP latency in milliseconds:

| Field bytes | Primary old → new | Reverse old → new |
|---|---:|---:|
| 2 KiB | 12.45 → 11.98 | 13.68 → 12.44 |
| 8 KiB | 18.89 → 15.48 | 19.79 → 16.34 |
| 32 KiB | 69.08 → 30.15 | 71.98 → 29.93 |
| 64 KiB | 202.10 → 48.51 | 204.41 → 48.53 |
| 128 KiB | 701.83 → 85.90 | 730.37 → 97.08 |

The 128-KiB dense point improves 8.17x/7.52x; 64 KiB improves 4.17x/4.21x.
Sparse-field controls remain near their original latencies: at 128 KiB they
are 20.45 → 21.13 ms and 21.79 → 22.00 ms. All ten complete density-case hit
oracles and the normal 32-row deadline-cohort output agree across both binaries
and orders. Native reference tests separately cover randomized UTF-8 clipping,
density ranking, greedy overlap selection, allocation faults and cancellation.
Equal-density/equal-start windows now have an explicit shortest-end tie rule.

The 250-ms projected deadline control previously returns HTTP 500 after median
1851.58/1888.69 ms. It now returns the existing HTTP 504 `query_timeout` envelope
after 252.19/252.10 ms, including its public retry contract. The hidden control
already stops near the deadline; its erroneous HTTP 500 is likewise corrected.
All new 1-ms controls return structured HTTP 504. These are cooperative budget
observations, not a hard wall-clock guarantee.

The broad sparse fixture does **not** establish a general search speedup:

| Control | Primary old → new ms | Reverse old → new ms |
|---|---:|---:|
| Eight hidden fields | 284.75 → 293.86 | 281.59 → 293.77 |
| Eight projected fields | 284.35 → 262.40 | 265.59 → 264.71 |
| One hidden field | 111.10 → 109.02 | 109.65 → 110.32 |

Wide hidden latency is 3.2%/4.3% slower; its measured highlight phase increases
from 213.71 → 229.31 ms and 217.90 → 228.61 ms. Projected/narrow controls and every
individual sample are retained. Routing and kernel changes are measured
together; these observations do not isolate a benefit from prepared routing.
The small sparse-control overhead is retained alongside cancellable CPU work
and separate scratch/result ownership.

The frozen server passes 75 targeted native tests with no failures, skips or
leaks and all 48 real-data E2Es (32 archive, 16 Iceberg/SQL), without skips.
New Parquet/Iceberg cases check hidden/projected snippets across restart while
source Parquet is unavailable, structured deadlines and a healthy request after
timeout. The Apache boundary check covers 2,555 sources. Measurements use Zig
0.17.0 Debug, warm local filesystem object storage and synthetic real Parquet.
No task-owned build, E2E or cleanup overlaps measurement; the host is shared.
These results do not qualify Release, cloud, empty-cache or archive-scale latency.

### Independently readable source pages and native result leases (v1)

`source_pages.py LABEL IMMUTABLE_BINARY SOURCE_COMMIT` measures one snippet from
an 8-MiB string, with matches at either end of independently written real Parquet.
Each large value has its own row group, respecting the existing Parquet page
limit. It records normal requests and 25-ms/50-ms deadline controls, with exact
complete-hit digests for every successful response. Run old/new, then new/old.
`highlight_kernels.py` supplies the separate density/deadline cohort;
`hidden_highlights.py` supplies wide hidden/projected and narrow controls.

Native stored-source recipe v5 (reader protocol 91) separates large string pages
and analyzer vectors from compact source navigation. Both payloads decode in
64-KiB blocks. Original-offset vectors are fenced by the complete analyzer
fingerprint and copied with their source pages through compaction. Compatible
highlights decode only selected snippet pages. Unsupported mappings and patterns
retain controlled analysis. Native result columns retain decoded leases through
encoding, avoiding source-object deep copies. Small projected fields, single
static bindings, and tiny matcher lists retain direct paths; scan cancellation
uses one byte budget across successive short tokens.

The measurements below are whole-server warm local Debug comparisons on a shared
host. The control predates main's embedded resolver, enum lifecycle, runtime-memory,
resolver repair/dependency-proof, SDK and CI updates; timings include those changes.
Three samples per point and order are retained. Builds, E2Es and cache cleanup do
not overlap measured requests. Release, cloud, empty-cache and archive-scale
behavior require separate qualification. The persisted vectors add publication
storage and build work; structured values continue to use typed AJV1 storage.

[native-source-pages-v1-observation.json](native-source-pages-v1-observation.json)
retains all 276 measured requests, complete-result digests, deadlines, inputs,
runner hashes, executable hashes, slower controls and both binary orders. The
runtime source is `d178035ab486795afc8271bab147ac2feac947ff`, including main
`4db533991dcb4ea9fd7ee7813139a45ed31f36e0`, frozen as
`1f988437c49caa1cd6a99cd02d1056e62594e249d80a16c3f29148a75b449953`.
The control is runtime `9efb6f766d665c6eeac865829db5bcebbc7c57b5`, executable
`95fed771fd16d872da973990ef8242d13850f777ca64e1c05d7998025a2369d1`.

Median HTTP latency in milliseconds:

| Query shape | Primary old → new | Reverse old → new |
|---|---:|---:|
| 8-MiB field, matches at start | 95.06 → 8.95 | 100.61 → 8.98 |
| 8-MiB field, matches at end | 95.00 → 9.16 | 93.96 → 9.13 |
| 128-KiB field, dense matches | 82.33 → 20.44 | 83.37 → 20.25 |
| Eight small hidden fields, 256 hits | 283.54 → 305.14 | 281.68 → 293.39 |
| Eight small projected fields, 256 hits | 255.52 → 284.84 | 257.21 → 279.24 |
| One small hidden field, 256 hits | 109.37 → 108.96 | 108.32 → 110.73 |

The 8-MiB cases improve 10.29–11.21x and the dense 128-KiB case improves
4.03–4.12x. All twelve new 25-ms sparse-field requests and twelve 50-ms requests
complete successfully. The 32-hit dense cohort completes all twelve 250-ms
controls; every 1-ms control returns the structured HTTP 504 `query_timeout`
envelope. Successful deadline requests exercise the benchmark's public response
envelope handling and compare the complete hits with the normal request.

The small/wide controls retain a measured tradeoff: hidden-wide latency increases
4.2–7.6%, and projected-wide increases 8.6–11.5%. The narrow control varies from
0.4% faster to 2.2% slower; the 2-KiB density points also retain small mixed
changes. These are whole-server comparisons that include the main changes listed
above. The raw observations preserve these regressions alongside the large-field
wins. Native tests separately prove tokenization reuse, lookup equivalence,
byte-budget amortization, allocation cleanup and selected source-page reads.
The sparse Unicode test reads 7,946 bytes from a 14,100-byte compressed source
column representing 262,144 original bytes. Source/vector metadata remains scoped
to its authenticated publication and analyzer pipeline.

Validation on the merged runtime: 85 native tests, zero failures/leaks; all 32
real Parquet archive E2Es and 16 real Iceberg SQL E2Es pass without skips. The
extended Parquet/Iceberg case removes original Parquet objects, restarts the
server, retrieves an 8-MiB-field snippet inside a 50-ms budget, compares hidden
and projected dense fragments, verifies deadline envelopes, and confirms a
healthy request after cancellation. The Apache product boundary covers 2,557
source files. Later commits record documentation and measurements only.

### Selective original-offset vectors

`selective_offsets.py LABEL IMMUTABLE_BINARY SOURCE_COMMIT` exercises rare exact,
prefix, wildcard and fuzzy matches in a 2-MiB real Parquet field with 2,049 distinct
terms. It records seven warm samples per shape, complete hit digests, frozen
binary and fixture hashes, and one table-publication/readiness observation. Run
both binary orders, using the existing E2E environment from the repository root:

```sh
TZ=UTC zig/e2e/antfly/.venv/bin/python -B zig/bench/lake/selective_offsets.py \
  LABEL PATH_TO_FROZEN_BINARY SOURCE_COMMIT
```

Pair it with `hidden_highlights.py`, `highlight_kernels.py` and `source_pages.py`
for wide-result, dense-occurrence and large sparse-field controls. Stop task-owned
builds, E2E runs and cache cleanup before timing. A publication observation includes
admission and readiness polling; it is not an isolated ingestion CPU benchmark.
The native `SOURCE_OFFSET_SELECT` and `SOURCE_ANALYSIS` probes independently check
physical selection and reuse rather than inferring those properties from latency.

The [selective-offset observation](selective-source-offsets-v1-observation.json)
records **388 measured HTTP requests** from the final merged runtime
`d1faae6a39cb7db654d46451e211e1c1f581404f` (binary SHA-256
`0a24c2db06c63608842c99e21c7a2f2f9c00b87d2ec758138451c03e905affb3`).
The control tree merges the pre-change branch `f828785768` with the **same main
revision `e61aff2e88`**; its tree and reconstruction commands are retained in the
observation. This isolates the selective offsets, indexing capture and string
preparation change from main's resource-policy updates. All complete-result
oracles match across binaries and both execution orders.

Warm local Debug medians, milliseconds:

| Shape | Old → new, primary order | Old → new, reverse order |
|---|---:|---:|
| Rare exact, 2-MiB vocabulary | 39.19 → 8.74 | 39.92 → 8.65 |
| Rare prefix, 2-MiB vocabulary | 50.80 → 8.53 | 50.64 → 8.25 |
| Rare wildcard, 2-MiB vocabulary | 68.66 → 10.40 | 69.18 → 10.38 |
| Rare fuzzy, 2-MiB vocabulary | 97.81 → 10.75 | 98.52 → 10.82 |
| Eight projected small fields, 256 hits | 304.15 → 280.00 | 291.25 → 275.20 |
| Eight hidden small fields, 256 hits | 311.56 → 307.13 | 303.10 → 309.39 |
| Sparse 128-KiB field | 16.60 → 11.15 | 17.07 → 10.53 |
| Dense 128-KiB field | 20.77 → 17.68 | 20.55 → 16.62 |
| Sparse 8-MiB field, matches at start | 9.16 → 9.24 | 9.02 → 9.48 |
| Sparse 8-MiB field, matches at end | 8.91 → 9.22 | 9.10 → 9.51 |

Rare-term shapes improve **4.49–9.10×**; projected-wide total latency improves
**5.5–7.9%**. Hidden-wide stays within roughly 2%, and narrow controls vary within
4%. The existing 8-MiB sparse controls cost another **0.08–0.46 ms (1–5%)**;
they continue to satisfy their 25-ms and 50-ms requests. Dense 128-KiB improves
15–19%. The 2–8-KiB kernel points remain within roughly 5%; all observations,
including slower points and structured 1-ms timeouts, are retained. These are
seven samples per rare-term point and three per control point, in both binary
orders on a shared host, with no task-owned build, E2E or cleanup overlap. They
do not qualify Release, cloud latency, empty caches or archive scale.

Native probes independently show 8,192 token records narrowing to 32 selected
occurrences, 64 term tests and **814 bytes read from a 37,343-byte vector**. A
physical-binding alias test captures one analyzer variant, reuses it once and
spills 110 bytes; it does not claim that separate physical indexes share every
indexing analysis. Both public lake native owners pass: **533 integration and
493 frontier cases**, each with its **240-case shared partition**; all report
zero skips, failures and leaks. The combined command exits successfully. All
**32 Parquet archive and 16 Iceberg SQL E2Es** pass without skips on this frozen
binary, including rare exact/prefix/wildcard/fuzzy source spans with original
objects unavailable after restart. Apache boundary checks cover 2,563 source
files, and license, lint, formatting and whitespace checks pass. Subsequent
changes only record evidence and normalize two headers inherited from main.

### Dense streams and page-local Unicode boundaries

`source_refinements.py LABEL IMMUTABLE_BINARY SOURCE_COMMIT` adds two real-Parquet
controls: a 2-MiB dense field and an 8-MiB variable-width Unicode field with rare
matches. Both validate complete hits and exact UTF-8 byte spans. Pair it with
`selective_offsets.py`, `hidden_highlights.py`, `highlight_kernels.py` and
`source_pages.py`; run old/new followed by new/old without task-owned build,
E2E or cleanup overlap. Publication readiness observations include indexing and
polling and must not be presented as isolated analyzer CPU measurements.

The native dense selector oracle compares the stream and vector kernels across
fragment sizes 1/17/100/513 and counts 1/2/3/8. It also exercises allocation
failures and cancellation. The Unicode probe admits a tiny snippet from an
8-MiB source under a 96-KiB limit, including page-boundary clipping. Reuse probes
verify analyzer-run counts and last-consumer ownership independently of timings.

The [dense-source observation](dense-source-refinements-v1-observation.json)
records **444 measured HTTP requests**, complete-result digests, deadline outcomes,
and executable/input/runner hashes. The control is the clean merge
`ccc012ee664f8e856b0b02d7dd25c04621d2cf87`; both binaries include main
`acc30f5415c8c96465384d4f0dd46b649959a1d5`. The final runtime is
`17d4465cc8e799bc287828e864c3b55ac4d310ec`, frozen as SHA-256
`2aa1ae8410c258f99d91f3777eaf7073b347bed515eb55565e6d9c4fcda722e2`.
Runners and corrected large-row fixtures are from `42ece3e65f`; that commit
does not change runtime code. All complete-result oracles match across binaries
and both execution orders.

Warm local Debug medians, milliseconds:

| Shape | Old → new, primary order | Old → new, reverse order |
|---|---:|---:|
| Dense 2-MiB field | 120.93 → 88.81 | 120.98 → 86.73 |
| Variable-width Unicode 8-MiB field | 17.18 → 11.01 | 17.37 → 10.97 |
| Rare exact, 2-MiB vocabulary | 9.08 → 9.19 | 9.30 → 9.12 |
| Rare fuzzy, 2-MiB vocabulary | 11.40 → 11.64 | 11.57 → 11.61 |
| Eight projected small fields, 256 hits | 276.72 → 266.83 | 285.49 → 265.33 |
| Eight hidden small fields, 256 hits | 314.28 → 312.49 | 309.43 → 301.40 |
| Sparse 2-KiB field | 10.44 → 11.39 | 11.15 → 11.81 |
| Sparse 128-KiB field | 10.09 → 10.86 | 10.93 → 10.95 |
| Dense 128-KiB field | 16.56 → 15.78 | 17.49 → 15.74 |
| Sparse 8-MiB field, matches at end | 9.60 → 9.40 | 9.35 → 9.31 |

Dense 2-MiB latency drops **26.6–28.3%**, and the Unicode case drops
**36.0–36.9%**. Projected-wide improves **3.6–7.1%**, hidden-wide
**0.6–2.6%**, and dense 128-KiB **4.7–10.0%**. Rare-term controls vary
between a 2.4% improvement and a 2.1% regression. Small sparse kernel controls
include regressions up to **0.94 ms (9.2%)**; their reverse-order differences
are generally smaller, but the 2-KiB sparse point remains **0.66 ms (5.9%)**
slower. These controls do not establish a universal latency improvement. The
8-MiB ASCII controls stay within 0.21 ms and all 25/50-ms requests succeed;
1-ms kernel requests retain structured timeouts and 250-ms requests succeed.
Every measured point, including slower controls, is retained in the observation.

The source-refinement and rare-term points have seven samples per shape/order;
other controls have three. These are whole-server warm local Debug observations
on a shared host with no task-owned build, E2E or cleanup overlap. They do not
qualify Release, remote-provider latency, empty caches or archive scale, and
publication readiness does not isolate ingestion CPU savings.

Native probes establish the memory and reuse properties independently: **18,461
selected occurrences** require a peak queue of **527**, **441** retained candidate
windows and **101,055 bytes** under a 256-KiB budget, with vector-equivalent
fragments across all tested counts and widths. An **8,388,600-byte** Unicode value
uses a **4,125-byte** descriptor and six boundary-map reads, peaking at
**89,310 bytes** under a 96-KiB budget. Both return to zero live bytes. The
equivalent-binding probe records **one analyzer run and one reuse**, plus one
captured source variant, one capture reuse and 110 spilled bytes. Fault and
cancellation tests verify ownership; streaming substring expansion retains its
existing allocation-free path.

Both native owners pass **533 integration and 493 frontier cases**, each with a
**243-case shared partition**, with no skips, failures or leaks. These are 1,512
owner executions with overlapping tests. All **48 distinct real-file E2Es** are
verified on the frozen runtime: the initial run passed 46 and failed two expanded
fixtures with `ParquetPageTooLarge`; after separating large rows into row groups
and separate Iceberg appends and bounding the Unicode vocabulary, the two targeted
cases pass. Runtime code is unchanged between runs. These cases verify sparse,
dense and Unicode snippets after removing the original objects and restarting.
Apache boundary checks cover 2,574 source files; license, lint, formatting and
whitespace checks pass.

Main advanced after that frozen comparison. The [post-merge validation](dense-source-refinements-postmerge-validation.json)
records main `f2b7b76d57d2b51bf78171dcef3e8c0ccc582ffa`, merged runtime
`d4950cdd761802aceef3b4911e7d7716fe2f9f12` and executable SHA-256
`54415ed80303c5487884c834de978067f208cddbe9ae0779d8ca69ce801b9992`.
Both owners pass **534 integration and 494 frontier cases**, each with its
**243-case shared partition**, with zero skips, failures and leaks. All **48
Parquet/Iceberg E2Es pass in one run**, without skips, in 367.58 seconds on that
frozen executable. This validation does not extend the earlier timing comparison
to the newer main revision.

The merge keeps source capture and active document-local token leases ahead of
main's optional compiled-position cache. A primed-cache probe verifies one
analysis, one reuse and intact original offsets for both single and grouped
bindings. After adapting an inherited test's helper arguments, the compiled
contribution target passes **74 cases plus nine wrapper checks**, with no failures
or leaks; its opt-in substring benchmark is skipped. The later `d3bcfe751e`
change only restores shared work to the analyzer profiling interval, and that
focused target passes again. Apache boundary checks cover **2,582 source files**;
license, formatting and whitespace checks pass. The validation record preserves
the initial combined command's test-compilation failure and the successful
separate rerun instead of treating them as one all-green command.

A final independent main update, `ea7bea940a6a051f0662f87f2a240db2d925726d`,
merges cleanly as `2bfc6f98872258c3dd75b5647492a61c2b00342e`. It changes the
page-unit HTTP cursor gate and hierarchy error identity, with no lake
query/highlight kernel changes. Targeted qualification passes **47 error-transport
checks and one HTTP hierarchy check**, and both **local/Lite page-unit E2Es**
pass in 13.60 seconds. The final CLI builds successfully as SHA-256
`6f551b64a4e423ab4c953eb6b596443661be82d939ee72efdf65ab3d536f338d`, unchanged
before and after those flow tests. The validation record distinguishes this
focused merge check from the earlier complete lake-owner and real-file runs.


### Exact block pruning and direct typed result construction

Stored-source recipe v8, native reader protocol 94 and runtime ABI 70 add
producer-attested term spacing and block prefix-maximum ends. Eligible disjoint
dense streams stop when no future window can improve the retained candidate
heap. Overlapping spans retain the complete scan; fragment replay seeks to its
output intervals. Selected occurrence reads use the existing source-owned I/O
queue. A final refinement reserves hints for future reads, avoiding a hint just
before the foreground read of the same block. This remains optional advisory I/O;
cloud-latency benefits require provider qualification.

Native result builders allocate final scalar vectors directly and widen only
heterogeneous columns. Projected highlighting materializes only its own field
dependencies. The native nullable-i64 probe retains **297,120 bytes** for 32,768
cells, with a **297,144-byte** allocation peak and zero live bytes after release,
under 512 KiB. The old intermediate JSON matrix alone required 1,835,008 bytes,
before final vectors and page overhead. This is a result-page allocation probe,
not a complete query/process memory measurement. A separate 40,001-byte unrelated
JSON column stays encoded while highlight dependencies are checked under 64 KiB.

The [block/delivery observation](dense-blocks-typed-results-v1-observation.json)
retains **528 measured HTTP requests** across six runners in both binary orders.
All complete-result digests, runner hashes and Parquet input hashes match across
runs. The control `cbc97c3a12a47c41ecc7dfb8a930f792406df2cb` and measured runtime
`fac8914a2508a3dff901c79d4f7731c846c43dfa` share main
`d35a1ef33ef01482e593f892e3f534b780cc19bc`. Frozen executable SHA-256 values are
`738ad162c2a10de6bece40ac9b6b38cef5fd6b5d8180eee003aeb9e0441c28ca` and
`9a537e5dcfa2e6b9dc563190813df2c1ed50adba4ce58adb664c8d3cc226a9dc`.

Warm local Debug medians, milliseconds:

| Shape | Old → new, primary | Old → new, reverse |
| --- | ---: | ---: |
| Dense 2-MiB source | 85.14 → 11.09 | 92.75 → 10.38 |
| Variable-width Unicode 8-MiB source | 10.66 → 10.28 | 11.54 → 10.15 |
| 64 projected integer columns, 256 hits | 274.05 → 261.90 | 279.83 → 247.22 |
| One projected integer column, 256 hits | 56.85 → 60.83 | 56.50 → 55.47 |
| Rare fuzzy match | 11.07 → 12.88 | 11.84 → 11.35 |
| Hidden eight-field highlighting, 256 hits | 290.78 → 289.41 | 289.55 → 307.30 |
| Dense 128-KiB source | 15.22 → 12.64 | 14.34 → 11.94 |
| Sparse 32-KiB source | 10.15 → 11.39 | 9.57 → 11.27 |

Dense 2-MiB latency falls **87.0–88.8%**; 64-column delivery falls **4.4–11.7%**.
Small controls include regressions up to **1.80 ms (16.3%)** in the rare-term
cohort and **1.70 ms (17.7%)** in the kernel cohort. Hidden-wide highlighting is
6.1% slower in the reverse pass. Keep every point and qualify these observations;
there is no universal latency improvement. Seven samples per dense/Unicode,
rare-term and typed-delivery point, three per other point/order. The shared host
had separate-worktree build activity before comparison; no task-owned build,
E2E or cleanup overlaps the 24 runs. Publication admission/readiness polling is
one observation per run and does not isolate indexing CPU. No Release,
remote-provider, empty-cache or archive-scale claim follows from these timings.

Reproduce with `source_refinements.py`, `typed_results.py`,
`selective_offsets.py`, `source_pages.py`, `highlight_kernels.py` and
`hidden_highlights.py`, each taking `LABEL IMMUTABLE_BINARY SOURCE_COMMIT`.
Use old/new then new/old execution blocks and compare all hit/input/runner hashes.
`typed_results.py` independently writes 256 Parquet rows with 1/8/64 nullable
integer columns and checks exact wire values above `2^53`. A corrected fixture
smoke run overlaps native verification; its timings are discarded.

The exact selector oracle visits **512 of 32,769 occurrences** and decodes six
blocks while matching the full selector across widths and fragment counts.
The selective-read oracle grows from 37,343 to 38,897 encoded bytes and from
814 to 1,206 read bytes: each block reference adds four wire bytes and each term
adds eight. This storage/read tradeoff remains visible alongside the dense gain.
Before the final advisory-prefetch/main refinement, full native owners pass
**535 integration and 495 frontier cases**, each with a **247-case shared
partition**, zero skips/failures/leaks. These are 1,524 overlapping executions.
All **48 real Parquet/Iceberg E2Es pass in one run** in 365.33 seconds. The later
selected-term-head extension separately passes 61 shared and 13 API checks.
Final main/prefetch verification is recorded separately from these frozen runs.


The [final merge qualification](dense-blocks-postmerge-validation.json) records
main `981ce6733db455e72ef401d82e7f32b1225a03e2`, merged as
`70dfea22e16d72fee24cefc3292dd11e8c284823`, and runtime
`cd092741073cd361b169fa89052df9543c6e45a5`. Its frozen CLI SHA-256 is
`511ddb065d231e6542bfa871e43e85ff374fbd23cec06ae905ec9c29d502f34b`.
The future-read-only refinement passes **61 shared plus 13 API cases**. Main's
compaction/contribution target passes **75 cases plus nine wrappers**; its optional
substring benchmark is skipped. The first combined invocation's API selection
audit rejected a compaction filter belonging to the contribution target; the
corrected API invocation passes with zero failures, leaks or skips. The CLI
step completes, and the frozen executable passes all **three final real-file
flows** in 22.05 seconds: dense Parquet highlighting, dense Iceberg highlighting
and wide native source-column delivery. These are focused checks of the later
main/advisory-I/O changes, separate from the earlier full owner and 48-case runs.
License, Apache boundary (2,582 source files), lint, formatting and whitespace
checks pass. Cache GC is invoked; its active-native-build guard skips cleanup.

A separate staged comparison retains another **492 measured HTTP requests**
across five runners and both orders, comparing the earlier `fac8914a25` executable
with the final main/future-read-only executable. All successful result oracles,
input hashes and runner hashes agree. It includes the independent main update,
so it cannot isolate the prefetch refinement. Final dense 2-MiB medians are
**11.06 and 10.80 ms**. Some small controls are slower: one reverse-order sparse
2-KiB point is **10.72 → 13.78 ms (28.6%)**, while that primary point is
**12.18 → 11.78 ms**. Nullable integer delivery is 0.9–5.5% slower than the prior
refinement in this staged comparison. Every sample is retained; there is no
additional validated warm-latency win from the prefetch refinement. Its concrete
benefit is avoiding advisory scheduling of the next required foreground read.
The local Debug/shared-host qualifications above continue to apply. Across both
comparisons, all **48 structured HTTP 504 outcomes** carry `query_timeout`; other
sampled deadline outcomes return the expected results.

### Layout-aware highlights and shared covering batches

The [layout and covering observation](native-layout-highlights-and-cover-batches-v1-observation.json)
qualifies stored-source recipe **v9**, native reader protocol **95** and runtime
ABI **71**. Both binaries share main
`207cd9ca3ba6a5fb2c7db72c11b8a0e4f7479563`: the control is merge commit
`326f9064da`, and the measured runtime is `bf97d8a2cb`. The frozen helpers are
committed at `df10848c3c`. Executable SHA-256 values are
`9e0e7f6a525b042d425eaa976eda5b421bea942e6d604e3b07676a52638a2a84` and
`92fe670927cdb91777736faeddcdf5697bb87af8bacded50639fe9f2ae359c04`.

This refinement keeps exact snippet semantics while addressing four remaining
costs: regular-span density bounds account for candidate alignment; broad,
diverse matches use source-order occurrence blocks instead of one decoded head
per term; economical small fields retain offsets inside shared compressed
source blocks; covering hydration builds one typed result page per decoded
physical group. Small-field admission estimates dictionary cost before encoding
and caps total encoded variants/descriptors at twice the source byte length.
Rejected captures do not append to a private spill run. SQL NULL remains distinct
from a non-null JSON `null` in shared covering pages.

The comparison retains **144 measured HTTP requests**, in old/new then new/old
blocks. Complete successful hit digests, Parquet inputs and helper hashes agree.
The small-field and covering reports also retain equal totals; the layout runner
does not retain outer total metadata. The covering runner independently checks
all 64 integer fields across 256 rows, including values above `2^53`.

Warm local Debug medians, milliseconds:

| Shape | Old → new, primary | Old → new, reverse |
| --- | ---: | ---: |
| Regular stride-eight 2-MiB source | 90.55 → 10.36 | 88.50 → 9.74 |
| Prefix matching 20,000 distinct terms | 797.57 → 104.06 | 766.16 → 101.89 |
| Sparse 32-KiB control | 9.60 → 9.27 | 9.00 → 8.62 |
| Hidden eight-field highlights, 256 hits | 289.18 → 163.31 | 321.03 → 185.29 |
| Projected eight-field highlights, 256 hits | 255.39 → 202.92 | 286.38 → 221.50 |
| Hidden one-field control, 256 hits | 107.10 → 95.08 | 115.25 → 109.25 |
| Covering 64 integer columns, 256 hits | 445.30 → 388.81 | 460.15 → 386.26 |
| Same covering query after process restart | 459.11 → 398.08 | 469.77 → 395.23 |

Both orders support the intended improvements: **88.6–89.0%** for regular dense
text, **86.7–87.0%** for the diverse prefix, **42.3–43.5%** for hidden wide
highlighting, **20.5–22.7%** for projected wide highlighting, and **12.7–16.1%**
for warm covering delivery. The measured controls improve too; this does not
establish a universal latency improvement. Seven samples per layout point and
three per other point/cache mode, on a shared host. Process restart retains OS
and persistent caches and readiness warming; it is not an empty-cache test.

The small-field fixture retains **5.2–5.3%** more total native artifact bytes
and readiness rises from **1.67–1.74 s** to **2.02–2.08 s**. These are single
observations per run, include superseded objects and do not isolate live text
bytes or indexing CPU. The layout fixture's total artifact bytes vary between
orders; no storage reduction is claimed. Its readiness rises by **0.18–0.30 s**.
No Release, provider-latency, archive-scale or full-query-memory qualification
follows from these observations.

Independent exactness/work probes show **512 of 32,768 occurrences visited**
and **four decoded blocks** for stride-eight text, where the prior bound visits
all occurrences. The 8,192-distinct-term probe decodes **11 blocks**, keeps the
fragment kernel under **112,537 bytes** inside a 256-KiB budget, and agrees with
the full selector. Term navigation is outside that kernel allocation count and
remains proportional to selected vocabulary. A separate oracle checks **288
configurations of 4,096 disjoint or crossing spans** against the full selector.
Fault, cancellation, small-offset admission, inline compaction, crossing-span
seeks and shared-page ownership cases pass.

Before the final storage-admission policy, the merged branch passes **526 API
integration cases and 115 frontier cases**, with **250 and 35 shared cases**
respectively, zero skips/failures/leaks. The final runtime then passes **65
shared highlight cases and 13 API checks**, and all **48 real Parquet/Iceberg
cases are verified** on its frozen executable. The 48-case invocation passes 46
and exposes two overly strict new test assertions requiring whole words at
fragment edges. After replacing them with exact clipped source intersections,
both affected cases pass in **9.60 s**, including hidden/projected fields,
restart, unavailable original objects and structured deadlines. Runtime code
and executable bytes do not change between those E2E runs. An earlier invocation
used a basename that selected the harness's legacy API root; it was canceled
and the identical bytes rerun under the expected `antfly` name. The retained
validation record distinguishes these attempts.

Use `highlight_layouts.py`, `hidden_highlights.py` and `covering_batches.py`, each
with `LABEL IMMUTABLE_BINARY RUNTIME_SOURCE_COMMIT`, from the worktree root.
Run all three with the old binary, all with the new binary, then repeat in the
opposite binary order. Freeze binaries in separate directories with basename
`antfly` so the E2E harness selects `/db/v1`. Retain every sample, digest and
input/helper/executable hash; stop task-owned builds, E2Es and cache cleanup
before measuring. The covering report replaces redundant ID arrays with their
counts and hashes while retaining complete hit hashes and every sort profile.


## Compact match plans, suffix bounds and request projection plans

The retained [observation](native-compact-plans-and-projection-cache-v1-observation.json)
contains **244 measured HTTP requests** for the main refinement, followed by a
separate **112-request** qualification of projection-plan admission. Every
successful complete hit and outer total agrees within each comparison, including
IDs, scores, sort values, source masks and exact snippets. Parquet and helper
hashes match. The 64-column case independently checks every integer above `2^53`;
the exclusion cases require the same two physical source columns in both binaries.

The primary comparison uses control `fd30bc6ffd` and runtime
`599b748319`, both on main `04dd50548a`. The frozen
helpers are at `a6541a5d36`. Executable SHA-256 values
are `d7f122b4600ef784d8493b8c62f0a20cee3f8c610d6039e4ad75928b467037d1` and
`e893763401f47f2e315d18d866e693d1c00ebb4d0b2a764ae5ac5312be4fdb88`. These are local Debug observations,
with seven samples per layout shape and three per other shape/cache mode. Old/new
and new/old blocks contain no overlapping task-owned build, E2E or cache cleanup.

Warm/local medians, milliseconds (restart rows are labeled explicitly):

| Shape | Old → new, primary | Old → new, reverse |
| --- | ---: | ---: |
| Regular 2-MiB control | 10.65 → 12.03 | 11.29 → 12.79 |
| Prefix matching 20,000 terms | 103.10 → 99.94 | 104.11 → 103.64 |
| Sparse 32-KiB control | 9.69 → 10.83 | 10.35 → 11.86 |
| Early dense cluster, mixed-term remainder | 21.27 → 13.09 | 22.05 → 13.28 |
| Hidden eight-field highlights, 256 hits | 180.21 → 171.06 | 162.65 → 162.57 |
| Projected eight-field highlights, 256 hits | 221.01 → 207.56 | 202.33 → 194.63 |
| Hidden one-field control, 256 hits | 109.99 → 107.43 | 92.36 → 90.74 |
| 64 covered integer columns, process restart | 390.24 → 296.98 | 415.84 → 289.97 |
| 64 covered integer columns, warm | 381.91 → 281.19 | 391.87 → 278.18 |
| Explicit narrow projection, process restart | 117.52 → 75.34 | 81.32 → 74.94 |
| Explicit narrow projection, warm | 50.26 → 49.95 | 54.15 → 50.58 |
| Wildcard exclusions, process restart | 139.98 → 75.32 | 112.48 → 74.90 |
| Wildcard exclusions, warm | 91.85 → 50.38 | 101.17 → 49.85 |
| Exclusion-only mask, process restart | 107.00 → 75.36 | 113.74 → 74.37 |
| Exclusion-only mask, warm | 95.96 → 50.80 | 99.80 → 49.71 |

The layout fixture reports **1.6–1.8%** more total artifact-store bytes and
readiness increases by **0.10–0.28 s**. The small-field fixture reports
**0.3–0.5%** more bytes, with readiness changing by **−0.03 to +0.25 s**.
Each is a single readiness/storage observation per run, includes superseded
objects, and does not isolate live index bytes or ingestion CPU.

Both orders support **26–29%** faster warm wide covering delivery, **45–51%**
faster warm excluded-column delivery, and **38–40%** faster highlights after an
early dense cluster. Broad-prefix latency changes by only **0.4–3.1%**; the
compact navigation and reduced block work do not establish a large end-to-end
win for that shape. Hidden wide highlighting ranges from effectively unchanged
to 5% faster; projected wide highlighting improves by 3.8–6.1%.

The regular and sparse one-hit controls are **11.8–14.6% slower**, about
**1.1–1.5 ms**. That tradeoff is retained rather than hidden. A subsequent
admission rule avoids building a projection plan when only one hit can use it;
its separate same-main comparison does **not** demonstrate a material latency
gain or explain/remove the earlier controls regression. It is an allocation and
reuse policy, not a claimed point-query speedup. There is no universal latency
improvement claim.

Independent work/exactness probes show:

- After the early cluster, **2,304 of 32,769 occurrences** and **10 blocks** are
  visited, versus 32,769 occurrences and 39 blocks in the prior probe, with exact
  full-selector agreement.
- For 8,192 distinct singleton terms, navigation retains a **1,024-byte term-ID
  mask**, no selected term strings and no inverted read references. The fragment
  kernel visits at most 1,024 spans and decodes **four blocks**, down from eleven.
  Its **112,537-byte peak** excludes navigation and is not a full-query heap claim.
- Whole and half-vocabulary selections over irregular spacing agree with the
  exact selector for four widths; another **288 configurations** cover disjoint
  and crossing spans. OOM and cancellation checks remain active.
- Request projection plans retain no page leases, reuse one plan across 256
  closed producer pages, cap retained schemas at four, and preserve nested
  fallback, include order, duplicate/wildcard masks, hidden dependencies and
  canonical numeric extremes.

The representation uses stored-source recipe **v10**, reader protocol **96**,
runtime ABI **72**, and `AFOV0005` offsets. Old native representations rebuild.
See [the current serving contract](../../REMOTE_TABLE_SERVING.md#request-owned-projection-plans-and-scalar-delivery)
for ownership, physical masks and canonical metadata. A strict response oracle
caught synthetic `_id`/`_type` leakage on covering reads; covering and accepted
sources now preserve physical columns and declared type values/nulls, while
virtual identity is explicit and identity sorting does not inject source fields.
The unavailable-Parquet regression fails on the old wildcard path and passes
with both directions of physical-ID sorting, ascending virtual-ID sorting,
explicit identity, restart, and unavailable originals after the correction.

The final tree additionally merges main `1cfa5178d4`, resolving the append-only
serving/cloud-retention documentation conflict by preserving both designs. Its
lake execution files match the measured runtime until the separately qualified
one-hit admission rule. The record identifies each measured revision; it does
not relabel the earlier timings as measurements of a later executable.

Reproduce from the worktree root with `highlight_refinements.py`,
`hidden_highlights.py`, `covering_batches.py` and `excluded_columns.py`, each
accepting `LABEL IMMUTABLE_BINARY RUNTIME_SOURCE_COMMIT`. Run all four old/new,
then all four new/old, using fresh labels and frozen executables named `antfly`.
The layout fixture has its own directory and asserts every input ID/body before
starting a server. Retain all samples, full hit/total oracles and input/helper/
executable hashes. The follow-up uses the layout helper alone and separately
identifies main `1cfa5178d4`, control `c532682337`, and admission revision
`e23de32d9c`.

Process restart retains OS/persistent caches and readiness warming; it is not an
empty-cache measurement. Artifact bytes include superseded objects, and single
readiness observations do not isolate ingestion CPU. The observation retains
those storage/publication samples. No Release, cloud, archive-scale, or full-query
memory qualification is claimed. Real cloud retention enforcement remains a
separate qualification, as previously deferred.


Final admission runtime `e23de32d9c` passes **32 API and 77 shared native checks**
with no failures, skips or leaks, and **48 real Parquet/Iceberg cases** in
**366.37 s**. Those checks include direct single-hit transport, multi-hit plan
reuse, exact highlighting, canonical source metadata, allocation failures,
unavailable originals, accepted rows, restart and deadline propagation. Success
E2E requests allow 500 ms for scheduling variability; separate 1-ms cases assert
the public timeout outcome. Native work budgets and the dedicated comparisons,
rather than a fragile success wall-clock threshold, qualify efficiency.

Before the final metadata correction, the broader source `1812cfbd12` passes
**531 API plus 316 shared checks**, and **115 frontier plus 35 shared checks**,
with no failures, skips or leaks. The record keeps that revision separate from
the final focused qualification and retains its frozen executable identity.
The latest-main source `c532682337` also passes 32 API/75 shared cases and all
48 real-file cases before admission refinement. These stage-specific counts are
not combined into a distinct-test or final-runtime full-suite claim.

## Shared navigation, typed composition and chunked strings

[Retained comparison](native-typed-navigation-and-chunked-delivery-v1-observation.json)
qualifies runtime `f6f37aa34a` against control `503044369d` on common main
`e5aad86397`. The control includes the inference-owner objectstore import needed
by main's GCS credential contract. Corrected helpers are pinned at
`52961fac46`; frozen executable SHA-256 values and every sample are retained.
The 336-request record comprises 224 same-main requests and a separately labelled
112-request historical v9 reference. Neither executable changes during a run.

The runtime shares validated immutable source-offset roots/dictionaries through
the existing bounded decoded cache. Matchers and selected masks remain local.
Mixed physical/native result pages retain original typed vectors through parent
leases, preserving SQL NULL and exact integers. Large native strings decode into
UTF-8 chunks and escape directly into response residency/spooling; generic value
consumers keep an explicit materialization boundary. Inspecting projected text
length before highlight routing avoids a hidden contiguous scratch copy.
See [ownership and admission](../../REMOTE_TABLE_SERVING.md#shared-source-navigation-and-composed-typed-delivery).
Runtime ABI is **73**; recipe **v10**, reader protocol **96**, and `AFOV0005` stay
unchanged on disk.

Same-main medians, warm shared-host local Debug milliseconds:

| Shape | Old → new, primary | Old → new, reverse |
| --- | ---: | ---: |
| Prefix matching 20,000 terms | 91.80 → 79.12 | 92.33 → 78.86 |
| Native large strings and four integer columns | 51.39 → 40.05 | 55.52 → 41.65 |
| Native large strings only | 49.54 → 39.69 | 57.15 → 40.34 |
| Mixed explicit identity, strings and integers* | 44.40 → 40.99 | 50.57 → 42.57 |
| Mixed explicit identity and strings* | 43.53 → 40.09 | 49.54 → 40.03 |
| Regular 2-MiB one-hit control | 10.96 → 10.95 | 11.19 → 11.39 |
| Sparse 32-KiB one-hit control | 10.10 → 9.89 | 10.12 → 10.37 |
| Early cluster with mixed-term remainder | 12.10 → 11.98 | 12.25 → 12.98 |

Both orders support **13.8–14.6%** faster broad-term highlighting and
**19.9–29.4%** faster native large-string delivery. Regular/sparse controls range
from 2.1% faster to 2.5% slower; the cluster control is 1.0% faster in primary and
**5.9% slower (0.72 ms)** in reverse. No additional latency win is claimed for
those controls. Publication and artifact-store observations remain in the record;
single readiness samples and superseded artifact totals do not isolate build CPU
or live representation size.

*The baseline silently omits explicitly requested source `_id` when combined
with covered fields. The refined runtime supplies it directly from admitted hit
identity and does not reopen Parquet for it. These **7.7–19.2%** faster mixed
measurements include a correctness change and are not identical-output speedups.
The helper explicitly characterizes the known baseline defect using
`--baseline-missing-identity`; refined requests must return every requested field.
Complete physical-source hits/totals agree after excluding only source `_id`;
complete native/layout results agree without exclusions. Every refined identity,
UTF-8/escaped value, null and integer above `2^53` is independently asserted.

The historical v9 comparison uses source `fd30bc6ffd` on main `04dd50548a`, so it
is not a same-main causal comparison. Regular/sparse one-hit medians are 5.4–8.1%
faster in the refined executable in both orders. The earlier 11.8–14.6% one-hit
regression did not reproduce in this run; this does not establish that it was
causally removed. Historical broad/cluster results are retained separately and
are not substituted for the same-main comparison.

Native qualification passes **32 API + 80 shared checks**, with no failures,
skips or leaks. Three repeated source selections perform **two immutable decodes
and three raw reads**, retaining no query lease; current deadlines still reject
cache hits. Typed composition exercises reordered parents, SQL NULL versus JSON
null, canonical integers, UTF-8 chunk escaping, generic materialization and every
allocation failure. **48 real Parquet/Iceberg E2E cases pass in 369.78 s** against
the frozen refined executable. They include an 8-MiB Unicode projection with
explicit identity and highlights while originals are unavailable, before and
after restart. Existing 500-ms success and independent 1-ms failure cases remain.

Reproduce from the worktree root with `highlight_refinements.py` and
`typed_navigation.py`, each taking `LABEL IMMUTABLE_BINARY RUNTIME_SOURCE_COMMIT`.
Only the known control uses the latter helper's explicit baseline-identity flag.
Run old/new then new/old with fresh labels. The large-string fixture uses twelve
rows, six physical columns, bounded Parquet pages and one-row writer batches.
Combining a large string with 64 columns exceeded the existing equal-share scan
page budget even with bounded pages; that failed setup is not included as a
latency sample, no limit was raised, and the prior 64-column fixture remains a
separate qualification.

No task-owned build/E2E/cleanup overlaps these measurements. Other native builds
run on the shared machine; `make gc` safely skipped while one was active. These
are warm local Debug observations with readiness warming and retained caches.
There is no Release, cloud, empty-cache, archive-scale or full-query-memory claim.
Real-provider retention enforcement remains separately deferred.

## Live Parquet admission and compiled delivery

The [retained comparison](native-live-admission-and-compiled-delivery-v1-observation.json)
contains **252 measured HTTP requests**, seven samples per case in old/new and
new/old orders. Both source images include main
`c912271651567d2294b18971ed86f2cf7ecbf834`. The control is
`08c31ae364852bb686cd4d05688f36b8f3e1844c`, executable SHA-256
`03133ac3adb54795c5f6dca9275aff1b2366ab3323c021da8c33838f5fda0de2`;
the refined runtime is `feceea5ebe760a06797f732f069d46c4bb060037`, executable
SHA-256 `ebeac0813963a6946deac297382f2ff3e0971cbab1387518055918eaf96e339a`.
Subsequent qualification changes affect tests, the benchmark helper and documentation.

Warm local Debug medians, milliseconds:

| Shape | Old → new, primary | Old → new, reverse |
| --- | ---: | ---: |
| 128 native rows, 64 extra integer columns | 77.65 → 76.91 | 78.39 → 77.95 |
| Same wide results with explicit source `_id` | 102.03 → 81.91 | 102.67 → 82.57 |
| Physical SQL scan of all 66 columns | 83.63 → 78.16 | 83.75 → 85.83 |
| Large native ASCII strings | 36.38 → 28.21 | 44.06 → 35.18 |
| ASCII strings with explicit identity | 37.19 → 28.94 | 43.79 → 35.75 |
| Large escaped/Unicode native strings | 37.65 → 33.90 | 41.40 → 34.02 |
| Escaped strings with explicit identity | 38.60 → 34.30 | 41.81 → 34.42 |
| Global exact disjunction over 128 terms | 63.87 → 56.62 | 59.85 → 57.18 |
| Global bounded 128-term prefix | 50.12 → 47.01 | 46.96 → 46.38 |

Compiled composition improves the wide explicit-identity case **19.6–19.7%**.
Native ASCII delivery improves **20.1–22.5%**; escaped/Unicode native delivery
improves **10.0–17.8%**. The global exact-term case improves **4.5–11.4%**, and
the bounded prefix improves **1.2–6.2%**. These HTTP observations measure the
combined refinements, rather than isolating a single kernel. Native-only wide
results remain within 1%. The physical SQL control ranges from **6.5% faster to
2.5% slower (2.08 ms)**; no scan latency win is claimed. The admission change
instead broadens the set of page cohorts fitting unchanged resource limits.

Every complete hit and total hash agrees across all four runs, including source
`_id`, exact integers above `2^53`, SQL integer strings, NULLs, Unicode and
escaping. The record retains every sample, profile, source/input/helper/binary
identity and individual report hash. The helper is
[live_delivery.py](live_delivery.py), SHA-256
`5a9f0749a565e0bd884bbd1526bdf0a022b95a07327c17e87e12bdec4e894130`.

The statistics fixture uses two independently written Parquet files with 128
distinct terms. Both `admission_window_segments` and `admission_window_bytes`
are set to 1, and every warmup asserts `bounded_global_text_frontier`. Every
measured profile also reports that plan. Setting only the segment threshold did
not suffice: compaction let an earlier 32-row fixture use ordinary whole-window
search. Its exploratory observations are excluded from this comparison. A
separate native work probe resolves **529 requested frequencies in two tree
visits with zero new decodes**, preserving duplicate positions and missing terms.
The old implementation created a cursor for each term; the work probe does not
claim a reduction in warm provider GETs.

Cursor tests admit a **66-column cohort containing a 256-KiB string** under
unchanged 2-MiB input/uncompressed and 2-MiB structural allowances, locally and
with cold/warm decoded caches. The old equal-share page check allowed only about
31 KiB per column. This is a cursor admission test, not a public-query latency
comparison. Separate tests reject aggregate vector/arena capacity even when raw
scalar bytes fit, retain credits through result leases and cursor teardown,
preserve dictionary ownership, refund injected allocation failures, and wake or
cancel parallel input waiters without increasing their allowance. Scheduler
fan-out estimates remain distinct from the shared live-byte admission rule.

Reproduce from the worktree root:

```sh
TZ=UTC zig/e2e/antfly/.venv/bin/python -B zig/bench/lake/live_delivery.py \
  LABEL IMMUTABLE_BINARY RUNTIME_SOURCE_COMMIT wide
```

Repeat with `ascii`, `escaped` and `statistics`, then reverse the executable
order using fresh labels. Wide mode also measures an ordinary physical SQL
scan. The helper verifies independently written Parquet values, every result,
all requested fields and the statistics plan before accepting samples.

No task-owned build, E2E or cleanup overlaps the timing runs. Other native work
runs on the shared host. These are warm local Debug observations with retained
caches and readiness warming. There is no Release, cloud, empty-cache,
archive-scale or full-query-memory qualification. Actual cloud retention
qualification remains deferred.

Final qualification passes **167 focused native checks** (117 API/frontier and
50 shared), with no failures, skips or leaks. The lease case is also repeated
with local and cached pages surviving cursor teardown. **50 real Parquet/Iceberg
E2E cases pass in 363.33 seconds** against the frozen refined executable,
including the new 66-column unequal-page SQL test for both formats, cold/warm
reads and restart. Existing hidden/projected highlights, unavailable originals,
exact signed decimals, deletes, native ordering, refresh and deadline cases
remain covered. The preceding 50-case run also passed before the final fan-out
correction; only the final executable supplies the timings above.

## Shared text pages and compact metadata writes

The [retained observation](native-page-leases-and-compact-writes-v1-observation.json)
qualifies runtime `8823bc6952824a913757bc2dae4f1a3c272b5160` against control
`c631577a9d4ab095ca3738fa0c19c39afbe95ebe`. Both include main
`c0705f12640e5fc9e52f289a55f7268a54fd1682`. The control is an ancestor of the
final merge, with the preceding lake runtime and the same main changes. During
its build, every `zig/pkg` file was verified against that committed control;
the committed candidate was restored before its separate build.

Frozen Debug executables:

- Control SHA-256: `82d3c9a083b1de43d9accf394d866a6fa795db4a4a6e4b896b069b0925a61976`.
- Candidate SHA-256: `892e44175aaf28cd2c6b36c80632d085fb20920a05011642dd9d9e678b2dd1a6`.

The latest changes fix reader-local cold-flight quota failures, share native
source text through decoded-page leases, separate Parquet fetching from CPU
decoding, reclaim typed codec scratch, and preserve compact keys through writes
and maintenance. Runtime ABI is **75**, native reader protocol **97**. The
stored-source recipe and page wire layouts remain unchanged. Protocol 97 fences
new publications whose encoded pages exceed the older key-expansion policy.

The HTTP comparison records **252 measured requests**, with seven samples per
case, in both executable orders. Independent Parquet values and complete
hit/total hashes agree across every sample and both binaries. The helper,
fixture, executable and complete-report hashes are retained alongside all
samples. Statistics requests still assert the bounded global frontier plan.

Warm local Debug medians, milliseconds:

| Shape | Control → candidate, primary | Control → candidate, reverse |
| --- | ---: | ---: |
| Large native ASCII strings | 28.518 → 20.152 | 35.596 → 27.213 |
| Large ASCII with explicit source identity | 29.143 → 20.972 | 36.164 → 27.665 |
| Large escaped/Unicode native strings | 34.233 → 26.258 | 34.311 → 25.860 |
| Escaped/Unicode with explicit source identity | 34.637 → 26.724 | 34.651 → 26.517 |
| Native-only wide control | 77.549 → 77.149 | 77.308 → 77.201 |
| Wide composition control | 83.900 → 83.038 | 83.697 → 82.745 |
| Physical SQL scan control | 78.299 → 78.480 | 77.676 → 78.833 |
| Global exact term control | 57.621 → 57.754 | 57.474 → 57.431 |
| Global prefix control | 47.365 → 47.880 | 47.533 → 47.369 |

Native ASCII delivery improves **23.5–29.3%** and escaped/Unicode delivery
**23.3–24.6%**. The identity variants improve **23.5–28.0%** and
**22.8–23.5%**, respectively. Wide and statistics controls do not establish
material gains. The physical SQL control is **0.2–1.5% slower**; no scan win is
claimed. These results measure the current refinements, separately from the
preceding live-admission and compiled-composition comparison above.

A separate **ReleaseSafe production-code stress probe** builds 512 ordered
131,000-byte keys sharing a long prefix, deletes one key, updates another and
walks GC records. Both implementations verify every full returned key/value,
record count, GC row count and allocation refund. Six alternating pairs retain
all samples, including the first pair. This deliberately wide synthetic input
qualifies compressed occupancy and work amplification, not archive throughput.

| Kernel | Control median | Candidate median | Speedup |
| --- | ---: | ---: | ---: |
| Initial build | 43.769 ms | 19.795 ms | 2.21× |
| Two-key edit | 3.530 ms | 0.238 ms | 14.86× |
| GC traversal | 8.600 ms | 0.609 ms | 14.13× |

Initial publication uses **77 → 1 pages**, height **3 → 0**, and
**10,102,296 → 136,694 encoded bytes**. Cumulative operation allocations fall
from **132,068,740 → 993,246 bytes** for construction,
**10,246,390 → 575,391** for edits, and **86,942,492 → 361,933** for GC.
These are allocated-byte totals, not peak memory measurements.

Reproduce the kernel with the same worktree cache:

```sh
mkdir -p .benchmark-results/lake/page-leases-v1/before
git show c631577a9d4ab095ca3738fa0c19c39afbe95ebe:zig/pkg/antfly/src/serverless/graph_segment/page_tree.zig \
  > .benchmark-results/lake/page-leases-v1/before/page_tree.zig
~/bin/zig test -lc -O ReleaseSafe --cache-dir zig/.zig-cache \
  --global-cache-dir zig/.zig-cache/global \
  --test-filter compact_write_production_comparison --dep before --dep after \
  -Mroot=zig/bench/lake/compact_write_probe.zig \
  -Mbefore=.benchmark-results/lake/page-leases-v1/before/page_tree.zig \
  -Mafter=zig/pkg/antfly/src/serverless/graph_segment/page_tree.zig
```

For HTTP runs, build and freeze both source revisions first. Run
`live_delivery.py LABEL IMMUTABLE_BINARY RUNTIME_SOURCE_COMMIT MODE` with
`ANTFLY_LAKE_DELIVERY_RESULTS` pointing to an isolated absolute output root.
For each of `wide`, `ascii`, `escaped` and `statistics`, run control/candidate,
then candidate/control under fresh labels. Timing runs must not overlap this
task's builds, native tests, E2E or cleanup.

Final qualification passes **438 distinct native checks**, with no failures,
skips or leaks, and **50 real Parquet/Iceberg E2E cases in 367.42 seconds**
against the frozen final executable. Native coverage includes reader-local
cold-flight retries; typed column/dictionary allocation faults; result pins
surviving cursor teardown and unavailable source bytes; rejected or non-consuming
callback transfers; UTF-8 page boundaries; delayed fetches leaving CPU admission
free; cancellation; wide compressed-key updates, retention, differences and GC;
and legacy explicit expansion limits. Both real-file formats retain cold/warm,
restart, highlights, exact decimals, deletes, ordering and refresh coverage.

The committed control independently fails the existing 145,186-row, 12-column
cached cohort with `ParquetPageTooLarge` under 32-MiB raw and 32-MiB structural
allowances. The candidate passes unchanged limits, checks peak residency at or
below 64 MiB, and asserts reclaimable cached vectors retaining fewer than 16
bytes per row per column. Codec staging arrays no longer remain in cache arenas.
This is a native cursor admission qualification, not a public-query memory claim.

Other work on the shared host is uncontrolled. HTTP observations use warm local
Debug with process/OS/persistent caches and readiness warming. There is no cloud,
empty-cache, archive-scale, Release HTTP or full-query-memory qualification.
Real-provider cloud retention qualification remains deferred.


## Physical page admission and compact routing fences

Runtime `306ec780312f95f215a156fc245a4ea5212c2e3e` includes main
`7ca4b04b814c13310c27718dea1ffd7059f5f6b1`. The new
[retained observation](physical-page-admission-and-compact-fences-v1-observation.json)
compares the production kernels with control
`f3b3b458c06166ee5fa126179fff502acf7a31f3`, the clean main merge before these fixes.
Both variants use ReleaseSafe and the same process/allocator. Each probe records
six pairs, alternating which variant runs first. No task-owned build, native
check, E2E or cleanup overlaps timing; other host work is uncontrolled.

| Kernel | Control median | Candidate median | Ratio |
| --- | ---: | ---: | ---: |
| One value update in a dense routing tree | 0.898 ms | 0.532 ms | 1.69x |
| Decode 1,024 independent PLAIN i64 pages | 21.436 ms | 11.421 ms | 1.88x |

The routing fixture has 512 keys of 16,384 bytes, with 32,768-byte values. It is
built by the production sorted builder and has height two; this deliberately
exercises dense **internal** pages rather than just one compressed leaf. Every
returned key/value is checked independently. Initial construction remains 515
pages and 25,284,367 encoded bytes in both variants. Build and GC allocation totals
are unchanged, and no new improvement is claimed for those controls.

The edit's cumulative requested allocation falls from **8,110,540 to 701,009
bytes**, a **91.4% reduction**. The separate native allocator test reports 705,713
bytes with its testing allocator, returns every allocation, and succeeds with a
4-MiB allocator. The original implementation fails that same bounded fixture.
Shared immutable seeds and owned suffixes also pass allocation-failure injection.

The scanner fixture contains 8,192 required PLAIN i64 values in each 65,536-byte
payload. It validates the complete vector outside the timed interval and checks
selected values on every iteration. Cumulative allocation over 1,024 iterations
falls from **235,012,096 to 134,217,728 bytes**. These are allocation totals, not
peak memory. A separate live-budget test measures **65,536 retained bytes** and a
**131,072-byte peak**. The old scanner cannot fit that payload-plus-final-vector
budget; the candidate passes. Required, nullable, dictionary, and compact delta
string pages adopt their final owned vectors/buffers, while multi-page chunks
retain aggregation. Single- and multi-page compact string paths also pass every
allocation-failure point.

Physical scan scheduling is qualified separately from kernel timings. The gated
native provider test covers default and constrained quotas, validated OffsetIndex
ranges and unindexed headers, cancellation, unrelated CPU work, and zero available
I/O workers. It skips a tiny earlier page, blocks both larger surviving payload
reads simultaneously, and asserts exact physical I/O reservations with no active
CPU workers. Thus directory pruning cannot leave a reservation sized for the
skipped page. No provider-latency percentage is inferred from these gates.

Final focused qualification passes **153 shared and 17 API checks**, with no
failures, skips or leaks, including the final indexed-range and compact string
refinements. **47 standalone ReleaseSafe tree checks** also pass. A broader
preceding run passed 263 shared and 531 API-selection checks before those last
refinements; one unrelated `fresh sparse fragment mutations keep quantization
error bounded across updates` stress test was explicitly excluded after running
CPU-bound in existing storage code for over twelve minutes. These suites overlap;
the counts must not be summed as distinct checks. Final real-file qualification
and the frozen executable identity are recorded in the observation. The final
candidate passes **50 real Parquet/Iceberg E2E cases in 367.76 seconds**, including
cold/warm reads, restart, unequal wide SQL projections, highlights, exact decimals,
ordering, deletes and refresh. Its executable SHA-256 is
`4d97fc7e24213996103df0aedaad60df441c15e3c2f6eecb4164c1ee0ec4d680`.

To reproduce, run from the worktree root, retaining dependencies under each
snapshot's `src` directory:

```sh
results=.benchmark-results/lake/physical-pages-v1
mkdir -p "$results/before/src" "$results/after/src"
git show f3b3b458c06166ee5fa126179fff502acf7a31f3:zig/pkg/antfly/src/serverless/graph_segment/page_tree.zig \
  > "$results/before/page_tree.zig"
git archive f3b3b458c06166ee5fa126179fff502acf7a31f3:zig/pkg/antfly-embedded/src \
  | tar -x -C "$results/before/src"
git archive 306ec780312f95f215a156fc245a4ea5212c2e3e:zig/pkg/antfly-embedded/src \
  | tar -x -C "$results/after/src"
for variant in before after; do
  cat > "$results/$variant/src/root.zig" <<'ZIG'
pub const page = @import("serverless/query/lake_parquet_page.zig");
ZIG
done
~/bin/zig test -lc -O ReleaseSafe --cache-dir zig/.zig-cache \
  --global-cache-dir zig/.zig-cache/global --test-no-exec \
  -femit-bin="$results/compact-fence-probe" \
  --test-filter compact_fence_production_comparison --dep before --dep after \
  -Mroot=zig/bench/lake/compact_fence_probe.zig \
  -Mbefore="$results/before/page_tree.zig" \
  -Mafter=zig/pkg/antfly/src/serverless/graph_segment/page_tree.zig
~/bin/zig test -lc -O ReleaseSafe --cache-dir zig/.zig-cache \
  --global-cache-dir zig/.zig-cache/global --test-no-exec \
  -femit-bin="$results/physical-page-probe" \
  --test-filter physical_page_production_comparison --dep before --dep after \
  -Mroot=zig/bench/lake/physical_page_probe.zig \
  -Mbefore="$results/before/src/root.zig" -Mafter="$results/after/src/root.zig"
```

Finish all task-owned builds/checks first, then run each probe serially and retain
its complete output. The observation includes all raw samples, exact helper and
source hashes, executable hashes, and independent count/value/memory oracles.
These are synthetic in-memory kernels. They establish neither a new HTTP scan
speedup nor cloud, empty-cache or archive-throughput performance. Earlier HTTP
observations remain separately attributed to their original revisions.


## Borrowed payloads and parked row-group lookahead

The [retained observation](borrowed-payloads-and-parked-lookahead-v1-observation.json)
compares merged control `945f7db284763fd441c8f6b05d75390c3bafbdb6` with runtime
`3196cec646e6259407e5d993f06c4d450fe96ea2`. Both include main
`b2f6226fbdc4baa7387fe2ca601833f5a48c6f1e`. Runtime ABI is **76**; native reader
protocol remains **97**. This comparison is separate from the previous
final-vector transfer and compact-fence observations.

For 1,024 independent 8,192-row uncompressed PLAIN i64 pages, six alternating
ReleaseSafe production-scanner pairs give median **13.106 → 6.632 ms (1.98×)**.
Cumulative requested allocation falls **134,217,728 → 67,108,864 bytes (50%)**.
A full-vector oracle runs before each timed sample; timed iterations sample an
independent expected value and require balanced allocation ownership.
A 64-KiB fixed allocator rejects the control and admits the candidate. The native
budget check separately reports **65,536 retained bytes and a 65,536-byte peak**
in the decoder allocator. The fetched input lease is outside that allocator and
remains separately accounted for. These figures establish an uncompressed
integer-kernel improvement, not an HTTP, archive, cloud or empty-cache speedup.
Compressed pages still own decompressed storage. Deferred projection framing
explicitly owns its payload; poisoned-input checks exercise later nullable
string and numeric evaluation, including allocation failures.

The row-group lookahead change has a separate progress oracle. Controlled
one- and two-column provider gates verify zero occupied CPU slots while reads
are parked, progress by an unrelated CPU task, cancellation and full cleanup.
CPU/I/O saturation drops optional work instead of executing it inline, while a
required cursor completes under the same pressure. Optional pull coordinators
leave half the descriptor and retained-memory capacity available to required
operators. No provider-latency percentage is inferred from those gates.

The final source passes **158 shared and 17 API focused checks**, with no
failures/skips/leaks, plus **12 standalone ReleaseSafe scheduler tests**.
These suites overlap and must not be summed as distinct checks. The observation
records final real-file qualification and the frozen executable identity.
Apache boundary (2,338 source files), license headers, formatting and whitespace
checks also pass. The final frozen executable passes **50 real Parquet/Iceberg
E2E cases in 365.78 seconds**, including cold/warm reads, restarts, unequal wide
projections, hidden/projected highlights, exact decimals, deletes, ordering and
refresh. Its SHA-256 is
`92a2556ffcd5d745cdca420606e6029069618c89afb9f4afb7529180fd9f3dc5`.

Reproduce the decoder comparison from the worktree root:

```sh
results=.benchmark-results/lake/borrowed-payloads-v1
mkdir -p "$results/before/src" "$results/after/src"
git archive 945f7db284763fd441c8f6b05d75390c3bafbdb6:zig/pkg/antfly-embedded/src \
  | tar -x -C "$results/before/src"
git archive 3196cec646e6259407e5d993f06c4d450fe96ea2:zig/pkg/antfly-embedded/src \
  | tar -x -C "$results/after/src"
for variant in before after; do
  cat > "$results/$variant/src/root.zig" <<'ZIG'
pub const page = @import("serverless/query/lake_parquet_page.zig");
ZIG
done
~/bin/zig test -lc -O ReleaseSafe --cache-dir zig/.zig-cache \
  --global-cache-dir zig/.zig-cache/global --test-no-exec \
  -femit-bin="$results/borrowed-payload-probe" \
  --test-filter borrowed_payload_production_comparison --dep before --dep after \
  -Mroot=zig/bench/lake/borrowed_payload_probe.zig \
  -Mbefore="$results/before/src/root.zig" -Mafter="$results/after/src/root.zig"
"$results/borrowed-payload-probe" > "$results/probe-results.log" 2>&1
```

Finish task-owned compilation, native tests, E2E and cleanup before timing the
probe. Other host activity is uncontrolled. All twelve raw samples, source/helper
hashes, probe hash, executable identity and memory oracles are in the observation.
Real-provider retention qualification remains deferred.

## Pressure-safe lookahead and leased publication

The [retained observation](phase-safe-prefetch-and-leased-publication-v1-observation.json)
qualifies implementation `f8f08c5da0` with main
`9986c8a10750eea05d20d5c966f38cd76f7f0ee8` merged. Later source commits register
and refine its tests. Runtime ABI is **77**; reader protocol remains **97**.

Controlled CPU/input pressure previously stranded an optional Parquet read even
after unrelated CPU work released admission. Speculative input reservations now
try once, and CPU admission retries wait only for a live sibling decoder. The
one/two-column gates check pressure decline while foreign CPU work is still
gated, required-read progress under the same limits, cancellation and complete
reservation cleanup. Separately, a three-page warm text read now emits **zero**
redundant raw prefetch hints (previously two), with zero new demand reads or
decodes. Declining one resident proof retains exactly that page's hint; authority
revocation after acquiring a lookup lease releases it before returning an error.

Index publication now consumes leased sort records and bounded physical/frozen
column windows. A same-binary Debug comparison retains the previous owning
`Sort.next` drain as the control. It checks every key and payload byte for 1,024
unique 2-KiB records, excludes setup/add, and includes first advancement/finish
and the complete drain. Six alternating pairs follow one warmup per variant at
each sort budget. Cumulative requested drain allocation falls:

| Sort budget | Owning control | Leased records | Reduction |
|---|---:|---:|---:|
| 8 MiB | 8,882,218 bytes | 3,307,562 bytes | 62.8% |
| 64 KiB | 9,328,010 bytes | 4,283,196 bytes | 54.1% |

Both budgets perform spill I/O, and encoded read/write counts match exactly
between variants. Thread CPU medians are **13.266 → 12.193 ms (1.09×)** and
**17.425 → 17.185 ms (1.01×)** respectively. Each budget includes one slower
candidate sample; all twelve pairs remain recorded. These results support
removing transient allocations, with only modest measured CPU improvement.
They establish neither live/peak RSS savings nor HTTP, archive, cloud or complete
publication throughput. Task-owned builds, tests and cleanup did not overlap
timing; unrelated compilation and other host activity were uncontrolled.

The owning and leased covering publishers also produce identical pack/directory
content addresses for resident and spilled input, including nested JSON, SQL
NULL and integers above 2^53. Focused checks cover allocation failures, repeated
EOF, bounded row/byte windows, aggregate spills and oversized-record fallback.
The merged runtime passes **25 shared and 31 API checks** with no failures,
skips or leaks. Three publisher checks and the enabled benchmark run separately;
these overlap and must not be added as distinct checks.

Reproduce from the worktree's `zig/` directory:

```sh
ZIG_GLOBAL_CACHE_DIR=.zig-cache/global ~/bin/zig build lake-api-test \
  -Dtest-filter='external lake leased publication' --summary all \
  > /tmp/lake-publication-build.log 2>&1
```

Freeze the emitted `lake-api-tests` executable reported in that build's run
command, recording its SHA-256 and source identity. After all task-owned builds,
tests and cleanup finish, run that executable directly with
`ANTFLY_LAKE_LEASED_PUBLICATION_BENCH=1` and
`--suite-filter 'external lake leased publication benchmark sorted record drain'`.
The observation contains the exact probe identity, source hashes and all samples.

The frozen CLI built from source
`5506d75a0d43e30a1bf43e7068bdfc58a7a9bff4` passes **50 real Parquet/Iceberg
E2E cases in 375.70 seconds**, including cold/warm reads, restarts, unequal wide
projections, hidden/projected highlights, exact decimals, deletes, ordering and
refresh. Its SHA-256 is
`6c03d86f83c245df81f268173e65bfe7b48626871a4e2390b5fd8626e7e68a2a`.
Apache boundary (2,338 source files), license, formatting, final source/binary
identity and whitespace checks pass. `make gc` completed its safety check; the
collector skipped deletion under its native-build/process-inspection safety guard.
Real-provider retention qualification remains deferred.

## Adaptive publication and wide covering reads

The [retained observation](adaptive-publication-and-covering-v1-observation.json)
qualifies runtime `5549fbc7e1138a8f4bfb94fd891afcad48c928ae` with main
`2011275565877bae7deff19b786b82f371a45345` merged. Runtime ABI **77**, reader
protocol **97** and persisted layouts remain unchanged.

A cold **128-column, 32-row** covering pack previously required **nine range
GETs** for **72,622 bytes**. Planning the complete selected stripe set before
splitting bounded physical job waves reduces that to **one GET for the same
bytes**. Warm reads make zero GETs. A fresh-scope sparse projection fetches only
identity and its two required stripes, in two GETs; uncached full projection
also uses one GET. Every selected value is checked independently. These are
instrumented FS provider-call counts, not cloud-latency or query-throughput
measurements.

Publication now shares keyed sort admission and unread-reference retention with
result delivery, while keeping its own **256-row / 512-KiB caller target**.
Ordinary indirect result delivery retains its **64-row / 128-KiB target**. Under
**1 MiB additional headroom**, an 8,192-row, eight-column fixture previously
failed before its first publication batch. The adaptive stream drains every row,
with every sorted key and ordinal-tagged value independently checked. Existing
owning and grouped-delivery controls also complete. Both resident and spilled
32-row mixed-type covering fixtures retain a full pack without pressure and
match owning artifact bytes. Independent publications use independent input
sorts; leased publication is single-pass.

Range tests cover all-wave byte/gap charging, zero-budget refusal, partial-wave
and allocation failures, and cancellation of an earlier parked provider after a
later read fails. The final source passes **114 shared and 81 API checks** with
no failures, skips or leaks. The frozen server passes **50 real Parquet/Iceberg
E2E cases in 371.08 seconds**, including cold/warm reads, restarts,
highlights, wide projections, exact values, deletes, ordering and refresh.
Its SHA-256 is
`501cd6347f8289535ad58a09e3bf83966ae35c3eb96a5f9810e9901f5f24f3b7`.
The observation records exact commands, source hashes and retained log hashes. Apache boundary (2,338 source files),
license, formatting and whitespace checks pass. Earlier kernel measurements
retain their original source identities; no new CPU or RSS claim is made.
Real-provider retention qualification remains deferred.

## Adaptive spill residency and notified joins

The [retained observation](adaptive-spill-cache-and-notified-joins-v1-observation.json)
qualifies runtime `69c65746520df8591e7721d8636522c792dd9bdc` with main
`c2896f43244e2e46d1cd1e294d93fde8604989f4` merged. Runtime ABI **78** fences the
changed in-process spill owners; reader protocol **97** and artifact formats
remain unchanged.

A fixed four-entry, 64-KiB payload cache repeatedly decoded the same physical
bodies during wide full drains. The production cache now uses an indexed LRU,
counts shared owners once, and adapts within existing statement admission.
Scoped managers and direct statement-budget allocations reclaim idle sibling
roots. Reclamation skips busy caches and allocator paths; only refundable frees
can reuse locks held by the current thread. Arena, monotonic and opaque paths
retain conservative caching outside shared reclamation. Allocation failures,
duplicate physical-address allocations, cancellation, pins, direct/nested
budgets, shared-lock arena growth and independent manager registries are tested.

An 8,192-row fixture checks every sorted key and ordinal/column tag across eight
128-byte strings. The same final Debug binary compares fixed and adaptive
policies with **32 MiB post-finish headroom** in both. With Snappy compression,
spill bytes read during the drain fall **70,618,654 → 5,166,118**;
payload reads fall **8,192 → 512**, with unchanged writes.
Total setup-plus-drain read/write bytes fall
**93,252,674 → 27,800,138**.
The uncompressed comparison and every raw sample remain in the observation.

After one warmup per variant, six alternating Snappy pairs have median drain
thread CPU **1022.116 → 117.790 ms (8.68×)**.
Setup/add and Sort.finish are excluded; first page advancement, the full drain
and independent oracles are included. Peak tracked live allocation increases
**403,053 → 13,829,101 bytes** as the cache uses available
memory; both retain the same **33,594,433-byte** limit. This is
a spill I/O and native drain CPU result, with no RSS, archive, cloud, HTTP or
whole-publication throughput claim. Task-owned builds, tests and cleanup did not
overlap timing; other host activity was uncontrolled.

The outer covering wave now shares completion-driven joins with raw ranges. A
later immediate failure cancels a blocked earlier sibling, preserving the
injected error instead of waiting for its two-second safeguard and returning
Timeout. Caller cancellation joins all child work before callback teardown.

Final qualification passes **266 shared and 85 API checks**, with no failures,
skips or leaks, and **50 real Parquet/Iceberg E2E cases in 383.50 seconds**.
The frozen CLI SHA-256 is
`58d1a35b8b0ba4a145e48db9125f4a23bfb04f720de07689cbbb48b241508ca7`.
The observation records native executable identity, commands, source and log
hashes. Apache boundary (2,338 files), license, formatting and whitespace checks
pass. Real-provider retention qualification remains deferred.

## Host admission and leased covering ranges

The [retained observation](host-admission-and-leased-ranges-v1-observation.json)
qualifies runtime `b872ea60c53bc47f6b426a681a5ded366e476123`, with main
`ebb4bfce947952a7ec2198a4ae3ad13fb7308289` merged cleanly. Runtime ABI **79**
fences the shared host-budget and residency layouts; reader protocol **97**
and persisted artifact formats remain unchanged.

The earlier adaptive spill measurement used a standard backing allocator.
The public SQL factory inserts a ResourceManager host allocator, which previously
kept production SQL on the conservative cache policy. Direct opt-in host owners
now participate in shared, round-robin optional reclamation without replacing
allocation paths or bypassing host and statement admission. Owned reservation
credit counts as reusable headroom. Busy caches and allocator paths are skipped;
pins survive eviction, and only refundable frees can reuse held locks. Arena,
monotonic and unknown paths retain conservative policy. The generic registry
lives in the platform allocator contract. A single-threaded native probe also
checks that page-allocator use no longer instantiates the threaded SMP allocator.
The full WASM bundle was not built.

Covering reads now retain authenticated immutable raw-cache or physical-response
leases instead of copying each stripe into an intermediate owned buffer.
Uncached response owners outlive their wave through reference counts and stable
caller allocators. Cached units still pass through verified admission and retain
their original persistent-cache identity; decoded cache entries keep their own
wire framing. Owning range callers retain their adapter. Exhaustive allocation
failure checks cover cached and uncached partial results.

A 2-MiB fixture of 32 adjacent authenticated units checks every byte. Both
variants include an identical simulated decoded-owner wire copy. Requested
caller allocation falls **6,308,312 → 4,212,736 bytes** without caching,
**4,225,614 → 2,130,995 bytes** on cached cold input, and
**4,215,070 → 2,119,481 bytes** on raw-cache-warm input: about **33–50% less**.
Provider work is identical: one 2-MiB filesystem range GET cold, zero GETs on
cached warm input, and one GET on an uncached repeat. These are caller allocation
counts, excluding backend cache allocations; no CPU, peak-memory, cloud or
whole-query throughput gain is inferred from this range kernel. Decoded-cache
hits already skip this wire path; this improvement serves decoded-cache misses.

The spill comparison now uses the **public `sql_memory_owner` factory and real
128-MiB ResourceManager host admission**. Both policies retain the same
33,594,433-byte statement limit and 32 MiB post-finish headroom. In the same
frozen Debug binary, six alternating Snappy pairs have median full-drain thread
CPU **978.761 → 122.732 ms (7.97×)**, with no slower candidate. Setup/add and
Sort.finish are excluded; first page advancement, full drain and independent
key/value oracles are included. Drain spill reads fall
**70,618,654 → 5,166,118 bytes**, payload reads **8,192 → 512**, with unchanged
**12,043,408-byte** writes. Tracked live peak rises
**403,053 → 13,829,101 bytes** within the identical limit. Both policies check
all 8,192 sorted keys and eight ordinal-tagged value columns. The raw samples
also include the uncompressed control. Builds, E2E and cleanup from this task
did not overlap timing; other host activity was uncontrolled. No RSS, cloud,
HTTP, archive, whole-query or whole-publication throughput gain is claimed.
The older bare-allocator 8.68× observation retains its historical identity.

Final qualification passes **351 shared and 87 API checks**, with no failures,
skips or leaks, plus **50 real Parquet/Iceberg E2E cases in 381.23 seconds**.
Both threaded host-owner and page-allocator-only single-threaded native probes
pass. The frozen CLI SHA-256 is
`3e6927f26cd07fd9a9f530bd9b533808f01e9cdbdaa365abfdd6e5a50ec48389`.
The observation retains exact commands, native executable identity, source and
raw-log hashes. Apache boundary (2,345 source files), license, formatting and
whitespace checks pass. `make gc` skipped cleanup after detecting native build
activity. Real-provider retention qualification remains deferred; the external
142,577-row HN fixture remains unavailable on this machine.

## Publication quota residency

The [retained observation](publication-residency-v1-observation.json) qualifies
runtime `a6985c62686d5510b2b02d770c1bf2d2144b5e90`, with main
`5a7e547b7adb51e5c7c666680a1695b7755b0adf` merged cleanly. That main update
changes only the documentation approval workflow; the qualified runtime tree
remains unchanged. Runtime ABI **80** fences the shared allocator contract;
reader protocol **97** and persisted formats remain unchanged.

Native publication previously inserted an opaque `WorkingSetAllocator`, which
kept spill readers on the four-root/64-KiB fallback even after public SQL host
admission was fixed. The adapter now delegates to a shared platform refundable
quota owner. Required growth can reclaim idle spill roots through stable
round-robin hooks, without replacing allocation paths or changing the build cap.
Reclamation skips busy owners, acquires known deallocation locks without waiting,
and reuses held locks only for refundable teardown. Pins survive eviction.
An arena hidden below the quota remains conservative. Existing backing-OOM and
configured-limit error classification remain distinct. Coordinator teardown
checks that every quota allocation and residency hook has retired.

The review's controlled native probe changes from **adaptive=false, zero hooks**
to **adaptive=true, one hook** with the real publication wrapper. Permanent
regressions exercise the direct wrapper, nested statement/host/quota chains,
independent managers, shared locks, pinned borrowers, busy caches, arena backings,
and genuine backing failure. A page-allocator-only single-threaded native probe
also passes; the full WASM bundle was not built.

[publication_residency.py](publication_residency.py) measures the actual
coordinator through the public HTTP API and real Parquet/FsStore. Each invocation
uses fresh server storage, with 8,192 rows, an interleaved ordering key and eight
128-byte string columns. Timing starts at table creation and ends at the first
queryable index revision, polled every 25 ms. Server startup, fixture encoding
and complete SQL result validation are excluded. After publication, bounded
ordered SQL pages verify every row and all ten columns. OS file caches remain
warm; this is not an empty-cache or cloud result. Run alternating before/after
invocations without overlapping task-owned builds, E2E or cleanup.

Six alternating pairs of those real coordinator builds have median wall time
**6,150.843 → 5,400.294 ms**, a **12.2% latency reduction (1.139×)**,
with no slower candidate pair. Every run verifies all 8,192 rows and ten columns
in tightly bounded 64-row SQL intervals, and all result, fixture, configuration
and runner digests match. The readiness guard requires the complete target
revision. An initial broader SQL validation request exceeded its work limit;
that incomplete run is excluded, and all timing pairs were restarted with the
bounded validator. Both frozen CLIs are Debug builds. Task-owned builds, E2E and
cleanup finished before timing; other host activity was uncontrolled.

The same final Debug native test executable compares fixed and adaptive spill
policies through the publication quota/lock chain, with **32 MiB post-finish
headroom** and the same **33,594,433-byte** limit. Six alternating Snappy pairs
have median drain thread CPU **954.724 → 112.284 ms (8.50×)**, with no slower
candidate. Spill drain reads fall **70,618,654 → 5,166,118 bytes**, payload reads
**8,192 → 512**, and writes remain **12,043,408 bytes**. Tracked live peak rises
**403,053 → 13,829,101 bytes** within the identical cap. Every sorted key and
all eight ordinal-tagged 128-byte values are checked. Setup/add and Sort.finish
are excluded; first page advancement and full drain are included. The native
fixture uses a 4-KiB sort workspace; the complete coordinator runner uses
production defaults. The drain result is separate from complete publication
latency and does not establish RSS, cloud, HTTP-query or archive-scale gains.

Final qualification passes **351 shared and 92 API checks**, with no failures,
skips or leaks, and **50 real Parquet/Iceberg E2E cases in 393.86 seconds**.
The frozen ABI 80 CLI SHA-256 is
`c3006a4bada5558bc3df766d397eccc8bb642577db3208f8a432586027d60bac`.
The observation records exact commands, native executable identity, source and
raw-log hashes. Apache boundary (2,345 source files), license, Zig formatting,
Python formatting/lint and whitespace checks pass. `make gc` skipped cleanup
after detecting native build activity. Real-provider retention qualification
remains deferred; the external 142,577-row HN fixture remains unavailable.

## Scan-resistant spill admission

The adaptive LRU has a capacity cliff when a cyclic payload working set exceeds
its residency: every reused root can be evicted before the next cycle. The
replacement policy uses a lazily allocated, quota-charged frequency sketch to
reject candidates no more useful than the LRU victim. Admission activates only
after twice-capacity consecutive misses, and twice-capacity hits restore ordinary
LRU. Its counters also age so a new hot set can replace an old one. This gate
preserves rapid adaptation between fitting phases; filling residency alone does
not prove thrashing. Rejected misses use periodic owner accounting;
admitted growth still refreshes actual retained sizes before trimming.

The native comparison keeps the same 64-row interleaved input cohorts, eight
128-byte tagged string columns, Snappy codec, 4-KiB sort workspace and publication
quota/lock chain. It checks every sorted key and each independent row/column tag.
A 32,768-row case checks the fitting-cache benefit, and a 65,536-row case exceeds
the cache's reuse capacity. The 64-KiB/four-root control and candidate use the same
executable and unchanged quotas. Thread CPU covers drain and its value oracles;
input construction and original sort finish are outside this interval. Reads,
writes, peak live allocation and admission rejections are reported separately.
These are local native drain measurements, not RSS or cloud latency claims.

The normal native test also runs an oversized 2,048-row case with a 512-KiB
cache. Its assertions cover read reduction, equal writes, complete ordered output
and unchanged hard admission. Separate shared tests cover cyclic reuse, a new hot
set, metadata allocation failures, reclamation and complete sketch retirement.

For larger repeated measurements, build the mounted lake API tests with
`-Dtest-filter='external lake leased publication adaptive spill cache'`, then run
that executable with `ANTFLY_LAKE_SCAN_CACHE_BENCH=1` and
`--suite-filter 'external lake leased publication adaptive spill cache'`. The
`SCAN_CACHE_SAMPLE` records identify six alternating pairs at each size, following
the ordinary qualification cases. Do not mix these records with the warmups.

Complete real-Parquet publication is measured separately with
`publication_residency.py LABEL IMMUTABLE_BINARY SOURCE_COMMIT ROWS`. Compare
independent fresh server directories and alternating immutable binaries at both
8,192 and 65,536 rows. The timer starts at table creation and stops at the first
complete, queryable target revision. Startup, fixture creation and complete SQL
validation stay outside that interval. Retained OS cache and unrelated host
activity remain uncontrolled; this is not an empty-cache or archive-scale result.

### External permutation experiment

`bounded_payload_permutation.zig` retains the rejected batch-based prototype. It
sorts compact references by physical offset/index, gathers native column batches,
then feeds a nonindirect external sort with the original encoded keys and
ordinals. Both temporary sorts use the original 4-MiB workspace and manager;
original payloads remain owned until the call completes. Additional reads,
writes, CPU and output verification are included in its timed interval, under
the same post-sort quota as ordinary delivery. The helper consumes the original
reference stream and returns a separately owned sort; it is benchmark-only.

Both scalar and typed-batch prototypes greatly reduced reads but consumed more
than twice the native CPU of scan-resistant direct delivery in the measured
65,536-row workload, and added payload writes. Automatic repacking is therefore
not selected in production. A future cost model must qualify CPU, write
amplification and actual storage latency together before choosing this strategy.

Final qualification at runtime source
`85a62db7644bdfde24b6f957d4d9f89f862c5371` is recorded in
[scan-resistant-spill-v1-observation.json](scan-resistant-spill-v1-observation.json).
Three alternating native pairs per size use the same executable:

| Native rows | Small-cache median CPU | Gated-cache median CPU | Payload reads, control → candidate |
| --- | ---: | ---: | ---: |
| 32,768 | 3,871.526 ms | 509.115 ms | 32,768 → 2,048 |
| 65,536 | 7,767.552 ms | 5,920.797 ms | 65,536 → 44,219 |

At 65,536 rows, drain traffic falls from 565,954,122 to 356,600,362 bytes,
with equal writes and the same 33,597,177-byte quota. The candidate's tracked peak
is 17,160,685 bytes. At 32,768 rows, all three candidates perform exactly 2,048
payload reads with zero rejected admissions. Capacity alone was insufficient
proof of thrashing: the ungated prototype delayed fitting phase changes and
performed 5,758 reads. The gate restores the original fitting read count.

The complete publication comparison uses the previous frozen quota-residency
binary and the final gated-policy binary, with three alternating pairs per size:

| Real Parquet rows | Previous median publication | Candidate median publication | Observed candidate change |
| --- | ---: | ---: | ---: |
| 8,192 | 5,807.650 ms | 5,707.188 ms | 1.7% lower |
| 65,536 | 43,842.417 ms | 45,140.032 ms | 3.0% higher |

Every run verifies all rows and all ten columns after publication. One of three
larger candidate pairs is slower, and samples overlap. These measurements do not
establish an end-to-end publication speedup; the native spill result must not be
presented as whole-publication throughput.

The final typed-batch permutation experiment uses 11,222.832 ms CPU versus
4,933.532 ms for direct delivery (2.27×), while reducing reads from 316,567,691 to
35,894,469 bytes and increasing total writes from 7,307,465 to 30,802,081 bytes.
The original sort workspace is 4 MiB and the unchanged post-sort quota is
33,977,075 bytes. These results support retaining it as an experiment instead of
automatic production selection on local spill storage.

The final runtime passes 403 shared checks, 92 API checks and 50 real
Parquet/Iceberg E2E cases (387.33 seconds), with no failures, skips or leaks in the
native checks. The single-threaded publication-owner probe, Apache boundary,
license headers, formatting and whitespace checks pass. The merged `origin/main`
revision `0ff73ed56fb7acfbe4d6a14158f84c4b74c8dadb` changes documentation and
examples; it leaves the qualified Zig runtime unchanged. Real-provider retention,
RSS, cold-storage/archive scale and a full WASM bundle remain unqualified here.


## Private metadata reduction

Metadata mutations now use one byte-bounded external sort and leased equal-key
reduction directly into the final copy-on-write tree. This removes fixed
1,024-record flushes, owning-row copies, intermediate private page trees and a
second binary merge forest. Durable maintenance checkpoint forests remain.
Latest-write and addition-priority ordering, destination reductions, cancellation
and resource admission retain their contracts.

[private-metadata-reduction-v1-observation.json](private-metadata-reduction-v1-observation.json)
records implementation source `fd245554ebd4d3e3bd0a4201d3e4f59b140eeee5`, frozen
binary hashes, exact commands and all twelve fully verified reports from
`publication_residency.py`. The before binary uses the already gated spill cache,
so the comparison isolates metadata staging from the preceding cache work.

| Real Parquet rows | Previous median publication | Candidate median publication | Candidate change | Faster candidate pairs |
| --- | ---: | ---: | ---: | ---: |
| 8,192 | 6,349.022 ms | 5,581.567 ms | 12.1% lower | 3/3 |
| 65,536 | 43,619.621 ms | 44,190.543 ms | 1.3% higher | 1/3 |

Timing includes table creation through the first queryable, complete publication
whose revision matches the target. Startup, fixture verification and complete SQL
verification in disjoint 64-row intervals are excluded. All runs verify every row
and all ten columns; fixture, configuration, runner and complete-result digests
match within each cohort. Both binaries use the same default 4-MiB coordinator
workspace, 1-MiB metadata sort preference and 1-GiB working-set cap. No task-owned
builds, native tests or E2E tests overlap the measured runs.

These are local Parquet/FsStore Debug observations with fresh server storage and
retained OS caches. Other host work was uncontrolled, and an unrelated inference
benchmark was observed during the larger cohort. The larger workload shows no
demonstrated publication improvement. The retained shape simplifies private
staging and improves the smaller measured cohort; it does not establish
optimized-build, archive-scale, cloud or peak-RSS gains.

Qualification covers 403 shared checks, 182 API checks and 50 real
Parquet/Iceberg E2E cases (397.32 seconds). The initial API run passed 181 checks;
its only failure was a test assertion treating cumulative writes as bounded by a
live disk quota. A test-only correction restores the recycling oracle. Explicit
API-root imports also make these metadata tests directly discoverable. Nine
focused metadata checks then pass, covering the corrected assertion and the
previously passing cases, with no skips or leaks. The qualified implementation
binary is unchanged by those test-only commits. Runtime ABI 81, reader protocol
97 and persisted formats remain unchanged.


## Ordered identity maps and typed merge columns

Fresh ID maps reuse the pending stream's existing order and leased records.
With no retained membership, queued changes or prior result root, the by-ID map
feeds the authenticated tree writer directly. A single input cohort has a
constant recipe prefix and preserves the same order. Retained or interleaved
input continues through the general mutation reducer.

Spill merges gather compatible primitive wire lanes from pinned blocks into
native columns, including dictionary values. Encoding gathers one column at a
time under the existing scratch quota. SQL NULL, integer precision and floating
bits retain their contracts; decimal, JSON null, heterogeneous and unsupported
columns use the scalar/frozen codec. Tiles below 16 rows avoid gathering.

[ordered-identity-and-typed-merges-v1-observation.json](ordered-identity-and-typed-merges-v1-observation.json)
qualifies runtime `4fb464d08b12f1aaf1d570ebf7d00698d285ecde` against the
post-main-merge baseline `bbbede9c8de3250f7e0f9eb9499c7c8891496e2f`.
Both include main `f91c9c0e4682a3ff20e39fce9a44d67b658a7b55`.
The exact native build command and build-options snapshot, frozen binary
identities, complete-publication commands, reports and result digests are
recorded separately from the older observations above.

Three alternating native pairs at 65,536 rows and a 4-MiB workspace measure the
complete sort, including add, finish, full leased drain, every value oracle and
sort teardown. Manager construction and executable startup are excluded.

| Compression | Previous median thread CPU | Candidate median thread CPU | Candidate change | Faster candidate pairs |
| --- | ---: | ---: | ---: | ---: |
| None | 5,068.054 ms | 4,717.067 ms | 6.9% lower | 3/3 |
| Snappy | 5,899.222 ms | 5,701.613 ms | 3.3% lower | 3/3 |

Reads, writes, ten merges, tracked live peaks and the 32-MiB quota are identical
within each compression cohort. Uncompressed reads/writes are 134,866,274 /
101,631,258 bytes, with a 973,907-byte tracked peak. Snappy reads/writes are
14,273,560 / 11,712,767 bytes, with a 985,850-byte peak. Every key, ordinal,
primitive value, SQL NULL and tagged string is checked. The 1-MiB workspace case
is essentially flat; 64-KiB workspace results are mixed. An initial ungated
diagnostic overlapped builds/tests and is excluded from performance claims.
These observations support keeping native gathering for productive tiles,
with scalar fallback for boundary flushes.

Three alternating complete real-Parquet publication pairs at each size verify
all rows and all ten columns after the timed interval:

| Parquet rows | Previous median publication | Candidate median publication | Candidate change | Faster candidate pairs |
| --- | ---: | ---: | ---: | ---: |
| 8,192 | 5,384.655 ms | 5,063.972 ms | 6.0% lower | 3/3 |
| 65,536 | 45,459.256 ms | 39,718.982 ms | 12.6% lower | 3/3 |

Timing runs from table creation through the first queryable, complete
publication whose revision matches its target. Startup, fixture creation and
complete SQL verification in disjoint 64-row intervals are excluded. Fixture,
runner, configuration, schema definition and complete-result digests match
within each cohort. Both servers use the default 4-MiB coordinator workspace,
1-MiB metadata sort preference and 1-GiB working-set cap. Task-owned builds,
tests and cleanup finish before measurement; other host work is uncontrolled.
These are fresh-storage local Parquet/FsStore Debug measurements with retained
OS caches. The combined refinements improve these publication cohorts; native
kernel timing measures a separate scope.

Qualification at this runtime passes 404 shared checks, 183 API checks and all
50 real Parquet/Iceberg E2E cases (385.45 seconds). Native checks have no failures,
skips or leaks. Apache boundary, license headers, formatting and whitespace
checks pass. Runtime ABI 81, reader protocol 97 and persisted formats are
unchanged. Real-provider retention, peak RSS, cold storage, archive scale,
optimized builds and a full WASM bundle remain unqualified here.


## Quota-governed residency and streamed capture

Build the native probes through the supported product graph (from `zig/`):

```sh
ZIG_GLOBAL_CACHE_DIR=.zig-cache/global ~/bin/zig build lake-native-bench-build -Doptimize=fast
```

This regenerates options, SDK metadata and dependencies after `make gc`; no
ignored build snapshot or generated source path is required. The individual
`lake-typed-merge-bench-build`, `lake-spill-residency-bench-build` and
`lake-source-capture-bench-build` steps install their corresponding executables
in `zig-out/bin`. Steps without `-build` run the probes. Historical observations
above preserve their original commands; these targets supersede their manual
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
and original byte-span digest, and the complete encoded offset SHA-256. Capture
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
