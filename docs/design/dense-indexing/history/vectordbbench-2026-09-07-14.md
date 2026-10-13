# VectorDBBench Findings: No-Copy Default, Posting-Row Experiments, and Ownership Comparison (2026-09-07 – 2026-09-14)

> Relocated verbatim from `zig/VECTORDBBENCH_FINDINGS.md` (lines 8519–10257 at commit 3ce5aff4ea) on 2026-10-12. Durable decisions, the no-copy default, and the 1M ownership-comparison table were folded into [VECTORDBBENCH_FINDINGS.md](../../../../zig/VECTORDBBENCH_FINDINGS.md) before the move. The closing line ("`primary_lsm` remains the creation default") was true when written; fresh local single-shard standalone tables now default to `vector_store` (see [VECTOR_STORE.md](../../../../zig/VECTOR_STORE.md#scoped-default-promotion)).
>
> Relative links were re-based to this file's location. Paths under `.benchmark-results/` refer to untracked local benchmark output at the repository root; `/private/tmp/...` paths name ephemeral local run directories that may no longer exist.

### No-copy default selection and preserved baselines (2026-09-07)

At the user's request, new posting builds now default to omitting the duplicate
posting-local projection plane. This accepts the documented 50K throughput and
restart tradeoff for single-copy storage; it is **not a new measured all-metric
win**. `ANTFLY_EXPERIMENT_POSTING_LOCAL_PROJECTIONS=1` explicitly retains the
locality-on option. No eager rewrite/deletion is introduced, and existing planes
remain readable. Centralized projection reads, authoritative exact completion,
source ownership, effort, source coverage, and generation leases are unchanged.
Subgroup training can still request projection input without retaining that
plane or requiring it for readiness. Other experimental controls remain off.

The matched progressive-admission matrix is preserved in
`zig/benchmark-baselines/pr593-locality-matched-20260906.json`; the older fast
50K locality reference is in `pr593-locality-fast-50k-20260906.json` beside it.
Both catalogs contain original receipts, qualified individual results/medians,
executed-binary identities, and per-file/archive SHA-256 hashes. Archives under
`.benchmark-assets/baselines/` preserve binaries, logs, raw results, memory
series, and still-available original measurement helpers (296 files / eight
arms and 170 files / four arms). The failed 1M locality repeat remains recorded
and excluded from qualified medians. Existing source data roots are untouched.
The archives intentionally exclude runtime data/model directories and a source
checkout; changed historical helper bytes are explicitly unavailable. These
are local, Git-ignored archives, not remote backups. The JSON catalogs are
intended for version control.

The archive helper refuses replacement, detects changed executed binaries and
evidence races, and publishes a catalog only after a complete archive. Its five
tests pass. Four focused Debug storage tests pass without leaks, including
no-copy default, explicit locality override, float16/float32 policy,
training-without-retention, preserved authoritative callbacks, and projection
validation. Seventeen runner tests pass. The ownership A/B now pins locality
on explicitly, while the locality A/B already sets each arm explicitly, so a
product default change cannot silently change those experiment contracts.
This policy edit has not received a new fresh 50K/1M qualification or full CI.

#### Next no-copy performance priorities

1. **Prove single-copy physical locality before changing the format.** Capture
   bounded batches of actual projection locations and evaluate source-trained,
   cross-shard chunk packing against the current hash-sharded placement. Prior
   within-page clustering reduced 1M physical reads only 0.67%; a larger generic
   page cache reduced reads about 3.6% while increasing bytes 16.4%. Require a
   material reduction in reads without inflated bytes before a storage rewrite.
   Then keep one projection per authoritative vector revision, a shared ID-to-
   location directory, and generation-bound posting references. Shared artifacts
   must not be owned by one index's topology. Foreground mutation remains WAL-
   backed; relocation happens in bounded immutable chunks with lease/reference-
   safe reclamation, not synchronous whole-corpus reordering.
2. **Separate warm direct access from cold I/O scheduling.** At 50K, syscall,
   copy, and helper overhead matter disproportionately. Evaluate generation-
   leased views over the existing single-copy projection bytes under an explicit
   residency budget, with bounded positional reads for cold/unadmitted pages.
   This is not unbounded mmap warmup, another full decoded heap plane, or merely
   inlining all reads (already tested without a win). Measure C1 and C30, cold
   and warm, plus physical footprint and cancellation/fairness. Do not assume a
   50K residency win generalizes to the 1M projection working set.
3. **Make routing savings survive total-cost accounting.** The global subgroup
   plan preserves the recall budget but spends 0.228 ms recovering only about
   0.244 ms of leaf scoring in the latest C30 diagnostic. Test compact generation-
   owned representatives, not per-query conversion. Keep this independent from
   the projection layout experiment; it has not reduced exact/projection reads.
4. **Bound mixed-workload publication cost.** Reusable immutable chunks and
   incremental shared-vector consolidation target catch-up, peak physical
   memory, and rewrite amplification. Preserve atomic coverage/revision binding
   and separate foreground/background admission. Compare equal offered write
   rates as well as saturation: faster writes alone can leave more replay debt.

The first release/performance gates remain the preserved same-binary baselines:
recover locality-on-class 50K throughput/tails while retaining no-copy disk, and
improve on the qualified no-copy 1M curve without losing recall, RSS/footprint,
mixed-write responsiveness, restart behavior, or durability. No speedup from
these proposed changes is claimed or implemented by this default-policy edit.

#### Four no-copy experiments: implementation and qualification ledger

The preserved locality-on/no-copy baselines above remain unchanged. The next
experiments are independent and default-off; none is promoted by implementation
alone. The work remains in `spfresh-segment-wal`.

| Experiment | Mechanism and status | Synergy to measure |
| --- | --- | --- |
| Single-copy locality | Actual bounded projection-request trace and source-only balanced cross-shard packing screen. The replay counts read spans and bytes, with an optional bounded LRU model. Not a new serving format or a measured QPS win. | Packing × bounded warm access; never train using query/neighbor labels. |
| Warm borrowed pages | `ANTFLY_EXPERIMENT_PROJECTION_BORROW` borrows checksum-validated views from the existing bounded clean-page cache. Query-owned leases prevent overwrite/reclamation and avoid the session's additional projection copy. Misses and allocation denial retain positional-read fallback. | Borrowed pages alone versus compact routing + borrowed pages. This is not unrestricted mmap residency. |
| Compact global routing | `ANTFLY_EXPERIMENT_COMPACT_SUBGROUP_ROUTING` adds lazily built, generation-owned, budgeted int8 representative hints to the existing global subgroup plan. Quantize each query once; exact scores and boundary completion are unchanged. No AFSG format change. | Compare exhaustive control, float32 global plan, compact global plan, and compact + borrowed pages. |
| Incremental source publication | A new no-copy runner arm explicitly composes the **existing** append-only source segments, selective GC, and coalesced disposable-directory snapshots. This does not implement reusable full-HBC checkpoint chunks. | Compare saturation and equal offered write rates; then combine qualified query changes with source publication. |

The warm path still allocates the established bounded batch destinations on
miss-capable calls; this first experiment removes copies on admitted hits, not
all query scratch. The cache remains limited to 32 MiB and charges retained pages
to the resource manager. Borrow-scope descriptors are independently charged to
the query working set. No authoritative float16 plane is duplicated on disk.

New counters `hbc_subgroup_compact_groups_scored` and
`hbc_rerank_vector_projection_borrows` distinguish active treatments from inert
configuration. The runners reject missing/zero treatment evidence. Publication
arms are rejected by the query-only runner because a restart-only clone cannot
measure their mutation effect.

The mixed-workload profiler now optionally accepts a node-total offered row
rate (zero preserves saturation mode). It reports scheduling delay and latency
from the intended send time separately from HTTP request latency. This exposes
missed offered load rather than hiding it as fewer completed writes. The
archived harness itself is unchanged; its pinned helper receipt captures this
profiler revision.

Initial Debug checks pass: compact/global planner lifecycle with flat/tree
routing (four tests), page-cache identity/reclamation/borrow ownership and
positional-read fallback (three tests), exact residual completion and scratch
accounting/default locality policy (four tests), and the standalone compact
hint allocation/overflow test. Python checks pass: 25 locality/layout/trace tests
and nine query-runner/summary tests. These are targeted checks, not full CI or
new 50K/1M performance qualification. Performance measurements are pending.

##### First 50K factorial screen: do not promote

`pr593-four-query-50k-20260907` completed all twelve independent query arms
(six modes, reversed order) using binary SHA-256
`601113c2dbe5f91b760d6b727f10b3711442365d29a1ae99409afef4ea367639`.
Every arm used the same saved 50K generation, aggregate admission, four-subgroup
layout, 1,000 fixed warm queries, and C1/C30 public HTTP diagnostics. All fixed-
query recall gates passed. These are diagnostic QPS, not fresh qualification;
compilation overlapped some arms and other work was active on the host.

| Mode | C1 p95 ms | C30 diagnostic QPS | C30 p95 ms | Fixed recall | Peak RSS MB | Peak physical footprint MB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Control | 5.72 | 1,431.0 | 49.85 | 98.405% | 786.3 | 332.4 |
| Queued pages | 6.35 | 1,133.0 | 70.33 | 98.405% | 895.7 | 393.5 |
| Borrowed pages | 9.44 | 1,128.8 | 80.73 | 98.405% | 829.8 | 391.1 |
| Global float32 hints | 11.28 | 1,039.0 | 92.50 | 97.978% | 730.8 | 358.7 |
| Global compact hints, mutex cache | 11.16 | 915.0 | 104.56 | 97.973% | 615.4 | 389.1 |
| Compact hints + borrowed pages | 26.09 | 790.2 | 114.86 | 97.973% | 653.6 | 445.7 |

Values are two-arm medians, MB decimal. RSS reduction alone is not a physical-
memory win: compact+borrowed has the lowest-but-one RSS and the highest physical
footprint. Borrowing avoided 55.6 projection copies per warm query, but C30 reads
fell only 436.7 to 375.5 while read bytes grew 1.267 to 1.534 MB/query. The extra
page traffic and limited reuse do not support enabling this cache policy.

The compact cache introduced a concrete query-serialization issue. Compact
routing cost improved at C1 (0.232 to 0.150 ms), but rose at C30 (0.250 to
1.154 ms). Both pairs reproduce this phase-specific regression despite host
variance. The first implementation used one mutex around the generation hint
map. The follow-up uses an append-only array of atomic pointer slots, sized from
the generation's directory entries, with bounded probing and float32 fallback
on collision/budget exhaustion. Entries never move or retire while generation
leases exist; cold builders stage outside publication and losers discard their
allocations. Debug concurrent-first-reader, allocation-failure, and flat/tree
lifecycle tests pass. The new implementation still requires a new binary and
live A/B; the table above deliberately preserves the failed first design.

##### Actual 50K projection-location replay

`pr593-projection-trace-50k-20260907` captured 98 complete bounded batches from
64 real public queries using a Debug diagnostic binary (SHA-256
`cb96fea2dd904f07238bd77450c48e79277bcf4e89d2074e5d9383adbe1ef68a`).
The offline exporter authenticates manifest/header/index/payload checksums,
requires every traced location to resolve, and trains only on the 50,000 source
vectors. No source database or authoritative artifact was rewritten. Debug
timings are not performance evidence.

The unchanged payload plane is 153.6 MB. Across these batches the scalar model
reads 17,636 projections / 54.178 MB. Adjacent-only coalescing in the current
layout saves 0.4%; source-trained 64 KiB chunk packing plus adjacent-only
coalescing reads 16,124 spans / the same 54.178 MB: **8.6% fewer reads**, below the
20% work-reduction gate. Packing with full-page LRU admission models 11,683 reads
but 191.414 MB fetched, 3.53 times the requested bytes. A same-policy comparison
does improve over current-layout LRU (15,915 reads / 260.751 MB), but that is not
an improvement over the scalar no-copy baseline in bytes. This first packing
trainer does not yet justify a durable format change. Directory/residual work,
cache locking, real disk latency, and residency are excluded from this model.

The first 1M trace attempt used an older preserved generation and failed startup
with `UnsupportedVectorBlockManifestVersion`; its receipt remains unqualified.
The archive was not rewritten or migrated. A compatible saved-generation trace
is being evaluated separately; no 1M locality result is inferred from 50K.

The compatible 1M clone also exceeded the Debug diagnostic's 180-second startup
deadline, without producing a request trace. The completed ReleaseFast binary
opened that same saved generation successfully, so `pr593-projection-trace-release-1m-20260907`
provides the actual 1M work screen. Traced latency remains excluded. It exports
1,000,000 authenticated source rows and the bounded prefix of 128 complete
request batches; it does not claim to trace every request in all 64 queries.

The 1.536 GB single-copy plane produces 25,792 scalar reads / 39.617 MB requested
in this prefix. Source-only packed contiguous spans reduce reads to 24,815
(**3.8%**) at unchanged bytes. Packed whole-page LRU models 20,578 reads but
337.150 MB fetched (**8.51×** requested bytes). Thus neither corpus meets the
20% reduction/no-extra-bytes gate for this source-geometry trainer. These models
preserve existing bounded request partitions; they do not model reordering across
separate query stages or a new whole-query I/O scheduler. Better correlation
between the actual candidate shell and physical chunks remains unproven.

The mixed workload helper's input loader now streams the same deterministic
2,000-row training prefix rather than materializing the entire 1M training file.
Two tests verify unchanged IDs/vectors/ground truth and reject invalid offered
rates before dataset I/O. This reduces load-generator pressure, not Antfly's
attributable demand; future same-binary arms pin this helper revision and must
not attribute a historical timing difference solely to server code.

##### Atomic compact-hint follow-up, 50K

`pr593-four-query-atomic-50k-20260907` completed eight reversed-order public-query
arms with ReleaseFast binary SHA-256
`87ec4c733432e53295a8c901bb8e359c98d27ef9ee656ddba0ce15dc5fd98c11`.
No compilation from this investigation overlapped these measured arms; the host
still had other work. All 1,000-query recall gates passed.

| Mode | C1 p95 ms | C30 diagnostic QPS | C30 p95 ms | Fixed recall | Peak RSS MB | Peak physical footprint MB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Exhaustive control | 7.39 | 1,448.2 | 50.93 | 98.405% | 1,024.3 | 320.4 |
| Global float32 hints | 5.51 | 1,609.5 | 38.13 | 97.978% | 1,066.6 | 310.3 |
| Atomic compact hints | 5.15 | 1,774.3 | 33.37 | 97.973% | 1,053.6 | 318.7 |
| Atomic compact + borrowed pages | 5.43 | 1,725.5 | 32.22 | 97.973% | 879.1 | 350.0 |

Atomic compact routing is 0.123 ms at C30 in both repeats, versus 0.205 ms for
float32 hints and 1.154 ms for the failed mutex implementation. The compact
arm's two QPS/p95 samples are 1,775.1/33.40 and 1,773.6/33.33. Exhaustive control
varies materially (1,193.7/66.07 and 1,702.8/35.79), so the median improvement is
not a precise causal effect size. Compact still improves QPS and p95 in each
paired comparison; the smaller second-pair gain versus exhaustive is 4.2% QPS
and 6.9% p95, with 0.432 percentage points less fixed-query recall.

This qualifies compact hints for larger experiments, **not default promotion**:
RSS is not demonstrably reduced, and fresh/mixed 1M behavior is outstanding.
Borrowing adds a small tail improvement but reduces throughput and raises
physical footprint versus compact alone. It remains default-off; lower RSS
does not establish the desired memory tradeoff.

Fresh source-publication A/B qualification is now running separately in
`pr593-incremental-publication-20260907`, using this same binary, subgroup training
and aggregate admission in both arms, batch 100, C1/10/20/30, and equal offered
mixed writes of 2,000 rows/s. Four 50K arms must qualify before four 1M arms.
The first control qualified at 17.20 s load/readiness, with 1,999.3 actual mixed
rows/s, 149.8 ms write p95, and 1.16 s catch-up; these are individual preliminary
samples, not a publication-treatment result. Source-only publication does not
change the ANN routing flag during these arms. Query/routing synergies require
separate comparisons on the resulting qualified generations.

Review before the larger compact-routing test found that the atomic cache had
been connected to `BudgetedAllocator.allocator()`, although concurrent cold
readers can allocate staged entries. It now uses `threadSafeAllocator()`;
only cold allocation/free takes that allocator lock, not warm slot lookups.
A Debug test concurrently stages 16 different representatives and checks that
all views publish and teardown returns live allocation accounting to zero.
It passes with no leaks. The atomic 50K numbers above are **diagnostic, not
qualified production evidence** because that binary predates this correction.
The publication matrix disables compact routing throughout and is unaffected
by the cache race. The corrected ReleaseFast build overlaps part of the ongoing
publication matrix; those overlapping timings must not be treated as clean
performance qualification or used to attribute a regression to publication.
The corrected routing comparison will run after compilation completes.

##### Fresh publication and changed-vector recovery observations

All four fresh 50K arms passed native readiness, mixed catch-up and restart.
The treatment enables append-only source segments, selective source GC and
coalesced source-directory publication together. It does **not** implement a
new reusable-chunk HBC checkpoint format. Routing remains exhaustive in these
arms; both arms train four subgroups and retain no duplicate float16 plane.

| Pair / mode | Ready s | C30 qualification QPS | C30 p95 ms | Recall | Allocated disk MB | Mixed actual rows/s | Write p95 ms | Catch-up s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 control | 17.20 | 2,033.5 | 26.96 | 98.420% | 396.0 | 1,999.3 | 149.8 | 1.16 |
| 1 candidate | 14.64 | 1,743.8 | 32.80 | 98.360% | 400.1 | 1,990.0 | 231.5 | 1.26 |
| 2 candidate | 14.77 | 1,541.9 | 37.28 | 98.300% | 402.0 | 1,960.7 | 298.4 | 1.57 |
| 2 control | 19.86 | 824.5 | 86.25 | 98.400% | 395.1 | 1,355.0 | 505.6 | 1.79 |

The two pairs disagree on query/write latency direction. Equal offered writes
are 2,000 rows/s, but the second control cannot sustain that rate; averaging
these samples is not a clean causal estimate. The first 1M pair also passes:
control/candidate ready time is 321.90/291.89 s, allocated disk 3.966/4.036 GB,
and mixed catch-up 77.95/79.05 s. It establishes neither a disk nor catch-up win.
The corrected compact build overlaps the candidate's later phases, excluding
their timings from clean performance attribution. Raw receipts retain all
observations; no default changes follow from them.

`scripts/check_source_publication_churn.py` supplements these idempotent mixed
writes with two rounds of version-changing updates to 2,000 vectors, deletion
of half, restoration, and graceful restart on independent first-pair 50K
clones. Before writes it verifies the exact index incarnation, and after
restoration/restart it requires native readiness, 50,000 documents and a
1,000-query recall gate. It pins the original binary/environment and all
measurement helpers and leaves original qualification data untouched.

Both arms in `pr593-publication-churn-20260907` pass. Control recall is
98.403% before / 98.405% restored-restarted; candidate is 98.365% / 98.358%.
This is changed-vector/recovery evidence, not crash-fault injection, an idle-GC
reclamation guarantee, or performance qualification. Two safety-gate unit tests
pass, rejecting wrong index identities/counts and missing/nonfinite/degraded
recall. The supplemental summary also distinguishes scheduled write delay from
HTTP write latency and labels memory spanning mixed work separately from
read-only memory.

The second 1M candidate recorded **798.9781 s** load/readiness (425.669 s
insertion + 373.309 s catch-up), versus 291.8861 s in its first run. Its load
overlapped compilation/other host work; the cause of this large outlier is not
established and is **not attributed to contention**. After retaining that
result, the repeat was deliberately interrupted by stopping its disposable
server. It is unqualified; subsequent request failures caused by that stop are
not spontaneous product failures. The second 1M control is not run. The first
complete 1M pair and all four 50K arms remain untouched. More overlapped repeats
are deferred in favor of the corrected compact-routing synergy diagnostic.

After the allocator correction, all three targeted Debug adapter tests pass:
concurrent compact-cache accounting and native compact-plan lifecycle with
flat/tree routing (zero failures/leaks). An initial invocation used the
nonexistent filter `compact global` and was rejected before running; the
corrected `compact subgroup` filter explicitly ran all three tests. The
physical-work/runner Python tests pass (26), recovery-summary/runner tests pass
(9), changed-vector gates pass (2), and bounded mixed-input/rate tests pass (2).

##### Corrected 1M routing × source-publication synergy

`pr593-four-synergy-safe-1m-20260907` uses corrected ReleaseFast SHA-256
`9388f180937ba6bb88b0a68110be0df64194de95e4af4d41c9140291f275f780`.
All eight reversed-order arms passed execution, native readiness after mixed
work, and the 1,000-query recall gate. Each starts from an independent clone of
the first qualified incremental-publication 1M candidate. Aggregate admission,
four-subgroup layout and incremental source publication are common to all arms.
There is no compilation or other benchmark from this investigation during this
matrix, but this is still a shared-host diagnostic, not leaderboard QPS.

After fixed warmup and C1/C30 measurement, each arm offers 1,000 rows/s for 15 s.
These moderate-load mixed results are separate from the fresh matrix's
2,000 rows/s / 30 s results. Memory below spans restart, warmup, read-only and
mixed phases; it must not be described as read-only RSS.

| Mode (two-arm median) | C30 diagnostic QPS | C30 p95 ms | Fixed recall | Peak RSS GB | Peak physical footprint GB | Mixed write p95 ms | Mixed query p95 ms | Catch-up s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Exhaustive control | 586.0 | 106.50 | 99.027% | 5.566 | 1.874 | 458.4 | 103.1 | 34.13 |
| Global float32 hints | 675.8 | 72.77 | 98.862% | 6.003 | 1.785 | 101.2 | 56.0 | 19.98 |
| Compact hints | 609.8 | 104.05 | 98.859% | 5.459 | 1.919 | 470.7 | 105.4 | 37.96 |
| Compact + borrowed pages | 523.0 | 105.30 | 98.859% | 5.019 | 2.122 | 510.3 | 97.8 | 62.07 |

Absolute results vary materially: control QPS/p95 is 712.6/70.07 then
459.4/142.93; compact is 737.6/67.67 then 482.1/140.43. Compact's paired gains
are only 3.5–4.9% QPS and 1.7–3.4% p95, not recovery of a historical absolute
best. Float32 hints are 707.6/68.40 and 644.1/77.14, so compact does not
consistently beat the existing float32 global plan. Do not promote either
hint policy from the median alone, or explain the large timing swings without
stage/host attribution.

Candidate work is stable across repetitions: 239,270 approximate scores/query
for exhaustive versus 179,468 for either global plan. All exact completions
remain approximately 145/query. Compact scores all 8,192 planned group hints
per query; it is not silently falling back to float32 hints. Its routing time
is 1.094/1.579 ms at C30 versus float32's 1.711/1.805 ms, establishing the
intended local CPU-work reduction, not an overall latency/memory win.

Borrowing is not a useful 1M synergy: the first arm borrows only 21.96
projections/query, still performs 592.11 physical reads/query and reads 1.051 MB
per query. QPS/p95 is 486.1/117.61 then 559.8/92.99. Physical footprint and
catch-up worsen relative to compact alone despite lower RSS. Keep it off;
removing a small number of copies has not solved scattered candidate I/O.

##### Corrected 50K synergy and final experiment disposition

`pr593-four-synergy-safe-50k-20260907` completes six reversed-order arms with
the same corrected binary and common options as the 1M synergy matrix. It
clones the first fresh incremental-publication 50K candidate, not the older
generation used for the earlier atomic-cache screen. Every arm passes native
readiness and the 1,000-query recall gate; no compilation from this
investigation overlaps this matrix.

| Mode (two-arm median) | C30 diagnostic QPS | C30 p95 ms | Fixed recall | Peak RSS GB | Peak physical footprint MB | Mixed write p95 ms | Catch-up s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Exhaustive control | 1,210.0 | 75.03 | 98.365% | 1.336 | 587.5 | 450.0 | 3.77 |
| Compact hints | 1,364.1 | 53.75 | 97.861% | 1.778 | 632.6 | 149.9 | 1.25 |
| Compact + borrowed pages | 1,479.9 | 40.81 | 97.861% | 1.537 | 629.7 | 149.7 | 1.10 |

Again, medians conceal substantial variation. Control QPS/p95 is 921.1/108.11
then 1,499.0/41.94; compact is 1,151.7/67.32 then 1,576.5/40.19. Compact beats
each paired control, but the smaller repeat gains are only 5.2% QPS / 4.2% p95.
Borrowing is 1,562.8/37.15 then 1,396.9/44.47: it improves the first compact arm
and regresses the second. Recall loss is 0.504 percentage points in both
repeats. Median physical footprint rises about 7–8% versus control; RSS rises
as well. These are not recovered historical-best results.

The first control sustains only 868.8 of the offered 1,000 rows/s and has
751.4 ms write p95; the second sustains 1,002.0 rows/s with 148.5 ms p95.
All treatment arms sustain approximately 1,000 rows/s with 149–151 ms p95.
Thus the median mixed-write improvement does not establish a stable benefit;
the second pair is essentially equal. Raw scheduling delay, offered/actual
rate, query latency, catch-up and phase-inclusive memory remain in receipts.

Disposition after all four experimental paths and their selected combinations:

- **Physical locality:** retain the source-only trainer/actual-request replay
  as a negative screen. The 8.6%/3.8% read reductions at 50K/1M do not pass the
  no-extra-bytes/material-reduction gate; no serving-format rewrite is enabled.
- **Warm borrowing:** retain the guarded implementation and lifetime/OOM
  tests, default-off. The 50K interaction is inconsistent and the 1M
  throughput/footprint/catch-up tradeoff is unfavorable.
- **Compact hints:** retain the corrected generation-owned atomic cache and
  SIMD scorer, default-off. Local routing cost improves and paired control
  comparisons show modest gains, but this does not consistently beat the
  existing float32 global plan or satisfy every memory/mixed-workload goal.
- **Publication:** the source-segment/selective-consolidation experiment passes
  changed-vector/delete/restore/restart checks, but does not establish a
  repeatable disk, catch-up or load-time win. Full reusable HBC checkpoint
  chunks remain unimplemented by this experiment; do not describe source-only
  option combinations as that complete design.

The locality-on and no-copy archived baselines remain unchanged, and no-copy
remains the default for new posting builds. No additional experimental flag is
promoted. All measurement servers and compilers owned by these experiments
have completed or were explicitly stopped; the interrupted publication repeat
is retained, not silently excluded or presented as qualified.

Final verification after measurement: eight focused Debug storage tests pass
with zero leaks (compact cache/accounting and flat/tree lifecycle, authoritative
rerank reuse including borrowed pages, cache identity and lease reclamation).
All 39 selected Python tests pass, and `git diff --check` is clean. Full CI is
not claimed. No commit/push or baseline replacement was performed in this turn.

#### Four follow-ups: bounded suffix compaction and outlier attribution

The next implementation adds default-off
`ANTFLY_EXPERIMENT_COMPACT_POSTING_DELTAS`. Once a non-empty base exists,
chain-limit/dead-row maintenance can fold the delta suffix while retaining the
base file and mapping. Patches are resolved one at a time and rebased against
that retained base, not against a retired delta. Still-current encoded scan
rows are preserved, including acceleration-only rows. Tombstones remain
explicit. CURRENT still binds the complete ordered segment set and sealed WAL
coverage in one durable publication. Stable-tip readiness still performs its
existing full layout consolidation; this is **not** arbitrary leaf-chunk
replacement or completion of the full reusable-chunk design.

Reclamation compares complete immutable descriptors with the new manifest.
Obsolete mappings retain an owned storage lease and delete their files only
after their last shared lease releases. Review caught and fixed an initially
borrowed storage pointer: delayed deletion must survive provider shutdown.
Providers without owned leases leave recoverable startup cleanup debt instead
of scheduling an unsafe callback. Ten focused Debug tests pass, including
patch rebasing/tombstones, scan-row preservation, stale publication rejection,
sealed-tail recovery and deletion after native-provider shutdown. A separate
thread-CPU clock test passes on this ARM64 Mac. These are focused checks, not
full CI or performance qualification.

The 798.978-second interrupted 1M run is now summarized in
`pr593-incremental-publication-20260907/outlier-publication-attribution.json`.
Its two full checkpoint staging events total **7.075 seconds**, with the final
one taking **2.684 seconds**. The log contains **1,111 boundary-mismatch
observations**. Those observations are not 1,111 rebuilds and have no duration
certificate. The final checkpoint encoding/fsync cannot explain the
373.309-second optimize interval by itself; neither CPU contention nor a
particular readiness predicate is established as its cause.

New checkpoint diagnostics distinguish worker queue delay, initial maintenance
admission, build wall/thread CPU, completed-worker waiting, preparation,
reader construction, rebase, durable publication and serving swap. The parser
keeps nested timers separate and missing CPU samples explicit. Readiness logs
now expose finalization, posting-base presence, vector-base cardinality,
sequence readiness and count readiness instead of reporting equal sequences
as an unexplained boundary mismatch. The non-suspending process sampler adds
CPU, page-in and instruction/cycle counters; process CPU is not per-query CPU.

The fresh-load runner now accepts `--control-binary` to compare a preserved
executable with the current candidate using the same public batch-100 harness,
reversed ordering, 50K-before-1M gates, read-only/mixed work and restart. Both
binaries and measurement dependencies are pinned. `--sample-process` adds
Darwin attribution without suspending the server. No archived baseline is
rewritten, and no additional product flag is promoted by these changes.

##### Fresh checkpoint-default versus preserved no-copy baseline: 50K gate

The accumulated experiment stack was checkpointed normally on this worktree as
`d41ecd5a8`; no new worktree or checkpoint branch was created. The fresh matrix
`pr593-checkpoint-default-vs-baseline-20260907` compares current ReleaseFast
SHA-256 `bf7598858b6d8e94b0405642eac19fbfe2c1325d4c5d9a661130afdbf7ccc594`
against preserved no-copy SHA-256
`205803589eb952d8a097f03173aba1a3848601dc543c402123fa2a1ba5c75f09`.
Both use no-copy, batch 100, four load workers, cosine/top-100, no full text,
unchanged query effort, and **no additional experimental flags**. In particular,
this is not a suffix-compaction treatment. Query durations are 30 seconds at
C1/10/20/30, followed by a 30-second mixed workload offering 1,000 rows/s,
restart checks and a fixed 1,000-query profile. This moderate offered write rate
is not directly comparable to historical saturated mixed-write throughput.

| Pair / arm | Ready s | C30 QPS | C30 p95 ms | Live recall | Mixed write p95 ms | Mixed query p95 ms | Catch-up s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 baseline | 17.365 | 1,592.2 | 50.419 | 98.49% | 164.3 | 58.8 | 1.153 |
| 1 current | 14.584 | 1,639.8 | 49.371 | 98.25% | 338.5 | 74.5 | 4.066 |
| 2 current | 25.713 | 1,628.9 | 52.530 | 98.35% | 311.3 | 77.3 | 2.516 |
| 2 baseline | 27.937 | 1,750.1 | 49.745 | 98.47% | 348.5 | 77.6 | 2.453 |

Both reversed pairs pass execution, live/reopened/fixed-profile recall parity
(maximum observed loss 0.24 percentage points) and native readiness. Current
loads faster in both pairs but does not consistently improve read-only QPS or
p95. The first mixed result regresses; the reversed pair is close. Do not
promote a new flag or claim a universal speedup from these measurements.

| Pair / arm | Load + read-only sampled RSS GB | Mixed sampled RSS GB | Live physical-footprint ledger peak GB | Allocated disk after restart GB |
| --- | ---: | ---: | ---: | ---: |
| 1 baseline | 1.587 | 1.916 | 0.479 | 0.419 |
| 1 current | 1.521 | 1.836 | 0.675 | 0.399 |
| 2 current | 1.511 | 1.997 | 0.555 | 0.399 |
| 2 baseline | 1.729 | 2.023 | 0.521 | 0.397 |

Lower RSS does not establish lower physical footprint: current's live ledger
peak is higher in both pairs. Raw arms and `50k-comparison.json` preserve the
separate measurements, rather than replacing either archived baseline. No
compiler or other benchmark owned by this investigation overlaps this matrix;
another actor's vector-progress experiment is present on the shared host.
That observation alone does not attribute the timing differences to contention.

The current 50K initial full checkpoint builds take 100.8/84.7 ms; completed
worker waits are 76.0/339.8 ms. These measured stages do not account for the
4.032/8.042-second optimize intervals. During mixed work, delta builds take
277.7/290.6 ms and wait another 802.9/1,418.5 ms before handoff. The publication
owner currently checks completion during source/maintenance passes; the worker
does not directly wake that owner. This is a concrete scheduling interval to
investigate, not evidence that the final file encoder explains the old 799s run.
The pinned VectorDBBench client polls readiness every two seconds, so its
optimize duration is also a sampled upper bound, not an exact internal
completion timestamp. Keep that polling contract identical across arms; do not
attribute the whole interval to checkpoint work or silently shorten the poll
to manufacture a load-time improvement.

Detailed readiness logs also show matching vector coverage/count with a missing
initial HBC base. Code tracing confirms `ensureVectorBlockBaseAtAppliedSequence`
returns early for the already-certified vector generation before native merge
or primary rescan. The caller then schedules HBC acceleration. Therefore the
generic `boundary_mismatch` observation is not proof of repeated vector builds;
simply suppressing maintenance on this condition could starve HBC publication.

At this checkpoint the four 50K arms are complete and the 1M matrix is running;
no 1M result is claimed yet. Measurement review corrected Darwin rusage CPU
units using the Mach timebase (125/3 on this ARM64 host, rather than treating
ticks as nanoseconds). Original ticks and conversion factors are retained.
`summarize_process_phases.py` excludes restart/phase/clock crossings and combines
user/system CPU only for identical observed intervals. Twenty-five focused
Python tests pass with the benchmark's Python environment; the system Python
3.9 invocation is unsupported by these existing Python 3.11+ harness helpers.

##### Completion-driven checkpoint handoff and suffix lifecycle coverage

The first 1M pair in `pr593-checkpoint-default-vs-baseline-20260907` has
completed execution and paired recall qualification. The reversed 1M pair
remains in progress. These binaries predate the completion-lane change below;
none of these results measures that change or enables suffix compaction.

| First 1M pair | Ready s | C30 QPS | C30 p95 ms | Live recall | Mixed write p95 ms | Mixed query p95 ms | Mixed catch-up s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Preserved no-copy | 387.473 | 790.4 | 83.137 | 99.08% | 98.937 | 52.013 | 39.817 |
| Current checkpoint default | 354.622 | 661.8 | 94.241 | 99.04% | 461.439 | 100.741 | 105.457 |

Current sampled load/read-only RSS is 5.751 GB versus 5.853 GB; mixed RSS is
4.442 GB versus 5.899 GB. Allocated disk after restart is 3.827 GB versus
3.813 GB. These modest load/RSS gains do not excuse the query and mixed-work
regressions. This is not an overall performance win.

The current 1M trace records completed workers waiting 3.015, 3.198, 5.871,
7.597 and 9.720 seconds for publication. Delta installation itself sometimes
takes about a second. A wakeup fix targets the measured *waiting* component;
it does not eliminate installation cost or explain all query latency.

Implemented a ResourceManager-owned, allocation-free coalesced completion
signal and a separate joined `std.Io` publication task. The task only attempts
already-completed HBC workers, uses nonblocking cache/catalog/index admission,
respects source-capture ownership and HA owner eligibility, and starts no
new compaction or tree repair. Busy owners retain pending work for a bounded
retry; idle consumers wait on an event. Optional LSM/vector maintenance has
its own task, so it cannot occupy this consumer. Shutdown joins the consumer
and detaches the event's I/O runtime before releasing it; late producers
retain only a notification epoch. The touched LSM lifecycle also now uses
`std.Io`, not a directly spawned `std.Thread`.

The catalog lease is also a backup boundary: native snapshot capture closes
and drains it before selecting/hardlink-pinning generated files, and releases
it only after those files are pinned. No extra primary apply lock is needed
for completion-only publication. Per-index capture ownership remains the
mutation boundary.

Focused Debug validation passes: six storage checks (including completion
ownership, signal lifecycle, concurrent-WAL rebasing and suffix retirement)
and three data-runtime checks covering pressure, cadence and HA fencing.
The new native suffix integration test pins an old reader, compacts two
deltas while preserving the base mapping, appends a newer WAL transaction
during staging, then checks tombstones, old/live visibility and restart.
These checks establish lifecycle coverage, not a suffix performance win.

ReleaseFast completion-lane binary SHA-256:
`b69423318276dc4c648083a765b70c6004b931d5325f2fb30c99ea52e6375edd`.
It is reserved for a separate matched comparison, not substituted into the
running baseline matrix. No new experiment is promoted. True reusable
physical checkpoint chunks remain unimplemented: folding a delta suffix
retains the base but still rewrites the selected suffix's serving rows.

The reversed 1M pair subsequently completed and passed the same qualification
and recall gates. Full results are in `full-comparison.json` under that matrix:

| Reversed 1M pair | Ready s | C30 QPS | C30 p95 ms | Live recall | Mixed write p95 ms | Mixed query p95 ms | Mixed catch-up s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Current checkpoint default | 290.453 | 873.8 | 65.104 | 99.07% | 90.157 | 45.287 | 31.528 |
| Preserved no-copy | 281.802 | 894.1 | 66.010 | 99.00% | 90.779 | 39.306 | 31.235 |

Current/baseline sampled load/read-only RSS is 6.146/6.118 GB, mixed RSS is
6.447/6.335 GB, live physical-footprint ledger peak is 0.524/0.537 GB and
allocated disk after restart is 3.799/3.804 GB. The large first-pair regression
does not repeat at the same magnitude, but current still does not win across
load, read-only, mixed, memory and disk. Approximate candidate work remains
about 239K–240K vectors/query in all four 1M fixed profiles. A scheduling fix
cannot be credited with reducing that routing work.

The dedicated completion-lane A/B is running independently under
`pr593-completion-lane-ab-20260908-retry`, against the pre-lane current binary.
The initial sandboxed attempt failed to bind the health server and is retained
as failed evidence, not a performance arm. The retry uses the same public API
contract outside the sandbox. No performance result for the lane is claimed
until the paired run finishes.

Follow-up review found that `decodeOwnedEntry` reused a mmap verification bit
to authenticate bytes from a separate cold-read buffer. Private checkpoint
reads now always authenticate their own candidate bytes with `antfly_hash.Crc32`
and validate projection rows; successful private reads no longer mark the
different mmap buffer as verified. A warm-mapping/corrupted-private-read test
guards against silently republishing corruption. This changes maintenance,
not the warm query fast path. The fix is committed as `16a18eb38`; the running
lane A/B deliberately retains the earlier binary to isolate that treatment.

Seven focused Debug storage checks pass, now including publication deferral
under the catalog barrier used by native backup. The standalone quantized
directory suite also passes, including the new private-read checks. The
benchmark launcher was paused only between timed arms for these compiles,
then resumed; no owned compiler overlapped measured server work.

`scripts/summarize_posting_reuse.py` adds a post-run evidence gate for the
separate suffix experiment, with six passing Python tests. It requires an
actual `compact_deltas` publication, not merely an enabled flag or a completed
worker, and retains repeated retained-byte samples rather than counting them
as cumulative disk savings. On the first pre-lane 1M candidate it finds 11
delta and two full handoffs, writing 1.631 GB and 0.422 GB respectively, and
no suffix publication (the flag was off, as intended). These are checkpoint
write totals, not total primary/source/WAL amplification or peak disk usage.

##### Wakeup-only 50K qualification and off-writer reader preparation

All four 50K arms of `pr593-completion-lane-ab-20260908-retry` passed execution,
native readiness/restart and paired recall gates. This compares the new
completion lane with the pre-lane current binary, not with the older preserved
no-copy binary. The separate 1M comparison is still running.

| Pair / arm | Ready s | C30 QPS | C30 p95 ms | Live recall | Mixed write p95 ms | Mixed query p95 ms | Mixed catch-up s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 pre-lane | 15.334 | 1,771.0 | 41.595 | 98.42% | 148.791 | 58.810 | 1.157 |
| 1 completion lane | 15.479 | 1,685.2 | 46.222 | 98.46% | 149.490 | 57.920 | 1.146 |
| 2 completion lane | 17.104 | 1,674.0 | 47.884 | 98.30% | 151.097 | 58.629 | 1.250 |
| 2 pre-lane | 15.082 | 1,731.5 | 45.556 | 98.36% | 148.866 | 57.737 | 1.039 |

| Pair / arm | Load/read RSS GB | Mixed RSS GB | Live physical-footprint ledger peak GB | Allocated disk GB |
| --- | ---: | ---: | ---: | ---: |
| 1 pre-lane | 1.611 | 1.735 | 0.567 | 0.418 |
| 1 completion lane | 1.514 | 1.428 | 0.783 | 0.425 |
| 2 completion lane | 1.531 | 1.820 | 0.571 | 0.418 |
| 2 pre-lane | 1.453 | 1.536 | 0.805 | 0.423 |

The initial full checkpoint's completed-worker wait falls from 153.397 ms
in the first control to 0.017/0.015 ms in the two candidates. Mixed delta
waits remain 708.640/764.389 and 749.135/982.890 ms, versus
732.796/670.627 ms in the first control. Wakeup removes the idle scheduling
interval but does not remove mutation ownership or installation work. C30
throughput is lower and p95 higher in both candidate pairs; memory and disk
are mixed. This is not an established end-to-end performance win. Raw stage
logs and `50k-comparison.json` retain the evidence without blaming host
contention or conflating a microsecond handoff improvement with request p95.

Implemented the next guarded experiment in `01123472c`:
`ANTFLY_EXPERIMENT_STAGE_POSTING_READERS=1`. The checkpoint worker opens
immutable readers, validates metadata and builds scan-admission/delta indexes
before signaling completion. This preparation performs no WAL rotation,
recovery replay or CURRENT write. The mutation owner independently prepares
the real durable transaction and checks every staged file descriptor,
namespace and checkpoint coverage before reusing those readers. It then
rebinds the root to the actual WAL identity and rebases newer immutable
mutation blobs, retaining tombstones and concurrent source coverage. Reader
roots own their allocation lifetime through the last query lease; the builder
does not leave a pointer to its temporary allocator behind.

Seven focused Debug tests pass with the flag enabled; nine pass with it
disabled, with no reported leaks. They include stale cached-reader rejection,
unchanged CURRENT/live-WAL during staging, every staged-reader allocation
failure, source-capture ownership and the full/delta/suffix pinned-reader plus
concurrent-tail/restart scenario. Small fixture traces show reader installation
at 0–1 microseconds with preparation in the worker. These fixture timings are
not 50K/1M performance results. `readers_stage_ns` is nested in worker build
wall time and must not be added to it.

ReleaseFast binary reserved for the separate reader-staging A/B:
`.benchmark-assets/pr593-staged-posting-readers-20260908/bin/antfly`, SHA-256
`3a77c694eb8d436b9d33e661485dba473cd268134c632006001fd5dee13043b7`.
The active wakeup matrix still uses its original pinned binaries. This new
flag remains off; its load/query/RSS tradeoffs have not yet been measured.
Rebase work remains in the mutation lane, and true reusable physical leaf
chunks remain unimplemented. Staging readers is not a substitute for either.

Review also corrected the suffix evidence reader to consult the matched
runner's complete environment receipt: the older harness's short environment
allowlist does not record the suffix flag. Missing/inconsistent binary or arm
receipts now fail qualification, and eight Python checks pass. An absent
flag in a partial harness receipt must not be interpreted as proof it was off.

##### Native preparation, certified subgroup, and encoded-row experiments (in progress)

The preserved no-copy and posting-local baselines are unchanged. These are
independent default-off treatments, not a promoted combined configuration:

- `ANTFLY_EXPERIMENT_STAGE_POSTING_READERS`: prepare immutable readers in the
  checkpoint worker (the previously implemented experiment).
- `ANTFLY_EXPERIMENT_STAGE_POSTING_REBASE`: includes reader preparation, then
  pins a committed live generation and prepares the bulk of its rebase in a
  second `std.Io` task. Shared overlay consolidation preserves that ancestor.
  Final publication validates file/WAL/root identities and handles only the
  newer tail when ancestry remains intact. A stable identical source takes
  the zero-allocation rebase path. Forced readiness/close retains the existing
  synchronous fallback; this is not yet an allocation-free O(1) writer handoff.
- `ANTFLY_EXPERIMENT_CERTIFIED_SUBGROUPS`: extends balanced subgroup layouts
  with conservative source-space balls. Training uses decoded projections and
  their authoritative float32 error/norm bounds, never ground-truth queries.
  Query pruning uses a strict lower-bound/top-k-upper-bound comparison, with
  outward numerical slack. Missing/unsafe certificates scan, and complete
  snapshot/filter paths retain their established fallback. Unlike the older
  75%-work representative policy, this mode does not skip groups based merely
  on their representative score. It requires a subgroup layout to be built.
- `ANTFLY_EXPERIMENT_REUSE_POSTING_ROWS`: forward an authenticated unchanged
  no-copy base leaf's encoded row instead of decoding/reordering/rebuilding it
  and fetching projection inputs again. It verifies immutable shadow identity,
  layout compatibility and each private buffer's SIMD CRC. Projection/residual
  locator planes are excluded. This reuses encoding, **not physical extents**;
  `reused_bytes` measures bytes forwarded and is not saved disk space. Reusable
  physical checkpoint chunks and their reference-manifest GC remain unfinished.

Completion traces now record source/maintenance capture overlap, off-lane
rebase time and failed lock-admission observations. Capture overlap and rebase
time can overlap inside completed-worker wait; neither their sum nor the
unattributed remainder is a measured scheduling delay. Lock deferrals are
counts, not durations, and can precede worker completion.

Debug validation so far includes seven standalone subgroup/cache tests,
three focused vector-index codec tests (including raw-row round-trip and
allocation failures), native certified flat/tree lifecycle tests requiring
float32 score/order parity, and pinned-reader/suffix/rebase/restart tests.
A deterministic handoff gate was added so the native suffix test cannot race
publication and silently skip its worker-rebase assertions. Eight Python
checks cover the stage summarizer and treatment evidence. An enabled flag or
an unpublished worker is not enough to qualify an experimental arm.

No 50K/1M performance improvement is claimed for these new treatments yet.
The completion-lane matrix's launcher was paused between arms while its
second 1M candidate finished and these changes compiled; its binaries and
measurement inputs remain pinned. The new comparisons must use the same
binary on/off, retain C1/10/20/30 and mixed workload measurements, and require
the existing native visibility/restart and one-percentage-point recall gates.

Checkpoint `b099035d7` contains these guarded implementations and focused
tests. ReleaseFast binary
`.benchmark-assets/pr593-native-preparation-20260908/bin/antfly` is pinned at
SHA-256 `52ec86674d9efb51780c697cabb5c43327ddd10fca7f57a8b25200d036dc072d`.
Compilation completed before resuming the old matrix's final 1M control.
The worktree also contains independent source-vector/GC changes; all new A/B
arms must use this same binary so those common changes are not attributed to
the native preparation flags. Only 33–34 GiB is currently free: a per-arm
headroom gate is required, and preserved baselines must not be reclaimed.

The completion-lane matrix's first 1M control sharpens the routing diagnosis:
its fixed profile performs 239,192 approximate scores and reads 23.117 MB of
leaf scan data per query. All seven frontier-bound checks overlap; missing
posting bounds and incomplete-top-k fallbacks are zero. The recorded suffix
lower bound is 0 versus a mean top-k upper bound of 0.17528. The current issue
is therefore loose resolved bounds, not the previously fixed NaN-radius gap.

Post-checkpoint review added explicit score-scratch capacity before certified
top-k calculations: a cold fused/global path must not rely on a prior rerank
having populated scratch. It also tightens group proofs by intersecting the
covering ball with the unit sphere, using cross-platform Zig SIMD for the
representative dot/norm calculation. These follow-ups require new Debug tests
and a separately pinned binary before routing qualification; they are not in
the `52ec8667` reader-staging executable above.

##### Completed wakeup-only 1M qualification and query-window attribution

All four 1M arms of
`.benchmark-results/pr593-completion-lane-ab-20260908-retry` passed execution,
restart, visibility, and paired recall gates. They do **not** qualify the
wakeup lane as an end-to-end performance improvement:

| Pair / arm | Ready s | C30 QPS | C30 p95 ms | Recall % | Mixed write p95 ms | Mixed query p95 ms | Catch-up s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 control | 299.815 | 861.319 | 67.083 | 99.02 | 116.799 | 52.522 | 43.506 |
| 1 completion lane | 361.712 | 622.877 | 102.318 | 99.01 | 663.107 | 111.883 | 64.460 |
| 2 completion lane | 422.928 | 672.493 | 96.185 | 99.05 | 134.710 | 57.648 | 73.648 |
| 2 control | 385.090 | 762.882 | 84.931 | 99.03 | 100.841 | 53.772 | 46.535 |

Read-only RSS, mixed RSS, and post-restart allocated disk in decimal GB were
5.220/5.200/3.821 (control 1), 5.729/4.921/3.817 (candidate 1),
5.970/4.780/3.831 (candidate 2), and 5.442/5.454/3.819 (control 2).
RSS is not physical footprint. Full receipts and every original metric remain
in `full-comparison.json`; no historical best was substituted for a control.

`scripts/summarize_native_query_windows.py` joins the explicit client C1/10/20/30
start/end timestamps to the **inner** Prometheus snapshots. Both candidate
C30 windows have zero checkpoint-completion-round increments. Thus active
checkpoint publication during those sampled windows does not explain their
query regression. Mean admission wait per sampled grant was 6.978/9.004 ms
in pair 1 and 7.893/8.332 ms in pair 2 (control/candidate). Peak sampled active
queries was 17 in every arm. These observations neither explain all missing
throughput nor establish host contention or an admission-policy root cause.
Counter windows cover 27–29 seconds, not the entire client wave; absent control
completion counters remain absent rather than being fabricated as zero.

The same-binary reader-staging matrix is separately in progress at
`.benchmark-results/pr593-staged-readers-ab-20260908/staged_readers`.
Its first mixed 50K delta moved 63.682 ms of control reader installation to
60.981 ms of candidate worker preparation; candidate writer reader work was
0.001 ms. However, that candidate still waited 1,211.271 ms after file-worker
completion, including 1,061.006 ms overlapping source capture, with 13 failed
lock observations. Control overlap was 909.016 ms of 1,052.732 ms waiting.
These are different live capture windows, not an isolated speedup ratio.
The candidate's final rebase/prepublication preparation was only 0.974 ms.
The data supports moving reader work but identifies capture lifetime—not that
small final rebase—as the dominant remaining blocker in this 50K sample.

Follow-up Debug native flat/tree certificate lifecycle tests pass with actual
bound pruning and float32 score/order parity. The Python runner/evidence/window
suite passes 29 tests, including hermetic disk-headroom checks. New runner
inputs and binaries are hashed in each arm receipt and must not be edited while
the matrix is active. No new experiment has been promoted to a default.

##### Capture preparation experiment (implementation, not yet qualified)

The measured capture overlap includes work preceding capture completion; it
must not be mislabeled as a one-second WAL fsync. Replay currently acquires its
source capture **before** opening the primary journal cursor and collecting,
decoding, and allocating the next replay window. That preparation is read-only.

`ANTFLY_EXPERIMENT_DEFER_SOURCE_CAPTURE` moves capture acquisition to the
existing post-collection/pre-apply window hook in both threaded executors.
The worker still owns one exact session token from the first mutation through
durable finish. Additional coalesced windows retain that token. Empty cursors
need no mutation capture, and acquisition failure aborts before any apply and
retries from the durable sequence. Index deletion and shadow activation already
stop/join the worker before replacing its index; this change does not bypass
that lifecycle fence. Primary cursor leases remain owned until close.

This experiment does not change batch size, replay byte/item limits, public
sync semantics, or atomic source coverage. It does **not** move artifact loading
inside the apply callback or tree mutation off the writer lane. Common
`ANTFLY_EXPERIMENT_CAPTURE_STAGES` tracing now reports collection and apply
separately from capture-finalization timers. Compare those stages and checkpoint
capture overlap before claiming it recovers a material part of the stall.
The active reader-staging matrix still uses its original pinned executable.

Validation: both new executor tests pass. A broader Debug run passes 25 tests
(both replay executors plus native flat/tree certificate lifecycle); five
additional ownership/empty-target checks pass. The additional existing
`db dense auto bulk finish wakes weak-sync replay and publishes visibility after
catch-up` test fails its `publish_blocking_checkpoint_clean` assertion with the
capture flag **both on and off in the same Debug executable**. The suite is not
fully green; do not suppress that separate lifecycle qualification issue or
attribute it to this treatment. Python analysis/runner tests pass 30 checks.

The first complete 1M reader-staging pair reports control/candidate readiness
340.271/306.589 s, C30 610.884/836.803 QPS, p95 101.487/72.747 ms, recall
99.04/99.03%, mixed write p95 160.417/91.437 ms, mixed query p95
56.103/42.316 ms, and catch-up 50.163/35.592 s. Read-only RSS increases
5.058→6.471 GB and mixed RSS 4.812→5.421 GB, while measured attributable
live demand falls 1.728→1.182 GB. Allocated disk is 3.815/3.797 GB.
This one pair is encouraging for latency but not an all-metric win, and the
post-restart fixed-query mean increases 11.653→12.318 ms. The reversed pair
remains necessary; first-pair results are not a promoted baseline.

The new ReleaseFast build failed after an intermediate `kmeans_metal.o` became
unavailable in the shared `/tmp/zig-local-cache`. No new performance executable
was produced; retry with a dedicated cache. The second reader-staging candidate
load briefly overlapped the failed build's remaining compiler before it was
terminated, so that load sample is not clean compiler-isolated evidence. Its
pinned binary/inputs are unchanged. Available disk meanwhile rose to about
160 GiB without this task deleting any benchmark data; the earlier headroom
blocker no longer applies.

##### Reader-staging matrix complete; capture experiment qualification gate

All eight reader-staging arms passed the archived harness's execution,
visibility, restart, and paired recall gates. Results are preserved in
`.benchmark-results/pr593-staged-readers-ab-20260908/full-comparison.json`.
The second 1M candidate's load retains the compiler-overlap caveat above.

| Case / reversed pair arm | Ready s | C30 QPS | C30 p95 ms | Recall % | Mixed write p95 ms | Mixed query p95 ms | Catch-up s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 50K / 1 control | 23.669 | 1437.901 | 51.859 | 98.36 | 164.888 | 73.612 | 1.357 |
| 50K / 1 staged | 16.318 | 1375.631 | 59.973 | 98.36 | 194.209 | 87.160 | 1.459 |
| 50K / 2 staged | 15.957 | 1653.696 | 45.467 | 98.36 | 150.201 | 58.000 | 1.038 |
| 50K / 2 control | 15.329 | 1632.518 | 49.331 | 98.44 | 149.650 | 57.587 | 1.157 |
| 1M / 1 control | 340.271 | 610.884 | 101.487 | 99.04 | 160.417 | 56.103 | 50.163 |
| 1M / 1 staged | 306.589 | 836.803 | 72.747 | 99.03 | 91.437 | 42.316 | 35.592 |
| 1M / 2 staged | 373.188 | 692.713 | 92.533 | 98.96 | 151.617 | 61.731 | 56.068 |
| 1M / 2 control | 427.790 | 784.836 | 82.613 | 99.02 | 92.743 | 50.524 | 46.008 |

The pair direction reverses for 1M QPS, query p95, mixed write p95, and
catch-up. Even the aggregate 1M C30 improvement (697.860 to 764.758 QPS;
92.050 to 82.640 ms p95) accompanies higher read-only RSS (4.922 to
5.994 GB), mixed RSS (4.892 to 5.018 GB), and a worse post-restart fixed
profile. Allocated disk is essentially unchanged (3.805 to 3.817 GB).
This is not a promoted all-metric baseline.

Deferred capture now has three passing Debug executor tests, including a real
`std.Io` worker proving that an empty dense target advances only after the
coverage callback permits it, without opening or finishing a mutation capture.
The A/B runner requires common capture-stage tracing and records observations
only when the exact source-session token successfully persists a watermark
covering the observed window. Coalesced windows can finish at a later sequence;
failed captures, insufficient coverage, duplicate committed tokens, and inert
treatments do not qualify. These collection times are not claimed as saved
wall time. Reader staging is common in both deferred-capture arms.

A suspected no-copy projection-loading waste was ruled out: catalog layout
configuration clears the projection loader when neither retention nor subgroup
training is enabled. No redundant fix was made there. Remaining capture work
includes mutation/application and intentional session reuse; the delayed-begin
experiment only removes initial read-only preparation from that interval.

#### Dense replay delete planning and single-read eager leaf refresh (2026-09-08)

The deferred-capture matrix stopped in its first 50K control during the official
cold-restart client: `httpx.ConnectError: [Errno 49] Can't assign requested address`.
The server had reopened and no matching crash/quarantine was found. This is an
unqualified arm, not a product or performance pass. Its data/logs remain under
`.benchmark-results/pr593-deferred-capture-ab-20260908/deferred_capture/`.
No candidate or 1M result was produced by that matrix.

The more substantial target is capture-held **apply**, not journal collection.
On a separate resumed 50K diagnostic, 800 updates took approximately 608–642 ms
in apply while HBC insertion itself took only 28–29 ms. Collection took roughly
0.3–1.4 ms in the original control. The diagnostic lives under
`.benchmark-results/pr593-apply-attribution-20260908/`; its initial relative-path
invocation failed result discovery, so it is not a fresh qualification result.
The absolute-path `-apply`, `-sample`, and `-earlysample` diagnostic resumes
completed. These repeatedly mutate one corpus and are attribution, not paired
readiness/recall/QPS evidence.

A five-second live sample, `/private/tmp/pr593-dense-apply-early-sample-20260908.txt`,
recorded 2,936 samples below async derived apply. Of these, 1,694 were in the
replacement-delete call and 945 in the subsequent overwritten-document delete
call (about 90% combined). The first included 1,167 samples in eager centroid
recomputation and 394 in leaf save/payload rebuilding. The second included 735
samples in batch deletion, almost entirely its missing-mapping full-tree fallback.
These are sampled stack observations, not independently additive timers.

Two independent default-off treatments now target that work:

- `ANTFLY_EXPERIMENT_COALESCE_REPLAY_DELETES`: build one ordered union of permitted
  replay deletes, per-index embedding replacements, and non-chunk parent
  overwrites. Deduplicate before any mutation, retaining the existing chunk
  ownership exclusions. One source capture still owns the entire operation.
- `ANTFLY_EXPERIMENT_REUSE_DELETE_VECTORS`: eager batch deletion loads each
  surviving leaf's authoritative transformed vectors once, then uses the same
  leaf-scoped matrix for centroid/radius and the existing payload refresh.
  Member ordering, arithmetic, dirty versions, quantization, and exact completion
  remain unchanged. Lazy/deferred refreshes, empty leaves, and nonquantized
  indexes retain their old paths. The matrix is reported as apply workspace and
  released before the next leaf; no cross-revision vector cache is introduced.

The zero-vector-cache test also exposed an existing local-cache ownership bug:
fallible admission could publish an entry, free it through overlapping error
handlers, and leave the cache pointing at freed memory. Local node, quantized,
vector and metadata admission now reserves both maps and allocates ownership
before publication; the two map inserts cannot allocate. Payload ownership
transfers only on success. Disabled local vector caching declines admission.

Seven focused Debug tests passed without leaks, including allocation-failure
sweeps for all four local caches, randomized tree churn, existing deferred
quantization, and delete-refresh parity across L2/cosine/inner-product with root
and split-leaf payloads. The parity test verifies fewer authoritative loader
calls, single-delete error behavior, and identical membership, centroid bits,
radius bits, posting versions and query results after reopen. Seven Python
experiment tests passed. The runner supports each treatment independently and
`dense_delete_plan` together, with equal stage instrumentation in both arms and
nonzero work-reduction evidence required.

No performance default is promoted. This reduces work inside the capture; it
does **not** implement private off-writer HBC mutation, bounded reader rebase, or
reusable physical checkpoint chunks. Same-vector replacement elision is also
intentionally excluded: the latest primary artifact is not proof of the vector
revision represented by the existing HBC payload. Matched public batch-100
50K/1M and mixed-workload results remain required before promotion.

The subsequent combined-treatment 50K reversed pairs all passed qualification
under `.benchmark-results/pr593-dense-delete-plan-ab-20260908/`. Both arms used
staged readers, capture tracing, public batch 100 and 1,000 mixed write rows/s.
The ReleaseFast binary SHA256 was
`bc38c484dc43cfdcbf897ffd779db1a57a409a32c21180a5db8d85098e6c6c76`.
Results below are control to candidate within each pair, not cross-pair bests.

| Metric | Pair 1 | Pair 2 (candidate ran first) |
| --- | --- | --- |
| Ready seconds | 14.612 → 17.624 | 15.687 → 15.898 |
| C30 QPS | 1,165.0 → 1,241.6 | 1,655.9 → 1,730.7 |
| C30 p95 ms | 72.439 → 68.254 | 46.755 → 44.963 |
| Live recall % | 98.42 → 98.30 | 98.47 → 98.41 |
| Mixed write p95 ms | 287.183 → 199.270 | 150.672 → 152.332 |
| Mixed query p95 ms | 75.822 → 68.066 | 58.615 → 61.154 |
| Mixed QPS | 308.90 → 344.43 | 404.37 → 383.79 |
| Mixed catch-up seconds | 1.395 → 0.841 | 0.944 → 0.626 |
| Read-only phase peak RSS GB | 1.512 → 1.581 | 1.484 → 1.500 |
| Mixed phase peak RSS GB | 1.548 → 1.437 | 1.192 → 1.378 |
| Apply ms / 1,000 replayed rows | 838.90 → 688.80 | 794.92 → 692.70 |

`scripts/summarize_dense_delete_experiment.py` attributes mixed apply through
the final covered source sequence, excluding fresh insertion and later churn.
It rejects failed/incomplete arms and duplicate replay sequences. Normalized
apply work fell 17.9% and 12.9%; window counts changed from 24 to 43 and 32 to 44,
so comparing individual windows alone would exaggerate the improvement.
In pair 1, cumulative delta completed-worker wait was essentially unchanged
(805.7 versus 796.8 ms), despite lower maximum individual stalls. Pair 2 totals
were 1,390.8 versus 758.4 ms. These intervals are not additive with capture
overlap or apply timers.

Three Debug DB integration tests passed with both treatments enabled, including
1,000-document batch-100 streaming activation/reopen. Thirty-three runner tests
and three attribution tests passed. The earlier cold-client connection failure
did not recur, but its precise cause was not established. Other builds/tests
were active on this host; the reversed pairs do not establish an all-metric
win, and no 1M qualification or default promotion is claimed.

The remaining structural cost is eager whole-leaf refresh: pair 1's candidate
still processed 1,647,680 surviving vector rows for 30,000 mixed updates.
The proposed next design is stable, explicitly identified scoring origins,
revision-aware row deletion/addition deltas, and bounded background recentering
and repacking. Deletion can retain a conservative bound about an unchanged
origin; insertion must expand it or disable pruning until safe. This requires
coherent publication of row revisions, routing bounds and source coverage,
bounded delta scan/recovery debt, and lease-safe reclamation. It is not yet
implemented, and is distinct from merely moving the current rebuild to another
thread or delaying maintenance without a debt limit.

#### Stable scoring origins: first integrated mutation experiment (2026-09-08)

`ANTFLY_EXPERIMENT_STABLE_POSTING_ORIGINS` is default off. The first integrated
stage selects surviving rows from a payload bound to the pre-delete membership
and posting mutation version. Codes, per-row error metadata and scoring origin
are copied unchanged; the old conservative routing sphere still covers the
remaining subset. Membership, payload, mappings and posting state commit through
the existing native capture/WAL and coherent search publication. No new float16
plane or source-vector cache is introduced.

Centroid statistics remain explicitly dirty while the payload version advances.
Subsequent appends use the existing quantizer's scoring origin and expand the
routing sphere about its unchanged anchor. A stale anchor is never weighted as
an exact mean. The experiment caps centroid-version lag at 64 mutations; an
exhausted cap falls back to authoritative refresh. Existing resource-governed
posting/layout maintenance also drains debt. Single-row deletion defers the
underfilled-leaf merge when rows were preserved, matching the background layout
path instead of immediately rereading survivors. Disabling the experiment after
restart refreshes stale statistics before incremental centroid arithmetic.

Review also found a duplicate delete in the true mixed HBC transaction: an eager
default-options pass preceded the intended options-aware pass. The first pass
was removed; the retained pass preserves the full delete-before-insert contract.

Debug coverage includes all three metrics and root/split leaves, bitwise selected
payload parity, zero survivor loader calls during row-preserving deletion,
reopen with explicit centroid debt, bounded repair, mixed replacement churn,
radius coverage, cap enforcement and flag-off restart. Row selection rejects
unordered/duplicate/out-of-range offsets and passes allocator-failure sweeps.
Public storage integration tests passed for indexed overwrite/reopen and the
1,000-document batch-100 native streaming/reopen path with the flag enabled.

This is deliberately NOT yet the full requested long-term format: surviving
rows are still compacted/copied into an aggregate payload, and generic WAL
patch reconstruction still materializes aggregates. The cap can still force a
foreground refresh. The remaining stage is revision-aware base-row references
plus inserted row blocks, query-side bounded selection under generation leases,
and off-writer chunk repacking with explicit admission/backpressure. Those pieces
must replace, not merely hide, the remaining copy/rewrite work. The existing WAL
has source-coverage/checksum identities and bounded delta chains, but those are
not substitutes for row-level revision identity or chunk-level reclamation.

All four 50K arms subsequently passed under
`.benchmark-results/pr593-stable-origins-ab-20260908/`. Both arms enabled staged
readers and the previous coalesced/single-read delete treatment. Only the
candidate enabled stable origins. The runner requires positive preserved-row
evidence; that treatment may supersede eager refresh entirely, so zero eager
reused rows is acceptable only alongside positive preserved-row evidence.
Public batch size remained 100, mixed offered writes 1,000 rows/s, query
concurrency 1/10/20/30, and no-copy projection policy remained unchanged.

The paired binary SHA256 was
`60c60dbfe767d072cec09c8a151b662df611284f454a3ea62e200dc61c86901e`.
This build started before the last lazy-policy cap guard; that guard affects
explicit lazy maintenance, which this matrix did not enable. A subsequent
ReleaseFast build including the guard also passed, SHA256
`a52229dae8091889818b40fa96fa4c2e2b24a357f2e903d000ee66c97b9fbe5e`, under
`.benchmark-assets/pr593-stable-origins-final-20260908/`; it is not the binary
used for the following paired measurements. Both executables are preserved.

| Metric | Pair 1 control → candidate | Pair 2 control → candidate |
| --- | --- | --- |
| Ready seconds | 16.388 → 14.972 | 16.982 → 15.246 |
| C30 QPS | 1,731.4 → 1,678.5 | 1,842.1 → 1,740.1 |
| C30 p95 ms | 44.215 → 46.003 | 40.258 → 44.987 |
| Live recall % | 98.27 → 98.40 | 98.42 → 98.34 |
| Mixed query QPS | 408.54 → 406.96 | 445.71 → 427.92 |
| Mixed query p95 ms | 58.860 → 60.033 | 60.085 → 58.988 |
| Mixed query p99 ms | 70.941 → 90.486 | 67.605 → 74.486 |
| Mixed write p95 ms | 149.459 → 154.426 | 150.637 → 151.204 |
| Mixed catch-up seconds | 0.849 → 0.115 | 0.653 → 0.226 |
| Mixed phase peak RSS GB | 1.410 → 1.517 | 1.339 → 1.034 |
| Read-only phase peak RSS GB | 1.459 → 1.476 | 1.491 → 1.444 |
| Pre-restart total allocated disk MB | 441.446 → 440.537 | 446.222 → 440.959 |
| Apply ms / 1,000 replayed rows | 694.55 → 343.62 | 631.06 → 336.21 |
| Maximum mixed apply window ms | 514 → 97 | 448 → 91 |

Both pairs replayed exactly 30,100 mixed rows per arm through source sequence
802. The normalized apply reductions were 50.5% and 46.7%. Window counts rose
from 49 to 242/250, however, and total insertion-stage work rose from
2.134 to 4.036 seconds and 2.108 to 3.486 seconds. Faster deletion drains the
backlog into smaller batches, exposing per-mutation setup/copying and reducing
grouping opportunities. This is evidence for the remaining row-reference design,
not a reason to add benchmark-specific batching delays.

Each control published three mixed delta checkpoints; neither candidate did.
That removes those particular completed-worker waits, not maintenance forever:
pair 1 retained an 84.4 MB logical posting WAL versus 22.9 MB in the control,
while persisted ANN data fell from 99.3 to 21.9 MB. Total allocated disk was
nearly unchanged. Fewer checkpoint output bytes cannot be reported as an equal
reduction in physical disk occupancy or total process write I/O.

Additional Debug tests passed for changed external vector revisions across
reopen (all three metrics), aborted-publication cache isolation and allocation
failure ownership. Twenty-seven Python runner/evidence tests passed. Other
builds/tests were active on this host; source-apply counters demonstrate less
work, but the public latency/RSS observations are not an all-metric win.
Read-only queries precede the mixed deletion treatment, so their QPS difference
is not established as a causal effect of row-preserving deletion.

No default is promoted and no 1M qualification is claimed. The next stage must
remove aggregate row copying/reconstruction and make native queries consume
bounded revision-aware base/delta row views under the same generation lease.
Selective off-writer repacking and a bytes/row-density/age debt policy remain
necessary to complete the requested shape; the current mutation-count cap is
only an experimental guard, not that production policy. The mixed p99 regression
still needs attribution rather than an assumption that all remaining latency
belongs to HBC maintenance.

#### Revision-aware posting row-store component experiment (2026-09-08)

Implementation status: **component experiment, not an activated HBC backend**.
`lib/vectorindex/src/posting_row_delta.zig` now implements immutable AFRC
RaBitQ chunks, ordered AFRM row-reference manifests, and a native fused range
scanner. A deletion copies references rather than survivor codes. Replacement
rows get a new physical chunk identity; a stale old-row tombstone cannot
delete the new revision of the same vector ID. Source coverage, logical
revision, leaf incarnation and scoring-origin identity are explicit.

Chunks reuse the existing aligned AFQD codec without a float16 projection
plane. Immutable references hold heap buffers or mapping leases through the
last query/manifest reference. Manifest decoding validates dependency identity,
length, checksum, row ranges, live-ID uniqueness and compatible origins before
exposing any scan. CRC remains `antfly_hash.Crc32`, not a new scalar checksum.
Chunk serial allocation/uniqueness remains the responsibility of the eventual
durable owner; CRC is not a substitute for revision identity.

The leaf-scoped repacker prepares from a pinned view, then rebases newer rows
outside publication. It rejects replaced reader/chunk identities, avoids
resurrecting deleted revisions, and does not read authoritative survivor vectors
or recenter implicitly. Soft maintenance debt is based on retained bytes,
fragment count, chunk count, tombstone density and age; hard bounds reject
excess debt rather than perform synchronous repacking. These are a policy/API
for the future resource-manager integration, **not a newly wired background
scheduler or filesystem reclamation implementation**.

One immediately integrated improvement removes the unnecessary whole-payload
clone before `prepareDeletedLeafRows` selects survivors: it now holds the
existing transaction/cache read lease for that read-only operation. The selected
aggregate is still copied/serialized by the current backend. This does not
silently activate row manifests or change defaults.

Correctness checks:

- Debug row/WAL tests: 26 passed, one ReleaseFast-only microbenchmark skipped.
  Coverage includes all three metrics, bit-exact scores/error bounds across
  repacking, canonical order, cancellation, stale/duplicate tombstones, changed
  vector revisions, missing/corrupt chunks, truncated manifests, source-sequence
  regressions, allocation-failure cleanup, empty views and last-lease release.
- A real `std.Io` worker prepares/rebases while the writer mutates and an older
  query keeps its rows. Both observation waits have five-second deadlines;
  worker join/cancellation precedes snapshot and I/O-runtime destruction.
- A framed posting-WAL fixture verifies every truncated suffix of a second
  transaction leaves the first manifest visible until commit. This is a
  transaction-codec test, **not a disk/fsync/CURRENT crash qualification**.
- The three existing HBC stable-origin tests passed after clone removal.
  Indexed overwrite/reopen (including durable LSM primary) and the 1,000-row,
  batch-100 native streaming/reopen test also passed in Debug.

The ReleaseFast component test uses one synthetic 1,024-row, 768-dimensional
leaf, removes 32 distributed rows, and optionally replaces all 32. It repeats
49 or 977 leaf operations, labelled `work_rows=50000/1000000`, in four reversed
control/candidate rounds. **These are work counts, not fresh 50K/1M datasets.**
Both arms include new-row quantization; the candidate also includes append-chunk
encoding/validation. The control starts with a borrowed decoded payload, then
selects survivors and encodes the replacement protobuf. Neither arm includes
HTTP, source artifacts, generic WAL-patch generation, fsync, routing, admission,
checkpoints, or exact completion.

For the 977-leaf work count:

| Component | Aggregate control | Row-reference candidate |
| --- | --- | --- |
| Delete preparation/encoding per leaf | 18.23–18.51 µs | 0.512–0.521 µs |
| Delete cumulative requested allocation bytes | 221.20 MB | 2.93 MB |
| Replace preparation/encoding per leaf | 40.40–41.35 µs | 30.25–30.77 µs |
| Replace cumulative requested allocation bytes | 324.18 MB | 138.96 MB |
| Replace encoded output, before WAL compression | 113.08 MB | 8.41 MB |

The first prototype rebuilt a survivor-sized validation table on every delete,
costing 14–15 µs/leaf. Validating the immutable parent once, then checking only
new append/survivor ID collisions, removed that remaining metadata allocation.
Delete-only mutations now perform four allocations per leaf versus seven in
the optimistic aggregate control. Replacement still performs 33 versus eight:
its byte savings are real but append construction has further allocator work.

Fused scanning of 33 retained runs within one chunk is nearly equal to a
single repacked run (roughly 7.7–8.0 ns/live row). **Two chunks after replacement
cost about 3–5% more** than repacking: 8.12–8.19 versus 7.76–7.91 ns/live row.
The dirty two-chunk leaf retains 133,384 encoded bytes versus 126,212 after
repack. Deferred maintenance is therefore not free; fan-out and reclaim debt
must remain bounded. Requested bytes are not peak RSS, and encoded bytes are
not total allocated disk or savings versus the existing AFPD-compressed WAL.

Raw output: `.benchmark-results/pr593-posting-row-store-20260908/component-final.log`.
Measured module SHA256:
`dcb1e30537db469701cb00251f6eaceacd57184dcf302305bfc0cc448c224dd1`.
Reproduce with `zig build lib-vectorindex-test -Doptimize=fast --summary all -- 'posting row representation microbenchmark'`.

**Remaining integration before public qualification:** allocate immutable chunk
identities through the durable index owner; stage/fsync chunks and commit row
manifests with membership, routing, mappings and source coverage in the native
capture; bootstrap/recover the representation without treating AFRM as the
existing quantized protobuf; connect complete SearchView leases and query
filters/coverage/exact completion; publish prepared repacks under the existing
generation-token validation; connect resource-manager admission and durable
file-reference reclamation. Then run matched public-API 50K and 1M read-only
and mixed qualification. No such qualification or default promotion is claimed
for this component checkpoint; the requested whole production shape is not yet
complete.

#### Integrated native posting rows — correctness checkpoint (2026-09-08)

The component above is now wired into the native HBC authority behind
`ANTFLY_EXPERIMENT_POSTING_ROW_DELTAS` (default off). This is an integrated
experiment, not a performance promotion. Optional LSM-authoritative sidecars
and V1 catalog entries do not use the row-only mutation path.

- A checksummed AFRA allocation cursor is committed at row-chunk key zero.
  New AFRC chunks and AFRM manifests participate in the existing source WAL
  transaction with membership, routing, vector mappings and source coverage.
  Source serials and checkpoint serials occupy disjoint namespaces; full
  checkpoints retain the cursor even when all old chunks become unreachable.
- Native deletes filter revision-qualified row references without reading or
  requantizing survivors. Appends quantize only new vectors against the retained
  scoring origin. Existing aggregate native leaves convert lazily through the
  quantized-store path; they remain readable alongside row-native leaves.
- Queries borrow generation-leased code spans and use the existing candidate
  selector, filters, coverage checks and authoritative float32 completion.
  Single-run membership is borrowed, not flattened into another ID allocation.
  Reads remain supported when the experiment is disabled after restart.
- Repacking runs in the governed checkpoint worker. Soft density/fan-out/byte
  limits and aging request maintenance; hard limits are retryable replay
  backpressure. Recovery detects manifest debt without reading code planes.
  A selective delta repack retains the existing base file. Full checkpoints
  flatten the bounded segment chain and drop unreachable chunks.
- A full checkpoint copies the captured manifest's chunk-reference closure
  even when publishing a compact replacement. A newer source WAL tail can
  still reference those captured chunks. Generation/backing leases prevent
  reclamation while older queries need retired files. A later full checkpoint
  removes chunks that are no longer reachable. This is chunk reuse within the
  existing immutable-segment lifecycle, not independently addressable chunk
  files or elimination of every whole-segment rewrite.

Integration testing caught an eager single-delete merge that undid row
preservation; the merge guard now also recognizes native rows. The scheduling
review also removed a whole-generation rewrite on each soft row-debt event:
those events use selective delta repacking while the segment chain has room.
Pre-qualification review also caught an admission overestimate: row-native
leaves must publish their RaBitQ scan-byte cost, not the float32 fallback cost
merely because they live outside the aggregate directory. Both checkpoint
encoders now preserve that distinction, with an integrated regression assertion.

Validation so far: 43 row/WAL/segment Debug tests passed (one performance-only
test skipped); 21 focused storage/capture/checkpoint tests passed without leaks;
the database overwrite/reopen checks and 1,000-/10,000-document streaming
replay checks passed with the flag enabled. The integrated fixture covers L2,
cosine and inner product, filtered/range and unfiltered score parity, capture
abort, missing chunks, same-ID source-vector replacement, staged full/delta
repacks with a newer WAL tail, pinned old readers, reopen, and disabling the
treatment. Ten benchmark-evidence tests pass; qualification requires actual
native deletions and a matching durable row-checkpoint publication, not merely
the environment flag. The archived-harness runner now also rejects public-write
errors and retries, source-store poisoning/OOM, and native chunk/capture errors
even when the VectorDBBench client process eventually exits successfully.

Matched public-API 50K/1M performance qualification is still pending. The
standalone microbenchmark numbers above must not be presented as integrated
latency, RSS, or disk improvements.

##### First integrated public-API 50K A/B (2026-09-08)

Frozen implementation: `0326a2bd9`, ReleaseFast executable under
`.benchmark-assets/pr593-integrated-posting-rows-qualified-20260908/bin/antfly`.
Evidence: `.benchmark-results/pr593-integrated-posting-rows-ab-20260908`.
The same executable includes common in-progress source-vector changes from the
shared worktree; this is a flag-off/on comparison, not a clean-commit comparison
against historical binaries. Each receipt records executable/helper SHA-256s.
Both arms use no-copy, staged readers, dense-delete preparation, batch 100,
`sync_level=write`, C1/10/20/30, fixed-query profiling and 1,000 offered mixed
overwrite rows/s. AB then BA fresh loads passed write-error, native-treatment,
visibility, restart and paired recall gates. The full-text index is disabled
equally through the public API. The subsequent 1M pairs also completed; see below.

Two-arm medians (decimal GB/MB; not a best-of-each composite):

| 50K metric | Control | Native row deltas |
| --- | ---: | ---: |
| Full readiness | 15.44 s | 16.01 s |
| C30 QPS | 1,673 | 1,639 |
| C30 p95 / p99 | 48.09 / 69.99 ms | 46.88 / 69.53 ms |
| Live recall | 98.33% | 98.46% |
| Mixed query QPS / p95 | 390 / 60.11 ms | 386 / 60.43 ms |
| Mixed write p95 | 151.59 ms | 151.57 ms |
| Final mixed catch-up | 0.737 s | 0.480 s |
| Sampled mixed RSS | 1.537 GB | 1.344 GB |
| Demand high-water | 1.007 GB | 0.823 GB |
| Physical-footprint ledger high-water | 0.783 GB | 0.823 GB |
| Allocated disk after restart | 440.49 MB | 499.14 MB |
| Post-restart leaf-scoring mean | 1.182 ms | 1.422 ms |

The normalized replay counters expose cost transfer rather than a total
maintenance win. Each arm overwrote and replayed 30,100 rows. Total apply cost
was 671.2/716.4 ms per 1,000 rows in controls and 781.8/769.7 in candidates.
Aggregate delete time fell from 17.78/18.85 s to 2.93/3.02 s, but embedding
apply rose from 2.12/2.43 s to 20.27/19.86 s. Existing detailed timers locate
most of that increase in leaf mutation, not capture finalization. Do not call
the smaller final catch-up timer a reduction in total replay work.

Disk attribution puts the increase in ANN/index persisted bytes (median
121.89 to 180.11 MB), not duplicated source embeddings. Lower RSS/demand does
not establish lower physical footprint: the ledger median is 5.1% higher.
Read-only and mixed throughput vary materially between repetitions. The row
path currently forces fused scoring, unlike ordinary leaves; that is a
candidate for an isolated follow-up, not yet a proven cause of the slower
post-restart leaf timer. Keep the experiment default-off pending recovery of
the measured mutation/space costs.

##### Completed integrated public-API 1M A/B (2026-09-08)

All eight 50K/1M arms completed successfully, including actual row mutation and
durable publication evidence, mixed-write coverage, restart and paired recall.
Every arm used executable SHA-256
`8b61fa303dbdcd145b2baa20a7e8c8bef2fe8a98b03ea19f9e59ee24d718dbcd`.
`matched-summary.json`, `replay-work-summary.json` and the original per-arm
receipts/logs are preserved in the A/B root above. Correctness qualification is
not performance promotion: no defaults were changed. This was a shared-host
experiment without CPU isolation; other work and bounded correctness-test builds
overlapped parts of the run. Keep both run orders and do not infer a historical
baseline comparison or statistical confidence from two repetitions.

| 1M metric, two-arm median | Control | Native row deltas |
| --- | ---: | ---: |
| Insert / full readiness | 263.53 / 347.74 s | 281.15 / 382.44 s |
| C1 QPS / p95 | 81.3 / 16.52 ms | 53.2 / 37.65 ms |
| C10 QPS / p95 | 647.6 / 23.91 ms | 281.3 / 96.10 ms |
| C20 QPS / p95 | 705.4 / 67.89 ms | 314.1 / 109.76 ms |
| C30 QPS / p95 / p99 | 593.5 / 110.43 / 144.24 ms | 436.2 / 117.69 / 150.03 ms |
| Live recall | 99.01% | 99.05% |
| Mixed query QPS / p95 | 195.5 / 120.35 ms | 225.0 / 81.79 ms |
| Mixed accepted write rate / request p95 | 856.1 rows/s / 778.69 ms | 860.3 rows/s / 715.72 ms |
| Final mixed catch-up | 5.420 s | 2.408 s |
| Sampled read-only / mixed RSS | 5.827 / 5.496 GB | 5.984 / 4.764 GB |
| Demand / physical-footprint ledger peak | 1.381 / 1.300 GB | 1.400 / 1.400 GB |
| Allocated disk after restart | 3.810 GB | 3.775 GB |
| Post-restart mean leaf scoring | 5.723 ms | 6.975 ms |

Readiness regresses in both pairs (386.48 -> 409.01 s; 309.01 -> 355.88 s),
as do C30 QPS (501.98 -> 376.17; 685.11 -> 496.27) and C30 p95
(125.62 -> 131.48 ms; 95.25 -> 103.90 ms). Mixed query p95 improves in both
pairs (166.71 -> 106.67 ms; 73.98 -> 56.92 ms), but mixed RSS reverses
direction (+7.1%, then -28.1%). Do not present the median mixed-RSS reduction
as a repeatable memory win. Settled allocated disk also reverses direction
(-3.0%, then +1.1%); the slightly smaller median does not prove less I/O.

Checkpoint amplification is substantially worse in both orders. Through source
sequence 10001, controls publish 14/14 generations and write 2.048/2.055 GB of
checkpoint bytes; candidates publish 60/55 generations and write 8.259/7.773 GB.
Each candidate performs six full checkpoints versus two per control (counts
exclude the initial empty-authority publication from the full-checkpoint count).
That is 4.03x/3.78x checkpoint output, despite similar post-restart retained disk.

Replay logs retain the same delete-to-append cost transfer at scale. In pair 1,
control/candidate delete stages total 27.51/7.86 s and embedding-apply stages
4.22/20.76 s; pair 2 gives 24.21/3.79 s and 4.34/24.04 s. These windows coalesce
overwrites: 21,500/21,700 public rows become 9,200/15,100 replayed documents in
pair 1, and 30,100 public rows per arm become 20,400/25,900 replayed documents
in pair 2. Per-replayed-document timers are not per-public-write timers, and
neither should be substituted for the measured mixed throughput/latency.

Code review identifies the next structural targets, not yet fixed by this A/B:

- AFRC currently embeds an AFQD header/origin for every append chunk. A
  768-dimensional float32 origin alone is 3,072 bytes (6,144 at 1,536 dimensions),
  disproportionate to a small RaBitQ append. Share immutable origin metadata;
  batch transaction-local rows without duplicating authoritative embeddings.
- Row-debt age/pending state is index-wide and is cleared only when the worker's
  mutation epoch still equals the writer's. Continuous ingest can keep the old
  debt signal active. Forced repacking then treats all fragmented leaves as aged.
  Track and retire repaired debt by leaf/revision, with bounded work admission.
- A newer absolute AFRM WAL-tail value can shadow a worker's compacted manifest
  and reference the original chunks. Retaining that reference closure is required
  for correctness, but does not preserve compaction progress. Publication needs
  a revision-aware composition of the compacted base with newer row operations,
  prepared off-lane and validated before the durable coherent generation swap.
  Never reclaim the old closure or discard newer updates to improve a timer.

The integrated row store is functional and recovery-tested, but is **not** an
overall performance winner or the final low-amplification production design.

##### Shared scoring-origin query preparation follow-up

The integrated scorer grouped tombstone gaps within each chunk but still
normalized and quantized the query again for every chunk. It now prepares once
per leaf scoring origin and borrows the scratch planes across all chunks.
Prepared contexts validate quantizer/origin compatibility and a nonwrapping
scratch epoch; preparing another query (including a zero-diff query) invalidates
the old context. This adds no heap buffers or changes to persisted formats,
candidate order, score arithmetic, error bounds or authoritative completion.
The existing filtered/range-serving fallback is unchanged.

Implemented in `027c58ebf`. Validation: all 226 standalone
vector-kernel/vectorindex Debug tests passed
(three skips), plus the integrated native mutation/checkpoint/reopen fixture
across all three metrics without leaks. Tests assert exactly one preparation
for a fragmented leaf, bitwise row-score/bound/order parity after repacking,
empty/invalid range behavior, cancellation, wrong origins/quantizers, stale
scratch and epoch exhaustion.
The quantizer tests also cross-compile for amd64/arm64 Linux; the database
overwrite/reopen and 10,000-document replay checks pass with the row flag on.
The same-binary ReleaseFast synthetic 1,024-row,
768-dimensional kernel A/B uses reversed order over four repetitions:

| Chunks per leaf | Repeated preparation, median | Shared preparation, median |
| --- | ---: | ---: |
| 1 | 8.14 us | 8.11 us |
| 16 | 11.49 us | 9.38 us |
| 64 | 21.62 us | 13.03 us |

Raw output: `.benchmark-results/pr593-shared-row-query-20260908/kernel.log`.
These are component timings, not public-query improvements. The completed
50K/1M integration A/B deliberately retains the original `0326a2bd9` executable
and does **not** include this follow-up. The broader row experiment stays
default-off; this change alone does not address append-stage work, redundant
origin bytes or repeated checkpoint publication.

#### Main reconciliation before the row-store maintenance follow-ups (2026-09-08)

Checkpointed all local source, schema, script and findings work in `2ba91d732`;
runtime database directories and lock files remain untracked. Reconciled main
at `3d3b6ed0a` before starting the shared-origin/per-leaf-debt/rebase work.

The merge exposed a wire identity collision: main's runtime-status V16 carries
inference diagnostics, whereas this branch had assigned V16 to framed native
index records. Main's V16 remains positional and readable; native framing and
capabilities now use V17. Negotiation explicitly supports V12/V15/V16/V17,
preserves native readiness fences, and rejects native-to-V16 downgrades.
Pre-merge experimental metadata using this branch's different V16 is not an
upgrade fixture: retain its frozen executable/data and use fresh qualification
directories for the merged candidate.

Other reconciliation points: keep main's hardware-dispatched checksum library,
preserve owned status publication through the newer targeted-index cache,
retain deep ownership of cached index errors, and use main's atomic dense
upsert without a separate replacement pre-delete. Existing cold-status tests
now model background recovery separately from cache-only HTTP observation.

Debug validation: 91 status/protocol/publication tests, native row mutation /
checkpoint / reopen across three metrics, and 10,000-document streaming replay
and reopen with row deltas enabled all pass without leaks. The earlier merged
overwrite/reopen pair also passed. Hash/vectorindex libraries passed 201 tests
with four skips; checksum tests cross-compile with LLVM for amd64 and arm64
Linux. Regenerated public schema/Zig APIs and Go SDK; Go `oapi` tests pass.
These are correctness checks, not a new 50K/1M performance qualification.

Remaining experiments, still default-off: shared immutable scoring origins,
revision-scoped per-leaf maintenance debt, and durable compaction progress
across newer source updates. Do not infer QPS recovery from reduced maintenance
bytes; qualify against the preserved matched control across all metrics.

#### Shared immutable scoring origins (2026-09-08; unqualified experiment)

AFRC V2 row chunks now reference a checksum-bound AFRO origin object containing
the centroid, metric and norm. Appends and repacks retain that object instead
of embedding another complete AFQD directory/centroid in every chunk. The
source WAL captures origins and chunks together; full checkpoints retain the
reference closure. Query chunks own independent origin/backing leases, not a
reference cycle through their generation's row cache. Missing or mismatched
origins fail closed.

The 768-dimensional, 64 single-row chunk fixture uses less than one eighth of
the previous encoded bytes (including the one shared origin). This is a format
sizing test, not an ingestion or QPS result. Debug vector-kernel/vectorindex
tests passed (227 passed, three skipped), as did native mutation/checkpoint/
reopen across L2, cosine and inner product, including missing-origin rejection.

The row experiment remains default-off. AFRC V1 was experimental, not released;
its frozen baseline executable and data stay together. Use fresh data for this
candidate; do not open old experimental row files with the new executable.
Per-leaf maintenance debt and durable progress across newer manifests remain
separate follow-ups. Main's release-script follow-up at `167d2bd27` was merged
after the larger reconciliation above.

#### Revision-scoped debt and durable row-reference translation (2026-09-08)

Implemented behind the existing default-off posting-row experiment, on top of
shared origins (`d260570b4`). These changes do not establish a performance win.

- AFRM V2 manifests persist physical-work statistics and their first outstanding
  maintenance revision. Immutable generation-local observations replace the
  index-wide mutation epoch, repack flag and shared age clock. Aborted captures
  cannot change committed debt. Reader staging preserves ages for unchanged
  leaf/origin/debt identities; full checkpoints no longer age every leaf as due.
- Delta maintenance selects the oldest due leaves, bounded to 64 leaves and
  64 MiB of described input per pass. An individual leaf can make progress on
  its own. Worker preparation validates each selected identity/revision. A
  conservative cached deadline may trigger an extra preparation after a debt
  disappears, but does not clear or postpone another leaf's debt.
- AFRR redirects bind original chunk identities/checksums and ordinal ranges
  to a compact chunk. They are written in the same checkpoint publication as
  that chunk. A newer source WAL manifest therefore resolves to compacted rows
  after CURRENT publication and recovery, without copying/replaying that WAL
  or rewriting the source manifest on the writer lane. Full checkpoints retain
  the required small redirect closure instead of the old scoring payload.
  Delta files still retain their older physical segments until consolidation;
  this does not introduce independent per-chunk file reclamation.
- Old query views keep independent backing/origin leases. New views preserve
  row order, source coverage and exact revision identity, including replacement
  of a vector with the same ID. Missing dependencies, stale rows and malformed
  mappings fail closed; redirect traversal is bounded. A compact base plus a
  previously admitted source tail may temporarily retain up to twice the
  64-MiB per-leaf physical threshold. Reads accept this bounded overlap; new
  mutations retain the strict admission limit until maintenance drains it.
- Whole-old-chunk deletion needs special treatment: a source manifest that
  previously looked physically clean can become a subset of the compact base.
  Small layout descriptors recover/schedule this debt without scanning scoring
  pages. Worker normalization writes the effective physical references even
  when another repack is unnecessary.

Review also found that the optional nonquantized split-prefix path could feed
AFRM into the protobuf decoder. It now dispatches that format before decoding.
The protobuf tag reader separately returns Overflow for an out-of-range field
number rather than trapping on its checked cast.

Validation: the native integration fixture covers ten successive repack/update
races per metric, delta-chain consolidation, old leases, deletes/replacements,
reopen and flag-off readability. It asserts that published newer views actually
retain the compact chunk (not merely correct search results). Component tests
cover shared origins, metadata debt, stale/missing/corrupt redirects, whole-chunk
deletion, score/order/bound parity and allocation failures. The vector libraries
passed 228 tests with three skips; protobuf wire tests passed nine tests. Native
row component tests cross-compile for amd64 and arm64 Linux. The final 10K Debug
replay/reopen check passed without leaks; it is not a load-time result.

AFRM V1, like AFRC V1, was an unreleased experiment. Keep frozen baseline data
with its frozen binary; qualification of this format must use fresh roots.
The preserved matched 50K/1M control remains the comparison target. No new public
50K/1M latency, QPS, RSS, footprint, disk or recall result is claimed here.
Concurrent source-vector edits are being preserved separately from this commit.

#### Shared allocation-free admission handoff (PR review follow-up)

The bandwidth queue published `admitted` before its final event/Io accesses.
A polling caller could return and retire its stack waiter while the publisher
still touched it. Both bandwidth and rerank admission now share an intrusive
FIFO and handoff primitive: detach and charge under the policy lock, signal
the event, then transfer lifetime with the final release store. Cancellation
rejoins that same lock and either removes a pending waiter or returns an
already-granted permit exactly once. Byte weights, helper limits and FIFO
fairness remain policy-owned; the fast path creates no waiter or allocation.

Debug validation: 72 resource/admission tests and five HBC admission integration
tests passed. Twenty repetitions of the ten focused queue/lifetime/cancellation
tests passed without leaks. The tests include immediate waiter destruction
while its publisher is still returning, token cancellation racing grant, and
`std.Io` cancellation of a queued stack waiter. Test registration audit and
repository formatting checks passed.

A local ReleaseFast rerank-admission microbenchmark compared the pre-change
queue at c8a91ad9e with the shared implementation, using the same harness.
Five paired runs with 12 callers, capacity four and a fixed 200-spin-hint
critical section (120,000 acquisitions/run) had median times of 4.729 us/op
before and 4.760 us/op after. Uncontended acquire/release was approximately
4.4--5.0 ns/op across the two harness variants. The earlier empty-critical-
section contention sample was scheduling-sensitive and is not a throughput
result. These measurements check abstraction overhead, not public query QPS;
no new 50K/1M readiness, latency, RSS, disk or recall qualification is claimed.


#### Post-merge ownership comparison: structural observation repair

The fresh comparison in `.benchmark-results/vector-store-promotion-20260913/`
passed four 50K timed and reclamation arms after automatic source maintenance
was restored in the compiled owner. Vector-store peak QPS was 5.4% below LSM
in AB order and 6.5% above in BA order; this is no consistent small-table win.
The first 1M LSM control then logged batch and query HTTP 500s. The runner
rejected it even though the client exited zero, and no 1M vector-store arm
started. These failed timings cannot support an ownership comparison.

Two connected defects are being qualified in
`.benchmark-results/vector-store-boundary-repair-20260913/`. StorageBusy had
no stable callback error detail and therefore became RuntimeBoundaryFailure.
The compiled structural reconciliation path also returned completion without
adding an owner observation. Targeted publication rejected the empty proof with
EmptyTargetedIndexObservation, repeatedly scheduling reconciliation that requested
exclusive owner access. The repair captures status before releasing the same
owner generation, carries it through targeted publication, and leaves a group
pending when no status proof is available. Existing table epoch, physical root
and target incarnation fences remain enforced. Transient startup inspection
still retires idle cold owners; ongoing structural work retains its owner.

StorageBusy now retains its retryable identity through callbacks and maps to
HTTP 503 in batch/query handlers. Ambiguous write outcomes remain conflicts;
the implementation does not retry commits automatically. The comparison still
rejects request errors and retries, so changing the HTTP status alone cannot
qualify an arm. Focused regressions, saved restart/maintenance gates and fresh
same-binary timings must pass before reconsidering the default.

The focused validation passes 18 callback/error-transport checks, three HTTP
and compiled write-callback checks, and the compiled-control structural
publication regression, with no leaks. The latter binds catalog authority,
verifies that an empty observation is still rejected, and publishes the captured
owner proof through the targeted path. Saved recovery and fresh timings remain
pending for this revision.


#### Post-merge comparison: periodic owner admission

The committed observation repair (`8337942cb1`) passed three saved vector-store
restart/automatic-GC checks and two saved LSM restart/concurrent-query checks.
All four fresh 50K arms passed their workloads and reclamation gates. In AB/BA
order, vector-store readiness improved 4.6%/7.8%, and total allocated disk after
restart fell 45.5%/43.0%. Peak QPS changed -12.4%/+11.6%; mixed-workload QPS
changed -8.8%/-3.8%. These controls share explicit ANN settings; ownership is
the table-level treatment.

The first fresh 1M LSM arm reached readiness in 318.5 seconds and completed the
read-only query windows, but a query returned HTTP 503 during mixed updates.
No 1M vector-store arm started. The empty targeted-observation loop and error
identity collapse were absent. A separate unsampled restart reproduction also
failed with query 503s. Evidence is preserved in
`.benchmark-results/vector-store-boundary-repair-20260913/` and
`.benchmark-results/vector-store-owner-admission-20260914/`.

Periodic startup inspection used the same waiting exclusive owner admission as
structural changes. Waiting behind an existing lease installed an exclusive
pending flag, which blocked new foreground leases; a long derived-index apply
could then exhaust their five-second admission deadline. A diagnostic sample
captured catch-up waiting for owner admission and maintenance waiting for the DB
apply lock. Sampling was intrusive and supplies stack evidence only; the fresh
failure and separate unsampled reproduction establish the request failure.

Periodic inspection now attempts exclusive admission only when idle, yielding
without installing a pending writer gate. Structural changes and explicit repair
retain their waiting admission. The compiled catch-up path also uses the existing
group admission/deferred-key protocol: a busy owner preserves an exact retry,
and a new attempt changes its generation so stale scheduler cleanup cannot
remove new debt. Owner/root/catalog identity checks and observation publication
fences remain enforced. Focused tests cover foreground admission, structural
writer preference, deferred retry retention and eventual completion. All six owner-source suite checks pass with no leaks. Saved-workload recovery
and fresh same-binary comparison qualification are pending; this change does not promote vector-store ownership to the default.


#### Completed fresh ownership comparison after admission repair

The pinned `5a90e99d6e` binary passed all eight fresh arms: 50K and 1M,
LSM/vector-store and vector-store/LSM order. Every workload, strict request-error
gate, restart and reclamation check passed. Before timing, six owner-source tests,
two unsampled 60-second saved mixed workloads across restart, three vector-store
restart/automatic-GC checks and two LSM restart/concurrent-query checks passed.
The saved mixed runs completed 25,935 queries and 424,300 written rows without
request errors. This fixes the reproduced foreground admission failure; changing
HTTP error classification alone was not accepted as qualification.

Both modes use the same binary and explicit common ANN settings, float32 serving
encoding, durability, batch size, concurrency and recall policy. The tables are
fresh, single-shard standalone tables on the available host. The 50K dataset is
OpenAI 1536D; the 1M dataset is Cohere 768D. Their absolute QPS is not comparable
as a size-only scaling result. Two runs per mode provide paired evidence, not a
confidence interval. Full results and receipts are in
[the comparison report](../../../../.benchmark-results/vector-store-owner-admission-20260914/RESULTS.md).

| 1M pair/order | Ownership | Readiness s | Peak QPS | Mixed QPS | Mixed write rows/s | Mixed query p99 ms |
|---|---|---:|---:|---:|---:|---:|
| 1 / AB | primary_lsm | 273.25 | 1144.2 | 211.29 | 4031.5 | 104.30 |
| 1 / AB | vector_store | 247.44 | 1210.3 | 232.80 | 3977.6 | 95.38 |
| 2 / BA | vector_store | 265.38 | 1225.6 | 225.02 | 3948.8 | 100.64 |
| 2 / BA | primary_lsm | 264.22 | 1141.9 | 189.00 | 3674.9 | 113.90 |

At 1M, vector-store peak QPS improves 5.8%/7.3%, mixed query QPS improves
10.2%/19.1%, and mixed query p99 improves 8.6%/11.6%. Readiness improves 9.4%
in the first pair and is essentially equal in the second (+0.4%). Recall stays
at 0.9901–0.9904. Total allocated disk after restart, including journals, falls
43.4% in both pairs: LSM uses 6.74–6.77 GiB and vector-store 3.82–3.84 GiB.
Maximum observed server RSS falls about 28% in both pairs, from 9.35/10.06 GiB
with LSM to 6.71/7.25 GiB with vector-store. These RSS figures use the continuous
resource sampler. The separate footprint demand metric has one sample per arm
and must not be presented as an ingestion peak. Kernel-reported lifetime
footprint is 5.18/4.76 GiB for LSM and 1.13/1.15 GiB for vector-store.

The remaining tradeoffs are measurable. At 50K, vector-store uses 47–49% less
disk, but peak QPS changes +5.8%/-22.5% and mixed query QPS changes -4.1%/-1.3%.
At 1M, fixed-count update/delete churn is 4.4%/15.5% slower. Reclamation settles
in 149/155 seconds with vector-store versus about 33 seconds with LSM, including
the gate's stability observation period. Each vector-store collection reads
3,072,000,000 bytes and writes 3,192,012,800 bytes, marks 29,080,108 rows across
its passes, and finishes with exactly 1M retained payloads and zero unreferenced
payload bytes at the final collection. Completion proves reclamation works;
it does not remove the full-rewrite cost. The controlled enrichment workload
also remains a tradeoff: updated embedding readiness is 1.12/1.90 seconds for
vector-store versus 1.00/1.04 seconds for LSM in the 1M arms.

The post-mixed 50K profiles identify a concrete follow-up: the two vector-store
runs perform almost identical scoring work, but resolve 0.225 versus 45.939
metadata entries per query. Average rerank vector loading rises from 0.245 to
1.496 ms while leaf scoring remains about 0.978 ms. These profiles run after
mixed updates and do not prove the cause of the earlier read-only peak-QPS
difference. Trace why the resolved fast path misses while preserving exact
generation/identity validation, then qualify any change against the saved data.
For GC, isolate copy/mark work and scheduling delays before selecting a different
policy. The large-table result favors vector-store, but the small-table lookup
variability and reclamation cost remain targets before blanket default promotion.
`primary_lsm` remains the creation default.
