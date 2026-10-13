# VectorDBBench Findings: Prepared Publication, Recovery, and Subgroup Routing Experiments (2026-09-05 – 2026-09-07)

> Relocated verbatim from `zig/VECTORDBBENCH_FINDINGS.md` (lines 5421–8518 at commit 3ce5aff4ea) on 2026-10-12. Durable decisions and the experiment-flag inventory were folded into [VECTORDBBENCH_FINDINGS.md](../../../../zig/VECTORDBBENCH_FINDINGS.md) before the move.
>
> Relative links were re-based to this file's location. Paths under `.benchmark-results/` refer to untracked local benchmark output at the repository root; `/private/tmp/...` paths name ephemeral local run directories that may no longer exist.

## 2026-09-05: prepared posting publication and capture-scoped value ownership

The failed 1M activation exposed two coupled issues: CURRENT was published
before the new reader could be admitted, and recovery reconstructed patch
bases through the generation-pinning query-cache accessor. The repair:

- prepares the next posting manifest/WAL and opens/validates readers before
  changing CURRENT, for both background and explicit HBC checkpoints;
- rejects a stale prepared WAL byte boundary, including same-sequence
  maintenance; ambiguous control-file durability still poisons the writer;
- preallocates the serving publication and keeps old-generation destruction
  outside the query publication fence;
- owns recovery, scan-admission, routing reconstruction, and patch-encoding
  temporaries per operation instead of pinning them in the serving cache;
- gives native write captures explicit value leases, released before their
  generation lease at commit/abort, so decoded patches remain reclaimable;
- reclaims only the exact obsolete generation names after publication.

This does not implement sealed WAL extents or move WAL-tail copying out of
the per-index writer lane. Nor does it restore whole-shard contiguous planes
in the streamed vector writer. Those remain separate performance work; the
checkpoint fix must not be described as completing those designs.

A query experiment replaces one std.Io task per residual read and an
eight-read barrier with at most eight workers sharing one batch queue. The
caller participates, unavailable concurrent lanes fall back to caller work,
and cancellation drains workers before releasing request memory. Physical
read concurrency, authoritative float32 completion, and payload scratch
remain unchanged. Performance qualification is pending.

The harness now validates every requested concurrency and all four parallel
measurement arrays, including finite positive QPS/latency. A NORMAL partial
result can no longer pass solely on its serial recall. The raw r126 live
result is rejected (missing c30); the complete scratchpool result is accepted.

Debug validation before performance qualification:

- 280 storage tests passed, zero failures/leaks; the preceding 277/278 run
  exposed an obsolete test assertion requiring an optional routing directory
  during exhaustive full-effort search. The test now permits the no-directory
  path while retaining full-corpus coverage and repeated-query assertions.
- Seven final capture/cache/checkpoint tests passed after the last ownership
  change, including a capture whose cache entry is evicted while leased.
- Prepared-reader allocation failure and incompatible HBC metadata leave
  CURRENT unchanged and the next write serviceable. Same-sequence tail and
  ambiguous-CURRENT tests pass.
- Six Python validator tests and shell syntax checking pass.

Logs: `/private/tmp/pr593-final-prepared-storage-debug-20260905.log`,
`/private/tmp/pr593-capture-value-lease-debug-20260905.log`, and
`/private/tmp/pr593-bounded-read-workers-debug-20260905.log`.

### Completed prepared-leases 50K measurement

`/private/tmp/vdbbench-pr593-prepared-leases-50k-20260905/` used binary
`557fe6941169331ced56a7fcb5f933d234aac07009723c71ce8db9c788f366ad`.
Ready was 34.2599 s (10.1902 insert + 24.0697 catch-up), recall .9864.
QPS c1/10/20/30 was 296.32/1655.78/1823.19/2011.01, with p95
3.712/9.438/29.584/51.506 ms. Load/read-only RSS peaked at 1.8040 GB.
Mixed work reached 132.07 query QPS and 1516.11 update rows/s, with query
p95 152.93 ms, write p95 527.56 ms, 1.889-s final catch-up, .97462 recall,
and 3.3319-GB phase RSS. Mixed requests reported no errors. Startup did log
a zero-progress quarantine while native publication was outstanding, so this
is a performance observation, not a clean lifecycle qualification. Neither
the read-only gain nor faster mixed writes establishes an all-metrics win.

### Sealed posting WAL, read-task governance and intra-wave proof experiment

Implementation underway; no performance result is attributed to it yet.
The preceding executable is preserved at
`/private/tmp/pr593-before-sealed-extents-antfly-20260905`.

- Online publication now reports admission contention separately from a
  successfully attempted empty pass. Startup retains readiness debt but does
  not charge a contended non-attempt to its zero-progress quarantine counter.
  Source-tip stabilization and an active checkpoint builder likewise report
  explicit deferral. Actual admitted non-progress retains the original bounded
  backoff; deferral does not assert readiness or count as published progress.
- Posting CURRENT V4 can reference bounded sealed WAL extents. Rotation fsyncs
  the committed old append target and publishes the new target before use.
  Checkpoints covering an exact sealed prefix retain later WAL file identities
  instead of rereading the covered prefix and copying the active tail. Older
  retained source views use the validated byte-prefix fallback. Recovery
  validates each sealed extent and only truncates an incomplete active tail.
  Backup includes sealed files and preserves the base sequence separately from
  the WAL tip. Reclamation excludes extents retained by the next manifest.
- Extra std.Io read workers have a node-wide ResourceManager limit, defaulting
  to twice logical CPUs and independently configurable from memory/bandwidth
  admission. Optional worker admission never waits: the already-admitted query
  caller continues draining its queue. Completion, failed task submission and
  cancellation release permits; stats expose active/peak/denied tasks.
- Unfiltered native scoring uses leased member IDs and implicit contiguous row
  positions, avoiding two scratch-array writes per approximate candidate.
  Native leaf scoring grows scalar score scratch, not the float32 fetch matrix.
- The raw historical 1M profile reported zero bound resolutions/fallbacks/stops
  and exactly one traversal wave: the default initial wave reached the effort
  cap before evaluating its proof. Bounds are now also checked every 256 probes
  within that wave. Maximum effort and exact completion are unchanged; only a
  strict conservative suffix proof can stop earlier. The bounded routing heap
  also retains an O(1) minimum-bound/resolution summary of rejected and evicted
  leaves. Its suffix proof covers the entire unseen directory without allocating
  a full-directory candidate array. An incomplete directory or any unresolved
  omitted leaf disables the proof.

Additional Debug checks cover a backup captured after WAL sealing but before
checkpoint publication, retaining multiple newer sealed extents while reclaiming
an older prefix, and exhaustive comparison of bounded-heap suffix summaries
against all unseen leaves (including an unresolved omitted leaf). The final
serving suite passed 19 tests and the explicitly exported proof/scratch suite
passed four. Seven Python validator tests pass. The qualification harness now
rejects known native lifecycle failures in server logs even when the client
eventually reports a complete successful query curve.

Focused Debug checks: three startup/publication tests, 18 store/read-worker
tests, five routing/checkpoint/worker tests, and three vector-library codec/
projection tests passed without failures or leaks. The store fault-injection
test intentionally logs an ambiguous CURRENT durability error. Logs:
`/private/tmp/pr593-native-contention-api-debug-20260905.log`,
`/private/tmp/pr593-native-contention-db-debug-20260905.log`,
`/private/tmp/pr593-governed-sealed-debug-20260905.log`, and
`/private/tmp/pr593-routing-sealed-debug-20260905.log`.

Remaining design work is explicit: reusable physical posting/scan chunks,
streamed replay-reader activation outside the writer lane, sealed shared-vector
WAL handoff, better-balanced routing experiments, and fully fused candidate
scoring. Sealed posting files alone do not implement those pieces. Fresh 50K
and 1M load/query/mixed/restart qualification remains required.

The first fresh sealed/proof 50K attempt
(`/private/tmp/vdbbench-pr593-sealed-proof-50k-20260905`, binary
`2857ef7c1bce21a2cdf791b7a3e5e668d70c4424fc856ba10e89c710ebaaacd5`)
was stopped after the same startup quarantine. Insert completed in 9.99 s,
but this is not a qualified load/query result. Publication contention alone
was not the complete cause: the initial startup audit excluded resident-owned
derived replay, while the final audit unconditionally returned non-progress
for that same lag, before preserving the native-publication deferral result.
The live log showed posting coverage advancing from sequence 126 to 251 across
the failed quanta, then completing the full native base at sequence 501.
Initial, final and broad-debt audits now share one replay-ownership predicate;
isolated owners still see the lag as startup work. Broad/repair handoffs also
preserve native deferral. The three focused Debug tests pass, including the
existing live-repair/status concurrency regression
(`/private/tmp/pr593-resident-ownership-debug-20260905.log`). All 17 posting
store tests also pass, including an ambiguous WAL-seal publication followed by
reopen and a successful append; its injected durability error is intentional
(`/private/tmp/pr593-final-sealed-store-debug-20260905.log`). Fresh performance
requalification is pending. Connection-refused errors after the intentional
stop are shutdown artifacts, not additional unexplained server failures.

### Corrected sealed-WAL/read-governor 50K qualification

`/private/tmp/vdbbench-pr593-sealed-proof-50k-20260905-b/` completed the fresh
public API load, c1/10/20/30 curve, concurrent updates/queries, and cold/warm
restart checks without the lifecycle errors above or mixed request errors.
Binary: `1958cdf8a369e3d3ec8fa9b83a607ca5a10d298b792ffc7be982b013d2b8d69b`.
Case/batch/workers/encoding remain 1536D50K/100/4/float16, with authoritative
completion and unchanged search effort.

| Metric | Preceding prepared-leases 50K | Corrected sealed/governed 50K |
| --- | ---: | ---: |
| Ready, insert + catch-up | 34.2599 s | 33.0744 s (10.9903 + 22.0841) |
| Fresh serial recall | 98.64% | 98.66% |
| QPS c1 / c10 / c20 / c30 | 296.32 / 1655.78 / 1823.19 / 2011.01 | 269.42 / 1880.80 / 2094.45 / 2026.93 |
| p95 ms c1 / c10 / c20 / c30 | 3.712 / 9.438 / 29.584 / 51.506 | 4.231 / 6.799 / 24.833 / 59.746 |
| Load/read-only phase peak RSS | 1.8040 GB | 1.5754 GB |
| Mixed phase peak RSS | 3.3319 GB | 2.3565 GB |
| Restart peak RSS | 0.5518 GB | 0.4064 GB |
| Mixed query QPS / p95 | 132.07 / 152.93 ms | 156.59 / 95.96 ms |
| Mixed update rows/s / p95 | 1516.11 / 527.56 ms | 1677.27 / 462.59 ms |
| Mixed recall / final catch-up | 97.462% / 1.889 s | 97.476% / 1.999 s |

This is not an all-metrics win: read-only c1 and c30 p95 worsened, even though
medium-concurrency throughput, mixed latency, and phase RSS improved. The
preceding sample also had the lifecycle error, so it is a performance
comparison, not a clean release baseline. Fresh native topology differs across
concurrent-load samples; these measurements do not isolate each change's cause.

After mixed updates and restart, the detailed 1000-query profile reported
24,596.9 approximate and 141.3 authoritative exact vectors/query, 98.30% recall,
and 5.477-ms mean / 6.330-ms p95 HTTP latency. It used multi-wave routing
(12.996 waves/query), with bound fallbacks and no certified stops. This is not
a profile of the earlier read-only concurrency curve, and must not be used to
claim that clean-generation flat bounds have already reduced candidate work.
The same binary's fresh 1M qualification at
`/private/tmp/vdbbench-pr593-sealed-proof-1m-20260905/` was subsequently stopped
as unqualified: it stalled during insertion around 262,500 indexed rows and
source target sequence 5104, while building the first delta at sequence 1876.
Health remained responsive; stack sampling and FD metrics identified a primary
LSM compaction descriptor-admission cycle, not slow HBC scoring. There were 967
admitted descriptors, eight persistent descriptors, zero cached idle entries,
and four admission waiters in a 1024-descriptor pool. One compactor held its
input cursors while waiting for output-directory creation; another waited for
input metadata. Derived replay waited for a primary point read and foreground
writes waited behind primary maintenance. The HBC builder was yielding behind
foreground pressure. Evidence:
`/private/tmp/pr593-sealed-proof-1m-stall-20260905.sample` and the run's
`metrics-live.prom`. Incomplete-run memory is not a qualified 1M RSS result.

The follow-up changes persisted-run compaction inputs to window-scoped private
cold readers. Immutable input names remain pinned by the existing run snapshot;
an input descriptor is closed before the cursor performs another admission or
output work. This preserves native cold-cache policy without allocating a
descriptor per run or treating a per-compaction capacity estimate as a node-wide
reservation. Positional reads into caller buffers also avoid the generic
temporary allocation/copy. A tiny four-descriptor-pool test retains 32 logical
cursors while producing output, verifies reads and error cleanup, and confirms
input permits return after every window. Stable WAL capture leases intentionally
keep their stronger inode-bound contract. Platforms without private cold readers
retain the ordinary positional fallback. This follow-up still needs fresh
performance qualification; it must not inherit the preceding binary's results.

The four focused Debug checks pass (windowed native reads with four slots,
streamed compaction, bounded output segmentation, and retained run snapshots):
`/private/tmp/pr593-windowed-four-slot-debug-20260905.log`.
Fresh retry `/private/tmp/vdbbench-pr593-sealed-proof-1m-20260905-b/` uses binary
`56eedf11aa2a21c25d1d80f59e7e798e864730008639f8a991c3878a7b876527`.
Its first delta published at sequence 1876 with an 81,550,984-byte concurrent
WAL tail, then indexed beyond the previous 262,500-row stall. The run remains
in progress, not qualified. Other worktrees were building/testing concurrently;
wall-clock results must retain that contention caveat.

#### Completed windowed-reader/sealed-WAL 1M qualification

The retry above completed fresh public load, all four read-only concurrency
stages, mixed updates/queries, and cold/warm restart with no lifecycle or
request errors. The binary is the `56eedf11...6527` SHA recorded above;
batch=100, load workers=4, cosine/k=100, unchanged default effort, and float16
candidate projection with authoritative completion. This is a completed
correctness/workload qualification, **not an all-metrics performance win**.

| Metric | Windowed-reader/sealed-WAL 1M |
| --- | ---: |
| Ready: insert + catch-up | 743.2529 s: 305.5210 + 437.7319 |
| Fresh recall | 99.25% |
| QPS c1 / c10 / c20 / c30 | 15.9056 / 125.9969 / 855.6047 / 890.5581 |
| p95 ms c1 / c10 / c20 / c30 | 27.908 / 121.681 / 53.789 / 82.506 |
| Load/read-only peak RSS | 7.5848 GB |
| Mixed peak RSS | 7.6647 GB |
| Restart sampled peak RSS | 1.8441 GB |
| Mixed query QPS / p95 | 242.75 / 58.397 ms |
| Mixed update rows/s / write p95 | 2333.70 / 371.271 ms |
| Mixed recall / final catch-up | 99.242% / 116.835 s |
| Cold / warm restart recall | 99.07% / 99.10% |
| Cold / warm restart serial p95 | 29.6 / 67.0 ms |

The earlier regression-recovery run achieved 828.00 C30 QPS / 94.317 ms p95
with 617.3662 s readiness and 7.0534 GB live RSS. The governor/cold run achieved
649.57 C30 QPS / 87.476 ms with 724.2544 s readiness and 6.2579 GB live RSS.
This candidate improves C30 in this sample but loses the readiness/RSS tradeoff.
Do not combine its best throughput with either older run's memory figure.
Other worktrees were active; the severely asymmetric c1/c10 versus c20/c30
curve and the slow restarted tail require controlled follow-up. Contention is
a caveat, not an established explanation that excuses those results.

The detailed profile was collected **after mixed updates and restart**, not
during the clean read-only curve: 238,933.4 approximate / 148.5 exact vectors,
2048 leaves, 5.670 ms mean leaf scoring and 0.476 ms exact-vector loading.
HTTP mean/p95 was 26.515/59.158 ms; server mean/p95 24.848/58.125 ms. All seven
intra-wave proof checks fell back on every query, with zero certified stops;
candidate work therefore did not fall. This does not justify lowering effort.

Stable-tip logs also expose no-op amplification: generations 12 through 18
each published a 101-byte delta at sequence 10001 with zero WAL prefix,
followed by a 1,828,993,038-byte full generation 19. Generation 11 had already
written a 1,729,333,369-byte delta at the same source tip. The empty deltas are
not useful acceleration progress. Their dependency/readiness classification
needs tracing; simply increasing the delta-chain limit is not a solution.

Still unimplemented: reusable physical checkpoint chunks and their durable/
leased reference reclamation, zero-replay reader handoff, sealed shared-vector
WAL tails, tighter/better-balanced routing, and a fully fused scoring/selection
kernel. The changes and completed runs above must not be described as that
whole design being finished or its memory/disk goals being achieved.

#### Matching windowed-reader/sealed-WAL 50K qualification

`/private/tmp/vdbbench-pr593-windowed-sealed-50k-20260905/` completed the same
fresh/mixed/restart gates on the identical `56eedf11...6527` binary. It reported
no lifecycle or request errors. The case is 1536D50K; all other query/load
settings match the 50K qualifications above.

| Metric | Matching 50K |
| --- | ---: |
| Ready: insert + catch-up | 32.5095 s: 10.4432 + 22.0663 |
| Fresh recall | 98.64% |
| QPS c1 / c10 / c20 / c30 | 277.8048 / 2125.0280 / 2201.3878 / 2170.6132 |
| p95 ms c1 / c10 / c20 / c30 | 3.505 / 5.261 / 21.953 / 54.418 |
| Load/read-only / mixed peak RSS | 1.7688 / 2.4111 GB |
| Restart sampled peak RSS | 0.3255 GB |
| Mixed query QPS / p95 | 117.21 / 170.068 ms |
| Mixed update rows/s / write p95 | 1927.50 / 316.691 ms |
| Mixed recall / final catch-up | 97.609% / 2.604 s |
| Cold / warm restart recall and serial p95 | 98.30%, 5.7 / 5.7 ms |

Read-only throughput and p95 improved versus the preceding sealed/governed
50K sample, but mixed query latency and RSS regressed while write throughput
improved. This again is not an across-the-board win. After mixed/restart,
the detailed profile measured 24,352.3 approximate / 141.0 exact vectors,
6.186-ms mean / 6.489-ms p95 HTTP latency, and 98.303% recall.
Shutdown filesystem allocation (`du -sk data`) was 838,012 KiB for 50K and
8,118,652 KiB for the matching 1M run. These are post-mixed durable allocations,
not peak transient disk usage or logical table-size counters.

After these measurements, a diagnostic-only change adds per-delta counts of
source values, changed leaves, emitted scan rows, and dirty/quantized/projection
deferrals to checkpoint logs. Those logs are **not present in the measured
binary**. The focused Debug suite passed three tests, including zero-new-source
acceleration with unavailable projections becoming available without changing
coverage (`/private/tmp/pr593-delta-progress-debug-20260905.log`). The counters
do not bypass readiness or resolve the observed empty-delta cycle themselves.
The seven harness validation tests also pass; no full CI-suite claim is made.

### September 6: retained-state handoff and fused candidate experiment

The background posting checkpoint handoff now opens only its immutable
segments and rebases live committed overlay values onto that prepared root.
It neither rereads the WAL tail nor copies its payloads to activate readers.
The captured root must match the live root, and live coverage/WAL generation/
committed bytes must match the serialized writer boundary before preparation.
Rebasing compares immutable blob identities, including present-null tombstones,
instead of assuming the captured generation remains in the parent chain: a
shared overlay collapse can legitimately remove that ancestry. Different
same-sequence maintenance batches therefore remain distinct. A coverage-only
handoff reuses the new root without retaining an empty overlay. Crash recovery
still reads and validates the durable WAL; publication remains fallible before
CURRENT and an allocation-free serving swap after CURRENT.

This removes WAL replay and payload copying, **not all preparation work**:
rebasing still enumerates live overlay keys and rebuilds changed-leaf admission
before entering the publication fence. The old retained-map representation is
not a persistent per-key delta tree, so this is not an O(1) whole handoff claim.
The same-sequence tail test now collapses overlays after capture, asserts that
the published tail retains the identical vector-metadata blob, confirms zero
root WAL bytes in memory, and reopens to verify durable replay. Separate
tombstone/resurrection/root-mismatch/coverage tests exhaust allocation failures.

Unchanged immutable posting segments also share reference-counted mappings
across successive generations. Reuse requires the same namespace, generation,
and both content/admission checksums. Final release frees the original owner’s
payload; old query leases remain valid. This avoids duplicate mmap aliases
during delta publication, but does **not** yet make physical checkpoint files
independently reusable leaf chunks or change durable filename reclamation.
Tests assert identical base mapping addresses across publication and independent
lease lifetime, including allocation failure and namespace/checksum mismatches.
Focused logs: `/private/tmp/pr593-rebase-ownership-debug-20260906.log`,
`/private/tmp/pr593-shared-posting-maps-debug-20260906.log` (19 passing tests,
two intentional durability-fault logs), and
`/private/tmp/pr593-physical-reuse-debug-20260906.log` (three passing tests).

The native unfiltered RaBitQ path now uses a statically dispatched score sink:
the existing estimator emits eight distances/error bounds directly into the
existing SIMD candidate-admission gate. No leaf-sized distance/error output or
identity-position array is written on this path. Filtered/non-native paths keep
the array API. The arithmetic, centroid special case, candidate heap semantics,
deferred projection obligation, and authoritative completion policy are unchanged;
native cancellation also reaches the quantizer’s bounded checks. Parity tests
cover L2, inner product, cosine, 3/64/65 dimensions, centroid/non-centroid queries,
37-row partial batches, and retained projection references. The existing vector
kernel cancellation test passes alongside them:
`/private/tmp/pr593-fused-parity-debug-20260906.log`.

Query profiles now separate unresolved-frontier, incomplete-top-k, and valid-but-
overlapping-bound outcomes, and count unresolved individual posting bounds and
incomplete routing directories. These are diagnostics, not tighter bounds or
reduced search effort. Existing certified-stop and no-proof/effort-contract tests
pass (`/private/tmp/pr593-routing-reasons-debug-20260906.log`). Fresh performance
measurement is pending; no speed/RSS improvement is inferred from these tests.

Still outstanding after these changes: independently reusable physical
checkpoint chunks with durable and query-lease reference accounting, sealed
shared-vector WAL tails, and better-balanced/tighter-bound routing. Posting WAL
checkpoint preparation still has a validated copying fallback for older,
unsealed retained boundaries; the new live-reader handoff does not remove it.

#### Retained-state/fused candidate: fresh 50K measurement

Pinned executable: `/private/tmp/pr593-rebase-fusion-antfly-20260906`, SHA-256
`21a992463d88e95036ad5b0c6176cf8cb7084d73b517a2b03bb086b86c29d419`.
Run: `/private/tmp/vdbbench-pr593-rebase-fusion-50k-20260906-b`.
This binary includes posting-reader rebasing, shared posting mappings, fused
candidate scoring and routing counters, **not** the shared-vector extent work
described below. Public API, 1536D50K, batch 100, ordinary ANN/rerank settings,
C1/10/20/30, 30-second curves, mixed writes/queries and restart. No request
errors were reported. A preceding sandbox-local startup attempt failed before
load and is not a benchmark result.

| Metric | Prior matching binary | Retained-state/fused |
| --- | ---: | ---: |
| Ready (insert + catch-up), s | 32.5095 (10.4432 + 22.0663) | 34.3678 (11.2866 + 23.0812) |
| Fresh recall | 98.64% | 98.56% |
| QPS C1 / C10 / C20 / C30 | 277.80 / 2125.03 / 2201.39 / 2170.61 | 297.09 / 2123.48 / 2251.79 / 2295.96 |
| p95 C1 / C10 / C20 / C30, ms | 3.505 / 5.261 / 21.953 / 54.418 | 3.663 / 5.263 / 20.990 / 46.014 |
| Load/read-only peak RSS, GB | 1.7688 | 1.6374 |
| Mixed peak RSS, GB | 2.4111 | 2.2107 |
| Restart sampled peak RSS, GB | 0.3255 | 0.4663 |
| Mixed query QPS / p95, ms | 117.21 / 170.07 | 162.19 / 108.73 |
| Mixed update rows/s / write p95, ms | 1927.50 / 316.69 | 1758.98 / 408.41 |
| Mixed recall / final catch-up, s | 97.609% / 2.604 | 97.150% / 2.414 |

The C30 query and live-memory results improved, but readiness, restart RSS,
and mixed write latency regressed. This is **not an all-metric win** or proof
that fusion alone caused the improvements. Both binaries include several
changes and these fresh corpora can have different online topology. Phase RSS
comes from `qualification-summary.json.phase_rss_profiles`, not the single
post-load footprint sample. Existing CPU contention was accepted by the user;
it remains a qualification caveat, not an explanation proven by these samples.

#### Shared-vector sealed extents and retained reader tails

The shared-vector manifest now supports bounded immutable WAL extent receipts
(V5 only when extents are present). Each receipt binds filename generation,
committed byte length, source coverage, last batch and optional minimum mutation
sequence. Sequence zero is valid; coverage-only extents are explicitly distinct.
Sealing syncs the old file, creates an empty append target and durably publishes
CURRENT. The manager preallocates its complete serving successor first, then
swaps the writer/reader target without replay. Ambiguous publication invalidates
the live vector writer so no caller can append through the old target.

Stable-tip and primary-snapshot staging seal before releasing writer exclusion.
An exact sealed prefix can be removed from the next manifest while retaining
the newer physical WAL files: no read/replay/copy of a growing suffix is required
to prepare CURRENT. The fast path requires every retained mutation to lie
strictly beyond the new base; old/unsealed/overlapping boundaries retain the
validated fallback. Source coverage alone is never used to identify a prefix.

Serving activation filters the persistent AVL by the sealed receipt's batch
identity, sharing unchanged subtrees and transaction payloads. Subtree min/max
batch metadata prunes wholly old/new subtrees; arbitrary-height AVL joins keep
the filtered tree balanced. This is zero WAL replay/copy, not constant-time
preparation. Old query leases retain their prior records. Recovery still reads,
checksums and validates every referenced extent; only the active file may have
an incomplete suffix trimmed. GC and backup enumeration retain sealed filenames.
Backup CURRENT preserves the physical base source floor instead of relabeling
an omitted empty base with the live WAL watermark.

Focused Debug tests pass: sealed compaction with racing appends/tombstones and
old leases; identical retained tail payload addresses; same-sequence coverage;
multiple extents including mutation sequence/batch zero; active torn suffix;
sealed corruption rejection; ambiguous CURRENT poisoning/reopen; AVL balance
at every cutoff and exhaustive allocator failures. Logs:
`/private/tmp/pr593-sealed-vector-handoff-debug-20260906.log` (6 tests),
`/private/tmp/pr593-sealed-vector-manifest-debug-20260906.log` (6 tests), and
`/private/tmp/pr593-sealed-vector-manager-debug-20260906.log` (4 tests).
The fault test intentionally logs an uncertain-publication error.
Performance of this subsequent shared-vector change is still unqualified.

Remaining physical-format work is independently reusable HBC checkpoint chunks,
with durable and query/maintenance-lease filename accounting. Sharing existing
whole-segment mappings and sealing WAL files do not remove the full HBC rewrite
after eight deltas. Tighter/better-balanced routing also remains experimental;
the new counters must establish whether proof failures come from unresolved
metadata, an incomplete top-k, or genuinely overlapping bounds before changing
partitions or stopping policy. The public profiling script now retains all five
new reason counters (the first 50K profile used its earlier field list).

#### Regression gate: 1M replayed-generation A/B and cosine-radius gap

The preserved old binary and retained-state/fused binary were both tested on
the same post-mixed 1M generation, using concurrency-only resume and unique
labels `before-rebase-fusion-20260906` / `after-rebase-fusion-20260906`.
These are **not fresh-ingest qualifications**, and concurrency-only reports
recall as zero because it does not execute the recall pass.

| Metric | Before | After |
| --- | ---: | ---: |
| QPS C1 / C10 / C20 / C30 | 26.420 / 459.299 / 664.905 / 520.453 | 28.117 / 414.932 / 371.685 / 298.370 |
| p95 C1 / C10 / C20 / C30, ms | 69.053 / 34.264 / 57.156 / 94.617 | 65.147 / 41.696 / 101.887 / 189.408 |

This is a substantial regression. Compilation overlapped these runs, but
contention is **not an established cause** and must not excuse the result.
Pause additional physical-format work until the query regression is isolated.

The candidate's 1,000-query diagnostic profile
(`public-query-profile-routing-reasons-20260906.json` in the same run root)
measured 99.099% recall, 238,933 approximate scores / 148.5 exact completions,
2,048 leaves/query, 39.14-ms mean / 85.04-ms p95 HTTP latency. Server mean was
37.04 ms, of which leaf scoring was 6.20 ms, artifact reads 3.83 ms, and child
expansion 1.94 ms. These stage totals do not account for the entire request;
new timers isolate initial/scan admission, native-leaf lookup, and deferred
projection completion. Do not label the remaining time CPU contention without
measurements. A same-process alternating ReleaseFast kernel microbenchmark
measured roughly 41.7--42.3 ns/vector for the array gate and 42.1--42.3 for the
fused gate; this does not explain the large end-to-end regression. Reproduce:
`zig build lib-vectorindex-test -Doptimize=fast -- 'fused native candidate scoring microbenchmark'`.

All seven stopping checks/query failed on an unresolved frontier, with 2,649
unresolved posting bounds and no incomplete routing directory. Code inspection
found a real design gap: `PostingStore.recomputeCentroid` explicitly discarded
the radius for every non-L2 metric, including cosine. Other construction/append
paths already supported cosine, so full maintenance could erase that proof.
This gap contributes to excessive candidate work but is **not proven to cause
the newest before/after regression**; both samples use the same stored tree.

Full centroid refresh now shares its radius routine with bulk construction and
splits. Cosine radii use normalized chord geometry with f64 accumulation and
outward expansion; empty postings retain zero, and zero/non-finite vectors or
centroids leave the bound unresolved. Inner product retains its safe fallback.
The existing loaded matrix is reused without additional vector I/O. Tests cover
batch-loader reuse, a finite conservative bound after full cosine refresh,
scale invariance, degenerate inputs, moving-centroid bounds and subtree
admissibility (`/private/tmp/pr593-cosine-bound-refresh-debug-20260906.log`).
Already-published NaN radii are not silently relabeled valid: they require
maintenance/rebuild. A fresh run is needed to qualify the routing benefit.

#### Pre-1M query-work reduction and residual scheduling A/B

Flat native admission now resolves each selected leaf to a query-generation-
bound directory/index handle and sums its authenticated scan cost by that
index. Scanning reuses the handle instead of repeating the overlay/delta and
directory search. It still validates payload checksums lazily. Dirty leaves
retain the ordinary resolver fallback; a token from another generation fails
before its directory pointer is dereferenced. The additional probe fields are
included in existing scratch capacity accounting. This does not change the
number of selected leaves, admission cost, scoring arithmetic or recall policy.

Native residual completion retains a query-owned arena across completion
batches. Backing allocation growth is reserved against the query's resource
slice before allocation, without double-counting the old transient byte
observer. Budget denial reports ResourceBudgetExceeded. All read tasks join
before arena reset, and query teardown frees capacity and releases credit.
This is intra-query reuse, not an unbounded cross-query residual cache and not
a claim that the first allocation per request disappears.

Focused Debug tests validate old leases after publication, foreign-generation
and wrong-posting rejection, dirty-leaf invalidation, exact float32 parity,
retained arena capacity/pointer reuse, allocation denial and zero remaining
reservation, plus governed worker cancellation/permit release:
`/private/tmp/pr593-leaf-residual-final-debug-20260906.log` (4 passing tests).
Earlier combined lifecycle coverage passed in
`/private/tmp/pr593-leaf-residual-debug-20260906-d.log`. The benchmark result
validator's seven tests and qualification shell syntax check pass.

`ANTFLY_EXPERIMENT_INLINE_NATIVE_RESIDUAL_READS=1` is a temporary qualification
A/B, recorded in both fresh and resume provenance. It keeps the same positional
read coalescing, validation, decoding and exact scoring, but skips helper-task
scheduling for native residual completion. Default scheduling is unchanged.
Compare the retained 1M generation with and without it, using the new initial
admission, scan admission, native-leaf lookup and projection-completion timers.
Do not promote the override or claim a speed win without those results.

#### No-progress projection deltas: publication and retry fix

The stage-timing 1M diagnostic observed repeated 31,169,318-byte publications
with zero source values, 1,769 changed leaves and the same 1,769 deferred
projections. The builder re-encoded an already-current quantized-only row when
its missing projection was still unavailable. Each no-op extended the chain
toward another full rewrite and replaced serving verification state. This is
real amplification, not completed acceleration or a valid readiness signal.

The delta builder now retains an already-current scan row when projection
loading cannot improve it. New/dirty rows still publish useful quantized-only
acceleration. A delta with no captured WAL bytes, no source values and no newly
written scan rows aborts its temporary writer before fsync/rename: no immutable
filename, CURRENT update, reader-generation replacement or chain-depth increase.
Real WAL/state work and full checkpoints retain the normal publication path.

An unchanged no-progress attempt backs off from one second to a bounded
60-second retry interval. A different HBC publication identity or shared-vector
publication revision bypasses the delay immediately, including vector rebuilds
at the same source sequence. The manager provides a monotonic process-local
revision (not an unleased pointer); loader policy changes reset retry state.
Embedders without a revision callback still get bounded retries. Acceleration
debt remains visible, and the outer scheduler keeps reporting deferred work
even when backoff intentionally leaves no builder running. This does not mark
unavailable projections ready or replace authoritative exact completion.

Focused Debug validation: 14 checkpoint/lease/WAL/admission tests passed in
`/private/tmp/pr593-no-progress-delta-regression-debug-20260906.log`; the sealed
WAL ambiguity test intentionally logs its injected durability error. Final
focused tests in `/private/tmp/pr593-no-progress-delta-final-debug-20260906.log`
also verify byte-identical CURRENT, no new segment file, unchanged chain/WAL
generation, pending debt through backoff, and immediate same-sequence recovery
when the projection revision advances. Timing tests use an explicit clock and
a pinned deadline rather than real sleeps.

The preceding 50K attempt reported 30.7041 seconds to readiness, but its running
harness subsequently exited with a shell parse error. It is **not qualified**.
The then-retained benchmark roots and pinned binaries under `/private/tmp`
subsequently became absent (not removed by this work). Their earlier timings
remain historical observations in this document, not currently reproducible
raw bundles. The harness currently passes shell syntax validation; no fresh
1M run should be accepted before this no-progress fix is requalified.

The additional fault check also passes:
`/private/tmp/pr593-no-progress-delta-corruption-debug-20260906.log` verifies
that a source revision exposing projection corruption fails without publishing,
keeps debt visible, and can subsequently recover at unchanged source coverage.

The frozen qualification runner is now retained in the worktree at
`scripts/run_vdbbench_qualification_snapshot_20260906.sh`, executable and
repository-relative, rather than only in `/private/tmp`. Its SHA-256 is
`81b5423a72f0a55e81a1154669cea662e7fdb994e5c7e4193d6c36ad8fec8af8`.
The benchmark logic is unchanged; this separate snapshot avoids live harness
edits invalidating a running shell's read position. It passes `bash -n`.

### 2026-09-06: readiness regression attribution and native checksum experiment

The complete no-progress 50K qualification is retained under
`.benchmark-results/pr593-no-progress-50k-20260906` in this worktree. Its
18.6207 s insertion + 20.3825 s catch-up = **39.0032 s readiness** remains a
regression against row-block B's 15.2988 + 9.0817 = **24.3805 s**. Catch-up
accounts for 11.30 s of the 14.62 s difference. Initial loading published one
178 MB full HBC checkpoint, not repeated no-progress deltas. Removing the
no-progress loop did not recover the load baseline.

New stage timers and a stack sample isolate useful targets without attributing
the regression to unspecified contention:

- `.benchmark-results/pr593-publication-profile-50k-20260906` used pinned
  binary SHA-256 `d221051cad6e1de7cd7ed1c7e86e8378e1ea1d346b8a04611feb945de2ac0a54`.
  Public batch 100 / four requested workers / normal effort / float16 with
  authoritative completion were unchanged. Query phases were shortened to
  five seconds, so this is **diagnostic evidence**, not the final QPS comparison.
  Readiness was 10.3710 + 22.0758 = **32.4468 s**, still above the best baseline.
  Full checkpoint staging took 2.876 s: overlay 4.7 ms, topology 39.6 ms,
  candidate scans 2.815 s, vector directory 13.8 ms, and sync 2.9 ms.
  Within that pass, projection reads consumed 1.833 s for 49,802 physical reads
  / 153.93 MB over 451 leaf batches; lookup consumed 24.3 ms.
- `.benchmark-results/pr593-replay-sample-50k-20260906/replay.sample.txt`
  samples the same binary during source catch-up. Of 775 samples on the replay
  worker, 497 were closing a source capture. Within those, 224 were in shared
  vector checkpoint work, 158 in posting publication (130 in replacement-patch
  construction), and 63 in shared-vector WAL successor preparation. These are
  sample counts from one window, **not whole-run elapsed-time percentages**.
  Hot inlined checksum loops appear in vector block writing and WAL parsing.
  The sampled run's 35.802 s load is not an uninstrumented qualification.
- Earlier implausible public LSM counters did not reproduce with this rebuilt
  binary (24 runs and zero current-scan readers at readiness). Their original
  cause is unresolved; do not use those corrupt-looking values to explain
  resource pressure.

Implemented candidates, all preserving source coverage and exact scores:

1. Reusable per-build artifact-key/request scratch, charged to the vector
   construction slice before growth and released when the generation build
   ends. Budget denial remains explicit backpressure.
2. Cold projection reads drain a shared work queue with bounded helpers instead
   of allocating/waiting for a task wave every eight shards. Helpers acquire
   node-wide read permits; cancellation joins them before scratch reuse.
3. Source revision certification uses sorted 256-key primary reads, preserves
   mutation order, hashes the same full artifact bytes, and skips tombstones.
4. Native format CRC32 uses target-feature-gated ARM IEEE CRC instructions or
   portable slicing-by-eight. The polynomial, initial/final state, stored
   checksums, validation requirements, and file versions are unchanged.
5. Replacement-patch matching uses SIMD-aware `indexOfDiff` for equal runs;
   it preserves the scalar match endpoint and patch operations rather than
   trading compression ratio for reduced search effort.

CRC-only ReleaseFast measurements (`pr593-native-crc-releasefast-20260906.log`
under `.benchmark-results`) show 64 MiB processed in 127.67–127.89 ms by the
standard byte loop versus 6.36–6.47 ms by the accelerated implementation,
approximately **20x for this kernel only**, with identical checksums. This
does not imply a 20x end-to-end speedup. Debug compatibility tests exercise
alignment, tails, incremental chunks, and the portable path explicitly.

The 24.38 s 50K target and all-metric qualification gate remain unchanged.
Fresh full-duration 50K/mixed/restart and 1M results are still required before
calling these candidates an overall improvement.

#### Pre-CRC control: faster readiness, failed query-tail/RSS gate

The full-duration control with reusable scratch, queued cold readers and batched
revision certification (but **without** accelerated CRC or SIMD patch matching)
is retained at `.benchmark-results/pr593-publication-workers-50k-20260906`.
Binary SHA-256:
`a3aaf0d7c5f0a2e423657a656c8d551764150ec8b4d6deaf242e0ffc62577b1d`.
Public batch 100/four requested load workers, default effort, C1/10/20/30 for
30 seconds each, 1,000 profiled queries and 30 seconds of mixed traffic remain
enabled. The CRC server build overlapped this control; this is not an isolated
A/B and that overlap does not establish the cause of the query regression.

| Metric | No-progress control | Pre-CRC candidate |
| --- | ---: | ---: |
| Ready (insert + catch-up), s | 39.0032 (18.6207 + 20.3825) | 26.8620 (12.7907 + 14.0713) |
| Recall | 98.55% | 98.49% |
| C1/10/20/30 QPS | 247.8 / 1666.8 / 2156.0 / 2210.8 | 267.6 / 1515.9 / 1755.4 / 1857.5 |
| C1/10/20/30 p95, ms | 4.879 / 8.543 / 25.471 / 50.993 | 3.989 / 11.199 / 36.145 / 66.802 |
| Read-only RSS, GB (decimal) | 0.9751 | 1.6100 |
| Mixed RSS, GB (decimal) | 4.0377 | 1.8038 |
| Allocated disk after mixed, GB | 0.9192 | 0.9310 |

The candidate's C30 p95 is 31% worse and QPS 16% lower than the no-progress
control. It is **not an overall win**, and readiness still misses 24.3805 s.
Mixed traffic had no request errors; write p95 was 718.4 ms, query HTTP p95
116.7 ms and server p95 36.7 ms. Restart checks completed with 98.16%/98.15%
cold/warm recall after updates. Footprint demand was sampled once, not a
continuous peak, and post-mixed disk is not initial-load peak disk usage.

Checkpoint scans improved to 2.214 s, with projection reads 1.151 s. Capture
finalization accumulated 4.083 s. Serial profile server p95 was 4.672 ms; that
does not explain C30's 66.802 ms tail. Concurrent server-stage measurements
and a repeated same-generation comparison are needed to separate service
time, admission and scheduling from client/transport delay. Do not assign
the unexplained difference to contention or treat CRC speed as a query fix.

#### Shared checksum library and architecture validation

The checksum is now `zig/lib/hash/src/crc32.zig`, exported by `antfly_hash`.
Native and WASM vector-index modules depend on it; `lib-hash-test` is included
in the aggregate unit-test target. It retains IEEE CRC32 format compatibility
and allocates no heap. No format version or integrity check was removed.

- ARM64 hardware-CRC and generic fallback Debug execution passed.
- AMD64 macOS Debug execution passed under Rosetta (not native AMD64 hardware).
- Linux AMD64 and ARM64-with-CRC Debug cross-compilation passed; these are not
  Linux runtime results. Docker runtime validation was unavailable because the
  Docker daemon was not running.
- All 57 selected native format/WAL Debug tests passed after moving the module.
- The earlier portable-path ReleaseFast microbenchmark took 24.05–24.09 ms per
  64 MiB versus 128.71–129.04 ms for the standard loop on this ARM64 host
  (approximately 5.3x for the portable kernel, not an AMD64 speed measurement).

Architecture logs are `.benchmark-results/pr593-lib-hash-*-20260906.log`.
The in-progress server performance build predates the module relocation but
contains the same checksum algorithm; its benchmark must be identified by
binary SHA rather than represented as a clean checkout of the current tree.

#### Completed CRC candidate: faster load/read-only curve, mixed crash rejects qualification

`.benchmark-results/pr593-native-crc-50k-20260906` used binary SHA-256
`b8e04650fbbc50f5230cbb47f966b56bce64d14c362a95ef16da76a63cbf5660`.
It includes native-vector CRC acceleration and SIMD patch matching, not the
subsequent shared-LSM/full-text CRC substitution or tree-scratch fix below.
The official public 50K phase completed at **11.5617 s insert + 10.0490 s
catch-up = 21.6107 s ready**, 98.69% recall, C1/10/20/30 QPS
285.56/2067.80/2097.22/2356.46, and p95 3.772/6.515/32.506/51.398 ms.
The lower-level insert loop reports 10.93 s; compare the full driver's 11.5617 s
against historical end-to-end insert durations, not those two different clocks.
Capture finalization accumulated 1.914 s versus the pre-CRC control's 4.083 s;
full HBC checkpoint staging took 1.990 s, including 0.970 s projection reads.
Those stage changes explain part, not necessarily all, of the 5.25 s readiness
improvement over the pre-CRC control; this was not a single-change isolated A/B.

**This run failed qualification:** PID 42959 crashed during mixed traffic with
SIGSEGV at address 0x4. The symbolized report is retained as
`antfly-mixed-crash.ips` in the run root. There is no successful mixed/restart
qualification or new all-metric baseline. Do not advance it to 1M or compare
its incomplete mixed RSS/disk sampling as if the full lifecycle finished.

The crash points to `addChildCandidatesFromIds` -> `estimateQuantizedDistances`:
the split scratch API grew child IDs without growing the two centroid-score
planes. Pressure reclamation frees oversized score planes to zero capacity
between queries; tree scoring must regrow them itself. This also protects a
flat/fused-to-tree transition, but such a transition was not established as the
trigger in this 50K run. A deterministic Debug
test reproduces `index 2, len 0` at the exact slice before the write. The fix
reserves scalar score capacity at the consuming tree-scoring boundary, not a
decoded `dims * child_count` vector matrix. It covers fresh and undersized
scratch, explicitly pressure-reclaimed scratch, and asserts vector-matrix
capacity is unchanged. The before/after logs
are `.benchmark-results/pr593-tree-scratch-{before,after}-debug-20260906.log`;
the after run passed three tests without leaks. Fused score parity also passes.
The pinned performance binary does not contain this fix yet.

The historical insertion concern is valid: the windowed/sealed run inserted in
10.4432 s and retained-state/fused in 11.2866 s, before accelerated native CRC.
The latter also delivered **46.014 ms C30 p95**, better than both the 50.993 ms
no-progress control and this candidate's 51.398 ms. The lost latency reference
must not be silently revised upward. Its readiness was 34.3678 s, so it did not
combine that tail result with 24.38 s readiness.

#### Same-generation residual scheduling A/B and paired tail attribution

`scripts/profile_vdbbench_concurrent_tail.py` is a read-only diagnostic tool,
with pre-encoded requests, paired HTTP/server timers, and slowest-5% cohorts.
Its unit test verifies that cohort attribution preserves paired timings rather
than subtracting unrelated percentiles. It is **not official VectorDBBench QPS**.
One-process/30-coroutine execution proved client-limited (442.7 QPS, 216.2 ms
HTTP p95 versus 4.2 ms server p95) and was rejected for server attribution.
The retained scripts support independent spawned client processes to avoid
that bottleneck; all processes synchronize after dataset preparation/warmup.

The 30-process/one-query-per-process A/B uses the same `b8e04650...` executable
and a clone of the already qualified pre-CRC control's post-mixed, restarted
generation (source sequence 862). No writes, routing/effort/precision changes,
or primary-vector ownership changes were made. Files are under
`.benchmark-results/pr593-tail-ab-20260906`. `queued-mp30.json` uses ordinary
governed helpers; `inline-mp30.json` changes only the existing
`ANTFLY_EXPERIMENT_INLINE_NATIVE_RESIDUAL_READS=1` switch.

| Diagnostic, 30 seconds | Queued helpers | Inline residual reads |
| --- | ---: | ---: |
| QPS | 1538.94 | 1496.34 |
| HTTP p95, ms | 46.519 | 47.881 |
| Server p95, ms | 33.830 | 33.928 |
| Recall | 98.107% | 98.105% |
| Slowest HTTP 5%: mean HTTP / server, ms | 60.150 / 43.138 | 61.313 / 41.949 |
| Slowest HTTP 5%: mean outside dense-server timer, ms | 17.012 | 19.364 |

Admission is negligible in these cohorts (mean 0.007/0.024 ms). Queued-tail
artifact-read time averages 24.90 ms, with leaf scoring 6.97 ms and exact-distance
work 11.58 ms; profile spans can overlap and must not be summed as exclusive
CPU costs. Inline scheduling does **not** materially improve the end-to-end or
server tail in this sample, so keep the governed default. This excludes one
simple scheduling explanation; it does not yet establish the root cause of the
remaining tail or justify reducing recall, exact completion, or durability.

#### Extend the shared CRC to full-text and primary LSM

Full-text `segment.zig`, LSM WAL encoding/replay, SST block/footer verification,
repository footer writing, and the shared atomic-sink CRC range scans now use
`antfly_hash.Crc32`. All previously used IEEE `std.hash.Crc32`; neither their
checksum bytes, polynomial, format versions, validation coverage nor replay
failure semantics change. Runtime, embedded/WASM and focused storage-test and
benchmark module dependencies are wired explicitly.

The focused Debug run passed **41 tests**, including full-text lazy corruption
checks, WAL torn/corrupt replay boundaries, and SST physical/footer checks:
`.benchmark-results/pr593-shared-storage-crc-debug-20260906.log`.
The LSM extension is relevant even with full-text disabled: benchmark document
and exact-source storage still passes through the primary WAL and SSTs.
Its end-to-end benefit remains unmeasured; do not attribute the earlier 21.61 s
result to this later change.

#### Shared-CRC/tree-scratch 50K qualification and remaining query debt

The next complete fresh public batch-100 run used binary SHA256
`2d96a8d37ab7543fddf1b737fcf7766e98991f215c65498ec0a009aed09ff602`,
including shared full-text/LSM CRC and the tree score-capacity fix. Evidence:
`.benchmark-results/pr593-shared-crc-tree-fix-50k-20260906`.
It completed the read-only curve, 30 seconds of concurrent updates/queries,
and cold/warm restart without request failures or the previous scratch crash.

| Metric | Result |
| --- | ---: |
| Ready / insert / catch-up, seconds | 17.5995 / 9.5597 / 8.0398 |
| Fresh recall | 98.44% |
| C1 / C10 / C20 / C30 QPS | 255.33 / 1417.23 / 1909.05 / 2038.19 |
| C1 / C10 / C20 / C30 p95, ms | 4.530 / 13.298 / 33.537 / 53.628 |
| Read-only / mixed / restart RSS peaks, decimal GB | 1.7255 / 2.0345 / 0.5435 |
| Mixed queries QPS / HTTP p95 / server p95, ms | 133.27 / 167.092 / 21.341 |
| Mixed updates rows/s / write p95, ms | 2020.89 / 323.266 |
| Post-mixed restart recall | 98.13% |
| Post-restart allocated / logical disk, decimal GB | 0.9240 / 0.9173 |

The single footprint observation reported 1.50 GB attributable demand; it is
not a continuously sampled demand peak. This sample recovers and improves
the historical 24.38 s readiness result, but does **not** recover the better
46.014 ms C30 p95 or establish an all-metric win. No isolated attribution of
the full readiness improvement to CRC is possible from these runs alone.
Mixed HTTP latency includes substantial time outside the dense search timer.

Additional Debug coverage passed: 8 integration tests for segment merge,
native atomic CRC sinks and tree scratch; 8 vector scratch/fused-scoring tests;
and an explicit pressure-reclamation reproduction. The latter grows score
buffers past the retained limit, reclaims them to zero, then traverses an
internal node without allocating a vector-fetch matrix. `search_runtime.zig`
is now explicitly included in vectorindex test discovery.

A separate no-update diagnostic with the earlier native-CRC executable,
`.benchmark-results/pr593-tail-fresh-50k-20260906`, completed at 23.7152 s
ready (11.6635 s insert), 98.55% recall, 2470.34 C30 QPS / 44.677 ms p95.
It ran only C1/C30 and no mixed phase, so it is not an overall qualified winner.
Its restarted profile used **zero projection reads**, 137.412 residual reads,
and no externally loaded artifact candidates per query.

The flat-exact routing arm of the earlier paired tail diagnostic
(`pr593-tail-ab-20260906/flat-mp30.json`) produced 1512.91 QPS, 48.111 ms
HTTP p95, 28.314 ms server p95 and 98.320% recall. Although the server timer
improved, HTTP latency and throughput did not beat queued-tree control.
Keep the default route; do not interpret profiling-process QPS as official
VectorDBBench throughput or change effort/recall to manufacture a win.

The completed mixed run also exposed a separate lifecycle performance gap:
its restart profile performs **277.49 projection reads + 129.158 residual
reads/query**, with 289.688 externally loaded artifact candidates. The
restart log explains why: 440 of 448 rebuilt leaf rows defer their projection
plane, then retry without publication at 1/2/4/8-second backoff. Quantized-only
rows are serviceable and source coverage is applied, but preferred native
acceleration is not complete. Successful HTTP/restart qualification must not
be mistaken for absence of this acceleration debt.

Root cause: `loadDenseVectorProjectionsForPostingBuildImpl` accepts float16
values only. Shared-vector WAL values remain authoritative float32, and one
such row rejects the entire leaf's optional plane. The proposed fix derives
the same bounded float16 encoding/error metadata from the pinned WAL value,
reuses one vector-sized decode buffer, and leaves exact float32 ownership and
completion unchanged. WAL rows deliberately carry no persistent block
locator; exact completion resolves them under the matching query generation.
No forced shared-vector compaction or primary scan is needed. Four focused
Debug tests passed (unaligned/scaled/signed-zero WAL projection, nonfinite
rejection, exact float32 rerank parity, and residual-location reuse).
Same-corpus post-update A/B qualification is still pending below.

#### WAL projection repair: same-corpus A/B and durable restart

The fix is implemented in `nativePostingProjectionFromWal` and the posting
projection loader. Its ReleaseFast executable is retained at
`.benchmark-assets/pr593-wal-projection/bin-root/bin/antfly`, SHA256
`9fa27b84fc4cd60c3120158a741303e437442f601e938ce8025410a99daff6d7`.
Both A/B roots are APFS clones of the completed mixed workload at sequence
1110, with identical vectors, topology, source ownership and query settings:
`.benchmark-results/pr593-wal-projection-ab-{control,fixed}-20260906`.
The public VectorDBBench client ran C1/C30 for 30 seconds each, plus separate
serial recall passes and 1,000 detailed profiled queries. No updates occurred
during these comparisons.

The first control overlapped our ReleaseFast build and is **not** the preferred
latency comparison. It measured 1608.01 C30 QPS / 44.065 ms p95 / 1.2368 GB RSS.
The first fixed process published all **440** missing projection planes in one
164,054,033-byte delta, with **zero deferred projections** and no repeated
checkpoint attempts. It measured 2071.05 QPS / 37.536 ms p95 / 1.8749 GB RSS.

We then restarted the fixed generation and repeated the old control after our
build had finished. Other work still ran on this shared host, so these remain
observational samples, not an exclusive-machine statistical qualification.

| Matched post-update serving metric | Old control repeat | Fixed, second restart |
| --- | ---: | ---: |
| C1 QPS / p95, ms | 285.51 / 4.071 | 290.38 / 3.826 |
| C30 QPS / p95 / p99, ms | 1871.70 / 35.763 / 59.287 | 2188.61 / 33.266 / 52.221 |
| Serial recall | 98.13% | 98.44% |
| Approximate scores/query | 23193.616 | 23193.616 |
| Projection reads/query | 277.490 | 0 |
| Total physical vector reads/query | 406.648 | 131.820 |
| Authoritative exact completions/query | 141.356 | 137.434 |
| Sampled cache-inclusive RSS peak, decimal GB | 1.1535 | 1.9164 |
| One post-curve attributable-demand observation, decimal GB | 0.2773 | 0.2218 |
| Allocated disk after first A/B, decimal GB | 0.9185 | 1.0825 |

The second restart performed **no projection rebuild**, preserved zero
projection reads and source coverage, and retained the same recall. Exact
float32 completion remains enabled; the WAL-derived plane only narrows the
candidate ambiguity set. The new plane has no fabricated persistent WAL
locator, so exact candidates may still need metadata/key resolution.

The repeated samples suggest ~16.9% higher C30 QPS and ~7.0% lower p95, but
**RSS rose ~66%** and allocated disk grew by ~164 MB. Do not present the first
timing difference as an isolated speedup, the single demand observation as a
peak, or this change as an all-metric win. The RSS increase survived restart;
it is not solely publication-time residency. Resource-manager query working
memory peaked at 254,687,620 bytes in the fixed repeat (173,411,640 retained at
the end), while HBC caches used about 8 MB. The larger gap to cache-inclusive
RSS still needs residency attribution; it must not be dismissed as equivalent
to attributable demand or explained by contention alone.

Four further Debug lifecycle tests passed: committed vector-WAL replay,
compaction preserving concurrent WAL suffixes and old query leases, sealed
WAL compaction without suffix copying, and bounded projection-build scratch.
Log: `.benchmark-results/pr593-wal-projection-lifecycle-debug-20260906.log`.

Remaining qualification: a fresh mixed 50K run with this last WAL-plane fix,
then 1M; the 17.5995 s full 50K result above predates this last change. Remaining
design work: bound/reclaim posting-local mmap residency and reduce obsolete
projection retention/write amplification without removing these serving planes
or weakening generation leases. This lifecycle fix does not by itself explain
the initial fresh-generation C30 tail, which occurs before mixed updates.

#### RSS attribution and batch-scoped decode/read memory

The subsequent read-only C30 diagnostic corrects the earlier mmap suspicion.
On the already-published fixed generation, RSS stayed around 450 MB through
serial/C1 and rose during C30 without any checkpoint. Near-peak `vmmap` reported
about **1.5 GiB resident MALLOC_MEDIUM**, only 147.4 MiB dirty in that region,
206.8 MiB mapped files, and 12.7 MiB thread stacks. Process physical footprint
was 221.4 MiB (243.5 MiB peak) despite roughly 2 GiB RSS. The large discrepancy
is principally allocator-region residency, not mapped posting files or live
query buffers of equivalent size. Raw reports are
`pr593-wal-projection-ab-fixed-20260906/vmmap-rss-regions-{early,late}.txt`
and `heap-rss-regions.txt` under `.benchmark-results/`. These memory-inspected
queries are diagnostic, not an uninstrumented latency qualification.

The heap also showed 30 allocations of 5,408 KiB each, consistent with one
900-candidate float32 matrix per query (900 * 1536 * 4 bytes before allocator
rounding). `ensureRerankCapacity` allocated this matrix before the projection
and exact-completion passes selected the actual load batch. That is unnecessary
wide storage even though complete-shell scalar/identity arrays remain required
for the boundary proof and authoritative fallback.

Implementation:

- Separate rerank metadata capacity from float32 decode capacity. Grow decode
  storage only at the unresolved projection batch, actual exact batch, or
  generic authoritative fallback. The latter still reserves all returned
  vector views; no recall, batching policy, or exact-score contract changes.
- Native lookup/read arenas previously allocated MiB-scale payload storage
  from libc and destroyed it each call. Native residual arenas likewise
  reused memory only within a query, then returned it to libc. Use OS-backed
  pages for these bounded temporary arenas so teardown returns their backing
  pages instead of accumulating general-allocator residency after bursts.
  Primary-only key arenas retain their existing allocator. Query-local reuse,
  read-task joins, cancellation lifetime, and residual-budget admission remain
  intact. There is no process-wide malloc purge or benchmark-only concurrency
  cap; syscall/latency effects must be measured before accepting the change.

Focused Debug checks passed: 9 vector scratch/batch tests, 7 HBC query tests,
5 native scoring/tree tests, and 2 residual-admission/read-worker lifetime
tests. Evidence logs have prefix `pr593-batch-scratch-` in `.benchmark-results`.
The new scratch regression verifies 900 scalar slots with no decode matrix,
then a 128-vector decode batch, stable metadata/scalar pointers during growth,
and safe growth to the complete shell for fallback. Existing overflow,
pressure-reclamation, saved-location, uncached, exact-score, and cancellation
checks remain green. End-to-end performance qualification follows below.

##### OS-backed arenas: RSS win, latency regression; superseded by reusable leases

Paired, uninstrumented public-API runs reused exactly the same published 50K
data root (`pr593-batch-scratch-pages-ab-20260906`). Both used batch-100-created
data, identical effort, 30-second C1/C30 windows, and 1,000 profiling queries.
Neither run performed ingestion or generation rebuilding. The old libc control
ran immediately after the page-backed candidate; this is one paired sample,
not a confidence interval.

| Metric | Original libc arenas | Batch decode + per-call OS arenas |
|---|---:|---:|
| C1 QPS | 284.18 | 278.90 |
| C30 QPS | 2,264.46 | 2,104.42 |
| C30 p95, ms | 34.702 | 39.060 |
| C30 p99, ms | 57.245 | 65.070 |
| Peak cache-inclusive RSS, decimal GB | 1.9172 | 0.6514 |
| Resource-governed search working-set peak, MB | 252.66 | 124.52 |
| Recall | 98.437% | 98.437% |

Approximate scores (23,193.616/query), exact completions (137.434/query), and
projection reads (zero) were unchanged. The RSS reduction is real, but C30
QPS fell 7.1% and p95 rose 12.6%; **the per-call OS-arena design is not accepted
as the final optimization**. Single demand observations were 218.6 MB/control
and 341.2 MB/candidate, not continuously measured peaks. Binary SHA-256 values:
control `9fa27b84fc4cd60c3120158a741303e437442f601e938ce8025410a99daff6d7`,
candidate `26cd3e42fd8d167a57784fb2c83b2366a9e1580f567db48a8ae38ac56b4d5362`.

The replacement retains batch-scoped decoding and adds an IndexManager-owned
native-read arena pool under the existing node resource governor. Exclusive
leases contain only temporary bytes; all read tasks join before lease return,
and generation lifetimes stay with their existing owners. Idle capacity remains
charged to `dense_search_working_set`, is reclaimable under pressure, and is
capped at 64 MiB aggregate, 32 slots, and 4 MiB per slot. These are cache limits,
not fixed query-concurrency admission limits. Oversized requests can still run
within admission but their arenas are discarded on return. OS page backing
avoids libc retention on eviction, while arena reuse avoids repeated mapping
and unmapping on ordinary warm queries. Pool locks only move pointers/counters;
allocation, arena reset, resource admission, and deallocation occur outside them.

Seven focused Debug tests pass: pooled/non-pooled exact-score/location parity,
idle reuse and reclamation, oversize/slot caps, hard-budget eviction/error
mapping, concurrent lease isolation using `std.Io`, and residual reservation
reuse. Nine vector scratch/batch tests also pass. Evidence:
`pr593-read-pool-debug-20260906.log` and
`pr593-read-pool-vector-debug-20260906.log`. Pool performance qualification is
pending; do not combine these read-only samples with an earlier fresh-load time.

Six additional scratch-accounting/pressure, exact-score, and read-worker
cancellation tests passed (`pr593-read-pool-lifetime-debug-20260906.log`). The
broader `unit-storage-test-audit` was not green: concurrent vector-storage work
had introduced `artifact_payload.zig`, `vector_payload_store.zig`, and
`vector_wal_view.zig` without manifest entries. This experiment does not alter
those unrelated files. Pool regressions live in the already-manifested
`index_manager.zig` test module.

The pooled binary built successfully in ReleaseFast; verification was fully
cached (SHA-256
`2b4c592a417aa0b7ed67f76e7c94e5d14df1a05e87a71f8db3852c21a5d8ae68`).
Same-root runs, in order after compilation:

| Sample | C1 QPS | C30 QPS | C30 p95 / p99, ms | Peak RSS, GB | One footprint observation, MB |
|---|---:|---:|---:|---:|---:|
| Pool | 307.67 | 2,378.16 | 37.990 / 63.703 | 0.6668 | 348.8 |
| Original libc repeat | 300.36 | 2,121.49 | 37.007 / 60.788 | 1.8439 | 231.4 |
| Pool repeat | 291.65 | 2,270.88 | 42.345 / 66.868 | 0.6570 | 344.9 |

Recall and approximate/exact work were unchanged in every sample. The first
pool run reported 102.39 MB retained / 132.54 MB peak governed search working
memory and zero soft-limit events or hard-limit rejections. The pool restores
throughput and bounds allocator residency, but **does not yet establish tail
latency or physical-footprint parity**. The footprint sampler obtained one
observation per run, not a reliable demand peak; its wired-growth component is
node-wide and must not be attributed wholly to Antfly. The pool's first sample
included 195.85 MB of wired growth; its repeat and the control had none.
These measurements do not justify promoting this candidate as an all-metric
win. Residual task scheduling is the next isolated diagnostic, not a reason to
change recall or exact-score semantics.

The same pooled binary with the existing diagnostic
`ANTFLY_EXPERIMENT_INLINE_NATIVE_RESIDUAL_READS=1` measured C1/C30
294.42/2,326.81 QPS, C30 p95/p99 41.458/65.955 ms, and 0.6052 GB peak RSS.
Recall and candidate counts were unchanged. Removing helper-task scheduling
did **not** materially resolve the tail regression, so this experiment does
not justify disabling parallel reads or introducing a new concurrency cutoff.
The subsequent fresh lifecycle qualification uses normal governed reads, not
this diagnostic flag.

##### Fresh 50K lifecycle qualification of the pooled candidate

`pr593-read-pool-full-50k-20260906` completed the public API batch-100 load,
C1/10/20/30 curves, 30 seconds of concurrent queries/updates, catch-up, and
cold/warm restart without request errors, panic, quarantine, or activation
failure. This uses the same pooled binary above and normal governed reads.

- Readiness: **18.2336 s** = 10.1762 s insert + 8.0574 s catch-up.
- Fresh recall: **98.68%**. C1/10/20/30 QPS:
  **274.50 / 1,751.77 / 2,152.09 / 2,117.21**.
- Corresponding p95: **3.994 / 8.594 / 25.571 / 55.453 ms**;
  p99: 5.966 / 12.373 / 51.900 / 71.666 ms.
- Load/read-only peak RSS before mixed updates: **1.6582 GB**; full live/mixed
  peak: **2.0183 GB**; restarted serial-query peak: **0.4282 GB**.
- Mixed queries: 130.94 QPS, 168.44 ms HTTP p95, 20.08 ms server p95.
  Updates: 1,860.18 rows/s, 367.68 ms write p95; catch-up 1.2643 s; no errors.
  Mixed sampled recall was 97.892%; warm restart recovered to 98.67%.
- Restart reported all 50,000 query-visible vectors, applied/target sequence
  1062/1062, and no posting or vector-projection publication pending.

This is **not** an all-metric improvement over the preceding full qualification:
17.5995 s readiness, 53.628 ms C30 p95, and 2.0345 GB mixed RSS remain the
appropriate prior-phase comparison. The large steady-generation RSS win does
not eliminate the live/mixed peak. At the end of this fresh mixed run the
resource ledger held 633.13 MB in the primary LSM block/table cache and
224.25 MB in LSM in-memory state. Search/apply working-set lifetime peaks were
458.23/350.66 MB (not necessarily simultaneous), while vector projection build
scratch peaked at 1 MiB. One physical-footprint observation was 1.8 GB live and
295.7 MB restarted; neither is a continuous demand peak. The remaining work is
mixed-path/fallback residency and tail attribution, not another unmeasured
blanket cache reduction. **1M remains unqualified for this candidate.**

A final read-only diagnostic used 30 independent client processes, pre-encoded
requests, and paired HTTP/server profile samples against the original same-root
50K generation (`pr593-read-pool-tail-20260906.json`). It is not official
VectorDBBench throughput. In its HTTP-slowest 5% cohort, mean HTTP/server times
were 70.15/57.95 ms, with 12.20 ms outside the server timer. Mean HBC search was
57.77 ms; residual/vector loading 23.77 ms; projection completion 19.90 ms;
leaf scoring 5.16 ms; and scan-admission wait zero. Stage timers nest and these
are wall-clock measurements, not CPU samples, so they must not be added as
independent costs or interpreted as proof of disk stalls. This supports
targeting projection completion and residual loading with CPU/off-CPU
attribution next, while the separate mixed workload also needs investigation
of its much larger HTTP/server gap. The diagnostic server was stopped cleanly.

##### Pooled-reader 1M qualification (2026-09-06)

Started a fresh `Performance768D1M` (Cohere 1M, 768 dimensions) qualification
in `.benchmark-results/pr593-read-pool-full-1m-20260906`, using the exact
50K-qualified pooled binary SHA `2b4c592a...a5d8ae68`, batch 100, four load
workers, unchanged effort, native HBC, float16 candidate planes with
authoritative completion, C1/10/20/30 for 30 seconds each, 30 seconds of mixed
updates/queries, and cold/warm restart. The default full-text index is removed
through the public API, as in the prior VectorDBBench comparisons. Dataset
download was required before timed insertion and is not part of load duration.

Continuous process-only memory sampling now uses the SDK's
`proc_pid_rusage(RUSAGE_INFO_V4)` through
`scripts/sample_macos_process_memory.py`, every 0.5 seconds. This avoids
periodic `vmmap` walks/suspension and records resident bytes, physical footprint,
the kernel's process-lifetime maximum physical footprint, disk I/O counters,
and process start identity across restart. It does not assign other processes'
wired growth to Antfly. The ABI layout and live sampling passed a local smoke
check. Raw timeline:
`.benchmark-results/pr593-read-pool-full-1m-kernel-20260906.jsonl`.

Promotion remains conditional. Historical whole-run references are:
617.3662 s / 828.00 C30 QPS / 94.317 ms p95 / 7.0534 GB live RSS for
regression recovery, versus 743.2529 s / 890.56 C30 QPS / 82.506 ms p95 /
7.6647 GB mixed RSS for the later completed sealed-WAL qualification. Keep
their distinct lifecycle phases and anomalous earlier concurrency curves
visible; no synthetic best-of-each baseline. Results follow when complete.

The full lifecycle completed without request, capture, activation, quarantine,
or restart errors. It establishes correctness/workload coverage, **not an
overall performance promotion**:

| Metric | Pooled candidate, fresh 1M |
|---|---:|
| Readiness: insert + catch-up | **327.1028 s: 236.8447 + 90.2581** |
| Fresh recall | **99.24%** |
| C1 / C10 / C20 / C30 QPS | 19.18 / 565.38 / 624.36 / **615.59** |
| Corresponding p95, ms | 44.040 / 28.198 / 75.423 / **97.858** |
| Corresponding p99, ms | 53.732 / 35.875 / 91.316 / 115.651 |
| Live/mixed peak RSS (`ps`) | **6.9973 GB** |
| Restarted serial peak RSS (`ps`) | **4.7107 GB** |
| Kernel lifetime max physical footprint: live / restart | **3.5193 / 0.3423 GB** |
| Mixed query QPS / HTTP p95 / server p95 | 119.47 / 116.21 ms / 83.83 ms |
| Mixed update rows/s / write p95 | **613.42 / 1,718.79 ms** |
| Mixed recall / final catch-up | 99.2448% / 39.0046 s |
| Cold / warm restart recall | 99.21% / 99.25% |
| Cold / warm restart serial p95 | 40.8 / 43.8 ms |
| Post-restart allocated / logical disk | **10.4802 / 10.3853 GB** |

Readiness is approximately 47% faster than regression recovery and 56% faster
than the later sealed-WAL reference. However, C30 throughput is 26–31% lower;
mixed writes regress sharply; and disk exceeds the earlier 8,118,652-KiB
post-mixed allocation (~8.31 GB). The cold C1 versus later serial-query timing
asymmetry remains visible and must not be averaged away. The continuous
kernel timeline contains 1,910 samples for the initial process and 200 for
its first restart; kernel high-water values include transient peaks between
samples. These are process-only physical-footprint measurements, not node-wide
demand estimates or the sum of separate lifetime peaks.

The post-mixed/restart profile performed 239,679.97 approximate scores/query,
2,048 leaf visits, 146.315 authoritative reranks, and 146.037 residual reads.
The 428.724 **total exact vectors scored** counter is not the authoritative
rerank count: native leaf scan fallbacks averaged 6.437/query, with 3.533 stale
payload observations. Seven conservative routing checks resolved on each query
but all overlapped the boundary; none stopped early and no unresolved posting
bounds remained. Leaf scoring averaged 14.16 ms (p95 34.78), versus only
0.569 ms mean residual/vector loading. This points to leaf/fallback work and
bound tightness rather than widening the exact-completion shortcut.

Generation 14 was repeatedly attempted with zero WAL bytes, but those attempts
published no new files and backoff grew from 1 to 60 seconds. The logged
projection-staging portion took roughly 0.5–0.9 ms with zero vectors. Therefore
the attempt count alone is not evidence that full checkpoint rewrites caused
the query regression. A same-generation original-buffer versus pooled-buffer
comparison follows to distinguish the scratch optimization from wider serving
and lifecycle differences. Do not promote this full candidate as best overall.

###### Same-generation 1M buffer A/B and promotion decision

After the full mixed/restart qualification, sequential original-buffer and
pooled-buffer resumes used the same durable data, source sequence 10186,
unchanged query settings, and C1/C30 30-second curves. Both included cold/warm
recall checks and 1,000 detailed profiling queries. Original binary SHA is
`9fa27b84...d5362`; pooled SHA is `2b4c592a...a5d8ae68`. Neither A/B server log
reported a new posting-checkpoint publication. Labels are `-libc-control` and
`-pool-stable` in the same run root; this is one paired sample, not a confidence
interval or a comparison of fresh-load speeds.

| Metric | Original buffers | Pooled buffers |
|---|---:|---:|
| C1 QPS | 85.80 | 77.92 |
| C1 p95 / p99, ms | 12.364 / 12.770 | 14.707 / 20.466 |
| C30 QPS | 669.18 | 671.88 |
| C30 p95 / p99, ms | 103.840 / 135.746 | 101.723 / 132.152 |
| Peak RSS (`ps`), GB | 4.4069 | 4.4113 |
| Kernel lifetime peak physical footprint, MB | 624.43 | 602.68 |
| Governed search-memory peak / retained, MB | 115.99 / 95.91 | 78.65 / 64.10 |
| Detailed recall | 99.252% | 99.252% |

Both detailed profiles scored exactly 239,962.381 approximate vectors and
146.482 exact/reranked vectors per query, with zero native leaf-scan fallbacks
and zero stale payload observations. Thus the earlier first-restart fallback
profile must not be compared as if it were this same stable serving phase.
The later stable A/B still falls short of historical peak throughput even
without stale leaf payloads; eliminating that fallback alone is not sufficient.

The pool reduces governed query memory by ~32% and remains a reasonable
bounded-memory implementation shape, but **the large 50K RSS gain does not
generalize to 1M total RSS**. The paired physical-footprint reduction is only
~3.5%; C30 throughput/tails are approximately unchanged, and C1 regressed in
this sample. Retain the pool as the memory-efficiency candidate, not a claim
that query latency is solved. Do not replace the best overall baseline: full
mixed-write latency, lower historical C30 throughput, and durable disk growth
remain material release/performance gates. Keep initial-ingest improvements
distinct from stable-query performance. The next investigations should target
serving readiness/fallback behavior, leaf/routing work, publication amplification,
and mixed-write stalls without weakening recall or exact-score semantics.

All qualification servers and the continuous sampler were stopped cleanly.
The sampler passed its SDK-layout/live-counter smoke check and Ruff checks;
no product code or benchmark effort was changed during this qualification.

###### Scan lookup, identity batching, and measured obsolete-row compaction (2026-09-06)

Investigation of the remaining mixed-write/query/disk gates found two repeated
read patterns and a missing disk-maintenance trigger. The candidate changes:

- Build a per-leaf immutable mutation-segment index while opening the native
  generation. Serving-row validity then uses constant-time lookups instead of
  probing every newer segment for each of three mutation families. Recovered
  WAL and live overlay invalidation checks remain mandatory; generation leases
  and authoritative scoring are unchanged.
- Prove identity-preserving document overwrites with three sorted batch reads
  on one snapshot (document ordinals, live states, canonical reverse mappings),
  replacing roughly three point reads per document. Missing/deleted states,
  mixed new/existing batches, deletes, and missing reverse mappings retain the
  existing mutation/repair path. Conflicting reverse mappings still fail.
  Also make partial lookup-key allocation cleanup safe on allocation failure.
- Count obsolete delta scan rows from authenticated directory/index metadata.
  Optional maintenance selects a streamed full checkpoint when measured dead
  rows exceed 64 MiB and at least one third of the retained segment chain.
  This is a conservative lower bound, excluding dead base rows and metadata.
  The existing atomic publication and lease-safe reclamation remain in use.
  This bounds retained disk debt, but **does not eliminate whole-generation
  write amplification**: physical chunk reuse remains a separate design gain.
  Qualification must measure the additional maintenance cost, not assume it
  is free merely because it runs in the background.

Before changing the binary, resumed the pooled 1M control with
`ANTFLY_BENCH_BATCH_PROFILE=1`, suffix `-mixed-stage-control`, 15-second C30 and
30-second mixed windows. This is a diagnostic restarted-state sample, not a
replacement for the original fresh-load qualification:

| Metric | Restarted control |
|---|---:|
| Read-only C30 QPS / p95 | 731.287 / 98.148 ms |
| Mixed query QPS / HTTP p95 | 246.697 / 58.965 ms |
| Mixed write rows/s / HTTP p95 | 2,259.896 / 412.972 ms |
| Mixed catch-up | 71.483 s |
| Request/lifecycle errors | none |

Most logged 100-row DB batches took 14–16 ms, including 8–9 ms in identity
metadata checks. The largest logged DB batch was 445 ms, including 432 ms in
the primary store. These timers are **not** complete HTTP latency. The original
fresh-load mixed result (613 rows/s, 1,719 ms write p95) did not reproduce after
restart, so post-load memory/maintenance state remains part of the diagnosis.
Do not attribute that difference to the candidate: this used the unchanged
`2b4c592a...a5d8ae68` binary.

Debug verification: 27 selected storage tests passed, including all document
identity tests, immutable replacement/scan-row shadowing for every mutation
kind, canonical-map conflict/repair, concurrent same-sequence WAL preservation,
and obsolete-byte policy boundaries. Log:
`.benchmark-results/pr593-scan-identity-compaction-debug-20260906.log`.
ReleaseFast/public-API performance qualification is pending; no performance
promotion is justified yet.

Fresh 50K qualification completed with pinned ReleaseFast binary SHA256
`ede009f10ae629c448c8aca49c699b155506b89f1bf2926e9f416b4a485d0c17`,
root `.benchmark-results/pr593-scan-identity-compaction-50k-20260906`.
Batch 100, four load workers, unchanged effort, cosine/k100, C1/10/20/30 for
30 seconds each, 30-second mixed workload, and 1,000 post-restart profile
queries. No request/lifecycle errors. The previous pooled fresh run is the
reference, not the faster same-generation 50K read-only sample.

| Metric | Previous pooled fresh 50K | Scan/identity/compaction candidate |
|---|---:|---:|
| Ready / insert / catch-up, s | 18.2336 / 10.1762 / 8.0574 | 16.3201 / 8.2807 / 8.0394 |
| C30 QPS / p95, ms | 2,117.21 / 55.453 | 2,509.55 / 39.641 |
| Fresh recall | 98.68% | 98.61% |
| Mixed write rows/s / p95, ms | 1,860 / 367.68 | 2,063.88 / 282.762 |
| Mixed query QPS / HTTP p95, ms | 130.94 / 168.44 | 117.75 / 171.123 |
| Mixed dense server p95, ms | 20.08 | 9.055 |
| Mixed catch-up, s | 1.2643 | 1.5749 |
| Load/read-only peak RSS, GB | 1.6582 | 1.8597 |
| Full live/mixed peak RSS, GB | 2.0183 | 2.1689 |
| Restart peak RSS, GB | 0.4282 | 0.6688 |
| Post-restart allocated disk, GB | 1.0758 | 1.0726 |

Candidate C1/10/20/30 QPS: 287.988 / 2,087.980 / 2,431.940 / 2,509.554;
p95: 3.819 / 6.224 / 19.649 / 39.641 ms. Cold/warm restart recall was
98.44% / 98.62%; serial p95 8.2 / 3.7 ms. Mixed recall was 97.800%, measured
during asynchronous mutations rather than after catch-up. The kernel sampler
observed initial-process lifetime physical-footprint peak 1.9613 GB and
restart peak 175.31 MB; these are not comparable with a single end-of-phase
footprint observation. Timeline:
`.benchmark-results/pr593-scan-identity-compaction-50k-kernel-20260906.jsonl`.

This is a throughput/write-latency improvement with a memory tradeoff, **not
an all-metric promotion**. Mixed HTTP/client throughput must remain separate
from the improved server timer; increased completed writes also change the
amount of concurrent indexing work. A fixed-write-rate experiment would be
needed to isolate interference at equal offered work. The 50K run had no
obsolete-delta compaction trigger, so it does not qualify that policy's disk
benefit. Fresh 1M qualification follows.

Fresh 1M qualification completed on the same `ede009f1...5d0c17` binary:
`.benchmark-results/pr593-scan-identity-compaction-1m-20260906`. All source
rows became visible, source coverage reached 10001 before mixed traffic and
10224 afterward, and the harness accepted fresh/mixed/restart checks without
request or lifecycle errors. This remains one whole-candidate paired sample,
not an isolated attribution of every difference to one code change.

| Metric | Previous pooled fresh 1M | Scan/identity/compaction candidate |
|---|---:|---:|
| Ready / insert / catch-up, s | 327.1028 / 236.8447 / 90.2581 | 317.6381 / 207.4219 / 110.2162 |
| C30 QPS / p95, ms | 615.589 / 97.858 | 818.195 / 73.278 |
| C10 QPS / p95, ms | 565.381 / 28.198 | 301.331 / 43.996 |
| Fresh recall | 99.24% | 99.20% |
| Mixed write rows/s / p95, ms | 613.420 / 1,718.794 | 739.959 / 1,435.441 |
| Mixed query QPS / HTTP p95, ms | 119.468 / 116.215 | 142.650 / 100.292 |
| Mixed dense server p95, ms | 83.833 | 68.141 |
| Mixed catch-up, s | 39.0046 | 47.3503 |
| Full live/mixed peak RSS, GB | 6.9973 | 6.0263 |
| Restart peak RSS, GB | 4.7107 | 3.3173 |
| Initial kernel lifetime peak physical footprint, GB | 3.5193 | 5.3066 |
| Post-restart allocated disk, GB | 10.4802 | 9.2075 |

Candidate C1/10/20/30 QPS: 74.789 / 301.331 / 649.675 / 818.195;
p95: 17.776 / 43.996 / 54.556 / 73.278 ms. Background generation publication
overlapped early query phases: the C10 regression must not be hidden by the
better C30. After restart, 111 dirty leaves were repaired and republished at
the same source sequence between query phases. Warm restart serial p95 was
75.0 ms (previous 43.8 ms), while the later detailed profile was substantially
faster; those are not the same serving phase. Detailed recall was 99.208%,
with 240,353.449 approximate scores, 146.437 authoritative reranks, and 6.464
native fallback leaves/query during that transition. Mixed recall was 99.2045%.

The obsolete-row policy triggered full generation 8 at 1,144,010,056 measured
dead bytes, then generation 11 at 1,343,446,876 dead bytes. Final ANN/index
allocation was 2.6402 GB versus 3.9292 GB previously. Primary/metadata remained
3.6258 GB and centralized serving blocks 2.9158 GB, plus a 25.66 MB vector WAL.
Pre-restart allocation was 8.6381 GB; use the **9.2075 GB final** value because
restart published the mixed-workload serving delta.

This reduces retained disk, not proven write amplification. Kernel process-I/O
counters observed about 43.829 GB written by the candidate's initial process,
versus 42.914 GB by the previous initial process; completed mixed writes also
differed (22,300 versus 18,600 rows). Read counters were 40.742 versus 45.063 GB.
These are process/kernel accounting counters, not a logical write-amplification
ratio or exact device traffic. The 5.306 GB physical-footprint peak occurred
during catch-up, about 293 seconds after startup. Lower RSS does not cancel
that physical-memory regression. Kernel timeline:
`.benchmark-results/pr593-scan-identity-compaction-1m-kernel-20260906.jsonl`.

Against the **best-throughput qualified** windowed-reader/sealed-WAL 1M run
(890.558 C30 QPS), allocated disk remains worse: 9,207,463,936 versus
8,313,499,648 bytes (`8,118,652 KiB`), **+893,964,288 bytes / +10.75%**.
Earlier r56 used 5,326,904 KiB with a different layout and roughly 427 QPS;
that is not the same best-throughput comparison. Do not call the new candidate
the best overall shape: catch-up, early/restart latency, physical footprint,
and true write amplification remain open. Stable-generation old/new query
comparison follows to separate query execution from publication/repair timing.

The final same-generation query-only A/B used source sequence 10224, C1/C30
for 30 seconds each, no mixed writes, and no checkpoint publications in either
server log. Labels are `-old-stable` and `-new-stable`. This was one sequential
pair, not repeated/alternating trials:

| Metric | Previous pooled binary | New candidate binary |
|---|---:|---:|
| C1 QPS / p95, ms | 82.484 / 13.954 | 92.766 / 12.598 |
| C30 QPS / p95, ms | 816.146 / 83.040 | 685.582 / 122.124 |
| C30 p99, ms | 98.336 | 270.505 |

The stable A/B does **not** qualify the new query code as a throughput win:
C1 improved but C30 regressed materially. The fresh-run improvement therefore
must not be attributed wholly to the query lookup change; generation layout,
repair timing, workload state, and concurrency behavior need further isolation.
Do not promote the candidate as best overall. All qualification servers and
the kernel sampler were stopped cleanly; `git diff --check` passed.

Idle reclamation is also bounded, not guaranteed convergence to the historical
8.31-GB allocation. Before the final small serving delta, recorded obsolete
scan rows were 547,911,828 bytes in an approximately 2.6-GB posting chain,
below the one-third compaction trigger. With no further mutations, that debt
can remain indefinitely. The final 9.21-GB result is not merely a transient
deletion queue that will necessarily disappear when the node becomes idle.
Further convergence needs a separately budgeted idle-consolidation policy or
finer-grained immutable chunk reclamation; reducing the trigger alone could
increase rewrite traffic and worsen the measured physical-memory peak.

#### Float16 source ownership and posting-local duplication qualification

The preceding 9.207-GB allocation was measured with table storage
`dense_embeddings=primary_lsm`, not the consolidated source-owner experiment
in `VECTOR_STORE.md`. Consolidation removes the duplicate lossless serving
corpus; it still permits an optional ANN-owned posting-local float16 plane.
One complete 1M/768D plane is 1,536,000,000 payload bytes before metadata,
obsolete revisions, and allocation rounding. This is not mandatory format
overhead, nor is consolidation alone evidence that this plane is removed.

`scripts/run_projection_locality_ab.py` starts the matched float16 ownership
comparison using the archived September 6 qualification harness, one pinned
binary, fresh roots, A/B then B/A ordering, and no inherited experiment flags.
It keeps batch 100, four requested load workers, cosine/k100, C1/10/20/30
for 30 seconds each, 30-second mixed writes/queries, 1,000 profiled queries,
and cold/warm restart. Every 50K arm must pass before 1M. The process envelope
is left automatic, matching the preceding fast float16 candidate; this differs
from the separate float32 source-store experiment's explicit 4096-MB envelope.
Input digests and individual receipts are retained under
`.benchmark-results/pr593-f16-source-ownership-ab-20260906/ab-runs.json`.
Pinned baseline executable SHA-256:
`ede009f10ae629c448c8aca49c699b155506b89f1bf2926e9f416b4a485d0c17`.

The subsequent locality experiment adds the default-on process flag
`ANTFLY_EXPERIMENT_POSTING_LOCAL_PROJECTIONS`. Setting it to `0` on a fresh
root omits the posting projection producer and its optional readiness
requirement. Shared lossless-vector storage, RaBitQ scanning, bounded shared
projection reads, generation leases, and authoritative exact completion remain
enabled. Existing persisted projection planes remain readable: changing this
flag on an existing root is not a valid disk-reclamation A/B. Both locality
arms must use the same new binary; do not compare the new no-local binary
against the older ownership-control binary as an isolated treatment.

Initial focused Debug validation: three tests passed without leaks, covering
the optional projection policy, bounded WAL projection metadata and source
preservation, and rejection of nonfinite projection inputs. Measurements and
promotion decisions remain pending.

Completed ownership 50K A/B + B/A (two fresh arms per mode), using float16:

| Median measurement | primary_lsm | vector_store |
| --- | ---: | ---: |
| Ready / insert, s | 16.116 / 8.080 | 16.696 / 10.655 |
| C30 QPS / p95, ms | 2,278.788 / 51.811 | 2,339.361 / 50.737 |
| Live recall | 98.600% | 98.650% |
| Allocated disk after restart, GB | 1.0931 | 0.7021 |
| Read-only phase peak RSS, GB | 1.789 | 1.515 |
| Mixed phase peak RSS, GB | 3.913 | 1.670 |
| Restart peak RSS, GB | 0.690 | 0.997 |
| Mixed write rows/s / batch p95, ms | 2,052.804 / 305.478 | 2,059.252 / 292.724 |
| Mixed query QPS / HTTP p95, ms | 119.878 / 171.053 | 109.534 / 173.275 |
| Warm restart serial p95, ms | 4.350 | 3.700 |

All four arms passed load, mixed workload, recall, and cold/warm restart gates.
Individual control C30 QPS were 2,518.000 and 2,039.576; source-store results
were 2,413.584 and 2,265.138. Timing variability is substantial: medians do
not establish a small QPS win. The repeated allocated-disk saving is about
35.8%; total readiness is 3.6% slower and restart RSS is 44.4% higher.
Mixed phases have equal duration, not identical write counts. RSS is sampled
resident memory, not allocator demand or the kernel lifetime physical peak.

The first source arm's allocated breakdown verifies actual consolidation:
primary/metadata 22.94 MB, source vectors 286.07 MB, serving references
13.09 MB including WAL, and ANN files 380.49 MB. The remaining ANN payload
and retained-version cost is the target of the separate locality treatment.
The 1M ownership stage began only after all four 50K arms completed. No
default has been changed or performance candidate promoted.

The optional-locality binary built successfully in ReleaseFast at
`.benchmark-assets/pr593-posting-locality-switch/bin/antfly`, SHA-256
`f328d6ccada973955edaa815c52bca849b697962189e84d956f6e11e6079babe`.
Eight focused DB Debug tests passed without leaks (policy, WAL projection,
exact residual scoring/reads, and bounded scratch ownership); the vectorindex
quantized-directory/deferred-projection test target also completed successfully.
Three hermetic runner tests verify alternating order/encoding, failure gating,
and invalidation if the binary changes during an arm. The locality measurement
is separate from the ownership matrix above and is not yet a measured win.

The first 1M source arm completed with exit code zero, but the original
overbroad input guard stopped further arms when another session edited
`run_vector_store_enrichment_ab.py`. An audit of all 16 original fingerprints
found exactly that one changed file. It is neither invoked nor imported by
this workload. The executable, archived harness, client preparation, public
query/mixed profilers, validation, summary, and disk-accounting helpers all
matched their pre-run fingerprints. The original `invalid_reason` remains in
the receipt; `unused-input-change-audit.json` records the hashes and explicit
acceptance of this one arm. The runners now watch explicit dependencies rather
than unrelated glob matches. The remaining reversed 1M pair resumed without
changing the measured binary, workload, encoding, or timeout.

First qualified 1M float16 ownership pair (reversed pair still pending):

| Measurement | primary_lsm | vector_store |
| --- | ---: | ---: |
| Ready, s | 304.178 | 301.362 |
| C30 QPS / p95, ms | 858.191 / 62.167 | 975.918 / 79.212 |
| Recall | 99.250% | 99.240% |
| Allocated disk after restart, GB | 9.240 | 5.890 |
| Read-only phase peak RSS, GB | 8.484 | 6.017 |
| Mixed phase peak RSS, GB | 6.469 | 7.744 |
| Restart peak RSS, GB | 3.430 | 6.807 |
| Mixed write rows/s / batch p95, ms | 869.983 / 1,228.299 | 3,071.427 / 255.857 |
| Mixed query QPS | 152.114 | 276.728 |
| Mixed catch-up, s | 45.350 | 119.145 |
| Warm restart serial p95, ms | 92.500 | 16.100 |

This is not an all-metrics win: source ownership saved 36.3% allocated disk
and improved throughput, but C30 p95, mixed RSS, restart RSS, and catch-up
regressed. The 119.145-second catch-up only narrowly passed the unchanged
120-second gate. Source mixed writes completed 92,400 rows versus 26,200 for
the control, so this is an equal-duration workload, not equal replay work.
Both arms reported no mixed errors and passed cold/warm restart checks.

Fresh-run query phases include real maintenance. The source arm published a
full 1.829-GB posting generation during query qualification: its projection
builder read 1,536,620,544 physical bytes for 1M vectors, and total staging
lasted 83.959 seconds including scheduling/yields. This confirms the optional
posting-local plane still duplicates payloads. It also means the observed
QPS gain cannot be attributed solely to faster shared-vector lookups; resulting
generation layout and maintenance timing differ. The pending no-locality
treatment is intended to measure the remaining payload/layout tradeoff.

The reversed 1M source arm **failed** the unchanged mixed catch-up gate.
Its read-only measurements were 297.099 s ready, 1,072.601 C30 QPS, and
99.26% recall. Mixed traffic completed 90,400 writes at 3,003.445 rows/s
(254.925 ms batch p95), plus 282.503 query QPS, but after 120.077 seconds
the index was still rebuilding with `dense_publish_pending=true`: applied
sequence 10832 versus target 10905. Row counts remained 1M, which does not
certify freshness of those updates. Exact-vector projection was not pending;
this was real source-replay debt, not only an optional-acceleration flag.
The last 1M control arm was gated off. Do not present the ownership experiment
as a fully qualified repeated 1M result or average the failed source arm into
qualified-result medians. No timeout was raised.

The fresh 50K posting-locality A/B + B/A then started under
`.benchmark-results/pr593-f16-posting-locality-ab-20260906`, using
`scripts/run_posting_locality_ab.py` and the single `f328d6cc...` binary in
both arms. It holds table ownership at `vector_store` and encoding at float16;
only `ANTFLY_EXPERIMENT_POSTING_LOCAL_PROJECTIONS=1/0` changes. The separate
binary incorporates the contemporaneous worktree and must not be compared
against the older ownership binary as an isolated code change. Eight hermetic
runner tests now cover controls, real-input versus unrelated-input changes,
failure gating, and resuming without overwriting qualified roots.

Completed optional-locality 50K medians (two fresh runs per arm, A/B + B/A):

| Measurement | local_on | local_off |
| --- | ---: | ---: |
| Ready, s | 17.061 | 16.395 |
| C30 QPS / p95, ms | 2,597.792 / 36.419 | 648.088 / 70.466 |
| Recall | 98.535% | 98.425% |
| Allocated disk after restart, GB | 0.6990 | 0.3751 |
| Read-only phase peak RSS, GB | 1.487 | 1.499 |
| Mixed phase peak RSS, GB | 1.930 | 1.644 |
| Restart peak RSS, GB | 0.798 | 0.819 |
| Mixed write rows/s / batch p95, ms | 2,161.683 / 270.372 | 1,995.398 / 346.788 |
| Mixed catch-up, s | 1.364 | 1.233 |
| Warm restart serial p95, ms | 3.350 | 4.500 |

All four arms passed the unchanged correctness, mixed-workload, and restart
gates. The initial posting base shrank from 178.48 MB to 21.89 MB, removing
roughly one 153.6-MB float16 plane. Total allocated disk fell 46.3%, but C30
throughput fell 75.1%; removing the plane alone is not a balanced improvement.
The first no-locality arm plateaued by C10 (668.8 / 663.3 / 656.5 QPS at
C10 / C20 / C30), despite nearly equal C1 throughput (284.7 versus 288.2).
The initial post-restart profile showed 439.6 physical reads/query without
locality versus 137.4 with it; about 303.9 were projection reads. Exact
completion stayed narrow (135.7 versus 137.4 vectors/query).

Separate post-restart concurrent attribution used the same pinned binary,
six client processes with five in-flight requests each, and 15 seconds per
arm. These are diagnostic timings, not replacement VectorDBBench results:

| Measurement | local_on | local_off |
| --- | ---: | ---: |
| Diagnostic QPS | 2,246.310 | 1,092.532 |
| HTTP p95, ms | 27.126 | 41.006 |
| Mean server time, ms | 4.670 | 21.212 |
| Mean admission wait, ms | 0.003 | 17.247 |
| Mean leaf scoring, ms | 1.351 | 1.190 |
| Mean artifact work, ms | 1.019 | 1.850 |

The no-locality arm spent 81% of server time in admission. Serial restart
metrics charged approximately 92.01 MB/query without locality versus 6.71 MB
with locality under the same 402.65-MB capacity. This does not prove that
payload duplication is intrinsically required for throughput. Both diagnostic
reopens published a small same-sequence repair delta, so these were not strictly
frozen-generation tests. Their JSON profiles are retained as
`local-{on,off}-concurrent-attribution.json` in the locality result root.

The 50K automatic routing mode is tree traversal; 1M uses flat routing and
already defers bandwidth admission until the selected leaves are known.
Both paths were present in the measured binary. Inspection found that restart
admission reconstruction treated missing optional projections as a reason to
resolve the old posting body even when the complete immutable RaBitQ row was
still current. The follow-up preserves authenticated base-row costs in that
case, retains missing-projection debt tracking, and keeps conservative fallback
charges for immutable/WAL shadows. Three focused Debug tests passed without
leaks, including explicit unchanged, immutable-shadow, and WAL-shadow cases.
Performance recovery from this correction is not yet established; no default
or admission limit has been changed, and no 1M locality arm has run yet.

#### Follow-up: generation-bound progressive tree scan admission

The user requested the longer-term admission changes before retesting. Native
tree routing now retains its SearchView first and admits cumulative selected
leaf work before loading/scoring each candidate payload. Reservations grow
geometrically to amortize governor synchronization without changing traversal,
pruning, search effort, or exact-completion semantics. Growth releases the old
permit before FIFO reacquisition; it never waits while retaining a smaller
permit. A request already owning the entire capacity need not requeue merely
because its cumulative work grows further. Legacy/root-only generations without
a compact directory keep their conservative entry admission, and flat routing
retains its single selected-frontier reservation. No capacity is raised.

Admission and serving now both choose tree versus flat routing from the pinned
generation's active count. Previously the live count could cross the automatic
threshold between admission, preparation, and scoring. The admission lease also
keeps an explicit generation identity after transferring ownership into the
read transaction; pairing it with another generation is rejected.

New public profile counters distinguish the initial coarse estimate, selected
scan bytes, peak granted bytes, reservation count, fallback-leaf count, and
logical scoring-plane bytes. The latter is not physical I/O or total memory
traffic; projection/residual reads retain their separate existing counters and
node-wide optional read-task governor. Concurrent attribution reports absent
new fields as unavailable, not zero, when profiling older binaries.

Seven focused storage Debug tests passed without leaks, including unchanged
versus shadowed rows, exact flat costs, progressive growth, FIFO ordering,
cancellation cleanup, publication/transaction identity, and the empty path.
The focused API table-read target passed 76 tests without leaks, including
public profile mapping. Vectorindex routing/scoring checks and nine Python
runner/attribution tests passed. The broader dirty-worktree admission sweep is
not green: its initial 199-test run had four failures (managed-index readiness /
artifact accounting/dependencies and backup validation). A rerun also hit two
HTTP setup failures under sandbox restrictions. These are recorded separately;
the focused passes do not certify the entire PR. Logs remain in the locality
result root (`admission-*-debug.log`).

The initial reconstruction-only performance build was stopped when the scope
expanded, so there is no reconstruction-only performance result. The complete
progressive-admission ReleaseFast binary is being built at
`.benchmark-assets/pr593-tree-scan-admission`; its matched locality qualification
will use fresh roots and the same batch, effort, recall, and readiness gates.

The complete ReleaseFast build succeeded. Its pinned executable SHA-256 is
`205803589eb952d8a097f03173aba1a3848601dc543c402123fa2a1ba5c75f09`.
Fresh 50K A/B + B/A qualification started under
`.benchmark-results/pr593-f16-progressive-admission-ab-20260906`. The locality
runner now supports resuming to 1M only when the prior arms passed and all
measurement-input hashes, commands, and controlled environment match. Nine
runner tests plus the concurrent-attribution test pass. No older result root
is overwritten or silently rerun.

Completed progressive-admission 50K medians (all four arms passed):

| Measurement | local_on | local_off |
| --- | ---: | ---: |
| Ready, s | 17.974 | 23.007 |
| C30 QPS / p95, ms | 2,372.633 / 50.222 | 1,729.279 / 49.052 |
| Recall | 98.570% | 98.380% |
| Allocated disk after restart, GB | 0.6997 | 0.3752 |
| Load/query phase peak RSS, GB | 1.509 | 1.501 |
| Mixed phase peak RSS, GB | 2.548 | 1.916 |
| Restart peak RSS, GB | 0.986 | 0.833 |
| Mixed write rows/s / batch p95, ms | 2,075.530 / 285.210 | 1,998.570 / 314.627 |
| Mixed catch-up, s | 1.477 | 1.847 |
| Warm restart serial p95, ms | 3.700 | 9.000 |

The harness's `read_only` RSS window starts at `live_load_and_query_start`, so
it includes loading as well as read-only queries; do not compare it with a
steady-state query-only RSS sample. None of these RSS samples establishes the
kernel lifetime physical-footprint peak or allocator demand.

No-locality C30 throughput repeated at 1,715.456 and 1,743.102 QPS, versus
656.529 and 639.647 in the preceding prototype. Its first post-restart profile
reserved a mean 6.61 MB rather than the former 92.01-MB coarse charge. Mean
selected bytes and logical scored bytes both equaled 4,625,052.288, with zero
fallback leaves. The whole first live arm recorded zero queued bandwidth waits;
peak active bytes were 211.92 MB under unchanged 402.65-MB capacity. Progressive
admission made about nine reservations/query and cost 0.0313 ms mean in the
serial attribution profile; this is not free, but it is no longer the old
17.2-ms concurrent queueing pathology. The initial coarse estimate was also
correctly reconstructed as 6.71 MB, independently confirming the base-row fix.

There is a material timing caveat: an independent benchmark under
`.benchmark-results/vector-next-experiments/scale` ran concurrently beginning
19:43:02 PDT (its next arm began 19:47:47). It overlapped the later arms of this
matrix. The second no-locality run had 100.8 C1 QPS; the last locality control
had 123.6 C1 / 628.6 C10 QPS, then recovered to 2,505.3 C30 QPS while still
reporting zero admission waits. Keep all samples, but do not call this an
isolated leaderboard comparison or attribute those fluctuations solely to code.
The repeated disk saving and removal of false admission queueing are stronger
evidence than small latency differences. The no-copy treatment still trades
throughput, cold/warm source reads, and some mixed-write latency for less disk;
it has not become the default.

The same runner resumed to fresh 1M arms only after the completed 50K gate,
without rebuilding, changing inputs, or increasing catch-up timeouts.

The first progressive-admission 1M pair passed all live, mixed, and restart
gates. Matched first-pair results (final reversed-pair outcome follows below):

| Measurement | local_on | local_off |
| --- | ---: | ---: |
| Ready, s | 352.240 | 306.669 |
| Insert / catch-up, s | 250.143 / 102.097 | 231.810 / 74.859 |
| C30 QPS / p95, ms | 893.044 / 87.139 | 857.340 / 69.780 |
| Recall | 99.280% | 99.040% |
| Allocated disk after restart, GB | 5.9202 | 3.8348 |
| Load/query phase peak RSS, GB | 6.109 | 6.110 |
| Mixed phase peak RSS, GB | 6.902 | 6.709 |
| Restart peak RSS, GB | 6.740 | 5.165 |
| Mixed write rows/s / batch p95, ms | 2,795.764 / 281.666 | 2,547.497 / 351.943 |
| Mixed catch-up, s | 114.240 | 92.125 |
| Warm restart serial p95, ms | 12.200 | 13.400 |

No-copy reduced the full posting checkpoint from 1,828,948,817 to 240,893,761
bytes. Post-restart it still issued 473.472 projection reads and 618.672 total
physical artifact reads/query; that locality cost has not disappeared.
Both modes use the flat route at 1M and reserve the chosen frontier once.
The no-copy mean reservation was 23,352,822.912 bytes (zero unknown-cost leaves),
not its 553,648,128-byte conservative whole-query estimate after mixed updates.
That distinction matters: genuine dirty fallback rows can raise the generation
maximum, but no longer force every native selected frontier to pay that maximum
for every leaf. Logical scored bytes averaged 23,128,419.648; selected-frontier
cost can exceed scored work when some candidates are not visited. The control
reserved 23,308,588.032 bytes/query. Serial scan-admission time was 0.173 ms off
and 0.218 ms on; this includes frontier cost resolution, not only queue waiting.

The first locality control recorded 22,598 queued waits across its live arm,
with peak 17 active queries and 399.12 MB active scan bytes under the unchanged
402.65-MB capacity. This is not the old false 92-MB/query 50K bottleneck: real
selected work at 1M still requires bandwidth governance. The first control's
114.24-second mixed catch-up also leaves little margin under the 120-second
gate. Do not promote the no-copy layout or claim an across-the-board win from
this preliminary pair. Its initial control ingestion also overlapped the end
of the independent 50K benchmark noted above.

#### Final progressive-admission qualification outcome

All four fresh 50K arms and both fresh no-copy 1M arms passed. The final
locality-on 1M control failed the unchanged 120-second mixed catch-up gate;
the runner exited nonzero and did not retry it. Its final state at 120.085 s
was applied sequence 10,832 versus target 10,893, `rebuilding=true`,
`dense_publish_pending=true`, and `dense_vector_projection_pending=false`.
All one million documents remained present. The recorded error was
`index did not return to query-visible ready state`; no capture abort or
activation/quarantine error appeared in the inspected control log.

The failed control had accepted 892 batches of 100 updates at 2,966.036 rows/s,
with write p95 252.590 ms. Its recorded replay-finalize total was 44.644 s
across 91 completed finishes, with a 2.231-second maximum, while sequence
metadata flushes totaled 0.513 s. These cumulative counters do not explain all
of the wall-clock debt or establish that admission caused it. They locate the
outstanding work in replay/publication rather than optional projection
completion. Faster acceptance also leaves more replay work after the fixed
30-second mixed-write window; the treatments did not accept an equal number
of updates.

Both no-copy 1M arms qualified completely. Their medians are:

| Measurement | local_off, two qualified arms |
| --- | ---: |
| Ready, s | 299.901 |
| C30 QPS / p95, ms | 873.905 / 67.437 |
| Recall | 99.055% |
| Allocated disk after restart, GB | 3.8219 |
| Load/query phase peak RSS, GB | 6.097 |
| Mixed phase peak RSS, GB | 6.412 |
| Restart peak RSS, GB | 5.160 |
| Mixed write rows/s / batch p95, ms | 2,608.621 / 332.202 |
| Mixed catch-up, s | 87.973 |
| Warm restart serial p95, ms | 14.450 |

The failed final control's *read-only stage* completed at 281.577 s readiness,
885.280 C30 QPS, 85.185 ms C30 p95, and 99.300% recall. Preserve those as
diagnostic partial results, not as a qualified run: it has no qualified
restart/disk result. In particular, its readiness was faster than either
no-copy arm (306.669 and 293.132 s), so a consistent load-time improvement
is not established. The summarizer correctly retains only **one** qualified
locality-on 1M control and **two** qualified no-copy arms; this is not a fully
passed balanced 1M matrix.

Conclusion: keep the generation-bound admission accounting independently of
the locality experiment. It removes the false 50K charge and preserves
generation identity, FIFO progress, cancellation, empty-index behavior, and
the existing scoring/recall policies. No-copy is a promising opt-in disk
tradeoff, not an across-the-board default win: it still loses substantial 50K
throughput, adds projection reads, does not lower load/query RSS, and has
mixed-write/restart-latency costs. The near-limit/failed locality controls keep
mixed replay drain as an explicit remaining qualification issue.

After the performance matrix, the seven focused storage Debug tests passed
again (zero leaks), and all ten Python runner/attribution tests passed from
the scripts directory. A root-directory unittest invocation initially failed
module discovery and was rerun from the correct directory; it was not a
product assertion failure. The admission source and pinned executable hashes
remained unchanged throughout the matrix. No commit, push, default change,
catch-up timeout increase, or recall/effort reduction was made in this step.

#### Four recovery treatments after progressive admission

The next user request was to implement and test all four proposed recovery
targets. They are independent default-off controls, not promoted defaults:

| Control | Treatment |
| --- | --- |
| `ANTFLY_EXPERIMENT_PROJECTION_PAGES=1` | Group up to 256 resolved projection requests by retained physical file/page; copy into existing query destinations; reuse clean 16-KiB pages through a node-wide, pressure-reclaimable cache. |
| `ANTFLY_EXPERIMENT_PHASE_ADMISSION=1` | Release scan bandwidth after candidate scanning, retaining the immutable generation and accounted scratch; acquire a FIFO, cancellation-aware caller-inclusive rerank lane. Existing helper-worker limits remain separate and nonblocking. |
| `ANTFLY_EXPERIMENT_SCAN_PREDICTION=1` | Learn filtered/unfiltered selected work within each immutable generation; use a bounded EWMA for the first tree reservation and retain progressive growth for underestimates. |
| `ANTFLY_EXPERIMENT_REPLAY_FINALIZE=1` | At an idle backlog, divide the existing replay byte envelope among up to four chunks per capture; stop between chunks on the cumulative byte limit, a one-second cooperative quantum, or returning foreground traffic. Add lock/streaming/capture finish-stage timing. |

Projection pages are keyed by non-reused process-local identities of retained
immutable file handles, not paths or ANN generation numbers. Cache hits still
validate each projection payload and retain the normal authoritative float32
completion path. A one-pass page stays on probation unless a grouped read
already contains multiple requests for it. The cache has a 32-MiB payload
ceiling, lazy page allocation, separately reserved metadata, and per-page
resource reservations under the node cache budget; it is an experimental
bounded cache, not an mmap residency claim. Cache pressure/lock contention
remains a normal miss. Grouping metadata and all possible worker-page scratch
are separately admitted before entering the stack frame. If that admission
fails, queries retain the original positional-read implementation.

The new rerank lane limits whole query callers, including their serial I/O,
using a CPU-derived node capacity. Optional helper workers still use the
existing shared helper pool. The scan permit is released before waiting for
rerank capacity, so no cross-pool permit upgrade is introduced. Generation
leases and separately accounted query memory survive that wait. Public profiles
now distinguish `hbc_rerank_admission_wait_ns` from scan admission time.

Replay coalescing is not a larger memory cap or a relaxed durability boundary.
It retains one source capture across complete bounded chunks and publishes
only their covered sequence. The cumulative byte check also prevents several
oversized single-record exceptions from accumulating in one coalesced call.
The time bound is cooperative: one indivisible chunk can exceed it, but another
chunk will not start after the bound. Existing explicit replay-window controls
are not overridden when they already request a different window count.

Validation before the performance build: 17 focused storage Debug tests passed
with zero leaks, including FIFO ordering/cancellation, generation-isolated
prediction, scan-to-rerank handoff, grouped physical reads, cache hit/reclaim
behavior, malformed sibling destinations, and complete-record replay quanta.
The API table-read target passed 76 tests without leaks; Python runner/profile
tests passed 11 tests; Ruff and `git diff --check` passed. The API mapping test
was additionally extended to assert the new rerank timing field. No whole-PR
qualification claim follows from these focused checks.

`scripts/run_posting_locality_ab.py --refinement` supports `pages`, `phases`,
`prediction`, `replay`, and `combined`. Refinement comparisons keep both arms
on `vector_store` with posting-local float16 copies disabled. They retain the
same executable, public API harness, 100-row batches, 4 insert workers,
C1/10/20/30 curve, recall settings, and mixed/readiness gates. The first planned
qualification is combined A/B + B/A at 50K; 1M remains gated on those results.
The ReleaseFast build is staged at `.benchmark-assets/pr593-four-recovery`.

The ReleaseFast build completed successfully with executable SHA-256
`b0c2fccbd403278bfc9015a12727199f604aa584dee29bf733c70e9e4bc86a7a`.
The API target was rerun with all four controls enabled: all 76 tests passed,
zero leaks. Fresh combined qualification started under
`.benchmark-results/pr593-four-recovery-combined-20260906` using the pinned
binary and the full 30-second concurrency/mixed phases. Both comparison arms
disable posting-local projection copies; the control disables all four
recovery switches and the candidate enables all four.

All four fresh 50K arms passed their query, mixed-write catch-up, and restart
gates. The A/B then B/A medians are:

| Metric | All controls off | All four on |
| --- | ---: | ---: |
| Insert + readiness, s | 15.722 | 18.404 |
| C1 QPS | 254.687 | 223.021 |
| C30 QPS | 1,476.038 | 1,676.175 |
| C30 p95, ms | 62.886 | 39.054 |
| Recall | 98.305% | 98.325% |
| Allocated disk after restart, GB | 0.373883 | 0.374043 |
| Load/query phase peak RSS, GB | 1.405 | 1.495 |
| Mixed phase peak RSS, GB | 1.496 | 1.395 |
| Restart peak RSS, GB | 0.665 | 0.826 |
| Mixed write rows/s | 1,597.969 | 1,446.682 |
| Mixed write batch p95, ms | 426.669 | 464.659 |
| Mixed catch-up, s | 1.318 | 1.574 |
| Post-restart physical artifact reads/query | 438.725 | 379.068 |

These are not a recovered all-metric optimum. In particular, throughput did
not improve consistently across pairs: control/candidate C30 was
1,233.943/1,742.696 in the first pair, but 1,718.133/1,609.654 in the reversed
pair. Independent user compiler processes overlapped the measurements; they
were left running as requested. The aggregate p95 improvement is encouraging,
but does not justify attributing the entire throughput difference to code or
promoting the combination. The load/query RSS window includes loading; it is
not steady-state query RSS or physical footprint.

Counter interpretation matters: `hbc_rerank_vector_projection_reads` counts
logical vectors served, including cache hits. It remains approximately 302
with pages enabled and must not be described as physical I/O. The actual
`hbc_rerank_vector_physical_reads` counter falls approximately 14%; larger page
fills also increase physical bytes to about 1.53 MB/query. Artifact read time
is essentially unchanged at 1.28 ms in the serial post-restart profile.
Generation-local prediction reduces reservation operations to 1.496–1.516 per
query, versus roughly nine previously. Neither bookkeeping reduction alone
establishes a latency win.

Before a new 1M qualification, isolate `pages`, `phases`, `prediction`, and
`combined` against the same offline qualified control generation, then reverse
the order. `scripts/run_dense_recovery_query_ab.py` preserves separate cloned
data roots, pins the executable and profiling inputs, records readiness and
process-only memory, and uses the public API. This is diagnostic closed-loop
traffic, not official VDBBench QPS. Client processes now start at staggered
query offsets instead of all starting at query zero, avoiding artificial
cross-client page reuse. Paired tail summaries include actual physical read
counts/bytes, scan and rerank admission waits, and reservation counts. The
expanded Python runner/profile suite passes 12 tests. No defaults were changed.

The same-data 50K diagnostic completed all ten arms under
`.benchmark-results/pr593-four-recovery-query-20260906b`. Each mode has two
independent process lifetimes, with C1 then six-client-process C30 traffic.
The initial unsuffixed attempt completed only its control before the runner's
port preflight rejected a normal TIME_WAIT socket; enabling SO_REUSEADDR on
that preflight fixed the runner. Its partial data are not pooled into this
matrix. No product failure or readiness timeout occurred in these diagnostics.

| Query treatment | Diagnostic C30 QPS | HTTP p95, ms | Dense-server p95, ms | Mean scan admission, ms | Mean rerank admission, ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Control | 1,235.364 | 59.550 | 21.839 | 0.0710 | 0.00008 |
| Pages | 1,124.995 | 80.904 | 18.479 | 0.0668 | 0.00012 |
| Phase admission | 1,039.680 | 91.454 | 21.856 | 0.0580 | 0.2580 |
| Prediction | 1,010.673 | 94.317 | 26.752 | 0.0395 | 0.00013 |
| Combined | 1,278.644 | 56.555 | 21.032 | 0.0383 | 0.3267 |

Independent compiler/test work and another benchmark server overlapped the
isolation pass (one observed foreign server used approximately 571% CPU).
Leave that work running, but do not turn this noisy QPS ordering into a
causal ranking. Pages improve the measured dense-server p95 while HTTP p95
worsens, demonstrating why the paired timers matter. The combined HTTP result
does not recover the historical locality-on throughput. Prediction reliably
removes reservation calls, but less than 0.1 ms/query was spent in scan
admission already; it is not a multi-millisecond tail solution by itself.

The capture-stage logs in the fresh candidate attribute approximately
18–216 ms/session to capture completion, versus approximately 0–6 ms to
streaming finish and negligible index-apply lock wait. These are 50K samples,
not an attribution of the earlier 1M drain failure. The next query-only matrix
reuses the qualified `Performance768D1M-2-local_off` generation to test whether
the phase split behaves differently at 1M. It is explicitly not a fresh 1M
ingest/replay qualification. The diagnostic safety/profile/runner suite now
passes 15 Python tests, including readiness rejection and bounded shutdown of
owned processes. `scripts/summarize_dense_recovery_query_ab.py` summarizes only
successful arms and retains the diagnostic-only designation and per-mode arm
counts.

Follow-up inspection found a specific no-copy fast-path coupling to test next.
In `hbc_index.zig`, `direct_native_scoring` currently requires a valid
`native_projections` plane. With posting-local float16 disabled, even the
unfiltered native RaBitQ path therefore writes identity member/position scratch
arrays, materializes distance/error arrays with `estimateQuantizedDistances`,
and passes those arrays through `addApproxResults`. With the plane present it
uses `estimateDistancesTo` and `NativeCandidateScoreSink`, avoiding those full
intermediate arrays. The quantizer already supports the statically dispatched
sink; sharing that scoring path must not require restoring float16 duplication.
At approximately 240K scored vectors this represents about 5.76 MB/query of
avoidable ID/position/distance/error scratch writes, plus their later reads.
This is cumulative memory traffic, not a claim of 5.76 MB lower live allocation
per query. Earlier isolated sink benchmarks were close, so benchmark the full
leaf path including identity preparation, not just arithmetic, before claiming
that this explains the throughput gap. Preserve candidate insertion order,
error bounds, ties, cancellation, and centralized authoritative completion.

The 1M source baseline already recorded 240,243.672 approximate scores/query
and one scan reservation/query before the four treatments. Thus prediction is
effectively a negative-control treatment on this flat-routing generation, not
a way to recover its query latency. The old approximately 69K-candidate profile
is from a substantially earlier experiment; it cannot establish a regression
introduced by these four changes. Its saved routing counters show complete
bounds but overlapping tests and zero certified stops while scoring 2,048
leaves. Bound tightness/routing quality remains a separate opportunity, not
missing-bound repair or a reason to simply reduce effort.

All ten 1M query-only arms subsequently passed under
`.benchmark-results/pr593-four-recovery-query-1m-20260906`. The executable is
the same pinned four-recovery ReleaseFast binary; each arm starts from its own
clone of the same offline source. Startup completed bounded posting maintenance
before public readiness, then C1 and six-process C30 profiling. This does not
qualify fresh 1M ingestion, mixed replay, or its disk amplification.

| Treatment | Diagnostic C30 QPS | HTTP p95, ms | Dense-server p95, ms | Scan admission mean, ms | Rerank admission mean, ms | Physical reads/query |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Control | 368.065 | 172.507 | 104.172 | 9.239 | 0.00011 | 617.252 |
| Pages | 549.980 | 108.038 | 81.496 | 8.735 | 0.00009 | 594.805 |
| Phase admission | 578.184 | 97.844 | 77.843 | 1.755 | 1.751 | 617.255 |
| Prediction | 551.501 | 102.780 | 80.063 | 8.910 | 0.00010 | 617.395 |
| Combined | 468.471 | 137.318 | 87.553 | 2.079 | 1.723 | 594.823 |

All modes averaged approximately 99.084–99.086% recall. Pages reduce physical
read count only 3.6% while increasing bytes from approximately 0.910 to
1.059 MB/query (16.4%). The same 32-MiB cache is substantially less effective
on the 1M scattered projection working set. Phase separation reduces the sum
of mean scan/rerank admission from approximately 9.24 to 3.51 ms, but the
combination still has approximately 13.1 ms mean artifact work and substantial
leaf-scoring time. Stage timers nest; their percentiles must not be summed.

Interpret the QPS gains cautiously: the prediction-only flag is effectively a
negative control on this flat generation, yet its median QPS differs from the
control by approximately 50%. Independent benchmark/compiler activity and
restart/warmup effects are sufficient concerns that this matrix cannot cleanly
rank treatment QPS or establish recovery against historical qualified runs.
The observed control/candidate ordering is recorded, not promoted as causal.
Restart-and-query peak RSS medians span 5.012–5.278 GB; sampled physical
footprint medians span 1.497–1.579 GB. These are distinct measurements, and no
all-metric memory win is established. All four experiments remain default-off.

The next targeted recovery work, based on these results, is:

1. Decouple direct unfiltered/fused native RaBitQ scoring from the presence of
   posting-local float16. Reuse leased member IDs, stream the same ordered
   scores into the existing candidate gate, and keep shared-store projection
   completion. First test exact candidate/order/bound parity, then benchmark
   the complete leaf path and matched public queries. Do not claim an expected
   percentage recovery from reduced memory traffic alone.
2. Fix the page prototype's cross-page/oversized fallback scheduling: it
   currently performs those reads serially while collecting grouped requests.
   They should join the same bounded worker queue. If pages still amplify I/O,
   test single-copy projection chunks clustered by co-access, with stable
   vector/revision indirection and generation-safe publication, instead of
   enlarging the cache or restoring a second float16 plane. A shared store
   cannot provide perfect locality for every independently partitioned index;
   measure that layout tradeoff explicitly.
3. Retain phase-specific queues as an experiment, but evaluate aggregate
   CPU/bandwidth demand across scanning, rerank callers, and helpers. Less
   waiting in one queue is not sufficient if the admitted service work or
   total active worker demand becomes more expensive.
4. Split capture-completion attribution further into patch construction,
   append/fsync, and delta-generation publication before changing durability
   or extending coalesced capture lifetimes. The current coalescer preserves
   the original total byte envelope; it does not by itself prove fewer durable
   publications or a faster 1M drain.

Tighter routing remains the higher-upside subsequent experiment: the current
1M routing metadata is complete, but its bounds overlap and it scans 2,048
leaves. Benchmark improved partition/bound quality at unchanged recall,
alongside a fixed-work kernel comparison; do not treat lower effort as a fix.

#### Recovery v2: independent scoring, I/O, admission, layout, and routing experiments

The next user request includes the larger routing opportunity, not only the
four previous runtime treatments. The following default-off experiments are
implemented for independent measurement:

| Refinement | Switch / change |
| --- | --- |
| `fused` | `ANTFLY_EXPERIMENT_FUSED_NO_COPY`: reuse immutable member IDs and stream ordered RaBitQ scores into the existing eight-candidate gate without requiring a local float16 plane. |
| `queued_pages` | `ANTFLY_EXPERIMENT_GROUPED_FALLBACKS` plus the page switch: enqueue oversized/cross-page reads with ordinary page groups instead of serially executing them during grouping. |
| `aggregate` | `ANTFLY_EXPERIMENT_AGGREGATE_ADMISSION` plus phase admission: a CPU-count-derived aggregate slot pool is shared by query callers and optional read helpers. |
| `angular` | `ANTFLY_EXPERIMENT_ANGULAR_BOUNDS`: tighten cosine posting bounds using spherical-cap geometry and the already-persisted conservative normalized chord radius. |
| `clustering` | `ANTFLY_EXPERIMENT_PROJECTION_CLUSTERING`: reorder existing projection bytes inside bounded streaming pages by a query-independent sparse-hyperplane similarity key. |
| `capture` | `ANTFLY_EXPERIMENT_CAPTURE_STAGES`: separately time patch preparation, durable WAL work, base handoff, immutable publication, and cleanup; split WAL encoding from append/fsync. |

The fused sink preserves insertion order, error intervals, ties, and the
shared-store authoritative completion path. Projection-carrying and plain
candidate sinks use compile-time specialization. Filtered/nonquantized paths
retain their existing implementation. Aggregate slots are acquired before
pinning a generation or obtaining scan bandwidth; helpers never wait and may
not bypass queued callers. Slots cover the caller through both query phases,
including its serial reads. This governs query drivers/helpers, not all node
CPU consumers or an assertion that blocked I/O consumes physical CPU. Admission
ordering and separately accounted memory remain explicit.

For unit vectors, if alpha is the query/centroid angle and theta is the cap
angle, all members have angle at least max(0, alpha-theta). The new bound
evaluates the cosine of that difference in f64 without inverse trigonometry,
with conservative input/output guards. Invalid radii retain fallback behavior.
The flat routing rank and effort are unchanged; only certified stopping bounds
are tightened. The ordinary triangle/chord bound remains the control. This
tests bound tightness without changing the durable radius format or assuming
that tighter bounds will actually stop on these corpora.

The layout experiment is deliberately bounded: hash-shard/index ordering,
vector/revision identity, payload checksums, and exact residuals stay unchanged.
The existing offset-based format permits a different projection arena order.
Streaming pages remain 1 MiB, the permutation metadata is capped at 64 KiB, and
the existing output arena is used rather than another temporary payload plane.
There is no persistent payload duplication or format-version change. The
similarity key is a semantic co-access proxy, not a learned query workload:
neither benchmark queries nor ground truth train it. This is **within-page**
clustering, not a claim that global cross-shard co-access layout is implemented.
Its effectiveness must be measured before investing in that broader format
change, particularly because each source store may serve multiple ANN indexes.

Debug validation: fused plain/local candidate parity, conservative angular
bounds over a grid of sphere/cap/member directions, and reordered f16/f32
exact reconstruction with tombstones and identical persisted size pass (three
tests; the ReleaseFast-only microbenchmark is skipped). Test-count inspection
caught lazy import discovery of the angular test; the module now explicitly
discovers it. Storage tests cover oversized fallback work with no helper lanes,
shared caller/helper capacity, FIFO cancellation, and native admission; the
expanded focused storage run passed with zero leaks. All 76 API table-read
tests pass with all new switches enabled. Seventeen Python runner/report tests
pass. These are focused checks, not whole-PR CI certification.

The performance build is staged at `.benchmark-assets/pr593-recovery-v2`.
`run_dense_recovery_query_ab.py` accepts an explicit independent-mode list and
`--warmup-count 1000` to warm the same complete query set before C1/C30 timing.
It rejects layout treatments on query-only clones. Fresh layout qualification
uses `run_posting_locality_ab.py --refinement clustering --capture-stages`;
capture tracing is identical in both arms. `summarize_dense_capture_stages.py`
keeps WAL sub-timers separate from their parent capture duration. No default
promotion or performance-recovery claim has been made before measurement.

The ReleaseFast build completed successfully (27/27 steps). The pinned binary
SHA-256 is
`0b76b5ea5760c91f3fc55e56cf9b432c039248274685b0c3849a79b21d612461`.
Post-build hashes of the eight modified native implementation files matched
their pre-build values. Query diagnostics use independent `control`, `fused`,
`pages`, `queued_pages`, `phases`, `aggregate`, and `angular` arms in forward
then reverse order, each with 1,000 fixed warmup queries and 30 seconds of C30
measurement. The aggregate driver queue's wait is included in
`hbc_admission_wait_ns`; interpreting just scan/rerank waits would hide that
new queue. The report now includes this timer, leaf-scoring time, and fixed-set
routing counters. A report regression test excludes failed arms and keeps
fixed-query metrics separate from time-limited throughput samples.

##### Recovery v2: completed 50K same-data attribution

All 14 arms passed under
`.benchmark-results/pr593-recovery-v2-query-50k-20260906`. These are profiled
public-API diagnostics, **not** fresh VectorDBBench qualification QPS. Every
fixed 1,000-query set produced 98.302% recall, 23,847.284 approximate scores,
135.738 authoritative completions, and 208 leaves/query. Runtime treatments
therefore did not change this workload's candidate work or recall.

| Treatment | C1 diagnostic QPS | C30 diagnostic QPS | HTTP p95, ms | Dense-server p95, ms | Peak RSS, GB | Peak physical footprint, GB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Control | 228.36 | 1,447.43 | 46.22 | 23.99 | 1.023 | 0.421 |
| Fused no-copy | 219.01 | 1,392.53 | 47.68 | 23.15 | 1.120 | 0.418 |
| Pages | 217.65 | 1,504.15 | 44.08 | 27.54 | 1.078 | 0.450 |
| Queued pages | 224.27 | 1,477.56 | 43.98 | 26.91 | 1.171 | 0.452 |
| Phase admission | 223.99 | 1,408.86 | 44.93 | 22.16 | 1.040 | 0.403 |
| Aggregate callers/helpers | 199.28 | 1,618.20 | 39.86 | 25.04 | 0.895 | 0.307 |
| Angular bounds | 205.16 | 1,272.74 | 56.62 | 18.78 | 0.954 | 0.473 |

Values are two-arm medians; decimal GB. Aggregate admission improves median
C30 QPS by 11.8% and HTTP p95 by 13.7%, but lowers C1 QPS by 12.7%. Its two
C30 samples were 1,698.38/1,538.01 versus control 1,393.15/1,501.70. In the
reverse pair its p95 was 42.91 ms versus control 41.90 ms. This is not a
repeatable all-metric win or justification for a default change.

Fused leaf-scoring mean remains approximately 1.31 ms versus control 1.30 ms;
the removed scratch writes did not produce a measured recovery. Queuing page
fallbacks modestly improves C1 over serial-fallback pages but does not clearly
improve C30. Both page variants reduce reads from approximately 438.7 to 379.4
per query while increasing bytes from 1.273 to 1.534 MB. Angular bounds turn
208 leaf proof fallbacks into resolved bounds, but the mutable tree's internal
frontier still cannot certify stopping; candidate work remains identical.
No routing speedup is claimed. Other user-owned test/compiler activity ran on
the host; HTTP and internal dense timers cover different intervals.

Fresh layout qualification will use
`--refinement clustering --common-refinement queued_pages --capture-stages`.
Both arms thus have the same reader and tracing; only physical projection
packing changes. Common treatments are recorded in receipts, and overlapping
common/A-B switches are rejected so a nominal experiment cannot silently
become inert. The expanded Python suite passes 20 tests. The 1M query matrix
uses the same executable and mode definitions; only the runner gained this
fresh-layout configuration option after the 50K matrix completed.

##### Recovery v2: completed 1M same-data attribution

All 14 arms passed under
`.benchmark-results/pr593-recovery-v2-query-1m-20260906`, using the same pinned
executable. The fixed 1,000-query sets all reached 99.083% recall. This clone
contains earlier mixed updates; restart repairs native posting debt before
public readiness, and approximately one leaf/query still takes the supported
stale-payload fallback. Approximate versus exact counts therefore vary slightly
with repair grouping (approximately 240K approximate scores/query), rather
than providing byte-identical work across independent server lifetimes.

| Treatment | C1 diagnostic QPS | C30 diagnostic QPS | HTTP p95, ms | Dense-server p95, ms | Peak RSS, GB | Peak physical footprint, GB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Control | 90.72 | 748.14 | 82.88 | 64.96 | 5.276 | 1.440 |
| Fused no-copy | 92.15 | 760.43 | 83.24 | 65.20 | 5.260 | 1.436 |
| Pages | 88.95 | 745.81 | 85.49 | 68.68 | 5.294 | 1.474 |
| Queued pages | 89.52 | 744.56 | 87.21 | 70.57 | 5.287 | 1.449 |
| Phase admission | 90.45 | 766.95 | 83.79 | 68.22 | 5.252 | 1.426 |
| Aggregate callers/helpers | 85.69 | 777.44 | 64.95 | 44.66 | 5.159 | 1.339 |
| Angular bounds | 90.25 | 747.36 | 83.01 | 65.15 | 5.257 | 1.439 |

Aggregate admission's two C30 samples were 776.11/778.78 QPS and
64.84/65.06 ms p95, against control 747.14/749.13 QPS and 83.61/82.15 ms.
The median differences are +3.9% QPS, -21.6% HTTP p95, -31.2% dense-server p95,
-2.2% RSS, and -7.0% physical footprint. However, C1 QPS is 5.5% lower. Mean
artifact work falls from 8.25 to 4.08 ms and leaf scoring from 8.21 to 6.90 ms,
while the new driver queue waits 9.74 ms/query. Do not mistake moving work into
admission for eliminating that wait, or add nested stage percentiles. This is
a repeatable high-concurrency tradeoff on this corpus, not an all-metric win.

Fused scoring improves median QPS only 1.6%, with essentially unchanged p95;
leaf scoring changes from 8.21 to 7.95 ms. Pages and queued pages retain only
approximately 3.6% fewer physical reads (617.27 to 594.80/query) while increasing
bytes 16.4% (0.910 to 1.059 MB/query). Fixing serialized fallbacks alone does
not recover throughput. Phase-only admission reduces mean scan/rerank waiting
from 4.75 to 1.39 ms, but artifact/scoring service grows and p95 does not improve.

The routing experiment is decisive about this particular bound change: both
angular runs retain 2,048 leaves/query and zero certified stops. In all 1,000
queries, the final unvisited-frontier minimum lower bound is exactly zero;
the retained top-k upper endpoint averages 0.17526. A covering cap that already
contains the query has zero lower bound under both formulas. Different bound
arithmetic cannot repair those broad caps. The larger routing opportunity
therefore remains better-separated partitions or smaller, independently
bounded subgroups, with dirty-state fallback and unchanged recall checks—not
lowering effort or promoting this ineffective angular switch.

These timings are much more stable than the preceding un-warmed matrix but
remain profiled query diagnostics, not fresh VectorDBBench load/mixed/restart
qualification. Existing user-owned tests ran concurrently. No experiment has
been made a default, and no historical qualified-QPS recovery is asserted from
this matrix. Fresh layout/capture qualification is recorded separately.

##### Recovery v2: fresh 50K layout and capture qualification

All four 50K arms passed in
`.benchmark-results/pr593-recovery-v2-layout-20260906` (execution on September
7; the artifact-root date was chosen before the launch). The reader uses
queued pages in both arms; capture tracing is also common. Batch 100, four
writers, `sync_level=write`, public API queries, mixed writes, and restart
qualification remain unchanged. The public index list was verified to contain
only `vec`; default full-text removal completes before loading.

| Metric | Control median | Clustered median |
| --- | ---: | ---: |
| Readiness | 16.250 s | 16.772 s |
| Insert | 11.216 s | 10.739 s |
| C1 QPS | 248.62 | 265.23 |
| C30 QPS | 996.62 | 1,186.79 |
| C30 p95 | 85.82 ms | 79.69 ms |
| Recall | 98.360% | 98.325% |
| Allocated disk after restart | 372.55 MB | 374.94 MB |
| Load/read-only peak RSS | 1.432 GB | 1.474 GB |
| Mixed peak RSS | 1.676 GB | 1.633 GB |
| Restart peak RSS | 0.685 GB | 0.809 GB |
| Mixed write rows/s | 1,462.99 | 1,803.95 |
| Mixed write p95 | 499.68 ms | 378.85 ms |
| Mixed catch-up | 1.958 s | 1.590 s |
| Post-restart physical reads/query | 379.709 | 379.187 |

Another user-owned benchmark ran concurrently, including overlapping
ingestion/query phases. These QPS/latency differences are **not** sufficient to
attribute a packing speedup. The physical-read change is only 0.14%; the first
pair's physical bytes were 1.538/1.533 MB/query with 304.017/303.076 projection
requests. Much of that difference is explained by requested candidate work,
not coalescing. Fresh online trees also differ slightly in their partitions.
Fixed-duration mixed phases perform different amounts of work, so their final
allocated disk is not a controlled per-vector format-size comparison.

An independent read-only check verified the prototype really changes the
physical arena: three source blocks retain identical sizes and valid
header/index/footer CRCs, while projection-offset descents change from zero in
control to 197/223/211 in the candidate. The key index remains ordered; exact
reconstruction and equal persisted size are covered by the codec tests.
Within-page similarity packing is not yet a demonstrated locality win.

Initial-load capture attribution uses `--through-sequence 501`, excluding the
later mixed writes. Across the four arms, inner posting-capture totals are
318.16/320.10/365.16/324.84 ms. Patch preparation accounts for
287.89/288.20/333.35/291.04 ms (approximately 90%); WAL work accounts for
25.47/26.97/26.38/28.14 ms; generation publication is only 3.85--4.43 ms in total.
The nested append/fsync totals are 9.34--12.04 ms, and WAL encoding is
14.44--16.65 ms. There are six or seven completed captures and no incomplete
captures in these initial-load intervals. Coverage-only WAL appends are counted
separately, so WAL append count need not equal capture count.

This establishes patch preparation as the main **inner posting-finalization**
cost, not as the explanation for the entire 16-second load. The outer capture
also certifies/publishes shared-vector mutations and performs checkpoint
maintenance. Existing shared-vector logs show roughly 100-ms certification
events; the new posting timers do not include those. Do not relax fsync or
extend source capture lifetimes to optimize a small part of total ingestion.
The next patch optimization should first separate base resolution from generic
replacement matching, retaining exact base/result checksums and source leases.
The 1M layout arms began only after all four 50K qualifications passed.

##### Recovery v2: completed fresh 1M layout and capture qualification

All four 1M arms also passed, completing the eight-arm fresh qualification.
Both orderings passed public readiness, query/recall, mixed-write/catch-up,
and cold/warm restart gates. No capture/publication/quarantine failure was
found in the initial or reopened server logs. These are the same common
queued-page reader and capture tracing as the 50K matrix; physical clustering
is the only A/B switch. Production defaults remain unchanged.

| Metric | Control median | Clustered median |
| --- | ---: | ---: |
| Readiness | 338.781 s | 387.797 s |
| Insert | 251.968 s | 265.269 s |
| C1 QPS | 80.97 | 67.92 |
| C30 QPS | 465.60 | 556.30 |
| C30 p95 | 144.76 ms | 128.76 ms |
| Recall | 99.025% | 99.070% |
| Allocated disk after restart | 3.806 GB | 3.818 GB |
| Load/read-only peak RSS | 5.809 GB | 5.602 GB |
| Mixed peak RSS | 5.094 GB | 4.693 GB |
| Restart peak RSS | 5.181 GB | 5.113 GB |
| Mixed write rows/s | 1,763.41 | 1,776.73 |
| Mixed write p95 | 519.71 ms | 498.95 ms |
| Mixed catch-up | 89.061 s | 86.522 s |
| Post-restart physical reads/query | 598.533 | 594.523 |

Control runs were 361.593/315.970 seconds to readiness and
438.53/492.68 C30 QPS; clustered runs were 384.895/390.700 seconds and
531.81/580.79 QPS. Clustering is slower to load in both pairs. Physical reads
fall only 0.67%, while projection requests also fall from 475.79 to 471.38
(0.93%); approximate work differs by 0.16% between the independently built
trees. This is not evidence of a material coalescing gain. Source-vector and
serving-vector persisted allocated sizes are identical between these arms;
the final total-disk difference is in other state after different amounts of
fixed-duration mixed work, not another full projection plane.

The concurrent user-owned benchmark remained active during these runs.
Additionally, the first control received a three-second read-only `sample`
during final ingestion maintenance, before query timing; the sparse stacks
landed in vector-base publication and did not isolate patch matching. That
load sample is not pristine. Preserve these receipts as qualification and
diagnostic evidence, **not** a causal layout speedup or a recovery of the
historical approximately 874-QPS qualified no-copy median. In particular,
profiled same-data diagnostic QPS must not be compared directly with this
unprofiled fresh VectorDBBench sweep.

For source coverage through sequence 10001 (excluding subsequent mixed
mutations), initial-load inner posting-capture totals in run order were
25.662/25.458/24.196/21.253 seconds. Patch preparation accounted for
23.174/23.946/22.595/19.854 seconds; WAL work for
2.127/1.232/1.247/1.144 seconds; publication for
0.310/0.224/0.299/0.204 seconds. All 81/82/82/82 captures completed.
These sequence-bounded reports were captured at readiness; coverage-only WAL
records and idle work at the same source sequence need not share capture counts.

The first control's existing outer `finalize_ns` counter was 53.765 seconds,
versus 25.662 seconds in the new inner capture timer. The inner patch stage is
only 6.4% of its 361.593-second readiness time. Base resolution, replacement
matching, shared-vector certification, and remaining replay/build work must
be distinguished before claiming a load-time recovery. Allocation-free
publication and stronger fsync batching alone cannot explain this gap.

Decision: retain all treatments as default-off experiments. Aggregate
caller/helper admission is the strongest repeatable 1M high-concurrency tail
tradeoff; fused no-copy is small and mixed; queued fallbacks do not cure sparse
reads; within-page clustering does not establish locality or ingestion gains;
angular arithmetic does not shrink the routing shell. Neither full cross-shard
single-copy co-access packing nor a new durable subgroup routing format has
been implemented or qualified by these results.

##### Recovery v2: larger routing opportunity, bounded subgroup feasibility

`scripts/probe_dense_subgroup_bounds.py` now preserves an offline structural
experiment in the repository. It trains 64 spherical coarse partitions from
at most 4,096 source rows, streams assignment in 1,024-row batches, and trains
1/4/16 subgroups per parent. Training uses only source embeddings, never query
vectors or benchmark ground truth. The probe retains a bounded corpus sample;
it is not the complete streaming production builder. BLAS concurrency is one.

Artifacts are in `.benchmark-results/pr593-recovery-v2-routing-probe-20260907`:
`openai-50k.json` uses all 50,000 source rows; `cohere-1m-sample.json` uses the
first 32,768 shuffled source rows. Both evaluate 128 held-out query rows at
top-k 100. Sample/script hashes and seed 593 are recorded. PyArrow emitted
sandbox CPU-cache discovery warnings but both probes completed successfully;
these are geometry/quality experiments, not timing qualification.

There are two deliberately separate measurements:

1. **Certified-bound feasibility:** use the true sampled-corpus top-k threshold
   as an optimistic oracle, then count whole groups whose conservative
   spherical-cap bounds still overlap it. Every group bound is checked against
   the authoritative minimum distance of all its members for every query.
   This validates the probe's evaluated bounds, not production floating-point
   code, dirty-state recovery, or a full 1M index.
2. **Approximate routing quality:** order groups by their trained centroid,
   consume whole groups to each candidate budget, and measure exact neighbor
   coverage. Whole-group budget overshoot is included. This curve is not
   certified stopping and cannot justify lowering production effort.

| Corpus | Subgroups/parent | Actual groups | Vectors retained by oracle bounds | Recall at approximately 35% routed work | Centroid/radius metadata |
| --- | ---: | ---: | ---: | ---: | ---: |
| Full 50K | 1 | 64 | 100.000% | 96.133% | 0.393 MB |
| Full 50K | 16 | 1,024 | 99.974% | 98.164% | 6.296 MB |
| 32,768-row 1M sample | 1 | 64 | 99.994% | 94.398% | 0.197 MB |
| 32,768-row 1M sample | 16 | 921 | 99.548% | 98.031% | 2.833 MB |

At approximately half-corpus routed work, subgroup recall increases from
98.359% to 99.133% on 50K and from 97.555% to 99.281% on the 1M sample.
On 50K, the 16-way layout reaches 98.164% at 17,543.95 vectors/query versus
the coarse layout's 98.359% at 25,497.30. That is 31.2% less sampled routing
work within 0.20 percentage points of recall **inside this offline experiment**,
not a comparison with the deployed online tree or measured QPS.

This sharpens the recommendation. Better-trained representatives can improve
recall per routed vector, but smaller spherical caps alone still retain almost
the entire corpus under a true top-k bound. Moving from 64 to 1,024 groups also
makes a naive full-dimensional directory scan 16 times larger (98,304 to
1,572,864 centroid coordinates/query at 50K). A durable rewrite premised on
large certified-stop gains would be premature. The next serving experiment
should use a bounded sampled trainer plus streamed assignment and a cheap
quantized representative directory, measuring total routing-plus-candidate
cost at matched recall. It must retain coherent generation identities,
dirty-group fallback, cancellation, and exact boundary completion. Full
cross-shard source packing remains a separate single-copy ownership problem.

The probe has tests for conservative member coverage, degenerate antipodal
centers, invalid vectors, and retaining exact ties. Together the runner/report
Python suites now pass 23 tests; Black/Ruff and `git diff --check` pass. The
native executable is unchanged from the pinned ReleaseFast build and focused
Debug validation above. No experimental mode is promoted to a default.

##### Recovery v3: selective enablement and quantized native routing

Carry **aggregate caller/helper admission plus phase admission** into the next
experimental baseline, not every recovery-v2 switch. It is the strongest
repeatable C30 latency tradeoff, but its C1 regression still prevents an
unconditional production-default promotion. Fused no-copy, page caching,
queued-page fallbacks, angular bounds, clustering, and capture tracing stay off
in the next query A/B. Common treatments are now supported and recorded by
`run_dense_recovery_query_ab.py`; overlapping common/A-B flags are rejected.

Before expanding the number of trained representatives, isolate the price of
their directory scan: `ANTFLY_EXPERIMENT_QUANTIZED_ROUTING=1` converts native
exact centroid blocks into the existing cross-platform RaBitQ representation.
It changes neither source-vector precision nor authoritative exact completion,
the ANN effort, leaf topology, nor the durable format. Small indexes keep their
existing tree route; the experiment targets the scale-selected flat path.

The conversion uses the existing single-flight, generation-bound, resource-
reserved directory builder. It quantizes only new exact blocks; already-
quantized parent blocks remain borrowed with their shadow masks and retained
parent/generation leases. A changed centroid delta does not rebuild or
requantize the full parent directory. Owned exact overlays release their
float32 working copies after conversion; immutable base centroid files remain
the durable authority. No document/vector fetches are introduced. Cancellation
is checked between blocks and before each ownership transfer. Failed builds
are not published to shared caches.

Persisted block sizes and large live overlays can exceed the current runtime
conversion block setting. Those blocks deliberately retain exact routing;
conversion never allocates a normalized matrix beyond the reserved block
workspace. The allocation-failure test also exercises this fallback before
converting the same data with a sufficient block limit. The initial optimized
build was stopped before measurement to include this review safeguard.

Tests cover borrowed-to-owned conversion, unchanged parent sharing, delta
shadow masks, generation identity, cancellation, every injected allocation
failure, and exact complete-coverage queries across a source mutation/restart.
The failure sweep exposed two existing cleanup gaps: cosine/inner-product
quantization leaked centroid-dot-product scratch if the final centroid copy
failed, and layered-directory construction leaked its transferred overlay if
allocating the backing lease failed. Both now unwind ownership correctly;
successful-path work is unchanged. Explicit filtered-test discovery for the
SPFresh module was added so these tests cannot silently run an empty suite.

Debug validation passed: four focused directory tests (including exhaustive
allocation failures), the native delta/restart integration test, 20 broader
flat/fused/angular tests (one performance-only test skipped), and all 76 API
table-read tests with routing/admission switches enabled. Python runner/report
checks pass. The next measurement is a warmed, reversed-order same-data 1M
query A/B with aggregate admission common to both arms. Results are pending;
this is the cheap-directory prerequisite, not an implemented durable trained-
subgroup layout or a performance-win claim.

Build infrastructure interruption: the shared `/tmp/zig-local-cache` was
cleared during the ReleaseFast link, leaving missing `kmeans_metal.o` inputs
for both the inference runtime and storage kernel. Inspection found the whole
shared cache reduced to 4 KB. This was not a source/test or query failure.
The retry uses private `local-cache` and `global-cache` directories beneath
`.benchmark-assets/pr593-quantized-routing-20260907`; no other build/cache was
stopped or modified. Correctness checks completed before that cache loss.

The isolated ReleaseFast build completed successfully (27/27 steps; storage
kernel compile nine minutes, peak compiler RSS 11 GB). The executable SHA-256
is `2caf30ca6434f8a6f416e93ff0dd554de88c7f8c3e5f3e321a7f850a223662b3`.
All five modified implementation/test-discovery file hashes matched their
pre-build values. The query matrix additionally includes unadmitted controls:
`control`, `aggregate`, `quantized_routing`, and `admitted_quantized_routing`,
then reverse order. This preserves the main one-factor admitted comparison
while checking whether combined routing/admission recovers C1 against defaults.
The two new runner guards bring the focused Python total to 25 passing tests.
Measurements are under `.benchmark-results/pr593-quantized-routing-query-1m-20260907`.

All eight query diagnostics completed without request errors. **Zero-centered
quantized routing fails the recall gate**: both orderings lose approximately
1.58 percentage points on the same fixed 1,000 queries. Completion receipts
are diagnostic execution success, not performance/recall qualification.

| Mode | C1 diagnostic QPS | C30 diagnostic QPS | HTTP p95 | Dense p95 | Routing mean | Fixed-set recall | Peak RSS | Peak footprint |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Defaults | 86.08 | 606.57 | 97.93 ms | 77.19 ms | 3.600 ms | 99.0835% | 5.160 GB | 1.502 GB |
| Aggregate admission | 76.56 | 637.11 | 87.83 ms | 62.06 ms | 2.429 ms | 99.0830% | 5.054 GB | 1.423 GB |
| Zero-centered routing | 75.75 | 676.28 | 90.69 ms | 72.87 ms | 2.208 ms | 97.5095% | 4.911 GB | 1.514 GB |
| Admission + zero-centered routing | 89.44 | 731.35 | 71.30 ms | 51.02 ms | 1.500 ms | 97.5100% | 4.891 GB | 1.489 GB |

These are two-arm medians, decimal GB, and profiled same-data public queries,
not fresh VectorDBBench qualification. Active user-owned work continues to
affect the host. Admission's individual C30 samples were 725.03/549.19 QPS
and 70.94/104.73 ms p95, versus default 618.25/594.89 and 95.34/100.53.
Its median is encouraging but the reverse pair is not a win. Do not promote
it universally from this matrix, or treat reduced RSS as reduced footprint:
unadmitted quantized routing slightly increases the latter. The quantized
variants remain rejected regardless of their throughput.

The targeted follow-up is `ANTFLY_EXPERIMENT_CENTERED_ROUTING`, used together
with `ANTFLY_EXPERIMENT_QUANTIZED_ROUTING`. Instead of quantizing each block's
centroids about zero, compute its arithmetic mean and quantize residuals about
that mean using the existing RaBitQ centroid norm/dot corrections. The mean
is not normalized. Construction streams each bounded block into one D-sized
f64 accumulator, explicitly added to the cold-build reservation; there is no
additional per-vector payload plane. Parent sharing, masks, oversized exact
fallback, cancellation, and source/generation identities are unchanged.

Both centered and zero-centered conversion now pass the exhaustive allocation
failure sweep, including checking the expected stored centers. The native
complete-coverage delta/restart test passes with centered routing. Ten broader
storage routing/admission tests also pass, including single-flight failure,
reservation transfer, last-reference accounting, and unpublished-state rejection.
The separate ReleaseFast build completed 27/27 steps. Its executable SHA-256 is
`b9e0b69183e1d504345fb7792cfd8a2ac086bd327bc9320224879c2ae424e671`.
Four of the five tracked source hashes remained unchanged during compilation;
`hbc_adapter.zig` changed in this shared worktree, so this is a binary-pinned
diagnostic, not a source-pinned release qualification. Both arms use that same
executable. The zero-centered executable/results remain preserved separately.

The follow-up uses `control,centered_routing` with `aggregate` as the common
treatment, then reverse order, under
`.benchmark-results/pr593-centered-routing-query-1m-20260907`. Here **control
means admitted exact routing**, not production defaults. All arms use the same
offline 1M generation and fixed 1,000-query warmup/recall set. Centered
recall/performance is unproven until this A/B completes; a promising result
would still require clean-build fresh-load/mixed-workload qualification.

The first centered control could not reach readiness:
`UnsupportedVectorBlockManifestVersion`. The shared branch now accepts manifest
V6, whereas the saved benchmark manifests inspected were V3/V4. The owned
server was stopped and the failed receipt preserved; no query samples from
that attempt count. No old manifest bytes or compatibility rules were changed.
A fresh public-API 1M baseline is being built at
`.benchmark-results/pr593-centered-routing-fresh-1m-20260907b` with the frozen
centered-capable executable, all new routing/admission treatments off, batch
100, four load workers, vector-store source ownership, and single-copy float16
projection storage. The public index list contains only `vec`. An earlier
fresh attempt without listener permission failed before loading any rows.
The new baseline is necessary to resume the same-data routing experiment; it
must not be compared directly to older-format load timings as an isolated
routing change.

The fresh baseline reached 1M-vector readiness in **358.3199 s** (228.3264 s
insert + 129.9935 s catch-up), with no dirty postings or pending projection.
Its initial official C1/C30 curve was 70.77/726.27 QPS, C30 p95 90.77 ms, and
serial recall 99.040%. This was a 15-second-per-concurrency baseline run, not a
matched fresh-load treatment comparison. Mixed writes were disabled.

The wrapper initially stopped after that successful live stage because a
relative `run_root` became relative to the client checkout after `cd`. The
result was found under `vector-source-client/.benchmark-results/.../results`
and independently passed the unchanged validator (1M rows, serial recall,
complete C1/C30 metrics, lifecycle log). The wrapper now canonicalizes the
created root before passing result/log paths to the client. Existing misplaced
evidence was preserved. Deferred official restart passes use suffix `-pathfix`.

The centered query A/B completed all four independent restart/query arms under
`.benchmark-results/pr593-centered-routing-query-fresh-1m-20260907`. Both modes
use aggregate admission; only the centered mode enables the two routing flags.
Same frozen executable, same offline fresh 1M source, fixed 1,000 queries,
unchanged effort, and 30-second C30 profiled diagnostics:

| Mode | C1 diagnostic QPS | C30 diagnostic QPS | HTTP p95 | Dense p95 | Routing mean | Fixed recall | Peak RSS | Peak footprint |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Admitted exact routing | 79.91 | 634.78 | 92.47 ms | 61.49 ms | 2.538 ms | 99.040% | 4.591 GB | 1.477 GB |
| Admitted mean-centered RaBitQ | 85.79 | 678.94 | 84.56 ms | 55.97 ms | 1.590 ms | 98.846% | 4.633 GB | 1.470 GB |

These two-sample medians suggest +7.4% C1 QPS, +7.0% C30 QPS, -8.6% HTTP
p95, and -37.3% routing time, at -0.194 recall percentage points. RSS is +0.9%
and footprint -0.5%; neither establishes a meaningful memory improvement.
Both centered arms reproduce the same fixed-set recall, within the one-point
budget. Exact completion stays narrow: 145.27 versus 145.30 vectors/query.
Candidate work is essentially unchanged: 239,498 versus 239,612 approximate
scores/query, 2,048 leaves, and zero certified stops. No disk format changed
between these routing treatments; disk/load savings are not claimed.

**Do not promote from the medians alone.** First-order C30 exact/centered was
523.47/646.75 QPS and 113.87/89.12 ms p95, but reverse-order centered/exact
was 711.13/746.09 QPS and 80.01/71.08 ms p95. Routing time and C1 improved in
both pairs; overall C30/p95 did not. This is useful evidence for a cheaper
representative directory, not a repeatable end-to-end throughput win. Keep
aggregate admission and mean-centered routing as explicit experimental
candidates, reject zero-centered routing, and leave all production defaults
unchanged. The next substantive step remains trained subgroup representatives
to reduce the approximately 240K candidate shell, with coherent generation
ownership and recall qualification; that durable subgroup layout is not yet
implemented here.

The deferred baseline restart checks completed successfully with the relative
root and fixed wrapper: official cold/warm recall both 99.040%, serial p95
16.0/12.8 ms, reopened C1/C30 92.36/620.44 QPS, and C30 p95 101.58 ms.
Results now land in the correct top-level `results/Antfly` directory. The
separate 1,000-query profile reproduced 99.040% recall. This verifies the
wrapper fix and baseline restart path, not a centered mixed-ingest workload.
Final Python recovery tests (10/10), shell syntax, and `git diff --check` pass.
No experimental production defaults were enabled, and no commit/push was made.

#### Generation-bound balanced subgroup layout: implementation, qualification pending

The next default-off experiment now persists deterministic, balanced cosine
subgroups in each immutable quantized leaf. A framed, authenticated row
permutation binds existing RaBitQ columns and member IDs to 4/8/16 source-space
representatives. It does **not** add another float16 projection plane or change
authoritative vector ownership. Mutation reconstruction inverse-maps the serving
order; generation leases, WAL coverage and existing dirty-leaf shadowing retain
their existing authority. Degenerate, oversized, missing-source or memory-denied
training falls back to the ordinary leaf representation.

The opt-in ANN path ranks these representatives, scores the selected three
quarters of groups through the existing fused selector, and reports skipped
vectors and routing time. These representatives are **not certified bounds**:
skipping disables claims of exhausted candidate coverage and full-leaf suffix
stopping. Full-coverage and filtered queries bypass this heuristic. Production
defaults remain unchanged. The acceptance gate is matched recall loss no greater
than one percentage point, with latency, throughput, RSS/footprint, load/catch-up,
mixed writes and allocated disk measured separately; fewer scores alone is not
a win.

Debug checks passed: 13 vector-index/codec/fused-scoring tests, the native
subgroup full-coverage/mutation/restart integration test, and 76 API table-read
tests. The codec check exercises allocation failures, canonical byte parity and
buffered/streaming equivalence. The test fixture uses the accelerated
`antfly_hash.Crc32`, not the standard-library checksum. Runner tests also passed.

Training is bounded to one leaf (4,096 rows/dimensions and at most 16 groups).
Its explicit peak admission is deliberately conservative and overlaps actual
allocations already charged by the checkpoint allocator; this can suppress
optional training under pressure. It must not be interpreted as measured demand
or an optimized final accounting scheme. Query admission retains conservative
full-leaf charges plus subgroup metadata until the experiment is qualified.

A separate ReleaseFast build is underway in the existing worktree, with a
dedicated output prefix and recorded pre-build source hashes. No serving
performance or recall result for this layout is established yet.

Follow-up review extended the integration test to delete/reinsert an existing
vector, hold an older read transaction across the mutation, compare its original
quantized bytes and metadata, and verify the replacement's exact top-1 result
both before and after reopen. That Debug test passes without leaks. Fifteen
fresh-runner tests and seven recovery-runner/summary tests pass; conflicting
physical subgroup flags are now rejected before an A/B starts.

The build-time source audit detected a concurrent `hbc_adapter.zig` edit before
this additional test-only change. Other 3,755 recorded files were unchanged at
that audit. The resulting experiment must be described as executable-pinned,
not a clean source-pinned release qualification.

The first executable (`bc446e965ffd3828040e05fb91d68659ec31330bfdd134d035fa92366a73d170`)
completed a fresh 50K control/candidate pair under
`.benchmark-results/pr593-subgroups4-fresh-50k-20260907`. **This pair does not
qualify subgroup routing:** the candidate's post-restart 1,000-query profile
reported zero subgroup leaves/skips. Physical segments did contain subgroup
records, but automatic routing selected the tree path at 50K, while the new
selection branch existed only in flat routing. The second candidate was stopped
with SIGINT to its verified owned process group. Original data/receipts and a
`treatment-audit.json` explaining the invalid treatment are preserved.

Diagnostic-only first-pair numbers (control/candidate): readiness
14.64/17.63 s; C30 1,861.8/1,599.4 QPS; p95 30.60/37.20 ms;
recall 98.30/98.42%; allocated disk 371.6/394.2 MB; sampled live read-only
RSS 1.698/1.554 GB; live physical-footprint ledger peak 680.4/825.2 MB.
These show the cost of the persisted layout without its intended query
treatment, not evidence for or against subgroup selection quality. The live
footprint high-water mark includes load and mixed updates; it is not a
read-only footprint measurement. No 1M scale-up or promotion is justified by
these results.

The fix moves selection into the common native-leaf scorer used by tree and
flat traversal. Both paths retain filtered/full-coverage fallbacks and report
non-exhausted coverage when rows were skipped. Expanded tree and flat lifecycle
tests pass (2/2), as do the 13 codec/vector/fused-scoring tests. The runner now
requires nonzero observed subgroup leaves **and** skipped vectors before
accepting an active subgroup treatment. Its 16 tests pass. A corrected
ReleaseFast executable is building; the next diagnostic reuses one saved
generation to isolate routing before repeating fresh-load qualification.

The corrected ReleaseFast build passed 27/27 steps; SHA-256 is
`02ec3dae2d27c2c4d1f6d07a737b9c0a267749ef89ebcdf2ea29376a28b6ef5c`.
All 3,756 audited worktree source files remained unchanged during this build.
The corrected 50K same-generation routing diagnostic is under
`.benchmark-results/pr593-subgroups4-query-50k-20260907`, with the saved first
candidate generation, fixed 1,000-query warmups, aggregate admission in both
arms, routing on/off and reverse order. The physical layout stays fixed; this
diagnostic does not measure its ingest/disk overhead.

The broader Debug run passed its first 19 tests, but was interrupted in
`flat traversal does not treat a full candidate heap as a pruning proof`.
A stack sample showed deeply recursive `splitInternalWithOptions` and
`computeNodeSplitRange` during that test's **128-row inner-product insertion**
with branching factor 2, before its query and without subgroup routing enabled.
Sample: `/private/tmp/pr593-subgroup-debug-sample-20260907.txt`.
This is an outstanding degenerate-tree construction investigation, not a
passing broad suite or an attributed subgroup-query regression. The test
process was stopped before timed query diagnostics began.

##### Corrected 50K routing A/B: reject the per-leaf quota

All four same-generation query arms completed. The two fixed-query controls
reproduce 98.405% recall, and both subgroup arms reproduce 90.753% recall.
The treatment now actually scores approximately 208 subgroup-enabled leaves
and skips 5,986 vectors/query. Two-arm medians:

| Metric | Routing off | Three of four groups per leaf |
| --- | ---: | ---: |
| Fixed 1,000-query recall | 98.405% | 90.753% |
| Approximate vectors/query | 24,014 | 18,028 |
| C1 diagnostic QPS | 238.3 | 193.7 |
| C30 diagnostic QPS | 1,986.7 | 1,735.6 |
| C30 HTTP p95 | 29.38 ms | 33.53 ms |
| Restart + query peak RSS | 939.8 MB | 1,027.2 MB |
| Restart + query peak physical footprint | 319.1 MB | 297.5 MB |

This is a repeatable rejection: -7.652 recall percentage points, -12.6% C30
QPS, +14.1% p95 and +9.3% RSS despite -24.9% approximate scores. The -6.8%
physical-footprint median does not rescue the quality/latency regression.
Both run orders regress throughput and p95. Exact completion remains narrow
(135.5 versus 134.2 vectors/query); weakening exact scoring is not the remedy.

The profile explains why the reduced shell is not faster: C1 leaf scoring
only falls from 1.099 to 1.051 ms/query, while source-space subgroup
representative scoring adds 0.912 ms/query. Fixed per-leaf setup and selection
still dominate enough of the candidate pass that skipping one quarter of its
codes does not save one quarter of its time. This experiment also prunes the
same fraction from every visited leaf, unlike the earlier offline **global**
representative-order budget. That offline quality curve did not validate this
local quota, and must not be cited as if it did.

Keep the persistent layout and shared-scorer integration experimental, and
**reject this selection policy**. Do not enable defaults or scale this policy
to 1M. A useful next design would prioritize subgroup work globally, permit
all groups from a promising leaf, and make representative evaluation compact
and SIMD-friendly. Those are hypotheses to qualify, not established wins;
simply increasing to 8/16 groups while retaining a fixed per-leaf discard
fraction has no demonstrated quality or cost justification. Likewise, do not
lower admission charges until the actual work plan is known and governed.

`summary.json` now records paired fixed-query recall checks independently of
successful process completion; both candidate arms fail the one-percentage-
point gate. Eight recovery-runner/summary tests and sixteen fresh-runner tests
pass. All owned benchmark processes have exited. No 1M subgroup run, promotion,
commit or push was performed.

##### Global subgroup selection and portable SIMD: offline gate (2026-09-07)

Following the rejected per-leaf quota, added three repository-owned tools:
`scripts/probe_dense_global_subgroups.py`,
`zig/tools/bench_subgroup_routing.zig`, and
`scripts/run_dense_subgroup_kernel_bench.py`. These are **offline experiments**;
this step changes no serving defaults, transaction boundaries, ownership,
admission charges, or authoritative score semantics.

The geometry screen uses all 50,000 OpenAI source rows and a bounded 32,768-row
prefix of the shuffled Cohere 1M corpus, with 128 held-out queries and top-k
100. Source-only balanced spherical bisection trains 256 parents and four
subgroups per parent. This mirrors the experimental trainer's shape but uses
NumPy/BLAS arithmetic, not bit-identical Zig training or the saved live index's
actual topology. Exact sample neighbors are evaluation labels only: neither
training nor group selection sees them. They do not provide an oracle stopping
threshold. No full-1M, public-API, or production-recall qualification is implied.

For each query, fix a centroid-ranked parent frontier and compare whole-group
global selection against the per-parent quota. Both use the same frontier;
whole-group budget overshoot counts as work. Reports preserve all predefined
25/50/75/100%-corpus frontier budgets and six global retention fractions rather
than reporting only the best setting. At the 50%-corpus frontier and 75%
retention setting:

| Corpus | Control vectors / neighbor coverage | Global f32 vectors / coverage | Global coverage loss | Per-leaf quota coverage loss |
| --- | ---: | ---: | ---: | ---: |
| Full 50K source | 25,011.34 / 98.4141% | 18,768.29 / 97.7734% | 0.6406 pp | 6.1250 pp |
| 32,768-row 1M sample | 16,384 / 98.7422% | 12,288 / 98.0938% | 0.6484 pp | 8.2266 pp |

Global selection therefore retains approximately 25% less candidate work
inside the one-percentage-point screen gate. Float16 representatives have
the same coverage at this setting; symmetric int8 query/representative
quantization loses 0.6406/0.6875 pp relative to control. Quantization uses one
scale per representative, with halfway rounding explicitly matched to Zig
`@round`; it is only an ANN routing hint, never a certified lower bound or an
exact-score substitute. The precision screen evaluates dequantized dots with
float64 accumulation; it does not prove identical near-tie ordering for f32
SIMD reductions.

**The remaining fixed-cost problem is measurable.** Global f32 selection skips
only 1.25/2.34 entire parents out of approximately 128, despite omitting 25% of
members. Almost every parent still pays leaf setup/query-quantization overhead.
This agrees with the earlier live test's small leaf-score saving (1.0995 to
1.0508 ms), and argues against translating vector-count reduction directly
into a latency prediction.

The standalone ReleaseFast benchmark consumes the screen's actual trained
representatives and query fixtures. Zig `@Vector` handles both ARM64 and
x86_64, including non-SIMD-width tails. It measures one contiguous 1,024-group
directory, includes per-query int8 quantization, reports score and stable
global-sort time separately, consumes every result, and reverses mode order
across five measured rounds after warmup. Latest repeat medians, score plus
sort:

| Kernel | 1,024 x 1,536-dimensional representatives | 1,024 x 768-dimensional representatives |
| --- | ---: | ---: |
| Scalar float64 | 1.0554 ms | 0.5268 ms |
| SIMD float32 | 0.1285 ms | 0.0868 ms |
| SIMD float16 | 0.1256 ms | 0.0873 ms |
| SIMD int8 | 0.0894 ms | 0.0646 ms |

The int8 plane plus scales is 1,576,960 / 790,528 bytes versus float32's
6,291,456 / 3,145,728 bytes. This is representative-plane size, **not** an index
RSS, footprint, or allocated-disk result. Both fixtures have 1,024 groups;
the Cohere kernel is not a full live 1M directory scan. Sorting alone remains
approximately 0.041 ms. Generation lookup, cold page faults, resource admission,
candidate scanning, exact completion, and network latency are not included.

Timings are shared-host diagnostics, not controlled performance qualification.
The initial 50K-shaped run was similar (int8 0.0889 ms; scalar 1.0654 ms), but
an overlapping screen/build run reached scalar 2.7575 ms and SIMD f32 0.7255 ms.
Other benchmark servers and compiler processes were observed and left alone.
All rounds, including this noisy repeat, remain in the receipts; no p95/QPS
claim or timing-based production promotion follows from them.

Artifacts: `.benchmark-results/pr593-global-subgroup-screen-20260907/`.
Use `openai-50k-reviewed.json`, `cohere-1m-reviewed.json`, and the
`*-kernel-repeat.json` receipts for final reviewed results. Earlier preliminary
and noisy runs are retained. Reports hash sources, normalized data, fixtures,
and the executable. The final kernel binary SHA-256 is
`f9ef23663f587b7cd1a422ea6bf4cc8d8bfb1d2c2f1ae9ae69a013590d26aea7`.
Seven Python subgroup tests and the ARM64 Debug SIMD/tail/int8 test pass;
x86_64-linux-musl Debug target checking also passes (not runtime testing).
Formatting and `git diff --check` pass. Review corrected int8 halfway rounding
and added the immediately-below-halfway regression case. The unrelated earlier
inner-product/binary-fanout construction stall remains unresolved by this work.

**Decision:** global selection passes this bounded geometry screen, and compact
SIMD makes representative scoring much cheaper. A net serving-cost win is
still unproven: even the int8 score-plus-sort cost is comparable to or larger
than the old live leaf-score saving, and almost no whole-leaf setup disappears.
Do not integrate/promote the scalar global sort as a product fix. The next
bounded prototype should combine weighted selection without a full sort with
a range-native candidate scorer that avoids per-row rejection and unnecessary
leaf work; then measure the complete selection-plus-scan cost on actual leased
leaf layouts. Preserve canonical insertion/tie order, cancellation, complete-
coverage fallback, and authoritative boundary completion. Only a winning
combined kernel should proceed to a public-API 50K A/B and full 1M qualification.
No server benchmark, full 1M run, commit, or push was performed in this step.

##### Weighted selection and range-native scoring: combined 50K kernel gate

Implemented the next bounded prototype in the existing worktree:

- `weighted_subgroup_selection.zig`: allocation-free, weighted partitioning
  selects exactly the same whole-group prefix as descending-score/ascending-ID
  sorting. The caller supplies query-owned entries and a membership mask;
  output scoring stays in original physical order, not partition order. It
  rejects duplicate/out-of-range IDs, nonfinite scores, zero weights, and an
  impossible budget. An introspective fallback sorts only the unresolved
  interval; validation, partitioning, and mask emission poll cancellation.
- `RaBitQuantizer.estimateDistancesInRangesTo`: validates ascending, disjoint,
  nonempty half-open ranges before any score emission, prepares the query once
  per leaf, and traverses selected ranges directly without visiting rejected
  rows. Empty plans avoid query preparation but still observe cancellation.
  The full-scan API uses the same arithmetic with one compile-time-selected
  full range. L2, inner product, cosine, and query-equals-centroid paths retain
  exactly the same scores and error bounds. Cancellation polls restart at each
  range, so gaps cannot skip every absolute multiple-of-64 polling location.
- `RangeCandidateScoreSink` flushes partial eight-score batches across gaps,
  preserving physical ID/score alignment and candidate insertion/tie order.
  The existing **default-off** subgroup experiment now uses this range scorer.
  Its rejected per-leaf quota is otherwise unchanged and remains rejected.
  **Global weighted selection is not wired into live traversal yet.**

`zig/tools/bench_subgroup_scan.zig` and
`scripts/run_dense_subgroup_scan_bench.py` preserve the combined-cost test. It
uses the authenticated **base** `segment-2.afps` from
`pr593-subgroups4-fresh-50k-20260907/Performance1536D50K-1-candidate`: 439 leaves,
1,756 persisted groups, and all 50,000 base rows. It rejects incomplete subgroup
coverage relative to base metadata. Segment-3 is an overlay, not a complete
directory; the prototype deliberately does not replay it or the WAL. Aligned
file bytes remain owned throughout the run, so borrowed views stay valid, but
this is not a test of a live query lease during generation publication.

Each of 128 fixed query vectors gets the same approximately half-corpus parent
frontier in every arm. Parent ordering uses an offline mean of persisted
source-space subgroup representatives, **not live HBC traversal**, and that
common setup is excluded from timing. Rotation is explicit (`none` here) and
the quantizer seed is read from authenticated base metadata. Treatments use
the same int8 representative scores and the same 75%-of-frontier whole-group
budget. No approximate/certified-bound equivalence or live recall is inferred.

The timed region includes representative scoring and query quantization,
selection/mask construction, RaBitQ leaf query preparation, candidate scoring,
and the existing approximate-result heap admission. It excludes outer routing,
exact completion, HTTP, resource admission, source/delta reads, and cold faults.
Every selected-work mode must match the sorting reference's **complete retained
candidate fields**, not just a checksum, for every query and every round.
Checksum receipts are additional evidence, not the correctness assertion.

Five measured rounds follow warmup and reverse mode order on odd rounds. Four
independent process runs produced these combined-cost medians:

| Run | Full scan | Global sort + row predicate | Weighted partition + predicate | Weighted partition + ranges |
| --- | ---: | ---: | ---: | ---: |
| 1 | 1.2194 ms | 1.1922 ms | 1.1892 ms | 1.1501 ms |
| 2 | 1.1925 ms | 1.1638 ms | 1.1417 ms | 1.1343 ms |
| 3 | 1.1839 ms | 1.1556 ms | 1.1277 ms | 1.1194 ms |
| Reviewed alignment/coverage checks | 1.1787 ms | 1.1409 ms | 1.1186 ms | 1.1092 ms |

The combined prototype reduces median kernel time **4.9–5.9%** relative to the
full scan within each run. All selected-work modes scan 18,807.84 vectors/query
versus control's 25,057.80 (24.94% less). In the reviewed run, selection drops
from 32.30 us for sorting to 9.26 us for partitioning; representative scoring
still costs 52.69 us, and range scanning/heap admission costs 1,047.55 us. No
large-sort fallback occurred. The incremental range-only benefit over weighted
partition + predicate is modest; most per-leaf setup remains. This is a small
positive combined-cost gate, **not** a 25% latency improvement or a p95/QPS win.
The shared host was not isolated; receipts retain all rounds and their spread.

Artifacts: `.benchmark-results/pr593-weighted-range-20260907/`, including
`openai-combined-{1,2,3}.json` and `openai-combined-reviewed.json`. The runner
hashes the executable, base segment, fixture, and relevant source inputs before
and after each run and rejects changing inputs. Reviewed Debug validation
uses four queries and asserts the same candidate-field parity; performance
measurements use ReleaseFast and all 128 queries. No full 1M subgroup generation
was built or qualified in this step.

Validation and review:

- 32 quantizer-kernel tests plus the separate cancellation test pass in Debug.
- Three weighted-selector tests cover stable-prefix equivalence across uneven
  weights/ties/edge budgets, forced sort fallback, invalid input, and cancellation
  inside partition work. These and the quantizer compile-check for x86_64 Linux;
  execution was on ARM64, not x86_64.
- Fused candidate parity now also covers the range sink across all three metrics,
  dimensions 3/64/65, centroid-equality queries, gaps and partial SIMD batches.
- Both flat/tree storage lifecycle tests pass without leaks: full coverage,
  filtered fallback, held-reader mutation isolation, replacement/insertion,
  canonical mutation bytes, and restart remain intact.
- Nine Python subgroup/receipt tests, formatting, and `git diff --check` pass.
- Fixed a cancellation-test fixture that requested its third poll from a
  one-row centroid-equality scan, which only polls twice. It now scans 129 rows
  and actually reaches the second periodic scan poll; no redundant hot-path
  poll was added merely to satisfy the test.
- An attempted broader vector-root run also exposed the existing external
  recall-fixture helper's `dirname(@src().file)` failure under this module root.
  That fixture-path issue is not fixed here. The new `lib-vector-kernel-test`
  target intentionally tests kernels without external recall fixtures; it is
  not reported as a passing full recall-fixture suite.

**Decision:** retain these building blocks and the default-off range integration.
The next live experiment needs a generation-bound global work plan with budgeted
representative access and explicit dirty/fallback handling for both flat and
tree traversal. Do not translate this offline parent proxy into a production
routing policy or rebuild ungoverned representative arrays per request. Public
50K recall/C1/C30/p95 and mixed-query/write qualification comes next only after
that integration; full 1M follows if it passes. No new production default,
durable format, authoritative-vector duplication, RSS/disk claim, commit, or
push accompanies this kernel result.

### Live generation-leased global subgroup work plan (2026-09-07)

The next default-off experiment is `ANTFLY_EXPERIMENT_GLOBAL_SUBGROUP_ROUTING`.
It stages the native flat/tree query frontier, ranks all of its persisted
subgroup representatives together, selects a whole-group prefix covering at
least 75% of the frontier's rows, and scans selected ranges in original physical
order. Representative scores are ANN hints, not certificates. Boundary rerank
and authoritative public scoring are unchanged; skipped rows mean candidate
coverage is `more`, not exhaustive.

The plan borrows IDs, quantized columns, and subgroup centers from the actual
query transaction's immutable generation lease. It copies small view headers,
not representative matrices. Query-owned leaf/selection buffers are admitted
before growth, included in retained scratch accounting, reset before lease
release, and reclaimable with idle scratch. Selected-frontier bandwidth remains
fully charged: this experiment does not obtain throughput by discounting its
reservation. The live version scores persisted float32 representatives directly
using portable Zig vectors; it does not recreate the offline int8 matrix for
every request.

Complete-coverage, filtered, distance-threshold, unsupported-metric, and
unsupported-layout queries use ordinary scoring. Encountering a dirty or
unsupported leaf drains staged leaves unpruned before continuing normal scoring;
it cannot accidentally enable the rejected per-leaf quota when both routing
flags are set. Non-finite representative arithmetic also disables pruning.
The plan verifies generation identity before consuming its borrowed spans.

Global selection establishes the frontier before candidate scoring, so it
disables score-dependent early termination for that query. The original routing
topology and leaf-effort ceiling remain, but wave growth/frontier membership
need not match an early-stopping control. Live recall must therefore be measured
independently of the previous offline geometry screen.

Debug validation: 14 targeted storage tests pass without leaks, including both
global flat/tree lifecycle tests, dirty-leaf updated-vector visibility, full
coverage, filtering, held-reader mutation isolation, restart, scratch accounting,
and retained-scratch reclamation. Eight vector-index scratch/weighted-selection
tests pass, including allocation failure and cancellation. Seventeen fresh-runner
tests and eight recovery/summary tests pass. The runner requires a nonempty fixed
query profile and positive live subgroup-skipping evidence for the treatment.

#### Public-API same-data 50K A/B result

ReleaseFast build: 27/27 steps passed. Binary:
`.benchmark-assets/pr593-global-live-20260907/bin/antfly`, SHA-256
`f1fce0a7f826da798026ebd2d9876e44e69cf88c44f16283f5f3d7e262c90b3b`.
Relevant planner/scorer/adapter source hashes remained unchanged across the build
check and measurement setup. The runner pins the executable and measurement
inputs and verifies them after each arm.

Artifacts: `.benchmark-results/pr593-global-live-query-50k-20260907/`, including
`runs.json`, `summary.json`, individual warmup/C1/C30 profiles, memory samples,
and server logs. Each arm independently clones the saved
`pr593-subgroups4-fresh-50k-20260907/Performance1536D50K-1-candidate/data`.
Order is control/global, then global/control. Both arms retain aggregate phase
admission, the same four-subgroup physical layout, effort, and boundary rerank.
The public table API serves 1,000 fixed recall queries, followed by 8-second C1
and 30-second C30 diagnostics (six client processes with five requests each).
All four arms completed with active-treatment evidence where required; server
logs contain no reported errors/quarantine failures.

| Metric (median of two independent arms) | Control | Global plan |
| --- | ---: | ---: |
| Fixed-query recall | 98.405% | 97.978% |
| Approximate vectors/query | 24,014.08 | 18,027.22 |
| Authoritative exact vectors/query | 135.512 | 135.364 |
| C1 diagnostic QPS | 144.38 | 126.25 |
| C1 HTTP p95 | 9.37 ms | 13.17 ms |
| C30 diagnostic QPS | 1,145.57 | 1,099.29 |
| C30 HTTP p95 | 74.31 ms | 75.61 ms |
| C30 mean leaf scoring | 1.660 ms | 1.416 ms |
| C30 mean subgroup planning | 0 | 0.228 ms |
| Restart/query peak RSS | 731.14 MB | 701.46 MB |
| Restart/query peak physical footprint | 386.45 MB | 376.47 MB |

Both recall comparisons pass the one-percentage-point gate: loss is **0.427 pp**
and identical across restarts. The global policy avoids the earlier local
quota's 7.652 pp recall loss while still reducing scored vectors by **24.93%**.
It does not materially reduce exact work or physical residual/projection reads.

There is **no established throughput/tail-latency win**. Median C30 QPS is 4.04%
lower and p95 is 1.75% higher. Leaf scoring plus subgroup planning is 1.644 ms
versus 1.660 ms: representative access/selection consumes nearly all the saved
scoring time. Separate arm results also expose host variability:

| Pair | Control C30 QPS / p95 | Global C30 QPS / p95 |
| --- | ---: | ---: |
| 1 (control first) | 1,016.81 / 92.46 ms | 1,097.11 / 74.64 ms |
| 2 (global first) | 1,274.33 / 56.17 ms | 1,101.46 / 76.58 ms |

Concurrent compiler/test activity was observed throughout; neither these
absolute latencies nor the small memory differences establish a historical
regression or a durable improvement. The memory figures are medians of each
process's sampled restart/query peaks, not ingestion peaks or demand accounting.
This is a query-only diagnostic, not fresh-load, disk, mixed-write, or official
VectorDBBench leaderboard qualification.

**Decision:** retain the tested default-off implementation and receipts, but do
not enable it by default or advance this shape to an expensive fresh 1M run.
The next focused opportunity is generation-owned compact representative access
(for example prebuilt int8 hints, budgeted and bound to the same generation),
not per-query matrix conversion, weaker admission, or eliminating exact
completion. That would need a new live recall and combined-cost gate; the prior
int8 kernel is encouragement, not proof. A 25% reduction in candidate count
alone is not sufficient to justify promotion.
