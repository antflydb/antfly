# Separate Vector Store: Intended Design

Status: experimental implementation, repeated 50K/1M measurements completed, 2026-09-09. The table setting,
reference-based source payload path, recovery/reader guards, and initial
reclamation/accounting are implemented in this worktree. Recovery qualification
passed; the 1M performance tradeoffs keep the settings opt-in.

Posting-local projection duplication is a separate policy: new posting builds
now default to no-copy (September 7, 2026). Set
`ANTFLY_EXPERIMENT_POSTING_LOCAL_PROJECTIONS=1` for the locality-on opt-in.
Existing planes remain readable without an eager rewrite. This does not change
the table-level source-ownership setting or exact-score requirements. Frozen
comparison catalogs and archive locations are in `benchmark-baselines/README.md`.

## Micro-profile of store costs (2026-09-10)

[VECTOR_STORE_PERF_PROFILE.md](VECTOR_STORE_PERF_PROFILE.md) attributes the
store's write, read, disk, and memory costs with the isolated
`vector_payload_bench` harness (`zig build vector-payload-bench`). It records
the SHA-256 digest as the dominant read-path CPU cost, synchronous WAL and
delta-chain checkpoints as the largest writer stalls, bootstrap write
amplification of about 3.6x, and the cold-reopen inventory scan, which now
reads only identity metadata instead of every payload byte.

## Sparse fallback and bounded planning follow-up

Sparse batching now applies only when no denser reclaimable segment was selected.
Empty segments do not suppress sparse progress; the existing copy-byte and row
caps remain. This avoids adding cold-segment copying to useful dense collections.

`ANTFLY_SOURCE_VECTOR_INCREMENTAL_PLANNING=1` computes segment liveness summaries
on the pinned marking cut, using the existing scan row/time budget. With unlocked
marking, the density scan runs outside the source mutex and before DB apply.
Summaries cost 24 bytes per physical segment and are included in admission.
Resurrected owners invalidate summaries after the scanner rejoins; no retirement
can use stale classifications. All-live verification skips density work unless a
post-cut preparation prevents the fast path. Final copy planning uses direct
reader indices, but copy-set construction and sorting still run under the lock.

The [qualification ledger](../.benchmark-results/vector-store-planning-targets-v2/RESULTS.md)
records eight fresh 50K arms, separately comparing the planner and sparse policy
in A/B then B/A order. Source/native tests in default and combined modes, 13 cache
tests, 10 API checks, eight reclamation checks and four independent inventories
passed. Neither source setting is promoted; this follow-up has not run at 1M.

| 50K comparison | Ready time | Mixed writes/s | Mixed p99 | Fixed churn time | Sampled physical footprint |
| --- | ---: | ---: | ---: | ---: | ---: |
| Bounded planner versus compact bitmap control | -3.9% | -0.1% | +2.9% | -3.0% | +20.5% |
| Sparse-only batches versus matched debt scheduler | -7.0% | -3.6% | +2.0% | +1.0% | +32.6% |

These are median paired observations on the available host. Planner churn
intervals reduce locked planning about 94% and total source-lock time about 34%.
Density visits contribute to marking counters in the treatment; marking plus
planning falls about 16.5%, with different layouts and copy volumes between arms.
Sparse-only fallback still allows extra copying during foreground work when only
sparse garbage exists. Separate foreground and idle copy budgets remain future
work. Copy-set construction and sorting also remain locked.

A [matched-input cleanup check](../.benchmark-results/vector-store-planning-targets-v2/matched-planning/RESULTS.md)
holds 64 MiB sparse batching constant and changes only bounded planning. Four
clones of the same saved database retain exactly 50,000 payloads and copy exactly
15,490,108 bytes in two collections. Median paired source-lock time falls 16.9%,
but marking plus planning rises 5.5%; settling remains about 8.44 seconds. This
confirms less locked work, without a cleanup-throughput win on that layout.

Nine query rejections at concurrency 30 occurred across three fresh arms: planner
pair-1 control and both sparse pair-2 arms. Successful lifecycle gates do not make
those runs error-free performance qualifications. The memory/foreground tradeoffs
and query rejections gate default promotion, combined foreground qualification,
and the next 1M run. Earlier results below describe the prior sparse policy and
remain preserved.

The first current-integration control exposed a separate cold-restart cost in
ANN metadata admission. CLOCK removal already swap-compacts its slots, but
insertion still scanned the full array for holes. That obsolete scan is removed
in the common control/candidate baseline. The sampled control and its cold
restart result remain diagnostic evidence, excluded from the fresh comparison.
A separate [same-data cache replay](../.benchmark-results/vector-store-planning-targets-v2/cache-replay/RESULTS.md)
compares the old and corrected binaries in ABBA order: mean query latency falls
about 65% with identical hits, scores and distances. Each process receives ten
warmup queries followed by 100 measured queries; this is not an OS-cache-flushed
cold benchmark. This cache gain is separate from the source-store treatments.

## Bitmap locator and sparse reclamation follow-up

Two focused opt-in changes now target the preceding experiments' costs.
`ANTFLY_SOURCE_VECTOR_BITMAP_LOCATOR=1` adds a snapshot-local locator to bitmap
marking. Its slots contain physical addresses; every probe verifies the complete
digest against pinned segment metadata. Construction yields within the existing
scan budget, and copy subsets share the completed locator and its source cut.
The existing lookup serves reads while construction is incomplete. At one million
physical rows the slot array is 16 MiB, in addition to bitmap and WAL-fallback
storage. `mark_bitmap_bytes` excludes locator bytes; total source heap includes
them, and scan counters include locator construction work.

`ANTFLY_SOURCE_VECTOR_SPARSE_GC_COPY_BYTES=67108864` allows one verified mark to
select several low-density garbage segments. It targets at most 64 MiB of selected
copy work and 65,536 additional sparse live rows, while preserving the existing
selection of denser garbage and permitting one oversized segment for progress.
Copy steps retain their independent byte budget. Planning admission failure
releases the cut for retry; old snapshots and post-cut writes retain their
existing protection. Neither setting changes durable formats or defaults.

The [qualification ledger](../.benchmark-results/vector-store-marking-targets/README.md)
tracks separate locator-versus-bitmap, compact-shape-versus-hash, and sparse-batch
comparisons. All twelve fresh 50K workload/reclamation arms and six independent
candidate inventories passed. Median paired changes are:

| 50K comparison | Ready time | Mixed writes/s | Mixed p99 | Fixed churn time | Mixed physical footprint |
| --- | ---: | ---: | ---: | ---: | ---: |
| Locator versus prior bitmap | +5.2% | -1.3% | +0.5% | +3.5% | -8.8% |
| Compact bitmap shape versus hash marking | +1.5% | -0.6% | +1.8% | +1.7% | -22.0% |
| Sparse batches versus matched debt scheduler | -5.7% | +0.5% | -0.8% | +1.0% | +30.5% |

The compact shape reduces measured marking and planning time in both hash-control
pairs, but does not yet improve end-to-end churn consistently. Sparse batching's
foreground changes are small; churn logical write I/O rises 5.5%, and sampled
physical footprint rises in both pairs (4.8% and 56.2%). The unbatched debt controls
need 407/356 seconds to reclaim after churn, versus 2.4/2.5 seconds for the batched
candidates. These runs finish with different layouts.

A separate [matched-layout ABBA check](../.benchmark-results/vector-store-marking-targets/matched-sparse/RESULTS.md)
clones the same saved sparse database for every arm and changes only the batch
target. Controls take 336/338 seconds and 56 collections; batching takes 8.8/9.8
seconds and two collections. Mark rows fall from 11.56 million to 461,014, with
exactly the same 15,490,108 payload bytes written. Post-GC serving and retained
counts pass in every arm. This isolates a substantial sparse-cleanup benefit on
that layout; it does not establish a universally optimal batch size.

The compact locator shape also passed all four 1M workload/reclamation checks
and both independent inventories. Readiness improves 15.9%/12.8%, but the mixed
and churn results reverse between pairs: writes +25.7%/-36.4%, p99 -21.8%/+95.3%,
and churn time -6.0%/+67.1%. Median paired changes are -14.3% readiness time,
-5.4% writes, +36.8% p99, +30.6% churn, +21.2% sampled physical footprint, and
+2.4% churn logical write I/O. This does not qualify a default. Both control
churn source-counter intervals are incomplete, so no paired marking-time ratio
is claimed at 1M. Candidate planning takes 2.1/6.5 seconds, including a 4.7-second
restore planning interval that writes only 309,724 payload bytes. Metadata
planning under the source lock remains a concrete profiling target; these
counters do not prove the cause of the entire latency regression.

Sparse batching also passed four separate 1M arms and both independent inventories.
Median paired changes are -2.4% readiness time, +5.7% mixed writes, -14.0% p99,
+29.8% churn time, -18.2% sampled physical footprint, and +6.8% churn logical write
I/O. Mixed writes change -13.9%/+25.3%, p99 +16.3%/-44.3%, and churn +75.7%/-16.1%
between pairs, so the medians are not a consistent foreground win. In the first
pair, batching copies 44.4 MB in six churn steps versus 6.25 MB in one control
step, with 3.6 versus 0.38 seconds of planning. The second pair's full source
counter intervals are unavailable. All four ordinary 1M idle drains settle in
15–19 seconds; the sparse-layout outlier is specifically demonstrated by the
matched 50K diagnostic.

Keep both settings opt-in. These results motivated sparse batching only
when no denser reclaimable work is selected, plus bounded metadata planning with
snapshot-safe segment liveness summaries. The follow-up implementation and its
separate qualification are recorded above. Separate foreground and idle copy
budgets remain unimplemented; combined foreground use is not yet qualified. All twenty timed 50K/1M arms, twenty
reclamation checks, ten independent inventories, and four matched-layout runs
passed; the frozen runtime and harness identities remain pinned in the ledger.

## Cost-recovery experiments

Four further opt-in treatments are implemented:
WAL-prefix/append-delta inventory updates, obsolete-debt background scheduling,
segment bitmap marking with a bounded WAL fallback, and a provisional small-table
inventory cutoff. None changes defaults. The cutoff is a candidate to measure,
not an established optimal crossover. Explicit collection, full sync, payload
ownership, old-reader leases, WAL bounds and memory admission remain intact.
See the [implementation and qualification ledger](../.benchmark-results/vector-store-cost-recovery/README.md)
for settings, safety boundaries, validation and the independent A/B matrix.

The isolated 50K ABBA runs passed all 16 workload/reclamation gates and eight
independent full-inventory audits. Against the prior eager shared-catalog,
incremental-inventory, unlocked-scan candidate, median paired changes were:

| Treatment | Ready time | Mixed writes/s | Mixed p99 | Fixed churn time |
| --- | ---: | ---: | ---: | ---: |
| Append/checkpoint delta inventory | +18.6% | -19.6% | +15.5% | +12.4% |
| Debt-based background scheduling | -21.2% | -9.8% | -3.9% | -0.9% |
| Segment bitmap marking | -8.8% | -2.6% | +3.7% | +5.2% |
| Provisional small-table cutoff | +13.6% | +0.2% | +2.0% | -1.6% |

Delta inventory eliminates eligible WAL retraversal but adds foreground map
work; inventory and preparation time increased in both pairs. Debt scheduling
avoids initial marking in both 50K candidates, but one later sparse-GC drain
took 365 seconds and 57 collections, revisiting 8.85 million mark rows. Garbage
declined throughout and the debt policy did not defer that drain: the existing
collector repeatedly chooses one sparse segment per full mark. The other debt
candidate drained in 2.54 seconds. These are unequal-layout lifecycle results,
not matched GC microbenchmarks.

Bitmap marking reduced sampled mixed physical footprint 6.5% at 50K, with the
foreground tradeoffs above. The cutoff did not establish an optimal threshold.

Debt scheduling and bitmap marking also completed separate 1M ABBA runs. All
eight workloads/reclamation checks and four independent inventories passed,
with the source limit held at 384 MiB. Median paired changes were:

| Treatment at 1M | Ready time | Mixed writes/s | Mixed p99 | Fixed churn time | Mixed physical footprint |
| --- | ---: | ---: | ---: | ---: | ---: |
| Debt-based background scheduling | -1.1% | +5.1% | -3.5% | -14.3% | +2.1% |
| Segment bitmap marking | +11.2% | -7.2% | +29.3% | +10.4% | +5.3% |

Debt scheduling improves churn in both pairs (15.6% and 13.1%), but mixed writes
and latency change direction between pairs. Initial mark rows are
control/candidate 6.03M/0.80M and 5.24M/5.50M: the large reduction in the first
pair does not repeat. All four debt runs reclaim in 17–21 seconds. The 50K
sparse-GC outlier remains relevant even though it does not recur at 1M.

Bitmap marking reduces the reachability representation to about 191–193 KiB
of sampled bitmap/offset storage, with zero WAL-fallback entries in these
ingestion samples. Other source allocations remain: maximum sampled source
heap is control/candidate 378.8/306.1 MiB and 372.1/365.4 MiB. These samples can
miss brief peaks. Mixed RSS falls 12.9% overall, while physical footprint changes
direction between pairs; the smaller marking representation is not a uniform
whole-process memory improvement. Ready time, mixed writes, p99 and churn all
regress in both pairs. The first pair spends 15.97 seconds marking 4.02M churn
rows versus the control's 2.02 seconds for 6.05M rows. The second candidate also
spends 15.10 seconds marking; its control's complete source interval is
unavailable. Compact marking needs faster location lookup before promotion.

Keep all four settings opt-in. Do not combine the foreground-cost regressions
or infer a new default from these comparisons against the previous candidate.
The locator and bounded sparse-reclamation follow-up is recorded above. Applying
queued WAL deltas at installation without eager foreground map updates remains
a separate hypothesis. The cutoff still needs a matched-dimension size sweep
before selecting a crossover; neither is an implemented or qualified new win.
Full per-arm results and limitations are in the
[measurement report](../.benchmark-results/vector-store-cost-recovery/RESULTS.md).

## Lifecycle fixes and isolated qualification

The completed qualification is recorded under
`.benchmark-results/vector-lifecycle-qualification/`. It first addresses two
failures seen in control arms, before measuring individual source-store changes.

The saved posting-WAL mismatch was an encoding error in AFQD V2, rather than
evidence of an incorrect sequence fence. Its L2 directory reader expanded an
omitted centroid-dot-product field to 75 zero floats. The next patch expected
the original 2,408-byte protobuf (CRC `8b77cabb`); directory reconstruction
produced 2,711 bytes (CRC `57cf4f95`). AFQD V3 records omission explicitly while
keeping the physical zero plane available for search. Current recovery applies
patches directly with strict base length/CRC and replacement validation.

These native formats are new in this PR, so only their latest layouts are
supported: quantized directory V3, vector block V4, vector manifest V6, posting
segment V4, posting WAL V5, and posting checkpoint V4. The obsolete readers,
conditional older-format writers, omitted-field recovery shim, and V2 fixture
have been removed. Earlier experimental stores must be recreated. Regression
coverage now writes the current directory format, reconstructs the exact L2
protobuf bytes through buffered/streamed/cold reads, applies the strict patch,
and rejects an expanded-zero base. Format tests reject obsolete and future
versions. Existing pre-PR HBC metadata/LSM migration remains supported; it is
separate from this unreleased native format history.

Generated-coverage initial builds can also outlive completion of the canonical
serving generation. These intents have no catalog-admission marker, so the
previous marker-only completion check could leave redundant shadow work queued.
The implementation now checks their durable source-replay boundary together
with current configuration, replica/root identity, replay checkpoint, complete
coverage counters, and physical artifact cardinality. It attaches the canonical
worker, discards only an inactive candidate, then removes repair debt. Continued
query access during activation separately requires a verified resident
predecessor; an unvalidated replacement cannot inherit that exception.
The frozen source passed 57 source/native recovery checks and 27 selected
lifecycle checks, including the then-present saved patch fixture, repeated full-checkpoint
tail opens, inactive-candidate retirement, invalid replacement rejection, and
rollback/restart. Two existing tests also reproduced failures before these
fixes because they inspected a certificate while its background checkpoint was
still publishing; they now await that boundary. These checks alone do not
constitute performance qualification.

Because this is a shared worktree with concurrent implementation work, this
qualification uses the source copy in `source/` and its `frozen-source.json`
manifest, a separately frozen VectorDBBench client, and dataset checksums.
The binary is built from that copy and pinned for every measurement arm.
The latest-format-only cleanup happened afterward. Those frozen inputs and
receipts remain unchanged: the timings below describe the pinned benchmark
binary, not a new measurement of the cleanup. The cleanup passed current-format checks, 56 source-payload/native-store tests,
and five checkpoint integration tests, including strict patch rejection and
repeated reopen with a concurrent WAL tail.

The measurement order is adaptive cache, append-only segments, then selective
GC with append-only mode and the same GC budget in both arms. Ownership and
snapshot reads retain individual controls. Group commit stays off. Fresh 50K
tables run sequential A/B and B/A on the available host. Receipts include
fixed-count update/delete/restore counters, preparation lock waits, directory
writes, segment counts, cache residency, and whole-process sampled logical and
physical I/O. Process I/O spans primary storage and journals as well as vector
storage; physical writeback and final sampling gaps are reported as limitations.
Only a subset that passes recovery and explains the mixed-workload tradeoff
advances to repeated 1M qualification.

All six isolated/subset 50K comparisons passed: 24 of 24 fresh arms completed
lifecycle qualification. These compare refinements on `vector_store` tables;
they are not a new `primary_lsm` versus `vector_store` table-mode comparison.
The following are median paired candidate/control ratios (smaller is better
for bytes, duration, and latency; larger for throughput):

| Treatment | Process logical writes | Fixed churn duration | Mixed writes/s | Mixed query QPS | Mixed p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Adaptive cache | 1.068 | 0.886 | 0.891 | 1.115 | 0.994 |
| Append-only segments | 0.865 | 0.987 | 0.983 | 1.034 | 1.001 |
| Selective GC, matched append-only control | 0.716 | 1.058 | 1.036 | 0.976 | 1.035 |
| Ownership indexing | 0.941 | 0.963 | 0.959 | 1.029 | 0.892 |
| Snapshot reads | 0.995 | 1.111 | 0.897 | 1.070 | 1.011 |
| Append-only plus selective GC, matched GC budget | 0.550 | 0.836 | 1.158 | 0.953 | 0.809 |

The selected 1M subset is **append-only segments plus selective GC**, with an
8 MiB GC step budget in both modes and enrichment batch size 64. All other
experimental flags remain off. Its two paired process-write ratios are
0.584/0.515; mixed writes/s 1.123/1.192; mixed p99 0.869/0.750; and fixed churn
0.713/0.960. Both pairs therefore retain reduced rewriting without the earlier
mixed write/churn slowdown. Mixed QPS is about 5% lower, source disk grows
0.78%, and segment count rises from 128 to 649–650. Peak physical footprint
falls in both pairs (0.756/0.904), while mixed RSS rises (1.431/1.074), so memory
remains an explicit scale-up question. All arms reclaim to exactly 50,000
payloads after restart; both candidates have already done so before restart.

Ownership indexing cuts marking from about 1.5 s to 0.33 s, but its write and
peak-memory results disagree between pairs. It remains separately measurable
and has not been added to the selected subset without a combination test.
Adaptive cache does not reduce final cache residency at 50K and worsens the
query-concurrency curve; snapshot reads reduce source lock wait but lose write
throughput in both pairs. Neither joins the bulk-serving candidate. Medians in
the table do not establish repeatability where individual pairs disagree.

The selected subset completed **four 1M arms in A/B and B/A order**, all passing
lifecycle gates with live/cold/warm recall 0.9899–0.9903. It is recovery-qualified,
but **not promoted as a balanced 1M performance default**:

| 1M candidate/control ratio | A/B | B/A |
| --- | ---: | ---: |
| Ready time | 0.753 | 0.901 |
| Whole-process logical writes | 0.666 | 0.660 |
| Concurrency-30 QPS | 1.453 | 1.156 |
| Concurrency-30 p99 | 0.602 | 0.834 |
| Mixed queries/s | 1.364 | 0.846 |
| Mixed writes/s | 1.294 | 0.811 |
| Mixed query p99 | 0.630 | 1.004 |
| Fixed 10K churn duration | 2.433 | 1.733 |
| Peak physical footprint | 1.052 | 1.007 |
| Mixed RSS | 1.315 | 0.796 |
| Final total disk | 1.016 | 1.042 |

Controls wrote 30.63/30.81 GB of sampled logical process I/O, versus
20.40/20.32 GB for candidates. Readiness and concurrency-30 queries improve in
both pairs, but mixed throughput changes direction and changed-vector churn
regresses. The churn timer includes the final index-sync fence and status
observation. Candidate churn took 121.69/96.02 s versus 50.01/55.42 s in controls.
The first candidate spent 52.7 s marking and wrote 352.3 MB of directories while
copying only 6.7 MB during GC. Faster copy completion permits more full marks
and directory publications; those costs now need to be bounded or coalesced.
Ownership indexing is promising for marking, but its combination has not been
qualified and is not silently enabled.

Both controls also stalled in the first single-client query window (p99
21.1/29.0 s versus candidate 13.12/61.38 ms). This is a repeatable initial-window
observation, not a general 100× query-throughput claim. The second candidate
had a long final publication interval before readiness, with transient zero
published counts while shared-vector projection maintenance completed. It
subsequently became fully ready without repair debt. All original timings and
receipts are retained, including these delays.

The pinned runs are under
`.benchmark-results/vector-lifecycle-qualification/qualified-1m/`. Cold restart
denotes a fresh server process, not an OS page-cache purge. Source preparation
lock wait does not include all outer DB serialization. One candidate's final
before-restart source counters and one control's after-restart counters were
unavailable; null counters and invalid per-phase deltas are excluded from
aggregation, while process I/O and request timing remain available.
All four supplemental reclamation checks passed on cloned databases: exactly
1,000,000 retained payloads / 3,072,000,000 payload bytes, zero pending collection,
zero unreferenced bytes, and successful serving plus complete publication after
GC. Controls settled in 107.08/117.55 s and candidates in 34.15/35.68 s including
process startup and validation. These observations prove eventual cleanup;
they do not replace the timed end snapshots. The final receipts are under
`reclamation-1m-final/` within the qualification root. Full per-pair evidence and accounting limits
are in `.benchmark-results/vector-lifecycle-qualification/RESULTS.md`.

Source counters in a best-effort status response can come from a read-only
fallback with zero volatile activity counters. The completed measurements
verify prepared-payload counts and bounded fixed-churn counter deltas; process
I/O sampling supplies an independent total across server lifetimes. A transient
zero status counter must not be interpreted as zero write cost.

## Decision and next work after 1M qualification

The experiments support keeping table-owned source payloads, shared ANN payload
ownership, and the lifecycle fixes. The append-only/selective-GC combination
has a repeatable write-I/O benefit, but its 1M churn regression prevents promoting
it as the balanced default. It remains an opt-in experiment. Adaptive cache and
snapshot reads stay out of the candidate; group commit stays off until payload
preparations can overlap outside the outer DB lock.

Proceed with a narrower GC experiment:

1. Bound marking work across collection steps and test the existing ownership
   index in combination. Its isolated 50K marking reduction is promising, but
   does not establish an end-to-end or 1M win. Preserve primary commit ownership,
   stale-completion checks, tombstones, and leases held by old readers.
2. Coalesce directory publication or make it incremental so a small collection
   step does not repeatedly rewrite the whole directory. Preserve durable
   publication ordering and crash reconstruction before retiring segments.
3. Measure the same fixed-count update/delete/restore workload first at 50K,
   then 1M. Attribute mutation time, final index-sync time, status-read time,
   outer DB lock wait, marking, and directory I/O separately. Match GC budgets
   and start from fresh tables on the available host.
4. Repeat A/B and B/A for the resulting subset. Promote only after the churn
   regression is removed while retaining the write reduction and passing
   readiness, query-tail, recall, memory, restart, and reclamation gates.

The implementation of this follow-up is now under qualification in
`.benchmark-results/vector-bounded-gc/`. `ANTFLY_SOURCE_VECTOR_MARK_STEP_ROWS`
limits primary ownership, ANN ownership, and exact inventory verification to
16,384 rows per maintenance turn in the candidate. A retained primary snapshot
and ANN generation define the mark; preparations after the cut are protected
in the WAL suffix, including retries of previously orphaned payloads. The scan
can also advance under writer WAL pressure. Shutdown cancels its primary read
transaction before destroying the backend. A retry seen first in the WAL tail
and later by the mark is counted once. Segment selection, sorting, copy,
and publication remain separate work; the row limit is not a wall-clock limit
on the whole collector. Marking and planning have distinct counters.

`ANTFLY_SOURCE_VECTOR_COALESCE_DIRECTORY=1` updates location hints only for
retired/replacement segments and persists a directory snapshot after at least
max(4,096, current entries / 4) changed hints. Explicit source checkpoints use
the same coalescing rule. CURRENT and immutable segments remain authoritative;
missing/stale hints fall back to them, and old readers validate hints against
their own manifests. Recovery ordering and retirement fences are unchanged.

The fixed-count churn harness retains its final full-index mutation batch.
Existing server batch profiles separate sync wait from client mutation/request
time; status reads are timed independently. Profile row counts and final-fence
counts must match the exact workload. An empty public batch is not used as a
fence because it can return without routing to a shard. Source status adds DB batch lock
wait, marking step count/maximum rows/maximum duration, planning time, and
directory publication/deferral counters. Whole-process I/O remains the total
write-accounting cross-check.

Initial qualification stopped on the reversed candidate: replay caught up, but
active-count metadata was 50,685 for exactly 50,000 unique posting members.
The saved index has no duplicate members or incorrect leaf mappings. A focused
regression reproduced a source-capture cache gap: local HBC transactions finish
before the outer source transaction publishes its immutable serving generation.
Search cache borrowing and admission must remain isolated for that whole interval,
including durable posting publication and rollback. The correction has local/shared
cache and delayed-reader coverage, and is pinned separately for fresh qualification.
The original failed arm remains preserved; a passing focused test does not qualify
the larger workload by itself.

Selective collection must make progress even when every nonempty segment is
below the garbage-density threshold. Empty segments may be retired with a pass,
but their presence must not suppress selecting the best segment containing real
garbage. A saved 50K reclamation failure exposed that starvation case. The fix
has a regression with an empty segment, a one-eighth-obsolete segment, bounded
marking, and an old reader lease. Partial-GC live/orphan counters use the entire
mark, including untouched cold segments; copied bytes are a separate quantity.
`revisions/selective-progress/` pins these corrections for fresh qualification.

The cache-corrected 50K comparisons covered the combined bounded GC/directory/
ownership candidate and isolated ownership with bounded GC/directory in both
arms. Ownership alone added no clear fixed-churn benefit. After the selective-GC
correction, a fresh combined 50K ABBA and all four matching reclamation checks
passed. Fixed-count churn improved 25.8% and mixed write throughput 30.2% by
median paired change; churn-envelope process logical writes increased 11.8%
and mixed query throughput fell 8.6%. Directory bytes written fell 72.8%.
The maximum mark step was 16,384 rows (18–29 ms), versus roughly 800K rows
(264–392 ms) in controls; this does not bound planning/publication. Only the
first pair has valid paired source-counter envelopes, so repeated write-cost
comparisons use process-sampled I/O.

Fresh 1M ABBA subsequently passed all four workload/lifecycle arms with the same
corrected binary, gated by matching 50K lifecycle and reclamation passes. Both
scales use fresh sequential tables, matched 8 MiB copy budgets, float32 source
payloads, and the same ANN settings. Adaptive cache, snapshot reads, and group
commit stay off. All four exact 1M reclamation checks also passed on clones: 35.3, 51.6,
47.8, and 33.4 seconds in recorded arm order. Each reached exactly one million
live payloads (3,072,000,000 raw bytes), zero orphan/pending bytes, and healthy
queries before and after GC.

At 1M, fixed-count churn fell from 81.2 to 62.8 seconds in the first pair and
420.9 to 46.6 seconds in the reversed pair. Churn-envelope process logical writes
fell 51–57% in both pairs. This addresses a substantial part of the maintenance
cost, but mixed write throughput fell 23–69%, mixed query throughput regressed,
and initial ready time worsened in both pairs. Cold and warm restart tails varied
substantially between pairs. Keep the options experimental: the subset does not
yet retain mixed-workload performance. Shared-host contention was observed; the
repeated measurements remain useful but do not establish exact causal percentages
for every latency or footprint change.

One candidate mark step still took 1.13 seconds at the 16,384-row limit (the other
arm's maximum was 34.1 ms). The next implementation should add an elapsed-time
budget and scan immutable snapshots outside the foreground source-writer lock,
retaining primary/ANN/source generation leases and merging the protected tail
under a brief lock. Planning needs its own bound. Then reduce conservative
post-cut reappends by protecting an already-durable payload without copying it
merely because marking has not visited it yet. Retry, abandoned collection,
old-reader, and restart tests must continue to prove those fences. These are
next steps, not implemented changes in this qualified binary.

Exact live-payload reclamation does not imply physical single copy: conservative
post-cut preparation can reappend an already-live digest before marking reaches
it. Whole-process disk/WAL accounting retains that cost. Both 1M controls lack
valid complete source-counter envelopes; use process-sampled I/O for repeated
write comparisons and label partial source-counter attribution separately.
See `.benchmark-results/vector-bounded-gc/revisions/selective-progress/RESULTS.md`
for per-pair results, preserved failures, and pinned source/harness receipts.

## Foreground marking follow-up

The elapsed-time/unlocked-scan follow-up is implemented under
`.benchmark-results/vector-foreground-gc/`. Its pinned build passed five API
lifecycle checks and all four fresh combined 50K A/B and B/A arms.
`ANTFLY_SOURCE_VECTOR_MARK_STEP_US` adds a cooperative elapsed budget to the row
cap; the proposed candidate uses 2,000 microseconds and 16,384 rows, whichever
is reached first. One backend operation or scheduling stall can exceed the
budget, but no subsequent row starts after it has expired. It is not a hard
real-time guarantee.

`ANTFLY_SOURCE_VECTOR_MARK_OUTSIDE_LOCK=1` scans retained primary, ANN, and source
snapshots outside both the source writer lock and DB apply lock. The scanner owns
its live map, cursor, and captured ANN scopes. Writers maintain a separate
protected WAL tail and record concurrent preparations; scan discoveries and those
preparations are reconciled under the source lock once per turn. A retry of a
version already scanned is counted once. No other collector or checkpoint can
advance or retire an in-flight scan. Cancellation waits for the scanner before
releasing snapshots, and ambiguous primary outcomes still prevent publication.

Setup, planning, copying, and durable publication retain their existing locks and
fences. This change does not yet bound planning or remove conservative payload
reappends. New counters separate outside-lock scan time, merge time/maximum,
elapsed-budget yields, and concurrent-scan deferrals from existing marking and
planning totals. The budget and unlocked mode can be measured independently as
`mark_time` and `mark_unlocked`; `foreground_gc` combines them against the previous
bounded-GC/coalescing/ownership baseline.

The source recovery suite now has 27 passing tests, including paused scans with
concurrent writes/deletes/retries, cancellation, ambiguous outcomes, poison
fencing, lagging ANN generations, old readers, restart, and elapsed-budget exact
verification. The 32 native-store tests also pass on the pinned source.

Both combined and unlocked-only 50K A/B and B/A passed all four timed arms and
all four exact-reclamation clones. Neither treatment is a performance promotion:

| Median paired change | 2 ms + unlocked | Unlocked only |
|---|---:|---:|
| Initial readiness time | +29.6% | +60.9% |
| Fixed-count churn time | -0.6% | -8.6% |
| Mixed writes/sec | -11.7% | +38.7% |
| Mixed queries/sec | -2.0% | -23.8% |
| Mixed p99 | +15.3% | +9.8% |
| Churn logical write bytes | +13.2% | +10.1% |

Combined DB batch lock wait fell 97.5%, but overall performance did not improve.
Unlocked-only also increased mixed RSS 53.2%, despite essentially unchanged final
disk use. One control's source-counter snapshot was missing; complete paired
source-counter comparisons for unlocked-only are invalid, while workload timings
and process I/O remain available. All eight GC clones reached exactly 50,000 unique
payloads, zero pending/orphan bytes, and healthy complete ANN serving. Candidate
reclamation ranged from 6 to 89 seconds, so passing inventory checks does not imply
uniform reclamation latency.

No new 1M run was started: neither 50K subset meets the intended performance gate.
The options remain experimental. The next boundary is worker scheduling: an active
scan currently pays a fixed 100 ms sleep between passes. Separate progress from
scan quantum with cooperative yielding and explicit background CPU budgeting,
then measure it independently. Remaining locked setup/planning/copy/publication
and conservative reappends during scanning also need isolated profiling. Any
concurrent deduplication optimization must retain the referenced source locations
through GC publication and preserve prepare-before-primary durability; an
unprotected membership lookup is insufficient. Exact receipts and per-arm numbers
are in `.benchmark-results/vector-foreground-gc/RESULTS.md`.

## Active scan progress and protected reuse experiment

The [2026-09-08 correctness and design review](VECTOR_STORE_REVIEW.md) records
the remaining audit boundaries and larger opportunities after these measurements.

The three structural follow-ups are now implemented under
`.benchmark-results/vector-structural/`: `ANTFLY_SOURCE_VECTOR_INDEPENDENT_SCAN=1`
separates incomplete scan turns from DB metadata maintenance,
`ANTFLY_SOURCE_VECTOR_SHARED_CATALOG=1` shares immutable segment and manifest
arrays across source leases/WAL successors, and
`ANTFLY_SOURCE_VECTOR_INCREMENTAL_INVENTORY=1` maintains rebuildable physical
occurrence counts across changed segments and WAL membership. They remain
independent experimental controls. Inventory counts handle duplicate physical
locations and never replace primary/ANN ownership authority; they add resident
metadata. All 35 source tests pass with switches off and with all three plus
ownership indexing enabled; 32 native tests and five public API lifecycle checks
pass on the pinned release. All 12 fresh 50K timed arms and 12 reclamation clones
pass. Two inventory candidates also reopen with incremental inventory and
checkpoint inventory restoration disabled: full scans independently recover
50,000 unique payloads / 307,200,000 raw bytes before collection publication.

The independent A/B and B/A comparisons do not make all three clear wins:

| Treatment | Median paired result | Decision |
|---|---|---|
| Independent scan | Readiness +8.1%, mixed queries +0.1%, mixed writes -1.9% | Keep experimental; avoided apply visits did not produce an overall improvement. |
| Shared catalog | C1 queries +20.4%, mixed writes +4.1%, churn time -7.8%, mixed RSS +22.2% | Strongest next candidate; attribute the repeatable RSS increase before promotion. |
| Incremental inventory | Publication-stage time -51.9%, mixed writes +6.7%, mixed queries -3.5% | Keep experimental; fewer segment visits cost more total inventory bookkeeping. |

Shared-catalog readiness is outlier-sensitive. Inventory readiness and RSS change
direction across pairs; its roughly 93% reduction in segment rows excludes WAL
traversal and map work. Inventory candidate reclamation settles in 2.93/2.90 s
versus 7.19/9.53 s for controls, with different starting layouts/debt. These are
lifecycle results, not identical-input GC microbenchmarks. No defaults change.
Profile shared-catalog residency and inventory WAL/map costs, then qualify a
selected subset against the stronger locked row-bounded baseline before 1M.
See [the complete measurement record](../.benchmark-results/vector-structural/RESULTS.md).

The memory investigation is recorded under
`.benchmark-results/vector-structural-refined/`. Identical-data memory-map
diagnostics did not reproduce a retained-catalog heap or mapping increase, so
shared-catalog ownership stays unchanged. Original phase-matched process samples
show mixed physical footprint +6.6% versus RSS +22.2%; fresh qualification must
continue reporting both. A separate WAL-membership prototype passed four 50K
arms, four reclamation checks, and two independent inventory audits, but did not
establish an overall performance win. That prototype remains in its frozen
experiment snapshot and is excluded from the current implementation.


The subsequent [source publication memory fix](../.benchmark-results/vector-source-memory-fix/README.md)
reproduces the suspected OOM as a source-budget rejection of a second WAL-sized
allocation during GC preparation. Full and selective GC now prepare readers and
inventory before publication, reuse the committed WAL suffix, and preserve the
current generation on pre-publication failure. Scratch admission and a
budget-derived WAL bound protect foreground progress; ambiguous durable writes
still require recovery. The controlled memory test now succeeds, and validation
covers allocation failures, old readers, retry, update/delete and repeated restart.
That revision passes fresh 50K ABBA and its lifecycle checks, but the second
1M candidate logs one maintenance OOM and is excluded. A same-binary diagnostic
identifies a separate 77,594,648-byte mark-map allocation with 332,745,026 live
bytes against the 402,653,184-byte source slice. The
[mark-workspace follow-up](../.benchmark-results/vector-source-memory-admission/README.md)
admits the map and source leases against the resident ANN snapshot before map
allocation. Rejected setup releases its temporary snapshots and retries later;
backing allocation and I/O failures still propagate. Its regression verifies
repeated deferral without retained memory, reclamation after pressure clears,
old readers and repeated reopen. Validation passes 42 source checks in each
configuration, 33 native checks, 256 publication allocation-failure positions,
and both five-case public API suites, with no test leaks. Fresh sequential 50K
and 1M ABBA passes all eight workloads and reclamation checks and all four
independent candidate inventories. No OOM/poison/retry error invalidates an arm.
The memory limits are unchanged; earlier failed arms and diagnostic timings
remain excluded.

The optional catalog/eager-inventory/unlocked-scan combination remains a
performance tradeoff against the fixed locked baseline. At 1M, median paired
mixed queries improve 9.5%, writes 7.4%, p99 14.5%, and C1 queries 7.8%; those
directions repeat in both pairs. Readiness is slower in both pairs (+9.8% paired median).
Mixed RSS falls 3.4%, while physical footprint varies by pair (+1.5% median).
At 50K, mixed writes fall 11.2% and p99 rises 13.2%, so no defaults are promoted.
Candidate maximum sampled source heap still reaches 372–374 MiB of the 384 MiB
slice; the fix provides admission and progress, not elimination of resident
memory demand. Maximum sampled source WAL is 48.05 MiB in all four 1M arms.
These periodic samples are not exact peaks. The second 1M control lacks a
complete churn-stage source-counter interval; its lock/inventory breakdown is
unavailable, not zero. See the
[qualified results](../.benchmark-results/vector-source-memory-admission/RESULTS.md).
Both arms contain the fixes: this is not a pre-fix/post-fix timing comparison,
a comparison against `primary_lsm`, or qualification of other deployment modes.
The frozen source/harness/binary hashes verify. Qualification excludes unrelated
concurrent changes in the live worktree; the final repository-wide and task-file
diff checks pass.

The completed isolated deferred-inventory 50K ABBA passes all four timed and
reclamation arms and both independent full inventories, but it is not selected:
median paired mixed writes fall 25.6%, churn time rises 12.9%, and mixed physical
footprint rises 15.8% despite RSS falling 13.7%. The completed comparison under
`.benchmark-results/vector-structural-selected/` combines shared catalogs with
**eager** incremental inventory against the earlier locked baseline; lazy
inventory is disabled. It uses the same recovery-qualified binary and a separate
frozen harness, preserving prior measurements. Combined 50K passes all four
timed/reclamation arms and both independent inventory audits. Median paired mixed
writes improve 3.3%, churn time 2.1%, and mixed p99 4.0%; query and
physical-footprint directions vary by pair.

All four 1M attempts finished lifecycle checks, including both independent
candidate inventories, but **1M performance qualification is incomplete**.
The last control encountered three out-of-memory errors and source-store
poisoning during ingestion; the client retried four HTTP 500s and exited
successfully. That control is excluded, leaving three clean arms and only one
complete clean pair. In that pair mixed queries improve 7.5%, writes 7.4%, p99
13.6%, and fixed churn time 5.4%; mixed RSS rises 27.0% and physical footprint
6.4%. These are single-pair observations, not a repeated scale win or evidence
for promotion.

The benchmark now rejects write errors/retries even when the client exits zero,
and the 1M gate rechecks historical 50K logs. Four regression tests pass; an audit
of all twelve timed arms in this revision finds only the last 1M control affected.
Its failed-run data and original receipts are preserved. Source heap rose during
collection before the errors, but the failing allocation is not identified.
Next, reproduce memory admission/collection scratch pressure with allocation
evidence, fix it while preserving publication and recovery fences, then rerun
fresh 1M ABBA. No defaults change. See the
[full results](../.benchmark-results/vector-structural-selected/RESULTS.md) and
[memory-failure evidence](../.benchmark-results/vector-structural-selected/MEMORY_FAILURE.md).

The current refinement is under `.benchmark-results/vector-structural-recovery/`.
`ANTFLY_SOURCE_VECTOR_LAZY_INVENTORY=1` lets an incremental-inventory store use
the totals from a validated checkpoint receipt without eagerly constructing its
occurrence map. Segment installation constructs the map before using it for
physical accounting. Missing, corrupt, or stale receipts retain full
reconstruction; preparation counters, ownership authority, payload validation,
and publication failure fences remain unchanged. All 37 source checks pass with
defaults and with shared catalogs, incremental/lazy inventory, ownership indexing,
and checkpoint receipts enabled. Investigation of the API timeout found that
both default and explicit settings already enable native ANN storage. A simulated
low-space test reproduced capacity-deferred backfill and exposed competing
in-place startup reconstruction, which failed by publishing a chunk at sequence
zero. Durable generation-repair ownership now excludes that reconstruction;
independent rebuild chunks retain their existing capture coverage. Five focused
checks pass, including restart and both model generations. The saved failed
database, capacity simulation, source patch and qualification receipts are in
[the recovery record](../.benchmark-results/vector-structural-recovery/README.md).
The pinned release passes both five-case public API suites, saved-database
recovery with update/delete and two further restarts, and both controlled
capacity/restart/resume cases without another write. The isolated and combined
50K comparisons and incomplete 1M qualification are recorded above. Per-arm disk checks include the native safety
reserve; host quietness is not required. No defaults change solely because these
switches are implemented.

The implementation and measurements are under `.benchmark-results/vector-progress-dedup/`.
`ANTFLY_SOURCE_VECTOR_SCAN_DUTY_PERCENT=50` replaces the fixed active-scan pause
with a pause equal to the preceding scan's wall duration (minimum 100 microseconds).
This policy applies only to unlocked marking; idle maintenance and planned copying
retain their existing intervals. It cooperatively limits wall-time duty rather
than promising a hard CPU quota or real-time deadline.

`ANTFLY_SOURCE_VECTOR_RESCUE_REAPPENDS=1` lets preparation protect an existing
durable payload in the current mark. The writer records protection separately;
the scanner rejoins before those entries are added to its live set and before any
copy plan is finalized. Verification iterators are invalidated before that map
can grow. This permits reuse through GC publication without another payload WAL
append. Once copying is planned, the normal append fallback remains. Cancellation
retains the original durable authority, and ambiguous primary outcomes still
fence publication. A mark containing newly protected payloads absent from the cut's live set cannot
certify its old primary epoch as fully live: some preparations may be abandoned,
so a later mark must revisit reachability. Already-live retries preserve both
verification progress and the original checkpoint proof.

New cumulative and maximum timings distinguish source lock holds, setup, planning,
copying, and publication; setup includes checkpoints, while publication includes
directory refresh, inventory, receipts, and reclamation. Avoided payload/byte and
requested-duty-pause counters expose the cost tradeoff. Nested stage totals must
not be added to the overall lock-hold total.

Scheduling, protected reuse, and their combination were measured independently
against the same 2 ms unlocked-marking baseline. All 12 timed 50K arms and all 12
exact-reclamation clones passed. The measured release passed 61 distinct storage
checks and five API lifecycle tests. Final review added a planning-allocation
failure cleanup guard and retry/reopen regression; the final source suite passes
30 checks with ownership indexing off and on, alongside the earlier 32 native
checks. The guard's separate post-measurement receipt preserves the measured binary.

Protected reuse alone reduced churn logical write I/O by 21.6%, but increased
mixed RSS by 30.9% and reduced C1 query throughput by 20.2%. The combination
completed post-workload reclamation in 8.18/8.20 seconds versus 271.37/69.77 seconds
for its controls, while readiness was 14.3% slower and C1 throughput 25.8% lower
(median paired ratios). Physical layouts and starting reclamation debt differ;
these are lifecycle comparisons rather than identical-state GC microbenchmarks.
Remaining source lock work is dominated by setup and publication, warranting
finer checkpoint/directory/inventory profiling. No performance winner is promoted;
the stronger row-bounded baseline comparison and new 1M runs remain gated.
Per-arm measurements and limitations are in
`.benchmark-results/vector-progress-dedup/RESULTS.md`. All settings remain experimental.

## Second experiment round: six independent controls

The six follow-up experiments are implemented behind process environment
controls. They are **not new production defaults**. The shared table setting,
reference identity, exact float32 reconstruction, and prepare-before-primary
commit protocol remain the same. Results for this round live under
`.benchmark-results/vector-next-experiments/` in this worktree.

| Experiment | Candidate control | What is being measured |
| --- | --- | --- |
| Append-only payload segments | `ANTFLY_SOURCE_VECTOR_APPEND_ONLY=1` | Seal source WALs without ordinary payload merging; use a separate digest-to-generation/shard directory. |
| Selective segment collection | `ANTFLY_SOURCE_VECTOR_SELECTIVE_GC=1` | Rewrite garbage-bearing segments while retaining cold segments byte-for-byte. Requires append-only mode. |
| Committed ownership replay | `ANTFLY_SOURCE_VECTOR_OWNERSHIP_INDEX=1` | Maintain a compact owner index in primary transactions and replay it through the existing primary WAL. |
| Shorter preparation exclusion and group commit | `ANTFLY_SOURCE_VECTOR_GROUP_COMMIT=1` | Decode outside the source mutex and combine waiting preparations into one durable append. |
| Artifact reads through retained snapshots | `ANTFLY_SOURCE_VECTOR_SNAPSHOT_READS=1` | Acquire a source lease after selecting a primary reference; fetch and reconstruct outside the writer mutex. |
| Adaptive location cache | `ANTFLY_SOURCE_VECTOR_ADAPTIVE_CACHE=1` | Allocate cache stripes on demand, admit repeated accesses, and release arrays through the resource governor. Requires nonzero location-cache entries. |

`SOURCE_DIRECTORY` is a checksummed, disposable metadata checkpoint containing
44-byte digest/location records, independent of vector payload files. Its
current implementation retains a hash directory in source-accounted memory,
updates it from newly sealed block metadata, and rewrites only directory bytes
at stable checkpoints. It is not a memory-free directory or a disk-resident LSM
index. Every hint must identify a segment retained by the requesting snapshot;
the reader checks the full key. Missing, corrupted, stale, or contended hints
fall back to normal lookup. Directory bytes, lookup counts, and metadata write
cost are exposed separately. Append-only chains use manifest V6 above the old
64-generation admission bound; older binaries reject the new version. The
experimental chain still has a 4096-generation safety bound, at which native
merging applies.

Selective collection first seals the source WAL and marks durable owners. It
selects segments with at least 25% dead payload bytes, or the best remaining
segment when none reaches that threshold, and copies their live contents using
the existing incremental byte budget. Other segments remain in CURRENT.
Post-cut preparations stay in the preserved WAL suffix; old query leases retain
removed files. This separates old stable runs from new update runs by age and
reclaimability; it does **not** yet predict document update frequency or maintain
separate hot/cold admission buffers. Existing physical-base layouts use full-base
collection because partial base deletion would violate the manifest contract.

The ownership experiment deliberately reuses primary transaction replay rather
than creating another independently retained event log. Successful artifact
writes and deletes update an owner-key digest to a 36-byte payload-digest/dimensions
record in the same transaction. A failed ownership mutation fences that
transaction's commit. An ownership epoch must match the primary reference epoch
before collection can scan the narrow owner prefix instead of the entire
primary store. Missing or incomplete coverage falls back to the full scan,
including after a disabled interval; fresh tables provide the intended A/B
configuration. This removes unrelated primary rows from marking, but still
visits all current ownership entries. Its WAL and compaction costs remain in
primary storage accounting. Durable catalog artifact scopes include quarantined
indexes and let GC ignore lagging serving references belonging only to dropped
scopes. Scope retirement is a shared correctness rule in every arm, including
when the optional ownership index is disabled after an earlier collection. Primary source references survive dropping the last ANN index.

Group commit adds no batching timer and does not acknowledge preparation before
fsync. Requests already waiting behind an append can share the next append,
bounded by 32 requests and 256 items (an individual larger request remains
indivisible). Primary transactions continue to commit independently afterward.
The current outer DB writer serialization may limit actual grouping; compare
`prepare_requests` with `prepare_batches`, rather than assuming a source-only
concurrency test predicts enrichment throughput. Independent decoding uses a
separate resource reservation. WAL encoding and publication still require
source writer exclusion.

Snapshot reads acquire a lease per payload after selecting its primary value.
Lease metadata uses a transient allocator instead of the caller's transaction
arena; shared source allocations serialize accounting so the last reader may
release a WAL node concurrently with successor preparation.
They do not pin a source view before the primary snapshot, which could miss a
concurrent committed reference. The experiment trades a shorter writer hold
for cloning snapshot metadata on each read; it does not yet amortize that cost
across a batch or add a caller-owned artifact reconstruction API. The adaptive
cache uses separate reservations and thread-safe backing allocation, bypasses
contended stripes, and treats eviction as a normal lookup miss.

The comparison definitions are centralized in
`scripts/vector_store_experiment_settings.py`. All next-round arms hold the
64-item enrichment batch cap constant. Selective-GC controls also enable
append-only storage and incremental GC; ownership controls enable incremental
GC; adaptive-cache controls allocate the same maximum number of entries eagerly.
This avoids attributing required dependencies to the experiment itself. The
`next_combined` arm tests their interaction. Each comparison uses fresh tables,
A/B then B/A ordering on the available host, pinned binaries and frozen scripts.
A quiet host is not required. Query failures disqualify a comparison and retain
admission-state diagnostics; independent experiments can still finish.

Validation so far: 24 source-store tests and 31 native segment tests passed with all six controls enabled,
including over-64-generation reopen, stale directory hints, selective collection
with old leases and post-cut updates/deletes, group-commit acknowledgement,
cache reclamation, and existing recovery tests. Seven DB tests passed on the revised allocator/scope implementation, including
stale completion fencing and dropping/rebuilding the last index. An existing
managed-admission test needed to await asynchronous durable checkpoint
publication rather than assume that starting finalization completed it. The
final executable also passed five public API tests covering multiple models,
updates/deletes, restart, and last-index drop/rebuild.

### Measured second-round results

All 28 small-workload arms completed: six individual experiments plus their
combination, with two alternating pairs each. Each arm used 4,000 documents,
128-dimensional deterministic enrichment, an updated embedding version,
full-text/semantic queries, and restart. Six comparisons passed their query
availability gates. The group-commit comparison failed because its second
**control** arm returned one `index_rebuilding` 503 after reporting readiness.
This is an unresolved generation-recovery lifecycle issue, also reproduced in
a preliminary append-only arm; the failed measurements are retained.

| Experiment | Observation | Decision |
| --- | --- | --- |
| Payload segments | Median paired initial/update readiness ratios 0.77/0.90; semantic throughput about unchanged; RSS ratio 1.06. | Promising, but checkpoint timing and individual readiness pairs vary. Keep experimental. |
| Selective GC | GC copied 242 KB and 100 bytes versus 2.54/5.08 MB in controls; checkpoint/directory writes offset some savings; update readiness ratio 1.27. | Measure total write amplification and retained segment count, not GC bytes alone. |
| Ownership index | Marking took 24/18 ms versus 41/25 ms; update readiness ratio 1.26. | Narrower marking works, but transactional maintenance has a cost. |
| Group commit | Both candidate arms had 979 requests and 979 durable batches. | Outer DB serialization prevented grouping; availability comparison also failed. |
| Snapshot reads | Retained reads worked; query QPS ratios were 5.97 and 0.98 in the two pairs. | The large first-pair result is not a repeatable speedup. Batch leases before claiming a performance benefit. |
| Adaptive cache | 9,312 bytes versus 8,922,208 eager bytes with zero isolated-arm cache hits; combined arms grew and served 68K/75K hits. | Demand-based allocation works. Keep pressure/reclamation tests and account for peak growth. |
| All six | Four small arms passed; update readiness ratio 1.13, semantic QPS ratio 0.89, RSS ratio 0.96. | Do not enable the combination by default. |

Ratios are candidate/control; lower readiness and memory ratios are favorable.
These are small shared-host measurements with only two pairs, not production
qualification. Detailed per-arm timings, counters, failed-query diagnostics,
and executable/script hashes are retained in
[`RESULTS.md`](../.benchmark-results/vector-next-experiments/RESULTS.md) and
[`small/summary.json`](../.benchmark-results/vector-next-experiments/small/summary.json).
The combined 50K diagnostic is complete under `scale/`; 1M qualification
remains gated on recovery/availability failures and the observed regressions.

### 50K combined diagnostic: restart gate failed

Both candidates and the first control completed successfully. The final
control failed cold-restart validation: the auxiliary enrichment table's
`semantic` ANN index reported `PostingPatchBaseMismatch` while replaying a
quantized posting patch, required rebuild, and entered quarantine. The main
50K vector index still answered its cold queries, but the harness rejected the
run because the other table had a native lifecycle failure. This is separate
from the small readiness 503. No 1M run followed this failed gate.

Only the first pair supplies a complete comparison:

| 50K / 1536D / float32 | Control | All-six candidate |
| --- | ---: | ---: |
| Initial readiness | 21.38 s | 22.20 s |
| Peak query throughput | 2,213/s | 2,352/s |
| Mixed query throughput | 169.8/s | 127.1/s |
| Mixed write throughput | 1,203 rows/s | 822 rows/s |
| Fixed churn, 10,000 row operations | 13.97 s | 17.58 s |
| Read-only peak RSS | 1.623 GB | 1.487 GB |
| Mixed peak RSS | 1.457 GB | 1.543 GB |
| Reported source writes before restart | 963.5 MB | 716.2 MB |
| Total disk after restart | 464.6 MB | 442.0 MB |
| Source-store disk including directory | 315.8 MB | 318.2 MB |

The write-byte sum includes source WAL, checkpoint, GC output and directory
counters, but excludes primary ownership/journal writes and subsequent restart
work. Total disk includes both stores and journals. Mixed phases completed
different write counts, and much of the disk difference was ANN state rather
than source payload storage. The successful second candidate ran faster than
the first during mixed traffic; its failed control prevents treating that as a
second qualified pair. Both successful candidates retained exactly 50,000
main-table payloads after updates, deletes, restoration, and restart.

Keep the controls opt-in. Fix and regress the ANN restart mismatch and the
ready-to-repair availability transition before 1M or promotion. Full metrics
and failed gates are preserved in
[`scale/Performance1536D50K-comparison.json`](../.benchmark-results/vector-next-experiments/scale/Performance1536D50K-comparison.json)
(`qualified: false`) and the [experiment report](../.benchmark-results/vector-next-experiments/RESULTS.md).

## Experimental implementation

Create a fresh standalone table with:

```json
{"num_shards": 1, "storage": {"dense_embeddings": "vector_store"}}
```

`primary_lsm` remains the default. The setting is persisted in the table catalog
and primary store, reported by table status, and immutable after creation.
Existing populated roots cannot be switched in place. Local single-shard LSM
tables are the initial supported deployment. Replication/HA, split and snapshot
paths are gated until they can carry the independent source store correctly.

`storage/artifact_payload.zig` provides the common transactional boundary.
`DocStore` converts dense artifact writes into references and reconstructs their
original envelope on reads, including cursors and multi-get. Documents, sparse
vectors and other artifact payloads retain their existing representation.

`storage/vector_payload_store.zig` owns `source-vectors/` at DB scope and reuses
the native shared vector-block/WAL implementation. References contain the
original source envelope, dimensions, and a SHA-256 identity over the logical
artifact key plus the complete versioned artifact. Different models, embedding
names, source hashes and dimensions remain distinct. Reusing an identical
artifact on retry reuses the payload identity; physical compaction does not
change the reference. Source payloads remain exact float32.

Preparation synchronously appends source payloads before the primary transaction
can commit references. Failed/ambiguous source appends fence that source owner
until reopen. A primary commit with an unresolved outcome prevents collection
until recovery. Transactions register reader ownership before taking primary
snapshots; cursors retain that ownership even after their parent transaction
ends. Collection first synchronizes primary storage and defers while any old
reader/preparation is active. This also protects the older *durable* primary
version when a newer in-memory commit has not reached disk.

The collector runs at writer reopen and explicit full DB sync, and is
available as `DB.collectSourceVectorGarbage`. It scans committed references and
publishes a replacement generation, reclaiming obsolete versions and uncommitted
preparations. Its scan, allocation and write costs count toward the experiment.
Its default is synchronous. The optional incremental collector described below
bounds payload copying per pass; marking and final publication still serialize
source access and need further work before general rollout. Table-owned embedding namespaces and their chunk
parents survive ANN index removal. Ordinary admissions with existing artifacts
bootstrap coverage through the existing generation-repair path.

The experimental mode now shares source payloads with ANN serving generations.
`source-vectors/` owns the lossless vectors. `indexes/vector-blocks/` is a compact
version map: its WAL and immutable blocks contain logical artifact keys,
dimensions, source sequence/revision, and a 32-byte immutable payload digest.
They no longer contain another corpus of exact vectors. ANN-specific RaBitQ
codes, centroids and optional compact posting projection planes remain derived
index data and still count in total disk/memory measurements.

Each serving generation pins an immutable source snapshot. Point and batched
reads follow the version map directly into the source snapshot's WAL or native
blocks, without a primary-LSM lookup, artifact-envelope reconstruction, or the
source writer mutex. Positional reads retain their bounded concurrency; mmap
views and persisted residual hints retain their existing fast paths. Source
checkpoints and collection can replace files while old query leases retain
their original bytes. A full collection refreshes the manager's source snapshot
so obsolete generations retire as their last reader exits.

Reference-map construction reads physical primary references. Mutation
certification captures all requested references in a single atomic probe
multi-get; this preserves a coherent primary tip without cloning and sorting
the entire memtable. Base construction scans references without fetching vector
payloads. The source store uses float32 or float16 plus lossless residuals,
matching the configured vector encoding; comparisons must keep encoding fixed.

The collector retains current primary references plus the **latest** committed
ANN reference per logical key, not every superseded entry still present in ANN
deltas. Old in-flight queries retain their independent source generation. The
collector remains quiescent for primary transactions and synchronizes primary
storage before collecting. It can conservatively retain source versions named
by a still-persisted ANN map after index retirement; online scope-aware cleanup
remains a rollout improvement.

This is a fresh-table experiment. Pre-consolidation experimental tables with a
payload-bearing ANN vector map fail closed with
`VectorStoreReferenceFormatRequired`; create fresh tables for comparisons.
Older binaries reject the new reference block/WAL/manifest tags. No in-place
migration or downgrade is implied by the table setting.

Table `storage_status.source_vectors` reports the latest owner observation of
source WAL/block sizes, allocator
residency, retained payloads, last-collection live/unreferenced payload bytes,
checkpoint/collection I/O, resolve demand, and reader/ambiguous-commit deferrals.
`prepare_batches`, `preparation_ns`, `durable_append_ns`, and `checkpoint_ns`
separate source preparation and sync cost from end-to-end enrichment readiness.
Cumulative counters reset on reopen; retained sizes are reconstructed. Collection
read bytes count decoded vector payloads, and collection write bytes count staged
block files; these are logical work counters, not physical device I/O or page
fault counts. Checkpoint reads count the consumed WAL prefix. Heap bytes exclude
mmap pages and request-owned reconstruction buffers. Use process
footprint/RSS as well as these owner counters. Source preparation currently adds
a synchronous durability barrier even when the primary uses asynchronous write
acknowledgement; retain the same client durability policy and include this cost.

`scripts/vector_store_disk_accounting.py` inventories **every** data-root file,
including primary storage, separate journals, source vectors, ANN serving
vectors and other indexes. Embedded transaction/replay records remain included
in primary totals; this inventory does not assign individual SSTable keys to
journal categories. It reports logical and allocated disk bytes separately.

Run the controlled driver after correctness gates pass:

```sh
python3 scripts/run_vector_store_ab.py .benchmark-results/vector-source-ab \
  --binary /absolute/path/to/antfly --pairs 2 --include-1m
```

It uses fresh tables sequentially, alternates A/B then B/A, pins binary and measurement-script hashes,
uses identical float32 serving encoding and load/query settings, and stops on
any failed arm before scaling up. The underlying qualification harness accepts
`--dense-embeddings primary_lsm|vector_store`, prepares an isolated client copy,
and verifies the table's effective setting. Each arm includes existing mixed
same-vector updates and cold/warm process restart passes. Storage-mode
qualification runs in both arms version-changing updates, deletes and restoration
of the original ground truth before restart queries. It also measures a separate
4,000-document managed enrichment/full-text table using a deterministic local
embedding provider. Both phases fence their final real batch at `full_index`;
an empty batch is not relied upon to establish readiness. Model inference speed
and semantic quality are outside this deterministic-provider measurement.
Restart is a process cache boundary, not proof that the OS page cache is cold.

Validation completed in this worktree:

- 11 source payload tests: immutable model identities, physical primary references,
  old readers/cursors, failed/ambiguous preparation without primary publication,
  both outcomes of a lost primary acknowledgement, retry, restart, read-only missing authority, and orphan/version collection.
- 4 DB integration tests: persisted mode without ANN indexes, last-consumer
  deletion and rebuild, unchanged enrichment source-hash reuse, and stale chunk
  embedding deletion.
- DocStore regression suite: 672 passed, 1 skipped, no failures or leaks.
- Runtime error ABI: 7 tests passed, including transport of storage-mode gates.
- Public API: external model/update/delete/restart tests passed in both modes;
  generated chunk-embedding/restart tests passed in both modes; 3 invalid-mode
  and deployment-gate cases passed.
- Native-HBC enrichment: both storage modes completed two 4,000-document
  versions plus 100 full-text and 100 semantic queries. Source-mode restart
  reclaimed the first version, leaving 4,000 payloads (2,048,000 bytes).
- Three status tests pass, including callback publication while the DB apply
  lock is held; the source counter survives cached-status cloning and restart.

A 50K source-mode diagnostic loaded all 50,000 OpenAI vectors, but exposed a
full ANN checkpoint failure with float32 serving encoding: beginning an optional
float16 projection session returned `Unsupported` and prevented publication.
Full checkpoint construction now handles unavailable projections consistently
with the existing delta path. The lifecycle regression verifies publication,
search, required-projection debt and propagation of corruption errors. Recovery
of the retained 50K database succeeded with the corrected binary: all 50,000
documents became query-visible and the full checkpoint published. That restart
also exposed missing cached source counters after startup writer retirement;
status publication now carries owner counters into the cache; five public API
cases and three focused status regressions pass with this fix.

The 50K diagnostic then completed two rounds of 2,000 vector updates, 1,000
deletes and 2,000 restores. Before collection it retained 54,000 payloads
(331,776,000 bytes). Reopen reclaimed 4,000 obsolete payloads (24,576,000 bytes),
leaving exactly 50,000 live payloads (307,200,000 bytes). A 20-query recall@100
sample was 0.9855 before churn and 0.9850 after churn/restart; each cold/warm pair
agreed. This small Debug-build sample is diagnostic evidence, not a performance
qualification or a claim that the recall difference is statistically significant.

The 4,000-document native-HBC enrichment phase exposed a regression in the new
cached accounting callback, reproduced in both storage modes. A stack sample
showed an enrichment activity callback trying to acquire the DB apply read lock
through LSM-statistics collection. Source counters now travel independently in
runtime status and use only the source owner's nonblocking snapshot; the callback
cannot reacquire the DB lock or fabricate unavailable LSM samples as zeros. A
regression test invokes this callback while holding the DB apply lock; all three
focused status tests pass. The complete 4,000-document workload now passes in
both modes, including source-mode restart reclamation. The harness also
waits for index provisioning before timing ingestion and retains partial receipts
when a phase fails.

Host idleness is not a prerequisite for useful comparisons. Run fresh tables
sequentially in repeated A/B and B/A order on the available host, retain every
arm, and report paired ratios and run-to-run ranges. Record concurrent host
load and process memory alongside the results. Debugger-assisted runs and runs
with checkpoint failures are diagnostic evidence, not final qualification.

The focused driver additionally samples RSS, records all files after orderly
stop and restart, pins binary/workload hashes, and runs 1,000 queries per
workload by default:

```sh
python3 scripts/run_vector_store_enrichment_ab.py .benchmark-results/vector-enrichment-ab \
  --binary /absolute/path/to/antfly --pairs 2 --encoding float32
```

Consolidation diagnostics found that source preparation, including durable
appends, took less than one second across 8,000 generated embeddings in one
4K run. This does not support attributing its tens-of-seconds readiness gap to
source fsync alone. A stack sample exposed repeated primary-memtable snapshot
cloning/sorting during certification and replay cleanup. Certification now
uses the atomic reference multi-get described above; replay retirement scans
only its append-only lanes before applying the existing atomic deletion batch.

Repeated runs also exposed an L2 quantizer append bug: a directory reader can
materialize omitted centroid-dot products as zeros, and an append extended all
required arrays while leaving that unused array at its old count (observed:
93 entries for 144 vectors). Checkpoint validation then repeatedly rejected the
leaf. Appends now discard the unused L2 array; recovered delta construction
omits that optional plane without changing distance data. A regression test
publishes and reads the resulting larger checkpoint. Final comparisons must
use this fix in both modes.

Native shadow repairs must inherit the table's source-store binding before
opening their index. Without it, rebuilding can silently recreate a payload-
bearing ANN base. Query scratch capacity must likewise follow the resolved
source encoding, not the 32-byte map encoding. The repair regression inspects
the persisted candidate map, and source-reader tests cover both float32 and
float16 blocks. Enrichment readiness requires complete index readiness and
finished backfill in addition to row counts. Public status can still precede
a background repair transition: Debug runs observed transient `IndexRebuilding`
responses even after these checks. Timed 503s are recorded without retry and
excluded from successful throughput; any such error disqualifies an arm. The
focused driver retains all arms, while the larger qualification gates scale-up.

### Consolidated source-store measurements (2026-09-06)

Six fresh pairs, alternating A/B and B/A, ran sequentially on the available
host with the same ReleaseFast binary, float32 encoding, 4,000 documents,
128 dimensions, two complete embedding versions, and 1,000 queries of each
kind per arm. The deterministic local provider isolates storage/enrichment
work from model inference. All 24,000 timed queries succeeded and all twelve
arms restarted successfully. These are small-workload qualification results,
not evidence for 50K/1M or an all-metric improvement.

| Measurement (median across six arms per mode) | primary_lsm | vector_store |
| --- | ---: | ---: |
| Initial enrichment ready | 2.518 s | 2.840 s |
| Updated embeddings ready | 2.869 s | 2.970 s |
| Full-text successful queries/sec | 2,478 | 2,602 |
| Semantic successful queries/sec | 1,836 | 1,777 |
| Semantic successful-query p99 | 0.784 ms | 0.798 ms |
| Total logical disk after stop/restart | 10.858 MB | 8.335 MB |
| Sampled peak RSS | 238.84 MiB | 238.53 MiB |
| Restart ready | 0.108 s | 0.214 s |

Median **paired** source/primary ratios were 1.127 for initial readiness,
1.127 for updated readiness, 0.942 for semantic throughput, 1.022 for full-text
throughput, 1.080 for semantic p99, 0.777 for total disk, 0.999 for RSS, and
1.976 for restart. Ratios of medians differ from medians of paired ratios.
Initial paired ratios ranged 0.493–1.330 and update ratios 0.551–1.171; the
first baseline was an outlier and remains included. The consistent result is
lower disk usage, with modest write/query overhead and a slower small-table
restart. Cache-inclusive RSS showed no clear improvement at this scale.

Receipts: `/private/tmp/vector-consolidated-enrichment-release-ab/comparison.json`
and `/private/tmp/vector-consolidated-enrichment-release-confirm/comparison.json`.
Both pin binary SHA-256
`26cd3e42fd8d167a57784fb2c83b2366a9e1580f567db48a8ae38ac56b4d5362`
and workload SHA-256
`9f086856e184850ef8673010da97b466cd7ec957b127a966e198f6756506d397`.
Do not compare these absolute ReleaseFast times with the earlier Debug
diagnostic to claim a speedup attributable solely to consolidation. The valid
ownership comparison is between modes within the same binary and workload.

The first 50K pair subsequently passed the live load, read-only curve, mixed
writes, churn, and enrichment phases. Initial readiness was 15.472 s for primary
LSM and 14.282 s for source mode; maximum measured read-only throughput was
436 versus 392 queries/sec. Total logical disk before restart was 860.7 MB
versus 516.3 MB. Source mode then aborted during restart, so these remain
**unqualified diagnostics**, and later pairs/1M were gated. The crash exposed
a status-cache merge that shallow-copied owned index error strings and arrays
into a replacement snapshot. The merge now transfers deeply cloned entries;
a regression releases the previous snapshot before reading the replacement.
The focused synthetic-status suite passes 12 tests without leaks.

Subsequent changes require new binary qualification: source bootstrap runs
are merged once when initial ANN indexing settles (ordinary preparation keeps
its append path), and float16 cold projection sessions open pinned source files
while retaining the ANN owner's I/O admission. Source checkpoint counters include
the bootstrap merge's reads, writes, and elapsed time. A native cold-read test
uses an empty reference map over sixteen source shards to verify routing and
admission. Earlier float32 receipts do not exercise the float16 cold path.
Collection also verifies every selected digest before skipping a rewrite when
the retained inventory is entirely live. A repeated-collection test asserts
that neither the manifest generation nor written-byte counter advances. This
avoids charging every clean restart for a whole-corpus rewrite. The copied 50K
failure fixture survived three Debug restarts and sixty vector queries after
the cache-ownership fix, with 50,000 live payloads (307.2 MB) after reclamation.
The final Debug binary also passed all five public API cases and three further
restarts of the reclaimed fixture (8.76, 3.45, and 3.56 seconds), with sixty
successful vector queries. Every restart retained all 50,000 payloads and
reported zero collection bytes read or written. These timings are recovery
checks, not an A/B performance comparison.

### Final consolidated binary: repeated enrichment comparison

Four additional fresh float32 pairs (A/B, B/A, A/B, B/A) used the final
ReleaseFast binary, including the bootstrap, cold-reader, status-ownership and
clean-collection fixes. All eight arms restarted and all 16,000 timed queries
succeeded. Data, dimensions, query count and workload hash match the earlier
small comparison; these results qualify this workload only.

| Measurement (median across four arms per mode) | primary_lsm | vector_store |
| --- | ---: | ---: |
| Initial enrichment ready | 2.519 s | 2.975 s |
| Updated embeddings ready | 2.894 s | 3.162 s |
| Full-text successful queries/sec | 2,750 | 2,756 |
| Semantic successful queries/sec | 1,955 | 1,918 |
| Semantic successful-query p99 | 0.740 ms | 0.765 ms |
| Total logical disk after stop/restart | 10.560 MB | 8.628 MB |
| Sampled peak RSS | 235.47 MiB | 250.07 MiB |
| Restart ready | 0.110 s | 0.212 s |

Median paired source/primary ratios: initial readiness 1.179, updates 1.080,
semantic throughput 0.972, full-text throughput 0.989, semantic p99 1.051,
disk 0.797, RSS 1.070, restart 1.933. The first baseline query arm was slow
(228 semantic queries/sec); it remains in the results. The repeated medians
show a disk benefit with remaining readiness, memory and restart costs. They
do not establish a uniform performance win or isolate individual fixes against
the older Debug measurement.

Receipt: `.benchmark-results/vector-store-history/results/vector-consolidated-final-enrichment/comparison.json`.
Binary SHA-256:
`12d8284ef004117eba065b5e70d38883a813ad8ea3b9df7d5d68bc0221e03bb2`.
Workload SHA-256:
`9f086856e184850ef8673010da97b466cd7ec957b127a966e198f6756506d397`.

The same final binary also passed two float16 pairs (four fresh arms, 8,000
timed queries, all restarts). Median paired source/primary ratios were 0.926
for initial readiness, 1.121 for updates, 0.943 for semantic throughput, 1.011
for full-text throughput, 1.096 for semantic p99, 0.611 for total disk, 1.010
for sampled RSS and 1.936 for restart. Median total disk was 12.410 MB versus
7.557 MB; semantic throughput was 2,030 versus 1,914 queries/sec. This exercises
float16 configuration without treating two pairs as precise estimates. Receipt:
`.benchmark-results/vector-store-history/results/vector-consolidated-final-enrichment-f16/comparison.json`.

## Refinements suggested by the measurements

All five refinements now have implementations for qualification. Metadata-only
reads are used by primary freshness checks, enrichment freshness checks, dense
counter updates during publication, and key-only artifact/delete scans. The
other four experiments are independently configurable on a fresh table's
server process:

| Experiment | Environment setting | Control |
| --- | --- | --- |
| Metadata consumers | `ANTFLY_SOURCE_VECTOR_METADATA_ONLY=1` (default) | `0`: resolve full payload too |
| Source location hints | `ANTFLY_SOURCE_VECTOR_LOCATION_CACHE_ENTRIES=65536` | absent/0 |
| Segment sizing by bytes | `ANTFLY_SOURCE_VECTOR_TARGET_SEGMENT_BYTES=8388608` | absent/0: fixed 128 shards |
| Incremental collection and checkpoint receipts | `ANTFLY_SOURCE_VECTOR_GC_STEP_BYTES=8388608` | absent/0: synchronous collection |
| Dense artifact publication batches | `ANTFLY_ENRICHMENT_ARTIFACT_BATCH_ITEMS=64` | absent/1: one artifact per transaction |

The metadata control forces full payload reconstruction at the metadata API;
physical key-only scans remain enabled in both arms. The comparison runners
accept `--refinement`: `run_vector_store_enrichment_ab.py` supports each of the
five settings and `combined`, while `run_vector_store_ab.py` supports
`combined` at 50K and optionally 1M. These compare control/candidate settings
on fresh `vector_store` tables with one binary and record every arm
environment. Without this option they retain primary/source A/B behavior.

The reusable experiment controllers and scale summarizer now live under
`scripts/` in this worktree:

- `scripts/run_vector_store_refinement_experiments.py --binary /path/to/antfly`
  runs all five controls separately and then together; select a Python with the
  benchmark dependencies using `--python`.
- `scripts/run_vector_store_refinement_qualification.py --include-1m` builds
  ReleaseFast, freezes the harness, runs lifecycle API tests, repeats GC and
  combined enrichment pairs, then qualifies the combined 50K/1M configuration.
  It accepts `--binary` to skip building and explicit VectorDBBench paths.
- `scripts/summarize_vector_store_refinement_scale.py /path/to/scale` reports
  completed arms, recall, latency, throughput, disk, RSS and paired ratios.

New controller runs retain scripts, logs, receipts and data under the worktree's
`.benchmark-results/` by default. Completed earlier comparison summaries and
receipts are preserved in `.benchmark-results/vector-store-history/results/`;
historical one-off diagnostics and source-edit helpers are archived separately
in `.benchmark-results/vector-store-history/scripts/`. These ignored result
directories survive reboot but are not part of git history.

Location hints contain a digest, generation, shard and validated block location;
they never own vector bytes or pointers to a retired reader. Contended cache
locks are bypassed. Segment sizing preserves mixed model dimensions, grows at
stable source publication and can shrink during collection. Publication batches
apply to plain and chunked dense embeddings; the same source-version fencing
also protects single and multimodal dense completion. Guarding is unconditional
in both table storage modes. Batch sizing changes commit granularity but keeps
the existing source preparation and primary commit durability contract.

Collection starts with a quiescent primary mark plus durable ANN ownership.
On the supported local LSM backend it synchronizes the primary WAL, including
its index, without flushing the primary memtable. This is the required recovery
boundary; forcing scalar documents into SSTables is unrelated to reclaiming
source vectors and measurably changes query performance.
It then copies a bounded number of payload bytes per maintenance pass. New
preparations remain in the WAL suffix; retrying an old orphan re-appends that
identity after the cut, so it cannot be lost at publication. Post-cut primary
readers and old ANN leases retain their selected values. WAL pressure makes
writers help finish the reserved generation before ordinary checkpointing.
The initial mark and final manifest publication are still synchronous; the
byte budget is not a bound on wall time, metadata work, or a single vector.

`SOURCE_CHECKPOINT` is a checksummed optimization receipt, never an ownership
authority. Inventory reuse requires the same source manifest and recovered WAL
tip. Skipping a liveness mark also requires the same primary reference epoch
and a digest of ANN CURRENT/WAL contents. Reference writes and deletes advance
the epoch atomically with primary commit; aborted transactions do not. Missing,
stale or corrupt receipts fall back to reconstruction. A partial copy can be
abandoned before CURRENT publication; an ambiguous publication fences the owner
and preserves output files for recovery. Post-cut orphan preparations require
a later mark rather than receiving an all-live certificate.

Accounting exposes location-cache hits/misses/bytes, current source shards,
collection steps/pending bytes/mark time, receipt hits/inventory restores and
receipt bytes written in addition to WAL, payload, compaction and heap totals.
Source heap accounting includes the collector's mark map and retained input
state. Logical read bytes include abandoned copy work; output counters describe
completed staged blocks. Disk inventories also include temporary files. Writable
startup reclaims recognized abandoned vector-store temporary outputs before
starting any builders; read-only opens preserve them. ANN WAL hashing is additional
metadata I/O and must remain included in end-to-end timings.

Focused validation so far: 19 source ownership/recovery tests, 15 DB tests
with controls, and 13 DB tests with cache, adaptive segments, incremental
collection and batching enabled passed without leaks. These include delayed
plain/chunked completion after updates and deletes, ambiguous collection
publication, delete-only ambiguous commits, retries during collection, old
readers and restart. Delayed-provider coverage also includes deletion followed
by recreation with identical document bytes, checking that the old provider
result cannot be reused as the recreated document's embedding. The collection durability regression test verifies both
primary WAL sync calls, preserves the mutable primary entries without creating
a run, abandons the backend, and recovers the current scalar document and exact
embedding. The corrected release binary also passed all five public API tests
in 17.41 seconds. Its subsequent performance qualification failed the query
availability gate described below. Measurements above and the completed 50K/1M
scale qualification below use the pinned **pre-refinement** binary.

### First refinement experiments

All six experiments completed two alternating pairs on the shared host: fresh
4,000-row, 128-dimensional float32 tables, two embedding versions and 2,000
timed queries per arm. All 24 arms, 48,000 timed queries and restart checks
passed. Each control/candidate pair used the same ReleaseFast binary, SHA-256
`701827edc4c129e30b5bd9aaefe44bb2f4005761c374357ebfb630fb88343d1c`.
Receipts: `.benchmark-results/vector-store-history/results/vector-refinement-experiments.json` and
`.benchmark-results/vector-store-history/results/vector-refinement-{metadata,cache,segments,gc,batch,combined}/comparison.json`.

These are medians of the two paired candidate/control ratios, not confidence
intervals. Lower is better for time, disk and RSS; higher for query throughput.

| Enabled change | Initial ready | Updated ready | Semantic QPS | Full-text QPS | Restart | Disk | Peak sampled RSS |
|---|---:|---:|---:|---:|---:|---:|---:|
| Metadata reads | 0.949 | 0.990 | 0.981 | 0.981 | 1.032 | 1.021 | 1.002 |
| Location cache | 0.871 | 0.967 | 0.980 | 0.990 | 1.236 | 0.982 | 1.006 |
| Byte-sized segments | 0.881 | 1.198 | 0.974 | 1.028 | 0.510 | 1.043 | 0.994 |
| Incremental GC, before WAL-only fix | 1.273 | 0.985 | 0.792 | 0.801 | 0.509 | 1.102 | 1.104 |
| Publication batching | 0.760 | 0.514 | 0.991 | 1.088 | 1.002 | 0.986 | 0.965 |
| Combined, before WAL-only fix | 0.943 | 0.820 | 0.830 | 0.838 | 0.505 | 1.072 | 1.181 |

Batching reduced source preparation calls from 7,802 to 979–980 for 8,000
payloads. Its update-readiness ratios were 0.481 and 0.546, a repeatable benefit
in this workload. The configured limit is 64, but provider windows supplied
about eight completed artifacts per actual publication batch.

Metadata-only reads reduced reconstructed artifact counts from 24,941–26,144
to 18,815–19,178, without a clear timing win. Segment sizing reduced the source
shard count from 128 to 16 and roughly halved restart time, while updated
readiness regressed in both pairs. The cache-only arms allocated about 8.9 MB
but recorded **zero cache lookups**: the small corpus used the hot WAL path.
Those timings cannot establish a cache benefit. Combined arms exercised the
cache after collection moved payloads into blocks, recording 78,930–89,460 hits
and 90 misses; larger-table qualification is needed to judge the tradeoff.

The first incremental-GC experiment reclaimed the old version (8,000 retained
payloads became 4,000), used restart inventory receipts and halved restart time,
but reduced both query throughputs by about 20%. Inspection found that the
collector's primary `sync(true)` also flushed scalar documents out of the
memtable. It now uses the supported LSM's WAL-only durability boundary before
marking. The GC and combined results above deliberately retain that failed
prototype measurement; follow-up runs must use the corrected binary. Collection
also moves vectors from the hot WAL into immutable blocks, so the corrected
durability call alone does not prove all query overhead is removed.

### Corrected collector qualification: availability gate failed

The corrected ReleaseFast binary includes WAL-only primary synchronization and
the document-incarnation fence. SHA-256:
`185dc8f81f77d8b6b77f0389e4eb2766a6aed047791931bc93bc28671645a677`.
The GC and combined reruns each completed two alternating small-table pairs.
The GC control returned two `503 index_rebuilding` responses among its first
pair's 1,000 semantic queries; the combined control returned one among its
second pair's 1,000 semantic queries. Both comparisons therefore have
`qualified: false`. All candidate timed queries succeeded, but this does not
qualify their paired throughput ratios against failing controls.

The saved public status had reported complete readiness, no active backfill,
and complete source coverage before timing. Server logs subsequently show an
index repair/rebuild overlapping the failed queries. The cause of that later
lifecycle transition still needs isolation; a timing delay or ignoring 503s
would not fix the public availability contract. Throughput also varied widely
between these arms. No corrected-GC query-speed improvement is claimed.

The enrichment comparison driver originally saved the failed qualification
flag but exited with status zero. It now exits unsuccessfully after preserving
the complete comparison when any arm has timed-query failures. Replaying the
actual failed GC receipt through its summarization/gate code confirms rejection;
the earlier successful batching receipt remains accepted. The already-started
scale continuation was stopped after discovering this missing exit-status gate.
**The combined refinements have no completed corrected 50K/1M qualification.**

The corrected small runs, server logs, and interrupted scale root are preserved
under `.benchmark-results/vector-store-history/runs/`; phase receipts and
correctness logs are in `results/`, and the pinned executable is in
`binaries/antfly-refinements-qualified`. The initial comparisons above remain
useful experiments, with batching the clearest measured benefit. Keep the
optional physical/maintenance settings gated while resolving the availability
failure and completing their scale qualification.

The ownership boundary is the intended long-term shape: primary artifact
metadata and committed stable references, a table-owned exact-payload store,
and ANN-owned version maps and derived search structures. The current physical
layout and maintenance policy are experimental. Lower disk usage alone does
not establish that their query, memory, write and restart tradeoffs are optimal.

Exact source versions are immutable objects. A useful target to evaluate is
sealed vector segments with a compact relocatable location directory: ordinary
publication appends objects and updates small metadata, while collection moves
live objects only when reclaiming worthwhile space. This could avoid merging
wide immutable payloads solely to reduce sorted-run lookup fan-out. Reusing the
current vector-block engine is appropriate for the reference implementation;
whether to specialize its source maintenance requires measured compaction I/O
and query lookup evidence. Stable artifact identities remain authoritative,
and ANN generations may retain validated physical hints into pinned segments.

The following records the motivation and acceptance criteria for each change.
Implementation alone does not establish a performance win; qualify each flag
separately and then together so effects remain attributable.

1. **Separate artifact metadata reads from payload reads.** Transparent `get`
   resolution preserves compatibility, but can hide unnecessary payload work.
   `storedOrPendingEmbeddingSourceHash` previously called the resolved store read
   and reconstructs the entire embedding to inspect a source hash already in
   the committed reference envelope. Introduce a common metadata-only artifact
   read in both modes, then audit freshness checks, version comparisons, delete
   bookkeeping and replay. Request exact values only where the consumer needs
   them. Preserve snapshot semantics, source fencing and corruption reporting;
   a metadata read does not certify that missing source bytes are healthy.
   At the end of the first final 50K source churn sequence, the owner reported
   409,560 resolved artifacts (2.516 GB reconstructed) for 54,000 prepared
   payloads. Direct shared ANN reads bypass this reconstruction counter. Some
   reconstruction is required, but this is enough volume to warrant a call-site
   audit before assuming the remaining cost lies only in physical storage.
2. **Reduce reference-resolution work without changing authority.** The ordinary
   reference path currently locates the ANN version, authenticates its digest,
   then locates that digest in the pinned source generation. Float16 residual
   hints already bypass some repeated lookup. Profile the remaining path and
   extend generation-checked physical hints or bounded location caching where
   useful. Batch resolved reads by physical block. A physical offset is a hint,
   not the durable artifact identity: generation mismatch must resolve through
   the authoritative stable reference. Do not introduce an unbounded per-vector
   heap directory or recreate a full serving payload corpus to win query time.
3. **Make segment sizing follow payload bytes.** Fixed shard/file overhead is
   significant for small tables. Consider adaptive segment targets and blocks
   with compatible dimensions/encoding, retaining shared table ownership.
   Model/version identity remains explicit, and separate indexes may consume
   the same artifact version. Physical grouping must not require one store or
   tiny file per model/index, nor impose one embedding dimension on the table.
   Reuse the existing block/WAL engine while measuring metadata residency,
   files touched per query and checkpoint write amplification.
4. **Make reclamation incremental and recoverable.** Clean collection now
   avoids rewriting live bytes, but still discovers reachability by scanning.
   A durable liveness checkpoint plus replay of committed ownership changes
   could bound clean startup work; background collection could select segments
   by obsolete-byte density. Its recovery proof must include unresolved primary
   commits, durable ANN lag, retired index scopes and old reader generations.
   Counters alone are not authority. Bound retained bytes and collector work,
   and preserve the existing conservative collector as a verification path.
5. **Batch publication where counters justify it.** Preparation currently owns
   a mutex through deduplication, encoding, successor allocation and durable
   append, and may checkpoint a full WAL before appending. Measure queue wait,
   preparation, append and checkpoint time separately before changing this.
   Group commit and preparation outside publication exclusion may reduce small
   batch costs, but every primary reference still requires its payload to be
   durable first. Existing measurements do not identify fsync as the dominant
   cause of the earlier readiness gap.

   The final float32 enrichment receipts each record 7,802 preparation calls
   for 8,000 payloads, 0.434–0.577 seconds of preparation and 0.405–0.536
   seconds of durable append time across both versions. This makes batching
   relevant to the remaining small gap even though it cannot explain the old
   tens-of-seconds delay. Preparation calls are not a measured count of fsync
   syscalls. Consider coalescing completed enrichment results before entering
   serialized primary application, with per-artifact freshness validation and
   bounded wait time; a group-commit queue cannot combine callers that an outer
   lock already serializes through the entire prepare/commit operation.

Prioritize metadata-only reads and vector-loading profiling after the 50K
comparison, then test restart/reclamation, segment sizing and publication
batching with 1M and churn.
Keep derived ANN projections where they earn their measured latency benefit;
consolidating exact source ownership does not require eliminating every useful
derived representation. Keep the table option experimental until these costs
and the unsupported deployment lifecycle paths are qualified.

The first completed final-binary 50K pair provides a more specific profiling
lead. In the 1,000-query post-restart profile, mean server search time was
2.384 ms for primary LSM and 2.762 ms for source mode. Mean rerank vector-load
time was 0.660 versus 0.978 ms, including artifact-read time of 0.475 versus
0.733 ms. Exact vector counts were close (268.1 versus 270.4 per query), as
was recall (0.98302 versus 0.98351). This points to vector loading as a useful
optimization target; the aggregate timers do not isolate digest lookup from
positional reads, cache effects or scheduling. Logical positional-read counts
also differed (257.3 versus 270.4 per query), so it is not evidence that the
entire gap is lookup CPU. Receipts are the two `public-query-profile.json`
files under `.benchmark-results/vector-store-history/results/vector-consolidated-final-scale/Performance1536D50K-1-*`.

Both final-binary 50K pairs subsequently completed all phases with no reported
mixed-workload errors and successful cold/warm restart checks. Medians across
the two arms per mode were:

| 50K measurement | primary_lsm | vector_store |
| --- | ---: | ---: |
| Initial readiness | 15.382 s | 14.359 s |
| Peak read-only throughput | 442.1 queries/s | 405.7 queries/s |
| Post-restart profile p99 | 5.183 ms | 6.312 ms |
| Live recall@100 | 0.98325 | 0.98330 |
| Total logical disk after restart | 877.8 MB | 474.8 MB |
| Read-only sampled peak RSS | 1.829 GB | 1.565 GB |
| Mixed successful query throughput | 177.7 queries/s | 152.3 queries/s |
| Mixed completed write rate | 1,400.8 rows/s | 1,211.5 rows/s |

The median paired disk ratio was 0.541, read-only throughput 0.919 and profile
p99 1.234. Mixed write-rate paired ratios varied substantially (0.581 and
1.268); report them rather than treating the median as a precise regression.
Mixed source runs wrote 28,800 and 44,300 rows versus 49,500 and 34,900 in
primary mode, so their lower mixed RSS is not an equal-work memory comparison.
Host-wide wired-memory changes also make the sampled demand proxy unsuitable
for claiming a precise process-memory improvement here. This is successful
correctness qualification with explicit performance tradeoffs. The pinned
binary has proceeded to 1M; it does not include the five new refinements.
Compact receipt: `.benchmark-results/vector-store-history/results/vector-consolidated-final-scale/50k-comparison.json`.

## Objective

Make a separate embedded vector store the durable owner of exact embedding
payloads. Keep documents, artifact identity, source freshness, and transactional
references in the primary LSM. Let ANN indexes consume those shared embeddings
and own their search-specific structures.

The vector store is part of Antfly's storage engine, with table ownership and
shard-local persistence alongside the primary store. It is not a new remote
service or a second database that applications must coordinate.

This removes full embedding payloads from ordinary primary SSTable compaction
and avoids retaining a permanent primary copy plus a separate exact-vector
serving copy. It also allows vector-specific layout, caching, maintenance, and
reclamation. Performance improvements must be measured; physical separation
alone does not eliminate vector WAL traffic, vector compaction, or ANN work.

## Ownership

| Component | Durable responsibility |
| --- | --- |
| Primary LSM | Documents; artifact identity and ownership; source version/hash; producer identity; enrichment status; committed vector references; small structured artifacts |
| Vector store | Exact embedding payloads and their immutable versions; physical placement; payload integrity; retained generations and reclamation |
| ANN indexes | Routing, postings, membership, quantized search representations, and coverage of committed artifact mutations |

An embedding artifact is independent of an ANN index. Multiple indexes using
the same artifact can share its exact payload. Dropping or rebuilding one index
must not delete the source embedding needed by another index, artifact reads,
or future rebuilds. Embeddings from different producers or source versions are
distinct artifacts even when their dimensions match.

One artifact API can eventually cover chunks, extractions, edges, assets, and
embeddings while dispatching to different physical stores. Dense embeddings are
the initial scope. Text and structured artifacts may remain compressed LSM
records; graph access may require its own structures. A common lifecycle does
not require one storage format for every enrichment kind.

## Logical references, independent physical placement

Keep each embedding's reference on its independently versioned artifact record,
associated with its parent document or chunk. Avoid a growing physical-offset
directory inside the parent document: asynchronous enrichment completion and
regeneration should not require rewriting unrelated parent fields.

Conceptually, resolution is:

```text
Primary artifact record
  artifact identity + committed artifact version + source/producer identity
                            |
                            v
Vector store version directory
  logical payload reference -> physical segment / block / row
                            |
                            v
Exact vector payload
```

The logical reference must distinguish table/shard incarnation where needed,
document or chunk identity, embedding name, and artifact version. The precise
encoding may use compact IDs. A source hash can help validate freshness; it is
not a substitute for commit identity or protection against delete/recreate
aliasing.

Physical offsets are internal to a pinned vector-store generation. Compaction
can relocate vectors without rewriting primary documents or artifact records.
Cached physical handles must retain their generation or be re-resolved when it
changes. A request for an older committed artifact version must never silently
resolve to the newest vector under the same logical key.

## Physical organization

Build on append-oriented mutation storage plus immutable, indexed vector
segments. Group payload blocks by compatible dimension and encoding, allowing
compact shared metadata and batched positional reads. Use independent integrity
checks and a compression policy suited to vector bytes, separate from primary
document and metadata compression.

The authoritative representation must preserve the exact source float32 vector,
either directly or through an exactly reconstructible encoding. Float16 or
quantized approximations alone cannot replace the exact payload under the
current scoring contract. ANN indexes may retain their own approximate
representations where those improve search locality.

The version directory can have its own compact indexing structure; its design
does not require another general-purpose LSM containing full vector values.
Bound write buffers, directory residency, read caches, and maintenance scratch
through the shared resource manager. Use streaming builders and retained input
generations so background work does not buffer an entire corpus.

SSTable-local value blocks remain a possible smaller experiment, but they have
a different boundary: if payload blocks die with their SSTable, primary
compaction still copies surviving vectors and rewrites their offsets. The
intended separate store retains payloads independently of primary SSTables.

## Commit and visibility contract

The two physical stores must present one logical commit decision. Two unrelated
successful writes do not establish atomicity.

The preferred starting protocol is payload preparation followed by publication
of the reference in the primary transaction:

1. Allocate an immutable payload version and stable operation identity. Append
   the vector and establish the recovery evidence required by the requested
   durability level. Register the preparation so reclamation cannot race it.
2. In the primary transaction, revalidate the source version, producer, and
   artifact incarnation. Commit the payload reference, artifact status, and
   replay/outbox metadata for downstream consumers together. This is the
   visibility decision. Coordinate distributed transaction intents with their
   existing commit decision rather than exposing prepared references.
3. Publish the committed version to readers and downstream index workers.
   Retire preparation ownership only after commit or abort is resolved.

Prepared payloads must remain invisible to artifact reads and ANN consumption.
An obsolete asynchronous enrichment result may leave an unused prepared
payload, but must not replace the artifact for a newer source version.

An API acknowledgement must preserve the existing sync-level contract. In
particular, a durable primary reference cannot outlive its recoverable payload:
any primary sync or checkpoint that makes that reference durable must first
establish the corresponding payload durability, or retain a durable journal
that can reconstruct it. Merely ordering unsynced writes to two files does not
provide that guarantee. `sync_level=write` must not acquire an implicit wait for
ANN completion.

A shared transaction journal carrying payload mutations and the commit decision
is an alternative implementation. Choose the protocol to fit Antfly's existing
Raft, replay, and transaction machinery. The visibility and recovery invariants
apply to either choice; this document does not prescribe an additional fsync
per vector or a new distributed commit protocol.

## Failure and recovery rules

| Failure boundary | Required result |
| --- | --- |
| Before payload preparation completes | No committed artifact reference; retry or abort safely |
| Payload prepared, primary commit absent | Invisible prepared/orphan payload; reclaim after proving no pending commit or retained owner can reference it |
| Primary commit outcome ambiguous | Resolve the existing operation through recovery; preserve possibly referenced payloads and prevent conflicting retries |
| Primary commit complete | The exact referenced payload is recoverable and readable at the committed version |
| Segment publication or compaction interrupted | Recover a complete published generation and its committed mutation suffix; retained readers remain valid |
| Committed payload missing or corrupt | Report failure or repair from an exact durable recovery source; never substitute a different version or an approximate vector |

Replay must be idempotent by stable mutation/operation identity. Local file
offsets are not portable replication identities. Every replica that exposes a
committed reference must possess its payload or sufficient retained recovery
data to materialize it. Replication and log truncation must account for both
stores' recovery progress.

The primary artifact commit sequence, vector-store physical publication
generation, and each ANN index's coverage watermark are separate concepts.
Physical payload presence alone proves neither artifact visibility nor ANN
readiness.

## Reads, updates, and deletion

Artifact reads resolve the version selected by the primary transaction or
snapshot. Search uses compatible retained index/vector views and the existing
source-visibility rules. Batch exact-vector requests by segment/block after
logical version resolution, without widening a query's visible version set.

An update prepares a new immutable vector version and atomically replaces the
artifact reference. Old versions remain available to existing readers and
recovery consumers. A document deletion or artifact invalidation commits the
appropriate logical deletion and downstream mutation; physical reclamation is
asynchronous. Recreating a document, artifact, or index must not revive stale
references from an earlier incarnation.

Reclamation must account for current committed references, retained snapshots
and query generations, pending preparations or ambiguous commits, index/replay
consumers, and backup/restore ownership. Observing no reference in today's
primary tip is insufficient proof that a payload is dead. The implemented
collector marks primary references and the latest durable ANN references,
retains post-cut preparations, and pins old serving generations. Incremental
copying and checkpoint receipts are described above. Retired ANN scopes can
still retain excess versions conservatively.

Dense completion compares the captured raw source digest inside the primary
write transaction, including parent existence for materialized chunks. It also
checks the document's persisted identity state: a deleted identity or a creation
sequence newer than the enrichment request rejects publication even if the
document bytes are identical. Both sequences use the existing derived replay
sequence domain. This reuses the document identity metadata already committed
with primary mutations; it adds no new per-document version key.

The tests interleave provider completion with changed-source updates, deletion,
and identical-content delete/recreate in both storage modes and both
plain/chunked paths, checking the actual output vector as well as its source
hash. Standalone runtime callers without DB identity metadata or a nonzero
request sequence retain content fencing only. Producer/index generation
ownership still follows the existing catalog and coverage protocol; document
identity is not a replacement for that separate boundary.

Backups capture a consistent primary snapshot plus all vector generations and
WAL boundaries needed by its references. Restore validates that closure before
exposing the table. Shard split, merge, and movement must transfer the same
logical ownership and recovery evidence; no imported primary reference may
depend on an unretained file in the old shard.

## Relationship to this branch

The branch already contains relevant machinery:

- [vector_block_store.zig](pkg/antfly/src/storage/vector_block_store.zig):
  table-level exact-vector blocks, committed WAL batches, `CURRENT`
  publication, and retained readers.
- [vector_wal_view.zig](pkg/antfly/src/storage/vector_wal_view.zig): vector WAL
  read/version machinery.
- [vector_block_manifest.zig](lib/vectorindex/src/vector_block_manifest.zig):
  vector generation metadata and coverage.
- [artifact_codec.zig](pkg/antfly/src/storage/db/enrichment/artifact_codec.zig):
  existing embedding artifact representation and source metadata.
- [VECTORDBBENCH_FINDINGS.md](VECTORDBBENCH_FINDINGS.md): measured primary-store
  costs, shared exact-vector experiments, and current qualification limits.

Reuse and evolve this shared store rather than introducing another permanent
exact-vector copy. Existing durable vector files and native ANN authority do
not by themselves prove that all primary embedding payloads can be removed.
Source ownership, transactions, repair, replication, and backup must first
support the reference-only representation.

## Experimental table setting

The optional, persisted setting at table creation lets fresh tables exercise
either ownership model with the same binary and public API. Implemented request
shape:

```json
{
  "num_shards": 1,
  "storage": {
    "dense_embeddings": "vector_store"
  }
}
```

| Mode | Source embedding ownership |
| --- | --- |
| `primary_lsm` (default) | Preserve the current primary artifact representation and existing serving behavior |
| `vector_store` (experimental) | Store exact payloads in the shared vector store and committed references in primary artifact records |

These modes select source ownership. `primary_lsm` may still use shared vector
files for serving; it does not mean disabling the existing vector read path.
Encoding, ANN configuration, scoring precision, and sync semantics remain
independent of the setting.

The setting must:

- Be immutable after table creation during initial qualification. Reject an
  attempt to change the mode on an existing table.
- Persist in catalog metadata, be reported through table metadata, and survive
  provisioning, reopen, restart, and backup/restore. Restore must preserve the
  mode or reject an unsupported format rather than silently defaulting it.
- Apply to all dense embedding artifacts in the table, including external
  embeddings and generated document/chunk embeddings. Sparse embeddings and
  other artifact kinds retain their existing storage paths initially.
- Require explicit capability admission for the selected deployment. Reject
  unsupported configurations before exposing the table; an environment flag
  must not silently change the persisted source authority.
- Preserve artifact ownership even when the table has no ANN indexes. Dropping
  the last index must leave its source artifacts available to reads and future
  index creation.

Initial performance qualification uses fresh, single-shard standalone tables.
Replication, HA, shard movement, and other deployment paths remain unavailable
for the experimental mode until their lifecycle contracts are implemented and
validated. Supported backup/restore paths must preserve reference closure; any
unimplemented path must reject the operation explicitly.

Switching an existing table requires a separate migration protocol. For the
initial experiment, returning to the default means creating a fresh default-mode
table and reloading it from the benchmark source. A runtime toggle is not a
rollback mechanism for reference-only artifacts.

## Implementation sequence and acceptance

### First milestone: fresh-table experiment

The first milestone is a small, crash-safe implementation that actually removes
full embedding payloads from primary artifact values. Migration of existing
tables is subsequent work.

1. Thread the mode through public table creation, catalog persistence,
   provisioning, and DB open. Define a common artifact payload interface for
   both modes. Separate source-store lifetime from `IndexManager`'s ANN
   lifecycle so a table with zero indexes can still own embeddings.
2. Specify stable artifact-version references, source fencing, and the
   prepare/commit recovery protocol. Reuse the shared vector store for external
   embeddings, enrichment results, updates, deletes, artifact reads, and index
   rebuilds. The experimental mode must commit references instead of full
   primary artifact payloads.
3. Prove recovery before timing the implementation. Exercise failures around
   preparation and primary commit, ambiguous outcomes, retries, stale results,
   retained readers, and restart. Verify that dropping the last ANN index
   preserves artifacts and that a new index rebuilds from them. Validate or
   explicitly gate each deployment and lifecycle path before admitting use.
4. Add per-table accounting for both stores and transaction/replay journals.
   Attribute physical bytes and retained buffers to their owners and record
   retention boundaries. A dual-write diagnostic may help verify equivalence,
   but does not qualify the reference-only design's performance.
5. Run controlled public-API A/B qualification as described below. Only then
   expand deployment support and implement existing-table migration.

### Broader rollout

1. Specify reference identity, source-version fencing, and the commit/recovery
   protocol. Inventory every producer and consumer of primary embedding bytes,
   including enrichment, external embeddings, artifact APIs, rebuilds, repair,
   backup, replication, and shard movement.
2. Add versioned resolution through the shared vector store. During transition,
   any dual representation has explicit authority and verification rules;
   fallback cannot mask missing committed data after authority has moved.
3. Move artifact commits to prepared payloads plus primary references. Keep
   vector bytes out of primary memtables and SSTables. Any temporary full bytes
   required in transaction/replay journals have explicit retention boundaries.
4. Gate reference-only authority on the capabilities of every relevant reader,
   replica, backup, and recovery path. Migrate existing artifacts with a captured
   source boundary, preserve concurrent mutations, and switch authority
   atomically before reclaiming old primary payloads.
5. Qualify correctness and performance before removing transitional paths.

Required correctness coverage includes crash injection around prepare/commit
and publication, ambiguous writes, idempotent retries, stale enrichment results,
old snapshots across update/delete/compaction, multi-index sharing, index drop
and rebuild, backup/restore, and replication/shard lifecycle recovery. Include
allocation failures and admission pressure where ownership changes occur.

Measure primary WAL/memtable/SSTable bytes, total durable bytes across both
stores and journals, compression CPU, total compaction read/write bytes, vector
read amplification, directory/cache demand, retained-version and orphan bytes,
and maintenance/replay lag. Report memory demand separately from cache-inclusive
RSS. Include overwrite/delete workloads to expose reclamation debt.

### Controlled A/B qualification

Create fresh `primary_lsm` and `vector_store` tables using the same binary and
public API. Run them sequentially on the same host to avoid mutual resource
contention, alternate run order, and repeat measurements. Record the effective
persisted mode in each result. Hold dataset/order, shard count, vector encoding,
ANN settings, exact scoring policy, batch size, writer concurrency, sync level,
and resource limits constant. A change in storage ownership must not silently
select a different search or durability configuration.

Start with public 50K qualification, then run 1M after correctness and any 50K
regressions are understood. Compare ingest and catch-up separately as well as
total readiness; query throughput and latency tails; total disk usage; memory
demand and RSS; and compaction/reclamation work. Include mixed updates and
deletes, cold and warm restart, and post-churn reclamation. Report completed
operation counts so fixed-duration runs with different write volumes are not
treated as identical memory workloads.

Also exercise normal enrichment and mixed full-text/vector use. An improvement
must not come from moving unaccounted bytes into another store, weakening
durability or exact scoring, or waiting indefinitely to reclaim obsolete
payloads. Report tradeoffs and unresolved regressions explicitly before deciding
whether the result warrants migration and broader deployment support.

## Completed pre-refinement 1M repeat qualification

Both A/B and B/A pairs completed with the same pinned consolidated binary used
for the final 50K qualification. All eight scale arms (four 50K, four 1M)
exited successfully, including mixed churn and cold/warm restart checks. The
following are medians of the two 1M arms per mode; host idleness was not required.

| Measurement, 1M rows / 768 dimensions / float32 | `primary_lsm` | `vector_store` |
| --- | ---: | ---: |
| Initial ready | 322.99 s | 326.20 s |
| Peak query throughput | 444.83/s | 442.72/s |
| Live recall | 0.9900 | 0.9899 |
| Total disk after restart | 7.588 GB | 4.490 GB |
| Sampled read-only phase peak RSS | 8.044 GB | 6.845 GB |
| Mixed query throughput | 111.45/s | 122.14/s |
| Mixed write throughput | 566.76 rows/s | 626.03 rows/s |
| Post-restart profile p99 | 91.55 ms | 83.40 ms |

This establishes a repeated disk reduction of about 41% at comparable recall
and aggregate query throughput, before the five new refinements. Two pairs do
not establish small timing differences precisely; report the individual arms
and retain host-load variation. RSS includes resident mapped pages and is not
identical to heap allocation or the configured resource budget. Mixed phases
run for equal time rather than equal numbers of writes.

Receipt: `.benchmark-results/vector-store-history/results/vector-consolidated-final-scale/1m-comparison.json`;
full provenance and exit status: `ab-runs.json` in the same directory.
