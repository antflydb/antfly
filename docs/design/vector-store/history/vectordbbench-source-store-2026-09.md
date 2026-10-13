# VectorDBBench Findings: Source-Store Qualification Narrative (September 2026)

> Relocated verbatim from `zig/VECTORDBBENCH_FINDINGS.md` (lines 14–297 at commit 3ce5aff4ea) on 2026-10-12. Durable decisions were folded into [VECTORDBBENCH_FINDINGS.md](../../../../zig/VECTORDBBENCH_FINDINGS.md) and [VECTOR_STORE.md](../../../../zig/VECTOR_STORE.md) before the move.
>
> Relative links were re-based to this file's location. Paths under `.benchmark-results/` refer to untracked local benchmark output at the repository root. Several paragraphs also appear in [experiments-2026-09.md](experiments-2026-09.md) and [review-2026-09-08.md](review-2026-09-08.md).

The subsequent source publication memory fix (`.benchmark-results/vector-source-memory-fix/README.md`)
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
mark-workspace follow-up (`.benchmark-results/vector-source-memory-admission/README.md`)
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
qualified results (`.benchmark-results/vector-source-memory-admission/RESULTS.md`).
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
full results (`.benchmark-results/vector-structural-selected/RESULTS.md`) and
memory-failure evidence (`.benchmark-results/vector-structural-selected/MEMORY_FAILURE.md`).

The catalog/inventory investigation is under
`.benchmark-results/vector-structural-refined/`. Identical-data memory-map captures
did not reproduce extra source-catalog heap or mapped-file retention. Reanalysis
of the original shared-catalog receipts gives mixed physical footprint +6.6% and
whole-run peak footprint +0.6%, versus mixed RSS +22.2%; both memory metrics must
remain visible. A separate WAL-membership prototype passed four timed 50K arms,
four reclamation checks, and two independent full-inventory reopens. Readiness
worsened 5.1% and fixed churn time 21.5% in median paired ratios, while mixed
query/write tradeoffs reversed between pairs. It is not selected.

Reopen diagnostics processed zero WAL rows while spending about a second on
inventory reconstruction. The current revision under
`.benchmark-results/vector-structural-lazy/` therefore defers occurrence-map
construction when validated checkpoint totals suffice, building the map before
segment installation needs it. Its 37 source checks pass in both configurations,
including checkpoint receipts. The follow-up under
`.benchmark-results/vector-structural-recovery/` investigates the API timeout:
default settings already use native ANN storage. Low-space simulation reproduces
capacity-deferred backfill and exposes competing in-place startup reconstruction
at an invalid posting sequence. The ownership/sequence fixes pass five focused
checks. The new pinned release passes both five-case public API suites, saved
database recovery with update/delete and repeated restart, and both controlled
capacity/restart/resume cases without another write. Initial saved recovery takes
6.04 seconds; capacity restoration resumes after 109.1 seconds in primary_lsm and
10.2 seconds in vector_store according to existing retry deadlines. Those are
lifecycle observations, not A/B timing. The subsequent 50K and incomplete 1M
qualification are recorded above. Capacity checks now include the native safety reserve;
a disk admission wait is not treated as a storage-throughput measurement.

The three structural vector-store experiments are complete under
`.benchmark-results/vector-structural/`: independent active scanning, shared
immutable segment catalogs, and incremental physical inventory. Each changes one
switch against the prior 2 ms unlocked, 50%-duty source-store baseline. All 12 fresh
50K A/B and B/A arms and all 12 reclamation clones pass, following 35 source checks
in both configurations, 32 native checks, and five API lifecycle checks. Two
inventory candidates also pass independent full-inventory reopen audits with the
cache and checkpoint inventory restoration disabled. No defaults change.

Median paired changes show different tradeoffs:

- Independent scanning: readiness +8.1%, mixed queries +0.1%, mixed writes -1.9%.
  Avoiding repeated apply visits did not produce an overall benefit.
- Shared catalogs: C1 queries +20.4%, mixed writes +4.1%, fixed churn time -7.8%,
  mixed RSS +22.2%. Those throughput/churn gains and the RSS increase repeat in
  both orders. The readiness median is dominated by an unusually slow first
  control; its cause is not established.
- Incremental inventory: publication-stage time -51.9%, mixed writes +6.7%, mixed
  queries -3.5%, churn logical writes -3.2%. Segment rows fall about 93%, but total
  inventory bookkeeping time rises; WAL traversal/map work is outside the row
  counter. Readiness and RSS changes reverse direction across pairs.

Inventory candidate reclamation settles in 2.93/2.90 seconds versus 7.19/9.53
seconds for controls. Starting layouts/debt differ, and all initial samples with
inventory already report 50,000 unique payloads, so this is a restart/reclamation
lifecycle comparison. Shared-catalog memory attribution and inventory WAL/map
cost are the next targets. A selected subset still needs comparison against the
stronger locked row-bounded baseline before new 1M qualification. Full tables,
missing-counter limitations, and audit receipts are in
the structural results (`.benchmark-results/vector-structural/RESULTS.md`).

Active scan scheduling, protected reuse, and their combined experiment are complete under
`.benchmark-results/vector-progress-dedup/`: active scan scheduling proportional
to scan duration, protected reuse of durable payloads during marking, and locked
GC stage profiling. All 12 timed 50K arms and all 12 exact-reclamation clones pass.
The measured release passed 61 distinct storage checks and five API lifecycle
checks. Post-measurement review fixed cleanup after planning allocation failure;
its retry/reopen regression brings the source suite to 30 passing checks with
ownership indexing off and on. The measured source/binary remain frozen.

Protected reuse alone cut churn logical write I/O 21.6%, with mixed RSS +30.9%
and C1 throughput -20.2%. Combined scheduling/reuse settled reclamation in
8.18/8.20 seconds versus 271.37/69.77 seconds for controls, with readiness +14.3%
and C1 throughput -25.8% (median paired ratios). Reclamation starting layouts/debt
differ, so these are end-to-end lifecycle results. Setup and publication dominate
remaining source lock work. No defaults are changed or performance winner selected.
The independent comparisons use the same 2 ms unlocked baseline; stronger locked
row-bounded comparisons and new 1M runs remain gated. Full tables and limitations
are in `.benchmark-results/vector-progress-dedup/RESULTS.md`.

The foreground-marking follow-up is implemented and qualified for correctness under
`.benchmark-results/vector-foreground-gc/`: cooperative elapsed budgeting and
primary/ANN/source snapshot scanning outside both the source writer and DB apply
locks. All 59 distinct pinned storage checks, five API lifecycle checks, eight fresh
50K timed arms, and eight exact-reclamation clones passed. The two experiments
remain optional and are not promoted:

- A 2 ms budget plus unlocked scanning: readiness +29.6%, mixed writes -11.7%,
  mixed queries -2.0%, churn logical writes +13.2% (median paired ratios).
- Unlocked scanning alone: fixed churn -8.6% and mixed writes +38.7%, but readiness
  +60.9%, mixed queries -23.8%, mixed RSS +53.2%, and churn logical writes +10.1%.

The fixed 100 ms active-worker sleep makes small scan quanta a progress-rate limit.
Separate scheduling from the quantum, profile the remaining locked work, and
protect source locations before attempting concurrent deduplication. No new 1M
run was started because neither 50K subset met the intended performance gate.
The second unlocked control omitted one source-counter snapshot; paired source
counter claims for that experiment are invalid. Timing and process I/O receipts
remain usable. Full results and exact inventory/restart evidence are in
`.benchmark-results/vector-foreground-gc/RESULTS.md`; prior 1M measurements below
belong to their prior pinned binary.

The bounded-GC follow-up is implemented and qualified for workload correctness
under `.benchmark-results/vector-bounded-gc/revisions/selective-progress/`. It
bounds primary/ANN marking and exact inventory verification by row count, protects
post-cut preparations, cancels primary snapshots before backend shutdown, and
coalesces directory snapshots after incremental location updates. Planning and
publication remain separate serialized work. Focused recovery checks, five API
lifecycle tests, and fresh 50K/1M A/B and B/A workload checks pass.

Qualification exposed two additional bugs, both now fixed with regressions that
failed before the correction. Search could fill the mutable ANN node cache during
an unfinished outer source capture; cache isolation now spans durable publication
and rollback. Selective GC could repeatedly retire empty segments while starving
sparse garbage; empty selection no longer suppresses the real-garbage fallback.
Partial-GC live/orphan accounting uses the complete mark. All four saved failure
databases and all four fresh 50K databases pass exact reclamation. All four fresh 1M
reclamation checks also pass on preserved-data clones (33–52 seconds), each with
exactly one million live payloads and zero orphan/pending bytes.

At 50K, median paired fixed-churn time fell 25.8%, mixed write throughput rose
30.2%, and directory writes fell 72.8%; churn-envelope process logical writes
rose 11.8% and mixed query throughput fell 8.6%. At 1M, fixed churn improved in
both pairs (81.2 → 62.8 s; 420.9 → 46.6 s), and matched churn-envelope writes fell
51–57%. However, mixed write throughput fell 23–69%, mixed queries also regressed,
and initial readiness worsened in both pairs. Restart tails varied sharply in
opposite directions. The options remain experimental; this is not a default
promotion. The ownership-only 50K comparison added no clear fixed-churn benefit.

The row bound worked, but one 16,384-row mark step still took 1.13 s at 1M.
Next work should budget elapsed marking time, move snapshot scanning outside the
foreground source-writer lock with fenced tail merging, and bound planning
separately. Avoiding conservative duplicate payload reappends is another remaining
write-cost opportunity. These are follow-ups, not qualified changes. Shared-host
contention and invalid source-counter envelopes are recorded in `RESULTS.md`;
paired process-sampled churn I/O remains available. Churn retains its final
full-index mutation batch, with server sync timing rather than an empty-batch fence.

Lifecycle follow-up and repeated qualification are recorded under
`.benchmark-results/vector-lifecycle-qualification/`. The saved sequence-89/90
posting-WAL mismatch was traced to AFQD V2 L2 field omission: directory
reconstruction changed the patch base's bytes without changing its search
meaning. AFQD V3 preserves omission. Following qualification, obsolete native
format readers/writers and the V2 recovery shim/fixture were removed because
these formats are new in this PR. Current-format regression tests require exact
protobuf reconstruction and strict patch base length/CRC validation. Frozen
benchmark inputs and receipts are preserved; the measurements below precede
that latest-format-only cleanup. Generated-enrichment initial-build retirement
and predecessor query admission passed the focused recovery checks. All 24
50K refinement arms passed lifecycle qualification. The selected append-only
plus selective-GC subset reduced whole-process logical writes by 42–48%,
increased mixed writes/s by 12–19%, and lowered mixed p99 by 13–25% in its two
pairs; mixed QPS fell about 5% and RSS rose. The matching 1M A/B and B/A
runs also passed all four lifecycle gates at 0.9899–0.9903 recall. They reduced
whole-process logical writes by 33–34% and readiness time by 10–25%, but fixed
changed-vector churn was 1.73–2.43× slower and mixed throughput changed direction
between pairs. The subset remains opt-in; bounded GC marking and directory
publication are the next performance targets. All four supplemental cloned-table
checks also reclaimed to exactly 1M live payloads and passed post-GC query and
publication checks. See [VECTOR_STORE.md](experiments-2026-09.md#lifecycle-fixes-and-isolated-qualification)
for the implementation boundary and qualification status.

The optional table-owned vector source and consolidation of the separate ANN
exact-vector copy are documented in [VECTOR_STORE.md](../../../../zig/VECTOR_STORE.md), including
recovery tests, byte accounting and repeated enrichment A/B receipts. The final
small-workload float32 comparison passed eight fresh arms: median paired source
ratios were 0.797 for total disk, 1.179 for initial enrichment readiness, 1.080
for updates and 0.972 for semantic throughput. These are workload-specific
results; larger-scale qualification and remaining tradeoffs are tracked there.

The five follow-up refinements now have independent controls and a combined
experiment. Their first 24 small-table arms passed all query/restart checks;
publication batching reduced updated-embedding readiness to 0.481 and 0.546
of its paired controls. The first GC prototype regressed query throughput and
prompted a fix to synchronize the primary WAL without flushing its memtable.
The design doc retains these initial results and tracks qualification of the
corrected collector. Update/delete tests also cover delayed enrichment across
identical-content document recreation, using the persisted document creation
sequence in the publication transaction.

The corrected collector's small reruns failed availability qualification:
three semantic requests in control arms returned `index_rebuilding` after
complete readiness had been reported. Their comparisons retain
`qualified: false`; the driver now propagates that failure as a nonzero exit.
The subsequent scale continuation was stopped, so corrected combined 50K/1M
results are not qualified. Reusable controllers now live in `scripts/`, with
earlier results and diagnostics preserved under
`.benchmark-results/vector-store-history/` in this worktree.

The next six experiments are now implemented and all 28 small A/B arms have
run: append-only payload segments, selective GC, committed ownership indexing,
group commit, snapshot reads, adaptive cache, and their combination. Six
comparisons passed; group commit's second control arm returned one readiness
503. The clearest accounting result was adaptive allocation: 9,312 cache bytes
versus 8,922,208 eager bytes when there were no cache hits. Selective GC reduced
payload copying but increased other checkpoint/directory work. Production
grouping remained one request per durable batch. The combined arm had 13%
slower updated readiness and 11% lower semantic throughput by median paired
ratio, so none of these results justify enabling all six together. See the
[second-round results](experiments-2026-09.md#measured-second-round-results) for
per-experiment observations and persistent receipts. The combined 50K
diagnostic attempted four arms: both candidates and the first control passed;
the final control's enrichment ANN index failed cold restart with
`PostingPatchBaseMismatch` and entered quarantine. Only one complete pair is
available. It reduced reported source write bytes by 26%, but mixed query and
write throughput fell 25%/32%. The scale summary retains `qualified: false`;
the ANN restart and readiness failures prevent 1M qualification or promotion.
