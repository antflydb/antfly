# VectorDBBench Findings

This is the working evidence log for the 50K and 1M Antfly VectorDBBench
investigation. Keep benchmark-harness changes separate from product fixes: a
vector-only control should not do full-text work, while normal Antfly users who
combine full-text and vector indexes must still get bounded memory and stable
ingest throughput.

## Benchmark contract

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
publication checks. See [VECTOR_STORE.md](VECTOR_STORE.md#lifecycle-fixes-and-isolated-qualification)
for the implementation boundary and qualification status.

The optional table-owned vector source and consolidation of the separate ANN
exact-vector copy are documented in [VECTOR_STORE.md](VECTOR_STORE.md), including
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
[second-round results](VECTOR_STORE.md#measured-second-round-results) for
per-experiment observations and persistent receipts. The combined 50K
diagnostic attempted four arms: both candidates and the first control passed;
the final control's enrichment ANN index failed cold restart with
`PostingPatchBaseMismatch` and entered quarantine. Only one complete pair is
available. It reduced reported source write bytes by 26%, but mixed query and
write throughput fell 25%/32%. The scale summary retains `qualified: false`;
the ANN restart and readiness failures prevent 1M qualification or promotion.

The representative runs use the upstream VectorDBBench runner through Antfly's
public `/db/v1` HTTP API, one shard, four concurrent load workers, batches of
100, packed little-endian float32 vectors, and `sync_level=write`. Readiness is
measured by polling the public dense-index status until all expected vectors are
published. This exercises the public server, table lifecycle, primary store,
replay journal, asynchronous derived-index worker, and dense HBC index.

`sync_level=write` does not wait for full-text or dense-index completion. It is
not `full_text` or `full_index` synchronization.

The provisioned dense-ingest guardrail is useful for repeatable internal
regressions, but it is not equivalent to VectorDBBench: it bypasses HTTP, uses a
single producer, and originally generated sorted keys. Its throughput must not
be reported as the end-to-end result.

## 2026-09-05: bounded native publication and candidate-plane experiment

This candidate extends the post-review fixes below. Performance qualification
is pending; these changes are not yet an across-the-board performance claim.

- Full native checkpoints consult native quantized entries before protobuf
  reconstruction. Necessary reconstructed values have operation-owned
  lifetimes, rather than pinning the serving patch cache for the generation.
- Online vector WAL successors share immutable transaction buffers through a
  persistent balanced version index. They parse only the new transaction;
  source-only coverage commits use the same allocation-before-append path.
  Recovery still reads and validates the durable WAL. Historical source
  boundaries and old query leases remain isolated.
- Both primary-snapshot publication and native base compaction prepare the
  suffix WAL and replacement readers outside the vector mutation mutex.
  Publication rechecks the live generation/source proof, commits CURRENT, and
  installs preallocated readers. Native compaction retries preparation up to
  three times without re-encoding its staged corpus. Obsolete-file cleanup
  runs after releasing publication exclusion. Ambiguous CURRENT publication
  fences stale in-process authority and preserves possibly published files.
- The unreleased quantized-directory format separates compact candidate
  authentication from wide float16 row authentication. Compact scans do not
  hash the entire matrix; every row used for scoring is independently checked.
  Maintenance validates all rows that it copies. Existing prototype files
  require a fresh experiment, not a claim of released-format compatibility.
- Required-but-unavailable leaf projections remain acceleration debt, even
  after a quantized-only checkpoint and without subsequent source mutation.
  Unsupported/missing sources can defer acceleration; read failures and
  corruption are propagated instead of being disguised as absent artifacts.
- Base, reshard, and sparse-checkpoint vector outputs flush bounded 1-MiB
  payload pages and retain a compact output index. This removes whole-shard
  output payload buffering and its second final encoding copy. It does **not**
  make the input spool or compact output index constant-space.

Remaining design limits are explicit: sparse WAL checkpointing still runs in
the serialized derived-mutation path, and CURRENT's durable control-file write
still requires publication exclusion. Prepared publication copies the captured
WAL suffix; it is not a sealed-extent/zero-copy WAL-tail format.

Debug evidence:

- `pr593-codecs-debug-20260905.log`: 23 direct codec tests pass, including
  multi-page authoritative float32 reconstruction and lazy-row corruption.
- `pr593-wal-ownership-debug-20260905.log`: five tests pass, including ordered
  AVL versions, exhaustive injected allocation failures, old query ownership,
  and a source append racing prepared CURRENT publication.
- `pr593-projection-debt-debug-20260905.log`: the quantized-leaf lifecycle
  retains missing-projection debt and completes it with no new source write.
- `pr593-final-manager-debug-20260905.log`: 58 focused native/manager tests
  pass. Injected error logs are expected fault-path evidence, not leaks.

## 2026-09-05: post-review native maintenance and serving fixes

### First bounded-publication qualification (rejected query-performance gate)

Run: `/private/tmp/vdbbench-pr593-design-shape-50k-20260905`, binary SHA-256
`a22720e6a62e307c974554878fb3aaea98eb2db4cad7039fcec6a1f7578197ec`.
Public batch 100, four writers, unchanged boundary rerank, c1/10/20/30:

| Metric | Previous corrected candidate | Bounded-publication candidate |
| --- | --- | --- |
| Ready seconds | 35.36 | 33.51 |
| Recall | 0.9864 | 0.9862 |
| QPS c1/c10/c20/c30 | 261/1065/1094/1173 | 99/523/620/650 |
| p95 ms c1/c10/c20/c30 | 4.28/16.12/47.48/71.95 | 11.08/35.94/81.09/98.80 |
| Load/read-only RSS GB | 2.121 | 1.775 |
| Mixed RSS GB | 4.333 | 2.498 |
| Mixed written rows | 27,600 | 43,600 |
| Mixed write rows/s | 913 | 1443 |
| Mixed query QPS / p95 ms | 151.5 / 121.4 | 140.0 / 92.6 |
| Mixed catch-up seconds | 3.77 | 1.89 |

Mixed recall was 0.97850 with no request errors; restart cold/warm serial
p95 was 9.9/9.6 ms at 0.9831 recall. RSS is cache-inclusive, not demand.
Final sampled attributable demand was 1.459 GB. This is a single-run
comparison, with differing fixed-duration mixed write volumes. It is not an
accepted all-metric improvement: read-only throughput regressed materially.

The follow-up memoizes each independently authenticated float16 row under its
immutable generation lease (one atomic byte per physical row plus per-leaf
offsets). Unseen rows are still verified before scoring; valid and corrupt
states persist only with that generation. Concurrent first readers can verify
independently without blocking on a mutex. This targets repeated CRC work
without restoring eager whole-matrix verification. The follow-up still needs
fresh performance measurement; the diagnosis is not itself an A/B result.

Final pre-benchmark Debug suites also passed: 190 public API tests and 51 dense
lifecycle tests, plus repair/status/runtime sub-suites. The first sandboxed
public API attempt failed to bind sockets (`EPERM`); the permitted rerun passed.

### Generation-scoped row verification follow-up (50K complete, 1M pending)

Run: `/private/tmp/vdbbench-pr593-verified-rows-50k-20260905`, binary SHA-256
`82d50852563e6bce4c0be6bebb7c5c9750c1a5514b9e91ad065eeb7b23ab5aa9`.
The same public batch-100/four-worker workload reached ready in 31.4874 s
(13.3956 insert + 18.0918 catch-up), with 0.9869 recall. C1/10/20/30 delivered
181.48/853.76/909.77/949.84 QPS and 7.84/25.96/73.41/85.95-ms p95.
Official post-curve serial p95 was 4.6 ms.

Load/read-only RSS peaked at 1.785 GB; mixed RSS at 2.211 GB. Mixed traffic
completed 27,900 rows (924.2 rows/s), 146.4 query QPS, 120.68-ms query p95,
0.97825 recall, and 2.83-s catch-up with no request errors. This write volume
is close to the previous corrected candidate's 27,600 rows, making its
4.333-to-2.211-GB mixed RSS reduction more informative than the earlier
fixed-duration run's substantially different volume. Demand's one final
sample was 1.400 GB, not a continuously sampled demand high-water mark.

Restart cold/warm serial p95 was 10.6/4.7 ms; recall was 0.9852/0.9865.
Restart RSS peaked at 522 MB. The detailed post-mixed warm profile measured
3.393-ms mean and 3.741-ms p95 server time, 23,878 approximate vectors and
137.3 exact completions/query. Candidate count/recall were not lowered.

This is **not** an accepted all-metric performance win. Against the previous
corrected candidate, readiness and RSS improve, but c30 QPS is 19% lower and
p95 is 19% higher. Other worktrees' Zig compilers and e2e Antfly servers were
observed during the concurrency curve; that confounds attribution but does
not establish parity. The running 1M lifecycle is diagnostic, not merge
sign-off. Recovering the prior query curve remains an explicit open gate.

Merged `origin/main` through `7f93fa0ba` in merge commit `052f03cd6`.
The fixes below remain tracked working-tree changes pending performance
qualification; this section does not claim a new latency or RSS result.

- Cold projection descriptor admission is all-or-nothing and nonblocking.
  Insufficient capacity uses the retained positional reader instead of holding
  a partial descriptor set while waiting for the rest.
- Source completion now certifies only an already-published generation.
  A separately owned `std.Io` publication task stages missing acceleration
  outside shared apply ownership. Its catalog lifetime admission resides on
  the stable index manager; shutdown joins it before destroying that manager.
  This also fixes standalone repair, which has no resident data-server
  maintenance loop. Durable source-outcome coverage remains required before
  staging a rebuilding index, and publication revalidates coverage/identity.
  Finalization ownership release also forwards a source-completion handoff
  that arrived during staging; otherwise the final write could strand
  readiness after the worker's last scan.
- Native vector compaction stages outside the vector-WAL mutex, preserves the
  exact concurrently committed WAL suffix, and leaves old query leases valid.
  A k-way merge replaces collect-and-sort for base compaction and the input
  side of resharding. Merge scratch scales with input runs, not vector count.
- Incremental posting checkpoints stream their output and carry replacement
  posting-local scan blocks. Later membership/state/quantized mutations shadow
  a replacement; dimensions and metric are validated on reopen. The root's
  nonquantized f32 payload is not decoded as a RaBitQ leaf. Publication consumes
  a durable staged receipt rather than retaining a second encoded delta.
- Patch-cache stripes hash the complete key. Decoded entries have request
  leases and physical accounting through their last reference, a 64-MiB
  per-root retention ceiling, and resource-manager eviction. Internal
  generation-scoped borrowers explicitly prevent eviction until retirement;
  those pins remain accounted and subject to admission. Scan admission checks
  quantized-value presence without reconstructing its protobuf payload.
- Native checkpoint/compaction temporary allocations use the shared compaction
  memory budget. Published readers do not retain a temporary budget allocator.
- Maintenance lease batches allocate capacity before acquiring the next pin
  and release all earlier pins on any allocation failure.

Debug evidence (before performance qualification):

- `/private/tmp/pr593-postmerge-contract-debug-20260905.log`: 51 lifecycle and
  190 public API tests passed, including recreation after corrupt artifacts.
- `/private/tmp/pr593-final-native-debug-20260905.log`: eight focused tests
  passed, covering cache eviction/accounting, streamed delta publication,
  descriptor admission, catalog admission, concurrent source writes, and
  native compaction WAL-suffix/old-query isolation.
- `/private/tmp/pr593-lease-oom-debug-20260905.log`: every injected lease-batch
  allocation failure releases all previously acquired pins.
- `/private/tmp/pr593-native-handoff-debug-20260905.log`: two tests passed,
  including deterministic delivery of a source-completion wakeup that races
  native publication ownership release.

The initial validation attempts exposed and corrected three integration
mistakes: assuming idle certification was synchronous, decoding a root f32
payload as RaBitQ, and passing an empty byte slice instead of a streaming
delta receipt. The public repair test additionally proved that merely removing
file construction from source callbacks strands standalone readiness without
an independently owned publication task. None of those failed candidates is
performance-qualified.

Next gate: fresh public-API 50K with batch 100 / four load workers, boundary
rerank, concurrency 1/10/20/30, mixed updates/queries, and cold/warm restart.
Compare against `vdbbench-pr593-native-bootstrap-50k-20260905` and the earlier
`vdbbench-searchview-final-50k-c-20260904`, holding recall and configuration
constant. Run 1M only after the 50K regression gate is understood.

## Confirmed findings

### The public create-table default adds unrelated full-text work

Creating a table with `{"num_shards":1}` automatically creates
`full_text_index_v0`. Although `sync_level=write` does not wait for it, that
index consumes replay records and performs full-text indexing asynchronously
during the vector load.

The Antfly VectorDBBench adapter now deletes `full_text_index_v0` through the
public API before it creates the external dense index. This is the default for
ANN benchmark comparability; set `ANTFLY_VDBBENCH_KEEP_DEFAULT_FULL_TEXT=1` for
an explicit mixed-index diagnostic.

This is benchmark isolation, not the product fix for mixed workloads.

### Type erasure silently changed point probes into snapshot reads

The runtime LSM deliberately has two read contracts. A snapshot read clones
the mutable generation so a multi-operation transaction remains stable across
concurrent writes. A point probe reads the current tip without cloning it.

`DocStore.get` incorrectly opened a snapshot for a single copied value and now
uses a point probe. More importantly, two generic storage adapters erased the
optional probe operation:

- `DocStore.backendStore()` did not advertise the store's `beginProbe` or
  `beginCurrentScan` operations. Transaction metadata lookups that requested a
  probe therefore fell back to `beginRead`.
- The namespace-erased store used by dense HBC indexing did not advertise
  `beginProbe`. HBC's `beginProbeOrRead` therefore also fell back to a full
  snapshot. Preserving this operation is a valid general fix, but the 50K A/B
  below showed that it was not the source of the remaining primary-store clone
  volume.

These are product bugs rather than benchmark-specific paths: any workload that
uses the generic transaction or namespaced index adapters can pay for repeated
whole-mutable-state copies.

A primary-only public-API control makes the first bug unambiguous. Before the
adapter fix, inserting 5,000 official shuffled rows took 0.951 s and issued 208
bound-read snapshot clones totaling 1,096,456,770 bytes. With probe/current-scan
operations preserved, the same control took 0.744 s and all clone classes
combined fell to 15 calls and 27,613,796 bytes: 97.5% less copied data and about
22% less elapsed time.

The first fix alone was not sufficient for dense indexing. One official 50K
vector-only run still issued 694 bound-read clones totaling 5.77 GB. Propagating
namespace probes improved readiness but did not reduce the clone counter: the
next run issued 725 bound-read clones totaling 5.63 GB. That negative result
localized the remaining cost back to the primary store.

The primary hot path was transaction intent resolution. A normal public batch
collects an intent prefix, validates its atomic revision, and resolves it under
the DB apply lock. The generic prefix helper nevertheless opened a stable read
snapshot for each scan, cloning unrelated table state.

The first replacement used a current-tip cursor. It reduced bound-read copies
to 29.8 MB in a 50K run, but a correct linear cursor had to retain the backend
mutex while walking mutable state. That serialized writers and regressed load
time to 69.95 s, so this implementation was rejected rather than hidden behind
a benchmark switch.

New transaction revisions now publish a durable, sorted intent-key manifest in
the same backend batch as their intents and transaction record. Collection and
resolution use current-tip point reads for those exact keys, avoiding both a
whole-table snapshot and a long-held scan lock. An in-flight transaction from
an older binary falls back to one stable prefix scan; its next intent write
publishes a complete manifest, preserving rolling-upgrade compatibility. The
full set of 108 transaction-related tests passes, including public HTTP,
restart, recovery, distributed coordination, transforms, and idempotent retry
cases.

The first manifest production run exposed a second lifecycle bug: resolution
deleted the manifest but retained the terminal transaction record. The recovery
worker revisited those records, could not distinguish them from pre-manifest
in-flight transactions, and repeatedly took the compatibility snapshot. An
LLDB trace captured the exact stack as `runRecoveryPageWithConfig` ->
`TxnManager.hasIntents` -> `backend_scan.scanPrefix`. That run issued 337 bound
snapshots totaling 2.39 GB. Resolution now retains an explicit four-byte empty
manifest until terminal metadata cleanup, so recovery uses the point-read path
without weakening legacy recovery.

The clone-byte figures are cumulative allocation/copy work, not simultaneous
RSS. Peak single-clone size in the diagnostic runs was about 20--22 MiB.

The accepted follow-up keeps that compatibility while batching ordinary intent
lookups with sorted `getMany` calls. The erased storage interfaces now preserve
the sorted multi-get operation too, and LSM transactions merge their private
overlay with current committed values without manufacturing a read snapshot.
This benefits normal multi-document writes and recovery, not just this load.

### Raising the WAL checkpoint floor alone is not a cloning fix

The primary WAL can be larger than the mutable table state because a document
write also persists replay metadata and embedding artifacts. A 1 MiB adaptive
checkpoint floor was associated with many small flushes and growing L0 debt, so
a 32 MiB floor aligned with the normal primary flush window is under test.

That change alone lets mutable state remain larger for longer and therefore can
make each unnecessary snapshot clone larger. It must be evaluated only after
removing the point-read cloning path. The floor is not accepted based on the
initial A/B observation alone.

### Sorted direct ingest is not representative of the load

VectorDBBench reads `shuffle_train.parquet`; request keys are not globally
sorted. The primary LSM reported direct-bulk attempts but zero direct-bulk
successes in the measured runs, with fallbacks split between a non-empty
backend and batches below the direct-ingest threshold. Optimizing only the
sorted synthetic guardrail would be a benchmark-specific shortcut.

### Dense replay memory estimates must use the configured dimension

The replay admission estimate previously assumed a 384-dimensional vector.
OpenAI 50K uses 1,536 dimensions and Cohere 1M uses 768, so the estimate could
under-reserve replay memory by 4x or 2x. Managed dense index references now
carry the actual configured vector-byte estimate, with the old fallback only
for callers that lack dimension metadata.

### Dense replay had repeated scalar reloads and unbounded finish work

Several independent HBC paths turned a logically batched replay window back
into per-vector storage traffic:

- identity and vector-id mappings were fetched one at a time through erased
  stores;
- leaf split range metadata was read through one point transaction per member;
- split and quantized-refresh work reloaded transformed vectors one at a time,
  including vectors that were already present in the active batch;
- quantized rebuild and leaf splitting could be deferred to an outer bulk
  finish, concentrating a large hidden tail in one publish operation.

The product paths now preserve sorted multi-get through both erased interfaces,
load split metadata and transformed vectors in batches, reuse the matrix that a
split already materialized, and keep live replay maintenance inside bounded
publish windows. The offline outer-finish mode remains available explicitly;
it is no longer the live public-write default.

### Replay and compaction need separate admission lanes

Deferring every compaction while a dense replay worker is active protects replay
latency but lets the primary L0 grow until public writers must compact in the
foreground. Conversely, letting primary maintenance consume all shared capacity
can starve the derived worker and inflate the replay journal. Resource admission
now distinguishes replay-priority work from soft background compaction, and the
derived backlog tracker applies a bounded 16-sequence drain window (high water
200, resume at 100) instead of allowing an unbounded wait.

The completed 50K and 1M runs show that this keeps replay lag bounded and
eliminates the old clone explosion. The 1M investigation exposed a separate
general LSM bug: persisted compaction releases the backend lock while building
its output, and concurrent flushes prepend newer L0 runs. Publication compared
the original positional slices, so it discarded valid completed work merely
because the same immutable input IDs had shifted right. Continuous traffic
could therefore starve compaction until ingestion stopped.

Publication now relocates the exact immutable input IDs and recomputes the
target-level overlap closure against the current run version. It accepts a
pure positional shift, but still rejects output if an input disappeared or a
new overlapping target run makes the plan genuinely stale. Focused tests cover
both cases. This is a storage-engine correctness/progress fix, not a benchmark
batch-size shortcut.

### The public write coalescer could starve its elected request

The coalescer elected one HTTP request as queue drainer. Under a continuously
replenished four-worker queue it kept that request inside the drain loop even
after its own entry had committed. Other requests continued to make progress,
but the elected public handler hit the client's exact 120-second timeout. This
was a public-API fairness bug, not an HBC timeout.

The drainer now hands ownership to a waiting request as soon as its own entry is
complete. A controlled concurrency test proves that the old owner returns while
a successor is still blocked. The 1M run with this change crossed the former
failure boundary without a timeout while dense replay remained within a few
hundred sequences of the source.

### Compile reservations should describe reality, not require `-j1`

ReleaseFast compilation remains normally parallel. Scheduler reservations now
cover observed macOS peaks for the API (11 GiB), storage/distributed runtime (18 GiB),
inference runtime (16 GiB), and CLI (3 GiB), plus 12 GiB for the broad data
runtime test and 14 GiB for the metadata public simulation. These are scheduling
claims, not process memory limits; they let the build graph overlap roots that
fit without requiring a global `-j1` workaround.

### Replay readers should snapshot only their append-only lane

The public table API creates `full_text_index_v0` by default. `sync_level=write`
does not wait for that index, but its replay worker still runs and previously
opened a broad current-scan transaction on every wake. That transaction cloned
the complete mutable primary state even though the worker only needed one
append-only replay-key range. At 50K with both full-text and dense indexing,
this produced 3.51 GB of cumulative copying across 296 calls.

The runtime LSM now snapshots only the requested mutable replay lane and pins
the immutable/run generation at the same backend-lock linearization point. The
merge cursor, tombstone handling, ordering, callback lifetime, and generic
backend fallback remain unchanged. Exact replay visibility also uses the
bounded all-lane iterator and compares the returned sequence; it does not infer
that sequence gaps are visible. A legacy store without replay lanes retains the
old current-scan fallback.

With both default full-text and dense indexing enabled, the final current-main
50K check copied 46.3 MB rather than 3.51 GB (about 76x less), completed in
41.78 seconds rather than 46.32 seconds, and published all 50,001 full-text
documents and all 50,000 vectors. This removes unrelated copying from the
general default-index product path; it is not a VectorDBBench-only full-text
disable.

### A lower checkpoint floor remains counterproductive

After the lane snapshot fix removed the original confounder, a controlled 50K
vector-only A/B compared the 32 MiB product default with a 1 MiB floor. The
32 MiB build completed in 38.35 seconds with 59 final L0 runs. The 1 MiB build
took 41.75 seconds, produced 147 final L0 runs and 86 flushes, and had the same
roughly 591 MB physical-footprint peak. It reduced cumulative clone bytes from
about 107 MB to 81 MB, but increased rotations and flush fragmentation. The
32 MiB default is retained.

### Compaction needs job-level high-water telemetry

Cumulative compaction time did not reveal whether maintenance consisted of
many bounded jobs or one user-visible latency and working-set spike. The LSM
now reports completed-job count, input/output byte totals, and the largest
completed input, output, and duration. Aggregation adds counters while
preserving maxima across primary and derived backends.

The instrumented 1M runs completed with full query visibility and zero final
hard debt, but observed largest jobs of 2.57--2.99 GB lasting 52.6--63.9
seconds. The 2.99 GB job exceeded the configured 2 GiB target through the
intentional oversized-single-job progress escape hatch. Simply changing the
numeric cap to 512 MiB would not bound this case: a single broad L0 source can
overlap several gigabytes of target-level runs, and rejecting the minimum
overlap-closed plan would reintroduce compaction starvation. The next design
experiment must make those closures partition-aware while retaining the
oversized escape hatch for guaranteed progress.

### Smaller leveled files do not split an overlap-closed job

A follow-up set the primary LSM's preferred run-file size to its 128 MiB
base-level target while retaining the separate 512 MiB physical admission
limit. This was deliberately a layout preference rather than a smaller maximum
record size. It produced the intended finer leveled geometry: after shutdown,
the primary L2 contained 27 runs with a 136.7 MB largest physical file instead
of a handful of roughly 512 MB files.

That geometry did not bound compaction. The 1M public-API lifecycle still
admitted 14 oversized selections, and its largest overlap-closed job consumed
2.562 GB of input, produced 2.454 GB of output, and lasted 55.15 seconds. The
run completed with all 1,000,000 vectors query-visible and zero final hard debt,
but took 1,753.77 seconds, rewrote 27.70 GB across 36 jobs, and peaked at
3.72 GB RSS / 1.50 GB process physical footprint. Those are regressions from
the 1,617.08-second, 3.19 GB RSS / 1.30 GB control, while the maximum job is
effectively unchanged from 2.57 GB. The 50K gate was also slightly slower at
43.97 seconds versus the current-main 38.72--43.14-second range.

The preference is therefore rejected. Splitting a completed compaction into
smaller output files does not split its input closure: a broad L0 range still
expands through overlapping target runs and older L0 runs before planning can
apply its byte budget. The next safe experiment must narrow persisted L0 source
ranges themselves (without workload-specific key assumptions), then combine
that with a lower soft input budget. The oversized-single-job escape remains
necessary for a minimum correct closure that cannot be divided.

The same run isolated a separate late-load pause. Status publication stopped
for about 84 seconds near 785K rows and then recovered without a client timeout.
A live stack sample showed the elected primary writer waiting in bounded
derived-backlog admission, the other public writers waiting for that group
operation, and the dense worker normalizing and splitting an oversized HBC
leaf. This was not the primary compaction above. It is one diagnostic sample,
not yet a product-change justification, but it shows that dense structural work
can consume most of the public client's 120-second timeout budget even while
the replay/backpressure invariants behave as designed.

### External indexes need an external-coverage admission fence

The public table API writes a readiness document before it creates the
caller-populated dense index. Managed admission previously treated any live
source document as proof that the new index required a full source rebuild.
That is correct for projected and generated indexes, but false for an
`external: true` dense or sparse index: unrelated source documents cannot
produce caller-owned vector artifacts. The single readiness row therefore
caused three unnecessary durable-repair attempts and added about 36 seconds to
the otherwise vector-only 50K lifecycle.

Managed admission now recognizes the external-coverage contract. It installs
the index at an activation fence only after a streaming key scan proves that no
matching caller-owned artifact predates the catalog entry, initializes its
artifact counter, and lets ordinary post-admission replay populate new
artifacts. If matching artifacts already exist, admission retains the durable
rebuild path. The public API still provides end-to-end durability and
query-visibility checks; this does not bypass replay or weaken `full_index`
synchronization.

The 50K public-API A/B completed in 42.08 seconds (34.99 seconds inserting and
7.08 seconds catching up), versus 77.90 seconds before the fix. It published
all 50,000 vectors with zero repair runs or attempts. Peak RSS was 1.42 GB,
attributable demand was 974.7 MB, and cumulative snapshot copying was 177.95 MB
across 42 calls at final status. This restores the established 30--40-second
band within normal single-run host variation while removing work that was
incorrect for every externally populated index, not just this benchmark.

### Smaller HBC replay batches trade away too much throughput

Two follow-ups tested whether bounding dense HBC apply size would reduce the
late-load leaf-normalization pause. Lowering the existing replay-window policy
to 4,096 items completed the 50K lifecycle in 92.41 seconds. It fragmented the
same source stream into 27 replay finalizations and was rejected.

A narrower prototype retained the large replay transaction but split only
known-new, insert-only HBC applies into 4,096-item calls. It lowered peak RSS
from 1.42 GB to 1.11 GB and the largest measured HBC apply from 2.35 seconds to
1.62 seconds, but increased the lifecycle to 46.45 seconds and increased HBC
finalization work. The prototype is also rejected: partial rollback across
chunks is more complex, and a roughly 10% throughput regression is not a good
general default for this memory reduction. A future HBC change should make
leaf normalization incrementally publishable or schedulable while preserving
one replay transaction, rather than fragmenting replay or its apply operation.

### Foreground pressure must honor the compaction-input budget

Background compaction selection honored `max_compaction_input_bytes`, but the
hard write-pressure path invoked an unbounded L0 selector. This made the option
ineffective precisely when L0 crossed its hard limit and public writes were
most exposed to compaction latency. The pressure path now uses the same input
budget while preserving its direct, scheduler-independent progress lane and
the oversized minimum-closure escape. Tests cover a fitting bounded window,
explicit no-oversize overload, and minimum-job progress when no correct closure
fits.

A 50K public-API gate with a 768 MiB experimental budget completed in 42.23
seconds (35.53 seconds inserting and 6.70 seconds catching up), essentially
unchanged from the 42.08-second external-admission result. Its one pressure job
was 235.8 MB, all 50,000 vectors were visible, and there was no overload.

The 1M lifecycle rejected 768 MiB as the product default. It completed with all
1,000,000 vectors visible and zero final hard debt, but took 1,854.21 seconds
(1,830.20 seconds inserting plus 24.01 seconds catching up). Twenty pressure
events completed 21 steps without overload, yet the smaller windows rewrote
27.08 GB and a late indivisible closure still reached 2.753 GB / 58.5 seconds.
Peak RSS was 3.64 GB, the process physical-footprint ledger peaked at 1.40 GB,
and cumulative mutable-snapshot copies were 1.96 GB. The experiment therefore
keeps the foreground-budget correctness fix but restores the 2 GiB product
default; lowering the number alone increases rewrite frequency without solving
broad shuffled-key L0 ranges.

The 2 GiB/default-budget 50K gate completed in 41.63 seconds (32.50 seconds
inserting plus 9.13 seconds catching up). It did not reach hard pressure, all
50,000 vectors were published and query-visible, and conservative attributable
demand peaked at 1.31 GB.

The isolated 2 GiB/default-budget 1M control retained the option fix and
completed in 1,603.35 seconds (1,545.39 seconds inserting plus 57.95 seconds
catching up). All 1,000,000 vectors were published and query-visible, replay reached
10,001/10,001, and repair remained clean. Eighteen pressure events completed 19
steps with no overload and left 57 L0 runs / zero hard debt. Compactions
consumed 29.31 GB and produced 22.48 GB; the largest minimum closure was still
2.599 GB / 51.60 seconds because the oversized progress escape correctly
admits an indivisible closure. Mutable-snapshot copies totaled 1.81 GB. Peak
RSS was 5.63 GB including reclaimable file cache, while the process physical-
footprint ledger peaked at 896 MB and conservative attributable demand at 1.90
GB. This is within the timing band of the 1,617.08-second instrumented control,
but makes the documented input-budget policy effective under hard pressure and
preserves liveness without changing the general default.

## Measurements

Unless a row explicitly reports a mean and range, the times below are
one-machine diagnostics rather than publication-grade means.

### Posting segment/WAL query gate

The earlier `spfresh-v2` diagnostic established that write throughput alone is
not a valid acceptance criterion. On a synthetic 5K-by-1536 public-shaped
workload, the first lazy segment implementation improved ingest from 377.6 ms
to 244.8 ms and reduced measured writes from 671.6 MB to 303.1 MB, but query
p50 regressed from about 0.32 ms to 6.65 ms and recall changed. Publishing a
coherent posting base, centroid directory, and RaBitQ checkpoint removed the
tail replay and restored query behavior (p50 0.309 ms, p95 0.422 ms, recall
0.188 versus 0.190), but also gave back nearly all of the write gain (368.9 ms
and 660.4 MB). The next design therefore uses an immutable packed checkpoint
plus a bounded committed WAL overlay; an unbounded lazy base/delta tail is
rejected even when its load number is attractive.

The current-main internal HBC reference now measures the same read invariants
before and after reopen. For 5,000 synthetic 1,536-dimensional vectors loaded
in batches of 100, one sample reported:

| Phase | p50 | p95 | p99 | QPS | sampled recall@10 |
| --- | ---: | ---: | ---: | ---: | ---: |
| immediately after ingest | 0.305 ms | 1.548 ms | 2.718 ms | 1,887 | 0.680 |
| after close/reopen, warm | 0.444 ms | 3.082 ms | 4.070 ms | 1,052 | 0.680 |

The internal online-coalesced build took 1.495 seconds (3,343 vectors/s) and
reported 222.9 MB written. This is not the public HTTP VectorDBBench load path
and must not be compared directly with the 50K lifecycle times below; it is a
fast query/recall regression gate for storage-format experiments. The cold
first query after reopen was 14.7 ms, which is tracked separately from the
warm distribution rather than hidden in an average.

A closer `public_ingest` arm now uses the derived replay path's external-vector
loader, known-new/coalesced batches, deferred RaBitQ rebuild policy, and one
bulk publication session. Three current-main 5K-by-1536 samples averaged
233.3 ms to build (21.4K vectors/s; 229.5--238.5 ms range) and 33.74 MB written.
Recall@10 was 0.670 in every before/after-reopen sample. After reopen, warm
query p50 averaged 0.340 ms, p95 1.405 ms, p99 1.756 ms, and throughput about
1,920 QPS. Immediately after ingest, p50 averaged 0.339 ms and p95 1.597 ms.
This is the matched internal gate for future storage arms; public HTTP E2E
latency and the real VectorDBBench corpus remain required before promotion.

The first current-main shadow checkpoint experiment exported the final packed
node and RaBitQ values after that same 5K public-ingest build. Fifty-six live
postings occupied a 2.098 MB immutable segment, versus 33.741 MB written while
building through the LSM (about 16.1x less final live payload than cumulative
write traffic). Three checkpoints took 9.69--10.13 ms and segment admission
took 7 microseconds. This does not yet measure the cost of appending/fsyncing a
WAL or replace the LSM at runtime, but it confirms that checkpoint generation
is small relative to the roughly 230--245 ms build.

The first reader recomputed CRC32 on every point access and measured about 52
microseconds p50, which would be unacceptable across many postings per query.
The format reader now keeps atomic per-entry verification state: the first
base-plus-RaBitQ verification was about 58 microseconds p50 in a follow-up,
while already-verified point lookups were below the benchmark clock's
1-microsecond resolution at p95 and 1 microsecond at p99. Verification failures
are memoized as well as successes. The runtime design should either preverify
hot/pinned postings or let their first cache admission pay this cost once; it
must never checksum an unchanged payload on every query.

The public qualification harness now keeps query behavior in the same evidence
bundle as load and memory. A current-main 50K vector-only control inserted in
28.88 seconds and became ready in 37.55 seconds. Its live public-API serial
search measured 98.49% recall@100 with 2.5 ms p50, 4.2 ms p95, and 9.7 ms p99.
The short concurrency curve peaked at 481 QPS with four clients; sixteen
clients were already saturated at 356 QPS and 452.9 ms p95. After a clean
restart, recall was 98.06%, while cold-cache latency rose to 3.9 ms p50,
9.8 ms p95, and 412.6 ms p99. The process was fully caught up by the final
status sample. Segment/WAL candidates must therefore preserve recall and beat
both live and post-reopen latency distributions, not only the load timer.

The first durable posting-store layer now publishes in the order immutable
segment, empty next-generation WAL, then checksummed `CURRENT`; old artifacts
are deleted only after `CURRENT` is durable. WAL appends require an initial
checkpoint, expose only complete committed batches, poison an ambiguous writer
after an append/sync error, and atomically discard incomplete or uncommitted
tails before accepting another append after reopen. This is storage-level
plumbing only: HBC still uses the LSM until transaction-aware runtime wiring can
prove that a checkpoint and WAL tail cover the same source sequence.

| Run | Insert | Ready/load | Notes |
| --- | ---: | ---: | --- |
| 50K OpenAI, batch 100, four workers, original public path | 46.39 s | 48.42 s | Default full-text index present |
| 50K OpenAI, batch 100, four workers, vector-only, 32 MiB checkpoint-floor experiment | 40.36 s | 42.38 s | Public HTTP; dense ready at 50,000 |
| 50K OpenAI, vector-only, first erased-adapter fix | 35.25 s | 41.29 s | Still 6.10 GB cumulative clones; exposed namespaced-adapter fallback |
| 50K OpenAI, vector-only, namespace probe propagation | 32.28 s | 32.31 s | 5.93 GB cumulative clones remained; disproved namespace path as clone source |
| 50K OpenAI, vector-only, lock-held intent cursor (rejected) | 67.93 s | 69.95 s | Bound-read clones fell to 29.8 MB, but backend lock contention serialized writers |
| 50K OpenAI, vector-only, intent manifest before terminal marker | 63.75 s | 74.41 s | Recovery re-scanned retained terminal records: 337 bound clones / 2.39 GB |
| 50K, bounded clones, first safe candidate | 65.10 s | 87.61 s | 238.7 MB clones; memory fixed but speed regression unacceptable |
| 50K, batched identity/mapping reads | -- | 59.12 s | Removed dense scalar mapping traffic |
| 50K, prefix-compressed batch block reuse | 28.03 s | 36.33 s | 83.7 MB clones; first bounded-memory recovery of the old speed band |
| 50K, HBC routing cache | 32.15 s | 38.46 s | 169 MB clones; 737 MB demand, 1.91 GB RSS |
| 50K, bounded backlog candidate | 31.61 s | 46.15 s | 166 MB clones; replay finish tail still visible |
| 50K, batched quantized refresh | 41.46 s | 43.81 s | 202.8 MB clones; 534 MB demand, 1.72 GB RSS |
| 50K, fair public coalescer | 41.40 s | 48.36 s | 157.2 MB clones; 636 MB demand, 1.79 GB RSS; timing contaminated by concurrent host compilation |
| 50K, fair maintenance + rate-limited sampler | 27.78 s | 29.88 s | 249.0 MB clones; 673 MB demand, 1.89 GB RSS; zero final replay lag |
| 50K, relocated compaction publication | 25.05 s | 27.53 s | 103.7 MB clones; 708.5 MB demand, 1.96 GB RSS; 59 final L0 runs / zero debt |
| 50K, clean merged control, vector-only | 27.14 s | 40.35 s | 168.5 MB clones; 601.9 MB physical footprint, 1.85 GB RSS |
| 50K, replay-lane snapshot, vector-only, current-main three-run mean | 30.73 s | 41.65 s (38.72--43.14 s range) | 92.1 MB mean clones; 531.0 MB mean physical footprint, 1.67 GB mean RSS; complete visibility, no overload |
| 50K, clean merged control, full-text + dense | 41.79 s | 46.32 s | 3.51 GB clones; both indexes ready |
| 50K, replay-lane snapshot, full-text + dense, current main | 32.90 s | 41.78 s | 46.3 MB clones; 702.4 MB physical footprint, 1.66 GB RSS; both indexes ready |
| 50K, replay-lane snapshot, 1 MiB checkpoint floor (rejected) | 28.40 s | 41.75 s | 80.5 MB clones; 147 final L0 runs / 86 flushes; no physical-footprint benefit |
| 50K, 128 MiB preferred primary runs (rejected) | 37.41 s | 43.97 s | Finer output layout; 1.61 GB RSS; slightly slower than the current-main range |
| 50K, external-coverage admission fence | 34.99 s | 42.08 s | Zero repair attempts; 177.95 MB clones, 974.7 MB demand, 1.42 GB RSS |
| 50K, 4,096-item replay windows (rejected) | 55.46 s | 92.41 s | 27 replay finalizations; 769 MB demand, 1.48 GB RSS |
| 50K, internal 4,096-item HBC applies (rejected) | 39.13 s | 46.45 s | 1.11 GB RSS but slower and more complex; prototype reverted |
| 50K, foreground 768 MiB pressure budget | 35.53 s | 42.23 s | One 235.8 MB pressure job; zero overload; 722 MB demand, 1.55 GB RSS |
| 50K, foreground budget wired, 2 GiB default | 32.50 s | 41.63 s | No hard-pressure event; 186.3 MB table clones, 1.31 GB demand, 1.50 GB RSS |
| 1M Cohere, batch 100, four workers, original public path | incomplete | projected about 45–50 min | Throughput fell from about 30.8K docs/min in minute one to about 21.1K docs/min in minute four |
| 1M, bounded clones + fair coalescer control | incomplete at 783,201 rows / 1,867 s | -- | Two exact 120 s timeouts from primary L0 pressure; 3.83 GB clones, 1.40 GB demand, 3.76 GB peak RSS; 200 ms `vmmap` and overlapping compilers contaminate speed |
| 1M, rate-limited sampler + maintenance fair turn | incomplete at 592,001 rows / about 725 s | -- | No timeout, but live dense bulk mode still reached 537 aggregate L0 runs; 1.59 GB clones, 888 MB demand, 3.02 GB peak RSS |
| 1M, primary + dense hard pressure before relocation | 1,536.04 s | 1,644.37 s | 825 final L0 runs / 582 debt; 16 compactions from 26 pressure events, 1.60 GB demand, 3.89 GB RSS |
| 1M, relocated compaction publication | 1,099.85 s | 1,110.66 s | Full E2E completion; 10.81 s catch-up, 111 final L0 runs / zero debt, 1.20 GB demand, 4.21 GB RSS |
| 1M, instrumented replay-lane snapshot | 1,605.87 s | 1,617.08 s | Full E2E completion; 1.65 GB clones, 1.30 GB physical footprint, 3.19 GB RSS; zero debt; 2.57 GB / 52.6 s largest compaction |
| 1M, 128 MiB preferred primary runs (rejected) | 1,751.71 s | 1,753.77 s | 2.07 GB clones, 1.50 GB physical footprint, 3.72 GB RSS; zero debt; 2.562 GB / 55.15 s largest compaction |
| 1M, foreground 768 MiB pressure budget (rejected) | 1,830.20 s | 1,854.21 s | 27.08 GB compaction input; 2.753 GB / 58.5 s largest minimum closure; 1.40 GB ledger, 3.64 GB RSS; zero debt |
| 1M, foreground budget wired, 2 GiB default | 1,545.39 s | 1,603.35 s | 29.31 GB compaction input; 2.599 GB / 51.60 s largest minimum closure; 896 MB ledger, 1.90 GB demand; 57 L0 runs / zero debt |

The original partial 1M run reached about 105K documents with 542 flushes, 212 L0 runs,
and roughly 23.6 GB of cumulative mutable-snapshot clone bytes. Dense HBC work
accounted for only about 21 seconds of the roughly 260-second sample, pointing
to primary-store/replay work as the dominant slowdown.

The later 1M control passed the coalescer's original early starvation boundary,
but was stopped after two later exact 120-second timeouts made a full timing
invalid. At capture it had 783,201 source rows, 773,400 indexed rows, and only
98 replay sequences of lag. The primary had 665 L0 runs / 4.03 GB against a
256-run hard limit, 733 flushes, and nine write-pressure compactions. A sampled
timeout stack showed the maintenance worker inside LSM compaction while public
handlers waited in bounded derived-backlog admission. This distinguishes the
remaining large-compaction latency from the already-fixed coalescer ownership
starvation.

The partial control's 3.83 GB of clone work at 783K rows is about 46x fewer
bytes per row than the original 23.6 GB at 105K, but it is still cumulative
work worth reducing. Its attributable demand peak was 1.40 GB, peak RSS was
3.76 GB, and the conservative demand-plus-host-wired diagnostic was 1.90 GB.

Rate-limiting `vmmap` and adding a fair maintenance turn improved the next 1M
curve materially: it reached 592K rows in about 725 seconds without a timeout,
versus 435K in 592 seconds and 517K in 782 seconds in the prior control. It was
still stopped because aggregate L0 grew to 537 runs / 2.21 GB with 349 runs of
hard-limit debt. The dense HBC LSM remained in bulk transaction mode throughout
live replay, and the default bulk policy suppresses hard write pressure for a
finite offline builder. A continuously replenished public replay stream is not
finite, so the dense profile now keeps bulk coalescing while explicitly
preserving hard L0 enforcement.

Enabling hard pressure for both primary and dense profiles produced the first
complete diagnostic, but did not make every assist productive. It took
1,536.04 seconds to insert and 108.33 seconds to catch up (1,644.37 seconds
total), finishing with 825 aggregate L0 runs / 582 runs of summed hard-limit
debt. Only 16 compactions published across 26 pressure events. This mismatch
led to the unlocked-publication investigation above.

With input-ID relocation, all 24 pressure events in the next full run published
24 compactions, with zero overloads. The aggregate L0 repeatedly crossed the
summed bound by a few runs while compaction was in flight, then recovered under
continued writes; it finished at 111 L0 runs / zero debt. Upstream
VectorDBBench reported 1,099.85 seconds of insertion and 10.81 seconds of public
readiness catch-up, or 1,110.66 seconds (18.51 minutes) total. All 1,000,000
vectors were published and query-visible, with no request timeout. This is
32.5% faster than the immediately preceding full run and 63% faster than the
reported roughly 3,000-second run. The host remained available to unrelated
work, so it is a contended-host result rather than a clean throughput ceiling;
it does not yet establish the hoped-for roughly 13-minute result.

The relocated-publication run peaked at 1.20 GB attributable physical
footprint, 4.21 GB RSS, and 2.31 GB in the separate footprint-plus-host-wired
diagnostic. It copied 2.09 GB of mutable snapshots across 324 calls, with a
25.2 MB largest copy. Compared with the preceding full run, attributable demand
fell 25%, clone bytes fell 22.6%, and the dense catch-up tail fell 90%; RSS rose
about 8%, reflecting a larger cache-inclusive peak rather than allocator
demand.

The latest clean bounded-memory 50K result is 27.53 seconds ready, faster than
both the 29.88-second safe result and the earlier 32.31-second clone-heavy
result. It copied 103.7 MB rather than roughly 6 GB of mutable state and ended
with zero replay lag and zero L0 hard debt. The recovered result confirms that
the 30--40-second target was conservative; 87 seconds was not redefined as
success.

The follow-up 1M lifecycle is not evidence that 1,617 seconds is the new
expected throughput. A same-code point-visibility variant completed in 1,572
seconds, while the earlier relocated-publication lifecycle completed in 1,111
seconds; both follow-up runs performed roughly 21--29 GB of compaction input
and admitted multi-gigabyte individual jobs. Their dense indexes stayed close
to the source, both finished with complete visibility and zero debt, and the
visibility variants differed by only about 3%. The large run-to-run wall-time
spread therefore remains a compaction scheduling/rewrite-amplification finding,
not a reason to weaken replay correctness or redefine the target upward.

## Immutable posting segment and WAL experiment

The first read-serving prototype now snapshots packed HBC nodes, posting
maintenance state, and RaBitQ checkpoints into one checksummed immutable file.
It publishes that file, an empty next-generation WAL, and a checksummed
`CURRENT` pointer in crash-safe order. Activation requires exact upstream
source-sequence coverage. Any ordinary write disables the sidecar before its
transaction starts, so the initial checkpoint experiment cannot serve stale
derived state.

On the internal public-ingest-shaped 5K x 1,536-dimensional workload, the
segment was 2.10 MB versus 33.74 MB written by the HBC LSM build and took about
19 ms to publish. Across matched 500-query samples, recall@10 remained 0.696.
Warm no-metadata latency was neutral: median p50/p95/p99 was
0.278/0.968/1.601 ms with the segment and 0.277/0.972/1.614 ms with the LSM,
while median throughput was 2,492 versus 2,484 QPS. The median cold first query
fell from 12.41 ms to 6.63 ms, with storage reads falling from 246 to 79 and
bytes from 5.33 MB to 0.49 MB. The remaining reads are source-document metadata
needed by the external vector loader, not HBC node or quantized payloads.

Startup initially read and checksummed the segment twice. Retaining the bytes
from store admission cut median activation from 8.24 ms to 4.18 ms. A later
three-sample run measured about 12.1 ms from the start of HBC reopen through
the first completed sidecar query, less than the LSM control's 12.41 ms query
alone. A mapped or range-backed reader remains worth exploring at larger
segments, but eager single-read admission is already small enough for the 5K
prototype and preserves lazy per-payload checksum verification.

Synthetic committed WAL tails make the next trade-off explicit:

| Tail | WAL bytes | Append time | Activation | Cold query | Warm p50 / p95 / p99 | Warm QPS |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 batches | 0 | -- | 4.18 ms median | 6.63 ms median | 0.278 / 0.968 / 1.601 ms median | 2,492 median |
| 100 batches | 3.83 MB | 35.3 ms | 19.3 ms | 2.66 ms | 0.294 / 1.010 / 1.628 ms | 2,415 |
| 1,000 batches | 37.63 MB | 2.15 s | 163.5 ms | 3.19 ms | 0.322 / 1.411 / 2.154 ms | 2,013 |

The tail rows are single diagnostic samples and rewrite one full posting
snapshot per synthetic batch; they are not claimed as public VectorDBBench
throughput. They do show that reopening and validating the segment for every
append was wrong (100 batches originally took 878 ms), that a retained writer
and non-fsync derived appends reduce that to 35 ms, and that an O(1)
posting-kind latest-value index is necessary on the query path. They also show
that a 37 MB tail is already too deep for this 2.1 MB segment: recovery and
warm tail latency both regress substantially.

The next prototype captures the posting ids touched by one authoritative HBC
transaction, waits for that transaction to commit, then snapshots only those
committed postings into one derived-WAL batch. Kind-specific tombstones prevent
a missing posting-maintenance or quantized value from falling back to an older
checkpoint value. Any failure before source commit cancels capture; any failure
after source commit leaves the source journal authoritative, and exact sequence
admission rejects the incomplete sidecar until replay repairs it.

A first three-sample qualification applied 1,000 direct existing-ID reinserts
in ten batches after checkpointing. Each isolated sample produced 499 WAL
records and an 8.52 MB tail. Median total mutation time was 733.35 ms:
673.00 ms in the authoritative HBC apply and 58.56 ms capturing/appending the
derived WAL. Full posting snapshots therefore added about 8.7% over the apply
time and made the tail four times larger than the 2.10 MB checkpoint. This
validated transaction capture but rejected full snapshots as the final
steady-state encoding.

The derived WAL now encodes replacement payloads as checksummed copy/literal
deltas against the checkpoint or preceding replacement. Eight-byte anchors at
a four-byte stride find unchanged packed-array runs even when insertion shifts
the remainder of the payload. Explicit coverage records advance the source
sequence when a committed transaction touches no posting payload, and
kind-specific tombstones prevent fallback to stale values. The sidecar is
admitted only at the exact durable source sequence. A short, corrupt, or
ahead-of-source tail is rejected and rebuilt from the authoritative source
journal. The first policy checkpointed at a 1 MiB tail, at 64 MiB
unconditionally, or when the tail reached half the checkpoint size; the
large-corpus results below supersede that deliberately aggressive prototype.

The direct-reinsert workload is supported internally but is not the public
table replacement contract. The public path atomically deletes the old
assignment and inserts the replacement. That exposed two independent fast-path
problems. Treating transaction-local deletes as proof that all writes were new
let grouped insertion route the complete replacement batch against one
pre-insert topology. Restricting grouped routing to callers that knew the ids
were absent before the transaction preserves the existence-lookup saving
without selecting the topology-sensitive grouped algorithm. Replacement also
rewrote quantized payloads repeatedly as individual delete/insert steps changed
the same postings. The replacement transaction now queues touched postings and
rebuilds their quantized payload once at commit.

Query latency is part of the same qualification, not a separate microbenchmark.
Freshly reopened LSM and freshly reopened segment-plus-WAL reads produced the
same result digest and recall in every sample:

| Reopened query path | Cold first query (median) | Warm p50 / p95 / p99 (median) | Warm QPS (median) | Recall@10 |
| --- | ---: | ---: | ---: | ---: |
| Authoritative HBC LSM | 14.15 ms | 0.268 / 0.683 / 1.141 ms | -- | 0.710 |
| Immutable segment + 4.54 MB raw stress tail | 3.03 ms | 0.267 / 0.692 / 1.221 ms | -- | 0.710 |

The sidecar preserved the exact ranked-result digest—not merely recall within
a tolerance—in every paired LSM/sidecar run. Warm latency remained neutral and
the cold query improved by about 79%. The 51.1 ms activation above deliberately
reopened an oversized raw tail; production would have checkpointed it once it
exceeded half of the 2.36 MB segment.

With one deferred quantized rebuild per replacement transaction, ten
100-vector replacement batches took 266.7 ms in authoritative HBC apply and
125.7 ms in derived capture on the diagnostic 5K x 1,536-dimensional workload.
Default boundary-rerank recall reached 0.710. The same final corpus built fresh
reached 0.675, while forcing a global quantized rebuild cost another 104.8 ms
and regressed recall to 0.628. The updated implementation therefore retains the
existing rerank-policy boundary and avoids a global refresh or a wider
candidate window: neither is justified by quality or latency.

The efficient product design is therefore a packed immutable checkpoint plus
a shallow, buffered derived WAL, not another general-purpose LSM. That design
is now wired through normal HBC lifecycle and replay behind
`ANTFLY_HBC_POSTING_SIDECAR=1`. The primary source journal remains the
durability authority. Only postings touched by a successfully committed source
transaction are captured, and the applied watermark is not published until
derived capture completes. Derived failures never fail a committed source
write: they durably invalidate `CURRENT`, after which ordinary replay repairs
the acceleration. Each query transaction leases one immutable posting
generation. Covered writes leave the last committed generation available,
then atomically publish a delta overlay before their applied watermark becomes
visible; in-flight queries finish on their old generation and new queries use
the new one. The LSM remains authoritative for source rows, vectors, metadata,
and recovery, but normal posting queries after the first checkpoint do not
fall through to the LSM.

### Public API qualification

One ReleaseFast lifecycle for each official VectorDBBench case exercised the
standalone public server, `/db/v1` table API, batch size 100 with four load
workers, visibility catch-up, process restart, cold and warm serial queries,
and Circus physical-footprint sampling. The adapter removed Antfly's default
full-text index before adding the external-vector index, so these results do
not charge unrelated asynchronous text work to HBC. They are diagnostic single
runs, not the three-run publication sample required by the memory methodology.

| Case | Insert | Catch-up | Total ready | Live recall | Live serial p50 / p95 / p99 | Live QPS at 1 / 4 / 16 | Reopened cold p50 / p95 / p99 | Reopened warm p50 / p95 / p99 | Demand / RSS peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| OpenAI 1,536D 50K | 35.63 s | 9.04 s | 44.67 s | 0.9849 | 2.9 / 5.6 / 13.8 ms | 44.58 / 399.82 / 670.94 | 5.3 / 10.3 / 423.7 ms | 4.6 / 6.1 / 422.9 ms | 734 MB / 1.80 GB |
| OpenAI 1,536D 50K, live immutable generations | 34.11 s | 11.19 s | 45.31 s | 0.9846 | 2.6 / 4.5 / 13.2 ms | 44.66 / 482.70 / 683.75 | 4.8 / 9.9 / 412.2 ms | 4.0 / 4.7 / 411.7 ms | 719 MB / 1.87 GB |
| Cohere 768D 1M | 1,579.41 s | 32.21 s | 1,611.63 s | 0.9863 | 50.6 / 617.3 / 664.6 ms | 5.08 / 17.67 / 37.89 | 43.0 / 467.1 / 495.1 ms | 9.0 / 15.6 / 436.3 ms | 1.40 GB / 4.99 GB |

The 50K result recovers the established 30--40 second insert range while
keeping demand bounded. Its reopened recall was 0.9742 in both sidecar and a
same-data sidecar-disabled LSM control, so the 1.07-point live-to-reopened
change belongs to HBC persistence rather than the segment reader. At 1M,
live/cold/warm recall was 0.9863/0.9863/0.9864. The internal paired harness
also produced identical ranked-result digests between reopened LSM and
segment-plus-WAL reads.

The 1M load is about 46% faster than the reported 3,000-second run and finished
with all vectors visible, 16 pressure compactions, and zero hard-limit debt.
It does not meet the hoped-for 13-minute target. Its sawtooth throughput tracks
aggregate LSM pressure: successful compactions repeatedly reduced L0 from near
the hard limit, while the sidecar stopped publishing new generations during
the long steady-state portion. Cumulative mutable snapshot copies reached
1.89 GB, far below the earlier multi-gigabyte cloning observed before 28K rows,
but still identify the remaining general optimization surface.

The original 1M artifact also exposed a lifecycle limitation. Incremental
sidecar maintenance safely invalidated after an HBC write outside a captured
source window; queries fell back to the authoritative LSM, and restart rebuilt
a 175 MB generation before the warm pass. The follow-up implementation gives
every derived batch an order independent from its source watermark, so
maintenance may append multiple batches at the same covered source sequence.
It also classifies posting mutations exactly: projection metadata and raw
vector writes do not invalidate `CURRENT`, while an uncovered packed-posting
write still fails closed before it can commit.

The live-generation 50K follow-up exercised that lifecycle through the public
API without a posting publication or invalidation warning. A delayed sequence
278 persistence callback arrived after a newer sidecar generation in the first
diagnostic attempt; treating that callback as out-of-order had needlessly
invalidated the sidecar. The corrected path records its captured mutations as
another derived batch at the already-covered source epoch and never regresses
source coverage. The clean run kept `CURRENT` through sequence 501, and restart
admitted the same 16 MB segment at exact sequence 501. Focused tests pin an old
generation across publication and prove that a new transaction sees the new
posting bytes while the old transaction remains unchanged.

### Full derived-state segment follow-up

The posting sidecar now contains the complete query-facing derived HBC state,
not only packed postings and quantized payloads. Packed nodes already contain
centroids and child/member topology. The segment adds node split ranges and a
compact vector directory for vector-to-leaf assignments and result metadata.
The checkpoint also retains the stable index configuration needed to reject an
incompatible generation during recovery. Projection watermarks remain in the
authoritative catalog because they have a different mutation lifetime.

The vector directory is an ordered immutable block rather than one generic
segment object per vector. It uses a contiguous value area, a fixed-width
binary-search index, an index checksum, and per-value checksums. This avoids
millions of allocator objects at 1M scale. Raw embeddings deliberately remain
source-owned when HBC has an external vector loader; copying the 1M corpus into
one posting checkpoint would be duplicate storage and a poor mmap/RSS boundary.

Native segment admission now retains a read-only mmap. Checkpoint v2 verifies
the segment header, footer, and complete index without faulting all payload
pages; posting payloads and vector-directory values are verified on first
access. Legacy v1 checkpoints retain whole-file checksum admission. The nested
vector directory does not pay a redundant outer payload scan: its own index is
checked eagerly and the requested leaf/metadata value is checked lazily.
Memory and object-storage implementations safely fall back to owned bytes.

Every read transaction leases one immutable generation. Batch reads for split
ranges, leaf assignments, vectors, and result metadata use that same lease.
Leased queries bypass the process-global metadata cache, which prevents an old
query from observing metadata admitted by a newer generation and lets mmap
pages replace duplicate heap residency. A test pins an old transaction across
publication and verifies that only a new transaction observes the replacement
topology and metadata.

The first public 50K run exposed avoidable capture overhead: tiny leaf and
metadata values performed scalar LSM reads and walked the immutable generation
chain to attempt replacement patches. It inserted in 44.79 seconds and became
ready in 51.60 seconds. Batch-reading all touched vector values once and only
patching large packed/quantized posting families recovered the expected load
rate:

| Public 50K full-derived checkpoint | Insert | Catch-up | Total ready | Snapshot copies | Sidecar | Physical footprint / RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Before capture fast path | 44.79 s | 6.81 s | 51.60 s | 133 MB | 19 MB | 829 MB / 1.60 GB during load |
| Batched tiny-value capture | 36.19 s | 13.41 s | 49.61 s | 116 MB | 17 MB + 5.9 MB WAL | 533 MB / 1.80 GB |
| Immutable flatten, half-segment policy | 45.05 s | 9.00 s | 54.05 s | 192 MB | 19 MB | 905 MB / 1.41 GB |
| Immutable flatten, bounded full-segment policy | 41.37 s | 9.01 s | 50.38 s | 164 MB | 19 MB | 576 MB / 1.54 GB |

The capture-only optimized insert reached the 30--40 second target range.
Total-ready time varies with asynchronous catch-up/checkpoint scheduling, so
insert and catch-up are reported separately. All runs reached sequence 501
with all 50,000 vectors query-visible. These are single diagnostic samples;
RSS is cache-inclusive and its high-water need not coincide with the native
physical-footprint ledger high-water.

At 1M, the half-segment policy exposed a repeat-checkpoint scalability issue.
The diagnostic was stopped at 451,500 query-visible rows after about 757
seconds: a fresh authoritative LSM scan during checkpointing temporarily put
replay about 120 source batches behind and pushed the sampler above its memory
target. Subsequent checkpoints now merge the complete leased immutable
directory and WAL overlays, so only generation 1 scans the authoritative LSM.
The first widened policy waited for at least 4 MiB and a tail equal to the
current segment size, with the existing unconditional 64 MiB cap. On 50K this
reduced checkpoint publications from ten to six and recovered 3.67 seconds
versus the half-segment flatten run while ending with an empty WAL. At 1M it
still published 18 full checkpoints before 575K and remained throughput-bound
on synchronous rewrites despite keeping L0 pressure and RSS bounded. A fixed
32 MiB policy reduced publication frequency but still made each full segment
construction and durable write part of replay's foreground critical path.

The current design separates three different bounds instead of treating them
as one compaction threshold:

- Every 32 MiB of WAL growth, live per-batch generations collapse to one
  overlay above the mmap root. If no query or builder leases the chain, owned
  payloads move into the newest map without copying; otherwise a replacement
  immutable overlay is copied and atomically installed, preserving the old
  transaction's view.
- At 128 MiB, one retained immutable generation is flattened and durably
  staged by a background worker. Source replay keeps appending to the current
  WAL while that work runs.
- Publication carries forward the exact committed byte suffix after the
  builder's boundary into a new WAL generation. This is byte-based rather
  than sequence-based because more than one ordered maintenance batch may
  legitimately commit at the same source sequence. Only a 256 MiB emergency
  ceiling can force replay to join a slow builder.

The crash order remains segment, next-generation WAL, then `CURRENT`; a crash
before `CURRENT` can leave only an unreferenced staged segment. The foreground
verifies the committed prefix and suffix before publication, and restart
checks the segment and WAL normally. A persistent background build or storage
failure propagates to the existing fail-closed invalidation path instead of
starting a new full build on every replay callback.

Moving construction to a worker without moving the durable segment write was
not enough: the first public 50K attempt regressed to 53.78 seconds ready. With
the segment staged by the worker, the same gate improved to 46.98 seconds
(33.64 insert plus 13.34 catch-up), 2.01 GB RSS, and 580.5 MB physical
footprint. Separating cheap overlay collapse from durable compaction improved a
load-only sample to 44.58 seconds (36.19 plus 8.39) with 1.98 GB RSS and
556.7 MB physical footprint.

The complete 50K lifecycle took 51.01 seconds (41.90 insert plus 9.11
catch-up) on the contended host. It published generation 1 and then retained a
52 MiB WAL without another full rewrite. Live recall@100 was 0.9837 and serial
p50/p95/p99 was 2.5/3.3/6.6 ms; after restart recall was 0.9839 and latency was
2.4/2.9/6.4 ms. Live QPS at concurrency 1/5/10/20/30/40/60/80 was
104/662/559/631/639/717/879/864; reopened QPS was
138/685/610/689/694/767/1,042/1,006. Restart became write-ready about 1.3
seconds after the public API was reachable. The load-only memory sample is the
cleaner comparison; the full lifecycle's query cache raised the combined
physical-footprint ledger to 1.40 GB while peak RSS was 1.92 GB.

The corresponding 1M lifecycle proves the handoff invariant but rejects this
dual-write implementation as the final ingest design. It took 2,160.94 seconds
to insert and 11.84 seconds to catch up, or 2,172.78 seconds ready. Thirteen
generations published without a forced builder join or a primary write-pressure
event, and every generation after the first was constructed from a retained
immutable generation rather than an LSM scan. The final sidecar was a 237 MiB
segment plus a 37 MiB WAL. Nevertheless, post-commit capture still opened a
primary snapshot to read final values back from the general LSM, producing
2.32 GB of cumulative mutable copying, and the same derived values were still
written to both storage engines. Load-only peak RSS was 4.12 GB and the native
physical-footprint ledger peak was 1.70 GB. These are bounded, but the duplicate
path is slower than the earlier 1,603-second LSM control and therefore rejected.

Live recall@100 was 0.9861. Serial p50/p95/p99 was
30.3/628.4/656.1 ms and QPS at concurrency 1/5/10/20/30/40/60/80 was
5.6/53.8/91.2/99.5/95.6/93.6/80.0/47.8. After restart, recall was 0.9862,
serial p50/p95/p99 was 27.6/519.3/538.2 ms, and the same concurrency curve was
7.1/74.8/119.7/104.8/108.0/128.5/119.6/92.8 QPS. Recall parity holds, but 1M
tail latency remains a separate query-path optimization target; the WAL-backed
mutation work must not trade recall for an attractive load number.

The full-derived query run preserved the existing boundary-rerank policy and
measured recall 0.9842 with serial p50/p95/p99 of 2.5/3.0/6.8 ms. Throughput
was 100 QPS at concurrency 1, 625 at 5, 698 at 20, and 774 at 40. Higher
concurrency crossed public-server admission and produced HTTP 429 responses,
so its nominal 905/985 QPS at 60/80 is not an accepted saturation result.
Restart admitted the mmap generation at exact sequence 501 and served 110 QPS
at concurrency 1 and 695 at concurrency 5 before the external harness was
interrupted. Recall remained within the prior public runs' normal spread.

### WAL-authoritative mutation-store follow-up

The next implementation removes normal query-facing HBC mutation persistence
from the general LSM after one complete immutable generation exists. This is a
storage ownership change, not a benchmark-only omission. The source table and
its replay journal remain authoritative for documents and raw embeddings. The
posting segment plus its ordered WAL become authoritative for the derived HBC
query state: packed nodes, posting-maintenance state, quantized checkpoints,
node split ranges, vector-to-leaf assignments, result metadata, and mutable
topology/count metadata.

The transition is explicit and crash-safe. A new or legacy index first builds
a complete checkpoint from the LSM. The next covered mutation pins that
immutable generation, owns each final value in a transaction-local map, and
stages a sticky authority marker in HBC metadata. Query-facing derived puts and
deletes then bypass the LSM. The source transaction/replay window establishes
its backend durability boundary before one checksummed posting-WAL batch is
fsynced; only after that append succeeds may the source applied watermark
advance. A crash after the posting append but before the applied-watermark
write is a prepared-ahead generation: startup admits it at or above the older
source watermark and idempotent source replay closes the gap.

Read-your-writes no longer requires a post-commit LSM snapshot. Write
transactions resolve the owned mutation map first and their pinned immutable
base second. Committed queries retain one immutable generation lease; an
overlay is installed only after its WAL commit, so old queries finish on the
old view and new queries observe the complete new view. Captured allocations
move into the immutable overlay rather than being copied again. Raw vectors
remain source-owned and use the external loader, avoiding a duplicate
1,536-dimensional or 768-dimensional corpus in the posting format.

Failure behavior changes once the marker commits. An optional pre-transition
sidecar may still invalidate and use the complete LSM. A WAL-authoritative
index may not: missing/corrupt startup state, a mutation outside capture, a
missing live generation, or failure to reinstall a durably appended generation
fails closed and leaves the source watermark unchanged. Ambiguous append
errors reopen and parse the durable prefix: an already committed intended
batch is acknowledged exactly once, an absent batch is retried once on the
repaired tail, and a different batch id is a single-writer violation. The
authority bit is atomic for concurrent query admission and remains effective
after restart even when the rollout environment flag is removed.

Maintenance uses the same store without fabricating source progress. A bounded
posting repair opens its own capture only when no source capture exists and
appends an independently ordered batch at the current covered source sequence.
If it runs inside source replay, it joins that source capture. Compaction is not
part of the foreground commit after a WAL batch is durable: overlays collapse
at 32 MiB, an immutable background checkpoint starts at 128 MiB, and only the
256 MiB recovery-debt ceiling may join the builder. Segment, next-generation
WAL, then `CURRENT` remains the publication order.

Legacy derived LSM rows are intentionally not deleted on the transition. They
are unreachable after the authority marker and removing them eagerly would
manufacture compaction debt during rollout. A rebuild can always regenerate a
complete generation from the source journal; physical legacy-key reclamation
belongs in a separately budgeted migration/compaction pass.

The final public 50K qualification of this WAL-authoritative path used the
standalone `/db/v1` API, batch size 100, four load workers, no default full-text
index, asynchronous replay, the normal boundary rerank policy, live search,
and process restart:

| Public 50K WAL-authoritative | Insert | Catch-up | Total ready | Recall / NDCG | Serial p50 / p95 / p99 | Valid QPS at 1 / 5 / 10 / 20 / 30 | Load demand / RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Live | 38.53 s | 2.27 s | 40.80 s | 0.9860 / 0.9882 | 2.8 / 3.9 / 15.9 ms | 99 / 558 / 382 / 435 / 571 | 879 MB / 1.49 GB |
| Restart at sequence 501 | -- | -- | -- | 0.9863 / 0.9885 | 2.5 / 3.2 / 6.7 ms | 104 / 608 / 350 / 435 / 509 | -- |

All 50,000 vectors were query-visible. The durable posting state was a 629 KB
bootstrap segment plus a 70.3 MB WAL; no derived leaf row for post-transition
vectors existed in the HBC LSM. Concurrency 40 and above crossed public-server
admission and returned HTTP 429, so VectorDBBench's larger nominal QPS values
are rejected-attempt artifacts and are not saturation results.

The first 1M attempt was stopped at about 843,400 visible rows rather than
accepted after `PostingWalMutationOutsideCapture`. Bounded posting maintenance
had just completed, and the next replay window inherited an already-open HBC
streaming session. A guard from the old snapshot/readback design silently
refused to start a new capture whenever LSM session batching was active. The
WAL backend does not need an LSM snapshot, so capture ownership is now
independent of LSM batching while publication remains forbidden until that
session establishes durability.

A second fresh run exposed a separate ownership race at about 562,400 visible
rows. Catch-up startup saw that posting maintenance already owned a capture and
treated `capture already active` as if the new source window owned it. The
maintenance transaction then published and closed its capture before the first
source mutation. Capture plus streaming-session acquisition is now atomic under
the per-index apply mutex, posting maintenance uses the same mutex through WAL
publication, and the only legal nested-capture case requires an already-open
streaming-session lease. An active capture without that lease is an explicit
`PostingWalCaptureOwnershipConflict`, never a silent join.

Focused HBC and index-manager regressions exercise streaming-session-first
ordering, independent-maintenance ownership rejection, legal source-window
nesting, sticky post-restart authority, WAL read-your-writes, abort without
publication, missing-generation fail-closed behavior, and restart recovery
without derived LSM persistence. Both stopped 1M samples are diagnostic only;
a fresh public qualification is required below.

The final fresh public 1M lifecycle completed without a posting-WAL, runtime,
or request failure. It crossed both former failure points and, more
importantly, exercised the contested transition directly: posting maintenance
repaired 73 steps at about 691K, 75 at about 834K, and 76 later in the load;
catch-up continued and published new immutable generations after every event.

| Public 1M WAL-authoritative | Insert | Catch-up | Total ready | Recall / NDCG | Serial p50 / p95 / p99 | Valid QPS at 1 / 5 / 10 / 20 / 30 | Demand / RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Live | 1,767.39 s | 6.72 s | 1,774.12 s | 0.9848 / 0.9869 | 35 / 638 / 660 ms | 3 / 28 / 66 / 86 / 68 | 2.91 GB / 2.59 GB ingestion |
| Restart at sequence 10001 | -- | -- | -- | 0.9851 / 0.9871 | 36 / 562 / 589 ms | 3 / 39 / 77 / 97 / 93 | 2.60 GB / 1.77 GB query |

All 1,000,000 vectors were query-visible. Recall changed by 0.03 percentage
points across restart. The final durable posting state was a 242.5 MB mapped
segment plus a 108.1 MB WAL; the entire table directory was 3.6 GB. The
ingestion physical-footprint ledger peak was 1.90 GB. Concurrency 40 and above
returned HTTP 429 and is excluded. The live query immediately followed primary
LSM compaction; restart improved c5--c30 throughput and tail latency while the
borrowed-WAL loader kept restart RSS well below ingestion RSS.

This is 41% faster than the reported 3,000-second load, but it does **not**
meet the hypothesized 13-minute target and is slower than the earlier 1,612
second full-segment diagnostic. The remaining ingestion cost is now visibly in
the source/embedding LSM rather than duplicated HBC posting persistence: the
final source LSM reported 2.52 GB of cumulative mutable snapshot copies and
3.24 GB of read-snapshot rotations despite only one pressure compaction. That
is the next general Antfly bottleneck; it should not be hidden by weakening
posting-WAL durability or rerank policy.

The follow-up sampler had one incomplete `vmmap` sample while the original
server process was exiting; its RSS fallback made the synthesized
`demand_peak_bytes` invalid. The table therefore reports the complete
physical-footprint ledger high-water (718.7 MB) as demand, plus the independent
cache-inclusive RSS peak (1.87 GB). Reopened recall was 0.9759 in both cold and
warm passes; as with the earlier control, the live-to-reopened change is not
specific to the segment reader.

## Whole-tree native HBC generation

The posting-only phase proved the WAL, recovery, query-generation lease, and
atomic publication protocol, but it was not the intended final experiment.
Keeping topology, node ranges, quantized payloads, result metadata, and the
vector-to-leaf directory in the generic HBC LSM retained its mutable-snapshot
copies and compaction work. The current format therefore extends the same
checksummed generation and WAL transaction across every query-facing HBC
namespace:

- packed tree nodes and topology;
- quantized and non-quantized leaf payloads;
- postings and node ranges;
- vector-to-leaf mappings and result metadata; and
- index metadata including `covered_source_sequence`.

An immutable generation is mmap-backed. Committed WAL transactions form an
in-memory delta over that base and publish as a single leased query generation.
A background checkpoint folds a pinned base plus a bounded WAL prefix into a
new segment, fsyncs it, establishes the next WAL, and atomically replaces
`CURRENT`. The sticky `AUTHORITY` marker makes recovery fail closed: after the
first complete native generation, Antfly never reopens the legacy HBC LSM or
silently treats its older rows as authoritative. The LSM backend and its native
storage owner are detached after activation; only the shared filesystem lease
needed by the segment/WAL store remains.

Source documents and their exact embeddings deliberately remain in the
primary LSM in this experiment. Approximate tree traversal is entirely native,
while the existing boundary-rerank policy still loads exact source embeddings
for its final candidates. Moving or sharing those source artifacts is a
separate ownership/migration decision and was explicitly deferred.

The first whole-tree public 50K lifecycle completed in 46.98 seconds (44.75
seconds insert plus 2.24 seconds catch-up). Live recall/NDCG was
0.9867/0.9888, with serial p50/p95/p99 of 2.7/5.4/12.1 ms; after graceful
restart it was 0.9870/0.9891 and 2.7/3.7/12.7 ms. Peak load RSS was 1.45 GB and
the complete HBC generation was about 20 MB. No dedicated HBC LSM remained.

The first whole-tree public 1M lifecycle completed in 1,859.94 seconds
(1,831.22 seconds insert plus 28.71 seconds catch-up). All rows were visible.
Live recall/NDCG was 0.9868/0.9888 and restart was 0.9869/0.9889. Peak valid
throughput was 87.3 QPS live and 68.4 QPS after restart at concurrency 20.
Serial live p50/p95/p99 was 36.1/653.2/682.9 ms; restart was
39.3/578.8/608.1 ms. Load RSS peaked at 2.78 GB. Native HBC durability occupied
about 236 MB of segment and 48 MB of WAL. Startup selected native authority at
sequence 10001 without replaying an HBC LSM; public write readiness was about
9.6 seconds.

The 1M query profile explains the remaining cold tail: native approximate HBC
traversal is active, but exact boundary rerank reads source embeddings from the
primary LSM, where block decode and `pread` dominate. The ingestion profile
also moved cleanly across the ownership boundary: primary dense-vector loading,
source point-run publication, and primary compaction dominate; generic HBC LSM
scans do not. This is useful separation rather than a claim that the primary
LSM is already optimal.

Large deferred leaf splits initially reloaded exact source vectors even though
the current replay transaction already owned them. The optimized path keeps a
resource-charged rolling append delta, merges it with the immutable leaf's
native non-quantized payload when exact reconstruction is possible, and retains
entries until bounded ring eviction. Updates/replacements conservatively clear
the append-only proof and fall back to the source loader. RaBit payloads are not
exact-reconstructable, so an older leaf prefix still comes from the primary
source while the rolling delta prevents newly appended members from being read
again. The ring is capped at 256 MB. Its constant-cardinality hash index uses
pre-reserved replacement windows and periodic in-place rebuilds so tombstones
cannot cause unbounded capacity growth. Deterministic churn tests require map
cardinality, slot ownership, fallback bounds, and resource charges to stay
exact. This optimization changes neither tree construction nor recall policy.

After bounding hash replacement churn and fixing deferred-node publication so
the bulk boundary cannot re-stage and discard its own writes, a fresh public
50K qualification completed in 42.16 seconds (39.92 seconds insert plus 2.24
seconds catch-up). Live recall/NDCG was 0.9860/0.9882 with serial
p50/p95/p99 of 2.8/5.3/13.2 ms and 1,018 QPS peak. Restart selected native
authority at sequence 501; recall/NDCG was 0.9866/0.9887, serial latency was
2.8/5.1/15.0 ms, and peak throughput was 1,114 QPS. Load-only peak RSS/demand
was 1.63/1.25 GB. The complete live-plus-restart sweep peaked at 2.04 GB RSS
during high-concurrency queries, not ingestion. The final primary LSM reported
253 MB of cumulative mutable snapshot copying; the detached HBC store reported
no generic LSM scan or compaction work.

A fresh whole-tree 1M qualification on the same corrected implementation
completed in 1,711.26 seconds (1,687.23 seconds insert plus 24.03 seconds
catch-up), versus the coworker-reported approximately 3,000 seconds. All
1,000,000 rows were query-visible at sequence 10001 and the process crossed the
previously failing approximately 843K boundary without a capture, publication,
or recovery error. Live recall/NDCG was 0.9854/0.9874 with serial
p50/p95/p99 of 35.2/654.3/691.0 ms and 77.9 peak QPS. Graceful restart selected
native authority without rebuilding; recall/NDCG was 0.9855/0.9875, serial
latency was 44.0/608.4/663.5 ms, and peak throughput was 89.6 QPS. Load RSS
peaked at 3.18 GB and the complete live-plus-restart sweep did not exceed that
RSS. The final immutable HBC generation was approximately 240 MB with a small
active WAL; the primary source LSM occupied approximately 3.51 GB.

The corrected 1M counters make the remaining boundary explicit. Native HBC
cache usage was only approximately 71 MB at load completion and no HBC LSM
existed, while the primary document/exact-embedding LSM accumulated 2.30 GB of
mutable snapshot copying. Applied-sequence publication spent 274.18 seconds in
native HBC WAL/generation publication across 223 flushes; increasingly large
background generation checkpoints remained crash-safe and bounded, but live
tree mutation/publication is now a material fraction of the remaining load
time. Query traversal stays native, while exact boundary rerank still performs
random primary-LSM reads; this preserves recall parity but explains the 1M
cold tail and the sharp throughput optimum near concurrency 20--30.

Profile-guided primary-LSM changes cache the decoded current entry in each
persisted compaction cursor and make adaptive Snappy back off after four
consecutive blocks fail its 12.5% savings floor, periodically reprobe, and
immediately re-enable after a successful probe. Prefix compression remains
enabled for every block and the on-disk codec is unchanged. A public 50K A/B
completed in 43.93 seconds (29.80 seconds insert plus 14.13 seconds catch-up),
with 0.9853/0.9877 live recall/NDCG and 1.30 GB peak RSS. A second diagnostic
run completed in 44.67 seconds (42.19 plus 2.48), demonstrating that foreground
insert versus HBC catch-up attribution is scheduler-sensitive even when total
load is stable. The faster-insert run left 118 primary L0 runs, so compaction
overlapped the immediate live query sweep; restart recovered serial
p50/p95/p99 to 3.1/5.0/36.1 ms with 0.9856/0.9880 recall/NDCG. These runs support
the CPU optimization but do not justify claiming an end-to-end 50K speedup.

Removing the redundant whole-payload CRC from staged-generation publication
while retaining an eager index/footer admission checksum and lazy per-entry
CRCs produced the fastest exact 50K load so far: 36.06 seconds (24.38 seconds
insert plus 11.68 seconds catch-up), with live/restart recall of 0.9851/0.9858.
The immediate query sweep was noisy (live serial p50/p95/p99
5.2/8.7/609.9 ms), so this is a load result rather than a query-latency win.
The corresponding public 1M lifecycle completed in 1,571.56 seconds
(1,551.96 plus 19.60), 8.16% faster than the prior 1,711.26-second run. Live
and restart recall were both 0.9900; live serial p50/p95/p99 was
33.8/687.9/713.0 ms at 80.0 peak QPS, and restart was 32.2/637.1/658.4 ms at
94.9 peak QPS. Load RSS/demand peaked at approximately 4.07/2.04 GB.

The admission checksum did not remove the main native publication cost:
`posting_publish_ns` was still 278.87 seconds across 248 flush calls. The run
wrote complete HBC generations growing from approximately 37 MB to 251 MB
about fourteen times. This identifies repeated O(live-tree) materialization,
not staging verification, as the remaining HBC-native load bottleneck. A
RaBit-only structural split reconstruction experiment was rejected despite a
38.62-second 50K load because recall collapsed to 0.6325; RaBit codes remain
appropriate for distance bounds, not exact topology construction.

The next native-format revision replaces repeated complete checkpoints with
one mmap base plus at most six ordered immutable replacement deltas. `CURRENT`
names the complete chain and covered source sequence in one checksummed 184-byte
record. Each delta has per-derived-family tombstones, publication carries the
exact concurrent WAL suffix into the next generation, queries lease the whole
chain, and the seventh rotation compacts it into a new complete base before
unlinking obsolete files. Normal delta construction includes only changes
above the immutable root; full compaction also enumerates delta-only vector
mappings and metadata. Focused restart and full-compaction regressions verify
that same-sequence WAL tails and mappings introduced only in deltas survive.

The first public 1M qualification of that bounded chain completed in 1,443.92
seconds (1,418.38 seconds insert plus 25.54 seconds catch-up), 127.64 seconds or
8.1% faster than the admission-checksum full-generation run. It crossed the
former approximately 843K mutation-boundary failure cleanly and reached exact
visibility at source sequence 10001. The run published six deltas, compacted
them into one 248 MB base, and finished with a small active WAL; restart chose
that native authority without an HBC LSM rebuild. Live recall/NDCG was
0.9865/0.9883 with serial p50/p95/p99 of 33.8/719.3/739.2 ms. Restart was
0.9866/0.9883 at 32.8/647.6/673.3 ms. The valid pre-HTTP-429 throughput peaks
were 93.1 and 101.2 QPS respectively.

The delta result exposes two separate remaining costs. Attributable load demand
fell to 1.34 GB and the native physical-footprint ledger peaked at 731 MB, but
cache-inclusive RSS briefly reached 4.93 GB while full compaction faulted the
six mapped deltas and the segment writer duplicated their live payloads.
`posting_publish_ns` was still 294.12 seconds across 227 flushes: background
delta publication removed repeated full-generation work but did not eliminate
foreground mutation encoding. Checkpoint writers now borrow values from their
pinned immutable generation until the final contiguous segment is built, and
the WAL encoder fast-paths a single insertion/deletion as direct prefix,
literal, and suffix operations before falling back to sparse shifted-run
matching. A first public 50K sample before sparse-anchor tuning remained
scheduler-noisy at 46.38 seconds ready, but publication CPU fell from 4.06
seconds over 23 calls to 3.55 seconds over 21 calls; load-only RSS/demand fell
from approximately 1.60 GB/929 MB to 1.51 GB/734 MB. Live recall/NDCG remained
0.9854/0.9878 with serial p50/p95/p99 of 2.9/3.2/12.6 ms. Restart selected
native authority at sequence 501 without rebuilding and returned
0.9853/0.9878 recall/NDCG at 2.9/3.3/12.3 ms and 952 QPS peak.
Sampling general replacement anchors every 16 rather than four base bytes
reduced the next 50K sample to 3.24 seconds of publication over 20 flushes and
a 56.8 MB WAL, without changing its approximately 46-second noisy total.
Live/restart recall was 0.9849/0.9854, serial p50 remained 2.9 ms, and both p99
values were approximately 12 ms. Load-only RSS/demand was 1.49 GB/731 MB.

The corresponding sparse-anchor 1M qualification completed in 1,397.78
seconds (1,372.32 seconds insert plus 25.46 seconds catch-up). This is another
46.14 seconds faster than the first delta run, 173.78 seconds or 11.1% faster
than the full-generation admission baseline, and less than half of the
coworker-reported approximately 3,000 seconds. Publication fell to 255.94
seconds over 221 flushes, a 13.0% CPU/wall reduction from the first delta run.
It crossed the approximately 843K boundary cleanly and reached exact visibility
at sequence 10001. Live recall/NDCG was 0.9861/0.9880, serial p50/p95/p99 was
34.8/710.0/728.1 ms, and the valid pre-429 peak was 94.6 QPS.
Restart selected native authority at sequence 10001 without rebuilding;
recall/NDCG was 0.9863/0.9882, serial p50/p95/p99 was 33.6/641.2/664.3 ms,
and the valid throughput peak was 101.2 QPS.

Borrowing pinned mmap payloads reduced the first six-delta full-compaction RSS
peak from approximately 3.89 GB to 3.31 GB. Load-cutoff RSS/demand was
4.12/1.62 GB, versus 4.93/1.34 GB for the first delta implementation; the
native physical-footprint ledger remained approximately 713 MB. An immediately
scheduled post-readiness full compaction raised cache-inclusive RSS to 4.70 GB
without raising attributable demand. An initial attempt to defer only the idle
checkpoint hook was insufficient: the final source mutation had already
crossed the ordinary 128 MiB threshold and started generation 15 as a full
rewrite at sequence 10001. That run still completed in 1,386.52 seconds
(1,369.07 seconds insert plus 17.45 seconds catch-up), but its query sweep used
the resulting single approximately 252 MB generation, not the six-delta chain.
Live recall/NDCG was 0.9867/0.9884 with p50/p95/p99 of
34.4/713.5/739.5 ms and 98.8 peak QPS; restart returned 0.9868/0.9884 at
35.4/637.8/661.8 ms and 104.5 peak QPS. Publication consumed 249.39 seconds
over 218 flushes. Load-only RSS/demand/physical-footprint-ledger peaks were
3.85/1.30/0.69 GB.

The corrected policy is cost-aware rather than benchmark-aware. Incremental
deltas still start at 128 MiB, while a max-chain full merge receives the
existing 256 MiB hard recovery budget because it faults and rewrites the whole
visible generation. A fresh public 1M qualification started its only full merge
at sequence 6838 with a 269.56 MB capture (the small excess is one atomic source
batch), produced a 172.28 MB base, then finished with six deltas and no active
WAL at exact source sequence 10001. It completed in 1,392.62 seconds
(1,369.04 seconds insert plus 23.58 seconds catch-up), 178.94 seconds or 11.4%
faster than the 1,571.56-second complete-generation admission baseline and less
than half of the coworker-reported approximately 3,000 seconds. Publication
fell to 241.59 seconds across 224 flushes. Load-only RSS/demand/physical-ledger
peaks were 4.18/1.65/0.88 GB; the complete live process peaked at 4.18 GB RSS
and 2.70 GB attributable demand during query cache warming.

This qualification genuinely queried the retained base plus six mmap deltas:
no generation 15 build occurred before, during, or after the live sweep. Live
recall/NDCG was 0.9862/0.9881, serial p50/p95/p99 was
35.9/731.2/756.2 ms, and peak throughput was 98.6 QPS. Restart selected the
same native authority directly while the compatibility mirror remained at
sequence 126; recall/NDCG was 0.9863/0.9882, serial latency was
37.0/659.3/683.2 ms, and peak throughput was 104.8 QPS. The bounded chain
therefore preserved recall and throughput, with only a small plausible serial
fan-out cost relative to the immediately preceding compacted sample.

The remaining format tradeoff is disk and mapped-address amplification, not
generic HBC LSM work. The final 172.28 MB base plus six 131--170 MB deltas used
1.08 GB (about 4.3 times one compacted generation), although pages remain
reclaimable and queries fault only what they visit. The next high-upside format
experiment should retain replacement patches or copy-on-write chunks in indexed
immutable deltas and materialize/cache a value on first access. More frequent
full rewrites would reduce disk but give back the load-time and RSS gains this
experiment was designed to obtain.

## Whole-HBC native query qualification and competitor target

The complete native HBC format now owns topology, quantized payloads, postings,
vector-to-leaf mappings, metadata, WAL replay, generation publication, and the
covered source sequence. A compacted 1M generation is approximately 251 MB and
restart mounts it directly; dedicated HBC LSM runs are no longer required. The
primary document/artifact LSM remains authoritative for exact source embeddings.
Its settled 1M layout had 16 runs (three L0 and thirteen lower-level runs), so
the former 825-L0-run compaction-debt failure is not present in this result.

The first complete-format 1M load was not a performance win: 1,897.67 seconds
of insertion plus 17.50 seconds of catch-up, 1,915.17 seconds total. The native
checkpoint itself is cheap; foreground source insertion and derived mutation
publication now dominate. The data directory occupied approximately 3.5 GiB,
including about 3.07 GB of irreducible float32 payload for one million 768-D
vectors. A final design cannot beat the roughly 3.15 GB Elasticsearch disk
result by adding another per-index exact-vector plane. Exact artifacts need one
shared table-level vector-block authority, or an exact reconstructible encoding,
before the duplicate primary artifact rows can be removed safely.

At search effort 0.45, the original policy produced 0.9628 recall and 0.9668
NDCG. A representative serial run was 24.6/53.8/65.4 ms p50/p95/p99 and peaked
at 349.5 QPS, but host contention makes absolute comparisons provisional. The
deterministic work profile is the more important result: the resolved width was
1,097 leaves, with about 130,000 quantized vectors scored per typical query and
hundreds of exact boundary candidates. Primary exact-artifact reads dominated
cold and tail latency.

Moving exact rerank batches from broad snapshot transactions to current-tip
point probes removes their mutable-state clone/rotation path. Shared-cache batch
admission is epoch guarded so a concurrent update cannot resurrect stale vector
bytes, and stable run-backed probe values remain pinned rather than being copied
a second time. The first query-only memory sample peaked at 2.25 GB attributable
demand and 1.54 GB RSS. After a full serial plus concurrent cache warmup, the
same process peaked at 2.56 GB demand and 1.24 GB RSS; approximately 716 MB of
that warm state was retained exact vectors in the HBC heap cache. This is bounded
but is not the desired steady-state architecture: a shared mmap vector-block
store should make those pages reclaimable and eliminate the second heap copy.

Pruning calibration exposed a sharp but useful quality knee on the full 1,000
query ground-truth set:

| epsilon | recall | NDCG | serial p50 | serial p95 |
| ---: | ---: | ---: | ---: | ---: |
| 0.19 | 0.3882 | 0.4101 | 3.2 ms | 86.1 ms |
| 0.70 | 0.9253 | 0.9319 | 56.2 ms | 146.6 ms |
| 0.90 | 0.9527 | 0.9574 | 46.3 ms | 103.2 ms |
| 0.95 | 0.9548 | 0.9593 | 39.2 ms | 86.8 ms |
| 0.97 | 0.9556 | 0.9602 | 40.7 ms | 97.9 ms |

Only the quality numbers are comparable across this sweep; unrelated builds
changed CPU and IO availability between runs. Epsilon 0.97 clears Circus's
0.955 calibration target, but a 100-query work sample still visited a mean of
1,014 leaves (p50 and p95 both at the 1,097 hard cap) and scored 119,734
quantized vectors on average. Lowering epsilon alone therefore gives away recall
without removing enough hard-query work and must not become a benchmark-specific
default. The production improvement is confidence-aware routing backed by stored
cluster bounds and better cluster quality, with the width retained only as a
safety cap.

The current Circus 1M comparison target is deliberately Pareto-oriented. At
recall at or above 0.955, first beat pgvector's 731.8 QPS, keep p95 below the
low-teens Chroma/Weaviate range, reduce attributable demand toward Elasticsearch's
2.06 GB, and reduce disk toward its 3.15 GB. Elasticsearch and Milvus publish
3,104.7 and 3,813.0 QPS respectively, so they remain the throughput stretch
target after the shared exact-vector read path and routing work are complete.

The public batch-100 qualification is an online incremental build, not a
recursive offline build. The VectorDBBench adapter creates the external dense
index before loading, then sends 10,000 ordinary public batch requests for the
1M case. Antfly's empty-index bulk path does select the balanced recursive
builder when a single derived bulk-ingest window contains at least 1,024
vectors, and production backfill/import can reach that path. That does not make
it valid to label the existing online run recursive: its lifecycle must remain
the apples-to-apples Circus score. A recursive backfill/import result should be
reported separately, including its build time, recall, restart behavior, and
steady-state footprint.

A subsequent public-API qualification removed per-candidate shared-cache lock
and refcount traffic by scoring a complete rerank batch under one shared cache
lease. The exact cosine and invalidation/epoch invariants remain unchanged. On
the preserved 1M generation, the default public request resolved to width 2,048
and epsilon 1.45; a warmed 100-query diagnostic measured 13.9 ms mean, 10.5 ms
p50, and 35.2 ms p95 while scoring approximately 240,000 quantized vectors and
450 exact vectors per query. Cache-hit distance work was only 0.2--0.3 ms even
on the slow tail, so routing and cache misses—not lease bookkeeping—now dominate.

Under the current host load, the full public harness scaled from 107.1 QPS at
C5 to 264.6 QPS at C10 and 265.0 QPS at C20, then fell to 227.2 QPS at C30.
These are diagnostic rather than publication numbers because another Zig build
was active. C40 exceeded the server's 32-request admission envelope and returned
429, so the run was stopped rather than recording invalid higher-concurrency
points. A live C30 sample used roughly 780% CPU and a 307 MB HBC cache: 141 MB
quantized topology plus 130 MB for 41,273 exact vectors. This validates the next
A/B: scan the small flat block-quantized leaf-centroid directory before selected
posting reads, then replace cold primary artifact point reads with a shared,
versioned mmap vector plane.

The flat RaBitQ centroid directory did not beat the hierarchy and is rejected as
the production default. It reached 0.9600 recall on the 200-query calibration
set at effort 0.47, but the full 1,000-query run scored roughly 167,625 centroid
candidates and 433 exact candidates per query for 0.95626 recall, 84.27 ms mean,
and 178.2 ms p95 under the then-current host load. The hierarchical directory
reached comparable quality with materially less deterministic work. Keeping the
flat selector is useful only as an explicit experimental A/B surface.

The adaptive primary exact-read path avoids decoding and pinning an entire
prefix-compressed table block when a sorted rerank batch touches only one large
artifact in that block. A second key in the same block promotes it to the
decoded-block path, preserving adjacent-batch amortization. Stable probes own
directly reconstructed values and pin decoded blocks and the run generation, so
this optimization changes copy amplification rather than visibility semantics.

With that path and the original boundary-rerank policy, the lowest public
`search_effort` that cleared both Circus gates on this generation was 0.437:
0.95625 recall on the 200-query calibration set and 0.95021 on the disjoint
800-query held-out set. The held-out sequential diagnostic averaged 6.37 ms with
8.09 ms p95 while visiting about 927 leaves, scoring 109,618 quantized vectors,
and reranking 431 exact vectors per query. Effort 0.436 cleared calibration but
fell to 0.94934 held-out recall and is therefore invalid.

At effort 0.45, the normal C1/C5/C10/C20/C30 sequence produced
148.3/602.9/505.7/460.9/420.5 QPS, with C1 and C5 p95 of 7.78 and 9.53 ms. A
heavily warmed effort-0.437 C5 diagnostic reached 670.7 QPS at 8.64 ms p95.
These results beat the published Antfly and Chroma throughput figures while
holding recall, but they do not yet beat pgvector's 731.8 QPS and are not
publication numbers: other Zig builds were active, and the 0.437 C5 run began
with a larger exact-vector cache than a fresh Circus lifecycle.

Two follow-up routing shortcuts were rejected. Reducing the approximate
candidate window from nine to eight times `k` only barely held the recall floor
and did not improve C5 throughput. A metric-ball A/B using each leaf's exact
centroid radius pruned no meaningful payload work because these online HBC
partitions overlap too broadly. The radius code was removed rather than leaving
an extra query-to-centroid pass. Faster routing therefore requires better
partition geometry, controlled boundary replication, or a tighter second-level
posting directory—not another benchmark-specific effort constant.

An exact-vector cache-ownership A/B also rejected unconditional transient
primary reads for public search. On the preserved 50K generation, serving the
first 100 query batches without admitting source physical blocks held those
block inserts essentially flat (126 to 127) and reduced the shared LSM cache
from the 313.6 MB control to 26.4 MB while the governed HBC cache retained
115.7 MB. Recall remained 0.985, but cold client latency regressed from the
53.6 ms control mean to 200.3 ms, with p50/p95 rising from 38.8/124.1 ms to
184.3/351.8 ms. The decoded cache removed artifact IO on the repeat pass, but
discarding every source block lost useful spatial reuse while that cache was
being populated. Public search must therefore retain source blocks under the
shared resource envelope; transient admission remains appropriate only for
paths that materialize and retain the complete useful representation. Run
qualifications with an explicit process memory envelope to compare memory and
latency on equal terms instead of inheriting an 8 GiB LSM cache ceiling from a
large development host.

The follow-up retained-cache run under an explicit 1 GiB process envelope
preserved that reuse without preserving the large-host footprint. After the
same first 100 queries, the ResourceManager held LSM residency at 234.9 MB
(224 MiB soft, 256 MiB hard), retained 115.7 MB of decoded HBC vectors, and the
process RSS was 454.8 MB. Low-priority block eviction made progress while every
run-table index stayed resident. Recall remained 0.985; cold mean/p50/p95 were
44.8/35.5/91.9 ms and the identical warm repeat reached 5.64/4.57/10.85 ms
with no artifact reads. On this contended host those absolute times are
diagnostic, but the controlled result supports normal cache admission plus an
explicit resource envelope over benchmark-specific transient reads.

The shared HBC cache then exposed two general foreground-tail bugs. Reusing an
evicted CLOCK slot scanned from the beginning of the full slot array, and a
second-chance miss could charge a complete CLOCK revolution to one insertion.
Keeping the slot array compact makes removal/reuse constant-time; bounding one
victim search to 64 entries distributes the second-chance sweep while advancing
the hand and preserving namespace fairness. Focused churn, recency, and bounded
scan tests cover the map/slot invariants. On the same persisted 50K generation,
the official warmed VectorDBBench p99 fell from 440.5 ms to 12.8 ms at 0.9867
recall after these changes.

Continuous macOS `vmmap --summary` sampling was the remaining regular tail, not
Antfly. It produced approximately 450--520 ms stalls every 75 queries and even
charged some pauses inside server search timers because `vmmap` inspects the
live target. The qualification runner now invokes the Circus sampler once after
each timed phase. The kernel footprint ledger preserves the process high-water,
so this retains the same primary memory yardstick without perturbing load or
query latency. With sampling moved out of the timed window, the checked-in
public profiler measured 3.63/4.26/4.74 ms p50/p95/p99, 14.87 ms maximum, and
0.98671 recall across 1,000 queries. The post-phase sample reported a 659.2 MB
physical-footprint ledger peak, 729.1 MB current RSS, and 700.7 MB conservative
demand under the explicit 1 GiB process envelope.

The first post-sampling 1M qualification under a 2 GiB envelope is invalid as
a timing result but exposed a general rollback amplification bug. At 568,800
query-visible documents, public inserts began hitting their exact 120-second
request timeout. A process sample caught derived replay inside
`commitDenseVectorMappingsWithRollback`: a primary mapping-store commit failure
started maintained inverse HBC deletes even though the active authoritative
source capture already owned the complete pre-mutation native generation.
Those deletes recomputed centroids and, with the governed HBC cache retaining
only one external vector, reloaded posting members from the primary LSM. The
correct rollback is to propagate the error and let the outer capture cancel
restore topology, metadata, search publication, and caches in constant time.
Pre-authority captures retain inverse rollback because they do not yet own a
complete restorable generation. Tests cover both sides of that authority
boundary.

The same partial run confirmed that exact rerank at 1M is now dominated by
sparse primary-artifact block reads rather than HBC routing. Sorted point
batches now overlap independent path-backed run-block reads with a bounded
four-slot pipeline. Each key still resolves mutable state and candidate runs in
strict newest-to-oldest order, so tombstone and overwrite precedence is
unchanged; only different keys overlap IO. The pipeline uses existing cache,
checksum, allocation, and read-stat paths and caps concurrent buffers at the
configured point-read ceiling. Its latency effect must be measured against the
preserved complete 1M generation before another fresh load qualification.

That preserved-generation A/B kept recall exactly 0.98535, exact rerank work
exactly 447.478 vectors per query, and approximate work exactly 237,596 vectors
per query across the same 1,000 public API queries. The bounded four-read
pipeline reduced client mean latency from 85.71 to 54.21 ms and p95 from
153.46 to 95.07 ms. Server-attributed artifact reads fell from 72.01 to 41.77
ms mean and from 138.21 to 81.38 ms p95. This is a 36.8% end-to-end mean
improvement and 42.0% artifact-read improvement without changing search work
or quality. Post-query demand was 1.12 GB versus 1.30 GB in the earlier run;
RSS was higher at 1.74 GB versus 1.46 GB because the merged cache governor
retained more reclaimable node metadata, so that one-sample cache-inclusive
difference is not attributed to the read pipeline.

After merging current `origin/main`, the stale sibling VectorDBBench checkout
failed before search because it POSTed the removed legacy
`/api/v1/tables/{name}` contract and received HTTP 405. The canonical runner
preserves that failed result, supports an explicitly labeled diagnostic-profile
mode, and accepts an explicit VectorDBBench checkout so the API-compatible
adapter can run the official lifecycle. Diagnostic profiles are not
publication-equivalent substitutes for the official client lifecycle.

The API-compatible checkout then completed the official reopened 1M serial
lifecycle. Cold recall was 0.9854 with 54.9/114.1/163.6 ms p50/p95/p99; the
immediate warm repeat held 0.9854 recall at 53.3/109.8/150.2 ms. The earlier
same-generation official run before sparse pipelining measured
140.7/466.7/731.4 ms cold and 87.4/214.8/319.3 ms warm. A detailed 1,000-query
profile after the official warm pass measured 48.52 ms mean, 86.08 ms p95,
102.86 ms p99, and 36.03 ms mean artifact-read time. Its post-phase sample was
1.22 GB demand, 1.20 GB physical-footprint ledger peak, and 1.57 GB RSS under
the same explicit 2 GiB process envelope.

Widening only the primary-store point pipeline from four to eight was rejected.
Although eight 32 KiB buffers look inexpensive in isolation, the preserved 1M
public profile made no bounded progress and one request exceeded the profiler's
120-second timeout. The server eventually unwound the in-flight request and
exited cleanly, but this is a hard tail failure, not a noisy benchmark sample.
Per-query read fanout composes with concurrent public queries, shared cache
admission, and the bounded service I/O lane; a local buffer calculation alone
is therefore not a safe concurrency policy. Keep four as the default until a
global admission controller can allocate read slots across queries.

The first fresh 50K qualification after the four-wide change recovered the
load target (31.20 seconds ready) but exposed an aggregate-cache deadlock at
five concurrent queries. An LSM cache admission held its accounting lock while
waiting for the resource-manager reclaimer gate, whose HBC callback waited for
the HBC cache owner. Making only the HBC callback nonblocking exposed the
complementary cycle on the next run: an HBC admission held the callback gate
while its LSM callback waited for LSM accounting. The production invariant is
therefore symmetric: resource-manager cache callbacks are opportunistic and
must never wait for an owner lock. HBC now try-locks its cache; LSM try-locks
accounting and individual shards, evicts only unpinned entries, and publishes
the exact released bytes. A contended callback returns zero, causing the
requesting cache allocation to use its bounded transient path without relaxing
the aggregate hard limit.

The fresh post-fix 50K public-API gate completed the full load/query/restart
lifecycle. It inserted in 29.17 seconds and was fully ready in 31.19 seconds;
the five-client 10-second phase completed at 214.01 aggregate QPS with 23.29 ms
mean, 37.04 ms p95, and 46.05 ms p99 concurrent latency. Live serial recall was
0.9853, and cold/warm reopened recall was 0.9841. Warm restart serial latency
was 3.0 ms p50, 3.6 ms p95, and 15.1 ms p99. A separate 100-query profiled pass
measured 3.63 ms mean, 4.09 ms p95, 4.58 ms p99, and 0.9826 recall (the smaller
sample explains the recall variance). Primary mutable snapshot copies totaled
106.27 MB, HBC cache demand was 16.42 MB, the data root occupied 409 MiB, live
demand/RSS peaked at 508.6 MB/1.25 GB, and reopened demand/RSS at 597.8 MB/
931.3 MB under the explicit 1 GiB envelope.

The subsequent full 50K ladder reproduced the load result at 28.07 seconds
insert and 30.10 seconds ready. Live recall was 0.9863. Aggregate QPS at
1/5/10/20/30 clients was 50.32/214.92/278.06/159.02/152.00; latency p95 was
34.18/36.78/62.17/212.59/334.71 ms. The throughput knee after ten clients is
CPU/global admission saturation rather than the prior deadlock. A 1,000-query
public profile measured recall 0.98541, 3.88 ms mean, 4.64 ms p95, and 8.33 ms
p99, with 557.96 exact vectors and 24,263 approximate vectors per query.

The fresh pre-resource-governor 1M qualification completed without the former
~568K rollback stall or ~843K posting-capture violation, but it remains too
slow. It inserted in 1,817.41 seconds and was ready in 1,821.51 seconds: better
than the reported 3,000 seconds, but 2.3x the 13-minute target. Live/reopened
recall was 0.9861/0.9858. Aggregate QPS at 1/5/10/20/30 clients was
5.51/23.22/37.52/46.21/44.10, with p95 latency
317.23/424.66/537.10/989.46/1,522.94 ms. The 1,000-query public profile measured
66.72 ms mean, 136.31 ms p95, and 183.22 ms p99. Artifact reads alone averaged
47.66 ms and exact rerank expanded to 2,068.97 vectors/query, versus about 447
on the earlier preserved generation at equivalent recall; query selection or
rerank-boundary behavior therefore regressed independently of sparse LSM I/O.

The completed 1M root occupied 3.76 GiB, primary mutable snapshot copies
totaled 1.46 GB, and HBC cache demand finished at 181.76 MB. Late ingest built
150 pressure events and 156 pressure compactions and still finished with 72 L0
runs. Direct observation caught about 3.27 GB RSS during ingest; the existing
post-phase one-sample footprint (1.01 GB RSS, 2.00 GB demand) did not capture
that peak and must not be presented as load peak memory. This is the clean
pre-PR-540 baseline for the lifetime-fenced resource governor.

The post-PR-540 integration preserves both sides of the cache callback
contract. ResourceManager invokes reclaimers outside its registry and
accounting locks and holds a fixed-slot in-flight lease so unregister can fence
callback context destruction. HBC and LSM callbacks remain opportunistic and
never wait for their cache owner locks. Managed query sessions reserve decoded
vector residency as one request-owned unit, switch coherently to retained LSM
residency before an overrun, and avoid loading metadata for already decoded
rerank hits. The resource-budget suite passed 73 tests, the LSM suite passed
323 with one intentional skip, the DB query suite passed 191 with two skips,
and the standalone vector-index suite passed all 38 tests.

The fresh post-governor 50K public-API gate inserted in 20.48 seconds and was
ready in 24.53 seconds. At five clients it reached 374.62 QPS with 28.66 ms
p95 and 0.9859 recall. The standardized full run was scheduler-noisier at
29.89 seconds insert and 31.93 seconds ready, but query throughput changed
materially: C1/C5/C10/C20/C30 reached
171.92/1330.34/1507.15/1308.57/1165.07 QPS, versus
50.32/214.92/278.06/159.02/152.00 before the governor follow-ups. Corresponding
p95 latency was 12.61/5.49/9.29/22.49/40.48 ms instead of
34.18/36.78/62.17/212.59/334.71 ms. Live recall was 0.9845 and warm reopened
recall was 0.9810. The warm 1,000-query profile measured 4.17 ms mean, 4.80 ms
p95, and 5.10 ms p99; governed decoded residency eliminated artifact read and
decode time in that warm phase.

The checked-in runner now complements the post-phase Circus footprint sample
with a lightweight 200 ms `ps` RSS timeline for the entire live and reopened
lifecycle. The full 50K run peaked at 1.55 GB RSS and 1.20 GB attributable
demand under the explicit 2 GiB envelope; the reopened process peaked at
456 MB RSS. This closes the methodology gap that hid the earlier 1M load peak
without reintroducing intrusive `vmmap` sampling into timed traffic.

The fresh post-governor 1M qualification completed without the former rollback
stall, posting-WAL capture violation, retry storm, or visibility loss. It
inserted in 1,499.99 seconds and was ready in 1,502.04 seconds: 17.5% faster
than the 1,821.51-second baseline, but still 1.9x the 13-minute target. Catch-up
is now only 2.05 seconds, so remaining load time belongs almost entirely to the
primary document/artifact LSM. Live recall was 0.9852. C1/C5/C10/C20/C30
throughput was 9.98/54.88/93.55/97.15/46.58 QPS, versus
5.51/23.22/37.52/46.21/44.10 before the governor follow-ups. The useful knee is
twenty clients; thirty clients saturated this host and raised p95 to 1.87
seconds. Warm reopened recall was 0.9851, and the 1,000-query profile improved
from 66.72 to 36.33 ms mean and from 136.31 to 77.35 ms p95. Exact rerank work
remained high at 2,087.31 vectors/query, so residency ownership accelerated the
same broad rerank boundary rather than hiding a recall reduction.

Continuous 1M RSS peaked at 2.50 GB live and 2.09 GB after restart; the
post-phase footprint ledger reported 2.13 GB demand. This is materially below
the prior 3.27--3.89 GB observations but does not meet a strict 2 GiB process
envelope. ResourceManager's managed-host peak was 1.10 GB, while dense apply
alone peaked at 222.42 MB against an 89.48 MB slice hard limit and two aggregate
accounting errors were recorded. The gap between managed charges, footprint,
and RSS needs an ownership audit; it must not be dismissed as harmless mapped
residency.

The final disk and write counters isolate the next general bottleneck. The
3.7 GiB root contained 3.3 GiB of primary LSM runs and only 357 MiB of the
native vector index. Primary ingest accepted five million logical entries, but
only 360,000 entries (7.2%) reached direct sorted ingest. The remainder caused
4,564 flushes, 16,595 flush output runs, 4.06 GB of flush output, and 4,381
manifest publications. It then required 147 pressure events and 151 pressure
compactions, finishing with 88 L0 and 13 lower-level runs. Mutable snapshot
copies totaled 1.51 GB and current-scan rotations 342.85 MB. The native HBC
generation/WAL design is no longer the dominant load or disk cost; making the
primary append-heavy document/artifact path publish larger sorted runs with
far fewer manifests is the highest-leverage next experiment.

## Primary geometric-merge experiment

The next 50K experiments changed only the primary LSM publication and
compaction policy. They retained the public `/db/v1` server path, batch size
100, twenty load workers, native HBC, the 2 GiB resource envelope, exact
rerank, and the same recall/query matrix. The implementation publishes sorted
bulk state at 8 MiB, assigns all partition files in one publication a durable
logical L0 sequence, and performs same-level streaming carries. Manifest v10
persists that chronology and remains able to read v9 manifests.

An exact four-way carry recovered fast load (21.66--23.10 seconds total) but
eventually fell through to generic L0-to-L1 compaction. That rewrote about
710 MiB and left live RSS around 1.86--1.92 GB. Raising the optional
foreground-query compaction pause from 2 to 25 milliseconds was rejected: it
stretched 662 MiB of compaction from roughly 4.7 to 14.3 seconds, raised RSS to
2.01 GB, and did not improve the first query lane.

Giving the tier lane ownership of run-count-only soft pressure reduced
compaction input to 75.4 MiB and load to 22.49 seconds, but retained 96 L0
files. Live RSS reached 2.13 GB and restart memory/latency regressed. A
two-to-four-way carry was also rejected earlier because it rewrote 1.02 GB in
thirteen jobs. The accepted refinement is a universal geometric carry: an
uneven contiguous generation window is eligible only when the output is at
least four times every input generation. This preserves chronological
overwrite/tombstone precedence and log-base-four write amplification while a
hard L0 run/byte limit remains an authoritative leveled-promotion escape.

Two fresh geometric-carry 50K runs were ready in 28.72 and 23.98 seconds. The
repeat ended with 24 L0 files, no lower-level files, 323.3 MiB compaction input,
321.4 MiB output, 2.38 seconds cumulative compaction, 134.9 MiB snapshot
copying, no write-pressure events, and 402.6 MB total disk. Its live RSS peak
was 1.54 GB and recall was 0.9854. Query results remained host/tree-sensitive:
the repeat reached 1,161 QPS but had 50.8/14.5/14.2/30.4/46.9 ms p95 at
concurrency 1/5/10/20/30, so the 1M gate must report query and restart
latencies rather than treating the improved storage counters as sufficient.

The first geometric 1M gate was correct and improved load from 1,502.04 to
1,267.79 seconds (1,263.74 insert plus 4.05 catch-up), but did not preserve its
early ~13-minute trajectory. Direct publication stopped after 1.087M of 5M
logical entries because transient bulk sessions no longer accumulated to the
8 MiB floor as the lower-level base slowed request overlap. The tail produced
3,869 flushes, 14,104 flush-output files, and 3,931 manifests. More
importantly, the hard-pressure lane bypassed geometric carries and performed
165 compactions with 92.74 GB input and 92.10 GB output for a 3.52 GB primary
run set. This is write amplification, not necessary dataset size.

Recall remained 0.9849. C1/C5/C10/C20/C30 throughput improved to
11.79/74.46/122.10/124.09/122.57 QPS, eliminating the former C30 collapse.
The warm 1,000-query profile was 35.26 ms mean and 77.89 ms p95 at 0.98452
recall, essentially the post-governor latency baseline. Demand improved to
1.70 GB live and 1.30 GB after restart, while cache-inclusive live RSS peaked
at 3.38 GB during compaction overlap. The next gate lowers the byte-based
direct-publication floor to 4 MiB and makes hard run-count pressure attempt a
geometric carry before generic promotion; byte-hard pressure remains directly
leveled because a same-level merge cannot reduce its byte debt.

The 4 MiB direct floor plus foreground geometric hard-pressure assist recovered
the intended load envelope. Its 50K gate finished in 21.74 seconds (19.71
insert plus 2.03 catch-up) at 0.9867 live recall, used 377 MiB on disk, cloned
76.7 MB, and performed seven geometric compactions over 326.8 MB. C1/C5/C10/
C20/C30 throughput was 59.44/1,253.64/1,580.69/1,276.12/1,285.59 QPS with
42.3/7.0/8.9/21.0/32.5 ms p95. No hard-pressure event occurred. Live RSS
peaked at 1.97 GB, so the speed result passed while memory retained noticeable
host/cache variance.

The matched 1M gate finished in 759.03 seconds (754.97 insert plus 4.06
catch-up), recovering the ~13-minute target while preserving 0.9859 recall.
Direct publication reached 3.9635M of 5M logical entries instead of freezing
at 1.087M. All 653 pressure events made progress with zero overloads. One
byte-hard promotion established a 2.09 GB lower-level base; geometric carries
then avoided rewriting it. Total compaction input fell from 92.74 GB to
14.55 GB, compaction time was 101.0 seconds, and final disk was 3.86 GB.

This load win is not yet the balanced endpoint. Live/restart demand was
3.15/2.18 GB and cache-inclusive RSS peaked at 3.94/2.19 GB. Warm recall was
0.9857, but warm p50/p95/p99 regressed to 41.1/90.0/108.3 ms. The public
profile attributes 29.88 ms mean and 77.45 ms p95 to rerank artifact reads.
After maintenance the primary still had 74 L0 runs containing 1.43 GB above a
2.09 GB base, while the LSM and HBC caches held about 470 and 408 MB. The next
experiment should retain geometric carries but permit one justified base-growth
merge when accumulated L0 is a substantial fraction of the lower base. That
is bounded size-ratio leveling, not a return to repeated low-growth rewrites.

A 50% base-growth promotion tested that hypothesis. The 50K gate remained
healthy at 22.41 seconds ready, 0.9861 recall, and 1.96 GB peak RSS. At 1M the
second promotion fired near 897K rows, reducing the query-visible primary from
1.43 GB in L0 over a 2.09 GB base to 415 MB in L0 over 3.09 GB in lower
levels. Live/restart demand improved from 3.15/2.18 GB to 1.97/1.52 GB, and
cache-inclusive peaks improved from 3.94/2.19 GB to 3.33/2.06 GB.

The leveled form is not the optimal endpoint. Load regressed from 759.03 to
788.81 seconds, compaction input rose from 14.55 to 18.40 GB, and compaction
time rose from 101.0 to 131.9 seconds. Warm p95 improved from 90.0 to 83.7 ms,
but the public profile improved only from 89.92 to 88.04 ms and C30 fell from
94.26 to 78.58 QPS. The 2.92 GB maximum job shows the ratio promotion merged
the new delta with an existing L1 fragment. The next variant should use the
same meaningful-growth threshold to seal all current L0 generations into one
large L0 generation instead. That keeps the existing lower base untouched,
removes fragmented L0 fan-out with roughly 1 GB of streaming input, and makes
the geometric growth rule prevent repeated rewrites of the sealed generation.

The first same-level implementation deliberately required the complete L0
output to be at least twice its largest input generation. Its 1M control never
sealed: the final manifest contained a 1.036 GB anchor plus 384.6 MB spread
over 17 newer generations, so the complete 1.42 GB L0 could not satisfy 2x
growth. It nevertheless provided a matched control at 818.28 seconds ready,
0.9852 live recall, 3.36 GB peak live RSS, 2.35 GB demand, and 42.73/91.82 ms
detailed mean/p95. Final L0 was 43 physical runs across 18 generations above
a 2.095 GB lower base. This corrected the policy: the established anchor must
not be part of a newer-delta seal.

The anchor-preserving prefix seal passed the next 1M gate. Once total L0
reached half the lower base, it compacted a newer prefix only after that prefix
was at least twice its largest component. The authoritative manifest retained
the original 991.9 MB anchor and 2.077 GB lower base, published one 381.9 MB
prefix generation, and finished with 28 L0 runs across 12 generations. Ready
time improved to 804.56 seconds, compaction time fell from 118.70 to 105.24
seconds, pressure steps fell from 654 to 615, and snapshot cloning fell from
917.8 to 792.8 MB. Total compaction input remained essentially flat at
15.22 versus 15.16 GB because the prefix seal replaced geometric carries
rather than adding a base rewrite.

Memory and warm query latency also improved: peak live RSS fell from 3.36 to
3.06 GB, demand from 2.35 to 2.28 GB, warm p95 from 88.6 to 79.8 ms, and the
detailed profile from 42.73/91.82 to 37.21/81.37 ms mean/p95. Detailed recall
was 0.98354 versus 0.98481, a 0.127 percentage-point delta. The live
concurrency curve regressed under a noisier host interval, however, and disk
rose from 3,773,788 to 3,857,468 KiB, mostly in native posting segments plus a
smaller primary-boundary difference. Treat the policy as a checkpoint, not the
final result.

A recursive geometric-stack extension was tested and rejected. It could
theoretically seal a newer prefix above each successively dominant anchor while
retaining the same 2x proof, but no recursive seal was legally eligible in its
full 1M run: the two closest prefix ratios finished at approximately 400/241
MB and 159/84 MB. The planner correctly left 44 L0 runs across 16 generations
instead of forcing either low-growth rewrite. This run recovered the load
baseline at 753.16 seconds ready with 0.9845 live recall, 14.18 GB compaction
input, 94.85 seconds compaction time, zero overloads, and 2.02 GB demand.
However, it was only another no-seal variance sample: live RSS was 3.21 GB and
the warm detailed profile regressed to 52.15/104.59 ms mean/p95. The recursive
planner and test were reverted; the validated anchor-preserving checkpoint is
the retained policy. Do not weaken the 2x bound merely to force a prettier
final manifest.

Public-profile boundary diagnostics also ruled out tree shape as the cause of
the earlier 1M query-latency spread. Replaying the same 1,000 public API
queries against the anchor-preserving and no-seal trees produced 443.69 versus
438.91 rerank candidates/query, 0.00789 versus 0.00786 mean boundary-tail
error, and materially identical interval gaps and recall. Their prior
37.21/81.37 versus 52.15/104.59 ms mean/p95 profiles were therefore cache and
host-state variance, not a quality regression caused by the L0 layout.

A promising-candidate-first progressive rerank was implemented and rejected.
It kept candidate selection unchanged and sorted vector ids within each
128-vector storage batch, but activated the existing interval stop much more
often. At 1M it skipped 75.17 of 444.88 candidates/query and improved a noisy
cold replay from 96.92/144.85 to 60.80/109.33 ms mean/p95, while recall moved
from 0.98392 to 0.98072. At 50K it skipped 106.11 of 254.31 candidates/query
but recall fell from 0.98396 to 0.95400, outside the parity envelope. The
quantization intervals are not a sufficient hard membership proof for this
reordering. The execution change was reverted; retain vector-id ordering and
boundary rerank until a stronger conservative bound is available. Also do not
compare the 50K diagnostic-only cold replay's 14.16 ms mean against the 3.67
ms post-warm profile: diagnostic-only resume intentionally skips the two
official serial warm-up stages.

Metric-correct angular covering radii were also implemented and rejected for
the Cohere cosine workload. A fresh 50K public-API qualification preserved the
load and quality envelope at 22.37 seconds ready and 0.9838 detailed recall,
and 208 of 214 frontier nodes resolved a durable exact-centroid bound. However,
all 1,000 queries still explored all 208 leaves and no bound stop fired. In
1,536 dimensions the exact leaf-enclosing angular spheres are nearly
hemispherical, so subtracting their radii from the query-to-centroid angle
collapses the triangle-inequality lower bound to zero. Do not pay write/format
cost for cosine enclosing spheres on this design; useful cosine stopping needs
a tighter proof object than a single centroid sphere. The experiment was
reverted. The profiler retains the traversal counters, and progressive rerank
now explicitly retires any candidates skipped by its existing proven stop so
they cannot re-enter the final approximate ordering.

The primary WAL checkpoint floor and immutable publication window must be
treated as two separate bounds. Raising both to 256 MiB reduced flush topology
but was rejected: the 1M lifecycle regressed to 735.01 seconds ready, cloned
8.57 GB cumulatively, and spent 144.63 seconds publishing native HBC posting
generations. Rotating the mutable epoch at 128 MiB while continuing to stream
two epochs into one 256 MiB immutable publication recovered the load baseline.
The fresh 1M public-API lifecycle completed in 623.16 seconds (613.58 insert +
9.59 catch-up), preserved 0.9845--0.9849 recall, peaked at 2.91 GB RSS with
1.40 GB attributable demand, and reduced manifests from the prior 598 to 167.
All nine primary pressure events completed with zero overload or hard debt;
cumulative snapshot copying was 3.24 GB. The corresponding 50K gate completed
in 22.43 seconds with 0.9849--0.9862 recall, 1.22 GB peak RSS, and 322.13 MB of
snapshot copies. Retain the nested 128/256 MiB bounds: a single large mutable
epoch couples source-scan cloning to HBC publication cadence even when the
eventual persisted flush topology looks cleaner.

Making the existing anchor-preserving L0 delta seal eligible under soft
maintenance was also tested and rejected. On a clone of the completed 1M
generation it reduced the active primary topology from 40 runs / 33 L0 runs to
20 / 13 without changing the seven-run lower base or recall. That did not make
reranking faster. The original detailed public profile was 36.19/82.12 ms
mean/p95 with 24.12 ms mean in exact-artifact reads; the immediate post-seal
profile was 46.89/93.02 ms with 34.77 ms artifact reads, and a second clean
reopen remained worse at 50.12/108.78 ms with 37.15 ms artifact reads. Fewer
generic LSM runs destroyed favorable artifact block locality rather than
improving it. The soft scheduling change was reverted; the hard-pressure seal
remains available for admission safety.

The half-window run therefore sharpens the next format boundary. Native HBC
tree traversal itself averaged about 10.2 ms once exact-vector loading is
subtracted, while primary artifact reads consumed roughly two thirds of warm
query time. A per-index float32 plane would duplicate about 3.07 GB at 1M and
is not an acceptable endpoint. The next high-upside design is one table-level,
versioned vector-block store shared by every index using an embedding artifact,
with source sequence and artifact revision in its WAL/delta publication and
generation leases matching HBC topology to the exact vector revision.

Before adding that format, a matched 1M read-path experiment showed that much
of the cold artifact cost was avoidable serialization. On the same repaired
native generation, raising sparse point-block overlap from four to sixteen cut
server mean/p95 from 86.51/166.44 to 51.72/93.73 ms and artifact-read mean from
72.33 to 39.65 ms, with identical 0.98477 recall, candidate counts, and cache
residency. Fixed sixteen was not the production endpoint: at 30 public clients
it peaked at 132.83 QPS with 630/1341 ms p95/p99 because every query could
independently fan out to sixteen reads.

The retained policy gives each batch up to sixteen reads but divides a
64-read backend-wide target across simultaneously active sparse batches. It is
non-blocking: later arrivals narrow themselves, and a share below two falls
back to the scalar precedence-preserving path. Against fixed sixteen, the
matched warmed 1/5/10/20/30-client curve changed QPS from
30.12/80.89/136.51/138.99/132.83 to
30.98/86.57/117.48/153.54/160.48. At 30 clients p95/p99 fell from
630/1341 to 475/645 ms. The post-curve detailed profile preserved 0.98477
recall and measured 35.02 ms mean, 71.16 ms p95, and 23.45 ms mean artifact
reads; the prior published half-window profile was 36.19/82.12 ms with
24.12 ms artifact reads. Peak sampled RSS was 1.865 GB and the process
physical-footprint ledger was 1.60 GB under the explicit 2 GiB envelope.

This is a general sparse LSM read improvement, not an HBC-only shortcut, and
it does not change key precedence, tombstone handling, block-cache ownership,
or recall policy. It improves the current primary-artifact miss path while the
shared table-level vector-block store remains the architectural endpoint.

The native posting/WAL and shared exact-vector block formats moved the next
query bottleneck into topology routing. A clean vector-block 1M generation
visited about 2,022 tree leaves, scored 235K approximate vectors, reranked 1,639
exact vectors, and measured 27.83 ms public p50 / 25.98 ms HBC p50 at 0.98437
detailed recall. Recursive binary and one-shot global k-means topology rebuilds
did not improve the leaf-recall curve. At 50K they increased topology and/or
workspace cost, retained roughly the same 208-leaf query budget, and produced
no latency win. They remain useful negative experiments, not production
defaults.

A flat full-precision centroid directory did improve the routing curve. At the
normal 0.5 effort, the same 1M generation reached 0.99095 recall with 15.34 ms
public p50 and 14.10 ms HBC p50. At the lowest measured parity boundary (0.47),
it reached 0.98495 recall with 13.40 ms public p50, visited 1,408 leaves, scored
165K approximate vectors, and reranked 443 exact vectors. The official public
API cold/warm runs at that boundary measured 12.2/12.2 ms p50 and 0.9829
recall, versus 28.6/30.6 ms and 0.9842 for the tree. The matched
1/5/10/20/30-client curve improved QPS from
60.83/215.15/219.67/199.38/183.97 to
81.04/322.14/332.81/320.96/291.32; concurrency-20 p95 fell from 146.71 to
79.11 ms. Attributable restart demand stayed 423--468 MB. Cache-inclusive RSS
rose when queries touched more mmap pages, so it must continue to be reported
separately from reclaimable demand.

The production policy is adaptive rather than a benchmark-wide override. Small
indexes keep tree routing (exact flat routing was about 3% slower at 50K), and
indexes estimated to have at least 1,024 postings use the exact directory at
the caller's unchanged effort. Full checkpoints now encode that directory as a
versioned, block-columnar entry in the same immutable native segment. Clean
generations borrow its aligned vectors directly under the query generation
lease; a post-checkpoint packed-node mutation refuses the stale entry and uses
the topology fallback until a matching generation is published. This removes
the restart topology scan and the roughly 54 MB exact-centroid heap copy at 1M
without introducing another store or publication boundary.

The complete persisted-centroid-delta lifecycle then passed a fresh 1M public
API qualification under a 2 GiB process budget. It inserted in 592.39 seconds
and reached full query visibility in 634.64 seconds, versus the earlier
619.96-second vector-block insert baseline. Live serial p50 was 13.7 ms at
0.9900 recall and the concurrency curve peaked at 274.54 QPS, improving the
earlier roughly 220-QPS peak. The final shared vector base encoded one million
768-dimensional vectors in 3.164 GB in 33.25 seconds; the native HBC generation
was 278.54 MB. All eight primary hard-pressure events completed with no
overload or remaining hard debt.

The same run separated reclaimable mmap residency from attributable demand.
Peak live demand was 1.812 GB and ResourceManager's managed peak was 1.040 GB
with zero release-accounting errors, but sampled RSS reached 6.128 GB live and
6.251 GB after restart. Disk was 6.5 GB total: 2.9 GB in shared vector blocks
and 270 MB in the native HBC index. Reopened detailed queries measured 27.28 ms
public p50 / 25.95 ms server p50 at 0.99055 recall, scoring 2,048 leaves and
239.6K approximate vectors; exact-vector loading accounted for 3.37 ms. The
literal first server search after a clean reopen was 199.57 ms, down from the
earlier roughly 650-ms activation sample, while the Python client's 1.76-second
wall time was dominated by loading its Parquet fixture.

A same-generation public `search_effort` sweep confirmed that candidate work,
not routing lookup or rerank storage, is the remaining steady-query lever.
Effort 0.40 visited 588 leaves and reached 11.18 ms server p50 but only 0.95255
recall. Effort 0.45 visited 1,097 leaves and reached 16.43 ms at 0.97895 recall.
Effort 0.46 visited 1,243 leaves, scored 145.8K approximate vectors, and reached
17.96 ms at 0.98195 recall, 0.86 percentage points below the default. This is a
useful explicit latency/quality point, not enough cross-dataset evidence to
change the public 0.5 default.

The restart RSS diagnosis exposed a physical-layout issue in the first shared
vector-block writer. Although vector payload checksums were already lazy, the
file interleaved each compact key with its roughly 3 KiB vector. Correct startup
validation of every key's checksum, shard, and total ordering consequently
faulted nearly every mmap page. New blocks now place all keys before an aligned
vector arena and retain the index/footer at the end. This preserves the same
reader, revision, checksum, and lazy-payload contracts—including compatibility
with existing interleaved blocks—but lets admission touch only compact key and
index pages. A fresh columnar-block lifecycle is required to quantify the RSS
and restart improvement; an old generation cannot gain it without rebuilding
its base.

The first fresh columnar-block 50K lifecycle passed the public API gate. It
inserted in 31.23 seconds and became ready in 33.34 seconds with 0.9869 live
recall, 7.7/11.5/14.5 ms serial p50/p95/p99, and a 699.65-QPS peak. The shared
base encoded 307.2 MB of float vectors into a 311.65 MB generation in 6.44
seconds. Most importantly, cache-inclusive restart RSS fell from the prior
interleaved-block sample's 609 MB to 383 MB, and live RSS fell from roughly
1.22 GB to 624 MB. Reopened recall remained 0.9851. Query latency in that
restart interval was host-contended (21.6 ms warm p50 versus the prior roughly
12 ms), and the system wired baseline moved enough to make the one-shot demand
delta non-comparable; neither is used to claim a search-kernel change. The RSS
direction is nevertheless the expected direct result of leaving untouched
vector pages out of the process mapping residency.

The interrupted first columnar 1M lifecycle exposed two readiness bugs rather
than a search failure. Background vector maintenance enumerated only live
write-cache databases, and the no-replay startup path returned before native
projection maintenance. Even after a later build, the public embeddings view
could erase the DB's pending state once external coverage converged. Native
projection absence is now broad startup debt, is executed at the stable source
tip, and is represented explicitly as `dense_vector_projection_pending` all
the way through shard aggregation and the public response. The checked-in
qualification runner refuses to query while that bit, backfill, or dense
publication is pending. LSM soft pressure still defers generic LSM work but no
longer starves this separately governed vector publication lane.

The 1M migration then built a complete 128-shard columnar float32 base in
41.98 seconds. Its actual ResourceManager builder/overlap peak was about
120.8 MB and the dedicated slice stayed within its 128 MiB hard limit with no
pressure event or rejection. A clean public restart measured 0.99061 recall,
28.41 ms client p50, 27.13 ms server p50, and 16.74 ms p50 in exact-vector
payload reads. It scored the same 2,048 leaves, 242.4K approximate vectors,
and 446.7 exact vectors as the prior generation, with zero primary-LSM or
vector-block fallback. Cache-inclusive RSS was 3.38 GB, but attributable
demand was only 437 MB; most of the difference was reclaimable mmap residency.

Ordering exact reads by `(shard, full_hash, key)` fixed a latent physical-sort
bug: ordering by full hash alone alternated among all low-bit-selected shard
files. A byte-bounded external rerank unit now coalesces sets that fit within
2 MiB (up to 512 vectors) and retains 128-entry progress checkpoints for long
tails. On the matched 1M corpus it preserved 0.99061 recall and improved server
p95/p99 from 80.07/128.42 to 75.94/108.72 ms, but did not improve p50. This
ruled out per-batch lease/sort overhead as the median bottleneck and isolated
the float32 exact plane's random payload reads.

A versioned float16 query projection produced the first large cross-scale
read-path gain without discarding the authoritative float32 artifact. The
source document/embedding store and mutation WAL remain float32; only the
immutable mmap base is encoded as float16, and source-sequence/revision misses
retain the primary fallback. On the matched 1M generation, recall changed only
from 0.99061 to 0.99047. Server p50/p95/p99 fell from
27.41/75.94/108.72 to 18.30/43.97/71.94 ms, client p50/p95/p99 fell from
28.64/77.39/109.91 to 19.65/45.25/73.19 ms, and exact-payload read p50 fell
from 17.12 to 7.79 ms. Cache-inclusive RSS fell from 3.38 to 2.28 GB while
attributable demand stayed about 434 MB. The base shrank from 3.162 to
1.626 GB, published in 40.02 seconds, used 57.4 MB at the governed builder
slice peak, and had zero misses, fallbacks, pressure events, or rejections.

The matched 50K comparison generalized the result. Float32 versus float16
recall was 0.98514 versus 0.98482, a 0.032-percentage-point change. Server
p50/p95/p99 improved from 15.69/29.81/53.68 to 9.37/12.61/13.97 ms and
client p50/p95/p99 from 17.45/32.19/55.28 to 11.37/14.72/16.71 ms. Restart
RSS fell from 457 to 380 MB and the base from roughly 312 to 158 MB. Its
coverage-only-WAL, 64-to-128-shard migration published in 3.43 seconds with a
48.2 MB builder-slice peak and no fallback or resource pressure.

That 50K migration deliberately found two publication invariants that the
already-128-shard 1M base did not exercise. First, a complete base replacement
was incorrectly forbidden from changing shard count. Second, the validator
treated a WAL containing only a coverage certificate as mutation debt. The
fixed contract permits an all-shard replacement to re-shard only when replay
contains no committed upsert or tombstone; coverage-only frames are subsumed by
the pinned snapshot. Sparse deltas and any mutation-bearing WAL retain the old
shard identity. Tests cover both the allowed coverage-only 4-to-8 replacement
and rejection of the same change after a committed vector mutation.

The current format version adds a per-vector float32 scale to float16 blocks.
Ordinary embeddings retain scale one; larger finite vectors are divided into
the safe float16 domain and rescaled during decode, while non-finite inputs
fail before mutating writer state. Readers remain compatible with v1 float32
and v2 unscaled float16 bases. Float16 is now the native projection default,
with an explicit float32 qualification/rollout override; this does not change
the precision of authoritative storage or recovery.

The first fresh scaled-float16 (`AFVBLK` v3) public lifecycle established the
current 50K end state. The official VectorDBBench load inserted in 22.16
seconds and reached full index/vector-projection readiness in 22.20 seconds,
with 0.9857 recall. The detailed 1,000-query public profile measured
7.75/9.75/11.21 ms client p50/p95/p99 and 5.97/7.88/9.08 ms server latency;
QPS at concurrency 1/10/30 was 95.5/966.5/1,083.8. Live/restart RSS was
926/368 MB and restart demand was 120 MB. The v3 block encoded 50,000 source
vectors into 158.3 MB in 3.06 seconds. The table had only the external vector
index during load; no full-text consumer participated.

The matching fresh 1M public lifecycle inserted in 751.66 seconds and became
fully ready in 755.71 seconds (12.60 minutes), correcting the reported
roughly-3,000-second batch-100 result while preserving 0.9901 official recall.
The final vector generation encoded 1,000,000 768D source vectors into 1.630 GB
in 30.82 seconds. Total disk was 5.54 GB: approximately 3.50 GB authoritative
primary document/artifact LSM, 1.59 GB shared exact-vector projection, 448 MB
native posting generations/WAL, and only 324 KiB in the compatibility HBC LSM.
Live RSS peaked at 3.41 GB; a clean restart used 3.01 GB RSS and 663 MB
attributable demand. Four concurrent public loaders were slower than the
earlier roughly-592-second serial result, so this meets the expected 13-minute
gate but does not close the remaining primary-store contention work.

That run also exposed a benchmark lifecycle contamination. VectorDBBench wrote
`key:__circus_write_probe__` before every skip-load search phase. On the first
1M reopen those non-vector source revisions overlapped bounded posting repair,
grew the posting WAL from source sequence 10001 to 10003, and made the supposed
cold measurement describe a mutated system. The adapter now has an explicit
read-only-reuse contract: query-only qualification verifies the existing table
and vector index but cannot issue the sentinel, remove full text, or create an
index. The repo runner also gates restart queries on zero posting backlog and
uses this contract for cold, warm, and concurrent reuse phases.

A controlled read-only rerun proved that boundary. The WAL mtime and size
remained exactly unchanged at 64,571,893 bytes, source applied/target sequence
remained 10003, and the reopened process reported zero dirty postings and zero
maintenance mutations. Genuine cold 1M serial p50/p95/p99 was
20.2/57.2/94.0 ms; the immediately warm official run was
14.2/16.9/18.9 ms at 0.9901 recall. A separate 100-query public profile measured
14.38/16.83/17.77 ms and 0.9895 recall. The stable 1,000-query auto-directory
control remains 14.50/17.47/21.54 ms at 0.99005 recall. `flat_rabitq` was
slightly faster but fell to 0.97423 recall, so the exact persisted directory
remains the production default.

Finally, `DB.runUntilIdle` had applied only one bounded 64-posting repair page.
That was safe for ordinary background maintenance but wrong for an explicit
lifecycle fence: large loads could return and reopen with later pages still
pending. The idle path now repeats bounded, query-cooperative pages until the
posting backlog is clean, without busy-waiting on asynchronous checkpoints or
the vector-block debounce. A regression dirties more than 64 leaves and proves
that one `runUntilIdle` call drains every page. Foreground/background write
paths remain bounded and may defer to active queries.

The fresh post-drain 50K gate showed that the honest fence does not regress the
small-corpus load target. Insert took 22.0018 seconds and complete readiness
22.0255 seconds, with 0.9860 official recall. Live serial p50/p95/p99 was
5.1/6.8/8.8 ms and QPS at concurrency 1/10/30 was
148.6/1,105.0/1,097.6. Read-only cold and warm restart p50 were both 6.2 ms.
The 1,000-query public profile measured 6.67/8.59/9.68 ms client and
4.97/6.85/7.70 ms server p50/p95/p99 at 0.98582 recall. Restart RSS was 374 MB
and its attributable footprint ledger was 120 MB. This supersedes the earlier
22.20-second v3 gate for current-code latency and lifecycle behavior.

The matching post-drain 1M load inserted in 737.35 seconds and the legacy
VectorDBBench optimize predicate returned at 745.53 seconds (12.43 minutes),
with 0.9899 recall. Steady live serial p50/p95/p99 was
14.2/17.0/18.2 ms and concurrency 10/20/30 reached
226.2/232.8/204.6 QPS. Live RSS peaked at 2.82 GB and the post-load process
footprint ledger at 1.90 GB. Disk was 5.61 GB: 3.53 GB primary LSM, 1.63 GB
float16 vector blocks, and 446 MB native HBC state.

That run also isolated a remaining readiness cliff without the old write
sentinel: concurrency one issued a single request that took 40.96 seconds.
The native vector generation was still encoding 1.63 GB from 3.10 GB of
authoritative artifacts and published in 40.78 seconds; the first request
arrived after the legacy adapter accepted `rebuilding=false` but before that
projection and the final 73 centroid repairs had completed. The adapter now
requires Antfly's authoritative readiness state, current replay watermarks,
zero dense publication/projection debt, and zero dirty postings. `runUntilIdle`
also preserves the ordinary bounded maintenance side effects, drains every
posting page, publishes the vector generation, and rechecks posting debt before
returning. This is a lifecycle fence, not a benchmark warm-up.

Primary ingest remained slower than the earlier 592.39-second four-worker
control even though both used batch 100 and the same client concurrency. The
post-drain run rotated 3.08 GB of mutable state through 51 current-scan epochs,
versus 1.26 GB through 22 epochs in that control. The 128 MiB WAL checkpoint
floor had coupled mutable-generation size to the 256 MiB immutable merge
window, recreating the large scan/snapshot surface that the window was meant
to remove. The next controlled A/B restores the independent 32 MiB mutable
floor while retaining WAL-backed epochs, the 256 MiB streaming linear merge,
four-way L0 tiers, and ratio sealing.

## 2026-08-26 end-state qualification

The linear-merge/WAL follow-up found that a logical immutable merge window must
not be forced out by every size rotation. Idle and WAL-pressure rotations still
publish partial windows, while ordinary size rotations accumulate to the
configured window. The WAL checkpoint floor also has a two-segment lower bound
for windowed publication so a segment-straddling retained tail cannot recreate
tiny flush cascades. Raising the resident mutable cap to 768 MiB was rejected:
it increased 1M readiness to 858.29 seconds and peak RSS to 3.17 GB without a
query benefit. The production cap remains 256 MiB.

Native posting checkpoints had another independent write-amplification bug.
The file format already supports eight immutable deltas, but managed policy
flattened after two. Using the format limit reduced the 1M load from five full
posting rewrites to two. On the final public batch-100/four-worker load, insert
completed in 715.28 seconds. Primary storage performed six pressure
compactions with zero overloads, flushed 3.576 GB in 116 flushes, accumulated
3.565 GB of mutable snapshot copies plus 1.936 GB of read-snapshot rotations,
and retained seven lower-level runs. Live RSS peaked at 3.187 GB. This is a
real public API load with the default full-text index removed through the API;
it is not a direct-store or batch-size shortcut.

Exact-vector publication exposed two production issues. First,
`BudgetedAllocator` did not retry incremental growth after aggregate cache
reclamation, unlike ordinary ResourceManager admission. A 1M builder could
scan and spool the full corpus, fail with `ResourceBudgetExceeded`, delete its
work, and immediately repeat. Incremental growth now reclaims governed HBC and
LSM cache before denying the allocation. Second, asynchronous status
invalidation left a race in which clients could observe the preceding ready
snapshot during a long base build. The maintenance owner now publishes pending
from its leased writer before the build and publishes the terminal state on
success or failure. Projection encoding and physical shard geometry are both
part of readiness, so a policy migration atomically replaces `CURRENT` rather
than silently retaining a suboptimal generation.

The builder now uses 64 KiB partition buffers. A controlled same-corpus A/B
rejected 256 vector shards even though they built in 34.40 seconds: cold
p50/p95/p99 was 28.1/66.2/101.5 ms, warm was 22.0/28.9/31.1 ms, maximum QPS
was 213.4, and restart RSS peaked at 2.605 GB. The final 128-shard generation
built under the same 2 GiB resource envelope in 43.29 seconds, removed every
spool after atomic publication, and reduced cold latency to
19.0/41.5/59.5 ms. Warm p50/p95/p99 was 12.3/15.2/19.3 ms at 0.9903 recall;
QPS at concurrency 1/10/20/30 was 78.1/267.2/225.9/214.6. The 1,000-query
public profile measured 12.86/15.70/18.96 ms client and
11.51/14.27/16.12 ms server p50/p95/p99 at 0.99027 recall. Restart RSS peaked
at 2.527 GB and attributable demand at 625 MB. The final vector directory is
1.630 GB across 128 float16 blocks; the complete data tree is 5.455 GB.

The measured final insert plus same-corpus 128-shard build is 758.57 seconds
(12.64 minutes), inside the 13-minute goal. A fresh single-lifecycle repeat is
still required before treating that sum as publication-grade readiness rather
than qualified component timing. The small-corpus end-to-end gate did complete
in one lifecycle: 50K inserted in 20.02 seconds and was fully ready in
20.05 seconds, with 0.9862 recall, live serial p50/p95/p99 4.8/6.8/9.1 ms,
1,090 peak QPS, 1.017 GB peak live RSS, and 325 MB peak restart RSS.

## Native shadow certification and restart end state

A capture-free dense repair candidate exposed a cross-generation publication
hole. The replacement could reach its source tip and publish its ready marker
without ever creating a native posting generation. Vector blocks correctly
refused to bind without a matching durable posting sequence, but activation
could still select the candidate's compatibility HBC LSM. The resulting index
was complete and queryable yet restarted with roughly 70 MB of obsolete HBC
LSM state, 1.50 GB RSS, and 36--79 ms query latency instead of the native read
path.

Shadow readiness now certifies a complete native posting generation before its
first ready marker, validates and flattens the converged candidate outside the
short activation fence, and verifies only the final WAL tail while source apply
is paused. Capture-free builders bootstrap one complete checkpoint directly
from their private compatibility projection; only after that checkpoint is
durable may they publish `AUTHORITY`, detach the live LSM, and bind a vector
generation at the identical `covered_source_sequence`. A native-first reopen
then removes only the obsolete `runs`, `wal`, manifest, and lock artifacts,
while preserving posting segments, generation identity, and active query
leases.

The repaired 50K generation demonstrated the intended restart state. Its
native-only HBC directory was 22 MB instead of 70 MB, the complete data tree
fell from 545 MB to 496 MB after safe legacy cleanup, restart RSS was 309 MB,
and a 1,000-query public profile measured 5.48/6.95/7.93 ms client and
3.80/5.16/6.16 ms server p50/p95/p99 at 0.98457 recall. Warm concurrency
1/10/20/30 reached 208/1,198/1,408/1,241 QPS. This was a migration of an
existing generation, so a fresh lifecycle remained the correctness gate.

The fresh public-API gate then inserted 50,000 1536D vectors in 21.11 seconds
and reached authoritative readiness in 21.13 seconds. The vector base encoded
in 2.60 seconds, live peak RSS was 1.147 GB, and restart peak RSS was 394 MB.
Cold and warm VectorDBBench recall were both 0.9862 with 6.3 ms p95 and
7.2--7.3 ms p99. The separate 1,000-query public profile measured
5.52/7.13/7.93 ms client and 3.85/5.26/6.18 ms server p50/p95/p99 at 0.98617
recall. Full text was removed through the public table API before load; the run
used batch 100, four public writers, native HBC WAL/segments, float16 vector
blocks, and read-only restart phases.

That fresh run also caught an empty-index lifecycle bug before the final gate.
An empty root intentionally has no quantized payload, but stable validation
treated that canonical absence as corruption and repeatedly repaired it back
to absence while holding structural admission. Missing payload is now valid
only for an empty posting; non-empty absence remains corrupt. Stable validation
also retries one fresh-lease no-progress observation, requires a clean
verification pass, and fails after bounded repeated mutations. This prevents
both premature native readiness and an infinite WAL rewrite if some future
payload cannot converge.

The next fresh gate found that a time-only quiescence test could still mistake
an LSM backpressure pause for the end of a burst. At sequence 204, with only
about 20,300 source rows, the primary still held 19 immutable memtables,
266.8 MB of immutable state, and a maintenance score of 111,276; nevertheless
the optional vector publisher built a 64 MB intermediate base. Opportunistic
publication now requires a complete quiet interval after the primary has zero
immutable state, WAL checkpoint/pressure debt, active compaction jobs, and
maintenance score. Caller-owned stable lifecycle fences remain immediate.

With that gate, the fresh public-API 50K run inserted in 20.87 seconds and was
ready in 20.90 seconds. It published no non-empty vector base before the final
source sequence 501. Recall was 0.9851 before and after restart; live p95/p99
was 6.1/7.0 ms, cold and warm p95/p99 was 6.4/7.8 ms, and peak QPS was 1,316.
The 1,000-query public profile measured 5.43/7.27/9.17 ms client and
3.70/5.11/5.98 ms server p50/p95/p99. Restart RSS was 321 MB. The 490 MB data
tree consisted primarily of the source document/artifact LSM, 151 MB of
float16 exact-vector blocks, and a 22 MB full native posting segment.

The first uninterrupted 1M lifecycle with the same online topology completed
insert-to-ready in 1,078.48 seconds (17.97 minutes). This is a large correction
from the reported 3,000 seconds, but it remains above the roughly 13-minute
target. The only non-empty exact-vector base published at final sequence 10,001
and encoded 3.072 GB of source vectors into 1.630 GB of float16 blocks in
36.1 seconds. Final posting validation flattened to a 279 MB native segment.
Thus, vector publication itself accounts for less than a minute; primary LSM
pressure and compaction set the remaining load curve.

The primary finished with 25 runs (18 L0), zero active maintenance job and zero
maintenance score, after 13 compactions read 7.754 GB and wrote 7.677 GB. Six
foreground pressure events all completed a pressure compaction, but ingestion
followed a reactive sawtooth near the 128-run hard limit. Direct sorted ingest
succeeded for 4.504M of 5.000M physical entries; 0.497M fell back while the
backend was pending. Cumulative mutable snapshot copies were 3.707 GB and read
snapshot rotations were 3.535 GB. The complete tree used 5.1 GiB: approximately
3.52 GB of source document/artifact runs, 1.63 GB of vector blocks, and 279 MB
of native HBC postings.

Recall was 0.9900 live, cold, and warm. Live queries overlapped the tail of
post-ingest storage work and were not publication-quality: serial p95/p99 was
47.5/68.5 ms, peak QPS was 87.6, and one concurrency-30 wave stalled for about
40.9 seconds. Reopened results isolate the native query path: cold p95/p99 was
15.7/21.8 ms and warm was 15.4/16.9 ms. The separate 1,000-query public profile
measured 12.97/15.42/16.48 ms client and 11.49/13.94/14.76 ms server
p50/p95/p99. It traversed 2,048 leaves and scored about 238K approximate
vectors/query before boundary rerank. Live/restart RSS peaked at 2.77/2.36 GB;
attributable restart demand was 409 MB.

A bounded recursive topology rebuild was then tested as an end-state
experiment. At 50K it consumed a measured 312.65 MB workspace and 4.6 seconds,
but reduced mean approximate candidates only from 24,189 to 23,927 and
regressed server p95/p99 from 5.11/5.98 ms to 6.17/7.38 ms. It is therefore not
a default-quality win. More importantly, that run exposed that the durable
tree was paired with a process-local "already rebuilt" sequence. Reopen reset
the marker, and coverage-only source advances from 501 to 503 rebuilt the same
tree twice more, consuming 4.4 seconds and 312.65 MB each time and changing
recall as randomized replacement trees published.

Topology rebuild identity is now durable in the same captured HBC metadata
transaction as the replacement root. Its epoch is the vector base generation,
the latest actual vector-WAL mutation sequence, and the algorithm. Generic
coverage commits do not change it. WAL replay reconstructs the same vector
epoch, while a real vector mutation, new base, or algorithm change admits one
new rebuild. This makes restart idempotent and prevents an optional optimizer
from silently becoming startup work; recursive topology remains opt-in until
it demonstrates recall/latency value. A fresh native-authority qualification
confirmed exactly one rebuild in the initial lifecycle and none after reopen:
root 441 and node count 1,315 were identical before and after coverage advanced
from sequence 501 to 503, recall remained 0.9831 live/cold/warm, and restart RSS
fell from the faulty run's 980 MB to 404 MB. Recursive still regressed ready
time to 22.39 seconds and server p95/p99 to 6.32/7.57 ms, so the 20.90-second
online topology run remains the qualified default.

### Exact-vector publication is part of readiness, not restart work

The first proactive-primary 1M run appeared to improve insert-to-ready from
1,078.48 to 740.27 seconds, but it was not a valid result. The public index
became ready while its exact-vector generation still covered the empty source
sequence. Reopen then built all 1M float16 vectors in 65.15 seconds. Query
visibility and recall happened to remain correct because the live process
could fall back to the primary LSM, but that made readiness, live latency, and
restart cost depend on an implementation fallback rather than the published
index generation.

Readiness now verifies source sequence, encoding, shard count, and exact vector
cardinality under one immutable generation lease. A vector mutation marks the
projection dirty and invalidates the old generation while holding the shared
publication mutex; a later vector-neutral transaction cannot advance the old
empty generation's coverage. The public status projects this writer-owned
pending fact as active finalization, so a client cannot observe ready between
derived catch-up and exact-vector publication. This ordering is crash-safe:
`CURRENT` remains on the last complete generation until the replacement base
is fully written and verified.

Waiting for the primary LSM to become completely idle before this optional
publication was correct but unnecessarily slow. A fresh 50K run inserted in
23.57 seconds, then waited 29 seconds for a small immutable tail to reach its
age-based flush threshold; the vector build itself took only 3.8 seconds.
Opportunistic publication now admits a stable primary tail only when there is
no WAL checkpoint or pressure block, hard L0 run/byte debt, or active
compaction, and the tail is bounded to 32 immutable memtables and 256 MiB.
The outer source-idle lease, two-second source debounce, and resource-manager
builder reservation remain mandatory. This is a general bounded-overlap rule,
not a VectorDBBench shortcut.

The resulting public-API 50K qualification inserted in 20.70 seconds and was
fully ready in 26.75 seconds. Generation 2 encoded all 50K vectors at source
sequence 501 in 2.79 seconds before readiness, with no restart rebuild. Recall
was 0.9849. Public client p50/p95/p99 was 5.45/6.99/8.06 ms and server
p50/p95/p99 was 3.74/5.15/6.20 ms. Live concurrency 1/10/20/30 reached
183/860/909/945 QPS. Live/restart peak RSS was 732/318 MB; attributable live
demand was 342 MB. The primary still had one safe 7.5 MB immutable tail at
publication, demonstrating that the removed wait—not weaker vector work—was
the speedup.

### Qualified proactive-tiering 1M end state

The uninterrupted 1M public-API qualification inserted in 663.75 seconds and
was fully ready in 717.10 seconds (11.95 minutes), beating the approximately
13-minute target. The exact-vector generation encoded all 1M source vectors
into 1.630 GB of float16 blocks at sequence 10,001 in 37.07 seconds before
readiness. The native posting store then flattened its seven-delta chain into
a 279 MB full generation. Reopen performed neither vector nor posting rebuild.

VectorDBBench measured 0.9902 recall and 0.9918 NDCG, with serial p95/p99 of
15.7/17.7 ms and peak throughput of 218.5 QPS on the contended development
host. Cold/warm reopen retained 0.9902 recall and measured 15.9/24.8 ms and
15.4/17.1 ms p95/p99 respectively. The separate 1,000-query public profile
measured 12.95/18.17/28.47 ms client and 11.48/14.95/22.05 ms server
p50/p95/p99. Live/restart peak RSS was 2.72/2.41 GB, while attributable demand
was 1.70/0.40 GB under the 2 GB process budget; mapped and reclaimable file
pages account for much of the RSS gap.

Primary proactive tiering limited the run to one completed pressure event.
Final primary state had 33 L0 runs, seven lower-level runs, no immutable tail,
and zero maintenance score. Cumulative primary mutable snapshot copies were
2.413 GB versus 3.707 GB in the 1,078-second baseline. The next load-side
opportunity is reducing current-scan copying and compaction write
amplification without returning to the reactive hard-limit sawtooth.

The dominant 1M query cost is now the exact flat centroid/quantized scan:
queries visit 2,048 leaves and score about 240K approximate vectors, consuming
11.93 ms mean HBC time. Exact boundary rerank loads about 448 vectors and costs
2.40 ms, of which artifact reads are 2.10 ms. Query work should therefore
prioritize recall-preserving routing/scoring layout and SIMD before changing
the rerank boundary. Packing immutable exact-vector shards into fewer indexed
container files is still worthwhile for metadata, restart, and artifact-read
tails, but it cannot by itself remove the larger approximate-scan cost.

## Native first-load transaction and linear consolidation (r55)

The first production bulk build must participate in the same exact-vector
capture as incremental `batchApply`. The initial implementation gated capture
on a nonzero HBC cardinality, and the recursive bulk-build wrappers bypassed
capture entirely. That made the empty sequence-one vector base stale on the
first replay window and forced a later primary-LSM reconstruction. Both gates
are now removed: ordinary and prepared-input bulk builders publish their
coalesced exact mutations into the HBC-native vector WAL transaction.

At a stable source tip, maintenance now treats a sequence-aligned native
WAL/delta generation as self-contained. It force-checkpoints the vector WAL to
the target encoding, merges base plus deltas one hash shard at a time, omits
latest tombstones from the complete replacement base, atomically publishes
`CURRENT`, and then validates/flattens the posting generation under the same
readiness fence. Only a missing generation or a base-only cardinality mismatch
uses the guarded pinned primary scan. This prevents an upload lull from causing
generic LSM snapshot cloning and prevents readiness from exposing a compact
vector base beside a large query-time posting overlay.

The public API batch-100 50K r55 qualification measured:

- 19.9995 s insert + 6.0391 s optimize = 26.0385 s ready;
- 0.9842 official recall and 0.9866 NDCG;
- 1,366.16 peak QPS; live serial p95/p99 6.7/8.9 ms;
- reopened detailed public p50/p95/p99 5.62/7.73/10.07 ms at 0.98421 recall;
- 1.484 GB cache-inclusive live peak RSS, 392 MB restart peak RSS, and
  115 MB attributable restart demand;
- one 154,800-KiB float16 vector base, one 23-MiB posting segment, and an
  empty current posting WAL.

The necessary A/B was r54. Publishing only the vector base left a 58-MiB
posting WAL instead of the 23-MiB immutable segment. Exact rerank candidates
rose from roughly 272 to 1,178, mean leaf scoring rose from 1.37 ms to 8.42 ms,
reopened public p95 rose to 15.75 ms, and live RSS peaked at 2.34 GB during the
query phase. Joining posting flattening to the stable-tip publication restored
latency and reduced the live peak by about 856 MB without changing the rerank
policy boundary.

## Native 1M qualification and bootstrap-linear checkpointing (r56)

The same public API lifecycle at 1M completed without retry, index
reactivation, capture-boundary failure, or primary-LSM vector reconstruction:

- 599.7847 s insert + 20.4622 s optimize = 620.2469 s ready, versus the r44
  843.5063 s insert + 81.7756 s optimize = 925.2819 s ready baseline;
- 0.9899 recall and 0.9916 NDCG, preserving the existing rerank boundary;
- 426.90 peak QPS, versus 235.58 in r44, with live serial p95/p99 of
  14.2/15.5 ms;
- restarted 1,000-query p50/p95/p99 of 12.10/15.07/22.66 ms at 0.98994
  recall, versus 14.58/20.66/23.95 ms in r44;
- 1.30 GB attributable live demand, versus 1.70 GB in r44;
- 4.773 GB cache-inclusive live RSS and 2.894 GB restart RSS, versus
  2.720/1.948 GB in r44;
- one 1,629,901,750-byte float16 vector base, one 280,684,301-byte posting
  segment, and empty mutation WALs. Total allocated data was 5,326,904 KiB,
  essentially unchanged from r44's 5,317,360 KiB because primary embedding
  ownership remains deliberately out of scope.

The RSS high-water occurred about 449 seconds into the load, not during final
publication. The vector manifest produced roughly 40 immutable generations.
Its former flat eight-delta limit repeatedly coalesced all accumulated
first-load vectors even though the bootstrap base was empty and the batches
were predominantly disjoint. This kept demand bounded but touched an
ever-growing set of clean mmaps and rewrote the same f16 projection repeatedly.

The r59 A/B allowed an empty base to append up to 64 immutable delta
generations, while an established base retained the eight-generation online
lookup limit. It measured:

- 503.9492 s insert + 24.4342 s optimize = 528.3834 s ready, 91.86 s (14.8%)
  faster than r56;
- 0.9903 recall, 536.55 peak QPS, and live serial p95/p99 of 13.5/13.9 ms;
- restarted detailed p50/p95/p99 of 11.45/13.94/20.89 ms at 0.99033 recall;
- 4.954 GB cache-inclusive live peak RSS and 1.70 GB sampled attributable
  demand, versus 4.773/1.30 GB in r56.

The 64-run policy therefore proves that repeated first-load rewrites cost
about 92 seconds, but it is too permissive as the final residency policy. The
production candidate checkpoints an empty bootstrap chain at 24 generations,
which should cause one mid-load coalescence on this corpus. The durable format
continues to admit 64 generations so tightening policy never makes a
previously valid `CURRENT` unreadable during restart or rolling upgrade.
Stable-tip maintenance still performs the complete shard-local merge,
atomically publishes `CURRENT`, and flattens postings under the same readiness
fence. This is a production first-load policy, not a batch-size exception:
crash recovery and queries continue to see every committed WAL/delta
generation, and ordinary online update fan-out is unchanged.

The 24-run r60 public qualification performed exactly one bootstrap
coalescence near 650K rows and measured 537.6565 s insert + 23.1201 s optimize
= 560.7766 s ready. This retains 59.47 seconds of the r59 speedup while the
4.814 GB cache-inclusive peak is effectively tied with r56. Recall was 0.9900;
the restarted detailed p50/p95/p99 was 11.79/14.43/23.55 ms. The format/policy
split therefore gives a better default balance than either eight or 64 runs.

r62 then released clean mmap residency after each input shard was durably
staged during delta and complete-base compaction. It preserved load throughput
(539.2666 s insert + 25.4540 s optimize = 564.7206 s ready), 0.9902 recall,
and live p95/p99 of 14.2/16.3 ms. It lowered RSS by approximately 135 MB at the
mid-load merge and reclaimed roughly 1.8 GB promptly after final publication,
but did not lower the historical high-water: validation had already touched
every new block's sorted index and key boundaries.

An r64 follow-up tested releasing those validation-touched pages immediately
after admission while retaining the immutable mapping. Reject this policy. It
made the 1M insert 601.0115 s (61.74 s slower than r62), raised live peak RSS
from 4.851 GB to 5.164 GB, and raised restart RSS from 2.624 GB to 2.891 GB.
Recall remained 0.9900 and detailed p50/p95/p99 was
11.60/14.19/21.87 ms, so there was no compensating query benefit. RSS briefly
fell to 1.10 GB after the mid-load merge, but continued mutation and final
publication refaulted the same pages and produced a higher high-water. Keep
post-shard maintenance reclaim from r62; do not evict a newly admitted
generation before its normal workload establishes actual residency.

The next query experiment kept the search effort and rerank boundary fixed and
changed only the AArch64 RaBitQ weighted-popcount reduction. Zig's prior
`@Vector(4, u64)` horizontal reduction lowered to repeated widening and scalar
extract sequences on NEON. Reducing 128-bit byte popcounts instead lowers to
`CNT` plus `UADDLV`; every other architecture retains the previous kernel. A
240K-candidate, 768-dimensional warm microbenchmark improved by 8.1%, with
identical integer results. Differential tests cover widths 0 through 32.

The r66 50K public lifecycle confirmed that the kernel win survives traversal
and heap admission: mean leaf scoring fell from 1.288 ms in r61 to 1.189 ms
(7.7%), mean HBC search fell from 3.817 to 3.706 ms, and peak QPS reached
1,649.6. Insert plus catch-up was 23.3697 + 6.0372 = 29.4069 s; recall was
0.9811, and restarted cold/warm p95 was 6.0/5.9 ms. The bit kernel is exact, so
the 0.21-percentage-point recall difference from r61 is concurrently-built tree
variance rather than approximate-math drift.

The r67 1M lifecycle then measured 556.4232 s insert + 35.1253 s catch-up =
591.5485 s ready, 0.9900 recall, and live serial p95/p99 of 13.8/14.9 ms. Mean
leaf scoring was 6.727 ms versus 7.223 ms in r62 (6.9% lower). Cache-inclusive
live peak RSS was 4.457 GB versus 4.851 GB in r62; restart RSS remained
effectively unchanged at 2.570 GB. The 2.20 GB physical-footprint sample is
within the run-to-run/host noise of r62's 2.27 GB and must not be claimed as a
demand reduction. The next query costs are the remaining 6.73 ms leaf scan and
2.02 ms exact-vector artifact read, not tree expansion (0.83 ms) or exact
distance arithmetic (0.05 ms).

The native exact-vector follow-up scores aligned little-endian float16 block
payloads directly instead of expanding them into request-sized float32 scratch
and then reading that scratch again. Conversion, query dot product, candidate
norm, and distance now share one SIMD pass. Payload CRC verification, immutable
generation leases, exact source-sequence equality, primary fallback, and the
rerank boundary are unchanged. Float32, WAL, unaligned, and big-endian values
retain the decoded path. Metric-parity tests cover L2, inner product, and
cosine including scalar tails.

r68 at 50K lowered mean native artifact read from 1.977 to 1.864 ms (5.7%) and
exact-vector load from 2.083 to 1.976 ms (5.1%) versus r66. Mean HBC search was
3.647 ms, peak QPS was 1,928.4, recall was 0.9820, and the complete public
lifecycle was 20.2534 s insert + 8.0855 s catch-up = 28.3389 s ready. r69 at
1M lowered artifact read from 2.021 to 1.875 ms (7.2%) and vector load from
2.331 to 2.185 ms (6.2%) versus r67. Mean HBC search was 10.788 ms, peak QPS
was 472.6, and recall was 0.9897. The lifecycle measured 569.5528 s insert +
2.0423 s catch-up = 571.5951 s ready. Its 4.852 GB live peak matched r62,
restart RSS was 2.563 GB, and sampled demand was 1.90 GB; do not attribute the
memory movement to this allocation-free query kernel. The concurrency-one
sample contained one 15.7-second cold/host stall and is not a steady-state
latency result.

Reader admission already validates the complete immutable index, every entry
range/flag/scale, the index checksum, every key checksum, and total hash/key/
source ordering. A further native lookup fast path reuses the artifact hash and
trusts those admitted index/key regions for the mmap lease lifetime instead of
re-parsing invariants and recomputing key CRCs at every binary-search step.
Per-vector payload CRC remains lazy and mandatory; checked admission and
compaction iteration are unchanged.

r70 at 50K lowered artifact read from 1.864 to 1.802 ms (3.3%), vector load
from 1.976 to 1.905 ms (3.6%), and mean HBC search from 3.647 to 3.532 ms
versus r68. Recall/restart was clean at 0.9840. A query-only reopen of r69's
identical 1M durable generation proved upgrade/restart compatibility and
preserved recall exactly at 0.98967. The first cold profile faulted mmap pages
and is intentionally not a steady-state comparison. An immediate warm repeat
lowered artifact read mean/p50 from 1.875/1.702 to 1.835/1.650 ms, vector load
from 2.185 to 2.122 ms, and mean HBC search from 10.788 to 10.401 ms; server
p95 fell from 13.02 to 12.21 ms on the same topology and source generation.

## Projection I/O, routing, and ingestion experiment matrix (r76-r87)

These experiments used the same preserved 1M public-API generation and fixed
the search effort/rerank boundary unless noted. New profile counters distinguish
logical exact/projection candidates from physical payload reads and bytes. The
current positional baseline averaged 557.43 physical reads and 857,178 bytes
per detailed query; this is the denominator for read-layout experiments.

- The current-code positional baseline (`read-baseline-v2`) measured 0.9521
  detailed recall, 69,128.7 approximate vectors/query, 146.3 exact
  completions/query, client p50/p95/p99 29.72/133.44/284.04 ms, server mean
  43.85 ms, and 678 MB sampled RSS. The unchanged official lifecycle
  (`govern-baseline-v2`) measured 0.9455 recall and concurrency
  1/10/20/30 throughput of 63.52/71.20/60.84/113.17 QPS.
- Sorting projection requests and coalescing offsets separated by at most 4
  KiB into at most 64-KiB reads was effectively a no-op: physical reads fell
  from 557.43 to 557.38/query while bytes rose slightly to 857,209/query.
  Client p95 was 106.33 ms, but the single short run is noise and cannot
  justify allocations, sorting, and copying on every query. Reject it for the
  current sparse candidates distributed across 128 shard files.
- Direct generation-leased mmap views eliminated the counted `pread` calls but
  raised RSS to 1.30 GB and worsened client mean latency to 62.92 ms. Mmap plus
  `MADV_WILLNEED` raised RSS further to 1.43 GB; client p95 was 126.84 ms.
  Reject both. Positional I/O is the material RSS win and must remain the
  serving default.
- Positional `MADV_WILLNEED` left reads/bytes unchanged and measured client
  mean/p95 48.73/103.20 ms, but raised RSS to 898 MB. The apparent latency
  movement was not replicated and costs roughly 220 MB, so cold-query
  readahead is rejected as a default. A future implementation would need a
  measured per-device cold classifier and a cache-pressure admission budget.
- Dividing a process-wide 128-read target by the number of active positional
  batches created wave barriers on top of the already shared `std.Io`
  scheduler. Throughput collapsed to 61.80/44.54/27.54/4.16 QPS at concurrency
  1/10/20/30. Keep the local eight-read wave and shared runtime governor; a
  future global policy must govern individual scheduled reads, not resize
  query-local synchronization waves.
- Naively reducing flat search effort fails recall parity: effort 0.38 scored
  about 53,847 vectors/query but recall fell to 0.9355, versus 0.9521 at effort
  0.40. A complete-directory covering-radius proof made zero certified stops:
  all nine attempted resolutions/query fell back because current generations
  contain unresolved radii. Do not publish heuristic pruning as correctness.
- Recursive HBC at effort 0.44 matched the flat detailed recall (0.9525 versus
  0.9521) but scored 110,656 vectors/query. Its official recall was 0.9452 and
  warm p95 55.6 ms, but concurrency-30 throughput was 53.25 QPS versus flat's
  113.17. Locality made the single-query profile look attractive while 60%
  more candidate work lost at concurrency. Keep flat routing; first make
  conservative radii complete/fresh, then retry proof-driven stopping.

The read-mode, readahead, ad-hoc governor, and uncertified routing prototypes
were removed after measurement. Physical-I/O telemetry remains. The runner
records retained experiment settings in `run-config.json` and exposes the
stable-tip posting-batch A/B; the rejected payload-eviction flag was removed
with its implementation.

Fresh 50K ingestion exposed two independent lifecycle problems. A background
timer repaired only one bounded dependency page per wake, and the stable-tip
finalizer required posting sequence parity before the same finalizer could
advance postings. The first attempted cooperative timer drain removed the
backlog but could monopolize the HBC apply fence and rebuild payloads from the
primary LSM before exact-vector publication. The production ordering under
qualification is therefore:

1. ordinary timer maintenance performs one bounded page;
2. a stable source barrier may publish a cardinality-certified exact-vector
   generation ahead of the last flattened posting generation;
3. that leading vector generation remains non-ready to queries but serves as
   the mmap repair source for the completeness-bounded posting drain;
4. posting `CURRENT` advances to the same sequence, after which the existing
   sequence/precision/scope/count checks may publish readiness.

The r79 diagnostic inserted 50K in 16.7465 s and encoded the final vector base
in 3.228 s, but the old order did not become ready until 322.8948 s and repaired
42 latent posting payloads first. It preserved 0.9828 recall and peaked at
1.045 GB RSS. r80 proved that vector-first alone is insufficient when an
ordinary all-pages timer drain wins the apply fence; that run was stopped and
is not a performance result.

The corrected r81 bounded-timer/vector-first qualification restored the target
50K lifecycle: 15.8543 s insert + 23.8986 s publication = 39.7529 s total.
Recall was 0.9819; concurrency-one throughput was 109.36 QPS with 9.04 ms mean,
12.54 ms p95, and 15.32 ms p99 latency. Cold/warm restart p95 was 18.5/16.6 ms.
Live RSS peaked at 877.6 MB and the process physical-footprint ledger at 564.6
MB; restart RSS and physical footprint were 216.3/90.6 MB. The final posting
validation repaired 17 dependency steps from the newly published mmap vector
generation and produced sequence-equal posting/vector authority.

Raising the ordinary maintenance page from 64 to 256 did not batch those
dependencies: every timer pass still reported one repaired step, and r82 lost
the publication race and remained unready for minutes. The run was stopped and
is not a performance result. Larger nominal pages are therefore not the
solution; the accepted stable-tip path drains the complete dependency chain at
one proven quiet boundary, while ordinary timers stay bounded.

The r83 payload-only `MADV_DONTNEED` A/B completed in 17.3425 + 23.3886 =
40.7311 s. It lowered sampled live/restart RSS from 877.6/216.3 MB to
829.9/169.1 MB, but live physical footprint was unchanged at 565.9 MB and
latency regressed: concurrency-one p95 rose from 12.54 to 17.51 ms and warm
restart p95 from 16.6 to 19.1 ms. This moves clean file pages out of the process
working set only to fault them back during queries; the prototype and flag were
removed rather than making cache displacement a production memory policy.

The fresh r84 1M public-API diagnostic inserted 1,000,000 vectors in 611.17 s
(1,636 rows/s) with a sampled RSS high-water of approximately 1.95 GiB. It did
not publish readiness: after the final source sequence, posting maintenance
continued at one dependency step per one-second timer turn and no preferred
vector base appeared. The run was stopped after more than four additional
minutes. This is useful ingestion/RSS evidence, but it has no valid query or
total-load result and must not be compared with the qualified r69 lifecycle.

r85 verified that merely retrying lifecycle finalization from the recurring
vector-maintenance lane does not close that gap. It inserted 50K in 16.5682 s
but spent 308.2775 s waiting for optimization (324.8457 s total). Once the
framework continued, recall was 0.9835 and concurrency-one throughput/mean/p95
were 104.65 QPS, 9.37 ms, and 12.38 ms, so serving correctness survived; the
publication latency is a decisive regression. Live RSS peaked at 1.064 GB,
with a 561 MB physical-footprint ledger high-water; restart RSS/footprint were
218.1/86.3 MB.

r86 and r87 isolated the durable shape of the stall. Their 50K insertion phases
finished in 18.00 and 15.61 s, respectively, then remained at public progress
0.999 for minutes and were stopped. The native exact-vector store already held
all 50K vectors in a one-shard float16-plus-lossless-residual delta, but its
coverage watermark was source sequence 126 while the public source/posting tip
was 501. Public coverage diagnostics simultaneously reported 50,000 produced
outcomes against 50,001 source records. Removing the write-cache bulk-session
skip did not help and was reverted; the independent dense-session/finalization
gate still correctly prevented maintenance from racing active projection work.

A bounded native topology-changing compactor was implemented and unit-tested:
it deduplicates one source shard at a time, fans exact encoded records through
fixed-size per-destination spools, builds one destination shard at a time, and
atomically publishes `CURRENT`. Restart, all nine test vectors, quantization
metadata, and lossless residuals survived a one-to-four-shard rewrite without
consulting the primary LSM. The public lifecycle has not yet invoked this path
in a completed 50K/1M run, so it is a correctness-tested prototype, not a
measured performance win. Qualification requires first making the stable-tip
coverage handoff reach the compactor without relaxing source-sequence or
cardinality readiness.

## Posting-local scan and fresh-publication follow-up (r88-r94)

The r88 minimal public-API 50K qualification remains the performance baseline
for this follow-up: 17.2530 s insert, 23.2939 s ready total, 0.9838 recall,
112.82 concurrency-one QPS, 8.76 ms mean and 12.05 ms p95 latency. Peak live
RSS was 975.4 MB, restart RSS was 232 MB, and durable data occupied 617 MB.
Any candidate-layout change must preserve those query semantics and recover
that lifecycle rather than comparing only an already-published generation.

Fresh r89-r94 attempts did not qualify. They inserted all 50K vectors but
remained at public progress 0.999 with dense publication pending. The attempted
background posting burst initially appeared to repair 17-23 steps per wake;
tri-state maintenance accounting proved those were debounce/write-plane
pending returns, not durable progress. Later attempts also regressed ingestion
to roughly two minutes and accumulated about 518 MB of mutable snapshot copies,
so none is a load-time result. Generic maintenance/status lanes may run during
the projection finalizer because its own reservation and commit fences preserve
publication ownership; an optimistic `dense_projection_finalizing` bit is not
itself active bulk work.

Reopening r94 exposed a separate native topology-compaction readiness defect.
The topology-changing compactor published posting/vector coverage at source
sequence 501 but failed its vector-count readiness certificate with
`VectorBlockPublishedGenerationNotReady`. Startup correctly fell back to an
authoritative primary snapshot and published 50K float16-plus-lossless-residual
vectors in 19.202 s. This is valid recovery behavior, but the extra primary scan
is publication debt and must not be counted as the intended native compactor's
load time.

The reopened generation also found and fixed an exact-batch counter overflow:
adding the residual-present `u1` flag directly to integer literal 1 overflowed
on every float16 exact read. The counter now widens before addition, and the
regression test exercises projection plus residual batch reads and exact value
reconstruction.

The first 1,000-query diagnostic after that fix measured 0.98312 recall,
23,809.8 approximate vectors, 207.9 leaves, and 135.6 authoritative exact
completions per query. Client mean/p50/p95 were 55.80/45.21/59.99 ms; server
mean/p50/p95 were 37.82/36.74/50.19 ms. Leaf scoring averaged 11.33 ms and
artifact reads 11.93 ms. The two-stage exact path still issued 304.6 compact
projection reads plus 135.6 residual reads (440.2 physical reads and 1.278 MB
per query). The narrow exact completion set is correct; the remaining fan-out
comes from the centralized hash-sharded float16 refinement plane, not from
unnecessarily exact-scoring the whole ANN shell.

An explicit `flat_rabitq` routing qualification on the same durable generation
is rejected. It returned only 0.9577 recall. The binary was subsequently found
to be Debug, so its cold/warm p95 and concurrency throughput are not comparable
performance data and must not be cited as a ReleaseFast regression. The recall
failure remains valid because the query set, truth set, and durable generation
were unchanged. Posting-local immutable membership/RaBitQ views are useful,
but the flat router does not meet the one-percentage-point recall requirement
and must not become the default.

The posting-local view is therefore also used by the established tree route.
It borrows co-located member IDs and RaBitQ bytes under the query's immutable
generation lease, and any WAL/delta shadow forces the prior authoritative path.
The common unfiltered heap-admission loop rejects noncompetitive groups with
Zig `@Vector(8, f32)` comparisons while replaying every possibly competitive
group through the existing scalar tie/interval logic. These changes still need
a fresh normal-route r95 qualification before they can be called a performance
win.

Certified flat stopping now has an implementation-level experiment as well:
for cosine/L2, selection retains the complete already-scored compact directory,
builds conservative suffix minima from persisted posting radii, and stops only
when the unseen suffix lower bound is strictly beyond the kth-smallest retained
upper endpoint. Unresolved/dirty bounds fall back to the existing effort path;
inner product has no metric-ball proof. This must be measured with reduced
initial probe waves and removed if it cannot restore recall parity.

## Posting-local ReleaseFast and native-capture follow-up (r96-r99)

r96 re-ran the unchanged r94 tree generation with a ReleaseFast binary. The
posting-local member/RaBitQ view scored 23,809.8 vectors across 207.9 leaves and
preserved 0.98312 recall. Client mean/p50/p95/p99 were
10.46/9.18/13.56/18.99 ms; server mean was 7.94 ms, including 1.11 ms leaf
scoring and 2.87 ms artifact reads. It still issued 304.6 float16 projection
reads and 135.6 lossless-residual reads per query. Official QPS at concurrency
1/10/20/30 was 77.56/131.08/112.47/95.43. This establishes that the
posting-local RaBitQ/SIMD path is sound; centralized refinement I/O, not leaf
scoring, is the remaining single-query bottleneck.

Fresh r97 stalled at public progress 0.999. r98 eventually qualified only
after 306.9 seconds of optimization: an empty one-shard exact generation
accepted the first vector window, but later capture eligibility incorrectly
required the preferred serving layout. The resulting sparse overlay was exact
but not preferred, so capture stopped, coverage stalled, and stable-tip repair
rescanned the primary artifact plane. Mutation admission now tests exact
transaction authority (precision, sequence, and scope), independently from
serving-layout readiness. A regression test covers an exact sparse generation
that remains capture-eligible before serving compaction.

r99 validates that lifecycle fix. It inserted 50K through the public API in
24.2855 s and compacted the native vector generations without a non-empty
primary snapshot scan in 8.1097 s, for 32.3952 s total. This recovers the
30--40-second load target. CPU contention makes the insert phase noisier than
r88, but the publication mechanism itself is now native and bounded.

The first r99 query attempted progressive float16 refinement in 64-candidate
chunks. It reduced projection reads from 304.6 to 177.3 and total physical
reads from 440.2 to 303.5; detailed client mean/p95 fell to 8.02/9.58 ms.
However recall fell from the accepted 0.9838 baseline to 0.9504. Raising effort
from 0.5 to 0.6 scored 26,212.6 vectors and achieved only 0.95217 recall.
Exhaustive routing scored all 50K vectors and still reached only 0.95985, which
rules out leaf routing as the cause. This optimization is rejected.

A same-generation A/B that refines the complete RaBitQ-selected shell before
applying the float16 boundary restored recall to 0.98451. It scored the same
24,366.9 approximate vectors/208 leaves, read 303.8 float16 projections, and
completed only 135.8 authoritative residuals. Client mean/p50/p95/p99 were
12.12/10.14/16.63/27.55 ms and server mean was 9.34 ms. Official QPS at
concurrency 1/10/20/30 was 83.07/134.70/115.25/117.46, with p95
18.01/103.31/256.38/401.44 ms. Compared with r96, posting-local scanning keeps
low/mid-concurrency throughput comparable and improves concurrency-30 QPS,
but randomized centralized float16 reads still cap single-query latency.

The correctness rule is now explicit: RaBitQ intervals select the complete
refinement shell; only after every selected candidate has a float16 interval
may the overlap boundary decide which candidates need lossless residual
completion. RaBitQ's stochastic interval must not be used as a certificate for
skipping unread float16 candidates. The next layout experiment should place
the float16 scan plane beside immutable leaf membership/RaBitQ bytes (or make
that the sole float16 authority) while retaining generation-leased centralized
residual completion. That changes hundreds of random projection reads into a
few sequential leaf reads without exact-scoring the whole shell.

A bounded 32 MiB cross-query float16 projection cache was also rejected on the
same r99 generation. It preserved 0.98451 recall and reduced total physical
reads from 439.64 to 347.51/query (about 303.85 to 211.71 projection reads once
the unchanged 135.80 residual reads are removed), but client mean/p95 regressed
from 12.12/16.63 ms to 13.78/18.66 ms, artifact time rose from 3.99 to 6.54 ms,
and RSS rose from 232.05 to 328.45 MB. Per-vector hash lookup, locking, copying,
allocation, and FIFO churn cost more than the saved warm reads. The prototype
was removed. Any future hot cache should admit immutable leaf scan pages as a
unit under the resource manager, after the durable posting-local layout exists;
it should not cache individually addressed projections.

## Posting-local float16 scan plane and certified routing (r100-r103)

r100 publishes a version-three immutable quantized directory with one
contiguous float16 candidate matrix and its conservative error/norm metadata
beside each leaf's membership and RaBitQ rows. Queries borrow the complete row
under the posting-generation lease; any membership, posting-state, or
quantized-payload shadow falls back to the authoritative overlay path. The hot
loop scores the float16 matrix with Zig `@Vector(8, f16/f32)` kernels and marks
those candidates as already refined. The public top-k boundary still controls
lossless float32 completion, so this is a layout change rather than a weaker
score contract.

The fresh public-API r100 qualification inserted in 28.2927 s and reached
ready in 34.3334 s. Recall/NDCG were 0.9867/0.9883. QPS at concurrency
1/10/20/30 was 123.39/324.52/263.03/248.66, versus
83.07/134.70/115.25/117.46 for r99. Serial p95 was 9.06 ms. The detailed
profile scored 23,828.7 approximate vectors across 208.0 leaves, completed
137.5 exact vectors, and measured 8.67 ms client mean, 6.43 ms server mean,
3.87 ms leaf scoring, and 1.23 ms artifact reads. Physical vector reads fell
from 439.64 to 275.01/query because the candidate shell no longer performs
centralized projection reads.

The remaining 275 reads are exactly two reads per 137.5 completed vectors: the
central exact-vector path rereads a float16 projection before its lossless
residual. The posting-local projection must either be passed into residual
completion, or become the sole generation-bound projection authority while a
central residual-only store remains keyed by vector identity. The latter also
removes the current duplicate float16 payload. r100's posting segment grew by
about 77 MB and total durable data was 761 MB; live RSS sampled 2.252 GB while
restart RSS was 422 MB. The query-speed result is accepted, but that duplicate
disk/RSS end state is not.

r101 initially attempted to reconstruct missing cosine leaf radii from the
same bounded projection rows. It incorrectly required an arithmetic-mean leaf
centroid to have unit norm. That condition safely fell back but produced zero
stops. r102 corrected the certificate to compare normalized projected members
with the normalized centroid and conservatively add the float16 normalization
perturbation. It reached ready in 28.9739 s, preserved 0.9868 recall, and
delivered 125.53/318.21/270.11/252.84 QPS. The detailed profile was likewise
unchanged: 23,984.8 vectors, 207.9 leaves, 137.4 exact completions, 8.64 ms
client mean, and zero certified stops.

Direct format inspection showed that r102 did publish finite certified radii
for all 441 immutable leaves. r103 then exposed an experiment-control error:
the default `auto` mode stays on the recursive tree below 1,024 completed
postings, so altering the persisted flat directory could not affect its 208
tree leaves. It remained at 0.9865 recall and zero stops and is a no-op routing
result, not evidence for exact or quantized flat routing.

r104 explicitly selected `flat_exact` with a 16-leaf initial wave. It reached
only 0.9731 recall and 103.52/213.44/85.78/72.33 QPS at concurrency
1/10/20/30. The detailed profile scored 20,985.0 vectors across 179.7 leaves,
with 15.05 ms client mean and 23.06 ms p95. Most importantly, all routing
bounds resolved but produced **zero certified stops**. The smaller shell and
recall loss came from the ordinary flat-route candidate limit, not from proof
pruning. Exact routing and reconstructed radii were removed: the current leaf
spheres overlap too much for radius-only stopping, so persisting a new
quantized encoding of the same directory would not create a query win. The
next routing experiment must improve partition balance/separation or use a
different certified summary, then compare at recall parity.

r105 passed each posting-local bounded projection through the generation lease
to exact completion, so an ambiguous candidate reads only its centralized
lossless residual. Binding validates vector generation/location, dimensions,
encoding, scale, error bound, decoded-norm bound, and the source projection
checksum. A mismatch falls through to the complete authoritative vector read;
it never turns a stale posting row into an exact score. This cut physical
vector reads from 275.01 to 137.42/query and bytes from about 769 KB to 347 KB,
but recomputing CRC32 over every borrowed 3 KiB projection made the warm 50K
profile slower (9.46 ms mean versus r100's 8.67 ms).

r106 therefore publishes the source projection checksum as a fourth metadata
column in quantized-directory V4. The directory reader authenticates the
complete immutable leaf entry once, after which exact binding compares the
persisted checksum without rehashing the row. V1-V3 remain readable; V3 lacks
that certificate and safely uses the complete-read fallback. The fresh public
API qualification inserted 50K rows in 18.1686 s and reached ready in 28.2068
s. Recall was 0.9863. QPS at concurrency 1/10/20/30 was
117.24/281.66/241.98/232.62; detailed client mean/p50/p95/p99 was
8.79/8.17/9.58/11.52 ms, server mean/p95 was 6.68/7.81 ms, leaf scoring was
4.13 ms, and artifact read time was 1.13 ms. It scored 24,174.6 vectors over
207.9 leaves and exactly completed 137.47 vectors with 137.47 physical reads
and 346,921 bytes/query. Live RSS peaked at 2.131 GB and restart RSS at 432 MB;
durable run-root size was 774 MB.

The residual-only authority is retained for a 1M qualification: r106 halves
random exact-completion I/O and preserves exactness, but normal warm 50K
variance does not establish a latency win over r100. The durable end state must
also remove the centralized duplicate float16 projection rather than merely
avoid reading it. That requires a generation manifest which owns posting-local
projection blocks plus centralized residual-only blocks as one atomic exact
artifact, with compaction/recovery retaining their shared vector identity and
checksum contract.

## Posting-local 768D 1M qualification (r107)

r107 used the exact Circus workload, `Performance768D1M` (Cohere 1M, 768
dimensions), through the public table API with batch 100, four client workers,
the default full-text index removed through the public contract, a 2 GiB
process envelope, and query concurrency 1/10/20/30. The first attempted case
name, `Performance1536D1M`, was rejected before load because VectorDBBench does
not define it; it is not a benchmark result. Dataset download preceded the
timed insert but occurred after the host wired baseline, so only process RSS is
usable from this first cached lifecycle.

The official insert completed in 814.682 s (13.58 minutes) and stable-tip
optimization took 62.505 s, for 877.187 s ready time (14.62 minutes). This
recovers the expected approximately 13-minute insertion time and disproves the
reported 3,000-second result for this implementation/configuration. Native
posting WAL/checkpoint recovery remained correct through sequence 10,001. It
periodically compacted bounded deltas and ultimately published a 1.840 GB full
posting segment; the exact-vector generation compacted all 1M vectors before
readiness.

Memory and disk do not yet qualify. The primary document/embedding LSM crossed
hard pressure near 644K and a pressure compaction raised sampled RSS above
3.5 GB. Stable-tip exact-vector plus posting publication ultimately produced a
5.479 GB RSS peak and a 4.800 GB `phys_footprint` ledger peak despite the 2 GiB
configured envelope. Final restart RSS peaked at 2.248 GB and the durable data
root was 7.7 GB. Source LSM mutable snapshot copying reached about 2.15 GB
cumulative; the final 1.7 GB posting segment also duplicates the float16 scan
plane still present in the central exact-vector generation. The next memory
fix must coordinate streaming builder buffers, source compaction, and page
cache residency; the next disk format must make posting-local projection plus
central residual the sole exact artifact rather than two projection copies.

Serial quality was strong at 0.9935 recall/0.9942 NDCG. The detailed public
profile scored 240,783.8 approximate vectors across exactly 2,048 leaves and
completed only 147.06 exact vectors. Residual-only completion worked as
designed: 147.06 physical reads and 186,920 bytes/query, with zero projection
reads. Client mean/p50/p95/p99 was 44.49/30.31/54.55/496.78 ms; server mean
was 32.14 ms, of which leaf scoring was 22.04 ms and artifact reads 5.13 ms.
Thus refinement I/O is no longer the dominant 1M cost; the 240K candidate shell
is.

The published concurrency numbers from this lifecycle are **invalid**. At
concurrency 20 and 30, ordinary queries exhausted the dense-search scratch
slice and the public endpoint emitted HTTP 500 `ResourceBudgetExceeded`
(45,165 server log occurrences, including wrapper lines). The framework still
reported 26.80/108.01/120.45/187.36 QPS because its upstream result does not
fail the run on these request errors. Root cause is the rejected certified
routing experiment: cosine selected all 8,878 posting probes per query to make
a suffix proof available, yet the profile recorded zero certified stops. Each
query retained about 3.35 MB of scratch and the roughly 90 MB dense-search
slice failed before public admission's nominal 32-request capacity.

Approximate requests now retain only `search_width + bounded filter slack`
posting probes; only explicit complete-snapshot validation allocates the full
directory. This removes the failed proof experiment's O(postings) per-query
scratch and restores the normal bounded ANN effort contract. A resumed
concurrency/profile run against the same immutable r107 generation is required
before claiming query latency or QPS. More generally, resource-derived query
capacity should eventually participate in public admission so a future
index-sized workspace cannot leak an internal 500 even when its estimator or
configuration is wrong.

### Bounded 1M routing and recall knee (r108)

The first resumed A/B retained only 2,064 of the 8,878 flat posting probes,
but still failed qualification. It reduced a scratch handle from roughly
3.35 MB to 561 KB before flat block scoring, then accidentally called the
general vector-fetch capacity helper for centroid output. That helper also
allocated a `dimensions * block_size` float32 vector batch plus lookup and
rerank columns which routing never reads. At concurrency 30, roughly 2.85 MB
of needless growth per request again filled the 90 MB dense-search slice. Its
apparent 225.1 peak QPS is rejected because the server logged 21,952
`ResourceBudgetExceeded` occurrences.

r108 splits flat centroid scoring into two scalar output planes and accounts
their growth before allocation; it no longer couples routing to vector decode
workspace. The second resumed default-effort curve had zero public failures
and delivered 18.40/140.23/136.83/127.11 QPS at concurrency 1/10/20/30. The
first point faults a cold 1.7 GB posting generation and is not a warm-latency
claim. At concurrency 30, attributable demand was 574 MB and cache-inclusive
RSS was 2.83 GB. A separate 1,000-query profile preserved 0.9935 recall while
scoring 240,783.8 vectors across 2,048 leaves; client p50/p95 was
25.56/27.03 ms, server mean was 27.58 ms, leaf scoring was 20.78 ms, and
artifact reads were 0.97 ms. Exact refinement is no longer material to the
default query cost.

The same immutable generation then isolated the public effort curve:

| effort | recall | leaves/query | approximate vectors/query | client p50 | client p95 |
|---:|---:|---:|---:|---:|---:|
| 0.30 | 0.83072 | 168 | 19,917.0 | 5.73 ms | 19.33 ms |
| 0.35 | 0.89888 | 315 | 37,276.5 | 7.09 ms | 9.10 ms |
| 0.40 | 0.94890 | 588 | 69,482.7 | 9.76 ms | 11.28 ms |
| 0.41 | 0.95642 | 666 | 78,677.0 | 10.57 ms | 11.82 ms |
| 0.50 | 0.99350 | 2,048 | 240,783.8 | 25.56 ms | 27.03 ms |

Effort 0.41 is the smallest measured point above the Circus 0.955 recall
floor. Its error-free concurrency curve was 63.29/179.28/147.46/142.23 QPS.
This improves default work by 67%, but remains below the current 768d/1M
competitor targets (238.7 QPS/6.6 ms p95 for Chroma and materially faster
graph engines). The current flat partition therefore cannot reach the desired
20--35K candidate shell at recall parity; merely changing the default effort
would trade away quality.

For comparison, native recursive topology at effort 0.44 reached only 0.9502
recall while scoring 112,709.6 vectors across 959.2 leaves, with 13.86 ms p50
and 15.18 ms p95. It is dominated by flat routing and remains rejected. The
next retained experiment uses the already co-located RaBitQ leaf codes as a
first-stage candidate heap, completes only admitted candidates from the
posting-local float16 plane, and retains the existing lossless-residual
completion solely for intervals which can cross the public top-k boundary.
This attacks per-leaf arithmetic without changing generation ownership or
authoritative score semantics; a better multi-representative partition or
directory is still required to reduce routed leaves themselves.

The two-stage implementation qualified on both public workloads. At 1M and
effort 0.41 it retained exactly 800 RaBitQ-admitted candidates/query for
float16 completion, preserved 0.95545 recall, and reduced leaf scoring from
6.74 to 2.06 ms. Client p50/p95 improved from 10.57/11.82 to
8.65/10.59 ms. Its error-free concurrency curve was
93.91/174.95/147.72/147.92 QPS: concurrency-1 improved materially, while the
throughput plateau moved to residual I/O and remaining routed-code work. The
run exactly completed 145.72 vectors/query, so RaBitQ admission did not widen
the authoritative boundary.

The existing 1536d/50K V4 generation retained 0.98622 recall. Its public
profile improved from 8.79/8.17/9.58 ms mean/p50/p95 to
7.23/5.72/8.48 ms, with leaf scoring falling from 4.13 to 1.18 ms. The valid
concurrency curve improved from 117.24/281.66/241.98/232.62 to
174.59/331.47/256.30/239.01 QPS. This is a general native-leaf optimization,
not a 1M-specific effort shortcut. High-concurrency scaling remains poor:
the next format experiment should co-locate the lossless residual plane with
the posting-local projection so boundary completion avoids roughly 140 sparse
central reads/query and the duplicate central float16 generation can be
removed.

A narrower exact-residual mmap experiment confirms why the next step must be
a format/layout change rather than another read-policy toggle. It retained the
posting-local candidate plane and mapped only the lossless central residual
rows admitted by the exact boundary. At 50K, physical residual reads fell from
137.43/query to zero and profile p95 improved from 8.48 to 7.73 ms, but QPS
changed from 174.59/331.47/256.30/239.01 to
144.52/264.18/275.43/224.75 at concurrency 1/10/20/30. The mixed curve is not
a throughput win.

At 1M effort 0.41, physical reads likewise fell from 145.72/query to zero,
with identical 0.95545 recall and candidate/exact counts. Warm profile p50/p95
improved from 8.65/10.59 to 6.23/8.92 ms, and concurrency-1 rose from 93.91 to
110.83 QPS. However, concurrency 10/20/30 regressed from
174.95/147.72/147.92 to 133.45/135.61/131.14 QPS, while peak RSS rose from
2.44 GB to 3.40 GB as random queries retained pages from the approximately
1.25 GB residual arena. The implementation and flag are removed. Bounded
positional reads remain the production policy; reducing this cost requires a
smaller residual-only central format and/or locality-preserving residual
packing, not unrestricted mmap touches.

A lazy four-representative `flat_rabitq` directory was also tested before any
durable-format change. Each leaf contributed its centroid plus three
deterministic posting-local member rows, and query selection deduplicated by
leaf after scoring the enlarged directory. At effort 0.30 it still searched
168 leaves/20,564 vectors, but recall collapsed from 0.8307 to 0.02151 and
routing alone cost 9.32 ms. Taking the minimum distance across raw member
samples creates an extreme-value bias toward diverse leaves with one
accidentally close representative. The prototype is removed. Any future
multi-representative directory must use learned subcentroids with balanced
assignment—or rebuild better-separated leaves—and must prove recall at fixed
candidate work before receiving a persisted format version.

### Global-clustering topology oracle (r109)

The 50K corpus was rebuilt on an APFS clone with the existing one-shot
`global_kmeans` topology builder. The rebuild consumed 322.7 MB of workspace,
retired 547 online nodes, created 488 nodes, and took 12.55 seconds. This is a
quality oracle rather than a production 1M builder: the current implementation
materializes the complete transformed float32 corpus and would require more
than 3 GB at 768d/1M, outside the 2 GiB service envelope.

At effort 0.30, global clustering searched approximately the same shell as the
online topology (42 leaves and 5,309 versus 4,910 vectors/query), but recall
improved from 0.80787 to 0.86921. The broader sweep was:

| effort | recall | leaves/query | approximate vectors/query | client p50 | client p95 |
|---:|---:|---:|---:|---:|---:|
| 0.30 | 0.86921 | 42 | 5,308.7 | 4.19 ms | 5.19 ms |
| 0.40 | 0.94233 | 94 | 11,861.3 | 4.61 ms | 5.68 ms |
| 0.45 | 0.96856 | 140 | 17,610.9 | 4.79 ms | 5.91 ms |
| 0.47 | 0.97667 | 164 | 20,598.7 | 4.98 ms | 6.01 ms |
| 0.50 | 0.98663 | 207 | 26,010.2 | 5.16 ms | 6.15 ms |

Effort 0.47 is within 0.955 percentage points of the accepted online-tree
default recall while scoring 14.8% fewer approximate vectors and reducing p95
by 29.2%. Its error-free concurrency curve was
187.80/301.44/238.45/222.16 QPS at concurrency 1/10/20/30. This improved
serial QPS by 7.6%, but regressed concurrency 10--30 by 6.9--9.1%; peak RSS was
approximately 1.00 GB with 410 MB attributable demand. The experiment proves
that partition quality can materially improve recall per routed leaf, but it
does not by itself solve shared-resource scaling.

The rebuild also exposed a lifecycle gap. A restarted, already-queryable index
did not schedule optional topology debt for more than 60 seconds, and an empty
public `sync_level=full_index` barrier only waited for current readiness. A
subsequent real source mutation caused idle maintenance to perform the rebuild.
Topology acceleration must therefore become explicit, non-blocking maintenance
debt: absence of the preferred topology must not make an otherwise valid index
unqueryable, while the background scheduler and an explicit acceleration wait
must be able to observe and drain it. The production builder must be bounded
and streaming (for example, a sampled hierarchical trainer followed by
batched assignment and atomic generation publication), not the whole-corpus
oracle used here.

The existing full-dimensional Hilbert-seeded bulk builder was also exposed as
a temporary topology-rebuild oracle and tested on a separate 50K clone. It
retired 547 online nodes, created 301 nodes, consumed 626.65 MB of explicitly
accounted workspace, and took 16.56 seconds. The 2 GiB resource governor
correctly denied that oversized repair reservation, so the oracle was built
under a temporary 4 GiB envelope and served afterward under the normal 2 GiB
envelope.

Its routing quality was decisively worse. At effort 0.30 it searched 42 leaves
and 7,047 vectors/query but reached only 0.36619 recall; at effort 0.50 it
searched 208 leaves and 34,899 vectors/query yet reached only 0.91518 recall.
The online/global-k-means comparisons at similar work are respectively
0.80787/0.86921 recall near 42 leaves and 0.98622/0.98663 near 208 leaves.
The experimental rebuild mode is removed. A full-dimensional space-filling
curve is not a viable high-dimensional partition here, and its O(N*D) keys
would also be an unsuitable external-sort format. Bounded production work
should focus on sampled hierarchical clustering with streamed assignment.

r99's live RSS sampler peaked at 1.9705 GB during the ingestion/publication
phase, while restart query RSS was 215.6 MB and the final durable footprint was
613 MB (273 MB exact vector blocks and 23 MB posting segments). The source
capture currently owns coalesced float32 vectors, then vector-WAL encoding and
WAL replay transiently materialize additional corpus-sized representations at
stable tip. A production memory fix should stream uncommitted exact-vector
records to a capture-scoped WAL/spool and publish its commit frame only after
the source transaction commits; it must not trade correctness for borrowed
request-buffer lifetimes or re-read the primary LSM.

### Bounded hierarchy and concurrent-cache follow-up (r113)

The bounded hierarchical-k-means builder now propagates exact leaf budgets
through the hierarchy and uses capacity-constrained assignment. On the 50K
corpus it completed in 39.33 seconds with 321.1 MB of accounted workspace,
retiring 547 nodes and creating 429. It did not beat the global-clustering
oracle: at effort 0.47 it reached 0.97275 recall while searching 164 leaves and
27,527 vectors/query, with 7.61 ms p95. The public concurrency curve was
154.60/273.92/211.90/192.02 QPS at concurrency 1/10/20/30. The builder is a
bounded production candidate, but this particular hierarchy is rejected as
the serving default until its partition quality matches the global oracle.

This run also exposed a maintenance-state bug. The topology maintenance helper
returned one boolean for both "nothing needed" and "work deferred", causing an
already-completed topology rebuild to remain scheduled and run again after
unrelated source mutations. Its result is now explicit: `not_needed`,
`completed`, or `deferred`; only deferred work retains the candidate. A runtime
reopen plus non-vector mutation confirmed that completed topology debt no
longer reappears.

Two flat-routing experiments are rejected. Exact flat routing did not improve
the qualified latency/recall tradeoff. Adaptive flat routing initially appeared
to reduce work, but that result exposed a correctness bug: traversal stopped
when the bounded candidate heap became full even though heap capacity is not a
pruning proof. Removing that early exit and adding a deterministic regression
restored recall, but no certified stops were possible with the current bounded
frontier and latency regressed. Future routing reductions must use persisted,
conservative leaf bounds rather than a candidate-count heuristic.

A valid concurrency-30 process sample then showed that the runtime admitted
many queries; the bottleneck was not a small HTTP worker limit. The dominant
search-side samples included 1,902 cache clock insertions, 1,328 shared apply
locks, 1,584 RaBitQ scoring samples, and roughly 600 residual reads. Rerank's
vector-to-document metadata API returned transaction-owned views, so its
"cached" helper still read every value and then cloned it into a retained cache
which that API could never borrow. Keeping those one-shot values in the search
transaction reduced clock samples to 126 and shared-lock samples to 502. A
same-generation, host-noisy A/B improved concurrency-10/30 from 273.92/192.02
to 297.13/270.48 QPS; concurrency 20 was a host-load outlier and is not used as
evidence.

The next A/B skipped the adapter-level decoded-vector cache whenever a complete,
sequence-matched native projection generation was already leased. Native
projection reads deliberately do not populate that heap cache, making the
probe pure shared-lock/hash overhead. On the same immutable generation the
public curve reached 233.69/680.96/457.67/519.77 QPS with p95
5.31/28.39/67.35/93.46 ms. This machine was under changing concurrent compiler
load, so the absolute curve remains provisional, but the process sample is
causal: cache-clock samples fell to 26 while RaBitQ and residual I/O became the
remaining search hot paths. Cache-inclusive peak RSS was 1.37 GB and
attributable demand was 600.7 MB. The manager still recorded 5.21 million
always-miss decoded-vector probes in its inner candidate batch. Removing that
second probe only while the matching native authority is present reduced the
decoded-vector cache's hits, misses, and insertions to exactly zero and lowered
shared apply-lock samples from 502 to 240. Attributable demand was 593.6 MB.
Its concurrency curve was 217.24/621.55/563.55/407.51 QPS, versus
233.69/680.96/457.67/519.77 in the immediately preceding run. The mixed delta
is treated as host/scheduler variance rather than a throughput claim.

The decisive 1,000-query public profile preserved exactly 0.97275 recall,
164 leaves/query, and 27,527.27 approximate vectors/query. It completed only
137.06 authoritative vectors/query, with zero native vector-block misses or
fallbacks and 137.06 residual reads/query. Client p50/p95 was 4.72/5.41 ms and
server p50/p95 was 3.33/3.88 ms. The two cache changes are accepted because
they remove provably useless retained-cache work, preserve recall and exact
completion semantics, reduce lock pressure and attributable demand, and
materially improve the warm single-query profile; the concurrency headline
still requires controlled-host repetition.

The accepted online V4 generation then supplied the production-path check.
At 50K, recall and work remained exactly 0.98622, 207.88 leaves, 24,174.57
approximate vectors, and 137.43 exact vectors/query. Warm client
mean/p50/p95 improved from 7.23/5.72/8.48 ms to 6.31/4.80/7.39 ms. Two
independent public concurrency lifecycles reproduced
251.73/947.44/1031.64/1012.31 and
255.56/948.41/995.33/1016.71 QPS at concurrency 1/10/20/30. The former V4
curve was 174.59/331.47/256.30/239.01 QPS. Concurrency-30 p95 fell from
193.23 ms to 69.54 and 69.24 ms. The runs had no request failures, retries,
resource-budget errors, native fallbacks, or decoded-vector cache activity.
Attributable demand was 269--282 MB; cache-inclusive RSS was 1.83--1.88 GB
because the higher query volume touched substantially more file-backed pages.

The same cache-free binary qualified at 1M using the production `auto`
centroid policy and effort 0.41. Recall and work remained exactly 0.95545,
666 leaves, 78,677.02 approximate vectors, and 145.718 exact vectors/query.
The warm profile reached 5.72 ms p50, with 1.99 ms leaf scoring and 1.08 ms
mean residual-read work; transient host stalls inflated p95 to 12.09 ms. The
public concurrency curve improved from 93.91/174.95/147.72/147.92 to
166.64/888.40/977.06/913.65 QPS, with p95
6.50/18.46/60.69/87.75 ms. Attributable demand was 692 MB and
cache-inclusive RSS was 2.85 GB. Forcing the older HBC tree directory at the
same nominal effort produced only 0.91383 recall and is rejected; benchmark
comparisons must retain the production `auto` routing contract.

These results move 1M throughput near pgvector and well above Chroma on the
current Circus snapshot, but remain behind Elastic, Milvus, and Weaviate. The
remaining warm server path is approximately 2.0 ms posting-local candidate
scoring plus 1.1 ms sparse lossless-residual completion. The next format
experiment should persist a compact, generation-bound residual locator per
vector rather than duplicate residual payloads in each HBC index. A locator
plane removes vector-to-document metadata and hash-directory lookup while
retaining one shared authoritative residual copy; it must fail closed and fall
back whenever the vector-block generation identity does not match.

### Generation-bound residual locator plane (V5, r114)

The posting-local projection format now has an optional V5 residual-locator
plane. Six explicit columns bind each float16 candidate row to the shared exact
vector generation: reader generation, shard, revision, residual offset,
residual length, and residual checksum. The 36-byte logical locator avoids
duplicating roughly 1.25 GB of residual payload at 1M while removing exact
completion's vector-to-document metadata read, artifact-key construction, and
hash-directory lookup. V1--V4 generations remain readable. A V5 hint is never
authority by itself: exact completion leases the sequence-matched vector
generation, validates its generation/shard and projection metadata, and
validates the residual checksum. Any unavailable or invalid hint leaves the
score unresolved for the established authoritative fallback.

The first fresh r114 run exposed a query-lifecycle bug rather than a format
bug. All 436 physical leaf entries had V5 flags and complete locators, but
location reuse was zero. Posting-local float16 rows completed the entire
bounded pass without invoking the external projection loader, so the query had
not yet pinned an exact-vector generation when authoritative completion began.
The located callback returned immediately and the old metadata path handled all
137.479 vectors/query. Exact completion now acquires one validated CURRENT
lease per batch when the bounded pass did not already pin one. A deterministic
test clears the pre-pinned session generation and verifies exact float32
reconstruction through this production path; stale generation hints are
rejected.

The same-corpus public profile after that correction preserved exactly 0.98673
recall, 207.955 leaves, 24,329.306 approximate vectors, and 137.479 exact
vectors/query. Metadata and external artifact loads fell from 137.479 to
exactly zero; generation-bound location reuses and residual reads were both
137.479, with zero projection rereads, block fallbacks, or score-policy
changes. The controlled before/after profile was:

| r114 50K profile | before lease fix | V5 locator path | change |
|---|---:|---:|---:|
| client mean | 6.199 ms | 5.389 ms | -13.1% |
| client p50 | 5.541 ms | 4.776 ms | -13.8% |
| client p95 | 7.214 ms | 5.968 ms | -17.3% |
| server mean | 4.159 ms | 3.515 ms | -15.5% |
| server p50 | 3.963 ms | 3.391 ms | -14.4% |
| server p95 | 5.427 ms | 4.268 ms | -21.4% |
| residual/artifact read mean | 1.394 ms | 1.149 ms | -17.6% |

The post-fix concurrency curve was 230.53/730.21/884.47/913.91 QPS at
concurrency 1/10/20/30, versus 234.69/801.05/842.74/760.63 in the immediately
preceding r114 lifecycle. Concurrency-20/30 improved while 1/10 regressed, so
the mixed curve remains scheduler-sensitive; the exact work counters and
single-query profile are the causal acceptance evidence. Restart query/profile
RSS peaked at 1.087 GB and attributable physical footprint at 472.1 MB while
the concurrency curve touched the mapped corpus. This is below the 2 GiB
envelope and should not be compared with a serial-only restart sample.

The V5 segment was 180,767,821 bytes versus 179,025,087 bytes for the accepted
V4 r106 generation: 1,742,734 bytes, or 34.85 bytes/source vector, with no
change to the 279,216 KiB shared vector-block directory. Fresh r114 load time
was 22.15 seconds of insert plus 9.06 seconds of public optimize/readiness,
31.21 seconds total. The comparable r106 lifecycle was 18.17 + 10.04 = 28.21
seconds; the 3.00-second difference is dominated by that run's slower insert,
not locator publication, but needs controlled repetition. The result remains
inside the 30--40-second 50K target and does not weaken load/RSS invariants.

### Crash-safe streaming checkpoint publication (r116/r117)

The buffered full-checkpoint builder had a corpus-sized peak that its cleanup
comment obscured. `quantized_directory.build()` first owned the complete native
candidate directory, then `posting_segment.Writer.build()` reserved the entire
final segment before copying and freeing that directory. At 1M the two
simultaneous allocations were approximately 1.88 GB each. Freeing each input
after its copy bounded retained memory but could not bound peak memory because
the complete output allocation already existed.

Full checkpoints now write directly to the storage layer's durable atomic
sink. The outer AFPS writer retains only fixed-size index entries; a nested
AFQD streaming writer retains only its compact leaf index and one leaf's
projection scratch. It patches the AFQD header inside the unpublished sibling,
finishes the AFPS index/footer, fsyncs, atomically renames the immutable
generation, and syncs the parent directory. Only after that staged receipt is
validated does the existing WAL-prefix transaction publish `CURRENT`. A fault
before the rename removes the temporary sibling; a crash after staging leaves
an unreferenced orphan; a crash after `CURRENT` sees the complete generation.
Older monolithic generations use the same bytes and remain readable.

The builder deliberately traverses immutable node metadata twice: the first
pass streams ordinary HBC values and constructs the small centroid directory;
the second streams the quantized/projection planes. This preserves contiguous
query layout without retaining a corpus-sized heap buffer. AFPS and AFQD
streaming encoders have byte-parity tests against their buffered counterparts,
including V5 residual locators. The background publication regression also
proved that a concurrently appended same-sequence WAL suffix survives the
staged full-checkpoint handoff and restart.

Fresh r116 50K qualification preserved 0.9868 recall, 208 leaves/query,
24,049.139 approximate vectors/query, and 137.416 exact completions/query.
Load was 26.50 seconds insert plus 8.55 seconds readiness, 35.05 seconds total.
The public QPS curve was 251.61/942.59/983.73/855.52 at concurrency
1/10/20/30. Detailed client p50/p95 was 4.93/6.04 ms and server p50/p95 was
3.44/4.30 ms. Relative to buffered r114, sampled RSS fell from 2.187 GB to
2.041 GB and the process physical-footprint ledger fell from 849.5 MB to
820.7 MB. Restart demand was 112.9 MB and restart RSS 356.1 MB.

Fresh r117 1M qualification completed in 710.92 seconds insert plus 70.10
seconds readiness, 781.01 seconds (13.02 minutes) total. Buffered r115b was
768.20 seconds, so bounded-memory publication cost 12.81 seconds, or 1.7%,
while producing a complete 1,876,801,174-byte V5 generation with zero WAL
tail. Default-effort recall remained 0.9923. This validates the HBC builder,
but it does **not** yet validate the end-to-end 2 GiB envelope: r117 peak RSS
was 6.059 GB and physical-footprint high-water was 4.40 GB, versus r115b's
5.685 GB and 4.70 GB.

The RSS timeline and public LSM counters identify the new dominant event. At
about 609K source rows the primary document LSM reached roughly 2.09 GB of L0,
then compacted approximately 1.88 GB into lower levels. RSS rose from about
2.1 GB to 6.06 GB during that compaction and receded before the later HBC full
checkpoint began. The final streamed 1.877 GB HBC publication raised current
RSS from about 2.81 GB to 3.86 GB through file-backed write-cache residency,
not a matching heap allocation. Restart physical demand was only 279.7 MB even
though restart RSS was 2.40 GB. Therefore the native HBC double buffer is
fixed, but production acceptance now requires bounded primary-LSM compaction
buffers plus post-fsync eviction of cold output pages; cache-inclusive RSS and
attributable demand must remain reported separately.

### Cold maintenance I/O and direct WAL resharding (r118/r119b)

Fresh r118 applied cold sequential policy to streamed HBC publication and LSM
compaction input/output without changing public durability. The 50K lifecycle
completed in 26.39 seconds insert plus 11.21 seconds readiness, 37.60 seconds
total, with 0.9857 recall. Live sampled RSS peaked at 874.3 MB versus 2.041 GB
in r116; the one-shot physical-footprint sample was 1.10 GB and attributable
demand was 1.20 GB. Restart RSS was 307 MB with a 118 MB footprint ledger.
This is the first fresh lifecycle to retain the 30--40-second load target while
removing most cache-inclusive load RSS.

The immediate cold query curve was 182.84/616.08/772.35/700.63 QPS at
concurrency 1/10/20/30, and the public profile reported client mean/p50/p95 of
7.94/6.47/11.27 ms and server mean/p50/p95 of 5.11/4.46/8.39 ms. It retained
208 leaves/query, 23,970 approximate scores, and 137.29 exact completions.
These numbers are slower than warm r116 and are intentionally recorded as a
cold-cache cost, not attributed to the scoring implementation. Future results
must report first-cold and warmed query phases separately.

Fresh r119b reached 1M inserts in 749.05 seconds but is **invalid** as a
qualification result. At source sequence 9810, derived catch-up encountered an
already active source capture and returned `PostingWalCaptureOwnershipConflict`;
the index reactivated and invalidated all later timing/readiness evidence. The
trace also showed peak RSS around 6.51 GB. A single-shard empty bootstrap
generation first checkpointed the complete vector WAL into an approximately
1.7 GB delta, then immediately resharded that delta into the intended 128-shard
base. Cold cache intent was only observed after a caller's complete
`appendSlice`, so a multi-gigabyte slice also evaded the intended residency
bound.

The durable correction is below both callers. A cold atomic sink now divides
every append at 64 MiB writeback boundaries, syncs completed prefixes, and
makes them reclaimable; immutable generation publication and primary LSM
flush/compaction output use that policy while small mutable authority files do
not. Cold compaction readers own private sequential descriptors and preserve a
descriptor slot for output plus the persistent-lock reserve, falling back to
ordinary positional readers rather than deadlocking under a low file limit.

Stable-tip vector resharding now consumes the complete committed float32 WAL
prefix directly together with immutable base/delta records. It encodes the
destination float16 scan plane and lossless residual in one pass, publishes a
complete replacement base, and atomically retains any later committed WAL
tail. The old corpus-sized bootstrap delta is never created. A deterministic
restart test covers an update, insert, and tombstone across a 1-to-4-shard
topology change and verifies exact float32 bit reconstruction, generation 2
publication, and an empty WAL. Derived catch-up explicitly borrows an active
source capture: it may record mutations against the same token but cannot
finish or cancel the owner's transaction.

The bootstrap authority itself is now V4: it records the final logical shard
count, exact-score precision, scoped zero-count certificates, and source
coverage with `segments = []`. Older manifest versions still require every
physical base shard, so a rolling downgrade rejects V4 explicitly instead of
misreading a sparse delta as a complete base. The first mutation checkpoint
therefore writes only touched blocks in the final 128-shard topology. Base
compaction understands an omitted physical base, merges sparse blocks plus a
committed WAL prefix shard-locally, publishes all 128 base members, and
preserves any later complete WAL tail. Focused tests cover empty restart,
sparse checkpoint restart, exact reads, WAL-over-sparse replacement, and final
base consolidation without creating empty shard files or a one-shard corpus
generation.

Fresh r120 validates that design end to end through the public API. The 50K
load completed in 16.77 seconds insert plus 15.36 seconds readiness, 32.14
seconds total. It published complete visibility at source sequence 501 with
0.9846 recall, within 0.22 percentage points of r116 and 0.11 points of r118.
The final vector generation has exactly 128 files totaling 285.6 MB; the HBC
posting generation is 181.1 MB. No one-shard corpus block was published.

Cache-inclusive RSS peaked at 1.558 GB during readiness versus 2.041 GB in
r116, a 24% reduction while load time improved from 35.05 to 32.14 seconds.
The post-phase physical-footprint ledger was 989.8 MB and attributable demand
was 1.080 GB. Restart RSS was 334.6 MB, its footprint ledger 106.2 MB, and
attributable demand 201.4 MB. The live numbers remain below the 2 GiB service
envelope without relying on a post-load restart.

Public QPS was 236.23/838.57/869.59/858.74 at concurrency 1/10/20/30. The
detailed restart profile reported client mean/p50/p95 of 5.80/5.10/6.62 ms and
server mean/p50/p95 of 3.72/3.56/4.73 ms, with 207.93 leaves, 23,486.73
approximate scores, and 137.47 exact completions/query. This is slightly below
r116 at concurrency 1--20 and essentially equal at concurrency 30; no query
algorithm changed in the low-memory patch, so the result is an acceptance
guard rather than evidence of a query-throughput gain.

### Durable cold generation I/O (r121--r125)

The first uninterrupted 1M run of the final-shard builder, r121, completed the
public insert in 644.58 seconds and readiness in another 70.23 seconds, 714.81
seconds total. It did not complete its query curve because the diagnostic run
was interrupted after concurrency 1 began, so it is not a query qualification.
Its live RSS evidence is valid: the process reached 5.753 GB during final
posting publication. The earlier 4.12 GB observation covered only the primary
LSM/vector overlap and was not the run peak.

r122 added cold immutable writes, private cold LSM compaction readers, and
page-by-page advisory reclamation while streaming a posting checkpoint. Its
fresh 50K result was strong: 20.13 seconds insert plus 10.22 seconds readiness,
30.35 seconds total, 0.9863 recall, and 231.98/846.86/875.95/929.56 QPS at
concurrency 1/10/20/30. Sampled peak RSS was 1.608 GB and restart RSS was 342.5
MB. The profile retained about 24,014 approximate scores and 137.46 exact
completions/query.

r123 showed why advisory mmap reclamation alone is not a durable 1M design.
The public load qualified in 665.67 seconds insert plus 68.93 seconds
readiness, 734.60 seconds (12.24 minutes) total, with 0.9926 recall. Query QPS
was 27.87/457.16/551.39/527.53 and serial p95/p99 was 12.5/14.3 ms. The detailed
warm profile reported 239,808 approximate scores and only 146.51 exact
completions/query, confirming that exact completion remained narrow.

However, RSS still peaked at 5.841 GB while flattening the 1.88 GB posting
generation; restart RSS was 2.456 GB. `madvise(DONTNEED)` reduced some already
consumed pages but did not constitute an enforceable bound over a concurrent,
generation-leased query mmap. The result is therefore a useful negative
experiment: correctness and load time qualified, but the memory mechanism did
not.

The long-term correction reads base candidate rows through a separate
maintenance descriptor instead of the serving mmap. Each quantized-directory
entry already has an indexed byte range and checksum. Full flattening now
performs an uncached positioned read of exactly one entry, authenticates it,
decodes it into one aligned leaf-sized owner, writes it to the cold atomic
output, and releases it before advancing. Foreground readers retain their mmap
and generation lease; delta/WAL-shadowed postings continue through the
authoritative resolver. Publication ordering, `covered_source_sequence`, WAL
tail preservation, float16 bounds, and lossless residual locations do not
change. This makes maintenance heap residency proportional to one leaf and no
longer depends on the operating system honoring advisory eviction of the
query mapping.

Fresh r124 validates the new path at 50K. Load completed in 19.67 seconds
insert plus 11.67 seconds readiness, 31.34 seconds total. Recall was 0.9861 and
QPS was 240.97/868.93/890.50/895.52. Live RSS peaked at 1.588 GB, the post-run
physical-footprint ledger was 907.4 MB, and restart RSS was 309.8 MB. The
detailed profile reported client mean/p50/p95 of 5.69/4.94/7.06 ms and server
mean/p50/p95 of 3.71/3.50/5.22 ms, with 24,103 approximate scores and 137.46
exact completions/query. Thus the durable reader preserves the 30--40-second
load target, recall parity, and serving performance.

Fresh r125 validates the same design at 1M. Insert completed in 692.19 seconds
and readiness in another 83.75 seconds, 775.94 seconds (12.93 minutes) total.
The complete generation covered source sequence 10001 and recall was 0.9925.
The 1.877 GB final posting generation published without increasing RSS: live
RSS peaked earlier at 3.860 GB during primary/vector consolidation and fell to
about 1.53 GB immediately after posting publication. This is 1.981 GB, or 34%,
below r123's 5.841 GB high-water. The sampled post-query physical-footprint
ledger was 2.60 GB; cold restart RSS was 2.400 GB with a 302.3 MB footprint
ledger.

The online curve immediately following deliberately cold publication was
14.13/426.46/502.27/483.43 QPS at concurrency 1/10/20/30. A separate reopened
concurrency pass distinguished first-touch I/O from steady serving and reached
46.55/478.64/591.84/561.32 QPS. Its RSS peaked at 2.860 GB and its footprint
ledger at 594.9 MB. The detailed profile reported client mean/p50/p95 of
14.51/11.23/14.24 ms and server mean/p50/p95 of 10.39/10.02/12.58 ms, with
239,507 approximate scores and only 146.60 exact completions/query. Exact
completion therefore remains appropriately narrow; remaining 1M latency is in
the approximate candidate shell and first-touch page admission, not reranking.

r125 traded 41.35 seconds (5.6%) versus r123's 734.60-second load for a real
memory bound, while remaining below the 13-minute target. Some run-to-run host
variance is visible in the insert phase itself (692.19 versus 665.67 seconds),
so publication overhead must not be inferred from the total delta alone. The
50K A/B isolates a much smaller 0.99-second total difference between r122 and
r124. Repeat runs on a controlled host remain necessary before assigning a
precise throughput cost.

### Native authority transactions and backup generations

Post-merge review exposed two lifecycle gaps outside the timed query path. A
derived replay window retired its streaming session before publishing the
source-owned posting capture, allowing stable-tip maintenance to consume the
handoff. Native-authoritative HBC indexes also still entered the legacy dense
LSM checkpoint path during backup, which correctly returned `Unsupported`
after the HBC generation had released that backend.

The long-term shape makes both boundaries explicit. Replay now retains the
opaque capture lease and the window's source sequence. Under one per-index
apply fence it retires the streaming session, validates lease ownership, and
publishes the capture at that sequence. A window that becomes mutation-empty
after lifecycle filtering still advances its source boundary with an empty WAL
transaction. Borrowers can append within an owner's transaction but still
cannot publish or cancel it; stale epochs and index incarnations continue to
fail closed.

Native backup format V4 identifies HBC authority as `hbc-native-v1`. While the
snapshot revision/mutation fence is held, it decodes each authoritative
`CURRENT` and constructs an explicit inventory: immutable posting segments and
vector blocks are hardlinked into a private pin generation, appendable WALs are
copied only through their committed byte boundary, and synthetic control files
carry the exact covered source sequence. Backup no longer walks a live HBC
directory, retains a mutable inode, or asks a released LSM for a checkpoint.
The fence cost is proportional to referenced file count plus the bounded WAL
tails, not corpus bytes.

Shared exact-vector blocks are now classified as rebuildable table-wide
acceleration rather than projection authority. A missing or corrupt shared
artifact discards the complete shared generation while preserving native
posting authority and primary-vector fallback. Projection-local corruption
continues to invalidate only that projection. This separates `queryable` from
`native_accelerated` without treating absence of an authoritative generation
as readiness.

Focused qualification covers immutable-inode replacement after pinning, exact
WAL-prefix capture, shared-acceleration corruption, concurrent writes across a
snapshot, restore of a native posting generation without an embedder, and the
deterministic derived-replay handoff. These changes do not alter candidate
routing, SIMD scoring, rerank bounds, or serving file layout, so r124/r125
remain the applicable query-path design measurements.

Fresh r126 confirms that the transaction boundary does not regress the public
50K lifecycle. Insert completed in 18.37 seconds and readiness in another
11.72 seconds, 30.09 seconds total, versus r124's 31.34 seconds. Recall remained
0.9861. QPS was 266.07/1031.50/1116.04/1064.82 at concurrency 1/10/20/30. The
detailed profile reported client mean/p50/p95 of 4.76/4.49/5.06 ms and server
mean/p50/p95 of 3.23/3.18/3.70 ms, with 23,725 approximate scores and 137.40
exact completions/query. Cold/warm restart serial p95 was 4.6/4.2 ms.

r126's sampled attributable demand was 557.3 MB and its post-phase physical-
footprint ledger was 534.7 MB. Cache-inclusive live RSS reached 2.200 GB, above
r124's 1.588 GB, while restart RSS was 335.1 MB. Because the native transaction
change neither maps nor retains serving files and the lower demand ledger does
not track the RSS increase, this single cache-inclusive high-water is recorded
as file-cache/host variance rather than attributed to the fix. It should not be
discarded: repeat controlled lifecycles remain the gate for a precise RSS
claim. The load-time, recall, query-latency, and throughput acceptance gates all
pass.

## Versioned physical-index lifecycle and rolling upgrades

Native HBC is now a physical vector-index version rather than an in-place
reinterpretation of the logical index directory. The logical catalog name and
configuration remain stable. In managed deployments, committed store records
advertise the native-v2 recovery capability through the rolling metadata
protocol. Authority remains closed until every table-serving store advertises
that capability. The Raft apply transaction that observes the complete capable
set records a monotonic activation version alongside the metadata incarnation;
data stores open their local authority gates only from that durable value, not
from an observed membership snapshot. The state machine then makes any stale
legacy store registration a deterministic no-op, closing the proposal/apply
race during the pre-promotion shadow-build window. Activation survives leader
changes, restart, and snapshot restore.

A legacy index continues serving while the durable index-repair state machine
builds a native shadow, replays it to a bounded activation gap, validates
coverage and structure, and atomically publishes it. Generation-manifest v2
records `dense_native_v2`, and the active-root pointer uses a deliberately
incompatible v2 header. Reopen requires the checksummed manifest and the
crash-sticky HBC `AUTHORITY` marker. This means an older binary fails closed
instead of silently opening stale compatibility LSM state. Manifest v1 remains
readable as `legacy_lsm`, so existing indexes need no offline rewrite.

Physical retirement is a separate catalog phase. Initial v1 files remain on
disk after v2 promotion and can be selected by the captured rollback pointer;
native reopen no longer deletes them. An explicit catalog-fenced retirement
call reclaims them only after the downgrade/rollback window advances. The same
shadow/pointer machinery applies to newly created managed indexes, avoiding a
special migration-only serving path. Standalone/Lite databases, which own their
entire compatibility domain, may still authorize local native publication.

Fresh dense admission now selects that end state directly once the durable
capability floor permits it. Creation stages an unpublished private root with a
checksummed construction manifest, establishes an O(1) empty native authority,
rewrites the root pointer with the incompatible v2 header, and commits the
logical catalog last. Managed/public admission therefore never builds a corpus
in the compatibility HBC LSM before scheduling its durable rebuild outbox; the
first user mutation is WAL-native. The construction capability is immutable and
scoped to that one entry, so building a new index cannot authorize native
transition on an unrelated live v1 index.

The synchronous standalone path uses the same lifecycle but backfills through
one pinned primary read transaction. The native capture records exactly that
transaction's replay sequence, writes the applied-sequence checkpoint, and
certifies the v2 generation at the same boundary; rows committed afterward stay
ordinary replay debt. A construction marker remains until the logical catalog
is durable, and explicit re-creation can reclaim a broken orphan pointer after a
crash. Before capability activation, fresh managed indexes remain v1, while all
pre-existing v1 indexes continue to use online shadow migration.

A follow-up fresh-backfill failure exposed that the posting and exact-vector
halves still established authority in the wrong order. Posting backfill could
see a direct document vector while the later vector-block snapshot searched only
for an index-managed embedding artifact which backfill had never materialized.
That both failed readiness and attempted a second primary scan. Fresh v2
construction now establishes the shared exact-vector base before capture:

- the first managed index publishes a real empty generation at source sequence
  zero with no physical shard files or primary scan;
- an existing exact table-wide generation adds a new zero-count artifact scope
  through a checksummed `CURRENT`-only transaction, preserving immutable blocks
  and the committed WAL prefix;
- synchronous backfill materializes the same index-managed source artifact as
  foreground direct-field writes, appends its exact vector to the native WAL,
  and builds HBC postings from that one pinned source transaction; and
- stable-tip publication compacts the captured native delta instead of
  rescanning primary artifacts. The wider snapshot sequence is accepted only
  while the generation is unpublished; promotion permanently closes that
  construction capability.

The regressions require zero primary vector snapshot builds for both managed
empty admission and standalone backfill, verify exact scoped coverage after a
second index joins a shared generation, reopen the metadata-only scope update
without changing WAL generation/bytes, and reject reuse of the construction
sequence override after v2 publication.

The managed corruption/recreate E2E then exposed a separate publication race:
repair-shadow orphan collection derived liveness only from one manager's
in-memory catalog. A catalog-lagging cleanup worker could therefore delete a
new native generation after its construction marker was cleared even though a
durable canonical `ACTIVE_ROOT` pointer already selected it. Cleanup now scans
all canonical pointers before orphan collection and treats their targets as
live without using payload health as deletion authority. The construction
marker protects a unique root before pointer publication; the durable pointer
protects it afterward. The exact public API corruption/delete/recreate test and
a catalog-lagging cleanup regression both pass with this rule.

The natural extension for reusable embeddings and other source artifacts is a
catalog-managed immutable artifact identity. Indexes should hold references,
not ownership by convention. Index-created artifacts remain scoped to their
producer; an explicit user promotion changes their lifecycle to managed/shared,
after which another index can reference the same artifact ID. Promotion must
verify schema/model/dimensions/source-generation identity and add a durable
reference before producer-index deletion can release its ownership. Physical
HBC posting/tree generations are index-specific and are not promoted as shared
source artifacts.

These controls add heartbeat/status fields, maintenance-time migration checks,
and O(1) manifest/pointer reads on open or promotion. They do not add work to
candidate routing, scoring, exact completion, or foreground mutation loops, so
the qualified r124-r126 latency and throughput measurements remain applicable.

## Native generation lifecycle hardening

The post-r126 PR review found three lifecycle gaps and the implementation now
uses the durable shape rather than benchmark-only workarounds:

- Native backup manifest v5 authenticates both the portable snapshot path and
  an explicit runtime `install_path`. Shared vector acceleration remains under
  the snapshot ownership namespace `indexes/vector-blocks`, but installs at
  the runtime-owned `vector-blocks` root. Duplicate or noncanonical install
  targets are rejected before any generated state is admitted.
- Snapshot admission acquires stable file-descriptor leases for exact committed
  posting/vector WAL prefixes. WAL copying, hashing, and fsync now happen after
  apply, replay, and structural mutation admission reopen. A deterministic test
  unlinks and replaces the live WAL before materialization and still recovers
  the selected committed prefix.
- Posting and vector generation directories reconcile strict native filenames
  against `CURRENT` at startup and publication boundaries. Known retirees are
  still deleted directly for storage-provider compatibility; inventory sweeps
  recover crash-before-publication orphans and retry failed unlinks. Cleanup
  reports `observed_debt`, `removed`, and `remaining_debt`, preserves unrelated
  files, and ordinary observational opens never reclaim concurrently staged
  generations.

These changes are outside the query and mutation hot paths. Snapshot fence work
is reduced from O(committed WAL bytes) to descriptor acquisition plus immutable
hardlink metadata. Publication adds one flat, filename-only inventory scan; it
does not read segment contents or recurse through the database tree.

The subsequent upgrade/restore review closed the remaining physical-generation
ownership gaps:

- Native authority can no longer appear as a side effect of an ordinary v1
  mutation. HBC requires an explicit authority-transition capability, and the
  catalog grants it only to an inactive candidate or an already-selected v2
  generation. Standalone storage skips distributed capability negotiation but
  still uses the same manifest plus incompatible pointer publication as a
  provisioned table.
- An authenticated native restore is rehomed into a deterministic v2 generation
  with an atomic directory rename, checksummed ready manifest, directory fsyncs,
  and pointer publication last. Retry validates or completes the same generation;
  it neither copies vector/index files nor replays the corpus.
- Compatibility LSM files inside the active v2 generation are restart-stable
  cleanup debt. The existing durable cleanup lane removes them only after native
  authority and the catalog capability floor are both proven (or, for standalone
  storage, after the v2 pointer has made downgrade fail closed).

The public serving and mutation loops are unchanged. Authority gating adds no
steady-state branch after the persisted-authority fast return; restore work is
O(index count) metadata plus directory renames; legacy retirement runs in the
background cleanup lane. A full DB lifecycle test now proves v1 remains
queryable during shadow construction, v2 promotion precedes retirement, native
backup/restore needs no embedder, and the restored read-only index has neither
format-migration nor repair debt.

A fresh post-merge r128 50K public-API lifecycle qualified correctness under
heavy host contention: recall was 0.9876 live, cold-reopened, and warm-reopened;
the published generation covered all 50,000 vectors; and no capture, generation,
recovery, or cleanup error was emitted. Restart RSS peaked at 350.8 MB and the
restart physical-footprint ledger at 124.2 MB. The host simultaneously ran two
unrelated CPU-saturating Zig test jobs, inflating insert to 207.68 seconds and
profiled server query time to 24.60 ms, so r128 is deliberately not timing
evidence. The uncontended r126 30.09-second lifecycle and 3.23 ms mean server
time remain the applicable performance baseline.

### Post-CI-fix performance qualification (r129)

Commit `d68605ac7` was first validated in Debug with the focused Zig coverage,
the public API regression suites, the exact managed-embedding restart test, and
the portable backup/restore test. ReleaseFast was used only for these fresh
performance measurements. Both runs used the public API, official VectorDBBench
cases, batch size 100, four load workers, the native HBC/vector-block path, and
float16 scan blocks. The adapter disabled the unrelated default full-text index.

The 50K lifecycle inserted in 21.85 seconds and reached native-ready in 25.88
seconds (4.03 seconds of final catch-up). Recall was 0.9836 live and warm after
restart, and 0.9769 cold after restart. The live concurrency 1/10/20/30 curve
was 173.0/401.9/422.8/347.7 QPS with p95 latency
7.0/47.7/100.6/157.8 ms. The warm profile scored 23,800 approximate vectors,
completed 135.6 exact vectors, and visited 207.9 leaves per query; mean client
and server times were 6.09 and 4.83 ms. Live peak RSS was 2.75 GB and sampled
attributable demand was 1.12 GB; restart peak RSS was 582 MB and restart demand
was 167 MB.

The 1M lifecycle inserted in 924.66 seconds and reached native-ready in 934.74
seconds (15.58 minutes total, including 10.08 seconds of final catch-up). Recall
was 0.9904 live/warm and 0.9905 cold. The live concurrency 1/10/20/30 curve was
6.72/107.48/131.45/124.79 QPS with p95 latency
64.5/116.6/212.6/344.9 ms; official serial p95/p99 was 17.7/18.3 ms. The warm
profile scored 239,240 approximate vectors, completed 146.3 exact vectors, and
visited all 2,048 leaves per query. Mean client/server time was 14.97/13.78 ms,
including 6.31 ms leaf scoring and 3.29 ms artifact reads. Cold-restart serial
p95/p99 was 127.8/138.0 ms, while warm-restart p95/p99 returned to 17.4/17.9
ms. Live peak RSS was 7.72 GB and sampled attributable demand was 2.50 GB;
restart peak RSS was 2.30 GB and restart demand was 940 MB.

These are correctness qualifications but fail the performance gate. Against
r126 at 50K, approximate work, exact completion, and leaves/query are nearly
unchanged, yet mean server time regressed from 3.23 to 4.83 ms and
concurrency-30 throughput from 1,065 to 348 QPS. Against r125 at 1M, ready time
regressed from 775.94 to 934.74 seconds and the live concurrency curve regressed
from 14.13/426.46/502.27/483.43 QPS. Attributable live demand remained nearly
flat (2.60 versus 2.50 GB) while cache-inclusive RSS rose from 3.86 to 7.72 GB.
Because the candidate shell is unchanged, the next diagnosis
should focus on serving synchronization, page residency/admission, and artifact
read behavior introduced after r126 rather than weakening routing or exact
rerank semantics.

## Memory methodology

Use Circus's native `footprint_sampler.py` against the Antfly server process
tree and capture the wired-memory baseline immediately before server start.
Datasets must already be cached. A valid publication number requires three
fresh lifecycles and reports mean plus range.

For native macOS runs, the primary demand number is the process tree's
`phys_footprint` ledger high-water. System-wide wired growth is reported as a
separate conservative diagnostic because unrelated host activity cannot be
attributed to Antfly. RSS remains the cache-inclusive point-in-time view. Do not
poll native `vmmap` during a timed phase: invoke the sampler once immediately
afterward and use the kernel-maintained footprint high-water for the phase peak.
The qualification runner captures live and restarted processes separately.
Historical scripts invoked `vmmap` every 200--300 ms and materially contaminated
both load throughput and query tails; those timings are not publication data.

The first partial 1M sample is diagnostic only: dataset download occurred after
the wired baseline, contaminating the system-wide wired delta. Its
cache-inclusive process-tree peak was about 1.18 GiB and its physical-footprint
ledger peak was about 785 MiB, but its wired-demand headline must not be
published.

## V1 mirror versus native-v2 authority

The rollback audit established an explicit storage-mode boundary that must not
be inferred from the presence of an optional posting generation:

- A v1 index remains LSM-authoritative, matching the v0.2.0 lifecycle. It may
  publish an immutable posting mirror for query acceleration, but mutations and
  their inverse rollback continue through the HBC LSM.
- Only a capability-authorized private candidate or an index selected by the
  incompatible v2 pointer may route mutations exclusively through the native
  posting WAL. Enabling that path without transition authority is a programmer
  invariant violation.
- A native capture pins its immutable base. Cancellation restores that base
  whether the candidate has published its irreversible authority marker yet or
  not; marker publication and rollback ownership are separate states.
- A delayed source callback below the capture's base coverage fails closed. It
  cannot relabel mutations at the current epoch. Legacy owners apply their true
  LSM inverse before cancellation; native owners restore the pinned generation.

The term `posting sidecar` predates this distinction and is now ambiguous. In
v2, the posting segment store is retained because it is the index authority;
only the compatibility HBC LSM is retired. Future API cleanup should use
`native_posting_store` for v2 and reserve `sidecar` for the optional v1 mirror.

### Review hardening: rollback capability is capture-local

The follow-up review found that mutation-store mode and rollback authority are
not equivalent. An authorized bootstrap or repair candidate can select native
mode before its first complete immutable generation exists. During that window
mutations still reach the compatibility LSM, and cancellation has no native
base to restore. Skipping the inverse rollback merely because native mode was
selected could therefore retain a phantom derived vector after the primary
mapping transaction failed.

Rollback now asks whether the active capture actually owns a pinned immutable
base. A base-owning capture restores that generation in constant time; a
base-less candidate and a v1 mirror perform the real LSM inverse. Tests cover
all three states. Native-store enablement also returns a runtime authorization
error instead of relying on `std.debug.assert`, preserving the authority fence
in production `ReleaseFast` builds without adding work to mutation or query
hot loops.

### Review hardening: artifact identity is a set

The exact-vector projection originally inferred one artifact family from
`embedding_name` or the index name. That model is incomplete for indexes which
union several embedding sources and was also inconsistent with the supported
external single-source form whose artifact name differs from its index name.
The durable contract is now explicit and shared by backfill, mutation-WAL
publication, mmap projection reads, topology projection builds, snapshot
construction, and readiness:

- a single-source index owns exactly its configured `embedding_name`, falling
  back to the index name only when no name was configured;
- a multi-source index owns the configured set of artifact families and each
  posting member retains its exact source artifact key;
- table-wide generation manifests declare the union of those scopes;
- base cardinality readiness sums the per-source certificates for the logical
  index instead of consulting an index-name surrogate.

This keeps the common single-source lookup at one derived key and adds no query
I/O. Multi-source projection lookup is cheaper than the broken path because it
uses the already-materialized member key directly. Tests publish both source
families through the native mutation WAL and certify their combined immutable
base coverage.

### Review hardening: orphan deletion is reauthorized

The initial canonical-pointer inventory remains an efficient filter, but it is
not deletion authority: another manager can publish a repair generation after
that scan and clear its construction marker. Cleanup now re-reads all canonical
pointers for each unmarked deletion candidate immediately before touching its
filesystem or algebraic state. The required publication order is therefore a
complete handoff: the construction marker protects the pre-pointer window and
the revalidated pointer protects the post-publication window. Invalid pointers
fail closed. A deterministic hook test publishes and clears the marker exactly
between inventory and deletion and verifies that the live generation survives.

## Stable-tip mirror flatten regression and recovery

An exact A/B within this PR isolated a query-layout regression. Commit
`2c9d731a9c` (the r126 experiment) finished with one flat posting generation,
whereas the later PR retained a base plus three delta generations. The search
work was otherwise comparable at 1M: about 240K approximate scores and 145
authoritative completions per query. The retained deltas shadowed almost every
leaf, reducing native leaf-scan hits from 2,048 to about 35 and forcing about
469 sparse projection reads per query. Concurrency-20 throughput fell from
679.28 QPS to 125.28 QPS and p95 rose from 66.62 ms to 218.54 ms.

The cause was an invalid coupling between physical layout and mutation
authority. Stable-tip readiness requested a full posting checkpoint only when
the native WAL was authoritative. A released-v1 index is deliberately still
LSM-authoritative, but its native query mirror is equally safe to flatten:
`make_authoritative` remains a separate, capability-fenced transition.

Allowing stable-tip flattening for managed v1 mirrors recovered the fast path
without weakening rollback or upgrade behavior. A fresh public-API 50K run at
batch 100 and four load workers produced:

- 23.5014 s readiness: 21.471 s insert plus 2.0304 s optimize;
- 0.9862 recall;
- 178.51 / 1070.49 / 1224.40 / 1146.74 QPS at concurrency 1/10/20/30;
- 7.09 / 15.26 / 33.87 / 62.92 ms p95 at concurrency 1/10/20/30;
- 207.926 native leaf-scan hits and zero leaf-scan fallbacks per query;
- zero approximate projection reads and 137.338 exact residual reads per query;
- one 172 MiB full posting segment and an empty WAL after restart;
- 2.80 GB cache-inclusive live peak RSS and 533 MB restarted peak RSS.

This is better than the pre-fix PR's 26.6046 s readiness and restores its
regressed query curve (405.15 / 423.87 / 379.32 QPS at concurrency 10/20/30)
to r126-class performance. The deterministic checkpoint test now proves that a
non-authoritative v1 mirror can flatten while retaining a concurrently appended
WAL tail and remaining non-authoritative throughout publication.

The first 1M validation then exposed a coverage-only amplification edge. The
intended generation 15 full checkpoint published a 1.877 GB flat base, but a
concurrent 188-byte record which only repeated source coverage caused readiness
to start an identical generation 16 rewrite. That rewrite overlapped the first
query curve: concurrency 10 collapsed to 9.05 QPS while the uncontaminated
concurrency 20/30 waves reached 781.51/838.70 QPS at 0.9924 recall. Live RSS
peaked at 8.24 GB. This run is diagnostic, not a publishable performance result.

The posting store now durably distinguishes state-bearing WAL records from
coverage-only records across append, checkpoint-tail publication, and reopen.
Repeating the current coverage watermark is an idempotent no-op. A newer
coverage-only watermark remains in the HBC-native WAL for crash-safe recovery,
but is not query-state debt and cannot trigger a corpus rewrite. Tests include
a coverage tail arriving after a zero-WAL checkpoint source boundary.

The same A/B also explained the remaining insert regression. Standalone uses
`LocalStandaloneMetadata`, not the distributed store reporter, so its
provisioned native-authority gate could never open. It therefore created a v1
mirror and paid both compatibility-LSM mutation work and native WAL publication;
1M dense finalization accumulated 259.43 seconds. A non-HA standalone process
has no mixed-version peer, so it now authorizes native v2 before exposing the
public listener. Standalone HA remains closed until its replication protocol
has an all-peer capability fence, and distributed deployments retain their
durable catalog capability floor.

A clean post-fix 50K public-API qualification confirmed the combined result:

- native-authoritative from initial catalog publication, with the legacy HBC
  LSM released before ingest;
- 24.8254 s readiness: 12.5043 s insert plus 12.3211 s optimize;
- 0.9863 recall;
- 280.50 / 1092.67 / 1218.01 / 1174.47 QPS at concurrency 1/10/20/30;
- 4.03 / 14.94 / 34.02 / 60.25 ms p95 at concurrency 1/10/20/30;
- exactly 208 native leaf-scan hits, zero leaf fallbacks, and zero approximate
  projection reads per query;
- one 172 MiB full posting segment, an empty WAL, and no post-readiness rewrite;
- 2.54 GB cache-inclusive live peak RSS and 339 MB restarted peak RSS, with
  post-phase attributable demand of 848 MB and 103 MB respectively.

The corresponding clean 1M qualification also recovered and exceeded the
earlier r126 result from this PR:

Audit correction (September 5): r126's original live c30 failed. Its c30
numbers below came from a separate restarted retry, so those comparisons
are not same-phase live qualification evidence.

- 617.3662 s readiness: 548.9080 s insert plus 68.4582 s optimize, versus
  r126's 706.2518 s total and 636.1355 s insert;
- 0.9925 recall versus 0.9932 for r126, a 0.07 percentage-point difference;
- 78.13 / 633.96 / 794.07 / 828.00 QPS at concurrency 1/10/20/30, improving
  r126 by 31.2% / 5.6% / 16.9% / 22.9%;
- 12.90 / 24.72 / 55.77 / 94.32 ms p95 at concurrency 1/10/20/30, improving
  the first three curves while concurrency-30 was 1.5% above r126's 92.88 ms;
- 8.86 ms mean and 9.52 ms p95 server time in the detailed public profile,
  versus 9.49 ms and 10.16 ms for r126;
- 239,734 approximate and 146.5 authoritative exact vectors per query, with
  all 2,048 explored leaves served by the native scan plane, zero leaf-scan
  fallbacks, and zero approximate projection reads;
- one 1,848,328 KiB full posting segment, an empty WAL, no redundant successor
  generation, and 8,177,052 KiB total durable data;
- 7.05 GB cache-inclusive live peak RSS and 2.39 GB restarted peak RSS, with
  post-phase attributable demand of 913 MB and 453 MB respectively.

Both r126 and the post-fix run reached their live RSS maxima roughly 40 seconds
before readiness, during final native publication rather than query serving.
The post-fix cache-inclusive maximum was 0.94 GB higher than r126, but its
post-phase attributable demand was 0.39 GB lower and its fresh-process restart
RSS was 34 MB lower. This classifies the remaining difference as transient
publication/file-cache residency, not retained serving heap. It remains a real
peak-RSS optimization target and must not be hidden by reporting demand alone.

### Bounded publication and concurrent-request workspaces

A follow-up allocation audit found that the remaining transient peaks were not
one problem and should not be hidden behind a larger process reservation:

- the V1 vector directory retained its value plane and 29-byte-per-entry index,
  then allocated the joined result before copying it into the staged segment;
- a full posting flatten reconstructed every immutable replacement patch before
  writing any of them;
- an index retained only one request scratch, so concurrent searches repeatedly
  allocated and destroyed otherwise reusable bounded buffers;
- replay leaf splits and posting commits rebuilt parallel lookup arrays for
  every operation; and
- an equal source-coverage append could skip the durability requested by a
  later `sync=true` caller.

The production shape now treats each category according to its lifetime. The
vector-directory writer streams values directly to the unpublished generation
and retains only its compact index, producing byte-identical V1 files. The
centroid-directory allocation is released as soon as its staged copy completes.
Immutable patch chains are represented by keys during a flatten, reconstructed
one logical value at a time, copied, and immediately released; they never enter
the query-lifetime patch cache. This changes peak reconstruction heap from the
sum of all patched values to the largest active patched value. On the measured
1M generation, streaming removes the extra value plane and joined directory
copy while retaining roughly the 58 MB compatibility index. That bounded
publication fix establishes a clean baseline before changing the physical
format.

Search scratch now uses a resource-manager-admitted pool capped at 32 entries
per index, matching the qualification's maximum fanout without becoming an
unbounded per-request cache. Pool metadata is reserved at index open so normal
acquire/release does not enter the allocator beneath the scratch lock. Pressure first trims
oversized request buffers and then discards secondary baseline entries while
keeping one serial fast-path slot. The replay split workspace likewise owns and
reuses its metadata/lookup arrays, and posting-WAL commit arrays retain ordinary
batch capacity but discard capacity above 4,096 entries. Both remain visible to
the existing apply/search resource ledgers.

Finally, equal-watermark coverage remains a logical no-op, but `sync=true` now
fsyncs a non-empty WAL that may have been appended unsynced. Coverage still
requires a published checkpoint, and an ambiguous sync failure poisons the
store until reopen. These semantics preserve idempotence without weakening the
caller's durability contract.

A fresh ReleaseFast 50K public-API qualification validates the combined shape.
The run used batch 100, four load workers, native HBC, float16 vector blocks,
flat-exact centroid routing, and the concurrency 1/10/20/30 query curve. It
completed in 30.3630 seconds (18.9887 seconds insert plus 11.3743 seconds
optimize) while another repository workload was active on the host. Recall was
0.9885. Throughput was 293.82 / 1,079.76 / 1,192.00 / 1,319.31 QPS and p95 was
3.88 / 17.03 / 49.38 / 71.27 ms at concurrency 1/10/20/30. Compared with the
clean pre-change qualification, concurrency-30 throughput increased 12.3%
from 1,174.47 QPS; the middle p95s remain scheduler-sensitive and should not be
claimed as latency wins from this contended sample.

More importantly for this change, cache-inclusive live peak RSS fell from
2.54 GB to 1.52 GB and restarted RSS remained essentially flat at 341 MB versus
339 MB. The post-phase footprint sample reported 963 MB attributable demand.
The detailed warm profile scored 24,412.5 approximate vectors and exactly
completed 137.5 vectors per query, with 4.85 ms mean client latency, 4.71 ms
p95, and 2.93 ms mean server time. Scratch acquisition itself averaged 0.0002
ms. Baseline scratch bytes are now admitted before allocation; the capacity-
stability test proves a second concurrent fanout reuses the existing payloads,
and pressure testing proves secondary entries are reclaimable while the serial
slot remains hot. This preserves the PR's query performance while removing the
publication peak and steady high-concurrency allocator churn that motivated
the follow-up.

### Experimental V2 vector directory: kind-separated blocks

The native index has not shipped, so this branch can evaluate the intended
physical layout without carrying an avoidable permanent V1 tax. V1 stores a
29-byte generic index entry for every `(kind, vector_id)` pair. With both leaf
assignment and external metadata present, that is 58 MB of index at 1M vectors,
in addition to the eight-byte leaf values and metadata payload. Its streaming
writer avoids a second value copy, but still retains the entire index until
publication; its reader also hashes and walks that full index at open.

The V2 experiment changes the directory to independently checksummed,
kind-separated blocks:

- leaf values keep their fixed eight-byte representation and need no generic
  offsets, lengths, or per-value checksums;
- metadata stores block-local cumulative end offsets, making value bounds O(1)
  while retaining a compact four-byte plane;
- sorted external IDs use delta varints when smaller and raw u64 values for
  sparse or adversarial IDs, so correctness never assumes user IDs are dense;
- blocks are capped at 256 entries and approximately 64 KiB of values;
- a small checksummed root contains block ranges, locations, and checksums;
  open validates only that root and structural bounds, while lookup validates
  the touched index and value block; and
- the streaming writer retains one block plus root descriptors, making its
  workspace O(blocks), with a sub-megabyte root at 1M rather than a 58 MB
  compatibility index.

A deterministic compactness test covers 4,096 vectors and requires the entire
directory—including leaf values, both kind indexes, checksums, descriptors,
header, and footer—to remain below 16 bytes per vector for compressible IDs.
Sparse-ID, block-boundary, streaming-equivalence, lazy-corruption, and wrapped-
region tests preserve the non-benchmark invariants. This is intentionally a
format experiment: 50K and 1M public-API results must still establish load,
restart, disk, recall, and the full concurrency latency curve before V2 replaces
the V1 baseline.

The first contended 50K V2 run completed the public lifecycle in 33.6910 seconds
(21.9621 seconds insert plus 11.7289 seconds optimize) at 0.9880 recall. Its full
posting segment was 178,868,456 bytes (170.6 MiB), about 1.4--1.5 MiB below the
V1 sample, and live peak RSS was effectively unchanged at 1.516 GB versus
1.519 GB. Restart RSS was 348 MB versus 341 MB. However, throughput regressed
to 241.45 / 1,025.42 / 1,102.51 / 1,089.58 QPS at concurrency 1/10/20/30, and
the detailed warm server mean increased from 2.93 ms to 3.97 ms. Recall and
candidate work remained essentially unchanged, so this cannot be attributed to
a different quality/effort point.

Host compiler contention affected the sample, but it also exposed a format
implementation mistake worth fixing independently: lazy block verification was
recalculating CRCs on every lookup. The follow-up reader retains two atomic
verification bitmaps (index and value, approximately 2 KiB total even at 1M).
The first concurrent reader to validate an immutable block publishes that fact;
all later readers under the same generation lease skip its CRC and structural
walk. Corruption still fails closed before a block is exposed, restart remains
root-only, and verification state disappears with the generation. A fresh V2
run is required after this change; the first latency curve is diagnostic, not an
accepted result.

The post-cache 50K run recovered the load target: 26.6993 seconds total
(13.9129 seconds insert plus 12.7864 seconds optimize) at 0.9862 recall. The
segment was 179,058,453 bytes, live peak RSS was 2.03 GB, and restarted RSS was
339 MB. Throughput was 243.94 / 1,002.52 / 1,075.69 / 1,082.55 QPS at
concurrency 1/10/20/30. The detailed warm server mean recovered from the flawed
V2 sample's 3.97 ms to 3.28 ms, versus 2.93 ms in the prior V1 sample. The host
remained compiler-contended, and all 137.4 exact vectors per query came from the
artifact cache with zero metadata-vector loads, proving that the vector
directory is not on this workload's scored query path. The remaining latency
difference therefore is not evidence for adding speculative directory indexes.

Before the 1M run, metadata's four-byte length plane was changed to cumulative
block-local end offsets. This is byte-for-byte the same size but changes random
metadata offset recovery from scanning up to 255 lengths to O(1); validation is
now a monotonic/end-bound check. Publication also reports exact directory entry,
block, value, index, root, and total byte counts so topology variation cannot be
mistaken for format savings. The 1M qualification should use this final shape.

The final-shape 1M lifecycle completed without capture, publication, restart,
or query failures. The published directory contains 2,000,000 logical entries
in 7,814 blocks and occupies 35,908,804 bytes: 17,888,890 value bytes,
17,519,770 index bytes, and a 500,096-byte root. The equivalent V1 value plane
plus 29-byte-per-entry index would occupy approximately 75,888,890 bytes, so V2
removes 39,980,086 bytes (52.7 percent) before counting its lower publication
workspace. Recall remained 0.9927, candidate work remained 238,867 approximate
and 146.5 exact vectors per query, and restarted demand/RSS were 379 MB/2.37 GB.
Live peak RSS was 5.73 GB versus 7.05 GB in the prior V1 qualification.

The run took 716.4625 seconds to readiness (630.0084 seconds insert plus
86.4541 seconds final catch-up). Its concurrency-1/10/20/30 throughput was
58.48 / 568.41 / 596.16 / 584.99 QPS, with p95 latency of
15.05 / 27.96 / 78.99 / 105.61 ms. Those timing numbers are recorded as a
contended qualification, not a clean V1/V2 comparison: other worktree test
servers and a CPU-heavy compiler/agent process were active during the run.
The directory is not on the scored candidate path, and the unchanged candidate
and exact-completion counts support that interpretation. A controlled rerun is
still required before attributing either the timing regression or the RSS gain
entirely to V2.

#### V2 row-block revision before the controlled rerun

The first block format still encoded the sorted vector-ID stream twice: once
for leaf assignments and once for sparse external metadata. It also decoded up
to 255 varints for a point lookup and issued one physical sink append for every
small leaf value. Those costs were implementation artifacts rather than useful
A/B variables, so the controlled rerun uses the completed row-block shape:

- each block is the ordered union of vector IDs and encodes every ID once;
- independent presence bitmaps select the required leaf-assignment plane and
  optional metadata plane, preserving metadata-only recovery rows and the
  distinction between absent and present-empty metadata;
- delta IDs have an absolute restart every 16 rows, bounding point lookup to a
  binary search over restarts plus at most 15 varint decodes;
- index, leaf, and metadata planes have independent checksums and independent
  once-per-generation verification bits, so touching one value family does not
  fault or hash the other;
- checkpoint publication performs two ordered base/overlay merges and pairs
  them by ID online. It retains one block and the bounded live overlay, not a
  corpus-sized join table; and
- data, index, and root bytes are each emitted in bounded sequential writes.
  A 2,048-value unit fixture now proves physical append count is proportional
  to blocks rather than entries.

The row count is at most the former leaf-entry count when every live vector has
a leaf assignment, while sparse metadata adds no duplicate row or ID. This
should reduce both the 7,814-block/500-KiB root and the 17.5-MB index observed
at 1M. The exact reduction and any publication/lookup CPU tradeoff remain
measurements; no result is claimed until a fresh public-API 50K qualification.
Adaptive leaf dictionaries remain a possible follow-up, but they depend on
observed per-block cardinality and are intentionally excluded from this
structural A/B. Metadata cumulative ends now select a two-byte representation
when a block's metadata plane is at most 65,535 bytes and widen to four bytes
only for a larger block. This preserves O(1) offset lookup and arbitrary value
sizes while avoiding a permanent four-byte-per-value tax.

Two controlled public-API 50K qualifications now validate the row-block
revision. Both used batch 100, four load workers, native HBC, float16 vector
blocks, the normal boundary-rerank policy, and 30-second concurrency
1/10/20/30 query phases:

| Directory | Ready (insert + catch-up) | Recall | QPS at 1 / 10 / 20 / 30 | p95 ms at 1 / 10 / 20 / 30 | Live RSS | Restart RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| V1 control | 27.37 s (17.50 + 9.87) | 0.9860 | 231 / 910 / 967 / 1,040 | 4.95 / 19.28 / 52.69 / 77.77 | 1.76 GB | 350 MB |
| Kind-separated V2 | 33.52 s (21.07 + 12.45) | 0.9857 | 225 / 920 / 996 / 953 | 5.28 / 19.04 / 48.66 / 81.75 | 1.46 GB | 337 MB |
| Row-block V2, run A | 28.48 s (12.64 + 15.84) | 0.9853 | 271 / 1,096 / 1,189 / 1,233 | 4.16 / 14.86 / 33.31 / 61.78 | 1.82 GB | 340 MB |
| Row-block V2, run B | 24.38 s (15.30 + 9.08) | 0.9857 | 284 / 1,084 / 1,203 / 1,245 | 3.99 / 15.32 / 32.87 / 58.69 | 2.08 GB | 340 MB |

The two row-block runs average 26.43 seconds to readiness and
277 / 1,090 / 1,196 / 1,239 QPS. Relative to the V1 control, that is 3.4
percent faster readiness and approximately 20 / 20 / 24 / 19 percent more
throughput. Mean recall differs by 0.0005, or 0.05 percentage point. Before the
adaptive metadata-end follow-up, the directory itself was 1,439,976 bytes
(838,890 values, 587,710 index, and 13,328 root) in 196 blocks. The earlier
kind-separated V2 used 1,781,658 bytes and 392 blocks; the equivalent V1
directory is approximately 3,738,930 bytes. Thus the row form removed another
19.2 percent from the first V2 and 61.5 percent from V1 while also eliminating
the duplicated ID decode and per-value sink calls. Normal 50K blocks carry
50,000 metadata ends, so the adaptive encoding should remove another 100,000
bytes; the 1M qualification will report the measured final size.

Cache-inclusive live RSS is not a demonstrated win: its 1.82--2.08 GB range is
above the 1.76 GB V1 sample even though the post-run attributable-demand
samples ranged from 517 MB to 1.17 GB and restart consistently returned to
340 MB. The RSS maximum occurred at the end of the fixed-duration query matrix,
not during directory construction. That points to mapped posting/projection
page residency and run-to-run reclaim timing rather than a retained V2 writer
allocation. Generation-aware page eviction under the resource governor should
be evaluated separately; changing the directory format to fit this cache high
water would conflate storage layout with serving-cache policy.

No further mandatory encoding is justified before a 1M row-block run. A
per-block leaf dictionary is beneficial only when leaf cardinality is low
enough to offset its dictionary and ordinal planes; more aggressive metadata
length compression needs the real length distribution.
Page-aligning this small point-lookup directory would add padding and resident
pages without helping the posting-local candidate scan. Record those
distributions during the 1M qualification, then make each encoding an adaptive
block-level choice if the measured saving pays for its decode branch.

Two non-format follow-ups remain substantive but should retain independent
A/Bs. Legacy first-generation migration still builds an owned directory before
publication and should eventually use the same staged streaming writer to keep
upgrade memory bounded. Separately, adversarial online insertion can produce a
deep tree; a bulk-balanced/recursive initial topology or bounded rebuild fixes
that structural problem, but mixing it into the directory qualification would
change routing, candidate work, and recall at the same time.

The final row-block 1M public qualification completed without capture,
publication, restart, or query failures. All 1,000,000 rows were query-visible
at source sequence 10001, recall was 0.9929, and restart preserved the same
recall. The final directory occupies 27,516,934 bytes in 3,907 blocks:
17,888,890 value bytes, 9,362,320 index bytes, and 265,676 root bytes. Compared
with the final kind-separated V2, this removes 8,391,870 bytes (23.4 percent),
halves the block count, and reduces the complete posting segment from
1,837,047,621 to 1,828,383,201 bytes. Compared with the equivalent V1
directory, it removes approximately 48.4 MB (63.7 percent).

| Public 1M directory A/B | Kind-separated V2 | Row-block V2 | Change |
| --- | ---: | ---: | ---: |
| Insert | 630.01 s | 636.06 s | +1.0% |
| Finalize | 86.45 s | 74.18 s | -14.2% |
| Total ready | 716.46 s | 710.24 s | -0.9% |
| Recall | 0.9927 | 0.9929 | +0.02 pp |
| QPS, concurrency 1 / 10 / 20 / 30 | 58 / 568 / 596 / 585 | 71 / 636 / 642 / 534 | +20.8% / +11.9% / +7.7% / -8.7% |
| p95 ms, concurrency 1 / 10 / 20 / 30 | 15.05 / 27.96 / 78.99 / 105.61 | 13.95 / 23.82 / 75.17 / 111.86 | -7.3% / -14.8% / -4.8% / +5.9% |
| Attributable demand after live phase | 1.90 GB | 1.80 GB | -5.3% |
| Cache-inclusive live peak RSS | 5.73 GB | 6.72 GB | +17.2% |
| Restart peak RSS | 2.37 GB | 2.12 GB | -10.6% |

The detailed profile kept quality/work essentially constant: 240,156
approximate and 146.5 exact vectors per query versus 238,867 and 146.5. Mean
server search time improved from 9.995 to 9.823 ms and leaf scoring from 6.372
to 6.187 ms. At concurrency 20, the row run sustained 642 QPS at 31.07 ms mean
latency, which accounts for essentially all 20 clients. Raising concurrency to
30 increased mean latency to 55.73 ms while throughput fell to 534 QPS; all 30
clients were still active. This is a memory-bandwidth/cache-contention knee,
not request rejection, less search work, or a rerank shortcut. The row format
changes directory publication and lookup rather than the 240K-vector scoring
plane, so it cannot by itself explain or repair this serving limit. A
query-only 1M repetition with normalized cache state and phase ordering is
warranted, but the measured regression must remain visible.

The row qualification is not an overall performance improvement over r126.
Audit correction (September 5): r126 c30 below is a restarted retry, not a
completed original live phase; use the audited full curves for qualification.
Its 710.24-second readiness is 0.6 percent slower than r126's 706.25 seconds,
with recall within 0.03 percentage points. It improves concurrency-1 and
concurrency-10 QPS over r126's inferred 59.55/600.34 QPS, but its 642/534 QPS
at concurrency 20/30 is approximately 5.4/20.8 percent below r126's
679.27/673.72 QPS, and its concurrency-30 p95 is 20.4 percent above r126's
92.88 ms. It is farther behind the best post-r126 qualification's 617.37-second
readiness and 794/828 QPS at concurrency 20/30. The row directory passes the
correctness, compactness, and restart-memory experiment gates; it does not yet
pass the full replacement performance gate.

Both 1M runs reached their cache-inclusive RSS maximum at the final full
checkpoint, not while querying. The row run's lower attributable demand,
smaller final segment, and lower restart RSS rule out a retained row-directory
buffer. Its higher live maximum is reclaimable old-generation/source/output
page residency during generation exchange. The immediate durable follow-up is
to stream final publication through bounded extents, evict consumed source and
fsynced staged-output pages, install the new generation without prefaulting its
payload, and unmap or advise-away retired payloads as soon as their query lease
count reaches zero. The resource governor must account resident bytes by
generation and component, including staged output and file cache, rather than
only allocator-owned demand. Longer term, the 1.83-GB monolithic posting
segment should become a manifest over immutable independently checksummed
chunks so consolidation rewrites only dirty chunks and atomically publishes a
new manifest. That bounds copy-on-write and cache overlap while preserving
generation leases, source-sequence coverage, and contiguous posting-local
scans. Weakening generation leases or making the directory less compact would
solve the wrong problem.

The load crossed the primary LSM's 2-GiB hard-byte boundary once. One pressure
compaction made progress, moved about 2.0 GB to lower levels, and admitted the
remaining writes without overload or a write stall. Cumulative primary mutable
snapshot copies were 1.16 GB and read-snapshot rotations were 3.85 GB. These
remain independent primary document/embedding-store costs; V2 neither hides nor
amplifies them. The run also observed one transient zero-count runtime-status
placeholder while the managed writer was busy. The next sample reported the
correct monotonically advancing counts, and query/durable state never regressed;
status UX should distinguish `temporarily_unavailable` from a literal empty
index in a separate change.

#### Node-wide scan admission and cold publication experiment

The concurrency-30 regression was a real memory-bandwidth knee rather than a
row-directory regression. On the existing 1M generation, fixed active-scan
caps of 24/20/16/12 produced 723.6/701.8/730.3/667.8 QPS and p95 latency of
90.8/89.5/85.7/92.0 ms respectively. The cap-16 prototype was the best sample,
but a process-wide constant would encode this machine and corpus into product
behavior. It was removed after establishing the knee.

The production-shaped replacement is a FIFO, work-weighted node admission
queue owned by `ResourceManager`. Each query is charged for its estimated
candidate scan bytes using the published active-vector/node shape, search
width, dimensions, and quantization encoding. It waits before acquiring an
MVCC transaction, generation fence, or query scratch; waits are cooperative
with the runtime and preserve request cancellation. Oversized queries consume
the complete budget and retain progress. The default capacity derives from the
dense-search working-set budget, can be explicitly configured or disabled,
and exports capacity, active/queued query and byte high waters, grants, waits,
cancellations, and cumulative wait time.

The same experiment isolates full-checkpoint projection reads from query
residency. One projection-build lifecycle pins a single exact vector generation
at the requested source sequence for the whole posting generation. Native
storage opens maintenance-private, FD-governed random-read descriptors. macOS
uses `F_NOCACHE`; Linux now prefers `O_DIRECT` through one reusable aligned
window per block and falls back to `NOREUSE` when the filesystem cannot honor
direct I/O. This replaces ineffective sub-page `DONTNEED` calls and never
evicts a foreground mmap of the same immutable inode. If a generation has more
blocks than the private-descriptor capacity, publication retains the pinned-
generation correctness boundary and falls back to ordinary positional reads.
Focused tests prove descriptor accounting, byte-equivalent batched projection
reads, FIFO/work-weighted admission, and cancelled-waiter removal.

A fresh public-API 50K qualification used batch 100, four load workers, native
HBC, float16 vector blocks, boundary rerank, and 30-second concurrency phases.
It completed in 30.49 seconds (20.44 insert + 10.05 catch-up), retained 0.9862
recall, delivered 274/1,109/1,198/1,229 QPS at concurrency 1/10/20/30, and had
p95 latency of 4.19/14.76/34.46/59.56 ms. Live/restart peak RSS was 1.88 GB /
345 MB. Its 30 active scans totaled only 110 MB against the 384-MiB budget, so
there were no waits: the governor stays out of the way below the bandwidth
knee.

The fresh public-API 1M qualification also completed without capture,
publication, restart, or query failures:

| 1M row V2 | Before admission/cold reads | Admission + cold reads | Change |
| --- | ---: | ---: | ---: |
| Insert | 636.06 s | 634.62 s | -0.2% |
| Finalize | 74.18 s | 89.63 s | +20.8% |
| Total ready | 710.24 s | 724.25 s | +2.0% |
| Recall | 0.9929 | 0.9924 | -0.05 pp |
| QPS, concurrency 1 / 10 / 20 / 30 | 70.6 / 636.1 / 642.3 / 533.9 | 70.7 / 626.8 / 701.6 / 649.6 | +0.2% / -1.5% / +9.2% / +21.7% |
| p95 ms, concurrency 1 / 10 / 20 / 30 | 13.95 / 23.82 / 75.17 / 111.86 | 13.85 / 24.68 / 66.73 / 87.48 | -0.7% / +3.6% / -11.2% / -21.8% |
| Cache-inclusive live peak RSS | 6.72 GB | 6.26 GB | -6.9% |
| Restart peak RSS | 2.12 GB | 2.37 GB | +11.8% |

The governor admitted at most 17 simultaneous 1M scans, charged 381 MB of its
402.7-MB capacity, queued at most 13 requests, and observed no cancellations.
A restarted reverse-order 30/20/10/1 curve delivered
666.0/699.6/624.7/97.6 QPS with p95 82.72/56.56/24.85/11.60 ms. Concurrency 30
is therefore 9.5 percent faster with 16.9 percent lower p95 than the uncapped
reverse-order control (608.3 QPS / 99.54 ms). It is within 1.2 percent of
r126's restarted-retry 673.6 QPS while improving on that retry's 92.88-ms p95.

The first governor used aggregate mean leaf occupancy, which is accurate for
the balanced qualification build but can undercharge a skewed online tree.
Search profiles now measure the largest posting presented to the candidate
loop before filtering. Admission takes the maximum of aggregate mean,
configured leaf size, an EWMA, and an immediately reacting, slowly decaying
observed high water. It records both posting occupancy and actual per-leaf scan
bytes, so stale-payload float32 fallback cannot be charged as RaBitQ merely
because the index is normally quantized. The observation runs after the query
has released its MVCC transaction and request scratch and affects only later
permits, so it never introduces a wait while those resources are held. Query
admission reads lock-free atomic snapshots of the model; concurrent serving
does not contend on the route-observation mutex.

Candidate work and exact completion stayed unchanged at 239,918 approximate
and 146.6 exact vectors/query. The admission gain is not a recall, routing, or
rerank shortcut. The cold-reader result is useful but not free: it removed
about 462 MB from the live RSS high water while adding 15.45 seconds to final
publication. Corrected by the September 5 raw-result audit: its 6.26-GB
peak is approximately 2.4 percent above r126's actual 6.11 GB; 5.73 GB
belonged to the separate kind-separated V2 run.
The durable end state remains chunked posting generations which reuse clean
projection chunks instead of rereading and rewriting the complete 1.83-GB
generation. Until then, private cold descriptors bound cache overlap at a
modest two-percent total-load cost without affecting foreground query layout.

A first follow-up tried to make both admission and those cold reads more
precise. It authenticated conservative per-generation leaf occupancy and scan
bytes in the quantized-directory header, sorted projection requests by physical
offset, and coalesced neighboring reads. That 50K run inserted in 17.94 seconds
and reached ready in 34.34 seconds, but it was rejected during the query curve:
the temporary root-only generation had raised a process-lifetime admission
floor which could never fall after compact publication. Every query was still
charged 214.70 MB, only one query fit in the 402.65-MB node budget, the queue
reached 19 requests, and cumulative wait time exceeded 572 seconds. Concurrency
10 consequently collapsed to 288.5 QPS. This was an admission-accounting bug,
not a routing, recall, or rerank change.

Admission is now generation-coupled instead of process-monotonic. A request
samples the current authenticated cost without retaining the generation while
it waits. After receiving bandwidth it retains the then-current immutable
generation, re-estimates its cost, and retries only if the bound generation is
more expensive than the permit request. The retained generation transfers
directly into the search transaction. Thus a query can never scan a more
expensive generation than it was charged for, while a compact replacement can
immediately lower the charge. A deterministic test publishes an expensive
generation, admits a request, installs a compact generation, and proves that
the first request remains on the old generation while the next permit falls
from 25.6 KB to 128 bytes. The wait path still owns no MVCC transaction,
request scratch, or large generation lease.

The coalesced projection reader also now preserves bounded parallelism: ranges
within one physical reader are coalesced, up to eight independent reader groups
run concurrently, and each at-most-1-MiB bounce extent is released as soon as
its group completes. Buffered singleton groups read directly into the caller's
destination, avoiding a bounce allocation and copy when coalescing cannot save
an I/O. Linux O_DIRECT continues to use aligned scratch. This bounds the extra
maintenance scratch at approximately 8 MiB rather than one extent per shard.

The final fresh public-API 50K qualification used the same batch-100,
four-worker, native-HBC, float16-vector-block, boundary-rerank configuration.
It reached ready in 31.2811 seconds (14.1282 insert + 17.1529 catch-up), retained
0.9855 recall, and produced the following live curve:

| concurrency | 1 | 10 | 20 | 30 |
| --- | ---: | ---: | ---: | ---: |
| QPS | 225.4 | 929.9 | 1,017.2 | 1,029.4 |
| p95 latency | 5.91 ms | 21.33 ms | 53.67 ms | 76.00 ms |

The machine remained contended by unrelated repository tests, so those
absolute QPS values are not an uncontended replacement for the earlier
274/1,109/1,198/1,229 curve. They do establish that the admission regression is
gone: the governor held all 30 concurrent queries, peaked at 201.28 MB admitted
against 402.65 MB capacity, and recorded zero queued requests, waits,
cancellations, or wait time. The detailed public profile scored 23,707.5
approximate and 137.3 exact vectors/query with 3.492-ms mean and 3.924-ms p95
server time. Live/restart peak RSS was 1.103 GB / 339 MB. The strict AFQD schema
is now version 1; numeric revisions 1--5 from earlier commits on this unreleased
branch are intentionally unsupported, while released legacy-index migration
remains governed by the separate public index lifecycle.

#### Search-view/MVCC qualification and mixed-workload baseline

The long-term publication work now gives every search a single immutable view
of the posting generation, publication identity, routing identity, and
storage/WAL bounds. Posting mutations publish append-only MVCC delta
generations; unchanged posting blobs and unchanged exact centroid-directory
blocks are retained rather than copied. Consolidation is bounded by delta
depth and bytes, and can collapse reader-free generations without duplicating
their payloads. Empty native generations bypass scan admission. Checkpoint
workers and their lifecycle tests use `std.Io`, with index shutdown ordered
before runtime shutdown and bounded observation deadlines.

The public API now also returns the source request identity captured under the
database search lease. Adapters no longer sample that identity independently
before entering the database, eliminating false retries when publication
advances between the two samples. This is part of the query linearization
contract rather than a benchmark accommodation.

The final fresh 50K qualification reached ready in 29.9782 seconds (19.6440
seconds insert plus 10.3342 seconds catch-up), with 0.9856 official recall and
239.9/986.6/1080.8/1147.1 QPS at concurrency 1/10/20/30. Corresponding p95
latencies were 4.77/17.85/41.64/67.91 ms. A detailed 1,000-query warm profile
reported 3.176-ms mean server latency, 23,959 approximate scores and 137.4
authoritative completions per query. Read-only cache-inclusive peak RSS was
1.733 GB and restart RSS was 337 MB.

This 50K result is in the desired 30-second load class and improves all four
throughput points over the immediately preceding governed sample
(225/930/1017/1029 QPS). It is not a new absolute serving or RSS record: the
best earlier samples reached approximately 294/1080/1192/1319 QPS with a
1.52-GB live RSS peak, while the lowest recent governed RSS sample was 1.103
GB. The current result therefore qualifies the correctness architecture and
load target, but does not supersede every isolated 50K high-water mark.

The final fresh 1M qualification reached ready in 767.8569 seconds (699.0891
seconds insert plus 68.7678 seconds catch-up) and retained 0.9902 official
recall. Its first live concurrency curve was invalid for performance comparison
because two unrelated Zig compiler processes each consumed roughly one CPU and
4.5 GB of RSS. After those processes exited, a restarted concurrency-only
repeat produced 59.0/596.1/642.4/577.2 QPS. A separate detailed warm profile
reported 9.733-ms mean server latency, 240,404 approximate scores and 146.5
authoritative completions per query, with 0.9927 recall. Read-only
cache-inclusive peak RSS was 5.064 GB; a repeated restart peaked at 2.245 GB.

The 1M result is only partially on par with the best experiments. Relative to
the best post-r126 result, readiness is 24.4 percent slower (767.9 versus 617.4
seconds), concurrency-20/30 throughput is approximately 19/30 percent lower
(642/577 versus 794/828 QPS), and mean warm server time is 9.8 percent higher
(9.73 versus 8.86 ms). It is close to r126 at concurrency 1 and 10 and preserves
recall parity, while improving read-only peak RSS by approximately 17.1 percent
from r126's actual 6.11 GB (corrected by the September 5 raw-result audit)
and by 28.2 percent from the post-r126 7.05-GB sample. This is a
memory/correctness win, not yet the overall performance winner.

Admission telemetry explains much of the high-concurrency gap. Concurrency 10
incurred no admission waits and reached 596 QPS. At concurrency 20 and 30, the
402.65-MB node budget admitted at most 12 searches and queued as many as 18.
The current conservative estimate charges roughly 33.5 MB per request by
multiplying the generation-wide maximum leaf size across the search width,
although the measured mean query scans about 240K candidates (roughly 23 MB of
RaBitQ codes). The next durable step is two-stage admission: acquire a small
routing permit, retain a compact immutable search-view token plus selected leaf
IDs, calculate the exact selected scan bytes, then acquire a scan permit before
allocating large scratch or entering the scan. Allocation capacity and memory-
bandwidth concurrency should be governed separately. This preserves the
worst-case memory invariant without throttling balanced generations according
to an impossible maximum-leaf-times-all-leaves product.

The new mixed public workload rewrites existing vectors under stable document
IDs while query workers run, so exact ground truth remains valid. At 50K,
10 query and four write workers sustained 121.8 QPS and 619 rows/s with 0.9740
recall; the post-write catch-up took 2.52 seconds and mixed peak RSS was 3.552
GB. At 1M they sustained 63.4 QPS and 330 rows/s with 0.9901 recall; catch-up
took 101.3 seconds and mixed peak RSS was 5.403 GB. Neither run produced a
request, capture, publication, or final-readiness error. These mixed RSS values
are deliberately reported separately from read-only serving RSS and are the
baseline for future ingest/query improvements, not evidence of a read-only
regression.

#### Route-first exact admission and dirty-leaf qualification

Native flat search now retains its immutable `SearchView`, routes before it
acquires scan bandwidth, sums authenticated physical costs for only the
selected postings, and then admits the candidate scan. The AFQD compact index
stores filtered and unfiltered costs beside each posting offset and checksum,
so this accounting does not fault or decode the corpus-sized payload before
admission. Empty and tree-fallback paths preserve their established behavior.

The first fresh 50K run with this protocol reached ready in 30.9174 seconds
(20.6682 seconds insert plus 10.2493 seconds catch-up), retained 0.9862 recall,
and produced 224.9/874.2/954.8/975.8 QPS at concurrency 1/10/20/30. Its p95
latencies were 5.06/24.23/68.88/82.47 ms. An unrelated Debug aggregate was
running on the same host, so this curve proves correctness and removal of the
old admission ceiling rather than establishing a new uncontended high-water
mark. Detailed warm serving averaged 3.724 ms in the server and completed only
137.4 authoritative vectors per query.

That run also exposed an MVCC edge: the first dirty posting lacked a compact
per-leaf cost and therefore inherited the generation-wide maximum. One mixed
query could be charged 214.70 MB despite touching ordinary leaves. Every
published delta now carries exact scalar scan costs for each changed leaf;
activation reconstructs the same map from immutable segments and the committed
WAL. Overlay consolidation merges those scalars newest-first without copying
posting payloads. The global maximum remains only a fail-safe for missing or
corrupt metadata.

On the already-dirty 50K corpus, the corrected run admitted all ten query
workers in 67.09 MB with zero waits. Relative to the faulty run, mixed server
p95 fell from 80.63 to 39.72 ms and successful write throughput rose from
568.1 to 1,057.2 rows/s; recall remained within normal mixed-update variance
(0.9739 versus 0.9768). This directly qualifies dirty-leaf accounting rather
than merely the immutable base.

A fresh 1M lifecycle preserved insertion time at 697.44 seconds and reached
ready in 800.21 seconds, with 0.9922 recall. It produced
83.1/557.7/602.3/609.4 QPS and 14.67/29.91/74.61/92.59-ms p95 at concurrency
1/10/20/30 on the still-contended host. Exact routing admitted up to 17
simultaneous scans, versus 12 under maximum-leaf accounting, and charged
395.34 MB of the 402.65-MB node budget at peak. The 6.33-GB cache-inclusive
RSS maximum occurred during final publication, about 71 seconds before query
serving began, so it is a publication/primary-store target rather than query
scratch demand.

The 1M mixed phase is not qualified. Both the initial process and a clean
restart reproduced four synchronized 30-second public write timeouts. On the
restart, all ten queries fit in 234.02 MB with zero admission waits and dense
catch-up finished each publication in at most 354 ms, yet cache-inclusive RSS
rose to 6.27 GB. Successful traffic reached 99.3 query QPS at 0.9899 recall and
363.9 write rows/s, with 29.19-ms mean and 37.84-ms p95 server query latency.
This isolates a separate primary-store/request scheduling or residency problem;
it must not be hidden by increasing the client timeout or described as an
admission failure.

#### Durable replay admission and asynchronous native authority

Stage profiling located the synchronized timeout precisely. Once the retained
derived backlog crossed 200 source sequences, a `sync_level=write` request ran
16 HBC replay sequences synchronously; one 100-row request spent 34.691 seconds
in `backlog_pressure` while its primary mutation took only milliseconds. The
threshold therefore coupled response latency to corpus-derived work and did not
provide a real memory bound.

Derived replay now reserves its exact encoded payload plus queue-entry overhead
from the node resource manager before primary commit. The reservation transfers
to the durable sequence after commit without allocating. Exhaustion fails
before the primary mutation with explicit retryable HTTP 429 backpressure;
successful `write` requests only notify the independent replay worker. Source
sequence identity is included in batch profiles, and query/write activity has
separate maintenance-deferral leases. Coverage-only and non-batch mutation
paths use the same precommit contract, so no producer can bypass the bound with
zero-byte records or a best-effort postcommit accounting call.

Native readiness is now distinct from physical consolidation. A certified
vector base plus its complete WAL/delta suffix is authoritative without a
million-vector rewrite. The initial empty bootstrap still requires a
cardinality-certified base. Likewise, stable-tip posting validation publishes
the applied watermark without flattening. A retained-generation posting
compactor stages files asynchronously and may later preserve the concurrent WAL
tail in an atomic publication; neither `runUntilIdle` nor `full_index` joins or
cancels that build while holding the index apply fence.

The resumed 50K mixed qualification (10 query workers, four write workers,
100-row public batches) sustained 136.9 query QPS and 1,426.9 rows/s at 0.9762
recall. Client query p50/p95/p99 were 69.68/100.32/151.87 ms; server p95 was
32.47 ms. Catch-up completed in 2.72 seconds with no errors. Server-side batch
work averaged 28.1 ms with a 50-ms p95. Cache-inclusive RSS peaked at 2.78 GiB,
versus 3.552 GB in the earlier dirty 50K baseline.

At 1M, the first decoupled run eliminated request timeouts but exposed a second
coupling: stable-tip readiness rewrote the complete vector and posting bases and
timed out at 120 seconds. Reordering the already-existing exact-overlay proof
reduced catch-up to 5.81 seconds in a short, heavily contended diagnostic. A
subsequent run then reproduced the remaining posting bug directly: two 1.50-GB
full checkpoints under the apply fence produced 24--64-second batch times.

After removing foreground replay pressure and the vector-base rewrite, but
before removing the final quiescent posting join, a 60-second stress phase
completed 638 public write batches with server-side batch p95/p99 of 50/75 ms
and no backlog waits or request errors while sustaining 183.5 query QPS, 1,061
rows/s, and 0.9905 recall. Server query p95/p99 were 33.59/42.77 ms. That run
then joined the quiescent full checkpoint and took 117.6 seconds to become
idle, serving as the negative control.

The final 30-second run used fully asynchronous quiescent publication. It
sustained 174.9 query QPS and 1,397.7 rows/s at 0.9920 recall with no errors.
Server query p50/p95/p99 were 17.37/21.79/29.15 ms; server batch p95/p99 were
39/71 ms. No full checkpoint was started or published by the lifecycle fence,
and catch-up completed in 57.39 seconds while draining 421 asynchronous replay
batches. This is a correctness and latency qualification, not an uncontended
hardware record: other worktrees were compiling on the same host.

The remaining regression is residency, not managed HBC heap. Final HBC cache
accounting was only 35.6 MiB with zero vector-cache bytes, but cache-inclusive
RSS peaked at 6.01 GiB. The next memory work must attribute primary
LSM and mmap/file-cache residency rather than shrinking already-bounded query
scratch. The 57-second replay drain also shows that stable-tip work should
coalesce adjacent dirty sequences into larger bounded transactions; it must not
restore synchronous foreground pressure.

#### Bounded WAL recovery and lifecycle-only publication

Review of the asynchronous handoff found two lifecycle holes that do not
appear in a clean benchmark run. First, the posting WAL's independent format
limit was treated as fatal by the derived worker even though a completed or
restartable checkpoint can release that capacity. Second, native authority
could become durable while an asynchronous builder still borrowed the legacy
LSM; the blocked detach was then forgotten until another mutation or restart.

`PostingWalTooLarge` is now explicit recoverable backpressure with bounded
worker backoff. The rejection path promotes the current builder, or starts a
full immutable builder when a prior failure left none. The mandatory
maintenance lane publishes only builders whose corpus work is already
complete, before optional foreground-pressure deferral. It never joins the
builder or performs a corpus scan under the per-index apply fence. Native
authority records legacy retirement as pending and drains it at the first safe
publication/capture boundary, so the compatibility LSM cannot remain resident
indefinitely.

The quiescent recursive topology experiment was removed from production
maintenance and from the qualification script. Its clustering implementation
interleaves corpus CPU work with live-tree writes, so a resource reservation
does not make it safe to execute under the mutation fence. A future topology
optimizer must construct a complete immutable `TopologyPlan` from a pinned
search/vector generation outside that fence, then publish with an epoch/CURRENT
compare-and-swap. Until that design exists, the qualified native layout keeps
the existing topology rather than exposing a disabled-by-default path capable
of multi-second foreground stalls.

The framed V16 runtime-status rollout also requires a two-phase bootstrap.
Advertising the dense-native capability in a data node's initial store record
made metadata reject that record until V16 activation, while the absent store
could not progress to its normal capability heartbeat. Initial registration
now uses the strongest already-activated status profile. Once V16 is durable,
the ordinary status report upgrades the store capability and the independent
dense-native cluster floor can activate. Native authority remains disabled
until that second floor is committed; no readiness or migration fact is
downgraded to make an old protocol appear safe.

Back-to-back public index creation exposed a separate dependency inversion.
The shared exact-vector file is table-wide, while native posting authority and
repair admission are index-generation scoped. A newly admitted external index
can therefore have its exact artifact durably present while its fail-closed
posting repair still has an empty active generation. Broad startup maintenance
was validating the table-wide file against that deliberately stale posting
count, publishing the file, returning
`VectorBlockPublishedGenerationNotReady`, and retrying before the only owner
capable of fixing the count was scheduled.

Projection maintenance now skips only repair generations whose serving gate is
blocked. Their durable repair owner builds and atomically activates postings;
the next ordinary projection pass then certifies the index against the already
available shared exact plane. Healthy indexes and operator rebuilds that retain
a serviceable generation are unchanged. Public readiness likewise derives
from each index's immutable posting/vector generation token rather than the
table-wide `dense_projection_finalizing` worker flag. The latter remains useful
aggregate telemetry, but can no longer make every sibling index report 99.9%
while unrelated projection work is active. The public-API regression with two
back-to-back external indexes, an immediate `full_index` batch, readiness, and
queries now completes in 4.43 seconds in Debug.

The same regression then exposed a node-wide admission issue on large or
cache-backed volumes. A raw five-percent disk reserve scales to about 46 GiB
on a 926 GiB filesystem; with 35 GiB actually free it rejected a fully
accounted 320 MiB shadow repair forever, even though the repair was the only
owner capable of restoring query service. The disk governor now keeps
`max(1 GiB, min(5% of capacity, 16 GiB))` as additional emergency durability
headroom. Candidate, replay, and cleanup bytes remain separately and fully
reserved, so this removes pathological proportional slack rather than
weakening repair accounting. The preserved public-API regression completes in
3.79 seconds in Debug on that below-five-percent-free host after merging the
current `origin/main`. The distributed non-host status/heartbeat regression
also completes in 3.93 seconds, confirming the merged runtime-status rollout.

#### Review follow-up: committed checkpoint identity and online acceleration (2026-09-05)

WAL capacity recovery exposed a mixed-generation checkpoint hazard. A rejected
source capture had already changed the working root/count when it started the
mandatory full checkpoint, but the builder retained the preceding committed
posting generation. Builders now decode metadata from that retained generation,
so metadata, postings, source coverage, and WAL prefix have one identity. The
regression rejects an actual insertion, publishes the recovery checkpoint,
reopens it, and successfully retries the insertion. Only pre-I/O capacity
rejections retain the valid WAL writer; ambiguous failures still reopen and
recover. This also removes full-WAL reparsing on each capacity retry.

Post-repair acceleration and recurring vector publication now use an explicit
catalog-lifetime pin, not an exclusive or shared primary apply lease. A shared
apply lease was insufficient: primary commits are exclusive, as the real-write
race regression demonstrated. Immutable vector staging retains its pinned
primary snapshot and preserves/revalidates native WAL suffixes; it does not
drain dirty postings or flatten an unrelated HBC generation. Structural catalog
retirement still drains the lifetime pin. Pending native acceleration no longer
selects broad group-exclusive recovery for a resident writer. Recurring passes
retain the source-tip debounce, and already-ready indexes do no staging work.

Tests cover write admission during staging, a real source commit racing the
snapshot, fail-closed stale publication followed by successful retry, unchanged
posting debt, structural retirement, and a zero-rebuild steady-state pass. The
multi-source cardinality regression now includes a complete outcome tuple so
the old missing-tuple fallback cannot make the test pass accidentally. These
regressions are included in the default dense lifecycle CI lane.

The qualification harness now records binary SHA-256 and tracked-diff SHA-256
alongside HEAD, making uncommitted-worktree measurements identifiable.

The first fresh ReleaseFast 50K run is a rejected performance experiment, not
qualification: `/private/tmp/vdbbench-pr593-online-publication-50k-20260905`,
binary `502caae2d0c909dcd32899c6b06438b48a05202f030615c0a93dc97015a7c2d3`.
It loaded in 31.5340 seconds (10.6536 insert + 20.8804 catch-up), but delivered
only 165.3/187.3/167.3/158.5 QPS at concurrency 1/10/20/30, with
7.64/82.13/189.81/309.52-ms p95 and 0.9834 recall. The upload remained in a
40-MB posting WAL/overlay above an empty base: vector-file readiness had
incorrectly stood in for initial posting-local scan-plane materialization.
The scan governor admitted at most four queries and recorded 17,019 waits;
raising its budget would conceal the missing layout rather than fix it.
Live RSS peaked at 3.248 GB including the separately sampled mixed phase;
restart RSS peaked at 366 MB. Mixed traffic had no request errors, 95.2 QPS,
0.97074 recall (versus 0.97400 in the comparable earlier mixed profile), and
2.51-second catch-up. Debug compilation overlapped the latter diagnostic
phases after the concurrency regression was established.

The correction makes initial posting acceleration an explicit one-time
publication requirement for non-empty v2 indexes. An empty native index still
has the zero-work path. The first non-empty posting base stages asynchronously
under a catalog lifetime pin, receives governed progress, and publishes at a
later non-blocking pass. Established bases remain ready through normal
WAL/delta changes; this does not restore full compaction on every write.
Resident broad catch-up now uses the same online handoff as repair, avoiding
stable-tip mutation attempts during active source sessions.

The corrected 50K read-only sweep completed at
`/private/tmp/vdbbench-pr593-native-bootstrap-50k-20260905`, binary
`710db5a76bdd4ba0b0fd152dd0b87cc167e76b97ec8a33b02dac8699275c2034`.
The public API workload remains batch 100, four insert workers, concurrency
1/10/20/30 for 30 seconds each, and unchanged boundary reranking. Comparison
is against the recent same-phase `vdbbench-searchview-final-50k-c-20260904`
run, not the failed candidate or a different mixed/restart workload:

| Metric | Recent baseline | Rejected candidate | Corrected candidate |
| --- | --- | --- | --- |
| Ready seconds | 29.98 | 31.53 | 35.36 |
| Recall | 0.9856 | 0.9834 | 0.9864 |
| QPS c1/c10/c20/c30 | 240/987/1081/1147 | 165/187/167/158 | 261/1065/1094/1173 |
| p95 ms c1/c10/c20/c30 | 4.77/17.85/41.64/67.91 | 7.64/82.13/189.81/309.52 | 4.28/16.12/47.48/71.95 |

The catastrophic concurrency regression is recovered, but this is not an
across-the-board performance win: corrected c20/c30 p95 remains 14.0%/5.9%
higher and readiness takes 18.0% longer than the recent baseline. Single-run
variation is not evidence that these remaining differences are harmless.
Debug lifecycle (51 tests) and public API parity (190 tests) passed before this
run, with zero failures or leaks.

The full harness completed successfully, but the performance gate is not a
clean pass. Mixed traffic improved to 151.5 QPS (baseline 121.8), query p95
121.4 ms (147.2), and 913 written rows/s (619), with no request errors and
recall 0.97722 (0.97400). Catch-up took 3.77 seconds (2.52). Sampled RSS for
the load/read-only phase peaked at 2.121 GB (baseline 1.733 GB), and the mixed
phase at 4.333 GB (3.552 GB). These are cache-inclusive RSS, not attributable
demand; the final footprint sample attributed 1.325 GB of demand. Mixed runs
are fixed-duration and completed different write volumes (27,600 versus
18,700 rows), so their RSS is not an equal-work comparison. The load/read-only
RSS increase still needs investigation independently of that difference.

Restart cold/warm serial p95 was 7.5/7.2 ms versus 6.6/4.2 ms, with recall
0.9829 versus 0.9853 and restart RSS 488 MB versus 337 MB. Restart follows
mixed writes, so the pending/consolidated layout must be inspected before
attributing the difference to restart itself. Corrected 1M qualification
remains pending while the residual tail-latency and memory regressions are
investigated; the recent baseline is not being revised upward.

## Next checks

1. Coalesce adjacent stable-tip replay sequences into larger bounded HBC WAL
   transactions. Preserve source-sequence ordering, capture identity, and the
   precommitted backlog reservation while reducing the final 421-batch/57-second
   drain. Never put this work back on `sync_level=write`.
2. Separate allocation-byte admission from memory-bandwidth task admission.
   Exact selected-block bandwidth is now implemented for clean and dirty native
   generations. Add an independently measured scratch/allocation permit and
   tune task concurrency from globally observed service time/queue pressure so
   high-concurrency throughput cannot buy an unbounded RSS increase.
3. Profile the 1M insertion delta versus r126, including source LSM pressure,
   posting drain cadence, WAL fsync, staged-generation writes, and final
   publication. One full checkpoint first returned `Unsupported` at sequence
   7975 and succeeded at sequence 8073; reject unsupported work before staging
   it so retries cannot consume load-path bandwidth.
4. Narrow broad persisted L0 source ranges with adaptive, workload-independent
   range slices, then pair them with a lower soft compaction-input budget.
   Merely splitting leveled output files does not split the fixed-point overlap
   closure. Preserve the oversized progress escape for a minimum indivisible
   closure and verify that the extra L0 runs do not amplify write pressure.
5. Design bounded or incrementally publishable HBC leaf normalization inside a
   single replay transaction. Smaller replay windows and naïve internal apply
   chunks both regressed 50K throughput, so preserve source-write ordering,
   rollback semantics, and replay amortization.
6. Focus remaining snapshot-copy work on current scans, which accounted for
   127.6 MB of the 150.7 MB live aggregate in the external-admission run;
   bound-read copies were 23.1 MB. Do not replace multi-operation snapshots
   with unsafe live probes merely to improve a cumulative counter.
7. Repeat the final 128-shard/64-KiB-buffer float16 50K and 1M lifecycle three
   times through the post-phase-sampled, read-only-restart harness on a
   controlled host and publish mean plus range. The uninterrupted r125 result
   now qualifies correctness and the 3.860 GB RSS bound; repetition is for
   variance and precise throughput attribution, not to complete the lifecycle.
8. Add deterministic fault injection at every posting-WAL append, fsync,
   checkpoint staging, `CURRENT` replacement, overlay allocation, and applied
   watermark boundary. The production ordering and fail-closed recovery paths
   now exist; exhaustive crash-matrix automation remains the release gate.
9. Add the public catalog/API lifecycle for promoting index-managed immutable
   source artifacts to shared artifacts, with generation-fenced references,
   compatibility validation, and deletion-safe reference accounting. Do not
   share index-specific HBC topology/posting generations.
10. Reduce the remaining primary document/artifact contention. The final 1M
   load still accumulated 3.565 GB of mutable snapshot copies and 1.936 GB of
   read-snapshot rotations. Attribute those copies by reader class and compare
   four public workers against a current serial control before changing the
   benchmark's concurrency.
11. Reduce the native posting chain's 448 MB disk footprint and query-time mmap
   residency without weakening `covered_source_sequence`, atomic CURRENT
   publication, generation leases, or boundary rerank. Prefer indexed
   patch-native/chunked-copy-on-write deltas and measure recovery time as well
   as bytes.
12. Optimize the exact flat centroid scan itself—SIMD/block layout, cache
   residency, and bounded parallel scoring—while keeping the demonstrated
   0.990 recall. RaBitQ routing's 1.55-percentage-point loss is outside the
   parity budget and must not become the default merely for latency.
13. Compare the qualified 128-shard curve (0.9903 recall, 267 peak QPS) against
    current Circus competitors using identical corpus, payload, recall, and
    concurrency semantics. Report cold and warm separately; never mix source
    mutation or maintenance into the first concurrency sample.
14. Audit the remaining gap between roughly 595 MB attributable warm-restart
    demand and 2.86 GB cache-inclusive RSS. Classify mmap residency, allocator
    arenas, primary run/index pages, cache leases, and runtime stacks before
    changing budgets; reclaimable file cache must not be mislabeled as demand.
15. Move candidate-page admission under an explicit resource-manager budget.
    The cold r125 concurrency-1 wave was 14.13 QPS while a reopened pass reached
    46.55 QPS. Split per-leaf authentication between compact RaBitQ and wide
    float16/residual planes, prefetch only the routed shell, and evict whole
    pages by generation. Do not restore unbounded mmap warmup merely to hide
    cold latency.
16. Reduce the remaining 3.60 GiB live high-water in primary/vector
    consolidation. The posting flatten no longer raises the peak; prioritize
    bounded vector-block source reads and primary LSM closure/output windows,
    preserving crash-safe final-shard publication and progress on an
    indivisible compaction closure.
17. Bound online HBC tree height under adversarially ordered vectors. The Debug
    aggregate's branch-local `flat traversal does not treat a full candidate
    heap as a pruning proof` fixture inserts 128 collinear inner-product vectors
    in increasing order and entered a deeply recursive internal split/range
    recomputation path, consuming about 1.5 GiB before the diagnostic run was
    stopped. Keep that adversarial coverage: add a maximum-height/balanced-split
    invariant or convert an over-deep online subtree through the recursive bulk
    builder. Do not make the test faster by hiding the production-shaped order.

## 2026-09-05: audited historical comparison baselines

A separate read-only agent audited the findings against saved raw results,
configs, RSS series, and server/client logs. No single historical run wins
every metric. These candidates completed fresh public load, queries, and
restart; none included the later mixed-ingest/query qualification. They are
performance references, not retrospective certification of every new
correctness invariant.

Common contract: public HTTP, one shard, cosine, k=100, batch 100, four load
workers, native HBC plus float16 vector blocks, default effort, no full-text
index, 30-second concurrency phases. RSS below is decimal GB, cache-inclusive
load/live-query peak from 0.2-second samples. Do not combine different runs'
best cells into a fictional baseline.

| Candidate | Insert + catch-up = ready seconds | Recall | QPS c1/c10/c20/c30 | p95 ms c1/c10/c20/c30 | Live/restart RSS GB |
| --- | --- | --- | --- | --- | --- |
| 50K scratchpool | 18.9887 + 11.3743 = 30.3630 | .9885 | 293.82/1079.76/1192.00/1319.31 | 3.878/17.034/49.380/71.274 | 1.5185/.3413 |
| 50K row-block B | 15.2988 + 9.0817 = 24.3805 | .9857 | 283.59/1084.20/1203.19/1245.37 | 3.988/15.323/32.868/58.690 | 2.0763/.3397 |
| 50K admission + cold publication | 20.4414 + 10.0498 = 30.4912 | .9862 | 273.64/1109.24/1198.41/1229.03 | 4.190/14.763/34.461/59.555 | 1.8759/.3453 |
| 1M regression-recovery | 548.9080 + 68.4582 = 617.3662 | .9925 | 78.13/633.96/794.07/828.00 | 12.899/24.716/55.769/94.317 | 7.0534/2.3907 |
| 1M admission + cold publication | 634.6249 + 89.6295 = 724.2544 | .9924 | 70.74/626.82/701.63/649.57 | 13.853/24.678/66.729/87.476 | 6.2579/2.3744 |
| 1M kind-separated V2 | 630.0084 + 86.4541 = 716.4625 | .9927 | 58.48/568.41/596.16/584.99 | 15.050/27.958/78.993/105.613 | 5.7328/2.3748 |

Use both 50K rows: scratchpool is the throughput/RSS reference, while row B
is the faster-load and lower-tail-latency reference. Use regression-recovery
as the primary raw-backed 1M speed reference; its 828 QPS comes with 7.05 GB
RSS. Kind-separated V2 is a lower-memory alternative, not equal-speed proof.
The initial audit omitted admission/cold publication; the follow-up raw-result
check restores it as the 1M concurrency-30 tail-latency/balanced reference.
Against regression-recovery it trades about 21.5% throughput and 17.3% longer
load for 7.3% lower c30 p95 and 11.3% lower live RSS, with 0.01 pp recall
difference. Neither dominates the other. Its fixed-cap-16 prototype's
730.3 QPS / 85.7-ms p95 is a separate existing-generation query experiment;
do not attach the production run's load time or RSS to that sample. Its
restarted reverse-order curve likewise remains separate: c30 665.97 QPS /
82.718-ms p95. These results do not establish mixed-ingest/query parity.
Scratchpool had another repository workload active; row B is documented as
controlled; regression-recovery as clean; kind-separated V2 was contended by
compilers/test servers. Repeat reconstructed A/Bs before causal attribution.

Raw bundles, each containing qualification-summary.json and original results:

- `/private/tmp/vdbbench-scratchpool-50k-20260903/`; recorded HEAD
  `d43e41083f1ef1caa42b6d470580e7fcd12f96b9`, explicit flat_exact routing.
  Online result `result_20260903_41780a9cf0ec421eb22c24656bb78146_antfly.json`.
  Cold/warm restart p95 4.0/4.0 ms; post-phase demand 963,274,880 bytes
  (one sample); saved table disk usage 801,127,550 bytes.
- `/private/tmp/vdbbench-directory-v2-row-50k-20260903-b/`; recorded HEAD
  `af77c6f2416e0a342371a88258c812ffa1aca7da`.
  Online result `result_20260903_9b0ee2cbe3d540de976621aa93671585_antfly.json`.
  Cold/warm restart p95 4.3/4.1 ms; post-phase demand 517,100,000 bytes
  (one sample); final total-disk evidence unavailable.
- `/private/tmp/vdbbench-pr593-regression-recovery-768d-1m-20260903-a/`;
  recorded HEAD `18bb0479fda02925b3cad0899321ddedc9e0d947`.
  Online result `result_20260903_c73140b696ee4085ac03ba8ec7957f64_antfly.json`.
  Cold/warm restart p95 12.0/11.0 ms; warm server mean/p95 8.858/9.524 ms.
  Post-phase demand 913,000,000 bytes (one sample); saved table disk usage
  8,330,825,396 bytes, distinct from filesystem allocated space.
- `/private/tmp/vdbbench-v2-directory-final-768d-1m-20260903-a/`;
  recorded HEAD `22914f799d32d74c9fcf7a5422c6c7f555a0a863`.
  Online result `result_20260903_13933def6c9f42eb83c75e51c2e63743_antfly.json`.
  Post-phase demand 1.90 GB (one sample).
- `/private/tmp/vdbbench-directory-v2-admission-coldproj-50k-20260903-a/`;
  online result `result_20260903_d96b1be36c944358ab64666f59a1a1bb_antfly.json`.
- `/private/tmp/vdbbench-directory-v2-admission-coldproj-1m-20260903-a/`;
  online result `result_20260903_aaa3c4abbc154a0c887fc5ebb3bf8f7f_antfly.json`.
  Live peak RSS 6,257,868,800 bytes. Separate restarted reverse-order result
  `result_20260903_e0dd8436fa8f45ae80ff3bc665c431ec_antfly.json`.
  Follow-up inspection found none of the failure signatures below, nor
  `error:`, in the original live/framework/initial/reopened logs.

Older configs did not record binary SHA or dirty-tree fingerprints. Recorded
HEAD alone does not identify the exact measured executable. New comparisons
must record both binary and source-state identities. The audit found no
BrokenProcessPool, Traceback, ResourceBudgetExceeded, or posting-store-unavailable
errors in the four inspected runs' framework/initial/reopened logs.

### Corrections to earlier r126 references

The raw `/private/tmp/vdbbench-r126-ab-768d-1m-20260902/` live RSS peak is
6,111,182,848 bytes (6.11 GB), not 5.73 GB. Earlier sections incorrectly
attributed the kind-separated V2 RSS to r126. Its original live sweep only
completed c1/c10/c20: c30 failed with BrokenProcessPool in vdbbench-live.log
lines 149-162, despite the harness writing a NORMAL result. The cited c30
673.5611 QPS / 92.880-ms p95 came from a separate restarted retry. It must
not be combined into an uninterrupted live qualification curve.

r125's 775.94-s / 3.860-GB 1M result remains a documented lower-memory target;
its raw bundle was not located by this audit. Its documented original live
c30 QPS was 483.43, versus 561.32 on restart—not 828 QPS at 3.86 GB.

The newer mixed references remain separate:
`vdbbench-searchview-final-50k-c-20260904` (121.8 query QPS, 619 rows/s,
2.52-s catch-up, 3.552-GB mixed RSS) and
`vdbbench-searchview-final-1m-20260904` (63.4 query QPS, 330 rows/s,
101.3-s catch-up, 5.403-GB mixed RSS). Recover the stronger historical
read-only frontier while also passing these newer durability/mixed gates.

### Current 1M candidate failed qualification

`/private/tmp/vdbbench-pr593-verified-rows-1m-20260905/` finished inserting
1M primary rows in 262.59 s, but did not reach ready. At applied sequence
6126 (612,500 indexed vectors; target sequence 10001), installing an already
durable HBC checkpoint failed with ResourceBudgetExceeded, followed by
PostingWalMutationStoreUnavailable. The run was stopped and its data/logs
preserved. Sampled RSS reached 4,870,438,912 bytes before stop; this is an
incomplete-run value, not a comparable qualified 1M memory result. No query,
mixed, or restart performance qualification was completed. The insertion
time must not be represented as a load-to-ready improvement.

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
`zig build lib-vectorindex-test -Doptimize=ReleaseFast -- 'fused native candidate scoring microbenchmark'`.

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
