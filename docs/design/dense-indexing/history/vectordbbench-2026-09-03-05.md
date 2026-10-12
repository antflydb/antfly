# VectorDBBench Findings: Bounded Publication, r129, and V2 Directory Experiments (2026-09-03 – 2026-09-05)

> Relocated verbatim from `zig/VECTORDBBENCH_FINDINGS.md` (lines 313–496, 4148–4190 and 4275–5420 at commit 3ce5aff4ea) on 2026-10-12. Durable decisions were folded into [VECTORDBBENCH_FINDINGS.md](../../../../zig/VECTORDBBENCH_FINDINGS.md) before the move. The "V1 mirror versus native-v2 authority" section (lines 4197–4274) moved to [DENSE_INDEXING_LIFECYCLE.md](../../../../zig/DENSE_INDEXING_LIFECYCLE.md#physical-index-versioning-and-rolling-upgrades).
>
> Relative links were re-based to this file's location. Paths under `.benchmark-results/` refer to untracked local benchmark output at the repository root; `/private/tmp/...` paths name ephemeral local run directories that may no longer exist.

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

