# Lite throughput profile (post WS1/WS3/WS4/WS5/WS7)

Status: measurement + fix-recommendations pass on `perf/lwa-throughput`
(base `perf/lwa-integration` 7ee2011df3), after the sync/byte-amplification
workstreams in [`lite-single-commit-write-path.md`](lite-single-commit-write-path.md)
landed. Companion to that doc's §5 acceptance gates: this is the throughput
side (`scripts/bench/lite_sqlite_comparison.py`, a ReleaseFast `libantfly`)
plus a CPU/syscall breakdown (`sample`) and the byte/sync counts from
`zig build lite-write-amplification` (Debug), run on this laptop
(wolfspider-class M-series host, **not quiet**: load average 9-25 throughout,
7+ other agents' Debug builds and this machine's other work competing for
CPU the whole time). Absolute rates are noisy; the within-round
SQLite-vs-Lite **ratio** and the syscall-level CPU breakdown are the
signal, per the task's own framing.

## 1. Headline findings

1. **A reader-epoch namespace directory is opened, scanned and closed from
   scratch on every single write**, even for a primary-only workload with
   zero concurrent readers. In a 10-second `sample` of one writer doing
   primary-only inserts, syscalls attributable to
   `reader_epochs.zig`'s `Namespace.open` (called once per write from
   `native.zig`'s `acquireEpochReuseLock`, itself called from
   `pageAllocatorFromFreeMapWithProof` inside `Store.stage`) account for
   **46% of all foreground samples on the calling thread** — more than the
   actual commit I/O (fsync + pwritev + preadv = 24%) combined. This is the
   single largest lever found this round. See §4.1 and §5 item 1.
2. **A full-text visibility correctness bug**: a sustained single-writer
   `sync_level: full_index` run (40s, ~3,968 indexed writes before this
   point in an adjacent shorter run, ~7,534 cumulative when it triggered)
   hit `assert search_count(latest) == 1` failing after `run_until_idle()`
   on reopen — i.e. the most recently acknowledged indexed write was not
   found by full-text search even after an explicit catch-up. This did
   **not** reproduce in two shorter (20s and 15s) runs at the same
   concurrency. See §6 — likely a merge/compaction-triggered race in the
   indexed acknowledgement path (`native_catalog`/`mutation_commit.zig`
   territory); flagged for the owning agents, not fixed here.
3. At 8 writers, lock/lease waits (not CPU) dominate: in an 8-second sample
   of one of 8 concurrent writer processes doing primary-only inserts,
   `__ulock_wait2` + `flock` + `__psynch_cvwait` + `__semwait_signal`
   account for the large majority of samples, syscalls that are part of
   actual work (`__openat`, `fsync`) drop to single-digit percentages. This
   is the writer-slot/cross-process path lease contention the design doc
   already names as WS2/WS3 territory.
4. Lite is now **much closer** to SQLite than the historical baseline, but
   still 13-42x slower depending on mode/concurrency (vs. 24-244x at the
   original baseline) — the sync/byte work landed has clearly helped, but
   the acceptance gate (Lite ≥ SQLite on the same host) is not met in any
   mode yet.

## 2. Throughput vs SQLite (ReleaseFast, this host, same run)

`scripts/bench/lite_sqlite_comparison.py --seconds 20 --workers 1 8 --rounds 2`,
library built from 7ee2011df3 with `-Doptimize=ReleaseFast`
(`/tmp/lwa-throughput-build/lib/libantfly-releasefast-7ee2011df3.dylib`,
sha256 recorded in the receipt). Full receipt:
[`sqlite-comparison-before.json`](sqlite-comparison-before.json).

| Mode | Workers | Antfly (w/s) | SQLite (w/s) | Ratio (SQLite/Antfly) | Load avg at sample |
|---|---|---|---|---|---|
| Primary only | 1 | 678 / 746 | 11,756 / 9,883 | 17.3x / 13.3x | 15.0-16.9 |
| Primary only | 8 | 490 / 236 | 9,859 / 9,796 | 20.1x / 41.6x | 12.4-15.2 |
| Full-text visible | 1 | 151 / 158 | 3,352 / 2,373 | 22.2x / 15.1x | 13.8-16.0 |
| Full-text visible | 8 | 97 / 78 | 3,594 / 2,926 | 37.0x / 37.7x | 14.7-15.7 |

(Two values per cell are the two alternating-order rounds.) Against the
historical wolfspider baseline in `lite-single-commit-write-path.md` (ratio
23.7x / 36.5x primary, 111x / 244x indexed), the indexed-mode ratio
improved roughly **5-6x** from WS4's native_catalog backend and the earlier
WS1/WS5/WS7 sync reductions; primary-mode ratio improved more modestly
(WS1's slot/manifest change removed one sync but the manifest+dedicated
slot pages added bytes back, see §3). SQLite itself showed nonzero
`busy_calls` (`qualification_passed: false`) in two of the eight-writer
rounds — a host-load artifact (SQLite's own `busy_timeout` getting
starved by CPU contention from other agents' builds), not a Lite defect;
noted via the `compiler_contamination_samples`/`pre_run_compiler_processes`
fields this round's tooling fix added (see commit list).

None of the four modes meet the §5 acceptance gate (Lite throughput ≥
SQLite on the same host); all are still 13-42x behind.

## 3. Sync and byte counts (Debug, build-independent)

`zig build lite-write-amplification` at 7ee2011df3. Full receipt saved as
[`write-amplification-current.json`](write-amplification-current.json).

| Metric | Baseline (doc) | After WS1/WS5/WS7 (before WS4) | After WS4 (receipt) | **Current (this run)** | Target (§5) |
|---|---|---|---|---|---|
| Primary calling-thread syncs/update | 3.0 | — | 2.0 | **2.0** | ≤1.0 (1w) / ≤0.25 (8w) |
| Primary calling-thread bytes/update | ~60 KB | — | ~45.0 KB | **~47.9 KB** | ≤40 KB (1w) / ≤12 KB (8w) |
| Indexed calling-thread syncs/update | 36.4 (orig), then 34.6 | 34.6 | 6.22 | **6.22** | ≤1.0 (1w) / ≤0.3 (8w) |
| Indexed calling-thread bytes/update | ~393 KB | 423.6 KB | 101.1 KB | **110.8 KB (1w) / 112.0 KB (8w)** | ≤64 KB |
| Primary-only background turns/leases/refreshes | 418/1,612/418 | — | 0/0/0 | **0/0/0** | 0/0/0 |

Notes:
- The indexed sync count is unchanged from the WS4 receipt (6.22, both 1
  and 8 writers — the harness runs the same total op count regardless of
  writer count, per its own documented limitation; it demonstrates op
  counts, not per-writer contention).
- Indexed bytes/update rose slightly from the WS4 receipt (101.1 KB -> 110.8
  KB) and primary bytes/update rose slightly too (~45.0 KB -> ~47.9 KB).
  Both are consistent with WS1's v8 protocol landing *after* that WS4
  receipt was taken: two dedicated 4 KiB slot pages (vs. shared sector 0)
  plus a per-commit manifest page are a real, designed bytes-for-safety
  trade against the old 3-fsync/shared-sector protocol, not a regression.
  Flagging only so the WS1 owner has the number; not asking for a change.
- Primary-only background work already meets the 0/0/0 target on the three
  metrics this harness can observe through its lower-level test seam
  (`capi.openLiteHandleWithRuntime`, bypassing `capi/lite_connection.zig`'s
  `Owner`/pool layer — see the harness README's own caveat). The large
  "other" background bucket (1,090 syncs / 6.9 MB / 545 commits over 128
  primary-only updates) is unattributed background work the harness's
  known_limitations already flags as useful follow-up instrumentation, not
  a new finding from this pass.
- The counting harness has no primary-only 8-writer mode, so primary bytes
  at 8 writers could not be measured this way; `peak_file_bytes` growth in
  the SQLite-comparison receipt is the only available proxy and isn't
  directly comparable to a per-update figure.

None of the sync/byte gates are met yet either, though the indexed gates
are now within ~2x-6x of target versus ~6x-10x+ before WS4.

## 4. Per-layer CPU/syscall breakdown (`sample`, ReleaseFast)

### 4.1 Primary-only, 1 writer (10s sample of the writer process)

Foreground (calling-thread) leaf samples only — two genuinely idle Zig
background/IO-pool threads parked the entire window in
`__psynch_cvwait`/`__ulock_wait2` were excluded as noise, not cost (they
account for essentially all of those two syscalls' raw counts; see
`sample-primary-1w.txt`).

| Category | Evidence (leaf syscalls / functions) | Samples | Share of foreground |
|---|---|---|---|
| **Reader-epoch namespace scan** (open/stat/scan/close/unlink the `.aflite-readers-<hash>` directory, once per write) | `__openat` 1813, `__close_nocancel` 330, `__unlinkat` 327, `fstat` 65, `flock` 67, `__getdirentries64` 44, `__getattrlist` 52, `faccessat` 9, `__fcntl` 18 | **2,725** | **46.4%** |
| Commit I/O (fsync + page write/read) | `fsync` 936, `pwritev` 376, `preadv` 102 | 1,414 | 24.1% |
| Primary B-tree (`IndexEditor.put/insert/flush/load/advance`, page decode/encode) | `storage.lite.native.IndexEditor.*` 1,604, `decodeDocumentIndexNode` 100, `encodeDocumentIndexNode` 38 | 1,742 | *(overlaps CPU bucket below; shown for attribution)* |
| Allocator ledger (incremental digest/fingerprint over the free-page set) | `allocator_v4.State.incrementalFingerprint` 84, `.pendingValueDigest` 48, `.checkpointStep` 31, `.complete` 24, `.counterValueDigest` 15, `.set` 14, `.retireCollected` 11, others | 252 | *(subset of CPU bucket)* |
| Hashing/CRC (page CRC + reader-epoch/allocator digests, BLAKE3) | `arm_crc.update` 136, `crypto.blake3.*` ~208, `mem.order` 142 | ~486 | *(subset of CPU bucket)* |
| Remaining in-library CPU (arena alloc, the above B-tree/allocator/hash work, misc) | — | 1,734 | 29.5% |

The capi entry (`antfly_db_batch_json` -> `trackedBatchWithWriterLaneHook`
-> ... -> `batchInternalPreparedOwned` -> `DocStore.putBatchWithReplayStagedAndParticipant`
-> `Txn.commitStaged` -> `Store.stage`) itself showed no measurable JSON-parse
or capi-marshalling cost in the leaf breakdown at this sample resolution —
the entire cost is inside `Store.stage`'s native commit path, dominated by
the reader-epoch scan.

### 4.2 Primary-only, 8 writers (8s sample of 1 of 8 writer processes)

| Category | Evidence | Samples | Share |
|---|---|---|---|
| Lock/lease wait | `__ulock_wait2` 2,428, `flock` 670, `__psynch_cvwait` 1,034, `__semwait_signal` 881 | 5,013 | ~98% |
| Actual work (open/fsync/close/unlink/read) | `__openat` 37, `fsync` 13, `__close_nocancel` 8, `preadv` 8, `__unlinkat` 6 | 72 | ~1.4% |

At 8 concurrent writer *processes* against one `.aflite` file, essentially
all of this process's sampled time is waiting on the cross-process writer
slot / path lease, not doing work — consistent with the design doc's
description of the writer slot, DB apply lock and path lease all being
held across the fsync, and with WS2/WS3's stated goal of releasing them
before the wait.

### 4.3 Indexed mode

Not independently sampled this round (a verification bug — see §6 —
forced the long sampling run that would have covered it to abort before
reaching the indexed case; the two shorter successful runs were too short
to safely interleave a `sample` attach). The §3 byte/sync counts
(`main_txn_commit`: 284 calling-thread syncs / 128 updates = 2.2/update,
~4.7 MB calling-thread bytes / 128 = ~37 KB/update on top of primary's own
~48 KB/update) stand in for the native_catalog layer's contribution; the
reader-epoch scan in §4.1 applies equally to indexed writes since it's in
the shared `Store.stage` path both modes go through.

## 5. Ranked fixes with estimated gain

1. **Cache/amortize the reader-epoch namespace instead of reopening it on
   every write.** `zig/pkg/antfly-embedded/src/storage/lite/reader_epochs.zig:43`
   (`Namespace.open`), called from `zig/pkg/antfly-embedded/src/storage/lite/native.zig`'s
   `NativeFile.acquireEpochReuseLock` (via `pageAllocatorFromFreeMapWithProof`,
   itself called from `Store.stage` on every commit). Evidence: §4.1, 46%
   of foreground samples. **Estimated gain: up to ~1.4-1.9x on primary-only
   single-writer throughput** if this drops to near zero (46% of wall time
   removed from the critical path), more at higher writer counts where the
   same scan still runs per writer before lock contention. **Not fixed in
   this pass** — see §7, this is native.zig/allocator-reuse-epoch territory
   (WS1/WS2 own this file per the task boundary) and the correctness
   invariants here (cross-process reader safety with "no clock, PID reuse,
   or timeout" per the file's own header comment) are not something to
   change without the owning agent's context.
2. **Investigate the full-text visibility race under sustained writes**
   (§6). Likely in `mutation_commit.zig`'s indexed plan contributors or the
   `native_catalog` backend's merge/compaction path (WS4). This is a
   correctness bug, not a throughput one, but it blocks reliably measuring
   indexed throughput over long runs and risks violating the §6 "every
   acknowledged document is searchable without replay" crash-test
   requirement even without a crash. **Not fixed in this pass** — squarely
   in owned mutation/commit files; flagged with repro conditions for the
   owning agent.
3. **Writer-slot/path-lease contention at 8 writers** (§4.2). Already the
   subject of WS2 (ticketed group commit) and WS3 (release the apply lock
   before the wait); this profile is additional evidence that it's still
   the dominant cost at 8 writers post-WS3. No new file to point at beyond
   what the design doc already names.
4. **Primary/indexed bytes-per-update still 1.2x-1.7x over gate** (§3):
   dedicated slot pages + manifest page (WS1) are a deliberate trade;
   closing the remaining gap is in the WS1 (native.zig) and WS4 (projection
   seal/progress folding, per that receipt's own known_limitations) lanes.
5. **JSON batch parsing** (`zig/pkg/antfly-embedded/src/api/batch.zig`
   `parseBatchRequestWithOptions`/`parseInserts`): investigated and found
   to be architecturally required, not a quick win. `std.json.Value`'s
   `jsonParse` hardcodes `.alloc_always` regardless of the `ParseOptions.allocate`
   passed to `parseFromSlice` (see `lib/std/json/dynamic.zig:88,105,163` in
   the pinned Zig 0.17 stdlib), so the `.allocate = .alloc_always` in
   `parseBatchRequest` is already a no-op, not a lever. `parseInserts`'
   parse-then-`Stringify.valueAlloc` round trip for each insert's value is
   there to canonicalize JSON text while preserving exact number-token
   spelling (see the file's own "distributed txn batch transport preserves
   exact row and transform number tokens" test) for internal replay/RPC
   transport shared with SQL and graph writes, not something Lite's path
   alone can skip without touching that shared contract. Not pursued
   further this round; did not show up as a measurable cost in §4.1's
   leaf breakdown at this sample resolution either, consistent with this
   conclusion.
6. **Text analysis pipeline** (`zig/pkg/antfly-embedded/src/search/analysis.zig`):
   read through the default analyzer chain (unicode_words -> lowercase ->
   English stop words -> Porter2 stemmer). Already has SIMD lowercasing
   with a skip-if-no-uppercase fast path, a skip-if-no-stopwords fast path,
   and a stack-buffer fast path in the stemmer for words that don't change.
   No quick win found; not pursued further.

## 6. Correctness bug found: full-text visibility after sustained writes

Repro: `uv run --no-project python scripts/bench/lite_sqlite_comparison.py
--library <ReleaseFast libantfly> --output /tmp/out.json --directory /tmp/fx
--seconds 40 --workers 1 8 --rounds 1 --engine antfly` (indexed, 1 writer
case). After ~3,968-7,534 cumulative `sync_level: full_index` writes on a
single worker (key space wraps every 128 keys), post-run verification
reopens the database, calls `run_until_idle()`, then asserts
`search_count(matchedworker0sequence<N-1>) == 1` for the latest sequence —
this failed once (`AssertionError: ('matchedworker0sequence7533', 'not
visible')`). Two shorter runs (20s/~3,370 writes, and 15s/~3,968 writes) at
the same concurrency did **not** reproduce it, suggesting a threshold tied
to accumulated write/merge count rather than a per-write race. Not
investigated further — this is `mutation_commit.zig`/`native_catalog`
territory (indexed acknowledgement path, WS4's known "periodic
merge/compaction" note in `write-amplification-after-ws4.json` is a
plausible trigger) and deserves the owning agent's attention with full
context, not a guess from this lane. Flagging prominently because it
appears to violate the §6 required-test intent ("every acknowledged
document is in primary and searchable without replay") even without an
injected crash.

## 7. What's in this pass vs. recommended for others

**Fixed here** (own commit, own lane):
- `scripts/bench/lite_sqlite_comparison.py` refused to run at all on this
  shared host (`assert not compiler_processes()` / `assert not
  contamination` — both hard-failed because zig/clang/cargo are
  continuously running from other agents' builds). Changed both to record
  the contamination in the report instead of aborting; correctness
  (`qualification_passed`) is unaffected since that still requires
  `zero_busy` and `all_workers_progressed`. Commit `39a1bbd9c2`.

**Investigated and found not to be a quick win in this lane**: JSON batch
parsing allocation strategy (§5 item 5), the text analysis pipeline (§5
item 6), per-write catalog/config lookups (`configureDbOpenOptions` is
open-time only, not per-write — checked, not an issue).

**Recommended for the owning agents** (precise file:function, evidence
above):
1. Reader-epoch namespace caching — `storage/lite/reader_epochs.zig:43`
   `Namespace.open`, called from `storage/lite/native.zig`
   `NativeFile.acquireEpochReuseLock`/`pageAllocatorFromFreeMapWithProof`.
   §4.1, §5.1.
2. Full-text visibility race under sustained writes — likely
   `mutation_commit.zig` indexed plan contributors or `native_catalog`
   merge/compaction (WS4). §6, §5.2.
3. Writer-slot/path-lease contention at 8 writers — already WS2/WS3 scope,
   this profile is confirming evidence. §4.2, §5.3.
4. Primary/indexed bytes/update still over gate by a moderate margin —
   WS1 (native.zig slot/manifest) and WS4 (projection-seal/progress
   folding) lanes. §3, §5.4.

No production throughput fix was safe to ship from this lane this round:
every measured hot spot large enough to matter traced back into
native.zig's commit/allocator-reuse protocol, the mutation/commit core, or
files tightly coupled to them, which this task's own boundary (and the
correctness stakes involved, per §6) make someone else's call. Because no
production code changed, a second ReleaseFast build/rerun this round would
reproduce §2/§3 exactly; the receipts already gathered stand as both the
"before" and current state.
