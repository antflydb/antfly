# Vector store micro-profile: write, read, disk, and memory costs

Date: 2026-09-10. Scope: the table-owned source payload store
(`storage/vector_payload_store.zig` over `storage/vector_block_store.zig`)
measured in isolation with the new `vector_payload_bench` harness, not the
end-to-end VectorDBBench arms recorded in [VECTOR_STORE.md](VECTOR_STORE.md).
The goal is attribution: which code paths cost what, so the next
qualification rounds target the largest costs first.

## Harness

```bash
cd zig
zig build vector-payload-bench -Doptimize=ReleaseFast              # baseline x86-64
zig build vector-payload-bench -Doptimize=ReleaseFast -Dcpu=native --prefix zig-out-native
./zig-out/bin/vector_payload_bench --vectors 50000 --dims 768 --batch 64 --reads 20000 --drop-caches --root /tmp/vps
./zig-out/bin/vector_payload_bench --vectors 50000 --dims 768 --reopen-only --drop-caches --root /tmp/vps   # after a --keep run
```

The harness drives the same surface the DB uses: `Session.put` and
`prepareCommit` per batch of 64 (durable sync append, as `Txn.commit` does),
`Session.get` resolves, `Store.checkpoint`, updates, and reopen. It runs on
native on-disk storage and reports the store's own `Stats`, a counting
allocator for the store heap, RSS, and file sizes. Phases: ingest, hot reads,
explicit checkpoint (bootstrap base), cold/warm reads, 10% updates, per-read
component costs, rerank-style batch reads (`locateHashed` +
`readExactIntoBatch`/`readProjectionsIntoBatch`), and cold reopen.

Host: 4 vCPU Xeon 2.1 GHz (AVX-512, SHA-NI), 15 GiB, ext4 on virtio.
Raw fsync of a 196 KiB append: p50 0.83 ms, p99 3.7 ms. Zig 0.16.0,
ReleaseFast, float32 payloads, 768 dims unless stated. Absolute numbers are
host-specific; the ratios and attributions are the result.

## Headline attribution

| Cost center | Evidence | Share |
| --- | --- | --- |
| SHA-256 digest (`Reference.forArtifact`) | callgrind, baseline build, 3K vectors + 9K reads | 86% of all instructions; 65% inside `resolve`, 21% inside `Session.put` |
| fsync per prepare batch | `durable_append_ns` | 36-45% of preparation time at 50K-200K |
| Synchronous WAL checkpoint under the store lock | `checkpoint_ns`, `prepare_max_us` | 31% of ingest time at 50K, 60% at 1M; one 33.4 s stall at 1M |
| Cold reopen inventory scan | `inventory_update_ns`, RSS delta | 92-96% of reopen time; faults the whole corpus into RSS (fixed here) |
| Everything else in the store (WAL AVL view, block writer, CRC32, compaction merge) | callgrind | < 3% each |

## 1. SHA-256 dominates artifact reads and a large share of writes

`resolveFrom` recomputes the full artifact digest on every read and compares it
to the reference; `Session.put` computes it once per write. The lookup key is
already the digest, the block index and WAL frames are CRC32-protected, and
`Reference.decode` validates the envelope, so the recompute only guards the
key-binding check (`VectorReferenceIdentityMismatch` when a reference is read
under a different artifact key) and write-path bugs.

Per-operation cost, warm page cache:

| Build | Dims | Digest | Locate (metadata) | Full resolve p50 | Rerank exact read (no digest) |
| --- | ---: | ---: | ---: | ---: | ---: |
| baseline x86-64 | 768 | 12.5 µs | 0.8 µs | 15.3 µs | 1.8-4 µs |
| `-Dcpu=native` (SHA-NI) | 768 | 3.4 µs | 0.8 µs | 5.1-5.7 µs | 1.7-4 µs |
| baseline x86-64 | 3072 | 45.8 µs | 0.2 µs | 45.0 µs | 4.6-5.9 µs |
| `-Dcpu=native` (SHA-NI) | 3072 | 10.7 µs | 0.3 µs | 17.2 µs | 4.5-5.6 µs |

Ingest at 50K: 17.0K vectors/s baseline versus 25.7K vectors/s native, with
`put_digest` falling from 626 ms to 157 ms. The 1M qualification workload uses
3072-dim payloads, where a baseline binary spends about 46 µs of CPU per exact
artifact read on the digest alone.

The container and artifact builds pass `-Dtarget=x86_64-linux-musl` without
`-Dcpu`, so shipped binaries compile for baseline x86-64 (no SHA-NI, no AVX2);
the compile line shows `-mcpu baseline`. `std.crypto` selects SHA-NI only at
compile time.

Options, in order of payoff for the least risk:

1. Add a runtime-dispatched SHA-256 kernel (SHA-NI on x86, SHA2 on aarch64)
   the way `lib/hash` already dispatches CRC32 at runtime. This alone makes
   digests 3.7-4.3x cheaper on hosts that have the instructions, with no
   format change.
2. Stop hashing the payload on every resolve. Keep the key binding by
   verifying a short key tag carried in the reference (or by hashing only the
   key and the CRC-verified payload checksum), and move full digest
   recomputation to an explicit scrub boundary. Reads then cost the
   metadata lookup plus the CRC32 the block reader already performs.
3. Ship x86-64-v3 (or v4) builds where the supported fleet allows it.

## 2. Cold reopen read and checksummed every payload byte (fixed in this branch)

`inventoryRetainedPayloads`, the incremental `Inventory.sync`,
`Directory.update`/`removeRetired`, GC planning `SegmentStats`, copy-set
construction, and the ANN mark scan all used `Reader.entryAt`, which validates
the payload CRC and therefore faults every vector page. Inventory only needs
identities, which `Reader.sourceIdentityAt` returns from the index and key
regions that reader admission has already validated. Payload CRCs remain
enforced at the read, exact-verification, and GC copy boundaries, which are
the places that consume payload bytes.

Cold reopen (`--drop-caches`, native build):

| Scale | Before: reopen / inventory / RSS delta | After: reopen / inventory / RSS delta |
| --- | --- | --- |
| 50K (146 MiB) | 1.47-1.71 s / 1.36-1.58 s / +171 MiB | 0.12-0.13 s / 8 ms / +26 MiB |
| 200K (586 MiB) | 6.2-7.1 s / 6.0-6.7 s / +609-670 MiB | 0.29-0.31 s / 26 ms / +26 MiB |
| 1M (2.9 GiB, 2,944 segments) | 38.7-45.3 s / 37.1-43.7 s / +3.1 GiB | 1.86 s / 0.20 s / +191 MiB |

After the change, the remaining 1.65 s of the 1M reopen is opening and
validating 2,944 cold segment files (index and key regions, about 120 bytes
per vector) plus WAL recovery; the settled 128-segment layout after base
compaction opens faster. The old scan also pre-warmed the page cache, so post-reopen reads looked fast
(5 µs) only because the inventory had just paged in the corpus. After the
change, reads after a cold reopen are honestly cold (45-50 µs at 50K) until
touched, and RSS grows with demand rather than by corpus size. Both store
suites pass (`vector-payload-test` 48/48, `vector-block-store-test` 33/33).

## 3. WAL checkpoint and delta-chain compaction run under the writer lock

`prepareBatch` calls `checkpointLocked` when the WAL reaches the admission
bound (64 MiB in the harness; `min(64 MiB, slice/8)` = 48 MiB when managed).
The WAL-to-delta rewrite of 48-64 MiB takes 0.36-0.6 s, during which every
preparation waits, and with it every primary commit in the batch path:

| Scale | Ingest wall | Checkpoint time inside ingest | Max prepare stall | Vectors/s |
| ---: | ---: | ---: | ---: | ---: |
| 50K | 1.95 s | 0.69 s (2 checkpoints) | 364 ms | 25.7K |
| 200K | 9.2-9.9 s | 3.6-3.9 s (9) | 430-530 ms | 20-22K |
| 1M | 91.9 s | 55.7 s (47) | 33,371 ms | 10.9K |

At 1M the bootstrap chain reached `max_bootstrap_delta_generations` (24), and
the next checkpoint rewrote all 24 deltas plus the WAL into one generation
under the lock: 1.57 GiB rewritten, one 33 s stall, and ingest throughput
halved relative to 200K. The stall grows with corpus size. The harness does
not hold the outer DB lock, so this is a candidate mechanism for the
mixed-workload p99 and readiness tails in the qualification arms rather than
a confirmed one; the `preparation_ns`, `checkpoint_ns`, and
`outer_db_batch_lock_wait_ns` counters in a VectorDBBench arm would confirm it.

Recommendation:

- Stage checkpoint output outside the store lock. The WAL view is a
  persistent, immutable structure, and `stageDeltasToBaseWithShardCount` plus
  `commitPrepared` already stage on a retained snapshot and install under a
  brief lock; the WAL-to-delta path should use the same shape.
- Trigger checkpoints from the background maintenance turn
  (`runArtifactRepairMetadataMaintenancePass`) at a soft watermark (for
  example half the admission bound) so the foreground only stalls at the
  hard bound.
- Replace the all-at-once delta-chain rewrite with tiered merging (merge the
  oldest K generations) so no single step is proportional to the corpus.

## 4. Bootstrap write amplification is about 3.6x

Every ingested byte is written three times before the base exists: WAL,
WAL-to-delta checkpoint, and base compaction, plus the delta-chain rewrite at
1M.

| Scale | Payload | WAL | Checkpoints during ingest | Base compaction | Total written | Amplification |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 50K | 146 MiB | 151 MiB | 129 MiB | 175 MiB | 455 MiB | 3.1x |
| 1M | 2,930 MiB | 3,083 MiB | 4,579 MiB (incl. 1,570 MiB chain rewrite) | 3,057 MiB | 10,719 MiB | 3.6x |

Base compaction alone took 53 s at 1M (about 58 MB/s in + out) and 11 s at
200K. The per-prepare fsync (15.6K fsyncs at 1M, 22 s total) is inherent to
prepare-before-primary-commit; group commit exists but is gated by the outer
DB lock as recorded in VECTOR_STORE.md. Each vector is also copied about four
times on the write path (session arena, `appendUpsert` scratch, frame
buffer, WAL view chunk), which callgrind puts under 2%.

Larger ideas, not measured: seal WAL generations (the extent machinery
exists) and build the base directly from sealed WALs at the stable tip,
skipping the WAL-to-delta rewrite during bootstrap; or raise the WAL
admission bound only while the base is empty.

## 5. The newest vectors are the slowest to read

Checkpoint and compaction outputs are written with `cold_sequential` intent,
which issues `fadvise(DONTNEED)` after the durability sync. The freshly
published generation is therefore never in the page cache, and the WAL-resident
copy is gone, so the first read of any recently checkpointed vector is a disk
read:

| Read path | Warm | Cold (fresh delta or evicted base) |
| --- | ---: | ---: |
| `Session.get` p50 | 5-7 µs | 46-92 µs |
| Rerank exact, serial, per vector | 1.8-2.8 µs | 22-40 µs |
| Rerank exact, 8-wide `std.Io` batch, per vector | 1.7-2.0 µs | 9.7-17 µs |

The eviction is deliberate for large compactions (bounded RSS), but for the
WAL-to-delta path the output is small (one admission bound) and is exactly the
data ANN completion and updates read next. Keeping the newest generation
cached (skip `DONTNEED` for that path, or re-touch its key/index regions)
removes a disk read per fresh-vector access. The concurrent read batch already
recovers 2.3x on cold reads; it is worth confirming that the ANN rerank path
always passes an `Io` so it gets that overlap.

## 6. Memory

The store's own heap is small: peak 70-74 MiB at every scale, which is the
resident WAL view (one copy of committed WAL bytes, `Chunk.copy`, plus about
130 bytes of AVL node per record) up to the admission bound. RSS beyond that is
mapped block residency, which grows with whatever touches payload pages. The
inventory scan was the largest such toucher (+3.1 GiB at 1M on reopen) and is
addressed above. Note that the harness's own dataset is resident too; the
`rss_ex_dataset_mib` field subtracts it.

Production uses glibc malloc (`platform.allocator.processAllocator`) and only
the inference server calls `malloc_trim`. In this harness, `smaps` showed no
heap retention beyond the dataset and the store's live allocations, so the
RSS-versus-physical-footprint divergence in the qualification reports is not
explained here; sampling `/proc/<pid>/smaps` during a VectorDBBench arm would
separate file-backed residency from heap in that setting.

## 7. Smaller observations

- `prepareBatch` probes each incoming digest with `Opened.get`, which reads and
  CRC-checks the payload when the digest already exists; `locateHashed`
  provides the dims needed by the retry and rescue paths without touching
  payload bytes.
- The managed open path selects float16 source encoding (`preferredEncoding`).
  For the source store that doubles checkpoint CPU (1.80 s versus 0.91 s at
  50K), slows exact reads about 20%, and does not shrink immutable bytes
  (132.9 versus 129.0 MiB) because residuals keep exactness. The benefit is
  halved projection I/O for rerank; that trade should be confirmed for the
  source store specifically rather than inherited from the ANN store default.
- A 15 MiB table still publishes 128 shard files per generation (258 files
  after the bootstrap base, about 2.5 ms per file). `SegmentSizing` already
  supports adaptive shard counts behind an environment variable.
- The persistent AVL WAL view allocates about log2(n) nodes per insert (about
  1,000 allocations per 64-vector batch under the lock). Callgrind puts this at
  2.5% plus 1.9% for releases; it is not a bottleneck today but would be the
  next CPU item once the digest cost is gone.

## Suggested order

1. Land the metadata-only inventory change (this branch) and re-run the 50K
   and 1M readiness/restart gates; expect restart to stop scaling with corpus
   bytes.
2. Runtime-dispatched SHA-256, then decide whether reads should recompute the
   digest at all.
3. Unlocked, background-triggered WAL checkpoints with tiered delta merging;
   re-measure mixed p99 and fixed-count churn, which are the metrics the
   qualification ledger shows regressing.
4. Keep the newest generation cached after WAL checkpoints; re-measure the
   first-window query p99.
5. Revisit bootstrap write amplification only after 3, since the checkpoint
   restructuring changes where those bytes are produced.
