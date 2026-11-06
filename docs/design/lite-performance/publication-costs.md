# Lite publication costs and the SQLite comparison

The matched comparison at `7bb05eb2b1` found a large gap before asynchronous
indexing is involved. The primary-only path uses Lite's native copy-on-write
B-tree. SQLite WAL/FULL was 24–36 times faster in these short shared-host runs;
requiring full-text visibility at acknowledgement increased the gap to 111–243
times. See [the immutable receipt](sqlite-matched-7bb.json). All latest values,
index visibility, overwrites, and integrity checks passed. One SQLite case
returned one Busy, so the matrix does not satisfy its zero-Busy qualification
rule. The rates are diagnostic, not production performance guarantees.

The harness is `scripts/bench/lite_sqlite_comparison.py`. It uses 1 KiB changing
text, one document per transaction, 128 keys per writer, one and eight writers,
and alternating engine order. SQLite uses WAL, synchronous FULL, ordinary sync
rather than fullfsync, and an external-content FTS5 table maintained by triggers
inside the document transaction. Lite uses durable `write` for primary-only and
`full_index` for the indexed case. The previous asynchronous, primary-acknowledged
contention benchmark is a different workload. Reuse its recorded references;
subsequent candidate runs can select `--engine antfly`.

SQLite's document and FTS shadow-table writes share the pager transaction. FTS5
also stores immutable segments and merges them, so segment merging alone does
not explain the gap. Sources: [WAL](https://sqlite.org/wal.html),
[architecture](https://sqlite.org/arch.html), and
[FTS5 structures](https://sqlite.org/fts5.html#fts5_data_structures).

## Measured physical publications

A separate local syscall interposer counted 128 updates using the same existing
optimized library. These are operation counts, not timing qualification. Counts
on the calling thread exclude background-thread syncs and include all direct
synchronous work performed before returning from the mutation:

| Indexed update stage | Syncs per update |
| --- | ---: |
| Primary commit | 3 |
| Unpublished artifact staging | 3 |
| Artifact linking | 3 |
| Durable artifact pin | 3 |
| Index WAL publication | 3 |
| Projection progress metadata | 3 |
| Index status snapshot | 3 |
| Physical replay retirement | 15 |
| Retired artifact cleanup, amortized | 0.375 |
| Total | 36.375 |

The primary-only calling thread performed three syncs per update. SQLite
performed 129 syncs for 128 updates in each mode. Including other threads, Lite
performed 660 syncs for primary-only and 5,024 for indexed updates. Native primary
writes also wrote substantially more bytes: about 7.7 MB versus 0.53 MB across
all threads for these 128 updates. Sync counts alone do not establish each
mechanism's contribution to elapsed time.

The operation-count diagnostic is reproducible on macOS without rebuilding a
reference. Compile `scripts/bench/lite_io_counter.c` as a dylib in the task's
worktree, then run `scripts/bench/lite_publication_io.py --library <candidate>
--shim <counter.dylib> --directory <owned-fixture-directory> --output <receipt>`.
It defaults to Antfly only, verifies the latest values and index visibility,
and removes the closed fixture. Keep the immutable SQLite count receipt above.

The first candidate's [Debug receipt](commit-io-v37-debug.json) verifies the
predicted reductions. Calling-thread indexed syncs fell from 36.375 to 18.375
per update; primary-only remained at three. Across all threads, the indexed run
performed 2,563 syncs and wrote 27.2 MB, versus 5,024 and 50.3 MB in the reference.
The candidate coalesces eligible replay-lane deletion, publishes progress and
status together, and transfers the prepared artifact's existing durable lease
into its immutable reader. Every latest value, index canary and integrity check
passed. Fixtures were closed and deleted. These are operation counts across
Debug and optimized builds; background scheduling can vary. This receipt does
not establish a throughput improvement or complete qualification.

The integrated atomic publisher's [Debug receipt](commit-io-v64-debug.json)
reduces calling-thread syncs further: two per primary-only write and 6.25 per
indexed write. That reduction does not establish a total-work improvement.
Across all threads, the indexed case still performed 4,802 syncs and wrote about
68 MB for 128 updates. A separate all-thread stack trace attributed 2,522 of
3,500 syncs to 1,261 durable artifact-lease acquisitions while maintenance
reopened indexes. Scheduling changes the background totals, so all-thread
counts remain part of the acceptance criteria.

Cache diagnostics identified an important lifetime boundary: expected
artifact-publication admission yields discarded the mutable maintenance
runtime and also skipped immutable-source preservation. The exact leased
inputs can remain useful even when that runtime loses publication authority.
The bounded immutable handoff must survive those expected yields, while a
later open still checks its current inode and immutable value identity.

## Required publication shape

One prepared index publication should own immutable artifacts, exact source and
catalog witnesses, activation mutations, and durable fragment progress. Its
native commit validates the witnesses and publishes activation plus progress
atomically; in-memory activation follows successful commit. Small artifacts can
join that publication. Large artifacts retain bounded unpublished staging, with
crash cleanup and no visibility before activation. Existing nested transaction
owners cannot safely be placed inside a new outer transaction.

Physical journal deletion belongs to a fair, bounded retirement service. It
must delete consumer references and eligible canonical chunks together and
return capacity only after those bytes are physically retired. Explicit drains
and capacity pressure must service this work, even without background workers.
The immediate native path now uses the existing coalesced deletion
service to eliminate a separate transaction per replay lane.

Progress is authoritative; diagnostic status snapshots should not require a
second commit for each fragment. Consolidating them requires preserving the
readiness witnesses that currently consume persisted status and projection
metadata. Unchanged observability state can be coalesced independently.

Revision-11 authenticated checkpoints use two durability barriers when the new
checkpoint is unambiguously newer than the retained checkpoint. The data barrier
remains before checkpoint publication, and the selector shares the final barrier.
Legacy or ambiguous histories retain the three-barrier protocol. A pager/WAL
redesign remains a separate foundation decision after unnecessary logical
publications and copy-on-write amplification have been measured independently.

## Completion criteria

Each change must demonstrate its predicted publication or byte-count reduction
locally, preserve crash recovery and exact consumer accounting, and pass the
existing correctness gates before the next optimized candidate build. Final
qualification on wolfspider uses the unchanged importer and recorded references,
with active-write storage plateau, visibility, all-worker progress, zero Busy,
and ten-minute soaks. The cold producer/consumer drain and forced-compaction
failures remain blockers until reproduced and fixed. Full factory remains owned
by Beads. Databases are reused or deleted after verification; receipts are kept.
