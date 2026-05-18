# Zig LMDB Engine

This file is the canonical design and roadmap note for the Zig LMDB engine used
by `antfly-zig`.

Use [DB.md](../../../../DB.md)
for the higher-level DB contract and storage backend boundary. This file stays
focused on the LMDB-compatible engine itself: invariants, wrapper parity,
verification, performance, and write-path roadmap.

## Goal

Make the Zig LMDB backend the primary storage engine for `antfly-zig`, with:

- feature parity for the wrapper surface used by the repo
- strong crash/reopen and concurrency confidence
- competitive performance against the C backend
- a clear path for higher-throughput write behavior, including batching, group
  commit, and optional async I/O

The engine targets LMDB semantics first and API cleanup later. The current
storage wrapper remains the caller-facing facade:

- [../storage/lmdb.zig](../storage/lmdb.zig)

## Current Status

The engine already has broad LMDB-compatible behavior:

- ordered KV
- named DBs
- dupsort, dupfixed, duplicate cursor support, and duplicate sub-DB promotion
- nested write transactions
- `write_map`, `map_async`, and `fixed_map`
- write cursors
- borrowed range-scan batches
- shared file-backed reader table
- dedicated writer lock boundary
- oldest-reader reclaim based on the shared reader table
- crash/reopen coverage across major commit boundaries

Status snapshot:

- `M1`: done
  - Zig is now the default backend in
    [../../../../build.zig](../../../../build.zig)
  - `-Dlmdb_backend=c` remains available as the oracle path
- `M2`: in progress
  - broad crash/reopen, differential, reducer, and replay-fixture coverage exists
  - remaining work is broader workload coverage and promotion of real reduced
    failures
- `M3`: in progress
  - benchmark timing and LMDB/WAL publication stats exist and are exposed
    through higher layers
  - remaining work is broader product-level reporting and workload integration
- `M4`: done
  - [../storage/wal.zig](../storage/wal.zig) has `appendBatch`
- `M5`: in progress
  - WAL-level group commit is implemented and propagated upward
  - broader adoption and validation across write-heavy product paths is pending

## Scope

There are two targets:

1. Repo parity
2. Full LMDB parity

The implementation should be designed for full LMDB parity, but milestones ship
repo parity first.

Repo parity is the minimum needed to replace the current backend used by:

- [../storage/docstore.zig](../storage/docstore.zig)
- [../storage/persistent.zig](../storage/persistent.zig)
- [../storage/wal.zig](../storage/wal.zig)
- [../storage/hbc_adapter.zig](../storage/hbc_adapter.zig)
- [../storage/transactions.zig](../storage/transactions.zig)
- [../storage/db/](../storage/db/)

The repo depends heavily on:

- read-only and read-write transactions
- ordered key/value lookup
- `MDB_SET_RANGE`-style cursor seek
- first/next iteration
- named databases
- predictable reopen and recovery behavior
- map sizing and growth

Full upstream parity remains broader than the repo requirement. Niche LMDB
feature families can stay behind explicit support and test gates unless a repo
caller needs them.

## Design Contract

These invariants are the real design contract. If any of them drift, the port
can pass unit tests and still fail under real load or recovery.

### Storage And Commit

- The file is memory-mapped.
- Pages have fixed size within an environment.
- Structural changes are copy-on-write.
- A write transaction publishes a new root state by writing a new meta page.
- There are two meta pages; readers select the newest valid committed one.
- A crash may lose the last transaction, but must not expose a torn committed
  state as valid.

### Concurrency

- Exactly one write transaction may be active at a time.
- Read transactions observe a stable snapshot selected at begin time.
- Readers never block writers on logical visibility.
- Writers must not reclaim pages still visible to active readers.
- Reader tracking must survive stale entries and process death.

### B+Tree Semantics

- Keys are stored in lexical order unless a custom comparison mode is enabled.
- Branch and leaf pages preserve search order invariants.
- Cursor traversal order is stable and total.
- `SET_RANGE` returns the first key greater than or equal to the search key.
- Split, merge, and rebalance operations preserve tree correctness.

### Space Management

- Freed pages are retired against the write transaction that freed them.
- Pages only re-enter allocation once no active reader can still see them.
- Overflow pages behave like contiguous page runs owned by one logical value.

### Error Semantics

- Map growth failure, bad DBI, invalid transaction reuse, and not-found behavior
  remain distinguishable.
- After commit or abort, a transaction object is dead.
- Cursor lifetime remains bounded by its parent transaction.

## On-Disk Model

The Zig implementation keeps the LMDB file layout mental model:

- meta pages
- freelist DB pages
- main DB pages
- leaf pages
- branch pages
- overflow pages

Meta pages carry:

- magic/version
- page size
- transaction ID
- root page for main DB
- root page for freelist DB
- mapsize and geometry fields needed for reopen
- integrity checks sufficient to reject partial commits

The publish sequence must guarantee that a reader sees either the old meta page
or the new meta page, never a partially committed hybrid.

Raw page access should stay centralized in the format/page/node modules so tree
code does not scatter pointer arithmetic.

## Module Layout

The package is organized by responsibility rather than by copying chunks of
`mdb.c`.

Core format and tree layers:

- [format.zig](format.zig)
- [page.zig](page.zig)
- [node.zig](node.zig)
- [meta.zig](meta.zig)
- [tree.zig](tree.zig)
- [cursor.zig](cursor.zig)
- [dupdata.zig](dupdata.zig)

Environment, transactions, and runtime:

- [env.zig](env.zig)
- [txn.zig](txn.zig)
- [writer_lock.zig](writer_lock.zig)
- [readers.zig](readers.zig)
- [commit_support.zig](commit_support.zig)
- [prepare_commit_support.zig](prepare_commit_support.zig)
- [materialize_support.zig](materialize_support.zig)

Write-path support:

- [write_state.zig](write_state.zig)
- [write_mutation_support.zig](write_mutation_support.zig)
- [write_page_state_support.zig](write_page_state_support.zig)
- [write_path_support.zig](write_path_support.zig)
- [dupsort_write_support.zig](dupsort_write_support.zig)
- [mutate_leaf.zig](mutate_leaf.zig)
- [rebalance_branch.zig](rebalance_branch.zig)
- [free_db.zig](free_db.zig)
- [split_support.zig](split_support.zig)

The public package entry remains:

- [root.zig](root.zig)

The storage wrapper remains the migration and compatibility facade:

- [../storage/lmdb.zig](../storage/lmdb.zig)
- [../storage/lmdb_c_backend.zig](../storage/lmdb_c_backend.zig)
- [../storage/lmdb_zig_backend.zig](../storage/lmdb_zig_backend.zig)

## Public API Boundary

The wrapper should insulate the rest of the repo from engine churn. Internally,
the engine boundary should remain explicit and testable around:

- environment open/close
- map-size management
- sync
- transaction begin/commit/abort
- DB open/lookup
- point get/put/delete
- cursor open/get/seek/put/delete

## Implemented Milestones

### Delete And Branch Rebalance

Ordinary deletes now stay on the page-state path instead of falling back to full
logical rebuilds.

Implemented:

- in-place leaf delete
- emptied leaf removal from parent
- ancestor first-key propagation
- branch collapse
- left and right sibling borrow
- sibling merge
- recursive upward rebalance
- final root collapse
- persistence coverage for branch borrow, branch merge, and root collapse

Remaining emphasis:

- keep rebuild fallback only for unsupported node flag families
- keep materialized `md_root`, `md_depth`, `md_branch_pages`, and
  `md_leaf_pages` validated against staged tree state

### Overflow Page-State Writes

Large-value put/update/delete stays on the page-state path.

Implemented:

- staged overflow run allocation
- old overflow-chain replacement on overwrite
- overflow-page retirement on delete and overwrite
- overflow page counts in materialized DB metadata
- reopen coverage for large-value overwrite/delete

### Named DB Structural Path

Named DB structural mutations persist without forcing unrelated main-DB rebuild.

Implemented:

- named DB root changes staged through page-state commit
- main-DB named-subdata updates staged through page-state commit
- first writes into empty main and named DBs stay on the page-state path

### Fallback Removal

`rebuild_required` is now the exception for repo-critical ordered-KV behavior.
Remaining rebuild fallbacks are concentrated in unsupported or non-core LMDB
edge cases.

### Reader And Allocator Parity

Implemented:

- shared file-backed reader table keyed by LMDB data path
- dedicated writer lock at write-transaction begin/commit/abort
- oldest-reader reclaim based on the shared table
- stale reader cleanup during oldest-reader scans
- active-reader-delayed free-page reuse coverage
- multi-reader reclaim coverage across reopen cycles
- multi-environment reader/writer coordination coverage
- fork-based writer-lock validation
- fork-based oldest-reader validation
- crash/reopen coverage for pre-meta and post-meta-write commit boundaries
- crash/reopen coverage for pre-data-sync commit boundary
- crash/reopen coverage for structural deletes that retire pages

Remaining:

- stronger cross-process coordination if needed beyond the current single-writer
  lock, shared reader table, and multi-environment regression coverage
- stronger recovery validation if needed beyond current commit-boundary reopen
  coverage
- longer-running soak or fault-injection validation before any broader default
  promotion decision

## Split Reality

The LMDB-backed split path is improving, but should not be described as a fully
cheap native split yet.

Current state:

- child docstore creation is page-level on Zig LMDB
- parent docstore reclaim is page-level on Zig LMDB
- text indexes use segment handoff and mixed-segment rewrite instead of full
  child rebuild and per-doc parent text deletion
- broader secondary-index split work is still active, especially dense vector,
  sparse, and graph handoff

That means the old full logical copy path is no longer the whole story, but the
end-to-end DB split is still not just an O(1)-style LMDB environment operation.

Compared to Pebble/RocksDB-style engines:

- split-key selection can still be harder when live logical documents or index
  metadata must be consulted instead of SSTable/file metadata
- page-level docstore work helps, but total split cost can still be dominated by
  secondary-index ownership and mixed-range cleanup
- index handoff must stay explicit per index family; docstore page work alone
  does not make the full product split cheap

Where LMDB can become faster later:

- deepen native page/environment-level split support where it is still useful
- avoid re-walking live document keys when persisted metadata can answer split
  planning questions
- make text, dense, sparse, and graph handoff cheaper than generic copy/prune or
  rebuild

The architectural upside is real, and the page-level pieces are now part of the
implementation, but the roadmap should still treat full split cheapness as an
active DB/index problem rather than a solved LMDB-only property.

## Verification Strategy

The C backend remains the oracle path for differential testing even though the
Zig backend is now the default.

Oracle and differential tests should compare:

- returned values
- returned errors
- iteration order
- visible state after reopen

Operation classes:

- create/open DB
- insert/update/delete
- overwrite and not-found cases
- cursor seek and scan
- multi-DB sequences
- dupsort growth and promotion
- nested child commit/abort
- reclaim-heavy delete cycles
- map growth scenarios
- `write_map`, `map_async`, and `fixed_map`

Recovery tests should cover interrupted commits:

- after dirty pages are written, before meta publish
- after one meta write, before sync
- after sync, before lock release
- around reclaim publication
- around dupsort promotion
- around nested transactions

Expected outcome:

- the DB reopens at either the old committed state or the new committed state
- it never reopens at a corrupt mixed state

Fuzzing and reducer workflows should use reproducible operation sequences and
compare the Zig backend against both the C backend and an ordered-map model for
search-order invariants.

## Performance Principles

1. Correctness before syscall cleverness.
   Group commit and durability policy matter more than replacing `fsync` with
   an async API.
2. Optimize the whole write pipeline, not one syscall.
   If the system is still single-writer and sync-per-commit, async I/O alone
   will not move throughput much.
3. Keep the LMDB package layered.
   `txn.zig` should remain lifecycle and public API glue, while write behavior
   lives in dedicated support modules.
4. Treat durability modes as product behavior, not just raw flags.
   `no_sync`, `no_meta_sync`, `write_map`, and `map_async` need explicit
   guarantees and tests.

Guidance:

- prefer explicit byte-slice manipulation over fine-grained heap allocation on
  the write path
- keep page parsing zero-copy where possible
- centralize dirty-page ownership and copy-on-write logic
- add instrumentation counters before premature optimization

## Roadmap

### M1. Default-Ready Zig Backend

Status: done.

Objective:

- make the Zig backend the default choice in normal development and CI while
  keeping a C escape hatch

Acceptance:

- `zig build lmdb-test` passes
- `zig build storage-lmdb-test -Dlmdb_backend=zig` passes
- no known correctness gaps in the wrapper surface used by the repo

### M2. Confidence And Differential Testing

Status: in progress.

Objective:

- prove the Zig backend is behaviorally stable under real repo workload shapes

Workloads to cover:

- main DB and named DB mixes
- dupsort growth and promotion
- nested child commit/abort
- reclaim-heavy delete cycles
- range iteration and reopen
- `write_map`, `map_async`, and `fixed_map`

Acceptance:

- deterministic wrapper workloads pass on both C and Zig backends
- crash/reopen matrix covers all commit boundaries modeled in
  [commit_support.zig](commit_support.zig)

### M3. Observability

Status: in progress.

Implemented:

- benchmark timing
- LMDB commit publication stats in [commit_support.zig](commit_support.zig) and
  [env.zig](env.zig)
- wrapper exposure in [../storage/lmdb.zig](../storage/lmdb.zig)
- higher-level exposure in [../storage/docstore.zig](../storage/docstore.zig)
  and [../storage/persistent.zig](../storage/persistent.zig)
- WAL stats in [../storage/wal.zig](../storage/wal.zig)

Remaining:

- thread stats into more product-level workloads and reporting surfaces
- extend comparable visibility to more non-benchmark DB flows

Counters/timers should cover:

- transaction open
- put loop
- prepare commit
- data sync
- meta sync
- WAL append
- reopen/read-open

Acceptance:

- every major write latency bucket is measurable
- commit-boundary costs are visible without code instrumentation edits

### M4. WAL Batching

Status: done.

Objective:

- stop paying one LMDB write transaction per WAL append when callers can
  tolerate batching

Implemented:

- `appendBatch` in [../storage/wal.zig](../storage/wal.zig)
- single append and batched append share encoding and durability path

Acceptance:

- existing single-append API remains correct
- batch path shares one durable commit boundary for multiple appended entries
- replay and truncate behavior remain unchanged

### M5. Group Commit

Status: in progress.

Objective:

- let multiple concurrent logical writes share one durable commit

Implemented:

- WAL-level group commit coordinator in [../storage/wal.zig](../storage/wal.zig)
- propagated options through
  [../storage/db/derived/derived_log.zig](../storage/db/derived/derived_log.zig)
  and [../storage/persistent.zig](../storage/persistent.zig)
- benchmark coverage

Remaining:

- push grouped durability through more higher-level write-heavy product paths
- expand validation beyond the current WAL-centered workloads
- decide whether a non-WAL LMDB-level coordinator is still needed after product
  integration data

Key constraints:

- single-writer LMDB semantics remain
- data pages must still be durable before meta publication
- failure reporting must remain per-request

Acceptance:

- N logical writes can share one physical sync
- throughput improves on fsync-bound workloads
- crash/reopen correctness remains unchanged

### M6. Product-Level Durability Policies

Objective:

- expose durable behavior in terms callers can reason about

Possible policy names:

- `strict`
- `fast`
- `async_flush`
- `mmap_async`

Document exact guarantees for:

- `no_sync`
- `no_meta_sync`
- `write_map`
- `map_async`
- `fixed_map`

Acceptance:

- product code can choose a policy without knowing LMDB internals
- tests explicitly pin reopen guarantees for each policy

### M7. Async Commit Backend

Objective:

- add an optional async I/O backend for the LMDB publish path

This should come after group commit. Without group commit, async I/O mostly
changes syscall shape, not durable throughput.

Deliverables:

- add a pluggable commit backend to [commit_support.zig](commit_support.zig)
  - sync backend: current `pwrite` / `fsync` / `msync`
  - async backend: likely `io_uring` or a platform equivalent
- support async submission for dirty page writes, data sync, meta write, and
  meta sync

Non-goals:

- changing LMDB's data-before-meta correctness rule
- changing single-writer ordering
- accelerating mmap reads

Acceptance:

- sync backend remains default and fully tested
- async backend matches sync backend crash/reopen guarantees
- measurable improvement exists on batched/grouped write workloads

### M8. Long-Running Soak And Fault Injection

Objective:

- make the engine boring under sustained and adversarial conditions

Deliverables:

- long-running mixed-feature soak tests
- more aggressive fault injection around data sync, meta publish, reclaim
  publication, dupsort promotion, nested transactions, and write-map modes

Acceptance:

- no divergence across long mixed workload runs
- no crash/reopen regressions under injected failure points

## Ordering

Recommended implementation order:

1. M1 default-ready cutover
2. M2 confidence and differential testing
3. M3 observability
4. M4 WAL batching
5. M5 group commit
6. M6 durability policies
7. M7 async commit backend
8. M8 long-running soak and fault injection

## Why Async I/O Is Not First

The current engine still has the classic LMDB durability barrier pattern:

- write dirty pages
- sync data
- write meta
- sync meta

Async I/O can improve how those operations are submitted, but not the facts
that:

- the writer is serialized
- data sync must precede meta publish
- the disk still decides the true durability latency

Group commit changes the system behavior more than async I/O does. That is why
it should come first.

## Done Definition

The roadmap is complete when:

- Zig is the default backend
- C remains only as an escape hatch or compatibility option
- broad repo workloads pass on Zig
- WAL and write-heavy paths have batched/grouped durability
- crash/reopen guarantees are documented and tested
- async I/O is either implemented with measured value or consciously rejected
  with data
