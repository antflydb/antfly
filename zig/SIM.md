# Antfly Simulation Testing Plan

## Goal

Move Antfly simulation testing toward a TigerBeetle-style deterministic
simulator: one process, deterministic seeds, virtual time, mocked I/O, explicit
fault injection, reproducible failures, and fast default test runs. Simulation
coverage should get deeper without making `zig build root-test` spend minutes
on wall-clock sleeps.

## Current Problem

Some metadata HTTP simulation tests model delayed Raft transport with real
`nanosleep` calls inside `DelayingRequestExecutor`. The delay is currently small
per request, but the tests run many rounds across several nodes, and each round
can fan out into many HTTP/Raft requests. That multiplies into minutes of wall
time inside `root-test`.

This is the opposite of a deterministic simulator. Simulation time should be a
data structure, not elapsed wall-clock time.

## Target Model

1. Deterministic single-process simulator
   - Run cluster components inside one process where possible.
   - Use deterministic seeds for workload generation, scheduling, failure
     injection, and message ordering.
   - Print replay information on failure: seed, scenario, operation count, and
     relevant fault schedule.

2. Virtual time
   - Replace wall-clock sleeps in simulation paths with a virtual clock.
   - Express transport latency, retry backoff, lease expiry, reconciliation
     cooldowns, and delayed visibility as simulated ticks.
   - The simulator advances virtual time only when the harness asks it to.

3. Deterministic transport
   - Replace real delayed HTTP transport in simulation tests with a transport
     model that can queue, reorder, duplicate, drop, partition, and release
     messages deterministically.
   - Keep real HTTP listener/executor tests as focused integration tests, not
     the default simulation substrate.

4. Fault model
   - Support crash/restart, leader loss, partitions, delayed delivery, dropped
     messages, duplicated messages, stale reads, storage reopen, and partial
     transition completion.
   - Separate safety mode from liveness mode. Safety mode can inject arbitrary
     faults. Liveness mode eventually heals a quorum/core and proves progress.

5. Workload model
   - Generate table lifecycle operations, range split/merge operations,
     placement changes, public API reads/writes, backup/restore actions, and
     enrichment/indexing workflows from a seed.
   - Keep small deterministic smoke scenarios for fast PR feedback.
   - Run longer randomized campaigns in dedicated simulation/chaos steps.

## Test Step Policy

1. `root-test`
   - Fast root-module compile smoke coverage.
   - No wall-clock sleeps.
   - No simulation campaigns by default.
   - Simulation smoke coverage belongs in focused `*-sim-test` steps until the
     harnesses have TigerBeetle-style virtual time and bounded tick budgets.
   - Broad unit coverage belongs in focused `unit-test` buckets, not in one
     monolithic root-module run that can hit the test runner response timeout.

2. `sim-test`
   - Mocked-time simulation scenarios only.
   - Uses virtual time and deterministic transport.
   - Does not run real HTTP listener/client forwarding scenarios.
   - Does not run storage workload simulations that still exercise real LMDB/WAL
     I/O instead of modeled I/O.

3. `chaos-test`
   - Longer fault campaigns: partitions, restarts, delayed delivery, and
     liveness recovery.
   - Still deterministic and replayable.

4. Real network integration tests
   - Keep direct std HTTP listener/executor coverage focused and small.
   - Do not use real sleeps to simulate latency in unit-style simulation tests.
   - Run through `integration-test` or narrower focused steps, not `sim-test`.

5. Real storage workload simulations
   - Keep deterministic seed/replay behavior.
   - Run through `storage-sim-test` or narrower focused steps until they have a
     modeled storage scheduler and virtual I/O.
   - Longer storage campaigns stay in `storage-sim-soak`.

## Migration Steps

1. Stop real sleeping in simulation delay paths.
   - Make the existing delayed request executor virtual by default.
   - Keep an explicit wall-clock mode only for focused integration tests that
     truly need real elapsed time.

2. Add simulator controls to the test runner.
   - Allow `root-test` to skip known slow chaos scenarios by default.
   - Keep the skipped scenarios reachable through `chaos-test` and explicit
     filters.

3. Introduce a virtual transport queue.
   - Model request delivery as scheduled events.
   - Add deterministic release policies: FIFO, random-by-seed, delayed ticks,
     drop, duplicate, and partition.

4. Move metadata HTTP simulations onto the virtual transport.
   - Convert delayed transport tests first because they are the current slow
     path.
   - Then convert restart/partition tests to share the same event scheduler.

5. Consolidate simulation harness APIs.
   - Expose one cluster simulation driver with `step`, `runUntil`, `inject`,
     `heal`, and `assertProgress` helpers.
   - Remove ad hoc round loops where possible.

6. Add seed replay.
   - Every generated scenario must print a replay command on failure.
   - Store minimal failing state in logs, not large generated fixtures.

7. Classify existing simulation tests.
   - Fast deterministic: stays in `sim-test` and may be part of `unit-test`.
   - Long chaos/liveness: moves to `chaos-test`.
   - Real I/O integration: gets a focused integration step.

## Immediate Fix

The immediate fix is to remove wall-clock sleep from the current delayed
request executor and ensure `root-test` has an explicit skip path for simulation
families that are not yet VOPR-style bounded runs. That makes the default test
path fast again while the larger VOPR migration replaces the underlying
transport with a true virtual event queue.

Implemented now:

- `DelayingRequestExecutor.init` records virtual elapsed time and request count
  instead of calling `nanosleep`; `initWallClock` is available only for explicit
  real-delay integration checks.
- The custom root test runner accepts repeated `--skip-test-filter` arguments.
- `root-test` skips the current Antfly simulation families by default:
  metadata HTTP cluster sims, Raft host/cluster sims, LSM backend sims, and the
  storage persistent/WAL/index-manager/DB-split randomized sim workloads.
- `root-test` also skips the long HBC recall fixture tests; those belong in
  `recall-harness` rather than the default unit path.
- `root-test` defaults to root-module compile smoke filters instead of compiling
  and running every test reachable from `root.zig`.
- `sim-test` owns only fast mocked-time simulation steps. At the moment that
  means the narrowed `lib-raft-sim-test` virtual-time smoke/invariant bucket
  and `lib-metadata-sim-smoke-test`, which runs a small metadata virtual Raft
  transport smoke set. It now also depends on `storage-vopr-test`, so modeled
  storage I/O coverage is part of the default fast sim bucket.
- `lib-raft-sim-test` is narrowed to quick deterministic Raft smoke/invariant
  scenarios; longer restart, real-HTTP, and multi-transition Raft campaigns are
  reachable through `lib-raft-chaos-test` and `chaos-test`.
- `go/pkg/antfly/lib/raft` runtime scheduling now exposes explicit virtual time:
  `Scheduler.advanceVirtualTime`, `MultiRaft.virtualRound`, and
  `MultiRaft.virtualTimeMs`. `runRound` advances this mocked time and passes it
  to transports that implement `advance_time_ms`.
- The standard HTTP listener shutdown path wakes the accept loop before closing
  the listening socket, avoiding debug `BADF` panics during focused real-HTTP
  tests.
- `integration-test` owns the focused real HTTP metadata/public API suites that
  are not deterministic VOPR-style simulations yet.
- `lib-metadata-sim-core-test` owns the full deterministic metadata
  virtual-transport scenario set. It is no longer real network backed, but it is
  still too broad for the default `sim-test` runtime budget.
- `storage-sim-test` owns the deterministic storage workload simulations that
  still use real storage I/O.
- `unit-test-progress` no longer runs the core metadata real-HTTP sim bucket.
- The metadata cluster harness now injects a `VirtualHttpNetwork` into each
  simulated Raft node. Raft frames still go through the HTTP codec/server route,
  but delivery is in-process through `sim://raft-node/<id>` endpoints instead
  of listener threads and sockets.
- `VirtualHttpNetwork` can inject and heal deterministic target-node
  partitions. `ManagedHttpClusterSimulation` exposes the first shared
  TigerBeetle-style driver helpers: `step`, `runUntil`, `assertProgress`,
  `inject`, `heal`, and `healAll`.
- The virtual transport now supports queued delivery for cluster sims. Raft
  frame sends still get synchronous acceptance from the HTTP frame driver, but
  delivery is modeled as deep-copied queued events drained by cluster `step`
  with virtual ticks.
- The queued transport supports drop-next, duplicate-next, seeded random drop,
  FIFO or seed-randomized release ordering, target-node partitions, and virtual
  delay ticks through the cluster `inject` API. The remaining transport work is
  broader conversion of ad hoc metadata loops onto the shared progress helpers.
- Raft frame sends now carry `source_id` through the frame driver and HTTP
  request, so the virtual network can model directed source-to-target link
  partitions without inferring source nodes from target URIs.
- `lib-metadata-vopr-test` now runs a seeded metadata VOPR campaign over the
  virtual HTTP/Raft transport. Each seed generates deterministic transport
  faults, restarts, heals the cluster for a liveness phase, creates table
  topology, drives a split intent through the control loop, and prints seed,
  operation index, action, and replay command on failure.
- `lib-metadata-vopr-chaos-test` runs the expanded metadata VOPR generated
  workload behind `chaos-test`, including table lifecycle, merge, placement
  churn, topology updates, multi-operation partitions, and leader restart
  during split.
- `sim-test` includes the fast metadata VOPR campaign alongside metadata smoke,
  Raft sim, and modeled storage VOPR coverage.
- The metadata cluster harness now has metadata-aware `runUntil` and
  `assertProgress` helpers. The common metadata leader/status wait helpers use
  that shared virtual-time progress loop, and the split/merge intent smoke
  scenarios now use shared transition-progress predicates instead of local
  polling loops.

### Metadata VOPR Task List

- [x] Add queued virtual HTTP transport with deterministic delivery/fault
  controls.
- [x] Carry Raft frame source IDs so directed link partitions are modeled
  explicitly.
- [x] Add a fast seeded metadata VOPR campaign with replay logging and a
  heal-then-progress liveness phase.
- [x] Move common metadata leader and hosted-replica wait helpers onto shared
  `runUntil`/`assertProgress` helpers.
- [x] Convert the remaining scenario-local ad hoc loops onto the shared
  progress helpers.
  - [x] Convert reusable public-API route/lookup/count-profile/median-key waits.
  - [x] Convert group leader and metadata-leader-excluding waits.
  - [x] Convert projected table/index propagation waits.
  - [x] Convert automatic split/merge finalize and topology-publication waits.
  - [x] Leave only helper-internal loops, VOPR retry loops, and true fixed-step
    fuzz/fault loops as explicit `stepAll` loops.
- [x] Expand the metadata VOPR workload model beyond post-fault table/split
  progress to generated table lifecycle, merge, topology, and placement
  changes.
  - [x] Generate table create/drop/recreate operations.
  - [x] Generate split and merge operations.
  - [x] Generate placement candidate churn.
  - [x] Generate store/node topology changes.
  - [x] Inject leader restart during a transition.
  - [x] Support multi-operation partitions with explicit heal operations.
- [x] Move longer metadata VOPR/chaos campaigns behind `chaos-test`, keeping
  `sim-test` bounded and replayable.

## Storage Modeled I/O Plan

For concrete storage simulation targets, fixture layout, replay artifact
promotion, and LMDB/WAL/persistent/index-manager/DB-split workflow details, use
[pkg/antfly/src/storage/SIM.md](pkg/antfly/src/storage/SIM.md).

The storage sims are currently deterministic workload generators running
against real storage implementations. They use real files, real LMDB/WAL paths,
real async commit backends, and some real sleeps/backoff. They should stay in
`storage-sim-test` until their I/O substrate is modeled.

### Storage VOPR Task List

- [x] Add a reusable storage simulation runtime with virtual time and scheduled
  events.
- [x] Add a modeled in-memory storage device with explicit volatile/durable
  state and crash behavior.
- [x] Add focused build targets for modeled storage work:
  `storage-sim-runtime-test`, `wal-vopr-test`, and `storage-vopr-test`.
- [x] Inject a virtual clock into WAL without changing production defaults.
- [x] Prove WAL group-commit coalescing can advance modeled time instead of
  sleeping the host.
- [x] Route WAL LSM persistence through the modeled device API for focused VOPR
  tests.
- [x] Model WAL LSM commit-completion delay on virtual time using
  `artificial_sync_delay_ns`.
- [x] Force full LSM-backed WAL commits to flush before acknowledging append,
  and cover modeled crash-before-close reopen.
- [x] Add WAL modeled replay/crash runners that reuse the existing action
  vocabulary on virtual storage.
- [x] Add a fast deterministic WAL modeled-storage VOPR campaign to
  `storage-vopr-test`.
- [x] Add a WAL commit-completion scheduler seam so modeled tests advance
  completion delays through the virtual runtime event queue.
- [x] Run existing WAL replay fixtures against modeled LSM storage and virtual
  time.
- [x] Move WAL commit backend completion timing onto modeled scheduled
  completions in sim mode.
- [x] Run existing WAL crash fixtures against modeled LSM storage at the
  expected outcome level.
- [x] Port WAL crash/replay fixtures from LMDB publish-phase hooks to modeled
  durable/volatile state.
- [x] Move persistent replay fixtures and a fast persistent workload onto the
  modeled WAL/device layer.
- [x] Move index-manager replay fixtures onto modeled persistent text storage.
- [x] Move DB split replay fixtures onto modeled DB primary and index storage.
- [x] Port persistent crash publish-phase fixtures to modeled durable/volatile
  state.
- [x] Move index-manager crash fixtures onto modeled persistent text storage.
- [x] Move DB split crash/soak sims onto modeled storage once their
  publish-phase hooks are modeled.
- [x] Move LSM backend sims onto the modeled device API for block/cache/storage
  I/O faults.
- [x] Add modeled compaction coverage for LSM data files and full-text segment
  storage.
  - [x] Generate write/delete/overwrite/flush/compact/reopen workloads with an
    in-memory oracle for visible document state.
  - [x] Cover full-text segment compaction invariants: stale terms disappear,
    updated docs do not appear twice, postings merge across segments, and
    all-deleted segments are retired.
  - [x] Inject modeled crash/fault points before compacted segment publish,
    after compacted segment write, after manifest/catalog update, and during old
    segment cleanup.
    - [x] LSM modeled-storage smoke covers compaction run write failure,
      manifest sync failure, and obsolete-run cleanup failure with crash/reopen
      recovery.
    - [x] Full-text persistent segment storage now covers segment write fault,
      catalog sync fault, post-catalog-publish crash/reopen, and old segment
      cleanup fault with a modeled device.
  - [x] Keep a fast seeded compaction smoke in `sim-test` and longer randomized
    compaction campaigns in `chaos-test`.
    - [x] Fast seeded compaction smoke is covered by `storage-vopr-test` through
      the existing LSM backend simulation target.
    - [x] Longer randomized LSM compaction campaigns are wired into
      `chaos-test`.

1. Add `StorageSimRuntime`
   - Own virtual time, deterministic RNG, an event queue, and the fault schedule.
   - Advance with explicit `tick`/`runUntil` calls instead of host sleeps.
   - Print seed, operation index, and fault schedule on failure.

2. Introduce a modeled device API
   - Define an interface for `read`, `write`, `sync`, `truncate`, `rename`,
     `remove`, and `list`.
   - Production implementations can remain POSIX/LMDB backed.
   - VOPR tests use an in-memory modeled device with deterministic completion.

3. Model durability explicitly
   - Keep separate volatile and durable state.
   - `write` updates volatile state.
   - `sync` promotes selected files/ranges to durable state.
   - `crash` drops volatile state and reopens from durable state.
   - Fault injection can drop, reorder, delay, tear, truncate, or fail
     writes/syncs.

4. Replace storage sleeps with scheduled completions
   - WAL group commit should schedule a commit at
     `now + group_commit_window_ns`.
   - Worker-thread and async-I/O commit modes should become deterministic event
     completions in sim mode.
   - Backoff/yield loops should advance the sim scheduler, not call
     `nanosleep`, `Clock.real().sleepMs`, or `std.Thread.sleep`.

5. Inject time into storage paths
   - Storage code that matters to sims should receive a clock or runtime handle
     through options/config.
   - `platform_time.monotonicNs()` and `Clock.real()` are fine for production and
     benchmarks, but sim paths should use the virtual clock.
   - Profiling timestamps should be disabled or virtual in sim mode so they do
     not affect behavior.

6. Convert by layer
   - Start with WAL because it already has crash/replay structure and explicit
     commit phases.
   - Move persistent storage next because it composes WAL/LMDB behavior.
   - Move index-manager and DB split sims after the lower layers can run on a
     modeled device.
   - Move LSM after the device API can model block/cache/storage I/O.

7. Keep test buckets honest
   - `sim-test`: fast mocked-time/model-I/O smoke and invariant checks.
   - `storage-sim-test`: legacy deterministic but real-I/O storage workload
     sims kept out of the default fast sim bucket.
   - `storage-vopr-test`: modeled-I/O randomized and smoke campaigns included
     by `sim-test`.
   - `storage-sim-soak`: longer real or modeled campaigns, never default.

8. Add compaction VOPR coverage
   - Treat compaction as a modeled publish protocol, not only as a background
     cleanup. Tests should observe the same logical state before and after
     compaction, across flush boundaries, restarts, and partially completed
     publish phases.
   - Drive LSM workloads with generated puts, deletes, overwrites, flushes,
     compactions, crashes, and reopens. Keep an in-memory oracle for the latest
     visible key/document state and assert point reads and range scans after
     every generated operation.
   - Drive full-text segment workloads with generated document inserts, updates,
     deletes, term queries, prefix/range queries, flushes, and compactions. The
     oracle should track per-document visible fields and derive expected query
     results from that model.
   - Fault points should include before writing the compacted segment, after
     writing but before syncing it, after syncing it but before manifest/catalog
     publish, after publish but before old segment cleanup, during cleanup, and
     during reopen. Recovery must never expose both old and new versions as
     live, lose acknowledged data, or resurrect deleted documents.
   - Fast coverage belongs in `storage-vopr-test`/`sim-test` with small seeds and
     bounded operation counts. Larger generated compaction campaigns, aggressive
     fault matrices, and multi-level full-text segment churn belong in
     `chaos-test`.

The design rule is that simulation time and persistence must be data in the
harness, not effects of the host OS. Once WAL has this seam, the rest of the
storage sims can migrate incrementally without faking the whole DB at once.
