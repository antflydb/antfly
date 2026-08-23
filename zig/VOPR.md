# VOPR: Deterministic Autonomous Testing for Antfly

Status (2026-08-23): the common VOPR engine, its deterministic `std.Io`
runtime, Phases 0 through 5, and the independent metadata, transaction, Raft,
storage, HA, data-plane, derived-workflow, backup/restore, and clock-fault
scenarios are implemented. The production DataServer now serves public HTTP on
borrowed `VoprIo`; background-owner lifecycle, serverless object-store faults,
resource admission, datagrams, corpus quarantine, multiverse artifacts, and a
scriptable/interactive debugger are executable. Routed data, Raft, split/merge,
query assembly, and DataServer-owned maintenance services now expose
production-safe scheduler boundaries. Campaigns export bounded counterfactual
graphs and explicit quarantine manifests; the virtual filesystem models
persistent sector corruption and torn synchronization.

Scope: Zig Antfly simulation, VOPR, modeled-storage, and deterministic chaos
testing. This is the living design and operating policy. Historical phase
progress remains available in git history.

## Purpose

VOPR is Antfly's self-contained deterministic simulation platform. It explores
workload, scheduling, fault, and parameter choices; runs real state machines
against modeled operating-system services; evaluates named properties; retains
interesting histories; and produces exact replay, reduction, formal-trace, and
debugging artifacts.

The reusable engine is application independent and lives in `lib/vopr`.
Antfly scenarios, fixtures, audits, command policy, and production adapters
live under `pkg/antfly/src`. Production components reach the simulator through
narrow interfaces, primarily `std.Io`; they never import the explorer.

The goal is not to reproduce a general-purpose hypervisor. VOPR virtualizes the
nondeterminism Antfly and registered in-process dependencies actually use. A
native process or real-network run remains a differential compatibility layer,
not a prerequisite for deterministic search.

## Implemented Conformance

"Implemented" means that the behavior has an executable test or command path.

| Capability | Implementation evidence | Verification |
| --- | --- | --- |
| Stable structured choices and exact clean-world replay | `lib/vopr/src/choice.zig`, `runner.zig`, `replay.zig`, canonical `vopr-trace-v1` | `zig build vopr-engine-test` |
| One-transition scheduling and typed termination | `scheduler.zig`, `scenario.zig`, `outcome.zig` | `zig build vopr-engine-test` |
| Antfly-independent runtime boundary | `runtime.zig`, `sim_runtime.zig`; `VoprIo` composes the narrow atomic executor into its scheduler; Antfly `DurableJobLane` adapter | `zig build vopr-engine-test vopr-runtime-test` |
| Deterministic `std.Io` tasks and synchronization | `vopr_io.zig`, `vopr_io_task.zig` | `zig build vopr-engine-test` in Debug and ReleaseSafe |
| Modeled files, durability, persistent sector corruption, torn synchronization, streams, datagrams, processes, and quotas | `vopr_io_file.zig`, `vopr_io_net.zig`, `vopr_io_process.zig` | `zig build vopr-engine-test` |
| Stable optional safepoints | `vopr_io_instrumentation.zig` | `zig build vopr-engine-test` |
| Clocks, timers, storage completions, and lifecycle faults | `time.zig`, `clock_fault.zig`, `fault.zig`, storage `sim_runtime.zig` | `zig build vopr-engine-test storage-vopr-runtime-test` |
| Properties, observations, semantic coverage, cross-revision corpus quarantine, property history, and guided search | `property.zig`, `observation.zig`, `coverage.zig`, `corpus.zig`, `explorer.zig` | `zig build vopr-engine-test vopr-benchmark` |
| Replay-before-retention campaigns, deterministic workers, bounded counterfactual graphs, and quarantine manifests/raw artifacts | Antfly `vopr/cli.zig` | `zig build vopr-meta-test` |
| Same-fingerprint reduction, promotion, and migration | `reducer.zig`, `fixture.zig`, `vopr-reduce`, `vopr-promote`, `vopr-migrate` | `zig build vopr-engine-test vopr-meta-test` |
| TLA+ export | transaction and Raft `vopr-tla` dispatch | `zig build transaction-vopr-test vopr-meta-test` |
| Ranked and explicitly budgeted counterfactual causality, persistent multiverse identities, clean-replay branching, and scriptable/interactive debug sessions | `causal.zig`, `multiverse.zig`, `debugger.zig`, `collector.zig`, `vopr-debug` | `zig build vopr-engine-test vopr-meta-test` |
| Metadata and acknowledged distributed-data durability | real metadata/Raft paths plus modeled storage | `zig build lib-metadata-vopr-test lib-metadata-vopr-data-test` |
| Per-group Raft scheduling | real `RawNode` message, persist, apply, restart, partition, proposal, and compaction choices | `zig build raft-vopr-test` |
| Storage differential and real-backend campaigns | WAL, LMDB, LSM, persistent index, index manager, and DB split | `zig build storage-vopr-test` |
| HA lifecycle | replication, fencing, promotion, retention, restart, and rejoin | `zig build ha-vopr-test ha-chaos-test` |
| Independent application domains | distributed transaction, data plane, derived workflow, backup/restore, and clock faults | their five focused `*-vopr-test` gates |
| Production public HTTP on deterministic I/O | `vopr/data_server.zig`, `vopr/http_lifecycle.zig`, borrowed `HttpRuntime` and `BackendRuntime` lanes, transport-neutral metadata executor; chunked upload, keep-alive pipeline, streaming response, and half-close | `zig build data-server-vopr-test` |
| Production background ownership and admission | `background_runtime.zig`, `vopr_durable_job_lane.zig`; transaction recovery, TTL, enrichment, text merge, sparse compaction, resolution, promotion, LSM maintenance, quarantine retry, repair, DataServer warmup/catch-up/root/status refresh, and auto-bulk finish work on borrowed `std.Io`/shared owners; `vopr/admission.zig` | `zig build storage-vopr-runtime-test vopr-runtime-test data-server-vopr-test admission-vopr-test` |
| Real serverless object-store protocols under deterministic provider faults | `objectstore/scripted_fault.zig`, Antfly `vopr/object_store.zig` | `zig build lib-objectstore-test serverless-object-store-vopr-test` |

The real DataServer listener, httpx client/server transport, request lifecycle,
deadline, shutdown, and partial writes now execute as deterministic `VoprIo`
transitions. Routed data and Raft operations remain deliberately conservative
where production has not yet exposed a safe suspension point. VOPR must not
counterfeit concurrency by opening a competing writer or bypassing production
lease ownership. Ordinary socket reads and writes always use the injected
`std.Io`; a runtime-proven native server handle may use kernel timeout options
for scalable blocking I/O, while borrowed/virtual handles use logical
Select-based timeouts and never reach POSIX with a virtual descriptor.

## Design Influences

The design adopts the useful application-level parts of Antithesis:

- deterministic replay of controlled nondeterminism
- small compatible commands and independently overlapping faults
- non-fatal safety, reachability, exercise-quality, and recovery properties
- coverage- and property-guided state-space exploration
- branching timelines and counterfactual debugging

References:

- [How Antithesis works](https://antithesis.com/docs/introduction/how_antithesis_works/)
- [Deterministic simulation testing](https://antithesis.com/docs/resources/deterministic_simulation_testing/)
- [Assertions](https://antithesis.com/docs/product/writing_tests/assertions/)
- [Controlling faults](https://antithesis.com/docs/product/writing_tests/controlling_faults/)
- [Debugging](https://antithesis.com/docs/product/debugging/)

These are influences, not a claim of hypervisor equivalence or identical search
algorithms.

## Goals and Non-Goals

### Goals

- Reproduce every retained failure from an explicit artifact, not merely a
  seed.
- Put workload, message, task, timer, storage, node, and fault interleavings
  under one scheduler.
- Run production components unchanged on `std.Io.Threaded` or deterministic
  `VoprIo` where their capability requirements are supported.
- Express correctness as stable named properties and distinguish product
  failures from harness failures.
- Guide exploration with semantic state, transition, property, and optional
  instrumentation feedback.
- Minimize failures while preserving their identity and promote only reviewed,
  replay-proven fixtures.
- Keep the entire workflow runnable locally and in CI without a hosted service.
- Reuse existing Raft, HA, LSM, storage, transaction, integration, and formal
  oracles rather than replacing them.

### Non-Goals

- A machine-code VM capable of running arbitrary binaries or operating systems.
- Raw heap, thread-stack, socket, or process snapshots as replay truth.
- Silent fallback to host threads, clocks, entropy, files, sockets, or
  processes.
- Proving liveness while a deliberately unrecoverable fault remains active.
- Replacing focused unit, integration, differential, formal, or native chaos
  tests.
- Making a nightly wall-clock budget deterministic. Every completed history,
  rather than the number completed, must be deterministic.
- Automatically committing generated fixtures.

## Architecture

```text
campaign / replay / reducer / debugger
                  |
        explicit ChoiceSource
                  |
      one-transition Scheduler
                  |
      Scenario + named Properties
                  |
 narrow Runtime / std.Io capability boundary
                  |
 VoprIo tasks, clocks, files, sockets, processes, quotas
                  |
 Antfly metadata, Raft, storage, HA, and application adapters
```

### Package Boundary

Use `lib/vopr`, not a generic `lib/sim`, for the reusable engine. The VOPR name
communicates stable replay, scheduling, property, corpus, and reduction
contracts. A component should move into another independent library only when
it is useful without VOPR campaign semantics, such as a general modeled block
device.

The dependency rules are:

- `lib/vopr` imports no Antfly metadata, Raft integration, storage, or API code.
- Antfly scenarios import VOPR and their production domains.
- Production code depends only on narrow runtime, clock, entropy, transport,
  storage, and executor interfaces.
- Test and campaign policy stays in `pkg/antfly/src/vopr`.

Key layout:

```text
lib/vopr/src/
  choice.zig                 scenario.zig
  scheduler.zig              runner.zig
  runtime.zig                sim_runtime.zig
  vopr_io.zig                 vopr_io_task.zig
  vopr_io_file.zig            vopr_io_net.zig
  vopr_io_process.zig         vopr_io_instrumentation.zig
  time.zig                   clock_fault.zig
  fault.zig                  property.zig
  observation.zig            coverage.zig
  corpus.zig                 explorer.zig
  trace.zig                  replay.zig
  reducer.zig                fixture.zig
  snapshot.zig               splice.zig
  causal.zig                 multiverse.zig
  debugger.zig               collector.zig
  vopr-trace-v1.schema.json

pkg/antfly/src/vopr/
  DETERMINISM_AUDIT.md
  cli.zig
  cli_runner.zig
  domain_vopr.zig
  request_lifecycle.zig
  data_server.zig
  object_store.zig
  admission.zig
  fixtures/<scenario>/
```

`VoprIo`, the `vopr_io*.zig` modules, `pkg/antfly/src/vopr`, `vopr_tests`
identifiers, and `vopr-*` build steps are canonical. `vopr-test` is the fast
Antfly aggregate; `vopr-engine-test` is the focused application-independent
engine gate. The old `sim-*` CLI, contract-test, and aggregate steps remain
temporary workflow aliases. Older randomized real-I/O suites use
`workload`/`integration` terminology when renamed; they do not become VOPR
suites merely by changing a label. Serialized `sim-io-*` backend identifiers
remain unchanged because they are part of the `vopr-trace-v1` replay ABI, not
source names.

Domain adapters may remain beside their production domains when that preserves
the cleanest dependency direction.

## Determinism and Replay Contract

A history is a sequence of explicit decisions, not the output of a seed. Seeds
are discovery metadata. Each choice record contains:

- a stable namespaced choice-site ID
- a site occurrence number
- the complete stable enabled-alternative set
- the selected alternative
- structured parameter values where applicable

Choice namespaces distinguish scheduler, workload, fault, parameter, entropy,
and recovery decisions. Pointer values, source line numbers, container
iteration order, wall time, and native thread identity are forbidden from
stable IDs.

At replay, a clean world consumes the recorded choices and verifies the choice
site, enabled set, selected alternative, transition identity, configured
observations, and expected outcome. The first mismatch is
`ReplayDiverged`—a compatibility result, never a product failure.

Determinism applies to:

- initial configuration and fixture bytes
- entropy and IDs that can affect behavior
- runnable task and transition selection
- timer and storage completion delivery
- network packet delivery and waiter wake order
- filesystem directory order and modeled durability
- fault start, continuation, healing, and overlap
- property encounters and observation digests

Every retained or promoted artifact is exact-replayed from a clean world before
it affects corpus state.

## Scheduler and Logical Concurrency

The scheduler selects exactly one enabled transition at a time. Typical
transitions include a workload command, runnable task, message delivery, timer,
storage completion, node lifecycle action, fault action, or recovery step.

Actors expose small start, poll, completion, cancellation, and cleanup steps.
This provides deterministic logical concurrency without requiring native
threads. Large atomic operations are acceptable only when the production
interface does not yet expose a safe narrower seam; such boundaries must be
documented explicitly.

Transition outcomes distinguish:

- completed work
- expected product rejection
- blocked with enabled future work
- quiescent success
- property failure
- replay divergence
- harness capability or validity error
- transition or resource budget exhaustion

The runner must never report a harness error as an Antfly correctness failure.

## `VoprIo` Runtime

`VoprIo` is an application-scoped deterministic virtual OS implemented as a real
`std.Io.VTable`. The same production component should be able to use
`std.Io.Threaded` or `VoprIo` without a second simulation-only business-logic
implementation.

### Fail-Closed Capability Model

Construction preflights declared capabilities. Unsupported operations return
their documented error or latch a harness violation when the vtable operation
cannot return an error. No handler aliases or delegates to `std.Io.Threaded`.
The backend identity pins the Zig version, supported capability set, virtual-OS
model, and instrumentation map.

### Implemented Task Kernel

Stackful simulated tasks support futures, groups, await, cancellation, sleep,
futexes, queues, mutexes, and selection. Runnable-task selection, park, wake,
cancel, spurious wake, eager completion, timer delivery, and futex waiter
selection are scheduler-visible stable choices.

### Implemented Files

Virtual integer handles provide directories, deterministic iteration, recursive
rename, positional and streaming I/O, atomic publication, locks, mappings,
metadata, descriptor and capacity limits, partial I/O, data sync, separate
namespace sync, dropped sync, precise one-shot read-range corruption, and crash
reconstruction from durable state.

Symlinks, hard links, and optimized file-to-file transfer currently fail
closed. Modeled storage distinguishes volatile from durable state; a crash
drops volatile state rather than invoking production close paths.

### Implemented Networking

IP and Unix listen/connect/accept, socket pairs, stream reads and writes,
half-close, close, deterministic loopback resolution, bounded send queues,
partial writes, and packet delivery are modeled in memory. Drop, duplicate,
reorder, outage, jam, directional partition, arbitrary delay, and backpressure
are explicit model state. Bound UDP datagrams preserve message boundaries and
source addresses while sharing scheduler-visible drop, duplicate, reorder, and
delivery behavior.

### Implemented Processes and Resources

Only registered in-process entrypoints may spawn. Unknown executables fail
deterministically. Child arguments and virtual process identity are owned by
the model; wait, kill, pause, resume, cancellation, CPU-work budgets, process
limits, file descriptors, sockets, storage capacity, send buffers, and
allocator limits are modeled.

### Instrumentation

Optional stable safepoints count hits and may yield the current fiber. Their map
digest participates in compatibility only when enabled. Instrumentation may
improve search efficiency but cannot change the meaning of an already
compatible trace.

## Fault Model

Faults have explicit start, active, and heal lifecycles. Independent faults may
overlap subject to scenario budgets and preconditions.

Supported fault families include:

- network drop, duplicate, reorder, delay, jam, outage, and directional or
  node partition
- node pause, crash, restart, and leadership loss
- monotonic advance, realtime jump or skew, oscillator-rate change, and
  deferred timer delivery
- write, sync, rename, delete, capacity, namespace-durability, partial-write,
  crash, and selected storage-completion faults
- process, task, CPU-work, allocator, descriptor, socket, queue, and storage
  resource limits
- scenario-specific lease expiry, stale owner, provider outcome, and
  publication-phase faults

Crashing a node and crashing its durable device are related but distinct
choices. Realtime and monotonic time are also distinct domains.

Each scenario declares maximum active faults, which combinations are valid,
minimum surviving capacity or quorum, and its healing policy. State-aware
preconditions prevent campaigns from spending most of their budget in invalid
or permanently unrecoverable worlds.

## Workloads, Properties, and Coverage

Workload commands are small, typed, and state aware. Candidate commands expose
stable IDs and preconditions; the explorer chooses only among enabled commands.
Large scripted regressions remain useful corpus seeds but are not the scheduler
abstraction.

Properties are registered before execution and evaluated non-fatally where
continuing is safe. Supported meanings include:

- `always`: encountered at least once and never false
- `always_or_unreachable`: never false when encountered
- `reachable` and `unreachable`
- `sometimes`: true at least once during the history
- `eventually_after_quiescence`: true after the deterministic recovery phase

Properties have stable IDs independent of their human-readable messages.
Online checks cover safety; final-state and liveness checks run after a quiet
suffix. Failures use stable fingerprints containing failure class, property ID
or normalized error identity, scenario version, optional domain identity, and
optional canonical observation digest.

Semantic observations include topology shapes, Raft roles and progress,
message and storage states, active fault combinations, workload phases,
property encounters, ownership generations, queue pressure, and domain-specific
states. Corpus retention uses new states, transitions, property outcomes, and
fault/workload combinations. Optional instrumentation coverage is secondary
feedback and not replay input.

## Exploration, Artifacts, and Debugging

The campaign loop starts from reviewed fixtures and retained replayable corpus
entries, mutates structured choices, branches after an exact prefix, generates
a suffix, runs a deterministic quiet phase where configured, and exact-replays
any candidate before retention.

Mutation supports decision replacement, range deletion, fault simplification,
workload and configuration shrinking, scheduling simplification, and compatible
prefix/suffix splicing. Multiple workers receive deterministic history IDs and
seeds; worker completion order does not decide canonical corpus contents.

### Artifact Contract

`vopr-trace-v1` NDJSON records a versioned header and configuration, choices,
transitions, faults, events, observations, property encounters, failures, and a
summary. The decision stream is authoritative; events and observations support
diagnostics, coverage, and formal export.

Generated artifacts normally live under:

```text
/tmp/antfly-vopr/<campaign>/<history-id>.voprtrace
```

Reviewed promoted fixtures live under:

```text
pkg/antfly/src/vopr/fixtures/<scenario>/<name>.voprtrace
```

Promotion is explicit. Migration must replay the source, translate it, replay
the target, and verify semantic outcome equivalence before writing a new
canonical artifact. Readers continue to accept `.simtrace` as a legacy filename
extension; the content format and serialized `sim-io-*` identities remain the
versioned replay ABI.

### Reduction

Reduction always starts from a clean world and preserves the target fingerprint.
It proceeds from broad decision-range deletion through fault, workload,
scheduling, and configuration simplification. Divergence, a harness error, or a
different property failure does not reproduce the target.

### Branching and Checkpoints

Rewind reconstructs a clean world and replays an explicit prefix. Logical
scenario snapshots may accelerate this only when their configuration and prefix
digests match and restore is replay-proven. Raw heap or fiber-stack copying is
never replay truth.

### Causality and Debugger Primitives

Bounded counterfactual analysis replaces a selected pre-failure decision,
explores deterministic descendants, exact-replays every child, ranks
failure-probability reductions, and records stable experiment IDs, trial-seed
digests, and a pointer-free parent/child multiverse graph. Prefix,
descendant-per-alternative, and total-experiment limits bound the analysis even
when a choice site has a large enabled set. The debugger cursor
can seek a choice prefix, list recorded alternatives, create and verify a child
branch, and collect deterministic state before, at, and after a failure.
`vopr-debug` exports replay-validated snapshots and provides the same
line-oriented `show`, `seek`, `causal`, `causal-window`, `collector`, `branch`,
and `compare` commands through a command file or interactive standard input.
Collector commands are available for scenarios that implement the generic
collector contract; unsupported scenarios fail closed.

## Quiet Suffix and Liveness

Safety is checked during active faults. A scenario validates liveness only
after this explicit deterministic recovery protocol:

1. Stop starting new workload operations.
2. Heal faults the scenario promises are recoverable.
3. Restart nodes covered by the recovery contract.
4. Restore normal network and resource policy.
5. Advance enabled transitions under a documented fair policy.
6. Stop on the recovery predicate, quiescence, or the transition budget.
7. Evaluate eventual and final-state properties.

Every heal and recovery transition is recorded. The runner never mutates
product state to make recovery succeed.

## Implemented Scenario Inventory

These are independent domains with durable CLI identities, scenario ABIs,
fixture namespaces, focused gates, and production-regression dependencies.
`domain-vopr-test` is only a lightweight convenience aggregate; it is not an
owner or prerequisite of the five application domains.

### Metadata and Distributed Data

The metadata scenario schedules virtual Raft HTTP delivery, partitions, drop,
duplication, delay, reordering, node pause/restart, table lifecycle, placement,
topology, split, merge, and a quiet recovery phase.

The distributed-data integration composes real public API writes and reads,
split, partition/restart, modeled durable-device crash/recovery, merge, and an
acknowledged-operation oracle. A focused production composition additionally
runs the real DataServer public listener and `/healthz` request through httpx on
borrowed `VoprIo`, including partial writes and deadline-first shutdown. Routed
write/read, Raft, and split/merge internals remain the next microstep boundary.

Focused gates: `lib-metadata-vopr-test`,
`lib-metadata-vopr-data-test`, `data-server-vopr-test`, and
`metadata-vopr-replay-stability-test`.

### Transaction

The focused transaction scenario exports TLA+ events. The independent
distributed-transaction scenario drives three production `TxnManager`
instances over independent Antfly stores: coordinator and participant setup,
prepare, durable decision, ambiguous response, crash/reopen, lease adoption,
stale-owner conflict, phase-two delivery, acknowledgement, and repair. Its
oracle reads production transaction records and visible values.

Focused gates: `transaction-vopr-test` and
`distributed-transaction-vopr-test`.

### Raft

The real `RawNode` cluster exposes message delivery/drop, deferred persistence
and application, proposal, restart, partition, and compaction independently.
Existing Raft invariant, differential, snapshot, and TLA+ checks remain
complementary gates and corpus sources.

Focused gate: `raft-vopr-test`.

### Storage: WAL, LMDB, LSM, Persistent, Index Manager, and DB Split

Storage scenarios adapt their real action vocabularies and oracles to the
common choice, artifact, replay, campaign, and fault lifecycle. Coverage
includes C-versus-Zig LMDB, memory-versus-real LSM, WAL publication and reopen,
real PersistentIndex, split IndexManager, full DB split, maintenance,
compaction, crash recovery, volatile/durable state, and typed storage faults.

Focused aggregate: `storage-vopr-test`. Exact fixture and legacy real-I/O
commands are documented under Test-Tier Policy below.

### HA

The HA scenario drives real primary and standby logs, progress WALs, slot and
fence stores, replication, application, partition, crash, retention, backup,
promotion, rejoin assessment, stale-owner fencing, and ordered applied-prefix
properties.

Focused gates: `ha-vopr-test` and `ha-chaos-test`.

### Data Plane

The independent modeled scenario decomposes admission, route, virtual packet,
Raft-log persistence, application, acknowledgement, writer-epoch handoff,
split copy/cutover, and point/query visibility. It directly consumes `VoprIo`
sockets and durable modeled files. Its focused gate also composes the real
metadata distributed-data and per-group Raft suites.

Focused gate: `data-plane-vopr-test`.

### Derived Workflows

The scenario schedules checkpoint, generation publication, repair, compaction,
cancellation, cleanup, and leadership fencing through the production
`DurableJobLane` adapter. Its focused gate composes real enrichment, index
lifecycle, repair, and background-lane regressions.

Focused gate: `derived-workflow-vopr-test`.

### Backup and Restore

The scenario models partial and duplicate transfer, crash/resume, manifest
publication, retention pins, durable restore jobs, download, topology
reconstruction, activation versus cancellation, and generation GC. It
round-trips and verifies the production HA backup manifest; the focused gate
also runs portable, restore-job, Raft restore, standalone, and HA regressions.

Focused gate: `backup-restore-vopr-test`.

### Clock Faults

The Antfly-independent clock surface separates realtime jumps, oscillator
frequency, node pause, monotonic passage, timer delivery, and stabilization.
The focused gate composes production TTL, transaction lease, HA retention, and
seed-lifecycle regressions.

Focused gate: `clock-fault-vopr-test`.

## Defects Found

VOPR work has found concrete production and harness defects:

- Transaction recovery retained the address of the temporary `DB` wrapper
  constructed inside `DB.open`, although the wrapper is returned by value.
  ReleaseSafe poisoned the stale address and recovery later entered the HA
  mutation barrier through it. Recovery now owns a separately allocated
  callback context with an atomic binding to the stable caller wrapper; close
  clears the binding and joins recovery before tearing down dependent state.
- The standard HTTP listener shutdown path could close the listening socket
  before waking the accept loop, producing Debug `BADF` failures. Shutdown now
  wakes the loop before close.
- A stop published before listener ownership, or after `listen` returned,
  could try to wake through an unacquired/released `HttpRuntime` lease. Wakeup
  is now gated by the listener's atomic ownership publication; startup still
  observes the already-published stop on both sides of bind.
- The independent-domain checkpoint exposed a latent compile defect caused by
  a local durable-job-lane variable shadowing the `lane` method.
- httpx bypassed its supplied `std.Io` backend for ordinary POSIX reads and
  listener creation. That made virtual sockets incomplete and split timeout
  semantics between host and modeled I/O. Reads, ordinary listen, and logical
  timeouts now stay on the injected backend.
- httpx listener shutdown used a hidden global `std.Io.Threaded` loopback
  connection to wake `accept`. On `VoprIo` that mixed unrelated handle spaces
  and failed with `BADF`; the wake connection now uses the listener's own I/O.
- `HttpRuntime` unconditionally created three hidden Threaded executors and a
  native descriptor observer. It now supports caller-owned backend-neutral
  lanes, preserves bounded admission, and fails closed when native disconnect
  observation is requested from a backend that cannot provide it.
- Borrowed HTTP runtimes could not observe a peer reset because the native
  descriptor observer cannot inspect virtual socket handles; DataServer
  consequently disabled hard-disconnect cancellation under deterministic I/O.
  httpx now accepts a backend-neutral reset probe, and `VoprIo` distinguishes
  reset from ordered FIN even when unread pipelined bytes remain.
- Supplying server TLS certificate and key paths only printed a warning and
  continued serving plaintext. Binding now rejects incomplete TLS
  configuration and fails closed before reserving runtime or socket capacity;
  the supported production boundary remains explicit TLS termination.
- Production metadata/table-read setup assumed concrete Threaded executors.
  Transport-neutral request executors and generic `std.Io` fanout now allow the
  same DataServer composition to run deterministically.
- The standard testing allocator's stack-trace unwinder was unsafe across
  manually switched fiber stacks. VOPR production compositions retain leak
  detection with stack-trace capture disabled rather than weakening allocator
  checks globally.
- Virtual socket admission counted every historical handle rather than live
  handles. Closing a connection therefore never restored capacity and could
  prevent the listener's shutdown wake connection. `VoprIo` now accounts live
  descriptors and proves reuse after close.
- Virtual TCP half-close marked EOF immediately even when earlier payload bytes
  were still queued, allowing FIN to overtake data. FIN is now its own ordered,
  scheduler-visible packet transition.
- The threaded durable-job lane held its global reap mutex while awaiting an
  owner job. A job that closed a nested DB/background owner then tried to enter
  the same reap path, deadlocking both teardown operations. Drains now detach
  entries under the mutex and await them after releasing it; a focused nested-
  owner regression preserves this reentrant lifetime contract.
- The VOPR task kernel treated current-task identity as ordinary mutable state
  across a stackful context switch. ReleaseSafe could expose the main fiber's
  cleared identity after a task resumed, crashing every scheduler path that
  parked a fiber. Task fibers now publish their identity on entry/resume, the
  main fiber clears it after return, and the compiler boundary uses atomic,
  non-inline access. Debug and ReleaseSafe run the same 90-test engine gate.
- Counterfactual analysis bounded the choice-prefix and descendants per
  alternative but not the number of enabled alternatives. A high-cardinality
  choice could therefore monopolize a campaign worker. The reusable engine now
  enforces an explicit total experiment budget and spends it on choices nearest
  the failure first.

The bounded independent-domain model campaigns completed without an additional
semantic product-property failure or replay divergence. Reports should keep
that result distinct from the defects above.

## CLI

The VOPR command is a harness-only, test-mode artifact. It must not be installed
or represented as a production Antfly binary.

```sh
# Generate one history.
zig build vopr-run -- \
  --scenario metadata \
  --seed 0xa17f0001 \
  --transitions 500 \
  --trace-out /tmp/metadata.voprtrace

# Exact replay, reduction, and reviewed promotion.
zig build vopr-replay -- --trace /tmp/metadata.voprtrace
zig build vopr-reduce -- \
  --trace /tmp/metadata.voprtrace \
  --out /tmp/metadata-reduced.voprtrace
zig build vopr-promote -- \
  --trace /tmp/metadata-reduced.voprtrace \
  --name split-leader-restart-before-finalize

# Replay-proven format migration.
zig build vopr-migrate -- \
  --trace /tmp/metadata-reduced.voprtrace \
  --out /tmp/metadata-migrated.voprtrace

# Formal export and causal explanation.
zig build vopr-tla -- \
  --trace /tmp/metadata-reduced.voprtrace \
  --domain raft \
  --out /tmp/metadata-raft.ndjson
zig build vopr-explain -- \
  --trace /tmp/metadata-reduced.voprtrace \
  --failure 0 \
  --out /tmp/metadata-causal.json

# Replay-validated navigation at an arbitrary choice prefix.
zig build vopr-debug -- \
  --trace /tmp/metadata-reduced.voprtrace \
  --prefix 12 \
  --out /tmp/metadata-debug.json

# Run a repeatable debugger recipe or enter the same line-oriented frontend.
zig build vopr-debug -- \
  --trace /tmp/metadata-reduced.voprtrace \
  --commands /tmp/debug.commands
zig build vopr-debug -- \
  --trace /tmp/metadata-reduced.voprtrace \
  --interactive

# Bounded deterministic campaign.
zig build vopr-campaign -- \
  --scenario metadata \
  --histories 1000 \
  --transitions 500 \
  --workers 8 \
  --artifact-dir /tmp/antfly-vopr

# Host-independent checkpoint-search benchmark.
zig build vopr-benchmark
```

Registered CLI scenario names are:

```text
metadata              transaction          distributed-data
distributed-transaction                    data-plane
derived-workflow      backup-restore       clock-fault
wal                   persistent           index-manager
db-split              raft                 lmdb
lsm                   ha
```

## Test-Tier Policy

### `root-test`

- Fast root-module compile smoke coverage.
- No wall-clock sleeps or generated campaigns.
- Broad unit coverage belongs in focused unit buckets rather than a monolithic
  root-module test.

### `vopr-test`

- Fast deterministic virtual-time and modeled-I/O smoke coverage.
- Promoted VOPR fixtures, replay-equivalence checks, bounded domain scenarios,
  metadata virtual transport, Raft scheduling, production public HTTP, and
  `storage-vopr-test`.
- No legacy real-I/O storage workload pretending to be modeled I/O.
- `vopr-engine-test` runs only the reusable `lib/vopr` contract; `sim-test` is
  a temporary compatibility alias for this aggregate.

### `chaos-test`

- Longer but transition- or history-bounded deterministic campaigns.
- Independent labeled nodes for metadata, transaction, Raft, WAL, LMDB, LSM,
  persistent index, index manager, DB split, HA, and application domains.
- Every failure prints or stores an exact replay artifact.

### `chaos-soak-test`

- Larger history counts, broader fault budgets, and retained legacy chaos
  suites.
- Never part of the default fast gate.

### Integration and legacy storage tests

- Real HTTP, native threads, processes, sockets, and local object stores remain
  focused integration differentials.
- Deterministic storage workloads that still use real LMDB/WAL/files stay in
  `storage-workload-test`; longer ones stay in `storage-workload-soak`.

Legacy storage commands are:

- LMDB: `storage-lmdb-test`, `storage-lmdb-test -Dlmdb_backend=c`,
  `lmdb-replay-fixtures`, and `lmdb-workload-soak`.
- WAL: `wal-test`, `wal-workload-test`, `wal-replay-fixtures`, and
  `wal-workload-soak`.
- Persistent index: `persistent-test`, `persistent-workload-test`,
  `persistent-replay-fixtures`, and `persistent-workload-soak`.
- Index manager: `index-manager-test`, `index-manager-workload-test`, and
  `index-manager-replay-fixtures`.
- DB split: `db-split-workload-test` and `db-split-replay-fixtures`.
- Aggregate legacy workloads: `storage-workload-test` and
  `storage-workload-soak`.

The old `*-sim-test` and `storage-sim-soak` spellings are compatibility aliases,
not canonical suite names.

Reduced legacy artifacts are written under `/tmp` with an
`antfly-{lmdb,wal,persistent,index-manager,db-split}-replay-` prefix. Promote a
reviewed artifact with `zig build storage-fixture-promote -- <artifact>`;
`--latest`, an optional destination stem, and `--force` are supported. Fixture
directories retain their existing `_sim_fixtures` names as checked-in format
and path compatibility, just as trace ABI identifiers do. Fixed-map LMDB stays
out of randomized reopen matrices because persisted addresses are host-layout
sensitive. Crash-mode WAL, persistent, and index-manager fixtures require the
Zig backend's publish-phase hooks; the C backend remains the differential
oracle.

PR gates use deterministic transition and history counts. Nightly/manual
controllers may use a wall-clock allocation across independently replayable
histories, merge and deduplicate corpus artifacts, run reduction, and validate
eligible TLA+ traces.

## Corpus and Fixture Policy

- Existing regressions and replay fixtures seed exploration.
- A candidate affecting corpus state must exact-replay from a clean world.
- Generated non-failing entries remain CI artifacts unless review identifies
  durable coverage value.
- Failures are reduced before promotion and named for behavior, not raw seeds.
- Campaigns never modify tracked files.
- Obsolete fixtures change only through explicit replay-proven migration.
- Fixed regressions do not depend on corpus scheduling or search heuristics.

## Roadmap

The remaining opportunities are targeted integration scenarios and operational
tooling rather than missing foundational infrastructure. They are not a second
numbered phase plan and are not dependencies of the already implemented domain
suites.

### Implemented Extension Seams

The following seams are already implemented. They remain documented here so
new product work extends the same ownership and scheduling contracts instead of
introducing native-only alternatives.

#### Deeper DataServer and Raft Microsteps

The production DataServer, public listener, health request, metadata executor,
and lifecycle safepoints run on borrowed `VoprIo`. A production-neutral
DataServer lifecycle seam now exposes routing, remote forwarding, proposal
acceptance, persistence observation, apply confirmation, visibility
confirmation, and response-ack readiness with stable group/table/log
identities. Persistence is observed safely from the production apply watermark,
which Raft cannot publish before Ready storage completes, rather than adding a
suspension inside Raft storage ownership. Split and merge prepare,
copy, cutover, and rollback completions carry stable transition identities;
synchronous paths reach them after transition locks and writer leases are
released, durable split-copy jobs explicitly release their per-source lane
before suspending, and finalize/rollback paths expose writer-handoff and
transition-runtime cleanup boundaries only after those scoped leases close.
Single-table, routed multi-query, and global multi-query result assembly now
crosses a production-neutral API seam carrying stable operation/table identity
and response size after read/storage leases release. Independent Raft campaigns
continue to expose the lower-level persistence/apply ordering. These seams
preserve the single production writer and its lease ownership; extend them when
new routed operations add distinct durable or result-assembly boundaries.

#### HTTP Lifecycle and Backpressure

The common listener and executors run on `VoprIo`; tests cover normal,
single-byte partial-write, deadline-first cancellation, chunked request bodies,
chunked streaming responses, keep-alive reuse, pipelining, half-close ordering,
accept-versus-shutdown, bounded connection/request admission, minimum socket
capacity, descriptor reuse, and overload recovery. Hard disconnect now has a
backend-neutral probe at the httpx handler boundary: native runtimes retain the
shared descriptor observer, while `VoprIo` models reset separately from FIN and
cancels an active handler even with unread pipelined input. Direct server TLS
configuration fails closed before runtime or socket admission; the supported
production boundary is explicit TLS termination at a reverse proxy or load
balancer. Extend this suite when httpx gains a production server-side TLS
implementation or another transport backend.

#### Background Runtime Lifecycle

`DurableJobLane` and both production/VOPR implementations now share
pause/resume/drain/close/reopen semantics; tests include admission while paused,
committed-job drain, close, reopen, wrapper relocation, nested-owner teardown,
and exact cleanup. `VoprIo` now composes the Antfly-independent narrow executor
into the same scheduler as its `std.Io` fibers, sockets, and virtual time, so
the Antfly adapter does not require a second simulated runtime.
Transaction recovery, TTL, enrichment, text merge, sparse compaction,
resolution, promotion, LSM maintenance, quarantine retry, and repair now
retain the backend-neutral `std.Io` borrowed from `BackendRuntime`; their
production passes and lifecycle controls execute on `VoprIo`. DataServer
provisioned warmup, startup catch-up, replica-root refresh, local/runtime status
refresh, and auto-bulk finish work now use one shared durable owner instead of
private native threads. Their run/deinit callbacks clear active state on normal
completion, submission failure, cancellation, and DataServer teardown; file
probes inside these services use the runtime's borrowed `std.Io`. Derived-index
execution was already owned by the DB background runtime. The focused
DataServer campaign proves both scheduler execution and queued cancellation;
extend the owner only when another DataServer-native service is introduced.

#### Replication Backfill and Rebalancing

The production snapshot and streaming runners now expose a neutral lifecycle
hook at provider preparation, apply, durable checkpoint, cutover, polling, and
failure-persistence boundaries. Both runners use their borrowed `std.Io` for
persisted wall-clock timestamps, while PostgreSQL execution deadlines retain
their existing host-monotonic clock contract. The Antfly VOPR adapter derives
stable phase, table, source, offset, authority, and checkpoint identities
without importing VOPR into the metadata kernel.

The `replication-backfill-vopr-test` gate runs the production runners through
clean snapshot-to-stream cutover plus source crash, target crash, cancellation,
stale work ownership, target-topology change, source-schema change, and stream
crash after apply but before checkpoint. Every history restarts from the
durable production status record and exact-replays twenty times. Properties
prove that a checkpoint never outruns applied data, repeated work is logically
idempotent, stale ownership is rejected, snapshot and stream data are not lost,
and every interrupted attempt recovers.

#### Standalone and Serverless Supervision

The serverless maintenance manager no longer owns a private native run-loop
thread. Production bootstrap lends its `std.Io`; the manager owns a
`std.Io.Future`, cancels and joins it during shutdown, and publishes the first
maintenance failure instead of discarding it. `serverless_main` now routes that
failure through the shared production `RuntimeSupervisor`, alongside public and
health listener failures. This keeps listener, maintenance, and process
cancellation under one owner and makes the loop runnable on `VoprIo`.

The production supervisor also accepts a borrowed `std.Io` for startup-deadline
checks and supports a fully stopped in-process restart without weakening the
executor-independent hard process watchdog. The `supervision-vopr-test` gate
exact-replays clean startup, partial-startup rollback, shutdown during startup,
child-service failure, coordinated shutdown, virtual watchdog expiry, and
restart. It proves readiness is published only after all children start, first
failure cancels the process, rollback and shutdown release every child, and a
new generation can become ready before its own coordinated teardown.

#### User and Authentication Lifecycle

`UserManager` now borrows `std.Io` for password salts, API-key identity and
secret generation, realtime expiry checks, and its mutation/seed-capture
mutex. Production standalone, metadata, and data roles pass their process
runtime explicitly; the manager no longer creates hidden `Threaded` executors.
Production-neutral lifecycle events identify user persistence/publication,
password persistence/publication, API-key persistence/publication/revocation,
permission changes, and row-filter changes.

The `auth-lifecycle-vopr-test` gate runs the real manager and stores through
password rotation, API-key rotation, permission and row-filter changes,
revocation with an already materialized reader, durable reload, and an injected
crash between user persistence and policy publication. A separate fiber
schedule holds the real seed-capture lease, forces a password mutation to park
on the production `std.Io.Mutex`, and proves it cannot finish before capture
releases the lease. Every lifecycle history exact-replays with deterministic
randomness and time.

#### Serverless Object-Store Protocols

Real WAL, catalog, manifest, artifact, and progress-store operations now run
over the reusable `ScriptedFaultClient`, covering partial committed transfer,
delayed visibility, duplicate completion, timeout-after-commit, cancellation,
retry, publication, reconciliation, and client crash. Object-backed WAL append
now also accepts a durable caller-supplied operation identity: retry after an
ambiguous timeout returns the original LSN, conflicting reuse fails closed,
and the identity survives read and truncation. Stores that cannot uphold the
contract reject idempotent append instead of silently degrading it. The full
HA seed backup/restore workflow uses the same provider: a committed chunk with
a lost response is reconciled by a restarted publisher, repeated publication
selects the same generation, cancellation before restore staging is harmless,
and retry downloads and verifies the complete chunked artifact. Extend these
protocol campaigns when new production object-store consumers are introduced.

#### Admission and Resource Pressure

The production resource manager now runs under replayable contention schedules
that prove hard-limit denial, idempotent release, accounting, and capacity
recovery. A composed production request acquires foreground admission, a
multi-slice batch, and scratch memory; cancellation at each admitted edge
returns every reservation. Priority campaigns prove that background soft
pressure does not block unrelated foreground work and that bounded oversized
single-work admission provides exactly one minimum-progress grant while
rejecting a concurrent contender. `VoprIo` independently enforces task, CPU,
allocator, file, socket, storage, and queue limits, and the DataServer covers
socket admission. Continue composing these policies into deeper DataServer
request microsteps as those seams are added.

#### Existing Antithesis-Class Tooling and Search Quality

- Persisted pointer-free multiverse nodes, ranked counterfactual experiments,
  stable trial metadata, explicit total experiment budgets, cross-revision
  property history, scheduler-controlled completion order, and bounded
  systematic starvation are implemented in the self-contained repository.
- Long-running campaigns write causal reports, counterfactual experiment and
  multiverse graphs, and a quarantine manifest plus the original quarantined
  bytes. Diagnostics run only for a newly observed failure fingerprint.
- `vopr-debug` supports replay-proven navigation, branch creation, logical
  collectors, causal and bounded counterfactual windows, and child comparison
  from command files or an interactive terminal.
- The virtual filesystem supports one-shot read corruption, durable sector
  corruption, clearing persistent corruption, and torn synchronization that
  preserves only a selected durable prefix while retaining the prior tail.
- The generic datagram model is ready, but this repository currently has no
  production provider-specific datagram consumer to campaign. Add such a
  campaign with the first real consumer rather than inventing a test-only one.
- Add stable source/basic-block coverage as secondary guidance when the Zig
  instrumentation surface can remain outside the replay ABI.

### Highest-Value New VOPR Test Suites

| Priority | Area | What to exercise |
| --- | --- | --- |
| P0 implemented | Replication backfill and rebalancing | `replication-backfill-vopr-test` covers snapshot-to-streaming cutover, resumable checkpoints, duplicate work, cancellation, source and target crashes, topology changes, stale ownership, schema changes, and exact replay through the production runners. |
| P0 implemented | Standalone and serverless supervision | `supervision-vopr-test` covers partial-startup rollback, readiness publication, child-service failure, coordinated shutdown, virtual watchdog expiry, and restart through the production supervisor. The serverless manager now owns a borrowed-`std.Io` Future instead of a native run-loop thread. |
| P0 implemented | User and authentication lifecycle | `auth-lifecycle-vopr-test` covers password, API-key, permission, and row-filter changes; deterministic seed capture; revoke and rotate; durable reload; partial persistence rollback; and stale-reader behavior through the production manager. |
| P1 | Complete serverless workflow | Claim, build, compact or enrich, publish, and catalog visibility with duplicate workers, lease takeover, ambiguous object-store completion, retry, cancellation, and crash recovery. Object-store protocols are covered; their orchestration is not yet covered end to end. |
| P1 | DB and index request races | Elevate meaningful native-thread regressions such as cross-index admission, reader/writer fairness, delete/materialize, capture, shutdown, and cancellation into VOPR transitions through production-safe seams. Do not mechanically port test threads. |
| P2 | Provider boundaries | Add deterministic response adapters for inference providers and PostgreSQL/libpq covering timeout, partial response, cancellation, retry, malformed data, and admission ownership. Keep actual model execution, GPU kernels, and libpq internals in differential and integration tests. |
| P2 | Composed query lifecycle | Exercise vector, text, graph, and global-query execution under partial failure, cancellation, resource pressure, and result-assembly races. |

Start with replication backfill, followed by the standalone/serverless
supervisor. Those areas have the richest combinations of durable state,
ownership, concurrency, and recovery.

### Antithesis-Class Features Still Worth Porting

VOPR already has the important core: structured controlled choices, the major
Antithesis assertion kinds and assertion cataloging, deterministic scheduling
and I/O, logical checkpoints, exact replay, reduction, semantic coverage,
starvation, causal and counterfactual analysis, and multiverse navigation. The
documented [Antithesis assertion
model](https://antithesis.com/docs/product/writing_tests/assertions/) is
therefore substantially covered.

The remaining high-value features are:

1. **Retroactive logging and flight recording.** Keep a bounded structured
   event ring for every history, but retain or materialize verbose data only
   when a property fails, a history becomes novel, or a debugger requests it.
   Antithesis describes retroactive logging in its [release
   notes](https://antithesis.com/docs/release_notes/).
2. **Queryable event history.** Add fielded event details and predicates such
   as `preceded_by`, `followed_by`, actor/resource/fault filters, and time
   windows. VOPR has canonical events and causal windows, but not a general
   event-query layer comparable to [Antithesis event
   logs](https://antithesis.com/docs/reference/event_logs/).
3. **Self-contained run/results API.** Expose run metadata, property results,
   event search, corpus entries, artifacts, quarantine state, and budget usage
   through stable JSON plus an optional static local report. This should remain
   a repository-owned local and CI interface rather than a hosted dependency.
4. **Automatic debug recipes.** For each new failure fingerprint,
   automatically run reduction, causal-window extraction, bounded
   counterfactual experiments, selected log queries, and before/after
   collectors, then package one reviewable artifact.
5. **Richer property evidence.** Preserve structured details, the first
   failure, the rarest successful witness, and declared-but-never-encountered
   results in reports and searches. The engine has cataloging and string
   details; the reporting layer should make them first-class.
6. **Explicit overlapping-fault algebra.** Define how partition, delay, node
   pause, storage corruption, resource exhaustion, and clock faults compose,
   including precedence and exclusion groups. Antithesis schedules faults
   independently and permits [overlapping
   faults](https://antithesis.com/docs/environment/fault_injection/).
7. **Structured-choice audit.** Require every scenario-level decision to be
   an immediate typed choice rather than a random integer used as a seed or
   interpreted later. This gives search guidance the structured alternatives
   described by [Antithesis controlled
   randomness](https://antithesis.com/docs/reference/sdk/generate_randomness/).
8. **Default harness-health properties.** Automatically enable no-progress or
   deadlock, task and descriptor leaks, allocator or storage exhaustion,
   cleanup, replay divergence, and harness-error properties for every
   scenario.
9. **Search-quality regression benchmarks.** Maintain a corpus of
   intentionally injected bugs and track discovery probability and transition
   cost across random, guided, spliced, starvation, and checkpoint-assisted
   search.

### Conditional Work

Wait for a real product or toolchain requirement before adding:

- datagram campaigns, until Antfly has a production datagram consumer;
- server-side TLS fault campaigns, until httpx has a production server TLS
  implementation;
- source/basic-block guidance, until Zig exposes sufficiently stable
  instrumentation—semantic coverage remains replay truth, while compiler
  coverage only guides exploration;
- a graphical hosted debugger—the line-oriented debugger plus machine-readable
  and static reports is sufficient initially; or
- a container or hypervisor clone, which would duplicate the expensive part of
  Antithesis without improving Antfly's in-process `std.Io` strategy.

### Delivery Order

1. Add replication-backfill and supervisor VOPR suites.
2. Add authentication and complete-serverless-workflow suites.
3. Build retroactive logging, event queries, and automatic debug recipes.
4. Standardize local run/results artifacts and nightly corpus merging.
5. Add DB/index/query compositions and provider model adapters.
6. Continuously audit new production loops so they borrow `std.Io` and
   `VoprIo` instead of creating native-only runtime paths.

The defects already found—lifetime errors, listener shutdown races, hidden
Threaded I/O, virtual socket accounting, FIN ordering, teardown deadlocks, TLS
fail-open behavior, and ReleaseSafe fiber identity corruption—demonstrate that
extending VOPR across the remaining orchestration boundaries is likely to pay
off. See [Defects Found](#defects-found) for the complete inventory.

## Risks and Required Safeguards

- **Hidden host nondeterminism:** audit every scenario and fail closed on an
  unsupported `std.Io` capability.
- **Simulator drift:** run the same component on Threaded and VoprIo where
  possible and retain focused real-backend differential tests.
- **Coarse atomicity:** document atomic boundaries and add only production-safe
  suspension points.
- **Invalid histories:** use typed preconditions, fault budgets, minimum quorum
  policies, and explicit quiet suffixes.
- **Property defects:** test property aggregation independently and keep stable
  IDs separate from messages.
- **Artifact fragility:** pin scenario and runtime compatibility IDs and report
  the first enabled-set or observation mismatch.
- **Wrong-failure reduction:** require exact fingerprint equality and reject
  divergence or harness errors.
- **State explosion:** prioritize semantic novelty and rare property outcomes;
  keep search policy separate from replay meaning.
- **Resource cost:** use bounded histories, compact observations, ReleaseSafe
  campaigns, and full diagnostics only near novel or failing histories.

## Success Metrics

Track replay success, replay divergence, reduction ratio, unique fingerprints,
time to local reproduction, semantic states and transitions per CPU hour,
property reachability, meaningful overlapping-fault coverage, promoted
regressions, and modeled-versus-production differential agreement.

The practical success condition is simple: a developer receiving a chaos
failure can reproduce it with one command, inspect a short causal history, and
retain the reduced case as a permanent reviewed regression.

## Stable Decisions

1. Build an in-process deterministic virtual OS behind `std.Io`, not a
   general-purpose hypervisor.
2. Make a single selected transition the scheduler abstraction.
3. Record structured choices; seeds are discovery metadata only.
4. Treat replay from a clean world as truth and logical snapshots as an
   optimization.
5. Use non-fatal stable properties and deterministic recovery suffixes.
6. Preserve the same failure fingerprint during reduction.
7. Keep semantic coverage stable and compiler coverage secondary.
8. Keep `lib/vopr` Antfly independent and scenario policy under Antfly.
9. Use a harness-only CLI artifact; production code never imports the explorer.
10. Fail closed instead of falling back to host services.
11. Preserve existing HA, Raft, LSM, storage, integration, and formal tests as
    independent complementary gates.
12. Require human review before fixture promotion.

## Conclusion

Antfly already owns the hard self-contained foundation: explicit deterministic
choices, one-transition scheduling, virtual tasks, files, sockets, processes,
clocks and durability, guided exploration, exact replay, reduction, formal
export, counterfactual analysis, and independent production-shaped scenarios.

The next work should exercise replication backfill, runtime supervision,
authentication, and complete serverless orchestration through those existing
primitives, then improve flight recording, event queries, and automated
failure packaging. Compiler-guided coverage and provider-specific datagram
campaigns remain conditional on stable instrumentation and real consumers.
New product services should cross the existing `std.Io`, lifecycle-hook,
admission, and owner seams rather than creating native-only loops, multiplying
suite names, or weakening production ownership contracts.
