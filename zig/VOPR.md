# VOPR: Deterministic Autonomous Simulation for Antfly

Status (2026-08-22): Phases 0 through 5 and their stated exit conditions are
implemented. The Phase 4 scenario has generated/replayable plan choices and
split-to-merge composition. Finer-grained control inside its real
DataServer/HTTP integration remains an explicitly bounded production-seam
expansion assigned to Next Step 2 and the `SimIo` adapter, not a change to the
stable replay kernel; the simulator must not counterfeit that control by
opening a second writer or weakening production lease ownership.

Scope: Zig Antfly simulation, VOPR, modeled-storage, and chaos tests

## Implementation Conformance Snapshot

This table is the requirement-to-evidence index for the implemented design. It
is deliberately more conservative than a list of source files: “implemented”
means the behavior has an executable test or command path.

| Requirement | Implementation evidence | Verification path |
| --- | --- | --- |
| Stable structured choices and exact clean-world replay | `lib/vopr/src/choice.zig`, `runner.zig`, `replay.zig`, and canonical `vopr-trace-v1` parsing | `zig build vopr-test` |
| One-transition scheduling, typed outcomes, and blocked/budget termination | `scheduler.zig`, `scenario.zig`, and `outcome.zig` | `zig build vopr-test` |
| Narrow application runtime with scheduler-only control | `runtime.zig` and `sim_runtime.zig`; Antfly `DurableJobLane` adapter | `zig build vopr-runtime-test` |
| Explicit clocks, timers, and storage completions | `time.zig`, `sim_runtime.zig`, and selected completion APIs in `storage/sim_runtime.zig` | `zig build vopr-test storage-sim-runtime-test` |
| Independent lifecycle faults with budgets | `fault.zig` plus metadata link, node, pause, crash, route, queue, and transport faults | `zig build vopr-test lib-metadata-vopr-test` |
| Typed state-aware workload vocabulary | `command.zig`; scenario-specific adapters retain domain execution | `zig build vopr-test` |
| Semantic properties, observations, coverage, corpus, and guided search | `property.zig`, `observation.zig`, `coverage.zig`, `corpus.zig`, and `explorer.zig` | `zig build vopr-test vopr-benchmark` |
| Exact-replay-before-retention campaigns and deterministic reporting | Antfly `sim/cli.zig` with persistent corpus mutation/splicing and first/smallest failure reporting | `zig build sim-campaign -- --scenario metadata --histories 3 --transitions 2 --workers 2 --artifact-dir /tmp/antfly-vopr-smoke` |
| Same-fingerprint reduction, including context-backed scenarios | `reducer.zig`, metadata configuration shrinking, and distributed-data context dispatch | `zig build vopr-test sim-meta-test` |
| Replay-proven fixture promotion and explicit migration | `fixture.zig` plus `sim-promote` and `sim-migrate` | `zig build vopr-test sim-meta-test` |
| Formal sidecars and causal triage | transaction/Raft `sim-tla` dispatch and `causal.zig`/`sim-explain` | `zig build transaction-vopr-test sim-meta-test` |
| Metadata replay stability across build modes | A dedicated 100-consecutive-replay acceptance test | `zig build metadata-vopr-replay-stability-test` and the same command with `-Doptimize=ReleaseSafe` |
| Distributed acknowledged-data durability | Real public API, split, partition/restart, modeled device crash/recovery, merge, and reference model | `zig build lib-metadata-vopr-data-test` |
| Per-group Raft scheduling and durability interleavings | Real `RawNode` cluster with selected delivery/drop, deferred persistence/apply, restart, partition, proposal, and compaction transitions | `zig build raft-vopr-test` in Debug and ReleaseSafe |
| Storage differential and real-backend campaigns | WAL, C-versus-Zig LMDB, memory-versus-real-LSM, real PersistentIndex, split IndexManager, and full DB split worlds with generated maintenance, crash recovery, and typed storage faults | `zig build storage-vopr-test` in Debug and ReleaseSafe; `zig build sim-registry-test` for cross-domain registry parity |
| HA crash, replication, fencing, promotion, retention, and rejoin lifecycle | Real primary/standby logs and progress WALs, slot store, fence store, promotion and rejoin assessment under generated and scripted histories | `zig build ha-vopr-test ha-chaos-test` in Debug and ReleaseSafe |

The reusable package is Antfly-independent. The physical distributed-data
shell is intentionally an integration differential: its generated plan is
replayable, while its real HTTP/DataServer phase remains a single atomic
transition until production exposes safe borrowed-lease and request-executor
suspension points.

## Executive Summary

Antfly already has most of the hard prerequisites for deterministic simulation
testing:

- deterministic Raft behavior with explicit randomness
- a virtual Raft HTTP network with queued delivery and injected faults
- modeled time and storage durability
- seeded VOPR and chaos workloads
- reference-model and differential checks
- failure reduction and checked-in replay fixtures in several storage suites
- TLA+ trace validation for Raft and transaction behavior

The missing piece is a shared exploration architecture. Today these capabilities
are distributed across suite-specific harnesses. Most campaigns run a fixed
high-level scenario with a seeded stream of random actions, execute nodes in a
fixed round order, stop at the first assertion, and reproduce failures by
rerunning a seed or promoting a suite-specific fixture.

This document proposes an Antithesis-inspired in-process simulator that unifies
those pieces. The important ideas are:

1. Represent every nondeterministic choice explicitly.
2. Schedule exactly one enabled transition at a time.
3. Compose workloads from small, state-aware commands.
4. Inject independently overlapping network, node, time, and storage faults.
5. Evaluate named properties continuously without necessarily terminating the
   history.
6. Retain histories that discover new semantic states or property outcomes.
7. Record complete replay artifacts, minimize failures, and promote them to
   permanent regression fixtures.

The goal is not to reproduce a general-purpose binary hypervisor. VOPR should
remain an in-process Zig simulator, but its next runtime layer should be an
application-scoped deterministic virtual OS implemented as a real
`std.Io.VTable`. That makes production code using `std.Io` directly simulatable
without a hosted service. Exact replay of an explicit decision prefix remains
the source of truth for rewind and branching; arbitrary heap snapshots are not
required for deterministic time travel.

The first vertical slice should convert the three-node metadata VOPR campaign.
It should be able to explore workload, message, node, and fault orderings;
evaluate safety and recovery properties after every transition; replay a
failure exactly; reduce it while preserving the same failure identity; and
promote it to a checked-in fixture.

## Design Influences

This proposal adopts the following publicly documented Antithesis principles:

- Deterministic simulation virtualizes nondeterministic dependencies, feeds the
  system controlled entropy, explores many states, and evaluates invariants.
- A test template is more effective when it is decomposed into granular,
  compatible commands that the explorer can select, order, and run with varied
  concurrency.
- Structured random choices are more steerable than drawing an opaque integer
  and interpreting it privately inside a workload.
- Properties should describe high-level correctness and reachability rather
  than encode one expected imperative path.
- Faults should be visible in the same event history as workload and assertion
  outcomes, and independent faults may overlap.
- Coverage and assertion feedback should guide the search toward interesting
  histories.
- Deterministic histories enable replay, changed interventions after a prefix,
  and causal debugging.

References:

- [Deterministic simulation testing](https://antithesis.com/docs/resources/deterministic_simulation_testing/)
- [How Antithesis works](https://antithesis.com/docs/introduction/how_antithesis_works/)
- [Test templates](https://antithesis.com/docs/product/test_templates/)
- [Test composition principles](https://antithesis.com/docs/product/test_templates/test_best_practices/)
- [Generating structured randomness](https://antithesis.com/docs/reference/sdk/generate_randomness/)
- [Assertions and properties](https://antithesis.com/docs/product/writing_tests/assertions/)
- [Fault injection](https://antithesis.com/docs/environment/fault_injection/)
- [Coverage instrumentation](https://antithesis.com/docs/instrumentation/coverage_instrumentation/)
- [Deterministic debugging](https://antithesis.com/docs/product/debugging/)

These are design influences, not a claim that the current in-process engine
already provides hypervisor-level determinism or identical search algorithms.
The self-contained roadmap below deliberately ports the useful hard features
at Antfly's `std.Io` boundary: deterministic task scheduling, synchronization,
files, sockets, clocks, entropy, registered process lifecycles, fault injection,
counterfactual causality, and an interactive multiverse debugger.

## Goals

### Primary Goals

- Make failures reproducible from an explicit artifact rather than only a seed.
- Expose message, timer, node, storage, fault, and workload interleavings as
  choices controlled by one scheduler.
- Search long-lived Antfly states rather than only enumerate test cases.
- Express safety, reachability, coverage, and recovery guarantees as named
  properties.
- Reuse the same runner across metadata, Raft, storage, HA, and data-plane
  scenarios.
- Minimize failures automatically and promote reduced histories to regression
  fixtures.
- Preserve the existing fast deterministic tests and use them as seed corpus.
- Produce artifacts useful to humans, CI, TLA+ validation, and future tooling.

### Secondary Goals

- Allow deterministic parallel campaign workers.
- Support semantic coverage before compiler-level coverage is available.
- Keep the complete deterministic exploration and debugging workflow runnable
  locally and in CI without depending on a hosted testing service.
- Make production components that use `std.Io` run unchanged on either the
  ordinary threaded backend or VOPR's deterministic backend.
- Separate harness failures from product property violations.

## Non-Goals

- Building a general-purpose machine-code virtual machine capable of running
  arbitrary unmodified binaries or operating systems.
- Treating raw heap, native thread-stack, socket, or external-process snapshots
  as the replay source of truth. Rewind reconstructs a clean world and replays
  explicit choices; logical snapshots remain an optimization.
- Replacing unit, integration, differential, TLA+, or production-like end-to-end
  tests.
- Silently falling back to real threads, clocks, entropy, files, sockets, or
  processes when a deterministic `std.Io` capability is unsupported.
- Modeling operating-system behavior that Antfly and its registered in-process
  dependencies do not use.
- Proving liveness while a deliberately unrecoverable fault remains active.
- Making wall-clock campaign duration itself deterministic. Individual
  histories must be deterministic; a nightly controller may run as many
  histories as fit within a wall-clock budget.
- Automatically committing newly discovered fixtures.

## Existing Foundation

### Metadata VOPR

[`pkg/antfly/src/metadata/sim_harness.zig`](pkg/antfly/src/metadata/sim_harness.zig)
contains a three-node metadata campaign with a fixed seed, a transport-fault
action enum, node restart, partitions, table lifecycle, placement churn, split,
merge, store liveness, and drain workflows.

This is a strong first workload because it already crosses several important
state machines. Its current structure also exposes the main limitations:

- the smoke and expanded tests use fixed seeds in their test bodies
- direct `std.Random` calls combine generation and execution
- random transport actions are inserted between mostly fixed workload phases
- at most one campaign-tracked partition is active
- invalid or redundant action choices often degrade into a generic step
- failure output reports a seed and operation index but not the concrete action
  history
- the replay command reruns the fixed test rather than consuming an artifact
- `std.testing` assertions terminate the current history

### Virtual Raft Network and Cluster

[`pkg/antfly/src/raft/sim_harness.zig`](pkg/antfly/src/raft/sim_harness.zig)
provides:

- queued and immediate request delivery
- FIFO and seeded-random queue release
- node and directed-link partitions
- one-shot drop, duplicate, and delay faults
- probabilistic seeded drops
- a virtual tick used by transition retry logic
- restartable managed HTTP nodes
- deterministic progress loops

The current `stepAll` operation drains due network traffic, steps every node in
index order, drains after each node, and then advances time. That is convenient
for scenario tests, but it hides many possible interleavings. The new scheduler
must make the individual deliveries, node rounds, and time advances selectable.

### Modeled Storage and Time

[`pkg/antfly/src/storage/sim_runtime.zig`](pkg/antfly/src/storage/sim_runtime.zig)
provides a deterministic event queue, virtual clock, modeled completion
scheduler, and a storage device that distinguishes volatile from durable state.
It already models crash recovery, directory durability, dropped syncs, and
targeted write, sync, and delete failures.

WAL, LMDB, persistent index, index-manager, DB split, and LSM tests build useful
reference models on top of these pieces. Several suites serialize failing
schedules, reduce them, and promote stable fixtures.

### Reduction and Fixtures

[`pkg/antfly/src/lmdb/sim.zig`](pkg/antfly/src/lmdb/sim.zig) contains a generic
sequence reducer based on deleting chunks from a failing schedule.
[`pkg/antfly/src/storage/sim_fixture.zig`](pkg/antfly/src/storage/sim_fixture.zig)
defines the shared storage fixture envelope, and
[`pkg/antfly/src/storage_fixture_promote.zig`](pkg/antfly/src/storage_fixture_promote.zig)
promotes generated artifacts into checked-in fixtures.

These should be generalized rather than replaced. The important correction is
that reduction must preserve a specific failure fingerprint. The current
generic reducer treats any replay error as a reproducing failure, which can
reduce a product bug into an unrelated invalid setup or harness error.

### Raft Determinism and TLA+

The imported Raft implementation exposes its entropy through `RandomSource` or
an explicit seed. It is intended to be deterministic given messages, ticks,
storage state, and injected randomness.

Antfly also emits Raft and transaction NDJSON traces for replay through TLA+
specifications. A unified simulation artifact should be able to reference or
embed these domain traces so that a discovered history can be checked both by
runtime properties and the formal trace validators.

### Current Build Tiers

The build currently distinguishes:

- `sim-test`: mocked-time and deterministic simulation checks
- `chaos-test`: bounded generated metadata, LSM, and HA campaigns
- `chaos-soak-test`: broader legacy metadata and Raft chaos tests
- checked-in replay-fixture steps for several storage systems

The proposed engine fits these lanes without changing their intent. The build
integration section defines the new responsibilities for each tier.

## Gap Analysis

| Area | Current state | Required state |
| --- | --- | --- |
| Entropy | Suite-local PRNGs and fixed seeds | Structured, named, recorded choices |
| Scheduling | Whole cluster rounds | One enabled transition per decision |
| Workload | Large scripted scenarios | Small compatible state-aware commands |
| Faults | Suite-local and often one-shot | Unified lifecycle with independent overlap |
| Checks | Fail-fast assertions | Named, continuously aggregated properties |
| Feedback | Pass/fail and hand-selected seeds | Semantic state and property novelty |
| Replay | Seed or suite-specific fixture | Versioned explicit decision trace |
| Reduction | Per-suite, often any-error predicate | Same-fingerprint hierarchical reduction |
| Branching | Restart from a different seed | Replay a prefix and alter the next choice |
| Diagnostics | Debug prints | Ordered events, observations, faults, properties |
| CI corpus | Fixed cases | Fixed regressions plus retained interesting traces |

## Terminology

- **World**: All modeled state for one simulated Antfly deployment, including
  nodes, network, clocks, devices, clients, reference models, and active faults.
- **Transition**: One atomic action that changes or observes the world.
- **Choice**: A decision among a stable set of alternatives.
- **History**: A clean world plus the ordered choices and transitions executed
  from it.
- **Scenario**: A compatible collection of workload commands, faults,
  properties, and observers.
- **Property**: A named statement about safety, reachability, coverage, or
  recovery.
- **Observation**: A canonical summary of relevant world state.
- **Corpus**: Interesting replayable histories retained as future mutation
  inputs.
- **Quiet suffix**: A terminal phase in which faults are healed and the system
  is given bounded deterministic work to recover before liveness checks.
- **Failure fingerprint**: Stable identity for the property violation or crash
  that a reducer must preserve.

## Proposed Architecture

```text
                         Campaign Runner
                budgets, workers, corpus, reporting
                                  |
                                  v
                         Exploration Engine
              choose, mutate, score, retain, branch
                                  |
                                  v
    Workload Commands ---> Deterministic Scheduler <--- Fault Controller
                                  |
                         one transition
                                  |
                                  v
                       Simulated Antfly World
        nodes | raft | metadata | data | network | time | storage
                         |                  |
                         v                  v
                  Property Registry   State Observers
                         \                  /
                          \                /
                           Event/Trace Sink
                                  |
                    replay | reduce | promote | TLA
```

The reusable engine lives under `lib/vopr/` as the standalone `vopr` Zig
package. Antfly scenario adapters, fixtures, and determinism audits live under
`pkg/antfly/src/sim/` or beside their domains. The scheduler, trace, property,
coverage, corpus, and reduction contracts must import only `std` and other
VOPR modules; they must not depend on metadata, Raft integration, or storage
types.

## Determinism Contract

A history is reproducible only if the world is a pure function of:

- the simulator and scenario version
- the initial configuration and fixture inputs
- the ordered decision trace
- explicitly modeled external outcomes

The deterministic test mode must therefore enforce these rules:

1. Do not read wall or monotonic time except through an injected clock.
2. Do not read OS randomness except through an injected choice or random
   source.
3. Do not start uncontrolled background threads.
4. Do not depend on hash-map iteration order when enumerating choices or
   computing observations.
5. Do not depend on pointer values, allocation addresses, temporary directory
   names, or process IDs in semantic state.
6. Give every queued operation a stable logical identity and sequence number.
7. Canonically sort candidates and observations before hashing or rendering.
8. Record all modeled errors and external outcomes that can influence later
   behavior.
9. Treat replay divergence as a harness error, not a product property failure.

Each scenario should have a determinism test that runs the same trace multiple
times and compares:

- ordered event kinds and stable fields
- observation hashes after every transition
- property outcomes
- final reference-model digest
- failure fingerprint, if any

Diagnostics such as allocation counts or real execution durations may be
reported, but they are not part of replay equivalence.

## Structured Choice Engine

Direct use of a PRNG inside a scenario makes generation, execution, and replay
inseparable. The simulator should instead use a `ChoiceSource` abstraction.

Conceptual API:

```zig
pub const ChoiceId = u64;

pub const Alternative = struct {
    id: u64,
    label: []const u8,
};

pub const ChoiceSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        choose: *const fn (
            ptr: *anyopaque,
            site: ChoiceId,
            occurrence: u64,
            alternatives: []const Alternative,
        ) anyerror!usize,
    };
};
```

Choice sites need explicit stable names, hashed into `ChoiceId`. Source line
numbers should not be used because harmless refactors would invalidate every
artifact. Alternative identity also matters: traces should record a logical
transition ID rather than only an index into a transient array.

Required implementations:

- **Seeded**: uniformly or weightedly selects choices from an explicit seed and
  records them.
- **Replay**: consumes recorded choices and fails with `ReplayDiverged` if the
  selected alternative is no longer enabled.
- **Mutating**: replays a prefix, changes selected decisions, and generates a
  suffix.
- **Scripted**: useful for named scenario tests and hand-authored fixtures.
- **Enumerating**: systematically explores small bounded dynamic choice trees
  in depth-first order without snapshotting world state.

Choice namespaces should distinguish scheduler, workload, fault, parameters,
and implementation entropy. This makes traces readable and permits targeted
mutation without requiring independent hidden PRNG streams.

## Transition Scheduler

The scheduler is the determinism kernel. Every operation that can meaningfully
interleave must become an enabled transition.

Initial transition kinds:

```zig
pub const Transition = union(enum) {
    workload_start: WorkloadStart,
    workload_poll: WorkloadPoll,
    node_round: NodeId,
    network_deliver: MessageId,
    network_drop: MessageId,
    network_duplicate: MessageId,
    timer_fire: TimerId,
    storage_complete: StorageOperationId,
    fault_start: FaultSpec,
    fault_stop: FaultId,
    node_crash: NodeId,
    node_restart: NodeId,
    time_advance: u64,
    observe,
};
```

Not every scenario must implement every transition. The world enumerates its
currently enabled transitions, sorts them by stable identity, and asks the
choice source to select one.

The existing `stepAll` behavior remains useful for compatibility and fast
convergence. It should be expressible as a deterministic policy over atomic
transitions rather than remain the indivisible scheduler primitive.

### Transition Identity

A transition ID should be derived from stable logical fields:

- transition kind
- actor or node ID
- resource ID, such as message, timer, or device operation
- creation sequence within that resource

Payload bytes should not normally be part of the ID, but an optional payload
digest should be recorded for divergence diagnostics.

### Enabled-Set Rules

- Candidate enumeration must have no side effects.
- Candidates must be returned in canonical order.
- A selected transition must still be enabled immediately before execution.
- An unavailable workload command is not silently converted to a node step.
- If the world has no enabled transition, the runner classifies it as clean
  quiescence, expected blocked state, liveness failure, or harness deadlock
  according to the scenario phase.

### Logical Concurrency

The implemented atomic scheduler models parallel clients and background work as
actor state machines with start, poll, and completion transitions. That remains
the fast path for focused scenarios.

The next runtime layer adds stackful simulated tasks behind `std.Io`. Each
`async`, `concurrent`, group, select, futex wait, queue wait, sleep, file
operation, and socket operation becomes either an immediate deterministic
result or a parked task plus a scheduler-visible completion. The scheduler
selects one runnable task or completion at a time. Code between `std.Io`
boundaries is atomic until optional instrumentation adds stable safepoints.

Native stress and thread-sanitizer tests remain complementary. They validate
the ordinary threaded backend, while the simulated backend systematically
controls logical scheduling and synchronization choices.

### Runtime Capability Boundary

VOPR already uses the successful shape of `std.Io`—small copyable handles
backed by an explicit context pointer and vtable—for its narrow runtime. The
self-contained target is now a real deterministic `std.Io.VTable`. In Zig 0.16
that interface provides exactly the interception surface needed for the hard
features: futures and groups, cancellation, futexes, batched operations,
directories, files and mappings, registered processes, clocks, entropy, and
sockets.

The existing generic package retains these narrow capabilities as its stable
internal scheduler API:

- `Executor`: submit, cancel, and idle state for stable logical tasks
- `Clock`: logical nanosecond time
- `Timers`: schedule and cancel stable logical timers
- `Runtime`: the application-facing aggregation of those capabilities
- `SchedulerPort`: scheduler-only enumeration and execution of ready work

Task ownership transfers only after a successful submit or schedule. The
accepting backend must destroy each task exactly once after execution,
cancellation, or teardown. Entropy is deliberately absent from `Runtime`;
simulation entropy remains an explicit `ChoiceSource` decision.

A production adapter may dispatch `Executor` tasks through `std.Io.Threaded`.
The implemented `SimRuntime` queues atomic tasks and timers and exposes them
through `SchedulerPort` as stable transitions. The planned `SimIo` composes
that scheduler with a fiber backend so ordinary synchronous-looking `std.Io`
code can park and resume without driving the scheduler recursively. Application
code receives `std.Io` or the narrow `Runtime`; only the runner receives
`SchedulerPort`.

`SimIo` is implemented in capability stages, but every stage presents a
complete vtable. Unimplemented entries return their documented unsupported
error or a stable harness error; they never delegate to `std.Io.Threaded`.
Scenario construction declares the required capability set and fails before
the first choice if the backend cannot satisfy it.

Capability preflight is mandatory because some `std.Io.VTable` entries are
infallible at the type level. If an undeclared infallible operation is somehow
reached, the handler latches a deterministic capability violation, returns only
the inert value needed to unwind the current transition, and forces a harness
error before another product transition can run. That return value is never a
modeled product result. Tests must exercise every unsupported entry so a Zig
toolchain upgrade cannot introduce an accidental host fallback or an
uninitialized function pointer.

The capability stages are:

1. **Tasks and synchronization**: futures, groups, cancellation, queues,
   select, futexes, mutex/event/condition behavior, sleeps, and all clock and
   entropy calls.
2. **Modeled files**: directory and file handles backed by `ModeledDevice`,
   positional and streaming I/O, sync, rename, truncate, locks, mappings, file
   descriptor limits, partial operations, corruption, and crash durability.
3. **Virtual sockets**: listen/connect/accept, DNS, reads/writes, shutdown,
   backpressure, half-close, asymmetric loss, delay, reorder, duplicate,
   partition, clog, and jam behavior.
4. **Registered processes and resources**: spawn/replace/wait/kill for
   explicitly registered in-process programs, process-local namespaces,
   pause/crash/restart, memory/file/socket quotas, and deterministic CPU work
   budgets. Registered programs must keep mutable state in an instance or an
   explicit virtual-process context; mutable process-global state is rejected
   unless the model proves it is immutable or safely shared. Arbitrary external
   executables fail closed.
5. **Instrumented preemption and debugging**: optional stable safepoints inside
   CPU-bound code, basic-block feedback, counterfactual causal search, artifact
   capture before/at/after a failure, and interactive rewind and branching.

The public Zig `std.Io` layout is toolchain-version-specific, so it is an
adapter ABI rather than the VOPR trace ABI. Artifacts record the Zig version,
SimIo capability digest, virtual-OS model version, and instrumentation-map
digest. Exact replay fails closed when any behavior-relevant value is
incompatible.

Rewind does not copy fiber stacks. It reconstructs a clean world and replays to
the selected choice prefix; scenario-owned logical snapshots may accelerate
that replay only when their existing configuration and prefix digests match.
This preserves deterministic debugging without pointer-bearing heap images.

## Simulated World Interface

The generic runner should use a type-erased or comptime scenario interface with
the following conceptual responsibilities:

```zig
pub const Scenario = struct {
    init: fn (alloc: Allocator, config: Config) !World,
    deinit: fn (*World) void,
    enumerate: fn (*World, *TransitionList) !void,
    execute: fn (*World, Transition, *EventSink) !TransitionOutcome,
    observe: fn (*World, *ObservationBuilder) !void,
    evaluate: fn (*World, *PropertySink) !void,
    beginQuietSuffix: fn (*World) !void,
    isRecovered: fn (*World) !bool,
};
```

Concrete Zig APIs can use comptime dispatch where it meaningfully reduces
allocation and type erasure. The artifact model must remain runtime-typed and
versioned so replay does not depend on the compiler's internal enum layout.

`TransitionOutcome` should distinguish:

- applied successfully
- rejected as a valid product response
- expected injected error
- property violation
- target reached
- process crash or panic
- harness error
- replay divergence

The explorer may retain all but ordinary successful/rejected outcomes, but it
must never report a harness error as an Antfly correctness bug.

## Fault Model

Faults are stateful entities with explicit start and stop events. One-shot
faults are represented as active faults that expire after matching one
operation. Independent faults may overlap unless the scenario declares the
combination invalid.

### Network Faults

Build first on `VirtualHttpNetwork`:

- directed and bidirectional partitions
- node isolation
- drop a selected message
- duplicate a selected message
- delay a selected message until a logical time or release event
- reorder selected queued messages
- bounded random or burst loss
- bandwidth or queue-capacity limits
- connection reset or route unavailability

Selecting a specific queued message is more powerful and reproducible than
"drop next." Compatibility helpers may continue translating `drop_next` into a
fault that matches the next eligible message.

### Node Faults

- graceful stop
- crash without graceful shutdown
- restart from durable state
- pause/hang selected services
- restart with preserved network partition
- restart during bootstrap, snapshot, split, merge, compaction, or drain

Crashing a node and crashing its modeled device should be related but distinct
choices. Some histories need process loss with intact durable media; others need
storage faults before or during the crash.

### Time Faults

- advance global virtual time
- per-node wall-clock offset
- forward and backward wall-clock jumps where production semantics allow them
- paused node clock
- drift between monotonic scheduling and reported realtime

Raft logical ticks, retry clocks, lease clocks, and user-visible timestamps
must remain distinguishable. Clock jumps should not mutate monotonic time unless
that is explicitly the modeled fault.

### Storage Faults

Generalize the existing modeled-device controls:

- fail selected read, write, sync, rename, truncate, or delete operations
- drop a selected sync
- partial or torn writes at explicit durability boundaries
- delayed or reordered completions
- device-full and admission failures
- crash after a selected persistence phase
- corrupt a selected volatile or durable artifact where recovery promises
  require detection

The storage fault layer must describe permitted uncertainty. For example, a
failed sync may legally expose either the old or new durable image depending on
the modeled phase, while an acknowledged full-durability write must survive.
Properties and reference models need to encode this outcome set rather than
assume a single result.

### Resource and Custom Faults

Later scenarios may add:

- memory or allocation failure at selected modeled allocation sites
- queue saturation and backpressure
- worker starvation
- capacity and health changes
- forced compaction, snapshot, repair, rebalance, or garbage collection
- administrative configuration changes

These use the same transition and trace machinery, even when they represent
hostile workload actions rather than environmental failures.

### Fault Budgets

Unconstrained overlapping faults mostly discover unsurprising total outages.
Each scenario should define tunable budgets:

- maximum simultaneous node failures
- maximum partitioned links
- maximum outstanding delayed messages
- maximum storage faults per durability epoch
- minimum healthy quorum, except in scenarios intentionally testing quorum loss
- quiet-suffix recovery policy

Budgets guide exploration; they must not be hidden repair logic that changes a
replayed history.

## Workload Composition

A scenario is composed from small commands that are mutually compatible. Each
command declares whether it is enabled in the current abstract model and
creates one or more actor transitions.

Conceptual API:

```zig
pub const Command = struct {
    id: CommandId,
    name: []const u8,
    class: enum { setup, driver, observer, recovery },
    enabled: *const fn (*const Model) bool,
    start: *const fn (*World, Parameters) anyerror!ActorId,
    parameterSchema: *const fn (*ParameterBuilder) anyerror!void,
};
```

The explorer separately chooses:

1. a command class
2. an enabled command
3. structured parameters
4. an actor scheduling interleaving

This prevents one opaque random number from controlling an entire complex
operation.

### Metadata Scenario Commands

The first scenario should include:

- create and drop table
- add range
- request, retry, roll back, or remove split
- request, retry, roll back, or remove merge
- update placement candidates and roles
- publish node or store state
- request node drain/shutdown
- transfer or campaign metadata leadership
- start and poll public table operations when public traffic is enabled

### Data Scenario Commands

Add after the metadata runner is stable:

- put, update, delete, get, scan, and search documents
- batched and transactional operations
- schema and index changes
- snapshot and restore
- split/merge traffic crossing range boundaries
- repair, compaction, and enrichment work

### Storage Scenario Commands

- append and append batch
- read and verify from a cursor
- truncate or checkpoint
- flush and compact
- reopen
- crash and recover
- mutate multiple namespaces
- schedule concurrent logical requests

Existing WAL, LMDB, persistent, and LSM action unions should be adapted into
this command model incrementally. They remain valid focused scenarios even when
the full distributed world is not involved.

## Reference Models and Checkers

The engine provides scheduling, replay, and property plumbing. Correctness still
depends on strong scenario-specific models.

### Online Safety Checks

Run after every transition when affordable:

- topology and range invariants
- Raft role/term/configuration invariants
- monotonic durable and applied watermarks
- split/merge ownership rules
- placement constraints
- state-machine internal consistency
- absence of invalid durable artifacts

Expensive checks may run at deterministic intervals selected through the trace,
but every property should declare its evaluation cadence.

### Client History Model

Every logical client operation receives a stable operation ID and records:

- invocation transition
- target and request
- response transition
- result or error class
- relevant logical timestamps or versions

After a quiet suffix, a simple key/value model can verify final visible state.
For concurrent operations, an offline checker may additionally test
linearizability, serializability, or a domain-specific consistency model.

Availability must be specified carefully: a request may legitimately fail or
time out during a partition. Safety properties constrain accepted responses and
post-recovery state; they should not require every request to succeed under
active faults.

### Differential Oracles

Continue using trusted alternate implementations where available:

- memory backend versus LSM backend
- C LMDB versus Zig LMDB
- Go etcd/raft traces versus Zig Raft
- modeled storage versus physical storage replay

A differential mismatch is a named property violation with a stable
fingerprint, not merely an arbitrary test error.

### TLA+ Trace Validation

The simulator's event sink should be able to forward domain events to the
existing Raft and transaction trace writers. A promoted history may include a
reference to the emitted NDJSON trace and the exact command for validation.

TLA+ remains complementary:

- simulator properties check concrete implementation state and user behavior
- trace refinement checks that concrete steps correspond to model actions
- standalone model checking explores abstract states beyond concrete campaign
  budgets

## Property System

Properties should be registered before a history runs and evaluated through a
sink that records outcomes without immediately terminating execution.

### Property Kinds

- **Always**: must be evaluated at least once and must never evaluate false.
- **Always or unreachable**: must never evaluate false, but not reaching the
  evaluation site is allowed.
- **Reachable**: a named state or code path must occur at least once.
- **Unreachable**: a named state or code path must never occur.
- **Sometimes**: a condition must be true at least once; false evaluations do
  not fail it before the history ends.
- **Eventually after quiescence**: after faults are healed, a condition must
  become true within a deterministic transition budget.

Conceptual record:

```zig
pub const PropertyEvaluation = struct {
    property_id: PropertyId,
    kind: PropertyKind,
    condition: bool,
    transition_index: u64,
    details: []const Detail,
};
```

Property IDs and human-readable messages are stable API. Source location is
useful diagnostic metadata but is not identity.

### Failure Behavior

On an ordinary property violation, the runner should:

1. record the first violating transition and observation
2. continue for a bounded diagnostic suffix when safe
3. allow other properties to report outcomes
4. retain the history with a failure fingerprint

Memory safety failures, panics, process termination, allocator corruption, or a
world that cannot execute further naturally end the history. The parent
campaign runner still records and reduces the artifact.

### Initial Distributed Properties

Safety:

- at most one Raft leader per term for a group
- committed and applied indices do not regress
- caught-up replicas agree on committed metadata projections
- active table ranges have no gaps or overlaps
- every active range has one authoritative group identity
- a split destination is not published before its safety prerequisites
- merge donor retirement does not lose acknowledged data
- stale or unfenced leaders cannot serve lease-protected reads
- acknowledged full-durability writes survive modeled crashes
- recovered WAL, index, and manifest state belongs to the permitted durability
  outcome set
- replica placement obeys count, role, health-domain, and membership rules

Recovery:

- a healthy quorum eventually elects a leader
- healed transport eventually drains eligible messages
- restart catches a replica up or classifies it for snapshot/reseed
- accepted split and merge transitions eventually complete, retry, or reach a
  documented terminal rollback state
- requested placement repairs converge after required stores become healthy
- after a quiet suffix, successful public writes are visible through the final
  topology

Coverage properties:

- a node crash overlaps each split and merge phase
- a follower catches up via snapshot
- a leader changes while public traffic is active
- storage failure occurs at each publish boundary
- compaction and crash overlap
- quorum loss and later recovery occur
- delayed and duplicated messages are actually delivered

Coverage properties distinguish an unproductive green campaign from one that
exercised the intended hostile states.

## Observations and Semantic Coverage

Compiler basic-block coverage is useful but should not block the first engine.
Distributed correctness depends heavily on long-lived combinations of state
that basic-block coverage does not describe well.

Each scenario supplies canonical observation features. The runner records both
individual features and hashes of feature combinations and transitions.

### Initial Feature Families

Raft:

- group ID, role, term bucket, known leader
- commit-minus-applied gap bucket
- follower match gap bucket
- voter/learner/joint-configuration shape
- snapshot or replay phase
- leadership transition kind

Network:

- active directed partition graph
- queued message count bucket
- queued message-type multiset
- oldest message age bucket
- active loss, duplication, delay, and capacity faults

Metadata:

- table lifecycle phase
- range count and topology digest
- split and merge phase pairs
- desired versus observed replica count
- placement role and health-domain shape
- reconcile lease owner
- drain and maintenance state

Storage:

- volatile/durable namespace digest
- dirty file and directory count buckets
- WAL segment and replay-debt buckets
- manifest generation
- LSM level/run shape
- compaction, flush, checkpoint, and recovery phase

Workload:

- command and actor phase
- inflight operation count
- reference-model size bucket
- response and error-class frequencies

Properties:

- first reach of each evaluation site
- first true and false outcome
- property-outcome transitions

### Novelty

The MVP can score a history using:

- new individual features
- new pairs of previous and current state hashes
- rare feature frequency
- new property reachability or outcome
- new error class
- proximity to a configured target state

Only canonical semantic fields participate. Logs, pointers, real durations, and
temporary paths must not affect novelty.

Compiler coverage may later be added as another feedback channel. Because Zig
can use LLVM-based code generation on supported targets, sanitizer-style
coverage may be possible, but toolchain support and runtime hooks must be
validated independently. Semantic coverage is the committed MVP.

## Exploration Engine

The first explorer should be deliberately simple and inspectable.

### Corpus Loop

1. Start with named scenario traces and existing regression fixtures.
2. Select a corpus entry according to rarity and prior yield.
3. Replay a prefix.
4. Apply one or more trace mutations.
5. Generate a suffix until the transition budget, terminal phase, or failure.
6. Run a quiet suffix when configured.
7. Compute semantic novelty and property outcomes.
8. Retain the history if it is novel, reaches a target, or fails.
9. Queue failures for same-fingerprint reduction.

### Mutation Operators

- replace one choice with another enabled alternative
- delete a range of workload or fault decisions
- insert a workload command
- insert, remove, or change a fault lifecycle
- change a structured parameter toward boundary values
- shorten or lengthen a delay or fault duration
- select a different queued message or node
- splice compatible prefixes and suffixes at matching state signatures
- move a fault earlier or later around a target phase

Mutations operate on logical choice records, not raw bytes. A later fuzzing
front end may mutate a compact binary representation, but the canonical trace
remains structured.

### Search Policy

An AFL-style queue is sufficient initially:

- give energy to rare observations and recently productive traces
- reduce energy for repeated no-novelty histories
- reserve a percentage for uniform exploration so every enabled command and
  fault remains reachable
- support explicit target features for focused campaigns

The algorithm must be pluggable. Search improvements should not change the
world, trace, or property contracts.

### Parallel Workers

Workers run independent deterministic histories. Shared corpus arrival order
may affect which histories are attempted, but never whether an individual
artifact replays.

Each artifact records its complete choices, so replay does not depend on:

- worker count
- worker index
- scheduling between workers
- corpus insertion order
- wall-clock campaign cutoff

Corpus merging deduplicates by canonical trace digest, final observation,
novelty set, and failure fingerprint.

## Replay Artifact

Introduce a new versioned artifact separate from the storage-specific fixture
format. NDJSON is recommended for the initial format because it streams, diffs,
and composes naturally with current trace tooling.

### Required Records

```text
{"type":"header","format":"vopr-trace-v1","system":"antfly",...}
{"type":"config",...}
{"type":"choice",...}
{"type":"transition","index":17,...}
{"type":"fault","phase":"start",...}
{"type":"event",...}
{"type":"observation",...}
{"type":"property",...}
{"type":"failure",...}
{"type":"summary",...}
```

The exact JSON schema should be checked into the repository before the first
artifact is promoted.

### Header Fields

- format version
- application or product identity in the `system` field
- scenario name and version
- simulator ABI version
- source revision, when available
- target and optimization mode
- initial seed, if generation used one
- transition and resource budgets
- stable hashes of initial fixture inputs
- feature flags and backend choices

Replay compatibility is determined by scenario and simulator versions, not by
source revision alone. Compatible code changes may intentionally preserve the
same replay ABI.

### Choice Record

- choice site ID and name
- occurrence number
- stable enabled-alternative IDs
- selected alternative ID
- structured parameter values

Recording the enabled set makes divergence diagnosis much easier. Large sets
may be represented by a digest plus the selected alternative in normal traces,
with a verbose mode for investigation.

### Transition and Event Records

Transitions record the selected atomic operation. Events record consequences,
including messages enqueued, state changes, client responses, injected errors,
and domain trace events.

The event stream is diagnostic and supports properties and TLA+ export. The
decision stream is the authoritative replay input.

### Observation Records

Store canonical feature IDs and hashes. Full verbose state snapshots are
optional and should be emitted at configurable deterministic checkpoints or
around failures.

### Failure Fingerprint

Use stable components:

- failure class
- property ID or normalized panic/error identity
- scenario name and version
- selected domain identity, such as group, table, or storage subsystem
- optional canonical observation digest

Do not require an exact native stack address. A symbolized stack digest may be
supplemental because optimization and harmless code movement can change it.

### Artifact Locations

Generated artifacts:

```text
/tmp/antfly-sim/<campaign>/<history-id>.simtrace
```

Promoted fixtures:

```text
pkg/antfly/src/sim/fixtures/<scenario>/<name>.simtrace
```

Large raw logs and verbose snapshots remain CI artifacts rather than checked-in
fixtures. Promoted traces should be reduced and human-reviewable.

## Replay, Branching, and Checkpoints

### Exact Replay

Replay builds a clean world and consumes recorded choices. At each step it
verifies that:

- the choice site matches
- the selected alternative is enabled
- the transition identity and payload digest match
- configured observation checkpoints match
- the expected property or failure identity is reproduced

A mismatch produces `ReplayDiverged` with the first differing transition,
enabled set, and observation. Replay divergence is valuable compatibility
information, not a successful reduction.

### Branching

The MVP branches by replaying a prefix and choosing a different enabled
alternative. This is slower than an in-memory snapshot but dramatically simpler
and robust across allocator ownership and pointer-rich structures.

Prefix caches may retain initialized fixtures or serialized domain snapshots,
provided their restore path is tested for replay equivalence.

### Explicit Snapshots

Later, worlds may implement:

```zig
snapshot(allocator) !WorldSnapshot
restore(allocator, snapshot) !World
```

Snapshots must serialize logical state and reconstruct ownership. Copying raw
heap pages is out of scope. Likely snapshot candidates are:

- modeled devices and durable images
- network queue and active fault state
- Raft storage and configuration
- metadata projection and client reference model
- virtual clocks and pending timers

Snapshot support is an optimization. Replay from the start remains the source
of truth.

## Reduction

Reduction should occur in layers while preserving the exact failure
fingerprint.

### Layer 1: Remove Decision Ranges

Reuse generalized delta debugging to remove chunks of workload, scheduler, and
fault decisions.

### Layer 2: Simplify Faults

- remove unrelated faults
- shorten fault duration
- reduce a node partition to a link partition
- reduce burst loss to one selected drop
- remove duplicated or delayed messages

### Layer 3: Simplify Workload

- remove operations and actors
- shrink batch sizes and document sets
- simplify keys, ranges, schemas, and values
- reduce replica and table counts where the same bug remains

### Layer 4: Simplify Scheduling

- replace explicit scheduling decisions with the default deterministic policy
- remove redundant node rounds and observations
- shorten idle and recovery suffixes

### Layer 5: Simplify Configuration

- reduce node count while maintaining prerequisites
- disable unrelated feature flags
- replace physical or complex backends with modeled equivalents only if the same
  property fingerprint remains meaningful

Every candidate must start from a clean world. A candidate that diverges,
produces a harness error, or violates a different property does not reproduce
the target.

Reducer output should include:

- original and reduced transition counts
- target fingerprint
- replay count and total reduction attempts
- normalized reduced artifact path
- exact replay command

## Quiet Suffix and Liveness

Safety can be evaluated during active faults. Liveness usually cannot.

Each scenario defines a deterministic quiet-suffix protocol:

1. stop generating new driver operations
2. heal configured recoverable faults
3. restart nodes that the scenario promises to recover automatically
4. restore default network delivery policy
5. advance enabled transitions under a fair deterministic policy
6. stop when the recovery predicate holds, the system becomes idle, or the
   transition budget expires
7. evaluate eventual and final-state properties

The artifact records every healing and recovery transition. The runner must not
silently repair product state.

Fairness for the quiet suffix can initially be round-robin over stable
transition classes and actors. Exploration scheduling need not be fair; recovery
validation must document its fairness assumptions.

## First Vertical Slice: Metadata VOPR

The metadata cluster is the right first integration point because it already
combines Raft, virtual HTTP transport, durable node restart, placement,
split/merge workflows, and progress assertions.

### World

- three managed metadata/Raft nodes
- current virtual HTTP network
- existing memory Raft storage initially
- virtual retry clock
- metadata workflow adapter
- abstract table/range/placement model
- property registry and semantic observer

Physical HTTP listeners and threaded public servers stay out of the first
world. They remain integration tests until their I/O and scheduling boundaries
can be modeled without weakening production parity.

### Atomic Transitions

- run one node service round
- deliver, drop, duplicate, or delay one queued request
- advance one virtual tick or to the next timer
- start or stop a link/node partition
- crash or restart one node
- start or poll one metadata command actor
- evaluate an observation/property checkpoint

### Initial Commands

- bootstrap cluster
- publish nodes and stores
- create/drop a table
- add a range
- report split/merge candidates
- request split/merge
- retry/rollback/remove a transition
- change placement candidates
- change node/store roles and liveness
- request drain/shutdown
- campaign or transfer leadership

### Initial Properties

- metadata projections agree once replicas share the same applied index
- range topology has no gaps or overlaps
- transition pairs have valid mirrored phases
- no impossible split/merge enrichment ownership
- placement never creates duplicate local replica identity
- committed node/store changes survive restart
- no stale reconcile lease performs ownership-sensitive work
- after healing, a healthy quorum elects a leader
- accepted lifecycle and transition commands converge or reach an allowed
  terminal state
- drain intent survives churn and restart

### Acceptance Criteria

1. Existing metadata VOPR smoke and expanded histories can be expressed through
   the new runner without losing coverage.
2. The same trace replays with identical observation and property streams at
   least 100 consecutive times in debug and release-safe builds.
3. A test-only injected property violation produces a trace and is reduced to a
   smaller same-fingerprint history.
4. Changing one recorded scheduling or fault choice creates a valid branch.
5. The explorer retains histories for new metadata/Raft semantic states.
6. A reduced trace can be promoted and run as part of `sim-test`.
7. No production code path changes behavior when simulation dependencies are
   not supplied.

## Expansion Plan

### Raft Runtime

Status: implemented. The metadata adapter exposes selected Raft HTTP messages
and node rounds, and `raft/vopr.zig` supplies the reusable per-group scenario.

Expose atomic host rounds, per-group scheduler choices, message deliveries,
snapshot operations, persistence/apply completions, and backpressure. Reuse the
existing differential trace corpus and compare selected histories with etcd.

### Modeled Storage

Status: implemented. WAL, LMDB, LSM, persistent index, index manager, and DB
split all use the common runner and versioned artifact. Each is registered for
CLI run, replay, reduce, migrate, promotion routing, persistent-corpus campaign
mutation/splicing, and exact-replay-before-retention. Their older fixture
parsers and focused fault matrices remain required complementary gates for
specialized publication, split, and full-text behavior.

Adapt WAL, LMDB, persistent index, DB split, index manager, and LSM campaigns to
the common trace/property interface. Preserve their focused reference models
and fixture parsers during migration.

The common runner should first wrap existing action unions; it should not force
one enormous distributed scenario before focused replay parity is proven.

### Data Plane

Status: first distributed durability vertical slice implemented; broader
document, enrichment, index, compaction, and repair command decomposition is
planned.

Add public document traffic, table routing, range transitions, snapshots,
enrichment, index lifecycle, compaction, and repair. The client history and
final-state oracle become essential here.

### HA

Status: implemented. `storage/ha/vopr.zig` drives the real primary, standby,
replication log, progress WAL, slot store, fence store, promotion, retention,
and former-primary rejoin primitives. Generated histories exact-replay from
clean worlds; scripted histories guarantee the receive/apply/report crash
windows, base-backup restart pin, lag-expiry/reseed path, and partitioned
unfenced-rejection/fenced-promotion/rejoin path. The focused HA chaos matrix
remains a required complementary gate and seed source rather than being
replaced by the common runner.

Convert crash-phase, standby, fencing, reseed, timeline, retention, and
promotion tests into commands and properties. Node and storage faults then use
the same lifecycle as metadata scenarios.

### Production-Like Processes

Native process/container chaos remains a distinct differential layer, but the
self-contained deterministic layer should simulate registered Antfly programs
through `SimIo`. A registered process receives its own virtual process ID,
environment, current directory, file/socket handle tables, resource budget,
clock view, and task tree. Spawn, wait, kill, pause, crash, and restart are
explicit transitions. The backend does not claim to execute arbitrary external
binaries: an unregistered executable is rejected deterministically.

This gives the repository the application-relevant process, thread, network,
clock, storage, and resource-fault behavior normally obtained from a hosted
deterministic environment. Real binaries and containers remain useful as a
non-deterministic compatibility check, not as a prerequisite for VOPR search,
replay, reduction, causality, or multiverse debugging.

## Module Layout

Use `lib/vopr`, not a generic `lib/sim`, for the reusable engine. `sim` is too
broad to communicate the replay ABI, scheduler, property, corpus, and reduction
contracts, and it would invite unrelated test helpers to acquire a false
stability promise. Antfly-independent deterministic exploration belongs in
`lib/vopr`; Antfly scenario adapters, determinism audits, fixtures, and CLI
policy belong in `pkg/antfly/src/sim`. A future library should move below VOPR
only when it is independently useful without VOPR campaign semantics—for
example, a general modeled block device—not merely because two Antfly tests use
it.

```text
lib/vopr/
  build.zig
  build.zig.zon
  src/
    root.zig
    choice.zig
    command.zig
    transition.zig
    runner.zig
    event.zig
    outcome.zig
    trace.zig
    replay.zig
    property.zig
    observation.zig
    runtime.zig
    sim_runtime.zig
    sim_io.zig
    sim_io_task.zig
    sim_io_file.zig
    sim_io_net.zig
    sim_io_process.zig
    sim_io_instrumentation.zig
    time.zig
    fault.zig
    vopr-trace-v1.schema.json
    scheduler.zig
    coverage.zig
    corpus.zig
    explorer.zig
    benchmark.zig
    splice.zig
    snapshot.zig
    causal.zig
    reducer.zig
    fixture.zig

pkg/antfly/src/sim/
  DETERMINISM_AUDIT.md
  cli.zig
  cli_runner.zig
  fixtures/
    metadata/
    distributed-data/
    transaction/
    raft/              # promoted-fixture namespace
    wal/               # promoted-fixture namespace
    persistent/        # promoted-fixture namespace
    index-manager/     # promoted-fixture namespace
    db-split/          # promoted-fixture namespace
    lmdb/              # promoted-fixture namespace
    lsm/               # promoted-fixture namespace
    ha/                # promoted-fixture namespace
  scenarios/           # optional registry extraction; adapters may stay by domain
    metadata.zig
    raft.zig
    wal.zig
    lmdb.zig
    lsm.zig
    ha.zig

pkg/antfly/src/metadata/
  sim_harness.zig # metadata and distributed-data scenario adapters
pkg/antfly/src/storage/
  wal_vopr.zig
  persistent_vopr.zig
  index_manager_vopr.zig
  db_split_vopr.zig
  lmdb_vopr.zig
  lsm_vopr.zig
  ha/vopr.zig
  transaction_vopr.zig
  vopr_durable_job_lane.zig # Antfly Job -> generic VOPR Executor adapter
```

Domain-specific adapters may live beside the domain when that produces a
cleaner dependency direction. The stable rule is:

- generic engine types do not import metadata, Raft integration, or storage
- scenarios import the generic engine and their domains
- production code only depends on narrow runtime, clock, entropy, transport,
  and storage interfaces, never on the explorer or `SchedulerPort`

Raft wraps the real `RawNode` cluster with independent message, persistence,
apply, restart, partition, proposal, and compaction choices. LMDB wraps the
existing C-versus-Zig action union and crash publication oracle. LSM runs a
live real backend against the memory oracle with generated KV operations,
explicit compaction/maintenance, crash recovery, and modeled storage faults.
HA wraps the real durable replication lifecycle with independent replication,
apply, progress, crash, partition, retention, backup, fencing, promotion, and
rejoin choices. Persistent index, index manager, and DB split now wrap their
existing real modeled worlds and reference summaries one action at a time,
including reopen, split/handoff, and final modeled crash recovery. Focused
suites keep their existing runners and oracles as complementary specialization
gates; common artifact and CLI parity no longer depends on those legacy fixture
formats.

The CLI is a standalone command surface, not additional behavior in Antfly's
ordinary unit-test runner. Its scenario implementations intentionally use
test-only resources such as `std.testing.tmpDir`, allocator leak checking, and
the test I/O instance. The build therefore emits a runnable test-mode artifact
with a dedicated `cli_runner.zig` that dispatches only the VOPR command
entrypoint. This preserves the harness-only contract while supporting budgets,
artifacts, replay modes, and property aggregation through normal `zig build
sim-* -- ...` commands. It must not be installed or represented as a production
Antfly binary.

## CLI and Build Integration

Implemented commands:

```sh
# Run one deterministic generated history.
zig build sim-run -- \
  --scenario metadata \
  --seed 0xa17f0001 \
  --transitions 500 \
  --trace-out /tmp/metadata.simtrace

# Replay an exact artifact.
zig build sim-replay -- \
  --trace /tmp/metadata.simtrace

# Reduce while preserving the target failure fingerprint.
zig build sim-reduce -- \
  --trace /tmp/metadata.simtrace \
  --out /tmp/metadata-reduced.simtrace

# Promote a reviewed reduced artifact.
zig build sim-promote -- \
  --trace /tmp/metadata-reduced.simtrace \
  --name split-leader-restart-before-finalize

# Rewrite only after source replay, migration, target replay, and semantic
# outcome equivalence all succeed.
zig build sim-migrate -- \
  --trace /tmp/metadata-reduced.simtrace \
  --out /tmp/metadata-migrated.simtrace

# Exact-replay an eligible artifact and export its formal Raft event stream.
zig build sim-tla -- \
  --trace /tmp/metadata-reduced.simtrace \
  --domain raft \
  --out /tmp/metadata-raft.ndjson

# Record and formally export a transaction history through the same CLI.
zig build sim-run -- \
  --scenario transaction \
  --seed 0xa17f7a4a \
  --trace-out /tmp/transaction.simtrace
zig build sim-tla -- \
  --trace /tmp/transaction.simtrace \
  --domain transaction \
  --out /tmp/transaction.ndjson

# Exact-replay a failure and render a deterministic semantic causal slice.
zig build sim-explain -- \
  --trace /tmp/metadata-reduced.simtrace \
  --failure 0 \
  --out /tmp/metadata-causal.json

# Run a bounded local campaign.
zig build sim-campaign -- \
  --scenario metadata \
  --histories 1000 \
  --transitions 500 \
  --workers 8 \
  --artifact-dir /tmp/antfly-sim

# The same exact-replay-before-retention campaign registry accepts every
# context-free domain, for example HA or persistent index.
zig build sim-campaign -- \
  --scenario persistent \
  --histories 100 \
  --workers 4 \
  --artifact-dir /tmp/antfly-sim/persistent

# Compare baseline and checkpoint-resumed search in deterministic work units.
zig build vopr-benchmark
```

Names may be refined during implementation, but run, replay, reduce, promote,
and campaign must remain distinct operations.

### Existing Test Tiers

`sim-test`:

- all promoted simulator fixtures
- deterministic replay-equivalence tests
- a small fixed generated corpus
- existing focused simulation tests that have not migrated

`chaos-test`:

- bounded transition- or history-count campaigns
- deterministic base corpus and search configuration
- metadata, Raft, WAL, LMDB, LSM, persistent index, index manager, DB split,
  and HA scenarios
- labeled progress and failure artifact paths

The storage domains enter this gate through the shared `storage-vopr-test`
dependency, while metadata, Raft, HA, and the longer focused LSM/HA matrices
retain their labeled campaign nodes.

`chaos-soak-test`:

- broader scenario set
- more workers and larger history budgets
- stress fault budgets
- existing legacy chaos tests until coverage is superseded

The cross-mode replay acceptance lane is intentionally opt-in because it
reconstructs a real three-node metadata world 100 times:

```sh
zig build metadata-vopr-replay-stability-test
zig build metadata-vopr-replay-stability-test -Doptimize=ReleaseSafe
```

Both commands must pass without divergence or leaks before changing the
metadata replay ABI.

Nightly/manual:

- wall-clock-controlled exploration across deterministic histories
- corpus merge and deduplication
- artifact upload
- automatic reduction attempt
- TLA+ validation for eligible retained histories

PR gates should use transition and history counts rather than a wall-clock
cutoff so work and failure attribution remain stable. Nightly orchestration may
use a wall-clock budget, but every completed history remains independently
replayable.

## Corpus and Fixture Policy

- Existing named tests and replay fixtures seed the initial corpus.
- Generated non-failing corpus entries are CI artifacts unless they provide
  durable, reviewed coverage value.
- Failures are reduced before promotion.
- Promotion is explicit and reviewable; campaigns never modify tracked files.
- A promoted fixture records its original seed and discovery metadata as
  comments or header fields, but replays from explicit choices.
- Fixtures are named for the behavior or bug, not a raw seed.
- Fixed regressions never depend on corpus scheduling or search heuristics.
- Obsolete fixtures may be rewritten only through an explicit format migration
  tool that proves equivalent replay outcomes.

## Observability and Triage

A campaign summary should report:

- histories and transitions executed
- clean, failed, divergent, and harness-error counts
- unique semantic states and transitions
- property catalog with pass/fail/not-reached status
- first and smallest artifact for each failure fingerprint
- fault and workload reachability
- corpus inputs that yielded new coverage
- replay and reduction commands

A per-failure timeline should interleave:

- workload invocation/completion
- scheduler choice
- network enqueue/delivery/drop/duplicate
- fault start/stop
- node lifecycle
- storage operation and durability phase
- important state observation changes
- property outcomes
- domain trace events

The initial `sim-explain` implementation emits a smaller deterministic causal
slice rather than copying the whole timeline. It starts at the selected failure
boundary, retains active or nearby faults and nearby client/injected-error
outcomes, then walks backward through shared stable actor and resource IDs.
This is explicitly a semantic debugging heuristic, not proof of causality. The
report records each retained transition's role and the stable failure identity,
and the command exact-replays the input before producing JSON.

Verbose logs remain available but should not be required to understand the
causal sequence.

## Testing the Simulator

The simulator is correctness infrastructure and requires its own tests.

### Choice and Replay

- stable alternative ordering
- seed determinism
- exact recorded replay
- useful divergence at the first mismatched choice
- mutation changes only intended decisions before suffix generation

### Scheduler

- one transition per step
- no side effects during enumeration
- stable transition IDs
- fair quiet-suffix policy
- correct deadlock and quiescence classification

### Faults

- independent overlap
- correct matching and one-shot expiry
- healing does not drop unrelated faults
- crash preserves only permitted durable state
- fault events align with affected operations

### Properties

- catalog semantics for every property kind
- non-fatal aggregation
- bounded diagnostic suffix
- final not-reached evaluation
- same-fingerprint classification

### Artifacts

- parser/render round trip
- format version rejection
- canonical rendering
- deterministic digest
- replay across debug and release-safe builds where supported

### Reduction

- removes irrelevant choices
- refuses different failures
- refuses replay divergence and harness errors
- terminates under a bounded attempt budget
- preserves required setup dependencies

### Meta-Testing

Create small intentionally faulty state machines whose bugs require:

- a particular message ordering
- a fault overlapping a workload phase
- a crash between write and sync
- a clock jump before a retry
- two logical clients interleaving

The explorer should reliably discover, replay, and reduce each bug under a
small fixed budget.

## Implementation Plan

### Phase 0: Determinism Audit and Contracts

- define scenario, choice, transition, property, observation, and trace APIs
- catalog direct time, randomness, thread, filesystem, and network use in the
  first metadata world
- add stable IDs and canonical ordering helpers
- specify `vopr-trace-v1`
- add replay-equivalence meta-tests

Exit condition: a hand-authored toy scenario records and exactly replays.

#### Phase 0 implementation (2026-08-22)

Phase 0 is implemented by the standalone `lib/vopr` package and re-exported as
`antfly.sim`. The implementation deliberately stops at the generic contract
boundary; adapting the metadata campaign is the Phase 1 exit condition.

- `scenario.zig` defines the compile-time scenario contract. `runner.zig`
  owns world lifecycle, transition budgets, canonical scheduling, event and
  observation capture, non-short-circuiting property evaluation, and failure
  aggregation.
- `choice.zig` provides seeded, scripted, and exact-replay sources. Replay
  checks the stable choice site, occurrence, full canonical enabled set, and
  selected alternative. Exhausted or trailing choices are harness errors.
- `id.zig` fixes the replay ABI to namespaced FNV-1a 64-bit IDs. Transition and
  observation builders reject duplicate IDs and impose canonical numeric
  ordering before choices or hashes are recorded.
- `property.zig` implements all six property kinds described above, including
  deterministic post-quiescence deadlines. Violations are accumulated and
  become stable `failure` records instead of aborting the first check.
- `event.zig` separates diagnostic consequences from authoritative choices.
  `trace.zig` owns, validates, parses, and canonically writes all v1 record
  kinds: header, config, choice, transition, fault, event, observation,
  property, failure, and summary.
- `vopr-trace-v1.schema.json` is the exact per-line JSON Schema. The header's
  required `system` field identifies an application such as `antfly`. The
  canonical NDJSON stream order is header, config, choice records, transition
  records, fault records, event records, observation records, property
  records, failure records, and summary. The parser rejects out-of-order,
  incompatible, internally inconsistent, or non-canonical artifacts.
- `replay.zig` rebuilds a clean world from the decision stream and requires the
  complete canonical artifact—including events, observations, property
  evaluations, failures, and summary—to match byte for byte.
- `DETERMINISM_AUDIT.md` catalogs time, entropy, scheduling, filesystem, and
  network boundaries in the first metadata world and names the Phase 1 adapter
  requirement for every partially controlled input.
- `toy_scenario.zig` is the hand-authored exit-condition scenario. The focused
  tests run 100 exact replays, test replay divergence and ABI rejection,
  exercise stable failure/fault records, parse the checked-in schema, and run
  the entire record/parse/replay path through allocator-failure injection.

The focused top-level gates are `zig build vopr-test` and the compatibility
alias `zig build sim-contract-test`; the latter remains a dependency of
`zig build sim-test`. The package also runs independently with
`cd lib/vopr && zig build test`. Debug and ReleaseSafe are both required before
Phase 0 is considered green.

### Phase 0.5: Standalone Package and Runtime Boundary

- extract all Antfly-independent engine code into `lib/vopr`
- make `vopr` independently buildable and testable with no Antfly imports
- retain Antfly audits, scenarios, fixtures, and campaign commands under
  `pkg/antfly/src/sim`
- establish the narrow `Executor`, `Clock`, `Timers`, `Runtime`, and
  scheduler-only `SchedulerPort` contracts
- rename the unpromoted generic replay ABI to `vopr-trace-v1` and add the
  required application `system` header field
- explicitly keep a complete `std.Io` backend out of the Phase 0.5 exit
  condition until blocking and capability semantics are proven

Exit condition: the standalone package passes the complete Phase 0 suite in
Debug and ReleaseSafe, Antfly consumes it only through the `vopr` module, and
the top-level simulation aggregate remains green.

#### Phase 0.5 implementation (2026-08-22)

Phase 0.5 is implemented. `lib/vopr/build.zig.zon` declares a dependency-free
package, `runtime.zig` defines the capability and ownership contracts above,
and the top-level build exposes `vopr-test` while preserving
`sim-contract-test`. No VOPR source imports an Antfly package. The metadata
determinism audit remains in `pkg/antfly/src/sim/`, ready to guide the Phase 1
adapter.

The completed package also owns the Antfly-independent command registry,
transition outcomes, fault lifecycle controller and budgets, node clock model,
fixture migration proof, campaign accounting, and context-aware reducer. These
belong in `lib/vopr`, not `lib/sim`: each has a replay/campaign semantic
contract. A general modeled component should move below VOPR only when it is
useful without exploration semantics, as the modeled storage device already
is.

Phase 0.5 intentionally stopped at the narrow runtime because a simulated
`std.Io.VTable` without fibers, a capability matrix, and fail-closed behavior
would have made a false determinism claim. Those prerequisites are now an
explicit post-Phase-5 roadmap rather than an unresolved architecture question.
The narrow runtime remains the scheduler kernel beneath `SimIo`, so this next
layer extends rather than replaces the completed phase.

### Phase 1: Traceable Metadata Campaign

- extract current metadata VOPR actions from test-local PRNG execution
- add the standalone CLI and build steps
- record explicit choices, transitions, faults, and observations
- reproduce the current smoke and expanded campaigns through scripted or seeded
  choice sources
- retain existing test aliases as wrappers

Exit condition: current VOPR behavior is preserved, but any generated history
can be replayed from a trace.

#### Phase 1 implementation (2026-08-22)

Phase 1 is implemented for the existing smoke and expanded metadata campaigns.
The transport campaign no longer draws opaque actions and parameters directly
from a test-local PRNG. It enumerates canonical concrete alternatives for node
rounds, selected queued messages, time advancement, exact link and node
partitions, one-shot transport faults, healing, and node restart, then consumes
them through `vopr.choice.Source`.

Every history records the full enabled set and selection plus atomic transition
payload identity, fault lifecycle, consequence event, semantic observation,
and the named leader-uniqueness and index-monotonicity properties. Scenario
parameters such as metadata IDs, workload tier, and operation budget are part
of the v1 artifact, so replay requires no out-of-band test configuration.
Both existing build aliases remain wrappers; each records a history, rebuilds
a fresh three-node cluster, consumes `choice.Replay`, and byte-compares the
complete canonical artifact. The standalone `sim-run` and `sim-replay` build
steps exercise the same non-test entry points and scratch-directory lifecycle.

### Phase 2: Atomic Scheduler and Properties

- implement `SimRuntime` task and timer queues behind the Phase 0.5 contracts
- expose ready task completion and timer firing as stable transitions
- split virtual network draining into selected message transitions
- expose individual node rounds and virtual-time transitions
- convert metadata progress and invariant checks to named properties
- implement quiet suffix and recovery properties
- allow overlapping lifecycle faults under budgets

Exit condition: scheduler mutation reaches different valid message/node/fault
interleavings while replay remains exact.

#### Phase 2 implementation progress (2026-08-22)

The generic deterministic execution kernel is implemented and independently
tested. `sim_runtime.zig` owns submitted tasks and timers, exposes each ready
task and due timer as a stable atomic transition, and exposes the next timer
deadline as an explicit virtual-time transition. Cancellation, execution,
failed submission, and teardown have exact-once ownership tests. Applications
receive only `Runtime`; only the scheduler receives `SchedulerPort`.

`scheduler.zig` canonicalizes enabled transitions and re-enumerates immediately
before execution. It rejects a scenario whose enumeration has side effects or
whose selected transition changes during selection. `choice.Mutating` now
provides clean-world prefix replay, a structured branch at one decision, and a
seeded suffix.

`outcome.zig` gives every transition an explicit applied, rejected,
injected-error, property, target, process-crash/panic, harness-error, or
replay-divergence result. `fault.zig` owns reusable lifecycle identity,
start/end/one-shot semantics, and node/link/delay/storage/quorum/quiet budgets.
`time.zig` separates global monotonic time, per-node monotonic domains,
realtime jumps and drift, pause/resume state, and Raft logical ticks. These are
independent engine contracts even where the first metadata adapter uses only a
subset of their fault and clock vocabulary.

The Antfly metadata adapter now exposes individual node rounds, virtual-time
advancement, and selected queued-message delivery, drop, duplication, delay,
and release. The virtual network provides a side-effect-free canonical message
snapshot and exact message operations with stable logical sequence IDs and
payload digests. Bounded burst loss, one-shot connection reset, persistent
route unavailability, and queue-capacity saturation are explicit injectable
faults with independent semantic counters. Compatibility `drop_next`,
`duplicate_next`, and `delay_next` operations remain, but generated histories
prefer selected-message operations whenever a message already exists.
A bounded quiet suffix disables hostile fault choices and evaluates the named
`metadata.eventually_recovers_after_quiescence` property. Metadata partitions
now have explicit start and stop lifecycle transitions: up to two independent
directed-link partitions may overlap, a whole-node partition remains exclusive
to preserve the scenario's healthy-quorum budget, and each active fault plus
the aggregate heal action is independently selectable. Node pause and resume
are lifecycle transitions: pausing suppresses only that node's scheduler round
while preserving its memory, durable state, and network fault state. Active
link, node, pause, route, queue, and total fault counts are semantic
observations. Version 6 additionally distinguishes graceful stop from a crash
interval, destroys the crashed process state, restarts from its durable
dependencies, and records matching start/end fault IDs. The remaining Phase 2
follow-on was to move a shared production background-service seam onto
`SimRuntime` instead of leaving all such work in manual domain runtimes.

That production seam is now implemented for the shared durable-job surface.
`storage/vopr_durable_job_lane.zig` adapts Antfly's existing
`DurableJobLane`—used by commit-durable, maintenance, and cleanup work—to the
generic VOPR `Executor`. Submission creates a scheduler-visible atomic task;
successful execution and owner cancellation preserve exact-once payload
destruction, owner close rejects later work, and job failures are classified in
stable adapter statistics instead of aborting the scheduler. The focused test
runs the production LSM background executor through `SimRuntime`, proving this
is a shared production seam rather than a test-only callback facade.

Synchronous `drainOwner` cannot drive the deterministic scheduler from inside
itself, so the adapter requires callers to reach quiescence before drain and
fails loudly on misuse; `closeOwner` cancels queued transitions. This makes the
lifetime distinction explicit instead of introducing hidden recursive
scheduling. The `zig build vopr-runtime-test` gate is included in `sim-test`.
Phase 2's exit condition is met. Long-lived services that require the full
`std.Io` concurrency, filesystem, and network surface are assigned to the
post-Phase-5 `SimIo` roadmap. The narrow runtime contract remains stable and
does not widen; the full vtable is an adapter layered above it.

### Phase 3: Coverage, Corpus, and Reduction

- add semantic observers and novelty scoring
- add corpus selection and structured mutation
- generalize same-fingerprint reduction
- add failure artifact and promotion workflow
- add campaign reporting

Exit condition: injected meta-test bugs are autonomously discovered, replayed,
reduced, and promoted.

#### Phase 3 implementation progress (2026-08-22)

The standalone engine now has the initial inspectable search loop described in
this design. `coverage.zig` derives semantic coverage only from stable
transition, event, observation-feature, property, and failure identities and
tracks fixed-point rarity. `corpus.zig` owns canonical traces, deduplicates by
trace digest, final observation, and failure fingerprint, and assigns bounded
rarity/productivity energy. `explorer.zig` runs deterministic history-count
campaigns, reserves configurable uniform exploration, mutates structured
choices, and retains novel or failing histories.

`reducer.zig` verifies the original exact replay, first delta-debugs contiguous
logical decision ranges, then explores individual structured choice
replacements. Range deletion exact-replays the retained prefix, rebases later
stable transition IDs only while they remain enabled at the same choice site,
and switches to a deterministic generated suffix at the first incompatibility.
Every candidate starts from a clean world; only a strictly simpler artifact
with the same failure fingerprint is accepted and exact-replayed. The report
separately counts deletion attempts and accepted deletions. Its meta-test
injects a named property bug and reduces a three-transition history to one
transition through this path.

The Antfly CLI now exposes distinct `sim-campaign`, `sim-reduce`, and
`sim-promote` workflows in addition to run and replay. Campaign workers execute
independent deterministic histories in parallel, merge stable semantic
coverage under a mutex, and retain novel or failing artifacts under the chosen
artifact directory. Metadata reduction first shrinks the generated workload
budget and then performs same-fingerprint structured scheduling/fault
substitution, exact-replaying every accepted result. Promotion refuses
non-failing or non-replaying traces, validates fixture names, and avoids
overwriting by default.

Every generated or mutated campaign artifact is now exact-replayed before it
can affect coverage, corpus retention, or failure reporting. The deterministic
summary separates clean histories, property failures, replay divergences, and
harness errors; reports transitions, unique semantic states and transition
IDs, reached faults/workloads, productive corpus parents, and splice outcomes;
lists every observed property as pass/fail/not-reached; and prints the first
and smallest artifact plus replay/reduction commands for each stable failure
fingerprint. Retained children credit their selected mutation/splice parents,
so corpus energy reflects measured productivity rather than only novelty.

The generic reducer accepts an optional scenario context and uses it for the
original proof, every candidate, and every exact-replay check. The CLI keeps
metadata's domain shrinker and dispatches generic transaction and
context-backed distributed-data reductions by the artifact scenario. Context
values that affect replay remain serialized in the artifact; the pointer is
only the clean-world construction mechanism.

The `sim-meta-test` gate closes the Phase 3 exit condition. It enables a
test-only named metadata oracle bug that is reachable only while two directed
link partitions overlap. A target-state choice source discovers that state
from the actual enabled transition sets; the test then exact-replays the
failure, reduces it under the same fingerprint, promotes the canonical trace
through the same guarded writer as the CLI, refuses an accidental overwrite,
parses the promoted file, and exact-replays it again. The injection changes no
production behavior and is serialized as an explicit scenario parameter.

Campaign artifact directories are now persistent mutation queues rather than
write-only output folders. At startup the CLI recursively loads `.simtrace`
entries in lexical order, validates and exact-replays each artifact under the
current scenario ABI, reconstructs semantic coverage, and inserts canonical
deduplicated entries into the energy-weighted corpus. Workers select those
entries as structured mutation parents; newly retained histories are merged
back into the in-memory queue and written atomically as independent files. The
meta-test also reopens its promoted fixture as a fresh persistent corpus,
selects it for mutation, and exact-replays the result. Scenario-specific
configuration shrinkers remain additive hooks after the generic decision-range
and choice simplifiers, as demonstrated by metadata operation-count reduction.

The standalone choice engine also implements weighted seeded selection while
preserving the legacy all-uniform PRNG stream, plus a bounded depth-first
`Enumerating` source for dynamic choice trees. A finite meta-suite exhaustively
discovers all five required interaction shapes—message ordering, fault/workload
overlap, crash between write and sync, clock jump before retry, and two-client
interleaving—then exact-replays and same-fingerprint reduces each failure.

### Phase 4: Storage and Data Integration

- wrap existing modeled-storage campaigns in the common runner
- unify storage fault transitions and durability outcome properties
- add public data operations and a client reference model
- combine split/merge, traffic, restart, and storage faults
- export eligible Raft and transaction traces to TLA+

Exit condition: one distributed scenario checks acknowledged user data across a
range transition, node restart, network fault, and modeled storage recovery.

#### Phase 4 implementation progress (2026-08-22)

The modeled WAL campaign now has a real adapter to the standalone scenario
contract in `pkg/antfly/src/storage/wal_vopr.zig`. It allocates a fresh modeled
device/runtime world for every history, enumerates append, bounded batch,
cursor verification, reopen, and truncate operations as stable transitions,
and makes the final modeled-device crash/recovery an explicit fault transition.
Its observations include the acknowledged model digest, visible entry count,
LSN watermarks, virtual time, reopen count, and recovery state. Named
properties continuously compare public WAL reads with the acknowledged model
and specifically require acknowledged entries to survive the modeled crash.

The smoke test records both existing WAL seeds as `vopr-trace-v1`, parses the
serialized artifact, rebuilds a clean device generation, and requires exact
byte-for-byte replay. This is the first storage suite using the common runner;
the older schedule fixtures remain supported as seed/regression inputs during
the migration.

PersistentIndex, IndexManager, and DB split now have equivalent live adapters
rather than build-gate labels around legacy fixtures. Their harnesses own the
same real domain objects and `ModeledDevice` instances used by the focused
suites. VOPR selects individual document/segment, reopen, split/handoff, and
crash/recovery transitions; named properties compare every observation with
the existing reference-summary oracle and require acknowledged state to
survive recovery. Each adapter records and repeatedly exact-replays clean
worlds in its existing focused build gate. The standalone registry exposes
them as `persistent`, `index-manager`, and `db-split`; `wal` now has matching
CLI parity as well.

Campaign dispatch is scenario-generic rather than metadata-only. It records
and exact-replays all registered domains, reloads only matching persistent
corpus entries, mutates a recorded structured choice by rerunning the owning
scenario, and splices compatible observation joins under the original
decision budget. A bounded two-worker persistent-index campaign proves both
mutation and successful splicing, and `sim-registry-test` records and
clean-world replays every context-free domain through this dispatch table.

The Phase 4 exit scenario is implemented as `metadata VOPR distributed data
survives split partition node restart and modeled storage crash`, with the
focused `zig build lib-metadata-vopr-data-test` gate and inclusion in
`zig build sim-test`. Each of the three data replicas opens its real LSM-backed
Antfly DB and indexes through a node-local `ModeledDevice`; the same injected
backend is used by hosted public APIs, median-key planning, runtime telemetry,
and the real split synchronization coordinator. The test acknowledges three
documents through the public batch endpoint, drives an automatic range split,
isolates the metadata leader while the transition finalizes, heals the
network, crashes the source leader's modeled device, restarts that node and
the public API stack, then acknowledges two more cross-range writes.

The same world now composes the split with an explicit merge of the two child
ranges. Because split assigns distinct document-identity namespaces, the merge
uses the production policy's explicit identity-reassignment opt-in rather than
weakening automatic admission. It waits for finalized transition state,
retires the donor, resolves a former right-range key through the surviving
group, and requires all five acknowledged documents plus the single-shard
query profile. This is a topology composition, not two independent fixtures,
so the preceding partition, restart, modeled crash, and acknowledged writes
remain live history for the merge assertion.

`DistributedDataVoprScenario` makes the expensive integration history a
versioned VOPR scenario. Seeded generation chooses immediate versus delayed
transport, one of three split fault plans, and one of four merge fault plans
from canonical weighted alternatives. The terminal workload transition runs
the physical three-node scenario with a required modeled storage crash. Its
artifact records stable observations and the named split/merge completion and
acknowledged-data properties; exact replay rebuilds fresh cluster, API, and
modeled-device state and byte-compares the complete artifact. The CLI accepts
`vopr run --scenario distributed-data` and dispatches its artifacts through the
same generic replay command. The focused gate performs both physical record and
clean-world replay.

The hosted HTTP topology rig deliberately models merge progress in
`SimMergeRuntime`; it does not own `DataServer`'s cached-writer leases and must
not open a second LSM writer to impersonate the data runtime. Its modeled
bootstrap therefore materializes donor documents on each active receiver
replica before retirement. A paired production-path regression in
`data/runtime.zig` exercises the real borrowed-lease `MergeCoordinator`: it
seeds an existing donor apply projection, adds a boundary document later to the
authoritative donor DB, performs merge accept/catch-up, and requires both the
old and boundary documents in the receiver. `initLocalMergeRuntime` now
reconciles an existing projection at its exact Raft watermark instead of using
the split-only seed-if-absent shortcut. This closes the split-destination-as-
merge-donor data-loss hole without giving the control-plane simulator unsafe
storage ownership. The regression is included in `zig build
lib-data-runtime-test`.

An explicit acknowledged-data reference model records keys only after a
successful public response. Its terminal property checks every modeled key by
public point lookup and checks the complete set through a routed full-text
query, in addition to the existing two-shard count/profile assertion. Runtime
status publication was also made faithful to the replacement-snapshot
protocol: capacity, Raft membership, and document-identity evidence are
reported together, so the scenario exercises the same automatic-split safety
preconditions as production.

Modeled WAL write and sync failures are now first-class fault transitions in
scenario version 2 rather than unclassified harness errors. Once armed, the
next enabled workload operation must consume the fault; the public append is
classified as rejected, is not added to the acknowledged model, and emits a
stable response event containing the error-class digest. Observations expose
pending-fault and rejected-operation state. A scripted regression forces both
outcomes, checks that neither becomes a phantom acknowledgement, crashes and
recovers the device, and exact-replays all five decisions.

Scenario version 3 adds two uncertainty outcomes that exercise the modeled
device rather than synthesizing a higher-level result. A partial-write fault
writes a bounded prefix into volatile storage and returns
`InjectedPartialWriteFault`; the operation is rejected, the torn bytes are not
acknowledged, and immediate crash recovery must discard them. A dropped-sync
fault consumes the real sync call but returns success. Its client response is
recorded as an acknowledgement with uncertain durability, and recovery permits
only a prefix of the modeled state: every required acknowledgement must remain,
while uncertain tail entries may survive, disappear, or cause a recognized
fail-closed WAL startup error. Fail-closed recovery is allowed only when no
required entry is at risk and is emitted as an explicit client outcome rather
than repaired by the harness. The modeled device counts fault consumption so a
fault that misses its intended write or sync becomes a harness error. Both
outcome sets exact-replay through the focused `wal-vopr-test` gate.

Scenario version 4 adds generated device-full failures. The scenario sets the
device capacity to the exact currently used byte count, requires the next WAL
append to consume `InjectedDeviceFull`, classifies the public operation as
rejected, clears the bounded fault, and exact-replays the resulting history.
The underlying `ModeledDevice` now has typed next-operation and path-targeted
failures for read, write, sync, truncate, rename, and delete; explicit capacity
accounting; and selected volatile-versus-durable byte corruption. Volatile
corruption disappears on crash unless synced, while durable corruption remains
in the recovered image, so recovery promises can test detection without
conflating the two states. Partial writes, dropped syncs, and device-full each
have explicit consumption counters that turn a missed intended operation into
a harness error.

The `storage-sim-runtime-test` target now actually references the modeled
runtime test declaration and supplies its platform dependency. This corrected
a prior zero-test gate: it now executes the complete imported storage suite
(168 tests at this checkpoint), including the typed fault, capacity, namespace
durability, and corruption cases.

Modeled completion scheduling is externally selectable rather than implicitly
FIFO-only. The runtime exposes a canonical pending-completion snapshot, can
delay or complete one selected stable completion ID, and can advance virtual
time without draining unrelated work. Tests prove that two operations due at
the same time can complete in either selected order without changing their
identity. This is the storage analogue of selected network delivery and is the
completion seam used by the migrated modeled-storage adapters.

Eligible metadata VOPR artifacts can also be exact-replayed with a formal Raft
sidecar through `zig build sim-tla -- --trace ... --domain raft --out ...`.
Replay attaches Antfly's existing `RaftNdjsonTraceLogger` only to the metadata
group at descriptor construction, so data-group events cannot contaminate the
single-group `Traceetcdraft.tla` model. The CLI validates every generated
NDJSON record before writing it, and the end-to-end meta-test requires a
promoted trace to replay byte-for-byte while producing a non-empty stream that
contains `InitState`. The resulting file is directly consumable by the
existing `tla-trace-raft` validation target; formal output remains a derived
sidecar, so it does not change the canonical `vopr-trace-v1` replay ABI.

Transaction artifacts now use the same workflow. The three-transition
`modeled-transaction` scenario runs the real memory-backed `TxnManager` through
begin, write-intent, and a selected commit/abort decision. The runner's
diagnostic-only scenario context attaches `AntflyNdjsonTraceWriter` during
exact replay; byte equality of the complete VOPR artifact proves that adding the
formal sink cannot influence choices or semantics. `sim-run --scenario
transaction`, ordinary `sim-replay`, and `sim-tla --domain transaction` produce
and consume these artifacts. The focused `transaction-vopr-test` validates the
sidecar shape, and the emitted five-event commit history passes the real
`make tla-trace-txn` segmentation and TLC refinement pipeline against
`TraceAntflyTransaction.tla`.

The HA lifecycle now uses the same workflow as well. Its scenario owns fresh
durable primary/standby/fence worlds, records every selected fault lifecycle,
and continuously checks ordered standby progress, applied-prefix consistency,
remote-apply acknowledgement soundness, backup-slot restart preservation,
durable fencing, monotonic promotion identity, and safe former-primary rejoin.
Three scripted histories make the critical crash, retention, and promotion
paths non-probabilistic, while seeded histories broaden ordering coverage and
must replay exactly. `ha-vopr-test` is part of `sim-test` and the bounded
`chaos-test` aggregate; `ha-chaos-test` remains green as the deeper focused
matrix.

Phase 4's stated exit condition is now met, including split-to-merge
composition and generated/replayable fault-plan choices. The physical
integration history remains one terminal workload transition because the
hosted HTTP rig owns real threaded listeners and cached DB writers; further
decomposition must happen at the DataServer lease and request-executor seams,
not by opening competing simulated writers.
Modeled partial writes, dropped syncs, device-full outcomes, and both Raft and
transaction TLA+ export are complete.

### Phase 5: Search and Snapshot Optimizations

- add target-state search and rarity-weighted energy
- add compatible trace splicing
- add explicit logical snapshots for expensive prefixes
- evaluate compiler coverage integration
- add richer causal reports

Exit condition: optimizations increase states or failures found per CPU unit
without altering replay semantics.

#### Phase 5 implementation progress (2026-08-22)

The generic explorer now accepts weighted semantic target features, retains
target-reaching histories even after ordinary novelty is exhausted, reports
target hits, and incorporates target proximity alongside existing inverse-hit
rarity in corpus energy. Target scoring is based only on canonical observation
IDs and values and counts each target once per history.

`lib/vopr/src/splice.zig` finds candidate joins using matching logical
observation digests and provides a choice source that combines an exact prefix
with a rebased suffix. The suffix must reproduce its stable choice sites and
complete enabled sets; the resulting artifact must still replay from a clean
world before retention, so a digest collision or insufficient state signature
cannot silently create an invalid history.

`lib/vopr/src/snapshot.zig` defines scenario-owned logical checkpoints with
integrity digests and deduplicated storage. Scenarios serialize modeled values
and logical IDs through explicit hooks instead of copying heap pointers or OS
resources. A checkpoint is bound to the scenario ABI and a stable digest of
the complete configuration and exact choice prefix. It also owns a canonical
copy of every property status, including first-failure and quiescence state, so
resuming cannot reset an accumulated safety or recovery obligation.

The generic campaign now chooses splicing under a bounded policy, rejects
incompatible joins, exact-replays every successful result, and reports attempt,
acceptance, and rejection counts. Its mutation path also selects logical
checkpoint prefixes, re-executes and byte-compares the complete canonical
prefix artifact, captures only scenario-owned bytes and property values, and
deduplicates the checkpoint. Subsequent mutations restore that state, begin at
the exact mutation occurrence, and combine the original prefix records with the
new suffix. A candidate that would affect corpus state is exact-replayed from a
clean world before retention; uninteresting candidates avoid that full replay.
Campaign reports separately count clean histories, property-failing histories,
non-property failures, harness errors, replay divergences, and exact-replay
checks. This prevents an invalid choice stream or simulator error from being
presented as an Antfly correctness failure.

The long-running Antfly metadata workers also attempt splices between retained
persistent-corpus entries. IDs for independent generated histories remain
stable within a campaign while seeds vary scheduling; candidate joins preserve
the metadata driver's fixed operation-plus-quiet-suffix choice count, reproduce
the complete enabled sets, and exact-replay before retention. The campaign
summary reports splice attempts and accepted joins.

`lib/vopr/src/causal.zig` implements the actor/resource/fault semantic slice
described above, and `zig build sim-explain` exposes it after dispatching exact
replay by artifact scenario. Promotion uses the same fail-closed registry and
separate metadata, distributed-data, and transaction fixture namespaces.
The injected-bug meta-test requires the promoted trace to yield a non-empty
causal report containing the stable property identity.

`lib/vopr/src/benchmark.zig` commits a host-independent benchmark that counts
executed modeled transitions rather than wall time. With 128 fixed-seed toy
histories, baseline and optimized campaigns both retain two histories, discover
20 semantic features, and find no failures. The baseline executes 512
transition work units; checkpoint resume executes 281, a 451,171 ppm (45.1%)
reduction, with 123 checkpoint hits and 244 prefix transitions avoided. The
`zig build vopr-benchmark` command emits these inputs and results as JSON, and
the test fails if search results differ or the optimization does not improve
work units.

Phase 5's exit condition is met. Compiler coverage was evaluated and remains an
optional secondary feedback channel: Zig/LLVM instrumentation interfaces are
not yet stable enough to become part of the replay or CI ABI, while semantic
coverage already provides stable host-independent guidance. Future causal
enrichment with domain-specific message and storage-operation links can improve
triage without reopening the phase or changing replay semantics.

## Next Steps

These are ordered by expected correctness value. Each new scenario must use the
common artifact, campaign, replay, reduction, promotion, and fixture paths; a
new suite-local seed runner is not sufficient.

### 1. Distributed Transaction Lifecycle

Replace the current three-transition, memory-backed transaction scenario with
a separate production-shaped scenario that retains the focused scenario as a
fast conformance gate. Drive real multi-table-group coordination, participant
prepare and phase-two delivery, durable session ownership, recovery indexes,
and table-group intent resolution.

Scheduler-visible operations should include:

- begin, read, write-intent, savepoint, commit, abort, and idempotent retry
- prepare and phase-two delivery to each selected participant
- ambiguous coordinator response before or after durable decision
- coordinator and participant crash/restart at every persistence boundary
- session lease renewal, expiry, adoption, release, and stale-owner attempt
- recovery scan pages, repair handoff, retry deadline, and intent cleanup
- split, merge, or leadership change while a transaction remains unresolved

Properties must require one globally consistent terminal decision; forbid
aborted visibility; preserve every acknowledged commit through crash; keep a
retry under the same transaction ID idempotent; fence stale owners; and, after
quiescence, resolve every durable intent or retain an explicit bounded repair
obligation. The scenario should export the existing transaction TLA+ sidecar
and add a differential oracle over the client history. The focused gate should
be `distributed-transaction-vopr-test` and join `sim-test` and `chaos-test`.

### 2. DataServer and Public Data-Plane Microsteps

Decompose `DistributedDataVoprScenario`'s terminal physical transition without
opening a second DB writer or bypassing production lease ownership. First add
borrowed-lease and request-executor suspension points, then expose:

- public request admission, routing, execution, acknowledgement, and timeout
- per-group data-Raft message delivery, persistence, application, and snapshot
- writer-lease acquisition, handoff, invalidation, and close
- split/merge copy, catch-up, cutover, rollback, cleanup, and identity transfer
- point reads, scans, full-text queries, and cross-range result assembly
- node-local storage completions, crash recovery, and status publication

The acknowledged-operation model remains authoritative. Online properties
check routing and ownership safety; the quiet suffix checks that all successful
writes are readable exactly once through both point and set-valued queries,
topology has no gap or overlap, retired owners cannot publish, and replicas at
the same applied index agree. This scenario should become the primary consumer
of `SimIo` sockets, task scheduling, and modeled files.

### 3. Enrichment, Index, Repair, and Compaction Workflows

Build a derived-state scenario around the real enrichment runtime, index
generation lifecycle, repair jobs, compaction schedulers, durable job stores,
and `DurableJobLane`. Select provider request/result, checkpoint write,
generation publication, cancellation, leadership loss, retry-time advance,
repair, compaction, cleanup, and crash/restart as independent transitions.

Use deterministic inference/extraction providers with structured success,
transient, permanent, malformed, partial, capacity, and cancellation outcomes.
Properties must ensure derived state never advances beyond its source; stale
generations never publish; retry and cancellation debt survives restart;
cancellation is not terminal before durable cleanup; resource budgets remain
bounded; acknowledged documents survive repair and compaction; and every
nonterminal obligation completes or reaches an allowed terminal state after
healing.

### 4. Backup, Restore, and HA Seed Lifecycle

Compose portable backup manifests, object-store transfer, HA seed capture and
activation, restore-job persistence, topology reconstruction, retention pins,
and generation garbage collection. Expose partial upload/download, delayed
visibility, duplicate request, manifest publication, cancellation, crash,
resume, leadership change, retention, activation, and GC as choices.

The oracle records the source table identities, configurations, acknowledged
documents, and index-generation state. A restore must become atomically usable
or fail closed; a partial restore cannot report success; idempotency keys cannot
alias principals or resources; cancellation racing successful completion must
have one durable winner; active backups and restores remain pinned; and GC must
never remove the only required generation. Run both modeled-object-store and
real local-object-store differential modes.

### 5. Clock, Lease, Retention, and TTL Faults

Generalize node clock domains into a reusable fault surface and apply it across
metadata cooldowns, transaction leases, TTL deletion, job backoff, HA
retention, backup pins, and generation GC. Select forward and backward realtime
jumps, temporary skew, frequency changes, monotonic advance, timer delivery,
and node pause independently; multiple clock faults may overlap subject to an
explicit budget.

Properties must separate realtime expiry from monotonic deadlines, prohibit
early lease takeover and premature deletion, fence expired owners, preserve
retry deadlines through restart, prevent timestamp regression from resurrecting
deleted or retired state, and require eligible cleanup after clocks stabilize.
Every time-sensitive production path in these scenarios must consume the
injected `std.Io` clock; a host-clock call is a harness failure.

### Self-Contained Antithesis-Class Runtime on `std.Io`

The five domain scenarios should be developed alongside `SimIo`, not after a
monolithic virtual OS is complete. Each capability is admitted only when a real
scenario and conformance test consume it.

Implementation order:

1. **Inventory and fail-closed shell.** Generate a Zig-version-pinned
   `std.Io.VTable`, implement deterministic unsupported handlers for every
   entry, declare capability requirements, and add a syscall audit proving a
   simulated history cannot reach `std.Io.Threaded`.
2. **Fiber task kernel.** Implement futures, groups, select, cancellation,
   queues, futexes, timers, and deterministic entropy over stackful simulated
   tasks. Every park, wake, cancel, spurious wake, eager completion, and runnable
   task selection has a stable choice identity.
3. **Modeled files and sockets.** Adapt `ModeledDevice` and the virtual HTTP
   network to actual `std.Io` file/directory and socket handles. Preserve the
   existing durability and message models while allowing production code to
   use standard interfaces unchanged.
4. **Registered process and resource model.** Run registered Antfly entrypoints
   with isolated virtual process state; add kill, pause, restart, quota,
   descriptor exhaustion, memory pressure, and deterministic CPU-work
   modulation. Reject arbitrary executables rather than escaping the model.
5. **Instrumented preemption and coverage.** Add optional stable safepoints for
   CPU-bound regions and basic-block feedback for search energy. Coverage and
   symbol maps are build sidecars, never implicit replay inputs; any safepoint
   selected by a trace is part of the scenario compatibility digest.
6. **Counterfactual causality.** For selected prefixes before a failure, replay
   alternate enabled decisions under bounded descendant budgets and report how
   often the same fingerprint remains reachable. Store the experiment
   configuration and child artifact digests so the causal report is itself
   reproducible.
7. **Multiverse debugger.** Add `sim-debug` commands to seek by choice or event,
   inspect tasks/futures/futexes/files/sockets/processes and domain observations,
   list enabled alternatives, branch or inject a fault, continue, and export a
   canonical child artifact. Seeking always means clean replay or a
   replay-proven logical checkpoint, never mutation of an unversioned heap
   snapshot.
8. **Time-travel artifact collection.** Let scenarios register deterministic
   logical collectors for manifests, task graphs, network queues, storage
   namespaces, and domain state. A failure can request artifacts from a prefix,
   the failure boundary, and a bounded diagnostic future by replaying those
   moments.

`SimIo` is accepted only when all of the following hold:

- the same production component passes behavioral conformance on
  `std.Io.Threaded` and `SimIo`
- retained histories exact-replay 100 consecutive times in Debug and
  ReleaseSafe with no real-I/O audit events
- meta-tests discover, replay, and reduce bugs requiring task preemption,
  cancellation, futex wake ordering, network backpressure, clock skew, process
  crash, storage publication, and resource exhaustion
- counterfactual reports and debugger-created branches exact-replay from a
  clean world
- unsupported operations fail before affecting product state and are reported
  as harness capability errors, never product failures
- disabling instrumentation changes search efficiency only; it cannot change
  the meaning of an already compatible artifact

#### SimIo implementation progress (2026-08-22)

The fail-closed shell and fiber task kernel are implemented in
`lib/vopr/src/sim_io.zig` and `lib/vopr/src/sim_io_task.zig`. Construction performs
capability preflight; the complete Zig 0.16 vtable is based only on
`std.Io.failing`; infallible unsupported paths latch a harness violation; and a
conformance test proves that no vtable entry aliases `std.Io.Threaded`.
Canonical trace backend IDs pin the Zig version, supported capability set,
virtual-OS model, and instrumentation map without changing `vopr-trace-v1`.

Stackful tasks now expose future and group execution, await, cancellation,
sleep, and futex wake selection through the existing `SchedulerPort`. Standard
`std.Io.Mutex`, queues, and `Select` run on those primitives. Task resumption,
futex waiter selection, and time advance all have logical stable IDs; no trace
identity contains a pointer. The focused scenario records a real `std.Io`
fiber history and exact-replays it 100 times from a clean world. These tests run
through `zig build vopr-test` in Debug and ReleaseSafe.

The first modeled-file capability is implemented in
`lib/vopr/src/sim_io_file.zig`. It uses virtual integer handles only and covers
deterministically ordered directory iteration, recursive directory rename,
positional and streaming reads/writes, atomic publication, locks, mappings,
metadata, descriptor/capacity limits, partial I/O, file-data sync, separate
namespace sync, dropped sync, and reconstruction from durable state after a
crash. The conformance test exercises that surface through ordinary `std.Io`
directory, file, atomic-file, lock, and memory-map APIs. Symlinks, hard links,
and optimized file-to-file transfer deliberately fail closed and latch a
harness capability violation; no operation delegates to a host filesystem.

Virtual sockets, registered processes/resources, richer storage corruption and
completion-order faults, instrumentation, counterfactual tooling, and the five
domain scenarios remain required work; the partial capability set does not
advertise those capabilities.

## Risks and Mitigations

### False Determinism

The same seed can appear stable while hidden wall time or thread scheduling
still affects behavior.

Mitigation: replay explicit choices, compare observations after every
transition, audit nondeterministic dependencies, and run repeated
replay-equivalence tests across build modes.

### Simulator Drift from Production

A fully mocked system may validate behavior that production does not execute.

Mitigation: keep production implementations behind the same narrow interfaces,
reuse real state machines and encoders, retain real-HTTP and process-level
integration tests, and add differential replay between modeled and physical
backends.

### Coarse Atomicity

If a transition executes too much work, important races stay hidden.

Mitigation: start with message, node-round, timer, storage-completion, and actor
boundaries; use discovered blind spots to introduce narrower scheduling seams.

### Excessive Invalid Histories

Uniformly combining every command and fault can spend most of the budget in
uninteresting outage states.

Mitigation: state-aware command preconditions, explicit fault budgets, minimum
quorum profiles, quiet suffixes, and coverage properties that measure whether
useful overlap occurred.

### Property Bugs

An incorrect oracle can produce false confidence or false failures.

Mitigation: keep properties small and named, test them against intentionally
faulty models, use differential oracles, preserve full histories, and cross-check
eligible traces with TLA+.

### Artifact Fragility

Refactors can invalidate traces if identity depends on enum ordinals, line
numbers, or incidental ordering.

Mitigation: explicit stable IDs, canonical rendering, versioned scenario ABI,
and a divergence report that identifies the first incompatible choice.

### Reduction to the Wrong Failure

Any-error reduction may find a simpler unrelated error.

Mitigation: require the same stable fingerprint and reject divergence, invalid
setup, or harness errors.

### State Explosion

Atomic scheduling dramatically increases possible histories.

Mitigation: bounded histories, semantic coverage, corpus retention, structured
mutation, scenario profiles, target states, and later snapshot caching.

### Performance and Memory Cost

Debug allocators, full observations, and verbose traces can dominate campaign
execution.

Mitigation: separate canonical compact records from optional verbose state,
bucket observations, stream NDJSON, use release-safe campaign builds, and retain
full diagnostics only around novel or failing histories.

## Success Metrics

Correctness and usability matter more than raw transition count.

Track:

- replay success rate for retained artifacts
- median and maximum reduction ratio
- unique failure fingerprints
- time from CI failure to local exact replay
- semantic states and transitions per CPU hour
- property reachability by scenario
- fraction of histories exercising meaningful overlapping faults
- promoted regressions that would not have been covered by fixed scenarios
- divergence rate after ordinary refactors
- modeled-versus-production differential agreement

The system is successful when a developer receiving a chaos failure can run one
command, reproduce it exactly, inspect a short causal history, and keep the
reduced case as a permanent regression.

## Recommended Decisions

The following decisions should be treated as defaults unless implementation
work uncovers a concrete contradiction:

1. Build an in-process, application-scoped deterministic virtual OS behind
   `std.Io`, not a general-purpose binary hypervisor.
2. Use the metadata VOPR cluster as the first vertical slice.
3. Make single-transition scheduling the core abstraction.
4. Record explicit structured choices; seeds are discovery metadata only.
5. Use semantic coverage as the stable replay-independent signal and add
   compiler/basic-block coverage as secondary search feedback.
6. Keep replay from a clean world as the source of truth; add snapshots only as
   an optimization.
7. Use non-fatal named properties and a deterministic quiet suffix.
8. Preserve the same failure fingerprint during reduction.
9. Use a standalone VOPR command surface compiled as a harness-only test
   artifact; keep the ordinary unit-test runner behavior simple and never ship
   the VOPR shell as a production binary.
10. Migrate existing suites incrementally and preserve their focused oracles.
11. Run real HTTP, threaded, filesystem, socket, and registered process code on
    `SimIo`; keep native process chaos as a differential compatibility layer.
12. Fail closed on every unsupported `std.Io` capability and prohibit hidden
    fallback to the host runtime during replay.
13. Require explicit human review before fixture promotion.

## Resolved Design Decisions and Follow-ons

Implementation resolved the Phase 0 design questions as follows:

- scenario execution uses compile-time dispatch while artifacts, choices, and
  application-facing runtime capabilities use stable runtime types
- canonical traces store the complete enabled alternative set, making replay
  divergence diagnostic rather than relying on an opaque set digest
- safety and client-visible properties prefer public observations; internal
  inspectors are limited to scheduler enablement, stable semantic coverage,
  and diagnostics that cannot change product decisions
- the first public-data consistency model is an acknowledged-operation model:
  only successful responses enter the required set, while explicitly uncertain
  durability outcomes have a separate allowed-result set
- trace compatibility is fail-closed on system, scenario version, schema
  version, choice site, occurrence, and complete enabled set; format migration
  must parse and exact-replay before producing a new canonical artifact
- the corpus manager remains Zig-native so selection, mutation, coverage, and
  replay share one deterministic implementation; external orchestration may
  allocate workers and wall-clock budgets but does not select transitions
- compiler coverage remains secondary feedback because the current Zig/LLVM
  instrumentation surface is not stable enough to join the replay ABI; stable
  instrumented safepoints may affect scheduling only when their map digest is
  an explicit scenario compatibility input
- VOPR command steps compile in test mode through a dedicated runner because
  Antfly scenarios use test-only allocators, I/O, and temporary directories;
  this is a command boundary, not permission for production code to import
  `std.testing`

One bounded expansion remains outside the completed Phase 0-5 exit conditions.
The distributed public-data scenario generates and replays its transport and
fault plan, but its terminal physical history can be decomposed further only
after DataServer borrowed-lease and HTTP execution seams become
scheduler-controlled.
Opening competing simulated writers would invalidate the test, so the current
integration transition deliberately preserves production ownership. Physical
temporary directories remain acceptable for that integration shell; every
durability claim inside the history uses `ModeledDevice`. This expansion is
Next Step 2 and should consume `SimIo`; it does not change the stable narrow
scheduler kernel or the `vopr-trace-v1` format.

## Conclusion

Antfly does not need a hosted deterministic-testing service to gain the hard
Antithesis-like behavior relevant to this codebase. It already has deterministic
cores, virtualized network and storage components, seeded workloads, reference
models, reduction, fixtures, and formal trace checking. The opportunity is to
connect production code to those components through `std.Io` and one explicit
decision and transition model.

The decisive first step is not adding more random seeds or more named chaos
tests. It is making each workload, scheduling, and fault decision visible and
replayable. The next decisive step is a fail-closed fiber-backed `SimIo`, grown
only through real Antfly consumers. That makes deterministic task scheduling,
virtual files and sockets, process/resource faults, counterfactual causality,
and multiverse debugging incremental capabilities on a self-contained shared
foundation.
