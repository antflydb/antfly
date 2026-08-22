# VOPR: Deterministic Autonomous Simulation for Antfly

Status: implementation in progress; Phases 0, 0.5, and 1 complete; Phases 2–4 partially implemented

Scope: Zig Antfly simulation, VOPR, modeled-storage, and chaos tests

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

The goal is not to reproduce Antithesis's deterministic hypervisor. The first
system should remain an in-process Zig simulator built around Antfly's existing
dependency seams. Exact replay of an explicit decision prefix provides most of
the debugging and branching value without requiring arbitrary heap snapshots.

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

These are design influences, not a claim that the proposed in-process engine
will initially provide hypervisor-level determinism, arbitrary process rewind,
or the same search algorithms.

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
- Make it straightforward to package the same workloads and properties for the
  hosted Antithesis product later.
- Separate harness failures from product property violations.

## Non-Goals

- Building a deterministic virtual machine or hypervisor.
- Snapshotting arbitrary Zig heaps, threads, sockets, or external processes in
  the first implementation.
- Replacing unit, integration, differential, TLA+, or production-like end-to-end
  tests.
- Modeling every operating-system failure immediately.
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
- **Enumerating**, later: systematically explores small bounded choice trees.

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

The MVP does not require OS threads. Parallel clients and background work are
modeled as actor state machines with start, poll, and completion transitions.
This exposes the relevant interleavings while preserving determinism.

Production threaded implementations still require separate stress and thread
sanitizer tests. Later, selected thread scheduling points may be exposed as
simulator transitions if the production seams permit it.

### Runtime Capability Boundary

VOPR should use the successful shape of `std.Io`—small copyable handles backed
by an explicit context pointer and vtable—without implementing the complete
`std.Io.VTable` as part of the deterministic kernel. In Zig 0.16 that vtable
owns concurrency, futexes, files, processes, clocks, entropy, networking, and
other OS behavior. Implementing it merely to control background jobs would
couple VOPR to a very large surface and invite accidental nondeterministic
fallbacks.

The generic package therefore defines narrow capabilities:

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
A future `SimRuntime` instead queues tasks and timers and exposes them through
`SchedulerPort` as stable transitions. Application code receives `Runtime`,
never `SchedulerPort`, so it cannot drive or inspect the simulator.

An actual `std.Io` backend is optional future work for dependencies that truly
require the complete interface. It must have a reviewed capability matrix and
must never silently delegate time, randomness, filesystem, network, process,
or scheduling operations to a real threaded backend during deterministic
replay. Arbitrary blocking `async`/`concurrent` callbacks also require a proven
fiber or equivalent suspension design; the atomic state-machine scheduler does
not pretend to provide that behavior.

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

Expose atomic host rounds, per-group scheduler choices, message deliveries,
snapshot operations, persistence/apply completions, and backpressure. Reuse the
existing differential trace corpus and compare selected histories with etcd.

### Modeled Storage

Adapt WAL, LMDB, persistent index, DB split, index manager, and LSM campaigns to
the common trace/property interface. Preserve their focused reference models
and fixture parsers during migration.

The common runner should first wrap existing action unions; it should not force
one enormous distributed scenario before focused replay parity is proven.

### Data Plane

Add public document traffic, table routing, range transitions, snapshots,
enrichment, index lifecycle, compaction, and repair. The client history and
final-state oracle become essential here.

### HA

Convert crash-phase, standby, fencing, reseed, timeline, retention, and
promotion tests into commands and properties. Node and storage faults then use
the same lifecycle as metadata scenarios.

### Production-Like Processes

Process/container chaos remains a distinct higher layer. A future adapter may
drive real Antfly binaries and record commands and fault events in the same
artifact envelope, but it will not have in-process scheduling control.

The in-process workload and property definitions should also map naturally to
actual Antithesis test commands and fallback JSONL assertions if hosted testing
is adopted.

## Module Layout

```text
lib/vopr/
  build.zig
  build.zig.zon
  src/
    root.zig
    choice.zig
    transition.zig
    runner.zig
    event.zig
    trace.zig
    replay.zig
    property.zig
    observation.zig
    runtime.zig
    sim_runtime.zig
    vopr-trace-v1.schema.json
    scheduler.zig
    coverage.zig
    corpus.zig
    explorer.zig
    reducer.zig
    fixture.zig

pkg/antfly/src/sim/
  DETERMINISM_AUDIT.md
  cli.zig
  scenarios/
    metadata.zig
    raft.zig
    wal.zig
    lsm.zig
    ha.zig
  fixtures/
    metadata/
    raft/
    wal/
    lsm/
    ha/
```

Domain-specific adapters may live beside the domain when that produces a
cleaner dependency direction. The stable rule is:

- generic engine types do not import metadata, Raft integration, or storage
- scenarios import the generic engine and their domains
- production code only depends on narrow runtime, clock, entropy, transport,
  and storage interfaces, never on the explorer or `SchedulerPort`

The CLI should be a standalone executable rather than additional behavior in
the unit-test runner. The current custom test runner has a deliberately small
argument surface and fail-fast `std.testing` semantics; campaigns need budgets,
artifacts, replay modes, and property aggregation.

## CLI and Build Integration

Proposed commands:

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

# Exact-replay an eligible artifact and export its formal Raft event stream.
zig build sim-tla -- \
  --trace /tmp/metadata-reduced.simtrace \
  --domain raft \
  --out /tmp/metadata-raft.ndjson

# Run a bounded local campaign.
zig build sim-campaign -- \
  --scenario metadata \
  --histories 1000 \
  --transitions 500 \
  --workers 8 \
  --artifact-dir /tmp/antfly-sim
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
- metadata, LSM, and HA scenarios as they migrate
- labeled progress and failure artifact paths

`chaos-soak-test`:

- broader scenario set
- more workers and larger history budgets
- stress fault budgets
- existing legacy chaos tests until coverage is superseded

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
- explicitly defer a complete `std.Io` backend until a dependency requires it
  and blocking/capability semantics are proven

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

The Antfly metadata adapter now exposes individual node rounds, virtual-time
advancement, and selected queued-message delivery, drop, and duplication. The
virtual network provides a side-effect-free canonical message snapshot and
exact message operations with stable logical sequence IDs and payload digests.
A bounded quiet suffix disables hostile fault choices and evaluates the named
`metadata.eventually_recovers_after_quiescence` property. Metadata partitions
now have explicit start and stop lifecycle transitions: up to two independent
directed-link partitions may overlap, a whole-node partition remains exclusive
to preserve the scenario's healthy-quorum budget, and each active fault plus
the aggregate heal action is independently selectable. Active link, node, and
total fault counts are semantic observations, and the metadata replay ABI was
bumped for the changed enabled sets (currently version 3 after adding the
meta-test configuration field). Phase 2 remains open for
moving additional production background services onto `SimRuntime` instead of
their current manual domain runtimes.

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

`reducer.zig` verifies the original exact replay, explores structured choice
replacements from a clean world, accepts only strictly simpler artifacts with
the same failure fingerprint, and exact-replays every accepted result. Its
meta-test injects a named property bug and reduces a three-transition history
to one transition.

The Antfly CLI now exposes distinct `sim-campaign`, `sim-reduce`, and
`sim-promote` workflows in addition to run and replay. Campaign workers execute
independent deterministic histories in parallel, merge stable semantic
coverage under a mutex, and retain novel or failing artifacts under the chosen
artifact directory. Metadata reduction first shrinks the generated workload
budget and then performs same-fingerprint structured scheduling/fault
substitution, exact-replaying every accepted result. Promotion refuses
non-failing or non-replaying traces, validates fixture names, and avoids
overwriting by default.

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
selects it for mutation, and exact-replays the result. Richer command-aware
deletion operators remain a useful follow-on reduction improvement.

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

Phase 4's stated exit condition is now met. The phase remains open for modeled
partial-write and dropped-sync outcome sets, migration of the fixed distributed
scenario into generated/replayable VOPR choices, merge composition, and
transaction-event TLA+ export.

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
resources. Checkpoints are exploration accelerators only: retained histories
remain decision traces and must pass clean-world exact replay.

Phase 5 remains open for wiring splice/checkpoint selection into long-running
campaign workers, evaluating compiler coverage hooks, causal-report enrichment,
and committing a benchmark that demonstrates improved states or failures per
CPU unit without changing replay results.

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

1. Build an in-process deterministic simulator, not a hypervisor.
2. Use the metadata VOPR cluster as the first vertical slice.
3. Make single-transition scheduling the core abstraction.
4. Record explicit structured choices; seeds are discovery metadata only.
5. Use semantic coverage before compiler coverage.
6. Keep replay from a clean world as the source of truth; add snapshots only as
   an optimization.
7. Use non-fatal named properties and a deterministic quiet suffix.
8. Preserve the same failure fingerprint during reduction.
9. Use a standalone campaign executable and keep unit-test runner behavior
   simple.
10. Migrate existing suites incrementally and preserve their focused oracles.
11. Keep real HTTP, threaded, and process chaos tests as complementary layers.
12. Require explicit human review before fixture promotion.

## Open Design Questions

These can be resolved during Phase 0 without changing the overall design:

- whether the generic scenario API should use comptime dispatch, type erasure,
  or a hybrid
- whether the canonical trace stores every enabled alternative or only an
  enabled-set digest outside verbose mode
- which metadata state should be observed through public APIs versus internal
  test-only inspectors
- how much node restart state can use the current physical temporary-directory
  paths before a fully modeled device is required
- the first client-history consistency model for public data traffic
- the compatibility policy and migration tooling for trace schema revisions
- whether the corpus manager belongs in Zig or a thin external orchestration
  tool while the deterministic runner remains Zig-native
- which LLVM/Zig coverage hooks are stable enough for an optional later phase

None of these questions blocks traceable metadata VOPR, atomic scheduling,
properties, semantic coverage, replay, or same-fingerprint reduction.

## Conclusion

Antfly does not need to start over to gain Antithesis-like testing behavior. It
already has deterministic cores, virtualized network and storage components,
seeded workloads, reference models, reduction, fixtures, and formal trace
checking. The opportunity is to connect them through one explicit decision and
transition model.

The decisive first step is not adding more random seeds or more named chaos
tests. It is making each workload, scheduling, and fault decision visible and
replayable. Once that exists, autonomous exploration, semantic feedback,
branching, minimization, and durable regression promotion become incremental
capabilities on a shared foundation.
