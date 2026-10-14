# VOPR and Antithesis: Comparison and Federated-Agent Proposal

> Moved verbatim from `zig/VOPR.md` (lines 209–258, 1997–2137 at commit c6c0ba51cd) on 2026-10-12. This is explanatory comparison and a forward-looking proposal, not history; binding rules and remaining work were folded into [VOPR.md](../../../zig/VOPR.md) before the move.

The living contract, conformance claims, and roadmap remain in [VOPR.md](../../../zig/VOPR.md). The federated agent/broker below is a conditional proposal: it is not built, and VOPR.md's Conditional Work section states when it may be.

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
- [Fault types and node scope](https://antithesis.com/docs/product/writing_tests/controlling_faults/fault_types/)
- [Multi-container test templates](https://antithesis.com/docs/product/writing_tests/test_templates/first_test/)
- [Debugging](https://antithesis.com/docs/product/debugging/)

These are influences, not a claim of hypervisor equivalence or identical search
algorithms.

### Distributed-System Correspondence

Antithesis treats containers or Kubernetes pods as distributed fault domains:
separate nodes can experience asymmetric network disruption, pause, kill,
restart, throttling, and independently placed workload commands. VOPR provides
the corresponding deterministic application-level mechanisms—logical nodes,
packet scheduling, directional partitions, process and node lifecycle,
node-scoped durable state, resource budgets, and multi-actor workloads—but
runs registered production entrypoints in one virtual `std.Io` world.

| Dimension | Antithesis | VOPR status |
| --- | --- | --- |
| Multiple logical nodes and clients | Multiple containers or pods | Integrated in metadata, distributed-data, distributed-transaction, Raft, standby, and data-plane suites |
| Link and packet faults | Asymmetric latency, loss, clogs, partitions, and recovery | Integrated drop, duplicate, reorder, delay, jam, outage, directional partition, and healing |
| Node lifecycle and pressure | Pause, stop/kill, restart, and throttling | Integrated at registered process/resource seams: pause, crash/restart, CPU-work exhaustion, descriptor, socket, allocator, and storage limits. Reversible logical per-node and per-operation slowdown/cost modeling is integrated through borrowed `std.Io`; arbitrary native CPU/thread throttling and native-thread pause are not implemented |
| Deterministic replay and branching | Deterministic hypervisor execution | Exact choice/transition/observation replay plus reduction and multiverse branching |
| Whole unmodified deployment | Arbitrary containerized binaries and sidecars | Deliberate non-goal; only registered in-process entrypoints are deterministic |
| One full Antfly deployment history | Runs a supplied Docker Compose or Kubernetes topology | Integrated in-process at named complementary seams: the production-owner full-cluster modes run the real metadata quorum, three production DataServer/data-Raft owners and resource managers, public two-table I/O, the serverless catalog, and an active split on one `VoprIo`, adding one fault or workflow seam per [scenario version](../../../zig/VOPR.md#full-cluster-scenario-versions); v9 supplies the hosted public graph and broader nine-fault vocabulary. Remaining topology, disk-pressure, socket/short-write, and storage/process/restart overlap, v53 completion, and co-resident standby are not yet on the production owners. Separate address spaces, native sidecars, DNS, kernels, and live mixed binaries remain conditional/differential concerns |

Antithesis therefore does support distributed-system testing directly: its
fault domains are containers or Kubernetes pods, including asymmetric network
and node faults. VOPR's corresponding deterministic distributed foundation is
integrated, but whole-deployment composition is not finished merely because
the focused distributed suites pass. Composition breadth can still grow inside
the existing virtual OS. Native multi-process,
container, Kubernetes, DNS, init-system, mixed-binary, and cross-language
behavior remains a focused differential/integration tier.

## Ported Antithesis-Class Features

VOPR already has the important core: structured controlled choices, the major
Antithesis assertion kinds and assertion cataloging, deterministic scheduling
and I/O, logical checkpoints, exact replay, reduction, semantic coverage,
starvation, causal and counterfactual analysis, and multiverse navigation. The
documented [Antithesis assertion
model](https://antithesis.com/docs/product/writing_tests/assertions/) is
therefore substantially covered. The hard, high-value features identified for
local replacement are integrated:

1. bounded retroactive flight recording with structured field/text selection
   and diagnostic before/after windows, the self-contained analogue of the
   [August 2026 Retroactive Logging
   feature](https://antithesis.com/docs/release_notes/);
2. fielded and temporal event-history queries comparable to [Antithesis event
   logs](https://antithesis.com/docs/reference/event_logs/);
3. stable repository-owned JSON results, a persistent cross-run usage index,
   and static reports corresponding to
   the run/log APIs described in the [Antithesis release
   notes](https://antithesis.com/docs/release_notes/);
4. automatic reduction/causal/counterfactual/query/collector/flight-window
   debug recipes;
5. first-failure, rare-success, structured-detail, and never-encountered
   property evidence;
6. explicit overlapping-fault precedence and exclusion algebra, matching the
   useful application-level behavior of [overlapping
   faults](https://antithesis.com/docs/product/writing_tests/controlling_faults/fault_types/);
7. automatic structured-choice auditing, following the controlled-alternative
   principle in [Antithesis controlled
   randomness](https://antithesis.com/docs/reference/sdk/generate_randomness/);
8. default harness-health reporting with automatic continuous,
   recovery/quiescent, and final snapshots, plus reusable `VoprIo` resource
   evidence and mature P0/P1 domain adapters; and
9. a representative injected-bug search-quality regression corpus with
   recurrence, rarity, confidence, modeled cost, and smallest-witness evidence
   across all five exploration policies; and
10. a registered deployment composer for roles, instances, readiness
    dependencies, directional links, typed process/storage/resource fault
    domains, measured node policy, and cluster-wide quiet suffixes;
11. versioned saved event-set programs with fail-closed DAG validation and
    bounded cross-history evaluation, plus a runner-owned bounded live NDJSON
    stream that cannot block or perturb replay (see the capability table);
    and
12. a reversible service-rate model with stable node and operation identities
    and per-node/per-operation accounting, adopted at six production
    boundaries (see the capability table).

These capabilities are local libraries, commands, reports, and CI gates. They
do not require an Antithesis account or hosted runtime. This is not a claim of
full Antithesis product parity: the implemented claim is limited to the named
self-contained engine features and application-level distributed fault domains,
while separate-address-space determinism and the operational work below remain
explicitly unfinished or conditional.

The remaining Antithesis-class opportunities are narrower than the engine
work already completed:

- extend the v11–v53 cluster compositions with search policies that overlap
  node-wide and operation-class slowdowns with remaining topology/ownership
  changes, the existing link/storage/resource/restart algebra, additional
  replication timings, metadata leadership loss during cutover, and quiet
  recovery. Arbitrary instruction-level native-thread throttling remains a
  federated/native instrumentation concern rather than an in-process claim;
- grow the saved event-set expression vocabulary only in response to concrete
  debugger/report needs. The versioned plan deliberately provides typed set,
  moment, previous/next, and sequence operations instead of an embedded
  general-purpose map/fold language; reports can add reviewed aggregations
  without making arbitrary code part of a saved query;
- delta-encode or intern repeated canonical observations in long histories.
  Exact replay must still compare every semantic observation, but a deep Debug
  campaign should not require multi-gigabyte retention merely because most
  adjacent states repeat. This is artifact/runtime efficiency work, not a
  relaxation of replay truth or a substitute for the bounded flight recorder;
- operate the scheduled sharded corpus-retaining workflow, review its quarantined
  traces, and add usage indexing and notifications;
- extend the registered-source determinism audit through transitive production
  callees reached by borrowed-`std.Io` scenarios, with reviewed exceptions and
  stable semantic lock/operation identities. The present manifest is a useful
  fail-closed gate but did not detect the production cache-role address order
  that the v12 deep history exposed;
- ingest source/basic-block coverage only as search guidance, with fail-closed
  symbolization, when Zig instrumentation is stable enough to avoid entering
  the replay ABI; and
- add more automatic debug-recipe policies as evidence accumulates, such as
  selecting collectors and counterfactual budgets from a failure class.

Antithesis's container/Kubernetes execution, browser notebook, arbitrary shell
and file injection, and hosted control plane are not missing VOPR correctness
features. They are deliberately replaced by registered `std.Io` production
entrypoints, exact local artifacts, the line-oriented debugger, and ordinary
native/container differential tests. Reimplementing a deterministic
hypervisor or hosted UI would be the expensive part of Antithesis without
improving the in-process scheduler's visibility.

Antithesis also announced control of the guest kernel's internal random-number
generator in its [August 2026 release
notes](https://antithesis.com/docs/release_notes/). VOPR's self-contained
equivalent is complete only for code that draws entropy through registered
`std.Io.randomSecure` and whose source is included in the registered audit.
Transitive production-call-graph proof, interception inside an arbitrary
unmodified native dependency, and guest-kernel RNG control are not implemented;
they belong with the audit and conditional federated/native fidelity work, not
under the integrated entropy claim.

Deterministic distributed testing itself is therefore not an unported
Antithesis engine feature: VOPR already supplies registered
node/process/resource/link domains. Antithesis currently scopes node faults to
containers or Kubernetes pods and can inject asymmetric network loss between
separate nodes, as documented in [Types of
faults](https://antithesis.com/docs/product/writing_tests/controlling_faults/fault_types/);
VOPR's application-level equivalent is its registered instance and directional-
link manifest. Deterministic execution of arbitrary
separate address spaces, sidecars, DNS resolvers, kernels, and mixed binaries
*is* an unimplemented fidelity layer. It should remain a native/container
differential or conditional future project unless a real defect class cannot
be reached through registered `std.Io` entrypoints. The immediate material gap
is which Antfly owners and public workflows have been composed into the same
history, tracked in the Distributed Completion Audit and Ongoing Roadmap.

## Federated VOPR Agent Proposal (not built)

If separate-address-space fidelity becomes necessary, keep it self-contained
as a **federated VOPR agent protocol**, not a hidden claim that ordinary
containers are exactly replayable. The repository-owned coordinator would
remain the sole source of structured choices, logical time, fault composition,
property identity, and artifacts. One versioned agent per instrumented Antfly
process would register its manifest instance and stable actor/resource IDs;
all cross-agent network, clock, entropy, process, and modeled-storage requests
would pass through a framed broker, which releases one completion at a time and
records the decision in the ordinary trace. Agent loss, protocol mismatch,
unregistered native I/O/thread/clock use, or a broker bypass must fail closed.
Logical checkpoints would require an acknowledged application checkpoint from
every agent rather than pretending to snapshot OS process memory.

That design has two explicitly different modes. **Brokered deterministic mode**
would accept only instrumented compatible binaries and could earn exact replay.
**Native/container differential mode** could launch unmodified binaries,
sidecars, DNS, TLS, and real kernels from the same deployment manifest and
collect the same properties/events, but would never be labeled exact replay.
Build the brokered mode only when an address-space-specific defect or live
mixed-version requirement justifies its protocol and operational cost; until
then, deepen the in-process registered-owner composition first.
