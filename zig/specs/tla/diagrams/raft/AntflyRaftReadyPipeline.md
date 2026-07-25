<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyRaftReadyPipeline — structural diagrams

Generated from [`AntflyRaftReadyPipeline.tla`](../../raft/AntflyRaftReadyPipeline.tla). 19 state variables, 11 actions in `Next`. 1 expected-failure mutant action(s) (gated by `Buggy*` constants) omitted.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `phase`

```mermaid
stateDiagram-v2
    direction LR
    continuation --> done : FinishContinuation
```

Writes whose source state is not statically determined:

- `BeginContinuation` sets `phase` to `"continuation"`
- `FinishWithoutContinuation` sets `phase` to `"done"`

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `BeginFair` | `phase` | `phase` |
| `CommitMembership` | `phase`, `admitted`, `raftConf` | `raftConf` |
| `CaptureSnapshotCandidate` | `raftConf`, `appliedConf`, `candidateIndex` | `candidateIndex`, `candidateConf` |
| `BeginContinuation` | `phase`, `fairVisited`, `continuationQueued` | `phase` |
| `FinishWithoutContinuation` | `phase`, `fairVisited`, `admitted`, `completed`, `continuationQueued` | `phase` |
| `FinishContinuation` | `phase`, `continuationQueued` | `phase` |
| `FairAttempt` | `phase`, `fairVisited`, `duplicateFairVisit`, `admitted`, `deferredGroups`, `continuationQueued` | `fairVisited`, `duplicateFairVisit`, `admitted`, `deferredGroups`, `continuationQueued` |
| `CloneReady` | `phase`, `admitted`, `cloned`, `ownedMessages` | `cloned`, `ownedMessages` |
| `CompleteReady` | `phase`, `admitted`, `cloned`, `completed`, `productiveGroups`, `continuationQueued`, `processedSteps`, `ownedMessages`, `appliedIndex`, `appliedConf`, `candidateIndex`, `configApplied`, `appliedWithoutOwnership` | `completed`, `productiveGroups`, `continuationQueued`, `processedSteps`, `ownedMessages`, `appliedIndex`, `appliedConf`, `configApplied`, `appliedWithoutOwnership` |
| `SendMessages` | `cloned`, `completed`, `ownedMessages`, `sentMessages` | `sentMessages` |
| `RunContinuation` | `phase`, `continuationQueued`, `processedSteps` | `continuationQueued`, `processedSteps` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[BeginFair]
        a1[CommitMembership]
        a2[CaptureSnapshotCandidate]
        a3[BeginContinuation]
        a4[FinishWithoutContinuation]
        a5[FinishContinuation]
        a6[FairAttempt]
        a7[CloneReady]
        a8[CompleteReady]
        a9[SendMessages]
        a10[RunContinuation]
    end
    subgraph state["State variables"]
        v0([phase])
        v1([admitted])
        v2([raftConf])
        v3([appliedConf])
        v4([candidateIndex])
        v5([candidateConf])
        v6([fairVisited])
        v7([continuationQueued])
        v8([completed])
        v9([duplicateFairVisit])
        v10([deferredGroups])
        v11([cloned])
        v12([ownedMessages])
        v13([productiveGroups])
        v14([processedSteps])
        v15([appliedIndex])
        v16([configApplied])
        v17([appliedWithoutOwnership])
        v18([sentMessages])
    end
    a0 --> v0
    a1 --> v2
    v0 -.-> a1
    v1 -.-> a1
    a2 --> v4
    a2 --> v5
    v2 -.-> a2
    v3 -.-> a2
    a3 --> v0
    v6 -.-> a3
    v7 -.-> a3
    a4 --> v0
    v6 -.-> a4
    v1 -.-> a4
    v8 -.-> a4
    v7 -.-> a4
    a5 --> v0
    v7 -.-> a5
    a6 --> v6
    a6 --> v9
    a6 --> v1
    a6 --> v10
    a6 --> v7
    v0 -.-> a6
    a7 --> v11
    a7 --> v12
    v0 -.-> a7
    v1 -.-> a7
    a8 --> v8
    a8 --> v13
    a8 --> v7
    a8 --> v14
    a8 --> v12
    a8 --> v15
    a8 --> v3
    a8 --> v16
    a8 --> v17
    v0 -.-> a8
    v1 -.-> a8
    v11 -.-> a8
    v4 -.-> a8
    a9 --> v18
    v11 -.-> a9
    v8 -.-> a9
    v12 -.-> a9
    a10 --> v7
    a10 --> v14
    v0 -.-> a10
```
