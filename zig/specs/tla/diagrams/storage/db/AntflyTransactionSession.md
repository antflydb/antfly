<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyTransactionSession — structural diagrams

Generated from [`AntflyTransactionSession.tla`](../../../storage/db/AntflyTransactionSession.tla). 13 state variables, 15 actions in `Next`. 3 expected-failure mutant action(s) (gated by `Buggy*` constants) omitted.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `status`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> idle
    idle --> open : BeginSession
    open --> aborted : DetectConflict, Abort, RecoverStalePending, CrashFinalizeAbortedOrphan
    prepared --> committed : Commit, CrashFinalizeCommittedOrphan
    prepared --> aborted : Abort, CrashFinalizeAbortedOrphan
    aborted --> idle : Cleanup
    committed --> idle : Cleanup
    open --> prepared : PrepareParticipant
```

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `BeginSession` | `status`, `visible` | `status`, `baseVisible`, `staged`, `intentCount`, `savepoint`, `participantPrepared`, `participantResolved`, `conflict`, `stalePending`, `rollbackDiscarded` |
| `StageWrite` | `status`, `baseVisible`, `staged`, `intentCount` | `staged`, `intentCount` |
| `CreateSavepoint` | `status`, `staged` | `savepoint` |
| `RollbackToSavepoint` | `status`, `staged`, `savepoint` | `staged`, `intentCount`, `rollbackDiscarded` |
| `DetectConflict` | `status` | `status`, `intentCount`, `conflict` |
| `Commit` | `status`, `baseVisible`, `intentCount` | `status`, `intentCount`, `visible`, `identityRows`, `stalePending` |
| `Abort` | `status`, `baseVisible` | `status`, `intentCount`, `visible`, `identityRows`, `stalePending` |
| `MarkStalePending` | `status` | `stalePending` |
| `RecoverStalePending` | `status`, `baseVisible`, `stalePending` | `status`, `intentCount`, `visible`, `identityRows`, `stalePending` |
| `CrashFinalizeCommittedOrphan` | `status`, `intentCount` | `status` |
| `CrashFinalizeAbortedOrphan` | `status`, `intentCount` | `status`, `stalePending` |
| `RecoverFinalizedIntents` | `status`, `baseVisible`, `intentCount` | `intentCount`, `visible`, `identityRows` |
| `Cleanup` | `status`, `intentCount`, `participantPrepared`, `participantResolved`, `visible` | `status`, `baseVisible`, `staged`, `savepoint`, `participantPrepared`, `participantResolved`, `conflict`, `stalePending`, `rollbackDiscarded` |
| `PrepareParticipant` | `status`, `intentCount`, `participantPrepared`, `conflict` | `status`, `participantPrepared` |
| `ResolveParticipant` | `status`, `participantPrepared`, `participantResolved` | `participantResolved` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[BeginSession]
        a1[StageWrite]
        a2[CreateSavepoint]
        a3[RollbackToSavepoint]
        a4[DetectConflict]
        a5[Commit]
        a6[Abort]
        a7[MarkStalePending]
        a8[RecoverStalePending]
        a9[CrashFinalizeCommittedOrphan]
        a10[CrashFinalizeAbortedOrphan]
        a11[RecoverFinalizedIntents]
        a12[Cleanup]
        a13[PrepareParticipant]
        a14[ResolveParticipant]
    end
    subgraph state["State variables"]
        v0([status])
        v1([baseVisible])
        v2([staged])
        v3([intentCount])
        v4([savepoint])
        v5([participantPrepared])
        v6([participantResolved])
        v7([visible])
        v8([conflict])
        v9([stalePending])
        v10([rollbackDiscarded])
        v11([identityRows])
    end
    a0 --> v0
    a0 --> v1
    a0 --> v2
    a0 --> v3
    a0 --> v4
    a0 --> v5
    a0 --> v6
    a0 --> v8
    a0 --> v9
    a0 --> v10
    v7 -.-> a0
    a1 --> v2
    a1 --> v3
    v0 -.-> a1
    v1 -.-> a1
    a2 --> v4
    v0 -.-> a2
    v2 -.-> a2
    a3 --> v2
    a3 --> v3
    a3 --> v10
    v0 -.-> a3
    v4 -.-> a3
    a4 --> v0
    a4 --> v3
    a4 --> v8
    a5 --> v0
    a5 --> v3
    a5 --> v7
    a5 --> v11
    a5 --> v9
    v1 -.-> a5
    a6 --> v0
    a6 --> v3
    a6 --> v7
    a6 --> v11
    a6 --> v9
    v1 -.-> a6
    a7 --> v9
    v0 -.-> a7
    a8 --> v0
    a8 --> v3
    a8 --> v7
    a8 --> v11
    a8 --> v9
    v1 -.-> a8
    a9 --> v0
    v3 -.-> a9
    a10 --> v0
    a10 --> v9
    v3 -.-> a10
    a11 --> v3
    a11 --> v7
    a11 --> v11
    v0 -.-> a11
    v1 -.-> a11
    a12 --> v0
    a12 --> v1
    a12 --> v2
    a12 --> v4
    a12 --> v5
    a12 --> v6
    a12 --> v8
    a12 --> v9
    a12 --> v10
    v3 -.-> a12
    v7 -.-> a12
    a13 --> v0
    a13 --> v5
    v3 -.-> a13
    v8 -.-> a13
    a14 --> v6
    v0 -.-> a14
    v5 -.-> a14
```
