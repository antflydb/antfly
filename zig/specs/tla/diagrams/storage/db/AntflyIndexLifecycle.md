<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyIndexLifecycle — structural diagrams

Generated from [`AntflyIndexLifecycle.tla`](../../../storage/db/AntflyIndexLifecycle.tla). 11 state variables, 11 actions in `Next`.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `state`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> stale
    failed --> building : StartBuild
    stale --> building : StartBuild
    building --> fresh : Swap
    building --> failed : FailBuild
    fresh --> stale : RequestSecondSchema
```

Writes whose source state is not statically determined:

- `CrashReopen` sets `state` to `"fresh"`
- `CrashReopen` sets `state` to `"stale"`
- `Write` sets `state` to `"building"`
- `Write` sets `state` to `"fresh"`

### `statusDurable`

Domain: `stale`, `building`, `fresh`, `failed`. No statically extractable guard/update transitions.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `Write` | `state`, `target`, `wakeQueued`, `workerAdmitted` | `state`, `target`, `wakeQueued`, `workerAdmitted` |
| `RunCompetingWork` | `competingWork` | `competingWork` |
| `AdmitIndexWorker` | `wakeQueued`, `workerAdmitted`, `competingWork` | `wakeQueued`, `workerAdmitted` |
| `StartBuild` | `state`, `workerAdmitted` | `state` |
| `BuildStep` | `state`, `applied`, `target`, `workerAdmitted` | `applied` |
| `Swap` | `state`, `applied`, `target`, `requestedSchema` | `state`, `builtSchema`, `workerAdmitted` |
| `FailBuild` | `state` | `state`, `wakeQueued`, `workerAdmitted` |
| `RequestSecondSchema` | `state`, `target`, `requestedSchema`, `builtSchema` | `state`, `applied`, `requestedSchema`, `wakeQueued`, `workerAdmitted`, `competingWork`, `secondWakeLost` |
| `PersistStatus` | `state`, `statusDurable` | `statusDurable` |
| `CrashReopen` | `applied`, `target`, `statusDurable`, `requestedSchema`, `builtSchema`, `wakeQueued` | `state`, `wakeQueued`, `workerAdmitted` |
| `Query` | `state`, `applied`, `target`, `servedFreshBehind` | `servedFreshBehind` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[Write]
        a1[RunCompetingWork]
        a2[AdmitIndexWorker]
        a3[StartBuild]
        a4[BuildStep]
        a5[Swap]
        a6[FailBuild]
        a7[RequestSecondSchema]
        a8[PersistStatus]
        a9[CrashReopen]
        a10[Query]
    end
    subgraph state["State variables"]
        v0([state])
        v1([target])
        v2([wakeQueued])
        v3([workerAdmitted])
        v4([competingWork])
        v5([applied])
        v6([requestedSchema])
        v7([builtSchema])
        v8([secondWakeLost])
        v9([statusDurable])
        v10([servedFreshBehind])
    end
    a0 --> v0
    a0 --> v1
    a0 --> v2
    a0 --> v3
    a1 --> v4
    a2 --> v2
    a2 --> v3
    v4 -.-> a2
    a3 --> v0
    v3 -.-> a3
    a4 --> v5
    v0 -.-> a4
    v1 -.-> a4
    v3 -.-> a4
    a5 --> v0
    a5 --> v7
    a5 --> v3
    v5 -.-> a5
    v1 -.-> a5
    v6 -.-> a5
    a6 --> v0
    a6 --> v2
    a6 --> v3
    a7 --> v0
    a7 --> v5
    a7 --> v6
    a7 --> v2
    a7 --> v3
    a7 --> v4
    a7 --> v8
    v1 -.-> a7
    v7 -.-> a7
    a8 --> v9
    v0 -.-> a8
    a9 --> v0
    a9 --> v2
    a9 --> v3
    v5 -.-> a9
    v1 -.-> a9
    v9 -.-> a9
    v6 -.-> a9
    v7 -.-> a9
    a10 --> v10
    v0 -.-> a10
    v5 -.-> a10
    v1 -.-> a10
```
