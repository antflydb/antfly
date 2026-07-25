<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyNodeDrainLifecycle — structural diagrams

Generated from [`AntflyNodeDrainLifecycle.tla`](../../metadata/AntflyNodeDrainLifecycle.tla). 8 state variables, 9 actions in `Next`.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `nodeLifecycle`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> active
    active --> draining : RequestShutdown
    draining --> active : CancelShutdown, ReRegisterNode
```

Writes whose source state is not statically determined:

- `FinalizeShutdown` sets `nodeLifecycle` to `"removed"`

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `RequestShutdown` | `nodeLifecycle` | `nodeLifecycle`, `storeDrain`, `safeReported` |
| `CancelShutdown` | `nodeLifecycle` | `nodeLifecycle`, `storeDrain`, `safeReported` |
| `ReRegisterNode` | `nodeLifecycle` | `nodeLifecycle` |
| `ComputeStatus` | `nodeLifecycle`, `placementIntent`, `groupHosted` | `safeReported` |
| `Terminate` | `safeReported`, `terminated` | `terminated` |
| `FinalizeShutdown` | `nodeLifecycle`, `storeDrain`, `finalizedWhileActive` | `nodeLifecycle`, `finalizedWhileActive` |
| `EvacuateReplica` | `nodeLifecycle`, `placementIntent`, `otherVoters` | `placementIntent` |
| `TeardownGroup` | `placementIntent`, `groupHosted` | `groupHosted` |
| `AddOtherVoter` | `otherVoters` | `otherVoters` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[RequestShutdown]
        a1[CancelShutdown]
        a2[ReRegisterNode]
        a3[ComputeStatus]
        a4[Terminate]
        a5[FinalizeShutdown]
        a6[EvacuateReplica]
        a7[TeardownGroup]
        a8[AddOtherVoter]
    end
    subgraph state["State variables"]
        v0([nodeLifecycle])
        v1([storeDrain])
        v2([safeReported])
        v3([terminated])
        v4([finalizedWhileActive])
        v5([placementIntent])
        v6([otherVoters])
        v7([groupHosted])
    end
    a0 --> v0
    a0 --> v1
    a0 --> v2
    a1 --> v0
    a1 --> v1
    a1 --> v2
    a2 --> v0
    a3 --> v2
    v0 -.-> a3
    a4 --> v3
    v2 -.-> a4
    a5 --> v0
    a5 --> v4
    v1 -.-> a5
    a6 --> v5
    v0 -.-> a6
    v6 -.-> a6
    a7 --> v7
    v5 -.-> a7
    a8 --> v6
```
