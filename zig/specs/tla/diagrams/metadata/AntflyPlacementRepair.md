<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyPlacementRepair — structural diagrams

Generated from [`AntflyPlacementRepair.tla`](../../metadata/AntflyPlacementRepair.tla). 15 state variables, 10 actions in `Next`.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `phase`

```mermaid
stateDiagram-v2
    direction LR
    planned --> expanded : ApplyExpansion
    expanded --> awaiting_proof : LatchFinalMembership
    awaiting_proof --> retired : RetireSource
    classDef c_planned fill:#2a78d630,stroke:#2a78d6
    class planned c_planned
    classDef c_expanded fill:#eb683430,stroke:#eb6834
    class expanded c_expanded
    classDef c_awaiting_proof fill:#1baf7a30,stroke:#1baf7a
    class awaiting_proof c_awaiting_proof
    classDef c_retired fill:#eda10030,stroke:#eda100
    class retired c_retired
```

Writes whose source state is not statically determined:

- `PlanRepair` sets `phase` to `"planned"`

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `RepairDuplicateIds` | `phase`, `declaredId` | `phase`, `declaredId`, `duplicateRepaired` |
| `SkipDuplicateRepair` | `phase`, `declaredId` | `phase`, `duplicateRepaired` |
| `PlanRepair` | `phase`, `loadPreference`, `declaredId` | `phase`, `latchedTarget`, `planTarget` |
| `FlipLoad` | `phase` | `loadPreference` |
| `RetryRepair` | `phase`, `loadPreference` | `planTarget` |
| `ApplyExpansion` | `phase`, `planTarget`, `declaredId` | `phase`, `replicas`, `declaredId`, `expandedPeers` |
| `LatchFinalMembership` | `phase`, `plannerDesired` | `phase`, `finalPeers` |
| `PlannerChurn` | `phase`, `finalPeers` | `finalPeers`, `plannerDesired` |
| `ObserveMembership` | `phase`, `finalPeers`, `proofAccepted` | `proofAccepted`, `proofWasValid`, `proofStore` |
| `RetireSource` | `phase`, `proofAccepted` | `phase`, `sourcePresent` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[RepairDuplicateIds]
        a1[SkipDuplicateRepair]
        a2[PlanRepair]
        a3[FlipLoad]
        a4[RetryRepair]
        a5[ApplyExpansion]
        a6[LatchFinalMembership]
        a7[PlannerChurn]
        a8[ObserveMembership]
        a9[RetireSource]
    end
    subgraph state["State variables"]
        v0([phase])
        v1([declaredId])
        v2([duplicateRepaired])
        v3([latchedTarget])
        v4([planTarget])
        v5([loadPreference])
        v6([replicas])
        v7([expandedPeers])
        v8([finalPeers])
        v9([plannerDesired])
        v10([proofAccepted])
        v11([proofWasValid])
        v12([proofStore])
        v13([sourcePresent])
    end
    a0 --> v0
    a0 --> v1
    a0 --> v2
    a1 --> v0
    a1 --> v2
    v1 -.-> a1
    a2 --> v0
    a2 --> v3
    a2 --> v4
    v1 -.-> a2
    a3 --> v5
    v0 -.-> a3
    a4 --> v4
    v0 -.-> a4
    v5 -.-> a4
    a5 --> v0
    a5 --> v6
    a5 --> v1
    a5 --> v7
    v4 -.-> a5
    a6 --> v0
    a6 --> v8
    v9 -.-> a6
    a7 --> v8
    a7 --> v9
    v0 -.-> a7
    a8 --> v10
    a8 --> v11
    a8 --> v12
    v0 -.-> a8
    v8 -.-> a8
    a9 --> v0
    a9 --> v13
    v10 -.-> a9
```
