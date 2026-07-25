<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyHASyncWait — structural diagrams

Generated from [`AntflyHASyncWait.tla`](../../../storage/ha/AntflyHASyncWait.tla). 15 state variables, 6 actions in `Next`.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `AppendWrite` | `primaryLsn` | `primaryLsn` |
| `FreezeSyncTarget` | `currentTimeline`, `primaryLsn`, `targetFrozen` | `targetFrozen`, `syncTargetTimeline`, `syncTargetLsn`, `frozenTimeline`, `frozenLsn` |
| `PromoteNewTimeline` | `currentTimeline`, `targetFrozen` | `currentTimeline`, `primaryLsn`, `syncTargetTimeline`, `syncTargetLsn` |
| `SlotJoinsCurrentTimeline` | `currentTimeline`, `slotTimeline`, `appliedLsn` | `slotTimeline`, `appliedLsn` |
| `ApplyOnSlot` | `appliedLsn` | `appliedLsn` |
| `ReportAck` | `targetFrozen`, `syncTargetTimeline`, `syncTargetLsn`, `slotTimeline`, `appliedLsn`, `acked` | `acked`, `ackSlot`, `ackSourceTimeline`, `ackSourceLsn`, `ackTimeline`, `ackLsn` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[AppendWrite]
        a1[FreezeSyncTarget]
        a2[PromoteNewTimeline]
        a3[SlotJoinsCurrentTimeline]
        a4[ApplyOnSlot]
        a5[ReportAck]
    end
    subgraph state["State variables"]
        v0([primaryLsn])
        v1([currentTimeline])
        v2([targetFrozen])
        v3([syncTargetTimeline])
        v4([syncTargetLsn])
        v5([frozenTimeline])
        v6([frozenLsn])
        v7([slotTimeline])
        v8([appliedLsn])
        v9([acked])
        v10([ackSlot])
        v11([ackSourceTimeline])
        v12([ackSourceLsn])
        v13([ackTimeline])
        v14([ackLsn])
    end
    a0 --> v0
    a1 --> v2
    a1 --> v3
    a1 --> v4
    a1 --> v5
    a1 --> v6
    v1 -.-> a1
    v0 -.-> a1
    a2 --> v1
    a2 --> v0
    a2 --> v3
    a2 --> v4
    v2 -.-> a2
    a3 --> v7
    a3 --> v8
    v1 -.-> a3
    a4 --> v8
    a5 --> v9
    a5 --> v10
    a5 --> v11
    a5 --> v12
    a5 --> v13
    a5 --> v14
    v2 -.-> a5
    v3 -.-> a5
    v4 -.-> a5
    v7 -.-> a5
    v8 -.-> a5
```
