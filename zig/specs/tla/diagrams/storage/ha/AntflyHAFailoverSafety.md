<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyHAFailoverSafety — structural diagrams

Generated from [`AntflyHAFailoverSafety.tla`](../../../storage/ha/AntflyHAFailoverSafety.tla). 14 state variables, 6 actions in `Next`. 1 expected-failure mutant action(s) (gated by `Buggy*` constants) omitted.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `AppendOldPrimary` | `primaryLsn`, `oldPrimaryWritable`, `oldHas` | `primaryLsn`, `oldHas` |
| `PromotedAppend` | `primaryLsn`, `promotedNode`, `promotedWritable`, `standbyHas` | `primaryLsn`, `standbyHas` |
| `AckAsyncPrimary` | `promotedNode`, `oldHas`, `syncAcked`, `asyncAcked` | `asyncAcked` |
| `FenceAndPromote` | `promotedNode`, `oldTimeline`, `standbyHas`, `syncAcked` | `oldPrimaryWritable`, `promotedNode`, `promotedWritable`, `newTimeline`, `fenceHeld`, `fenceOwner` |
| `ReplicateToStandby` | `promotedNode`, `oldHas`, `standbyHas` | `standbyHas` |
| `AckSyncFromStandby` | `promotedNode`, `oldHas`, `standbyHas`, `syncAcked`, `asyncAcked`, `ackSource` | `syncAcked`, `ackSource` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[AppendOldPrimary]
        a1[PromotedAppend]
        a2[AckAsyncPrimary]
        a3[FenceAndPromote]
        a4[ReplicateToStandby]
        a5[AckSyncFromStandby]
    end
    subgraph state["State variables"]
        v0([primaryLsn])
        v1([oldPrimaryWritable])
        v2([oldHas])
        v3([promotedNode])
        v4([promotedWritable])
        v5([standbyHas])
        v6([syncAcked])
        v7([asyncAcked])
        v8([oldTimeline])
        v9([newTimeline])
        v10([fenceHeld])
        v11([fenceOwner])
        v12([ackSource])
    end
    a0 --> v0
    a0 --> v2
    v1 -.-> a0
    a1 --> v0
    a1 --> v5
    v3 -.-> a1
    v4 -.-> a1
    a2 --> v7
    v3 -.-> a2
    v2 -.-> a2
    v6 -.-> a2
    a3 --> v1
    a3 --> v3
    a3 --> v4
    a3 --> v9
    a3 --> v10
    a3 --> v11
    v8 -.-> a3
    v5 -.-> a3
    v6 -.-> a3
    a4 --> v5
    v3 -.-> a4
    v2 -.-> a4
    a5 --> v6
    a5 --> v12
    v3 -.-> a5
    v2 -.-> a5
    v5 -.-> a5
    v7 -.-> a5
```
