<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyHAPartitionFence — structural diagrams

Generated from [`AntflyHAPartitionFence.tla`](../../../storage/ha/AntflyHAPartitionFence.tla). 9 state variables, 7 actions in `Next`.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `PartitionOldPrimary` | `partitioned` | `partitioned` |
| `RequestFence` | `fenceRequested` | `fenceRequested` |
| `DeliverFenceToOldPrimary` | `partitioned`, `fenceRequested`, `fenceDeliveredOldPrimary` | `fenceDeliveredOldPrimary`, `oldPrimaryWritable` |
| `HealPartition` | `partitioned` | `partitioned` |
| `PromoteStandby` | `fenceRequested`, `fenceDeliveredOldPrimary`, `promotedWritable` | `promotedWritable` |
| `OldPrimaryAppend` | `oldPrimaryWritable`, `promotedWritable`, `oldWrites`, `oldWritesAfterPromotion`, `nextWrite` | `oldWrites`, `oldWritesAfterPromotion`, `nextWrite` |
| `PromotedAppend` | `promotedWritable`, `promotedWrites`, `nextWrite` | `promotedWrites`, `nextWrite` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[PartitionOldPrimary]
        a1[RequestFence]
        a2[DeliverFenceToOldPrimary]
        a3[HealPartition]
        a4[PromoteStandby]
        a5[OldPrimaryAppend]
        a6[PromotedAppend]
    end
    subgraph state["State variables"]
        v0([partitioned])
        v1([fenceRequested])
        v2([fenceDeliveredOldPrimary])
        v3([oldPrimaryWritable])
        v4([promotedWritable])
        v5([oldWrites])
        v6([oldWritesAfterPromotion])
        v7([nextWrite])
        v8([promotedWrites])
    end
    a0 --> v0
    a1 --> v1
    a2 --> v2
    a2 --> v3
    v0 -.-> a2
    v1 -.-> a2
    a3 --> v0
    a4 --> v4
    v1 -.-> a4
    v2 -.-> a4
    a5 --> v5
    a5 --> v6
    a5 --> v7
    v3 -.-> a5
    v4 -.-> a5
    a6 --> v8
    a6 --> v7
    v4 -.-> a6
```
