<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyLsmReserveCleanup — structural diagrams

Generated from [`AntflyLsmReserveCleanup.tla`](../../../storage/lsm/AntflyLsmReserveCleanup.tla). 6 state variables, 7 actions in `Next`.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `ReserveCleanupSlot` | `reserved` | `reserved` |
| `ReserveFails` | `reserved`, `live` | — |
| `PublishLive` | `reserved`, `live` | `live`, `activeLease` |
| `RetireWithActiveLease` | `reserved`, `live`, `activeLease` | `live`, `retired` |
| `ReleaseRetiredLease` | `activeLease`, `retired` | `activeLease`, `retired` |
| `AllocateTemp` | `tempAllocated` | `tempAllocated` |
| `FailAfterTemp` | `tempAllocated` | `tempAllocated`, `leaked` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[ReserveCleanupSlot]
        a1[ReserveFails]
        a2[PublishLive]
        a3[RetireWithActiveLease]
        a4[ReleaseRetiredLease]
        a5[AllocateTemp]
        a6[FailAfterTemp]
    end
    subgraph state["State variables"]
        v0([reserved])
        v1([live])
        v2([activeLease])
        v3([retired])
        v4([tempAllocated])
        v5([leaked])
    end
    a0 --> v0
    v0 -.-> a1
    v1 -.-> a1
    a2 --> v1
    a2 --> v2
    v0 -.-> a2
    a3 --> v1
    a3 --> v3
    v0 -.-> a3
    v2 -.-> a3
    a4 --> v2
    a4 --> v3
    a5 --> v4
    a6 --> v4
    a6 --> v5
```
