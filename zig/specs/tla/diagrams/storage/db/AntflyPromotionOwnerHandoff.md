<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyPromotionOwnerHandoff — structural diagrams

Generated from [`AntflyPromotionOwnerHandoff.tla`](../../../storage/db/AntflyPromotionOwnerHandoff.tla). 5 state variables, 6 actions in `Next`.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `rangeOwner`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> old
    old --> new : TransferRange
```

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `StartHandoffDetachOld` | `attached`, `handoffStarted` | `attached`, `handoffStarted` |
| `TransferRange` | `rangeOwner`, `handoffStarted` | `rangeOwner` |
| `AttachNew` | `rangeOwner`, `attached`, `handoffStarted` | `attached` |
| `CrashSide` | `attached` | `attached` |
| `ReattachAfterRecovery` | `rangeOwner`, `attached`, `handoffStarted` | `attached` |
| `Promote` | `rangeOwner`, `attached`, `promotedBy`, `unownedPromotion` | `promotedBy`, `unownedPromotion` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[StartHandoffDetachOld]
        a1[TransferRange]
        a2[AttachNew]
        a3[CrashSide]
        a4[ReattachAfterRecovery]
        a5[Promote]
    end
    subgraph state["State variables"]
        v0([attached])
        v1([handoffStarted])
        v2([rangeOwner])
        v3([promotedBy])
        v4([unownedPromotion])
    end
    a0 --> v0
    a0 --> v1
    a1 --> v2
    v1 -.-> a1
    a2 --> v0
    v2 -.-> a2
    v1 -.-> a2
    a3 --> v0
    a4 --> v0
    v2 -.-> a4
    v1 -.-> a4
    a5 --> v3
    a5 --> v4
    v2 -.-> a5
    v0 -.-> a5
```
