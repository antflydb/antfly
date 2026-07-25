<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyHAGateTransitions — structural diagrams

Generated from [`AntflyHAGateTransitions.tla`](../../../storage/ha/AntflyHAGateTransitions.tla). 4 state variables, 5 actions in `Next`.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `role`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> primary
    primary --> standby : BecomeStandby
    primary --> former_primary : BecomeFormerPrimary
    standby --> primary : PromoteStandby
```

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `RecomputeGates` | `role`, `fenced` | `writeAllowed`, `backgroundRunning` |
| `BecomeStandby` | `role` | `role`, `writeAllowed`, `backgroundRunning` |
| `BecomeFormerPrimary` | `role` | `role`, `writeAllowed`, `backgroundRunning` |
| `FencePrimary` | `role`, `fenced` | `fenced`, `writeAllowed`, `backgroundRunning` |
| `PromoteStandby` | `role`, `fenced` | `role`, `fenced`, `writeAllowed`, `backgroundRunning` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[RecomputeGates]
        a1[BecomeStandby]
        a2[BecomeFormerPrimary]
        a3[FencePrimary]
        a4[PromoteStandby]
    end
    subgraph state["State variables"]
        v0([role])
        v1([fenced])
        v2([writeAllowed])
        v3([backgroundRunning])
    end
    a0 --> v2
    a0 --> v3
    v0 -.-> a0
    v1 -.-> a0
    a1 --> v0
    a1 --> v2
    a1 --> v3
    a2 --> v0
    a2 --> v2
    a2 --> v3
    a3 --> v1
    a3 --> v2
    a3 --> v3
    v0 -.-> a3
    a4 --> v0
    a4 --> v1
    a4 --> v2
    a4 --> v3
```
