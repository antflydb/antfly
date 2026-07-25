<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyRaftSchedulerFairness — structural diagrams

Generated from [`AntflyRaftSchedulerFairness.tla`](../../raft/AntflyRaftSchedulerFairness.tla). 4 state variables, 2 actions in `Next`.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `Tick` | `tickCursor`, `tickGap` | `tickCursor`, `tickGap` |
| `Ready` | `readyCursor`, `readyGap` | `readyCursor`, `readyGap` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[Tick]
        a1[Ready]
    end
    subgraph state["State variables"]
        v0([tickCursor])
        v1([tickGap])
        v2([readyCursor])
        v3([readyGap])
    end
    a0 --> v0
    a0 --> v1
    a1 --> v2
    a1 --> v3
```
