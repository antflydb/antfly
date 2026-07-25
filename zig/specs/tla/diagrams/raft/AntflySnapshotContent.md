<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflySnapshotContent — structural diagrams

Generated from [`AntflySnapshotContent.tla`](../../raft/AntflySnapshotContent.tla). 7 state variables, 4 actions in `Next`. 1 expected-failure mutant action(s) (gated by `Buggy*` constants) omitted.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `CreateSnapshot` | `created`, `stored`, `content` | `created`, `stored`, `content` |
| `SendSnapshot` | `stored`, `target` | `target` |
| `FetchSnapshot` | `stored`, `content`, `target` | `stored`, `content`, `followerNeedsContent` |
| `ApplySnapshot` | `stored`, `content`, `target`, `applied`, `appliedContent` | `target`, `followerNeedsContent`, `applied`, `appliedContent` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[CreateSnapshot]
        a1[SendSnapshot]
        a2[FetchSnapshot]
        a3[ApplySnapshot]
    end
    subgraph state["State variables"]
        v0([created])
        v1([stored])
        v2([content])
        v3([target])
        v4([followerNeedsContent])
        v5([applied])
        v6([appliedContent])
    end
    a0 --> v0
    a0 --> v1
    a0 --> v2
    a1 --> v3
    v1 -.-> a1
    a2 --> v1
    a2 --> v2
    a2 --> v4
    v3 -.-> a2
    a3 --> v3
    a3 --> v4
    a3 --> v5
    a3 --> v6
    v1 -.-> a3
    v2 -.-> a3
```
