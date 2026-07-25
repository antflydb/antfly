<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyShardSplitSeq — structural diagrams

Generated from [`AntflyShardSplitSeq.tla`](../../metadata/AntflyShardSplitSeq.tla). 5 state variables, 5 actions in `Next`.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `AppendFirst` | `parentLog`, `splitActive` | `parentLog` |
| `AppendSecondSameKey` | `parentLog`, `splitActive` | `parentLog` |
| `Replay` | `parentLog`, `childLog` | `childLog` |
| `SetFence` | `parentLog`, `splitActive`, `fenceSeq` | `fenceSeq` |
| `CompleteCutover` | `parentLog`, `childLog`, `splitActive`, `fenceSeq` | `splitActive`, `cutover` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[AppendFirst]
        a1[AppendSecondSameKey]
        a2[Replay]
        a3[SetFence]
        a4[CompleteCutover]
    end
    subgraph state["State variables"]
        v0([parentLog])
        v1([splitActive])
        v2([childLog])
        v3([fenceSeq])
        v4([cutover])
    end
    a0 --> v0
    v1 -.-> a0
    a1 --> v0
    v1 -.-> a1
    a2 --> v2
    v0 -.-> a2
    a3 --> v3
    v1 -.-> a3
    a4 --> v1
    a4 --> v4
    v0 -.-> a4
    v2 -.-> a4
    v3 -.-> a4
```
