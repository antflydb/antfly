<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyLiveSourceCardinality — structural diagrams

Generated from [`AntflyLiveSourceCardinality.tla`](../../../storage/db/AntflyLiveSourceCardinality.tla). 8 state variables, 3 actions in `Next`.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `BindStableOwner` | `durableSourceCount`, `dbCachedSourceCount`, `ownerBound` | `dbCachedSourceCount`, `ownerBound`, `ownerTargetsLiveDb` |
| `CommitTtlDelete` | `dbCachedSourceCount`, `ownerBound`, `ownerTargetsLiveDb`, `deleteCommitted` | `durableSourceCount`, `dbCachedSourceCount`, `deleteCommitted` |
| `PublishLiveStatus` | `dbCachedSourceCount`, `visibleSourceCount`, `deleteCommitted`, `statusPublished`, `publishParity` | `visibleSourceCount`, `statusPublished`, `publishParity` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[BindStableOwner]
        a1[CommitTtlDelete]
        a2[PublishLiveStatus]
    end
    subgraph state["State variables"]
        v0([durableSourceCount])
        v1([dbCachedSourceCount])
        v2([ownerBound])
        v3([ownerTargetsLiveDb])
        v4([deleteCommitted])
        v5([visibleSourceCount])
        v6([statusPublished])
        v7([publishParity])
    end
    a0 --> v1
    a0 --> v2
    a0 --> v3
    v0 -.-> a0
    a1 --> v0
    a1 --> v1
    a1 --> v4
    v2 -.-> a1
    v3 -.-> a1
    a2 --> v5
    a2 --> v6
    a2 --> v7
    v1 -.-> a2
    v4 -.-> a2
```
