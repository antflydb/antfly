<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyQueryCompleteness — structural diagrams

Generated from [`AntflyQueryCompleteness.tla`](../../metadata/AntflyQueryCompleteness.tla). 7 state variables, 5 actions in `Next`.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `CopyRightDocToChild` | `childHasRight` | `childHasRight` |
| `PublishChildServing` | `childHasRight`, `childServing` | `childServing` |
| `PublishRouteToChild` | `childServing`, `routeToChild` | `parentRightOwned`, `routeToChild`, `parentStillScansRight` |
| `FinishParentTrim` | `routeToChild`, `parentStillScansRight` | `parentStillScansRight` |
| `RunQuery` | `parentRightOwned`, `childHasRight`, `childServing`, `routeToChild`, `parentStillScansRight`, `queryRan` | `queryRan`, `resultCount` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[CopyRightDocToChild]
        a1[PublishChildServing]
        a2[PublishRouteToChild]
        a3[FinishParentTrim]
        a4[RunQuery]
    end
    subgraph state["State variables"]
        v0([childHasRight])
        v1([childServing])
        v2([parentRightOwned])
        v3([routeToChild])
        v4([parentStillScansRight])
        v5([queryRan])
        v6([resultCount])
    end
    a0 --> v0
    a1 --> v1
    v0 -.-> a1
    a2 --> v2
    a2 --> v3
    a2 --> v4
    v1 -.-> a2
    a3 --> v4
    v3 -.-> a3
    a4 --> v5
    a4 --> v6
```
