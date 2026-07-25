<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyPlacementReadiness — structural diagrams

Generated from [`AntflyPlacementReadiness.tla`](../../metadata/AntflyPlacementReadiness.tla). 13 state variables, 7 actions in `Next`.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `PublishLeaderReport` | `reportPresent`, `reportKnowsVoterSet`, `reportCount`, `leaderKnown`, `leaderPlaced`, `healthyReports` | `reportPresent`, `reportKnowsVoterSet`, `reportCount`, `leaderKnown`, `leaderPlaced`, `healthyReports` |
| `PublishUnknownFollowerConflict` | `reportPresent`, `reportKnowsVoterSet`, `reportCount` | `reportPresent`, `reportKnowsVoterSet`, `reportCount` |
| `PublishConvergedFollowerReport` | `reportPresent`, `reportKnowsVoterSet`, `reportCount` | `reportPresent`, `reportKnowsVoterSet`, `reportCount` |
| `SetJointConsensus` | `jointConsensus` | `jointConsensus` |
| `ClearJointConsensus` | `jointConsensus` | `jointConsensus` |
| `RecomputePlacementEvidence` | `reportPresent`, `reportKnowsVoterSet`, `reportCount`, `observedVoterCount`, `voterCountKnown`, `ambiguousVoterCount`, `unknownReportLatchedConflict` | `observedVoterCount`, `voterCountKnown`, `ambiguousVoterCount`, `unknownReportLatchedConflict` |
| `StartTransition` | `leaderKnown`, `leaderPlaced`, `healthyReports`, `jointConsensus`, `observedVoterCount`, `voterCountKnown`, `ambiguousVoterCount`, `transitionStarted`, `transitionStartedWithoutStablePlacement` | `transitionStarted`, `transitionStartedWithoutStablePlacement` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[PublishLeaderReport]
        a1[PublishUnknownFollowerConflict]
        a2[PublishConvergedFollowerReport]
        a3[SetJointConsensus]
        a4[ClearJointConsensus]
        a5[RecomputePlacementEvidence]
        a6[StartTransition]
    end
    subgraph state["State variables"]
        v0([reportPresent])
        v1([reportKnowsVoterSet])
        v2([reportCount])
        v3([leaderKnown])
        v4([leaderPlaced])
        v5([healthyReports])
        v6([jointConsensus])
        v7([observedVoterCount])
        v8([voterCountKnown])
        v9([ambiguousVoterCount])
        v10([unknownReportLatchedConflict])
        v11([transitionStarted])
        v12([transitionStartedWithoutStablePlacement])
    end
    a0 --> v0
    a0 --> v1
    a0 --> v2
    a0 --> v3
    a0 --> v4
    a0 --> v5
    a1 --> v0
    a1 --> v1
    a1 --> v2
    a2 --> v0
    a2 --> v1
    a2 --> v2
    a3 --> v6
    a4 --> v6
    a5 --> v7
    a5 --> v8
    a5 --> v9
    a5 --> v10
    v0 -.-> a5
    v1 -.-> a5
    v2 -.-> a5
    a6 --> v11
    a6 --> v12
    v3 -.-> a6
    v4 -.-> a6
```
