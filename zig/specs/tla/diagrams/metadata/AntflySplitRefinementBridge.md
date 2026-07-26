<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflySplitRefinementBridge — structural diagrams

Generated from [`AntflySplitRefinementBridge.tla`](../../metadata/AntflySplitRefinementBridge.tla). 15 state variables, 13 actions in `Next`. 3 expected-failure mutant action(s) (gated by `Buggy*` constants) omitted.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `phase`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> single
    single --> splitting : BeginSplit
    splitting --> cutover : CompleteShardCutover
    cutover --> children : PublishDbChildServing
    splitting --> rolledBack : Rollback
    classDef c_single fill:#2a78d630,stroke:#2a78d6
    class single c_single
    classDef c_splitting fill:#eb683430,stroke:#eb6834
    class splitting c_splitting
    classDef c_cutover fill:#1baf7a30,stroke:#1baf7a
    class cutover c_cutover
    classDef c_children fill:#eda10030,stroke:#eda100
    class children c_children
    classDef c_rolledBack fill:#e87ba430,stroke:#e87ba4
    class rolledBack c_rolledBack
```

### `routeRightOwner`

Domain: `parent`, `child`. No statically extractable guard/update transitions.

Writes whose source state is not statically determined:

- `BeginSplit` sets `routeRightOwner` to `"parent"`
- `Rollback` sets `routeRightOwner` to `"parent"`
- `RouteMetadataToChild` sets `routeRightOwner` to `"child"`

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `BeginSplit` | `phase` | `phase`, `parentAcceptsRight`, `childAcceptsRight`, `routeRightOwner` |
| `ObserveDestinationStablePlacement` | `phase`, `placementBridge` | `placementBridge` |
| `BootstrapDestination` | `phase`, `placementBridge` | `placementBridge` |
| `ParentRightWriteDuringSplit` | `phase`, `dbDeltaSeq`, `parentAcceptsRight` | `shardCutoverReady`, `dbDeltaSeq` |
| `ReplayDelta` | `phase`, `dbDeltaSeq`, `dbReplaySeq` | `dbReplaySeq` |
| `BuildTextIndex` | `phase`, `dbReplaySeq`, `dbTextIndexSeq` | `dbTextIndexSeq` |
| `BuildSparseIndex` | `phase`, `dbReplaySeq`, `dbSparseIndexSeq` | `dbSparseIndexSeq` |
| `BuildGraphIndex` | `phase`, `dbReplaySeq`, `dbGraphIndexSeq` | `dbGraphIndexSeq` |
| `SetShardFence` | `phase`, `dbDeltaSeq`, `dbReplaySeq`, `placementBridge` | `shardFenceSet`, `shardFenceSeq` |
| `CompleteShardCutover` | `phase`, `shardFenceSet`, `dbDeltaSeq`, `dbReplaySeq` | `phase`, `shardCutoverReady` |
| `PublishDbChildServing` | `phase`, `shardCutoverReady`, `dbDeltaSeq`, `dbReplaySeq`, `dbTextIndexSeq`, `dbSparseIndexSeq`, `dbGraphIndexSeq` | `phase`, `dbChildServing`, `parentAcceptsRight`, `childAcceptsRight` |
| `RouteMetadataToChild` | `phase`, `shardCutoverReady`, `dbChildServing` | `routeRightOwner` |
| `Rollback` | `phase` | `phase`, `shardFenceSet`, `shardFenceSeq`, `shardCutoverReady`, `dbChildServing`, `parentAcceptsRight`, `childAcceptsRight`, `routeRightOwner` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[BeginSplit]
        a1[ObserveDestinationStablePlacement]
        a2[BootstrapDestination]
        a3[ParentRightWriteDuringSplit]
        a4[ReplayDelta]
        a5[BuildTextIndex]
        a6[BuildSparseIndex]
        a7[BuildGraphIndex]
        a8[SetShardFence]
        a9[CompleteShardCutover]
        a10[PublishDbChildServing]
        a11[RouteMetadataToChild]
        a12[Rollback]
    end
    subgraph state["State variables"]
        v0([phase])
        v1([parentAcceptsRight])
        v2([childAcceptsRight])
        v3([routeRightOwner])
        v4([placementBridge])
        v5([shardCutoverReady])
        v6([dbDeltaSeq])
        v7([dbReplaySeq])
        v8([dbTextIndexSeq])
        v9([dbSparseIndexSeq])
        v10([dbGraphIndexSeq])
        v11([shardFenceSet])
        v12([shardFenceSeq])
        v13([dbChildServing])
    end
    a0 --> v0
    a0 --> v1
    a0 --> v2
    a0 --> v3
    a1 --> v4
    v0 -.-> a1
    a2 --> v4
    v0 -.-> a2
    a3 --> v5
    a3 --> v6
    v0 -.-> a3
    v1 -.-> a3
    a4 --> v7
    v0 -.-> a4
    v6 -.-> a4
    a5 --> v8
    v0 -.-> a5
    v7 -.-> a5
    a6 --> v9
    v0 -.-> a6
    v7 -.-> a6
    a7 --> v10
    v0 -.-> a7
    v7 -.-> a7
    a8 --> v11
    a8 --> v12
    v0 -.-> a8
    v6 -.-> a8
    v4 -.-> a8
    a9 --> v0
    a9 --> v5
    v11 -.-> a9
    a10 --> v0
    a10 --> v13
    a10 --> v1
    a10 --> v2
    v5 -.-> a10
    a11 --> v3
    v0 -.-> a11
    v5 -.-> a11
    v13 -.-> a11
    a12 --> v0
    a12 --> v11
    a12 --> v12
    a12 --> v5
    a12 --> v13
    a12 --> v1
    a12 --> v2
    a12 --> v3
```
