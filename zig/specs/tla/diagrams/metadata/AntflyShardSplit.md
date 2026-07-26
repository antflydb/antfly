<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyShardSplit — structural diagrams

Generated from [`AntflyShardSplit.tla`](../../metadata/AntflyShardSplit.tla). 14 state variables, 20 actions in `Next`. 1 expected-failure mutant action(s) (gated by `Buggy*` constants) omitted.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `splitPhase`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> none
    none --> prepare : PrepareSplit
    prepare --> splitting : SetSplittingPhase
    splitting --> none : FinalizeSplitComplete, TimeoutRollback
    prepare --> none : TimeoutRollback
    classDef c_none fill:#2a78d630,stroke:#2a78d6
    class none c_none
    classDef c_prepare fill:#eb683430,stroke:#eb6834
    class prepare c_prepare
    classDef c_splitting fill:#1baf7a30,stroke:#1baf7a
    class splitting c_splitting
```

### `newShardState`

```mermaid
stateDiagram-v2
    direction LR
    state "default" as s_default
    [*] --> none
    none --> splittingOff : StartNewShard
    preSnap --> preSnap : NewShardReceivesSnapshot
    splittingOff --> preSnap : NewShardReceivesSnapshot
    preSnap --> s_default : TablemgrTransitionsChild
    splittingOff --> s_default : TablemgrTransitionsChild
    classDef c_none fill:#2a78d630,stroke:#2a78d6
    class none c_none
    classDef c_splittingOff fill:#eb683430,stroke:#eb6834
    class splittingOff c_splittingOff
    classDef c_preSnap fill:#1baf7a30,stroke:#1baf7a
    class preSnap c_preSnap
    classDef c_s_default fill:#eda10030,stroke:#eda100
    class s_default c_s_default
```

Writes whose source state is not statically determined:

- `TimeoutRollback` sets `newShardState` to `"none"`

### `dataStore`

Domain: `parent`, `child`, `both`. No statically extractable guard/update transitions.

Writes whose source state is not statically determined:

- `ClientWriteDeltaToParent` sets `dataStore` to `"both"`
- `ClientWriteDeltaToParent` sets `dataStore` to `"child"`
- `ClientWriteToChild` sets `dataStore` to `"both"`
- `ClientWriteToChild` sets `dataStore` to `"parent"`
- `ClientWriteToParent` sets `dataStore` to `"both"`
- `ClientWriteToParent` sets `dataStore` to `"child"`
- `FinalizeSplitComplete` sets `dataStore` to `"child"`
- `NewShardReceivesSnapshot` sets `dataStore` to `"both"`
- `NewShardReceivesSnapshot` sets `dataStore` to `"parent"`
- `TimeoutRollback` sets `dataStore` to `"parent"`

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `PrepareSplit` | `splitPhase`, `parentHasLeader`, `newShardState` | `splitPhase`, `parentDeltaKeys`, `childReplayedKeys`, `splitCutoverReady`, `splitFenceSet` |
| `SetSplittingPhase` | `splitPhase`, `parentHasLeader` | `splitPhase` |
| `ApplySplitOp` | `splitPhase`, `archiveCreated`, `parentHasLeader` | `parentRange`, `archiveCreated` |
| `MetadataUpdateRouting` | `splitPhase`, `archiveCreated`, `routingUpdated` | `routingUpdated` |
| `StartNewShard` | `archiveCreated`, `newShardState`, `routingUpdated` | `newShardState`, `newShardInitializing` |
| `NewShardReceivesSnapshot` | `archiveCreated`, `newShardState`, `newShardHasSnapshot`, `dataStore` | `newShardState`, `newShardHasSnapshot`, `dataStore` |
| `ChildClearsInitializing` | `newShardHasSnapshot`, `newShardInitializing`, `newShardHasLeader`, `parentDeltaKeys`, `childReplayedKeys` | `newShardInitializing` |
| `TablemgrTransitionsChild` | `newShardState`, `newShardHasSnapshot`, `newShardInitializing`, `newShardHasLeader`, `splitCutoverReady` | `newShardState` |
| `FinalizeSplitSetFence` | `splitPhase`, `archiveCreated`, `parentHasLeader`, `newShardHasSnapshot`, `newShardInitializing`, `newShardHasLeader`, `parentDeltaKeys`, `childReplayedKeys` | `splitFenceSet` |
| `FinalizeSplitComplete` | `splitPhase`, `parentHasLeader`, `dataStore`, `parentDeltaKeys`, `childReplayedKeys`, `splitFenceSet` | `splitPhase`, `dataStore`, `parentDeltaKeys`, `childReplayedKeys`, `splitCutoverReady`, `splitFenceSet` |
| `TimeoutRollback` | `splitPhase`, `parentHasLeader` | `splitPhase`, `parentRange`, `archiveCreated`, `newShardState`, `newShardHasSnapshot`, `newShardInitializing`, `newShardHasLeader`, `routingUpdated`, `dataStore`, `parentDeltaKeys`, `childReplayedKeys`, `splitCutoverReady`, `splitFenceSet` |
| `ParentLosesLeader` | `parentHasLeader` | `parentHasLeader` |
| `ParentGainsLeader` | `parentHasLeader` | `parentHasLeader` |
| `NewShardLosesLeader` | `newShardHasLeader` | `newShardHasLeader` |
| `NewShardGainsLeader` | `newShardState`, `newShardHasLeader` | `newShardHasLeader` |
| `LeaderSynchronization` | `parentHasLeader`, `newShardState`, `newShardHasLeader` | `parentHasLeader`, `newShardHasLeader` |
| `ClientWriteToParent` | `splitPhase`, `parentRange`, `parentHasLeader`, `dataStore` | `dataStore` |
| `ClientWriteDeltaToParent` | `splitPhase`, `parentHasLeader`, `newShardState`, `newShardHasSnapshot`, `newShardInitializing`, `newShardHasLeader`, `routingUpdated`, `dataStore`, `parentDeltaKeys`, `splitCutoverReady` | `dataStore`, `parentDeltaKeys` |
| `ClientWriteToChild` | `newShardState`, `newShardHasSnapshot`, `newShardInitializing`, `newShardHasLeader`, `routingUpdated`, `dataStore`, `splitCutoverReady` | `dataStore` |
| `ChildReplaysDelta` | `newShardHasSnapshot`, `newShardHasLeader`, `parentDeltaKeys`, `childReplayedKeys` | `childReplayedKeys` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[PrepareSplit]
        a1[SetSplittingPhase]
        a2[ApplySplitOp]
        a3[MetadataUpdateRouting]
        a4[StartNewShard]
        a5[NewShardReceivesSnapshot]
        a6[ChildClearsInitializing]
        a7[TablemgrTransitionsChild]
        a8[FinalizeSplitSetFence]
        a9[FinalizeSplitComplete]
        a10[TimeoutRollback]
        a11[ParentLosesLeader]
        a12[ParentGainsLeader]
        a13[NewShardLosesLeader]
        a14[NewShardGainsLeader]
        a15[LeaderSynchronization]
        a16[ClientWriteToParent]
        a17[ClientWriteDeltaToParent]
        a18[ClientWriteToChild]
        a19[ChildReplaysDelta]
    end
    subgraph state["State variables"]
        v0([splitPhase])
        v1([parentHasLeader])
        v2([newShardState])
        v3([parentDeltaKeys])
        v4([childReplayedKeys])
        v5([splitCutoverReady])
        v6([splitFenceSet])
        v7([parentRange])
        v8([archiveCreated])
        v9([routingUpdated])
        v10([newShardInitializing])
        v11([newShardHasSnapshot])
        v12([dataStore])
        v13([newShardHasLeader])
    end
    a0 --> v0
    a0 --> v3
    a0 --> v4
    a0 --> v5
    a0 --> v6
    v1 -.-> a0
    v2 -.-> a0
    a1 --> v0
    v1 -.-> a1
    a2 --> v7
    a2 --> v8
    v0 -.-> a2
    v1 -.-> a2
    a3 --> v9
    v0 -.-> a3
    v8 -.-> a3
    a4 --> v2
    a4 --> v10
    v8 -.-> a4
    v9 -.-> a4
    a5 --> v2
    a5 --> v11
    a5 --> v12
    v8 -.-> a5
    a6 --> v10
    v11 -.-> a6
    v13 -.-> a6
    a7 --> v2
    v11 -.-> a7
    v10 -.-> a7
    v13 -.-> a7
    v5 -.-> a7
    a8 --> v6
    v0 -.-> a8
    v8 -.-> a8
    v1 -.-> a8
    a9 --> v0
    a9 --> v12
    a9 --> v3
    a9 --> v4
    a9 --> v5
    a9 --> v6
    v1 -.-> a9
    a10 --> v0
    a10 --> v7
    a10 --> v8
    a10 --> v2
    a10 --> v11
    a10 --> v10
    a10 --> v13
    a10 --> v9
    a10 --> v12
    a10 --> v3
    a10 --> v4
    a10 --> v5
    a10 --> v6
    v1 -.-> a10
    a11 --> v1
    a12 --> v1
    a13 --> v13
    a14 --> v13
    v2 -.-> a14
    a15 --> v1
    a15 --> v13
    v2 -.-> a15
    a16 --> v12
    v1 -.-> a16
    a17 --> v12
    a17 --> v3
    a18 --> v12
    a19 --> v4
    v11 -.-> a19
    v13 -.-> a19
    v3 -.-> a19
```
