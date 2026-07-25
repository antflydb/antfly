<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyDbSplitVisibility — structural diagrams

Generated from [`AntflyDbSplitVisibility.tla`](../../../storage/db/AntflyDbSplitVisibility.tla). 30 state variables, 15 actions in `Next`. 4 expected-failure mutant action(s) (gated by `Buggy*` constants) omitted.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `phase`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> single
    single --> prepared : PrepareSplit
    prepared --> replaying : ParentRightWriteDuringSplit, ReplaySplitDelta
    replaying --> replaying : ParentRightWriteDuringSplit, ReplaySplitDelta
    prepared --> children : FinalizeSplit
    replaying --> children : FinalizeSplit
    single --> mergePrepared : StartMerge
    mergePrepared --> merged : FinalizeMerge
```

### `childArtifactPlacement`

Domain: `none`, `local`, `remote`. No statically extractable guard/update transitions.

Writes whose source state is not statically determined:

- `FinalizeSplit` sets `childArtifactPlacement` to `"remote"`
- `PrepareSplit` sets `childArtifactPlacement` to `"local"`

### `rightRouteOwner`

Domain: `none`, `parent`, `child`, `donor`, `receiver`. No statically extractable guard/update transitions.

Writes whose source state is not statically determined:

- `FinalizeMerge` sets `rightRouteOwner` to `"receiver"`
- `FinalizeSplit` sets `rightRouteOwner` to `"child"`
- `PrepareSplit` sets `rightRouteOwner` to `"parent"`
- `StartMerge` sets `rightRouteOwner` to `"donor"`

### `enrichmentOwner`

Domain: `none`, `parent`, `child`, `donor`, `receiver`. No statically extractable guard/update transitions.

Writes whose source state is not statically determined:

- `ChildRightWrite` sets `enrichmentOwner` to `"none"`
- `DonorRightWriteBeforeMerge` sets `enrichmentOwner` to `"none"`
- `FinalizeMerge` sets `enrichmentOwner` to `"none"`
- `FinalizeSplit` sets `enrichmentOwner` to `"none"`
- `ParentRightWriteDuringSplit` sets `enrichmentOwner` to `"none"`
- `PrepareSplit` sets `enrichmentOwner` to `"none"`
- `ReceiverRightWrite` sets `enrichmentOwner` to `"none"`
- `StartMerge` sets `enrichmentOwner` to `"none"`

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `ParentLeftWrite` | `leftSeq`, `parentOwnsLeft` | `leftSeq` |
| `ParentRightWriteBeforeSplit` | `phase`, `parentRightSeq`, `parentOwnsRight`, `parentAcceptsRight` | `parentRightSeq` |
| `PrepareSplit` | `phase`, `parentRightSeq` | `phase`, `splitSnapshotSeq`, `splitDeltaSeq`, `childReplaySeq`, `childRightSeq`, `childTextIndexSeq`, `childSparseIndexSeq`, `childGraphIndexSeq`, `childArtifactPlacement`, `rightRouteOwner`, `enrichmentOwner` |
| `ParentRightWriteDuringSplit` | `phase`, `parentRightSeq`, `splitDeltaSeq`, `parentOwnsRight`, `parentAcceptsRight` | `phase`, `parentRightSeq`, `splitDeltaSeq`, `enrichmentOwner` |
| `ReplaySplitDelta` | `phase`, `splitSnapshotSeq`, `splitDeltaSeq`, `childReplaySeq` | `phase`, `childReplaySeq`, `childRightSeq` |
| `BuildChildTextIndex` | `phase`, `childRightSeq`, `childTextIndexSeq` | `childTextIndexSeq` |
| `BuildChildSparseIndex` | `phase`, `childRightSeq`, `childSparseIndexSeq` | `childSparseIndexSeq` |
| `BuildChildGraphIndex` | `phase`, `childRightSeq`, `childGraphIndexSeq` | `childGraphIndexSeq` |
| `FinalizeSplit` | `phase`, `parentRightSeq`, `splitDeltaSeq`, `childReplaySeq`, `childRightSeq`, `childTextIndexSeq`, `childSparseIndexSeq`, `childGraphIndexSeq` | `phase`, `childArtifactPlacement`, `childServing`, `parentOwnsRight`, `childOwnsRight`, `parentAcceptsRight`, `childAcceptsRight`, `rightRouteOwner`, `enrichmentOwner` |
| `ChildRightWrite` | `phase`, `parentRightSeq`, `childRightSeq`, `childOwnsRight`, `childAcceptsRight` | `parentRightSeq`, `childRightSeq`, `childTextIndexSeq`, `childSparseIndexSeq`, `childGraphIndexSeq`, `enrichmentOwner` |
| `StartMerge` | `phase`, `parentRightSeq` | `phase`, `parentOwnsRight`, `donorOwnsRight`, `receiverOwnsRight`, `parentAcceptsRight`, `donorAcceptsRight`, `receiverAcceptsRight`, `donorRightSeq`, `receiverRightSeq`, `receiverTextIndexSeq`, `receiverSparseIndexSeq`, `receiverGraphIndexSeq`, `rightRouteOwner`, `enrichmentOwner` |
| `DonorRightWriteBeforeMerge` | `phase`, `donorOwnsRight`, `donorAcceptsRight`, `donorRightSeq` | `donorRightSeq`, `enrichmentOwner` |
| `FinalizeMerge` | `phase`, `donorRightSeq` | `phase`, `donorOwnsRight`, `receiverOwnsRight`, `donorAcceptsRight`, `receiverAcceptsRight`, `receiverRightSeq`, `receiverTextIndexSeq`, `receiverSparseIndexSeq`, `receiverGraphIndexSeq`, `rightRouteOwner`, `enrichmentOwner` |
| `ReceiverRightWrite` | `phase`, `receiverOwnsRight`, `receiverAcceptsRight`, `receiverRightSeq` | `receiverRightSeq`, `receiverTextIndexSeq`, `receiverSparseIndexSeq`, `receiverGraphIndexSeq`, `enrichmentOwner` |
| `PublishEnrichment` | `phase` | `enrichmentOwner` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[ParentLeftWrite]
        a1[ParentRightWriteBeforeSplit]
        a2[PrepareSplit]
        a3[ParentRightWriteDuringSplit]
        a4[ReplaySplitDelta]
        a5[BuildChildTextIndex]
        a6[BuildChildSparseIndex]
        a7[BuildChildGraphIndex]
        a8[FinalizeSplit]
        a9[ChildRightWrite]
        a10[StartMerge]
        a11[DonorRightWriteBeforeMerge]
        a12[FinalizeMerge]
        a13[ReceiverRightWrite]
        a14[PublishEnrichment]
    end
    subgraph state["State variables"]
        v0([leftSeq])
        v1([parentOwnsLeft])
        v2([phase])
        v3([parentRightSeq])
        v4([parentOwnsRight])
        v5([parentAcceptsRight])
        v6([splitSnapshotSeq])
        v7([splitDeltaSeq])
        v8([childReplaySeq])
        v9([childRightSeq])
        v10([childTextIndexSeq])
        v11([childSparseIndexSeq])
        v12([childGraphIndexSeq])
        v13([childArtifactPlacement])
        v14([rightRouteOwner])
        v15([enrichmentOwner])
        v16([childServing])
        v17([childOwnsRight])
        v18([childAcceptsRight])
        v19([donorOwnsRight])
        v20([receiverOwnsRight])
        v21([donorAcceptsRight])
        v22([receiverAcceptsRight])
        v23([donorRightSeq])
        v24([receiverRightSeq])
        v25([receiverTextIndexSeq])
        v26([receiverSparseIndexSeq])
        v27([receiverGraphIndexSeq])
    end
    a0 --> v0
    v1 -.-> a0
    a1 --> v3
    v2 -.-> a1
    v4 -.-> a1
    v5 -.-> a1
    a2 --> v2
    a2 --> v6
    a2 --> v7
    a2 --> v8
    a2 --> v9
    a2 --> v10
    a2 --> v11
    a2 --> v12
    a2 --> v13
    a2 --> v14
    a2 --> v15
    v3 -.-> a2
    a3 --> v2
    a3 --> v3
    a3 --> v7
    a3 --> v15
    v4 -.-> a3
    v5 -.-> a3
    a4 --> v2
    a4 --> v8
    a4 --> v9
    v6 -.-> a4
    v7 -.-> a4
    a5 --> v10
    v2 -.-> a5
    v9 -.-> a5
    a6 --> v11
    v2 -.-> a6
    v9 -.-> a6
    a7 --> v12
    v2 -.-> a7
    v9 -.-> a7
    a8 --> v2
    a8 --> v13
    a8 --> v16
    a8 --> v4
    a8 --> v17
    a8 --> v5
    a8 --> v18
    a8 --> v14
    a8 --> v15
    a9 --> v3
    a9 --> v9
    a9 --> v10
    a9 --> v11
    a9 --> v12
    a9 --> v15
    v2 -.-> a9
    v17 -.-> a9
    v18 -.-> a9
    a10 --> v2
    a10 --> v4
    a10 --> v19
    a10 --> v20
    a10 --> v5
    a10 --> v21
    a10 --> v22
    a10 --> v23
    a10 --> v24
    a10 --> v25
    a10 --> v26
    a10 --> v27
    a10 --> v14
    a10 --> v15
    v3 -.-> a10
    a11 --> v23
    a11 --> v15
    v2 -.-> a11
    v19 -.-> a11
    v21 -.-> a11
    a12 --> v2
    a12 --> v19
    a12 --> v20
    a12 --> v21
    a12 --> v22
    a12 --> v24
    a12 --> v25
    a12 --> v26
    a12 --> v27
    a12 --> v14
    a12 --> v15
    v23 -.-> a12
    a13 --> v24
    a13 --> v25
    a13 --> v26
    a13 --> v27
    a13 --> v15
    v2 -.-> a13
    v20 -.-> a13
    v22 -.-> a13
    a14 --> v15
```
