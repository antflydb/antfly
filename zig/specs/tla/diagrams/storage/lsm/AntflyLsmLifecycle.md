<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyLsmLifecycle — structural diagrams

Generated from [`AntflyLsmLifecycle.tla`](../../../storage/lsm/AntflyLsmLifecycle.tla). 8 state variables, 19 actions in `Next`. 1 expected-failure mutant action(s) (gated by `Buggy*` constants) omitted.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `cacheLoc`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> Absent
    Absent --> Live : CacheOpenSucceeds
    Live --> Destroyed : CacheRetireInactive
    Live --> Retired : CacheRetireActive
    Retired --> Destroyed : CacheReleaseRetiredLease
```

### `snapshotLoc`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> NoSnapshot
    NoSnapshot --> MutableOwner : BeginReadSnapshotSucceeds
    MutableOwner --> RetiredOwner : InvalidateMutableSnapshotWithActiveReader
    MutableOwner --> Destroyed : InvalidateMutableSnapshotWithoutReader
    RetiredOwner --> Destroyed : ReleaseReaderDrainsRetiredSnapshot
```

### `indexTemp`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> None
    None --> NewOnly : IndexAllocateNewSegments
    NewOnly --> Freed : IndexRetiredAllocationFails
    NewOnly --> BothAllocated : IndexRetiredAllocationSucceeds
    BothAllocated --> Freed : IndexRebuildFails
    BothAllocated --> Published : IndexRebuildPublishes
```

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `CacheOpenReserveFails` | `cacheLoc` | — |
| `CacheOpenSucceeds` | `cacheLoc`, `cacheLeases`, `cacheRetiredCap` | `cacheLoc`, `cacheLeases`, `cacheRetiredCap` |
| `CacheReleaseLiveLease` | `cacheLoc`, `cacheLeases` | `cacheLeases` |
| `CacheRetireInactive` | `cacheLoc`, `cacheLeases` | `cacheLoc` |
| `CacheRetireActive` | `cacheLoc`, `cacheLeases`, `cacheRetiredCap` | `cacheLoc` |
| `CacheReleaseRetiredLease` | `cacheLoc`, `cacheLeases` | `cacheLoc`, `cacheLeases` |
| `BeginReadSnapshotReserveFails` | `snapshotLoc`, `activeReaders` | — |
| `BeginReadSnapshotSucceeds` | `snapshotLoc`, `activeReaders` | `snapshotLoc`, `activeReaders`, `snapshotRetiredCap` |
| `BeginAdditionalRead` | `snapshotLoc`, `activeReaders` | — |
| `InvalidateMutableSnapshotWithActiveReader` | `snapshotLoc`, `activeReaders`, `snapshotRetiredCap` | `snapshotLoc` |
| `ReleaseOnlyReaderBeforeInvalidation` | `snapshotLoc`, `activeReaders` | `activeReaders` |
| `InvalidateMutableSnapshotWithoutReader` | `snapshotLoc`, `activeReaders` | `snapshotLoc` |
| `ReleaseReaderDrainsRetiredSnapshot` | `snapshotLoc`, `activeReaders` | `snapshotLoc`, `activeReaders` |
| `IndexAllocateNewSegments` | `indexTemp` | `indexTemp` |
| `IndexRetiredAllocationFails` | `indexTemp` | `indexTemp`, `indexOpFailed` |
| `IndexRetiredAllocationSucceeds` | `indexTemp` | `indexTemp` |
| `IndexRebuildFails` | `indexTemp` | `indexTemp`, `indexOpFailed` |
| `IndexRebuildPublishes` | `indexTemp` | `indexTemp` |
| `Stutter` | — | — |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[CacheOpenReserveFails]
        a1[CacheOpenSucceeds]
        a2[CacheReleaseLiveLease]
        a3[CacheRetireInactive]
        a4[CacheRetireActive]
        a5[CacheReleaseRetiredLease]
        a6[BeginReadSnapshotReserveFails]
        a7[BeginReadSnapshotSucceeds]
        a8[BeginAdditionalRead]
        a9[InvalidateMutableSnapshotWithActiveReader]
        a10[ReleaseOnlyReaderBeforeInvalidation]
        a11[InvalidateMutableSnapshotWithoutReader]
        a12[ReleaseReaderDrainsRetiredSnapshot]
        a13[IndexAllocateNewSegments]
        a14[IndexRetiredAllocationFails]
        a15[IndexRetiredAllocationSucceeds]
        a16[IndexRebuildFails]
        a17[IndexRebuildPublishes]
        a18[Stutter]
    end
    subgraph state["State variables"]
        v0([cacheLoc])
        v1([cacheLeases])
        v2([cacheRetiredCap])
        v3([snapshotLoc])
        v4([activeReaders])
        v5([snapshotRetiredCap])
        v6([indexTemp])
        v7([indexOpFailed])
    end
    v0 -.-> a0
    a1 --> v0
    a1 --> v1
    a1 --> v2
    a2 --> v1
    v0 -.-> a2
    a3 --> v0
    v1 -.-> a3
    a4 --> v0
    v1 -.-> a4
    v2 -.-> a4
    a5 --> v0
    a5 --> v1
    v3 -.-> a6
    v4 -.-> a6
    a7 --> v3
    a7 --> v4
    a7 --> v5
    v3 -.-> a8
    v4 -.-> a8
    a9 --> v3
    v4 -.-> a9
    v5 -.-> a9
    a10 --> v4
    v3 -.-> a10
    a11 --> v3
    v4 -.-> a11
    a12 --> v3
    a12 --> v4
    a13 --> v6
    a14 --> v6
    a14 --> v7
    a15 --> v6
    a16 --> v6
    a16 --> v7
    a17 --> v6
```
