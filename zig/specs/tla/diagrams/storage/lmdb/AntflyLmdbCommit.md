<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyLmdbCommit — structural diagrams

Generated from [`AntflyLmdbCommit.tla`](../../../storage/lmdb/AntflyLmdbCommit.tla). 18 state variables, 15 actions in `Next`. 2 expected-failure mutant action(s) (gated by `Buggy*` constants) omitted.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `BeginWrite` | `activeMeta`, `metaTxn`, `writerActive` | `writerActive`, `writerTxn`, `parentDirty`, `childActive`, `childDirty`, `dataWritten`, `dataSynced`, `metaWritten`, `metaSynced`, `preparedPages`, `preparedRetired` |
| `ParentPut` | `writerActive`, `childActive`, `dataWritten` | `parentDirty` |
| `BeginChild` | `writerActive`, `childActive`, `dataWritten` | `childActive`, `childDirty` |
| `ChildPut` | `childActive` | `childDirty` |
| `CommitChild` | `childActive`, `childDirty` | `parentDirty`, `childActive`, `childDirty` |
| `AbortChild` | `childActive` | `childActive`, `childDirty` |
| `WriteDataPage` | `activeMeta`, `metaTxn`, `pageTxn`, `snapshotPages`, `freeRecordTxn`, `writerActive`, `writerTxn`, `parentDirty`, `childActive`, `dataWritten`, `readerTxn` | `pageTxn`, `dataWritten`, `preparedPages`, `preparedRetired` |
| `SyncData` | `writerActive`, `writerTxn`, `dataWritten`, `dataSynced` | `durableDataTxn`, `dataSynced` |
| `WriteMeta` | `activeMeta`, `metaTxn`, `writerActive`, `writerTxn`, `childActive`, `dataSynced`, `metaWritten` | `metaTxn`, `metaWritten` |
| `SyncMeta` | `writerActive`, `metaWritten`, `metaSynced` | `metaSynced` |
| `PublishMeta` | `activeMeta`, `snapshotPages`, `freeRecordTxn`, `writerActive`, `writerTxn`, `metaSynced`, `preparedPages`, `preparedRetired` | `activeMeta`, `snapshotPages`, `freeRecordTxn`, `writerActive`, `writerTxn`, `parentDirty`, `childActive`, `childDirty`, `dataWritten`, `dataSynced`, `metaWritten`, `metaSynced`, `preparedPages`, `preparedRetired` |
| `AbortWrite` | `writerActive`, `dataWritten` | `writerActive`, `writerTxn`, `parentDirty`, `childActive`, `childDirty`, `dataWritten`, `dataSynced`, `metaWritten`, `metaSynced`, `preparedPages`, `preparedRetired` |
| `CrashAndReopen` | `activeMeta`, `metaTxn`, `durableDataTxn`, `snapshotPages`, `freeRecordTxn`, `writerActive`, `writerTxn`, `metaSynced`, `preparedPages`, `preparedRetired` | `activeMeta`, `snapshotPages`, `freeRecordTxn`, `writerActive`, `writerTxn`, `parentDirty`, `childActive`, `childDirty`, `dataWritten`, `dataSynced`, `metaWritten`, `metaSynced`, `preparedPages`, `preparedRetired`, `readerTxn` |
| `BeginReader` | `activeMeta`, `metaTxn`, `readerTxn` | `readerTxn` |
| `EndReader` | `readerTxn` | `readerTxn` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[BeginWrite]
        a1[ParentPut]
        a2[BeginChild]
        a3[ChildPut]
        a4[CommitChild]
        a5[AbortChild]
        a6[WriteDataPage]
        a7[SyncData]
        a8[WriteMeta]
        a9[SyncMeta]
        a10[PublishMeta]
        a11[AbortWrite]
        a12[CrashAndReopen]
        a13[BeginReader]
        a14[EndReader]
    end
    subgraph state["State variables"]
        v0([writerActive])
        v1([writerTxn])
        v2([parentDirty])
        v3([childActive])
        v4([childDirty])
        v5([dataWritten])
        v6([dataSynced])
        v7([metaWritten])
        v8([metaSynced])
        v9([preparedPages])
        v10([preparedRetired])
        v11([pageTxn])
        v12([snapshotPages])
        v13([durableDataTxn])
        v14([metaTxn])
        v15([activeMeta])
        v16([freeRecordTxn])
        v17([readerTxn])
    end
    a0 --> v0
    a0 --> v1
    a0 --> v2
    a0 --> v3
    a0 --> v4
    a0 --> v5
    a0 --> v6
    a0 --> v7
    a0 --> v8
    a0 --> v9
    a0 --> v10
    a1 --> v2
    v0 -.-> a1
    v3 -.-> a1
    v5 -.-> a1
    a2 --> v3
    a2 --> v4
    v0 -.-> a2
    v5 -.-> a2
    a3 --> v4
    v3 -.-> a3
    a4 --> v2
    a4 --> v3
    a4 --> v4
    a5 --> v3
    a5 --> v4
    a6 --> v11
    a6 --> v5
    a6 --> v9
    a6 --> v10
    v12 -.-> a6
    v0 -.-> a6
    v1 -.-> a6
    v2 -.-> a6
    v3 -.-> a6
    a7 --> v13
    a7 --> v6
    v0 -.-> a7
    v1 -.-> a7
    v5 -.-> a7
    a8 --> v14
    a8 --> v7
    v0 -.-> a8
    v1 -.-> a8
    v3 -.-> a8
    v6 -.-> a8
    a9 --> v8
    v0 -.-> a9
    v7 -.-> a9
    a10 --> v15
    a10 --> v12
    a10 --> v16
    a10 --> v0
    a10 --> v1
    a10 --> v2
    a10 --> v3
    a10 --> v4
    a10 --> v5
    a10 --> v6
    a10 --> v7
    a10 --> v8
    a10 --> v9
    a10 --> v10
    a11 --> v0
    a11 --> v1
    a11 --> v2
    a11 --> v3
    a11 --> v4
    a11 --> v5
    a11 --> v6
    a11 --> v7
    a11 --> v8
    a11 --> v9
    a11 --> v10
    a12 --> v15
    a12 --> v12
    a12 --> v16
    a12 --> v0
    a12 --> v1
    a12 --> v2
    a12 --> v3
    a12 --> v4
    a12 --> v5
    a12 --> v6
    a12 --> v7
    a12 --> v8
    a12 --> v9
    a12 --> v10
    a12 --> v17
    v14 -.-> a12
    v13 -.-> a12
    a13 --> v17
    a14 --> v17
```
