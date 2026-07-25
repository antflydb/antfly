<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyLsmWalCompaction — structural diagrams

Generated from [`AntflyLsmWalCompaction.tla`](../../../storage/lsm/AntflyLsmWalCompaction.tla). 15 state variables, 12 actions in `Next`. 3 expected-failure mutant action(s) (gated by `Buggy*` constants) omitted.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `AppendWal` | `walEnd`, `writtenSeqs`, `durableSeqs`, `segmentOfSeq`, `currentSegment`, `corruptTailSegment` | `walEnd`, `durableWalEnd`, `writtenSeqs`, `durableSeqs`, `segmentOfSeq`, `mutableSeq` |
| `SyncCurrentSegment` | `writtenSeqs`, `durableSeqs`, `segmentOfSeq`, `currentSegment` | `durableWalEnd`, `durableSeqs` |
| `RotateSegment` | `writtenSeqs`, `durableSeqs`, `segmentOfSeq`, `currentSegment`, `corruptTailSegment` | `currentSegment` |
| `InjectCurrentCorruptTail` | `writtenSeqs`, `segmentOfSeq`, `currentSegment`, `corruptTailSegment` | `corruptTailSegment` |
| `ReplayDropsCurrentCorruptTail` | `currentSegment`, `corruptTailSegment` | `corruptTailSegment` |
| `CrashDropsUnsyncedTail` | `walEnd`, `durableWalEnd`, `durableSeqs` | `walEnd`, `writtenSeqs`, `durableSeqs`, `mutableSeq`, `compactionActive`, `compactionInput`, `corruptTailSegment` |
| `StartCompaction` | `durableWalEnd`, `mutableSeq`, `tableSeq`, `compactionActive` | `compactionActive`, `compactionInput` |
| `PublishCompaction` | `durableWalEnd`, `compactionActive`, `compactionInput` | `tableSeq`, `compactionActive`, `compactionInput` |
| `Checkpoint` | `durableWalEnd`, `checkpointSeq`, `tableSeq` | `checkpointSeq` |
| `PinReader` | `writtenSeqs`, `segmentOfSeq`, `readerPinnedSegment`, `deletedSegments` | `readerPinnedSegment` |
| `UnpinReader` | `readerPinnedSegment` | `readerPinnedSegment` |
| `RetireCoveredSegment` | `writtenSeqs`, `segmentOfSeq`, `currentSegment`, `oldestRetainedSegment`, `checkpointSeq`, `readerPinnedSegment`, `deletedSegments` | `oldestRetainedSegment`, `deletedSegments` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[AppendWal]
        a1[SyncCurrentSegment]
        a2[RotateSegment]
        a3[InjectCurrentCorruptTail]
        a4[ReplayDropsCurrentCorruptTail]
        a5[CrashDropsUnsyncedTail]
        a6[StartCompaction]
        a7[PublishCompaction]
        a8[Checkpoint]
        a9[PinReader]
        a10[UnpinReader]
        a11[RetireCoveredSegment]
    end
    subgraph state["State variables"]
        v0([walEnd])
        v1([durableWalEnd])
        v2([writtenSeqs])
        v3([durableSeqs])
        v4([segmentOfSeq])
        v5([currentSegment])
        v6([mutableSeq])
        v7([corruptTailSegment])
        v8([compactionActive])
        v9([compactionInput])
        v10([tableSeq])
        v11([checkpointSeq])
        v12([readerPinnedSegment])
        v13([deletedSegments])
        v14([oldestRetainedSegment])
    end
    a0 --> v0
    a0 --> v1
    a0 --> v2
    a0 --> v3
    a0 --> v4
    a0 --> v6
    v5 -.-> a0
    v7 -.-> a0
    a1 --> v1
    a1 --> v3
    v5 -.-> a1
    a2 --> v5
    v3 -.-> a2
    v7 -.-> a2
    a3 --> v7
    v5 -.-> a3
    a4 --> v7
    v5 -.-> a4
    a5 --> v0
    a5 --> v2
    a5 --> v3
    a5 --> v6
    a5 --> v8
    a5 --> v9
    a5 --> v7
    v1 -.-> a5
    a6 --> v8
    a6 --> v9
    v1 -.-> a6
    v6 -.-> a6
    v10 -.-> a6
    a7 --> v10
    a7 --> v8
    a7 --> v9
    v1 -.-> a7
    a8 --> v11
    v1 -.-> a8
    v10 -.-> a8
    a9 --> v12
    v13 -.-> a9
    a10 --> v12
    a11 --> v14
    a11 --> v13
```
