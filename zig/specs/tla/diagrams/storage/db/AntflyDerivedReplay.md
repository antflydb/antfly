<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyDerivedReplay — structural diagrams

Generated from [`AntflyDerivedReplay.tla`](../../../storage/db/AntflyDerivedReplay.tla). 12 state variables, 13 actions in `Next`. 1 expected-failure mutant action(s) (gated by `Buggy*` constants) omitted.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `TruncateReplayFloor` | `journalSeq`, `replayAll`, `hintLane`, `latestHintMeta`, `truncateFloor`, `appliedRecords` | `hintLane`, `truncateFloor` |
| `AppendAllLaneOnly` | `journalSeq`, `replayAll` | `journalSeq`, `replayAll` |
| `AppendHintedRecord` | `journalSeq`, `replayAll`, `hintLane`, `latestHintMeta` | `journalSeq`, `replayAll`, `hintLane`, `latestHintMeta` |
| `ToggleHintLaneAvailability` | `hintLaneAvailable` | `hintLaneAvailable` |
| `StartBulkSession` | `catchupActive`, `bulkSessionActive` | `bulkSessionActive` |
| `FinishBulkSession` | `bulkSessionActive` | `bulkSessionActive` |
| `ObserveReplayTarget` | `latestHintMeta`, `target`, `catchupActive`, `bulkSessionActive` | `target` |
| `StartCatchup` | `applied`, `target`, `catchupActive`, `bulkSessionActive` | `catchupActive` |
| `AdvanceWhenNoVisibleHintMatch` | `replayAll`, `truncateFloor`, `applied`, `target`, `catchupActive` | `applied` |
| `FinishCatchup` | `applied`, `target`, `catchupActive` | `catchupActive` |
| `AdvanceQueryTarget` | `applied`, `queryTarget`, `catchupActive`, `bulkSessionActive` | `queryTarget` |
| `ApplyHintMatch` | `replayAll`, `hintLane`, `hintLaneAvailable`, `truncateFloor`, `applied`, `appliedRecords`, `target`, `catchupActive` | `applied`, `appliedRecords` |
| `ApplyFallbackMatch` | `replayAll`, `hintLane`, `hintLaneAvailable`, `truncateFloor`, `applied`, `appliedRecords`, `target`, `catchupActive` | `applied`, `appliedRecords` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[TruncateReplayFloor]
        a1[AppendAllLaneOnly]
        a2[AppendHintedRecord]
        a3[ToggleHintLaneAvailability]
        a4[StartBulkSession]
        a5[FinishBulkSession]
        a6[ObserveReplayTarget]
        a7[StartCatchup]
        a8[AdvanceWhenNoVisibleHintMatch]
        a9[FinishCatchup]
        a10[AdvanceQueryTarget]
        a11[ApplyHintMatch]
        a12[ApplyFallbackMatch]
    end
    subgraph state["State variables"]
        v0([journalSeq])
        v1([hintLane])
        v2([truncateFloor])
        v3([replayAll])
        v4([latestHintMeta])
        v5([hintLaneAvailable])
        v6([catchupActive])
        v7([bulkSessionActive])
        v8([target])
        v9([applied])
        v10([queryTarget])
        v11([appliedRecords])
    end
    a0 --> v1
    a0 --> v2
    v0 -.-> a0
    a1 --> v0
    a1 --> v3
    a2 --> v0
    a2 --> v3
    a2 --> v1
    a2 --> v4
    a3 --> v5
    a4 --> v7
    v6 -.-> a4
    a5 --> v7
    a6 --> v8
    v4 -.-> a6
    v6 -.-> a6
    v7 -.-> a6
    a7 --> v6
    v9 -.-> a7
    v8 -.-> a7
    v7 -.-> a7
    a8 --> v9
    v8 -.-> a8
    v6 -.-> a8
    a9 --> v6
    v9 -.-> a9
    v8 -.-> a9
    a10 --> v10
    v9 -.-> a10
    v6 -.-> a10
    v7 -.-> a10
    a11 --> v9
    a11 --> v11
    v5 -.-> a11
    v8 -.-> a11
    v6 -.-> a11
    a12 --> v9
    a12 --> v11
    v5 -.-> a12
    v8 -.-> a12
    v6 -.-> a12
```
