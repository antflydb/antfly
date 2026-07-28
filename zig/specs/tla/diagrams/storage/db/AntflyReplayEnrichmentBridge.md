<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyReplayEnrichmentBridge — structural diagrams

Generated from [`AntflyReplayEnrichmentBridge.tla`](../../../storage/db/AntflyReplayEnrichmentBridge.tla). 9 state variables, 9 actions in `Next`.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `AppendGeneratedSource` | `sourceSeq`, `journal`, `coverageDebt` | `sourceSeq`, `journal`, `coverageDebt` |
| `FastConsumerAdvance` | `sourceSeq`, `fastApplied` | `fastApplied` |
| `ProviderFails` | `providerUp` | `providerUp` |
| `ProviderRecovers` | `providerUp` | `providerUp` |
| `AdvanceEnrichment` | `sourceSeq`, `journal`, `enrichmentApplied`, `coverageDebt`, `volatileCollected` | `enrichmentApplied` |
| `Restart` | `processEpoch` | `volatileCollected`, `processEpoch` |
| `CollectPending` | `journal`, `coverageDebt`, `volatileCollected` | `volatileCollected` |
| `CompleteEnrichment` | `journal`, `coverageDebt`, `completed`, `volatileCollected`, `providerUp` | `coverageDebt`, `completed`, `volatileCollected` |
| `TruncateReplay` | `journal`, `fastApplied`, `enrichmentApplied` | `journal` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[AppendGeneratedSource]
        a1[FastConsumerAdvance]
        a2[ProviderFails]
        a3[ProviderRecovers]
        a4[AdvanceEnrichment]
        a5[Restart]
        a6[CollectPending]
        a7[CompleteEnrichment]
        a8[TruncateReplay]
    end
    subgraph state["State variables"]
        v0([sourceSeq])
        v1([journal])
        v2([coverageDebt])
        v3([fastApplied])
        v4([providerUp])
        v5([enrichmentApplied])
        v6([volatileCollected])
        v7([processEpoch])
        v8([completed])
    end
    a0 --> v0
    a0 --> v1
    a0 --> v2
    a1 --> v3
    v0 -.-> a1
    a2 --> v4
    a3 --> v4
    a4 --> v5
    v0 -.-> a4
    v1 -.-> a4
    v2 -.-> a4
    v6 -.-> a4
    a5 --> v6
    a5 --> v7
    a6 --> v6
    v1 -.-> a6
    v2 -.-> a6
    a7 --> v2
    a7 --> v8
    a7 --> v6
    v1 -.-> a7
    v4 -.-> a7
    a8 --> v1
    v3 -.-> a8
    v5 -.-> a8
```
