<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyReplayEnrichmentBridge — structural diagrams

Generated from [`AntflyReplayEnrichmentBridge.tla`](../../../storage/db/AntflyReplayEnrichmentBridge.tla). 11 state variables, 12 actions in `Next`.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `AppendGeneratedSource` | `sourceSeq`, `journal`, `coverageDebt` | `sourceSeq`, `journal`, `coverageDebt` |
| `FastConsumerAdvance` | `sourceSeq`, `fastApplied` | `fastApplied` |
| `ProviderFails` | `providerUp` | `providerUp` |
| `ProviderRecovers` | `providerUp` | `providerUp`, `retryAttemptsSinceBoundary` |
| `TransientProviderRetry` | `coverageDebt`, `providerUp`, `workerArmed`, `retryAttemptsSinceBoundary` | `retryAttemptsSinceBoundary` |
| `RetrySchedulerBoundary` | `retryAttemptsSinceBoundary` | `retryAttemptsSinceBoundary` |
| `AdvanceEnrichment` | `sourceSeq`, `journal`, `enrichmentApplied`, `coverageDebt`, `volatileCollected` | `enrichmentApplied` |
| `Restart` | `processEpoch` | `volatileCollected`, `workerArmed`, `retryAttemptsSinceBoundary`, `processEpoch` |
| `ArmStartupEnrichment` | `workerArmed`, `processEpoch` | `workerArmed` |
| `CollectPending` | `journal`, `coverageDebt`, `volatileCollected` | `volatileCollected` |
| `CompleteEnrichment` | `journal`, `coverageDebt`, `completed`, `volatileCollected`, `providerUp`, `workerArmed` | `coverageDebt`, `completed`, `volatileCollected` |
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
        a4[TransientProviderRetry]
        a5[RetrySchedulerBoundary]
        a6[AdvanceEnrichment]
        a7[Restart]
        a8[ArmStartupEnrichment]
        a9[CollectPending]
        a10[CompleteEnrichment]
        a11[TruncateReplay]
    end
    subgraph state["State variables"]
        v0([sourceSeq])
        v1([journal])
        v2([coverageDebt])
        v3([fastApplied])
        v4([providerUp])
        v5([retryAttemptsSinceBoundary])
        v6([workerArmed])
        v7([enrichmentApplied])
        v8([volatileCollected])
        v9([processEpoch])
        v10([completed])
    end
    a0 --> v0
    a0 --> v1
    a0 --> v2
    a1 --> v3
    v0 -.-> a1
    a2 --> v4
    a3 --> v4
    a3 --> v5
    a4 --> v5
    v2 -.-> a4
    v4 -.-> a4
    v6 -.-> a4
    a5 --> v5
    a6 --> v7
    v0 -.-> a6
    v1 -.-> a6
    v2 -.-> a6
    v8 -.-> a6
    a7 --> v8
    a7 --> v6
    a7 --> v5
    a7 --> v9
    a8 --> v6
    v9 -.-> a8
    a9 --> v8
    v1 -.-> a9
    v2 -.-> a9
    a10 --> v2
    a10 --> v10
    a10 --> v8
    v1 -.-> a10
    v4 -.-> a10
    v6 -.-> a10
    a11 --> v1
    v3 -.-> a11
    v7 -.-> a11
```
