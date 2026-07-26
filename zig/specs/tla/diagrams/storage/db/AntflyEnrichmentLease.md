<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyEnrichmentLease — structural diagrams

Generated from [`AntflyEnrichmentLease.tla`](../../../storage/db/AntflyEnrichmentLease.tla). 20 state variables, 13 actions in `Next`. 2 expected-failure mutant action(s) (gated by `Buggy*` constants) omitted.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `leaseOwner`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> none
    nodeA --> none : LoseLease
    nodeB --> none : LoseLease
    classDef c_none fill:#2a78d630,stroke:#2a78d6
    class none c_none
    classDef c_nodeA fill:#eb683430,stroke:#eb6834
    class nodeA c_nodeA
    classDef c_nodeB fill:#1baf7a30,stroke:#1baf7a
    class nodeB c_nodeB
```

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `AppendSource` | `sourceSeq`, `pendingRequired` | `sourceSeq`, `targetSeq`, `pendingRequired`, `retrying`, `retrySeq`, `workerFailed`, `isolatedFailedIndexes` |
| `PublishReplay` | `sourceSeq`, `visibleReplay` | `visibleReplay` |
| `CollectPending` | `leaseValid`, `leaseEpoch`, `targetSeq`, `appliedSeq`, `visibleReplay`, `pendingRequired`, `collected`, `collectedEpoch`, `retrying`, `workerFailed` | `collected`, `collectedEpoch` |
| `GenerateArtifact` | `leaseValid`, `leaseEpoch`, `collected`, `collectedEpoch`, `generated`, `generatedEpoch`, `retrying`, `workerFailed` | `generated`, `generatedEpoch` |
| `PublishGenerated` | `leaseValid`, `leaseEpoch`, `visibleReplay`, `generated`, `generatedEpoch`, `publishedArtifacts`, `publishValid`, `retrying`, `workerFailed` | `publishedArtifacts`, `publishValid` |
| `RetryTransient` | `leaseValid`, `targetSeq`, `appliedSeq`, `visibleReplay`, `pendingRequired`, `publishedArtifacts`, `retrying`, `workerFailed`, `isolatedSeqs` | `retrying`, `retrySeq` |
| `RetryLater` | `retrying` | `retrying`, `retrySeq` |
| `LoseLease` | `leaseOwner`, `leaseValid`, `lostLeaseCount` | `leaseOwner`, `leaseValid`, `lostLeaseCount` |
| `FatalWorkerFailure` | `leaseValid`, `workerFailed` | `retrying`, `retrySeq`, `workerFailed` |
| `AdvanceAppliedOne` | `leaseValid`, `targetSeq`, `appliedSeq`, `visibleReplay`, `pendingRequired`, `publishedArtifacts`, `retrying`, `workerFailed`, `isolatedSeqs` | `appliedSeq` |
| `AdvanceNoPendingToTarget` | `leaseValid`, `targetSeq`, `appliedSeq`, `pendingRequired`, `retrying`, `workerFailed` | `appliedSeq` |
| `AcquireLease` | `leaseOwner`, `leaseValid`, `leaseEpoch` | `leaseOwner`, `leaseValid`, `leaseEpoch` |
| `IsolateRequestFailure` | `leaseValid`, `targetSeq`, `appliedSeq`, `visibleReplay`, `pendingRequired`, `publishedArtifacts`, `retrying`, `isolatedFailedIndexes`, `isolatedSeqs` | `workerFailed`, `isolatedFailedIndexes`, `isolatedSeqs` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[AppendSource]
        a1[PublishReplay]
        a2[CollectPending]
        a3[GenerateArtifact]
        a4[PublishGenerated]
        a5[RetryTransient]
        a6[RetryLater]
        a7[LoseLease]
        a8[FatalWorkerFailure]
        a9[AdvanceAppliedOne]
        a10[AdvanceNoPendingToTarget]
        a11[AcquireLease]
        a12[IsolateRequestFailure]
    end
    subgraph state["State variables"]
        v0([sourceSeq])
        v1([targetSeq])
        v2([pendingRequired])
        v3([retrying])
        v4([retrySeq])
        v5([workerFailed])
        v6([isolatedFailedIndexes])
        v7([visibleReplay])
        v8([leaseValid])
        v9([leaseEpoch])
        v10([appliedSeq])
        v11([collected])
        v12([collectedEpoch])
        v13([generated])
        v14([generatedEpoch])
        v15([publishedArtifacts])
        v16([publishValid])
        v17([isolatedSeqs])
        v18([leaseOwner])
        v19([lostLeaseCount])
    end
    a0 --> v0
    a0 --> v1
    a0 --> v2
    a0 --> v3
    a0 --> v4
    a0 --> v5
    a0 --> v6
    a1 --> v7
    v0 -.-> a1
    a2 --> v11
    a2 --> v12
    v8 -.-> a2
    v9 -.-> a2
    v1 -.-> a2
    v10 -.-> a2
    v3 -.-> a2
    v5 -.-> a2
    a3 --> v13
    a3 --> v14
    v8 -.-> a3
    v9 -.-> a3
    v11 -.-> a3
    v12 -.-> a3
    v3 -.-> a3
    v5 -.-> a3
    a4 --> v15
    a4 --> v16
    v8 -.-> a4
    v9 -.-> a4
    v7 -.-> a4
    v13 -.-> a4
    v14 -.-> a4
    v3 -.-> a4
    v5 -.-> a4
    a5 --> v3
    a5 --> v4
    v8 -.-> a5
    v1 -.-> a5
    v10 -.-> a5
    v15 -.-> a5
    v5 -.-> a5
    v17 -.-> a5
    a6 --> v3
    a6 --> v4
    a7 --> v18
    a7 --> v8
    a7 --> v19
    a8 --> v3
    a8 --> v4
    a8 --> v5
    v8 -.-> a8
    a9 --> v10
    v8 -.-> a9
    v1 -.-> a9
    v3 -.-> a9
    v5 -.-> a9
    a10 --> v10
    v8 -.-> a10
    v1 -.-> a10
    v3 -.-> a10
    v5 -.-> a10
    a11 --> v18
    a11 --> v8
    a11 --> v9
    a12 --> v5
    a12 --> v6
    a12 --> v17
    v8 -.-> a12
    v1 -.-> a12
    v10 -.-> a12
    v15 -.-> a12
    v3 -.-> a12
```
