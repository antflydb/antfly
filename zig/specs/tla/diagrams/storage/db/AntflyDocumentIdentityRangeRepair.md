<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyDocumentIdentityRangeRepair — structural diagrams

Generated from [`AntflyDocumentIdentityRangeRepair.tla`](../../../storage/db/AntflyDocumentIdentityRangeRepair.tla). 23 state variables, 11 actions in `Next`.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `sourceStatus`

Domain: `healthyA`, `healthyB`, `oldNoRows`, `active`, `conflict`, `rebuild`, `exhausted`. No statically extractable guard/update transitions.

### `donorStatus`

Domain: `healthyA`, `healthyB`, `oldNoRows`, `active`, `conflict`, `rebuild`, `exhausted`. No statically extractable guard/update transitions.

### `receiverStatus`

Domain: `healthyA`, `healthyB`, `oldNoRows`, `active`, `conflict`, `rebuild`, `exhausted`. No statically extractable guard/update transitions.

### `expectedArtifact`

Domain: `bound`, `other`, `none`. No statically extractable guard/update transitions.

### `reportedArtifact`

Domain: `bound`, `other`, `none`. No statically extractable guard/update transitions.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `ValidateSplit` | `sourceStatus` | `splitAccepted` |
| `ObserveSplitDestinationStatus` | `destStoredNamespace` | `splitStatusAccepted` |
| `ValidateMerge` | `donorStatus`, `receiverStatus`, `mergeOptIn` | `mergeAccepted` |
| `ReassignReceiverNamespace` | `donorStatus`, `receiverStatus`, `mergeOptIn` | `receiverReassigned` |
| `StrictDeferredRestore` | `restoreSourceNamespace`, `restoreTargetNamespace` | `strictRestoreAccepted` |
| `RecoverIncompleteImport` | — | `importRecovered`, `runtimeRepairNeeded` |
| `RunRuntimeRepair` | `importRecovered`, `runtimeRepairNeeded`, `expectedArtifact`, `reportedArtifact`, `replicaRepairComplete` | `runtimeRepairComplete`, `reportedArtifact`, `replicaRepairComplete` |
| `ReportReplicaRepair` | `importRecovered`, `runtimeRepairNeeded`, `runtimeRepairComplete`, `expectedArtifact`, `reportedArtifact`, `replicaRepairComplete`, `acceptedMismatchedArtifact` | `runtimeRepairComplete`, `reportedArtifact`, `replicaRepairComplete`, `acceptedMismatchedArtifact` |
| `ClearRestoreIntent` | `importRecovered`, `runtimeRepairNeeded`, `runtimeRepairComplete`, `replicaRepairComplete` | `restoreIntentCleared`, `restorePending` |
| `UpdateRestoreReadiness` | `restorePending`, `groupReady` | `groupReady` |
| `StartSplitDuringRestore` | `restorePending`, `splitStarted` | `splitStarted` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[ValidateSplit]
        a1[ObserveSplitDestinationStatus]
        a2[ValidateMerge]
        a3[ReassignReceiverNamespace]
        a4[StrictDeferredRestore]
        a5[RecoverIncompleteImport]
        a6[RunRuntimeRepair]
        a7[ReportReplicaRepair]
        a8[ClearRestoreIntent]
        a9[UpdateRestoreReadiness]
        a10[StartSplitDuringRestore]
    end
    subgraph state["State variables"]
        v0([sourceStatus])
        v1([splitAccepted])
        v2([destStoredNamespace])
        v3([splitStatusAccepted])
        v4([donorStatus])
        v5([receiverStatus])
        v6([mergeOptIn])
        v7([mergeAccepted])
        v8([receiverReassigned])
        v9([restoreSourceNamespace])
        v10([restoreTargetNamespace])
        v11([strictRestoreAccepted])
        v12([importRecovered])
        v13([runtimeRepairNeeded])
        v14([runtimeRepairComplete])
        v15([expectedArtifact])
        v16([reportedArtifact])
        v17([replicaRepairComplete])
        v18([acceptedMismatchedArtifact])
        v19([restoreIntentCleared])
        v20([restorePending])
        v21([groupReady])
        v22([splitStarted])
    end
    a0 --> v1
    v0 -.-> a0
    a1 --> v3
    v2 -.-> a1
    a2 --> v7
    v4 -.-> a2
    v5 -.-> a2
    v6 -.-> a2
    a3 --> v8
    v4 -.-> a3
    v5 -.-> a3
    v6 -.-> a3
    a4 --> v11
    v9 -.-> a4
    v10 -.-> a4
    a5 --> v12
    a5 --> v13
    a6 --> v14
    a6 --> v16
    a6 --> v17
    v12 -.-> a6
    v13 -.-> a6
    v15 -.-> a6
    a7 --> v14
    a7 --> v16
    a7 --> v17
    a7 --> v18
    v12 -.-> a7
    v13 -.-> a7
    v15 -.-> a7
    a8 --> v19
    a8 --> v20
    v12 -.-> a8
    v13 -.-> a8
    v14 -.-> a8
    v17 -.-> a8
    a9 --> v21
    v20 -.-> a9
    a10 --> v22
    v20 -.-> a10
```
