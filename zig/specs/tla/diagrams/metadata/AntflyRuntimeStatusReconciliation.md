<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyRuntimeStatusReconciliation — structural diagrams

Generated from [`AntflyRuntimeStatusReconciliation.tla`](../../metadata/AntflyRuntimeStatusReconciliation.tla). 26 state variables, 10 actions in `Next`. 2 expected-failure mutant action(s) (gated by `Buggy*` constants) omitted.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `PlanJoin` | `topologyGen`, `activeNodes`, `rootGen`, `selectedTopology`, `selectedRoot`, `selectedFresh`, `selectedSource`, `selectedDiskKnown`, `selectedDiskEvidenceRoot`, `joinPlanned` | `joinPlanned`, `joinUsedIncomplete` |
| `FinalizeSchemaMigration` | `topologyGen`, `activeNodes`, `selectedTopology`, `selectedFresh`, `selectedSchema`, `selectedSchemaCurrent`, `targetSchema`, `migrationFinalized` | `readSchema`, `migrationFinalized`, `finalizedWithoutCoverage` |
| `RefreshDiskFacts` | `activeNodes`, `rootGen`, `localDiskKnown`, `localDiskBytes`, `localDiskEvidenceRoot` | `localDiskKnown`, `localDiskBytes`, `localDiskEvidenceRoot` |
| `PublishFreshStatus` | `topologyGen`, `activeNodes`, `rootGen`, `localDiskKnown`, `localDiskBytes`, `localDiskEvidenceRoot`, `schemaBuilt`, `selectedTopology`, `selectedRoot`, `selectedStatusGen`, `selectedFresh`, `selectedSource`, `selectedDiskKnown`, `selectedDiskBytes`, `selectedDiskEvidenceRoot`, `selectedSchema`, `selectedSchemaCurrent` | `selectedTopology`, `selectedRoot`, `selectedStatusGen`, `selectedFresh`, `selectedSource`, `selectedDiskKnown`, `selectedDiskBytes`, `selectedDiskEvidenceRoot`, `selectedSchema`, `selectedSchemaCurrent` |
| `PublishOlderStatus` | `activeNodes`, `selectedTopology`, `selectedRoot`, `selectedStatusGen`, `selectedFresh`, `selectedSource`, `selectedDiskKnown`, `selectedDiskBytes`, `selectedDiskEvidenceRoot`, `selectedSchema`, `selectedSchemaCurrent` | `selectedTopology`, `selectedRoot`, `selectedStatusGen`, `selectedFresh`, `selectedSource`, `selectedDiskKnown`, `selectedDiskBytes`, `selectedDiskEvidenceRoot`, `selectedSchema`, `selectedSchemaCurrent`, `staleDisplaced` |
| `RotateStorageRoot` | `activeNodes`, `rootGen`, `localDiskKnown`, `localDiskBytes`, `localDiskEvidenceRoot` | `rootGen`, `localDiskKnown`, `localDiskBytes`, `localDiskEvidenceRoot` |
| `RemoveOwner` | `activeNodes` | `topologyGen`, `activeNodes` |
| `PublishRemovedOwner` | `activeNodes`, `selectedFresh` | `selectedFresh`, `removedOwnerSelected` |
| `DropOldReadSchema` | `activeNodes`, `availableSchemas`, `migrationFinalized` | `availableSchemas` |
| `BuildTargetSchema` | `activeNodes`, `schemaBuilt`, `availableSchemas`, `targetSchema` | `schemaBuilt`, `availableSchemas` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[PlanJoin]
        a1[FinalizeSchemaMigration]
        a2[RefreshDiskFacts]
        a3[PublishFreshStatus]
        a4[PublishOlderStatus]
        a5[RotateStorageRoot]
        a6[RemoveOwner]
        a7[PublishRemovedOwner]
        a8[DropOldReadSchema]
        a9[BuildTargetSchema]
    end
    subgraph state["State variables"]
        v0([activeNodes])
        v1([selectedFresh])
        v2([joinPlanned])
        v3([joinUsedIncomplete])
        v4([selectedSchema])
        v5([readSchema])
        v6([targetSchema])
        v7([migrationFinalized])
        v8([finalizedWithoutCoverage])
        v9([rootGen])
        v10([localDiskKnown])
        v11([localDiskBytes])
        v12([localDiskEvidenceRoot])
        v13([topologyGen])
        v14([schemaBuilt])
        v15([selectedTopology])
        v16([selectedRoot])
        v17([selectedStatusGen])
        v18([selectedSource])
        v19([selectedDiskKnown])
        v20([selectedDiskBytes])
        v21([selectedDiskEvidenceRoot])
        v22([selectedSchemaCurrent])
        v23([staleDisplaced])
        v24([removedOwnerSelected])
        v25([availableSchemas])
    end
    a0 --> v2
    a0 --> v3
    v0 -.-> a0
    v1 -.-> a0
    a1 --> v5
    a1 --> v7
    a1 --> v8
    v0 -.-> a1
    v4 -.-> a1
    v6 -.-> a1
    a2 --> v10
    a2 --> v11
    a2 --> v12
    v0 -.-> a2
    v9 -.-> a2
    a3 --> v15
    a3 --> v16
    a3 --> v17
    a3 --> v1
    a3 --> v18
    a3 --> v19
    a3 --> v20
    a3 --> v21
    a3 --> v4
    a3 --> v22
    v13 -.-> a3
    v0 -.-> a3
    v9 -.-> a3
    v10 -.-> a3
    v11 -.-> a3
    v12 -.-> a3
    v14 -.-> a3
    a4 --> v15
    a4 --> v16
    a4 --> v17
    a4 --> v1
    a4 --> v18
    a4 --> v19
    a4 --> v20
    a4 --> v21
    a4 --> v4
    a4 --> v22
    a4 --> v23
    v0 -.-> a4
    a5 --> v9
    a5 --> v10
    a5 --> v11
    a5 --> v12
    v0 -.-> a5
    a6 --> v13
    a6 --> v0
    a7 --> v1
    a7 --> v24
    v0 -.-> a7
    a8 --> v25
    v0 -.-> a8
    v7 -.-> a8
    a9 --> v14
    a9 --> v25
    v0 -.-> a9
    v6 -.-> a9
```
