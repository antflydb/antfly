<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyDocumentIdentity — structural diagrams

Generated from [`AntflyDocumentIdentity.tla`](../../../storage/db/AntflyDocumentIdentity.tla). 16 state variables, 8 actions in `Next`. 3 expected-failure mutant action(s) (gated by `Buggy*` constants) omitted.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `ReassignNamespace` | `createdGeneration`, `storedNamespace`, `canonicalNamespace` | `storedNamespace`, `canonicalNamespace`, `filterAccepted`, `openAccepted` |
| `BuildWireFilter` | `currentGeneration`, `primaryLive`, `docOrdinal`, `storedNamespace` | `filterOrdinal`, `filterGeneration`, `filterNamespace`, `filterAccepted` |
| `UseWireFilter` | `currentGeneration`, `createdGeneration`, `deletedGeneration`, `storedNamespace`, `filterOrdinal`, `filterGeneration`, `filterNamespace` | `filterAccepted` |
| `OpenWithConfiguredNamespace` | `storedNamespace` | `openConfiguredNamespace`, `openAccepted` |
| `InsertNew` | `currentGeneration`, `nextOrdinal`, `primaryLive`, `docOrdinal`, `ordinalOwner`, `everOwner`, `createdGeneration`, `deletedGeneration`, `storedNamespace`, `canonicalNamespace` | `currentGeneration`, `nextOrdinal`, `primaryLive`, `docOrdinal`, `ordinalOwner`, `everOwner`, `createdGeneration`, `deletedGeneration`, `canonicalNamespace`, `filterAccepted`, `openAccepted` |
| `UpdateExisting` | `currentGeneration`, `primaryLive` | `currentGeneration`, `filterAccepted`, `openAccepted` |
| `DeleteDoc` | `currentGeneration`, `primaryLive`, `docOrdinal`, `deletedGeneration` | `currentGeneration`, `primaryLive`, `deletedGeneration`, `filterAccepted`, `openAccepted` |
| `ResurrectDoc` | `currentGeneration`, `primaryLive`, `docOrdinal`, `createdGeneration`, `deletedGeneration` | `currentGeneration`, `primaryLive`, `createdGeneration`, `deletedGeneration`, `filterAccepted`, `openAccepted` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[ReassignNamespace]
        a1[BuildWireFilter]
        a2[UseWireFilter]
        a3[OpenWithConfiguredNamespace]
        a4[InsertNew]
        a5[UpdateExisting]
        a6[DeleteDoc]
        a7[ResurrectDoc]
    end
    subgraph state["State variables"]
        v0([storedNamespace])
        v1([canonicalNamespace])
        v2([filterAccepted])
        v3([openAccepted])
        v4([currentGeneration])
        v5([primaryLive])
        v6([docOrdinal])
        v7([filterOrdinal])
        v8([filterGeneration])
        v9([filterNamespace])
        v10([openConfiguredNamespace])
        v11([nextOrdinal])
        v12([ordinalOwner])
        v13([everOwner])
        v14([createdGeneration])
        v15([deletedGeneration])
    end
    a0 --> v0
    a0 --> v1
    a0 --> v2
    a0 --> v3
    a1 --> v7
    a1 --> v8
    a1 --> v9
    a1 --> v2
    v4 -.-> a1
    v5 -.-> a1
    v6 -.-> a1
    v0 -.-> a1
    a2 --> v2
    v4 -.-> a2
    v0 -.-> a2
    v7 -.-> a2
    v8 -.-> a2
    v9 -.-> a2
    a3 --> v10
    a3 --> v3
    v0 -.-> a3
    a4 --> v4
    a4 --> v11
    a4 --> v5
    a4 --> v6
    a4 --> v12
    a4 --> v13
    a4 --> v14
    a4 --> v15
    a4 --> v1
    a4 --> v2
    a4 --> v3
    v0 -.-> a4
    a5 --> v4
    a5 --> v2
    a5 --> v3
    v5 -.-> a5
    a6 --> v4
    a6 --> v5
    a6 --> v15
    a6 --> v2
    a6 --> v3
    v6 -.-> a6
    a7 --> v4
    a7 --> v5
    a7 --> v14
    a7 --> v15
    a7 --> v2
    a7 --> v3
    v6 -.-> a7
```
