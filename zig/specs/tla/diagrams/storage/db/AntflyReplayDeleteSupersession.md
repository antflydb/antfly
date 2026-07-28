<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyReplayDeleteSupersession — structural diagrams

Generated from [`AntflyReplayDeleteSupersession.tla`](../../../storage/db/AntflyReplayDeleteSupersession.tla). 7 state variables, 7 actions in `Next`.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `documentState`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> absent
    absent --> live : WriteDocument
    live --> deleted : DeleteDocument
    classDef c_absent fill:#2a78d630,stroke:#2a78d6
    class absent c_absent
    classDef c_live fill:#eb683430,stroke:#eb6834
    class live c_live
    classDef c_deleted fill:#1baf7a30,stroke:#1baf7a
    class deleted c_deleted
```

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `WriteDocument` | `documentState`, `pendingUpsert` | `documentState`, `pendingUpsert` |
| `DeleteDocument` | `documentState`, `pendingDelete` | `documentState`, `pendingDelete` |
| `PublishUnknownMissingUpsert` | `documentState`, `pendingUpsert` | `pendingUpsert` |
| `ApplyLiveUpsert` | `documentState`, `pendingUpsert` | `pendingUpsert`, `indexVisible`, `appliedUpsert` |
| `ApplySupersededUpsert` | `documentState`, `pendingUpsert` | `pendingUpsert`, `indexVisible`, `appliedUpsert` |
| `RetryMissingUpsert` | `documentState`, `pendingUpsert`, `retryParity` | `retryParity` |
| `ApplyDelete` | `pendingUpsert`, `pendingDelete` | `pendingDelete`, `indexVisible`, `appliedDelete` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[WriteDocument]
        a1[DeleteDocument]
        a2[PublishUnknownMissingUpsert]
        a3[ApplyLiveUpsert]
        a4[ApplySupersededUpsert]
        a5[RetryMissingUpsert]
        a6[ApplyDelete]
    end
    subgraph state["State variables"]
        v0([documentState])
        v1([pendingUpsert])
        v2([pendingDelete])
        v3([indexVisible])
        v4([appliedUpsert])
        v5([retryParity])
        v6([appliedDelete])
    end
    a0 --> v0
    a0 --> v1
    a1 --> v0
    a1 --> v2
    a2 --> v1
    v0 -.-> a2
    a3 --> v1
    a3 --> v3
    a3 --> v4
    v0 -.-> a3
    a4 --> v1
    a4 --> v3
    a4 --> v4
    v0 -.-> a4
    a5 --> v5
    v0 -.-> a5
    v1 -.-> a5
    a6 --> v2
    a6 --> v3
    a6 --> v6
    v1 -.-> a6
```
