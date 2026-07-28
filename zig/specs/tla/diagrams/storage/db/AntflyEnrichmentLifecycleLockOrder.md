<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyEnrichmentLifecycleLockOrder — structural diagrams

Generated from [`AntflyEnrichmentLifecycleLockOrder.tla`](../../../storage/db/AntflyEnrichmentLifecycleLockOrder.tla). 5 state variables, 3 actions in `Next`.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `applyOwner`

Domain: `none`, `status`. No statically extractable guard/update transitions.

Writes whose source state is not statically determined:

- `StatusSnapshot` sets `applyOwner` to `"none"`

### `lifecycleOwner`

Domain: `none`, `delete`. No statically extractable guard/update transitions.

Writes whose source state is not statically determined:

- `DeleteFinishes` sets `lifecycleOwner` to `"none"`

### `workerPhase`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> waiting_apply
    waiting_apply --> done : WorkerPublishes
    classDef c_waiting_apply fill:#2a78d630,stroke:#2a78d6
    class waiting_apply c_waiting_apply
    classDef c_done fill:#eb683430,stroke:#eb6834
    class done c_done
```

### `deletePhase`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> waiting_worker
    waiting_worker --> done : DeleteFinishes
    classDef c_waiting_worker fill:#2a78d630,stroke:#2a78d6
    class waiting_worker c_waiting_worker
    classDef c_done fill:#eb683430,stroke:#eb6834
    class done c_done
```

### `statusPhase`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> waiting_lifecycle
    waiting_lifecycle --> done : StatusSnapshot
    classDef c_waiting_lifecycle fill:#2a78d630,stroke:#2a78d6
    class waiting_lifecycle c_waiting_lifecycle
    classDef c_done fill:#eb683430,stroke:#eb6834
    class done c_done
```

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `StatusSnapshot` | `lifecycleOwner`, `statusPhase` | `applyOwner`, `statusPhase` |
| `WorkerPublishes` | `applyOwner`, `workerPhase` | `workerPhase` |
| `DeleteFinishes` | `workerPhase`, `deletePhase` | `lifecycleOwner`, `deletePhase` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[StatusSnapshot]
        a1[WorkerPublishes]
        a2[DeleteFinishes]
    end
    subgraph state["State variables"]
        v0([applyOwner])
        v1([lifecycleOwner])
        v2([statusPhase])
        v3([workerPhase])
        v4([deletePhase])
    end
    a0 --> v0
    a0 --> v2
    v1 -.-> a0
    a1 --> v3
    v0 -.-> a1
    a2 --> v1
    a2 --> v4
    v3 -.-> a2
```
