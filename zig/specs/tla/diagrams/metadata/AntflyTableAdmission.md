<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyTableAdmission — structural diagrams

Generated from [`AntflyTableAdmission.tla`](../../metadata/AntflyTableAdmission.tla). 5 state variables, 5 actions in `Next`. 1 expected-failure mutant action(s) (gated by `Buggy*` constants) omitted.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `requestKind`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> none
    invalid --> none : RejectInvalid
    valid --> none : CommitValid
    classDef c_none fill:#2a78d630,stroke:#2a78d6
    class none c_none
    classDef c_valid fill:#eb683430,stroke:#eb6834
    class valid c_valid
    classDef c_invalid fill:#1baf7a30,stroke:#1baf7a
    class invalid c_invalid
```

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `BeginRequest` | `requestKind`, `desired`, `committed` | `requestKind`, `rejected`, `accepted` |
| `RejectInvalid` | `requestKind`, `desired`, `committed` | `requestKind`, `rejected` |
| `PersistValid` | `requestKind`, `desired` | `desired` |
| `CommitValid` | `requestKind`, `desired`, `committed` | `requestKind`, `committed`, `accepted` |
| `DropTable` | `desired`, `committed` | `desired`, `committed` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[BeginRequest]
        a1[RejectInvalid]
        a2[PersistValid]
        a3[CommitValid]
        a4[DropTable]
    end
    subgraph state["State variables"]
        v0([requestKind])
        v1([desired])
        v2([committed])
        v3([rejected])
        v4([accepted])
    end
    a0 --> v0
    a0 --> v3
    a0 --> v4
    v1 -.-> a0
    v2 -.-> a0
    a1 --> v0
    a1 --> v3
    v1 -.-> a1
    v2 -.-> a1
    a2 --> v1
    v0 -.-> a2
    a3 --> v0
    a3 --> v2
    a3 --> v4
    v1 -.-> a3
    a4 --> v1
    a4 --> v2
```
