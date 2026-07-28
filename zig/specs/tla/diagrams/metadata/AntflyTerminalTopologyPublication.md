<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyTerminalTopologyPublication — structural diagrams

Generated from [`AntflyTerminalTopologyPublication.tla`](../../metadata/AntflyTerminalTopologyPublication.tla). 4 state variables, 5 actions in `Next`. 1 expected-failure mutant action(s) (gated by `Buggy*` constants) omitted.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `transitionPhase`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> active
    active --> finalized : PublishFinalizedTransition
    finalized --> none : CompactTerminalTransition
    classDef c_active fill:#2a78d630,stroke:#2a78d6
    class active c_active
    classDef c_finalized fill:#eb683430,stroke:#eb6834
    class finalized c_finalized
    classDef c_none fill:#1baf7a30,stroke:#1baf7a
    class none c_none
```

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `CompleteShardCutover` | `shardCutoverComplete`, `transitionPhase` | `shardCutoverComplete` |
| `PublishFinalizedTransition` | `shardCutoverComplete`, `transitionPhase` | `transitionPhase` |
| `FoldFinalizedTopology` | `transitionPhase`, `desiredIntent` | `catalogRangeCount`, `desiredIntent` |
| `CompactTerminalTransition` | `transitionPhase`, `desiredIntent` | `transitionPhase` |
| `TerminalIdle` | `transitionPhase` | — |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[CompleteShardCutover]
        a1[PublishFinalizedTransition]
        a2[FoldFinalizedTopology]
        a3[CompactTerminalTransition]
        a4[TerminalIdle]
    end
    subgraph state["State variables"]
        v0([shardCutoverComplete])
        v1([transitionPhase])
        v2([catalogRangeCount])
        v3([desiredIntent])
    end
    a0 --> v0
    v1 -.-> a0
    a1 --> v1
    v0 -.-> a1
    a2 --> v2
    a2 --> v3
    v1 -.-> a2
    a3 --> v1
    v3 -.-> a3
    v1 -.-> a4
```
