<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyMlGraphDagPasses — structural diagrams

Generated from [`AntflyMlGraphDagPasses.tla`](../../ml/AntflyMlGraphDagPasses.tla). 14 state variables, 2 actions in `Next`.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `phase`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> init
    init --> cse : RunCSE
    cse --> dce : RunDCE
```

### `kind`

Domain: `param`, `const`, `opA`, `opB`, `opC`. No statically extractable guard/update transitions.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `RunCSE` | `phase`, `kind`, `inputs`, `outputs`, `parameters` | `phase`, `cseMap`, `postInputs`, `postOutputs`, `postParameters` |
| `RunDCE` | `phase`, `parameters`, `postInputs`, `postOutputs`, `postParameters` | `phase`, `dceLive`, `dceMap`, `finalInputs`, `finalOutputs`, `finalParameters` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[RunCSE]
        a1[RunDCE]
    end
    subgraph state["State variables"]
        v0([phase])
        v1([inputs])
        v2([outputs])
        v3([parameters])
        v4([cseMap])
        v5([postInputs])
        v6([postOutputs])
        v7([postParameters])
        v8([dceLive])
        v9([dceMap])
        v10([finalInputs])
        v11([finalOutputs])
        v12([finalParameters])
    end
    a0 --> v0
    a0 --> v4
    a0 --> v5
    a0 --> v6
    a0 --> v7
    v1 -.-> a0
    v2 -.-> a0
    v3 -.-> a0
    a1 --> v0
    a1 --> v8
    a1 --> v9
    a1 --> v10
    a1 --> v11
    a1 --> v12
    v3 -.-> a1
    v5 -.-> a1
    v6 -.-> a1
    v7 -.-> a1
```
