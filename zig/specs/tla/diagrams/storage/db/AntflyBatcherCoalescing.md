<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyBatcherCoalescing — structural diagrams

Generated from [`AntflyBatcherCoalescing.tla`](../../../storage/db/AntflyBatcherCoalescing.tla). 5 state variables, 3 actions in `Next`.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `opLog`

Domain: `delete`, `write`, `none`. No statically extractable guard/update transitions.

### `durableValue`

Domain: `none`, `deleted`, `written`, `partial`. No statically extractable guard/update transitions.

Writes whose source state is not statically determined:

- `BeginFlush` sets `durableValue` to `"partial"`

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `Enqueue` | `enqueuedThrough`, `opLog` | `enqueuedThrough`, `opLog` |
| `BeginFlush` | `enqueuedThrough`, `flushing`, `durableValue`, `flushedThrough` | `flushing`, `durableValue` |
| `FinishFlush` | `enqueuedThrough`, `opLog`, `flushing` | `flushing`, `durableValue`, `flushedThrough` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[Enqueue]
        a1[BeginFlush]
        a2[FinishFlush]
    end
    subgraph state["State variables"]
        v0([enqueuedThrough])
        v1([opLog])
        v2([flushing])
        v3([durableValue])
        v4([flushedThrough])
    end
    a0 --> v0
    a0 --> v1
    a1 --> v2
    a1 --> v3
    v0 -.-> a1
    v4 -.-> a1
    a2 --> v2
    a2 --> v3
    a2 --> v4
    v0 -.-> a2
```
