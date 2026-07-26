<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyCdcCutover — structural diagrams

Generated from [`AntflyCdcCutover.tla`](../../metadata/AntflyCdcCutover.tla). 5 state variables, 5 actions in `Next`. 4 expected-failure mutant action(s) (gated by `Buggy*` constants) omitted.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `phase`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> snapshot
    snapshot --> stream : FinishSnapshot
    snapshot --> crashed : Crash
    stream --> crashed : Crash
    crashed --> snapshot : Resume
    crashed --> stream : Resume
    classDef c_snapshot fill:#2a78d630,stroke:#2a78d6
    class snapshot c_snapshot
    classDef c_stream fill:#eb683430,stroke:#eb6834
    class stream c_stream
    classDef c_crashed fill:#1baf7a30,stroke:#1baf7a
    class crashed c_crashed
```

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `DeliverSnapshotNext` | `phase`, `delivered`, `snapshotCursor` | `delivered`, `checkpoint`, `snapshotCursor` |
| `FinishSnapshot` | `phase`, `delivered` | `phase`, `streamCursor` |
| `DeliverStreamNext` | `phase`, `delivered`, `streamCursor` | `delivered`, `checkpoint`, `streamCursor` |
| `Crash` | `phase` | `phase`, `snapshotCursor`, `streamCursor` |
| `Resume` | `phase`, `checkpoint` | `phase`, `snapshotCursor`, `streamCursor` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[DeliverSnapshotNext]
        a1[FinishSnapshot]
        a2[DeliverStreamNext]
        a3[Crash]
        a4[Resume]
    end
    subgraph state["State variables"]
        v0([phase])
        v1([delivered])
        v2([checkpoint])
        v3([snapshotCursor])
        v4([streamCursor])
    end
    a0 --> v1
    a0 --> v2
    a0 --> v3
    v0 -.-> a0
    a1 --> v0
    a1 --> v4
    v1 -.-> a1
    a2 --> v1
    a2 --> v2
    a2 --> v4
    v0 -.-> a2
    a3 --> v0
    a3 --> v3
    a3 --> v4
    a4 --> v0
    a4 --> v3
    a4 --> v4
    v2 -.-> a4
```
