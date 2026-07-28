<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyLeaseRetryBackoff — structural diagrams

Generated from [`AntflyLeaseRetryBackoff.tla`](../../../storage/db/AntflyLeaseRetryBackoff.tla). 5 state variables, 4 actions in `Next`.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `AcquireDenied` | `foreignLease`, `coolingDown`, `attempts`, `acquired` | `coolingDown`, `attempts` |
| `WaitRetry` | `coolingDown`, `waits` | `coolingDown`, `waits` |
| `LeaseExpires` | `foreignLease` | `foreignLease` |
| `AcquireSuccess` | `foreignLease`, `coolingDown`, `acquired` | `acquired` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[AcquireDenied]
        a1[WaitRetry]
        a2[LeaseExpires]
        a3[AcquireSuccess]
    end
    subgraph state["State variables"]
        v0([foreignLease])
        v1([coolingDown])
        v2([attempts])
        v3([acquired])
        v4([waits])
    end
    a0 --> v1
    a0 --> v2
    v0 -.-> a0
    v3 -.-> a0
    a1 --> v1
    a1 --> v4
    a2 --> v0
    a3 --> v3
    v0 -.-> a3
    v1 -.-> a3
```
