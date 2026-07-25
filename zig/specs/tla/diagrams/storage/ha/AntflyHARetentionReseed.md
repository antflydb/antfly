<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyHARetentionReseed — structural diagrams

Generated from [`AntflyHARetentionReseed.tla`](../../../storage/ha/AntflyHARetentionReseed.tla). 8 state variables, 8 actions in `Next`. 1 expected-failure mutant action(s) (gated by `Buggy*` constants) omitted.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `Append` | `primaryLsn` | `primaryLsn` |
| `Truncate` | `primaryLsn`, `slotActive`, `reseedMarked`, `restartLsn`, `truncatedBelow` | `truncatedBelow` |
| `BeginBackup` | `primaryLsn`, `slotActive`, `reseedMarked`, `restartLsn`, `backupInFlight` | `slotActive`, `reseedMarked`, `restartLsn`, `backupInFlight`, `backupLsn` |
| `EndBackupOk` | `slotActive`, `reseedMarked`, `truncatedBelow`, `backupInFlight`, `backupLsn` | `slotActive`, `backupInFlight` |
| `EndBackupFailed` | `slotActive`, `reseedMarked`, `truncatedBelow`, `backupInFlight`, `backupLsn` | `slotActive`, `reseedMarked`, `backupInFlight` |
| `MarkReseed` | `primaryLsn`, `slotActive`, `reseedMarked`, `restartLsn` | `reseedMarked` |
| `DropSlot` | `slotActive`, `reseedMarked` | `slotActive` |
| `AdvanceSlot` | `primaryLsn`, `slotActive`, `reseedMarked`, `restartLsn` | `restartLsn` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[Append]
        a1[Truncate]
        a2[BeginBackup]
        a3[EndBackupOk]
        a4[EndBackupFailed]
        a5[MarkReseed]
        a6[DropSlot]
        a7[AdvanceSlot]
    end
    subgraph state["State variables"]
        v0([primaryLsn])
        v1([truncatedBelow])
        v2([slotActive])
        v3([reseedMarked])
        v4([restartLsn])
        v5([backupInFlight])
        v6([backupLsn])
    end
    a0 --> v0
    a1 --> v1
    a2 --> v2
    a2 --> v3
    a2 --> v4
    a2 --> v5
    a2 --> v6
    v0 -.-> a2
    a3 --> v2
    a3 --> v5
    v3 -.-> a3
    v1 -.-> a3
    v6 -.-> a3
    a4 --> v2
    a4 --> v3
    a4 --> v5
    v1 -.-> a4
    v6 -.-> a4
    a5 --> v3
    v0 -.-> a5
    v2 -.-> a5
    v4 -.-> a5
    a6 --> v2
    v3 -.-> a6
    a7 --> v4
    v0 -.-> a7
    v2 -.-> a7
    v3 -.-> a7
```
