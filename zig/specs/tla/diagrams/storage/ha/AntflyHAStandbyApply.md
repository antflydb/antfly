<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyHAStandbyApply — structural diagrams

Generated from [`AntflyHAStandbyApply.tla`](../../../storage/ha/AntflyHAStandbyApply.tla). 11 state variables, 5 actions in `Next`. 2 expected-failure mutant action(s) (gated by `Buggy*` constants) omitted.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `ReceiveRecord` | `receivedLsn` | `receivedLsn` |
| `ApplyNextSuccess` | `receivedLsn`, `appliedLsn`, `effectCount`, `failedPending`, `failedLsn` | `appliedLsn`, `safeReadLsn`, `haMarkerLsn`, `effectCount`, `failedPending`, `failedLsn` |
| `ApplyNextFailure` | `receivedLsn`, `appliedLsn`, `failedPending` | `appliedLsn`, `safeReadLsn`, `haMarkerLsn`, `failedPending`, `failedLsn` |
| `DuplicateApplyAlreadyMarked` | `haMarkerLsn`, `effectCount` | `effectCount` |
| `CrashAndReopen` | `receivedLsn`, `appliedLsn`, `crashed` | `receivedLsn`, `crashed`, `crashReceivedSnapshot` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[ReceiveRecord]
        a1[ApplyNextSuccess]
        a2[ApplyNextFailure]
        a3[DuplicateApplyAlreadyMarked]
        a4[CrashAndReopen]
    end
    subgraph state["State variables"]
        v0([receivedLsn])
        v1([appliedLsn])
        v2([safeReadLsn])
        v3([haMarkerLsn])
        v4([effectCount])
        v5([failedPending])
        v6([failedLsn])
        v7([crashed])
        v8([crashReceivedSnapshot])
    end
    a0 --> v0
    a1 --> v1
    a1 --> v2
    a1 --> v3
    a1 --> v4
    a1 --> v5
    a1 --> v6
    v0 -.-> a1
    a2 --> v1
    a2 --> v2
    a2 --> v3
    a2 --> v5
    a2 --> v6
    v0 -.-> a2
    a3 --> v4
    v3 -.-> a3
    a4 --> v0
    a4 --> v7
    a4 --> v8
    v1 -.-> a4
```
