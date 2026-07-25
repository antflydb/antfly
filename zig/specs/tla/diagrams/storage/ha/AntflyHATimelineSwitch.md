<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyHATimelineSwitch — structural diagrams

Generated from [`AntflyHATimelineSwitch.tla`](../../../storage/ha/AntflyHATimelineSwitch.tla). 15 state variables, 7 actions in `Next`. 1 expected-failure mutant action(s) (gated by `Buggy*` constants) omitted.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `ReceiveParentRecord` | `receivedLsn`, `durableSwitchPresent`, `switchReceived` | `receivedLsn` |
| `ApplyParentRecord` | `receivedLsn`, `appliedLsn`, `switchReceived` | `appliedLsn` |
| `AdvanceSafeRead` | `appliedLsn`, `safeReadLsn`, `switchReceived` | `safeReadLsn` |
| `ReceiveTimelineSwitch` | `receivedLsn`, `appliedLsn`, `safeReadLsn`, `durableSwitchPresent`, `switchReceived` | `identityTimeline`, `identityEpoch`, `receivedLsn`, `appliedLsn`, `safeReadLsn`, `durableSwitchPresent`, `switchTimeline`, `switchEpoch`, `switchLsn`, `switchPreviousLsn`, `switchAppliedAtAppend`, `switchReceived` |
| `DurableSwitchBeforeProgressPersisted` | `receivedLsn`, `appliedLsn`, `safeReadLsn`, `durableSwitchPresent`, `switchReceived` | `durableSwitchPresent`, `switchTimeline`, `switchEpoch`, `switchLsn`, `switchPreviousLsn`, `switchAppliedAtAppend` |
| `RecoverDurableTimelineSwitch` | `receivedLsn`, `appliedLsn`, `safeReadLsn`, `durableSwitchPresent`, `switchTimeline`, `switchEpoch`, `switchLsn`, `switchPreviousLsn`, `switchReceived` | `identityTimeline`, `identityEpoch`, `receivedLsn`, `appliedLsn`, `safeReadLsn`, `switchReceived`, `recoveryUsed`, `recoveredFromReceived` |
| `ReceiveCurrentTimelineRecord` | `receivedLsn`, `switchReceived` | `receivedLsn` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[ReceiveParentRecord]
        a1[ApplyParentRecord]
        a2[AdvanceSafeRead]
        a3[ReceiveTimelineSwitch]
        a4[DurableSwitchBeforeProgressPersisted]
        a5[RecoverDurableTimelineSwitch]
        a6[ReceiveCurrentTimelineRecord]
    end
    subgraph state["State variables"]
        v0([receivedLsn])
        v1([durableSwitchPresent])
        v2([switchReceived])
        v3([appliedLsn])
        v4([safeReadLsn])
        v5([identityTimeline])
        v6([identityEpoch])
        v7([switchTimeline])
        v8([switchEpoch])
        v9([switchLsn])
        v10([switchPreviousLsn])
        v11([switchAppliedAtAppend])
        v12([recoveryUsed])
        v13([recoveredFromReceived])
    end
    a0 --> v0
    v1 -.-> a0
    v2 -.-> a0
    a1 --> v3
    v0 -.-> a1
    v2 -.-> a1
    a2 --> v4
    v3 -.-> a2
    v2 -.-> a2
    a3 --> v5
    a3 --> v6
    a3 --> v0
    a3 --> v3
    a3 --> v4
    a3 --> v1
    a3 --> v7
    a3 --> v8
    a3 --> v9
    a3 --> v10
    a3 --> v11
    a3 --> v2
    a4 --> v1
    a4 --> v7
    a4 --> v8
    a4 --> v9
    a4 --> v10
    a4 --> v11
    v0 -.-> a4
    v2 -.-> a4
    a5 --> v5
    a5 --> v6
    a5 --> v0
    a5 --> v3
    a5 --> v4
    a5 --> v2
    a5 --> v12
    a5 --> v13
    v1 -.-> a5
    v7 -.-> a5
    v8 -.-> a5
    v9 -.-> a5
    v10 -.-> a5
    a6 --> v0
    v2 -.-> a6
```
