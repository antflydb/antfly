<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyHARejoin — structural diagrams

Generated from [`AntflyHARejoin.tla`](../../../storage/ha/AntflyHARejoin.tla). 22 state variables, 4 actions in `Next`.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `timelineState`

Domain: `parent`, `new`, `other`. No statically extractable guard/update transitions.

### `action`

Domain: `none`, `reject_unfenced`, `already_current`, `rewind`, `reseed`. No statically extractable guard/update transitions.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `Assess` | `assessed` | `assessed`, `hasFence`, `identityMatches`, `oldPrimaryMatches`, `timelineState`, `formerLastLsn`, `forkLsn`, `retainedFromLsn`, `forcedPromotion`, `allowForcedRewind`, `action`, `targetTimeline`, `targetEpoch`, `dataLossDiscarded`, `logLastLsn`, `logCurrentLastLsn`, `forkRecordPresent`, `forkRecordMatches` |
| `LateFormerPrimaryWrite` | `assessed`, `logLastLsn`, `executed` | `logLastLsn`, `logCurrentLastLsn` |
| `ExecuteRewind` | `assessed`, `formerLastLsn`, `forkLsn`, `retainedFromLsn`, `action`, `logLastLsn`, `forkRecordPresent`, `forkRecordMatches`, `executed` | `logCurrentLastLsn`, `executed`, `executionPreviousLastLsn` |
| `ExecuteReseed` | `assessed`, `action`, `executed` | `executed`, `reseedRequired`, `baseBackupRequired` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[Assess]
        a1[LateFormerPrimaryWrite]
        a2[ExecuteRewind]
        a3[ExecuteReseed]
    end
    subgraph state["State variables"]
        v0([assessed])
        v1([hasFence])
        v2([identityMatches])
        v3([oldPrimaryMatches])
        v4([timelineState])
        v5([formerLastLsn])
        v6([forkLsn])
        v7([retainedFromLsn])
        v8([forcedPromotion])
        v9([allowForcedRewind])
        v10([action])
        v11([targetTimeline])
        v12([targetEpoch])
        v13([dataLossDiscarded])
        v14([logLastLsn])
        v15([logCurrentLastLsn])
        v16([forkRecordPresent])
        v17([forkRecordMatches])
        v18([executed])
        v19([executionPreviousLastLsn])
        v20([reseedRequired])
        v21([baseBackupRequired])
    end
    a0 --> v0
    a0 --> v1
    a0 --> v2
    a0 --> v3
    a0 --> v4
    a0 --> v5
    a0 --> v6
    a0 --> v7
    a0 --> v8
    a0 --> v9
    a0 --> v10
    a0 --> v11
    a0 --> v12
    a0 --> v13
    a0 --> v14
    a0 --> v15
    a0 --> v16
    a0 --> v17
    a1 --> v14
    a1 --> v15
    v0 -.-> a1
    v18 -.-> a1
    a2 --> v15
    a2 --> v18
    a2 --> v19
    v0 -.-> a2
    v6 -.-> a2
    v14 -.-> a2
    a3 --> v18
    a3 --> v20
    a3 --> v21
    v0 -.-> a3
    v10 -.-> a3
```
