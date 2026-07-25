<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyTableLifecycle — structural diagrams

Generated from [`AntflyTableLifecycle.tla`](../../metadata/AntflyTableLifecycle.tla). 8 state variables, 11 actions in `Next`. 1 expected-failure mutant action(s) (gated by `Buggy*` constants) omitted.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `OpCreate` | `dTable`, `opBudget` | `dTable`, `dRanges`, `opBudget` |
| `OpDrop` | `dTable`, `opBudget` | `dTable`, `dRanges`, `opBudget` |
| `ApplyUpsertTable` | `dTable`, `cTable` | `cTable` |
| `ApplyRemoveTable` | `dTable`, `cTable` | `cTable` |
| `CrashRestart` | `cTable`, `cRanges` | `dTable`, `dRanges` |
| `ApplyUpsertRange` | `dRanges`, `cRanges` | `cRanges` |
| `ApplyRemoveRange` | `dRanges`, `cRanges` | `cRanges` |
| `PlanIntent` | `dTable`, `dRanges`, `cRanges`, `cIntents`, `intentPlannedUndesired` | `cIntents`, `intentPlannedUndesired` |
| `RemoveIntent` | `dRanges`, `cIntents` | `cIntents` |
| `GroupUp` | `cIntents`, `groupsLive` | `groupsLive` |
| `GroupDown` | `cIntents`, `groupsLive` | `groupsLive` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[OpCreate]
        a1[OpDrop]
        a2[ApplyUpsertTable]
        a3[ApplyRemoveTable]
        a4[CrashRestart]
        a5[ApplyUpsertRange]
        a6[ApplyRemoveRange]
        a7[PlanIntent]
        a8[RemoveIntent]
        a9[GroupUp]
        a10[GroupDown]
    end
    subgraph state["State variables"]
        v0([dTable])
        v1([dRanges])
        v2([opBudget])
        v3([cTable])
        v4([cRanges])
        v5([cIntents])
        v6([intentPlannedUndesired])
        v7([groupsLive])
    end
    a0 --> v0
    a0 --> v1
    a0 --> v2
    a1 --> v0
    a1 --> v1
    a1 --> v2
    a2 --> v3
    v0 -.-> a2
    a3 --> v3
    v0 -.-> a3
    a4 --> v0
    a4 --> v1
    v3 -.-> a4
    v4 -.-> a4
    a5 --> v4
    v1 -.-> a5
    a6 --> v4
    v1 -.-> a6
    a7 --> v5
    a7 --> v6
    v0 -.-> a7
    v1 -.-> a7
    v4 -.-> a7
    a8 --> v5
    v1 -.-> a8
    a9 --> v7
    v5 -.-> a9
    a10 --> v7
    v5 -.-> a10
```
