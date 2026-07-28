<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyRepairArtifactRecovery — structural diagrams

Generated from [`AntflyRepairArtifactRecovery.tla`](../../../storage/db/AntflyRepairArtifactRecovery.tla). 7 state variables, 7 actions in `Next`.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `repairPhase`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> building
    building --> coverage_failed : ObserveCoverageFailure
    coverage_failed --> retry_wait : ResetCandidate
    retry_wait --> building : RetryBuild
    building --> ready : Activate
    classDef c_building fill:#2a78d630,stroke:#2a78d6
    class building c_building
    classDef c_coverage_failed fill:#eb683430,stroke:#eb6834
    class coverage_failed c_coverage_failed
    classDef c_retry_wait fill:#1baf7a30,stroke:#1baf7a
    class retry_wait c_retry_wait
    classDef c_ready fill:#eda10030,stroke:#eda100
    class ready c_ready
```

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `ObserveCoverageFailure` | `candidateComplete`, `repairPhase` | `repairPhase` |
| `QueueArtifactRecovery` | `artifactQueued`, `repairPhase` | `artifactQueued` |
| `DrainArtifactRecoveryForeground` | `artifactQueued`, `artifactValid`, `repairPhase` | `artifactValid` |
| `ResetCandidate` | `artifactValid`, `repairPhase` | `candidateExists`, `repairPhase` |
| `RetryBuild` | `artifactValid`, `candidateExists`, `candidateComplete`, `repairPhase` | `candidateExists`, `candidateComplete`, `repairPhase` |
| `Activate` | `candidateComplete`, `repairPhase` | `repairPhase` |
| `CheckStartupPlanner` | `repairPhase`, `startupPlanChecked` | `startupPlanChecked`, `startupPlanClean` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[ObserveCoverageFailure]
        a1[QueueArtifactRecovery]
        a2[DrainArtifactRecoveryForeground]
        a3[ResetCandidate]
        a4[RetryBuild]
        a5[Activate]
        a6[CheckStartupPlanner]
    end
    subgraph state["State variables"]
        v0([candidateComplete])
        v1([repairPhase])
        v2([artifactQueued])
        v3([artifactValid])
        v4([candidateExists])
        v5([startupPlanChecked])
        v6([startupPlanClean])
    end
    a0 --> v1
    v0 -.-> a0
    a1 --> v2
    v1 -.-> a1
    a2 --> v3
    v2 -.-> a2
    v1 -.-> a2
    a3 --> v4
    a3 --> v1
    v3 -.-> a3
    a4 --> v4
    a4 --> v0
    a4 --> v1
    v3 -.-> a4
    a5 --> v1
    v0 -.-> a5
    a6 --> v5
    a6 --> v6
    v1 -.-> a6
```
