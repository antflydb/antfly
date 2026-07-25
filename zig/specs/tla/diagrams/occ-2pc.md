<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# model — structural diagrams

Generated from [`occ-2pc.tla`](../occ-2pc.tla). 7 state variables, 13 actions in `Next`.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `orchestratorState`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> none
    none --> inited : InitTransaction
    inited --> intentsWritten : IntentsComplete
    inited --> aborted : AbortAfterIntentFailure
    intentsWritten --> committed : CommitTransaction
    committed --> resolved : ResolutionComplete
```

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `InitTransaction` | `txnRecords`, `orchestratorState` | `txnRecords`, `orchestratorState` |
| `CheckPredicates` | `keyVersions`, `orchestratorState`, `predicateSnapshot` | `predicateSnapshot` |
| `WriteIntents` | `txnRecords`, `intents`, `keyVersions`, `orchestratorState`, `predicateSnapshot` | `intents` |
| `IntentsComplete` | `intents`, `orchestratorState` | `orchestratorState` |
| `AbortAfterIntentFailure` | `txnRecords`, `intents`, `orchestratorState` | `txnRecords`, `orchestratorState` |
| `CommitTransaction` | `txnRecords`, `orchestratorState` | `txnRecords`, `orchestratorState` |
| `ResolveIntents` | `txnRecords`, `intents`, `keyVersions`, `versionCounter` | `txnRecords`, `intents`, `keyVersions`, `versionCounter` |
| `ResolutionComplete` | `intents`, `orchestratorState` | `orchestratorState` |
| `CleanupAbortedIntents` | `txnRecords`, `intents` | `intents` |
| `RecoveryNotify` | `txnRecords`, `intents`, `keyVersions`, `versionCounter`, `recoveryActive` | `txnRecords`, `intents`, `keyVersions`, `versionCounter` |
| `BecomeLeader` | `recoveryActive` | `recoveryActive` |
| `LoseLeadership` | `recoveryActive` | `recoveryActive` |
| `ExternalWrite` | `keyVersions`, `versionCounter` | `keyVersions`, `versionCounter` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[InitTransaction]
        a1[CheckPredicates]
        a2[WriteIntents]
        a3[IntentsComplete]
        a4[AbortAfterIntentFailure]
        a5[CommitTransaction]
        a6[ResolveIntents]
        a7[ResolutionComplete]
        a8[CleanupAbortedIntents]
        a9[RecoveryNotify]
        a10[BecomeLeader]
        a11[LoseLeadership]
        a12[ExternalWrite]
    end
    subgraph state["State variables"]
        v0([txnRecords])
        v1([orchestratorState])
        v2([keyVersions])
        v3([predicateSnapshot])
        v4([intents])
        v5([versionCounter])
        v6([recoveryActive])
    end
    a0 --> v0
    a0 --> v1
    a1 --> v3
    v2 -.-> a1
    v1 -.-> a1
    a2 --> v4
    v0 -.-> a2
    v2 -.-> a2
    v1 -.-> a2
    v3 -.-> a2
    a3 --> v1
    v4 -.-> a3
    a4 --> v0
    a4 --> v1
    v4 -.-> a4
    a5 --> v0
    a5 --> v1
    a6 --> v0
    a6 --> v4
    a6 --> v2
    a6 --> v5
    a7 --> v1
    v4 -.-> a7
    a8 --> v4
    v0 -.-> a8
    a9 --> v0
    a9 --> v4
    a9 --> v2
    a9 --> v5
    v6 -.-> a9
    a10 --> v6
    a11 --> v6
    a12 --> v2
    a12 --> v5
```
