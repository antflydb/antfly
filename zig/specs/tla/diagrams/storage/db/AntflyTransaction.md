<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyTransaction — structural diagrams

Generated from [`AntflyTransaction.tla`](../../../storage/db/AntflyTransaction.tla). 9 state variables, 15 actions in `Next`.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `txnStatus`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> idle
    idle --> preparing : InitTransaction
    preparing --> predicatesChecked : CheckPredicates
    predicatesChecked --> committed : CommitTransaction
    aborting --> aborted : AbortTransaction
    predicatesChecked --> aborted : DirectAbort
    preparing --> aborted : DirectAbort
    aborted --> done : OrchestratorDone
    committed --> done : OrchestratorDone, OrchestratorCrash
    predicatesChecked --> done : OrchestratorCrashPrepare
    preparing --> done : OrchestratorCrashPrepare
    predicatesChecked --> aborting : WriteIntentFails
```

### `txnRecords`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> none
    pending --> aborted : RecoveryAutoAbort
    aborted --> deleted : CleanupTxnRecord
    committed --> deleted : CleanupTxnRecord
```

Writes whose source state is not statically determined:

- `AbortTransaction` sets `txnRecords` to `"aborted"`
- `CommitTransaction` sets `txnRecords` to `"committed"`
- `DirectAbort` sets `txnRecords` to `"aborted"`
- `InitTransaction` sets `txnRecords` to `"pending"`

### `intents`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> none
    none --> written : WriteIntentOnShard, ResolveIntentsOnShard, RecoveryResolve
    none --> resolved : ResolveIntentsOnShard, RecoveryResolve
    written --> resolved : ResolveIntentsOnShard, RecoveryResolve
    written --> written : ResolveIntentsOnShard, RecoveryResolve
```

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `TickClock` | `clock` | `clock` |
| `InitTransaction` | `clock`, `txnStatus`, `txnTimestamp`, `txnRecords` | `clock`, `txnStatus`, `txnTimestamp`, `txnRecords` |
| `CheckPredicates` | `txnStatus`, `dataStore`, `predicateSnapshot` | `txnStatus`, `predicateSnapshot` |
| `CommitTransaction` | `txnStatus`, `txnRecords`, `intentShards` | `txnStatus`, `txnRecords` |
| `AbortTransaction` | `txnStatus`, `txnRecords` | `txnStatus`, `txnRecords` |
| `DirectAbort` | `txnStatus`, `txnRecords` | `txnStatus`, `txnRecords` |
| `OrchestratorDone` | `txnStatus`, `intents` | `txnStatus` |
| `OrchestratorCrash` | `txnStatus`, `intents` | `txnStatus` |
| `OrchestratorCrashPrepare` | `txnStatus`, `txnRecords` | `txnStatus` |
| `RecoveryAutoAbort` | `clock`, `txnStatus`, `txnTimestamp`, `txnRecords` | `txnRecords` |
| `CleanupTxnRecord` | `txnRecords`, `resolvedParts`, `intents` | `txnRecords` |
| `WriteIntentOnShard` | `txnStatus`, `txnRecords`, `intents`, `dataStore`, `intentShards`, `predicateSnapshot` | `intents`, `intentShards` |
| `WriteIntentFails` | `txnStatus`, `txnRecords`, `intents`, `dataStore`, `predicateSnapshot` | `txnStatus` |
| `ResolveIntentsOnShard` | `txnTimestamp`, `txnRecords`, `resolvedParts`, `intents`, `dataStore` | `resolvedParts`, `intents`, `dataStore` |
| `RecoveryResolve` | `txnStatus`, `txnTimestamp`, `txnRecords`, `resolvedParts`, `intents`, `dataStore` | `resolvedParts`, `intents`, `dataStore` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[TickClock]
        a1[InitTransaction]
        a2[CheckPredicates]
        a3[CommitTransaction]
        a4[AbortTransaction]
        a5[DirectAbort]
        a6[OrchestratorDone]
        a7[OrchestratorCrash]
        a8[OrchestratorCrashPrepare]
        a9[RecoveryAutoAbort]
        a10[CleanupTxnRecord]
        a11[WriteIntentOnShard]
        a12[WriteIntentFails]
        a13[ResolveIntentsOnShard]
        a14[RecoveryResolve]
    end
    subgraph state["State variables"]
        v0([clock])
        v1([txnStatus])
        v2([txnTimestamp])
        v3([txnRecords])
        v4([dataStore])
        v5([predicateSnapshot])
        v6([intentShards])
        v7([intents])
        v8([resolvedParts])
    end
    a0 --> v0
    a1 --> v0
    a1 --> v1
    a1 --> v2
    a1 --> v3
    a2 --> v1
    a2 --> v5
    v4 -.-> a2
    a3 --> v1
    a3 --> v3
    v6 -.-> a3
    a4 --> v1
    a4 --> v3
    a5 --> v1
    a5 --> v3
    a6 --> v1
    v7 -.-> a6
    a7 --> v1
    v7 -.-> a7
    a8 --> v1
    v3 -.-> a8
    a9 --> v3
    v0 -.-> a9
    v1 -.-> a9
    v2 -.-> a9
    a10 --> v3
    v8 -.-> a10
    v7 -.-> a10
    a11 --> v7
    a11 --> v6
    v1 -.-> a11
    a12 --> v1
    v7 -.-> a12
    a13 --> v8
    a13 --> v7
    a13 --> v4
    v2 -.-> a13
    v3 -.-> a13
    a14 --> v8
    a14 --> v7
    a14 --> v4
    v1 -.-> a14
    v2 -.-> a14
    v3 -.-> a14
```
