<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflySnapshotTransfer — structural diagrams

Generated from [`AntflySnapshotTransfer.tla`](../../raft/AntflySnapshotTransfer.tla). 9 state variables, 10 actions in `Next`. 1 expected-failure mutant action(s) (gated by `Buggy*` constants) omitted.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `transferState`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> idle
    idle --> fetching : RaftSendsSnapshot
    fetching --> done : TransferSucceeds
    fetching --> failed : TransferPermanentFailure, TransferExhaustedRetries
    done --> idle : ApplySnapshot
    classDef c_idle fill:#2a78d630,stroke:#2a78d6
    class idle c_idle
    classDef c_fetching fill:#eb683430,stroke:#eb6834
    class fetching c_fetching
    classDef c_done fill:#1baf7a30,stroke:#1baf7a
    class done c_done
    classDef c_failed fill:#eda10030,stroke:#eda100
    class failed c_failed
```

Writes whose source state is not statically determined:

- `NodeCrash` sets `transferState` to `"idle"`

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `ElectLeader` | `nodeUp` | `leader` |
| `CreateSnapshot` | `leader`, `snapCounter`, `persistedSnap`, `snapStore`, `transferState`, `nodeUp` | `snapCounter`, `persistedSnap`, `snapStore` |
| `RaftSendsSnapshot` | `leader`, `persistedSnap`, `targetSnap`, `needsSnap`, `transferState`, `retryCount`, `nodeUp` | `targetSnap`, `needsSnap`, `transferState`, `retryCount` |
| `TransferSucceeds` | `targetSnap`, `snapStore`, `needsSnap`, `transferState`, `nodeUp` | `snapStore`, `needsSnap`, `transferState` |
| `TransferRetry` | `targetSnap`, `snapStore`, `transferState`, `retryCount`, `nodeUp` | `retryCount` |
| `TransferPermanentFailure` | `targetSnap`, `snapStore`, `needsSnap`, `transferState`, `nodeUp` | `needsSnap`, `transferState` |
| `TransferExhaustedRetries` | `targetSnap`, `snapStore`, `needsSnap`, `transferState`, `retryCount`, `nodeUp` | `needsSnap`, `transferState` |
| `ApplySnapshot` | `persistedSnap`, `targetSnap`, `snapStore`, `transferState`, `nodeUp` | `persistedSnap`, `targetSnap`, `transferState` |
| `NodeCrash` | `leader`, `targetSnap`, `needsSnap`, `transferState`, `retryCount`, `nodeUp` | `leader`, `targetSnap`, `needsSnap`, `transferState`, `retryCount`, `nodeUp` |
| `NodeRestart` | `nodeUp` | `nodeUp` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[ElectLeader]
        a1[CreateSnapshot]
        a2[RaftSendsSnapshot]
        a3[TransferSucceeds]
        a4[TransferRetry]
        a5[TransferPermanentFailure]
        a6[TransferExhaustedRetries]
        a7[ApplySnapshot]
        a8[NodeCrash]
        a9[NodeRestart]
    end
    subgraph state["State variables"]
        v0([leader])
        v1([nodeUp])
        v2([snapCounter])
        v3([persistedSnap])
        v4([snapStore])
        v5([transferState])
        v6([targetSnap])
        v7([needsSnap])
        v8([retryCount])
    end
    a0 --> v0
    v1 -.-> a0
    a1 --> v2
    a1 --> v3
    a1 --> v4
    v0 -.-> a1
    v5 -.-> a1
    v1 -.-> a1
    a2 --> v6
    a2 --> v7
    a2 --> v5
    a2 --> v8
    v0 -.-> a2
    v3 -.-> a2
    v1 -.-> a2
    a3 --> v4
    a3 --> v7
    a3 --> v5
    v6 -.-> a3
    v1 -.-> a3
    a4 --> v8
    v6 -.-> a4
    v4 -.-> a4
    v5 -.-> a4
    v1 -.-> a4
    a5 --> v7
    a5 --> v5
    v6 -.-> a5
    v4 -.-> a5
    v1 -.-> a5
    a6 --> v7
    a6 --> v5
    v6 -.-> a6
    v4 -.-> a6
    v8 -.-> a6
    v1 -.-> a6
    a7 --> v3
    a7 --> v6
    a7 --> v5
    v4 -.-> a7
    v1 -.-> a7
    a8 --> v0
    a8 --> v6
    a8 --> v7
    a8 --> v5
    a8 --> v8
    a8 --> v1
    a9 --> v1
```
