<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# etcdraft — structural diagrams

Generated from [`etcdraft.tla`](../etcdraft.tla). 14 state variables, 13 actions in `Next`.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `RequestVote` | `pendingMessages`, `currentTerm`, `state`, `log`, `votesResponded`, `config` | `pendingMessages` |
| `BecomeLeader` | `state`, `log`, `votesGranted`, `matchIndex`, `config` | `state`, `matchIndex` |
| `ClientRequest` | `currentTerm`, `state`, `log` | `log` |
| `AdvanceCommitIndex` | `currentTerm`, `state`, `log`, `commitIndex`, `matchIndex`, `config` | `commitIndex` |
| `AppendEntriesInRangeToPeer` | `pendingMessages`, `currentTerm`, `state`, `log`, `commitIndex`, `matchIndex`, `config` | `pendingMessages` |
| `AppendEntriesToSelf` | `pendingMessages`, `currentTerm`, `state`, `log` | `pendingMessages` |
| `ReceiveDirect` | `messages`, `pendingMessages`, `currentTerm`, `state`, `votedFor`, `log`, `commitIndex`, `votesResponded`, `votesGranted`, `matchIndex` | `messages`, `pendingMessages`, `currentTerm`, `state`, `votedFor`, `log`, `commitIndex`, `votesResponded`, `votesGranted`, `matchIndex` |
| `Timeout` | `currentTerm`, `state`, `votedFor`, `votesResponded`, `votesGranted`, `config` | `currentTerm`, `state`, `votedFor`, `votesResponded`, `votesGranted` |
| `Ready` | `messages`, `pendingMessages`, `currentTerm`, `votedFor`, `log`, `commitIndex`, `config`, `durableState` | `messages`, `pendingMessages`, `durableState` |
| `StepDownToFollower` | `currentTerm`, `state`, `votedFor` | `currentTerm`, `state`, `votedFor` |
| `Restart` | `pendingMessages`, `currentTerm`, `state`, `votedFor`, `log`, `commitIndex`, `votesResponded`, `votesGranted`, `matchIndex`, `pendingConfChangeIndex`, `config`, `durableState` | `pendingMessages`, `currentTerm`, `state`, `votedFor`, `log`, `commitIndex`, `votesResponded`, `votesGranted`, `matchIndex`, `pendingConfChangeIndex`, `config` |
| `DuplicateMessage` | `messages` | `messages` |
| `DropMessage` | `messages` | `messages` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[RequestVote]
        a1[BecomeLeader]
        a2[ClientRequest]
        a3[AdvanceCommitIndex]
        a4[AppendEntriesInRangeToPeer]
        a5[AppendEntriesToSelf]
        a6[ReceiveDirect]
        a7[Timeout]
        a8[Ready]
        a9[StepDownToFollower]
        a10[Restart]
        a11[DuplicateMessage]
        a12[DropMessage]
    end
    subgraph state["State variables"]
        v0([pendingMessages])
        v1([currentTerm])
        v2([state])
        v3([log])
        v4([votesResponded])
        v5([votesGranted])
        v6([matchIndex])
        v7([commitIndex])
        v8([messages])
        v9([votedFor])
        v10([durableState])
        v11([pendingConfChangeIndex])
        v12([config])
    end
    a0 --> v0
    v1 -.-> a0
    v2 -.-> a0
    v3 -.-> a0
    v4 -.-> a0
    a1 --> v2
    a1 --> v6
    v3 -.-> a1
    v5 -.-> a1
    a2 --> v3
    a3 --> v7
    v1 -.-> a3
    v2 -.-> a3
    v3 -.-> a3
    v6 -.-> a3
    a4 --> v0
    v1 -.-> a4
    v2 -.-> a4
    v3 -.-> a4
    v7 -.-> a4
    v6 -.-> a4
    a5 --> v0
    v1 -.-> a5
    v2 -.-> a5
    v3 -.-> a5
    a6 --> v8
    a6 --> v0
    a6 --> v1
    a6 --> v2
    a6 --> v9
    a6 --> v3
    a6 --> v7
    a6 --> v4
    a6 --> v5
    a6 --> v6
    a7 --> v1
    a7 --> v2
    a7 --> v9
    a7 --> v4
    a7 --> v5
    a8 --> v8
    a8 --> v0
    a8 --> v10
    a9 --> v1
    a9 --> v2
    a9 --> v9
    a10 --> v0
    a10 --> v1
    a10 --> v2
    a10 --> v9
    a10 --> v3
    a10 --> v7
    a10 --> v4
    a10 --> v5
    a10 --> v6
    a10 --> v11
    a10 --> v12
    v10 -.-> a10
    a11 --> v8
    a12 --> v8
```
