# TLA+ Formal Specifications

## Specs

### Transaction Protocol

Formal verification of the distributed 2PC + OCC + recovery + cleanup protocol.

- `AntflyTransaction.tla` -- Main specification (11 actions, 6 safety invariants, 3 liveness properties)
- `MC.tla` -- Model checking module with concrete constants for a small model
- `AntflyTransaction.cfg` -- TLC configuration
- `occ-2pc.tla` / `occ-2pc.cfg` -- Historical Piledriver spec that found the OCC lost update bug (PR #381)

### Shard Split Protocol

- `AntflyShardSplit.tla` -- Shard split lifecycle with delta replay, dual-actor cutover, child leader election, and non-atomic finalize (18 actions, 10 safety invariants, 1 liveness property)
- `ShardSplitMC.tla` -- Model checking module
- `AntflyShardSplit.cfg` -- TLC configuration

### Snapshot Transfer Protocol

Formal verification of the multi-raft snapshot creation, transfer, GC, and error classification.

- `AntflySnapshotTransfer.tla` -- Main specification (10 actions, 6 safety invariants, 2 liveness properties)
- `SnapshotTransferMC.tla` -- Model checking module (3 nodes, configurable retries/snapshots)
- `AntflySnapshotTransfer.cfg` -- Full TLC configuration (safety + liveness)
- `AntflySnapshotTransfer-safety.cfg` -- Safety-only configuration (fast, ~90s)

## Installation

### macOS

Install the TLA+ Toolbox (includes bundled JRE and `tla2tools.jar`):

```bash
brew install --cask tla+-toolbox
```

This installs:
- **GUI**: `/Applications/TLA+ Toolbox.app`
- **`tla2tools.jar`**: `/Applications/TLA+ Toolbox.app/Contents/Eclipse/tla2tools.jar`
- **Bundled Java**: `/Applications/TLA+ Toolbox.app/Contents/Eclipse/plugins/org.lamport.openjdk.macosx.x86_64_14.0.1.7/Contents/Home/bin/java`

> **Note**: The Toolbox cask is built for Intel macOS and requires Rosetta 2 on Apple Silicon.

### Alternative: standalone tla2tools.jar

Download `tla2tools.jar` directly from [TLA+ GitHub releases](https://github.com/tlaplus/tlaplus/releases) and use your own Java installation (Java 11+).

## Running the Model Checker

### Using the bundled Toolbox Java

```bash
cd specs/tla

JAVA="/Applications/TLA+ Toolbox.app/Contents/Eclipse/plugins/org.lamport.openjdk.macosx.x86_64_14.0.1.7/Contents/Home/bin/java"
TLA2TOOLS="/Applications/TLA+ Toolbox.app/Contents/Eclipse/tla2tools.jar"

"$JAVA" -XX:+UseParallelGC -cp "$TLA2TOOLS" tlc2.TLC MC.tla \
    -config AntflyTransaction.cfg -workers auto -deadlock
```

### Using system Java + standalone jar

```bash
cd specs/tla
java -XX:+UseParallelGC -cp /path/to/tla2tools.jar tlc2.TLC MC.tla \
    -config AntflyTransaction.cfg -workers auto -deadlock
```

### Expected output

```
Model checking completed. No error has been found.
2362 states generated, 637 distinct states found, 0 states left on queue.
```

The `-deadlock` flag suppresses false positives from terminal quiescence (all transactions completed, no more actions enabled).

## What it Verifies

### Safety Invariants (checked at every reachable state)

| Invariant | What it catches |
|---|---|
| `TypeOK` | All variables stay within their declared types |
| `AtomicityInvariant` | Aborted transaction writes never appear in the data store |
| `NoOrphanedIntents` | Txn records not deleted while intents still exist (the orphaned intents bug) |
| `OCCSerializationInvariant` | Conflicting OCC transactions can't both have intents written |
| `LWWConsistency` | Last-writer-wins timestamp ordering is correctly maintained |
| `SerializableReads` | Two txns that read the same version of a key can't both commit (catches the OCC lost update bug from PR #381) |

### Liveness Properties (checked under weak fairness)

| Property | What it ensures |
|---|---|
| `EventualCompletion` | Committed transaction intents are all eventually resolved |
| `EventualCleanup` | Fully resolved txn records are eventually deleted |
| `EventualDecision` | No transaction stays in "preparing" or "predicatesChecked" forever |

## Key Design: Split OCC Predicate Check

The spec models the OCC predicate check as two separate steps with a window between them:

1. **`CheckPredicates(t)`** -- Snapshots committed key versions (the `:t` timestamp metadata)
2. **`WriteIntentOnShard(t, s)`** -- Validates both:
   - Committed version predicates still hold (versions haven't changed since snapshot)
   - No conflicting pending intents from other transactions (`hasConflictingIntentForKey`)

This split faithfully models the vulnerability surface that caused the OCC lost update bug (PR #381): between the predicate snapshot and intent write, another transaction can interleave. The `SerializableReads` invariant catches this -- without the intent conflict check, two transactions that snapshot the same version can both commit.

## Model Configuration

The small model in `MC.tla` uses:

- **2 transactions** (`t1`, `t2`) -- enough for OCC conflict scenarios
- **2 shards** (`s1`, `s2`) -- enough for multi-shard transactions
- **2 keys** (`k1`, `k2`) -- `k1` is the conflict key shared by both txns
- **MaxTimestamp = 4** -- bounds the HLC clock for finite state space

Transaction setup:
- `t1` writes `k1` on `s1` and `k2` on `s2` (multi-shard, coordinator `s1`)
- `t2` writes `k1` on `s1` (single-shard, coordinator `s1`)
- Both read `k1` (OCC conflict on `k1`)

## Mapping to Go Implementation

| TLA+ Variable | Go Code |
|---|---|
| `clock` | `hlc.Now()` in `src/metadata/hlc.go` |
| `txnStatus` | Orchestrator state machine in `ExecuteTransaction()` at `src/metadata/transaction.go:21` |
| `txnRecords` | Pebble records at `__txn_records__:` prefix, managed by `src/store/db/db.go:3883` and `src/store/db/helpers.go:148` |
| `resolvedParts` | `resolved_participants` field in txn record, tracked in `src/store/db/db.go:679` |
| `intents` | Intent records at `__txn_intents__:` prefix, written by `src/store/db/db.go:3942` |
| `dataStore` | Pebble key-value data + `:t` timestamp metadata, written during `src/store/db/db.go:4039` |
| `predicateSnapshot` | Version predicates captured during client read, sent as `VersionPredicates` in WriteIntent RPC |

| TLA+ Action | Go Code |
|---|---|
| `InitTransaction` | `metadata/transaction.go:217` (initTransaction) + `store/db/db.go:3883` (InitTransaction) |
| `CheckPredicates` | Client-side read capturing versions, sent as predicates in WriteIntent RPC |
| `WriteIntentOnShard` | `store/db/db.go:3942` (WriteIntent) with `checkVersionPredicates` + `hasConflictingIntentForKey` |
| `WriteIntentFails` | `store/db/db.go:3962-3967` (ErrVersionConflict or ErrIntentConflict) |
| `CommitTransaction` | `store/db/helpers.go:148` (finalizeTransaction with status=1) |
| `AbortTransaction` | `store/db/helpers.go:148` (finalizeTransaction with status=2) |
| `ResolveIntentsOnShard` | `store/db/db.go:4039` (ResolveIntents) |
| `RecoveryResolve` | `store/db/db.go:661` (transactionRecoveryLoop) |
| `CleanupTxnRecord` | `store/db/db.go:724-743` (allResolved check) |

---

## Snapshot Transfer Protocol

### Running

**Safety only** (27M distinct states, ~90s):

```bash
"$JAVA" -XX:+UseParallelGC -cp "$TLA2TOOLS" tlc2.TLC SnapshotTransferMC.tla \
    -config AntflySnapshotTransfer-safety.cfg -workers auto -deadlock
```

**Safety + liveness** (requires reduced constants — edit `SnapshotTransferMC.tla`):

Set `MCMaxRetries == 1` and `MCMaxSnapshots == 1`, then:

```bash
"$JAVA" -XX:+UseParallelGC -cp "$TLA2TOOLS" tlc2.TLC SnapshotTransferMC.tla \
    -config AntflySnapshotTransfer.cfg -workers auto -deadlock
```

> **Note**: Liveness checking with strong fairness (SF) is expensive.
> MaxRetries=1, MaxSnapshots=1 completes in ~25s.
> MaxRetries=2, MaxSnapshots=2 takes 30+ minutes.
> Safety checking scales well to the full model (3,3).

### What it Verifies

#### Safety Invariants (checked at every reachable state)

| Invariant | What it catches |
|---|---|
| `TypeOK` | All variables stay within their declared types |
| `AppliedSnapshotIsValid` | A node in "done" state has the snapshot in its local store |
| `GCSafety` | A node's persisted snapshot is always in its local store |
| `RetryBound` | No node exceeds MaxRetries |
| `NoFetchingWithoutNeed` | A fetching node is always in the needsSnap set |
| `SnapshotIDsMonotonic` | Snapshot IDs never exceed the global counter |

#### Liveness Properties (checked under strong fairness)

| Property | What it ensures |
|---|---|
| `EventualTransferResolution` | A fetching node eventually reaches idle or failed (no stuck transfers) |
| `EventualPermanentDetection` | If snapshot is GC'd everywhere, the node eventually stops fetching |

### Key Design Decisions

**Split persisted vs in-flight state**: The spec separates `persistedSnap` (survives crashes, loaded from Pebble) from `targetSnap` (in-memory, lost on crash). This distinction was discovered during model checking — an earlier version using a single `currentSnap` variable violated `GCSafety` when a node crashed mid-transfer.

**Strong fairness (SF)**: Transfer-related actions (`TransferSucceeds`, `TransferPermanentFailure`, `TransferRetry`) use SF instead of WF. Peer crashes make these actions intermittently enabled/disabled. WF only guarantees firing for *continuously* enabled actions, which is insufficient when peers crash and restart. SF reflects the real system's retry loop eventually hitting a window where the peer is available.

**RaftSendsSnapshot guard**: The spec requires `persistedSnap[leader] > persistedSnap[n]` — Raft only sends snapshots to followers that are actually behind. Without this, TLC found a scenario where Raft redundantly sends an already-applied snapshot, the recipient becomes leader and GCs it, violating `AppliedSnapshotIsValid`.

### Model Configuration

The model in `SnapshotTransferMC.tla` uses:

- **3 nodes** (`n1`, `n2`, `n3`) -- leader + 2 peers for transfer dynamics
- **MaxRetries = 3** (safety) / **1** (liveness) -- retry exhaustion
- **MaxSnapshots = 3** (safety) / **1** (liveness) -- GC scenarios

### Mapping to Go Implementation

| TLA+ Variable | Go Code |
|---|---|
| `leader` | Raft election in `src/raft/raft.go` |
| `persistedSnap` | `snapshotIndex.Store()` in `raft.go:886`, loaded from Pebble on startup |
| `targetSnap` | `snapshotMeta.GetID()` in `publishSnapshot` (`raft.go:719`) |
| `snapStore` | `snapstore.SnapStore` interface (`src/snapstore/snapstore.go`) |
| `transferState` | `GetSnapshot` retry loop in `transport.go:236` |
| `retryCount` | `retry.WithMaxRetries(10, b)` in `transport.go:234` |

| TLA+ Action | Go Code |
|---|---|
| `CreateSnapshot` | `maybeTriggerSnapshot` in `raft.go:773-900` |
| `RaftSendsSnapshot` | Raft MsgSnap triggering `publishSnapshot` in `raft.go:701` |
| `TransferSucceeds` | `sendSnapshotRequest` success in `transport.go:274` |
| `TransferPermanentFailure` | `allNotFound` check in `transport.go:278-281` |
| `TransferRetry` | `retry.RetryableError` in `transport.go:283` |
| `ApplySnapshot` | `publishSnapshot` signaling `commitC` in `raft.go:748-753` |
