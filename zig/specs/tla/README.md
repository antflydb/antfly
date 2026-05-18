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

### Raft Consensus (etcd/raft)

Abstract raft model forked from etcd's TLA+ spec, extended for antfly-zig's raft implementation.

- `etcdraft.tla` -- Core raft spec (elections, log replication, snapshots, config changes, message pipeline with `pendingMessages` -> `Ready` -> `messages`)
- `MCetcdraft.tla` / `MCetcdraft.cfg` -- Standalone model checking module
- `etcdraft.cfg` -- TLC configuration

### Raft Trace Validation

Validates that the zig raft implementation (`../raft/`) conforms to `etcdraft.tla` by replaying ndjson event traces through the TLA+ model. The zig test suite emits trace events when built with `-Dwith_tla=true`; each event is matched to a corresponding TLA+ action, and 8 safety invariants are checked at every state.

- `Traceetcdraft.tla` -- Trace refinement spec (maps ndjson events to etcdraft actions, adds `HandleSnapshotFromTrace` for cross-segment snapshots and `TracePostDrain` for end-of-trace message draining)
- `Traceetcdraft.cfg` -- TLC configuration (checks `TraceMatched`, `etcdSpec`, and 8 raft safety invariants)
- `../../scripts/tla-segment-raft-trace.py` -- Segments multi-run traces into per-cluster-lifecycle ndjson files with validity filtering
- `../../scripts/tla-validate-trace.sh` -- Runs TLC on each segment in parallel
- `../../src/tracing/raft_trace_logger.zig` -- Zig trace logger that emits ndjson events (pre-event synthesis for self-votes and self-acks, MsgSnap encoding matching the TLA+ snapshot model)

### Transaction Trace Validation

Validates that the distributed transaction implementation conforms to `AntflyTransaction.tla` by replaying ndjson traces. Constants (transactions, shards, keys) are derived from the trace file -- no MC module needed.

- `TraceAntflyTransaction.tla` -- Trace refinement spec
- `TraceAntflyTransaction.cfg` -- TLC configuration (checks `TraceMatched` and 5 safety invariants)
- `../../scripts/tla-filter-txn-trace.py` -- Filters transaction traces for spec compatibility

## Makefile Targets

From the repo root:

```bash
make tla-tools                  # Download tla2tools.jar + CommunityModules (one-time)
make tla-check                  # Model check all specs (txn, split, snapshot)
make tla-check-txn              # Model check transaction spec
make tla-check-split            # Model check shard split spec
make tla-check-snap             # Model check snapshot transfer spec (safety only, ~90s)

# Trace validation (requires building with -Dwith_tla=true first)
make tla-trace-raft TRACE_FILES=/tmp/raft-trace.ndjson
make tla-trace-txn  TRACE_FILES=/tmp/txn-trace.ndjson
```

## Raft Trace Validation Workflow

Build the zig raft tests with tracing enabled, then validate the trace:

```bash
# 1. Build and run raft tests, capturing trace to stderr
~/bin/zig build -Dwith_tla=true raft-test 2>/tmp/zig-raft-trace.ndjson

# 2. Segment + validate (the Makefile target does both)
make tla-trace-raft TRACE_FILES=/tmp/zig-raft-trace.ndjson
```

The pipeline:
1. **Segmentation** (`scripts/tla-segment-raft-trace.py`) -- Splits the multi-run trace at cluster initialization boundaries. Filters out segments that can't be independently validated (partial runs, cross-segment snapshots without elections, missing nodes).
2. **Validation** (`scripts/tla-validate-trace.sh`) -- Runs TLC on each segment in parallel, checking that every trace event maps to a valid `etcdraft.tla` action and all 8 safety invariants hold.

### What's checked

| Property | What it ensures |
|---|---|
| `TraceMatched` | The entire trace is consumed (TLC explores the full ndjson log) |
| `etcdSpec` | Every trace event corresponds to a valid `etcdraft!NextDynamic` action |
| `LogInv` | Logs are append-only with monotonic terms |
| `ElectionSafetyInv` | At most one leader per term |
| `LogMatchingInv` | If two logs have an entry with same index and term, all preceding entries match |
| `QuorumLogInv` | Committed entries exist in a quorum of logs |
| `LeaderCompletenessInv` | A leader's log contains all committed entries from prior terms |
| `MoreThanOneLeaderInv` | No two leaders in the same term |
| `MoreUpToDateCorrectInv` | Vote comparison correctly identifies more up-to-date logs |
| `CommittedIsDurableInv` | Committed state is persisted to durable storage |

### Key design decisions

**Pre-event synthesis**: The zig raft engine processes some operations in a different order than the TLA+ model expects. The trace logger synthesizes events to bridge this:
- **Self-vote flush**: In 1-node clusters, a `Ready` event is synthesized before `ReceiveRequestVoteResponse` to flush the self-vote from `pendingMessages` to `messages`.
- **Self-ack flush**: Before `Commit` events, synthetic `Ready` + `ReceiveAppendEntriesResponse` events are emitted so `AdvanceCommitIndex` can see updated `matchIndex`.

**Post-trace drain**: After the last trace event, `TracePostDrain` uses `Ready` and `DropMessage` to empty the message bags so TLC's queue drains to zero. The `etcdSpec` check is relaxed during drain because `NextUnreliable` only allows `DropMessage` for messages with count=1, but accumulated heartbeats can have higher counts.

**Snapshot handling**: `HandleSnapshotFromTrace` handles `ReceiveSnapshot` events where the corresponding `SendAppendEntriesRequest` (MsgSnap) is in a prior trace segment. It consumes the message if present, otherwise proceeds without it, and correctly advances `commitIndex` to the snapshot index.

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

## Mapping to Zig Implementation

| TLA+ Variable | Zig Code |
|---|---|
| `clock` | Timestamp parameter in `initTransactionWithParticipants` |
| `txnStatus` | Orchestrator state in `src/api/distributed_txn.zig` (`ParticipantWorker` vtable) |
| `txnRecords` | Transaction records managed by `src/storage/transactions.zig` (`TxnManager`) |
| `resolvedParts` | Participant tracking in `TxnManager.resolveIntents` |
| `intents` | Write intents in `src/storage/transactions.zig` (`WriteIntent` struct) |
| `dataStore` | LMDB key-value data, written during intent resolution |
| `predicateSnapshot` | `VersionPredicate` structs passed to `writeIntents` and `checkVersionPredicates` |

| TLA+ Action | Zig Code |
|---|---|
| `InitTransaction` | `storage/transactions.zig:initTransactionWithParticipants` |
| `CheckPredicates` | `storage/transactions.zig:checkVersionPredicates` |
| `WriteIntentOnShard` | `storage/transactions.zig:writeIntents` (success path) |
| `WriteIntentFails` | `storage/transactions.zig:writeIntents` (error: VersionConflict or IntentConflict) |
| `CommitTransaction` | `api/distributed_txn.zig:resolve_group` (status=committed) |
| `AbortTransaction` | `api/distributed_txn.zig:resolve_group` (status=aborted) |
| `ResolveIntentsOnShard` | `storage/transactions.zig:resolveIntents` |
| `RecoveryResolve` | `storage/db/maintenance/transaction_runtime.zig:runRecoveryWithConfig` |
| `CleanupTxnRecord` | `storage/db/maintenance/transaction_runtime.zig` (after cleanup) |

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

### Mapping to Zig Implementation

| TLA+ Variable | Zig Code |
|---|---|
| `leader` | Raft election in `../raft/src/core/raft.zig` |
| `persistedSnap` | Snapshot state in `pkg/antfly/src/raft/host.zig` |
| `targetSnap` | In-memory snapshot metadata during transfer |
| `snapStore` | Snapshot storage in `pkg/antfly/src/raft/storage/` |
| `transferState` | Snapshot fetch in `pkg/antfly/src/raft/transport/` |
| `retryCount` | Retry logic in snapshot transport |

| TLA+ Action | Zig Code |
|---|---|
| `CreateSnapshot` | Snapshot creation in `pkg/antfly/src/raft/managed_host.zig` |
| `RaftSendsSnapshot` | Raft MsgSnap handling in `pkg/antfly/src/raft/host.zig` |
| `TransferSucceeds` | Snapshot transport success |
| `TransferPermanentFailure` | Permanent failure detection in transport |
| `TransferRetry` | Retryable error in transport |
| `ApplySnapshot` | Snapshot application via state machine |
