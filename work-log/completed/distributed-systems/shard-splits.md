# Online Shard Splitting

Antfly's shard splitting capability evolved through two major iterations. V1 introduced a continuous-catchup design based on Raft log replay to eliminate write downtime during shard splits. After encountering race conditions and limitations around index availability and leadership resilience, V2 redesigned the algorithm with Raft-replicated split state, shadow IndexManagers for zero index read downtime, pre-built indexes in archives, and a timeout-based rollback mechanism. Both versions are documented here along with design decisions, race condition analysis, and implementation details.

---

## V1: Online Shard Splits

### Overview

Implement online shard splitting that allows both the original and new shards to handle writes throughout the split process, eliminating write downtime during shard splits.

### Current State (Pre-V1)

#### Current Split Workflow

1. **Metadata server** marks shard as `PreSplit` and creates `SplitTransition`
2. **Original shard** creates snapshot of `[splitKey, high)` range via `SplitOnline()`
3. **Original shard** immediately updates its range to `[low, splitKey)` and deletes `[splitKey, high)` data
4. **New shard** starts in `SplittingOff` state, loads snapshot from archive
5. **New shard** transitions to `Default` state once fully initialized
6. **Metadata** updates routing table

#### Current Problems

1. **Write availability gap**: Once original shard updates its range (step 3), writes to `[splitKey, high)` are rejected until new shard is fully initialized (step 5)
2. **Snapshot staleness**: Data written between snapshot creation and new shard initialization is lost
3. **No catchup mechanism**: New shard doesn't have a way to get updates that occurred during its bootstrap

#### Existing Bug Fixed

**Location**: `src/store/dbwrapper.go:279-325` (`proposalDequeuer`)

**Bug**: Range validation sent error for out-of-range keys but still appended all writes to the merged batch, causing data corruption.

**Fix**: Changed validation to reject entire batch if ANY key is out of range using `goto nextProposal` to skip batch merging.

### Proposed Solution: Continuous Catchup

#### Design Principles

1. **Metadata-coordinated routing**: Metadata server handles all routing decisions, not storage layer
2. **Raft log as source of truth**: New shard catches up via Raft log replay
3. **Snapshot with Raft index**: Snapshot includes the Raft applied index for precise replay
4. **Continuous catchup**: New shard continuously catches up until routing switches
5. **Client-driven split handling**: Clients handle cross-shard batches by refreshing routing and retrying

#### Architecture

```
Phase 1: Snapshot Creation (Raft Index Tracking)

Original Shard (Leader):
  - Still serving ALL writes to [low, high)
  - Create snapshot of [splitKey, high) data
  - Record Raft appliedIndex = N
  - Package: {snapshot.tar.zst, metadata: {raftIndex: N}}
  - State: PreSplit

Phase 2: Bootstrap with Continuous Catchup

New Shard:
  - Load snapshot -> data up to Raft index N
  - Start Raft group, elect leader
  - State: SplittingOff

Continuous Catchup Loop:
  - Request log replay from original shard:
    POST /shards/{origID}/replay-log
    Body: {fromIndex: N+1, toIndex: current, keyRange: [splitKey, high)}
  - Original shard streams Raft entries N+1->current filtered by key range
  - New shard applies entries via Raft.Propose()
  - Check gap: if (origIndex - newIndex) < threshold then mark State: SplitOffReady
  - Continue catching up even after marking ready

Phase 3: Metadata Routes to New Shard

Metadata Server:
  - Polls shard states via reconciler
  - Detects: NewShard.State == SplitOffReady
  - Updates routing table atomically:
    [low, splitKey) -> Original Shard
    [splitKey, high) -> New Shard
  - Clients get new routing on next metadata fetch
  - Notifies both shards: "routing_updated"

Phase 4: Original Shard Cleanup & Final Catchup

Original Shard:
  - Receives "routing_updated" notification
  - Stops accepting writes to [splitKey, high) (returns ErrKeyOutOfRange)
  - Updates local range to [low, splitKey)
  - DeleteRange([splitKey, nil))
  - State: Default

New Shard:
  - Receives "routing_updated" notification
  - Performs one final catchup round
  - Stops catchup loop
  - State: Default

Clients:
  - Batches with [splitKey, high) keys sent to original shard get ErrKeyOutOfRange
  - Refresh routing from metadata
  - Retry to correct shard
```

#### Timeline Example

```
Time  Original  New       Action
      Index     Index
------------------------------------------------------------
T0    100       -         Snapshot created @ index 100
T1    120       -         New shard loads snapshot
T2    135       -         New shard starts Raft
T3    150       100       Catchup round 1: replay 101->150
T4    165       150       Catchup round 2: replay 151->165
T5    178       165       Catchup round 3: replay 166->178
T6    185       178       Gap=7, mark SplitOffReady
T7    190       180       Metadata detects ready
T8    195       185       Metadata updates routing
T9    200       190       Clients refresh routing
T10   205       195       Final catchup: 196->205
T11   210       205       Stop catchup, both shards Default
------------------------------------------------------------
Result: Zero write failures, ~5 entry lag at cutover
```

### Implementation Tasks

#### 1. Extend Shard State Machine

**File**: `src/store/store.proto`

Add new shard state:
```protobuf
enum ShardState {
  Default = 0;
  SplittingOff = 1;
  SplitOffPreSnap = 2;
  PreSplit = 3;
  PreMerge = 4;
  Initializing = 5;
  Splitting = 6;
  SplitOffReady = 7;  // NEW: Ready to receive traffic
}
```

#### 2. Add Raft Index to Snapshot Metadata

**File**: `src/store/db.go` (`SplitOnline`, `Snapshot`)

```go
type SnapshotMetadata struct {
    RaftAppliedIndex uint64       `json:"raftAppliedIndex"`
    KeyRange         common.Range `json:"keyRange"`
    CreatedAt        time.Time    `json:"createdAt"`
    ShardID          int64        `json:"shardId"`
}
```

#### 3. Implement Raft Log Replay API

**New file**: `src/store/replay.go`

```go
type ReplayLogRequest struct {
    FromIndex uint64       `json:"fromIndex"`
    ToIndex   uint64       `json:"toIndex"` // 0 = current
    KeyRange  common.Range `json:"keyRange"`
}

type ReplayLogResponse struct {
    Entries []ReplayEntry `json:"entries"`
}

type ReplayEntry struct {
    Index  uint64 `json:"index"`
    Writes []byte `json:"writes"` // Encoded BatchOp
}
```

#### 4. Implement Continuous Catchup in New Shard

**New file**: `src/store/catchup.go`

```go
type CatchupManager struct {
    shard             *DBWrapper
    parentShardID     int64
    parentLeaderURL   string
    snapshotRaftIndex uint64
    keyRange          common.Range
    stopC             chan struct{}
    stoppedC          chan struct{}
    lastAppliedIndex  atomic.Uint64
    routingUpdated    atomic.Bool
}
```

The CatchupManager runs a 1-second ticker loop, requesting log replay from the parent shard, applying entries via `Raft.Propose()`, and marking `SplitOffReady` when the gap drops below 100 entries. After routing is updated, it performs one final catchup round and stops.

#### 5-7. Metadata Routing, Reconciler State Machine, and Notification Handler

- Updated routing logic to check for `SplitOffReady` child shards
- Extended reconciler to compute `update_routing`, `cleanup_original`, and `finalize_new` actions
- Added `/shards/:id/notify` endpoint with `routing_updated` event handling

#### 8. Client SDK Updates

Clients handle `ErrKeyOutOfRange` by refreshing the routing table, splitting cross-shard batches, and retrying to the correct shard(s).

### Rollout Plan

- **Phase 1** (Week 1): Foundation -- states, snapshot metadata, replay API
- **Phase 2** (Week 2): Catchup mechanism
- **Phase 3** (Week 3): Metadata coordination
- **Phase 4** (Week 4): Client SDKs, E2E tests, docs

### Metrics

- `split_catchup_rounds_total` -- Number of catchup rounds per split
- `split_catchup_gap_entries` -- Raft index gap during catchup
- `split_catchup_duration_seconds` -- Time from snapshot to SplitOffReady
- `split_routing_update_lag_seconds` -- Time from SplitOffReady to routing update
- `split_cross_shard_batch_rejections_total` -- Client batches rejected

### Risks & Mitigations

1. **Raft Log Compaction During Catchup**: Delay compaction while `splitInProgress` flag is set; fall back to full resnapshot if compaction happens.
2. **Catchup Never Converges**: Set maximum catchup duration (5 minutes); pause parent writes briefly or fall back to offline split.
3. **Metadata Routing Race Condition**: TTL on routing cache (10s), client refreshes on `ErrKeyOutOfRange`, multiple retries with exponential backoff.

### Success Criteria

1. **Zero write downtime**: No 503 errors during split
2. **Bounded catchup time**: New shard reaches SplitOffReady within 30s
3. **Low routing update lag**: < 5s from SplitOffReady to client routing refresh
4. **No data loss**: All writes accounted for in final verification
5. **Performance**: Split throughput >= 90% of pre-split throughput

---

## V1 Design Decisions

### 1. Routing Coordination: Metadata Server vs Storage Layer

**Decision**: Metadata-level routing (not storage-level forwarding).

**Rationale**: Clean separation of concerns -- routing belongs in metadata layer. Storage layer doesn't need HTTP client for peer-to-peer communication. Metadata server already has routing logic and shard state tracking.

**Trade-off**: Requires clients to handle `ErrKeyOutOfRange` and refresh routing, but this is already a pattern in the system.

### 2. Catchup Mechanism: Forwarding vs Raft Log Replay

**Decision**: Continuous catchup via Raft log replay.

**Rationale**: Raft log is already the source of truth -- no new synchronization primitive needed. No forwarding complexity in storage layer. Natural fit with existing snapshot + replay pattern. Bounded inconsistency window (new shard catches up in <1 second after routing update).

**Trade-off**: Small replication lag (typically 5-100 entries) at routing cutover, but acceptable given the system already allows overwrites and has no read-after-write guarantees across shards.

### 3. Cross-Shard Batch Handling

**Decision**: Reject entire batch with `ErrKeyOutOfRange`, client retries with split batches.

**Rationale**: Safest behavior -- prevents partial batch application. Discovered and fixed bug where out-of-range keys were flagged but still applied. Changed `proposalDequeuer()` to use `goto nextProposal` to skip entire batch.

### 4. State Machine Extension

**Decision**: Add `SplitOffReady` state. Mark when Raft index gap < 100 entries (tunable).

### 5. Snapshot Metadata Enhancement

**Decision**: Include Raft applied index in snapshot metadata for precise replay starting point (index N+1).

### 6. Continuous vs One-Time Catchup

**Decision**: Continuous catchup loop that runs until routing is updated. Handles the gap between "mark ready" and "routing updated".

### 7. Notification Mechanism

**Decision**: Metadata server explicitly notifies both shards when routing is updated via HTTP POST `/shards/:id/notify`.

### 8. Batch Validation Fix

**Bug Found**: `proposalDequeuer()` sent error for out-of-range keys but still appended all writes. **Fix**: Validate ALL keys first, use `goto nextProposal` to skip batch entirely if any key is out of range.

### Rejected Alternatives

- **Write Forwarding**: Added significant complexity to storage layer for minimal benefit.
- **Brief Write Freeze**: Defeats the purpose of "online" shard splitting. Even 1-5 seconds is user-visible.
- **Client-Side Buffering**: Pushes complexity to all clients, increases memory usage.
- **Delayed Range Update**: Memory overhead for buffering, risk of overflow.

### Open Questions (from V1)

1. **Raft Log Compaction During Catchup**: What if parent shard compacts before new shard finishes catching up? Proposed: delay compaction, or fall back to resnapshot.
2. **Catchup Timeout**: What if catchup never converges under extreme write load? Proposed: 5-minute maximum with fallback.
3. **Client Routing Cache TTL**: Current 10s TTL might be too long for fast split scenarios.

---

## V1 Race Condition Analysis

### The Problem

Parallel execution of `SplitShard` and `StartShard` causes data loss with "received status 500: not found" errors. Sequential execution (split first, then start) works correctly. The root cause is a race between Raft node initialization and archive availability across peers.

### The Race Condition

#### Timeline: Parallel Execution (FAILS)

```
Time Event
---- ----------------------------------------------------------
 t0  StartShard request arrives -> store.StartRaftGroup() called

 t1  dbWrapper created, dbDir doesn't exist yet
     -> calls loadAndRecoverFromPersistentSnapshot() IMMEDIATELY

 t2  newDBWrapper() completes quickly (creates directories, initializes)

 t3  go newDBWrapper() returns to StartRaftGroup
     -> raftNode.SetLeaderFactory() called
     -> m.shardsMap.Store(shardID, &Shard{...})  // Shard ADDED TO ROUTING

 t4  Shard is NOW AVAILABLE IN ROUTING TABLE but NOT fully initialized!

 t5  NEW REQUESTS arrive for the shard
     -> Routed to shard that hasn't finished initialization
     -> dbwrapper.readCommits() not yet processing commits
     -> startRaft() still running asynchronously

 t6  SplitShard proposal BEGINS to propagate via Raft
     -> Peers start applying applyOpSplit
     -> Archive created on peers

 t7  NewRaftNode's serveChannels() tries to load snapshot
     -> Calls getSnapshotID() -> GetSnapshotID()
     -> Calls loadSnapshot() -> tries maybeFetchStorageSnapshot()
     -> Transport.GetSnapshot() calls peers requesting archive

 t8  **RACE: Is archive available yet?**
     - IF peers have applied split: GetSnapshot succeeds via retry logic
     - IF peers haven't applied split yet: GetSnapshot retries until timeout

 t9  Archive fetch completes (or times out after 30 seconds with retries)

 t10 dbwrapper.loadAndRecoverFromPersistentSnapshot() completes
     -> dbwrapper.readCommits() starts processing commits
     -> Shard is NOW READY for queries

 BUG: Between t4-t10, shard is in routing table but not ready!
      Requests fail with "not found" or data loss errors.
```

#### Timeline: Sequential Execution (WORKS)

```
Time Event
---- ----------------------------------------------------------
 t0  SplitShard proposal sent to Raft
 t1  Raft replication completes across all peers
     -> All peers apply applyOpSplit
     -> Archives created on all peers
 t2  SplitShard returns to caller
 t3  StartShard request BEGINS (sequential, after split completes)
 t4-t9  Normal initialization with archives already present
 t9  Shard added to routing table ONLY AFTER full initialization
```

### Root Causes

1. **Shard Added to Routing Before Initialization Completes** (`src/store/store.go:567-573`): Shard is stored in `m.shardsMap` BEFORE dbWrapper is fully initialized, specifically before `loadAndRecoverFromPersistentSnapshot()` completes.

2. **Asynchronous Initialization with Late Snapshot Fetch**: Raft node initialization is async (spawned as goroutine). Snapshot fetch happens later inside `serveChannels/GetSnapshotID`. No way to know when initialization is truly complete.

3. **Archive Availability Depends on Raft Consensus**: The split archive is created by applying the split operation through Raft -- this requires leader proposal, log replication to quorum, commit confirmation, and application on all peers.

4. **Transport.GetSnapshot Has Retry Logic BUT with Time Limits**: Retries with exponential backoff (1 second initial, capped at 30 seconds with 20% jitter). Split operation might take longer than expected.

### The Data Loss Mechanism

When requests route to a shard before it's ready:
1. Shard exists in routing table but hasn't loaded initial state
2. `dbwrapper.readCommits()` hasn't started because initialization is blocked on snapshot fetch
3. Request attempts to query the shard
4. Shard returns "not found" because byte range is wrong, data hasn't loaded, or database directory doesn't exist

### Fix Strategy

**Chosen approach**: Sequential execution in reconciler (`executor.go:505-529`). Archive is guaranteed to exist before `StartShard` begins.

**Future options**:
- Add synchronization point in `StartRaftGroup` (channel signals when shard is "ready")
- Delayed routing -- don't add shard to routing table until fully initialized
- Make archive fetch synchronous before returning from `StartRaftGroup()`

---

## V2: Zero-Downtime Shard Split RFC

### Summary

This RFC proposes a redesigned two-phase shard split algorithm that ensures:
1. **Write availability** throughout the split process
2. **Index read availability** throughout the split process
3. **Guaranteed finalization** even across leadership changes
4. **Raft-replicated split state** eliminating local-only state vulnerabilities

### Motivation

The V1 split implementation has several availability and consistency gaps:

1. **`pendingSplitKey` is local state** (not Raft-replicated). A leadership change between PrepareSplit and SplitShard would cause the new leader to accept writes that get lost.

2. **Indexes are not split at prepare time**. Parent indexes still cover full original range until FinalizeSplit. Index reads may return stale data for the split-off range.

3. **No guarantee that FinalizeSplit happens**. Triggered only when new shard reports `HasSnapshot=true`. If new shard never starts, parent retains split-off data indefinitely.

4. **Archive does not include pre-built indexes**. New shard must rebuild indexes from scratch, creating an index read unavailability window.

| Scenario | Current Behavior | User Impact |
|----------|------------------|-------------|
| Leadership change during split | pendingSplitKey lost, writes accepted then lost | Data loss |
| New shard fails to start | FinalizeSplit never called, parent holds stale data | Disk bloat, inconsistency |
| Index read during split | May return phantom results from split-off range | Incorrect search results |
| New shard bootstrap | Index rebuild required, no reads until complete | Read unavailability |

### Split State Machine

```
                                    SPLIT TIMEOUT
                                  (configurable, e.g. 5min)

 Normal --> PrepareSplit --> Splitting --> Finalize --> Normal
                |                |             |
                |                |             |
          Raft-replicated  Raft-replicated  Raft-replicated
                                |
                                | timeout
                                v
                            Rollback --> Normal
```

### State Definitions

```protobuf
message SplitState {
  enum Phase {
    NONE = 0;
    PREPARE = 1;      // Shadow index created, dual-write active
    SPLITTING = 2;    // Archive created, new shard starting
    FINALIZING = 3;   // New shard ready, cleaning up parent
    ROLLING_BACK = 4; // Timeout reached, reverting to pre-split
  }

  Phase phase = 1;
  bytes split_key = 2;
  uint64 new_shard_id = 3;
  int64 started_at_unix_nanos = 4;
  bytes original_range_end = 5;  // For rollback
}
```

### Phase 1: PrepareSplit

**Trigger**: Reconciler detects shard exceeds `MaxShardSizeBytes`

**Operations** (all in single Raft proposal):

1. **Store SplitState in Pebble** (replicated via Raft)
2. **Update byteRange immediately** to `[currentRange[0], medianKey]` -- rejects writes to split-off range at proposal time
3. **Create shadow IndexManager for split-off range** with separate directory, same Pebble instance, starts with backfill from existing data
4. **Enable dual-write mode** -- route writes to shadow vs parent index based on split key

**Write Availability**: Writes continue to parent Pebble. Writes to split-off range are rejected at proposal time (byteRange updated).

**Index Read Availability**: Queries for `[start, splitKey)` go to parent IndexManager; queries for `[splitKey, end)` go to shadow IndexManager (backfilling, then live).

### Phase 2: Splitting

**Trigger**: Shadow index backfill complete

**Operations**:
1. Transition state to `SPLITTING`
2. Create archive with data AND indexes (shadow indexes exported alongside Pebble data)
3. Start new shard -- extracts archive with pre-built indexes, can serve reads immediately

### Phase 3: Finalize

**Trigger**: New shard reports `HasSnapshot=true` and is serving traffic

**Operations**:
1. Transition state to `FINALIZING`
2. Delete split-off data from parent Pebble
3. Close and remove shadow IndexManager
4. Clear split state

### Phase 4: Rollback (Timeout Path)

**Trigger**: `time.Now() - splitState.StartedAt > SplitTimeout` (e.g., 5 minutes)

**Operations**:
1. Transition state to `ROLLING_BACK`
2. Restore original byteRange
3. Close shadow IndexManager (discard approach -- parent index will re-index via normal enrichment)
4. Clear split state
5. Mark shard for retry with cooldown

### Index Read Routing

During a split, queries are routed based on key range:
- Query parent index for `[start, splitKey)`
- Query shadow index for `[splitKey, originalEnd)`
- Results are merged

### Write Path Changes

Proposal validation reads from the Raft-replicated `SplitState` (not local variable). During `PREPARE` phase, writes to the split-off range are rejected. The apply path routes index writes to the appropriate IndexManager based on key comparison with the split key.

### Archive Format v2

```
archive.tar.zst
+-- pebble/
|   +-- MANIFEST-...
|   +-- OPTIONS-...
|   +-- *.sst
+-- indexes/
|   +-- full_text_v0/
|   +-- aknn_v0/
+-- metadata.json
    {"version": 2, "byteRange": [...], "indexVersions": {...}}
```

New shard bootstrap: extract archive, move Pebble data, move pre-built indexes, skip backfill.

### Configuration

```yaml
shard_split:
  split_timeout: 5m
  finalize_timeout: 1m
  rollback_timeout: 2m
  failed_split_cooldown: 10m
  include_indexes_in_archive: true
  shadow_index_backfill_batch_size: 1000
```

### Latency Impact

| Operation | Current | Proposed | Delta |
|-----------|---------|----------|-------|
| Write (split-off range) | Rejected | Rejected | Same |
| Write (parent range) | ~1ms | ~1.2ms | +20% (dual index) |
| Read (parent range) | ~5ms | ~5ms | Same |
| Read (split-off range) | ~5ms (may have stale data) | ~5ms (correct) | Same + correctness |
| New shard first read | ~30s (backfill) | ~1s (pre-built) | -97% |

### Alternatives Considered

1. **Synchronous Full Split**: Rejected -- write unavailability unacceptable for production.
2. **Log-Based Split**: Rejected -- unbounded log size, complex replay logic.
3. **Copy-on-Write Index Structures**: Future consideration -- requires rewriting all index implementations.
4. **Forward-Only Without Rollback**: Rejected -- stuck splits would require manual intervention.

---

## V2 Implementation Plan

### Implementation Phases

| Phase | Description | Estimated Complexity | Dependencies |
|-------|-------------|---------------------|--------------|
| 1 | SplitState Protobuf & Persistence | Low | None |
| 2 | Raft-Replicated Split State | Medium | Phase 1 |
| 3 | Shadow IndexManager | High | Phase 1 |
| 4 | Dual-Write Routing | Medium | Phase 3 |
| 5 | Query Routing | Medium | Phase 3 |
| 6 | Archive Format v2 | Medium | Phase 3 |
| 7 | Reconciler State Machine | Medium | Phase 2 |
| 8 | Rollback Mechanism | Medium | Phase 7 |
| 9 | Integration Testing | High | Phases 1-8 |
| 10 | Migration & Feature Flags | Low | Phases 1-8 |

### Phase 1: SplitState Protobuf & Persistence

Add `SplitState` message to protobuf. Replace `pendingSplitKey` field in `dbWrapper` with `splitState`. Add persistence layer using a well-known Pebble key `__antfly_split_state__` for load/save on startup.

### Phase 2: Raft-Replicated Split State

Replace local `pendingSplitKey` with Raft-replicated `SplitState`. Modify the Split method to propose a `SetSplitState` op through Raft. Update `proposalDequeuer` to read from `splitState` (loaded from Pebble) instead of local variable. Update apply path in `applyOpSplit` to save SplitState.

### Phase 3: Shadow IndexManager

Create range-scoped IndexManager for split-off data. Add `byteRange` filtering to the `Index` method. Implement `ExportTo` for archive creation. Add Export to `Index` interface for each index type (full_text_v0, aknn_v0).

### Phase 4: Dual-Write Routing

Route index writes to the correct IndexManager based on key:
```go
func (s *dbWrapper) routeToIndex(key, value []byte) error {
    splitState := s.splitState
    if splitState != nil && splitState.Phase == SplitState_PHASE_PREPARE {
        if bytes.Compare(key, splitState.SplitKey) >= 0 {
            return s.shadowIndexMgr.Index(key, value)
        }
    }
    return s.indexMgr.Index(key, value)
}
```

### Phase 5: Query Routing

Route index reads to the correct IndexManager based on query range, merging results from both parent and shadow indexes during split.

### Phase 6: Archive Format v2

Include shadow indexes in the archive. Modify new shard bootstrap to restore indexes from archive and skip backfill when pre-built indexes are present.

### Phase 7: Reconciler State Machine

Add new split state actions: `Prepare`, `TransitionToSplit`, `Finalize`, `Rollback`. Add timeout detection for stuck splits.

### Phase 8: Rollback Mechanism

Implement `RollbackSplit` operation: restore original range, close and remove shadow index (discard approach), clear split state. Add to reconciler as a timeout-triggered action.

### Parallel Execution Map

```
Week 1: Phase 1 + Phase 3 exploration (parallel)
Week 2: Phase 2 + Phase 3 implementation
Week 3: Phase 4 & 5 (parallel) + Phase 6
Week 4: Phase 7 + Phase 8
Week 5: Phase 9 + Phase 10 (parallel)
```

### Feature Flags

```yaml
shard_split:
  enable_raft_replicated_split_state: true
  enable_shadow_index: true
  enable_indexes_in_archive: true
  enable_split_rollback: true
  split_timeout: "5m"
```

### Success Criteria

**Functional**:
- Split completes with zero write rejections (except split-off range)
- Index queries return correct results throughout split
- Leadership change during split does not lose data
- New shard serves reads within 5s of starting (pre-built indexes)
- Timeout triggers rollback and shard returns to normal

**Performance**:
- Write latency increase < 20% during split
- Read latency unchanged during split
- New shard bootstrap time reduced by > 90%

---

## V2 Implementation Notes

### Phase 1: Proto Exploration

#### Existing Proto Files

**store.proto** (lines 10-36):
- `StoreState` enum: Store health state
- `ShardState` enum: Shard lifecycle states including split-related:
  - `SplittingOff = 1`
  - `SplitOffPreSnap = 2`
  - `PreSplit = 3`
  - `Splitting = 6`

**kvstore.proto** (lines 9-106):
- `KvstoreOp` message with nested `OpType` enum:
  - `OpSplit = 1`
  - `OpFinalizeSplit = 12`
- `SplitOp` message (lines 94-97)
- `FinalizeSplitOp` message (lines 103-106)

#### Patterns to Follow

1. **Field presence**: Use `features.field_presence = IMPLICIT`
2. **Nested enums**: Define inside the message
3. **Builder pattern**: Generated code uses `SplitState_builder{...}.Build()`
4. **Well-known keys**: Store state at constant key in Pebble

#### Files Modified

1. `src/store/kvstore.proto` -- Add SplitState message
2. `src/store/dbwrapper.go` -- Add persistence layer
3. Run `make generate` after proto changes

### Phase 3: IndexManager Architecture

The IndexManager **already supports shadow instances**. Key findings:

#### Creation Pattern
- `NewIndexManager()` accepts `byteRange` parameter
- Each IndexManager gets its own `dir` directory
- Multiple IndexManagers can share same Pebble DB

#### Byte Range Filtering
- Location: `indexmgr.go:134-145`
- Filters in `indexOp.Execute()` -- drops keys outside range
- All backfills respect byte range bounds

#### Backfill Support
- Both FullTextV0 and AknnV0 implement `BackfillableIndex`
- Uses `.rebuildstate` file for resume capability
- Independent backfills can run concurrently on same DB

#### Key Constraints
- Byte ranges MUST NOT overlap between managers
- Directory paths MUST be unique
- Each manager has own WAL buffer

#### Shadow IndexManager Implementation

Already demonstrated in `db.go:888-912`:
```go
newIndexManager, err := NewIndexManager(
    db.logger,
    db.antflyConfig,
    db.pdb,
    filepath.Join(destDir1, "indexes"),  // Separate directory
    db.schema,
    range1,  // New byteRange
)
```

#### Index Directory Structure
```
{shard_dir}/indexes/
+-- full_text_index/
|   +-- bleve/
|   +-- indexWAL/
|   +-- rebuild.state
+-- embedding_index/
|   +-- reverseindex/
|   +-- indexWAL/
|   +-- rebuild.state
+-- graph_index/
    +-- reverseindex/
    +-- rebuild.state
```

---

## V2 Test Coverage

### Test Coverage

#### Unit Tests: Split State (`src/store/split_state_test.go`)

| Test | Description |
|------|-------------|
| `TestSplitStatePersistence` | Set/Get/Clear split state |
| `TestSplitStateLoadOnStartup` | Persistence across restarts |
| `TestSplitStateClearedAfterNil` | Clear persists across restarts |
| `TestSplitStatePhaseTransitions` | All 5 phases can be set |
| `TestShadowIndexManagerLifecycle` | Create/close/idempotency |
| `TestShadowIndexManagerDirectory` | Directory structure |
| `TestDualWriteRoutingDuringSplit` | Write routing during phases |
| `TestSearchRoutingDuringSplit` | Query routing during phases |

**Coverage**: 100%

#### Unit Tests: Reconciler Splits (`src/metadata/reconciler/reconciler_splits_test.go`)

| Test | Description |
|------|-------------|
| `TestComputeSplitTransitions` | 8 subtests for split/merge decisions |
| `TestSplitStatePhaseActions` | 7 subtests for phase transitions |
| `TestGetSplitTimeout` | Timeout configuration |
| `TestIsSplitTimedOut` | 4 subtests for timeout detection |

**Coverage**: 100%

#### E2E Tests: Online Shard Split (`e2e/online_shard_split_test.go`)

| Test | Description |
|------|-------------|
| `TestE2E_OnlineSplit_ContinuousWrites` | Write availability during split |
| `TestE2E_OnlineSplit_CrossShardBatchRejection` | Batch rejection after split |
| `TestE2E_OnlineSplit_CatchupConvergence` | New shard catches up |
| `TestE2E_OnlineSplit_RoutingUpdateTiming` | No writes lost during routing update |
| `TestE2E_OnlineSplit_ConcurrentSplits` | Multiple shards splitting |
| `TestE2E_OnlineSplit_RaftLogCompaction` | Catchup with log compaction |
| `TestE2E_OnlineSplit_TimeoutRollsBack` | Timeout triggers rollback |
| `TestE2E_OnlineSplit_PreBuiltIndexes` | Indexes work immediately after split |
| `TestE2E_OnlineSplit_LeadershipChange` | Split survives leadership change |
| `TestE2E_SplitAvailabilityContinuousReads` | Read availability during split |
| `TestE2E_SplitAvailabilityIndexQueries` | Search queries during split |
| `TestE2E_SplitFailureLeaderCrash` | Recovery from leader crash |
| `TestE2E_SplitFailureNewShardFails` | Rollback when new shard fails |

### Running Split Tests

```bash
# Run all split-related E2E tests
RUN_SHARD_SPLIT_TESTS=true go test ./e2e -run TestE2E_OnlineSplit -v -timeout 30m > /tmp/split-tests.log 2>&1

# Run specific test
RUN_SHARD_SPLIT_TESTS=true go test ./e2e -run TestE2E_OnlineSplit_LeadershipChange -v -timeout 10m

# Run unit tests
go test ./src/store -run TestSplitState -v
go test ./src/metadata/reconciler -run TestSplitState -v
```

### Test Cluster Configuration

```go
cluster := NewTestCluster(t, ctx, TestClusterConfig{
    NumStoreNodes:       3,
    NumShards:           1,
    ReplicationFactor:   3,
    MaxShardSizeBytes:   10 * 1024,        // 10KB - low threshold for quick splits
    DisableShardAlloc:   false,
    ShardCooldownPeriod: 5 * time.Second,  // Short cooldown for faster testing
    SplitTimeout:        30 * time.Second, // Short timeout for rollback testing
})
```

### Verification Checklist

**Functional Requirements**:
- Split completes with zero write rejections (except split-off range)
- Index queries return correct results throughout split
- Leadership change during split does not lose data
- New shard serves reads within 5s of starting (pre-built indexes)
- Timeout triggers rollback and shard returns to normal

**Performance Requirements**:
- Write latency increase < 20% during split
- Read latency unchanged during split
- New shard bootstrap time reduced by > 90% (vs no pre-built indexes)

**Reliability Requirements**:
- All existing E2E tests pass
- New availability tests pass at 99.9% uptime
- Chaos tests pass (leader crash, network partition)
