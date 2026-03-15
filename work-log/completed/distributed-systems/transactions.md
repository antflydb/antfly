# Distributed Write Transactions

Antfly supports atomic cross-shard write transactions using a write-intent pattern with coordinator-based Two-Phase Commit (2PC). This document consolidates the design, implementation decisions, extensions (cross-table transactions, OCC read-modify-write, cross-table joins), end-to-end test strategy, and the TLA+-verified fix for stuck-pending transactions.

---

## 2PC Design

### Overview

Atomic cross-shard write transactions for Antfly use a write-intent pattern with coordinator-based 2PC. Batches sent through metadata servers are applied atomically across multiple storage shards.

### Architecture Summary

```
┌──────────────┐
│    Client    │
└──────┬───────┘
       │
       ▼
┌──────────────────┐    HLC timestamp
│ Metadata Server  │◄───(stateless)
│  (Coordinator)   │
└────┬─────────┬───┘
     │         │
     │         └─────────────────────┐
     ▼                               ▼
┌────────────────┐           ┌──────────────┐
│ Coordinator    │           │ Participant  │
│ Shard (Raft)   │──notify──▶│ Shard (Raft) │
│                │           │              │
│ Txn Record:    │           │ Write Intent:│
│ - TxnID        │           │ - TxnID      │
│ - Status       │           │ - Value      │
│ - Participants │           │ - Coord ID   │
└────────────────┘           └──────────────┘
```

**Key Components**:
1. **Metadata Server**: Routes requests, allocates timestamps (HLC), orchestrates protocol
2. **Coordinator Shard**: One storage shard owns transaction record, manages commit/abort
3. **Participant Shards**: Storage shards that write intents and apply committed writes

### Implementation Phases

#### Phase 1: Core Data Structures (Protobuf Definitions)

**Files Modified**:
- `src/store/kvstore.proto`
- `src/common/transaction.proto` (new file)

**Transaction Messages** (`src/common/transaction.proto`):

```protobuf
syntax = "proto3";
package common;

enum TxnStatus {
  Pending = 0;
  Committed = 1;
  Aborted = 2;
}

// Stored on coordinator shard at key: \x00\x00__txn_records__:<txnID>
message TransactionRecord {
  bytes txn_id = 1;
  TxnStatus status = 2;
  uint64 timestamp = 3;           // From metadata HLC
  repeated bytes participants = 4; // Shard IDs
  int64 created_at = 5;            // Unix timestamp
  int64 committed_at = 6;          // Unix timestamp (0 if pending)
}

// Stored on participant shards at key: \x00\x00__txn_intents__:<txnID>:<userKey>
message WriteIntent {
  bytes value = 1;           // The provisional write (compressed if needed)
  bytes txn_id = 2;          // Transaction ID
  uint64 timestamp = 3;      // From metadata HLC
  TxnStatus status = 4;      // Always Pending when written
  bytes coordinator_shard = 5; // Shard ID of coordinator
  bool is_delete = 6;        // True if this is a delete intent
}
```

**Kvstore Operations** (`src/store/kvstore.proto`):

```protobuf
// Add to KvstoreOp_OpType enum
enum OpType {
  // ... existing ops ...
  OpInitTransaction = 10;
  OpCommitTransaction = 11;
  OpAbortTransaction = 12;
  OpWriteIntent = 13;
  OpResolveIntents = 14;
}

message InitTransactionOp {
  bytes txn_id = 1;
  uint64 timestamp = 2;
  repeated bytes participants = 3; // Shard IDs
}

message CommitTransactionOp {
  bytes txn_id = 1;
}

message AbortTransactionOp {
  bytes txn_id = 1;
}

message WriteIntentOp {
  bytes txn_id = 1;
  uint64 timestamp = 2;
  bytes coordinator_shard = 3;
  BatchOp batch = 4; // Reuse existing BatchOp for writes/deletes
}

message ResolveIntentsOp {
  bytes txn_id = 1;
  TxnStatus status = 2; // Committed or Aborted
}
```

#### Phase 2: Metadata Server - HLC and Transaction Orchestration

**Files Modified**:
- `src/metadata/runner.go`
- `src/metadata/api.go`
- `src/metadata/transaction.go` (new file)

**HLC** (`src/metadata/runner.go`):

```go
type HLC struct {
    logical atomic.Uint64
}

func NewHLC() *HLC {
    return &HLC{
        logical: atomic.Uint64{},
    }
}

func (h *HLC) Now() uint64 {
    return h.logical.Add(1)
}
```

**Transaction Orchestration** (`src/metadata/transaction.go`):

```go
// ExecuteTransaction orchestrates distributed transaction across shards
func (s *MetadataServer) ExecuteTransaction(
    ctx context.Context,
    writes map[types.ID][][2][]byte,
    deletes map[types.ID][][]byte,
) error {
    txnID := uuid.New()
    timestamp := s.hlc.Now()
    coordinatorID := s.pickCoordinator(txnID, writes)

    // PHASE 1: Prepare
    // Step 1: Initialize transaction on coordinator
    if err := s.initTransaction(ctx, coordinatorID, txnID, timestamp, writes); err != nil {
        return fmt.Errorf("initializing transaction: %w", err)
    }

    // Step 2: Write intents to all participants (parallel)
    if err := s.writeIntents(ctx, txnID, timestamp, coordinatorID, writes, deletes); err != nil {
        s.abortTransaction(ctx, coordinatorID, txnID)
        return fmt.Errorf("writing intents: %w", err)
    }

    // PHASE 2: Commit
    // Step 3: Commit transaction on coordinator (commit point!)
    if err := s.commitTransaction(ctx, coordinatorID, txnID); err != nil {
        return fmt.Errorf("committing transaction: %w", err)
    }

    // Coordinator will notify participants asynchronously via recovery loop
    return nil
}

// pickCoordinator selects coordinator shard deterministically
func (s *MetadataServer) pickCoordinator(
    txnID uuid.UUID,
    writes map[types.ID][][2][]byte,
) types.ID {
    hash := xxhash.Sum64(txnID[:])
    shardIDs := make([]types.ID, 0, len(writes))
    for id := range writes {
        shardIDs = append(shardIDs, id)
    }
    sort.Slice(shardIDs, func(i, j int) bool {
        return shardIDs[i] < shardIDs[j]
    })
    return shardIDs[hash % uint64(len(shardIDs))]
}
```

#### Phase 3: Storage Shard - Transaction Operations

**Files Modified**:
- `src/store/shard.go`
- `src/store/db.go`
- `src/store/dbwrapper.go`
- `src/store/api.go`

Core DB operations implemented:
- `InitTransaction` -- creates transaction record on coordinator
- `CommitTransaction` -- updates status to Committed (the commit point)
- `AbortTransaction` -- updates status to Aborted
- `WriteIntent` -- writes provisional write intents in separate keyspace
- `ResolveIntents` -- converts intents to actual writes/deletes (on commit) or removes them (on abort)
- `GetTransactionStatus` -- queries transaction status

Key helpers:

```go
func makeTxnKey(txnID []byte) []byte {
    return append([]byte("\x00\x00__txn_records__:"), txnID...)
}

func makeIntentKey(txnID []byte, userKey []byte) []byte {
    key := append([]byte("\x00\x00__txn_intents__:"), txnID...)
    key = append(key, ':')
    key = append(key, userKey...)
    return key
}
```

#### Phase 4: Coordinator Recovery Loop

**Files Modified**:
- `src/store/db.go`
- `src/store/shard.go`

```go
func (db *DBImpl) transactionRecoveryLoop(ctx context.Context) {
    // Immediately notify on becoming leader
    db.notifyPendingResolutions(ctx)

    // Then run periodically
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            db.notifyPendingResolutions(ctx)
        }
    }
}
```

Recovery loop behavior:
- Runs every 30 seconds when node is Raft leader
- Scans all transaction records
- For committed/aborted transactions within last 5 minutes: sends notifications to participants
- Cleans up transaction records older than 5 minutes

#### Phase 5: Cross-Cluster Participant Notification

**Problem**: Participant shards may be on different nodes in the cluster, requiring proper routing through the metadata server.

**Solution**:

1. **ShardNotifier Interface** (`src/store/db.go`):
```go
type ShardNotifier interface {
    NotifyResolveIntent(ctx context.Context, shardID []byte, txnID []byte, status int32) error
}
```

2. **HTTPShardNotifier** (`src/store/shard_notifier.go`):
```go
type HTTPShardNotifier struct {
    client      *http.Client
    metadataURL string
    logger      *zap.Logger
}
```

Request flow:
```
Coordinator Shard -> Metadata Server -> Participant Shard
                    /_internal/v1/shard/{shardID}/txn/resolve
```

3. **Metadata Server Proxy Endpoint** (`src/metadata/metadata.go`):
- Receives notification requests from coordinators
- Uses existing `leaderClientForShard()` to find which node hosts the participant shard
- Forwards the `ResolveIntentsOp` to the appropriate storage node

**Key Design Decisions**:
- **Metadata Server as Proxy**: Routes notifications through the metadata server which already has cluster topology via `leaderClientForShard()`
- **Background Notifications**: Coordinator commits immediately and sends notifications asynchronously
- **Idempotent Resolution**: `ResolveIntents` is idempotent -- multiple notifications are safe
- **Base64 Encoding**: Shard IDs stored as base64 in transaction records for JSON serialization

**Files Modified**:
- `src/store/db.go` -- notification logic in recovery loop
- `src/store/shard_notifier.go` -- HTTP notifier implementation
- `src/store/dbwrapper.go` -- wired up ShardNotifier dependency
- `src/store/store.go` -- configured notifier with metadata URL
- `src/metadata/metadata.go` -- proxy endpoint and forwarding logic
- `src/metadata/runner.go` -- registered new internal endpoint
- `src/store/db_transaction_distributed_test.go` -- end-to-end tests

### Testing Strategy

**Test Levels**:

1. **Unit Tests** (~80% coverage target): Individual transaction operations, HLC timestamp generation, coordinator selection, intent key generation/parsing
2. **Integration Tests**: Full transaction lifecycle, coordinator failover, network partitions, concurrent transactions
3. **Chaos Tests**: Random leader kills, network delays, disk failures, clock skew
4. **Performance Tests**: Transaction throughput, latency percentiles, recovery time, memory usage

### Performance Characteristics

**Latency** (happy path):
```
Step 1: Metadata HLC allocation          ~0.1ms (atomic increment)
Step 2: Coordinator txn record (Raft)    ~5ms   (Raft commit)
Step 3: Participant intents (parallel)   ~10ms  (Raft commit per shard)
Step 4: Coordinator commit (Raft)        ~5ms   (Raft commit)
Total:                                   ~20ms  (end-to-end)
```

**Throughput**:
- Limited by coordinator Raft throughput (~5K-10K txns/sec per coordinator)
- Load distributed across shards (different txns lead to different coordinators)
- Metadata HLC not a bottleneck (atomic increment >> Raft throughput)

**Storage Overhead**:
- Transaction record: ~200 bytes per txn (cleaned up after 5 minutes)
- Write intent: ~100 bytes + value size per write (cleaned up after resolution)
- Timestamp metadata: 8 bytes per value (separate key:t, permanent)

### References

- CockroachDB transaction protocol: https://www.cockroachlabs.com/docs/stable/architecture/transaction-layer
- Percolator (Google): https://research.google/pubs/pub36726/
- Spanner (Google): https://research.google/pubs/pub39966/
- etcd Raft: https://github.com/etcd-io/raft

---

## Design Decisions

### Context

Antfly needs atomic cross-shard write transactions to ensure batches sent through metadata servers are applied atomically across multiple storage shards. The system uses a multi-raft architecture with separate Raft groups for metadata and each storage shard.

### Key Requirements

1. **Write-only transactions**: No read/write transactions needed -- all updates are overwrites
2. **Metadata as stateless router**: Metadata servers coordinate but don't persist transaction state
3. **No metadata->storage coupling**: Storage shards resolve intents peer-to-peer, not via metadata
4. **Zero read overhead**: Readers should never see or check for write intents
5. **Simple timestamp allocation**: No complex timestamp oracle needed

### Decision 1: WriteIntent Pattern with Coordinator Shard

**Decision**: Use write intents stored separately from user data, with one storage shard acting as transaction coordinator.

**Rationale**:
- Write intents are invisible to readers (stored in separate keyspace: `\x00\x00__txn_intents__:<txnID>:<userKey>`)
- Coordinator shard determined by hashing `txnID` across participating shards
- Coordinator stores transaction record in its own Pebble database (not metadata)
- Clean separation: metadata routes, coordinator decides, participants apply

**Alternatives Considered**:
- Metadata as coordinator: Would violate "no metadata storage" requirement
- No coordinator (consensus): Too complex, requires cross-shard Raft
- Visible intents: Would add read path overhead

### Decision 2: Metadata HLC for Timestamp Allocation

**Decision**: Use simple monotonic counter (HLC) on metadata server for transaction timestamps.

```go
type HLC struct {
    logical atomic.Uint64
}

func (h *HLC) Now() uint64 {
    return h.logical.Add(1)
}
```

**Rationale**:
- Metadata is stateless -- counter resets on restart (acceptable for write-only txns)
- No clock synchronization needed (not doing serializable reads)
- Fast allocation (atomic increment, no Raft)
- Provides ordering for debugging/observability

**Alternatives Considered**:
- Metadata Raft commit index: Too slow, couples timestamp to metadata Raft throughput
- Coordinator Raft index: Different coordinators have incomparable timestamps
- TrueTime/Spanner: Overkill for write-only transactions

### Decision 3: Push-Only Intent Resolution

**Decision**: Coordinator proactively notifies participants to resolve intents. No participant polling.

**Rationale**:
- **Simpler**: Participants have no background jobs, just apply when notified
- **More efficient**: 1 coordinator scanning vs N participants polling
- **Self-healing**: Coordinator retries every 30 seconds until cleanup
- **Fast recovery**: New coordinator leader immediately re-notifies on election

**Recovery Mechanism**:
```go
// Coordinator runs this on Raft leader
func transactionRecoveryLoop() {
    // On leader election: immediately re-notify all recent txns
    // Every 30s: re-scan and re-notify committed txns from last 5 min
    // After 5 min: cleanup old transaction records
}
```

**Trade-offs**:
- Resolution within 30 seconds worst-case
- Idempotent notifications (safe to retry)
- Coordinator does periodic scanning (lightweight -- just local Pebble scan)

### Decision 4: Intent Storage in Separate Keyspace

**Decision**: Store intents with prefix `\x00\x00__txn_intents__:<txnID>:<userKey>`, completely separate from user keyspace.

**Rationale**:
- **Zero read overhead**: Normal Get/Scan operations skip intent keyspace entirely
- **Clean separation**: Intents are transaction machinery, not user data
- **Easy resolution**: Scan by txnID prefix to resolve all intents for a transaction
- **No MVCC needed**: Since writes are overwrites and readers don't see intents

**Intent Value Structure**:
```protobuf
message WriteIntent {
  bytes value = 1;              // The provisional write
  bytes txn_id = 2;             // Transaction ID
  uint64 timestamp = 3;         // From metadata HLC
  TxnStatus status = 4;         // Pending
  bytes coordinator_shard = 5;  // Which shard owns txn record
  bool is_delete = 6;           // True for delete intents
}
```

### Decision 5: Coordinator Selection by Hash

**Decision**: Deterministically select coordinator via `hash(txnID) % numParticipants`.

```go
func pickCoordinator(txnID uuid.UUID, participants map[types.ID]*ShardBatch) types.ID {
    hash := xxhash.Sum64(txnID[:])
    shardIDs := sortedKeys(participants)
    return shardIDs[hash % uint64(len(shardIDs))]
}
```

**Rationale**:
- **Deterministic**: All nodes agree on coordinator without coordination
- **Load distribution**: Evenly spreads coordinator role across shards
- **Fault-tolerant**: Coordinator failure leads to Raft electing new leader with same txn record

### Decision 6: Metadata Routes Participant Queries to Coordinator

**Decision**: When participants need transaction status (during recovery), they query through metadata server which routes to the coordinator shard.

**Flow**:
```
Participant -> Metadata (route request)
            -> Coordinator Shard (get txn status)
            -> Metadata (route response)
            -> Participant (apply resolution)
```

**Note**: In push-only design, this path is rarely used (only if notifications fail and coordinator retries).

### Decision 7: Transaction Record Cleanup

**Decision**: Coordinator retains transaction records for 5 minutes after commit/abort, then deletes them.

**Rationale**:
- **5 minutes** allows for: multiple notification retries (10 attempts at 30s intervals), network partition recovery, participant recovery from crashes
- **Bounded memory**: Old transactions don't accumulate
- **Safe deletion**: After 5 min without notifications, participants have resolved intents

```go
if record.Status != TxnStatus_Pending &&
   time.Since(record.CommittedAt) > 5*time.Minute {
    db.pdb.Delete(txnKey, pebble.Sync)
}
```

### Decision 8: Two-Phase Commit Protocol

**Decision**: Use simplified 2PC with push-based recovery:

**Phase 1: Prepare**
1. Metadata allocates timestamp (HLC)
2. Coordinator creates transaction record (status: Pending)
3. Participants write intents in parallel

**Phase 2: Commit**
4. Coordinator updates status to Committed (commit point)
5. Coordinator notifies participants to resolve intents
6. Participants convert intents to actual writes

**Simplified vs Traditional 2PC**:
- No prepare vote: Participants don't vote (write-only, no conflicts)
- No participant uncertainty: Intents are invisible to readers
- Push-based: Coordinator drives resolution (no participant timeout logic)

### Decision 9: Timestamp-Based Conflict Resolution

**Decision**: Use HLC timestamps to deterministically resolve conflicting writes. Higher timestamp always wins (Last-Write-Wins).

**Implementation**:
```go
// Storage layout - timestamp in separate key (following embeddings/summaries pattern)
user:key:s              -> document (unchanged, no metadata)
user:key:i:index:e      -> embedding vector
user:key:i:index:s      -> summary text
user:key:t              -> timestamp (uint64, 8 bytes)

// During intent resolution, check existing value's timestamp
func (db *DBImpl) shouldWriteValue(key []byte, newTimestamp uint64) (bool, error) {
    txnKey := append(key, TransactionSuffix...)  // key:t
    existingBytes, _ := db.pdb.Get(txnKey)
    existingTimestamp := binary.LittleEndian.Uint64(existingBytes)
    return newTimestamp > existingTimestamp, nil
}
```

**Conflict Scenarios**:
```
Transaction T1 (ts=100): write key="user:123" value="Alice"
Transaction T2 (ts=150): write key="user:123" value="Bob"

Case 1 - T1 resolves first:
  1. T1 resolves -> writes Alice (ts=100)
  2. T2 resolves -> writes Bob (ts=150 > 100)

Case 2 - T2 resolves first:
  1. T2 resolves -> writes Bob (ts=150)
  2. T1 resolves -> skips (ts=100 < 150)

Result: Bob wins in both cases (deterministic!)
```

**Trade-offs**:
- Simple: No conflict detection, no aborts, no deadlock prevention
- Predictable: Timestamp ordering is explicit and visible
- Fast: Separate key storage is ~5x faster than embedding in document (50-100us vs 200-400us)
- Clean: Documents have no internal metadata fields
- Storage overhead: 8 bytes per value (separate key:t for timestamp)
- Application constraints: Users must avoid read-modify-write patterns without OCC

**Safe Use Cases**: Overwrites (batch imports), append-only logs, idempotent operations, time-series data

**Unsafe Use Cases** (application-level race without OCC): Read-modify-write, counters, inventory

### Summary of Key Invariants

1. **Commit point**: Transaction is committed when coordinator writes `TxnStatus_Committed` to Pebble via Raft
2. **Visibility**: Write intents are NEVER visible to readers (separate keyspace)
3. **Idempotence**: All operations (write intent, resolve intent, notify) are idempotent
4. **Coordinator authority**: Only coordinator can commit/abort a transaction
5. **Raft guarantees**: All state changes go through Raft (coordinator txn record, participant intents, resolutions)
6. **Bounded resolution**: Intents resolved within 5 minutes worst-case (10 retries x 30s)

### Non-Goals (Explicitly Out of Scope for initial implementation)

- Read/write transactions (only write-only)
- Serializable isolation (no timestamp oracle, no MVCC)
- Interactive transactions (entire batch submitted at once)
- Cross-datacenter transactions (assumes single region)
- Long-running transactions (5 min timeout)
- Write-write conflict detection (uses Last-Write-Wins instead)
- Transaction abort on conflict (all transactions commit, highest timestamp wins)
- Optimistic concurrency control (no version checking, no compare-and-swap)

### Implemented Features

1. **Timestamp-based conflict resolution**: HLC timestamps provide deterministic Last-Write-Wins semantics (Decision 9). Optimized with separate key:t storage (5x faster: 50-100us vs 200-400us per check). 8 bytes per value, following embeddings/summaries pattern.
2. **Observability metrics**: 7 Prometheus metrics tracking transaction operations, latency, intents, and recovery
3. **Batch API integration**: Multi-shard batches automatically use distributed transactions for atomicity

### Known Limitations

1. **No batching**: Metadata does not batch multiple client transactions into one coordinator operation
2. **No intent compression**: Intents are not compressed (though values are already zstd compressed)
3. **No backpressure**: No mechanism to handle coordinator overload
4. **All-or-nothing participant writes**: If a participant fails during intent write, the entire transaction aborts (no per-participant retry)
5. **Fixed conflict resolution**: Timestamp-based Last-Write-Wins is the only conflict resolution strategy
6. **Write-only transactions**: Read-write serializable transactions with MVCC are not yet supported

---

## Read-Modify-Write & Multi-Table Transactions

### Context

Antfly has implicit single-table transactions: when a batch spans multiple shards within one table, the metadata server automatically uses 2PC (`ExecuteTransaction` in `src/metadata/transaction.go`). Two gaps existed:

1. **No cross-table atomicity**: `BatchWrite` (`src/metadata/api.go:1156`) is scoped to a single `tableName`. There's no way to atomically write to "users" and "orders" in one request, despite the 2PC engine being table-agnostic at the shard level.

2. **No read-modify-write transactions**: Clients that need to read a document, compute a new value, and write it back have no protection against lost updates. Transforms (`$inc`, `$set`, etc.) cover some cases but not arbitrary application logic.

Both can be addressed incrementally, reusing the existing 2PC infrastructure (`ExecuteTransaction` operates on `map[types.ID][][2][]byte` keyed by shard ID -- it never references table names).

### Phase 1: Cross-Table Batch Transactions

#### Approach

Add a new `/batch` endpoint (table-independent) that accepts writes/deletes/transforms keyed by table name. For each table, partition keys into shards using `Table.PartitionKeysByShard()` (`src/store/table.go:155`), merge all shard maps, and feed them into the existing `ExecuteTransaction`.

#### Changes

**OpenAPI spec** (`src/metadata/api.yaml`):
- `MultiBatchRequest` -- `{ tables: map<tableName, BatchRequest>, sync_level?: SyncLevel }`
- `MultiBatchResponse` -- `{ tables: map<tableName, BatchResponse> }`
- `POST /batch` -- operationId `multiBatchWrite`, tag `data_operations`

**New handler** (`src/metadata/api_multi_batch.go`):
```
func (t *TableApi) MultiBatchWrite(w, r)
  1. Decode MultiBatchRequest
  2. Auth: extract username from X-Authenticated-User header and call
     ms.um.Enforce(username, ResourceTypeTable, tableName, PermissionTypeWrite)
     for each table in the request body
  3. For each table in request.Tables:
     a. tm.GetTable(tableName) -- validate exists
     b. Inject _timestamp if not present
     c. Validate docs against table schema via table.ValidateDoc()
     d. table.PartitionKeysByShard(insertKeys) -- resolve shards for inserts
     e. table.PartitionKeysByShard(deleteKeys) -- resolve shards for deletes
     f. Convert transforms via TransformFromAPI()
     g. Partition transform keys by shard
     h. Check for ShardState_SplittingOff on all resolved shards
     i. Accumulate into merged maps: writes[shardID], deletes[shardID], transforms[shardID]
  4. If single shard total -> forwardBatchToShard (fast path)
     If multiple shards -> ExecuteTransaction(ctx, writes, deletes, transforms, syncLevel)
  5. Return MultiBatchResponse with per-table counts
```

**Go SDK** (`antfly-go/antfly/operations.go`):
- Add `MultiBatch(ctx, MultiBatchRequest)` method alongside existing `Batch()`

**E2E test** (`e2e/cross_table_transaction_test.go`):
- Cross-table commit: two tables, batch insert to both, verify both readable
- Cross-table with mixed ops: insert table A + delete table B atomically
- Abort: invalid doc in one table, verify neither table modified
- Guard: `skipUnlessEnv(t, "RUN_TXN_TESTS")`

**Files NOT modified**: `src/metadata/transaction.go` (already works with shard IDs), `src/store/db/` (store layer is table-unaware), `src/store/client/` (existing RPCs sufficient)

### Phase 2: OCC-Based Read-Modify-Write Transactions

#### Approach

Stateless OCC: the server holds no transaction state between requests. The client reads documents (capturing version tokens from response headers), computes writes locally, then submits everything in a single commit request. The server validates that all read versions still match, then executes writes via existing 2PC.

#### Step 2a: Expose version tokens on lookups

Thread an HLC timestamp ("version") through the existing lookup path so clients can capture it for OCC validation.

**Store layer** (`src/store/db/db.go`): Add `GetTimestamp(key []byte) (uint64, error)` -- reads `key:t` from Pebble using `encoding.DecodeUint64Ascending` (big-endian). Returns 0 if key doesn't exist.

**Store API** (`src/store/api.go`): Modify `handleLookup` to call `shard.GetTimestamp(key)` and set `X-Antfly-Version` response header.

**Store client** (`src/store/client/store_client.go`): Add `LookupWithVersion(ctx, shardID, key) ([]byte, uint64, error)` -- same HTTP GET as existing `Lookup`, additionally parses `X-Antfly-Version` from response header.

**Metadata API handler** (`src/metadata/api.go`): Modify `LookupKey` handler to use `forwardLookupToShardWithVersion` and set `X-Antfly-Version` header on the HTTP response.

#### Step 2b: OCC commit endpoint

**OpenAPI spec** (`src/metadata/api.yaml`):
- `TransactionReadItem` -- `{ table: string, key: string, version: string }`
- `TransactionCommitRequest` -- `{ read_set: []TransactionReadItem, tables: map<tableName, BatchRequest>, sync_level?: SyncLevel }`
- `TransactionCommitResponse` -- `{ status: "committed"|"aborted", conflict?: { table, key, message }, tables?: map<tableName, BatchResponse> }`
- `POST /transactions/commit` -- returns 200 on commit, 409 on conflict

No `begin` endpoint -- transactions are stateless on the server.

**Handler** (`src/metadata/api_transaction.go`):
```
func (t *TableApi) CommitTransaction(w, r)
  1. Decode TransactionCommitRequest
  2. Auth: um.Enforce() with read permission on read-set tables,
     write permission on write-set tables
  3. VALIDATE read set (parallel via errgroup):
     For each (table, key, version) in read_set (concurrent):
       a. tm.GetTable(table)
       b. table.FindShardForKey(key)
       c. forwardLookupToShardWithVersion(ctx, shardID, key) -> currentVersion
       d. Version "0" means "key did not exist at read time"
       e. If currentVersion != version -> return 409
  4. WRITE phase (reuses Phase 1 multi-table logic):
     Convert read set -> predicates map[shardID][]*VersionPredicate
     Call ExecuteTransaction(ctx, writes, deletes, transforms, predicates, syncLevel)
  5. Return 200 with committed status
```

**Go SDK** (`antfly-go/antfly/`):
```go
type Transaction struct {
    client  *AntflyClient
    readSet []TransactionReadItem
}

func (c *AntflyClient) NewTransaction() *Transaction

func (tx *Transaction) Read(ctx context.Context, table, key string) (map[string]any, error)
    // calls LookupKey, captures X-Antfly-Version header, appends to readSet

func (tx *Transaction) Commit(ctx context.Context, writes map[string]BatchRequest) (*TransactionCommitResult, error)
    // sends read_set + writes to POST /transactions/commit
```

#### Step 2c: Store-level version predicates

Closes the TOCTOU gap between validation reads and the 2PC write by moving version checks inside the 2PC itself.

**Proto changes** (`src/store/db/ops.proto`):
```protobuf
message VersionPredicate {
  bytes key = 1;
  uint64 expected_version = 2;  // 0 means "key must not exist"
}

message WriteIntentOp {
  // ... existing fields ...
  repeated VersionPredicate predicates = 5;  // checked during intent write
}
```

**Store DB** (`src/store/db/db.go`): Add `checkVersionPredicates(predicates []VersionPredicate) error`. Modify `WriteIntent()` to call `checkVersionPredicates` before writing intents. If check fails, return error (intent write rejected).

**Metadata transaction layer** (`src/metadata/transaction.go`): Extend `ExecuteTransaction` signature to accept predicates:
```go
func (ms *MetadataStore) ExecuteTransaction(
    ctx context.Context,
    writes map[types.ID][][2][]byte,
    deletes map[types.ID][][]byte,
    transforms map[types.ID][]*db.Transform,
    predicates map[types.ID][]*db.VersionPredicate,  // NEW
    syncLevel db.Op_SyncLevel,
) error
```

If any shard rejects (predicate failure), abort transaction and return conflict error. Existing callers pass `nil` predicates (no behavior change).

### Implementation Order

1. Phase 1 (cross-table batch) -- API routing only, no store changes
2. Phase 2a (version on lookups) -- store + metadata plumbing, backward compatible
3. Phase 2b (OCC commit endpoint + SDK) -- new endpoint, uses Phase 1 + 2a infrastructure
4. Phase 2c (version predicates) -- integrates predicate checks into 2PC for airtight OCC

### Verification

```bash
# Phase 1
make generate
GOEXPERIMENT=simd go build ./...
GOEXPERIMENT=simd go test ./src/metadata/...
RUN_TXN_TESTS=true GOEXPERIMENT=simd go test -v ./e2e -run CrossTable -timeout 10m

# Phase 2 (2a + 2b + 2c)
make generate
GOEXPERIMENT=simd go build ./...
GOEXPERIMENT=simd go test ./src/store/db/... ./src/metadata/...
RUN_TXN_TESTS=true GOEXPERIMENT=simd go test -v ./e2e -run OCC -timeout 10m

# SDK compatibility
cd antfly-go/antfly && go test ./...
cd ts && npm test
cd py && uv run pytest
```

---

## Cross-Table Joins

### Overview

Implemented efficient cross-table join operations for Antfly with three join strategies optimized for different data distribution patterns.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Query Request                            │
│                    (with join clause)                           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Join Planner                                │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │ Table Stats     │  │ Cost Estimator  │  │ Plan Cache      │  │
│  │ (HyperLogLog)   │  │                 │  │                 │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Strategy Selection                            │
│  ┌───────────────┐  ┌───────────────┐  ┌───────────────────┐    │
│  │   Broadcast   │  │ Index Lookup  │  │     Shuffle       │    │
│  │  (< 10MB)     │  │ (Selective)   │  │  (Large-Large)    │    │
│  └───────────────┘  └───────────────┘  └───────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Join Executor                                │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                   Memory Manager                         │    │
│  │              (Spill-to-disk if needed)                   │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Join Result                                 │
│            (merged rows + execution stats)                       │
└─────────────────────────────────────────────────────────────────┘
```

### Join Strategies

| Strategy | Best For | How It Works |
|----------|----------|--------------|
| **Broadcast** | Small dimension tables (< 10MB) | Fetches entire right table, builds hash table, performs local join on each left row |
| **Index Lookup** | Selective joins with indexed keys | Collects join keys from left table, performs batch lookups on right table |
| **Shuffle** | Large-large table joins | Hash-partitions both tables by join key, joins matching partitions in parallel |

### Implementation

**Phase 1: Foundation**
- Added `JoinClause`, `JoinCondition`, `JoinFilters` to API schema
- Created join query planner with cost-based strategy selection
- Implemented broadcast join executor
- Implemented index lookup join executor
- Added table statistics collection via HyperLogLog

**Phase 2: Optimization**
- Filter pushdown to reduce data before join
- Join order optimization for multi-way joins
- Query plan caching with configurable TTL
- Memory management with spill-to-disk

**Phase 3: Shuffle Join**
- Shuffle hash join for large-large table joins
- Sort-merge join alternative for sorted data

### Files Created

| File | Lines | Description |
|------|-------|-------------|
| `src/metadata/join/types.go` | 256 | Core types: Strategy, Type, Condition, Plan, HashTable |
| `src/metadata/join/planner.go` | 365 | Query planner with cost-based strategy selection |
| `src/metadata/join/executor.go` | 293 | Base executor interface and helpers |
| `src/metadata/join/broadcast.go` | 316 | Broadcast join executor |
| `src/metadata/join/index_lookup.go` | 353 | Index lookup executor with caching |
| `src/metadata/join/shuffle.go` | 490 | Shuffle hash + sort-merge executors |
| `src/metadata/join/statistics.go` | 376 | Table statistics via HyperLogLog |
| `src/metadata/join/memory.go` | 400 | Memory management with spill-to-disk |
| `src/metadata/join/adapter.go` | 304 | Antfly infrastructure integration |
| `src/metadata/join/join_test.go` | 737 | Comprehensive tests |
| `src/metadata/api_join.go` | 497 | JoinService integration |

### API Usage

```json
{
  "query": "*",
  "join": {
    "table": "orders",
    "type": "inner",
    "conditions": [
      {
        "left_field": "customers.id",
        "operator": "eq",
        "right_field": "orders.customer_id"
      }
    ],
    "filters": {
      "right_filter": "status:completed"
    }
  }
}
```

---

## E2E Test Plan

### Overview

Comprehensive e2e test for distributed transactions verifying Two-Phase Commit (2PC) behavior across multiple shards in a **true multi-node cluster**, including commit, abort, and recovery scenarios.

### Multi-Node Cluster Setup

**Cluster Topology:**
- **1 metadata node** (ID 11) -- cluster coordinator
- **2 store nodes** (IDs 1, 2) -- each hosting different shards
- **4 shards** distributed across the 2 store nodes

Each component runs in a separate goroutine with its own ports (API + Raft).

**Startup Flow:**
1. Start metadata server via `metadata.RunAsMetadataServer()`
2. Wait for metadata server to be ready (readyC channel)
3. Start store nodes via `store.RunAsStore()` -- they auto-register with metadata
4. Wait for store nodes to be ready and registered
5. Create table with `NumShards: 4` and `ReplicationFactor: 1`
6. Wait for shards to be allocated across stores and elect leaders

The 2PC path in `src/metadata/api.go:1445` (`executeBatchWithTransaction`) is triggered when a batch operation touches keys that partition to different shards. Cross-shard notifications flow through metadata server via `/_internal/v1/shard/{shardID}/txn/resolve` endpoint.

### Key Partitioning Strategy

Keys are assigned to shards based on lexicographic byte ranges. With 4 shards distributed across 2 stores:
```go
"0_account_a"  // Low byte range  -> Store 1
"4_account_b"  // Mid-low         -> Store 1
"8_account_c"  // Mid-high        -> Store 2
"c_account_d"  // High byte range -> Store 2
```

### Test Functions

#### 1. TestE2E_DistributedTransaction_MultiShardCommit

Verify batch operation touching multiple shards commits atomically via 2PC.

Steps:
1. Skip if `testing.Short()` or `RUN_TXN_TESTS != "true"`
2. Start swarm, create table with `NumShards: 4` (no indexes needed)
3. Wait for shards to elect leaders
4. Insert initial documents across 4 shards using `client.Batch()`
5. Perform multi-key update via `client.Batch()` that modifies all 4 keys
6. Verify all updates committed using `client.LookupKey()` for each key

#### 2. TestE2E_DistributedTransaction_MultiShardAbort

Verify that when context is cancelled mid-transaction, no data is committed.

Steps:
1. Start swarm, create table with 4 shards
2. Insert initial documents across shards
3. Store original values via `client.LookupKey()`
4. Create a short-timeout context (1ms)
5. Attempt batch update that should timeout mid-transaction
6. Verify all values unchanged (atomicity -- nothing committed)

#### 3. TestE2E_DistributedTransaction_RecoveryNotification

Verify the recovery loop properly resolves committed transactions.

Steps:
1. Start swarm, create table with 4 shards
2. Perform successful multi-shard transaction
3. Verify data is correct
4. Wait for recovery loop interval (~35 seconds) OR verify intents are cleaned up
5. Perform another read to confirm data is still accessible

#### 4. TestE2E_DistributedTransaction_AtomicMultiKeyUpdate

Classic "bank transfer" scenario -- atomic update of multiple keys.

Steps:
1. Start swarm, create table with 2+ shards
2. Insert accounts on different shards: `"0_alice": {"balance": 1000}`, `"8_bob": {"balance": 0}`
3. Perform atomic "transfer" via batch: `"0_alice": {"balance": 500}`, `"8_bob": {"balance": 500}`
4. Verify both updates applied atomically
5. Verify sum of balances preserved (1000 total)

### Multi-Node Cluster Helper

```go
type DistributedCluster struct {
    T              *testing.T
    Logger         *zap.Logger
    Config         *common.Config
    MetadataAPIURL string
    StoreAPIURLs   []string
    Client         *antfly.AntflyClient
    Cancel         context.CancelFunc
    DataDir        string
}

func startDistributedCluster(t *testing.T, ctx context.Context) *DistributedCluster
```

### Key Differences from Single-Node Swarm

| Aspect | Single-Node Swarm | Multi-Node Cluster |
|--------|-------------------|-------------------|
| Metadata nodes | 1 (in-process) | 1 (separate goroutine) |
| Store nodes | 1 (in-process) | 2 (separate goroutines) |
| Shard allocation | Disabled | Enabled -- shards distributed |
| Transaction notifications | Local function calls | HTTP via `/_internal/v1/shard/{shardID}/txn/resolve` |
| Network partitions | Not testable | Potentially testable |
| Realistic 2PC | Partial | Full cross-node coordination |

### Running the Tests

```bash
# Run all transaction e2e tests
RUN_TXN_TESTS=true go test -v ./e2e -run DistributedTransaction -timeout 5m

# Run specific test
RUN_TXN_TESTS=true go test -v ./e2e -run TestE2E_DistributedTransaction_MultiShardCommit -timeout 2m
```

### Critical Files

| File | Purpose |
|------|---------|
| `e2e/docsaf_test.go` | Pattern for swarm startup and test structure |
| `e2e/test_helpers.go` | `GetFreePort()`, `CreateTestConfig()`, `WaitForHTTP()` |
| `src/metadata/runner.go` | `RunAsMetadataServer()` -- starts metadata node |
| `src/store/runner.go` | `RunAsStore()` -- starts store node with auto-registration |
| `src/metadata/transaction.go` | `ExecuteTransaction()` -- 2PC orchestration |
| `src/metadata/api.go:1435-1515` | Batch API transaction path decision |
| `src/store/table.go:135-145` | `FindShardForKey()` -- key partitioning |
| `antfly-go/antfly/operations.go` | Client SDK: `Batch()`, `LookupKey()` |
| `Procfile` | Reference for production multi-node topology |

---

## TLA+ Stuck-Pending Fix

### Context

When `commitTransaction` or `abortTransaction` fails (network error, context timeout), the txn record stays Pending (status=0) with intents on participant shards. The recovery loop (`notifyPendingResolutions` at `db.go:724`) only processes records with `status != 0`, so Pending records are **ignored forever**. This means:
- Pending intents permanently block future OCC transactions via `hasConflictingIntentForKey`
- `created_at` is stored at init time (`db.go:3905`) but never read by any cleanup logic

Three deliverables: TLA+ spec update, Go fix, unit test.

### 1. TLA+ Spec -- Model the bug and the fix

**File: `specs/tla/AntflyTransaction.tla`**

**New constant: `StalePendingThreshold`** -- minimum clock ticks before a Pending txn is considered stale.

**New action: `OrchestratorCrashPrepare(t)`** -- orchestrator crashes during prepare phase, leaving txn record Pending with intents potentially written:
```
OrchestratorCrashPrepare(t) ==
    /\ txnStatus[t] \in {"preparing", "predicatesChecked"}
    /\ txnRecords[t] = "pending"
    /\ txnStatus' = [txnStatus EXCEPT ![t] = "done"]
    /\ UNCHANGED all other vars
```

**New action: `RecoveryAutoAbort(t)`** -- recovery loop detects stale Pending txn and auto-aborts:
```
RecoveryAutoAbort(t) ==
    /\ txnRecords[t] = "pending"
    /\ txnStatus[t] = "done"
    /\ clock - txnTimestamp[t] >= StalePendingThreshold
    /\ txnRecords' = [txnRecords EXCEPT ![t] = "aborted"]
    /\ UNCHANGED all other vars
```

After `RecoveryAutoAbort`, the existing `RecoveryResolve` and `CleanupTxnRecord` actions handle intent resolution and cleanup since they accept `txnRecords[t] = "aborted"`.

**New liveness property: `EventualPendingResolution`**:
```
EventualPendingResolution ==
    \A t \in Txns :
        (txnRecords[t] = "pending" /\ txnStatus[t] = "done")
        ~> txnRecords[t] \in {"aborted", "deleted"}
```

Both new actions added to `Next`. `WF_vars(RecoveryAutoAbort(t))` added to `Fairness`. No fairness for `OrchestratorCrashPrepare` (crashes are non-deterministic).

**Model checking files** (`specs/tla/MC.tla` and `specs/tla/AntflyTransaction.cfg`):
- Add `MCStalePendingThreshold == 2` to MC.tla
- Add `StalePendingThreshold <- MCStalePendingThreshold` to cfg CONSTANTS
- Add `EventualPendingResolution` to PROPERTIES

**Verification**: Run TLC, expect all 6 safety invariants + 4 liveness properties to pass.

### 2. Unit Test -- Catches the bug before the fix

**New file: `src/store/db/transaction_stale_pending_test.go`**

Three tests following the pattern in `transaction_orphaned_intents_test.go`:

**`TestStalePendingTransaction_AutoAbort`**:
1. Create test DB
2. Wire `proposeAbortTransactionFunc` to directly call `AbortTransaction` (bypass Raft for unit test)
3. Wire `failingShardNotifier`
4. Init a Pending txn + write intents on coordinator
5. Backdate `created_at` to 10 minutes ago (past 5-min cutoff)
6. Run `notifyPendingResolutions`
7. Assert: before fix, txn stays Pending. After fix, txn is Aborted.

**`TestStalePendingTransaction_NotAbortedIfRecent`**:
1. Same setup but `created_at` is recent (1 minute ago)
2. Run `notifyPendingResolutions`
3. Assert txn stays Pending -- recovery loop must not abort active transactions

**`TestStalePendingTransaction_IntentConflictCleared`**:
1. Create stuck Pending txn with intent on key "k"
2. Verify `hasConflictingIntentForKey("k")` returns conflict
3. Backdate, run recovery to auto-abort
4. Wire `proposeResolveIntentsFunc` to call `ResolveIntents` directly
5. Run recovery again (now status=aborted, resolves intents)
6. Verify `hasConflictingIntentForKey("k")` returns no conflict

### 3. Go Fix -- Auto-abort stale Pending transactions

**File: `src/store/db/db.go`**

**A. Add `proposeAbortTransactionFunc` field** (~line 294 near existing `proposeResolveIntentsFunc`):
```go
proposeAbortTransactionFunc func(ctx context.Context, op *AbortTransactionOp) error
```
Add setter `SetProposeAbortTransactionFunc`.

**B. Modify `notifyPendingResolutions`** (after line 720, before line 724):

Insert new block that handles Pending (status=0) records:
```go
createdAt, _ := record["created_at"].(float64)
if int32(status) == 0 && int64(createdAt) < cutoff {
    // Propose abort through Raft
    if db.proposeAbortTransactionFunc != nil {
        abortOp := AbortTransactionOp_builder{TxnId: txnIDBytes}.Build()
        err = db.proposeAbortTransactionFunc(ctx, abortOp)
    }
    continue  // Next iteration handles the now-aborted txn
}
```

**C. Wire in storedb.go** (~line 100-103 after `SetProposeResolveIntentsFunc`):

```go
func (s *StoreDB) proposeAbortTransaction(ctx context.Context, op *AbortTransactionOp) error {
    kvOp := Op_builder{Op: Op_OpAbortTransaction, AbortTransaction: op}.Build()
    return s.proposeOnlyWriteOp(ctx, kvOp)
}
```

**D. Log the silent abort failure** in `metadata/transaction.go:54`:
Change `_ = ms.abortTransaction(...)` to log the error with a warning.

### Implementation Order

1. TLA+ spec changes -- run TLC to verify
2. Unit test (should fail before fix, documenting the bug)
3. Go fix in `db.go` + `storedb.go`
4. Run unit test again (should pass)
5. Run existing transaction unit tests to verify no regression

### Verification

```bash
# TLC
cd specs/tla && "$JAVA" -cp "$TLA2TOOLS" tlc2.TLC MC.tla -config AntflyTransaction.cfg -workers auto -deadlock

# Unit tests
GOEXPERIMENT=simd go test -v -run TestStalePending ./src/store/db/ -timeout 2m

# Existing transaction tests (regression)
GOEXPERIMENT=simd go test -v -run TestOrphaned ./src/store/db/ -timeout 2m
GOEXPERIMENT=simd go test -v -run TestTransaction ./src/store/db/ -timeout 2m
```
