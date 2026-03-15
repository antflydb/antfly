# S3 Storage Architecture

## Overview

This document explains the architectural decisions and design patterns behind Antfly's S3 storage implementation. It covers the multi-writer problem, LeaderFactory integration, Pebble's checkpointing mechanism, and garbage collection strategy.

## Table of Contents

- [The Multi-Writer Problem](#the-multi-writer-problem)
- [Leader-Only Writes Solution](#leader-only-writes-solution)
- [LeaderFactory Integration](#leaderfactory-integration)
- [Pebble Checkpointing with S3](#pebble-checkpointing-with-s3)
- [Reference Markers and Garbage Collection](#reference-markers-and-garbage-collection)
- [Why Raft is Still Needed](#why-raft-is-still-needed)

## The Multi-Writer Problem

### The Conflict

With traditional 3-way Raft replication, each replica independently runs Pebble compaction:

```
Raft Group (Shard 123)
┌─────────────────────────────────────────────────────┐
│ Replica A (Leader)    → Local Pebble → Compaction  │
│ Replica B (Follower)  → Local Pebble → Compaction  │
│ Replica C (Follower)  → Local Pebble → Compaction  │
└─────────────────────────────────────────────────────┘
         ↓                    ↓                ↓
    All write to S3: bucket/shard-123/

Problem:
- Each replica independently compacts
- File numbers may overlap
- Conflicting S3 object names → corruption
```

### Pebble's CreatorID Mechanism

Pebble provides `CreatorID` to handle multiple writers by namespacing objects:

```go
// Replica A
db.SetCreatorID("replica-a")
// Writes: s3://bucket/creator-replica-a/000123.sst

// Replica B
db.SetCreatorID("replica-b")
// Writes: s3://bucket/creator-replica-b/000123.sst

// Replica C
db.SetCreatorID("replica-c")
// Writes: s3://bucket/creator-replica-c/000123.sst
```

**Result**: No conflicts, but **3x storage cost** — completely defeating the purpose of S3 storage!

### Solution Architectures Considered

We evaluated three approaches:

#### 1. Leader-Only Writes (Chosen)
Only the Raft leader writes sstables to S3. Followers read foreign objects.
- ✅ Real cost savings (1x storage in S3)
- ✅ Compatible with existing Raft architecture
- ✅ Fast failover (WAL still replicated)
- ⚠️ Leader does more work (compaction + S3 uploads)

#### 2. WAL-Only Raft + Separate Storage (Neon-Style)
Decouple consensus (Raft) from storage (Pebble+S3).
- ✅ Maximum cost savings
- ✅ Independent scaling
- ⚠️ Major architecture change (not pursued)

#### 3. Hybrid WAL Replication
Raft replicates WAL (fast), leader writes sstables to S3.
- ✅ Fast failover
- ⚠️ Similar to approach #1, but more complex

**Decision**: We chose **leader-only writes** for the best balance of cost savings, operational simplicity, and compatibility with our existing Raft implementation.

## Leader-Only Writes Solution

### Architecture

```
┌──────────────────────────────────┐
│ Leader (Replica A)               │
│  • Accepts writes                │
│  • Compacts locally              │
│  • Writes sstables → S3          │
│  • CreatorID: "shard-123"        │
│  • WAL kept local (failover)     │
└──────────────────────────────────┘
            ↓ writes sstables
     S3: shard-123/000123.sst  (1x copy!)
            ↑ reads         ↑ reads
┌──────────────────┐  ┌──────────────────┐
│ Follower (Rep B) │  │ Follower (Rep C) │
│  • Reads from S3 │  │  • Reads from S3 │
│  • WAL local     │  │  • WAL local     │
└──────────────────┘  └──────────────────┘
```

### Cost Analysis

**Traditional (all local)**:
- 3 replicas × 1TB = 3TB × $0.10/GB = $300/month

**Leader-only S3**:
- Leader hot data: 200GB × $0.10 = $20/month
- S3 cold data: 800GB × $0.023 = $18/month
- Follower WAL: 2 × 50GB × $0.10 = $10/month
- **Total: $48/month (84% savings)**

## LeaderFactory Integration

### Existing LeaderFactory Pattern

Antfly uses `LeaderFactory` for leader-only work throughout the codebase:

```go
// src/raft/raft.go - Raft detects leadership changes
if rd.RaftState == raft.StateLeader {
    rc.isLeader = true
    rc.startLeader()  // ← Calls LeaderFactory goroutine
} else if rc.isLeader {
    rc.isLeader = false
    rc.stopLeader()   // ← Cancels context
}
```

**Current uses**:
- Index enrichers (src/store/db.go:327) - Only leader generates embeddings
- Metadata reconciliation (src/metadata/runner.go:161) - Only leader rebalances shards

### Leadership-Aware S3 Storage Wrapper

We extend this pattern to S3 storage with a wrapper:

```go
// src/store/s3storage/leader_aware.go
type LeaderAwareS3Storage struct {
    underlying *S3Storage
    isLeader   *atomic.Bool
}

func (s *LeaderAwareS3Storage) CreateObject(objectName string) (io.WriteCloser, error) {
    if !s.isLeader.Load() {
        // Not leader - Pebble will keep sstable local
        return nil, fmt.Errorf("not raft leader, cannot write to S3: %s", objectName)
    }
    // Leader - write to S3
    return s.underlying.CreateObject(objectName)
}

func (s *LeaderAwareS3Storage) ReadObject(ctx context.Context, objectName string) (remote.ObjectReader, error) {
    // All replicas can read from S3 (foreign objects)
    return s.underlying.ReadObject(ctx, objectName)
}
```

### Integration Flow

#### 1. DBImpl Initialization

```go
// src/store/db.go
type DBImpl struct {
    // ... existing fields ...
    isLeader   atomic.Bool
    s3Storage  *s3storage.LeaderAwareS3Storage
}

func (db *DBImpl) Open() error {
    // ... existing Pebble options ...

    if db.antflyConfig.S3Info != nil && db.antflyConfig.S3Info.Enabled {
        // Create base S3 storage
        minioClient, _ := common.NewMinioClient(db.antflyConfig.S3Info.Endpoint)
        baseS3, _ := s3storage.NewS3Storage(
            minioClient,
            db.antflyConfig.S3Info.Bucket,
            db.antflyConfig.S3Info.Prefix,
        )

        // Wrap with leadership awareness
        db.s3Storage = s3storage.NewLeaderAwareS3Storage(
            baseS3,
            &db.isLeader,  // ← Share atomic bool
        )

        // Configure Pebble with S3 backend
        pebbleOpts.Experimental.RemoteStorage = remote.MakeSimpleFactory(
            map[remote.Locator]remote.Storage{"s3": db.s3Storage},
        )
        pebbleOpts.Experimental.CreateOnShared = remote.CreateOnSharedAll
        pebbleOpts.Experimental.CreateOnSharedLocator = "s3"

        // Set CreatorID to shard ID (not replica ID!)
        creatorID := base.MakeCreatorID(uint64(db.shardID))
    }

    db.pdb, err = pebble.Open(pebbleDir, pebbleOpts)
    // ...
}
```

#### 2. LeaderFactory Updates Flag

```go
// src/store/db.go
func (db *DBImpl) LeaderFactory(ctx context.Context, persistFunc PersistFunc) error {
    // Set leadership flag when we become leader
    db.isLeader.Store(true)
    defer db.isLeader.Store(false)  // Clear when we lose leadership

    db.logger.Info("Starting leader factory",
        zap.Bool("s3Enabled", db.s3Storage != nil),
    )

    // Start index enrichers (existing code)
    for {
        if err := db.indexManager.StartLeaderFactory(ctx, persistFunc); err != nil &&
            !errors.Is(err, context.Canceled) {
            db.logger.Error("Failed to start index manager leader factory", zap.Error(err))
        }
        select {
        case <-ctx.Done():
            db.logger.Info("Leader factory context cancelled")
            return ctx.Err()
        case <-db.restartIndexManagerFactory:
            // ... restart logic ...
        }
    }
}
```

### Lifecycle Example

**Time 0: Node A is leader**
```
Node A (Leader):
  • LeaderFactory started, isLeader=true
  • Pebble compaction → CreateObject() → isLeader check → Write to S3 ✓
  • S3: shard-123/000001.sst

Node B (Follower):
  • LeaderFactory NOT running, isLeader=false
  • Pebble compaction → CreateObject() → isLeader check → Keep local ✓
  • Local: /data/shard-123/000001.sst
```

**Time 60: Node A crashes, Node B elected leader**
```
Node B (NEW Leader):
  • Raft: startLeader() → LeaderFactory started
  • isLeader=true
  • Pebble compaction → CreateObject() → Write to S3 ✓
  • S3: shard-123/000002.sst
  • Can read Node A's sstables from S3 (foreign objects)

Node C (Follower):
  • Still follower, isLeader=false
  • Keeps compactions local
```

**Benefits**:
- ✅ Uses existing LeaderFactory pattern (proven in production)
- ✅ Minimal code changes (just wrapper + flag)
- ✅ Automatic failover (handled by Raft)
- ✅ Clean separation (LeaderFactory manages timing, wrapper manages writes)

## Pebble Checkpointing with S3

### Remote Files Are Not Copied

Pebble's `Checkpoint()` function has special handling for remote storage. From Pebble source:

> "We don't copy remote files. This is desirable as checkpointing is supposed to be a fast operation, and references to remote files can always be resolved by any checkpoint readers by reading the object catalog."

When creating a checkpoint with S3 storage:

**Copied to checkpoint (small)**:
- MANIFEST file (~few KB)
- WAL files (recent uncommitted writes, typically <10MB)
- OPTIONS file
- Object catalog metadata

**Referenced only (large)**:
- All S3 sstables (L0-L6) - Only metadata references created

### Checkpoint Size Comparison

**Traditional local storage**:
```
10GB Shard → Checkpoint: 10GB copied (slow!)
```

**With S3 storage**:
```
10GB Shard → Checkpoint: ~10MB copied (fast!)
  - 10GB sstables in S3 (referenced, not copied)
  - ~10MB MANIFEST + WAL + metadata (copied)
```

### How It Works in Shard Splits

```go
// src/store/db.go
func (db *DBImpl) Split(currRange common.Range, splitKey []byte,
                        destDir1, destDir2 string) error {
    secondHalfSpan := pebble.CheckpointSpan{
        Start: splitKey,
        End:   currRange[1],
    }

    err := db.pdb.Checkpoint(
        destDir2,
        pebble.WithFlushedWAL(),
        pebble.WithRestrictToSpans([]pebble.CheckpointSpan{secondHalfSpan}),
    )
    // Creates checkpoint with references to S3 objects
    // Only copies local MANIFEST/WAL/metadata
}
```

### Shared References

After a split, both shards reference the same S3 sstables:

**S3 Bucket Structure**:
```
bucket/shard-123/
├── 000123.sst (10GB sstable containing keys A-Z)
└── markers/
    ├── 000123.sst.ref-shard-123    ← Original shard
    ├── 000123.sst.ref-shard-124    ← Shard 1 (keys A-M)
    └── 000123.sst.ref-shard-125    ← Shard 2 (keys N-Z)
```

**How it works**:
1. Both shards reference the same sstable - No data duplication
2. Each shard only reads its key range - Pebble filters at read time
3. Reference markers track usage
4. After compaction, each shard creates its own sstables
5. Original shared sstable deleted when all markers removed

## Reference Markers and Garbage Collection

### Pebble's Reference Counting System

Pebble uses reference markers stored in S3 to track object lifecycle:

```
S3 Bucket Structure:
bucket/
├── shard-123/
│   ├── 000001.sst              ← Actual sstable data
│   ├── 000002.sst
│   └── markers/
│       ├── 000001.sst.ref-node-a  ← Reference marker from node A
│       ├── 000001.sst.ref-node-b  ← Reference marker from node B
│       └── 000002.sst.ref-node-a  ← Reference marker from node A
```

### How It Works

**Creating objects**:
```go
// When leader creates a new sstable in S3:
pebbleOpts.Experimental.CreateOnShared = remote.CreateOnSharedAll

// Pebble automatically:
// 1. Uploads 000001.sst to S3
// 2. Creates reference marker: markers/000001.sst.ref-<creatorID>
```

**Compaction**:
```go
// When leader compacts 000001.sst + 000002.sst → 000003.sst:
// 1. Create 000003.sst in S3
// 2. Create marker: markers/000003.sst.ref-<creatorID>
// 3. Remove markers for 000001.sst and 000002.sst
// 4. When all markers removed, objects become eligible for deletion
```

**Garbage collection**:
```go
// Pebble's internal GC process:
// - Periodically scans markers/
// - Finds objects with zero reference markers
// - Deletes unreferenced sstables from S3
```

### Leader-Only Deletes

Our `LeaderAwareS3Storage` wrapper enforces leader-only deletes:

```go
// src/store/s3storage/leader_aware.go
func (s *LeaderAwareS3Storage) Delete(objectName string) error {
    if !s.isLeader.Load() {
        // Not the leader - return error to prevent deletion
        return fmt.Errorf("not raft leader, cannot delete from S3: %s", objectName)
    }
    return s.underlying.Delete(objectName)
}
```

**Why leader-only?**
- Consistent with our leader-only write pattern
- Prevents race conditions between replicas during GC
- Only the leader creates reference markers, so only leader removes them
- Followers never delete objects they're reading

### SharedCleanupMethod

Pebble's `objstorage` package defines cleanup methods:

**SharedRefTracking (Default, what we use)**:
- Pebble manages object lifecycle via reference markers
- Objects deleted when all markers removed
- Automatic cleanup

**SharedNoCleanup**:
- Objects managed externally (not by Pebble)
- Used for backup files, external imports
- Not appropriate for our use case

## Why Raft is Still Needed

Even with S3 providing durability (11 nines), Raft is essential for:

### 1. Leader Election
Determines which node accepts writes and writes sstables to S3.

### 2. Write Ordering
Ensures linearizability and consistency across all operations.

### 3. WAL Replication
Provides fast durability guarantees. Cannot wait for S3 writes (10-50ms latency) for every write.

### 4. Conflict Resolution
Prevents split-brain scenarios where multiple nodes think they're the leader.

### 5. Membership Changes
Adding/removing nodes safely with consensus.

### Hybrid Storage Model

```
Raft Replication (Fast)        S3 Storage (Cheap)
--------------------            ------------------
• WAL files                     • SST files (L0-L6)
• MANIFEST files                • Compacted data
• ~5% of data                   • ~95% of data
• <1ms latency                  • 10-50ms latency
• $0.10/GB/month                • $0.023/GB/month
```

## Configuration Options

### CreateOnSharedAll (Current)

All sstables (L0-L6) go to S3:

```go
pebbleOpts.Experimental.CreateOnShared = remote.CreateOnSharedAll
```

**Pros**:
- Maximum storage efficiency
- Fastest splits (all data in S3)
- Lowest local disk requirements

**Cons**:
- All reads go to S3 (higher latency)
- Requires good block cache configuration

**Best for**: Cost-sensitive deployments, large databases (>1TB)

### CreateOnSharedLower (Alternative)

Only lower levels (L5-L6) go to S3:

```go
pebbleOpts.Experimental.CreateOnShared = remote.CreateOnSharedLower
```

**Pros**:
- Hot data (L0-L4) stays local (faster reads)
- Reduced S3 GET requests
- Lower latency

**Cons**:
- Higher local disk requirements
- Slower splits (need to copy L0-L4)

**Best for**: Latency-sensitive workloads, smaller databases (<500GB)

### Level Distribution

```
L0: 5% of data (newest, unsorted)
L1-L4: 20% of data (warm)
L5-L6: 75% of data (cold, largest) ← Most data here!
```

With `CreateOnSharedLower`:
- 75% in S3 (L5-L6)
- 25% local (L0-L4)
- Still get most split/migration benefits

## References

- [Pebble Shared Storage Design](https://github.com/cockroachdb/pebble/issues/3177)
- [Pebble objstorage Package](https://pkg.go.dev/github.com/cockroachdb/pebble/objstorage)
- [Neon Serverless Postgres](https://neon.tech/blog/architecture-decisions-in-neon)
- [CockroachDB Serverless Architecture](https://www.cockroachlabs.com/blog/how-we-built-cockroachdb-serverless/)
