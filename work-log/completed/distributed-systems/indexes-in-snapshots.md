# Plan: Include Indexes in Raft Snapshots

## Overview

Modify `db.Snapshot()` to include the `indexes/` directory alongside Pebble data so that indexes don't need to be rebuilt when shards move between nodes.

## Architecture Understanding

**Two-level WAL structure:**
```
applyOpBatch()
  → IndexManager WAL → IndexManager plexer
    → Individual Index WALs → Individual plexers
      → Index files
```

**Index types requiring Pause/Resume:**
- `BleveIndexV2` (full_text_v0.go) - has plexer
- `EmbeddingIndex` (aknn_v0.go) - has plexer
- `GraphIndexV0` (graph_v0.go) - no plexer, processes in Batch()

## Approach: Fast Snapshotting (No Drain)

Pause plexers, copy everything (including WALs), resume. WALs replay on restore.

### Snapshot Flow

```
1. Pause IndexManager plexer (stops dispatching to individual indexes)
2. Pause individual index plexers (stops applying to index files)
3. Pebble checkpoint (with WithFlushedWAL)
4. Copy indexes/ directory (includes all WALs + index files)
5. Resume all plexers
```

### Restore Flow

```
1. Extract Pebble data
2. Extract indexes/ directory (if present)
3. Open indexes - WALs replay automatically at both levels
4. If WAL entry references missing document, skip it (Raft replay will re-add)
```

## Implementation Steps

### Step 1: Add Pause/Resume to Index Interface

**File:** `src/store/indexes/indexes.go`

```go
type Index interface {
    // ... existing methods ...
    Pause(ctx context.Context) error
    Resume()
}
```

### Step 2: Implement Pause/Resume on Each Index Type

**Pattern for indexes with plexers (BleveIndexV2, EmbeddingIndex):**

Add fields:
```go
pauseMu     sync.Mutex
paused      atomic.Bool
pausedAckCh chan struct{}
```

Implement methods:
```go
func (idx *Index) Pause(ctx context.Context) error {
    idx.pauseMu.Lock()
    defer idx.pauseMu.Unlock()

    if idx.paused.Load() {
        return nil // already paused
    }

    idx.pausedAckCh = make(chan struct{}) // fresh channel each time
    idx.paused.Store(true)

    // Wake plexer
    select {
    case idx.enqueueChan <- 0:
    default:
    }

    // Wait for ack
    select {
    case <-idx.pausedAckCh:
        return nil
    case <-ctx.Done():
        idx.paused.Store(false)
        return ctx.Err()
    }
}

func (idx *Index) Resume() {
    idx.paused.Store(false)
    select {
    case idx.enqueueChan <- 0:
    default:
    }
}
```

Modify plexer loop:
```go
for {
    if idx.paused.Load() {
        if idx.pausedAckCh != nil {
            close(idx.pausedAckCh)
            idx.pausedAckCh = nil
        }
        <-idx.enqueueChan // wait for resume
        continue
    }
    // ... rest of loop
}
```

**Pattern for GraphIndexV0 (no plexer):**

```go
func (gi *GraphIndexV0) Pause(ctx context.Context) error {
    gi.paused.Store(true)
    // Wait for any in-flight Batch() to complete
    gi.batchMu.Lock()
    gi.batchMu.Unlock()
    return nil
}

func (gi *GraphIndexV0) Resume() {
    gi.paused.Store(false)
}

func (gi *GraphIndexV0) Batch(...) error {
    gi.batchMu.Lock()
    defer gi.batchMu.Unlock()

    if gi.paused.Load() {
        return ErrIndexPaused // or queue for later
    }
    // ... rest of method
}
```

### Step 3: Add Pause/Resume to IndexManager

**File:** `src/store/indexmgr.go`

```go
func (im *IndexManager) Pause(ctx context.Context) error {
    // 1. Pause own plexer first
    // (same pattern as individual indexes)

    // 2. Pause each individual index
    var errs []error
    im.indexes.Range(func(name string, idx Index) bool {
        if err := idx.Pause(ctx); err != nil {
            errs = append(errs, fmt.Errorf("pause %s: %w", name, err))
        }
        return true
    })

    if len(errs) > 0 {
        im.Resume() // rollback
        return errors.Join(errs...)
    }
    return nil
}

func (im *IndexManager) Resume() {
    // Resume indexes first, then own plexer
    im.indexes.Range(func(name string, idx Index) bool {
        idx.Resume()
        return true
    })
    // Resume own plexer
    im.paused.Store(false)
    select {
    case im.enqueueChan <- struct{}{}:
    default:
    }
}

func (im *IndexManager) GetDir() string {
    return im.dir
}
```

### Step 4: Modify db.Snapshot()

**File:** `src/store/db.go` (lines 2423-2454)

```go
func (s *DBImpl) Snapshot(id string) (int64, error) {
    stagingDir := filepath.Join(os.TempDir(), fmt.Sprintf("antfly-snap-%s-%s", id, uuid.NewString()))
    defer os.RemoveAll(stagingDir)

    // Step 1: Pause IndexManager (and all indexes)
    if s.indexManager != nil {
        ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
        defer cancel()

        if err := s.indexManager.Pause(ctx); err != nil {
            s.logger.Warn("Failed to pause IndexManager, snapshot without indexes", zap.Error(err))
        } else {
            defer s.indexManager.Resume()
        }
    }

    // Step 2: Pebble checkpoint (Archive Format v2)
    pebbleStagingDir := filepath.Join(stagingDir, "pebble")
    span := pebble.CheckpointSpan{Start: s.byteRange[0], End: s.byteRange.EndForPebble()}
    if err := s.pdb.Checkpoint(pebbleStagingDir,
        pebble.WithFlushedWAL(),
        pebble.WithRestrictToSpans([]pebble.CheckpointSpan{span}),
    ); err != nil {
        return 0, fmt.Errorf("pebble checkpoint: %w", err)
    }

    // Step 3: Copy indexes directory (only if pause succeeded)
    if s.indexManager != nil && !s.indexManager.paused.Load() {
        // Pause failed, skip indexes
    } else if s.indexManager != nil {
        indexDir := s.indexManager.GetDir()
        if indexDir != "" {
            indexStagingDir := filepath.Join(stagingDir, "indexes")
            if err := common.CopyDir(indexDir, indexStagingDir); err != nil {
                s.logger.Warn("Failed to copy indexes", zap.Error(err))
                os.RemoveAll(indexStagingDir)
            }
        }
    }

    // Step 4: Create archive
    return s.snapStore.CreateSnapshot(context.Background(), id, stagingDir)
}
```

### Step 5: Handle Missing Documents During WAL Replay

**Files:** `src/store/indexes/full_text_v0.go`, `aknn_v0.go`

In the Execute/dequeue methods, when fetching document:
```go
doc, err := storeutils.GetDocument(...)
if err != nil {
    if errors.Is(err, pebble.ErrNotFound) {
        // Document not in snapshot, skip - Raft replay will re-add
        idx.logger.Debug("Skipping WAL entry for missing document", zap.String("key", key))
        continue
    }
    return err
}
```

## Files to Modify

| File | Changes |
|------|---------|
| `src/store/indexes/indexes.go` | Add `Pause(ctx)` and `Resume()` to Index interface |
| `src/store/indexes/full_text_v0.go` | Implement Pause/Resume, modify plexer loop, handle missing docs |
| `src/store/indexes/aknn_v0.go` | Implement Pause/Resume, modify plexer loop, handle missing docs |
| `src/store/indexes/graph_v0.go` | Implement Pause/Resume (simpler, no plexer) |
| `src/store/indexmgr.go` | Implement Pause/Resume (coordinates all indexes), add GetDir() |
| `src/store/db.go` | Modify Snapshot() to pause and copy indexes |

## Edge Cases

- **IndexManager nil**: Skip pause/copy (new shard with no indexes)
- **Pause timeout**: Log warning, continue without indexes (will rebuild on restore)
- **Copy failure**: Remove partial copy, continue (will rebuild on restore)
- **Missing document during replay**: Skip entry, Raft log replay will re-add it
- **Register/Unregister during pause**: Block these operations while paused

## Test Plan

1. Unit test: Pause/Resume on each index type
2. Unit test: Snapshot includes indexes/ directory
3. Integration test: Restore loads pre-built indexes, WALs replay
4. Integration test: Concurrent writes during snapshot
5. Edge case test: Missing document during WAL replay is skipped
