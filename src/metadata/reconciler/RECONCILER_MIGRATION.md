# Reconciler Migration Plan

## Overview

This document outlines the plan to migrate `src/metadata/metadata.go` to use the new reconciler architecture defined in:
- `src/metadata/reconciler/reconciler.go` - Core reconciliation logic
- `src/metadata/reconciler/executor.go` - Execution of reconciliation plans
- `src/metadata/reconciler_adapters.go` - Adapter interfaces connecting reconciler to MetadataStore

## Goals

1. **Separation of Concerns**: Move all reconciliation decision-making and execution logic into the reconciler package
2. **Testability**: Enable comprehensive testing of reconciliation logic without metadata server dependencies
3. **Simplification**: Reduce `metadata.go` from ~1900 lines by removing complex reconciliation functions
4. **Single Source of Truth**: Consolidate cooldown management in the reconciler

## Benefits

- **Improved Testability**: Reconciliation logic can be tested independently with mock implementations
- **Better Code Organization**: Clear separation between cluster state management and reconciliation logic
- **Reduced Complexity**: `metadata.go` focuses on metadata server operations, not reconciliation algorithms
- **Consistent Cooldown Management**: Single cooldown implementation instead of ttlcache + reconciler maps
- **Easier Maintenance**: Reconciliation logic is isolated and can be modified without touching metadata.go

## Migration Steps

### Step 1: Update MetadataStore Struct

**File**: `src/metadata/metadata.go` (lines 37-52)

**Changes**:
```go
type MetadataStore struct {
    logger *zap.Logger

    metadataStore *kv.MetadataStore

    config *common.Config
    tm     *tablemgr.TableManager
    um     *usermgr.UserManager

    // ADD: Reconciler instance
    reconciler *reconciler.Reconciler

    // ADD: Debug hash for state change detection
    prevDebugShardsHash uint64

    // REMOVE: shardCooldown        *ttlcache.Cache[types.ID, struct{}]
    // REMOVE: shardForNodeCooldown *ttlcache.Cache[string, struct{}]
    embeddingCache       *ttlcache.Cache[string, []float32]

    runHealthCheckC  chan struct{}
    reconcileShardsC chan struct{}
}
```

**Also remove** the package-level variable (line 963):
```go
// REMOVE: var prevDebugShardsHash uint64
```

### Step 2: Initialize Reconciler

**File**: `src/metadata/metadata.go` - in the `NewMetadataStore` or constructor function

**Add initialization code**:
```go
func NewMetadataStore(...) *MetadataStore {
    ms := &MetadataStore{
        logger: logger,
        config: config,
        tm:     tableManager,
        um:     userManager,
        // ... other fields ...

        // REMOVE ttlcache initializations:
        // shardCooldown:        ttlcache.New[types.ID, struct{}](...),
        // shardForNodeCooldown: ttlcache.New[string, struct{}](...),
    }

    // Initialize reconciler with adapters
    shardOps := NewMetadataShardOperations(ms)
    tableOps := NewMetadataTableOperations(ms)
    storeOps := NewMetadataStoreOperations(ms)

    reconcilerConfig := reconciler.ReconciliationConfig{
        ReplicationFactor:  config.ReplicationFactor,
        MaxShardSizeBytes:  config.MaxShardSizeBytes,
        MaxShardsPerTable:  config.MaxShardsPerTable,
        DisableShardAlloc:  config.DisableShardAlloc,
    }

    ms.reconciler = reconciler.NewReconciler(
        shardOps,
        tableOps,
        storeOps,
        reconcilerConfig,
        logger,
    )

    return ms
}
```

### Step 3: Refactor checkShardAssignments

**File**: `src/metadata/metadata.go` (lines 977-1296)

**Replace the entire function body** after state gathering:

```go
func (ln *MetadataStore) checkShardAssignments(ctx context.Context) {
    // ========================================================================
    // KEEP: State gathering (lines 978-1196)
    // ========================================================================
    desiredShards, err := ln.tm.GetShardStatuses()
    if err != nil {
        ln.logger.Error("Failed to get shard statuses", zap.Error(err))
        return
    }

    // Collect current shard assignments from node statuses
    currentStores := []types.ID{}
    removedStores, err := ln.tm.GetStoreTombstones(ctx)
    if err != nil {
        ln.logger.Warn("Failed to get tombstoned stores", zap.Error(err))
    }
    removedStoresSet := make(map[types.ID]struct{}, len(removedStores))
    for _, id := range removedStores {
        removedStoresSet[id] = struct{}{}
    }

    tablesMap, err := ln.tm.TablesMap()
    if err != nil {
        ln.logger.Warn("Failed to get tables", zap.Error(err))
    }

    currentShards := make(map[types.ID]*store.ShardInfo)
    eg, _ := errgroup.WithContext(ctx)

    err = ln.tm.RangeStoreStatuses(func(peerID types.ID, storeStatus *tablemgr.StoreStatus) bool {
        if _, ok := removedStoresSet[peerID]; !ok {
            currentStores = append(currentStores, peerID)
        }
        for shardID, shardInfo := range storeStatus.Shards {
            // ... existing merge logic ...
            // (lines 1008-1101)
        }
        return true
    })

    if err != nil {
        ln.logger.Error("Failed to get store statuses", zap.Error(err))
        return
    }

    // Early return if no stores available
    slices.Sort(currentStores)
    if len(currentStores) == 0 {
        ln.logger.Warn("No nodes available")
        return
    }

    // ========================================================================
    // NEW: Reconciliation using the reconciler
    // ========================================================================

    // Cleanup expired cooldown entries
    ln.reconciler.CleanupExpiredCooldowns()

    // Build state structs for reconciler
    current := reconciler.CurrentClusterState{
        Stores:        currentStores,
        Shards:        currentShards,
        RemovedStores: removedStoresSet,
        Tables:        tablesMap,
    }

    desired := reconciler.DesiredClusterState{
        Shards: desiredShards,
    }

    // Execute reconciliation
    if err := ln.reconciler.Reconcile(ctx, current, desired); err != nil {
        ln.logger.Error("Reconciliation failed", zap.Error(err))
        // Continue - reconciler logs individual operation failures
    }

    // Debug logging (with hash-based deduplication)
    ln.reconciler.LogDebugState(current, desired, &ln.prevDebugShardsHash)
}
```

### Step 4: Remove Redundant Functions

**File**: `src/metadata/metadata.go`

The following large functions become redundant and should be **completely removed**:

1. **`executeShardTransitionPlan`** (lines 1597-1908)
   - Replaced by `reconciler/executor.go:executeShardTransitionPlan`

2. **`executeSplitTransitions`** (lines 599-722)
   - Replaced by `reconciler/executor.go:executeSplitAndMergeTransitions`

3. **`advanceSplittingStates`** (lines 457-597)
   - Replaced by `reconciler/reconciler.go:computeSplitStateActions` + executor

4. **`computeSplitTransitions`** (lines 820-953)
   - Replaced by `reconciler/reconciler.go:computeSplitAndMergeTransitions`

5. **`reconcileRaftVoters`** (lines 783-817)
   - Replaced by `reconciler/reconciler.go:computeRaftVoterFixes` + executor

6. **`removeShardsFromRemovedStores`** (lines 732-781)
   - Replaced by `reconciler/reconciler.go:computeRemovedStorePeerRemovals` + executor

7. **`idealShardAssignments`** (lines 257-288)
   - Replaced by `reconciler/assignment_v2.go:IdealShardAssignmentsV2`

### Step 5: Keep Required Helper Methods

The following methods **must be kept** because they are used by the reconciler adapters:

- `leaderClientForShard` (lines 290-304) - Used by `MetadataStoreOperations.GetLeaderClientForShard`
- `startShardOnNodes` (lines 342-396) - Used by `MetadataShardOperations.StartShard`
- `startShardOnNode` (lines 400-455) - Used by `MetadataShardOperations.StartShardOnNode`
- `stopShardOnNode` (lines 1482-1500) - Used by `MetadataShardOperations.StopShard`
- `splitShardOnNode` (lines 1547-1566) - Used by `MetadataShardOperations.SplitShard`
- `mergeRangeOnShard` (lines 1534-1544) - Used by `MetadataShardOperations.MergeRange`
- `addPeerToShard` (lines 1569-1583) - Used by `MetadataShardOperations.AddPeer`
- `removePeerFromShard` (lines 1586-1594) - Used by `MetadataShardOperations.RemovePeer`
- `getMedianKeyForShard` (lines 1503-1509) - Used by `MetadataShardOperations.GetMedianKey`
- `updateSchemaOnShard` (lines 965-974) - Used by reconciler for schema updates
- `addIndexOnShard` (lines 1520-1531) - Used by reconciler for index operations
- `dropIndexOnShard` (lines 1511-1518) - Used by reconciler for index operations

## Cooldown Migration Details

### Before (ttlcache-based):
```go
// In MetadataStore
shardCooldown        *ttlcache.Cache[types.ID, struct{}]
shardForNodeCooldown *ttlcache.Cache[string, struct{}]

// Usage
ln.shardCooldown.Set(shardID, struct{}{}, time.Minute)
if ln.shardCooldown.Has(shardID) {
    // skip
}
```

### After (reconciler-managed):
```go
// In Reconciler
shardCooldown        map[types.ID]time.Time
shardForNodeCooldown map[string]time.Time

// Usage (internal to reconciler)
r.SetShardCooldown(shardID, time.Minute)
if r.IsShardInCooldown(shardID) {
    // skip
}

// Cleanup called at start of each reconciliation
r.CleanupExpiredCooldowns()
```

**Key differences**:
- TTL cache auto-expires entries; map-based approach requires manual cleanup
- Cleanup is called once per reconciliation cycle (every ~1 second)
- Performance impact is negligible (O(n) where n = number of cooldown entries, typically < 100)

## Testing Strategy

### Before Migration
1. **Run existing integration tests** to establish baseline behavior
2. **Document current reconciliation behavior** via logs in test runs

### After Migration
1. **Unit test the reconciler** independently (already exists in reconciler package)
2. **Run the same integration tests** to verify identical behavior
3. **Add new tests** for edge cases now easier to test with reconciler isolation

### Test Commands
```bash
# Run all tests
go test ./src/metadata/...

# Run reconciler tests specifically
go test ./src/metadata/reconciler/...

# Run with race detector
go test -race ./src/metadata/...
```
