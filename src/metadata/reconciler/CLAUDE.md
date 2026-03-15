# CLAUDE.md - Reconciler Package

This file provides guidance to Claude Code when working with the reconciler package.

## Overview

The reconciler package implements cluster reconciliation logic, refactored from `metadata.go` for testability and maintainability.

**Status**: ✅ Complete - Full feature parity with original implementation
**Test Coverage**: 58 test functions, >90% coverage, all passing

## Core Architecture

**Separation of Planning and Execution**:
```
Current + Desired State → ComputePlan() [pure] → ReconciliationPlan → ExecutePlan() [I/O] → Updated Cluster
```

### Key Files

- **`types.go`** (300 lines) - Interfaces (ShardOperations, TableOperations, StoreOperations) and data structures
- **`reconciler.go`** (783 lines) - Planning logic (pure functions, no I/O)
- **`executor.go`** (720 lines) - Execution logic (I/O operations with dependency injection)
- **`assignments.go`** (235 lines) - Shard placement and load balancing logic

### Test Files (5041 lines total)

- `reconciler_test.go` - Core planning tests
- `reconciler_cooldown_test.go` - Cooldown mechanism tests
- `reconciler_splits_test.go` - Split/merge planning tests
- `executor_test.go` - Execution logic tests
- `executor_shardtransitions_test.go` - Shard transition tests
- `assignments_test.go` - Assignment tests

## Running Tests

```bash
# Run all tests
go test github.com/antflydb/antfly/src/metadata/reconciler

# Specific test
go test github.com/antflydb/antfly/src/metadata/reconciler -run TestName

# With coverage
go test -cover github.com/antflydb/antfly/src/metadata/reconciler
```

## Adding New Reconciliation Logic

1. **Planning** (`reconciler.go`) - Add pure function returning actions:
   ```go
   func (r *Reconciler) computeMyNewFeature(current, desired ClusterState) []MyAction
   ```

2. **Plan Structure** (`types.go`) - Add field to `ReconciliationPlan`:
   ```go
   type ReconciliationPlan struct {
       MyNewActions []MyAction
   }
   ```

3. **Execution** (`executor.go`) - Add executor function:
   ```go
   func (r *Reconciler) executeMyNewActions(ctx context.Context, actions []MyAction) error
   ```

4. **Wire Up** - Call from `ComputePlan()` and `ExecutePlan()`

5. **Test** - Unit test planning (no mocks), integration test execution (with mocks)

## Reconciliation Phases (Sequential)

1. **Removed Store Cleanup** - Remove peers from shards
2. **Tombstone Deletion** - Delete tombstone records
3. **Raft Voter Fixes** - Fix peer/voter inconsistencies
4. **Index Operations** - Add/drop/update indexes
5. **Split State Actions** - Handle SplittingOff, PreSplit, PreMerge states
6. **Split/Merge Transitions** - Execute splits and merges
7. **Shard Transitions** - Move shards to ideal assignments

## Testing Patterns

**Unit Tests** (pure functions, no mocks):
```go
reconciler := NewReconciler(nil, nil, nil, config, logger)
actions := reconciler.computeMyFeature(current, desired)
assert.Equal(t, expected, actions)
```

**Integration Tests** (with mocks):
```go
mockOps := &MockShardOperations{}
reconciler := NewReconciler(mockOps, ...)
err := reconciler.ExecutePlan(ctx, plan, current, desired)
mockOps.AssertExpectations(t)
```

**Test Builder**:
```go
shard := NewShardStatusBuilder().
    WithID(1).WithTable("users").WithPeers(1,2,3).Build()
```

## Key Principles

- **Planning is pure** - No I/O, all decisions in pure functions
- **Execution uses DI** - All external ops through mockable interfaces
- **Graceful degradation** - Log errors, continue execution
- **Context aware** - Check cancellation between phases
- **Cooldown tracking** - Prevent rapid shard changes

## Configuration

Key settings in `ReconciliationConfig`:
- `MaxShardSizeBytes` - Trigger split threshold
- `ReplicationFactor` - Number of replicas
- `ShardCooldown` - Wait time between operations (recommended: 5-10 min)
- `DisableShardAlloc` - Disable automatic allocation

## Documentation

- `RECONCILER_REFACTOR.md` - Design principles and migration plan
- `RECONCILER_SUMMARY.md` - Detailed implementation summary
- Original code: `src/metadata/metadata.go:checkShardAssignments()`
