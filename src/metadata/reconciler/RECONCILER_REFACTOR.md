# Reconciliation Refactoring Plan

## Goals

1. **Improve Testability**: Separate decision logic from I/O operations
2. **Reduce Complexity**: Break down large functions into focused, composable units
3. **Enable Mocking**: Use interfaces for external dependencies
4. **Pure Functions**: Extract computation that doesn't depend on external state
5. **Maintain Compatibility**: Keep existing code working while adding refactored version

## Principles

- **Separate "What" from "How"**: Decision functions return plans, executor functions perform I/O
- **Dependency Injection**: Pass interfaces instead of concrete types
- **Pure Functions First**: Prefer stateless functions that are deterministic
- **Small, Focused Functions**: Each function should have a single responsibility

## Architecture

### Current State (metadata.go)

```
checkShardAssignments (319 lines)
├── Collect current state (I/O)
├── Reconcile indexes (I/O + decisions)
├── Remove shards from removed stores (I/O + decisions)
├── Advance splitting states (I/O + decisions)
├── Compute split transitions (pure)
├── Execute splits/merges (I/O)
├── Compute ideal assignments (pure)
└── Execute shard transitions (I/O)
```

### Refactored State (reconciler.go)

```
Reconciler (orchestrator)
├── ShardOperations (interface - mockable)
├── TableOperations (interface - mockable)
├── StoreOperations (interface - mockable)
└── Pure decision functions (testable)
```

## Interfaces to Extract

### 1. ShardOperations
Abstracts all shard-level operations:
- StartShard, StopShard
- SplitShard, MergeRange
- AddPeer, RemovePeer
- GetMedianKey

### 2. TableOperations
Abstracts table management:
- AddIndex, DropIndex, UpdateSchema
- ReassignShardsForSplit, ReassignShardsForMerge

### 3. StoreOperations
Abstracts store/node operations:
- GetStoreClient
- GetStoreStatus
- GetShardStatuses

## Pure Functions to Extract

### 1. computeReconciliationPlan
Takes current/desired state, returns what needs to happen:
```go
func computeReconciliationPlan(
    config ReconciliationConfig,
    current CurrentClusterState,
    desired DesiredClusterState,
) ReconciliationPlan
```

### 2. computeShardMovements
Decides which shards need to move where:
```go
func computeShardMovements(
    currentShards map[types.ID]*store.ShardInfo,
    idealShards map[types.ID]*store.ShardInfo,
) ShardMovements
```

### 3. computeSplitStateActions
Decides what split/merge state transitions to make:
```go
func computeSplitStateActions(
    desiredShards map[types.ID]*store.ShardStatus,
) []SplitStateAction
```

### 4. computeIndexReconciliation
Decides what index operations are needed:
```go
func computeIndexReconciliation(
    desiredIndexes map[string]indexes.IndexConfig,
    actualIndexes map[string]indexes.IndexConfig,
) []IndexOperation
```

## Data Structures

### ReconciliationPlan
Top-level plan containing all reconciliation actions:
```go
type ReconciliationPlan struct {
    RemovedStoreCleanup []types.ID
    RaftVoterFixes      []RaftVoterFix
    IndexOperations     []IndexOperation
    SplitStateActions   []SplitStateAction
    SplitTransitions    []SplitTransition
    MergeTransitions    []MergeTransition
    ShardTransitions    *ShardTransitionPlan
}
```

### CurrentClusterState
Snapshot of current cluster state:
```go
type CurrentClusterState struct {
    Stores        []types.ID
    Shards        map[types.ID]*store.ShardInfo
    RemovedStores map[types.ID]struct{}
    Tables        map[string]*tablemgr.Table
}
```

### DesiredClusterState
What the cluster should look like:
```go
type DesiredClusterState struct {
    Shards map[types.ID]*store.ShardStatus
    Tables map[string]*tablemgr.Table
}
```

## Migration Path

### Phase 1: Create Interfaces and Types (reconciler.go)
- Define all interfaces
- Define all data structures
- No behavior changes

### Phase 2: Extract Pure Functions
- Move computation logic to pure functions
- Keep I/O in existing methods
- Validate both produce same results

### Phase 3: Implement Reconciler
- Create Reconciler struct
- Implement reconciliation using new interfaces
- Add comprehensive tests

### Phase 4: Adapter Layer
- Create adapters that implement interfaces using existing MetadataStore methods
- Wire up Reconciler in MetadataStore

### Phase 5: Gradual Migration
- Add feature flag to switch between old/new reconciliation
- Run both in parallel, compare results
- Eventually remove old code

## Testing Strategy

### Unit Tests for Pure Functions
```go
func TestComputeReconciliationPlan(t *testing.T) {
    current := CurrentClusterState{...}
    desired := DesiredClusterState{...}
    plan := computeReconciliationPlan(config, current, desired)
    // Assert plan contains expected actions
}
```

### Integration Tests with Mocks
```go
func TestReconciler_Execute(t *testing.T) {
    mockShardOps := &MockShardOperations{}
    reconciler := NewReconciler(mockShardOps, ...)
    err := reconciler.Execute(ctx, plan)
    // Verify mock was called correctly
}
```

### Property-Based Tests
```go
func TestIdealShardAssignments_AlwaysBalanced(t *testing.T) {
    // Generate random cluster states
    // Verify assignments are always balanced
}
```

## Benefits

1. **Easy to Test**: Pure functions with no I/O
2. **Easy to Reason About**: Clear separation of concerns
3. **Easy to Mock**: Interface-based dependencies
4. **Easy to Debug**: Can inspect plans before execution
5. **Easy to Extend**: Add new reconciliation logic without touching I/O

## File Organization

```
src/metadata/reconciler/
├── reconciler.go
├── executor.go
├── types.go                   # Data structures
├── assignments.go             # Ideal assignment logic
└── ../reconciler_adapters.go  # Adapters to existing code
```
