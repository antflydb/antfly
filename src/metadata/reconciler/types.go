// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package reconciler

import (
	"context"
	"time"

	"github.com/antflydb/antfly/lib/clock"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/client"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/tablemgr"
)

// ============================================================================
// INTERFACES - Abstractions for external dependencies
// ============================================================================

// TimeProvider abstracts time for testing.
type TimeProvider = clock.Clock

// RealTimeProvider uses actual system time
type RealTimeProvider struct{ clock.RealClock }

// MockTimeProvider allows controlling time in tests
type MockTimeProvider struct {
	current time.Time
	mock    *clock.MockClock
}

func (m *MockTimeProvider) Now() time.Time {
	if m.mock != nil {
		return m.mock.Now()
	}
	return m.current
}

func (m *MockTimeProvider) Advance(d time.Duration) {
	if m.mock != nil {
		m.mock.Advance(d)
		m.current = m.mock.Now()
		return
	}
	m.current = m.current.Add(d)
}

func (m *MockTimeProvider) Set(t time.Time) {
	m.current = t
	if m.mock != nil {
		m.mock.Set(t)
	}
}

func (m *MockTimeProvider) After(d time.Duration) <-chan time.Time {
	return m.ensureMockClock().After(d)
}

func (m *MockTimeProvider) NewTimer(d time.Duration) clock.Timer {
	return m.ensureMockClock().NewTimer(d)
}

func (m *MockTimeProvider) NewTicker(d time.Duration) clock.Ticker {
	return m.ensureMockClock().NewTicker(d)
}

func (m *MockTimeProvider) Sleep(d time.Duration) {
	m.Advance(d)
}

func (m *MockTimeProvider) ensureMockClock() *clock.MockClock {
	if m.mock == nil {
		m.mock = clock.NewMockClock(m.current)
	}
	return m.mock
}

// ShardOperations abstracts all shard-level operations for testing
type ShardOperations interface {
	// StartShard starts a shard on the specified nodes
	StartShard(
		ctx context.Context,
		shardID types.ID,
		peers types.IDSlice,
		config *store.ShardConfig,
		join, splitStart bool,
	) error

	// StartShardOnNode starts a shard on a specific node, joining an existing raft group
	// This is used when adding a peer to an existing shard - the node starts the shard
	// and joins the raft group composed of existingPeers
	StartShardOnNode(
		ctx context.Context,
		nodeID types.ID,
		shardID types.ID,
		existingPeers types.IDSlice,
		config *store.ShardConfig,
		splitStart bool,
	) error

	// StopShard stops a shard on a specific node
	StopShard(ctx context.Context, shardID types.ID, nodeID types.ID) error

	// PrepareSplit sets pendingSplitKey on the shard to reject writes in the split-off range.
	// This is a fast, local operation that should be called before updating metadata.
	PrepareSplit(ctx context.Context, shardID types.ID, splitKey []byte) error

	// SplitShard splits a shard at the specified key
	SplitShard(ctx context.Context, shardID, newShardID types.ID, splitKey []byte) error

	// FinalizeSplit completes a split by cleaning up the parent shard's split-off data
	// This should be called when the new shard has HasSnapshot=true
	FinalizeSplit(ctx context.Context, shardID types.ID, newRangeEnd []byte) error

	// MergeRange merges a range into a shard
	MergeRange(ctx context.Context, shardID types.ID, byteRange [2][]byte) error

	// RollbackSplit rolls back a stuck split operation, restoring the original state.
	// This clears the SplitState and removes shadow indexes.
	RollbackSplit(ctx context.Context, shardID types.ID) error

	// AddPeer adds a peer to an existing shard's raft group
	AddPeer(
		ctx context.Context,
		shardID types.ID,
		leaderClient client.StoreRPC,
		newPeerID types.ID,
	) error

	// RemovePeer removes a peer from an existing shard's raft group.
	// If async is true, returns after proposing. If false, waits for the conf change to be applied.
	RemovePeer(
		ctx context.Context,
		shardID types.ID,
		leaderClient client.StoreRPC,
		peerToRemoveID types.ID,
		async bool,
	) error

	// GetMedianKey gets the median key for a shard (for splitting)
	GetMedianKey(ctx context.Context, shardID types.ID) ([]byte, error)

	// IsIDRemoved checks if a peer has been removed from a shard
	IsIDRemoved(
		ctx context.Context,
		shardID types.ID,
		leaderClient client.StoreRPC,
		peerID types.ID,
	) (bool, error)
}

// TableOperations abstracts table-level operations
type TableOperations interface {
	// AddIndex adds an index to a shard
	AddIndex(ctx context.Context, shardID types.ID, name string, config *indexes.IndexConfig) error

	// DropIndex drops an index from a shard
	DropIndex(ctx context.Context, shardID types.ID, name string) error

	// UpdateSchema updates the schema on a shard
	UpdateSchema(ctx context.Context, shardID types.ID, schema *schema.TableSchema) error

	// ReassignShardsForSplit reassigns shards for a split transition
	ReassignShardsForSplit(
		transition tablemgr.SplitTransition,
	) (types.IDSlice, *store.ShardConfig, error)

	// ReassignShardsForMerge reassigns shards for a merge transition
	ReassignShardsForMerge(transition tablemgr.MergeTransition) (*store.ShardConfig, error)

	// HasReallocationRequest checks if a manual reallocation has been requested
	HasReallocationRequest(ctx context.Context) (bool, error)

	// ClearReallocationRequest clears the reallocation request flag
	ClearReallocationRequest(ctx context.Context) error

	// DropReadSchema drops the read schema for a table after index rebuild completes
	DropReadSchema(tableName string) error

	// GetTablesWithReadSchemas returns all tables that have read schemas (for index rebuild monitoring)
	GetTablesWithReadSchemas() ([]*store.Table, error)

	// DeleteTombstones deletes tombstone records for stores that are no longer voters
	DeleteTombstones(ctx context.Context, storeIDs []types.ID) error
}

// StoreOperations abstracts store/node operations
type StoreOperations interface {
	// GetStoreClient gets a client for a specific store
	GetStoreClient(ctx context.Context, nodeID types.ID) (client.StoreRPC, bool, error)

	// GetStoreStatus gets the status of a specific store
	GetStoreStatus(ctx context.Context, nodeID types.ID) (*tablemgr.StoreStatus, error)

	// GetShardStatuses gets all shard statuses
	GetShardStatuses() (map[types.ID]*store.ShardStatus, error)

	// GetLeaderClientForShard gets a client for the leader of a shard
	GetLeaderClientForShard(ctx context.Context, shardID types.ID) (client.StoreRPC, error)

	// UpdateShardSplitState updates the SplitState of a shard in the metadata store.
	// This is called after PrepareSplit succeeds to ensure the SplitState is immediately
	// visible to subsequent reconciliation runs, without waiting for heartbeat updates.
	UpdateShardSplitState(ctx context.Context, shardID types.ID, splitState *db.SplitState) error
}

// ============================================================================
// DATA STRUCTURES - State and plans
// ============================================================================

// ReconciliationConfig contains configuration for reconciliation
type ReconciliationConfig struct {
	ReplicationFactor   uint64
	MaxShardSizeBytes   uint64
	MaxShardsPerTable   uint64
	DisableShardAlloc   bool
	ShardCooldownPeriod time.Duration
	// AutoSplitPerTableLimit is the maximum number of new automatic split transitions
	// that may be planned for a single table in one reconciliation cycle.
	// If zero, defaults to 1.
	AutoSplitPerTableLimit int
	// AutoSplitClusterLimit is the maximum number of in-flight automatic splits
	// allowed cluster-wide. If the cluster is already at or above this budget,
	// no new automatic splits will be planned. If zero, defaults to 1.
	AutoSplitClusterLimit int
	// SplitTimeout is the maximum duration for a split operation before triggering rollback.
	// If zero, defaults to 5 minutes.
	SplitTimeout time.Duration
	// SplitFinalizeGracePeriod is the minimum duration a split-off shard must be continuously
	// ready (HasSnapshot, !Initializing, hasLeader) before FinalizeSplit deletes data from the
	// parent. This prevents data loss when the new shard's leader becomes temporarily unavailable
	// right after appearing ready. If zero, defaults to 15 seconds.
	SplitFinalizeGracePeriod time.Duration
}

// CurrentClusterState represents the current state of the cluster
type CurrentClusterState struct {
	// Active stores in the cluster
	Stores []types.ID

	// Current shard assignments and their state
	Shards map[types.ID]*store.ShardInfo

	// Stores that have been removed/tombstoned
	RemovedStores map[types.ID]struct{}

	// Table definitions
	Tables map[string]*store.Table
}

// DesiredClusterState represents the desired state of the cluster
type DesiredClusterState struct {
	// Desired shard assignments
	Shards map[types.ID]*store.ShardStatus

	// Table definitions
	Tables map[string]*store.Table
}

// ReconciliationPlan contains all actions needed to reconcile cluster state
type ReconciliationPlan struct {
	// Tombstoned stores that need to be removed from shards (still voters)
	RemovedStorePeerRemovals []types.ID

	// Tombstone records that can be deleted (no longer voters anywhere)
	TombstoneDeletions []types.ID

	// Raft voter fixes needed
	RaftVoterFixes []RaftVoterFix

	// Index operations to perform
	IndexOperations []IndexOperation

	// Split state transitions to advance
	SplitStateActions []SplitStateAction

	// Split transitions to execute
	SplitTransitions []tablemgr.SplitTransition

	// Merge transitions to execute
	MergeTransitions []tablemgr.MergeTransition

	// Shard placement transitions
	ShardTransitions *ShardTransitionPlan

	// Whether there are unreconciled split or merge operations
	HasUnreconciledSplitOrMerge bool

	// Whether to skip further processing
	SkipRemainingSteps bool

	// Whether this plan should force reallocation regardless of config
	ForcedReallocation bool

	// Whether to clear reallocation request after execution
	ClearReallocationRequest bool

	// Index rebuild completion actions
	IndexRebuildCompletions []IndexRebuildCompletion
}

// IndexRebuildCompletion represents a completed index rebuild that needs cleanup
type IndexRebuildCompletion struct {
	TableName string
	IndexName string
}

// RaftVoterFix represents a fix needed for raft voter membership
type RaftVoterFix struct {
	ShardID types.ID
	PeerID  types.ID
	Action  RaftVoterAction
}

// RaftVoterAction represents the type of raft voter fix
type RaftVoterAction int

const (
	RaftVoterActionStopShard RaftVoterAction = iota // Peer is not in voters, should be stopped
)

// IndexOperation represents an index operation to perform
type IndexOperation struct {
	ShardID   types.ID
	IndexName string
	Operation IndexOpType
	Config    *indexes.IndexConfig // Only for Add operations
	Schema    *schema.TableSchema  // Only for UpdateSchema operations
}

// IndexOpType represents the type of index operation
type IndexOpType int

const (
	IndexOpAdd IndexOpType = iota
	IndexOpDrop
	IndexOpUpdateSchema
)

// SplitStateAction represents a split/merge state advancement action
type SplitStateAction struct {
	ShardID      types.ID
	State        store.ShardState
	Action       SplitStateActionType
	NewShardID   types.ID  // For split operations
	SplitKey     []byte    // For split operations
	TargetRange  [2][]byte // For merge operations
	PeersToStart types.IDSlice
	ShardConfig  *store.ShardConfig
}

// SplitStateActionType represents the type of split state action
type SplitStateActionType int

const (
	SplitStateActionStartShard SplitStateActionType = iota
	SplitStateActionSplit
	SplitStateActionMerge
	SplitStateActionFinalizeSplit // Finalize a split by cleaning up parent shard

	// Zero-downtime split state actions
	SplitStateActionPrepare           // Initiate prepare phase (shadow index, dual-write)
	SplitStateActionTransitionToSplit // Move from PREPARE to SPLITTING (create archive)
	SplitStateActionRollback          // Timeout reached, revert to pre-split state
)

// ShardMovements represents movements of shards between nodes
type ShardMovements struct {
	Starts      []ShardStart
	Stops       []ShardStop
	Transitions []ShardTransition
}

// ShardStart represents starting a shard
type ShardStart struct {
	ShardID    types.ID
	Peers      types.IDSlice
	Config     *store.ShardConfig
	SplitStart bool
}

// ShardStop represents stopping a shard
type ShardStop struct {
	ShardID types.ID
	Peers   types.IDSlice
}

// ShardTransition represents a shard peer change
type ShardTransition struct {
	ShardID     types.ID
	AddPeers    types.IDSlice
	RemovePeers types.IDSlice
}

// MedianKeyGetter is a function that retrieves the median key for a shard
type MedianKeyGetter func(ctx context.Context, shardID types.ID) ([]byte, error)

// Empty returns true if the plan has no actions to perform
func (p *ReconciliationPlan) Empty() bool {
	return len(p.RemovedStorePeerRemovals) == 0 &&
		len(p.TombstoneDeletions) == 0 &&
		len(p.RaftVoterFixes) == 0 &&
		len(p.IndexOperations) == 0 &&
		len(p.SplitStateActions) == 0 &&
		len(p.SplitTransitions) == 0 &&
		len(p.MergeTransitions) == 0 &&
		len(p.IndexRebuildCompletions) == 0 &&
		(p.ShardTransitions == nil || p.ShardTransitions.Empty())
}

// Empty returns true if the shard transition plan has no actions
func (p *ShardTransitionPlan) Empty() bool {
	if p == nil {
		return true
	}
	return len(p.Starts) == 0 &&
		len(p.Stops) == 0 &&
		len(p.Transitions) == 0
}
