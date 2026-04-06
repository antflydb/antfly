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

package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/raft"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/google/uuid"
	"go.etcd.io/raft/v3/raftpb"
)

// ErrShardNotReady is returned when a shard operation is attempted before the shard is fully initialized
var ErrShardNotReady = errors.New("shard is still initializing")

var _ ShardIface = (*Shard)(nil)

type ShardIface interface {
	Backup(ctx context.Context, location, backupID string) error
	ExportPortable(ctx context.Context, w io.Writer) error
	ImportPortable(ctx context.Context, r io.Reader) error
	PrepareSplit(ctx context.Context, splitKey []byte) error
	Split(ctx context.Context, newShardID uint64, splitKey []byte) error
	FinalizeSplit(ctx context.Context, newRangeEnd []byte) error
	RollbackSplit(ctx context.Context) error
	SetMergeState(ctx context.Context, state *MergeState) error
	FinalizeMerge(ctx context.Context, byteRange [2][]byte) error
	AddIndex(ctx context.Context, config indexes.IndexConfig) error
	DropIndex(ctx context.Context, name string) error
	SetRange(ctx context.Context, byteRange [2][]byte) error
	UpdateSchema(ctx context.Context, schema *schema.TableSchema) error
	FindMedianKey() ([]byte, error)
	Search(ctx context.Context, query []byte) ([]byte, error)
	SearchTyped(ctx context.Context, req *indexes.RemoteIndexSearchRequest) (*indexes.RemoteIndexSearchResult, error)
	Lookup(ctx context.Context, key string) (map[string]any, error)
	GetTimestamp(key string) (uint64, error)
	Batch(ctx context.Context, batchOp *BatchOp, proposeOnly bool) error
	ApplyMergeChunk(ctx context.Context, batchOp *BatchOp) error
	Scan(ctx context.Context, fromKey []byte, toKey []byte, opts ScanOptions) (*ScanResult, error)
	ExportRangeChunk(ctx context.Context, startKey, endKey, afterKey []byte, limit int) ([][2][]byte, []byte, bool, error)
	ListMergeDeltaEntriesAfter(afterSeq uint64) ([]*MergeDeltaEntry, error)
	ProposeConfChange(cc raftpb.ConfChange) error // Using our minimal ConfChange
	// ApplyConfChange proposes a conf change and waits for it to be applied to the Raft state machine.
	ApplyConfChange(ctx context.Context, cc raftpb.ConfChange) error
	// Direct raft node forwards
	IsIDRemoved(id uint64) bool
	TransferLeadership(ctx context.Context, target types.ID)
	// Transaction methods
	SyncWriteOp(ctx context.Context, op *Op) error
	GetTransactionStatus(ctx context.Context, txnID []byte) (int32, error)
	GetCommitVersion(ctx context.Context, txnID []byte) (uint64, error)
	ListTxnRecords(ctx context.Context) ([]TxnRecord, error)
	ListTxnIntents(ctx context.Context) ([]TxnIntent, error)
	// Graph methods
	GetEdges(ctx context.Context, indexName string, key []byte, edgeType string, direction indexes.EdgeDirection) ([]indexes.Edge, error)
	TraverseEdges(ctx context.Context, indexName string, startKey []byte, rules indexes.TraversalRules) ([]*indexes.TraversalResult, error)
	GetNeighbors(ctx context.Context, indexName string, key []byte, edgeType string, direction indexes.EdgeDirection) ([]*indexes.TraversalResult, error)
	FindShortestPath(ctx context.Context, indexName string, source, target []byte, edgeTypes []string, direction indexes.EdgeDirection, weightMode indexes.PathWeightMode, maxDepth int, minWeight, maxWeight float64) (*indexes.Path, error)
}

type ShardStartRequest struct {
	ShardConfig

	Peers      []common.Peer `json:"peers"`
	Join       bool          `json:"join"`
	Timestamp  []byte        `json:"timestamp,omitempty"` // Timestamp is used to ensure that the shard is started with the correct
	SplitStart bool          `json:"split_start"`
}
type ShardSplitRequest struct {
	NewShardID string `json:"new_shard_id"`
	SplitKey   []byte `json:"split_key"`
}
type ShardPrepareSplitRequest struct {
	SplitKey []byte `json:"split_key"`
}
type ShardAddPeerRequest struct {
	PeerURL string `json:"peer_url"`
}
type ShardRemovePeerRequest struct {
	Timestamp []byte `json:"timestamp,omitempty"`
}

// type ShardUpdatePeersRequest struct {
// 	AddPeers    []string `json:"add_peers,omitempty"`
// 	RemovePeers []string `json:"remove_peers,omitempty"`
// 	Timestamp   []byte   `json:"timestamp,omitempty"`
// }
// type ShardUpdateConfigRequest struct {
// 	ByteRange   [2][]byte             `json:"byte_range"`
// 	Schema      *indexes.TableSchema  `json:"schema,omitempty"`
// 	DropIndexes []string              `json:"drop_indexes,omitempty"`
// 	AddIndexes  []indexes.IndexConfig `json:"add_indexes,omitempty"`
// }

// TODO (ajr) Should this should be a configuation update path?
type ShardSetRangeRequest struct {
	ByteRange [2][]byte `json:"byte_range"`
}
type ShardUpdateSchemaRequest struct {
	Schema *schema.TableSchema `json:"schema,omitempty"`
}
type ShardDropIndexRequest struct {
	Name string `json:"name"`
}
type ShardMergeStateRequest struct {
	State *MergeState `json:"state,omitempty"`
}
type ShardFinalizeMergeRequest struct {
	ByteRange [2][]byte `json:"byte_range"`
}
type ShardExportRangeRequest struct {
	StartKey []byte `json:"start_key"`
	EndKey   []byte `json:"end_key"`
	AfterKey []byte `json:"after_key,omitempty"`
	Limit    int    `json:"limit,omitempty"`
}
type ShardMergeDeltaRequest struct {
	AfterSeq uint64 `json:"after_seq"`
}

type Shard struct {
	storeDB     *StoreDB
	raftNode    raft.RaftNode
	confChangeC chan<- *raft.ConfChangeProposal
	// TODO (ajr) Handle these errors
	errorC <-chan error
	// cancelLeaderFactory cancels the LeaderFactory goroutine in swarm mode
	// In raft mode, this is nil since raft manages the lifecycle
	cancelLeaderFactory context.CancelFunc
	closeOnce           sync.Once
	closeErr            error
}

// NewShard creates a new Shard with the given components.
func NewShard(
	db *StoreDB,
	raftNode raft.RaftNode,
	confChangeC chan<- *raft.ConfChangeProposal,
	errorC <-chan error,
	cancelLeaderFactory context.CancelFunc,
) *Shard {
	return &Shard{
		storeDB:             db,
		raftNode:            raftNode,
		confChangeC:         confChangeC,
		errorC:              errorC,
		cancelLeaderFactory: cancelLeaderFactory,
	}
}

// NewInitializingShard creates a placeholder Shard that is still initializing.
// The shard will return ErrShardNotReady for all operations until replaced
// with a fully initialized shard via NewShard.
func NewInitializingShard() *Shard {
	return &Shard{}
}

// checkReady returns an error if the shard is not ready to handle operations
func (s *Shard) checkReady() error {
	if s.storeDB == nil {
		return ErrShardNotReady
	}
	if s.storeDB.IsInitializing() {
		return ErrShardNotReady
	}
	return nil
}

// IsInitializing returns true if the shard is still initializing.
// A shard is considered initializing when its storeDB is nil.
func (s *Shard) IsInitializing() bool {
	return s.storeDB == nil || s.storeDB.IsInitializing()
}

// StoreDB returns the underlying StoreDB for local coordination paths such as
// split replay catch-up. It returns nil while the shard is still initializing.
func (s *Shard) StoreDB() *StoreDB {
	if s.storeDB == nil {
		return nil
	}
	return s.storeDB
}

// ErrorC returns the error channel for monitoring raft errors.
func (s *Shard) ErrorC() <-chan error {
	return s.errorC
}

// Close shuts down the shard, stopping all background goroutines and releasing resources.
// It closes channels, cancels contexts, waits for the raft node to stop, and closes the database.
// Close is safe to call multiple times; only the first call performs the shutdown.
func (s *Shard) Close() error {
	if s.storeDB == nil {
		return ErrShardNotReady
	}

	s.closeOnce.Do(func() {
		// Close confChangeC to stop accepting configuration changes
		if s.confChangeC != nil {
			close(s.confChangeC)
		}

		// In swarm mode, cancel the LeaderFactory context to stop background goroutines
		if s.cancelLeaderFactory != nil {
			s.cancelLeaderFactory()
		}

		// Close proposeC to trigger raft node shutdown, then wait for it to complete
		// before closing the database to avoid race conditions with LeaderFactory goroutines
		if s.storeDB != nil {
			s.storeDB.CloseProposeC()
		}
		if s.raftNode != nil {
			s.raftNode.Shutdown()
		}

		// Wait for raft node to fully stop (errorC will close when raft stops)
		if s.errorC != nil {
			for range s.errorC {
				// Drain any remaining errors
			}
		}

		// Now safe to close the database
		if s.storeDB != nil {
			s.closeErr = s.storeDB.CloseDB()
		}
	})
	return s.closeErr
}

func (s *Shard) Info() *ShardInfo {
	if s.IsInitializing() {
		return &ShardInfo{
			Initializing: true,
		}
	}
	var raftStatus *common.RaftStatus
	var peers map[types.ID]struct{}
	var hasSnapshot bool

	if s.raftNode != nil {
		raftStatus = common.NewRaftStatus(s.raftNode.Status())
		peers = s.raftNode.Peers()
		hasSnapshot = s.raftNode.SnapshotIndex() > 0
	}

	splitReplayRequired, splitReplayCaughtUp, splitCutoverReady, splitReplaySeq, splitReplayParentShardID := s.storeDB.SplitReplayInfo()
	mergeDeltaSeq, _ := s.storeDB.GetMergeDeltaSeq()
	mergeDeltaFinalSeq, _ := s.storeDB.GetMergeDeltaFinalSeq()

	return &ShardInfo{
		ShardConfig: ShardConfig{
			ByteRange: s.storeDB.byteRange,
			Schema:    s.storeDB.schema,
			Indexes:   s.storeDB.coreDB.GetIndexes(),
		},
		ShardStats:          s.storeDB.Stats(),
		RaftStatus:          raftStatus,
		Peers:               peers,
		HasSnapshot:         hasSnapshot,
		Initializing:        s.storeDB.IsInitializing(),
		Splitting:           s.storeDB.IsSplitting(),
		SplitState:          s.storeDB.GetSplitState(),
		MergeState:          s.storeDB.GetMergeState(),
		SplitReplayRequired: splitReplayRequired,
		SplitReplayCaughtUp: splitReplayCaughtUp,
		SplitCutoverReady:   splitCutoverReady,
		SplitReplaySeq:      splitReplaySeq,
		SplitParentShardID:  splitReplayParentShardID,
		MergeDeltaSeq:       mergeDeltaSeq,
		MergeDeltaFinalSeq:  mergeDeltaFinalSeq,
	}
}

// TODO (ajr) Maybe this should just be IsMember?
func (s *Shard) IsIDRemoved(id uint64) bool {
	if s.raftNode != nil {
		return s.raftNode.IsIDRemoved(id)
	}
	return false
}

func (s *Shard) TransferLeadership(ctx context.Context, target types.ID) {
	if s.raftNode != nil {
		s.raftNode.TransferLeadership(ctx, target)
	}
}

func (s *Shard) ProposeConfChange(cc raftpb.ConfChange) error {
	if s.IsInitializing() {
		return ErrShardNotReady
	}
	// In swarm mode without Raft, configuration changes are not supported
	if s.confChangeC == nil {
		return fmt.Errorf("configuration changes not supported in swarm mode")
	}
	proposeDoneC := make(chan error, 1)
	s.confChangeC <- &raft.ConfChangeProposal{
		ConfChange:   cc,
		ProposeDoneC: proposeDoneC,
	}
	if err := <-proposeDoneC; err != nil {
		return fmt.Errorf("proposing confChange to raft: %w", err)
	}
	return nil
}

// ApplyConfChange proposes a conf change and waits for it to be applied to the Raft state machine.
// This is synchronous - it blocks until the conf change is committed and applied.
func (s *Shard) ApplyConfChange(ctx context.Context, cc raftpb.ConfChange) error {
	if s.IsInitializing() {
		return ErrShardNotReady
	}
	// In swarm mode without Raft, configuration changes are not supported
	if s.confChangeC == nil {
		return fmt.Errorf("configuration changes not supported in swarm mode")
	}

	// Generate a callback ID to track when this conf change is applied
	callbackID := uuid.New()

	proposeDoneC := make(chan error, 1)
	s.confChangeC <- &raft.ConfChangeProposal{
		ConfChange:      cc,
		ProposeDoneC:    proposeDoneC,
		ApplyCallbackID: callbackID,
	}

	// Wait for the proposal to be accepted
	if err := <-proposeDoneC; err != nil {
		return fmt.Errorf("proposing confChange to raft: %w", err)
	}

	// Wait for the conf change to be applied
	appliedCh := s.raftNode.GetConfChangeCallback(callbackID)
	if appliedCh == nil {
		// Callback was already triggered (conf change already applied)
		return nil
	}

	select {
	case <-appliedCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Shard) Lookup(ctx context.Context, key string) (map[string]any, error) {
	if err := s.checkReady(); err != nil {
		return nil, err
	}
	return s.storeDB.Lookup(ctx, key)
}

func (s *Shard) GetTimestamp(key string) (uint64, error) {
	if err := s.checkReady(); err != nil {
		return 0, err
	}
	return s.storeDB.GetTimestamp(key)
}

func (s *Shard) AddIndex(ctx context.Context, config indexes.IndexConfig) error {
	if err := s.checkReady(); err != nil {
		return err
	}
	return s.storeDB.AddIndex(ctx, config)
}

func (s *Shard) DropIndex(ctx context.Context, name string) error {
	if err := s.checkReady(); err != nil {
		return err
	}
	return s.storeDB.DeleteIndex(ctx, name)
}

func (s *Shard) Batch(ctx context.Context, batchOp *BatchOp, proposeOnly bool) error {
	if err := s.checkReady(); err != nil {
		return err
	}
	// Note: proposeOnly is not used - sync level is determined by batchOp.GetSyncLevel()
	return s.storeDB.Batch(ctx, batchOp)
}

func (s *Shard) ApplyMergeChunk(ctx context.Context, batchOp *BatchOp) error {
	if err := s.checkReady(); err != nil {
		return err
	}
	return s.storeDB.ApplyMergeChunk(ctx, batchOp)
}

func (s *Shard) PrepareSplit(ctx context.Context, splitKey []byte) error {
	if err := s.checkReady(); err != nil {
		return err
	}
	return s.storeDB.PrepareSplit(ctx, splitKey)
}

func (s *Shard) Split(ctx context.Context, newShardID uint64, medianKey []byte) error {
	if err := s.checkReady(); err != nil {
		return err
	}
	return s.storeDB.Split(ctx, newShardID, medianKey)
}

func (s *Shard) FinalizeSplit(ctx context.Context, newRangeEnd []byte) error {
	if err := s.checkReady(); err != nil {
		return err
	}
	return s.storeDB.FinalizeSplit(ctx, newRangeEnd)
}

func (s *Shard) RollbackSplit(ctx context.Context) error {
	if err := s.checkReady(); err != nil {
		return err
	}
	return s.storeDB.RollbackSplit(ctx)
}

func (s *Shard) SetMergeState(ctx context.Context, state *MergeState) error {
	if err := s.checkReady(); err != nil {
		return err
	}
	return s.storeDB.SetMergeState(ctx, state)
}

func (s *Shard) FinalizeMerge(ctx context.Context, byteRange [2][]byte) error {
	if err := s.checkReady(); err != nil {
		return err
	}
	return s.storeDB.FinalizeMerge(ctx, byteRange)
}

func (s *Shard) Backup(ctx context.Context, loc, id string) error {
	if err := s.checkReady(); err != nil {
		return err
	}
	return s.storeDB.Backup(ctx, loc, id)
}

func (s *Shard) ExportPortable(ctx context.Context, w io.Writer) error {
	if err := s.checkReady(); err != nil {
		return err
	}
	return s.storeDB.ExportPortable(ctx, w)
}

func (s *Shard) ImportPortable(ctx context.Context, r io.Reader) error {
	if err := s.checkReady(); err != nil {
		return err
	}
	return s.storeDB.ImportPortable(ctx, r)
}

func (s *Shard) SetRange(ctx context.Context, byteRange [2][]byte) error {
	if err := s.checkReady(); err != nil {
		return err
	}
	return s.storeDB.SetRange(ctx, byteRange)
}

func (s *Shard) UpdateSchema(ctx context.Context, tableSchema *schema.TableSchema) error {
	if err := s.checkReady(); err != nil {
		return err
	}
	return s.storeDB.UpdateSchema(ctx, tableSchema)
}

func (s *Shard) FindMedianKey() ([]byte, error) {
	if err := s.checkReady(); err != nil {
		return nil, err
	}
	return s.storeDB.FindMedianKey()
}

func (s *Shard) Search(ctx context.Context, req []byte) ([]byte, error) {
	if err := s.checkReady(); err != nil {
		return nil, err
	}
	return s.storeDB.Search(ctx, req)
}

func (s *Shard) SearchTyped(ctx context.Context, req *indexes.RemoteIndexSearchRequest) (*indexes.RemoteIndexSearchResult, error) {
	if err := s.checkReady(); err != nil {
		return nil, err
	}
	return s.storeDB.SearchTyped(ctx, req)
}

func (s *Shard) Scan(ctx context.Context, fromKey []byte, toKey []byte, opts ScanOptions) (*ScanResult, error) {
	if err := s.checkReady(); err != nil {
		return nil, err
	}
	return s.storeDB.Scan(ctx, fromKey, toKey, opts)
}

func (s *Shard) ExportRangeChunk(
	ctx context.Context,
	startKey, endKey, afterKey []byte,
	limit int,
) ([][2][]byte, []byte, bool, error) {
	if err := s.checkReady(); err != nil {
		return nil, nil, false, err
	}
	return s.storeDB.ExportRangeChunk(ctx, startKey, endKey, afterKey, limit)
}

func (s *Shard) ListMergeDeltaEntriesAfter(afterSeq uint64) ([]*MergeDeltaEntry, error) {
	if err := s.checkReady(); err != nil {
		return nil, err
	}
	return s.storeDB.ListMergeDeltaEntriesAfter(afterSeq)
}

// SyncWriteOp synchronously proposes an Op through Raft and waits for commit
func (s *Shard) SyncWriteOp(ctx context.Context, op *Op) error {
	if err := s.checkReady(); err != nil {
		return err
	}
	return s.storeDB.syncWriteOp(ctx, op)
}

// GetTransactionStatus retrieves the status of a transaction from the coordinator
func (s *Shard) GetTransactionStatus(ctx context.Context, txnID []byte) (int32, error) {
	if err := s.checkReady(); err != nil {
		return 0, err
	}
	return s.storeDB.GetTransactionStatus(ctx, txnID)
}

// GetCommitVersion returns the commit version for a committed transaction.
func (s *Shard) GetCommitVersion(ctx context.Context, txnID []byte) (uint64, error) {
	if err := s.checkReady(); err != nil {
		return 0, err
	}
	return s.storeDB.GetCommitVersion(ctx, txnID)
}

func (s *Shard) ListTxnRecords(ctx context.Context) ([]TxnRecord, error) {
	if err := s.checkReady(); err != nil {
		return nil, err
	}
	return s.storeDB.ListTxnRecords(ctx)
}

func (s *Shard) ListTxnIntents(ctx context.Context) ([]TxnIntent, error) {
	if err := s.checkReady(); err != nil {
		return nil, err
	}
	return s.storeDB.ListTxnIntents(ctx)
}

// GetEdges retrieves edges for a document in a graph index
func (s *Shard) GetEdges(ctx context.Context, indexName string, key []byte, edgeType string, direction indexes.EdgeDirection) ([]indexes.Edge, error) {
	if err := s.checkReady(); err != nil {
		return nil, err
	}
	return s.storeDB.GetEdges(ctx, indexName, key, edgeType, direction)
}

// TraverseEdges performs graph traversal from a starting node
func (s *Shard) TraverseEdges(ctx context.Context, indexName string, startKey []byte, rules indexes.TraversalRules) ([]*indexes.TraversalResult, error) {
	if err := s.checkReady(); err != nil {
		return nil, err
	}
	return s.storeDB.TraverseEdges(ctx, indexName, startKey, rules)
}

// GetNeighbors retrieves direct neighbors of a node (1-hop traversal)
func (s *Shard) GetNeighbors(ctx context.Context, indexName string, key []byte, edgeType string, direction indexes.EdgeDirection) ([]*indexes.TraversalResult, error) {
	if err := s.checkReady(); err != nil {
		return nil, err
	}
	return s.storeDB.GetNeighbors(ctx, indexName, key, edgeType, direction)
}

// FindShortestPath finds the shortest path between two nodes in the graph
func (s *Shard) FindShortestPath(ctx context.Context, indexName string, source, target []byte, edgeTypes []string, direction indexes.EdgeDirection, weightMode indexes.PathWeightMode, maxDepth int, minWeight, maxWeight float64) (*indexes.Path, error) {
	if err := s.checkReady(); err != nil {
		return nil, err
	}
	return s.storeDB.FindShortestPath(ctx, indexName, source, target, edgeTypes, direction, weightMode, maxDepth, minWeight, maxWeight)
}
