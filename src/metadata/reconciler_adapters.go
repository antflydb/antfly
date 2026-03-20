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

package metadata

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/metadata/reconciler"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/client"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/tablemgr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// ============================================================================
// Adapter implementations for reconciler interfaces
// ============================================================================

// MetadataShardOperations implements reconciler.ShardOperations by delegating to MetadataStore
type MetadataShardOperations struct {
	ms *MetadataStore
}

// NewMetadataShardOperations creates a new shard operations adapter
func NewMetadataShardOperations(ms *MetadataStore) *MetadataShardOperations {
	return &MetadataShardOperations{ms: ms}
}

func (m *MetadataShardOperations) StartShard(
	ctx context.Context,
	shardID types.ID,
	peerSlice types.IDSlice,
	config *store.ShardConfig,
	join, splitStart bool,
) error {
	peers := make(common.Peers, 0, len(peerSlice))
	for _, peerID := range peerSlice {
		nodeStatus, err := m.ms.tm.GetStoreStatus(ctx, peerID)
		if err != nil {
			m.ms.logger.Debug("skipping peer not found",
				zap.Error(err),
				zap.Stringer("peerID", peerID))
			continue
		}
		peers = append(peers, common.Peer{ID: peerID, URL: nodeStatus.RaftURL})
	}
	req := &store.ShardStartRequest{
		ShardConfig: *config,
		Peers:       peers,
		Join:        join,
		SplitStart:  splitStart,
		Timestamp:   time.Now().UTC().AppendFormat(nil, time.RFC3339),
	}
	m.ms.logger.Info(
		"Starting shard on peers",
		zap.Stringer("shardID", shardID),
		zap.Stringer("peers", peers),
	)
	// Start the shard on each peer
	eg, _ := errgroup.WithContext(ctx)
	for _, peer := range peers {
		eg.Go(func() error {
			// The check for existence is handled above
			nodeStatus, _ := m.ms.tm.GetStoreStatus(ctx, peer.ID)
			if err := nodeStatus.StartShard(ctx, shardID, req); err != nil {
				if !errors.Is(err, client.ErrAlreadyExists) {
					return fmt.Errorf("adding peer %s: %w", peer.ID, err)
				}
				return nil
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		m.ms.logger.Warn(
			"Failed to start shard on nodes",
			zap.Stringer("shardID", shardID),
			zap.Error(err),
		)
	}
	return nil
}

func (m *MetadataShardOperations) StartShardOnNode(
	ctx context.Context,
	nodeID types.ID,
	shardID types.ID,
	existingPeers types.IDSlice,
	config *store.ShardConfig,
	splitStart bool,
) error {
	// Get the store client for the node we're starting on
	storeClient, reachable, err := m.ms.tm.GetStoreClient(ctx, nodeID)
	if err != nil {
		return fmt.Errorf("getting store client for node %s: %w", nodeID, err)
	}
	if !reachable {
		return fmt.Errorf("node %s is not reachable", nodeID)
	}

	// Build the peer list from existing voters (needed for join request)
	peers := make(common.Peers, 0, len(existingPeers))
	for _, peerID := range existingPeers {
		nodeStatus, err := m.ms.tm.GetStoreStatus(ctx, peerID)
		if err != nil {
			m.ms.logger.Debug("skipping peer not found when building join request",
				zap.Error(err),
				zap.Stringer("peerID", peerID))
			continue
		}
		peers = append(peers, common.Peer{ID: peerID, URL: nodeStatus.RaftURL})
	}

	// Create the start request with join=true
	req := &store.ShardStartRequest{
		ShardConfig: *config,
		Peers:       peers,
		Join:        true, // Always joining an existing group
		SplitStart:  splitStart,
		Timestamp:   time.Now().UTC().AppendFormat(nil, time.RFC3339),
	}

	m.ms.logger.Info(
		"Starting shard on specific node to join existing raft group",
		zap.Stringer("shardID", shardID),
		zap.Stringer("nodeID", nodeID),
		zap.Stringer("existingPeers", existingPeers),
	)

	// Start the shard on this specific node only
	if err := storeClient.StartShard(ctx, shardID, req); err != nil {
		if !errors.Is(err, client.ErrAlreadyExists) {
			return fmt.Errorf("starting shard on node %s: %w", nodeID, err)
		}
		// If shard already exists, that's okay - treat as success
	}

	return nil
}

func (m *MetadataShardOperations) StopShard(
	ctx context.Context,
	shardID types.ID,
	nodeID types.ID,
) error {
	m.ms.logger.Debug(
		"Stopping shard on node",
		zap.Stringer("shardID", shardID),
		zap.Stringer("nodeID", nodeID),
	)
	storeClient, reacheable, err := m.ms.tm.GetStoreClient(ctx, nodeID)
	if err != nil {
		return fmt.Errorf("node %s not found in store clients: %w", nodeID, err)
	}
	if !reacheable {
		return fmt.Errorf("node %s is not reachable", nodeID)
	}
	return storeClient.StopShard(ctx, shardID)
}

func (m *MetadataShardOperations) PrepareSplit(
	ctx context.Context,
	shardID types.ID,
	splitKey []byte,
) error {
	leaderClient, err := m.ms.leaderClientForShard(ctx, shardID)
	if err != nil {
		return err
	}
	if err := leaderClient.PrepareSplit(ctx, shardID, splitKey); err != nil {
		if errors.Is(err, client.ErrKeyOutOfRange) {
			// The shard may have already been split
			return nil
		}
		return fmt.Errorf("preparing split: %w", err)
	}
	return nil
}

func (m *MetadataShardOperations) SplitShard(
	ctx context.Context,
	shardID, newShardID types.ID,
	splitKey []byte,
) error {
	leaderClient, err := m.ms.leaderClientForShard(ctx, shardID)
	if err != nil {
		return err
	}
	if err := leaderClient.SplitShard(ctx, shardID, newShardID, splitKey); err != nil {
		return fmt.Errorf("splitting shard: %w", err)
	}
	return nil
}

func (m *MetadataShardOperations) FinalizeSplit(
	ctx context.Context,
	shardID types.ID,
	newRangeEnd []byte,
) error {
	leaderClient, err := m.ms.leaderClientForShard(ctx, shardID)
	if err != nil {
		return err
	}
	if err := leaderClient.FinalizeSplit(ctx, shardID, newRangeEnd); err != nil {
		return fmt.Errorf("finalizing split on shard %s: %w", shardID, err)
	}
	return nil
}

func (m *MetadataShardOperations) MergeRange(
	ctx context.Context,
	shardID types.ID,
	byteRange [2][]byte,
) error {
	nodeStatus, err := m.ms.leaderClientForShard(ctx, shardID)
	if err != nil {
		return fmt.Errorf("getting leader for shard %s: %w", shardID, err)
	}
	return nodeStatus.MergeRange(ctx, shardID, byteRange)
}

func (m *MetadataShardOperations) RollbackSplit(
	ctx context.Context,
	shardID types.ID,
) error {
	leaderClient, err := m.ms.leaderClientForShard(ctx, shardID)
	if err != nil {
		return err
	}
	if err := leaderClient.RollbackSplit(ctx, shardID); err != nil {
		return fmt.Errorf("rolling back split on shard %s: %w", shardID, err)
	}
	return nil
}

func (m *MetadataShardOperations) AddPeer(
	ctx context.Context,
	shardID types.ID,
	leaderClient client.StoreRPC,
	newPeerID types.ID,
) error {
	newNodeStatus, err := m.ms.tm.GetStoreStatus(ctx, newPeerID)
	if err != nil {
		return fmt.Errorf("new peer %s not found: %w", newPeerID, err)
	}
	m.ms.logger.Info("Adding peer to shard on node",
		zap.Stringer("newPeerID", newPeerID),
		zap.Stringer("shardID", shardID))
	return leaderClient.AddPeer(ctx, shardID, newPeerID, newNodeStatus.RaftURL)
}

func (m *MetadataShardOperations) RemovePeer(
	ctx context.Context,
	shardID types.ID,
	leaderClient client.StoreRPC,
	peerToRemoveID types.ID,
	async bool,
) error {
	timestampBytes := time.Now().UTC().AppendFormat(nil, time.RFC3339)
	if async {
		return leaderClient.RemovePeer(ctx, shardID, peerToRemoveID, timestampBytes)
	}
	return leaderClient.RemovePeerSync(ctx, shardID, peerToRemoveID, timestampBytes)
}

func (m *MetadataShardOperations) GetMedianKey(
	ctx context.Context,
	shardID types.ID,
) ([]byte, error) {
	storeClient, err := m.ms.healthyPeerForShard(shardID)
	if err != nil {
		return nil, fmt.Errorf("finding healthy peer for shard %s: %w", shardID, err)
	}
	return storeClient.MedianKeyForShard(ctx, shardID)
}

func (m *MetadataShardOperations) IsIDRemoved(
	ctx context.Context,
	shardID types.ID,
	leaderClient client.StoreRPC,
	peerID types.ID,
) (bool, error) {
	return leaderClient.IsIDRemoved(ctx, shardID, peerID)
}

// MetadataTableOperations implements reconciler.TableOperations by delegating to MetadataStore
type MetadataTableOperations struct {
	ms *MetadataStore
}

// NewMetadataTableOperations creates a new table operations adapter
func NewMetadataTableOperations(ms *MetadataStore) *MetadataTableOperations {
	return &MetadataTableOperations{ms: ms}
}

func (m *MetadataTableOperations) AddIndex(
	ctx context.Context,
	shardID types.ID,
	name string,
	config *indexes.IndexConfig,
) error {
	leaderClient, err := m.ms.leaderClientForShard(ctx, shardID)
	if err != nil {
		return err
	}
	return leaderClient.AddIndex(ctx, shardID, name, config)
}

func (m *MetadataTableOperations) DropIndex(
	ctx context.Context,
	shardID types.ID,
	name string,
) error {
	leaderClient, err := m.ms.leaderClientForShard(ctx, shardID)
	if err != nil {
		return err
	}
	return leaderClient.DropIndex(ctx, shardID, name)
}

func (m *MetadataTableOperations) UpdateSchema(
	ctx context.Context,
	shardID types.ID,
	schema *schema.TableSchema,
) error {
	leaderClient, err := m.ms.leaderClientForShard(ctx, shardID)
	if err != nil {
		return err
	}
	return leaderClient.UpdateSchema(ctx, shardID, schema)
}

func (m *MetadataTableOperations) ReassignShardsForSplit(
	transition tablemgr.SplitTransition,
) (types.IDSlice, *store.ShardConfig, error) {
	return m.ms.tm.ReassignShardsForSplit(transition)
}

func (m *MetadataTableOperations) ReassignShardsForMerge(
	transition tablemgr.MergeTransition,
) (*store.ShardConfig, error) {
	return m.ms.tm.ReassignShardsForMerge(transition)
}

func (m *MetadataTableOperations) HasReallocationRequest(ctx context.Context) (bool, error) {
	return m.ms.tm.HasReallocationRequest(ctx)
}

func (m *MetadataTableOperations) ClearReallocationRequest(ctx context.Context) error {
	return m.ms.tm.ClearReallocationRequest(ctx)
}

func (m *MetadataTableOperations) DropReadSchema(tableName string) error {
	return m.ms.tm.DropReadSchema(tableName)
}

func (m *MetadataTableOperations) GetTablesWithReadSchemas() ([]*store.Table, error) {
	return m.ms.GetTablesWithReadSchemas()
}

func (m *MetadataTableOperations) DeleteTombstones(ctx context.Context, storeIDs []types.ID) error {
	return m.ms.tm.DeleteTombstones(ctx, storeIDs)
}

// MetadataStoreOperations implements reconciler.StoreOperations by delegating to MetadataStore
type MetadataStoreOperations struct {
	ms *MetadataStore
}

// NewMetadataStoreOperations creates a new store operations adapter
func NewMetadataStoreOperations(ms *MetadataStore) *MetadataStoreOperations {
	return &MetadataStoreOperations{ms: ms}
}

func (m *MetadataStoreOperations) GetStoreClient(
	ctx context.Context,
	nodeID types.ID,
) (client.StoreRPC, bool, error) {
	return m.ms.tm.GetStoreClient(ctx, nodeID)
}

func (m *MetadataStoreOperations) GetStoreStatus(
	ctx context.Context,
	nodeID types.ID,
) (*tablemgr.StoreStatus, error) {
	return m.ms.tm.GetStoreStatus(ctx, nodeID)
}

func (m *MetadataStoreOperations) GetShardStatuses() (map[types.ID]*store.ShardStatus, error) {
	return m.ms.tm.GetShardStatuses()
}

func (m *MetadataStoreOperations) GetLeaderClientForShard(
	ctx context.Context,
	shardID types.ID,
) (client.StoreRPC, error) {
	return m.ms.leaderClientForShard(ctx, shardID)
}

func (m *MetadataStoreOperations) UpdateShardSplitState(
	ctx context.Context,
	shardID types.ID,
	splitState *db.SplitState,
) error {
	return m.ms.tm.UpdateShardSplitState(ctx, shardID, splitState)
}

// GetTablesWithReadSchemas returns all tables that have a read schema defined
// (indicating an index rebuild is in progress)
func (ms *MetadataStore) GetTablesWithReadSchemas() ([]*store.Table, error) {
	tables, err := ms.tm.Tables(nil, nil)
	if err != nil {
		return nil, err
	}

	result := []*store.Table{}
	for _, table := range tables {
		if table.ReadSchema != nil {
			result = append(result, table)
		}
	}
	return result, nil
}

// Verify that our adapters implement the reconciler interfaces at compile time
var (
	_ reconciler.ShardOperations = (*MetadataShardOperations)(nil)
	_ reconciler.TableOperations = (*MetadataTableOperations)(nil)
	_ reconciler.StoreOperations = (*MetadataStoreOperations)(nil)
)
