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

package client

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/store/storeutils"
	"go.etcd.io/raft/v3/raftpb"
)

var _ StoreRPC = (*LocalStoreClient)(nil)

// LocalStoreClient implements StoreRPC by calling the store directly in-process,
// bypassing HTTP. Used in swarm mode where metadata and store share a process.
type LocalStoreClient struct {
	nodeID types.ID
	store  store.StoreIface
}

func NewLocalStoreClient(nodeID types.ID, s store.StoreIface) *LocalStoreClient {
	return &LocalStoreClient{nodeID: nodeID, store: s}
}

func (c *LocalStoreClient) ID() types.ID { return c.nodeID }

func (c *LocalStoreClient) shard(shardID types.ID) (store.ShardIface, error) {
	shard, ok := c.store.Shard(shardID)
	if !ok {
		return nil, fmt.Errorf("shard %s not found on store %s", shardID, c.nodeID)
	}
	return shard, nil
}

func (c *LocalStoreClient) Batch(
	ctx context.Context,
	shardID types.ID,
	writes [][2][]byte,
	deletes [][]byte,
	transforms []*db.Transform,
	syncLevel db.Op_SyncLevel,
) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	err = shard.Batch(ctx, db.BatchOp_builder{
		Writes:     db.WritesFromTuples(writes),
		Deletes:    deletes,
		Transforms: transforms,
		SyncLevel:  &syncLevel,
		Timestamp:  storeutils.GetTimestampFromContext(ctx),
	}.Build(), false)
	if errors.Is(err, db.ErrPartialSuccess) {
		return nil
	}
	return err
}

func (c *LocalStoreClient) ApplyMergeChunk(
	ctx context.Context,
	shardID types.ID,
	writes [][2][]byte,
	deletes [][]byte,
	_ db.Op_SyncLevel,
) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	level := db.Op_SyncLevelInternalMergeCopy
	err = shard.ApplyMergeChunk(ctx, db.BatchOp_builder{
		Writes:    db.WritesFromTuples(writes),
		Deletes:   deletes,
		SyncLevel: &level,
		Timestamp: storeutils.GetTimestampFromContext(ctx),
	}.Build())
	if errors.Is(err, db.ErrPartialSuccess) {
		return nil
	}
	return err
}

func (c *LocalStoreClient) Backup(ctx context.Context, shardID types.ID, loc, id string) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	return shard.Backup(ctx, loc, id)
}

func (c *LocalStoreClient) Lookup(ctx context.Context, shardID types.ID, keys []string) (map[string][]byte, error) {
	shard, err := c.shard(shardID)
	if err != nil {
		return nil, err
	}
	results := make(map[string][]byte, len(keys))
	for _, key := range keys {
		doc, lookupErr := shard.Lookup(ctx, key)
		if lookupErr != nil {
			if errors.Is(lookupErr, db.ErrNotFound) || lookupErr.Error() == "not found" {
				continue
			}
			return nil, lookupErr
		}
		encoded, marshalErr := json.Marshal(doc)
		if marshalErr != nil {
			return nil, fmt.Errorf("marshalling lookup result for %q: %w", key, marshalErr)
		}
		results[key] = encoded
	}
	return results, nil
}

func (c *LocalStoreClient) LookupWithVersion(ctx context.Context, shardID types.ID, key string) ([]byte, uint64, error) {
	shard, err := c.shard(shardID)
	if err != nil {
		return nil, 0, err
	}
	doc, err := shard.Lookup(ctx, key)
	if err != nil {
		return nil, 0, err
	}
	encoded, err := json.Marshal(doc)
	if err != nil {
		return nil, 0, fmt.Errorf("marshalling lookup result for %q: %w", key, err)
	}
	version, err := shard.GetTimestamp(key)
	if err != nil {
		return nil, 0, err
	}
	return encoded, version, nil
}

func (c *LocalStoreClient) AddIndex(ctx context.Context, shardID types.ID, name string, config *indexes.IndexConfig) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	if config == nil {
		return fmt.Errorf("index config is required")
	}
	configCopy := *config
	configCopy.Name = name
	return shard.AddIndex(ctx, configCopy)
}

func (c *LocalStoreClient) MergeRange(ctx context.Context, shardID types.ID, byteRange [2][]byte) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	return shard.SetRange(ctx, byteRange)
}

func (c *LocalStoreClient) SetMergeState(ctx context.Context, shardID types.ID, state *db.MergeState) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	return shard.SetMergeState(ctx, state)
}

func (c *LocalStoreClient) FinalizeMerge(ctx context.Context, shardID types.ID, byteRange [2][]byte) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	return shard.FinalizeMerge(ctx, byteRange)
}

func (c *LocalStoreClient) UpdateSchema(ctx context.Context, shardID types.ID, tableSchema *schema.TableSchema) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	return shard.UpdateSchema(ctx, tableSchema)
}

func (c *LocalStoreClient) PrepareSplit(ctx context.Context, shardID types.ID, splitKey []byte) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	return shard.PrepareSplit(ctx, splitKey)
}

func (c *LocalStoreClient) SplitShard(ctx context.Context, shardID types.ID, newShardID types.ID, splitKey []byte) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	return shard.Split(ctx, uint64(newShardID), splitKey)
}

func (c *LocalStoreClient) FinalizeSplit(ctx context.Context, shardID types.ID, newRangeEnd []byte) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	return shard.FinalizeSplit(ctx, newRangeEnd)
}

func (c *LocalStoreClient) RollbackSplit(ctx context.Context, shardID types.ID) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	return shard.RollbackSplit(ctx)
}

func (c *LocalStoreClient) TransferLeadership(ctx context.Context, shardID types.ID, targetNodeID types.ID) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	shard.TransferLeadership(ctx, targetNodeID)
	return nil
}

func (c *LocalStoreClient) AddPeer(ctx context.Context, shardID types.ID, newPeerID types.ID, newPeerURL string) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	return shard.ApplyConfChange(ctx, raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  uint64(newPeerID),
		Context: []byte(newPeerURL),
	})
}

func (c *LocalStoreClient) RemovePeer(ctx context.Context, shardID types.ID, peerToRemoveID types.ID, timestamp []byte) error {
	return c.removePeer(ctx, shardID, peerToRemoveID, timestamp)
}

func (c *LocalStoreClient) RemovePeerSync(ctx context.Context, shardID types.ID, peerToRemoveID types.ID, timestamp []byte) error {
	return c.removePeer(ctx, shardID, peerToRemoveID, timestamp)
}

func (c *LocalStoreClient) removePeer(ctx context.Context, shardID types.ID, peerToRemoveID types.ID, timestamp []byte) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	ctxBytes := timestamp
	if len(ctxBytes) == 0 {
		ctxBytes = make([]byte, 8)
		binary.LittleEndian.PutUint64(ctxBytes, uint64(peerToRemoveID))
	}
	return shard.ApplyConfChange(ctx, raftpb.ConfChange{
		Type:    raftpb.ConfChangeRemoveNode,
		NodeID:  uint64(peerToRemoveID),
		Context: ctxBytes,
	})
}

func (c *LocalStoreClient) StopShard(ctx context.Context, shardID types.ID) error {
	return c.store.StopRaftGroup(shardID)
}

func (c *LocalStoreClient) StartShard(ctx context.Context, shardID types.ID, req *store.ShardStartRequest) error {
	if req == nil {
		return fmt.Errorf("start shard request is required")
	}
	return c.store.StartRaftGroup(shardID, req.Peers, req.Join, &store.ShardStartConfig{
		ShardConfig: req.ShardConfig,
		Timestamp:   req.Timestamp,
	})
}

func (c *LocalStoreClient) DropIndex(ctx context.Context, shardID types.ID, name string) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	return shard.DropIndex(ctx, name)
}

func (c *LocalStoreClient) Status(ctx context.Context) (*store.StoreStatus, error) {
	return c.store.Status(), nil
}

func (c *LocalStoreClient) MedianKeyForShard(ctx context.Context, shardID types.ID) ([]byte, error) {
	shard, err := c.shard(shardID)
	if err != nil {
		return nil, err
	}
	return shard.FindMedianKey()
}

func (c *LocalStoreClient) IsIDRemoved(ctx context.Context, shardID types.ID, nodeID types.ID) (bool, error) {
	shard, err := c.shard(shardID)
	if err != nil {
		return false, err
	}
	return shard.IsIDRemoved(uint64(nodeID)), nil
}

func (c *LocalStoreClient) Scan(
	ctx context.Context,
	shardID types.ID,
	fromKey []byte,
	toKey []byte,
	opts ScanOptions,
) (*db.ScanResult, error) {
	return c.store.Scan(ctx, shardID, fromKey, toKey, db.ScanOptions{
		InclusiveFrom:    opts.InclusiveFrom,
		ExclusiveTo:      opts.ExclusiveTo,
		IncludeDocuments: opts.IncludeDocuments,
		FilterQuery:      opts.FilterQuery,
		Limit:            opts.Limit,
	})
}

func (c *LocalStoreClient) ExportRangeChunk(
	ctx context.Context,
	shardID types.ID,
	startKey, endKey, afterKey []byte,
	limit int,
) ([][2][]byte, []byte, bool, error) {
	shard, err := c.shard(shardID)
	if err != nil {
		return nil, nil, false, err
	}
	return shard.ExportRangeChunk(ctx, startKey, endKey, afterKey, limit)
}

func (c *LocalStoreClient) ListMergeDeltaEntriesAfter(
	ctx context.Context,
	shardID types.ID,
	afterSeq uint64,
) ([]*db.MergeDeltaEntry, error) {
	shard, err := c.shard(shardID)
	if err != nil {
		return nil, err
	}
	return shard.ListMergeDeltaEntriesAfter(afterSeq)
}

func (c *LocalStoreClient) InitTransaction(
	ctx context.Context,
	shardID types.ID,
	txnID []byte,
	timestamp uint64,
	participants [][]byte,
) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	return shard.SyncWriteOp(ctx, db.Op_builder{
		Op: db.Op_OpInitTransaction,
		InitTransaction: db.InitTransactionOp_builder{
			TxnId:        txnID,
			Timestamp:    timestamp,
			Participants: participants,
		}.Build(),
	}.Build())
}

func (c *LocalStoreClient) WriteIntent(
	ctx context.Context,
	shardID types.ID,
	txnID []byte,
	timestamp uint64,
	coordinatorShard []byte,
	writes [][2][]byte,
	deletes [][]byte,
	transforms []*db.Transform,
	predicates []*db.VersionPredicate,
) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	return shard.SyncWriteOp(ctx, db.Op_builder{
		Op: db.Op_OpWriteIntent,
		WriteIntent: db.WriteIntentOp_builder{
			TxnId:            txnID,
			Timestamp:        timestamp,
			CoordinatorShard: coordinatorShard,
			Batch: db.BatchOp_builder{
				Writes:     db.WritesFromTuples(writes),
				Deletes:    deletes,
				Transforms: transforms,
			}.Build(),
			Predicates: predicates,
		}.Build(),
	}.Build())
}

func (c *LocalStoreClient) CommitTransaction(ctx context.Context, shardID types.ID, txnID []byte) (uint64, error) {
	shard, err := c.shard(shardID)
	if err != nil {
		return 0, err
	}
	if err := shard.SyncWriteOp(ctx, db.Op_builder{
		Op: db.Op_OpCommitTransaction,
		CommitTransaction: db.CommitTransactionOp_builder{
			TxnId: txnID,
		}.Build(),
	}.Build()); err != nil {
		return 0, err
	}
	return shard.GetCommitVersion(ctx, txnID)
}

func (c *LocalStoreClient) AbortTransaction(ctx context.Context, shardID types.ID, txnID []byte) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	return shard.SyncWriteOp(ctx, db.Op_builder{
		Op: db.Op_OpAbortTransaction,
		AbortTransaction: db.AbortTransactionOp_builder{
			TxnId: txnID,
		}.Build(),
	}.Build())
}

func (c *LocalStoreClient) ResolveIntent(
	ctx context.Context,
	shardID types.ID,
	txnID []byte,
	status int32,
	commitVersion uint64,
) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	return shard.SyncWriteOp(ctx, db.Op_builder{
		Op: db.Op_OpResolveIntents,
		ResolveIntents: db.ResolveIntentsOp_builder{
			TxnId:         txnID,
			Status:        status,
			CommitVersion: commitVersion,
		}.Build(),
	}.Build())
}

func (c *LocalStoreClient) GetTransactionStatus(ctx context.Context, shardID types.ID, txnID []byte) (int32, error) {
	shard, err := c.shard(shardID)
	if err != nil {
		return 0, err
	}
	return shard.GetTransactionStatus(ctx, txnID)
}

func (c *LocalStoreClient) GetEdges(
	ctx context.Context,
	shardID types.ID,
	indexName string,
	key string,
	edgeType string,
	direction indexes.EdgeDirection,
) ([]indexes.Edge, error) {
	shard, err := c.shard(shardID)
	if err != nil {
		return nil, err
	}
	return shard.GetEdges(ctx, indexName, []byte(key), edgeType, direction)
}
