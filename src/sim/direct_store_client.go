package sim

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	json "github.com/antflydb/antfly/pkg/libaf/json"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store"
	storeclient "github.com/antflydb/antfly/src/store/client"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"go.etcd.io/raft/v3/raftpb"
)

type directStoreClient struct {
	h      *Harness
	nodeID types.ID
}

var _ storeclient.StoreRPC = (*directStoreClient)(nil)

const simulatorControlPlaneTimeout = 5 * time.Minute

func (c *directStoreClient) ID() types.ID {
	return c.nodeID
}

func (c *directStoreClient) Batch(
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
	return shard.Batch(ctx, db.BatchOp_builder{
		Writes:     db.WritesFromTuples(writes),
		Deletes:    deletes,
		Transforms: transforms,
		SyncLevel:  &syncLevel,
	}.Build(), false)
}

func (c *directStoreClient) Backup(ctx context.Context, shardID types.ID, loc, id string) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	return shard.Backup(ctx, loc, id)
}

func (c *directStoreClient) Lookup(ctx context.Context, shardID types.ID, keys []string) (map[string][]byte, error) {
	shard, err := c.shard(shardID)
	if err != nil {
		return nil, err
	}

	results := make(map[string][]byte, len(keys))
	for _, key := range keys {
		doc, err := shard.Lookup(ctx, key)
		if err != nil {
			if errors.Is(err, db.ErrNotFound) || err.Error() == "not found" {
				continue
			}
			return nil, err
		}
		encoded, err := json.Marshal(doc)
		if err != nil {
			return nil, fmt.Errorf("marshalling lookup result for %q: %w", key, err)
		}
		results[key] = encoded
	}
	return results, nil
}

func (c *directStoreClient) LookupWithVersion(ctx context.Context, shardID types.ID, key string) ([]byte, uint64, error) {
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

func (c *directStoreClient) AddIndex(ctx context.Context, shardID types.ID, name string, config *indexes.IndexConfig) error {
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

func (c *directStoreClient) MergeRange(ctx context.Context, shardID types.ID, byteRange [2][]byte) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	return shard.SetRange(ctx, byteRange)
}

func (c *directStoreClient) UpdateSchema(ctx context.Context, shardID types.ID, tableSchema *schema.TableSchema) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	return shard.UpdateSchema(ctx, tableSchema)
}

func (c *directStoreClient) PrepareSplit(ctx context.Context, shardID types.ID, splitKey []byte) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	return c.h.runWithProgress(simulatorControlPlaneTimeout, func() error {
		return shard.PrepareSplit(ctx, splitKey)
	})
}

func (c *directStoreClient) SplitShard(ctx context.Context, shardID types.ID, newShardID types.ID, splitKey []byte) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	return c.h.runWithProgress(simulatorControlPlaneTimeout, func() error {
		return shard.Split(ctx, uint64(newShardID), splitKey)
	})
}

func (c *directStoreClient) FinalizeSplit(ctx context.Context, shardID types.ID, newRangeEnd []byte) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	c.h.recordEvent("finalize_split", "attempt shard=%s node=%s range_end=%x", shardID, c.nodeID, newRangeEnd)
	err = c.h.runWithProgress(simulatorControlPlaneTimeout, func() error {
		return shard.FinalizeSplit(ctx, newRangeEnd)
	})
	if err != nil {
		c.h.recordEvent("finalize_split", "failed shard=%s node=%s err=%v", shardID, c.nodeID, err)
		return err
	}
	c.h.recordEvent("finalize_split", "completed shard=%s node=%s", shardID, c.nodeID)
	return nil
}

func (c *directStoreClient) RollbackSplit(ctx context.Context, shardID types.ID) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	return shard.RollbackSplit(ctx)
}

func (c *directStoreClient) TransferLeadership(ctx context.Context, shardID types.ID, targetNodeID types.ID) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	shard.TransferLeadership(ctx, targetNodeID)
	return nil
}

func (c *directStoreClient) AddPeer(ctx context.Context, shardID types.ID, newPeerID types.ID, newPeerURL string) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	return c.h.runWithProgress(simulatorControlPlaneTimeout, func() error {
		return shard.ApplyConfChange(ctx, raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  uint64(newPeerID),
			Context: []byte(newPeerURL),
		})
	})
}

func (c *directStoreClient) RemovePeer(ctx context.Context, shardID types.ID, peerToRemoveID types.ID, timestamp []byte) error {
	return c.removePeer(ctx, shardID, peerToRemoveID, timestamp)
}

func (c *directStoreClient) RemovePeerSync(ctx context.Context, shardID types.ID, peerToRemoveID types.ID, timestamp []byte) error {
	return c.removePeer(ctx, shardID, peerToRemoveID, timestamp)
}

func (c *directStoreClient) StopShard(ctx context.Context, shardID types.ID) error {
	node, err := c.node()
	if err != nil {
		return err
	}
	node.store.StopRaftGroup(shardID)
	return nil
}

func (c *directStoreClient) StartShard(ctx context.Context, shardID types.ID, req *store.ShardStartRequest) error {
	node, err := c.node()
	if err != nil {
		return err
	}
	if req == nil {
		return fmt.Errorf("start shard request is required")
	}
	initWithDBArchive := ""
	if req.SplitStart {
		initWithDBArchive = common.SplitArchive(shardID)
	}
	return node.store.StartRaftGroup(shardID, req.Peers, req.Join, &store.ShardStartConfig{
		InitWithDBArchive: initWithDBArchive,
		ShardConfig:       req.ShardConfig,
		Timestamp:         req.Timestamp,
	})
}

func (c *directStoreClient) DropIndex(ctx context.Context, shardID types.ID, name string) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	return shard.DropIndex(ctx, name)
}

func (c *directStoreClient) Status(ctx context.Context) (_ *store.StoreStatus, err error) {
	node, err := c.node()
	if err != nil {
		return nil, err
	}
	if node.store == nil {
		return nil, fmt.Errorf("store %s is not running", c.nodeID)
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("store %s status unavailable: %v", c.nodeID, r)
		}
	}()
	return node.store.Status(), nil
}

func (c *directStoreClient) MedianKeyForShard(ctx context.Context, shardID types.ID) ([]byte, error) {
	shard, err := c.shard(shardID)
	if err != nil {
		return nil, err
	}
	return shard.FindMedianKey()
}

func (c *directStoreClient) IsIDRemoved(ctx context.Context, shardID types.ID, nodeID types.ID) (bool, error) {
	shard, err := c.shard(shardID)
	if err != nil {
		return false, err
	}
	return shard.IsIDRemoved(uint64(nodeID)), nil
}

func (c *directStoreClient) Scan(
	ctx context.Context,
	shardID types.ID,
	fromKey []byte,
	toKey []byte,
	opts storeclient.ScanOptions,
) (*db.ScanResult, error) {
	node, err := c.node()
	if err != nil {
		return nil, err
	}
	return node.store.Scan(ctx, shardID, fromKey, toKey, db.ScanOptions{
		InclusiveFrom:    opts.InclusiveFrom,
		ExclusiveTo:      opts.ExclusiveTo,
		IncludeDocuments: opts.IncludeDocuments,
		FilterQuery:      opts.FilterQuery,
		Limit:            opts.Limit,
	})
}

func (c *directStoreClient) InitTransaction(
	ctx context.Context,
	shardID types.ID,
	txnID []byte,
	timestamp uint64,
	participants [][]byte,
) error {
	return c.syncWriteOp(ctx, shardID, db.Op_builder{
		Op: db.Op_OpInitTransaction,
		InitTransaction: db.InitTransactionOp_builder{
			TxnId:        txnID,
			Timestamp:    timestamp,
			Participants: participants,
		}.Build(),
	}.Build())
}

func (c *directStoreClient) WriteIntent(
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
	return c.syncWriteOp(ctx, shardID, db.Op_builder{
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

func (c *directStoreClient) CommitTransaction(ctx context.Context, shardID types.ID, txnID []byte) (uint64, error) {
	if err := c.syncWriteOp(ctx, shardID, db.Op_builder{
		Op: db.Op_OpCommitTransaction,
		CommitTransaction: db.CommitTransactionOp_builder{
			TxnId: txnID,
		}.Build(),
	}.Build()); err != nil {
		return 0, err
	}
	shard, err := c.shard(shardID)
	if err != nil {
		return 0, err
	}
	return shard.GetCommitVersion(ctx, txnID)
}

func (c *directStoreClient) AbortTransaction(ctx context.Context, shardID types.ID, txnID []byte) error {
	return c.syncWriteOp(ctx, shardID, db.Op_builder{
		Op: db.Op_OpAbortTransaction,
		AbortTransaction: db.AbortTransactionOp_builder{
			TxnId: txnID,
		}.Build(),
	}.Build())
}

func (c *directStoreClient) ResolveIntent(
	ctx context.Context,
	shardID types.ID,
	txnID []byte,
	status int32,
	commitVersion uint64,
) error {
	return c.syncWriteOp(ctx, shardID, db.Op_builder{
		Op: db.Op_OpResolveIntents,
		ResolveIntents: db.ResolveIntentsOp_builder{
			TxnId:         txnID,
			Status:        status,
			CommitVersion: commitVersion,
		}.Build(),
	}.Build())
}

func (c *directStoreClient) GetTransactionStatus(ctx context.Context, shardID types.ID, txnID []byte) (int32, error) {
	shard, err := c.shard(shardID)
	if err != nil {
		return 0, err
	}
	return shard.GetTransactionStatus(ctx, txnID)
}

func (c *directStoreClient) GetEdges(
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

func (c *directStoreClient) node() (*storeNode, error) {
	node := c.h.stores[c.nodeID]
	if node == nil || node.store == nil {
		return nil, fmt.Errorf("store %s is not running", c.nodeID)
	}
	return node, nil
}

func (c *directStoreClient) shard(shardID types.ID) (store.ShardIface, error) {
	node, err := c.node()
	if err != nil {
		return nil, err
	}
	shard, ok := node.store.Shard(shardID)
	if !ok {
		return nil, fmt.Errorf("shard %s not found on store %s", shardID, c.nodeID)
	}
	return shard, nil
}

func (c *directStoreClient) removePeer(ctx context.Context, shardID types.ID, peerToRemoveID types.ID, timestamp []byte) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	ctxBytes := timestamp
	if len(ctxBytes) == 0 {
		ctxBytes = make([]byte, 8)
		binary.LittleEndian.PutUint64(ctxBytes, uint64(peerToRemoveID))
	}
	return c.h.runWithProgress(30*time.Second, func() error {
		return shard.ApplyConfChange(ctx, raftpb.ConfChange{
			Type:    raftpb.ConfChangeRemoveNode,
			NodeID:  uint64(peerToRemoveID),
			Context: ctxBytes,
		})
	})
}

func (c *directStoreClient) syncWriteOp(ctx context.Context, shardID types.ID, op *db.Op) error {
	shard, err := c.shard(shardID)
	if err != nil {
		return err
	}
	return c.h.runWithProgress(30*time.Second, func() error {
		return shard.SyncWriteOp(ctx, op)
	})
}

func newDirectStoreClient(h *Harness, id types.ID) storeclient.StoreRPC {
	return &directStoreClient{
		h:      h,
		nodeID: id,
	}
}
