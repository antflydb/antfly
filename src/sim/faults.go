package sim

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store"
	storeclient "github.com/antflydb/antfly/src/store/client"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/store/db/indexes"
)

type RPCEvent struct {
	NodeID  types.ID
	Method  string
	ShardID types.ID
}

type rpcHook struct {
	remaining int
	match     func(RPCEvent) bool
	run       func(context.Context, RPCEvent) error
}

type rpcFaultController struct {
	mu     sync.Mutex
	before []*rpcHook
	after  []*rpcHook
	tracer *TraceRecorder
	now    func() time.Time
}

func newRPCFaultController(tracer *TraceRecorder, now func() time.Time) *rpcFaultController {
	return &rpcFaultController{tracer: tracer, now: now}
}

func (c *rpcFaultController) addBefore(remaining int, match func(RPCEvent) bool, run func(context.Context, RPCEvent) error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.before = append(c.before, &rpcHook{remaining: remaining, match: match, run: run})
}

func (c *rpcFaultController) addAfter(remaining int, match func(RPCEvent) bool, run func(context.Context, RPCEvent) error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.after = append(c.after, &rpcHook{remaining: remaining, match: match, run: run})
}

func (c *rpcFaultController) runBefore(ctx context.Context, event RPCEvent) error {
	return c.runHooks(ctx, event, true)
}

func (c *rpcFaultController) runAfter(ctx context.Context, event RPCEvent) error {
	return c.runHooks(ctx, event, false)
}

func (c *rpcFaultController) runHooks(ctx context.Context, event RPCEvent, before bool) error {
	var hooks []*rpcHook

	c.mu.Lock()
	target := &c.after
	if before {
		target = &c.before
	}
	next := (*target)[:0]
	for _, hook := range *target {
		if hook == nil {
			continue
		}
		if hook.match == nil || hook.match(event) {
			hooks = append(hooks, hook)
			if hook.remaining > 0 {
				hook.remaining--
			}
		}
		if hook.remaining != 0 {
			next = append(next, hook)
		}
	}
	*target = next
	c.mu.Unlock()

	for _, hook := range hooks {
		if hook.run == nil {
			continue
		}
		if c.tracer != nil {
			stage := "after"
			if before {
				stage = "before"
			}
			at := time.Now().UTC()
			if c.now != nil {
				at = c.now()
			}
			c.tracer.RecordEvent(at, "rpc_fault", "%s %s shard=%s node=%s", stage, event.Method, event.ShardID, event.NodeID)
		}
		if err := hook.run(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (h *Harness) AddBeforeRPCFault(
	remaining int,
	match func(RPCEvent) bool,
	run func(context.Context, RPCEvent) error,
) {
	h.applyStoreClientFactory(func(client *http.Client, id types.ID, url string) storeclient.StoreRPC {
		return h.newStoreClient(client, id, url)
	})
	h.faults.addBefore(remaining, match, run)
}

func (h *Harness) AddAfterRPCFault(
	remaining int,
	match func(RPCEvent) bool,
	run func(context.Context, RPCEvent) error,
) {
	h.applyStoreClientFactory(func(client *http.Client, id types.ID, url string) storeclient.StoreRPC {
		return h.newStoreClient(client, id, url)
	})
	h.faults.addAfter(remaining, match, run)
}

func (h *Harness) newStoreClient(_ *http.Client, id types.ID, url string) storeclient.StoreRPC {
	inner := newDirectStoreClient(h, id)
	return &faultingStoreClient{
		StoreRPC:   inner,
		nodeID:     id,
		controller: h.faults,
	}
}

type faultingStoreClient struct {
	storeclient.StoreRPC
	nodeID     types.ID
	controller *rpcFaultController
}

func (c *faultingStoreClient) invokeBefore(ctx context.Context, method string, shardID types.ID) error {
	if c.controller == nil {
		return nil
	}
	return c.controller.runBefore(ctx, RPCEvent{NodeID: c.nodeID, Method: method, ShardID: shardID})
}

func (c *faultingStoreClient) invokeAfter(ctx context.Context, method string, shardID types.ID) error {
	if c.controller == nil {
		return nil
	}
	return c.controller.runAfter(ctx, RPCEvent{NodeID: c.nodeID, Method: method, ShardID: shardID})
}

func (c *faultingStoreClient) Batch(
	ctx context.Context,
	shardID types.ID,
	writes [][2][]byte,
	deletes [][]byte,
	transforms []*db.Transform,
	syncLevel db.Op_SyncLevel,
) error {
	if err := c.invokeBefore(ctx, "Batch", shardID); err != nil {
		return err
	}
	err := c.StoreRPC.Batch(ctx, shardID, writes, deletes, transforms, syncLevel)
	if afterErr := c.invokeAfter(ctx, "Batch", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return err
}

func (c *faultingStoreClient) Lookup(ctx context.Context, shardID types.ID, keys []string) (map[string][]byte, error) {
	if err := c.invokeBefore(ctx, "Lookup", shardID); err != nil {
		return nil, err
	}
	result, err := c.StoreRPC.Lookup(ctx, shardID, keys)
	if afterErr := c.invokeAfter(ctx, "Lookup", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return result, err
}

func (c *faultingStoreClient) LookupWithVersion(
	ctx context.Context,
	shardID types.ID,
	key string,
) ([]byte, uint64, error) {
	if err := c.invokeBefore(ctx, "LookupWithVersion", shardID); err != nil {
		return nil, 0, err
	}
	value, version, err := c.StoreRPC.LookupWithVersion(ctx, shardID, key)
	if afterErr := c.invokeAfter(ctx, "LookupWithVersion", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return value, version, err
}

func (c *faultingStoreClient) AddIndex(ctx context.Context, shardID types.ID, name string, config *indexes.IndexConfig) error {
	if err := c.invokeBefore(ctx, "AddIndex", shardID); err != nil {
		return err
	}
	err := c.StoreRPC.AddIndex(ctx, shardID, name, config)
	if afterErr := c.invokeAfter(ctx, "AddIndex", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return err
}

func (c *faultingStoreClient) MergeRange(ctx context.Context, shardID types.ID, byteRange [2][]byte) error {
	if err := c.invokeBefore(ctx, "MergeRange", shardID); err != nil {
		return err
	}
	err := c.StoreRPC.MergeRange(ctx, shardID, byteRange)
	if afterErr := c.invokeAfter(ctx, "MergeRange", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return err
}

func (c *faultingStoreClient) SetMergeState(
	ctx context.Context,
	shardID types.ID,
	state *db.MergeState,
) error {
	if err := c.invokeBefore(ctx, "SetMergeState", shardID); err != nil {
		return err
	}
	err := c.StoreRPC.SetMergeState(ctx, shardID, state)
	if afterErr := c.invokeAfter(ctx, "SetMergeState", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return err
}

func (c *faultingStoreClient) FinalizeMerge(
	ctx context.Context,
	shardID types.ID,
	byteRange [2][]byte,
) error {
	if err := c.invokeBefore(ctx, "FinalizeMerge", shardID); err != nil {
		return err
	}
	err := c.StoreRPC.FinalizeMerge(ctx, shardID, byteRange)
	if afterErr := c.invokeAfter(ctx, "FinalizeMerge", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return err
}

func (c *faultingStoreClient) UpdateSchema(ctx context.Context, shardID types.ID, tableSchema *schema.TableSchema) error {
	if err := c.invokeBefore(ctx, "UpdateSchema", shardID); err != nil {
		return err
	}
	err := c.StoreRPC.UpdateSchema(ctx, shardID, tableSchema)
	if afterErr := c.invokeAfter(ctx, "UpdateSchema", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return err
}

func (c *faultingStoreClient) PrepareSplit(ctx context.Context, shardID types.ID, splitKey []byte) error {
	if err := c.invokeBefore(ctx, "PrepareSplit", shardID); err != nil {
		return err
	}
	err := c.StoreRPC.PrepareSplit(ctx, shardID, splitKey)
	if afterErr := c.invokeAfter(ctx, "PrepareSplit", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return err
}

func (c *faultingStoreClient) SplitShard(ctx context.Context, shardID, newShardID types.ID, splitKey []byte) error {
	if err := c.invokeBefore(ctx, "SplitShard", shardID); err != nil {
		return err
	}
	err := c.StoreRPC.SplitShard(ctx, shardID, newShardID, splitKey)
	if afterErr := c.invokeAfter(ctx, "SplitShard", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return err
}

func (c *faultingStoreClient) FinalizeSplit(ctx context.Context, shardID types.ID, newRangeEnd []byte) error {
	if err := c.invokeBefore(ctx, "FinalizeSplit", shardID); err != nil {
		return err
	}
	err := c.StoreRPC.FinalizeSplit(ctx, shardID, newRangeEnd)
	if afterErr := c.invokeAfter(ctx, "FinalizeSplit", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return err
}

func (c *faultingStoreClient) RollbackSplit(ctx context.Context, shardID types.ID) error {
	if err := c.invokeBefore(ctx, "RollbackSplit", shardID); err != nil {
		return err
	}
	err := c.StoreRPC.RollbackSplit(ctx, shardID)
	if afterErr := c.invokeAfter(ctx, "RollbackSplit", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return err
}

func (c *faultingStoreClient) TransferLeadership(ctx context.Context, shardID, targetNodeID types.ID) error {
	if err := c.invokeBefore(ctx, "TransferLeadership", shardID); err != nil {
		return err
	}
	err := c.StoreRPC.TransferLeadership(ctx, shardID, targetNodeID)
	if afterErr := c.invokeAfter(ctx, "TransferLeadership", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return err
}

func (c *faultingStoreClient) AddPeer(ctx context.Context, shardID, newPeerID types.ID, newPeerURL string) error {
	if err := c.invokeBefore(ctx, "AddPeer", shardID); err != nil {
		return err
	}
	err := c.StoreRPC.AddPeer(ctx, shardID, newPeerID, newPeerURL)
	if afterErr := c.invokeAfter(ctx, "AddPeer", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return err
}

func (c *faultingStoreClient) RemovePeer(ctx context.Context, shardID, peerToRemoveID types.ID, timestamp []byte) error {
	if err := c.invokeBefore(ctx, "RemovePeer", shardID); err != nil {
		return err
	}
	err := c.StoreRPC.RemovePeer(ctx, shardID, peerToRemoveID, timestamp)
	if afterErr := c.invokeAfter(ctx, "RemovePeer", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return err
}

func (c *faultingStoreClient) RemovePeerSync(ctx context.Context, shardID, peerToRemoveID types.ID, timestamp []byte) error {
	if err := c.invokeBefore(ctx, "RemovePeerSync", shardID); err != nil {
		return err
	}
	err := c.StoreRPC.RemovePeerSync(ctx, shardID, peerToRemoveID, timestamp)
	if afterErr := c.invokeAfter(ctx, "RemovePeerSync", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return err
}

func (c *faultingStoreClient) StopShard(ctx context.Context, shardID types.ID) error {
	if err := c.invokeBefore(ctx, "StopShard", shardID); err != nil {
		return err
	}
	err := c.StoreRPC.StopShard(ctx, shardID)
	if afterErr := c.invokeAfter(ctx, "StopShard", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return err
}

func (c *faultingStoreClient) StartShard(ctx context.Context, shardID types.ID, req *store.ShardStartRequest) error {
	if err := c.invokeBefore(ctx, "StartShard", shardID); err != nil {
		return err
	}
	err := c.StoreRPC.StartShard(ctx, shardID, req)
	if afterErr := c.invokeAfter(ctx, "StartShard", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return err
}

func (c *faultingStoreClient) DropIndex(ctx context.Context, shardID types.ID, name string) error {
	if err := c.invokeBefore(ctx, "DropIndex", shardID); err != nil {
		return err
	}
	err := c.StoreRPC.DropIndex(ctx, shardID, name)
	if afterErr := c.invokeAfter(ctx, "DropIndex", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return err
}

func (c *faultingStoreClient) Status(ctx context.Context) (*store.StoreStatus, error) {
	if err := c.invokeBefore(ctx, "Status", 0); err != nil {
		return nil, err
	}
	status, err := c.StoreRPC.Status(ctx)
	if afterErr := c.invokeAfter(ctx, "Status", 0); err == nil && afterErr != nil {
		err = afterErr
	}
	return status, err
}

func (c *faultingStoreClient) MedianKeyForShard(ctx context.Context, shardID types.ID) ([]byte, error) {
	if err := c.invokeBefore(ctx, "MedianKeyForShard", shardID); err != nil {
		return nil, err
	}
	median, err := c.StoreRPC.MedianKeyForShard(ctx, shardID)
	if afterErr := c.invokeAfter(ctx, "MedianKeyForShard", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return median, err
}

func (c *faultingStoreClient) IsIDRemoved(ctx context.Context, shardID, nodeID types.ID) (bool, error) {
	if err := c.invokeBefore(ctx, "IsIDRemoved", shardID); err != nil {
		return false, err
	}
	removed, err := c.StoreRPC.IsIDRemoved(ctx, shardID, nodeID)
	if afterErr := c.invokeAfter(ctx, "IsIDRemoved", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return removed, err
}

func (c *faultingStoreClient) Scan(
	ctx context.Context,
	shardID types.ID,
	fromKey []byte,
	toKey []byte,
	opts storeclient.ScanOptions,
) (*db.ScanResult, error) {
	if err := c.invokeBefore(ctx, "Scan", shardID); err != nil {
		return nil, err
	}
	result, err := c.StoreRPC.Scan(ctx, shardID, fromKey, toKey, opts)
	if afterErr := c.invokeAfter(ctx, "Scan", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return result, err
}

func (c *faultingStoreClient) ExportRangeChunk(
	ctx context.Context,
	shardID types.ID,
	startKey, endKey, afterKey []byte,
	limit int,
) ([][2][]byte, []byte, bool, error) {
	if err := c.invokeBefore(ctx, "ExportRangeChunk", shardID); err != nil {
		return nil, nil, false, err
	}
	writes, nextKey, done, err := c.StoreRPC.ExportRangeChunk(ctx, shardID, startKey, endKey, afterKey, limit)
	if afterErr := c.invokeAfter(ctx, "ExportRangeChunk", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return writes, nextKey, done, err
}

func (c *faultingStoreClient) ListMergeDeltaEntriesAfter(
	ctx context.Context,
	shardID types.ID,
	afterSeq uint64,
) ([]*db.MergeDeltaEntry, error) {
	if err := c.invokeBefore(ctx, "ListMergeDeltaEntriesAfter", shardID); err != nil {
		return nil, err
	}
	entries, err := c.StoreRPC.ListMergeDeltaEntriesAfter(ctx, shardID, afterSeq)
	if afterErr := c.invokeAfter(ctx, "ListMergeDeltaEntriesAfter", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return entries, err
}

func (c *faultingStoreClient) InitTransaction(
	ctx context.Context,
	shardID types.ID,
	txnID []byte,
	timestamp uint64,
	participants [][]byte,
) error {
	if err := c.invokeBefore(ctx, "InitTransaction", shardID); err != nil {
		return err
	}
	err := c.StoreRPC.InitTransaction(ctx, shardID, txnID, timestamp, participants)
	if afterErr := c.invokeAfter(ctx, "InitTransaction", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return err
}

func (c *faultingStoreClient) WriteIntent(
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
	if err := c.invokeBefore(ctx, "WriteIntent", shardID); err != nil {
		return err
	}
	err := c.StoreRPC.WriteIntent(ctx, shardID, txnID, timestamp, coordinatorShard, writes, deletes, transforms, predicates)
	if afterErr := c.invokeAfter(ctx, "WriteIntent", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return err
}

func (c *faultingStoreClient) CommitTransaction(ctx context.Context, shardID types.ID, txnID []byte) (uint64, error) {
	if err := c.invokeBefore(ctx, "CommitTransaction", shardID); err != nil {
		return 0, err
	}
	version, err := c.StoreRPC.CommitTransaction(ctx, shardID, txnID)
	if afterErr := c.invokeAfter(ctx, "CommitTransaction", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return version, err
}

func (c *faultingStoreClient) AbortTransaction(ctx context.Context, shardID types.ID, txnID []byte) error {
	if err := c.invokeBefore(ctx, "AbortTransaction", shardID); err != nil {
		return err
	}
	err := c.StoreRPC.AbortTransaction(ctx, shardID, txnID)
	if afterErr := c.invokeAfter(ctx, "AbortTransaction", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return err
}

func (c *faultingStoreClient) ResolveIntent(
	ctx context.Context,
	shardID types.ID,
	txnID []byte,
	status int32,
	commitVersion uint64,
) error {
	if err := c.invokeBefore(ctx, "ResolveIntent", shardID); err != nil {
		return err
	}
	err := c.StoreRPC.ResolveIntent(ctx, shardID, txnID, status, commitVersion)
	if afterErr := c.invokeAfter(ctx, "ResolveIntent", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return err
}

func (c *faultingStoreClient) GetTransactionStatus(ctx context.Context, shardID types.ID, txnID []byte) (int32, error) {
	if err := c.invokeBefore(ctx, "GetTransactionStatus", shardID); err != nil {
		return 0, err
	}
	status, err := c.StoreRPC.GetTransactionStatus(ctx, shardID, txnID)
	if afterErr := c.invokeAfter(ctx, "GetTransactionStatus", shardID); err == nil && afterErr != nil {
		err = afterErr
	}
	return status, err
}
