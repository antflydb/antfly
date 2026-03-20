package sim

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/antflydb/antfly/lib/clock"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/workerpool"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/metadata"
	"github.com/antflydb/antfly/src/metadata/kv"
	"github.com/antflydb/antfly/src/snapstore"
	"github.com/antflydb/antfly/src/store"
	storeclient "github.com/antflydb/antfly/src/store/client"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/tablemgr"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type HarnessConfig struct {
	BaseDir                  string
	Start                    time.Time
	Logger                   *zap.Logger
	MetadataID               types.ID
	MetadataIDs              []types.ID
	StoreIDs                 []types.ID
	ReplicationFactor        uint64
	MaxShardSizeBytes        uint64
	MaxShardsPerTable        uint64
	DisableShardAlloc        bool
	SplitTimeout             time.Duration
	SplitFinalizeGracePeriod time.Duration
	TxnIDGenerator           func() uuid.UUID
}

type Harness struct {
	logger *zap.Logger
	config *common.Config

	clock   *clock.MockClock
	network *Network
	router  *Router
	client  *http.Client

	tableManager             *tablemgr.TableManager
	storeClientFactory       tablemgr.StoreClientFactory
	metadataAlias            string
	metadataOrder            []types.ID
	metadataNodes            map[types.ID]*metadataNode
	storeOrder               []types.ID
	stores                   map[types.ID]*storeNode
	expectedDocs             map[string][]byte
	expectedKeys             map[string]string
	lastRefreshed            time.Time
	pool                     *workerpool.Pool
	faults                   *rpcFaultController
	tracer                   *TraceRecorder
	lastLeaders              map[types.ID]types.ID
	lastSplits               map[types.ID]string
	lastMetaLeader           types.ID
	txnIDGenerator           func() uuid.UUID
	splitFinalizeGracePeriod time.Duration
}

type storeNode struct {
	info      *store.StoreInfo
	transport *Transport
	store     *store.Store
	errC      <-chan error
}

type metadataNode struct {
	info      *store.StoreInfo
	transport *Transport
	kv        *kv.MetadataStore
	tm        *tablemgr.TableManager
	node      *metadata.MetadataStore
	errC      <-chan error
}

func NewHarness(cfg HarnessConfig) (*Harness, error) {
	if cfg.BaseDir == "" {
		return nil, fmt.Errorf("base dir is required")
	}
	if cfg.Start.IsZero() {
		cfg.Start = time.Unix(0, 0).UTC()
	}
	if cfg.MetadataID == 0 {
		cfg.MetadataID = 100
	}
	if len(cfg.MetadataIDs) == 0 {
		cfg.MetadataIDs = []types.ID{cfg.MetadataID}
	}
	if len(cfg.StoreIDs) == 0 {
		cfg.StoreIDs = []types.ID{1, 2, 3}
	}
	if cfg.ReplicationFactor == 0 {
		cfg.ReplicationFactor = uint64(len(cfg.StoreIDs))
	}

	metadataIDs := slices.Clone(cfg.MetadataIDs)
	slices.Sort(metadataIDs)
	storeIDs := slices.Clone(cfg.StoreIDs)
	slices.Sort(storeIDs)

	clk := clock.NewMockClock(cfg.Start)
	network := NewNetwork(cfg.Start)
	network.SetDefaultLatency(0)
	network.SetSnapshotResolver(func(nodeID, shardID types.ID) (snapstore.SnapStore, error) {
		return snapstore.NewLocalSnapStore(cfg.BaseDir, shardID, nodeID)
	})
	router := NewRouter()
	httpClient := &http.Client{
		Transport: router,
		Timeout:   5 * time.Second,
	}

	antflyConfig := &common.Config{
		ReplicationFactor: cfg.ReplicationFactor,
		MaxShardSizeBytes: cfg.MaxShardSizeBytes,
		MaxShardsPerTable: cfg.MaxShardsPerTable,
		DisableShardAlloc: cfg.DisableShardAlloc,
		SplitTimeout:      cfg.SplitTimeout,
	}
	antflyConfig.Storage.Local.BaseDir = cfg.BaseDir

	metadataAlias := "http://metadata-sim.local"
	if antflyConfig.Metadata.OrchestrationUrls == nil {
		antflyConfig.Metadata.OrchestrationUrls = make(map[string]string)
	}
	antflyConfig.Metadata.OrchestrationUrls[metadataIDs[0].String()] = metadataAlias

	pool, err := workerpool.NewPool()
	if err != nil {
		return nil, fmt.Errorf("creating worker pool: %w", err)
	}

	h := &Harness{
		logger:                   cfg.Logger,
		config:                   antflyConfig,
		clock:                    clk,
		network:                  network,
		router:                   router,
		client:                   httpClient,
		metadataAlias:            metadataAlias,
		metadataOrder:            metadataIDs,
		metadataNodes:            make(map[types.ID]*metadataNode, len(metadataIDs)),
		storeOrder:               storeIDs,
		stores:                   make(map[types.ID]*storeNode, len(storeIDs)),
		expectedDocs:             make(map[string][]byte),
		expectedKeys:             make(map[string]string),
		pool:                     pool,
		tracer:                   NewTraceRecorder(),
		lastLeaders:              make(map[types.ID]types.ID),
		lastSplits:               make(map[types.ID]string),
		txnIDGenerator:           cfg.TxnIDGenerator,
		splitFinalizeGracePeriod: cfg.SplitFinalizeGracePeriod,
	}
	if h.logger == nil {
		h.logger = zap.NewNop()
	}
	metadataProxy := &metadataDBProxy{
		leader: h.metadataLeaderNode,
		run:    h.runWithProgress,
	}
	h.tableManager, err = tablemgr.NewTableManager(metadataProxy, httpClient, 0)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("creating leader-routed table manager: %w", err)
	}
	h.applyStoreClientFactory(func(_ *http.Client, id types.ID, url string) storeclient.StoreRPC {
		return newDirectStoreClient(h, id)
	})
	h.network.SetSnapshotObserver(func(event SnapshotTransferEvent) {
		h.recordEvent(
			"snapshot_transfer",
			"kind=%s shard=%s from=%s to=%s id=%s",
			event.Kind,
			event.ShardID,
			event.From,
			event.To,
			event.SnapshotID,
		)
	})
	h.faults = newRPCFaultController(h.tracer, h.clock.Now)
	if err := h.router.Register(metadataAlias, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		node, leaderErr := h.metadataLeaderNode()
		if leaderErr != nil {
			http.Error(w, leaderErr.Error(), http.StatusServiceUnavailable)
			return
		}
		node.node.InternalHandler().ServeHTTP(w, r)
	})); err != nil {
		_ = h.Close()
		return nil, fmt.Errorf("registering metadata alias route: %w", err)
	}
	if err := h.startMetadataCluster(cfg); err != nil {
		_ = h.Close()
		return nil, err
	}
	if _, err := h.WaitForMetadataLeader(20 * time.Second); err != nil {
		_ = h.Close()
		return nil, fmt.Errorf("waiting for metadata leader: %w", err)
	}

	for _, storeID := range storeIDs {
		if err := h.startStore(storeID); err != nil {
			_ = h.Close()
			return nil, err
		}
	}
	return h, nil
}

func (h *Harness) TableManager() *tablemgr.TableManager {
	return h.tableManager
}

func (h *Harness) Tables() (map[string]*store.Table, error) {
	return h.tableManager.TablesMap()
}

func (h *Harness) Trace() *TraceRecorder {
	return h.tracer
}

func (h *Harness) MetadataNode() *metadata.MetadataStore {
	node, err := h.metadataLeaderNode()
	if err != nil {
		return nil
	}
	return node.node
}

func (h *Harness) WaitForMetadataLeader(timeout time.Duration) (types.ID, error) {
	deadline := h.clock.Now().Add(timeout)
	for !h.clock.Now().After(deadline) {
		if leaderID, ok := h.currentMetadataLeader(); ok {
			return leaderID, nil
		}
		if err := h.Advance(200 * time.Millisecond); err != nil {
			return 0, err
		}
	}
	return 0, fmt.Errorf("metadata leader not elected within %s", timeout)
}

func (h *Harness) currentMetadataLeader() (types.ID, bool) {
	if len(h.metadataOrder) == 1 {
		nodeID := h.metadataOrder[0]
		node := h.metadataNodes[nodeID]
		if node != nil && node.kv != nil {
			return nodeID, true
		}
		return 0, false
	}
	leaderVotes := make(map[types.ID]int)
	healthy := 0
	for _, nodeID := range h.metadataOrder {
		node := h.metadataNodes[nodeID]
		if node == nil || node.kv == nil {
			continue
		}
		healthy++
		info := node.kv.Info()
		if info == nil || info.RaftStatus == nil || info.RaftStatus.Lead == 0 {
			continue
		}
		leaderVotes[types.ID(info.RaftStatus.Lead)]++
	}
	quorum := healthy/2 + 1
	for leaderID, votes := range leaderVotes {
		if votes >= quorum {
			if node := h.metadataNodes[leaderID]; node != nil && node.kv != nil {
				return leaderID, true
			}
		}
	}
	return 0, false
}

func (h *Harness) metadataLeaderNode() (*metadataNode, error) {
	leaderID, ok := h.currentMetadataLeader()
	if !ok {
		return nil, fmt.Errorf("metadata leader unavailable")
	}
	node := h.metadataNodes[leaderID]
	if node == nil || node.kv == nil || node.node == nil {
		return nil, fmt.Errorf("metadata leader %s unavailable", leaderID)
	}
	return node, nil
}

func (h *Harness) applyStoreClientFactory(factory tablemgr.StoreClientFactory) {
	h.storeClientFactory = factory
	if h.tableManager != nil {
		h.tableManager.SetStoreClientFactory(factory)
	}
	for _, nodeID := range h.metadataOrder {
		node := h.metadataNodes[nodeID]
		if node == nil || node.tm == nil {
			continue
		}
		node.tm.SetStoreClientFactory(factory)
	}
}

func (h *Harness) applyStoreClientFactoryToMetadataNode(nodeID types.ID) {
	factory := h.storeClientFactory
	if factory == nil {
		return
	}
	node := h.metadataNodes[nodeID]
	if node == nil || node.tm == nil {
		return
	}
	node.tm.SetStoreClientFactory(factory)
}

func (h *Harness) startMetadataCluster(cfg HarnessConfig) error {
	peers := make(common.Peers, 0, len(h.metadataOrder))
	for _, nodeID := range h.metadataOrder {
		peers = append(peers, common.Peer{
			ID:  nodeID,
			URL: fmt.Sprintf("raft://metadata-%d.local", nodeID),
		})
	}
	for _, nodeID := range h.metadataOrder {
		if err := h.startMetadataNode(cfg, nodeID, peers); err != nil {
			return err
		}
	}
	h.applyStoreClientFactory(func(_ *http.Client, id types.ID, url string) storeclient.StoreRPC {
		return newDirectStoreClient(h, id)
	})
	return nil
}

func (h *Harness) startMetadataNode(cfg HarnessConfig, nodeID types.ID, peers common.Peers) error {
	info := &store.StoreInfo{
		ID:      nodeID,
		RaftURL: fmt.Sprintf("raft://metadata-%d.local", nodeID),
		ApiURL:  fmt.Sprintf("http://metadata-%d.local", nodeID),
	}
	node := &metadataNode{
		info:      info,
		transport: h.network.NewTransport(nodeID),
	}
	metadataStore, err := kv.NewMetadataStoreWithOptions(
		h.logger,
		h.config,
		node.transport,
		info,
		peers,
		false,
		nil,
		kv.Options{Clock: h.clock},
	)
	if err != nil {
		return fmt.Errorf("creating metadata store %s: %w", nodeID, err)
	}
	node.kv = metadataStore
	node.errC = metadataStore.ErrorC()
	node.tm, err = tablemgr.NewTableManager(metadataStore, h.client, 0)
	if err != nil {
		metadataStore.Close()
		return fmt.Errorf("creating metadata table manager %s: %w", nodeID, err)
	}
	node.node, err = metadata.NewSimulationStore(
		h.logger,
		h.config,
		metadataStore,
		node.tm,
		metadata.SimulationOptions{
			Clock:                    h.clock,
			Pool:                     h.pool,
			TxnIDGenerator:           cfg.TxnIDGenerator,
			SplitFinalizeGracePeriod: cfg.SplitFinalizeGracePeriod,
			EnableLeaderLoop:         len(h.metadataOrder) > 1,
		},
	)
	if err != nil {
		metadataStore.Close()
		return fmt.Errorf("creating simulation metadata node %s: %w", nodeID, err)
	}
	if err := h.router.Register(info.ApiURL, node.node.InternalHandler()); err != nil {
		node.node = nil
		metadataStore.Close()
		return fmt.Errorf("registering metadata route for %s: %w", nodeID, err)
	}
	h.metadataNodes[nodeID] = node
	return nil
}

func (h *Harness) Clock() *clock.MockClock {
	return h.clock
}

func (h *Harness) Network() *Network {
	return h.network
}

func (h *Harness) ReconcileOnce(ctx context.Context) error {
	if err := h.RefreshStatuses(ctx); err != nil {
		return err
	}
	node, err := h.metadataLeaderNode()
	if err != nil {
		return err
	}
	if err := h.runWithProgress(simulatorControlPlaneTimeout, func() error {
		node.node.ReconcileOnce(ctx)
		return nil
	}); err != nil {
		return err
	}
	return h.RefreshStatuses(ctx)
}

func (h *Harness) RunWithProgress(timeout time.Duration, fn func() error) error {
	return h.runWithProgress(timeout, fn)
}

func (h *Harness) Close() error {
	var firstErr error
	for i := len(h.storeOrder) - 1; i >= 0; i-- {
		storeID := h.storeOrder[i]
		node := h.stores[storeID]
		if node == nil || node.store == nil {
			continue
		}
		if err := h.router.Unregister(node.info.ApiURL); err != nil && firstErr == nil {
			firstErr = err
		}
		node.store.Close()
		node.store = nil
		node.errC = nil
	}
	if h.metadataAlias != "" {
		if err := h.router.Unregister(h.metadataAlias); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for i := len(h.metadataOrder) - 1; i >= 0; i-- {
		nodeID := h.metadataOrder[i]
		node := h.metadataNodes[nodeID]
		if node == nil || node.kv == nil {
			continue
		}
		if err := h.router.Unregister(node.info.ApiURL); err != nil && firstErr == nil {
			firstErr = err
		}
		node.kv.Close()
		node.kv = nil
		node.node = nil
		node.tm = nil
		node.errC = nil
	}
	if h.pool != nil {
		h.pool.Close()
		h.pool = nil
	}
	return firstErr
}

func (h *Harness) CreateSingleShardTable(name string, shardID types.ID) (types.ID, error) {
	table, err := h.tableManager.CreateTable(name, tablemgr.TableConfig{
		NumShards: 1,
		StartID:   shardID,
	})
	if err != nil {
		return 0, fmt.Errorf("creating table: %w", err)
	}
	for id := range table.Shards {
		return id, nil
	}
	return 0, fmt.Errorf("table %s has no shards", name)
}

func (h *Harness) CreateTable(name string, cfg tablemgr.TableConfig) (*store.Table, error) {
	return h.tableManager.CreateTable(name, cfg)
}

func (h *Harness) ShardForKey(tableName, key string) (types.ID, error) {
	table, err := h.tableManager.GetTable(tableName)
	if err != nil {
		return 0, fmt.Errorf("loading table %s: %w", tableName, err)
	}
	shardID, err := table.FindShardForKey(key)
	if err != nil {
		return 0, fmt.Errorf("finding shard for %s/%s: %w", tableName, key, err)
	}
	return shardID, nil
}

func (h *Harness) WriteKey(tableName, key string, value []byte) error {
	shardID, err := h.ShardForKey(tableName, key)
	if err != nil {
		return err
	}
	if err := h.Write(shardID, key, value); err != nil {
		return err
	}
	h.expectedKeys[key] = tableName
	return nil
}

func (h *Harness) LookupKey(tableName, key string) ([]byte, error) {
	shardID, err := h.ShardForKey(tableName, key)
	if err != nil {
		return nil, err
	}
	leaderID, err := h.WaitForLeader(shardID, 20*time.Second)
	if err != nil {
		return nil, err
	}
	results, err := h.LookupFromStore(leaderID, shardID, []string{key})
	if err != nil {
		return nil, err
	}
	return results[key], nil
}

func (h *Harness) WaitForShardCount(tableName string, count int, timeout time.Duration) (*store.Table, error) {
	deadline := h.clock.Now().Add(timeout)
	for !h.clock.Now().After(deadline) {
		table, err := h.tableManager.GetTable(tableName)
		if err == nil && len(table.Shards) == count {
			return table, nil
		}
		if err := h.Advance(500 * time.Millisecond); err != nil {
			return nil, err
		}
	}
	table, err := h.tableManager.GetTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("loading table %s: %w", tableName, err)
	}
	return nil, fmt.Errorf("table %s reached %d shards, expected %d", tableName, len(table.Shards), count)
}

func (h *Harness) WaitFor(timeout time.Duration, check func() error) error {
	deadline := h.clock.Now().Add(timeout)
	var lastErr error
	for !h.clock.Now().After(deadline) {
		if err := check(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if err := h.Advance(200 * time.Millisecond); err != nil {
			return err
		}
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("condition not satisfied within %s", timeout)
}

func (h *Harness) GetTable(tableName string) (*store.Table, error) {
	return h.tableManager.GetTable(tableName)
}

func (h *Harness) GetShardStatus(shardID types.ID) (*store.ShardStatus, error) {
	return h.tableManager.GetShardStatus(shardID)
}

func (h *Harness) ExecuteTransaction(
	timeout time.Duration,
	writes map[types.ID][][2][]byte,
	deletes map[types.ID][][]byte,
	transforms map[types.ID][]*db.Transform,
	predicates map[types.ID][]*db.VersionPredicate,
	syncLevel db.Op_SyncLevel,
) error {
	h.recordEvent("txn", "begin writes=%d deletes=%d transforms=%d predicates=%d", len(writes), len(deletes), len(transforms), len(predicates))
	return h.runWithProgress(timeout, func() error {
		node, err := h.metadataLeaderNode()
		if err != nil {
			return err
		}
		err = node.node.ExecuteTransaction(
			context.Background(),
			writes,
			deletes,
			transforms,
			predicates,
			syncLevel,
		)
		if err != nil {
			h.recordEvent("txn", "failed: %v", err)
		} else {
			h.recordEvent("txn", "committed shard_groups=%d", len(writes)+len(deletes)+len(transforms))
		}
		return err
	})
}

func (h *Harness) StartShardOnAllStores(shardID types.ID) error {
	shardStatus, err := h.tableManager.GetShardStatus(shardID)
	if err != nil {
		return fmt.Errorf("loading shard %s status: %w", shardID, err)
	}
	table, err := h.tableManager.GetTable(shardStatus.Table)
	if err != nil {
		return fmt.Errorf("loading shard %s table: %w", shardID, err)
	}
	conf := table.Shards[shardID]
	if conf == nil {
		return fmt.Errorf("shard %s config missing", shardID)
	}
	start := &store.ShardStartConfig{ShardConfig: *conf}
	peers := h.storePeers()
	for _, storeID := range h.storeOrder {
		node := h.stores[storeID]
		if node == nil || node.store == nil {
			return fmt.Errorf("store %s not running", storeID)
		}
		if err := node.store.StartRaftGroup(shardID, peers, false, start); err != nil {
			return fmt.Errorf("starting shard %s on store %s: %w", shardID, storeID, err)
		}
	}
	return h.RefreshStatuses(context.Background())
}

func (h *Harness) Advance(d time.Duration) error {
	for d > 0 {
		step := min(d, 100*time.Millisecond)
		h.clock.Advance(step)
		h.yieldBackgroundWork()
		if err := h.pump(); err != nil {
			return err
		}
		h.network.Advance(step)
		if err := h.pump(); err != nil {
			return err
		}
		time.Sleep(time.Millisecond)
		d -= step
	}
	return nil
}

func (h *Harness) RefreshStatuses(ctx context.Context) error {
	statuses := make(map[types.ID]*tablemgr.StoreStatus, len(h.storeOrder))
	err := h.tableManager.RangeStoreStatuses(func(storeID types.ID, status *tablemgr.StoreStatus) bool {
		next := &tablemgr.StoreStatus{
			StoreInfo:   status.StoreInfo,
			State:       status.State,
			LastSeen:    status.LastSeen,
			Shards:      status.Shards,
			StoreClient: status.StoreClient,
		}
		storeStatus, statusErr := status.Status(ctx)
		if statusErr != nil {
			next.State = store.StoreState_Unhealthy
		} else {
			next.State = store.StoreState_Healthy
			next.LastSeen = h.clock.Now()
			next.Shards = storeStatus.Shards
		}
		statuses[storeID] = next
		return true
	})
	if err != nil {
		if isTransientMetadataUnavailable(err) {
			h.recordMetadataLeaderTransition()
			return nil
		}
		return fmt.Errorf("loading store statuses: %w", err)
	}
	if err := h.tableManager.UpdateStatuses(ctx, statuses); err != nil {
		if isTransientMetadataUnavailable(err) {
			h.recordMetadataLeaderTransition()
			return nil
		}
		return fmt.Errorf("updating store statuses: %w", err)
	}
	h.lastRefreshed = h.clock.Now()
	h.recordMetadataLeaderTransition()
	h.recordStatusTransitions()
	return nil
}

func isTransientMetadataUnavailable(err error) bool {
	return err != nil && strings.Contains(err.Error(), "metadata leader unavailable")
}

func (h *Harness) WaitForLeader(shardID types.ID, timeout time.Duration) (types.ID, error) {
	deadline := h.clock.Now().Add(timeout)
	for !h.clock.Now().After(deadline) {
		if err := h.RefreshStatuses(context.Background()); err != nil {
			return 0, err
		}
		if leaderID, ok := h.currentLeader(shardID); ok {
			return leaderID, nil
		}
		if err := h.Advance(200 * time.Millisecond); err != nil {
			return 0, err
		}
	}
	return 0, fmt.Errorf(
		"leader for shard %s not elected within %s (%s)",
		shardID,
		timeout,
		h.describeShardLeadership(shardID),
	)
}

func (h *Harness) Write(shardID types.ID, key string, value []byte) error {
	leaderID, err := h.WaitForLeader(shardID, 30*time.Second)
	if err != nil {
		return err
	}
	status, err := h.tableManager.GetStoreStatus(context.Background(), leaderID)
	if err != nil {
		return fmt.Errorf("loading leader store %s status: %w", leaderID, err)
	}
	err = h.runWithProgress(60*time.Second, func() error {
		return status.StoreClient.Batch(
			context.Background(),
			shardID,
			[][2][]byte{{[]byte(key), value}},
			nil,
			nil,
			db.Op_SyncLevelWrite,
		)
	})
	if err != nil {
		return fmt.Errorf(
			"writing key %q via leader %s: %w (%s)",
			key,
			leaderID,
			err,
			h.describeShardLeadership(shardID),
		)
	}
	h.expectedDocs[key] = bytes.Clone(value)
	h.recordEvent("write", "shard=%s leader=%s key=%q bytes=%d", shardID, leaderID, key, len(value))
	return nil
}

func (h *Harness) LookupFromStore(storeID, shardID types.ID, keys []string) (map[string][]byte, error) {
	status, err := h.tableManager.GetStoreStatus(context.Background(), storeID)
	if err != nil {
		return nil, fmt.Errorf("loading store %s status: %w", storeID, err)
	}
	results, err := status.StoreClient.Lookup(context.Background(), shardID, keys)
	if err != nil {
		return nil, fmt.Errorf("lookup on store %s: %w", storeID, err)
	}
	return results, nil
}

func (h *Harness) CrashStore(storeID types.ID) error {
	node := h.stores[storeID]
	if node == nil || node.store == nil {
		return fmt.Errorf("store %s is not running", storeID)
	}
	if err := h.router.Unregister(node.info.ApiURL); err != nil {
		return err
	}
	node.store.Close()
	node.store = nil
	node.errC = nil
	h.recordEvent("crash", "store=%s", storeID)
	return h.RefreshStatuses(context.Background())
}

func (h *Harness) CrashMetadataNode(nodeID types.ID) error {
	node := h.metadataNodes[nodeID]
	if node == nil || node.kv == nil {
		return fmt.Errorf("metadata node %s is not running", nodeID)
	}
	if err := h.router.Unregister(node.info.ApiURL); err != nil {
		return err
	}
	node.kv.Close()
	node.kv = nil
	node.node = nil
	node.tm = nil
	node.errC = nil
	h.recordEvent("metadata_crash", "node=%s", nodeID)
	h.recordMetadataLeaderTransition()
	return nil
}

func (h *Harness) RestartMetadataNode(nodeID types.ID) error {
	node := h.metadataNodes[nodeID]
	if node == nil {
		return fmt.Errorf("metadata node %s is not configured", nodeID)
	}
	if node.kv != nil {
		return fmt.Errorf("metadata node %s is already running", nodeID)
	}
	peers := make(common.Peers, 0, len(h.metadataOrder))
	for _, peerID := range h.metadataOrder {
		peers = append(peers, common.Peer{
			ID:  peerID,
			URL: fmt.Sprintf("raft://metadata-%d.local", peerID),
		})
	}
	if err := h.startMetadataNode(HarnessConfig{
		TxnIDGenerator:           h.txnIDGenerator,
		SplitFinalizeGracePeriod: h.splitFinalizeGracePeriod,
	}, nodeID, peers); err != nil {
		return err
	}
	h.applyStoreClientFactoryToMetadataNode(nodeID)
	h.recordEvent("metadata_restart", "node=%s", nodeID)
	return nil
}

func (h *Harness) PartitionMetadataNode(nodeID types.ID) {
	for _, otherID := range h.metadataOrder {
		if otherID == nodeID {
			continue
		}
		h.network.Partition(nodeID, otherID)
	}
	h.recordEvent("metadata_partition", "node=%s", nodeID)
}

func (h *Harness) HealMetadataNode(nodeID types.ID) {
	for _, otherID := range h.metadataOrder {
		if otherID == nodeID {
			continue
		}
		h.network.Heal(nodeID, otherID)
	}
	h.recordEvent("metadata_heal", "node=%s", nodeID)
}

func (h *Harness) RestartStore(storeID types.ID) error {
	node := h.stores[storeID]
	if node == nil {
		return fmt.Errorf("store %s is not configured", storeID)
	}
	if node.store != nil {
		return fmt.Errorf("store %s is already running", storeID)
	}
	return h.restartStore(storeID)
}

func (h *Harness) PartitionStore(storeID types.ID) {
	for _, otherID := range h.storeOrder {
		if otherID == storeID {
			continue
		}
		h.network.Partition(storeID, otherID)
	}
	h.recordEvent("partition", "store=%s", storeID)
}

func (h *Harness) HealStore(storeID types.ID) {
	for _, otherID := range h.storeOrder {
		if otherID == storeID {
			continue
		}
		h.network.Heal(storeID, otherID)
	}
	h.recordEvent("heal", "store=%s", storeID)
}

func (h *Harness) CheckConverged(shardID types.ID) error {
	return h.WaitForConvergence(shardID, 30*time.Second)
}

func (h *Harness) WaitForConvergence(shardID types.ID, timeout time.Duration) error {
	deadline := h.clock.Now().Add(timeout)
	var lastErr error
	for !h.clock.Now().After(deadline) {
		if err := h.checkConvergedNow(shardID); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if err := h.Advance(500 * time.Millisecond); err != nil {
			return err
		}
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("shard %s did not converge within %s", shardID, timeout)
}

func (h *Harness) checkConvergedNow(shardID types.ID) error {
	if err := h.RefreshStatuses(context.Background()); err != nil {
		return err
	}
	status, err := h.tableManager.GetShardStatus(shardID)
	if err != nil {
		return fmt.Errorf("loading shard %s status: %w", shardID, err)
	}
	if status.RaftStatus == nil || status.RaftStatus.Lead == 0 {
		return fmt.Errorf("shard %s has no elected leader", shardID)
	}

	keys := mapsKeys(h.expectedDocs)
	for _, storeID := range h.storeOrder {
		storeStatus, err := h.tableManager.GetStoreStatus(context.Background(), storeID)
		if err != nil {
			return fmt.Errorf("loading store %s status: %w", storeID, err)
		}
		if !storeStatus.IsReachable() {
			return fmt.Errorf("store %s is not healthy", storeID)
		}
		results, err := storeStatus.StoreClient.Lookup(context.Background(), shardID, keys)
		if err != nil {
			return fmt.Errorf("lookup on store %s: %w", storeID, err)
		}
		if len(results) != len(h.expectedDocs) {
			return fmt.Errorf("store %s returned %d docs, expected %d", storeID, len(results), len(h.expectedDocs))
		}
		for key, expected := range h.expectedDocs {
			actual, ok := results[key]
			if !ok {
				return fmt.Errorf("store %s missing key %q", storeID, key)
			}
			if !bytes.Equal(actual, expected) {
				return fmt.Errorf("store %s key %q mismatch", storeID, key)
			}
		}
	}
	return nil
}

func (h *Harness) startStore(storeID types.ID) error {
	info := &store.StoreInfo{
		ID:      storeID,
		RaftURL: fmt.Sprintf("raft://store-%d.local", storeID),
		ApiURL:  fmt.Sprintf("http://store-%d.local", storeID),
	}
	node := &storeNode{
		info:      info,
		transport: h.network.NewTransport(storeID),
	}

	storeInstance, errC, err := store.NewStoreWithOptions(
		h.logger,
		h.config,
		node.transport,
		info,
		nil,
		store.Options{
			Clock:      h.clock,
			HTTPClient: h.client,
		},
	)
	if err != nil {
		return fmt.Errorf("creating store %s: %w", storeID, err)
	}
	node.store = storeInstance
	node.errC = errC

	if err := h.router.Register(info.ApiURL, storeInstance.NewHttpAPI()); err != nil {
		storeInstance.Close()
		return fmt.Errorf("registering store route for %s: %w", storeID, err)
	}
	h.stores[storeID] = node

	if err := h.runWithProgress(simulatorControlPlaneTimeout, func() error {
		return h.tableManager.RegisterStore(context.Background(), &store.StoreRegistrationRequest{
			StoreInfo: *info,
		})
	}); err != nil {
		_ = h.router.Unregister(info.ApiURL)
		storeInstance.Close()
		node.store = nil
		node.errC = nil
		delete(h.stores, storeID)
		return fmt.Errorf("registering store %s in metadata: %w", storeID, err)
	}
	return nil
}

func (h *Harness) restartStore(storeID types.ID) error {
	node := h.stores[storeID]
	if node == nil {
		return fmt.Errorf("store %s is not configured", storeID)
	}
	storeInstance, errC, err := store.NewStoreWithOptions(
		h.logger,
		h.config,
		node.transport,
		node.info,
		nil,
		store.Options{
			Clock:      h.clock,
			HTTPClient: h.client,
		},
	)
	if err != nil {
		return fmt.Errorf("restarting store %s: %w", storeID, err)
	}
	if err := h.rehydrateStoreShards(storeInstance); err != nil {
		storeInstance.Close()
		return fmt.Errorf("rehydrating store %s shards: %w", storeID, err)
	}
	if err := h.router.Register(node.info.ApiURL, storeInstance.NewHttpAPI()); err != nil {
		storeInstance.Close()
		return fmt.Errorf("registering restarted store route for %s: %w", storeID, err)
	}
	node.store = storeInstance
	node.errC = errC
	h.recordEvent("restart", "store=%s", storeID)
	return h.RefreshStatuses(context.Background())
}

func (h *Harness) currentLeader(shardID types.ID) (types.ID, bool) {
	leaderVotes := make(map[types.ID]int)
	healthyStores := 0
	for _, storeID := range h.storeOrder {
		storeStatus, err := h.tableManager.GetStoreStatus(context.Background(), storeID)
		if err != nil || storeStatus == nil || !storeStatus.IsReachable() {
			continue
		}
		healthyStores++
		shardStatus := storeStatus.Shards[shardID]
		if shardStatus == nil || shardStatus.RaftStatus == nil {
			continue
		}
		if shardStatus.RaftStatus.Lead != 0 {
			leaderVotes[shardStatus.RaftStatus.Lead]++
		}
	}
	quorum := healthyStores/2 + 1
	for leaderID, votes := range leaderVotes {
		if votes >= quorum {
			leaderStatus, err := h.tableManager.GetStoreStatus(context.Background(), leaderID)
			if err == nil && leaderStatus != nil && leaderStatus.IsReachable() {
				return leaderID, true
			}
		}
	}
	return 0, false
}

func (h *Harness) pump() error {
	for range 64 {
		runtime.Gosched()
		delivered := h.network.DeliverReady()
		if err := h.checkMetadataErrors(); err != nil {
			return err
		}
		if err := h.checkStoreErrors(); err != nil {
			return err
		}
		if delivered == 0 {
			if h.network.Pending() == 0 {
				return nil
			}
			runtime.Gosched()
		}
	}
	if err := h.checkMetadataErrors(); err != nil {
		return err
	}
	return h.checkStoreErrors()
}

func (h *Harness) yieldBackgroundWork() {
	for range 8 {
		runtime.Gosched()
	}
}

func (h *Harness) checkStoreErrors() error {
	for _, storeID := range h.storeOrder {
		node := h.stores[storeID]
		if node == nil || node.errC == nil {
			continue
		}
		for {
			select {
			case err, ok := <-node.errC:
				if !ok {
					node.errC = nil
					break
				}
				if err != nil {
					if strings.Contains(err.Error(), "raft: stopped") {
						continue
					}
					return fmt.Errorf("store %s reported raft error: %w", storeID, err)
				}
			default:
				goto nextStore
			}
		}
	nextStore:
	}
	return nil
}

func (h *Harness) checkMetadataErrors() error {
	for _, nodeID := range h.metadataOrder {
		node := h.metadataNodes[nodeID]
		if node == nil || node.errC == nil {
			continue
		}
		for {
			select {
			case err, ok := <-node.errC:
				if !ok {
					node.errC = nil
					break
				}
				if err != nil {
					if strings.Contains(err.Error(), "raft: stopped") {
						continue
					}
					return fmt.Errorf("metadata node %s reported raft error: %w", nodeID, err)
				}
			default:
				goto nextMetadataNode
			}
		}
	nextMetadataNode:
	}
	return nil
}

func (h *Harness) runWithProgress(timeout time.Duration, fn func() error) error {
	done := make(chan error, 1)
	go func() {
		done <- fn()
	}()

	deadline := h.clock.Now().Add(timeout)
	for !h.clock.Now().After(deadline) {
		select {
		case err := <-done:
			return err
		default:
		}
		if err := h.Advance(100 * time.Millisecond); err != nil {
			return err
		}
	}

	select {
	case err := <-done:
		return err
	default:
		return fmt.Errorf("operation did not complete within %s", timeout)
	}
}

func (h *Harness) storePeers() common.Peers {
	peers := make(common.Peers, 0, len(h.storeOrder))
	for _, storeID := range h.storeOrder {
		node := h.stores[storeID]
		peers = append(peers, common.Peer{
			ID:  storeID,
			URL: node.info.RaftURL,
		})
	}
	return peers
}

func mapsKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func (h *Harness) rehydrateStoreShards(storeInstance *store.Store) error {
	currentStatus := storeInstance.Status()
	shardStatuses, err := h.tableManager.GetShardStatuses()
	if err != nil {
		return fmt.Errorf("loading shard statuses: %w", err)
	}
	for shardID, shardStatus := range shardStatuses {
		if _, ok := currentStatus.Shards[shardID]; ok {
			continue
		}
		start := &store.ShardStartConfig{ShardConfig: shardStatus.ShardConfig}
		if err := storeInstance.StartRaftGroup(shardID, h.storePeers(), true, start); err != nil {
			return fmt.Errorf("starting shard %s during rehydrate: %w", shardID, err)
		}
	}
	return nil
}

func (h *Harness) describeShardLeadership(shardID types.ID) string {
	parts := make([]string, 0, len(h.storeOrder))
	for _, storeID := range h.storeOrder {
		storeStatus, err := h.tableManager.GetStoreStatus(context.Background(), storeID)
		if err != nil {
			parts = append(parts, fmt.Sprintf("%s=err:%v", storeID, err))
			continue
		}
		shardStatus := storeStatus.Shards[shardID]
		if shardStatus == nil || shardStatus.RaftStatus == nil {
			parts = append(parts, fmt.Sprintf("%s=healthy:%t lead:-", storeID, storeStatus.IsReachable()))
			continue
		}
		parts = append(parts, fmt.Sprintf(
			"%s=healthy:%t lead:%s voters:%d",
			storeID,
			storeStatus.IsReachable(),
			shardStatus.RaftStatus.Lead,
			len(shardStatus.RaftStatus.Voters),
		))
	}
	return strings.Join(parts, ", ")
}

func (h *Harness) TrackExpectedDoc(tableName, key string, value []byte) {
	h.expectedKeys[key] = tableName
	h.expectedDocs[key] = bytes.Clone(value)
}

func (h *Harness) recordEvent(kind, format string, args ...any) {
	if h.tracer != nil {
		h.tracer.RecordEvent(h.clock.Now(), kind, format, args...)
	}
}

func (h *Harness) RecordDigest(note string) error {
	snapshot, err := h.Snapshot(context.Background())
	if err != nil {
		return err
	}
	return h.tracer.RecordDigest(snapshot, note)
}

func (h *Harness) recordStatusTransitions() {
	shards, err := h.tableManager.GetShardStatuses()
	if err != nil {
		return
	}
	nextLeaders := make(map[types.ID]types.ID, len(shards))
	nextSplits := make(map[types.ID]string, len(shards))
	shardIDs := make([]types.ID, 0, len(shards))
	for shardID := range shards {
		shardIDs = append(shardIDs, shardID)
	}
	slices.Sort(shardIDs)
	for _, shardID := range shardIDs {
		status := shards[shardID]
		leaderID := types.ID(0)
		if status.RaftStatus != nil {
			leaderID = types.ID(status.RaftStatus.Lead)
		}
		nextLeaders[shardID] = leaderID
		if prevLeader, ok := h.lastLeaders[shardID]; !ok || prevLeader != leaderID {
			h.recordEvent("leader", "shard=%s leader=%s prev=%s", shardID, leaderID, prevLeader)
		}

		splitSig := status.State.String()
		if status.SplitState != nil {
			splitSig += "|" + status.SplitState.GetPhase().String()
		}
		if flags := splitFlags(status); flags != "" {
			splitSig += "|" + flags
		}
		nextSplits[shardID] = splitSig
		prevSplit := h.lastSplits[shardID]
		if prevSplit != splitSig && (status.SplitState != nil ||
			status.State.Transitioning() ||
			status.State == store.ShardState_SplitOffPreSnap ||
			prevSplit != "" && prevSplit != store.ShardState_Default.String()) {
			h.recordEvent("split", "shard=%s state=%s prev=%s", shardID, splitSig, prevSplit)
		}
	}
	h.lastLeaders = nextLeaders
	h.lastSplits = nextSplits
}

func (h *Harness) recordMetadataLeaderTransition() {
	leaderID, ok := h.currentMetadataLeader()
	if !ok {
		leaderID = 0
	}
	if h.lastMetaLeader != leaderID {
		h.recordEvent("metadata_leader", "leader=%s prev=%s", leaderID, h.lastMetaLeader)
		h.lastMetaLeader = leaderID
	}
}
