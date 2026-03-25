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
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/antflydb/antfly/lib/clock"
	"github.com/antflydb/antfly/lib/middleware"
	"github.com/antflydb/antfly/lib/multirafthttp/transport"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/workerpool"
	"github.com/antflydb/antfly/src/common"
	antflymcp "github.com/antflydb/antfly/src/mcp"
	"github.com/antflydb/antfly/src/metadata/foreign"
	"github.com/antflydb/antfly/src/metadata/kv"
	"github.com/antflydb/antfly/src/metadata/reconciler"
	"github.com/antflydb/antfly/src/raft"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/tablemgr"
	"github.com/antflydb/antfly/src/usermgr"
	"github.com/antflydb/termite/pkg/termite/lib/modelregistry"
	"github.com/jellydator/ttlcache/v3"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"go.uber.org/zap"
)

// Runtime owns the metadata server's long-lived state without opening any
// listeners. Bootstrap layers can start Raft and serve the returned handler.
type Runtime struct {
	logger *zap.Logger
	config *common.Config
	info   *store.StoreInfo

	raft          *raft.MultiRaft
	metadataStore *kv.MetadataStore
	node          *MetadataStore
	tableManager  *tablemgr.TableManager
	userManager   *usermgr.UserManager

	httpClient  *http.Client
	httpCloser  io.Closer
	workerPool  *workerpool.Pool
	embedCache  *ttlcache.Cache[string, []float32]
	httpHandler http.Handler

	closeOnce sync.Once
	closeErr  error
}

func NewRuntime(
	zl *zap.Logger,
	config *common.Config,
	conf *store.StoreInfo,
	peers common.Peers,
	join bool,
	cache *pebbleutils.Cache,
) (*Runtime, error) {
	dataDir := config.GetBaseDir()
	rs, err := raft.NewMultiRaftServer(zl, dataDir, conf.ID, conf.RaftURL)
	if err != nil {
		return nil, fmt.Errorf("creating raft server: %w", err)
	}

	metadataStore, err := kv.NewMetadataStore(zl, config, rs, conf, peers, join, cache)
	if err != nil {
		return nil, fmt.Errorf("creating metadata store: %w", err)
	}

	httpClient, httpCloser, err := newStoreHTTPClient(config)
	if err != nil {
		metadataStore.Close()
		return nil, fmt.Errorf("creating metadata http client: %w", err)
	}

	tm, err := tablemgr.NewTableManager(metadataStore, httpClient, config.MaxShardSizeBytes)
	if err != nil {
		metadataStore.Close()
		if httpCloser != nil {
			_ = httpCloser.Close()
		}
		return nil, fmt.Errorf("creating table manager: %w", err)
	}

	um, err := usermgr.NewUserManager(metadataStore)
	if err != nil {
		metadataStore.Close()
		if httpCloser != nil {
			_ = httpCloser.Close()
		}
		return nil, fmt.Errorf("creating user manager: %w", err)
	}
	if adminUser, _ := um.GetUser("admin"); adminUser == nil {
		if _, err := um.CreateUser("admin", "admin", []usermgr.Permission{{
			Resource: "*", ResourceType: "*", Type: "*",
		}}); err != nil {
			zl.Warn("Error creating admin user", zap.Error(err))
		}
	}

	embeddingCache := ttlcache.New(
		ttlcache.WithTTL[string, []float32](5*time.Minute),
		ttlcache.WithCapacity[string, []float32](10000),
	)
	go embeddingCache.Start()

	pool, err := workerpool.NewPool()
	if err != nil {
		embeddingCache.Stop()
		metadataStore.Close()
		if httpCloser != nil {
			_ = httpCloser.Close()
		}
		return nil, fmt.Errorf("creating worker pool: %w", err)
	}

	node := &MetadataStore{
		logger: zl,

		metadataStore: metadataStore,
		config:        config,
		tm:            tm,
		um:            um,
		pool:          pool,

		embeddingCache: embeddingCache,

		runHealthCheckC:  make(chan struct{}, 1),
		reconcileShardsC: make(chan struct{}, 1),

		hlc:   NewHLCWithClock(clock.RealClock{}),
		clock: clock.RealClock{},
	}

	shardOps := NewMetadataShardOperations(node)
	tableOps := NewMetadataTableOperations(node)
	storeOps := NewMetadataStoreOperations(node)

	reconcilerConfig := reconciler.ReconciliationConfig{
		ReplicationFactor:        config.ReplicationFactor,
		MaxShardSizeBytes:        config.MaxShardSizeBytes,
		MinShardSizeBytes:        config.MinShardSizeBytes,
		MinShardsPerTable:        config.MinShardsPerTable,
		MaxShardsPerTable:        config.MaxShardsPerTable,
		DisableShardAlloc:        config.DisableShardAlloc,
		ShardCooldownPeriod:      config.ShardCooldownPeriod,
		SplitTimeout:             config.SplitTimeout,
		SplitFinalizeGracePeriod: config.SplitFinalizeGracePeriod,
	}

	node.reconciler = reconciler.NewReconciler(
		shardOps,
		tableOps,
		storeOps,
		reconcilerConfig,
		zl,
	)

	replMgr := foreign.NewReplicationManager(zl.Named("cdc"), node, metadataStore, tm)
	metadataStore.SetLeaderFactory(newMetadataLeaderFactory(zl, node, metadataStore, replMgr))

	runtime := &Runtime{
		logger:        zl,
		config:        config,
		info:          conf,
		raft:          rs,
		metadataStore: metadataStore,
		node:          node,
		tableManager:  tm,
		userManager:   um,
		httpClient:    httpClient,
		httpCloser:    httpCloser,
		workerPool:    pool,
		embedCache:    embeddingCache,
	}
	runtime.httpHandler = runtime.newHTTPHandler()
	return runtime, nil
}

func (r *Runtime) StartRaft() {
	go r.raft.Start()
}

func (r *Runtime) HTTPHandler() http.Handler {
	return r.httpHandler
}

func (r *Runtime) Node() *MetadataStore {
	return r.node
}

func (r *Runtime) Close() error {
	r.closeOnce.Do(func() {
		if r.metadataStore != nil {
			r.metadataStore.Close()
		}
		if r.raft != nil {
			if err := r.raft.Stop(); err != nil {
				if r.closeErr == nil {
					r.closeErr = err
				}
				r.logger.Error("failed to stop raft server", zap.Error(err))
			}
		}
		if r.embedCache != nil {
			r.embedCache.Stop()
		}
		if r.workerPool != nil {
			r.workerPool.Close()
		}
		if r.httpCloser != nil {
			if err := r.httpCloser.Close(); err != nil && r.closeErr == nil {
				r.closeErr = err
			}
		}
	})
	return r.closeErr
}

func newStoreHTTPClient(config *common.Config) (*http.Client, io.Closer, error) {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	t := &http.Transport{
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		DisableKeepAlives:     false,
		ForceAttemptHTTP2:     true,
		ResponseHeaderTimeout: 5 * time.Minute,
		IdleConnTimeout:       5 * time.Minute,
		DialContext:           dialer.DialContext,
	}

	var closer io.Closer
	if config.Tls.Cert != "" && config.Tls.Key != "" {
		tlsInfo := transport.TLSInfoSimple{
			CertFile: config.Tls.Cert,
			KeyFile:  config.Tls.Key,
		}
		tr := &http3.Transport{
			TLSClientConfig: tlsInfo.ClientConfig(),
			QUICConfig: &quic.Config{
				HandshakeIdleTimeout: 10 * time.Second,
				MaxIdleTimeout:       10 * time.Second,
				KeepAlivePeriod:      5 * time.Second,
			},
		}
		t.RegisterProtocol("https", tr)
		closer = tr
	}

	return &http.Client{
		Timeout:   time.Second * 540,
		Transport: t,
	}, closer, nil
}

func newMetadataLeaderFactory(
	zl *zap.Logger,
	ln *MetadataStore,
	metadataStore *kv.MetadataStore,
	replMgr *foreign.ReplicationManager,
) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		if err := metadataStore.RegisterKeyPattern(
			"tm:t:{tableName}",
			func(ctx context.Context, key []byte, value []byte, isDelete bool, params map[string]string) error {
				tableName := params["tableName"]
				if isDelete {
					zl.Info("Table deleted, triggering reconciliation",
						zap.String("tableName", tableName))
				} else {
					zl.Info("Table modified (schema/indexes/shards), triggering reconciliation",
						zap.String("tableName", tableName))
				}

				select {
				case ln.reconcileShardsC <- struct{}{}:
				default:
				}

				replMgr.NotifyTableChanged()
				return nil
			},
		); err != nil {
			zl.Error("Failed to register table pattern listener", zap.Error(err))
		}

		if err := metadataStore.RegisterKeyPattern(
			"tm:shs:{shardID}",
			func(ctx context.Context, key []byte, value []byte, isDelete bool, params map[string]string) error {
				shardID := params["shardID"]
				if isDelete {
					zl.Info("Shard status deleted, triggering reconciliation",
						zap.String("shardID", shardID))
				} else {
					zl.Debug("Shard status updated, triggering reconciliation",
						zap.String("shardID", shardID))
				}

				select {
				case ln.reconcileShardsC <- struct{}{}:
				default:
				}
				return nil
			},
		); err != nil {
			zl.Error("Failed to register shard status pattern listener", zap.Error(err))
		}

		if err := metadataStore.RegisterKeyPattern(
			"tm:sts:{storeID}",
			func(ctx context.Context, key []byte, value []byte, isDelete bool, params map[string]string) error {
				storeID := params["storeID"]
				if isDelete {
					zl.Info("Store status deleted, triggering reconciliation",
						zap.String("storeID", storeID))
				} else {
					zl.Debug("Store status updated",
						zap.String("storeID", storeID))
				}

				select {
				case ln.reconcileShardsC <- struct{}{}:
				default:
				}
				return nil
			},
		); err != nil {
			zl.Error("Failed to register store status pattern listener", zap.Error(err))
		}

		if err := metadataStore.RegisterKeyPattern(
			"tm:stb:{storeID}",
			func(ctx context.Context, key []byte, value []byte, isDelete bool, params map[string]string) error {
				storeID := params["storeID"]
				if !isDelete {
					zl.Warn("Store tombstoned, triggering immediate reconciliation",
						zap.String("storeID", storeID))

					select {
					case ln.reconcileShardsC <- struct{}{}:
					default:
					}
				}
				return nil
			},
		); err != nil {
			zl.Error("Failed to register store tombstone pattern listener", zap.Error(err))
		}

		metadataStore.RegisterKeyPrefixListener(
			[]byte("tm:rar:"),
			func(ctx context.Context, key, value []byte, isDelete bool) error {
				if !isDelete {
					zl.Info("Manual table reallocation requested, triggering immediate reconciliation",
						zap.String("key", types.FormatKey(key)))
					select {
					case ln.reconcileShardsC <- struct{}{}:
					default:
					}
				}
				return nil
			},
		)

		go ln.reconcileShards(ctx)
		go func() {
			if err := replMgr.Run(ctx); err != nil && ctx.Err() == nil {
				zl.Error("CDC replication manager exited with error", zap.Error(err))
			}
		}()

		return nil
	}
}

func (r *Runtime) newHTTPHandler() http.Handler {
	internalMux := http.NewServeMux()
	internalMux.HandleFunc("POST /store", r.node.handleStoreRegistration)
	internalMux.HandleFunc("DELETE /store/{store}", r.node.handleStoreDeregistration)
	internalMux.HandleFunc("POST /shard/{shardID}/txn/resolve", r.node.handleForwardResolveIntent)

	api := kv.NewMetadataStoreAPI(r.logger, r.metadataStore)
	api.AddRoutes(internalMux)
	internalMux.HandleFunc("POST /reallocate", r.node.handleReallocateShards)

	publicMux := r.node.publicApiRoutes()
	apiRoutes := http.NewServeMux()
	apiRoutes.Handle("/api/v1/", http.StripPrefix("/api/v1", publicMux))
	apiRoutes.Handle("/_internal/v1/", http.StripPrefix("/_internal/v1", internalMux))
	addAntfarmRoutes(apiRoutes)

	registryURL := r.config.RegistryUrl
	if registryURL == "" {
		registryURL = modelregistry.DefaultRegistryURL
	}
	addRegistryProxy(apiRoutes, registryURL)

	if r.config.Termite.ApiUrl != "" {
		addTermiteProxy(apiRoutes, r.config.Termite.ApiUrl)
	}

	mcpAdapter := newMCPAdapter(NewTableApi(r.logger, r.node, r.tableManager))
	mcpServer := antflymcp.NewMCPServer(mcpAdapter)
	mcpHandler := antflymcp.NewMCPHandler(mcpServer)
	apiRoutes.Handle("/mcp/v1/", http.StripPrefix("/mcp/v1", mcpHandler))
	r.logger.Info("MCP server mounted", zap.String("path", "/mcp/v1/"))

	a2aRoutes := mountA2ARoutes(r.logger, r.node, r.tableManager, r.info.ApiURL)
	apiRoutes.Handle("/a2a", a2aRoutes.jsonrpcHandler)
	apiRoutes.Handle("/.well-known/agent.json", a2aRoutes.cardHandler)
	r.logger.Info("A2A facade mounted", zap.String("path", "/a2a"))

	return middleware.CORSMiddlewareWithConfig(
		newNotFoundHandler(apiRoutes, r.logger),
		&r.config.Cors,
	)
}
