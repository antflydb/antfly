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
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/workerpool"

	"github.com/antflydb/antfly/lib/middleware"
	"github.com/antflydb/antfly/lib/multirafthttp/transport"
	"github.com/antflydb/antfly/lib/pebbleutils"
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
	"golang.org/x/sync/errgroup"
)

// RunAsMetadataServer implements a leader node that monitors and manages the cluster
// The readyC channel will be closed when all HTTP servers are ready to accept connections
func RunAsMetadataServer(
	ctx context.Context,
	zl *zap.Logger,
	config *common.Config,
	conf *store.StoreInfo,
	peers common.Peers,
	join bool,
	readyC chan<- struct{},
	cache *pebbleutils.Cache,
) {
	zl = zl.Named("metadataServer")
	zl.Info("Starting metadata node",
		zap.Stringer("peers", peers),
		zap.Any("config", config),
		zap.Any("storeInfo", conf))

	u, err := url.Parse(conf.ApiURL)
	if err != nil {
		zl.Fatal("Error parsing API URL", zap.Error(err))
	}

	dataDir := config.GetBaseDir()
	rs, err := raft.NewMultiRaftServer(zl, dataDir, conf.ID, conf.RaftURL)
	if err != nil {
		zl.Fatal("Failed to create Raft server", zap.Error(err))
	}
	go rs.Start()
	defer func() {
		if err := rs.Stop(); err != nil {
			zl.Error("failed to stop raft server", zap.Error(err))
		}
	}()

	metadataStore, err := kv.NewMetadataStore(zl, config, rs, conf, peers, join, cache)
	if err != nil {
		zl.Fatal("Failed to create store", zap.Error(err))
	}
	defer metadataStore.Close()

	dialer := &net.Dialer{
		Timeout:   30 * time.Second, // Set a reasonable timeout for dialing
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
	if config.Tls.Cert != "" && config.Tls.Key != "" {
		tlsInfo := transport.TLSInfoSimple{
			CertFile: config.Tls.Cert,
			KeyFile:  config.Tls.Key,
		}
		tr := &http3.Transport{
			TLSClientConfig: tlsInfo.ClientConfig(), // set a TLS client config, if desired
			QUICConfig: &quic.Config{
				HandshakeIdleTimeout: 10 * time.Second,
				MaxIdleTimeout:       10 * time.Second,
				KeepAlivePeriod:      5 * time.Second,
			}, // QUIC connection options
		}
		defer func() {
			if err := tr.Close(); err != nil {
				zl.Error("failed to close transport", zap.Error(err))
			}
		}()
		t.RegisterProtocol("https", tr)
	}
	client := &http.Client{
		// This is a long timeout to allow for long-running operations like backup
		Timeout:   time.Second * 540,
		Transport: t,
	}
	tm, err := tablemgr.NewTableManager(metadataStore, client, config.MaxShardSizeBytes)
	if err != nil {
		zl.Fatal("Error creating table manager", zap.Error(err))
	}
	um, err := usermgr.NewUserManager(metadataStore)
	if err != nil {
		zl.Fatal("Error creating table manager", zap.Error(err))
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
	defer embeddingCache.Stop()
	pool, err := workerpool.NewPool()
	if err != nil {
		zl.Fatal("Failed to create worker pool", zap.Error(err))
	}
	defer pool.Close()

	ln := &MetadataStore{
		logger: zl,

		metadataStore: metadataStore,
		config:        config,
		tm:            tm,
		um:            um,
		pool:          pool,

		embeddingCache: embeddingCache,

		runHealthCheckC:  make(chan struct{}, 1),
		reconcileShardsC: make(chan struct{}, 1),

		hlc: NewHLC(),
	}

	// Initialize reconciler with adapters
	shardOps := NewMetadataShardOperations(ln)
	tableOps := NewMetadataTableOperations(ln)
	storeOps := NewMetadataStoreOperations(ln)

	reconcilerConfig := reconciler.ReconciliationConfig{
		ReplicationFactor:   config.ReplicationFactor,
		MaxShardSizeBytes:   config.MaxShardSizeBytes,
		MaxShardsPerTable:   config.MaxShardsPerTable,
		DisableShardAlloc:   config.DisableShardAlloc,
		ShardCooldownPeriod: config.ShardCooldownPeriod,
		SplitTimeout:        config.SplitTimeout,
	}

	ln.reconciler = reconciler.NewReconciler(
		shardOps,
		tableOps,
		storeOps,
		reconcilerConfig,
		zl,
	)

	ctx, cancel := context.WithCancel(ctx)
	eg, egCtx := errgroup.WithContext(ctx)
	defer cancel()

	// Create CDC replication manager (before reconciler so listeners can reference it)
	replMgr := foreign.NewReplicationManager(zl.Named("cdc"), ln, metadataStore, tm)

	reconciler := func(ctx context.Context) error {
		// Register key pattern listeners for metadata changes
		// These will trigger reconciliation when specific keys are modified

		// 1. Table metadata listener (includes index changes)
		// Pattern: "tm:t:{tableName}" matches keys like "tm:t:users", "tm:t:products"
		// Note: Index changes trigger table re-saves since indexes are part of the table structure
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

				// Trigger reconciliation
				select {
				case ln.reconcileShardsC <- struct{}{}:
				default:
					// Already queued
				}

				// Notify CDC manager to check for new replication sources
				replMgr.NotifyTableChanged()

				return nil
			},
		); err != nil {
			zl.Error("Failed to register table pattern listener", zap.Error(err))
		}

		// 2. Shard status listener with parameter extraction
		// Pattern: "tm:shs:{shardID}" extracts the shard ID
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

				// Trigger reconciliation for shard status changes
				select {
				case ln.reconcileShardsC <- struct{}{}:
				default:
					// Already queued
				}
				return nil
			},
		); err != nil {
			zl.Error("Failed to register shard status pattern listener", zap.Error(err))
		}

		// 3. Store status listener with parameter extraction
		// Pattern: "tm:sts:{storeID}" extracts the store/node ID
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

				// Trigger reconciliation for store status changes
				select {
				case ln.reconcileShardsC <- struct{}{}:
				default:
					// Already queued
				}
				return nil
			},
		); err != nil {
			zl.Error("Failed to register store status pattern listener", zap.Error(err))
		}

		// 4. Store tombstone listener (dead/removed stores)
		// Pattern: "tm:stb:{storeID}" for tombstoned stores
		if err := metadataStore.RegisterKeyPattern(
			"tm:stb:{storeID}",
			func(ctx context.Context, key []byte, value []byte, isDelete bool, params map[string]string) error {
				storeID := params["storeID"]
				if !isDelete {
					zl.Warn("Store tombstoned, triggering immediate reconciliation",
						zap.String("storeID", storeID))

					// Immediate reconciliation for store removal
					select {
					case ln.reconcileShardsC <- struct{}{}:
					default:
						// Already queued
					}
				}
				return nil
			},
		); err != nil {
			zl.Error("Failed to register store tombstone pattern listener", zap.Error(err))
		}

		// 5. Table reallocation request listener
		// Prefix: "tm:rar:" for manual reallocation requests
		metadataStore.RegisterKeyPrefixListener(
			[]byte("tm:rar:"),
			func(ctx context.Context, key, value []byte, isDelete bool) error {
				if !isDelete {
					zl.Info("Manual table reallocation requested, triggering immediate reconciliation",
						zap.String("key", types.FormatKey(key)))

					// Trigger reconciliation immediately for reallocation requests
					select {
					case ln.reconcileShardsC <- struct{}{}:
					default:
						// Already queued
					}
				}
				return nil
			},
		)

		// Start reconciliation loop
		go ln.reconcileShards(ctx)

		// Start CDC replication workers for tables with replication sources
		go func() {
			if err := replMgr.Run(ctx); err != nil && ctx.Err() == nil {
				zl.Error("CDC replication manager exited with error", zap.Error(err))
			}
		}()

		return nil
	}
	metadataStore.SetLeaderFactory(reconciler)

	// Combined API server replaces both internal and public API servers
	eg.Go(func() error {
		// Create internal API routes
		internalMux := http.NewServeMux()

		// Store registration endpoints
		internalMux.HandleFunc("POST /store", ln.handleStoreRegistration)
		internalMux.HandleFunc("DELETE /store/{store}", ln.handleStoreDeregistration)

		// Transaction coordination endpoints (for cross-shard notifications)
		internalMux.HandleFunc("POST /shard/{shardID}/txn/resolve", ln.handleForwardResolveIntent)

		// Metadata store API endpoints (peer management, batch operations)
		api := kv.NewMetadataStoreAPI(zl, metadataStore)
		api.AddRoutes(internalMux)

		// Debug/admin endpoints (moved from public API)
		internalMux.HandleFunc("POST /reallocate", ln.handleReallocateShards)

		// Public API routes
		zl.Debug("Setting up public API routes")
		publicMux := ln.publicApiRoutes()

		// Combined router
		apiRoutes := http.NewServeMux()
		zl.Debug("Registering public API routes", zap.String("pattern", "/api/v1/"), zap.String("prefix_strip", "/api/v1"))
		apiRoutes.Handle("/api/v1/", http.StripPrefix("/api/v1", publicMux))
		zl.Debug("Registering internal API routes", zap.String("pattern", "/_internal/v1/"), zap.String("prefix_strip", "/_internal/v1"))
		apiRoutes.Handle("/_internal/v1/", http.StripPrefix("/_internal/v1", internalMux))
		addAntfarmRoutes(apiRoutes)

		// Reverse proxies for the Antfarm dashboard
		registryURL := config.RegistryUrl
		if registryURL == "" {
			registryURL = modelregistry.DefaultRegistryURL
		}
		addRegistryProxy(apiRoutes, registryURL)

		if config.Termite.ApiUrl != "" {
			addTermiteProxy(apiRoutes, config.Termite.ApiUrl)
		}

		// Mount MCP server
		mcpAdapter := newMCPAdapter(NewTableApi(zl, ln, tm))
		mcpServer := antflymcp.NewMCPServer(mcpAdapter)
		mcpHandler := antflymcp.NewMCPHandler(mcpServer)
		apiRoutes.Handle("/mcp/v1/", http.StripPrefix("/mcp/v1", mcpHandler))
		zl.Info("MCP server mounted", zap.String("path", "/mcp/v1/"))

		// Mount A2A facade
		a2aRoutes := mountA2ARoutes(zl, ln, tm, conf.ApiURL)
		apiRoutes.Handle("/a2a", a2aRoutes.jsonrpcHandler)
		apiRoutes.Handle("/.well-known/agent.json", a2aRoutes.cardHandler)
		zl.Info("A2A facade mounted", zap.String("path", "/a2a"))

		// Wrap with helpful 404 handler for typo suggestions
		apiRoutesWithNotFound := newNotFoundHandler(apiRoutes, zl)

		// Single HTTP server with CORS middleware
		srv := http.Server{
			Addr:        u.Host,
			Handler:     middleware.CORSMiddlewareWithConfig(apiRoutesWithNotFound, &config.Cors),
			ReadTimeout: 10 * time.Second,
		}

		// Graceful shutdown
		go func() {
			<-egCtx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdownCtx)
		}()

		zl.Info("Combined API server starting", zap.String("address", u.Host))

		// Create listener first so we know when we're ready
		listener, err := net.Listen("tcp", u.Host)
		if err != nil {
			return fmt.Errorf("failed to create listener: %w", err)
		}

		// Signal that API server is ready
		if readyC != nil {
			close(readyC)
			zl.Info("API server is ready")
		}

		// Serve on single port
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			zl.Error("HTTP server error", zap.Error(err))
			return fmt.Errorf("starting HTTP server: %w", err)
		} else if errors.Is(err, http.ErrServerClosed) {
			zl.Info("HTTP server closed gracefully")
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		zl.Fatal("HTTP server error", zap.Error(err))
	}
	zl.Info("HTTP server stopped")
}
