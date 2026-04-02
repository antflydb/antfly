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

package e2e

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/types"
	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/metadata"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/termite/pkg/termite"
	"go.uber.org/zap"
)

// SwarmInstance represents a running single-node Antfly swarm for testing.
// It includes a metadata server, store server, and Termite ML server.
type SwarmInstance struct {
	T               *testing.T
	Logger          *zap.Logger
	Config          *common.Config
	NodeID          types.ID
	MetadataAPIURL  string
	MetadataRaftURL string
	StoreAPIURL     string
	StoreRaftURL    string
	Client          *antfly.AntflyClient
	Cancel          context.CancelFunc
	DataDir         string
}

// Cleanup stops the swarm and cleans up resources
func (s *SwarmInstance) Cleanup() {
	s.Cancel()
	// Give servers time to shutdown gracefully
	time.Sleep(500 * time.Millisecond)
}

// SwarmOptions configures optional behaviour for startAntflySwarmWithOptions.
type SwarmOptions struct {
	// DisableTermite skips starting the Termite ML server. Useful for tests
	// that don't need embeddings/chunking/reranking.
	DisableTermite bool

	// EnableAuth enables authentication and authorization. When enabled, a
	// default admin:admin user with full permissions is auto-created at startup.
	EnableAuth bool

	// LocalBypass enables in-process communication between metadata and store,
	// bypassing HTTP for shard queries and StoreRPC. Mirrors swarm mode behavior.
	LocalBypass bool
}

// startAntflySwarm starts a full Antfly swarm (metadata + store + Termite) for testing.
func startAntflySwarm(t *testing.T, ctx context.Context) *SwarmInstance {
	return startAntflySwarmWithOptions(t, ctx, SwarmOptions{})
}

// startAntflySwarmWithOptions starts an Antfly swarm with configurable options.
func startAntflySwarmWithOptions(t *testing.T, ctx context.Context, opts SwarmOptions) *SwarmInstance {
	t.Helper()

	// Reduce Pebble cache size for tests to avoid OOM (16MB vs default 320MB per shard)
	db.DefaultPebbleCacheSizeMB = 16

	logger := GetTestLogger(t)
	nodeID := types.ID(1)
	dataDir := t.TempDir()

	// Allocate dynamic ports
	metadataAPIPort := GetFreePort(t)
	metadataRaftPort := GetFreePort(t)
	storeAPIPort := GetFreePort(t)
	storeRaftPort := GetFreePort(t)

	metadataAPIURL := fmt.Sprintf("http://localhost:%d", metadataAPIPort)
	metadataRaftURL := fmt.Sprintf("http://localhost:%d", metadataRaftPort)
	storeAPIURL := fmt.Sprintf("http://localhost:%d", storeAPIPort)
	storeRaftURL := fmt.Sprintf("http://localhost:%d", storeRaftPort)

	// Create config
	config := CreateTestConfig(t, dataDir, nodeID)
	config.EnableAuth = opts.EnableAuth
	config.Metadata.OrchestrationUrls = map[string]string{
		nodeID.String(): metadataAPIURL,
	}

	if !opts.DisableTermite {
		termiteAPIPort := GetFreePort(t)
		termiteAPIURL := fmt.Sprintf("http://localhost:%d", termiteAPIPort)

		// Configure Termite for chunking and reranking
		repoRoot := findRepoRoot(t)
		modelsDir := filepath.Join(repoRoot, "models")

		config.Termite = termite.Config{
			ApiUrl:          termiteAPIURL,
			ModelsDir:       modelsDir,
			MaxLoadedModels: 2, // Limit concurrent models to reduce memory pressure
			PoolSize:        1, // Single pipeline per model (default: min(NumCPU, 4))
		}
		t.Logf("Configured Termite with models directory: %s (max_loaded_models=2, pool_size=1)", modelsDir)
	}

	// Clean any existing raft logs
	CleanupAntflyData(t, dataDir, nodeID)

	// Create context for servers
	swarmCtx, cancel := context.WithCancel(ctx)

	// Create readiness channels
	metadataReadyC := make(chan struct{})
	storeReadyC := make(chan struct{})

	if !opts.DisableTermite {
		termiteReadyC := make(chan struct{})

		go termite.RunAsTermite(
			swarmCtx,
			logger.Named("termite"),
			config.Termite,
			termiteReadyC,
		)

		// Wait for Termite to be ready
		// Generator models (e.g., Gemma) can take 30+ seconds to load
		select {
		case <-termiteReadyC:
			logger.Info("Termite server ready", zap.String("termite_api", config.Termite.ApiUrl))
			SetTermiteURL(config.Termite.ApiUrl)
		case <-time.After(60 * time.Second):
			cancel()
			t.Fatal("Timeout waiting for Termite server to be ready")
		}
	}

	// Start metadata and store servers.
	peers := common.Peers{
		{ID: nodeID, URL: metadataRaftURL},
	}

	metaConf := &store.StoreInfo{
		ID:      nodeID,
		RaftURL: metadataRaftURL,
		ApiURL:  metadataAPIURL,
	}
	storeConf := &store.StoreInfo{
		ID:      nodeID,
		ApiURL:  storeAPIURL,
		RaftURL: storeRaftURL,
	}

	if opts.LocalBypass {
		startSwarmWithLocalBypass(t, swarmCtx, logger, config, metaConf, storeConf, peers, metadataReadyC, storeReadyC)
	} else {
		startSwarmWithHTTP(t, swarmCtx, logger, config, metaConf, storeConf, peers, metadataReadyC, storeReadyC)
	}

	// Wait for both servers to be ready
	select {
	case <-metadataReadyC:
		logger.Info("Metadata server ready")
	case <-time.After(30 * time.Second):
		cancel()
		t.Fatal("Timeout waiting for metadata server to be ready")
	}

	select {
	case <-storeReadyC:
		logger.Info("Store server ready")
	case <-time.After(30 * time.Second):
		cancel()
		t.Fatal("Timeout waiting for store server to be ready")
	}

	// Give servers a moment to fully initialize HTTP handlers
	time.Sleep(500 * time.Millisecond)

	// Create client with timeout to prevent hanging on slow LLM responses
	apiURL := metadataAPIURL + "/api/v1"
	httpClient := &http.Client{Timeout: 10 * time.Minute}
	client, err := antfly.NewAntflyClient(apiURL, httpClient)
	if err != nil {
		cancel()
		t.Fatalf("Failed to create Antfly client: %v", err)
	}

	logger.Info("Swarm started successfully",
		zap.String("termite_api", config.Termite.ApiUrl),
		zap.String("metadata_api", metadataAPIURL),
		zap.String("store_api", storeAPIURL),
	)

	return &SwarmInstance{
		T:               t,
		Logger:          logger,
		Config:          config,
		NodeID:          nodeID,
		MetadataAPIURL:  metadataAPIURL,
		MetadataRaftURL: metadataRaftURL,
		StoreAPIURL:     storeAPIURL,
		StoreRaftURL:    storeRaftURL,
		Client:          client,
		Cancel:          cancel,
		DataDir:         dataDir,
	}
}

// startSwarmWithHTTP starts metadata and store using the standard HTTP-based path.
func startSwarmWithHTTP(
	t *testing.T,
	ctx context.Context,
	logger *zap.Logger,
	config *common.Config,
	metaConf, storeConf *store.StoreInfo,
	peers common.Peers,
	metadataReadyC, storeReadyC chan struct{},
) {
	t.Helper()
	go metadata.RunAsMetadataServer(ctx, logger.Named("metadata"), config, metaConf, peers, false, metadataReadyC, nil)
	go store.RunAsStore(ctx, logger.Named("store"), config, storeConf, "", storeReadyC, nil)
}

// startSwarmWithLocalBypass starts metadata and store with in-process bypass,
// mirroring the production swarm.go wiring.
func startSwarmWithLocalBypass(
	t *testing.T,
	ctx context.Context,
	logger *zap.Logger,
	config *common.Config,
	metaConf, storeConf *store.StoreInfo,
	peers common.Peers,
	metadataReadyC, storeReadyC chan struct{},
) {
	t.Helper()

	// Create store runtime inline.
	storeRuntime, err := store.NewRuntime(logger.Named("store"), config, storeConf, nil)
	if err != nil {
		t.Fatalf("creating store runtime: %v", err)
	}
	t.Cleanup(func() { _ = storeRuntime.Close() })

	// Create metadata runtime with local bypass already selected.
	metaRuntime, err := metadata.NewRuntime(
		logger.Named("metadata"),
		config,
		metaConf,
		peers,
		false,
		nil,
		metadata.RuntimeOptions{
			ExecutionProvider: metadata.NewLocalExecutionProvider(storeRuntime.Store()),
		},
	)
	if err != nil {
		t.Fatalf("creating metadata runtime: %v", err)
	}
	metaRuntime.StartRaft()
	t.Cleanup(func() { _ = metaRuntime.Close() })

	// Start metadata HTTP server.
	go func() {
		u, _ := url.Parse(metaConf.ApiURL)
		srv := &http.Server{Addr: u.Host, Handler: metaRuntime.HTTPHandler(), ReadHeaderTimeout: 30 * time.Second}
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdownCtx)
		}()
		listener, listenErr := net.Listen("tcp", u.Host)
		if listenErr != nil {
			logger.Fatal("metadata listener failed", zap.Error(listenErr))
		}
		close(metadataReadyC)
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metadata HTTP error", zap.Error(err))
		}
	}()

	// Wait for metadata before starting store.
	select {
	case <-metadataReadyC:
	case <-time.After(30 * time.Second):
		t.Fatal("Timeout waiting for metadata in local bypass mode")
	}

	storeRuntime.StartRaft()
	logger.Info("Local shard bypass enabled for e2e test")

	// Start store HTTP server (still needed for raft transport).
	go func() {
		u, _ := url.Parse(storeConf.ApiURL)
		srv := &http.Server{Addr: u.Host, Handler: storeRuntime.HTTPHandler(), ReadTimeout: time.Minute}
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdownCtx)
		}()
		listener, listenErr := net.Listen("tcp", u.Host)
		if listenErr != nil {
			logger.Fatal("store listener failed", zap.Error(listenErr))
		}
		close(storeReadyC)
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("store HTTP error", zap.Error(err))
		}
	}()

	// Register store with metadata.
	go func() {
		orchURLs, _ := config.Metadata.GetOrchestrationURLs()
		if err := store.RegisterWithLeaderWithRetry(ctx, logger, storeRuntime.Store(), storeConf, orchURLs); err != nil {
			if ctx.Err() == nil {
				logger.Error("store registration failed", zap.Error(err))
			}
		}
	}()
}

// findRepoRoot finds the repository root by walking up from the test file
// looking for the .git directory. We use .git instead of go.mod because
// sub-modules (e.g., e2e/) have their own go.mod files.
func findRepoRoot(t *testing.T) string {
	t.Helper()

	// Get the directory of this test file
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("Failed to get current file path")
	}

	dir := filepath.Dir(filename)

	// Walk up until we find .git
	for {
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("Could not find repository root (.git not found)")
		}
		dir = parent
	}
}

// waitForShardsReady polls the table status until shards are ready to accept writes
func waitForShardsReady(t *testing.T, ctx context.Context, client *antfly.AntflyClient, tableName string, timeout time.Duration) {
	t.Helper()

	t.Log("Waiting for shards to be ready...")

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	pollCount := 0

	for {
		select {
		case <-ctx.Done():
			t.Fatal("Context cancelled while waiting for shards")
		case <-ticker.C:
			pollCount++

			if time.Now().After(deadline) {
				t.Fatalf("Timeout waiting for shards after %d polls", pollCount)
			}

			// Get table status
			status, err := client.GetTable(ctx, tableName)
			if err != nil {
				t.Logf("  [Poll %d] Error getting table status: %v", pollCount, err)
				continue
			}

			// Check if we have at least one shard
			if len(status.Shards) > 0 {
				//  Wait longer to ensure leader election completes and propagates
				// In swarm mode, the shard starts quickly but leader status needs time to propagate
				// to the metadata server's view
				if pollCount >= 6 {
					t.Logf("Shards ready after %d polls (~%dms)", pollCount, pollCount*500)
					return
				}
				t.Logf("  [Poll %d] Found %d shard(s), waiting for leader status to propagate", pollCount, len(status.Shards))
			} else {
				t.Logf("  [Poll %d] No shards found yet", pollCount)
			}
		}
	}
}

// waitForEmbeddings polls the named index status until embedding enrichment is complete.
func waitForEmbeddings(t *testing.T, ctx context.Context, client *antfly.AntflyClient, tableName, indexName string, expectedDocs int, timeout time.Duration) {
	t.Helper()

	t.Logf("Waiting for embedding enrichment on index %q to complete (%d documents expected)...", indexName, expectedDocs)

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	pollCount := 0
	lastIndexed := uint64(0)
	stableCount := 0

	for {
		select {
		case <-ctx.Done():
			t.Fatal("Context cancelled while waiting for embeddings")
		case <-ticker.C:
			pollCount++

			if time.Now().After(deadline) {
				t.Fatalf("Timeout waiting for embedding enrichment after %d polls (got %d/%d embeddings)",
					pollCount, lastIndexed, expectedDocs)
			}

			// Get index status directly
			indexStatus, err := client.GetIndex(ctx, tableName, indexName)
			if err != nil {
				t.Logf("  [Poll %d] Error getting index status: %v", pollCount, err)
				continue
			}

			// Parse embedding index stats
			stats, err := indexStatus.Status.AsEmbeddingsIndexStats()
			if err != nil {
				t.Logf("  [Poll %d] Error decoding embedding stats: %v", pollCount, err)
				continue
			}

			totalIndexed := stats.TotalIndexed
			t.Logf("  [Poll %d] Embeddings: %d/%d indexed (%.1f%%)",
				pollCount, totalIndexed, expectedDocs,
				float64(totalIndexed)/float64(expectedDocs)*100)

			// Check if we've reached the expected number of embeddings
			if totalIndexed >= uint64(expectedDocs) { //nolint:gosec // G115: bounded value, cannot overflow in practice
				t.Logf("All embeddings indexed after %d polls (~%ds)",
					pollCount, pollCount*5)
				return
			}

			// If the count hasn't changed for 3 consecutive polls, we're likely done
			// (some documents might not have content to embed)
			if totalIndexed == lastIndexed {
				stableCount++
				if stableCount >= 3 && totalIndexed > 0 {
					t.Logf("Embedding count stable at %d/%d after %d polls, proceeding",
						totalIndexed, expectedDocs, pollCount)
					return
				}
			} else {
				stableCount = 0
			}
			lastIndexed = totalIndexed
		}
	}
}
