// Copyright 2026 Antfly, Inc.
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
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/types"
	antfly "github.com/antflydb/antfly/pkg/client"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/metadata"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// StoreNode represents a running store node in the test cluster
type StoreNode struct {
	ID       types.ID
	APIPort  int
	RaftPort int
	APIURL   string
	RaftURL  string
	Cancel   context.CancelFunc
	ReadyC   chan struct{}
}

// MetadataNode represents the metadata server in the test cluster
type MetadataNode struct {
	ID       types.ID
	APIPort  int
	RaftPort int
	APIURL   string
	RaftURL  string
	ReadyC   chan struct{}
}

type clusterStatusResponse struct {
	Health  string `json:"health"`
	Message string `json:"message"`
	Stores  struct {
		Statuses map[string]json.RawMessage `json:"statuses"`
	} `json:"stores"`
	Shards struct {
		Statuses map[string]*store.ShardStatus `json:"statuses"`
	} `json:"shards"`
}

type nodeShutdownStatusResponse struct {
	Phase           string   `json:"phase"`
	SafeToTerminate bool     `json:"safe_to_terminate"`
	Blocked         bool     `json:"blocked,omitempty"`
	BlockedReason   string   `json:"blocked_reason,omitempty"`
	Message         string   `json:"message,omitempty"`
	BypassedGroups  []string `json:"bypassed_groups,omitempty"`
}

// TestCluster represents a multi-node test cluster for distributed e2e testing.
// It manages metadata and store nodes with dynamic scaling capabilities.
type TestCluster struct {
	T             *testing.T
	Logger        *zap.Logger
	Config        *common.Config
	DataDir       string
	MetadataNode  *MetadataNode
	StoreNodes    map[types.ID]*StoreNode
	Client        *antfly.AntflyClient
	Cancel        context.CancelFunc
	mu            sync.RWMutex
	nextNodeID    types.ID
	availabilityC chan struct{} // Closed when availability check fails
}

// TestClusterConfig configures the test cluster
type TestClusterConfig struct {
	NumStoreNodes       int           // Number of store nodes to start
	NumShards           int           // Default shards per table
	ReplicationFactor   int           // Replication factor (default 1)
	MaxShardSizeBytes   int64         // Max shard size before splits (0 = default)
	DisableShardAlloc   bool          // Disable automatic shard allocation
	ShardCooldownPeriod time.Duration // Cooldown period after shard operations (0 = default 1 minute)
	SplitTimeout        time.Duration // Timeout for split operations before rollback (0 = default 5 minutes)
}

// DefaultTestClusterConfig returns default configuration for test clusters
func DefaultTestClusterConfig() TestClusterConfig {
	return TestClusterConfig{
		NumStoreNodes:     2,
		NumShards:         4,
		ReplicationFactor: 1,
		MaxShardSizeBytes: 0, // Use server default
		DisableShardAlloc: false,
	}
}

// NewTestCluster creates a new test cluster with the specified configuration
func NewTestCluster(t *testing.T, ctx context.Context, cfg TestClusterConfig) *TestCluster {
	t.Helper()

	// Reduce Pebble cache size for tests to avoid OOM (2MB vs default 320MB per shard)
	// With 60 shard replicas (3 nodes × 20 shards), 2MB × 60 = 120MB vs 960MB at 16MB
	db.DefaultPebbleCacheSizeMB = 2

	logger := GetTestLogger(t)
	dataDir := t.TempDir()

	// Create cluster context
	clusterCtx, cancel := context.WithCancel(ctx)

	cluster := &TestCluster{
		T:             t,
		Logger:        logger,
		DataDir:       dataDir,
		StoreNodes:    make(map[types.ID]*StoreNode),
		Cancel:        cancel,
		nextNodeID:    100, // Start store nodes at 100
		availabilityC: make(chan struct{}),
	}

	// Create and start metadata node
	cluster.startMetadataNode(clusterCtx, logger, cfg)

	// Wait for metadata to be ready
	select {
	case <-cluster.MetadataNode.ReadyC:
		logger.Info("Metadata server ready", zap.String("api", cluster.MetadataNode.APIURL))
	case <-time.After(30 * time.Second):
		cancel()
		t.Fatal("Timeout waiting for metadata server to be ready")
	}

	// Give metadata a moment to fully initialize
	time.Sleep(500 * time.Millisecond)

	// Create client
	apiURL := cluster.MetadataNode.APIURL + "/api/v1"
	httpClient := &http.Client{Timeout: 5 * time.Minute}
	client, err := antfly.NewAntflyClient(apiURL, httpClient)
	if err != nil {
		cancel()
		t.Fatalf("Failed to create Antfly client: %v", err)
	}
	cluster.Client = client

	// Start initial store nodes
	for i := 0; i < cfg.NumStoreNodes; i++ {
		cluster.AddStoreNode(clusterCtx)
	}

	// Wait for all stores to register with metadata
	if cfg.NumStoreNodes > 0 {
		err := cluster.WaitForStoresRegistered(ctx, cfg.NumStoreNodes, 60*time.Second)
		if err != nil {
			cancel()
			t.Fatalf("Failed waiting for stores to register: %v", err)
		}
	}

	return cluster
}

// startMetadataNode starts the metadata server
func (c *TestCluster) startMetadataNode(ctx context.Context, logger *zap.Logger, cfg TestClusterConfig) {
	metadataID := types.ID(11)
	metadataAPIPort := GetFreePort(c.T)
	metadataRaftPort := GetFreePort(c.T)
	metadataAPIURL := fmt.Sprintf("http://localhost:%d", metadataAPIPort)
	metadataRaftURL := fmt.Sprintf("http://localhost:%d", metadataRaftPort)

	// Create config
	config := &common.Config{
		SwarmMode:             false, // Not swarm mode - multi-node cluster
		DisableShardAlloc:     cfg.DisableShardAlloc,
		ReplicationFactor:     uint64(cfg.ReplicationFactor), //nolint:gosec // G115: bounded value, cannot overflow in practice
		DefaultShardsPerTable: uint64(cfg.NumShards),         //nolint:gosec // G115: bounded value, cannot overflow in practice
		HealthPort:            GetFreePort(c.T),
		Storage: common.StorageConfig{
			Local: common.LocalStorageConfig{
				BaseDir: c.DataDir,
			},
			Data:     common.StorageBackendLocal,
			Metadata: common.StorageBackendLocal,
		},
		Metadata: common.MetadataInfo{
			OrchestrationUrls: map[string]string{
				metadataID.String(): metadataAPIURL,
			},
		},
	}

	// Configure autoscaling thresholds if specified
	if cfg.MaxShardSizeBytes > 0 {
		config.MaxShardSizeBytes = uint64(cfg.MaxShardSizeBytes)
		config.MaxShardsPerTable = 50 // Allow plenty of shards for splits
	}

	// Configure shard cooldown period for faster testing
	if cfg.ShardCooldownPeriod > 0 {
		config.ShardCooldownPeriod = cfg.ShardCooldownPeriod
	}

	// Configure split timeout for faster testing of rollback scenarios
	if cfg.SplitTimeout > 0 {
		config.SplitTimeout = cfg.SplitTimeout
	}

	c.Config = config

	readyC := make(chan struct{})
	c.MetadataNode = &MetadataNode{
		ID:       metadataID,
		APIPort:  metadataAPIPort,
		RaftPort: metadataRaftPort,
		APIURL:   metadataAPIURL,
		RaftURL:  metadataRaftURL,
		ReadyC:   readyC,
	}

	peers := common.Peers{
		{ID: metadataID, URL: metadataRaftURL},
	}

	go func() {
		metadata.RunAsMetadataServer(
			ctx,
			logger.Named("metadata"),
			config,
			&store.StoreInfo{
				ID:      metadataID,
				RaftURL: metadataRaftURL,
				ApiURL:  metadataAPIURL,
			},
			peers,
			false, // join
			readyC,
			nil,
		)
	}()
}

// AddStoreNode adds a new store node to the cluster
func (c *TestCluster) AddStoreNode(ctx context.Context) *StoreNode {
	c.mu.Lock()
	nodeID := c.nextNodeID
	c.nextNodeID++
	c.mu.Unlock()

	storeAPIPort := GetFreePort(c.T)
	storeRaftPort := GetFreePort(c.T)
	storeAPIURL := fmt.Sprintf("http://localhost:%d", storeAPIPort)
	storeRaftURL := fmt.Sprintf("http://localhost:%d", storeRaftPort)

	readyC := make(chan struct{})

	// Create a cancellable context for this store
	storeCtx, cancel := context.WithCancel(ctx) //nolint:gosec // G118: cancel stored in StoreNode.Cancel for lifecycle management

	node := &StoreNode{
		ID:       nodeID,
		APIPort:  storeAPIPort,
		RaftPort: storeRaftPort,
		APIURL:   storeAPIURL,
		RaftURL:  storeRaftURL,
		Cancel:   cancel,
		ReadyC:   readyC,
	}

	c.mu.Lock()
	c.StoreNodes[nodeID] = node
	c.mu.Unlock()

	go func() {
		store.RunAsStore(
			storeCtx,
			c.Logger.Named(fmt.Sprintf("store-%d", nodeID)),
			c.Config,
			&store.StoreInfo{
				ID:      nodeID,
				ApiURL:  storeAPIURL,
				RaftURL: storeRaftURL,
			},
			"", // serviceHostname
			readyC,
			nil,
		)
	}()

	// Wait for store to be ready
	select {
	case <-readyC:
		c.Logger.Info("Store node ready", zap.Stringer("nodeID", nodeID), zap.String("api", storeAPIURL))
	case <-time.After(30 * time.Second):
		c.T.Fatalf("Timeout waiting for store node %d to be ready", nodeID)
	}

	// Give the store time to register with metadata
	time.Sleep(500 * time.Millisecond)

	return node
}

// RemoveStoreNode gracefully removes a store node from the cluster. It requests
// runtime drain, waits until metadata reports that the node is safe to
// terminate, stops the local process, and finalizes the node lifecycle record.
func (c *TestCluster) RemoveStoreNode(ctx context.Context, nodeID types.ID) error {
	c.mu.RLock()
	node, exists := c.StoreNodes[nodeID]
	if !exists {
		c.mu.RUnlock()
		return fmt.Errorf("store node %d not found", nodeID)
	}
	c.mu.RUnlock()

	c.Logger.Info("Removing store node", zap.Stringer("nodeID", nodeID))

	// Request node shutdown in metadata before stopping the store node. Node
	// lifecycle APIs use decimal path IDs to match the Zig runtime contract.
	nodeIDString := strconv.FormatUint(uint64(nodeID), 10)
	nodeURL := fmt.Sprintf("%s/_internal/v1/nodes/%s", c.MetadataNode.APIURL, nodeIDString)
	shutdownURL := nodeURL + "/shutdown"
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, shutdownURL, bytes.NewBufferString(`{"type":"remove","reason":"e2e graceful removal"}`))
	if err != nil {
		return fmt.Errorf("creating node shutdown request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return fmt.Errorf("requesting node shutdown: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("node shutdown returned status %d", resp.StatusCode)
	}

	shutdownRequested := true
	nodeStopped := false
	defer func() {
		if !shutdownRequested || nodeStopped {
			return
		}
		cancelCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cancelReq, cancelErr := http.NewRequestWithContext(cancelCtx, http.MethodDelete, shutdownURL, nil)
		if cancelErr != nil {
			c.Logger.Warn("Failed to create node shutdown cancellation request", zap.Stringer("nodeID", nodeID), zap.Error(cancelErr))
			return
		}
		cancelResp, cancelErr := http.DefaultClient.Do(cancelReq) //nolint:gosec // G704: HTTP client calling configured endpoint
		if cancelErr != nil {
			c.Logger.Warn("Failed to cancel node shutdown after removal failure", zap.Stringer("nodeID", nodeID), zap.Error(cancelErr))
			return
		}
		_, _ = io.Copy(io.Discard, cancelResp.Body)
		_ = cancelResp.Body.Close()
		if cancelResp.StatusCode < 200 || cancelResp.StatusCode >= 300 {
			c.Logger.Warn("Node shutdown cancellation returned non-success status", zap.Stringer("nodeID", nodeID), zap.Int("status", cancelResp.StatusCode))
		}
	}()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		statusReq, err := http.NewRequestWithContext(ctx, http.MethodGet, shutdownURL, nil)
		if err != nil {
			return fmt.Errorf("creating node shutdown status request: %w", err)
		}
		statusResp, err := http.DefaultClient.Do(statusReq) //nolint:gosec // G704: HTTP client calling configured endpoint
		if err != nil {
			return fmt.Errorf("getting node shutdown status: %w", err)
		}
		if statusResp.StatusCode < 200 || statusResp.StatusCode >= 300 {
			_, _ = io.Copy(io.Discard, statusResp.Body)
			_ = statusResp.Body.Close()
			return fmt.Errorf("node shutdown status returned status %d", statusResp.StatusCode)
		}
		var status nodeShutdownStatusResponse
		if err := json.NewDecoder(statusResp.Body).Decode(&status); err != nil {
			_ = statusResp.Body.Close()
			return fmt.Errorf("decoding node shutdown status: %w", err)
		}
		_ = statusResp.Body.Close()
		if status.Blocked {
			message := status.Message
			if message == "" {
				message = status.BlockedReason
			}
			if message == "" {
				message = "metadata reported blocked node shutdown"
			}
			return fmt.Errorf("node shutdown blocked in phase %q: %s", status.Phase, message)
		}
		if status.SafeToTerminate {
			break
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for node shutdown safety: %w", ctx.Err())
		case <-ticker.C:
		}
	}

	// Stop the store node only after metadata reports that no local runtime
	// ownership remains.
	node.Cancel()
	nodeStopped = true

	c.mu.Lock()
	delete(c.StoreNodes, nodeID)
	c.mu.Unlock()

	finalizeReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, nodeURL, nil)
	if err != nil {
		return fmt.Errorf("creating node shutdown finalization request: %w", err)
	}
	finalizeResp, err := http.DefaultClient.Do(finalizeReq) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return fmt.Errorf("finalizing node shutdown: %w", err)
	}
	defer func() { _ = finalizeResp.Body.Close() }()
	_, _ = io.Copy(io.Discard, finalizeResp.Body)
	if finalizeResp.StatusCode < 200 || finalizeResp.StatusCode >= 300 {
		return fmt.Errorf("node shutdown finalization returned status %d", finalizeResp.StatusCode)
	}

	c.Logger.Info("Store node removed after graceful shutdown", zap.Stringer("nodeID", nodeID))
	return nil
}

// CrashStoreNode simulates a sudden crash of a store node by cancelling its context
// without deregistering it from metadata. This is useful for testing failure recovery
// scenarios where the metadata still thinks the node is alive.
func (c *TestCluster) CrashStoreNode(nodeID types.ID) error {
	c.mu.Lock()
	node, exists := c.StoreNodes[nodeID]
	if !exists {
		c.mu.Unlock()
		return fmt.Errorf("store node %d not found", nodeID)
	}
	delete(c.StoreNodes, nodeID)
	c.mu.Unlock()

	c.Logger.Info("Crashing store node (without deregistering)", zap.Stringer("nodeID", nodeID))

	// Just cancel the node context without deregistering
	// The metadata will still think this node is alive until heartbeat timeout
	node.Cancel()

	c.Logger.Info("Store node crashed", zap.Stringer("nodeID", nodeID))
	return nil
}

// GetStoreNode returns a store node by ID
func (c *TestCluster) GetStoreNode(nodeID types.ID) (*StoreNode, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	node, exists := c.StoreNodes[nodeID]
	return node, exists
}

// GetActiveStoreCount returns the number of active store nodes
func (c *TestCluster) GetActiveStoreCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.StoreNodes)
}

// GetStoreNodeIDs returns all active store node IDs
func (c *TestCluster) GetStoreNodeIDs() []types.ID {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ids := make([]types.ID, 0, len(c.StoreNodes))
	for id := range c.StoreNodes {
		ids = append(ids, id)
	}
	return ids
}

// Cleanup stops all nodes in the cluster
func (c *TestCluster) Cleanup() {
	c.Cancel()
	// Give more time for Bleve indexes and other resources to close properly
	// before Go's t.TempDir() cleanup removes the directory tree.
	time.Sleep(2 * time.Second)
}

// WaitForStoresRegistered polls until expected number of stores are registered with metadata
func (c *TestCluster) WaitForStoresRegistered(ctx context.Context, expectedStores int, timeout time.Duration) error {
	c.T.Helper()

	c.T.Logf("Waiting for %d stores to register with metadata...", expectedStores)

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	pollCount := 0

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled while waiting for stores")
		case <-ticker.C:
			pollCount++

			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for %d stores to register after %d polls", expectedStores, pollCount)
			}

			// Call the status endpoint to check registered stores
			resp, err := http.Get(c.MetadataNode.APIURL + "/api/v1/status")
			if err != nil {
				c.T.Logf("  [Poll %d] Error getting status: %v", pollCount, err)
				continue
			}

			var status struct {
				Stores struct {
					Statuses map[string]any `json:"statuses"`
				} `json:"stores"`
			}

			if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
				_ = resp.Body.Close()
				c.T.Logf("  [Poll %d] Error decoding status: %v", pollCount, err)
				continue
			}
			_ = resp.Body.Close()

			registeredCount := len(status.Stores.Statuses)
			c.T.Logf("  [Poll %d] Registered stores: %d/%d", pollCount, registeredCount, expectedStores)

			if registeredCount >= expectedStores {
				c.T.Logf("All %d stores registered after %d polls", expectedStores, pollCount)
				return nil
			}
		}
	}
}

// WaitForShardsReady waits for all shards of a table to be allocated and have Raft leaders elected.
// It checks both the shard count and cluster health to ensure shards are fully operational.
func (c *TestCluster) WaitForShardsReady(ctx context.Context, tableName string, expectedShards int, timeout time.Duration) error {
	c.T.Helper()

	c.T.Logf("Waiting for %d shards to be ready for table %s...", expectedShards, tableName)

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	pollCount := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			pollCount++

			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for shards after %d polls", pollCount)
			}

			// First check shard count
			tableStatus, err := c.Client.GetTable(ctx, tableName)
			if err != nil {
				c.T.Logf("  [Poll %d] Error getting table status: %v", pollCount, err)
				continue
			}

			shardCount := len(tableStatus.Shards)
			if shardCount < expectedShards {
				c.T.Logf("  [Poll %d] Shards: %d/%d", pollCount, shardCount, expectedShards)
				continue
			}

			// Shards exist, now check cluster health to ensure Raft leaders are elected
			resp, err := http.Get(c.MetadataNode.APIURL + "/api/v1/status")
			if err != nil {
				c.T.Logf("  [Poll %d] Error getting cluster status: %v", pollCount, err)
				continue
			}

			var clusterStatus struct {
				Health  string `json:"health"`
				Message string `json:"message"`
			}

			if err := json.NewDecoder(resp.Body).Decode(&clusterStatus); err != nil {
				_ = resp.Body.Close()
				c.T.Logf("  [Poll %d] Error decoding cluster status: %v", pollCount, err)
				continue
			}
			_ = resp.Body.Close()

			if clusterStatus.Health != "healthy" {
				c.T.Logf("  [Poll %d] Shards: %d/%d, cluster health: %s (%s)",
					pollCount, shardCount, expectedShards, clusterStatus.Health, clusterStatus.Message)
				continue
			}

			c.Logger.Info("Shards ready",
				zap.String("table", tableName),
				zap.Int("count", shardCount),
				zap.Int("expected", expectedShards))
			return nil
		}
	}
}

// WaitForShardCount waits for the table to have at least the expected number of shards
func (c *TestCluster) WaitForShardCount(ctx context.Context, tableName string, minShards int, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastCount int
	for {
		select {
		case <-ctx.Done():
			return lastCount, ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return lastCount, fmt.Errorf("timeout waiting for shard count >= %d (got %d)", minShards, lastCount)
			}

			status, err := c.Client.GetTable(ctx, tableName)
			if err != nil {
				continue
			}

			lastCount = len(status.Shards)
			if lastCount >= minShards {
				c.Logger.Info("Shard count reached",
					zap.String("table", tableName),
					zap.Int("count", lastCount),
					zap.Int("minExpected", minShards))
				return lastCount, nil
			}
		}
	}
}

func (c *TestCluster) getClusterStatus(ctx context.Context) (*clusterStatusResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.MetadataNode.APIURL+"/api/v1/status", nil)
	if err != nil {
		return nil, fmt.Errorf("creating cluster status request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return nil, fmt.Errorf("getting cluster status: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("cluster status returned %d", resp.StatusCode)
	}

	var status clusterStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("decoding cluster status: %w", err)
	}

	return &status, nil
}

func (c *TestCluster) getStoreStatus(ctx context.Context, node *StoreNode) (*store.StoreStatus, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, node.APIURL+"/status", nil)
	if err != nil {
		return nil, fmt.Errorf("creating store status request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return nil, fmt.Errorf("getting store status for node %d: %w", node.ID, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("store status for node %d returned %d", node.ID, resp.StatusCode)
	}

	var status store.StoreStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("decoding store status for node %d: %w", node.ID, err)
	}

	return &status, nil
}

func (c *TestCluster) sampleReadsHealthy(ctx context.Context, samples map[string][]string) error {
	for tableName, keys := range samples {
		for _, key := range keys {
			record, err := c.Client.LookupKey(ctx, tableName, key)
			if err != nil {
				return fmt.Errorf("lookup %s/%s: %w", tableName, key, err)
			}
			if record == nil {
				return fmt.Errorf("lookup %s/%s returned nil record", tableName, key)
			}
		}
	}
	return nil
}

func (c *TestCluster) WaitForNodeAssigned(ctx context.Context, nodeID types.ID, timeout time.Duration) error {
	c.T.Helper()

	c.T.Logf("Waiting for store node %d to receive shard assignments...", nodeID)

	c.mu.RLock()
	node, exists := c.StoreNodes[nodeID]
	c.mu.RUnlock()
	if !exists {
		return fmt.Errorf("store node %d not found", nodeID)
	}

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	pollCount := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			pollCount++
			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for node %d assignment after %d polls", nodeID, pollCount)
			}

			status, err := c.getClusterStatus(ctx)
			if err != nil {
				c.T.Logf("  [Poll %d] Error getting cluster status: %v", pollCount, err)
				continue
			}

			storeStatus, err := c.getStoreStatus(ctx, node)
			if err != nil {
				c.T.Logf("  [Poll %d] Error getting store status for node %d: %v", pollCount, nodeID, err)
				continue
			}

			assignedShards := len(storeStatus.Shards)
			if assignedShards == 0 || status.Health != "healthy" {
				c.T.Logf("  [Poll %d] Node %d local shards=%d, cluster health=%s (%s)",
					pollCount, nodeID, assignedShards, status.Health, status.Message)
				continue
			}

			c.T.Logf("Store node %d received %d local shards after %d polls", nodeID, assignedShards, pollCount)
			return nil
		}
	}
}

func (c *TestCluster) WaitForNodeRemovedAndStable(
	ctx context.Context,
	nodeID types.ID,
	samples map[string][]string,
	timeout time.Duration,
) error {
	c.T.Helper()

	c.T.Logf("Waiting for store node %d to be removed from shard assignments...", nodeID)

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	pollCount := 0
	consecutiveHealthyReads := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			pollCount++
			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for node %d removal after %d polls", nodeID, pollCount)
			}

			if pollCount%5 == 1 {
				if err := c.TriggerReallocate(ctx); err != nil {
					c.T.Logf("  [Poll %d] TriggerReallocate failed: %v", pollCount, err)
				}
			}

			status, err := c.getClusterStatus(ctx)
			if err != nil {
				c.T.Logf("  [Poll %d] Error getting cluster status: %v", pollCount, err)
				continue
			}

			c.mu.RLock()
			activeNodes := make([]*StoreNode, 0, len(c.StoreNodes))
			for _, node := range c.StoreNodes {
				activeNodes = append(activeNodes, node)
			}
			c.mu.RUnlock()

			shardsWithRemovedNode := make(map[types.ID]struct{})
			shardsWithoutLeader := make(map[types.ID]struct{})
			storeStatusErrors := 0
			for _, node := range activeNodes {
				storeStatus, err := c.getStoreStatus(ctx, node)
				if err != nil {
					c.T.Logf("  [Poll %d] Error getting store status for node %d: %v", pollCount, node.ID, err)
					storeStatusErrors++
					continue
				}
				for shardID, shard := range storeStatus.Shards {
					if shard == nil {
						continue
					}
					if shard.RaftStatus == nil || shard.RaftStatus.Lead == 0 {
						shardsWithoutLeader[shardID] = struct{}{}
					}
					if shard.Peers.Contains(nodeID) ||
						(shard.RaftStatus != nil && shard.RaftStatus.Voters.Contains(nodeID)) {
						shardsWithRemovedNode[shardID] = struct{}{}
					}
				}
			}

			if err := c.sampleReadsHealthy(ctx, samples); err != nil {
				consecutiveHealthyReads = 0
				c.T.Logf(
					"  [Poll %d] Node %d still present in %d shards, shards without leader=%d, store status errors=%d, cluster health=%s (%s), sample reads not ready: %v",
					pollCount,
					nodeID,
					len(shardsWithRemovedNode),
					len(shardsWithoutLeader),
					storeStatusErrors,
					status.Health,
					status.Message,
					err,
				)
				continue
			}

			consecutiveHealthyReads++
			c.T.Logf(
				"  [Poll %d] Node %d still present in %d shards, shards without leader=%d, store status errors=%d, cluster health=%s (%s), healthy read streak=%d",
				pollCount,
				nodeID,
				len(shardsWithRemovedNode),
				len(shardsWithoutLeader),
				storeStatusErrors,
				status.Health,
				status.Message,
				consecutiveHealthyReads,
			)
			if storeStatusErrors > 0 || consecutiveHealthyReads < 3 {
				continue
			}

			c.T.Logf("Store node %d reached stable read availability after %d polls", nodeID, pollCount)
			return nil
		}
	}
}

// TriggerReallocate calls the /_internal/v1/reallocate endpoint to force the
// reconciler to run immediately. This is useful for testing shard splits.
func (c *TestCluster) TriggerReallocate(ctx context.Context) error {
	c.T.Helper()

	url := c.MetadataNode.APIURL + "/_internal/v1/reallocate"
	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return fmt.Errorf("reallocate request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("reallocate returned status %d", resp.StatusCode)
	}

	return nil
}

// WaitForKeyAvailable polls until a key is available in the table.
// This is needed because distributed transactions resolve intents asynchronously.
func (c *TestCluster) WaitForKeyAvailable(ctx context.Context, tableName string, key string, timeout time.Duration) error {
	c.T.Helper()

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				if lastErr != nil {
					return fmt.Errorf("timeout waiting for key %q: %w", key, lastErr)
				}
				return fmt.Errorf("timeout waiting for key %q", key)
			}

			_, err := c.Client.LookupKey(ctx, tableName, key)
			if err == nil {
				return nil
			}
			lastErr = err
		}
	}
}

// VerifyKeyValues verifies multiple keys have expected values.
// It first waits for each key to become available (handles async intent resolution).
func (c *TestCluster) VerifyKeyValues(ctx context.Context, tableName string, expected map[string]any) error {
	c.T.Helper()

	for key, expectedValue := range expected {
		// Wait for key to be available (handles async intent resolution)
		if err := c.WaitForKeyAvailable(ctx, tableName, key, 10*time.Second); err != nil {
			return fmt.Errorf("key %q not available: %w", key, err)
		}

		doc, err := c.Client.LookupKey(ctx, tableName, key)
		if err != nil {
			return fmt.Errorf("failed to lookup key %q: %w", key, err)
		}

		expectedDoc, ok := expectedValue.(map[string]any)
		if !ok {
			return fmt.Errorf("expected value for key %q is not a map", key)
		}

		// Compare specific fields we care about
		for field, expected := range expectedDoc {
			actual, exists := doc[field]
			if !exists {
				return fmt.Errorf("key %q: field %q not found in document", key, field)
			}

			// Handle numeric comparison (JSON unmarshals numbers as float64)
			switch ev := expected.(type) {
			case int:
				actualNum, ok := actual.(float64)
				if !ok {
					return fmt.Errorf("key %q: field %q expected number, got %T", key, field, actual)
				}
				if int(actualNum) != ev {
					return fmt.Errorf("key %q: field %q expected %d, got %v", key, field, ev, actual)
				}
			case float64:
				actualNum, ok := actual.(float64)
				if !ok {
					return fmt.Errorf("key %q: field %q expected number, got %T", key, field, actual)
				}
				if actualNum != ev {
					return fmt.Errorf("key %q: field %q expected %f, got %v", key, field, ev, actual)
				}
			case string:
				actualStr, ok := actual.(string)
				if !ok {
					return fmt.Errorf("key %q: field %q expected string, got %T", key, field, actual)
				}
				if actualStr != ev {
					return fmt.Errorf("key %q: field %q expected %q, got %q", key, field, ev, actualStr)
				}
			default:
				// Use bytes comparison for complex types
				expectedBytes, _ := json.Marshal(expected)
				actualBytes, _ := json.Marshal(actual)
				if !bytes.Equal(expectedBytes, actualBytes) {
					return fmt.Errorf("key %q: field %q value mismatch: expected %v, got %v", key, field, expected, actual)
				}
			}
		}
	}

	return nil
}

// GetShardDistribution returns shard IDs for a table.
// Note: The SDK doesn't expose store-to-shard mapping, so this just returns shard IDs.
func (c *TestCluster) GetShardDistribution(ctx context.Context, tableName string) []string {
	c.T.Helper()

	status, err := c.Client.GetTable(ctx, tableName)
	require.NoError(c.T, err, "Failed to get table status")

	shardIDs := make([]string, 0, len(status.Shards))
	for shardID := range status.Shards {
		shardIDs = append(shardIDs, shardID)
	}

	return shardIDs
}

// GetClusterStatus returns the current cluster status as JSON for debugging
func (c *TestCluster) GetClusterStatus(ctx context.Context) string {
	status := struct {
		StoreNodes []types.ID     `json:"store_nodes"`
		Tables     map[string]int `json:"tables"`
	}{
		StoreNodes: c.GetStoreNodeIDs(),
		Tables:     make(map[string]int),
	}

	data, _ := json.MarshalIndent(status, "", "  ")
	return string(data)
}

// AvailabilityChecker continuously checks cluster availability during operations
type AvailabilityChecker struct {
	cluster     *TestCluster
	tableName   string
	ctx         context.Context
	cancel      context.CancelFunc
	successOps  atomic.Int64
	failedOps   atomic.Int64
	maxDowntime time.Duration
	lastSuccess time.Time
	mu          sync.Mutex
}

// NewAvailabilityChecker creates a new availability checker
func NewAvailabilityChecker(cluster *TestCluster, tableName string) *AvailabilityChecker {
	ctx, cancel := context.WithCancel(context.Background())
	return &AvailabilityChecker{
		cluster:     cluster,
		tableName:   tableName,
		ctx:         ctx,
		cancel:      cancel,
		lastSuccess: time.Now(),
	}
}

// Start begins checking availability
func (ac *AvailabilityChecker) Start() {
	go ac.run()
}

// run is the main availability checking loop
func (ac *AvailabilityChecker) run() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ac.ctx.Done():
			return
		case <-ticker.C:
			reqCtx, cancel := context.WithTimeout(ac.ctx, 2*time.Second)
			_, err := ac.cluster.Client.GetTable(reqCtx, ac.tableName)
			cancel()

			ac.mu.Lock()
			if err != nil {
				ac.failedOps.Add(1)
				downtime := time.Since(ac.lastSuccess)
				if downtime > ac.maxDowntime {
					ac.maxDowntime = downtime
				}
			} else {
				ac.successOps.Add(1)
				ac.lastSuccess = time.Now()
			}
			ac.mu.Unlock()
		}
	}
}

// Stop stops the availability checker
func (ac *AvailabilityChecker) Stop() {
	ac.cancel()
}

// Stats returns the availability statistics
func (ac *AvailabilityChecker) Stats() (success, failed int64, maxDowntime time.Duration) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	return ac.successOps.Load(), ac.failedOps.Load(), ac.maxDowntime
}

// GenerateTestData generates test data of approximately the specified size.
// Uses random bytes that are difficult to compress, ensuring the on-disk size
// (after Pebble compression) is close to the logical size.
func GenerateTestData(size int) map[string]any {
	// Generate random bytes that won't compress well
	// Using base64 encoding to ensure valid UTF-8 for JSON storage
	padding := make([]byte, size)
	for i := range padding {
		// Random printable ASCII characters (32-126)
		padding[i] = byte(32 + rand.Intn(95)) //nolint:gosec // G115: bounded value, cannot overflow in practice; G404: non-security randomness for ML/jitter
	}

	return map[string]any{
		"content":   string(padding),
		"timestamp": time.Now().Unix(),
	}
}
