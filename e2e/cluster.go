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
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/http"
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

// RemoveStoreNode removes a store node from the cluster by deregistering it
func (c *TestCluster) RemoveStoreNode(ctx context.Context, nodeID types.ID) error {
	c.mu.Lock()
	node, exists := c.StoreNodes[nodeID]
	if !exists {
		c.mu.Unlock()
		return fmt.Errorf("store node %d not found", nodeID)
	}
	delete(c.StoreNodes, nodeID)
	c.mu.Unlock()

	c.Logger.Info("Removing store node", zap.Stringer("nodeID", nodeID))

	// Deregister the store from metadata (this will trigger shard reallocation)
	// Note: The URL path parameter is parsed as a hex string by IDFromString,
	// so we must use nodeID.String() which returns the hex representation.
	deregisterURL := fmt.Sprintf("%s/_internal/v1/store/%s", c.MetadataNode.APIURL, nodeID.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, deregisterURL, nil)
	if err != nil {
		return fmt.Errorf("creating deregister request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return fmt.Errorf("deregistering store: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("deregister returned status %d", resp.StatusCode)
	}

	// Stop the store node
	node.Cancel()

	c.Logger.Info("Store node removed and deregistered", zap.Stringer("nodeID", nodeID))
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
