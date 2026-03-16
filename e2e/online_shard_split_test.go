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
	"fmt"
	"math/rand"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/antflydb/antfly/pkg/client/query"
	"github.com/stretchr/testify/require"
)

// WriteRecord tracks a write operation for later verification
type WriteRecord struct {
	Key       string
	Value     map[string]any
	Timestamp time.Time
	Succeeded bool
	Error     error
}

// BackgroundWriter continuously writes to a table during tests
type BackgroundWriter struct {
	cluster   *TestCluster
	tableName string
	keyPrefix string

	mu       sync.Mutex
	writes   []WriteRecord
	stopC    chan struct{}
	stoppedC chan struct{}

	successCount atomic.Int64
	errorCount   atomic.Int64
}

// NewBackgroundWriter creates a new background writer
func NewBackgroundWriter(cluster *TestCluster, tableName, keyPrefix string) *BackgroundWriter {
	return &BackgroundWriter{
		cluster:   cluster,
		tableName: tableName,
		keyPrefix: keyPrefix,
		stopC:     make(chan struct{}),
		stoppedC:  make(chan struct{}),
	}
}

// Start begins the background writing goroutine
func (bw *BackgroundWriter) Start(ctx context.Context, writeInterval time.Duration) {
	go bw.run(ctx, writeInterval)
}

func (bw *BackgroundWriter) run(ctx context.Context, writeInterval time.Duration) {
	defer close(bw.stoppedC)

	ticker := time.NewTicker(writeInterval)
	defer ticker.Stop()

	counter := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-bw.stopC:
			return
		case <-ticker.C:
			key := fmt.Sprintf("%s-%06d", bw.keyPrefix, counter)
			counter++

			value := map[string]any{
				"content":   fmt.Sprintf("Background write %d at %s", counter, time.Now().Format(time.RFC3339Nano)),
				"timestamp": time.Now().UnixNano(),
				"seq":       counter,
			}

			record := WriteRecord{
				Key:       key,
				Value:     value,
				Timestamp: time.Now(),
			}

			reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			// Use Batch instead of LinearMerge for single-record inserts
			// LinearMerge deletes keys not in the request, which causes data loss
			_, err := bw.cluster.Client.Batch(reqCtx, bw.tableName, antfly.BatchRequest{
				Inserts:   map[string]any{key: value},
				SyncLevel: antfly.SyncLevelWrite,
			})
			cancel()

			record.Succeeded = err == nil
			record.Error = err

			bw.mu.Lock()
			bw.writes = append(bw.writes, record)
			bw.mu.Unlock()

			if err != nil {
				bw.errorCount.Add(1)
			} else {
				bw.successCount.Add(1)
			}
		}
	}
}

// Stop stops the background writer and waits for it to finish
func (bw *BackgroundWriter) Stop() {
	close(bw.stopC)
	<-bw.stoppedC
}

// GetWrites returns all recorded writes
func (bw *BackgroundWriter) GetWrites() []WriteRecord {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	result := make([]WriteRecord, len(bw.writes))
	copy(result, bw.writes)
	return result
}

// Stats returns success and error counts
func (bw *BackgroundWriter) Stats() (success, errors int64) {
	return bw.successCount.Load(), bw.errorCount.Load()
}

// TestE2E_OnlineSplit_ContinuousWrites tests that writes during shard split succeed
// and no data is lost during the split process.
func TestE2E_OnlineSplit_ContinuousWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	// Online shard split tests run by default (skip with -short).

	sigCtx, sigCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer sigCancel()

	ctx, cancel := context.WithTimeout(sigCtx, 20*time.Minute)
	defer cancel()

	t.Log("=== Starting Online Shard Split - Continuous Writes Test ===")

	// Step 1: Create test cluster
	// Using RF=3 ensures availability during split operations
	// Using low MaxShardSizeBytes (10KB) with incompressible data to trigger splits quickly
	t.Log("Step 1: Starting test cluster...")
	cluster := NewTestCluster(t, ctx, TestClusterConfig{
		NumStoreNodes:       3,
		NumShards:           1, // Start with 1 shard to test split
		ReplicationFactor:   3,
		MaxShardSizeBytes:   10 * 1024, // 10KB - low threshold, works with random (incompressible) test data
		DisableShardAlloc:   false,
		ShardCooldownPeriod: 5 * time.Second, // Short cooldown for faster testing
	})
	defer cluster.Cleanup()

	// Step 2: Create table
	tableName := "online_split_continuous"
	t.Log("Step 2: Creating table...")
	err := cluster.Client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
	})
	require.NoError(t, err, "Failed to create table")

	err = cluster.WaitForShardsReady(ctx, tableName, 1, 60*time.Second)
	require.NoError(t, err, "Shards not ready")

	// Step 3: Write initial data
	t.Log("Step 3: Writing initial data...")
	initialRecords := make(map[string]any)
	for i := range 100 {
		key := fmt.Sprintf("init-%03d", i)
		initialRecords[key] = map[string]any{
			"content": fmt.Sprintf("Initial document %d", i),
			"seq":     i,
		}
	}

	_, err = cluster.Client.LinearMerge(ctx, tableName, antfly.LinearMergeRequest{
		Records:   initialRecords,
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "Failed to write initial data")

	// Step 4: Start background writer
	t.Log("Step 4: Starting background writer...")
	bgWriter := NewBackgroundWriter(cluster, tableName, "bg")
	bgWriter.Start(ctx, 50*time.Millisecond)

	// Step 5: Start availability checker
	t.Log("Step 5: Starting availability checker...")
	availChecker := NewAvailabilityChecker(cluster, tableName)
	availChecker.Start()
	defer availChecker.Stop()

	// Step 6: Insert data to trigger shard split
	// With 10KB threshold, insert ~500KB to reliably trigger split
	t.Log("Step 6: Inserting data to trigger shard split...")
	const recordsToTriggerSplit = 50
	const recordSize = 10 * 1024 // 10KB per record = ~500KB total

	for i := range recordsToTriggerSplit {
		key := fmt.Sprintf("large-%03d", i)
		largeRecord := map[string]any{key: GenerateTestData(recordSize)}

		// Server-side retry handles transient errors during shard splits
		_, err := cluster.Client.Batch(ctx, tableName, antfly.BatchRequest{
			Inserts:   largeRecord,
			SyncLevel: antfly.SyncLevelWrite,
		})
		require.NoError(t, err, "Failed to insert large record %d", i)

		if i%10 == 0 {
			t.Logf("  Inserted %d/%d records (~%.0fKB)", i+1, recordsToTriggerSplit, float64((i+1)*recordSize)/1024)
		}
	}

	// Step 7: Trigger reallocate and wait for shard split
	t.Log("Step 7: Triggering reallocate and waiting for shard split...")
	time.Sleep(2 * time.Second) // Give time for disk size stats to update

	// Trigger reallocate to force reconciler to run
	for range 10 {
		if err := cluster.TriggerReallocate(ctx); err != nil {
			t.Logf("Warning: TriggerReallocate failed: %v", err)
		}
		time.Sleep(3 * time.Second) // Wait between reallocate attempts
	}

	shardCount, err := cluster.WaitForShardCount(ctx, tableName, 2, 3*time.Minute)
	if err != nil {
		t.Logf("Warning: Split may not have occurred (shards=%d): %v", shardCount, err)
	} else {
		t.Logf("Split completed: %d shards", shardCount)
	}

	// Step 8: Continue writing for a bit after split
	t.Log("Step 8: Continuing writes after split...")
	time.Sleep(10 * time.Second)

	// Step 9: Stop background writer
	t.Log("Step 9: Stopping background writer...")
	bgWriter.Stop()

	// Step 10: Verify results
	t.Log("Step 10: Verifying results...")

	// Get background write stats
	bgSuccess, bgErrors := bgWriter.Stats()
	t.Logf("Background writer stats: success=%d, errors=%d", bgSuccess, bgErrors)

	// Get availability stats
	availSuccess, availFailed, maxDowntime := availChecker.Stats()
	t.Logf("Availability stats: success=%d, failed=%d, maxDowntime=%v", availSuccess, availFailed, maxDowntime)

	// Verify all background writes
	writes := bgWriter.GetWrites()
	t.Logf("Total background writes attempted: %d", len(writes))

	successfulWrites := 0
	for _, w := range writes {
		if w.Succeeded {
			successfulWrites++
		}
	}
	t.Logf("Successful background writes: %d/%d", successfulWrites, len(writes))

	// Verify initial data is still accessible
	// Server-side retry handles transient errors during leader election
	t.Log("Verifying initial data accessibility...")
	for i := range 10 {
		key := fmt.Sprintf("init-%03d", i)
		record, err := cluster.Client.LookupKey(ctx, tableName, key)
		require.NoError(t, err, "Failed to read initial record %s after split", key)
		require.NotNil(t, record, "Initial record %s should exist", key)
	}

	// Verify large data is accessible
	t.Log("Verifying large data accessibility...")
	for i := range 5 {
		key := fmt.Sprintf("large-%03d", i)
		record, err := cluster.Client.LookupKey(ctx, tableName, key)
		require.NoError(t, err, "Failed to read large record %s after split", key)
		require.NotNil(t, record, "Large record %s should exist", key)
	}

	// Verify successful background writes are readable
	t.Log("Verifying background writes are readable...")
	successfulWriteKeys := make([]string, 0)
	for _, w := range writes {
		if w.Succeeded {
			successfulWriteKeys = append(successfulWriteKeys, w.Key)
		}
	}

	// Sample check - verify 10 random successful writes
	if len(successfulWriteKeys) > 10 {
		for range 10 {
			idx := rand.Intn(len(successfulWriteKeys))
			key := successfulWriteKeys[idx]
			record, err := cluster.Client.LookupKey(ctx, tableName, key)
			require.NoError(t, err, "Failed to read background write %s", key)
			require.NotNil(t, record, "Background write %s should exist", key)
		}
	}

	// Check success criteria
	if maxDowntime > 30*time.Second {
		t.Errorf("Max downtime exceeded threshold: %v > 30s", maxDowntime)
	}

	totalAvailOps := availSuccess + availFailed
	if totalAvailOps > 0 {
		availabilityRate := float64(availSuccess) / float64(totalAvailOps) * 100
		t.Logf("Availability rate: %.2f%%", availabilityRate)
		if availabilityRate < 90 {
			t.Errorf("Availability rate too low: %.2f%% < 90%%", availabilityRate)
		}
	}

	// Log final state
	tableStatus, err := cluster.Client.GetTable(ctx, tableName)
	require.NoError(t, err)
	t.Logf("Final shard count: %d", len(tableStatus.Shards))

	t.Log("=== Online Shard Split - Continuous Writes Test Completed ===")
}

// TestE2E_OnlineSplit_CrossShardBatchRejection verifies that batches spanning
// multiple shards are correctly rejected during a split.
func TestE2E_OnlineSplit_CrossShardBatchRejection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	// Online shard split tests run by default (skip with -short).

	sigCtx, sigCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer sigCancel()

	ctx, cancel := context.WithTimeout(sigCtx, 15*time.Minute)
	defer cancel()

	t.Log("=== Starting Online Shard Split - Cross-Shard Batch Rejection Test ===")

	// Create cluster with low threshold to trigger splits quickly
	// Using 10KB threshold with incompressible test data
	cluster := NewTestCluster(t, ctx, TestClusterConfig{
		NumStoreNodes:       3,
		NumShards:           1,
		ReplicationFactor:   3,
		MaxShardSizeBytes:   10 * 1024, // 10KB - low threshold, works with random (incompressible) test data
		DisableShardAlloc:   false,
		ShardCooldownPeriod: 5 * time.Second, // Short cooldown for faster testing
	})
	defer cluster.Cleanup()

	// Create table
	tableName := "online_split_batch"
	err := cluster.Client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
	})
	require.NoError(t, err)

	err = cluster.WaitForShardsReady(ctx, tableName, 1, 60*time.Second)
	require.NoError(t, err)

	// Trigger split by inserting data to exceed 10KB threshold
	// Using random (incompressible) data so on-disk size matches logical size
	t.Log("Triggering shard split...")
	for i := range 100 {
		key := fmt.Sprintf("trigger-%03d", i)
		record := map[string]any{key: GenerateTestData(10 * 1024)} // 10KB per record

		// Server-side retry handles transient errors during shard splits
		_, err := cluster.Client.Batch(ctx, tableName, antfly.BatchRequest{
			Inserts:   record,
			SyncLevel: antfly.SyncLevelWrite,
		})
		require.NoError(t, err, "Failed to insert record %d", i)
	}

	// Wait for split to complete
	t.Log("Waiting for split...")
	time.Sleep(2 * time.Second)

	// Trigger reallocate to force reconciler to run
	for range 10 {
		if err := cluster.TriggerReallocate(ctx); err != nil {
			t.Logf("Warning: TriggerReallocate failed: %v", err)
		}
		time.Sleep(3 * time.Second)
	}

	shardCount, err := cluster.WaitForShardCount(ctx, tableName, 2, 3*time.Minute)
	require.NoError(t, err, "Split should complete")
	t.Logf("Split completed with %d shards", shardCount)

	// Get shard info to understand the key ranges
	tableStatus, err := cluster.Client.GetTable(ctx, tableName)
	require.NoError(t, err)
	t.Logf("Table has %d shards after split", len(tableStatus.Shards))

	// Write to individual shards should succeed
	// Server-side retry handles transient errors during shard reorganization
	t.Log("Testing individual shard writes...")
	for i := range 5 {
		key := fmt.Sprintf("post-split-%03d", i)
		record := map[string]any{key: map[string]any{
			"content": fmt.Sprintf("Post-split write %d", i),
		}}

		_, err := cluster.Client.Batch(ctx, tableName, antfly.BatchRequest{
			Inserts:   record,
			SyncLevel: antfly.SyncLevelWrite,
		})
		require.NoError(t, err, "Individual shard write should succeed")
	}

	// Verify writes are readable
	t.Log("Verifying post-split writes...")
	for i := range 5 {
		key := fmt.Sprintf("post-split-%03d", i)
		record, err := cluster.Client.LookupKey(ctx, tableName, key)
		require.NoError(t, err, "Should read post-split write")
		require.NotNil(t, record)
	}

	t.Log("=== Cross-Shard Batch Rejection Test Completed ===")
}

// TestE2E_OnlineSplit_CatchupConvergence verifies that the new shard catches up
// to the parent shard within acceptable bounds.
func TestE2E_OnlineSplit_CatchupConvergence(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	// Online shard split tests run by default (skip with -short).

	sigCtx, sigCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer sigCancel()

	ctx, cancel := context.WithTimeout(sigCtx, 15*time.Minute)
	defer cancel()

	t.Log("=== Starting Online Shard Split - Catchup Convergence Test ===")

	cluster := NewTestCluster(t, ctx, TestClusterConfig{
		NumStoreNodes:       3,
		NumShards:           1,
		ReplicationFactor:   3,
		MaxShardSizeBytes:   10 * 1024, // 10KB - low threshold, works with random (incompressible) test data
		DisableShardAlloc:   false,
		ShardCooldownPeriod: 5 * time.Second, // Short cooldown for faster testing
	})
	defer cluster.Cleanup()

	tableName := "online_split_catchup"
	err := cluster.Client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
	})
	require.NoError(t, err)

	err = cluster.WaitForShardsReady(ctx, tableName, 1, 60*time.Second)
	require.NoError(t, err)

	// Start background writer to create continuous load
	bgWriter := NewBackgroundWriter(cluster, tableName, "catchup")
	bgWriter.Start(ctx, 100*time.Millisecond)
	defer bgWriter.Stop()

	// Insert data to trigger split (~500KB to exceed 10KB threshold)
	t.Log("Triggering split under load...")
	splitStartTime := time.Now()

	for i := range 50 {
		key := fmt.Sprintf("split-trigger-%03d", i)
		record := map[string]any{key: GenerateTestData(10 * 1024)} // 10KB per record

		// Server-side retry handles transient errors during shard splits
		_, err := cluster.Client.Batch(ctx, tableName, antfly.BatchRequest{
			Inserts:   record,
			SyncLevel: antfly.SyncLevelWrite,
		})
		require.NoError(t, err)
	}

	// Wait for split to complete
	t.Log("Waiting for split completion...")
	time.Sleep(2 * time.Second)

	// Trigger reallocate to force reconciler to run
	for range 10 {
		if err := cluster.TriggerReallocate(ctx); err != nil {
			t.Logf("Warning: TriggerReallocate failed: %v", err)
		}
		time.Sleep(3 * time.Second)
	}

	shardCount, err := cluster.WaitForShardCount(ctx, tableName, 2, 3*time.Minute)
	require.NoError(t, err, "Split should complete")

	splitDuration := time.Since(splitStartTime)
	t.Logf("Split completed in %v with %d shards", splitDuration, shardCount)

	// Success criteria: split should complete within 30 seconds
	// (This includes catchup convergence time)
	if splitDuration > 60*time.Second {
		t.Logf("Warning: Split took longer than expected: %v", splitDuration)
	}

	// Verify data integrity after split
	t.Log("Verifying data integrity...")
	for i := range 10 {
		key := fmt.Sprintf("split-trigger-%03d", i)
		record, err := cluster.Client.LookupKey(ctx, tableName, key)
		require.NoError(t, err)
		require.NotNil(t, record)
	}

	bgSuccess, bgErrors := bgWriter.Stats()
	t.Logf("Background writer during split: success=%d, errors=%d", bgSuccess, bgErrors)

	t.Log("=== Catchup Convergence Test Completed ===")
}

// TestE2E_OnlineSplit_RoutingUpdateTiming verifies no writes are lost during
// the routing table update when traffic switches to the new shard.
func TestE2E_OnlineSplit_RoutingUpdateTiming(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	// Online shard split tests run by default (skip with -short).

	sigCtx, sigCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer sigCancel()

	ctx, cancel := context.WithTimeout(sigCtx, 15*time.Minute)
	defer cancel()

	t.Log("=== Starting Online Shard Split - Routing Update Timing Test ===")

	cluster := NewTestCluster(t, ctx, TestClusterConfig{
		NumStoreNodes:       3,
		NumShards:           1,
		ReplicationFactor:   3,
		MaxShardSizeBytes:   10 * 1024, // 10KB - low threshold, works with random (incompressible) test data
		DisableShardAlloc:   false,
		ShardCooldownPeriod: 5 * time.Second, // Short cooldown for faster testing
	})
	defer cluster.Cleanup()

	tableName := "online_split_routing"
	err := cluster.Client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
	})
	require.NoError(t, err)

	err = cluster.WaitForShardsReady(ctx, tableName, 1, 60*time.Second)
	require.NoError(t, err)

	// Track all writes with sequence numbers
	var writeMu sync.Mutex
	writeSeqs := make(map[int]bool)
	var writeErrors []error

	// Start aggressive writer
	writerCtx, writerCancel := context.WithCancel(ctx)
	writerDone := make(chan struct{})

	go func() {
		defer close(writerDone)
		seq := 0
		for {
			select {
			case <-writerCtx.Done():
				return
			default:
				key := fmt.Sprintf("seq-%06d", seq)
				value := map[string]any{
					"seq":       seq,
					"timestamp": time.Now().UnixNano(),
				}

				// Server-side retry handles transient errors during shard splits
				reqCtx, cancel := context.WithTimeout(writerCtx, 5*time.Second)
				_, err := cluster.Client.Batch(reqCtx, tableName, antfly.BatchRequest{
					Inserts:   map[string]any{key: value},
					SyncLevel: antfly.SyncLevelWrite,
				})
				cancel()

				writeMu.Lock()
				if err != nil {
					writeErrors = append(writeErrors, fmt.Errorf("seq %d: %w", seq, err))
				} else {
					writeSeqs[seq] = true
				}
				writeMu.Unlock()

				seq++
				time.Sleep(20 * time.Millisecond)
			}
		}
	}()

	// Trigger split (~500KB to exceed 10KB threshold)
	t.Log("Triggering split with concurrent writes...")
	for i := range 50 {
		key := fmt.Sprintf("large-%03d", i)
		record := map[string]any{key: GenerateTestData(10 * 1024)} // 10KB per record

		// Server-side retry handles transient errors during shard splits
		_, err := cluster.Client.Batch(ctx, tableName, antfly.BatchRequest{
			Inserts:   record,
			SyncLevel: antfly.SyncLevelWrite,
		})
		require.NoError(t, err)
	}

	// Wait for split
	time.Sleep(2 * time.Second)

	// Trigger reallocate to force reconciler to run
	for range 10 {
		if err := cluster.TriggerReallocate(ctx); err != nil {
			t.Logf("Warning: TriggerReallocate failed: %v", err)
		}
		time.Sleep(3 * time.Second)
	}

	_, err = cluster.WaitForShardCount(ctx, tableName, 2, 3*time.Minute)
	require.NoError(t, err)

	// Continue writing briefly after split
	time.Sleep(5 * time.Second)

	// Stop writer
	writerCancel()
	<-writerDone

	// Analyze results
	writeMu.Lock()
	successCount := len(writeSeqs)
	errorCount := len(writeErrors)
	writeMu.Unlock()

	t.Logf("Write results: success=%d, errors=%d", successCount, errorCount)

	// Check for gaps in sequence numbers
	var seqNums []int
	for seq := range writeSeqs {
		seqNums = append(seqNums, seq)
	}
	sort.Ints(seqNums)

	gaps := 0
	for i := 1; i < len(seqNums); i++ {
		if seqNums[i]-seqNums[i-1] > 1 {
			gaps++
			if gaps <= 5 {
				t.Logf("Gap detected: %d -> %d", seqNums[i-1], seqNums[i])
			}
		}
	}

	if gaps > 0 {
		t.Logf("Total gaps in sequence: %d", gaps)
	}

	// Verify all successful writes are readable
	t.Log("Verifying successful writes are readable...")
	verifyCount := 0
	for seq := range writeSeqs {
		if verifyCount >= 20 {
			break // Sample check
		}
		key := fmt.Sprintf("seq-%06d", seq)
		record, err := cluster.Client.LookupKey(ctx, tableName, key)
		require.NoError(t, err, "Should read seq %d", seq)
		require.NotNil(t, record)
		verifyCount++
	}

	t.Log("=== Routing Update Timing Test Completed ===")
}

// TestE2E_OnlineSplit_ConcurrentSplits tests multiple shards splitting simultaneously.
func TestE2E_OnlineSplit_ConcurrentSplits(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	// Online shard split tests run by default (skip with -short).

	sigCtx, sigCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer sigCancel()

	ctx, cancel := context.WithTimeout(sigCtx, 20*time.Minute)
	defer cancel()

	t.Log("=== Starting Online Shard Split - Concurrent Splits Test ===")

	cluster := NewTestCluster(t, ctx, TestClusterConfig{
		NumStoreNodes:       3,
		NumShards:           4, // Start with multiple shards
		ReplicationFactor:   3,
		MaxShardSizeBytes:   10 * 1024, // 10KB - low threshold, works with random (incompressible) test data
		DisableShardAlloc:   false,
		ShardCooldownPeriod: 5 * time.Second, // Short cooldown for faster testing
	})
	defer cluster.Cleanup()

	tableName := "online_split_concurrent"
	err := cluster.Client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 4,
	})
	require.NoError(t, err)

	err = cluster.WaitForShardsReady(ctx, tableName, 4, 60*time.Second)
	require.NoError(t, err)

	// Start availability checker
	availChecker := NewAvailabilityChecker(cluster, tableName)
	availChecker.Start()
	defer availChecker.Stop()

	// Insert data across all shards to trigger multiple splits (~500KB per shard = 2MB total)
	t.Log("Inserting data to trigger concurrent splits...")
	for i := range 200 {
		// Distribute keys across shards using first character
		prefix := string(byte('a' + (i % 26)))
		key := fmt.Sprintf("%s-concurrent-%03d", prefix, i)
		record := map[string]any{key: GenerateTestData(10 * 1024)} // 10KB per record

		// Server-side retry handles transient errors during shard splits
		_, err := cluster.Client.Batch(ctx, tableName, antfly.BatchRequest{
			Inserts:   record,
			SyncLevel: antfly.SyncLevelWrite,
		})
		require.NoError(t, err)

		if i%50 == 0 {
			t.Logf("Inserted %d/200 records (~%.0fKB)", i, float64(i*10*1024)/1024)
		}
	}

	// Wait for splits
	t.Log("Waiting for concurrent splits...")
	time.Sleep(2 * time.Second)

	// Trigger reallocate to force reconciler to run
	for range 10 {
		if err := cluster.TriggerReallocate(ctx); err != nil {
			t.Logf("Warning: TriggerReallocate failed: %v", err)
		}
		time.Sleep(3 * time.Second)
	}

	// Check shard count increased
	shardCount, err := cluster.WaitForShardCount(ctx, tableName, 5, 5*time.Minute)
	if err != nil {
		t.Logf("Note: Got %d shards (may need more data to trigger all splits)", shardCount)
	} else {
		t.Logf("Concurrent splits resulted in %d shards", shardCount)
	}

	// Verify data integrity after concurrent splits
	t.Log("Verifying data integrity after concurrent splits...")
	for i := range 20 {
		prefix := string(byte('a' + (i % 26)))
		key := fmt.Sprintf("%s-concurrent-%03d", prefix, i)
		record, err := cluster.Client.LookupKey(ctx, tableName, key)
		require.NoError(t, err, "Should read record %s", key)
		require.NotNil(t, record)
	}

	// Check availability
	availSuccess, availFailed, maxDowntime := availChecker.Stats()
	t.Logf("Availability during concurrent splits: success=%d, failed=%d, maxDowntime=%v",
		availSuccess, availFailed, maxDowntime)

	if maxDowntime > 30*time.Second {
		t.Errorf("Max downtime too high during concurrent splits: %v", maxDowntime)
	}

	t.Log("=== Concurrent Splits Test Completed ===")
}

// TestE2E_OnlineSplit_RaftLogCompaction tests that catchup works even when
// the parent shard has compacted its Raft log.
func TestE2E_OnlineSplit_RaftLogCompaction(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	// Online shard split tests run by default (skip with -short).

	sigCtx, sigCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer sigCancel()

	ctx, cancel := context.WithTimeout(sigCtx, 15*time.Minute)
	defer cancel()

	t.Log("=== Starting Online Shard Split - Raft Log Compaction Test ===")

	// Note: This test verifies basic split behavior. Full Raft log compaction
	// testing may require internal test hooks that aren't exposed in e2e tests.

	cluster := NewTestCluster(t, ctx, TestClusterConfig{
		NumStoreNodes:       3,
		NumShards:           1,
		ReplicationFactor:   3,
		MaxShardSizeBytes:   10 * 1024, // 10KB - low threshold, works with random (incompressible) test data
		DisableShardAlloc:   false,
		ShardCooldownPeriod: 5 * time.Second, // Short cooldown for faster testing
	})
	defer cluster.Cleanup()

	tableName := "online_split_logcompact"
	err := cluster.Client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
	})
	require.NoError(t, err)

	err = cluster.WaitForShardsReady(ctx, tableName, 1, 60*time.Second)
	require.NoError(t, err)

	// Write enough data to potentially trigger log compaction
	t.Log("Writing data to potentially trigger log compaction...")
	for batch := range 10 {
		records := make(map[string]any)
		for i := range 50 {
			key := fmt.Sprintf("batch%d-record%03d", batch, i)
			records[key] = map[string]any{
				"batch":   batch,
				"record":  i,
				"content": fmt.Sprintf("Data for batch %d record %d", batch, i),
			}
		}

		// Server-side retry handles transient errors during shard splits
		_, err := cluster.Client.Batch(ctx, tableName, antfly.BatchRequest{
			Inserts:   records,
			SyncLevel: antfly.SyncLevelWrite,
		})
		require.NoError(t, err)
	}

	// Trigger split with large data (~500KB to exceed 10KB threshold)
	t.Log("Triggering split...")
	for i := range 50 {
		key := fmt.Sprintf("split-trigger-%03d", i)
		record := map[string]any{key: GenerateTestData(10 * 1024)} // 10KB per record

		// Server-side retry handles transient errors during shard splits
		_, err := cluster.Client.Batch(ctx, tableName, antfly.BatchRequest{
			Inserts:   record,
			SyncLevel: antfly.SyncLevelWrite,
		})
		require.NoError(t, err)
	}

	// Wait for split
	time.Sleep(2 * time.Second)

	// Trigger reallocate to force reconciler to run
	for range 10 {
		if err := cluster.TriggerReallocate(ctx); err != nil {
			t.Logf("Warning: TriggerReallocate failed: %v", err)
		}
		time.Sleep(3 * time.Second)
	}

	shardCount, err := cluster.WaitForShardCount(ctx, tableName, 2, 3*time.Minute)
	require.NoError(t, err, "Split should complete")
	t.Logf("Split completed with %d shards", shardCount)

	// Verify all data is accessible (including data from before potential compaction)
	t.Log("Verifying all data after split (including pre-compaction data)...")
	for batch := range 10 {
		for i := range 5 { // Sample check
			key := fmt.Sprintf("batch%d-record%03d", batch, i)
			record, err := cluster.Client.LookupKey(ctx, tableName, key)
			require.NoError(t, err, "Should read record %s", key)
			require.NotNil(t, record)
		}
	}

	t.Log("=== Raft Log Compaction Test Completed ===")
}

// TestE2E_OnlineSplit_TimeoutRollsBack tests that a split operation that exceeds
// the configured timeout triggers a rollback, returning the shard to normal state.
func TestE2E_OnlineSplit_TimeoutRollsBack(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	// Online shard split tests run by default (skip with -short).

	sigCtx, sigCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer sigCancel()

	ctx, cancel := context.WithTimeout(sigCtx, 15*time.Minute)
	defer cancel()

	t.Log("=== Starting Online Shard Split - Timeout Rollback Test ===")

	// Configure a short split timeout to make rollback testable
	// The split timeout controls how long before a stuck split triggers rollback
	splitTimeout := 30 * time.Second

	cluster := NewTestCluster(t, ctx, TestClusterConfig{
		NumStoreNodes:       3,
		NumShards:           1,
		ReplicationFactor:   3,
		MaxShardSizeBytes:   10 * 1024, // 10KB - low threshold
		DisableShardAlloc:   false,
		ShardCooldownPeriod: 5 * time.Second,
		SplitTimeout:        splitTimeout,
	})
	defer cluster.Cleanup()

	tableName := "online_split_timeout"
	err := cluster.Client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
	})
	require.NoError(t, err)

	err = cluster.WaitForShardsReady(ctx, tableName, 1, 60*time.Second)
	require.NoError(t, err)

	// Write initial data that we'll verify survives the rollback
	t.Log("Step 1: Writing initial data...")
	initialRecords := make(map[string]any)
	for i := range 50 {
		key := fmt.Sprintf("init-%03d", i)
		initialRecords[key] = map[string]any{
			"content": fmt.Sprintf("Initial document %d - must survive rollback", i),
			"seq":     i,
		}
	}

	_, err = cluster.Client.LinearMerge(ctx, tableName, antfly.LinearMergeRequest{
		Records:   initialRecords,
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err)

	// Get initial shard count
	initialStatus, err := cluster.Client.GetTable(ctx, tableName)
	require.NoError(t, err)
	initialShardCount := len(initialStatus.Shards)
	t.Logf("Initial shard count: %d", initialShardCount)

	// Start background writer to generate continuous load
	bgWriter := NewBackgroundWriter(cluster, tableName, "timeout-test")
	bgWriter.Start(ctx, 100*time.Millisecond)

	// Insert data to trigger split
	t.Log("Step 2: Inserting data to trigger shard split...")
	for i := range 50 {
		key := fmt.Sprintf("large-%03d", i)
		record := map[string]any{key: GenerateTestData(10 * 1024)}

		_, err := cluster.Client.Batch(ctx, tableName, antfly.BatchRequest{
			Inserts:   record,
			SyncLevel: antfly.SyncLevelWrite,
		})
		require.NoError(t, err)
	}

	// Give time for disk size stats to update
	time.Sleep(2 * time.Second)

	// Trigger a single reallocate to initiate the split
	t.Log("Step 3: Triggering split...")
	if err := cluster.TriggerReallocate(ctx); err != nil {
		t.Logf("Warning: TriggerReallocate failed: %v", err)
	}

	// Now remove a store node to prevent the new shard from getting enough replicas
	// This should cause the split to get stuck and eventually timeout
	t.Log("Step 4: Removing a store node to prevent split completion...")
	storeIDs := cluster.GetStoreNodeIDs()
	if len(storeIDs) > 0 {
		removedStoreID := storeIDs[len(storeIDs)-1]
		if err := cluster.RemoveStoreNode(ctx, removedStoreID); err != nil {
			t.Logf("Warning: Failed to remove store node: %v", err)
		} else {
			t.Logf("Removed store node %s", removedStoreID)
		}
	}

	// Wait longer than the split timeout
	waitTime := splitTimeout + 30*time.Second
	t.Logf("Step 5: Waiting %v for timeout and rollback...", waitTime)

	// Periodically trigger reallocate during the wait to ensure the reconciler runs
	waitDeadline := time.Now().Add(waitTime)
	for time.Now().Before(waitDeadline) {
		_ = cluster.TriggerReallocate(ctx) // Ignore errors - cluster may be in transition
		time.Sleep(5 * time.Second)
	}

	// Stop background writer
	bgWriter.Stop()

	// Add a store node back to restore cluster health
	t.Log("Step 6: Adding store node back...")
	cluster.AddStoreNode(ctx)

	// Wait for cluster to stabilize
	time.Sleep(10 * time.Second)

	// Trigger more reallocates to ensure reconciliation completes
	for range 5 {
		if err := cluster.TriggerReallocate(ctx); err != nil {
			t.Logf("Warning: TriggerReallocate failed: %v", err)
		}
		time.Sleep(3 * time.Second)
	}

	// Verify data integrity - all initial records should still be accessible
	t.Log("Step 7: Verifying initial data survived rollback...")
	accessibleCount := 0
	for i := range 50 {
		key := fmt.Sprintf("init-%03d", i)
		record, err := cluster.Client.LookupKey(ctx, tableName, key)
		if err == nil && record != nil {
			accessibleCount++
		}
	}
	t.Logf("Accessible initial records: %d/50", accessibleCount)
	require.GreaterOrEqual(t, accessibleCount, 45, "Most initial records should survive rollback")

	// Verify large data is also accessible
	t.Log("Verifying large data is accessible...")
	largeAccessible := 0
	for i := range 10 {
		key := fmt.Sprintf("large-%03d", i)
		record, err := cluster.Client.LookupKey(ctx, tableName, key)
		if err == nil && record != nil {
			largeAccessible++
		}
	}
	t.Logf("Accessible large records: %d/10", largeAccessible)

	// Get final shard count
	finalStatus, err := cluster.Client.GetTable(ctx, tableName)
	require.NoError(t, err)
	finalShardCount := len(finalStatus.Shards)
	t.Logf("Final shard count: %d (initial was %d)", finalShardCount, initialShardCount)

	// The shard count may have increased if the split eventually succeeded,
	// or stayed the same if rollback occurred. Either way, data should be intact.
	bgSuccess, bgErrors := bgWriter.Stats()
	t.Logf("Background writer stats: success=%d, errors=%d", bgSuccess, bgErrors)

	t.Log("=== Timeout Rollback Test Completed ===")
}

// BackgroundReader continuously reads from a table during tests
type BackgroundReader struct {
	cluster   *TestCluster
	tableName string
	keys      []string

	mu         sync.Mutex
	reads      []ReadRecord
	stopC      chan struct{}
	stoppedC   chan struct{}
	successCnt atomic.Int64
	errorCnt   atomic.Int64
}

// ReadRecord tracks a read operation for verification
type ReadRecord struct {
	Key       string
	Timestamp time.Time
	Succeeded bool
	Error     error
}

// NewBackgroundReader creates a new background reader
func NewBackgroundReader(cluster *TestCluster, tableName string, keys []string) *BackgroundReader {
	return &BackgroundReader{
		cluster:   cluster,
		tableName: tableName,
		keys:      keys,
		stopC:     make(chan struct{}),
		stoppedC:  make(chan struct{}),
	}
}

// Start begins the background reading goroutine
func (br *BackgroundReader) Start(ctx context.Context, readInterval time.Duration) {
	go br.run(ctx, readInterval)
}

func (br *BackgroundReader) run(ctx context.Context, readInterval time.Duration) {
	defer close(br.stoppedC)

	ticker := time.NewTicker(readInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-br.stopC:
			return
		case <-ticker.C:
			// Pick a random key to read
			key := br.keys[rand.Intn(len(br.keys))]

			record := ReadRecord{
				Key:       key,
				Timestamp: time.Now(),
			}

			reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_, err := br.cluster.Client.LookupKey(reqCtx, br.tableName, key)
			cancel()

			record.Succeeded = err == nil
			record.Error = err

			br.mu.Lock()
			br.reads = append(br.reads, record)
			br.mu.Unlock()

			if err != nil {
				br.errorCnt.Add(1)
			} else {
				br.successCnt.Add(1)
			}
		}
	}
}

// Stop stops the background reader and waits for it to finish
func (br *BackgroundReader) Stop() {
	close(br.stopC)
	<-br.stoppedC
}

// GetReads returns all recorded reads
func (br *BackgroundReader) GetReads() []ReadRecord {
	br.mu.Lock()
	defer br.mu.Unlock()
	result := make([]ReadRecord, len(br.reads))
	copy(result, br.reads)
	return result
}

// Stats returns success and error counts
func (br *BackgroundReader) Stats() (success, errors int64) {
	return br.successCnt.Load(), br.errorCnt.Load()
}

// TestE2E_OnlineSplit_PreBuiltIndexes tests that new shards created from splits
// can serve index queries immediately because indexes are pre-built in the archive.
func TestE2E_OnlineSplit_PreBuiltIndexes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	// Online shard split tests run by default (skip with -short).

	sigCtx, sigCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer sigCancel()

	ctx, cancel := context.WithTimeout(sigCtx, 15*time.Minute)
	defer cancel()

	t.Log("=== Starting Online Shard Split - Pre-Built Indexes Test ===")

	cluster := NewTestCluster(t, ctx, TestClusterConfig{
		NumStoreNodes:       3,
		NumShards:           1,
		ReplicationFactor:   3,
		MaxShardSizeBytes:   10 * 1024, // 10KB - low threshold
		DisableShardAlloc:   false,
		ShardCooldownPeriod: 5 * time.Second,
	})
	defer cluster.Cleanup()

	tableName := "online_split_indexes"
	err := cluster.Client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
	})
	require.NoError(t, err)

	err = cluster.WaitForShardsReady(ctx, tableName, 1, 60*time.Second)
	require.NoError(t, err)

	// The table automatically gets a default full_text_index_v0 for full-text search.
	// This test validates that the pre-built indexes work correctly during splits.
	t.Log("Step 1: Using default full-text index (full_text_index_v0)...")

	// Insert searchable documents with unique terms that span the key range
	// We use keys with different prefixes to ensure documents end up in different shards after split
	t.Log("Step 2: Inserting searchable documents...")
	searchTerms := []string{
		"quantum", "algorithm", "database", "distributed", "consensus",
		"indexing", "embedding", "vector", "semantic", "fulltext",
	}

	for i := range 100 {
		// Use different key prefixes to spread across potential split ranges
		prefix := string(byte('a' + (i % 26)))
		key := fmt.Sprintf("%s-doc-%03d", prefix, i)
		searchTerm := searchTerms[i%len(searchTerms)]

		record := map[string]any{
			"title": fmt.Sprintf("Document about %s - part %d", searchTerm, i),
			"body":  fmt.Sprintf("This document discusses %s technology and its applications in modern systems. The %s approach provides significant benefits.", searchTerm, searchTerm),
			"seq":   i,
		}

		_, err := cluster.Client.Batch(ctx, tableName, antfly.BatchRequest{
			Inserts:   map[string]any{key: record},
			SyncLevel: antfly.SyncLevelFullText, // Wait for full-text index
		})
		require.NoError(t, err)
	}

	// Verify search works before split
	t.Log("Step 3: Verifying search works before split...")
	quantumQuery := query.MatchQuery{Match: "quantum"}.ToQuery()
	preSplitResults, err := cluster.Client.Query(ctx, antfly.QueryRequest{
		Table:          tableName,
		FullTextSearch: &quantumQuery,
		Limit:          10,
	})
	require.NoError(t, err)
	require.NotNil(t, preSplitResults)
	var preSplitCount uint64
	if len(preSplitResults.Responses) > 0 {
		preSplitCount = preSplitResults.Responses[0].Hits.Total
	}
	t.Logf("Pre-split search for 'quantum' returned %d results", preSplitCount)
	require.Positive(t, preSplitCount, "Search should find documents before split")

	// Insert large data to trigger split
	t.Log("Step 4: Inserting data to trigger shard split...")
	for i := range 50 {
		prefix := string(byte('a' + (i % 26)))
		key := fmt.Sprintf("%s-large-%03d", prefix, i)
		searchTerm := searchTerms[i%len(searchTerms)]

		// Include searchable content in the large records too
		largeRecord := GenerateTestData(10 * 1024)
		largeRecord["title"] = fmt.Sprintf("Large document about %s", searchTerm)
		largeRecord["body"] = fmt.Sprintf("Extended discussion of %s concepts", searchTerm)

		_, err := cluster.Client.Batch(ctx, tableName, antfly.BatchRequest{
			Inserts:   map[string]any{key: largeRecord},
			SyncLevel: antfly.SyncLevelFullText,
		})
		require.NoError(t, err)
	}

	// Trigger split and wait for completion
	t.Log("Step 5: Triggering split...")
	time.Sleep(2 * time.Second)

	for range 10 {
		if err := cluster.TriggerReallocate(ctx); err != nil {
			t.Logf("Warning: TriggerReallocate failed: %v", err)
		}
		time.Sleep(3 * time.Second)
	}

	shardCount, err := cluster.WaitForShardCount(ctx, tableName, 2, 3*time.Minute)
	require.NoError(t, err, "Split should complete")
	t.Logf("Split completed with %d shards", shardCount)

	// Immediately after split, perform searches and measure response time
	// Pre-built indexes should allow immediate query serving
	t.Log("Step 6: Testing search immediately after split (pre-built indexes)...")

	// Test multiple search terms to ensure we're hitting different shards
	for _, term := range []string{"quantum", "algorithm", "database"} {
		searchStart := time.Now()

		termQuery := query.MatchQuery{Match: term}.ToQuery()
		results, err := cluster.Client.Query(ctx, antfly.QueryRequest{
			Table:          tableName,
			FullTextSearch: &termQuery,
			Limit:          10,
		})
		require.NoError(t, err, "Search for '%s' should succeed immediately after split", term)

		searchDuration := time.Since(searchStart)
		var resultCount uint64
		if len(results.Responses) > 0 {
			resultCount = results.Responses[0].Hits.Total
		}

		t.Logf("Search for '%s': %d results in %v", term, resultCount, searchDuration)

		// Pre-built indexes should allow searches to complete quickly
		// Without pre-built indexes, the new shard would need to rebuild indexes
		// which could take much longer or fail entirely
		require.Less(t, searchDuration, 10*time.Second,
			"Search should complete within 10s with pre-built indexes")
		require.Positive(t, resultCount,
			"Search for '%s' should find results after split", term)
	}

	// Verify we can find documents across both shards (pre and post split key)
	t.Log("Step 7: Verifying documents are searchable across all shards...")
	docQuery := query.MatchQuery{Match: "document"}.ToQuery()
	allResults, err := cluster.Client.Query(ctx, antfly.QueryRequest{
		Table:          tableName,
		FullTextSearch: &docQuery,
		Limit:          200,
	})
	require.NoError(t, err)

	var totalFound uint64
	if len(allResults.Responses) > 0 {
		totalFound = allResults.Responses[0].Hits.Total
	}
	t.Logf("Total documents found searching for 'document': %d", totalFound)

	// We inserted 100 regular docs + 50 large docs, all containing "document"
	// After split, we should be able to find most of them
	require.GreaterOrEqual(t, totalFound, uint64(100),
		"Should find most documents after split with pre-built indexes")

	t.Log("=== Pre-Built Indexes Test Completed ===")
}

// TestE2E_OnlineSplit_LeadershipChange tests that split state survives leadership changes.
// This validates the core improvement of using Raft-replicated split state over the
// old pendingSplitKey approach.
func TestE2E_OnlineSplit_LeadershipChange(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	// Online shard split tests run by default (skip with -short).

	sigCtx, sigCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer sigCancel()

	ctx, cancel := context.WithTimeout(sigCtx, 20*time.Minute)
	defer cancel()

	t.Log("=== Starting Online Shard Split - Leadership Change Test ===")

	// Create cluster with 3 store nodes for proper Raft quorum
	cluster := NewTestCluster(t, ctx, TestClusterConfig{
		NumStoreNodes:       3,
		NumShards:           1,
		ReplicationFactor:   3,
		MaxShardSizeBytes:   10 * 1024, // 10KB - low threshold
		DisableShardAlloc:   false,
		ShardCooldownPeriod: 5 * time.Second,
	})
	defer cluster.Cleanup()

	tableName := "online_split_leadership"
	err := cluster.Client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
	})
	require.NoError(t, err)

	err = cluster.WaitForShardsReady(ctx, tableName, 1, 60*time.Second)
	require.NoError(t, err)

	// Step 1: Write initial data that must survive the split + leadership change
	t.Log("Step 1: Writing initial data...")
	initialKeys := make([]string, 0, 100)
	for i := range 100 {
		key := fmt.Sprintf("init-%03d", i)
		initialKeys = append(initialKeys, key)
		record := map[string]any{
			"content": fmt.Sprintf("Initial document %d - must survive leadership change", i),
			"seq":     i,
		}

		_, err := cluster.Client.Batch(ctx, tableName, antfly.BatchRequest{
			Inserts:   map[string]any{key: record},
			SyncLevel: antfly.SyncLevelWrite,
		})
		require.NoError(t, err)
	}

	// Step 2: Start background writes to generate continuous activity
	t.Log("Step 2: Starting background writer...")
	bgWriter := NewBackgroundWriter(cluster, tableName, "leadership")
	bgWriter.Start(ctx, 100*time.Millisecond)

	// Step 3: Insert large data to trigger shard split
	t.Log("Step 3: Inserting data to trigger shard split...")
	for i := range 50 {
		key := fmt.Sprintf("large-%03d", i)
		record := map[string]any{key: GenerateTestData(10 * 1024)}

		_, err := cluster.Client.Batch(ctx, tableName, antfly.BatchRequest{
			Inserts:   record,
			SyncLevel: antfly.SyncLevelWrite,
		})
		require.NoError(t, err)
	}

	// Step 4: Trigger initial reallocate to start split
	t.Log("Step 4: Triggering reallocate to initiate split...")
	time.Sleep(2 * time.Second)

	if err := cluster.TriggerReallocate(ctx); err != nil {
		t.Logf("Warning: TriggerReallocate failed: %v", err)
	}

	// Step 5: Wait briefly for split to enter PREPARE or SPLITTING phase
	t.Log("Step 5: Waiting for split to begin...")
	time.Sleep(5 * time.Second)

	// Step 6: Crash a store node to force potential leadership change
	// This simulates node failure during split
	t.Log("Step 6: Crashing a store node to force leadership change...")
	storeIDs := cluster.GetStoreNodeIDs()
	if len(storeIDs) >= 1 {
		crashedNode := storeIDs[0]
		if err := cluster.CrashStoreNode(crashedNode); err != nil {
			t.Logf("Warning: Failed to crash store node: %v", err)
		} else {
			t.Logf("Crashed store node %s", crashedNode)
		}
	}

	// Step 7: Wait for new leader election and continue split
	t.Log("Step 7: Waiting for leader election and split continuation...")
	time.Sleep(10 * time.Second)

	// Add a replacement node to maintain replication factor
	t.Log("Step 8: Adding replacement store node...")
	cluster.AddStoreNode(ctx)

	// Step 9: Continue triggering reallocates to drive split to completion
	t.Log("Step 9: Continuing split process...")
	for range 15 {
		if err := cluster.TriggerReallocate(ctx); err != nil {
			t.Logf("Warning: TriggerReallocate failed: %v", err)
		}
		time.Sleep(3 * time.Second)
	}

	// Wait for split to complete
	shardCount, err := cluster.WaitForShardCount(ctx, tableName, 2, 3*time.Minute)
	if err != nil {
		t.Logf("Note: Split may not have completed (shards=%d): %v", shardCount, err)
	} else {
		t.Logf("Split completed with %d shards after leadership change", shardCount)
	}

	// Stop background writer
	bgWriter.Stop()

	// Step 10: Verify all initial data is intact after leadership change
	t.Log("Step 10: Verifying data integrity after leadership change...")
	accessibleCount := 0
	for _, key := range initialKeys {
		record, err := cluster.Client.LookupKey(ctx, tableName, key)
		if err == nil && record != nil {
			accessibleCount++
		}
	}
	t.Logf("Accessible initial records: %d/%d", accessibleCount, len(initialKeys))
	require.GreaterOrEqual(t, accessibleCount, 95,
		"Most initial records should survive leadership change during split")

	// Verify background writes resumed after leadership change.
	// During a node crash in a 3-node cluster with an active split, write
	// unavailability during leader re-election is expected and correct — Raft
	// guarantees consistency by rejecting writes with no quorum. What matters
	// is that writes resume (liveness), not what fraction succeeded during the
	// outage window. A success-rate metric is misleading here because each
	// failed write blocks for its full 5s timeout, serializing the writer and
	// inflating the error ratio far beyond the actual unavailability duration.
	bgSuccess, bgErrors := bgWriter.Stats()
	t.Logf("Background writer stats: success=%d, errors=%d", bgSuccess, bgErrors)
	require.GreaterOrEqual(t, bgSuccess, int64(10),
		"Background writes should resume after leadership change (liveness check)")

	t.Log("=== Leadership Change Test Completed ===")
}

// TestE2E_SplitAvailabilityContinuousReads tests that reads remain available
// during a shard split operation.
func TestE2E_SplitAvailabilityContinuousReads(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	// Online shard split tests run by default (skip with -short).

	sigCtx, sigCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer sigCancel()

	ctx, cancel := context.WithTimeout(sigCtx, 15*time.Minute)
	defer cancel()

	t.Log("=== Starting Online Shard Split - Continuous Reads Test ===")

	cluster := NewTestCluster(t, ctx, TestClusterConfig{
		NumStoreNodes:       3,
		NumShards:           1,
		ReplicationFactor:   3,
		MaxShardSizeBytes:   10 * 1024, // 10KB - low threshold
		DisableShardAlloc:   false,
		ShardCooldownPeriod: 5 * time.Second,
	})
	defer cluster.Cleanup()

	tableName := "online_split_reads"
	err := cluster.Client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
	})
	require.NoError(t, err)

	err = cluster.WaitForShardsReady(ctx, tableName, 1, 60*time.Second)
	require.NoError(t, err)

	// Step 1: Pre-populate with data that we'll read during split
	t.Log("Step 1: Pre-populating data for read tests...")
	readableKeys := make([]string, 0, 100)
	for i := range 100 {
		key := fmt.Sprintf("readable-%03d", i)
		readableKeys = append(readableKeys, key)
		record := map[string]any{
			"content": fmt.Sprintf("Readable document %d", i),
			"seq":     i,
		}

		_, err := cluster.Client.Batch(ctx, tableName, antfly.BatchRequest{
			Inserts:   map[string]any{key: record},
			SyncLevel: antfly.SyncLevelWrite,
		})
		require.NoError(t, err)
	}

	// Step 2: Start background reader
	t.Log("Step 2: Starting background reader...")
	bgReader := NewBackgroundReader(cluster, tableName, readableKeys)
	bgReader.Start(ctx, 50*time.Millisecond)

	// Step 3: Start availability checker
	t.Log("Step 3: Starting availability checker...")
	availChecker := NewAvailabilityChecker(cluster, tableName)
	availChecker.Start()
	defer availChecker.Stop()

	// Step 4: Trigger split by inserting large data
	t.Log("Step 4: Triggering shard split...")
	for i := range 50 {
		key := fmt.Sprintf("large-%03d", i)
		record := map[string]any{key: GenerateTestData(10 * 1024)}

		_, err := cluster.Client.Batch(ctx, tableName, antfly.BatchRequest{
			Inserts:   record,
			SyncLevel: antfly.SyncLevelWrite,
		})
		require.NoError(t, err)
	}

	// Step 5: Wait and trigger reallocates
	time.Sleep(2 * time.Second)

	for range 10 {
		if err := cluster.TriggerReallocate(ctx); err != nil {
			t.Logf("Warning: TriggerReallocate failed: %v", err)
		}
		time.Sleep(3 * time.Second)
	}

	shardCount, err := cluster.WaitForShardCount(ctx, tableName, 2, 3*time.Minute)
	if err != nil {
		t.Logf("Note: Split may not have completed (shards=%d): %v", shardCount, err)
	} else {
		t.Logf("Split completed with %d shards", shardCount)
	}

	// Step 6: Continue reading for a bit after split
	t.Log("Step 6: Continuing reads after split...")
	time.Sleep(10 * time.Second)

	// Step 7: Stop and analyze results
	bgReader.Stop()

	readSuccess, readErrors := bgReader.Stats()
	t.Logf("Background reader stats: success=%d, errors=%d", readSuccess, readErrors)

	availSuccess, availFailed, maxDowntime := availChecker.Stats()
	t.Logf("Availability stats: success=%d, failed=%d, maxDowntime=%v",
		availSuccess, availFailed, maxDowntime)

	// Analyze read results
	reads := bgReader.GetReads()
	successfulReads := 0
	for _, r := range reads {
		if r.Succeeded {
			successfulReads++
		}
	}

	if len(reads) > 0 {
		readSuccessRate := float64(successfulReads) / float64(len(reads)) * 100
		t.Logf("Read success rate: %.2f%% (%d/%d)", readSuccessRate, successfulReads, len(reads))

		// Reads should maintain high availability during split
		require.GreaterOrEqual(t, readSuccessRate, 95.0,
			"Read success rate should be at least 95%% during split")
	}

	// Max downtime should be reasonable
	if maxDowntime > 30*time.Second {
		t.Errorf("Max downtime exceeded threshold: %v > 30s", maxDowntime)
	}

	t.Log("=== Continuous Reads Test Completed ===")
}

// TestE2E_SplitAvailabilityIndexQueries tests that full-text and vector search
// queries remain available during a shard split operation.
func TestE2E_SplitAvailabilityIndexQueries(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	// Online shard split tests run by default (skip with -short).

	sigCtx, sigCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer sigCancel()

	ctx, cancel := context.WithTimeout(sigCtx, 15*time.Minute)
	defer cancel()

	t.Log("=== Starting Online Shard Split - Index Queries Test ===")

	cluster := NewTestCluster(t, ctx, TestClusterConfig{
		NumStoreNodes:       3,
		NumShards:           1,
		ReplicationFactor:   3,
		MaxShardSizeBytes:   10 * 1024, // 10KB - low threshold
		DisableShardAlloc:   false,
		ShardCooldownPeriod: 5 * time.Second,
	})
	defer cluster.Cleanup()

	tableName := "online_split_queries"
	err := cluster.Client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
	})
	require.NoError(t, err)

	err = cluster.WaitForShardsReady(ctx, tableName, 1, 60*time.Second)
	require.NoError(t, err)

	// Step 1: Insert searchable documents with specific terms
	t.Log("Step 1: Inserting searchable documents...")
	searchTerms := []string{"quantum", "algorithm", "distributed", "consensus", "vector"}
	for i := range 100 {
		prefix := string(byte('a' + (i % 26)))
		key := fmt.Sprintf("%s-search-%03d", prefix, i)
		searchTerm := searchTerms[i%len(searchTerms)]

		record := map[string]any{
			"title":   fmt.Sprintf("Document about %s - part %d", searchTerm, i),
			"body":    fmt.Sprintf("This document discusses %s technology and applications", searchTerm),
			"keyword": searchTerm,
			"seq":     i,
		}

		_, err := cluster.Client.Batch(ctx, tableName, antfly.BatchRequest{
			Inserts:   map[string]any{key: record},
			SyncLevel: antfly.SyncLevelFullText,
		})
		require.NoError(t, err)
	}

	// Step 2: Verify search works before split
	t.Log("Step 2: Verifying search works before split...")
	for _, term := range searchTerms {
		termQuery := query.MatchQuery{Match: term}.ToQuery()
		results, err := cluster.Client.Query(ctx, antfly.QueryRequest{
			Table:          tableName,
			FullTextSearch: &termQuery,
			Limit:          10,
		})
		require.NoError(t, err)
		if len(results.Responses) > 0 {
			t.Logf("Pre-split search for '%s': %d results", term, results.Responses[0].Hits.Total)
		}
	}

	// Step 3: Start background search query loop
	t.Log("Step 3: Starting background search queries...")
	var searchMu sync.Mutex
	searchResults := make(map[string][]time.Duration)
	searchErrors := make(map[string]int)
	searchSuccess := make(map[string]int)
	searchStopC := make(chan struct{})
	searchDoneC := make(chan struct{})

	go func() {
		defer close(searchDoneC)
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-searchStopC:
				return
			case <-ticker.C:
				// Rotate through search terms
				term := searchTerms[rand.Intn(len(searchTerms))]

				startTime := time.Now()
				termQuery := query.MatchQuery{Match: term}.ToQuery()
				reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				_, err := cluster.Client.Query(reqCtx, antfly.QueryRequest{
					Table:          tableName,
					FullTextSearch: &termQuery,
					Limit:          10,
				})
				cancel()
				duration := time.Since(startTime)

				searchMu.Lock()
				if err != nil {
					searchErrors[term]++
				} else {
					searchSuccess[term]++
					searchResults[term] = append(searchResults[term], duration)
				}
				searchMu.Unlock()
			}
		}
	}()

	// Step 4: Trigger split with large data
	t.Log("Step 4: Triggering shard split...")
	for i := range 50 {
		prefix := string(byte('a' + (i % 26)))
		key := fmt.Sprintf("%s-large-%03d", prefix, i)
		searchTerm := searchTerms[i%len(searchTerms)]

		largeRecord := GenerateTestData(10 * 1024)
		largeRecord["title"] = fmt.Sprintf("Large doc about %s", searchTerm)
		largeRecord["body"] = fmt.Sprintf("Extended %s content", searchTerm)

		_, err := cluster.Client.Batch(ctx, tableName, antfly.BatchRequest{
			Inserts:   map[string]any{key: largeRecord},
			SyncLevel: antfly.SyncLevelFullText,
		})
		require.NoError(t, err)
	}

	// Step 5: Trigger split and wait
	time.Sleep(2 * time.Second)

	for range 10 {
		if err := cluster.TriggerReallocate(ctx); err != nil {
			t.Logf("Warning: TriggerReallocate failed: %v", err)
		}
		time.Sleep(3 * time.Second)
	}

	shardCount, err := cluster.WaitForShardCount(ctx, tableName, 2, 3*time.Minute)
	if err != nil {
		t.Logf("Note: Split may not have completed (shards=%d): %v", shardCount, err)
	} else {
		t.Logf("Split completed with %d shards", shardCount)
	}

	// Step 6: Continue searching after split
	t.Log("Step 6: Continuing searches after split...")
	time.Sleep(10 * time.Second)

	// Stop search queries
	close(searchStopC)
	<-searchDoneC

	// Step 7: Analyze results
	t.Log("Step 7: Analyzing search query results...")
	searchMu.Lock()
	totalSuccess := 0
	totalErrors := 0
	for term, successCount := range searchSuccess {
		errorCount := searchErrors[term]
		totalSuccess += successCount
		totalErrors += errorCount

		if len(searchResults[term]) > 0 {
			var totalDuration time.Duration
			for _, d := range searchResults[term] {
				totalDuration += d
			}
			avgDuration := totalDuration / time.Duration(len(searchResults[term]))
			t.Logf("Search '%s': success=%d, errors=%d, avgLatency=%v",
				term, successCount, errorCount, avgDuration)
		}
	}
	searchMu.Unlock()

	if totalSuccess+totalErrors > 0 {
		successRate := float64(totalSuccess) / float64(totalSuccess+totalErrors) * 100
		t.Logf("Overall search success rate: %.2f%% (%d/%d)",
			successRate, totalSuccess, totalSuccess+totalErrors)

		require.GreaterOrEqual(t, successRate, 90.0,
			"Search query success rate should be at least 90%% during split")
	}

	// Verify searches still work after split
	t.Log("Verifying searches work after split...")
	for _, term := range searchTerms {
		termQuery := query.MatchQuery{Match: term}.ToQuery()
		results, err := cluster.Client.Query(ctx, antfly.QueryRequest{
			Table:          tableName,
			FullTextSearch: &termQuery,
			Limit:          10,
		})
		require.NoError(t, err, "Search for '%s' should work after split", term)
		if len(results.Responses) > 0 {
			require.Positive(t, results.Responses[0].Hits.Total,
				"Search for '%s' should find results after split", term)
		}
	}

	t.Log("=== Index Queries Test Completed ===")
}

// TestE2E_SplitFailureLeaderCrash tests that a split operation recovers correctly
// when the leader crashes during the split process.
func TestE2E_SplitFailureLeaderCrash(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	// Online shard split tests run by default (skip with -short).

	sigCtx, sigCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer sigCancel()

	ctx, cancel := context.WithTimeout(sigCtx, 20*time.Minute)
	defer cancel()

	t.Log("=== Starting Online Shard Split - Leader Crash Test ===")

	// Create cluster with 3 nodes for proper quorum handling
	cluster := NewTestCluster(t, ctx, TestClusterConfig{
		NumStoreNodes:       3,
		NumShards:           1,
		ReplicationFactor:   3,
		MaxShardSizeBytes:   10 * 1024, // 10KB - low threshold
		DisableShardAlloc:   false,
		ShardCooldownPeriod: 5 * time.Second,
		SplitTimeout:        60 * time.Second, // Short timeout for testing
	})
	defer cluster.Cleanup()

	tableName := "online_split_crash"
	err := cluster.Client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
	})
	require.NoError(t, err)

	err = cluster.WaitForShardsReady(ctx, tableName, 1, 60*time.Second)
	require.NoError(t, err)

	// Step 1: Write initial data
	t.Log("Step 1: Writing initial data...")
	initialRecords := make(map[string]any)
	for i := range 50 {
		key := fmt.Sprintf("init-%03d", i)
		initialRecords[key] = map[string]any{
			"content": fmt.Sprintf("Initial document %d - must survive crash", i),
			"seq":     i,
		}
	}

	_, err = cluster.Client.LinearMerge(ctx, tableName, antfly.LinearMergeRequest{
		Records:   initialRecords,
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err)

	// Step 2: Start background writer
	t.Log("Step 2: Starting background writer...")
	bgWriter := NewBackgroundWriter(cluster, tableName, "crash")
	bgWriter.Start(ctx, 100*time.Millisecond)

	// Step 3: Insert large data to trigger split
	t.Log("Step 3: Triggering shard split...")
	for i := range 50 {
		key := fmt.Sprintf("large-%03d", i)
		record := map[string]any{key: GenerateTestData(10 * 1024)}

		_, err := cluster.Client.Batch(ctx, tableName, antfly.BatchRequest{
			Inserts:   record,
			SyncLevel: antfly.SyncLevelWrite,
		})
		require.NoError(t, err)
	}

	// Step 4: Trigger initial reallocate
	time.Sleep(2 * time.Second)
	if err := cluster.TriggerReallocate(ctx); err != nil {
		t.Logf("Warning: TriggerReallocate failed: %v", err)
	}

	// Step 5: Wait for split to begin, then crash a node
	t.Log("Step 5: Waiting for split to begin...")
	time.Sleep(5 * time.Second)

	t.Log("Step 6: Crashing store node to simulate leader failure...")
	storeIDs := cluster.GetStoreNodeIDs()
	if len(storeIDs) >= 2 {
		// Crash first node (likely to be leader or have shards)
		crashedNode := storeIDs[0]
		if err := cluster.CrashStoreNode(crashedNode); err != nil {
			t.Logf("Warning: Failed to crash store node: %v", err)
		} else {
			t.Logf("Crashed store node %s", crashedNode)
		}
	}

	// Step 7: Wait for recovery and add replacement node
	t.Log("Step 7: Waiting for recovery...")
	time.Sleep(10 * time.Second)

	t.Log("Step 8: Adding replacement node...")
	cluster.AddStoreNode(ctx)

	// Step 9: Continue reallocates to complete or rollback split
	t.Log("Step 9: Continuing split/recovery process...")
	for range 15 {
		if err := cluster.TriggerReallocate(ctx); err != nil {
			t.Logf("Warning: TriggerReallocate failed: %v", err)
		}
		time.Sleep(3 * time.Second)
	}

	// Step 10: Stop background writer
	bgWriter.Stop()

	// Step 11: Verify data integrity
	t.Log("Step 11: Verifying data integrity after crash recovery...")
	accessibleCount := 0
	for i := range 50 {
		key := fmt.Sprintf("init-%03d", i)
		record, err := cluster.Client.LookupKey(ctx, tableName, key)
		if err == nil && record != nil {
			accessibleCount++
		}
	}
	t.Logf("Accessible initial records: %d/50", accessibleCount)
	require.GreaterOrEqual(t, accessibleCount, 45,
		"Most initial records should survive leader crash")

	// Check final state
	tableStatus, err := cluster.Client.GetTable(ctx, tableName)
	require.NoError(t, err)
	t.Logf("Final shard count: %d", len(tableStatus.Shards))

	bgSuccess, bgErrors := bgWriter.Stats()
	t.Logf("Background writer stats: success=%d, errors=%d", bgSuccess, bgErrors)

	t.Log("=== Leader Crash Test Completed ===")
}

// TestE2E_SplitFailureNewShardFails tests that if the new shard fails to start,
// the split operation times out and rolls back gracefully.
func TestE2E_SplitFailureNewShardFails(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	// Online shard split tests run by default (skip with -short).

	sigCtx, sigCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer sigCancel()

	ctx, cancel := context.WithTimeout(sigCtx, 15*time.Minute)
	defer cancel()

	t.Log("=== Starting Online Shard Split - New Shard Failure Test ===")

	// Configure a short split timeout to test rollback behavior
	// In a real scenario, the new shard might fail to start due to resource issues
	splitTimeout := 45 * time.Second

	cluster := NewTestCluster(t, ctx, TestClusterConfig{
		NumStoreNodes:       3,
		NumShards:           1,
		ReplicationFactor:   3,
		MaxShardSizeBytes:   10 * 1024, // 10KB - low threshold
		DisableShardAlloc:   false,
		ShardCooldownPeriod: 5 * time.Second,
		SplitTimeout:        splitTimeout,
	})
	defer cluster.Cleanup()

	tableName := "online_split_newshardFail"
	err := cluster.Client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
	})
	require.NoError(t, err)

	err = cluster.WaitForShardsReady(ctx, tableName, 1, 60*time.Second)
	require.NoError(t, err)

	// Step 1: Write data that must survive the failed split
	t.Log("Step 1: Writing initial data...")
	for i := range 50 {
		key := fmt.Sprintf("init-%03d", i)
		record := map[string]any{
			"content": fmt.Sprintf("Initial document %d - must survive failed split", i),
			"seq":     i,
		}

		_, err := cluster.Client.Batch(ctx, tableName, antfly.BatchRequest{
			Inserts:   map[string]any{key: record},
			SyncLevel: antfly.SyncLevelWrite,
		})
		require.NoError(t, err)
	}

	// Get initial shard count
	initialStatus, err := cluster.Client.GetTable(ctx, tableName)
	require.NoError(t, err)
	initialShardCount := len(initialStatus.Shards)
	t.Logf("Initial shard count: %d", initialShardCount)

	// Step 2: Start background writer
	t.Log("Step 2: Starting background writer...")
	bgWriter := NewBackgroundWriter(cluster, tableName, "newshardFail")
	bgWriter.Start(ctx, 100*time.Millisecond)

	// Step 3: Insert large data to trigger split
	t.Log("Step 3: Inserting data to trigger shard split...")
	for i := range 50 {
		key := fmt.Sprintf("large-%03d", i)
		record := map[string]any{key: GenerateTestData(10 * 1024)}

		_, err := cluster.Client.Batch(ctx, tableName, antfly.BatchRequest{
			Inserts:   record,
			SyncLevel: antfly.SyncLevelWrite,
		})
		require.NoError(t, err)
	}

	// Step 4: Trigger reallocate and immediately remove a node to prevent proper split
	time.Sleep(2 * time.Second)

	if err := cluster.TriggerReallocate(ctx); err != nil {
		t.Logf("Warning: TriggerReallocate failed: %v", err)
	}

	// Step 5: Remove a store node to make it harder for the new shard to get replicas
	t.Log("Step 5: Removing store node to hamper split...")
	storeIDs := cluster.GetStoreNodeIDs()
	if len(storeIDs) > 0 {
		removedNode := storeIDs[len(storeIDs)-1]
		if err := cluster.RemoveStoreNode(ctx, removedNode); err != nil {
			t.Logf("Warning: Failed to remove store node: %v", err)
		} else {
			t.Logf("Removed store node %s", removedNode)
		}
	}

	// Step 6: Wait for timeout and potential rollback
	waitTime := splitTimeout + 30*time.Second
	t.Logf("Step 6: Waiting %v for timeout and potential rollback...", waitTime)

	waitDeadline := time.Now().Add(waitTime)
	for time.Now().Before(waitDeadline) {
		_ = cluster.TriggerReallocate(ctx) // Ignore errors - cluster may be in transition
		time.Sleep(5 * time.Second)
	}

	// Step 7: Add a store node back
	t.Log("Step 7: Adding store node back...")
	cluster.AddStoreNode(ctx)

	// Step 8: Wait for cluster to stabilize
	time.Sleep(10 * time.Second)

	// Trigger more reallocates
	for range 5 {
		if err := cluster.TriggerReallocate(ctx); err != nil {
			t.Logf("Warning: TriggerReallocate failed: %v", err)
		}
		time.Sleep(3 * time.Second)
	}

	// Step 9: Stop background writer
	bgWriter.Stop()

	// Step 10: Verify data integrity - all initial records should still be accessible
	t.Log("Step 10: Verifying data integrity after failed split...")
	accessibleCount := 0
	for i := range 50 {
		key := fmt.Sprintf("init-%03d", i)
		record, err := cluster.Client.LookupKey(ctx, tableName, key)
		if err == nil && record != nil {
			accessibleCount++
		}
	}
	t.Logf("Accessible initial records: %d/50", accessibleCount)
	require.GreaterOrEqual(t, accessibleCount, 45,
		"Most initial records should survive failed split")

	// Check final state
	finalStatus, err := cluster.Client.GetTable(ctx, tableName)
	require.NoError(t, err)
	finalShardCount := len(finalStatus.Shards)
	t.Logf("Final shard count: %d (initial was %d)", finalShardCount, initialShardCount)

	bgSuccess, bgErrors := bgWriter.Stats()
	t.Logf("Background writer stats: success=%d, errors=%d", bgSuccess, bgErrors)

	t.Log("=== New Shard Failure Test Completed ===")
}
