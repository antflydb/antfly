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

package join

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	// DefaultStatsRefreshInterval is how often to refresh statistics.
	DefaultStatsRefreshInterval = 5 * time.Minute

	// DefaultHLLPrecision is the HyperLogLog precision for cardinality estimation.
	DefaultHLLPrecision = 14 // ~0.81% error rate
)

// StatisticsCollector collects and caches table statistics for query planning.
type StatisticsCollector struct {
	logger     *zap.Logger
	cache      map[string]*TableStatistics
	cacheMu    sync.RWMutex
	refreshTTL time.Duration
	querier    StatisticsQuerier

	// For background refresh
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// StatisticsQuerier is the interface for querying table statistics.
type StatisticsQuerier interface {
	// GetTableInfo returns basic table information.
	GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error)

	// CountRows returns the approximate row count for a table.
	CountRows(ctx context.Context, tableName string) (int64, error)

	// GetTableSize returns the approximate size in bytes.
	GetTableSize(ctx context.Context, tableName string) (int64, error)

	// GetFieldCardinality returns the approximate unique value count for a field.
	GetFieldCardinality(ctx context.Context, tableName string, fieldName string) (int64, error)

	// GetFieldStats returns detailed field statistics.
	GetFieldStats(ctx context.Context, tableName string, fieldName string) (*FieldStats, error)
}

// StatisticsCollectorConfig contains configuration for the statistics collector.
type StatisticsCollectorConfig struct {
	RefreshInterval time.Duration
	CacheSize       int
}

// DefaultStatisticsCollectorConfig returns the default configuration.
func DefaultStatisticsCollectorConfig() *StatisticsCollectorConfig {
	return &StatisticsCollectorConfig{
		RefreshInterval: DefaultStatsRefreshInterval,
		CacheSize:       1000,
	}
}

// NewStatisticsCollector creates a new statistics collector.
func NewStatisticsCollector(config *StatisticsCollectorConfig, logger *zap.Logger, querier StatisticsQuerier) *StatisticsCollector {
	if config == nil {
		config = DefaultStatisticsCollectorConfig()
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	return &StatisticsCollector{
		logger:     logger,
		cache:      make(map[string]*TableStatistics),
		refreshTTL: config.RefreshInterval,
		querier:    querier,
		stopCh:     make(chan struct{}),
	}
}

// Start starts the background statistics refresh goroutine.
func (c *StatisticsCollector) Start() {
	c.wg.Add(1)
	go c.refreshLoop()
}

// Stop stops the background refresh goroutine.
func (c *StatisticsCollector) Stop() {
	close(c.stopCh)
	c.wg.Wait()
}

// refreshLoop periodically refreshes cached statistics.
func (c *StatisticsCollector) refreshLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.refreshTTL)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.refreshAllStats()
		}
	}
}

// refreshAllStats refreshes statistics for all cached tables.
func (c *StatisticsCollector) refreshAllStats() {
	c.cacheMu.RLock()
	tables := make([]string, 0, len(c.cache))
	for table := range c.cache {
		tables = append(tables, table)
	}
	c.cacheMu.RUnlock()

	ctx := context.Background()
	for _, table := range tables {
		stats, err := c.collectStats(ctx, table)
		if err != nil {
			c.logger.Warn("Failed to refresh statistics",
				zap.String("table", table),
				zap.Error(err))
			continue
		}

		c.cacheMu.Lock()
		c.cache[table] = stats
		c.cacheMu.Unlock()
	}
}

// GetStatistics returns cached statistics for a table, collecting if needed.
func (c *StatisticsCollector) GetStatistics(ctx context.Context, tableName string) (*TableStatistics, error) {
	// Check cache first
	c.cacheMu.RLock()
	stats, ok := c.cache[tableName]
	c.cacheMu.RUnlock()

	if ok && time.Since(stats.LastUpdated) < c.refreshTTL {
		return stats, nil
	}

	// Collect fresh statistics
	stats, err := c.collectStats(ctx, tableName)
	if err != nil {
		// Return stale stats if available
		if ok {
			c.logger.Warn("Using stale statistics",
				zap.String("table", tableName),
				zap.Error(err))
			return stats, nil
		}
		return nil, err
	}

	// Update cache
	c.cacheMu.Lock()
	c.cache[tableName] = stats
	c.cacheMu.Unlock()

	return stats, nil
}

// collectStats collects statistics for a table.
func (c *StatisticsCollector) collectStats(ctx context.Context, tableName string) (*TableStatistics, error) {
	if c.querier == nil {
		return nil, fmt.Errorf("no statistics querier configured")
	}

	stats := &TableStatistics{
		FieldStats:  make(map[string]*FieldStats),
		LastUpdated: time.Now(),
	}

	// Get row count
	rowCount, err := c.querier.CountRows(ctx, tableName)
	if err != nil {
		c.logger.Warn("Failed to get row count", zap.String("table", tableName), zap.Error(err))
	} else {
		stats.RowCount = rowCount
	}

	// Get table size
	size, err := c.querier.GetTableSize(ctx, tableName)
	if err != nil {
		c.logger.Warn("Failed to get table size", zap.String("table", tableName), zap.Error(err))
	} else {
		stats.SizeBytes = size
	}

	c.logger.Debug("Collected table statistics",
		zap.String("table", tableName),
		zap.Int64("row_count", stats.RowCount),
		zap.Int64("size_bytes", stats.SizeBytes))

	return stats, nil
}

// CollectFieldStats collects statistics for a specific field.
func (c *StatisticsCollector) CollectFieldStats(ctx context.Context, tableName, fieldName string) (*FieldStats, error) {
	if c.querier == nil {
		return nil, fmt.Errorf("no statistics querier configured")
	}

	return c.querier.GetFieldStats(ctx, tableName, fieldName)
}

// UpdateStatistics updates cached statistics for a table.
func (c *StatisticsCollector) UpdateStatistics(tableName string, stats *TableStatistics) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	stats.LastUpdated = time.Now()
	c.cache[tableName] = stats
}

// InvalidateStatistics invalidates cached statistics for a table.
func (c *StatisticsCollector) InvalidateStatistics(tableName string) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	delete(c.cache, tableName)
}

// InvalidateAll invalidates all cached statistics.
func (c *StatisticsCollector) InvalidateAll() {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	c.cache = make(map[string]*TableStatistics)
}

// EstimateJoinCardinality estimates the result cardinality of a join.
func EstimateJoinCardinality(leftStats, rightStats *TableStatistics, condition Condition) int64 {
	if leftStats == nil || rightStats == nil {
		// Default estimate: smaller table size
		if leftStats != nil {
			return leftStats.RowCount
		}
		if rightStats != nil {
			return rightStats.RowCount
		}
		return 1000
	}

	// Get cardinality of join fields
	var leftCardinality, rightCardinality = leftStats.RowCount, rightStats.RowCount

	if leftStats.FieldStats != nil {
		if fs, ok := leftStats.FieldStats[condition.LeftField]; ok && fs.Cardinality > 0 {
			leftCardinality = fs.Cardinality
		}
	}

	if rightStats.FieldStats != nil {
		if fs, ok := rightStats.FieldStats[condition.RightField]; ok && fs.Cardinality > 0 {
			rightCardinality = fs.Cardinality
		}
	}

	// Estimate join size using containment assumption
	// Assumes smaller domain is contained in larger domain
	smallerCardinality := min(rightCardinality, leftCardinality)

	// For equality join: |R ⋈ S| ≈ |R| * |S| / max(V(R,a), V(S,b))
	// where V(R,a) is the cardinality of attribute a in relation R
	maxCardinality := max(rightCardinality, leftCardinality)

	if maxCardinality == 0 {
		return 0
	}

	estimate := min(
		// Apply join type adjustments
		// For inner join, use the estimate as-is
		// For outer joins, add unmatched rows
		// Cap at reasonable bounds
		max(

			(leftStats.RowCount*rightStats.RowCount)/maxCardinality, 0), leftStats.RowCount*rightStats.RowCount)

	// Minimum estimate is the smaller cardinality
	if estimate < smallerCardinality && smallerCardinality < leftStats.RowCount*rightStats.RowCount {
		estimate = smallerCardinality
	}

	return estimate
}

// EstimateJoinCost estimates the cost of executing a join with the given strategy.
func EstimateJoinCost(strategy Strategy, leftStats, rightStats *TableStatistics) float64 {
	var leftRows, rightRows, leftSize, rightSize int64 = 1000, 1000, 10000, 10000

	if leftStats != nil {
		leftRows = leftStats.RowCount
		leftSize = leftStats.SizeBytes
	}
	if rightStats != nil {
		rightRows = rightStats.RowCount
		rightSize = rightStats.SizeBytes
	}

	switch strategy {
	case StrategyBroadcast:
		// Cost = transfer right table + hash probe for each left row
		// Network cost dominates for broadcast
		networkCost := float64(rightSize) / 1024 / 1024 // MB transferred
		probeCost := float64(leftRows) * 0.001          // Hash probe is cheap
		return networkCost*10 + probeCost

	case StrategyIndexLookup:
		// Cost = network round-trips + index lookups
		batchSize := int64(1000)
		numBatches := (leftRows + batchSize - 1) / batchSize
		networkCost := float64(numBatches) * 5 // Cost per batch
		lookupCost := float64(leftRows) * 0.01 // Index lookup cost
		return networkCost + lookupCost

	case StrategyShuffle:
		// Cost = shuffle both tables + local hash join
		shuffleCost := float64(leftSize+rightSize) / 1024 / 1024 * 5 // MB * cost factor
		joinCost := float64(leftRows+rightRows) * 0.001
		return shuffleCost + joinCost

	default:
		return float64(leftRows * rightRows)
	}
}

// MemoryEstimator estimates memory usage for different join strategies.
type MemoryEstimator struct {
	bytesPerRow int64 // Average bytes per row
}

// NewMemoryEstimator creates a new memory estimator.
func NewMemoryEstimator(bytesPerRow int64) *MemoryEstimator {
	if bytesPerRow <= 0 {
		bytesPerRow = 200
	}
	return &MemoryEstimator{bytesPerRow: bytesPerRow}
}

// EstimateBroadcastMemory estimates memory for broadcast join.
func (e *MemoryEstimator) EstimateBroadcastMemory(rightRowCount int64) int64 {
	// Hash table overhead is roughly 1.5x the data size
	return int64(float64(rightRowCount*e.bytesPerRow) * 1.5)
}

// EstimateIndexLookupMemory estimates memory for index lookup join.
func (e *MemoryEstimator) EstimateIndexLookupMemory(batchSize int) int64 {
	// Only need to hold one batch in memory at a time
	return int64(batchSize) * e.bytesPerRow * 2
}

// EstimateShuffleMemory estimates memory for shuffle join.
func (e *MemoryEstimator) EstimateShuffleMemory(leftRowCount, rightRowCount int64, numPartitions int) int64 {
	// Memory per partition
	leftPerPartition := leftRowCount / int64(numPartitions)
	rightPerPartition := rightRowCount / int64(numPartitions)

	// Need to hold one partition from each side
	return (leftPerPartition + rightPerPartition) * e.bytesPerRow * 2
}
