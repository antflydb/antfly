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

	lru "github.com/hashicorp/golang-lru/v2"
	"go.uber.org/zap"
)

const (
	// DefaultBroadcastThreshold is the maximum table size in bytes for broadcast join.
	DefaultBroadcastThreshold = 10 * 1024 * 1024 // 10MB

	// DefaultIndexLookupSelectivity is the maximum selectivity ratio for index lookup join.
	DefaultIndexLookupSelectivity = 0.1 // 10% of rows

	// DefaultPlanCacheSize is the number of plans to cache.
	DefaultPlanCacheSize = 1000

	// DefaultPlanCacheTTL is how long cached plans remain valid.
	DefaultPlanCacheTTL = 5 * time.Minute
)

// PlannerConfig contains configuration for the join planner.
type PlannerConfig struct {
	BroadcastThreshold     int64
	IndexLookupSelectivity float64
	PlanCacheSize          int
	PlanCacheTTL           time.Duration
	EnableFilterPushdown   bool
	EnableJoinReordering   bool
}

// DefaultPlannerConfig returns the default planner configuration.
func DefaultPlannerConfig() *PlannerConfig {
	return &PlannerConfig{
		BroadcastThreshold:     DefaultBroadcastThreshold,
		IndexLookupSelectivity: DefaultIndexLookupSelectivity,
		PlanCacheSize:          DefaultPlanCacheSize,
		PlanCacheTTL:           DefaultPlanCacheTTL,
		EnableFilterPushdown:   true,
		EnableJoinReordering:   true,
	}
}

// cachedPlan wraps a plan with expiration time.
type cachedPlan struct {
	plan      *Plan
	createdAt time.Time
}

// Planner creates execution plans for join operations.
type Planner struct {
	config     *PlannerConfig
	logger     *zap.Logger
	statsCache map[string]*TableStatistics
	planCache  *lru.Cache[string, *cachedPlan]
	mu         sync.RWMutex
}

// NewPlanner creates a new join planner.
func NewPlanner(config *PlannerConfig, logger *zap.Logger) (*Planner, error) {
	if config == nil {
		config = DefaultPlannerConfig()
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	planCache, err := lru.New[string, *cachedPlan](config.PlanCacheSize)
	if err != nil {
		return nil, fmt.Errorf("creating plan cache: %w", err)
	}

	return &Planner{
		config:     config,
		logger:     logger,
		statsCache: make(map[string]*TableStatistics),
		planCache:  planCache,
	}, nil
}

// PlanInput contains all information needed to create a join plan.
type PlanInput struct {
	LeftTable  string
	LeftStats  *TableStatistics
	RightTable string
	RightStats *TableStatistics
	JoinClause *Clause
	LeftFields []string
}

// CreatePlan creates an execution plan for the given join clause.
func (p *Planner) CreatePlan(ctx context.Context, input *PlanInput) (*Plan, error) {
	if input.JoinClause == nil {
		return nil, fmt.Errorf("join clause is required")
	}

	// Check plan cache first
	cacheKey := p.planCacheKey(input)
	if cached, ok := p.planCache.Get(cacheKey); ok {
		if time.Since(cached.createdAt) < p.config.PlanCacheTTL {
			p.logger.Debug("Using cached join plan",
				zap.String("left_table", input.LeftTable),
				zap.String("right_table", input.JoinClause.RightTable))
			return cached.plan, nil
		}
		// Expired, remove from cache
		p.planCache.Remove(cacheKey)
	}

	plan := &Plan{
		LeftTable:    input.LeftTable,
		RightTable:   input.JoinClause.RightTable,
		JoinType:     input.JoinClause.JoinType,
		Condition:    input.JoinClause.On,
		RightFilters: input.JoinClause.RightFilters,
		LeftFields:   input.LeftFields,
		RightFields:  input.JoinClause.RightFields,
	}

	// Set default join type
	if plan.JoinType == "" {
		plan.JoinType = TypeInner
	}

	// Set default operator
	if plan.Condition.Operator == "" {
		plan.Condition.Operator = OpEqual
	}

	// Select strategy
	if input.JoinClause.StrategyHint != "" {
		plan.Strategy = input.JoinClause.StrategyHint
		p.logger.Debug("Using strategy hint",
			zap.String("strategy", string(plan.Strategy)))
	} else {
		plan.Strategy = p.selectStrategy(input.LeftStats, input.RightStats, input.JoinClause)
	}

	// Apply optimizations
	if p.config.EnableFilterPushdown {
		p.applyFilterPushdown(plan, input)
	}

	// Calculate cost estimates
	p.estimateCosts(plan, input.LeftStats, input.RightStats)

	// Handle nested joins recursively
	if input.JoinClause.NestedJoin != nil {
		nestedInput := &PlanInput{
			LeftTable:  input.JoinClause.RightTable,
			LeftStats:  input.RightStats,
			RightTable: input.JoinClause.NestedJoin.RightTable,
			JoinClause: input.JoinClause.NestedJoin,
		}
		nestedPlan, err := p.CreatePlan(ctx, nestedInput)
		if err != nil {
			return nil, fmt.Errorf("creating nested join plan: %w", err)
		}
		plan.NestedPlan = nestedPlan
	}

	// Cache the plan
	p.planCache.Add(cacheKey, &cachedPlan{
		plan:      plan,
		createdAt: time.Now(),
	})

	return plan, nil
}

// selectStrategy chooses the optimal join strategy based on table statistics.
func (p *Planner) selectStrategy(leftStats, rightStats *TableStatistics, clause *Clause) Strategy {
	// If we don't have stats, default to broadcast (safest for small tables)
	if leftStats == nil && rightStats == nil {
		p.logger.Debug("No statistics available, defaulting to broadcast")
		return StrategyBroadcast
	}

	var leftSize, rightSize int64
	var leftRows, rightRows int64

	if leftStats != nil {
		leftSize = leftStats.SizeBytes
		leftRows = leftStats.RowCount
	}
	if rightStats != nil {
		rightSize = rightStats.SizeBytes
		rightRows = rightStats.RowCount
	}

	// Use broadcast if either table is small enough
	if rightSize > 0 && rightSize < p.config.BroadcastThreshold {
		p.logger.Debug("Selecting broadcast join - right table is small",
			zap.Int64("right_size_bytes", rightSize))
		return StrategyBroadcast
	}

	if leftSize > 0 && leftSize < p.config.BroadcastThreshold {
		p.logger.Debug("Selecting broadcast join - left table is small",
			zap.Int64("left_size_bytes", leftSize))
		return StrategyBroadcast
	}

	// Check if index lookup is appropriate based on selectivity
	if rightStats != nil && leftStats != nil && leftRows > 0 && rightRows > 0 {
		// Estimate selectivity based on join field cardinality
		if rightStats.FieldStats != nil {
			if fieldStats, ok := rightStats.FieldStats[clause.On.RightField]; ok {
				if fieldStats.Cardinality > 0 {
					// Selectivity = matching rows / total rows
					estimatedSelectivity := float64(leftRows) / float64(fieldStats.Cardinality)
					if estimatedSelectivity < p.config.IndexLookupSelectivity {
						p.logger.Debug("Selecting index lookup join - low selectivity",
							zap.Float64("selectivity", estimatedSelectivity))
						return StrategyIndexLookup
					}
				}
			}
		}
	}

	// For large tables with high selectivity, use shuffle
	if leftSize > p.config.BroadcastThreshold && rightSize > p.config.BroadcastThreshold {
		p.logger.Debug("Selecting shuffle join - both tables are large",
			zap.Int64("left_size_bytes", leftSize),
			zap.Int64("right_size_bytes", rightSize))
		return StrategyShuffle
	}

	// Default to broadcast for simplicity
	return StrategyBroadcast
}

// applyFilterPushdown pushes filters as close to the data source as possible.
func (p *Planner) applyFilterPushdown(plan *Plan, input *PlanInput) {
	// If there are right filters, they're already at the right table
	// This is the basic case - more advanced pushdown would analyze
	// filter predicates and push them to the appropriate table

	p.logger.Debug("Applied filter pushdown optimization",
		zap.Bool("has_right_filters", plan.RightFilters != nil))
}

// estimateCosts calculates estimated execution costs for the plan.
func (p *Planner) estimateCosts(plan *Plan, leftStats, rightStats *TableStatistics) {
	var leftRows, rightRows int64 = 1000, 1000 // Default estimates

	if leftStats != nil {
		leftRows = leftStats.RowCount
	}
	if rightStats != nil {
		rightRows = rightStats.RowCount
	}

	switch plan.Strategy {
	case StrategyBroadcast:
		// Cost: transfer small table + hash probe for each left row
		plan.EstimatedCost = float64(rightRows) + float64(leftRows)*0.001
		plan.EstimatedMemory = rightRows * 200 // Assume 200 bytes per row
		plan.EstimatedRows = leftRows          // Worst case: all left rows match

	case StrategyIndexLookup:
		// Cost: network round-trip per batch + index lookup
		batchSize := int64(1000)
		numBatches := (leftRows + batchSize - 1) / batchSize
		plan.EstimatedCost = float64(numBatches)*10 + float64(leftRows)*0.01
		plan.EstimatedMemory = batchSize * 200
		plan.EstimatedRows = leftRows

	case StrategyShuffle:
		// Cost: shuffle both tables + local hash join
		plan.EstimatedCost = float64(leftRows+rightRows) * 2
		plan.EstimatedMemory = (leftRows + rightRows) * 200 / 10 // Partitioned
		plan.EstimatedRows = leftRows
	}

	p.logger.Debug("Estimated plan costs",
		zap.String("strategy", string(plan.Strategy)),
		zap.Float64("cost", plan.EstimatedCost),
		zap.Int64("memory_bytes", plan.EstimatedMemory),
		zap.Int64("estimated_rows", plan.EstimatedRows))
}

// planCacheKey generates a cache key for the given plan input.
func (p *Planner) planCacheKey(input *PlanInput) string {
	// Create a simple cache key based on table names and join clause
	key := fmt.Sprintf("%s:%s:%s:%s:%s",
		input.LeftTable,
		input.JoinClause.RightTable,
		input.JoinClause.On.LeftField,
		input.JoinClause.On.RightField,
		input.JoinClause.StrategyHint)
	return key
}

// UpdateStatistics updates cached statistics for a table.
func (p *Planner) UpdateStatistics(tableName string, stats *TableStatistics) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.statsCache[tableName] = stats
}

// GetStatistics returns cached statistics for a table.
func (p *Planner) GetStatistics(tableName string) *TableStatistics {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.statsCache[tableName]
}

// OptimizeJoinOrder reorders joins for optimal execution in multi-way joins.
// This implements a simple greedy algorithm that puts smaller tables first.
func (p *Planner) OptimizeJoinOrder(ctx context.Context, tables []TableInfo, joins []*Clause) ([]*Clause, error) {
	if !p.config.EnableJoinReordering || len(joins) <= 1 {
		return joins, nil
	}

	// Simple greedy: sort by estimated size, smallest first
	// This ensures smaller tables are on the build side of hash joins

	type joinWithCost struct {
		join *Clause
		cost float64
	}

	joinCosts := make([]joinWithCost, len(joins))
	for i, j := range joins {
		var cost = 1e9 // Default high cost
		for _, t := range tables {
			if t.Name == j.RightTable && t.Statistics != nil {
				cost = float64(t.Statistics.SizeBytes)
				break
			}
		}
		joinCosts[i] = joinWithCost{join: j, cost: cost}
	}

	// Sort by cost (ascending)
	for i := 0; i < len(joinCosts)-1; i++ {
		for j := i + 1; j < len(joinCosts); j++ {
			if joinCosts[j].cost < joinCosts[i].cost {
				joinCosts[i], joinCosts[j] = joinCosts[j], joinCosts[i]
			}
		}
	}

	result := make([]*Clause, len(joins))
	for i, jc := range joinCosts {
		result[i] = jc.join
	}

	p.logger.Debug("Optimized join order",
		zap.Int("num_joins", len(joins)))

	return result, nil
}

// ClearCache clears the plan cache.
func (p *Planner) ClearCache() {
	p.planCache.Purge()
}
