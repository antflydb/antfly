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
	"maps"
	"net/http"
	"slices"
	"time"

	"github.com/antflydb/antfly/lib/evaluator"
	"go.uber.org/zap"
)

// ExecutorConfig contains configuration for join executors.
type ExecutorConfig struct {
	// MaxBroadcastSize is the maximum size in bytes for broadcast table data.
	MaxBroadcastSize int64

	// BatchSize is the number of keys to lookup in a single batch.
	BatchSize int

	// MaxConcurrency is the maximum number of concurrent operations.
	MaxConcurrency int

	// Timeout is the maximum time for a join operation.
	Timeout time.Duration

	// MemoryLimit is the maximum memory usage for intermediate results.
	MemoryLimit int64

	// SpillToDiskThreshold is the memory threshold at which to spill to disk.
	SpillToDiskThreshold int64
}

// DefaultExecutorConfig returns the default executor configuration.
func DefaultExecutorConfig() *ExecutorConfig {
	return &ExecutorConfig{
		MaxBroadcastSize:     100 * 1024 * 1024, // 100MB
		BatchSize:            1000,
		MaxConcurrency:       10,
		Timeout:              5 * time.Minute,
		MemoryLimit:          1 * 1024 * 1024 * 1024, // 1GB
		SpillToDiskThreshold: 512 * 1024 * 1024,      // 512MB
	}
}

// TableQuerier is the interface for querying table data.
type TableQuerier interface {
	// QueryTable executes a query against a table and returns the results.
	QueryTable(ctx context.Context, tableName string, filters *Filters, fields []string, limit int) ([]Row, error)

	// LookupKeys looks up specific keys in a table.
	LookupKeys(ctx context.Context, tableName string, keys []any, keyField string, fields []string) ([]Row, error)

	// GetTableStatistics returns statistics for a table.
	GetTableStatistics(ctx context.Context, tableName string) (*TableStatistics, error)
}

// ExecutorFactory creates executors based on the join strategy.
type ExecutorFactory struct {
	config  *ExecutorConfig
	logger  *zap.Logger
	client  *http.Client
	querier TableQuerier
}

// NewExecutorFactory creates a new executor factory.
func NewExecutorFactory(config *ExecutorConfig, logger *zap.Logger, client *http.Client, querier TableQuerier) *ExecutorFactory {
	if config == nil {
		config = DefaultExecutorConfig()
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &ExecutorFactory{
		config:  config,
		logger:  logger,
		client:  client,
		querier: querier,
	}
}

// GetExecutor returns the appropriate executor for the given plan.
func (f *ExecutorFactory) GetExecutor(plan *Plan) (Executor, error) {
	switch plan.Strategy {
	case StrategyBroadcast:
		return NewBroadcastExecutor(f.config, f.logger, f.querier), nil
	case StrategyIndexLookup:
		return NewIndexLookupExecutor(f.config, f.logger, f.querier), nil
	case StrategyShuffle:
		return NewShuffleExecutor(f.config, f.logger, f.querier), nil
	default:
		return nil, fmt.Errorf("unknown join strategy: %s", plan.Strategy)
	}
}

// ExecuteJoin executes a join plan and returns the results.
func (f *ExecutorFactory) ExecuteJoin(ctx context.Context, plan *Plan, leftRows []Row) ([]JoinedRow, *Result, error) {
	executor, err := f.GetExecutor(plan)
	if err != nil {
		return nil, nil, err
	}

	// Apply timeout
	ctx, cancel := context.WithTimeout(ctx, f.config.Timeout)
	defer cancel()

	startTime := time.Now()
	result, joinResult, err := executor.Execute(ctx, plan, leftRows)
	if err != nil {
		return nil, nil, err
	}

	// Handle nested joins
	if plan.NestedPlan != nil && len(result) > 0 {
		// Convert JoinedRows back to Rows for the next join
		intermediateRows := make([]Row, len(result))
		for i, jr := range result {
			intermediateRows[i] = jr.Row
		}

		nestedResult, nestedJoinResult, err := f.ExecuteJoin(ctx, plan.NestedPlan, intermediateRows)
		if err != nil {
			return nil, nil, fmt.Errorf("nested join failed: %w", err)
		}

		// Merge join results
		joinResult.JoinTime += nestedJoinResult.JoinTime
		result = nestedResult
	}

	joinResult.JoinTime = time.Since(startTime)
	return result, joinResult, nil
}

// compareValues compares two values using the specified operator.
func compareValues(left, right any, op Operator) bool {
	if left == nil || right == nil {
		return false
	}

	switch op {
	case OpEqual:
		return evaluator.ValuesEqual(left, right)
	case OpNotEqual:
		return !evaluator.ValuesEqual(left, right)
	case OpLessThan:
		return evaluator.CompareOrdered(left, right) < 0
	case OpLessEqual:
		return evaluator.CompareOrdered(left, right) <= 0
	case OpGreaterThan:
		return evaluator.CompareOrdered(left, right) > 0
	case OpGreaterEqual:
		return evaluator.CompareOrdered(left, right) >= 0
	default:
		return evaluator.ValuesEqual(left, right)
	}
}

// mergeFields merges right table fields into the joined row with table prefix.
func mergeFields(left Row, rightRows []Row, rightTable string, rightFields []string) JoinedRow {
	joined := JoinedRow{
		Row: Row{
			ID:     left.ID,
			Fields: make(map[string]any),
			Score:  left.Score,
		},
		RightRows: rightRows,
	}

	// Copy left fields
	maps.Copy(joined.Fields, left.Fields)

	// Merge first right row fields with table prefix
	if len(rightRows) > 0 {
		right := rightRows[0]
		for k, v := range right.Fields {
			// Apply field filter if specified
			if len(rightFields) > 0 {
				found := slices.Contains(rightFields, k)
				if !found {
					continue
				}
			}
			// Prefix with table name
			joined.Fields[rightTable+"."+k] = v
		}
	}

	return joined
}

// createNullJoinedRow creates a joined row with null values for the right side.
func createNullJoinedRow(left Row, rightTable string, rightFields []string) JoinedRow {
	joined := JoinedRow{
		Row: Row{
			ID:     left.ID,
			Fields: make(map[string]any),
			Score:  left.Score,
		},
	}

	// Copy left fields
	maps.Copy(joined.Fields, left.Fields)

	// Add null values for right fields
	for _, f := range rightFields {
		joined.Fields[rightTable+"."+f] = nil
	}

	return joined
}
