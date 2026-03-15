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

// BroadcastExecutor implements the broadcast join strategy.
// It fetches the entire right table (or filtered subset) and broadcasts it
// to perform local hash joins with the left table rows.
type BroadcastExecutor struct {
	config  *ExecutorConfig
	logger  *zap.Logger
	querier TableQuerier
}

// NewBroadcastExecutor creates a new broadcast join executor.
func NewBroadcastExecutor(config *ExecutorConfig, logger *zap.Logger, querier TableQuerier) *BroadcastExecutor {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &BroadcastExecutor{
		config:  config,
		logger:  logger,
		querier: querier,
	}
}

// Name returns the executor name.
func (e *BroadcastExecutor) Name() string {
	return "broadcast"
}

// CanExecute returns true if this executor can handle the given plan.
func (e *BroadcastExecutor) CanExecute(plan *Plan) bool {
	return plan.Strategy == StrategyBroadcast
}

// Execute performs the broadcast join operation.
func (e *BroadcastExecutor) Execute(ctx context.Context, plan *Plan, leftRows []Row) ([]JoinedRow, *Result, error) {
	startTime := time.Now()
	result := &Result{
		StrategyUsed:    StrategyBroadcast,
		LeftRowsScanned: int64(len(leftRows)),
	}

	if len(leftRows) == 0 {
		return []JoinedRow{}, result, nil
	}

	// Determine limit for right table query
	rightLimit := 0 // No limit by default
	if plan.RightFilters != nil && plan.RightFilters.Limit > 0 {
		rightLimit = plan.RightFilters.Limit
	}

	// Fetch the right table data
	e.logger.Debug("Fetching right table for broadcast join",
		zap.String("table", plan.RightTable),
		zap.Int("right_limit", rightLimit))

	rightRows, err := e.querier.QueryTable(ctx, plan.RightTable, plan.RightFilters, plan.RightFields, rightLimit)
	if err != nil {
		return nil, nil, fmt.Errorf("querying right table %s: %w", plan.RightTable, err)
	}

	result.RightRowsScanned = int64(len(rightRows))

	// Check size limit
	estimatedSize := int64(len(rightRows)) * 200 // Rough estimate
	if estimatedSize > e.config.MaxBroadcastSize {
		return nil, nil, fmt.Errorf("right table too large for broadcast join: %d bytes (max %d)",
			estimatedSize, e.config.MaxBroadcastSize)
	}

	// Build hash table on join key
	hashTable := NewHashTable()
	for _, row := range rightRows {
		key := extractFieldValue(row, plan.Condition.RightField)
		if key != nil {
			hashTable.Insert(key, row)
		}
	}

	e.logger.Debug("Built hash table for broadcast join",
		zap.Int("unique_keys", hashTable.Size()),
		zap.Int("total_rows", hashTable.RowCount()))

	result.MemoryUsedBytes = estimatedSize

	// Perform the join
	joinedRows := make([]JoinedRow, 0, len(leftRows))
	var matchCount, unmatchedLeft int64

	for _, leftRow := range leftRows {
		leftKey := extractFieldValue(leftRow, plan.Condition.LeftField)
		if leftKey == nil {
			if plan.JoinType == TypeLeft {
				joinedRows = append(joinedRows, createNullJoinedRow(leftRow, plan.RightTable, plan.RightFields))
				unmatchedLeft++
			}
			continue
		}

		var matches []Row
		if plan.Condition.Operator != OpEqual {
			// Non-equality operators require a full scan; findMatchesWithOperator
			// already applies compareValues, so no second filter is needed.
			matches = e.findMatchesWithOperator(rightRows, leftKey, plan.Condition)
		} else {
			matches = hashTable.Lookup(leftKey)
		}

		if len(matches) > 0 {
			joinedRows = append(joinedRows, mergeFields(leftRow, matches, plan.RightTable, plan.RightFields))
			matchCount++
		} else if plan.JoinType == TypeLeft {
			joinedRows = append(joinedRows, createNullJoinedRow(leftRow, plan.RightTable, plan.RightFields))
			unmatchedLeft++
		}
	}

	// Handle right outer join - add unmatched right rows
	if plan.JoinType == TypeRight {
		// Find right rows that didn't match any left row
		matchedRightIDs := make(map[string]bool)
		for _, jr := range joinedRows {
			for _, rr := range jr.RightRows {
				matchedRightIDs[rr.ID] = true
			}
		}

		for _, rightRow := range rightRows {
			if !matchedRightIDs[rightRow.ID] {
				// Create a row with null left values
				joined := JoinedRow{
					Row: Row{
						ID:     rightRow.ID,
						Fields: make(map[string]any),
					},
					RightRows: []Row{rightRow},
				}
				// Add right fields with prefix
				for k, v := range rightRow.Fields {
					joined.Fields[plan.RightTable+"."+k] = v
				}
				// Add null values for left fields
				for _, f := range plan.LeftFields {
					joined.Fields[f] = nil
				}
				joinedRows = append(joinedRows, joined)
				result.RowsUnmatchedRight++
			}
		}
	}

	result.RowsMatched = matchCount
	result.RowsUnmatchedLeft = unmatchedLeft
	result.JoinTime = time.Since(startTime)

	e.logger.Debug("Broadcast join completed",
		zap.Int64("left_rows", result.LeftRowsScanned),
		zap.Int64("right_rows", result.RightRowsScanned),
		zap.Int64("matched", result.RowsMatched),
		zap.Int("result_rows", len(joinedRows)),
		zap.Duration("duration", result.JoinTime))

	return joinedRows, result, nil
}

// findMatchesWithOperator finds all matches for non-equality operators.
func (e *BroadcastExecutor) findMatchesWithOperator(rightRows []Row, leftKey any, condition Condition) []Row {
	var matches []Row
	for _, row := range rightRows {
		rightKey := extractFieldValue(row, condition.RightField)
		if compareValues(leftKey, rightKey, condition.Operator) {
			matches = append(matches, row)
		}
	}
	return matches
}

// ParallelBroadcastExecutor is a concurrent version of BroadcastExecutor
// that processes left rows in parallel batches.
type ParallelBroadcastExecutor struct {
	*BroadcastExecutor
	concurrency int
}

// NewParallelBroadcastExecutor creates a new parallel broadcast executor.
func NewParallelBroadcastExecutor(config *ExecutorConfig, logger *zap.Logger, querier TableQuerier) *ParallelBroadcastExecutor {
	return &ParallelBroadcastExecutor{
		BroadcastExecutor: NewBroadcastExecutor(config, logger, querier),
		concurrency:       config.MaxConcurrency,
	}
}

// Execute performs the broadcast join with parallel processing.
func (e *ParallelBroadcastExecutor) Execute(ctx context.Context, plan *Plan, leftRows []Row) ([]JoinedRow, *Result, error) {
	startTime := time.Now()
	result := &Result{
		StrategyUsed:    StrategyBroadcast,
		LeftRowsScanned: int64(len(leftRows)),
	}

	if len(leftRows) == 0 {
		return []JoinedRow{}, result, nil
	}

	// Fetch and build hash table (same as sequential)
	rightLimit := 0
	if plan.RightFilters != nil && plan.RightFilters.Limit > 0 {
		rightLimit = plan.RightFilters.Limit
	}

	rightRows, err := e.querier.QueryTable(ctx, plan.RightTable, plan.RightFilters, plan.RightFields, rightLimit)
	if err != nil {
		return nil, nil, fmt.Errorf("querying right table %s: %w", plan.RightTable, err)
	}

	result.RightRowsScanned = int64(len(rightRows))

	// Build hash table
	hashTable := NewHashTable()
	for _, row := range rightRows {
		key := extractFieldValue(row, plan.Condition.RightField)
		if key != nil {
			hashTable.Insert(key, row)
		}
	}

	// Process left rows in parallel batches
	batchSize := max((len(leftRows)+e.concurrency-1)/e.concurrency, 100)

	type batchResult struct {
		rows          []JoinedRow
		matched       int64
		unmatchedLeft int64
	}

	var wg sync.WaitGroup
	resultChan := make(chan batchResult, e.concurrency)

	for i := 0; i < len(leftRows); i += batchSize {
		end := min(i+batchSize, len(leftRows))
		batch := leftRows[i:end]

		wg.Add(1)
		go func(batch []Row) {
			defer wg.Done()

			var br batchResult
			br.rows = make([]JoinedRow, 0, len(batch))

			for _, leftRow := range batch {
				leftKey := extractFieldValue(leftRow, plan.Condition.LeftField)
				if leftKey == nil {
					if plan.JoinType == TypeLeft {
						br.rows = append(br.rows, createNullJoinedRow(leftRow, plan.RightTable, plan.RightFields))
						br.unmatchedLeft++
					}
					continue
				}

				matches := hashTable.Lookup(leftKey)
				if plan.Condition.Operator != OpEqual {
					matches = e.findMatchesWithOperator(rightRows, leftKey, plan.Condition)
				}

				if len(matches) > 0 {
					br.rows = append(br.rows, mergeFields(leftRow, matches, plan.RightTable, plan.RightFields))
					br.matched++
				} else if plan.JoinType == TypeLeft {
					br.rows = append(br.rows, createNullJoinedRow(leftRow, plan.RightTable, plan.RightFields))
					br.unmatchedLeft++
				}
			}

			resultChan <- br
		}(batch)
	}

	// Close channel when all goroutines complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	var joinedRows []JoinedRow
	for br := range resultChan {
		joinedRows = append(joinedRows, br.rows...)
		result.RowsMatched += br.matched
		result.RowsUnmatchedLeft += br.unmatchedLeft
	}

	result.JoinTime = time.Since(startTime)
	return joinedRows, result, nil
}
