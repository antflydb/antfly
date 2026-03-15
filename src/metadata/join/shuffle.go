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
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/antflydb/antfly/lib/evaluator"
	"go.uber.org/zap"
)

// ShuffleExecutor implements the shuffle hash join strategy.
// It hash-partitions both tables by join key and performs local joins
// on matching partitions. Best for large-large table joins.
type ShuffleExecutor struct {
	config  *ExecutorConfig
	logger  *zap.Logger
	querier TableQuerier
}

// NewShuffleExecutor creates a new shuffle join executor.
func NewShuffleExecutor(config *ExecutorConfig, logger *zap.Logger, querier TableQuerier) *ShuffleExecutor {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &ShuffleExecutor{
		config:  config,
		logger:  logger,
		querier: querier,
	}
}

// Name returns the executor name.
func (e *ShuffleExecutor) Name() string {
	return "shuffle"
}

// CanExecute returns true if this executor can handle the given plan.
func (e *ShuffleExecutor) CanExecute(plan *Plan) bool {
	return plan.Strategy == StrategyShuffle
}

// Execute performs the shuffle hash join operation.
func (e *ShuffleExecutor) Execute(ctx context.Context, plan *Plan, leftRows []Row) ([]JoinedRow, *Result, error) {
	startTime := time.Now()
	result := &Result{
		StrategyUsed:    StrategyShuffle,
		LeftRowsScanned: int64(len(leftRows)),
	}

	if len(leftRows) == 0 {
		return []JoinedRow{}, result, nil
	}

	// Determine number of partitions based on data size and concurrency
	numPartitions := e.calculatePartitions(len(leftRows))
	result.ShufflePartitions = numPartitions

	e.logger.Debug("Starting shuffle join",
		zap.Int("left_rows", len(leftRows)),
		zap.Int("partitions", numPartitions))

	// Fetch right table data
	rightLimit := 0
	if plan.RightFilters != nil && plan.RightFilters.Limit > 0 {
		rightLimit = plan.RightFilters.Limit
	}

	rightRows, err := e.querier.QueryTable(ctx, plan.RightTable, plan.RightFilters, plan.RightFields, rightLimit)
	if err != nil {
		return nil, nil, fmt.Errorf("querying right table %s: %w", plan.RightTable, err)
	}

	result.RightRowsScanned = int64(len(rightRows))

	// Partition left table by hash of join key
	leftPartitions := e.partitionRows(leftRows, plan.Condition.LeftField, numPartitions)

	// Partition right table by hash of join key
	rightPartitions := e.partitionRows(rightRows, plan.Condition.RightField, numPartitions)

	e.logger.Debug("Partitioned data for shuffle join",
		zap.Int("left_partitions", len(leftPartitions)),
		zap.Int("right_partitions", len(rightPartitions)))

	// Process each partition pair in parallel
	var wg sync.WaitGroup
	resultChan := make(chan *partitionJoinResult, numPartitions)

	sem := make(chan struct{}, e.config.MaxConcurrency)

	for i := range numPartitions {
		wg.Add(1)
		go func(partitionID int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			pr := e.joinPartition(plan, leftPartitions[partitionID], rightPartitions[partitionID])
			resultChan <- pr
		}(i)
	}

	// Close channel when all goroutines complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results from all partitions
	var joinedRows []JoinedRow
	var matchCount, unmatchedLeft, unmatchedRight int64
	var memoryUsed int64

	for pr := range resultChan {
		joinedRows = append(joinedRows, pr.rows...)
		matchCount += pr.matched
		unmatchedLeft += pr.unmatchedLeft
		unmatchedRight += pr.unmatchedRight
		memoryUsed += pr.memoryUsed
	}

	result.RowsMatched = matchCount
	result.RowsUnmatchedLeft = unmatchedLeft
	result.RowsUnmatchedRight = unmatchedRight
	result.MemoryUsedBytes = memoryUsed
	result.JoinTime = time.Since(startTime)

	e.logger.Debug("Shuffle join completed",
		zap.Int64("left_rows", result.LeftRowsScanned),
		zap.Int64("right_rows", result.RightRowsScanned),
		zap.Int64("matched", result.RowsMatched),
		zap.Int("result_rows", len(joinedRows)),
		zap.Int("partitions", numPartitions),
		zap.Duration("duration", result.JoinTime))

	return joinedRows, result, nil
}

// calculatePartitions determines the optimal number of partitions.
func (e *ShuffleExecutor) calculatePartitions(leftRowCount int) int {
	// Base on row count and memory limit
	// Aim for ~10MB per partition
	targetPartitionSize := 10 * 1024 * 1024 // 10MB
	estimatedRowSize := 200                 // bytes

	totalSize := leftRowCount * estimatedRowSize
	partitions := min(
		// Bound by concurrency and reasonable limits
		min(

			max(

				(totalSize+targetPartitionSize-1)/targetPartitionSize, 1), e.config.MaxConcurrency*2), 128)

	return partitions
}

// partitionRows partitions rows by hash of the specified field.
func (e *ShuffleExecutor) partitionRows(rows []Row, field string, numPartitions int) [][]Row {
	partitions := make([][]Row, numPartitions)
	for i := range partitions {
		partitions[i] = make([]Row, 0)
	}

	for _, row := range rows {
		key := extractFieldValue(row, field)
		partitionID := e.hashKey(key, numPartitions)
		partitions[partitionID] = append(partitions[partitionID], row)
	}

	return partitions
}

// hashKey computes the partition ID for a key.
func (e *ShuffleExecutor) hashKey(key any, numPartitions int) int {
	if key == nil {
		return 0
	}

	h := fnv.New32a()

	switch v := key.(type) {
	case string:
		_, _ = h.Write([]byte(v))
	case int:
		_, _ = h.Write(fmt.Appendf(nil, "%d", v))
	case int64:
		_, _ = h.Write(fmt.Appendf(nil, "%d", v))
	case float64:
		_, _ = h.Write(fmt.Appendf(nil, "%f", v))
	default:
		_, _ = h.Write(fmt.Appendf(nil, "%v", v))
	}

	return int(h.Sum32()) % numPartitions
}

// partitionJoinResult holds the result of joining a single partition pair.
type partitionJoinResult struct {
	rows           []JoinedRow
	matched        int64
	unmatchedLeft  int64
	unmatchedRight int64
	memoryUsed     int64
}

// joinPartition performs a hash join on a single partition pair.
func (e *ShuffleExecutor) joinPartition(plan *Plan, leftPartition, rightPartition []Row) *partitionJoinResult {
	result := &partitionJoinResult{}

	if len(leftPartition) == 0 && len(rightPartition) == 0 {
		return result
	}

	// Build hash table from right partition (typically smaller in each partition)
	hashTable := NewHashTable()
	for _, row := range rightPartition {
		key := extractFieldValue(row, plan.Condition.RightField)
		if key != nil {
			hashTable.Insert(key, row)
		}
	}

	result.memoryUsed = int64(hashTable.RowCount() * 200)

	// Track matched right rows for outer joins
	matchedRightIDs := make(map[string]bool)

	// Probe with left partition
	for _, leftRow := range leftPartition {
		leftKey := extractFieldValue(leftRow, plan.Condition.LeftField)
		if leftKey == nil {
			if plan.JoinType == TypeLeft {
				result.rows = append(result.rows, createNullJoinedRow(leftRow, plan.RightTable, plan.RightFields))
				result.unmatchedLeft++
			}
			continue
		}

		matches := hashTable.Lookup(leftKey)

		// Filter by operator
		if plan.Condition.Operator != OpEqual && len(matches) > 0 {
			filteredMatches := make([]Row, 0, len(matches))
			for _, match := range matches {
				rightKey := extractFieldValue(match, plan.Condition.RightField)
				if compareValues(leftKey, rightKey, plan.Condition.Operator) {
					filteredMatches = append(filteredMatches, match)
				}
			}
			matches = filteredMatches
		}

		if len(matches) > 0 {
			result.rows = append(result.rows, mergeFields(leftRow, matches, plan.RightTable, plan.RightFields))
			result.matched++

			// Track matched right rows
			for _, match := range matches {
				matchedRightIDs[match.ID] = true
			}
		} else if plan.JoinType == TypeLeft {
			result.rows = append(result.rows, createNullJoinedRow(leftRow, plan.RightTable, plan.RightFields))
			result.unmatchedLeft++
		}
	}

	// Handle right outer join
	if plan.JoinType == TypeRight {
		for _, rightRow := range rightPartition {
			if !matchedRightIDs[rightRow.ID] {
				joined := JoinedRow{
					Row: Row{
						ID:     rightRow.ID,
						Fields: make(map[string]any),
					},
					RightRows: []Row{rightRow},
				}
				for k, v := range rightRow.Fields {
					joined.Fields[plan.RightTable+"."+k] = v
				}
				for _, f := range plan.LeftFields {
					joined.Fields[f] = nil
				}
				result.rows = append(result.rows, joined)
				result.unmatchedRight++
			}
		}
	}

	return result
}

// SortMergeExecutor implements the sort-merge join strategy.
// Best for pre-sorted data or when memory is constrained.
type SortMergeExecutor struct {
	config  *ExecutorConfig
	logger  *zap.Logger
	querier TableQuerier
}

// NewSortMergeExecutor creates a new sort-merge join executor.
func NewSortMergeExecutor(config *ExecutorConfig, logger *zap.Logger, querier TableQuerier) *SortMergeExecutor {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &SortMergeExecutor{
		config:  config,
		logger:  logger,
		querier: querier,
	}
}

// Name returns the executor name.
func (e *SortMergeExecutor) Name() string {
	return "sort_merge"
}

// CanExecute returns true if this executor can handle the given plan.
func (e *SortMergeExecutor) CanExecute(plan *Plan) bool {
	// Sort-merge is a special case, not exposed in API
	return false
}

// Execute performs the sort-merge join operation.
func (e *SortMergeExecutor) Execute(ctx context.Context, plan *Plan, leftRows []Row) ([]JoinedRow, *Result, error) {
	startTime := time.Now()
	result := &Result{
		StrategyUsed:    StrategyShuffle, // Report as shuffle since sort-merge is internal
		LeftRowsScanned: int64(len(leftRows)),
	}

	if len(leftRows) == 0 {
		return []JoinedRow{}, result, nil
	}

	// Fetch right table data
	rightLimit := 0
	if plan.RightFilters != nil && plan.RightFilters.Limit > 0 {
		rightLimit = plan.RightFilters.Limit
	}

	rightRows, err := e.querier.QueryTable(ctx, plan.RightTable, plan.RightFilters, plan.RightFields, rightLimit)
	if err != nil {
		return nil, nil, fmt.Errorf("querying right table %s: %w", plan.RightTable, err)
	}

	result.RightRowsScanned = int64(len(rightRows))

	// Sort both sides by join key
	sortedLeft := e.sortByField(leftRows, plan.Condition.LeftField)
	sortedRight := e.sortByField(rightRows, plan.Condition.RightField)

	e.logger.Debug("Sorted data for merge join",
		zap.Int("left_rows", len(sortedLeft)),
		zap.Int("right_rows", len(sortedRight)))

	// Merge join
	joinedRows := make([]JoinedRow, 0)
	var matchCount, unmatchedLeft, unmatchedRight int64

	leftIdx, rightIdx := 0, 0

	for leftIdx < len(sortedLeft) && rightIdx < len(sortedRight) {
		leftRow := sortedLeft[leftIdx]
		rightRow := sortedRight[rightIdx]

		leftKey := extractFieldValue(leftRow, plan.Condition.LeftField)
		rightKey := extractFieldValue(rightRow, plan.Condition.RightField)

		cmp := evaluator.CompareOrdered(leftKey, rightKey)

		switch {
		case cmp < 0:
			// Left key is smaller, advance left
			if plan.JoinType == TypeLeft {
				joinedRows = append(joinedRows, createNullJoinedRow(leftRow, plan.RightTable, plan.RightFields))
				unmatchedLeft++
			}
			leftIdx++

		case cmp > 0:
			// Right key is smaller, advance right
			if plan.JoinType == TypeRight {
				joined := JoinedRow{
					Row: Row{
						ID:     rightRow.ID,
						Fields: make(map[string]any),
					},
					RightRows: []Row{rightRow},
				}
				for k, v := range rightRow.Fields {
					joined.Fields[plan.RightTable+"."+k] = v
				}
				joinedRows = append(joinedRows, joined)
				unmatchedRight++
			}
			rightIdx++

		default:
			// Keys match - collect all matching rows from both sides
			matchingLeft := e.collectMatchingRows(sortedLeft, leftIdx, plan.Condition.LeftField, leftKey)
			matchingRight := e.collectMatchingRows(sortedRight, rightIdx, plan.Condition.RightField, rightKey)

			// Cross product of matching rows
			for _, lr := range matchingLeft {
				joinedRows = append(joinedRows, mergeFields(lr, matchingRight, plan.RightTable, plan.RightFields))
				matchCount++
			}

			leftIdx += len(matchingLeft)
			rightIdx += len(matchingRight)
		}
	}

	// Handle remaining rows for outer joins
	for leftIdx < len(sortedLeft) {
		if plan.JoinType == TypeLeft {
			joinedRows = append(joinedRows, createNullJoinedRow(sortedLeft[leftIdx], plan.RightTable, plan.RightFields))
			unmatchedLeft++
		}
		leftIdx++
	}

	for rightIdx < len(sortedRight) {
		if plan.JoinType == TypeRight {
			rightRow := sortedRight[rightIdx]
			joined := JoinedRow{
				Row: Row{
					ID:     rightRow.ID,
					Fields: make(map[string]any),
				},
				RightRows: []Row{rightRow},
			}
			for k, v := range rightRow.Fields {
				joined.Fields[plan.RightTable+"."+k] = v
			}
			joinedRows = append(joinedRows, joined)
			unmatchedRight++
		}
		rightIdx++
	}

	result.RowsMatched = matchCount
	result.RowsUnmatchedLeft = unmatchedLeft
	result.RowsUnmatchedRight = unmatchedRight
	result.JoinTime = time.Since(startTime)

	e.logger.Debug("Sort-merge join completed",
		zap.Int64("matched", result.RowsMatched),
		zap.Int("result_rows", len(joinedRows)),
		zap.Duration("duration", result.JoinTime))

	return joinedRows, result, nil
}

// sortByField sorts rows by the specified field value.
func (e *SortMergeExecutor) sortByField(rows []Row, field string) []Row {
	sorted := make([]Row, len(rows))
	copy(sorted, rows)

	sort.Slice(sorted, func(i, j int) bool {
		vi := extractFieldValue(sorted[i], field)
		vj := extractFieldValue(sorted[j], field)
		return evaluator.CompareOrdered(vi, vj) < 0
	})

	return sorted
}

// collectMatchingRows collects all consecutive rows with the same key value.
func (e *SortMergeExecutor) collectMatchingRows(rows []Row, startIdx int, field string, key any) []Row {
	var matching []Row
	for i := startIdx; i < len(rows); i++ {
		rowKey := extractFieldValue(rows[i], field)
		if !evaluator.ValuesEqual(rowKey, key) {
			break
		}
		matching = append(matching, rows[i])
	}
	return matching
}
