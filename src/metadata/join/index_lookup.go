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

// IndexLookupExecutor implements the index lookup join strategy.
// It collects join keys from the left table and performs batch lookups
// against the right table's indexes.
type IndexLookupExecutor struct {
	config  *ExecutorConfig
	logger  *zap.Logger
	querier TableQuerier
}

// NewIndexLookupExecutor creates a new index lookup join executor.
func NewIndexLookupExecutor(config *ExecutorConfig, logger *zap.Logger, querier TableQuerier) *IndexLookupExecutor {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &IndexLookupExecutor{
		config:  config,
		logger:  logger,
		querier: querier,
	}
}

// Name returns the executor name.
func (e *IndexLookupExecutor) Name() string {
	return "index_lookup"
}

// CanExecute returns true if this executor can handle the given plan.
func (e *IndexLookupExecutor) CanExecute(plan *Plan) bool {
	return plan.Strategy == StrategyIndexLookup
}

// Execute performs the index lookup join operation.
func (e *IndexLookupExecutor) Execute(ctx context.Context, plan *Plan, leftRows []Row) ([]JoinedRow, *Result, error) {
	startTime := time.Now()
	result := &Result{
		StrategyUsed:    StrategyIndexLookup,
		LeftRowsScanned: int64(len(leftRows)),
	}

	if len(leftRows) == 0 {
		return []JoinedRow{}, result, nil
	}

	// Collect unique join keys from left table
	keySet := make(map[any][]Row)
	for _, row := range leftRows {
		key := extractFieldValue(row, plan.Condition.LeftField)
		if key != nil {
			keySet[key] = append(keySet[key], row)
		}
	}

	e.logger.Debug("Collected join keys for index lookup",
		zap.Int("unique_keys", len(keySet)),
		zap.Int("left_rows", len(leftRows)))

	// Batch the keys for lookup
	keys := make([]any, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}

	// Perform batch lookups
	batchSize := e.config.BatchSize
	var rightRows []Row
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Limit concurrency
	sem := make(chan struct{}, e.config.MaxConcurrency)

	for i := 0; i < len(keys); i += batchSize {
		end := min(i+batchSize, len(keys))
		batchKeys := keys[i:end]

		wg.Add(1)
		go func(batch []any) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			rows, err := e.querier.LookupKeys(ctx, plan.RightTable, batch, plan.Condition.RightField, plan.RightFields)
			if err != nil {
				e.logger.Error("Batch key lookup failed",
					zap.String("table", plan.RightTable),
					zap.Int("batch_size", len(batch)),
					zap.Error(err))
				return
			}

			mu.Lock()
			rightRows = append(rightRows, rows...)
			mu.Unlock()
		}(batchKeys)
	}

	wg.Wait()

	result.RightRowsScanned = int64(len(rightRows))

	// Build a hash table from the looked-up rows
	hashTable := NewHashTable()
	for _, row := range rightRows {
		key := extractFieldValue(row, plan.Condition.RightField)
		if key != nil {
			hashTable.Insert(key, row)
		}
	}

	e.logger.Debug("Built hash table from lookup results",
		zap.Int("unique_keys", hashTable.Size()),
		zap.Int("total_rows", hashTable.RowCount()))

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

		matches := hashTable.Lookup(leftKey)

		// For non-equality operators, filter the matches
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
			joinedRows = append(joinedRows, mergeFields(leftRow, matches, plan.RightTable, plan.RightFields))
			matchCount++
		} else if plan.JoinType == TypeLeft {
			joinedRows = append(joinedRows, createNullJoinedRow(leftRow, plan.RightTable, plan.RightFields))
			unmatchedLeft++
		}
	}

	// Handle right outer join
	if plan.JoinType == TypeRight {
		// For index lookup, we only looked up keys that exist in the left table,
		// so there are no unmatched right rows from the lookup
		// We would need to query the full right table to find unmatched rows
		e.logger.Warn("Right outer join with index_lookup strategy may be incomplete - consider using broadcast strategy")
	}

	result.RowsMatched = matchCount
	result.RowsUnmatchedLeft = unmatchedLeft
	result.JoinTime = time.Since(startTime)

	e.logger.Debug("Index lookup join completed",
		zap.Int64("left_rows", result.LeftRowsScanned),
		zap.Int64("right_rows", result.RightRowsScanned),
		zap.Int64("matched", result.RowsMatched),
		zap.Int("result_rows", len(joinedRows)),
		zap.Duration("duration", result.JoinTime))

	return joinedRows, result, nil
}

// IndexLookupWithCache adds caching to the index lookup executor
// for repeated lookups of the same keys.
type IndexLookupWithCache struct {
	*IndexLookupExecutor
	cache    map[any][]Row
	cacheMu  sync.RWMutex
	maxCache int
}

// NewIndexLookupWithCache creates an index lookup executor with caching.
func NewIndexLookupWithCache(config *ExecutorConfig, logger *zap.Logger, querier TableQuerier, maxCacheSize int) *IndexLookupWithCache {
	return &IndexLookupWithCache{
		IndexLookupExecutor: NewIndexLookupExecutor(config, logger, querier),
		cache:               make(map[any][]Row),
		maxCache:            maxCacheSize,
	}
}

// Execute performs the index lookup join with caching.
func (e *IndexLookupWithCache) Execute(ctx context.Context, plan *Plan, leftRows []Row) ([]JoinedRow, *Result, error) {
	startTime := time.Now()
	result := &Result{
		StrategyUsed:    StrategyIndexLookup,
		LeftRowsScanned: int64(len(leftRows)),
	}

	if len(leftRows) == 0 {
		return []JoinedRow{}, result, nil
	}

	// Collect unique join keys from left table
	keySet := make(map[any][]Row)
	for _, row := range leftRows {
		key := extractFieldValue(row, plan.Condition.LeftField)
		if key != nil {
			keySet[key] = append(keySet[key], row)
		}
	}

	// Check cache for existing results
	var keysToLookup []any
	cachedResults := make(map[any][]Row)

	e.cacheMu.RLock()
	for key := range keySet {
		cacheKey := fmt.Sprintf("%s:%s:%v", plan.RightTable, plan.Condition.RightField, key)
		if rows, ok := e.cache[cacheKey]; ok {
			cachedResults[key] = rows
		} else {
			keysToLookup = append(keysToLookup, key)
		}
	}
	e.cacheMu.RUnlock()

	e.logger.Debug("Cache hit stats",
		zap.Int("cached", len(cachedResults)),
		zap.Int("to_lookup", len(keysToLookup)))

	// Lookup missing keys
	if len(keysToLookup) > 0 {
		batchSize := e.config.BatchSize
		var lookupResults []Row
		var mu sync.Mutex
		var wg sync.WaitGroup

		sem := make(chan struct{}, e.config.MaxConcurrency)

		for i := 0; i < len(keysToLookup); i += batchSize {
			end := min(i+batchSize, len(keysToLookup))
			batchKeys := keysToLookup[i:end]

			wg.Add(1)
			go func(batch []any) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()

				rows, err := e.querier.LookupKeys(ctx, plan.RightTable, batch, plan.Condition.RightField, plan.RightFields)
				if err != nil {
					e.logger.Error("Batch key lookup failed", zap.Error(err))
					return
				}

				mu.Lock()
				lookupResults = append(lookupResults, rows...)
				mu.Unlock()
			}(batchKeys)
		}

		wg.Wait()

		// Update cache and build lookup map
		e.cacheMu.Lock()
		for _, row := range lookupResults {
			key := extractFieldValue(row, plan.Condition.RightField)
			if key != nil {
				cacheKey := fmt.Sprintf("%s:%s:%v", plan.RightTable, plan.Condition.RightField, key)
				e.cache[cacheKey] = append(e.cache[cacheKey], row)

				// Also add to cached results for this join
				cachedResults[key] = append(cachedResults[key], row)
			}
		}

		// Evict if cache is too large
		if len(e.cache) > e.maxCache {
			// Simple eviction: remove half the entries
			count := 0
			for k := range e.cache {
				delete(e.cache, k)
				count++
				if count >= e.maxCache/2 {
					break
				}
			}
		}
		e.cacheMu.Unlock()

		result.RightRowsScanned = int64(len(lookupResults))
	}

	// Build hash table from all results
	hashTable := NewHashTable()
	for key, rows := range cachedResults {
		for _, row := range rows {
			hashTable.Insert(key, row)
		}
	}

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

		matches := hashTable.Lookup(leftKey)

		if len(matches) > 0 {
			joinedRows = append(joinedRows, mergeFields(leftRow, matches, plan.RightTable, plan.RightFields))
			matchCount++
		} else if plan.JoinType == TypeLeft {
			joinedRows = append(joinedRows, createNullJoinedRow(leftRow, plan.RightTable, plan.RightFields))
			unmatchedLeft++
		}
	}

	result.RowsMatched = matchCount
	result.RowsUnmatchedLeft = unmatchedLeft
	result.JoinTime = time.Since(startTime)

	return joinedRows, result, nil
}

// ClearCache clears the lookup cache.
func (e *IndexLookupWithCache) ClearCache() {
	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()
	e.cache = make(map[any][]Row)
}
