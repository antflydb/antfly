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

// Package join implements cross-table join operations for Antfly.
// It provides query planning, execution strategies (broadcast, index lookup, shuffle),
// and optimization for distributed join execution across shards.
package join

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/antflydb/antfly/lib/types"
)

// Strategy represents the join execution strategy.
type Strategy string

const (
	// StrategyBroadcast broadcasts the smaller table to all shards of the larger table.
	// Best for dimension tables < 10MB.
	StrategyBroadcast Strategy = "broadcast"

	// StrategyIndexLookup uses batch key lookups via indexes.
	// Best for selective joins with indexed join keys.
	StrategyIndexLookup Strategy = "index_lookup"

	// StrategyShuffle hash-partitions both tables by join key and joins matching partitions.
	// Best for large-large table joins.
	StrategyShuffle Strategy = "shuffle"
)

// Type represents the type of join operation.
type Type string

const (
	TypeInner Type = "inner"
	TypeLeft  Type = "left"
	TypeRight Type = "right"
)

// Operator represents comparison operators for join conditions.
type Operator string

const (
	OpEqual        Operator = "eq"
	OpNotEqual     Operator = "neq"
	OpLessThan     Operator = "lt"
	OpLessEqual    Operator = "lte"
	OpGreaterThan  Operator = "gt"
	OpGreaterEqual Operator = "gte"
)

// Condition represents a join condition between two tables.
type Condition struct {
	LeftField  string   `json:"left_field"`
	RightField string   `json:"right_field"`
	Operator   Operator `json:"operator,omitempty"`
}

// Filters represents filters to apply to a table before joining.
type Filters struct {
	FilterQuery  json.RawMessage `json:"filter_query,omitempty"`
	FilterPrefix []byte          `json:"filter_prefix,omitempty"`
	Limit        int             `json:"limit,omitempty"`
}

// Clause represents a complete join specification.
type Clause struct {
	RightTable   string    `json:"right_table"`
	JoinType     Type      `json:"join_type,omitempty"`
	On           Condition `json:"on"`
	RightFilters *Filters  `json:"right_filters,omitempty"`
	RightFields  []string  `json:"right_fields,omitempty"`
	StrategyHint Strategy  `json:"strategy_hint,omitempty"`
	NestedJoin   *Clause   `json:"nested_join,omitempty"`
}

// Result contains statistics and metadata about join execution.
type Result struct {
	StrategyUsed       Strategy      `json:"strategy_used"`
	LeftRowsScanned    int64         `json:"left_rows_scanned"`
	RightRowsScanned   int64         `json:"right_rows_scanned"`
	RowsMatched        int64         `json:"rows_matched"`
	RowsUnmatchedLeft  int64         `json:"rows_unmatched_left"`
	RowsUnmatchedRight int64         `json:"rows_unmatched_right"`
	JoinTime           time.Duration `json:"join_time_ms"`
	ShufflePartitions  int           `json:"shuffle_partitions,omitempty"`
	MemoryUsedBytes    int64         `json:"memory_used_bytes,omitempty"`
}

// Row represents a single row in join processing.
type Row struct {
	ID     string         `json:"id"`
	Fields map[string]any `json:"fields"`
	Score  float64        `json:"score,omitempty"`
}

// JoinedRow represents a row after joining.
type JoinedRow struct {
	Row
	// RightRows contains matched rows from the right table (for 1:N joins)
	RightRows []Row `json:"right_rows,omitempty"`
}

// TableInfo contains information about a table for join planning.
type TableInfo struct {
	Name       string
	ShardCount int
	Statistics *TableStatistics
}

// TableStatistics contains statistics about a table for query optimization.
type TableStatistics struct {
	RowCount    int64                  `json:"row_count"`
	SizeBytes   int64                  `json:"size_bytes"`
	FieldStats  map[string]*FieldStats `json:"field_stats,omitempty"`
	LastUpdated time.Time              `json:"last_updated"`
}

// FieldStats contains statistics about a specific field.
type FieldStats struct {
	Cardinality int64 `json:"cardinality"`
	NullCount   int64 `json:"null_count"`
	MinValue    any   `json:"min_value,omitempty"`
	MaxValue    any   `json:"max_value,omitempty"`
	AvgSize     int   `json:"avg_size,omitempty"`
}

// Plan represents a join execution plan.
type Plan struct {
	LeftTable  string
	RightTable string
	JoinType   Type
	Condition  Condition
	Strategy   Strategy

	// Filters for each side
	LeftFilters  *Filters
	RightFilters *Filters

	// Fields to include in result
	LeftFields  []string
	RightFields []string

	// Nested join plan (for multi-way joins)
	NestedPlan *Plan

	// Estimated cost metrics
	EstimatedCost   float64
	EstimatedRows   int64
	EstimatedMemory int64
}

// Executor is the interface for join execution strategies.
type Executor interface {
	// Execute performs the join operation.
	Execute(ctx context.Context, plan *Plan, leftRows []Row) ([]JoinedRow, *Result, error)

	// Name returns the executor name for logging/metrics.
	Name() string

	// CanExecute returns true if this executor can handle the given plan.
	CanExecute(plan *Plan) bool
}

// ShardPeers maps shard IDs to their peer URLs.
type ShardPeers map[types.ID][]string

// HashTable is a simple hash table for join lookups.
type HashTable struct {
	data map[any][]Row
	mu   sync.RWMutex
}

// NewHashTable creates a new hash table for join operations.
func NewHashTable() *HashTable {
	return &HashTable{
		data: make(map[any][]Row),
	}
}

// Insert adds a row to the hash table keyed by the given value.
func (h *HashTable) Insert(key any, row Row) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.data[key] = append(h.data[key], row)
}

// Lookup returns all rows matching the given key.
func (h *HashTable) Lookup(key any) []Row {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.data[key]
}

// Size returns the number of unique keys in the hash table.
func (h *HashTable) Size() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.data)
}

// RowCount returns the total number of rows in the hash table.
func (h *HashTable) RowCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	count := 0
	for _, rows := range h.data {
		count += len(rows)
	}
	return count
}

// Clear removes all entries from the hash table.
func (h *HashTable) Clear() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.data = make(map[any][]Row)
}

// extractFieldValue extracts a field value from a row, supporting nested fields.
func extractFieldValue(row Row, field string) any {
	// First, try the field as a flat key (e.g., "customers.address_id")
	// This handles join-prefixed fields
	if val, ok := row.Fields[field]; ok {
		return val
	}

	// Handle nested fields (e.g., "address.city" where address is a nested object)
	parts := splitField(field)
	if len(parts) <= 1 {
		return nil // Already tried as flat key
	}

	current := any(row.Fields)
	for _, part := range parts {
		if m, ok := current.(map[string]any); ok {
			current = m[part]
		} else {
			return nil
		}
	}
	return current
}

// splitField splits a dotted field path into parts.
func splitField(field string) []string {
	if field == "" {
		return nil
	}
	if !strings.Contains(field, ".") {
		return []string{field}
	}
	return strings.Split(field, ".")
}
