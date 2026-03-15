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
	"encoding/json"
	"fmt"

	"go.uber.org/zap"
)

// maxLookupLimit caps the number of rows returned from a key lookup to avoid
// unbounded result sets. Callers with higher fan-out should paginate.
const maxLookupLimit = 50000

// QueryRunner executes a query against a table and returns result rows.
// The implementation handles routing to the correct data source (Antfly table,
// foreign table, etc.).
type QueryRunner func(ctx context.Context, table string, filterQuery json.RawMessage, filterPrefix []byte, fields []string, limit int) ([]Row, error)

// StatisticsProvider returns statistics for a table.
type StatisticsProvider func(ctx context.Context, tableName string) (*TableStatistics, error)

// AntflyTableQuerier implements TableQuerier using callback functions provided
// by the caller (typically the metadata package). This avoids a circular import
// while letting the join package drive query execution.
type AntflyTableQuerier struct {
	logger   *zap.Logger
	runQuery QueryRunner
	getStats StatisticsProvider
}

// NewAntflyTableQuerier creates a new AntflyTableQuerier.
func NewAntflyTableQuerier(logger *zap.Logger, runQuery QueryRunner, getStats StatisticsProvider) *AntflyTableQuerier {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &AntflyTableQuerier{
		logger:   logger,
		runQuery: runQuery,
		getStats: getStats,
	}
}

// QueryTable executes a query against a table and returns the results.
func (q *AntflyTableQuerier) QueryTable(ctx context.Context, tableName string, filters *Filters, fields []string, limit int) ([]Row, error) {
	q.logger.Debug("Querying table for join",
		zap.String("table", tableName),
		zap.Int("limit", limit))

	if limit <= 0 {
		limit = 10000
	}

	var filterQuery json.RawMessage
	var filterPrefix []byte
	if filters != nil {
		filterQuery = filters.FilterQuery
		filterPrefix = filters.FilterPrefix
	}

	return q.runQuery(ctx, tableName, filterQuery, filterPrefix, fields, limit)
}

// LookupKeys looks up specific keys in a table using a structured disjuncts query.
func (q *AntflyTableQuerier) LookupKeys(ctx context.Context, tableName string, keys []any, keyField string, fields []string) ([]Row, error) {
	if len(keys) == 0 {
		return []Row{}, nil
	}

	q.logger.Debug("Looking up keys in table for join",
		zap.String("table", tableName),
		zap.Int("key_count", len(keys)))

	filterQuery, err := BuildDisjunctsFilter(keyField, keys)
	if err != nil {
		return nil, fmt.Errorf("building lookup filter: %w", err)
	}

	limit := min(len(keys)*10, maxLookupLimit)
	return q.runQuery(ctx, tableName, filterQuery, nil, fields, limit)
}

// GetTableStatistics returns statistics for a table.
func (q *AntflyTableQuerier) GetTableStatistics(ctx context.Context, tableName string) (*TableStatistics, error) {
	q.logger.Debug("Getting table statistics for join planning",
		zap.String("table", tableName))

	return q.getStats(ctx, tableName)
}

// BuildDisjunctsFilter builds a structured Bleve disjuncts JSON filter for matching
// multiple term values on a single field. This avoids query string escaping issues
// and is safely parsed by both Bleve and the SQL filter translator.
func BuildDisjunctsFilter(field string, keys []any) (json.RawMessage, error) {
	if len(keys) == 0 {
		return json.RawMessage(`{"match_all": true}`), nil
	}

	type termQuery struct {
		Term  any    `json:"term"`
		Field string `json:"field"`
	}
	disjuncts := make([]termQuery, len(keys))
	for i, key := range keys {
		disjuncts[i] = termQuery{Term: key, Field: field}
	}
	return json.Marshal(map[string]any{"disjuncts": disjuncts})
}

// ValidateJoinClause validates a join clause before execution.
func ValidateJoinClause(clause *Clause) error {
	if clause == nil {
		return fmt.Errorf("join clause is nil")
	}

	if clause.RightTable == "" {
		return fmt.Errorf("right_table is required")
	}

	if clause.On.LeftField == "" {
		return fmt.Errorf("on.left_field is required")
	}

	if clause.On.RightField == "" {
		return fmt.Errorf("on.right_field is required")
	}

	// Validate join type
	switch clause.JoinType {
	case "", TypeInner, TypeLeft, TypeRight:
		// Valid
	default:
		return fmt.Errorf("invalid join_type: %s", clause.JoinType)
	}

	// Validate operator
	switch clause.On.Operator {
	case "", OpEqual, OpNotEqual, OpLessThan, OpLessEqual, OpGreaterThan, OpGreaterEqual:
		// Valid
	default:
		return fmt.Errorf("invalid operator: %s", clause.On.Operator)
	}

	// Validate strategy hint
	switch clause.StrategyHint {
	case "", StrategyBroadcast, StrategyIndexLookup, StrategyShuffle:
		// Valid
	default:
		return fmt.Errorf("invalid strategy_hint: %s", clause.StrategyHint)
	}

	// Recursively validate nested joins
	if clause.NestedJoin != nil {
		if err := ValidateJoinClause(clause.NestedJoin); err != nil {
			return fmt.Errorf("nested join: %w", err)
		}
	}

	return nil
}
