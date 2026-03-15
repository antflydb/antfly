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

package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"time"

	"github.com/antflydb/antfly/src/metadata/join"
	"go.uber.org/zap"
)

// JoinService handles cross-table join operations.
type JoinService struct {
	logger     *zap.Logger
	planner    *join.Planner
	httpClient *http.Client
	tableApi   *TableApi
}

// NewJoinService creates a new join service.
func NewJoinService(logger *zap.Logger, httpClient *http.Client, tableApi *TableApi) (*JoinService, error) {
	if logger == nil {
		logger = zap.NewNop()
	}

	planner, err := join.NewPlanner(join.DefaultPlannerConfig(), logger)
	if err != nil {
		return nil, fmt.Errorf("creating join planner: %w", err)
	}

	return &JoinService{
		logger:     logger,
		planner:    planner,
		httpClient: httpClient,
		tableApi:   tableApi,
	}, nil
}

// ExecuteQueryWithJoin executes a query with join support.
func (s *JoinService) ExecuteQueryWithJoin(ctx context.Context, queryReq *QueryRequest) QueryResult {
	// First, execute the primary query without the join
	primaryQuery := *queryReq
	primaryQuery.Join = JoinClause{} // Clear join for primary query

	startTime := time.Now()
	primaryResult := s.tableApi.runQuery(ctx, &primaryQuery)

	if primaryResult.Status != http.StatusOK {
		return primaryResult
	}

	// If no join clause, return the primary result
	if queryReq.Join.RightTable == "" {
		return primaryResult
	}

	// Convert primary results to join rows
	leftRows := hitsToJoinRows(primaryResult.Hits.Hits)

	s.logger.Debug("Executing join after primary query",
		zap.String("left_table", queryReq.Table),
		zap.String("right_table", queryReq.Join.RightTable),
		zap.Int("left_rows", len(leftRows)))

	// Convert API join clause to internal format
	joinClause := s.convertAPIJoinClause(&queryReq.Join)

	// Validate the join clause
	if err := join.ValidateJoinClause(joinClause); err != nil {
		return QueryResult{
			Status: http.StatusBadRequest,
			Error:  fmt.Sprintf("invalid join clause: %v", err),
		}
	}

	// Execute the join
	joinedRows, joinResult, err := s.executeJoin(ctx, queryReq.Table, leftRows, joinClause, queryReq.ForeignSources)
	if err != nil {
		return QueryResult{
			Status: http.StatusInternalServerError,
			Error:  fmt.Sprintf("join execution failed: %v", err),
		}
	}

	// Convert joined rows back to query hits
	hits := joinedRowsToHits(joinedRows)

	// Build the result
	result := QueryResult{
		Status: http.StatusOK,
		Hits: QueryHits{
			Hits:  hits,
			Total: uint64(len(hits)),
		},
		Took:  time.Since(startTime),
		Table: queryReq.Table,
	}

	// Add join profile metadata when profiling is enabled
	if queryReq.Profile {
		result.Profile = &QueryProfile{
			Join: JoinProfile{
				StrategyUsed:       JoinStrategy(joinResult.StrategyUsed),
				LeftRowsScanned:    joinResult.LeftRowsScanned,
				RightRowsScanned:   joinResult.RightRowsScanned,
				RowsMatched:        joinResult.RowsMatched,
				RowsUnmatchedLeft:  joinResult.RowsUnmatchedLeft,
				RowsUnmatchedRight: joinResult.RowsUnmatchedRight,
				DurationMs:         joinResult.JoinTime.Milliseconds(),
			},
		}
	}

	// Preserve aggregations from primary query
	result.Aggregations = primaryResult.Aggregations

	return result
}

// executeJoin performs the actual join operation using AntflyTableQuerier.
func (s *JoinService) executeJoin(ctx context.Context, leftTable string, leftRows []join.Row, joinClause *join.Clause, foreignSources map[string]ForeignSource) ([]join.JoinedRow, *join.Result, error) {
	// Create the querier with closures that route to runQuery and getStats
	querier := join.NewAntflyTableQuerier(
		s.logger,
		s.makeQueryRunner(foreignSources),
		s.makeStatisticsProvider(foreignSources),
	)

	// Get statistics for planning
	leftStats, err := querier.GetTableStatistics(ctx, leftTable)
	if err != nil {
		s.logger.Debug("Failed to get left table statistics, using defaults",
			zap.String("table", leftTable), zap.Error(err))
	}
	rightStats, err := querier.GetTableStatistics(ctx, joinClause.RightTable)
	if err != nil {
		s.logger.Debug("Failed to get right table statistics, using defaults",
			zap.String("table", joinClause.RightTable), zap.Error(err))
	}

	// Create the join plan
	planInput := &join.PlanInput{
		LeftTable:  leftTable,
		LeftStats:  leftStats,
		RightTable: joinClause.RightTable,
		RightStats: rightStats,
		JoinClause: joinClause,
	}

	plan, err := s.planner.CreatePlan(ctx, planInput)
	if err != nil {
		return nil, nil, fmt.Errorf("creating join plan: %w", err)
	}

	s.logger.Info("Executing join with plan",
		zap.String("strategy", string(plan.Strategy)),
		zap.Float64("estimated_cost", plan.EstimatedCost))

	// Create executor factory
	factory := join.NewExecutorFactory(join.DefaultExecutorConfig(), s.logger, s.httpClient, querier)

	// Execute the join
	return factory.ExecuteJoin(ctx, plan, leftRows)
}

// makeQueryRunner returns a QueryRunner closure that routes queries through
// the TableApi (which handles both Antfly and foreign tables).
func (s *JoinService) makeQueryRunner(foreignSources map[string]ForeignSource) join.QueryRunner {
	return func(ctx context.Context, table string, filterQuery json.RawMessage, filterPrefix []byte, fields []string, limit int) ([]join.Row, error) {
		queryReq := &QueryRequest{
			Table:          table,
			Fields:         fields,
			FilterQuery:    filterQuery,
			FilterPrefix:   filterPrefix,
			Limit:          limit,
			ForeignSources: foreignSources,
		}

		result := s.tableApi.runQuery(ctx, queryReq)
		if result.Status != http.StatusOK {
			return nil, fmt.Errorf("query failed: %s", result.Error)
		}

		return hitsToJoinRows(result.Hits.Hits), nil
	}
}

// makeStatisticsProvider returns a StatisticsProvider closure that handles both
// Antfly tables (via shard stats) and foreign tables (via SQL statistics).
func (s *JoinService) makeStatisticsProvider(foreignSources map[string]ForeignSource) join.StatisticsProvider {
	return func(ctx context.Context, tableName string) (*join.TableStatistics, error) {
		// Check if this is a foreign table
		if source, ok := foreignSources[tableName]; ok {
			ds, err := s.tableApi.foreignPool.Get(string(source.Type), source.Dsn)
			if err != nil {
				return nil, err
			}
			rowCount, sizeBytes, err := ds.Statistics(ctx, source.PostgresTable)
			if err != nil {
				return nil, err
			}
			return &join.TableStatistics{
				RowCount:  rowCount,
				SizeBytes: sizeBytes,
			}, nil
		}

		_, shardStatuses, err := s.tableApi.tm.GetTableWithShardStatuses(tableName)
		if err != nil {
			return nil, err
		}

		stats := &join.TableStatistics{}
		for _, status := range shardStatuses {
			if status.ShardStats != nil && status.ShardStats.Storage != nil {
				stats.SizeBytes += int64(status.ShardStats.Storage.DiskSize) //nolint:gosec // G115: bounded value, cannot overflow in practice
			}
		}

		// Estimate row count from size if not available
		const estimatedBytesPerRow = 200
		if stats.RowCount == 0 && stats.SizeBytes > 0 {
			stats.RowCount = stats.SizeBytes / estimatedBytesPerRow
		}

		return stats, nil
	}
}

// hitsToJoinRows converts QueryHits to join.Rows.
// A shallow copy of each hit's Source map is made to prevent join executors
// from mutating the original query results.
func hitsToJoinRows(hits []QueryHit) []join.Row {
	rows := make([]join.Row, len(hits))
	for i, hit := range hits {
		fields := make(map[string]any, len(hit.Source))
		maps.Copy(fields, hit.Source)
		rows[i] = join.Row{
			ID:     hit.ID,
			Fields: fields,
			Score:  hit.Score,
		}
	}
	return rows
}

// joinedRowsToHits converts join.JoinedRows to QueryHits.
func joinedRowsToHits(rows []join.JoinedRow) []QueryHit {
	hits := make([]QueryHit, len(rows))
	for i, row := range rows {
		hits[i] = QueryHit{
			ID:     row.ID,
			Score:  row.Score,
			Source: row.Fields,
		}
	}
	return hits
}

// convertAPIJoinClause converts an API JoinClause to internal format.
func (s *JoinService) convertAPIJoinClause(apiClause *JoinClause) *join.Clause {
	rightFields := apiClause.RightFields
	// Ensure the join key field is always fetched from the right table so the
	// hash-table/lookup can match rows. This is essential for foreign (SQL)
	// tables where only the requested fields are SELECTed.
	if len(rightFields) > 0 && !slices.Contains(rightFields, apiClause.On.RightField) {
		rightFields = append(slices.Clone(rightFields), apiClause.On.RightField)
	}

	clause := &join.Clause{
		RightTable:  apiClause.RightTable,
		JoinType:    join.Type(apiClause.JoinType),
		RightFields: rightFields,
		On: join.Condition{
			LeftField:  apiClause.On.LeftField,
			RightField: apiClause.On.RightField,
		},
	}

	// Convert operator if present
	if apiClause.On.Operator != "" {
		clause.On.Operator = join.Operator(apiClause.On.Operator)
	}

	// Convert strategy hint
	if apiClause.StrategyHint != "" {
		clause.StrategyHint = join.Strategy(apiClause.StrategyHint)
	}

	// Convert right filters
	if apiClause.RightFilters.FilterQuery != nil || apiClause.RightFilters.FilterPrefix != nil {
		clause.RightFilters = &join.Filters{
			FilterQuery:  apiClause.RightFilters.FilterQuery,
			FilterPrefix: apiClause.RightFilters.FilterPrefix,
			Limit:        apiClause.RightFilters.Limit,
		}
	}

	// Convert nested join recursively
	if apiClause.NestedJoin != nil && apiClause.NestedJoin.RightTable != "" {
		clause.NestedJoin = s.convertAPIJoinClause(apiClause.NestedJoin)
	}

	return clause
}

// runQueryWithJoin runs a query with join support.
func (t *TableApi) runQueryWithJoin(ctx context.Context, queryReq *QueryRequest) QueryResult {
	// If no join clause, use regular query
	if queryReq.Join.RightTable == "" {
		return t.runQuery(ctx, queryReq)
	}

	// Initialize join service once (safe for concurrent callers).
	t.joinOnce.Do(func() {
		js, err := NewJoinService(t.logger, t.tm.HttpClient(), t)
		if err != nil {
			t.joinInitErr = err
			return
		}
		t.joinService = js
	})
	if t.joinInitErr != nil {
		return QueryResult{
			Status: http.StatusInternalServerError,
			Error:  fmt.Sprintf("initializing join service: %v", t.joinInitErr),
		}
	}

	return t.joinService.ExecuteQueryWithJoin(ctx, queryReq)
}
