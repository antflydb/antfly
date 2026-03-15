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
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/antflydb/antfly/lib/evaluator"
	"github.com/antflydb/antfly/src/metadata/foreign"
)

// runForeignQuery executes a query against a foreign data source (e.g. PostgreSQL).
// It rejects operations not supported on foreign tables and delegates to the foreign package.
func (t *TableApi) runForeignQuery(ctx context.Context, queryReq *QueryRequest, source ForeignSource) QueryResult {
	startTime := time.Now()

	// Reject unsupported operations on foreign tables
	if queryReq.FullTextSearch != nil {
		return QueryResult{
			Status: http.StatusBadRequest,
			Error:  "full_text_search is not supported on foreign tables",
		}
	}
	if queryReq.SemanticSearch != "" {
		return QueryResult{
			Status: http.StatusBadRequest,
			Error:  "semantic_search is not supported on foreign tables",
		}
	}
	if len(queryReq.GraphSearches) > 0 {
		return QueryResult{
			Status: http.StatusBadRequest,
			Error:  "graph_searches is not supported on foreign tables",
		}
	}
	if queryReq.Reranker != nil {
		return QueryResult{
			Status: http.StatusBadRequest,
			Error:  "reranker is not supported on foreign tables",
		}
	}

	// Get or create a DataSource from the pool
	ds, err := t.foreignPool.Get(string(source.Type), source.Dsn)
	if err != nil {
		return QueryResult{
			Status: http.StatusBadGateway,
			Error:  fmt.Sprintf("connecting to foreign source: %v", err),
		}
	}

	// Convert API columns to foreign package columns
	var columns []foreign.ForeignColumn
	for _, c := range source.Columns {
		columns = append(columns, foreign.ForeignColumn{
			Name:     c.Name,
			Type:     c.Type,
			Nullable: c.Nullable,
		})
	}

	limit := queryReq.Limit
	if limit <= 0 {
		limit = 10
	}

	params := &foreign.QueryParams{
		Table:       source.PostgresTable,
		Fields:      queryReq.Fields,
		FilterQuery: queryReq.FilterQuery,
		Limit:       limit,
		Offset:      queryReq.Offset,
		Columns:     columns,
		OrderBy:     queryReq.OrderBy,
	}

	// Apply a timeout to the foreign query to avoid hanging on slow/unresponsive sources.
	const defaultForeignTimeout = 30 * time.Second
	queryCtx, cancel := context.WithTimeout(ctx, defaultForeignTimeout)
	defer cancel()

	// Handle aggregations if requested.
	if len(queryReq.Aggregations) > 0 {
		return t.runForeignAggregations(queryCtx, ds, source, queryReq, columns)
	}

	result, err := ds.Query(queryCtx, params)
	if err != nil {
		// Check if it's a QueryError with an HTTP status
		if qe, ok := err.(*foreign.QueryError); ok {
			return QueryResult{
				Status: int32(qe.Status), //nolint:gosec // G115: bounded value, cannot overflow in practice
				Error:  qe.Message,
			}
		}
		return QueryResult{
			Status: http.StatusInternalServerError,
			Error:  fmt.Sprintf("foreign query failed: %v", err),
		}
	}

	// Convert rows to QueryHits
	hits := make([]QueryHit, len(result.Rows))
	for i, row := range result.Rows {
		// Use a stable ID from the row if available, otherwise use index
		id := fmt.Sprintf("%d", i)
		if v, ok := row["id"]; ok {
			id = fmt.Sprintf("%v", v)
		} else if v, ok := row["_id"]; ok {
			id = fmt.Sprintf("%v", v)
		}
		hits[i] = QueryHit{
			ID:     id,
			Score:  1.0, // foreign results don't have relevance scores
			Source: row,
		}
	}

	return QueryResult{
		Status: http.StatusOK,
		Hits: QueryHits{
			Hits:  hits,
			Total: uint64(result.Total), //nolint:gosec // G115: bounded value, cannot overflow in practice
		},
		Took:  time.Since(startTime),
		Table: queryReq.Table,
	}
}

// supportedForeignAggTypes are aggregation types that map cleanly to SQL.
var supportedForeignAggTypes = map[AggregationType]string{
	AggregationTypeCount: "count",
	AggregationTypeSum:   "sum",
	AggregationTypeAvg:   "avg",
	AggregationTypeMin:   "min",
	AggregationTypeMax:   "max",
	AggregationTypeStats: "stats",
	AggregationTypeTerms: "terms",
}

// runForeignAggregations executes aggregation queries against a foreign SQL data source.
func (t *TableApi) runForeignAggregations(ctx context.Context, ds foreign.DataSource, source ForeignSource, queryReq *QueryRequest, columns []foreign.ForeignColumn) QueryResult {
	startTime := time.Now()

	// Build aggregation definitions, rejecting unsupported types.
	aggDefs := make(map[string]foreign.AggregationDef, len(queryReq.Aggregations))
	for name, aggReq := range queryReq.Aggregations {
		sqlType, ok := supportedForeignAggTypes[aggReq.Type]
		if !ok {
			return QueryResult{
				Status: http.StatusBadRequest,
				Error:  fmt.Sprintf("aggregation type %q is not supported on foreign tables", aggReq.Type),
			}
		}
		size := 0
		if aggReq.Size != nil {
			size = *aggReq.Size
		}
		aggDefs[name] = foreign.AggregationDef{
			Type:  sqlType,
			Field: aggReq.Field,
			Size:  size,
		}
	}

	aggDS, ok := ds.(foreign.Aggregator)
	if !ok {
		return QueryResult{
			Status: http.StatusBadRequest,
			Error:  "foreign data source does not support aggregations",
		}
	}

	aggResult, err := aggDS.Aggregate(ctx, &foreign.AggregateParams{
		Table:        source.PostgresTable,
		FilterQuery:  queryReq.FilterQuery,
		Columns:      columns,
		Aggregations: aggDefs,
	})
	if err != nil {
		return QueryResult{
			Status: http.StatusInternalServerError,
			Error:  fmt.Sprintf("foreign aggregation failed: %v", err),
		}
	}

	// Convert foreign aggregation results to API AggregationResult format.
	apiAggs := make(map[string]AggregationResult, len(aggResult.Results))
	for name, raw := range aggResult.Results {
		apiAggs[name] = convertForeignAggResult(queryReq.Aggregations[name].Type, raw)
	}

	return QueryResult{
		Status:       http.StatusOK,
		Aggregations: apiAggs,
		Took:         time.Since(startTime),
		Table:        queryReq.Table,
	}
}

// convertForeignAggResult converts a raw foreign aggregation result to the API format.
func convertForeignAggResult(aggType AggregationType, raw any) AggregationResult {
	switch aggType {
	case AggregationTypeStats:
		statsMap, _ := raw.(map[string]any)
		return AggregationResult{
			Count: toIntPtr(statsMap["count"]),
			Min:   toFloat64Ptr(statsMap["min"]),
			Max:   toFloat64Ptr(statsMap["max"]),
			Avg:   toFloat64Ptr(statsMap["avg"]),
			Sum:   toFloat64Ptr(statsMap["sum"]),
		}
	case AggregationTypeTerms:
		bucketList, _ := raw.([]map[string]any)
		buckets := make([]AggregationBucket, 0, len(bucketList))
		for _, b := range bucketList {
			docCount, _ := b["doc_count"].(int64)
			buckets = append(buckets, AggregationBucket{
				Key:      fmt.Sprintf("%v", b["key"]),
				DocCount: int(docCount),
			})
		}
		return AggregationResult{Buckets: buckets}
	default:
		// Metric aggregations: count, sum, avg, min, max
		val := toFloat64Ptr(raw)
		return AggregationResult{Value: val}
	}
}

func toFloat64Ptr(v any) *float64 {
	f, ok := evaluator.ToFloat64(v)
	if !ok {
		return nil
	}
	return &f
}

func toIntPtr(v any) *int {
	if v == nil {
		return nil
	}
	switch n := v.(type) {
	case int64:
		i := int(n)
		return &i
	case int32:
		i := int(n)
		return &i
	case int:
		return &n
	case float64:
		i := int(n)
		return &i
	case uint64:
		if n > math.MaxInt {
			return nil
		}
		i := int(n)
		return &i
	case uint:
		if n > uint(math.MaxInt) {
			return nil
		}
		i := int(n)
		return &i
	default:
		return nil
	}
}
