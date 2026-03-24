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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	json "github.com/antflydb/antfly/pkg/libaf/json"

	"github.com/antflydb/antfly/lib/embeddings"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vector/stats"
	"github.com/antflydb/antfly/lib/workerpool"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/cespare/xxhash/v2"
	"github.com/google/dotprompt/go/dotprompt"
	"github.com/jellydator/ttlcache/v3"
	"go.uber.org/zap"
)

func (t *TableApi) executeHybridFullTextFallback(
	ctx context.Context,
	shardIndexes indexes.RemoteIndexes,
	queryReq *QueryRequest,
	q *indexes.Query,
	graphSearches map[string]*indexes.GraphQuery,
	table *store.Table,
	profile *QueryProfile,
	hybridFullTextFallback query.Query,
) QueryResult {
	searchResults, err := shardIndexes.FullTextSearch(
		ctx,
		q.HybridFullTextFallbackQuery(hybridFullTextFallback),
		q.AggregationRequests,
		q.FullTextPagingOptions(),
	)
	if err != nil {
		return QueryResult{
			Status: http.StatusInternalServerError,
			Error:  fmt.Sprintf("performing full text fallback: %v", err),
		}
	}

	queryResult := t.bleveResultToQueryResult(ctx, searchResults, queryReq, q, graphSearches, table, profile)
	if queryReq.Analyses != nil {
		queryResult.Analyses = t.computeAnalyses(queryReq, queryResult.Hits.Hits)
		if queryResult.Analyses == nil {
			return QueryResult{
				Status: http.StatusInternalServerError,
				Error:  "failed to compute analyses",
			}
		}
	}

	return queryResult
}

func (q *QueryRequest) ToRemoteIndexQuery() (*indexes.Query, error) {
	rq := indexes.Query{}
	rq.Embeddings = make(map[string]vector.T, len(q.Embeddings))
	for k, v := range q.Embeddings {
		if dense, err := v.AsEmbedding0(); err == nil {
			rq.Embeddings[k] = dense
		} else if sparse, err := v.AsEmbedding1(); err == nil {
			if rq.SparseEmbeddings == nil {
				rq.SparseEmbeddings = make(map[string]indexes.SparseVec)
			}
			rq.SparseEmbeddings[k] = indexes.SparseVec{
				Indices: sparse.Indices,
				Values:  sparse.Values,
			}
		}
	}
	rq.Fields = q.Fields
	rq.Count = q.Count
	var err error
	hasFilter := len(q.FilterQuery) > 0 && !bytes.Equal(q.FilterQuery, []byte("null"))
	hasFullText := len(q.FullTextSearch) > 0 && !bytes.Equal(q.FullTextSearch, []byte("null"))
	hasSemantic := len(q.SemanticSearch) > 0
	hasGraphSearch := len(q.GraphSearches) > 0

	// Inject match_all query for limit-only queries (or when no search criteria is specified)
	if !hasFullText && !hasFilter && !hasSemantic && !hasGraphSearch {
		// Create match_all query for queries that only specify a limit or no criteria at all
		matchAll := query.NewMatchAllQuery()
		matchAllBytes, _ := json.Marshal(matchAll)
		q.FullTextSearch = matchAllBytes
		hasFullText = true
	} else if hasFilter && !hasFullText && !hasSemantic {
		// Use filter_query as full_text_search if no other query is specified
		q.FullTextSearch = q.FilterQuery
		q.FilterQuery = nil
		hasFullText = true
	}
	// Parse the native Bleve query JSON into a Bleve query object.
	if hasFullText {
		rq.FullTextSearch, err = query.ParseQuery(q.FullTextSearch)
		if err != nil {
			return nil, err
		}
	}
	if hasFilter {
		rq.FilterQuery, err = query.ParseQuery(q.FilterQuery)
		if err != nil {
			return nil, err
		}
	}
	if len(q.ExclusionQuery) > 0 && !bytes.Equal(q.ExclusionQuery, []byte("null")) {
		rq.ExclusionQuery, err = query.ParseQuery(q.ExclusionQuery)
		if err != nil {
			return nil, err
		}
	}
	rq.Table = q.Table
	rq.Limit = q.Limit
	rq.Offset = q.Offset
	rq.FilterPrefix = q.FilterPrefix
	rq.DistanceUnder = q.DistanceUnder
	rq.DistanceOver = q.DistanceOver
	rq.OrderBy = q.OrderBy
	rq.SearchAfter = q.SearchAfter
	rq.SearchBefore = q.SearchBefore
	rq.Reranker = q.Reranker
	if q.Reranker != nil {
		// Validation is done in QueryRequest.Validate(), called by runQuery
		rq.RerankerQuery = q.SemanticSearch
	}

	// Convert aggregations from API types to internal types
	aggregations := make(indexes.AggregationRequests, len(q.Aggregations))
	for name, agg := range q.Aggregations {
		aggReq := &indexes.AggregationRequest{
			Type:  string(agg.Type),
			Field: agg.Field,
		}

		if agg.Size != nil {
			aggReq.Size = *agg.Size
		}
		if agg.Precision != nil {
			aggReq.Precision = uint8(*agg.Precision) //nolint:gosec // G115: bounded value, cannot overflow in practice
			aggReq.GeohashPrecision = *agg.Precision
		}
		if agg.Interval != nil {
			aggReq.Interval = *agg.Interval
		}
		if agg.CalendarInterval != nil {
			aggReq.CalendarInterval = string(*agg.CalendarInterval)
		}
		if agg.Algorithm != nil {
			aggReq.SignificanceAlgorithm = string(*agg.Algorithm)
		}
		if agg.MinDocCount != nil {
			aggReq.MinDocCount = int64(*agg.MinDocCount)
		}

		// Geo distance configuration
		if agg.Origin != nil {
			// Parse origin string (format: "lat,lon")
			var lat, lon float64
			if _, err := fmt.Sscanf(*agg.Origin, "%f,%f", &lat, &lon); err == nil {
				aggReq.CenterLat = lat
				aggReq.CenterLon = lon
			}
		}
		if agg.Unit != nil {
			aggReq.DistanceUnit = string(*agg.Unit)
		}

		// Convert ranges
		if len(agg.Ranges) > 0 {
			aggReq.NumericRanges = make([]*indexes.NumericRange, len(agg.Ranges))
			for i, r := range agg.Ranges {
				aggReq.NumericRanges[i] = &indexes.NumericRange{
					Name:  r.Name,
					Start: r.From,
					End:   r.To,
				}
			}
		}
		if len(agg.DateRanges) > 0 {
			aggReq.DateTimeRanges = make([]*indexes.DateTimeRange, len(agg.DateRanges))
			for i, r := range agg.DateRanges {
				aggReq.DateTimeRanges[i] = &indexes.DateTimeRange{
					Name:  r.Name,
					Start: r.From,
					End:   r.To,
				}
			}
		}
		if len(agg.DistanceRanges) > 0 {
			aggReq.DistanceRanges = make([]*indexes.DistanceRange, len(agg.DistanceRanges))
			for i, r := range agg.DistanceRanges {
				aggReq.DistanceRanges[i] = &indexes.DistanceRange{
					Name: r.Name,
					From: r.From,
					To:   r.To,
				}
			}
		}

		// Recursively convert sub-aggregations
		if len(agg.SubAggregations) > 0 {
			aggReq.Aggregations = make(indexes.AggregationRequests, len(agg.SubAggregations))
			for subName, subAgg := range agg.SubAggregations {
				subAggReq := &indexes.AggregationRequest{
					Type:  string(subAgg.Type),
					Field: subAgg.Field,
				}
				if subAgg.Size != nil {
					subAggReq.Size = *subAgg.Size
				}
				// Note: For simplicity, only supporting one level of sub-aggregations here
				// Full recursive support could be added if needed
				aggReq.Aggregations[subName] = subAggReq
			}
		}

		aggregations[name] = aggReq
	}
	rq.AggregationRequests = aggregations

	// Convert GraphSearches from API type (value) to internal type (pointer)
	if len(q.GraphSearches) > 0 {
		rq.GraphSearches = make(map[string]*indexes.GraphQuery, len(q.GraphSearches))
		for name, gq := range q.GraphSearches {
			gqCopy := gq // Copy to get addressable value
			rq.GraphSearches[name] = &gqCopy
		}
	}

	// Pass MergeConfig through to the internal query
	rq.MergeConfig = q.mergeConfigToInternal()

	// Pass Pruner through for score-based filtering
	if !q.Pruner.IsEmpty() {
		rq.Pruner = &q.Pruner
	}

	return &rq, nil
}

var ErrBadRequest = errors.New("bad request")

// mergeStrategy returns the merge strategy from MergeConfig, defaulting to empty.
func (q *QueryRequest) mergeStrategy() indexes.MergeStrategy {
	if q.MergeConfig.Strategy != nil {
		return *q.MergeConfig.Strategy
	}
	return ""
}

// mergeConfigToInternal converts the API MergeConfig (value type with omitzero)
// to the internal indexes.MergeConfig pointer type.
func (q *QueryRequest) mergeConfigToInternal() *indexes.MergeConfig {
	mc := q.MergeConfig
	if mc.Strategy == nil && mc.Weights == nil && mc.WindowSize == 0 && mc.RankConstant == 0 {
		return nil
	}
	return &indexes.MergeConfig{
		Strategy:     mc.Strategy,
		Weights:      mc.Weights,
		WindowSize:   mc.WindowSize,
		RankConstant: mc.RankConstant,
	}
}

func (t *TableApi) runQuery(ctx context.Context, queryReq *QueryRequest) QueryResult {
	// Validate request configuration first
	if err := queryReq.Validate(); err != nil {
		return QueryResult{
			Status: http.StatusBadRequest,
			Error:  err.Error(),
		}
	}

	// Route to foreign data source if this table is in foreign_sources
	if source, ok := queryReq.ForeignSources[queryReq.Table]; ok {
		return t.runForeignQuery(ctx, queryReq, source)
	}

	// Set default merge strategy to "rrf" if not specified
	if queryReq.mergeStrategy() == "" {
		rrf := indexes.MergeStrategyRrf
		queryReq.MergeConfig.Strategy = &rrf
	}

	q, err := queryReq.ToRemoteIndexQuery()
	if err != nil {
		return QueryResult{
			Status: http.StatusBadRequest,
			Error:  fmt.Sprintf("parsing query: %v", err),
		}
	}

	// Extract GraphSearches to execute at metadata level (cross-shard aware)
	// Don't send them to individual shards
	graphSearches := q.GraphSearches
	q.GraphSearches = nil // Clear for shard-level execution
	q.ExpandStrategy = "" // Clear expand strategy (applied at metadata level)

	table, shardStatuses, err := t.tm.GetTableWithShardStatuses(q.Table)
	if err != nil {
		return QueryResult{
			Status: http.StatusNotFound,
			Error:  fmt.Sprintf("getting table %s: %v", q.Table, err),
		}
	}
	// FIXME (ajr) Unhealthy shards should get an error in the response map
	shardPeers, err := t.ln.getHealthyShardPeers(shardStatuses)
	if err != nil {
		return QueryResult{
			Status: http.StatusInternalServerError,
			Error:  fmt.Sprintf("getting healthy shard peers: %v", err),
		}
	}

	// During schema migration, use ReadSchema (old version) for queries so we
	// target the fully-built index rather than the one still being rebuilt.
	querySchema := table.Schema
	if table.ReadSchema != nil {
		querySchema = table.ReadSchema
	}
	shardIndexes, err := indexes.MakeIndexesForShards(
		t.tm.HttpClient(),
		querySchema,
		shardPeers,
		&indexes.FieldFilter{
			Fields:    q.Fields,
			Star:      len(q.Fields) == 0,
			CountStar: q.Count && len(q.Fields) == 0 && queryReq.FullTextSearch != nil,
		},
	)
	if err != nil {
		return QueryResult{
			Status: http.StatusInternalServerError,
			Error:  fmt.Sprintf("creating search indexes: %v", err),
		}
	}
	t.logger.Debug("Received JSON query",
		zap.Any("remoteindex.Query", q),
		zap.Any("QueryRequest", queryReq))
	queryResult := QueryResult{
		// FIXME (ajr) Instead of errors returning
		// set status and error here
		Status: 200,
	}
	if status, errMsg := t.generateQueryEmbeddings(ctx, queryReq, table, q); errMsg != "" {
		return QueryResult{Status: int32(status), Error: errMsg} //nolint:gosec // G115: bounded value, cannot overflow in practice
	}

	hasAnyEmbeddings := len(q.Embeddings) > 0 || len(q.SparseEmbeddings) > 0
	if hasAnyEmbeddings {
		hybridFullTextFallback := q.PrepareHybridFullTextForSemanticSearch()
		results, err := shardIndexes.FusedSearch(ctx, q)
		if err != nil {
			return QueryResult{
				Status: http.StatusInternalServerError,
				Error:  fmt.Sprintf("performing fused search: %v", err),
			}
		}
		t.logger.Debug(
			"Found hits from RRF search",
			zap.Any("query", q),
			zap.Int("numHits", len(results.Hits)),
			zap.String("semanticSearch", queryReq.SemanticSearch),
			zap.Int("total", int(results.Total)), //nolint:gosec // G115: bounded value, cannot overflow in practice
		)

		var profile *QueryProfile
		if queryReq.Profile {
			profile = buildShardsProfile(results.Status)
		}

		hadSemanticHits := len(results.Hits) > 0
		if indexes.ShouldUseHybridFullTextFallback(q.HybridFullTextMode, hybridFullTextFallback, hadSemanticHits) {
			return t.executeHybridFullTextFallback(
				ctx,
				shardIndexes,
				queryReq,
				q,
				graphSearches,
				table,
				profile,
				hybridFullTextFallback,
			)
		}
		indexes.ApplyPrunerToFusionResult(results, q.Pruner)
		queryResult = t.fusionResultToQueryResult(ctx, results, graphSearches, table, profile)
		if queryReq.Analyses != nil {
			queryResult.Analyses = t.computeAnalyses(queryReq, queryResult.Hits.Hits)
			if queryResult.Analyses == nil {
				return QueryResult{
					Status: http.StatusInternalServerError,
					Error:  "failed to compute analyses",
				}
			}
		}

		return queryResult
	}

	// Combine full text search with filter and exclusion queries
	fullTextQuery := q.FullTextSearch
	if q.FilterQuery != nil {
		fullTextQuery = query.NewConjunctionQuery([]query.Query{fullTextQuery, q.FilterQuery})
	}
	if q.ExclusionQuery != nil {
		fullTextQuery = query.NewBooleanQuery(
			[]query.Query{fullTextQuery},
			nil,
			[]query.Query{q.ExclusionQuery},
		)
	}
	searchResults, err := shardIndexes.FullTextSearch(ctx, fullTextQuery,
		q.AggregationRequests,
		q.FullTextPagingOptions())
	if err != nil {
		return QueryResult{
			Status: http.StatusInternalServerError,
			Error:  fmt.Sprintf("performing full text search: %v", err),
		}
	}

	return t.bleveResultToQueryResult(ctx, searchResults, queryReq, q, graphSearches, table, nil)
}

func (t *TableApi) handleQuery(w http.ResponseWriter, r *http.Request, maybeTableName string) {
	// t.logger.Debug("Handling query request", zap.String("tableName", maybeTableName))
	queryReqs := make([]QueryRequest, 0, 1)
	decoder := json.NewDecoder(r.Body)
	// While not an EOF, keep decoding QueryRequest objects
	for {
		queryReq := QueryRequest{}
		if err := decoder.Decode(&queryReq); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			errorResponse(
				w,
				fmt.Errorf("Error decoding request body: %w", err).Error(),
				http.StatusBadRequest,
			)
			return
		}
		if maybeTableName != "" {
			if queryReq.Table != "" && queryReq.Table != maybeTableName {
				errorResponse(w, "table name in path and query mismatch", http.StatusBadRequest)
				return
			}
			queryReq.Table = maybeTableName
		}
		queryReqs = append(queryReqs, queryReq)
	}
	if len(queryReqs) == 0 {
		errorResponse(w, "No query requests found", http.StatusBadRequest)
		return
	}

	var queryResults []QueryResult

	// If all queries target the same table, use batch execution for efficiency
	if len(queryReqs) > 1 && allSameTable(queryReqs) {
		queryResults = t.runBatchQueriesForTable(r.Context(), queryReqs)
	} else {
		queryResults = t.runQueries(r.Context(), queryReqs)
	}

	if len(queryResults) == 0 {
		errorResponse(w, "No query results", http.StatusInternalServerError)
		return
	}
	queryResps := &QueryResponses{
		Responses: queryResults,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(queryResps); err != nil {
		t.logger.Warn("Failed to marshal response", zap.Error(err))
		errorResponse(w, "Failed to marshal response", http.StatusInternalServerError)
		return
	}
}

// allSameTable checks if all queries target the same table
func allSameTable(reqs []QueryRequest) bool {
	if len(reqs) == 0 {
		return true
	}
	table := reqs[0].Table
	for _, req := range reqs[1:] {
		if req.Table != table {
			return false
		}
	}
	return true
}

// runQueries executes multiple query requests in parallel using the shared pool.
// Errors are captured per-result, not propagated.
func (t *TableApi) runQueries(ctx context.Context, queryReqs []QueryRequest) []QueryResult {
	rg, _ := workerpool.NewResultGroup[QueryResult](ctx, t.pool)
	for i := range queryReqs {
		rg.Go(func(ctx context.Context) (QueryResult, error) {
			if queryReqs[i].Join.RightTable != "" {
				return t.runQueryWithJoin(ctx, &queryReqs[i]), nil
			}
			return t.runQuery(ctx, &queryReqs[i]), nil
		})
	}
	results, _ := rg.Wait()
	return results
}

// generateQueryEmbeddings generates dense and sparse embeddings for the given query,
// populating q.Embeddings and q.SparseEmbeddings. It handles full_text_index setup,
// caching, template-based embedding, and failover strategy.
// Returns (0, "") on success, or (httpStatusCode, errorMessage) on failure.
func (t *TableApi) generateQueryEmbeddings(
	ctx context.Context,
	queryReq *QueryRequest,
	table *store.Table,
	q *indexes.Query,
) (int, string) {
	if len(queryReq.Indexes) == 0 {
		return 0, ""
	}

	for _, idxName := range queryReq.Indexes {
		if strings.HasPrefix(idxName, "full_text_index") {
			if queryReq.FullTextSearch != nil && !bytes.Equal(queryReq.FullTextSearch, []byte("null")) {
				return http.StatusBadRequest, "cannot use full_text_index in indexes and in full_text_search"
			}
			queryString := query.NewQueryStringQuery(queryReq.SemanticSearch)
			queryString.SetBoost(5)
			q.FullTextSearch = query.NewDisjunctionQuery([]query.Query{queryString, &query.MatchAllQuery{}})
			continue
		}

		index, ok := table.Indexes[idxName]
		if !ok {
			return http.StatusBadRequest, fmt.Sprintf("index %s not found on table %s", idxName, q.Table)
		}

		// Sparse embeddings indexes: embed once here and pass the pre-computed vector to shards.
		embeddingsConfig, ecErr := index.AsEmbeddingsIndexConfig()
		if ecErr == nil && embeddingsConfig.Sparse {
			if embeddingsConfig.Embedder == nil {
				return http.StatusBadRequest, fmt.Sprintf("sparse index %s has no embedder configured", idxName)
			}
			embedder, eErr := embeddings.NewEmbedder(*embeddingsConfig.Embedder)
			if eErr != nil {
				if queryReq.mergeStrategy() == indexes.MergeStrategyFailover {
					t.logger.Warn("Failed to create sparse embedder, skipping for failover",
						zap.String("index", idxName), zap.Error(eErr))
					continue
				}
				return http.StatusInternalServerError, fmt.Sprintf("creating sparse embedder for index %s: %v", idxName, eErr)
			}
			sparseEmb, ok := embedder.(embeddings.SparseEmbedder)
			if !ok {
				return http.StatusInternalServerError, fmt.Sprintf("embedder for sparse index %s does not support sparse embedding", idxName)
			}
			vecs, sErr := sparseEmb.SparseEmbed(ctx, []string{queryReq.SemanticSearch})
			if sErr != nil {
				if queryReq.mergeStrategy() == indexes.MergeStrategyFailover {
					t.logger.Warn("Sparse embedding failed, skipping for failover",
						zap.String("index", idxName), zap.Error(sErr))
					continue
				}
				return http.StatusInternalServerError, fmt.Sprintf("computing sparse embedding for index %s: %v", idxName, sErr)
			}
			if len(vecs) == 0 {
				if queryReq.mergeStrategy() == indexes.MergeStrategyFailover {
					continue
				}
				return http.StatusInternalServerError, "no sparse vector returned for index " + idxName
			}
			if q.SparseEmbeddings == nil {
				q.SparseEmbeddings = make(map[string]indexes.SparseVec)
			}
			q.SparseEmbeddings[idxName] = indexes.SparseVec{
				Indices: vecs[0].Indices,
				Values:  vecs[0].Values,
			}
			continue
		}

		// Dense embeddings
		modelConfig, err := index.GetEmbedderConfig()
		if err != nil {
			return http.StatusInternalServerError, fmt.Sprintf("getting config for index %s: %v", idxName, err)
		}

		cacheKeyInput := queryReq.SemanticSearch
		if queryReq.EmbeddingTemplate != "" {
			cacheKeyInput += "|" + queryReq.EmbeddingTemplate
		}
		cacheKey := fmt.Sprintf("%s:%s:%d", q.Table, idxName, xxhash.Sum64String(cacheKeyInput))

		if cachedEmbedding := t.ln.embeddingCache.Get(cacheKey); cachedEmbedding != nil && cachedEmbedding.Value() != nil {
			q.Embeddings[idxName] = cachedEmbedding.Value()
			continue
		}

		embedder, err := embeddings.NewEmbedder(*modelConfig)
		if err != nil {
			return http.StatusBadRequest, fmt.Sprintf("creating embedder for model %s: %v", idxName, err)
		}

		var queryEmbeddings [][]float32
		if queryReq.EmbeddingTemplate != "" {
			doc := schema.Document{Fields: map[string]any{"this": queryReq.SemanticSearch}}
			dp := dotprompt.NewDotprompt(nil)
			queryEmbeddings, err = embeddings.EmbedDocuments(ctx, embedder, dp, queryReq.EmbeddingTemplate, []schema.Document{doc})
		} else {
			queryEmbeddings, err = embeddings.EmbedText(ctx, embedder, []string{queryReq.SemanticSearch})
		}

		if err != nil {
			if queryReq.mergeStrategy() == indexes.MergeStrategyFailover {
				t.logger.Warn("Embedding generation failed, skipping index for failover",
					zap.String("index", idxName), zap.Error(err))
				continue
			}
			return http.StatusInternalServerError, fmt.Sprintf("computing embedding for model %s: %v", idxName, err)
		}

		if len(queryEmbeddings) == 0 {
			if queryReq.mergeStrategy() == indexes.MergeStrategyFailover {
				continue
			}
			return http.StatusInternalServerError, "no embedding returned for model " + idxName
		}

		t.ln.embeddingCache.Set(cacheKey, queryEmbeddings[0], ttlcache.DefaultTTL)
		q.Embeddings[idxName] = queryEmbeddings[0]
	}

	// Handle failover when all embeddings failed
	hasAnyEmbeddings := len(q.Embeddings) > 0 || len(q.SparseEmbeddings) > 0
	if !hasAnyEmbeddings && len(queryReq.Indexes) > 0 {
		if queryReq.mergeStrategy() == indexes.MergeStrategyFailover {
			t.logger.Info("All embedding generations failed with failover strategy, falling back to full-text search",
				zap.String("table", q.Table),
				zap.String("query", queryReq.SemanticSearch))
		} else if len(queryReq.Indexes) != 1 || !strings.HasPrefix(queryReq.Indexes[0], "full_text_index") {
			return http.StatusInternalServerError, "embedding generation failed for the specified indexes and failover strategy was not used"
		}
	} else if queryReq.mergeStrategy() == indexes.MergeStrategyFailover {
		queryReq.FullTextSearch = nil
	}

	return 0, ""
}

// preparedQuery holds a query that has been validated and had embeddings generated
type preparedQuery struct {
	originalIdx    int
	queryReq       *QueryRequest
	query          *indexes.Query
	graphSearches  map[string]*indexes.GraphQuery
	hybridFallback query.Query
	err            string
	errStatus      int
}

// prepareQueryForBatch validates a query and generates embeddings.
// This extracts the preparation logic from runQuery for use in batch execution.
func (t *TableApi) prepareQueryForBatch(
	ctx context.Context,
	queryReq *QueryRequest,
	table *store.Table,
) (*indexes.Query, map[string]*indexes.GraphQuery, query.Query, int, string) {
	// Validate request configuration first
	if err := queryReq.Validate(); err != nil {
		return nil, nil, nil, http.StatusBadRequest, err.Error()
	}

	// Set default merge strategy
	if queryReq.mergeStrategy() == "" {
		rrf := indexes.MergeStrategyRrf
		queryReq.MergeConfig.Strategy = &rrf
	}

	q, err := queryReq.ToRemoteIndexQuery()
	if err != nil {
		return nil, nil, nil, http.StatusBadRequest, fmt.Sprintf("parsing query: %v", err)
	}

	// Extract GraphSearches to execute at metadata level
	graphSearches := q.GraphSearches
	q.GraphSearches = nil
	q.ExpandStrategy = ""

	// Generate embeddings for semantic search indexes
	if status, errMsg := t.generateQueryEmbeddings(ctx, queryReq, table, q); errMsg != "" {
		return nil, nil, nil, status, errMsg
	}

	// Validate limit for semantic search
	hasAnyEmbeddings := len(q.Embeddings) > 0 || len(q.SparseEmbeddings) > 0
	var hybridFallback query.Query
	if hasAnyEmbeddings {
		hybridFallback = q.PrepareHybridFullTextForSemanticSearch()
	}
	if hasAnyEmbeddings && q.Limit == 0 {
		return nil, nil, nil, http.StatusBadRequest, "limit is required for semantic search"
	}

	return q, graphSearches, hybridFallback, 0, ""
}

// runBatchQueriesForTable executes multiple queries for the same table,
// batching shard-level requests for efficiency using BatchMultiSearch
func (t *TableApi) runBatchQueriesForTable(ctx context.Context, queryReqs []QueryRequest) []QueryResult {
	if len(queryReqs) == 0 {
		return nil
	}

	results := make([]QueryResult, len(queryReqs))
	tableName := queryReqs[0].Table

	// Get table info once for all queries
	table, shardStatuses, err := t.tm.GetTableWithShardStatuses(tableName)
	if err != nil {
		errResult := QueryResult{
			Status: http.StatusNotFound,
			Error:  fmt.Sprintf("getting table %s: %v", tableName, err),
		}
		for i := range results {
			results[i] = errResult
		}
		return results
	}

	shardPeers, err := t.ln.getHealthyShardPeers(shardStatuses)
	if err != nil {
		errResult := QueryResult{
			Status: http.StatusInternalServerError,
			Error:  fmt.Sprintf("getting healthy shard peers: %v", err),
		}
		for i := range results {
			results[i] = errResult
		}
		return results
	}

	numShards := len(shardPeers)
	if numShards == 0 {
		errResult := QueryResult{
			Status: http.StatusInternalServerError,
			Error:  "no healthy shards available",
		}
		for i := range results {
			results[i] = errResult
		}
		return results
	}

	// Prepare all queries in parallel (validation, embeddings, etc.)
	rg, _ := workerpool.NewResultGroup[preparedQuery](ctx, t.pool)
	for i := range queryReqs {
		rg.Go(func(ctx context.Context) (preparedQuery, error) {
			pq := preparedQuery{originalIdx: i, queryReq: &queryReqs[i]}
			q, graphSearches, hybridFallback, status, errStr := t.prepareQueryForBatch(ctx, &queryReqs[i], table)
			if errStr != "" {
				pq.err = errStr
				pq.errStatus = status
			} else {
				pq.query = q
				pq.graphSearches = graphSearches
				pq.hybridFallback = hybridFallback
			}
			return pq, nil // errors stored in pq.err, never returned
		})
	}
	prepared, _ := rg.Wait()

	// During schema migration, use ReadSchema (old version) for queries so we
	// target the fully-built index rather than the one still being rebuilt.
	querySchema := table.Schema
	if table.ReadSchema != nil {
		querySchema = table.ReadSchema
	}

	// Build batched shard requests
	// For each valid query, we need to send to ALL shards
	// Structure: [query0_shard0, query0_shard1, ..., query1_shard0, query1_shard1, ...]
	var shardReqs []*indexes.RemoteIndexSearchRequest
	var shardIdxs []*indexes.RemoteIndex
	validQueryIndices := []int{}
	queryShardIndexes := make(map[int]indexes.RemoteIndexes)

	for i, pq := range prepared {
		if pq.err != "" {
			results[i] = QueryResult{Status: int32(pq.errStatus), Error: pq.err} //nolint:gosec // G115: bounded value, cannot overflow in practice
			continue
		}

		// Create shard indexes for this query
		shardIndexes, err := indexes.MakeIndexesForShards(
			t.tm.HttpClient(),
			querySchema,
			shardPeers,
			&indexes.FieldFilter{
				Fields:    pq.query.Fields,
				Star:      len(pq.query.Fields) == 0,
				CountStar: pq.query.Count && len(pq.query.Fields) == 0 && pq.queryReq.FullTextSearch != nil,
			},
		)
		if err != nil {
			results[i] = QueryResult{
				Status: http.StatusInternalServerError,
				Error:  fmt.Sprintf("creating search indexes: %v", err),
			}
			continue
		}

		// Convert query to search request
		searchReq, err := pq.query.RemoteIndexSearchRequest()
		if err != nil {
			results[i] = QueryResult{
				Status: http.StatusInternalServerError,
				Error:  fmt.Sprintf("converting query to search request: %v", err),
			}
			continue
		}

		validQueryIndices = append(validQueryIndices, i)
		queryShardIndexes[i] = shardIndexes

		// Add request for each shard
		for _, shardIdx := range shardIndexes {
			shardReqs = append(shardReqs, searchReq)
			shardIdxs = append(shardIdxs, shardIdx)
		}
	}

	if len(validQueryIndices) == 0 {
		return results
	}

	// Execute all shard requests using BatchMultiSearch
	// This batches requests to the same shard together
	shardResults, err := indexes.BatchMultiSearch(ctx, shardReqs, shardIdxs)
	if err != nil {
		t.logger.Error("BatchMultiSearch failed", zap.Error(err))
		// Fall back to error for all valid queries
		for _, idx := range validQueryIndices {
			results[idx] = QueryResult{
				Status: http.StatusInternalServerError,
				Error:  fmt.Sprintf("batch search failed: %v", err),
			}
		}
		return results
	}

	// Process results for each query
	for vi, origIdx := range validQueryIndices {
		pq := prepared[origIdx]

		// Collect and merge results from all shards for this query
		startIdx := vi * numShards
		endIdx := min(startIdx+numShards, len(shardResults))

		var merged *indexes.RemoteIndexSearchResult
		for si := startIdx; si < endIdx; si++ {
			sr := shardResults[si]
			if sr == nil {
				continue
			}
			if merged == nil {
				merged = sr
				continue
			}
			// Merge per-field like remoteindex.go
			if sr.BleveSearchResult != nil {
				if merged.BleveSearchResult == nil {
					merged.BleveSearchResult = sr.BleveSearchResult
				} else {
					merged.BleveSearchResult.Merge(sr.BleveSearchResult)
				}
			}
			if sr.VectorSearchResult != nil {
				if merged.VectorSearchResult == nil {
					merged.VectorSearchResult = sr.VectorSearchResult
				} else {
					for idx := range sr.VectorSearchResult {
						if merged.VectorSearchResult[idx] == nil {
							merged.VectorSearchResult[idx] = sr.VectorSearchResult[idx]
						} else {
							merged.VectorSearchResult[idx].Merge(sr.VectorSearchResult[idx])
						}
					}
				}
			}
			if sr.FusionResult != nil {
				if merged.FusionResult == nil {
					merged.FusionResult = sr.FusionResult
				} else {
					merged.FusionResult.Merge(sr.FusionResult)
				}
			}
			if sr.AggregationResults != nil {
				if merged.AggregationResults == nil {
					merged.AggregationResults = sr.AggregationResults
				} else {
					merged.AggregationResults.Merge(sr.AggregationResults)
				}
			}
			if sr.Status != nil && merged.Status != nil {
				merged.Status.Merge(sr.Status)
			}
		}

		// Sort fusion results once after all shards have been merged.
		if merged != nil && merged.FusionResult != nil {
			merged.FusionResult.FinalizeSort()
		}

		if merged == nil {
			results[origIdx] = QueryResult{Status: http.StatusOK}
			continue
		}

		// Build profile if requested
		var profile *QueryProfile
		if pq.queryReq.Profile {
			profile = buildShardsProfile(merged.Status)
		}

		// Apply fusion strategy
		var fusionResult *indexes.FusionResult
		if len(pq.query.Embeddings) > 0 || len(pq.query.SparseEmbeddings) > 0 {
			mc := pq.query.MergeConfig
			mergeStart := time.Now()
			if mc != nil && mc.Strategy != nil && *mc.Strategy == indexes.MergeStrategyRsf {
				windowSize := mc.WindowSize
				if windowSize <= 0 {
					windowSize = pq.query.Limit
				}
				var weights map[string]float64
				if mc.Weights != nil {
					weights = *mc.Weights
				}
				fusionResult = merged.RSFResults(pq.query.Limit, windowSize, weights)
			} else {
				rankConstant := 60.0
				if mc != nil && mc.RankConstant != 0 {
					rankConstant = mc.RankConstant
				}
				var weights map[string]float64
				if mc != nil && mc.Weights != nil {
					weights = *mc.Weights
				}
				fusionResult = merged.RRFResults(pq.query.Limit, rankConstant, weights)
			}

			if profile != nil {
				profile.Merge = buildMergeProfile(mc, merged, time.Since(mergeStart))
			}

			fusionResult.Aggregations = merged.AggregationResults

			hadSemanticHits := len(fusionResult.Hits) > 0
			if indexes.ShouldUseHybridFullTextFallback(
				pq.query.HybridFullTextMode,
				pq.hybridFallback,
				hadSemanticHits,
			) {
				results[origIdx] = t.executeHybridFullTextFallback(
					ctx,
					queryShardIndexes[origIdx],
					pq.queryReq,
					pq.query,
					pq.graphSearches,
					table,
					profile,
					pq.hybridFallback,
				)
				continue
			}
			indexes.ApplyPrunerToFusionResult(fusionResult, pq.query.Pruner)

			// Convert to QueryResult
			results[origIdx] = t.fusionResultToQueryResult(ctx, fusionResult, pq.graphSearches, table, profile)
		} else {
			// Full-text search only
			results[origIdx] = t.bleveResultToQueryResult(ctx, merged.BleveSearchResult, pq.queryReq, pq.query, pq.graphSearches, table, profile)
		}
	}

	return results
}

// runGraphSearches executes graph searches if any are present and returns the results.
// Returns nil if graphSearches is empty or on error (errors are logged, not propagated).
func (t *TableApi) runGraphSearches(
	ctx context.Context,
	table *store.Table,
	graphSearches map[string]*indexes.GraphQuery,
	searchResult *indexes.RemoteIndexSearchResult,
) map[string]indexes.GraphQueryResult {
	if len(graphSearches) == 0 {
		return nil
	}
	coordinator := NewGraphQueryCoordinator(t.ln)
	graphResults, err := coordinator.ExecuteGraphSearches(ctx, table, graphSearches, searchResult)
	if err != nil {
		t.logger.Warn("Failed to execute graph searches", zap.Error(err))
		return nil
	}
	return graphResults
}

// fusionResultToQueryResult converts a FusionResult to QueryResult
func (t *TableApi) fusionResultToQueryResult(
	ctx context.Context,
	fusionResult *indexes.FusionResult,
	graphSearches map[string]*indexes.GraphQuery,
	table *store.Table,
	profile *QueryProfile,
) QueryResult {
	queryResult := QueryResult{Status: http.StatusOK}

	resp := make([]QueryHit, len(fusionResult.Hits))
	for i, r := range fusionResult.Hits {
		qh := QueryHit{
			Source: r.Fields,
			ID:     r.ID,
			Score:  r.Score,
		}
		if r.RerankedScore != nil {
			qh.Score = *r.RerankedScore
		}
		indexScores := map[string]any{}
		for idx, score := range r.IndexScores {
			indexScores[idx] = score
		}
		qh.IndexScores = indexScores
		resp[i] = qh
	}

	queryResult.Hits.Hits = resp
	queryResult.Hits.MaxScore = fusionResult.MaxScore
	queryResult.Took = fusionResult.Took
	queryResult.Hits.Total = fusionResult.Total
	queryResult.Profile = profile

	queryResult.GraphResults = t.runGraphSearches(ctx, table, graphSearches,
		&indexes.RemoteIndexSearchResult{FusionResult: fusionResult})

	// Process aggregations
	if fusionResult.Aggregations != nil {
		queryResult.Aggregations = t.convertAggregationResults(fusionResult.Aggregations)
	}

	return queryResult
}

// computeAnalyses extracts embedding vectors from hits and computes PCA/t-SNE analyses.
// Returns nil if computation fails.
func (t *TableApi) computeAnalyses(queryReq *QueryRequest, hits []QueryHit) map[string]AnalysesResult {
	analyses := map[string]AnalysesResult{}
	vectorSets := map[string]*vector.Set{}
	for _, hit := range hits {
		embMap, ok := hit.Source["_embeddings"].(map[string]any)
		if !ok {
			continue
		}
		for indexName, embValue := range embMap {
			embArray, ok := embValue.([]any)
			if !ok {
				continue
			}
			embedding := make([]float32, len(embArray))
			for i, v := range embArray {
				switch val := v.(type) {
				case float64:
					embedding[i] = float32(val)
				case float32:
					embedding[i] = val
				case json.Number:
					if f, err := val.Float64(); err == nil {
						embedding[i] = float32(f)
					}
				}
			}
			if _, exists := vectorSets[indexName]; !exists {
				vectorSets[indexName] = vector.MakeSet(len(embedding))
			}
			vectorSets[indexName].Add(embedding)
		}
	}
	for indexName, vectorSet := range vectorSets {
		embeddings := stats.DenseMatrixFromVectorSet(vectorSet)
		analysesRes := AnalysesResult{}
		if queryReq.Analyses.Pca {
			proj, err := stats.PCA(embeddings)
			if err != nil {
				t.logger.Warn("Failed to compute PCA", zap.String("index", indexName), zap.Error(err))
				return nil
			}
			analysesRes.Pca = stats.MatrixToFlat(proj)
		}
		if queryReq.Analyses.Tsne {
			tsne := stats.TSNE(embeddings, 30.0, 200.0, 1000)
			analysesRes.Tsne = stats.MatrixToFlat(tsne)
		}
		analyses[indexName] = analysesRes
	}
	return analyses
}

// bleveResultToQueryResult converts a Bleve SearchResult to QueryResult
func (t *TableApi) bleveResultToQueryResult(
	ctx context.Context,
	searchResult *bleve.SearchResult,
	queryReq *QueryRequest,
	q *indexes.Query,
	graphSearches map[string]*indexes.GraphQuery,
	table *store.Table,
	profile *QueryProfile,
) QueryResult {
	if searchResult == nil {
		return QueryResult{Status: http.StatusOK}
	}

	queryResult := QueryResult{Status: http.StatusOK, Profile: profile}

	resp := make([]QueryHit, len(searchResult.Hits))
	for i, r := range searchResult.Hits {
		filteredFields := make(map[string]any)
		if len(q.Fields) == 0 {
			filteredFields = r.Fields
		}
		for _, col := range q.Fields {
			if field, ok := r.Fields[col]; ok {
				filteredFields[col] = field
			}
		}
		resp[i] = QueryHit{
			Source: filteredFields,
			Score:  r.Score,
			ID:     r.ID,
		}
		if len(r.Sort) > 0 {
			resp[i].Sort = r.Sort
		}
	}

	if len(resp) > 0 && (len(q.Fields) != 0 || queryReq.FullTextSearch != nil || !q.Count) {
		queryResult.Hits.Hits = resp
	}
	queryResult.Hits.Total = searchResult.Total
	queryResult.Hits.MaxScore = searchResult.MaxScore
	queryResult.Took = searchResult.Took

	queryResult.GraphResults = t.runGraphSearches(ctx, table, graphSearches,
		&indexes.RemoteIndexSearchResult{BleveSearchResult: searchResult})

	// Process aggregations
	if searchResult.Aggregations != nil {
		queryResult.Aggregations = t.convertAggregationResults(searchResult.Aggregations)
	}

	return queryResult
}

// buildShardsProfile creates a QueryProfile with shard statistics from the search status.
// Returns nil if profiling data is not available.
func buildShardsProfile(status *indexes.RemoteIndexSearchStatus) *QueryProfile {
	if status == nil {
		return nil
	}
	return &QueryProfile{
		Shards: ShardsProfile{
			Total:      status.Total,
			Successful: status.Successful,
			Failed:     status.Failed,
		},
	}
}

// buildMergeProfile creates a MergeProfile from merge config and pre-merge hit counts.
func buildMergeProfile(mc *indexes.MergeConfig, merged *indexes.RemoteIndexSearchResult, mergeDuration time.Duration) MergeProfile {
	mp := MergeProfile{
		DurationMs: mergeDuration.Milliseconds(),
	}
	if mc != nil && mc.Strategy != nil {
		mp.Strategy = *mc.Strategy
	}
	if merged.BleveSearchResult != nil {
		mp.FullTextHits = len(merged.BleveSearchResult.Hits)
	}
	for _, vr := range merged.VectorSearchResult {
		if vr != nil {
			mp.SemanticHits += len(vr.Hits)
		}
	}
	return mp
}

// convertAggregationResults converts bleve AggregationResults to API AggregationResult types
func (t *TableApi) convertAggregationResults(aggs search.AggregationResults) map[string]AggregationResult {
	if aggs == nil {
		return nil
	}

	results := make(map[string]AggregationResult)
	for name, agg := range aggs {
		result := AggregationResult{}

		// Handle metric aggregation values
		if agg.Value != nil {
			switch v := agg.Value.(type) {
			case float64:
				result.Value = &v
			case int64:
				fv := float64(v)
				result.Value = &fv
			case *search.AvgResult:
				result.Avg = &v.Avg
				count := int(v.Count)
				result.Count = &count
				result.Sum = &v.Sum
			case *search.StatsResult:
				result.Avg = &v.Avg
				result.Min = &v.Min
				result.Max = &v.Max
				result.Sum = &v.Sum
				count := int(v.Count)
				result.Count = &count
				result.StdDeviation = &v.StdDev
			case *search.CardinalityResult:
				// For cardinality, we use the Value field
				cardinality := float64(v.Cardinality)
				result.Value = &cardinality
			}
		}

		// Handle bucket aggregations
		if len(agg.Buckets) > 0 {
			buckets := make([]AggregationBucket, len(agg.Buckets))
			for i, b := range agg.Buckets {
				bucket := AggregationBucket{
					DocCount: int(b.Count),
				}

				// Convert bucket key to string
				switch key := b.Key.(type) {
				case string:
					bucket.Key = key
				case float64:
					bucket.Key = fmt.Sprintf("%v", key)
					// Store formatted number as key_as_string
					keyStr := fmt.Sprintf("%v", key)
					bucket.KeyAsString = &keyStr
				case int64:
					bucket.Key = fmt.Sprintf("%v", key)
					keyStr := fmt.Sprintf("%v", key)
					bucket.KeyAsString = &keyStr
				default:
					bucket.Key = fmt.Sprintf("%v", key)
				}

				// Handle sub-aggregations recursively
				if len(b.Aggregations) > 0 {
					subAggs := t.convertAggregationResults(b.Aggregations)
					bucket.SubAggregations = subAggs
				}

				buckets[i] = bucket
			}
			result.Buckets = buckets
		}

		results[name] = result
	}

	return results
}
