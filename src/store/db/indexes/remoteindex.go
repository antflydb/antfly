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

package indexes

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/antflydb/antfly/lib/reranking"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vectorindex"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/common"
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
	bleveindex "github.com/blevesearch/bleve_index_api"
)

type FieldFilter struct {
	Fields    []string `json:"fields,omitempty"`
	CountStar bool     `json:"count_star,omitempty"`
	Star      bool     `json:"star,omitempty"`
}
type RemoteIndex struct {
	client *http.Client
	urls   []string
	shard  types.ID

	mapping mapping.IndexMapping
	schema  *schema.TableSchema

	q *FieldFilter
}

// NewRemoteIndex creates a new client connecting to a remote bleve index
func NewRemoteIndex(client *http.Client, urls []string, shard types.ID) (*RemoteIndex, error) {
	return &RemoteIndex{
		client: client,
		urls:   urls,
		shard:  shard,
	}, nil
}

func (r *RemoteIndex) Search(req *bleve.SearchRequest) (*bleve.SearchResult, error) {
	res, err := r.SearchInContext(context.Background(), req)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, fmt.Errorf("search result is nil from shard %s", r.shard)
	}
	return res, nil
}

type RemoteIndexSearchRequest struct {
	Columns             []string              `json:"columns,omitempty"`
	CountStar           bool                  `json:"count_star,omitempty"`
	Limit               int                   `json:"limit,omitempty"` // Limit for the number of results
	BlevePagingOpts     FullTextPagingOptions `json:"bleve_paging_options"`
	VectorPagingOpts    VectorPagingOptions   `json:"vector_paging_options"`
	Star                bool                  `json:"star,omitempty"`
	FilterPrefix        []byte                `json:"filter_prefix,omitempty"`         // Prefix for filtering keys
	FilterQuery         json.RawMessage       `json:"filter_query,omitzero,omitempty"` // Query for filtering documents
	AggregationRequests AggregationRequests   `json:"aggregations,omitzero,omitempty"`

	RerankerConfig   *reranking.RerankerConfig `json:"reranker,omitempty,omitzero"`
	RerankerTemplate string                    `json:"reranker_template,omitempty,omitzero"`
	RerankerField    string                    `json:"reranker_field,omitempty,omitzero"`
	RerankerQuery    string                    `json:"reranker_query,omitempty,omitzero"`

	// Used for bleve only search interface
	BleveSearchRequest   *bleve.SearchRequest `json:"bleve_search,omitempty"` // Query for bleve or rrf
	FullTextIndexVersion uint32               `json:"full_text_index_version,omitempty"`

	VectorSearches map[string]vector.T `json:"vector_searches,omitzero,omitempty"`

	// SparseSearches maps sparse embeddings index names to pre-computed sparse vectors.
	// Sparse embedding is done once at the metadata level and broadcast to all shards.
	SparseSearches map[string]SparseVec `json:"sparse_searches,omitzero,omitempty"`

	// Graph searches (executed after full-text/vector searches)
	GraphSearches map[string]*GraphQuery `json:"graph_searches,omitempty"`

	// Fusion configuration for combining results
	MergeConfig *MergeConfig `json:"merge_config,omitempty"`

	// ExpandStrategy defines how to incorporate graph search results with other search results
	// Options: "union" (merge all results), "intersection" (only common results), "" (keep separate)
	ExpandStrategy string `json:"expand_strategy,omitempty"`
}

// FusionKeyFullText is the named weight key for the full-text search index.
const FusionKeyFullText = "full_text"

func (res *RemoteIndexSearchResult) RRFResults(limit int, rankConstant float64, weights map[string]float64) *FusionResult {
	// Combine the vector search results using Reciprocal Rank Fusion
	//
	// --- RRF Calculation ---
	docScores := make(map[string]*FusionHit)

	// Helper to look up a named weight, defaulting to 1.0
	weight := func(key string) float64 {
		if weights == nil {
			return 1.0
		}
		if w, ok := weights[key]; ok {
			return w
		}
		return 1.0
	}

	// Process Bleve results
	if res.BleveSearchResult != nil {
		res.Total = res.BleveSearchResult.Total
		w := weight(FusionKeyFullText)
		for rank, hit := range res.BleveSearchResult.Hits {
			docScores[hit.ID] = &FusionHit{
				ID:     hit.ID,
				Fields: hit.Fields,
				IndexScores: map[string]float64{
					hit.Index: hit.Score,
				},
			}
			// Bleve ranks start from 0, formula uses rank starting from 1
			docScores[hit.ID].Score += w * 1.0 / (rankConstant + float64(rank+1))
		}
	}

	// Process Vector results
	if res.VectorSearchResult != nil {
		for indexName, vectorRes := range res.VectorSearchResult {
			// If Bleve didn't return results, we can use the total from Vector search
			res.Total = max(res.Total, vectorRes.Total)
			w := weight(indexName)
			for rank, hit := range vectorRes.Hits {
				score := float64(hit.Distance)
				if hit.Score != 0 {
					score = float64(hit.Score)
				}
				if _, exists := docScores[hit.ID]; !exists {
					// If Bleve didn't find it, create a new entry
					docScores[hit.ID] = &FusionHit{
						ID:     hit.ID,
						Fields: hit.Fields,
						IndexScores: map[string]float64{
							// Vector distance is inverted for scoring
							hit.Index: score,
						},
					}
				} else {
					// If it exists, just update the score for this index
					docScores[hit.ID].IndexScores[hit.Index] = score
				}
				// Vector ranks start from 0, formula uses rank starting from 1
				docScores[hit.ID].Score += w * 1.0 / (rankConstant + float64(rank+1))
			}
		}
	}

	// Combine results into a slice for sorting
	lenExistingRRFHits := 0
	if res.FusionResult != nil {
		lenExistingRRFHits = len(res.FusionResult.Hits)
	}
	combinedResults := make([]*FusionHit, 0, len(docScores)+lenExistingRRFHits)
	if res.FusionResult != nil && res.FusionResult.Hits != nil {
		combinedResults = append(combinedResults, res.FusionResult.Hits...)
	}
	for _, doc := range docScores {
		combinedResults = append(combinedResults, doc)
	}

	// Sort by RRF score descending
	slices.SortStableFunc(combinedResults, func(x, y *FusionHit) int {
		if x.RerankedScore != nil && y.RerankedScore != nil {
			return -cmp.Compare(*x.RerankedScore, *y.RerankedScore)
		}
		return -cmp.Compare(x.Score, y.Score)
	})

	// --- Construct Final Bleve SearchResult ---
	finalResult := &FusionResult{
		Hits:  make([]*FusionHit, 0, min(len(combinedResults), limit)),
		Total: res.Total,
	}

	maxScore := 0.0

	// Populate final hits, up to K results
	for i := range min(len(combinedResults), limit) {
		rrfDoc := combinedResults[i]
		if rrfDoc.RerankedScore != nil {
			maxScore = max(*rrfDoc.RerankedScore, maxScore)
		} else {
			maxScore = max(rrfDoc.Score, maxScore)
		}
		finalResult.Hits = append(finalResult.Hits, rrfDoc)
	}

	finalResult.MaxScore = maxScore
	return finalResult
}

// RSFResults implements Relative Score Fusion by normalizing scores within a window
// and combining them with named weights. The weights map is keyed by index name
// (FusionKeyFullText for full-text, embedding index names for vector indexes).
// Missing keys default to weight 1.0.
func (res *RemoteIndexSearchResult) RSFResults(limit int, windowSize int, weights map[string]float64) *FusionResult {
	if windowSize <= 0 {
		windowSize = limit
	}

	// Helper to look up a named weight, defaulting to 1.0
	namedWeight := func(key string) float64 {
		if weights == nil {
			return 1.0
		}
		if w, ok := weights[key]; ok {
			return w
		}
		return 1.0
	}

	// Collect all hits with their scores
	docScores := make(map[string]*FusionHit)

	// Process Bleve results (full-text search)
	if res.BleveSearchResult != nil {
		res.Total = res.BleveSearchResult.Total
		bleveHits := res.BleveSearchResult.Hits

		// Limit to window size for normalization
		windowLimit := min(len(bleveHits), windowSize)
		if windowLimit > 0 {
			// Find min and max scores in the window
			maxScore := bleveHits[0].Score
			minScore := bleveHits[windowLimit-1].Score
			denom := maxScore - minScore
			w := namedWeight(FusionKeyFullText)

			for i := range windowLimit {
				hit := bleveHits[i]
				norm := 1.0
				if denom > 0 {
					norm = (hit.Score - minScore) / denom
				}

				docScores[hit.ID] = &FusionHit{
					ID:     hit.ID,
					Fields: hit.Fields,
					IndexScores: map[string]float64{
						hit.Index: hit.Score,
					},
					Score: w * norm,
				}
			}
		}
	}

	// Process Vector results with named weights
	if res.VectorSearchResult != nil {
		for indexName, vectorRes := range res.VectorSearchResult {
			// If Bleve didn't return results, we can use the total from Vector search
			res.Total = max(res.Total, vectorRes.Total)

			// Limit to window size for normalization
			windowLimit := min(len(vectorRes.Hits), windowSize)
			if windowLimit == 0 {
				continue
			}

			// Collect scores for normalization
			scores := make([]float64, windowLimit)
			for i := range windowLimit {
				hit := vectorRes.Hits[i]
				score := float64(hit.Distance)
				if hit.Score != 0 {
					score = float64(hit.Score)
				}
				scores[i] = score
			}

			// Find min and max scores in the window
			maxScore := scores[0]
			minScore := scores[0]
			for _, s := range scores {
				if s > maxScore {
					maxScore = s
				}
				if s < minScore {
					minScore = s
				}
			}
			denom := maxScore - minScore

			w := namedWeight(indexName)

			for i := range windowLimit {
				hit := vectorRes.Hits[i]
				score := scores[i]
				norm := 1.0
				if denom > 0 {
					norm = (score - minScore) / denom
				}

				if existing, exists := docScores[hit.ID]; exists {
					// Document already exists, add to score
					existing.Score += w * norm
					existing.IndexScores[indexName] = score
				} else {
					// New document
					docScores[hit.ID] = &FusionHit{
						ID:     hit.ID,
						Fields: hit.Fields,
						IndexScores: map[string]float64{
							indexName: score,
						},
						Score: w * norm,
					}
				}
			}
		}
	}

	// Combine results into a slice for sorting
	lenExistingRRFHits := 0
	if res.FusionResult != nil {
		lenExistingRRFHits = len(res.FusionResult.Hits)
	}
	combinedResults := make([]*FusionHit, 0, len(docScores)+lenExistingRRFHits)
	if res.FusionResult != nil && res.FusionResult.Hits != nil {
		combinedResults = append(combinedResults, res.FusionResult.Hits...)
	}
	for _, doc := range docScores {
		combinedResults = append(combinedResults, doc)
	}

	// Sort by RSF score descending
	slices.SortStableFunc(combinedResults, func(x, y *FusionHit) int {
		if x.RerankedScore != nil && y.RerankedScore != nil {
			return -cmp.Compare(*x.RerankedScore, *y.RerankedScore)
		}
		return -cmp.Compare(x.Score, y.Score)
	})

	// Construct final result
	finalResult := &FusionResult{
		Hits:  make([]*FusionHit, 0, min(len(combinedResults), limit)),
		Total: res.Total,
	}

	maxScore := 0.0

	// Populate final hits, up to limit results
	for i := range min(len(combinedResults), limit) {
		rsfDoc := combinedResults[i]
		if rsfDoc.RerankedScore != nil {
			maxScore = max(*rsfDoc.RerankedScore, maxScore)
		} else {
			maxScore = max(rsfDoc.Score, maxScore)
		}
		finalResult.Hits = append(finalResult.Hits, rsfDoc)
	}

	finalResult.MaxScore = maxScore
	return finalResult
}

type RemoteIndexSearchResult struct {
	Took               time.Duration                        `json:"took,omitempty"`
	Status             *RemoteIndexSearchStatus             `json:"status,omitempty"`
	Total              uint64                               `json:"total,omitempty"`
	AggregationResults search.AggregationResults            `json:"aggregations,omitempty"`
	BleveSearchResult  *bleve.SearchResult                  `json:"bleve_search_result,omitempty"`
	VectorSearchResult map[string]*vectorindex.SearchResult `json:"search_result,omitempty"`
	FusionResult       *FusionResult                        `json:"fusion_result,omitempty"`

	// Graph query results keyed by query name
	GraphResults map[string]*GraphQueryResult `json:"graph_results,omitempty"`
}

type RemoteIndexSearchStatus struct {
	Total      int              `json:"total"`
	Failed     int              `json:"failed"`
	Successful int              `json:"successful"`
	Errors     map[string]error `json:"errors,omitempty"`

	// Component-specific status
	BleveStatus  *SearchComponentStatus            `json:"bleve_status,omitempty"`
	VectorStatus map[string]*SearchComponentStatus `json:"vector_status,omitempty"`
	GraphStatus  map[string]*SearchComponentStatus `json:"graph_status,omitempty"`
}

// SearchComponentStatus tracks success/failure of individual search components
type SearchComponentStatus struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
	Partial bool   `json:"partial,omitempty"` // Some shards failed

	// For cross-shard operations
	ShardsQueried int `json:"shards_queried,omitempty"`
	ShardsFailed  int `json:"shards_failed,omitempty"`
}

func (ss *RemoteIndexSearchStatus) Merge(other *RemoteIndexSearchStatus) {
	if ss == nil {
		ss = other
	}
	if other == nil {
		return
	}
	ss.Total += other.Total
	ss.Failed += other.Failed
	ss.Successful += other.Successful
	if len(other.Errors) > 0 {
		if ss.Errors == nil {
			ss.Errors = make(map[string]error)
		}
		maps.Copy(ss.Errors, other.Errors)
	}
}

func MakeIndexesForShards(
	client *http.Client,
	tableSchema *schema.TableSchema,
	peers map[types.ID][]string,
	ff *FieldFilter,
) (RemoteIndexes, error) {
	indexes := []*RemoteIndex{}

	// For each shard, use the provided peer URLs to create remote indexes
	for shardID, peerURLs := range peers {
		if len(peerURLs) == 0 {
			return nil, fmt.Errorf("no peer URLs found for shard %s", shardID)
		}
		slices.Sort(peerURLs)
		// Create a remote index
		remoteIndex, err := NewRemoteIndex(client, peerURLs, shardID)
		if err != nil {
			return nil, fmt.Errorf("creating remote index: %v", err)
		}
		remoteIndex.q = ff
		remoteIndex.mapping = schema.NewIndexMapFromSchema(tableSchema)
		remoteIndex.schema = tableSchema
		indexes = append(indexes, remoteIndex)
	}

	return indexes, nil
}

type RemoteIndexes []*RemoteIndex

type FullTextPagingOptions struct {
	OrderBy      []SortField
	Limit        int
	Offset       int
	SearchAfter  []string
	SearchBefore []string
}

// applyFullTextPaging applies sort order and cursor-based pagination to a bleve
// search request. SearchAfter/SearchBefore override Offset (setting From to 0).
func applyFullTextPaging(req *bleve.SearchRequest, opts FullTextPagingOptions) {
	req.Size = opts.Limit
	req.From = opts.Offset
	for _, sf := range opts.OrderBy {
		desc := sf.Desc != nil && *sf.Desc
		req.Sort = append(req.Sort, &search.SortField{Field: sf.Field, Desc: desc})
	}
	if len(opts.SearchAfter) > 0 {
		req.SearchAfter = opts.SearchAfter
		req.From = 0
	}
	if len(opts.SearchBefore) > 0 {
		req.SearchBefore = opts.SearchBefore
		req.From = 0
	}
}

type VectorPagingOptions struct {
	OrderBy       []SortField
	Limit         int
	DistanceUnder *float32
	DistanceOver  *float32
	SearchEffort  *float32
}

// DateTimeRange represents a datetime range for aggregations
type DateTimeRange struct {
	Name  string  `json:"name,omitempty"`
	Start *string `json:"start,omitempty"`
	End   *string `json:"end,omitempty"`
}

// NumericRange represents a numeric range for aggregations
type NumericRange struct {
	Name  string   `json:"name,omitempty"`
	Start *float64 `json:"start,omitempty"`
	End   *float64 `json:"end,omitempty"`
}

// DistanceRange represents a distance range for geo_distance aggregations
type DistanceRange struct {
	Name string   `json:"name,omitempty"`
	From *float64 `json:"from,omitempty"`
	To   *float64 `json:"to,omitempty"`
}

// AggregationRequests groups together all aggregation requests
type AggregationRequests map[string]*AggregationRequest

// AggregationRequest describes an aggregation to be computed over the result set
type AggregationRequest struct {
	Type  string `json:"type"`
	Field string `json:"field"`

	// Bucket aggregation configuration
	Size                  int                 `json:"size,omitzero"`
	Precision             uint8               `json:"precision,omitzero"`
	Interval              float64             `json:"interval,omitzero"`
	CalendarInterval      string              `json:"calendar_interval,omitempty"`
	FixedInterval         string              `json:"fixed_interval,omitempty"`
	NumericRanges         []*NumericRange     `json:"ranges,omitempty"`
	DateTimeRanges        []*DateTimeRange    `json:"date_ranges,omitempty"`
	DistanceRanges        []*DistanceRange    `json:"distance_ranges,omitempty"`
	CenterLat             float64             `json:"center_lat,omitzero"`
	CenterLon             float64             `json:"center_lon,omitzero"`
	DistanceUnit          string              `json:"distance_unit,omitempty"`
	GeohashPrecision      int                 `json:"geohash_precision,omitzero"`
	SignificanceAlgorithm string              `json:"significance_algorithm,omitempty"`
	TermPrefix            string              `json:"term_prefix,omitempty"`
	TermPattern           string              `json:"term_pattern,omitempty"`
	MinDocCount           int64               `json:"min_doc_count,omitzero"`
	Aggregations          AggregationRequests `json:"aggregations,omitempty"`
}

func (r RemoteIndexes) FullTextSearch(
	ctx context.Context,
	q query.Query,
	facetOptions AggregationRequests,
	pagingOpts FullTextPagingOptions,
) (*bleve.SearchResult, error) {
	if len(r) == 0 {
		return nil, errors.New("no indexes available")
	}
	// Bleve requires a non-zero limit, so we set a default if not provided
	if pagingOpts.Limit == 0 {
		pagingOpts.Limit = 1000
	}
	searchReq := bleve.NewSearchRequest(q)
	for facetName, facetOpt := range facetOptions {
		size := facetOpt.Size
		size = max(size, 10)
		facetReq := bleve.NewFacetRequest(facetOpt.Field, size)
		for _, dr := range facetOpt.DateTimeRanges {
			facetReq.AddDateTimeRangeString(dr.Name, dr.Start, dr.End)
		}
		for _, nr := range facetOpt.NumericRanges {
			facetReq.AddNumericRange(nr.Name, nr.Start, nr.End)
		}
		searchReq.AddFacet(facetName, facetReq)
	}
	applyFullTextPaging(searchReq, pagingOpts)
	index := bleve.NewIndexAlias()
	if err := index.SetIndexMapping(r[0].mapping); err != nil {
		return nil, fmt.Errorf("setting index mapping: %w", err)
	}
	for _, i := range r {
		index.Add(i)
	}
	origCtx := ctx
	ctx = r.withGlobalScoringCtx(ctx)
	searchResults, err := index.SearchInContext(ctx, searchReq)
	if err != nil && ctx.Value(search.SearchTypeKey) == search.GlobalScoring && isBM25FieldStatError(err) {
		searchResults, err = index.SearchInContext(origCtx, searchReq)
	}
	if err != nil {
		return nil, fmt.Errorf("searching index: %w", err)
	}
	return searchResults, nil
}

// withGlobalScoringCtx enables global BM25 scoring so IDF and average document
// length are computed from the combined corpus rather than per-shard.
// GlobalScoring requires the _all field to have content; without it, bleve
// cannot compute cross-shard BM25 stats and every search fails with
// "field stat for bm25 not present _all".
func (r RemoteIndexes) withGlobalScoringCtx(ctx context.Context) context.Context {
	if len(r) > 1 {
		if m, ok := r[0].mapping.(*mapping.IndexMappingImpl); ok && m.ScoringModel == bleveindex.BM25Scoring {
			// Only enable GlobalScoring when the _all document mapping is
			// enabled (i.e., at least one field has IncludeInAll=true).
			if allMapping, ok := m.TypeMapping["_all"]; ok && allMapping.Enabled {
				return context.WithValue(ctx, search.SearchTypeKey, search.GlobalScoring)
			}
		}
	}
	return ctx
}

// isBM25FieldStatError returns true if the error is caused by missing BM25
// field statistics (e.g. an empty index in a GlobalScoring alias).
func isBM25FieldStatError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "field stat for bm25 not present")
}

func (r *RemoteIndex) RemoteSearch(
	ctx context.Context,
	req *RemoteIndexSearchRequest,
) (*RemoteIndexSearchResult, error) {
	version := uint32(0)
	if r.schema != nil {
		version = r.schema.Version
	}
	req.FullTextIndexVersion = version
	reqBytes, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal search request: %w", err)
	}

	// FIXME (ajr) Try failing over to other URLs if the first one fails
	hreq, err := common.NewShardRequest(
		r.shard,
		http.MethodPost,
		r.urls[0]+"/search",
		bytes.NewReader(reqBytes),
	)
	if err != nil {
		return nil, fmt.Errorf("creating search request: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq = hreq.WithContext(ctx)

	resp, err := r.client.Do(hreq) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return nil, fmt.Errorf("requesting search: %w", err)
	}

	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("searching error: %s, body: %s", resp.Status, string(bodyBytes))
	}
	decoder := json.NewDecoder(resp.Body)
	var result RemoteIndexSearchResult
	if err := decoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("unmarshalling search result: %w", err)
	}

	return &result, nil
}

// BatchRemoteSearch executes multiple search requests against this shard in a single HTTP call
// using ndjson format (newline-delimited JSON)
func (r *RemoteIndex) BatchRemoteSearch(
	ctx context.Context,
	reqs []*RemoteIndexSearchRequest,
) ([]*RemoteIndexSearchResult, []error) {
	if len(reqs) == 0 {
		return nil, nil
	}

	version := uint32(0)
	if r.schema != nil {
		version = r.schema.Version
	}

	// Build ndjson body
	var buf bytes.Buffer
	for i, req := range reqs {
		req.FullTextIndexVersion = version
		encoded, err := json.Marshal(req)
		if err != nil {
			errors := make([]error, len(reqs))
			errors[i] = fmt.Errorf("failed to marshal search request: %w", err)
			return nil, errors
		}
		buf.Write(encoded)
		if i < len(reqs)-1 {
			buf.WriteByte('\n')
		}
	}

	hreq, err := common.NewShardRequest(
		r.shard,
		http.MethodPost,
		r.urls[0]+"/search",
		&buf,
	)
	if err != nil {
		return nil, []error{fmt.Errorf("creating batch search request: %w", err)}
	}
	hreq.Header.Set("Content-Type", "application/x-ndjson")
	hreq = hreq.WithContext(ctx)

	resp, err := r.client.Do(hreq) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return nil, []error{fmt.Errorf("requesting batch search: %w", err)}
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, []error{fmt.Errorf("batch search error: %s, body: %s", resp.Status, string(bodyBytes))}
	}

	// Parse ndjson response
	results := make([]*RemoteIndexSearchResult, len(reqs))
	errors := make([]error, len(reqs))
	decoder := json.NewDecoder(resp.Body)
	for i := range reqs {
		var result RemoteIndexSearchResult
		if err := decoder.Decode(&result); err != nil {
			if err == io.EOF {
				break
			}
			errors[i] = fmt.Errorf("unmarshalling search result: %w", err)
			continue
		}
		results[i] = &result
	}

	return results, errors
}

// BatchMultiSearch executes multiple search requests across multiple shards,
// batching requests to the same shard for efficiency.
// Returns results in the same order as the input requests.
func BatchMultiSearch(
	ctx context.Context,
	reqs []*RemoteIndexSearchRequest,
	indexes []*RemoteIndex,
) ([]*RemoteIndexSearchResult, error) {
	if len(reqs) != len(indexes) {
		return nil, fmt.Errorf("mismatched requests and indexes: %d vs %d", len(reqs), len(indexes))
	}
	if len(reqs) == 0 {
		return nil, nil
	}

	// Group requests by shard
	type indexedReq struct {
		originalIdx int
		req         *RemoteIndexSearchRequest
	}
	shardRequests := make(map[string][]indexedReq)
	shardIndex := make(map[string]*RemoteIndex)

	for i, idx := range indexes {
		shardName := idx.Name()
		shardRequests[shardName] = append(shardRequests[shardName], indexedReq{
			originalIdx: i,
			req:         reqs[i],
		})
		shardIndex[shardName] = idx
	}

	// Execute batch requests per shard in parallel
	results := make([]*RemoteIndexSearchResult, len(reqs))
	var mu sync.Mutex
	var wg sync.WaitGroup

	for shardName, indexedReqs := range shardRequests {
		wg.Add(1)
		go func(shardName string, indexedReqs []indexedReq) {
			defer wg.Done()

			idx := shardIndex[shardName]
			batchReqs := make([]*RemoteIndexSearchRequest, len(indexedReqs))
			for i, ir := range indexedReqs {
				batchReqs[i] = ir.req
			}

			batchResults, batchErrors := idx.BatchRemoteSearch(ctx, batchReqs)

			mu.Lock()
			for i, ir := range indexedReqs {
				if i < len(batchResults) && batchResults[i] != nil {
					results[ir.originalIdx] = batchResults[i]
				} else if i < len(batchErrors) && batchErrors[i] != nil {
					// Create an error result
					results[ir.originalIdx] = &RemoteIndexSearchResult{
						Status: &RemoteIndexSearchStatus{
							Total:  1,
							Failed: 1,
							Errors: map[string]error{shardName: batchErrors[i]},
						},
					}
				}
			}
			mu.Unlock()
		}(shardName, indexedReqs)
	}

	wg.Wait()

	return results, nil
}

func (r *RemoteIndex) SearchInContext(
	ctx context.Context,
	req *bleve.SearchRequest,
) (*bleve.SearchResult, error) {
	version := uint32(0)
	if r.schema != nil {
		version = r.schema.Version
	}
	riReq := RemoteIndexSearchRequest{BleveSearchRequest: req, FullTextIndexVersion: version}
	if r.q != nil {
		if r.q.CountStar {
			riReq.CountStar = true
		} else if r.q.Star {
			riReq.Star = true
		} else {
			riReq.Columns = r.q.Fields
		}
	}
	reqBytes, err := json.Marshal(riReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal search request: %w", err)
	}

	// FIXME (ajr) Try failing over to other URLs if the first one fails
	hreq, err := common.NewShardRequest(
		r.shard,
		http.MethodPost,
		r.urls[0]+"/search",
		bytes.NewReader(reqBytes),
	)
	if err != nil {
		return nil, fmt.Errorf("creating search request: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")

	resp, err := r.client.Do(hreq) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return nil, fmt.Errorf("requesting search: %w", err)
	}

	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("searching error: %s, body: %s", resp.Status, string(bodyBytes))
	}
	decoder := json.NewDecoder(resp.Body)
	var result RemoteIndexSearchResult
	if err := decoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("unmarshalling search result: %w", err)
	}

	return result.BleveSearchResult, nil
}

func (r *RemoteIndex) IndexSynonym(
	id string,
	collection string,
	definition *bleve.SynonymDefinition,
) error {
	return errors.New("operation not supported remotely")
}

func (r *RemoteIndex) Index(id string, data any) error {
	return errors.New("operation not supported remotely")
}

func (r *RemoteIndex) Delete(id string) error {
	return errors.New("operation not supported remotely")
}

func (r *RemoteIndex) NewBatch() *bleve.Batch {
	panic("operation not supported remotely")
}

func (r *RemoteIndex) Batch(b *bleve.Batch) error {
	return errors.New("operation not supported remotely")
}

func (r *RemoteIndex) Document(id string) (bleveindex.Document, error) {
	return nil, errors.New("operation not supported remotely")
}

func (r *RemoteIndex) DocCount() (uint64, error) {
	return 0, errors.New("operation not supported remotely")
}

func (r *RemoteIndex) Fields() ([]string, error) {
	return nil, errors.New("operation not implemented remotely")
}

func (r *RemoteIndex) FieldDict(field string) (bleveindex.FieldDict, error) {
	return nil, errors.New("operation not implemented remotely")
}

func (r *RemoteIndex) FieldDictRange(
	field string,
	startTerm []byte,
	endTerm []byte,
) (bleveindex.FieldDict, error) {
	return nil, errors.New("operation not implemented remotely")
}

func (r *RemoteIndex) FieldDictPrefix(field string, termPrefix []byte) (bleveindex.FieldDict, error) {
	return nil, errors.New("operation not implemented remotely")
}
func (r *RemoteIndex) Close() error                  { return nil }
func (r *RemoteIndex) Mapping() mapping.IndexMapping { return nil }
func (r *RemoteIndex) Stats() *bleve.IndexStat       { return nil }
func (r *RemoteIndex) StatsMap() map[string]any      { return nil }
func (r *RemoteIndex) GetInternal(key []byte) ([]byte, error) {
	return nil, errors.New("operation not implemented remotely")
}

func (r *RemoteIndex) SetInternal(key, val []byte) error {
	return errors.New("operation not implemented remotely")
}

func (r *RemoteIndex) DeleteInternal(key []byte) error {
	return errors.New("operation not implemented remotely")
}
func (r *RemoteIndex) Name() string        { return r.shard.String() }
func (r *RemoteIndex) Type() IndexType     { return IndexTypeFullText }
func (r *RemoteIndex) SetName(name string) {}
func (r *RemoteIndex) Advanced() (bleveindex.Index, error) {
	return nil, errors.New("operation not supported remotely")
}

// MultiSearch executes a SearchRequest across multiple Index objects,
// then merges the results.  The indexes must honor any ctx deadline.
func MultiSearch(
	ctx context.Context,
	req *RemoteIndexSearchRequest,
	indexes ...*RemoteIndex,
) (*RemoteIndexSearchResult, error) {
	searchStart := time.Now()
	asyncResults := make(chan *asyncSearchResult, len(indexes))

	// run search on each index in separate go routine
	var waitGroup sync.WaitGroup

	searchChildIndex := func(in *RemoteIndex, childReq *RemoteIndexSearchRequest) {
		rv := asyncSearchResult{Name: in.Name()}
		rv.Result, rv.Err = in.RemoteSearch(ctx, childReq)
		asyncResults <- &rv
		waitGroup.Done()
	}

	waitGroup.Add(len(indexes))
	for _, in := range indexes {
		go searchChildIndex(in, req)
	}

	// on another go routine, close after finished
	go func() {
		waitGroup.Wait()
		close(asyncResults)
	}()

	var sr *RemoteIndexSearchResult
	indexErrors := make(map[string]error)

	for asr := range asyncResults {
		if asr.Err != nil {
			indexErrors[asr.Name] = asr.Err
			continue
		}
		if sr == nil {
			// first result
			sr = asr.Result
			continue
		}
		if asr.Result.BleveSearchResult != nil {
			if sr.BleveSearchResult == nil {
				sr.BleveSearchResult = asr.Result.BleveSearchResult
			} else {
				sr.BleveSearchResult.Merge(asr.Result.BleveSearchResult)
			}
		}
		if asr.Result.VectorSearchResult != nil {
			for idx := range asr.Result.VectorSearchResult {
				// Take the top K hits over asr and sr
				sr.VectorSearchResult[idx].Merge(asr.Result.VectorSearchResult[idx])
			}
		}
		if asr.Result.FusionResult != nil {
			if sr.FusionResult == nil {
				sr.FusionResult = asr.Result.FusionResult
			} else {
				// TODO (ajr) Implement fusion result merging
				sr.FusionResult.Merge(asr.Result.FusionResult)
			}
		}
		if asr.Result.AggregationResults != nil {
			if sr.AggregationResults == nil {
				sr.AggregationResults = asr.Result.AggregationResults
			} else {
				sr.AggregationResults.Merge(asr.Result.AggregationResults)
			}
		}

		sr.Status.Merge(asr.Result.Status)
	}

	// Sort fusion results once after all shards have been merged.
	if sr != nil && sr.FusionResult != nil {
		sr.FusionResult.FinalizeSort()
	}

	// handle case where no results were successful
	if sr == nil {
		sr = &RemoteIndexSearchResult{}
	}

	searchDuration := time.Since(searchStart)
	sr.Took = searchDuration

	// fix up errors
	if len(indexErrors) > 0 {
		if sr.Status == nil {
			sr.Status = &RemoteIndexSearchStatus{}
		}
		if sr.Status.Errors == nil {
			sr.Status.Errors = make(map[string]error)
		}
		for indexName, indexErr := range indexErrors {
			sr.Status.Errors[indexName] = indexErr
			sr.Status.Total++
			sr.Status.Failed++
		}
	}

	return sr, nil
}

type asyncSearchResult struct {
	Name   string
	Result *RemoteIndexSearchResult
	Err    error
}

// FusionHit holds the document ID, its fused score, and the original document match details.
type FusionHit struct {
	ID    string  `json:"id"`
	Score float64 `json:"score"`

	RerankedScore *float64 `json:"reranked_score,omitempty"`

	Fields      map[string]any     `json:"fields,omitempty"`
	IndexScores map[string]float64 `json:"index_scores,omitempty"` // Scores from each index, if needed
}
type FusionResult struct {
	Hits         []*FusionHit              `json:"hits"`
	Total        uint64                    `json:"total"`
	MaxScore     float64                   `json:"max_score"`
	Status       *RemoteIndexSearchStatus  `json:"status"`
	Took         time.Duration             `json:"took,omitempty"`
	Aggregations search.AggregationResults `json:"aggregations,omitempty"`
}

// IsEmpty returns true if no pruning options are configured.
func (rp *Pruner) IsEmpty() bool {
	if rp == nil {
		return true
	}
	return rp.MinScoreRatio == 0 &&
		rp.MaxScoreGapPercent == 0 &&
		rp.MinAbsoluteScore == 0 &&
		!rp.RequireMultiIndex &&
		rp.StdDevThreshold == 0
}

// PruneResults applies all configured pruning strategies to the fusion results.
// Pruning is applied in this order:
// 1. RequireMultiIndex - filter to multi-index hits only
// 2. MinAbsoluteScore - hard floor threshold
// 3. MinScoreRatio - relative to max score
// 4. StdDevThreshold - statistical outlier removal
// 5. MaxScoreGapPercent - elbow detection (stops at first large gap)
func (rp *Pruner) PruneResults(hits []*FusionHit) []*FusionHit {
	if rp == nil || len(hits) == 0 {
		return hits
	}

	result := hits

	// 1. Filter to multi-index hits if required
	if rp.RequireMultiIndex {
		result = pruneRequireMultiIndex(result)
	}

	if len(result) == 0 {
		return result
	}

	// 2. Apply minimum absolute score threshold
	if rp.MinAbsoluteScore > 0 {
		result = pruneByMinAbsoluteScore(result, rp.MinAbsoluteScore)
	}

	if len(result) == 0 {
		return result
	}

	// 3. Apply relative score threshold (ratio of max score)
	if rp.MinScoreRatio > 0 {
		result = pruneByScoreRatio(result, rp.MinScoreRatio)
	}

	if len(result) == 0 {
		return result
	}

	// 4. Apply standard deviation threshold
	if rp.StdDevThreshold > 0 {
		result = pruneByStdDev(result, rp.StdDevThreshold)
	}

	if len(result) == 0 {
		return result
	}

	// 5. Apply score gap detection (elbow detection)
	if rp.MaxScoreGapPercent > 0 {
		result = pruneByScoreGap(result, rp.MaxScoreGapPercent)
	}

	return result
}

// getEffectiveScore returns the score to use for pruning calculations,
// preferring RerankedScore if available.
func getEffectiveScore(hit *FusionHit) float64 {
	if hit.RerankedScore != nil {
		return *hit.RerankedScore
	}
	return hit.Score
}

// pruneRequireMultiIndex keeps only hits that appear in multiple indexes.
func pruneRequireMultiIndex(hits []*FusionHit) []*FusionHit {
	result := make([]*FusionHit, 0, len(hits))
	for _, hit := range hits {
		if len(hit.IndexScores) >= 2 {
			result = append(result, hit)
		}
	}
	return result
}

// pruneByMinAbsoluteScore removes hits below the absolute score threshold.
func pruneByMinAbsoluteScore(hits []*FusionHit, minScore float64) []*FusionHit {
	result := make([]*FusionHit, 0, len(hits))
	for _, hit := range hits {
		if getEffectiveScore(hit) >= minScore {
			result = append(result, hit)
		}
	}
	return result
}

// pruneByScoreRatio keeps hits with score >= maxScore * ratio.
func pruneByScoreRatio(hits []*FusionHit, ratio float64) []*FusionHit {
	if len(hits) == 0 || ratio <= 0 {
		return hits
	}

	maxScore := getEffectiveScore(hits[0])
	for _, hit := range hits[1:] {
		if s := getEffectiveScore(hit); s > maxScore {
			maxScore = s
		}
	}

	// For negative scores (e.g. cross-encoder logits), ratio-based pruning
	// uses absolute value: keep hits where |score| >= |maxScore| * ratio.
	// This avoids the case where maxScore * ratio produces a threshold
	// less negative than all scores, filtering everything out.
	var threshold float64
	if maxScore < 0 {
		threshold = maxScore / ratio
	} else {
		threshold = maxScore * ratio
	}
	result := make([]*FusionHit, 0, len(hits))
	for _, hit := range hits {
		if getEffectiveScore(hit) >= threshold {
			result = append(result, hit)
		}
	}
	return result
}

// pruneByStdDev keeps hits within N standard deviations below the mean.
func pruneByStdDev(hits []*FusionHit, stdDevMultiplier float64) []*FusionHit {
	if len(hits) < 3 {
		return hits
	}

	// Calculate mean
	var sum float64
	for _, hit := range hits {
		sum += getEffectiveScore(hit)
	}
	mean := sum / float64(len(hits))

	// Calculate standard deviation
	var sumSq float64
	for _, hit := range hits {
		diff := getEffectiveScore(hit) - mean
		sumSq += diff * diff
	}
	variance := sumSq / float64(len(hits))
	stdDev := math.Sqrt(variance)

	// Keep results above (mean - N*stdDev)
	threshold := mean - stdDevMultiplier*stdDev

	result := make([]*FusionHit, 0, len(hits))
	for _, hit := range hits {
		if getEffectiveScore(hit) >= threshold {
			result = append(result, hit)
		}
	}
	return result
}

// pruneByScoreGap stops returning results when a consecutive score gap
// exceeds maxDropPercent of the total score range (max - min). This
// normalizes gap detection to the actual score distribution, making it
// effective across different score scales (e.g. reranker scores in [0,1]
// vs fusion scores in [0,100]).
func pruneByScoreGap(hits []*FusionHit, maxDropPercent float64) []*FusionHit {
	if len(hits) <= 1 || maxDropPercent <= 0 {
		return hits
	}

	// Compute score range for normalization
	maxScore := getEffectiveScore(hits[0])
	minScore := maxScore
	for _, hit := range hits[1:] {
		s := getEffectiveScore(hit)
		if s > maxScore {
			maxScore = s
		}
		if s < minScore {
			minScore = s
		}
	}
	scoreRange := maxScore - minScore
	if scoreRange <= 0 {
		return hits // all scores identical
	}

	result := []*FusionHit{hits[0]}
	prevScore := getEffectiveScore(hits[0])

	for i := 1; i < len(hits); i++ {
		currentScore := getEffectiveScore(hits[i])

		// Gap as percentage of the total score range
		gapPercent := (prevScore - currentScore) / scoreRange * 100
		if gapPercent > maxDropPercent {
			break
		}

		result = append(result, hits[i])
		prevScore = currentScore
	}

	return result
}

// Merge combines two FusionResults, deduplicating hits by ID and aggregating scores.
// When the same document appears in both results (from different shards), their
// scores are summed and IndexScores are merged. Call FinalizeSort after all Merge
// calls to sort the combined hits and update MaxScore.
func (fsr *FusionResult) Merge(other *FusionResult) {
	if fsr == nil || other == nil {
		return
	}

	// Build index of existing hits by ID for deduplication
	hitIndex := make(map[string]int, len(fsr.Hits))
	for i, hit := range fsr.Hits {
		hitIndex[hit.ID] = i
	}

	// Merge hits, deduplicating by ID
	for _, otherHit := range other.Hits {
		if existingIdx, exists := hitIndex[otherHit.ID]; exists {
			// Duplicate found - merge the hits
			existing := fsr.Hits[existingIdx]

			// Sum scores (appropriate for RRF and most fusion methods)
			existing.Score += otherHit.Score

			// Merge reranked scores if both have them
			if existing.RerankedScore != nil && otherHit.RerankedScore != nil {
				merged := *existing.RerankedScore + *otherHit.RerankedScore
				existing.RerankedScore = &merged
			} else if otherHit.RerankedScore != nil {
				existing.RerankedScore = otherHit.RerankedScore
			}

			// Merge IndexScores
			if otherHit.IndexScores != nil {
				if existing.IndexScores == nil {
					existing.IndexScores = make(map[string]float64)
				}
				for idx, score := range otherHit.IndexScores {
					existing.IndexScores[idx] += score
				}
			}

			// Merge Fields (prefer existing, add missing)
			if otherHit.Fields != nil {
				if existing.Fields == nil {
					existing.Fields = make(map[string]any)
				}
				for k, v := range otherHit.Fields {
					if _, exists := existing.Fields[k]; !exists {
						existing.Fields[k] = v
					}
				}
			}
		} else {
			// New hit - append
			hitIndex[otherHit.ID] = len(fsr.Hits)
			fsr.Hits = append(fsr.Hits, otherHit)
		}
	}

	// Total is the count of unique documents
	fsr.Total = uint64(len(fsr.Hits))

	// Merge status
	if fsr.Status == nil {
		fsr.Status = other.Status
	} else if other.Status != nil {
		fsr.Status.Merge(other.Status)
	}

	// Take max of Took
	fsr.Took = max(fsr.Took, other.Took)
}

// FinalizeSort sorts the merged hits by score descending and updates MaxScore.
// Call this once after all Merge calls are complete.
func (fsr *FusionResult) FinalizeSort() {
	if fsr == nil || len(fsr.Hits) == 0 {
		return
	}

	slices.SortStableFunc(fsr.Hits, func(a, b *FusionHit) int {
		return -cmp.Compare(getEffectiveScore(a), getEffectiveScore(b))
	})

	fsr.MaxScore = getEffectiveScore(fsr.Hits[0])
}

type Query struct {
	Table  string   `json:"table,omitzero"`
	Count  bool     `json:"count,omitempty,omitzero"`
	Fields []string `json:"fields,omitempty,omitzero"`
	// TODO (ajr) Support FilterPrefix for the full text index
	FilterPrefix   []byte      `json:"filter_prefix,omitempty"`   // Prefix for key/docID filtering
	FilterQuery    query.Query `json:"filter_query,omitempty"`    // Query for filtering
	ExclusionQuery query.Query `json:"exclusion_query,omitempty"` // Query for exclusion

	// Raw JSON of FilterQuery, preserved from the API request to avoid
	// re-serialization when passing to shards.
	FilterQueryRaw json.RawMessage `json:"-"`

	// Full text search
	OrderBy             []SortField         `json:"order_by,omitempty"`
	Offset              int                 `json:"offset,omitempty,omitzero"`
	SearchAfter         []string            `json:"search_after,omitempty"`
	SearchBefore        []string            `json:"search_before,omitempty"`
	FullTextSearch      query.Query         `json:"full_text_search,omitempty"`
	AggregationRequests AggregationRequests `json:"aggregations,omitempty"`
	HybridFullTextMode  HybridFullTextMode  `json:"-"`

	// For topk or for Full text paging
	Limit int `json:"limit,omitempty"`

	// Mapping from index name to embedding
	Embeddings    map[string]vector.T `json:"embeddings,omitempty,omitzero"`
	DistanceUnder *float32            `json:"distance_under,omitempty"`
	DistanceOver  *float32            `json:"distance_over,omitempty"`
	SearchEffort  *float32            `json:"search_effort,omitempty"`

	// Graph searches (executed after full-text/vector searches)
	GraphSearches map[string]*GraphQuery `json:"graph_searches,omitempty"`

	Reranker         *reranking.RerankerConfig `json:"reranker,omitempty,omitzero"`
	RerankerTemplate string                    `json:"reranker_template,omitempty,omitzero"`
	RerankerField    string                    `json:"reranker_field,omitempty,omitzero"`
	RerankerQuery    string                    `json:"reranker_query,omitempty,omitzero"`

	MergeConfig    *MergeConfig `json:"merge_config,omitempty"`
	ExpandStrategy string       `json:"expand_strategy,omitempty"` // Graph fusion: "union", "intersection", ""

	// SparseEmbeddings maps sparse embeddings index names to pre-computed sparse vectors.
	// Computed once at the metadata level and broadcast to all shards.
	SparseEmbeddings map[string]SparseVec `json:"sparse_embeddings,omitempty,omitzero"`

	// Pruner configures score-based filtering to remove low-relevance results
	Pruner *Pruner `json:"pruner,omitempty"`
}

type HybridFullTextMode string

const (
	HybridFullTextModeNone      HybridFullTextMode = ""
	HybridFullTextModeMatchAll  HybridFullTextMode = "match_all"
	HybridFullTextModeMatchNone HybridFullTextMode = "match_none"
)

// PrepareHybridFullTextForSemanticSearch removes broad lexical branches from
// hybrid execution while preserving an optional full-text fallback query.
//
// Explicit match_all behaves like a lexical no-op when semantic results exist,
// but we keep the original query around so callers can fall back to full-text
// if semantic retrieval returns nothing globally.
//
// Explicit match_none should never contribute lexical hits in hybrid mode.
func (q *Query) PrepareHybridFullTextForSemanticSearch() query.Query {
	if q == nil {
		return nil
	}

	originalFullText := q.FullTextSearch
	if q.FullTextSearch != nil {
		q.FullTextSearch, q.HybridFullTextMode = normalizeHybridFullTextQueryWithMode(q.FullTextSearch)
	} else {
		q.HybridFullTextMode = HybridFullTextModeNone
	}

	if len(q.Embeddings) == 0 && len(q.SparseEmbeddings) == 0 {
		return nil
	}

	switch q.HybridFullTextMode {
	case HybridFullTextModeMatchAll:
		q.FullTextSearch = nil
		return originalFullText
	case HybridFullTextModeMatchNone:
		q.FullTextSearch = nil
	}

	return nil
}

func ShouldUseHybridFullTextFallback(
	mode HybridFullTextMode,
	fallback query.Query,
	hadSemanticHits bool,
) bool {
	return mode == HybridFullTextModeMatchAll &&
		fallback != nil &&
		!hadSemanticHits
}

func ApplyPrunerToFusionResult(fusionResult *FusionResult, pruner *Pruner) {
	if fusionResult == nil || pruner == nil || pruner.IsEmpty() {
		return
	}

	originalCount := len(fusionResult.Hits)
	fusionResult.Hits = pruner.PruneResults(fusionResult.Hits)

	if len(fusionResult.Hits) == 0 {
		fusionResult.MaxScore = 0
		return
	}

	if len(fusionResult.Hits) < originalCount {
		fusionResult.MaxScore = getEffectiveScore(fusionResult.Hits[0])
		for _, hit := range fusionResult.Hits[1:] {
			if s := getEffectiveScore(hit); s > fusionResult.MaxScore {
				fusionResult.MaxScore = s
			}
		}
	}
}

func (q *Query) RemoteIndexSearchRequest() (*RemoteIndexSearchRequest, error) {
	if len(q.Embeddings) == 0 && q.FullTextSearch == nil && len(q.GraphSearches) == 0 && len(q.SparseEmbeddings) == 0 {
		return nil, errors.New("search requires at least one of: full_text_search, embeddings, sparse_embeddings, or graph_searches")
	}

	if q.Limit <= 0 {
		return nil, errors.New("sematntic search requires topk limit to be positive")
	}
	if len(q.Embeddings) > 1 || (len(q.Embeddings) == 1 && q.FullTextSearch != nil) {
		if q.DistanceOver != nil || q.DistanceUnder != nil {
			return nil, errors.New(
				"distance_over and distance_under are not supported with rrf search against multiple indexes",
			)
		}
	}
	if len(q.Embeddings) > 0 {
		if q.Offset != 0 {
			return nil, errors.New("offset is not supported with vector searches")
		}
	}
	// Build the combined filter query for the wire format.
	// When possible, use preserved raw JSON bytes to avoid re-serialization.
	var filterQuery query.Query
	var fqBytes json.RawMessage
	if q.FilterQuery != nil && q.ExclusionQuery != nil {
		// Must compose a new boolean query — need to marshal
		filterQuery = query.NewBooleanQuery([]query.Query{
			q.FilterQuery,
		}, nil /* should */, []query.Query{
			q.ExclusionQuery,
		})
		var err error
		fqBytes, err = json.Marshal(filterQuery)
		if err != nil {
			return nil, fmt.Errorf("marshalling filter query: %w", err)
		}
	} else if q.FilterQuery != nil {
		filterQuery = q.FilterQuery
		if len(q.FilterQueryRaw) > 0 {
			fqBytes = q.FilterQueryRaw // pass through original bytes
		} else {
			var err error
			fqBytes, err = json.Marshal(filterQuery)
			if err != nil {
				return nil, fmt.Errorf("marshalling filter query: %w", err)
			}
		}
	} else if q.ExclusionQuery != nil {
		// Must wrap in boolean query — always needs marshaling
		filterQuery = query.NewBooleanQuery(nil /* must */, nil /* should */, []query.Query{q.ExclusionQuery})
		var err error
		fqBytes, err = json.Marshal(filterQuery)
		if err != nil {
			return nil, fmt.Errorf("marshalling exclusion query: %w", err)
		}
	}
	risr := &RemoteIndexSearchRequest{
		Columns:             q.Fields,
		Star:                len(q.Fields) == 0,
		BlevePagingOpts:     q.FullTextPagingOptions(),
		VectorPagingOpts:    q.VectorPagingOptions(),
		Limit:               q.Limit,
		FilterPrefix:        q.FilterPrefix,
		FilterQuery:         fqBytes,
		VectorSearches:      q.Embeddings,
		SparseSearches:      q.SparseEmbeddings,
		GraphSearches:       q.GraphSearches,
		AggregationRequests: q.AggregationRequests,
		RerankerConfig:      q.Reranker,
		RerankerQuery:       q.RerankerQuery,
		MergeConfig:         q.MergeConfig,
		ExpandStrategy:      q.ExpandStrategy,
	}
	// TODO (ajr) Add docID to the schema to support full_text prefix search on ids?
	if q.FullTextSearch != nil {
		fullTextSearch := q.FullTextSearch
		if filterQuery != nil {
			fullTextSearch = query.NewConjunctionQuery([]query.Query{q.FullTextSearch, filterQuery})
		}
		risr.BleveSearchRequest = bleve.NewSearchRequest(fullTextSearch)
		applyFullTextPaging(risr.BleveSearchRequest, q.FullTextPagingOptions())
	}
	return risr, nil
}

func (q *Query) FullTextPagingOptions() FullTextPagingOptions {
	if q == nil {
		return FullTextPagingOptions{}
	}
	return FullTextPagingOptions{
		OrderBy:      q.OrderBy,
		Limit:        q.Limit,
		Offset:       q.Offset,
		SearchAfter:  q.SearchAfter,
		SearchBefore: q.SearchBefore,
	}
}

func (q *Query) VectorPagingOptions() VectorPagingOptions {
	if q == nil {
		return VectorPagingOptions{}
	}
	return VectorPagingOptions{
		OrderBy:       q.OrderBy,
		Limit:         q.Limit,
		DistanceUnder: q.DistanceUnder,
		DistanceOver:  q.DistanceOver,
		SearchEffort:  q.SearchEffort,
	}
}

// FusedSearch combines results from Bleve and Vector searches using fusion strategies.
// Supports both RRF (Reciprocal Rank Fusion) and RSF (Relative Score Fusion).
// The fusion strategy and parameters are determined by the MergeConfig in the query.
func (r RemoteIndexes) FusedSearch(ctx context.Context, jsonQuery *Query) (*FusionResult, error) {
	searchStart := time.Now()
	risr, err := jsonQuery.RemoteIndexSearchRequest()
	if err != nil {
		return nil, fmt.Errorf("creating remote index search request: %w", err)
	}

	var remoteRes *RemoteIndexSearchResult
	var remoteErr error
	var wg sync.WaitGroup
	wg.Go(func() {
		remoteRes, remoteErr = MultiSearch(ctx, risr, r...)
	})

	wg.Wait()

	var fusedResults *FusionResult

	mc := risr.MergeConfig
	// Choose fusion strategy based on merge config
	if mc != nil && mc.Strategy != nil && *mc.Strategy == MergeStrategyRsf {
		// Use Relative Score Fusion
		windowSize := mc.WindowSize
		if windowSize <= 0 {
			windowSize = jsonQuery.Limit
		}

		var weights map[string]float64
		if mc.Weights != nil {
			weights = *mc.Weights
		}

		fusedResults = remoteRes.RSFResults(jsonQuery.Limit, windowSize, weights)
	} else {
		// Use Reciprocal Rank Fusion (default)
		rankConstant := 60.0
		if mc != nil && mc.RankConstant != 0 {
			rankConstant = mc.RankConstant
		}
		var weights map[string]float64
		if mc != nil && mc.Weights != nil {
			weights = *mc.Weights
		}
		fusedResults = remoteRes.RRFResults(jsonQuery.Limit, rankConstant, weights)
	}

	fusedResults.Status = remoteRes.Status
	fusedResults.Aggregations = remoteRes.AggregationResults
	fusedResults.Took = time.Since(searchStart)
	return fusedResults, remoteErr
}
