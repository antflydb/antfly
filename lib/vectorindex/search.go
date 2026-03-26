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

package vectorindex

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"time"

	"github.com/antflydb/antfly/lib/vector"
)

type RerankPolicy string

const (
	RerankPolicyNever    RerankPolicy = "never"
	RerankPolicyBoundary RerankPolicy = "boundary"
	RerankPolicyAlways   RerankPolicy = "always"
)

type SearchRequest struct {
	Embedding vector.T
	K         int

	DistanceOver  *float32 `json:"distance_over,omitempty"`
	DistanceUnder *float32 `json:"distance_under,omitempty"`

	// SearchEffort controls the recall vs latency tradeoff.
	// Range: 0.0 (fastest) to 1.0 (highest recall). nil uses the balanced
	// default effort (0.5) unless SearchWidth or Epsilon2 is explicitly set.
	SearchEffort *float32 `json:"search_effort,omitempty"`

	// SearchWidth, if set, overrides the index's configured SearchWidth
	// (number of leaf clusters to explore).
	SearchWidth *int `json:"search_width,omitempty"`

	// Epsilon2, if set, overrides the index's configured Episilon2 for
	// dynamic pruning.
	Epsilon2 *float32 `json:"epsilon2,omitempty"`

	// RerankPolicy controls whether exact distances are computed for the final
	// candidates after approximate search.
	// nil uses the index default.
	RerankPolicy *RerankPolicy `json:"rerank_policy,omitempty"`

	// Debug, when non-nil, is populated with search diagnostics.
	Debug *SearchDebugInfo `json:"debug,omitempty"`

	FilterPrefix []byte   `json:"filter_prefix,omitempty"`
	ExcludeIDs   []uint64 `json:"exclude_ids,omitempty"`
	FilterIDs    []uint64 `json:"filter_ids,omitempty"`
}

type SearchStatus struct {
	Total      uint64           `json:"total"`
	Failed     int              `json:"failed"`
	Successful int              `json:"successful"`
	Errors     map[string]error `json:"errors,omitempty"`
}

func (ss *SearchStatus) Merge(other *SearchStatus) {
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

type SearchResult struct {
	Took    time.Duration    `json:"took,omitempty"`
	Hits    []*SearchHit     `json:"hits,omitempty"`
	Status  *SearchStatus    `json:"status,omitempty"`
	Request *SearchRequest   `json:"request,omitempty"`
	Total   uint64           `json:"total,omitempty"`
	Debug   *SearchDebugInfo `json:"debug,omitempty"`
}

type SearchHit struct {
	NodeID     uint64         `json:"node_id,omitempty"`
	Index      string         `json:"index,omitempty"`
	ID         string         `json:"id,omitempty"`
	Distance   float32        `json:"distance,omitempty"`
	ErrorBound float32        `json:"error_bound,omitempty"`
	Score      float32        `json:"score,omitempty"`
	Fields     map[string]any `json:"fields,omitempty"`
}

type SearchDebugHit struct {
	ID         uint64  `json:"id,omitempty"`
	Distance   float32 `json:"distance,omitempty"`
	ErrorBound float32 `json:"error_bound,omitempty"`
	LowerBound float32 `json:"lower_bound,omitempty"`
	UpperBound float32 `json:"upper_bound,omitempty"`
}

type SearchDebugPair struct {
	Left        SearchDebugHit `json:"left"`
	Right       SearchDebugHit `json:"right"`
	DistanceGap float32        `json:"distance_gap,omitempty"`
	IntervalGap float32        `json:"interval_gap,omitempty"`
	Overlaps    bool           `json:"overlaps,omitempty"`
}

type SearchDebugInfo struct {
	ResolvedSearchWidth        int              `json:"resolved_search_width,omitempty"`
	ResolvedEpsilon2           float32          `json:"resolved_epsilon2,omitempty"`
	NodesExplored              int              `json:"nodes_explored,omitempty"`
	LeavesExplored             int              `json:"leaves_explored,omitempty"`
	StoppedBySearchWidth       bool             `json:"stopped_by_search_width,omitempty"`
	ResolvedRerankPolicy       RerankPolicy     `json:"resolved_rerank_policy,omitempty"`
	ExactRerank                bool             `json:"exact_rerank,omitempty"`
	RerankCandidateCount       int              `json:"rerank_candidate_count,omitempty"`
	ApproxCandidateCount       int              `json:"approx_candidate_count,omitempty"`
	TopKCount                  int              `json:"top_k_count,omitempty"`
	AmbiguousTopKPairs         int              `json:"ambiguous_top_k_pairs,omitempty"`
	AmbiguousBoundaryPairs     int              `json:"ambiguous_boundary_pairs,omitempty"`
	AmbiguousDistanceOverHits  int              `json:"ambiguous_distance_over_hits,omitempty"`
	AmbiguousDistanceUnderHits int              `json:"ambiguous_distance_under_hits,omitempty"`
	MinDistanceGapTopK         float32          `json:"min_distance_gap_top_k,omitempty"`
	MinIntervalGapTopK         float32          `json:"min_interval_gap_top_k,omitempty"`
	ClosestPairTopK            *SearchDebugPair `json:"closest_pair_top_k,omitempty"`
	BoundaryPair               *SearchDebugPair `json:"boundary_pair,omitempty"`
	ApproxTop                  []SearchDebugHit `json:"approx_top,omitempty"`
	ExactTop                   []SearchDebugHit `json:"exact_top,omitempty"`
}

func (sr *SearchResult) Merge(other *SearchResult) {
	// Take the top K hits over asr and sr
	sr.Hits = append(sr.Hits, other.Hits...)
	slices.SortFunc(sr.Hits, func(a, b *SearchHit) int {
		return cmp.Compare(a.Distance, b.Distance)
	})
	sr.Total += other.Total
	if len(sr.Hits) > 0 {
		sr.Hits = sr.Hits[:min(sr.Request.K, len(sr.Hits))]
	}
	sr.Status.Merge(other.Status)
}

// Similar to the bleve interface
func SearchInContext(
	ctx context.Context,
	idx VectorIndex,
	searchRequest *SearchRequest,
) (*SearchResult, error) {
	startTime := time.Now()
	results, err := idx.Search(searchRequest)
	hits := make([]*SearchHit, len(results))
	for i, result := range results {
		hits[i] = &SearchHit{
			Index:      idx.Name(),
			ID:         string(result.Metadata),
			Distance:   result.Distance,
			ErrorBound: result.ErrorBound,
			NodeID:     result.ID,
		}
	}
	return &SearchResult{
		Hits:  hits,
		Total: idx.TotalVectors(),
		Status: &SearchStatus{
			Total:      idx.TotalVectors(),
			Failed:     0,
			Successful: len(results),
		},
		Request: searchRequest,
		Took:    time.Since(startTime),
		Debug:   searchRequest.Debug,
	}, err
}
