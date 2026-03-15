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

type SearchRequest struct {
	Embedding vector.T
	K         int

	DistanceOver  *float32 `json:"distance_over,omitempty"`
	DistanceUnder *float32 `json:"distance_under,omitempty"`

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
	Took    time.Duration  `json:"took,omitempty"`
	Hits    []*SearchHit   `json:"hits,omitempty"`
	Status  *SearchStatus  `json:"status,omitempty"`
	Request *SearchRequest `json:"request,omitempty"`
	Total   uint64         `json:"total,omitempty"`
}

type SearchHit struct {
	NodeID   uint64         `json:"node_id,omitempty"`
	Index    string         `json:"index,omitempty"`
	ID       string         `json:"id,omitempty"`
	Distance float32        `json:"distance,omitempty"`
	Score    float32        `json:"score,omitempty"`
	Fields   map[string]any `json:"fields,omitempty"`
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
			Index:    idx.Name(),
			ID:       string(result.Metadata),
			Distance: result.Distance,
			NodeID:   result.ID,
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
	}, err
}
