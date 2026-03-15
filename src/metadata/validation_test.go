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
	"testing"

	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/stretchr/testify/assert"
)

func TestValidateMergeConfig(t *testing.T) {
	tests := []struct {
		name    string
		query   QueryRequest
		wantErr string
	}{
		{
			name:  "empty merge_config is valid",
			query: QueryRequest{},
		},
		{
			name: "valid merge_config with all fields",
			query: QueryRequest{
				MergeConfig: indexes.MergeConfig{
					Strategy:     new(indexes.MergeStrategyRrf),
					RankConstant: 30.0,
					WindowSize:   100,
					Weights:      &map[string]float64{"full_text": 0.3, "title_embedding": 1.0},
				},
			},
		},
		{
			name: "negative rank_constant",
			query: QueryRequest{
				MergeConfig: indexes.MergeConfig{
					RankConstant: -1.0,
				},
			},
			wantErr: "rank_constant must be non-negative",
		},
		{
			name: "negative window_size",
			query: QueryRequest{
				MergeConfig: indexes.MergeConfig{
					WindowSize: -5,
				},
			},
			wantErr: "window_size must be positive",
		},
		{
			name: "negative weight value",
			query: QueryRequest{
				MergeConfig: indexes.MergeConfig{
					Weights: &map[string]float64{"full_text": -0.5},
				},
			},
			wantErr: `weight for "full_text" must be non-negative`,
		},
		{
			name: "zero weight is valid",
			query: QueryRequest{
				MergeConfig: indexes.MergeConfig{
					Weights: &map[string]float64{"full_text": 0.0, "embedding": 1.0},
				},
			},
		},
		{
			name: "zero rank_constant is valid",
			query: QueryRequest{
				MergeConfig: indexes.MergeConfig{
					RankConstant: 0,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.query.validateMergeConfig()
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateSearchAfterSearchBefore(t *testing.T) {
	orderBy := []SortField{{Field: "created_at", Desc: new(true)}}

	tests := []struct {
		name    string
		query   QueryRequest
		wantErr string
	}{
		{
			name: "search_after with order_by is valid",
			query: QueryRequest{
				OrderBy:     orderBy,
				SearchAfter: []string{"value1"},
			},
		},
		{
			name: "search_before with order_by is valid",
			query: QueryRequest{
				OrderBy:      orderBy,
				SearchBefore: []string{"value1"},
			},
		},
		{
			name: "search_after and search_before are mutually exclusive",
			query: QueryRequest{
				OrderBy:      orderBy,
				SearchAfter:  []string{"a"},
				SearchBefore: []string{"b"},
			},
			wantErr: "search_after and search_before are mutually exclusive",
		},
		{
			name: "search_after and offset are mutually exclusive",
			query: QueryRequest{
				OrderBy:     orderBy,
				SearchAfter: []string{"a"},
				Offset:      10,
			},
			wantErr: "search_after/search_before and offset are mutually exclusive",
		},
		{
			name: "search_after requires order_by",
			query: QueryRequest{
				SearchAfter: []string{"a"},
			},
			wantErr: "order_by is required when using search_after or search_before",
		},
		{
			name: "search_after not supported with semantic_search",
			query: QueryRequest{
				OrderBy:        orderBy,
				SearchAfter:    []string{"a"},
				SemanticSearch: "find me something",
				Indexes:        []string{"idx"},
			},
			wantErr: "search_after/search_before is not supported with semantic_search",
		},
		{
			name: "search_after length must match order_by length",
			query: QueryRequest{
				OrderBy:     orderBy,
				SearchAfter: []string{"a", "b"},
			},
			wantErr: "search_after must have the same number of values as order_by fields (got 2, expected 1)",
		},
		{
			name: "search_before length must match order_by length",
			query: QueryRequest{
				OrderBy:      orderBy,
				SearchBefore: []string{"a", "b"},
			},
			wantErr: "search_before must have the same number of values as order_by fields (got 2, expected 1)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.query.Validate()
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
