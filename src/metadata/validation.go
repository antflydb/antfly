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
	"errors"
	"fmt"
	"slices"
)

// Validate checks if the QueryRequest has valid configuration.
// This should be called early in request handling to fail fast before
// processing begins.
func (q *QueryRequest) Validate() error {
	// Validate reranker configuration
	if q.Reranker != nil {
		if err := q.Reranker.Validate(); err != nil {
			return fmt.Errorf("invalid reranker configuration: %w", err)
		}
		if q.SemanticSearch == "" {
			return errors.New("semantic_search is required when using a reranker")
		}
	}

	// Validate semantic_search and indexes mutual requirement
	if len(q.SemanticSearch) > 0 && len(q.Indexes) == 0 {
		return errors.New("indexes requires at least one to be specified for semantic_search")
	}
	if len(q.SemanticSearch) == 0 && len(q.Indexes) > 0 {
		return errors.New("semantic_search is required when indexes are specified")
	}

	// Validate analyses requires _embeddings field
	if q.Analyses != nil && len(q.Fields) > 0 {
		if (len(q.Fields) == 1 && q.Fields[0] != "*" && q.Fields[0] != "_embeddings") ||
			!slices.Contains(q.Fields, "_embeddings") {
			return errors.New("analyses require _embeddings field to be included in fields list")
		}
	}

	// Validate search_after / search_before cursor pagination
	hasSearchAfter := len(q.SearchAfter) > 0
	hasSearchBefore := len(q.SearchBefore) > 0
	hasCursor := hasSearchAfter || hasSearchBefore
	if hasSearchAfter && hasSearchBefore {
		return errors.New("search_after and search_before are mutually exclusive")
	}
	if hasCursor && q.Offset != 0 {
		return errors.New("search_after/search_before and offset are mutually exclusive")
	}
	if hasCursor && len(q.OrderBy) == 0 {
		return errors.New("order_by is required when using search_after or search_before")
	}
	if hasCursor && len(q.SemanticSearch) > 0 {
		return errors.New("search_after/search_before is not supported with semantic_search")
	}
	if hasSearchAfter && len(q.SearchAfter) != len(q.OrderBy) {
		return fmt.Errorf("search_after must have the same number of values as order_by fields (got %d, expected %d)", len(q.SearchAfter), len(q.OrderBy))
	}
	if hasSearchBefore && len(q.SearchBefore) != len(q.OrderBy) {
		return fmt.Errorf("search_before must have the same number of values as order_by fields (got %d, expected %d)", len(q.SearchBefore), len(q.OrderBy))
	}

	// Validate merge_config
	if err := q.validateMergeConfig(); err != nil {
		return fmt.Errorf("invalid merge_config: %w", err)
	}

	return nil
}

// validateMergeConfig checks that merge_config values are valid.
func (q *QueryRequest) validateMergeConfig() error {
	mc := q.MergeConfig
	if mc.RankConstant < 0 {
		return errors.New("rank_constant must be non-negative")
	}
	if mc.WindowSize < 0 {
		return errors.New("window_size must be positive when specified")
	}
	if mc.Weights != nil {
		for key, w := range *mc.Weights {
			if w < 0 {
				return fmt.Errorf("weight for %q must be non-negative", key)
			}
		}
	}
	return nil
}
