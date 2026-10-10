// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"strings"

	antfly "github.com/antflydb/antfly/go/pkg/sdk"
	"github.com/antflydb/antfly/go/pkg/sdk/query"
)

func (s *SearchServer) searchRequest(queryText string) antfly.QueryRequest {
	indexes := s.indexes
	if len(indexes) == 0 {
		indexes = searchIndexNames()
	}
	request := antfly.QueryRequest{Table: s.tableName, SemanticSearch: queryText, Indexes: indexes, Limit: 20}
	if s.corpus {
		// Return the source row with matching descendants, retaining native page/time provenance.
		match := query.NewMatch(queryText, "text")
		request.FullTextSearch = &match
		request.FullTextIndex = corpusTextIndex
		request.SemanticSearch = ""
		request.Indexes = nil
		for _, index := range indexes {
			if index != corpusTextIndex && index != DefaultFullTextIndex {
				request.SemanticSearch = queryText
				request.Indexes = append(request.Indexes, index)
			}
		}
		request.Fields = []string{"title", "mime_type", "original_url", "metadata"}
		request.Hierarchy = &antfly.QueryHierarchy{
			GroupBy:   &antfly.HierarchyGroupBy{Level: "source", Matches: &antfly.HierarchyMatches{Limit: 3, Fields: []string{"text", "_start_time_ms"}}},
			Ancestors: &antfly.HierarchyAncestors{Unit: &antfly.HierarchyProjection{Fields: []string{"provenance.page_number"}}},
		}

	}
	return request
}
func applyCorpusHit(result *SearchResult, hit antfly.QueryHit) {
	mime, _ := hit.Source["mime_type"].(string)
	result.Audio = strings.HasPrefix(mime, "audio/")
	if original, ok := hit.Source["original_url"].(string); ok {
		result.URL = original
	}
	matches := hit.Hierarchy.Matches
	if len(matches) == 0 {
		matches = hit.Hierarchy.Chunks
	}
	if len(matches) == 0 {
		return
	}
	match := matches[0]
	if text, ok := match.Source["text"].(string); ok {
		result.Content = truncateContent(text, 2000)
	}
	unit := match.Hierarchy.Ancestors.Unit.Document
	if provenance, ok := unit["provenance"].(map[string]any); ok {
		unit = provenance
	}
	page, _ := unit["page_number"].(float64)
	if page == 0 {
		page, _ = match.Source["page_number"].(float64)
	}
	if page > 0 {
		metadata, _ := hit.Source["metadata"].(map[string]any)
		result.URL, result.PageNum = corpusPageCitation(result.URL, metadata, page)
	}
	// Native transcription assigns each chunk the first overlapping phrase's
	// timestamp. The unit's first span may be minutes before this search match.
	if result.Audio {
		if start, ok := match.Source["_start_time_ms"].(float64); ok && start >= 0 {
			result.URL = strings.Split(result.URL, "#")[0] + fmt.Sprintf("#t=%.3f", start/1000)
		}
	}
}

func corpusPageCitation(original string, metadata map[string]any, page float64) (string, int) {
	if page <= 0 {
		return original, 0
	}
	start := float64(1)
	if value, ok := metadata["page_start"].(float64); ok && value > 0 {
		start = value
	}
	number := int(start + page - 1)
	return strings.Split(original, "#")[0] + fmt.Sprintf("#page=%d", number), number
}
