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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// Verify every native unit, including units without searchable chunks. Work is
// bounded by the manifest count and 100-row pages, never a corpus-sized list.
func qualifyMediaUnits(ctx context.Context, client *http.Client, tableURL, id, source string, expected int, grounded bool) (int, error) {
	const pageSize = 100
	units, evidence := 0, 0
	var cursor []string
	snapshot := ""
	for {
		body := map[string]any{"fields": []string{"text", "provenance"}, "hierarchy": map[string]any{"children": map[string]any{"level": "unit", "parent": map[string]any{"level": "source", "id": id}}}, "order_by": []any{map[string]any{"field": "_hierarchy.position", "desc": false}}, "limit": pageSize}
		if cursor != nil {
			body["search_after"] = cursor
		}
		if snapshot != "" {
			body["remote_snapshot"] = snapshot
		}
		raw, err := corpusRequestWithContext(ctx, client, "POST", tableURL+"/query", body)
		if err != nil {
			return 0, err
		}
		var response struct {
			Responses []struct {
				Status   int             `json:"status"`
				Error    json.RawMessage `json:"error"`
				Snapshot string          `json:"remote_snapshot"`
				Hits     *struct {
					Hits []struct {
						ID     string   `json:"_id"`
						Sort   []string `json:"_sort"`
						Source struct {
							Text       string `json:"text"`
							Provenance struct {
								Source          string `json:"source_url"`
								Used            bool   `json:"transcript_used"`
								OCRUsed         bool   `json:"ocr_used"`
								TranscriptSpans []struct {
									Start     *uint64 `json:"start_ms"`
									End       *uint64 `json:"end_ms"`
									CharStart *uint64 `json:"char_start"`
									CharEnd   *uint64 `json:"char_end"`
								} `json:"transcript_spans"`
							} `json:"provenance"`
						} `json:"_source"`
					} `json:"hits"`
				} `json:"hits"`
			} `json:"responses"`
		}
		if err := json.Unmarshal(raw, &response); err != nil {
			return 0, err
		}
		if len(response.Responses) != 1 {
			return 0, fmt.Errorf("%s: invalid native unit traversal response", id)
		}
		page := response.Responses[0]
		if page.Status != 200 || page.Hits == nil || (len(page.Error) > 0 && string(page.Error) != "null") {
			return 0, fmt.Errorf("%s: native unit traversal is incomplete", id)
		}
		if snapshot != "" && page.Snapshot != snapshot {
			return 0, fmt.Errorf("%s: native unit snapshot changed", id)
		}
		snapshot = page.Snapshot
		hits := page.Hits.Hits
		for _, hit := range hits {
			unit := hit.Source
			if units >= expected || hit.ID == "" || strings.TrimSpace(unit.Text) == "" || unit.Provenance.Source != source {
				return 0, fmt.Errorf("%s: missing native unit text/source evidence", id)
			}
			if grounded {
				blocks, err := validateGroundedImage(unit.Text)
				if err != nil || !unit.Provenance.OCRUsed {
					return 0, fmt.Errorf("%s: missing grounded image evidence: %v", id, err)
				}
				evidence += blocks
				units++
				continue
			}
			if !unit.Provenance.Used || len(unit.Provenance.TranscriptSpans) == 0 {
				return 0, fmt.Errorf("%s: missing transcript timing", id)
			}
			var previous uint64
			for _, span := range unit.Provenance.TranscriptSpans {
				if span.Start == nil || span.End == nil || span.CharStart == nil || span.CharEnd == nil || *span.End <= *span.Start || *span.Start < previous || *span.CharEnd <= *span.CharStart || *span.CharEnd > uint64(len(unit.Text)) {
					return 0, fmt.Errorf("%s: invalid transcript timestamp/span evidence", id)
				}
				previous = *span.Start
				evidence++
			}
			units++
		}
		if len(hits) < pageSize {
			break
		}
		next := hits[len(hits)-1].Sort
		if len(next) != 2 || (len(cursor) == 2 && cursor[0] == next[0] && cursor[1] == next[1]) {
			return 0, fmt.Errorf("%s: native unit traversal has no stable continuation", id)
		}
		cursor = next
	}
	if units != expected {
		return 0, fmt.Errorf("%s: native unit inventory differs from manifest", id)
	}
	return evidence, nil
}
