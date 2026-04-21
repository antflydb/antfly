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

package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
)

type responseKind string

const (
	responseKindUnknown           responseKind = ""
	responseKindSearch            responseKind = "query.search"
	responseKindGraphNeighbors    responseKind = "graph.neighbors"
	responseKindGraphTraverse     responseKind = "graph.traverse"
	responseKindGraphShortestPath responseKind = "graph.shortest_path"
)

type publicSearchResponse struct {
	Kind                responseKind      `json:"kind"`
	Backend             string            `json:"backend"`
	Tenant              string            `json:"tenant"`
	Table               string            `json:"table"`
	View                string            `json:"view"`
	Version             *uint64           `json:"version,omitempty"`
	FreshnessLagRecords *uint64           `json:"freshness_lag_records,omitempty"`
	Total               uint64            `json:"total"`
	Hits                []publicSearchHit `json:"hits"`
}

type publicSearchHit struct {
	ID          string             `json:"id"`
	Score       float64            `json:"score"`
	Distance    *float64           `json:"distance,omitempty"`
	Document    any                `json:"document,omitempty"`
	Fields      map[string]any     `json:"fields,omitempty"`
	Sources     []string           `json:"sources,omitempty"`
	IndexScores map[string]float64 `json:"index_scores,omitempty"`
}

type publicGraphResponse struct {
	Kind                responseKind `json:"kind"`
	Backend             string       `json:"backend"`
	Tenant              string       `json:"tenant"`
	Table               string       `json:"table"`
	Version             *uint64      `json:"version,omitempty"`
	FreshnessLagRecords *uint64      `json:"freshness_lag_records,omitempty"`
	Total               uint64       `json:"total"`
	Result              any          `json:"result"`
}

func normalizeSuccessfulResponse(resp *http.Response, req RequestContext) ([]byte, bool, error) {
	if resp == nil || resp.Body == nil {
		return nil, false, nil
	}

	body, err := readAndReplaceBody(resp)
	if err != nil {
		return nil, false, err
	}

	kind := classifyResponseKind(req.BackendPath)
	if kind == responseKindUnknown || !isJSONResponse(resp, body) {
		return body, false, nil
	}

	var normalized any
	switch kind {
	case responseKindSearch:
		normalized, err = normalizeSearchResponse(body, req)
	case responseKindGraphNeighbors, responseKindGraphTraverse, responseKindGraphShortestPath:
		normalized, err = normalizeGraphResponse(kind, body, req)
	default:
		return body, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	encoded, err := json.Marshal(normalized)
	if err != nil {
		return nil, false, err
	}
	return encoded, true, nil
}

func classifyResponseKind(path string) responseKind {
	path = canonicalPublicBackendPath(path)
	switch {
	case strings.HasSuffix(path, "/query/search"), path == "/query/search":
		return responseKindSearch
	case strings.HasSuffix(path, "/graph/neighbors"), path == "/graph/neighbors":
		return responseKindGraphNeighbors
	case strings.HasSuffix(path, "/graph/traverse"), path == "/graph/traverse":
		return responseKindGraphTraverse
	case strings.HasSuffix(path, "/graph/shortest-path"), path == "/graph/shortest-path":
		return responseKindGraphShortestPath
	default:
		return responseKindUnknown
	}
}

func isJSONResponse(resp *http.Response, body []byte) bool {
	contentType := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Type")))
	if strings.Contains(contentType, "application/json") {
		return true
	}
	trimmed := bytes.TrimSpace(body)
	return len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[')
}

func readAndReplaceBody(resp *http.Response) ([]byte, error) {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	return body, nil
}

func normalizeSearchResponse(body []byte, req RequestContext) (any, error) {
	backend := strings.TrimSpace(string(req.PreferredBackend))
	if backend == "" {
		backend = inferBackendFromPath(req, body)
	}
	if backend == string(BackendServerless) {
		return normalizeServerlessSearchResponse(body, req)
	}
	return normalizeStatefulSearchResponse(body, req)
}

func normalizeServerlessSearchResponse(body []byte, req RequestContext) (any, error) {
	var payload struct {
		Namespace           string `json:"namespace"`
		Version             uint64 `json:"version"`
		View                string `json:"view"`
		FreshnessLagRecords uint64 `json:"freshness_lag_records"`
		HitCount            uint64 `json:"hit_count"`
		Hits                []struct {
			DocID string          `json:"doc_id"`
			Body  json.RawMessage `json:"body"`
			Score float64         `json:"score"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}

	hits := make([]publicSearchHit, 0, len(payload.Hits))
	for _, hit := range payload.Hits {
		hits = append(hits, publicSearchHit{
			ID:       hit.DocID,
			Score:    hit.Score,
			Document: decodePossiblyJSONDocument(hit.Body),
			Sources:  []string{"serverless"},
		})
	}

	version := payload.Version
	freshness := payload.FreshnessLagRecords
	return publicSearchResponse{
		Kind:                responseKindSearch,
		Backend:             string(BackendServerless),
		Tenant:              req.Tenant,
		Table:               req.ResourceName(),
		View:                firstNonEmpty(payload.View, NormalizePolicy(req.Policy).View),
		Version:             &version,
		FreshnessLagRecords: &freshness,
		Total:               payload.HitCount,
		Hits:                hits,
	}, nil
}

func normalizeStatefulSearchResponse(body []byte, req RequestContext) (any, error) {
	var payload struct {
		Total             uint64 `json:"total"`
		BleveSearchResult *struct {
			Hits []struct {
				ID     string         `json:"id"`
				Score  float64        `json:"score"`
				Fields map[string]any `json:"fields"`
			} `json:"hits"`
		} `json:"bleve_search_result"`
		VectorSearchResult map[string]*struct {
			Hits []struct {
				ID       string         `json:"id"`
				Score    float64        `json:"score"`
				Distance *float64       `json:"distance,omitempty"`
				Fields   map[string]any `json:"fields"`
			} `json:"hits"`
		} `json:"search_result"`
		FusionResult *struct {
			Total uint64 `json:"total"`
			Hits  []struct {
				ID          string             `json:"id"`
				Score       float64            `json:"score"`
				Fields      map[string]any     `json:"fields"`
				IndexScores map[string]float64 `json:"index_scores"`
			} `json:"hits"`
		} `json:"fusion_result"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}

	hitIndex := map[string]*publicSearchHit{}
	addOrMerge := func(hit publicSearchHit) {
		existing, ok := hitIndex[hit.ID]
		if !ok {
			copyHit := hit
			hitIndex[hit.ID] = &copyHit
			return
		}
		if hit.Score > existing.Score {
			existing.Score = hit.Score
		}
		if hit.Distance != nil && existing.Distance == nil {
			existing.Distance = hit.Distance
		}
		if len(hit.Fields) > 0 && len(existing.Fields) == 0 {
			existing.Fields = hit.Fields
		}
		existing.Sources = appendUniqueStrings(existing.Sources, hit.Sources...)
		if len(hit.IndexScores) > 0 {
			if existing.IndexScores == nil {
				existing.IndexScores = map[string]float64{}
			}
			for k, v := range hit.IndexScores {
				existing.IndexScores[k] = v
			}
		}
	}

	total := payload.Total
	if payload.FusionResult != nil && len(payload.FusionResult.Hits) > 0 {
		if payload.FusionResult.Total > 0 {
			total = payload.FusionResult.Total
		}
		for _, hit := range payload.FusionResult.Hits {
			addOrMerge(publicSearchHit{
				ID:          hit.ID,
				Score:       hit.Score,
				Fields:      hit.Fields,
				Sources:     []string{"fusion"},
				IndexScores: hit.IndexScores,
			})
		}
	} else {
		if payload.BleveSearchResult != nil {
			for _, hit := range payload.BleveSearchResult.Hits {
				addOrMerge(publicSearchHit{
					ID:      hit.ID,
					Score:   hit.Score,
					Fields:  hit.Fields,
					Sources: []string{"bleve"},
				})
			}
		}
		for indexName, result := range payload.VectorSearchResult {
			if result == nil {
				continue
			}
			source := "vector:" + indexName
			for _, hit := range result.Hits {
				addOrMerge(publicSearchHit{
					ID:       hit.ID,
					Score:    hit.Score,
					Distance: hit.Distance,
					Fields:   hit.Fields,
					Sources:  []string{source},
				})
			}
		}
	}

	hits := make([]publicSearchHit, 0, len(hitIndex))
	for _, hit := range hitIndex {
		hits = append(hits, *hit)
	}

	return publicSearchResponse{
		Kind:    responseKindSearch,
		Backend: string(BackendStateful),
		Tenant:  req.Tenant,
		Table:   req.ResourceName(),
		View:    NormalizePolicy(req.Policy).View,
		Total:   total,
		Hits:    hits,
	}, nil
}

func normalizeGraphResponse(kind responseKind, body []byte, req RequestContext) (any, error) {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}

	version := extractUint64(payload["version"])
	freshness := extractUint64(payload["freshness_lag_records"])
	total := uint64(0)
	switch kind {
	case responseKindGraphNeighbors:
		total = uint64Value(payload["neighbor_count"])
	case responseKindGraphTraverse:
		total = uint64Value(payload["node_count"])
	case responseKindGraphShortestPath:
		if found, ok := payload["found"].(bool); ok && found {
			total = 1
		}
	}

	result := cloneMap(payload)
	delete(result, "namespace")
	delete(result, "version")
	delete(result, "freshness_lag_records")

	return publicGraphResponse{
		Kind:                kind,
		Backend:             string(BackendServerless),
		Tenant:              req.Tenant,
		Table:               req.ResourceName(),
		Version:             version,
		FreshnessLagRecords: freshness,
		Total:               total,
		Result:              result,
	}, nil
}

func inferBackendFromPath(req RequestContext, body []byte) string {
	_ = body
	if req.RequireGraph || classifyResponseKind(req.BackendPath) != responseKindSearch {
		return string(BackendServerless)
	}
	return string(BackendStateful)
}

func decodePossiblyJSONDocument(raw json.RawMessage) any {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err == nil {
		return decoded
	}
	return string(raw)
}

func appendUniqueStrings(dst []string, values ...string) []string {
	seen := map[string]struct{}{}
	for _, value := range dst {
		seen[value] = struct{}{}
	}
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		dst = append(dst, value)
	}
	return dst
}

func extractUint64(value any) *uint64 {
	switch typed := value.(type) {
	case float64:
		v := uint64(typed)
		return &v
	case json.Number:
		if parsed, err := strconv.ParseUint(string(typed), 10, 64); err == nil {
			return &parsed
		}
	}
	return nil
}

func uint64Value(value any) uint64 {
	if parsed := extractUint64(value); parsed != nil {
		return *parsed
	}
	return 0
}

func cloneMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
