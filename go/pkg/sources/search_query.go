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

package sources

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"
)

// SearchRequest intentionally has no arbitrary native query/template override.
// Source filters narrow selection; authenticated native/gateway policy remains
// the security boundary. Exact grep retains its separate exhaustive scanner.
type SearchVerification struct {
	AllowScan    bool `json:"allow_scan"`
	MaxDocuments int  `json:"max_documents"`
	MaxBytes     int  `json:"max_bytes"`
}
type SearchRequest struct {
	Verification    *SearchVerification `json:"verification,omitempty"`
	RequireSnapshot bool                `json:"require_snapshot,omitempty"`
	Stream          string              `json:"stream"`
	Mode            string              `json:"mode"`
	Text            string              `json:"text,omitempty"`
	Vector          []float32           `json:"vector,omitempty"`
	VectorSpace     *VectorSpace        `json:"vector_space,omitempty"`
	Limit           int                 `json:"limit,omitempty"`
	Matches         int                 `json:"matches,omitempty"`
}

func BuildSearchQuery(binding Binding, request SearchRequest) ([]byte, error) {
	if err := ValidateSearchBindings(binding.SearchVersion, binding.Streams, binding.Artifacts); err != nil {
		return nil, err
	}
	if binding.SearchVersion != SearchVersion {
		return nil, fmt.Errorf("ranked stream retrieval requires search_version=2")
	}
	var selected *SearchStream
	for i := range binding.Streams {
		stream := &binding.Streams[i]
		if request.Stream != "" {
			if stream.Name == request.Stream {
				selected = stream
				break
			}
			continue
		}
		if stream.Retrieval == nil {
			continue
		}
		isDefault := request.Mode == "semantic" && stream.Retrieval.Semantic != nil && stream.Retrieval.Semantic.Default || request.Mode == "full_text" && stream.Retrieval.FullText != nil && stream.Retrieval.FullText.Default
		if isDefault {
			if selected != nil {
				return nil, fmt.Errorf("select a stream explicitly when several defaults are enabled")
			}
			selected = stream
		}
	}
	if selected == nil || selected.Retrieval == nil {
		return nil, fmt.Errorf("unknown or unavailable stream")
	}
	stream := *selected
	if request.Limit == 0 {
		request.Limit = 10
	}
	if request.Matches == 0 {
		request.Matches = 3
	}
	if request.Limit < 1 || request.Limit > 100 || request.Matches < 1 || request.Matches > 10 || request.Limit*request.Matches > 1000 {
		return nil, fmt.Errorf("search response exceeds group/member budget")
	}
	if !utf8.ValidString(request.Text) {
		return nil, fmt.Errorf("query text is not valid UTF-8")
	}
	if len(request.Text) > 65536 {
		return nil, fmt.Errorf("query text exceeds budget")
	}
	fields := []string{"source", "permissions", "metadata", "content.mime_type"}
	if binding.Fields != nil {
		fields = []string{binding.Fields.URI, binding.Fields.Path, binding.Fields.Revision, binding.Fields.Metadata}
	}
	memberField := stream.Field
	if request.Mode == "semantic" && stream.Retrieval.Semantic != nil {
		memberField = stream.Retrieval.Semantic.MemberField
	}
	memberFields := []string{}
	for _, field := range []string{memberField, stream.LocationField} {
		if field != "" {
			memberFields = append(memberFields, field)
		}
	}
	// Media evidence is a parent projection, never an unscoped provider URL.
	if stream.ResourceField != "" {
		fields = append(fields, stream.ResourceField)
	}
	scope, err := canonicalSearchFilter(binding.Filter)
	if err != nil {
		return nil, err
	}
	body := map[string]any{"filter_query": scope, "limit": request.Limit, "fields": fields, "hierarchy": map[string]any{"group_by": map[string]any{"level": "source", "matches": map[string]any{"limit": request.Matches, "fields": memberFields}}}}
	if stream.ResourceField != "" {
		body["filter_query"] = map[string]any{"bool": map[string]any{"filter": []any{scope, map[string]any{"exists": map[string]any{"path": bindingPointer(stream.ResourceField + ".uri")}}}}}
	}

	if v := request.Verification; v != nil {
		if v.MaxDocuments < 1 || v.MaxDocuments > 1000000 || v.MaxBytes < 1 || v.MaxBytes > 256<<20 {
			return nil, fmt.Errorf("invalid verification budget")
		}
		body["execution"] = map[string]any{"value_verification": v}
	}
	switch request.Mode {
	case "full_text":
		if stream.Retrieval.FullText == nil || request.Text == "" || len(request.Vector) > 0 {
			return nil, fmt.Errorf("full-text stream requires query text")
		}
		body["full_text_index"] = stream.Retrieval.FullText.Index
		body["full_text_search"] = map[string]any{"query": request.Text}
	case "semantic":
		semantic := stream.Retrieval.Semantic
		if semantic != nil && semantic.External {
			delete(body, "hierarchy")
			// Native external vectors retain their legacy omission semantics.
			// Canonical acquisition replaces metadata without writer receipts;
			// require the exact selected recipe receipt so an older retained vector is
			// never exposed after a provider revision replaces its parent.
			receipt := map[string]any{"term": map[string]any{
				"path":  "/metadata/embedding_spaces/" + strings.ReplaceAll(strings.ReplaceAll(semantic.Index, "~", "~0"), "/", "~1"),
				"value": VectorSpaceIdentity(semantic.VectorSpace),
			}}
			body["filter_query"] = map[string]any{"bool": map[string]any{"filter": []any{body["filter_query"], receipt}}}
		}
		if semantic == nil {
			return nil, fmt.Errorf("stream has no semantic retrieval")
		}
		body["indexes"] = []string{semantic.Index}
		accepted := map[string]bool{}
		for _, v := range semantic.QueryInputs {
			accepted[string(v)] = true
		}
		if len(request.Vector) > 0 {
			if request.Text != "" || !accepted["vector"] || request.VectorSpace == nil || !CompatibleVectorSpace(semantic.VectorSpace, *request.VectorSpace) || len(request.Vector) != semantic.VectorSpace.Dimension {
				return nil, fmt.Errorf("incompatible query vector")
			}
			for _, v := range request.Vector {
				if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
					return nil, fmt.Errorf("non-finite query vector")
				}
			}
			body["embeddings"] = map[string]any{semantic.Index: request.Vector}
		} else {
			if request.Text == "" || !accepted["text"] {
				return nil, fmt.Errorf("unsupported semantic query input")
			}
			body["semantic_search"] = request.Text
		}
	default:
		return nil, fmt.Errorf("ranked retrieval supports full_text or semantic; exact search requires the exhaustive scanner")
	}
	return json.Marshal(body)
}

// Search uses the normal native query API. It validates native bindings and
// convergence immediately before querying; clients still inspect the returned
// native consistency/policy context and must not treat this as a snapshot lease.
func (p *Publisher) Search(ctx context.Context, binding Binding, request SearchRequest) ([]byte, error) {
	ctx = scopedSearchContext(ctx, binding)
	if binding.Status != "searchable" || binding.ValidUntil == nil || time.Now().Unix() >= *binding.ValidUntil {
		return nil, fmt.Errorf("source acquisition/access is not searchable")
	}
	raw, err := BuildSearchQuery(binding, request)
	if err != nil {
		return nil, err
	}
	// The builder has already rejected ambiguous defaults.
	var selected SearchStream
	for _, stream := range binding.Streams {
		if stream.Name == request.Stream {
			selected = stream
			break
		}
		if request.Stream == "" && stream.Retrieval != nil {
			r := stream.Retrieval
			if request.Mode == "semantic" && r.Semantic != nil && r.Semantic.Default || request.Mode == "full_text" && r.FullText != nil && r.FullText.Default {
				selected = stream
				break
			}
		}
	}
	indexName := ""
	semantic := request.Mode == "semantic"
	if semantic {
		indexName = selected.Retrieval.Semantic.Index
	} else {
		indexName = selected.Retrieval.FullText.Index
	}
	observe := func() ([32]byte, error) {
		response, err := p.InspectSearchIndex(ctx, binding.Table, indexName)
		if err != nil {
			return [32]byte{}, err
		}
		index, err := decodeSearchIndex(response)
		if err != nil {
			return [32]byte{}, err
		}
		if err = checkNativeStream(selected, index, semantic); err != nil {
			return [32]byte{}, err
		}
		if !index.Status.Readiness.Complete || !index.Status.Readiness.Queryable {
			return [32]byte{}, fmt.Errorf("native search index is incomplete")
		}
		var wire map[string]any
		if err = json.Unmarshal(response, &wire); err != nil {
			return [32]byte{}, err
		}
		status, _ := wire["status"].(map[string]any)
		encoded, _ := json.Marshal(map[string]any{"config": index.Config, "readiness": status["readiness"]})
		return sha256.Sum256(encoded), nil
	}
	before, err := observe()
	if err != nil {
		return nil, err
	}
	response, err := p.Query(ctx, binding.Table, raw)
	if err != nil {
		return nil, err
	}
	after, err := observe()
	if err != nil {
		return nil, err
	}
	if time.Now().Unix() >= *binding.ValidUntil {
		return nil, fmt.Errorf("source access expired during retrieval")
	}
	if before != after {
		return nil, fmt.Errorf("native search generation changed during retrieval")
	}
	consistency, err := SearchConsistency(response)
	if err != nil {
		return nil, err
	}
	if request.RequireSnapshot && consistency != "retained_snapshot" {
		return nil, fmt.Errorf("operation requires a retained snapshot; native runtime returned %s", consistency)
	}
	// Keep native hits opaque. Decoding arbitrary evidence into interface values
	// both rounds JSON numbers and allocates a second object tree for the result.
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(response, &envelope); err != nil {
		return nil, err
	}
	result := envelope
	if raw, ok := envelope["responses"]; ok {
		var responses []map[string]json.RawMessage
		if err := json.Unmarshal(raw, &responses); err != nil {
			return nil, err
		}
		if len(responses) != 1 {
			return nil, fmt.Errorf("unexpected search response count")
		}
		result = responses[0]
	}
	hits := bytes.TrimSpace(result["hits"])
	if len(hits) == 0 || hits[0] != '{' {
		return nil, fmt.Errorf("native search hits unavailable")
	}
	// Unretained relevance results cannot prove authoritative absence.
	metadata, err := json.Marshal(map[string]any{"source_id": binding.ID, "stream": selected.Name, "index": indexName, "mode": request.Mode, "purpose": selected.Purpose, "generated": selected.Generated, "consistency": consistency})
	if err != nil {
		return nil, err
	}
	envelope["source_search"] = metadata
	return json.Marshal(envelope)
}

// SearchConsistency reports what the response actually observed, not a blanket
// capability inferred from an endpoint or successful write acknowledgement.
func SearchConsistency(raw []byte) (string, error) {
	var response struct {
		Responses []struct {
			Error          json.RawMessage `json:"error"`
			Status         int             `json:"status"`
			RemoteSnapshot string          `json:"remote_snapshot"`
			LiveRead       bool            `json:"live_read"`
		} `json:"responses"`
		RemoteSnapshot string          `json:"remote_snapshot"`
		LiveRead       bool            `json:"live_read"`
		Error          json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return "unknown", err
	}
	if len(response.Error) > 0 && string(response.Error) != "null" {
		return "unknown", fmt.Errorf("native query failed")
	}
	if len(response.Responses) > 0 {
		if len(response.Responses) != 1 {
			return "unknown", fmt.Errorf("unexpected search response count")
		}
		r := response.Responses[0]
		if r.Status >= 400 || len(r.Error) > 0 && string(r.Error) != "null" {
			return "unknown", fmt.Errorf("native query failed")
		}
		response.RemoteSnapshot = r.RemoteSnapshot
		response.LiveRead = r.LiveRead
	}
	if response.RemoteSnapshot != "" {
		return "retained_snapshot", nil
	}
	if response.LiveRead {
		return "live_read", nil
	}
	// Unordered relevance queries do not promise a retained generation.
	// Absence of a token is never upgraded into snapshot or live-read proof.
	return "unretained", nil
}
