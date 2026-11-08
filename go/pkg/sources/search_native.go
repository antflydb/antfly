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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

type ProcessingDeclaration = management.ProcessingDeclaration

// SearchIndexClient uses ordinary native index APIs with runtime-owned
// credentials. Implementations must bound responses and refuse redirects.
type SearchIndexClient interface {
	InspectSearchIndex(context.Context, string, string) ([]byte, error)
	CreateSearchIndex(context.Context, string, string, map[string]any) error
}

func (p *Publisher) InspectSearchIndex(ctx context.Context, table, index string) ([]byte, error) {
	return p.call(ctx, "GET", tablePath(table)+"/indexes/"+url.PathEscape(index), nil)
}
func (p *Publisher) CreateSearchIndex(ctx context.Context, table, index string, config map[string]any) error {
	_, err := p.call(ctx, "POST", tablePath(table)+"/indexes/"+url.PathEscape(index), config)
	return err
}

// VectorSpaceIdentity binds every compatibility field into the native producer
// identity. It is a declaration, not proof of how external vectors were made.
func VectorSpaceIdentity(space VectorSpace) string {
	raw, _ := json.Marshal(space)
	sum := sha256.Sum256(raw)
	return "sources:v2:" + hex.EncodeToString(sum[:])
}

type nativeSearchIndex struct {
	Config map[string]any `json:"config"`
	Status struct {
		Readiness struct {
			State     string `json:"state"`
			Complete  bool   `json:"complete"`
			Queryable bool   `json:"queryable"`
			Sources   []struct {
				Artifact string `json:"artifact"`
				State    string `json:"state"`
				Complete bool   `json:"complete"`
			} `json:"sources"`
		} `json:"readiness"`
	} `json:"status"`
}

func decodeSearchIndex(raw []byte) (nativeSearchIndex, error) {
	var out nativeSearchIndex
	err := json.Unmarshal(raw, &out)
	if err == nil && out.Config == nil {
		err = fmt.Errorf("native index configuration missing")
	}
	return out, err
}
func nativeSources(config map[string]any) map[string]string {
	out := map[string]string{}
	sources, _ := config["sources"].([]any)
	for _, value := range sources {
		source, ok := value.(map[string]any)
		if !ok {
			continue
		}
		name, _ := source["artifact"].(string)
		field, _ := source["field"].(string)
		if field == "" {
			field, _ = config["field"].(string)
		}
		out[name] = field
	}
	return out
}
func nativeEnrichments(config map[string]any) map[string]map[string]any {
	out := map[string]map[string]any{}
	values, _ := config["enrichments"].([]any)
	for _, v := range values {
		if item, ok := v.(map[string]any); ok {
			name, _ := item["name"].(string)
			out[name] = item
		}
	}
	return out
}
func nativeLineage(enrichments map[string]map[string]any, member string, stream SearchStream) bool {
	seen := map[string]bool{}
	for hops := 0; hops <= 64; hops++ {
		if stream.Artifact != "" && member == stream.Artifact {
			return true
		}
		if seen[member] {
			return false
		}
		seen[member] = true
		e, ok := enrichments[member]
		if !ok {
			return false
		}
		upstream, _ := e["source_artifact_name"].(string)
		if upstream == "" {
			if stream.Artifact != "" {
				return false
			}
			expected := stream.Field
			if stream.ResourceField != "" {
				expected = stream.ResourceField + ".uri"
			}
			field, _ := e["field"].(string)
			template, _ := e["template"].(string)
			return expected != "" && (field == expected || template == "{{"+expected+"}}")
		}
		member = upstream
	}
	return false
}

func checkNativeStream(stream SearchStream, index nativeSearchIndex, semantic bool) error {
	cfg := index.Config
	sources := nativeSources(cfg)
	if !semantic {
		if cfg["type"] != "full_text" {
			return fmt.Errorf("text binding does not name a full-text index")
		}
		if stream.Artifact != "" {
			if len(sources) != 1 {
				return fmt.Errorf("text stream index must scope the declared artifact; use a union stream for combined retrieval")
			}
			field, ok := sources[stream.Artifact]
			if !ok || field != stream.Field && !(field == "" && stream.Field == "$") {
				return fmt.Errorf("text artifact projection mismatch")
			}
		} else if cfg["field"] != stream.Field {
			return fmt.Errorf("text field mismatch")
		}
		return nil
	}
	binding := stream.Retrieval.Semantic
	if cfg["type"] != "embeddings" || cfg["sparse"] == true || intNumber(cfg["dimension"]) != binding.VectorSpace.Dimension {
		return fmt.Errorf("dense vector index dimension/type mismatch")
	}
	metric, _ := cfg["distance_metric"].(string)
	if metric == "" {
		metric = "cosine"
	}
	if metric != string(binding.VectorSpace.Metric) {
		return fmt.Errorf("vector metric mismatch")
	}
	external, _ := cfg["external"].(bool)
	if external != binding.External {
		return fmt.Errorf("vector producer origin mismatch")
	}
	if external {
		if cfg["external_vector_space"] != VectorSpaceIdentity(binding.VectorSpace) || len(binding.MemberArtifacts) != 0 || stream.Artifact != "" {
			return fmt.Errorf("external vector declaration mismatch")
		}
		return nil
	}
	if len(binding.MemberArtifacts) == 0 {
		return fmt.Errorf("managed semantic bindings require explicit member artifacts")
	}
	if len(sources) != len(binding.MemberArtifacts) {
		return fmt.Errorf("semantic stream must declare every member of its index")
	}
	enrichments := nativeEnrichments(cfg)
	for _, member := range binding.MemberArtifacts {
		if _, ok := sources[member]; !ok {
			return fmt.Errorf("semantic member absent from index")
		}
		e, ok := enrichments[member]
		if !ok || e["kind"] != "embedding" || e["vector_space"] != VectorSpaceIdentity(binding.VectorSpace) {
			return fmt.Errorf("embedding recipe identity mismatch")
		}
		if !nativeLineage(enrichments, member, stream) {
			return fmt.Errorf("semantic member does not derive from representation")
		}
	}
	for _, input := range binding.QueryInputs {
		if input == "text" {
			embedder, _ := cfg["embedder"].(map[string]any)
			if embedder["model"] != binding.VectorSpace.Model {
				return fmt.Errorf("query embedder differs from pinned model")
			}
		}
	}
	return nil
}
func intNumber(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	}
	return 0
}

// ReconcileProcessing creates missing native indexes. Existing names are never
// replaced: a changed recipe uses a new name, preserving shared-table owners.
// Native create starts resumable backfill; acquisition is not replayed.
func ReconcileProcessing(ctx context.Context, client SearchIndexClient, table string, plan *ProcessingDeclaration) error {
	if plan == nil {
		return nil
	}
	if err := ValidateProcessing(plan); err != nil {
		return err
	}
	for _, declaration := range plan.Indexes {
		raw, err := client.InspectSearchIndex(ctx, table, declaration.Name)
		var httpErr interface{ HTTPStatus() int }
		var publish *PublishHTTPError
		missing := errors.As(err, &publish) && publish.Status == 404 || errors.As(err, &httpErr) && httpErr.HTTPStatus() == 404
		if err != nil && !missing {
			return err
		}
		if missing {
			if err = client.CreateSearchIndex(ctx, table, declaration.Name, declaration.Config); err != nil {
				// A concurrent reconciler or lost acknowledgement may have
				// created the same declaration. Reobserve, never replay a write.
				observed, readErr := client.InspectSearchIndex(ctx, table, declaration.Name)
				if readErr != nil {
					return err
				}
				installed, readErr := decodeSearchIndex(observed)
				if readErr != nil || !containsNativeConfig(installed.Config, declaration.Config) {
					return err
				}
			}
			continue
		}
		index, err := decodeSearchIndex(raw)
		if err != nil {
			return err
		}
		// Compare submitted fields; native configurations include defaults and names.
		if !containsNativeConfig(index.Config, declaration.Config) {
			return fmt.Errorf("native index %s conflicts with desired processing; migrate to a new index name", declaration.Name)
		}
	}
	return nil
}
func containsNativeConfig(actual, desired map[string]any) bool {
	for key, want := range desired {
		// Native readback redacts producer config. Recipe names are content-addressed
		// and are never overwritten by source reconciliation.
		if key == "producer" || key == "producer_json" || key == "transcriber" {
			continue
		}
		got, ok := actual[key]
		if !ok {
			return false
		}
		if nested, ok := want.(map[string]any); ok {
			g, ok := got.(map[string]any)
			if !ok || !containsNativeConfig(g, nested) {
				return false
			}
			continue
		}
		if list, ok := want.([]any); ok {
			g, ok := got.([]any)
			if !ok || len(g) != len(list) {
				return false
			}
			for i, item := range list {
				if nested, ok := item.(map[string]any); ok {
					m, ok := g[i].(map[string]any)
					if !ok || !containsNativeConfig(m, nested) {
						return false
					}
				} else {
					a, _ := json.Marshal(item)
					z, _ := json.Marshal(g[i])
					if string(a) != string(z) {
						return false
					}
				}
			}
			continue
		}
		a, _ := json.Marshal(got)
		z, _ := json.Marshal(want)
		if string(a) != string(z) {
			return false
		}
	}
	return true
}

// NativeIndexName makes immutable recipe migrations explicit, including fields
// native readback deliberately redacts. Existing indexes are never overwritten.
func NativeIndexName(prefix string, config map[string]any) string {
	raw, _ := json.Marshal(config)
	sum := sha256.Sum256(raw)
	return prefix + "-" + hex.EncodeToString(sum[:16])
}
func hasWriteOnlyProducer(v any) bool {
	switch value := v.(type) {
	case map[string]any:
		for k, v := range value {
			if k == "producer" || k == "producer_json" || k == "transcriber" {
				return true
			}
			if hasWriteOnlyProducer(v) {
				return true
			}
		}
	case []any:
		for _, v := range value {
			if hasWriteOnlyProducer(v) {
				return true
			}
		}
	}
	return false
}

func ValidateProcessing(plan *ProcessingDeclaration) error {
	if plan == nil {
		return nil
	}
	if len(plan.Indexes) > 32 {
		return fmt.Errorf("too many processing indexes")
	}
	names := map[string]bool{}
	for _, index := range plan.Indexes {
		if err := validateProcessingSecrets(index.Config); err != nil {
			return err
		}
		if hasWriteOnlyProducer(index.Config) && !strings.HasSuffix(index.Name, NativeIndexName("", index.Config)) {
			return fmt.Errorf("write-only producer recipes require a content-addressed native index name")
		}

		if !validBindingName(index.Name) || names[index.Name] || index.Config["type"] == nil {
			return fmt.Errorf("invalid native index declaration")
		}
		names[index.Name] = true
	}
	return nil
}

// ObserveSearch checks native bindings once per distinct index, with bounded
// metadata. Readiness is advisory; callers must recheck at query time. It never
// turns successful acquisition into invented inference completion.
func ObserveSearch(ctx context.Context, client SearchIndexClient, binding Binding) management.SearchStatus {
	ctx, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	out := management.SearchStatus{Version: SearchVersion, ObservedAt: int(time.Now().Unix()), Consistency: "unknown", Streams: []management.RepresentationStatus{}, Artifacts: []management.RepresentationStatus{}}
	cache := map[string]nativeSearchIndex{}
	failures := map[string]error{}
	metadataBytes := 0
	inspect := func(name string) (nativeSearchIndex, error) {
		if v, ok := cache[name]; ok {
			return v, nil
		}
		if err, ok := failures[name]; ok {
			return nativeSearchIndex{}, err
		}
		if metadataBytes >= 32<<20 {
			return nativeSearchIndex{}, fmt.Errorf("native metadata budget exceeded")
		}
		raw, err := client.InspectSearchIndex(ctx, binding.Table, name)
		metadataBytes += len(raw)
		if metadataBytes > 32<<20 {
			return nativeSearchIndex{}, fmt.Errorf("native metadata budget exceeded")
		}
		if err == nil {
			var v nativeSearchIndex
			v, err = decodeSearchIndex(raw)
			if err == nil {
				cache[name] = v
				return v, nil
			}
		}
		failures[name] = err
		return nativeSearchIndex{}, err
	}
	for _, stream := range binding.Streams {
		status := management.RepresentationStatus{Name: stream.Name, State: "ready"}
		r := stream.Retrieval
		if r == nil {
			if stream.Index != "" {
				r = &RetrievalBindings{FullText: &management.FullTextRetrieval{Index: stream.Index}}
			}
		}
		if r != nil {
			for _, check := range []struct {
				name     string
				semantic bool
			}{{func() string {
				if r.FullText != nil {
					return r.FullText.Index
				}
				return ""
			}(), false}, {func() string {
				if r.Semantic != nil {
					return r.Semantic.Index
				}
				return ""
			}(), true}} {
				if check.name == "" {
					continue
				}
				index, err := inspect(check.name)
				if err != nil {
					status.State = "building"
					status.Reason = "native_index_unavailable"
					continue
				}
				if err = checkNativeStream(stream, index, check.semantic); err != nil {
					status.State = "unsupported"
					status.Reason = "native_binding_mismatch"
					break
				}
				if index.Status.Readiness.State == "failed" {
					status.State = "failed"
					status.Reason = "native_index_failed"
					break
				}
				if !index.Status.Readiness.Complete || !index.Status.Readiness.Queryable {
					if index.Status.Readiness.State == "queryable_partial" {
						status.State = "partial"
						status.Reason = "native_index_partial"
					} else if status.State != "partial" {
						status.State = "building"
						status.Reason = "native_index_incomplete"
					}
				}
			}
		}
		out.Streams = append(out.Streams, status)
	}
	for _, artifact := range binding.Artifacts {
		status := management.RepresentationStatus{Name: artifact.Name, State: "unsupported", Reason: "per_artifact_coverage_not_observed"}
		for _, index := range cache {
			for _, source := range index.Status.Readiness.Sources {
				if source.Artifact != artifact.Artifact {
					continue
				}
				if source.State == "failed" {
					status.State = "failed"
					status.Reason = "native_artifact_failed"
				} else if !source.Complete {
					if status.State != "failed" {
						status.State = "building"
						status.Reason = "native_artifact_incomplete"
					}
				} else if status.State == "unsupported" {
					status.State = "ready"
					status.Reason = ""
				}
			}
		}
		out.Artifacts = append(out.Artifacts, status)
	}
	return out
}

// Keep this compile-time assertion close to the portable/native boundary.
var _ SearchIndexClient = (*Publisher)(nil)

// Processing is driven independently of acquisition. Runtime adapters can call
// this after desired state persistence, and retry it without provider downloads.
func ReconcileSourceSearch(ctx context.Context, client SearchIndexClient, source Source) error {
	if err := ValidateSearchBindings(source.SearchVersion, source.Streams, source.Artifacts); err != nil {
		return err
	}
	return ReconcileProcessing(ctx, client, source.Table, source.Processing)
}

func validateProcessingSecrets(value any) error { return validateProcessingSecretsDepth(value, 0) }

func validateProcessingSecretsDepth(value any, depth int) error {
	if depth > 64 {
		return fmt.Errorf("processing declaration exceeds depth budget")
	}
	switch v := value.(type) {
	case map[string]any:
		for key, value := range v {
			switch strings.ToLower(key) {
			case "api_key", "token", "access_token", "client_secret", "private_key", "password", "secret_access_key", "access_key_id", "session_token", "authorization":
				if text, ok := value.(string); ok && text != "" && !strings.HasPrefix(text, "${secret:") {
					return fmt.Errorf("processing credentials require native secret references")
				}
			}
			if err := validateProcessingSecretsDepth(value, depth+1); err != nil {
				return err
			}
		}
	case []any:
		for _, value := range v {
			if err := validateProcessingSecretsDepth(value, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

// SourceTableConfig declares source ownership predicates as exact keyword
// columns up front. Inferred prose fields are not an indexed equality contract.
// Existing tables are never implicitly rewritten or migrated.
func SourceTableConfig() map[string]any {
	keyword := func() map[string]any { return map[string]any{"type": "string", "x-antfly-types": []string{"keyword"}} }
	return map[string]any{"num_shards": 1, "schema": map[string]any{"default_type": "doc", "document_schemas": map[string]any{"doc": map[string]any{"schema": map[string]any{"type": "object", "additionalProperties": true, "x-antfly-dynamic-indexing": map[string]any{"mode": "infer_types"}, "properties": map[string]any{"source_id": keyword(), "doc_type": keyword(), "permissions": map[string]any{"type": "object", "additionalProperties": true, "properties": map[string]any{"public": map[string]any{"type": "boolean"}, "readers": map[string]any{"type": "array", "items": keyword()}}}}}}}}}
}
