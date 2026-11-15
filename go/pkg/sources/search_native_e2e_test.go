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
	"encoding/json"
	"fmt"
	"github.com/antflydb/antfly/go/pkg/sources/management"
	"github.com/stretchr/testify/require"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"
)

// Deterministic native integration; supplied vectors do not qualify model relevance.
func TestNativeSourceSearchStreams(t *testing.T) {
	endpoint := os.Getenv("ANTFLY_SOURCES_E2E_URL")
	if endpoint == "" {
		t.Skip("set ANTFLY_SOURCES_E2E_URL to an isolated native server")
	}
	u, err := url.Parse(endpoint)
	require.NoError(t, err)
	require.True(t, u.Scheme == "http" && net.ParseIP(u.Hostname()).IsLoopback())
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	p := &Publisher{Endpoint: endpoint}
	table := fmt.Sprintf("source_search_contract_%d", time.Now().UnixNano())
	require.NoError(t, p.EnsureTable(ctx, table))
	plan, text, artifact, err := BuildTextProcessing(TextProcessingPreset{Name: "content", Field: "content.text"})
	require.NoError(t, err)
	space := testVectorSpace()
	ext, err := ExternalVectorIndex("original_vectors", space)
	require.NoError(t, err)
	units := text.Artifact
	chunkConfig := map[string]any{"kind": "chunk", "name": "fixture_chunks", "source_artifact_name": units, "field": "text", "chunk_size": 512, "chunk_overlap": 64}
	chunkArtifact := "fixture_chunks"
	chunkIndex := management.NativeIndexDeclaration{Name: "chunk_text", Config: map[string]any{"type": "full_text", "sources": []any{map[string]any{"artifact": chunkArtifact, "field": "text"}}, "enrichments": []any{chunkConfig}}}
	plan.Indexes = append(plan.Indexes, ext, chunkIndex)
	require.NoError(t, ReconcileProcessing(ctx, p, table, &plan))
	// A matching index-level field cannot turn artifact-only sources into
	// canonical parent text. Verify against actual native normalized readback.
	misboundConfig := map[string]any{"type": "full_text", "field": "content.text", "sources": []any{map[string]any{"artifact": units, "field": "text"}}}
	require.NoError(t, p.CreateSearchIndex(ctx, table, "misbound_text", misboundConfig))
	misboundRaw, err := p.InspectSearchIndex(ctx, table, "misbound_text")
	require.NoError(t, err)
	misboundIndex, err := decodeSearchIndex(misboundRaw)
	require.NoError(t, err)
	require.Error(t, checkNativeStream(SearchStream{Name: "parent", Field: "content.text"}, misboundIndex, false))
	original := SearchStream{Name: "original", ResourceField: "content.resource", Retrieval: &RetrievalBindings{Semantic: &management.SemanticRetrieval{Index: ext.Name, External: true, VectorSpace: space, QueryInputs: []management.SemanticRetrievalQueryInputs{"vector"}}}}
	parents := map[string]any{}
	for _, item := range []struct {
		id      string
		content map[string]any
		vector  []float32
	}{
		{"text", map[string]any{"text": "launch at 04:12"}, []float32{1, 0, 0}},
		{"image", map[string]any{"resource": map[string]any{"type": "source", "uri": "http://127.0.0.1:1/revision-pinned-image", "version": "r1", "mime_type": "image/png"}}, []float32{0, 1, 0}},
	} {
		doc, err := WithExternalVector(map[string]any{"source_id": testSourceID, "doc_type": "source_item", "source": map[string]any{"uri": "local:///" + item.id, "path": item.id, "revision": "r1"}, "content": item.content, "permissions": map[string]any{"public": true}, "metadata": map[string]any{}}, ext.Name, space, item.vector)
		require.NoError(t, err)
		parents[item.id] = doc
	}
	invalid, err := WithExternalVector(parents["image"].(map[string]any), ext.Name, space, []float32{0, 1, 0})
	require.NoError(t, err)
	invalid["metadata"].(map[string]any)["embedding_spaces"].(map[string]any)[ext.Name] = "different-recipe"
	require.Error(t, p.Batch(ctx, table, map[string]any{"invalid-vector": invalid}, nil), "public index translation must preserve external writer admission identity")
	_, err = p.call(ctx, "POST", tablePath(table)+"/batch", map[string]any{"inserts": parents, "sync_level": "write"})
	require.NoError(t, err)
	valid := time.Now().Add(5 * time.Minute).Unix()
	binding := Binding{ID: testSourceID, Table: table, SearchVersion: 2, Streams: []SearchStream{text, original}, Artifacts: []ArtifactBinding{artifact}, Filter: SourceFilter(testSourceID), Status: "searchable", ValidUntil: &valid}
	defer func() {
		if t.Failed() {
			status := ObserveSearch(ctx, p, binding)
			t.Logf("final readiness: %+v", status)
		}
	}()
	require.Eventually(t, func() bool {
		for _, s := range ObserveSearch(ctx, p, binding).Streams {
			if s.State != "ready" {
				return false
			}
		}
		return true
	}, 60*time.Second, 100*time.Millisecond)
	raw, err := p.Search(ctx, binding, SearchRequest{Stream: text.Name, Mode: "full_text", Text: "launch"})
	require.NoError(t, err)
	var wire struct {
		Responses []struct {
			Hits struct {
				Hits []struct {
					ID        string `json:"_id"`
					Hierarchy struct {
						Matches []struct {
							Source map[string]any `json:"_source"`
						} `json:"matches"`
					} `json:"hierarchy"`
				} `json:"hits"`
			} `json:"hits"`
		} `json:"responses"`
	}
	require.NoError(t, json.Unmarshal(raw, &wire))
	require.Len(t, wire.Responses, 1)
	require.Len(t, wire.Responses[0].Hits.Hits, 1)
	require.Equal(t, "text", wire.Responses[0].Hits.Hits[0].ID)
	require.NotEmpty(t, wire.Responses[0].Hits.Hits[0].Hierarchy.Matches)
	require.Equal(t, "launch at 04:12", wire.Responses[0].Hits.Hits[0].Hierarchy.Matches[0].Source["$"])
	chunkStream := text
	chunkStream.Name, chunkStream.Artifact, chunkStream.Field = "chunks", chunkArtifact, "text"
	chunkStream.Retrieval = &RetrievalBindings{FullText: &management.FullTextRetrieval{Index: chunkIndex.Name}}
	binding.Streams = append(binding.Streams, chunkStream)
	require.Eventually(t, func() bool {
		status := ObserveSearch(ctx, p, binding)
		return status.Streams[len(status.Streams)-1].State == "ready"
	}, 60*time.Second, 100*time.Millisecond)
	raw, err = p.Search(ctx, binding, SearchRequest{Stream: chunkStream.Name, Mode: "full_text", Text: "launch"})
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &wire))
	require.Len(t, wire.Responses[0].Hits.Hits, 1)
	require.Equal(t, "launch at 04:12", wire.Responses[0].Hits.Hits[0].Hierarchy.Matches[0].Source["text"])
	request := SearchRequest{Stream: original.Name, Mode: "semantic", Vector: []float32{0, 1, 0}, VectorSpace: &space}
	raw, err = p.Search(ctx, binding, request)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &wire))
	require.Len(t, wire.Responses[0].Hits.Hits, 1)
	require.Equal(t, "image", wire.Responses[0].Hits.Hits[0].ID)
	require.Contains(t, string(raw), "revision-pinned-image")
	require.Contains(t, string(raw), `"source_search"`)
	require.Contains(t, string(raw), `"consistency":"unretained"`)
	request.RequireSnapshot = true
	_, err = p.Search(ctx, binding, request)
	require.ErrorContains(t, err, "requires a retained snapshot")
	binding.Filter = SourceFilter("00000000-0000-4000-8000-000000000099")
	request.RequireSnapshot = false
	raw, err = p.Search(ctx, binding, request)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &wire))
	require.Empty(t, wire.Responses[0].Hits.Hits)
	binding.Filter = SourceFilter(testSourceID)
	// Acquisition replaces metadata and does not assert an external producer
	// receipt. Native omission may retain vector bytes; discovery must hide them.
	media := parents["image"].(map[string]any)
	delete(media, "_embeddings")
	media["metadata"] = map[string]any{}
	media["source"].(map[string]any)["revision"] = "r2"
	_, err = p.call(ctx, "POST", tablePath(table)+"/batch", map[string]any{"inserts": map[string]any{"image": media}, "sync_level": "write"})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		for _, s := range ObserveSearch(ctx, p, binding).Streams {
			if s.State != "ready" {
				return false
			}
		}
		return true
	}, 60*time.Second, 100*time.Millisecond)
	raw, err = p.Search(ctx, binding, request)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &wire))
	require.Empty(t, wire.Responses[0].Hits.Hits, "new acquisition cannot expose an older external vector")
	media["metadata"] = map[string]any{"embedding_spaces": map[string]any{ext.Name: VectorSpaceIdentity(space)}}
	err = p.Batch(ctx, table, map[string]any{"image": media}, nil)
	require.Error(t, err, "a valid receipt without an accompanying vector cannot reauthorize retained bytes")
	media["metadata"] = map[string]any{"embedding_spaces": map[string]any{ext.Name: "different-recipe"}}
	_, err = p.call(ctx, "POST", tablePath(table)+"/batch", map[string]any{"inserts": map[string]any{"image": media}, "sync_level": "full_index"})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		for _, status := range ObserveSearch(ctx, p, binding).Streams {
			if status.State != "ready" {
				return false
			}
		}
		return true
	}, 60*time.Second, 100*time.Millisecond)
	raw, err = p.Search(ctx, binding, request)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &wire))
	require.Empty(t, wire.Responses[0].Hits.Hits, "a mismatched receipt cannot authorize an omitted retained vector")
	// Declared plaintext retains its exact type even when its bytes parse as JSON.
	plain := parents["text"].(map[string]any)
	delete(plain, "_embeddings")
	plain["metadata"] = map[string]any{}
	plain["content"].(map[string]any)["text"] = `{"text":"launch"}`
	plain["source"].(map[string]any)["revision"] = "r2"
	_, err = p.call(ctx, "POST", tablePath(table)+"/batch", map[string]any{"inserts": map[string]any{"text": plain}, "sync_level": "full_index"})
	require.NoError(t, err)
	// Acknowledged parent writes can still advance dependent artifact coverage.
	// Wait on the actual query admission boundary rather than assuming every
	// derived source remains complete immediately after the write receipt.
	require.Eventually(t, func() bool {
		raw, err = p.Search(ctx, binding, SearchRequest{Stream: text.Name, Mode: "full_text", Text: "launch"})
		return err == nil
	}, 30*time.Second, 100*time.Millisecond)
	require.NoError(t, json.Unmarshal(raw, &wire))
	require.Len(t, wire.Responses[0].Hits.Hits, 1)
	require.Equal(t, `{"text":"launch"}`, wire.Responses[0].Hits.Hits[0].Hierarchy.Matches[0].Source["$"])
	catalogBinding := binding
	catalogBinding.DocumentVersion = SourceRecordVersion
	catalogBinding.Fields = CanonicalDocumentFields()
	record := NewCatalogRecord(catalogBinding, ProviderS3, "acquired-r2", Permissions{Public: true})
	record.AcquisitionID = "acquisition-recipe-pin"
	catalog := CatalogStore{Client: p, Table: table + "_catalog"}
	require.NoError(t, catalog.Publish(ctx, record))
	records, err := catalog.Read(ctx, []string{binding.ID})
	require.NoError(t, err)
	require.Equal(t, record.AcquisitionID, records[binding.ID].AcquisitionID, "HTTP catalog projection must retain acquisition identity")
	// Management repair controls delegate to the ordinary durable native job API.
	source := management.Source{Table: table, Artifacts: []ArtifactBinding{artifact}}
	job, err := p.ProcessingJob(ctx, source, management.ProcessingJobRequest{Artifact: artifact.Artifact, Action: "start", Limit: 2})
	require.NoError(t, err)
	id, ok := job["job_id"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)
	_, err = p.ProcessingJob(ctx, source, management.ProcessingJobRequest{Artifact: artifact.Artifact, Action: "status", JobId: id})
	require.NoError(t, err)
	_, err = p.ProcessingJob(ctx, source, management.ProcessingJobRequest{Artifact: artifact.Artifact, Action: "advance", JobId: id})
	require.NoError(t, err)
	_, err = p.ProcessingJob(ctx, source, management.ProcessingJobRequest{Artifact: artifact.Artifact, Action: "cancel", JobId: id})
	require.NoError(t, err)

}

// Invalid input shapes must fail before model resolution or catalog publication.
func TestNativePDFInputAdmission(t *testing.T) {
	endpoint := os.Getenv("ANTFLY_SOURCES_E2E_URL")
	if endpoint == "" {
		t.Skip("set ANTFLY_SOURCES_E2E_URL to an isolated native server")
	}
	u, err := url.Parse(endpoint)
	require.NoError(t, err)
	require.True(t, u.Scheme == "http" && net.ParseIP(u.Hostname()).IsLoopback())
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	p := &Publisher{Endpoint: endpoint}
	table := fmt.Sprintf("pdf_input_admission_%d", time.Now().UnixNano())
	require.NoError(t, p.EnsureTable(ctx, table))
	require.NoError(t, p.Batch(ctx, table, map[string]any{"doc": map[string]any{"body": "https://example.invalid/source.pdf"}}, nil))
	client := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for _, sparse := range []bool{false, true} {
		name := "pdf_chunks"
		enrichment := map[string]any{"name": "pages", "kind": "embedding", "field": "text", "embedding_input": "pdf_page_images", "source_artifact_name": "links", "expected_dims": 3}
		config := map[string]any{"type": "embeddings", "dimension": 3, "embedder": map[string]any{"provider": "ollama", "model": "unavailable-fixture"}, "sources": []any{map[string]any{"artifact": "pages"}}, "enrichments": []any{map[string]any{"name": "links", "kind": "chunk", "field": "body", "chunk_size": 512}, enrichment}}
		if sparse {
			name = "pdf_sparse"
			config["sparse"] = true
			delete(config, "dimension")
			delete(enrichment, "source_artifact_name")
			delete(enrichment, "expected_dims")
			enrichment["field"] = "body"
		}
		body, err := json.Marshal(config)
		require.NoError(t, err)
		target := endpoint + "/tables/" + table + "/indexes/" + name
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		require.NoError(t, err)
		raw, err := io.ReadAll(io.LimitReader(response.Body, 4096))
		response.Body.Close()
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, response.StatusCode, "invalid %s: %s", name, raw)
		request, err = http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		require.NoError(t, err)
		response, err = client.Do(request)
		require.NoError(t, err)
		raw, err = io.ReadAll(io.LimitReader(response.Body, 4096))
		response.Body.Close()
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, response.StatusCode, "invalid index was published: %s", raw)
	}
}
