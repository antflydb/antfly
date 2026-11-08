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
	"encoding/json"
	"fmt"
	"github.com/antflydb/antfly/go/pkg/sources/management"
	"github.com/stretchr/testify/require"
	"net"
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
	_, err = p.call(ctx, "POST", tablePath(table)+"/batch", map[string]any{"inserts": parents, "sync_level": "write"})
	require.NoError(t, err)
	valid := time.Now().Add(5 * time.Minute).Unix()
	binding := Binding{ID: testSourceID, Table: table, SearchVersion: 2, Streams: []SearchStream{text, original}, Artifacts: []ArtifactBinding{artifact}, Filter: SourceFilter(testSourceID), Status: "searchable", ValidUntil: &valid}
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
