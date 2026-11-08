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
	"github.com/antflydb/antfly/go/pkg/sources/management"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func testVectorSpace() VectorSpace {
	return VectorSpace{Id: "images-v1", Dimension: 3, Metric: "cosine", Model: "pinned-model@revision", Revision: "revision", QueryPreprocessing: "text:v1", DocumentPreprocessing: "image:v1", QueryModality: "text", DocumentModality: "image"}
}
func testMediaStream() SearchStream {
	return SearchStream{Name: "original", ResourceField: "content.resource", Retrieval: &RetrievalBindings{Semantic: &management.SemanticRetrieval{Index: "image_vectors", MemberArtifacts: []string{"image_dense"}, VectorSpace: testVectorSpace(), QueryInputs: []management.SemanticRetrievalQueryInputs{"vector"}, Default: true}}}
}
func TestSearchStreamContracts(t *testing.T) {
	media := testMediaStream()
	require.NoError(t, ValidateSearchBindings(2, []SearchStream{media}, nil))
	require.Error(t, ValidateSearchBindings(1, []SearchStream{media}, nil))
	for _, mutate := range []func(*SearchStream){
		func(s *SearchStream) { s.Retrieval = nil },
		func(s *SearchStream) { s.ResourceField = "" },
		func(s *SearchStream) { s.Default = true },
		func(s *SearchStream) { s.Retrieval.FullText = &management.FullTextRetrieval{Index: "text"} },
		func(s *SearchStream) {
			s.Retrieval.Semantic.QueryInputs = []management.SemanticRetrievalQueryInputs{"vector", "vector"}
		},
		func(s *SearchStream) { s.Retrieval.Semantic.VectorSpace.Revision = "" },
	} {
		stream := testMediaStream()
		mutate(&stream)
		require.Error(t, ValidateSearchBindings(2, []SearchStream{stream}, nil))
	}
	legacy := DefaultTextStreams()
	require.NoError(t, ValidateTextStreams(legacy))
	require.NoError(t, ValidateSearchBindings(0, legacy, nil))
	require.Error(t, ValidateSearchBindings(3, nil, nil))
	_, ok := LegacyStream(media)
	require.False(t, ok)
	exact := DefaultSearchStreams(2)
	adapted, ok := LegacyStream(exact[0])
	require.True(t, ok)
	require.Equal(t, legacy[0], adapted)
	require.NoError(t, ValidateSearchBindings(2, nil, []ArtifactBinding{{Name: "labels", Artifact: "labels", ContentType: "application/json"}}))
}
func TestSearchQueryCompatibilityAndBudgets(t *testing.T) {
	stream := testMediaStream()
	binding := Binding{SearchVersion: 2, Table: "shared", Streams: []SearchStream{stream}, Filter: SourceFilter("private-source"), Fields: CanonicalDocumentFields()}
	space := testVectorSpace()
	request := SearchRequest{Stream: "original", Mode: "semantic", Vector: []float32{1, 0, 0}, VectorSpace: &space}
	raw, err := BuildSearchQuery(binding, request)
	require.NoError(t, err)
	var body map[string]any
	require.NoError(t, json.Unmarshal(raw, &body))
	require.Contains(t, string(raw), "private-source")
	require.Equal(t, "source", body["hierarchy"].(map[string]any)["group_by"].(map[string]any)["level"])
	require.Contains(t, string(raw), "content.resource")
	incompatible := space
	incompatible.QueryPreprocessing = "other"
	request.VectorSpace = &incompatible
	_, err = BuildSearchQuery(binding, request)
	require.Error(t, err)
	request.VectorSpace = &space
	request.Text = "not advertised"
	_, err = BuildSearchQuery(binding, request)
	require.Error(t, err)
	request.Text = ""
	request.Limit = 101
	_, err = BuildSearchQuery(binding, request)
	require.Error(t, err)
	request.Limit = 10
	request.Vector = nil
	request.Text = "hello"
	_, err = BuildSearchQuery(binding, request)
	require.Error(t, err)
	require.NotEqual(t, VectorSpaceIdentity(space), VectorSpaceIdentity(incompatible))
}

type testSearchIndexClient struct {
	raw     map[string][]byte
	calls   map[string]int
	created int
}

func (c *testSearchIndexClient) InspectSearchIndex(_ context.Context, _, index string) ([]byte, error) {
	if c.calls == nil {
		c.calls = map[string]int{}
	}
	c.calls[index]++
	raw, ok := c.raw[index]
	if !ok {
		return nil, &PublishHTTPError{Status: 404}
	}
	return raw, nil
}
func (c *testSearchIndexClient) CreateSearchIndex(_ context.Context, _, _ string, _ map[string]any) error {
	c.created++
	return nil
}
func testNativeIndex(stream SearchStream, complete bool) []byte {
	space := stream.Retrieval.Semantic.VectorSpace
	raw, _ := json.Marshal(map[string]any{"config": map[string]any{"type": "embeddings", "dimension": 3, "distance_metric": "cosine", "sources": []any{map[string]any{"artifact": "image_dense"}}, "enrichments": []any{map[string]any{"name": "image_dense", "kind": "embedding", "template": "{{content.resource.uri}}", "vector_space": VectorSpaceIdentity(space)}}}, "status": map[string]any{"readiness": map[string]any{"queryable": true, "complete": complete}}})
	return raw
}
func TestSearchReadinessVerifiesNativeRecipeAndCachesIndexes(t *testing.T) {
	stream := testMediaStream()
	other := stream
	other.Name = "another-view"
	client := &testSearchIndexClient{raw: map[string][]byte{"image_vectors": testNativeIndex(stream, true)}}
	binding := Binding{Table: "sources", Streams: []SearchStream{stream, other}, Artifacts: []ArtifactBinding{{Name: "facts", Artifact: "facts", ContentType: "application/json"}}}
	status := ObserveSearch(t.Context(), client, binding)
	require.Equal(t, management.RepresentationStatusState("ready"), status.Streams[0].State)
	require.Equal(t, 1, client.calls["image_vectors"])
	require.Equal(t, management.RepresentationStatusState("unsupported"), status.Artifacts[0].State)
	binding.Streams[0].Retrieval.Semantic.VectorSpace.Revision = "wrong"
	status = ObserveSearch(t.Context(), client, binding)
	require.Equal(t, management.RepresentationStatusState("unsupported"), status.Streams[0].State)
	client = &testSearchIndexClient{raw: map[string][]byte{"image_vectors": testNativeIndex(stream, false)}}
	binding.Streams = []SearchStream{stream}
	status = ObserveSearch(t.Context(), client, binding)
	require.Equal(t, management.RepresentationStatusState("building"), status.Streams[0].State)
}
func TestProcessingDoesNotOverwriteSharedIndexes(t *testing.T) {
	client := &testSearchIndexClient{raw: map[string][]byte{"text": []byte(`{"config":{"type":"full_text","field":"other"}}`)}}
	plan := &ProcessingDeclaration{Indexes: []management.NativeIndexDeclaration{{Name: "text", Config: map[string]any{"type": "full_text", "field": "content.text"}}}}
	require.Error(t, ReconcileProcessing(t.Context(), client, "shared", plan))
	require.Zero(t, client.created)
	delete(client.raw, "text")
	require.NoError(t, ReconcileProcessing(t.Context(), client, "shared", plan))
	require.Equal(t, 1, client.created)
}

func TestProcessingPresetsAreImmutableAndNative(t *testing.T) {
	plan, stream, artifact, err := BuildTextProcessing(TextProcessingPreset{Name: "summary", Field: "content.text", Purpose: "summary", Generated: true, Producer: map[string]any{"type": "generator", "config": map[string]any{"prompt": "Summarize"}}})
	require.NoError(t, err)
	require.NoError(t, ValidateSearchBindings(2, []SearchStream{stream}, []ArtifactBinding{artifact}))
	require.Contains(t, plan.Indexes[0].Name, "summary_text-")
	changed, _, _, err := BuildTextProcessing(TextProcessingPreset{Name: "summary", Field: "content.text", Purpose: "summary", Producer: map[string]any{"type": "generator", "config": map[string]any{"prompt": "Different recipe"}}})
	require.NoError(t, err)
	require.NotEqual(t, plan.Indexes[0].Name, changed.Indexes[0].Name)
	require.NotEqual(t, plan.Indexes[0].Config["enrichments"].([]any)[0].(map[string]any)["name"], changed.Indexes[0].Config["enrichments"].([]any)[0].(map[string]any)["name"], "changed producer recipes must not reuse mutable artifact identities")
	require.True(t, containsNativeConfig(map[string]any{"type": "full_text", "enrichments": []any{map[string]any{"name": "asset", "kind": "asset"}}}, map[string]any{"type": "full_text", "enrichments": []any{map[string]any{"name": "asset", "kind": "asset", "producer": map[string]any{"type": "copy"}}}}))
	external, err := ExternalVectorIndex("external", testVectorSpace())
	require.NoError(t, err)
	require.Equal(t, VectorSpaceIdentity(testVectorSpace()), external.Config["external_vector_space"])
	parent := map[string]any{"source": map[string]any{"revision": "r1"}, "metadata": map[string]any{"keep": "yes"}}
	output, err := WithExternalVector(parent, "external", testVectorSpace(), []float32{1, 0, 0})
	require.NoError(t, err)
	require.NotContains(t, parent, "_embeddings")
	require.Equal(t, "yes", output["metadata"].(map[string]any)["keep"])
}

func TestSearchConsistencyIsPerOperation(t *testing.T) {
	value, err := SearchConsistency([]byte(`{"responses":[{"status":200,"remote_snapshot":"pinned"}]}`))
	require.NoError(t, err)
	require.Equal(t, "retained_snapshot", value)
	value, err = SearchConsistency([]byte(`{"responses":[{"status":200,"live_read":true}]}`))
	require.NoError(t, err)
	require.Equal(t, "live_read", value)
	value, err = SearchConsistency([]byte(`{"responses":[{"status":200}]}`))
	require.NoError(t, err)
	require.Equal(t, "unretained", value)
}

func TestProcessingChangesDoNotInvalidateAcquisition(t *testing.T) {
	source := Source{ID: "11111111-1111-4111-8111-111111111111", Table: "sources", Connection: "docs", Config: json.RawMessage(`{"bucket":"data"}`)}
	connection := Connection{Provider: ProviderS3}
	before, err := sourceConfigurationHash(source, connection)
	mediaBefore := sourceMediaBinding(source)
	require.NoError(t, err)
	source.SearchVersion = 2
	source.Streams = []SearchStream{testMediaStream()}
	source.Artifacts = []ArtifactBinding{{Name: "facts", Artifact: "facts", ContentType: "application/json"}}
	source.Processing = &ProcessingDeclaration{Indexes: []management.NativeIndexDeclaration{{Name: "new-index", Config: map[string]any{"type": "full_text", "field": "content.text"}}}}
	after, err := sourceConfigurationHash(source, connection)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, mediaBefore, sourceMediaBinding(source), "processing must preserve revision-pinned original resources")
}

func TestHostedSearchReceiptsAreNotCapabilitiesAndMustStayPinned(t *testing.T) {
	scope := &searchScope{source: "selected", access: "policy-one", hosted: true}
	first := http.Header{"X-Antfly-Subject": []string{"user:alice"}, "X-Antfly-Source-Access": []string{"policy-one"}, "X-Antfly-Source-Publication": []string{"publication-one"}}
	require.NoError(t, scope.observe(first))
	request := http.Header{}
	scope.headers(request)
	require.Equal(t, "selected", request.Get("X-Antfly-Source-ID"))
	require.Equal(t, "publication-one", request.Get("X-Antfly-Source-Publication"))
	changed := first.Clone()
	changed.Set("X-Antfly-Subject", "user:bob")
	require.Error(t, scope.observe(changed))
	changed = first.Clone()
	changed.Set("X-Antfly-Source-Publication", "publication-two")
	require.Error(t, scope.observe(changed))
	require.Error(t, scope.observe(http.Header{}))
	local := &searchScope{source: "selected"}
	require.NoError(t, local.observe(http.Header{}))
}

func TestPublisherSearchPinsGatewayReceiptsAndReportsLiveConsistency(t *testing.T) {
	space := testVectorSpace()
	stream := testMediaStream()
	stream.Retrieval.Semantic.External = true
	stream.Retrieval.Semantic.MemberArtifacts = nil
	expires := time.Now().Add(time.Minute).Unix()
	binding := Binding{ID: "11111111-1111-4111-8111-111111111111", SearchVersion: 2, Table: "sources", Status: "searchable", ValidUntil: &expires, AccessID: "policy-one", Filter: SourceFilter("11111111-1111-4111-8111-111111111111"), Streams: []SearchStream{stream}}
	var changed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, binding.ID, r.Header.Get("X-Antfly-Source-ID"))
		require.Equal(t, "policy-one", r.Header.Get("X-Antfly-Source-Access"))
		w.Header().Set("X-Antfly-Subject", "user:alice")
		w.Header().Set("X-Antfly-Source-Access", "policy-one")
		publication := "publication-one"
		if changed.Load() && r.Method == "POST" {
			publication = "publication-two"
		}
		w.Header().Set("X-Antfly-Source-Publication", publication)
		if r.Method == "GET" {
			_ = json.NewEncoder(w).Encode(map[string]any{"config": map[string]any{"type": "embeddings", "external": true, "external_vector_space": VectorSpaceIdentity(space), "dimension": 3, "distance_metric": "cosine"}, "status": map[string]any{"readiness": map[string]any{"queryable": true, "complete": true}}})
			return
		}
		require.Equal(t, "publication-one", r.Header.Get("X-Antfly-Source-Publication"))
		_, _ = w.Write([]byte(`{"responses":[{"status":200,"live_read":true,"hits":{"hits":[]}}]}`))
	}))
	defer server.Close()
	publisher := &Publisher{Endpoint: server.URL}
	request := SearchRequest{Stream: "original", Mode: "semantic", Vector: []float32{0, 1, 0}, VectorSpace: &space}
	raw, err := publisher.Search(t.Context(), binding, request)
	require.NoError(t, err)
	consistency, err := SearchConsistency(raw)
	require.NoError(t, err)
	require.Equal(t, "live_read", consistency)
	request.RequireSnapshot = true
	_, err = publisher.Search(t.Context(), binding, request)
	require.Error(t, err)
	request.RequireSnapshot = false
	changed.Store(true)
	_, err = publisher.Search(t.Context(), binding, request)
	require.Error(t, err)
}

func TestRefreshSearchDiscoveryDoesNotAcquireOrExtendAccess(t *testing.T) {
	runner, connector, _ := runnerFixture(t)
	require.NoError(t, runner.Sync(t.Context()))
	var before Catalog
	require.NoError(t, loadJSON(runner.CatalogPath, &before, 32<<20))
	runner.Profile.Sources[0].SearchVersion = 2
	runner.Profile.Sources[0].Streams = DefaultSearchStreams(2)
	// No provider method may run during declaration publication.
	runner.Factory = func(context.Context, Connection) (Connector, error) {
		t.Fatal("unexpected provider acquisition")
		return connector, nil
	}
	require.NoError(t, runner.RefreshSearchDiscovery(t.Context(), testSourceID))
	var after Catalog
	require.NoError(t, loadJSON(runner.CatalogPath, &after, 32<<20))
	require.Equal(t, 2, after.Sources[0].SearchVersion)
	require.Equal(t, before.Sources[0].ValidUntil, after.Sources[0].ValidUntil)
	require.Equal(t, before.Sources[0].PublishedRevision, after.Sources[0].PublishedRevision)
	// Changed acquisition cannot borrow the previous generation's publication.
	runner.Profile.Sources[0].Permissions.Public = true
	runner.Profile.Sources[0].Streams[0].Name = "unpublished"
	require.NoError(t, runner.RefreshSearchDiscovery(t.Context(), testSourceID))
	require.NoError(t, loadJSON(runner.CatalogPath, &after, 32<<20))
	require.NotEqual(t, "unpublished", after.Sources[0].Streams[0].Name)
}
