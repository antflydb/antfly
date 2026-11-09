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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antflydb/antfly/go/pkg/sources/management"
	"github.com/stretchr/testify/require"
)

func TestManagedCredentialsSurvivePublicReadback(t *testing.T) {
	base := map[string]any{"provider": "antfly", "model": "pinned", "model_identity": testVectorSpace().Revision, "retrieval": map[string]any{"query_instruction": "find evidence"}}
	expected, err := NativeVectorSpace("text", base, 3, "cosine", "text")
	require.NoError(t, err)
	for _, credential := range []string{"", "${secret:inference}", "${secret:rotated}"} {
		t.Run(credential, func(t *testing.T) {
			embedder := map[string]any{}
			for k, v := range base {
				embedder[k] = v
			}
			embedder["api_key"] = credential
			space, err := NativeVectorSpace("text", embedder, 3, "cosine", "text")
			require.NoError(t, err)
			require.Equal(t, expected, space)
			plan, stream, _, err := BuildTextProcessing(TextProcessingPreset{Name: "content", Field: "content.text", VectorSpace: &space, Embedder: embedder})
			require.NoError(t, err)
			client := &testSearchIndexClient{raw: map[string][]byte{}}
			for i, index := range plan.Indexes {
				raw, err := json.Marshal(index.Config)
				require.NoError(t, err)
				var observed map[string]any
				require.NoError(t, json.Unmarshal(raw, &observed))
				if config, ok := observed["embedder"].(map[string]any); ok {
					delete(config, "api_key")
				}
				for _, item := range observed["enrichments"].([]any) {
					delete(item.(map[string]any), "producer")
				}
				raw, err = json.Marshal(map[string]any{"config": observed})
				require.NoError(t, err)
				client.raw[index.Name] = raw
				decoded, err := decodeSearchIndex(raw)
				require.NoError(t, err)
				require.NoError(t, checkNativeStream(stream, decoded, i == 1))
			}
			require.NoError(t, ReconcileProcessing(t.Context(), client, "sources", &plan))
			require.Zero(t, client.created)
			require.NoError(t, ValidateManagedVectorSpace(space, base))
			changed := map[string]any{}
			for k, v := range base {
				changed[k] = v
			}
			changed["retrieval"] = map[string]any{"query_instruction": "different encoding"}
			require.Error(t, ValidateManagedVectorSpace(space, changed))
		})
	}
	// Credentials omitted from public comparison still require immutable private
	// recipe ownership, even when a declaration contains no opaque producer.
	config := map[string]any{"type": "embeddings", "embedder": map[string]any{"api_key": "${secret:inference}"}}
	require.Error(t, ValidateProcessing(&ProcessingDeclaration{Indexes: []management.NativeIndexDeclaration{{Name: "mutable", Config: config}}}))
	require.NoError(t, ValidateProcessing(&ProcessingDeclaration{Indexes: []management.NativeIndexDeclaration{{Name: NativeIndexName("private", config), Config: config}}}))
}

func TestParentFullTextRejectsArtifactSources(t *testing.T) {
	stream := SearchStream{Name: "original", Field: "content.text", Retrieval: &RetrievalBindings{FullText: &management.FullTextRetrieval{Index: "text"}}}
	for _, sources := range []any{nil, []any{map[string]any{"artifact": "summary", "field": "text"}}, []any{map[string]any{"artifact": "summary"}, map[string]any{"artifact": "caption"}}} {
		config := map[string]any{"type": "full_text", "field": "content.text"}
		if sources != nil {
			config["sources"] = sources
		}
		raw, err := json.Marshal(map[string]any{"config": config})
		require.NoError(t, err)
		index, err := decodeSearchIndex(raw)
		require.NoError(t, err)
		if sources == nil {
			require.NoError(t, checkNativeStream(stream, index, false))
		} else {
			require.Error(t, checkNativeStream(stream, index, false))
		}
	}
}

func TestSemanticMediaTypedTemplateLineage(t *testing.T) {
	for _, template := range []string{"{{remoteMedia url=content.resource.uri}}", "{{media url=content.resource.uri}}", "{{ remoteMedia  url=content.resource.uri }}"} {
		t.Run(template, func(t *testing.T) {
			stream := testMediaStream()
			index, err := decodeSearchIndex(testNativeIndex(stream, true))
			require.NoError(t, err)
			nativeEnrichments(index.Config)["image_dense"]["template"] = template
			require.NoError(t, checkNativeStream(stream, index, true))
		})
	}
	for _, template := range []string{"{{content.resource.uri}}", "{{{content.resource.uri}}}", "{{remoteMedia url=content.resource.uri mode=\"extract\"}}", "{{media url=other.uri}}", "{{remoteMedia url=\"content.resource.uri\"}}", "{{media url=content.resource.uri}}{{other}}", "", "{{{", "{{content.resource.uri}}}", "{{{content.resource.uri}}", "{{other.uri}}", "{{helper content.resource.uri}}", "{{#if content}}{{content.resource.uri}}{{/if}}", "{{content.resource.uri}}{{other}}"} {
		stream := testMediaStream()
		index, err := decodeSearchIndex(testNativeIndex(stream, true))
		require.NoError(t, err)
		nativeEnrichments(index.Config)["image_dense"]["template"] = template
		require.Error(t, checkNativeStream(stream, index, true), template)
	}
}

func TestSemanticMediaInputModesAndModalities(t *testing.T) {
	for _, tc := range []struct {
		name, field, template, input, modality, upstream string
		valid                                            bool
	}{
		{name: "raw_uri_is_text", field: "content.resource.uri", modality: "image"},
		{name: "plain_template_is_text", template: "{{content.resource.uri}}", modality: "audio"},
		{name: "typed_image", template: "{{media url=content.resource.uri}}", modality: "image", valid: true},
		{name: "typed_audio", template: "{{remoteMedia url=content.resource.uri}}", modality: "audio", valid: true},
		{name: "typed_multimodal", template: "{{media url=content.resource.uri}}", modality: "multimodal", valid: true},
		{name: "media_cannot_claim_text", template: "{{media url=content.resource.uri}}", modality: "text"},
		{name: "pdf_literal_dotted_field_is_not_nested", field: "content.resource.uri", input: "pdf_page_images", modality: "image"},
		{name: "pdf_escaped_uri_template", template: "{{content.resource.uri}}", input: "pdf_page_images", modality: "image"},
		{name: "pdf_uri_template", template: "{{{content.resource.uri}}}", input: "pdf_page_images", modality: "image", valid: true},
		{name: "pdf_wrong_modality", field: "content.resource.uri", input: "pdf_page_images", modality: "audio"},
		{name: "pdf_wrong_field", field: "other.uri", input: "pdf_page_images", modality: "image"},
		{name: "pdf_typed_template_is_not_uri", template: "{{media url=content.resource.uri}}", input: "pdf_page_images", modality: "image"},
		{name: "unknown_input", template: "{{media url=content.resource.uri}}", input: "future_mode", modality: "image"},
		{name: "media_cannot_claim_artifact_ancestor", template: "{{media url=content.resource.uri}}", upstream: "chunks", modality: "image"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := testMediaStream()
			stream.Retrieval.Semantic.VectorSpace.DocumentModality = management.VectorSpaceDocumentModality(tc.modality)
			index, err := decodeSearchIndex(testNativeIndex(stream, true))
			require.NoError(t, err)
			e := nativeEnrichments(index.Config)["image_dense"]
			e["field"], e["template"], e["embedding_input"], e["source_artifact_name"] = tc.field, tc.template, tc.input, tc.upstream
			err = checkNativeStream(stream, index, true)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestNativeTextLineageUsesLiteralFieldsAndNestedTemplates(t *testing.T) {
	for _, tc := range []struct {
		name, field, template, representation string
		valid                                 bool
	}{
		{name: "literal_root", field: "body", representation: "body", valid: true},
		{name: "literal_dotted_is_not_nested", field: "content.text", representation: "content.text"},
		{name: "raw_nested", template: "{{{content.text}}}", representation: "content.text", valid: true},
		{name: "conflicting_field_and_template", field: "body", template: "{{{content.text}}}", representation: "content.text"},
		{name: "wrong_nested", template: "{{{other.text}}}", representation: "content.text"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := testMediaStream()
			stream.ResourceField = ""
			stream.Field = tc.representation
			stream.Retrieval.Semantic.MemberField = tc.representation
			stream.Retrieval.Semantic.VectorSpace.DocumentModality = "text"
			index, err := decodeSearchIndex(testNativeIndex(stream, true))
			require.NoError(t, err)
			e := nativeEnrichments(index.Config)["image_dense"]
			e["field"], e["template"] = tc.field, tc.template
			if tc.valid {
				require.NoError(t, checkNativeStream(stream, index, true))
			} else {
				require.Error(t, checkNativeStream(stream, index, true))
			}
		})
	}
}

type concurrentSearchFixture struct {
	calls   atomic.Int32
	active  atomic.Int32
	peak    atomic.Int32
	delay   time.Duration
	raw     []byte
	blocked string
	mu      sync.Mutex
	names   map[string]int
}

func (c *concurrentSearchFixture) InspectSearchIndex(ctx context.Context, _, name string) ([]byte, error) {
	c.calls.Add(1)
	active := c.active.Add(1)
	defer c.active.Add(-1)
	for peak := c.peak.Load(); active > peak; peak = c.peak.Load() {
		if c.peak.CompareAndSwap(peak, active) {
			break
		}
	}
	c.mu.Lock()
	if c.names == nil {
		c.names = map[string]int{}
	}
	c.names[name]++
	c.mu.Unlock()
	if name == c.blocked {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	timer := time.NewTimer(c.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return c.raw, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (*concurrentSearchFixture) CreateSearchIndex(context.Context, string, string, map[string]any) error {
	return fmt.Errorf("unexpected write")
}
func readinessBinding(n int) Binding {
	binding := Binding{Table: "sources", SearchVersion: 2}
	for i := 0; i < n; i++ {
		binding.Streams = append(binding.Streams, SearchStream{Name: fmt.Sprint(i), Field: "content.text", Retrieval: &RetrievalBindings{FullText: &management.FullTextRetrieval{Index: fmt.Sprint(i)}}})
	}
	return binding
}
func readyTextIndex() []byte {
	return []byte(`{"config":{"type":"full_text","field":"content.text"},"status":{"readiness":{"complete":true,"queryable":true}}}`)
}
func TestReadinessInspectsHealthyIndexesConcurrently(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	client := &concurrentSearchFixture{delay: 100 * time.Millisecond, raw: readyTextIndex(), blocked: "none"}
	binding := readinessBinding(32)
	duplicate := binding.Streams[0]
	duplicate.Name = "duplicate"
	binding.Streams = append(binding.Streams, duplicate)
	status := ObserveSearch(ctx, client, binding)
	for _, stream := range status.Streams {
		require.Equal(t, management.RepresentationStatusStateReady, stream.State, stream.Name)
	}
	require.EqualValues(t, 32, client.calls.Load())
	require.Greater(t, client.peak.Load(), int32(1))
	require.LessOrEqual(t, client.peak.Load(), int32(8))
	require.Equal(t, 1, client.names["0"])
}
func TestReadinessTimeoutDoesNotBlockHealthyIndexes(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	client := &concurrentSearchFixture{delay: time.Millisecond, raw: readyTextIndex(), blocked: "0"}
	status := ObserveSearch(ctx, client, readinessBinding(32))
	require.Equal(t, management.RepresentationStatusStateUnsupported, status.Streams[0].State)
	require.Equal(t, "native_observation_timeout", status.Streams[0].Reason)
	for _, stream := range status.Streams[1:] {
		require.Equal(t, management.RepresentationStatusStateReady, stream.State, stream.Name)
	}
	require.Zero(t, client.active.Load())
}
func TestReadinessBoundsAggregateMetadata(t *testing.T) {
	raw := append(readyTextIndex(), []byte(strings.Repeat(" ", 2<<20))...)
	client := &concurrentSearchFixture{raw: raw, blocked: "none"}
	status := ObserveSearch(t.Context(), client, readinessBinding(64))
	limited := 0
	for _, stream := range status.Streams {
		if stream.Reason == "native_metadata_budget_exceeded" {
			limited++
			require.Equal(t, management.RepresentationStatusStateUnsupported, stream.State)
		}
	}
	require.Greater(t, limited, 0)
	require.LessOrEqual(t, client.calls.Load(), int32(24))
	require.Zero(t, client.active.Load())
}
