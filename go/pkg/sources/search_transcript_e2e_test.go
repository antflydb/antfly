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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antflydb/antfly/go/pkg/sources/management"
	"github.com/stretchr/testify/require"
)

// Exercise the actual native transcription/unit publication path with a small
// deterministic remote-provider fixture. No model download or relevance claim.
func TestNativeTranscriptSearchEvidence(t *testing.T) {
	endpoint := os.Getenv("ANTFLY_SOURCES_E2E_URL")
	if endpoint == "" {
		t.Skip("set ANTFLY_SOURCES_E2E_URL")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	wav := make([]byte, 44+45*32000)
	copy(wav, "RIFF")
	binary.LittleEndian.PutUint32(wav[4:], uint32(len(wav)-8))
	copy(wav[8:], "WAVEfmt ")
	binary.LittleEndian.PutUint32(wav[16:], 16)
	binary.LittleEndian.PutUint16(wav[20:], 1)
	binary.LittleEndian.PutUint16(wav[22:], 1)
	binary.LittleEndian.PutUint32(wav[24:], 16000)
	binary.LittleEndian.PutUint32(wav[28:], 32000)
	binary.LittleEndian.PutUint16(wav[32:], 2)
	binary.LittleEndian.PutUint16(wav[34:], 16)
	copy(wav[36:], "data")
	binary.LittleEndian.PutUint32(wav[40:], uint32(len(wav)-44))
	var transcriptions atomic.Int32
	fixture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/recording.wav":
			w.Header().Set("Content-Type", "audio/wav")
			_, _ = w.Write(wav)
		case "/v1/audio/transcriptions":
			if _, err := io.Copy(io.Discard, r.Body); err != nil {
				http.Error(w, "invalid audio request", 400)
				return
			}
			transcriptions.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"text":"intro words. launch at dawn.","language":"en","duration":45,"segments":[{"start":0,"end":1,"text":"intro words."},{"start":42,"end":45,"text":"launch at dawn."}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer fixture.Close()
	p := &Publisher{Endpoint: endpoint}
	table := fmt.Sprintf("source_transcript_%d", time.Now().UnixNano())
	require.NoError(t, p.EnsureTable(ctx, table))
	preview, err := PreviewProcessing(management.ProcessingPreset{Name: "speech", Kind: "transcription", ProducerConfig: map[string]any{"provider": "openai", "model": "whisper-1", "base_url": fixture.URL + "/v1"}})
	require.NoError(t, err)
	chunkName := "speech_passages"
	preview.Processing.Indexes = append(preview.Processing.Indexes, management.NativeIndexDeclaration{Name: "speech_passages_text", Config: map[string]any{"type": "full_text", "sources": []any{map[string]any{"artifact": chunkName, "field": "text"}}, "enrichments": []any{map[string]any{"kind": "chunk", "name": chunkName, "source_artifact_name": preview.Stream.Artifact, "field": "text", "chunk_size": 16, "chunk_overlap": 8}}}})
	require.NoError(t, ReconcileProcessing(ctx, p, table, &preview.Processing))
	doc := map[string]any{"source_id": testSourceID, "doc_type": "source_item", "source": map[string]any{"uri": "local:///recording.wav", "path": "recording.wav", "revision": "r1"}, "content": map[string]any{"resource": map[string]any{"type": "source", "uri": fixture.URL + "/recording.wav", "version": "r1", "mime_type": "audio/wav"}}, "permissions": map[string]any{"public": true}, "metadata": map[string]any{}}
	_, err = p.call(ctx, "POST", tablePath(table)+"/batch", map[string]any{"inserts": map[string]any{"recording": doc}, "sync_level": "write"})
	require.NoError(t, err)
	passages := SearchStream{Name: "passages", Artifact: chunkName, Field: "text", LocationField: "provenance", Boundary: "unit", Retrieval: &RetrievalBindings{FullText: &management.FullTextRetrieval{Index: "speech_passages_text"}}}
	until := time.Now().Add(2 * time.Minute).Unix()
	binding := Binding{ID: testSourceID, Table: table, SearchVersion: 2, Fields: CanonicalDocumentFields(), Streams: []SearchStream{preview.Stream, passages}, Filter: SourceFilter(testSourceID), Status: "searchable", ValidUntil: &until}
	var observed management.SearchStatus
	require.Eventually(t, func() bool {
		observed = ObserveSearch(ctx, p, binding)
		for _, s := range observed.Streams {
			if s.State != "ready" {
				return false
			}
		}
		return true
	}, 60*time.Second, 100*time.Millisecond, "readiness: %v", observed)
	for _, name := range []string{"speech", "passages"} {
		raw, err := p.Search(ctx, binding, SearchRequest{Stream: name, Mode: "full_text", Text: "launch"})
		require.NoError(t, err)
		var response struct {
			Responses []struct {
				Hits struct {
					Hits []struct {
						Hierarchy struct {
							Matches []struct {
								Source map[string]any `json:"_source"`
							} `json:"matches"`
						} `json:"hierarchy"`
					} `json:"hits"`
				} `json:"hits"`
			} `json:"responses"`
		}
		require.NoError(t, json.Unmarshal(raw, &response))
		require.Len(t, response.Responses, 1)
		require.NotEmpty(t, response.Responses[0].Hits.Hits, string(raw))
		matches := response.Responses[0].Hits.Hits[0].Hierarchy.Matches
		require.NotEmpty(t, matches)
		evidence, _ := json.Marshal(matches)
		require.Contains(t, string(evidence), "42000", name)
		require.Contains(t, string(evidence), "45000", name)
		require.Contains(t, string(evidence), "provenance", name)
		if name == "passages" {
			require.True(t, strings.Contains(string(evidence), "start_time_ms"))
			require.Contains(t, string(evidence), "transcript_spans")
		}
	}
	require.GreaterOrEqual(t, transcriptions.Load(), int32(1))
	// Reconciliation observes the existing recipe; it must not invoke STT again.
	before := transcriptions.Load()
	require.NoError(t, ReconcileProcessing(ctx, p, table, &preview.Processing))
	require.Equal(t, before, transcriptions.Load())
	_, err = p.call(ctx, "POST", tablePath(table)+"/batch", map[string]any{"deletes": []string{"recording"}, "sync_level": "full_index"})
	require.NoError(t, err)
	// Removing a recording also withdraws its units and derived passages.
	require.Eventually(t, func() bool {
		for _, name := range []string{"speech", "passages"} {
			raw, err := p.Search(ctx, binding, SearchRequest{Stream: name, Mode: "full_text", Text: "launch"})
			if err != nil {
				return false
			}
			var response struct {
				Responses []struct {
					Hits struct {
						Hits []json.RawMessage `json:"hits"`
					} `json:"hits"`
				} `json:"responses"`
			}
			if json.Unmarshal(raw, &response) != nil || len(response.Responses) != 1 || len(response.Responses[0].Hits.Hits) != 0 {
				return false
			}
		}
		return true
	}, 20*time.Second, 100*time.Millisecond)
}
