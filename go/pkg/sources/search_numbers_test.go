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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSearchPreservesOpaqueNativeEvidence(t *testing.T) {
	evidence := `{"integer":9007199254740993,"unsigned":18446744073709551615,"negative":-9007199254740993,"decimal":1.0000000000000001,"nested":[{"counter":9007199254740993}]}`
	for _, wrapped := range []bool{false, true} {
		t.Run(fmt.Sprint(wrapped), func(t *testing.T) {
			stream := testMediaStream()
			native := `{"status":200,"hits":{"hits":[{"_id":"doc","_score":1.0000000000000001,"_source":` + evidence + `}]},"opaque_future_field":` + evidence + `}`
			if wrapped {
				native = `{"responses":[` + native + `],"opaque_envelope_field":` + evidence + `}`
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					_, _ = w.Write(testNativeIndex(stream, true))
					return
				}
				_, _ = w.Write([]byte(native))
			}))
			defer server.Close()
			until := time.Now().Add(time.Minute).Unix()
			binding := Binding{ID: testSourceID, Table: "docs", SearchVersion: 2, Streams: []SearchStream{stream}, Filter: SourceFilter(testSourceID), Status: "searchable", ValidUntil: &until}
			p := Publisher{Endpoint: server.URL}
			raw, err := p.Search(context.Background(), binding, SearchRequest{Mode: "semantic", Stream: stream.Name, Vector: []float32{1, 0, 0}, VectorSpace: &stream.Retrieval.Semantic.VectorSpace})
			require.NoError(t, err)
			var output map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &output))
			require.NotEmpty(t, output["source_search"])
			delete(output, "source_search")
			restored, err := json.Marshal(output)
			require.NoError(t, err)
			// Raw numeric lexemes and opaque fields survive both supported envelopes.
			var expected map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(native), &expected))
			want, err := json.Marshal(expected)
			require.NoError(t, err)
			require.Equal(t, string(want), string(restored))
		})
	}
}

func TestExternalVectorPreservesParentNumbersAndIndependence(t *testing.T) {
	metadata := map[string]any{"integer": uint64(9007199254740993), "unsigned": uint64(18446744073709551615), "decimal": json.Number("1.0000000000000001"), "embedding_spaces": map[string]any{"other": "kept"}, "nested": []any{map[string]any{"counter": int64(-9007199254740993)}}}
	parent := map[string]any{"source": map[string]any{"revision": "r1"}, "metadata": metadata, "_embeddings": map[string]any{"other": []float32{1, 0, 0}}}
	before, err := json.Marshal(parent)
	require.NoError(t, err)
	result, err := WithExternalVector(parent, "images", testVectorSpace(), []float32{1, 0, 0})
	require.NoError(t, err)
	raw, err := json.Marshal(result)
	require.NoError(t, err)
	for _, literal := range []string{"9007199254740993", "18446744073709551615", "1.0000000000000001", "-9007199254740993"} {
		require.Contains(t, string(raw), literal)
	}
	require.Contains(t, string(raw), `"other":"kept"`)
	result["source"].(map[string]any)["revision"] = "changed"
	result["metadata"].(map[string]any)["nested"].([]any)[0].(map[string]any)["counter"] = 0
	after, err := json.Marshal(parent)
	require.NoError(t, err)
	require.True(t, bytes.Equal(before, after), "adding vectors or editing returned data must not mutate the authored parent")
}

func TestSearchRejectsMalformedNativeEvidence(t *testing.T) {
	for _, body := range []string{`{"hits":null}`, `{"hits":[]}`, `{"hits":"not-an-object"}`, `{"responses":null}`, `{"responses":[null]}`, `{"responses":[{"hits":{}},{"hits":{}}]}`} {
		t.Run(body, func(t *testing.T) {
			stream := testMediaStream()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					_, _ = w.Write(testNativeIndex(stream, true))
					return
				}
				_, _ = w.Write([]byte(body))
			}))
			defer server.Close()
			until := time.Now().Add(time.Minute).Unix()
			binding := Binding{ID: testSourceID, Table: "docs", SearchVersion: 2, Streams: []SearchStream{stream}, Filter: SourceFilter(testSourceID), Status: "searchable", ValidUntil: &until}
			p := Publisher{Endpoint: server.URL}
			_, err := p.Search(context.Background(), binding, SearchRequest{Mode: "semantic", Stream: stream.Name, Vector: []float32{1, 0, 0}, VectorSpace: &stream.Retrieval.Semantic.VectorSpace})
			require.Error(t, err)
		})
	}
}
