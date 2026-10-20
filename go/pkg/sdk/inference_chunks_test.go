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

package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
	"github.com/stretchr/testify/require"
)

func TestChunkEmbeddingStreamRequiresCoverage(t *testing.T) {
	for _, terminal := range []bool{true, false} {
		t.Run(fmt.Sprint(terminal), func(t *testing.T) {
			plan := strings.Repeat("1", 64)
			item := map[string]any{"descriptor": map[string]any{"id": plan + ":0", "ordinal": 0, "coordinates": map[string]any{"kind": "text", "start_byte": 0, "end_byte": 7}, "input_tokens": 4}, "mime_type": "text/plain", "text": "hé🐜", "embedding": []float32{.6, .8}}
			coverage := map[string]any{"object": "chunk.embedding.list", "status": "partial", "data": []any{}, "source_fingerprint": strings.Repeat("2", 64), "source_version": "v1", "plan_id": plan, "model": "model", "model_identity": strings.Repeat("3", 64), "model_revision": strings.Repeat("4", 64), "task_type": "RETRIEVAL_DOCUMENT", "dimensions": 2, "policy": map[string]any{}, "start_ordinal": 0, "covered_through_ordinal": 1, "next_continuation": plan + ":1", "gaps": []any{}}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "application/x-ndjson", r.Header.Get("Accept"))
				w.Header().Set("Content-Type", "application/x-ndjson")
				encoder := json.NewEncoder(w)
				header := map[string]any{}
				for key, value := range coverage {
					header[key] = value
				}
				require.NoError(t, encoder.Encode(map[string]any{"object": "chunk.embedding.start", "context": header}))
				require.NoError(t, encoder.Encode(map[string]any{"object": "chunk.embedding", "data": item}))
				if terminal {
					require.NoError(t, encoder.Encode(map[string]any{"object": "chunk.embedding.complete", "response": coverage}))
				}
			}))
			defer server.Close()
			client, err := NewInferenceClient(server.URL, server.Client())
			require.NoError(t, err)
			var input oapi.InferenceChunkRequest_Input
			require.NoError(t, input.FromInferenceChunkRequestInput0("hé🐜"))
			count := 0
			result, err := client.ChunkAndEmbedStream(context.Background(), oapi.InferenceChunkRequest{Input: input, Embedding: oapi.ChunkEmbeddingTarget{Model: "model"}}, func(chunk oapi.ChunkEmbeddingObject) error {
				count++
				require.Equal(t, "hé🐜", chunk.Text)
				return nil
			})
			require.Equal(t, 1, count)
			if terminal {
				require.NoError(t, err)
				require.Equal(t, int64(1), result.CoveredThroughOrdinal)
				require.Equal(t, plan+":1", result.NextContinuation)
			} else {
				require.ErrorContains(t, err, "before terminal coverage")
				require.Nil(t, result)
			}
		})
	}
}
