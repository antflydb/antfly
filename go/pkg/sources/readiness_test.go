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
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func readinessDocument(t *testing.T) Document {
	t.Helper()
	doc, err := Normalize("source", Item{ID: "item", Path: "a%20b.txt", URL: "s3://docs/a%2520b.txt", Revision: "same-etag", SourceKind: ProviderS3, Metadata: map[string]any{"bucket": "docs", "connection_id": "one"}}, []byte("same content"), Permissions{Public: true}, time.Unix(123, 0))
	require.NoError(t, err)
	return doc
}
func readinessResponse(doc Document) []byte {
	raw, _ := json.Marshal(map[string]any{"responses": []any{map[string]any{"status": 200, "remote_snapshot": "snapshot", "hits": map[string]any{"hits": []any{map[string]any{"_id": "item", "_source": map[string]any{"grep": doc}}}}}}})
	return raw
}
func TestCanonicalReadinessRejectsStaleNormalizedMetadata(t *testing.T) {
	for _, field := range []string{"uri", "path", "permissions", "connection", "version", "indexed_at", "content_digest"} {
		t.Run(field, func(t *testing.T) {
			want := readinessDocument(t)
			old := want
			old.Metadata = cloneSourceMetadata(want.Metadata)
			switch field {
			case "uri":
				old.URI = "s3://docs/a%20b.txt"
			case "path":
				old.Path = "old-name.txt"
			case "permissions":
				old.Permissions = Permissions{Readers: []string{"user:old"}}
			case "connection":
				old.Metadata["connection_id"] = "old"
			case "version":
				old.Version = 0
			case "indexed_at":
				old.IndexedAt = 1
			case "content_digest":
				old.Metadata["sha256"] = "old"
			}
			calls := 0
			query := func(_ context.Context, _ string, request []byte) ([]byte, error) {
				calls++
				var body struct {
					Fields []string `json:"fields"`
				}
				require.NoError(t, json.Unmarshal(request, &body))
				require.Equal(t, QueryDocumentFields(), body.Fields)
				if calls == 1 {
					return readinessResponse(old), nil
				}
				return readinessResponse(want), nil
			}
			require.NoError(t, WaitForDocuments(t.Context(), query, "docs", map[string]Document{"item": want}))
			require.Equal(t, 2, calls, "same provider revision must not approve stale canonical metadata")
		})
	}
}
func TestCanonicalReadinessStaleURIExpiresAndRejectsMissingSnapshot(t *testing.T) {
	want := readinessDocument(t)
	old := want
	old.URI = "s3://docs/a%20b.txt"
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	require.Error(t, WaitForDocuments(ctx, func(context.Context, string, []byte) ([]byte, error) { return readinessResponse(old), nil }, "docs", map[string]Document{"item": want}))
	require.Error(t, WaitForDocuments(t.Context(), func(context.Context, string, []byte) ([]byte, error) {
		return []byte(`{"responses":[{"status":200,"hits":{"hits":[]}}]}`), nil
	}, "docs", map[string]Document{"item": want}))
}
func TestCanonicalReadinessPreservesMetadataJSONNumbers(t *testing.T) {
	want := readinessDocument(t)
	want.Metadata["large"] = int64(9007199254740993)
	want.Metadata["nested"] = json.RawMessage(`{"z":2,"a":1}`)
	require.NoError(t, WaitForDocuments(t.Context(), func(context.Context, string, []byte) ([]byte, error) { return readinessResponse(want), nil }, "docs", map[string]Document{"item": want}))
}
