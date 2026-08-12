/*
Copyright 2026 The Antfly Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sdk

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsolidatedClientAppliesRequestEditorsToAntflyAndInference(t *testing.T) {
	seen := make(map[string]bool)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer secret", r.Header.Get("Authorization"))
		assert.Equal(t, "tenant-1", r.Header.Get("X-Tenant"))
		seen[r.URL.Path] = true

		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/db/v1/tables":
			_, _ = io.WriteString(w, "[]")
		case "/ai/v1/models":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"embedders": map[string]any{}, "chunkers": map[string]any{}, "rerankers": map[string]any{},
				"generators": map[string]any{}, "recognizers": map[string]any{}, "extractors": map[string]any{},
				"rewriters": map[string]any{}, "classifiers": map[string]any{}, "readers": map[string]any{},
				"transcribers": map[string]any{},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
		RequestEditors: []oapi.RequestEditorFn{
			WithToken("secret"),
			func(_ context.Context, request *http.Request) error {
				request.Header.Set("X-Tenant", "tenant-1")
				return nil
			},
		},
	})
	require.NoError(t, err)
	_, err = client.Antfly().ListTables(context.Background())
	require.NoError(t, err)
	_, err = client.Inference().ListModels(context.Background())
	require.NoError(t, err)
	assert.Equal(t, map[string]bool{"/db/v1/tables": true, "/ai/v1/models": true}, seen)
}
