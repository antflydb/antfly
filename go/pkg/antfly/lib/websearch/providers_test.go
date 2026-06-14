// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package websearch

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSearchProviderResolvesSecretRefs(t *testing.T) {
	t.Setenv("EXA_API_KEY", "resolved-exa-key")

	provider, err := NewSearchProvider(WebSearchConfig{
		Provider: WebSearchProviderExa,
		ApiKey:   "${secret:exa.api_key}",
	})

	require.NoError(t, err)
	exa, ok := provider.(*apiKeyProvider)
	require.True(t, ok)
	assert.Equal(t, "resolved-exa-key", exa.apiKey)
}

func TestNewSearchProviderFailsOnMissingSecretRef(t *testing.T) {
	_, err := NewSearchProvider(WebSearchConfig{
		Provider: WebSearchProviderExa,
		ApiKey:   "${secret:missing.api_key}",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolving exa API key")
	assert.Contains(t, err.Error(), "missing.api_key")
}

func TestVertexProviderSearch(t *testing.T) {
	var requestBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "test-project", r.Header.Get("X-Goog-User-Project"))
		require.NoError(t, json.NewDecoder(r.Body).Decode(&requestBody))
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(`{
			"results": [
				{
					"document": {
						"id": "doc-1",
						"derivedStructData": {
							"title": "Result title",
							"link": "https://example.com/result",
							"snippets": [{"snippet": "Result snippet"}]
						}
					}
				}
			],
			"summary": {"summaryText": "Summary answer"}
		}`))
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	provider := &vertexProvider{
		BaseProvider: newBaseProvider(WebSearchConfig{MaxResults: 3}),
		client:       server.Client(),
		projectID:    "test-project",
		endpoint:     server.URL,
	}

	response, err := provider.Search(t.Context(), "antfly docs", SearchOptions{})

	require.NoError(t, err)
	assert.Equal(t, float64(3), requestBody["pageSize"])
	assert.Equal(t, "antfly docs", requestBody["query"])
	assert.Equal(t, "Summary answer", response.Answer)
	if assert.Len(t, response.Results, 1) {
		assert.Equal(t, "Result title", response.Results[0].Title)
		assert.Equal(t, "https://example.com/result", response.Results[0].Url)
		assert.Equal(t, "Result snippet", response.Results[0].Snippet)
		assert.Equal(t, "example.com", response.Results[0].Source)
	}
}

func TestVertexSearchEndpoint(t *testing.T) {
	assert.Equal(
		t,
		"https://discoveryengine.googleapis.com/v1/projects/project-1/locations/global/collections/default_collection/dataStores/docs/servingConfigs/default_config:search",
		vertexSearchEndpoint("project-1", "global", "docs", "default_config"),
	)
}
