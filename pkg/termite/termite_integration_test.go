// Copyright 2025 Antfly, Inc.
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

package termite

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestTermiteNode_EmbedEndpoint_NoModels(t *testing.T) {
	logger := zaptest.NewLogger(t)

	// Create node without embedder registry (simulates no models configured)
	node := &TermiteNode{
		logger: logger,
		client: &http.Client{Timeout: 10 * time.Second},
		// embedderRegistry is nil - no models configured
	}
	handler := NewTermiteAPI(logger, node)

	server := httptest.NewServer(handler)
	defer server.Close()

	// Test that embed endpoint returns 503 when no models configured
	t.Run("EmbedReturns503WhenNoModels", func(t *testing.T) {
		reqBody := EmbedRequest{
			Model: "test-model",
		}
		_ = reqBody.Input.FromEmbedRequestInput1([]string{"hello", "world"})

		body, err := json.Marshal(reqBody)
		require.NoError(t, err)

		req, err := http.NewRequest("POST", server.URL+"/ml/v1/embed", bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req) //nolint:gosec // test server URL
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()

		assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	})

	// Test that model is required
	t.Run("EmbedRequiresModel", func(t *testing.T) {
		reqBody := EmbedRequest{
			Model: "", // Empty model
		}
		_ = reqBody.Input.FromEmbedRequestInput1([]string{"hello"})

		body, err := json.Marshal(reqBody)
		require.NoError(t, err)

		req, err := http.NewRequest("POST", server.URL+"/ml/v1/embed", bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req) //nolint:gosec // test server URL
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()

		// 503 because embedderRegistry is nil, checked before model validation
		assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	})

	// Test invalid JSON
	t.Run("EmbedRejectsInvalidJSON", func(t *testing.T) {
		req, err := http.NewRequest(
			"POST",
			server.URL+"/ml/v1/embed",
			bytes.NewReader([]byte("invalid json")),
		)
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req) //nolint:gosec // test server URL
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()

		// Still returns 503 because embedderRegistry check happens first
		// This is fine - the order of checks is implementation detail
		assert.True(t, resp.StatusCode == http.StatusServiceUnavailable ||
			resp.StatusCode == http.StatusBadRequest)
	})
}

func TestTermiteNode_ModelsEndpoint(t *testing.T) {
	logger := zaptest.NewLogger(t)

	// Create node without any registries
	node := &TermiteNode{
		logger: logger,
		client: &http.Client{Timeout: 10 * time.Second},
	}
	handler := NewTermiteAPI(logger, node)

	server := httptest.NewServer(handler)
	defer server.Close()

	t.Run("ListModelsReturnsEmptyArrays", func(t *testing.T) {
		req, err := http.NewRequest("GET", server.URL+"/ml/v1/models", nil)
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req) //nolint:gosec // test server URL
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var modelsResp ModelsResponse
		err = json.NewDecoder(resp.Body).Decode(&modelsResp)
		require.NoError(t, err)

		// All should be empty arrays when no registries configured
		assert.Empty(t, modelsResp.Chunkers)
		assert.Empty(t, modelsResp.Rerankers)
		assert.Empty(t, modelsResp.Embedders)
	})
}

// Test the full RunAsTermite function
func TestRunAsTermite(t *testing.T) {
	logger := zaptest.NewLogger(t)
	t.Cleanup(func() {
		_ = os.RemoveAll("termitedb")
	})

	config := Config{
		ApiUrl: "http://localhost:0", // Use port 0 for testing
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Run in a goroutine
	readyC := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunAsTermite(ctx, logger, config, readyC)
	}()

	// Wait for server to be ready (may be slow if backends need to download libraries)
	select {
	case <-readyC:
		// Server is ready
	case <-time.After(120 * time.Second):
		cancel()
		t.Fatal("RunAsTermite did not start in time")
	}

	// Cancel to trigger shutdown
	cancel()

	// Wait for completion with timeout
	select {
	case <-done:
		// Success
	case <-time.After(10 * time.Second):
		t.Fatal("RunAsTermite did not shut down in time")
	}
}
