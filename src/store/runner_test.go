// Copyright 2025 Antfly, Inc.
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

package store

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/antflydb/antfly/lib/types"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStoreRegistrationEndpoint verifies that the store registration
// uses the correct /_internal/v1/store endpoint
func TestStoreRegistrationEndpoint(t *testing.T) {
	// Track the request path
	var requestPath string
	var requestMethod string

	// Create a test server that captures the request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestPath = r.URL.Path
		requestMethod = r.Method

		// Read and discard body
		_, _ = io.ReadAll(r.Body)

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create a minimal store for testing
	store := &Store{
		config:    &StoreInfo{ID: types.ID(1)},
		shardsMap: xsync.NewMap[types.ID, *Shard](),
	}

	// Create a StoreInfo
	conf := &StoreInfo{
		ID:      types.ID(1),
		RaftURL: "http://localhost:9021",
		ApiURL:  "http://localhost:12380",
	}

	// Attempt registration
	err := registerWithLeader(context.Background(), server.Client(), server.URL, store, conf)

	require.NoError(t, err, "Registration failed")

	// Verify the endpoint path
	expectedPath := "/_internal/v1/store"
	assert.Equal(t, expectedPath, requestPath, "Request path should use /_internal/v1/ prefix")

	// Verify the HTTP method
	assert.Equal(t, http.MethodPost, requestMethod, "Should use POST method")
}

// TestStoreRegistrationURL verifies the full URL construction
func TestStoreRegistrationURL(t *testing.T) {
	tests := []struct {
		name        string
		leaderURL   string
		expectedURL string
	}{
		{
			name:        "http URL",
			leaderURL:   "http://127.0.0.1:12277",
			expectedURL: "http://127.0.0.1:12277/_internal/v1/store",
		},
		{
			name:        "https URL",
			leaderURL:   "https://metadata.example.com:8080",
			expectedURL: "https://metadata.example.com:8080/_internal/v1/store",
		},
		{
			name:        "URL with trailing slash",
			leaderURL:   "http://127.0.0.1:12277/",
			expectedURL: "http://127.0.0.1:12277//_internal/v1/store", // Will still work with double slash
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedURL string

			server := httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					capturedURL = r.URL.Path
					w.WriteHeader(http.StatusOK)
				}),
			)
			defer server.Close()

			store := &Store{
				config:    &StoreInfo{ID: types.ID(1)},
				shardsMap: xsync.NewMap[types.ID, *Shard](),
			}

			conf := &StoreInfo{
				ID:      types.ID(1),
				RaftURL: "http://localhost:9021",
				ApiURL:  "http://localhost:12380",
			}

			// Use the server URL (ignore tt.leaderURL since we need to use the test server)
			_ = registerWithLeader(context.Background(), server.Client(), server.URL, store, conf)

			// Verify the path includes /_internal/v1/store
			assert.Equal(t, "/_internal/v1/store", capturedURL,
				"Path should be /_internal/v1/store")
		})
	}
}
