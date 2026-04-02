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

package metadata

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/antflydb/antfly/src/metadata/kv"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestNewAPIServerUsesUploadFriendlyReadTimeout(t *testing.T) {
	t.Parallel()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	srv := NewAPIServer("127.0.0.1:8080", handler)

	assert.Equal(t, "127.0.0.1:8080", srv.Addr)
	assert.NotNil(t, srv.Handler)
	assert.Equal(t, time.Minute, srv.ReadTimeout)
}

// TestCombinedAPIServerRouting tests that the combined API server
// properly routes requests to internal and public APIs
func TestCombinedAPIServerRouting(t *testing.T) {
	logger := zap.NewNop()

	// Create internal API routes
	internalMux := http.NewServeMux()

	// Add a test internal route
	internalMux.HandleFunc("POST /test-internal", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("internal")); err != nil {
			logger.Error("failed to write response", zap.Error(err))
		}
	})

	// Add metadata store API routes
	api := kv.NewMetadataStoreAPI(logger, nil)
	api.AddRoutes(internalMux)

	// Create public API routes
	publicMux := http.NewServeMux()
	publicMux.HandleFunc("GET /test-public", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("public")); err != nil {
			logger.Error("failed to write response", zap.Error(err))
		}
	})

	// Combined router (mimicking the structure in runner.go)
	apiRoutes := http.NewServeMux()
	apiRoutes.Handle("/api/v1/", http.StripPrefix("/api/v1", publicMux))
	apiRoutes.Handle("/_internal/v1/", http.StripPrefix("/_internal/v1", internalMux))

	tests := []struct {
		name           string
		method         string
		path           string
		expectedStatus int
		expectedBody   string
		skipExecution  bool // Skip actual handler execution (would panic with nil store)
	}{
		{
			name:           "internal route accessible at /_internal/v1/",
			method:         "POST",
			path:           "/_internal/v1/test-internal",
			expectedStatus: http.StatusOK,
			expectedBody:   "internal",
		},
		{
			name:           "public route accessible at /api/v1/",
			method:         "GET",
			path:           "/api/v1/test-public",
			expectedStatus: http.StatusOK,
			expectedBody:   "public",
		},
		{
			name:           "metadata store batch endpoint at /_internal/v1/batch",
			method:         "POST",
			path:           "/_internal/v1/batch",
			expectedStatus: http.StatusBadRequest, // No body, so will fail parsing
		},
		{
			name:           "non-existent route returns 404",
			method:         "GET",
			path:           "/nonexistent",
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()

			apiRoutes.ServeHTTP(rec, req)

			assert.Equal(t, tt.expectedStatus, rec.Code)

			if tt.expectedBody != "" {
				assert.Equal(t, tt.expectedBody, rec.Body.String())
			}
		})
	}
}

// TestInternalRoutesNotAccessibleAtRoot verifies that internal routes
// are not accessible without the /_internal/v1/ prefix
func TestInternalRoutesNotAccessibleAtRoot(t *testing.T) {
	internalMux := http.NewServeMux()
	internalMux.HandleFunc("POST /store", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	apiRoutes := http.NewServeMux()
	apiRoutes.Handle("/_internal/v1/", http.StripPrefix("/_internal/v1", internalMux))

	// Try to access the route without the prefix
	req := httptest.NewRequest("POST", "/store", nil)
	rec := httptest.NewRecorder()

	apiRoutes.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code,
		"Expected 404 for route without prefix")

	// Now try with the correct prefix
	req = httptest.NewRequest("POST", "/_internal/v1/store", nil)
	rec = httptest.NewRecorder()

	apiRoutes.ServeHTTP(rec, req)

	assert.NotEqual(t, http.StatusNotFound, rec.Code,
		"Expected route to be found with /_internal/v1/ prefix")
}
