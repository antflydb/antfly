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

package kv

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestMetadataStoreAPI_AddRoutes(t *testing.T) {
	logger := zap.NewNop()
	api := NewMetadataStoreAPI(logger, nil)
	mux := http.NewServeMux()

	// Add routes - this should not panic
	api.AddRoutes(mux)

	// Verify routes are registered by checking that we get method-specific responses
	// Routes should be: POST /peer/{peer}, DELETE /peer/{peer}, POST /batch

	// Test wrong method on /batch - should get 405 Method Not Allowed, not 404
	req := httptest.NewRequest("GET", "/batch", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	assert.NotEqual(t, http.StatusNotFound, rec.Code,
		"Route POST /batch not registered - got 404 when testing with GET method")
}

func TestNewMetadataStoreAPI(t *testing.T) {
	logger := zap.NewNop()
	api := NewMetadataStoreAPI(logger, nil)

	require.NotNil(t, api, "NewMetadataStoreAPI returned nil")
	assert.Equal(t, logger, api.logger, "Logger not set correctly")
}
