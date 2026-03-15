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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func TestApplyPatternCorrections(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "singular table to plural",
			input:    "/api/v1/table",
			expected: "/api/v1/tables",
		},
		{
			name:     "singular index to plural",
			input:    "/api/v1/tables/foo/index",
			expected: "/api/v1/tables/foo/indexes",
		},
		{
			name:     "nested index path",
			input:    "/api/v1/tables/mytable/index/myindex",
			expected: "/api/v1/tables/mytable/indexes/myindex",
		},
		{
			name:     "singular agent to plural",
			input:    "/api/v1/agent/retrieval",
			expected: "/api/v1/agents/retrieval",
		},
		{
			name:     "already plural - no change",
			input:    "/api/v1/tables",
			expected: "/api/v1/tables",
		},
		{
			name:     "non-API path - no change",
			input:    "/some/random/path",
			expected: "/some/random/path",
		},
		{
			name:     "multiple corrections in one path",
			input:    "/api/v1/table/foo/index/bar",
			expected: "/api/v1/tables/foo/indexes/bar",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := applyPatternCorrections(tt.input)
			if result != tt.expected {
				t.Errorf("applyPatternCorrections(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestMatchesPattern(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		pattern  string
		expected bool
	}{
		{
			name:     "exact match",
			path:     "/api/v1/tables",
			pattern:  "/api/v1/tables",
			expected: true,
		},
		{
			name:     "parameter match",
			path:     "/api/v1/tables/mytable",
			pattern:  "/api/v1/tables/{tableName}",
			expected: true,
		},
		{
			name:     "multiple parameters",
			path:     "/api/v1/tables/mytable/indexes/myindex",
			pattern:  "/api/v1/tables/{tableName}/indexes/{indexName}",
			expected: true,
		},
		{
			name:     "no match - different segments",
			path:     "/api/v1/table",
			pattern:  "/api/v1/tables",
			expected: false,
		},
		{
			name:     "no match - different length",
			path:     "/api/v1/tables/foo/bar",
			pattern:  "/api/v1/tables/{tableName}",
			expected: false,
		},
		{
			name:     "trailing slash handling",
			path:     "/api/v1/tables/",
			pattern:  "/api/v1/tables",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchesPattern(tt.path, tt.pattern)
			if result != tt.expected {
				t.Errorf("matchesPattern(%q, %q) = %v, want %v", tt.path, tt.pattern, result, tt.expected)
			}
		})
	}
}

func TestLevenshteinDistance(t *testing.T) {
	tests := []struct {
		name     string
		s1       string
		s2       string
		expected int
	}{
		{
			name:     "identical strings",
			s1:       "hello",
			s2:       "hello",
			expected: 0,
		},
		{
			name:     "one character difference",
			s1:       "hello",
			s2:       "hallo",
			expected: 1,
		},
		{
			name:     "insertion",
			s1:       "hello",
			s2:       "helllo",
			expected: 1,
		},
		{
			name:     "deletion",
			s1:       "hello",
			s2:       "helo",
			expected: 1,
		},
		{
			name:     "table vs tables",
			s1:       "table",
			s2:       "tables",
			expected: 1,
		},
		{
			name:     "complete mismatch",
			s1:       "abc",
			s2:       "xyz",
			expected: 3,
		},
		{
			name:     "empty strings",
			s1:       "",
			s2:       "",
			expected: 0,
		},
		{
			name:     "one empty string",
			s1:       "hello",
			s2:       "",
			expected: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := levenshteinDistance(tt.s1, tt.s2)
			if result != tt.expected {
				t.Errorf("levenshteinDistance(%q, %q) = %d, want %d", tt.s1, tt.s2, result, tt.expected)
			}
		})
	}
}

func TestReplaceParameters(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "single parameter",
			input:    "/tables/{tableName}",
			expected: "/tables/:param",
		},
		{
			name:     "multiple parameters",
			input:    "/tables/{tableName}/indexes/{indexName}",
			expected: "/tables/:param/indexes/:param",
		},
		{
			name:     "no parameters",
			input:    "/tables/status",
			expected: "/tables/status",
		},
		{
			name:     "parameter at end",
			input:    "/lookup/{key}",
			expected: "/lookup/:param",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := replaceParameters(tt.input)
			if result != tt.expected {
				t.Errorf("replaceParameters(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestFuzzyMatchEndpoints(t *testing.T) {
	tests := []struct {
		name          string
		requestedPath string
		expectMatch   bool
		expectContain string // Expected to contain this endpoint
	}{
		{
			name:          "typo in tables",
			requestedPath: "/api/v1/tabel",
			expectMatch:   true,
			expectContain: "/api/v1/tables",
		},
		{
			name:          "missing character",
			requestedPath: "/api/v1/tbles",
			expectMatch:   true,
			expectContain: "/api/v1/tables",
		},
		{
			name:          "extra character",
			requestedPath: "/api/v1/tabless",
			expectMatch:   true,
			expectContain: "/api/v1/tables",
		},
		{
			name:          "status typo",
			requestedPath: "/api/v1/statu",
			expectMatch:   true,
			expectContain: "/api/v1/status",
		},
		{
			name:          "completely different - no good match",
			requestedPath: "/api/v1/xyz123",
			expectMatch:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			suggestions := fuzzyMatchEndpoints(tt.requestedPath, 3)

			if tt.expectMatch {
				if len(suggestions) == 0 {
					t.Errorf("fuzzyMatchEndpoints(%q) returned no suggestions, expected at least one", tt.requestedPath)
					return
				}

				if tt.expectContain != "" {
					found := slices.Contains(suggestions, tt.expectContain)
					if !found {
						t.Errorf("fuzzyMatchEndpoints(%q) = %v, expected to contain %q", tt.requestedPath, suggestions, tt.expectContain)
					}
				}
			} else {
				if len(suggestions) > 0 {
					t.Logf("fuzzyMatchEndpoints(%q) returned suggestions %v, but none expected (not necessarily an error)", tt.requestedPath, suggestions)
				}
			}
		})
	}
}

func TestFindSimilarEndpoints(t *testing.T) {
	tests := []struct {
		name          string
		requestedPath string
		expectContain string
	}{
		{
			name:          "pattern correction - singular table",
			requestedPath: "/api/v1/table",
			expectContain: "/api/v1/tables",
		},
		{
			name:          "pattern correction - nested index",
			requestedPath: "/api/v1/tables/foo/index",
			expectContain: "/api/v1/tables/{tableName}/indexes",
		},
		{
			name:          "fuzzy match - typo",
			requestedPath: "/api/v1/tabel",
			expectContain: "/api/v1/tables",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			suggestions := findSimilarEndpoints(tt.requestedPath)

			if len(suggestions) == 0 {
				t.Errorf("findSimilarEndpoints(%q) returned no suggestions", tt.requestedPath)
				return
			}

			found := slices.Contains(suggestions, tt.expectContain)
			if !found {
				t.Errorf("findSimilarEndpoints(%q) = %v, expected to contain %q", tt.requestedPath, suggestions, tt.expectContain)
			}
		})
	}
}

func TestFormatSuggestions(t *testing.T) {
	tests := []struct {
		name          string
		requestedPath string
		suggestions   []string
		expectContain string
	}{
		{
			name:          "single suggestion",
			requestedPath: "/api/v1/table",
			suggestions:   []string{"/api/v1/tables"},
			expectContain: "Did you mean: /api/v1/tables?",
		},
		{
			name:          "multiple suggestions",
			requestedPath: "/api/v1/tab",
			suggestions:   []string{"/api/v1/tables", "/api/v1/status"},
			expectContain: "Did you mean one of:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatSuggestions(tt.requestedPath, tt.suggestions)

			if !strings.Contains(result, tt.expectContain) {
				t.Errorf("formatSuggestions(%q, %v) = %q, expected to contain %q", tt.requestedPath, tt.suggestions, result, tt.expectContain)
			}

			// Ensure requested path is in the message
			if !strings.Contains(result, tt.requestedPath) {
				t.Errorf("formatSuggestions should include requested path %q in message: %q", tt.requestedPath, result)
			}
		})
	}
}

func TestNotFoundHandler(t *testing.T) {
	// Create a simple handler that returns 404 for all requests
	baseHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})

	logger := zap.NewNop()
	handler := newNotFoundHandler(baseHandler, logger)

	tests := []struct {
		name               string
		requestPath        string
		expectStatus       int
		expectContainError bool
		expectSuggestion   string
	}{
		{
			name:               "typo in table",
			requestPath:        "/api/v1/table",
			expectStatus:       http.StatusNotFound,
			expectContainError: true,
			expectSuggestion:   "/api/v1/tables",
		},
		{
			name:               "typo in nested path",
			requestPath:        "/api/v1/tables/foo/index",
			expectStatus:       http.StatusNotFound,
			expectContainError: true,
			expectSuggestion:   "indexes",
		},
		{
			name:               "fuzzy match",
			requestPath:        "/api/v1/tabel",
			expectStatus:       http.StatusNotFound,
			expectContainError: true,
			expectSuggestion:   "/api/v1/tables",
		},
		{
			name:               "completely wrong path",
			requestPath:        "/api/v1/nonexistent",
			expectStatus:       http.StatusNotFound,
			expectContainError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.requestPath, nil)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != tt.expectStatus {
				t.Errorf("handler returned wrong status code: got %v want %v", rec.Code, tt.expectStatus)
			}

			if tt.expectContainError {
				var response map[string]any
				if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
					t.Fatalf("failed to unmarshal response: %v", err)
				}

				errorMsg, ok := response["error"].(string)
				if !ok || errorMsg == "" {
					t.Errorf("expected error field in response, got: %v", response)
				}

				if tt.expectSuggestion != "" {
					if !strings.Contains(errorMsg, tt.expectSuggestion) {
						t.Errorf("expected error message to contain %q, got: %q", tt.expectSuggestion, errorMsg)
					}
				}
			}
		})
	}
}

func TestNotFoundHandler_ValidRequest(t *testing.T) {
	// Create a handler that returns 200 for valid requests
	baseHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/status" {
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(`{"status":"ok"}`)); err != nil {
				t.Logf("failed to write response: %v", err)
			}
		} else {
			http.NotFound(w, r)
		}
	})

	logger := zap.NewNop()
	handler := newNotFoundHandler(baseHandler, logger)

	req := httptest.NewRequest("GET", "/api/v1/status", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	// Should return 200 and not trigger 404 handler
	if rec.Code != http.StatusOK {
		t.Errorf("handler returned wrong status code for valid request: got %v want %v", rec.Code, http.StatusOK)
	}

	if !strings.Contains(rec.Body.String(), "status") {
		t.Errorf("expected valid response body, got: %s", rec.Body.String())
	}
}

func TestNotFoundHandler_NoDoubleWriteHeader(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
	}{
		{
			name: "200 with explicit WriteHeader then Write",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"ok":true}`))
			}),
		},
		{
			name: "200 with implicit WriteHeader via Write only",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(`{"ok":true}`))
			}),
		},
		{
			name: "201 with WriteHeader then Write",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`created`))
			}),
		},
		{
			name: "204 No Content with WriteHeader only",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Wrap recorder to count WriteHeader calls
			counting := &countingResponseWriter{ResponseWriter: httptest.NewRecorder()}

			logger := zap.NewNop()
			handler := newNotFoundHandler(tt.handler, logger)
			req := httptest.NewRequest("GET", "/api/v1/status", nil)
			handler.ServeHTTP(counting, req)

			if counting.writeHeaderCalls > 1 {
				t.Errorf("WriteHeader called %d times, want at most 1", counting.writeHeaderCalls)
			}
		})
	}
}

// countingResponseWriter counts WriteHeader calls to detect double-writes
type countingResponseWriter struct {
	http.ResponseWriter
	writeHeaderCalls int
}

func (w *countingResponseWriter) WriteHeader(statusCode int) {
	w.writeHeaderCalls++
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *countingResponseWriter) Write(b []byte) (int, error) {
	return w.ResponseWriter.Write(b)
}

func TestCaptureResponseWriter(t *testing.T) {
	rec := httptest.NewRecorder()
	crw := &captureResponseWriter{
		ResponseWriter: rec,
		statusCode:     http.StatusOK,
		buffer:         &strings.Builder{},
	}

	// Test WriteHeader
	crw.WriteHeader(http.StatusNotFound)
	if crw.statusCode != http.StatusNotFound {
		t.Errorf("expected status code %d, got %d", http.StatusNotFound, crw.statusCode)
	}

	// Test Write with 404 status (should buffer)
	n, err := crw.Write([]byte("test"))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if n != 4 {
		t.Errorf("expected to write 4 bytes, wrote %d", n)
	}
	// For 404, content should be buffered, not written
	if rec.Body.Len() > 0 {
		t.Error("expected 404 response to be buffered, not written to underlying writer")
	}
	if crw.buffer.Len() != 4 {
		t.Errorf("expected buffer to contain 4 bytes, got %d", crw.buffer.Len())
	}
}
