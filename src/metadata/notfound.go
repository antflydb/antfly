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
	"fmt"
	"net/http"
	"sort"
	"strings"

	"go.uber.org/zap"
)

// validEndpoints contains all valid API endpoint patterns
// extracted from src/metadata/api.yaml OpenAPI specification
var validEndpoints = []string{
	// Cluster
	"/api/v1/status",
	"/api/v1/backup",
	"/api/v1/restore",
	"/api/v1/backups",

	// Global operations
	"/api/v1/query",

	// Table management
	"/api/v1/tables",
	"/api/v1/tables/{tableName}",
	"/api/v1/tables/{tableName}/schema",

	// Table operations
	"/api/v1/tables/{tableName}/query",
	"/api/v1/tables/{tableName}/batch",
	"/api/v1/tables/{tableName}/merge",
	"/api/v1/tables/{tableName}/lookup",
	"/api/v1/tables/{tableName}/lookup/{key}",
	"/api/v1/tables/{tableName}/backup",
	"/api/v1/tables/{tableName}/restore",

	// Index management
	"/api/v1/tables/{tableName}/indexes",
	"/api/v1/tables/{tableName}/indexes/{indexName}",

	// Agents
	"/api/v1/agents/retrieval",
	"/api/v1/agents/query-builder",

	// Cross-table operations
	"/api/v1/batch",

	// Transactions
	"/api/v1/transactions/commit",

	// MCP (Model Context Protocol)
	"/mcp/v1/",

	// API Key management
	"/api/v1/users/{userName}/api-keys",
	"/api/v1/users/{userName}/api-keys/{keyId}",
}

// patternReplacements maps common typos to their correct forms
var patternReplacements = map[string]string{
	"table":  "tables",
	"index":  "indexes",
	"agent":  "agents",
	"query":  "queries", // Less common but still possible
	"status": "statuses",
}

// notFoundHandler wraps an http.Handler and provides helpful suggestions
// when a 404 is encountered
type notFoundHandler struct {
	handler http.Handler
	logger  *zap.Logger
}

// newNotFoundHandler creates a new 404-handling middleware wrapper
func newNotFoundHandler(handler http.Handler, logger *zap.Logger) http.Handler {
	return &notFoundHandler{
		handler: handler,
		logger:  logger,
	}
}

// ServeHTTP implements http.Handler by intercepting 404 responses
func (h *notFoundHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Create a custom response writer to capture the status code
	// and prevent 404 responses from being written
	crw := &captureResponseWriter{
		ResponseWriter: w,
		statusCode:     http.StatusOK,
		buffer:         &strings.Builder{},
	}

	// Call the wrapped handler
	h.handler.ServeHTTP(crw, r)

	// If it's a 404, provide helpful suggestions instead of default response
	if crw.statusCode == http.StatusNotFound {
		requestedPath := r.URL.Path
		suggestions := findSimilarEndpoints(requestedPath)

		if len(suggestions) > 0 {
			h.logger.Debug("404 with suggestions",
				zap.String("requested", requestedPath),
				zap.Strings("suggestions", suggestions))

			message := formatSuggestions(requestedPath, suggestions)
			errorResponse(w, message, http.StatusNotFound)
		} else {
			// No suggestions found, return generic 404
			errorResponse(w, fmt.Sprintf("Endpoint not found: %s", requestedPath), http.StatusNotFound)
		}
		return
	}

	// For non-404 responses, flush the buffered content
	if crw.flushed {
		// Already flushed by Write — nothing to do.
	} else if crw.buffer.Len() > 0 {
		w.WriteHeader(crw.statusCode)
		if _, err := w.Write([]byte(crw.buffer.String())); err != nil {
			h.logger.Error("failed to write response", zap.Error(err))
		}
	} else if crw.headerWritten {
		// Handler called WriteHeader (e.g., 204 No Content) without writing a body.
		w.WriteHeader(crw.statusCode)
	}
}

// captureResponseWriter wraps http.ResponseWriter to capture status code
// and buffer 404 responses before writing
type captureResponseWriter struct {
	http.ResponseWriter
	statusCode    int
	headerWritten bool // WriteHeader was called by the inner handler
	flushed       bool // status was forwarded to the underlying writer
	buffer        *strings.Builder
}

func (w *captureResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.headerWritten = true
	// Don't write header yet - we'll decide what to do based on status code
}

func (w *captureResponseWriter) Write(b []byte) (int, error) {
	// If it's a 404, buffer the response instead of writing
	if w.statusCode == http.StatusNotFound {
		return w.buffer.Write(b)
	}

	// Flush the status code to the underlying writer once
	if !w.flushed && w.headerWritten {
		w.ResponseWriter.WriteHeader(w.statusCode)
	}
	w.flushed = true
	return w.ResponseWriter.Write(b)
}

// Flush implements http.Flusher to support SSE streaming
func (w *captureResponseWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// findSimilarEndpoints returns up to 3 similar endpoints for a given path
// Uses pattern matching first, then fuzzy matching as fallback
func findSimilarEndpoints(requestedPath string) []string {
	// Try pattern-based correction first (fast path)
	if corrected := applyPatternCorrections(requestedPath); corrected != requestedPath {
		// Check if the corrected path exists
		for _, endpoint := range validEndpoints {
			if matchesPattern(corrected, endpoint) {
				return []string{endpoint}
			}
		}
	}

	// Fall back to fuzzy matching
	return fuzzyMatchEndpoints(requestedPath, 3)
}

// applyPatternCorrections applies common singular→plural corrections
func applyPatternCorrections(path string) string {
	// Normalize path (ensure it starts with /api/v1/)
	normalized := path
	if !strings.HasPrefix(normalized, "/api/v1/") {
		return normalized // Don't try to correct non-API paths
	}

	// Split path into segments
	segments := strings.Split(strings.TrimPrefix(normalized, "/"), "/")

	// Apply replacements to each segment
	for i, segment := range segments {
		if replacement, exists := patternReplacements[segment]; exists {
			segments[i] = replacement
		}
	}

	return "/" + strings.Join(segments, "/")
}

// matchesPattern checks if a path matches an endpoint pattern
// Handles parameterized segments like {tableName}
func matchesPattern(path, pattern string) bool {
	// Strip trailing slashes for comparison
	path = strings.TrimSuffix(path, "/")
	pattern = strings.TrimSuffix(pattern, "/")

	pathSegments := strings.Split(path, "/")
	patternSegments := strings.Split(pattern, "/")

	if len(pathSegments) != len(patternSegments) {
		return false
	}

	for i := range pathSegments {
		// Parameters like {tableName} match any segment
		if strings.HasPrefix(patternSegments[i], "{") && strings.HasSuffix(patternSegments[i], "}") {
			continue
		}
		if pathSegments[i] != patternSegments[i] {
			return false
		}
	}

	return true
}

// fuzzyMatchEndpoints finds endpoints similar to the requested path
// using Levenshtein distance, returning up to maxSuggestions results
func fuzzyMatchEndpoints(requestedPath string, maxSuggestions int) []string {
	type match struct {
		endpoint string
		distance int
	}

	// Strip /api/v1/ prefix for comparison to reduce distance noise
	normalizedPath := strings.TrimPrefix(requestedPath, "/api/v1")
	normalizedPath = strings.TrimSuffix(normalizedPath, "/")

	var matches []match
	const maxDistance = 5 // Maximum edit distance to consider

	for _, endpoint := range validEndpoints {
		normalizedEndpoint := strings.TrimPrefix(endpoint, "/api/v1")
		normalizedEndpoint = strings.TrimSuffix(normalizedEndpoint, "/")

		// Replace {param} patterns with a placeholder for comparison
		normalizedEndpoint = replaceParameters(normalizedEndpoint)
		normalizedRequestPath := replaceParameters(normalizedPath)

		distance := levenshteinDistance(normalizedRequestPath, normalizedEndpoint)

		if distance <= maxDistance {
			matches = append(matches, match{endpoint: endpoint, distance: distance})
		}
	}

	// Sort by distance (closest first)
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].distance < matches[j].distance
	})

	// Return top N suggestions
	results := make([]string, 0, maxSuggestions)
	for i := 0; i < len(matches) && i < maxSuggestions; i++ {
		results = append(results, matches[i].endpoint)
	}

	return results
}

// replaceParameters replaces {param} patterns with a fixed placeholder
// to normalize paths for distance calculation
func replaceParameters(path string) string {
	// Simple approach: replace any {.*} with :param
	result := path
	for {
		start := strings.Index(result, "{")
		if start == -1 {
			break
		}
		end := strings.Index(result[start:], "}")
		if end == -1 {
			break
		}
		result = result[:start] + ":param" + result[start+end+1:]
	}
	return result
}

// levenshteinDistance calculates the edit distance between two strings
// Uses dynamic programming with O(n*m) time and O(min(n,m)) space
func levenshteinDistance(s1, s2 string) int {
	// Ensure s1 is the shorter string to optimize space
	if len(s1) > len(s2) {
		s1, s2 = s2, s1
	}

	// Use rolling array to save space
	prev := make([]int, len(s1)+1)
	curr := make([]int, len(s1)+1)

	// Initialize first row
	for i := range prev {
		prev[i] = i
	}

	// Fill the matrix
	for i := 1; i <= len(s2); i++ {
		curr[0] = i
		for j := 1; j <= len(s1); j++ {
			cost := 1
			if s1[j-1] == s2[i-1] {
				cost = 0
			}

			curr[j] = min(
				prev[j]+1,      // deletion
				curr[j-1]+1,    // insertion
				prev[j-1]+cost, // substitution
			)
		}
		prev, curr = curr, prev
	}

	return prev[len(s1)]
}

// formatSuggestions creates a human-friendly error message with suggestions
func formatSuggestions(requestedPath string, suggestions []string) string {
	if len(suggestions) == 1 {
		return fmt.Sprintf("Endpoint not found: %s. Did you mean: %s?", requestedPath, suggestions[0])
	}

	suggestionList := strings.Join(suggestions, ", ")
	return fmt.Sprintf("Endpoint not found: %s. Did you mean one of: %s?", requestedPath, suggestionList)
}
