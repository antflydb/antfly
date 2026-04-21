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

package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// resolveRowFilter returns the security filter for a given table from the
// principal's RowFilter map. Table-specific entries take precedence over the
// wildcard "*" key.
func resolveRowFilter(filters map[string]json.RawMessage, table string) json.RawMessage {
	if f, ok := filters[table]; ok {
		return f
	}
	if f, ok := filters["*"]; ok {
		return f
	}
	return nil
}

// injectRowFilterIntoRequest reads the request body, injects the security
// filter into every JSON object's filter_query field, and replaces the body.
// For NDJSON bodies (one JSON object per line), each object is processed.
func injectRowFilterIntoRequest(r *http.Request, secFilter json.RawMessage) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}
	r.Body.Close()

	modified, err := injectFilterIntoBody(body, secFilter)
	if err != nil {
		return fmt.Errorf("inject filter: %w", err)
	}

	r.Body = io.NopCloser(bytes.NewReader(modified))
	r.ContentLength = int64(len(modified))
	return nil
}

// injectFilterIntoBody processes one or more newline-delimited JSON objects
// and merges the security filter into each object's filter_query field.
func injectFilterIntoBody(body []byte, secFilter json.RawMessage) ([]byte, error) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		// Empty body: wrap the security filter as the sole query object.
		obj := map[string]json.RawMessage{"filter_query": secFilter}
		return json.Marshal(obj)
	}

	lines := splitNDJSON(body)
	var out bytes.Buffer
	for i, line := range lines {
		merged, err := mergeFilterQuery(line, secFilter)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", i, err)
		}
		if i > 0 {
			out.WriteByte('\n')
		}
		out.Write(merged)
	}
	return out.Bytes(), nil
}

// injectFilterIntoAgentBody handles retrieval agent request bodies that contain
// a top-level "queries" array, injecting the security filter into each query's
// filter_query field.
func injectFilterIntoAgentBody(body []byte, secFilter json.RawMessage) ([]byte, error) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return body, nil
	}

	var obj map[string]json.RawMessage
	if err := json.Unmarshal(body, &obj); err != nil {
		return nil, fmt.Errorf("parse agent body: %w", err)
	}

	queriesRaw, ok := obj["queries"]
	if !ok {
		// No queries array — treat as a single query object.
		return mergeFilterQuery(body, secFilter)
	}

	var queries []json.RawMessage
	if err := json.Unmarshal(queriesRaw, &queries); err != nil {
		return nil, fmt.Errorf("parse queries array: %w", err)
	}

	for i, q := range queries {
		merged, err := mergeFilterQuery(q, secFilter)
		if err != nil {
			return nil, fmt.Errorf("query %d: %w", i, err)
		}
		queries[i] = merged
	}

	updatedQueries, err := json.Marshal(queries)
	if err != nil {
		return nil, err
	}
	obj["queries"] = updatedQueries

	return json.Marshal(obj)
}

// mergeFilterQuery merges the security filter into a single JSON object's
// filter_query field. If filter_query already exists, the two are conjuncted.
func mergeFilterQuery(objJSON []byte, secFilter json.RawMessage) ([]byte, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(objJSON, &obj); err != nil {
		return nil, err
	}

	existing, hasExisting := obj["filter_query"]
	if !hasExisting || isNullOrEmpty(existing) {
		obj["filter_query"] = secFilter
	} else {
		conjunction, err := json.Marshal(map[string]interface{}{
			"conjuncts": []json.RawMessage{existing, secFilter},
		})
		if err != nil {
			return nil, err
		}
		obj["filter_query"] = conjunction
	}

	return json.Marshal(obj)
}

func isNullOrEmpty(data json.RawMessage) bool {
	trimmed := bytes.TrimSpace(data)
	return len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null"))
}

// splitNDJSON splits a body into individual JSON objects. It handles both
// newline-delimited JSON and a single JSON object.
func splitNDJSON(body []byte) [][]byte {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return nil
	}
	// If it starts with '{', check if there are multiple lines.
	lines := bytes.Split(body, []byte("\n"))
	var result [][]byte
	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) > 0 {
			result = append(result, line)
		}
	}
	return result
}

// isAgentPath returns true if the backend path routes to the retrieval agent.
func isAgentPath(backendPath string) bool {
	return strings.HasPrefix(backendPath, "/agents/") || backendPath == "/agents"
}
