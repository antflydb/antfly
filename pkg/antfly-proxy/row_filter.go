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

package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
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

func extractTableAccessesFromBody(body []byte, defaultTable string, agentBody bool) ([]TableAccess, error) {
	body = bytes.TrimSpace(body)
	accesses := map[string]TableAccess{}
	addTableAccess(accesses, defaultTable, OperationRead)
	if len(body) == 0 {
		return sortedTableAccesses(accesses), nil
	}

	if agentBody {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(body, &obj); err != nil {
			return nil, fmt.Errorf("parse agent body: %w", err)
		}
		if queriesRaw, ok := obj["queries"]; ok {
			var queries []json.RawMessage
			if err := json.Unmarshal(queriesRaw, &queries); err != nil {
				return nil, fmt.Errorf("parse queries array: %w", err)
			}
			for i, query := range queries {
				if err := extractQueryTableAccesses(query, defaultTable, accesses); err != nil {
					return nil, fmt.Errorf("query %d: %w", i, err)
				}
			}
			return sortedTableAccesses(accesses), nil
		}
		if err := extractQueryTableAccesses(body, defaultTable, accesses); err != nil {
			return nil, err
		}
		return sortedTableAccesses(accesses), nil
	}

	for i, line := range splitNDJSON(body) {
		if err := extractQueryTableAccesses(line, defaultTable, accesses); err != nil {
			return nil, fmt.Errorf("line %d: %w", i, err)
		}
	}
	return sortedTableAccesses(accesses), nil
}

func injectRowFiltersIntoBody(body []byte, defaultTable string, filters map[string]json.RawMessage) ([]byte, error) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		if secFilter := resolveRowFilter(filters, defaultTable); secFilter != nil {
			obj := map[string]json.RawMessage{"filter_query": secFilter}
			return json.Marshal(obj)
		}
		return body, nil
	}

	lines := splitNDJSON(body)
	var out bytes.Buffer
	for i, line := range lines {
		merged, err := injectRowFiltersIntoQuery(line, defaultTable, filters)
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

func injectRowFiltersIntoAgentBody(body []byte, defaultTable string, filters map[string]json.RawMessage) ([]byte, error) {
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
		return injectRowFiltersIntoQuery(body, defaultTable, filters)
	}

	var queries []json.RawMessage
	if err := json.Unmarshal(queriesRaw, &queries); err != nil {
		return nil, fmt.Errorf("parse queries array: %w", err)
	}

	for i, q := range queries {
		merged, err := injectRowFiltersIntoQuery(q, defaultTable, filters)
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

	merged, err := mergeFilterRawMessage(obj["filter_query"], secFilter)
	if err != nil {
		return nil, err
	}
	obj["filter_query"] = merged

	return json.Marshal(obj)
}

func injectRowFiltersIntoQuery(objJSON []byte, defaultTable string, filters map[string]json.RawMessage) ([]byte, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(objJSON, &obj); err != nil {
		return nil, err
	}

	table := queryObjectTable(obj, defaultTable)
	if secFilter := resolveRowFilter(filters, table); secFilter != nil {
		merged, err := mergeFilterRawMessage(obj["filter_query"], secFilter)
		if err != nil {
			return nil, err
		}
		obj["filter_query"] = merged
	}

	if joinRaw, ok := obj["join"]; ok && !isNullOrEmpty(joinRaw) {
		updated, err := injectRowFiltersIntoJoin(joinRaw, filters)
		if err != nil {
			return nil, err
		}
		obj["join"] = updated
	}

	return json.Marshal(obj)
}

func injectRowFiltersIntoJoin(joinJSON []byte, filters map[string]json.RawMessage) (json.RawMessage, error) {
	var join map[string]json.RawMessage
	if err := json.Unmarshal(joinJSON, &join); err != nil {
		return nil, err
	}

	rightTable := stringField(join["right_table"])
	if secFilter := resolveRowFilter(filters, rightTable); secFilter != nil {
		var rightFilters map[string]json.RawMessage
		if raw := join["right_filters"]; !isNullOrEmpty(raw) {
			if err := json.Unmarshal(raw, &rightFilters); err != nil {
				return nil, fmt.Errorf("parse right_filters: %w", err)
			}
		}
		if rightFilters == nil {
			rightFilters = map[string]json.RawMessage{}
		}
		merged, err := mergeFilterRawMessage(rightFilters["filter_query"], secFilter)
		if err != nil {
			return nil, err
		}
		rightFilters["filter_query"] = merged
		encoded, err := json.Marshal(rightFilters)
		if err != nil {
			return nil, err
		}
		join["right_filters"] = encoded
	}

	if nestedRaw, ok := join["nested_join"]; ok && !isNullOrEmpty(nestedRaw) {
		updated, err := injectRowFiltersIntoJoin(nestedRaw, filters)
		if err != nil {
			return nil, fmt.Errorf("nested_join: %w", err)
		}
		join["nested_join"] = updated
	}

	encoded, err := json.Marshal(join)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

func mergeFilterRawMessage(existing json.RawMessage, secFilter json.RawMessage) (json.RawMessage, error) {
	if isNullOrEmpty(existing) {
		return secFilter, nil
	}
	conjunction, err := json.Marshal(map[string]interface{}{
		"conjuncts": []json.RawMessage{existing, secFilter},
	})
	if err != nil {
		return nil, err
	}
	return conjunction, nil
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
	if json.Valid(body) {
		return [][]byte{body}
	}
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

func extractQueryTableAccesses(objJSON []byte, defaultTable string, accesses map[string]TableAccess) error {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(objJSON, &obj); err != nil {
		return err
	}
	addTableAccess(accesses, queryObjectTable(obj, defaultTable), OperationRead)
	if joinRaw, ok := obj["join"]; ok && !isNullOrEmpty(joinRaw) {
		if err := extractJoinTableAccesses(joinRaw, accesses); err != nil {
			return fmt.Errorf("join: %w", err)
		}
	}
	return nil
}

func extractJoinTableAccesses(joinJSON []byte, accesses map[string]TableAccess) error {
	var join map[string]json.RawMessage
	if err := json.Unmarshal(joinJSON, &join); err != nil {
		return err
	}
	addTableAccess(accesses, stringField(join["right_table"]), OperationRead)
	if nestedRaw, ok := join["nested_join"]; ok && !isNullOrEmpty(nestedRaw) {
		if err := extractJoinTableAccesses(nestedRaw, accesses); err != nil {
			return fmt.Errorf("nested_join: %w", err)
		}
	}
	return nil
}

func addTableAccess(accesses map[string]TableAccess, table string, operation OperationKind) {
	table = strings.TrimSpace(table)
	if table == "" {
		return
	}
	if operation == "" {
		operation = OperationRead
	}
	accesses[table] = TableAccess{Table: table, Operation: operation}
}

func sortedTableAccesses(accesses map[string]TableAccess) []TableAccess {
	if len(accesses) == 0 {
		return nil
	}
	keys := make([]string, 0, len(accesses))
	for key := range accesses {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]TableAccess, 0, len(keys))
	for _, key := range keys {
		out = append(out, accesses[key])
	}
	return out
}

func queryObjectTable(obj map[string]json.RawMessage, defaultTable string) string {
	if table := stringField(obj["table"]); table != "" {
		return table
	}
	return defaultTable
}

func stringField(raw json.RawMessage) string {
	if isNullOrEmpty(raw) {
		return ""
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return ""
	}
	return strings.TrimSpace(value)
}
