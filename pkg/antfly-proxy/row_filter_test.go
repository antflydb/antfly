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
	"encoding/json"
	"testing"
)

func TestResolveRowFilter(t *testing.T) {
	filters := map[string]json.RawMessage{
		"docs": json.RawMessage(`{"term":{"dept":"eng"}}`),
		"*":    json.RawMessage(`{"term":{"active":"true"}}`),
	}

	t.Run("table specific", func(t *testing.T) {
		got := resolveRowFilter(filters, "docs")
		if got == nil {
			t.Fatal("expected filter for docs")
		}
		if string(got) != `{"term":{"dept":"eng"}}` {
			t.Fatalf("unexpected filter: %s", got)
		}
	})

	t.Run("wildcard fallback", func(t *testing.T) {
		got := resolveRowFilter(filters, "other_table")
		if got == nil {
			t.Fatal("expected wildcard filter")
		}
		if string(got) != `{"term":{"active":"true"}}` {
			t.Fatalf("unexpected filter: %s", got)
		}
	})

	t.Run("nil map", func(t *testing.T) {
		got := resolveRowFilter(nil, "docs")
		if got != nil {
			t.Fatalf("expected nil, got %s", got)
		}
	})

	t.Run("no match", func(t *testing.T) {
		got := resolveRowFilter(map[string]json.RawMessage{"docs": json.RawMessage(`{}`)}, "other")
		if got != nil {
			t.Fatalf("expected nil, got %s", got)
		}
	})
}

func TestInjectFilterIntoBody_SingleObject(t *testing.T) {
	body := []byte(`{"full_text_search":{"query":"hello"}}`)
	secFilter := json.RawMessage(`{"term":{"dept":"eng"}}`)

	result, err := injectFilterIntoBody(body, secFilter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsed map[string]json.RawMessage
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("parse result: %v", err)
	}
	if string(parsed["filter_query"]) != `{"term":{"dept":"eng"}}` {
		t.Fatalf("unexpected filter_query: %s", parsed["filter_query"])
	}
	// Original field preserved
	if _, ok := parsed["full_text_search"]; !ok {
		t.Fatal("full_text_search field missing")
	}
}

func TestInjectFilterIntoBody_ExistingFilter(t *testing.T) {
	body := []byte(`{"filter_query":{"term":{"status":"active"}},"full_text_search":{"query":"hello"}}`)
	secFilter := json.RawMessage(`{"term":{"dept":"eng"}}`)

	result, err := injectFilterIntoBody(body, secFilter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsed map[string]json.RawMessage
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("parse result: %v", err)
	}

	// Should be a conjunction
	var conjunction map[string][]json.RawMessage
	if err := json.Unmarshal(parsed["filter_query"], &conjunction); err != nil {
		t.Fatalf("parse conjunction: %v", err)
	}
	if len(conjunction["conjuncts"]) != 2 {
		t.Fatalf("expected 2 conjuncts, got %d", len(conjunction["conjuncts"]))
	}
}

func TestInjectFilterIntoBody_NDJSON(t *testing.T) {
	body := []byte("{\"full_text_search\":{\"query\":\"a\"}}\n{\"full_text_search\":{\"query\":\"b\"}}")
	secFilter := json.RawMessage(`{"term":{"dept":"eng"}}`)

	result, err := injectFilterIntoBody(body, secFilter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	lines := splitNDJSON(result)
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}

	for i, line := range lines {
		var parsed map[string]json.RawMessage
		if err := json.Unmarshal(line, &parsed); err != nil {
			t.Fatalf("line %d parse: %v", i, err)
		}
		if string(parsed["filter_query"]) != `{"term":{"dept":"eng"}}` {
			t.Fatalf("line %d unexpected filter_query: %s", i, parsed["filter_query"])
		}
	}
}

func TestInjectFilterIntoBody_EmptyBody(t *testing.T) {
	secFilter := json.RawMessage(`{"term":{"dept":"eng"}}`)

	result, err := injectFilterIntoBody(nil, secFilter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsed map[string]json.RawMessage
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("parse result: %v", err)
	}
	if string(parsed["filter_query"]) != `{"term":{"dept":"eng"}}` {
		t.Fatalf("unexpected filter_query: %s", parsed["filter_query"])
	}
}

func TestInjectFilterIntoAgentBody(t *testing.T) {
	body := []byte(`{"queries":[{"full_text_search":{"query":"a"}},{"filter_query":{"term":{"status":"active"}},"full_text_search":{"query":"b"}}]}`)
	secFilter := json.RawMessage(`{"term":{"dept":"eng"}}`)

	result, err := injectFilterIntoAgentBody(body, secFilter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsed struct {
		Queries []map[string]json.RawMessage `json:"queries"`
	}
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("parse result: %v", err)
	}

	if len(parsed.Queries) != 2 {
		t.Fatalf("expected 2 queries, got %d", len(parsed.Queries))
	}

	// First query: no existing filter, should just be the sec filter
	if string(parsed.Queries[0]["filter_query"]) != `{"term":{"dept":"eng"}}` {
		t.Fatalf("query 0 unexpected filter_query: %s", parsed.Queries[0]["filter_query"])
	}

	// Second query: had existing filter, should be a conjunction
	var conjunction map[string][]json.RawMessage
	if err := json.Unmarshal(parsed.Queries[1]["filter_query"], &conjunction); err != nil {
		t.Fatalf("parse conjunction: %v", err)
	}
	if len(conjunction["conjuncts"]) != 2 {
		t.Fatalf("expected 2 conjuncts, got %d", len(conjunction["conjuncts"]))
	}
}

func TestInjectFilterIntoAgentBody_NoQueriesField(t *testing.T) {
	body := []byte(`{"full_text_search":{"query":"hello"}}`)
	secFilter := json.RawMessage(`{"term":{"dept":"eng"}}`)

	result, err := injectFilterIntoAgentBody(body, secFilter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsed map[string]json.RawMessage
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("parse result: %v", err)
	}
	if string(parsed["filter_query"]) != `{"term":{"dept":"eng"}}` {
		t.Fatalf("unexpected filter_query: %s", parsed["filter_query"])
	}
}

func TestExtractTableAccessesFromBody_JoinAndNestedJoin(t *testing.T) {
	body := []byte(`{
		"table": "orders",
		"join": {
			"right_table": "customers",
			"on": {"left_field": "customer_id", "right_field": "id"},
			"nested_join": {
				"right_table": "addresses",
				"on": {"left_field": "customers.address_id", "right_field": "id"}
			}
		}
	}`)

	accesses, err := extractTableAccessesFromBody(body, "orders", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := make([]string, 0, len(accesses))
	for _, access := range accesses {
		got = append(got, access.Table)
	}
	want := []string{"addresses", "customers", "orders"}
	if len(got) != len(want) {
		t.Fatalf("got accesses %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got accesses %v want %v", got, want)
		}
	}
}

func TestInjectRowFiltersIntoBody_AppliesPerTableJoinFilters(t *testing.T) {
	body := []byte(`{
		"table": "orders",
		"filter_query": {"query": "status:pending"},
		"join": {
			"right_table": "customers",
			"right_filters": {"filter_query": {"query": "tier:premium"}},
			"nested_join": {
				"right_table": "addresses"
			}
		}
	}`)
	filters := map[string]json.RawMessage{
		"orders":    json.RawMessage(`{"term":{"tenant_id":"t1"}}`),
		"customers": json.RawMessage(`{"term":{"region":"na"}}`),
		"addresses": json.RawMessage(`{"term":{"country":"us"}}`),
	}

	result, err := injectRowFiltersIntoBody(body, "orders", filters)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsed map[string]json.RawMessage
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("parse result: %v", err)
	}
	var primary map[string][]json.RawMessage
	if err := json.Unmarshal(parsed["filter_query"], &primary); err != nil {
		t.Fatalf("parse primary filter: %v", err)
	}
	if len(primary["conjuncts"]) != 2 {
		t.Fatalf("expected primary conjunction, got %s", parsed["filter_query"])
	}

	var join map[string]json.RawMessage
	if err := json.Unmarshal(parsed["join"], &join); err != nil {
		t.Fatalf("parse join: %v", err)
	}
	var rightFilters map[string]json.RawMessage
	if err := json.Unmarshal(join["right_filters"], &rightFilters); err != nil {
		t.Fatalf("parse right filters: %v", err)
	}
	var right map[string][]json.RawMessage
	if err := json.Unmarshal(rightFilters["filter_query"], &right); err != nil {
		t.Fatalf("parse right filter: %v", err)
	}
	if len(right["conjuncts"]) != 2 {
		t.Fatalf("expected right conjunction, got %s", rightFilters["filter_query"])
	}

	var nested map[string]json.RawMessage
	if err := json.Unmarshal(join["nested_join"], &nested); err != nil {
		t.Fatalf("parse nested join: %v", err)
	}
	var nestedFilters map[string]json.RawMessage
	if err := json.Unmarshal(nested["right_filters"], &nestedFilters); err != nil {
		t.Fatalf("parse nested right filters: %v", err)
	}
	if string(nestedFilters["filter_query"]) != `{"term":{"country":"us"}}` {
		t.Fatalf("unexpected nested filter: %s", nestedFilters["filter_query"])
	}
}

func TestInjectRowFiltersIntoAgentBody_UsesPerQueryTable(t *testing.T) {
	body := []byte(`{"queries":[{"table":"orders"},{"table":"customers","filter_query":{"query":"active:true"}}]}`)
	filters := map[string]json.RawMessage{
		"orders":    json.RawMessage(`{"term":{"tenant_id":"t1"}}`),
		"customers": json.RawMessage(`{"term":{"region":"na"}}`),
	}

	result, err := injectRowFiltersIntoAgentBody(body, "orders", filters)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsed struct {
		Queries []map[string]json.RawMessage `json:"queries"`
	}
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("parse result: %v", err)
	}
	if string(parsed.Queries[0]["filter_query"]) != `{"term":{"tenant_id":"t1"}}` {
		t.Fatalf("unexpected orders filter: %s", parsed.Queries[0]["filter_query"])
	}
	var conjunction map[string][]json.RawMessage
	if err := json.Unmarshal(parsed.Queries[1]["filter_query"], &conjunction); err != nil {
		t.Fatalf("parse customers conjunction: %v", err)
	}
	if len(conjunction["conjuncts"]) != 2 {
		t.Fatalf("expected customers conjunction, got %s", parsed.Queries[1]["filter_query"])
	}
}

func TestParseBearerTokensJSON_WithRowFilter(t *testing.T) {
	raw := `{
		"tok_acme": {
			"subject": "acme-api",
			"tenant": "acme",
			"tables": ["docs"],
			"row_filter": {
				"docs": {"term": {"tenant_id": "acme"}}
			}
		}
	}`

	tokens, err := parseBearerTokensJSON(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	principal, ok := tokens["tok_acme"]
	if !ok {
		t.Fatal("expected tok_acme principal")
	}
	if len(principal.RowFilter) != 1 {
		t.Fatalf("expected 1 row filter entry, got %d", len(principal.RowFilter))
	}
	filter, ok := principal.RowFilter["docs"]
	if !ok {
		t.Fatal("expected row filter for docs table")
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(filter, &parsed); err != nil {
		t.Fatalf("parse filter: %v", err)
	}
	term, ok := parsed["term"].(map[string]interface{})
	if !ok {
		t.Fatal("expected term query")
	}
	if term["tenant_id"] != "acme" {
		t.Fatalf("unexpected tenant_id: %v", term["tenant_id"])
	}
}

func TestIsAgentPath(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{"/agents/retrieval", true},
		{"/agents", true},
		{"/query/search", false},
		{"/graph/neighbors", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := isAgentPath(tt.path); got != tt.want {
			t.Errorf("isAgentPath(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}
