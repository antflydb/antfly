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

package evaluator

import (
	"encoding/json"
	"testing"
)

func TestParseFilter_Empty(t *testing.T) {
	node, err := ParseFilter(nil)
	if err != nil {
		t.Fatalf("ParseFilter(nil) error: %v", err)
	}
	if _, ok := node.(*MatchAllNode); !ok {
		t.Errorf("ParseFilter(nil) = %T, want *MatchAllNode", node)
	}
	matched, err := node.Evaluate(map[string]any{"x": 1})
	if err != nil || !matched {
		t.Errorf("MatchAllNode.Evaluate() = (%v, %v), want (true, nil)", matched, err)
	}
}

func TestParseFilter_MatchAll(t *testing.T) {
	node, err := ParseFilter(json.RawMessage(`{"match_all": {}}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := node.(*MatchAllNode); !ok {
		t.Errorf("got %T, want *MatchAllNode", node)
	}
}

func TestParseFilter_MatchNone(t *testing.T) {
	node, err := ParseFilter(json.RawMessage(`{"match_none": {}}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := node.(*MatchNoneNode); !ok {
		t.Errorf("got %T, want *MatchNoneNode", node)
	}
	matched, err := node.Evaluate(map[string]any{"x": 1})
	if err != nil || matched {
		t.Errorf("MatchNoneNode.Evaluate() = (%v, %v), want (false, nil)", matched, err)
	}
}

func TestParseFilter_Term(t *testing.T) {
	node, err := ParseFilter(json.RawMessage(`{"term": "active", "field": "status"}`))
	if err != nil {
		t.Fatal(err)
	}
	tn, ok := node.(*TermNode)
	if !ok {
		t.Fatalf("got %T, want *TermNode", node)
	}
	if tn.Field != "status" || tn.Value != "active" {
		t.Errorf("TermNode = {%q, %v}, want {status, active}", tn.Field, tn.Value)
	}

	// Match
	matched, _ := node.Evaluate(map[string]any{"status": "active"})
	if !matched {
		t.Error("expected match for status=active")
	}
	// No match
	matched, _ = node.Evaluate(map[string]any{"status": "inactive"})
	if matched {
		t.Error("expected no match for status=inactive")
	}
	// Missing field
	matched, _ = node.Evaluate(map[string]any{"other": "active"})
	if matched {
		t.Error("expected no match for missing field")
	}
}

func TestParseFilter_TermNumeric(t *testing.T) {
	node, err := ParseFilter(json.RawMessage(`{"term": 42, "field": "count"}`))
	if err != nil {
		t.Fatal(err)
	}
	// JSON numbers decode as float64
	matched, _ := node.Evaluate(map[string]any{"count": float64(42)})
	if !matched {
		t.Error("expected match for count=42")
	}
	matched, _ = node.Evaluate(map[string]any{"count": int(42)})
	if !matched {
		t.Error("expected match for count=int(42) via cross-type equality")
	}
}

func TestParseFilter_Match(t *testing.T) {
	node, err := ParseFilter(json.RawMessage(`{"match": "hello", "field": "text"}`))
	if err != nil {
		t.Fatal(err)
	}
	matched, _ := node.Evaluate(map[string]any{"text": "hello"})
	if !matched {
		t.Error("expected match")
	}
	matched, _ = node.Evaluate(map[string]any{"text": "world"})
	if matched {
		t.Error("expected no match")
	}
}

func TestParseFilter_Prefix(t *testing.T) {
	node, err := ParseFilter(json.RawMessage(`{"prefix": "hel", "field": "text"}`))
	if err != nil {
		t.Fatal(err)
	}
	matched, _ := node.Evaluate(map[string]any{"text": "hello"})
	if !matched {
		t.Error("expected match for hello with prefix hel")
	}
	matched, _ = node.Evaluate(map[string]any{"text": "world"})
	if matched {
		t.Error("expected no match for world with prefix hel")
	}
	// Non-string field
	matched, _ = node.Evaluate(map[string]any{"text": 123})
	if matched {
		t.Error("expected no match for non-string field")
	}
}

func TestParseFilter_Wildcard(t *testing.T) {
	node, err := ParseFilter(json.RawMessage(`{"wildcard": "h*o", "field": "text"}`))
	if err != nil {
		t.Fatal(err)
	}
	matched, _ := node.Evaluate(map[string]any{"text": "hello"})
	if !matched {
		t.Error("expected match for hello with h*o")
	}
	matched, _ = node.Evaluate(map[string]any{"text": "ho"})
	if !matched {
		t.Error("expected match for ho with h*o")
	}
	matched, _ = node.Evaluate(map[string]any{"text": "world"})
	if matched {
		t.Error("expected no match for world with h*o")
	}
}

func TestParseFilter_Range(t *testing.T) {
	node, err := ParseFilter(json.RawMessage(`{"field": "age", "min": 18, "max": 65}`))
	if err != nil {
		t.Fatal(err)
	}
	rn := node.(*RangeNode)
	if !rn.InclusiveMin || !rn.InclusiveMax {
		t.Error("expected default inclusive bounds")
	}

	matched, _ := node.Evaluate(map[string]any{"age": float64(30)})
	if !matched {
		t.Error("expected match for age=30")
	}
	matched, _ = node.Evaluate(map[string]any{"age": float64(18)})
	if !matched {
		t.Error("expected match for age=18 (inclusive)")
	}
	matched, _ = node.Evaluate(map[string]any{"age": float64(17)})
	if matched {
		t.Error("expected no match for age=17")
	}
	matched, _ = node.Evaluate(map[string]any{"age": float64(66)})
	if matched {
		t.Error("expected no match for age=66")
	}
}

func TestParseFilter_RangeExclusive(t *testing.T) {
	node, err := ParseFilter(json.RawMessage(`{"field": "x", "min": 10, "inclusive_min": false, "max": 20, "inclusive_max": false}`))
	if err != nil {
		t.Fatal(err)
	}
	matched, _ := node.Evaluate(map[string]any{"x": float64(10)})
	if matched {
		t.Error("expected no match for x=10 (exclusive)")
	}
	matched, _ = node.Evaluate(map[string]any{"x": float64(15)})
	if !matched {
		t.Error("expected match for x=15")
	}
	matched, _ = node.Evaluate(map[string]any{"x": float64(20)})
	if matched {
		t.Error("expected no match for x=20 (exclusive)")
	}
}

func TestParseFilter_Conjuncts(t *testing.T) {
	filter := json.RawMessage(`{"conjuncts": [
		{"term": "active", "field": "status"},
		{"term": "admin", "field": "role"}
	]}`)
	node, err := ParseFilter(filter)
	if err != nil {
		t.Fatal(err)
	}

	matched, _ := node.Evaluate(map[string]any{"status": "active", "role": "admin"})
	if !matched {
		t.Error("expected match for both conditions met")
	}
	matched, _ = node.Evaluate(map[string]any{"status": "active", "role": "user"})
	if matched {
		t.Error("expected no match when role doesn't match")
	}
}

func TestParseFilter_Disjuncts(t *testing.T) {
	filter := json.RawMessage(`{"disjuncts": [
		{"term": "admin", "field": "role"},
		{"term": "superuser", "field": "role"}
	]}`)
	node, err := ParseFilter(filter)
	if err != nil {
		t.Fatal(err)
	}

	matched, _ := node.Evaluate(map[string]any{"role": "admin"})
	if !matched {
		t.Error("expected match for admin")
	}
	matched, _ = node.Evaluate(map[string]any{"role": "superuser"})
	if !matched {
		t.Error("expected match for superuser")
	}
	matched, _ = node.Evaluate(map[string]any{"role": "user"})
	if matched {
		t.Error("expected no match for user")
	}
}

func TestParseFilter_MustNot(t *testing.T) {
	filter := json.RawMessage(`{"must_not": {"term": "deleted", "field": "status"}}`)
	node, err := ParseFilter(filter)
	if err != nil {
		t.Fatal(err)
	}

	matched, _ := node.Evaluate(map[string]any{"status": "active"})
	if !matched {
		t.Error("expected match for status=active (not deleted)")
	}
	matched, _ = node.Evaluate(map[string]any{"status": "deleted"})
	if matched {
		t.Error("expected no match for status=deleted")
	}
}

func TestParseFilter_QueryString(t *testing.T) {
	filter := json.RawMessage(`{"query": "status:active OR role:admin"}`)
	node, err := ParseFilter(filter)
	if err != nil {
		t.Fatal(err)
	}

	matched, _ := node.Evaluate(map[string]any{"status": "active", "role": "user"})
	if !matched {
		t.Error("expected match for status=active")
	}
	matched, _ = node.Evaluate(map[string]any{"status": "inactive", "role": "admin"})
	if !matched {
		t.Error("expected match for role=admin")
	}
	matched, _ = node.Evaluate(map[string]any{"status": "inactive", "role": "user"})
	if matched {
		t.Error("expected no match")
	}
}

func TestParseFilter_QueryStringWildcard(t *testing.T) {
	filter := json.RawMessage(`{"query": "*"}`)
	node, err := ParseFilter(filter)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := node.(*MatchAllNode); !ok {
		t.Errorf("query '*' should parse to MatchAllNode, got %T", node)
	}
}

func TestParseFilter_Unsupported(t *testing.T) {
	_, err := ParseFilter(json.RawMessage(`{"unknown_key": "value"}`))
	if err == nil {
		t.Error("expected error for unsupported filter type")
	}
}

func TestMatchWildcard(t *testing.T) {
	tests := []struct {
		pattern, s string
		want       bool
	}{
		{"*", "anything", true},
		{"*", "", true},
		{"?", "x", true},
		{"?", "", false},
		{"?", "xy", false},
		{"h*o", "hello", true},
		{"h*o", "ho", true},
		{"h*o", "h", false},
		{"h?llo", "hello", true},
		{"h?llo", "hallo", true},
		{"h?llo", "hllo", false},
		{"a*b*c", "abc", true},
		{"a*b*c", "aXbYc", true},
		{"a*b*c", "aXbY", false},
		{"", "", true},
		{"", "x", false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"/"+tt.s, func(t *testing.T) {
			got := MatchWildcard(tt.pattern, tt.s)
			if got != tt.want {
				t.Errorf("MatchWildcard(%q, %q) = %v, want %v", tt.pattern, tt.s, got, tt.want)
			}
		})
	}
}

func TestSplitFieldValue(t *testing.T) {
	tests := []struct {
		input      string
		field, val string
		ok         bool
	}{
		{"status:active", "status", "active", true},
		{"field:val:ue", "field", "val:ue", true},
		{"no_colon", "", "", false},
		{`esc\:aped:value`, `esc\:aped`, "value", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			field, val, ok := SplitFieldValue(tt.input)
			if ok != tt.ok || field != tt.field || val != tt.val {
				t.Errorf("SplitFieldValue(%q) = (%q, %q, %v), want (%q, %q, %v)",
					tt.input, field, val, ok, tt.field, tt.val, tt.ok)
			}
		})
	}
}

func TestUnescapeQueryString(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"hello", "hello"},
		{`hello\ world`, "hello world"},
		{`a\:b`, "a:b"},
		{`trailing\\`, `trailing\`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := UnescapeQueryString(tt.input)
			if got != tt.want {
				t.Errorf("UnescapeQueryString(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
