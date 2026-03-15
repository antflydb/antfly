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

package foreign

import (
	"encoding/json"
	"testing"

	"github.com/antflydb/antfly/lib/evaluator"
)

func TestEvaluateFilter_Empty(t *testing.T) {
	matched, err := EvaluateFilter(nil, map[string]any{"a": 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Error("nil filter should match (match-all)")
	}

	matched, err = EvaluateFilter(json.RawMessage{}, map[string]any{"a": 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Error("empty filter should match (match-all)")
	}
}

func TestEvaluateFilter_MatchAll(t *testing.T) {
	matched, err := EvaluateFilter(json.RawMessage(`{"match_all": {}}`), map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Error("match_all should match")
	}
}

func TestEvaluateFilter_MatchNone(t *testing.T) {
	matched, err := EvaluateFilter(json.RawMessage(`{"match_none": {}}`), map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Error("match_none should not match")
	}
}

func TestEvaluateFilter_Term(t *testing.T) {
	filter := json.RawMessage(`{"term": "active", "field": "status"}`)

	// Match
	matched, err := EvaluateFilter(filter, map[string]any{"status": "active"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Error("expected match for status=active")
	}

	// No match
	matched, err = EvaluateFilter(filter, map[string]any{"status": "inactive"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Error("expected no match for status=inactive")
	}
}

func TestEvaluateFilter_TermNumeric(t *testing.T) {
	filter := json.RawMessage(`{"term": 42, "field": "count"}`)

	// JSON numbers decode to float64, doc value is int — should match via numeric coercion
	matched, err := EvaluateFilter(filter, map[string]any{"count": int64(42)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Error("expected numeric match (float64 42 == int64 42)")
	}

	// No match
	matched, err = EvaluateFilter(filter, map[string]any{"count": int64(99)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Error("expected no match for count=99")
	}
}

func TestEvaluateFilter_TermBool(t *testing.T) {
	filter := json.RawMessage(`{"term": true, "field": "active"}`)

	matched, err := EvaluateFilter(filter, map[string]any{"active": true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Error("expected match for active=true")
	}

	matched, err = EvaluateFilter(filter, map[string]any{"active": false})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Error("expected no match for active=false")
	}
}

func TestEvaluateFilter_Match(t *testing.T) {
	filter := json.RawMessage(`{"match": "alice", "field": "name"}`)

	matched, err := EvaluateFilter(filter, map[string]any{"name": "alice"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Error("expected match for name=alice")
	}

	matched, err = EvaluateFilter(filter, map[string]any{"name": "bob"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Error("expected no match for name=bob")
	}
}

func TestEvaluateFilter_Prefix(t *testing.T) {
	filter := json.RawMessage(`{"prefix": "ali", "field": "name"}`)

	matched, err := EvaluateFilter(filter, map[string]any{"name": "alice"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Error("expected match for prefix ali in alice")
	}

	matched, err = EvaluateFilter(filter, map[string]any{"name": "bob"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Error("expected no match for prefix ali in bob")
	}

	// Non-string value should not match
	matched, err = EvaluateFilter(filter, map[string]any{"name": 123})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Error("expected no match for prefix on non-string value")
	}
}

func TestEvaluateFilter_Wildcard(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		value   string
		want    bool
	}{
		{"star match", `al*ce`, "alice", true},
		{"star match multi", `al*`, "alice", true},
		{"star no match", `al*ce`, "bob", false},
		{"question match", `v?lue`, "value", true},
		{"question no match", `v?lue`, "vaalue", false},
		{"star and question", `a*b?c`, "axyzbzc", true},
		{"star and question no match", `a*b?c`, "axyzbd", false},
		{"all star", `*`, "anything", true},
		{"empty pattern empty string", ``, "", true},
		{"star matches empty", `*`, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := json.RawMessage(`{"wildcard": "` + tt.pattern + `", "field": "val"}`)
			matched, err := EvaluateFilter(filter, map[string]any{"val": tt.value})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if matched != tt.want {
				t.Errorf("wildcard %q vs %q: got %v, want %v", tt.pattern, tt.value, matched, tt.want)
			}
		})
	}
}

func TestEvaluateFilter_Range(t *testing.T) {
	// Inclusive range [18, 65]
	filter := json.RawMessage(`{"field": "age", "min": 18, "max": 65}`)

	tests := []struct {
		name string
		age  any
		want bool
	}{
		{"below min", float64(17), false},
		{"at min", float64(18), true},
		{"in range", float64(30), true},
		{"at max", float64(65), true},
		{"above max", float64(66), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matched, err := EvaluateFilter(filter, map[string]any{"age": tt.age})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if matched != tt.want {
				t.Errorf("age=%v: got %v, want %v", tt.age, matched, tt.want)
			}
		})
	}
}

func TestEvaluateFilter_RangeExclusive(t *testing.T) {
	filter := json.RawMessage(`{"field": "age", "min": 18, "inclusive_min": false, "max": 65, "inclusive_max": false}`)

	tests := []struct {
		name string
		age  any
		want bool
	}{
		{"at min exclusive", float64(18), false},
		{"above min", float64(19), true},
		{"at max exclusive", float64(65), false},
		{"below max", float64(64), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matched, err := EvaluateFilter(filter, map[string]any{"age": tt.age})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if matched != tt.want {
				t.Errorf("age=%v: got %v, want %v", tt.age, matched, tt.want)
			}
		})
	}
}

func TestEvaluateFilter_RangeMinOnly(t *testing.T) {
	filter := json.RawMessage(`{"field": "score", "min": 4.0, "inclusive_min": true}`)

	matched, err := EvaluateFilter(filter, map[string]any{"score": float64(4.0)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Error("score=4.0 should match min=4.0 inclusive")
	}

	matched, err = EvaluateFilter(filter, map[string]any{"score": float64(3.9)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Error("score=3.9 should not match min=4.0")
	}
}

func TestEvaluateFilter_BooleanConjuncts(t *testing.T) {
	filter := json.RawMessage(`{
		"conjuncts": [
			{"term": "active", "field": "status"},
			{"min": 18, "field": "age"}
		]
	}`)

	// Both match
	matched, err := EvaluateFilter(filter, map[string]any{"status": "active", "age": float64(25)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Error("expected match when both conjuncts match")
	}

	// First matches, second doesn't
	matched, err = EvaluateFilter(filter, map[string]any{"status": "active", "age": float64(10)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Error("expected no match when second conjunct fails")
	}
}

func TestEvaluateFilter_BooleanDisjuncts(t *testing.T) {
	filter := json.RawMessage(`{
		"disjuncts": [
			{"term": "active", "field": "status"},
			{"term": "pending", "field": "status"}
		]
	}`)

	// First matches
	matched, err := EvaluateFilter(filter, map[string]any{"status": "active"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Error("expected match when first disjunct matches")
	}

	// Neither matches
	matched, err = EvaluateFilter(filter, map[string]any{"status": "deleted"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Error("expected no match when no disjunct matches")
	}
}

func TestEvaluateFilter_MustNot(t *testing.T) {
	filter := json.RawMessage(`{
		"conjuncts": [{"term": "active", "field": "status"}],
		"must_not": {"term": "admin", "field": "role"}
	}`)

	// Matches conjuncts, doesn't match must_not → true
	matched, err := EvaluateFilter(filter, map[string]any{"status": "active", "role": "user"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Error("expected match: conjunct matches, must_not doesn't")
	}

	// Matches conjuncts AND must_not → false
	matched, err = EvaluateFilter(filter, map[string]any{"status": "active", "role": "admin"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Error("expected no match: must_not matches")
	}
}

func TestEvaluateFilter_MustNotOnly(t *testing.T) {
	// Bare must_not without conjuncts/disjuncts should act as "NOT (inner)"
	filter := json.RawMessage(`{"must_not": {"term": "deleted", "field": "status"}}`)

	// Non-matching must_not → true
	matched, err := EvaluateFilter(filter, map[string]any{"status": "active"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Error("expected match: must_not doesn't match (status=active)")
	}

	// Matching must_not → false
	matched, err = EvaluateFilter(filter, map[string]any{"status": "deleted"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Error("expected no match: must_not matches (status=deleted)")
	}
}

func TestEvaluateFilter_QueryStringSingle(t *testing.T) {
	filter := json.RawMessage(`{"query": "status:active"}`)

	matched, err := EvaluateFilter(filter, map[string]any{"status": "active"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Error("expected match for status:active")
	}

	matched, err = EvaluateFilter(filter, map[string]any{"status": "inactive"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Error("expected no match for status:inactive")
	}
}

func TestEvaluateFilter_QueryStringOR(t *testing.T) {
	filter := json.RawMessage(`{"query": "status:active OR status:pending"}`)

	matched, err := EvaluateFilter(filter, map[string]any{"status": "pending"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Error("expected match for status=pending with OR query")
	}

	matched, err = EvaluateFilter(filter, map[string]any{"status": "deleted"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Error("expected no match for status=deleted with OR query")
	}
}

func TestEvaluateFilter_QueryStringMatchAll(t *testing.T) {
	matched, err := EvaluateFilter(json.RawMessage(`{"query": "*"}`), map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !matched {
		t.Error("query * should match all")
	}
}

func TestEvaluateFilter_MissingField(t *testing.T) {
	filter := json.RawMessage(`{"term": "active", "field": "status"}`)

	// Field not present in doc → no match, no error
	matched, err := EvaluateFilter(filter, map[string]any{"name": "alice"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Error("expected no match when field is missing from doc")
	}
}

func TestEvaluateFilter_NilFieldValue(t *testing.T) {
	filter := json.RawMessage(`{"term": "active", "field": "status"}`)

	// Field present but nil → no match
	matched, err := EvaluateFilter(filter, map[string]any{"status": nil})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Error("expected no match when field value is nil")
	}
}

func TestEvaluateFilter_EmptyDoc(t *testing.T) {
	filter := json.RawMessage(`{"term": "active", "field": "status"}`)

	matched, err := EvaluateFilter(filter, map[string]any{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if matched {
		t.Error("expected no match on empty doc")
	}
}

func TestEvaluateFilter_InvalidJSON(t *testing.T) {
	_, err := EvaluateFilter(json.RawMessage(`{not json}`), map[string]any{})
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestEvaluateFilter_UnsupportedType(t *testing.T) {
	_, err := EvaluateFilter(json.RawMessage(`{"unknown_op": true}`), map[string]any{})
	if err == nil {
		t.Error("expected error for unsupported filter type")
	}
}

func TestMatchWildcard(t *testing.T) {
	tests := []struct {
		pattern string
		s       string
		want    bool
	}{
		{"", "", true},
		{"*", "", true},
		{"*", "abc", true},
		{"?", "a", true},
		{"?", "", false},
		{"?", "ab", false},
		{"a*b", "ab", true},
		{"a*b", "axb", true},
		{"a*b", "axyzb", true},
		{"a*b", "axyzc", false},
		{"a?c", "abc", true},
		{"a?c", "ac", false},
		{"a*b*c", "abc", true},
		{"a*b*c", "aXbYc", true},
		{"a*b*c", "aXbYd", false},
		{"hello", "hello", true},
		{"hello", "world", false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.s, func(t *testing.T) {
			got := evaluator.MatchWildcard(tt.pattern, tt.s)
			if got != tt.want {
				t.Errorf("evaluator.MatchWildcard(%q, %q) = %v, want %v", tt.pattern, tt.s, got, tt.want)
			}
		})
	}
}
