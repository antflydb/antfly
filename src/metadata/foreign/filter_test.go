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
	"fmt"
	"testing"

	"github.com/antflydb/antfly/lib/evaluator"
)

func pgPlaceholder(n int) string { return fmt.Sprintf("$%d", n) }

var testColumns = []ForeignColumn{
	{Name: "id", Type: "integer"},
	{Name: "name", Type: "text"},
	{Name: "email", Type: "text"},
	{Name: "age", Type: "integer"},
	{Name: "status", Type: "text"},
	{Name: "created_at", Type: "timestamp"},
}

func TestTranslateFilter_Empty(t *testing.T) {
	where, args, err := TranslateFilter(nil, pgPlaceholder, testColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if where != "" {
		t.Errorf("expected empty WHERE, got %q", where)
	}
	if len(args) != 0 {
		t.Errorf("expected no args, got %v", args)
	}
}

func TestTranslateFilter_MatchAll(t *testing.T) {
	where, args, err := TranslateFilter(json.RawMessage(`{"match_all": {}}`), pgPlaceholder, testColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if where != "" {
		t.Errorf("expected empty WHERE for match_all, got %q", where)
	}
	if len(args) != 0 {
		t.Errorf("expected no args, got %v", args)
	}
}

func TestTranslateFilter_Term(t *testing.T) {
	where, args, err := TranslateFilter(
		json.RawMessage(`{"term": "active", "field": "status"}`),
		pgPlaceholder, testColumns,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if where != `"status" = $1` {
		t.Errorf("expected '\"status\" = $1', got %q", where)
	}
	if len(args) != 1 || args[0] != "active" {
		t.Errorf("expected args=[active], got %v", args)
	}
}

func TestTranslateFilter_Match(t *testing.T) {
	where, args, err := TranslateFilter(
		json.RawMessage(`{"match": "alice", "field": "name"}`),
		pgPlaceholder, testColumns,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if where != `"name" = $1` {
		t.Errorf("expected '\"name\" = $1', got %q", where)
	}
	if len(args) != 1 || args[0] != "alice" {
		t.Errorf("expected args=[alice], got %v", args)
	}
}

func TestTranslateFilter_Range(t *testing.T) {
	where, args, err := TranslateFilter(
		json.RawMessage(`{"field": "age", "min": 18, "max": 65}`),
		pgPlaceholder, testColumns,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `"age" >= $1 AND "age" <= $2`
	if where != expected {
		t.Errorf("expected %q, got %q", expected, where)
	}
	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(args))
	}
	if args[0] != float64(18) || args[1] != float64(65) {
		t.Errorf("expected args=[18, 65], got %v", args)
	}
}

func TestTranslateFilter_RangeExclusive(t *testing.T) {
	where, args, err := TranslateFilter(
		json.RawMessage(`{"field": "age", "min": 18, "inclusive_min": false, "max": 65, "inclusive_max": false}`),
		pgPlaceholder, testColumns,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `"age" > $1 AND "age" < $2`
	if where != expected {
		t.Errorf("expected %q, got %q", expected, where)
	}
	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(args))
	}
}

func TestTranslateFilter_BooleanConjuncts(t *testing.T) {
	filter := `{
		"conjuncts": [
			{"term": "active", "field": "status"},
			{"field": "age", "min": 21}
		]
	}`
	where, args, err := TranslateFilter(json.RawMessage(filter), pgPlaceholder, testColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `("status" = $1) AND ("age" >= $2)`
	if where != expected {
		t.Errorf("expected %q, got %q", expected, where)
	}
	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(args))
	}
}

func TestTranslateFilter_BooleanDisjuncts(t *testing.T) {
	filter := `{
		"disjuncts": [
			{"term": "active", "field": "status"},
			{"term": "pending", "field": "status"}
		]
	}`
	where, args, err := TranslateFilter(json.RawMessage(filter), pgPlaceholder, testColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `(("status" = $1) OR ("status" = $2))`
	if where != expected {
		t.Errorf("expected %q, got %q", expected, where)
	}
	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(args))
	}
}

func TestTranslateFilter_MustNot(t *testing.T) {
	filter := `{
		"conjuncts": [
			{"term": "active", "field": "status"}
		],
		"must_not": {"term": "deleted", "field": "status"}
	}`
	where, args, err := TranslateFilter(json.RawMessage(filter), pgPlaceholder, testColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `("status" = $1) AND NOT ("status" = $2)`
	if where != expected {
		t.Errorf("expected %q, got %q", expected, where)
	}
	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(args))
	}
}

func TestTranslateFilter_QueryStringSingle(t *testing.T) {
	where, args, err := TranslateFilter(
		json.RawMessage(`{"query": "status:active"}`),
		pgPlaceholder, testColumns,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if where != `"status" = $1` {
		t.Errorf("expected '\"status\" = $1', got %q", where)
	}
	if len(args) != 1 || args[0] != "active" {
		t.Errorf("expected args=[active], got %v", args)
	}
}

func TestTranslateFilter_QueryStringOR(t *testing.T) {
	// This is the format produced by buildTermsQuery in api_join.go
	where, args, err := TranslateFilter(
		json.RawMessage(`{"query": "id:1 OR id:2 OR id:3"}`),
		pgPlaceholder, testColumns,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `"id" IN ($1, $2, $3)`
	if where != expected {
		t.Errorf("expected %q, got %q", expected, where)
	}
	if len(args) != 3 {
		t.Fatalf("expected 3 args, got %d", len(args))
	}
}

func TestTranslateFilter_QueryStringMatchAll(t *testing.T) {
	where, _, err := TranslateFilter(
		json.RawMessage(`{"query": "*"}`),
		pgPlaceholder, testColumns,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if where != "" {
		t.Errorf("expected empty WHERE for *, got %q", where)
	}
}

func TestTranslateFilter_QueryStringEscaped(t *testing.T) {
	// Value with special chars escaped by escapeQueryString
	where, args, err := TranslateFilter(
		json.RawMessage(`{"query": "email:user\\@example.com"}`),
		pgPlaceholder, testColumns,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if where != `"email" = $1` {
		t.Errorf("expected '\"email\" = $1', got %q", where)
	}
	if len(args) != 1 || args[0] != "user@example.com" {
		t.Errorf("expected args=[user@example.com], got %v", args)
	}
}

func TestTranslateFilter_UnknownColumn(t *testing.T) {
	_, _, err := TranslateFilter(
		json.RawMessage(`{"term": "value", "field": "nonexistent"}`),
		pgPlaceholder, testColumns,
	)
	if err == nil {
		t.Fatal("expected error for unknown column")
	}
}

func TestTranslateFilter_NoColumns(t *testing.T) {
	// When columns are empty, field validation is skipped
	where, args, err := TranslateFilter(
		json.RawMessage(`{"term": "value", "field": "anything"}`),
		pgPlaceholder, nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if where != `"anything" = $1` {
		t.Errorf("expected '\"anything\" = $1', got %q", where)
	}
	if len(args) != 1 {
		t.Errorf("expected 1 arg, got %d", len(args))
	}
}

func TestTranslateFilter_Prefix(t *testing.T) {
	where, args, err := TranslateFilter(
		json.RawMessage(`{"prefix": "ali", "field": "name"}`),
		pgPlaceholder, testColumns,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `"name" LIKE $1 ESCAPE '\'`
	if where != expected {
		t.Errorf("expected %q, got %q", expected, where)
	}
	if len(args) != 1 || args[0] != "ali%" {
		t.Errorf("expected args=[ali%%], got %v", args)
	}
}

func TestTranslateFilter_PrefixWithMetachars(t *testing.T) {
	where, args, err := TranslateFilter(
		json.RawMessage(`{"prefix": "100%_done", "field": "status"}`),
		pgPlaceholder, testColumns,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `"status" LIKE $1 ESCAPE '\'`
	if where != expected {
		t.Errorf("expected %q, got %q", expected, where)
	}
	// % and _ in the literal value should be escaped
	if len(args) != 1 || args[0] != `100\%\_done%` {
		t.Errorf(`expected args=[100\%%\_done%%], got %v`, args)
	}
}

func TestTranslateFilter_Wildcard(t *testing.T) {
	where, args, err := TranslateFilter(
		json.RawMessage(`{"wildcard": "al*ce", "field": "name"}`),
		pgPlaceholder, testColumns,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `"name" LIKE $1 ESCAPE '\'`
	if where != expected {
		t.Errorf("expected %q, got %q", expected, where)
	}
	if len(args) != 1 || args[0] != "al%ce" {
		t.Errorf("expected args=[al%%ce], got %v", args)
	}
}

func TestTranslateFilter_WildcardSingleChar(t *testing.T) {
	where, args, err := TranslateFilter(
		json.RawMessage(`{"wildcard": "v?lue", "field": "name"}`),
		pgPlaceholder, testColumns,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `"name" LIKE $1 ESCAPE '\'`
	if where != expected {
		t.Errorf("expected %q, got %q", expected, where)
	}
	if len(args) != 1 || args[0] != "v_lue" {
		t.Errorf("expected args=[v_lue], got %v", args)
	}
}

func TestTranslateFilter_WildcardEscapesLikeMetachars(t *testing.T) {
	// Literal % and _ in the wildcard pattern should be escaped
	where, args, err := TranslateFilter(
		json.RawMessage(`{"wildcard": "100%_*", "field": "status"}`),
		pgPlaceholder, testColumns,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `"status" LIKE $1 ESCAPE '\'`
	if where != expected {
		t.Errorf("expected %q, got %q", expected, where)
	}
	// literal % → \%, literal _ → \_, * → %
	if len(args) != 1 || args[0] != `100\%\_%` {
		t.Errorf(`expected args=[100\%%\_%%), got %v`, args)
	}
}

func TestFilterToLiteralSQL_Empty(t *testing.T) {
	sql, err := FilterToLiteralSQL(nil, testColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "" {
		t.Errorf("expected empty, got %q", sql)
	}
}

func TestFilterToLiteralSQL_MatchAll(t *testing.T) {
	sql, err := FilterToLiteralSQL(json.RawMessage(`{"match_all": {}}`), testColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "" {
		t.Errorf("expected empty for match_all, got %q", sql)
	}
}

func TestFilterToLiteralSQL_MatchNone(t *testing.T) {
	sql, err := FilterToLiteralSQL(json.RawMessage(`{"match_none": {}}`), testColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "FALSE" {
		t.Errorf("expected %q for match_none, got %q", "FALSE", sql)
	}
}

func TestFilterToLiteralSQL_Term(t *testing.T) {
	sql, err := FilterToLiteralSQL(
		json.RawMessage(`{"term": "active", "field": "status"}`),
		testColumns,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `"status" = 'active'`
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
}

func TestFilterToLiteralSQL_TermNumeric(t *testing.T) {
	sql, err := FilterToLiteralSQL(
		json.RawMessage(`{"term": 42, "field": "age"}`),
		testColumns,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `"age" = 42`
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
}

func TestFilterToLiteralSQL_TermBool(t *testing.T) {
	sql, err := FilterToLiteralSQL(
		json.RawMessage(`{"term": true, "field": "status"}`),
		nil, // skip column validation
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `"status" = true`
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
}

func TestFilterToLiteralSQL_Range(t *testing.T) {
	sql, err := FilterToLiteralSQL(
		json.RawMessage(`{"field": "age", "min": 18, "max": 65}`),
		testColumns,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `"age" >= 18 AND "age" <= 65`
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
}

func TestFilterToLiteralSQL_Conjuncts(t *testing.T) {
	filter := `{
		"conjuncts": [
			{"term": "active", "field": "status"},
			{"field": "age", "min": 18}
		]
	}`
	sql, err := FilterToLiteralSQL(json.RawMessage(filter), testColumns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `("status" = 'active') AND ("age" >= 18)`
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
}

func TestFilterToLiteralSQL_QuotesEscape(t *testing.T) {
	sql, err := FilterToLiteralSQL(
		json.RawMessage(`{"term": "it's", "field": "name"}`),
		testColumns,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `"name" = 'it''s'`
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
}

func TestFilterToLiteralSQL_Prefix(t *testing.T) {
	sql, err := FilterToLiteralSQL(
		json.RawMessage(`{"prefix": "ali", "field": "name"}`),
		testColumns,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `"name" LIKE 'ali%' ESCAPE '\'`
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
}

func TestFilterToLiteralSQL_UnknownColumn(t *testing.T) {
	_, err := FilterToLiteralSQL(
		json.RawMessage(`{"term": "value", "field": "nonexistent"}`),
		testColumns,
	)
	if err == nil {
		t.Fatal("expected error for unknown column")
	}
}

func TestSplitFieldValue(t *testing.T) {
	tests := []struct {
		input string
		field string
		value string
		ok    bool
	}{
		{"status:active", "status", "active", true},
		{"email:user\\:name", "email", "user\\:name", true}, // escaped colon in value
		{"nofield", "", "", false},
	}
	for _, tt := range tests {
		field, value, ok := evaluator.SplitFieldValue(tt.input)
		if ok != tt.ok || field != tt.field || value != tt.value {
			t.Errorf("evaluator.SplitFieldValue(%q) = (%q, %q, %v), want (%q, %q, %v)",
				tt.input, field, value, ok, tt.field, tt.value, tt.ok)
		}
	}
}

func TestUnescapeQueryString(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"hello", "hello"},
		{`user\@example.com`, "user@example.com"},
		{`path\/to\/file`, "path/to/file"},
		{`no\\escape`, `no\escape`},
	}
	for _, tt := range tests {
		got := evaluator.UnescapeQueryString(tt.input)
		if got != tt.want {
			t.Errorf("evaluator.UnescapeQueryString(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
