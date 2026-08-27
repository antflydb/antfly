// Copyright 2026 The Antfly Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package sdk

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	querydsl "github.com/antflydb/antfly/go/pkg/sdk/query"
)

func TestGraphQueryConstructors(t *testing.T) {
	notEqual, err := NewGraphNotEqual("a", "b")
	if err != nil {
		t.Fatal(err)
	}
	notExists, err := NewGraphNotExists([]GraphMatchEdge{{From: "a", To: "b"}})
	if err != nil {
		t.Fatal(err)
	}
	where, err := NewGraphWhereAnd(notEqual, notExists)
	if err != nil {
		t.Fatal(err)
	}
	neighborCount, err := CountGraphAlias("b", true)
	if err != nil {
		t.Fatal(err)
	}
	graphReturn, err := NewGraphAggregatesReturn(map[string]GraphCountAggregate{
		"rows":      CountGraphRows(),
		"neighbors": neighborCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	query, err := NewGraphMatchQuery(GraphMatchQuery{
		Index: "graph_idx",
		Match: GraphMatch{
			Anchor: "a",
			Nodes:  map[string]GraphMatchNode{"a": {}, "b": {}},
			Edges:  []GraphMatchEdge{{From: "a", To: "b", Types: []string{"links"}}},
			Where:  where,
		},
		Return: graphReturn,
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(query)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["index"] != "graph_idx" {
		t.Fatalf("index = %v", decoded["index"])
	}
	returns, ok := decoded["return"].(map[string]any)
	if !ok {
		t.Fatalf("return = %T", decoded["return"])
	}
	if _, ok := returns["aggregates"]; !ok {
		t.Fatalf("return = %#v", returns)
	}
	match := decoded["match"].(map[string]any)
	whereJSON := match["where"].(map[string]any)
	if predicates, ok := whereJSON["and"].([]any); !ok || len(predicates) != 2 {
		t.Fatalf("where = %#v", whereJSON)
	}
	if _, err := json.Marshal(QueryRequest{GraphQueries: map[string]GraphQuery{"authors": query}}); err != nil {
		t.Fatalf("valid graph query failed QueryRequest boundary validation: %v", err)
	}
}

func TestGraphSelectorAndProjectionConstructors(t *testing.T) {
	filter, err := NewGraphDocumentFilter(querydsl.TermQuery{Term: "beta", Field: "title"}.ToQuery())
	if err != nil {
		t.Fatal(err)
	}
	start, err := NewGraphKeySelector("doc:a", "doc:b")
	if err != nil {
		t.Fatal(err)
	}
	graphReturn, err := NewGraphBindingsReturn([]string{"b"}, GraphBindingsOptions{
		Limit:            25,
		IncludeDocuments: true,
		Fields:           []string{"title", "summary"},
	})
	if err != nil {
		t.Fatal(err)
	}
	query, err := NewGraphTraverseQuery(GraphTraverseQuery{
		Index: "graph_idx",
		Traverse: GraphTraversal{
			Start:     start,
			Direction: EdgeDirectionBoth,
			Limit:     25,
			Filter:    filter,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	encodedSelector, err := json.Marshal(query)
	if err != nil {
		t.Fatal(err)
	}
	var selectorJSON map[string]any
	if err := json.Unmarshal(encodedSelector, &selectorJSON); err != nil {
		t.Fatal(err)
	}
	traverse := selectorJSON["traverse"].(map[string]any)
	if traverse["direction"] != "both" {
		t.Fatalf("direction = %#v", traverse["direction"])
	}
	startJSON := traverse["start"].(map[string]any)
	if keys, ok := startJSON["keys"].([]any); !ok || len(keys) != 2 {
		t.Fatalf("start = %#v", startJSON)
	}
	filterJSON := traverse["filter"].(map[string]any)
	if filterJSON["term"] != "beta" || filterJSON["path"] != "/title" {
		t.Fatalf("filter = %#v", filterJSON)
	}

	encodedReturn, err := json.Marshal(graphReturn)
	if err != nil {
		t.Fatal(err)
	}
	var returnJSON map[string]any
	if err := json.Unmarshal(encodedReturn, &returnJSON); err != nil {
		t.Fatal(err)
	}
	if returnJSON["include_documents"] != true {
		t.Fatalf("return = %#v", returnJSON)
	}
	if fields, ok := returnJSON["fields"].([]any); !ok || len(fields) != 2 {
		t.Fatalf("return = %#v", returnJSON)
	}
}

func TestGraphConstructorsRejectSemanticErrors(t *testing.T) {
	if _, err := NewGraphDocumentFilter(querydsl.NewMatch("beta", "title")); err == nil {
		t.Fatal("expected analyzer-backed graph filter to fail")
	}
	if _, err := NewGraphDocumentFilter(querydsl.NewGeoBoundingBox(37, -123, 38, -122, "location")); err == nil {
		t.Fatal("expected index-only graph filter to fail")
	}
	if _, err := NewGraphDocumentFilter(querydsl.TermQuery{Term: "beta", Field: "title", Boost: 2}.ToQuery()); err == nil {
		t.Fatal("expected scoring graph filter to fail")
	}
	if _, err := NewGraphDocumentFilter(querydsl.ConjunctionQuery{Conjuncts: []querydsl.Query{
		querydsl.TermQuery{Term: "beta", Field: "title", Boost: 2}.ToQuery(),
	}}.ToQuery()); err == nil {
		t.Fatal("expected nested scoring graph filter to fail")
	}
	if _, err := NewGraphDocumentFilter(querydsl.DocIdQuery{Ids: []string{"doc:a", "doc:a"}}.ToQuery()); err == nil {
		t.Fatal("expected duplicate graph ids to fail")
	}
	if _, err := NewGraphDocumentFilter(querydsl.NumericRangeQuery{Field: "score"}.ToQuery()); err == nil {
		t.Fatal("expected boundless graph range filter to fail")
	}
	if _, err := NewGraphResultRefSelector("$embeddings_results.vector_idx", 10); err == nil {
		t.Fatal("expected invalid result reference")
	}
	if _, err := NewGraphKeySelector("doc:a", "doc:a"); err == nil {
		t.Fatal("expected duplicate key error")
	}
	identity, err := NewGraphIdentity("doc:a", "docs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewGraphIdentitySelector(identity, identity); err == nil {
		t.Fatal("expected duplicate identity error")
	}
	if _, err := NewGraphBindingsReturn([]string{"a"}, GraphBindingsOptions{Fields: []string{"title"}}); err == nil {
		t.Fatal("expected projection hydration error")
	}
	if _, err := NewGraphBindingsReturn([]string{"a", "a"}, GraphBindingsOptions{}); err == nil {
		t.Fatal("expected duplicate binding projection error")
	}
	if _, err := NewGraphBindingsReturn([]string{"a", "b"}, GraphBindingsOptions{
		Limit:            5_001,
		IncludeDocuments: true,
	}); err == nil || !strings.Contains(err.Error(), "10000") {
		t.Fatalf("expected actionable binding hydration budget error, got %v", err)
	}
	if _, err := NewGraphBindingsReturn([]string{"a", "b"}, GraphBindingsOptions{
		Limit:            5_000,
		IncludeDocuments: true,
	}); err != nil {
		t.Fatalf("expected binding hydration at the budget boundary to succeed: %v", err)
	}
	tooManyBindings := make([]string, maxGraphMatchNodes+1)
	for i := range tooManyBindings {
		tooManyBindings[i] = fmt.Sprintf("alias_%d", i)
	}
	if _, err := NewGraphBindingsReturn(tooManyBindings, GraphBindingsOptions{}); err == nil {
		t.Fatal("expected binding projection complexity error")
	}
	graphReturn, err := NewGraphBindingsReturn([]string{"missing"}, GraphBindingsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewGraphMatchQuery(GraphMatchQuery{
		Index: "graph_idx",
		Match: GraphMatch{
			Anchor: "a",
			Nodes:  map[string]GraphMatchNode{"a": {}, "b": {}},
			Edges:  []GraphMatchEdge{{From: "a", To: "b"}},
		},
		Return: graphReturn,
	}); err == nil {
		t.Fatal("expected unknown return alias error")
	}

	var uncheckedHydrationReturn GraphReturn
	if err := uncheckedHydrationReturn.FromGraphBindingsReturn(GraphBindingsReturn{
		Bindings:         []string{"a", "b"},
		Limit:            5_001,
		IncludeDocuments: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewGraphMatchQuery(GraphMatchQuery{
		Index: "graph_idx",
		Match: GraphMatch{
			Anchor: "a",
			Nodes:  map[string]GraphMatchNode{"a": {}, "b": {}},
			Edges:  []GraphMatchEdge{{From: "a", To: "b"}},
		},
		Return: uncheckedHydrationReturn,
	}); err == nil || !strings.Contains(err.Error(), "10000") {
		t.Fatalf("expected final graph return validation to enforce the hydration budget, got %v", err)
	}

	for name, rawReturn := range map[string]string{
		"mixed variants":          `{"bindings":["a"],"aggregates":{"rows":{"count":"*"}}}`,
		"unknown field":           `{"bindings":["a"],"unexpected":true}`,
		"distinct rows":           `{"aggregates":{"rows":{"count":"*","distinct":true}}}`,
		"explicit false rows":     `{"aggregates":{"rows":{"count":"*","distinct":false}}}`,
		"unknown aggregate field": `{"aggregates":{"rows":{"count":"*","unexpected":true}}}`,
		"too many counts":         mustMarshalGraphAggregates(t, maxGraphCountAggregates+1),
	} {
		t.Run("unchecked return rejects "+name, func(t *testing.T) {
			var unchecked GraphReturn
			if err := json.Unmarshal([]byte(rawReturn), &unchecked); err != nil {
				t.Fatal(err)
			}
			if _, err := NewGraphMatchQuery(GraphMatchQuery{
				Index: "graph_idx",
				Match: GraphMatch{
					Anchor: "a",
					Nodes:  map[string]GraphMatchNode{"a": {}, "b": {}},
					Edges:  []GraphMatchEdge{{From: "a", To: "b"}},
				},
				Return: unchecked,
			}); err == nil {
				t.Fatalf("expected unchecked %s to fail final validation", name)
			}
		})
	}

	validReturn, err := NewGraphBindingsReturn([]string{"b"}, GraphBindingsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	edges := make([]GraphMatchEdge, maxGraphMatchEdges+1)
	for i := range edges {
		edges[i] = GraphMatchEdge{From: "a", To: "b"}
	}
	if _, err := NewGraphMatchQuery(GraphMatchQuery{
		Index: "graph_idx",
		Match: GraphMatch{
			Anchor: "a",
			Nodes:  map[string]GraphMatchNode{"a": {}, "b": {}},
			Edges:  edges,
		},
		Return: validReturn,
	}); err == nil {
		t.Fatal("expected graph edge complexity budget error")
	}
	oversizedAlias := strings.Repeat("a", maxGraphIdentifierBytes+1)
	if _, err := NewGraphBindingsReturn([]string{oversizedAlias}, GraphBindingsOptions{}); err == nil {
		t.Fatal("expected oversized binding projection error")
	}
	if _, err := NewGraphMatchQuery(GraphMatchQuery{
		Index: "graph_idx",
		Match: GraphMatch{
			Anchor: oversizedAlias,
			Nodes:  map[string]GraphMatchNode{oversizedAlias: {}},
		},
		Return: validReturn,
	}); err == nil {
		t.Fatal("expected oversized graph alias error")
	}
	if _, err := NewGraphMatchQuery(GraphMatchQuery{
		Index: "graph_idx",
		Match: GraphMatch{
			Anchor: "a",
			Nodes: map[string]GraphMatchNode{
				"a": {},
				"b": {Table: "  "},
			},
			Edges: []GraphMatchEdge{{From: "a", To: "b"}},
		},
		Return: validReturn,
	}); err == nil {
		t.Fatal("expected blank graph alias table error")
	}
}

func TestGraphDocumentFilterUsesDiscriminatedRangeWireShape(t *testing.T) {
	zero := 0.0
	ten := 10.0
	exclusive := false
	filter, err := NewGraphDocumentFilter(querydsl.NumericRangeQuery{
		Field:        "score",
		Min:          &zero,
		Max:          &ten,
		InclusiveMin: &exclusive,
	}.ToQuery())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(filter)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(encoded, &value); err != nil {
		t.Fatal(err)
	}
	body, ok := value["numeric_range"].(map[string]any)
	if !ok || body["path"] != "/score" || body["min"] != float64(0) || body["max"] != float64(10) || body["inclusive_min"] != false {
		t.Fatalf("unexpected graph range filter: %#v", value)
	}
}

func TestGraphDocumentFilterUsesUnambiguousBoolFieldWireShape(t *testing.T) {
	filter, err := NewGraphDocumentFilter(querydsl.BoolFieldQuery{
		Field: "published",
		Bool:  true,
	}.ToQuery())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(filter)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(encoded, &value); err != nil {
		t.Fatal(err)
	}
	body, ok := value["bool_field"].(map[string]any)
	if !ok || body["path"] != "/published" || body["value"] != true {
		t.Fatalf("unexpected graph bool field filter: %#v", value)
	}
	if _, found := value["bool"]; found {
		t.Fatalf("graph bool field filter collides with the compound bool root: %#v", value)
	}
}

func TestGraphDocumentFilterUsesDiscriminatedDateRangeWireShape(t *testing.T) {
	start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	filter, err := NewGraphDocumentFilter(querydsl.DateRangeStringQuery{
		Field: "created_at",
		Start: &start,
	}.ToQuery())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(filter)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(encoded, &value); err != nil {
		t.Fatal(err)
	}
	body, ok := value["date_range"].(map[string]any)
	if !ok || body["path"] != "/created_at" || body["start"] != "2026-01-01T00:00:00Z" {
		t.Fatalf("unexpected graph date range filter: %#v", value)
	}
}

func TestGraphDocumentFilterNormalizesAndBoundsDateRangeInstants(t *testing.T) {
	start := time.Date(2300, time.January, 1, 0, 0, 0, 0, time.FixedZone("test", -7*60*60))
	filter, err := NewGraphDocumentFilter(querydsl.DateRangeStringQuery{
		Field: "created_at",
		Start: &start,
	}.ToQuery())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(filter)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(encoded, &value); err != nil {
		t.Fatal(err)
	}
	body, ok := value["date_range"].(map[string]any)
	if !ok || body["start"] != "2300-01-01T07:00:00Z" {
		t.Fatalf("graph date range did not normalize to UTC: %#v", value)
	}

	for _, invalid := range []time.Time{
		time.Date(1969, time.December, 31, 23, 59, 59, 999_999_999, time.UTC),
		time.Date(2554, time.July, 21, 23, 34, 33, 709_551_616, time.UTC),
	} {
		if _, err := NewGraphDocumentFilter(querydsl.DateRangeStringQuery{
			Field: "created_at",
			Start: &invalid,
		}.ToQuery()); err == nil || !strings.Contains(err.Error(), "supported Unix-nanosecond range") {
			t.Fatalf("expected unsupported graph date %s to fail, got %v", invalid, err)
		}
	}
}

func TestGraphOpaqueUnionValidationRejectsUnknownMembers(t *testing.T) {
	t.Run("query", func(t *testing.T) {
		var query GraphQuery
		if err := json.Unmarshal([]byte(`{"traverse":{},"unexpected":true}`), &query); err != nil {
			t.Fatal(err)
		}
		if err := validateGraphQuery(query); err == nil || !strings.Contains(err.Error(), "unexpected") {
			t.Fatalf("expected unknown graph query member error, got %v", err)
		}
	})

	for name, encoded := range map[string]string{
		"unknown":         `{"keys":["doc:a"],"unexpected":true}`,
		"variant field":   `{"keys":["doc:a"],"limit":1}`,
		"null binding":    `{"result_ref":"$graph_results.prior","binding":null}`,
		"null limit":      `{"result_ref":"$graph_results.prior","limit":null}`,
		"null result ref": `{"result_ref":null}`,
	} {
		t.Run("selector "+name, func(t *testing.T) {
			var selector GraphNodeSelector
			if err := json.Unmarshal([]byte(encoded), &selector); err != nil {
				t.Fatal(err)
			}
			if err := validateGraphSelector(selector); err == nil {
				t.Fatalf("expected invalid selector %s to fail", encoded)
			}
		})
	}

	t.Run("where", func(t *testing.T) {
		var where GraphWhereExpression
		if err := json.Unmarshal([]byte(
			`{"not_equal":{"left":{"alias":"a"},"right":{"alias":"b"}},"unexpected":true}`,
		), &where); err != nil {
			t.Fatal(err)
		}
		complexity := graphMatchComplexity{}
		aliases := map[string]struct{}{"a": {}, "b": {}}
		if err := validateGraphWhereExpression(where, aliases, &complexity, 0); err == nil || !strings.Contains(err.Error(), "unexpected") {
			t.Fatalf("expected unknown graph where member error, got %v", err)
		}
	})

	t.Run("nested document filter", func(t *testing.T) {
		var graphQuery GraphQuery
		if err := json.Unmarshal([]byte(
			`{"index":"graph_idx","match":{"anchor":"a","nodes":{"a":{"filter":{"term":"active","path":"/status","unexpected":true}}},"edges":[]},"return":{"bindings":["a"]}}`,
		), &graphQuery); err != nil {
			t.Fatal(err)
		}
		_, err := json.Marshal(QueryRequest{GraphQueries: map[string]GraphQuery{"filtered": graphQuery}})
		if err == nil || !strings.Contains(err.Error(), "unexpected") {
			t.Fatalf("expected nested unknown graph filter member error, got %v", err)
		}
	})
}

func TestGraphDocumentFilterRejectsFullTextDateParser(t *testing.T) {
	start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	if _, err := NewGraphDocumentFilter(querydsl.DateRangeStringQuery{
		Field:          "created_at",
		Start:          &start,
		DatetimeParser: "2006/01/02",
	}.ToQuery()); err == nil {
		t.Fatal("expected graph date filter to reject the full-text datetime parser")
	}
}

func TestGraphDocumentFilterCanonicalizesNestedAndEscapedPaths(t *testing.T) {
	filter, err := NewGraphDocumentFilter(querydsl.TermQuery{Term: "beta", Field: "author.display/name~raw"}.ToQuery())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(filter)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(encoded, &value); err != nil {
		t.Fatal(err)
	}
	if value["path"] != "/author/display~1name~0raw" {
		t.Fatalf("path = %#v", value["path"])
	}

	if _, err := NewGraphDocumentFilter(querydsl.TermQuery{Term: "beta", Field: "/bad~escape"}.ToQuery()); err == nil {
		t.Fatal("expected invalid JSON Pointer error")
	}
}

func TestGraphAggregateConstructorEnforcesComplexityBudget(t *testing.T) {
	aggregates := make(map[string]GraphCountAggregate, maxGraphCountAggregates+1)
	for i := 0; i <= maxGraphCountAggregates; i++ {
		aggregates[fmt.Sprintf("aggregate_%d", i)] = CountGraphRows()
	}
	if _, err := NewGraphAggregatesReturn(aggregates); err == nil {
		t.Fatal("expected graph aggregate complexity budget error")
	}
}

func TestGraphAggregateConstructorAllowsDuplicateExpressionsUnderDifferentNames(t *testing.T) {
	personCount, err := CountGraphAlias("person", true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewGraphAggregatesReturn(map[string]GraphCountAggregate{
		"first":  personCount,
		"second": personCount,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCountGraphAliasRejectsInvalidIdentifier(t *testing.T) {
	if _, err := CountGraphAlias("$internal", true); err == nil {
		t.Fatal("expected invalid graph count alias error")
	}
}

func TestGraphIdentifiersReserveControlTokens(t *testing.T) {
	for _, name := range []string{"*", "$query_results", " \t "} {
		if _, err := NewGraphAggregatesReturn(map[string]GraphCountAggregate{
			name: CountGraphRows(),
		}); err == nil {
			t.Fatalf("expected aggregate name %q to be rejected", name)
		}
		if _, err := NewGraphBindingsReturn([]string{name}, GraphBindingsOptions{}); err == nil {
			t.Fatalf("expected binding alias %q to be rejected", name)
		}
	}
}

func TestGraphQueryResultUsesStableDiscriminator(t *testing.T) {
	var canonical GraphQueryResult
	if err := json.Unmarshal([]byte(`{"kind":"nodes","nodes":[],"paths":[],"stats":{"returned_items":0,"truncated":false}}`), &canonical); err != nil {
		t.Fatal(err)
	}

	value, err := DecodeGraphQueryResult(canonical)
	if err != nil {
		t.Fatal(err)
	}
	nodes, ok := value.(GraphNodesResult)
	if !ok {
		t.Fatalf("result = %T, want GraphNodesResult", value)
	}
	if nodes.Kind != GraphNodesResultKindNodes {
		t.Fatalf("nodes kind = %q, want nodes", nodes.Kind)
	}

	var result GraphResult
	if err := json.Unmarshal([]byte(`{"type":"neighbors","nodes":[],"paths":[],"total":0,"took":1}`), &result); err != nil {
		t.Fatal(err)
	}
	value, err = DecodeGraphResult(result)
	if err != nil {
		t.Fatal(err)
	}
	legacy, ok := value.(LegacyGraphQueryResult)
	if !ok {
		t.Fatalf("result = %T, want LegacyGraphQueryResult", value)
	}
	if legacy.Type != GraphQueryTypeNeighbors {
		t.Fatalf("legacy type = %q, want neighbors", legacy.Type)
	}

	for _, malformed := range []string{
		`{"type":"neighbors"}`,
		`{"type":"neighbors","total":null}`,
		`{"type":"unknown","total":0}`,
		`{"kind":null,"type":"neighbors","total":0}`,
	} {
		if err := json.Unmarshal([]byte(malformed), &result); err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeGraphResult(result); err == nil {
			t.Fatalf("expected malformed legacy graph result to fail: %s", malformed)
		}
	}

	if err := json.Unmarshal([]byte(`{"kind":"unknown"}`), &result); err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeGraphResult(result); err == nil {
		t.Fatal("expected unknown graph result kind to fail")
	}
}

func TestQueryGraphResponsesHonorRequestedDialectAndOperations(t *testing.T) {
	decode := func(encoded string) GraphResult {
		t.Helper()
		var result GraphResult
		if err := json.Unmarshal([]byte(encoded), &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	canonical := decode(`{"kind":"nodes","nodes":[],"paths":[],"stats":{"returned_items":0,"truncated":false}}`)
	legacy := decode(`{"type":"neighbors","nodes":[],"paths":[],"total":0,"took":0}`)
	var traversal GraphQuery
	if err := json.Unmarshal([]byte(`{"index":"graph","traverse":{"start":{"keys":["a"]}}}`), &traversal); err != nil {
		t.Fatal(err)
	}
	var aggregation GraphQuery
	if err := json.Unmarshal([]byte(`{"index":"graph","match":{"anchor":"a","nodes":{"a":{}},"edges":[]},"return":{"aggregates":{"rows":{"count":"*"}}}}`), &aggregation); err != nil {
		t.Fatal(err)
	}

	canonicalRequest := []QueryRequest{{GraphQueries: map[string]GraphQuery{"walk": traversal}}}
	canonicalResponse := QueryResponses{Responses: []QueryResult{{
		GraphResults: map[string]GraphResult{"walk": canonical},
	}}}
	if err := validateQueryGraphResponses(canonicalRequest, &canonicalResponse); err != nil {
		t.Fatalf("valid canonical response: %v", err)
	}

	legacyRequest := []QueryRequest{{GraphSearches: map[string]LegacyGraphQuery{"walk": {}}}}
	legacyResponse := QueryResponses{Responses: []QueryResult{{
		GraphResults: map[string]GraphResult{"walk": legacy},
	}}}
	if err := validateQueryGraphResponses(legacyRequest, &legacyResponse); err != nil {
		t.Fatalf("valid legacy response: %v", err)
	}

	tests := []struct {
		name      string
		requests  []QueryRequest
		responses QueryResponses
		contains  string
	}{
		{
			name:      "canonical rejects legacy",
			requests:  canonicalRequest,
			responses: legacyResponse,
			contains:  "canonical graph result requires a discriminator",
		},
		{
			name:      "legacy rejects canonical",
			requests:  legacyRequest,
			responses: canonicalResponse,
			contains:  "legacy graph result requires discriminator",
		},
		{
			name:     "operation names must match",
			requests: canonicalRequest,
			responses: QueryResponses{Responses: []QueryResult{{
				GraphResults: map[string]GraphResult{"other": canonical},
			}}},
			contains: "missing=[walk] unexpected=[other]",
		},
		{
			name:      "graph request response cardinality",
			requests:  canonicalRequest,
			responses: QueryResponses{},
			contains:  "response count 0 does not match request count 1",
		},
		{
			name:     "operation result kind must match",
			requests: canonicalRequest,
			responses: QueryResponses{Responses: []QueryResult{{
				GraphResults: map[string]GraphResult{"walk": decode(`{"kind":"aggregates","aggregates":{"count":{"value":"1","exact":true}},"stats":{"returned_items":1,"truncated":false}}`)},
			}}},
			contains: `requires result kind "nodes", got "aggregates"`,
		},
		{
			name:     "exact aggregate result cannot be truncated",
			requests: []QueryRequest{{GraphQueries: map[string]GraphQuery{"count": aggregation}}},
			responses: QueryResponses{Responses: []QueryResult{{
				GraphResults: map[string]GraphResult{"count": decode(`{"kind":"aggregates","aggregates":{"rows":{"value":"1","exact":true}},"stats":{"returned_items":1,"truncated":true}}`)},
			}}},
			contains: "exact aggregate graph results cannot be truncated",
		},
		{
			name:     "non graph request rejects graph results",
			requests: []QueryRequest{{}},
			responses: QueryResponses{Responses: []QueryResult{{
				GraphResults: map[string]GraphResult{"walk": canonical},
			}}},
			contains: "without graph operations",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateQueryGraphResponses(test.requests, &test.responses)
			if err == nil || !strings.Contains(err.Error(), test.contains) {
				t.Fatalf("expected error containing %q, got %v", test.contains, err)
			}
		})
	}
}

func TestDecodeGraphResultForQueryValidatesRequestedProjection(t *testing.T) {
	decodeQuery := func(encoded string) GraphQuery {
		t.Helper()
		var query GraphQuery
		if err := json.Unmarshal([]byte(encoded), &query); err != nil {
			t.Fatal(err)
		}
		return query
	}
	decodeResult := func(encoded string) GraphResult {
		t.Helper()
		var result GraphResult
		if err := json.Unmarshal([]byte(encoded), &result); err != nil {
			t.Fatal(err)
		}
		return result
	}

	bindingsQuery := decodeQuery(`{"index":"graph","match":{"anchor":"a","nodes":{"a":{},"b":{}},"edges":[{"from":"a","to":"b"}]},"return":{"bindings":["a","b"]}}`)
	validBindings := decodeResult(`{"kind":"bindings","rows":[{"a":{"key":"1"},"b":null}],"stats":{"returned_items":1,"truncated":false}}`)
	if _, err := DecodeGraphResultForQuery(bindingsQuery, validBindings); err != nil {
		t.Fatalf("valid request-bound bindings result: %v", err)
	}
	wrongBindings := decodeResult(`{"kind":"bindings","rows":[{"a":{"key":"1"},"c":null}],"stats":{"returned_items":1,"truncated":false}}`)
	if _, err := DecodeGraphResultForQuery(bindingsQuery, wrongBindings); err == nil || !strings.Contains(err.Error(), "unrequested alias") {
		t.Fatalf("expected projected alias mismatch, got %v", err)
	}

	aggregatesQuery := decodeQuery(`{"index":"graph","match":{"anchor":"a","nodes":{"a":{}},"edges":[]},"return":{"aggregates":{"rows":{"count":"*"}}}}`)
	wrongAggregates := decodeResult(`{"kind":"aggregates","aggregates":{"other":{"value":"1","exact":true}},"stats":{"returned_items":1,"truncated":false}}`)
	if _, err := DecodeGraphResultForQuery(aggregatesQuery, wrongAggregates); err == nil || !strings.Contains(err.Error(), "do not match requested names") {
		t.Fatalf("expected aggregate name mismatch, got %v", err)
	}
}

func TestCanonicalGraphResultPreservesOpaqueHydratedJSON(t *testing.T) {
	var canonical GraphQueryResult
	if err := json.Unmarshal([]byte(`{"kind":"nodes","nodes":[{"key":"a","depth":0,"document":{"title":"alpha","nested":{"values":[1,true,null]}},"evidence":{"source":"edge"}}],"paths":[],"stats":{"returned_items":1,"truncated":false}}`), &canonical); err != nil {
		t.Fatal(err)
	}
	value, err := DecodeGraphQueryResult(canonical)
	if err != nil {
		t.Fatal(err)
	}
	nodes := value.(GraphNodesResult)
	if nodes.Nodes[0].Document["title"] != "alpha" {
		t.Fatalf("unexpected hydrated document: %#v", nodes.Nodes[0].Document)
	}
	nested, ok := nodes.Nodes[0].Document["nested"].(map[string]any)
	if !ok || len(nested["values"].([]any)) != 3 || nodes.Nodes[0].Evidence["source"] != "edge" {
		t.Fatalf("opaque JSON was not preserved: document=%#v evidence=%#v", nodes.Nodes[0].Document, nodes.Nodes[0].Evidence)
	}
}

func TestCanonicalGraphResultRejectsRowsOverSchemaPropertyLimit(t *testing.T) {
	row := make(map[string]any, maxGraphMatchNodes+1)
	for i := range maxGraphMatchNodes + 1 {
		row[fmt.Sprintf("alias%d", i)] = map[string]any{"key": fmt.Sprintf("key%d", i)}
	}
	encoded, err := json.Marshal(map[string]any{
		"kind":  "bindings",
		"rows":  []any{row},
		"stats": map[string]any{"returned_items": 1, "truncated": false},
	})
	if err != nil {
		t.Fatal(err)
	}
	var canonical GraphQueryResult
	if err := json.Unmarshal(encoded, &canonical); err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeGraphQueryResult(canonical); err == nil || !strings.Contains(err.Error(), "between 1 and 64 properties") {
		t.Fatalf("expected row property limit error, got %v", err)
	}
}

func TestCanonicalGraphResultDecodersFailClosed(t *testing.T) {
	malformed := []string{
		`{"kind":"nodes"}`,
		`{"kind":"nodes","nodes":[],"paths":[]}`,
		`{"kind":"nodes","nodes":[],"paths":[],"stats":{"returned_items":0}}`,
		`{"kind":"nodes","nodes":[],"paths":[],"stats":{"returned_items":null,"truncated":false}}`,
		`{"kind":"nodes","nodes":[],"paths":[],"stats":{"returned_items":1,"truncated":false}}`,
		`{"kind":"nodes","nodes":[{"key":"","depth":0}],"paths":[],"stats":{"returned_items":1,"truncated":false}}`,
		`{"kind":"nodes","nodes":[{"key":"a","table":"","depth":0}],"paths":[],"stats":{"returned_items":1,"truncated":false}}`,
		`{"kind":"nodes","nodes":[{"key":"a","table":null,"depth":0}],"paths":[],"stats":{"returned_items":1,"truncated":false}}`,
		`{"kind":"bindings","rows":[{"a":{"key":"a","table":""}}],"stats":{"returned_items":1,"truncated":false}}`,
		`{"kind":"bindings","rows":[{"a":{"key":"a","table":null}}],"stats":{"returned_items":1,"truncated":false}}`,
		`{"kind":"nodes","nodes":[],"paths":[{"nodes":[{"key":"a"}],"edges":[],"weight_mode":"min_hops"}],"stats":{"returned_items":1,"truncated":false}}`,
		`{"kind":"nodes","nodes":[],"paths":[{"nodes":[{"key":"a","table":""}],"edges":[],"length":0,"weight_mode":"min_hops","weight_sum":0,"objective_value":0}],"stats":{"returned_items":1,"truncated":false}}`,
		`{"kind":"nodes","nodes":[{"key":"wrong","depth":0}],"paths":[{"nodes":[{"key":"a"}],"edges":[],"length":0,"weight_mode":"min_hops","weight_sum":0,"objective_value":0}],"stats":{"returned_items":1,"truncated":false}}`,
		`{"kind":"nodes","nodes":[{"key":"a","table":"entities","depth":0}],"paths":[{"nodes":[{"key":"a"}],"edges":[],"length":0,"weight_mode":"min_hops","weight_sum":0,"objective_value":0}],"stats":{"returned_items":1,"truncated":false}}`,
		`{"kind":"nodes","nodes":[{"key":"b","depth":0,"path":[{"key":"a"},{"key":"b"}]}],"paths":[],"stats":{"returned_items":1,"truncated":false}}`,
		`{"kind":"nodes","nodes":[{"key":"b","depth":1,"path":[{"key":"a","table":null},{"key":"b"}],"path_edges":[{"from":{"key":"a"},"to":{"key":"b"},"type":"edge","weight":1}]}],"paths":[],"stats":{"returned_items":1,"truncated":false}}`,
		`{"kind":"nodes","nodes":[{"key":"b","depth":1,"path":[{"key":"a"},{"key":"b"}],"path_edges":[{"from":{"key":"a"},"to":{"key":"b"},"direction":"sideways","type":"edge","weight":1}]}],"paths":[],"stats":{"returned_items":1,"truncated":false}}`,
		`{"kind":"nodes","nodes":[{"key":"wrong","depth":1,"path":[{"key":"a"},{"key":"b"}]}],"paths":[],"stats":{"returned_items":1,"truncated":false}}`,
		`{"kind":"nodes","nodes":[],"paths":[{"nodes":[{"key":"a"},{"key":"b"}],"edges":[{"from":{"key":"a"},"to":{"key":"b"},"type":"edge"}],"length":1,"weight_mode":"min_hops","weight_sum":0,"objective_value":1}],"stats":{"returned_items":1,"truncated":false}}`,
		`{"kind":"nodes","nodes":[{"key":"b","depth":0}],"paths":[{"nodes":[{"key":"a"},{"key":"b"}],"edges":[{"from":{"key":"a","table":null},"to":{"key":"b"},"type":"edge","weight":1}],"length":1,"weight_mode":"min_hops","weight_sum":1,"objective_value":1}],"stats":{"returned_items":1,"truncated":false}}`,
		fmt.Sprintf(`{"kind":"nodes","nodes":[],"paths":[{"nodes":[{"key":"a"},{"key":"b"}],"edges":[{"from":{"key":"a"},"to":{"key":"b"},"type":%q,"weight":1}],"length":1,"weight_mode":"min_hops","weight_sum":1,"objective_value":1}],"stats":{"returned_items":1,"truncated":false}}`, strings.Repeat("é", maxGraphEdgeTypeBytes/2+1)),
		`{"kind":"nodes","nodes":[],"paths":[],"stats":{"returned_items":0,"truncated":false},"unexpected":true}`,
		`{"kind":"nodes","nodes":[],"paths":[],"stats":{"returned_items":0,"truncated":false,"unexpected":true}}`,
		`{"kind":"nodes","nodes":[{"key":"a","depth":0,"unexpected":true}],"paths":[],"stats":{"returned_items":1,"truncated":false}}`,
		`{"kind":"bindings","rows":[],"stats":{"returned_items":1,"truncated":false}}`,
		`{"kind":"bindings","rows":[{}],"stats":{"returned_items":1,"truncated":false}}`,
		`{"kind":"bindings","rows":[{"a":{}}],"stats":{"returned_items":1,"truncated":false}}`,
		`{"kind":"aggregates","aggregates":{"count":{"value":"1","exact":false}},"stats":{"returned_items":1,"truncated":false}}`,
		`{"kind":"aggregates","aggregates":{"count":{"value":"1.0","exact":true}},"stats":{"returned_items":1,"truncated":false}}`,
		`{"kind":"aggregates","aggregates":{"count":{"value":"1","exact":true}},"stats":{"returned_items":1,"truncated":true}}`,
	}

	for _, encoded := range malformed {
		t.Run(encoded, func(t *testing.T) {
			var canonical GraphQueryResult
			if err := json.Unmarshal([]byte(encoded), &canonical); err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeGraphQueryResult(canonical); err == nil {
				t.Fatal("expected canonical graph query result to be rejected")
			}

			var result GraphResult
			if err := json.Unmarshal([]byte(encoded), &result); err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeGraphResult(result); err == nil {
				t.Fatal("expected canonical graph result to be rejected")
			}
		})
	}
}

func TestGraphMatchConstructorRequiresDeclaredSourceAnchor(t *testing.T) {
	graphReturn, err := NewGraphBindingsReturn([]string{"a"}, GraphBindingsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, anchor := range []string{"", "missing"} {
		_, err := NewGraphMatchQuery(GraphMatchQuery{
			Index: "graph_idx",
			Match: GraphMatch{
				Anchor: anchor,
				Nodes:  map[string]GraphMatchNode{"a": {}},
				Edges:  []GraphMatchEdge{},
			},
			Return: graphReturn,
		})
		if err == nil {
			t.Fatalf("expected invalid graph anchor %q to fail", anchor)
		}
	}
}

func TestGraphResultBindingSelector(t *testing.T) {
	selector, err := NewGraphResultBindingSelector("authors_and_posts", "post", 100)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(selector)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(encoded, &value); err != nil {
		t.Fatal(err)
	}
	if value["result_ref"] != "$graph_results.authors_and_posts" || value["binding"] != "post" {
		t.Fatalf("selector = %#v", value)
	}
	if _, err := NewGraphResultBindingSelector("", "post", 1); err == nil {
		t.Fatal("expected empty graph result query name to fail")
	}
	for _, tc := range []struct {
		name    string
		binding string
	}{
		{name: " authors", binding: "post"},
		{name: "authors ", binding: "post"},
		{name: "authors", binding: " post"},
		{name: "authors", binding: "post\u00a0"},
		{name: "authors", binding: "post\ncomment"},
		{name: "authors", binding: "post\x00comment"},
		{name: "authors", binding: "post\u00a0comment"},
		{name: "authors", binding: "post\u200bcomment"},
		{name: "authors", binding: "post\u2028comment"},
		{name: "authors", binding: "post\u202ecomment"},
	} {
		if _, err := NewGraphResultBindingSelector(tc.name, tc.binding, 1); err == nil {
			t.Fatalf("expected identifiers with unsafe whitespace or controls to fail: %#v", tc)
		}
	}
	if _, err := NewGraphResultBindingSelector("authors and posts", "post author", 1); err != nil {
		t.Fatalf("expected ordinary internal spaces to remain valid: %v", err)
	}
	if _, err := NewGraphResultBindingSelector("作者", "文章", 1); err != nil {
		t.Fatalf("expected visible Unicode identifiers to remain valid: %v", err)
	}
	if _, err := NewGraphResultBindingSelector("$reserved", "post", 1); err == nil {
		t.Fatal("expected reserved graph result query name to fail")
	}
	if _, err := NewGraphResultRefSelector("$graph_results."+strings.Repeat("q", maxGraphIdentifierRunes+1), 1); err == nil {
		t.Fatal("expected oversized graph result query name to fail")
	}
	if _, err := NewGraphResultRefSelector("$full_text_results", 1); err == nil {
		t.Fatal("expected legacy lane-specific reference to fail in canonical helper")
	}
}

func TestGraphIdentifiersMatchVersionedWirePolicy(t *testing.T) {
	if GraphIdentifierUnicodeVersion != "15.0.0" || GraphIdentifierPolicyVersion != 1 {
		t.Fatalf("unexpected graph identifier policy %d / Unicode %s", GraphIdentifierPolicyVersion, GraphIdentifierUnicodeVersion)
	}
	for _, tc := range graphIdentifierConformanceCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsValidGraphIdentifier(tc.value); got != tc.valid {
				t.Fatalf("IsValidGraphIdentifier(%q) = %t, want %t", tc.value, got, tc.valid)
			}
		})
	}
}

func TestGraphMatchEdgeValidationMatchesServerDefaultsAndBudgets(t *testing.T) {
	zero := 0.0
	negative := -1.0
	if err := validateGraphMatchEdgeShape(GraphMatchEdge{From: "a", To: "b", Direction: EdgeDirectionBoth}); err != nil {
		t.Fatalf("both direction must be accepted: %v", err)
	}
	if err := validateGraphMatchEdgeShape(GraphMatchEdge{From: "a", To: "b", Direction: EdgeDirection("sideways")}); err == nil {
		t.Fatal("expected invalid graph edge direction to fail")
	}
	if err := validateGraphMatchEdgeShape(GraphMatchEdge{From: "a", To: "b", MinHops: 2}); err == nil {
		t.Fatal("expected omitted max_hops to default to one and reject min_hops=2")
	}
	if err := validateGraphMatchEdgeShape(GraphMatchEdge{From: "a", To: "b", MinWeight: &zero, MaxWeight: &zero}); err != nil {
		t.Fatalf("explicit zero weight range must be representable: %v", err)
	}
	encoded, err := json.Marshal(GraphMatchEdge{From: "a", To: "b", MinWeight: &zero, MaxWeight: &zero})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"min_weight":0`) || !strings.Contains(string(encoded), `"max_weight":0`) {
		t.Fatalf("explicit zero weight range was omitted: %s", encoded)
	}
	if err := validateGraphMatchEdgeShape(GraphMatchEdge{From: "a", To: "b", MinWeight: &zero, MaxWeight: &negative}); err == nil {
		t.Fatal("expected inverted graph weight range to fail")
	}
	if err := validateGraphMatchEdgeShape(GraphMatchEdge{From: "a", To: "b", Types: []string{"links", "links"}}); err == nil {
		t.Fatal("expected duplicate graph edge types to fail")
	}
	if err := validateGraphMatchEdgeShape(GraphMatchEdge{From: "a", To: "b", Types: []string{""}}); err == nil {
		t.Fatal("expected empty graph edge type to fail")
	}
	if err := validateGraphMatchEdgeShape(GraphMatchEdge{From: "a", To: "b", Types: []string{"bad\xff"}}); err == nil {
		t.Fatal("expected invalid UTF-8 graph edge type to fail")
	}
	if err := validateGraphMatchEdgeShape(GraphMatchEdge{From: "a", To: "b", Types: []string{strings.Repeat("文", maxGraphEdgeTypeBytes/3+1)}}); err == nil {
		t.Fatal("expected encoded graph edge type byte limit to fail")
	}
}

func TestGraphTraversalAndPathDirectionValidation(t *testing.T) {
	start, err := NewGraphKeySelector("doc:a")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateGraphTraverseQuery(GraphTraverseQuery{
		Index:    "graph_idx",
		Traverse: GraphTraversal{Start: start, Direction: EdgeDirectionIn},
	}); err != nil {
		t.Fatalf("incoming traversal failed validation: %v", err)
	}
	if err := validateGraphTraverseQuery(GraphTraverseQuery{
		Index:    "graph_idx",
		Traverse: GraphTraversal{Start: start, Direction: EdgeDirection("sideways")},
	}); err == nil {
		t.Fatal("expected invalid traversal direction to fail")
	}
	if _, err := NewGraphShortestPathQuery(GraphShortestPathQuery{
		Index: "graph_idx",
		ShortestPath: GraphShortestPath{
			From:      GraphPathEndpoint{Key: "doc:a"},
			To:        GraphPathEndpoint{Key: "doc:b"},
			Direction: EdgeDirectionBoth,
		},
	}); err != nil {
		t.Fatalf("undirected shortest path failed validation: %v", err)
	}
	if _, err := NewGraphKShortestPathsQuery(GraphKShortestPathsQuery{
		Index: "graph_idx",
		KShortestPaths: GraphKShortestPaths{
			From:      GraphPathEndpoint{Key: "doc:a"},
			To:        GraphPathEndpoint{Key: "doc:b"},
			K:         2,
			Direction: EdgeDirection("sideways"),
		},
	}); err == nil {
		t.Fatal("expected invalid k-shortest-path direction to fail")
	}
}

func TestGraphResultRowUsesNullableTypedBindings(t *testing.T) {
	row := GraphResultRow{
		"author":   &GraphBindingNode{Key: "person:1"},
		"optional": nil,
	}
	encoded, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	var decoded GraphResultRow
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["author"] == nil || decoded["author"].Key != "person:1" {
		t.Fatalf("author binding = %#v", decoded["author"])
	}
	if decoded["optional"] != nil {
		t.Fatalf("optional binding = %#v", decoded["optional"])
	}
}

func mustMarshalGraphAggregates(t *testing.T, count int) string {
	t.Helper()
	aggregates := make(map[string]GraphCountAggregate, count)
	for i := 0; i < count; i++ {
		aggregates[fmt.Sprintf("count_%d", i)] = CountGraphRows()
	}
	encoded, err := json.Marshal(map[string]any{"aggregates": aggregates})
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}
