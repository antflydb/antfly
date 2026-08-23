// Copyright 2026 The Antfly Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package sdk

import (
	"fmt"
	"testing"

	"github.com/antflydb/antfly/go/pkg/libaf/json"
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
	graphReturn, err := NewGraphAggregatesReturn(map[string]GraphCountAggregate{
		"rows":      CountGraphRows(),
		"neighbors": CountGraphAlias("b", true),
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
			Start:  start,
			Limit:  25,
			Filter: filter,
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
	startJSON := traverse["start"].(map[string]any)
	if keys, ok := startJSON["keys"].([]any); !ok || len(keys) != 2 {
		t.Fatalf("start = %#v", startJSON)
	}
	filterJSON := traverse["filter"].(map[string]any)
	if filterJSON["term"] != "beta" || filterJSON["field"] != "title" {
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
	if _, err := NewGraphDocumentFilter(querydsl.NumericRangeQuery{Field: "score"}.ToQuery()); err == nil {
		t.Fatal("expected boundless graph range filter to fail")
	}
	if _, err := NewGraphResultRefSelector("$embeddings_results.vector_idx", 10); err == nil {
		t.Fatal("expected invalid result reference")
	}
	if _, err := NewGraphKeySelector("doc:a", "doc:a"); err == nil {
		t.Fatal("expected duplicate key error")
	}
	if _, err := NewGraphIdentitySelector(
		GraphPathEndpoint{Table: "docs", Key: "doc:a"},
		GraphPathEndpoint{Table: "docs", Key: "doc:a"},
	); err == nil {
		t.Fatal("expected duplicate identity error")
	}
	if _, err := NewGraphBindingsReturn([]string{"a"}, GraphBindingsOptions{Fields: []string{"title"}}); err == nil {
		t.Fatal("expected projection hydration error")
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
	if !ok || body["field"] != "score" || body["min"] != float64(0) || body["max"] != float64(10) || body["inclusive_min"] != false {
		t.Fatalf("unexpected graph range filter: %#v", value)
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
	if _, err := NewGraphAggregatesReturn(map[string]GraphCountAggregate{
		"first":  CountGraphAlias("person", true),
		"second": CountGraphAlias("person", true),
	}); err != nil {
		t.Fatal(err)
	}
}

func TestGraphQueryResultUsesStableDiscriminator(t *testing.T) {
	var result GraphQueryResult
	if err := json.Unmarshal([]byte(`{"kind":"nodes","nodes":[],"paths":[],"stats":{"returned_items":0,"truncated":false},"took":1}`), &result); err != nil {
		t.Fatal(err)
	}

	kind, err := result.Discriminator()
	if err != nil {
		t.Fatal(err)
	}
	if kind != "nodes" {
		t.Fatalf("kind = %q, want nodes", kind)
	}
	value, err := DecodeGraphQueryResult(result)
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

	if err := json.Unmarshal([]byte(`{"type":"neighbors","nodes":[],"paths":[],"total":0,"took":1}`), &result); err != nil {
		t.Fatal(err)
	}
	value, err = DecodeGraphQueryResult(result)
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

	if err := json.Unmarshal([]byte(`{"kind":"unknown"}`), &result); err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeGraphQueryResult(result); err == nil {
		t.Fatal("expected unknown graph result kind to fail")
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
	if _, err := NewGraphResultRefSelector("$full_text_results", 1); err == nil {
		t.Fatal("expected legacy lane-specific reference to fail in canonical helper")
	}
}

func TestGraphResultRowUsesNullableTypedBindings(t *testing.T) {
	row := GraphResultRow{
		"author":   &GraphResultNode{Key: "person:1"},
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
