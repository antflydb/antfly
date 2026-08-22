// Copyright 2026 The Antfly Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package sdk

import (
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
			Nodes: map[string]GraphMatchNode{"a": {}, "b": {}},
			Edges: []GraphMatchEdge{{From: "a", To: "b", Types: []string{"links"}}},
			Where: where,
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
	filter, err := NewGraphFilter(querydsl.TermQuery{Term: "beta", Field: "title"}.ToQuery())
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
			Nodes: map[string]GraphMatchNode{"a": {}, "b": {}},
			Edges: []GraphMatchEdge{{From: "a", To: "b"}},
		},
		Return: graphReturn,
	}); err == nil {
		t.Fatal("expected unknown return alias error")
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
