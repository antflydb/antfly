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
	"testing"
)

func TestGraphQueryConstructors(t *testing.T) {
	graphReturn, err := NewGraphAggregatesReturn(map[string]GraphCountAggregate{
		"rows":      CountGraphRows(),
		"neighbors": CountGraphAlias("b", true),
	})
	if err != nil {
		t.Fatal(err)
	}
	query, err := NewGraphMatchQuery(GraphMatchQuery{
		Index:  "graph_idx",
		Match:  GraphMatch{},
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
}
