// Copyright 2026 The Antfly Contributors
// SPDX-License-Identifier: Apache-2.0

package oapi

import (
	"encoding/json"
	"testing"
)

func TestQueryExpressionsPreserveLiteralNullAndNestedCalls(t *testing.T) {
	input := QueryExpression{Literal: json.RawMessage("null")}
	expression := QueryExpression{
		Call:      QueryExpressionCallAiProbability,
		Input:     &input,
		Statement: "Refund?",
		Decider:   "local",
	}
	encoded, err := json.Marshal(expression)
	if err != nil {
		t.Fatal(err)
	}
	var decoded QueryExpression
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Input == nil || string(decoded.Input.Literal) != "null" {
		t.Fatalf("literal NULL was lost: %s", encoded)
	}
	if decoded.Call != expression.Call || decoded.Decider != expression.Decider {
		t.Fatalf("decision call changed: %#v", decoded)
	}
}
