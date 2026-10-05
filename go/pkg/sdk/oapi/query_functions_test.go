// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
