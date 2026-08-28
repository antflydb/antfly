// Copyright 2026 The Antfly Contributors
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
	"strings"
	"testing"
)

func TestCanonicalGraphOpaqueObjectsRejectExplicitNull(t *testing.T) {
	tests := []struct {
		name    string
		encoded string
		value   func() any
		message string
	}{
		{
			name:    "binding document",
			encoded: `{"key":"node","document":null}`,
			value:   func() any { return &GraphBindingNode{} },
			message: "graph binding document must be omitted or an object",
		},
		{
			name:    "result document",
			encoded: `{"key":"node","depth":0,"document":null}`,
			value:   func() any { return &GraphResultNode{} },
			message: "graph result document must be omitted or an object",
		},
		{
			name:    "result evidence",
			encoded: `{"key":"node","depth":0,"evidence":null}`,
			value:   func() any { return &GraphResultNode{} },
			message: "graph result evidence must be omitted or an object",
		},
		{
			name:    "path edge metadata",
			encoded: `{"from":{"key":"a"},"to":{"key":"b"},"direction":"out","type":"edge","weight":1,"metadata":null}`,
			value:   func() any { return &GraphPathEdge{} },
			message: "graph path edge metadata must be omitted or an object",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := json.Unmarshal([]byte(test.encoded), test.value())
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("expected %q, got %v", test.message, err)
			}
		})
	}
}

func TestCanonicalGraphOpaqueObjectsAcceptObjectOrOmission(t *testing.T) {
	var binding GraphBindingNode
	if err := json.Unmarshal([]byte(`{"key":"node","document":{"title":"hello"}}`), &binding); err != nil {
		t.Fatal(err)
	}
	if binding.Document["title"] != "hello" {
		t.Fatalf("document = %#v", binding.Document)
	}

	var node GraphResultNode
	if err := json.Unmarshal([]byte(`{"key":"node","depth":0,"evidence":{"source":"edge"}}`), &node); err != nil {
		t.Fatal(err)
	}
	if node.Document != nil || node.Evidence["source"] != "edge" {
		t.Fatalf("document = %#v, evidence = %#v", node.Document, node.Evidence)
	}

	var edge GraphPathEdge
	if err := json.Unmarshal([]byte(`{"from":{"key":"a"},"to":{"key":"b"},"direction":"out","type":"edge","weight":1}`), &edge); err != nil {
		t.Fatal(err)
	}
	if edge.Metadata != nil {
		t.Fatalf("metadata = %#v", edge.Metadata)
	}
}

func TestCanonicalGraphNullScanDoesNotAllocate(t *testing.T) {
	encoded := []byte(`{"index":"graph","match":{"anchor":"a","nodes":{"a":{"filter":{"term":"null","path":"/title"}}},"edges":[]},"return":{"bindings":["a"]}}`)
	if allocations := testing.AllocsPerRun(1000, func() {
		if err := rejectCanonicalGraphNulls(encoded); err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("canonical graph null scan allocated %v objects per call", allocations)
	}
}

func TestGraphRequestUnionExtensionsSelectStrictArms(t *testing.T) {
	t.Run("selector", func(t *testing.T) {
		var selector GraphNodeSelector
		if err := json.Unmarshal([]byte(`{"keys":["a","b"]}`), &selector); err != nil {
			t.Fatal(err)
		}
		decoded, err := selector.DecodeStrictVariant()
		if err != nil {
			t.Fatal(err)
		}
		if decoded.Kind != GraphNodeSelectorVariantKeys || len(decoded.Keys.Keys) != 2 {
			t.Fatalf("decoded selector = %#v", decoded)
		}

		if err := json.Unmarshal([]byte(`{"keys":["a"],"limit":1}`), &selector); err != nil {
			t.Fatal(err)
		}
		if _, err := selector.DecodeStrictVariant(); err == nil {
			t.Fatal("expected result-reference-only option to be rejected")
		}
	})

	t.Run("where", func(t *testing.T) {
		var where GraphWhereExpression
		if err := json.Unmarshal([]byte(`{"not_equal":{"left":{"alias":"a"},"right":{"alias":"b"}}}`), &where); err != nil {
			t.Fatal(err)
		}
		decoded, err := where.DecodeStrictVariant()
		if err != nil {
			t.Fatal(err)
		}
		if decoded.Kind != GraphWhereVariantNotEqual || decoded.NotEqual.NotEqual.Left.Alias != "a" {
			t.Fatalf("decoded where = %#v", decoded)
		}

		if err := json.Unmarshal([]byte(`{"and":[],"not_exists":{"edges":[]}}`), &where); err != nil {
			t.Fatal(err)
		}
		if _, err := where.DecodeStrictVariant(); err == nil {
			t.Fatal("expected multiple predicate arms to be rejected")
		}
	})

	t.Run("count", func(t *testing.T) {
		var aggregate GraphCountAggregate
		if err := json.Unmarshal([]byte(`{"count":"a","distinct":true}`), &aggregate); err != nil {
			t.Fatal(err)
		}
		decoded, err := aggregate.DecodeStrictVariant()
		if err != nil {
			t.Fatal(err)
		}
		if decoded.Count != "a" || !decoded.Distinct {
			t.Fatalf("decoded count = %#v", decoded)
		}

		if err := json.Unmarshal([]byte(`{"count":"*","distinct":false}`), &aggregate); err != nil {
			t.Fatal(err)
		}
		if _, err := aggregate.DecodeStrictVariant(); err == nil {
			t.Fatal("expected distinct count(*) member to be rejected")
		}
	})

	t.Run("document filter", func(t *testing.T) {
		var filter GraphDocumentFilter
		if err := json.Unmarshal([]byte(`{"term":"beta","path":"/title"}`), &filter); err != nil {
			t.Fatal(err)
		}
		decoded, err := filter.DecodeStrictVariant()
		if err != nil {
			t.Fatal(err)
		}
		if decoded.Kind != GraphDocumentFilterVariantTerm || decoded.Term.Path != "/title" {
			t.Fatalf("decoded filter = %#v", decoded)
		}

		if err := json.Unmarshal([]byte(`{"term":null,"path":"/title"}`), &filter); err != nil {
			t.Fatal(err)
		}
		if _, err := filter.DecodeStrictVariant(); err == nil || !strings.Contains(err.Error(), "term must not be null") {
			t.Fatalf("expected null term to be rejected, got %v", err)
		}

		if err := json.Unmarshal([]byte(`{"term":"beta","path":"/title","boost":2}`), &filter); err != nil {
			t.Fatal(err)
		}
		if _, err := filter.DecodeStrictVariant(); err == nil {
			t.Fatal("expected unknown scoring option to be rejected")
		}
	})
}
