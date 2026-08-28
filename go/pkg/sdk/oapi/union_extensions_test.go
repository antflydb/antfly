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
