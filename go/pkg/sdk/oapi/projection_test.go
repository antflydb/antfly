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
	"reflect"
	"testing"
)

func TestSelectPreservesFieldsAndRecursiveExpressions(t *testing.T) {
	body := []byte(`{"fields":["revision"],"params":{"cached":"opaque"},"select":{"identity":{"call":"value_identity","args":[{"field":"image"}]},"value":{"case":[{"when":{"is_distinct_from":[{"ref":"identity"},{"param":"cached"}]},"then":{"field":"image"}}],"else":{"literal":null}},"typed":{"cast":{"type":"integer","expression":{"literal":3}}}}}`)
	var request QueryRequest
	if err := json.Unmarshal(body, &request); err != nil {
		t.Fatal(err)
	}
	// This remains the existing typed SDK field mask.
	fields := []string{"revision"}
	request.Fields = &fields
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var expected, actual any
	if err := json.Unmarshal(body, &expected); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &actual); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("projection round trip differs: %s", encoded)
	}
}
