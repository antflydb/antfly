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

package query

import (
	"encoding/json"
	"testing"
)

func TestLogicalQueryPreservesExplicitCaseFolding(t *testing.T) {
	sensitive := false
	queries := []any{
		ContainsQuery{Contains: "kelvin", Field: "content", CaseSensitive: &sensitive},
		EqualsQuery{Equals: "kelvin", Field: "content", CaseSensitive: &sensitive},
		MatchesQuery{Matches: "kelvin", Field: "content", CaseSensitive: &sensitive},
	}
	for _, query := range queries {
		encoded, err := json.Marshal(query)
		if err != nil {
			t.Fatal(err)
		}
		var wire map[string]any
		if err := json.Unmarshal(encoded, &wire); err != nil {
			t.Fatal(err)
		}
		if value, present := wire["case_sensitive"]; !present || value != false {
			t.Fatalf("explicit case folding was lost: %s", encoded)
		}
	}
	encoded, err := json.Marshal(ContainsQuery{Contains: "x", Field: "content"})
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	if _, present := wire["case_sensitive"]; present {
		t.Fatalf("omission must retain the server's case-sensitive default: %s", encoded)
	}
}
