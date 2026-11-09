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

func TestContextManagementKeepRecentTurnsPresence(t *testing.T) {
	zero, two := 0, 2
	for _, tc := range []struct {
		name  string
		value *int
		want  string
	}{
		{name: "omitted", value: nil},
		{name: "explicit zero", value: &zero, want: "0"},
		{name: "nonzero", value: &two, want: "2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := json.Marshal(AgentContextManagement{ContextWindowTokens: 32768, KeepRecentTurns: tc.value})
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(encoded, &fields); err != nil {
				t.Fatal(err)
			}
			got, present := fields["keep_recent_turns"]
			if present != (tc.value != nil) || string(got) != tc.want {
				t.Fatalf("keep_recent_turns presence/value: got %s in %s; want %q", got, encoded, tc.want)
			}
		})
	}
}

func TestContextEvidenceSourceIdentityPresence(t *testing.T) {
	for _, raw := range []string{
		`{"tool_call_id":"c1","tool_name":"search","facts":"fact"}`,
		`{"tool_call_id":"c1","tool_name":"search","facts":"fact","source_ids":[]}`,
		`{"tool_call_id":"c1","tool_name":"search","facts":"fact","source_ids":["doc:1","doc:1-extra","quoted\"id","世界"]}`,
	} {
		var value InferenceContextToolEvidence
		if err := json.Unmarshal([]byte(raw), &value); err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var before, after map[string]json.RawMessage
		if err := json.Unmarshal([]byte(raw), &before); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(encoded, &after); err != nil {
			t.Fatal(err)
		}
		if string(before["source_ids"]) != string(after["source_ids"]) {
			t.Fatalf("source identity presence/value changed: %s -> %s", raw, encoded)
		}
	}
}
