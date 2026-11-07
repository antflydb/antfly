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

package sdk

import (
	"encoding/json"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

func TestDecisionRevisionManifestPreservesEmptyPresence(t *testing.T) {
	for _, raw := range []string{
		`{"name":"proxy","decision_method":"typed","escalated":false}`,
		`{"name":"proxy","decision_method":"typed","escalated":false,"upstream_revisions":[]}`,
		`{"name":"strong","decision_method":"typed","escalated":false,"agreement_questions":["same"],"agreement_status":"agreed","agreed":true}`,
	} {
		var trace oapi.DecisionCascadeStageTrace
		if err := json.Unmarshal([]byte(raw), &trace); err != nil {
			t.Fatal(err)
		}
		wire, err := json.Marshal(trace)
		if err != nil {
			t.Fatal(err)
		}
		var original, roundtrip map[string]json.RawMessage
		if err := json.Unmarshal([]byte(raw), &original); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(wire, &roundtrip); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"upstream_revisions", "agreement_questions"} {
			if string(original[key]) != string(roundtrip[key]) {
				t.Fatalf("lost %s: %s -> %s", key, raw, wire)
			}
		}
	}
}
