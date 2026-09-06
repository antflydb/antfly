// Copyright 2026 Antfly, Inc.
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

func TestRateLimitPresenceRoundTrip(t *testing.T) {
	for name, newConfig := range map[string]func() any{
		"embedder":  func() any { return &EmbedderConfig{} },
		"generator": func() any { return &GeneratorConfig{} },
		"reranker":  func() any { return &RerankerConfig{} },
	} {
		t.Run(name, func(t *testing.T) {
			for _, policy := range []string{"", `{}`, `{"requests_per_minute":60,"max_concurrency":2}`} {
				input := `{"provider":"antfly","model":"test"`
				if policy != "" {
					input += `,"rate_limit":` + policy
				}
				input += `}`
				cfg := newConfig()
				if err := json.Unmarshal([]byte(input), cfg); err != nil {
					t.Fatal(err)
				}
				wire, err := json.Marshal(cfg)
				if err != nil {
					t.Fatal(err)
				}
				var object map[string]json.RawMessage
				if err := json.Unmarshal(wire, &object); err != nil {
					t.Fatal(err)
				}
				raw, present := object["rate_limit"]
				if present != (policy != "") {
					t.Fatalf("policy %q changed presence: %s", policy, wire)
				}
				if policy != "" {
					var actual, expected map[string]int64
					if err := json.Unmarshal(raw, &actual); err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal([]byte(policy), &expected); err != nil {
						t.Fatal(err)
					}
					if len(actual) != len(expected) {
						t.Fatalf("policy changed: %s", raw)
					}
					for k, v := range expected {
						if actual[k] != v {
							t.Fatalf("%s = %d, want %d", k, actual[k], v)
						}
					}
				}
			}
		})
	}
}

func TestLegacyEmbedderRateLimitDoesNotAcquireNestedPolicy(t *testing.T) {
	var cfg EmbedderConfig
	if err := json.Unmarshal([]byte(`{"provider":"openai","model":"text-embedding-3-small","requests_per_minute":60,"burst":2}`), &cfg); err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(wire, &object); err != nil {
		t.Fatal(err)
	}
	if _, present := object["rate_limit"]; present {
		t.Fatalf("injected conflicting policy: %s", wire)
	}
	if string(object["requests_per_minute"]) != "60" || string(object["burst"]) != "2" {
		t.Fatalf("lost legacy settings: %s", wire)
	}
}
