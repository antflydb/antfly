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
	"math"
	"net/http"
	"testing"
)

func TestMemorySupersessionBoundaryPreservesUnknownAndEpoch(t *testing.T) {
	for _, tc := range []struct {
		name     string
		raw      string
		known    bool
		boundary int64
	}{
		{name: "missing", raw: `{}`},
		{name: "unknown", raw: `{"superseded_valid_at":null,"explicit_disposition":{"superseded_valid_at":null}}`},
		{name: "epoch", raw: `{"superseded_valid_at":0,"explicit_disposition":{"superseded_valid_at":0}}`, known: true},
		{name: "dated", raw: `{"superseded_valid_at":300,"explicit_disposition":{"superseded_valid_at":300}}`, known: true, boundary: 300},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var record MemoryRecord
			if err := json.Unmarshal([]byte(tc.raw), &record); err != nil {
				t.Fatal(err)
			}
			for _, boundary := range []*int64{record.SupersededValidAt, record.ExplicitDisposition.SupersededValidAt} {
				if (boundary != nil) != tc.known {
					t.Fatalf("replacement time lost its known/unknown state: %v", boundary)
				}
				if boundary != nil && *boundary != tc.boundary {
					t.Fatalf("replacement time = %d, want %d", *boundary, tc.boundary)
				}
			}
			encoded, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			var decoded MemoryRecord
			if err = json.Unmarshal(encoded, &decoded); err != nil {
				t.Fatal(err)
			}
			if (decoded.SupersededValidAt != nil) != tc.known {
				t.Fatalf("round trip lost the replacement boundary: %s", encoded)
			}
			if tc.known && *decoded.SupersededValidAt != tc.boundary {
				t.Fatalf("round trip changed the replacement boundary: %s", encoded)
			}
		})
	}
}

// Temporal presence must survive generated typed requests, not just raw JSON.
func TestMemoryNullableNumbersPreserveZeroAndAbsence(t *testing.T) {
	cases := []struct {
		name, raw string
		newValue  func() any
		fields    []string
	}{
		{"recall", `{"known_at":0,"valid_at":0}`, func() any { return &MemoryRecallRequest{} }, []string{"known_at", "valid_at"}},
		{"explain", `{"known_at":0,"valid_at":0}`, func() any { return &MemoryExplainRequest{} }, []string{"known_at", "valid_at"}},
		{"assertion", `{"valid_from":0,"valid_to":0}`, func() any { return &MemoryAssertion{} }, []string{"valid_from", "valid_to"}},
		{"evidence", `{"event_time":0,"expires_at":0,"ordinal":0}`, func() any { return &MemoryEvidence{} }, []string{"event_time", "expires_at", "ordinal"}},
		{"config", `{"retention_ms":0}`, func() any { return &MemorySpaceConfig{} }, []string{"retention_ms"}},
		{"record", `{"asserted_valid_to":0,"recorded_to":0}`, func() any { return &MemoryRecord{} }, []string{"asserted_valid_to", "recorded_to"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, present := range []bool{false, true} {
				raw := `{}`
				if present {
					raw = tc.raw
				}
				value := tc.newValue()
				if err := json.Unmarshal([]byte(raw), value); err != nil {
					t.Fatal(err)
				}
				encoded, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				var fields map[string]json.RawMessage
				if err = json.Unmarshal(encoded, &fields); err != nil {
					t.Fatal(err)
				}
				for _, name := range tc.fields {
					field, exists := fields[name]
					if present && (!exists || string(field) != "0") {
						t.Fatalf("explicit zero %s lost: %s", name, encoded)
					}
					if !present && exists && string(field) != "null" {
						t.Fatalf("absent %s became a timestamp: %s", name, encoded)
					}
				}
			}
		})
	}
}

func TestMemoryListQueryPreservesEpochZero(t *testing.T) {
	epoch := int64(0)
	request, err := NewListMemoriesRequest("http://memory.test", "team", &ListMemoriesParams{KnownAt: &epoch, ValidAt: &epoch})
	if err != nil {
		t.Fatal(err)
	}
	query := request.URL.Query()
	for _, name := range []string{"known_at", "valid_at"} {
		if !query.Has(name) || query.Get(name) != "0" {
			t.Fatalf("epoch query %s lost: %s", name, request.URL)
		}
	}
}

func TestMemoryHTTPOptionalParametersPreserveAbsence(t *testing.T) {
	database := "projects"
	requests := []func() (*http.Request, error){
		func() (*http.Request, error) {
			return NewListMemoriesRequest("http://memory.test", "team", &ListMemoriesParams{XAntflyDatabase: &database})
		},
		func() (*http.Request, error) {
			return NewGetMemoryHistoryRequest("http://memory.test", "team", "claim", &GetMemoryHistoryParams{XAntflyDatabase: &database})
		},
		func() (*http.Request, error) {
			return NewListMemoryEntitiesRequest("http://memory.test", "team", &ListMemoryEntitiesParams{XAntflyDatabase: &database})
		},
	}
	for _, build := range requests {
		request, err := build()
		if err != nil {
			t.Fatal(err)
		}
		if request.URL.RawQuery != "" {
			t.Fatalf("absent parameters sent: %s", request.URL)
		}
		if got := request.Header.Get("X-Antfly-Database"); got != database {
			t.Fatalf("scope lost: %q", got)
		}
		if got := request.Header.Values("X-Antfly-Namespace"); len(got) != 0 {
			t.Fatalf("absent namespace sent: %v", got)
		}
	}
}

func TestMemoryProvisionalFilterPreservesOmittedFalseAndTrue(t *testing.T) {
	for _, raw := range []string{`{}`, `{"include_provisional":false}`, `{"include_provisional":true}`} {
		var request MemoryRecallRequest
		if err := json.Unmarshal([]byte(raw), &request); err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		if string(encoded) != raw {
			t.Fatalf("filter changed: %s -> %s", raw, encoded)
		}
	}
	exclude := false
	request, err := NewListMemoriesRequest("http://memory.test", "team", &ListMemoriesParams{IncludeProvisional: &exclude})
	if err != nil {
		t.Fatal(err)
	}
	if q := request.URL.Query(); len(q) != 1 || q.Get("include_provisional") != "false" {
		t.Fatalf("filter changed: %s", request.URL)
	}
}

func TestMemoryNativeIntegerDomainsRoundTrip(t *testing.T) {
	var config MemorySpaceConfig
	if err := json.Unmarshal([]byte(`{"retention_ms":18446744073709551615}`), &config); err != nil {
		t.Fatal(err)
	}
	if config.RetentionMs == nil || *config.RetentionMs != math.MaxUint64 {
		t.Fatalf("uint64 lost: %v", config.RetentionMs)
	}
	var correction MemoryCorrection
	if err := json.Unmarshal([]byte(`{"expected_revision":18446744073709551615}`), &correction); err != nil {
		t.Fatal(err)
	}
	if correction.ExpectedRevision != math.MaxUint64 {
		t.Fatalf("uint64 revision lost: %v", correction.ExpectedRevision)
	}
	var request MemoryRecallRequest
	if err := json.Unmarshal([]byte(`{"known_at":-9223372036854775808,"valid_at":9223372036854775807}`), &request); err != nil {
		t.Fatal(err)
	}
	if request.KnownAt == nil || *request.KnownAt != math.MinInt64 || request.ValidAt == nil || *request.ValidAt != math.MaxInt64 {
		t.Fatal("signed timestamp bounds lost")
	}
	encoded, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	var decoded MemorySpaceConfig
	if err = json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.RetentionMs == nil || *decoded.RetentionMs != math.MaxUint64 {
		t.Fatalf("round trip lost retention: %s", encoded)
	}
}
