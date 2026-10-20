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

func TestMemorySupersessionBoundaryPreservesUnknownAndEpoch(t *testing.T) {
	for _, tc := range []struct {
		name     string
		raw      string
		known    bool
		boundary int
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
			for _, boundary := range []*int{record.SupersededValidAt, record.ExplicitDisposition.SupersededValidAt} {
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
