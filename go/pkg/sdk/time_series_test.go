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
	"strings"
	"testing"
	"time"
)

func TestSeriesQueryPreservesTemporalContract(t *testing.T) {
	start := time.Date(2026, 11, 6, 12, 0, 0, 1, time.UTC)
	request, err := NewSeriesQuery(SeriesQueryOptions{Table: "readings", Timestamp: "observed_at", Measurement: "temperature", Start: start, End: start.Add(time.Hour), Every: 5 * time.Minute, GroupBy: []string{"device_id"}, Dimensions: map[string]string{"device_id": "sensor"}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{"300000000000ns", "2026-11-06T12:00:00.000000001Z", "date_bounds", "inclusive_end", `"limit":0`, "sub_aggregations", `"equals":"sensor"`} {
		if !strings.Contains(string(data), part) {
			t.Fatalf("missing %q: %s", part, data)
		}
	}
}

func TestSeriesQueryExactBucketLimit(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	opts := SeriesQueryOptions{Table: "r", Timestamp: "ts", Measurement: "v", Start: start, End: start.Add(10000 * time.Second), Every: time.Second}
	if _, err := NewSeriesQuery(opts); err != nil {
		t.Fatal(err)
	}
	opts.End = opts.End.Add(time.Nanosecond)
	if _, err := NewSeriesQuery(opts); err == nil {
		t.Fatal("expected 10001 bucket rejection")
	}
}
