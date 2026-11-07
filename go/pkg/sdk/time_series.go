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
	"fmt"
	"math/big"
	"time"
)

// SeriesQueryOptions builds ordinary exact JSON aggregations. Start and End
// are preserved as RFC3339Nano instants. GroupBy fields use keyword mappings.
type SeriesQueryOptions struct {
	Table, Timestamp, Measurement string
	Start, End                    time.Time
	Every                         time.Duration
	GroupBy                       []string
	Dimensions                    map[string]string
	MaxSeries                     int
}

// NewSeriesQuery returns average/min/max buckets in a half-open time range.
// Materialization reuse remains the server planner's decision.
func NewSeriesQuery(options SeriesQueryOptions) (*QueryRequest, error) {
	if options.Timestamp == "" || options.Measurement == "" || !options.Start.Before(options.End) || options.Every <= 0 {
		return nil, fmt.Errorf("antfly: timestamp, measurement, increasing range and positive interval required")
	}
	ns := func(value time.Time) *big.Int {
		return new(big.Int).Add(new(big.Int).Mul(big.NewInt(value.Unix()), big.NewInt(1_000_000_000)), big.NewInt(int64(value.Nanosecond())))
	}
	width := big.NewInt(int64(options.Every))
	first := new(big.Int).Div(ns(options.Start), width)
	last := new(big.Int).Div(new(big.Int).Sub(ns(options.End), big.NewInt(1)), width)
	buckets := new(big.Int).Add(new(big.Int).Sub(last, first), big.NewInt(1))
	if buckets.Cmp(big.NewInt(10000)) > 0 {
		return nil, fmt.Errorf("antfly: select a coarser interval (10000 bucket limit)")
	}
	maxSeries := options.MaxSeries
	if maxSeries == 0 {
		maxSeries = 100
	}
	if maxSeries < 1 || maxSeries > 1000 {
		return nil, fmt.Errorf("antfly: max series must be 1–1000")
	}
	start, end := options.Start.Format(time.RFC3339Nano), options.End.Format(time.RFC3339Nano)
	metrics := map[string]any{}
	for _, op := range []string{"avg", "min", "max"} {
		metrics[op] = map[string]any{"type": op, "field": options.Measurement}
	}
	points := map[string]any{"type": "date_histogram", "field": options.Timestamp, "fixed_interval": fmt.Sprintf("%dns", options.Every.Nanoseconds()), "date_bounds": map[string]any{"start": start, "end": end}, "min_doc_count": 0, "sub_aggregations": metrics}
	aggregations := map[string]any{"points": points}
	if len(options.GroupBy) > 0 {
		aggregations = map[string]any{"series": map[string]any{"type": "terms", "fields": options.GroupBy, "size": maxSeries, "sub_aggregations": aggregations}}
	}
	clauses := []any{map[string]any{"field": options.Timestamp, "start": start, "end": end, "inclusive_start": true, "inclusive_end": false}}
	for field, value := range options.Dimensions {
		clauses = append(clauses, map[string]any{"field": field, "equals": value})
	}
	wire := map[string]any{"table": options.Table, "limit": 0, "filter_query": map[string]any{"conjuncts": clauses}, "aggregations": aggregations}
	data, err := json.Marshal(wire)
	if err != nil {
		return nil, err
	}
	var request QueryRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, err
	}
	return &request, nil
}
