// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package metadata

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAggregationRequest_MetricAggregations tests metric aggregation types
func TestAggregationRequest_MetricAggregations(t *testing.T) {
	tests := []struct {
		name     string
		aggType  AggregationType
		field    string
		wantType AggregationType
	}{
		{"Sum", "sum", "price", "sum"},
		{"Avg", "avg", "rating", "avg"},
		{"Min", "min", "temperature", "min"},
		{"Max", "max", "score", "max"},
		{"Count", "count", "items", "count"},
		{"Stats", "stats", "revenue", "stats"},
		{"Cardinality", "cardinality", "user_id", "cardinality"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg := AggregationRequest{
				Type:  tt.aggType,
				Field: tt.field,
			}

			assert.Equal(t, tt.wantType, agg.Type)
			assert.Equal(t, tt.field, agg.Field)
		})
	}
}

// TestAggregationRequest_BucketAggregations tests bucket aggregation types
func TestAggregationRequest_BucketAggregations(t *testing.T) {
	t.Run("Terms", func(t *testing.T) {
		size := 10
		minDocCount := 1
		agg := AggregationRequest{
			Type:        "terms",
			Field:       "category",
			Size:        &size,
			MinDocCount: &minDocCount,
		}

		assert.Equal(t, AggregationType("terms"), agg.Type)
		assert.Equal(t, "category", agg.Field)
		assert.NotNil(t, agg.Size)
		assert.Equal(t, 10, *agg.Size)
		assert.NotNil(t, agg.MinDocCount)
		assert.Equal(t, 1, *agg.MinDocCount)
	})

	t.Run("Range", func(t *testing.T) {
		from1 := float64(0)
		to1 := float64(100)
		from2 := float64(100)
		to2 := float64(500)
		from3 := float64(500)

		agg := AggregationRequest{
			Type:  "range",
			Field: "price",
			Ranges: []AggregationRange{
				{Name: "cheap", From: &from1, To: &to1},
				{Name: "medium", From: &from2, To: &to2},
				{Name: "expensive", From: &from3},
			},
		}

		assert.Equal(t, AggregationType("range"), agg.Type)
		assert.Len(t, agg.Ranges, 3)
		assert.Equal(t, "cheap", agg.Ranges[0].Name)
		assert.NotNil(t, agg.Ranges[0].From)
		assert.Equal(t, float64(0), *agg.Ranges[0].From)
		assert.NotNil(t, agg.Ranges[0].To)
		assert.Equal(t, float64(100), *agg.Ranges[0].To)
		assert.Nil(t, agg.Ranges[2].To) // No upper bound for expensive
	})

	t.Run("DateRange", func(t *testing.T) {
		from1 := "2024-01-01"
		to1 := "2024-06-30"
		from2 := "2024-07-01"
		to2 := "2024-12-31"

		agg := AggregationRequest{
			Type:  "date_range",
			Field: "created_at",
			DateRanges: []AggregationDateRange{
				{Name: "H1", From: &from1, To: &to1},
				{Name: "H2", From: &from2, To: &to2},
			},
		}

		assert.Equal(t, AggregationType("date_range"), agg.Type)
		assert.Len(t, agg.DateRanges, 2)
		assert.Equal(t, "H1", agg.DateRanges[0].Name)
		assert.NotNil(t, agg.DateRanges[0].From)
		assert.Equal(t, "2024-01-01", *agg.DateRanges[0].From)
	})

	t.Run("Histogram", func(t *testing.T) {
		interval := float64(10)
		agg := AggregationRequest{
			Type:     "histogram",
			Field:    "age",
			Interval: &interval,
		}

		assert.Equal(t, AggregationType("histogram"), agg.Type)
		assert.NotNil(t, agg.Interval)
		assert.Equal(t, float64(10), *agg.Interval)
	})

	t.Run("DateHistogram", func(t *testing.T) {
		calendarInterval := CalendarInterval("day")
		agg := AggregationRequest{
			Type:             "date_histogram",
			Field:            "timestamp",
			CalendarInterval: &calendarInterval,
		}

		assert.Equal(t, AggregationType("date_histogram"), agg.Type)
		assert.NotNil(t, agg.CalendarInterval)
		assert.Equal(t, CalendarInterval("day"), *agg.CalendarInterval)
	})
}

// TestAggregationRequest_SubAggregations tests nested aggregations
func TestAggregationRequest_SubAggregations(t *testing.T) {
	t.Run("TermsWithAvg", func(t *testing.T) {
		size := 10
		agg := AggregationRequest{
			Type:  "terms",
			Field: "category",
			Size:  &size,
			SubAggregations: map[string]AggregationRequest{
				"avg_price": {
					Type:  "avg",
					Field: "price",
				},
			},
		}

		assert.Equal(t, AggregationType("terms"), agg.Type)
		assert.NotNil(t, agg.SubAggregations)
		assert.Len(t, agg.SubAggregations, 1)
		assert.Contains(t, agg.SubAggregations, "avg_price")

		subAgg := agg.SubAggregations["avg_price"]
		assert.Equal(t, AggregationType("avg"), subAgg.Type)
		assert.Equal(t, "price", subAgg.Field)
	})

	t.Run("RangeWithStats", func(t *testing.T) {
		from := float64(0)
		to := float64(1000)

		agg := AggregationRequest{
			Type:  "range",
			Field: "price",
			Ranges: []AggregationRange{
				{Name: "all", From: &from, To: &to},
			},
			SubAggregations: map[string]AggregationRequest{
				"stats": {
					Type:  "stats",
					Field: "price",
				},
			},
		}

		assert.NotNil(t, agg.SubAggregations)
		assert.Contains(t, agg.SubAggregations, "stats")
		assert.Equal(t, AggregationType("stats"), agg.SubAggregations["stats"].Type)
	})

	t.Run("MultiLevelNesting", func(t *testing.T) {
		size := 10
		agg := AggregationRequest{
			Type:  "terms",
			Field: "category",
			Size:  &size,
			SubAggregations: map[string]AggregationRequest{
				"by_brand": {
					Type:  "terms",
					Field: "brand",
					Size:  &size,
					SubAggregations: map[string]AggregationRequest{
						"avg_price": {
							Type:  "avg",
							Field: "price",
						},
						"max_price": {
							Type:  "max",
							Field: "price",
						},
					},
				},
			},
		}

		assert.NotNil(t, agg.SubAggregations)
		assert.Contains(t, agg.SubAggregations, "by_brand")

		brandAgg := agg.SubAggregations["by_brand"]
		assert.Equal(t, AggregationType("terms"), brandAgg.Type)
		assert.NotNil(t, brandAgg.SubAggregations)
		assert.Len(t, brandAgg.SubAggregations, 2)
		assert.Contains(t, brandAgg.SubAggregations, "avg_price")
		assert.Contains(t, brandAgg.SubAggregations, "max_price")
	})
}

// TestAggregationRequest_JSONMarshaling tests JSON serialization
func TestAggregationRequest_JSONMarshaling(t *testing.T) {
	t.Run("SimpleMetric", func(t *testing.T) {
		agg := AggregationRequest{
			Type:  "sum",
			Field: "revenue",
		}

		data, err := json.Marshal(agg)
		require.NoError(t, err)

		var decoded AggregationRequest
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)

		assert.Equal(t, agg.Type, decoded.Type)
		assert.Equal(t, agg.Field, decoded.Field)
	})

	t.Run("ComplexBucket", func(t *testing.T) {
		size := 10
		from := float64(0)
		to := float64(100)

		agg := AggregationRequest{
			Type:  "range",
			Field: "price",
			Size:  &size,
			Ranges: []AggregationRange{
				{Name: "affordable", From: &from, To: &to},
			},
			SubAggregations: map[string]AggregationRequest{
				"avg": {Type: "avg", Field: "rating"},
			},
		}

		data, err := json.Marshal(agg)
		require.NoError(t, err)

		var decoded AggregationRequest
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)

		assert.Equal(t, agg.Type, decoded.Type)
		assert.Equal(t, agg.Field, decoded.Field)
		assert.NotNil(t, decoded.Size)
		assert.Equal(t, 10, *decoded.Size)
		assert.Len(t, decoded.Ranges, 1)
		assert.NotNil(t, decoded.SubAggregations)
	})
}

// TestAggregationResult_MetricResults tests metric aggregation results
func TestAggregationResult_MetricResults(t *testing.T) {
	t.Run("SingleValue", func(t *testing.T) {
		value := float64(12345.67)
		result := AggregationResult{
			Value: &value,
		}

		assert.NotNil(t, result.Value)
		assert.Equal(t, 12345.67, *result.Value)
	})

	t.Run("Stats", func(t *testing.T) {
		count := 100
		min := float64(10.5)
		max := float64(999.9)
		sum := float64(50000)
		sumOfSquares := float64(30000000)
		avg := float64(500)
		stdDeviation := float64(150.5)
		variance := float64(22650.25)

		result := AggregationResult{
			Count:        &count,
			Min:          &min,
			Max:          &max,
			Sum:          &sum,
			SumOfSquares: &sumOfSquares,
			Avg:          &avg,
			StdDeviation: &stdDeviation,
			Variance:     &variance,
		}

		assert.NotNil(t, result.Count)
		assert.Equal(t, 100, *result.Count)
		assert.NotNil(t, result.Min)
		assert.Equal(t, 10.5, *result.Min)
		assert.NotNil(t, result.Max)
		assert.Equal(t, 999.9, *result.Max)
		assert.NotNil(t, result.Sum)
		assert.Equal(t, 50000.0, *result.Sum)
		assert.NotNil(t, result.Avg)
		assert.Equal(t, 500.0, *result.Avg)
		assert.NotNil(t, result.StdDeviation)
		assert.Equal(t, 150.5, *result.StdDeviation)
	})
}

// TestAggregationResult_BucketResults tests bucket aggregation results
func TestAggregationResult_BucketResults(t *testing.T) {
	t.Run("TermsBuckets", func(t *testing.T) {
		result := AggregationResult{
			Buckets: []AggregationBucket{
				{Key: "electronics", DocCount: 150},
				{Key: "books", DocCount: 120},
				{Key: "clothing", DocCount: 95},
			},
		}

		assert.NotNil(t, result.Buckets)
		assert.Len(t, result.Buckets, 3)
		assert.Equal(t, "electronics", result.Buckets[0].Key)
		assert.Equal(t, 150, result.Buckets[0].DocCount)
	})

	t.Run("RangeBuckets", func(t *testing.T) {
		from1 := float64(0)
		to1 := float64(100)
		from2 := float64(100)
		to2 := float64(500)

		result := AggregationResult{
			Buckets: []AggregationBucket{
				{
					Key:      "cheap",
					DocCount: 50,
					From:     &from1,
					To:       &to1,
				},
				{
					Key:      "medium",
					DocCount: 75,
					From:     &from2,
					To:       &to2,
				},
			},
		}

		assert.Len(t, result.Buckets, 2)
		assert.Equal(t, "cheap", result.Buckets[0].Key)
		assert.NotNil(t, result.Buckets[0].From)
		assert.Equal(t, float64(0), *result.Buckets[0].From)
		assert.NotNil(t, result.Buckets[0].To)
		assert.Equal(t, float64(100), *result.Buckets[0].To)
	})

	t.Run("DateRangeBuckets", func(t *testing.T) {
		keyAsString := "2024-Q1"
		fromAsString := "2024-01-01"
		toAsString := "2024-03-31"

		result := AggregationResult{
			Buckets: []AggregationBucket{
				{
					Key:          "q1",
					DocCount:     200,
					KeyAsString:  &keyAsString,
					FromAsString: &fromAsString,
					ToAsString:   &toAsString,
				},
			},
		}

		assert.Len(t, result.Buckets, 1)
		assert.NotNil(t, result.Buckets[0].KeyAsString)
		assert.Equal(t, "2024-Q1", *result.Buckets[0].KeyAsString)
		assert.NotNil(t, result.Buckets[0].FromAsString)
		assert.Equal(t, "2024-01-01", *result.Buckets[0].FromAsString)
	})
}

// TestAggregationResult_NestedResults tests sub-aggregation results
func TestAggregationResult_NestedResults(t *testing.T) {
	t.Run("BucketWithMetric", func(t *testing.T) {
		avgValue := float64(299.99)
		maxValue := float64(999.99)

		result := AggregationResult{
			Buckets: []AggregationBucket{
				{
					Key:      "electronics",
					DocCount: 150,
					SubAggregations: map[string]AggregationResult{
						"avg_price": {Value: &avgValue},
						"max_price": {Value: &maxValue},
					},
				},
			},
		}

		assert.Len(t, result.Buckets, 1)
		assert.NotNil(t, result.Buckets[0].SubAggregations)
		assert.Len(t, result.Buckets[0].SubAggregations, 2)
		assert.Contains(t, result.Buckets[0].SubAggregations, "avg_price")
		assert.Contains(t, result.Buckets[0].SubAggregations, "max_price")

		avgResult := result.Buckets[0].SubAggregations["avg_price"]
		assert.NotNil(t, avgResult.Value)
		assert.Equal(t, 299.99, *avgResult.Value)
	})

	t.Run("MultiLevelNesting", func(t *testing.T) {
		avgPrice := float64(150.0)

		result := AggregationResult{
			Buckets: []AggregationBucket{
				{
					Key:      "electronics",
					DocCount: 500,
					SubAggregations: map[string]AggregationResult{
						"by_brand": {
							Buckets: []AggregationBucket{
								{
									Key:      "Apple",
									DocCount: 200,
									SubAggregations: map[string]AggregationResult{
										"avg_price": {Value: &avgPrice},
									},
								},
							},
						},
					},
				},
			},
		}

		assert.Len(t, result.Buckets, 1)
		assert.Contains(t, result.Buckets[0].SubAggregations, "by_brand")

		brandAgg := result.Buckets[0].SubAggregations["by_brand"]
		assert.Len(t, brandAgg.Buckets, 1)
		assert.Equal(t, "Apple", brandAgg.Buckets[0].Key)
		assert.Contains(t, brandAgg.Buckets[0].SubAggregations, "avg_price")

		avgPriceResult := brandAgg.Buckets[0].SubAggregations["avg_price"]
		assert.NotNil(t, avgPriceResult.Value)
		assert.Equal(t, 150.0, *avgPriceResult.Value)
	})
}

// TestQueryRequest_WithAggregations tests query requests with aggregations
func TestQueryRequest_WithAggregations(t *testing.T) {
	t.Run("FullTextWithAggregations", func(t *testing.T) {
		size := 10
		ftQuery := json.RawMessage(`{"query": "laptop"}`)

		req := QueryRequest{
			FullTextSearch: ftQuery,
			Aggregations: map[string]AggregationRequest{
				"price_stats": {
					Type:  "stats",
					Field: "price",
				},
				"categories": {
					Type:  "terms",
					Field: "category",
					Size:  &size,
				},
			},
		}

		assert.NotEmpty(t, req.FullTextSearch)
		assert.NotNil(t, req.Aggregations)
		assert.Len(t, req.Aggregations, 2)
		assert.Contains(t, req.Aggregations, "price_stats")
		assert.Contains(t, req.Aggregations, "categories")
	})

	t.Run("FilterWithAggregations", func(t *testing.T) {
		filterQuery := json.RawMessage(`{"query": "status:published"}`)

		req := QueryRequest{
			FilterQuery: filterQuery,
			Aggregations: map[string]AggregationRequest{
				"by_author": {
					Type:  "terms",
					Field: "author",
				},
			},
		}

		assert.NotEmpty(t, req.FilterQuery)
		assert.NotNil(t, req.Aggregations)
		assert.Contains(t, req.Aggregations, "by_author")
	})

	t.Run("EmptyResultSetAggregations", func(t *testing.T) {
		// Aggregations should work even with no matching documents
		ftQuery := json.RawMessage(`{"query": "nonexistent"}`)

		req := QueryRequest{
			FullTextSearch: ftQuery,
			Aggregations: map[string]AggregationRequest{
				"count": {
					Type:  "count",
					Field: "_id",
				},
			},
		}

		assert.NotEmpty(t, req.FullTextSearch)
		assert.NotNil(t, req.Aggregations)
	})
}

// TestCalendarInterval_Values tests calendar interval enum values
func TestCalendarInterval_Values(t *testing.T) {
	validIntervals := []CalendarInterval{
		"minute",
		"hour",
		"day",
		"week",
		"month",
		"quarter",
		"year",
	}

	for _, interval := range validIntervals {
		t.Run(string(interval), func(t *testing.T) {
			assert.NotEmpty(t, interval)
		})
	}
}

// TestDistanceUnit_Values tests distance unit enum values
func TestDistanceUnit_Values(t *testing.T) {
	validUnits := []DistanceUnit{
		"m",  // meters
		"km", // kilometers
		"mi", // miles
		"ft", // feet
		"yd", // yards
	}

	for _, unit := range validUnits {
		t.Run(string(unit), func(t *testing.T) {
			assert.NotEmpty(t, unit)
		})
	}
}

// TestSignificanceAlgorithm_Values tests significance algorithm enum values
func TestSignificanceAlgorithm_Values(t *testing.T) {
	validAlgorithms := []SignificanceAlgorithm{
		"jlh",
		"mutual_information",
		"chi_squared",
		"percentage",
	}

	for _, algo := range validAlgorithms {
		t.Run(string(algo), func(t *testing.T) {
			assert.NotEmpty(t, algo)
		})
	}
}

// TestAggregationRequest_GeoAggregations tests geo-specific aggregations
func TestAggregationRequest_GeoAggregations(t *testing.T) {
	t.Run("GeohashGrid", func(t *testing.T) {
		precision := 6
		origin := "37.7749,-122.4194"

		agg := AggregationRequest{
			Type:      "geohash_grid",
			Field:     "location",
			Precision: &precision,
			Origin:    &origin,
		}

		assert.Equal(t, AggregationType("geohash_grid"), agg.Type)
		assert.NotNil(t, agg.Precision)
		assert.Equal(t, 6, *agg.Precision)
		assert.NotNil(t, agg.Origin)
		assert.Equal(t, "37.7749,-122.4194", *agg.Origin)
	})

	t.Run("GeoDistance", func(t *testing.T) {
		unit := DistanceUnit("km")
		from1 := float64(0)
		to1 := float64(10)
		from2 := float64(10)
		to2 := float64(50)

		agg := AggregationRequest{
			Type:  "geo_distance",
			Field: "location",
			Unit:  &unit,
			DistanceRanges: []DistanceRange{
				{Name: "nearby", From: &from1, To: &to1},
				{Name: "far", From: &from2, To: &to2},
			},
		}

		assert.Equal(t, AggregationType("geo_distance"), agg.Type)
		assert.NotNil(t, agg.Unit)
		assert.Equal(t, DistanceUnit("km"), *agg.Unit)
		assert.Len(t, agg.DistanceRanges, 2)
		assert.Equal(t, "nearby", agg.DistanceRanges[0].Name)
	})
}

// TestAggregationRequest_SignificantTerms tests significant terms aggregation
func TestAggregationRequest_SignificantTerms(t *testing.T) {
	size := 20
	minDocCount := 3
	algo := SignificanceAlgorithm("jlh")
	backgroundFilter := json.RawMessage(`{"query": "type:article"}`)

	agg := AggregationRequest{
		Type:             "significant_terms",
		Field:            "tags",
		Size:             &size,
		MinDocCount:      &minDocCount,
		Algorithm:        &algo,
		BackgroundFilter: backgroundFilter,
	}

	assert.Equal(t, AggregationType("significant_terms"), agg.Type)
	assert.NotNil(t, agg.Algorithm)
	assert.Equal(t, SignificanceAlgorithm("jlh"), *agg.Algorithm)
	assert.NotEmpty(t, agg.BackgroundFilter)
}

// TestAggregationBucket_SignificantTerms tests significant terms bucket results
func TestAggregationBucket_SignificantTerms(t *testing.T) {
	score := float64(0.85)
	bgCount := 1000

	bucket := AggregationBucket{
		Key:      "machine-learning",
		DocCount: 50,
		Score:    &score,
		BgCount:  &bgCount,
	}

	assert.Equal(t, "machine-learning", bucket.Key)
	assert.Equal(t, 50, bucket.DocCount)
	assert.NotNil(t, bucket.Score)
	assert.Equal(t, 0.85, *bucket.Score)
	assert.NotNil(t, bucket.BgCount)
	assert.Equal(t, 1000, *bucket.BgCount)
}

// TestQueryResult_WithAggregations tests query result structure with aggregations
func TestQueryResult_WithAggregations(t *testing.T) {
	avgValue := float64(299.99)
	count := 150

	result := QueryResult{
		Aggregations: map[string]AggregationResult{
			"avg_price": {Value: &avgValue},
			"stats": {
				Count: &count,
				Min:   &avgValue,
				Max:   &avgValue,
			},
			"categories": {
				Buckets: []AggregationBucket{
					{Key: "electronics", DocCount: 100},
					{Key: "books", DocCount: 50},
				},
			},
		},
	}

	assert.NotNil(t, result.Aggregations)
	assert.Len(t, result.Aggregations, 3)
	assert.Contains(t, result.Aggregations, "avg_price")
	assert.Contains(t, result.Aggregations, "stats")
	assert.Contains(t, result.Aggregations, "categories")

	// Check avg_price
	avgPrice := result.Aggregations["avg_price"]
	assert.NotNil(t, avgPrice.Value)
	assert.Equal(t, 299.99, *avgPrice.Value)

	// Check stats
	stats := result.Aggregations["stats"]
	assert.NotNil(t, stats.Count)
	assert.Equal(t, 150, *stats.Count)

	// Check categories buckets
	categories := result.Aggregations["categories"]
	assert.Len(t, categories.Buckets, 2)
	assert.Equal(t, "electronics", categories.Buckets[0].Key)
	assert.Equal(t, 100, categories.Buckets[0].DocCount)
}

// TestAggregationRequest_Validation tests edge cases and validation
func TestAggregationRequest_Validation(t *testing.T) {
	t.Run("MinDocCountZero", func(t *testing.T) {
		minDocCount := 0
		agg := AggregationRequest{
			Type:        "terms",
			Field:       "category",
			MinDocCount: &minDocCount,
		}

		assert.NotNil(t, agg.MinDocCount)
		assert.Equal(t, 0, *agg.MinDocCount)
	})

	t.Run("EmptyRanges", func(t *testing.T) {
		agg := AggregationRequest{
			Type:   "range",
			Field:  "price",
			Ranges: []AggregationRange{},
		}

		assert.NotNil(t, agg.Ranges)
		assert.Empty(t, agg.Ranges)
	})

	t.Run("NilSubAggregations", func(t *testing.T) {
		agg := AggregationRequest{
			Type:            "terms",
			Field:           "category",
			SubAggregations: nil,
		}

		assert.Nil(t, agg.SubAggregations)
	})

	t.Run("EmptySubAggregations", func(t *testing.T) {
		agg := AggregationRequest{
			Type:            "terms",
			Field:           "category",
			SubAggregations: map[string]AggregationRequest{},
		}

		assert.NotNil(t, agg.SubAggregations)
		assert.Empty(t, agg.SubAggregations)
	})
}

// TestAggregationResult_EdgeCases tests edge cases for aggregation results
func TestAggregationResult_EdgeCases(t *testing.T) {
	t.Run("EmptyBuckets", func(t *testing.T) {
		result := AggregationResult{
			Buckets: []AggregationBucket{},
		}

		assert.NotNil(t, result.Buckets)
		assert.Empty(t, result.Buckets)
	})

	t.Run("ZeroDocCount", func(t *testing.T) {
		bucket := AggregationBucket{
			Key:      "empty",
			DocCount: 0,
		}

		assert.Equal(t, 0, bucket.DocCount)
	})

	t.Run("NilOptionalFields", func(t *testing.T) {
		result := AggregationResult{
			Value:        nil,
			Count:        nil,
			Min:          nil,
			Max:          nil,
			Sum:          nil,
			SumOfSquares: nil,
			Avg:          nil,
			StdDeviation: nil,
			Variance:     nil,
			Buckets:      nil,
		}

		assert.Nil(t, result.Value)
		assert.Nil(t, result.Count)
		assert.Nil(t, result.Buckets)
	})
}
