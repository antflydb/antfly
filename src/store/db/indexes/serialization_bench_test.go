package indexes

import (
	"encoding/binary"
	"encoding/base64"
	stdjson "encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	json "github.com/antflydb/antfly/pkg/libaf/json"

	"github.com/antflydb/antfly/lib/vector"
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/query"
)

// ---------- helpers ----------

// legacyFloat32Slice is the old JSON representation: a plain []float32 without
// custom MarshalJSON, so encoding/json writes a JSON array of numbers.
type legacyFloat32Slice []float32

func makeEmbedding(dims int) vector.T {
	v := make(vector.T, dims)
	for i := range v {
		v[i] = float32(i) * 0.00123
	}
	return v
}

func makeLegacyEmbedding(dims int) legacyFloat32Slice {
	v := make(legacyFloat32Slice, dims)
	for i := range v {
		v[i] = float32(i) * 0.00123
	}
	return v
}

// legacySearchRequest mirrors RemoteIndexSearchRequest but uses plain
// []float32 for vector searches so JSON encodes as number arrays.
type legacySearchRequest struct {
	Columns          []string                          `json:"columns,omitempty"`
	Limit            int                               `json:"limit,omitempty"`
	BlevePagingOpts  FullTextPagingOptions             `json:"bleve_paging_options"`
	VectorPagingOpts VectorPagingOptions               `json:"vector_paging_options"`
	Star             bool                              `json:"star,omitempty"`
	FilterQuery      json.RawMessage                   `json:"filter_query,omitzero,omitempty"`
	VectorSearches   map[string]legacyFloat32Slice     `json:"vector_searches,omitzero,omitempty"`
	MergeConfig      *MergeConfig                      `json:"merge_config,omitempty"`
	BleveSearch      *bleve.SearchRequest              `json:"bleve_search,omitempty"`
}

// ---------- Change #2: Vector encoding benchmarks ----------

func BenchmarkVectorEncoding(b *testing.B) {
	for _, dims := range []int{384, 768, 1536} {
		b.Run(fmt.Sprintf("dims=%d", dims), func(b *testing.B) {
			b.Run("marshal/base64", func(b *testing.B) {
				v := makeEmbedding(dims)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					_, _ = json.Marshal(v)
				}
			})

			b.Run("marshal/json_array", func(b *testing.B) {
				v := makeLegacyEmbedding(dims)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					_, _ = json.Marshal(v)
				}
			})

			b.Run("unmarshal/base64", func(b *testing.B) {
				v := makeEmbedding(dims)
				data, _ := json.Marshal(v)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					var out vector.T
					_ = json.Unmarshal(data, &out)
				}
			})

			b.Run("unmarshal/json_array", func(b *testing.B) {
				v := makeLegacyEmbedding(dims)
				data, _ := json.Marshal(v)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					var out legacyFloat32Slice
					_ = json.Unmarshal(data, &out)
				}
			})
		})
	}
}

func BenchmarkVectorPayloadSize(b *testing.B) {
	for _, dims := range []int{384, 768, 1536} {
		b.Run(fmt.Sprintf("dims=%d", dims), func(b *testing.B) {
			newData, _ := json.Marshal(makeEmbedding(dims))
			oldData, _ := json.Marshal(makeLegacyEmbedding(dims))
			b.ReportMetric(float64(len(newData)), "bytes/base64")
			b.ReportMetric(float64(len(oldData)), "bytes/json_array")
			b.ReportMetric(float64(len(oldData))/float64(len(newData)), "ratio")
		})
	}
}

// ---------- Full request roundtrip benchmarks ----------

func BenchmarkSearchRequestRoundtrip(b *testing.B) {
	// Simulate a typical hybrid search: 2 embedding indexes + full-text + filter
	filterJSON := []byte(`{"match":{"field":"category","match":"electronics"}}`)
	rrf := MergeStrategyRrf
	queryString := query.NewQueryStringQuery("bluetooth speakers")

	for _, dims := range []int{384, 1536} {
		b.Run(fmt.Sprintf("dims=%d", dims), func(b *testing.B) {
			b.Run("marshal/new_base64", func(b *testing.B) {
				req := &RemoteIndexSearchRequest{
					Star:        true,
					Limit:       10,
					FilterQuery: filterJSON,
					VectorSearches: map[string]vector.T{
						"embedding_index_a": makeEmbedding(dims),
						"embedding_index_b": makeEmbedding(dims),
					},
					MergeConfig:      &MergeConfig{Strategy: &rrf},
					BleveSearchRequest: bleve.NewSearchRequest(queryString),
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					_, _ = json.Marshal(req)
				}
			})

			b.Run("marshal/old_json_array", func(b *testing.B) {
				req := &legacySearchRequest{
					Star:        true,
					Limit:       10,
					FilterQuery: filterJSON,
					VectorSearches: map[string]legacyFloat32Slice{
						"embedding_index_a": makeLegacyEmbedding(dims),
						"embedding_index_b": makeLegacyEmbedding(dims),
					},
					MergeConfig:  &MergeConfig{Strategy: &rrf},
					BleveSearch:  bleve.NewSearchRequest(queryString),
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					_, _ = json.Marshal(req)
				}
			})

			b.Run("roundtrip/new_base64", func(b *testing.B) {
				req := &RemoteIndexSearchRequest{
					Star:        true,
					Limit:       10,
					FilterQuery: filterJSON,
					VectorSearches: map[string]vector.T{
						"embedding_index_a": makeEmbedding(dims),
						"embedding_index_b": makeEmbedding(dims),
					},
					MergeConfig:      &MergeConfig{Strategy: &rrf},
					BleveSearchRequest: bleve.NewSearchRequest(queryString),
				}
				data, _ := json.Marshal(req)
				b.SetBytes(int64(len(data)))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					data, _ = json.Marshal(req)
					var out RemoteIndexSearchRequest
					_ = json.Unmarshal(data, &out)
				}
			})

			b.Run("roundtrip/old_json_array", func(b *testing.B) {
				req := &legacySearchRequest{
					Star:        true,
					Limit:       10,
					FilterQuery: filterJSON,
					VectorSearches: map[string]legacyFloat32Slice{
						"embedding_index_a": makeLegacyEmbedding(dims),
						"embedding_index_b": makeLegacyEmbedding(dims),
					},
					MergeConfig:  &MergeConfig{Strategy: &rrf},
					BleveSearch:  bleve.NewSearchRequest(queryString),
				}
				data, _ := json.Marshal(req)
				b.SetBytes(int64(len(data)))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					data, _ = json.Marshal(req)
					var out legacySearchRequest
					_ = json.Unmarshal(data, &out)
				}
			})
		})
	}
}

// ---------- Change #3: Filter query passthrough benchmarks ----------

func BenchmarkFilterQueryPassthrough(b *testing.B) {
	// Build a complex filter query using Bleve's query API, then serialize it
	// to get valid Bleve JSON format.
	matchCategory := query.NewMatchQuery("electronics")
	matchCategory.SetField("category")
	min := 10.0
	max := 1000.0
	numRange := query.NewNumericRangeQuery(&min, &max)
	numRange.SetField("price")
	matchSony := query.NewMatchQuery("sony")
	matchSony.SetField("brand")
	matchSamsung := query.NewMatchQuery("samsung")
	matchSamsung.SetField("brand")
	brandDisjunction := query.NewDisjunctionQuery([]query.Query{matchSony, matchSamsung})
	conjunction := query.NewConjunctionQuery([]query.Query{matchCategory, numRange, brandDisjunction})
	complexFilter, _ := json.Marshal(conjunction)

	b.Run("passthrough_raw_bytes", func(b *testing.B) {
		// New approach: keep raw bytes, skip parse+re-marshal
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			// Just pass through the raw bytes (what our optimization does)
			fqBytes := json.RawMessage(complexFilter)
			_ = fqBytes
		}
	})

	b.Run("parse_then_remarshal", func(b *testing.B) {
		// Old approach: parse JSON → query.Query → marshal back to JSON
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			q, err := query.ParseQuery(complexFilter)
			if err != nil {
				b.Fatal(err)
			}
			fqBytes, err := json.Marshal(q)
			if err != nil {
				b.Fatal(err)
			}
			_ = fqBytes
		}
	})

	b.Run("parse_only", func(b *testing.B) {
		// Just the parse cost (unavoidable for the Bleve path)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			q, err := query.ParseQuery(complexFilter)
			if err != nil {
				b.Fatal(err)
			}
			_ = q
		}
	})

	b.Run("marshal_only", func(b *testing.B) {
		// Just the marshal cost (what we avoid)
		q, _ := query.ParseQuery(complexFilter)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			fqBytes, err := json.Marshal(q)
			if err != nil {
				b.Fatal(err)
			}
			_ = fqBytes
		}
	})
}

// ---------- Full response roundtrip benchmarks ----------

func BenchmarkSearchResponseRoundtrip(b *testing.B) {
	// Simulate a typical response with 10 fusion hits
	makeResponse := func() *RemoteIndexSearchResult {
		hits := make([]*FusionHit, 10)
		for i := range hits {
			hits[i] = &FusionHit{
				ID:    fmt.Sprintf("doc_%d", i),
				Score: float64(10 - i) * 0.1,
				Fields: map[string]any{
					"title":       fmt.Sprintf("Document %d about bluetooth speakers", i),
					"description": "A longer description field that contains more text to simulate real payloads with actual content.",
					"price":       float64(i) * 9.99,
					"category":    "electronics",
					"tags":        []any{"audio", "bluetooth", "wireless"},
				},
				IndexScores: map[string]float64{
					"full_text":         float64(10-i) * 0.05,
					"embedding_index_a": float64(i) * 0.03,
				},
			}
		}
		return &RemoteIndexSearchResult{
			Took: 15 * time.Millisecond,
			Status: &RemoteIndexSearchStatus{
				Total:      3,
				Successful: 3,
			},
			FusionResult: &FusionResult{
				Hits:     hits,
				Total:    42,
				MaxScore: 1.0,
			},
		}
	}

	b.Run("marshal", func(b *testing.B) {
		resp := makeResponse()
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			_, _ = json.Marshal(resp)
		}
	})

	b.Run("roundtrip", func(b *testing.B) {
		resp := makeResponse()
		data, _ := json.Marshal(resp)
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			data, _ = json.Marshal(resp)
			var out RemoteIndexSearchResult
			_ = json.Unmarshal(data, &out)
		}
	})
}

// ---------- stdlib vs goccy/go-json comparison ----------

func BenchmarkJSONLibrary(b *testing.B) {
	// Compare stdlib encoding/json vs goccy/go-json on a realistic payload
	rrf := MergeStrategyRrf
	queryString := query.NewQueryStringQuery("bluetooth speakers")
	filterJSON := []byte(`{"match":{"field":"category","match":"electronics"}}`)

	req := &RemoteIndexSearchRequest{
		Star:        true,
		Limit:       10,
		FilterQuery: filterJSON,
		VectorSearches: map[string]vector.T{
			"embedding_index": makeEmbedding(384),
		},
		MergeConfig:        &MergeConfig{Strategy: &rrf},
		BleveSearchRequest: bleve.NewSearchRequest(queryString),
	}

	b.Run("marshal/goccy", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = json.Marshal(req)
		}
	})

	b.Run("marshal/stdlib", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = stdjson.Marshal(req)
		}
	})

	data, _ := json.Marshal(req)

	b.Run("unmarshal/goccy", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var out RemoteIndexSearchRequest
			_ = json.Unmarshal(data, &out)
		}
	})

	b.Run("unmarshal/stdlib", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var out RemoteIndexSearchRequest
			_ = stdjson.Unmarshal(data, &out)
		}
	})
}

// ---------- End-to-end wire size comparison ----------

func TestWireSizeComparison(t *testing.T) {
	rrf := MergeStrategyRrf
	queryString := query.NewQueryStringQuery("bluetooth speakers")
	filterJSON := []byte(`{"match":{"field":"category","match":"electronics"}}`)

	for _, dims := range []int{384, 768, 1536} {
		// New format (base64 vectors)
		newReq := &RemoteIndexSearchRequest{
			Star:        true,
			Limit:       10,
			FilterQuery: filterJSON,
			VectorSearches: map[string]vector.T{
				"embedding_index_a": makeEmbedding(dims),
				"embedding_index_b": makeEmbedding(dims),
			},
			MergeConfig:        &MergeConfig{Strategy: &rrf},
			BleveSearchRequest: bleve.NewSearchRequest(queryString),
		}
		newData, _ := json.Marshal(newReq)

		// Old format (JSON arrays)
		oldReq := &legacySearchRequest{
			Star:        true,
			Limit:       10,
			FilterQuery: filterJSON,
			VectorSearches: map[string]legacyFloat32Slice{
				"embedding_index_a": makeLegacyEmbedding(dims),
				"embedding_index_b": makeLegacyEmbedding(dims),
			},
			MergeConfig: &MergeConfig{Strategy: &rrf},
			BleveSearch: bleve.NewSearchRequest(queryString),
		}
		oldData, _ := json.Marshal(oldReq)

		// Raw binary size (theoretical minimum for vectors only)
		rawVecBytes := dims * 4 * 2 // 2 indexes, float32

		t.Logf("dims=%-4d | old=%-7d bytes | new=%-7d bytes | savings=%-5d bytes (%.0f%%) | raw_vectors=%-5d bytes",
			dims, len(oldData), len(newData),
			len(oldData)-len(newData),
			float64(len(oldData)-len(newData))/float64(len(oldData))*100,
			rawVecBytes,
		)
	}
}

// Ensure base64 encoding produces valid output that can be decoded on the other side
func TestBase64VectorInterop(t *testing.T) {
	dims := 384
	v := makeEmbedding(dims)

	// Marshal with our custom MarshalJSON
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}

	// Verify it's a base64 string (starts with ")
	if data[0] != '"' {
		t.Fatalf("expected base64 string, got: %c...", data[0])
	}

	// Manually decode to verify format
	str := string(data[1 : len(data)-1])
	raw, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != dims*4 {
		t.Fatalf("expected %d bytes, got %d", dims*4, len(raw))
	}

	// Verify individual floats
	for i := range dims {
		bits := binary.LittleEndian.Uint32(raw[i*4:])
		got := math.Float32frombits(bits)
		want := float32(i) * 0.00123
		if got != want {
			t.Fatalf("float[%d]: got %v, want %v", i, got, want)
		}
	}
}
