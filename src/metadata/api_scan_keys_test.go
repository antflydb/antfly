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
	"strings"
	"testing"

	"github.com/antflydb/antfly/src/store/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLookupKeyParamsParsing tests that the LookupKeyParams struct works correctly
func TestLookupKeyParamsParsing(t *testing.T) {
	t.Run("EmptyFields", func(t *testing.T) {
		params := LookupKeyParams{}
		assert.Empty(t, params.Fields)
	})

	t.Run("SingleField", func(t *testing.T) {
		params := LookupKeyParams{Fields: "title"}
		assert.Equal(t, "title", params.Fields)
	})

	t.Run("MultipleFields", func(t *testing.T) {
		params := LookupKeyParams{Fields: "title,author,metadata.tags"}
		fields := strings.Split(params.Fields, ",")
		assert.Len(t, fields, 3)
		assert.Equal(t, "title", fields[0])
		assert.Equal(t, "author", fields[1])
		assert.Equal(t, "metadata.tags", fields[2])
	})

	t.Run("FieldsWithSpaces", func(t *testing.T) {
		params := LookupKeyParams{Fields: "title, author, metadata.tags"}
		fields := strings.Split(params.Fields, ",")
		for i := range fields {
			fields[i] = strings.TrimSpace(fields[i])
		}
		assert.Equal(t, "title", fields[0])
		assert.Equal(t, "author", fields[1])
		assert.Equal(t, "metadata.tags", fields[2])
	})

	t.Run("SpecialFields", func(t *testing.T) {
		params := LookupKeyParams{Fields: "_embeddings,_summaries,-_chunks.*._embedding"}
		fields := strings.Split(params.Fields, ",")
		assert.Len(t, fields, 3)
	})
}

// TestScanKeysRequestParsing tests that the ScanKeysRequest struct correctly parses JSON
func TestScanKeysRequestParsing(t *testing.T) {
	t.Run("EmptyRequest", func(t *testing.T) {
		req := ScanKeysRequest{}
		assert.Empty(t, req.From)
		assert.Empty(t, req.To)
		assert.False(t, req.InclusiveFrom)
		assert.False(t, req.ExclusiveTo)
		assert.Empty(t, req.Fields)
	})

	t.Run("WithRange", func(t *testing.T) {
		req := ScanKeysRequest{
			From: "user:100",
			To:   "user:200",
		}
		assert.Equal(t, "user:100", req.From)
		assert.Equal(t, "user:200", req.To)
	})

	t.Run("WithPrefixRange", func(t *testing.T) {
		// Test that from/to can be prefixes, not just full keys
		req := ScanKeysRequest{
			From: "user:",
			To:   "user:\xff",
		}
		assert.Equal(t, "user:", req.From)
		assert.Equal(t, "user:\xff", req.To)
	})

	t.Run("WithBoundaryOptions", func(t *testing.T) {
		req := ScanKeysRequest{
			From:          "a",
			To:            "z",
			InclusiveFrom: true,
			ExclusiveTo:   true,
		}
		assert.True(t, req.InclusiveFrom)
		assert.True(t, req.ExclusiveTo)
	})

	t.Run("WithFields", func(t *testing.T) {
		req := ScanKeysRequest{
			Fields: []string{"title", "author", "metadata.tags"},
		}
		assert.Len(t, req.Fields, 3)
		assert.Equal(t, "title", req.Fields[0])
		assert.Equal(t, "author", req.Fields[1])
		assert.Equal(t, "metadata.tags", req.Fields[2])
	})

	t.Run("WithSpecialFields", func(t *testing.T) {
		req := ScanKeysRequest{
			Fields: []string{"_embeddings", "_summaries", "_chunks"},
		}
		assert.Len(t, req.Fields, 3)
	})
}

// TestScanKeysFieldProjection tests that field projection works correctly with scan results
func TestScanKeysFieldProjection(t *testing.T) {
	t.Run("ProjectSimpleFields", func(t *testing.T) {
		doc := map[string]any{
			"title":  "Test Document",
			"author": "John Doe",
			"body":   "This is a long body that we don't want to include",
			"views":  100,
		}

		fields := []string{"title", "author"}
		result := db.ProjectFields(doc, fields)

		assert.Len(t, result, 2)
		assert.Equal(t, "Test Document", result["title"])
		assert.Equal(t, "John Doe", result["author"])
		assert.NotContains(t, result, "body")
		assert.NotContains(t, result, "views")
	})

	t.Run("ProjectNestedFields", func(t *testing.T) {
		doc := map[string]any{
			"title": "Test",
			"metadata": map[string]any{
				"tags":     []string{"a", "b", "c"},
				"category": "tech",
				"author": map[string]any{
					"name":  "Jane",
					"email": "jane@example.com",
				},
			},
		}

		fields := []string{"title", "metadata.tags", "metadata.author.name"}
		result := db.ProjectFields(doc, fields)

		require.NotNil(t, result["title"])
		assert.Equal(t, "Test", result["title"])

		require.NotNil(t, result["metadata"])
		metadata := result["metadata"].(map[string]any)
		assert.Contains(t, metadata, "tags")
		assert.Contains(t, metadata, "author")
		assert.NotContains(t, metadata, "category")
	})

	t.Run("ProjectWithExclusion", func(t *testing.T) {
		doc := map[string]any{
			"title": "Test",
			"_chunks": map[string]any{
				"chunk1": map[string]any{
					"_content":   "text content",
					"_embedding": []float64{0.1, 0.2, 0.3},
				},
			},
		}

		// Include chunks but exclude embeddings
		fields := []string{"title", "_chunks.*", "-_chunks.*._embedding"}
		result := db.ProjectFields(doc, fields)

		assert.Equal(t, "Test", result["title"])
		require.NotNil(t, result["_chunks"])
	})

	t.Run("ProjectEmptyFields", func(t *testing.T) {
		doc := map[string]any{
			"title": "Test",
			"body":  "Content",
		}

		// Empty fields list should return empty result (key-only mode)
		fields := []string{}
		result := db.ProjectFields(doc, fields)

		// ProjectFields with empty fields returns empty map
		assert.Empty(t, result)
	})

	t.Run("ProjectNonExistentFields", func(t *testing.T) {
		doc := map[string]any{
			"title": "Test",
		}

		fields := []string{"nonexistent", "also_missing"}
		result := db.ProjectFields(doc, fields)

		// Non-existent fields should simply not appear in result
		assert.NotContains(t, result, "nonexistent")
		assert.NotContains(t, result, "also_missing")
	})
}

// TestScanKeysByteBoundaryComparison tests byte-level key range comparisons
func TestScanKeysByteBoundaryComparison(t *testing.T) {
	t.Run("PrefixRangeComparison", func(t *testing.T) {
		// Test that prefix-based ranges work correctly with byte comparison
		from := []byte("user:")
		to := []byte("user:\xff")

		// Keys that should be included
		key1 := []byte("user:100")
		key2 := []byte("user:999")
		key3 := []byte("user:abc")

		// Keys that should be excluded
		key4 := []byte("admin:100")
		key5 := []byte("users:100") // note: 's' > ':' in ASCII

		// Verify byte ordering
		assert.True(t, string(from) <= string(key1) && string(key1) < string(to))
		assert.True(t, string(from) <= string(key2) && string(key2) < string(to))
		assert.True(t, string(from) <= string(key3) && string(key3) < string(to))
		assert.Less(t, string(key4), string(from))  // 'a' < 'u'
		assert.Greater(t, string(key5), string(to)) // "users:" > "user:\xff" because 's' > '\xff' is false, but "users" > "user:" is true by length
	})

	t.Run("EmptyFromMeansStart", func(t *testing.T) {
		req := ScanKeysRequest{
			From: "",
			To:   "zzz",
		}
		assert.Empty(t, req.From)
		// Empty 'from' should mean start from beginning of table/shard
	})

	t.Run("EmptyToMeansEnd", func(t *testing.T) {
		req := ScanKeysRequest{
			From: "aaa",
			To:   "",
		}
		assert.Empty(t, req.To)
		// Empty 'to' should mean scan to end of table/shard
	})
}
