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

package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProjectFields(t *testing.T) {
	// Sample document with chunks
	doc := map[string]any{
		"title":  "Test Document",
		"author": "John Doe",
		"_chunks": map[string]any{
			"my_index": []any{
				map[string]any{
					"_id":         "0",
					"_start_char": 0,
					"_end_char":   100,
					"_content":    "First chunk content...",
				},
				map[string]any{
					"_id":         "1",
					"_start_char": 100,
					"_end_char":   200,
					"_content":    "Second chunk content...",
				},
			},
			"other_index": []any{
				map[string]any{
					"_id":         "0",
					"_start_char": 0,
					"_end_char":   150,
					"_content":    "Other index chunk...",
				},
			},
		},
		"_summaries": map[string]any{
			"my_index": "Summary text",
		},
	}

	t.Run("include specific top-level fields", func(t *testing.T) {
		result := ProjectFields(doc, []string{"title", "author"})
		assert.Len(t, result, 2)
		assert.Equal(t, "Test Document", result["title"])
		assert.Equal(t, "John Doe", result["author"])
		assert.Nil(t, result["_chunks"])
	})

	t.Run("include nested field with wildcard", func(t *testing.T) {
		result := ProjectFields(doc, []string{"_chunks.*._id", "_chunks.*._content"})
		assert.NotNil(t, result["_chunks"])
		chunks := result["_chunks"].(map[string]any)

		// Should have both indexes
		assert.NotNil(t, chunks["my_index"])
		assert.NotNil(t, chunks["other_index"])

		// Check my_index chunks
		myIndexChunks := chunks["my_index"].([]any)
		assert.Len(t, myIndexChunks, 2)
		chunk0 := myIndexChunks[0].(map[string]any)
		assert.Equal(t, "0", chunk0["_id"])
		assert.Equal(t, "First chunk content...", chunk0["_content"])
		assert.Nil(t, chunk0["_start_char"]) // Not included
		assert.Nil(t, chunk0["_end_char"])   // Not included
	})

	t.Run("include all chunks with wildcard", func(t *testing.T) {
		result := ProjectFields(doc, []string{"_chunks.*"})
		assert.NotNil(t, result["_chunks"])
		chunks := result["_chunks"].(map[string]any)

		// Should have both indexes with all fields
		myIndexChunks := chunks["my_index"].([]any)
		assert.Len(t, myIndexChunks, 2)
		chunk0 := myIndexChunks[0].(map[string]any)
		assert.Equal(t, "0", chunk0["_id"])
		assert.Equal(t, "First chunk content...", chunk0["_content"])
		assert.Equal(t, 0, chunk0["_start_char"]) // Included
		assert.Equal(t, 100, chunk0["_end_char"]) // Included
	})

	t.Run("exclude _content from chunks", func(t *testing.T) {
		result := ProjectFields(doc, []string{"-_chunks.*._content"})

		// Should have full document except chunk _content fields
		assert.Equal(t, "Test Document", result["title"])
		assert.NotNil(t, result["_chunks"])

		chunks := result["_chunks"].(map[string]any)
		myIndexChunks := chunks["my_index"].([]any)
		chunk0 := myIndexChunks[0].(map[string]any)

		// Should have metadata but not content
		assert.Equal(t, "0", chunk0["_id"])
		assert.Equal(t, 0, chunk0["_start_char"])
		assert.Equal(t, 100, chunk0["_end_char"])
		assert.Nil(t, chunk0["_content"]) // Excluded
	})

	t.Run("include specific index chunks only", func(t *testing.T) {
		result := ProjectFields(doc, []string{"_chunks.my_index"})
		assert.NotNil(t, result["_chunks"])
		chunks := result["_chunks"].(map[string]any)

		// Should only have my_index
		assert.NotNil(t, chunks["my_index"])
		assert.Nil(t, chunks["other_index"]) // Not included

		myIndexChunks := chunks["my_index"].([]any)
		assert.Len(t, myIndexChunks, 2)
	})

	t.Run("include chunks without content (metadata only)", func(t *testing.T) {
		result := ProjectFields(doc, []string{"_chunks.*._id", "_chunks.*._start_char", "_chunks.*._end_char"})
		chunks := result["_chunks"].(map[string]any)
		myIndexChunks := chunks["my_index"].([]any)
		chunk0 := myIndexChunks[0].(map[string]any)

		// Should have metadata only
		assert.Equal(t, "0", chunk0["_id"])
		assert.Equal(t, 0, chunk0["_start_char"])
		assert.Equal(t, 100, chunk0["_end_char"])
		assert.Nil(t, chunk0["_content"]) // Not included
	})

	t.Run("empty fields returns empty result", func(t *testing.T) {
		result := ProjectFields(doc, []string{})
		assert.Empty(t, result)
	})

	t.Run("combine inclusion and exclusion", func(t *testing.T) {
		result := ProjectFields(doc, []string{"_chunks.*", "-_chunks.*._content"})
		chunks := result["_chunks"].(map[string]any)
		myIndexChunks := chunks["my_index"].([]any)
		chunk0 := myIndexChunks[0].(map[string]any)

		// Should have all chunks with metadata but no content
		assert.Equal(t, "0", chunk0["_id"])
		assert.Equal(t, 0, chunk0["_start_char"])
		assert.Nil(t, chunk0["_content"]) // Excluded
	})
}

func TestProjectFieldsNested(t *testing.T) {
	doc := map[string]any{
		"user": map[string]any{
			"name":  "John",
			"email": "john@example.com",
			"address": map[string]any{
				"city":    "NYC",
				"zipcode": "10001",
			},
		},
		"metadata": map[string]any{
			"created": "2024-01-01",
		},
	}

	t.Run("include nested path", func(t *testing.T) {
		result := ProjectFields(doc, []string{"user.name", "user.address.city"})
		assert.NotNil(t, result["user"])
		user := result["user"].(map[string]any)
		assert.Equal(t, "John", user["name"])
		assert.Nil(t, user["email"]) // Not included

		address := user["address"].(map[string]any)
		assert.Equal(t, "NYC", address["city"])
		assert.Nil(t, address["zipcode"]) // Not included
	})

	t.Run("exclude nested field", func(t *testing.T) {
		result := ProjectFields(doc, []string{"-user.email"})
		user := result["user"].(map[string]any)
		assert.Equal(t, "John", user["name"])
		assert.Nil(t, user["email"]) // Excluded
		assert.NotNil(t, user["address"])
	})
}
