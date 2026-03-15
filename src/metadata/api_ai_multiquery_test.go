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
	"testing"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeduplicateDocuments(t *testing.T) {
	t.Run("empty input returns empty output", func(t *testing.T) {
		docs, mapping := deduplicateDocuments(nil)
		assert.Empty(t, docs)
		assert.Empty(t, mapping)

		docs, mapping = deduplicateDocuments([]documentWithSubQuestions{})
		assert.Empty(t, docs)
		assert.Empty(t, mapping)
	})

	t.Run("single document returns unchanged", func(t *testing.T) {
		input := []documentWithSubQuestions{
			{
				Doc:          schema.Document{ID: "doc1", Fields: map[string]any{"title": "Test"}},
				SubQuestions: []int{0},
			},
		}

		docs, mapping := deduplicateDocuments(input)

		require.Len(t, docs, 1)
		assert.Equal(t, "doc1", docs[0].ID)
		assert.Equal(t, []int{0}, mapping["doc1"])
	})

	t.Run("deduplicates documents by ID", func(t *testing.T) {
		input := []documentWithSubQuestions{
			{
				Doc:          schema.Document{ID: "doc1", Fields: map[string]any{"title": "Test 1"}},
				SubQuestions: []int{0},
			},
			{
				Doc:          schema.Document{ID: "doc2", Fields: map[string]any{"title": "Test 2"}},
				SubQuestions: []int{1},
			},
			{
				Doc:          schema.Document{ID: "doc1", Fields: map[string]any{"title": "Test 1"}}, // duplicate
				SubQuestions: []int{1},
			},
		}

		docs, mapping := deduplicateDocuments(input)

		require.Len(t, docs, 2, "Should have 2 unique documents")
		assert.Equal(t, "doc1", docs[0].ID)
		assert.Equal(t, "doc2", docs[1].ID)

		// Verify mapping tracks merged sub-questions
		assert.Contains(t, mapping["doc1"], 0)
		assert.Contains(t, mapping["doc1"], 1)
		assert.Equal(t, []int{1}, mapping["doc2"])
	})

	t.Run("merges sub-question indices for duplicates", func(t *testing.T) {
		input := []documentWithSubQuestions{
			{
				Doc:          schema.Document{ID: "doc1", Fields: map[string]any{"title": "Test 1"}},
				SubQuestions: []int{0},
			},
			{
				Doc:          schema.Document{ID: "doc1", Fields: map[string]any{"title": "Test 1"}},
				SubQuestions: []int{1, 2},
			},
			{
				Doc:          schema.Document{ID: "doc1", Fields: map[string]any{"title": "Test 1"}},
				SubQuestions: []int{0, 3}, // 0 is duplicate, 3 is new
			},
		}

		docs, mapping := deduplicateDocuments(input)

		require.Len(t, docs, 1)
		assert.Equal(t, "doc1", docs[0].ID)

		// Should have merged sub-questions: 0, 1, 2, 3 (without duplicates)
		subQs := mapping["doc1"]
		assert.Contains(t, subQs, 0)
		assert.Contains(t, subQs, 1)
		assert.Contains(t, subQs, 2)
		assert.Contains(t, subQs, 3)
	})

	t.Run("preserves order of first occurrence", func(t *testing.T) {
		input := []documentWithSubQuestions{
			{
				Doc:          schema.Document{ID: "doc3", Fields: map[string]any{"title": "Third"}},
				SubQuestions: []int{0},
			},
			{
				Doc:          schema.Document{ID: "doc1", Fields: map[string]any{"title": "First"}},
				SubQuestions: []int{1},
			},
			{
				Doc:          schema.Document{ID: "doc2", Fields: map[string]any{"title": "Second"}},
				SubQuestions: []int{2},
			},
			{
				Doc:          schema.Document{ID: "doc3", Fields: map[string]any{"title": "Third"}}, // duplicate
				SubQuestions: []int{1},
			},
		}

		docs, _ := deduplicateDocuments(input)

		require.Len(t, docs, 3)
		// Order should be: doc3, doc1, doc2 (order of first occurrence)
		assert.Equal(t, "doc3", docs[0].ID)
		assert.Equal(t, "doc1", docs[1].ID)
		assert.Equal(t, "doc2", docs[2].ID)
	})

	t.Run("handles multiple duplicates across sub-questions", func(t *testing.T) {
		// Simulates real multiquery scenario where same doc appears for multiple sub-questions
		input := []documentWithSubQuestions{
			// Sub-question 0 results
			{Doc: schema.Document{ID: "doc1"}, SubQuestions: []int{0}},
			{Doc: schema.Document{ID: "doc2"}, SubQuestions: []int{0}},
			{Doc: schema.Document{ID: "doc3"}, SubQuestions: []int{0}},
			// Sub-question 1 results (doc1 and doc2 appear again)
			{Doc: schema.Document{ID: "doc1"}, SubQuestions: []int{1}},
			{Doc: schema.Document{ID: "doc2"}, SubQuestions: []int{1}},
			{Doc: schema.Document{ID: "doc4"}, SubQuestions: []int{1}},
			// Sub-question 2 results (doc1 appears again)
			{Doc: schema.Document{ID: "doc1"}, SubQuestions: []int{2}},
			{Doc: schema.Document{ID: "doc5"}, SubQuestions: []int{2}},
		}

		docs, mapping := deduplicateDocuments(input)

		require.Len(t, docs, 5, "Should have 5 unique documents")

		// doc1 should appear in all 3 sub-questions
		assert.ElementsMatch(t, []int{0, 1, 2}, mapping["doc1"])
		// doc2 should appear in sub-questions 0 and 1
		assert.ElementsMatch(t, []int{0, 1}, mapping["doc2"])
		// doc3 should only appear in sub-question 0
		assert.Equal(t, []int{0}, mapping["doc3"])
		// doc4 should only appear in sub-question 1
		assert.Equal(t, []int{1}, mapping["doc4"])
		// doc5 should only appear in sub-question 2
		assert.Equal(t, []int{2}, mapping["doc5"])
	})
}
