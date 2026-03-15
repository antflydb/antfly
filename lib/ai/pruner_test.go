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

package ai

import (
	"testing"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPruner(t *testing.T) {
	pruner, err := NewPruner("")
	require.NoError(t, err)
	assert.NotNil(t, pruner)
	assert.Equal(t, DefaultDocumentRenderer, pruner.documentRenderer)
}

func TestNewPrunerWithCustomRenderer(t *testing.T) {
	customRenderer := "{{this.fields.title}}: {{this.fields.content}}"
	pruner, err := NewPruner(customRenderer)
	require.NoError(t, err)
	assert.NotNil(t, pruner)
	assert.Equal(t, customRenderer, pruner.documentRenderer)
}

func TestCountTokens(t *testing.T) {
	pruner, err := NewPruner("")
	require.NoError(t, err)

	tests := []struct {
		name     string
		text     string
		minCount int
		maxCount int
	}{
		{
			name:     "empty string",
			text:     "",
			minCount: 0,
			maxCount: 0,
		},
		{
			name:     "single word",
			text:     "hello",
			minCount: 1,
			maxCount: 3,
		},
		{
			name:     "short sentence",
			text:     "The quick brown fox jumps over the lazy dog.",
			minCount: 8,
			maxCount: 15,
		},
		{
			name:     "longer text",
			text:     "Artificial intelligence is transforming the way we work and live. Machine learning algorithms can now process vast amounts of data to identify patterns and make predictions.",
			minCount: 20,
			maxCount: 50,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count := pruner.CountTokens(tt.text)
			assert.GreaterOrEqual(t, count, tt.minCount, "token count should be at least %d", tt.minCount)
			assert.LessOrEqual(t, count, tt.maxCount, "token count should be at most %d", tt.maxCount)
		})
	}
}

func TestRenderDocument(t *testing.T) {
	pruner, err := NewPruner("")
	require.NoError(t, err)

	doc := schema.Document{
		ID: "doc1",
		Fields: map[string]any{
			"title":   "Test Document",
			"content": "This is the document content.",
		},
	}

	rendered, err := pruner.RenderDocument(doc)
	require.NoError(t, err)

	// Should contain document ID
	assert.Contains(t, rendered, "doc1")
	// Should contain field values (rendered via TOON)
	assert.Contains(t, rendered, "Test Document")
	assert.Contains(t, rendered, "This is the document content.")
}

func TestRenderDocumentExcludesEmbeddings(t *testing.T) {
	pruner, err := NewPruner("")
	require.NoError(t, err)

	doc := schema.Document{
		ID: "doc1",
		Fields: map[string]any{
			"title":       "Test Document",
			"_embeddings": []float32{0.1, 0.2, 0.3}, // Should be excluded
		},
	}

	rendered, err := pruner.RenderDocument(doc)
	require.NoError(t, err)

	// Should not contain embeddings
	assert.NotContains(t, rendered, "_embeddings")
	assert.NotContains(t, rendered, "0.1")
}

func TestEstimateDocumentTokens(t *testing.T) {
	pruner, err := NewPruner("")
	require.NoError(t, err)

	doc := schema.Document{
		ID: "doc1",
		Fields: map[string]any{
			"title":   "Test Document",
			"content": "This is a test document with some content for token estimation.",
		},
	}

	tokens, err := pruner.EstimateDocumentTokens(doc)
	require.NoError(t, err)
	assert.Positive(t, tokens)
}

func TestPruneToTokenBudget_NoPruningNeeded(t *testing.T) {
	pruner, err := NewPruner("")
	require.NoError(t, err)

	docs := []schema.Document{
		{ID: "doc1", Fields: map[string]any{"content": "Short text."}},
		{ID: "doc2", Fields: map[string]any{"content": "Another short text."}},
	}

	// Large budget - no pruning needed
	prunedDocs, stats, err := pruner.PruneToTokenBudget(docs, 100000, 1000)
	require.NoError(t, err)

	assert.Len(t, prunedDocs, 2)
	assert.Equal(t, 2, stats.ResourcesKept)
	assert.Equal(t, 0, stats.ResourcesPruned)
}

func TestPruneToTokenBudget_SomePruned(t *testing.T) {
	pruner, err := NewPruner("")
	require.NoError(t, err)

	docs := []schema.Document{
		{ID: "doc1", Fields: map[string]any{"content": "First document with some content."}},
		{ID: "doc2", Fields: map[string]any{"content": "Second document with more content here."}},
		{ID: "doc3", Fields: map[string]any{"content": "Third document that might get pruned."}},
	}

	// Estimate tokens for first doc to set a tight budget
	tokens1, err := pruner.EstimateDocumentTokens(docs[0])
	require.NoError(t, err)

	// Set budget to only fit the first document
	prunedDocs, stats, err := pruner.PruneToTokenBudget(docs, tokens1+10, 0)
	require.NoError(t, err)

	assert.Len(t, prunedDocs, 1)
	assert.Equal(t, "doc1", prunedDocs[0].ID)
	assert.Equal(t, 1, stats.ResourcesKept)
	assert.Equal(t, 2, stats.ResourcesPruned)
}

func TestPruneToTokenBudget_AllPruned(t *testing.T) {
	pruner, err := NewPruner("")
	require.NoError(t, err)

	docs := []schema.Document{
		{ID: "doc1", Fields: map[string]any{"content": "Document content."}},
	}

	// Reserve more tokens than available
	prunedDocs, stats, err := pruner.PruneToTokenBudget(docs, 100, 200)
	require.NoError(t, err)

	assert.Empty(t, prunedDocs)
	assert.Equal(t, 0, stats.ResourcesKept)
	assert.Equal(t, 1, stats.ResourcesPruned)
}

func TestPruneToTokenBudget_EmptyDocs(t *testing.T) {
	pruner, err := NewPruner("")
	require.NoError(t, err)

	prunedDocs, stats, err := pruner.PruneToTokenBudget(nil, 1000, 100)
	require.NoError(t, err)

	assert.Empty(t, prunedDocs)
	assert.Equal(t, 0, stats.ResourcesKept)
	assert.Equal(t, 0, stats.ResourcesPruned)
}

func TestPruneToTokenBudget_PreservesOrder(t *testing.T) {
	pruner, err := NewPruner("")
	require.NoError(t, err)

	docs := []schema.Document{
		{ID: "doc1", Fields: map[string]any{"content": "First."}},
		{ID: "doc2", Fields: map[string]any{"content": "Second."}},
		{ID: "doc3", Fields: map[string]any{"content": "Third."}},
	}

	// Estimate tokens to fit first two docs
	tokens1, _ := pruner.EstimateDocumentTokens(docs[0])
	tokens2, _ := pruner.EstimateDocumentTokens(docs[1])

	prunedDocs, stats, err := pruner.PruneToTokenBudget(docs, tokens1+tokens2+10, 0)
	require.NoError(t, err)

	// Should keep first two docs in order
	assert.Len(t, prunedDocs, 2)
	assert.Equal(t, "doc1", prunedDocs[0].ID)
	assert.Equal(t, "doc2", prunedDocs[1].ID)
	assert.Equal(t, 2, stats.ResourcesKept)
	assert.Equal(t, 1, stats.ResourcesPruned)
}

func TestEstimateTotalTokens(t *testing.T) {
	pruner, err := NewPruner("")
	require.NoError(t, err)

	docs := []schema.Document{
		{ID: "doc1", Fields: map[string]any{"content": "First document."}},
		{ID: "doc2", Fields: map[string]any{"content": "Second document."}},
	}

	total, err := pruner.EstimateTotalTokens(docs)
	require.NoError(t, err)
	assert.Positive(t, total)

	// Total should be sum of individual estimates
	tokens1, _ := pruner.EstimateDocumentTokens(docs[0])
	tokens2, _ := pruner.EstimateDocumentTokens(docs[1])
	assert.Equal(t, tokens1+tokens2, total)
}
