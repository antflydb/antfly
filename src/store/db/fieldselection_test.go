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

func TestParseFieldSelection_Defaults(t *testing.T) {
	// Test default behavior with no fields specified
	regularPaths, queryOpts := ParseFieldSelection([]string{})

	assert.Empty(t, regularPaths, "No regular paths should be returned for star query")
	assert.True(t, queryOpts.AllSummaries, "Summaries should be included by default")
	assert.False(t, queryOpts.AllEmbeddings, "Embeddings should be excluded by default")
	assert.Empty(t, queryOpts.EmbeddingSuffixes)
	assert.Empty(t, queryOpts.SummarySuffixes)
}

func TestParseFieldSelection_RegularFieldsOnly(t *testing.T) {
	// Test with only regular fields
	regularPaths, queryOpts := ParseFieldSelection([]string{"name", "email", "user.address"})

	assert.Equal(t, []string{"name", "email", "user.address"}, regularPaths)
	assert.True(t, queryOpts.AllSummaries, "Summaries should be included by default")
	assert.False(t, queryOpts.AllEmbeddings, "Embeddings should be excluded by default")
}

func TestParseFieldSelection_AllEmbeddings(t *testing.T) {
	// Test requesting all embeddings
	regularPaths, queryOpts := ParseFieldSelection([]string{"name", "_embeddings"})

	assert.Equal(t, []string{"name"}, regularPaths)
	assert.True(t, queryOpts.AllEmbeddings, "Should include all embeddings")
	assert.True(t, queryOpts.AllSummaries, "Should include all summaries by default")
	assert.Empty(t, queryOpts.EmbeddingSuffixes)
}

func TestParseFieldSelection_SpecificEmbedding(t *testing.T) {
	// Test requesting specific embedding index
	regularPaths, queryOpts := ParseFieldSelection([]string{"name", "_embeddings.semantic_index"})

	assert.Equal(t, []string{"name"}, regularPaths)
	assert.False(t, queryOpts.AllEmbeddings, "Should not include all embeddings")
	assert.Equal(t, [][]byte{[]byte(":i:semantic_index:e")}, queryOpts.EmbeddingSuffixes)
	assert.True(t, queryOpts.AllSummaries, "Should include all summaries by default")
}

func TestParseFieldSelection_MultipleSpecificEmbeddings(t *testing.T) {
	// Test requesting multiple specific embedding indexes
	regularPaths, queryOpts := ParseFieldSelection([]string{
		"name",
		"_embeddings.semantic_index",
		"_embeddings.image_index",
	})

	assert.Equal(t, []string{"name"}, regularPaths)
	assert.False(t, queryOpts.AllEmbeddings)
	assert.Equal(t, [][]byte{
		[]byte(":i:semantic_index:e"),
		[]byte(":i:image_index:e"),
	}, queryOpts.EmbeddingSuffixes)
}

func TestParseFieldSelection_EmbeddingsWildcard(t *testing.T) {
	// Test explicit wildcard for embeddings
	regularPaths, queryOpts := ParseFieldSelection([]string{"name", "_embeddings.*"})

	assert.Equal(t, []string{"name"}, regularPaths)
	assert.True(t, queryOpts.AllEmbeddings, "Wildcard should include all embeddings")
	assert.Empty(t, queryOpts.EmbeddingSuffixes)
}

func TestParseFieldSelection_SpecificSummary(t *testing.T) {
	// Test requesting specific summary index
	regularPaths, queryOpts := ParseFieldSelection([]string{"name", "_summaries.summary_v2"})

	assert.Equal(t, []string{"name"}, regularPaths)
	assert.False(
		t,
		queryOpts.AllSummaries,
		"Should not include all summaries when specific requested",
	)
	assert.Equal(t, [][]byte{[]byte(":i:summary_v2:s")}, queryOpts.SummarySuffixes)
	assert.False(t, queryOpts.AllEmbeddings, "Embeddings should be excluded by default")
}

func TestParseFieldSelection_AllSummaries(t *testing.T) {
	// Test explicitly requesting all summaries
	regularPaths, queryOpts := ParseFieldSelection([]string{"name", "_summaries"})

	assert.Equal(t, []string{"name"}, regularPaths)
	assert.True(t, queryOpts.AllSummaries)
	assert.Empty(t, queryOpts.SummarySuffixes)
}

func TestParseFieldSelection_MixedFields(t *testing.T) {
	// Test complex mix of field types
	regularPaths, queryOpts := ParseFieldSelection([]string{
		"name",
		"user.email",
		"items.#.price",
		"_embeddings.semantic_index",
		"_summaries.summary_v2",
	})

	assert.Equal(t, []string{"name", "user.email", "items.#.price"}, regularPaths)
	assert.False(t, queryOpts.AllEmbeddings)
	assert.Equal(t, [][]byte{[]byte(":i:semantic_index:e")}, queryOpts.EmbeddingSuffixes)
	assert.False(t, queryOpts.AllSummaries)
	assert.Equal(t, [][]byte{[]byte(":i:summary_v2:s")}, queryOpts.SummarySuffixes)
}

func TestParseFieldSelection_StarQuery(t *testing.T) {
	// Test star query (select all)
	regularPaths, queryOpts := ParseFieldSelection([]string{})

	assert.Empty(t, regularPaths, "Star query should not filter regular fields")
	assert.True(t, queryOpts.AllSummaries, "Summaries included by default")
	assert.False(t, queryOpts.AllEmbeddings, "Embeddings excluded by default")
}

func TestParseFieldSelection_OnlySpecialFields(t *testing.T) {
	// Test with only special fields, no regular fields
	regularPaths, queryOpts := ParseFieldSelection([]string{
		"_embeddings",
		"_summaries",
	})

	assert.Empty(t, regularPaths, "No regular fields requested")
	assert.True(t, queryOpts.AllEmbeddings)
	assert.True(t, queryOpts.AllSummaries)
}

func TestParseFieldSelection_AllChunks(t *testing.T) {
	// Test requesting all chunks
	regularPaths, queryOpts := ParseFieldSelection([]string{"name", "_chunks"})

	assert.Equal(t, []string{"name"}, regularPaths)
	assert.True(t, queryOpts.AllChunks, "Should include all chunks")
	assert.True(t, queryOpts.AllSummaries, "Should include all summaries by default")
	assert.False(t, queryOpts.AllEmbeddings, "Should exclude embeddings by default")
}

func TestParseFieldSelection_ChunksWildcard(t *testing.T) {
	// Test explicit wildcard for chunks
	regularPaths, queryOpts := ParseFieldSelection([]string{"name", "_chunks.*"})

	assert.Equal(t, []string{"name"}, regularPaths)
	assert.True(t, queryOpts.AllChunks, "Wildcard should include all chunks")
}

func TestParseFieldSelection_MixedWithChunks(t *testing.T) {
	// Test complex mix of field types including chunks
	regularPaths, queryOpts := ParseFieldSelection([]string{
		"name",
		"user.email",
		"_embeddings.semantic_index",
		"_summaries.summary_v2",
		"_chunks",
	})

	assert.Equal(t, []string{"name", "user.email"}, regularPaths)
	assert.False(t, queryOpts.AllEmbeddings)
	assert.Equal(t, [][]byte{[]byte(":i:semantic_index:e")}, queryOpts.EmbeddingSuffixes)
	assert.False(t, queryOpts.AllSummaries)
	assert.Equal(t, [][]byte{[]byte(":i:summary_v2:s")}, queryOpts.SummarySuffixes)
	assert.True(t, queryOpts.AllChunks)
}

func TestParseFieldSelection_DefaultsExcludeChunks(t *testing.T) {
	// Test that chunks are excluded by default
	regularPaths, queryOpts := ParseFieldSelection([]string{})

	assert.Empty(t, regularPaths)
	assert.False(t, queryOpts.AllChunks, "Chunks should be excluded by default")
	assert.True(t, queryOpts.AllSummaries, "Summaries should be included by default")
	assert.False(t, queryOpts.AllEmbeddings, "Embeddings should be excluded by default")
}
