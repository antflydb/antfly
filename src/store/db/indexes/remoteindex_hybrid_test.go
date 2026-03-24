package indexes

import (
	"testing"

	"github.com/antflydb/antfly/lib/vector"
	blevequery "github.com/blevesearch/bleve/v2/search/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrepareHybridFullTextForSemanticSearch(t *testing.T) {
	t.Run("match_all becomes semantic noop with fallback", func(t *testing.T) {
		fullText := blevequery.NewMatchAllQuery()
		q := &Query{
			FullTextSearch: fullText,
			Embeddings:     map[string]vector.T{"emb": {1, 0, 0}},
		}

		fallback := q.PrepareHybridFullTextForSemanticSearch()

		require.Same(t, fullText, fallback)
		assert.Nil(t, q.FullTextSearch)
	})

	t.Run("match_none is removed from hybrid execution", func(t *testing.T) {
		q := &Query{
			FullTextSearch: blevequery.NewMatchNoneQuery(),
			Embeddings:     map[string]vector.T{"emb": {1, 0, 0}},
		}

		fallback := q.PrepareHybridFullTextForSemanticSearch()

		assert.Nil(t, fallback)
		assert.Nil(t, q.FullTextSearch)
	})

	t.Run("non hybrid query keeps full text intact", func(t *testing.T) {
		fullText := blevequery.NewMatchAllQuery()
		q := &Query{
			FullTextSearch: fullText,
		}

		fallback := q.PrepareHybridFullTextForSemanticSearch()

		assert.Nil(t, fallback)
		assert.Equal(t, HybridFullTextModeMatchAll, q.HybridFullTextMode)
		require.Same(t, fullText, q.FullTextSearch)
	})

	t.Run("normal lexical query is unchanged", func(t *testing.T) {
		fullText := blevequery.NewMatchQuery("hello")
		q := &Query{
			FullTextSearch: fullText,
			Embeddings:     map[string]vector.T{"emb": {1, 0, 0}},
		}

		fallback := q.PrepareHybridFullTextForSemanticSearch()

		assert.Nil(t, fallback)
		assert.Equal(t, HybridFullTextModeNone, q.HybridFullTextMode)
		require.Same(t, fullText, q.FullTextSearch)
	})

	t.Run("wrapped match_all normalizes and falls back", func(t *testing.T) {
		fullText := blevequery.NewDisjunctionQuery([]blevequery.Query{
			blevequery.NewMatchAllQuery(),
			blevequery.NewMatchQuery("hello"),
		})
		q := &Query{
			FullTextSearch: fullText,
			Embeddings:     map[string]vector.T{"emb": {1, 0, 0}},
		}

		fallback := q.PrepareHybridFullTextForSemanticSearch()

		assert.Equal(t, HybridFullTextModeMatchAll, q.HybridFullTextMode)
		require.Same(t, fullText, fallback)
		assert.Nil(t, q.FullTextSearch)
	})

	t.Run("wrapped match_none normalizes away", func(t *testing.T) {
		q := &Query{
			FullTextSearch: blevequery.NewConjunctionQuery([]blevequery.Query{
				blevequery.NewMatchNoneQuery(),
				blevequery.NewMatchQuery("hello"),
			}),
			Embeddings: map[string]vector.T{"emb": {1, 0, 0}},
		}

		fallback := q.PrepareHybridFullTextForSemanticSearch()

		assert.Equal(t, HybridFullTextModeMatchNone, q.HybridFullTextMode)
		assert.Nil(t, fallback)
		assert.Nil(t, q.FullTextSearch)
	})

	t.Run("synthetic full_text_index query normalizes after generation", func(t *testing.T) {
		fullText := blevequery.NewDisjunctionQuery([]blevequery.Query{
			blevequery.NewQueryStringQuery("semantic search"),
			blevequery.NewMatchAllQuery(),
		})
		q := &Query{
			FullTextSearch: fullText,
			Embeddings:     map[string]vector.T{"emb": {1, 0, 0}},
		}

		fallback := q.PrepareHybridFullTextForSemanticSearch()

		assert.Equal(t, HybridFullTextModeMatchAll, q.HybridFullTextMode)
		require.Same(t, fullText, fallback)
		assert.Nil(t, q.FullTextSearch)
	})
}

func TestHybridFullTextFallbackQuery(t *testing.T) {
	t.Run("applies filter and exclusion to fallback", func(t *testing.T) {
		q := &Query{
			FilterQuery:    blevequery.NewMatchQuery("filtered"),
			ExclusionQuery: blevequery.NewMatchQuery("blocked"),
		}

		fallback := q.HybridFullTextFallbackQuery(blevequery.NewMatchAllQuery())

		booleanFallback, ok := fallback.(*blevequery.BooleanQuery)
		require.True(t, ok)
		require.NotNil(t, booleanFallback.Must)
		require.NotNil(t, booleanFallback.MustNot)
	})
}

func TestShouldUseHybridFullTextFallback(t *testing.T) {
	fallback := blevequery.NewMatchAllQuery()

	assert.True(t, ShouldUseHybridFullTextFallback(
		HybridFullTextModeMatchAll,
		fallback,
		false,
	))

	assert.False(t, ShouldUseHybridFullTextFallback(
		HybridFullTextModeMatchAll,
		fallback,
		true,
	))

	assert.False(t, ShouldUseHybridFullTextFallback(
		HybridFullTextModeMatchNone,
		fallback,
		false,
	))
}

func TestApplyPrunerToFusionResult(t *testing.T) {
	fusionResult := &FusionResult{
		Hits: []*FusionHit{
			{ID: "doc1", Score: 10},
			{ID: "doc2", Score: 5},
		},
		MaxScore: 10,
	}

	ApplyPrunerToFusionResult(fusionResult, &Pruner{MinAbsoluteScore: 9})

	require.Len(t, fusionResult.Hits, 1)
	assert.Equal(t, "doc1", fusionResult.Hits[0].ID)
	assert.Equal(t, 10.0, fusionResult.MaxScore)
}
