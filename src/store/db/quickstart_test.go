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
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/antflydb/antfly/lib/chunking"
	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vectorindex"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/searcher"
	"github.com/cespare/xxhash/v2"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// TestFilterQueryWithChunkedEmbeddings verifies that filter_query combined with
// vector search returns hits when the embedding index uses chunked embeddings.
func TestFilterQueryWithChunkedEmbeddings(t *testing.T) {
	dir := t.TempDir()
	lg := zaptest.NewLogger(t)

	db := &DBImpl{
		logger: lg,
	}

	require.NoError(t, db.Open(dir, false, nil, types.Range{nil, []byte{0xFF}}))
	defer db.Close()
	defer os.RemoveAll(dir)

	dimension := 3
	indexName := "test_emb"

	tableSchema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"content": map[string]any{"type": "string"},
						"title":   map[string]any{"type": "string"},
					},
				},
			},
		},
	}

	indexConfig := indexes.EmbeddingsIndexConfig{
		Dimension: dimension,
		Field:     "content",
		Template:  "{{content}}",
		Chunker: &chunking.ChunkerConfig{
			Provider:    chunking.ChunkerProviderMock,
			StoreChunks: false,
		},
	}
	idxCfg := indexes.NewEmbeddingsConfig(indexName, indexConfig)

	indexManager, err := NewIndexManager(lg, &common.Config{}, db.pdb, dir, tableSchema,
		types.Range{nil, []byte{0xFF}}, nil)
	require.NoError(t, err)
	require.NoError(t, db.SetIndexManager(indexManager))

	fullTextConfig := indexes.NewFullTextIndexConfig("full_text_index_v0", false)
	require.NoError(t, indexManager.Register("full_text_index_v0", false, *fullTextConfig))
	require.NoError(t, indexManager.Register(indexName, false, *idxCfg))
	require.NoError(t, indexManager.Start(false))

	ctx := context.Background()

	type testDoc struct {
		key       string
		content   string
		chunkEmbs []vector.T
	}
	docs := []testDoc{
		{
			key:     "Funny (song)",
			content: "A funny song about something amusing and entertaining",
			chunkEmbs: []vector.T{
				{1.0, 0.0, 0.0},
				{0.9, 0.1, 0.0},
			},
		},
		{
			key:     "Gothic (Nox Arcana album)",
			content: "Gothic is the 22nd concept album by the musical group",
			chunkEmbs: []vector.T{
				{0.0, 1.0, 0.0},
				{0.1, 0.9, 0.0},
			},
		},
		{
			key:     "Fahad Iqbal",
			content: "Pakistani first-class cricketer who played for multiple teams",
			chunkEmbs: []vector.T{
				{0.0, 0.0, 1.0},
				{0.0, 0.1, 0.9},
			},
		},
	}

	// Insert base documents into the full-text index.
	for _, d := range docs {
		doc := map[string]any{"content": d.content}
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)

		err = db.Batch(ctx, [][2][]byte{{[]byte(d.key), docJSON}}, nil, Op_SyncLevelFullText)
		if err != nil && !errors.Is(err, ErrPartialSuccess) {
			require.NoError(t, err)
		}
	}

	t.Run("verify_full_text_index", func(t *testing.T) {
		matchAll := bleve.NewMatchAllQuery()
		bleveReq := bleve.NewSearchRequest(matchAll)
		bleveReq.Size = 100
		resp, err := db.SearchIndex(ctx, "full_text_index_v0", bleveReq)
		require.NoError(t, err)
		result := resp.(*bleve.SearchResult)
		require.Equal(t, uint64(3), result.Total)
	})

	// Write chunk embeddings via db.Batch, simulating what the enricher produces.
	embedderSuffix := fmt.Appendf(nil, ":i:%s:e", indexName)
	var chunkEmbWrites [][2][]byte
	for _, d := range docs {
		for chunkIdx, emb := range d.chunkEmbs {
			chunkKey := storeutils.MakeChunkKey([]byte(d.key), indexName, uint32(chunkIdx))
			chunkEmbKey := append(chunkKey, embedderSuffix...)

			hashID := xxhash.Sum64String(fmt.Sprintf("chunk %d of %s", chunkIdx, d.key))
			val, err := vectorindex.EncodeEmbeddingWithHashID(nil, emb, hashID)
			require.NoError(t, err)

			chunkEmbWrites = append(chunkEmbWrites, [2][]byte{chunkEmbKey, val})
		}
	}
	err = db.Batch(ctx, chunkEmbWrites, nil, Op_SyncLevelEmbeddings)
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	t.Run("verify_vector_index", func(t *testing.T) {
		searchReq := &vectorindex.SearchRequest{
			K:         10,
			Embedding: vector.T{1.0, 0.0, 0.0},
		}
		resp, err := db.SearchIndex(ctx, indexName, searchReq)
		require.NoError(t, err)
		result := resp.(*vectorindex.SearchResult)
		assert.NotEmpty(t, result.Hits)
	})

	t.Run("filter_query_with_chunked_vector_search", func(t *testing.T) {
		searchReq := indexes.RemoteIndexSearchRequest{
			Star:        true,
			Limit:       10,
			FilterQuery: json.RawMessage(`{"match_all":{}}`),
			VectorSearches: map[string]vector.T{
				indexName: {1.0, 0.0, 0.0},
			},
		}

		encodedReq, err := json.Marshal(searchReq)
		require.NoError(t, err)

		respBytes, err := db.Search(ctx, encodedReq)
		require.NoError(t, err)

		var result indexes.RemoteIndexSearchResult
		require.NoError(t, json.Unmarshal(respBytes, &result))

		vResult, ok := result.VectorSearchResult[indexName]
		require.True(t, ok, "Expected vector search result")
		assert.NotEmpty(t, vResult.Hits,
			"filter_query + vector search should return hits with chunked embeddings")
	})

	t.Run("vector_search_without_filter_works", func(t *testing.T) {
		searchReq := indexes.RemoteIndexSearchRequest{
			Star:  true,
			Limit: 10,
			VectorSearches: map[string]vector.T{
				indexName: {1.0, 0.0, 0.0},
			},
		}

		encodedReq, err := json.Marshal(searchReq)
		require.NoError(t, err)

		respBytes, err := db.Search(ctx, encodedReq)
		require.NoError(t, err)

		var result indexes.RemoteIndexSearchResult
		require.NoError(t, json.Unmarshal(respBytes, &result))

		vResult, ok := result.VectorSearchResult[indexName]
		require.True(t, ok)
		assert.NotEmpty(t, vResult.Hits)
	})
}

func TestDeleteRemovesChunkedEmbeddings(t *testing.T) {
	dir := t.TempDir()
	lg := zaptest.NewLogger(t)

	db := &DBImpl{
		logger:       lg,
		antflyConfig: &common.Config{},
		indexes:      make(map[string]indexes.IndexConfig),
	}

	require.NoError(t, db.Open(dir, false, nil, types.Range{nil, []byte{0xFF}}))
	defer db.Close()

	indexName := "test_emb_delete"
	idxCfg := indexes.NewEmbeddingsConfig(indexName, indexes.EmbeddingsIndexConfig{
		Dimension: 3,
		Field:     "content",
		Chunker: &chunking.ChunkerConfig{
			Provider:    chunking.ChunkerProviderMock,
			StoreChunks: true,
		},
	})
	require.NoError(t, db.AddIndex(*idxCfg))

	ctx := context.Background()
	docKey := []byte("doc1")
	docJSON, err := json.Marshal(map[string]any{"content": "chunked content"})
	require.NoError(t, err)
	err = db.Batch(ctx, [][2][]byte{{docKey, docJSON}}, nil, Op_SyncLevelEmbeddings)
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	chunkKey := storeutils.MakeChunkKey(docKey, indexName, 0)
	chunk := chunking.NewTextChunk(0, "chunk body", 0, 10)
	chunkJSON, err := json.Marshal(chunk)
	require.NoError(t, err)
	chunkValue := make([]byte, 0, len(chunkJSON)+8)
	chunkValue = encoding.EncodeUint64Ascending(chunkValue, xxhash.Sum64String("chunk body"))
	chunkValue = append(chunkValue, chunkJSON...)

	chunkEmbKey := append(bytes.Clone(chunkKey), fmt.Appendf(nil, ":i:%s:e", indexName)...)
	embeddingValue, err := vectorindex.EncodeEmbeddingWithHashID(nil, vector.T{1.0, 0.0, 0.0}, xxhash.Sum64(chunkKey))
	require.NoError(t, err)

	err = db.Batch(ctx, [][2][]byte{
		{chunkKey, chunkValue},
		{chunkEmbKey, embeddingValue},
	}, nil, Op_SyncLevelEmbeddings)
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	searchReq := &vectorindex.SearchRequest{
		K:         10,
		Embedding: vector.T{1.0, 0.0, 0.0},
	}
	resp, err := db.SearchIndex(ctx, indexName, searchReq)
	require.NoError(t, err)
	result := resp.(*vectorindex.SearchResult)
	require.NotEmpty(t, result.Hits)
	require.Equal(t, string(chunkKey), result.Hits[0].ID)

	err = db.Batch(ctx, nil, [][]byte{docKey}, Op_SyncLevelEmbeddings)
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	resp, err = db.SearchIndex(ctx, indexName, searchReq)
	require.NoError(t, err)
	result = resp.(*vectorindex.SearchResult)
	require.Empty(t, result.Hits)
}

func TestDeleteRemovesChunkedEmbeddingsWhenDocKeyContainsIndexMarker(t *testing.T) {
	dir := t.TempDir()
	lg := zaptest.NewLogger(t)

	db := &DBImpl{
		logger:       lg,
		antflyConfig: &common.Config{},
		indexes:      make(map[string]indexes.IndexConfig),
	}

	require.NoError(t, db.Open(dir, false, nil, types.Range{nil, []byte{0xFF}}))
	defer db.Close()

	indexName := "test_emb_delete"
	idxCfg := indexes.NewEmbeddingsConfig(indexName, indexes.EmbeddingsIndexConfig{
		Dimension: 3,
		Field:     "content",
		Chunker: &chunking.ChunkerConfig{
			Provider:    chunking.ChunkerProviderMock,
			StoreChunks: true,
		},
	})
	require.NoError(t, db.AddIndex(*idxCfg))

	ctx := context.Background()
	docKey := []byte("doc:i:1")
	docJSON, err := json.Marshal(map[string]any{"content": "chunked content"})
	require.NoError(t, err)
	err = db.Batch(ctx, [][2][]byte{{docKey, docJSON}}, nil, Op_SyncLevelEmbeddings)
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	chunkKey := storeutils.MakeChunkKey(docKey, indexName, 0)
	chunk := chunking.NewTextChunk(0, "chunk body", 0, 10)
	chunkJSON, err := json.Marshal(chunk)
	require.NoError(t, err)
	chunkValue := make([]byte, 0, len(chunkJSON)+8)
	chunkValue = encoding.EncodeUint64Ascending(chunkValue, xxhash.Sum64String("chunk body"))
	chunkValue = append(chunkValue, chunkJSON...)

	chunkEmbKey := append(bytes.Clone(chunkKey), fmt.Appendf(nil, ":i:%s:e", indexName)...)
	embeddingValue, err := vectorindex.EncodeEmbeddingWithHashID(nil, vector.T{1.0, 0.0, 0.0}, xxhash.Sum64(chunkKey))
	require.NoError(t, err)

	err = db.Batch(ctx, [][2][]byte{
		{chunkKey, chunkValue},
		{chunkEmbKey, embeddingValue},
	}, nil, Op_SyncLevelEmbeddings)
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	searchReq := &vectorindex.SearchRequest{
		K:         10,
		Embedding: vector.T{1.0, 0.0, 0.0},
	}
	resp, err := db.SearchIndex(ctx, indexName, searchReq)
	require.NoError(t, err)
	result := resp.(*vectorindex.SearchResult)
	require.NotEmpty(t, result.Hits)
	require.Equal(t, string(chunkKey), result.Hits[0].ID)

	err = db.Batch(ctx, nil, [][]byte{docKey}, Op_SyncLevelEmbeddings)
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	resp, err = db.SearchIndex(ctx, indexName, searchReq)
	require.NoError(t, err)
	result = resp.(*vectorindex.SearchResult)
	require.Empty(t, result.Hits)
}

func init() {
	_ = searcher.NewMatchAllSearcher
}
