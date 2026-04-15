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
	"encoding/binary"
	"math"
	"testing"

	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/vectorindex"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExportImportPortable_Documents(t *testing.T) {
	srcDB := setupTestDB(t)
	defer cleanupTestDB(t, srcDB)

	ctx := context.Background()

	// Insert documents directly into Pebble with the standard key encoding
	docs := map[string][]byte{
		"doc1": []byte(`{"id":"doc1","title":"Hello"}`),
		"doc2": []byte(`{"id":"doc2","title":"World"}`),
		"doc3": []byte(`{"id":"doc3","title":"Test"}`),
	}

	batch := srcDB.pdb.NewBatch()
	for key, val := range docs {
		pebbleKey := storeutils.KeyRangeStart([]byte(key))
		require.NoError(t, batch.Set(pebbleKey, val, nil))
	}
	require.NoError(t, batch.Commit(pebble.Sync))

	// Export
	var buf bytes.Buffer
	err := srcDB.ExportPortable(ctx, &buf)
	require.NoError(t, err)

	// Verify it's a valid AFB file
	assert.True(t, common.IsAFBFormat(buf.Bytes()))

	// Import into a fresh DB
	dstDB := setupTestDB(t)
	defer cleanupTestDB(t, dstDB)

	err = dstDB.ImportPortable(ctx, &buf)
	require.NoError(t, err)

	// Verify all documents were imported
	for key, expectedVal := range docs {
		pebbleKey := storeutils.KeyRangeStart([]byte(key))
		val, closer, err := dstDB.pdb.Get(pebbleKey)
		require.NoError(t, err, "key %s should exist", key)
		assert.Equal(t, expectedVal, val)
		closer.Close()
	}
}

func TestExportImportPortable_Embeddings(t *testing.T) {
	srcDB := setupTestDB(t)
	defer cleanupTestDB(t, srcDB)

	ctx := context.Background()

	// Write a document
	docKey := []byte("emb-doc1")
	pebbleDocKey := storeutils.KeyRangeStart(docKey)
	require.NoError(t, srcDB.pdb.Set(pebbleDocKey, []byte(`{"id":"emb-doc1"}`), pebble.Sync))

	// Write an embedding: <docKey>:i:<indexName>:e
	indexName := "my_embedding"
	embKey := storeutils.MakeEmbeddingKey(docKey, indexName)
	vec := []float32{0.1, 0.2, 0.3, 0.4}
	var hashID uint64 = 12345
	embVal, err := vectorindex.EncodeEmbeddingWithHashID(nil, vec, hashID)
	require.NoError(t, err)
	require.NoError(t, srcDB.pdb.Set(embKey, embVal, pebble.Sync))

	// Export
	var buf bytes.Buffer
	err = srcDB.ExportPortable(ctx, &buf)
	require.NoError(t, err)

	// Import into fresh DB
	dstDB := setupTestDB(t)
	defer cleanupTestDB(t, dstDB)

	err = dstDB.ImportPortable(ctx, &buf)
	require.NoError(t, err)

	// Verify document
	val, closer, err := dstDB.pdb.Get(pebbleDocKey)
	require.NoError(t, err)
	assert.Equal(t, []byte(`{"id":"emb-doc1"}`), val)
	closer.Close()

	// Verify embedding
	restoredEmbKey := storeutils.MakeEmbeddingKey(docKey, indexName)
	embData, closer, err := dstDB.pdb.Get(restoredEmbKey)
	require.NoError(t, err)
	defer closer.Close()

	restoredHash, restoredVec, _, err := vectorindex.DecodeEmbeddingWithHashID(embData)
	require.NoError(t, err)
	assert.Equal(t, hashID, restoredHash)
	assert.InDeltaSlice(t, vec, []float32(restoredVec), 1e-6)
}

func TestExportImportPortable_SparseVectors(t *testing.T) {
	srcDB := setupTestDB(t)
	defer cleanupTestDB(t, srcDB)

	ctx := context.Background()

	// Write document
	docKey := []byte("sparse-doc1")
	pebbleDocKey := storeutils.KeyRangeStart(docKey)
	require.NoError(t, srcDB.pdb.Set(pebbleDocKey, []byte(`{"id":"sparse-doc1"}`), pebble.Sync))

	// Write sparse vector: <docKey>:i:<indexName>:sp
	indexName := "my_sparse"
	sparseKey := storeutils.MakeSparseKey(docKey, indexName)
	var sparseHashID uint64 = 67890
	indices := []uint32{0, 5, 10, 42}
	values := []float32{1.0, 0.5, 0.25, 0.1}
	sparseVal := encoding.EncodeUint64Ascending(nil, sparseHashID)
	sparseVal = encodeSparseVecRaw(sparseVal, indices, values)
	require.NoError(t, srcDB.pdb.Set(sparseKey, sparseVal, pebble.Sync))

	// Export
	var buf bytes.Buffer
	err := srcDB.ExportPortable(ctx, &buf)
	require.NoError(t, err)

	// Import
	dstDB := setupTestDB(t)
	defer cleanupTestDB(t, dstDB)

	err = dstDB.ImportPortable(ctx, &buf)
	require.NoError(t, err)

	// Verify sparse vector
	restoredSparseKey := storeutils.MakeSparseKey(docKey, indexName)
	data, closer, err := dstDB.pdb.Get(restoredSparseKey)
	require.NoError(t, err)
	defer closer.Close()

	// Decode: [hashID:u64][sparse vec]
	remaining, restoredHash, err := encoding.DecodeUint64Ascending(data)
	require.NoError(t, err)
	assert.Equal(t, sparseHashID, restoredHash)

	restoredIndices, restoredValues, err := decodeSparseVecRaw(remaining)
	require.NoError(t, err)
	assert.Equal(t, indices, restoredIndices)
	assert.InDeltaSlice(t, values, restoredValues, 1e-6)
}

func TestExportImportPortable_Edges(t *testing.T) {
	srcDB := setupTestDB(t)
	defer cleanupTestDB(t, srcDB)

	ctx := context.Background()

	// Write source and target documents
	for _, key := range []string{"alice", "bob"} {
		pebbleKey := storeutils.KeyRangeStart([]byte(key))
		require.NoError(t, srcDB.pdb.Set(pebbleKey, []byte(`{"id":"`+key+`"}`), pebble.Sync))
	}

	// Write outgoing edge: alice -> bob
	indexName := "social"
	edgeType := "follows"
	edgeKey := storeutils.MakeEdgeKey([]byte("alice"), []byte("bob"), indexName, edgeType)

	// Simple edge value: [weight:f64][created:u64][updated:u64][metadata]
	edgeVal := make([]byte, 24)
	binary.LittleEndian.PutUint64(edgeVal[0:8], math.Float64bits(1.0))
	binary.LittleEndian.PutUint64(edgeVal[8:16], 1000)
	binary.LittleEndian.PutUint64(edgeVal[16:24], 2000)
	edgeVal = append(edgeVal, []byte("{}")...)
	require.NoError(t, srcDB.pdb.Set(edgeKey, edgeVal, pebble.Sync))

	// Export
	var buf bytes.Buffer
	err := srcDB.ExportPortable(ctx, &buf)
	require.NoError(t, err)

	// Import
	dstDB := setupTestDB(t)
	defer cleanupTestDB(t, dstDB)

	err = dstDB.ImportPortable(ctx, &buf)
	require.NoError(t, err)

	// Verify edge was restored
	restoredEdgeKey := storeutils.MakeEdgeKey([]byte("alice"), []byte("bob"), indexName, edgeType)
	data, closer, err := dstDB.pdb.Get(restoredEdgeKey)
	require.NoError(t, err)
	defer closer.Close()

	assert.Equal(t, edgeVal, data)
}

func TestExportImportPortable_SkipsDerivedData(t *testing.T) {
	srcDB := setupTestDB(t)
	defer cleanupTestDB(t, srcDB)

	ctx := context.Background()

	// Write a document
	docKey := []byte("skip-doc")
	pebbleDocKey := storeutils.KeyRangeStart(docKey)
	require.NoError(t, srcDB.pdb.Set(pebbleDocKey, []byte(`{"id":"skip-doc"}`), pebble.Sync))

	// Write a summary (should be skipped)
	summaryKey := storeutils.MakeSummaryKey(docKey, "my_summary")
	require.NoError(t, srcDB.pdb.Set(summaryKey, []byte("some summary text"), pebble.Sync))

	// Export
	var buf bytes.Buffer
	err := srcDB.ExportPortable(ctx, &buf)
	require.NoError(t, err)

	// Import
	dstDB := setupTestDB(t)
	defer cleanupTestDB(t, dstDB)

	err = dstDB.ImportPortable(ctx, &buf)
	require.NoError(t, err)

	// Document should exist
	_, closer, err := dstDB.pdb.Get(pebbleDocKey)
	require.NoError(t, err)
	closer.Close()

	// Summary should NOT exist (skipped — rebuilt by enrichment)
	restoredSummaryKey := storeutils.MakeSummaryKey(docKey, "my_summary")
	_, _, err = dstDB.pdb.Get(restoredSummaryKey)
	assert.ErrorIs(t, err, pebble.ErrNotFound)
}

func TestExportImportPortable_RoundTrip(t *testing.T) {
	srcDB := setupTestDB(t)
	defer cleanupTestDB(t, srcDB)

	ctx := context.Background()

	// Write a mix of data
	batch := srcDB.pdb.NewBatch()

	// Documents
	for i := 0; i < 100; i++ {
		key := []byte("doc-" + string(rune('a'+i%26)) + string(rune('0'+i/26)))
		pebbleKey := storeutils.KeyRangeStart(key)
		val := []byte(`{"id":"` + string(key) + `","n":` + string(rune('0'+i%10)) + `}`)
		require.NoError(t, batch.Set(pebbleKey, val, nil))
	}
	require.NoError(t, batch.Commit(pebble.Sync))

	// Export
	var buf bytes.Buffer
	err := srcDB.ExportPortable(ctx, &buf)
	require.NoError(t, err)

	// Import
	dstDB := setupTestDB(t)
	defer cleanupTestDB(t, dstDB)

	err = dstDB.ImportPortable(ctx, bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	// Verify all keys exist with correct values by scanning both DBs
	srcIter, err := srcDB.pdb.NewIter(&pebble.IterOptions{})
	require.NoError(t, err)
	defer srcIter.Close()

	dstIter, err := dstDB.pdb.NewIter(&pebble.IterOptions{})
	require.NoError(t, err)
	defer dstIter.Close()

	srcCount := 0
	for srcIter.First(); srcIter.Valid(); srcIter.Next() {
		key := srcIter.Key()
		// Skip metadata keys
		if bytes.HasPrefix(key, storeutils.MetadataPrefix) {
			continue
		}
		srcCount++

		dstVal, closer, err := dstDB.pdb.Get(key)
		require.NoError(t, err, "key %q should exist in destination", key)
		assert.Equal(t, srcIter.Value(), dstVal, "value mismatch for key %q", key)
		closer.Close()
	}
	require.NoError(t, srcIter.Error())
	assert.Greater(t, srcCount, 0, "should have exported at least one key")
}

func TestExportPortable_EmptyDB(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	var buf bytes.Buffer
	err := db.ExportPortable(context.Background(), &buf)
	require.NoError(t, err)

	// Should still produce a valid AFB file
	assert.True(t, common.IsAFBFormat(buf.Bytes()))
	assert.Greater(t, buf.Len(), common.AFBHeaderSize)
}

func TestKeyClassification(t *testing.T) {
	t.Run("embedding key", func(t *testing.T) {
		key := storeutils.MakeEmbeddingKey([]byte("doc1"), "idx1")
		assert.True(t, isEmbeddingKey(key))
		assert.False(t, isSparseKey(key))
		assert.False(t, isOutgoingEdgeKey(key))
	})

	t.Run("sparse key", func(t *testing.T) {
		key := storeutils.MakeSparseKey([]byte("doc1"), "idx1")
		assert.True(t, isSparseKey(key))
		assert.False(t, isEmbeddingKey(key))
	})

	t.Run("outgoing edge key", func(t *testing.T) {
		key := storeutils.MakeEdgeKey([]byte("src"), []byte("tgt"), "idx", "type1")
		assert.True(t, isOutgoingEdgeKey(key))
		assert.False(t, isIncomingEdgeKey(key))
	})

	t.Run("document key", func(t *testing.T) {
		key := storeutils.KeyRangeStart([]byte("doc1"))
		assert.True(t, bytes.HasSuffix(key, storeutils.DBRangeStart))
		assert.False(t, isEmbeddingKey(key))
		assert.False(t, isSparseKey(key))
	})
}

func TestParseEnrichmentKey(t *testing.T) {
	t.Run("embedding key", func(t *testing.T) {
		key := storeutils.MakeEmbeddingKey([]byte("my-doc"), "my-index")
		userKey, indexName := parseEnrichmentKey(key, storeutils.EmbeddingSuffix)
		assert.Equal(t, []byte("my-doc"), userKey)
		assert.Equal(t, "my-index", indexName)
	})

	t.Run("sparse key", func(t *testing.T) {
		key := storeutils.MakeSparseKey([]byte("another-doc"), "sparse-idx")
		userKey, indexName := parseEnrichmentKey(key, storeutils.SparseSuffix)
		assert.Equal(t, []byte("another-doc"), userKey)
		assert.Equal(t, "sparse-idx", indexName)
	})

	t.Run("invalid key returns nil", func(t *testing.T) {
		userKey, indexName := parseEnrichmentKey([]byte("no-enrichment-here"), storeutils.EmbeddingSuffix)
		assert.Nil(t, userKey)
		assert.Empty(t, indexName)
	})
}

func TestSparseVecRoundTrip(t *testing.T) {
	indices := []uint32{1, 5, 100, 999}
	values := []float32{0.1, 0.5, 0.9, 1.0}

	encoded := encodeSparseVecRaw(nil, indices, values)
	decodedIndices, decodedValues, err := decodeSparseVecRaw(encoded)
	require.NoError(t, err)
	assert.Equal(t, indices, decodedIndices)
	assert.InDeltaSlice(t, values, decodedValues, 1e-7)
}
