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
	"context"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestComputeDocumentHash tests the deterministic hashing of documents
func TestComputeDocumentHash(t *testing.T) {
	doc1 := map[string]any{
		"id":   "123",
		"name": "test",
		"age":  30,
	}

	// Same document should produce same hash
	hash1, err := ComputeDocumentHash(doc1)
	require.NoError(t, err)
	hash2, err := ComputeDocumentHash(doc1)
	require.NoError(t, err)
	assert.Equal(t, hash1, hash2, "same document should produce same hash")

	// Different key order should produce same hash (canonical JSON)
	doc2 := map[string]any{
		"age":  30,
		"name": "test",
		"id":   "123",
	}
	hash3, err := ComputeDocumentHash(doc2)
	require.NoError(t, err)
	assert.Equal(t, hash1, hash3, "key order should not affect hash")

	// Different content should produce different hash
	doc3 := map[string]any{
		"id":   "123",
		"name": "test",
		"age":  31, // Changed age
	}
	hash4, err := ComputeDocumentHash(doc3)
	require.NoError(t, err)
	assert.NotEqual(t, hash1, hash4, "different content should produce different hash")
}

// TestComputeDocumentHashFromCompressed tests hash computation from compressed values
func TestComputeDocumentHashFromCompressed(t *testing.T) {
	doc := map[string]any{
		"id":    "test-doc",
		"value": "test-value",
	}

	// Marshal to JSON
	jsonData, err := json.Marshal(doc)
	require.NoError(t, err)

	// Compress using zstd
	encoder, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	compressed := encoder.EncodeAll(jsonData, nil)

	// Compute hash from compressed
	hash1, err := computeDocumentHashFromCompressed(compressed)
	require.NoError(t, err)

	// Compute hash directly from document
	hash2, err := ComputeDocumentHash(doc)
	require.NoError(t, err)

	assert.Equal(t, hash1, hash2, "hash from compressed should match hash from document")
}

// TestScanEmptyRange tests scanning an empty range
func TestScanEmptyRange(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	ctx := t.Context()
	scanResult, err := db.Scan(ctx, []byte("aaa"), []byte("bbb"), ScanOptions{})
	require.NoError(t, err)
	assert.Empty(t, scanResult.Hashes, "empty range should return empty map")
}

// TestScanSingleDocument tests scanning a range with a single document
func TestScanSingleDocument(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	ctx := t.Context()

	// Insert a single document
	doc := map[string]any{"id": "doc1", "value": "test"}
	err := db.Batch(t.Context(), [][2][]byte{
		{[]byte("doc1"), mustMarshal(t, doc)},
	}, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Scan range that includes this document
	scanResult, err := db.Scan(ctx, []byte("doc0"), []byte("doc2"), ScanOptions{})
	require.NoError(t, err)
	assert.Len(t, scanResult.Hashes, 1, "should find one document")
	assert.Contains(t, scanResult.Hashes, "doc1", "should contain doc1")
	assert.NotZero(t, scanResult.Hashes["doc1"], "hash should not be zero")
}

// TestScanMultipleDocuments tests scanning a range with multiple documents
func TestScanMultipleDocuments(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	ctx := t.Context()

	// Insert multiple documents
	writes := [][2][]byte{
		{[]byte("doc1"), mustMarshal(t, map[string]any{"id": "doc1", "value": "test1"})},
		{[]byte("doc2"), mustMarshal(t, map[string]any{"id": "doc2", "value": "test2"})},
		{[]byte("doc3"), mustMarshal(t, map[string]any{"id": "doc3", "value": "test3"})},
		{[]byte("doc4"), mustMarshal(t, map[string]any{"id": "doc4", "value": "test4"})},
	}
	err := db.Batch(t.Context(), writes, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Scan all documents
	scanResult, err := db.Scan(ctx, []byte("doc0"), []byte("doc5"), ScanOptions{})
	require.NoError(t, err)
	assert.Len(t, scanResult.Hashes, 4, "should find all documents")

	// Scan partial range
	scanResult, err = db.Scan(ctx, []byte("doc1"), []byte("doc3"), ScanOptions{})
	require.NoError(t, err)
	assert.Len(t, scanResult.Hashes, 2, "should find doc2 and doc3")
	assert.Contains(t, scanResult.Hashes, "doc2")
	assert.Contains(t, scanResult.Hashes, "doc3")
	assert.NotContains(t, scanResult.Hashes, "doc1", "should exclude lower bound")
	assert.NotContains(t, scanResult.Hashes, "doc4")
}

// TestScanSkipsMetadata tests that metadata keys are skipped
func TestScanSkipsMetadata(t *testing.T) {
	// defer goleak.VerifyNone(t,
	// 	goleak.IgnoreTopFunction("github.com/panjf2000/ants/v2.(*poolCommon).purgeStaleWorkers"),
	// 	goleak.IgnoreTopFunction("github.com/panjf2000/ants/v2.(*poolCommon).ticktock"),
	// 	goleak.IgnoreTopFunction("github.com/blevesearch/bleve_index_api.AnalysisWorker"),
	// 	goleak.IgnoreTopFunction("github.com/cockroachdb/pebble/v2/vfs.(*diskHealthCheckingFS).startTickerLocked.func1"),
	// 	goleak.IgnoreTopFunction("github.com/jellydator/ttlcache/v3.(*Cache[...]).Start"),
	// )

	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// Add 5 second timeout to fail fast instead of waiting 10 minutes
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Insert document and metadata
	doc := map[string]any{"id": "doc1", "value": "test"}
	err := db.Batch(ctx, [][2][]byte{
		{[]byte("doc1"), mustMarshal(t, doc)},
	}, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Scan should skip metadata keys
	scanResult, err := db.Scan(ctx, []byte("\x00\x00__meta__"), []byte("zzz"), ScanOptions{})
	require.NoError(t, err)
	// Should only find the document, not metadata
	assert.Len(t, scanResult.Hashes, 1)
	assert.Contains(t, scanResult.Hashes, "doc1")
}

// TestScanSkipsEnrichments tests that enrichment keys are skipped
func TestScanSkipsEnrichments(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	ctx := t.Context()

	// Insert document
	doc := map[string]any{"id": "doc1", "value": "test"}
	err := db.Batch(t.Context(), [][2][]byte{
		{[]byte("doc1"), mustMarshal(t, doc)},
	}, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Manually insert embedding suffix key (simulating enrichment)
	embKey := append(storeutils.KeyRangeStart([]byte("doc1")), []byte(":i:test_index:e")...)
	embValue := []byte("fake-embedding-data")
	err = db.pdb.Set(embKey, embValue, nil)
	require.NoError(t, err)

	// Manually insert summary suffix key
	sumKey := append(storeutils.KeyRangeStart([]byte("doc1")), []byte(":i:test_index:s")...)
	sumValue := []byte("fake-summary-data")
	err = db.pdb.Set(sumKey, sumValue, nil)
	require.NoError(t, err)

	// Scan should only return the main document
	scanResult, err := db.Scan(ctx, []byte("doc0"), []byte("doc2"), ScanOptions{})
	require.NoError(t, err)
	assert.Len(t, scanResult.Hashes, 1, "should only find main document, not enrichments")
	assert.Contains(t, scanResult.Hashes, "doc1")
}

// TestScanUserIDExtraction tests that user IDs are correctly extracted
func TestScanUserIDExtraction(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	ctx := t.Context()

	// Insert documents with various ID formats
	writes := [][2][]byte{
		{[]byte("simple"), mustMarshal(t, map[string]any{"id": "simple"})},
		{[]byte("with-dash"), mustMarshal(t, map[string]any{"id": "with-dash"})},
		{[]byte("with_underscore"), mustMarshal(t, map[string]any{"id": "with_underscore"})},
		{[]byte("rec_123"), mustMarshal(t, map[string]any{"id": "rec_123"})},
	}
	err := db.Batch(t.Context(), writes, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Scan all
	scanResult, err := db.Scan(ctx, []byte(""), []byte("zzz"), ScanOptions{})
	require.NoError(t, err)

	// Verify IDs match exactly (no :\x00 suffix)
	assert.Contains(t, scanResult.Hashes, "simple")
	assert.Contains(t, scanResult.Hashes, "with-dash")
	assert.Contains(t, scanResult.Hashes, "with_underscore")
	assert.Contains(t, scanResult.Hashes, "rec_123")
}

// TestScanHashDeterminism tests that the same document always produces the same hash
func TestScanHashDeterminism(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	ctx := t.Context()

	doc := map[string]any{
		"id":    "test",
		"value": "deterministic",
		"count": 42,
	}

	// Insert document
	err := db.Batch(t.Context(), [][2][]byte{
		{[]byte("test"), mustMarshal(t, doc)},
	}, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Scan multiple times
	result1, err := db.Scan(ctx, []byte(""), []byte("zzz"), ScanOptions{})
	require.NoError(t, err)
	result2, err := db.Scan(ctx, []byte(""), []byte("zzz"), ScanOptions{})
	require.NoError(t, err)

	assert.Equal(t, result1.Hashes["test"], result2.Hashes["test"], "hash should be deterministic")
}

// TestScanHashChangesWithContent tests that hash changes when content changes
func TestScanHashChangesWithContent(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	ctx := t.Context()

	// Insert initial document
	doc1 := map[string]any{"id": "test", "value": "initial"}
	err := db.Batch(t.Context(), [][2][]byte{
		{[]byte("test"), mustMarshal(t, doc1)},
	}, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Get initial hash
	result1, err := db.Scan(ctx, []byte(""), []byte("zzz"), ScanOptions{})
	require.NoError(t, err)
	hash1 := result1.Hashes["test"]

	// Update document
	doc2 := map[string]any{"id": "test", "value": "updated"}
	err = db.Batch(t.Context(), [][2][]byte{
		{[]byte("test"), mustMarshal(t, doc2)},
	}, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Get new hash
	result2, err := db.Scan(ctx, []byte(""), []byte("zzz"), ScanOptions{})
	require.NoError(t, err)
	hash2 := result2.Hashes["test"]

	assert.NotEqual(t, hash1, hash2, "hash should change when content changes")
}

// TestScanContextCancellation tests that scan respects context cancellation
func TestScanContextCancellation(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// Insert many documents
	writes := make([][2][]byte, 100)
	for i := range 100 {
		key := []byte(string(rune('a' + i)))
		doc := map[string]any{"id": i, "value": "test"}
		writes[i] = [2][]byte{key, mustMarshal(t, doc)}
	}
	err := db.Batch(t.Context(), writes, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Create context that cancels immediately
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	// Scan should return context error
	_, err = db.Scan(ctx, []byte(""), []byte("zzz"), ScanOptions{})
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestScanRangeBoundaries tests exclusive lower and inclusive upper bounds
func TestScanRangeBoundaries(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	ctx := t.Context()

	// Insert documents with predictable ordering
	writes := [][2][]byte{
		{[]byte("rec_001"), mustMarshal(t, map[string]any{"id": "rec_001"})},
		{[]byte("rec_002"), mustMarshal(t, map[string]any{"id": "rec_002"})},
		{[]byte("rec_003"), mustMarshal(t, map[string]any{"id": "rec_003"})},
		{[]byte("rec_004"), mustMarshal(t, map[string]any{"id": "rec_004"})},
		{[]byte("rec_005"), mustMarshal(t, map[string]any{"id": "rec_005"})},
	}
	err := db.Batch(t.Context(), writes, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Test (rec_002, rec_004] should return rec_003 and rec_004
	scanResult, err := db.Scan(ctx, []byte("rec_002"), []byte("rec_004"), ScanOptions{})
	require.NoError(t, err)
	assert.Len(t, scanResult.Hashes, 2)
	assert.Contains(t, scanResult.Hashes, "rec_003")
	assert.Contains(t, scanResult.Hashes, "rec_004")
	assert.NotContains(t, scanResult.Hashes, "rec_002", "should exclude lower bound")
	assert.NotContains(t, scanResult.Hashes, "rec_001")
	assert.NotContains(t, scanResult.Hashes, "rec_005")
}

// Helper functions

func setupTestDB(t *testing.T) *DBImpl {
	t.Helper()

	dir := t.TempDir()

	db := &DBImpl{
		logger:       zap.NewNop(),
		antflyConfig: &common.Config{},
		indexes:      make(map[string]indexes.IndexConfig),
		byteRange:    types.Range{[]byte(""), []byte("")},
	}

	schema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
				},
			},
		},
	}

	err := db.Open(dir, false, schema, types.Range{[]byte(""), []byte("")})
	require.NoError(t, err)

	return db
}

func cleanupTestDB(t *testing.T, db *DBImpl) {
	t.Helper()
	err := db.Close()
	require.NoError(t, err)
}

func mustMarshal(t *testing.T, doc map[string]any) []byte {
	t.Helper()
	data, err := json.Marshal(doc)
	require.NoError(t, err)
	return data
}

// TestScanWithTimeout tests scan behavior with timeout context
func TestScanWithTimeout(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// Insert document
	doc := map[string]any{"id": "test", "value": "test"}
	err := db.Batch(t.Context(), [][2][]byte{
		{[]byte("test"), mustMarshal(t, doc)},
	}, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Scan with generous timeout should succeed
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	scanResult, err := db.Scan(ctx, []byte(""), []byte("zzz"), ScanOptions{})
	require.NoError(t, err)
	assert.Len(t, scanResult.Hashes, 1)
}

// TestScanCorruptedDocument tests handling of corrupted compressed data
func TestScanCorruptedDocument(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	ctx := t.Context()

	// Insert valid document first
	doc := map[string]any{"id": "valid", "value": "test"}
	err := db.Batch(t.Context(), [][2][]byte{
		{[]byte("valid"), mustMarshal(t, doc)},
	}, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Manually insert corrupted data (not properly compressed)
	corruptedKey := storeutils.KeyRangeStart([]byte("corrupted"))
	corruptedValue := []byte("this-is-not-compressed-data")
	err = db.pdb.Set(corruptedKey, corruptedValue, nil)
	require.NoError(t, err)

	// Scan should continue despite corruption, using hash 0 for corrupted doc
	scanResult, err := db.Scan(ctx, []byte(""), []byte("zzz"), ScanOptions{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(scanResult.Hashes), 1, "should at least find valid doc")

	// Corrupted document should have hash 0
	if hash, ok := scanResult.Hashes["corrupted"]; ok {
		assert.Equal(t, uint64(0), hash, "corrupted document should have hash 0")
	}
}

// TestScanEmptyDocument tests scanning documents with empty content
func TestScanEmptyDocument(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	ctx := t.Context()

	// Insert empty document
	doc := map[string]any{}
	err := db.Batch(t.Context(), [][2][]byte{
		{[]byte("empty"), mustMarshal(t, doc)},
	}, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	scanResult, err := db.Scan(ctx, []byte(""), []byte("zzz"), ScanOptions{})
	require.NoError(t, err)
	assert.Len(t, scanResult.Hashes, 1)
	assert.Contains(t, scanResult.Hashes, "empty")
	assert.NotZero(t, scanResult.Hashes["empty"], "even empty doc should have non-zero hash")
}

// TestScanLargeDocument tests scanning large documents
func TestScanLargeDocument(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	ctx := t.Context()

	// Create a large document
	largeArray := make([]string, 1000)
	for i := range largeArray {
		largeArray[i] = "item-" + string(rune(i))
	}
	doc := map[string]any{
		"id":    "large",
		"items": largeArray,
	}

	err := db.Batch(t.Context(), [][2][]byte{
		{[]byte("large"), mustMarshal(t, doc)},
	}, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	scanResult, err := db.Scan(ctx, []byte(""), []byte("zzz"), ScanOptions{})
	require.NoError(t, err)
	assert.Len(t, scanResult.Hashes, 1)
	assert.Contains(t, scanResult.Hashes, "large")
	assert.NotZero(t, scanResult.Hashes["large"])
}

// TestScanNestedDocument tests scanning documents with nested structures
func TestScanNestedDocument(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	ctx := t.Context()

	doc := map[string]any{
		"id": "nested",
		"user": map[string]any{
			"name": "John",
			"address": map[string]any{
				"city":  "NYC",
				"state": "NY",
			},
		},
	}

	err := db.Batch(t.Context(), [][2][]byte{
		{[]byte("nested"), mustMarshal(t, doc)},
	}, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	scanResult, err := db.Scan(ctx, []byte(""), []byte("zzz"), ScanOptions{})
	require.NoError(t, err)
	assert.Len(t, scanResult.Hashes, 1)
	assert.NotZero(t, scanResult.Hashes["nested"])

	// Changing nested value should change hash
	doc["user"].(map[string]any)["name"] = "Jane"
	err = db.Batch(t.Context(), [][2][]byte{
		{[]byte("nested"), mustMarshal(t, doc)},
	}, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	result2, err := db.Scan(ctx, []byte(""), []byte("zzz"), ScanOptions{})
	require.NoError(t, err)
	assert.NotEqual(
		t,
		scanResult.Hashes["nested"],
		result2.Hashes["nested"],
		"nested change should affect hash",
	)
}

// TestScanOptions tests the InclusiveFrom and ExclusiveTo options
func TestScanOptions(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	ctx := t.Context()

	// Insert test documents
	writes := [][2][]byte{
		{[]byte("rec_001"), mustMarshal(t, map[string]any{"id": "rec_001"})},
		{[]byte("rec_002"), mustMarshal(t, map[string]any{"id": "rec_002"})},
		{[]byte("rec_003"), mustMarshal(t, map[string]any{"id": "rec_003"})},
		{[]byte("rec_004"), mustMarshal(t, map[string]any{"id": "rec_004"})},
		{[]byte("rec_005"), mustMarshal(t, map[string]any{"id": "rec_005"})},
	}
	err := db.Batch(t.Context(), writes, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Test 1: Default (exclusive from, inclusive to) - (rec_002, rec_004]
	scanResult, err := db.Scan(ctx, []byte("rec_002"), []byte("rec_004"), ScanOptions{})
	require.NoError(t, err)
	assert.Len(t, scanResult.Hashes, 2)
	assert.Contains(t, scanResult.Hashes, "rec_003")
	assert.Contains(t, scanResult.Hashes, "rec_004")
	assert.NotContains(t, scanResult.Hashes, "rec_002")

	// Test 2: Inclusive from - [rec_002, rec_004]
	scanResult, err = db.Scan(
		ctx,
		[]byte("rec_002"),
		[]byte("rec_004"),
		ScanOptions{InclusiveFrom: true},
	)
	require.NoError(t, err)
	assert.Len(t, scanResult.Hashes, 3)
	assert.Contains(t, scanResult.Hashes, "rec_002")
	assert.Contains(t, scanResult.Hashes, "rec_003")
	assert.Contains(t, scanResult.Hashes, "rec_004")

	// Test 3: Exclusive to - (rec_002, rec_004)
	scanResult, err = db.Scan(
		ctx,
		[]byte("rec_002"),
		[]byte("rec_004"),
		ScanOptions{ExclusiveTo: true},
	)
	require.NoError(t, err)
	assert.Len(t, scanResult.Hashes, 1)
	assert.Contains(t, scanResult.Hashes, "rec_003")
	assert.NotContains(t, scanResult.Hashes, "rec_002")
	assert.NotContains(t, scanResult.Hashes, "rec_004")

	// Test 4: Both inclusive - [rec_002, rec_004]
	scanResult, err = db.Scan(ctx, []byte("rec_002"), []byte("rec_004"), ScanOptions{
		InclusiveFrom: true,
		ExclusiveTo:   false,
	})
	require.NoError(t, err)
	assert.Len(t, scanResult.Hashes, 3)
	assert.Contains(t, scanResult.Hashes, "rec_002")
	assert.Contains(t, scanResult.Hashes, "rec_003")
	assert.Contains(t, scanResult.Hashes, "rec_004")

	// Test 5: Both exclusive - (rec_002, rec_004)
	scanResult, err = db.Scan(ctx, []byte("rec_002"), []byte("rec_004"), ScanOptions{
		InclusiveFrom: false,
		ExclusiveTo:   true,
	})
	require.NoError(t, err)
	assert.Len(t, scanResult.Hashes, 1)
	assert.Contains(t, scanResult.Hashes, "rec_003")
	assert.NotContains(t, scanResult.Hashes, "rec_002")
	assert.NotContains(t, scanResult.Hashes, "rec_004")
}

// TestScanIncludeDocuments tests the IncludeDocuments option
func TestScanIncludeDocuments(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	ctx := context.Background()

	// Insert test documents with known content
	writes := [][2][]byte{
		{[]byte("doc1"), mustMarshal(t, map[string]any{"id": "doc1", "name": "Alice", "age": 30})},
		{[]byte("doc2"), mustMarshal(t, map[string]any{"id": "doc2", "name": "Bob", "age": 25})},
		{
			[]byte("doc3"),
			mustMarshal(t, map[string]any{"id": "doc3", "name": "Charlie", "age": 35}),
		},
	}
	err := db.Batch(t.Context(), writes, nil, Op_SyncLevelWrite)
	require.NoError(t, err)

	// Test 1: Default (IncludeDocuments: false) - should only return hashes
	scanResult, err := db.Scan(ctx, []byte(""), []byte("zzz"), ScanOptions{})
	require.NoError(t, err)
	assert.Len(t, scanResult.Hashes, 3, "should have 3 hashes")
	assert.Nil(t, scanResult.Documents, "documents should be nil when not requested")

	// Test 2: IncludeDocuments: true - should return both hashes and documents
	scanResult, err = db.Scan(ctx, []byte(""), []byte("zzz"), ScanOptions{IncludeDocuments: true})
	require.NoError(t, err)
	assert.Len(t, scanResult.Hashes, 3, "should have 3 hashes")
	assert.Len(t, scanResult.Documents, 3, "should have 3 documents")

	// Verify document content
	assert.Contains(t, scanResult.Documents, "doc1")
	assert.Contains(t, scanResult.Documents, "doc2")
	assert.Contains(t, scanResult.Documents, "doc3")

	// Verify document fields
	assert.Equal(t, "Alice", scanResult.Documents["doc1"]["name"])
	assert.Equal(t, float64(30), scanResult.Documents["doc1"]["age"])
	assert.Equal(t, "Bob", scanResult.Documents["doc2"]["name"])
	assert.Equal(t, float64(25), scanResult.Documents["doc2"]["age"])
	assert.Equal(t, "Charlie", scanResult.Documents["doc3"]["name"])
	assert.Equal(t, float64(35), scanResult.Documents["doc3"]["age"])

	// Test 3: Verify hashes match document content
	for id, doc := range scanResult.Documents {
		// Compute expected hash
		expectedHash, err := ComputeDocumentHash(doc)
		require.NoError(t, err)
		assert.Equal(t, expectedHash, scanResult.Hashes[id], "hash should match document content")
	}

	// Test 4: IncludeDocuments with range filtering
	scanResult, err = db.Scan(
		ctx,
		[]byte("doc1"),
		[]byte("doc2"),
		ScanOptions{IncludeDocuments: true},
	)
	require.NoError(t, err)
	assert.Len(t, scanResult.Hashes, 1, "should only have doc2")
	assert.Len(t, scanResult.Documents, 1, "should only have doc2 document")
	assert.Contains(t, scanResult.Documents, "doc2")
	assert.Equal(t, "Bob", scanResult.Documents["doc2"]["name"])
}
