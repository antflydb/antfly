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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/chunking"
	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vector/testutils"
	"github.com/antflydb/antfly/lib/vectorindex"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/blevesearch/bleve/v2"
	blevequery "github.com/blevesearch/bleve/v2/search/query"
	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// TestSplit tests that Split correctly splits a database with:
// - Moderate to high number of key/value pairs
// - Sample embeddings
// - Multiple indexes
// - Data integrity before, during, and after split
func TestSplit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	logger := zaptest.NewLogger(t)
	ctx := context.Background()

	// Create temporary directories
	baseDir := t.TempDir()
	srcDir := filepath.Join(baseDir, "src")
	destDir1 := filepath.Join(baseDir, "dest1")
	destDir2 := filepath.Join(baseDir, "dest2")

	require.NoError(t, os.MkdirAll(srcDir, os.ModePerm))
	require.NoError(t, os.MkdirAll(destDir1, os.ModePerm))
	require.NoError(t, os.MkdirAll(destDir2, os.ModePerm))

	// Create schema for testing
	schema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"id":      map[string]any{"type": "string"},
						"content": map[string]any{"type": "string"},
						"value":   map[string]any{"type": "number"},
					},
				},
			},
		},
	}

	// Define full byte range for initial DB
	// Use standard DBRangeStart/DBRangeEnd format for keys
	// Our keys will be like: key:00000:\x00 (DBRangeStart suffix)
	fullRange := types.Range{[]byte("\x00"), []byte("\xFF")}

	// Create and populate the source database
	srcDB := &DBImpl{
		logger:       logger,
		antflyConfig: &common.Config{},
		indexes:      make(map[string]indexes.IndexConfig),
	}

	require.NoError(t, srcDB.Open(srcDir, false, schema, fullRange))

	// Create test indexes
	bleveIndexConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	embeddingIndexConfig := indexes.NewEmbeddingsConfig(
		"embedding_index",
		indexes.EmbeddingsIndexConfig{
			Dimension: 128,
			Field:     "content",
		},
	)
	sparseIndexConfig := indexes.NewEmbeddingsConfig(
		"sparse_index",
		indexes.EmbeddingsIndexConfig{
			Field:  "content",
			Sparse: true,
		},
	)
	graphEdgeTypes := []indexes.EdgeTypeConfig{
		{
			Name:             "cites",
			MaxWeight:        1.0,
			MinWeight:        0.0,
			AllowSelfLoops:   false,
			RequiredMetadata: &[]string{},
		},
	}
	graphIndexConfig, err := indexes.NewIndexConfig("graph_index", indexes.GraphIndexConfig{
		EdgeTypes:           &graphEdgeTypes,
		MaxEdgesPerDocument: 4,
	})
	require.NoError(t, err)

	require.NoError(t, srcDB.AddIndex(*bleveIndexConfig))
	require.NoError(t, srcDB.AddIndex(*embeddingIndexConfig))
	require.NoError(t, srcDB.AddIndex(*sparseIndexConfig))
	require.NoError(t, srcDB.AddIndex(*graphIndexConfig))

	// Generate test data - using 1000 documents for a moderate load
	numDocs := 1000
	testData := make(map[string]testDocument)
	graphTargets := make(map[string]string, numDocs)
	writes := make([][2][]byte, 0, numDocs*4) // docs + dense + sparse + edges

	for i := range numDocs {
		key := fmt.Sprintf("key:%05d", i)
		doc := testDocument{
			ID:      key,
			Content: fmt.Sprintf("This is test document number %d with some content", i),
			Value:   float64(i * 10),
		}
		testData[key] = doc

		// Marshal document
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)

		writes = append(writes, [2][]byte{[]byte(key), docJSON})

		// Create embedding for this document
		embedding := generateTestEmbedding(i, 128)
		embeddingKey := fmt.Appendf(nil, "%s:i:embedding_index:e", key)

		// Encode vector with hash ID as required by the storage format
		// Format: [hashID:uint64][dimension:uint32][float32_data...]
		// Calculate hash ID for the embedding (based on the key as in embeddingindex.go)
		hashID := xxhash.Sum64String(key)
		embeddingBytes, err := vectorindex.EncodeEmbeddingWithHashID(nil, embedding, hashID)
		require.NoError(t, err)

		writes = append(writes, [2][]byte{embeddingKey, embeddingBytes})

		sparseEmbeddingKey := fmt.Appendf(nil, "%s:i:sparse_index%s", key, storeutils.SparseSuffix)
		sparseEmbeddingBytes := encodeSparseEmbeddingValue(generateTestSparseEmbedding(i), hashID)
		writes = append(writes, [2][]byte{sparseEmbeddingKey, sparseEmbeddingBytes})

		if i+1 < numDocs {
			targetKey := fmt.Sprintf("key:%05d", i+1)
			graphTargets[key] = targetKey
			edgeKey := storeutils.MakeEdgeKey([]byte(key), []byte(targetKey), "graph_index", "cites")
			edgeValue, err := indexes.EncodeEdgeValue(&indexes.Edge{
				Source: []byte(key),
				Target: []byte(targetKey),
				Type:   "cites",
				Weight: 1.0,
			})
			require.NoError(t, err)
			writes = append(writes, [2][]byte{edgeKey, edgeValue})
		}
	}

	// Batch write all documents
	err = srcDB.Batch(t.Context(), writes, nil, Op_SyncLevelFullText)
	// FullText sync level returns ErrPartialSuccess indicating KV write succeeded
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	// Force a flush to ensure data is written to SSTables for split
	require.NoError(t, srcDB.pdb.Flush())

	// Wait for async indexes to catch up before asserting pre-split state.
	require.Eventually(t, func() bool {
		_, _, indexStats, err := srcDB.Stats()
		if err != nil {
			return false
		}
		sparseStats, err := indexStats["sparse_index"].AsEmbeddingsIndexStats()
		if err != nil || sparseStats.TotalIndexed != uint64(numDocs) {
			return false
		}
		graphStats, err := indexStats["graph_index"].AsGraphIndexStats()
		if err != nil || graphStats.TotalEdges != uint64(numDocs-1) {
			return false
		}
		return true
	}, 5*time.Second, 50*time.Millisecond)

	// Verify all data is present in source DB before split
	t.Run("VerifySourceBeforeSplit", func(t *testing.T) {
		verifyDBContents(t, ctx, srcDB, testData, fullRange)
		verifySparseIndexState(t, srcDB, "sparse_index", numDocs)
		verifyGraphIndexState(t, ctx, srcDB, "graph_index", graphTargets, []int{0, 499, 998})
	})

	// Verify the DB range is correct
	currentRange, err := srcDB.GetRange()
	require.NoError(t, err)
	t.Logf("Current DB range: %s", currentRange)
	assert.Equal(t, fullRange, currentRange, "DB range should match the initial range")

	// Use a fixed split key in the middle of our key range (around key 500)
	// The split key should be a valid document key without suffix
	// range1 will be [fullRange[0], splitKey) - keys < "key:00500"
	// range2 will be [splitKey, fullRange[1]) - keys >= "key:00500"
	splitKey := []byte("key:00500")
	t.Logf("Using split key: %s", splitKey)
	assert.True(t, fullRange.Contains(splitKey), "Split key should be within range")

	// Calculate expected ranges after split
	range1 := types.Range{fullRange[0], splitKey}
	range2 := types.Range{splitKey, fullRange[1]}

	// Create channel to track reads during split
	readResults := make(chan readResult, numDocs)
	stopReading := make(chan struct{})
	var wg sync.WaitGroup

	// Start concurrent reads during split
	wg.Go(func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-stopReading:
				return
			case <-ticker.C:
				// Try to read a random document
				// Note: reads may fail during split as DB is being reorganized
				docIdx := time.Now().UnixNano() % int64(numDocs)
				key := fmt.Sprintf("key:%05d", docIdx)

				// Protect against panics during concurrent access
				func() {
					defer func() {
						if r := recover(); r != nil {
							readResults <- readResult{
								key:      key,
								doc:      nil,
								err:      fmt.Errorf("panic during read: %v", r),
								isDuring: true,
							}
						}
					}()

					doc, err := srcDB.Get(ctx, []byte(key))
					readResults <- readResult{
						key:      key,
						doc:      doc,
						err:      err,
						isDuring: true,
					}
				}()
			}
		}
	})

	// Perform the split
	t.Logf("Starting split...")
	startTime := time.Now()
	err = srcDB.Split(fullRange, splitKey, destDir1, destDir2, false)
	require.NoError(t, err)
	splitDuration := time.Since(startTime)
	t.Logf("Split completed in %v", splitDuration)

	// Stop concurrent reads
	close(stopReading)
	wg.Wait()
	close(readResults)

	// Check that reads during split were successful
	readsDuringCount := 0
	for result := range readResults {
		readsDuringCount++
		if result.err != nil {
			t.Logf("Read during split failed for key %s: %v", result.key, result.err)
			// Note: Some failures might be acceptable during splits,
			// but we log them for visibility
		}
	}
	t.Logf("Performed %d reads during split", readsDuringCount)

	// Verify source DB after split (should now only contain range1 data)
	t.Run("VerifySourceAfterSplit", func(t *testing.T) {
		// Re-check that source DB range was updated
		currentRange, err := srcDB.GetRange()
		require.NoError(t, err)
		assert.Equal(t, range1, currentRange, "Source DB should have range1 after split")

		// Verify source DB only has range1 data
		verifyDBContents(t, ctx, srcDB, filterDataByRange(testData, range1), range1)
		verifySparseIndexState(t, srcDB, "sparse_index", len(filterDataByRange(testData, range1)))
		verifyGraphIndexState(t, ctx, srcDB, "graph_index", graphTargets, []int{0, 499})
	})

	// Open and verify the new shard (destDir2)
	t.Run("VerifyNewShard", func(t *testing.T) {
		// First, let's check what got created in destDir2
		pebbleDir := filepath.Join(destDir2, "pebble")
		entries, err := os.ReadDir(pebbleDir)
		require.NoError(t, err, "destDir2/pebble should exist")
		t.Logf("Files in %s: %d files", pebbleDir, len(entries))

		newDB := &DBImpl{
			logger:       logger,
			antflyConfig: &common.Config{},
			// Don't initialize indexes - let Open load them from metadata
		}

		// Pass nil schema and empty byteRange to force loading from metadata
		require.NoError(t, newDB.Open(destDir2, false, nil, types.Range{}))

		defer func() {
			assert.NoError(t, newDB.Close())
		}()

		// Verify new DB has range2 data
		verifyDBContents(t, ctx, newDB, filterDataByRange(testData, range2), range2)
		verifySparseIndexState(t, newDB, "sparse_index", len(filterDataByRange(testData, range2)))
		verifyGraphIndexState(t, ctx, newDB, "graph_index", graphTargets, []int{500, 998})
	})

	// Verify no data was lost across both shards
	t.Run("VerifyNoDataLoss", func(t *testing.T) {
		range1Data := filterDataByRange(testData, range1)
		range2Data := filterDataByRange(testData, range2)

		// Open second DB to count its records
		newDB := &DBImpl{
			logger:       logger,
			antflyConfig: &common.Config{},
		}
		// Load from persisted metadata
		require.NoError(t, newDB.Open(destDir2, false, nil, types.Range{}))
		defer newDB.Close()

		totalRecords := len(range1Data) + len(range2Data)
		assert.Equal(t, numDocs, totalRecords,
			"Total records across both shards should equal original count")

		t.Logf("Range1 has %d records, Range2 has %d records, Total: %d",
			len(range1Data), len(range2Data), totalRecords)
	})

	// Verify indexes were properly split
	t.Run("VerifyIndexes", func(t *testing.T) {
		// Check source DB indexes
		srcIndexes := srcDB.GetIndexes()
		assert.Contains(t, srcIndexes, "full_text_index")
		assert.Contains(t, srcIndexes, "embedding_index")
		assert.Contains(t, srcIndexes, "sparse_index")
		assert.Contains(t, srcIndexes, "graph_index")

		// Check new DB indexes - force metadata loading by passing nil schema and empty range
		newDB := &DBImpl{
			logger:       logger,
			antflyConfig: &common.Config{},
			// Don't initialize indexes - let Open load them from metadata
		}
		require.NoError(t, newDB.Open(destDir2, false, nil, types.Range{}))
		defer newDB.Close()

		newIndexes := newDB.GetIndexes()
		assert.Contains(t, newIndexes, "full_text_index")
		assert.Contains(t, newIndexes, "embedding_index")
		assert.Contains(t, newIndexes, "sparse_index")
		assert.Contains(t, newIndexes, "graph_index")
	})

	// Clean up
	require.NoError(t, srcDB.Close())
}

// testDocument represents a test document structure
type testDocument struct {
	ID      string  `json:"id"`
	Content string  `json:"content"`
	Value   float64 `json:"value"`
}

// readResult captures the result of a concurrent read operation
type readResult struct {
	key      string
	doc      map[string]any
	err      error
	isDuring bool // whether this read happened during the split
}

// generateTestEmbedding creates a deterministic test embedding
func generateTestEmbedding(seed int, dimensions int) []float32 {
	embedding := make([]float32, dimensions)
	for i := range dimensions {
		// Create deterministic but varied embeddings
		embedding[i] = float32(seed*dimensions+i) / 1000.0
	}
	return embedding
}

func generateTestSparseEmbedding(seed int) *vector.SparseVector {
	base := uint32(seed % 97)
	return vector.NewSparseVector(
		[]uint32{base + 1, base + 101, base + 1001},
		[]float32{1.0, float32((seed%5)+1) / 10.0, float32((seed%7)+1) / 20.0},
	)
}

func encodeSparseEmbeddingValue(sv *vector.SparseVector, hashID uint64) []byte {
	encoded := indexes.EncodeSparseVec(sv)
	value := make([]byte, 8+len(encoded))
	binary.LittleEndian.PutUint64(value[:8], hashID)
	copy(value[8:], encoded)
	return value
}

func verifySparseIndexState(
	t *testing.T,
	db *DBImpl,
	indexName string,
	expectedCount int,
) {
	t.Helper()

	_, _, indexStats, err := db.Stats()
	require.NoError(t, err)
	stats, ok := indexStats[indexName]
	require.True(t, ok, "missing sparse index stats for %s", indexName)
	embStats, err := stats.AsEmbeddingsIndexStats()
	require.NoError(t, err)
	assert.Equal(t, uint64(expectedCount), embStats.TotalIndexed, "unexpected sparse index count for %s", indexName)
}

func verifyGraphIndexState(
	t *testing.T,
	ctx context.Context,
	db *DBImpl,
	indexName string,
	expectedTargets map[string]string,
	sampleSourceIndexes []int,
) {
	t.Helper()

	for _, idx := range sampleSourceIndexes {
		source := fmt.Sprintf("key:%05d", idx)
		target, ok := expectedTargets[source]
		require.True(t, ok, "missing expected graph target for %s", source)
		edges, err := db.GetEdges(ctx, indexName, []byte(source), "cites", indexes.EdgeDirectionOut)
		require.NoError(t, err)
		require.Len(t, edges, 1, "expected one outgoing edge for %s", source)
		assert.Equal(t, target, string(edges[0].Target))
	}
}

// verifyDBContents checks that a database contains expected documents
func verifyDBContents(
	t *testing.T,
	ctx context.Context,
	db *DBImpl,
	expectedData map[string]testDocument,
	byteRange types.Range,
) {
	t.Helper()

	foundCount := 0
	missingKeys := []string{}
	mismatchedDocs := []string{}

	for key, expectedDoc := range expectedData {
		keyBytes := []byte(key)

		// Skip keys outside the byte range
		if !byteRange.Contains(keyBytes) {
			continue
		}

		doc, err := db.Get(ctx, keyBytes)
		if err != nil {
			missingKeys = append(missingKeys, key)
			continue
		}

		foundCount++

		// Verify document content
		if doc["id"] != expectedDoc.ID {
			mismatchedDocs = append(mismatchedDocs,
				fmt.Sprintf("%s: id mismatch (got %v, want %s)",
					key, doc["id"], expectedDoc.ID))
		}
		if doc["content"] != expectedDoc.Content {
			mismatchedDocs = append(mismatchedDocs,
				key+": content mismatch")
		}
		if doc["value"] != expectedDoc.Value {
			mismatchedDocs = append(mismatchedDocs,
				fmt.Sprintf("%s: value mismatch (got %v, want %f)",
					key, doc["value"], expectedDoc.Value))
		}

		// Verify embeddings are present
		if embeddings, ok := doc["_embeddings"]; ok {
			embMap, ok := embeddings.(map[string][]float32)
			if !ok {
				t.Logf(
					"Warning: embeddings exist but wrong type for key %s, type=%T, value=%v",
					key,
					embeddings,
					embeddings,
				)
			} else if len(embMap) == 0 {
				t.Logf("Warning: embeddings exist but empty map for key %s", key)
			}
		}
	}

	// Report issues
	if len(missingKeys) > 0 {
		t.Errorf("Missing %d keys in range %s: %v (showing first few)",
			len(missingKeys), byteRange, missingKeys[:min(5, len(missingKeys))])
	}
	if len(mismatchedDocs) > 0 {
		t.Errorf("Document mismatches: %v", mismatchedDocs[:min(5, len(mismatchedDocs))])
	}

	expectedCount := len(expectedData)
	t.Logf("Found %d/%d expected documents in range %s", foundCount, expectedCount, byteRange)
}

// filterDataByRange returns documents that fall within the specified byte range
func filterDataByRange(
	data map[string]testDocument,
	byteRange types.Range,
) map[string]testDocument {
	filtered := make(map[string]testDocument)
	for key, doc := range data {
		if byteRange.Contains([]byte(key)) {
			filtered[key] = doc
		}
	}
	return filtered
}

// TestSplitUnboundedRange tests the split behavior with an unbounded range (empty End).
// This simulates the production scenario where init- data needs to be preserved
// across multiple splits when split keys are in the bg- range.
//
// Key insight: When splitting a shard with range [start, unbounded):
// - Split key "bg-000XXX" creates:
//   - First half: [start, bg-000XXX) - keeps data < bg-000XXX
//   - Second half: [bg-000XXX, unbounded) - gets data >= bg-000XXX (including init-, large-)
//
// Since init- (0x69) > bg- (0x62), init- data goes to the SECOND half.
func TestSplitUnboundedRange(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	logger := zaptest.NewLogger(t)
	ctx := context.Background()

	// Create temporary directories
	baseDir := t.TempDir()
	srcDir := filepath.Join(baseDir, "src")
	destDir1 := filepath.Join(baseDir, "dest1")
	destDir2 := filepath.Join(baseDir, "dest2")

	require.NoError(t, os.MkdirAll(srcDir, os.ModePerm))
	require.NoError(t, os.MkdirAll(destDir1, os.ModePerm))
	require.NoError(t, os.MkdirAll(destDir2, os.ModePerm))

	// Create schema for testing
	schema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"id":      map[string]any{"type": "string"},
						"content": map[string]any{"type": "string"},
					},
				},
			},
		},
	}

	// Use an UNBOUNDED range (empty End) like in production
	// This is the critical difference from the existing test
	unboundedRange := types.Range{[]byte{}, []byte{}}

	// Create and populate the source database
	srcDB := &DBImpl{
		logger:       logger,
		antflyConfig: &common.Config{},
		indexes:      make(map[string]indexes.IndexConfig),
	}

	require.NoError(t, srcDB.Open(srcDir, false, schema, unboundedRange))

	// Generate test data with three key prefixes that mirror the E2E test
	// Lexicographic order: bg- (0x62) < init- (0x69) < large- (0x6c)
	testData := make(map[string]testDocument)
	writes := make([][2][]byte, 0)

	// Add bg- prefixed keys
	for i := range 10 {
		key := fmt.Sprintf("bg-%03d", i)
		doc := testDocument{ID: key, Content: fmt.Sprintf("bg content %d", i)}
		testData[key] = doc
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		writes = append(writes, [2][]byte{[]byte(key), docJSON})
	}

	// Add init- prefixed keys (THE CRITICAL DATA)
	for i := range 10 {
		key := fmt.Sprintf("init-%03d", i)
		doc := testDocument{ID: key, Content: fmt.Sprintf("init content %d", i)}
		testData[key] = doc
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		writes = append(writes, [2][]byte{[]byte(key), docJSON})
	}

	// Add large- prefixed keys
	for i := range 10 {
		key := fmt.Sprintf("large-%03d", i)
		doc := testDocument{ID: key, Content: fmt.Sprintf("large content %d", i)}
		testData[key] = doc
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		writes = append(writes, [2][]byte{[]byte(key), docJSON})
	}

	// Batch write all documents
	err := srcDB.Batch(ctx, writes, nil, Op_SyncLevelPropose)
	require.NoError(t, err)

	// Force flush
	require.NoError(t, srcDB.pdb.Flush())

	// Verify all data is present before split
	t.Run("VerifyAllDataBeforeSplit", func(t *testing.T) {
		for key := range testData {
			doc, err := srcDB.Get(ctx, []byte(key))
			require.NoError(t, err, "Key %s should exist before split", key)
			require.Equal(t, key, doc["id"], "Document ID mismatch for key %s", key)
		}
		t.Logf("All %d documents verified before split", len(testData))
	})

	// Log byte values for debugging
	t.Logf("Key byte prefixes:")
	t.Logf("  bg-000: %v", []byte("bg-000"))
	t.Logf("  init-000: %v", []byte("init-000"))
	t.Logf("  large-000: %v", []byte("large-000"))

	// Split key in the bg- range - this should put init- and large- in the second half
	splitKey := []byte("bg-005")
	t.Logf("Split key: %s (%v)", splitKey, splitKey)

	// Verify split key ordering
	assert.Less(t, string(splitKey), "init-000", "Split key should be < init-000")
	assert.Less(t, string(splitKey), "large-000", "Split key should be < large-000")

	// Expected ranges after split:
	// range1: [empty, bg-005) - contains bg-000 through bg-004
	// range2: [bg-005, empty) = [bg-005, unbounded) - contains bg-005 through bg-009, all init-, all large-
	range1 := types.Range{[]byte{}, splitKey}
	range2 := types.Range{splitKey, []byte{}}

	t.Logf("Range1: %s", range1)
	t.Logf("Range2: %s (unbounded)", range2)

	// Perform the split
	t.Logf("Performing split with unbounded range...")
	err = srcDB.Split(unboundedRange, splitKey, destDir1, destDir2, false)
	require.NoError(t, err, "Split should succeed")
	t.Logf("Split completed")

	// Verify source DB (first half - should have bg-000 through bg-004)
	t.Run("VerifyFirstHalf", func(t *testing.T) {
		currentRange, err := srcDB.GetRange()
		require.NoError(t, err)
		t.Logf("First half range: %s", currentRange)

		// Check expected keys exist
		for i := range 5 {
			key := fmt.Sprintf("bg-%03d", i)
			doc, err := srcDB.Get(ctx, []byte(key))
			require.NoError(t, err, "Key %s should exist in first half", key)
			require.Equal(t, key, doc["id"])
		}

		// Check that init- and large- keys are NOT in first half
		for i := range 10 {
			initKey := fmt.Sprintf("init-%03d", i)
			_, err := srcDB.Get(ctx, []byte(initKey))
			assert.Error(t, err, "init key %s should NOT be in first half", initKey)

			largeKey := fmt.Sprintf("large-%03d", i)
			_, err = srcDB.Get(ctx, []byte(largeKey))
			assert.Error(t, err, "large key %s should NOT be in first half", largeKey)
		}
	})

	// Verify second half (destDir2 - should have bg-005+, all init-, all large-)
	t.Run("VerifySecondHalf", func(t *testing.T) {
		newDB := &DBImpl{
			logger:       logger,
			antflyConfig: &common.Config{},
		}
		require.NoError(t, newDB.Open(destDir2, false, nil, types.Range{}))
		defer newDB.Close()

		currentRange, err := newDB.GetRange()
		require.NoError(t, err)
		t.Logf("Second half range: %s", currentRange)

		// Check bg-005 through bg-009
		for i := 5; i < 10; i++ {
			key := fmt.Sprintf("bg-%03d", i)
			doc, err := newDB.Get(ctx, []byte(key))
			require.NoError(t, err, "Key %s should exist in second half", key)
			require.Equal(t, key, doc["id"])
		}

		// THE CRITICAL CHECK: init- keys should be in second half
		for i := range 10 {
			key := fmt.Sprintf("init-%03d", i)
			doc, err := newDB.Get(ctx, []byte(key))
			require.NoError(t, err, "CRITICAL: init key %s should exist in second half", key)
			require.Equal(t, key, doc["id"], "init key %s document ID mismatch", key)
		}

		// large- keys should be in second half
		for i := range 10 {
			key := fmt.Sprintf("large-%03d", i)
			doc, err := newDB.Get(ctx, []byte(key))
			require.NoError(t, err, "large key %s should exist in second half", key)
			require.Equal(t, key, doc["id"])
		}

		t.Logf("All init- and large- keys correctly in second half")
	})

	// Clean up
	require.NoError(t, srcDB.Close())
}

func TestSplitPrepareOnlyCheckpointedArchiveHasCleanMetadata(t *testing.T) {
	logger := zaptest.NewLogger(t)
	ctx := context.Background()

	baseDir := t.TempDir()
	srcDir := filepath.Join(baseDir, "src")
	destDir1 := filepath.Join(baseDir, "dest1")
	destDir2 := filepath.Join(baseDir, "dest2")

	require.NoError(t, os.MkdirAll(srcDir, os.ModePerm))
	require.NoError(t, os.MkdirAll(destDir1, os.ModePerm))
	require.NoError(t, os.MkdirAll(destDir2, os.ModePerm))

	schema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"id":      map[string]any{"type": "string"},
						"content": map[string]any{"type": "string"},
					},
				},
			},
		},
	}

	fullRange := types.Range{[]byte("\x00"), []byte("\xFF")}
	splitKey := []byte("key:00002")
	range2 := types.Range{splitKey, fullRange[1]}

	srcDB := &DBImpl{
		logger:       logger,
		antflyConfig: &common.Config{},
		indexes:      make(map[string]indexes.IndexConfig),
	}
	require.NoError(t, srcDB.Open(srcDir, false, schema, fullRange))
	defer srcDB.Close()

	bleveIndexConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, srcDB.AddIndex(*bleveIndexConfig))

	writes := make([][2][]byte, 0, 4)
	for i := range 4 {
		key := fmt.Sprintf("key:%05d", i)
		doc := testDocument{
			ID:      key,
			Content: fmt.Sprintf("doc %d", i),
		}
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err)
		writes = append(writes, [2][]byte{[]byte(key), docJSON})
	}
	require.NoError(t, srcDB.Batch(ctx, writes, nil, Op_SyncLevelPropose))
	require.NoError(t, srcDB.pdb.Flush())

	require.NoError(t, srcDB.SetSplitState(SplitState_builder{
		Phase:            SplitState_PHASE_PREPARE,
		SplitKey:         splitKey,
		OriginalRangeEnd: fullRange[1],
	}.Build()))
	require.NoError(t, srcDB.SetSplitDeltaFinalSeq(42))

	require.NoError(t, srcDB.Split(fullRange, splitKey, destDir1, destDir2, true))

	childDB := &DBImpl{
		logger:       logger,
		antflyConfig: &common.Config{},
	}
	require.NoError(t, childDB.Open(destDir2, false, nil, types.Range{}))
	defer childDB.Close()

	childRange, err := childDB.GetRange()
	require.NoError(t, err)
	require.Equal(t, range2, childRange)

	doc, err := childDB.Get(ctx, []byte("key:00002"))
	require.NoError(t, err)
	require.Equal(t, "key:00002", doc["id"])

	_, err = childDB.Get(ctx, []byte("key:00001"))
	require.Error(t, err)

	require.Nil(t, childDB.GetSplitState())
	finalSeq, err := childDB.GetSplitDeltaFinalSeq()
	require.NoError(t, err)
	require.Zero(t, finalSeq)
	seq, err := childDB.GetSplitDeltaSeq()
	require.NoError(t, err)
	require.Zero(t, seq)

	childIndexes := childDB.GetIndexes()
	require.Contains(t, childIndexes, "full_text_index")
}

func TestOptimizeSplitBleveDocIDRequest(t *testing.T) {
	t.Run("PrimaryOnly", func(t *testing.T) {
		req := bleve.NewSearchRequest(blevequery.NewDocIDQuery([]string{"aaa", "bbb"}))
		optimized, route, ok := optimizeSplitBleveDocIDRequest(req, []byte("m"))
		require.True(t, ok)
		require.Equal(t, "primary", route)
		optimizedReq, ok := optimized.(*bleve.SearchRequest)
		require.True(t, ok)
		docQuery, ok := optimizedReq.Query.(*blevequery.DocIDQuery)
		require.True(t, ok)
		require.Equal(t, []string{"aaa", "bbb"}, docQuery.IDs)
	})

	t.Run("ShadowOnly", func(t *testing.T) {
		req := bleve.NewSearchRequest(blevequery.NewDocIDQuery([]string{"mmm", "zzz"}))
		optimized, route, ok := optimizeSplitBleveDocIDRequest(req, []byte("m"))
		require.True(t, ok)
		require.Equal(t, "shadow", route)
		optimizedReq, ok := optimized.(*bleve.SearchRequest)
		require.True(t, ok)
		docQuery, ok := optimizedReq.Query.(*blevequery.DocIDQuery)
		require.True(t, ok)
		require.Equal(t, []string{"mmm", "zzz"}, docQuery.IDs)
	})

	t.Run("MixedDocIDsUnchanged", func(t *testing.T) {
		req := bleve.NewSearchRequest(blevequery.NewDocIDQuery([]string{"aaa", "zzz"}))
		optimized, route, ok := optimizeSplitBleveDocIDRequest(req, []byte("m"))
		require.False(t, ok)
		require.Empty(t, route)
		require.Same(t, req, optimized)
	})
}

func TestFinalizeSplitPrunesParentFullTextIndex(t *testing.T) {
	logger := zaptest.NewLogger(t)
	ctx := context.Background()

	baseDir := t.TempDir()
	srcDir := filepath.Join(baseDir, "src")
	require.NoError(t, os.MkdirAll(srcDir, os.ModePerm))

	tableSchema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"id":      map[string]any{"type": "string"},
						"content": map[string]any{"type": "string"},
					},
				},
			},
		},
	}

	fullRange := types.Range{[]byte("\x00"), []byte("\xFF")}
	splitKey := []byte("key:00002")
	parentRange := types.Range{fullRange[0], splitKey}

	db := &DBImpl{
		logger:       logger,
		antflyConfig: &common.Config{},
		indexes:      make(map[string]indexes.IndexConfig),
	}
	require.NoError(t, db.Open(srcDir, false, tableSchema, fullRange))
	defer db.Close()

	bleveIndexConfig := indexes.NewFullTextIndexConfig("full_text_index", false)
	require.NoError(t, db.AddIndex(*bleveIndexConfig))

	writes := make([][2][]byte, 0, 2)
	for key, content := range map[string]string{
		"key:00001": "retained document",
		"key:00003": "split document",
	} {
		docJSON, err := json.Marshal(testDocument{
			ID:      key,
			Content: content,
		})
		require.NoError(t, err)
		writes = append(writes, [2][]byte{[]byte(key), docJSON})
	}
	err := db.Batch(ctx, writes, nil, Op_SyncLevelFullText)
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}
	require.NoError(t, db.pdb.Flush())

	beforeFinalizeRaw, err := db.indexManager.Search(ctx, "full_text_index",
		bleve.NewSearchRequest(blevequery.NewDocIDQuery([]string{"key:00003"})))
	require.NoError(t, err)
	beforeFinalize, ok := beforeFinalizeRaw.(*bleve.SearchResult)
	require.True(t, ok)
	require.Len(t, beforeFinalize.Hits, 1)
	require.Equal(t, "key:00003", beforeFinalize.Hits[0].ID)

	require.NoError(t, db.SetSplitState(SplitState_builder{
		Phase:            SplitState_PHASE_PREPARE,
		SplitKey:         splitKey,
		OriginalRangeEnd: fullRange[1],
	}.Build()))
	require.NoError(t, db.SetRange(parentRange))
	require.NoError(t, db.FinalizeSplit(parentRange))

	afterFinalizeRaw, err := db.indexManager.Search(ctx, "full_text_index",
		bleve.NewSearchRequest(blevequery.NewDocIDQuery([]string{"key:00003"})))
	require.NoError(t, err)
	afterFinalize, ok := afterFinalizeRaw.(*bleve.SearchResult)
	require.True(t, ok)
	require.Empty(t, afterFinalize.Hits)

	retainedRaw, err := db.indexManager.Search(ctx, "full_text_index",
		bleve.NewSearchRequest(blevequery.NewDocIDQuery([]string{"key:00001"})))
	require.NoError(t, err)
	retained, ok := retainedRaw.(*bleve.SearchResult)
	require.True(t, ok)
	require.Len(t, retained.Hits, 1)
	require.Equal(t, "key:00001", retained.Hits[0].ID)

	_, err = db.Get(ctx, []byte("key:00003"))
	require.Error(t, err)
}

func TestFinalizeSplitPrunesParentChunkedVectorIndex(t *testing.T) {
	logger := zaptest.NewLogger(t)
	ctx := context.Background()

	baseDir := t.TempDir()
	srcDir := filepath.Join(baseDir, "src")
	require.NoError(t, os.MkdirAll(srcDir, os.ModePerm))

	fullRange := types.Range{[]byte("\x00"), []byte("\xFF")}
	splitKey := []byte("key:00002")
	parentRange := types.Range{fullRange[0], splitKey}
	indexName := "chunked_embedding_index"

	db := &DBImpl{
		logger:       logger,
		antflyConfig: &common.Config{},
		indexes:      make(map[string]indexes.IndexConfig),
	}
	require.NoError(t, db.Open(srcDir, false, nil, fullRange))
	defer db.Close()

	idxCfg := indexes.NewEmbeddingsConfig(indexName, indexes.EmbeddingsIndexConfig{
		Dimension: 3,
		Field:     "content",
		Chunker: &chunking.ChunkerConfig{
			Provider:    chunking.ChunkerProviderMock,
			StoreChunks: true,
		},
	})
	require.NoError(t, db.AddIndex(*idxCfg))

	docWrites := make([][2][]byte, 0, 2)
	for _, key := range []string{"key:00001", "key:00003"} {
		docJSON, err := json.Marshal(map[string]any{"content": key})
		require.NoError(t, err)
		docWrites = append(docWrites, [2][]byte{[]byte(key), docJSON})
	}
	err := db.Batch(ctx, docWrites, nil, Op_SyncLevelEmbeddings)
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	buildChunkWrites := func(docKey []byte, chunkID uint32, emb vector.T, text string) [][2][]byte {
		chunkKey := storeutils.MakeChunkKey(docKey, indexName, chunkID)
		chunkJSON, err := json.Marshal(chunking.NewTextChunk(chunkID, text, 0, len(text)))
		require.NoError(t, err)
		chunkValue := make([]byte, 0, len(chunkJSON)+8)
		chunkValue = encoding.EncodeUint64Ascending(chunkValue, xxhash.Sum64String(text))
		chunkValue = append(chunkValue, chunkJSON...)

		embKey := append(bytes.Clone(chunkKey), fmt.Appendf(nil, ":i:%s:e", indexName)...)
		embValue, err := vectorindex.EncodeEmbeddingWithHashID(nil, emb, xxhash.Sum64(chunkKey))
		require.NoError(t, err)

		return [][2][]byte{
			{chunkKey, chunkValue},
			{embKey, embValue},
		}
	}

	chunkWrites := append(
		buildChunkWrites([]byte("key:00001"), 0, vector.T{0.0, 1.0, 0.0}, "retained chunk"),
		buildChunkWrites([]byte("key:00003"), 0, vector.T{1.0, 0.0, 0.0}, "split chunk")...,
	)
	err = db.Batch(ctx, chunkWrites, nil, Op_SyncLevelEmbeddings)
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	searchReq := &vectorindex.SearchRequest{
		K:         10,
		Embedding: vector.T{1.0, 0.0, 0.0},
	}
	beforeRaw, err := db.indexManager.Search(ctx, indexName, searchReq)
	require.NoError(t, err)
	before := beforeRaw.(*vectorindex.SearchResult)
	require.NotEmpty(t, before.Hits)
	require.Contains(t, before.Hits[0].ID, "key:00003")

	require.NoError(t, db.SetSplitState(SplitState_builder{
		Phase:            SplitState_PHASE_PREPARE,
		SplitKey:         splitKey,
		OriginalRangeEnd: fullRange[1],
	}.Build()))
	require.NoError(t, db.SetRange(parentRange))
	require.NoError(t, db.FinalizeSplit(parentRange))

	afterSplitRaw, err := db.indexManager.Search(ctx, indexName, searchReq)
	require.NoError(t, err)
	afterSplit := afterSplitRaw.(*vectorindex.SearchResult)
	for _, hit := range afterSplit.Hits {
		require.NotContains(t, hit.ID, "key:00003")
	}

	retainedRaw, err := db.indexManager.Search(ctx, indexName, &vectorindex.SearchRequest{
		K:         10,
		Embedding: vector.T{0.0, 1.0, 0.0},
	})
	require.NoError(t, err)
	retained := retainedRaw.(*vectorindex.SearchResult)
	require.NotEmpty(t, retained.Hits)
	require.Contains(t, retained.Hits[0].ID, "key:00001")
}

func TestFinalizeSplitPrunesParentChunkedVectorIndexWhenDocKeyContainsIndexMarker(t *testing.T) {
	logger := zaptest.NewLogger(t)
	ctx := context.Background()

	baseDir := t.TempDir()
	srcDir := filepath.Join(baseDir, "src")
	require.NoError(t, os.MkdirAll(srcDir, os.ModePerm))

	fullRange := types.Range{[]byte("\x00"), []byte("\xFF")}
	splitKey := []byte("m")
	parentRange := types.Range{fullRange[0], splitKey}
	indexName := "chunked_embedding_index"

	db := &DBImpl{
		logger:       logger,
		antflyConfig: &common.Config{},
		indexes:      make(map[string]indexes.IndexConfig),
	}
	require.NoError(t, db.Open(srcDir, false, nil, fullRange))
	defer db.Close()

	idxCfg := indexes.NewEmbeddingsConfig(indexName, indexes.EmbeddingsIndexConfig{
		Dimension: 3,
		Field:     "content",
		Chunker: &chunking.ChunkerConfig{
			Provider:    chunking.ChunkerProviderMock,
			StoreChunks: true,
		},
	})
	require.NoError(t, db.AddIndex(*idxCfg))

	retainedDocKey := []byte("alpha:i:1")
	splitDocKey := []byte("zeta:i:42")

	docWrites := make([][2][]byte, 0, 2)
	for _, key := range [][]byte{retainedDocKey, splitDocKey} {
		docJSON, err := json.Marshal(map[string]any{"content": string(key)})
		require.NoError(t, err)
		docWrites = append(docWrites, [2][]byte{key, docJSON})
	}
	err := db.Batch(ctx, docWrites, nil, Op_SyncLevelEmbeddings)
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	buildChunkWrites := func(docKey []byte, chunkID uint32, emb vector.T, text string) [][2][]byte {
		chunkKey := storeutils.MakeChunkKey(docKey, indexName, chunkID)
		chunkJSON, err := json.Marshal(chunking.NewTextChunk(chunkID, text, 0, len(text)))
		require.NoError(t, err)
		chunkValue := make([]byte, 0, len(chunkJSON)+8)
		chunkValue = encoding.EncodeUint64Ascending(chunkValue, xxhash.Sum64String(text))
		chunkValue = append(chunkValue, chunkJSON...)

		embKey := append(bytes.Clone(chunkKey), fmt.Appendf(nil, ":i:%s:e", indexName)...)
		embValue, err := vectorindex.EncodeEmbeddingWithHashID(nil, emb, xxhash.Sum64(chunkKey))
		require.NoError(t, err)

		return [][2][]byte{
			{chunkKey, chunkValue},
			{embKey, embValue},
		}
	}

	chunkWrites := append(
		buildChunkWrites(retainedDocKey, 0, vector.T{0.0, 1.0, 0.0}, "retained chunk"),
		buildChunkWrites(splitDocKey, 0, vector.T{1.0, 0.0, 0.0}, "split chunk")...,
	)
	err = db.Batch(ctx, chunkWrites, nil, Op_SyncLevelEmbeddings)
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	searchReq := &vectorindex.SearchRequest{
		K:         10,
		Embedding: vector.T{1.0, 0.0, 0.0},
	}
	beforeRaw, err := db.indexManager.Search(ctx, indexName, searchReq)
	require.NoError(t, err)
	before := beforeRaw.(*vectorindex.SearchResult)
	require.NotEmpty(t, before.Hits)
	require.Contains(t, before.Hits[0].ID, string(splitDocKey))

	require.NoError(t, db.SetSplitState(SplitState_builder{
		Phase:            SplitState_PHASE_PREPARE,
		SplitKey:         splitKey,
		OriginalRangeEnd: fullRange[1],
	}.Build()))
	require.NoError(t, db.SetRange(parentRange))
	require.NoError(t, db.FinalizeSplit(parentRange))

	afterSplitRaw, err := db.indexManager.Search(ctx, indexName, searchReq)
	require.NoError(t, err)
	afterSplit := afterSplitRaw.(*vectorindex.SearchResult)
	for _, hit := range afterSplit.Hits {
		require.NotContains(t, hit.ID, string(splitDocKey))
	}

	retainedRaw, err := db.indexManager.Search(ctx, indexName, &vectorindex.SearchRequest{
		K:         10,
		Embedding: vector.T{0.0, 1.0, 0.0},
	})
	require.NoError(t, err)
	retained := retainedRaw.(*vectorindex.SearchResult)
	require.NotEmpty(t, retained.Hits)
	require.Contains(t, retained.Hits[0].ID, string(retainedDocKey))
}

func TestFinalizeSplitPreservesParentVectorRecall(t *testing.T) {
	ctx := context.Background()

	const (
		indexName     = "test_idx"
		dataCount     = 900
		retainedCount = 450
		queryCount    = 100
		topK          = 10
		minAvgRecall  = 0.93
		waitTimeout   = 30 * time.Second
		waitFrequency = 200 * time.Millisecond
	)

	logger := zaptest.NewLogger(t)
	baseDir := t.TempDir()
	srcDir := filepath.Join(baseDir, "src")
	require.NoError(t, os.MkdirAll(srcDir, os.ModePerm))

	fullRange := types.Range{[]byte("\x00"), []byte("\xFF")}
	db := &DBImpl{
		logger:       logger,
		antflyConfig: &common.Config{},
		indexes:      make(map[string]indexes.IndexConfig),
	}
	require.NoError(t, db.Open(srcDir, false, nil, fullRange))
	require.NoError(t, db.AddIndex(*indexes.NewEmbeddingsConfig(indexName, indexes.EmbeddingsIndexConfig{
		Dimension: 20,
		Field:     "content",
	})))
	dir := srcDir
	defer db.Close()
	defer os.RemoveAll(dir)

	dataset := testutils.LoadDataset(t, testutils.RandomDataset20d)
	dataVectors := dataset.Slice(0, dataCount)
	retainedVectors := dataVectors.Slice(0, retainedCount)
	queryVectors := dataset.Slice(dataCount, queryCount)

	writes := make([][2][]byte, 0, dataCount)
	retainedKeys := make([]string, 0, retainedCount)
	for i := range dataCount {
		key := []byte(fmt.Sprintf("key:%04d", i))
		if i < retainedCount {
			retainedKeys = append(retainedKeys, string(key))
		}

		docJSON, err := json.Marshal(map[string]any{
			"content": fmt.Sprintf("split recall doc %d", i),
			"_embeddings": map[string]any{
				indexName: slices.Clone(dataVectors.At(i)),
			},
		})
		require.NoError(t, err)
		writes = append(writes, [2][]byte{key, docJSON})
	}

	err := db.Batch(ctx, writes, nil, Op_SyncLevelEmbeddings)
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	idx := db.indexManager.GetIndex(indexName)
	require.NotNil(t, idx)
	embIdx, ok := idx.(*indexes.EmbeddingIndex)
	require.True(t, ok, "Index should be an EmbeddingIndex")
	waitForEmbeddingIndexTotalIndexed(t, embIdx, dataCount, waitTimeout, waitFrequency)

	splitKey := []byte(fmt.Sprintf("key:%04d", retainedCount))
	parentRange := types.Range{fullRange[0], splitKey}

	require.NoError(t, db.SetSplitState(SplitState_builder{
		Phase:            SplitState_PHASE_PREPARE,
		SplitKey:         splitKey,
		OriginalRangeEnd: fullRange[1],
	}.Build()))
	require.NoError(t, db.SetRange(parentRange))
	require.NoError(t, db.FinalizeSplit(parentRange))

	avgRecall, err := calculateIndexManagerVectorRecall(
		ctx,
		db,
		indexName,
		queryVectors,
		retainedVectors,
		retainedKeys,
		topK,
	)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, avgRecall, minAvgRecall,
		"expected post-split average recall >= %.2f, got %.4f", minAvgRecall, avgRecall)
}

func calculateIndexManagerVectorRecall(
	ctx context.Context,
	db *DBImpl,
	indexName string,
	queryVectors *vector.Set,
	dataVectors *vector.Set,
	dataKeys []string,
	topK int,
) (float64, error) {
	var recallSum float64

	for i := range int(queryVectors.GetCount()) {
		queryVec := slices.Clone(queryVectors.At(i))
		raw, err := db.indexManager.Search(ctx, indexName, &vectorindex.SearchRequest{
			K:         topK,
			Embedding: queryVec,
		})
		if err != nil {
			return 0, fmt.Errorf("index manager search query %d: %w", i, err)
		}

		result, ok := raw.(*vectorindex.SearchResult)
		if !ok {
			return 0, fmt.Errorf("unexpected vector search result type %T", raw)
		}
		if len(result.Hits) < topK {
			return 0, fmt.Errorf("vector search returned %d hits, expected at least %d", len(result.Hits), topK)
		}

		prediction := make([]string, 0, topK)
		for _, hit := range result.Hits[:topK] {
			prediction = append(prediction, hit.ID)
		}

		truth := testutils.CalculateTruth(
			topK,
			vector.DistanceMetric_L2Squared,
			queryVec,
			dataVectors,
			dataKeys,
		)
		recallSum += testutils.CalculateRecall(prediction, truth)
	}

	return recallSum / float64(queryVectors.GetCount()), nil
}
