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

package sparseindex

import (
	"fmt"
	"math"
	"testing"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

func newTestDB(t *testing.T) *pebble.DB {
	t.Helper()
	db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
	if err != nil {
		t.Fatalf("opening pebble: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func newTestIndex(t *testing.T) *SparseIndex {
	t.Helper()
	db := newTestDB(t)
	return New(db, Config{ChunkSize: 4}) // small chunks for testing
}

func newTestMergeDB(t *testing.T) *pebble.DB {
	t.Helper()
	reg := pebbleutils.NewRegistry()
	RegisterChunkMerger(reg, nil)
	db, err := pebble.Open("", &pebble.Options{
		FS:     vfs.NewMem(),
		Merger: reg.NewMerger("test.v1"),
	})
	if err != nil {
		t.Fatalf("opening pebble with merger: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func newTestMergeIndex(t *testing.T) *SparseIndex {
	t.Helper()
	db := newTestMergeDB(t)
	return New(db, Config{ChunkSize: 4, UseMerge: true})
}

func TestEncodingRoundTrip(t *testing.T) {
	c := &chunk{
		DocNums: []uint64{10, 20, 30, 40},
		DocIDs:  [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
		Weights: []float32{0.5, 1.0, 0.75, 0.25},
	}

	data, err := encodeChunk(c)
	if err != nil {
		t.Fatalf("encodeChunk: %v", err)
	}

	docNums, weights, maxW, err := decodeChunk(data)
	if err != nil {
		t.Fatalf("decodeChunk: %v", err)
	}

	if len(docNums) != 4 {
		t.Fatalf("expected 4 docNums, got %d", len(docNums))
	}
	if len(weights) != 4 {
		t.Fatalf("expected 4 weights, got %d", len(weights))
	}
	if maxW != 1.0 {
		t.Errorf("expected maxW=1.0, got %f", maxW)
	}

	// Doc nums should be exactly preserved
	for i, expected := range []uint64{10, 20, 30, 40} {
		if docNums[i] != expected {
			t.Errorf("docNums[%d]: expected %d, got %d", i, expected, docNums[i])
		}
	}

	// Weights should be approximately preserved (uint8 quantization)
	for i, expected := range []float32{0.5, 1.0, 0.75, 0.25} {
		if math.Abs(float64(weights[i]-expected)) > 0.01 {
			t.Errorf("weights[%d]: expected ~%f, got %f", i, expected, weights[i])
		}
	}
}

func TestQuantizationAccuracy(t *testing.T) {
	// Test that encode→decode preserves weights within quantization step size
	weights := []float32{0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0}
	c := &chunk{
		DocNums: make([]uint64, len(weights)),
		DocIDs:  make([][]byte, len(weights)),
		Weights: weights,
	}
	for i := range c.DocNums {
		c.DocNums[i] = uint64(i)
	}

	data, err := encodeChunk(c)
	if err != nil {
		t.Fatalf("encodeChunk: %v", err)
	}

	_, decodedWeights, _, err := decodeChunk(data)
	if err != nil {
		t.Fatalf("decodeChunk: %v", err)
	}

	maxWeight := float32(1.0)
	minWeight := float32(0.0)
	stepSize := (maxWeight - minWeight) / 255.0

	for i, expected := range weights {
		diff := float32(math.Abs(float64(decodedWeights[i] - expected)))
		if diff > stepSize+0.001 { // small epsilon for float rounding
			t.Errorf("weights[%d]: expected ~%f, got %f (diff %f > step %f)",
				i, expected, decodedWeights[i], diff, stepSize)
		}
	}
}

func TestForwardIndexRoundTrip(t *testing.T) {
	e := &fwdEntry{
		DocNum: 42,
		Vec:    vector.NewSparseVector([]uint32{10, 20, 30}, []float32{0.5, 1.0, 0.75}),
	}

	data := encodeFwdEntry(e)
	decoded, err := decodeFwdEntry(data)
	if err != nil {
		t.Fatalf("decodeFwdEntry: %v", err)
	}

	if decoded.DocNum != 42 {
		t.Errorf("expected docNum=42, got %d", decoded.DocNum)
	}
	decodedIndices := decoded.Vec.GetIndices()
	decodedValues := decoded.Vec.GetValues()
	if len(decodedIndices) != 3 {
		t.Fatalf("expected 3 indices, got %d", len(decodedIndices))
	}
	for i, expected := range []uint32{10, 20, 30} {
		if decodedIndices[i] != expected {
			t.Errorf("indices[%d]: expected %d, got %d", i, expected, decodedIndices[i])
		}
	}
	for i, expected := range []float32{0.5, 1.0, 0.75} {
		if decodedValues[i] != expected {
			t.Errorf("values[%d]: expected %f, got %f", i, expected, decodedValues[i])
		}
	}
}

func TestSingleInsertAndSearch(t *testing.T) {
	idx := newTestIndex(t)

	err := idx.Batch([]BatchInsert{
		{
			DocID: []byte("doc1"),
			Vec:   vector.NewSparseVector([]uint32{1, 5, 10}, []float32{0.5, 1.0, 0.3}),
		},
	}, nil)
	if err != nil {
		t.Fatalf("Batch insert: %v", err)
	}

	result, err := idx.Search(vector.NewSparseVector([]uint32{5, 10}, []float32{1.0, 1.0}), 10, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(result.Hits) != 1 {
		t.Fatalf("expected 1 hit, got %d", len(result.Hits))
	}
	if string(result.Hits[0].DocID) != "doc1" {
		t.Errorf("expected doc1, got %s", result.Hits[0].DocID)
	}
	// Score should be approximately 1.0*1.0 + 0.3*1.0 = 1.3
	// (with quantization tolerance)
	if result.Hits[0].Score < 1.0 || result.Hits[0].Score > 1.5 {
		t.Errorf("expected score ~1.3, got %f", result.Hits[0].Score)
	}
}

func TestMultiDocSearchTopK(t *testing.T) {
	idx := newTestIndex(t)

	inserts := []BatchInsert{
		{
			DocID: []byte("low"),
			Vec:   vector.NewSparseVector([]uint32{1}, []float32{0.1}),
		},
		{
			DocID: []byte("medium"),
			Vec:   vector.NewSparseVector([]uint32{1}, []float32{0.5}),
		},
		{
			DocID: []byte("high"),
			Vec:   vector.NewSparseVector([]uint32{1}, []float32{1.0}),
		},
	}

	err := idx.Batch(inserts, nil)
	if err != nil {
		t.Fatalf("Batch insert: %v", err)
	}

	// Search with k=2
	result, err := idx.Search(vector.NewSparseVector([]uint32{1}, []float32{1.0}), 2, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(result.Hits) != 2 {
		t.Fatalf("expected 2 hits, got %d", len(result.Hits))
	}

	// Results should be sorted by score descending
	if result.Hits[0].Score < result.Hits[1].Score {
		t.Errorf("results not sorted by score: %f < %f",
			result.Hits[0].Score, result.Hits[1].Score)
	}

	// Top hit should be "high"
	if string(result.Hits[0].DocID) != "high" {
		t.Errorf("expected top hit 'high', got '%s'", result.Hits[0].DocID)
	}
}

func TestDeleteRemovesFromResults(t *testing.T) {
	idx := newTestIndex(t)

	err := idx.Batch([]BatchInsert{
		{DocID: []byte("doc1"), Vec: vector.NewSparseVector([]uint32{1}, []float32{1.0})},
		{DocID: []byte("doc2"), Vec: vector.NewSparseVector([]uint32{1}, []float32{0.5})},
	}, nil)
	if err != nil {
		t.Fatalf("Batch insert: %v", err)
	}

	// Delete doc1
	err = idx.Batch(nil, [][]byte{[]byte("doc1")})
	if err != nil {
		t.Fatalf("Batch delete: %v", err)
	}

	result, err := idx.Search(vector.NewSparseVector([]uint32{1}, []float32{1.0}), 10, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(result.Hits) != 1 {
		t.Fatalf("expected 1 hit after delete, got %d", len(result.Hits))
	}
	if string(result.Hits[0].DocID) != "doc2" {
		t.Errorf("expected doc2, got %s", result.Hits[0].DocID)
	}
}

func TestChunkBoundary(t *testing.T) {
	// Use chunk size of 4, insert 6 docs for same term to trigger overflow
	idx := newTestIndex(t) // chunk size = 4

	inserts := make([]BatchInsert, 6)
	for i := range inserts {
		inserts[i] = BatchInsert{
			DocID: []byte(string(rune('a' + i))),
			Vec:   vector.NewSparseVector([]uint32{1}, []float32{float32(i+1) * 0.1}),
		}
	}

	err := idx.Batch(inserts, nil)
	if err != nil {
		t.Fatalf("Batch insert: %v", err)
	}

	// All 6 should be searchable
	result, err := idx.Search(vector.NewSparseVector([]uint32{1}, []float32{1.0}), 10, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(result.Hits) != 6 {
		t.Errorf("expected 6 hits across chunks, got %d", len(result.Hits))
	}
}

func TestFilterIDs(t *testing.T) {
	idx := newTestIndex(t)

	err := idx.Batch([]BatchInsert{
		{DocID: []byte("doc1"), Vec: vector.NewSparseVector([]uint32{1}, []float32{1.0})},
		{DocID: []byte("doc2"), Vec: vector.NewSparseVector([]uint32{1}, []float32{0.5})},
		{DocID: []byte("doc3"), Vec: vector.NewSparseVector([]uint32{1}, []float32{0.8})},
	}, nil)
	if err != nil {
		t.Fatalf("Batch insert: %v", err)
	}

	// Search with filter
	result, err := idx.Search(vector.NewSparseVector([]uint32{1}, []float32{1.0}), 10, []string{"doc1", "doc3"})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(result.Hits) != 2 {
		t.Fatalf("expected 2 filtered hits, got %d", len(result.Hits))
	}

	// No doc2 in results
	for _, hit := range result.Hits {
		if string(hit.DocID) == "doc2" {
			t.Error("doc2 should be filtered out")
		}
	}
}

func TestEmptySearch(t *testing.T) {
	idx := newTestIndex(t)

	// Search on empty index
	result, err := idx.Search(vector.NewSparseVector([]uint32{1}, []float32{1.0}), 10, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(result.Hits) != 0 {
		t.Errorf("expected 0 hits on empty index, got %d", len(result.Hits))
	}
}

func TestEmptyQuery(t *testing.T) {
	idx := newTestIndex(t)

	result, err := idx.Search(vector.NewSparseVector(nil, nil), 10, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(result.Hits) != 0 {
		t.Errorf("expected 0 hits for empty query, got %d", len(result.Hits))
	}
}

func TestSortSparseVec(t *testing.T) {
	v := vector.NewSparseVector([]uint32{30, 10, 20}, []float32{0.3, 0.1, 0.2})
	sortSparseVec(v)

	indices := v.GetIndices()
	values := v.GetValues()
	expected := []uint32{10, 20, 30}
	for i, e := range expected {
		if indices[i] != e {
			t.Errorf("indices[%d]: expected %d, got %d", i, e, indices[i])
		}
	}
	expectedVals := []float32{0.1, 0.2, 0.3}
	for i, e := range expectedVals {
		if values[i] != e {
			t.Errorf("values[%d]: expected %f, got %f", i, e, values[i])
		}
	}
}

func TestTermMetaRoundTrip(t *testing.T) {
	tm := &termMeta{
		MaxWeight:  0.95,
		ChunkCount: 42,
	}
	data := encodeTermMeta(tm)
	decoded, err := decodeTermMeta(data)
	if err != nil {
		t.Fatalf("decodeTermMeta: %v", err)
	}
	if math.Abs(float64(decoded.MaxWeight-0.95)) > 0.001 {
		t.Errorf("expected maxWeight ~0.95, got %f", decoded.MaxWeight)
	}
	if decoded.ChunkCount != 42 {
		t.Errorf("expected chunkCount=42, got %d", decoded.ChunkCount)
	}
}

func TestStats(t *testing.T) {
	idx := newTestIndex(t)

	err := idx.Batch([]BatchInsert{
		{DocID: []byte("doc1"), Vec: vector.NewSparseVector([]uint32{1}, []float32{1.0})},
	}, nil)
	if err != nil {
		t.Fatalf("Batch insert: %v", err)
	}

	stats := idx.Stats()
	if stats["doc_count"] != uint64(1) {
		t.Errorf("expected doc_count=1, got %v", stats["doc_count"])
	}
}

func TestMultiTermScoring(t *testing.T) {
	idx := newTestIndex(t)

	// Doc that matches on multiple terms should score higher than one that matches on fewer
	err := idx.Batch([]BatchInsert{
		{
			DocID: []byte("multi_match"),
			Vec:   vector.NewSparseVector([]uint32{1, 2, 3}, []float32{0.5, 0.5, 0.5}),
		},
		{
			DocID: []byte("single_match"),
			Vec:   vector.NewSparseVector([]uint32{1}, []float32{0.5}),
		},
	}, nil)
	if err != nil {
		t.Fatalf("Batch insert: %v", err)
	}

	result, err := idx.Search(vector.NewSparseVector([]uint32{1, 2, 3}, []float32{1.0, 1.0, 1.0}), 10, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(result.Hits) != 2 {
		t.Fatalf("expected 2 hits, got %d", len(result.Hits))
	}

	if string(result.Hits[0].DocID) != "multi_match" {
		t.Errorf("expected top hit 'multi_match', got '%s'", result.Hits[0].DocID)
	}
	if result.Hits[0].Score <= result.Hits[1].Score {
		t.Errorf("multi_match should score higher: %f <= %f",
			result.Hits[0].Score, result.Hits[1].Score)
	}
}

func TestDeleteAfterChunkSplit(t *testing.T) {
	// Regression test: delete must find docs even after chunk splits move
	// them to a chunk number different from docNum/chunkSize.
	idx := newTestIndex(t) // chunk size = 4

	// Insert 6 docs sharing term 1 to trigger a chunk split
	inserts := make([]BatchInsert, 6)
	for i := range inserts {
		inserts[i] = BatchInsert{
			DocID: []byte(string(rune('a' + i))),
			Vec:   vector.NewSparseVector([]uint32{1}, []float32{float32(i+1) * 0.1}),
		}
	}
	if err := idx.Batch(inserts, nil); err != nil {
		t.Fatalf("Batch insert: %v", err)
	}

	// Verify all 6 present
	result, err := idx.Search(vector.NewSparseVector([]uint32{1}, []float32{1.0}), 10, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(result.Hits) != 6 {
		t.Fatalf("expected 6 hits before delete, got %d", len(result.Hits))
	}

	// Delete the last doc (most likely to be in a split chunk)
	if err := idx.Batch(nil, [][]byte{[]byte("f")}); err != nil {
		t.Fatalf("Batch delete: %v", err)
	}

	result, err = idx.Search(vector.NewSparseVector([]uint32{1}, []float32{1.0}), 10, nil)
	if err != nil {
		t.Fatalf("Search after delete: %v", err)
	}
	if len(result.Hits) != 5 {
		t.Fatalf("expected 5 hits after delete, got %d", len(result.Hits))
	}
	for _, hit := range result.Hits {
		if string(hit.DocID) == "f" {
			t.Error("doc 'f' should have been deleted")
		}
	}

	// Also delete from first chunk
	if err := idx.Batch(nil, [][]byte{[]byte("a")}); err != nil {
		t.Fatalf("Batch delete 'a': %v", err)
	}

	result, err = idx.Search(vector.NewSparseVector([]uint32{1}, []float32{1.0}), 10, nil)
	if err != nil {
		t.Fatalf("Search after second delete: %v", err)
	}
	if len(result.Hits) != 4 {
		t.Fatalf("expected 4 hits after deleting a+f, got %d", len(result.Hits))
	}
}

func TestReverseIndexLookup(t *testing.T) {
	// Verify the reverse index (rev:<docNum> → docID) is correctly maintained
	idx := newTestIndex(t)

	err := idx.Batch([]BatchInsert{
		{DocID: []byte("alpha"), Vec: vector.NewSparseVector([]uint32{1}, []float32{0.5})},
		{DocID: []byte("beta"), Vec: vector.NewSparseVector([]uint32{1}, []float32{1.0})},
	}, nil)
	if err != nil {
		t.Fatalf("Batch insert: %v", err)
	}

	// Search should return correct doc IDs via reverse lookup
	result, err := idx.Search(vector.NewSparseVector([]uint32{1}, []float32{1.0}), 10, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(result.Hits) != 2 {
		t.Fatalf("expected 2 hits, got %d", len(result.Hits))
	}

	// Top hit should be "beta" (higher weight)
	if string(result.Hits[0].DocID) != "beta" {
		t.Errorf("expected top hit 'beta', got '%s'", result.Hits[0].DocID)
	}

	// Delete "alpha" and verify reverse index is cleaned up
	if err := idx.Batch(nil, [][]byte{[]byte("alpha")}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	result, err = idx.Search(vector.NewSparseVector([]uint32{1}, []float32{1.0}), 10, nil)
	if err != nil {
		t.Fatalf("Search after delete: %v", err)
	}
	if len(result.Hits) != 1 {
		t.Fatalf("expected 1 hit after delete, got %d", len(result.Hits))
	}
	if string(result.Hits[0].DocID) != "beta" {
		t.Errorf("expected 'beta', got '%s'", result.Hits[0].DocID)
	}
}

func TestEncodeChunkUint32Overflow(t *testing.T) {
	// Doc nums exceeding uint32 should produce an error, not silent truncation
	c := &chunk{
		DocNums: []uint64{math.MaxUint32 + 1},
		Weights: []float32{1.0},
	}

	_, err := encodeChunk(c)
	if err == nil {
		t.Fatal("expected error for doc num exceeding uint32 range")
	}
}

func TestBatchInternalVisibility(t *testing.T) {
	// Two inserts in the same Batch() call that share a chunk key must both
	// be visible after commit. Before the IndexedBatch fix, the second insert
	// would overwrite the first because si.db.Get couldn't see uncommitted writes.
	idx := newTestIndex(t) // chunk size = 4

	// Both docs share term 1, same chunk (docNums 0 and 1, both in chunk 0)
	err := idx.Batch([]BatchInsert{
		{DocID: []byte("first"), Vec: vector.NewSparseVector([]uint32{1}, []float32{0.5})},
		{DocID: []byte("second"), Vec: vector.NewSparseVector([]uint32{1}, []float32{1.0})},
	}, nil)
	if err != nil {
		t.Fatalf("Batch insert: %v", err)
	}

	result, err := idx.Search(vector.NewSparseVector([]uint32{1}, []float32{1.0}), 10, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(result.Hits) != 2 {
		t.Fatalf("expected 2 hits (both batch inserts visible), got %d", len(result.Hits))
	}
}

func TestCacheCoherenceOnDelete(t *testing.T) {
	// Insert docs, search (populates caches), delete a doc, search again.
	// The deleted doc must not appear in results even though caches were warm.
	idx := newTestIndex(t)

	err := idx.Batch([]BatchInsert{
		{DocID: []byte("keep"), Vec: vector.NewSparseVector([]uint32{1, 2}, []float32{0.8, 0.5})},
		{DocID: []byte("remove"), Vec: vector.NewSparseVector([]uint32{1, 2}, []float32{1.0, 0.9})},
	}, nil)
	if err != nil {
		t.Fatalf("Batch insert: %v", err)
	}

	// First search populates chunk cache, revCache, and termMetaCache
	result, err := idx.Search(vector.NewSparseVector([]uint32{1, 2}, []float32{1.0, 1.0}), 10, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(result.Hits) != 2 {
		t.Fatalf("expected 2 hits before delete, got %d", len(result.Hits))
	}

	// Delete "remove"
	err = idx.Batch(nil, [][]byte{[]byte("remove")})
	if err != nil {
		t.Fatalf("Batch delete: %v", err)
	}

	// Second search must reflect the deletion despite caches
	result, err = idx.Search(vector.NewSparseVector([]uint32{1, 2}, []float32{1.0, 1.0}), 10, nil)
	if err != nil {
		t.Fatalf("Search after delete: %v", err)
	}
	if len(result.Hits) != 1 {
		t.Fatalf("expected 1 hit after delete, got %d", len(result.Hits))
	}
	if string(result.Hits[0].DocID) != "keep" {
		t.Errorf("expected 'keep', got '%s'", result.Hits[0].DocID)
	}
}

func TestChunkCacheInvalidationOnInsert(t *testing.T) {
	// Insert docs, search (populates chunk cache), insert more docs to the
	// same chunks, search again. The new docs must appear in results.
	idx := newTestIndex(t) // chunk size = 4

	// First batch: 2 docs sharing term 1
	err := idx.Batch([]BatchInsert{
		{DocID: []byte("a"), Vec: vector.NewSparseVector([]uint32{1}, []float32{0.5})},
		{DocID: []byte("b"), Vec: vector.NewSparseVector([]uint32{1}, []float32{0.7})},
	}, nil)
	if err != nil {
		t.Fatalf("First batch: %v", err)
	}

	// Search to warm chunk cache
	result, err := idx.Search(vector.NewSparseVector([]uint32{1}, []float32{1.0}), 10, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(result.Hits) != 2 {
		t.Fatalf("expected 2 hits, got %d", len(result.Hits))
	}

	// Second batch: add 2 more docs to the same chunk (docNums 2,3 still in chunk 0)
	err = idx.Batch([]BatchInsert{
		{DocID: []byte("c"), Vec: vector.NewSparseVector([]uint32{1}, []float32{0.9})},
		{DocID: []byte("d"), Vec: vector.NewSparseVector([]uint32{1}, []float32{0.3})},
	}, nil)
	if err != nil {
		t.Fatalf("Second batch: %v", err)
	}

	// Search must see all 4 docs (chunk cache must have been invalidated)
	result, err = idx.Search(vector.NewSparseVector([]uint32{1}, []float32{1.0}), 10, nil)
	if err != nil {
		t.Fatalf("Search after second insert: %v", err)
	}
	if len(result.Hits) != 4 {
		t.Fatalf("expected 4 hits after second insert, got %d", len(result.Hits))
	}
}

func TestSearchSIMD(t *testing.T) {
	idx := newTestIndex(t)

	err := idx.Batch([]BatchInsert{
		{
			DocID: []byte("doc1"),
			Vec:   vector.NewSparseVector([]uint32{1, 2, 3, 4, 5}, []float32{0.5, 0.3, 0.8, 0.1, 0.9}),
		},
		{
			DocID: []byte("doc2"),
			Vec:   vector.NewSparseVector([]uint32{1, 2}, []float32{0.2, 0.4}),
		},
	}, nil)
	if err != nil {
		t.Fatalf("Batch insert: %v", err)
	}

	result, err := idx.searchSIMD(vector.NewSparseVector([]uint32{1, 2, 3, 4, 5}, []float32{1.0, 1.0, 1.0, 1.0, 1.0}), 10, nil)
	if err != nil {
		t.Fatalf("searchSIMD: %v", err)
	}

	if len(result.Hits) != 2 {
		t.Fatalf("expected 2 hits, got %d", len(result.Hits))
	}

	// doc1 should be top hit (matches all 5 terms)
	if string(result.Hits[0].DocID) != "doc1" {
		t.Errorf("expected top hit 'doc1', got '%s'", result.Hits[0].DocID)
	}
}

// --- Merge-mode integration tests ---
// These verify the full insert→search round-trip through the Pebble merge
// operator, mirroring the key tests from the non-merge path.

func TestMerge_InsertAndSearch(t *testing.T) {
	idx := newTestMergeIndex(t)

	err := idx.Batch([]BatchInsert{
		{
			DocID: []byte("doc1"),
			Vec:   vector.NewSparseVector([]uint32{1, 5, 10}, []float32{0.5, 1.0, 0.3}),
		},
	}, nil)
	if err != nil {
		t.Fatalf("Batch insert: %v", err)
	}

	result, err := idx.Search(vector.NewSparseVector([]uint32{5, 10}, []float32{1.0, 1.0}), 10, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(result.Hits) != 1 {
		t.Fatalf("expected 1 hit, got %d", len(result.Hits))
	}
	if string(result.Hits[0].DocID) != "doc1" {
		t.Errorf("expected doc1, got %s", result.Hits[0].DocID)
	}
	if result.Hits[0].Score < 1.0 || result.Hits[0].Score > 1.5 {
		t.Errorf("expected score ~1.3, got %f", result.Hits[0].Score)
	}
}

func TestMerge_MultiDocTopK(t *testing.T) {
	idx := newTestMergeIndex(t)

	inserts := []BatchInsert{
		{DocID: []byte("low"), Vec: vector.NewSparseVector([]uint32{1}, []float32{0.1})},
		{DocID: []byte("medium"), Vec: vector.NewSparseVector([]uint32{1}, []float32{0.5})},
		{DocID: []byte("high"), Vec: vector.NewSparseVector([]uint32{1}, []float32{1.0})},
	}

	if err := idx.Batch(inserts, nil); err != nil {
		t.Fatalf("Batch insert: %v", err)
	}

	result, err := idx.Search(vector.NewSparseVector([]uint32{1}, []float32{1.0}), 2, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(result.Hits) != 2 {
		t.Fatalf("expected 2 hits, got %d", len(result.Hits))
	}
	if result.Hits[0].Score < result.Hits[1].Score {
		t.Errorf("results not sorted by score: %f < %f",
			result.Hits[0].Score, result.Hits[1].Score)
	}
	if string(result.Hits[0].DocID) != "high" {
		t.Errorf("expected top hit 'high', got '%s'", result.Hits[0].DocID)
	}
}

func TestMerge_Delete(t *testing.T) {
	idx := newTestMergeIndex(t)

	err := idx.Batch([]BatchInsert{
		{DocID: []byte("doc1"), Vec: vector.NewSparseVector([]uint32{1}, []float32{1.0})},
		{DocID: []byte("doc2"), Vec: vector.NewSparseVector([]uint32{1}, []float32{0.5})},
	}, nil)
	if err != nil {
		t.Fatalf("Batch insert: %v", err)
	}

	if err := idx.Batch(nil, [][]byte{[]byte("doc1")}); err != nil {
		t.Fatalf("Batch delete: %v", err)
	}

	result, err := idx.Search(vector.NewSparseVector([]uint32{1}, []float32{1.0}), 10, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(result.Hits) != 1 {
		t.Fatalf("expected 1 hit after delete, got %d", len(result.Hits))
	}
	if string(result.Hits[0].DocID) != "doc2" {
		t.Errorf("expected doc2, got %s", result.Hits[0].DocID)
	}
}

func TestMerge_MultipleBatches(t *testing.T) {
	// Verify that multiple Batch() calls produce merge operands that
	// Pebble resolves correctly on read.
	idx := newTestMergeIndex(t)

	// Batch 1
	if err := idx.Batch([]BatchInsert{
		{DocID: []byte("a"), Vec: vector.NewSparseVector([]uint32{1}, []float32{0.5})},
		{DocID: []byte("b"), Vec: vector.NewSparseVector([]uint32{1}, []float32{0.7})},
	}, nil); err != nil {
		t.Fatalf("First batch: %v", err)
	}

	// Batch 2 — more entries to the same chunk via merge
	if err := idx.Batch([]BatchInsert{
		{DocID: []byte("c"), Vec: vector.NewSparseVector([]uint32{1}, []float32{0.9})},
		{DocID: []byte("d"), Vec: vector.NewSparseVector([]uint32{1}, []float32{0.3})},
	}, nil); err != nil {
		t.Fatalf("Second batch: %v", err)
	}

	result, err := idx.Search(vector.NewSparseVector([]uint32{1}, []float32{1.0}), 10, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(result.Hits) != 4 {
		t.Fatalf("expected 4 hits across merge batches, got %d", len(result.Hits))
	}
}

func TestMerge_MultiTermScoring(t *testing.T) {
	idx := newTestMergeIndex(t)

	err := idx.Batch([]BatchInsert{
		{
			DocID: []byte("multi_match"),
			Vec:   vector.NewSparseVector([]uint32{1, 2, 3}, []float32{0.5, 0.5, 0.5}),
		},
		{
			DocID: []byte("single_match"),
			Vec:   vector.NewSparseVector([]uint32{1}, []float32{0.5}),
		},
	}, nil)
	if err != nil {
		t.Fatalf("Batch insert: %v", err)
	}

	result, err := idx.Search(vector.NewSparseVector([]uint32{1, 2, 3}, []float32{1.0, 1.0, 1.0}), 10, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(result.Hits) != 2 {
		t.Fatalf("expected 2 hits, got %d", len(result.Hits))
	}
	if string(result.Hits[0].DocID) != "multi_match" {
		t.Errorf("expected top hit 'multi_match', got '%s'", result.Hits[0].DocID)
	}
	if result.Hits[0].Score <= result.Hits[1].Score {
		t.Errorf("multi_match should score higher: %f <= %f",
			result.Hits[0].Score, result.Hits[1].Score)
	}
}

func TestMerge_Stats(t *testing.T) {
	idx := newTestMergeIndex(t)

	if err := idx.Batch([]BatchInsert{
		{DocID: []byte("doc1"), Vec: vector.NewSparseVector([]uint32{1}, []float32{1.0})},
	}, nil); err != nil {
		t.Fatalf("Batch insert: %v", err)
	}

	stats := idx.Stats()
	if stats["doc_count"] != uint64(1) {
		t.Errorf("expected doc_count=1, got %v", stats["doc_count"])
	}
}

func TestMerge_CompactChunks(t *testing.T) {
	// Directly write an oversized chunk to verify CompactChunks splits it.
	// Normal merge-mode inserts with sequential docNums don't create oversized
	// chunks (each chunk gets exactly chunkSize entries). This tests the
	// CompactChunks safety net for edge cases.
	idx := newTestMergeIndex(t) // chunkSize = 4

	// Insert 4 docs normally to set up the index state (rev mappings etc.)
	for i := range 8 {
		if err := idx.Batch([]BatchInsert{
			{
				DocID: fmt.Appendf(nil, "doc%02d", i),
				Vec:   vector.NewSparseVector([]uint32{1}, []float32{float32(i+1) * 0.1}),
			},
		}, nil); err != nil {
			t.Fatalf("Batch %d: %v", i, err)
		}
	}

	// Manually write an oversized chunk (6 entries, chunkSize=4) for term 42
	// to simulate what would happen with delete merge operands.
	oversized := &chunk{
		DocNums: []uint64{0, 1, 2, 3, 4, 5},
		Weights: []float32{0.1, 0.2, 0.3, 0.4, 0.5, 0.6},
	}
	data, err := encodeChunk(oversized)
	if err != nil {
		t.Fatalf("encodeChunk: %v", err)
	}
	chunkKey := idx.invChunkKey(42, 0)
	if err := idx.db.Set(chunkKey, data, nil); err != nil {
		t.Fatalf("Set oversized chunk: %v", err)
	}

	// Run compaction
	splits, err := idx.CompactChunks()
	if err != nil {
		t.Fatalf("CompactChunks: %v", err)
	}
	if splits != 1 {
		t.Errorf("expected 1 split, got %d", splits)
	}
}

func TestMerge_DeleteMultiTerm(t *testing.T) {
	// Verify that deleting a doc with multiple terms emits delete merge
	// operands for each term's chunk and all are resolved correctly.
	idx := newTestMergeIndex(t)

	err := idx.Batch([]BatchInsert{
		{DocID: []byte("multi"), Vec: vector.NewSparseVector([]uint32{1, 2, 3, 4, 5}, []float32{0.5, 0.4, 0.3, 0.2, 0.1})},
		{DocID: []byte("single"), Vec: vector.NewSparseVector([]uint32{1}, []float32{0.9})},
	}, nil)
	if err != nil {
		t.Fatalf("Batch insert: %v", err)
	}

	// Delete the multi-term doc
	if err := idx.Batch(nil, [][]byte{[]byte("multi")}); err != nil {
		t.Fatalf("Batch delete: %v", err)
	}

	// Searching any of the terms should NOT return "multi"
	for _, termID := range []uint32{1, 2, 3, 4, 5} {
		result, err := idx.Search(vector.NewSparseVector([]uint32{termID}, []float32{1.0}), 10, nil)
		if err != nil {
			t.Fatalf("Search term %d: %v", termID, err)
		}
		for _, hit := range result.Hits {
			if string(hit.DocID) == "multi" {
				t.Errorf("term %d: deleted doc 'multi' still found in results", termID)
			}
		}
	}

	// "single" should still be searchable on term 1
	result, err := idx.Search(vector.NewSparseVector([]uint32{1}, []float32{1.0}), 10, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(result.Hits) != 1 || string(result.Hits[0].DocID) != "single" {
		t.Errorf("expected only 'single', got %v", result.Hits)
	}
}

func TestMerge_DeleteAllDocs(t *testing.T) {
	idx := newTestMergeIndex(t)

	err := idx.Batch([]BatchInsert{
		{DocID: []byte("a"), Vec: vector.NewSparseVector([]uint32{1}, []float32{0.5})},
		{DocID: []byte("b"), Vec: vector.NewSparseVector([]uint32{1}, []float32{0.7})},
		{DocID: []byte("c"), Vec: vector.NewSparseVector([]uint32{1}, []float32{0.9})},
	}, nil)
	if err != nil {
		t.Fatalf("Batch insert: %v", err)
	}

	// Delete all docs
	if err := idx.Batch(nil, [][]byte{[]byte("a"), []byte("b"), []byte("c")}); err != nil {
		t.Fatalf("Batch delete all: %v", err)
	}

	result, err := idx.Search(vector.NewSparseVector([]uint32{1}, []float32{1.0}), 10, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(result.Hits) != 0 {
		t.Errorf("expected 0 hits after deleting all, got %d", len(result.Hits))
	}
}

func TestMerge_InsertDeleteInsert(t *testing.T) {
	// Insert→delete→insert cycle to verify merge resolution handles
	// interleaved operations correctly.
	idx := newTestMergeIndex(t)

	// First insert
	if err := idx.Batch([]BatchInsert{
		{DocID: []byte("doc1"), Vec: vector.NewSparseVector([]uint32{1}, []float32{0.5})},
	}, nil); err != nil {
		t.Fatalf("First insert: %v", err)
	}

	// Delete
	if err := idx.Batch(nil, [][]byte{[]byte("doc1")}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Verify deleted
	result, err := idx.Search(vector.NewSparseVector([]uint32{1}, []float32{1.0}), 10, nil)
	if err != nil {
		t.Fatalf("Search after delete: %v", err)
	}
	if len(result.Hits) != 0 {
		t.Errorf("expected 0 hits after delete, got %d", len(result.Hits))
	}

	// Re-insert with same DocID but different weight
	if err := idx.Batch([]BatchInsert{
		{DocID: []byte("doc1"), Vec: vector.NewSparseVector([]uint32{1}, []float32{0.9})},
	}, nil); err != nil {
		t.Fatalf("Re-insert: %v", err)
	}

	result, err = idx.Search(vector.NewSparseVector([]uint32{1}, []float32{1.0}), 10, nil)
	if err != nil {
		t.Fatalf("Search after re-insert: %v", err)
	}
	if len(result.Hits) != 1 {
		t.Fatalf("expected 1 hit after re-insert, got %d", len(result.Hits))
	}
	if string(result.Hits[0].DocID) != "doc1" {
		t.Errorf("expected doc1, got %s", result.Hits[0].DocID)
	}
}

func TestMerge_DeleteNonexistent(t *testing.T) {
	idx := newTestMergeIndex(t)

	// Deleting a doc that doesn't exist should be a no-op
	if err := idx.Batch(nil, [][]byte{[]byte("ghost")}); err != nil {
		t.Fatalf("Delete nonexistent: %v", err)
	}

	// Insert a doc and verify it's unaffected
	if err := idx.Batch([]BatchInsert{
		{DocID: []byte("real"), Vec: vector.NewSparseVector([]uint32{1}, []float32{0.5})},
	}, nil); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	result, err := idx.Search(vector.NewSparseVector([]uint32{1}, []float32{1.0}), 10, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(result.Hits) != 1 {
		t.Errorf("expected 1 hit, got %d", len(result.Hits))
	}
}

func TestMerge_LargeInsertBatch(t *testing.T) {
	// Insert enough docs in a single batch to verify merge operands handle
	// multiple entries per chunk correctly.
	idx := newTestMergeIndex(t) // chunk size = 4

	inserts := make([]BatchInsert, 20)
	for i := range inserts {
		inserts[i] = BatchInsert{
			DocID: fmt.Appendf(nil, "doc%03d", i),
			Vec:   vector.NewSparseVector([]uint32{1, 2}, []float32{float32(i+1) * 0.05, float32(20-i) * 0.05}),
		}
	}

	if err := idx.Batch(inserts, nil); err != nil {
		t.Fatalf("Batch insert: %v", err)
	}

	result, err := idx.Search(vector.NewSparseVector([]uint32{1, 2}, []float32{1.0, 1.0}), 20, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(result.Hits) != 20 {
		t.Fatalf("expected 20 hits, got %d", len(result.Hits))
	}
}
