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

package storeutils

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/logger"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

// setupTestDB creates an in-memory Pebble database for testing
func setupTestDB(t *testing.T) *pebble.DB {
	// Create a new filesystem for each test to ensure complete isolation
	memFS := vfs.NewMem()
	opts := &pebble.Options{
		Logger:          &logger.NoopLoggerAndTracer{},
		LoggerAndTracer: &logger.NoopLoggerAndTracer{},
		Cache:           pebble.NewCache(128 << 20),
		FS:              memFS,
	}

	db, err := pebble.Open("", opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	return db
}

// compressDocument compresses a document using zstd
func compressDocument(t *testing.T, doc []byte) []byte {
	var buf bytes.Buffer
	encoder, err := zstd.NewWriter(&buf)
	require.NoError(t, err)
	_, err = encoder.Write(doc)
	require.NoError(t, err)
	require.NoError(t, encoder.Close())
	return buf.Bytes()
}

// encodeEmbedding encodes an embedding with its hash ID
// Format: [hashID:uint64][dimension:uint32][float32_data...]
func encodeEmbedding(t *testing.T, emb []float32, hashID uint64) []byte {
	// Encode hashID first, then vector
	encoded := encoding.EncodeUint64Ascending(nil, hashID)
	encoded, err := vector.Encode(encoded, emb)
	require.NoError(t, err)
	return encoded
}

// encodeSummary encodes a summary with its hash ID
func encodeSummary(summary string, hashID uint64) []byte {
	encoded := encoding.EncodeUint64Ascending(nil, hashID)
	encoded = append(encoded, []byte(summary)...)
	return encoded
}

// insertDocument inserts a document with optional embeddings and summaries into the database
func insertDocument(
	t *testing.T,
	db *pebble.DB,
	key []byte,
	doc []byte,
	embeddings map[string][]float32,
	summaries map[string]string,
) {
	batch := db.NewBatch()
	defer func() {
		require.NoError(t, batch.Close())
	}()

	// Insert main document
	docKey := KeyRangeStart(key)
	compressed := compressDocument(t, doc)
	require.NoError(t, batch.Set(docKey, compressed, nil))

	// Insert embeddings
	for indexName, emb := range embeddings {
		embKey := append(bytes.Clone(key), []byte(":i:"+indexName+":e")...)
		hashID := uint64(12345) // Test hash ID
		encoded := encodeEmbedding(t, emb, hashID)
		require.NoError(t, batch.Set(embKey, encoded, nil))
	}

	// Insert summaries
	for indexName, summary := range summaries {
		sumKey := append(bytes.Clone(key), []byte(":i:"+indexName+":s")...)
		hashID := uint64(67890) // Test hash ID
		encoded := encodeSummary(summary, hashID)
		require.NoError(t, batch.Set(sumKey, encoded, nil))
	}

	require.NoError(t, batch.Commit(pebble.Sync))
}

func TestScan_Basic(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	// Insert some documents
	insertDocument(t, db, []byte("doc:1"), []byte(`{"id": 1}`), nil, nil)
	insertDocument(t, db, []byte("doc:2"), []byte(`{"id": 2}`), nil, nil)
	insertDocument(t, db, []byte("doc:3"), []byte(`{"id": 3}`), nil, nil)

	var keys [][]byte
	err := Scan(ctx, db, ScanOptions{
		LowerBound: []byte("doc:"),
		UpperBound: []byte("doc;"),
	}, func(key []byte, value []byte) (bool, error) {
		keys = append(keys, bytes.Clone(key))
		return true, nil
	})

	require.NoError(t, err)
	require.Len(t, keys, 3)
}

func TestScan_WithSkipPoint(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	// Insert documents with embeddings
	insertDocument(
		t,
		db,
		[]byte("doc:1"),
		[]byte(`{"id": 1}`),
		map[string][]float32{"idx1": {1.0, 2.0}},
		nil,
	)
	insertDocument(
		t,
		db,
		[]byte("doc:2"),
		[]byte(`{"id": 2}`),
		map[string][]float32{"idx1": {3.0, 4.0}},
		nil,
	)

	var docKeys [][]byte
	err := Scan(ctx, db, ScanOptions{
		LowerBound: []byte("doc:"),
		UpperBound: []byte("doc;"),
		SkipPoint: func(userKey []byte) bool {
			// Only keep document keys, skip embeddings
			return !bytes.HasSuffix(userKey, DBRangeStart)
		},
	}, func(key []byte, value []byte) (bool, error) {
		docKeys = append(docKeys, bytes.Clone(key))
		return true, nil
	})

	require.NoError(t, err)
	require.Len(t, docKeys, 2, "Should only see document keys, not embeddings")
}

func TestScan_EarlyStop(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	// Insert documents
	for i := 1; i <= 10; i++ {
		doc := []byte(`{"id": ` + string(rune('0'+i)) + `}`)
		insertDocument(t, db, []byte("doc:"+string(rune('0'+i))), doc, nil, nil)
	}

	count := 0
	err := Scan(ctx, db, ScanOptions{
		LowerBound: []byte("doc:"),
		UpperBound: []byte("doc;"),
	}, func(key []byte, value []byte) (bool, error) {
		count++
		if count >= 5 {
			return false, nil // Stop after 5
		}
		return true, nil
	})

	require.NoError(t, err)
	require.Equal(t, 5, count)
}

func TestScanForEnrichment_DocumentsNeedingEmbeddings(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	embSuffix := []byte(":i:myindex:e")

	// Insert documents: some with embeddings, some without
	insertDocument(
		t,
		db,
		[]byte("doc:1"),
		[]byte(`{"id": 1}`),
		map[string][]float32{"myindex": {1.0, 2.0}},
		nil,
	)
	insertDocument(t, db, []byte("doc:2"), []byte(`{"id": 2}`), nil, nil) // No embedding
	insertDocument(
		t,
		db,
		[]byte("doc:3"),
		[]byte(`{"id": 3}`),
		map[string][]float32{"myindex": {3.0, 4.0}},
		nil,
	)
	insertDocument(t, db, []byte("doc:4"), []byte(`{"id": 4}`), nil, nil) // No embedding

	var needsEnrichment []DocumentScanState
	err := ScanForEnrichment(ctx, db, EnrichmentScanOptions{
		ByteRange:        [2][]byte{[]byte("doc:"), []byte("doc;")},
		EnrichmentSuffix: embSuffix,
		BatchSize:        10,
		ProcessBatch: func(ctx context.Context, batch []DocumentScanState) error {
			needsEnrichment = append(needsEnrichment, batch...)
			return nil
		},
	})

	require.NoError(t, err)
	require.Len(t, needsEnrichment, 2, "Should find 2 documents without embeddings")
	require.Equal(t, []byte("doc:2"), needsEnrichment[0].CurrentDocKey)
	require.Equal(t, []byte("doc:4"), needsEnrichment[1].CurrentDocKey)

	// Verify documents are deserialized
	require.NotNil(t, needsEnrichment[0].Document)
	require.Equal(t, float64(2), needsEnrichment[0].Document["id"])
}

func TestScanForEnrichment_SummariesNeedingEmbeddings(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	embSuffix := []byte(":i:myindex:e")
	sumSuffix := []byte(":i:myindex:s")

	// Insert documents with summaries: some summaries have embeddings, some don't
	insertDocument(t, db, []byte("doc:1"), []byte(`{"id": 1}`),
		map[string][]float32{"myindex": {1.0, 2.0}}, // Summary has embedding
		map[string]string{"myindex": "Summary 1"})

	insertDocument(t, db, []byte("doc:2"), []byte(`{"id": 2}`),
		nil, // Summary doesn't have embedding
		map[string]string{"myindex": "Summary 2"})

	insertDocument(t, db, []byte("doc:3"), []byte(`{"id": 3}`),
		map[string][]float32{"myindex": {3.0, 4.0}}, // Summary has embedding
		map[string]string{"myindex": "Summary 3"})

	var needsEnrichment []DocumentScanState
	err := ScanForEnrichment(ctx, db, EnrichmentScanOptions{
		ByteRange:        [2][]byte{[]byte("doc:"), []byte("doc;")},
		PrimarySuffix:    sumSuffix, // Scan summaries instead of documents
		EnrichmentSuffix: embSuffix, // Check for embeddings
		BatchSize:        10,
		ProcessBatch: func(ctx context.Context, batch []DocumentScanState) error {
			needsEnrichment = append(needsEnrichment, batch...)
			return nil
		},
	})

	require.NoError(t, err)
	require.Len(t, needsEnrichment, 1, "Should find 1 summary without embedding")
	require.Equal(t, []byte("doc:2"), needsEnrichment[0].CurrentDocKey)

	// Verify summary content is in Enrichment field
	require.NotNil(t, needsEnrichment[0].Enrichment)
	require.Equal(t, "Summary 2", needsEnrichment[0].Enrichment.(string))
}

func TestScanForEnrichment_SummariesNeedingEmbeddings_WithIndexMarkerInDocKey(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	embSuffix := []byte(":i:myindex:e")
	sumSuffix := []byte(":i:myindex:s")

	insertDocument(t, db, []byte("tenant:i:1"), []byte(`{"id": 1}`),
		nil,
		map[string]string{"myindex": "Summary 1"})

	var needsEnrichment []DocumentScanState
	err := ScanForEnrichment(ctx, db, EnrichmentScanOptions{
		ByteRange:        [2][]byte{[]byte("tenant"), []byte("tenanu")},
		PrimarySuffix:    sumSuffix,
		EnrichmentSuffix: embSuffix,
		BatchSize:        10,
		ProcessBatch: func(ctx context.Context, batch []DocumentScanState) error {
			needsEnrichment = append(needsEnrichment, batch...)
			return nil
		},
	})

	require.NoError(t, err)
	require.Len(t, needsEnrichment, 1)
	require.Equal(t, []byte("tenant:i:1"), needsEnrichment[0].CurrentDocKey)
	require.Equal(t, "Summary 1", needsEnrichment[0].Enrichment.(string))
}

func TestScanForEnrichment_Batching(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	embSuffix := []byte(":i:idx:e")

	// Insert 10 documents without embeddings
	for i := range 10 {
		key := []byte("doc:" + string(rune('0'+i)))
		doc := []byte(`{"id": ` + string(rune('0'+i)) + `}`)
		insertDocument(t, db, key, doc, nil, nil)
	}

	batchCount := 0
	var batchSizes []int
	err := ScanForEnrichment(ctx, db, EnrichmentScanOptions{
		ByteRange:        [2][]byte{[]byte("doc:"), []byte("doc;")},
		EnrichmentSuffix: embSuffix,
		BatchSize:        3, // Small batch size to test batching
		ProcessBatch: func(ctx context.Context, batch []DocumentScanState) error {
			batchCount++
			batchSizes = append(batchSizes, len(batch))
			return nil
		},
	})

	require.NoError(t, err)
	require.Equal(t, 4, batchCount, "Should process in 4 batches (3+3+3+1)")
	require.Equal(t, []int{3, 3, 3, 1}, batchSizes)
}

func TestScanForEnrichment_AllHaveEnrichment(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	embSuffix := []byte(":i:idx:e")

	// Insert documents all with embeddings
	insertDocument(
		t,
		db,
		[]byte("doc:1"),
		[]byte(`{"id": 1}`),
		map[string][]float32{"idx": {1.0}},
		nil,
	)
	insertDocument(
		t,
		db,
		[]byte("doc:2"),
		[]byte(`{"id": 2}`),
		map[string][]float32{"idx": {2.0}},
		nil,
	)
	insertDocument(
		t,
		db,
		[]byte("doc:3"),
		[]byte(`{"id": 3}`),
		map[string][]float32{"idx": {3.0}},
		nil,
	)

	// Use atomic int with explicit store to work around Go runtime bug with -count flag
	// where local variables may contain garbage memory between test runs.
	// This ensures the variable is properly zero-initialized even when the test binary
	// is reused across multiple test iterations.
	var callCount atomic.Int32
	callCount.Store(0) // Explicitly initialize to 0
	err := ScanForEnrichment(ctx, db, EnrichmentScanOptions{
		ByteRange:        [2][]byte{[]byte("doc:"), []byte("doc;")},
		EnrichmentSuffix: embSuffix,
		BatchSize:        10,
		ProcessBatch: func(ctx context.Context, batch []DocumentScanState) error {
			callCount.Add(1)
			return nil
		},
	})

	require.NoError(t, err)
	require.Equal(t, int32(0), callCount.Load(), "ProcessBatch should not be called when all documents have enrichment")
}

func TestScanForBackfill_WithSummaries(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	// Insert documents with summaries
	insertDocument(t, db, []byte("doc:1"), []byte(`{"id": 1, "text": "doc1"}`), nil,
		map[string]string{
			"idx1": "Summary 1 for idx1",
			"idx2": "Summary 1 for idx2",
		})

	insertDocument(t, db, []byte("doc:2"), []byte(`{"id": 2, "text": "doc2"}`), nil,
		map[string]string{
			"idx1": "Summary 2 for idx1",
		})

	insertDocument(t, db, []byte("doc:3"), []byte(`{"id": 3, "text": "doc3"}`), nil, nil)

	var scannedDocs []DocumentScanState
	err := ScanForBackfill(ctx, db, BackfillScanOptions{
		ByteRange:        [2][]byte{[]byte("doc:"), []byte("doc;")},
		IncludeSummaries: true,
		BatchSize:        10,
		ProcessBatch: func(ctx context.Context, docs []DocumentScanState) error {
			scannedDocs = append(scannedDocs, docs...)
			return nil
		},
	})

	require.NoError(t, err)
	require.Len(t, scannedDocs, 3)

	// Verify doc:1
	require.Equal(t, []byte("doc:1"), scannedDocs[0].CurrentDocKey)
	require.NotNil(t, scannedDocs[0].Document)
	require.Equal(t, float64(1), scannedDocs[0].Document["id"])
	require.Equal(t, "doc1", scannedDocs[0].Document["text"])
	require.Len(t, scannedDocs[0].Summaries, 2)
	require.Equal(t, "Summary 1 for idx1", scannedDocs[0].Summaries["idx1"])
	require.Equal(t, "Summary 1 for idx2", scannedDocs[0].Summaries["idx2"])

	// Verify doc:2
	require.Equal(t, []byte("doc:2"), scannedDocs[1].CurrentDocKey)
	require.Len(t, scannedDocs[1].Summaries, 1)
	require.Equal(t, "Summary 2 for idx1", scannedDocs[1].Summaries["idx1"])

	// Verify doc:3 has no summaries
	require.Equal(t, []byte("doc:3"), scannedDocs[2].CurrentDocKey)
	require.Empty(t, scannedDocs[2].Summaries)
}

func TestScanForBackfill_WithoutSummaries(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	// Insert documents
	insertDocument(t, db, []byte("doc:1"), []byte(`{"id": 1}`), nil, nil)
	insertDocument(t, db, []byte("doc:2"), []byte(`{"id": 2}`), nil, nil)

	var scannedDocs []DocumentScanState
	err := ScanForBackfill(ctx, db, BackfillScanOptions{
		ByteRange:        [2][]byte{[]byte("doc:"), []byte("doc;")},
		IncludeSummaries: false,
		BatchSize:        10,
		ProcessBatch: func(ctx context.Context, docs []DocumentScanState) error {
			scannedDocs = append(scannedDocs, docs...)
			return nil
		},
	})

	require.NoError(t, err)
	require.Len(t, scannedDocs, 2)
	require.Nil(t, scannedDocs[0].Summaries)
	require.Nil(t, scannedDocs[1].Summaries)
}

func TestScanForBackfill_Batching(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	// Insert 7 documents
	for i := 1; i <= 7; i++ {
		key := []byte("doc:" + string(rune('0'+i)))
		doc := []byte(`{"id": ` + string(rune('0'+i)) + `}`)
		insertDocument(t, db, key, doc, nil, nil)
	}

	var batchSizes []int
	err := ScanForBackfill(ctx, db, BackfillScanOptions{
		ByteRange:        [2][]byte{[]byte("doc:"), []byte("doc;")},
		IncludeSummaries: false,
		BatchSize:        3,
		ProcessBatch: func(ctx context.Context, docs []DocumentScanState) error {
			batchSizes = append(batchSizes, len(docs))
			return nil
		},
	})

	require.NoError(t, err)
	require.Equal(t, []int{3, 3, 1}, batchSizes, "Should batch as 3+3+1")
}

func TestMakeMediaChunkKey(t *testing.T) {
	tests := []struct {
		name      string
		docKey    []byte
		indexName string
		chunkID   uint32
		want      string
	}{
		{
			name:      "basic key",
			docKey:    []byte("doc:1"),
			indexName: "myindex",
			chunkID:   0,
			want:      "doc:1:i:myindex:0:cm",
		},
		{
			name:      "chunk ID 42",
			docKey:    []byte("doc:abc"),
			indexName: "embeddings_v0",
			chunkID:   42,
			want:      "doc:abc:i:embeddings_v0:42:cm",
		},
		{
			name:      "large chunk ID",
			docKey:    []byte("table/shard/key"),
			indexName: "idx",
			chunkID:   999,
			want:      "table/shard/key:i:idx:999:cm",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MakeMediaChunkKey(tt.docKey, tt.indexName, tt.chunkID)
			require.Equal(t, tt.want, string(got))
		})
	}
}

func TestMakeMediaChunkPrefix(t *testing.T) {
	tests := []struct {
		name      string
		docKey    []byte
		indexName string
		want      string
	}{
		{
			name:      "basic prefix",
			docKey:    []byte("doc:1"),
			indexName: "myindex",
			want:      "doc:1:i:myindex:",
		},
		{
			name:      "complex doc key",
			docKey:    []byte("table/shard/key"),
			indexName: "embeddings_v0",
			want:      "table/shard/key:i:embeddings_v0:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MakeMediaChunkPrefix(tt.docKey, tt.indexName)
			require.Equal(t, tt.want, string(got))
		})
	}

	// MakeMediaChunkPrefix should produce the same result as MakeChunkPrefix
	// since they share the same key space
	docKey := []byte("doc:1")
	indexName := "idx"
	require.Equal(t,
		string(MakeChunkPrefix(docKey, indexName)),
		string(MakeMediaChunkPrefix(docKey, indexName)),
		"MakeMediaChunkPrefix should delegate to MakeChunkPrefix",
	)
}

func TestIsMediaChunkKey(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want bool
	}{
		{
			name: "valid media chunk key",
			key:  "doc:1:i:myindex:0:cm",
			want: true,
		},
		{
			name: "valid media chunk key with large ID",
			key:  "doc:abc:i:idx:42:cm",
			want: true,
		},
		{
			name: "regular chunk key is not media",
			key:  "doc:1:i:myindex:0:c",
			want: false,
		},
		{
			name: "full-text chunk key is not media",
			key:  "doc:1:i:myindex:0:cft",
			want: false,
		},
		{
			name: "random string",
			key:  "hello-world",
			want: false,
		},
		{
			name: "embedding key",
			key:  "doc:1:i:myindex:e",
			want: false,
		},
		{
			name: "summary key",
			key:  "doc:1:i:myindex:s",
			want: false,
		},
		{
			name: "empty key",
			key:  "",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsMediaChunkKey([]byte(tt.key))
			require.Equal(t, tt.want, got)
		})
	}
}

func TestIsChunkKey_IncludesMedia(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want bool
	}{
		{
			name: "regular chunk key",
			key:  "doc:1:i:myindex:0:c",
			want: true,
		},
		{
			name: "full-text chunk key",
			key:  "doc:1:i:myindex:0:cft",
			want: true,
		},
		{
			name: "media chunk key",
			key:  "doc:1:i:myindex:0:cm",
			want: true,
		},
		{
			name: "embedding key is not chunk",
			key:  "doc:1:i:myindex:e",
			want: false,
		},
		{
			name: "summary key is not chunk",
			key:  "doc:1:i:myindex:s",
			want: false,
		},
		{
			name: "random string",
			key:  "not-a-chunk",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsChunkKey([]byte(tt.key))
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseChunkKey(t *testing.T) {
	tests := []struct {
		name          string
		key           []byte
		wantDocKey    []byte
		wantIndexName string
		wantOK        bool
	}{
		{
			name:          "regular chunk key",
			key:           []byte("doc:1:i:myindex:0:c"),
			wantDocKey:    []byte("doc:1"),
			wantIndexName: "myindex",
			wantOK:        true,
		},
		{
			name:          "full text chunk key",
			key:           []byte("doc:1:i:myindex:0:cft"),
			wantDocKey:    []byte("doc:1"),
			wantIndexName: "myindex",
			wantOK:        true,
		},
		{
			name:          "doc key contains index marker",
			key:           []byte("tenant:i:42:i:embeddings:7:c"),
			wantDocKey:    []byte("tenant:i:42"),
			wantIndexName: "embeddings",
			wantOK:        true,
		},
		{
			name:   "non chunk key",
			key:    []byte("doc:1:i:myindex:e"),
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			docKey, indexName, ok := ParseChunkKey(tt.key)
			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.wantDocKey, docKey)
			require.Equal(t, tt.wantIndexName, indexName)

			extractedDocKey, extractedOK := ExtractDocKeyFromChunk(tt.key)
			require.Equal(t, tt.wantOK, extractedOK)
			require.Equal(t, tt.wantDocKey, extractedDocKey)
		})
	}
}

func TestParseSummaryKey(t *testing.T) {
	tests := []struct {
		name          string
		key           []byte
		wantDocKey    []byte
		wantIndexName string
		wantOK        bool
	}{
		{
			name:          "regular summary key",
			key:           []byte("doc:1:i:myindex:s"),
			wantDocKey:    []byte("doc:1"),
			wantIndexName: "myindex",
			wantOK:        true,
		},
		{
			name:          "doc key contains index marker",
			key:           []byte("tenant:i:42:i:summary_idx:s"),
			wantDocKey:    []byte("tenant:i:42"),
			wantIndexName: "summary_idx",
			wantOK:        true,
		},
		{
			name:   "non summary key",
			key:    []byte("doc:1:i:myindex:e"),
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			docKey, indexName, ok := ParseSummaryKey(tt.key)
			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.wantDocKey, docKey)
			require.Equal(t, tt.wantIndexName, indexName)
		})
	}
}

func TestIsDudEnrichment(t *testing.T) {
	require.True(t, IsDudEnrichment(DudEnrichmentValue))
	require.False(t, IsDudEnrichment(nil))
	require.False(t, IsDudEnrichment([]byte{}))
	require.False(t, IsDudEnrichment([]byte{0x00}))
	require.False(t, IsDudEnrichment([]byte{0xDD, 0x00}))

	// A real embedding value should not be a dud
	realEmb := encodeEmbedding(t, []float32{1.0, 2.0}, 12345)
	require.False(t, IsDudEnrichment(realEmb))

	// A real summary value should not be a dud
	realSum := encodeSummary("some summary", 67890)
	require.False(t, IsDudEnrichment(realSum))
}

func TestScanForEnrichment_DudEnrichmentNotSkipped(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	embSuffix := []byte(":i:myindex:e")

	// Insert 3 documents
	insertDocument(t, db, []byte("doc:1"), []byte(`{"id": 1}`), nil, nil)
	insertDocument(t, db, []byte("doc:2"), []byte(`{"id": 2}`), nil, nil)
	insertDocument(t, db, []byte("doc:3"), []byte(`{"id": 3}`), nil, nil)

	// doc:1 has a real embedding — should be skipped
	batch := db.NewBatch()
	embKey1 := append(bytes.Clone([]byte("doc:1")), embSuffix...)
	require.NoError(t, batch.Set(embKey1, encodeEmbedding(t, []float32{1.0, 2.0}, 12345), nil))
	// doc:2 has a dud enrichment — should NOT be skipped (needs re-evaluation)
	embKey2 := append(bytes.Clone([]byte("doc:2")), embSuffix...)
	require.NoError(t, batch.Set(embKey2, DudEnrichmentValue, nil))
	require.NoError(t, batch.Commit(pebble.Sync))
	require.NoError(t, batch.Close())

	// doc:3 has no enrichment at all — should be included

	var needsEnrichment []DocumentScanState
	err := ScanForEnrichment(ctx, db, EnrichmentScanOptions{
		ByteRange:        [2][]byte{[]byte("doc:"), []byte("doc;")},
		EnrichmentSuffix: embSuffix,
		BatchSize:        10,
		ProcessBatch: func(ctx context.Context, batch []DocumentScanState) error {
			needsEnrichment = append(needsEnrichment, batch...)
			return nil
		},
	})

	require.NoError(t, err)
	require.Len(t, needsEnrichment, 2, "Should find doc:2 (dud) and doc:3 (missing)")

	var keys []string
	for _, s := range needsEnrichment {
		keys = append(keys, string(s.CurrentDocKey))
	}
	require.Equal(t, []string{"doc:2", "doc:3"}, keys)

	// Verify documents are deserialized
	require.NotNil(t, needsEnrichment[0].Document)
	require.Equal(t, float64(2), needsEnrichment[0].Document["id"])
	require.NotNil(t, needsEnrichment[1].Document)
	require.Equal(t, float64(3), needsEnrichment[1].Document["id"])
}

func TestScanForEnrichment_DudReplacedByRealEnrichment(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	embSuffix := []byte(":i:myindex:e")

	// Insert a document
	insertDocument(t, db, []byte("doc:1"), []byte(`{"id": 1}`), nil, nil)

	// First: write a dud enrichment
	embKey := append(bytes.Clone([]byte("doc:1")), embSuffix...)
	require.NoError(t, db.Set(embKey, DudEnrichmentValue, pebble.Sync))

	// Scan should find it (dud is not skipped)
	var found []DocumentScanState
	err := ScanForEnrichment(ctx, db, EnrichmentScanOptions{
		ByteRange:        [2][]byte{[]byte("doc:"), []byte("doc;")},
		EnrichmentSuffix: embSuffix,
		BatchSize:        10,
		ProcessBatch: func(ctx context.Context, batch []DocumentScanState) error {
			found = append(found, batch...)
			return nil
		},
	})
	require.NoError(t, err)
	require.Len(t, found, 1, "Dud enrichment should not prevent scanning")

	// Now replace the dud with a real embedding
	realEmb := encodeEmbedding(t, []float32{1.0, 2.0}, 12345)
	require.NoError(t, db.Set(embKey, realEmb, pebble.Sync))

	// Scan should now skip doc:1 (has real enrichment)
	found = nil
	err = ScanForEnrichment(ctx, db, EnrichmentScanOptions{
		ByteRange:        [2][]byte{[]byte("doc:"), []byte("doc;")},
		EnrichmentSuffix: embSuffix,
		BatchSize:        10,
		ProcessBatch: func(ctx context.Context, batch []DocumentScanState) error {
			found = append(found, batch...)
			return nil
		},
	})
	require.NoError(t, err)
	require.Empty(t, found, "Real enrichment should be skipped")
}

func TestMakeMediaChunkKey_DoesNotMutateDocKey(t *testing.T) {
	docKey := []byte("doc:1")
	original := bytes.Clone(docKey)
	_ = MakeMediaChunkKey(docKey, "idx", 0)
	require.Equal(t, original, docKey, "MakeMediaChunkKey should not mutate the input docKey")
}

func TestScanForBackfill_Empty(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	called := false
	err := ScanForBackfill(ctx, db, BackfillScanOptions{
		ByteRange:        [2][]byte{[]byte("doc:"), []byte("doc;")},
		IncludeSummaries: false,
		BatchSize:        10,
		ProcessBatch: func(ctx context.Context, docs []DocumentScanState) error {
			called = true
			return nil
		},
	})

	require.NoError(t, err)
	require.False(t, called, "ProcessBatch should not be called for empty range")
}

// TestScanForEnrichment_EphemeralChunkEmbeddings verifies that the backfill
// scanner correctly recognises documents whose embeddings are stored at
// chunk-level keys (ephemeral chunking mode).
//
// In ephemeral mode chunks are not persisted; only embeddings are, and they
// are keyed off virtual chunk keys:
//
//	docKey:i:<index>:<chunkID>:c:i:<index>:e
//
// The scanner looks for EnrichmentSuffix (:i:<index>:e) on the document key,
// but the base key extracted from a chunk-level embedding key is the chunk key
// (docKey:i:<index>:0:c), which does not equal the document key (docKey).
// This causes the scanner to treat every document as unenriched on restart.
func TestScanForEnrichment_EphemeralChunkEmbeddings(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	indexName := "myindex"
	embSuffix := []byte(":i:" + indexName + ":e")

	// --- Setup: 3 documents, 2 with chunk-level embeddings, 1 without ---

	// doc:1 – has chunk-level embeddings (simulating ephemeral mode output)
	insertDocument(t, db, []byte("doc:1"), []byte(`{"id": 1, "text": "hello world"}`), nil, nil)
	insertChunkEmbeddings(t, db, []byte("doc:1"), indexName, [][]float32{
		{1.0, 2.0}, // chunk 0
		{3.0, 4.0}, // chunk 1
	})

	// doc:2 – no embeddings at all (needs enrichment)
	insertDocument(t, db, []byte("doc:2"), []byte(`{"id": 2, "text": "needs enrichment"}`), nil, nil)

	// doc:3 – has chunk-level embeddings
	insertDocument(t, db, []byte("doc:3"), []byte(`{"id": 3, "text": "already done"}`), nil, nil)
	insertChunkEmbeddings(t, db, []byte("doc:3"), indexName, [][]float32{
		{5.0, 6.0}, // chunk 0
	})

	// --- Scan: same parameters the ephemeral embedding enricher uses ---
	var needsEnrichment []DocumentScanState
	err := ScanForEnrichment(ctx, db, EnrichmentScanOptions{
		ByteRange:        [2][]byte{[]byte("doc:"), []byte("doc;")},
		PrimarySuffix:    DBRangeStart,
		EnrichmentSuffix: embSuffix,
		BatchSize:        10,
		ProcessBatch: func(ctx context.Context, batch []DocumentScanState) error {
			needsEnrichment = append(needsEnrichment, batch...)
			return nil
		},
	})
	require.NoError(t, err)

	// Only doc:2 truly needs enrichment.
	// The scanner should recognise that doc:1 and doc:3 already have
	// embeddings (at chunk-level keys) and skip them.
	var keys []string
	for _, s := range needsEnrichment {
		keys = append(keys, string(s.CurrentDocKey))
	}
	require.Equal(t, []string{"doc:2"}, keys,
		"scanner should skip documents that already have chunk-level embeddings, "+
			"but got: %v", keys)
}

// insertChunkEmbeddings inserts embeddings at chunk-level keys, matching the
// key layout produced by the ephemeral chunking pipeline:
//
//	<docKey>:i:<indexName>:<chunkID>:c  +  :i:<indexName>:e
//
// i.e.  <docKey>:i:<indexName>:<chunkID>:c:i:<indexName>:e
func insertChunkEmbeddings(
	t *testing.T,
	db *pebble.DB,
	docKey []byte,
	indexName string,
	embeddings [][]float32,
) {
	t.Helper()
	batch := db.NewBatch()
	defer func() { require.NoError(t, batch.Close()) }()

	for i, emb := range embeddings {
		chunkKey := MakeChunkKey(docKey, indexName, uint32(i))
		embKey := MakeEmbeddingKey(chunkKey, indexName)
		hashID := uint64(99999 + i)
		encoded := encodeEmbedding(t, emb, hashID)
		require.NoError(t, batch.Set(embKey, encoded, nil))
	}
	require.NoError(t, batch.Commit(pebble.Sync))
}
