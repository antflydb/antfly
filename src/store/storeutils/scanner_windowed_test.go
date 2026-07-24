// Copyright 2026 Antfly, Inc.
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
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

// TestScanForEnrichment_WindowedIteratorReleased proves ProcessBatch runs with
// the scan iterator closed: a write made during one batch is visible to the
// next window's fresh iterator. Under a single scan-long iterator the write
// would be invisible (snapshot semantics) and doc:2 would be re-emitted even
// though it was just enriched.
func TestScanForEnrichment_WindowedIteratorReleased(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()
	embSuffix := []byte(":i:myindex:e")

	insertDocument(t, db, []byte("doc:1"), []byte(`{"id": 1}`), nil, nil)
	insertDocument(t, db, []byte("doc:2"), []byte(`{"id": 2}`), nil, nil)

	var emitted [][]byte
	err := ScanForEnrichment(ctx, db, EnrichmentScanOptions{
		ByteRange:        [2][]byte{[]byte("doc:"), []byte("doc;")},
		EnrichmentSuffix: embSuffix,
		BatchSize:        1,
		ProcessBatch: func(ctx context.Context, batch []DocumentScanState) error {
			for _, s := range batch {
				emitted = append(emitted, bytes.Clone(s.CurrentDocKey))
			}
			// Enrich doc:2 while doc:1's batch is being processed, as a live
			// writer (or this very backfill's persist step) would.
			if bytes.Equal(batch[0].CurrentDocKey, []byte("doc:1")) {
				embKey := append([]byte("doc:2"), embSuffix...)
				encoded := encodeEmbedding(t, []float32{1.0, 2.0}, 7)
				require.NoError(t, db.Set(embKey, encoded, pebble.Sync))
			}
			return nil
		},
	})

	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("doc:1")}, emitted,
		"doc:2 was enriched between windows and must not be emitted")
}

func TestScanForEnrichment_SummaryEnrichedBetweenWindows(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()
	sumSuffix := []byte(":i:myindex:s")
	embSuffix := []byte(":i:myindex:e")

	insertDocument(t, db, []byte("doc:1"), []byte(`{"id": 1}`), nil,
		map[string]string{"myindex": "one"})
	insertDocument(t, db, []byte("doc:2"), []byte(`{"id": 2}`), nil,
		map[string]string{"myindex": "two"})

	var emitted [][]byte
	err := ScanForEnrichment(ctx, db, EnrichmentScanOptions{
		ByteRange:        [2][]byte{[]byte("doc:"), []byte("doc;")},
		PrimarySuffix:    sumSuffix,
		EnrichmentSuffix: embSuffix,
		BatchSize:        1,
		ProcessBatch: func(ctx context.Context, batch []DocumentScanState) error {
			for _, state := range batch {
				emitted = append(emitted, bytes.Clone(state.CurrentDocKey))
			}
			if bytes.Equal(batch[0].CurrentDocKey, []byte("doc:1")) {
				encoded := encodeEmbedding(t, []float32{1, 2}, 7)
				require.NoError(t, db.Set(
					append([]byte("doc:2"), embSuffix...), encoded, pebble.Sync))
			}
			return nil
		},
	})

	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("doc:1")}, emitted,
		"the fresh window must observe doc:2's newly inserted embedding")
}

func TestScanForEnrichment_SummaryEnrichmentDeletedBetweenWindows(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()
	sumSuffix := []byte(":i:myindex:s")
	embSuffix := []byte(":i:myindex:e")

	insertDocument(t, db, []byte("doc:1"), []byte(`{"id": 1}`), nil,
		map[string]string{"myindex": "one"})
	insertDocument(t, db, []byte("doc:2"), []byte(`{"id": 2}`), nil,
		map[string]string{"myindex": "two"})
	doc2EmbeddingKey := append([]byte("doc:2"), embSuffix...)
	require.NoError(t, db.Set(doc2EmbeddingKey,
		encodeEmbedding(t, []float32{1, 2}, 7), pebble.Sync))

	var emitted [][]byte
	err := ScanForEnrichment(ctx, db, EnrichmentScanOptions{
		ByteRange:        [2][]byte{[]byte("doc:"), []byte("doc;")},
		PrimarySuffix:    sumSuffix,
		EnrichmentSuffix: embSuffix,
		BatchSize:        1,
		ProcessBatch: func(ctx context.Context, batch []DocumentScanState) error {
			for _, state := range batch {
				emitted = append(emitted, bytes.Clone(state.CurrentDocKey))
			}
			if bytes.Equal(batch[0].CurrentDocKey, []byte("doc:1")) {
				require.NoError(t, db.Delete(doc2EmbeddingKey, pebble.Sync))
			}
			return nil
		},
	})

	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("doc:1"), []byte("doc:2")}, emitted,
		"the fresh window must not retain doc:2's deleted embedding state")
}

// TestScanForEnrichment_WindowCutPrefixInterleavedKeys pins the resume-key
// choice: doc keys that are prefixes of one another interleave their key
// groups in raw byte order ("user:10:*" sorts inside the span between
// "user:*" and "user:2:*"). Cutting a window at the exact primary key must
// emit every document exactly once regardless.
func TestScanForEnrichment_WindowCutPrefixInterleavedKeys(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	docKeys := [][]byte{
		[]byte("user"),
		[]byte("user:1"),
		[]byte("user:10"),
		[]byte("user:2"),
	}
	for _, k := range docKeys {
		insertDocument(t, db, k, []byte(`{"v": true}`), nil, nil)
	}

	// 0 exercises the <1 clamp: unclamped it would never advance the window.
	for _, batchSize := range []int{0, 1, 2, 3, 10} {
		var emitted [][]byte
		err := ScanForEnrichment(ctx, db, EnrichmentScanOptions{
			ByteRange:        [2][]byte{[]byte("user"), []byte("user;")},
			EnrichmentSuffix: []byte(":i:myindex:e"),
			BatchSize:        batchSize,
			ProcessBatch: func(ctx context.Context, batch []DocumentScanState) error {
				for _, s := range batch {
					emitted = append(emitted, bytes.Clone(s.CurrentDocKey))
				}
				return nil
			},
		})
		require.NoError(t, err)
		require.ElementsMatch(t, docKeys, emitted,
			"batchSize=%d: every doc exactly once, no boundary dup or loss", batchSize)
	}
}

// TestScanForBackfill_WindowCutPreservesSummaries proves a window cut at a
// document boundary keeps each document's summaries attached: the boundary
// document's group re-streams whole in the next window.
func TestScanForBackfill_WindowCutPreservesSummaries(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()

	docKeys := [][]byte{[]byte("doc:1"), []byte("doc:2"), []byte("doc:3"), []byte("doc:4")}
	for _, k := range docKeys {
		insertDocument(t, db, k, []byte(`{"n": 1}`), nil, map[string]string{
			"myindex": string(k) + "-summary",
		})
	}

	// 0 exercises the <1 clamp: unclamped it would never advance the window.
	for _, batchSize := range []int{0, 1} {
		var emitted []DocumentScanState
		err := ScanForBackfill(ctx, db, BackfillScanOptions{
			ByteRange:        [2][]byte{[]byte("doc:"), []byte("doc;")},
			IncludeSummaries: true,
			BatchSize:        batchSize,
			ProcessBatch: func(ctx context.Context, batch []DocumentScanState) error {
				emitted = append(emitted, batch...)
				return nil
			},
		})

		require.NoError(t, err)
		require.Len(t, emitted, len(docKeys))
		for i, s := range emitted {
			require.Equal(t, docKeys[i], s.CurrentDocKey)
			require.Equal(t, string(docKeys[i])+"-summary", s.Summaries["myindex"],
				"summary must survive the window cut at its own boundary")
		}
	}
}
