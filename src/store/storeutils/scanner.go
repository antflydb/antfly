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
	"errors"
	"fmt"
	"io"

	json "github.com/antflydb/antfly/pkg/libaf/json"

	"github.com/antflydb/antfly/lib/chunking"
	"github.com/antflydb/antfly/lib/encoding"
	"github.com/cockroachdb/pebble/v2"
	"github.com/klauspost/compress/zstd"
)

// ScanCallback is called for each matching key during a scan.
// Return false to stop the scan early.
type ScanCallback func(key []byte, value []byte) (bool, error)

// ScanOptions configures how to scan through the database
type ScanOptions struct {
	// LowerBound is the start of the scan range (inclusive)
	LowerBound []byte
	// UpperBound is the end of the scan range (exclusive)
	UpperBound []byte
	// SkipPoint is a function that returns true if the key should be skipped
	// This is more efficient than filtering in the callback
	SkipPoint func(userKey []byte) bool
}

// Scan iterates through the database calling the callback for each matching key/value pair.
// The callback receives the raw key and value bytes.
// If the callback returns false or an error, the scan stops.
func Scan(ctx context.Context, db *pebble.DB, opts ScanOptions, callback ScanCallback) error {
	var err error
	defer func() {
		if r := recover(); r != nil {
			switch e := r.(type) {
			case error:
				if errors.Is(e, pebble.ErrClosed) {
					err = e
					return
				}
			}
			panic(r)
		}
	}()

	iterOpts := &pebble.IterOptions{
		LowerBound: opts.LowerBound,
		UpperBound: opts.UpperBound,
		SkipPoint:  opts.SkipPoint,
	}

	iter, err := db.NewIterWithContext(ctx, iterOpts)
	if err != nil {
		return fmt.Errorf("creating iterator: %w", err)
	}
	defer func() {
		_ = iter.Close()
	}()

	for iter.First(); iter.Valid(); iter.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		cont, err := callback(iter.Key(), iter.Value())
		if err != nil {
			return err
		}
		if !cont {
			break
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	return nil
}

// BatchProcessor is called when a batch of documents needs enrichment.
// It receives the document scan states (with deserialized documents) that need processing.
// Return an error to stop the scan.
type BatchProcessor func(ctx context.Context, batch []DocumentScanState) error

// EnrichmentScanOptions configures the enrichment scan
type EnrichmentScanOptions struct {
	// ByteRange is the range to scan
	ByteRange [2][]byte
	// PrimarySuffix is the suffix for the primary keys to scan (e.g., DBRangeStart for documents, SummarySuffix for summaries)
	// If nil, defaults to DBRangeStart (documents)
	PrimarySuffix []byte
	// EnrichmentSuffix is the suffix for enrichment keys to check (e.g., ":i:index_name:e" for embeddings, ":i:index_name:s" for summaries)
	EnrichmentSuffix []byte
	// BatchSize is the maximum number of items to accumulate before calling the processor
	BatchSize int
	// ProcessBatch is called for each batch of items that need enrichment
	ProcessBatch BatchProcessor
}

// ScanForEnrichment scans for items (documents or summaries) that need enrichment,
// processing them in batches. This is more efficient than collecting all results and
// then processing, as it can stream through large datasets.
//
// Use PrimarySuffix to specify what to scan (DBRangeStart for documents, SummarySuffix for summaries).
// Use EnrichmentSuffix to specify what enrichment to check for (e.g., EmbeddingSuffix, SummarySuffix).
func ScanForEnrichment(ctx context.Context, db *pebble.DB, opts EnrichmentScanOptions) error {
	var err error
	defer func() {
		if r := recover(); r != nil {
			switch e := r.(type) {
			case error:
				if errors.Is(e, pebble.ErrClosed) {
					err = e
					return
				}
			}
			panic(r)
		}
	}()

	// Default to scanning for documents if no primary suffix specified
	primarySuffix := opts.PrimarySuffix
	if primarySuffix == nil {
		primarySuffix = DBRangeStart
	}

	// When scanning summaries, we don't need JSON deserialization
	scanningDocuments := bytes.Equal(primarySuffix, DBRangeStart)

	batch := make([]DocumentScanState, 0, opts.BatchSize)
	var currentDoc *DocumentScanState
	var hasEnrichment bool
	// Track enrichments seen before primary key (e.g., embeddings come before summaries)
	seenEnrichments := make(map[string]bool)
	// Track enrichment hash IDs seen before primary key
	seenEnrichmentHashIDs := make(map[string]uint64)

	var reader *zstd.Decoder
	if scanningDocuments {
		var err error
		reader, err = zstd.NewReader(nil)
		if err != nil {
			return fmt.Errorf("creating zstd reader: %w", err)
		}
		defer reader.Close()
	}

	flushBatch := func() error {
		if len(batch) > 0 {
			if err := opts.ProcessBatch(ctx, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
		return nil
	}

	skipPoint := func(userKey []byte) bool {
		// Skip metadata keys
		if bytes.HasPrefix(userKey, MetadataPrefix) {
			return true
		}
		// Only look at primary keys and enrichment suffix
		return !bytes.HasSuffix(userKey, primarySuffix) &&
			!bytes.HasSuffix(userKey, opts.EnrichmentSuffix)
	}

	err = Scan(ctx, db, ScanOptions{
		LowerBound: opts.ByteRange[0],
		UpperBound: opts.ByteRange[1],
		SkipPoint:  skipPoint,
	}, func(key []byte, value []byte) (bool, error) {
		if bytes.HasSuffix(key, primarySuffix) {
			// This is a primary key (document or summary)
			if currentDoc != nil {
				// Check if the previous item needs enrichment
				if !hasEnrichment {
					batch = append(batch, *currentDoc)
					if len(batch) >= opts.BatchSize {
						if err := flushBatch(); err != nil {
							return false, err
						}
					}
				}
			}

			itemKey := key[:len(key)-len(primarySuffix)]

			if scanningDocuments {
				// Decompress the document value
				if err := reader.Reset(bytes.NewReader(value)); err != nil {
					return false, fmt.Errorf("resetting zstd reader for %s: %w", itemKey, err)
				}
				decompressed := &bytes.Buffer{}
				if _, err := io.Copy(decompressed, reader); err != nil {
					return false, fmt.Errorf("decompressing value for %s: %w", itemKey, err)
				}

				// Deserialize JSON document
				var doc map[string]any
				if err := json.NewDecoder(decompressed).Decode(&doc); err != nil {
					return false, fmt.Errorf("decoding JSON document for %s: %w", itemKey, err)
				}

				// Start tracking a new document
				currentDoc = &DocumentScanState{
					CurrentDocKey: append([]byte(nil), itemKey...),
					Document:      doc,
				}
			} else {
				// Scanning summaries - extract summary content
				if len(value) < 8 {
					return true, nil // Skip malformed value
				}

				summary := string(value[8:])

				// Start tracking a new summary (use Enrichment field)
				currentDoc = &DocumentScanState{
					CurrentDocKey: append([]byte(nil), itemKey...),
					Enrichment:    summary,
				}
			}

			// Check if we saw an enrichment for this key earlier
			itemKeyStr := string(itemKey)
			hasEnrichment = seenEnrichments[itemKeyStr]
			if hasEnrichment {
				delete(seenEnrichments, itemKeyStr) // Clean up
				if hashID, ok := seenEnrichmentHashIDs[itemKeyStr]; ok {
					currentDoc.EnrichmentHashID = hashID
					delete(seenEnrichmentHashIDs, itemKeyStr) // Clean up
				}
			}
		} else if bytes.HasSuffix(key, opts.EnrichmentSuffix) {
			// This is an enrichment key
			// Extract the base key (without the enrichment suffix)
			baseKey := key[:len(key)-len(opts.EnrichmentSuffix)]

			// If the base key is a chunk key (e.g., docKey:i:indexName:0:c),
			// resolve it to the parent document key so that chunk-level
			// embeddings are correctly attributed to their document.
			// This is the common case in ephemeral chunking mode where
			// chunks are not persisted but their embeddings are.
			matchKey := baseKey
			if docKey, ok := ExtractDocKeyFromChunk(baseKey); ok {
				matchKey = docKey
			}

			// Extract hash ID from the value
			var hashID uint64
			if bytes.HasSuffix(opts.EnrichmentSuffix, EmbeddingSuffix) {
				// For embeddings: hashID (8 bytes) + dimension (uint32) + vector data (4*dimension bytes)
				if len(value) >= 12 { // At least hashID + dimension encoding
					// Decode hashID first
					_, hashID, err = encoding.DecodeUint64Ascending(value)
					if err != nil {
						hashID = 0 // Reset on error
					}
				}
			} else if bytes.HasSuffix(opts.EnrichmentSuffix, SummarySuffix) {
				// For summaries: hashID (8 bytes) followed by text
				if len(value) >= 8 {
					_, hashID, _ = encoding.DecodeUint64Ascending(value)
				}
			}

			if IsDudEnrichment(value) {
				// Dud enrichment: the item was previously unenrichable.
				// Don't mark as enriched so it gets re-evaluated by
				// generatePrompts, which will skip it again cheaply if
				// the source content hasn't changed.
			} else if currentDoc != nil && bytes.Equal(matchKey, currentDoc.CurrentDocKey) {
				// This enrichment belongs to the current primary key
				hasEnrichment = true
				currentDoc.EnrichmentHashID = hashID
			} else {
				// This enrichment comes before its primary key, remember it
				matchKeyStr := string(matchKey)
				seenEnrichments[matchKeyStr] = true
				if hashID != 0 {
					seenEnrichmentHashIDs[matchKeyStr] = hashID
				}
			}
		}
		return true, nil
	})
	if err != nil {
		return err
	}

	// Don't forget the last item
	if currentDoc != nil && !hasEnrichment {
		batch = append(batch, *currentDoc)
	}

	// Flush any remaining items
	return flushBatch()
}

// DocumentScanState tracks document scanning for backfill operations
type DocumentScanState struct {
	// CurrentDocKey is the key being accumulated
	CurrentDocKey []byte
	// Document is the deserialized JSON document (when scanning documents)
	Document map[string]any
	// Enrichment contains the enrichment data being scanned (e.g., summary text when scanning summaries)
	Enrichment any
	// Summaries maps index names to their summaries for the current document
	Summaries map[string]string
	// Chunks maps index names to their chunks for the current document
	Chunks map[string][]chunking.Chunk
	// EnrichmentHashID is the hash ID of the enrichment (embedding or summary)
	// Populated during ScanForEnrichment when an enrichment key is encountered
	EnrichmentHashID uint64
}

// BackfillScanOptions configures scanning for backfill operations
type BackfillScanOptions struct {
	// ByteRange is the range to scan
	ByteRange [2][]byte
	// IncludeSummaries indicates whether to collect summaries during the scan
	IncludeSummaries bool
	// IncludeChunks indicates whether to collect full-text chunks (:cft:) during the scan
	IncludeChunks bool
	// BatchSize is the maximum number of documents to accumulate before calling the processor
	BatchSize int
	// ProcessBatch is called for each batch of documents
	ProcessBatch BatchProcessor
}

// ScanForBackfill scans documents and their associated summaries for backfilling indexes.
// This is used by BleveIndexV2 during rebuild to efficiently collect documents with their summaries.
func ScanForBackfill(ctx context.Context, db *pebble.DB, opts BackfillScanOptions) error {
	var err error
	defer func() {
		if r := recover(); r != nil {
			switch e := r.(type) {
			case error:
				if errors.Is(e, pebble.ErrClosed) {
					err = e
					return
				}
			}
			panic(r)
		}
	}()

	batch := make([]DocumentScanState, 0, opts.BatchSize)
	var currentDoc *DocumentScanState
	reader, err := zstd.NewReader(nil)
	if err != nil {
		return fmt.Errorf("creating zstd reader: %w", err)
	}
	defer reader.Close()

	flushBatch := func() error {
		if len(batch) > 0 {
			if err := opts.ProcessBatch(ctx, batch); err != nil {
				return err
			}
			// Reset batch but keep capacity
			batch = batch[:0]
		}
		return nil
	}

	skipPoint := func(userKey []byte) bool {
		// Skip metadata keys
		if bytes.HasPrefix(userKey, MetadataPrefix) {
			return true
		}
		if opts.IncludeSummaries && opts.IncludeChunks {
			return !bytes.HasSuffix(userKey, DBRangeStart) &&
				!bytes.HasSuffix(userKey, SummarySuffix) &&
				!bytes.Contains(userKey, ChunkingFullTextSuffix)
		} else if opts.IncludeSummaries {
			return !bytes.HasSuffix(userKey, DBRangeStart) &&
				!bytes.HasSuffix(userKey, SummarySuffix)
		} else if opts.IncludeChunks {
			return !bytes.HasSuffix(userKey, DBRangeStart) &&
				!bytes.Contains(userKey, ChunkingFullTextSuffix)
		}
		return !bytes.HasSuffix(userKey, DBRangeStart)
	}

	err = Scan(ctx, db, ScanOptions{
		LowerBound: opts.ByteRange[0],
		UpperBound: opts.ByteRange[1],
		SkipPoint:  skipPoint,
	}, func(key []byte, value []byte) (bool, error) {
		if bytes.HasSuffix(key, SummarySuffix) {
			// This is a summary key
			if currentDoc == nil {
				return true, nil // Skip if we haven't seen a document yet
			}

			docKey, indexName, ok := ParseSummaryKey(key)
			if !ok {
				return true, nil // Skip malformed key
			}

			// Check if this summary belongs to a different document
			if !bytes.Equal(docKey, currentDoc.CurrentDocKey) {
				// This is a summary for a different document, save current and start new
				if currentDoc != nil {
					batch = append(batch, *currentDoc)
					if len(batch) >= opts.BatchSize {
						if err := flushBatch(); err != nil {
							return false, err
						}
					}
				}
				currentDoc = nil
				return true, nil
			}

			// Extract summary value (skip first 8 bytes which are hashID)
			if len(value) < 8 {
				return true, nil
			}
			summary := string(value[8:])
			if currentDoc.Summaries == nil {
				currentDoc.Summaries = make(map[string]string)
			}
			currentDoc.Summaries[indexName] = summary

		} else if bytes.Contains(key, ChunkingFullTextSuffix) {
			// This is a chunk key: docKey:i:indexName:chunkID:cft
			if currentDoc == nil {
				return true, nil // Skip if we haven't seen a document yet
			}

			// Find :cft marker to split the key
			before, _, ok := bytes.Cut(key, ChunkingFullTextSuffix)
			if !ok {
				return true, nil // Skip malformed key
			}

			// Extract base key (everything before :cft)
			keyBeforeCft := before

			docKey, indexName, ok := ParseChunkKey(append(bytes.Clone(keyBeforeCft), ChunkingFullTextSuffix...))
			if !ok {
				return true, nil // Skip malformed key
			}

			// Check if this chunk belongs to a different document
			if !bytes.Equal(docKey, currentDoc.CurrentDocKey) {
				// This is a chunk for a different document, save current and start new
				if currentDoc != nil {
					batch = append(batch, *currentDoc)
					if len(batch) >= opts.BatchSize {
						if err := flushBatch(); err != nil {
							return false, err
						}
					}
				}
				currentDoc = nil
				return true, nil
			}

			// Extract chunk JSON (skip first 8 bytes which are hashID)
			if len(value) < 8 {
				return true, nil
			}
			var chunk chunking.Chunk
			if err := json.NewDecoder(bytes.NewReader(value[8:])).Decode(&chunk); err != nil {
				// Skip malformed chunk
				return true, nil
			}

			// Initialize Chunks map if needed
			if currentDoc.Chunks == nil {
				currentDoc.Chunks = make(map[string][]chunking.Chunk)
			}

			// Append chunk to the index's chunk list
			currentDoc.Chunks[indexName] = append(currentDoc.Chunks[indexName], chunk)

		} else if bytes.HasSuffix(key, DBRangeStart) {
			// This is a main document key
			if currentDoc != nil {
				// Save the previous document
				batch = append(batch, *currentDoc)
				if len(batch) >= opts.BatchSize {
					if err := flushBatch(); err != nil {
						return false, err
					}
				}
			}

			// Decompress the document value
			docKey := key[:len(key)-len(DBRangeStart)]
			if err := reader.Reset(bytes.NewReader(value)); err != nil {
				return false, fmt.Errorf("resetting zstd reader for %s: %w", docKey, err)
			}
			decompressed := &bytes.Buffer{}
			if _, err := io.Copy(decompressed, reader); err != nil {
				return false, fmt.Errorf("decompressing value for %s: %w", docKey, err)
			}

			// Deserialize JSON document
			var doc map[string]any
			if err := json.NewDecoder(decompressed).Decode(&doc); err != nil {
				return false, fmt.Errorf("decoding JSON document for %s: %w", docKey, err)
			}

			// Start a new document
			currentDoc = &DocumentScanState{
				CurrentDocKey: append([]byte(nil), docKey...),
				Document:      doc,
			}
			if opts.IncludeSummaries {
				currentDoc.Summaries = make(map[string]string)
			}
			if opts.IncludeChunks {
				currentDoc.Chunks = make(map[string][]chunking.Chunk)
			}
		}

		return true, nil
	})
	if err != nil {
		return err
	}

	// Don't forget the last document
	if currentDoc != nil {
		batch = append(batch, *currentDoc)
	}

	// Flush any remaining documents
	return flushBatch()
}
