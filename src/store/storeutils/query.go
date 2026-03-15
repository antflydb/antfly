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
	"strings"

	json "github.com/antflydb/antfly/pkg/libaf/json"

	"github.com/antflydb/antfly/lib/chunking"
	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/utils"
	"github.com/antflydb/antfly/lib/vectorindex"
	"github.com/cockroachdb/pebble/v2"
	"github.com/klauspost/compress/zstd"
)

// QueryOptions specifies what additional data to retrieve with a document
type QueryOptions struct {
	// EmbeddingSuffix specifies the suffix to look for a single specific embedding (e.g., ":i:index_name:e")
	// If nil, no single specific embedding is retrieved
	// Can be used alongside EmbeddingSuffixes to retrieve multiple specific embeddings
	// Mutually exclusive with AllEmbeddings
	EmbeddingSuffix []byte

	// EmbeddingSuffixes specifies multiple suffixes to look for specific embeddings
	// If nil/empty, no multiple specific embeddings are retrieved
	// Can be used alongside EmbeddingSuffix
	// Mutually exclusive with AllEmbeddings
	EmbeddingSuffixes [][]byte

	// SummarySuffix specifies the suffix to look for a single specific summary (e.g., ":i:index_name:s")
	// If nil, no single specific summary is retrieved
	// Can be used alongside SummarySuffixes to retrieve multiple specific summaries
	// Mutually exclusive with AllSummaries
	SummarySuffix []byte

	// SummarySuffixes specifies multiple suffixes to look for specific summaries
	// If nil/empty, no multiple specific summaries are retrieved
	// Can be used alongside SummarySuffix
	// Mutually exclusive with AllSummaries
	SummarySuffixes [][]byte

	// ChunkingIndexNames specifies which index names to retrieve chunks for
	// If nil/empty, no chunks are retrieved
	// Can be used alongside AllChunks
	ChunkingIndexNames []string

	// ChunkSuffix specifies the suffix to check for chunk 0 existence (e.g., ":i:index_name:0:c" or ":i:index_name:0:cft")
	// If non-empty, retrieves ChunksHashID from chunk 0 but doesn't retrieve chunk data
	// Useful for checking if chunks are up-to-date without the overhead of fetching them
	ChunkSuffix []byte

	// AllEmbeddings if true, retrieves all embeddings for the document
	// Mutually exclusive with EmbeddingSuffix and EmbeddingSuffixes
	AllEmbeddings bool

	// AllSummaries if true, retrieves all summaries for the document
	// Mutually exclusive with SummarySuffix and SummarySuffixes
	AllSummaries bool

	// AllChunks if true, retrieves all chunks for the document (all indexes)
	AllChunks bool

	// SkipDocument if true, skips retrieving and decompressing the main document
	// Useful when only embeddings/summaries/chunks are needed for performance
	SkipDocument bool
}

// GetDocument retrieves a document with optional embeddings and summaries based on QueryOptions
func GetDocument(
	ctx context.Context,
	db *pebble.DB,
	key []byte,
	opts QueryOptions,
) (*Document, error) {
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

	k := KeyRangeStart(key)
	end := KeyRangeEnd(key)

	// Use iterator with bounds
	iterOpts := &pebble.IterOptions{
		LowerBound: k,
		UpperBound: utils.PrefixSuccessor(end),
	}

	iter, err := db.NewIterWithContext(ctx, iterOpts)
	if err != nil {
		return nil, fmt.Errorf("creating iterator for %s: %w", key, err)
	}
	defer func() {
		_ = iter.Close()
	}()

	// Seek to the main document key
	if !iter.First() {
		return nil, pebble.ErrNotFound
	}

	// Verify we found the exact key
	if !bytes.Equal(iter.Key(), k) {
		return nil, pebble.ErrNotFound
	}

	result := &Document{}

	// Decompress and decode the main document (unless skipped)
	if !opts.SkipDocument {
		reader, err := zstd.NewReader(bytes.NewReader(iter.Value()))
		if err != nil {
			return nil, fmt.Errorf("creating zstd reader for %s: %w", key, err)
		}
		defer reader.Close()

		// Decode JSON document
		var doc map[string]any
		if err := json.NewDecoder(reader).Decode(&doc); err != nil {
			return nil, fmt.Errorf("decoding JSON document for %s: %w", key, err)
		}
		result.Document = doc
	}

	// If retrieving all embeddings and/or summaries and/or chunks, iterate through all keys
	if opts.AllEmbeddings || opts.AllSummaries || opts.AllChunks {
		if opts.AllEmbeddings {
			result.Embeddings = make(map[string][]float32)
		}
		if opts.AllSummaries {
			result.Summaries = make(map[string]string)
		}
		if opts.AllChunks {
			result.Chunks = make(map[string][]chunking.Chunk)
		}

		for iter.Next() && bytes.HasPrefix(iter.Key(), key) {
			if opts.AllEmbeddings && bytes.HasSuffix(iter.Key(), EmbeddingSuffix) {
				// Extract index name from key: <key>:i:<indexName>:e
				// 1. Remove ':e' suffix
				indexName := string(iter.Key()[len(key) : len(iter.Key())-len(EmbeddingSuffix)])
				// 2. Remove '...:i:' prefix
				i := strings.LastIndex(indexName, ":i:")
				if i != -1 {
					indexName = indexName[i+3:]
					// Skip dud enrichment markers
					if IsDudEnrichment(iter.Value()) {
						continue
					}
					// Decode the embedding with hashID prefix: [hashID:uint64][vector]
					_, emb, _, err := vectorindex.DecodeEmbeddingWithHashID(iter.Value())
					if err != nil {
						return nil, fmt.Errorf("decoding embedding for %s: %w", iter.Key(), err)
					}
					result.Embeddings[indexName] = emb
				}
			}

			if opts.AllSummaries && bytes.HasSuffix(iter.Key(), SummarySuffix) {
				// Skip dud enrichment markers
				if IsDudEnrichment(iter.Value()) {
					continue
				}
				// Extract index name from key: <key>:i:<indexName>:s
				// 1. Remove ':s' suffix
				indexName := string(iter.Key()[len(key) : len(iter.Key())-len(SummarySuffix)])
				// 2. Remove '...:i:' prefix
				i := strings.LastIndex(indexName, ":i:")
				if i != -1 {
					indexName = indexName[i+3:]
					// Skip the hashID (first 8 bytes) and get the summary
					result.Summaries[indexName] = string(iter.Value()[8:])
				}
			}

			if opts.AllChunks && (bytes.Contains(iter.Key(), ChunkingFullTextSuffix) || bytes.Contains(iter.Key(), ChunkingSuffix)) {
				// Skip dud enrichment markers
				if IsDudEnrichment(iter.Value()) {
					continue
				}
				// Extract index name from key:
				// Full-text chunks: <key>:i:<indexName>:cft:<chunkID>
				// Vector chunks: <key>:i:<indexName>:c:<chunkID>
				keyStr := string(iter.Key()[len(key):])
				// Find :i: prefix
				if _, after, ok := strings.Cut(keyStr, ":i:"); ok {
					remainder := after
					// Try :cft: separator first (full-text chunks)
					var indexName string
					if before, _, ok := strings.Cut(remainder, ":cft:"); ok {
						indexName = before
					} else if before, _, ok := strings.Cut(remainder, ":c:"); ok {
						// Try :c: separator (vector chunks)
						indexName = before
					}

					if indexName != "" {
						// Decode chunk JSON (skip hashID first 8 bytes)
						var chunk chunking.Chunk
						if err := json.Unmarshal(iter.Value()[8:], &chunk); err == nil {
							if result.Chunks[indexName] == nil {
								result.Chunks[indexName] = make([]chunking.Chunk, 0)
							}
							result.Chunks[indexName] = append(result.Chunks[indexName], chunk)
						}
					}
				}
			}
		}
	} else {
		// Single embedding suffix (legacy, but still supported)
		if opts.EmbeddingSuffix != nil {
			embKey := append(bytes.Clone(key), opts.EmbeddingSuffix...)
			if iter.SeekGE(embKey) && bytes.Equal(iter.Key(), embKey) && !IsDudEnrichment(iter.Value()) {
				// Decode embedding with hashID prefix: [hashID:uint64][vector]
				result.EmbeddingHashID, result.Embedding, _, err = vectorindex.DecodeEmbeddingWithHashID(iter.Value())
				if err != nil {
					return nil, fmt.Errorf("decoding embedding for %s: %w", key, err)
				}
			}
		}

		// Multiple embedding suffixes
		if len(opts.EmbeddingSuffixes) > 0 {
			result.Embeddings = make(map[string][]float32)
			for _, suffix := range opts.EmbeddingSuffixes {
				embKey := append(bytes.Clone(key), suffix...)
				if iter.SeekGE(embKey) && bytes.Equal(iter.Key(), embKey) {
					// Extract index name from suffix (e.g., ":i:index_name:e")
					indexName := string(suffix)
					i := strings.LastIndex(indexName, ":i:")
					if i != -1 {
						indexName = indexName[i+3:]
						indexName = strings.TrimSuffix(indexName, ":e")
						// Skip dud enrichment markers
						if IsDudEnrichment(iter.Value()) {
							continue
						}
						// Decode the embedding with hashID prefix: [hashID:uint64][vector]
						_, emb, _, err := vectorindex.DecodeEmbeddingWithHashID(iter.Value())
						if err != nil {
							return nil, fmt.Errorf("decoding embedding for %s: %w", embKey, err)
						}
						result.Embeddings[indexName] = emb
					}
				}
			}
		}

		// Single summary suffix (legacy, but still supported)
		if opts.SummarySuffix != nil {
			sumKey := append(bytes.Clone(key), opts.SummarySuffix...)
			if iter.SeekGE(sumKey) && bytes.Equal(iter.Key(), sumKey) && !IsDudEnrichment(iter.Value()) {
				remaining := iter.Value()
				remaining, result.SummaryHashID, err = encoding.DecodeUint64Ascending(remaining)
				if err != nil {
					return nil, fmt.Errorf("decoding summary hashID for %s: %w", key, err)
				}
				result.Summary = string(remaining)
			}
		}

		// Multiple summary suffixes
		if len(opts.SummarySuffixes) > 0 {
			result.Summaries = make(map[string]string)
			for _, suffix := range opts.SummarySuffixes {
				sumKey := append(bytes.Clone(key), suffix...)
				if iter.SeekGE(sumKey) && bytes.Equal(iter.Key(), sumKey) && !IsDudEnrichment(iter.Value()) {
					// Extract index name from suffix (e.g., ":i:index_name:s")
					indexName := string(suffix)
					i := strings.LastIndex(indexName, ":i:")
					if i != -1 {
						indexName = indexName[i+3:]
						indexName = strings.TrimSuffix(indexName, ":s")
						// Skip the hashID (first 8 bytes) and get the summary
						result.Summaries[indexName] = string(iter.Value()[8:])
					}
				}
			}
		}

		// Check for chunk hashID if ChunkSuffix is specified
		if len(opts.ChunkSuffix) > 0 {
			// Build chunk 0 key from suffix (e.g., key + ":i:indexName:0:c")
			chunk0Key := append(bytes.Clone(key), opts.ChunkSuffix...)
			if iter.SeekGE(chunk0Key) && bytes.Equal(iter.Key(), chunk0Key) && !IsDudEnrichment(iter.Value()) {
				// Found chunk 0, extract hashID
				if len(iter.Value()) >= 8 {
					_, result.ChunksHashID, err = encoding.DecodeUint64Ascending(iter.Value()[:8])
					if err != nil {
						return nil, fmt.Errorf("decoding chunk hashID for %s: %w", chunk0Key, err)
					}
				}
			}
		}

		// Specific chunks by index name
		if len(opts.ChunkingIndexNames) > 0 {
			result.Chunks = make(map[string][]chunking.Chunk)
			for _, indexName := range opts.ChunkingIndexNames {
				// Scan for all full-text chunks: <key>:i:<indexName>:cft:*
				// Bleve always fetches :cft: chunks (not :c: vector chunks)
				chunkPrefix := MakeChunkFullTextPrefix(key, indexName)
				chunkUpperBound := utils.PrefixSuccessor(chunkPrefix)

				// Create iterator for this index's chunks
				chunkIter, err := db.NewIterWithContext(ctx, &pebble.IterOptions{
					LowerBound: chunkPrefix,
					UpperBound: chunkUpperBound,
				})
				if err != nil {
					return nil, fmt.Errorf("creating chunk iterator for %s: %w", indexName, err)
				}

				chunks := make([]chunking.Chunk, 0)
				for chunkIter.First(); chunkIter.Valid(); chunkIter.Next() {
					// Skip if value is too short
					if len(chunkIter.Value()) < 8 {
						continue
					}
					// Decode chunk JSON (skip hashID first 8 bytes)
					var chunk chunking.Chunk
					if err := json.Unmarshal(chunkIter.Value()[8:], &chunk); err == nil {
						chunks = append(chunks, chunk)
					}
				}

				if err := chunkIter.Close(); err != nil {
					return nil, fmt.Errorf("closing chunk iterator for %s: %w", indexName, err)
				}

				if len(chunks) > 0 {
					result.Chunks[indexName] = chunks
				}
			}
		}
	}

	return result, nil
}
