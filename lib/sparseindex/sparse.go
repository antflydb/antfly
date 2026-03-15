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

// Package sparseindex provides a Pebble-backed chunked inverted index for
// sparse vectors (SPLADE-style). It stores posting lists in fixed-size chunks
// with uint8-quantized weights and delta-encoded doc IDs, enabling efficient
// Block-Max WAND search with SIMD-accelerated decoding.
package sparseindex

import (
	"fmt"
	"sync"

	"github.com/antflydb/antfly/lib/vector"
	"github.com/cockroachdb/pebble/v2"
)

const (
	// DefaultChunkSize is the number of documents per posting list chunk.
	DefaultChunkSize = 1024

	// formatVersion is the current chunk encoding format.
	formatVersion uint8 = 1
)

// Config configures a SparseIndex instance.
type Config struct {
	// ChunkSize is the number of documents per posting list chunk.
	// Defaults to DefaultChunkSize (1024).
	ChunkSize int

	// Prefix is prepended to all Pebble keys to namespace the index.
	Prefix []byte

	// ChunkCacheSize is the maximum number of decoded posting list chunks
	// to keep in memory. Defaults to 50000. Set to -1 to disable.
	ChunkCacheSize int

	// SyncLevel controls the durability of Batch() commits.
	// Defaults to pebble.Sync if nil.
	SyncLevel *pebble.WriteOptions

	// UseMerge enables the Pebble Merge operator for chunk writes.
	// When true, Batch() uses batch.Merge() instead of read-modify-write
	// for posting list chunks, eliminating Pebble reads during inserts.
	// Requires the Pebble DB to be opened with a compatible Merger.
	UseMerge bool
}

// decodedChunk holds a pre-decoded posting list chunk for caching.
type decodedChunk struct {
	DocNums   []uint64
	Weights   []float32
	MaxWeight float32
}

// chunkCache is a bounded map cache for decoded posting list chunks.
// It uses its own mutex so it can be safely accessed from both Batch()
// (under SparseIndex.mu write lock) and Search() (under read lock).
// When the cache exceeds maxSize, half the entries are evicted randomly.
type chunkCache struct {
	mu      sync.Mutex
	entries map[string]*decodedChunk
	maxSize int
}

func newChunkCache(maxSize int) *chunkCache {
	return &chunkCache{
		entries: make(map[string]*decodedChunk, min(maxSize, 4096)),
		maxSize: maxSize,
	}
}

func (c *chunkCache) Get(key string) (*decodedChunk, bool) {
	c.mu.Lock()
	v, ok := c.entries[key]
	c.mu.Unlock()
	return v, ok
}

func (c *chunkCache) Set(key string, value *decodedChunk) {
	c.mu.Lock()
	if len(c.entries) >= c.maxSize {
		// Evict half the entries. Map iteration order is random in Go,
		// giving approximate random eviction.
		target := c.maxSize / 2
		for k := range c.entries {
			delete(c.entries, k)
			target--
			if target <= 0 {
				break
			}
		}
	}
	c.entries[key] = value
	c.mu.Unlock()
}

func (c *chunkCache) Delete(key string) {
	c.mu.Lock()
	delete(c.entries, key)
	c.mu.Unlock()
}

// SparseIndex is a Pebble-backed chunked inverted index for sparse vectors.
type SparseIndex struct {
	db        *pebble.DB
	chunkSize int
	prefix    []byte
	syncOpt   *pebble.WriteOptions
	useMerge  bool

	mu            sync.RWMutex
	termMetaCache map[uint32]*termMeta // authoritative under mu
	revCache      map[uint64][]byte    // docNum → docID, authoritative under mu
	ccache        *chunkCache          // decoded chunk cache (nil if disabled)
}

// SearchResult holds the results of a sparse index search, compatible with
// vectorindex.SearchResult for fusion.
type SearchResult struct {
	Hits   []SearchHit
	Total  int
	Status *SearchStatus
}

// SearchHit is a single document match with its score.
type SearchHit struct {
	// DocID is the original document key.
	DocID []byte
	// Score is the sparse dot-product similarity score.
	Score float64
}

// SearchStatus reports search execution status.
type SearchStatus struct {
	Total      int
	Successful int
	Failed     int
}

// BatchInsert represents a single document to insert into the sparse index.
type BatchInsert struct {
	DocID []byte
	Vec   *vector.SparseVector
}

// New creates a new SparseIndex backed by the given Pebble database.
func New(db *pebble.DB, cfg Config) *SparseIndex {
	chunkSize := cfg.ChunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}

	cacheSize := cfg.ChunkCacheSize
	if cacheSize == 0 {
		cacheSize = 50_000
	}

	var cc *chunkCache
	if cacheSize > 0 {
		cc = newChunkCache(cacheSize)
	}

	syncOpt := cfg.SyncLevel
	if syncOpt == nil {
		syncOpt = pebble.Sync
	}

	return &SparseIndex{
		db:            db,
		chunkSize:     chunkSize,
		prefix:        cfg.Prefix,
		syncOpt:       syncOpt,
		useMerge:      cfg.UseMerge,
		termMetaCache: make(map[uint32]*termMeta),
		revCache:      make(map[uint64][]byte),
		ccache:        cc,
	}
}

// Stats returns index statistics.
func (si *SparseIndex) Stats() map[string]any {
	si.mu.RLock()
	defer si.mu.RUnlock()

	docCount, _ := si.getDocCount()
	return map[string]any{
		"doc_count":  docCount,
		"chunk_size": si.chunkSize,
	}
}

// CompactChunks scans for oversized posting list chunks (those exceeding
// chunkSize) and splits them. The merge operator can accumulate entries beyond
// chunkSize since it cannot create new keys; this method resolves that as
// periodic maintenance. It should be run on the leader.
func (si *SparseIndex) CompactChunks() (int, error) {
	si.mu.Lock()
	defer si.mu.Unlock()

	prefix := append([]byte(nil), si.prefix...)
	prefix = append(prefix, "inv:"...)
	iter, err := si.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixEnd(prefix),
	})
	if err != nil {
		return 0, fmt.Errorf("creating compact iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	batch := si.db.NewBatch()
	defer func() { _ = batch.Close() }()

	splits := 0
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if !isChunkKey(key) {
			continue
		}

		docNums, weights, _, decErr := decodeChunk(iter.Value())
		if decErr != nil {
			return splits, fmt.Errorf("decoding chunk during compaction: %w", decErr)
		}

		if len(docNums) <= si.chunkSize {
			continue
		}

		// Split: keep left half in current key, create new key for right half.
		mid := len(docNums) / 2
		leftChunk := &chunk{DocNums: docNums[:mid], Weights: weights[:mid]}
		rightChunk := &chunk{DocNums: docNums[mid:], Weights: weights[mid:]}

		leftData, encErr := encodeChunk(leftChunk)
		if encErr != nil {
			return splits, fmt.Errorf("encoding left chunk: %w", encErr)
		}
		if err := batch.Set(iter.Key(), leftData, nil); err != nil {
			return splits, fmt.Errorf("writing left chunk: %w", err)
		}

		// Find a free chunk key for the right half by probing forward.
		rightChunkNum := rightChunk.DocNums[0] / uint64(si.chunkSize) //nolint:gosec // G115: bounded value, cannot overflow in practice
		// Extract termID from the key. The key format is:
		// <prefix>inv:<termID>:chunk<chunkNum>
		termID, parseErr := parseTermIDFromChunkKey(iter.Key(), si.prefix)
		if parseErr != nil {
			return splits, fmt.Errorf("parsing term ID from chunk key: %w", parseErr)
		}

		rightKey := si.invChunkKey(termID, rightChunkNum)
		for {
			if _, closer, err := si.db.Get(rightKey); err == pebble.ErrNotFound {
				break
			} else if err == nil {
				_ = closer.Close()
				rightChunkNum++
				rightKey = si.invChunkKey(termID, rightChunkNum)
			} else {
				return splits, fmt.Errorf("probing chunk key: %w", err)
			}
		}

		rightData, encErr := encodeChunk(rightChunk)
		if encErr != nil {
			return splits, fmt.Errorf("encoding right chunk: %w", encErr)
		}
		if err := batch.Set(rightKey, rightData, nil); err != nil {
			return splits, fmt.Errorf("writing right chunk: %w", err)
		}

		// Invalidate cache for both keys
		if si.ccache != nil {
			si.ccache.Delete(string(iter.Key()))
			si.ccache.Delete(string(rightKey))
		}

		splits++
	}

	if err := iter.Error(); err != nil {
		return splits, fmt.Errorf("compact iterator error: %w", err)
	}

	if splits > 0 {
		if err := batch.Commit(si.syncOpt); err != nil {
			return splits, fmt.Errorf("committing compaction: %w", err)
		}
	}

	return splits, nil
}

// Close releases resources. The underlying Pebble DB is not closed.
func (si *SparseIndex) Close() error {
	return nil
}

// Pebble key helpers

func (si *SparseIndex) fwdKey(docID []byte) []byte {
	key := make([]byte, 0, len(si.prefix)+4+len(docID))
	key = append(key, si.prefix...)
	key = append(key, "fwd:"...)
	key = append(key, docID...)
	return key
}

func (si *SparseIndex) revKey(docNum uint64) []byte {
	return fmt.Appendf(append([]byte(nil), si.prefix...), "rev:%d", docNum)
}

func (si *SparseIndex) invChunkKey(termID uint32, chunkNum uint64) []byte {
	return fmt.Appendf(append([]byte(nil), si.prefix...), "inv:%d:chunk%d", termID, chunkNum)
}

func (si *SparseIndex) invMetaKey(termID uint32) []byte {
	return fmt.Appendf(append([]byte(nil), si.prefix...), "inv:%d:meta", termID)
}

func (si *SparseIndex) docCountKey() []byte {
	key := make([]byte, 0, len(si.prefix)+14)
	key = append(key, si.prefix...)
	key = append(key, "meta:doc_count"...)
	return key
}

// parseTermIDFromChunkKey extracts the term ID from a chunk key.
// Key format: <prefix>inv:<termID>:chunk<chunkNum>
func parseTermIDFromChunkKey(key, prefix []byte) (uint32, error) {
	rest := key[len(prefix):]
	// rest should start with "inv:"
	if len(rest) < 4 {
		return 0, fmt.Errorf("key too short after prefix")
	}
	rest = rest[4:] // skip "inv:"
	// Parse termID (digits before next ':')
	var termID uint32
	i := 0
	for i < len(rest) && rest[i] != ':' {
		if rest[i] < '0' || rest[i] > '9' {
			return 0, fmt.Errorf("non-digit in term ID: %q", rest)
		}
		termID = termID*10 + uint32(rest[i]-'0')
		i++
	}
	return termID, nil
}

func (si *SparseIndex) getDocCount() (uint64, error) {
	return si.getDocCountFrom(si.db)
}

func (si *SparseIndex) getDocCountFrom(r pebble.Reader) (uint64, error) {
	val, closer, err := r.Get(si.docCountKey())
	if err != nil {
		if err == pebble.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}
	defer func() { _ = closer.Close() }()
	if len(val) < 8 {
		return 0, nil
	}
	return decodeUint64(val), nil
}
