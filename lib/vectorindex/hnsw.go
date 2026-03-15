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

// Package vectorindex provides implementations for vector similarity search indexes.
// This file implements PebbleANN, a vector index that uses PebbleDB
// (a Log-Structured Merge-Tree based key-value store) for fully transactional storage
// of vectors, graph connections, and metadata.
package vectorindex

import (
	"bytes"
	"cmp"
	"container/heap"
	"container/list" // Import for LRU cache list
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/antflydb/antfly/lib/logger"
	"github.com/antflydb/antfly/lib/utils"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

const (
	pebbleDirname = "pebble" // Directory for PebbleDB data
	indexVersion  = 2        // Version of the index metadata format - bumped for HNSW support
)

// PebbleDB key prefixes for different data types
var (
	pebbleNodeKeyPrefix     = []byte("n:") // Key prefix for node data
	pebbleGraphKeySuffix    = []byte(":g") // Suffix for graph keys
	pebbleVectorKeySuffix   = []byte(":v") // Suffix for vector keys
	pebbleMetadataKeySuffix = []byte(":m") // Suffix for metadata keys
	pebbleLayerKeySuffix    = []byte(":l") // Suffix for layer assignment
	// Key structure: i:<[]byte> -> nodeID (user metadata invert)
	pebbleMetadataInvertKeyPrefix = []byte("i:")
	// Key structure: meta -> serialized index metadata
	indexMetaPrefix = []byte("\x00\x00__meta__:")
	// Key structure: __active_count__:<indexName> -> uint64
	activeCountPrefix = []byte("\x00\x00__active_count__:")
	// Key structure: __entry_point__:<indexName> -> uint64
	entryPointPrefix = []byte("\x00\x00__entry_point__:")
)

func makePebbleNodePrefix(node uint64) []byte {
	// Key structure: n:<nodeID>:g
	key := make([]byte, len(pebbleNodeKeyPrefix)+8)
	copy(key, pebbleNodeKeyPrefix)
	binary.BigEndian.PutUint64(key[len(pebbleNodeKeyPrefix):], node)
	return key
}

func makePebbleNodePrefixEnd(node uint64) []byte {
	// Key structure: n:<nodeID>:g
	key := make([]byte, len(pebbleNodeKeyPrefix)+8+1)
	copy(key, pebbleNodeKeyPrefix)
	binary.BigEndian.PutUint64(key[len(pebbleNodeKeyPrefix):], node)
	key[len(key)-1] = 0xFF // Set last byte to 0xFF to mark the end of the prefix
	return key
}

func makePebbleNodeRange(nodeID uint64) (lower, upper []byte) {
	// Key structure: n:<nodeID>:
	lower = makePebbleNodePrefix(nodeID)
	upper = makePebbleNodePrefixEnd(nodeID)
	upper = utils.PrefixSuccessor(upper)
	return lower, upper
}

func makePebbleVectorKey(node uint64) []byte {
	// Key structure: n:<nodeID>:v
	key := make([]byte, len(pebbleNodeKeyPrefix)+8+2)
	copy(key, pebbleNodeKeyPrefix)
	binary.BigEndian.PutUint64(key[len(pebbleNodeKeyPrefix):], node)
	copy(key[len(pebbleNodeKeyPrefix)+8:], pebbleVectorKeySuffix)
	return key
}

func makePebbleMetadataKey(node uint64) []byte {
	// Key structure: n:<nodeID>:m
	key := make([]byte, len(pebbleNodeKeyPrefix)+8+2)
	copy(key, pebbleNodeKeyPrefix)
	binary.BigEndian.PutUint64(key[len(pebbleNodeKeyPrefix):], node)
	copy(key[len(pebbleNodeKeyPrefix)+8:], pebbleMetadataKeySuffix)
	return key
}

func makePebbleLayerKey(node uint64) []byte {
	// Key structure: n:<nodeID>:l
	key := make([]byte, len(pebbleNodeKeyPrefix)+8+2)
	copy(key, pebbleNodeKeyPrefix)
	binary.BigEndian.PutUint64(key[len(pebbleNodeKeyPrefix):], node)
	copy(key[len(pebbleNodeKeyPrefix)+8:], pebbleLayerKeySuffix)
	return key
}

// makePebbleGraphLayerKey creates a key for graph connections at a specific layer
func makePebbleGraphLayerKey(node uint64, layer int) []byte {
	// Key structure: n:<nodeID>:g<layer>
	key := make([]byte, len(pebbleNodeKeyPrefix)+8+2+1+4)
	copy(key, pebbleNodeKeyPrefix)
	binary.BigEndian.PutUint64(key[len(pebbleNodeKeyPrefix):], node)
	copy(key[len(pebbleNodeKeyPrefix)+8:], pebbleGraphKeySuffix)
	key[len(key)-5] = ':'
	binary.BigEndian.PutUint32(key[len(pebbleNodeKeyPrefix)+8+2+1:], uint32(layer)) //nolint:gosec // G115: bounded value, cannot overflow in practice
	return key
}

// makePebbleMetadataInvertKey creates a PebbleDB key for user metadata
func makePebbleMetadataInvertKey(metadata []byte) []byte {
	key := make([]byte, len(pebbleMetadataInvertKeyPrefix)+len(metadata))
	copy(key, pebbleMetadataInvertKeyPrefix)
	copy(key[len(pebbleMetadataInvertKeyPrefix):], metadata)
	return key
}

// makeActiveCountKey creates a PebbleDB key for the active count
func makeActiveCountKey(indexName string) []byte {
	return append(bytes.Clone(activeCountPrefix), indexName...)
}

// makeEntryPointKey creates a PebbleDB key for the entry point
func makeEntryPointKey(indexName string) []byte {
	return append(bytes.Clone(entryPointPrefix), indexName...)
}

// HNSWConfig holds configuration parameters specific to the PebbleDB-based index.
type HNSWConfig struct {
	// Dimensionality of the vectors
	Dimension uint32
	// Distance function (e.g., CosineDistance, EuclideanDistance)
	DistanceMetric vector.DistanceMetric
	// Base path/directory where index files will be stored
	IndexPath string

	Name string

	// Max connections per node at layer 0
	Neighbors int32
	// Max connections per node at higher layers (defaults to Neighbors if not set)
	NeighborsHigherLayers int32
	// Search width during construction
	EfConstruction int32
	// Search width during querying
	EfSearch int32
	// Level multiplier for layer assignment probability (defaults to 1/ln(2.0))
	LevelMultiplier float64
	// PebbleANN specific parameters:
	// CacheSizeNodes specifies the maximum number of nodes (vector + neighbors) to keep in the LRU cache.
	// A value <= 0 disables the cache.
	CacheSizeNodes int
	// PebbleOptions allows customizing the underlying PebbleDB store.
	// If nil, default options will be used.
	PebbleOptions *pebble.Options
	// PebbleSyncWrite controls whether Pebble writes (Set, Delete) are synchronous.
	// Default is false (pebble.NoSync), which is faster but less durable in crashes.
	PebbleSyncWrite bool
	PebbleMemOnly   bool

	DirectFiltering bool
}

// HNSWNodeData holds the data retrieved for a node, used by the cache and search.
type HNSWNodeData struct {
	id        uint64 // Store ID for efficient cache eviction
	vector    []float32
	neighbors map[int][]*PriorityItem // Neighbors per layer
	metadata  []byte                  // User metadata associated with the node
	layer     int                     // Highest layer this node appears in
}

// indexMetadata holds the global index metadata stored in PebbleDB
type indexMetadata struct {
	Version         uint32
	Dimension       uint32
	ActiveCount     uint64
	EntryPoint      uint64
	M               int32
	MHigherLayers   int32
	EfConstruction  int32
	LevelMultiplier float64
	MaxLevel        int32
}

// HNSWIndex represents the PebbleDB-backed HNSW-like index.
// It uses PebbleDB for fully transactional storage of vectors, graph connections, and metadata.
type HNSWIndex struct {
	sync.RWMutex

	assignmentProbs []float64 // Precomputed probabilities for layer assignment based on exponential decay

	config HNSWConfig
	rand   *rand.Rand

	// PebbleDB instance for all data storage
	db     *pebble.DB
	dbPath string // Path to the PebbleDB directory

	// LRU Cache implementation for node data (vector + neighbors)

	cacheMu     sync.RWMutex
	cacheList   *list.List               // Doubly linked list for LRU order (Front = MRU, Back = LRU)
	nodeCache   map[uint64]*list.Element // Map from node ID to list element
	cacheHits   int64
	cacheMisses int64

	// Pebble write options (sync or no-sync)
	writeOpts *pebble.WriteOptions

	// Sync pools for memory reuse
	nodeDataPool *sync.Pool
	visitedPool  *sync.Pool
}

// Name returns the base path of the index.
func (idx *HNSWIndex) Name() string {
	return idx.config.IndexPath
}

var ErrActiveCountNotFound = errors.New("active count not found in index")

// getActiveCount reads the active count from PebbleDB
func (idx *HNSWIndex) getActiveCount(db pebble.Reader) (uint64, error) {
	key := makeActiveCountKey(idx.config.Name)
	value, closer, err := db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, ErrActiveCountNotFound
		}
		return 0, fmt.Errorf("failed to read active count: %w", err)
	}
	defer func() { _ = closer.Close() }()

	if len(value) != 8 {
		return 0, fmt.Errorf("invalid active count value size: %d", len(value))
	}

	return binary.LittleEndian.Uint64(value), nil
}

// setActiveCount writes the active count to PebbleDB
func (idx *HNSWIndex) setActiveCount(batch *pebble.Batch, count uint64) error {
	key := makeActiveCountKey(idx.config.Name)
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, count)
	return batch.Set(key, value, nil)
}

var ErrEntryPointNotFound = errors.New("entry point not found")

// getEntryPoint reads the entry point from PebbleDB
func (idx *HNSWIndex) getEntryPoint(db pebble.Reader) (uint64, error) {
	key := makeEntryPointKey(idx.config.Name)
	value, closer, err := db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, ErrEntryPointNotFound
		}
		return 0, fmt.Errorf("failed to read entry point: %w", err)
	}
	defer func() { _ = closer.Close() }()

	if len(value) != 8 {
		return 0, fmt.Errorf("invalid entry point value size: %d", len(value))
	}

	return binary.LittleEndian.Uint64(value), nil
}

// setEntryPoint writes the entry point to PebbleDB
func (idx *HNSWIndex) setEntryPoint(batch *pebble.Batch, entryPoint uint64) error {
	key := makeEntryPointKey(idx.config.Name)
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, entryPoint)
	return batch.Set(key, value, nil)
}

// NewHNSWIndex creates or loads a PebbleANN index.
// If the IndexPath exists and contains a valid index, it loads it.
// Otherwise, it initializes a new index structure on disk, including the PebbleDB store.
func NewHNSWIndex(config HNSWConfig, randSource rand.Source) (*HNSWIndex, error) {
	if config.IndexPath == "" {
		return nil, errors.New("indexPath must be provided in PebbleANNConfig")
	}
	if config.Dimension <= 0 {
		return nil, errors.New("dimension must be positive")
	}
	if config.Neighbors <= 0 {
		return nil, errors.New("neighbors (M) must be positive")
	}
	if config.DistanceMetric == 0 {
		config.DistanceMetric = vector.DistanceMetric_L2Squared // Default distance
	}
	if config.NeighborsHigherLayers <= 0 {
		config.NeighborsHigherLayers = config.Neighbors / 2
	}
	if config.LevelMultiplier <= 0 {
		// config.LevelMultiplier = 1.0 / math.Log(4)
		// Default as per HNSW paper
		// Default as per FAISS
		config.LevelMultiplier = 1.0 / math.Log(float64(config.NeighborsHigherLayers))
	}

	assignmentProbs := make([]float64, 0, 100)
	for i := range 100 {
		proba := math.Exp(
			-float64(i)/config.LevelMultiplier,
		) * (1 - math.Exp(-float64(1)/config.LevelMultiplier))
		if proba < math.Exp(-9) {
			break
		}
		// Precompute probabilities for layer assignment based on exponential decay
		assignmentProbs = append(assignmentProbs, proba)
	}

	idx := &HNSWIndex{
		config:          config,
		assignmentProbs: assignmentProbs,
		rand:            rand.New(randSource),           //nolint:gosec // G404: non-security randomness for ML/jitter
		cacheList:       list.New(),                     // Initialize LRU list
		nodeCache:       make(map[uint64]*list.Element), // Initialize cache map
		dbPath:          filepath.Join(config.IndexPath, pebbleDirname),
		nodeDataPool: &sync.Pool{
			New: func() any {
				return &HNSWNodeData{
					neighbors: make(map[int][]*PriorityItem, 4),
				}
			},
		},
		visitedPool: &sync.Pool{
			New: func() any {
				// Pre-allocate visited map with typical size based on EfConstruction
				return make(map[uint64]struct{}, config.EfConstruction)
			},
		},
	}

	// Initialize cache size if needed
	if idx.config.CacheSizeNodes == 0 {
		idx.config.CacheSizeNodes = 10000 // Default cache size if not specified
	}

	// Set Pebble Write Options
	idx.writeOpts = pebble.Sync
	if !config.PebbleSyncWrite {
		idx.writeOpts = pebble.NoSync
	}

	// Create index directory if it doesn't exist
	if err := os.MkdirAll(config.IndexPath, os.ModePerm); err != nil { //nolint:gosec // G301: standard permissions for data directory
		return nil, fmt.Errorf("failed to create index directory: %w", err)
	}

	// Open or create PebbleDB instance
	opts := idx.config.PebbleOptions
	if opts == nil {
		cache := pebble.NewCache(128 << 20) // 128 MB default cache
		defer cache.Unref()                 // Release our reference; pebble.Open retains its own
		opts = &pebble.Options{
			Logger: &logger.NoopLoggerAndTracer{},
			Cache:  cache,
			FS:     vfs.Default,
		}
		if config.PebbleMemOnly {
			opts.FS = vfs.NewMem()
		}
	}

	var err error
	if idx.db, err = pebble.Open(idx.dbPath, opts); err != nil {
		return nil, fmt.Errorf("opening pebble database at %s: %w", idx.dbPath, err)
	}

	// Check if index already exists in PebbleDB by looking for metadata
	if exists, err := idx.loadIndex(); err != nil {
		_ = idx.db.Close() // Close DB on error
		return nil, fmt.Errorf("loading index: %w", err)
	} else if exists {
		// Ensure loaded config matches provided config for critical parameters
		if idx.config.Dimension != config.Dimension {
			_ = idx.db.Close()
			return nil, fmt.Errorf("dimension mismatch index: %d, config: %d", idx.config.Dimension, config.Dimension)
		}

		// Update mutable settings from provided config
		idx.config.EfSearch = config.EfSearch
	} else {
		// Initialize new index
		if err := idx.initializeNewIndex(); err != nil {
			_ = idx.db.Close()
			return nil, fmt.Errorf("initializing index: %w", err)
		}
	}

	batch := idx.db.NewIndexedBatch()

	defer func() {
		_ = batch.Close()
	}()
	// Final consistency check
	if err := idx.checkConsistency(batch); err != nil {
		_ = idx.db.Close() // Close DB on error
		// log.Printf("Warning: Index consistency check failed after load/init: %v", err)
		return nil, fmt.Errorf("index consistency check failed: %w", err)
	}
	if batch.Len() > 0 {
		if err := batch.Commit(idx.writeOpts); err != nil {
			_ = idx.db.Close() // Close DB on error
			// log.Printf("Warning: Failed to commit index after checking consistency: %v", err)
			return nil, fmt.Errorf("index consistency check commit failed: %w", err)
		}
	}

	return idx, nil
}

// saveIndex persists the current index metadata to PebbleDB.
// Assumes Lock is held.
func (idx *HNSWIndex) saveIndex(batch *pebble.Batch) error {
	// Ensure derived counts are up-to-date
	meta := indexMetadata{
		Version:         indexVersion,
		Dimension:       idx.config.Dimension,
		M:               idx.config.Neighbors,
		MHigherLayers:   idx.config.NeighborsHigherLayers,
		EfConstruction:  idx.config.EfConstruction,
		LevelMultiplier: idx.config.LevelMultiplier,
	}

	// Serialize metadata
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(meta); err != nil {
		return fmt.Errorf("failed to encode index metadata: %w", err)
	}

	// Write to PebbleDB
	indexMetaKey := append(bytes.Clone(indexMetaPrefix), idx.config.Name...)
	if err := batch.Set(indexMetaKey, buf.Bytes(), nil); err != nil {
		return fmt.Errorf("failed to write index metadata to PebbleDB: %w", err)
	}

	return nil
}

// initializeNewIndex sets up a new index in PebbleDB.
// Assumes Lock is already held.
func (idx *HNSWIndex) initializeNewIndex() error {
	// Save initial metadata to PebbleDB
	batch := idx.db.NewIndexedBatch()
	defer func() { _ = batch.Close() }()

	// Initialize activeCount and entryPoint
	if err := idx.setActiveCount(batch, 0); err != nil {
		return fmt.Errorf("failed to set initial active count: %w", err)
	}
	if err := idx.setEntryPoint(batch, 0); err != nil {
		return fmt.Errorf("failed to set initial entry point: %w", err)
	}

	if err := idx.saveIndex(batch); err != nil {
		return fmt.Errorf("failed to save initial index metadata: %w", err)
	}
	if err := batch.Commit(idx.writeOpts); err != nil {
		return fmt.Errorf("failed to commit initial index metadata: %w", err)
	}

	return nil
}

// loadIndex reads index metadata from PebbleDB.
// Does NOT load tombstones (done separately in loadTombstones).
// Assumes that PebbleDB is already open.
func (idx *HNSWIndex) loadIndex() (bool, error) {
	indexMetaKey := append(bytes.Clone(indexMetaPrefix), idx.config.Name...)
	value, closer, err := idx.db.Get(indexMetaKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("failed to read index metadata from PebbleDB: %w", err)
	}
	defer func() { _ = closer.Close() }()

	// Make a copy of the value as the buffer might be reused
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	// Deserialize metadata
	buf := bytes.NewReader(valueCopy)
	dec := gob.NewDecoder(buf)
	var meta indexMetadata
	if err := dec.Decode(&meta); err != nil {
		return true, fmt.Errorf("failed to decode index metadata: %w", err)
	}

	// Validate version
	if meta.Version != indexVersion {
		return true, fmt.Errorf(
			"unsupported index version: stored has %d, expected %d",
			meta.Version,
			indexVersion,
		)
	}

	// Apply loaded metadata
	idx.config.Neighbors = meta.M
	idx.config.EfConstruction = meta.EfConstruction
	if meta.MHigherLayers > 0 {
		idx.config.NeighborsHigherLayers = meta.MHigherLayers
	}
	if meta.LevelMultiplier > 0 {
		idx.config.LevelMultiplier = meta.LevelMultiplier
	}
	// Note: EfSearch is intentionally NOT loaded, allowing it to be changed on startup
	return true, nil
}

// checkConsistency verifies internal counts against loaded data.
// Assumes Lock is held and PebbleDB is already open.
func (idx *HNSWIndex) checkConsistency(batch *pebble.Batch) error {
	activeCount, err := idx.getActiveCount(batch)
	if err != nil {
		return fmt.Errorf("failed to get active count: %w", err)
	}

	entryPoint, err := idx.getEntryPoint(batch)
	if err != nil {
		return fmt.Errorf("failed to get entry point: %w", err)
	}

	// 4. Check if entry point is valid (exists and not deleted)
	if activeCount > 0 { // Only check if there should be active nodes
		_, closer, err := batch.Get(makePebbleVectorKey(entryPoint))
		if err == nil {
			_ = closer.Close()
		} else if errors.Is(err, pebble.ErrNotFound) {
			iter, err := batch.NewIterWithContext(context.TODO(), &pebble.IterOptions{
				LowerBound: pebbleNodeKeyPrefix,
				UpperBound: utils.PrefixSuccessor(pebbleNodeKeyPrefix),
				SkipPoint: func(userKey []byte) bool {
					suffixToCheck := make([]byte, len(pebbleGraphKeySuffix)+1+4)
					copy(suffixToCheck, pebbleGraphKeySuffix)
					suffixToCheck[len(pebbleGraphKeySuffix)] = ':'
					binary.LittleEndian.PutUint32(suffixToCheck[len(pebbleGraphKeySuffix)+1:], uint32(0)) // Layer 0
					return len(userKey) < len(pebbleNodeKeyPrefix)+8 || !bytes.HasSuffix(userKey, suffixToCheck)
				},
			})
			if err != nil {
				return fmt.Errorf("error creating iterator: %w", err)
			}
			defer func() {
				_ = iter.Close()
			}()

			var entryPointValid bool
			if iter.First() {
				// FIXME (ajr): Set the entrypoint into the highest layer
				key := iter.Key()
				// Extract node ID from the key (after prefix)
				newEntryID := binary.BigEndian.Uint64(key[len(pebbleNodeKeyPrefix) : len(pebbleNodeKeyPrefix)+8])
				log.Printf("Info: Found new valid entry point: %d (replacing invalid %d)", newEntryID, entryPoint)
				// Save updated entry point
				if err := idx.setEntryPoint(batch, newEntryID); err != nil {
					log.Printf("Warning: Failed to save entry point after update: %v", err)
				}
				entryPointValid = true
			}

			if !entryPointValid {
				log.Printf("Error: Could not find valid entry point. Index may be unusable until new vectors are added.")
			}
		}

	}

	return nil
}

// releaseNodeData clears an HNSWNodeData and returns it to the pool.
func (idx *HNSWIndex) releaseNodeData(nd *HNSWNodeData) {
	if nd.neighbors != nil {
		for _, neighbors := range nd.neighbors {
			clear(neighbors)
		}
		clear(nd.neighbors)
	}
	nd.vector = nil
	nd.metadata = nil
	idx.nodeDataPool.Put(nd)
}

// Close saves final metadata and closes the PebbleDB instance.
func (idx *HNSWIndex) Close() error {
	var closeErrors []error

	// Save metadata before closing DB
	if idx.db != nil {
		// No need to save activeCount and entryPoint on close as they're already persisted

		// Close PebbleDB
		if err := idx.db.Close(); err != nil {
			closeErrors = append(
				closeErrors,
				fmt.Errorf("failed to close pebble database: %w", err),
			)
		}
		idx.db = nil
	}

	// Clear cache on close and return resources to pools
	for elem := idx.cacheList.Front(); elem != nil; {
		next := elem.Next()
		idx.releaseNodeData(elem.Value.(*HNSWNodeData))
		elem = next
	}
	idx.cacheList.Init()                           // Clear the list
	idx.nodeCache = make(map[uint64]*list.Element) // Clear the map
	idx.cacheHits = 0
	idx.cacheMisses = 0

	// No need to clean up visitedPool as maps will be garbage collected

	if len(closeErrors) > 0 {
		// Combine errors or return the first one
		return fmt.Errorf(
			"errors occurred during close: %w (and potentially others)",
			closeErrors[0],
		)
	}
	return nil
}

// Insert adds a single vector to the index.
func (idx *HNSWIndex) Insert(id uint64, vec vector.T, metadata []byte) error {
	return idx.BatchInsert(context.TODO(), []uint64{id}, []vector.T{vec}, [][]byte{metadata})
}

// assignLayer assigns a layer to a new node based on exponential decay probability
func (idx *HNSWIndex) assignLayer() int {
	f := idx.rand.Float64() // Ensure we have a fresh random number
	for level, proba := range idx.assignmentProbs {
		if f < proba {
			// If the random number is less than the probability, assign this layer
			return level
		}
		f -= proba // Decrease the probability by the current layer's probability
	}
	return len(idx.assignmentProbs) - 1 // If no layer was assigned, return the last layer
}

func (idx *HNSWIndex) InitializeNodeData(
	vecBuf []byte,
	batch *pebble.Batch,
	nodeLayer int,
	id uint64,
	vec []float32,
	metadata []byte,
) error {
	// 1. Serialize vector
	var err error
	vecBuf = vecBuf[:0]
	vecBuf, err = vector.Encode(vecBuf, vec)
	if err != nil {
		return fmt.Errorf("encoding vector for node %d: %w", id, err)
	}
	// Clone buffer to prevent aliasing when vecBuf is reused across iterations
	vecCopy := bytes.Clone(vecBuf)
	if err := batch.Set(makePebbleVectorKey(id), vecCopy, nil); err != nil {
		return fmt.Errorf("adding %d vector to batch: %w", id, err)
	}

	// 2. Store layer assignment
	layerBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(layerBuf, uint32(nodeLayer)) //nolint:gosec // G115: bounded value, cannot overflow in practice
	if err := batch.Set(makePebbleLayerKey(id), layerBuf, nil); err != nil {
		return fmt.Errorf("adding %d layer to batch: %w", id, err)
	}

	if len(metadata) > 0 {
		if err := batch.Set(makePebbleMetadataKey(id), metadata, nil); err != nil {
			return fmt.Errorf("adding %d metadata to batch: %w", id, err)
		}
		// TODO (ajr) If metadata changes we should remove the old metadata invert key
		if err := batch.Set(makePebbleMetadataInvertKey(metadata), binary.BigEndian.AppendUint64(nil, id), nil); err != nil {
			return fmt.Errorf("adding %d metadata invert to batch: %w", id, err)
		}
	}

	// 4. Add empty graph entries for each layer this node appears in
	for layer := 0; layer <= nodeLayer; layer++ {
		if err := batch.Set(makePebbleGraphLayerKey(id, layer), []byte{}, nil); err != nil {
			return fmt.Errorf(
				"adding graph entry for node %d layer %d to batch: %w",
				id,
				layer,
				err,
			)
		}
	}
	return nil
}

// BatchInsert adds multiple vectors. More efficient for bulk loading.
func (idx *HNSWIndex) BatchInsert(
	ctx context.Context,
	ids []uint64,
	vectors []vector.T,
	metadataList [][]byte,
) error {
	if len(vectors) == 0 {
		return nil
	}

	if metadataList != nil && len(metadataList) != len(vectors) {
		return errors.New("metadata list length must match vectors length")
	}
	if len(ids) != len(vectors) {
		return errors.New("ids length must match vectors length")
	}

	if idx.db == nil {
		return errors.New("index database is not open for writing")
	}

	// Create a single batch for the entire operation
	// idx.config.Dimension * 4 bytes per float32 * len(vecctors) * 8 bytes per uint64 per neighbor * idx.config.Neighbors
	batch := idx.db.NewIndexedBatchWithSize(
		32 << 20,
	) // 32MB Initial size estimate, will grow as needed
	defer func() {
		_ = batch.Close()
	}()

	numVectors := len(vectors)
	if numVectors > math.MaxUint32 {
		return fmt.Errorf(
			"too many vectors to insert: %d exceeds maximum %d",
			numVectors,
			math.MaxUint32,
		)
	}

	// Get current active count
	activeCount, err := idx.getActiveCount(batch)
	if err != nil {
		return fmt.Errorf("failed to get active count: %w", err)
	}

	// Track successful insertions to update active count accurately
	insertedCount := uint64(0)

	vecBuf := make([]byte, 4*idx.config.Dimension)

	var maxLayer int
	var entryPoint uint64
	var entryPointNeighbors map[int][]*PriorityItem
	if activeCount == 0 {
		// Assign layer for this node
		nodeLayer := idx.assignLayer()
		if err := idx.setEntryPoint(batch, ids[0]); err != nil {
			return fmt.Errorf("setting entry point for new node %d: %w", ids[0], err)
		}
		entryPoint = ids[0] // Update entryPoint to the new node

		var metadata []byte
		if len(metadataList) > 0 {
			metadata = metadataList[0]
		}
		if err := idx.InitializeNodeData(vecBuf, batch, nodeLayer, ids[0], vectors[0], metadata); err != nil {
			return fmt.Errorf("initializing node data for %d: %w", ids[0], err)
		}
		insertedCount++
		maxLayer = nodeLayer // Set maxLayer to the layer of the first node

		// Process remaining vectors (skip the first one)
		vectors = vectors[1:] // Remove first vector since it's already processed
		ids = ids[1:]         // Remove first ID since it's already processed
		if len(metadataList) > 0 {
			metadataList = metadataList[1:] // Remove first metadata since it's already processed
		}
	} else {
		var err error
		entryPoint, err = idx.getEntryPoint(batch)
		if err != nil {
			return fmt.Errorf("getting entry point: %w", err)
		}
		entryPointData, err := idx.getNodeData(batch, entryPoint)
		if err != nil {
			return fmt.Errorf("getting node data for entry point %d: %w", entryPoint, err)
		}
		maxLayer = entryPointData.layer
		entryPointNeighbors = entryPointData.neighbors
	}

	buf := bytes.NewBuffer(nil)
	for i, vec := range vectors {
		if ids[i] == 0 {
			return fmt.Errorf("vector at index %d has zero ID, which is not allowed", i)
		}
		vecLen := len(vec)
		if vecLen > math.MaxUint32 {
			return fmt.Errorf(
				"vector at index %d exceeds maximum length of %d: got %d",
				i,
				math.MaxUint32,
				vecLen,
			)
		}
		if uint32(vecLen) != idx.config.Dimension {
			return fmt.Errorf("vector at index %d has wrong dimension: expected %d, got %d",
				i, idx.config.Dimension, vecLen)
		}
		if entryPoint == ids[i] {
			// return fmt.Errorf("entry point %d is the same as the new node ID %d", entryPoint, ids[i])
			// We're updating the entryPoint doc here so find a new entryPoint
			if len(entryPointNeighbors) == 0 {
				// If we have no neighbors we're the only node in the index so just update
				clear(vecBuf)
				vecBuf, err = vector.Encode(vecBuf, vec)
				if err != nil {
					return fmt.Errorf("encoding vector for node %d: %w", ids[i], err)
				}
				vecKey := makePebbleVectorKey(ids[i])
				// Clone buffer to prevent aliasing when vecBuf is reused
				vecCopy := bytes.Clone(vecBuf)
				if err := batch.Set(vecKey, vecCopy, nil); err != nil {
					return fmt.Errorf("adding vector %d to batch: %w", i, err)
				}
				vecBuf = vecBuf[:0]
				if metadataList != nil && len(metadataList[i]) > 0 {
					if err := batch.Set(makePebbleMetadataKey(ids[i]), metadataList[i], nil); err != nil {
						return fmt.Errorf("adding metadata %d to batch: %w", i, err)
					}
					if err := batch.Set(makePebbleMetadataInvertKey(metadataList[i]), binary.BigEndian.AppendUint64(nil, ids[i]), nil); err != nil {
						return fmt.Errorf("adding metadata invert %d to batch: %w", i, err)
					}
				}
				continue
			}
			for layer := maxLayer; layer >= 0; layer-- {
				if len(entryPointNeighbors[layer]) == 0 {
					continue
				}
				if err := idx.setEntryPoint(batch, entryPointNeighbors[layer][0].ID); err != nil {
					return fmt.Errorf("failed to update entry point: %w", err)
				}
				entryPoint = entryPointNeighbors[layer][0].ID // Update entryPoint to the first neighbor
				maxLayer = layer                              // Update maxLayer to the layer of the new entry point
				break                                         // Use the highest-layer neighbor
			}
		}

		// Assign layer for this node
		nodeLayer := idx.assignLayer()
		graphUpdateEntryPoint := entryPoint
		graphUpdateEntryPointLayer := maxLayer
		if nodeLayer > maxLayer {
			if err := idx.setEntryPoint(batch, ids[i]); err != nil {
				return fmt.Errorf("setting entry point for new node %d: %w", ids[i], err)
			}
			entryPoint = ids[i]  // Update entryPoint to the new node
			maxLayer = nodeLayer // Update maxLayer to the new node's layer
		}

		var metadata []byte
		if len(metadataList) > 0 {
			metadata = metadataList[i]
		}
		if err := idx.InitializeNodeData(vecBuf, batch, nodeLayer, ids[i], vectors[i], metadata); err != nil {
			return fmt.Errorf("initializing node data for %d: %w", ids[i], err)
		}
		insertedCount++
		if err := idx.updateGraphForNode(buf, batch, graphUpdateEntryPoint, graphUpdateEntryPointLayer, min(nodeLayer, graphUpdateEntryPointLayer), ids[i], vectors[i]); err != nil {
			log.Printf(
				"Warning: Error updating graph for node %d (%d/%d in batch): %v. Continuing...",
				ids[i],
				i+1,
				len(ids),
				err,
			)
		}
	}

	// Set active count based on actual successful insertions
	newActiveCount := activeCount + insertedCount
	if err := idx.setActiveCount(batch, newActiveCount); err != nil {
		return fmt.Errorf("failed to update active count: %w", err)
	}

	// Save updated metadata
	if err := idx.saveIndex(batch); err != nil {
		return fmt.Errorf("saving index metadata: %w", err)
	}

	// Commit the entire batch atomically
	if err := batch.Commit(idx.writeOpts); err != nil {
		return fmt.Errorf("committing batch insert: %w", err)
	}

	return nil
}

// Search finds k nearest neighbors for the query vector using HNSW algorithm.
func (idx *HNSWIndex) Search(req *SearchRequest) ([]*Result, error) {
	// FIXME (ajr) Need to implement filtering and excluding
	query := req.Embedding
	k := req.K
	filterPrefix := req.FilterPrefix
	queryLen := len(query)
	if queryLen > math.MaxUint32 {
		return nil, fmt.Errorf(
			"query vector exceeds maximum length of %d: got %d",
			math.MaxUint32,
			queryLen,
		)
	}
	if uint32(queryLen) != idx.config.Dimension {
		return nil, fmt.Errorf("query dimension mismatch: expected %d, got %d",
			idx.config.Dimension, queryLen)
	}

	// Only lock to safely read the db pointer
	idx.RLock()
	db := idx.db
	idx.RUnlock()

	if db == nil {
		return nil, errors.New("index database is not open")
	}
	activeCount, err := idx.getActiveCount(db)
	if err != nil {
		return nil, fmt.Errorf("failed to get active count: %w", err)
	}

	if activeCount == 0 {
		return nil, nil // Empty index
	}

	entryPoint, err := idx.getEntryPoint(db)
	if err != nil {
		return nil, fmt.Errorf("failed to get entry point: %w", err)
	}

	// If we have a filter prefix, decide between graph search and direct iteration
	if len(filterPrefix) > 0 && !idx.config.DirectFiltering {
		// Count entries with the prefix
		prefixCount, err := idx.countMetadataPrefix(filterPrefix)
		if err != nil {
			return nil, fmt.Errorf("failed to count metadata prefix: %w", err)
		}
		if prefixCount == 0 {
			return nil, nil // No entries match the prefix
		}

		// Heuristic: if k is >= 20% of matching entries, or if matching entries are < 100,
		// do direct iteration instead of graph search
		// This avoids inefficient graph traversal when we need most of the filtered results anyway
		if float64(k) >= 0.2*float64(prefixCount) || prefixCount < 100 {
			return idx.searchDirect(query, k, filterPrefix)
		}
	}

	// Get the layer of the entry point
	entryPointLayer, err := idx.getNodeLayer(db, entryPoint)
	if err != nil {
		return nil, fmt.Errorf("failed to get entry point layer: %w", err)
	}

	// Start search from the top layer
	currentNearest := entryPoint
	var prev *PriorityQueue
	minDist := float32(math.MaxFloat32)
	for layer := entryPointLayer; layer > 0; layer-- {
		// Search at current layer with ef=1 to find nearest point
		var err error
		prev, err = idx.searchLayerPebbleInternal(
			nil,
			query,
			nil,
			currentNearest,
			int(idx.config.EfSearch),
			layer,
			nil,
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf("search failed at layer %d: %w", layer, err)
		}
		if prev.Len() > 0 {
			nearest := prev.Items(1)[0]
			if nearest.Distance > minDist {
				return nil, fmt.Errorf(
					"invariant violated: candidate at layer %d with distance %f > minimum distance %f",
					layer, nearest.Distance, minDist,
				)
			}
			currentNearest = nearest.ID
			minDist = nearest.Distance
		}
	}

	// Search at layer 0 with full ef
	pqueue, err := idx.searchLayerPebbleInternal(
		nil,
		query,
		filterPrefix,
		currentNearest,
		max(int(idx.config.EfSearch), k),
		0,
		nil,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("search failed at layer 0: %w", err)
	}
	candidateItems := pqueue.Items(k)

	// 3. Extract top K results from the candidates found
	results := make([]*Result, len(candidateItems))

	// The candidateItems are already sorted by distance ascending
	for i, item := range candidateItems {
		results[i] = &Result{
			ID:       item.ID,
			Distance: item.Distance,
			Metadata: item.Metadata,
		}
	}

	return results, nil
}

func (idx *HNSWIndex) DeleteByMetadata(key []byte) error {
	if idBytes, closer, err := idx.db.Get(makePebbleMetadataInvertKey(key)); err == nil {
		id := binary.BigEndian.Uint64(idBytes)
		_ = closer.Close()
		if err := idx.Delete(id); err != nil {
			return fmt.Errorf("failed to add metadata invert delete to batch: %w", err)
		}
	}
	return nil
}

// Delete marks a node as deleted (soft delete).
func (idx *HNSWIndex) Delete(ids ...uint64) error {
	// log.Printf("Attempting to delete node %d", id)

	// --- Pre-checks ---
	if idx.db == nil {
		return errors.New("index database is not open for deletion")
	}
	// --- Start transaction via PebbleDB batch ---
	batch := idx.db.NewIndexedBatchWithSize(1 << 20)
	defer func() { _ = batch.Close() }()

	buf := bytes.NewBuffer(nil)
	for _, id := range ids {
		// --- Read node data BEFORE marking deleted (needed for cleanup) ---
		nodeData, err := idx.getNodeData(batch, id)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			// Other read errors are more problematic
			return fmt.Errorf("failed to read node data for cleanup of node %d: %w", id, err)
		}

		if len(nodeData.metadata) > 0 {
			if err := batch.Delete(makePebbleMetadataInvertKey(nodeData.metadata), nil); err != nil {
				return fmt.Errorf("failed to add metadata invert delete to batch: %w", err)
			}
		}
		lower, upper := makePebbleNodeRange(id)
		if err := batch.DeleteRange(lower, upper, nil); err != nil {
			return fmt.Errorf("failed to add tombstone for ID %d to batch: %w", id, err)
		}

		// Handle entry point replacement before neighbor cleanup.
		// Select replacement from the highest layer neighbor.
		entryPoint, err := idx.getEntryPoint(batch)
		if err != nil {
			log.Printf("Warning: Failed to get entry point during deletion: %v", err)
		} else if entryPoint == id {
			replaced := false
			for layer := nodeData.layer; layer >= 0 && !replaced; layer-- {
				for _, neighbor := range nodeData.neighbors[layer] {
					if err := idx.setEntryPoint(batch, neighbor.ID); err != nil {
						log.Printf("Warning: Failed to update entry point during deletion: %v", err)
					} else {
						replaced = true
						break
					}
				}
			}
		}

		// Update neighbor nodes to remove references to this node at all layers
		for layer, layerNeighbors := range nodeData.neighbors {
			for _, neighbor := range layerNeighbors {
				// Read current neighbors of the neighbor node at this layer
				currentNeighborNeighbors, neighborReadErr := idx.readNodeNeighbors(
					batch,
					neighbor.ID,
					layer,
				)
				if neighborReadErr != nil {
					if !errors.Is(neighborReadErr, ErrNotFound) {
						log.Printf("Warning: Failed to read neighbors for neighbor %d at layer %d during cleanup: %v. Skipping edge removal.",
							neighbor.ID, layer, neighborReadErr)
					}
					continue
				}

				// Remove the deleted node from neighbor's connections
				i := slices.IndexFunc(currentNeighborNeighbors, func(item *PriorityItem) bool {
					return item.ID == id
				})
				if i == -1 {
					continue
				}
				newNeighbors := slices.Delete(currentNeighborNeighbors, i, i+1)

				// Save updated connections
				buf.Reset()
				if err := serializeNeighbors(buf, newNeighbors); err != nil {
					log.Printf(
						"Warning: Failed to serialize updated neighbors for neighbor %d at layer %d: %v. Skipping.",
						neighbor.ID,
						layer,
						err,
					)
					continue
				} else if err := batch.Set(makePebbleGraphLayerKey(neighbor.ID, layer), buf.Bytes(), nil); err != nil {
					log.Printf("Warning: Failed to add update for neighbor %d at layer %d to batch: %v. Skipping.",
						neighbor.ID, layer, err)
					continue
				}
				idx.updateCache(neighbor.ID, layer, newNeighbors)
			}
		}

		idx.invalidateCache(id)

		// Update active count
		activeCount, err := idx.getActiveCount(batch)
		if err != nil {
			return fmt.Errorf("failed to get active count during delete: %w", err)
		}
		if activeCount > 0 {
			if err := idx.setActiveCount(batch, activeCount-1); err != nil {
				return fmt.Errorf("failed to update active count during delete: %w", err)
			}
		}
	}

	// Consistency check and metadata save once after all deletions
	if err := idx.checkConsistency(batch); err != nil {
		return fmt.Errorf("index consistency check failed during delete: %w", err)
	}
	if err := idx.saveIndex(batch); err != nil {
		return fmt.Errorf("saving index metadata after delete: %w", err)
	}
	if batch.Len() > 0 {
		// 3. Commit the entire batch atomically
		if err := batch.Commit(idx.writeOpts); err != nil {
			return fmt.Errorf("failed to commit delete transaction: %w", err)
		}
	}

	return nil
}

func (idx *HNSWIndex) readMetadata(db pebble.Reader, id uint64) (metadata []byte, err error) {
	if idx.db == nil {
		return nil, errors.New("database is not open")
	}
	// --- Read Metadata from PebbleDB if exists ---
	metadataBytes, metaCloser, err := db.Get(makePebbleMetadataKey(id))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			// No metadata for this vector, return empty slice
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to read metadata for ID %d: %w", id, err)
	}

	metadata = bytes.Clone(metadataBytes)
	return metadata, metaCloser.Close()
}

// GetVector retrieves a vector and its associated user metadata by ID from PebbleDB.
func (idx *HNSWIndex) GetMetadata(id uint64) (metadata []byte, err error) {
	return idx.readMetadata(idx.db, id)
}

func (idx *HNSWIndex) TotalVectors() uint64 {
	idx.RLock()
	defer idx.RUnlock()

	// Return the current active count of nodes
	activeCount, err := idx.getActiveCount(idx.db)
	if err != nil {
		// Log error but return 0 to avoid breaking the interface
		log.Printf("Error getting active count in TotalVectors: %v", err)
		return 0
	}
	return activeCount
}

// Stats returns statistics about the PebbleDB-based index.
func (idx *HNSWIndex) Stats() map[string]any {
	idx.RLock()
	defer idx.RUnlock()

	activeCount, _ := idx.getActiveCount(idx.db)
	entryPoint, _ := idx.getEntryPoint(idx.db)

	stats := map[string]any{
		"implementation": "PebbleANN",
		"dimension":      idx.config.Dimension,
		"index_path":     idx.config.IndexPath,
		"db_path":        idx.dbPath,
		"nodes_active":   activeCount,
		"entry_point":    entryPoint,
		"config_M":       idx.config.Neighbors,
		"config_EfC":     idx.config.EfConstruction,
		"config_EfS":     idx.config.EfSearch,
		"sync_writes":    idx.config.PebbleSyncWrite,
	}

	// PebbleDB statistics
	if idx.db != nil {
		// Count nodes by key prefix types
		// 		stats["vectors_count"] = idx.countKeysByPrefix(vectorKeyPrefix)
		// 		stats["graph_edges_count"] = idx.countKeysByPrefix(pebbleGraphKeyPrefix)
		// 		stats["metadata_count"] = idx.countKeysByPrefix(metadataKeyPrefix)
		// 		stats["tombstones_count"] = idx.countKeysByPrefix(deletedKeyPrefix)
		//
		// 		// Calculate approximate disk usage by key type
		// 		stats["vectors_bytes"] = idx.estimateSizeByPrefix(vectorKeyPrefix)
		// 		stats["graph_bytes"] = idx.estimateSizeByPrefix(pebbleGraphKeyPrefix)
		// 		stats["metadata_bytes"] = idx.estimateSizeByPrefix(metadataKeyPrefix)

		// Pebble DB internal metrics
		metrics := idx.db.Metrics()
		stats["pebble_block_cache_hits"] = metrics.BlockCache.Hits
		stats["pebble_block_cache_misses"] = metrics.BlockCache.Misses
		stats["pebble_block_cache_size_bytes"] = metrics.BlockCache.Size
		stats["pebble_block_cache_count"] = metrics.BlockCache.Count
		stats["pebble_levels"] = len(metrics.Levels)
		// stats["pebble_disk_space_bytes"] = metrics.Total().TablesSize
		stats["pebble_disk_bytes_in"] = metrics.WAL.BytesIn
		stats["pebble_wal_bytes"] = metrics.WAL.Size
	}

	// LRU Cache statistics
	stats["cache_size_nodes_config"] = idx.config.CacheSizeNodes
	stats["cache_count_current"] = idx.cacheList.Len()
	stats["cache_hits"] = idx.cacheHits
	stats["cache_misses"] = idx.cacheMisses
	if idx.cacheHits+idx.cacheMisses > 0 {
		stats["cache_hit_rate"] = float64(idx.cacheHits) / float64(idx.cacheHits+idx.cacheMisses)
	} else {
		stats["cache_hit_rate"] = 0.0
	}

	// Add layer distribution stats
	layerCounts := idx.getLayerCounts()
	stats["layer_distribution"] = layerCounts
	stats["max_layer"] = len(layerCounts) - 1

	return stats
}

// getLayerCounts returns the count of nodes at each layer
func (idx *HNSWIndex) getLayerCounts() map[int]int {
	layerCounts := make(map[int]int)

	iter, err := idx.db.NewIterWithContext(context.TODO(), &pebble.IterOptions{
		LowerBound: pebbleNodeKeyPrefix,
		UpperBound: utils.PrefixSuccessor(pebbleNodeKeyPrefix),
		SkipPoint: func(userKey []byte) bool {
			return !bytes.HasSuffix(userKey, pebbleLayerKeySuffix)
		},
	})
	if err != nil {
		return layerCounts
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		value := iter.Value()
		if len(value) == 4 {
			layer := int(binary.LittleEndian.Uint32(value))
			// Count this node in all layers from 0 to its assigned layer
			for l := 0; l <= layer; l++ {
				layerCounts[l]++
			}
		}
	}

	return layerCounts
}

// getNodeLayer retrieves the layer assignment for a node
func (idx *HNSWIndex) getNodeLayer(db pebble.Reader, id uint64) (int, error) {
	layerData, closer, err := db.Get(makePebbleLayerKey(id))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			// For backward compatibility, nodes without layer info are at layer 0
			return 0, nil
		}
		return 0, fmt.Errorf("failed to read layer for ID %d: %w", id, err)
	}
	defer func() { _ = closer.Close() }()

	if len(layerData) != 4 {
		return 0, fmt.Errorf("invalid layer data size for ID %d: %d", id, len(layerData))
	}

	return int(binary.LittleEndian.Uint32(layerData)), nil
}

// countMetadataPrefix counts the number of metadata entries with a specific prefix
// Assumes RLock is held.
func (idx *HNSWIndex) countMetadataPrefix(prefix []byte) (int, error) {
	if len(prefix) == 0 {
		return 0, nil
	}

	count := 0
	p := makePebbleMetadataInvertKey(prefix)
	iter, err := idx.db.NewIterWithContext(context.TODO(), &pebble.IterOptions{
		LowerBound: p,
		UpperBound: utils.PrefixSuccessor(p),
	})
	if err != nil {
		return 0, fmt.Errorf("failed to create iterator: %w", err)
	}
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}
	return count, iter.Close()
}

// searchDirect performs a direct iteration search over metadata entries with a prefix
// This is more efficient than graph search when k is large relative to the number of matching entries
// Assumes RLock is held.
func (idx *HNSWIndex) searchDirect(
	query []float32,
	k int,
	filterPrefix []byte,
) ([]*Result, error) {
	// Priority queue to keep top k results
	results := NewPriorityQueue(true, k) // Max-heap for keeping closest k

	iter, err := idx.db.NewIterWithContext(context.TODO(), &pebble.IterOptions{
		LowerBound: pebbleNodeKeyPrefix,
		UpperBound: utils.PrefixSuccessor(pebbleNodeKeyPrefix),
		SkipPoint: func(userKey []byte) bool {
			return !bytes.HasSuffix(userKey, pebbleMetadataKeySuffix)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		metadataValue := iter.Value()

		// Check if metadata matches the filter prefix
		if len(filterPrefix) > 0 && !bytes.HasPrefix(metadataValue, filterPrefix) {
			continue
		}

		// Extract node ID from the key
		key := iter.Key()
		if len(key) < len(pebbleNodeKeyPrefix)+8 {
			continue // Invalid key
		}
		nodeID := binary.BigEndian.Uint64(
			key[len(pebbleNodeKeyPrefix) : len(pebbleNodeKeyPrefix)+8],
		)

		// Read the vector for this node
		vec, err := idx.readVector(idx.db, nodeID)
		if err != nil {
			// Skip nodes where vector can't be read (might be corrupted or deleted)
			continue
		}

		// Calculate distance
		distance := vector.MeasureDistance(idx.config.DistanceMetric, query, vec)

		// Add to results heap
		if results.Len() < k {
			heap.Push(results, &PriorityItem{
				ID:       nodeID,
				Distance: distance,
				Metadata: metadataValue,
			})
		} else if distance < results.Peek().Distance {
			// If this item is closer than the worst in our heap, replace it
			heap.Pop(results)
			heap.Push(results, &PriorityItem{
				ID:       nodeID,
				Distance: distance,
				Metadata: metadataValue,
			})
		}
	}

	// Extract final results from the heap
	finalResults := make([]*Result, results.Len())
	for i := len(finalResults) - 1; i >= 0; i-- {
		item := heap.Pop(results).(*PriorityItem)
		finalResults[i] = &Result{
			ID:       item.ID,
			Distance: item.Distance,
			Metadata: item.Metadata,
		}
	}

	return finalResults, nil
}

// serializeNeighbors converts a slice of neighbor IDs into a byte slice.
func serializeNeighbors(buf *bytes.Buffer, neighbors []*PriorityItem) error {
	numNeighbors := len(neighbors)
	if numNeighbors == 0 {
		return nil // Return empty slice for no neighbors
	}
	if numNeighbors > math.MaxUint32 {
		return fmt.Errorf("too many neighbors: %d exceeds maximum %d", numNeighbors, math.MaxUint32)
	}
	count := uint32(numNeighbors)
	if err := binary.Write(buf, binary.LittleEndian, count); err != nil {
		return fmt.Errorf("failed to write neighbor count: %w", err)
	}
	for _, n := range neighbors {
		if err := binary.Write(buf, binary.LittleEndian, n.ID); err != nil {
			return fmt.Errorf("failed to write neighbor id %d: %w", n.ID, err)
		}
		// FIXME (ajr) This distance could be out of data if the neighbor gets updated
		// or the id could become invalid after a deletion
		if err := binary.Write(buf, binary.LittleEndian, n.Distance); err != nil {
			return fmt.Errorf("failed to write neighbor distance %f: %w", n.Distance, err)
		}
	}
	return nil
}

// deserializeNeighborsWithPool converts a byte slice back into a slice of neighbor IDs using a pool.
func (idx *HNSWIndex) deserializeNeighborsWithPool(data []byte) ([]*PriorityItem, error) {
	if len(data) == 0 {
		// Return an empty slice without using the pool
		return []*PriorityItem{}, nil
	}
	reader := bytes.NewReader(data)

	// Optional: Read count first if serializeNeighbors writes it
	var count uint32
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
		return nil, fmt.Errorf("failed to read neighbor count: %w", err)
	}

	neighbors := make([]*PriorityItem, count)
	for i := range neighbors {
		if neighbors[i] == nil {
			neighbors[i] = &PriorityItem{}
		}
		if err := binary.Read(reader, binary.LittleEndian, &neighbors[i].ID); err != nil {
			// Check for unexpected EOF
			if err == io.ErrUnexpectedEOF || err == io.EOF {
				return nil, fmt.Errorf(
					"unexpected end of data while reading neighbor %d of %d: %w",
					i+1,
					count,
					err,
				)
			}
			return nil, fmt.Errorf("failed to read neighbor ID at index %d: %w", i, err)
		}
		if err := binary.Read(reader, binary.LittleEndian, &neighbors[i].Distance); err != nil {
			// Check for unexpected EOF
			if err == io.ErrUnexpectedEOF || err == io.EOF {
				return nil, fmt.Errorf(
					"unexpected end of data while reading neighbor %d of %d: %w",
					i+1,
					count,
					err,
				)
			}
			return nil, fmt.Errorf("failed to read neighbor ID at index %d: %w", i, err)
		}
	}
	return neighbors, nil
}

// readVector reads a specific vector from PebbleDB.
// Assumes RLock is held.
func (idx *HNSWIndex) readVector(db pebble.Reader, id uint64) ([]float32, error) {
	if db == nil {
		return nil, errors.New("database is not open")
	}

	// Read vector data from PebbleDB
	vectorData, closer, err := db.Get(makePebbleVectorKey(id))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, fmt.Errorf("vector data for ID %d not found", id)
		}
		return nil, fmt.Errorf("failed to read vector data for ID %d from database: %w", id, err)
	}
	defer func() { _ = closer.Close() }()

	_, vec, err := vector.Decode(vectorData)
	return vec, err
}

// readNodeNeighbors reads neighbor IDs for a node from PebbleDB at a specific layer.
// For backward compatibility, if layer is -1, it reads from the old non-layered key.
// Assumes RLock is held.
func (idx *HNSWIndex) readNodeNeighbors(
	db pebble.Reader,
	id uint64,
	layer int,
) ([]*PriorityItem, error) {
	if idx.db == nil {
		return nil, errors.New("database is not open")
	}

	key := makePebbleGraphLayerKey(id, layer)
	value, closer, err := db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			// Return ErrNotFound to signal the key wasn't found. Caller can decide how to handle.
			return nil, ErrNotFound // Propagate pebble.ErrNotFound
		}
		return nil, fmt.Errorf("failed to get neighbors for node %d from pebble: %w", id, err)
	}

	defer func() { _ = closer.Close() }()
	neighbors, err := idx.deserializeNeighborsWithPool(value)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize neighbors for node %d: %w", id, err)
	}
	return neighbors, nil
}

// getNodeData retrieves vector and neighbors for a node, using the cache.
// Reads vector from file and neighbors from PebbleDB if not cached.
// Assumes RLock is held.
func (idx *HNSWIndex) getNodeData(batch *pebble.Batch, id uint64) (*HNSWNodeData, error) {
	var db pebble.Reader = idx.db
	if batch != nil {
		db = batch
	}
	// Check cache first
	idx.cacheMu.Lock()
	if elem, found := idx.nodeCache[id]; found {
		idx.cacheHits++
		idx.cacheList.MoveToFront(elem) // Mark as recently used
		idx.cacheMu.Unlock()
		return elem.Value.(*HNSWNodeData), nil
	}
	idx.cacheMisses++
	idx.cacheMu.Unlock()

	// Use iterator to scan all keys for this node
	lower, upper := makePebbleNodeRange(id)
	iter, err := db.NewIterWithContext(context.TODO(), &pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator for node %d: %w", id, err)
	}
	defer func() {
		_ = iter.Close()
	}()

	// Get nodeData from pool
	nodeData := idx.nodeDataPool.Get().(*HNSWNodeData)
	nodeData.id = id // Store the ID within the cached data
	// Clear the neighbors map
	for k := range nodeData.neighbors {
		delete(nodeData.neighbors, k)
	}

	var foundVector bool
	var foundAnyGraph bool

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		// Determine key type by suffix
		if bytes.HasSuffix(key, pebbleVectorKeySuffix) {
			_, vec, err := vector.Decode(value)
			if err != nil {
				return nil, fmt.Errorf("decoding vector for node %d: %w", id, err)
			}
			nodeData.vector = vec
			foundVector = true
		} else if bytes.HasSuffix(key[:len(key)-5], pebbleGraphKeySuffix) {
			layer := int(binary.BigEndian.Uint32(key[len(key)-4:]))
			neighbors, err := idx.deserializeNeighborsWithPool(value)
			if err != nil {
				idx.releaseNodeData(nodeData)
				return nil, fmt.Errorf("failed to deserialize neighbors for node %d layer %d: %w", id, layer, err)
			}
			nodeData.neighbors[layer] = neighbors
			if layer > nodeData.layer {
				nodeData.layer = layer
			}
			foundAnyGraph = true
		} else if bytes.HasSuffix(key, pebbleMetadataKeySuffix) {
			// Metadata (optional)
			nodeData.metadata = bytes.Clone(value)
		} else if bytes.HasSuffix(key, pebbleLayerKeySuffix) {
			// Layer assignment
			if len(value) == 4 {
				nodeData.layer = int(binary.LittleEndian.Uint32(value))
			}
		}
	}

	if err := iter.Error(); err != nil {
		idx.releaseNodeData(nodeData)
		return nil, fmt.Errorf("iterator error for node %d: %w", id, err)
	}

	// Check if we found the required data
	if !foundVector || !foundAnyGraph {
		idx.releaseNodeData(nodeData)
		return nil, ErrNotFound
	}

	// Add to LRU cache (if enabled) and handle eviction
	if idx.config.CacheSizeNodes > 0 && nodeData.layer >= 0 {
		idx.cacheMu.Lock()
		// Re-check cache: another goroutine may have inserted this node
		// while we were reading from Pebble.
		if elem, found := idx.nodeCache[id]; found {
			idx.cacheList.MoveToFront(elem)
			idx.cacheMu.Unlock()
			idx.releaseNodeData(nodeData)
			return elem.Value.(*HNSWNodeData), nil
		}
		// Evict LRU item if cache is full
		if idx.cacheList.Len() >= idx.config.CacheSizeNodes {
			if lruElement := idx.cacheList.Back(); lruElement != nil {
				evictedData := idx.cacheList.Remove(lruElement).(*HNSWNodeData)
				delete(idx.nodeCache, evictedData.id)
				idx.releaseNodeData(evictedData)
			}
		}
		// Add new item to front of list and map
		element := idx.cacheList.PushFront(nodeData)
		idx.nodeCache[id] = element
		idx.cacheMu.Unlock()
	}

	return nodeData, nil
}

func (idx *HNSWIndex) GetClosestEntryAtLayer(
	batch *pebble.Batch,
	query []float32,
	entryPoint uint64,
	distance float32,
	layer int,
	visited map[uint64]struct{},
) (uint64, float32, error) {
	epData, err := idx.getNodeData(batch, entryPoint) // Fetches vector + neighbors (from Pebble)
	if err != nil {
		return 0, 0, fmt.Errorf(
			"failed to get entry point data for ID %d in search: %w",
			entryPoint,
			err,
		)
	}
	results := entryPoint
	if distance == math.MaxFloat32 {
		// Calculate distance to entry point
		distance = vector.MeasureDistance(idx.config.DistanceMetric, query, epData.vector)
	}
	for _, neighbor := range epData.neighbors[layer] {
		if _, isVisited := visited[neighbor.ID]; isVisited {
			continue // Already processed this node
		}
		visited[neighbor.ID] = struct{}{} // Mark as visited
		neighborData, err := idx.getNodeData(batch, neighbor.ID)
		if err != nil {
			if !errors.Is(err, ErrNotFound) {
				return 0, 0, fmt.Errorf(
					"failed to get entry point data for ID %d in search: %w",
					entryPoint,
					err,
				)
			}
			continue
		}
		neighborDist := vector.MeasureDistance(
			idx.config.DistanceMetric,
			query,
			neighborData.vector,
		)
		if neighborDist < distance {
			// If this neighbor is closer than the current best, update results
			results = neighborData.id
			distance = neighborDist
		}
	}
	return results, distance, nil
}

// searchLayerPebbleInternal performs graph traversal using PebbleDB to find candidate neighbors at a specific layer.
// Returns a sorted list of the top 'ef' closest items found.
// Assumes RLock is held.
// , candidates *PriorityQueue, results *PriorityQueue, visited map[uint64]struct{}
func (idx *HNSWIndex) searchLayerPebbleInternal(
	batch *pebble.Batch,
	query []float32,
	filterPrefix []byte,
	entryPoint uint64,
	ef int,
	layer int,
	prev *PriorityQueue,
	visited map[uint64]struct{},
) (*PriorityQueue, error) {
	// candidates: Min-heap storing distances of nodes to visit.
	// results: Max-heap storing distances (keeps track of the 'ef' furthest among the closest found so far).
	var candidates *PriorityQueue
	if prev != nil {
		candidates = prev.Clone(false)
	} else {
		candidates = NewPriorityQueue(false, ef) // Min-heap
	}
	var results *PriorityQueue
	if prev != nil {
		results = prev
	} else {
		results = NewPriorityQueue(true, ef) // Max-heap
	}
	if visited == nil {
		visited = idx.visitedPool.Get().(map[uint64]struct{})
		defer func() {
			// Clear the map before returning to pool
			clear(visited)
			idx.visitedPool.Put(visited)
		}()
	}

	if _, isVisited := visited[entryPoint]; !isVisited {
		epData, err := idx.getNodeData(
			batch,
			entryPoint,
		) // Fetches vector + neighbors (from Pebble)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to get entry point data for ID %d in search: %w",
				entryPoint,
				err,
			)
		}

		// Start search with the entry point
		epDist := vector.MeasureDistance(idx.config.DistanceMetric, query, epData.vector)
		item := &PriorityItem{ID: entryPoint, Distance: epDist, Metadata: epData.metadata}
		heap.Push(candidates, item)
		if len(filterPrefix) > 0 {
			if epData.metadata != nil && bytes.HasPrefix(epData.metadata, filterPrefix) {
				// If the entry point matches the filter, add it to results
				heap.Push(results, item)
			}
			// If not found or doesn't match, we don't add it to results.
		} else {
			heap.Push(results, item)
		}
		visited[entryPoint] = struct{}{}
	}

	// Perform Greedy/Beam Search
	for candidates.Len() > 0 {
		currentCandidateItem := heap.Pop(candidates).(*PriorityItem)
		// Termination: if the closest candidate is further than the worst in results, stop.
		if results.Len() >= ef && len(visited) > ef &&
			currentCandidateItem.Distance > results.Peek().Distance {
			break
		}

		nodeData, err := idx.getNodeData(batch, currentCandidateItem.ID)
		if err != nil {
			if !errors.Is(err, ErrNotFound) {
				// TODO (ajr) Add a metric for this?
				log.Printf(
					"Search Warning: Failed to get data for candidate node %d: %v. Skipping.",
					currentCandidateItem.ID,
					err,
				)
			}
			continue // Skip this candidate if data retrieval fails
		}

		// Get neighbors at the current layer
		neighbors := nodeData.neighbors[layer]
		if len(neighbors) == 0 {
			// Node might not have connections at this layer
			continue
		}

		// Explore neighbors
		for _, neighbor := range neighbors {
			if _, isVisited := visited[neighbor.ID]; isVisited {
				continue // Already processed this node
			}
			visited[neighbor.ID] = struct{}{} // Mark as visited

			// Fetch neighbor data (vector required for distance calculation)
			neighborData, err := idx.getNodeData(batch, neighbor.ID)
			if err != nil {
				if !errors.Is(err, ErrNotFound) {
					log.Printf(
						"Search Warning: Failed to get data for neighbor node %d: %v. Skipping.",
						neighbor.ID,
						err,
					)
				}
				continue
			}

			neighborDist := vector.MeasureDistance(
				idx.config.DistanceMetric,
				query,
				neighborData.vector,
			)

			// Check if this neighbor should be added to our potential results
			// Add to results max-heap if it's better than the current worst in the heap, or if heap isn't full.
			addToResults := false
			addToCandidates := false
			if results.Len() < ef {
				addToCandidates = true
				if len(filterPrefix) > 0 {
					if neighborData.metadata != nil &&
						bytes.HasPrefix(neighborData.metadata, filterPrefix) {
						addToResults = true
					}
				} else {
					addToResults = true // Add if results heap is not yet full
				}
			} else {
				// Get the current worst distance in results
				if neighborDist < results.Peek().Distance {
					// If neighbor is closer than the worst, remove the worst and plan to add neighbor
					if len(filterPrefix) > 0 {
						if neighborData.metadata != nil && bytes.HasPrefix(neighborData.metadata, filterPrefix) {
							heap.Pop(results)
							addToResults = true
							addToCandidates = true
						}
					} else {
						heap.Pop(results)
						addToResults = true
						addToCandidates = true
					}
				}
			}

			if addToCandidates {
				// Add to both results (max-heap, positive distance) and candidates (min-heap, negative distance)
				item := &PriorityItem{
					ID:       neighbor.ID,
					Distance: neighborDist,
					Metadata: neighborData.metadata,
				}
				heap.Push(candidates, item)
				if addToResults {
					heap.Push(results, item)
				}
			}
		}
	}

	return results, nil
}

// Add this optimized version that works with a batch
func (idx *HNSWIndex) selectNeighborsHeuristicWithBatch(
	batch *pebble.Batch,
	candidates []*PriorityItem,
	M int,
	layer int,
) []*PriorityItem {
	if len(candidates) <= M {
		return candidates
	}

	// Pre-load all candidate vectors
	candidateNeighbors := make(map[uint64][]*PriorityItem)
	for _, candidate := range candidates {
		if nodeData, err := idx.getNodeData(batch, candidate.ID); err == nil {
			candidateNeighbors[candidate.ID] = nodeData.neighbors[layer]
		}
	}

	// Sort candidates by distance
	slices.SortFunc(candidates, func(a, b *PriorityItem) int {
		return cmp.Compare(a.Distance, b.Distance)
	})

	selected := make([]*PriorityItem, 0, M)

	for i, candidate := range candidates {
		if len(selected) >= M {
			break
		}

		if len(selected) == 0 {
			selected = append(selected, candidate)
			candidates[i] = nil // Mark as used
			continue
		}

		shouldAdd := true
		for _, s := range selected {
			selectedVec, ok := candidateNeighbors[s.ID]
			if !ok {
				continue
			}
			i := slices.IndexFunc(selectedVec, func(a *PriorityItem) bool {
				return a.ID == candidate.ID
			})
			if i < 0 {
				continue // Skip if candidate is not in selected neighbors
			}

			distBetweenNeighbors := selectedVec[i].Distance
			// distBetweenNeighbors := idx.config.DistanceFunc(candidateVec, selectedVec)

			if distBetweenNeighbors < candidate.Distance {
				shouldAdd = false
				break
			}
		}

		if shouldAdd {
			selected = append(selected, candidate)
			candidates[i] = nil // Mark as used
		}
	}

	// Fill remaining slots if needed
	if len(selected) < M {
		for _, candidate := range candidates {
			if candidate == nil {
				continue // Skip found candidates
			}
			selected = append(selected, candidate)
			if len(selected) >= M {
				break
			}
		}
	}

	return selected
}

// updateGraphForNode connects a new node into the HNSW graph stored in PebbleDB.
// Uses a Pebble Batch for atomicity of reciprocal updates.
// Assumes Lock is held.
func (idx *HNSWIndex) updateGraphForNode(
	buf *bytes.Buffer,
	batch *pebble.Batch,
	entryPoint uint64,
	entryPointLayer int,
	nodeLayer int,
	nodeID uint64,
	nodeVector []float32,
) error {
	if entryPoint == nodeID {
		return fmt.Errorf("entry point %d is the same as the new node ID %d", entryPoint, nodeID)
	}

	// Start from the top layer and search down to the node's layer
	currentNearest := entryPoint
	// var currentNearestDist float32 = math.MaxFloat32
	visited := idx.visitedPool.Get().(map[uint64]struct{}) // Get visited map from pool
	defer func() {
		// Clear the map before returning to pool
		clear(visited)
		idx.visitedPool.Put(visited)
	}()
	var prev *PriorityQueue
	minDist := float32(math.MaxFloat32)
	for layer := entryPointLayer; layer > nodeLayer; layer-- {
		// Search at current layer with ef=M to find nearest point
		var err error
		// currentNearest, currentNearestDist, err = idx.GetClosestEntryAtLayer(batch, nodeVector, currentNearest, currentNearestDist, layer, entryVisits)
		prev, err = idx.searchLayerPebbleInternal(
			batch,
			nodeVector,
			nil,
			currentNearest,
			1,
			layer,
			nil,
			nil,
		)
		if err != nil {
			return fmt.Errorf("searching at layer %d for node %d: %w", layer, nodeID, err)
		}
		if prev.Len() > 0 {
			nearest := prev.Items(1)[0]
			if nearest.Distance > minDist {
				return fmt.Errorf(
					"invariant violated: candidate at layer %d with distance %f > minimum distance %f",
					layer, nearest.Distance, minDist,
				)
			}
			currentNearest = nearest.ID
			minDist = nearest.Distance
		}
	}

	// Clear visited map for reuse at insertion layers
	clear(visited)
	// Now insert at all layers from nodeLayer down to 0
	for layer := nodeLayer; layer >= 0; layer-- {
		// Determine M for this layer
		M := idx.config.NeighborsHigherLayers
		if layer == 0 {
			M = idx.config.Neighbors
		}

		// Search for neighbors at this layer
		var err error
		prev, err = idx.searchLayerPebbleInternal(
			batch,
			nodeVector,
			nil,
			currentNearest,
			int(idx.config.EfConstruction),
			layer,
			nil,
			nil,
		)
		if err != nil {
			return fmt.Errorf(
				"failed during neighbor search at layer %d for node %d: %w",
				layer,
				nodeID,
				err,
			)
		}
		// prev.ShrinkTo(int(M))
		// candidates := prev.Items(int(M))
		allCandidates := prev.Items(prev.Len()) // Get all candidates
		candidates := idx.selectNeighborsHeuristicWithBatch(batch, allCandidates, int(M), layer)

		// Update the new node's neighbor list at this layer
		buf.Reset()
		if err := serializeNeighbors(buf, candidates); err != nil {
			return fmt.Errorf(
				"serializing neighbors for node %d at layer %d: %w",
				nodeID,
				layer,
				err,
			)
		}
		if err := batch.Set(makePebbleGraphLayerKey(nodeID, layer), buf.Bytes(), nil); err != nil {
			return fmt.Errorf(
				"failed adding node %d layer %d update to batch: %w",
				nodeID,
				layer,
				err,
			)
		}
		buf.Reset()
		// Update reciprocal connections
		for _, neighbor := range candidates {
			if neighbor.ID == nodeID {
				continue
			}

			neighborData, err := idx.getNodeData(batch, neighbor.ID)
			if err != nil {
				log.Printf(
					"Warning: Failed to read neighbors for node %d at layer %d: %v",
					neighbor.ID,
					layer,
					err,
				)
				continue
			}
			neighborNeighbors := neighborData.neighbors[layer]

			// Add new node to neighbor's connections
			maxConnections := int(M)
			i := slices.IndexFunc(neighborNeighbors, func(item *PriorityItem) bool {
				return item.Distance < neighbor.Distance
			})
			if i == -1 && len(neighborNeighbors) >= maxConnections {
				continue // Don't add if list is full and new node is worse
			}

			neighborNeighbors = append(
				neighborNeighbors,
				&PriorityItem{ID: nodeID, Distance: neighbor.Distance},
			)
			if i != -1 {
				slices.SortFunc(neighborNeighbors, func(a, b *PriorityItem) int {
					return cmp.Compare(a.Distance, b.Distance)
				})
				if len(neighborNeighbors) > maxConnections {
					neighborNeighbors = neighborNeighbors[:maxConnections]
				}
			}

			// Save updated neighbor connections

			if err := serializeNeighbors(buf, neighborNeighbors); err != nil {
				log.Printf(
					"Warning: Failed to serialize neighbors for node %d at layer %d: %v",
					neighbor.ID,
					layer,
					err,
				)
				continue
			}
			if err := batch.Set(makePebbleGraphLayerKey(neighbor.ID, layer), buf.Bytes(), nil); err != nil {
				return fmt.Errorf(
					"failed adding neighbor %d layer %d update to batch: %w",
					neighbor.ID,
					layer,
					err,
				)
			}
			buf.Reset()
			idx.updateCache(neighbor.ID, layer, neighborNeighbors)
		}

		// Use the found neighbors as starting points for the next layer
		if len(candidates) > 0 {
			currentNearest = candidates[0].ID
		}
	}
	return nil
}

// invalidateCache removes a node ID from the LRU cache.
func (idx *HNSWIndex) invalidateCache(ids ...uint64) {
	idx.cacheMu.Lock()
	defer idx.cacheMu.Unlock()

	for _, id := range ids {
		if elem, found := idx.nodeCache[id]; found {
			evictedData := idx.cacheList.Remove(elem).(*HNSWNodeData)
			delete(idx.nodeCache, id)
			idx.releaseNodeData(evictedData)
		}
	}
}

func (idx *HNSWIndex) updateCache(id uint64, layer int, neighbors []*PriorityItem) {
	idx.cacheMu.Lock()
	defer idx.cacheMu.Unlock()

	if elem, found := idx.nodeCache[id]; found {
		nodeData := elem.Value.(*HNSWNodeData)
		if nodeData.neighbors == nil {
			nodeData.neighbors = make(map[int][]*PriorityItem)
		}
		nodeData.neighbors[layer] = neighbors
	}
}
