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
	"log"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"

	"github.com/antflydb/antfly/lib/logger"
	"github.com/antflydb/antfly/lib/utils"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

const (
	pebbleDirname          = "pebble" // Directory for PebbleDB data
	indexVersion           = 2        // Version of the index metadata format - bumped for HNSW support
	serializedNeighborSize = 12
)

// PebbleDB key prefixes for different data types
var (
	pebbleNodeKeyPrefix      = []byte("n:") // Key prefix for node data
	pebbleGraphKeySuffix     = []byte(":g") // Suffix for graph keys
	pebbleVectorKeySuffix    = []byte(":v") // Suffix for vector keys
	pebbleMetadataKeySuffix  = []byte(":m") // Suffix for metadata keys
	pebbleLayerKeySuffix     = []byte(":l") // Suffix for layer assignment
	pebbleOverlayKeySuffix   = []byte(":o") // Suffix for overlay membership marker
	pebbleReverseGraphPrefix = []byte("r:")
	// Key structure: i:<[]byte> -> nodeID (user metadata invert)
	pebbleMetadataInvertKeyPrefix = []byte("i:")
	// Key structure: meta -> serialized index metadata
	indexMetaPrefix = []byte("\x00\x00__meta__:")
	// Key structure: __active_count__:<indexName> -> uint64
	activeCountPrefix = []byte("\x00\x00__active_count__:")
	// Key structure: __entry_point__:<indexName> -> uint64
	entryPointPrefix = []byte("\x00\x00__entry_point__:")
	// Key structure: __base_entry_point__:<indexName> -> uint64
	baseEntryPointPrefix = []byte("\x00\x00__base_entry_point__:")
	// Key structure: __overlay_entry_point__:<indexName> -> uint64
	overlayEntryPointPrefix = []byte("\x00\x00__overlay_entry_point__:")
	// Key structure: __overlay_count__:<indexName> -> uint64
	overlayCountPrefix = []byte("\x00\x00__overlay_count__:")
)

var hnswOverlayActivationMinNodes uint64 = 256

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

func makePebbleNodeKey(node uint64, suffix []byte) []byte {
	key := make([]byte, len(pebbleNodeKeyPrefix)+8+len(suffix))
	copy(key, pebbleNodeKeyPrefix)
	binary.BigEndian.PutUint64(key[len(pebbleNodeKeyPrefix):], node)
	copy(key[len(pebbleNodeKeyPrefix)+8:], suffix)
	return key
}

func makePebbleVectorKey(node uint64) []byte {
	return makePebbleNodeKey(node, pebbleVectorKeySuffix)
}

func makePebbleMetadataKey(node uint64) []byte {
	return makePebbleNodeKey(node, pebbleMetadataKeySuffix)
}

func makePebbleLayerKey(node uint64) []byte {
	return makePebbleNodeKey(node, pebbleLayerKeySuffix)
}

func makePebbleOverlayKey(node uint64) []byte {
	return makePebbleNodeKey(node, pebbleOverlayKeySuffix)
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

func makePebbleReverseGraphPrefix(target uint64) []byte {
	key := make([]byte, len(pebbleReverseGraphPrefix)+8)
	copy(key, pebbleReverseGraphPrefix)
	binary.BigEndian.PutUint64(key[len(pebbleReverseGraphPrefix):], target)
	return key
}

func makePebbleReverseGraphKey(target uint64, layer int, source uint64) []byte {
	key := make([]byte, len(pebbleReverseGraphPrefix)+8+4+8)
	copy(key, pebbleReverseGraphPrefix)
	binary.BigEndian.PutUint64(key[len(pebbleReverseGraphPrefix):], target)
	binary.BigEndian.PutUint32(key[len(pebbleReverseGraphPrefix)+8:], uint32(layer)) //nolint:gosec // G115
	binary.BigEndian.PutUint64(key[len(pebbleReverseGraphPrefix)+8+4:], source)
	return key
}

func decodePebbleReverseGraphKey(key []byte) (target uint64, layer int, source uint64, ok bool) {
	if len(key) != len(pebbleReverseGraphPrefix)+8+4+8 || !bytes.HasPrefix(key, pebbleReverseGraphPrefix) {
		return 0, 0, 0, false
	}
	target = binary.BigEndian.Uint64(key[len(pebbleReverseGraphPrefix):])
	layer = int(binary.BigEndian.Uint32(key[len(pebbleReverseGraphPrefix)+8:]))
	source = binary.BigEndian.Uint64(key[len(pebbleReverseGraphPrefix)+8+4:])
	return target, layer, source, true
}

// makePebbleMetadataInvertPrefix creates the prefix for metadata->node reverse lookups.
func makePebbleMetadataInvertPrefix(metadata []byte) []byte {
	key := make([]byte, len(pebbleMetadataInvertKeyPrefix)+len(metadata))
	copy(key, pebbleMetadataInvertKeyPrefix)
	copy(key[len(pebbleMetadataInvertKeyPrefix):], metadata)
	return key
}

// makePebbleMetadataInvertKey creates a PebbleDB key for user metadata and node ID.
func makePebbleMetadataInvertKey(metadata []byte, id uint64) []byte {
	key := make([]byte, len(pebbleMetadataInvertKeyPrefix)+len(metadata)+8)
	copy(key, pebbleMetadataInvertKeyPrefix)
	copy(key[len(pebbleMetadataInvertKeyPrefix):], metadata)
	binary.BigEndian.PutUint64(key[len(pebbleMetadataInvertKeyPrefix)+len(metadata):], id)
	return key
}

func decodeMetadataInvertKey(prefix []byte, key, value []byte) (uint64, []byte, bool) {
	if len(key) >= len(prefix)+8 && bytes.HasPrefix(key, prefix) {
		id := binary.BigEndian.Uint64(key[len(key)-8:])
		metadata := key[len(pebbleMetadataInvertKeyPrefix) : len(key)-8]
		return id, metadata, true
	}
	if len(key) != len(prefix) || len(value) != 8 || !bytes.Equal(key, prefix) {
		return 0, nil, false
	}
	id := binary.BigEndian.Uint64(value)
	metadata := key[len(pebbleMetadataInvertKeyPrefix):]
	return id, metadata, true
}

// makeActiveCountKey creates a PebbleDB key for the active count
func makeActiveCountKey(indexName string) []byte {
	return append(bytes.Clone(activeCountPrefix), indexName...)
}

// makeEntryPointKey creates a PebbleDB key for the entry point
func makeEntryPointKey(indexName string) []byte {
	return append(bytes.Clone(entryPointPrefix), indexName...)
}

func makeBaseEntryPointKey(indexName string) []byte {
	return append(bytes.Clone(baseEntryPointPrefix), indexName...)
}

func makeOverlayEntryPointKey(indexName string) []byte {
	return append(bytes.Clone(overlayEntryPointPrefix), indexName...)
}

func makeOverlayCountKey(indexName string) []byte {
	return append(bytes.Clone(overlayCountPrefix), indexName...)
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

	activeCountKey       []byte
	entryPointKey        []byte
	baseEntryPointKey    []byte
	overlayEntryPointKey []byte
	overlayCountKey      []byte
	indexMetaKey         []byte
	nodeKeyUpperEnd      []byte

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

type hnswEntryPointClass int

const (
	hnswEntryPointAny hnswEntryPointClass = iota
	hnswEntryPointBase
	hnswEntryPointOverlay
)

type hnswMutableState struct {
	activeCount uint64

	globalEntryPoint  uint64
	globalMaxLayer    int
	baseEntryPoint    uint64
	baseMaxLayer      int
	overlayEntryPoint uint64
	overlayMaxLayer   int
	overlayCount      uint64
}

func (s *hnswMutableState) overlayActive() bool {
	return s.overlayCount > 0 || s.activeCount >= hnswOverlayActivationMinNodes
}

type hnswBatchContext struct {
	batch *pebble.Batch
	nodes map[uint64]*HNSWNodeData
}

func newHNSWBatchContext(batch *pebble.Batch) *hnswBatchContext {
	return &hnswBatchContext{
		batch: batch,
		nodes: make(map[uint64]*HNSWNodeData, 256),
	}
}

// Name returns the base path of the index.
func (idx *HNSWIndex) Name() string {
	return idx.config.IndexPath
}

var ErrActiveCountNotFound = errors.New("active count not found in index")

func readUint64Key(db pebble.Reader, key []byte, notFound error, name string) (uint64, error) {
	value, closer, err := db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, notFound
		}
		return 0, fmt.Errorf("failed to read %s: %w", name, err)
	}
	defer func() { _ = closer.Close() }()

	if len(value) != 8 {
		return 0, fmt.Errorf("invalid %s value size: %d", name, len(value))
	}

	return binary.LittleEndian.Uint64(value), nil
}

func setUint64Key(batch *pebble.Batch, key []byte, value uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	return batch.Set(key, buf[:], nil)
}

func nodeExists(db pebble.Reader, id uint64) (bool, error) {
	value, closer, err := db.Get(makePebbleVectorKey(id))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check vector for ID %d: %w", id, err)
	}
	defer func() { _ = closer.Close() }()
	return len(value) > 0, nil
}

func hasReverseGraphEntries(db pebble.Reader) (bool, error) {
	lower := pebbleReverseGraphPrefix
	upper := utils.PrefixSuccessor(pebbleReverseGraphPrefix)
	iter, err := db.NewIterWithContext(context.TODO(), &pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return false, fmt.Errorf("creating reverse graph iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()
	if iter.First() {
		return true, nil
	}
	return false, iter.Error()
}

func (idx *HNSWIndex) rebuildReverseGraphIndex(batch *pebble.Batch) error {
	if err := batch.DeleteRange(pebbleReverseGraphPrefix, utils.PrefixSuccessor(pebbleReverseGraphPrefix), nil); err != nil {
		return fmt.Errorf("clearing reverse graph index: %w", err)
	}

	iter, err := idx.db.NewIterWithContext(context.TODO(), &pebble.IterOptions{
		LowerBound: pebbleNodeKeyPrefix,
		UpperBound: idx.nodeKeyUpperEnd,
	})
	if err != nil {
		return fmt.Errorf("creating graph iterator for reverse graph rebuild: %w", err)
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < len(pebbleNodeKeyPrefix)+8+len(pebbleGraphKeySuffix)+1+4 ||
			!bytes.HasSuffix(key[:len(key)-5], pebbleGraphKeySuffix) {
			continue
		}

		source := binary.BigEndian.Uint64(key[len(pebbleNodeKeyPrefix) : len(pebbleNodeKeyPrefix)+8])
		layer := int(binary.BigEndian.Uint32(key[len(key)-4:]))
		neighbors, err := idx.deserializeNeighborsWithPool(iter.Value())
		if err != nil {
			return fmt.Errorf("rebuilding reverse graph for node %d layer %d: %w", source, layer, err)
		}
		for _, neighbor := range neighbors {
			if err := batch.Set(makePebbleReverseGraphKey(neighbor.ID, layer, source), nil, nil); err != nil {
				return fmt.Errorf("writing reverse graph edge %d <- %d at layer %d: %w", neighbor.ID, source, layer, err)
			}
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterating graph for reverse graph rebuild: %w", err)
	}
	return nil
}

func readIncomingEdges(db pebble.Reader, target uint64) (map[int][]uint64, error) {
	prefix := makePebbleReverseGraphPrefix(target)
	iter, err := db.NewIterWithContext(context.TODO(), &pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: utils.PrefixSuccessor(prefix),
	})
	if err != nil {
		return nil, fmt.Errorf("creating reverse graph iterator for %d: %w", target, err)
	}
	defer func() { _ = iter.Close() }()

	incoming := make(map[int][]uint64)
	for iter.First(); iter.Valid(); iter.Next() {
		_, layer, source, ok := decodePebbleReverseGraphKey(iter.Key())
		if !ok {
			continue
		}
		incoming[layer] = append(incoming[layer], source)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterating reverse graph for %d: %w", target, err)
	}
	return incoming, nil
}

func (idx *HNSWIndex) resetEntryPoint(batch *pebble.Batch, invalidEntry uint64) (bool, error) {
	iter, err := batch.NewIterWithContext(context.TODO(), &pebble.IterOptions{
		LowerBound: pebbleNodeKeyPrefix,
		UpperBound: idx.nodeKeyUpperEnd,
		SkipPoint: func(userKey []byte) bool {
			suffixToCheck := make([]byte, len(pebbleGraphKeySuffix)+1+4)
			copy(suffixToCheck, pebbleGraphKeySuffix)
			suffixToCheck[len(pebbleGraphKeySuffix)] = ':'
			binary.LittleEndian.PutUint32(suffixToCheck[len(pebbleGraphKeySuffix)+1:], uint32(0)) // Layer 0
			return len(userKey) < len(pebbleNodeKeyPrefix)+8 || !bytes.HasSuffix(userKey, suffixToCheck)
		},
	})
	if err != nil {
		return false, fmt.Errorf("error creating iterator: %w", err)
	}
	defer func() {
		_ = iter.Close()
	}()

	if !iter.First() {
		return false, nil
	}

	key := iter.Key()
	newEntryID := binary.BigEndian.Uint64(key[len(pebbleNodeKeyPrefix) : len(pebbleNodeKeyPrefix)+8])
	log.Printf("Info: Found new valid entry point: %d (replacing invalid %d)", newEntryID, invalidEntry)
	if err := idx.setEntryPoint(batch, newEntryID); err != nil {
		return false, err
	}
	return true, nil
}

// getActiveCount reads the active count from PebbleDB
func (idx *HNSWIndex) getActiveCount(db pebble.Reader) (uint64, error) {
	return readUint64Key(db, idx.activeCountKey, ErrActiveCountNotFound, "active count")
}

// setActiveCount writes the active count to PebbleDB
func (idx *HNSWIndex) setActiveCount(batch *pebble.Batch, count uint64) error {
	return setUint64Key(batch, idx.activeCountKey, count)
}

var ErrEntryPointNotFound = errors.New("entry point not found")
var ErrBaseEntryPointNotFound = errors.New("base entry point not found")
var ErrOverlayEntryPointNotFound = errors.New("overlay entry point not found")
var ErrOverlayCountNotFound = errors.New("overlay count not found")

// getEntryPoint reads the entry point from PebbleDB
func (idx *HNSWIndex) getEntryPoint(db pebble.Reader) (uint64, error) {
	return readUint64Key(db, idx.entryPointKey, ErrEntryPointNotFound, "entry point")
}

// setEntryPoint writes the entry point to PebbleDB
func (idx *HNSWIndex) setEntryPoint(batch *pebble.Batch, entryPoint uint64) error {
	return setUint64Key(batch, idx.entryPointKey, entryPoint)
}

func (idx *HNSWIndex) getBaseEntryPoint(db pebble.Reader) (uint64, error) {
	return readUint64Key(db, idx.baseEntryPointKey, ErrBaseEntryPointNotFound, "base entry point")
}

func (idx *HNSWIndex) setBaseEntryPoint(batch *pebble.Batch, entryPoint uint64) error {
	return setUint64Key(batch, idx.baseEntryPointKey, entryPoint)
}

func (idx *HNSWIndex) getOverlayEntryPoint(db pebble.Reader) (uint64, error) {
	return readUint64Key(db, idx.overlayEntryPointKey, ErrOverlayEntryPointNotFound, "overlay entry point")
}

func (idx *HNSWIndex) setOverlayEntryPoint(batch *pebble.Batch, entryPoint uint64) error {
	return setUint64Key(batch, idx.overlayEntryPointKey, entryPoint)
}

func (idx *HNSWIndex) getOverlayCount(db pebble.Reader) (uint64, error) {
	return readUint64Key(db, idx.overlayCountKey, ErrOverlayCountNotFound, "overlay count")
}

func (idx *HNSWIndex) setOverlayCount(batch *pebble.Batch, count uint64) error {
	return setUint64Key(batch, idx.overlayCountKey, count)
}

func (idx *HNSWIndex) nodeIsOverlay(db pebble.Reader, id uint64) (bool, error) {
	value, closer, err := db.Get(makePebbleOverlayKey(id))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("failed to read overlay marker for node %d: %w", id, err)
	}
	defer func() { _ = closer.Close() }()
	return len(value) > 0, nil
}

func (idx *HNSWIndex) setNodeOverlay(batch *pebble.Batch, id uint64, isOverlay bool) error {
	if isOverlay {
		return batch.Set(makePebbleOverlayKey(id), []byte{1}, nil)
	}
	return batch.Delete(makePebbleOverlayKey(id), nil)
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
		config:               config,
		assignmentProbs:      assignmentProbs,
		rand:                 rand.New(randSource),           //nolint:gosec // G404: non-security randomness for ML/jitter
		cacheList:            list.New(),                     // Initialize LRU list
		nodeCache:            make(map[uint64]*list.Element), // Initialize cache map
		dbPath:               filepath.Join(config.IndexPath, pebbleDirname),
		activeCountKey:       makeActiveCountKey(config.Name),
		entryPointKey:        makeEntryPointKey(config.Name),
		baseEntryPointKey:    makeBaseEntryPointKey(config.Name),
		overlayEntryPointKey: makeOverlayEntryPointKey(config.Name),
		overlayCountKey:      makeOverlayCountKey(config.Name),
		indexMetaKey:         append(bytes.Clone(indexMetaPrefix), config.Name...),
		nodeKeyUpperEnd:      utils.PrefixSuccessor(pebbleNodeKeyPrefix),
		nodeDataPool: &sync.Pool{
			New: func() any {
				return &HNSWNodeData{
					neighbors: make(map[int][]*PriorityItem, 4),
					layer:     -1,
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
	activeCount, err := idx.getActiveCount(batch)
	if err != nil && !errors.Is(err, ErrActiveCountNotFound) {
		_ = idx.db.Close()
		return nil, fmt.Errorf("checking active count during open: %w", err)
	}
	if err == nil && activeCount > 0 {
		hasReverseEdges, reverseErr := hasReverseGraphEntries(batch)
		if reverseErr != nil {
			_ = idx.db.Close()
			return nil, fmt.Errorf("checking reverse graph index: %w", reverseErr)
		}
		if !hasReverseEdges {
			if err := idx.rebuildReverseGraphIndex(batch); err != nil {
				_ = idx.db.Close()
				return nil, fmt.Errorf("rebuilding reverse graph index: %w", err)
			}
		}
	}
	if err := idx.ensureMutableState(batch); err != nil {
		_ = idx.db.Close()
		return nil, fmt.Errorf("ensuring mutable state: %w", err)
	}
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
	if err := batch.Set(idx.indexMetaKey, buf.Bytes(), nil); err != nil {
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
	if err := idx.setBaseEntryPoint(batch, 0); err != nil {
		return fmt.Errorf("failed to set initial base entry point: %w", err)
	}
	if err := idx.setOverlayEntryPoint(batch, 0); err != nil {
		return fmt.Errorf("failed to set initial overlay entry point: %w", err)
	}
	if err := idx.setOverlayCount(batch, 0); err != nil {
		return fmt.Errorf("failed to set initial overlay count: %w", err)
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
	value, closer, err := idx.db.Get(idx.indexMetaKey)
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

func (idx *HNSWIndex) loadMutableState(db pebble.Reader) (*hnswMutableState, error) {
	state := &hnswMutableState{
		globalMaxLayer:  -1,
		baseMaxLayer:    -1,
		overlayMaxLayer: -1,
	}

	activeCount, err := idx.getActiveCount(db)
	if err != nil {
		return nil, fmt.Errorf("loading active count: %w", err)
	}
	state.activeCount = activeCount

	globalEntryPoint, err := idx.getEntryPoint(db)
	if err != nil {
		return nil, fmt.Errorf("loading global entry point: %w", err)
	}
	state.globalEntryPoint = globalEntryPoint
	if globalEntryPoint != 0 {
		state.globalMaxLayer, err = idx.getNodeLayer(db, globalEntryPoint)
		if err != nil {
			return nil, fmt.Errorf("loading global entry point layer for %d: %w", globalEntryPoint, err)
		}
	}

	baseEntryPoint, err := idx.getBaseEntryPoint(db)
	if err != nil {
		if !errors.Is(err, ErrBaseEntryPointNotFound) {
			return nil, fmt.Errorf("loading base entry point: %w", err)
		}
		baseEntryPoint = globalEntryPoint
	}
	state.baseEntryPoint = baseEntryPoint
	if baseEntryPoint != 0 {
		state.baseMaxLayer, err = idx.getNodeLayer(db, baseEntryPoint)
		if err != nil {
			return nil, fmt.Errorf("loading base entry point layer for %d: %w", baseEntryPoint, err)
		}
	}

	overlayEntryPoint, err := idx.getOverlayEntryPoint(db)
	if err != nil {
		if !errors.Is(err, ErrOverlayEntryPointNotFound) {
			return nil, fmt.Errorf("loading overlay entry point: %w", err)
		}
		overlayEntryPoint = 0
	}
	state.overlayEntryPoint = overlayEntryPoint
	if overlayEntryPoint != 0 {
		state.overlayMaxLayer, err = idx.getNodeLayer(db, overlayEntryPoint)
		if err != nil {
			return nil, fmt.Errorf("loading overlay entry point layer for %d: %w", overlayEntryPoint, err)
		}
	}

	overlayCount, err := idx.getOverlayCount(db)
	if err != nil {
		if !errors.Is(err, ErrOverlayCountNotFound) {
			return nil, fmt.Errorf("loading overlay count: %w", err)
		}
		overlayCount = 0
	}
	state.overlayCount = overlayCount

	return state, nil
}

func (idx *HNSWIndex) persistMutableState(batch *pebble.Batch, state *hnswMutableState) error {
	if err := idx.setActiveCount(batch, state.activeCount); err != nil {
		return fmt.Errorf("writing active count: %w", err)
	}
	if err := idx.setEntryPoint(batch, state.globalEntryPoint); err != nil {
		return fmt.Errorf("writing global entry point: %w", err)
	}
	if err := idx.setBaseEntryPoint(batch, state.baseEntryPoint); err != nil {
		return fmt.Errorf("writing base entry point: %w", err)
	}
	if err := idx.setOverlayEntryPoint(batch, state.overlayEntryPoint); err != nil {
		return fmt.Errorf("writing overlay entry point: %w", err)
	}
	if err := idx.setOverlayCount(batch, state.overlayCount); err != nil {
		return fmt.Errorf("writing overlay count: %w", err)
	}
	return nil
}

func (idx *HNSWIndex) findEntryPointCandidate(
	db pebble.Reader,
	class hnswEntryPointClass,
) (uint64, int, bool, error) {
	iter, err := db.NewIterWithContext(context.TODO(), &pebble.IterOptions{
		LowerBound: pebbleNodeKeyPrefix,
		UpperBound: idx.nodeKeyUpperEnd,
		SkipPoint: func(userKey []byte) bool {
			return !bytes.HasSuffix(userKey, pebbleLayerKeySuffix)
		},
	})
	if err != nil {
		return 0, 0, false, fmt.Errorf("creating entry point iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	var (
		bestID    uint64
		bestLayer = -1
	)
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < len(pebbleNodeKeyPrefix)+8+len(pebbleLayerKeySuffix) {
			continue
		}
		id := binary.BigEndian.Uint64(key[len(pebbleNodeKeyPrefix) : len(pebbleNodeKeyPrefix)+8])
		isOverlay, err := idx.nodeIsOverlay(db, id)
		if err != nil {
			return 0, 0, false, err
		}
		switch class {
		case hnswEntryPointBase:
			if isOverlay {
				continue
			}
		case hnswEntryPointOverlay:
			if !isOverlay {
				continue
			}
		}
		value := iter.Value()
		if len(value) != 4 {
			continue
		}
		layer := int(binary.LittleEndian.Uint32(value))
		if layer > bestLayer || (layer == bestLayer && (bestID == 0 || id < bestID)) {
			bestID = id
			bestLayer = layer
		}
	}
	if err := iter.Error(); err != nil {
		return 0, 0, false, fmt.Errorf("iterating entry point candidates: %w", err)
	}
	if bestID == 0 {
		return 0, 0, false, nil
	}
	return bestID, bestLayer, true, nil
}

func (idx *HNSWIndex) ensureMutableState(batch *pebble.Batch) error {
	state, err := idx.loadMutableState(batch)
	if err != nil {
		return err
	}
	if state.activeCount == 0 {
		state.globalEntryPoint = 0
		state.baseEntryPoint = 0
		state.overlayEntryPoint = 0
		state.globalMaxLayer = -1
		state.baseMaxLayer = -1
		state.overlayMaxLayer = -1
		state.overlayCount = 0
		return idx.persistMutableState(batch, state)
	}

	globalValid, err := idx.isValidEntryPoint(batch, state.globalEntryPoint, hnswEntryPointAny)
	if err != nil {
		return err
	}
	if !globalValid {
		id, layer, ok, err := idx.findEntryPointCandidate(batch, hnswEntryPointAny)
		if err != nil {
			return err
		}
		if ok {
			state.globalEntryPoint = id
			state.globalMaxLayer = layer
		} else {
			state.globalEntryPoint = 0
			state.globalMaxLayer = -1
		}
	}

	baseValid, err := idx.isValidEntryPoint(batch, state.baseEntryPoint, hnswEntryPointBase)
	if err != nil {
		return err
	}
	if !baseValid {
		id, layer, ok, err := idx.findEntryPointCandidate(batch, hnswEntryPointBase)
		if err != nil {
			return err
		}
		if ok {
			state.baseEntryPoint = id
			state.baseMaxLayer = layer
		} else {
			state.baseEntryPoint = 0
			state.baseMaxLayer = -1
		}
	}

	if state.overlayCount == 0 {
		state.overlayEntryPoint = 0
		state.overlayMaxLayer = -1
	} else {
		overlayValid, err := idx.isValidEntryPoint(batch, state.overlayEntryPoint, hnswEntryPointOverlay)
		if err != nil {
			return err
		}
		if !overlayValid {
			id, layer, ok, err := idx.findEntryPointCandidate(batch, hnswEntryPointOverlay)
			if err != nil {
				return err
			}
			if ok {
				state.overlayEntryPoint = id
				state.overlayMaxLayer = layer
			} else {
				state.overlayEntryPoint = 0
				state.overlayMaxLayer = -1
				state.overlayCount = 0
			}
		}
	}

	if state.globalEntryPoint == 0 {
		state.globalEntryPoint = state.baseEntryPoint
		state.globalMaxLayer = state.baseMaxLayer
		if state.globalEntryPoint == 0 {
			state.globalEntryPoint = state.overlayEntryPoint
			state.globalMaxLayer = state.overlayMaxLayer
		}
	}
	if state.baseEntryPoint == 0 && state.overlayCount == 0 {
		state.baseEntryPoint = state.globalEntryPoint
		state.baseMaxLayer = state.globalMaxLayer
	}

	return idx.persistMutableState(batch, state)
}

func (idx *HNSWIndex) refreshStateEntryPoint(
	db pebble.Reader,
	state *hnswMutableState,
	class hnswEntryPointClass,
) error {
	id, layer, ok, err := idx.findEntryPointCandidate(db, class)
	if err != nil {
		return err
	}
	if !ok {
		id = 0
		layer = -1
	}
	switch class {
	case hnswEntryPointAny:
		state.globalEntryPoint = id
		state.globalMaxLayer = layer
	case hnswEntryPointBase:
		state.baseEntryPoint = id
		state.baseMaxLayer = layer
	case hnswEntryPointOverlay:
		state.overlayEntryPoint = id
		state.overlayMaxLayer = layer
	}
	return nil
}

func (idx *HNSWIndex) isValidEntryPoint(db pebble.Reader, id uint64, class hnswEntryPointClass) (bool, error) {
	if id == 0 {
		return false, nil
	}
	exists, err := nodeExists(db, id)
	if err != nil || !exists {
		return exists, err
	}
	if class == hnswEntryPointAny {
		return true, nil
	}
	isOverlay, err := idx.nodeIsOverlay(db, id)
	if err != nil {
		return false, err
	}
	switch class {
	case hnswEntryPointBase:
		return !isOverlay, nil
	case hnswEntryPointOverlay:
		return isOverlay, nil
	default:
		return true, nil
	}
}

func (ctx *hnswBatchContext) getNodeData(idx *HNSWIndex, id uint64, includeVector bool) (*HNSWNodeData, error) {
	if nodeData, ok := ctx.nodes[id]; ok {
		if includeVector && nodeData.vector == nil {
			vec, err := idx.readVector(ctx.batch, id)
			if err != nil {
				return nil, err
			}
			nodeData.vector = vec
		}
		return nodeData, nil
	}

	nodeData, err := idx.getNodeDataForUpdate(ctx.batch, id, includeVector)
	if err != nil {
		return nil, err
	}
	ctx.nodes[id] = nodeData
	return nodeData, nil
}

func (ctx *hnswBatchContext) getTraversalNodeData(idx *HNSWIndex, id uint64) (*HNSWNodeData, error) {
	return ctx.getNodeData(idx, id, true)
}

func (ctx *hnswBatchContext) storeNodeData(nodeData *HNSWNodeData) {
	ctx.nodes[nodeData.id] = nodeData
}

func (ctx *hnswBatchContext) invalidate(id uint64) {
	delete(ctx.nodes, id)
}

func (ctx *hnswBatchContext) updateNeighbors(id uint64, layer int, neighbors []*PriorityItem) {
	nodeData, ok := ctx.nodes[id]
	if !ok {
		return
	}
	if nodeData.neighbors == nil {
		nodeData.neighbors = make(map[int][]*PriorityItem, max(layer+1, 1))
	}
	nodeData.neighbors[layer] = neighbors
	if layer > nodeData.layer {
		nodeData.layer = layer
	}
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
			entryPointValid, err := idx.resetEntryPoint(batch, entryPoint)
			if err != nil {
				log.Printf("Warning: Failed to save entry point after update: %v", err)
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
	var layerBuf [4]byte
	binary.LittleEndian.PutUint32(layerBuf[:], uint32(nodeLayer)) //nolint:gosec // G115: bounded value, cannot overflow in practice
	if err := batch.Set(makePebbleLayerKey(id), layerBuf[:], nil); err != nil {
		return fmt.Errorf("adding %d layer to batch: %w", id, err)
	}

	if len(metadata) > 0 {
		if err := batch.Set(makePebbleMetadataKey(id), metadata, nil); err != nil {
			return fmt.Errorf("adding %d metadata to batch: %w", id, err)
		}
		// TODO (ajr) If metadata changes we should remove the old metadata invert key
		var idBuf [8]byte
		binary.BigEndian.PutUint64(idBuf[:], id)
		if err := batch.Set(makePebbleMetadataInvertKey(metadata, id), idBuf[:], nil); err != nil {
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

func compactBatchInsertInputs(
	ids []uint64,
	vectors []vector.T,
	metadataList [][]byte,
) ([]uint64, []vector.T, [][]byte) {
	lastIndex := make(map[uint64]int, len(ids))
	for i, id := range ids {
		lastIndex[id] = i
	}

	compactIDs := make([]uint64, 0, len(lastIndex))
	compactVectors := make([]vector.T, 0, len(lastIndex))
	var compactMetadata [][]byte
	if metadataList != nil {
		compactMetadata = make([][]byte, 0, len(lastIndex))
	}

	for i, id := range ids {
		if lastIndex[id] != i {
			continue
		}
		compactIDs = append(compactIDs, id)
		compactVectors = append(compactVectors, vectors[i])
		if metadataList != nil {
			compactMetadata = append(compactMetadata, metadataList[i])
		}
	}

	return compactIDs, compactVectors, compactMetadata
}

func (idx *HNSWIndex) classifyExistingIDs(ids []uint64) ([]bool, bool, error) {
	existing := make([]bool, len(ids))
	var hasExisting bool
	for i, id := range ids {
		found, err := nodeExists(idx.db, id)
		if err != nil {
			return nil, false, err
		}
		existing[i] = found
		hasExisting = hasExisting || found
	}
	return existing, hasExisting, nil
}

func metadataAt(metadataList [][]byte, i int) []byte {
	if metadataList == nil {
		return nil
	}
	return metadataList[i]
}

func (idx *HNSWIndex) insertNewNode(
	batchCtx *hnswBatchContext,
	batch *pebble.Batch,
	buf *bytes.Buffer,
	vecBuf []byte,
	id uint64,
	vec vector.T,
	metadata []byte,
	state *hnswMutableState,
) error {
	nodeLayer := idx.assignLayer()
	if state.activeCount == 0 {
		if err := idx.InitializeNodeData(vecBuf, batch, nodeLayer, id, vec, metadata); err != nil {
			return fmt.Errorf("initializing node data for %d: %w", id, err)
		}
		if err := idx.setNodeOverlay(batch, id, false); err != nil {
			return fmt.Errorf("clearing overlay marker for %d: %w", id, err)
		}
		state.activeCount++
		state.globalEntryPoint = id
		state.globalMaxLayer = nodeLayer
		state.baseEntryPoint = id
		state.baseMaxLayer = nodeLayer
		state.overlayEntryPoint = 0
		state.overlayMaxLayer = -1
		state.overlayCount = 0
		batchCtx.storeNodeData(&HNSWNodeData{
			id:        id,
			vector:    vec,
			neighbors: make(map[int][]*PriorityItem, nodeLayer+1),
			metadata:  bytes.Clone(metadata),
			layer:     nodeLayer,
		})
		return nil
	}

	isOverlay := state.overlayActive()
	graphUpdateEntryPoint := state.globalEntryPoint
	graphUpdateEntryPointLayer := state.globalMaxLayer
	if isOverlay && state.overlayEntryPoint != 0 {
		graphUpdateEntryPoint = state.overlayEntryPoint
		graphUpdateEntryPointLayer = state.overlayMaxLayer
	}
	if graphUpdateEntryPoint == 0 {
		graphUpdateEntryPoint = state.baseEntryPoint
		graphUpdateEntryPointLayer = state.baseMaxLayer
	}

	if err := idx.InitializeNodeData(vecBuf, batch, nodeLayer, id, vec, metadata); err != nil {
		return fmt.Errorf("initializing node data for %d: %w", id, err)
	}
	batchCtx.storeNodeData(&HNSWNodeData{
		id:        id,
		vector:    vec,
		neighbors: make(map[int][]*PriorityItem, nodeLayer+1),
		metadata:  bytes.Clone(metadata),
		layer:     nodeLayer,
	})
	if err := idx.setNodeOverlay(batch, id, isOverlay); err != nil {
		return fmt.Errorf("writing overlay marker for %d: %w", id, err)
	}
	state.activeCount++
	if isOverlay {
		state.overlayCount++
		if state.overlayEntryPoint == 0 || nodeLayer > state.overlayMaxLayer {
			state.overlayEntryPoint = id
			state.overlayMaxLayer = nodeLayer
		}
	} else if state.baseEntryPoint == 0 || nodeLayer > state.baseMaxLayer {
		state.baseEntryPoint = id
		state.baseMaxLayer = nodeLayer
	}
	if nodeLayer > state.globalMaxLayer {
		state.globalEntryPoint = id
		state.globalMaxLayer = nodeLayer
	}
	if err := idx.updateGraphForNode(
		batchCtx,
		buf,
		batch,
		graphUpdateEntryPoint,
		graphUpdateEntryPointLayer,
		min(nodeLayer, graphUpdateEntryPointLayer),
		id,
		vec,
	); err != nil {
		return fmt.Errorf("updating graph for node %d: %w", id, err)
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
	ids, vectors, metadataList = compactBatchInsertInputs(ids, vectors, metadataList)
	if len(vectors) == 0 {
		return nil
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

	state, err := idx.loadMutableState(batch)
	if err != nil {
		return fmt.Errorf("failed to load mutable state: %w", err)
	}
	batchCtx := newHNSWBatchContext(batch)
	existingIDs, hasExistingIDs, err := idx.classifyExistingIDs(ids)
	if err != nil {
		return fmt.Errorf("classifying existing IDs: %w", err)
	}

	vecBuf := make([]byte, 4*idx.config.Dimension)
	buf := bytes.NewBuffer(nil)
	if !hasExistingIDs {
		for i, vec := range vectors {
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
			if err := idx.insertNewNode(
				batchCtx,
				batch,
				buf,
				vecBuf,
				ids[i],
				vec,
				metadataAt(metadataList, i),
				state,
			); err != nil {
				return err
			}
		}
	} else {
		for i, vec := range vectors {
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

			if existingIDs[i] {
				if err := idx.deleteNodeFromBatch(batchCtx, batch, buf, ids[i], state); err != nil {
					return fmt.Errorf("updating existing node %d: %w", ids[i], err)
				}
			}

			if err := idx.insertNewNode(
				batchCtx,
				batch,
				buf,
				vecBuf,
				ids[i],
				vec,
				metadataAt(metadataList, i),
				state,
			); err != nil {
				return err
			}
		}
	}

	if err := idx.persistMutableState(batch, state); err != nil {
		return fmt.Errorf("failed to update mutable state: %w", err)
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

func mergePriorityItems(k int, resultSets ...[]*PriorityItem) []*PriorityItem {
	if k <= 0 {
		return nil
	}
	byID := make(map[uint64]*PriorityItem)
	for _, resultSet := range resultSets {
		for _, item := range resultSet {
			current, exists := byID[item.ID]
			if !exists || item.Distance < current.Distance {
				byID[item.ID] = item
			}
		}
	}
	if len(byID) == 0 {
		return nil
	}
	items := make([]*PriorityItem, 0, len(byID))
	for _, item := range byID {
		items = append(items, item)
	}
	slices.SortFunc(items, func(a, b *PriorityItem) int {
		return cmp.Compare(a.Distance, b.Distance)
	})
	if len(items) > k {
		items = items[:k]
	}
	return items
}

func (idx *HNSWIndex) searchFromEntryPoint(
	query []float32,
	filterPrefix []byte,
	entryPoint uint64,
	k int,
) ([]*PriorityItem, error) {
	entryPointLayer, err := idx.getNodeLayer(idx.db, entryPoint)
	if err != nil {
		return nil, fmt.Errorf("failed to get entry point layer: %w", err)
	}

	currentNearest := entryPoint
	minDist := float32(math.MaxFloat32)
	for layer := entryPointLayer; layer > 0; layer-- {
		pqueue, err := idx.searchLayerPebbleInternal(
			nil,
			nil,
			query,
			nil,
			currentNearest,
			1,
			layer,
		)
		if err != nil {
			return nil, fmt.Errorf("search failed at layer %d: %w", layer, err)
		}
		if pqueue.Len() == 0 {
			continue
		}
		nearest := pqueue.Best()
		if nearest.Distance > minDist {
			return nil, fmt.Errorf(
				"invariant violated: candidate at layer %d with distance %f > minimum distance %f",
				layer, nearest.Distance, minDist,
			)
		}
		currentNearest = nearest.ID
		minDist = nearest.Distance
	}

	pqueue, err := idx.searchLayerPebbleInternal(
		nil,
		nil,
		query,
		filterPrefix,
		currentNearest,
		max(int(idx.config.EfSearch), k),
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("search failed at layer 0: %w", err)
	}
	return pqueue.Items(k), nil
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
	state, err := idx.loadMutableState(db)
	if err != nil {
		return nil, fmt.Errorf("failed to load mutable state: %w", err)
	}

	if state.activeCount == 0 {
		return nil, nil // Empty index
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

	entryPoints := make([]uint64, 0, 3)
	if state.baseEntryPoint != 0 {
		entryPoints = append(entryPoints, state.baseEntryPoint)
	}
	if state.overlayEntryPoint != 0 && state.overlayEntryPoint != state.baseEntryPoint {
		entryPoints = append(entryPoints, state.overlayEntryPoint)
	}
	if len(entryPoints) == 0 && state.globalEntryPoint != 0 {
		entryPoints = append(entryPoints, state.globalEntryPoint)
	}

	var resultSets [][]*PriorityItem
	for _, entryPoint := range entryPoints {
		items, err := idx.searchFromEntryPoint(query, filterPrefix, entryPoint, k)
		if err != nil {
			return nil, err
		}
		resultSets = append(resultSets, items)
	}
	candidateItems := mergePriorityItems(k, resultSets...)

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
	prefix := makePebbleMetadataInvertPrefix(key)
	iter, err := idx.db.NewIterWithContext(context.TODO(), &pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: utils.PrefixSuccessor(prefix),
	})
	if err != nil {
		return fmt.Errorf("failed to create metadata iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	var ids []uint64
	for iter.First(); iter.Valid(); iter.Next() {
		id, metadata, ok := decodeMetadataInvertKey(prefix, iter.Key(), iter.Value())
		if !ok || !bytes.Equal(metadata, key) {
			continue
		}
		ids = append(ids, id)
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("failed to iterate metadata entries: %w", err)
	}
	if len(ids) == 0 {
		return nil
	}
	return idx.Delete(ids...)
}

func (idx *HNSWIndex) deleteNodeFromBatch(
	batchCtx *hnswBatchContext,
	batch *pebble.Batch,
	buf *bytes.Buffer,
	id uint64,
	state *hnswMutableState,
) error {
	isOverlay, err := idx.nodeIsOverlay(batch, id)
	if err != nil {
		return err
	}

	nodeData, err := batchCtx.getNodeData(idx, id, false)
	if err != nil {
		return err
	}

	if len(nodeData.metadata) > 0 {
		if err := batch.Delete(makePebbleMetadataInvertKey(nodeData.metadata, id), nil); err != nil {
			return fmt.Errorf("failed to delete metadata invert key for node %d: %w", id, err)
		}
		// Also delete the legacy exact-metadata key if this node came from an older index.
		if err := batch.Delete(makePebbleMetadataInvertPrefix(nodeData.metadata), nil); err != nil {
			return fmt.Errorf("failed to delete legacy metadata invert key for node %d: %w", id, err)
		}
	}

	lower, upper := makePebbleNodeRange(id)
	if err := batch.DeleteRange(lower, upper, nil); err != nil {
		return fmt.Errorf("failed to add tombstone for ID %d to batch: %w", id, err)
	}
	if err := batch.Delete(makePebbleOverlayKey(id), nil); err != nil {
		return fmt.Errorf("failed to delete overlay marker for node %d: %w", id, err)
	}
	incomingEdges, err := readIncomingEdges(batch, id)
	if err != nil {
		return fmt.Errorf("reading incoming edges for node %d: %w", id, err)
	}
	if err := batch.DeleteRange(
		makePebbleReverseGraphPrefix(id),
		utils.PrefixSuccessor(makePebbleReverseGraphPrefix(id)),
		nil,
	); err != nil {
		return fmt.Errorf("failed to delete reverse edges targeting node %d: %w", id, err)
	}
	for layer, layerNeighbors := range nodeData.neighbors {
		for _, neighbor := range layerNeighbors {
			if err := batch.Delete(makePebbleReverseGraphKey(neighbor.ID, layer, id), nil); err != nil {
				return fmt.Errorf("failed to delete reverse edge %d <- %d at layer %d: %w", neighbor.ID, id, layer, err)
			}
		}
	}

	for layer, sources := range incomingEdges {
		for _, sourceID := range sources {
			currentNeighborNeighbors, neighborReadErr := idx.readNodeNeighbors(batch, sourceID, layer)
			if neighborReadErr != nil {
				if !errors.Is(neighborReadErr, ErrNotFound) {
					log.Printf("Warning: Failed to read neighbors for neighbor %d at layer %d during cleanup: %v. Skipping edge removal.",
						sourceID, layer, neighborReadErr)
				}
				continue
			}

			i := slices.IndexFunc(currentNeighborNeighbors, func(item *PriorityItem) bool {
				return item.ID == id
			})
			if i == -1 {
				continue
			}
			oldNeighbors := slices.Clone(currentNeighborNeighbors)
			newNeighbors := slices.Delete(currentNeighborNeighbors, i, i+1)

			if err := idx.writeNodeNeighbors(batch, buf, sourceID, layer, oldNeighbors, newNeighbors); err != nil {
				log.Printf("Warning: Failed to add update for neighbor %d at layer %d to batch: %v. Skipping.",
					sourceID, layer, err)
				continue
			}
			idx.updateCache(sourceID, layer, newNeighbors)
			batchCtx.updateNeighbors(sourceID, layer, newNeighbors)
		}
	}

	idx.invalidateCache(id)
	batchCtx.invalidate(id)

	if state != nil && state.activeCount > 0 {
		state.activeCount--
		if isOverlay && state.overlayCount > 0 {
			state.overlayCount--
		}
		if state.activeCount == 0 {
			state.globalEntryPoint = 0
			state.globalMaxLayer = -1
			state.baseEntryPoint = 0
			state.baseMaxLayer = -1
			state.overlayEntryPoint = 0
			state.overlayMaxLayer = -1
			state.overlayCount = 0
			return nil
		}
		if state.globalEntryPoint == id {
			if err := idx.refreshStateEntryPoint(batch, state, hnswEntryPointAny); err != nil {
				return fmt.Errorf("failed to refresh global entry point during deletion: %w", err)
			}
		}
		if state.baseEntryPoint == id {
			if err := idx.refreshStateEntryPoint(batch, state, hnswEntryPointBase); err != nil {
				return fmt.Errorf("failed to refresh base entry point during deletion: %w", err)
			}
		}
		if state.overlayCount == 0 {
			state.overlayEntryPoint = 0
			state.overlayMaxLayer = -1
		} else if state.overlayEntryPoint == id {
			if err := idx.refreshStateEntryPoint(batch, state, hnswEntryPointOverlay); err != nil {
				return fmt.Errorf("failed to refresh overlay entry point during deletion: %w", err)
			}
		}
		if state.baseEntryPoint == 0 && state.overlayCount == 0 {
			state.baseEntryPoint = state.globalEntryPoint
			state.baseMaxLayer = state.globalMaxLayer
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

	state, err := idx.loadMutableState(batch)
	if err != nil {
		return fmt.Errorf("failed to load mutable state during delete: %w", err)
	}

	buf := bytes.NewBuffer(nil)
	batchCtx := newHNSWBatchContext(batch)
	for _, id := range ids {
		if err := idx.deleteNodeFromBatch(batchCtx, batch, buf, id, state); err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return fmt.Errorf("failed to delete node %d: %w", id, err)
		}
	}

	if err := idx.persistMutableState(batch, state); err != nil {
		return fmt.Errorf("failed to update mutable state during delete: %w", err)
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
	baseEntryPoint, _ := idx.getBaseEntryPoint(idx.db)
	overlayEntryPoint, _ := idx.getOverlayEntryPoint(idx.db)
	overlayCount, _ := idx.getOverlayCount(idx.db)

	stats := map[string]any{
		"implementation":      "PebbleANN",
		"dimension":           idx.config.Dimension,
		"index_path":          idx.config.IndexPath,
		"db_path":             idx.dbPath,
		"nodes_active":        activeCount,
		"entry_point":         entryPoint,
		"base_entry_point":    baseEntryPoint,
		"overlay_entry_point": overlayEntryPoint,
		"overlay_count":       overlayCount,
		"config_M":            idx.config.Neighbors,
		"config_EfC":          idx.config.EfConstruction,
		"config_EfS":          idx.config.EfSearch,
		"sync_writes":         idx.config.PebbleSyncWrite,
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
		UpperBound: idx.nodeKeyUpperEnd,
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
	p := makePebbleMetadataInvertPrefix(prefix)
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

	prefix := makePebbleMetadataInvertPrefix(filterPrefix)
	iter, err := idx.db.NewIterWithContext(context.TODO(), &pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: utils.PrefixSuccessor(prefix),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		nodeID, metadataValue, ok := decodeMetadataInvertKey(prefix, iter.Key(), iter.Value())
		if !ok || !bytes.HasPrefix(metadataValue, filterPrefix) {
			continue
		}

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
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("failed to iterate metadata entries: %w", err)
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
	buf.Reset()
	numNeighbors := len(neighbors)
	if numNeighbors == 0 {
		return nil // Return empty slice for no neighbors
	}
	if numNeighbors > math.MaxUint32 {
		return fmt.Errorf("too many neighbors: %d exceeds maximum %d", numNeighbors, math.MaxUint32)
	}
	buf.Grow(4 + numNeighbors*serializedNeighborSize)
	var countBuf [4]byte
	binary.LittleEndian.PutUint32(countBuf[:], uint32(numNeighbors))
	if _, err := buf.Write(countBuf[:]); err != nil {
		return fmt.Errorf("failed to write neighbor count: %w", err)
	}
	var entryBuf [serializedNeighborSize]byte
	for _, n := range neighbors {
		binary.LittleEndian.PutUint64(entryBuf[:8], n.ID)
		binary.LittleEndian.PutUint32(entryBuf[8:], math.Float32bits(n.Distance))
		if _, err := buf.Write(entryBuf[:]); err != nil {
			return fmt.Errorf("failed to write neighbor id %d: %w", n.ID, err)
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
	if len(data) < 4 {
		return nil, fmt.Errorf("failed to read neighbor count: buffer too short: %d", len(data))
	}
	count := binary.LittleEndian.Uint32(data[:4])
	data = data[4:]
	requiredBytes := int(count) * serializedNeighborSize
	if len(data) < requiredBytes {
		return nil, fmt.Errorf(
			"unexpected end of data while reading neighbors: need %d bytes, got %d",
			requiredBytes,
			len(data),
		)
	}
	data = data[:requiredBytes]

	neighbors := make([]*PriorityItem, count)
	for i := range neighbors {
		offset := i * serializedNeighborSize
		neighbors[i] = &PriorityItem{
			ID:       binary.LittleEndian.Uint64(data[offset : offset+8]),
			Distance: math.Float32frombits(binary.LittleEndian.Uint32(data[offset+8 : offset+serializedNeighborSize])),
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
			return nil, ErrNotFound
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

func (idx *HNSWIndex) getNodeDataForUpdate(
	db pebble.Reader,
	id uint64,
	includeVector bool,
) (*HNSWNodeData, error) {
	layer, err := idx.getNodeLayer(db, id)
	if err != nil {
		return nil, err
	}

	nodeData := &HNSWNodeData{
		id:        id,
		layer:     layer,
		neighbors: make(map[int][]*PriorityItem, layer+1),
	}
	if includeVector {
		nodeData.vector, err = idx.readVector(db, id)
		if err != nil {
			return nil, err
		}
	}
	metadata, err := idx.readMetadata(db, id)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	if err == nil {
		nodeData.metadata = metadata
	}

	foundAnyGraph := false
	for currentLayer := 0; currentLayer <= layer; currentLayer++ {
		neighbors, err := idx.readNodeNeighbors(db, id, currentLayer)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return nil, err
		}
		nodeData.neighbors[currentLayer] = neighbors
		foundAnyGraph = true
	}
	if !foundAnyGraph {
		return nil, ErrNotFound
	}
	return nodeData, nil
}

func (idx *HNSWIndex) getNodeDataForTraversal(
	batchCtx *hnswBatchContext,
	batch *pebble.Batch,
	id uint64,
) (*HNSWNodeData, error) {
	if batchCtx != nil {
		return batchCtx.getTraversalNodeData(idx, id)
	}
	if batch != nil {
		return idx.getNodeDataForUpdate(batch, id, true)
	}
	return idx.getNodeData(nil, id)
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

	// Use iterator to scan all keys for this node. In practice this is faster
	// than multiple point-lookups when search is touching many nodes.
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
	nodeData.layer = -1
	nodeData.vector = nil
	nodeData.metadata = nil
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
				idx.releaseNodeData(nodeData)
				return nil, fmt.Errorf("decoding vector for node %d: %w", id, err)
			}
			nodeData.vector = vec
			foundVector = true
		} else if len(key) >= len(pebbleNodeKeyPrefix)+8+len(pebbleGraphKeySuffix)+1+4 &&
			bytes.HasSuffix(key[:len(key)-5], pebbleGraphKeySuffix) {
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

// searchLayerPebbleInternal performs graph traversal using PebbleDB to find candidate neighbors at a specific layer.
// Returns a sorted list of the top 'ef' closest items found.
// Assumes RLock is held.
// , candidates *PriorityQueue, results *PriorityQueue, visited map[uint64]struct{}
func (idx *HNSWIndex) searchLayerPebbleInternal(
	batchCtx *hnswBatchContext,
	batch *pebble.Batch,
	query []float32,
	filterPrefix []byte,
	entryPoint uint64,
	ef int,
	layer int,
) (*PriorityQueue, error) {
	// candidates: Min-heap storing distances of nodes to visit.
	// results: Max-heap storing distances (keeps track of the 'ef' furthest among the closest found so far).
	candidates := NewPriorityQueue(false, ef) // Min-heap
	bestSeen := NewPriorityQueue(true, ef)    // Max-heap
	filteredResults := bestSeen
	if len(filterPrefix) > 0 {
		filteredResults = NewPriorityQueue(true, ef)
	}
	visited := idx.visitedPool.Get().(map[uint64]struct{})
	defer func() {
		// Clear the map before returning to pool
		clear(visited)
		idx.visitedPool.Put(visited)
	}()

	if _, isVisited := visited[entryPoint]; !isVisited {
		epData, err := idx.getNodeDataForTraversal(batchCtx, batch, entryPoint)
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
		heap.Push(bestSeen, item)
		if len(filterPrefix) > 0 {
			if epData.metadata != nil && bytes.HasPrefix(epData.metadata, filterPrefix) {
				heap.Push(filteredResults, item)
			}
		}
		visited[entryPoint] = struct{}{}
	}

	// Perform Greedy/Beam Search
	for candidates.Len() > 0 {
		currentCandidateItem := heap.Pop(candidates).(*PriorityItem)
		// Termination should depend on the best unfiltered frontier, not filtered matches.
		if bestSeen.Len() >= ef && currentCandidateItem.Distance > bestSeen.Peek().Distance {
			break
		}

		nodeData, err := idx.getNodeDataForTraversal(batchCtx, batch, currentCandidateItem.ID)
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
			neighborData, err := idx.getNodeDataForTraversal(batchCtx, batch, neighbor.ID)
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

			addToCandidates := bestSeen.Len() < ef || neighborDist < bestSeen.Peek().Distance
			if !addToCandidates {
				continue
			}

			item := &PriorityItem{
				ID:       neighbor.ID,
				Distance: neighborDist,
				Metadata: neighborData.metadata,
			}
			if bestSeen.Len() >= ef {
				heap.Pop(bestSeen)
			}
			heap.Push(bestSeen, item)
			heap.Push(candidates, item)

			if len(filterPrefix) == 0 {
				continue
			}
			if neighborData.metadata != nil && bytes.HasPrefix(neighborData.metadata, filterPrefix) {
				if filteredResults.Len() >= ef {
					if neighborDist >= filteredResults.Peek().Distance {
						continue
					}
					heap.Pop(filteredResults)
				}
				heap.Push(filteredResults, item)
			}
		}
	}

	return filteredResults, nil
}

// Add this optimized version that works with a batch
func (idx *HNSWIndex) selectNeighborsHeuristicWithBatch(
	batchCtx *hnswBatchContext,
	batch *pebble.Batch,
	candidates []*PriorityItem,
	M int,
	layer int,
) []*PriorityItem {
	if len(candidates) <= M {
		return candidates
	}

	// Pre-load all candidate vectors
	candidateNeighbors := make(map[uint64][]*PriorityItem, len(candidates))
	for _, candidate := range candidates {
		if nodeData, err := batchCtx.getNodeData(idx, candidate.ID, false); err == nil {
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
			distBetweenNeighbors, ok := findNeighborDistance(selectedVec, candidate.ID)
			if !ok {
				continue // Skip if candidate is not in selected neighbors
			}

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

func findNeighborDistance(neighbors []*PriorityItem, id uint64) (float32, bool) {
	for _, neighbor := range neighbors {
		if neighbor.ID == id {
			return neighbor.Distance, true
		}
	}
	return 0, false
}

func (idx *HNSWIndex) writeNodeNeighbors(
	batch *pebble.Batch,
	buf *bytes.Buffer,
	nodeID uint64,
	layer int,
	oldNeighbors []*PriorityItem,
	newNeighbors []*PriorityItem,
) error {
	if err := serializeNeighbors(buf, newNeighbors); err != nil {
		return fmt.Errorf("serializing neighbors for node %d at layer %d: %w", nodeID, layer, err)
	}
	if err := batch.Set(makePebbleGraphLayerKey(nodeID, layer), buf.Bytes(), nil); err != nil {
		return fmt.Errorf("writing neighbors for node %d at layer %d: %w", nodeID, layer, err)
	}

	oldSet := make(map[uint64]struct{}, len(oldNeighbors))
	for _, neighbor := range oldNeighbors {
		oldSet[neighbor.ID] = struct{}{}
	}
	newSet := make(map[uint64]struct{}, len(newNeighbors))
	for _, neighbor := range newNeighbors {
		newSet[neighbor.ID] = struct{}{}
		if _, exists := oldSet[neighbor.ID]; exists {
			continue
		}
		if err := batch.Set(makePebbleReverseGraphKey(neighbor.ID, layer, nodeID), nil, nil); err != nil {
			return fmt.Errorf("writing reverse edge %d <- %d at layer %d: %w", neighbor.ID, nodeID, layer, err)
		}
	}
	for _, neighbor := range oldNeighbors {
		if _, exists := newSet[neighbor.ID]; exists {
			continue
		}
		if err := batch.Delete(makePebbleReverseGraphKey(neighbor.ID, layer, nodeID), nil); err != nil {
			return fmt.Errorf("deleting reverse edge %d <- %d at layer %d: %w", neighbor.ID, nodeID, layer, err)
		}
	}
	return nil
}

func insertNeighborByDistance(
	neighbors []*PriorityItem,
	newNeighbor *PriorityItem,
	maxConnections int,
) ([]*PriorityItem, bool) {
	existingIdx := slices.IndexFunc(neighbors, func(item *PriorityItem) bool {
		return item.ID == newNeighbor.ID
	})
	if existingIdx >= 0 {
		if neighbors[existingIdx].Distance <= newNeighbor.Distance {
			return neighbors, false
		}
		neighbors = slices.Delete(neighbors, existingIdx, existingIdx+1)
	}

	insertIdx := sort.Search(len(neighbors), func(i int) bool {
		return neighbors[i].Distance > newNeighbor.Distance
	})
	if len(neighbors) >= maxConnections && insertIdx == len(neighbors) {
		return neighbors, false
	}

	neighbors = append(neighbors, nil)
	copy(neighbors[insertIdx+1:], neighbors[insertIdx:])
	neighbors[insertIdx] = newNeighbor
	if len(neighbors) > maxConnections {
		neighbors = neighbors[:maxConnections]
	}
	return neighbors, true
}

// updateGraphForNode connects a new node into the HNSW graph stored in PebbleDB.
// Uses a Pebble Batch for atomicity of reciprocal updates.
// Assumes Lock is held.
func (idx *HNSWIndex) updateGraphForNode(
	batchCtx *hnswBatchContext,
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
	minDist := float32(math.MaxFloat32)
	for layer := entryPointLayer; layer > nodeLayer; layer-- {
		// Search at current layer with ef=M to find nearest point
		// currentNearest, currentNearestDist, err = idx.GetClosestEntryAtLayer(batch, nodeVector, currentNearest, currentNearestDist, layer, entryVisits)
		pqueue, err := idx.searchLayerPebbleInternal(
			batchCtx,
			batch,
			nodeVector,
			nil,
			currentNearest,
			1,
			layer,
		)
		if err != nil {
			return fmt.Errorf("searching at layer %d for node %d: %w", layer, nodeID, err)
		}
		if pqueue.Len() > 0 {
			nearest := pqueue.Best()
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

	// Now insert at all layers from nodeLayer down to 0
	for layer := nodeLayer; layer >= 0; layer-- {
		// Determine M for this layer
		M := idx.config.NeighborsHigherLayers
		if layer == 0 {
			M = idx.config.Neighbors
		}

		// Search for neighbors at this layer
		pqueue, err := idx.searchLayerPebbleInternal(
			batchCtx,
			batch,
			nodeVector,
			nil,
			currentNearest,
			int(idx.config.EfConstruction),
			layer,
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
		allCandidates := pqueue.Items(pqueue.Len()) // Get all candidates
		candidates := idx.selectNeighborsHeuristicWithBatch(batchCtx, batch, allCandidates, int(M), layer)

		// Update the new node's neighbor list at this layer
		if err := idx.writeNodeNeighbors(batch, buf, nodeID, layer, nil, candidates); err != nil {
			return fmt.Errorf(
				"failed adding node %d layer %d update to batch: %w",
				nodeID,
				layer,
				err,
			)
		}
		batchCtx.updateNeighbors(nodeID, layer, candidates)
		buf.Reset()
		// Update reciprocal connections
		for _, neighbor := range candidates {
			if neighbor.ID == nodeID {
				continue
			}

			neighborData, err := batchCtx.getNodeData(idx, neighbor.ID, false)
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
			oldNeighbors := slices.Clone(neighborNeighbors)

			// Add new node to neighbor's connections
			maxConnections := int(M)
			updatedNeighbors, changed := insertNeighborByDistance(
				neighborNeighbors,
				&PriorityItem{ID: nodeID, Distance: neighbor.Distance},
				maxConnections,
			)
			if !changed {
				continue
			}
			neighborNeighbors = updatedNeighbors

			// Save updated neighbor connections

			if err := idx.writeNodeNeighbors(batch, buf, neighbor.ID, layer, oldNeighbors, neighborNeighbors); err != nil {
				return fmt.Errorf(
					"failed adding neighbor %d layer %d update to batch: %w",
					neighbor.ID,
					layer,
					err,
				)
			}
			buf.Reset()
			idx.updateCache(neighbor.ID, layer, neighborNeighbors)
			batchCtx.updateNeighbors(neighbor.ID, layer, neighborNeighbors)
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
