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

package vectorindex

import (
	"bytes"
	"cmp"
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ajroetker/go-highway/hwy/contrib/vec"
	"github.com/antflydb/antfly/lib/utils"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vector/allocator"
	"github.com/antflydb/antfly/lib/vector/cluster"
	"github.com/antflydb/antfly/lib/vector/quantize"
	"github.com/cespare/xxhash/v2"
	"github.com/chewxy/math32"
	"github.com/cockroachdb/pebble/v2"
	"github.com/jellydator/ttlcache/v3"
	"google.golang.org/protobuf/proto"
)

const (
	hbcIndexVersion        = 1
	defaultBranchingFactor = 16
	defaultLeafSize        = 100
)

// PebbleDB key prefixes for HBC
var (
	hbcIndexPrefix       = []byte("\x00\x00hbc:") // Key prefix for HBC index
	hbcMetadataKeySuffix = []byte(":m")           // Suffix for metadata keys
	hbcChildrenKeySuffix = []byte(":c")           // Suffix for children keys
	hbcParentKeySuffix   = []byte(":p")           // Suffix for parent key
	hbcCentroidKeySuffix = []byte(":ct")          // Suffix for centroid vector
	hbcMembersKeySuffix  = []byte(":mb")          // Suffix for cluster members (leaf nodes)

	// Global metadata keys
	hbcIndexMetaPrefix = []byte("meta:")

	// Quantized data keys
	hbcQuantizedKeySuffix = []byte(":q") // Suffix for quantized vector data
)

// HBCConfig holds configuration for the HBC index
type HBCConfig struct {
	Dimension      uint32
	DistanceMetric vector.DistanceMetric
	Name           string

	SplitAlgo vector.ClustAlgorithm

	// https://proceedings.nips.cc/paper_files/paper/2021/file/299dc35e747eb77177d9cea10a802da2-Paper.pdf
	// Episilon1 float32 // Threshold for RNG rule in SPANN
	// We do not use the RNG Epsilon1 as we want to be able to handle updates
	Episilon2 float32 // Threshold for Query aware dynamic pruning in SPANN

	// HBC specific parameters
	BranchingFactor int // Number of children per internal node
	LeafSize        int // Maximum vectors per leaf cluster
	SearchWidth     int // Number of clusters to explore during search

	// Quantization configuration
	UseQuantization     bool // Whether to use quantization for non-root nodes
	DisableReranking    bool
	QuantizerSeed       uint64 // Seed for quantizer initialization
	UseRandomOrthoTrans bool

	// Pebble configuration
	CacheSizeNodes int
	CacheTTL       time.Duration // TTL for cached nodes
	VectorDB       *pebble.DB    // Required: PebbleDB instance for storing vectors
	IndexDB        *pebble.DB    // Required: PebbleDB instance for storing index data (often same as VectorDB)

	// SyncLevel controls the durability of batch commits.
	// Defaults to pebble.Sync if nil.
	SyncLevel *pebble.WriteOptions

	// Result collapsing configuration
	// CollapseKeysFunc optionally deduplicates results by extracting a collapse key
	// from metadata. If nil, no collapsing is performed.
	// Example: Extract document key from chunk keys to return best chunk per document.
	CollapseKeysFunc func(metadataKey []byte) []byte
}

// hbcIndexMetadata holds the global index metadata
type hbcIndexMetadata struct {
	Version           uint32
	Dimension         uint32
	HasDistanceMetric bool
	DistanceMetric    vector.DistanceMetric
	BranchingFactor   int64
	LeafSize          int64
	RootNode          uint64
	ActiveCount       uint64
	NodeCount         uint64
	UseQuantization   bool
	QuantizerSeed     uint64
}

// HBCNode represents a node in the hierarchical clustering tree
type HBCNode struct {
	ID       uint64
	IsLeaf   bool
	Centroid vector.T
	Children []uint64 // For internal nodes
	Members  []uint64 // For leaf nodes (vector IDs)
	Parent   uint64
	// FIXME (ajr) Make level in graph work
	Level            int
	QuantizedVectors quantize.QuantizedVectorSet // Quantized vectors for this node (nil for root)
}

// reset clears the node for reuse from the pool
func (n *HBCNode) reset() {
	n.ID = 0
	n.IsLeaf = false
	clear(n.Centroid)
	n.Children = n.Children[:0]
	n.Members = n.Members[:0]
	n.Parent = 0
	n.Level = 0
	if n.QuantizedVectors != nil {
		n.QuantizedVectors.Clear(n.Centroid)
	}
}

// We only CopyFrom when altering the node, during insertions or updates
// in which case we recompute the QuantizedVectors anyways
func (n *HBCNode) CopyFrom(other *HBCNode) {
	n.ID = other.ID
	n.IsLeaf = other.IsLeaf
	copy(n.Centroid, other.Centroid)
	n.Children = slices.Clone(other.Children)
	n.Members = slices.Clone(other.Members)
	n.Parent = other.Parent
	n.Level = other.Level

	// TODO (ajr) This is super expensive
	// if other.QuantizedVectors != nil {
	// n.QuantizedVectors = other.QuantizedVectors.Clone()
	// }
}

// HBCIndex implements hierarchical balanced clustering with Pebble storage
type HBCIndex struct {
	// Global metadata lock - only for metadata operations
	metaMu sync.RWMutex

	commitMu sync.Mutex // Mutex for commit operations

	config   HBCConfig
	syncOpt  *pebble.WriteOptions
	suffix   []byte     // Suffix for vector keys from the main pebble instance, e.g. ":i:my_index:e"
	prefix   []byte     // Unique ID for this index instance
	vectorDB *pebble.DB // Database for storing vectors (same as config.VectorDB)
	indexDB  *pebble.DB // Database for storing index data (same as config.IndexDB)

	// Cache for nodes
	nodeCache   *ttlcache.Cache[uint64, *HBCNode]
	readerCache *readerCache
	writerCache *writerCache

	// Pools for memory reuse
	nodePool *sync.Pool

	clusterer cluster.Clusterer
	// writerAllocator is used for allocating memory (satisfies the StackAllocator requirements)
	writerAllocator allocator.Allocator
	quantizer       quantize.Quantizer
	rootQuantizer   quantize.Quantizer

	// rot is used for random orthogonal transformations (see the RaBit quantizer paper)
	//
	// in practice this doesn't seem necessary
	rot *vector.RandomOrthogonalTransformer
}

// Helper functions for key generation
func makeHBCMetadataKey(prefix []byte, vectorID uint64) []byte {
	key := make([]byte, len(prefix)+8+len(hbcMetadataKeySuffix))
	copy(key, prefix)
	binary.BigEndian.PutUint64(key[len(prefix):], vectorID)
	copy(key[len(prefix)+8:], hbcMetadataKeySuffix)
	return key
}

func makeHBCCentroidKey(prefix []byte, nodeID uint64) []byte {
	key := make([]byte, len(prefix)+8+len(hbcCentroidKeySuffix))
	copy(key, prefix)
	binary.BigEndian.PutUint64(key[len(prefix):], nodeID)
	copy(key[len(prefix)+8:], hbcCentroidKeySuffix)
	return key
}

func makeHBCChildrenKey(prefix []byte, nodeID uint64) []byte {
	key := make([]byte, len(prefix)+8+len(hbcChildrenKeySuffix))
	copy(key, prefix)
	binary.BigEndian.PutUint64(key[len(prefix):], nodeID)
	copy(key[len(prefix)+8:], hbcChildrenKeySuffix)
	return key
}

func makeHBCMembersKey(prefix []byte, nodeID uint64) []byte {
	key := make([]byte, len(prefix)+8+len(hbcMembersKeySuffix))
	copy(key, prefix)
	binary.BigEndian.PutUint64(key[len(prefix):], nodeID)
	copy(key[len(prefix)+8:], hbcMembersKeySuffix)
	return key
}

func makeHBCParentKey(prefix []byte, nodeID uint64) []byte {
	key := make([]byte, len(prefix)+8+len(hbcParentKeySuffix))
	copy(key, prefix)
	binary.BigEndian.PutUint64(key[len(prefix):], nodeID)
	copy(key[len(prefix)+8:], hbcParentKeySuffix)
	return key
}

func makeHBCQuantizedKey(prefix []byte, nodeID uint64) []byte {
	key := make([]byte, len(prefix)+8+len(hbcQuantizedKeySuffix))
	copy(key, prefix)
	binary.BigEndian.PutUint64(key[len(prefix):], nodeID)
	copy(key[len(prefix)+8:], hbcQuantizedKeySuffix)
	return key
}

// createClusterer creates the appropriate clusterer based on the split algorithm
func createClusterer(
	splitAlgo vector.ClustAlgorithm,
	allocator allocator.Allocator,
	randSource rand.Source,
	distanceMetric vector.DistanceMetric,
) cluster.Clusterer {
	switch splitAlgo {
	case vector.ClustAlgorithm_Hilbert:
		return &cluster.HilbertClusterer{
			Allocator:      allocator,
			DistanceMetric: distanceMetric,
		}
	case vector.ClustAlgorithm_Kmeans:
		fallthrough
	default:
		return &cluster.BalancedBinaryKmeans{
			Allocator:      allocator,
			MaxIterations:  16,
			Rand:           rand.New(randSource), //nolint:gosec // G404: non-security randomness for ML/jitter
			DistanceMetric: distanceMetric,
		}
	}
}

// NewHBCIndex creates a new HBC index
func NewHBCIndex(config HBCConfig, randSource rand.Source) (*HBCIndex, error) {
	if config.Name == "" {
		return nil, errors.New("name must be provided")
	}
	if config.Dimension <= 0 {
		return nil, errors.New("dimension must be positive")
	}
	if config.VectorDB == nil {
		return nil, errors.New("vectorDB must be provided")
	}
	if config.IndexDB == nil {
		return nil, errors.New("indexDB must be provided")
	}
	if config.DistanceMetric == 0 {
		// Default to L2Squared; Cosine also works but requires normalized vectors
		config.DistanceMetric = vector.DistanceMetric_L2Squared
	}
	if config.BranchingFactor <= 0 {
		config.BranchingFactor = defaultBranchingFactor
	}
	if config.LeafSize <= 0 {
		config.LeafSize = defaultLeafSize
	}
	if config.SearchWidth <= 0 {
		config.SearchWidth = config.BranchingFactor * 2
	}
	if config.SplitAlgo == 0 {
		config.SplitAlgo = vector.ClustAlgorithm_Kmeans
	}

	var stackAllocator allocator.StackAllocator

	// Set default cache TTL if not specified
	if config.CacheTTL == 0 {
		config.CacheTTL = 5 * time.Minute
	}

	// Set default cache size if not specified
	cacheSize := config.CacheSizeNodes
	if cacheSize == 0 {
		cacheSize = 10000
	}

	// Create TTL cache
	nodeCache := ttlcache.New(
		ttlcache.WithTTL[uint64, *HBCNode](config.CacheTTL),
		ttlcache.WithCapacity[uint64, *HBCNode](uint64(cacheSize)),
	)

	syncOpt := config.SyncLevel
	if syncOpt == nil {
		syncOpt = pebble.Sync
	}

	idx := &HBCIndex{
		config:    config,
		syncOpt:   syncOpt,
		suffix:    fmt.Appendf(nil, ":i:%s:e", config.Name),
		prefix:    fmt.Appendf(bytes.Clone(hbcIndexPrefix), "%d:", xxhash.Sum64String(config.Name)),
		vectorDB:  config.VectorDB,
		indexDB:   config.IndexDB,
		nodeCache: nodeCache,
		nodePool: &sync.Pool{
			New: func() any {
				return &HBCNode{
					Centroid: make(vector.T, config.Dimension),
				}
			},
		},

		clusterer: createClusterer(
			config.SplitAlgo,
			&stackAllocator,
			randSource,
			config.DistanceMetric,
		),
		writerAllocator: &stackAllocator,
	}
	idx.readerCache = &readerCache{idx: idx, cache: nodeCache}
	idx.writerCache = newWriteCache(idx, nodeCache)

	// Set up cache eviction callback to return nodes to pool
	nodeCache.OnEviction(
		func(ctx context.Context, reason ttlcache.EvictionReason, item *ttlcache.Item[uint64, *HBCNode]) {
			node := item.Value()
			if node != nil {
				// Reset and return node to pool
				node.reset()
				idx.nodePool.Put(node)
			}
		},
	)

	// Initialize quantizer if enabled
	if config.UseQuantization {
		if config.QuantizerSeed == 0 {
			config.QuantizerSeed = rand.Uint64() //nolint:gosec // G404: non-security randomness for ML/jitter
		}
		idx.quantizer = quantize.NewRaBitQuantizer(
			int(config.Dimension),
			config.QuantizerSeed,
			config.DistanceMetric,
		)
		idx.rootQuantizer = quantize.NewNonQuantizer(int(config.Dimension), config.DistanceMetric)
	} else {
		idx.quantizer = quantize.NewNonQuantizer(int(config.Dimension), config.DistanceMetric)
		idx.rootQuantizer = idx.quantizer
	}
	idx.rot = &vector.RandomOrthogonalTransformer{}
	// Initialize random orthogonal transformer if enabled
	// Note: this is not strictly necessary and in practice does not seem to help
	// with search quality, but is included here for completeness
	//
	// See: https://arxiv.org/abs/2006.01875 and https://arxiv.org/abs/2202.09361
	if idx.config.UseRandomOrthoTrans {
		idx.rot.Init(vector.RotAlgorithm_Givens, int(config.Dimension), config.QuantizerSeed)
	} else {
		idx.rot.Init(vector.RotAlgorithm_None, int(config.Dimension), config.QuantizerSeed)
	}

	// Load or initialize index
	if exists, err := idx.loadIndex(); err != nil {
		return nil, fmt.Errorf("loading index: %w", err)
	} else if !exists {
		if err := idx.initializeNewIndex(); err != nil {
			return nil, fmt.Errorf("initializing index: %w", err)
		}
	}

	return idx, nil
}

// Name returns the index name
func (idx *HBCIndex) Name() string {
	return idx.config.Name
}

// loadIndex loads existing index metadata
func (idx *HBCIndex) loadIndex() (bool, error) {
	indexMetaKey := append(bytes.Clone(idx.prefix), hbcIndexMetaPrefix...)
	value, closer, err := idx.indexDB.Get(indexMetaKey)
	if err != nil {
		if err == pebble.ErrNotFound {
			return false, nil
		}
		return false, fmt.Errorf("loading index metadata: %w", err)
	}
	defer func() { _ = closer.Close() }()

	var meta hbcIndexMetadata
	buf := bytes.NewReader(value)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&meta); err != nil {
		return true, fmt.Errorf("decoding metadata: %w", err)
	}

	if meta.Version != hbcIndexVersion {
		return true, fmt.Errorf("unsupported version: %d", meta.Version)
	}
	if meta.Dimension != idx.config.Dimension {
		return true, fmt.Errorf("dimension mismatch index: %d, config: %d",
			meta.Dimension, idx.config.Dimension)
	}
	if meta.HasDistanceMetric && meta.DistanceMetric != idx.config.DistanceMetric {
		return true, fmt.Errorf("distance metric mismatch index: %s, config: %s",
			meta.DistanceMetric, idx.config.DistanceMetric)
	}

	idx.config.BranchingFactor = int(meta.BranchingFactor)
	idx.config.LeafSize = int(meta.LeafSize)
	idx.config.UseQuantization = meta.UseQuantization
	idx.config.QuantizerSeed = meta.QuantizerSeed

	// Re-initialize quantizer with loaded configuration
	if idx.config.UseQuantization {
		idx.quantizer = quantize.NewRaBitQuantizer(
			int(idx.config.Dimension),
			idx.config.QuantizerSeed,
			idx.config.DistanceMetric,
		)
		idx.rootQuantizer = quantize.NewNonQuantizer(
			int(idx.config.Dimension),
			idx.config.DistanceMetric,
		)
	}

	return true, nil
}

// initializeNewIndex creates a new empty index
func (idx *HBCIndex) initializeNewIndex() error {
	batch := idx.indexDB.NewIndexedBatch()
	defer func() { _ = batch.Close() }()

	// Create root node
	rootID := uint64(1)
	root := &HBCNode{
		ID:       rootID,
		IsLeaf:   true,
		Centroid: make(vector.T, idx.config.Dimension),
		Members:  []uint64{},
		Level:    0,
	}
	nodeCache := &readerCache{
		idx:   idx,
		cache: idx.nodeCache,
	}

	if err := idx.saveNode(batch, nodeCache, root); err != nil {
		return fmt.Errorf("failed to save root node: %w", err)
	}

	// Save metadata
	meta := hbcIndexMetadata{
		Version:           hbcIndexVersion,
		Dimension:         idx.config.Dimension,
		HasDistanceMetric: true,
		DistanceMetric:    idx.config.DistanceMetric,
		BranchingFactor:   int64(idx.config.BranchingFactor),
		LeafSize:          int64(idx.config.LeafSize),
		RootNode:          rootID,
		ActiveCount:       0,
		NodeCount:         1,
		UseQuantization:   idx.config.UseQuantization,
		QuantizerSeed:     idx.config.QuantizerSeed,
	}

	if err := idx.saveMetadata(batch, &meta); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	return batch.Commit(pebble.Sync)
}

// saveMetadata saves index metadata
func (idx *HBCIndex) saveMetadata(batch *pebble.Batch, meta *hbcIndexMetadata) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(meta); err != nil {
		return fmt.Errorf("failed to encode metadata: %w", err)
	}

	indexMetaKey := append(bytes.Clone(idx.prefix), hbcIndexMetaPrefix...)
	return batch.Set(indexMetaKey, buf.Bytes(), nil)
}

var ErrIndexClosed = errors.New("index closed")
var ErrIndexNotEmpty = errors.New("index not empty")
var ErrDuplicateVectorID = errors.New("duplicate vector id")

// getMetadata retrieves current metadata
func (idx *HBCIndex) getMetadata(db pebble.Reader) (*hbcIndexMetadata, error) {
	indexMetaKey := append(bytes.Clone(idx.prefix), hbcIndexMetaPrefix...)
	value, closer, err := db.Get(indexMetaKey)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}
	defer func() { _ = closer.Close() }()

	var meta hbcIndexMetadata
	buf := bytes.NewReader(value)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&meta); err != nil {
		return nil, fmt.Errorf("failed to decode metadata: %w", err)
	}

	return &meta, nil
}

// saveQuantizedVectors updates the quantized vector set for a node
func (idx *HBCIndex) saveQuantizedVectors(batch *pebble.Batch, node *HBCNode) error {
	if !idx.config.UseQuantization {
		return nil
	}

	var vectorSet *vector.Set
	if node.IsLeaf {
		// Load all vectors for this node
		vectorSet = idx.writerAllocator.AllocVectorSet(len(node.Members), int(idx.config.Dimension))
		defer idx.writerAllocator.FreeVectorSet(vectorSet)
		for i, childID := range node.Members {
			// FIXME (ajr): We need to do this for members but get the centroids for children
			if _, err := idx.GetVector(batch, childID, vectorSet.At(i)); err != nil {
				return fmt.Errorf("loading vector %d: %w", childID, err)
			}
			idx.TransformVector(vectorSet.At(i), vectorSet.At(i))
		}
	} else {
		// Load all centroids for this node's children
		vectorSet = idx.writerAllocator.AllocVectorSet(len(node.Children), int(idx.config.Dimension))
		defer idx.writerAllocator.FreeVectorSet(vectorSet)
		for i, childID := range node.Children {
			vecKey := makeHBCCentroidKey(idx.prefix, childID)
			vecData, closer, err := batch.Get(vecKey)
			if err != nil {
				return fmt.Errorf("loading vector %d: %w", childID, err)
			}
			_, err = vector.DecodeTo(vecData, vectorSet.At(i))
			if err != nil {
				return fmt.Errorf("decoding centroid vector %d: %w", childID, err)
			}
			_ = closer.Close()
			// Centroids are means of transformed vectors and need normalization
			// for Cosine distance since the mean of unit vectors is not a unit vector.
			if idx.config.DistanceMetric == vector.DistanceMetric_Cosine {
				vec.NormalizeFloat32(vectorSet.At(i))
			}
		}
	}

	// Quantize vectors
	var quantizer quantize.Quantizer
	if node.Parent == 0 {
		create := true
		if node.QuantizedVectors != nil {
			if _, ok := node.QuantizedVectors.(*quantize.NonQuantizedVectorSet); ok {
				// Clear existing quantized vectors for reuse
				node.QuantizedVectors.Clear(node.Centroid)
				create = false
			}
		}
		if create {
			node.QuantizedVectors = idx.rootQuantizer.NewSet(
				int(vectorSet.GetCount()),
				node.Centroid,
			)
		}
		quantizer = idx.rootQuantizer
		// node.QuantizedVectors = idx.rootQuantizer.Quantize(idx.workspace, vectorSet)
	} else {
		create := true
		if node.QuantizedVectors != nil {
			if _, ok := node.QuantizedVectors.(*quantize.RaBitQuantizedVectorSet); ok {
				// Clear existing quantized vectors for reuse
				node.QuantizedVectors.Clear(node.Centroid)
				create = false
			}
		}
		if create {
			node.QuantizedVectors = idx.quantizer.NewSet(int(vectorSet.GetCount()), node.Centroid)
		}
		quantizer = idx.quantizer
		// node.QuantizedVectors = idx.quantizer.Quantize(idx.workspace, vectorSet)
	}
	quantizer.QuantizeWithSet(idx.writerAllocator, node.QuantizedVectors, vectorSet)

	// Serialize and save quantized data
	quantizedKey := makeHBCQuantizedKey(idx.prefix, node.ID)
	quantizedData := idx.serializeQuantizedSet(node.QuantizedVectors)
	return batch.Set(quantizedKey, quantizedData, nil)
}

func (idx *HBCIndex) saveNodeParentOnly(
	batch *pebble.Batch,
	nodeCache nodeCache,
	node *HBCNode,
) error {
	// Save parent
	if node.Parent != 0 {
		parentKey := makeHBCParentKey(idx.prefix, node.ID)
		parentData := make([]byte, 8)
		binary.LittleEndian.PutUint64(parentData, node.Parent)
		if err := batch.Set(parentKey, parentData, nil); err != nil {
			return fmt.Errorf("failed to save parent: %w", err)
		}
	}
	// Update cache
	nodeCache.Set(node)
	return nil
}

// saveNode saves a cluster node to PebbleDB
func (idx *HBCIndex) saveNodeNoUpdateQuantizedVectors(
	batch *pebble.Batch,
	nodeCache nodeCache,
	node *HBCNode,
) error {
	// Serialize and save quantized data
	if node.QuantizedVectors != nil {
		quantizedKey := makeHBCQuantizedKey(idx.prefix, node.ID)
		quantizedData := idx.serializeQuantizedSet(node.QuantizedVectors)
		if err := batch.Set(quantizedKey, quantizedData, nil); err != nil {
			return fmt.Errorf("saving quantized vectors: %w", err)
		}
	}
	// Save centroid
	centroidKey := makeHBCCentroidKey(idx.prefix, node.ID)
	// TODO (ajr) Save a quantized version of the centroid for easier searching
	vecData := make([]byte, 0, 4*(len(node.Centroid)+1))
	vecData, _ = vector.Encode(vecData, node.Centroid)
	if err := batch.Set(centroidKey, vecData, nil); err != nil {
		return fmt.Errorf("saving centroid: %w", err)
	}

	// Save parent
	if node.Parent != 0 {
		parentKey := makeHBCParentKey(idx.prefix, node.ID)
		parentData := make([]byte, 8)
		binary.LittleEndian.PutUint64(parentData, node.Parent)
		if err := batch.Set(parentKey, parentData, nil); err != nil {
			return fmt.Errorf("saving parent: %w", err)
		}
	}

	// Save children or members
	if node.IsLeaf {
		membersKey := makeHBCMembersKey(idx.prefix, node.ID)
		membersData := make([]byte, 8*len(node.Members))
		for i, member := range node.Members {
			binary.LittleEndian.PutUint64(membersData[i*8:], member)
		}
		if err := batch.Set(membersKey, membersData, nil); err != nil {
			return fmt.Errorf("saving members: %w", err)
		}
	} else {
		childrenKey := makeHBCChildrenKey(idx.prefix, node.ID)
		childrenData := make([]byte, 8*len(node.Children))
		for i, child := range node.Children {
			binary.LittleEndian.PutUint64(childrenData[i*8:], child)
		}
		if err := batch.Set(childrenKey, childrenData, nil); err != nil {
			return fmt.Errorf("saving children: %w", err)
		}
	}

	// Update cache
	nodeCache.Set(node)

	return nil
}

// saveNode saves a cluster node to PebbleDB
func (idx *HBCIndex) saveNode(
	batch *pebble.Batch,
	nodeCache nodeCache,
	node *HBCNode,
	addVecs ...vector.T,
) error {
	// Update quantized vectors with the new vector(s)
	var addVecSet *vector.Set
	if len(addVecs) > 0 {
		addVecSet = addVecs[0].AsSet()
		for i := 1; i < len(addVecs); i++ {
			addVecSet.Add(addVecs[i])
		}
	}
	if err := idx.updateQuantizedVectors(batch, node, addVecSet); err != nil {
		return fmt.Errorf("saving quantized vectors: %w", err)
	}

	// Save centroid
	centroidKey := makeHBCCentroidKey(idx.prefix, node.ID)
	// TODO (ajr) Save a quantized version of the centroid for easier searching
	vecData := make([]byte, 0, 4*(len(node.Centroid)+1))
	vecData, _ = vector.Encode(vecData, node.Centroid)
	if err := batch.Set(centroidKey, vecData, nil); err != nil {
		return fmt.Errorf("failed to save centroid: %w", err)
	}

	// Save parent
	if node.Parent != 0 {
		parentKey := makeHBCParentKey(idx.prefix, node.ID)
		parentData := make([]byte, 8)
		binary.LittleEndian.PutUint64(parentData, node.Parent)
		if err := batch.Set(parentKey, parentData, nil); err != nil {
			return fmt.Errorf("failed to save parent: %w", err)
		}
	}

	// Save children or members
	if node.IsLeaf {
		membersKey := makeHBCMembersKey(idx.prefix, node.ID)
		membersData := make([]byte, 8*len(node.Members))
		for i, member := range node.Members {
			binary.LittleEndian.PutUint64(membersData[i*8:], member)
		}
		if err := batch.Set(membersKey, membersData, nil); err != nil {
			return fmt.Errorf("failed to save members: %w", err)
		}
	} else {
		childrenKey := makeHBCChildrenKey(idx.prefix, node.ID)
		childrenData := make([]byte, 8*len(node.Children))
		for i, child := range node.Children {
			binary.LittleEndian.PutUint64(childrenData[i*8:], child)
		}
		if err := batch.Set(childrenKey, childrenData, nil); err != nil {
			return fmt.Errorf("failed to save children: %w", err)
		}
	}

	// Update cache
	nodeCache.Set(node)

	return nil
}

// updateQuantizedVectors updates the quantized vector set for a node
func (idx *HBCIndex) updateQuantizedVectors(
	batch *pebble.Batch,
	node *HBCNode,
	addVecs *vector.Set, // Optional additional vectors to add
) error {
	if !idx.config.UseQuantization {
		return nil
	}

	if addVecs != nil && node.QuantizedVectors != nil && node.QuantizedVectors.GetCount() > 0 &&
		addVecs.GetCount() > 0 {
		// Quantize vectors
		if node.Parent == 0 {
			if _, ok := node.QuantizedVectors.(*quantize.NonQuantizedVectorSet); !ok {
				return idx.saveQuantizedVectors(batch, node)
			}
			idx.rootQuantizer.QuantizeWithSet(idx.writerAllocator,
				node.QuantizedVectors, addVecs)
		} else {
			if _, ok := node.QuantizedVectors.(*quantize.RaBitQuantizedVectorSet); !ok {
				return idx.saveQuantizedVectors(batch, node)
			}
			idx.quantizer.QuantizeWithSet(idx.writerAllocator,
				node.QuantizedVectors, addVecs)
		}
		// Serialize and save quantized data
		quantizedKey := makeHBCQuantizedKey(idx.prefix, node.ID)
		quantizedData := idx.serializeQuantizedSet(node.QuantizedVectors)
		return batch.Set(quantizedKey, quantizedData, nil)
	}

	return idx.saveQuantizedVectors(batch, node)
}

func (idx *HBCIndex) loadNodeNoCache(db pebble.Reader, nodeID uint64) (*HBCNode, error) {
	node := idx.nodePool.Get().(*HBCNode)
	node.reset()
	node.ID = nodeID

	// Load centroid
	centroidKey := makeHBCCentroidKey(idx.prefix, nodeID)
	centroidData, closer, err := db.Get(centroidKey)
	if err != nil {
		node.reset()
		idx.nodePool.Put(node)
		return nil, fmt.Errorf("loading centroid for %d: %w", nodeID, err)
	}

	node.Centroid = make(vector.T, idx.config.Dimension)
	_, err = vector.DecodeTo(centroidData, node.Centroid)
	if err != nil {
		node.reset()
		idx.nodePool.Put(node)
		return nil, fmt.Errorf("loading centroid for %d: %w", nodeID, err)
	}

	_ = closer.Close()

	// Load parent
	parentKey := makeHBCParentKey(idx.prefix, nodeID)

	if parentData, closer, err := db.Get(parentKey); err == nil {
		node.Parent = binary.LittleEndian.Uint64(parentData)
		_ = closer.Close()
	}

	// Check if leaf by trying to load members
	membersKey := makeHBCMembersKey(idx.prefix, nodeID)

	if membersData, closer, err := db.Get(membersKey); err == nil {
		node.IsLeaf = true
		node.Members = make([]uint64, len(membersData)/8)
		for i := range node.Members {
			node.Members[i] = binary.LittleEndian.Uint64(membersData[i*8:])
		}
		_ = closer.Close()
	} else {

		// Load children
		childrenKey := makeHBCChildrenKey(idx.prefix, nodeID)
		if childrenData, closer, err := db.Get(childrenKey); err == nil {
			node.IsLeaf = false
			node.Children = make([]uint64, len(childrenData)/8)
			for i := range node.Children {
				node.Children[i] = binary.LittleEndian.Uint64(childrenData[i*8:])
			}
			_ = closer.Close()
		}
	}

	if len(node.Centroid) != int(idx.config.Dimension) {
		node.reset()
		idx.nodePool.Put(node)
		return nil, fmt.Errorf("centroid dimension mismatch for node %d: expected %d, got %d",
			nodeID, idx.config.Dimension, len(node.Centroid))
	}

	// Load quantized vectors if available

	if idx.config.UseQuantization {
		quantizedKey := makeHBCQuantizedKey(idx.prefix, nodeID)
		if quantizedData, closer, err := db.Get(quantizedKey); err == nil {
			_ = closer.Close()
			expectedCount := len(node.Children)
			if node.IsLeaf {
				expectedCount = len(node.Members)
			}
			node.QuantizedVectors, err = idx.deserializeQuantizedSet(
				quantizedData, node.Parent == 0, expectedCount)
			if err != nil {
				node.reset()
				idx.nodePool.Put(node)
				return nil, fmt.Errorf("loading quantized vectors for node %d: %w", nodeID, err)
			}
		}
	}

	return node, nil
}

// loadNode loads a cluster node from PebbleDB
func (idx *HBCIndex) loadNode(
	db pebble.Reader,
	cacheBatch nodeCache,
	nodeID uint64,
) (*HBCNode, error) {
	if cacheBatch == nil {
		cacheBatch = &readerCache{
			idx:   idx,
			cache: idx.nodeCache,
		}
	}

	// Check cache first
	node, err := cacheBatch.GetOrSet(db, nodeID)
	if err != nil {
		return nil, fmt.Errorf("loading node %d from cache: %w", nodeID, err)
	}
	if node == nil {
		return nil, fmt.Errorf("node %d not found in cache", nodeID)
	}
	return node, nil
}

type writerCache struct {
	idx     *HBCIndex
	batch   map[uint64]*HBCNode
	deletes map[uint64]struct{}
	cache   *ttlcache.Cache[uint64, *HBCNode]
}

func newWriteCache(idx *HBCIndex, cache *ttlcache.Cache[uint64, *HBCNode]) *writerCache {
	return &writerCache{
		idx:     idx,
		batch:   make(map[uint64]*HBCNode, 5000),
		deletes: make(map[uint64]struct{}, 500),
		cache:   cache,
	}
}

func (ncv *writerCache) Reset() {
	// Reset the batch and deletes
	clear(ncv.batch)
	clear(ncv.deletes)
}

func (ncb *writerCache) Commit() error {
	for id, node := range ncb.batch {
		// Save to cache
		ncb.cache.Set(id, node, ttlcache.DefaultTTL)
	}

	for id := range ncb.deletes {
		// Delete from cache
		ncb.cache.Delete(id)
	}

	ncb.Reset()
	return nil
}

func (ncb *writerCache) Set(node *HBCNode) {
	// Add to batch
	c := ncb.idx.nodePool.Get().(*HBCNode)
	c.CopyFrom(node)
	if node.QuantizedVectors != nil {
		c.QuantizedVectors = node.QuantizedVectors.Clone()
	}
	ncb.batch[c.ID] = c
}

func (ncb *writerCache) Delete(nodeID uint64) {
	// Add to deletes
	ncb.deletes[nodeID] = struct{}{}
}

func (ncb *writerCache) GetOrSet(db pebble.Reader, nodeID uint64) (*HBCNode, error) {
	node, exists := ncb.batch[nodeID]
	if !exists {
		var err error
		if item, _ /* created */ := ncb.cache.GetOrSetFunc(nodeID, func() *HBCNode {
			node, err = ncb.idx.loadNodeNoCache(db, nodeID)
			if err != nil {
				return nil
			}
			return node
		}); item != nil {
			return item.Value(), err
		}
		return nil, err
	}
	return node, nil
}

type nodeCache interface {
	GetOrSet(db pebble.Reader, nodeID uint64) (*HBCNode, error)
	Set(node *HBCNode)
	Delete(nodeID uint64)
}
type readerCache struct {
	idx   *HBCIndex
	cache *ttlcache.Cache[uint64, *HBCNode]
}

func (ncb *readerCache) Set(node *HBCNode) {
	ncb.cache.Set(node.ID, node, ttlcache.DefaultTTL)
}

func (ncb *readerCache) Delete(nodeID uint64) {
	ncb.cache.Delete(nodeID)
}

func (ncb *readerCache) GetOrSet(db pebble.Reader, nodeID uint64) (*HBCNode, error) {
	var err error
	if item, _ /* created */ := ncb.cache.GetOrSetFunc(nodeID, func() *HBCNode {
		var node *HBCNode
		node, err = ncb.idx.loadNodeNoCache(db, nodeID)
		if err != nil {
			return nil
		}
		return node
	}); item != nil {
		return item.Value(), err
	}
	return nil, err
}

func (idx *HBCIndex) Batch(ctx context.Context, b *Batch) (err error) {
	defer func() {
		if r := recover(); r != nil {
			idx.writerCache.Reset()
			switch e := r.(type) {
			case error:
				// FIXME (probably need to work out better shutdown semantics, this has a code smell)
				if errors.Is(e, pebble.ErrClosed) {
					err = e
					return
				}
			}
			panic(r)
		}
		if err != nil {
			idx.writerCache.Reset()
		}
	}()
	vectors := b.Vectors
	ids := b.IDs
	metadataList := b.MetadataList

	// Early return only if there are no operations at all
	if len(vectors) == 0 && len(b.Deletes) == 0 {
		return nil
	}

	// Validate insertions only if there are vectors to insert
	if len(vectors) > 0 {
		if len(ids) != len(vectors) {
			return errors.New("ids and vectors must have same length")
		}

		if metadataList != nil && len(metadataList) != len(vectors) {
			return errors.New("metadata list must match vectors length")
		}

		for _, vec := range vectors {
			if uint32(len(vec)) != idx.config.Dimension { //nolint:gosec // G115: bounded value, cannot overflow in practice
				return fmt.Errorf(
					"dimension mismatch: expected %d, got %d",
					idx.config.Dimension,
					len(vec),
				)
			}
		}
	}

	batch := idx.indexDB.NewIndexedBatchWithSize(
		32 << 20,
	) // 32MB Initial size estimate, will grow as needed
	defer func() { _ = batch.Close() }()

	// Get current metadata with read lock
	idx.metaMu.RLock()
	if idx.indexDB == nil {
		idx.metaMu.RUnlock()
		return ErrIndexClosed
	}
	meta, err := idx.getMetadata(batch)
	if err != nil {
		idx.metaMu.RUnlock()
		return fmt.Errorf("failed to get metadata: %w", err)
	}
	// Make a copy of metadata for modifications
	metaCopy := *meta
	meta = &metaCopy
	idx.metaMu.RUnlock()

	for _, id := range b.Deletes {
		// Remove from tree structure by findng the leaf containing this vector
		leafID, err := idx.findLeafForVectorID(batch, idx.writerCache, meta.RootNode, id)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return err
		}

		if err := idx.removeFromLeaf(batch, idx.writerCache, meta, leafID, id); err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return fmt.Errorf("removing from tree: %w", err)
		}

		// Note: Vectors are stored in VectorDB and managed externally - we only delete metadata

		// Delete metadata
		metaKey := makeHBCMetadataKey(idx.prefix, id)
		if err := batch.Delete(metaKey, nil); err != nil {
			return fmt.Errorf("failed to delete metadata: %w", err)
		}

		if meta.ActiveCount > 0 {
			meta.ActiveCount--
		}
	}

	// Insert each vector
	for i, vec := range vectors {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if uint32(len(vec)) != idx.config.Dimension { //nolint:gosec // G115: bounded value, cannot overflow in practice
			return fmt.Errorf(
				"vector dimension mismatch: expected %d, got %d",
				idx.config.Dimension,
				len(vec),
			)
		}
		// Check if vector already exists in the tree
		existingLeafID, err := idx.findLeafForVectorID(
			batch,
			idx.writerCache,
			meta.RootNode,
			ids[i],
		)
		if err != nil && !errors.Is(err, pebble.ErrNotFound) && !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("checking for existing vector id %d: %w", ids[i], err)
		}

		// Save metadata if provided
		if metadataList != nil && len(metadataList[i]) > 0 {
			metaKey := makeHBCMetadataKey(idx.prefix, ids[i])
			if err := batch.Set(metaKey, metadataList[i], nil); err != nil {
				return fmt.Errorf("saving metadata: %w", err)
			}
		}

		// Note: Vectors are stored in VectorDB and managed externally - we only manage metadata here

		// Insert into tree
		inserted, err := idx.insertIntoTree(batch, idx.writerCache, meta, ids[i], vec, existingLeafID)
		if err != nil {
			return fmt.Errorf("inserting %d tree: %w", ids[i], err)
		}
		if inserted {
			meta.ActiveCount++
		}
	}

	// Acquire locks in the same order as Search to prevent ABBA deadlock:
	// commitMu -> metaMu (Search acquires commitMu then metaMu.RLock)
	idx.commitMu.Lock()
	defer idx.commitMu.Unlock()

	// Save updated metadata with write lock
	idx.metaMu.Lock()
	if err := idx.saveMetadata(batch, meta); err != nil {
		idx.metaMu.Unlock()
		return fmt.Errorf("saving metadata: %w", err)
	}
	idx.metaMu.Unlock()

	if err := batch.Commit(idx.syncOpt); err != nil {
		return fmt.Errorf("committing pebble batch: %w", err)
	}
	if err := idx.writerCache.Commit(); err != nil {
		return fmt.Errorf("committing writer cache: %w", err)
	}

	// Cleanup expired cache entries after batch insert
	idx.nodeCache.DeleteExpired()
	return nil
}

type hbcBulkBuildInput struct {
	ID          uint64
	Metadata    []byte
	Transformed vector.T
}

// BulkBuild constructs an HBC tree recursively from an existing set of vectors.
// Raw vectors must already exist in VectorDB under metadata+suffix keys.
func (idx *HBCIndex) BulkBuild(ctx context.Context, b *Batch) (err error) {
	defer func() {
		if r := recover(); r != nil {
			idx.writerCache.Reset()
			switch e := r.(type) {
			case error:
				if errors.Is(e, pebble.ErrClosed) {
					err = e
					return
				}
			}
			panic(r)
		}
		if err != nil {
			idx.writerCache.Reset()
		}
	}()

	if b == nil || len(b.Vectors) == 0 {
		return nil
	}
	if len(b.Deletes) > 0 {
		return errors.New("bulk build does not support deletes")
	}
	if len(b.IDs) != len(b.Vectors) {
		return errors.New("ids and vectors must have same length")
	}
	if len(b.MetadataList) != len(b.Vectors) {
		return errors.New("metadata list must match vectors length")
	}

	inputs := make([]hbcBulkBuildInput, len(b.Vectors))
	seen := make(map[uint64]struct{}, len(b.IDs))
	for i, id := range b.IDs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if _, ok := seen[id]; ok {
			return ErrDuplicateVectorID
		}
		seen[id] = struct{}{}

		vec := b.Vectors[i]
		if uint32(len(vec)) != idx.config.Dimension { //nolint:gosec // G115: bounded value
			return fmt.Errorf("vector dimension mismatch: expected %d, got %d", idx.config.Dimension, len(vec))
		}
		if len(b.MetadataList[i]) == 0 {
			return fmt.Errorf("metadata required for bulk build id %d", id)
		}

		transformed := make(vector.T, idx.config.Dimension)
		idx.TransformVector(vec, transformed)
		inputs[i] = hbcBulkBuildInput{
			ID:          id,
			Metadata:    bytes.Clone(b.MetadataList[i]),
			Transformed: transformed,
		}
	}

	batch := idx.indexDB.NewIndexedBatchWithSize(32 << 20)
	defer func() { _ = batch.Close() }()

	idx.metaMu.RLock()
	if idx.indexDB == nil {
		idx.metaMu.RUnlock()
		return ErrIndexClosed
	}
	meta, err := idx.getMetadata(batch)
	if err != nil {
		idx.metaMu.RUnlock()
		return fmt.Errorf("failed to get metadata: %w", err)
	}
	metaCopy := *meta
	meta = &metaCopy
	idx.metaMu.RUnlock()

	if meta.ActiveCount != 0 {
		return ErrIndexNotEmpty
	}
	if meta.RootNode == 0 {
		meta.RootNode = 1
	}
	if err := idx.deleteNode(batch, idx.writerCache, meta.RootNode); err != nil {
		return fmt.Errorf("deleting empty root before bulk build: %w", err)
	}

	nextNodeID := meta.RootNode + 1
	nodeCount, err := idx.buildBulkSubtreeRecursive(
		ctx,
		batch,
		idx.writerCache,
		inputs,
		meta.RootNode,
		0,
		0,
		&nextNodeID,
	)
	if err != nil {
		return err
	}

	meta.ActiveCount = uint64(len(inputs))
	meta.NodeCount = nodeCount

	idx.commitMu.Lock()
	defer idx.commitMu.Unlock()

	idx.metaMu.Lock()
	if err := idx.saveMetadata(batch, meta); err != nil {
		idx.metaMu.Unlock()
		return fmt.Errorf("saving metadata: %w", err)
	}
	idx.metaMu.Unlock()

	if err := batch.Commit(idx.syncOpt); err != nil {
		return fmt.Errorf("committing pebble batch: %w", err)
	}
	if err := idx.writerCache.Commit(); err != nil {
		return fmt.Errorf("committing writer cache: %w", err)
	}

	idx.nodeCache.DeleteExpired()
	return nil
}

func (idx *HBCIndex) buildBulkSubtreeRecursive(
	ctx context.Context,
	batch *pebble.Batch,
	cacheBatch nodeCache,
	inputs []hbcBulkBuildInput,
	nodeID uint64,
	parentID uint64,
	level int,
	nextNodeID *uint64,
) (uint64, error) {
	if len(inputs) == 0 {
		return 0, errors.New("bulk build requires at least one input")
	}
	if len(inputs) <= idx.config.LeafSize {
		if err := idx.buildBulkLeaf(batch, cacheBatch, inputs, nodeID, parentID, level); err != nil {
			return 0, err
		}
		return nodeID, nil
	}

	leftInputs, rightInputs, leftCentroid, rightCentroid, err := idx.partitionBulkBuildInputs(inputs)
	if err != nil {
		return 0, err
	}

	leftID := *nextNodeID
	*nextNodeID++
	rightID := *nextNodeID
	*nextNodeID++

	leftMaxID, err := idx.buildBulkSubtreeRecursive(ctx, batch, cacheBatch, leftInputs, leftID, nodeID, level+1, nextNodeID)
	if err != nil {
		return 0, err
	}
	rightMaxID, err := idx.buildBulkSubtreeRecursive(ctx, batch, cacheBatch, rightInputs, rightID, nodeID, level+1, nextNodeID)
	if err != nil {
		return 0, err
	}

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	centroid := make(vector.T, idx.config.Dimension)
	copy(centroid, leftCentroid)
	vec.AddFloat32(centroid, rightCentroid)
	vec.ScaleFloat32(0.5, centroid)

	node := &HBCNode{
		ID:       nodeID,
		IsLeaf:   false,
		Centroid: centroid,
		Children: []uint64{leftID, rightID},
		Parent:   parentID,
		Level:    level,
	}
	if err := idx.saveNode(batch, cacheBatch, node); err != nil {
		return 0, fmt.Errorf("saving bulk internal node %d: %w", nodeID, err)
	}

	return max(nodeID, max(leftMaxID, rightMaxID)), nil
}

func (idx *HBCIndex) buildBulkLeaf(
	batch *pebble.Batch,
	cacheBatch nodeCache,
	inputs []hbcBulkBuildInput,
	nodeID uint64,
	parentID uint64,
	level int,
) error {
	members := make([]uint64, len(inputs))
	centroid := make(vector.T, idx.config.Dimension)
	for i, input := range inputs {
		metaKey := makeHBCMetadataKey(idx.prefix, input.ID)
		if err := batch.Set(metaKey, input.Metadata, nil); err != nil {
			return fmt.Errorf("saving metadata for vector %d: %w", input.ID, err)
		}
		members[i] = input.ID
		vec.AddFloat32(centroid, input.Transformed)
	}
	vec.ScaleFloat32(1/float32(len(inputs)), centroid)

	node := &HBCNode{
		ID:       nodeID,
		IsLeaf:   true,
		Centroid: centroid,
		Members:  members,
		Parent:   parentID,
		Level:    level,
	}
	if err := idx.saveNode(batch, cacheBatch, node); err != nil {
		return fmt.Errorf("saving bulk leaf node %d: %w", nodeID, err)
	}
	return nil
}

func (idx *HBCIndex) partitionBulkBuildInputs(
	inputs []hbcBulkBuildInput,
) ([]hbcBulkBuildInput, []hbcBulkBuildInput, vector.T, vector.T, error) {
	vectorSet := vector.MakeSet(int(idx.config.Dimension))
	vectorSet.EnsureCapacity(len(inputs))
	ids := make([]uint64, len(inputs))
	for i, input := range inputs {
		vectorSet.Add(input.Transformed)
		ids[i] = uint64(i)
	}

	leftCentroid, leftIDs, rightCentroid, rightIDs, err := idx.splitVectorSet(vectorSet, ids)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("partitioning bulk build inputs: %w", err)
	}
	if len(leftIDs) == 0 || len(rightIDs) == 0 {
		mid := len(inputs) / 2
		if mid == 0 || mid == len(inputs) {
			return nil, nil, nil, nil, errors.New("bulk build produced degenerate partition")
		}
		left := slices.Clone(inputs[:mid])
		right := slices.Clone(inputs[mid:])
		return left, right, centroidForBulkBuildInputs(left, idx.config.Dimension), centroidForBulkBuildInputs(right, idx.config.Dimension), nil
	}

	left := make([]hbcBulkBuildInput, 0, len(leftIDs))
	for _, inputID := range leftIDs {
		left = append(left, inputs[inputID])
	}
	right := make([]hbcBulkBuildInput, 0, len(rightIDs))
	for _, inputID := range rightIDs {
		right = append(right, inputs[inputID])
	}
	return left, right, leftCentroid, rightCentroid, nil
}

func centroidForBulkBuildInputs(inputs []hbcBulkBuildInput, dimension uint32) vector.T {
	centroid := make(vector.T, dimension)
	for _, input := range inputs {
		vec.AddFloat32(centroid, input.Transformed)
	}
	vec.ScaleFloat32(1/float32(len(inputs)), centroid)
	return centroid
}

// insertIntoTree inserts a vector into the hierarchical tree
func (idx *HBCIndex) insertIntoTree(
	batch *pebble.Batch,
	cacheBatch nodeCache,
	meta *hbcIndexMetadata,
	vectorID uint64,
	inputVector vector.T,
	existingLeafID uint64, // Optional existing leaf ID if known
) (inserted bool, err error) {
	q := &queryVector{}
	q.InitOriginal(idx.config.DistanceMetric, inputVector, idx.rot)
	// Start from root and find the best leaf
	currentID := meta.RootNode

	results := NewPriorityQueue(true, idx.config.BranchingFactor) // Max-heap for results
	for {
		nodeUnsafe, err := idx.loadNode(batch, cacheBatch, currentID)
		if err != nil {
			return false, fmt.Errorf("loading node %d: %w", currentID, err)
		}
		if nodeUnsafe.IsLeaf {
			if existingLeafID != 0 {
				if existingLeafID != nodeUnsafe.ID {
					if err := idx.removeFromLeaf(batch, cacheBatch, meta, existingLeafID, vectorID); err != nil {
						return false, fmt.Errorf(
							"removing existing vector %d from leaf %d: %w",
							vectorID,
							existingLeafID,
							err,
						)
					}
				} else {
					// Already in the correct leaf, no need to re-insert
					return false, nil
				}
			}
			node := idx.nodePool.Get().(*HBCNode)
			node.CopyFrom(nodeUnsafe)
			if nodeUnsafe.QuantizedVectors != nil {
				node.QuantizedVectors = nodeUnsafe.QuantizedVectors.Clone()
			}
			defer func() {
				// Reset and return node to pool
				node.reset()
				idx.nodePool.Put(node)
			}()
			// Add to this leaf
			node.Members = append(node.Members, vectorID)
			if len(node.Centroid) == 0 {
				node.Centroid = slices.Clone(q.Transformed())
			} else {
				nf := float32(len(node.Members))
				for i := range node.Centroid {
					node.Centroid[i] = node.Centroid[i]*(nf-1)/nf + q.Transformed()[i]/nf
				}
				idx.maybeNormalizeForMetric(node.Centroid)
			}
			if err := idx.saveNode(batch, cacheBatch, node, q.Transformed()); err != nil {
				return false, fmt.Errorf("saving node: %w", err)
			}
			// Check if we need to split
			if len(node.Members) > idx.config.LeafSize {
				if err := idx.splitLeaf(batch, cacheBatch, meta, node); err != nil {
					return false, fmt.Errorf("splitting leaf %d: %w", node.ID, err)
				}
			}
			return true, nil
		} else {
			// Find closest child
			minDist := float32(math.MaxFloat32)
			var closestChild uint64

			if idx.config.UseQuantization {
				// Use quantized distance if available
				if nodeUnsafe.QuantizedVectors == nil {
					return false, fmt.Errorf("quantized vectors not found for node %d", nodeUnsafe.ID)
				}
				tempDists := idx.writerAllocator.AllocFloat32s(nodeUnsafe.QuantizedVectors.GetCount())
				tempErrorBounds := idx.writerAllocator.AllocFloat32s(nodeUnsafe.QuantizedVectors.GetCount())
				var quantizer quantize.Quantizer
				if nodeUnsafe.Parent == 0 {
					if _, ok := nodeUnsafe.QuantizedVectors.(*quantize.RaBitQuantizedVectorSet); ok {
						return false, fmt.Errorf("expected non-quantized vector set for root node %d", nodeUnsafe.ID)
					}
					quantizer = idx.rootQuantizer
				} else {
					if _, ok := nodeUnsafe.QuantizedVectors.(*quantize.NonQuantizedVectorSet); ok {
						return false, fmt.Errorf("expected RaBitQuantizedVectorSet for non-root node %d", nodeUnsafe.ID)
					}
					quantizer = idx.quantizer
				}
				quantizer.EstimateDistances(idx.writerAllocator, nodeUnsafe.QuantizedVectors, q.Transformed(), tempDists, tempErrorBounds)
			Outer:
				for i := range nodeUnsafe.QuantizedVectors.GetCount() {
					childID := nodeUnsafe.Children[i]

					pi := &PriorityItem{
						Distance:    tempDists[i],
						ErrorBounds: tempErrorBounds[i],
						ID:          childID,
					}
					if results.Len() == 0 {
						heap.Push(results, pi)
					}
					worstResult := results.Peek()
					for pi.DefinitelyCloser(worstResult) {
						heap.Pop(results)
						if results.Len() == 0 {
							heap.Push(results, pi)
							continue Outer
						}
						worstResult = results.Peek()
					}
					if pi.MaybeCloser(worstResult) {
						heap.Push(results, pi)
						continue
					}
				}
				if results.Len() == 1 {
					// Only one child, no need to check further
					item := heap.Pop(results).(*PriorityItem)
					closestChild = item.ID
				} else {
					for results.Len() > 0 {
						item := heap.Pop(results).(*PriorityItem)
						childID := item.ID

						// Potentially closer, compute exact distance
						vecKey := makeHBCCentroidKey(idx.prefix, childID)
						vecData, closer, err := batch.Get(vecKey)
						if err != nil {
							// FIXME (ajr) This can happen if the index is corrupted, we should probably handle this better
							continue
						}
						var childCentroid vector.T
						_, childCentroid, err = vector.Decode(vecData)
						if err != nil {
							// FIXME (ajr) This can happen if the index is corrupted, we should probably handle this better
							continue
						}
						_ = closer.Close()
						dist := vector.MeasureDistance(idx.config.DistanceMetric, q.Transformed(), childCentroid)
						if dist < minDist {
							minDist = dist
							closestChild = childID
						}
					}
				}
				idx.writerAllocator.FreeFloat32s(tempErrorBounds)
				idx.writerAllocator.FreeFloat32s(tempDists)

			} else {
				for _, childID := range nodeUnsafe.Children {
					vecKey := makeHBCCentroidKey(idx.prefix, childID)
					vecData, closer, err := batch.Get(vecKey)
					if err != nil {
						return false, fmt.Errorf("loading vector %d: %w", childID, err)
					}
					var childCentroid vector.T
					_, childCentroid, err = vector.Decode(vecData)
					if err != nil {
						return false, fmt.Errorf("decoding centroid vector %d: %w", childID, err)
					}
					_ = closer.Close()
					dist := vector.MeasureDistance(idx.config.DistanceMetric, q.Transformed(), childCentroid)

					if dist < minDist {
						minDist = dist
						closestChild = childID
					}
				}
			}

			if closestChild == 0 {
				return false, errors.New("no valid child found")
			}

			currentID = closestChild
		}
	}
}

// splitLeaf splits a leaf node that has exceeded capacity
func (idx *HBCIndex) splitLeaf(
	batch *pebble.Batch,
	cacheBatch nodeCache,
	meta *hbcIndexMetadata,
	leaf *HBCNode,
) error {
	// Use k-means with k=2 to split the leaf
	vectorSet := idx.writerAllocator.AllocVectorSet(len(leaf.Members), int(idx.config.Dimension))
	defer idx.writerAllocator.FreeVectorSet(vectorSet)
	// TODO (ajr) Add the transformations here which requires adding the
	// query comparer and search set errors bound fixing as well
	//
	// This adds rotation to the centroids.
	// We'd also need to add the query comparer to inserts and deletes too.
	for i, childID := range leaf.Members {
		if _, err := idx.GetVector(batch, childID, vectorSet.At(i)); err != nil {
			return fmt.Errorf("loading vector %d: %w", childID, err)
		}
		idx.TransformVector(vectorSet.At(i), vectorSet.At(i))
	}

	// Simple k-means with k=2
	c1, group1, c2, group2, err := idx.splitVectorSet(vectorSet, leaf.Members)
	if err != nil {
		return fmt.Errorf("executing split algo: %w", err)
	}

	// Create two new leaf nodes
	meta.NodeCount++
	node1 := &HBCNode{
		ID:       meta.NodeCount,
		IsLeaf:   true,
		Members:  group1,
		Parent:   leaf.Parent,
		Level:    leaf.Level,
		Centroid: c1,
	}

	meta.NodeCount++
	node2 := &HBCNode{
		ID:       meta.NodeCount,
		IsLeaf:   true,
		Members:  group2,
		Parent:   leaf.Parent,
		Level:    leaf.Level,
		Centroid: c2,
	}

	// Update parent or create new root
	if leaf.Parent == 0 && leaf.ID == meta.RootNode {
		// Splitting the root node - create new root
		meta.NodeCount++
		newRoot := &HBCNode{
			ID:       meta.NodeCount,
			IsLeaf:   false,
			Children: []uint64{node1.ID, node2.ID},
			Level:    leaf.Level,
			Centroid: slices.Clone(node1.Centroid),
		}
		vec.AddFloat32(newRoot.Centroid, node2.Centroid)
		vec.ScaleFloat32(0.5, newRoot.Centroid)
		idx.maybeNormalizeForMetric(newRoot.Centroid)

		node1.Parent = newRoot.ID
		node1.Level = leaf.Level + 1
		node2.Parent = newRoot.ID
		node2.Level = leaf.Level + 1

		if err := idx.saveNode(batch, cacheBatch, node1); err != nil {
			return fmt.Errorf("saving left node %d: %w", node1.ID, err)
		}
		if err := idx.saveNode(batch, cacheBatch, node2); err != nil {
			return fmt.Errorf("saving right node %d: %w", node2.ID, err)
		}
		if err := idx.saveNode(batch, cacheBatch, newRoot); err != nil {
			return fmt.Errorf("saving new root node %d: %w", newRoot.ID, err)
		}
		meta.RootNode = newRoot.ID
		if err := idx.saveMetadata(batch, meta); err != nil {
			return fmt.Errorf("saving metadata after split: %w", err)
		}
	} else if leaf.Parent != 0 {
		// Save new nodes
		if err := idx.saveNode(batch, cacheBatch, node1); err != nil {
			return err
		}
		if err := idx.saveNode(batch, cacheBatch, node2); err != nil {
			return err
		}

		// Update parent's children
		parentUnsafe, err := idx.loadNode(batch, cacheBatch, leaf.Parent)
		if err != nil {
			return fmt.Errorf("loading parent node %d: %w", leaf.Parent, err)
		}
		parent := idx.nodePool.Get().(*HBCNode)
		parent.CopyFrom(parentUnsafe)
		defer func() {
			// Reset and return node to pool
			parent.reset()
			idx.nodePool.Put(parent)
		}()

		// Replace leaf with new nodes
		i := slices.IndexFunc(parent.Children, func(id uint64) bool {
			return id == leaf.ID
		})
		parent.Children[i] = node1.ID
		parent.Children = append(parent.Children, node2.ID)
		if err := idx.saveNode(batch, cacheBatch, parent); err != nil {
			return fmt.Errorf("updating node: %w", err)
		}

		// Check if parent needs splitting
		if len(parent.Children) > idx.config.BranchingFactor {
			if err := idx.splitInternal(batch, cacheBatch, meta, parent); err != nil {
				return err
			}
		}
	}
	if err := idx.deleteNode(batch, cacheBatch, leaf.ID); err != nil {
		return err
	}

	return nil
}

// splitInternal splits an internal node
func (idx *HBCIndex) splitInternal(
	batch *pebble.Batch,
	cacheBatch nodeCache,
	meta *hbcIndexMetadata,
	node *HBCNode,
) error {
	// Similar to splitLeaf but for internal nodes
	// Group children by centroid similarity
	childCentroids := idx.writerAllocator.AllocVectorSet(
		len(node.Children),
		int(idx.config.Dimension),
	)
	defer idx.writerAllocator.FreeVectorSet(childCentroids)
	for i, childID := range node.Children {
		vecKey := makeHBCCentroidKey(idx.prefix, childID)
		vecData, closer, err := batch.Get(vecKey)
		if err != nil {
			return fmt.Errorf("loading vector %d: %w", childID, err)
		}
		_, err = vector.DecodeTo(vecData, childCentroids.At(i))
		if err != nil {
			return fmt.Errorf("decoding centroid vector %d: %w", childID, err)
		}
		_ = closer.Close()
	}

	c1, group1, c2, group2, err := idx.splitVectorSet(childCentroids, node.Children)
	if err != nil {
		return fmt.Errorf("executing kMeans split: %w", err)
	}

	// Create two new internal nodes
	meta.NodeCount++
	node1 := &HBCNode{
		ID:       meta.NodeCount,
		IsLeaf:   false,
		Children: group1,
		Parent:   node.Parent,
		Level:    node.Level,
		Centroid: c1,
	}

	meta.NodeCount++
	node2 := &HBCNode{
		ID:       meta.NodeCount,
		IsLeaf:   false,
		Children: group2,
		Parent:   node.Parent,
		Level:    node.Level,
		Centroid: c2,
	}

	// Update children's parents
	for _, childID := range group1 {
		childUnsafe, err := idx.loadNode(batch, cacheBatch, childID)
		if err != nil {
			return fmt.Errorf("loading child %d during split: %w", childID, err)
		}
		child := idx.nodePool.Get().(*HBCNode)
		child.CopyFrom(childUnsafe)
		if childUnsafe.QuantizedVectors != nil {
			child.QuantizedVectors = childUnsafe.QuantizedVectors.Clone()
		}
		child.Parent = node1.ID
		if err := idx.saveNodeParentOnly(batch, cacheBatch, child); err != nil {
			child.reset()
			idx.nodePool.Put(child)
			return fmt.Errorf("saving parent for child %d: %w", childID, err)
		}
		// Reset and return node to pool
		child.reset()
		idx.nodePool.Put(child)
	}

	for _, childID := range group2 {
		childUnsafe, err := idx.loadNode(batch, cacheBatch, childID)
		if err != nil {
			return fmt.Errorf("loading child %d during split: %w", childID, err)
		}
		child := idx.nodePool.Get().(*HBCNode)
		child.CopyFrom(childUnsafe)
		if childUnsafe.QuantizedVectors != nil {
			child.QuantizedVectors = childUnsafe.QuantizedVectors.Clone()
		}
		child.Parent = node2.ID
		if err := idx.saveNodeParentOnly(batch, cacheBatch, child); err != nil {
			child.reset()
			idx.nodePool.Put(child)
			return fmt.Errorf("saving parent for child %d: %w", childID, err)
		}
		// Reset and return node to pool
		child.reset()
		idx.nodePool.Put(child)
	}

	// Update parent or create new root
	if node.Parent == 0 {
		// Create new root
		meta.NodeCount++
		newRoot := &HBCNode{
			ID:       meta.NodeCount,
			IsLeaf:   false,
			Children: []uint64{node1.ID, node2.ID},
			Level:    node.Level + 1,
			Centroid: slices.Clone(node1.Centroid),
		}
		vec.AddFloat32(newRoot.Centroid, node2.Centroid)
		vec.ScaleFloat32(0.5, newRoot.Centroid)
		idx.maybeNormalizeForMetric(newRoot.Centroid)

		node1.Parent = newRoot.ID
		node2.Parent = newRoot.ID

		if err := idx.saveNode(batch, cacheBatch, node1); err != nil {
			return err
		}
		if err := idx.saveNode(batch, cacheBatch, node2); err != nil {
			return err
		}
		if err := idx.saveNode(batch, cacheBatch, newRoot); err != nil {
			return err
		}

		meta.RootNode = newRoot.ID
		if err := idx.saveMetadata(batch, meta); err != nil {
			return fmt.Errorf("saving metadata after split: %w", err)
		}
	} else {
		// Save new nodes
		if err := idx.saveNode(batch, cacheBatch, node1); err != nil {
			return err
		}
		if err := idx.saveNode(batch, cacheBatch, node2); err != nil {
			return err
		}

		// Update parent's children
		parentUnsafe, err := idx.loadNode(batch, cacheBatch, node.Parent)
		if err != nil {
			return err
		}
		parent := idx.nodePool.Get().(*HBCNode)
		parent.CopyFrom(parentUnsafe)
		defer func() {
			// Reset and return node to pool
			parent.reset()
			idx.nodePool.Put(parent)
		}()

		// Replace node with new nodes
		i := slices.IndexFunc(parent.Children, func(id uint64) bool {
			return id == node.ID
		})
		parent.Children[i] = node1.ID
		parent.Children = append(parent.Children, node2.ID)
		if err := idx.saveNode(batch, cacheBatch, parent); err != nil {
			return err
		}
		// Recursively check if parent needs splitting
		if len(parent.Children) > idx.config.BranchingFactor {
			if err := idx.splitInternal(batch, cacheBatch, meta, parent); err != nil {
				return err
			}
		}
	}

	if err := idx.deleteNode(batch, cacheBatch, node.ID); err != nil {
		return err
	}
	return nil
}

// TransformVector performs a random orthogonal transformation (ROT) on the
// "original" vector and writes it to the "randomized" vector. If the index uses
// the Cosine distance metric, it also ensures that the original vector has been
// normalized into a unit vector (norm = 1). The caller is responsible for
// allocating the randomized vector with length equal to the index's dimensions.
//
// Randomizing vectors distributes skew more evenly across dimensions and
// across vectors in a set. Distance and angle between any two vectors
// remains unchanged, as long as the same ROT is applied to both.
//
// Query and data vectors are assumed to be normalized when calculating Cosine
// distances.
func (idx *HBCIndex) maybeNormalizeForMetric(v vector.T) {
	if idx.config.DistanceMetric == vector.DistanceMetric_Cosine {
		vec.NormalizeFloat32(v)
	}
}

func (idx *HBCIndex) TransformVector(original vector.T, randomized vector.T) vector.T {
	idx.rot.Transform(original, randomized)
	if idx.quantizer.GetDistanceMetric() == vector.DistanceMetric_Cosine {
		vec.NormalizeFloat32(randomized)
	}
	return randomized
}

// UnTransformVector inverts the random orthogonal transformation performed by
// TransformVector, in order to recover the normalized vector in the case of
// Cosine distance, or the original vector in the case of other distance
// functions. The caller is responsible for allocating the original vector with
// length equal to the index's dimensions.
func (idx *HBCIndex) UnTransformVector(randomized vector.T, normalized vector.T) vector.T {
	return idx.rot.UnTransformVector(randomized, normalized)
}

// splitVectorSet performs clustering with k=2 using the configured clusterer
func (idx *HBCIndex) splitVectorSet(
	vectors *vector.Set,
	ids []uint64,
) ([]float32, []uint64, []float32, []uint64, error) {
	leftCentroid := make(vector.T, idx.config.Dimension)
	rightCentroid := make(vector.T, idx.config.Dimension)
	tempAssignments := idx.writerAllocator.AllocUint64s(int(vectors.GetCount()))
	defer idx.writerAllocator.FreeUint64s(tempAssignments)

	// Use the clusterer to compute centroids and assign partitions
	idx.clusterer.ComputeCentroids(
		vectors,
		leftCentroid,
		rightCentroid,
		false, /* pinLeftCentroid */
	)
	idx.clusterer.AssignPartitions(vectors, leftCentroid, rightCentroid, tempAssignments)

	// Build the two groups based on assignments
	group1 := make([]uint64, 0, int(vectors.GetCount())/2)
	group2 := make([]uint64, 0, int(vectors.GetCount())/2)
	for i := range tempAssignments {
		if tempAssignments[i] == 0 {
			group1 = append(group1, ids[i])
		} else {
			group2 = append(group2, ids[i])
		}
	}
	if idx.quantizer.GetDistanceMetric() != vector.DistanceMetric_L2Squared {
		vec.NormalizeFloat32(leftCentroid)
		vec.NormalizeFloat32(rightCentroid)
	}

	return leftCentroid, group1, rightCentroid, group2, nil
}

// insertResultWithCollapse inserts an item into the results heap, handling collapse key deduplication
func insertResultWithCollapse(
	results *PriorityQueue,
	item *PriorityItem,
	metadata []byte,
	collapseKeyToItem map[string]*PriorityItem,
	collapseKeysFunc func([]byte) []byte,
) bool {
	// Check collapse key if configured
	if collapseKeyToItem != nil && len(metadata) > 0 {
		collapseKey := string(collapseKeysFunc(metadata))
		if existing, found := collapseKeyToItem[collapseKey]; found {
			// Already have this collapse key - check if new result is better
			// TODO: Add error bounds checks to distance comparison (use DefinitelyCloser/MaybeCloser)
			if item.Distance < existing.Distance {
				// New result is better - mark old one as removed
				existing.Removed = true
				collapseKeyToItem[collapseKey] = item
				heap.Push(results, item)
				return true
			}
			// Existing result is better - skip this one
			return false
		}
		// First time seeing this collapse key
		collapseKeyToItem[collapseKey] = item
	}
	heap.Push(results, item)
	return true
}

func (idx *HBCIndex) resolveRerankPolicy(req *SearchRequest) RerankPolicy {
	if !idx.config.UseQuantization || idx.config.DisableReranking {
		return RerankPolicyNever
	}
	if req != nil && req.RerankPolicy != nil {
		switch *req.RerankPolicy {
		case RerankPolicyNever, RerankPolicyBoundary, RerankPolicyAlways:
			return *req.RerankPolicy
		}
	}
	return RerankPolicyAlways
}

func resultMaybeCloser(r1 *Result, r2 *Result) bool {
	return r1.Distance-r1.ErrorBound <= r2.Distance+r2.ErrorBound
}

func resultMaybeOver(r *Result, dist float32) bool {
	return r.Distance+r.ErrorBound >= dist
}

func resultDefinitelyOver(r *Result, dist float32) bool {
	return r.Distance-r.ErrorBound > dist
}

func resultMaybeUnder(r *Result, dist float32) bool {
	return r.Distance-r.ErrorBound <= dist
}

func resultDefinitelyUnder(r *Result, dist float32) bool {
	return r.Distance+r.ErrorBound < dist
}

func shouldAutoRerank(
	results []*Result,
	k int,
	distanceOver *float32,
	distanceUnder *float32,
) bool {
	return len(selectAutoRerankCandidates(results, k, distanceOver, distanceUnder)) > 0
}

func selectAutoRerankCandidates(
	results []*Result,
	k int,
	distanceOver *float32,
	distanceUnder *float32,
) []int {
	limit := min(k, len(results))
	if limit == 0 {
		return nil
	}

	// Threshold ambiguity can change membership after filtering, so rerank the
	// retained set rather than trying to maintain a smaller safe window here.
	for i := range limit {
		res := results[i]
		if distanceOver != nil && resultMaybeOver(res, *distanceOver) && !resultDefinitelyOver(res, *distanceOver) {
			return fullResultIndexList(len(results))
		}
		if distanceUnder != nil && resultMaybeUnder(res, *distanceUnder) && !resultDefinitelyUnder(res, *distanceUnder) {
			return fullResultIndexList(len(results))
		}
	}

	rerank := make(map[int]struct{}, limit)
	for i := range limit {
		for j := limit; j < len(results); j++ {
			if resultMaybeCloser(results[j], results[i]) {
				rerank[i] = struct{}{}
				rerank[j] = struct{}{}
			}
		}
	}

	if len(rerank) == 0 {
		return nil
	}
	indices := make([]int, 0, len(rerank))
	for idx := range rerank {
		indices = append(indices, idx)
	}
	slices.Sort(indices)
	return indices
}

func fullResultIndexList(count int) []int {
	indices := make([]int, count)
	for i := range count {
		indices[i] = i
	}
	return indices
}

func debugHitFromResult(r *Result) SearchDebugHit {
	return SearchDebugHit{
		ID:         r.ID,
		Distance:   r.Distance,
		ErrorBound: r.ErrorBound,
		LowerBound: r.Distance - r.ErrorBound,
		UpperBound: r.Distance + r.ErrorBound,
	}
}

func debugPairFromResults(left *Result, right *Result) SearchDebugPair {
	leftHit := debugHitFromResult(left)
	rightHit := debugHitFromResult(right)
	intervalGap := rightHit.LowerBound - leftHit.UpperBound
	return SearchDebugPair{
		Left:        leftHit,
		Right:       rightHit,
		DistanceGap: right.Distance - left.Distance,
		IntervalGap: intervalGap,
		Overlaps:    intervalGap <= 0,
	}
}

func populateApproxSearchDebug(
	debug *SearchDebugInfo,
	results []*Result,
	k int,
	distanceOver *float32,
	distanceUnder *float32,
	rerankPolicy RerankPolicy,
) {
	if debug == nil {
		return
	}

	*debug = SearchDebugInfo{
		ResolvedRerankPolicy: rerankPolicy,
		ApproxCandidateCount: len(results),
		TopKCount:            min(k, len(results)),
		MinDistanceGapTopK:   math32.MaxFloat32,
		MinIntervalGapTopK:   math32.MaxFloat32,
	}

	limit := min(debug.TopKCount, 5)
	debug.ApproxTop = make([]SearchDebugHit, 0, limit)
	for i := range limit {
		debug.ApproxTop = append(debug.ApproxTop, debugHitFromResult(results[i]))
	}

	topKCount := debug.TopKCount
	for i := range topKCount {
		res := results[i]
		if distanceOver != nil && resultMaybeOver(res, *distanceOver) && !resultDefinitelyOver(res, *distanceOver) {
			debug.AmbiguousDistanceOverHits++
		}
		if distanceUnder != nil && resultMaybeUnder(res, *distanceUnder) && !resultDefinitelyUnder(res, *distanceUnder) {
			debug.AmbiguousDistanceUnderHits++
		}
	}

	for i := range topKCount {
		for j := i + 1; j < topKCount; j++ {
			pair := debugPairFromResults(results[i], results[j])
			if pair.DistanceGap < debug.MinDistanceGapTopK {
				debug.MinDistanceGapTopK = pair.DistanceGap
			}
			if pair.IntervalGap < debug.MinIntervalGapTopK {
				debug.MinIntervalGapTopK = pair.IntervalGap
				pairCopy := pair
				debug.ClosestPairTopK = &pairCopy
			}
			if pair.Overlaps {
				debug.AmbiguousTopKPairs++
			}
		}
	}

	if debug.MinDistanceGapTopK == math32.MaxFloat32 {
		debug.MinDistanceGapTopK = 0
	}
	if debug.MinIntervalGapTopK == math32.MaxFloat32 {
		debug.MinIntervalGapTopK = 0
	}

	if topKCount > 0 && topKCount <= len(results) {
		for i := topKCount; i < len(results); i++ {
			pair := debugPairFromResults(results[topKCount-1], results[i])
			if pair.Overlaps {
				debug.AmbiguousBoundaryPairs++
				if debug.BoundaryPair == nil {
					pairCopy := pair
					debug.BoundaryPair = &pairCopy
				}
			}
		}
		if debug.BoundaryPair == nil && len(results) > topKCount {
			pair := debugPairFromResults(results[topKCount-1], results[topKCount])
			pairCopy := pair
			debug.BoundaryPair = &pairCopy
		}
	}
}

func populateExactSearchDebug(debug *SearchDebugInfo, results []*Result, exactRerank bool) {
	if debug == nil {
		return
	}
	debug.ExactRerank = exactRerank
	if !exactRerank {
		debug.ExactTop = nil
		return
	}
	limit := min(len(results), 5)
	debug.ExactTop = make([]SearchDebugHit, 0, limit)
	for i := range limit {
		debug.ExactTop = append(debug.ExactTop, debugHitFromResult(results[i]))
	}
}

// Search finds k nearest neighbors
func (idx *HBCIndex) Search(req *SearchRequest) (r []*Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch e := r.(type) {
			case error:
				// FIXME (probably need to work out better shutdown semantics, this has a code smell)
				if errors.Is(e, pebble.ErrClosed) {
					err = e
					return
				}
			}
			panic(r)
		}
	}()
	var stackAllocator allocator.StackAllocator

	q := &queryVector{}
	q.InitOriginal(idx.config.DistanceMetric, req.Embedding, idx.rot)

	// For Cosine distance, create a normalized (but not ROT-transformed) copy
	// of the query embedding for comparing against member vectors in leaf nodes.
	// Member vectors are stored as-is (not ROT-transformed), so we can't use
	// q.Transformed() for them. Centroids ARE ROT-transformed, so we use
	// q.Transformed() when comparing against centroids.
	normalizedEmbedding := req.Embedding
	if idx.config.DistanceMetric == vector.DistanceMetric_Cosine {
		normalizedEmbedding = slices.Clone(req.Embedding)
		vec.NormalizeFloat32(normalizedEmbedding)
	}

	var excludeMembers map[uint64]struct{}
	if len(req.ExcludeIDs) > 0 {
		excludeMembers = make(map[uint64]struct{}, len(req.ExcludeIDs))
		for _, id := range req.ExcludeIDs {
			excludeMembers[id] = struct{}{}
		}
	}
	var includeMembers map[uint64]struct{}
	if len(req.FilterIDs) > 0 {
		includeMembers = make(map[uint64]struct{}, len(req.FilterIDs))
		for _, id := range req.FilterIDs {
			includeMembers[id] = struct{}{}
		}
	}
	shouldSkip := func(memberID uint64) bool {
		if len(excludeMembers) > 0 {
			if _, ok := excludeMembers[memberID]; ok {
				return true
			}
		}
		if len(req.FilterPrefix) > 0 {
			metaKey := makeHBCMetadataKey(idx.prefix, memberID)
			if metaData, closer, err := idx.indexDB.Get(metaKey); err == nil {
				if !bytes.HasPrefix(metaData, req.FilterPrefix) {
					_ = closer.Close()
					return true
				}
				_ = closer.Close()
			} else {
				return true
			}
		}
		if len(includeMembers) > 0 {
			if _, ok := includeMembers[memberID]; ok {
				return false
			}
			return true
		}
		return false
	}
	if uint32(len(req.Embedding)) != idx.config.Dimension { //nolint:gosec // G115: bounded value, cannot overflow in practice
		return nil, fmt.Errorf(
			"query dimension mismatch: expected %d, got %d",
			idx.config.Dimension,
			len(req.Embedding),
		)
	}

	// Only need read lock for metadata
	idx.commitMu.Lock()
	defer idx.commitMu.Unlock()
	idx.metaMu.RLock()
	meta, err := idx.getMetadata(idx.indexDB)
	if err != nil {
		idx.metaMu.RUnlock()
		return nil, fmt.Errorf("getting metadata: %w", err)
	}
	rootNodeID := meta.RootNode
	activeCount := meta.ActiveCount
	idx.metaMu.RUnlock()

	if activeCount == 0 {
		return nil, nil
	}

	// Resolve per-query overrides for search width and dynamic pruning
	searchWidth := idx.config.SearchWidth
	if req.SearchWidth != nil {
		searchWidth = *req.SearchWidth
	}
	epsilon2 := idx.config.Episilon2
	if req.Epsilon2 != nil {
		epsilon2 = *req.Epsilon2
	}

	// Use a priority queue to track best clusters to explore
	candidates := NewPriorityQueue(false, searchWidth*2) // Min-heap with larger capacity
	results := NewPriorityQueue(true, req.K)             // Max-heap for results

	// Track collapse keys during traversal to handle duplicates
	var collapseKeyToItem map[string]*PriorityItem
	if idx.config.CollapseKeysFunc != nil {
		collapseKeyToItem = make(map[string]*PriorityItem, req.K)
	}

	// Start with root
	root, err := idx.loadNode(idx.indexDB, idx.readerCache, rootNodeID)
	if err != nil {
		return nil, fmt.Errorf("loading root: %w", err)
	}
	// Use q.Transformed() for centroids (they are ROT-transformed and normalized)
	rootDist := vector.MeasureDistance(idx.config.DistanceMetric, q.Transformed(), root.Centroid)
	heap.Push(candidates, &PriorityItem{ID: root.ID, Distance: rootDist})

	visited := make(map[uint64]bool, searchWidth*2)
	nodesExplored := 0
	leavesExplored := 0

	var dynamicPruningMin float32 = math32.MaxFloat32
	// Traverse tree
	for candidates.Len() > 0 /* && nodesExplored < 16*idx.config.SearchWidth */ && leavesExplored < searchWidth {
		item := heap.Pop(candidates).(*PriorityItem)

		if visited[item.ID] {
			continue
		}
		visited[item.ID] = true
		nodesExplored++

		node, err := idx.loadNode(idx.indexDB, idx.readerCache, item.ID)
		if err != nil {
			continue
		}

		// Use read lock for node access
		if len(node.Centroid) == 0 {
			// TODO (ajr) [concurrent rw]
			continue
		}

		if !node.IsLeaf && results.Len() >= req.K && dynamicPruningMin < math32.MaxFloat32 {
			if item.Distance > dynamicPruningMin*(epsilon2+1) {
				// Dynamic pruning: skip this node if it's too far
				continue
			}
		}

		if node.IsLeaf {
			if results.Len() >= req.K && item.Distance < dynamicPruningMin {
				dynamicPruningMin = item.Distance
			} else if results.Len() >= req.K &&
				math32.Abs(item.Distance) > math32.Abs(dynamicPruningMin*(epsilon2+1)) {
				// Dynamic pruning: skip this leaf if it's too far
				continue
			}
			leavesExplored++

			// Use quantized vectors if available
			if idx.config.UseQuantization && node.QuantizedVectors != nil {
				// Use quantized search
				tempDists := stackAllocator.AllocFloat32s(len(node.Members))
				tempErrorBounds := stackAllocator.AllocFloat32s(len(node.Members))
				var quantizer quantize.Quantizer
				if node.Parent == 0 {
					quantizer = idx.rootQuantizer
				} else {
					quantizer = idx.quantizer
				}
				quantizer.EstimateDistances(
					&stackAllocator,
					node.QuantizedVectors,
					q.Transformed(),
					tempDists,
					tempErrorBounds,
				)

				for i, memberID := range node.Members {
					// Check filter if provided
					if shouldSkip(memberID) {
						continue
					}

					dist := tempDists[i]
					pi := &PriorityItem{
						ID:          memberID,
						Distance:    dist,
						ErrorBounds: tempErrorBounds[i],
					}
					if req.DistanceOver != nil {
						if !pi.MaybeOver(*req.DistanceOver) {
							continue
						}
					}
					if req.DistanceUnder != nil {
						if !pi.MaybeUnder(*req.DistanceUnder) {
							continue
						}
					}

					// Get metadata for collapse key checking
					var metadata []byte
					if collapseKeyToItem != nil {
						metaKey := makeHBCMetadataKey(idx.prefix, memberID)
						if metaData, closer, err := idx.indexDB.Get(metaKey); err == nil {
							metadata = bytes.Clone(metaData)
							_ = closer.Close()
						}
					}

					if results.Len() < req.K {
						insertResultWithCollapse(results, pi, metadata, collapseKeyToItem, idx.config.CollapseKeysFunc)
						continue
					}
					topCurrentResult := results.Peek()
					if topCurrentResult == nil {
						continue
					}
					if dist < topCurrentResult.Distance {
						if pi.DefinitelyCloser(topCurrentResult) {
							// Remove the furthest result if the new result is closer even considering error bounds
							heap.Pop(results)
						}
						insertResultWithCollapse(results, pi, metadata, collapseKeyToItem, idx.config.CollapseKeysFunc)
					} else if pi.MaybeCloser(topCurrentResult) {
						insertResultWithCollapse(results, pi, metadata, collapseKeyToItem, idx.config.CollapseKeysFunc)
					}
					if results.Len() > req.K*int(math32.Ceil(epsilon2+1)) {
						heap.Pop(results)
					}
				}
				stackAllocator.FreeFloat32s(tempErrorBounds)
				stackAllocator.FreeFloat32s(tempDists)
			} else {
				// Use exact search (for root or when quantization is disabled)
				members := node.Members

				tempVec := stackAllocator.AllocVector(int(idx.config.Dimension))
				for _, memberID := range members {
					// Check filter if provided
					if shouldSkip(memberID) {
						continue
					}

					var err error
					if tempVec, err = idx.GetVector(idx.indexDB, memberID, tempVec); err != nil {
						// FIXME (ajr) Handle error properly
						continue
					}
					// Use normalizedEmbedding for member vectors (not ROT-transformed, just normalized for Cosine)
					dist := vector.MeasureDistance(idx.config.DistanceMetric, normalizedEmbedding, tempVec)

					if req.DistanceOver != nil {
						if dist <= *req.DistanceOver {
							continue
						}
					}
					if req.DistanceUnder != nil {
						if dist >= *req.DistanceUnder {
							continue
						}
					}

					// Get metadata for collapse key checking
					var metadata []byte
					if collapseKeyToItem != nil {
						metaKey := makeHBCMetadataKey(idx.prefix, memberID)
						if metaData, closer, err := idx.indexDB.Get(metaKey); err == nil {
							metadata = bytes.Clone(metaData)
							_ = closer.Close()
						}
					}

					pi := &PriorityItem{ID: memberID, Distance: dist}
					if results.Len() < req.K {
						insertResultWithCollapse(results, pi, metadata, collapseKeyToItem, idx.config.CollapseKeysFunc)
					} else {
						topResult := results.Peek()
						if topResult != nil && dist < topResult.Distance {
							heap.Pop(results)
							insertResultWithCollapse(results, pi, metadata, collapseKeyToItem, idx.config.CollapseKeysFunc)
						}
					}
				}
				stackAllocator.FreeVector(tempVec)
			}
		} else {
			// Use quantized vectors if available
			if idx.config.UseQuantization && node.QuantizedVectors != nil {
				// Use quantized search
				count := len(node.Children)
				tempFloats := stackAllocator.AllocFloat32s(count * 2)

				// Estimate distances of the data vectors from the query vector.
				tempDistances := tempFloats[:count]
				tempErrorBounds := tempFloats[count : count*2]
				var quantizer quantize.Quantizer
				if node.Parent == 0 {
					quantizer = idx.rootQuantizer
				} else {
					quantizer = idx.quantizer
				}
				quantizer.EstimateDistances(&stackAllocator, node.QuantizedVectors, q.Transformed(), tempDistances, tempErrorBounds)

				children := node.Children

				for i, childID := range children {
					dist := tempDistances[i]
					if !visited[childID] {
						heap.Push(candidates, &PriorityItem{ID: childID, Distance: dist})
					}
				}
				stackAllocator.FreeFloat32s(tempFloats)
			} else {
				// Add children to candidates
				children := node.Children

				for _, childID := range children {
					if !visited[childID] {
						child, err := idx.loadNode(idx.indexDB, idx.readerCache, childID)
						if err != nil {
							continue
						}
						childCentroid := child.Centroid
						childDist := vector.MeasureDistance(idx.config.DistanceMetric, q.Transformed(), childCentroid)
						heap.Push(candidates, &PriorityItem{ID: childID, Distance: childDist})
					}
				}
			}
		}
	}

	// Extract results, skipping items marked as removed during collapse key deduplication
	finalResults := make([]*Result, 0, results.Len())

	for results.Len() > 0 {
		item := heap.Pop(results).(*PriorityItem)

		// Skip tombstoned items that were superseded by better results for same collapse key
		if item.Removed {
			continue
		}

		// Get metadata if exists
		var metadata []byte
		metaKey := makeHBCMetadataKey(idx.prefix, item.ID)
		if metaData, closer, err := idx.indexDB.Get(metaKey); err == nil {
			metadata = bytes.Clone(metaData)
			_ = closer.Close()
		}

		childDist := item.Distance
		res := &Result{
			ID:         item.ID,
			Distance:   childDist,
			Metadata:   metadata,
			ErrorBound: item.ErrorBounds,
		}
		finalResults = append(finalResults, res)
	}
	slices.SortFunc(finalResults, func(a, b *Result) int {
		return cmp.Compare(a.Distance, b.Distance)
	})
	rerankPolicy := idx.resolveRerankPolicy(req)
	populateApproxSearchDebug(req.Debug, finalResults, req.K, req.DistanceOver, req.DistanceUnder, rerankPolicy)
	exactRerank := rerankPolicy == RerankPolicyAlways
	rerankCandidates := []int(nil)
	switch rerankPolicy {
	case RerankPolicyAlways:
		rerankCandidates = fullResultIndexList(len(finalResults))
	case RerankPolicyBoundary:
		rerankCandidates = selectAutoRerankCandidates(
			finalResults,
			req.K,
			req.DistanceOver,
			req.DistanceUnder,
		)
		exactRerank = len(rerankCandidates) > 0
	}
	if req.Debug != nil {
		req.Debug.RerankCandidateCount = len(rerankCandidates)
	}
	if exactRerank && len(rerankCandidates) > 0 {
		exactResults := make([]*Result, 0, len(rerankCandidates))
		for _, candidateIdx := range rerankCandidates {
			res := finalResults[candidateIdx]
			res.Vector = make(vector.T, int(idx.config.Dimension))
			var err error
			if res.Vector, err = idx.GetVector(idx.indexDB, res.ID, res.Vector); err != nil {
				// FIXME (ajr) Handle error properly
				continue
			}
			exactResults = append(exactResults, res)
		}
		q.ComputeExactDistances(true, exactResults)
		for _, res := range exactResults {
			res.Vector = nil
		}
		slices.SortFunc(finalResults, func(a, b *Result) int {
			return cmp.Compare(a.Distance, b.Distance)
		})
	}
	populateExactSearchDebug(req.Debug, finalResults, exactRerank)
	if req.DistanceOver != nil {
		// Trim results that don't satisfy the DistanceOver condition.
		// Results are sorted ascending by distance, so find the first
		// index where distance > threshold and discard everything before it.
		start := len(finalResults)
		for i, res := range finalResults {
			if res.Distance > *req.DistanceOver {
				start = i
				break
			}
		}
		finalResults = finalResults[start:]
	}
	if req.DistanceUnder != nil {
		// Trim results that don't satisfy the DistanceUnder condition.
		// Results are sorted ascending, so find the last index where
		// distance < threshold and discard everything after it.
		end := 0
		for i := len(finalResults) - 1; i >= 0; i-- {
			if finalResults[i].Distance < *req.DistanceUnder {
				end = i + 1
				break
			}
		}
		finalResults = finalResults[:end]
	}

	return finalResults[:min(len(finalResults), req.K)], nil
}

// Delete removes vectors from the index
func (idx *HBCIndex) Delete(ids ...uint64) error {
	b := &Batch{
		Deletes: ids,
	}
	return idx.Batch(context.Background(), b)
}

func (idx *HBCIndex) minLeafOccupancy() int {
	if idx.config.LeafSize <= 2 {
		return 1
	}
	return idx.config.LeafSize / 2
}

func (idx *HBCIndex) recomputeLeafCentroid(batch pebble.Reader, leaf *HBCNode) error {
	if len(leaf.Members) == 0 {
		clear(leaf.Centroid)
		return nil
	}
	if len(leaf.Centroid) != int(idx.config.Dimension) {
		leaf.Centroid = make(vector.T, idx.config.Dimension)
	} else {
		clear(leaf.Centroid)
	}
	tempVec := idx.writerAllocator.AllocVector(int(idx.config.Dimension))
	defer idx.writerAllocator.FreeVector(tempVec)
	for _, memberID := range leaf.Members {
		var err error
		tempVec, err = idx.GetVector(batch, memberID, tempVec)
		if err != nil {
			return fmt.Errorf("loading vector %d: %w", memberID, err)
		}
		idx.TransformVector(tempVec, tempVec)
		vec.AddFloat32(leaf.Centroid, tempVec)
	}
	vec.ScaleFloat32(1/float32(len(leaf.Members)), leaf.Centroid)
	idx.maybeNormalizeForMetric(leaf.Centroid)
	return nil
}

func (idx *HBCIndex) recomputeInternalCentroid(batch pebble.Reader, cacheBatch nodeCache, node *HBCNode) error {
	if len(node.Children) == 0 {
		clear(node.Centroid)
		return nil
	}
	if len(node.Centroid) != int(idx.config.Dimension) {
		node.Centroid = make(vector.T, idx.config.Dimension)
	} else {
		clear(node.Centroid)
	}
	for _, childID := range node.Children {
		child, err := idx.loadNode(batch, cacheBatch, childID)
		if err != nil {
			return fmt.Errorf("loading child %d: %w", childID, err)
		}
		vec.AddFloat32(node.Centroid, child.Centroid)
	}
	vec.ScaleFloat32(1/float32(len(node.Children)), node.Centroid)
	idx.maybeNormalizeForMetric(node.Centroid)
	return nil
}

func (idx *HBCIndex) collapseSingleChildParents(
	batch *pebble.Batch,
	cacheBatch nodeCache,
	meta *hbcIndexMetadata,
	startNodeID uint64,
) error {
	nodeID := startNodeID
	for nodeID != 0 {
		nodeUnsafe, err := idx.loadNode(batch, cacheBatch, nodeID)
		if err != nil {
			return err
		}
		if nodeUnsafe.IsLeaf || len(nodeUnsafe.Children) != 1 {
			return nil
		}

		childID := nodeUnsafe.Children[0]
		childUnsafe, err := idx.loadNode(batch, cacheBatch, childID)
		if err != nil {
			return err
		}
		child := idx.nodePool.Get().(*HBCNode)
		child.CopyFrom(childUnsafe)
		if childUnsafe.QuantizedVectors != nil {
			child.QuantizedVectors = childUnsafe.QuantizedVectors.Clone()
		}
		parentID := nodeUnsafe.Parent
		child.Parent = parentID
		if err := idx.saveNode(batch, cacheBatch, child); err != nil {
			child.reset()
			idx.nodePool.Put(child)
			return err
		}
		child.reset()
		idx.nodePool.Put(child)

		if parentID == 0 {
			meta.RootNode = childID
			if err := idx.deleteNode(batch, cacheBatch, nodeID); err != nil {
				return err
			}
			return nil
		}

		parentUnsafe, err := idx.loadNode(batch, cacheBatch, parentID)
		if err != nil {
			return err
		}
		parent := idx.nodePool.Get().(*HBCNode)
		parent.CopyFrom(parentUnsafe)
		if parentUnsafe.QuantizedVectors != nil {
			parent.QuantizedVectors = parentUnsafe.QuantizedVectors.Clone()
		}
		for i := range parent.Children {
			if parent.Children[i] == nodeID {
				parent.Children[i] = childID
				break
			}
		}
		if err := idx.recomputeInternalCentroid(batch, cacheBatch, parent); err != nil {
			parent.reset()
			idx.nodePool.Put(parent)
			return err
		}
		if err := idx.saveNode(batch, cacheBatch, parent); err != nil {
			parent.reset()
			idx.nodePool.Put(parent)
			return err
		}
		parent.reset()
		idx.nodePool.Put(parent)
		if err := idx.deleteNode(batch, cacheBatch, nodeID); err != nil {
			return err
		}
		nodeID = parentID
	}
	return nil
}

// removeFromLeaf removes a vector from the tree structure
func (idx *HBCIndex) removeFromLeaf(
	batch *pebble.Batch,
	cacheBatch nodeCache,
	meta *hbcIndexMetadata,
	leafID uint64,
	vectorID uint64,
) error {
	leafUnsafe, err := idx.loadNode(batch, cacheBatch, leafID)
	if err != nil {
		return err
	}
	leaf := idx.nodePool.Get().(*HBCNode)
	leaf.CopyFrom(leafUnsafe)
	defer func() {
		// Reset and return node to pool
		leaf.reset()
		idx.nodePool.Put(leaf)
	}()

	// Remove from members
	i := slices.IndexFunc(leaf.Members, func(id uint64) bool {
		return id == vectorID
	})
	if i < 0 {
		return fmt.Errorf("vector %d not found in leaf %d", vectorID, leafID)
	}
	if leafUnsafe.QuantizedVectors != nil {
		leaf.QuantizedVectors = leafUnsafe.QuantizedVectors.Clone()
	}

	// Load the removed vector for incremental centroid update. If the vector
	// is already gone from VectorDB (externally deleted), we skip the
	// incremental update and just clear the centroid — the next split will
	// recompute it exactly.
	removedVec := idx.writerAllocator.AllocVector(int(idx.config.Dimension))
	defer idx.writerAllocator.FreeVector(removedVec)
	hasRemovedVec := false
	if _, err := idx.GetVector(batch, vectorID, removedVec); err == nil {
		idx.TransformVector(removedVec, removedVec)
		hasRemovedVec = true
	}

	oldCount := len(leaf.Members)
	leaf.Members = utils.ReplaceWithLast(leaf.Members, i)
	if leaf.QuantizedVectors != nil {
		leaf.QuantizedVectors.ReplaceWithLast(i)
	}

	if len(leaf.Members) > 0 && hasRemovedVec && len(leaf.Centroid) == len(removedVec) {
		nfOld := float32(oldCount)
		nfNew := float32(len(leaf.Members))
		for i := range leaf.Centroid {
			leaf.Centroid[i] = (leaf.Centroid[i]*nfOld - removedVec[i]) / nfNew
		}
	} else if len(leaf.Members) == 0 {
		clear(leaf.Centroid)
	}

	if len(leaf.Members) == 0 && leaf.Parent != 0 {
		parentUnsafe, err := idx.loadNode(batch, cacheBatch, leaf.Parent)
		if err != nil {
			return err
		}
		parent := idx.nodePool.Get().(*HBCNode)
		parent.CopyFrom(parentUnsafe)
		if parentUnsafe.QuantizedVectors != nil {
			parent.QuantizedVectors = parentUnsafe.QuantizedVectors.Clone()
		}
		filtered := parent.Children[:0]
		for _, childID := range parent.Children {
			if childID != leafID {
				filtered = append(filtered, childID)
			}
		}
		parent.Children = filtered
		if err := idx.recomputeInternalCentroid(batch, cacheBatch, parent); err != nil {
			parent.reset()
			idx.nodePool.Put(parent)
			return err
		}
		if err := idx.saveNode(batch, cacheBatch, parent); err != nil {
			parent.reset()
			idx.nodePool.Put(parent)
			return err
		}
		parent.reset()
		idx.nodePool.Put(parent)
		if err := idx.deleteNode(batch, cacheBatch, leafID); err != nil {
			return err
		}
		return idx.collapseSingleChildParents(batch, cacheBatch, meta, leaf.Parent)
	}

	if err := idx.saveNodeNoUpdateQuantizedVectors(batch, cacheBatch, leaf); err != nil {
		return err
	}

	if leaf.Parent == 0 || len(leaf.Members) >= idx.minLeafOccupancy() {
		return nil
	}

	parentUnsafe, err := idx.loadNode(batch, cacheBatch, leaf.Parent)
	if err != nil {
		return err
	}
	var bestSibling *HBCNode
	bestDist := float32(math.MaxFloat32)
	for _, childID := range parentUnsafe.Children {
		if childID == leafID {
			continue
		}
		siblingUnsafe, err := idx.loadNode(batch, cacheBatch, childID)
		if err != nil || !siblingUnsafe.IsLeaf {
			continue
		}
		if len(siblingUnsafe.Members)+len(leaf.Members) > idx.config.LeafSize {
			continue
		}
		dist := vector.MeasureDistance(idx.config.DistanceMetric, leaf.Centroid, siblingUnsafe.Centroid)
		if dist < bestDist {
			if bestSibling != nil {
				bestSibling.reset()
				idx.nodePool.Put(bestSibling)
			}
			bestDist = dist
			bestSibling = idx.nodePool.Get().(*HBCNode)
			bestSibling.CopyFrom(siblingUnsafe)
			if siblingUnsafe.QuantizedVectors != nil {
				bestSibling.QuantizedVectors = siblingUnsafe.QuantizedVectors.Clone()
			}
		}
	}
	if bestSibling == nil {
		return nil
	}

	bestSibling.Members = append(bestSibling.Members, leaf.Members...)
	if err := idx.recomputeLeafCentroid(batch, bestSibling); err != nil {
		bestSibling.reset()
		idx.nodePool.Put(bestSibling)
		return err
	}
	if err := idx.saveNode(batch, cacheBatch, bestSibling); err != nil {
		bestSibling.reset()
		idx.nodePool.Put(bestSibling)
		return err
	}
	bestSibling.reset()
	idx.nodePool.Put(bestSibling)

	parent := idx.nodePool.Get().(*HBCNode)
	parent.CopyFrom(parentUnsafe)
	if parentUnsafe.QuantizedVectors != nil {
		parent.QuantizedVectors = parentUnsafe.QuantizedVectors.Clone()
	}
	filtered := parent.Children[:0]
	for _, childID := range parent.Children {
		if childID != leafID {
			filtered = append(filtered, childID)
		}
	}
	parent.Children = filtered
	if err := idx.recomputeInternalCentroid(batch, cacheBatch, parent); err != nil {
		parent.reset()
		idx.nodePool.Put(parent)
		return err
	}
	if err := idx.saveNode(batch, cacheBatch, parent); err != nil {
		parent.reset()
		idx.nodePool.Put(parent)
		return err
	}
	parent.reset()
	idx.nodePool.Put(parent)

	if err := idx.deleteNode(batch, cacheBatch, leafID); err != nil {
		return err
	}
	return idx.collapseSingleChildParents(batch, cacheBatch, meta, leaf.Parent)
}

// findLeafForVectorID finds the leaf node containing a vector
func (idx *HBCIndex) findLeafForVectorID(
	batch pebble.Reader,
	cacheBatch nodeCache,
	nodeID uint64,
	vectorID uint64,
) (uint64, error) {
	// First check if the vector exists in the database
	metaKey := makeHBCMetadataKey(idx.prefix, vectorID)
	_, closer, err := batch.Get(metaKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, ErrNotFound
		}
		return 0, fmt.Errorf("checking existence: %w", err)
	}
	_ = closer.Close()

	node, err := idx.loadNode(batch, cacheBatch, nodeID)
	if err != nil {
		return 0, fmt.Errorf("loading nodeID for search %d: %w", nodeID, err)
	}
	if node.IsLeaf {
		if slices.Contains(node.Members, vectorID) {
			return nodeID, nil
		}
		return 0, ErrNotFound
	}

	// Search children
	for _, childID := range node.Children {
		leafID, err := idx.findLeafForVectorID(batch, cacheBatch, childID, vectorID)
		if err == nil && leafID != 0 {
			return leafID, nil
		}
	}

	return 0, ErrNotFound
}

// GetMetadata retrieves metadata for a vector
func (idx *HBCIndex) GetMetadata(id uint64) ([]byte, error) {
	metaKey := makeHBCMetadataKey(idx.prefix, id)
	metaData, closer, err := idx.indexDB.Get(metaKey)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}
	defer func() { _ = closer.Close() }()

	return bytes.Clone(metaData), nil
}

// TotalVectors returns the total number of active vectors
func (idx *HBCIndex) TotalVectors() uint64 {
	idx.metaMu.RLock()
	defer idx.metaMu.RUnlock()

	meta, err := idx.getMetadata(idx.indexDB)
	if err != nil {
		return 0
	}

	return meta.ActiveCount
}

// Stats returns index statistics
func (idx *HBCIndex) Stats() map[string]any {
	idx.metaMu.RLock()
	meta, _ := idx.getMetadata(idx.indexDB)
	idx.metaMu.RUnlock()

	stats := map[string]any{
		"cache_size": idx.nodeCache.Len(),
		// "cache_ttl":  idx.config.CacheTTL,
	}
	if meta != nil {
		stats["total_indexed"] = meta.ActiveCount
		stats["total_nodes"] = meta.NodeCount
	}

	return stats
}

// PrettyPrint returns a string representation of the HBC tree structure
func (idx *HBCIndex) PrettyPrint(batch *pebble.Batch, cacheBatch nodeCache) string {
	var db pebble.Reader = idx.indexDB
	if batch != nil {
		db = batch
	}
	meta, err := idx.getMetadata(db)
	if err != nil {
		return fmt.Sprintf("Error loading metadata: %v", err)
	}

	var result string
	result += fmt.Sprintf("HBC Index: %s\n", idx.config.Name)
	result += fmt.Sprintf("├── Dimension: %d\n", idx.config.Dimension)
	result += fmt.Sprintf("├── Branching Factor: %d\n", idx.config.BranchingFactor)
	result += fmt.Sprintf("├── Leaf Size: %d\n", idx.config.LeafSize)
	result += fmt.Sprintf("├── Active Vectors: %d\n", meta.ActiveCount)
	result += fmt.Sprintf("├── Total Nodes: %d\n", meta.NodeCount)
	result += "└── Tree Structure:\n"

	if meta.RootNode == 0 {
		result += "    └── (empty)\n"
		return result
	}

	// Traverse and print tree
	treeStr, err := idx.prettyPrintNode(db, cacheBatch, meta.RootNode, "    ", true)
	if err != nil {
		result += fmt.Sprintf("    └── Error: %v\n", err)
	} else {
		result += treeStr
	}

	return result
}

// prettyPrintNode recursively prints a node and its children
func (idx *HBCIndex) prettyPrintNode(
	db pebble.Reader,
	cacheBatch nodeCache,
	nodeID uint64,
	prefix string,
	isLast bool,
) (string, error) {
	node, err := idx.loadNode(db, cacheBatch, nodeID)
	if err != nil {
		return "", err
	}
	var result string

	// Determine the connector
	connector := "├── "
	if isLast {
		connector = "└── "
	}

	// Format node info
	nodeInfo := fmt.Sprintf("Node %d", node.ID)
	if node.IsLeaf {
		nodeInfo += fmt.Sprintf(" (Leaf, %d vectors)", len(node.Members))
		if len(node.Members) > 0 && len(node.Members) <= 5 {
			nodeInfo += fmt.Sprintf(" [%v]", node.Members)
		} else if len(node.Members) > 5 {
			nodeInfo += fmt.Sprintf(" [%v...%v]", node.Members[:3], node.Members[len(node.Members)-2:])
		}
	} else {
		nodeInfo += fmt.Sprintf(" (Internal, %d children)", len(node.Children))
	}

	// Add centroid preview (first 3 dimensions)
	if len(node.Centroid) > 0 {
		centroidPreview := "["
		dims := min(3, len(node.Centroid))
		var centroidPreviewSb2168 strings.Builder
		for i := range dims {
			if i > 0 {
				centroidPreviewSb2168.WriteString(", ")
			}
			fmt.Fprintf(&centroidPreviewSb2168, "%.3f", node.Centroid[i])
		}
		centroidPreview += centroidPreviewSb2168.String()
		if len(node.Centroid) > 3 {
			centroidPreview += "..."
		}
		centroidPreview += "]"
		nodeInfo += " centroid: " + centroidPreview
	}

	result += prefix + connector + nodeInfo + "\n"

	// Process children if internal node
	if !node.IsLeaf && len(node.Children) > 0 {
		childPrefix := prefix
		if isLast {
			childPrefix += "    "
		} else {
			childPrefix += "│   "
		}

		var resultSb2192 strings.Builder
		for i, childID := range node.Children {
			isLastChild := i == len(node.Children)-1
			childStr, err := idx.prettyPrintNode(db, cacheBatch, childID, childPrefix, isLastChild)
			if err != nil {
				resultSb2192.WriteString(childPrefix + "└── Error loading child " + fmt.Sprintf(
					"%d: %v\n",
					childID,
					err,
				))
			} else {
				resultSb2192.WriteString(childStr)
			}
		}
		result += resultSb2192.String()
	}

	return result, nil
}

// deleteNode deletes all keys associated with a node from PebbleDB
func (idx *HBCIndex) deleteNode(batch *pebble.Batch, cacheBatch nodeCache, nodeID uint64) error {
	// Delete centroid
	centroidKey := makeHBCCentroidKey(idx.prefix, nodeID)
	if err := batch.Delete(centroidKey, nil); err != nil {
		return fmt.Errorf("failed to delete centroid for node %d: %w", nodeID, err)
	}

	// Delete parent
	parentKey := makeHBCParentKey(idx.prefix, nodeID)
	if err := batch.Delete(parentKey, nil); err != nil {
		return fmt.Errorf("failed to delete parent for node %d: %w", nodeID, err)
	}

	// Delete children
	childrenKey := makeHBCChildrenKey(idx.prefix, nodeID)
	if err := batch.Delete(childrenKey, nil); err != nil {
		return fmt.Errorf("failed to delete children for node %d: %w", nodeID, err)
	}

	// Delete members
	membersKey := makeHBCMembersKey(idx.prefix, nodeID)
	if err := batch.Delete(membersKey, nil); err != nil {
		return fmt.Errorf("failed to delete members for node %d: %w", nodeID, err)
	}

	// Remove from cache (eviction callback will handle cleanup)
	cacheBatch.Delete(nodeID)

	return nil
}

// serializeQuantizedSet serializes a quantized vector set for storage
func (idx *HBCIndex) serializeQuantizedSet(qs quantize.QuantizedVectorSet) []byte {
	// For RaBitQ, we need to serialize the RaBitQuantizedVectorSet
	if rqs, ok := qs.(*quantize.RaBitQuantizedVectorSet); ok {
		b, err := proto.Marshal(rqs)
		if err != nil {
			panic(fmt.Errorf("failed to marshal quantized set: %w", err))
		}
		return b
	}

	// For NonQuantizer, serialize the vector set
	if uqs, ok := qs.(*quantize.NonQuantizedVectorSet); ok {
		b, err := proto.Marshal(uqs)
		if err != nil {
			panic(fmt.Errorf("failed to marshal quantized set: %w", err))
		}
		return b
	}

	panic(fmt.Errorf("unknown quantized set type: %T", qs))
}

// deserializeQuantizedSet deserializes a quantized vector set from storage
func (idx *HBCIndex) deserializeQuantizedSet(
	data []byte, isRoot bool, expectedCount int,
) (quantize.QuantizedVectorSet, error) {
	if idx.config.UseQuantization && !isRoot {
		// Deserialize RaBitQuantizedVectorSet
		r := &quantize.RaBitQuantizedVectorSet{}
		if err := proto.Unmarshal(data, r); err != nil {
			return nil, fmt.Errorf("decoding quantized set: %w", err)
		}
		if err := idx.validateRaBitQuantizedSet(r, expectedCount); err != nil {
			return nil, err
		}
		return r, nil
	} else {
		r := &quantize.NonQuantizedVectorSet{}
		if err := proto.Unmarshal(data, r); err != nil {
			return nil, fmt.Errorf("decoding quantized set: %w", err)
		}
		return r, nil
	}
}

func (idx *HBCIndex) validateRaBitQuantizedSet(
	qs *quantize.RaBitQuantizedVectorSet, expectedCount int,
) error {
	if qs.GetMetric() != idx.config.DistanceMetric {
		return fmt.Errorf("quantized set distance metric mismatch: expected %s, got %s",
			idx.config.DistanceMetric, qs.GetMetric())
	}
	if len(qs.GetCentroid()) != int(idx.config.Dimension) {
		return fmt.Errorf("quantized set centroid dimension mismatch: expected %d, got %d",
			idx.config.Dimension, len(qs.GetCentroid()))
	}

	codes := qs.GetCodes()
	if codes == nil {
		return errors.New("quantized set is missing codes")
	}
	expectedWidth := quantize.RaBitQCodeSetWidth(int(idx.config.Dimension))
	if int(codes.GetWidth()) != expectedWidth {
		return fmt.Errorf("quantized set code width mismatch: expected %d, got %d",
			expectedWidth, codes.GetWidth())
	}
	if int(codes.GetCount()) != expectedCount {
		return fmt.Errorf("quantized set code count mismatch: expected %d, got %d",
			expectedCount, codes.GetCount())
	}
	if len(codes.GetData()) != expectedCount*expectedWidth {
		return fmt.Errorf("quantized set code data length mismatch: expected %d, got %d",
			expectedCount*expectedWidth, len(codes.GetData()))
	}
	if len(qs.GetCodeCounts()) != expectedCount {
		return fmt.Errorf("quantized set code_counts length mismatch: expected %d, got %d",
			expectedCount, len(qs.GetCodeCounts()))
	}
	if len(qs.GetCentroidDistances()) != expectedCount {
		return fmt.Errorf(
			"quantized set centroid_distances length mismatch: expected %d, got %d",
			expectedCount, len(qs.GetCentroidDistances()))
	}
	if len(qs.GetQuantizedDotProducts()) != expectedCount {
		return fmt.Errorf(
			"quantized set quantized_dot_products length mismatch: expected %d, got %d",
			expectedCount, len(qs.GetQuantizedDotProducts()))
	}
	if idx.config.DistanceMetric != vector.DistanceMetric_L2Squared &&
		len(qs.GetCentroidDotProducts()) != expectedCount {
		return fmt.Errorf(
			"quantized set centroid_dot_products length mismatch: expected %d, got %d",
			expectedCount, len(qs.GetCentroidDotProducts()))
	}
	return nil
}

// Close closes the index and releases resources
func (idx *HBCIndex) Close() error {
	idx.metaMu.Lock()
	defer idx.metaMu.Unlock()

	// Don't call DeleteAll() as it can cause race conditions in ttlcache's
	// eviction callback WaitGroup management. The cache will be garbage
	// collected naturally, and nodes in the pool don't need explicit cleanup.
	// See: https://github.com/jellydator/ttlcache/issues with concurrent evictions

	// Note: We don't close the databases here since they're externally managed
	return nil
}

func (idx *HBCIndex) GetVector(r pebble.Reader, id uint64, dst vector.T) (vector.T, error) {
	// Get the docID from metadata (stored in indexDB or from a batch during transaction)
	// Use the provided reader 'r' which could be a batch with uncommitted changes
	metaKey := makeHBCMetadataKey(idx.prefix, id)
	metaData, closer, err := r.Get(metaKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("reading metadata: %w", err)
	}
	docID := bytes.Clone(metaData)
	_ = closer.Close()

	// Get the vector using the docID from vectorDB
	// Note: Vectors are always in vectorDB, not in the batch
	vecKey := append(docID, idx.suffix...)
	if err := GetVector(idx.vectorDB, vecKey, dst); err != nil {
		return dst, fmt.Errorf("getting vector for key %s: %w", vecKey, err)
	}
	return dst, nil
}

// DeleteIndex deletes all data associated with an HBC index
func DeleteIndex(batch *pebble.Batch, name string) error {
	pre := fmt.Appendf(bytes.Clone(hbcIndexPrefix), "%d:", xxhash.Sum64String(name))
	if err := batch.DeleteRange(pre, utils.PrefixSuccessor(pre), nil); err != nil {
		return fmt.Errorf("deleting node data: %w", err)
	}
	return nil
}
