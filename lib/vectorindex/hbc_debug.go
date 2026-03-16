package vectorindex

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"slices"

	"github.com/cockroachdb/pebble/v2"
)

type HBCDebugNode struct {
	ID       uint64    `json:"id"`
	IsLeaf   bool      `json:"is_leaf"`
	Parent   uint64    `json:"parent"`
	Level    int       `json:"level"`
	Centroid []float32 `json:"centroid"`
	Children []uint64  `json:"children,omitempty"`
	Members  []uint64  `json:"members,omitempty"`
}

type HBCDebugDump struct {
	Dimension       uint32         `json:"dimension"`
	BranchingFactor int            `json:"branching_factor"`
	LeafSize        int            `json:"leaf_size"`
	SearchWidth     int            `json:"search_width"`
	UseQuantization bool           `json:"use_quantization"`
	UseROT          bool           `json:"use_rot"`
	QuantizerSeed   uint64         `json:"quantizer_seed"`
	RootNode        uint64         `json:"root_node"`
	ActiveCount     uint64         `json:"active_count"`
	NodeCount       uint64         `json:"node_count"`
	Nodes           []HBCDebugNode `json:"nodes"`
}

func (idx *HBCIndex) DebugDump() (*HBCDebugDump, error) {
	meta, err := idx.getMetadata(idx.indexDB)
	if err != nil {
		return nil, err
	}

	nodes := make([]HBCDebugNode, 0, meta.NodeCount)
	seen := map[uint64]struct{}{}
	queue := []uint64{meta.RootNode}

	for len(queue) > 0 {
		nodeID := queue[0]
		queue = queue[1:]
		if nodeID == 0 {
			continue
		}
		if _, ok := seen[nodeID]; ok {
			continue
		}
		seen[nodeID] = struct{}{}

		node, err := idx.loadNode(idx.indexDB, nil, nodeID)
		if err != nil {
			return nil, err
		}

		debugNode := HBCDebugNode{
			ID:       node.ID,
			IsLeaf:   node.IsLeaf,
			Parent:   node.Parent,
			Level:    node.Level,
			Centroid: slices.Clone(node.Centroid),
			Children: slices.Clone(node.Children),
			Members:  slices.Clone(node.Members),
		}
		nodes = append(nodes, debugNode)
		queue = append(queue, node.Children...)
	}

	slices.SortFunc(nodes, func(a, b HBCDebugNode) int {
		switch {
		case a.ID < b.ID:
			return -1
		case a.ID > b.ID:
			return 1
		default:
			return 0
		}
	})

	return &HBCDebugDump{
		Dimension:       idx.config.Dimension,
		BranchingFactor: idx.config.BranchingFactor,
		LeafSize:        idx.config.LeafSize,
		SearchWidth:     idx.config.SearchWidth,
		UseQuantization: idx.config.UseQuantization,
		UseROT:          idx.config.UseRandomOrthoTrans,
		QuantizerSeed:   idx.config.QuantizerSeed,
		RootNode:        meta.RootNode,
		ActiveCount:     meta.ActiveCount,
		NodeCount:       meta.NodeCount,
		Nodes:           nodes,
	}, nil
}

func OpenDebugPebbleBackedHBC(baseDir string, config HBCConfig, randSource rand.Source) (*HBCIndex, func() error, error) {
	db, err := pebble.Open(filepath.Join(baseDir, "pebble"), &pebble.Options{})
	if err != nil {
		return nil, nil, err
	}
	config.VectorDB = db
	config.IndexDB = db

	idx, err := NewHBCIndex(config, randSource)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}

	closeFn := func() error {
		if err := idx.Close(); err != nil {
			_ = db.Close()
			return err
		}
		return db.Close()
	}
	return idx, closeFn, nil
}

func OpenDebugPebbleBackedHBCWithBatch(baseDir string, config HBCConfig, randSource rand.Source, batch *Batch) (*HBCIndex, func() error, error) {
	db, err := pebble.Open(filepath.Join(baseDir, "pebble"), &pebble.Options{})
	if err != nil {
		return nil, nil, err
	}

	if batch != nil {
		if err := populateDebugVectors(db, config.Name, batch); err != nil {
			_ = db.Close()
			return nil, nil, err
		}
	}

	config.VectorDB = db
	config.IndexDB = db

	idx, err := NewHBCIndex(config, randSource)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}

	closeFn := func() error {
		if err := idx.Close(); err != nil {
			_ = db.Close()
			return err
		}
		return db.Close()
	}
	return idx, closeFn, nil
}

func populateDebugVectors(db *pebble.DB, indexName string, batch *Batch) error {
	suffix := fmt.Appendf(nil, ":i:%s:e", indexName)
	pbatch := db.NewBatch()
	defer func() { _ = pbatch.Close() }()

	if batch.MetadataList == nil {
		batch.MetadataList = make([][]byte, len(batch.IDs))
	}

	for i, id := range batch.IDs {
		docID := batch.MetadataList[i]
		if docID == nil {
			docID = fmt.Appendf(nil, "doc_%d", id)
			batch.MetadataList[i] = docID
		}
		vecKey := append(bytes.Clone(docID), suffix...)
		vecData := make([]byte, 0, 8+4*(len(batch.Vectors[i])+1))
		vecData, err := EncodeEmbeddingWithHashID(vecData, batch.Vectors[i], 0)
		if err != nil {
			return err
		}
		if err := pbatch.Set(vecKey, vecData, nil); err != nil {
			return err
		}
	}

	return pbatch.Commit(pebble.Sync)
}
