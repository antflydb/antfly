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

package indexes

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/template"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/utils"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble/v2"
	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
)

// NeedsEnricher returns true if the graph index has field-based edges or a summarizer configured.
func (g *GraphIndexV0) NeedsEnricher() bool {
	if g.conf.Summarizer != nil {
		return true
	}
	if g.conf.EdgeTypes != nil {
		for _, et := range *g.conf.EdgeTypes {
			if et.Field != "" {
				return true
			}
		}
	}
	return false
}

// LeaderFactory implements EnrichableIndex. It runs the graph enricher on the leader node.
// If no enrichment is configured, it blocks until the context is cancelled (no-op).
func (g *GraphIndexV0) LeaderFactory(ctx context.Context, persistFunc PersistFunc) error {
	if !g.NeedsEnricher() {
		<-ctx.Done()
		return nil
	}

	enricher, err := newGraphEnricher(ctx, g, persistFunc)
	if err != nil {
		return fmt.Errorf("creating graph enricher: %w", err)
	}

	g.enricherMu.Lock()
	g.enricher = enricher
	g.enricherMu.Unlock()

	// Block until context is cancelled (leadership lost)
	<-ctx.Done()

	g.enricherMu.Lock()
	g.enricher = nil
	g.enricherMu.Unlock()

	g.logger.Info("Graph enricher stopped", zap.String("name", g.name))
	return nil
}

// fieldEdgeTypes returns edge types that have a field configured.
func (g *GraphIndexV0) fieldEdgeTypes() []EdgeTypeConfig {
	if g.conf.EdgeTypes == nil {
		return nil
	}
	var result []EdgeTypeConfig
	for _, et := range *g.conf.EdgeTypes {
		if et.Field != "" {
			result = append(result, et)
		}
	}
	return result
}

// IsNavigable returns true if this graph has a tree edge type and a summarizer.
func (g *GraphIndexV0) IsNavigable() bool {
	if g.conf.Summarizer == nil {
		return false
	}
	if g.conf.EdgeTypes != nil {
		for _, et := range *g.conf.EdgeTypes {
			if et.Topology == EdgeTypeConfigTopologyTree {
				return true
			}
		}
	}
	return false
}

// GetTreeEdgeType returns the first edge type with tree topology, or empty string.
func (g *GraphIndexV0) GetTreeEdgeType() string {
	if g.conf.EdgeTypes != nil {
		for _, et := range *g.conf.EdgeTypes {
			if et.Topology == EdgeTypeConfigTopologyTree {
				return et.Name
			}
		}
	}
	return ""
}

// GetRoots returns all node keys that have no incoming tree edges.
func (g *GraphIndexV0) GetRoots(ctx context.Context) ([][]byte, error) {
	treeEdgeType := g.GetTreeEdgeType()
	if treeEdgeType == "" {
		return nil, fmt.Errorf("no tree edge type configured")
	}

	// Collect all nodes that ARE targets of tree edges (i.e., have a parent)
	hasParent := make(map[string]bool)

	// Scan the reverse index for all incoming tree edges
	inMarker := []byte(":i:" + g.name + ":in:" + treeEdgeType + ":")
	iter, err := g.indexDB.NewIterWithContext(ctx, &pebble.IterOptions{})
	if err != nil {
		return nil, fmt.Errorf("creating iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if bytes.Contains(key, inMarker) {
			// Extract target (everything before :i:)
			target, _, ok := bytes.Cut(key, []byte(":i:"))
			if ok {
				hasParent[string(target)] = true
			}
		}
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	// Scan all documents in range and find those without a parent
	var roots [][]byte
	err = storeutils.Scan(ctx, g.db, storeutils.ScanOptions{
		LowerBound: g.byteRange[0],
		UpperBound: g.byteRange[1],
		SkipPoint: func(userKey []byte) bool {
			return bytes.Contains(userKey, []byte(":i:"))
		},
	}, func(key []byte, value []byte) (bool, error) {
		// Only consider base document keys (those ending with :0x00)
		if !bytes.HasSuffix(key, storeutils.DBRangeStart) {
			return true, nil
		}
		// Extract the document key (before the suffix)
		docKey := key[:len(key)-len(storeutils.DBRangeStart)]
		if !hasParent[string(docKey)] {
			roots = append(roots, slices.Clone(docKey))
		}
		return true, nil
	})
	if err != nil {
		return nil, fmt.Errorf("scanning for roots: %w", err)
	}

	return roots, nil
}

// graphEnricher processes field-based edge extraction and summary generation
// for the graph index on the leader node.
type graphEnricher struct {
	graph       *GraphIndexV0
	persistFunc PersistFunc
	summarizer  ai.DocumentSummarizer
	zstdReader  *zstd.Decoder
	docQueue    chan []byte // queue of doc keys for inline processing after backfill
}

func newGraphEnricher(ctx context.Context, g *GraphIndexV0, persistFunc PersistFunc) (*graphEnricher, error) {
	zstdReader, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("creating zstd reader: %w", err)
	}

	e := &graphEnricher{
		graph:       g,
		persistFunc: persistFunc,
		zstdReader:  zstdReader,
		docQueue:    make(chan []byte, 1000),
	}

	// Initialize summarizer if configured (non-fatal: field edges still work without it)
	if g.conf.Summarizer != nil {
		summarizer, err := ai.NewDocumentSummarizer(*g.conf.Summarizer)
		if err != nil {
			g.logger.Warn("Failed to initialize document summarizer, summaries will be skipped",
				zap.String("name", g.name),
				zap.Error(err))
		} else {
			e.summarizer = summarizer
		}
	}

	// Start backfill + queue processor in background
	g.eg.Go(func() error {
		// Phase 1: backfill existing documents
		if err := e.backfill(g.egCtx); err != nil {
			return err
		}
		// Phase 2: process new document writes from queue
		return e.processQueue(g.egCtx)
	})

	g.logger.Info("Graph enricher started",
		zap.String("name", g.name),
		zap.Int("fieldEdgeTypes", len(g.fieldEdgeTypes())),
		zap.Bool("hasSummarizer", g.conf.Summarizer != nil))

	return e, nil
}

// Enqueue adds a document key to the processing queue.
// Non-blocking: if the queue is full, the document is skipped (will be caught by next backfill).
func (e *graphEnricher) Enqueue(docKey []byte) {
	select {
	case e.docQueue <- slices.Clone(docKey):
	default:
		e.graph.logger.Debug("Graph enricher queue full, skipping document",
			zap.String("key", types.FormatKey(docKey)))
	}
}

// processQueue drains the document queue and processes each document for field edges and summaries.
func (e *graphEnricher) processQueue(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case docKey := <-e.docQueue:
			// Read document from main DB
			fullKey := append(slices.Clone(docKey), storeutils.DBRangeStart...)
			value, closer, err := e.graph.db.Get(fullKey)
			if err != nil {
				if !errors.Is(err, pebble.ErrNotFound) {
					e.graph.logger.Warn("Failed to read document for enrichment",
						zap.String("key", types.FormatKey(docKey)),
						zap.Error(err))
				}
				continue
			}
			valueCopy := slices.Clone(value)
			_ = closer.Close()

			doc, err := parseDocument(valueCopy, e.zstdReader)
			if err != nil {
				e.graph.logger.Warn("Failed to parse document for enrichment",
					zap.String("key", types.FormatKey(docKey)),
					zap.Error(err))
				continue
			}

			e.EnrichDocument(ctx, docKey, doc)
		}
	}
}

// backfill scans all existing documents and processes field edges and summaries.
func (e *graphEnricher) backfill(ctx context.Context) error {
	totalProcessed := 0

	err := storeutils.Scan(ctx, e.graph.db, storeutils.ScanOptions{
		LowerBound: e.graph.byteRange[0],
		UpperBound: e.graph.byteRange[1],
		SkipPoint: func(userKey []byte) bool {
			// Skip index keys — only process base document keys
			return bytes.Contains(userKey, []byte(":i:"))
		},
	}, func(key []byte, value []byte) (bool, error) {
		// Only process base document keys (ending with :0x00)
		if !bytes.HasSuffix(key, storeutils.DBRangeStart) {
			return true, nil
		}

		docKey := key[:len(key)-len(storeutils.DBRangeStart)]

		// Parse document
		doc, err := parseDocument(value, e.zstdReader)
		if err != nil {
			e.graph.logger.Warn("Failed to parse document during backfill",
				zap.String("key", types.FormatKey(docKey)),
				zap.Error(err))
			return true, nil // skip but continue
		}

		// Process field edges
		if err := e.reconcileFieldEdges(ctx, docKey, doc); err != nil {
			e.graph.logger.Warn("Failed to reconcile field edges during backfill",
				zap.String("key", types.FormatKey(docKey)),
				zap.Error(err))
		}

		// Process summaries
		if e.summarizer != nil && e.graph.conf.Template != "" {
			if err := e.reconcileSummary(ctx, docKey, doc); err != nil {
				e.graph.logger.Warn("Failed to reconcile summary during backfill",
					zap.String("key", types.FormatKey(docKey)),
					zap.Error(err))
			}
		}

		totalProcessed++
		if totalProcessed%1000 == 0 {
			e.graph.logger.Debug("Graph enricher backfill progress",
				zap.String("name", e.graph.name),
				zap.Int("totalProcessed", totalProcessed))
		}
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("scanning for enrichment: %w", err)
	}

	e.graph.logger.Info("Graph enricher backfill complete",
		zap.String("name", e.graph.name),
		zap.Int("totalProcessed", totalProcessed))
	return nil
}

// EnrichDocument processes a single document for field edges and summaries.
// Called when new documents are written via Batch().
func (e *graphEnricher) EnrichDocument(ctx context.Context, docKey []byte, doc map[string]any) {
	if err := e.reconcileFieldEdges(ctx, docKey, doc); err != nil {
		e.graph.logger.Warn("Failed to reconcile field edges",
			zap.String("key", types.FormatKey(docKey)),
			zap.Error(err))
	}

	if e.summarizer != nil && e.graph.conf.Template != "" {
		if err := e.reconcileSummary(ctx, docKey, doc); err != nil {
			e.graph.logger.Warn("Failed to reconcile summary",
				zap.String("key", types.FormatKey(docKey)),
				zap.Error(err))
		}
	}
}

// reconcileFieldEdges processes field-based edge extraction for a single document.
// Uses hash-based change detection to skip unchanged documents.
func (e *graphEnricher) reconcileFieldEdges(ctx context.Context, docKey []byte, doc map[string]any) error {
	fieldEdgeTypes := e.graph.fieldEdgeTypes()
	if len(fieldEdgeTypes) == 0 {
		return nil
	}

	for _, et := range fieldEdgeTypes {
		// 1. Read field value, compute hash
		fieldValue, ok := doc[et.Field]
		if !ok {
			continue // field not present in document
		}
		desiredTargets := toStringSlice(fieldValue)
		fieldHash := xxhash.Sum64String(fmt.Sprint(desiredTargets))

		// 2. Check stored hash — skip if unchanged
		hashKey := makeFieldHashKey(docKey, e.graph.name, et.Name)
		storedHashVal, storedCloser, err := e.graph.db.Get(hashKey)
		if err == nil {
			_, storedHash, decErr := encoding.DecodeUint64Ascending(storedHashVal)
			_ = storedCloser.Close()
			if decErr == nil && storedHash == fieldHash {
				continue // field unchanged, skip
			}
		} else if !errors.Is(err, pebble.ErrNotFound) {
			return fmt.Errorf("reading field hash: %w", err)
		}

		// 3. Field changed: delete ALL existing edges for this doc+edgeType
		var writes [][2][]byte
		var deletes [][2][]byte // use writes with nil value for deletes through persistFunc

		edgePrefix := storeutils.EdgeIteratorPrefix(docKey, e.graph.name, et.Name)
		iter, err := e.graph.db.NewIterWithContext(ctx, &pebble.IterOptions{
			LowerBound: edgePrefix,
			UpperBound: utils.PrefixSuccessor(edgePrefix),
		})
		if err != nil {
			return fmt.Errorf("creating edge iterator: %w", err)
		}

		// Collect existing edges to delete via reverse index
		var edgeKeysToDelete [][]byte
		for iter.First(); iter.Valid(); iter.Next() {
			edgeKeysToDelete = append(edgeKeysToDelete, slices.Clone(iter.Key()))
		}
		if err := iter.Error(); err != nil {
			_ = iter.Close()
			return fmt.Errorf("edge iterator error: %w", err)
		}
		_ = iter.Close()

		// Remove old edges from reverse index
		if len(edgeKeysToDelete) > 0 {
			batch := e.graph.indexDB.NewBatch()
			for _, oldEdgeKey := range edgeKeysToDelete {
				source, target, _, edgeType, parseErr := storeutils.ParseEdgeKey(oldEdgeKey)
				if parseErr != nil {
					continue
				}
				if err := e.graph.removeFromEdgeIndex(batch, target, source, edgeType); err != nil {
					_ = batch.Close()
					return fmt.Errorf("removing old edge from index: %w", err)
				}
			}
			if err := batch.Commit(pebble.Sync); err != nil {
				_ = batch.Close()
				return fmt.Errorf("committing edge index deletes: %w", err)
			}
			_ = batch.Close()

			// Delete old forward edge keys through Raft
			for _, oldEdgeKey := range edgeKeysToDelete {
				deletes = append(deletes, [2][]byte{oldEdgeKey, nil})
			}
		}

		// 4. Create new edges from current field value
		now := time.Now()
		for _, target := range desiredTargets {
			if target == "" {
				continue
			}
			edgeKey := storeutils.MakeEdgeKey(docKey, []byte(target), e.graph.name, et.Name)
			edge := &Edge{
				Source:    docKey,
				Target:    []byte(target),
				Type:      et.Name,
				Weight:    1.0,
				CreatedAt: now,
				UpdatedAt: now,
			}
			edgeVal, encErr := EncodeEdgeValue(edge)
			if encErr != nil {
				return fmt.Errorf("encoding edge: %w", encErr)
			}
			writes = append(writes, [2][]byte{edgeKey, edgeVal})
		}

		// 5. Persist hash marker
		hashVal := make([]byte, 0, 8)
		hashVal = encoding.EncodeUint64Ascending(hashVal, fieldHash)
		writes = append(writes, [2][]byte{hashKey, hashVal})

		// 6. Persist deletes through Raft (nil value = delete)
		if len(deletes) > 0 {
			if err := e.persistFunc(ctx, deletes); err != nil {
				return fmt.Errorf("persisting edge deletes: %w", err)
			}
		}

		// 7. Persist new edges + hash through Raft
		if len(writes) > 0 {
			if err := e.persistFunc(ctx, writes); err != nil {
				return fmt.Errorf("persisting field edges: %w", err)
			}
		}
	}
	return nil
}

// reconcileSummary generates a summary for a document if the template input has changed.
// Uses hash-based change detection to skip unchanged documents.
func (e *graphEnricher) reconcileSummary(ctx context.Context, docKey []byte, doc map[string]any) error {
	// 1. Render template input
	input, err := template.Render(e.graph.conf.Template, doc)
	if err != nil {
		return fmt.Errorf("rendering template: %w", err)
	}
	if input == "" {
		return nil // empty input, skip
	}
	inputHash := xxhash.Sum64String(input)

	// 2. Check existing summary hash
	summaryKey := storeutils.MakeSummaryKey(docKey, e.graph.name)
	existing, existCloser, err := e.graph.db.Get(summaryKey)
	if err == nil {
		if len(existing) >= 8 {
			_, storedHash, decErr := encoding.DecodeUint64Ascending(existing[:8])
			_ = existCloser.Close()
			if decErr == nil && storedHash == inputHash {
				return nil // unchanged, skip
			}
		} else {
			_ = existCloser.Close()
		}
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return fmt.Errorf("reading existing summary: %w", err)
	}

	// 3. Generate new summary
	summaries, err := e.summarizer.SummarizeRenderedDocs(ctx, []string{input})
	if err != nil {
		return fmt.Errorf("generating summary: %w", err)
	}
	if len(summaries) == 0 {
		return nil
	}
	summary := summaries[0]

	// 4. Persist: [hashID:uint64][summary_text]
	val := make([]byte, 0, 8+len(summary))
	val = encoding.EncodeUint64Ascending(val, inputHash)
	val = append(val, summary...)

	return e.persistFunc(ctx, [][2][]byte{{summaryKey, val}})
}

// validateTreeTopology checks that adding an edge doesn't violate tree constraints.
// For tree topology: each source node can have at most one outgoing edge of this type.
// This enforces single-parent semantics: e.g., for "child_of" edges, each child
// can only point to one parent.
func (g *GraphIndexV0) validateTreeTopology(target []byte, edgeType string, source []byte) error {
	// Scan outgoing edges from the source for this edge type
	scanPrefix := storeutils.EdgeIteratorPrefix(source, g.name, edgeType)

	// Forward edge keys are stored in the main DB (not indexDB).
	// By the time Batch() calls this, the pebble batch is already committed.
	iter, err := g.db.NewIter(&pebble.IterOptions{
		LowerBound: scanPrefix,
		UpperBound: utils.PrefixSuccessor(scanPrefix),
	})
	if err != nil {
		return fmt.Errorf("creating tree validation iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		// Extract the existing target from the edge key
		_, existingTarget, _, _, parseErr := storeutils.ParseEdgeKey(iter.Key())
		if parseErr != nil {
			continue
		}

		// If the existing target is different from the new target, reject
		if !bytes.Equal(existingTarget, target) {
			return fmt.Errorf("tree topology violation: node %s already has outgoing %q edge to %s, cannot add edge to %s",
				types.FormatKey(source), edgeType, types.FormatKey(existingTarget), types.FormatKey(target))
		}
	}
	return iter.Error()
}

// getEdgeTypeConfig returns the EdgeTypeConfig for the given edge type name, or nil.
func (g *GraphIndexV0) getEdgeTypeConfig(edgeType string) *EdgeTypeConfig {
	if g.conf.EdgeTypes == nil {
		return nil
	}
	for i, et := range *g.conf.EdgeTypes {
		if et.Name == edgeType {
			return &(*g.conf.EdgeTypes)[i]
		}
	}
	return nil
}

// --- Helper functions ---

// parseDocument decompresses (zstd) and deserializes a document value into a map.
// Falls back to raw JSON if decompression fails (for uncompressed values).
func parseDocument(value []byte, zstdReader *zstd.Decoder) (map[string]any, error) {
	data := value

	// Try zstd decompression (documents are stored compressed in pebble)
	if zstdReader != nil {
		if err := zstdReader.Reset(bytes.NewReader(value)); err == nil {
			if decompressed, err := io.ReadAll(zstdReader); err == nil {
				data = decompressed
			}
		}
	}

	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("unmarshaling document: %w", err)
	}
	return doc, nil
}

// toStringSlice converts a field value to a slice of strings.
// Handles: string, []string, []any (containing strings).
func toStringSlice(v any) []string {
	switch val := v.(type) {
	case string:
		return []string{val}
	case []string:
		return val
	case []any:
		var result []string
		for _, item := range val {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
		return result
	default:
		return nil
	}
}

// makeFieldHashKey creates a key for storing field-based edge hash.
// Format: <docKey>:i:<graphName>:<edgeType>:fh
func makeFieldHashKey(docKey []byte, graphName, edgeType string) []byte {
	key := bytes.Clone(docKey)
	key = append(key, []byte(":i:")...)
	key = append(key, []byte(graphName)...)
	key = append(key, ':')
	key = append(key, []byte(edgeType)...)
	key = append(key, []byte(":fh")...)
	return key
}
