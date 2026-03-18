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
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	aflogger "github.com/antflydb/antfly/lib/logger"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/utils"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cockroachdb/pebble/v2"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var _ Index = (*GraphIndexV0)(nil)           // Compile-time check for Index interface implementation
var _ EnrichableIndex = (*GraphIndexV0)(nil) // Compile-time check for EnrichableIndex interface implementation

func init() {
	RegisterIndex(IndexTypeGraphV0, NewGraphIndexV0)
	RegisterIndex(IndexTypeGraph, NewGraphIndexV0)
}

// GraphIndexV0 implements the Index interface for graph edges.
// It maintains an edge index for efficient reverse lookups (target → sources).
// The reverse index is stored in a separate Pebble database instance for better isolation.
type GraphIndexV0 struct {
	name      string
	db        *pebble.DB // Main shard DB - outgoing edges are stored here
	indexDB   *pebble.DB // Separate DB - reverse index entries stored here
	indexPath string
	byteRange types.Range
	conf      GraphIndexConfig
	schema    *schema.TableSchema
	logger    *zap.Logger

	// Background backfill management
	rebuildState *RebuildState
	backfillDone chan struct{}
	egCancel     context.CancelFunc
	egCtx        context.Context
	eg           *errgroup.Group

	// Pause/Resume support
	pauseMu    sync.Mutex
	pauseCond  *sync.Cond
	pauseAckCh chan struct{} // Separate channel for backfill pause acknowledgment
	paused     atomic.Bool

	// Enricher for field-based edges and summaries
	enricherMu sync.Mutex
	enricher   *graphEnricher

	// Antfly config needed for AI provider initialization
	antflyConfig *common.Config

	// In-memory edge counters for O(1) Stats() reads.
	// totalEdges is derived from edgeTypeCounts under edgeTypeCountsMu.
	edgeTypeCountsMu sync.RWMutex
	edgeTypeCounts   map[string]uint64

	// Backfill tracking
	backfillTracker
}

// NewGraphIndexV0 creates a new graph index instance.
func NewGraphIndexV0(
	logger *zap.Logger,
	antflyConfig *common.Config,
	db *pebble.DB,
	dir string,
	name string,
	config *IndexConfig,
	cache *pebbleutils.Cache,
) (Index, error) {
	conf, err := config.AsGraphIndexConfig()
	if err != nil {
		return nil, fmt.Errorf("invalid graph index config: %w", err)
	}

	edgeTypesCount := 0
	if conf.EdgeTypes != nil {
		edgeTypesCount = len(*conf.EdgeTypes)
	}
	logger.Info("Creating graph index",
		zap.String("name", name),
		zap.Int("edge_types", edgeTypesCount))

	// Create index directory path
	indexPath := filepath.Join(dir, name)

	// Create separate Pebble instance for reverse index
	pebbleOpts := &pebble.Options{
		Logger:          &aflogger.NoopLoggerAndTracer{},
		LoggerAndTracer: &aflogger.NoopLoggerAndTracer{},
	}
	cache.Apply(pebbleOpts, 20*1024*1024) // 20MB fallback
	indexDB, err := pebble.Open(filepath.Join(indexPath, "reverseindex"), pebbleOpts)
	if err != nil {
		return nil, fmt.Errorf("opening reverse index database: %w", err)
	}

	g := &GraphIndexV0{
		name:           name,
		db:             db,
		indexDB:        indexDB,
		indexPath:      indexPath,
		conf:           conf,
		logger:         logger,
		rebuildState:   NewRebuildState(indexPath),
		antflyConfig:   antflyConfig,
		edgeTypeCounts: make(map[string]uint64),
		pauseAckCh:     make(chan struct{}, 1),
	}
	g.pauseCond = sync.NewCond(&g.pauseMu)

	// Initialize errgroup context for background operations
	g.egCtx, g.egCancel = context.WithCancel(context.Background())
	g.eg, g.egCtx = errgroup.WithContext(g.egCtx)

	return g, nil
}

// Name returns the index name
func (g *GraphIndexV0) Name() string {
	return g.name
}

func (g *GraphIndexV0) Type() IndexType {
	return IndexTypeGraph
}

// Batch processes edge write/delete operations and updates the edge index
func (g *GraphIndexV0) Batch(ctx context.Context, writes [][2][]byte, deletes [][]byte, sync bool) error {
	// Hold pauseMu for the duration of indexDB work so Pause() can wait
	// for any in-flight Batch to complete before returning.
	g.pauseMu.Lock()
	defer g.pauseMu.Unlock()

	// Check if paused - if so, skip processing (return nil, not error)
	if g.paused.Load() {
		return nil
	}

	// Use indexDB for reverse index writes (not main db)
	batch := g.indexDB.NewBatch()
	defer func() { _ = batch.Close() }()

	// Track edge type changes to update counters after successful commit.
	// Lazy-initialized only when edge keys are actually encountered.
	var edgeAdds map[string]int
	var edgeDeletes map[string]int

	// Process writes - update edge index for incoming edges
	for _, kv := range writes {
		key, value := kv[0], kv[1]

		// Check if this is an edge key
		if !g.isEdgeKey(key) {
			continue
		}

		source, target, indexName, edgeType, err := storeutils.ParseEdgeKey(key)
		if err != nil {
			g.logger.Warn("Failed to parse edge key",
				zap.String("key", types.FormatKey(key)),
				zap.Error(err))
			continue
		}

		// Only process edges for this index
		if indexName != g.name {
			continue
		}

		// Validate edge type
		if !g.isValidEdgeType(edgeType) {
			return fmt.Errorf("invalid edge type: %s (not configured in graph index)", edgeType)
		}

		// Validate tree topology constraints
		if etConf := g.getEdgeTypeConfig(edgeType); etConf != nil && etConf.Topology == EdgeTypeConfigTopologyTree {
			if err := g.validateTreeTopology(target, edgeType, source); err != nil {
				return err
			}
		}

		// If value is nil, read it from main db (index manager zeros out values)
		if len(value) == 0 {
			val, closer, err := g.db.Get(key)
			if errors.Is(err, pebble.ErrNotFound) {
				// Edge doesn't exist in main DB - this shouldn't happen since db.go
				// writes edges before indexing, but handle gracefully
				g.logger.Warn("Edge not found in main DB during index update",
					zap.String("key", types.FormatKey(key)),
					zap.String("source", types.FormatKey(source)),
					zap.String("target", types.FormatKey(target)),
					zap.String("edgeType", edgeType))
				continue
			}
			if err != nil {
				return fmt.Errorf("reading edge value from pebble: %w", err)
			}
			value = bytes.Clone(val) // Clone the value
			_ = closer.Close()
		}

		// Update edge index: store reverse mapping (target → source) in indexDB
		if err := g.addToEdgeIndex(batch, target, source, edgeType, value); err != nil {
			return fmt.Errorf("updating edge index: %w", err)
		}

		if edgeAdds == nil {
			edgeAdds = make(map[string]int)
		}
		edgeAdds[edgeType]++
	}

	// Process deletes - remove from edge index
	for _, key := range deletes {
		// Check if this is an edge key
		if !g.isEdgeKey(key) {
			continue
		}

		source, target, indexName, edgeType, err := storeutils.ParseEdgeKey(key)
		if err != nil {
			g.logger.Warn("Failed to parse edge key during delete",
				zap.String("key", types.FormatKey(key)),
				zap.Error(err))
			continue
		}

		// Only process edges for this index
		if indexName != g.name {
			continue
		}

		// Remove from edge index
		if err := g.removeFromEdgeIndex(batch, target, source, edgeType); err != nil {
			return fmt.Errorf("removing from edge index: %w", err)
		}

		if edgeDeletes == nil {
			edgeDeletes = make(map[string]int)
		}
		edgeDeletes[edgeType]++
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("committing edge index batch: %w", err)
	}

	// Update in-memory counters only after successful commit
	if len(edgeAdds) > 0 || len(edgeDeletes) > 0 {
		g.edgeTypeCountsMu.Lock()
		for et, count := range edgeAdds {
			g.edgeTypeCounts[et] += uint64(count) //nolint:gosec // G115: edge count is non-negative
		}
		for et, count := range edgeDeletes {
			if g.edgeTypeCounts[et] >= uint64(count) { //nolint:gosec // G115: edge count is non-negative
				g.edgeTypeCounts[et] -= uint64(count) //nolint:gosec // G115: edge count is non-negative
			} else {
				g.edgeTypeCounts[et] = 0
			}
		}
		g.edgeTypeCountsMu.Unlock()
	}

	// Enqueue base document writes for field-based edge enrichment
	g.enricherMu.Lock()
	enricher := g.enricher
	g.enricherMu.Unlock()

	if enricher != nil {
		for _, kv := range writes {
			key := kv[0]
			// Base document keys arrive as raw user keys (no :\x00 suffix) from the
			// index manager. Index-related keys (edges, embeddings, summaries, chunks)
			// all contain ":i:" in the key, so we can identify base documents by
			// checking for the absence of that marker.
			if !bytes.Contains(key, []byte(":i:")) {
				enricher.Enqueue(key)
			}
		}
	}

	return nil
}

// Search queries the edge index for reverse lookups
// query format: map[string]any{"target": []byte, "edgeType": string}
func (g *GraphIndexV0) Search(ctx context.Context, query any) (any, error) {
	q, ok := query.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid query format for graph index")
	}

	target, ok := q["target"].([]byte)
	if !ok {
		return nil, fmt.Errorf("missing or invalid 'target' field in query")
	}

	edgeType, _ := q["edgeType"].(string) // Optional edge type filter

	// Query edge index for incoming edges
	return g.queryEdgeIndex(ctx, target, edgeType)
}

// Close closes the graph index
func (g *GraphIndexV0) Close() error {
	g.logger.Info("Closing graph index", zap.String("name", g.name))

	// Cancel background operations
	if g.egCancel != nil {
		g.egCancel()
	}

	// Wait for background tasks to complete
	if g.eg != nil {
		if err := g.eg.Wait(); err != nil {
			// Ignore context canceled errors as they're expected
			if !errors.Is(err, context.Canceled) {
				g.logger.Warn("Error waiting for background tasks",
					zap.String("name", g.name),
					zap.Error(err))
			}
		}
	}

	// Close the separate indexDB instance
	if g.indexDB != nil {
		if err := g.indexDB.Close(); err != nil {
			g.logger.Warn("Error closing indexDB",
				zap.String("name", g.name),
				zap.Error(err))
			return fmt.Errorf("closing indexDB: %w", err)
		}
	}

	return nil
}

// Delete removes the graph index
func (g *GraphIndexV0) Delete() error {
	g.logger.Info("Deleting graph index", zap.String("name", g.name))

	// Remove the entire index directory (includes indexDB)
	if err := os.RemoveAll(g.indexPath); err != nil {
		g.logger.Warn("Failed to remove index directory",
			zap.String("indexPath", g.indexPath),
			zap.Error(err))
		return fmt.Errorf("removing index directory: %w", err)
	}

	return nil
}

// Open initializes the graph index
func (g *GraphIndexV0) Open(rebuild bool, schema *schema.TableSchema, byteRange types.Range) error {
	g.schema = schema
	g.byteRange = byteRange

	g.logger.Info("Opening graph index",
		zap.String("name", g.name),
		zap.Bool("rebuild", rebuild))

	// Check rebuild state
	var rebuildFrom []byte
	if !rebuild {
		var needsRebuild bool
		var err error
		rebuildFrom, needsRebuild, err = g.rebuildState.Check()
		if err != nil {
			return fmt.Errorf("checking rebuild status: %w", err)
		}
		if needsRebuild {
			g.logger.Info("Rebuild status found: resuming rebuilding graph index",
				zap.ByteString("rebuildFrom", rebuildFrom))
			rebuild = true
		}
	}

	if !rebuild {
		return nil // No rebuild needed
	}

	// Set rebuild state
	if err := g.rebuildState.Update(rebuildFrom); err != nil {
		g.logger.Warn("Failed to set rebuild status for graph index",
			zap.String("indexPath", g.indexPath),
			zap.Error(err))
		return fmt.Errorf("setting rebuild status: %w", err)
	}

	// Start backfill in background
	g.backfillDone = make(chan struct{})
	g.eg.Go(func() error {
		return g.backfillReverseIndex(rebuildFrom)
	})

	return nil
}

// backfillReverseIndex scans outgoing edges and rebuilds the reverse index
func (g *GraphIndexV0) backfillReverseIndex(startKey []byte) error {
	defer close(g.backfillDone)
	g.startBackfill()
	defer g.finishBackfill()
	defer func() {
		if err := g.rebuildState.Clear(); err != nil {
			g.logger.Warn("Failed to clear rebuild state",
				zap.String("indexPath", g.indexPath),
				zap.Error(err))
		}
	}()

	const maxBatchSize = 1000
	batch := g.indexDB.NewBatch()
	batchCount := 0
	totalScanned := 0
	totalIndexed := 0

	g.logger.Info("Starting backfill of graph reverse index",
		zap.String("name", g.name),
		zap.ByteString("startKey", startKey))

	// Determine start point
	lowerBound := startKey
	if lowerBound == nil || bytes.Compare(lowerBound, g.byteRange[0]) < 0 {
		lowerBound = g.byteRange[0]
	}

	// Scan outgoing edges in main DB
	iter, err := g.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: g.byteRange[1],
	})
	if err != nil {
		return fmt.Errorf("creating iterator for backfill: %w", err)
	}
	defer func() { _ = iter.Close() }()

	var lastKey []byte
	localEdgeCounts := make(map[string]uint64)

	for iter.First(); iter.Valid(); iter.Next() {
		select {
		case <-g.egCtx.Done():
			return g.egCtx.Err()
		default:
		}

		// Check if paused at start of each iteration
		if g.paused.Load() {
			// Send acknowledgment without holding any mutex
			select {
			case g.pauseAckCh <- struct{}{}:
			default:
			}

			// Now wait for resume
			g.pauseMu.Lock()
			for g.paused.Load() {
				select {
				case g.pauseAckCh <- struct{}{}:
				default:
				}
				g.pauseCond.Wait()
			}
			g.pauseMu.Unlock()
		}

		key := iter.Key()
		totalScanned++

		// Skip non-edge keys
		if !g.isEdgeKey(key) {
			continue
		}

		// Parse edge
		source, target, indexName, edgeType, err := storeutils.ParseEdgeKey(key)
		if err != nil {
			g.logger.Warn("Failed to parse edge key during backfill",
				zap.String("key", types.FormatKey(key)),
				zap.Error(err))
			continue
		}

		// Only process edges for this index
		if indexName != g.name {
			continue
		}

		// Add to reverse index batch
		value := slices.Clone(iter.Value())
		indexKey := g.makeEdgeIndexKey(target, source, edgeType)
		if err := batch.Set(indexKey, value, nil); err != nil {
			return fmt.Errorf("adding to batch: %w", err)
		}

		batchCount++
		totalIndexed++
		lastKey = slices.Clone(key)

		// Accumulate edge type counts locally, flush under lock at batch boundaries
		localEdgeCounts[edgeType]++

		if batchCount >= maxBatchSize {
			// Commit batch
			if err := batch.Commit(pebble.Sync); err != nil {
				return fmt.Errorf("committing batch: %w", err)
			}

			// Flush local edge counts to shared state
			g.edgeTypeCountsMu.Lock()
			for et, count := range localEdgeCounts {
				g.edgeTypeCounts[et] += count
			}
			g.edgeTypeCountsMu.Unlock()
			clear(localEdgeCounts)

			g.addBackfillItems(batchCount)
			g.updateBackfillProgress(estimateProgress(g.byteRange[0], g.byteRange[1], lastKey))

			g.logger.Debug("Backfill progress",
				zap.String("name", g.name),
				zap.Int("batchSize", batchCount),
				zap.Int("totalIndexed", totalIndexed),
				zap.Int("totalScanned", totalScanned))

			// Update rebuild state
			if err := g.rebuildState.Update(lastKey); err != nil {
				g.logger.Warn("Failed to update rebuild state",
					zap.String("indexPath", g.indexPath),
					zap.Error(err))
			}

			_ = batch.Close()
			batch = g.indexDB.NewBatch()
			batchCount = 0
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error during backfill: %w", err)
	}

	// Final batch
	if batchCount > 0 {
		if err := batch.Commit(pebble.Sync); err != nil {
			return fmt.Errorf("committing final batch: %w", err)
		}

		// Flush remaining local edge counts
		g.edgeTypeCountsMu.Lock()
		for et, count := range localEdgeCounts {
			g.edgeTypeCounts[et] += count
		}
		g.edgeTypeCountsMu.Unlock()
	}
	_ = batch.Close()

	g.addBackfillItems(batchCount)

	g.logger.Info("Completed backfill of graph reverse index",
		zap.String("name", g.name),
		zap.Int("totalIndexed", totalIndexed),
		zap.Int("totalScanned", totalScanned))

	return nil
}

// Stats returns index statistics
func (g *GraphIndexV0) Stats() IndexStats {
	g.edgeTypeCountsMu.RLock()
	var etCopy map[string]uint64
	var totalEdges uint64
	if len(g.edgeTypeCounts) > 0 {
		etCopy = make(map[string]uint64, len(g.edgeTypeCounts))
		for k, v := range g.edgeTypeCounts {
			etCopy[k] = v
			totalEdges += v
		}
	}
	g.edgeTypeCountsMu.RUnlock()

	is := GraphIndexStats{
		TotalEdges:             totalEdges,
		Rebuilding:             g.backfilling.Load(),
		BackfillItemsProcessed: g.backfillItemsProcessed.Load(),
		BackfillProgress:       g.loadBackfillProgress(),
	}
	if etCopy != nil {
		is.EdgeTypes = &etCopy
	}
	return is.AsIndexStats()
}

// UpdateRange updates the key range for this index
func (g *GraphIndexV0) UpdateRange(byteRange types.Range) error {
	g.byteRange = byteRange
	return nil
}

// UpdateSchema updates the table schema
func (g *GraphIndexV0) UpdateSchema(schema *schema.TableSchema) error {
	g.schema = schema
	return nil
}

// Helper methods

// isEdgeKey checks if a key is an edge key for this index
func (g *GraphIndexV0) isEdgeKey(key []byte) bool {
	// Edge keys have format: sourceKey:i:<indexName>:out:<edgeType>:<targetKey>:o
	return bytes.Contains(key, []byte(":i:"+g.name+":out:"))
}

// isValidEdgeType checks if an edge type is configured
func (g *GraphIndexV0) isValidEdgeType(edgeType string) bool {
	if g.conf.EdgeTypes == nil || len(*g.conf.EdgeTypes) == 0 {
		// If no edge types configured, allow all
		return true
	}

	for _, et := range *g.conf.EdgeTypes {
		if et.Name == edgeType {
			return true
		}
	}
	return false
}

// Edge index key format: <target>:i:<indexName>:in:<edgeType>:<source>:i
// This mirrors the outgoing edge key format for consistency
func (g *GraphIndexV0) makeEdgeIndexKey(target, source []byte, edgeType string) []byte {
	key := bytes.Clone(target)
	key = append(key, []byte(":i:")...)
	key = append(key, []byte(g.name)...)
	key = append(key, []byte(":in:")...)
	key = append(key, []byte(edgeType)...)
	key = append(key, ':')
	key = append(key, source...)
	key = append(key, []byte(":i")...)
	return key
}

// addToEdgeIndex adds an entry to the reverse index
func (g *GraphIndexV0) addToEdgeIndex(batch *pebble.Batch, target, source []byte, edgeType string, edgeValue []byte) error {
	indexKey := g.makeEdgeIndexKey(target, source, edgeType)
	return batch.Set(indexKey, edgeValue, nil)
}

// removeFromEdgeIndex removes an entry from the reverse index
func (g *GraphIndexV0) removeFromEdgeIndex(batch *pebble.Batch, target, source []byte, edgeType string) error {
	indexKey := g.makeEdgeIndexKey(target, source, edgeType)
	return batch.Delete(indexKey, nil)
}

// queryEdgeIndex queries the reverse index for incoming edges
func (g *GraphIndexV0) queryEdgeIndex(ctx context.Context, target []byte, edgeType string) ([]Edge, error) {
	// Construct prefix for target node
	// Format: <target>:i:<indexName>:in:<edgeType>:<source>:i
	prefix := bytes.Clone(target)
	prefix = append(prefix, []byte(":i:")...)
	prefix = append(prefix, []byte(g.name)...)
	prefix = append(prefix, []byte(":in:")...)
	if edgeType != "" {
		prefix = append(prefix, []byte(edgeType)...)
		prefix = append(prefix, ':')
	}

	// Read from indexDB (not main db)
	iter, err := g.indexDB.NewIterWithContext(ctx, &pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: utils.PrefixSuccessor(prefix),
	})
	if err != nil {
		return nil, fmt.Errorf("creating edge index iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	var edges []Edge

	for iter.First(); iter.Valid(); iter.Next() {
		// Parse the index key to extract source and edge type
		// Format: <target>:i:<indexName>:in:<edgeType>:<source>:i
		key := iter.Key()

		// Find :i: marker to locate where target ends
		indexMarker := []byte(":i:")
		_, after, ok := bytes.Cut(key, indexMarker)
		if !ok {
			continue
		}

		// Skip past target and ":i:<indexName>:in:"
		remaining := after

		// Parse: <indexName>:in:<edgeType>:<source>:i
		parts := bytes.SplitN(remaining, []byte{':'}, 4)
		if len(parts) < 4 {
			continue
		}

		// parts[0] = indexName, parts[1] = "in", parts[2] = edgeType, parts[3] = source:i
		parsedEdgeType := string(parts[2])

		// Remove :i suffix from source
		sourceWithSuffix := parts[3]
		if len(sourceWithSuffix) < 2 || !bytes.HasSuffix(sourceWithSuffix, []byte(":i")) {
			continue
		}
		source := sourceWithSuffix[:len(sourceWithSuffix)-2]

		// Decode edge value
		edge, err := DecodeEdgeValue(iter.Value())
		if err != nil {
			g.logger.Warn("Failed to decode edge value in index",
				zap.String("key", types.FormatKey(key)),
				zap.Error(err))
			continue
		}

		// Clone source to avoid sharing iterator's key buffer
		edge.Source = bytes.Clone(source)
		edge.Target = target
		edge.Type = parsedEdgeType

		edges = append(edges, *edge)
	}

	return edges, iter.Error()
}

// WaitForBackfill waits for the index backfill to complete.
// Returns immediately if no backfill is running or context is canceled.
func (g *GraphIndexV0) WaitForBackfill(ctx context.Context) {
	if g.backfillDone != nil {
		select {
		case <-g.backfillDone:
		case <-ctx.Done():
		}
	}
}

// GetIndexDB returns the separate Pebble database instance used for the reverse index.
// This is primarily for testing purposes.
func (g *GraphIndexV0) GetIndexDB() *pebble.DB {
	return g.indexDB
}

// Pause pauses the graph index (waits for in-flight Batch operations and backfill)
func (g *GraphIndexV0) Pause(ctx context.Context) error {
	g.pauseMu.Lock()

	// Check if already paused
	if g.paused.Load() {
		g.pauseMu.Unlock()
		return nil
	}

	// Set paused flag
	g.paused.Store(true)

	// Drain any stale acknowledgments from the channel before signaling
	select {
	case <-g.pauseAckCh:
	default:
	}

	// Release mutex BEFORE waiting for ack to avoid deadlock with backfill
	g.pauseMu.Unlock()

	// If backfill is running, wait for it to acknowledge the pause
	if g.backfillDone != nil {
		select {
		case <-g.backfillDone:
			// Backfill already finished, no ack needed
		default:
			// Backfill is running, wait for ack
			select {
			case <-g.pauseAckCh:
				// Backfill acknowledged pause
			case <-g.backfillDone:
				// Backfill finished while waiting
			case <-ctx.Done():
				g.paused.Store(false)
				return ctx.Err()
			}
		}
	}

	// Wait for any in-flight Batch() to complete. Batch() holds pauseMu for
	// the duration of its indexDB work, so acquiring it here guarantees no
	// Batch is mid-flight when we return.
	g.pauseMu.Lock()
	g.pauseMu.Unlock() //nolint:staticcheck // SA2001: intentional empty critical section to drain in-flight Batch()

	g.logger.Debug("Paused graph index", zap.String("name", g.name))
	return nil
}

// Resume resumes the graph index
func (g *GraphIndexV0) Resume() {
	g.pauseMu.Lock()
	defer g.pauseMu.Unlock()

	if !g.paused.Load() {
		return
	}

	g.paused.Store(false)
	g.pauseCond.Broadcast()

	g.logger.Debug("Resumed graph index", zap.String("name", g.name))
}
