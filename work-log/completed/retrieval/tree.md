# Plan: Add Topology, Field-Based Edges, and Summarizer to Graph Index

## Summary

Extend `GraphIndexV0` with:
1. **Field** on `EdgeTypeConfig` - automatically extract edges from document fields
2. **Topology** on `EdgeTypeConfig` (`tree` | `graph`) - per-edge-type structural constraints
3. **Summarizer** + **Template** on `GraphIndexV0Config` - generate summaries for graph nodes (enables PageIndex-style tree navigation)

This follows the same enrichment patterns as the embedding index (`LeaderFactory` + `PersistFunc` through Raft).

### Example Config

```yaml
GraphIndexV0Config:
  summarizer:                        # GeneratorConfig - at index level (applies to nodes)
    provider: "openai"
    model: "gpt-4o-mini"
  template: "{{title}}\n{{content}}" # handlebars input for summarizer (same as aknn pattern)
  edge_types:
    - name: "child_of"
      field: "parent_id"             # auto-extract from document field
      topology: "tree"               # single parent, no cycles
    - name: "related_to"
      field: "related_ids"           # array field → multiple edges
      # topology defaults to "graph"
    - name: "cites"
      # no field → uses _edges explicitly (existing behavior)
```

---

## Files to Modify

### 1. OpenAPI Spec: `src/store/db/indexes/openapi.yaml`

**a. Add to `EdgeTypeConfig` (line 227):**

```yaml
EdgeTypeConfig:
  type: object
  properties:
    name:
      type: string
      description: "Edge type name (e.g., 'cites', 'child_of')"
    field:                                    # NEW
      x-go-type-skip-optional-pointer: true
      type: string
      description: |
        Document field containing target node key(s) for automatic edge creation.
        Supports string (single target) or array of strings (multiple targets).
        When omitted, edges must be provided explicitly via _edges.
    topology:                                 # NEW
      x-go-type-skip-optional-pointer: true
      type: string
      enum:
        - tree
        - graph
      default: graph
      description: |
        Topology constraint for this edge type:
        - tree: Single parent per node, no cycles
        - graph: No constraints (default)
    # ... existing fields (max_weight, min_weight, allow_self_loops, required_metadata)
```

**b. Add to `GraphIndexV0Config` (line 214):**

```yaml
GraphIndexV0Config:
  type: object
  properties:
    summarizer:                               # NEW
      description: "Configuration for generating node summaries (enables tree navigation in AnswerAgent)"
      $ref: "../../../lib/ai/openapi.yaml#/components/schemas/GeneratorConfig"
    template:                                 # NEW (matches aknn naming)
      x-go-type-skip-optional-pointer: true
      type: string
      description: |
        Handlebars template for generating summarizer input text.
        Uses document fields as template variables.
        Same pattern as EmbeddingIndexConfig.template.
      example: "{{title}}\n{{content}}"
    edge_types:                               # existing
      ...
    max_edges_per_document:                   # existing
      ...
```

**c. Run `make generate`** to regenerate `openapi.gen.go`.

### 2. Config Equality: `src/store/db/indexes/openapi.go`

Update `GraphIndexV0Config.Equal()` (line 145) to include new fields:

```go
func (a GraphIndexV0Config) Equal(b GraphIndexV0Config) bool {
    return a.MaxEdgesPerDocument == b.MaxEdgesPerDocument &&
        a.Template == b.Template &&
        reflect.DeepEqual(a.EdgeTypes, b.EdgeTypes) &&
        reflect.DeepEqual(a.Summarizer, b.Summarizer)
}
```

### 3. Graph Index Implementation: `src/store/db/indexes/graph_v0.go`

**a. Implement `EnrichableIndex` interface.**

Currently (line 25):
```go
var _ Index = (*GraphIndexV0)(nil)
```

Add:
```go
var _ EnrichableIndex = (*GraphIndexV0)(nil)
```

**b. Add `NeedsEnricher()` method:**

```go
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
```

**c. Add `LeaderFactory()` method** (following `aknn_v0.go:1078-1218` pattern):

`StartLeaderFactory()` in `indexmgr.go` calls `LeaderFactory` on ALL `EnrichableIndex` implementations at startup, regardless of `NeedsEnricher()`. So we must handle the no-op case.

```go
func (g *GraphIndexV0) LeaderFactory(ctx context.Context, persistFunc PersistFunc) error {
    if !g.NeedsEnricher() {
        // No enrichment configured - just block until cancelled
        <-ctx.Done()
        return nil
    }

    // Create enricher with WAL-based queue (same pattern as aknn)
    enricher := newGraphEnricher(g, persistFunc)
    g.enricherMu.Lock()
    g.enricher = enricher
    g.enricherMu.Unlock()

    // Run enricher (blocks - processes queue continuously)
    enricher.Run(ctx)

    // Cleanup on context cancellation
    g.enricherMu.Lock()
    g.enricher = nil
    g.enricherMu.Unlock()

    return nil
}
```

**d. Graph Enricher - continuous processing:**

The enricher must run continuously (not one-shot) to handle documents written after initial backfill. Following the aknn pattern:

```go
type graphEnricher struct {
    graph       *GraphIndexV0
    persistFunc PersistFunc
    walBuf      *inflight.WALBuffer  // queue for new document writes
}

func (e *graphEnricher) Run(ctx context.Context) {
    // Phase 1: Backfill - scan all existing documents
    e.backfillFieldEdges(ctx)
    e.backfillSummaries(ctx)

    // Phase 2: Continuous - process new writes from WAL queue
    for {
        select {
        case <-ctx.Done():
            return
        case <-e.walBuf.Signal():
            batch := e.walBuf.Drain()
            e.processFieldEdges(ctx, batch)
            e.processSummaries(ctx, batch)
        }
    }
}
```

Hook into `Batch()` to enqueue base document writes:

```go
func (g *GraphIndexV0) Batch(ctx context.Context, writes [][2][]byte, ...) error {
    // Existing: process edge keys → reverse index
    // ...

    // NEW: if enricher exists, enqueue base document writes for field edge extraction
    if g.enricher != nil {
        for _, kv := range writes {
            if isBaseDocKey(kv[0]) {
                g.enricher.Enqueue(kv[0])
            }
        }
    }

    return nil
}
```

**e. Edge Reconciliation with HashID:**

When processing a document for field-based edges, use hash-based change detection + full replace:

```go
func (e *graphEnricher) reconcileFieldEdges(ctx context.Context, docKey []byte, doc map[string]any) error {
    for _, et := range e.graph.fieldEdgeTypes() {
        // 1. Read field value, compute hash
        fieldValue := doc[et.Field]
        desiredTargets := toStringSlice(fieldValue) // handles string or []string
        fieldHash := xxhash.Sum64([]byte(fmt.Sprint(desiredTargets)))

        // 2. Check stored hash - skip if unchanged
        hashKey := makeFieldHashKey(docKey, e.graph.name, et.Name) // doc:i:<graph>:<edgeType>:fh
        storedHash, err := e.graph.db.Get(hashKey)
        if err == nil && decodeHash(storedHash) == fieldHash {
            continue // field unchanged, skip
        }

        // 3. Field changed: delete ALL existing edges for this doc+edgeType
        var writes [][2][]byte
        var deletes [][]byte

        prefix := makeEdgePrefix(docKey, e.graph.name, et.Name) // doc:i:<graph>:out:<edgeType>:
        iter := e.graph.db.NewIter(&pebble.IterOptions{
            LowerBound: prefix,
            UpperBound: prefixEnd(prefix),
        })
        for iter.First(); iter.Valid(); iter.Next() {
            deletes = append(deletes, slices.Clone(iter.Key()))
        }
        iter.Close()

        // 4. Create new edges from current field value
        for _, target := range desiredTargets {
            edgeKey := storeutils.MakeEdgeKey(docKey, []byte(target), e.graph.name, et.Name)
            edgeVal, _ := EncodeEdgeValue(&Edge{
                Source: docKey, Target: []byte(target),
                Type: et.Name, Weight: 1.0,
                CreatedAt: time.Now(), UpdatedAt: time.Now(),
            })
            writes = append(writes, [2][]byte{edgeKey, edgeVal})
        }

        // 5. Persist hash marker
        hashVal := encodeHash(fieldHash)
        writes = append(writes, [2][]byte{hashKey, hashVal})

        // 6. Persist all through Raft (deletes + writes in one batch)
        if err := e.persistFunc(ctx, writes); err != nil {
            return err
        }
        // Deletes go through Raft too
        if len(deletes) > 0 {
            if err := e.persistDeletes(ctx, deletes); err != nil {
                return err
            }
        }
    }
    return nil
}
```

**Hash key format:** `<docKey>:i:<graphName>:<edgeType>:fh` (field hash)
**Hash value format:** `[uint64]` (xxhash of field value)

**f. Summary Reconciliation with HashID:**

Same pattern as aknn embeddings - hash the template input, skip if unchanged:

```go
func (e *graphEnricher) reconcileSummary(ctx context.Context, docKey []byte, doc map[string]any) error {
    // 1. Render template input
    input, err := template.Render(e.graph.conf.Template, doc)
    if err != nil {
        return err
    }
    inputHash := xxhash.Sum64String(input)

    // 2. Check existing summary hash
    summaryKey := storeutils.MakeSummaryKey(docKey, e.graph.name)
    existing, err := e.graph.db.Get(summaryKey)
    if err == nil {
        storedHash := encoding.DecodeUint64Ascending(existing[:8])
        if storedHash == inputHash {
            return nil // unchanged, skip
        }
    }

    // 3. Generate new summary
    summary, err := e.summarizer.Summarize(ctx, input)
    if err != nil {
        return err
    }

    // 4. Persist: [hashID:uint64][summary_text]
    val := make([]byte, 0, 8+len(summary))
    val = encoding.EncodeUint64Ascending(val, inputHash)
    val = append(val, summary...)

    return e.persistFunc(ctx, [][2][]byte{{summaryKey, val}})
}
```

**g. Add topology validation in `Batch()` (line 119):**

For edge types with `topology: "tree"`:
- On edge write, check reverse index: if target already has an incoming edge of this type from a *different* source, reject with error
- Log warning for topology violations

**h. Add helper methods:**

```go
// IsNavigable returns true if this graph has a tree edge type and a summarizer
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

// GetTreeEdgeType returns the first edge type with tree topology, or empty string
func (g *GraphIndexV0) GetTreeEdgeType() string { ... }

// GetRoots returns all nodes with no incoming tree edges
func (g *GraphIndexV0) GetRoots(ctx context.Context) ([][]byte, error) { ... }

// fieldEdgeTypes returns edge types that have a field configured
func (g *GraphIndexV0) fieldEdgeTypes() []EdgeTypeConfig { ... }
```

### 4. No changes to `src/store/db/db.go` or `src/store/db/helpers.go`

Field-based edge extraction happens through the enricher (`LeaderFactory`), not during document writes. The existing `_edges` extraction path remains unchanged.

### 5. Index Manager: `src/store/db/indexmgr.go`

No changes needed. The existing `StartLeaderFactory()` (line 351) calls `LeaderFactory` on all `EnrichableIndex` implementations. The `Register()` method (line 656) uses `NeedsEnricher()` for dynamically registered indexes. Both paths will work with the new `GraphIndexV0` implementation.

### 6. AnswerAgent: `src/metadata/api_ai.go` (follow-up PR)

Future work - extend AnswerAgent to detect `IsNavigable()` graph indexes and use tree navigation with summaries.

---

## Edge Reconciliation Strategy

**Problem:** When a document's field changes (e.g., `parent_id: "B"` → `parent_id: "C"`), old edges must be cleaned up.

**Solution:** Hash-based change detection + full replace (matches `_edges` reconciliation pattern):

```
For each document + field-based edge type:
  1. Read field value → compute xxhash
  2. Check stored hash at doc:i:<graph>:<edgeType>:fh
  3. If hash matches → skip (no change, most common case)
  4. If hash differs:
     a. Delete ALL existing edges for this doc+edgeType (prefix scan)
     b. Create new edges from current field value
     c. Update stored hash
     d. Persist all through Raft
```

**Why this is efficient:**
- Step 3 short-circuits for unchanged documents (single Pebble read)
- Only documents with actual field changes trigger writes
- Full replace is simple and correct (no diff logic needed)
- Hash marker persisted through Raft (consistent across replicas)

**Consistency note:** Between document write and enricher processing, edges may be stale. This is the same eventual consistency model as aknn (embedding may not exist yet). For tree topology, a node may temporarily have an old parent.

---

## Implementation Order

### Phase 1: OpenAPI + Code Generation
1. Add `field`, `topology` to `EdgeTypeConfig` in `openapi.yaml`
2. Add `summarizer`, `template` to `GraphIndexV0Config` in `openapi.yaml`
3. Run `make generate` to regenerate `openapi.gen.go`
4. Update `GraphIndexV0Config.Equal()` in `openapi.go` for new fields

### Phase 2: EnrichableIndex on GraphIndexV0
5. Add `EnrichableIndex` interface assertion
6. Implement `NeedsEnricher()` (returns false when no field/summarizer → safe no-op in LeaderFactory)
7. Implement `LeaderFactory()` with no-op guard and continuous enricher

### Phase 3: Field-Based Edge Enrichment + Reconciliation
8. Implement `graphEnricher` with WAL-based queue (matches aknn pattern)
9. Implement `reconcileFieldEdges()` with hash-based change detection + full replace
10. Hook `Batch()` to enqueue base document writes to enricher
11. Add tests for field-based edge extraction and reconciliation

### Phase 4: Topology Validation
12. Add topology validation in `Batch()` - tree rejects multi-parent
13. Add `IsNavigable()`, `GetTreeEdgeType()`, `GetRoots()`
14. Add tests for topology constraints

### Phase 5: Summarizer Enrichment
15. Implement `reconcileSummary()` with hash-based change detection
16. Add handlebars template rendering (reuse `lib/template.Render()`)
17. Add tests for summary generation

### Phase 6: AnswerAgent Integration (follow-up PR)
18. Detect navigable graph in AnswerAgent
19. Implement tree navigation logic
20. Add e2e tests

---

## Key Patterns to Reuse

| Pattern | Source File | Line | Reuse For |
|---------|------------|------|-----------|
| `LeaderFactory` lifecycle | `aknn_v0.go` | 1078-1218 | Graph enricher lifecycle |
| `PersistFunc` type | `indexes.go` | 79 | Edge + summary persistence through Raft |
| `PersistSummariesFunc` | `summarizeenricher.go` | 200 | Summary value encoding |
| Summary key format | `storeutils.MakeSummaryKey()` | storeutils.go:156 | Graph node summaries |
| HashID idempotency | `aknn_v0.go` | 1087-1103 | Skip already-enriched docs |
| Document scanning | `storeutils.Scan()` | storeutils.go | Find docs needing enrichment |
| `RebuildState` | `rebuild_state.go` | 10-81 | Resume interrupted enrichment |
| Handlebars rendering | `lib/template.Render()` | template.go:194 | Summarizer template input |
| `GeneratorConfig` | `lib/ai/openapi.yaml` | - | Summarizer model config |
| Edge encoding | `traversal.go` | 55-107 | `EncodeEdgeValue`/`DecodeEdgeValue` |
| Edge key format | `storeutils.MakeEdgeKey()` | storeutils/edge.go | Field-extracted edge keys |
| Reverse index pattern | `graph_v0.go` | 512-521 | `addToEdgeIndex`/`removeFromEdgeIndex` |
| WAL buffer | `lib/inflight` | - | Queue document writes for enricher |
| `_edges` reconciliation | `db.go` | 1828-1889 | Pattern for full-replace reconciliation |
| `GraphIndexV0Config.Equal()` | `openapi.go` | 145-148 | Must update for new fields |

---

## Testing Strategy

### Unit Tests (`src/store/db/indexes/graph_v0_test.go`)

- **Topology validation**: tree rejects multi-parent on same edge type, graph allows it
- **Field extraction**: reads string field → single edge, array field → multiple edges
- **Edge reconciliation**: field change triggers delete old + create new edges
- **Hash idempotency**: re-enriching unchanged documents is a no-op
- **Summary enrichment**: generates summaries, stores with hashID, skips existing
- **Summary reconciliation**: document content change triggers re-summarization
- **`NeedsEnricher()`**: true when field or summarizer configured, false otherwise
- **`IsNavigable()`**: true only when tree edge type + summarizer
- **Mixed edge types**: tree + graph edge types in same index
- **LeaderFactory no-op**: when no enrichment configured, blocks safely

### E2E Tests (`e2e/graph_tree_test.go`)

- Create table with tree topology + field + summarizer configured
- Write documents with `parent_id` field
- Verify edges created through Raft (forward + reverse)
- Update document's `parent_id` → verify old edge deleted, new edge created
- Verify tree topology enforced (reject multi-parent)
- Verify summaries generated for all nodes
- Update document content → verify summary regenerated
- Query: get roots, get children, get ancestors
- Multi-shard: verify cross-shard edges and summaries

```bash
# Unit tests
GOEXPERIMENT=simd go test ./src/store/db/indexes/ -run TestGraphV0

# E2E tests
make e2e E2E_TEST=TestGraphTree
```
