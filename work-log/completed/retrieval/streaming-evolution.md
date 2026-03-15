# RAG Streaming Evolution

## Overview

This document chronicles the evolution of Antfly's RAG (Retrieval-Augmented Generation) streaming API through three major phases, each improving user experience and functionality. The progression moved from batch JSON responses to true token-by-token streaming with multi-table support and individual hit streaming.

## Phase 1: Event-Based Streaming (002)

**Status**: ✅ Completed
**Performance**: 78-85 streaming chunks vs 1-2 large chunks (50x improvement)

### Problem Statement

The initial streaming implementation attempted to stream structured JSON incrementally, but regex-based parsing only matched complete fields. This resulted in the entire summary being sent as one event rather than true token-by-token streaming.

### Solution: Event-Based Format

Replaced JSON streaming with a simple event-based format:
1. Stream summary text naturally (line by line)
2. Print separator token: `<<<CITATIONS>>>`
3. Print citations in format: `[doc_id <ID>] quote text`

### Breaking Changes

**Callback signature changed**:
```go
// Old signature
func(ctx context.Context, text string) error

// New signature
func(ctx context.Context, eventType string, data interface{}) error
```

### Event Types

- `summary`: Text chunk for the summary (data: string)
- `citation`: Complete citation object (data: Citation)
- `done`: Streaming complete
- `error`: Error occurred (data: string)

### Key Design Decisions

1. **Event-based over JSON**: Eliminates incomplete JSON parsing complexity
2. **Separator token**: `<<<CITATIONS>>>` unlikely to appear in summary text
3. **Token-by-token streaming**: Only buffer last 16 chars to avoid emitting partial separator
4. **Best-effort parsing**: Graceful degradation ensures users always get output

### Implementation

**State machine with minimal buffering**:
```go
type streamState int
const (
    stateSummarizing streamState = iota
    stateCiting
)

// Summary mode: Stream tokens immediately (only buffer 16 chars for separator detection)
// Citation mode: Buffer complete lines for parsing
```

### Performance Results

- **Before**: 1-2 large chunks (entire summary at once)
- **After**: 78-85 incremental chunks (true token-by-token streaming)
- **Improvement**: 50x better chunk granularity

## Phase 2: Multi-Table Support (003)

**Status**: ✅ Completed
**Tests**: 9/9 passing

### Problem Statement

Users needed to query multiple tables in a single RAG request for federated search across related data sources (images, products, documents).

### Solution: Queries Array

Replaced single `query` field with `queries[]` array, allowing:
- Single table: `queries: [{"table": "papers", ...}]`
- Broadcast: `queries: [{"table": "images", ...}, {"table": "products", ...}]`
- Different queries per table with unique document renderers

### Breaking Changes

**API schema change**:
```diff
-{
-  "query": {"table": "papers", "semantic_search": "..."},
-  "summarizer": {...}
-}
+{
+  "queries": [{"table": "papers", "semantic_search": "..."}],
+  "summarizer": {...}
+}
```

### Key Design Decisions

1. **Schema-level design**: Added `document_renderer` to `QueryRequest` instead of creating new `RetrievalRequest` type
2. **Partial failure handling**: Return results from successful tables even if others fail
3. **Heterogeneous results**: Preserve table-specific data for type-aware frontend rendering
4. **Per-query document renderers**: Different Go templates per table/query

### SSE Event Structure

```
event: table_result
data: {"table": "images", "hits": {...}, "status": 200}

event: table_result
data: {"table": "products", "hits": {...}, "status": 200}

event: summary
data: "Here are relevant images and products..."

event: citation
data: {"id": "img1", "quote": "..."}

event: done
data: {"complete": true}
```

### Implementation

```go
// Execute all queries (including failures)
queryResults := t.executeMultiTableQueries(ctx, ragReq)

// Collect successful hits for summarization
var allHits []QueryHit
for _, result := range queryResults {
    if result.Status == 200 {
        allHits = append(allHits, result.Hits.Hits...)
    }
}

// Stream table results then summary
if ragReq.WithStreaming {
    for _, result := range queryResults {
        streamEvent(w, "table_result", result)
    }
    // ... stream summary and citations
}
```

### Frontend Integration Example

TypeScript handling for heterogeneous results:
```typescript
if (event.event === "table_result") {
  const result = JSON.parse(event.data);

  if (result.status !== 200) {
    console.warn(`Table ${result.table} failed: ${result.error}`);
    return;
  }

  // Type-aware rendering
  if (result.table === "images") {
    renderImageGallery(result.hits.hits);
  } else if (result.table === "products") {
    renderProductCards(result.hits.hits);
  }
}
```

### Future Enhancements

- Cross-table deduplication
- Parallel query execution (currently sequential)
- Semantic query sharing (generate embeddings once, reuse across tables)

## Phase 3: Individual Hit Streaming (005)

**Status**: ✅ Completed
**Performance**: First results appear 95ms earlier (~47% faster time-to-first-content)

### Problem Statement

`table_result` events contained all hits as a single JSON blob. Dashboard couldn't render individual hits incrementally, and LLM didn't start until after all results were serialized.

### Solution: Stream Individual Hits with Parallel LLM

Execute LLM in background goroutine while streaming hits:
1. Execute all queries → merge with RRF
2. Start LLM in background (runs in parallel)
3. Stream individual `hit` events with table boundaries
4. Stream buffered LLM events (summary, citations)

### Event Structure

New event types:
- `hits_start`: Marks beginning of results for a table
- `hit`: Individual search result (uses QueryHit type)
- `hits_end`: Marks end of results with summary stats
- `summary`: LLM-generated text chunks (unchanged)
- `citation`: Source citations (unchanged)
- `done`: Completion signal (unchanged)

### Complete SSE Stream Example

```
event: hits_start
data: {"table":"papers","status":200}

event: hit
data: {"_id":"doc1","_score":0.95,"_source":{"title":"ML Paper"}}

event: hit
data: {"_id":"doc2","_score":0.92,"_source":{"title":"AI Research"}}

event: hits_end
data: {"table":"papers","total":25,"returned":2,"took":"5ms"}

event: summary
data: "The research demonstrates that"

event: summary
data: " machine learning is increasingly"

event: citation
data: {"id":"doc1","quote":"ML is transforming technology"}

event: done
data: {"complete":true}
```

### Implementation: Parallel LLM Execution

Using `errgroup` for coordinated goroutines:

```go
func (t *TableStore) streamRagResults(...) {
    flusher := w.(http.Flusher)

    // Buffered channel for LLM events
    llmEventChan := make(chan sseEvent, 100)

    eg, egCtx := errgroup.WithContext(r.Context())

    // Start LLM in background
    eg.Go(func() error {
        defer close(llmEventChan)

        llmStreamCallback := func(ctx context.Context, eventType string, data any) error {
            select {
            case llmEventChan <- sseEvent{Type: eventType, Data: data}:
                return nil
            case <-egCtx.Done():
                return egCtx.Err()
            }
        }

        opts := append(summarizeOpts, embeddings.WithStreaming(llmStreamCallback))
        _, err := summarizer.SummarizeWithResult(egCtx, contents, opts...)
        return err
    })

    // Stream individual hits while LLM runs
    for _, result := range queryResults {
        streamEvent(w, flusher, "hits_start", HitsStartEvent{...})

        if result.Status == 200 {
            for _, hit := range result.Hits.Hits {
                streamEvent(w, flusher, "hit", hit)
            }
        }

        streamEvent(w, flusher, "hits_end", HitsEndEvent{...})
    }

    // Stream buffered LLM events
    for event := range llmEventChan {
        streamEvent(w, flusher, event.Type, event.Data)
    }

    // Wait for LLM completion
    if err := eg.Wait(); err != nil {
        streamEvent(w, flusher, "error", map[string]string{"error": err.Error()})
        return
    }

    streamEvent(w, flusher, "done", map[string]bool{"complete": true})
}
```

### Performance Characteristics

**Timing Comparison**:
```
Old flow:
T+0ms:   Queries start
T+100ms: Queries complete
T+150ms: Serialize all results
T+200ms: Stream table_result events
T+250ms: LLM starts
T+5000ms: LLM completes

New flow:
T+0ms:   Queries start
T+100ms: Queries complete
T+101ms: LLM starts (parallel)
T+105ms: First hit in UI ← 95ms faster
T+120ms: All hits streamed
T+5000ms: Final summary appears
```

**Memory Profile**:
- Lower peak: Results discarded after streaming (not held until JSON serialization)
- Bounded LLM buffer: Max 100 events in channel

### TypeScript SDK Updates

```typescript
export interface RAGStreamCallbacks {
  onHitsStart?: (data: { table: string; status: number; error?: string }) => void;
  onHit?: (hit: QueryHit) => void;
  onHitsEnd?: (data: { table: string; total: number; returned: number; took: string }) => void;
  onSummary?: (chunk: string) => void;
  onCitation?: (citation: Citation) => void;
  onDone?: (data?: { complete: boolean }) => void;
  onError?: (error: string) => void;
}
```

### Benefits

- ✅ Immediate feedback: First hit appears as soon as queries complete
- ✅ Parallel processing: LLM runs while hits stream (no idle time)
- ✅ Better UX: Progressive rendering
- ✅ Lower memory: Results streamed and discarded
- ✅ Error isolation: Query failures don't block LLM
- ✅ Table grouping: Clear boundaries for multi-table queries

## Current State Summary

The RAG streaming API now provides:

1. **True token-by-token streaming** (Phase 1)
   - 50x improvement in chunk granularity
   - Event-based format eliminates JSON parsing complexity

2. **Multi-table support** (Phase 2)
   - Federated search across related tables
   - Partial failure handling
   - Per-query document renderers

3. **Individual hit streaming** (Phase 3)
   - Parallel LLM execution
   - 47% faster time-to-first-content
   - Progressive rendering for better UX

### Complete Event Catalog

| Event | Phase | Purpose | Data Type |
|-------|-------|---------|-----------|
| `hits_start` | 3 | Mark beginning of table results | `{table, status, error?}` |
| `hit` | 3 | Individual search result | `QueryHit` |
| `hits_end` | 3 | Mark end of table results | `{table, total, returned, took}` |
| `summary` | 1 | LLM-generated text chunks | `string` |
| `citation` | 1 | Source citations | `{id, quote}` |
| `done` | 1 | Streaming complete | `{complete: true}` |
| `error` | 1 | Error occurred | `{error: string}` |
| `table_result` | 2 | Complete table results (deprecated in Phase 3) | `QueryResult` |

### Migration Guide

**From Phase 1 to Phase 2** (Single table → Multi-table):
```diff
-{
-  "query": {"table": "papers", "semantic_search": "machine learning"},
-  "with_streaming": true
-}
+{
+  "queries": [{"table": "papers", "semantic_search": "machine learning"}],
+  "with_streaming": true
+}
```

**From Phase 2 to Phase 3** (table_result → individual hits):
```typescript
// Old: Handle complete table_result
eventSource.addEventListener('table_result', (e) => {
  const result = JSON.parse(e.data);
  renderAllHits(result.hits.hits); // All at once
});

// New: Handle individual hits progressively
eventSource.addEventListener('hits_start', (e) => {
  const {table, status} = JSON.parse(e.data);
  initializeTableSection(table);
});

eventSource.addEventListener('hit', (e) => {
  const hit = JSON.parse(e.data);
  renderHitIncrementally(hit); // One at a time
});

eventSource.addEventListener('hits_end', (e) => {
  const {table, total, returned} = JSON.parse(e.data);
  finalizeTableSection(table, total, returned);
});
```

## References

- Implementation: `src/metadata/api.go` (handleRagQuery, streamRagResults)
- Event parsing: `lib/embeddings/genkit.go` (SummarizeWithResult streaming state machine)
- TypeScript SDK: `ts/packages/sdk/src/client.ts` (RAG streaming callbacks)
- OpenAPI spec: `src/metadata/api.yaml` (RAGRequest, RAGResult schemas)
