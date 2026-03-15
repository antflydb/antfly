# Plan: Agent Architecture Refactor — Retrieval and Generation

## Summary

Refactor the agent architecture into two composable primitives:

1. **`/agents/retrieval`** — DFA-based retrieval (clarify → select_strategy → refine_query → execute)
2. **`/agents/generation`** — Generate answers from documents

Both support optional multi-turn for user feedback (clarification, tool selection). Higher-level patterns (RAG, deep research) are composed at the searchaf layer.

The existing PageIndex-style tree search becomes one strategy inside the retrieval DFA's `execute` step.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     /agents/retrieval                        │
│                                                              │
│  DFA: clarify → select_strategy → refine_query → execute     │
│                                                              │
│  • Find documents using available tools                      │
│  • Tools: semantic, graph, tree, metadata, web, a2a          │
│  • Returns: documents + reasoning_chain                      │
│  • Optional multi-turn for clarification/tool selection      │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│                     /agents/generation                       │
│                                                              │
│  Generate answers from documents                             │
│                                                              │
│  • Takes: query + documents + context                        │
│  • Returns: answer + citations                               │
│  • Handles: prompt construction, streaming, citations        │
│  • Optional multi-turn for follow-up questions               │
└──────────────────────────────────────────────────────────────┘

Composition at searchaf layer:
┌─────────────────────────────────────────────────────────────┐
│  RAG         = retrieval → generation                       │
│  Deep search = retrieval (tree, 10 iter) → generation       │
│  Just search = retrieval only                               │
│  Summarize   = generation only (user provides docs)         │
└─────────────────────────────────────────────────────────────┘
```

| Agent | Purpose | Multi-turn |
|-------|---------|------------|
| `/agents/retrieval` | DFA-based document retrieval | Optional (clarify, tool selection) |
| `/agents/generation` | Generate answers from documents | Optional (follow-up) |

---

## Retrieval DFA

The core state machine:

```
┌─────────┐     ┌─────────────────┐     ┌──────────────┐     ┌─────────┐
│ CLARIFY │ ──▶ │ SELECT_STRATEGY │ ──▶ │ REFINE_QUERY │ ──▶ │ EXECUTE │
└─────────┘     └─────────────────┘     └──────────────┘     └────┬────┘
     ▲                                                            │
     │                                                            │
     └──────────────────── (if unclear) ──────────────────────────┘
```

### States

**CLARIFY** (optional, multi-turn):
- Disambiguate query if needed
- "Did you mean OAuth1 or OAuth2?"
- Can loop back from EXECUTE if results insufficient

**SELECT_STRATEGY**:
- Analyze query + available index capabilities
- Choose: semantic, bm25, tree, graph, metadata, or hybrid
- Based on query classification (exploratory vs specific vs relational vs structured)

**REFINE_QUERY**:
- Transform query for chosen strategy
- Semantic: embed query
- Metadata: query_builder_bleve.go → Bleve query
- Tree/graph: prepare start nodes

**EXECUTE**:
- Run the chosen strategy
- Strategies may iterate internally (tree search: navigate → sufficiency → backtrack)
- Returns documents + reasoning_chain

### Strategies (inside EXECUTE)

| Strategy | Internal Behavior | Index Type |
|----------|-------------------|------------|
| semantic | Single step: embed → cosine similarity | aknn_v0 |
| bm25 | Single step: tokenize → BM25 score | full_text_v0 |
| metadata | Single step: query_builder → filter | any (fields) |
| tree | Iterative: navigate → sufficiency → backtrack | graph_v0 (navigable) |
| graph | Iterative: traverse relationships | graph_v0 |
| hybrid | Combine multiple, RRF or rerank | multiple |

### Tree Search (PageIndex-style, inside EXECUTE)

Tree search always starts with a query to get starting nodes, then navigates from there.

**In the request**, tree search is specified as a query in the pipeline:

```yaml
"queries": [
  {
    "name": "find_chapters",
    "semantic_search": { "index": "embeddings", "query": "..." }
  },
  {
    "name": "navigate_tree",
    "tree_search": {
      "index": "doc_hierarchy",
      "start_nodes": "$find_chapters",  # reference prior query
      "max_depth": 5,
      "beam_width": 3
    }
  }
]
```

**start_nodes options** (always a query reference):
- `"$query_name"` — Results from a prior named query in the pipeline
- `"$roots"` — Shorthand: query graph for root nodes (nodes with no parents)

```
tree_search(query, index, start_nodes_ref):

  # Step 1: Resolve start nodes from reference
  IF start_nodes_ref == "roots":
    start_nodes = query_roots(index)
  ELIF start_nodes_ref.startswith("$"):
    start_nodes = resolve_query_results(start_nodes_ref)

  # Step 2: Navigate from start nodes
  collected = []
  stack = [start_nodes]
  backtrack_candidates = {}

  WHILE stack not empty AND not sufficient:
    nodes = stack.pop()
    summaries = lookup(nodes, fields=_summaries.{index})

    selected = LLM_select_branches(query, summaries, collected)
    backtrack_candidates[parent] = unselected

    FOR node in selected:
      IF leaf OR is_answer: collected.append(node)
      ELSE: stack.push(children(node))

    IF sufficient(collected, query): break
    IF stack empty: stack.push(backtrack_candidates.pop())

  RETURN collected + reasoning_chain
```

Uses existing APIs:
- Summaries: `GET /tables/{table}/lookup/{key}?fields=_summaries.{index}`
- Children: `GET /graph/edges?index={index}&key={key}&direction=out`
- Roots: `GET /graph/roots?index={index}`

---

## API Design

### /agents/retrieval

```yaml
POST /agents/retrieval
{
  "query": "How do I configure OAuth?",
  "table": "docs",

  "queries": [                          # pipeline of queries
    {
      "name": "semantic",
      "semantic_search": { "index": "doc_embeddings" }
    },
    {
      "name": "tree",
      "tree_search": {
        "index": "doc_hierarchy",
        "start_nodes": "$semantic",     # reference prior query
        "max_depth": 5,
        "beam_width": 3
      }
    }
  ],

  "context": [...],                     # optional, prior messages for multi-turn
  "max_iterations": 5,                  # max DFA loops
  "stream": true,                       # SSE streaming vs JSON

  "generator": GeneratorConfig,         # default generator for DFA steps
  "chain": [ChainLink, ...],            # alternative: chain config

  "steps": {
    "clarify": { "enabled": true },
    "select_strategy": { "enabled": true, "generator": ... },
    "refine_query": { "generator": ... }
  }
}

# Response
{
  "documents": [...],
  "reasoning_chain": [...],
  "strategy_used": "tree",
  "state": "complete" | "awaiting_clarification",
  "clarification_request": { ... }      # if awaiting
}
```

### /agents/generation

```yaml
POST /agents/generation
{
  "query": "How do I configure OAuth?",
  "documents": [...],                   # documents to generate from

  "context": [...],                     # optional, prior messages for multi-turn
  "stream": true,                       # SSE streaming vs JSON

  "generator": GeneratorConfig,
  "chain": [ChainLink, ...],

  "system_prompt": "...",               # optional custom system prompt
  "citation_style": "inline" | "footnote" | "none"
}

# Response
{
  "answer": "To configure OAuth, you need to...",
  "citations": [
    { "document_id": "doc1", "text": "...", "score": 0.95 }
  ],
  "state": "complete" | "awaiting_clarification"
}
```

### Composition Examples (searchaf layer)

```yaml
# RAG = retrieval → generation
1. POST /agents/retrieval { query, queries, ... }
   → { documents, reasoning_chain }
2. POST /agents/generation { query, documents, ... }
   → { answer, citations }

# Deep research = retrieval with tree search + high iterations
POST /agents/retrieval {
  query,
  queries: [semantic, tree_search],
  max_iterations: 10,
  steps: { clarify: { enabled: true }, select_strategy: { enabled: true } }
}

# Fast search = retrieval with minimal DFA
POST /agents/retrieval {
  query,
  queries: [semantic],
  max_iterations: 1,
  steps: { clarify: { enabled: false }, select_strategy: { enabled: false } }
}

# Summarize = generation only (user provides docs)
POST /agents/generation { query: "Summarize these", documents: [...] }
```

---

## Files to Modify/Create

### New Files

| File | Purpose |
|------|---------|
| `src/metadata/retrieval_agent.go` | Retrieval DFA implementation |
| `src/metadata/generation_agent.go` | Generation agent implementation |
| `lib/ai/retrieval_prompts.go` | Prompts for DFA steps (clarify, strategy, etc.) |

### Modify

| File | Changes |
|------|---------|
| `src/metadata/api.yaml` | Add `/agents/retrieval`, `/agents/generation` endpoints |
| `src/metadata/api_ai.go` | Remove old `/agents/answer`, wire new endpoints |
| `src/metadata/metadata.go` | Add cross-shard helpers for tree search |
| `e2e/docsaf_test.go` | Replace answer/chat tests with retrieval/generation tests |
| `e2e/test_queries.json` | Update query format for new retrieval API |
| `e2e/chat_test_queries.json` | Update for retrieval multi-turn format |

### Reuse

| File | What to Reuse |
|------|---------------|
| `lib/ai/chain.go` | ExecuteChain, ResolveGeneratorOrChain |
| `lib/ai/query_builder_bleve.go` | Metadata query building |
| `lib/ai/chat.go:549-630` | FilterSpecToQuery |
| `lib/ai/prompts.go` | Existing classification prompts |
| `src/metadata/api.go:852-976` | Lookup with `?fields=_summaries` |
| `src/store/api.go:1300-1374` | Graph edges API |

---

## Implementation Order

### Phase 1: Retrieval DFA Core
1. OpenAPI schema for RetrievalAgentRequest/Response
2. `retrieval_agent.go` with DFA state machine
3. CLARIFY state (optional, multi-turn support)
4. SELECT_STRATEGY state (index capability discovery + classification)
5. REFINE_QUERY state (query_builder_bleve for metadata, embed for semantic)
6. EXECUTE state with strategy dispatch

### Phase 2: Retrieval Strategies
7. Semantic strategy (wrap existing)
8. BM25 strategy (wrap existing)
9. Metadata strategy (use query_builder_bleve)
10. Tree strategy (PageIndex-style with sufficiency/backtrack)
11. Graph strategy (extend existing traversal)
12. Hybrid strategy (combine + rerank)

### Phase 3: Generation Agent
13. OpenAPI schema for GenerationAgentRequest/Response
14. `generation_agent.go` — document-to-answer with citations
15. Wire up `/agents/generation` endpoint
16. Citation extraction logic

### Phase 4: Multi-turn Clarification
17. Clarification flow (pause DFA, return `awaiting_clarification` state)
18. Resume DFA on user response (client sends context with prior messages)
19. Integrate with chain/generator for clarification prompts

### Phase 5: E2E Tests & Cleanup
20. Update `e2e/docsaf_test.go` — Replace answer/chat tests with retrieval/generation tests
21. Update test query JSON files for new API format
22. Add `TestE2E_RetrievalAgent`, `TestE2E_GenerationAgent`, `TestE2E_RAGComposition` tests
23. Remove old `/agents/answer` and `/agents/chat` endpoints
24. Update SDK generation (`make generate`)

---

## Testing Strategy

**Unit tests** (`src/metadata/`):
- DFA state transitions
- Strategy selection logic
- Tree search with mock LLM
- Query builder integration
- Citation extraction

**Integration tests** (`src/metadata/`):
- End-to-end retrieval agent
- Generation agent with real documents
- Retrieval → generation composition

**E2E tests** (`e2e/`):

Files to update:
- `e2e/docsaf_test.go` — Replace answer/chat agent tests with retrieval/generation tests
- `e2e/test_queries.json` — Update query format for new API
- `e2e/chat_test_queries.json` — Update for retrieval multi-turn format

Existing patterns to reuse:
- `setupEvalTest()` (line 808) — Memory monitoring, swarm setup
- `setupTestTable()` (line 743) — Table creation with backup/restore
- `indexAntflyDocs()` (line 100) — Index Antfly docs for testing
- `aggregateEvalResults()` (line 443) — Score aggregation
- `newTestQueryConfig()` (line 667) — Generator/reranker config
- `SaveEvaluationReport()` (line 548) — Markdown report generation

New E2E tests to add:
1. `TestE2E_RetrievalAgent` — Test DFA with semantic, tree, hybrid strategies
2. `TestE2E_GenerationAgent` — Test document-to-answer with citations
3. `TestE2E_RAGComposition` — Test retrieval → generation flow
4. `TestE2E_RetrievalClarification` — Test multi-turn clarification

```bash
# Unit tests
GOEXPERIMENT=simd go test ./src/metadata/ -run TestRetrievalAgent
GOEXPERIMENT=simd go test ./src/metadata/ -run TestGenerationAgent

# E2E tests
make e2e E2E_TEST=TestE2E_RetrievalAgent
make e2e E2E_TEST=TestE2E_GenerationAgent
make e2e E2E_TEST=TestE2E_RAGComposition
```

---

## Migration / Backward Compatibility

**Decision: Breaking API changes allowed.**

The existing `/agents/answer` and `/agents/chat` will be replaced with two primitives:
- `/agents/retrieval` — New, DFA-based document retrieval
- `/agents/generation` — New, document-to-answer generation

RAG and other patterns composed at searchaf layer.

**Multi-turn state: Stateless.**
- Client sends `messages[]` or `context[]` each request
- No server-side conversation storage needed
- Follows current `/agents/chat` pattern

---

## Gaps Identified

### 1. Multi-turn State: Stateless

**Decision**: Client sends `context[]` each request. No server-side storage.

### 2. Clarification Edge Case

**What if clarify is disabled but query is ambiguous?**

**Decision**: Proceed but document ambiguity in `reasoning_chain`.

### 3. Index Capability Discovery

**Current state**: No runtime capability discovery API. Index types known at schema definition time.

**Implementation approach:**
```go
// Extend Index interface
type IndexCapabilities struct {
    Type        string   // "aknn_v0", "graph_v0", "full_text_v0"
    IsNavigable bool     // graph with tree topology + summarizer
    HasEmbedder bool     // can embed queries
    EdgeTypes   []string // for graph traversal
}

func (idx *Index) Capabilities() IndexCapabilities
```

Read from existing config - no new storage needed.

### 4. Graph Coordination in EXECUTE

**How tree search uses existing APIs:**
```
getRoots():
  → broadcast GET /graph/roots to all shards
  → merge results

getNodeSummaries(keys):
  → batch GET /tables/{t}/lookup/{k}?fields=_summaries to shards
  → merge results

getChildren(parentKeys):
  → batch GET /graph/edges?direction=out to shards
  → merge results
```

Uses existing `graph_query_coordinator.go` patterns for cross-shard.

---

## SSE Streaming for DFA

Extend existing event types:

```go
// DFA state transitions
streamEvent(w, "dfa_state", map[string]any{
    "from": "select_strategy",
    "to": "refine_query",
    "strategy": "tree",
})

// Tree search progress (inside EXECUTE)
streamEvent(w, "tree_level", map[string]any{
    "depth": 1,
    "selected": [...],
    "skipped": [...],
})

streamEvent(w, "sufficiency_check", map[string]any{
    "sufficient": false,
    "reason": "Need more context on OAuth scopes",
})
```

---

## Decisions Made

- **2 endpoints**: `/agents/retrieval` + `/agents/generation` (composable primitives)
- **RAG/deep research**: Composed at searchaf layer, not baked into antfly
- **max_iterations default**: 5
- **Hybrid semantic + tree**: Tree search starts with a query (semantic, metadata, or roots) — the `start_nodes` config handles this composition naturally
- **Breaking changes**: Allowed — old `/agents/answer` and `/agents/chat` will be removed
- **Stateless multi-turn**: Client sends `context[]` each request
