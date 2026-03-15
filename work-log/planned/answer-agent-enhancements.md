# Answer Agent Enhancements

This document outlines proposed enhancements to the antfly answer agent and RAG system. These features build on the unified query transformation architecture (strategy router, semantic modes) already implemented.

## Table of Contents

1. [Agentic RAG](#agentic-rag)
2. [Lost-in-the-Middle Mitigation](#lost-in-the-middle-mitigation)
3. [Conversation History](#conversation-history)
4. [User Feedback Loop](#user-feedback-loop)
5. [Query Analytics](#query-analytics)
6. [Corrective RAG (CRAG)](#corrective-rag-crag)
7. [Semantic Caching](#semantic-caching)
8. [Parent Document Retriever](#parent-document-retriever)
9. [Contextual Compression](#contextual-compression)
10. [Grounded Citation Verification](#grounded-citation-verification)

---

## Agentic RAG

### Overview

Enable the LLM to decide when and how to retrieve, filter, and refine results through tool use. Instead of a fixed retrieve-then-generate pipeline, the agent can iteratively search, filter, and ask clarifying questions.

### Available Tools

```go
type AgentTool struct {
    Name        string
    Description string
    Parameters  map[string]ToolParam
    Execute     func(ctx context.Context, args map[string]any, state *AgentState) (any, error)
}

// Core retrieval tools
var AgentTools = []AgentTool{
    {
        Name:        "search",
        Description: "Search for documents matching a query",
        Parameters: map[string]ToolParam{
            "query": {Type: "string", Description: "Search query", Required: true},
            "limit": {Type: "integer", Description: "Max results", Default: 10},
        },
    },
    {
        Name:        "filter_by_date",
        Description: "Filter current results to a date range",
        Parameters: map[string]ToolParam{
            "after":  {Type: "string", Description: "ISO date, keep docs after this"},
            "before": {Type: "string", Description: "ISO date, keep docs before this"},
        },
    },
    {
        Name:        "filter_contains",
        Description: "Keep only documents containing specific terms",
        Parameters: map[string]ToolParam{
            "terms": {Type: "array", Description: "Terms that must appear"},
        },
    },
    {
        Name:        "filter_by_source",
        Description: "Keep documents from specific sources/tables",
        Parameters: map[string]ToolParam{
            "sources": {Type: "array", Description: "Source names to include"},
        },
    },
    {
        Name:        "filter_by_type",
        Description: "Keep documents of specific types",
        Parameters: map[string]ToolParam{
            "types": {Type: "array", Description: "Document types (tutorial, reference, etc.)"},
        },
    },
    {
        Name:        "ask_clarification",
        Description: "Ask the user for clarification before proceeding",
        Parameters: map[string]ToolParam{
            "question": {Type: "string", Description: "Question to ask the user"},
        },
    },
    {
        Name:        "generate_answer",
        Description: "Generate final answer from current document set",
        Parameters: map[string]ToolParam{
            "focus": {Type: "string", Description: "Optional focus area for the answer"},
        },
    },
}
```

### Agent State

```go
type AgentState struct {
    OriginalQuery   string
    CurrentResults  []schema.Document
    SearchHistory   []SearchAction
    FilterHistory   []FilterAction
    Iteration       int
    MaxIterations   int
}

type SearchAction struct {
    Query     string
    Results   int
    Timestamp time.Time
}

type FilterAction struct {
    Tool      string
    Args      map[string]any
    Before    int  // Doc count before
    After     int  // Doc count after
    Timestamp time.Time
}
```

### Implementation Location

- `lib/ai/agent.go` - Agent loop and tool execution
- `lib/ai/agent_tools.go` - Tool definitions and implementations
- `src/metadata/api_ai.go` - New `/api/answer-agent/agentic` endpoint

### Example Flow

```
User: "What security vulnerabilities were fixed in the last quarter?"

Agent thinks: Need to search for security vulnerabilities, then filter by date

Tool call: search(query="security vulnerabilities fixed")
Result: 50 documents

Agent thinks: Too many results, need to narrow by date

Tool call: filter_by_date(after="2024-09-01")
Result: 12 documents

Agent thinks: Some might be proposals, not actual fixes

Tool call: filter_contains(terms=["fixed", "resolved", "patched"])
Result: 7 documents

Agent thinks: Good set, can generate answer now

Tool call: generate_answer()
Result: Final answer with citations
```

---

## Lost-in-the-Middle Mitigation

### Overview

LLMs tend to focus on information at the beginning and end of the context, losing information in the middle. Mitigate this by strategic document ordering.

### Implementation

```go
// DocumentOrderingStrategy determines how documents are arranged in context
type DocumentOrderingStrategy string

const (
    // OrderByRelevance - standard ordering by relevance score (default)
    OrderByRelevance DocumentOrderingStrategy = "relevance"

    // OrderSandwich - most relevant at start AND end, less relevant in middle
    OrderSandwich DocumentOrderingStrategy = "sandwich"

    // OrderReversed - least relevant first, most relevant last (recency bias)
    OrderReversed DocumentOrderingStrategy = "reversed"
)

// ReorderDocuments applies the ordering strategy to mitigate lost-in-the-middle
func ReorderDocuments(docs []schema.Document, scores []float32, strategy DocumentOrderingStrategy) []schema.Document {
    if len(docs) <= 2 || strategy == OrderByRelevance {
        return docs
    }

    // Sort by score descending first
    sorted := sortByScore(docs, scores)

    switch strategy {
    case OrderSandwich:
        // Place top docs at start and end
        // [1, 3, 5, 7, 8, 6, 4, 2] for 8 docs ranked 1-8
        result := make([]schema.Document, len(sorted))
        left, right := 0, len(sorted)-1
        for i, doc := range sorted {
            if i%2 == 0 {
                result[left] = doc
                left++
            } else {
                result[right] = doc
                right--
            }
        }
        return result

    case OrderReversed:
        // Reverse so most relevant is last (exploits recency bias)
        slices.Reverse(sorted)
        return sorted

    default:
        return sorted
    }
}
```

### Configuration

Add to `AnswerAgentRequest`:

```yaml
document_ordering:
  type: string
  enum: ["relevance", "sandwich", "reversed"]
  default: "sandwich"
  description: |
    Strategy for ordering documents in context to mitigate lost-in-the-middle:
    - relevance: Standard order by relevance score
    - sandwich: Most relevant at start AND end (recommended)
    - reversed: Most relevant last (exploits recency bias)
```

### Implementation Location

- `lib/ai/ordering.go` - Document reordering logic
- `lib/ai/genkit.go` - Apply ordering before context assembly

---

## Conversation History

### Overview

Enable multi-turn conversations where follow-up questions reference prior context. The agent maintains conversation state and can resolve references like "it", "that", "the previous one".

### Data Model

```go
type ConversationMessage struct {
    Role      string    `json:"role"`      // "user" or "assistant"
    Content   string    `json:"content"`
    Timestamp time.Time `json:"timestamp"`
    // For assistant messages, track what was retrieved
    RetrievedDocs []string `json:"retrieved_docs,omitempty"`
}

type ConversationContext struct {
    SessionID string                `json:"session_id"`
    Messages  []ConversationMessage `json:"messages"`
    // Extracted entities and topics for reference resolution
    Entities  map[string]string     `json:"entities,omitempty"`
}
```

### API Extension

Add to `AnswerAgentRequest`:

```yaml
conversation_history:
  type: array
  items:
    $ref: '#/components/schemas/ConversationMessage'
  description: |
    Previous messages in the conversation for multi-turn context.
    Used to resolve references and maintain conversational flow.
  example:
    - role: "user"
      content: "What authentication methods does the API support?"
    - role: "assistant"
      content: "The API supports OAuth 2.0 and API keys [doc_id auth-docs]."
    - role: "user"
      content: "How do I rotate them?"
```

### Query Rewriting with Context

```go
// RewriteQueryWithHistory rewrites ambiguous queries using conversation history
func RewriteQueryWithHistory(query string, history []ConversationMessage) string {
    if len(history) == 0 {
        return query
    }

    // Prompt the LLM to resolve references
    prompt := fmt.Sprintf(`Given this conversation history:
%s

The user now asks: "%s"

Rewrite this query to be self-contained, resolving any references like "it", "them", "that", etc.
If the query is already self-contained, return it unchanged.

Rewritten query:`, formatHistory(history), query)

    // Execute rewrite...
}
```

### Implementation Location

- `lib/ai/conversation.go` - Conversation context management
- `lib/ai/answer.go` - Query rewriting with history
- `src/metadata/api_ai.go` - Handle conversation_history in request

---

## User Feedback Loop

### Overview

Collect user feedback on answer quality to improve retrieval and generation over time. Feedback can be used for:
- Fine-tuning reranker models
- Identifying low-quality document sources
- Detecting query patterns that need improvement

### Data Model

```go
type AnswerFeedback struct {
    QueryID       string    `json:"query_id"`
    SessionID     string    `json:"session_id,omitempty"`
    Rating        int       `json:"rating"`         // 1-5 scale
    FeedbackType  string    `json:"feedback_type"`  // helpful, unhelpful, incorrect, missing_info, irrelevant
    Query         string    `json:"query"`
    Answer        string    `json:"answer"`
    RetrievedDocs []string  `json:"retrieved_docs"` // Document IDs that were retrieved
    CitedDocs     []string  `json:"cited_docs"`     // Document IDs that were actually cited
    Corrections   string    `json:"corrections,omitempty"`
    Timestamp     time.Time `json:"timestamp"`
}

type FeedbackStats struct {
    TotalQueries     int     `json:"total_queries"`
    AverageRating    float64 `json:"average_rating"`
    HelpfulRate      float64 `json:"helpful_rate"`
    IncorrectRate    float64 `json:"incorrect_rate"`
    MissingInfoRate  float64 `json:"missing_info_rate"`
}
```

### API Endpoints

```yaml
/api/answer-agent/feedback:
  post:
    summary: Submit feedback for an answer
    requestBody:
      content:
        application/json:
          schema:
            $ref: '#/components/schemas/AnswerFeedback'
    responses:
      '200':
        description: Feedback recorded

/api/answer-agent/feedback/stats:
  get:
    summary: Get feedback statistics
    parameters:
      - name: table
        in: query
        schema:
          type: string
      - name: period
        in: query
        schema:
          type: string
          enum: [day, week, month]
    responses:
      '200':
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/FeedbackStats'
```

### Feedback Analysis

```go
// AnalyzeFeedback identifies patterns in negative feedback
type FeedbackAnalysis struct {
    // Documents that are retrieved but rarely cited (potential relevance issue)
    LowCitationDocs []DocCitationStats `json:"low_citation_docs"`

    // Query patterns with low ratings
    ProblematicPatterns []QueryPattern `json:"problematic_patterns"`

    // Topics with high "missing_info" feedback
    GapTopics []string `json:"gap_topics"`
}

type DocCitationStats struct {
    DocID         string  `json:"doc_id"`
    RetrievalCount int    `json:"retrieval_count"`
    CitationCount  int    `json:"citation_count"`
    CitationRate   float64 `json:"citation_rate"`
}
```

### Implementation Location

- `lib/ai/feedback.go` - Feedback data structures and analysis
- `src/metadata/api_feedback.go` - Feedback API endpoints
- `src/store/feedback_store.go` - Feedback persistence

---

## Query Analytics

### Overview

Track query patterns, latency breakdown, and retrieval effectiveness to identify areas for improvement.

### Metrics Tracked

```go
type QueryMetrics struct {
    // Timing breakdown
    QueryID              string        `json:"query_id"`
    TotalLatency         time.Duration `json:"total_latency"`
    ClassificationTime   time.Duration `json:"classification_time"`
    RetrievalTime        time.Duration `json:"retrieval_time"`
    RerankingTime        time.Duration `json:"reranking_time,omitempty"`
    GenerationTime       time.Duration `json:"generation_time"`

    // Retrieval stats
    Strategy             string   `json:"strategy"`
    SemanticMode         string   `json:"semantic_mode"`
    DocumentsRetrieved   int      `json:"documents_retrieved"`
    DocumentsCited       int      `json:"documents_cited"`
    TablesQueried        []string `json:"tables_queried"`

    // Quality indicators
    Confidence           float32  `json:"confidence"`
    ContextRelevance     float32  `json:"context_relevance,omitempty"`

    // Classification
    RouteType            string   `json:"route_type"`
    QueryLength          int      `json:"query_length"`
    HasFollowup          bool     `json:"has_followup"`
}

type AggregatedAnalytics struct {
    Period           string  `json:"period"`
    TotalQueries     int     `json:"total_queries"`
    AvgLatency       float64 `json:"avg_latency_ms"`
    P95Latency       float64 `json:"p95_latency_ms"`
    P99Latency       float64 `json:"p99_latency_ms"`

    // Strategy distribution
    StrategyBreakdown map[string]int `json:"strategy_breakdown"`

    // Quality metrics
    AvgConfidence    float64 `json:"avg_confidence"`
    AvgDocsRetrieved float64 `json:"avg_docs_retrieved"`
    AvgDocsCited     float64 `json:"avg_docs_cited"`
    CitationRate     float64 `json:"citation_rate"` // cited/retrieved

    // Problem indicators
    LowConfidenceRate float64 `json:"low_confidence_rate"` // % with confidence < 0.5
    ZeroResultRate    float64 `json:"zero_result_rate"`    // % with no results
}
```

### API Endpoints

```yaml
/api/answer-agent/analytics:
  get:
    summary: Get query analytics
    parameters:
      - name: period
        in: query
        schema:
          type: string
          enum: [hour, day, week, month]
      - name: table
        in: query
        schema:
          type: string
    responses:
      '200':
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/AggregatedAnalytics'

/api/answer-agent/analytics/slow-queries:
  get:
    summary: Get slowest queries for optimization
    parameters:
      - name: limit
        in: query
        schema:
          type: integer
          default: 100
      - name: min_latency_ms
        in: query
        schema:
          type: integer
    responses:
      '200':
        content:
          application/json:
            schema:
              type: array
              items:
                $ref: '#/components/schemas/QueryMetrics'
```

### Implementation Location

- `lib/ai/analytics.go` - Metrics collection and aggregation
- `src/metadata/api_analytics.go` - Analytics API endpoints
- `src/store/analytics_store.go` - Time-series storage for metrics

---

## Corrective RAG (CRAG)

### Overview

After retrieval, evaluate document relevance before generation. If documents are irrelevant or ambiguous, take corrective action:
- **Relevant**: Proceed to generation
- **Ambiguous**: Transform query and re-retrieve
- **Irrelevant**: Fall back to "I don't know" or web search

### Implementation

```go
type RelevanceAssessment struct {
    Status     RelevanceStatus `json:"status"`
    Confidence float32         `json:"confidence"`
    Reasoning  string          `json:"reasoning,omitempty"`
}

type RelevanceStatus string

const (
    RelevanceCorrect   RelevanceStatus = "correct"   // Docs are relevant, proceed
    RelevanceAmbiguous RelevanceStatus = "ambiguous" // Uncertain, try to improve
    RelevanceIncorrect RelevanceStatus = "incorrect" // Docs not relevant
)

// AssessRelevance evaluates if retrieved documents are relevant to the query
func (g *GenKitSummarizerImpl) AssessRelevance(
    ctx context.Context,
    query string,
    docs []schema.Document,
) (*RelevanceAssessment, error) {
    prompt := fmt.Sprintf(`Evaluate if these documents can answer the query.

Query: "%s"

Documents:
%s

Assessment (respond with JSON):
{
  "status": "correct" | "ambiguous" | "incorrect",
  "confidence": 0.0-1.0,
  "reasoning": "brief explanation"
}`, query, formatDocsForAssessment(docs))

    // Execute assessment...
}

// CorrectiveRAG implements the CRAG pattern
func (g *GenKitSummarizerImpl) CorrectiveRAG(
    ctx context.Context,
    query string,
    docs []schema.Document,
    opts ...AnswerOption,
) (*AnswerResult, error) {
    // Step 1: Assess relevance
    assessment, err := g.AssessRelevance(ctx, query, docs)
    if err != nil {
        return nil, err
    }

    switch assessment.Status {
    case RelevanceCorrect:
        // Proceed with generation
        return g.Answer(ctx, query, docs, opts...)

    case RelevanceAmbiguous:
        // Try query transformation and re-retrieve
        transformed, _ := g.ClassifyImproveAndTransformQuery(ctx, query)
        // Re-retrieve with transformed query...
        // Then re-assess...

    case RelevanceIncorrect:
        // Return "I don't know" or fall back to web search
        return &AnswerResult{
            Answer: "I don't have enough relevant information to answer this question accurately.",
            AnswerConfidence: 0.1,
            ContextRelevance: assessment.Confidence,
        }, nil
    }
}
```

### Configuration

```yaml
with_corrective_rag:
  type: boolean
  default: false
  description: |
    Enable Corrective RAG (CRAG) pattern. When enabled, the agent assesses
    document relevance before generation and takes corrective action if
    documents are not relevant.

corrective_rag_fallback:
  type: string
  enum: ["none", "web_search", "clarify"]
  default: "none"
  description: |
    Action to take when documents are assessed as irrelevant:
    - none: Return "I don't know" response
    - web_search: Fall back to web search (requires web search config)
    - clarify: Ask user for clarification
```

### Implementation Location

- `lib/ai/crag.go` - Corrective RAG implementation
- `lib/ai/relevance.go` - Relevance assessment

---

## Semantic Caching

### Overview

Cache query-answer pairs and return cached results for semantically similar queries. This dramatically reduces latency and cost for common queries.

### Implementation

```go
type SemanticCache struct {
    embeddingIndex *EmbeddingIndex
    store          CacheStore
    ttl            time.Duration
    similarityThreshold float32
}

type CachedAnswer struct {
    Query         string          `json:"query"`
    QueryEmbedding []float32      `json:"query_embedding"`
    Answer        *AnswerResult   `json:"answer"`
    Confidence    float32         `json:"confidence"`
    CreatedAt     time.Time       `json:"created_at"`
    HitCount      int             `json:"hit_count"`
}

// Get returns a cached answer if a semantically similar query exists
func (c *SemanticCache) Get(ctx context.Context, query string) (*CachedAnswer, bool) {
    // Embed the query
    embedding, err := c.embedQuery(ctx, query)
    if err != nil {
        return nil, false
    }

    // Search for similar cached queries
    results, err := c.embeddingIndex.Search(embedding, 1)
    if err != nil || len(results) == 0 {
        return nil, false
    }

    // Check similarity threshold
    if results[0].Score < c.similarityThreshold {
        return nil, false
    }

    // Retrieve cached answer
    cached, err := c.store.Get(results[0].ID)
    if err != nil || cached.CreatedAt.Add(c.ttl).Before(time.Now()) {
        return nil, false
    }

    // Update hit count
    cached.HitCount++
    c.store.Put(results[0].ID, cached)

    return cached, true
}

// Put stores a query-answer pair in the cache
func (c *SemanticCache) Put(ctx context.Context, query string, answer *AnswerResult) error {
    embedding, err := c.embedQuery(ctx, query)
    if err != nil {
        return err
    }

    cached := &CachedAnswer{
        Query:          query,
        QueryEmbedding: embedding,
        Answer:         answer,
        Confidence:     answer.AnswerConfidence,
        CreatedAt:      time.Now(),
        HitCount:       0,
    }

    id := generateCacheID(query)
    if err := c.embeddingIndex.Add(id, embedding); err != nil {
        return err
    }

    return c.store.Put(id, cached)
}
```

### Configuration

```yaml
semantic_cache:
  type: object
  properties:
    enabled:
      type: boolean
      default: false
    ttl_seconds:
      type: integer
      default: 3600
      description: Cache TTL in seconds
    similarity_threshold:
      type: number
      default: 0.95
      description: Minimum cosine similarity to consider a cache hit
    max_entries:
      type: integer
      default: 10000
      description: Maximum number of cached entries
```

### Implementation Location

- `lib/ai/cache.go` - Semantic cache implementation
- `src/store/cache_store.go` - Cache persistence

---

## Parent Document Retriever

### Overview

When chunks are retrieved, fetch the full parent document or a larger context window to prevent loss of context from aggressive chunking.

### Implementation

```go
type ParentDocumentRetriever struct {
    chunkIndex   Index
    documentStore DocumentStore
    contextWindow int // Number of surrounding chunks to include
}

type ChunkWithContext struct {
    Chunk         schema.Document
    ParentDoc     *schema.Document
    SurroundingChunks []schema.Document
    ChunkIndex    int
    TotalChunks   int
}

// RetrieveWithContext retrieves chunks and expands to include parent context
func (r *ParentDocumentRetriever) RetrieveWithContext(
    ctx context.Context,
    query string,
    limit int,
) ([]ChunkWithContext, error) {
    // Retrieve chunks
    chunks, err := r.chunkIndex.Search(ctx, query, limit)
    if err != nil {
        return nil, err
    }

    results := make([]ChunkWithContext, len(chunks))
    for i, chunk := range chunks {
        // Get parent document ID from chunk metadata
        parentID := chunk.Fields["_parent_id"].(string)
        chunkIdx := chunk.Fields["_chunk_index"].(int)

        // Fetch surrounding chunks
        surrounding, err := r.getSurroundingChunks(ctx, parentID, chunkIdx)
        if err != nil {
            continue
        }

        // Optionally fetch full parent document
        var parent *schema.Document
        if r.contextWindow < 0 { // -1 means fetch full document
            parent, _ = r.documentStore.Get(ctx, parentID)
        }

        results[i] = ChunkWithContext{
            Chunk:            chunk,
            ParentDoc:        parent,
            SurroundingChunks: surrounding,
            ChunkIndex:       chunkIdx,
        }
    }

    return results, nil
}
```

### Configuration

```yaml
parent_document_retrieval:
  type: object
  properties:
    enabled:
      type: boolean
      default: false
    context_window:
      type: integer
      default: 2
      description: |
        Number of surrounding chunks to include (before and after).
        Set to -1 to retrieve the full parent document.
    merge_overlapping:
      type: boolean
      default: true
      description: Merge overlapping chunk contexts from the same document
```

### Implementation Location

- `lib/ai/parent_retriever.go` - Parent document retrieval
- `src/store/indexes/chunkingenricher.go` - Store parent relationships during chunking

---

## Contextual Compression

### Overview

After retrieval but before generation, compress documents to extract only the portions relevant to the query. This reduces token usage while preserving signal.

### Implementation

```go
type ContextualCompressor struct {
    model Model
}

type CompressedDocument struct {
    OriginalID   string `json:"original_id"`
    OriginalSize int    `json:"original_size"`
    Compressed   string `json:"compressed"`
    Relevance    float32 `json:"relevance"`
}

// Compress extracts relevant portions from documents
func (c *ContextualCompressor) Compress(
    ctx context.Context,
    query string,
    docs []schema.Document,
) ([]CompressedDocument, error) {
    results := make([]CompressedDocument, 0, len(docs))

    for _, doc := range docs {
        content := formatDocContent(doc)

        prompt := fmt.Sprintf(`Extract only the portions of this document that are relevant to answering the query.
Preserve important context but remove irrelevant information.

Query: "%s"

Document:
%s

Extracted relevant content (or "NOT_RELEVANT" if nothing is relevant):`, query, content)

        compressed, err := c.model.Generate(ctx, prompt)
        if err != nil {
            continue
        }

        if compressed != "NOT_RELEVANT" {
            results = append(results, CompressedDocument{
                OriginalID:   doc.ID,
                OriginalSize: len(content),
                Compressed:   compressed,
                Relevance:    float32(len(compressed)) / float32(len(content)),
            })
        }
    }

    return results, nil
}
```

### Configuration

```yaml
contextual_compression:
  type: object
  properties:
    enabled:
      type: boolean
      default: false
    min_document_length:
      type: integer
      default: 1000
      description: Only compress documents longer than this (in characters)
    compression_model:
      type: string
      description: Model to use for compression (defaults to main summarizer model)
```

### Implementation Location

- `lib/ai/compression.go` - Contextual compression implementation

---

## Grounded Citation Verification

### Overview

Post-process generated answers to verify that each citation actually supports the claim it's attached to. Flag or remove hallucinated citations.

### Implementation

```go
type CitationVerification struct {
    Citation  string  `json:"citation"`     // e.g., "[doc_id doc1]"
    Claim     string  `json:"claim"`        // The text being cited
    DocID     string  `json:"doc_id"`
    Verified  bool    `json:"verified"`
    Confidence float32 `json:"confidence"`
    Reasoning string  `json:"reasoning,omitempty"`
}

type VerificationResult struct {
    OriginalAnswer   string                `json:"original_answer"`
    VerifiedAnswer   string                `json:"verified_answer"`
    Citations        []CitationVerification `json:"citations"`
    HallucinatedCount int                  `json:"hallucinated_count"`
}

// VerifyCitations checks that each citation supports its claim
func (g *GenKitSummarizerImpl) VerifyCitations(
    ctx context.Context,
    answer string,
    docs []schema.Document,
) (*VerificationResult, error) {
    // Extract citations and their surrounding claims
    citations := extractCitations(answer)

    result := &VerificationResult{
        OriginalAnswer: answer,
        Citations:      make([]CitationVerification, len(citations)),
    }

    for i, cit := range citations {
        // Find the referenced document
        doc := findDoc(docs, cit.DocID)
        if doc == nil {
            result.Citations[i] = CitationVerification{
                Citation:   cit.Raw,
                Claim:      cit.Claim,
                DocID:      cit.DocID,
                Verified:   false,
                Confidence: 0,
                Reasoning:  "Document not found in retrieved set",
            }
            result.HallucinatedCount++
            continue
        }

        // Verify the claim is supported by the document
        verification := verifyClaim(ctx, cit.Claim, doc)
        result.Citations[i] = verification
        if !verification.Verified {
            result.HallucinatedCount++
        }
    }

    // Generate corrected answer with hallucinated citations removed
    result.VerifiedAnswer = removeBadCitations(answer, result.Citations)

    return result, nil
}
```

### Configuration

```yaml
citation_verification:
  type: object
  properties:
    enabled:
      type: boolean
      default: false
    remove_hallucinated:
      type: boolean
      default: true
      description: Remove citations that can't be verified
    include_verification:
      type: boolean
      default: false
      description: Include verification results in response
```

### Implementation Location

- `lib/ai/verification.go` - Citation verification implementation

---

## Priority and Implementation Order

| Feature | Impact | Complexity | Recommended Priority |
|---------|--------|------------|---------------------|
| Lost-in-the-Middle | +10% answer quality | Low | 1 - Quick win |
| Semantic Caching | -50% latency/cost | Medium | 2 - High ROI |
| User Feedback Loop | Long-term improvement | Low | 3 - Foundation |
| Query Analytics | Operational insight | Medium | 4 - Observability |
| Conversation History | Better UX | Medium | 5 - User experience |
| Parent Document Retriever | +15% context quality | Medium | 6 - Retrieval quality |
| Contextual Compression | -30% tokens | Medium | 7 - Cost optimization |
| CRAG | Reliability | High | 8 - Advanced |
| Grounded Citation Verification | Trust | High | 9 - Advanced |
| Agentic RAG | Complex query handling | High | 10 - Advanced |

---

## Related Files

- `lib/ai/answer.go` - Main answer agent implementation
- `lib/ai/prompts.go` - Prompt templates
- `lib/ai/genkit.go` - RAG implementation
- `src/metadata/api_ai.go` - API handlers
- `src/store/indexes/` - Index implementations
