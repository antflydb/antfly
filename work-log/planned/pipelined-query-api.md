# Pipelined Query API - Implementation Plan

**Feature:** Multi-Stage Query Pipelines with Operations (Delete-by-Query, Update-by-Query, Cross-Table Joins)
**Target Release:** TBD
**Estimated Effort:** 4-6 days
**Dependencies:**
- Graph Query Infrastructure (✅ Completed - merged to main)
- Document Update DML (🔄 Planned - work-log/planned/001-document-update-dml)
- Distributed Transactions (✅ Completed)

## Overview

Enable multi-stage query pipelines where users can:
1. Execute queries and reference their results in subsequent stages
2. Perform operations on query results (delete, update, batch)
3. Chain queries across tables (join-like operations)
4. Combine full-text, semantic, and graph searches with data operations

**Key Design Principle:** Build on existing infrastructure rather than creating a new execution engine. The graph query implementation already demonstrates result references (`$full_text_results`), so we extend that pattern.

## Architecture

### Current State (Graph Queries)

Graph queries already support multi-stage execution:

```go
// From src/store/db/indexes/remoteindex.go (graphdb branch)
type RemoteIndexSearchRequest struct {
    BleveSearchRequest *bleve.SearchRequest
    VectorSearches     map[string]vector.T
    GraphSearches      map[string]*GraphQuery  // ✅ Already supports multiple stages
    FusionParams       *FusionParams
}

// Graph queries reference previous results
type GraphNodeSelector struct {
    ResultRef string // e.g., "$full_text_results", "$aknn_results.my_index"
}
```

**Pattern to extend:** Add operation stages that reference query results.

### Proposed Extension

```
┌─────────────────────────────────────────────────────────┐
│                    Query Pipeline                        │
├─────────────────────────────────────────────────────────┤
│  Stage 1: Query                                          │
│    ├─ full_text_search / semantic_search / graph        │
│    └─ Results: QueryResult (with IDs, hits, etc.)       │
│                                                          │
│  Stage 2: Reference Results + Operate                    │
│    ├─ source: "$stage1"                                 │
│    └─ operation: delete | batch | transform | query     │
│                                                          │
│  Stage 3: Reference Multiple Results                     │
│    ├─ source: "$stage1", "$stage2"                      │
│    └─ Combine/Join operations                           │
└─────────────────────────────────────────────────────────┘
```

## Implementation Phases

### Phase 1: Core Types & Stage Naming

**Files to modify:**
- `src/metadata/api.yaml` (OpenAPI spec)
- `src/store/db/indexes/remoteindex.go` (if needed)

**Changes:**

#### 1.1 Add `stage_name` to QueryRequest

```yaml
# In src/metadata/api.yaml

QueryRequest:
  type: object
  properties:
    # ... existing fields (table, full_text_search, semantic_search, etc.) ...

    stage_name:
      type: string
      description: |
        Optional name for this query stage. Required when using in pipelines
        or when other stages reference this query's results.
        Must be unique within a pipeline.
      pattern: "^[a-zA-Z_][a-zA-Z0-9_]*$"
      example: "search_papers"
```

#### 1.2 Add Operation Types

```yaml
PipelineOperation:
  oneOf:
    - $ref: "#/components/schemas/QueryOperation"
    - $ref: "#/components/schemas/DeleteByQueryOperation"
    - $ref: "#/components/schemas/BatchByQueryOperation"
    - $ref: "#/components/schemas/TransformByQueryOperation"  # After DML
  discriminator:
    propertyName: operation_type

QueryOperation:
  description: |
    Standard query operation (full-text, semantic, graph).
    This is just QueryRequest with operation_type added for discrimination.
  allOf:
    - $ref: "#/components/schemas/QueryRequest"
    - type: object
      required:
        - operation_type
      properties:
        operation_type:
          type: string
          enum: ["query"]

DeleteByQueryOperation:
  type: object
  required:
    - operation_type
    - table
    - source
  properties:
    operation_type:
      type: string
      enum: ["delete_by_query"]
    table:
      type: string
      description: Table to delete from
    source:
      type: string
      description: |
        Reference to previous query stage results.
        Uses $stage_name syntax to reference document IDs.
      pattern: "^\\$[a-zA-Z_][a-zA-Z0-9_]*$"
      example: "$search_results"
    condition:
      type: string
      description: |
        Optional condition expression. Only execute if condition is true.
        Supports simple comparisons on stage results.
      example: "$search_results.count > 0"

BatchByQueryOperation:
  type: object
  required:
    - operation_type
    - table
    - source
  properties:
    operation_type:
      type: string
      enum: ["batch_by_query"]
    table:
      type: string
    source:
      type: string
      description: Reference to query results for keys
      pattern: "^\\$[a-zA-Z_][a-zA-Z0-9_]*$"
    batch:
      $ref: "#/components/schemas/BatchRequest"
      description: |
        Batch operation to apply. The 'source' IDs are used as keys
        for the batch operations.

TransformByQueryOperation:
  type: object
  required:
    - operation_type
    - table
    - source
    - operations
  properties:
    operation_type:
      type: string
      enum: ["transform_by_query"]
    table:
      type: string
    source:
      type: string
      description: Reference to query results to transform
      pattern: "^\\$[a-zA-Z_][a-zA-Z0-9_]*$"
    operations:
      type: array
      items:
        $ref: "#/components/schemas/TransformOp"  # From DML plan
      description: |
        Transform operations to apply to all matching documents.
        These operations are applied to every document in the source results.
    condition:
      type: string
      description: Optional condition
      example: "$source.count > 0"

PipelineRequest:
  type: object
  required:
    - stages
  properties:
    stages:
      type: array
      description: |
        Ordered list of pipeline stages to execute. Stages execute sequentially,
        and later stages can reference results from earlier stages using $stage_name.
      items:
        $ref: "#/components/schemas/PipelineOperation"
      minItems: 1
    return_stages:
      type: array
      items:
        type: string
      description: |
        Optional list of stage names to include in response.
        If empty, returns all stages. Use to minimize response size.
      example: ["final_results"]

PipelineResult:
  type: object
  required:
    - execution_time
    - stages
  properties:
    execution_time:
      type: integer
      format: int64
      x-go-type: time.Duration
      description: Total pipeline execution time
    stages:
      type: object
      additionalProperties:
        $ref: "#/components/schemas/StageResult"
      description: Results keyed by stage name

StageResult:
  type: object
  required:
    - stage_name
    - operation_type
    - status
    - took
  properties:
    stage_name:
      type: string
    operation_type:
      type: string
      enum: ["query", "delete_by_query", "batch_by_query", "transform_by_query"]
    status:
      type: string
      enum: ["success", "error", "skipped"]
    took:
      type: integer
      format: int64
      x-go-type: time.Duration
    error:
      type: string
    result:
      description: Operation-specific result (QueryResult, DeleteResult, etc.)
      oneOf:
        - $ref: "#/components/schemas/QueryResult"
        - $ref: "#/components/schemas/OperationResult"

OperationResult:
  type: object
  properties:
    affected_count:
      type: integer
      description: Number of documents affected by the operation
    failed_keys:
      type: array
      items:
        type: string
      description: Keys that failed (for batch/transform operations)
```

#### 1.3 Add Endpoints

```yaml
/pipeline:
  post:
    summary: Execute a query pipeline
    tags:
      - query_operations
    description: |
      Execute a multi-stage query pipeline where each stage can reference
      results from previous stages. Supports queries, deletes, updates, and
      cross-table operations.
    operationId: executePipeline
    requestBody:
      required: true
      content:
        application/json:
          schema:
            $ref: "#/components/schemas/PipelineRequest"
    responses:
      "200":
        description: Pipeline executed successfully
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/PipelineResult"
      "400":
        $ref: "#/components/responses/BadRequest"
      "500":
        $ref: "#/components/responses/InternalServerError"

/table/{tableName}/pipeline:
  post:
    summary: Execute pipeline scoped to a table
    description: |
      Convenience endpoint for pipelines operating primarily on a single table.
      The table name is automatically set for all stages unless overridden.
    tags:
      - query_operations
    parameters:
      - name: tableName
        in: path
        required: true
        schema:
          type: string
    operationId: executeTablePipeline
    requestBody:
      required: true
      content:
        application/json:
          schema:
            $ref: "#/components/schemas/PipelineRequest"
    responses:
      "200":
        description: Pipeline executed successfully
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/PipelineResult"
```

**Testing:**
- Validate OpenAPI spec generation
- Verify types compile correctly

**Estimated time:** 1 day

---

### Phase 2: Result Reference Resolution

**Files to create:**
- `src/metadata/pipeline.go` (new)

**Implementation:**

Create helper functions for resolving stage references:

```go
package metadata

import (
    "fmt"
    "strings"

    "github.com/antflydb/antfly/src/store/db/indexes"
)

// StageContext holds results from executed stages
type StageContext struct {
    results map[string]*StageResult
}

func NewStageContext() *StageContext {
    return &StageContext{
        results: make(map[string]*StageResult),
    }
}

// AddResult stores a stage result
func (sc *StageContext) AddResult(stageName string, result *StageResult) {
    sc.results[stageName] = result
}

// ResolveReference resolves a $stage_name reference to document IDs
func (sc *StageContext) ResolveReference(ref string) ([]string, error) {
    // Parse reference: "$stage_name" or "$stage_name.field"
    if !strings.HasPrefix(ref, "$") {
        return nil, fmt.Errorf("invalid reference: %s (must start with $)", ref)
    }

    parts := strings.SplitN(ref[1:], ".", 2)
    stageName := parts[0]

    result, exists := sc.results[stageName]
    if !exists {
        return nil, fmt.Errorf("stage not found: %s", stageName)
    }

    if result.Status != "success" {
        return nil, fmt.Errorf("cannot reference failed stage: %s", stageName)
    }

    // Extract IDs from query result
    if queryResult, ok := result.Result.(*QueryResult); ok {
        ids := make([]string, 0, len(queryResult.Hits.Hits))
        for _, hit := range queryResult.Hits.Hits {
            ids = append(ids, hit.ID)
        }
        return ids, nil
    }

    return nil, fmt.Errorf("stage %s does not have query results", stageName)
}

// EvaluateCondition evaluates simple conditions like "$stage.count > 0"
func (sc *StageContext) EvaluateCondition(condition string) (bool, error) {
    if condition == "" {
        return true, nil // No condition = always true
    }

    // Simple parser for conditions like "$stage.count > 0"
    // Format: $stage_name.field operator value
    parts := strings.Fields(condition)
    if len(parts) != 3 {
        return false, fmt.Errorf("invalid condition format: %s", condition)
    }

    ref := parts[0]
    operator := parts[1]
    valueStr := parts[2]

    // Resolve the reference
    if !strings.HasPrefix(ref, "$") {
        return false, fmt.Errorf("condition reference must start with $")
    }

    refParts := strings.SplitN(ref[1:], ".", 2)
    if len(refParts) != 2 {
        return false, fmt.Errorf("condition must specify field: $stage.field")
    }

    stageName := refParts[0]
    field := refParts[1]

    result, exists := sc.results[stageName]
    if !exists {
        return false, fmt.Errorf("stage not found in condition: %s", stageName)
    }

    // Handle special fields
    switch field {
    case "count":
        if queryResult, ok := result.Result.(*QueryResult); ok {
            count := int(queryResult.Hits.Total)
            value, err := strconv.Atoi(valueStr)
            if err != nil {
                return false, fmt.Errorf("invalid value in condition: %s", valueStr)
            }

            switch operator {
            case ">":
                return count > value, nil
            case ">=":
                return count >= value, nil
            case "<":
                return count < value, nil
            case "<=":
                return count <= value, nil
            case "==":
                return count == value, nil
            case "!=":
                return count != value, nil
            default:
                return false, fmt.Errorf("unsupported operator: %s", operator)
            }
        }
    }

    return false, fmt.Errorf("unsupported condition field: %s", field)
}
```

**Testing:**
- Unit tests for reference resolution
- Test various reference formats
- Test condition evaluation

**Estimated time:** 0.5 day

---

### Phase 3: Delete-by-Query Implementation

**Files to modify:**
- `src/metadata/pipeline.go`
- `src/metadata/api.go`

**Implementation:**

```go
// In src/metadata/pipeline.go

type PipelineExecutor struct {
    ms     *MetadataStore
    logger *zap.Logger
}

func NewPipelineExecutor(ms *MetadataStore, logger *zap.Logger) *PipelineExecutor {
    return &PipelineExecutor{
        ms:     ms,
        logger: logger,
    }
}

// ExecutePipeline executes a multi-stage pipeline
func (pe *PipelineExecutor) ExecutePipeline(
    ctx context.Context,
    req *PipelineRequest,
    defaultTable string,
) (*PipelineResult, error) {
    stageCtx := NewStageContext()
    stageResults := make(map[string]*StageResult)

    startTime := time.Now()

    for i, stage := range req.Stages {
        stageName := stage.StageName
        if stageName == "" {
            stageName = fmt.Sprintf("stage_%d", i)
        }

        pe.logger.Info("Executing pipeline stage",
            zap.String("stage", stageName),
            zap.String("type", stage.OperationType))

        stageStart := time.Now()

        var result *StageResult
        var err error

        switch stage.OperationType {
        case "query":
            result, err = pe.executeQueryStage(ctx, stage, defaultTable)

        case "delete_by_query":
            result, err = pe.executeDeleteByQuery(ctx, stage, stageCtx)

        case "batch_by_query":
            result, err = pe.executeBatchByQuery(ctx, stage, stageCtx)

        case "transform_by_query":
            result, err = pe.executeTransformByQuery(ctx, stage, stageCtx)

        default:
            err = fmt.Errorf("unknown operation type: %s", stage.OperationType)
        }

        if err != nil {
            result = &StageResult{
                StageName:     stageName,
                OperationType: stage.OperationType,
                Status:        "error",
                Error:         err.Error(),
                Took:          time.Since(stageStart),
            }
        } else {
            result.StageName = stageName
            result.OperationType = stage.OperationType
            result.Status = "success"
            result.Took = time.Since(stageStart)
        }

        stageResults[stageName] = result
        stageCtx.AddResult(stageName, result)

        // Stop on error unless specified otherwise
        if err != nil && !stage.ContinueOnError {
            pe.logger.Warn("Stage failed, stopping pipeline",
                zap.String("stage", stageName),
                zap.Error(err))
            break
        }
    }

    // Filter results if return_stages specified
    if len(req.ReturnStages) > 0 {
        filtered := make(map[string]*StageResult)
        for _, stageName := range req.ReturnStages {
            if result, exists := stageResults[stageName]; exists {
                filtered[stageName] = result
            }
        }
        stageResults = filtered
    }

    return &PipelineResult{
        ExecutionTime: time.Since(startTime),
        Stages:        stageResults,
    }, nil
}

func (pe *PipelineExecutor) executeQueryStage(
    ctx context.Context,
    stage *PipelineOperation,
    defaultTable string,
) (*StageResult, error) {
    // Use existing query execution logic
    queryReq := stage.QueryRequest
    if queryReq.Table == "" && defaultTable != "" {
        queryReq.Table = defaultTable
    }

    result, err := pe.ms.executeQuery(ctx, queryReq)
    if err != nil {
        return nil, err
    }

    return &StageResult{
        Result: result,
    }, nil
}

func (pe *PipelineExecutor) executeDeleteByQuery(
    ctx context.Context,
    stage *DeleteByQueryOperation,
    stageCtx *StageContext,
) (*StageResult, error) {
    // Evaluate condition if specified
    if stage.Condition != "" {
        shouldExecute, err := stageCtx.EvaluateCondition(stage.Condition)
        if err != nil {
            return nil, fmt.Errorf("evaluating condition: %w", err)
        }
        if !shouldExecute {
            pe.logger.Info("Skipping delete_by_query due to condition",
                zap.String("condition", stage.Condition))
            return &StageResult{
                Status: "skipped",
                Result: &OperationResult{AffectedCount: 0},
            }, nil
        }
    }

    // Resolve source reference to get document IDs
    ids, err := stageCtx.ResolveReference(stage.Source)
    if err != nil {
        return nil, fmt.Errorf("resolving source: %w", err)
    }

    if len(ids) == 0 {
        pe.logger.Info("No documents to delete")
        return &StageResult{
            Result: &OperationResult{AffectedCount: 0},
        }, nil
    }

    pe.logger.Info("Deleting documents",
        zap.String("table", stage.Table),
        zap.Int("count", len(ids)))

    // Convert IDs to keys and execute batch delete
    keys := make([]string, len(ids))
    for i, id := range ids {
        keys[i] = id
    }

    // Use existing batch API
    batchReq := &BatchRequest{
        Deletes: keys,
    }

    err = pe.ms.executeBatch(ctx, stage.Table, batchReq)
    if err != nil {
        return nil, fmt.Errorf("executing batch delete: %w", err)
    }

    return &StageResult{
        Result: &OperationResult{
            AffectedCount: len(ids),
        },
    }, nil
}
```

**Integration with API handler:**

```go
// In src/metadata/api.go

func (t *TableApi) ExecutePipeline(
    w http.ResponseWriter,
    r *http.Request,
    tableName string,
) {
    var req PipelineRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        errorResponse(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
        return
    }

    executor := NewPipelineExecutor(t.ln, t.logger)
    result, err := executor.ExecutePipeline(r.Context(), &req, tableName)
    if err != nil {
        errorResponse(w, fmt.Sprintf("pipeline execution failed: %v", err), http.StatusInternalServerError)
        return
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(result)
}
```

**Testing:**
- Integration test for delete-by-query
- Test condition evaluation
- Test with empty results
- Test cross-shard deletes

**Estimated time:** 1.5 days

---

### Phase 4: Batch-by-Query Implementation

**Files to modify:**
- `src/metadata/pipeline.go`

**Implementation:**

```go
func (pe *PipelineExecutor) executeBatchByQuery(
    ctx context.Context,
    stage *BatchByQueryOperation,
    stageCtx *StageContext,
) (*StageResult, error) {
    // Resolve source reference
    ids, err := stageCtx.ResolveReference(stage.Source)
    if err != nil {
        return nil, fmt.Errorf("resolving source: %w", err)
    }

    if len(ids) == 0 {
        return &StageResult{
            Result: &OperationResult{AffectedCount: 0},
        }, nil
    }

    // Apply batch operations to the resolved IDs
    // This allows operations like:
    // - Delete all IDs from source
    // - Update fields on all IDs
    // - Mix of operations

    batchReq := stage.Batch

    // If batch has its own inserts/deletes, apply them
    // If source is meant to be the keys for operations, handle that

    err = pe.ms.executeBatch(ctx, stage.Table, batchReq)
    if err != nil {
        return nil, fmt.Errorf("executing batch: %w", err)
    }

    return &StageResult{
        Result: &OperationResult{
            AffectedCount: len(ids),
        },
    }, nil
}
```

**Estimated time:** 0.5 day

---

### Phase 5: Transform-by-Query Implementation (After DML)

**Files to modify:**
- `src/metadata/pipeline.go`

**Dependencies:** Wait for Document Update DML (Phase 1-5) to be completed

**Implementation:**

```go
func (pe *PipelineExecutor) executeTransformByQuery(
    ctx context.Context,
    stage *TransformByQueryOperation,
    stageCtx *StageContext,
) (*StageResult, error) {
    // Evaluate condition if specified
    if stage.Condition != "" {
        shouldExecute, err := stageCtx.EvaluateCondition(stage.Condition)
        if err != nil {
            return nil, fmt.Errorf("evaluating condition: %w", err)
        }
        if !shouldExecute {
            return &StageResult{
                Status: "skipped",
                Result: &OperationResult{AffectedCount: 0},
            }, nil
        }
    }

    // Resolve source reference
    ids, err := stageCtx.ResolveReference(stage.Source)
    if err != nil {
        return nil, fmt.Errorf("resolving source: %w", err)
    }

    if len(ids) == 0 {
        return &StageResult{
            Result: &OperationResult{AffectedCount: 0},
        }, nil
    }

    // Build transform operations for each ID
    transforms := make([]*Transform, 0, len(ids))
    for _, id := range ids {
        transforms = append(transforms, &Transform{
            Key:        id,
            Operations: stage.Operations,
            Upsert:     false, // Don't upsert in transform-by-query
        })
    }

    // Execute batch with transforms
    batchReq := &BatchRequest{
        Transforms: transforms,
    }

    err = pe.ms.executeBatch(ctx, stage.Table, batchReq)
    if err != nil {
        return nil, fmt.Errorf("executing transforms: %w", err)
    }

    return &StageResult{
        Result: &OperationResult{
            AffectedCount: len(ids),
        },
    }, nil
}
```

**Testing:**
- Test transform operations on query results
- Test with different operators ($set, $inc, etc.)
- Test condition-based execution

**Estimated time:** 1 day (includes waiting for DML)

---

### Phase 6: Documentation & Examples

**Files to create:**
- `docs/pipelined-queries.md` (new)
- `examples/pipelines/` (new directory)

**Content:**

#### 6.1 Documentation

```markdown
# Pipelined Queries

Execute multi-stage query pipelines where each stage can reference results from previous stages.

## Use Cases

### Delete by Query
Remove all documents matching search criteria:
```json
POST /api/v1/pipeline
{
  "stages": [
    {
      "stage_name": "find_old",
      "operation_type": "query",
      "table": "articles",
      "full_text_search": {"query": "status:archived AND created:<2020-01-01"},
      "limit": 1000
    },
    {
      "operation_type": "delete_by_query",
      "table": "articles",
      "source": "$find_old",
      "condition": "$find_old.count > 0"
    }
  ]
}
```

### Cross-Table Join
Query one table and use results to filter another:
```json
{
  "stages": [
    {
      "stage_name": "premium_users",
      "operation_type": "query",
      "table": "users",
      "full_text_search": {"query": "role:premium"}
    },
    {
      "stage_name": "user_orders",
      "operation_type": "query",
      "table": "orders",
      "filter_prefix": "$premium_users.ids[*]"
    }
  ]
}
```

### Update by Query
Increment counters for search results:
```json
{
  "stages": [
    {
      "stage_name": "popular",
      "operation_type": "query",
      "table": "posts",
      "semantic_search": "best tutorials",
      "indexes": ["content_embedding"],
      "limit": 20
    },
    {
      "operation_type": "transform_by_query",
      "table": "posts",
      "source": "$popular",
      "operations": [
        {"op": "$inc", "path": "$.views", "value": 1},
        {"op": "$currentDate", "path": "$.last_viewed"}
      ]
    }
  ]
}
```

### Graph Expansion + Update
```json
{
  "stages": [
    {
      "stage_name": "papers",
      "operation_type": "query",
      "table": "papers",
      "semantic_search": "transformers",
      "graph_searches": {
        "citations": {
          "type": "traverse",
          "index_name": "paper_graph",
          "start_nodes": {"result_ref": "$full_text_results"},
          "params": {"edge_types": ["cites"], "max_depth": 2}
        }
      }
    },
    {
      "operation_type": "transform_by_query",
      "table": "papers",
      "source": "$papers",
      "operations": [
        {"op": "$inc", "path": "$.citation_count", "value": 1}
      ]
    }
  ]
}
```

## Stage Reference Syntax

- `$stage_name` - Reference a stage's document IDs
- `$stage_name.count` - Number of results from stage
- `$stage_name.ids` - Array of document IDs

## Conditions

Stages can execute conditionally:
```json
{
  "condition": "$previous_stage.count > 0"
}
```

Supported operators: `>`, `>=`, `<`, `<=`, `==`, `!=`

## Error Handling

By default, pipeline stops on first error. To continue:
```json
{
  "stage_name": "optional_stage",
  "continue_on_error": true,
  ...
}
```

## Best Practices

1. **Name your stages** clearly for debugging
2. **Use conditions** to avoid unnecessary operations
3. **Use return_stages** to minimize response size
4. **Limit query results** before operations to control batch size
5. **Use table-scoped endpoint** when operating on single table
```

#### 6.2 Example Applications

Create working examples:

1. **Cleanup service** - Delete old/archived documents
2. **Analytics updater** - Increment counters based on searches
3. **Cross-table reporting** - Join-like operations across tables
4. **Batch processor** - Complex multi-stage data transformations

**Estimated time:** 1 day

---

### Phase 7: Testing & Validation

#### 7.1 Unit Tests

**Files:**
- `src/metadata/pipeline_test.go` (new)

**Coverage:**
- Reference resolution
- Condition evaluation
- Stage context management
- Error handling

#### 7.2 Integration Tests

**Files:**
- `e2e/pipeline_test.go` (new)

**Scenarios:**
- Delete-by-query across shards
- Cross-table queries
- Multi-stage pipelines (3+ stages)
- Conditional execution
- Error handling and recovery
- Transform-by-query (after DML)
- Graph expansion + operations

#### 7.3 Performance Tests

**Benchmarks:**
- Pipeline overhead vs individual operations
- Large result set handling
- Multi-shard coordination
- Memory usage

**Target:** <50ms overhead for typical 2-3 stage pipelines

**Estimated time:** 1.5 days

---

## Implementation Order

1. **Phase 1:** Core types & OpenAPI (1 day)
2. **Phase 2:** Reference resolution (0.5 day)
3. **Phase 3:** Delete-by-query (1.5 days) ← **Ship this first**
4. **Phase 4:** Batch-by-query (0.5 day)
5. **Phase 5:** Transform-by-query (1 day, after DML)
6. **Phase 6:** Documentation (1 day)
7. **Phase 7:** Testing (1.5 days)

**Total:** 7.5 days (6.5 days before DML dependency)

## Rollout Strategy

### Stage 1: Delete-by-Query MVP (Week 1)
- Ship phases 1-3 only
- Most common use case (90% of requests)
- Simple, well-understood semantics
- Easy to validate correctness

### Stage 2: Batch Operations (Week 2)
- Add batch-by-query
- Enable more complex workflows
- Build on proven delete-by-query pattern

### Stage 3: Update Operations (Week 3+)
- Wait for DML completion
- Add transform-by-query
- Full feature parity with MongoDB aggregation pipelines

## Success Criteria

- [ ] Delete-by-query working across shards
- [ ] Cross-table queries (join-like operations)
- [ ] Conditional execution
- [ ] Transform-by-query (after DML)
- [ ] Documentation with 3+ examples
- [ ] <50ms pipeline overhead
- [ ] Zero regressions in existing query API

## Dependencies Timeline

```
Week 1-2: Pipelined Queries (Phases 1-4)
   ├─ Builds on: Graph Queries (✅ Complete)
   └─ Builds on: Distributed Transactions (✅ Complete)

Week 3-4: Document Update DML
   └─ Independent implementation

Week 5: Integration
   └─ Transform-by-query (Phase 5)
```

## Alternatives Considered

### 1. Complex DAG Executor (Original Proposal)
**Rejected:** Too complex, over-engineered for current needs

### 2. GraphQL-style Query Language
**Rejected:** Poor fit for document database, steep learning curve

### 3. SQL-like Syntax
**Rejected:** Doesn't fit NoSQL paradigm, limited by SQL semantics

### 4. Extend Graph Queries Only
**Rejected:** Graph queries are for traversal, not data manipulation

## Migration Path

Existing query API remains unchanged:
```json
POST /api/v1/table/articles/query
{
  "full_text_search": {"query": "..."}
}
```

New pipeline API is additive:
```json
POST /api/v1/pipeline
{
  "stages": [...]
}
```

Users can adopt incrementally.

## Related Work

- **MongoDB Aggregation Pipeline:** Similar multi-stage concept, but specialized for aggregations
- **Elasticsearch Update By Query:** Delete/update by query, but single-stage only
- **Apache Beam:** Complex DAG processing, but for batch/streaming (overkill for us)
- **Our Graph Queries:** Demonstrates result references pattern we're extending

## Open Questions

- [ ] Should we support parallel stage execution for independent stages?
- [ ] Maximum pipeline depth limit (prevent abuse)?
- [ ] Streaming results for long-running pipelines?
- [ ] Dry-run mode for validation?
- [ ] How to handle partial failures in distributed transactions?

## Future Enhancements

- **Aggregations:** `aggregate_by_query` operation
- **Joins:** More sophisticated cross-table joins
- **Subqueries:** Nested pipelines
- **Streaming:** SSE for long-running pipelines
- **Caching:** Cache intermediate stage results
