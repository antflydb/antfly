# Query Sampler Feature Implementation Plan

**Date:** 2025-11-06
**Feature:** Named query samplers for capturing query embeddings and results

## Overview

Add support for named samplers that capture semantic search queries, their embeddings, and results to another Antfly table for observability, monitoring, and ML training dataset collection.

## Design Decisions

### Architecture Choices

1. **Named Samplers**: Multiple samplers per table with unique names
   - Allows different use cases (training, monitoring, debugging)
   - Independent configuration and enable/disable per sampler
   - RESTful API: `/api/v1/table/{table}/sampler/{name}`

2. **Versioned Schemas**: Standard sampler target table schemas
   - Version: `antfly_sampler_v1` (initial version)
   - Validated at sampler creation time
   - Allows schema evolution without breaking existing samplers

3. **Sample Timing**: After query execution
   - Captures complete picture: query + embeddings + results + latency
   - Can sample even failed queries for debugging

4. **Auto-Disable on Failures**
   - Target table missing → disable sampler
   - Schema mismatch → disable sampler
   - Updates sampler config automatically

5. **Query Type Support**: Design for both, implement semantic first
   - Config: `query_types: ["semantic", "full_text"]`
   - Initially only semantic_search implemented
   - Full-text search sampling planned for future

## API Design

### Endpoints

```
POST   /api/v1/table/{table}/sampler/{name}   - Create sampler
GET    /api/v1/table/{table}/sampler/{name}   - Get sampler config and stats
PATCH  /api/v1/table/{table}/sampler/{name}   - Update sampler (enable/disable, rate)
DELETE /api/v1/table/{table}/sampler/{name}   - Delete sampler
GET    /api/v1/table/{table}/sampler          - List all samplers for table
```

### Request/Response Schemas

#### CreateSamplerRequest
```json
{
  "target_table": "query_samples",
  "target_index": "default",          // optional
  "schema_version": "antfly_sampler_v1",
  "sample_rate": 0.1,                 // 0.0-1.0 (10%)
  "query_types": ["semantic"],        // ["semantic", "full_text"]
  "index_names": ["embeddings_v2"],   // empty = all indexes
  "enabled": true
}
```

#### PatchSamplerRequest
```json
{
  "enabled": false,          // optional
  "sample_rate": 0.05,       // optional
  "query_types": ["semantic", "full_text"],  // optional
  "index_names": []          // optional
}
```

#### SamplerConfig (Response)
```json
{
  "name": "ml_training",
  "enabled": true,
  "target_table": "query_samples",
  "target_index": "default",
  "schema_version": "antfly_sampler_v1",
  "sample_rate": 0.1,
  "query_types": ["semantic"],
  "index_names": ["embeddings_v2"],
  "created_at": "2025-11-06T...",
  "last_sample_at": "2025-11-06T...",
  "sample_count": 1523,
  "error_count": 2,
  "last_error": "target table not found",
  "last_error_at": "2025-11-06T..."
}
```

## Data Model

### Table Extension
```go
// src/store/table.go
type Table struct {
    Name       string
    Schema     *schema.TableSchema
    Indexes    map[string]indexes.IndexConfig
    Shards     map[types.ID]*ShardConfig
    Samplers   map[string]*SamplerConfig  // NEW
}
```

### SamplerConfig
```go
type SamplerConfig struct {
    Name           string
    Enabled        bool
    TargetTable    string
    TargetIndex    string   // optional
    SchemaVersion  string
    SampleRate     float64  // 0.0-1.0
    QueryTypes     []string // ["semantic", "full_text"]
    IndexNames     []string // source indexes to sample

    // System-tracked metadata
    CreatedAt      time.Time
    LastSampleAt   time.Time
    SampleCount    uint64
    ErrorCount     uint64
    LastError      string
    LastErrorAt    time.Time
}
```

### Storage
- **Key pattern**: `tm:t:{tableName}:s:{samplerName}`
- **Storage**: Metadata raft group (alongside table configs)
- **Caching**: Loaded into memory with table configs for fast access

## Sampler Schema v1

### Required Fields for Target Table

The `antfly_sampler_v1` schema defines the following fields that target tables must have:

```go
// src/store/schemas/sampler_v1.go
var SamplerV1Schema = schema.TableSchema{
    Version: 1,
    Fields: []schema.Field{
        {Name: "query_id", Type: "string"},          // UUID for this sample
        {Name: "timestamp", Type: "datetime"},       // When query occurred
        {Name: "sampler_name", Type: "string"},      // Which sampler captured this
        {Name: "source_table", Type: "string"},      // Table that was queried
        {Name: "query_type", Type: "string"},        // "semantic" or "full_text"
        {Name: "user_query", Type: "string"},        // Original text query
        {Name: "index_names", Type: "[]string"},     // Indexes used
        {Name: "embeddings", Type: "map[string][]float32"},  // Index → embedding
        {Name: "result_count", Type: "uint64"},      // Number of hits
        {Name: "latency_ms", Type: "int64"},         // Query execution time
        {Name: "max_score", Type: "float64"},        // Highest score
        {Name: "error", Type: "string"},             // Error message if failed
        {Name: "query_params", Type: "json"},        // Full query parameters
    },
}
```

### Sample Document Example
```json
{
  "query_id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-11-06T15:23:45Z",
  "sampler_name": "ml_training",
  "source_table": "documents",
  "query_type": "semantic",
  "user_query": "how to deploy kubernetes",
  "index_names": ["embeddings_v2"],
  "embeddings": {
    "embeddings_v2": [0.123, -0.456, 0.789, ...]
  },
  "result_count": 42,
  "latency_ms": 125,
  "max_score": 0.87,
  "error": null,
  "query_params": {
    "limit": 10,
    "distance_under": 0.5,
    "filter_prefix": ""
  }
}
```

## Query Execution Flow

### Current Flow
```
Client → POST /api/v1/table/{table}/query
  ↓
handleQuery() → runQuery()
  ↓
Generate embeddings (with caching)
  ↓
shardIndexes.RRFSearch(ctx, query)
  ↓
Return QueryResult
```

### With Sampling Hook
```
Client → POST /api/v1/table/{table}/query
  ↓
handleQuery() → runQuery()
  ↓
Generate embeddings (with caching)
  ↓
startTime = now()
  ↓
shardIndexes.RRFSearch(ctx, query)
  ↓
latency = time.Since(startTime)
  ↓
NEW: go func() {
    foreach sampler in table.Samplers:
        if sampler.Enabled && shouldSampleQueryType() && ShouldSample(rate):
            sample = BuildSample(query, embeddings, result, latency)
            WriteSample(target_table, sample)
            if error:
                UpdateStats(sampler, error)
                if isSchemaError || isTableNotFound:
                    DisableSampler(sampler)
}()
  ↓
Return QueryResult (no blocking)
```

## Implementation Phases

### Phase 1: Schema Definition
**File:** `src/store/schemas/sampler_v1.go` (NEW)

- Define `SamplerSchemaV1` constant
- Define `SamplerV1Schema` with all required fields
- Implement `ValidateSamplerSchema(tableSchema, version)` function
- Unit tests for schema validation

### Phase 2: Data Model Extension
**File:** `src/store/table.go`

- Add `Samplers map[string]*SamplerConfig` to `Table` struct
- Define `SamplerConfig` struct with all fields
- Update table serialization/deserialization

### Phase 3: API Specification
**File:** `src/metadata/api.yaml`

- Add 5 new endpoint definitions
- Add `CreateSamplerRequest` schema
- Add `PatchSamplerRequest` schema
- Add `SamplerConfig` response schema
- Run `make generate` to update client SDKs

### Phase 4: CRUD Handler Implementation
**File:** `src/metadata/api.go`

Implement handlers:
- `CreateSampler(w, r)` - Validate and create
  - Check source table exists
  - Check target table exists
  - Validate schema matches version
  - Validate config (rate, query types)
  - Check for cycles in sampler graph
  - Save to metadata

- `GetSampler(w, r)` - Return config and stats

- `PatchSampler(w, r)` - Update enabled/rate/filters
  - Validate new values
  - Update metadata

- `DeleteSampler(w, r)` - Remove sampler

- `ListSamplers(w, r)` - Return all samplers for table

### Phase 5: TableManager Extensions
**File:** `src/tablemgr/table.go`

Add methods:
```go
func (tm *TableManager) SaveSampler(table, name string, config *SamplerConfig) error
func (tm *TableManager) GetSampler(table, name string) (*SamplerConfig, error)
func (tm *TableManager) ListSamplers(table string) (map[string]*SamplerConfig, error)
func (tm *TableManager) DeleteSampler(table, name string) error
func (tm *TableManager) UpdateSamplerStats(table, name string, stats *SamplerStats) error
```

Storage operations use key pattern: `tm:t:{table}:s:{name}`

### Phase 6: Sampling Engine
**File:** `src/metadata/sampler.go` (NEW)

Core sampling logic:
```go
type QuerySampler struct {
    tm     *tablemgr.TableManager
    logger *zap.Logger
    rng    *rand.Rand
    mu     sync.Mutex
}

func NewQuerySampler(tm *tablemgr.TableManager, logger *zap.Logger) *QuerySampler

// Sampling decision (thread-safe random)
func (s *QuerySampler) ShouldSample(rate float64) bool

// Build sample document for v1 schema
func (s *QuerySampler) BuildSampleV1(
    samplerName, sourceTable string,
    queryReq *QueryRequest,
    embeddings map[string][]float32,
    result *QueryResult,
    latency time.Duration,
) map[string]interface{}

// Write sample to target table
func (s *QuerySampler) WriteSample(
    ctx context.Context,
    targetTable, targetIndex string,
    sample map[string]interface{},
) error

// Auto-disable sampler
func (s *QuerySampler) DisableSampler(
    ctx context.Context,
    tableName, samplerName, reason string,
) error

// Update sampler statistics
func (s *QuerySampler) UpdateStats(
    ctx context.Context,
    tableName, samplerName string,
    success bool,
    err error,
) error
```

Error classification helpers:
```go
func isSchemaError(err error) bool
func isTableNotFoundError(err error) bool
```

### Phase 7: Query Hook Integration
**File:** `src/metadata/api.go` in `runQuery()`

Add sampling after query execution:
```go
func (t *TableApi) runQuery(ctx context.Context, queryReq *QueryRequest) QueryResult {
    startTime := time.Now()

    // ... existing embedding generation ...

    // Execute query
    results, err := shardIndexes.RRFSearch(ctx, q)
    latency := time.Since(startTime)

    // Build result
    queryResult := QueryResult{...}

    // NEW: Process all samplers (async, non-blocking)
    go t.processSamplers(q.Table, queryReq, q.Embeddings, &queryResult, latency)

    return queryResult
}

func (t *TableApi) processSamplers(
    tableName string,
    queryReq *QueryRequest,
    embeddings map[string][]float32,
    result *QueryResult,
    latency time.Duration,
) {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    // Get all samplers
    samplers, err := t.tm.ListSamplers(tableName)
    if err != nil || len(samplers) == 0 {
        return
    }

    // Process each sampler independently
    for samplerName, sampler := range samplers {
        if !sampler.Enabled {
            continue
        }

        // Check query type filter
        if !shouldSampleQueryType(queryReq, sampler.QueryTypes) {
            continue
        }

        // Sampling decision
        if !t.sampler.ShouldSample(sampler.SampleRate) {
            continue
        }

        // Build sample based on schema version
        var sample map[string]interface{}
        switch sampler.SchemaVersion {
        case schemas.SamplerSchemaV1:
            sample = t.sampler.BuildSampleV1(
                samplerName, tableName, queryReq,
                embeddings, result, latency,
            )
        default:
            t.logger.Error("unknown sampler schema version",
                zap.String("version", sampler.SchemaVersion))
            continue
        }

        // Write sample
        err = t.sampler.WriteSample(ctx, sampler.TargetTable, sampler.TargetIndex, sample)

        // Handle errors
        if err != nil {
            t.logger.Warn("sample write failed",
                zap.String("sampler", samplerName),
                zap.Error(err))

            // Auto-disable on schema/table errors
            if isSchemaError(err) || isTableNotFoundError(err) {
                _ = t.sampler.DisableSampler(ctx, tableName, samplerName, err.Error())
            }
        }

        // Update stats
        _ = t.sampler.UpdateStats(ctx, tableName, samplerName, err == nil, err)
    }
}

func shouldSampleQueryType(req *QueryRequest, allowedTypes []string) bool {
    if len(allowedTypes) == 0 {
        return true // Sample all types
    }

    hasSemanticQuery := req.SemanticSearch != ""
    hasFullTextQuery := req.FilterQuery != ""

    for _, t := range allowedTypes {
        if t == "semantic" && hasSemanticQuery {
            return true
        }
        if t == "full_text" && hasFullTextQuery {
            return true
        }
    }
    return false
}
```

### Phase 8: Validation Logic
**File:** `src/metadata/api.go`

Implement validation functions:
```go
// Prevent circular sampler chains
func detectSamplerCycle(tm *tablemgr.TableManager, sourceTable, targetTable string) error {
    visited := make(map[string]bool)
    return detectCycleRecursive(tm, targetTable, sourceTable, visited)
}

func detectCycleRecursive(tm *tablemgr.TableManager, current, target string, visited map[string]bool) error {
    if current == target {
        return fmt.Errorf("circular sampler chain detected")
    }
    if visited[current] {
        return nil
    }
    visited[current] = true

    samplers, err := tm.ListSamplers(current)
    if err != nil {
        return err
    }

    for _, sampler := range samplers {
        if err := detectCycleRecursive(tm, sampler.TargetTable, target, visited); err != nil {
            return err
        }
    }
    return nil
}

// Validate sampler config
func validateSamplerConfig(config *CreateSamplerRequest, sourceTable string) error {
    // Validate sample rate
    if config.SampleRate < 0.0 || config.SampleRate > 1.0 {
        return fmt.Errorf("sample_rate must be between 0.0 and 1.0")
    }

    // Validate query types
    for _, qt := range config.QueryTypes {
        if qt != "semantic" && qt != "full_text" {
            return fmt.Errorf("invalid query_type: %s", qt)
        }
    }

    // Prevent self-sampling
    if config.TargetTable == sourceTable {
        return fmt.Errorf("table cannot sample to itself")
    }

    return nil
}
```

### Phase 9: Testing

#### Unit Tests
**File:** `src/metadata/sampler_test.go`

Tests:
- `TestShouldSample_Distribution` - Verify sample rate accuracy (statistical)
- `TestBuildSampleV1` - Verify document structure
- `TestValidateSamplerSchema` - Schema validation logic
- `TestDetectSamplerCycle` - Cycle detection
- `TestIsSchemaError` - Error classification
- `TestIsTableNotFoundError` - Error classification

#### Integration Tests
**File:** `src/metadata/api_test.go`

Tests:
- `TestCreateSampler_Success` - End-to-end creation
- `TestCreateSampler_SchemaMismatch` - Validation error
- `TestCreateSampler_CycleDetection` - Prevent cycles
- `TestPatchSampler_EnableDisable` - Toggle sampler
- `TestQueryWithSampling` - Full flow with sample write
- `TestAutoDisable_MissingTable` - Target deleted
- `TestAutoDisable_SchemaError` - Schema changed
- `TestMultipleSamplers` - Multiple samplers on same table
- `TestSamplerStats` - Stats tracking

#### Performance Tests
**File:** `src/metadata/api_bench_test.go`

Benchmarks:
- `BenchmarkQueryWithoutSampling` - Baseline
- `BenchmarkQueryWithSampling` - With 1 sampler
- `BenchmarkQueryWithMultipleSamplers` - With 5 samplers
- Verify <1ms overhead for sampling

### Phase 10: Documentation

#### CLAUDE.md
Add section:
```markdown
## Query Samplers

Samplers capture query embeddings and results for observability and ML training.

### Creating a Sampler

1. Create target table with sampler schema:
curl -X POST /api/v1/table -d '{
  "name": "query_samples",
  "schema": { ... antfly_sampler_v1 fields ... }
}'

2. Create sampler:
curl -X POST /api/v1/table/my_docs/sampler/ml_training -d '{
  "target_table": "query_samples",
  "schema_version": "antfly_sampler_v1",
  "sample_rate": 0.1,
  "query_types": ["semantic"]
}'

### Managing Samplers

# Disable temporarily
curl -X PATCH /api/v1/table/my_docs/sampler/ml_training -d '{"enabled": false}'

# Get stats
curl /api/v1/table/my_docs/sampler/ml_training

# List all samplers
curl /api/v1/table/my_docs/sampler

# Delete sampler
curl -X DELETE /api/v1/table/my_docs/sampler/ml_training

### Sampler Schema Versions

- `antfly_sampler_v1`: Initial version with query, embeddings, results
```

## Edge Cases & Error Handling

### Scenarios

1. **Target table deleted after sampler created**
   - WriteSample returns 404
   - Auto-disable sampler
   - Log warning with details
   - Operator can re-enable after recreating table

2. **Target table schema changed**
   - WriteSample returns 400 with validation error
   - Auto-disable sampler
   - Operator must recreate sampler with correct schema

3. **Circular sampler chains**
   - A samples to B, B samples to A
   - Detected at creation time
   - Return 400 error

4. **High sample rate with high QPS**
   - Sampling is async and non-blocking
   - Failed samples logged but don't fail queries
   - Monitor sampler error_count

5. **Multiple samplers with different rates**
   - Each sampler makes independent decision
   - Different samplers can capture different subsets
   - Example: 1% for monitoring, 10% for training

6. **Query with no embeddings**
   - semantic_search queries without embeddings field
   - Embeddings generated and cached
   - All enabled samplers capture the same embeddings

7. **Failed queries**
   - Queries that return errors still sampled
   - Sample includes error message
   - Useful for debugging query failures

## Future Extensions

### Phase 2: Full-Text Search Sampling
- Implement `shouldSampleQueryType` for "full_text"
- Capture BM25 scores and terms
- Extend schema or create v2

### Phase 3: Advanced Filtering
- Sample based on result_count thresholds
- Sample based on latency percentiles
- Sample based on user metadata

### Phase 4: Sampling Metrics
- Prometheus metrics:
  - `antfly_sampler_samples_total{table, sampler}`
  - `antfly_sampler_errors_total{table, sampler, reason}`
  - `antfly_sampler_disabled_total{table, sampler}`

### Phase 5: Resampling & Deduplication
- Deduplicate similar queries
- Resample based on diversity metrics

## Implementation Checklist

- [ ] Phase 1: Schema definition (`src/store/schemas/sampler_v1.go`)
- [ ] Phase 2: Data model (`src/store/table.go`)
- [ ] Phase 3: API spec (`src/metadata/api.yaml`)
- [ ] Phase 4: CRUD handlers (`src/metadata/api.go`)
- [ ] Phase 5: TableManager methods (`src/tablemgr/table.go`)
- [ ] Phase 6: Sampling engine (`src/metadata/sampler.go`)
- [ ] Phase 7: Query hook (`src/metadata/api.go::runQuery`)
- [ ] Phase 8: Validation logic
- [ ] Phase 9: Testing (unit, integration, performance)
- [ ] Phase 10: Documentation (CLAUDE.md)
- [ ] Run `make generate` to update SDKs
- [ ] Run full test suite
- [ ] Manual testing with example samplers

## Success Criteria

- [ ] Can create named samplers via API
- [ ] Samplers capture semantic_search queries at configured rate
- [ ] Sample documents match v1 schema exactly
- [ ] Auto-disable works when target missing
- [ ] Auto-disable works on schema mismatch
- [ ] Cycle detection prevents circular chains
- [ ] Multiple samplers work independently
- [ ] Sampling adds <1ms to query latency
- [ ] All tests pass
- [ ] Documentation complete
