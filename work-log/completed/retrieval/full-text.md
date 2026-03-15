# Full-Text Search Features

Antfly's full-text search capabilities are built on Bleve (BM25-based indexing). This document consolidates work across four related feature areas: full-text indexing for vector index chunks, cross-field `_all` configuration, dynamic templates for automatic field mapping, and aggregations for analytics on search results.

---

## Full-Text Chunks

Full-text indexing for document chunks created by vector indexes, enabling BM25 search over chunked content alongside vector similarity search.

### Design Overview

- Vector indexes with `chunker.full_text: {}` create chunks stored at `:i:<indexName>:cft:<chunkID>` in Pebble
- Vector indexes without `full_text` create chunks at `:i:<indexName>:c:<chunkID>` (existing behavior)
- Both chunk types use same persistence format: `[hashID:uint64][chunkJSON]`
- Chunk persistence already implemented in `db.go` -- handles both `:c:` and `:cft:` via `IsChunkKey()`
- Bleve fetches chunks from `:cft:` and adds them to parent document's `_chunks` object (keyed by index name)
- Search results include populated `_chunks: { <indexName>: [...] }` field (like `_summaries`)
- Chunks are indexed as part of parent document content, not as separate documents
- Vector index configs are immutable -- once `full_text: {}` is set, suffix choice (`:c:` vs `:cft:`) is permanent

### Key Findings

#### Bleve Mapping Immutability

Bleve index mappings cannot be modified after the index is created. Evidence from the codebase:

1. Mappings are created only during index creation/opening (`full_text_v0.go:299-410`)
2. `UpdateSchema()` only updates the in-memory schema reference, NOT the Bleve index mapping
3. Schema/mapping changes require a full rebuild with `rebuild: true`

#### Solution: Predefined Fields

Add `_chunks` as a predefined field in all Bleve mappings (following the `_summaries` pattern in `lib/schema/bleveutils.go:314-328`). This ensures:

- No rebuild required when introducing chunk support
- Forward compatibility with existing indexes
- Consistent with `_summaries` and `_timestamp` patterns

### Implementation Steps

#### 0. Add predefined `_chunks` field to Bleve mappings (lib/schema/bleveutils.go)

Add after the `_summaries` field definition (around line 328):

```go
// Add default _chunks field if not already present
// Structure: _chunks: { <indexName>: [ {chunk}, {chunk}, ... ], ... }
if _, hasChunks := properties["_chunks"]; !hasChunks {
	properties["_chunks"] = map[string]any{
		"type": "object",
		"additionalProperties": map[string]any{
			"type": "array",
			"items": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"_id":         {"type": "string", "x-antfly-types": []string{"keyword"}},
					"_start_char": {"type": "integer", "x-antfly-types": []string{"numeric"}},
					"_end_char":   {"type": "integer", "x-antfly-types": []string{"numeric"}},
					"_content":    {"type": "string", "x-antfly-types": []string{"text"}},
				},
			},
		},
	}
}
```

The `_matched` field is not included in the initial implementation. Future enhancement will add logic to identify which specific chunks matched the search query.

#### 1. Add `:cft:` suffix pattern (src/store/storeutils/)

- Add `ChunkingFullTextSuffix = []byte{':', 'c', 'f', 't'}` constant
- Add `MakeChunkFullTextKey(docKey, indexName, chunkID)` helper
- Add `MakeChunkFullTextPrefix(docKey, indexName)` for iteration
- Update `IsChunkKey()` to check for BOTH `:c:` and `:cft:` patterns (existing db.go persistence will work for both)
- Update key parsing/decoding functions to recognize `:cft:` suffix

#### 2. Update ChunkingEnricher to support full-text chunks (src/store/indexes/chunkingenricher.go)

- Add `full_text` object field to `ChunkerConfig` in openapi.yaml (initially empty `{}`, allows future options like boosting, field mapping, etc.)
- Modify `persistChunks` to:
  - Store chunks with `:cft:` suffix when `full_text` is present (non-nil)
  - Store chunks with `:c:` suffix when `full_text` is absent (existing behavior for vector-only)
  - Use same persistence format for both: `[hashID:uint64][chunkJSON]`
- Update backfill logic:
  - Check for first chunk at appropriate suffix (`:cft:0` vs `:c:0`) based on config
  - Vector index configs are immutable, so suffix choice is permanent once set
- Update vector embedding enricher to read chunks from `:cft:` when `full_text` is configured, otherwise from `:c:`

#### 3. Extend Bleve to fetch and index chunks (src/store/indexes/full_text_v0.go)

- Update `GetDocument()` calls to always fetch chunks from `:cft:` suffix (similar to how summaries are fetched)
- Modify document preparation before indexing:
  - Fetch all `:cft:` chunks across all indexes (no need to check `full_text` config)
  - Build `_chunks` object: `{ <indexName>: [ {_id, _start_char, _end_char, _content}, ... ], ... }`
  - Add `_chunks` to document map before calling `MapDocument()`
- Update backfill (`ScanForBackfill`) to include chunk fetching and population
- Chunks are indexed as part of parent document (Bleve indexes the `_content` fields within the array)

#### 4. Enhance field projection for nested/wildcard support (src/store/graph_query.go)

- Extend existing `fields` parameter to support JSONPath/wildcard patterns for nested objects:
  - `_chunks.*._content` - All chunk content across all indexes
  - `_chunks.<indexName>._id` - Chunk IDs from specific index
  - `_chunks.*` - All chunk fields
- Default behavior when `_content` not requested: Exclude it from chunk objects to reduce response size
- Apply projection after document fetch, before returning results

#### 5. Return chunks in search results (src/store/indexes/full_text_v0.go)

- Search results automatically include `_chunks` object (it's part of the indexed parent document)
- Field projection handles chunk field selection via patterns like `_chunks.*._content`
- Initial implementation: All chunks returned (no `_matched` filtering)
- Future enhancement: Add logic to determine which chunks matched and filter/flag them

#### 6. Support chunk-focused queries (documentation/examples)

- Users can query chunk content using Bleve query syntax targeting `_chunks.<indexName>._content` fields
- Standard Bleve queries work: full-text search, phrase queries, boolean combinations, etc.
- No special API needed -- handled through normal Bleve query capabilities

#### 7. Update OpenAPI schemas (lib/chunking/openapi.yaml, src/store/indexes/openapi.yaml)

- Add `full_text` object field to `ChunkerConfig` (type: object, properties: {}, additionalProperties: true for future options)
- Update search response schema to document `_chunks` field structure
- Document field projection patterns for chunks (e.g., `fields: ["_chunks.*._content"]`)
- Document chunk query patterns and examples
- Run `make generate`

#### 8. Add tests

- Unit tests for `:cft:` key generation/parsing (storeutils_test.go)
- Integration tests for chunk creation with `full_text: {}` configured
- Tests verifying vector embedding enricher reads from correct suffix (`:c:` vs `:cft:`)
- Search tests validating `_chunks` object is populated in results
- Tests for field projection with chunk patterns (e.g., `_chunks.*._content`)
- Tests for querying chunk content via Bleve query syntax
- Backfill tests ensuring chunks are fetched and indexed with parent documents

### Key Files

- `lib/schema/bleveutils.go` - Add predefined `_chunks` field to all Bleve mappings
- `src/store/storeutils/storeutils.go` - Add `:cft:` suffix constants and helpers, update `IsChunkKey()`
- ~~`src/store/db.go`~~ - Already done! Chunk persistence implemented via `IsChunkKey()`
- `src/store/indexes/chunkingenricher.go` - Support `:cft:` storage when `full_text` present
- `lib/chunking/openapi.yaml` - Add `full_text` object field
- `src/store/indexes/full_text_v0.go` - Fetch, index, and aggregate chunks
- `src/store/graph_query.go` - Enhance field projection for nested/wildcard patterns
- `src/store/indexes/openapi.yaml` - Update response schemas
- `src/store/storeutils/query.go` - Add chunk fetching support for `:cft:` suffix

### User Requirements Summary

1. **Bleve structure:** Chunks stored in `_chunks: { <indexName>: [...] }` object on parent document (NOT separate Bleve documents)
2. **Storage suffix:** New suffix `:cft:` for full-text chunks (vs existing `:c:` for vector-only chunks)
3. **Config location:** `full_text: {}` object on `ChunkerConfig` (on vector indexes, determines `:c:` vs `:cft:` suffix)
4. **Chunk aggregation:** Bleve always fetches `:cft:` chunks (like summaries), no need to check `full_text` config
5. **Search results:** `_chunks` object always returned when chunks exist:
   - Field projection controls chunk fields: Use `fields` parameter with JSONPath/wildcard patterns
   - Default: Exclude `_content` to reduce response size (return only `{_id, _start_char, _end_char}`)
   - Examples: `fields: ["_chunks.*._content"]`, `fields: ["_chunks.*"]`, `fields: ["title", "_chunks.my_index._id"]`
   - No `_matched` in v1: Future enhancement to identify which chunks matched
6. **Chunk queries:** Users query chunk content via Bleve syntax (e.g., `_chunks.<indexName>._content:"search term"`)
7. **Predefined fields:** Add `_chunks` as predefined field in Bleve mappings (like `_summaries`) to avoid index rebuilds
8. **Field projection enhancement:** Extend existing `fields` parameter to support JSONPath/wildcard patterns for nested field selection
9. **No limits in v1:** No max chunk limit per index for initial implementation

---

## All Field Configuration

Schema-level configuration for Bleve `_all` field via `x-antfly-include-in-all`, enabling full-text search across multiple fields without explicitly specifying them in queries.

### Configuration Format

```json
{
  "type": "object",
  "x-antfly-include-in-all": ["title", "description", "tags"],
  "properties": {
    "title": {"type": "string"},
    "description": {"type": "string"},
    "tags": {"type": "string"},
    "id": {"type": "string"}
  }
}
```

### Design Decisions

#### 1. Schema-Level Array

Following JSON Schema conventions (like `required`), the configuration is at the schema level rather than per-field:
- **Format**: `x-antfly-include-in-all: ["field1", "field2"]`
- **Rationale**: Cleaner, easier to maintain, see all included fields in one place

#### 2. Text Fields Only

Only text-based field types are supported:
- Supported: `text`, `keyword`, `search_as_you_type`, `link`
- Rejected: `numeric`, `boolean`, `datetime`, `geopoint`, `geoshape`, `embedding`, `blob`
- Non-text fields in the array are silently skipped with warning logs

#### 3. Multi-Type Field Handling

For fields with multiple types (e.g., `x-antfly-types: ["text", "keyword", "search_as_you_type"]`):
- Only the **primary text field** is included in `_all`
- Suffix variants (`__keyword`, `__2gram`) are excluded
- **Example**:
  ```
  name (text) -> IncludeInAll = true
  name__keyword -> IncludeInAll = false
  name__2gram -> IncludeInAll = false
  ```
- **Rationale**: Avoids duplication while making field searchable

#### 4. Auto-Enable _all Mapping

The `_all` document mapping is automatically enabled when any field uses it:
- No `x-antfly-include-in-all` -> `_all` disabled
- Empty array `x-antfly-include-in-all: []` -> `_all` disabled
- Non-empty array -> `_all` enabled
- **Rationale**: User-friendly, no need for separate enable flag

### Implementation

#### Backend Changes

**Files Modified**:
- `src/store/indexes/openapi.go` - Added `XAntflyIncludeInAll` constant
- `src/store/indexes/bleve.go` - Core implementation:
  - Modified `buildMappingFromJSONSchema` to accept `includeInAll []string` parameter
  - Updated `NewIndexMapFromSchema` to extract and apply configuration
  - Conditionally enable `_all` mapping based on usage
- `src/store/indexes/bleve_test.go` - Comprehensive test suite

**Key Functions**:
```go
// Updated signature
func buildMappingFromJSONSchema(
    schema map[string]any,
    includeInAll []string,
) (*mapping.DocumentMapping, bool, bool)
// Returns: (docMapping, searchAsYouTypeNeeded, allFieldUsed)

// Extraction in NewIndexMapFromSchema
var includeInAll []string
if includeInAllI, ok := docSchema.Schema[XAntflyIncludeInAll]; ok {
    switch v := includeInAllI.(type) {
    case []any:
        for _, fieldName := range v {
            if fieldStr, ok := fieldName.(string); ok {
                includeInAll = append(includeInAll, fieldStr)
            }
        }
    case []string:
        includeInAll = v
    }
}

// Enable _all mapping conditionally
allDocumentMapping := bleve.NewDocumentMapping()
allDocumentMapping.Enabled = allFieldUsed
indexMapping.AddDocumentMapping("_all", allDocumentMapping)
```

#### Frontend Integration

**Antfarm Schema Builder** (`antfarm/src/components/schema-builder/`):
- Schema-level multi-select control for text-based fields
- Visual indicators (badges/icons) on included fields
- Only text field types shown as selectable options
- Zod schema validation for `string[]` at schema level

### Query Behavior

**Default queries search `_all`**:
```
Query: "smartphone"
-> Searches _all field by default (if enabled)
-> Matches documents with "smartphone" in title, description, or tags
```

**Explicit `_all` queries**:
```
Query: "_all:smartphone"
-> Explicitly searches _all field
```

**Excluded fields not searchable via `_all`**:
```
Query: "_all:SKU-12345"
-> No match if "SKU" field not in x-antfly-include-in-all
```

### Testing

#### Test Coverage

1. **TestIncludeInAllBasic** - Text fields included in _all
2. **TestIncludeInAllTextTypesOnly** - Non-text types silently skipped
3. **TestIncludeInAllMultiType** - Only primary text field included (not suffixes)
4. **TestIncludeInAllEnablesMapping** - Auto-enable _all based on usage
5. **TestIncludeInAllQueries** - Query results correctness
6. **TestIncludeInAllNested** - Nested object behavior

#### Edge Cases Tested

- Empty `x-antfly-include-in-all` array
- References to non-existent fields
- Invalid type (not array)
- Mixed valid/invalid field types
- Nested object fields

### Example Usage

#### Schema Definition

```json
{
  "type": "object",
  "x-antfly-include-in-all": ["title", "description", "content"],
  "properties": {
    "title": {
      "type": "string",
      "x-antfly-types": ["text", "keyword"]
    },
    "description": {
      "type": "string",
      "x-antfly-types": ["text"]
    },
    "content": {
      "type": "string",
      "x-antfly-types": ["text"]
    },
    "sku": {
      "type": "string",
      "x-antfly-types": ["keyword"]
    },
    "price": {
      "type": "number",
      "x-antfly-types": ["numeric"]
    }
  }
}
```

#### Query Examples

```bash
# Search all included fields
curl -X POST /api/v1/tables/products/query \
  -d '{"full_text_search": {"query": "smartphone"}}'

# Explicit _all field query
curl -X POST /api/v1/tables/products/query \
  -d '{"full_text_search": {"query": "_all:smartphone"}}'

# Search specific field (bypasses _all)
curl -X POST /api/v1/tables/products/query \
  -d '{"full_text_search": {"query": "title:smartphone"}}'
```

### References

- Implementation: `src/store/indexes/bleve.go` (buildMappingFromJSONSchema, NewIndexMapFromSchema)
- Tests: `src/store/indexes/bleve_test.go`
- Configuration: `src/store/indexes/openapi.go` (XAntflyIncludeInAll constant)
- Frontend: `antfarm/src/components/schema-builder/`

---

## Dynamic Templates

Support for Bleve dynamic templates, allowing automatic field mapping based on patterns without requiring full index rebuilds when templates change.

### Background

Dynamic templates are rules for mapping fields that don't have explicit mappings when dynamic indexing is enabled. They evaluate matching criteria (field name patterns, path patterns, type filters) to automatically apply the correct field mapping at index time.

Key insight: Template changes only affect **future documents** -- they don't require reindexing existing data. This differs from document schema changes which may require a full rebuild.

### Goals

1. Add `dynamic_templates` field to `TableSchema`
2. Translate antfly templates to bleve `DynamicTemplate` structs
3. Update existing indexes in-place when only templates change (no version bump, no rebuild)
4. Leverage bleve's `OpenUsing` with `updated_mapping` for hot-reload

### Non-Goals

- Retroactive reindexing when templates change
- Template inheritance across tables
- Custom analyzer definitions (use existing analyzers)

### Schema Changes

Add `dynamic_templates` to `TableSchema` in `lib/schema/openapi.yaml`:

```yaml
TableSchema:
  properties:
    version:
      type: integer
      format: uint32
    document_schemas:
      # ... existing
    dynamic_templates:
      type: array
      items:
        $ref: "#/components/schemas/DynamicTemplate"

DynamicTemplate:
  type: object
  properties:
    name:
      type: string
      description: Optional identifier for the template
    match:
      type: string
      description: Glob pattern for field name (e.g., "*_text")
    unmatch:
      type: string
      description: Exclusion pattern for field name
    path_match:
      type: string
      description: Full dotted path pattern (e.g., "metadata.**")
    path_unmatch:
      type: string
      description: Path exclusion pattern
    match_mapping_type:
      type: string
      enum: [string, number, boolean, date, object]
      description: Filter by detected JSON type
    mapping:
      $ref: "#/components/schemas/TemplateFieldMapping"

TemplateFieldMapping:
  type: object
  properties:
    type:
      type: string
      enum: [text, keyword, numeric, boolean, datetime, geopoint]
    analyzer:
      type: string
      description: Analyzer name (e.g., "standard", "keyword", "html_analyzer")
    index:
      type: boolean
      default: true
    include_in_all:
      type: boolean
      default: false
```

### Mapping Translation

Update `lib/schema/bleveutils.go` to translate antfly templates to bleve:

```go
func applyDynamicTemplates(indexMapping *mapping.IndexMappingImpl, templates []DynamicTemplate) {
    for _, t := range templates {
        bleveTemplate := mapping.NewDynamicTemplate()

        if t.Match != "" {
            bleveTemplate.MatchField(t.Match)
        }
        if t.Unmatch != "" {
            bleveTemplate.UnmatchField(t.Unmatch)
        }
        if t.PathMatch != "" {
            bleveTemplate.MatchPath(t.PathMatch)
        }
        if t.PathUnmatch != "" {
            bleveTemplate.UnmatchPath(t.PathUnmatch)
        }
        if t.MatchMappingType != "" {
            bleveTemplate.MatchMappingType(t.MatchMappingType)
        }

        fieldMapping := translateFieldMapping(t.Mapping)
        bleveTemplate.WithMapping(fieldMapping)

        indexMapping.DefaultMapping.AddDynamicTemplate(t.Name, bleveTemplate)
    }
}
```

### Index Update Flow

#### TableManager: No version bump for template-only changes

```go
// In tablemgr/table.go UpdateSchema()
func (tm *TableManager) UpdateSchema(tableName string, tableSchema *schema.TableSchema) (*store.Table, error) {
    // ...existing code...

    prevVersion := uint32(0)
    if table.Schema != nil {
        prevVersion = table.Schema.Version
    }

    // Only bump version if document schemas changed
    if documentSchemasChanged(table.Schema, tableSchema) {
        tableSchema.Version = prevVersion + 1
        // ... existing versioned index creation logic
    } else {
        // Template-only change - keep same version
        tableSchema.Version = prevVersion
        // No new indexes created, existing ones will update in-place
    }

    // ... rest of method
}

func documentSchemasChanged(old, new *schema.TableSchema) bool {
    if old == nil || new == nil {
        return old != new
    }
    // Compare document_schemas only, ignore dynamic_templates
    return !reflect.DeepEqual(old.DocumentSchemas, new.DocumentSchemas)
}
```

#### BleveIndexV2: Hot-reload via OpenUsing

```go
// In src/store/indexes/full_text_v0.go
func (bi *BleveIndexV2) UpdateSchema(newSchema *schema.TableSchema) error {
    oldSchema := bi.schema
    bi.schema = newSchema

    // Check if we need to update the bleve mapping
    if !templatesEqual(oldSchema, newSchema) {
        return bi.reloadMapping(newSchema)
    }
    return nil
}

func (bi *BleveIndexV2) reloadMapping(newSchema *schema.TableSchema) error {
    // Generate new mapping with updated templates
    newMapping := schema.NewIndexMapFromSchema(newSchema)
    mappingBytes, err := json.Marshal(newMapping)
    if err != nil {
        return fmt.Errorf("marshaling new mapping: %w", err)
    }

    // Close current index
    if err := bi.bidx.Close(); err != nil {
        return fmt.Errorf("closing index for mapping update: %w", err)
    }

    // Reopen with updated mapping
    bleveIndexPath := bi.indexPath + "/bleve"
    bi.bidx, err = bleve.OpenUsing(bleveIndexPath, map[string]interface{}{
        "updated_mapping": string(mappingBytes),
    })
    if err != nil {
        return fmt.Errorf("reopening index with updated mapping: %w", err)
    }

    bi.logger.Info("Reloaded bleve index with updated mapping",
        zap.String("path", bleveIndexPath))
    return nil
}

func templatesEqual(old, new *schema.TableSchema) bool {
    if old == nil && new == nil {
        return true
    }
    if old == nil || new == nil {
        return false
    }
    return reflect.DeepEqual(old.DynamicTemplates, new.DynamicTemplates)
}
```

### Bleve Mapping Update Constraints

Bleve validates mapping updates via `DeletedFields()`. The update will fail if:
- Existing indexed fields are removed
- Field types are changed incompatibly

Template updates are safe because they only affect **future** dynamic field mappings.

### Implementation Phases

1. **Schema & Types** - Add `DynamicTemplate` and `TemplateFieldMapping` to `lib/schema/openapi.yaml`, run `make generate`, update `TableSchema` struct
2. **Mapping Translation** - Add `applyDynamicTemplates()` to `lib/schema/bleveutils.go`, update `NewIndexMapFromSchema()`, unit tests
3. **Index Hot-Reload** - Add `reloadMapping()` to `BleveIndexV2`, update `UpdateSchema()`, error handling with rollback
4. **TableManager Integration** - Add `documentSchemasChanged()`, skip version bump for template-only changes
5. **Testing** - Template matching patterns, update-without-rebuild, new docs use updated templates, existing docs unchanged
6. **Documentation** - API docs, SDK examples, pattern syntax docs

### Example Usage

#### API Request

```json
{
  "document_schemas": {
    "article": {
      "schema": {
        "type": "object",
        "properties": {
          "title": {"type": "string", "x-antfly-types": ["text"]}
        },
        "additionalProperties": true
      }
    }
  },
  "dynamic_templates": [
    {
      "name": "text_fields",
      "match": "*_text",
      "mapping": {
        "type": "text",
        "analyzer": "standard"
      }
    },
    {
      "name": "keyword_fields",
      "match": "*_id",
      "mapping": {
        "type": "keyword"
      }
    },
    {
      "name": "skip_internal",
      "path_match": "_internal.**",
      "mapping": {
        "index": false
      }
    }
  ]
}
```

#### Behavior

1. `title` field: Explicitly mapped via document schema
2. `author_text` field: Matches `*_text` -> text with standard analyzer
3. `category_id` field: Matches `*_id` -> keyword
4. `_internal.debug` field: Matches `_internal.**` -> not indexed
5. Other dynamic fields: Default bleve dynamic mapping

### Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Bleve rejects mapping update | Wrap in transaction, rollback on error |
| Brief unavailability during reload | Index is closed briefly; consider read-only mode during update |
| Template patterns too permissive | Document pattern syntax clearly; validate patterns on API |
| Conflict with explicit field mappings | Explicit mappings always take precedence (bleve behavior) |

### Dependencies

- `github.com/blevesearch/bleve/v2` (antflydb fork with dynamic templates)
- `github.com/bmatcuk/doublestar/v4` (transitive via bleve for glob patterns)

### References

- Bleve dynamic templates docs: `antflydb/bleve/docs/dynamic-templates.md`
- Elasticsearch dynamic templates: https://www.elastic.co/guide/en/elasticsearch/reference/current/dynamic-templates.html

---

## Aggregations

Support for Bleve aggregations in Antfly's search API, enabling analytics on search results including metrics (sum, avg, cardinality) and bucket aggregations (terms, histograms, date ranges).

### Background

Aggregations compute analytics over search results during query execution. They run inline during document collection with zero additional I/O overhead, piggybacking on field value visits.

Antfly currently has basic faceting support (`TermFacetResult` in the API). Aggregations augment/replace this with more flexible, composable analytics.

### Aggregation Types

**Metric Aggregations** (single numeric value):
- `sum`, `avg`, `min`, `max`, `count`
- `stats` (comprehensive: count, sum, avg, min, max, variance, stddev)
- `cardinality` (approximate unique count via HyperLogLog++)

**Bucket Aggregations** (group documents):
- `terms` - Group by unique field values
- `range` - Numeric ranges
- `date_range` - Custom date ranges
- `histogram` - Fixed-interval numeric buckets
- `date_histogram` - Time interval buckets
- `significant_terms` - Uncommonly common terms in results
- `geohash_grid` - Geographic grid cells
- `geo_distance` - Distance ranges from center point

**Sub-Aggregations**: Bucket aggregations support nested sub-aggregations for multi-level analytics (e.g., by region -> by category -> sum price).

### Goals

1. Extend search request schema to include aggregations
2. Translate antfly aggregation requests to bleve aggregations
3. Return aggregation results alongside search hits
4. Support distributed aggregation merging across shards

### Non-Goals

- Pipeline aggregations (aggregations on aggregation results)
- Scripted aggregations
- Real-time streaming aggregations

### API Schema Changes

Add aggregation types to `src/metadata/api.yaml`:

```yaml
components:
  schemas:
    SearchRequest:
      properties:
        # ... existing fields ...
        aggregations:
          type: object
          additionalProperties:
            $ref: "#/components/schemas/AggregationRequest"
          description: Named aggregations to compute over search results

    AggregationRequest:
      type: object
      required:
        - type
      properties:
        type:
          type: string
          enum:
            - sum
            - avg
            - min
            - max
            - count
            - stats
            - cardinality
            - terms
            - range
            - date_range
            - histogram
            - date_histogram
          description: Aggregation type
        field:
          type: string
          description: Field to aggregate on
        size:
          type: integer
          description: Max buckets to return (for bucket aggregations)
        interval:
          type: number
          description: Interval for histogram aggregations
        calendar_interval:
          type: string
          enum: [minute, hour, day, week, month, quarter, year]
          description: Calendar interval for date_histogram
        ranges:
          type: array
          items:
            $ref: "#/components/schemas/AggregationRange"
          description: Ranges for range/date_range aggregations
        precision:
          type: integer
          minimum: 10
          maximum: 18
          default: 14
          description: HyperLogLog precision for cardinality (higher = more accurate, more memory)
        aggregations:
          type: object
          additionalProperties:
            $ref: "#/components/schemas/AggregationRequest"
          description: Sub-aggregations (for bucket aggregations only)

    AggregationRange:
      type: object
      properties:
        name:
          type: string
          description: Optional name for the range bucket
        from:
          description: Start of range (inclusive)
        to:
          description: End of range (exclusive)

    SearchResponse:
      properties:
        # ... existing fields ...
        aggregations:
          type: object
          additionalProperties:
            $ref: "#/components/schemas/AggregationResult"

    AggregationResult:
      type: object
      properties:
        field:
          type: string
        type:
          type: string
        value:
          description: Result value for metric aggregations
        buckets:
          type: array
          items:
            $ref: "#/components/schemas/AggregationBucket"
          description: Buckets for bucket aggregations

    AggregationBucket:
      type: object
      properties:
        key:
          description: Bucket key (string, number, or date)
        doc_count:
          type: integer
          description: Number of documents in bucket
        aggregations:
          type: object
          additionalProperties:
            $ref: "#/components/schemas/AggregationResult"
          description: Sub-aggregation results
```

### Translation Layer

Add aggregation translation in `src/store/indexes/full_text_v0.go`:

```go
func translateAggregations(req map[string]AggregationRequest) (*search.AggregationsRequest, error) {
    if len(req) == 0 {
        return nil, nil
    }

    aggs := make(search.AggregationsRequest)
    for name, aggReq := range req {
        agg, err := translateAggregation(aggReq)
        if err != nil {
            return nil, fmt.Errorf("aggregation %s: %w", name, err)
        }
        aggs[name] = agg
    }
    return &aggs, nil
}

func translateAggregation(req AggregationRequest) (*search.AggregationRequest, error) {
    agg := &search.AggregationRequest{
        Type:  req.Type,
        Field: req.Field,
    }

    if req.Size != nil {
        agg.Size = req.Size
    }

    switch req.Type {
    case "terms":
        // Terms aggregation - no additional config needed

    case "histogram":
        if req.Interval == nil {
            return nil, fmt.Errorf("histogram requires interval")
        }
        agg.Interval = *req.Interval

    case "date_histogram":
        if req.CalendarInterval != "" {
            agg.CalendarInterval = req.CalendarInterval
        } else if req.Interval != nil {
            agg.FixedInterval = time.Duration(*req.Interval) * time.Millisecond
        } else {
            return nil, fmt.Errorf("date_histogram requires interval or calendar_interval")
        }

    case "range", "date_range":
        if len(req.Ranges) == 0 {
            return nil, fmt.Errorf("%s requires ranges", req.Type)
        }
        for _, r := range req.Ranges {
            agg.AddRange(r.Name, r.From, r.To)
        }

    case "cardinality":
        if req.Precision != nil {
            agg.Precision = *req.Precision
        }

    case "sum", "avg", "min", "max", "count", "stats":
        // Metric aggregations - field only
    }

    // Translate sub-aggregations recursively
    if len(req.Aggregations) > 0 {
        subAggs, err := translateAggregations(req.Aggregations)
        if err != nil {
            return nil, fmt.Errorf("sub-aggregations: %w", err)
        }
        agg.Aggregations = *subAggs
    }

    return agg, nil
}
```

### Distributed Aggregation Merging

Aggregations must be merged across shards in scatter-gather queries. Bleve provides `AggregationResults.Merge()`:

```go
// In metadata/api.go or wherever scatter-gather happens
func mergeSearchResults(shardResults []*bleve.SearchResult, limit int) *bleve.SearchResult {
    if len(shardResults) == 0 {
        return nil
    }

    merged := shardResults[0]
    for _, result := range shardResults[1:] {
        // Merge hits (existing logic)
        merged.Hits = append(merged.Hits, result.Hits...)
        merged.Total += result.Total

        // Merge aggregations
        if result.Aggregations != nil {
            if merged.Aggregations == nil {
                merged.Aggregations = result.Aggregations
            } else {
                merged.Aggregations.Merge(result.Aggregations)
            }
        }
    }

    // Sort and limit hits
    sort.Slice(merged.Hits, func(i, j int) bool {
        return merged.Hits[i].Score > merged.Hits[j].Score
    })
    if len(merged.Hits) > limit {
        merged.Hits = merged.Hits[:limit]
    }

    return merged
}
```

Bleve's merge handles:
- Metric aggregations: sums added, mins/maxs compared, avgs recalculated
- Bucket aggregations: counts summed, sub-aggregations merged recursively
- Cardinality: HyperLogLog sketches merge correctly

### Response Translation

Translate bleve results back to API response:

```go
func translateAggregationResults(bleveAggs search.AggregationResults) map[string]AggregationResult {
    results := make(map[string]AggregationResult)

    for name, agg := range bleveAggs {
        result := AggregationResult{
            Field: agg.Field,
            Type:  agg.Type,
        }

        if agg.Value != nil {
            result.Value = agg.Value
        }

        if len(agg.Buckets) > 0 {
            result.Buckets = make([]AggregationBucket, len(agg.Buckets))
            for i, bucket := range agg.Buckets {
                result.Buckets[i] = AggregationBucket{
                    Key:      bucket.Key,
                    DocCount: bucket.Count,
                }
                if len(bucket.Aggregations) > 0 {
                    result.Buckets[i].Aggregations = translateAggregationResults(bucket.Aggregations)
                }
            }
        }

        results[name] = result
    }

    return results
}
```

### Implementation Phases

1. **API Schema** - Add aggregation types to `src/metadata/api.yaml`, run `make generate`, update SDK types
2. **Translation Layer** - Add `translateAggregations()` and `translateAggregationResults()`, unit tests
3. **Search Integration** - Update `BleveIndexV2.Search()`, update request parsing, integration tests for single-shard
4. **Distributed Merging** - Update scatter-gather logic, call `Merge()` on aggregation results, multi-shard tests
5. **Facet Migration** (optional) - Deprecate existing facet API, add migration guide, maintain backward compatibility
6. **Documentation** - API docs, aggregation type use cases, SDK examples, distributed merge behavior

### Example Usage

#### Request

```json
{
  "query": {"match": {"content": "database"}},
  "size": 10,
  "aggregations": {
    "categories": {
      "type": "terms",
      "field": "category",
      "size": 10
    },
    "price_ranges": {
      "type": "range",
      "field": "price",
      "ranges": [
        {"name": "cheap", "to": 100},
        {"name": "mid", "from": 100, "to": 500},
        {"name": "expensive", "from": 500}
      ]
    },
    "avg_price": {
      "type": "avg",
      "field": "price"
    },
    "monthly_sales": {
      "type": "date_histogram",
      "field": "sale_date",
      "calendar_interval": "month",
      "aggregations": {
        "total_revenue": {
          "type": "sum",
          "field": "price"
        }
      }
    }
  }
}
```

#### Response

```json
{
  "hits": [],
  "total": 1250,
  "aggregations": {
    "categories": {
      "field": "category",
      "type": "terms",
      "buckets": [
        {"key": "electronics", "doc_count": 450},
        {"key": "books", "doc_count": 320},
        {"key": "clothing", "doc_count": 280}
      ]
    },
    "price_ranges": {
      "field": "price",
      "type": "range",
      "buckets": [
        {"key": "cheap", "doc_count": 600},
        {"key": "mid", "doc_count": 500},
        {"key": "expensive", "doc_count": 150}
      ]
    },
    "avg_price": {
      "field": "price",
      "type": "avg",
      "value": 245.50
    },
    "monthly_sales": {
      "field": "sale_date",
      "type": "date_histogram",
      "buckets": [
        {
          "key": "2025-01-01T00:00:00Z",
          "doc_count": 400,
          "aggregations": {
            "total_revenue": {"value": 98000}
          }
        },
        {
          "key": "2025-02-01T00:00:00Z",
          "doc_count": 450,
          "aggregations": {
            "total_revenue": {"value": 112500}
          }
        }
      ]
    }
  }
}
```

### Performance Considerations

| Aggregation Type | Memory | Time Complexity |
|-----------------|--------|-----------------|
| Metric (sum, avg, etc.) | O(1) | O(matching docs) |
| Terms | O(unique terms) | O(matching docs) |
| Histogram | O(buckets) | O(matching docs) |
| Cardinality | O(2^precision) | O(matching docs) |

**Optimization tips:**
- Use query filtering to reduce matching documents before aggregation
- Limit `size` for terms aggregations
- Choose appropriate cardinality precision (14 = 16KB, 0.81% error)

### Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Memory pressure from large terms aggs | Enforce `size` limits, document best practices |
| Slow queries with many nested aggs | Limit nesting depth, add query timeout |
| Cardinality accuracy across shards | HLL++ merges correctly; document precision tradeoffs |
| Breaking change to search response | Add `aggregations` field (additive), keep existing fields |

### Dependencies

- `github.com/blevesearch/bleve/v2` (antflydb fork with aggregations)
- Existing scatter-gather infrastructure in `src/metadata/`

### References

- Bleve aggregations docs: `antflydb/bleve/docs/aggregations.md`
- Elasticsearch aggregations: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html
