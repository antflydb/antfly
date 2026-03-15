# Document Update DML

MongoDB-style transform operations (`$set`, `$inc`, `$push`, etc.) for Antfly's batch API, enabling efficient in-place document updates without read-modify-write races. This feature addresses common use cases like incrementing counters, appending to arrays, partial field updates, and array deduplication/filtering -- all without requiring a full document replacement.

---

## Design Decisions

### 1. Transform Operation (Not Simple UPDATE)

**Decision:** Implement MongoDB-style `Transform` operations with multiple operation types, not RFC 7386 JSON Merge Patch.

**Rationale:**
- JSON Merge Patch cannot handle array manipulation (append, remove elements)
- Cannot increment counters atomically
- No path-based operations
- Transform operations align with Antfly's existing JSONPath usage
- More powerful and future-proof

**Alternatives Considered:**
- Simple UPDATE with JSON Merge Patch: Too limited
- Custom DSL: Reinventing the wheel, poor DX

### 2. MongoDB Operator Alignment

**Decision:** Use exact MongoDB operator names and semantics.

**Operators to implement:**
- `$set` - Set value at path
- `$unset` - Remove field
- `$inc` - Increment/decrement numeric value
- `$push` - Append to array
- `$pull` - Remove array elements matching value
- `$addToSet` - Add to array if not exists (dedupe)
- `$pop` - Remove first (-1) or last (1) array element
- `$mul` - Multiply numeric value
- `$min` - Set to minimum of current and value
- `$max` - Set to maximum of current and value
- `$currentDate` - Set to current timestamp
- `$rename` - Rename field

**Rationale:**
- Developers already familiar with MongoDB will find it intuitive
- Well-documented semantics
- Covers 95%+ of real-world use cases
- Easy to add more operators later (e.g., `$bit`, `$pushAll`, etc.)

**Alternatives Considered:**
- Custom operator names: Poor DX, documentation burden
- PostgreSQL jsonb operations: Less comprehensive for NoSQL use cases

### 3. Protobuf Structure: `repeated Transform`

**Decision:** Use `repeated Transform` in `BatchOp`, matching the `repeated Write` pattern.

```protobuf
message Transform {
  bytes key = 1;
  repeated TransformOp operations = 2;
}

message BatchOp {
  repeated Write writes = 1;
  repeated bytes deletes = 2;
  repeated Transform transforms = 3;  // NEW
  KvstoreOp.SyncLevel sync_level = 4;
}
```

**Rationale:**
- Consistent with existing `Write` pattern
- Clear semantics: one Transform per key with multiple operations
- Simple batch merging logic
- Operations applied in sequence per key

**Alternatives Considered:**
- `map<string, TransformOpList>`: More efficient lookup, but inconsistent with existing patterns
- Flat `repeated TransformOp` with embedded keys: Harder to merge, unclear grouping

### 4. Conflict Resolution Strategy

**Decision:** Last-write-wins semantics with operation-specific merging.

**Ordering rules:**
```
WRITE + TRANSFORM    -> WRITE wins (full replacement)
TRANSFORM + WRITE    -> WRITE wins (full replacement)
DELETE + TRANSFORM   -> DELETE wins
TRANSFORM + DELETE   -> DELETE wins
TRANSFORM + TRANSFORM -> Merge operations (apply in order)
```

**Rationale:**
- Consistent with existing batch merging behavior
- Writes and deletes always have precedence (simpler semantics)
- Multiple transforms on same key merge naturally
- Deterministic and easy to reason about

### 5. Resolution Timing: During Commit, Not Proposal

**Decision:** Transform resolution (read-modify-write) happens in `flushBatch` during commit processing, not during proposal merging.

**Rationale:**
- Proposal merging stays fast (just protobuf manipulation)
- Correct ordering: see all conflicting operations before resolving
- Batch database reads efficiently
- Aligns with existing architecture

**Implementation:**
1. `proposalDequeuer`: Simple concatenation of transforms (like writes)
2. `readCommits`: Merge transforms by key, handle conflicts with writes/deletes
3. `flushBatch`: Resolve transforms to writes via `resolveTransforms()`
4. `applyOpBatch`: Apply resolved writes to Pebble

### 6. Path Syntax

**Decision:** Accept both formats (`user.name` and `$.user.name`), normalize internally to JSONPath by adding `$.` prefix if missing.

```go
func normalizeJSONPath(path string) string {
    if path[0] != '$' {
        return "$." + path
    }
    return path
}
```

**Rationale:**
- **Consistency**: Antfly already does this exact normalization in `db.go:1850` and `aknn_v0.go:310`
- **User-friendly**: Users can write `"user.name"` (concise) or `"$.user.name"` (explicit)
- **Internally consistent**: Always work with JSONPath standard format after normalization

**Why normalize to JSONPath (not the other way):**
- JSONPath is the industry standard (RFC 9535)
- `github.com/theory/jsonpath` library requires `$.` prefix
- Extensible to full JSONPath features (filters, wildcards, slices)

### 7. Error Handling

**Decision:** Best-effort operation application with logging.

**Behavior:**
- Invalid operation (e.g., INC on non-numeric field): Log warning, skip operation, continue
- Missing path on SET: Create nested structure
- Missing path on INC: Initialize to delta value
- Missing array on PUSH: Create array with single element
- Type mismatch: Log warning, skip operation

**Rationale:**
- Matches MongoDB behavior
- Resilient to schema changes
- Prevents single bad operation from blocking entire batch

### 8. API Surface

**Decision:** Add transforms to existing `/tables/{table}/batch` endpoint, no new endpoint needed.

**Request format:**
```json
{
  "transforms": [{
    "key": "article:123",
    "operations": [
      {"op": "$inc", "path": "$.views", "value": 1},
      {"op": "$push", "path": "$.tags", "value": "trending"}
    ]
  }],
  "sync_level": "write"
}
```

**Rationale:**
- Keeps API surface small
- Natural fit with existing batch operations
- Can mix writes, deletes, and transforms in one request

### 9. Upsert Support

**Decision:** Add `upsert` flag at the Transform level (per key), not at BatchOp or TransformOp level.

```protobuf
message Transform {
  bytes key = 1;
  repeated TransformOp operations = 2;
  bool upsert = 3;  // If true, create document if doesn't exist
}
```

**Semantics:**
- `upsert: true` - Start with empty document `{}` if key doesn't exist, apply operations, write result
- `upsert: false` (default) - Skip transform if key doesn't exist

**Rationale:**
- Matches MongoDB's `updateOne(..., {upsert: true})` semantics exactly
- Per-key granularity allows mixing upsert and non-upsert transforms in same batch
- Clear semantics: "This key-level update is an upsert"

**Alternatives Considered:**
- BatchOp-level flag: Less flexible, all-or-nothing
- TransformOp-level flag: Doesn't make semantic sense (can't "upsert an operation")

**Example usage:**
```json
{
  "transforms": [{
    "key": "counter:views",
    "upsert": true,
    "operations": [
      {"op": "$inc", "path": "$.count", "value": 1},
      {"op": "$currentDate", "path": "$.last_updated"}
    ]
  }]
}
```

### Non-Goals

- **Query-based updates**: Not implementing `UPDATE WHERE` style operations (use bulk API if needed)
- **Triggers**: No hooks for transform operations
- **Change streams**: Not exposing transform operations in change feeds (for now)
- **Full JSONPath expressions**: Starting with simple dot notation
- **Atomic multi-key transactions**: Transforms are per-key atomic, not cross-key

---

## Path Syntax

### Basic Approach

**User can write:** `"user.name"` OR `"$.user.name"`
**Internally normalized to:** `"$.user.name"`

This matches existing Antfly behavior in `db.go:1850` and `aknn_v0.go:310`.

### JSONPath vs Handlebars-style Comparison

**JSONPath (our choice):**
```json
{
  "op": "$set",
  "path": "$.user.name",
  "value": "Alice"
}
```

**Handlebars-style (alternative):**
```json
{
  "op": "$set",
  "path": "user.name",
  "value": "Alice"
}
```

### Simple Field Access

| Operation | JSONPath | Handlebars |
|-----------|----------|------------|
| Root field | `$.title` | `title` |
| Nested field | `$.user.profile.bio` | `user.profile.bio` |
| Deep nesting | `$.a.b.c.d.e` | `a.b.c.d.e` |

### Array Operations

| Operation | JSONPath | Handlebars | Notes |
|-----------|----------|------------|-------|
| Append | `$.tags` | `tags` | Same for both |
| Index access | `$.tags[0]` | `tags.0` | Handlebars uses dot notation |
| Nested array | `$.users[0].name` | `users.0.name` | Handlebars less standard |
| Last element | `$.tags[-1]` | `tags.-1` | JSONPath has standard syntax |

### Future Extensions

| Feature | JSONPath | Handlebars | Support |
|---------|----------|------------|---------|
| Wildcards | `$.users[*].email` | `users.*.email` | JSONPath standard |
| Filters | `$.items[?(@.price > 10)]` | Not supported | JSONPath only |
| Slices | `$.tags[0:3]` | Not supported | JSONPath only |
| Recursive | `$..price` | Not supported | JSONPath only |

### Real-World Examples

**Increment View Count (JSONPath):**
```json
{
  "key": "article:123",
  "operations": [
    {"op": "$inc", "path": "$.views", "value": 1},
    {"op": "$currentDate", "path": "$.last_viewed"}
  ]
}
```

**Nested Object Update (JSONPath):**
```json
{
  "op": "$set",
  "path": "$.user.settings.notifications.email",
  "value": true
}
```

**Array Element Update (future, JSONPath):**
```json
{
  "op": "$set",
  "path": "$.items[0].status",
  "value": "shipped"
}
```

### Integration with Handlebars Templating (Scripting)

If a `$script` operator with Handlebars templating is added in the future, paths work differently:

```json
{
  "op": "$script",
  "language": "handlebars",
  "source": "{{#if (gt user.views 100)}}trending{{else}}normal{{/if}}",
  "path": "$.status"
}
```

Inside the template, Handlebars syntax is used (`{{user.views}}`), while the `path` parameter still uses JSONPath to specify where the result goes.

### Summary

| Aspect | JSONPath | Handlebars | Winner |
|--------|----------|------------|--------|
| Verbosity | More verbose | More concise | Handlebars |
| Clarity | Very clear | Ambiguous | JSONPath |
| Standard | RFC 9535 | Template-only | JSONPath |
| Extensibility | Excellent | Limited | JSONPath |
| Consistency | With Antfly | Against Antfly | JSONPath |
| Tooling | Excellent | Template-focused | JSONPath |
| Array syntax | `[0]` standard | `.0` non-standard | JSONPath |

**Conclusion:** Use JSONPath for path parameters, optionally support Handlebars for template-based scripting in the future.

---

## Conditional Patterns

### The Challenge

Users often need updates like:
- "Increment views, but only if less than 1000"
- "Set status to 'premium' if price > 100, else 'standard'"
- "Add tag only if user.verified is true"

Without conditionals, users must read-modify-write, losing atomicity.

### Option 1: MongoDB Aggregation Pipeline Style

MongoDB 4.2+ allows aggregation expressions in updates.

**Problem:** MongoDB uses two different syntaxes:
- Target paths: Could use JSONPath `$.status` (consistent with Antfly)
- Expression field references: MongoDB uses `$views` (not JSONPath `$.views`)

**Sub-option 1a: MongoDB's Exact Syntax**
```json
{
  "key": "article:123",
  "operations": [{
    "op": "$set",
    "path": "$.status",
    "value": {
      "$cond": {
        "if": {"$gt": ["$views", 100]},
        "then": "trending",
        "else": "normal"
      }
    }
  }]
}
```
- Inconsistent: Mixes JSONPath (`$.status`) with MongoDB syntax (`$views`)

**Sub-option 1b: Adapted for JSONPath Consistency**
```json
{
  "key": "article:123",
  "operations": [{
    "op": "$set",
    "path": "$.status",
    "value": {
      "$cond": {
        "$if": {"$gt": ["$.views", 100]},
        "$then": "trending",
        "$else": "normal"
      }
    }
  }]
}
```
- Consistent internally, but not MongoDB-compatible

**Pros (either sub-option):**
- Powerful (can reference other fields)
- Composable operators

**Cons (either sub-option):**
- Very verbose
- Complex nested syntax
- Requires expression evaluator
- Path syntax inconsistency (1a) OR MongoDB incompatibility (1b)

### Option 2: Per-Operation Conditions (Simpler)

Add optional `condition` field to each operation:

```protobuf
message Condition {
  string path = 1;          // Field to check (e.g., "$.views")
  CompareOp compare = 2;    // lt, lte, gt, gte, eq, ne, exists
  bytes value = 3;          // Value to compare against
}
```

**Usage:**
```json
{
  "key": "article:123",
  "operations": [{
    "op": "$inc",
    "path": "$.views",
    "value": 1,
    "condition": {
      "path": "$.views",
      "compare": "lt",
      "value": 1000
    }
  }]
}
```

**Pros:** Simple, intuitive, covers 90% of use cases, fast evaluation.
**Cons:** Only one condition per operation, no AND/OR, can't reference multiple fields.

### Option 3: Guard Clauses (Transform-Level)

Add conditions at the Transform level to guard entire operation set:

```json
{
  "key": "user:123",
  "guard": {
    "path": "$.verified",
    "compare": "eq",
    "value": true
  },
  "operations": [
    {"op": "$set", "path": "$.tier", "value": "premium"},
    {"op": "$inc", "path": "$.credits", "value": 100}
  ]
}
```

**Pros:** All-or-nothing semantics, efficient (one check for multiple ops).
**Cons:** Can't have different conditions per operation.

### Option 4: Conditional Set Operators (Specialized)

Add specialized operators for common patterns:

```protobuf
enum OpType {
  // ... existing ...
  SET_IF_GT = 13;    // Set only if new value > current
  SET_IF_LT = 14;    // Set only if new value < current
  INC_IF_LT = 15;    // Increment only if result < max
  INC_IF_GT = 16;    // Increment only if result > min
}
```

**Pros:** Clear intent, type-safe, easy to optimize.
**Cons:** Operator explosion, limited flexibility, not composable.

### Option 5: Expression Values (Sear - Bleve's Expression Language)

Use `github.com/blevesearch/sear` for evaluating expressions. Since Antfly already uses Bleve for full-text indexing, sear is a natural fit.

**For Conditional Values:**
```json
{
  "op": "$set",
  "path": "$.status",
  "sear": "if views > 100 then 'trending' else 'normal' end"
}
```

**For Conditions:**
```json
{
  "op": "$inc",
  "path": "$.views",
  "value": 1,
  "condition": {
    "sear": "views < 1000"
  }
}
```

**Sear Syntax Examples:**
```javascript
// Comparisons
views > 100
verified == true
price >= 10.0

// Boolean logic
verified == true && credits > 100
status == "active" || admin == true

// Conditionals
if views > 100 then "trending" else "normal" end

// Math
price * 0.9
views + likes + shares
```

**Pros:**
- Already in ecosystem (Bleve dependency)
- Small, safe (designed for filtering, not Turing-complete)
- No security concerns (sandboxed by design)
- Fast (compiled expressions)
- Can reference fields (e.g., `views`, `user.verified`)

**Cons:**
- Need to map JSONPath (`$.views`) to sear paths (`views`)
- Limited compared to full scripting (no loops, functions)

### Option 6: Composite Conditions (Advanced)

Extend Option 2 to support AND/OR:

```json
{
  "op": "$set",
  "path": "$.tier",
  "value": "premium",
  "condition": {
    "composite": {
      "op": "and",
      "conditions": [
        {"path": "$.verified", "compare": "eq", "value": true},
        {"path": "$.credits", "compare": "gt", "value": 100}
      ]
    }
  }
}
```

### Comparison Table

| Feature | Option 1 | Option 2 | Option 3 | Option 4 | Option 5 (Sear) | Option 6 |
|---------|----------|----------|----------|----------|-----------------|----------|
| Complexity | High | Low | Low | Low | Medium | Medium |
| Flexibility | High | Medium | Low | Low | High | High |
| Safety | Medium | High | High | High | High | High |
| Performance | Medium | High | High | High | High | Medium |
| Field references | Yes | No | No | No | Yes | No |
| MongoDB-like | Yes | No | No | No | No | No |
| Bleve integration | No | No | No | No | Yes | No |
| Implementation effort | 2 weeks | 2 days | 1 day | 3 days | 3 days | 1 week |

### Recommendation

**Phase 1 (MVP):** No conditionals - Ship operators only. Most conditional logic belongs in application code.

**Phase 2 (If users need conditionals):** Option 5 - Sear expressions (best middle ground).

**Alternative Phase 2:** If Sear is too much, start with Option 2 (simple conditions).

**Phase 3 (Only if Sear isn't enough):** Full scripting (Lua/Starlark).

---

## Implementation Plan

### Phase 1: Protobuf Schema Changes

**Files to modify:** `src/store/kvstore.proto`

1. Add `TransformOp` message:
```protobuf
message TransformOp {
  enum OpType {
    SET = 0;            // $set - Set value at path
    UNSET = 1;          // $unset - Remove field
    INC = 2;            // $inc - Increment/decrement numeric value
    PUSH = 3;           // $push - Append to array
    PULL = 4;           // $pull - Remove array elements matching value
    ADD_TO_SET = 5;     // $addToSet - Add to array if not exists
    POP = 6;            // $pop - Remove first (-1) or last (1) array element
    MUL = 7;            // $mul - Multiply numeric value
    MIN = 8;            // $min - Set to minimum of current and value
    MAX = 9;            // $max - Set to maximum of current and value
    CURRENT_DATE = 10;  // $currentDate - Set to current timestamp
    RENAME = 11;        // $rename - Rename field
  }

  string path = 1;     // JSONPath expression (e.g., "$.tags", "$.user.name")
  OpType op = 2;
  bytes value = 3;     // JSON-encoded value (optional for UNSET, CURRENT_DATE)
}
```

2. Add `Transform` message:
```protobuf
message Transform {
  bytes key = 1;
  repeated TransformOp operations = 2;
  bool upsert = 3;  // If true, create document if it doesn't exist
}
```

3. Update `BatchOp`:
```protobuf
message BatchOp {
  repeated Write writes = 1;
  repeated bytes deletes = 2;
  repeated Transform transforms = 3;  // NEW
  KvstoreOp.SyncLevel sync_level = 4;
}
```

4. Run `make generate`

**Estimated time:** 0.5 day

### Phase 2: JSONPath Helper Functions

**Files to create:** `src/store/transform.go`

```go
package store

// Normalize path to JSONPath format (add $. prefix if missing)
func normalizeJSONPath(path string) string

// Simple path parser for "$.foo.bar" style paths
func parseSimplePath(path string) []string

// Get value at nested path
func getNestedValue(doc map[string]any, parts []string) (any, error)

// Set value at nested path
func setNestedValue(doc map[string]any, parts []string, value any) map[string]any

// Remove value at nested path
func removeNestedValue(doc map[string]any, parts []string) map[string]any

// Deep equality check for values
func deepEqual(a, b any) bool
```

**Implementation notes:**
- Start with simple dot-notation (e.g., `$.user.name`)
- Handle missing intermediate objects (create them for SET operations)
- Return errors for invalid paths or type mismatches
- Use `bytedance/sonic` for JSON marshaling (consistent with rest of codebase)

**Estimated time:** 1 day

### Phase 3: Transform Operation Implementations

**Files to modify:** `src/store/transform.go`

Implement all 12 MongoDB operators:

```go
func applyTransformOp(doc map[string]any, op *TransformOp) (map[string]any, error)

func setJSONPath(doc map[string]any, path string, value any) (map[string]any, error)
func unsetJSONPath(doc map[string]any, path string) (map[string]any, error)
func incrementJSONPath(doc map[string]any, path string, delta float64) (map[string]any, error)
func pushJSONPath(doc map[string]any, path string, value any) (map[string]any, error)
func pullJSONPath(doc map[string]any, path string, value any) (map[string]any, error)
func addToSetJSONPath(doc map[string]any, path string, value any) (map[string]any, error)
func popJSONPath(doc map[string]any, path string, position int) (map[string]any, error)
func multiplyJSONPath(doc map[string]any, path string, multiplier float64) (map[string]any, error)
func minJSONPath(doc map[string]any, path string, value float64) (map[string]any, error)
func maxJSONPath(doc map[string]any, path string, value float64) (map[string]any, error)
func currentDateJSONPath(doc map[string]any, path string) (map[string]any, error)
func renameJSONPath(doc map[string]any, oldPath string, newPath string) (map[string]any, error)
```

**Key behaviors:**
1. **Path normalization:** All operations normalize paths using `normalizeJSONPath()`
2. **$set:** Set value, create intermediate objects if needed
3. **$unset:** Remove field, no-op if doesn't exist
4. **$inc:** Increment number, initialize to delta if missing
5. **$push:** Append to array, create array if missing
6. **$pull:** Remove matching elements from array
7. **$addToSet:** Add if not in array (use deepEqual for comparison)
8. **$pop:** Remove first (-1) or last (1) element
9. **$mul:** Multiply number, error if not numeric
10. **$min:** Set to min(current, value), initialize if missing
11. **$max:** Set to max(current, value), initialize if missing
12. **$currentDate:** Set to RFC3339Nano timestamp
13. **$rename:** Move field from old path to new path

**Error handling:** Log warnings for type mismatches, skip operation. Return errors only for critical failures. Match MongoDB's lenient behavior.

**Estimated time:** 2 days

### Phase 4: Batch Merging Logic

**Files to modify:** `src/store/dbwrapper.go`

#### 4.1 Update `readCommits` function (line ~654)

Add transforms slice to declarations, defer cleanup, and update `flushBatch`:

```go
flushBatch := func() {
    // Resolve transforms to writes first
    if len(transforms) > 0 {
        resolvedWrites, err := s.resolveTransforms(ctx, transforms)
        if err != nil {
            s.logger.Error("Failed to resolve transforms", zap.Error(err))
        } else {
            writes = append(writes, resolvedWrites...)
        }
    }

    // ... existing sync level and apply logic ...
}
```

#### 4.2 Update `KvstoreOp_OpBatch` case to handle transforms

Handle conflict resolution between writes, deletes, and transforms:
- Deletes remove pending transforms for the same key
- Writes remove pending transforms for the same key
- New transforms remove pending writes/deletes and merge with existing transforms

#### 4.3 Add `resolveTransforms` method to `dbWrapper`

```go
func (s *dbWrapper) resolveTransforms(ctx context.Context, transforms []*Transform) ([][2][]byte, error) {
    resolved := make([][2][]byte, 0, len(transforms))

    for _, transform := range transforms {
        existing, err := s.coreDB.Get(ctx, transform.GetKey())
        if err != nil {
            if errors.Is(err, ErrNotFound) {
                if !transform.GetUpsert() {
                    continue  // Skip non-upsert transforms for missing keys
                }
                existing = make(map[string]any)
            } else {
                return nil, fmt.Errorf("reading key for transform: %w", err)
            }
        }

        modified := existing
        for _, op := range transform.GetOperations() {
            modified, err = applyTransformOp(modified, op)
            if err != nil {
                s.logger.Warn("Failed to apply transform operation", ...)
                continue
            }
        }

        modifiedBytes, err := sonic.Marshal(modified)
        if err != nil {
            return nil, fmt.Errorf("marshalling transformed doc: %w", err)
        }
        resolved = append(resolved, [2][]byte{transform.GetKey(), modifiedBytes})
    }

    return resolved, nil
}
```

#### 4.4 Update `proposalDequeuer` function (line ~292)

Add transform validation and merging (simple concatenation; merging happens in readCommits).

**Estimated time:** 1.5 days

### Phase 5: API Integration

**Files to modify:**
- `src/metadata/api.yaml` (OpenAPI spec)
- `src/metadata/api.go` (handler implementation)

#### OpenAPI Schema Additions

```yaml
TransformOp:
  type: object
  required:
    - op
    - path
  properties:
    op:
      type: string
      enum:
        - $set
        - $unset
        - $inc
        - $push
        - $pull
        - $addToSet
        - $pop
        - $mul
        - $min
        - $max
        - $currentDate
        - $rename
    path:
      type: string
      description: JSONPath to field (e.g., "$.user.name", "$.tags")
    value:
      description: Value for operation (not required for $unset, $currentDate)

Transform:
  type: object
  required:
    - key
    - operations
  properties:
    key:
      type: string
    operations:
      type: array
      items:
        $ref: '#/components/schemas/TransformOp'
    upsert:
      type: boolean
      default: false
```

#### Handler Changes

Modify the existing batch handler in `api.go` to convert API transforms to protobuf `Transform` messages and add them to the `BatchOp`.

Run `make generate` after OpenAPI changes.

**Estimated time:** 1 day

### Phase 6: Documentation

- Create `docs/transform-operations.md` with examples for all 12 operators
- Update `www/content/docs/api/data-operations.mdx`
- Create example applications: counter service, tag manager, status updater

**Estimated time:** 1 day

### Phase 7: Testing & Validation

**Unit Tests** (`src/store/transform_test.go`, `src/store/dbwrapper_test.go`):
- All 12 operators with edge cases
- JSONPath helpers
- Conflict resolution logic
- Target: 90%+ code coverage

**Integration Tests** (`e2e/transform_test.go`):
- Single and multiple operations per key
- Mixed batches (writes + transforms + deletes)
- Concurrent transforms
- Raft replication correctness
- Upsert and non-upsert behavior

**Performance Tests** (`src/store/transform_bench_test.go`):
- Transform overhead vs plain write
- Batch merging performance
- Large document transforms
- Target: <10ms overhead for typical transforms

**MongoDB Compatibility Tests** (`src/store/mongo_compat_test.go`):
- Each operator matches MongoDB behavior
- Edge cases (missing fields, type mismatches)
- Operation sequences

**Estimated time:** 2 days

### Implementation Order

| Phase | Description | Estimate |
|-------|-------------|----------|
| 1 | Protobuf schema | 0.5 days |
| 2 | JSONPath helpers | 1 day |
| 3 | Transform operators | 2 days |
| 4 | Batch merging | 1.5 days |
| 5 | API integration | 1 day |
| 6 | Documentation | 1 day |
| 7 | Testing | 2 days |
| **Total** | | **9 days** |

### Success Criteria

- All 12 operators implemented and tested
- 90%+ code coverage on transform logic
- MongoDB compatibility test suite passes 100%
- Performance benchmarks show <10ms overhead
- Documentation complete with 3+ examples
- No regressions in existing batch operations

### Rollback Plan

If critical issues are found:
1. Feature flag to disable transforms at API level
2. Remove `transforms` field from OpenAPI spec
3. Batch operations continue to work normally
4. No data corruption risk (transforms resolve to writes)

### Future Considerations

- **Additional MongoDB Operators:** `$bit`, `$setOnInsert`, `$each`/`$slice` modifiers, array filters
- **Optimistic Locking:** Version-based concurrency control
- **Bulk Transform API:** Specialized endpoint for large-scale migrations with query filters
- **Scripted Updates:** `$script` operator with `expr`, `handlebars`, or `lua`/`starlark` for complex logic beyond predefined operators

### Open Questions

- Should transforms be validated against table schema?
- Should enrichers (embeddings, etc.) re-run on transforms?
- How to handle transforms in audit logs?
- Rate limiting for transform-heavy workloads?
