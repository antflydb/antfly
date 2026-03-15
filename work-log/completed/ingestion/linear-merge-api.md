# Linear Merge API

## Status

✅ **Completed** - Production-ready stateless linear merge API for progressive synchronization

## Overview

The Linear Merge API provides stateless, progressive synchronization from external data sources (SQL, NoSQL, REST APIs) to Antfly. It automatically deletes records absent from incoming batches based on key ranges, enabling one-way pull-only synchronization.

## Algorithm: Stateless Range-Based Merge

### Per-Request Processing

1. **Define Range**: `(last_merged_id, max(records[].id)]`
2. **Query Antfly**: Fetch all keys in range using prefix scan
3. **Linear Scan**: Iterate through Antfly keys and input records (both sorted)
   - Match found → UPSERT record (if hash differs)
   - Antfly key not in input → DELETE
4. **Return**: `next_cursor = max(records[].id)`

### Client Workflow

```
Page 1: POST /merge {"records": [sorted 1-1000], "last_merged_id": ""}
        ← {"next_cursor": "rec_1000"}

Page 2: POST /merge {"records": [sorted 1001-2000], "last_merged_id": "rec_1000"}
        ← {"next_cursor": "rec_2000"}

...continue until external DB exhausted
```

## Key Features

### 1. Stateless Operation
- No server-side session tracking between requests
- Client tracks progress via `next_cursor`
- Retry-safe (idempotent)

### 2. Content Hash-Based Optimization
- Uses `sonic` with `SortMapKeys` + `xxhash` for deterministic hashing
- Automatically skips unchanged documents
- Reports skipped count in response

### 3. Progressive Deletion
- Deletes records absent from input incrementally per batch
- No end-of-sync cleanup needed
- Range: `(last_merged_id, max(input_ids)]`

### 4. Shard Boundary Handling
- Detects when batch crosses shard boundary
- Returns `partial` status with `next_cursor`
- Client retries from cursor to continue

### 5. Dry Run Support
- Preview mode with `dry_run: true`
- Returns `deleted_ids` array
- Validates sync behavior before committing

### 6. Safety Limits
- Max 10,000 records per request
- Max 100,000 keys scanned per request
- Prevents memory exhaustion

### 7. Comprehensive Error Reporting
- `failed` array with per-record errors
- Operation type (upsert/delete) included
- Continues processing other records on partial failures

## API Reference

### Endpoint

```
POST /table/{tableName}/merge
```

### Request Schema

```json
{
  "records": {
    "doc_id_1": {"field": "value"},
    "doc_id_2": {"field": "value"}
  },
  "last_merged_id": "doc_id_previous",
  "dry_run": false
}
```

**Fields**:
- `records` (object, required): Map of document ID to document object
  - Server automatically sorts keys lexicographically
  - Format avoids duplicate IDs
- `last_merged_id` (string): ID of last record from previous request
  - Empty string `""` for first request
  - Defines lower bound of key range
- `dry_run` (boolean, default: false): Preview deletions without making changes

### Response Schema

```json
{
  "status": "success",
  "upserted": 10,
  "skipped": 2,
  "deleted": 1,
  "deleted_ids": ["doc_3"],
  "failed": [],
  "next_cursor": "doc_id_last",
  "keys_scanned": 13,
  "message": "Processed 10 records successfully"
}
```

**Fields**:
- `status`: `"success"` | `"partial"` | `"error"`
  - `success`: All records in batch processed
  - `partial`: Stopped at shard boundary, retry with next_cursor
  - `error`: Fatal error, no records processed
- `upserted`: Records inserted or updated (0 if dry_run)
- `skipped`: Records skipped (content hash matched)
- `deleted`: Records deleted (or would be if dry_run)
- `deleted_ids`: Array of deleted IDs (only if dry_run=true)
- `failed`: Array of failed operations with errors
- `next_cursor`: Cursor for next request
- `keys_scanned`: Number of Antfly keys scanned
- `message`: Human-readable status message

## Usage Examples

### Example 1: Basic Merge

```go
import "github.com/antflydb/antfly-go/antfly"

client, _ := antfly.NewAntflyClient("http://localhost:8080/api/v1", http.DefaultClient)

result, err := client.LinearMerge(ctx, "users", antfly.LinearMergeRequest{
    Records: map[string]interface{}{
        "user_001": map[string]interface{}{"name": "Alice", "age": 30},
        "user_002": map[string]interface{}{"name": "Bob", "age": 25},
    },
    LastMergedId: "",
    DryRun:       false,
})

fmt.Printf("Upserted: %d, Deleted: %d, Skipped: %d\n",
    result.Upserted, result.Deleted, result.Skipped)
```

### Example 2: Progressive Sync with Pagination

```go
cursor := ""
for hasMore {
    // Fetch next batch from external source
    records := fetchFromExternalDB(cursor, 1000)

    result, err := client.LinearMerge(ctx, "users", antfly.LinearMergeRequest{
        Records:      records,
        LastMergedId: cursor,
        DryRun:       false,
    })

    switch result.Status {
    case antfly.LinearMergePageStatusSuccess:
        fmt.Printf("Merged batch: +%d, -%d\n", result.Upserted, result.Deleted)
        cursor = result.NextCursor
        hasMore = hasMoreExternalRecords()

    case antfly.LinearMergePageStatusPartial:
        // Hit shard boundary, continue with same batch
        cursor = result.NextCursor
        continue

    case antfly.LinearMergePageStatusError:
        return fmt.Errorf("merge failed: %s", result.Message)
    }
}
```

### Example 3: Dry Run (Preview Deletions)

```go
result, err := client.LinearMerge(ctx, "users", antfly.LinearMergeRequest{
    Records: map[string]interface{}{
        "user_001": map[string]interface{}{"name": "Alice", "age": 30},
        // user_002 and user_003 omitted - would be deleted
    },
    LastMergedId: "",
    DryRun:       true,
})

if result.Deleted > 0 {
    fmt.Printf("Would delete %d records: %v\n", result.Deleted, result.DeletedIds)
}
```

### Example 4: Handle Shard Boundaries

```go
cursor := ""
batchNum := 1

for {
    result, err := client.LinearMerge(ctx, "users", antfly.LinearMergeRequest{
        Records:      largeRecordBatch,
        LastMergedId: cursor,
        DryRun:       false,
    })

    fmt.Printf("Batch %d: %s, Upserted: %d\n",
        batchNum, result.Status, result.Upserted)

    if result.Status == antfly.LinearMergePageStatusPartial {
        // Hit shard boundary, continue with next batch
        cursor = result.NextCursor
        batchNum++
        continue
    }

    // Success or error - done
    break
}
```

## Design Decisions

1. **Key Format**: Users send raw IDs (`"user_123"`), server transforms internally
2. **Data Format**: Object map `{"id1": {...}, "id2": {...}}` avoids duplicate IDs
3. **Automatic Sorting**: Server sorts keys automatically (simpler client integration)
4. **Content Hashing**: `sonic.SortMapKeys` + `xxhash` for deterministic, efficient hashing
5. **Skip Unchanged**: Compare hashes before upsert, only write if changed
6. **Shard Boundaries**: Detect and return `partial` status when crossing boundaries
7. **Empty Records**: Allowed with `last_merged_id` for delete-only operations
8. **Cursor on Error**: Returns `last_merged_id` (cursor doesn't advance)
9. **Idempotent Retry**: Client resends full batch, server skips already-processed records
10. **Parallel Operations**: Uses errgroups for concurrent shard operations

## Walkthrough Example

### Initial State

Antfly contains: `rec_1, rec_2, rec_3, rec_4, rec_5, rec_999, rec_1000`

### Request 1

```json
{
  "records": {
    "rec_1": {"data": "updated"},
    "rec_2": {"data": "updated"}
  },
  "last_merged_id": ""
}
```

**Processing**:
1. Range: `("", "rec_2"]`
2. Antfly keys in range: `rec_1, rec_2, rec_3`
3. Input has: `rec_1, rec_2`
4. **Missing from input: `rec_3`** → DELETE `rec_3`
5. Result: `upserted: 2, deleted: 1, next_cursor: "rec_2"`

### Request 2

```json
{
  "records": {
    "rec_4": {"data": "..."},
    "rec_5": {"data": "..."}
  },
  "last_merged_id": "rec_2"
}
```

**Processing**:
1. Range: `("rec_2", "rec_5"]`
2. Antfly keys in range: `rec_4, rec_5` (rec_3 already deleted)
3. Input has: `rec_4, rec_5`
4. **No deletions needed**
5. Result: `upserted: 2, deleted: 0, next_cursor: "rec_5"`

### Request 3 (Shard Boundary)

```json
{
  "records": {
    "rec_998": {"data": "..."},
    "rec_999": {"data": "..."},
    "rec_1000": {"data": "..."},
    "rec_1001": {"data": "..."}
  },
  "last_merged_id": "rec_997"
}
```

**Scenario**: Shard 1 owns up to `rec_1000`, Shard 2 starts at `rec_1001`

**Processing**:
1. Range: `("rec_997", "rec_1001"]`
2. Detect shard boundary at `rec_1000`
3. Process only: `rec_998, rec_999, rec_1000`
4. Result: `status: "partial", next_cursor: "rec_1000"`
5. Client retries with `last_merged_id: "rec_1000"` → processes `rec_1001` in Shard 2

## Performance Characteristics

| Metric | Limit | Rationale |
|--------|-------|-----------|
| Records per request | 10,000 | Balance throughput vs memory |
| Keys scanned | 100,000 | Prevent memory exhaustion |
| Batch size | 1,000-5,000 | Recommended for optimal performance |
| Shard operations | Parallel | Uses errgroups for concurrency |

## Known Limitations

1. **Not Thread-Safe**: Concurrent merges on the same table with overlapping ranges may produce undefined results
2. **Single or Adjacent Shard**: Current implementation assumes range doesn't cross multiple non-adjacent shards
3. **No Partial Batch Resume**: If batch partially fails, client must retry entire batch (server skips processed records)

## Client SDK Support

The Linear Merge API is available in all three client SDKs:

### Go SDK

```go
import "github.com/antflydb/antfly-go/antfly"

result, err := client.LinearMerge(ctx, tableName, antfly.LinearMergeRequest{
    Records:      records,
    LastMergedId: cursor,
    DryRun:       false,
})
```

### TypeScript SDK

```typescript
import { AntflyClient } from 'antfly-ts';

const result = await client.linearMerge(tableName, {
  records: records,
  last_merged_id: cursor,
  dry_run: false
});
```

### Python SDK

```python
from antfly import AntflyClient

result = client.linear_merge(table_name, {
    'records': records,
    'last_merged_id': cursor,
    'dry_run': False
})
```

## Implementation

- **API Handler**: `src/metadata/api.go` (LinearMerge function, ~360 lines)
- **Storage Layer**: `src/store/db.go` (Scan method with ScanOptions)
- **Content Hashing**: `src/store/db.go` (ComputeDocumentHash)
- **Tests**: `src/metadata/api_linear_merge_test.go` (13 tests) + `src/store/db_scan_test.go` (17 tests)
- **OpenAPI Schema**: `src/metadata/api.yaml` (LinearMergeRequest, LinearMergeResult)

## Use Cases

- **SQL database synchronization**: Pull from MySQL/Postgres tables
- **NoSQL store imports**: Sync from MongoDB/DynamoDB
- **REST API data merging**: Pull from external APIs
- **ETL pipelines**: Transform and load from any source
- **Data replication**: One-way sync from primary to secondary systems

## References

- API Endpoint: `POST /table/{tableName}/merge`
- Example Code: `work-log/006-create-linear-merge-api/example.go`
- Implementation: `src/metadata/api.go` (LinearMerge handler)
- Storage Layer: `src/store/db.go` (Scan method)
