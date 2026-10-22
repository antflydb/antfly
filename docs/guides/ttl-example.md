# Document TTL (Time-To-Live) Feature

This document describes how to use the TTL feature in Antfly to automatically expire and delete documents after a specified duration.

## Overview

The TTL feature allows you to configure automatic expiration for documents in a table. Documents are automatically deleted after a specified duration from a reference timestamp field.

## Configuration

### Basic TTL Configuration

Configure TTL when creating a table (`POST /db/v1/tables/{tableName}`) with a
`ttl` policy under `schema`:

```json
{
  "schema": {
    "ttl": {"duration": "7d"},
    "document_schemas": {
      "default": {
        "schema": {
          "type": "object",
          "properties": {
            "data": {"type": "string"}
          }
        }
      }
    }
  }
}
```

In this example:
- Documents will expire 7 days after their `_timestamp` field value
- The `_timestamp` field is automatically added to documents at insertion time
- Expired documents are automatically deleted by the background cleanup job

### Custom TTL Reference Field

You can specify a custom timestamp field as the TTL reference:

```json
{
  "schema": {
    "ttl": {
      "duration": "24h",
      "field": "created_at"
    },
    "document_schemas": {
      "default": {
        "schema": {
          "type": "object",
          "properties": {
            "created_at": {"type": "string", "format": "date-time"},
            "data": {"type": "string"}
          },
          "required": ["created_at"]
        }
      }
    }
  }
}
```

In this example:
- Documents expire 24 hours after their `created_at` field value
- This example's document schema requires `created_at`; TTL itself falls back to
  the write timestamp when a custom field is missing or null unless schema
  validation rejects the document
- The TTL parser accepts nonnegative integer Unix nanoseconds, decimal timestamp
  strings, or supported RFC3339/RFC3339Nano strings (e.g., `2025-01-01T12:00:00Z`).
  Floating-point timestamps are not accepted; the example schema requires strings.

## TTL Duration Format

TTL durations use Antfly's integer-component duration format. Supported units
are `ns`, `us`, `ms`, `s`, `m`, `h`, and `d`:
- `30s` - 30 seconds
- `5m` - 5 minutes
- `24h` - 24 hours
- `7d` - 7 days (treated as 168h)
- `30d` - 30 days (treated as 720h)

## How It Works

### 1. Document Insertion

When documents are inserted:
- If using the default `_timestamp` field, it's automatically added with the current time
- Missing or null custom TTL fields fall back to the write timestamp
- Document-schema requirements may independently reject missing/null fields
- **Performance Optimization**: The TTL timestamp is extracted and stored in a separate internal TTL key for fast lookups

```bash
# Insert document (using default _timestamp)
curl -X POST http://localhost:8080/db/v1/tables/my_table/batch \
  -H "Content-Type: application/json" \
  -d '{
    "inserts": {
      "doc1": {"data": "test"}
    }
  }'
# _timestamp is automatically added, and an internal TTL key is written for doc1
```

**Storage Layout:** the document is stored under its key, and its expiry reference is stored in a structured internal TTL key holding an 8-byte little-endian u64 of Unix nanoseconds.

### 2. Background Cleanup

A background job runs on the Raft leader:
- **Optimized Scanning**: Scans only internal TTL keys - no JSON deserialization needed
- Is scheduled at a 30-second interval; this is not a deletion deadline
- Deletes documents where `current_time > timestamp + ttl_duration + grace_period`
- Adds a 5-second cleanup grace; elapsed wall time is not a replication fence
- Clamps candidate pages to `min(128, max(1, batch_size))` for both document and
  graph cleanup, separately from scan budgets of 4096 keys or 4 MiB

**Performance**: Scanning is O(1) per document - just reads an 8-byte timestamp, no JSON parsing required.

### 3. Query Filtering

Expired documents are filtered from query results:
- **Fast TTL Check**: Reads only the internal TTL key - no document deserialization
- Get operations return "not found" for expired documents
- Search results exclude expired documents
- Filtering happens in real-time before the cleanup job runs

### 4. TTL Extension (Session Refresh)

You can extend a document's TTL by rewriting its TTL reference field, which is useful for session management and activity-based expiration. An insert replaces the whole document, so include every field the schema requires:

```bash
# Example: Extend TTL on every access by rewriting the TTL reference field
curl -X POST http://localhost:8080/db/v1/tables/sessions/batch \
  -H "Content-Type: application/json" \
  -d '{
    "inserts": {
      "session:12345": {"user_id": "user-1", "last_accessed": "2025-01-01T12:30:00Z"}
    }
  }'
```

## Example: Session Storage

Use TTL for automatic session cleanup:

```json
{
  "schema": {
    "ttl": {
      "duration": "1h",
      "field": "last_accessed"
    },
    "document_schemas": {
      "session": {
        "schema": {
          "type": "object",
          "properties": {
            "user_id": {"type": "string"},
            "last_accessed": {"type": "string", "format": "date-time"},
            "data": {"type": "object"}
          },
          "required": ["user_id", "last_accessed"]
        }
      }
    }
  }
}
```

Sessions expire 1 hour after the `last_accessed` timestamp. Update `last_accessed` on each access to extend the session.

## Example: Event Logs

Use TTL for automatic log rotation:

```json
{
  "schema": {
    "ttl": {"duration": "30d"},
    "document_schemas": {
      "event": {
        "schema": {
          "type": "object",
          "properties": {
            "event_type": {"type": "string"},
            "timestamp": {"type": "string", "format": "date-time"},
            "data": {"type": "object"}
          }
        }
      }
    }
  }
}
```

Events expire 30 days after their `_timestamp` (insertion time). Old events are automatically deleted.

## Monitoring

Expired documents are hidden from reads immediately; physical cleanup runs in the background. The cleanup job does not currently emit dedicated TTL logs or metrics. Table/storage status reports aggregate storage state, not a TTL deletion deadline. Hosted embedded profiles disable the background TTL runtime; hosts drive supported maintenance explicitly, and `runUntilIdle()` advances derived indexing rather than promising TTL deletion.

## Modifying TTL Configuration

### Adding TTL to Existing Table

TTL can be added to an existing table with a JSON Merge Patch:

```http
PATCH /db/v1/tables/my_table/schema
Content-Type: application/merge-patch+json
If-Match: "schema-3"

{"ttl":{"duration":"7d"}}
```

Successful schema mutations return the committed schema ETag (for example,
`ETag: "schema-4"`). Reuse that value in `If-Match` when concurrent editors
must not overwrite each other. Omitting `If-Match` applies the merge patch to
the newest authoritative schema and retries an internal metadata race.

- Applies retroactively to all existing documents
- Documents already expired based on the new TTL are marked for immediate deletion

### Removing TTL

TTL can be removed explicitly with a JSON Merge Patch. Other schema fields are
preserved:

```http
PATCH /db/v1/tables/my_table/schema
Content-Type: application/merge-patch+json
If-Match: "schema-4"

{"ttl": null}
```

After removal:
- All expiration processing stops immediately
- All documents become permanent
- Previously expired documents remain (are not deleted)

### Changing TTL Duration

TTL duration can be changed with the same PATCH endpoint. `PUT` replaces the
complete schema and should only be used when the caller intends replacement:
- New duration applies immediately to all documents
- All documents recalculate expiration using existing timestamps with new duration

## Implementation Details

### Separate TTL Timestamp Keys

**Performance Optimization**: TTL timestamps are stored in separate structured internal TTL keys:
- **Fast Cleanup Scans**: O(1) per document - no JSON deserialization
- **Fast Query Filtering**: Single key lookup to check expiration
- **Minimal Storage Overhead**: an 8-byte little-endian u64 of Unix nanoseconds per document

### Grace Period

A 5-second grace period is added to the expiration time to prevent premature deletion:
- Delays cleanup eligibility; it does not prove replication completion
- Ownership, write admission, and bounded scan pages can delay deletion further
- Only applies to background cleanup (not query filtering)

### Leader-Only Operation

TTL cleanup runs only on the Raft leader:
- Ensures only one node performs cleanup
- Cleanup operations go through Raft consensus
- Leadership changes automatically start/stop cleanup on appropriate nodes

### Cleanup Batch Size

The configured cleanup batch default is 256, but both document and graph
collectors clamp candidate pages to `min(128, max(1, batch_size))`. The embedded
`ttl_cleanup_batch_size` option uses zero to preserve the configured default of
256, yielding an effective candidate cap of 128. Nonzero values are forwarded
to the collectors; values above 128 are clamped, not rejected. The collector's
internal zero-to-one clamp does not change the embedded option's zero-as-default
semantics.
The independent scan budgets are 4096 keys or 4 MiB per page. A page can contain
fewer eligible deletions than candidates scanned.

Supporting larger embedded batches would require a separate implementation task
that checks downstream safety budgets and both cleanup paths. This guide does
not change the runtime bound.

### Performance Characteristics

Cleanup scans structured timestamp keys instead of parsing every document.
Read-time checks still require storage access; latency and throughput depend on
cache state, workload, expiry density, and admission. No fixed speedup or
zero-latency overhead is guaranteed.

## Limitations and Considerations

1. **Clock Synchronization**: TTL relies on system clocks being synchronized across cluster nodes (use NTP)
2. **Cleanup Latency**: Cleanup is scheduled every 30 seconds; grace, bounded pages, leader ownership, and admission can delay physical deletion
3. **Query Filtering**: Even though cleanup runs periodically, expired documents are filtered in real-time from queries
4. **Timestamp Format**: Use nonnegative integer Unix nanoseconds, decimal timestamp strings, or supported RFC3339/RFC3339Nano strings; floats are rejected
5. **Table-Level Configuration**: TTL is configured per-table, not per-document

## Best Practices

1. **Use Default Field**: Use the default `_timestamp` field unless you need custom TTL behavior
2. **Appropriate Durations**: Set TTL durations appropriate to your use case (avoid very short durations like seconds)
3. **Monitor Cleanup**: Check read-time expiry and available table/storage status; there are no dedicated TTL cleanup logs or metrics
4. **Test Before Production**: Test TTL behavior with sample data before production deployment
5. **Document Schema**: Document your TTL configuration in your schema for maintainability
