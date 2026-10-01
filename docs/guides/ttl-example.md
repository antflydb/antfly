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
- The `created_at` field must be present in all documents
- Timestamps must be in RFC3339 format (e.g., `2025-01-01T12:00:00Z`)

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
- If using a custom TTL field, it must be present in the document
- Validation ensures the TTL field exists
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
- Runs every 30 seconds
- Deletes documents where `current_time > timestamp + ttl_duration + grace_period`
- Includes a 5-second grace period to ensure writes are fully replicated
- Processes deletions in batches of 256, with each scan page bounded to 4096 keys or 4 MiB

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

Expired documents are hidden from reads immediately; physical cleanup runs in the background. The cleanup job does not currently emit dedicated TTL logs or metrics.

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
- Ensures writes are fully replicated across the cluster
- Prevents race conditions between write and expiration
- Only applies to background cleanup (not query filtering)

### Leader-Only Operation

TTL cleanup runs only on the Raft leader:
- Ensures only one node performs cleanup
- Cleanup operations go through Raft consensus
- Leadership changes automatically start/stop cleanup on appropriate nodes

### Cleanup Batch Size

Documents are deleted in batches of 256, and each scan page is bounded to 4096 keys or 4 MiB:
- Prevents overwhelming the system with large deletions
- Maintains system responsiveness

### Performance Characteristics

**Cleanup Scan Performance:**
- Traditional approach: O(N × document_size) - must decompress and parse every document
- Optimized approach: O(N × 30 bytes) - only reads timestamp keys
- **Speedup**: ~100-1000x faster depending on document size

**Query Filtering Performance:**
- Single point lookup (~microseconds)
- No impact on query latency
- Scales to millions of documents

## Limitations and Considerations

1. **Clock Synchronization**: TTL relies on system clocks being synchronized across cluster nodes (use NTP)
2. **Cleanup Latency**: Expired documents are deleted within ~30 seconds (cleanup interval)
3. **Query Filtering**: Even though cleanup runs periodically, expired documents are filtered in real-time from queries
4. **Timestamp Format**: Timestamps must be in RFC3339 or RFC3339Nano format
5. **Table-Level Configuration**: TTL is configured per-table, not per-document

## Best Practices

1. **Use Default Field**: Use the default `_timestamp` field unless you need custom TTL behavior
2. **Appropriate Durations**: Set TTL durations appropriate to your use case (avoid very short durations like seconds)
3. **Monitor Cleanup**: Watch cleanup logs to ensure expired documents are being processed
4. **Test Before Production**: Test TTL behavior with sample data before production deployment
5. **Document Schema**: Document your TTL configuration in your schema for maintainability
