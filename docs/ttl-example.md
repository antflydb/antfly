# Document TTL (Time-To-Live) Feature

This document describes how to use the TTL feature in Antfly to automatically expire and delete documents after a specified duration.

## Overview

The TTL feature allows you to configure automatic expiration for documents in a table. Documents are automatically deleted after a specified duration from a reference timestamp field.

## Configuration

### Basic TTL Configuration

Configure TTL when creating a table by setting the `ttl_duration` field:

```json
{
  "name": "my_table",
  "ttl_duration": "7d",
  "document_schemas": {
    "default": {
      "type": "object",
      "properties": {
        "data": {"type": "string"}
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
  "name": "my_table",
  "ttl_duration": "24h",
  "ttl_field": "created_at",
  "document_schemas": {
    "default": {
      "type": "object",
      "properties": {
        "created_at": {"type": "string", "format": "date-time"},
        "data": {"type": "string"}
      },
      "required": ["created_at"]
    }
  }
}
```

In this example:
- Documents expire 24 hours after their `created_at` field value
- The `created_at` field must be present in all documents
- Timestamps must be in RFC3339 format (e.g., `2025-01-01T12:00:00Z`)

## TTL Duration Format

TTL durations use Go's duration format:
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
- **Performance Optimization**: The TTL timestamp is extracted and stored as a separate key (`:t` suffix) for fast lookups

```bash
# Insert document (using default _timestamp)
curl -X POST http://localhost:8080/api/v1/my_table/batch \
  -H "Content-Type: application/json" \
  -d '{
    "inserts": {
      "doc1": {"data": "test"}
    }
  }'
# _timestamp is automatically added
# TTL timestamp key is created: doc1:t → "2025-01-01T12:00:00Z"
```

**Storage Layout:**
```
doc1::     → {"data": "test", "_timestamp": "2025-01-01T12:00:00Z"} (compressed)
doc1:t     → "2025-01-01T12:00:00Z" (raw timestamp for fast TTL checks)
```

### 2. Background Cleanup

A background job runs on the Raft leader:
- **Optimized Scanning**: Scans only TTL timestamp keys (`:t` suffix) - no JSON deserialization needed
- Runs every 30 seconds
- Deletes documents where `current_time > timestamp + ttl_duration + grace_period`
- Includes a 5-second grace period to ensure writes are fully replicated
- Processes deletions in batches of 1000
- Logs cleanup operations and metrics

**Performance**: Scanning is O(1) per document - just reads a timestamp string, no JSON parsing required.

### 3. Query Filtering

Expired documents are filtered from query results:
- **Fast TTL Check**: Reads only the `:t` key - no document deserialization
- Get operations return "not found" for expired documents
- Search results exclude expired documents
- Filtering happens in real-time before the cleanup job runs

### 4. TTL Extension (Session Refresh)

You can extend the TTL for a document without modifying it:
- Updates only the `:t` timestamp key
- Very efficient - no document rewrite needed
- Perfect for session management and activity-based expiration

```go
// Example: Extend TTL on every access
db.ExtendTTL(ctx, []byte("session:12345"), time.Now())
```

## Example: Session Storage

Use TTL for automatic session cleanup:

```json
{
  "name": "sessions",
  "ttl_duration": "1h",
  "ttl_field": "last_accessed",
  "document_schemas": {
    "session": {
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
```

Sessions expire 1 hour after the `last_accessed` timestamp. Update `last_accessed` on each access to extend the session.

## Example: Event Logs

Use TTL for automatic log rotation:

```json
{
  "name": "events",
  "ttl_duration": "30d",
  "document_schemas": {
    "event": {
      "type": "object",
      "properties": {
        "event_type": {"type": "string"},
        "timestamp": {"type": "string", "format": "date-time"},
        "data": {"type": "object"}
      }
    }
  }
}
```

Events expire 30 days after their `_timestamp` (insertion time). Old events are automatically deleted.

## Monitoring

TTL operations are logged with the following information:

```
INFO  Starting TTL cleanup job ttl_field=_timestamp ttl_duration=24h0m0s
INFO  Cleaned up expired documents count=42 duration=245ms total_expired=1337
DEBUG TTL scan completed scanned_documents=10000 expired_documents=42
```

Metrics tracked:
- Total documents expired since leader became active
- Last cleanup duration
- Scanned vs expired document counts

## Modifying TTL Configuration

### Adding TTL to Existing Table

TTL can be added to an existing table through schema updates:
- Applies retroactively to all existing documents
- Documents already expired based on the new TTL are marked for immediate deletion

### Removing TTL

TTL can be removed by setting `ttl_duration` to empty:
- All expiration processing stops immediately
- All documents become permanent
- Previously expired documents remain (are not deleted)

### Changing TTL Duration

TTL duration can be changed through schema updates:
- New duration applies immediately to all documents
- All documents recalculate expiration using existing timestamps with new duration

## Implementation Details

### Separate TTL Timestamp Keys

**Performance Optimization**: TTL timestamps are stored as separate keys with `:t` suffix:
- **Fast Cleanup Scans**: O(1) per document - no JSON deserialization
- **Fast Query Filtering**: Single key lookup to check expiration
- **Efficient TTL Extension**: Update timestamp without rewriting document
- **Minimal Storage Overhead**: ~30 bytes per document (just the timestamp)

**Key Format:**
```
<baseKey>:t → "2025-01-01T12:00:00.123456789Z"
```

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

Documents are deleted in batches of 1000:
- Prevents overwhelming the system with large deletions
- Provides regular progress logging
- Maintains system responsiveness

### Performance Characteristics

**Cleanup Scan Performance:**
- Traditional approach: O(N × document_size) - must decompress and parse every document
- Optimized approach: O(N × 30 bytes) - only reads timestamp keys
- **Speedup**: ~100-1000x faster depending on document size

**Query Filtering Performance:**
- Single Pebble Get operation (~microseconds)
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
