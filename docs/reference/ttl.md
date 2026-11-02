# Document TTL

A table-level TTL policy expires documents a fixed duration after a timestamp
field. Expired documents disappear from reads immediately and are deleted in the
background.

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

## Duration Format

Durations use integer components with the units `ns`, `us`, `ms`, `s`, `m`, `h`,
and `d`, for example `30s`, `5m`, `24h`, or `7d` (168h).

## Extending a Document's TTL

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

## Changing TTL on an Existing Table

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

## How Expiry Works

- Reads (gets and searches) hide expired documents as soon as they expire.
- Physical deletion runs on the Raft leader, scheduled every 30 seconds, with a
  5-second grace period. Deletion can lag further under load.
- TTL relies on synchronized clocks across cluster nodes; run NTP.
- There are no dedicated TTL cleanup logs or metrics. Table and storage status
  reports aggregate storage state, not deletion deadlines.
- Hosted embedded profiles do not run background TTL cleanup. The embedded
  `ttl_cleanup_batch_size` option sets the cleanup page size (0 keeps the default;
  pages are capped at 128 candidates).
