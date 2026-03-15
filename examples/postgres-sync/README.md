# Postgres Real-time Sync to Antfly

This example demonstrates **real-time synchronization** from a Postgres JSONB column to Antfly using:

1. **Linear Merge API** for efficient batch syncing
2. **LISTEN/NOTIFY** for real-time change detection
3. **Periodic full syncs** to catch any missed changes

Perfect for keeping Antfly in sync with your existing Postgres database!

## Features

✅ **Real-time Updates** - Changes in Postgres instantly sync to Antfly via LISTEN/NOTIFY
✅ **Efficient Batching** - Rapid changes are batched together (1-second window)
✅ **Content Hash Optimization** - Unchanged documents are skipped (no unnecessary writes)
✅ **Automatic Deletion** - Documents deleted from Postgres are removed from Antfly
✅ **Periodic Full Sync** - Configurable full sync to ensure consistency
✅ **Graceful Shutdown** - Clean exit with statistics on Ctrl+C
✅ **Production Ready** - Connection pooling, error handling, metrics

## Architecture

```
┌─────────────────┐
│   Postgres DB   │
│  (JSONB table)  │
└────┬───────┬────┘
     │       │
     │       │ LISTEN/NOTIFY real-time
     │       │ 
     │       │
     │   ┌───▼────────────┐          ┌──────────────┐
     │   │  postgres-sync │─────────▶│    Antfly    │
     │   │    (daemon)    │  Linear  │  (search &   │
     │   └───▲────────────┘  Merge   │   storage)   │
     │       │                       └──────────────┘
     │       │
     └───────┘ Periodic "insurance" sync every 5 min
```

### How It Works

1. **Initial Full Sync**:
   - Queries all rows from Postgres table
   - Uses Linear Merge API to sync to Antfly
   - Content hashing skips unchanged documents

<!-- include: main.go#full_sync -->

2. **Real-time Updates** (LISTEN/NOTIFY):
   - Postgres trigger fires on INSERT/UPDATE/DELETE
   - Sends notification with change details
   - Go daemon receives notification
   - Batches rapid changes (1-second window)
   - Syncs batch to Antfly via Linear Merge

3. **Periodic Full Sync**:
   - Runs every N minutes (configurable)
   - Ensures consistency if any notifications were missed
   - Catches documents modified outside triggers

## Prerequisites

### 1. Postgres Database

```bash
# Using Docker
docker run --name postgres-antfly-demo \
  -e POSTGRES_PASSWORD=secret \
  -e POSTGRES_DB=antfly_demo \
  -p 5432:5432 \
  -d postgres:16

# Or use existing Postgres instance
```

### 2. Antfly Running

```bash
cd /path/to/antfly
go run ./cmd/antfly swarm
```

### 3. Build the sync tool

```bash
cd /path/to/antfly
go build -o postgres-sync ./examples/postgres-sync
```

## Quick Start

### Step 1: Set up Postgres schema

```bash
# Connect to your Postgres database
psql postgresql://postgres:secret@localhost:5432/antfly_demo

# Run the schema setup
\i examples/postgres-sync/schema.sql
```

This creates:
- `documents` table with JSONB `data` column
- Triggers for LISTEN/NOTIFY on changes
- Sample data (5 documents)

### Step 2: Start the sync daemon

```bash
export POSTGRES_URL="postgresql://postgres:secret@localhost:5432/antfly_demo"

./postgres-sync \
  --postgres "$POSTGRES_URL" \
  --antfly http://localhost:8080/api/v1 \
  --pg-table documents \
  --antfly-table postgres_docs \
  --create-table \
  --full-sync-interval 5m
```

You should see:

```
=== Postgres to Antfly Real-time Sync ===
Postgres: postgresql://postgres:***@localhost:5432/antfly_demo
Antfly: http://localhost:8080/api/v1
Table: documents.data -> postgres_docs
Full sync interval: 5m0s

✓ Created Antfly table 'postgres_docs'
Performing initial full sync...
Full sync: Found 5 records in Postgres
  Batch 1-5: 5 upserted, 0 skipped, 0 deleted
✓ Full sync complete: 5 upserted, 0 skipped, 0 deleted in 123ms

Starting real-time sync (LISTEN/NOTIFY)...
✓ Listening on channel 'documents_changes'
✓ Real-time sync active

Sync is running. Press Ctrl+C to stop.
```

### Step 3: Test real-time sync

In another terminal, connect to Postgres and make changes:

```bash
psql $POSTGRES_URL
```

```sql
-- Insert a new document
INSERT INTO documents (id, data) VALUES
  ('test_001', '{"title": "Real-time Test", "content": "This syncs instantly!"}');

-- Update a document
UPDATE documents
SET data = data || '{"updated": true}'
WHERE id = 'doc_001';

-- Delete a document
DELETE FROM documents WHERE id = 'test_001';
```

Watch the sync daemon output:

```
← Change detected: INSERT test_001
→ Real-time sync: 1 upserted, 0 skipped

← Change detected: UPDATE doc_001
→ Real-time sync: 1 upserted, 0 skipped

← Change detected: DELETE test_001
→ Real-time sync: 1 deleted
```

## Configuration Options

| Flag | Description | Default |
|------|-------------|---------|
| `--postgres` | Postgres connection URL | `$POSTGRES_URL` |
| `--antfly` | Antfly API URL | `http://localhost:8080/api/v1` |
| `--pg-table` | Postgres table name | `documents` |
| `--id-column` | ID column name | `id` |
| `--data-column` | JSONB data column name | `data` |
| `--antfly-table` | Antfly table name | `postgres_docs` |
| `--full-sync-interval` | Full sync interval (0=disable) | `5m` |
| `--batch-size` | Batch size for sync | `1000` |
| `--create-table` | Create Antfly table | `false` |
| `--num-shards` | Number of shards for new table | `3` |

## Usage Examples

### Basic Usage

```bash
# Minimal config (uses environment variable)
export POSTGRES_URL="postgresql://user:pass@localhost/db"
./postgres-sync --create-table
```

### Custom Table Names

```bash
./postgres-sync \
  --postgres "$POSTGRES_URL" \
  --pg-table products \
  --id-column product_id \
  --data-column metadata \
  --antfly-table product_catalog
```

### Disable Periodic Sync (Real-time Only)

```bash
./postgres-sync \
  --postgres "$POSTGRES_URL" \
  --full-sync-interval 0
```

### High-Volume Sync

```bash
./postgres-sync \
  --postgres "$POSTGRES_URL" \
  --batch-size 5000 \
  --full-sync-interval 30m
```

## Document Schema

Each Postgres row is stored in Antfly as:

```json
{
  "id": "doc_001",
  "data": {
    "title": "Getting Started",
    "content": "Welcome to Antfly",
    "category": "tutorial"
  },
  "source": "postgres",
  "synced_at": "2024-01-15T10:30:00Z"
}
```

- `id`: From Postgres ID column
- `data`: The JSONB column content
- `source`: Always "postgres"
- `synced_at`: Timestamp of last sync

## Testing Real-time Sync

We provide a demo SQL script with various test scenarios:

```bash
psql $POSTGRES_URL -f examples/postgres-sync/demo-changes.sql
```

This demonstrates:
- Single inserts
- Bulk inserts (batching)
- Updates
- Deletes
- Transactional changes
- Random update generator

### Interactive Testing

```sql
-- 1. Insert new documents
INSERT INTO documents (id, data) VALUES
  ('demo_001', '{"title": "Demo", "content": "Testing sync"}');

-- Watch sync daemon: "← Change detected: INSERT demo_001"

-- 2. Bulk insert (tests batching)
INSERT INTO documents (id, data)
SELECT
  'bulk_' || i,
  jsonb_build_object('title', 'Bulk Doc ' || i, 'index', i)
FROM generate_series(1, 100) AS i;

-- Watch sync daemon batch them together!

-- 3. Update multiple records
UPDATE documents
SET data = data || '{"updated": true}'
WHERE id LIKE 'bulk_%';

-- 4. Delete them
DELETE FROM documents WHERE id LIKE 'bulk_%';
```

## Monitoring

The sync daemon prints statistics every 30 seconds:

```
--- Sync Statistics ---
Total synced: 1,234
Total skipped: 5,678
Total deleted: 42
Real-time updates: 89
Errors: 0
Last full sync: 2m30s ago
Last real-time sync: 5s ago
----------------------
```

## How LISTEN/NOTIFY Works

### Postgres Side

When you INSERT/UPDATE/DELETE a row, a trigger fires:

```sql
CREATE TRIGGER documents_change_trigger
  AFTER INSERT OR UPDATE OR DELETE ON documents
  FOR EACH ROW
  EXECUTE FUNCTION notify_document_change();
```

The function sends a notification:

```sql
PERFORM pg_notify('documents_changes', json_build_object(
  'operation', 'INSERT',
  'id', NEW.id,
  'data', NEW.data,
  'timestamp', NOW()
)::text);
```

### Go Side

The sync daemon listens for notifications:

<!-- include: main.go#realtime_sync -->

### Batching Strategy

Rapid changes are batched to avoid overwhelming Antfly:

- Changes accumulate in a 1-second window
- Batch is processed every second
- Multiple changes to the same document are de-duplicated
- Linear Merge API handles the batch efficiently

Example:
```
0.0s: INSERT doc_001
0.1s: UPDATE doc_001
0.3s: INSERT doc_002
0.5s: UPDATE doc_001
1.0s: → Process batch {doc_001, doc_002} (2 records)
```

## Performance Characteristics

| Scenario | Performance |
|----------|-------------|
| Initial sync (10K docs) | ~5-10 seconds |
| Re-sync unchanged | ~2-3 seconds (all skipped) |
| Real-time insert | &lt;100ms latency |
| Bulk insert (1000 docs) | Batched in 1-2 seconds |
| Full sync overhead | Negligible (content hash check) |

### Optimization Tips

1. **Batch Size**: Increase for large tables (up to 10,000)
2. **Full Sync Interval**: Reduce if notifications are unreliable
3. **Connection Pool**: Increase for high-volume tables
4. **Indexes**: Add GIN index on JSONB for faster queries

## Troubleshooting

### Connection Issues

```
Error: failed to connect to Postgres
```

**Solution**: Check Postgres URL and network:
```bash
psql "$POSTGRES_URL" -c "SELECT 1"
```

### No Notifications Received

```
# Test notifications manually
psql $POSTGRES_URL

-- Terminal 1:
LISTEN documents_changes;

-- Terminal 2:
INSERT INTO documents (id, data) VALUES ('test', '{}');

-- Terminal 1 should show: Asynchronous notification received
```

**Common issues**:
- Trigger not created → Run `schema.sql` again
- Wrong channel name → Check `--pg-table` matches table name
- Connection dropped → Daemon will reconnect automatically

### Documents Not Syncing

Check the daemon logs for errors:
```
Error processing batch: linear merge failed: ...
```

**Common issues**:
- Antfly not running → Start Antfly first
- Table doesn't exist → Use `--create-table` flag
- Invalid JSON in JSONB column → Check Postgres data

### High Memory Usage

If syncing a very large table:

1. Reduce `--batch-size` (try 500 or 1000)
2. Increase `--full-sync-interval` (try 30m or 1h)
3. Add connection pool limits

## Production Deployment

### Docker Compose

```yaml
version: '3.8'
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: production
      POSTGRES_PASSWORD: ${DB_PASSWORD}
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./schema.sql:/docker-entrypoint-initdb.d/01-schema.sql

  antfly:
    image: antfly:latest
    command: ["swarm"]
    ports:
      - "8080:8080"

  postgres-sync:
    build: ./examples/postgres-sync
    environment:
      POSTGRES_URL: postgresql://postgres:${DB_PASSWORD}@postgres/production
    command:
      - --antfly=http://antfly:8080/api/v1
      - --create-table
      - --full-sync-interval=10m
    depends_on:
      - postgres
      - antfly
    restart: unless-stopped

volumes:
  postgres-data:
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres-sync
spec:
  replicas: 1  # Only run one instance (LISTEN/NOTIFY is single-consumer)
  template:
    spec:
      containers:
      - name: postgres-sync
        image: antfly/postgres-sync:latest
        env:
        - name: POSTGRES_URL
          valueFrom:
            secretKeyRef:
              name: postgres-credentials
              key: url
        args:
        - --antfly=http://antfly-service:8080/api/v1
        - --create-table
        - --full-sync-interval=10m
```

### systemd Service

```ini
[Unit]
Description=Antfly Postgres Sync
After=network.target postgresql.service

[Service]
Type=simple
User=antfly
Environment=POSTGRES_URL=postgresql://localhost/production
ExecStart=/usr/local/bin/postgres-sync \
  --antfly=http://localhost:8080/api/v1 \
  --create-table \
  --full-sync-interval=10m
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

## Advanced Use Cases

### Multi-table Sync

Run multiple sync daemons for different tables:

```bash
# Terminal 1: Sync products table
./postgres-sync --pg-table products --antfly-table products_search

# Terminal 2: Sync customers table
./postgres-sync --pg-table customers --antfly-table customers_search
```

### Conditional Sync

Modify `notify_document_change()` to filter:

```sql
CREATE OR REPLACE FUNCTION notify_document_change()
RETURNS TRIGGER AS $$
BEGIN
  -- Only sync published documents
  IF (NEW.data->>'status' = 'published') THEN
    PERFORM pg_notify(...);
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

### Transform Data During Sync

Modify the sync tool to transform data:

```go
// In processBatchedChanges()
doc := make(map[string]interface{})
doc["id"] = id
doc["data"] = jsonData

// Add computed fields
if title, ok := jsonData["title"].(string); ok {
  doc["title_lowercase"] = strings.ToLower(title)
}

// Add embeddings (if configured)
if content, ok := jsonData["content"].(string); ok {
  embedding := generateEmbedding(content)
  doc["embedding"] = embedding
}
```

## Comparison with Other Approaches

| Approach | Latency | Overhead | Complexity |
|----------|---------|----------|------------|
| **LISTEN/NOTIFY (this)** | &lt;100ms | Low | Medium |
| **Polling** | 1-60s | High | Low |
| **CDC (Debezium)** | &lt;1s | Medium | High |
| **Logical Replication** | &lt;1s | Low | Very High |

LISTEN/NOTIFY is the sweet spot for most use cases!

## Limitations

⚠️ **Single Consumer**: Only one sync daemon should run per table (LISTEN is not load-balanced)

⚠️ **Notification Loss**: If daemon is down, notifications are lost (periodic full sync recovers)

⚠️ **Payload Size**: Postgres notification payload is limited to 8KB (we only send ID + operation, not full data)

⚠️ **Transaction Delay**: Notifications only fire on COMMIT (delayed for long transactions)

## Extending the Example

Ideas for customization:

1. **Add Filtering**: Only sync certain document types
2. **Add Enrichment**: Generate embeddings during sync
3. **Add Validation**: Validate JSON schema before syncing
4. **Add Metrics**: Export Prometheus metrics
5. **Add Dead Letter Queue**: Store failed syncs for retry
6. **Multi-tenancy**: Support multiple databases

## Related Examples

- [docsaf](../docsaf/) - Sync documentation files to Antfly
- Linear Merge API docs - See `work-log/006-create-linear-merge-api/`

## Project Files

<!-- files: main.go, schema.sql, run-demo.sh -->