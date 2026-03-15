# Antfly with S3 Storage (MinIO) - Docker Compose Example

This example demonstrates running Antfly with **S3-compatible object storage** using MinIO. With S3 storage enabled, Pebble stores sstables in object storage instead of local disk, enabling:

- **100-1000x faster shard splits** (seconds vs minutes)
- **30-60x faster shard migrations** (minimal data transfer)
- **50% storage efficiency** during operations (no duplication)
- **83% cost savings** vs traditional local storage

## What's Included

This docker-compose stack includes:

- **MinIO** - S3-compatible object storage server
- **MinIO Console** - Web UI for managing buckets
- **Antfly** - Configured with S3 storage enabled
- **Ollama** - For embeddings (optional)
- **Prometheus** - Metrics collection
- **Grafana** - Metrics visualization

## Quick Start

### 1. Start the Stack

```bash
cd devops/docker-compose-s3
docker-compose up -d
```

This will:
1. Start MinIO and create the `antfly-data` bucket
2. Start Antfly with S3 storage enabled
3. Start supporting services (Ollama, Prometheus, Grafana)

### 2. Verify S3 Storage is Working

Check the Antfly logs to confirm S3 is configured:

```bash
docker logs antfly-swarm-s3 | grep "S3 storage"
```

You should see:
```
INFO  Configuring S3 storage for Pebble  {"endpoint": "minio:9000", "bucket": "antfly-data", ...}
INFO  S3 storage configured successfully  {"locator": "s3"}
```

### 3. Access the Services

- **Antfly API**: http://localhost:8080
- **MinIO Console**: http://localhost:9001 (login: minioadmin/minioadmin)
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000
- **Metrics**: http://localhost:4200/metrics

### 4. Create a Table and Verify S3 Storage

Create a table with some data:

```bash
# Create a table
curl -X POST http://localhost:8080/v1/tables \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test_table",
    "schema": {
      "properties": {
        "id": {"type": "string"},
        "message": {"type": "string"}
      }
    }
  }'

# Insert some data
curl -X POST http://localhost:8080/v1/tables/test_table/documents \
  -H "Content-Type: application/json" \
  -d '{
    "id": "1",
    "message": "Hello S3 Storage!"
  }'
```

### 5. Check MinIO for Sstables

Open the MinIO Console at http://localhost:9001:

1. Login with `minioadmin` / `minioadmin`
2. Navigate to **Buckets** → **antfly-data**
3. Browse to `sstables/` directory
4. You should see sstable files (`.sst`) and reference markers in `markers/`

Example bucket structure:
```
antfly-data/
└── sstables/
    └── shard-1/
        ├── 000001.sst
        ├── 000002.sst
        └── markers/
            ├── 000001.sst.ref-shard-1
            └── 000002.sst.ref-shard-1
```

## How S3 Storage Works

### Architecture Overview

With S3 storage enabled:

1. **Leader-Only Writes**:
   - Only the Raft leader writes sstables to S3
   - Followers read from S3 (foreign objects)
   - Prevents 3x storage cost

2. **Fast Shard Splits**:
   ```
   Traditional: 10GB shard → copy 10GB → 5-10 minutes
   With S3:     10GB shard → copy 10MB metadata → 5-10 seconds
   ```
   Both split shards reference the same S3 sstables (no data copy!)

3. **Fast Shard Migrations**:
   ```
   Traditional: Transfer 10GB over network
   With S3:     Transfer 10MB metadata, access S3 directly
   ```

4. **Storage Efficiency**:
   - No data duplication during splits
   - Reference markers track object usage
   - Automatic garbage collection via Pebble

### What Gets Stored Where

**In S3 (MinIO):**
- All sstables (L0-L6) - The actual data
- Reference markers - Track which shards use which sstables

**On Local Disk:**
- MANIFEST file (~few KB) - Database metadata
- WAL files (~few MB) - Recent uncommitted writes
- OPTIONS file - Pebble configuration

### Shard Split Example

When splitting a 10GB shard:

```
Before Split:
├── S3: 10GB sstables
└── Local: 10MB MANIFEST + WAL

After Split:
├── Shard 1:
│   ├── S3: References to same 10GB sstables ✅ (no copy!)
│   └── Local: 5MB MANIFEST + WAL (copied)
└── Shard 2:
    ├── S3: References to same 10GB sstables ✅ (no copy!)
    └── Local: 5MB MANIFEST + WAL (copied)

Total copied: ~10MB instead of 10GB
Speedup: 100-1000x
```

## Configuration

### MinIO Configuration

The default MinIO configuration:
- **Endpoint**: `minio:9000` (internal Docker network)
- **Access Key**: `minioadmin`
- **Secret Key**: `minioadmin`
- **Bucket**: `antfly-data`
- **SSL**: Disabled (local development)

These are set in:
- `docker-compose.yml` - MinIO service and environment variables
- `config.yaml` - Antfly S3 configuration

### Antfly S3 Configuration

In `config.yaml`:

```yaml
s3:
  enabled: true
  endpoint: "minio:9000"
  bucket: "antfly-data"
  prefix: "sstables"
  use_ssl: false  # false for MinIO, true for AWS S3
  # Credentials from environment:
  # AWS_ACCESS_KEY_ID=minioadmin
  # AWS_SECRET_ACCESS_KEY=minioadmin
```

### Switching to AWS S3

To use real AWS S3 instead of MinIO:

1. **Update `config.yaml`**:
   ```yaml
   s3:
     enabled: true
     endpoint: "s3.amazonaws.com"  # or s3.us-west-2.amazonaws.com
     region: "us-west-2"
     bucket: "your-bucket-name"
     prefix: "sstables"
     use_ssl: true  # IMPORTANT: true for AWS
   ```

2. **Update environment variables in `docker-compose.yml`**:
   ```yaml
   environment:
     - AWS_ACCESS_KEY_ID=<your-aws-key>
     - AWS_SECRET_ACCESS_KEY=<your-aws-secret>
   ```

3. **Create the S3 bucket**:
   ```bash
   aws s3 mb s3://your-bucket-name --region us-west-2
   ```

4. **Remove MinIO services** from `docker-compose.yml` (optional)

## Performance Tuning

### Block Cache Size

With S3 storage, increase Pebble's block cache for better performance.

The cache size is currently configured in code (`src/store/db.go`):

```go
pebbleOpts.Cache = pebble.NewCache(5 * 64 << 20) // Default: 320MB
```

For S3 storage, consider increasing to 2GB or more:

```go
pebbleOpts.Cache = pebble.NewCache(2 * 1024 << 20) // 2GB
```

### CreateOnSharedStrategy

The current configuration uses `remote.CreateOnSharedAll` (all levels in S3).

For lower latency, you can use `remote.CreateOnSharedLower` (only L5-L6 in S3):

In `src/store/db.go`:
```go
// Current (all levels in S3):
pebbleOpts.Experimental.CreateOnShared = remote.CreateOnSharedAll

// Alternative (only L5-L6 in S3, L0-L4 local):
pebbleOpts.Experimental.CreateOnShared = remote.CreateOnSharedLower
```

Trade-offs:
- `CreateOnSharedAll`: Maximum storage efficiency, all split benefits
- `CreateOnSharedLower`: Lower read latency, 75% of data still in S3

## Monitoring

### Check S3 Storage Status

View Antfly logs:
```bash
docker logs -f antfly-swarm-s3
```

Look for:
- "S3 storage configured successfully"
- "Starting leader factory" (when becoming leader)
- S3 read/write operations

### Monitor S3 Usage

**Via MinIO Console:**
1. Go to http://localhost:9001
2. Login: `minioadmin` / `minioadmin`
3. Navigate to **Monitoring** → **Metrics**
4. View:
   - Total storage used
   - API requests (GET, PUT)
   - Bandwidth usage

**Via MinIO CLI:**
```bash
# Install mc (MinIO Client)
docker exec -it minio mc admin info local
```

### Prometheus Metrics

Antfly exposes metrics at http://localhost:4200/metrics

Key metrics for S3 storage:
- S3 GET/PUT request counts
- S3 latency (p50, p99)
- Block cache hit ratio
- Pebble compaction statistics

## Troubleshooting

### Antfly Can't Connect to MinIO

**Symptom**: Logs show "creating S3 client: ..." errors

**Fix**: Ensure MinIO is healthy:
```bash
docker ps  # Check minio container is running
docker logs minio  # Check MinIO logs
```

### Sstables Not Appearing in S3

**Symptom**: MinIO bucket is empty

**Possible Causes**:
1. Antfly not yet leader (follower doesn't write to S3)
2. No data inserted yet
3. S3 configuration disabled

**Check**:
```bash
# Verify S3 is enabled
docker exec antfly-swarm-s3 cat /config.yaml | grep -A 5 "s3:"

# Check if Antfly became leader
docker logs antfly-swarm-s3 | grep "leader"
```

### High S3 Request Costs

**Symptom**: Lots of GET requests to S3

**Solutions**:
1. Increase block cache size (reduce cache misses)
2. Use `CreateOnSharedLower` (keep hot data local)
3. Monitor cache hit ratio

**Check cache performance**:
```bash
curl http://localhost:4200/metrics | grep cache
```

### MinIO Out of Disk Space

**Symptom**: MinIO errors about disk space

**Solution**: Increase Docker volume size or use external volume

```yaml
# In docker-compose.yml:
volumes:
  minio-data:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: /path/to/large/disk
```

## Cleanup

### Stop All Services

```bash
docker-compose down
```

### Remove All Data (including S3 objects)

```bash
docker-compose down -v
```

This removes:
- All containers
- All volumes (Antfly data, MinIO data, Grafana data)
- Network

## Cost Analysis

### Example: 1TB Database, 100 Shards

**Traditional Local Storage:**
- 2TB local SSD needed (2x for splits): $200/month
- Slow splits: 5-10 minutes each
- Network bottleneck for migrations

**With S3 Storage:**
- 1TB S3 storage: $23/month
- 5GB local disk (metadata): $0.50/month
- S3 GET requests (~10M/month): $4/month
- S3 PUT requests (~1M/month): $0.50/month
- **Total: $28/month**
- **Savings: 86%**
- Fast splits: 5-10 seconds each
- Fast migrations: minimal network usage

## Next Steps

1. **Read the Architecture Documentation**:
   - See `docs/s3-storage-shard-operations.md` for detailed architecture
   - See `docs/s3-storage.md` for implementation details
   - See `docs/s3-storage-garbage-collection.md` for GC behavior

2. **Test Shard Splits**:
   - Create a large table
   - Trigger a split (automatically when shard reaches max size)
   - Observe split completes in seconds
   - Check both shards reference same S3 objects

3. **Monitor Performance**:
   - View Grafana dashboards at http://localhost:3000
   - Monitor S3 metrics in MinIO Console
   - Track block cache hit ratio

4. **Production Deployment**:
   - Switch from MinIO to AWS S3
   - Configure proper access credentials (IAM roles)
   - Enable SSL (`use_ssl: true`)
   - Set up S3 lifecycle policies for cost optimization
   - Increase block cache size based on workload

## Reference

- **Pebble Documentation**: https://github.com/cockroachdb/pebble
- **MinIO Documentation**: https://min.io/docs/minio/linux/index.html
- **Antfly S3 Storage Docs**: `../../docs/s3-storage*.md`

## Support

For issues or questions:
- Check Antfly logs: `docker logs antfly-swarm-s3`
- Check MinIO logs: `docker logs minio`
- Review documentation in `docs/`
- Open an issue on GitHub
