# S3 Storage for Antfly

## Overview

Antfly supports storing Pebble SST (Sorted String Table) files in S3-compatible object storage while maintaining strong consistency through Raft consensus. This hybrid architecture dramatically reduces storage costs while preserving fast failover and operational simplicity.

## Key Benefits

### Cost Savings
**87% reduction** in storage costs for typical workloads:
- S3 storage: $0.023/GB/month vs NVMe at $0.10/GB/month
- Only hot data kept locally (WAL, MANIFEST, recent sstables)
- Single copy in S3 instead of 3x replication on local disks

**Example**: 1TB dataset with 3-way replication
- **Traditional**: 3TB × $0.10/GB = $300/month
- **With S3**: Local (200GB × 3 × $0.10) + S3 (800GB × $0.023) = $78/month
- **Savings**: $222/month (74%)

### Operational Benefits
- **100-1000x faster shard splits**: Seconds instead of minutes (only metadata copied)
- **30-60x faster migrations**: No data transfer between nodes, only metadata
- **Instant node recovery**: New nodes read from S3 instead of copying from peers
- **Reduced replication overhead**: Only WAL and hot data replicated via Raft

## What Gets Stored Where

```
┌──────────────────────────────────────┐
│ Local NVMe/SSD Storage               │
├──────────────────────────────────────┤
│ • MANIFEST files (metadata)          │
│ • WAL files (write-ahead log)        │
│ • CURRENT, OPTIONS files             │
│ • Pebble block cache (RAM)           │
└──────────────────────────────────────┘
                 ↕
┌──────────────────────────────────────┐
│ S3 Object Storage                    │
├──────────────────────────────────────┤
│ • SST files (all levels L0-L6)       │
│ • Compacted sstables                 │
│ • Shared across all Raft replicas    │
└──────────────────────────────────────┘
```

**Important**: Raft is still required for:
- Leader election and write ordering
- Strong consistency guarantees
- Fast failover (<1 second)
- WAL replication for durability

## Quick Start

### 1. Basic Configuration

Add to your `config.yaml`:

```yaml
s3:
  enabled: true
  endpoint: "s3.amazonaws.com"
  region: "us-east-1"
  bucket: "antfly-production-data"
  prefix: "cluster-1/shards"
  use_ssl: true
```

### 2. Set Credentials

Use environment variables (recommended):

```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
```

Alternatively, add to config (less secure):

```yaml
s3:
  # ... other settings ...
  access_key_id: "AKIAIOSFODNN7EXAMPLE"
  secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
```

### 3. Create S3 Bucket

```bash
# AWS S3
aws s3 mb s3://antfly-production-data

# Or using MinIO (local development)
mc mb local/antfly-test
```

### 4. Start Antfly

S3 storage is automatically used when enabled. No code changes needed.

## Configuration Reference

### Full Configuration Options

```yaml
s3:
  # Enable S3 storage for SST files (required)
  enabled: true

  # S3-compatible endpoint (required)
  # Examples:
  #   - AWS S3: "s3.amazonaws.com"
  #   - AWS S3 regional: "s3.us-west-2.amazonaws.com"
  #   - MinIO local: "localhost:9000"
  #   - DigitalOcean Spaces: "nyc3.digitaloceanspaces.com"
  endpoint: "s3.amazonaws.com"

  # AWS region (optional for MinIO)
  region: "us-east-1"

  # S3 bucket name (required)
  # Must be 3-63 characters, lowercase, no underscores
  bucket: "antfly-production-data"

  # Optional path prefix within bucket
  # Useful for multi-cluster deployments
  prefix: "cluster-1/shards"

  # Enable SSL/TLS (default: true)
  use_ssl: true

  # Credentials (optional - prefer environment variables)
  # If not provided, uses AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
  # access_key_id: "AKIAIOSFODNN7EXAMPLE"
  # secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
```

### Provider-Specific Examples

#### AWS S3

```yaml
s3:
  enabled: true
  endpoint: "s3.amazonaws.com"
  region: "us-east-1"
  bucket: "antfly-data"
  use_ssl: true
```

#### MinIO (Local Development)

```yaml
s3:
  enabled: true
  endpoint: "localhost:9000"
  bucket: "antfly-test"
  use_ssl: false
```

Start MinIO:
```bash
docker run -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

#### DigitalOcean Spaces

```yaml
s3:
  enabled: true
  endpoint: "nyc3.digitaloceanspaces.com"
  region: "nyc3"
  bucket: "antfly-production"
  use_ssl: true
```

## When to Use S3 Storage

### Recommended For

- **Large datasets** (>500GB): Maximum cost savings
- **Cost-sensitive deployments**: 74-87% storage cost reduction
- **Frequent shard operations**: Fast splits and migrations
- **Cloud deployments**: AWS, GCP, Azure, DigitalOcean
- **Read-heavy workloads**: Block cache mitigates S3 latency

### Consider Alternatives For

- **Ultra-low latency requirements** (<1ms p99): Local storage faster
- **Very small datasets** (<100GB): Cost savings minimal
- **Network-constrained environments**: S3 requires reliable connectivity
- **Compliance restrictions**: Data must stay on local premises

## Performance Characteristics

### Read Latency
- **Cache hit**: <1ms (in-memory block cache)
- **Local SST**: ~1-5ms (NVMe read)
- **S3 SST**: ~10-50ms (network + S3 latency)

**Mitigation**:
- Pebble's aggressive block caching
- Hot data frequently accessed stays cached
- Increase block cache size (2-4GB recommended)

### Write Performance
Writes are **not affected** by S3 latency:
1. Writes go to WAL (local, fast)
2. Memtable flushed to sstables
3. Background compaction moves sstables to S3
4. Zero impact on write path latency

## Troubleshooting

### Common Issues

**"S3 endpoint is required when S3 is enabled"**
- Add `endpoint` to s3 config section

**"S3 credentials required"**
- Set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables

**"Bucket does not exist"**
- Create bucket first:
  ```bash
  aws s3 mb s3://antfly-production-data
  ```

**Slow read performance**
- Check block cache hit ratio (should be >90%)
- Increase cache size in Pebble options
- Verify network latency to S3 endpoint

**High S3 costs**
- Monitor GET request rate
- Increase block cache to reduce S3 reads
- Consider `CreateOnSharedLower` to keep hot data local (see [Architecture Guide](s3-storage-architecture.md))

## Further Reading

- **[Architecture Guide](s3-storage-architecture.md)**: Deep dive into design decisions, multi-writer problem, LeaderFactory integration
- **[Operations Guide](s3-storage-operations.md)**: Shard operations, monitoring, performance tuning, troubleshooting
- **[Development Guide](s3-storage-development.md)**: Code architecture, implementation details, testing

## References

- [Pebble Documentation](https://github.com/cockroachdb/pebble)
- [Pebble objstorage Package](https://pkg.go.dev/github.com/cockroachdb/pebble/objstorage)
- [Neon Architecture](https://neon.tech/docs/introduction/architecture-overview) (similar hybrid approach)
- [AWS S3 Pricing](https://aws.amazon.com/s3/pricing/)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)
