# S3 Storage Operations Guide

## Overview

This guide covers operational aspects of running Antfly with S3 storage in production, including shard operations, performance tuning, monitoring, and troubleshooting.

## Table of Contents

- [Shard Operations](#shard-operations)
- [Performance Tuning](#performance-tuning)
- [Monitoring and Observability](#monitoring-and-observability)
- [Garbage Collection Management](#garbage-collection-management)
- [Failure Scenarios](#failure-scenarios)
- [Troubleshooting](#troubleshooting)

## Shard Operations

### Shard Splitting

One of the major benefits of S3 storage is dramatically faster shard splits.

#### Performance Comparison

**Traditional (local storage)**:
```
10GB shard → split
  Time: 5-10 minutes
  Process:
    1. Copy 5GB for shard 1: 5-10 min
    2. Copy 5GB for shard 2: 5-10 min
    3. Temporary 2x storage (20GB total)
```

**With S3 storage**:
```
10GB shard → split
  Time: 5-10 seconds
  Process:
    1. Checkpoint shard 1: 5 sec (~5MB metadata)
    2. Checkpoint shard 2: 5 sec (~5MB metadata)
    3. Both reference same S3 objects (10GB shared)

Speedup: 100-1000x faster
```

#### How to Monitor Splits

**Key metrics**:
- Split operation duration (should be <10 seconds)
- Checkpoint size (should be <100MB)
- Data copied during split (should be minimal)

**Health checks after a split**:
```bash
# 1. Verify both shards have separate MANIFEST files
ls -lh /data/shard-124/MANIFEST* /data/shard-125/MANIFEST*

# 2. Check S3 for reference markers
aws s3 ls s3://bucket/shard-123/markers/ | grep -E "(shard-124|shard-125)"

# 3. Test reads from both shards
# Should work immediately without copying data
```

#### Post-Split Lifecycle

**Immediately after split**:
- Both shards reference same S3 sstables (inherited from parent)
- Each has separate MANIFEST and WAL files
- Each creates reference markers for shared objects

**After compaction (hours/days later)**:
- Shard 1 compacts keys A-M → new S3 sstable (only A-M data)
- Shard 2 compacts keys N-Z → new S3 sstable (only N-Z data)
- Original shared sstables deleted when all markers removed
- No manual cleanup needed!

### Shard Migration

Moving a shard from Node A to Node B is dramatically faster with S3.

#### Performance Comparison

**Traditional (local storage)**:
```
100GB shard migration
  Time: 30-60 minutes
  Network: 100GB transfer
  Process:
    1. Snapshot creation: 5 min
    2. Network transfer: 25-50 min
    3. Snapshot load: 5 min
```

**With S3 storage**:
```
100GB shard migration
  Time: 30-60 seconds
  Network: ~50MB transfer
  Process:
    1. Checkpoint creation: 5 sec
    2. Metadata transfer: 20 sec (50MB)
    3. Checkpoint load: 5 sec
    4. Node B accesses S3 directly: instant

Speedup: 30-60x faster
Cost savings: 99.95% less network traffic
```

#### Migration Procedure

1. **Create checkpoint on Node A**:
   ```go
   db.pdb.Checkpoint(destDir, pebble.WithFlushedWAL())
   ```

2. **Transfer checkpoint metadata to Node B** (~50MB):
   ```bash
   scp -r /data/checkpoint node-b:/data/shard-123
   ```

3. **Node B opens checkpoint**:
   - Reads object catalog metadata
   - Gets list of S3 objects needed
   - Accesses sstables directly from S3

4. **Node B becomes Raft leader**:
   - `LeaderFactory` sets `isLeader = true`
   - Starts writing new sstables to S3
   - Creates reference markers

5. **Verify migration**:
   ```bash
   # Check new node can read from S3
   # Check new node became Raft leader
   # Verify new sstables written to S3
   ```

#### Health Checks After Migration

```bash
# 1. Verify new node can read from S3
aws s3 ls s3://bucket/shard-123/

# 2. Check new node became Raft leader
# (check logs for "Starting leader factory")

# 3. Confirm new sstables written to S3 by new leader
aws s3 ls s3://bucket/shard-123/ --recursive | tail -10

# 4. Monitor checkpoint transfer size (should be small)
du -sh /data/checkpoint
```

## Performance Tuning

### Block Cache Configuration

With S3 storage, increase Pebble's block cache significantly:

```go
// Before (local storage only):
pebbleOpts.Cache = pebble.NewCache(5 * 64 << 20) // 320MB

// Recommended (with S3 storage):
pebbleOpts.Cache = pebble.NewCache(2 * 1024 << 20) // 2GB

// For large deployments:
pebbleOpts.Cache = pebble.NewCache(4 * 1024 << 20) // 4GB
```

**Target**: Cache hit ratio >90% to minimize S3 reads.

### Read Performance

**Latency characteristics**:
- Cache hit: <1ms (in-memory)
- Local SST: ~1-5ms (NVMe)
- S3 SST: ~10-50ms (network + S3)

**Optimization strategies**:
1. Increase block cache size
2. Monitor cache hit ratio
3. Consider `CreateOnSharedLower` for hot data
4. Use read-ahead for sequential scans

### S3 Request Optimization

**GET request costs**: $0.0004 per 1,000 requests

**Reduce request volume**:
- Larger block cache (fewer cache misses)
- Pebble automatically uses HTTP range requests
- Batch operations when possible

**Monitor costs**:
```bash
# Check S3 GET request metrics
aws cloudwatch get-metric-statistics \
  --namespace AWS/S3 \
  --metric-name GetRequests \
  --dimensions Name=BucketName,Value=antfly-data \
  --start-time 2024-01-01T00:00:00Z \
  --end-time 2024-01-02T00:00:00Z \
  --period 3600 \
  --statistics Sum
```

### CreateOnSharedAll vs CreateOnSharedLower

#### CreateOnSharedAll (Current Default)

All sstables (L0-L6) go to S3.

**Best for**:
- Cost-sensitive deployments
- Large databases (>1TB)
- Frequent shard splits/migrations

**Trade-offs**:
- Higher S3 read latency
- More S3 GET requests
- Requires good block cache

#### CreateOnSharedLower (Alternative)

Only L5-L6 go to S3, L0-L4 stay local.

**Best for**:
- Latency-sensitive workloads
- Smaller databases (<500GB)
- Read-heavy workloads

**Trade-offs**:
- Higher local disk requirements
- Slower splits (need to copy L0-L4)

**Configuration**:
```go
pebbleOpts.Experimental.CreateOnShared = remote.CreateOnSharedLower
// ~75% of data in S3, 25% local
```

## Monitoring and Observability

### Key Metrics

#### 1. Split Performance
```
# Metric: shard_split_duration_seconds
# Target: <10 seconds
# Alert: >30 seconds

# Metric: shard_split_bytes_copied
# Target: <100MB
# Alert: >1GB (indicates S3 not working)
```

#### 2. S3 Operations
```
# Metric: s3_get_requests_total
# Monitor rate and cost

# Metric: s3_latency_seconds
# Target: p50 < 50ms, p99 < 200ms

# Metric: s3_error_rate
# Alert: >1%

# Metric: s3_bytes_read_total
# Monitor trends
```

#### 3. Block Cache
```
# Metric: pebble_block_cache_hit_ratio
# Target: >90%
# Alert: <80%

# Metric: pebble_block_cache_size_bytes
# Monitor utilization

# Metric: pebble_block_cache_evictions_total
# Should be stable, not growing
```

#### 4. Leadership
```
# Metric: raft_leadership_changes_total
# Monitor for instability

# Metric: s3_write_attempts_total{role="leader"}
# Only leader should have non-zero

# Metric: s3_write_attempts_total{role="follower", result="rejected"}
# Followers rejected (expected)
```

### Logging

**Important log events to monitor**:

```
# Leader starts S3 writes
"Starting leader factory" s3Enabled=true

# S3 write success (leader)
"S3 sstable uploaded" object=shard-123/000456.sst size=10MB

# S3 write rejection (follower - expected)
"not raft leader, cannot write to S3" object=shard-123/000456.sst

# S3 read (all replicas)
"S3 sstable read" object=shard-123/000456.sst latency=25ms

# Split operation
"Shard split completed" duration=5s checkpoint_size=10MB
```

### Dashboards

**Recommended Grafana dashboard panels**:

1. **S3 Operations Overview**:
   - GET/PUT request rate
   - S3 latency (p50, p99)
   - Error rate
   - Cost estimate

2. **Performance**:
   - Block cache hit ratio
   - Split operation duration
   - Migration duration
   - Read/write latency

3. **Storage**:
   - S3 bucket size
   - Local disk usage
   - Reference marker count
   - Cost savings vs local-only

4. **Health**:
   - Leadership changes
   - S3 availability
   - Raft replica lag
   - Failed operations

## Garbage Collection Management

### How GC Works

Pebble automatically manages S3 object lifecycle:

1. Objects created with reference markers
2. Compaction removes old markers
3. GC scans for objects with zero markers
4. Unreferenced objects deleted

### Monitoring GC

#### Check Marker Accumulation

```bash
# List all markers
aws s3 ls s3://bucket/shard-123/markers/ --recursive | wc -l

# Should stabilize, not grow unbounded
# Monitor weekly trends
```

#### Check for Orphaned Objects

```bash
# List sstables
aws s3 ls s3://bucket/shard-123/ | grep ".sst$" > sstables.txt

# List markers
aws s3 ls s3://bucket/shard-123/markers/ --recursive > markers.txt

# Find sstables without markers (should be cleaned up)
# This is a sign GC is working or not
```

#### Monitor Storage Growth

```bash
# Daily check - should stabilize after initial growth
aws s3 ls s3://bucket/ --recursive --summarize | grep "Total Size"

# Alert if continuously growing (GC not working)
```

### S3 Lifecycle Policies

Set up S3 lifecycle rules as a safety net:

```json
{
  "Rules": [
    {
      "Id": "DeleteOldMarkers",
      "Status": "Enabled",
      "Prefix": "markers/",
      "Expiration": {
        "Days": 7
      }
    },
    {
      "Id": "TransitionOldData",
      "Status": "Enabled",
      "Prefix": "shard-",
      "Transitions": [
        {
          "Days": 30,
          "StorageClass": "STANDARD_IA"
        },
        {
          "Days": 90,
          "StorageClass": "GLACIER"
        }
      ]
    }
  ]
}
```

Apply policy:
```bash
aws s3api put-bucket-lifecycle-configuration \
  --bucket antfly-data \
  --lifecycle-configuration file://lifecycle.json
```

## Failure Scenarios

### S3 Unavailable

**Symptom**: S3 service outage or network partition

**Impact**:
- Recent writes still in WAL/memtable (OK)
- Reads for cold data fail (degraded)
- Writes continue (to WAL)
- Compaction may fail

**Mitigation**:
```
1. Monitor S3 availability
2. Alert on S3 error rate >5%
3. Have local cache layer for critical data
4. Automatic retry with backoff
```

**Recovery**: Service resumes when S3 becomes available.

### Node Failure

**Symptom**: Storage node crashes

**Impact**:
- Raft elects new leader (<1 second)
- New leader reads sstables from S3
- Faster recovery than copying from peers

**Recovery procedure**:
```
1. Raft detects failure (heartbeat timeout)
2. New leader elected
3. LeaderFactory starts on new leader
4. isLeader flag set to true
5. New leader writes to S3
6. Followers continue reading from S3
```

### Network Partition

**Symptom**: Node isolated from S3 but Raft cluster intact

**Impact**:
- Raft handles leader election
- S3 becomes unavailable to isolated nodes
- Degraded reads for cold data

**Mitigation**:
```
1. Raft majority still functional
2. Nodes with S3 access continue serving
3. Isolated nodes serve from cache
```

### Leadership Flapping

**Symptom**: Rapid leadership changes

**Impact**:
- Multiple nodes attempting S3 writes
- LeaderAwareS3Storage rejects non-leader writes
- Temporary write failures during transitions

**Mitigation**:
```
1. Investigate Raft stability
2. Check network latency
3. Tune Raft election timeouts
4. Monitor leadership_changes_total metric
```

**Recovery**: Leadership stabilizes, normal operation resumes.

## Troubleshooting

### Split is Slow (>30 seconds)

**Check**:
```bash
# 1. Is S3 storage enabled?
grep "s3:" config.yaml

# 2. Are sstables actually in S3?
aws s3 ls s3://bucket/shard-123/

# 3. Is local disk full?
df -h /data

# 4. WAL size too large?
du -sh /data/shard-*/WAL
```

**Solutions**:
- Verify S3 configuration enabled
- Flush WAL before split
- Free up local disk space
- Check S3 connectivity

### High S3 Costs

**Symptom**: Unexpected S3 GET request costs

**Diagnose**:
```bash
# 1. Check request rate
aws cloudwatch get-metric-statistics \
  --metric-name GetRequests \
  --namespace AWS/S3 ...

# 2. Check cache hit ratio
# (should be >90% in metrics)

# 3. Check block size configuration
# Smaller blocks = more requests
```

**Solutions**:
- Increase block cache size
- Monitor cache hit ratio
- Consider `CreateOnSharedLower`
- Optimize query patterns

### Slow Read Performance

**Symptom**: High p99 read latency

**Diagnose**:
```bash
# 1. Check cache hit ratio
# Target: >90%

# 2. Check S3 latency
# Target: p50 < 50ms

# 3. Check local vs S3 reads
# Most reads should be cached or local
```

**Solutions**:
```
1. Increase block cache size (2-4GB)
2. Use CreateOnSharedLower for hot data
3. Optimize S3 endpoint (regional)
4. Check network latency to S3
```

### Orphaned Reference Markers

**Symptom**: Old markers not deleted, S3 costs growing

**Diagnose**:
```bash
# Count markers over time
aws s3 ls s3://bucket/markers/ --recursive | wc -l

# Check if growing unbounded
# Should stabilize after initial period
```

**Solutions**:
```
1. Verify Pebble GC is running (automatic)
2. Check shards properly closed when deleted
3. Apply S3 lifecycle policy for old markers
4. Manual cleanup if needed (careful!)
```

### Follower Cannot Read from S3

**Symptom**: Read errors on follower nodes

**Diagnose**:
```bash
# 1. Check S3 permissions
aws s3 ls s3://bucket/shard-123/ --profile follower-node

# 2. Check network connectivity
ping s3.amazonaws.com

# 3. Check credentials
echo $AWS_ACCESS_KEY_ID
```

**Solutions**:
- Ensure all nodes have S3 read access
- Verify credentials configured
- Check network/firewall rules
- Test S3 connectivity

### Leader Cannot Write to S3

**Symptom**: Errors like "not raft leader, cannot write to S3"

**Diagnose**:
```bash
# 1. Check isLeader flag
# (in logs: "Starting leader factory" isLeader=true)

# 2. Check LeaderFactory is running
# (should see "leader factory started")

# 3. Check S3 write permissions
aws s3 cp test.txt s3://bucket/test.txt
```

**Solutions**:
- Verify LeaderFactory called by Raft
- Check S3 write permissions
- Review leadership logs
- Verify S3 storage configured

## Best Practices

### 1. Capacity Planning

**Local disk sizing**:
```
With CreateOnSharedAll:
  - WAL + MANIFEST + metadata
  - ~5-10% of total data per shard
  - Example: 1TB shard → 50-100GB local

With CreateOnSharedLower:
  - WAL + MANIFEST + L0-L4 sstables
  - ~25-30% of total data per shard
  - Example: 1TB shard → 250-300GB local
```

**S3 sizing**:
```
With CreateOnSharedAll:
  - ~95% of total data (all sstables)
  - Single copy (not replicated)
  - Example: 3-node cluster, 1TB per shard → 1TB in S3

With CreateOnSharedLower:
  - ~75% of total data (L5-L6 sstables)
  - Single copy
  - Example: Same cluster → 750GB in S3
```

### 2. Backup Strategy

**What to backup**:
- S3 sstables already backed up (11 nines durability)
- Enable S3 versioning for point-in-time recovery
- Backup WAL and MANIFEST files separately
- Consider cross-region S3 replication

**Backup procedure**:
```bash
# S3 versioning (automatic)
aws s3api put-bucket-versioning \
  --bucket antfly-data \
  --versioning-configuration Status=Enabled

# Cross-region replication
aws s3api put-bucket-replication \
  --bucket antfly-data \
  --replication-configuration file://replication.json
```

### 3. Cost Optimization

**Monitor and optimize**:
```
1. Block cache hit ratio (reduce S3 reads)
2. GET request rate (minimize unnecessary requests)
3. Storage class transitions (move old data to Glacier)
4. Lifecycle policies (auto-delete old markers)
```

**Cost breakdown** (example 1TB):
```
Storage: 1TB × $0.023/GB = $23/month
GET requests: 10M × $0.0004/1000 = $4/month
PUT requests: 100K × $0.005/1000 = $0.50/month
Total: ~$27.50/month

vs local: 3TB × $0.10/GB = $300/month
Savings: 91%
```

### 4. Security

**Best practices**:
```
1. Use IAM roles (not hardcoded credentials)
2. Enable S3 encryption at rest (SSE-S3 or SSE-KMS)
3. Enable S3 encryption in transit (use_ssl: true)
4. Restrict S3 bucket access (bucket policies)
5. Enable S3 access logging
6. Use VPC endpoints for S3 (avoid internet)
```

**Example IAM policy**:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::antfly-data/*",
        "arn:aws:s3:::antfly-data"
      ]
    }
  ]
}
```

## Summary

S3 storage dramatically improves operational efficiency:

- **Shard splits**: 100-1000x faster (seconds vs minutes)
- **Migrations**: 30-60x faster (minimal network transfer)
- **Cost savings**: 74-91% reduction in storage costs
- **Monitoring**: Key metrics for health and performance
- **Reliability**: Fast failover and automatic GC

Key operational considerations:
- Increase block cache size (2-4GB)
- Monitor cache hit ratio (>90% target)
- Set up S3 lifecycle policies
- Plan for S3 unavailability scenarios
- Regular monitoring of GC and storage growth
