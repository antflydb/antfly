# S3 Storage Development Guide

## Overview

This guide is for developers working on Antfly's S3 storage implementation. It covers code architecture, implementation details, testing strategies, and future enhancements.

## Table of Contents

- [Code Architecture](#code-architecture)
- [Implementation Details](#implementation-details)
- [Testing Strategy](#testing-strategy)
- [Future Enhancements](#future-enhancements)

## Code Architecture

### Component Overview

```
src/common/config.go
  ├─ S3Info struct (configuration)
  └─ Validation logic

src/store/s3storage/
  ├─ s3storage.go (base S3 storage backend)
  └─ leader_aware.go (leadership-aware wrapper)

src/store/db.go
  ├─ DBImpl struct (isLeader, s3Storage fields)
  ├─ Open() (Pebble initialization with S3)
  └─ LeaderFactory() (leadership state management)

src/raft/raft.go
  └─ Leadership change detection → LeaderFactory
```

### Key Interfaces

#### 1. Pebble's remote.Storage Interface

```go
// github.com/cockroachdb/pebble/objstorage/remote
type Storage interface {
    CreateObject(objectName string) (io.WriteCloser, error)
    ReadObject(ctx context.Context, objectName string) (ObjectReader, error)
    Size(objectName string) (int64, error)
    IsNotExistError(err error) bool
    Close() error
    Delete(objectName string) error
}
```

Our implementation: `src/store/s3storage/s3storage.go`

#### 2. LeaderAwareS3Storage Wrapper

```go
// src/store/s3storage/leader_aware.go
type LeaderAwareS3Storage struct {
    underlying *S3Storage
    isLeader   *atomic.Bool
}

// CreateObject only works for leader
func (s *LeaderAwareS3Storage) CreateObject(objectName string) (io.WriteCloser, error) {
    if !s.isLeader.Load() {
        return nil, fmt.Errorf("not raft leader, cannot write to S3: %s", objectName)
    }
    return s.underlying.CreateObject(objectName)
}

// ReadObject works for all replicas
func (s *LeaderAwareS3Storage) ReadObject(ctx context.Context, objectName string) (remote.ObjectReader, error) {
    return s.underlying.ReadObject(ctx, objectName)
}

// Delete only works for leader (for GC)
func (s *LeaderAwareS3Storage) Delete(objectName string) error {
    if !s.isLeader.Load() {
        return fmt.Errorf("not raft leader, cannot delete from S3: %s", objectName)
    }
    return s.underlying.Delete(objectName)
}
```

### Configuration Structure

```go
// src/common/config.go
type S3Info struct {
    Enabled          bool   `yaml:"enabled"`
    Endpoint         string `yaml:"endpoint"`         // Required if enabled
    Region           string `yaml:"region"`           // Optional for MinIO
    Bucket           string `yaml:"bucket"`           // Required if enabled
    Prefix           string `yaml:"prefix"`           // Optional path prefix
    UseSSL           bool   `yaml:"use_ssl"`          // Default: true
    AccessKeyID      string `yaml:"access_key_id"`    // Optional (env var preferred)
    SecretAccessKey  string `yaml:"secret_access_key"` // Optional (env var preferred)
}

// Validation
func (s *S3Info) Validate() error {
    if !s.Enabled {
        return nil
    }
    if s.Endpoint == "" {
        return fmt.Errorf("s3.endpoint is required when S3 is enabled")
    }
    if s.Bucket == "" {
        return fmt.Errorf("s3.bucket is required when S3 is enabled")
    }
    // Bucket name validation (3-63 chars, lowercase, no underscores)
    if len(s.Bucket) < 3 || len(s.Bucket) > 63 {
        return fmt.Errorf("s3.bucket must be 3-63 characters")
    }
    // Check credentials (config or environment)
    if s.AccessKeyID == "" && os.Getenv("AWS_ACCESS_KEY_ID") == "" {
        return fmt.Errorf("s3 credentials required (config or AWS_ACCESS_KEY_ID)")
    }
    return nil
}
```

## Implementation Details

### DBImpl Integration

#### 1. Struct Fields

```go
// src/store/db.go
type DBImpl struct {
    // ... existing fields ...

    isLeader   atomic.Bool                         // Leadership state
    s3Storage  *s3storage.LeaderAwareS3Storage    // S3 backend (nil if disabled)
}
```

#### 2. Initialization in Open()

```go
// src/store/db.go
func (db *DBImpl) Open() error {
    pebbleOpts := &pebble.Options{
        Logger: &logger.NoopLoggerAndTracer{},
        Cache:  pebble.NewCache(5 * 64 << 20), // 320MB default
    }

    // Configure S3 if enabled
    if db.antflyConfig.S3Info != nil && db.antflyConfig.S3Info.Enabled {
        // Create MinIO client
        minioClient, err := common.NewMinioClient(
            db.antflyConfig.S3Info.Endpoint,
            db.antflyConfig.S3Info.Region,
            db.antflyConfig.S3Info.UseSSL,
            db.antflyConfig.S3Info.AccessKeyID,
            db.antflyConfig.S3Info.SecretAccessKey,
        )
        if err != nil {
            return fmt.Errorf("failed to create MinIO client: %w", err)
        }

        // Create base S3 storage
        baseS3, err := s3storage.NewS3Storage(
            minioClient,
            db.antflyConfig.S3Info.Bucket,
            db.antflyConfig.S3Info.Prefix,
        )
        if err != nil {
            return fmt.Errorf("failed to create S3 storage: %w", err)
        }

        // Wrap with leadership awareness
        db.s3Storage = s3storage.NewLeaderAwareS3Storage(
            baseS3,
            &db.isLeader,  // Share atomic bool
        )

        // Configure Pebble with S3 backend
        pebbleOpts.Experimental.RemoteStorage = remote.MakeSimpleFactory(
            map[remote.Locator]remote.Storage{"s3": db.s3Storage},
        )
        pebbleOpts.Experimental.CreateOnShared = remote.CreateOnSharedAll
        pebbleOpts.Experimental.CreateOnSharedLocator = "s3"

        // Set CreatorID to shard ID (not replica ID!)
        creatorID := base.MakeCreatorID(uint64(db.shardID))

        db.logger.Info("S3 storage configured",
            zap.String("bucket", db.antflyConfig.S3Info.Bucket),
            zap.String("prefix", db.antflyConfig.S3Info.Prefix),
            zap.Uint64("creatorID", uint64(db.shardID)),
        )
    }

    db.pdb, err = pebble.Open(pebbleDir, pebbleOpts)
    if err != nil {
        return fmt.Errorf("failed to open Pebble: %w", err)
    }

    return nil
}
```

#### 3. LeaderFactory Updates

```go
// src/store/db.go
func (db *DBImpl) LeaderFactory(
    ctx context.Context,
    persistFunc PersistFunc,
) error {
    // Set leadership flag when we become leader
    db.isLeader.Store(true)
    defer db.isLeader.Store(false)  // Clear when we lose leadership

    db.isLeaderMu.Lock()
    db.restartIndexManagerFactory = make(chan struct{})
    db.isLeaderMu.Unlock()

    db.logger.Info("Starting leader factory",
        zap.Bool("hasPersistFuncSet", persistFunc != nil),
        zap.Bool("s3Enabled", db.s3Storage != nil),
        zap.Uint64("shardID", uint64(db.shardID)),
    )

    // Start index enrichers (existing code)
    for {
        if err := db.indexManager.StartLeaderFactory(ctx, persistFunc); err != nil &&
            !errors.Is(err, context.Canceled) {
            db.logger.Error("Failed to start index manager leader factory",
                zap.Error(err))
        }
        select {
        case <-ctx.Done():
            db.logger.Info("Leader factory context cancelled")
            return ctx.Err()
        case <-db.restartIndexManagerFactory:
            db.logger.Info("Restarting index manager leader factory")
            if err := db.indexManager.CloseLeaderFactory(); err != nil {
                db.logger.Error("Failed to close index manager", zap.Error(err))
            }
        }
    }
}
```

### S3Storage Implementation

```go
// src/store/s3storage/s3storage.go
type S3Storage struct {
    client *minio.Client
    bucket string
    prefix string
}

func NewS3Storage(client *minio.Client, bucket, prefix string) (*S3Storage, error) {
    // Ensure bucket exists
    exists, err := client.BucketExists(context.Background(), bucket)
    if err != nil {
        return nil, fmt.Errorf("failed to check bucket: %w", err)
    }
    if !exists {
        return nil, fmt.Errorf("bucket does not exist: %s", bucket)
    }

    return &S3Storage{
        client: client,
        bucket: bucket,
        prefix: prefix,
    }, nil
}

func (s *S3Storage) CreateObject(objectName string) (io.WriteCloser, error) {
    fullPath := s.objectPath(objectName)

    // Use PutObject with pipe for streaming
    pr, pw := io.Pipe()

    go func() {
        _, err := s.client.PutObject(
            context.Background(),
            s.bucket,
            fullPath,
            pr,
            -1, // Unknown size (streaming)
            minio.PutObjectOptions{},
        )
        if err != nil {
            pr.CloseWithError(err)
        } else {
            pr.Close()
        }
    }()

    return pw, nil
}

func (s *S3Storage) ReadObject(ctx context.Context, objectName string) (remote.ObjectReader, error) {
    fullPath := s.objectPath(objectName)

    obj, err := s.client.GetObject(
        ctx,
        s.bucket,
        fullPath,
        minio.GetObjectOptions{},
    )
    if err != nil {
        return nil, err
    }

    // Get object info for size
    info, err := obj.Stat()
    if err != nil {
        return nil, err
    }

    return &s3ObjectReader{
        obj:  obj,
        size: info.Size,
    }, nil
}

func (s *S3Storage) Size(objectName string) (int64, error) {
    fullPath := s.objectPath(objectName)

    info, err := s.client.StatObject(
        context.Background(),
        s.bucket,
        fullPath,
        minio.StatObjectOptions{},
    )
    if err != nil {
        return 0, err
    }

    return info.Size, nil
}

func (s *S3Storage) Delete(objectName string) error {
    fullPath := s.objectPath(objectName)

    return s.client.RemoveObject(
        context.Background(),
        s.bucket,
        fullPath,
        minio.RemoveObjectOptions{},
    )
}

func (s *S3Storage) IsNotExistError(err error) bool {
    if err == nil {
        return false
    }
    // Check MinIO error codes
    errResponse := minio.ToErrorResponse(err)
    return errResponse.Code == "NoSuchKey"
}

func (s *S3Storage) Close() error {
    // MinIO client doesn't require explicit close
    return nil
}

func (s *S3Storage) objectPath(name string) string {
    if s.prefix == "" {
        return name
    }
    return s.prefix + "/" + name
}
```

### ObjectReader Implementation

```go
// src/store/s3storage/s3storage.go
type s3ObjectReader struct {
    obj  *minio.Object
    size int64
}

func (r *s3ObjectReader) ReadAt(p []byte, off int64) (int, error) {
    // Use HTTP range request for efficiency
    _, err := r.obj.Seek(off, io.SeekStart)
    if err != nil {
        return 0, err
    }
    return r.obj.Read(p)
}

func (r *s3ObjectReader) Close() error {
    return r.obj.Close()
}

func (r *s3ObjectReader) Size() int64 {
    return r.size
}

func (r *s3ObjectReader) NewReadHandle(ctx context.Context) remote.ReadHandle {
    // Return self (we don't pool handles)
    return r
}
```

## Testing Strategy

### Unit Tests

#### 1. S3Storage Tests

```go
// src/store/s3storage/s3storage_test.go
func TestS3Storage_CreateAndRead(t *testing.T) {
    // Setup MinIO client (local or test container)
    client := setupMinioClient(t)
    storage, err := NewS3Storage(client, "test-bucket", "test-prefix")
    require.NoError(t, err)

    // Create object
    objectName := "test.sst"
    writer, err := storage.CreateObject(objectName)
    require.NoError(t, err)

    data := []byte("test data")
    _, err = writer.Write(data)
    require.NoError(t, err)
    err = writer.Close()
    require.NoError(t, err)

    // Read object
    reader, err := storage.ReadObject(context.Background(), objectName)
    require.NoError(t, err)
    defer reader.Close()

    readData := make([]byte, len(data))
    n, err := reader.ReadAt(readData, 0)
    require.NoError(t, err)
    require.Equal(t, len(data), n)
    require.Equal(t, data, readData)

    // Check size
    size, err := storage.Size(objectName)
    require.NoError(t, err)
    require.Equal(t, int64(len(data)), size)

    // Delete
    err = storage.Delete(objectName)
    require.NoError(t, err)
}
```

#### 2. LeaderAwareS3Storage Tests

```go
// src/store/s3storage/leader_aware_test.go
func TestLeaderAwareS3Storage_OnlyLeaderWrites(t *testing.T) {
    isLeader := &atomic.Bool{}
    baseS3 := setupBaseS3(t)
    storage := NewLeaderAwareS3Storage(baseS3, isLeader)

    // Follower: write should fail
    isLeader.Store(false)
    _, err := storage.CreateObject("test.sst")
    require.Error(t, err)
    require.Contains(t, err.Error(), "not raft leader")

    // Leader: write should succeed
    isLeader.Store(true)
    w, err := storage.CreateObject("test.sst")
    require.NoError(t, err)
    require.NotNil(t, w)
    w.Write([]byte("data"))
    w.Close()

    // All replicas can read
    isLeader.Store(false)
    r, err := storage.ReadObject(context.Background(), "test.sst")
    require.NoError(t, err)
    require.NotNil(t, r)
    r.Close()

    // Follower: delete should fail
    err = storage.Delete("test.sst")
    require.Error(t, err)
    require.Contains(t, err.Error(), "not raft leader")

    // Leader: delete should succeed
    isLeader.Store(true)
    err = storage.Delete("test.sst")
    require.NoError(t, err)
}
```

### Integration Tests

#### 1. Three-Node Raft Cluster Test

```go
// src/store/db_test.go
func TestDBImpl_S3Storage_LeaderOnlyWrites(t *testing.T) {
    // Start 3-node Raft cluster with S3 enabled
    nodes := startTestCluster(t, 3, withS3Config(testS3Config))
    defer stopTestCluster(nodes)

    // Wait for leader election
    leader := waitForLeader(t, nodes)
    followers := getFollowers(nodes, leader)

    // Insert data (goes to leader)
    key := []byte("test-key")
    value := []byte("test-value")
    err := leader.Put(key, value)
    require.NoError(t, err)

    // Wait for compaction
    time.Sleep(5 * time.Second)
    triggerCompaction(t, leader)

    // Check S3: should have sstables from leader only
    s3Objects := listS3Objects(t, testBucket, "shard-123/")
    require.Greater(t, len(s3Objects), 0)

    // Verify followers didn't write to S3
    // (check logs for "not raft leader" messages)
    verifyFollowerRejects(t, followers)

    // Kill leader, wait for new leader
    stopNode(leader)
    newLeader := waitForLeader(t, followers)

    // New leader should write to S3
    err = newLeader.Put([]byte("key2"), []byte("value2"))
    require.NoError(t, err)
    time.Sleep(5 * time.Second)
    triggerCompaction(t, newLeader)

    // Verify new sstables in S3
    newObjects := listS3Objects(t, testBucket, "shard-123/")
    require.Greater(t, len(newObjects), len(s3Objects))
}
```

#### 2. Shard Split Test

```go
// src/store/db_test.go
func TestDBImpl_S3Storage_FastSplits(t *testing.T) {
    db := setupDBWithS3(t)
    defer db.Close()

    // Insert 10GB of data
    insertTestData(t, db, 10*1024*1024*1024)

    // Trigger compaction to S3
    triggerCompaction(t, db)

    // Measure split time
    start := time.Now()
    err := db.Split(
        common.Range{[]byte("a"), []byte("z")},
        []byte("m"),
        "/tmp/shard-1",
        "/tmp/shard-2",
    )
    duration := time.Since(start)

    require.NoError(t, err)
    require.Less(t, duration, 30*time.Second, "Split should be <30 seconds")

    // Verify checkpoint sizes
    size1 := getDirSize("/tmp/shard-1")
    size2 := getDirSize("/tmp/shard-2")
    require.Less(t, size1, 200*1024*1024, "Checkpoint <200MB")
    require.Less(t, size2, 200*1024*1024, "Checkpoint <200MB")

    // Verify both shards can read
    shard1 := openDB(t, "/tmp/shard-1")
    shard2 := openDB(t, "/tmp/shard-2")
    defer shard1.Close()
    defer shard2.Close()

    // Both should access S3 objects
    verifyCanRead(t, shard1, []byte("a"))
    verifyCanRead(t, shard2, []byte("z"))
}
```

### Performance Benchmarks

```go
// src/store/s3storage/s3storage_bench_test.go
func BenchmarkS3Storage_Write(b *testing.B) {
    storage := setupS3Storage(b)
    data := make([]byte, 1024*1024) // 1MB

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        w, _ := storage.CreateObject(fmt.Sprintf("bench-%d.sst", i))
        w.Write(data)
        w.Close()
    }
}

func BenchmarkS3Storage_Read(b *testing.B) {
    storage := setupS3Storage(b)
    // Pre-populate object
    w, _ := storage.CreateObject("bench.sst")
    data := make([]byte, 1024*1024)
    w.Write(data)
    w.Close()

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        r, _ := storage.ReadObject(context.Background(), "bench.sst")
        buf := make([]byte, 1024*1024)
        r.ReadAt(buf, 0)
        r.Close()
    }
}
```

### Test Setup Helpers

```go
// src/store/s3storage/testing.go
func setupMinioClient(t *testing.T) *minio.Client {
    // Use testcontainers or local MinIO
    endpoint := os.Getenv("MINIO_ENDPOINT")
    if endpoint == "" {
        endpoint = "localhost:9000"
    }

    client, err := minio.New(endpoint, &minio.Options{
        Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
        Secure: false,
    })
    require.NoError(t, err)

    // Create test bucket
    bucketName := "test-bucket-" + randomString(8)
    err = client.MakeBucket(context.Background(), bucketName, minio.MakeBucketOptions{})
    require.NoError(t, err)

    t.Cleanup(func() {
        // Cleanup bucket
        objects := client.ListObjects(context.Background(), bucketName, minio.ListObjectsOptions{
            Recursive: true,
        })
        for obj := range objects {
            client.RemoveObject(context.Background(), bucketName, obj.Key, minio.RemoveObjectOptions{})
        }
        client.RemoveBucket(context.Background(), bucketName)
    })

    return client
}
```

## Future Enhancements

### 1. Tiering Policies

Automatically move sstables to S3 based on age/access frequency:

```go
// Future: Automatic tiering
type TieringPolicy struct {
    HotDataThreshold  time.Duration // Keep recent data local
    ColdDataThreshold time.Duration // Move old data to S3
}

// Levels 0-2: Always local (hot data)
// Levels 3-4: Configurable based on age
// Levels 5-6: Always S3 (cold data)
```

### 2. Local Cache Layer

SSD cache for S3 sstables to improve read latency:

```go
// Future: Local cache for S3 objects
type CachedS3Storage struct {
    underlying *S3Storage
    cache      *LocalCache
    cacheSize  int64
}

// LRU eviction policy
// Transparent to Pebble
```

### 3. Compression

Compress sstables before uploading to S3:

```go
// Future: Transparent compression
pebbleOpts.Experimental.RemoteStorageCompression = "zstd"

// Reduces:
// - S3 storage costs (smaller objects)
// - Network transfer costs
// - GET request latency (less data)
```

### 4. Multipart Uploads

For large sstables (>5MB):

```go
// Future: Multipart uploads for large objects
if size > 5*1024*1024 {
    // Use S3 multipart upload API
    // Better parallelism and resume capability
}
```

### 5. Range Request Optimization

Optimize ReadAt() with HTTP range headers:

```go
// Future: Optimize ReadAt with range requests
func (r *s3ObjectReader) ReadAt(p []byte, off int64) (int, error) {
    opts := minio.GetObjectOptions{}
    opts.SetRange(off, off+int64(len(p)))

    // Fetch only requested range
    obj, err := r.client.GetObject(ctx, bucket, key, opts)
    // ...
}
```

### 6. S3 Select Integration

Push down query filters to S3:

```go
// Future: Use S3 Select for filtered reads
// SELECT * FROM s3object WHERE key > 'start' AND key < 'end'
// Reduces data transfer and costs
```

### 7. Metrics and Instrumentation

Comprehensive metrics for monitoring:

```go
// Future: Prometheus metrics
var (
    s3WriteLatency = prometheus.NewHistogram(...)
    s3ReadLatency  = prometheus.NewHistogram(...)
    s3ErrorRate    = prometheus.NewCounter(...)
    s3BytesWritten = prometheus.NewCounter(...)
    s3BytesRead    = prometheus.NewCounter(...)
)
```

### 8. Cross-Region Replication

For disaster recovery:

```yaml
# Future: Multi-region support
s3:
  enabled: true
  bucket: "antfly-data"
  regions:
    - name: "us-east-1"
      primary: true
    - name: "us-west-2"
      replica: true
```

## Development Checklist

For implementing new features:

- [ ] Update configuration structs in `src/common/config.go`
- [ ] Modify S3Storage interface implementation
- [ ] Update LeaderAwareS3Storage wrapper if needed
- [ ] Add logging for observability
- [ ] Write unit tests (coverage >80%)
- [ ] Write integration tests
- [ ] Add performance benchmarks
- [ ] Update documentation
- [ ] Test with MinIO locally
- [ ] Test with AWS S3 in staging
- [ ] Monitor metrics in production
- [ ] Update troubleshooting guide

## References

- [Pebble objstorage Package](https://pkg.go.dev/github.com/cockroachdb/pebble/objstorage)
- [Pebble Remote Storage Design](https://github.com/cockroachdb/pebble/blob/master/docs/remote_storage.md)
- [MinIO Go Client](https://github.com/minio/minio-go)
- [AWS S3 API Reference](https://docs.aws.amazon.com/s3/index.html)
- [Testcontainers for Go](https://golang.testcontainers.org/)
