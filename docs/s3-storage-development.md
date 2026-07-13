# Developing object storage

The supported implementation is the Zig serverless `object` engine. The former
Go/Pebble remote-SST prototype is retired and is not a compatibility target.

Start local S3-compatible development with:

```console
docker compose -f devops/docker-compose-s3/docker-compose.yml up --build
```

Object-storage changes should preserve these invariants:

- manifests and progress use compare-and-swap publication;
- immutable artifacts are content addressed;
- WAL positions are monotonic and retained until publication is safe;
- connection capabilities, bucket allowlists, and prefix boundaries validate
  before workers start;
- explicit malformed configuration and environment values fail closed;
- clients and HTTP pools are shared for lanes using the same connection, while
  separate credential profiles remain isolated;
- query caches and maintenance work have explicit memory/concurrency bounds;
- retry behavior is idempotent and does not turn authorization or validation
  failures into infinite retry loops.

Add focused unit tests for object codecs/stores and an integration test against
MinIO. Provider-specific tests should use short-lived credentials and isolated
prefixes. Measure request count, transferred bytes, cache hit rate, publish
latency, WAL lag, and compaction backlog rather than relying only on throughput.

See [S3 object storage](s3-storage.md) for configuration and
[object-storage operations](s3-storage-operations.md) for production guidance.
