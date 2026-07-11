# Serverless object storage with MinIO

This development stack runs the Zig serverless combined role against one
S3-compatible MinIO bucket. It exercises Antfly's current object engine; it is
not the removed Go/Pebble remote-SST design.

```console
docker compose -f devops/docker-compose-s3/docker-compose.yml up --build
```

Endpoints:

- Antfly public API: `http://localhost:8080/db/v1`
- Antfly readiness: `http://localhost:4200/readyz`
- MinIO console: `http://localhost:9001`

The example credentials are intentionally local-only. The MinIO bucket is not
public. Production deployments should provision buckets outside Antfly, use
TLS, select `bucket_provisioning: require_existing`, and use workload identity
or `${secret:...}` references rather than static environment credentials.

[`config.json`](config.json) demonstrates the named `storage.primary`
connection and the `storage.engine: object` tagged union. Add lane overrides
under `storage.object.lanes` when WAL, manifests, artifacts, progress, or the
catalog need different buckets, prefixes, accounts, or credentials.

Stop and remove the local data with:

```console
docker compose -f devops/docker-compose-s3/docker-compose.yml down --volumes
```
