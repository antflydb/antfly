# objectstore

`lib/objectstore` provides the storage transport layer used by the serverless
path in `antfly-zig`.

Currently supported backends:

- local filesystem
- S3-compatible object storage
- GCS JSON API

## Test Commands

Run the default unit suite:

```bash
cd lib/objectstore
zig build test
```

The default suite stays local and deterministic. Real backend integration tests
are opt-in via environment variables.

## S3 / MinIO / R2 Integration

Enable the real S3-compatible round-trip test with:

```bash
export OBJECTSTORE_S3_INTEGRATION=1
export OBJECTSTORE_S3_TEST_BUCKET=my-test-bucket
export AWS_ENDPOINT_URL=http://127.0.0.1:9000
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1

cd lib/objectstore
zig build test
```

This path is suitable for:

- MinIO
- AWS S3
- Cloudflare R2 through the S3-compatible layer
- other S3-compatible endpoints

Optional variables:

- `AWS_SESSION_TOKEN`

## GCS Integration

Enable the real GCS JSON API round-trip test with:

```bash
export OBJECTSTORE_GCS_INTEGRATION=1
export OBJECTSTORE_GCS_TEST_BUCKET=my-test-bucket
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

cd lib/objectstore
zig build test
```

GCS auth resolution order:

1. `GCS_BEARER_TOKEN`
2. `GOOGLE_OAUTH_ACCESS_TOKEN`
3. `GOOGLE_SERVICE_ACCOUNT_JSON`
4. `GOOGLE_APPLICATION_CREDENTIALS`

Optional GCS variables:

- `GOOGLE_CLOUD_PROJECT`
- `GCLOUD_PROJECT`
- `GCS_OAUTH_SCOPE`
- `GCS_JSON_API_ENDPOINT`
- `GCS_JSON_API_UPLOAD_ENDPOINT`

The service-account path is implemented in
`lib/objectstore/src/google_auth.zig`. It is intentionally narrow and specific
to GCS objectstore auth, not a generic repo-wide OAuth library.

## Serverless Runtime

The serverless runtime consumes these backends through URI configuration:

- `file://...`
- `s3://bucket/prefix`
- `gs://bucket/prefix`

Cloudflare R2 uses the same `s3://bucket/prefix` runtime path and the same
AWS-compatible environment variables as other S3-compatible backends.

Relevant runtime variables:

- `ANTFLY_SERVERLESS_ARTIFACTS_URI`
- `ANTFLY_SERVERLESS_MANIFESTS_URI`
- `ANTFLY_SERVERLESS_WAL_URI`
- `ANTFLY_SERVERLESS_PROGRESS_URI`
- `ANTFLY_SERVERLESS_CATALOG_URI`

The runtime-level backend matrix is also documented in
`SERVERLESS.md`.
