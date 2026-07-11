# S3 object storage

Antfly's Zig serverless runtime uses object storage as the durable engine for
artifacts, manifests, WAL, progress, and catalog state. Object storage is an
independent storage engine, not a local/distributed per-table override.

```text
deployment mode: serverless
storage engine:  object
protocol:        S3-compatible or Google Cloud Storage
```

The directory-backed `local` engine and single-file `lite` engine do not accept
S3 fields. Changing engines is an explicit backup/restore migration.

## Configuration

The runtime consumes JSON. A production S3 configuration uses a named
capability-scoped connection:

```json
{
  "deployment_mode": "serverless",
  "connections": {
    "production-storage": {
      "kind": "external_io",
      "capabilities": ["storage.primary"],
      "external_io": {
        "protocol": "s3",
        "region": "us-west-2",
        "buckets": ["antfly-data", "antfly-wal"],
        "prefix": "production",
        "bucket_provisioning": "require_existing"
      }
    }
  },
  "storage": {
    "engine": "object",
    "object": {
      "connection": "production-storage",
      "bucket": "antfly-data",
      "prefix": "production/cluster-1",
      "lanes": {
        "wal": {
          "bucket": "antfly-wal",
          "prefix": "production/cluster-1/wal"
        }
      }
    }
  }
}
```

The root location supplies defaults. Each lane can override `connection`,
`bucket`, or `prefix`:

- `artifacts`
- `manifests`
- `wal`
- `progress`
- `catalog`

Locations using the same connection share one object-store client and HTTP
connection pool. A different connection creates an isolated credential and
transport boundary. This supports multiple buckets, accounts, regions, and
durability classes without duplicating clients unnecessarily.

Connection bucket allowlists and prefix boundaries are validated at startup.
Missing named connections, incompatible capabilities, malformed explicit
environment overrides, and disallowed buckets fail closed.

## Credentials

On AWS, omit static keys to use the refreshable default credential chain:
environment credentials, web identity/IRSA, shared profiles, ECS task
credentials, and EC2 instance metadata. Temporary credentials refresh before
their safety window.

For an S3-compatible service that requires static credentials, reference a
protected secret-store file:

```json
{
  "external_io": {
    "protocol": "s3",
    "endpoint": "minio.internal:9000",
    "use_ssl": true,
    "access_key_id": "${secret:storage.access_key_id}",
    "secret_access_key": "${secret:storage.secret_access_key}",
    "buckets": ["antfly-data"]
  }
}
```

Primary-storage credentials grant database write authority. Keep them separate
from `remote_content.s3`, which grants read access to customer-provided objects
for template helpers. Several named remote-content credentials may select
different buckets without gaining access to Antfly's durable state.

## Bucket provisioning

Use `require_existing` in production. Infrastructure code should create the
bucket, encryption policy, versioning/lifecycle policy, access logging, and
least-privilege IAM policy before Antfly starts. `create_if_missing` is intended
for local development and controlled test environments.

At minimum, a storage connection needs object read/write/list/delete access
within its configured bucket and prefix. Narrow separate lane credentials when
organizational or retention boundaries warrant it.

## Running

```console
antfly serverless combined --config /etc/antfly/config.json \
  --secret-store-path /run/secrets/antfly/secrets.json
```

Low-level `--artifacts-uri`, `--manifests-uri`, `--wal-uri`,
`--progress-uri`, and `--catalog-uri` flags remain explicit location overrides
for debugging and specialized orchestration. Prefer the tagged configuration
for normal deployments because it validates connection capabilities and
bucket boundaries as one contract.

See [`configs/config-s3-example.json`](../configs/config-s3-example.json) and
the [MinIO Compose example](../devops/docker-compose-s3/README.md).
