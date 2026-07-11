# Secrets management

Antfly resolves secret references without putting credential values in the
main configuration. The Zig runtime accepts JSON configuration only.

```json
{
  "connections": {
    "openai-production": {
      "kind": "inference",
      "capabilities": ["models.generate", "models.embed"],
      "inference": {
        "provider": "openai",
        "api_key": "${secret:openai.api_key}"
      }
    }
  }
}
```

References have the exact form `${secret:key.name}`. Keys may contain ASCII
letters, numbers, `.`, `_`, and `-`.

## Sources and precedence

For a referenced key, Antfly checks:

1. each `--secret-store-path` file in command-line order;
2. the corresponding environment variable.

The environment name is the uppercased key with punctuation replaced by `_`.
For example, `openai.api_key` maps to `OPENAI_API_KEY`.

The local secret store is a JSON file protected by operating-system file
permissions. It is deliberately not described as an encrypted keystore: disk
encryption or a platform secret volume must provide encryption at rest. Do not
commit this file, bake it into a container image, or place it beside public
configuration.

Antfly keeps a last-known-good in-memory snapshot during an invalid or missing
file reload and exposes stale/reload health without returning values. Updates
are written atomically and consumers refresh generation-aware credentials.

## Recommended production setup

Mount a secret-store file from the platform secret manager as read-only to the
container and restrict it to the Antfly service account:

```json
{
  "secrets": [
    { "key": "openai.api_key", "value": "REDACTED" },
    { "key": "content.internal_api_authorization", "value": "REDACTED" }
  ]
}
```

```console
antfly standalone \
  --config /etc/antfly/config.json \
  --secret-store-path /run/secrets/antfly/secrets.json
```

The service account should own the file with mode `0600`; the containing
directory should not be writable by unrelated workloads. Rotation should
replace the mounted file atomically, never rewrite it in place. Antfly notices
the metadata change and reloads it while retaining the previous snapshot if
the replacement is malformed.

Multiple paths provide explicit fallback layers. Put the most specific and
most frequently rotated source first:

```console
antfly standalone \
  --config /etc/antfly/config.json \
  --secret-store-path /run/secrets/tenant/secrets.json \
  --secret-store-path /run/secrets/platform/secrets.json
```

Environment variables are convenient for local development and platform
workload identity. Avoid process arguments because they may be visible in
process listings.

## Managing standalone secrets through the API

When standalone is configured with a writable secret store, its authenticated
`/db/v1` management API can list metadata, put a value, and delete a value.
Values are accepted only in request bodies and are never returned.

```console
curl -X PUT https://antfly.example.com/db/v1/secrets/openai.api_key \
  -H 'Authorization: Bearer TOKEN' \
  -H 'Content-Type: application/json' \
  --data '{"value":"REDACTED"}'

curl https://antfly.example.com/db/v1/secrets \
  -H 'Authorization: Bearer TOKEN'

curl -X DELETE https://antfly.example.com/db/v1/secrets/openai.api_key \
  -H 'Authorization: Bearer TOKEN'
```

These endpoints are unavailable when there is no writable local secret store
and must be protected by normal authentication and admin authorization. Use a
platform secret manager rather than the API when the mounted file is
read-only.

## Storage credentials and remote-read credentials

Primary database storage and user-provided remote content are separate trust
domains:

- `connections.*` with capability `storage.primary` authorizes Antfly to read
  and write database artifacts. Serverless lanes may select different named
  connections and buckets.
- `remote_content.s3.*` authorizes read access to customer documents used by
  `remoteText`, `remotePDF`, and related helpers. Several named credentials can
  be selected by bucket.

Do not reuse a broad storage writer credential for remote content. Prefer
short-lived workload credentials. S3 primary storage without static keys uses
the refreshable AWS default credential chain, including environment, web
identity/IRSA, shared profiles, ECS, and EC2 metadata credentials.

Static S3-compatible credentials can use references:

```json
{
  "connections": {
    "primary-storage": {
      "kind": "external_io",
      "capabilities": ["storage.primary"],
      "external_io": {
        "protocol": "s3",
        "endpoint": "minio.internal:9000",
        "access_key_id": "${secret:storage.access_key_id}",
        "secret_access_key": "${secret:storage.secret_access_key}",
        "buckets": ["antfly-data"]
      }
    }
  }
}
```

See [`configs/config-secrets-example.json`](../configs/config-secrets-example.json)
for a complete JSON example.

## Failure behavior

Startup fails closed when a required reference cannot be resolved or a secret
store is malformed. An already-running process keeps its last-known-good
snapshot when a subsequent reload fails and reports the stale state. Literal
credentials remain accepted by the schema for development and migration, but
production deployments should use references or workload identity.
