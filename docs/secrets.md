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

## Distributed internal-service authentication

Distributed metadata and data processes require a dedicated credential for
node-to-node `/internal/v1` RPC. This credential is a separate trust domain
from `antfly.trusted_principal.*`, which may be held by an ingress gateway or
managed control plane and must never grant raw storage authority.

Provision the same values on every metadata and data node before starting a
distributed cluster:

```json
{
  "secrets": [
    {
      "key": "antfly.internal_service.secret",
      "value": "REPLACE-WITH-AT-LEAST-32-RANDOM-BYTES"
    },
    {
      "key": "antfly.internal_service.issuer",
      "value": "production-cluster-a"
    }
  ]
}
```

Without a secret-store file, the equivalent environment variables are
`ANTFLY_INTERNAL_SERVICE_SECRET` and `ANTFLY_INTERNAL_SERVICE_ISSUER`. Startup
fails before opening the listener when either value is missing or invalid.
Generate the secret with a cryptographically secure random generator; do not
reuse an API key, admin token, trusted-principal key, or another cluster's
credential. Distributed startup also rejects an internal-service secret that
is byte-for-byte equal to the configured trusted-principal secret.

For a rolling upgrade from a version that does not sign internal requests, use
an explicit two-phase rollout:

1. Pre-provision the dedicated secret and issuer on every node. Set
   `antfly.internal_service.rollout_mode` to `migration` (or set
   `ANTFLY_INTERNAL_SERVICE_ROLLOUT_MODE=migration`) and roll out the new
   binary. New nodes sign all outbound RPC while temporarily accepting old
   unsigned peers. **Withdraw every externally addressable Service or ingress
   that targets this listener before entering migration mode.** Responses to
   such legacy requests carry
   `X-Antfly-Internal-Auth: legacy-migration` for traffic verification.
2. After every peer is upgraded and legacy-marked traffic has stopped, set the
   mode to `enforce` (the default) and perform a second rolling restart. Nodes
   already in migration mode accept signed requests from enforcing peers, so
   this phase remains available throughout the rollout.

Migration mode deliberately weakens the internal listener boundary and emits
a prominent startup warning. Keep the listener private during that phase and
do not leave migration mode enabled after the upgrade. The Kubernetes operator
temporarily drains the optional `*-public-api` Service before migration while
preserving its ClusterIP and load-balancer address. Endpoints return only after
every metadata and data
StatefulSet has completed its `enforce` rollout. This makes the compatibility
phase an observable maintenance window instead of exposing unsigned
`/internal/v1` routes.

### Kubernetes operator deployments

The operator does not need permission to read or manage Secrets. Provision a
per-cluster Secret through your deployment controller or secret manager, then
reference only its name and key from the `AntflyCluster`:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: antflydb-internal-service-auth
type: Opaque
stringData:
  secret: REPLACE-WITH-AT-LEAST-32-RANDOM-BYTES
---
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: antflydb
spec:
  mode: Distributed
  internalServiceAuth:
    secretKeyRef:
      name: antflydb-internal-service-auth
      key: secret
      optional: false
  # ...
```

Kubelet injects the selected value directly into metadata and data containers;
the operator never fetches it. The issuer is derived from the immutable cluster
UID. New clusters start in enforcement mode. For an existing cluster, the
operator first rolls every StatefulSet in migration mode, waits for all replicas
to be updated and ready, and requires every metadata process to advertise the
new authentication capability. It then performs a second rollout in enforcement
mode. A runtime that ignores the new environment can therefore never make the
operator advance merely by reporting Ready.

Add `spec.internalServiceAuth` before changing an existing distributed
cluster's runtime image. If the reference is absent, admission and controller
fallback validation stop reconciliation before a fail-closed runtime can be
rolled. If the referenced Secret or key is absent, Kubernetes keeps the new pod
Pending and the rollout remains in migration; Secret values are never made
optional to preserve availability.

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
  `remoteText`, the deprecated `remotePDF`, and related helpers. Several named credentials can
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
