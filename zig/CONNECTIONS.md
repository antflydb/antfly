# Connections

This document defines the long-run `/connections` interface and the first
implementation target for Antfly Zig.

Connections are external systems Antfly can use for inference, storage, content
fetching, replication, agents, backups, and related workflows. They should
become first-class resources with inventory, health, capability, purpose, and
authorization metadata.

## Goals

- Give operators one inventory of configured external systems.
- Keep provider-specific details out of top-level connection kinds.
- Avoid leaking secrets or raw DSNs through inventory APIs.
- Let RBAC authorize use of a connection for a specific workflow.
- Make UI affordances explicit: a user should know which connections they can
  see, use, and administer.
- Preserve config-derived connections during the transition to persisted
  first-class connection resources.

## Naming

Top-level connection kinds are broad physical categories:

- `inference`
- `object_store`
- `remote_content`
- `cdc`

Provider or protocol belongs inside the kind-specific payload:

```json
{
  "kind": "inference",
  "inference": {
    "provider": "openai"
  }
}
```

```json
{
  "kind": "remote_content",
  "remote_content": {
    "provider": "http"
  }
}
```

```json
{
  "kind": "cdc",
  "cdc": {
    "provider": "postgres"
  }
}
```

Use `object_store`, not `objectstore`, in public API fields. The common product
term is "object store", and the underscore form reads correctly next to other
snake-case API values.

`remote_content` is intentionally separate from `object_store`. An S3 bucket can
be an object store used for remote content, while HTTP is a content-fetch
provider and not an object store.

## Core Model

Long-run connections should have a stable ID and a display name. Config-derived
connections may initially synthesize IDs from their source path.

```json
{
  "id": "conn_openai_prod",
  "name": "openai-prod",
  "display_name": "OpenAI production",
  "kind": "inference",
  "status": "connected",
  "sources": ["config:generators/primary"],
  "capabilities": ["generate", "embed"],
  "purposes": ["agents", "indexing"],
  "inference": {
    "provider": "openai",
    "url": "https://api.openai.com",
    "models": {
      "generators": [{ "name": "gpt-4o", "configured": true }],
      "embedders": [{ "name": "text-embedding-3-small" }]
    }
  },
  "permissions": {
    "can_read": true,
    "can_use": true,
    "can_admin": false,
    "can_view_secret_refs": false
  }
}
```

The distinction between `capabilities`, `purposes`, and policy matters:

- `capabilities`: what the connection can technically do.
- `purposes`: which Antfly workflows the connection is intended to serve.
- `policy`: who may read, use, or administer it for each action.

## Kinds

### Inference

Inference connections describe model providers. A single provider instance can
serve multiple capabilities.

Example:

```json
{
  "id": "conn_openai_prod",
  "kind": "inference",
  "provider": "openai",
  "capabilities": ["generate", "embed"],
  "purposes": ["agents", "indexing"],
  "inference": {
    "provider": "openai",
    "url": "https://api.openai.com",
    "configured_model_types": ["generator", "embedder"]
  }
}
```

Common capabilities:

- `generate`
- `embed`
- `rerank`
- `chunk`
- `transcribe`
- `classify`
- `extract`

### Object Store

Object-store connections describe bucket/key storage systems.

Example:

```json
{
  "id": "conn_s3_backups",
  "kind": "object_store",
  "provider": "s3",
  "capabilities": ["read_objects", "write_objects"],
  "purposes": ["backup_restore"],
  "object_store": {
    "backend": "s3",
    "endpoint": "https://s3.us-east-1.amazonaws.com",
    "buckets": ["antfly-backups"],
    "prefix": "prod/"
  }
}
```

Object stores can be used for multiple purposes:

- `storage`
- `inference_models`
- `remote_content`
- `backup_restore`

### Remote Content

Remote-content connections describe URL or content-fetch providers. HTTP is the
first provider.

Example:

```json
{
  "id": "conn_docs_site",
  "kind": "remote_content",
  "provider": "http",
  "capabilities": ["fetch_content"],
  "purposes": ["indexing", "agents"],
  "remote_content": {
    "provider": "http",
    "hosts": ["https://docs.example.com"]
  }
}
```

Credentials and headers must not be returned by default. If the UI needs to
show that a secret reference exists, expose a redacted secret-ref summary behind
an explicit `connection.secret_ref:read` permission.

### CDC

CDC connections describe change-stream sources. Postgres is the first provider.

Example:

```json
{
  "id": "conn_pg_users_cdc",
  "kind": "cdc",
  "provider": "postgres",
  "status": "connected",
  "capabilities": ["read_stream"],
  "purposes": ["cdc"],
  "cdc": {
    "provider": "postgres",
    "table_name": "users",
    "source_ordinal": 0,
    "external_table": "public.users",
    "slot_name": "antfly_users_public_users",
    "publication_name": "antfly_pub_users_public_users",
    "phase": "streaming",
    "lag_records": 0,
    "lag_millis": 120,
    "last_success_at_ms": 1770500000000,
    "last_change_applied_at_ms": 1770500000000,
    "updated_at_ms": 1770500001000
  }
}
```

CDC inventory is derived from table replication-source config plus persisted
replication-source status. Raw DSNs and resolved credentials must never appear
in `/connections`.

## Status

Connection status should have narrow semantics:

- `configured`: the connection exists, but this response did not live-probe it.
- `connected`: a live probe, listing, or runtime status indicates success.
- `error`: a live probe, listing, or runtime status failed.
- `unsupported`: no probe is available for this kind/provider.

`GET /connections` should be cheap by default. Expensive live provider calls
must remain opt-in through expansions such as `include=models`.

`refresh=true` means bypass the short live-check/model-list cache. It does not
force node config, metadata, or secrets to reload.

## RBAC And Policy

Connections should integrate with the user/RBAC system. Authorization must be
checked at the point of use, not only when rendering the dashboard.

Policy should support both broad and action-specific permissions:

```json
{
  "policy": {
    "read": ["role:platform-admin", "role:developer"],
    "admin": ["role:platform-admin"],
    "use": ["role:developer"],
    "use:generate": ["role:agent-user"],
    "use:embed": ["role:index-admin"],
    "use:read_objects": ["role:restore-admin"],
    "use:write_objects": ["role:backup-admin"],
    "use:read_stream": ["role:ingestion-admin"]
  }
}
```

Recommended permission names:

- `connection:read`
- `connection:admin`
- `connection:use`
- `connection:use:generate`
- `connection:use:embed`
- `connection:use:rerank`
- `connection:use:fetch_content`
- `connection:use:read_objects`
- `connection:use:write_objects`
- `connection:use:read_stream`
- `connection:secret_ref:read`

Workflow-level checks should combine ordinary resource permissions with
connection-use permissions. Examples:

- Creating an embedding index requires table/index-admin permission and
  `connection:use:embed` on the selected inference connection.
- Agent generation requires agent/API permission and `connection:use:generate`.
- Backup creation requires backup-admin permission and
  `connection:use:write_objects`.
- Restore requires restore-admin permission and `connection:use:read_objects`.
- CDC setup requires table replication-source admin permission and
  `connection:use:read_stream`.
- Remote URL ingestion requires ingest permission and
  `connection:use:fetch_content`.

The `/connections` response should include current-user affordances:

```json
{
  "permissions": {
    "can_read": true,
    "can_use": true,
    "can_admin": false,
    "can_view_secret_refs": false
  }
}
```

## API Surface

Initial inventory endpoint:

```text
GET /db/v1/connections
```

Query parameters:

- `types`: comma-separated connection kinds, such as `inference,cdc`.
- `include`: comma-separated expansions. First expansion: `models`.
- `refresh`: `true` to bypass the short server-side live-check cache.

Future first-class resource endpoints:

```text
GET    /db/v1/connections
POST   /db/v1/connections
GET    /db/v1/connections/{connectionId}
PATCH  /db/v1/connections/{connectionId}
DELETE /db/v1/connections/{connectionId}

GET    /db/v1/connections/{connectionId}/policy
PUT    /db/v1/connections/{connectionId}/policy

POST   /db/v1/connections/{connectionId}/probe
GET    /db/v1/connections/{connectionId}/status
```

The first implementation can return config-derived connections only. The API
shape should still reserve fields for the first-class resource model:

- `id`
- `display_name`
- `capabilities`
- `purposes`
- `permissions`
- `sources`
- kind-specific payload

## Secrets

Connections must be secret-safe by default.

Do not return:

- raw API keys
- raw DSNs
- bearer tokens
- HTTP credential headers
- resolved object-store credentials

Allowed by default:

- provider type
- non-secret endpoint URL
- buckets/prefixes when not marked secret
- configured host/base URL
- CDC table/status identifiers
- redacted health/error state

Optional future secret-reference visibility should be explicit and separate:

```json
{
  "secret_refs": [
    {
      "field": "api_key",
      "ref": "${secret:openai.api_key}",
      "status": "present"
    }
  ]
}
```

That expansion should require `connection:secret_ref:read`.

## First Implementation Slice

Implement now:

- Use top-level kinds `inference`, `object_store`, `remote_content`, and `cdc`.
- Return kind-specific payloads named `inference`, `object_store`,
  `remote_content`, and `cdc`.
- Keep `GET /db/v1/connections` config-derived and cheap by default.
- Keep `include=models` as the explicit live inference-provider expansion.
- Keep `refresh=true` scoped to short live-check caches.
- Derive CDC connections from table `replication_sources_json` and
  `replication_source_statuses`.
- Surface CDC status as `connected` when a non-configured runtime phase exists,
  `error` when the status has an error/failure class, and `configured` when no
  live status exists.
- Expose `permissions` as a response field when auth wiring is ready; until
  then omit it rather than guessing.

Do not implement in this slice:

- persisted connection resources
- policy mutation endpoints
- raw secret or DSN display
- per-connection secret editing
- cross-node connection scheduling

## Migration Notes

The previous draft names `inference_provider` and `remote_content_http` should
not ship as stable public enum values. If an internal branch has generated
clients with those names, regenerate from the current OpenAPI spec.

If compatibility is needed before GA, accept old query filter aliases for a
short period:

- `inference_provider` -> `inference`
- `remote_content_http` -> `remote_content`

Do not return old names in responses.
