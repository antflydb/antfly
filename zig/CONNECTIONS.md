# Connections

This document defines the long-run `/connections` interface and the first
implementation target for Antfly Zig.

Connections are external systems Antfly can use for inference, storage, content
fetching, replication, agents, backups, and related workflows. They should
become first-class resources with inventory, health, capability, and
authorization metadata.

## Goals

- Give operators one inventory of configured external systems.
- Keep provider-specific details out of top-level connection kinds.
- Avoid leaking secrets or raw DSNs through inventory APIs.
- Let RBAC authorize use of a connection for a specific workflow.
- Make UI affordances explicit: a user should know which connections they can
  see, use, and administer.
- Use the same public model in configuration and API responses before the
  product has shipped, so users learn one connections vocabulary.

## Naming

Top-level connection kinds are broad physical categories:

- `inference`
- `web_search`
- `external_io`
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
  "kind": "web_search",
  "provider": "exa",
  "web_search": {
    "max_results": 10
  }
}
```

```json
{
  "kind": "external_io",
  "capabilities": ["content.fetch"],
  "external_io": {
    "protocol": "http"
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

Use `web_search` for queryable external knowledge/search providers with ranking,
snippets, citations, freshness, and agent-tool behavior. Use `external_io` for
configured external read/write/fetch endpoints such as S3, GCS, filesystem
paths, and HTTP content sources. Keep the transport in
`external_io.protocol`. Express both technical actions and workflow use cases in
namespaced `capabilities`.

Do not use `access` as the kind name. It collides conceptually with RBAC and
authorization language. `external_io` is more explicit: it describes external
input/output endpoints, while permissions describe who may use them.

## Core Model

Connections are configured under one public `connections` map. The map key is
the stable connection ID. The same ID is used by API responses, workflow
references, future policy resources, and future persisted connection records.

```yaml
connections:
  openai-prod:
    display_name: OpenAI production
    kind: inference
    capabilities:
      - models.generate
      - models.embed
      - agents.use
      - indexing.use
    inference:
      provider: openai
      url: https://api.openai.com
      api_key: ${secret:openai.api_key}

  agent-web:
    display_name: Exa agent search
    kind: web_search
    provider: exa
    capabilities:
      - web.search
      - web.semantic_search
      - web.fetch
      - agents.use
    web_search:
      max_results: 10
      include_content: true
      include_highlights: true
      api_key: ${secret:exa.api_key}

  docs-site:
    kind: external_io
    capabilities:
      - content.fetch
      - indexing.use
      - agents.use
    external_io:
      protocol: http
      hosts:
        - https://docs.example.com
      headers:
        Authorization: ${secret:docs.token}

  backups:
    kind: external_io
    capabilities:
      - objects.read
      - objects.write
      - backup.write
      - restore.read
    external_io:
      protocol: s3
      endpoint: https://s3.us-east-1.amazonaws.com
      buckets:
        - antfly-backups
      prefix: prod/
      access_key_id: ${secret:aws.access_key_id}
      secret_access_key: ${secret:aws.secret_access_key}

  users-pg:
    kind: cdc
    capabilities:
      - cdc.read_stream
    cdc:
      provider: postgres
      table_name: users
      source_ordinal: 0
      external_table: public.users
      dsn: ${secret:pg.replication_dsn}
```

```json
{
  "id": "conn_openai_prod",
  "name": "openai-prod",
  "display_name": "OpenAI production",
  "kind": "inference",
  "status": "connected",
  "sources": ["config:generators/primary"],
  "capabilities": [
    "models.generate",
    "models.embed",
    "agents.use",
    "indexing.use"
  ],
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

Capabilities and policy are separate concerns:
Use one namespaced `capabilities` list for both low-level actions and
workflow-specific uses:

- `policy`: who may read, use, or administer it for each action.
- technical examples: `objects.read`, `objects.write`, `content.fetch`
- workflow examples: `backup.write`, `restore.read`, `indexing.use`,
  `agents.use`

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
  "capabilities": ["models.generate", "models.embed", "agents.use", "indexing.use"],
  "inference": {
    "provider": "openai",
    "url": "https://api.openai.com",
    "configured_model_types": ["generator", "embedder"]
  }
}
```

Common capabilities:

- `models.generate`
- `models.embed`
- `models.rerank`
- `models.chunk`
- `models.transcribe`
- `models.recognize`
- `models.rewrite`
- `models.read`
- `models.classify`
- `models.extract`
- `agents.use`
- `indexing.use`

The connection ID `local-inference` is reserved for the inference runtime
embedded in the Antfly instance and cannot be used by configured connections.
Proxying an inference operation requires its matching `models.<operation>`
capability on the selected connection.

### Web Search

Web-search connections describe queryable external search providers. They are
separate from `external_io` because the user-facing contract is search results,
ranking, snippets, citations, freshness, and optional content extraction rather
than generic HTTP access.

Example:

```json
{
  "id": "conn_agent_web",
  "kind": "web_search",
  "provider": "exa",
  "capabilities": ["web.search", "web.semantic_search", "web.fetch", "agents.use"],
  "web_search": {
    "max_results": 10,
    "safe_search": true,
    "include_content": true,
    "include_highlights": true,
    "configured": true
  }
}
```

Common providers:

- `exa`
- `tavily`
- `brave`
- `serper`
- `you`
- `linkup`
- `vertex`

Common capabilities:

- `web.search`
- `web.semantic_search`
- `web.news`
- `web.images`
- `web.fetch`
- `web.answer`
- `agents.use`
- `indexing.use`

### External IO

External-IO connections describe configured external read/write/fetch endpoints.
They cover object stores and remote-content sources under one kind.

Example:

```json
{
  "id": "conn_s3_backups",
  "kind": "external_io",
  "capabilities": ["objects.read", "objects.write", "backup.write", "restore.read"],
  "external_io": {
    "protocol": "s3",
    "endpoint": "https://s3.us-east-1.amazonaws.com",
    "buckets": ["antfly-backups"],
    "prefix": "prod/"
  }
}
```

HTTP remote content uses the same kind:

```json
{
  "id": "conn_docs_site",
  "kind": "external_io",
  "capabilities": ["content.fetch", "indexing.use", "agents.use"],
  "external_io": {
    "protocol": "http",
    "hosts": ["https://docs.example.com"]
  }
}
```

Common protocols:

- `s3`
- `gcs`
- `filesystem`
- `http`

Common capabilities:

- `objects.read`
- `objects.write`
- `content.fetch`
- `backup.read`
- `backup.write`
- `restore.read`
- `models.load`
- `indexing.use`
- `agents.use`

Credentials and headers must not be returned by default. If the UI needs to
show that a secret reference exists, expose a redacted secret-ref summary behind
an explicit `connection.secret_ref:read` permission.

For backup and restore connections, object-store credential references are
resolved when an operation or live probe starts rather than expanded into
long-lived plaintext at node startup. Secret rotation consequently applies to
new backup, restore, and probe clients without a restart. Each client retains
one consistent snapshot for its lifetime, and a probe cache key includes only a
cryptographic digest of the resolved credential material. Primary object
storage resolves its snapshot at bootstrap; dynamic AWS credential sources
refresh in place, while rotating static primary keys requires a controlled
restart. Connection capabilities and bucket/prefix scopes remain config-owned
authorization and are never sourced from the secret store.

Live probes use bounded fanout on the server's shared `std.Io` runtime. S3,
GCS, and credential-refresh transports borrow that runtime rather than creating
one threaded scheduler per connection. This keeps probe cost proportional to
active network work and the configured worker bound, even when many buckets and
credential domains are registered.

### CDC

CDC connections describe change-stream sources. Postgres is the first provider.

Example:

```json
{
  "id": "conn_pg_users_cdc",
  "kind": "cdc",
  "provider": "postgres",
  "status": "connected",
  "capabilities": ["cdc.read_stream"],
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

CDC inventory is derived from `connections` config plus persisted
replication-source status when a configured CDC connection points at a table
and source ordinal. Raw DSNs and resolved credentials must never appear in
`/connections`.

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
    "use:models.generate": ["role:agent-user"],
    "use:models.embed": ["role:index-admin"],
    "use:objects.read": ["role:restore-admin"],
    "use:objects.write": ["role:backup-admin"],
    "use:cdc.read_stream": ["role:ingestion-admin"]
  }
}
```

Recommended permission names:

- `connection:read`
- `connection:admin`
- `connection:use`
- `connection:use:models.generate`
- `connection:use:models.embed`
- `connection:use:models.rerank`
- `connection:use:content.fetch`
- `connection:use:objects.read`
- `connection:use:objects.write`
- `connection:use:backup.read`
- `connection:use:backup.write`
- `connection:use:restore.read`
- `connection:use:cdc.read_stream`
- `connection:secret_ref:read`

Workflow-level checks should combine ordinary resource permissions with
connection-use permissions. Examples:

- Creating an embedding index requires table/index-admin permission and
  `connection:use:models.embed` on the selected inference connection.
- Agent generation requires agent/API permission and
  `connection:use:models.generate`.
- Backup creation requires backup-admin permission and
  `connection:use:backup.write`.
- Restore requires restore-admin permission and `connection:use:restore.read`.
- CDC setup requires table replication-source admin permission and
  `connection:use:cdc.read_stream`.
- Remote URL ingestion requires ingest permission and
  `connection:use:content.fetch`.

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

- `types`: comma-separated connection kinds, such as `inference,web_search,external_io,cdc`.
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

The first implementation returns config-derived connections only. The API shape
uses the first-class resource model now:

- `id`
- `display_name`
- `capabilities`
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

- Configure public connection resources under top-level `connections`.
- Use top-level kinds `inference`, `web_search`, `external_io`, and `cdc`.
- Return kind-specific payloads named `inference`, `web_search`, `external_io`, and `cdc`.
- Keep `GET /db/v1/connections` config-derived and cheap by default.
- Keep `include=models` as the explicit live inference-provider expansion.
- Keep `refresh=true` scoped to short live-check caches.
- Derive CDC connections from `connections` config and enrich them from
  `replication_source_statuses` when table/source status exists.
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
