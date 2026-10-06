# Connections

This document defines the long-run `/connections` interface and the first
implementation target for Antfly Zig.

Connections are external systems Antfly can use for inference, storage, content
fetching, replication, agents, backups, and related workflows. They should
become first-class resources with inventory, health, capability, and
authorization metadata.

## Local Google and AWS login

`antfly connections` manages provider credentials separately from Antfly identity
under `antfly auth`. ChatGPT uses the local server's protected grant store (see
[CHATGPT.md](CHATGPT.md)). Google and AWS use vendor-managed local credential
stores, shared by the existing storage and inference adapters. These CLI commands
do not create named resources in the `/connections` inventory.

```sh
antfly connections login google --project my-project
antfly connections list google
antfly connections logout google

antfly connections login aws --profile work
antfly connections list aws --profile work
antfly connections logout aws --profile work
```

Install `gcloud` for Google or AWS CLI v2 for AWS (2.32 or later for console
sign-in). Antfly delegates browser authorization to these tools and needs no
Antfly-owned OAuth client or AWS application registration. Commands work without
a running Antfly server; `ANTFLY_URL` and Antfly API authentication do not affect
them. Run them as the OS user that runs Antfly and restart an existing server
after login/logout. Credentials stay on that machine and are shared with other
local applications using the same vendor credential store.

### Google Application Default Credentials

Login runs `gcloud auth application-default login` with `cloud-platform`, `openid`
and email scopes, then checks that ADC can refresh a token. `--no-browser` uses
gcloud's local manual URL flow. `--project` selects the ADC quota project; it does
not create a project, enable APIs, grant IAM access or configure Antfly's Vertex
resource project.

GCS default credentials and Vertex inference already load ADC and refresh
`authorized_user` tokens. Configure Vertex's `project_id` and `location` (or the
existing supported project environment variables), enable the required Google
APIs and grant the user suitable IAM permissions. Google embeddings here mean
Vertex AI embeddings billed to a Google Cloud project; consumer Gemini
subscriptions and Gemini API keys remain separate.

`GOOGLE_APPLICATION_CREDENTIALS` overrides local user ADC. Management commands
reject that override instead of changing an unused credential file. Explicit
service-account credentials remain supported in resource configuration.
`CLOUDSDK_CONFIG` selects the vendor config directory. Logout revokes the shared
local ADC and can affect other applications. `list` checks credential refresh,
not permission to every bucket/model. Tokens and credential-check diagnostics
never appear in its JSON output.

### AWS console and IAM Identity Center

Always specify `--profile`, including for list/logout. A configured SSO profile
automatically uses `aws sso login`; `--sso` selects it explicitly. Configure IAM
Identity Center first with `aws configure sso --profile work`. SSO supports
`--no-browser`. Console sign-in uses `aws login` and requires a local browser;
Antfly does not enable the vendor's separate remote authorization flow.

For profiles containing `login_session`, `sso_session` or legacy `sso_start_url`,
the credential resolver shared by S3 and Bedrock delegates resolution and refresh
to `aws configure export-credentials --format process`. Captured credentials stay
in memory, temporary grants retain expiration, and the existing cache refreshes
them before expiration. The subprocess shares the request's remaining deadline
and cancellation, with a 30-second ceiling, bounded output and no shell
interpolation. A failed selected browser profile fails closed rather than
falling back to instance metadata or stale static keys.

Start Antfly with `AWS_PROFILE=work` for default S3 and Bedrock/Titan credentials.
Existing environment static keys and web identity take precedence; unset static
keys to use browser credentials. A named S3 connection can select a profile
explicitly:

```yaml
connections:
  archives:
    kind: external_io
    capabilities: [objects.read, objects.write, backup.write, restore.read]
    external_io:
      protocol: s3
      buckets: [my-archive-bucket]
      credentials:
        source: profile
        profile: work
```

The identity needs separate IAM permissions for S3 and Bedrock and an applicable
region/model configuration. Login does not grant permissions or model access.
Native static profiles, service credentials, web identity, ECS and instance
credentials retain their paths. Explicit `shared_credentials_file` retains
file-only semantics. Browser profiles require the AWS CLI to remain installed
for refresh. Antfly does not execute arbitrary `credential_process` commands
itself.

Console logout selects the requested login profile. Profiles without a console
login session are rejected. `aws sso logout` clears **all** cached SSO sessions;
Antfly rejects per-profile SSO logout and explains how to invoke that global
vendor command deliberately. Restart servers after logout to discard cached
unexpired credentials. Local logout does not promise immediate upstream
revocation of all issued STS credentials.

### Configuration and hosted boundary

Top-level `connectors` owns application integration policy and OAuth settings;
`connections` owns configured resources; `auth_providers` owns Antfly identity
login. Only `connectors.chatgpt.enabled` is implemented in this slice. Local
Google/AWS credentials need no placeholder `connectors.google` or
`connectors.aws` settings and are not per-user Antfly server grants.

Antfly Cloud should use managed workload identities or explicitly configured
service credentials. Laptop login does not authenticate a remote Antfly/Colony
deployment. Hosted per-user grants, `auth_ref`, Google Drive indexing and scope
escalation, cloud inference sign-in and remote credential transfer remain future
work. Hosted connectors must define OAuth registration, tenant ownership, scopes,
protected grant storage, refresh/revocation and resource references before adding
login endpoints.

Vendor contracts: [Google ADC login](https://docs.cloud.google.com/sdk/gcloud/reference/auth/application-default/login),
[ADC resolution](https://docs.cloud.google.com/docs/authentication/application-default-credentials),
[AWS console sign-in](https://docs.aws.amazon.com/signin/latest/userguide/command-line-sign-in.html),
[AWS credential export](https://docs.aws.amazon.com/cli/latest/reference/configure/export-credentials.html)
and [AWS logout](https://docs.aws.amazon.com/cli/latest/reference/logout/).

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

Live probes use bounded fanout on the server's shared `std.Io` runtime.
Backup, restore, and probe clients receive separate network and filesystem
authorities: S3/GCS requests and credential refresh borrow `apiNetworkIo()`,
while local repository paths, filesystem probes, Google credential files, and
AWS profile/web-identity/container token files use `apiFilesystemIo()`.
Remote clients retain their network authority at construction; choosing an I/O
runtime later cannot replace that captured transport. Embedded callers that
omit an authority use an owned fallback. This keeps server probe cost
proportional to active network work and the configured worker bound, even when
many buckets and credential domains are registered.

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
