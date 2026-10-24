# Portable source ingestion

This module acquires canonical text from GitHub repositories and collaboration
resources, Google Drive (including Docs and Slides text exports), GCS, and S3.
It has no Colony, Cloud account, PostgreSQL, or Antfly CLI dependency.

`Connector` receives credentials through an injected client/access resolver.
`Normalize` produces the same complete versioned document used by hosted imports.
`Runner` supplies a filesystem-backed, one-shot runtime; Colony supplies its own
credential, staging, scheduling and durable job adapters around the same providers.

## Self-hosted sync and search

Build from this checkout:

```sh
cd go/pkg/sources
go install ./cmd/antfly-source
```

Create `sources.json` with secret **references**, rather than secret values:

```json
{
  "version": 1,
  "endpoint": "http://127.0.0.1:8080/api/v1",
  "data_plane_token_env": "ANTFLY_WRITE_KEY",
  "connections": {
    "company-docs": {
      "provider": "s3",
      "access_key_env": "SOURCE_AWS_ACCESS_KEY_ID",
      "secret_key_env": "SOURCE_AWS_SECRET_ACCESS_KEY"
    }
  },
  "sources": [{
    "id": "11111111-1111-4111-8111-111111111111",
    "alias": "manuals",
    "connection": "company-docs",
    "table": "sources",
    "config": {"bucket": "company-docs", "prefix": "manuals/"},
    "permissions": {"public": true, "readers": []}
  }]
}
```

Source UUIDs and connection map keys are durable identities. Multiple sources can
reuse a connection. Other connection forms:

- GitHub: `provider: "github"`, `token_env: "SOURCE_GITHUB_TOKEN"`; use a token
  scoped to the selected repositories/resources. Config accepts `repositories`
  with per-repository `repository`, `ref`, `prefix` and `include` selections.
  Supported includes: code, issues, prs, comments, reviews and discussions.
- Drive: `provider: "google_drive"`, `credentials_env: "SOURCE_GOOGLE_JSON"`;
  config has `folder_id` and optional `shared_drive_id`.
- GCS: `provider: "gcs"`, the same explicit Google JSON reference;
  config has `bucket` and optional `prefix` or `key`.
- S3-compatible storage: optional `s3_endpoint`, `s3_insecure`, and
  `session_token_env`. Insecure S3 transport is an explicit operator choice.

Google credentials are service-account or authorized-user JSON. There is no
ambient ADC fallback or browser enrollment. The caller owns secret storage;
Google token renewal is handled by the injected OAuth client.

```sh
antfly-source sync --config sources.json \
  --state-dir /var/lib/antfly-sources --catalog-out ./catalog.json

export ANTFLY_GREP_TABLE_TOKEN="$ANTFLY_READ_KEY"
antfly-grep --source-catalog ./catalog.json --source manuals 'retry budget'
```

The runner creates a missing destination through the normal table API, writes
canonical rows with a write barrier, and checks revision, content digest and
permissions through a snapshot-capable query before declaring them searchable.
Use a dedicated destination: the runner never imports arbitrary application rows
into its ownership ledger. All queries use existing Antfly table endpoints;
self-hosted grep never invokes `antfly-cloud`.

The catalog contains aliases, physical tables, selection filters and readiness,
not provider/data-plane credentials. `permissions.public` means public within the
independently authorized table scope. **Catalogs and document permission metadata
do not install server authorization.** For shared deployments configure Antfly or
a trusted gateway's authentication/row policies, or separate tables and keys by
trust boundary. Catalog principals are only grep's additional local filter.

## Recovery and operation

Run the command periodically with cron, systemd timers, or a Kubernetes CronJob
using persistent state and a single writer. A process lock prevents overlapping
runs against the same state directory. The state is scoped to source ID, endpoint
and table; it cannot silently retarget ownership to a different destination.

A complete inventory is pinned before publication. The ownership journal is
fsynced before writes, so a lost acknowledgement cannot lose potentially written
rows. Retrying uses the pinned selection; `--restart` explicitly re-enumerates
while retaining the ownership ledger. Provider revision/access failures and
incomplete inventories leave the catalog stale. GitHub omitted identities are
checked independently before deletion. Only previously owned IDs are retired.

The catalog is marked syncing before writes and published atomically after
readiness and reconciliation. Its permission lease starts at enumeration, not
retry completion (default 900 seconds, configurable 60..86400). Grep rejects
pending, syncing, stale or expired bindings. Existing Antfly server authorization
remains responsible for current access during retained query snapshots.

Each item is at most 32 MiB of complete UTF-8 text; each inventory has at most
10,000 items, and a sync reads at most 10 GiB. Metadata/state/catalog sizes are
bounded separately. A bounded `max_objects` listing is not deletion authority and
cannot establish a complete searchable publication. Binary extraction, Sheets
whole-workbook extraction, visual rendering, provider-native ACL mirroring,
webhooks, distributed scheduling and live-provider qualification are outside this
runner's current text-ingestion scope. Removing a source from configuration hides
it from the emitted catalog and retains its indexed records/state; it is not a
purge request.

Ownership of up to 100,000 potentially written rows is retained across interrupted
inventories; active inventories remain limited to 10,000 items. Reconciliation
checks deletion visibility before publishing a searchable catalog.

## Durable Antfly catalog

The runner publishes one versioned `source_catalog` record per source in the
ordinary `antfly_sources` table. Set `catalog_table` in a profile to choose another
name. Content destinations cannot use that table. Records contain the source
binding, provider kind, document version, published revision, explicit sharing
metadata and freshness lease. Secrets, provider selectors and job checkpoints
stay outside Antfly's catalog. `--catalog-out` is an optional file export; the
runner also retains a private mirror in its state directory for recovery.

```sh
antfly-source sync --config sources.json --state-dir /var/lib/antfly-sources
antfly-grep --source-endpoint https://antfly.example/api/v1 \
  --source manuals 'retry budget'
```

Use `--source-table NAME` for a custom catalog and repeat `--source-principal ID`
for additional local reader filtering on private content. Authenticate with the
normal data-plane token. Provision server read/row policy for both catalog and
content tables; only the ingestion identity should be able to write the catalog.
Cloud supplies its own current permission checks over the same catalog records.

A snapshot-visible syncing record precedes any content mutation. Searchable
publication follows query-visible writes and owned-only reconciliation. There is
no cross-table transaction; interrupted publication remains syncing/stale or has
only its previous bounded lease. Catalog reads fail on invalid versions, missing
publication metadata, snapshot changes, duplicates and traversal budgets. Removing
a source from a profile retires its catalog record while retaining content/state;
an empty source list retires all entries owned by that profile.
