# Portable source ingestion

This module acquires canonical text from GitHub repositories and collaboration
resources, Google Workspace (Drive, Gmail and Calendar), Slack, Discord, GCS, and S3.
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
  "endpoint": "http://127.0.0.1:8080/db/v1",
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
- Google Workspace: `provider: "google_workspace"`,
  `credentials_env: "SOURCE_GOOGLE_JSON"`, and explicit `services` containing
  `drive`, `gmail`, and/or `calendar` (default: Drive only). Source configs select
  `resource: "drive"` with `folder_id` and optional `shared_drive_id`,
  `resource: "gmail"` with optional `label_ids`, or `resource: "calendar"` with
  `calendar_ids`. One connection can supply separately shared sources for each.
- Slack: `provider: "slack"`, `token_env: "SOURCE_SLACK_TOKEN"`; source config
  has `team_id` and `channel_ids`. The token must read the selected channels and
  their replies. Membership and workspace identity are checked explicitly.
- Discord: `provider: "discord"`, `token_env: "SOURCE_DISCORD_TOKEN"`; source
  config has `guild_id` and `channel_ids`. The bot needs message-content access,
  channel visibility and history permissions. Threads are included by default;
  `include_threads: false` limits selection to explicitly named channels.
- GCS: `provider: "gcs"`, the same explicit Google JSON reference;
  config has `bucket` and optional `prefix` or `key`.
- S3-compatible storage: optional `s3_endpoint`, `s3_insecure`, and
  `session_token_env`. Insecure S3 transport is an explicit operator choice.

Google credentials are service-account or authorized-user JSON. There is no
ambient ADC fallback or browser enrollment. The caller owns secret storage;
Google token renewal is handled by the injected OAuth client.

Set `attachments: true` on a Slack, Discord, Gmail, Calendar, or Drive source to
extract attached/binary content. Text, HTML, DOCX and PPTX work directly; PDFs
require `pdftotext`, and images require `tesseract`, or an injected extractor.
Unsupported formats and failed extraction stop publication explicitly. Calendar
Drive attachments also require the connection's `drive` service. Drive links,
mail thread headers, calendar invitation UIDs, and attachment parent identities
are retained as relationships; linked content remains independently authorized.
See [collaboration source design](../../../docs/design/collaboration-sources.md)
for the hosted onboarding flow, scale limits, and qualification requirements.

```sh
antfly-source sync --config sources.json \
  --state-dir /var/lib/antfly-sources --catalog-out ./catalog.json

export ANTFLY_GREP_TABLE_TOKEN="$ANTFLY_READ_KEY"
antfly-grep --source-catalog ./catalog.json --source manuals 'retry budget'
```

The runner creates a missing destination through the normal table API, writes
canonical rows with a write barrier, and checks revision, content digest and
permissions through a snapshot-capable query before declaring them searchable.
Multiple sources may share an ingestion destination. The runner never imports
arbitrary application rows into its ownership ledger. All queries use existing Antfly table endpoints;
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
checked independently before deletion. Legacy provider-key rows are retired only when present in the ownership journal.

The catalog is marked syncing before writes and published atomically after
readiness and reconciliation. For collaboration providers, a fresh container-access check before publication starts
the permission lease. Other providers retain the enumeration-time lease
(default 900 seconds, configurable 60..86400). Grep rejects
pending, syncing, stale or expired bindings. Existing Antfly server authorization
remains responsible for current access during retained query snapshots.

Each item is at most 32 MiB of complete UTF-8 text; collaboration inventories admit up to 100,000 items (other providers retain their
10,000-item limit), and a sync reads at most 10 GiB. Metadata/state/catalog sizes are
bounded separately. A bounded `max_objects` listing is not deletion authority and
cannot establish a complete searchable publication. Sheets
whole-workbook extraction, visual rendering, provider-native ACL mirroring,
webhooks, distributed scheduling and live-provider qualification are outside this
runner's current text-ingestion scope. Removing a source from configuration hides
it from the emitted catalog and retains its indexed records/state; it is not a
purge request.

Ownership of up to 1,000,000 potentially written rows is retained across interrupted
inventories; active inventory limits still apply. Reconciliation
checks deletion visibility before publishing a searchable catalog.

Gmail persists history IDs and Calendar persists per-calendar sync tokens after a
successful publication. Expired cursors trigger a complete rebuild. Successful
inventory pages are checkpointed privately so rate-limited backfills can resume;
checkpoints do not replace current access checks. Slack and Discord use complete
polling inventories; event-driven ingestion and Drive changes cursors remain future
work. Provider reads still verify pinned content on each publication. Calendar sync also
refreshes previously imported Drive attachments, whose files can change without a
Calendar event update.

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
antfly-grep --source-endpoint https://antfly.example/db/v1 \
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


Canonical readiness is shared with hosted ingestion. A snapshot must expose the
complete normalized metadata projection, including URI/path, sharing, connection
metadata, indexing time and content digest, before publication can complete.
Unchanged provider revisions do not bypass this check. GCS cache access checks
are pinned to the original object generation and enforce its current permissions.

S3 inventories persist the exact `version_id`, resolving prefix-list entries with
HEAD before pinning. Downloads and cached-byte authorization address that version
and verify the returned version, ETag and size. The literal `null` version is sent
explicitly. Versionless snapshots are accepted only while the provider still
returns an unversioned object with the pinned ETag and size; a versioned response
requires a freshly enumerated snapshot. A readable replacement never authorizes
cached bytes from another version.

S3 cache authorization exercises the content data plane through a conditional GET
of the pinned version, so revoked SSE-KMS decrypt access blocks staged plaintext
reuse. Nonempty objects request a one-byte range; empty objects use an ordinary
GET. Responses must match version, ETag and object size, and the probe reads at
most one byte even when an S3-compatible server ignores Range.

## Shared LinearMerge synchronization

Colony source jobs and `antfly-source` use the same `SyncWriter`. Provider IDs
remain stable acquisition/retry identities; destination keys use
`source://job/<source UUID>/records/<SHA-256 of provider ID>`. Each source owns an
exclusive key range, so reconciling one source cannot remove another source's
records or unrelated application keys. Hosted deployments still reserve source
tables to protect their ACL markers.

The writer first queries normalized metadata, preserving `indexed_at` only when
all canonical fields and the content digest are unchanged. Volatile enumeration
positions are excluded. Non-destructive LinearMerge pages compare canonical
content hashes and skip unchanged rows. Every page must pass the shared snapshot
readiness barrier. Only a fully verified authoritative inventory can issue the
final bounded cleanup, retaining its verified IDs without uploading their text a
second time. Partial selections, failed reads, and repair retries never reconcile
missing rows. Provider access/absence checks precede cleanup, and a fresh query
proves the range contains no extra records before catalog publication.

The existing `/tables/{table}/merge` endpoint accepts the versioned
`application/vnd.antfly.scoped-merge.v1+json` envelope (`scoped_merge` with explicit
`key_range`, `records`, optional `retain_ids`, and `delete_missing`). Bounds are
exclusive; deletion defaults to false. `retain_ids` is valid only on empty-record
cleanup and every retained ID must exist before storage mutation starts. There is
no fallback to the legacy unbounded merge format. Upgrade the Antfly server before
upgrading ingestion workers; older servers reject this envelope before mutation.

Existing standalone state is upgraded in place: write and verify the bounded
representation, reconcile its complete inventory, then delete only journaled
legacy provider keys. Interrupted migration resumes from the same durable state.
Colony preserves its job manifests, attempt fences, and ownership ledger, retiring
legacy rows only after their replacement is query-visible. Source IDs must remain
stable, and overlapping writers for the same source must be serialized (the
standalone state lock or Colony's job admission). Distinct sources can sync
independently. `antfly-grep` continues to use the catalog and ordinary queries;
it does not schedule ingestion.


Workspace consent expansion preserves standalone acquisition checkpoints and the
published Gmail/Calendar incremental baseline. Only the connection's service list
is excluded from replay identity; credential references, source selection and
sharing remain bound, and resumed reads recheck current account/service access.
Old checkpoints adopt this identity when their complete old configuration still
matches. If that proof is unavailable after an earlier configuration change, use
an explicit restart to select fresh content.


Standalone collaboration leases begin at the start of the final permission audit.
The audit receives the configured TTL as its deadline; an expired audit keeps its
pinned inventory for retry and leaves the source stale. A slow check cannot mint
a new full lease after its earliest scope checks have aged out.

Google API errors retain a bounded reason code and provider `Retry-After` deadline.
Documented `rateLimitExceeded` and `userRateLimitExceeded` responses are retryable
on HTTP 403 as well as 429. Permission, domain-policy, usage-cap and unknown 403
errors remain permanent. The same classification applies to Gmail, Calendar and
legacy Drive/GCS acquisition, including Calendar-linked Drive downloads/exports.
`IsRateLimited` recognizes wrapped errors for audit waits and progress accounting;
provider messages and response bodies never enter persisted error diagnostics.

Discord previews sample at most eight channel/thread containers and never follow
archive cursors; narrowing the selection is required when the sample is exhausted.
Normal syncs retain complete archive pagination. Google OAuth refresh failures use
the same safe retry/cooldown contract as resource errors: transient failures retry,
invalid grants remain permanent, and token response bodies are not retained.

Calendar attachment previews acquire at most eight Drive files; unsampled files
receive no Drive reads. Attachment downloads and native exports share the remaining
parent byte budget. Discord resource lists reuse one guild authority snapshot and
preserve provider rate limits instead of treating them as missing access.
