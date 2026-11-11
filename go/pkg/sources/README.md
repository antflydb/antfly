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

- Local store: `provider: "local"`, `local_root: "/var/lib/antfly-uploads"`; for
  deployments with no object store. See [local content store](#local-content-store).
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
permissions through a snapshot-capable query before declaring them searchable. On Lite, which
cannot retain a snapshot, the same query reads live state and says so with `live_read`.
An empty live result does not prove absence while inserts are awaiting indexing.
Lite therefore checks authoritative cleanup against primary storage through scoped
merge, with `full_index` visibility before completion. Catalog publication and
retirement also use `full_index`. Retained snapshots keep the no-op cleanup shortcut.
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

The writer first queries a bounded list of document identities using
`{"query":{"doc_id":{"ids":["DOCUMENT_KEY"]}}}` and requires a successful fresh
snapshot. It compares the complete normalized projection and content digest,
including the outer ownership/filtering fields. Identical rows are verified
without issuing a content merge, and keep their original `indexed_at`. Input
text must match its normalized SHA-256 digest. Volatile enumeration positions
are excluded. Mixed pages merge only the changed rows, with deletion disabled;
those rows must pass the shared snapshot readiness barrier.

Only a fully verified authoritative inventory can reconcile missing rows.
After provider access/absence checks, a bounded fresh range query proves whether
there are extra records. A clean range rechecks retained IDs in bounded fresh queries and finishes without a cleanup merge;
otherwise the final scoped merge retains all verified IDs and a fresh query
proves that cleanup is visible. Thus an unchanged sync sends no content or
cleanup merges. Partial selections, failed reads and failed writes never
reconcile missing rows. No-op results include verified rows in `Skipped`, with
no mutation cursor or range acknowledgement.

Read-only data-plane GETs and POST queries retry transient transport failures
and HTTP 408/425/429/500/502/503/504 up to three total attempts, within a shared
30-second budget or the caller's earlier deadline. Backoff is cancellable and
honors a supported `Retry-After`; a cooldown longer than the remaining budget
returns the error immediately. Batch, table creation and scoped merges are never
blindly retried. Applications can implement `SyncMergeValidator` to apply a
read-only budget check to the exact serialized request before dispatch. Local
rejection leaves the item unverified and allows sibling writes; it cannot
authorize deletion. An ambiguous or invalid merge acknowledgement closes that
writer to further mutation or cleanup. Resume the durable source run: its
ownership journal and pinned inventory survive, and a new writer uses fresh
visibility proofs before deciding which rows or range still need work. A
committed write or cleanup whose acknowledgement was lost is therefore not
replayed just to obtain another acknowledgement.

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

Slack lease renewal requires live granular history grants (and files:read for
attachments); channel membership alone is insufficient. Missing scope proofs never
renew a lease. Slack bot tokens require include_threads:false for channel sources.
Calendar confidential visibility is private. Extractor v3 invalidates older
visibility metadata. Discord moderators use effective parent-channel permissions;
ordinary private-thread readers still require explicit membership.

Gmail permission audits prove current body-reading authority through one live,
one-result, ID-only search request. Gmail forbids search queries for metadata-only
credentials, so a profile/label grant cannot renew retained bodies. Proof responses
are never persisted, even with acquisition caching enabled. Successful proof is
shared only within the current bounded audit, including empty mailboxes.

Gmail attachment bytes are acquired lazily after preview selection and remaining
parent byte limits. Previews read at most eight attachments; full sync retains all
selected attachments and Calendar invitation relationships. Known oversized parts
fail before download, and base64 size limits apply before decoded allocation.

Discord acquisition and audits share guild authority, channel metadata and active
threads only within the current operation and access-check window. New audits
always recheck live grants. Moderators use the complete private archive endpoint;
ordinary readers use joined private archives. Complete archive pagination and
absence reconciliation retain accessible messages after a thread is archived.

Calendar `writerWithoutPrivateAccess` is selectable like `reader`: ordinary events
remain readable, while retained private/confidential details cannot be renewed or
reused through this restricted role. Unknown and free/busy-only roles are rejected.

Calendar alias/role resolution is shared within each bounded audit and acquisition
phase. A 20-calendar audit makes 20 calendar metadata reads, rather than 230.
New audits and publication checks clear proofs; long operations refresh expired
proofs. Provider failures never become permission proofs.

Drive authorization verifies the actual token's content scopes as well as the
file's canDownload capability. A drive.file grant also requires isAppAuthorized
on that file; metadata-only grants cannot authorize retained content. Missing or
malformed scope proof remains retryable without renewing publication access.
Permission metadata uses the exact token whose grant was checked, with no content
reads or token-info requests. Scope text and tokens never enter diagnostics.

Build Google clients with GoogleHTTPClient(ctx, credentials) after loading them
through GoogleCredentialsForServices. This preserves verified service-account
JWT scope provenance when token response scopes are omitted. Custom OAuth clients
must expose an oauth2.Transport with actual token scope extras; opaque transports
or authorized-user tokens without scope proof cannot authorize cached Drive data.

## Local native end-to-end qualification

The regular Go suite uses HTTP protocol fixtures. To exercise the real database,
first build `antfly` and `antfly-grep` from the same checkout, start a dedicated
loopback-only standalone, and pass its API to the opt-in test:

```sh
# From zig/, using the repository's supported compiler and worktree caches:
make build
zig build grep -Dgrep-only=true
# Start in a separate terminal; keep all server state inside this worktree.
source_e2e_dir="$PWD/../.cache/source-e2e"
mkdir -p "$source_e2e_dir"
cat > "$source_e2e_dir/config.json" <<EOF
{"public_inference":{"enabled":false},"inference":{"models_dir":"$source_e2e_dir/models"},"secrets":{"native":{"name":"native","path":"$source_e2e_dir/secrets.json"},"environment":true}}
EOF
./zig-out/bin/antfly standalone --config "$source_e2e_dir/config.json" \
  --host 127.0.0.1 --port 18080 --control-tick-ms 5 \
  --data-dir "$source_e2e_dir/data" \
  --replica-root-dir "$source_e2e_dir/replicas" \
  --replica-catalog-path "$source_e2e_dir/catalog.txt" \
  --snapshot-root-dir "$source_e2e_dir/snapshots" --auth false --health false

# From go/pkg/sources/:
ANTFLY_SOURCES_E2E_URL=http://127.0.0.1:18080/db/v1 \
ANTFLY_GREP_TEST_BINARY="$PWD/../../../zig/zig-out/bin/antfly-grep" \
GOWORK=off GOEXPERIMENT=simd go test -v -run '^TestLocalAntflySourcePipeline$' -timeout 20m
```

Use a fresh standalone data directory. The test refuses non-loopback endpoints
and creates uniquely named synthetic tables in the supplied database. It runs the
actual source CLI against a local S3 fixture, publishes 475 records plus another
source sharing the table, and invokes native grep through file and table catalogs.
It checks no-op synchronization, changed records, post-commit connection loss,
cleanup recovery, upstream access failure, and preservation of foreign records.
`ANTFLY_SOURCES_E2E_RECORDS=4` selects a short smoke run; the default is 475.
`ANTFLY_SOURCE_TEST_BINARY` may select an already-built source CLI.

Native GCS PDF artifact fetching has a separate opt-in test in
`examples/grepstein/local_e2e_test.go`. From that example directory, set
`ANTFLY_GREPSTEIN_E2E_BINARY` to the same Antfly binary and run
`go test -v -run '^TestLocalNativeGCSExtraction$' -timeout 10m` with `GOWORK=off`
and `GOEXPERIMENT=simd`. It starts its own isolated standalone with a scoped GCS
connection and a local storage fixture, then verifies generation-bound PDF text
extraction, the qualification gate, and owner-side bucket/prefix/version rejection.
These tests do not qualify real Google identity exchange or model-based OCR,
embeddings, relationship extraction, or production GPU capacity. They are
explicitly opt-in and do not make normal source-module tests build Antfly.

## Local content store

The `local` provider serves a content-addressed directory through the same records
and media protocol as a bucket, so an application keeps the same code when it moves
to object storage. Objects live at `<local_root>/<prefix>/objects/sha256/<digest>[.ext]`.
The name is the revision, and the extension selects the MIME type. Go applications
can write with `PutLocalObject`; others write to a temporary dot-file in that
directory, fsync the file, and publish it atomically without replacing an existing
object (for example, link the temporary file to the final name). Verify any
existing object instead of replacing it, then fsync the containing directories
before starting sync. Use owner-only directories and files. Source config accepts
an optional `prefix`.

`ANTFLY_SOURCE_LOCAL_ROOTS` lists the directories a connection may use, separated by
the platform path-list separator. Unset denies local connections, because whoever can
register a connection would otherwise choose which files the service reads.

The store directory must already exist. A missing directory fails the run rather
than reading as empty, so a lost mount cannot delete every published row. Removing an
object retires its row on the next run. Symlinks and unexpected names fail the run.
Objects are limited to 256 MiB, because delivery reads and verifies the complete object
and serves its ranges from that copy. The service retains verified copies in a 256 MiB
cache, so repeated ranges reuse them while still rechecking access. A changed object
never delivers different bytes under the published ETag: it fails with 412 once its
verified copy is gone, so write new content under its own digest. `PutLocalObject`
replaces a damaged object with the same digest.
Antfly reaches the store through `antfly-source serve`, which it must be allowed to
fetch: list the listener in `remote_content.security.source_media_base_urls`, such as
`http://127.0.0.1:8082/v1/media/`. This allows that listener without opening other
private addresses or ports. Run one source with
`POST /v1/sync` and `{"source_id": "<uuid>"}` to publish a new upload without syncing
the others. Upload callers must set `"restart": true` to enumerate newly written
objects even after a failed pinned inventory. Runs are serialized across the service, so a concurrent run returns 409.

### Moving a local store to a bucket

```sh
# Against a live "serve" instance -- no delivery downtime; its own admission
# serializes the repoint with any other run there.
antfly-source migrate-local --server http://127.0.0.1:8082 --admin-token-env ANTFLY_SOURCE_ADMIN_TOKEN \
  --source <source-uuid> --from-connection local --from-prefix assets \
  --to-connection bucket --to-bucket my-bucket --to-prefix migrated \
  [--dry-run]

# Without --server, in-process against a stopped service's state directory
# (nothing is being delivered either way, so there's no downtime to protect).
antfly-source migrate-local --config sources.json --state-dir /var/lib/antfly-sources/runner \
  --public-url http://127.0.0.1:8082 --admin-token-env ANTFLY_SOURCE_ADMIN_TOKEN --media-key-env ANTFLY_SOURCE_MEDIA_KEY \
  --source <source-uuid> --from-connection local --from-prefix assets \
  --to-connection bucket --to-bucket my-bucket --to-prefix migrated \
  [--dry-run]
```

This copies one source's objects to an S3-compatible or GCS connection,
keeping the `objects/sha256/<digest>[.ext]` layout so the digest stays the
key. Bucket prefixes are literal prefixes: leading slashes in their suffixes
are significant. The migration configures a trailing-slash destination prefix
so those suffixes match the local store's relative keys exactly. Every copy
is verified by an independent read-back and hash of the destination object, whether just uploaded or already there from an earlier
run, before anything can reference it; a mismatch fails that object and the
run (GCS gives back CRC32C and MD5, not SHA-256, so this reads the object
back and hashes it the same way S3 does, rather than trusting either). GCS
uploads pass `ifGenerationMatch=0`, so two runs (or a run colliding with
anything else) can never overwrite each other's object at the same
content-addressed key; S3 has no equivalent atomic precondition, so it relies
on the existence check before uploading instead. Rerunning only uploads
what's still missing, and a clean rerun uploads nothing. Every rerun still
completes a source sync: a committed configuration PUT alone cannot prove
that a previous run published the new references. A retry after a failed
sync or an interrupted cutover therefore repairs publication automatically.
It never deletes or edits the local store; once satisfied, remove
`<local_root>/<prefix>/objects/sha256` yourself.

Copying and verifying never take any service lock; they only read the local
store and write the destination bucket. Repointing (the catalog `PUT` and
resync) is the one step that needs admission, and has two modes:

- **`--server URL`**: sends the same `PUT /v1/sources/{id}` and
  `POST /v1/sync` an operator would, over real HTTP to that live `serve`
  instance's own admin API, and polls the job. Its own run admission
  serializes this with any other run there, so there's no need to stop it
  and no delivery downtime from this process holding a lock. A live 409 ("a
  run is already in progress") is retried with bounded backoff before
  failing clearly.
- **No `--server`**: repoints in-process against `--state-dir`, exactly like
  `serve` would, taking that directory's exclusive lock for the repoint step.
  Use this only with `serve` stopped -- nothing is being delivered either
  way, so there's no downtime to protect, and `migrate-local` refuses to
  start if `serve` (or another `migrate-local` run) already holds that
  directory, rather than racing it.

Live cutover requires a companion advertising conditional source updates
(`X-Antfly-Source-Preconditions: 1` on `/v1/profile`). Migration re-reads the
source after copying, preserves concurrent naming, permission and stream edits,
and rejects concurrent acquisition, identity or transfer-connection changes. Both the source PUT
and sync admission use an atomic `If-Match` precondition, together with
configuration digests for both transfer connections; a conflicting edit
fails cutover without overwriting it. Copied objects remain available for retry.
Destination copy keys and source selection use the same canonical prefix; `.`
names the bucket root, just like an empty prefix. Namespace adoption checks provider
identity outside the service mutex with a bounded deadline. Concurrent source,
connection or credential changes invalidate that check before the source is written.

Live migration requires companion precondition protocol version 2. Cutover
atomically persists a migration receipt alongside the source. Every retry must
match the original local root, prefix, destination connection, bucket, prefix,
source namespace and table. A different target fails before copying or syncing.
The receipt survives companion restarts. Bucket-backed sources cut over by older
versions without a receipt cannot safely infer their original local store;
automatic mop-up refuses them. Upgrade the companion before the first cutover.

The destination connection must already exist in the repointed-against
profile (the live server's own, or the one seeded at `--state-dir`); this is
checked, and fails clearly, before anything is copied. An upload that lands
in the local store while this runs (or afterward) is simply left there; it is
never lost, and a later `migrate-local` run copies and publishes it even
after the source already points at the bucket.

**A reference issued before repointing stops working immediately, not
gradually.** The signed media URL a client already holds is bound to a hash
of the source's own connection and config; `PUT`-ing the new connection
changes that hash before a single byte is re-copied or any resync runs, so
the very next fetch of an old reference gets `410 Gone`. Local bytes are
never deleted -- only the reference stops resolving. Measured against a real
`serve` process and a real MinIO bucket while continuously re-querying the
catalog and fetching whatever reference it currently returned: delivery
degraded only in that one window, lasting well under a second between the
`PUT` and the resync publishing the bucket-backed row (5 of 151 fetch
attempts, in one run, got `410` there); it never went down because of a held
lock, and a client that always fetches the current reference (rather than
holding on to an old one) sees at most that one short gap. Fetching the exact
URL a client already has, right in that gap, fails with `410`, consistent
with the reference's own contract that an invalidated URL never serves
different bytes under the old signature.

**Repointing re-keys every document.** A provider switch changes the item
identity (`local://...` becomes `s3://...` or `gcs://...`), and the catalog's
document ID is a hash of that identity, not of the bytes. The resync
therefore deletes each old row and inserts a new one, even though the digest
is unchanged. Any derived data attached to the old row -- OCR text, chunks,
embeddings -- does not carry over and must be recomputed against the new
row. Only the original content's digest survives unrecomputed; search over
the migrated content still works once that recomputation finishes (plain
text and metadata are available immediately). `migrate-local` prints this
plainly before copying anything, including on a dry run.

A GCS destination uses `GoogleCredentialsForGCSWrite` (read/write scope)
rather than the read-only scope every other Google connection requests;
a credential that only has read-only storage access fails the upload itself
with a clear `HTTP 403` naming the missing scope, rather than failing
earlier with something more cryptic.

## Media and runtime source registration

Run the optional **source companion** when applications need runtime connections,
private media, or source sync from TypeScript, Python, or Rust. It shares the same
`Runner`, scoped merge, ownership journal, catalog, and deletion barriers as the
one-shot CLI. It has its own listener and operator credential; it does not add
routes to the Antfly database or require public inference.

Start with a profile containing a fixed database destination and optional initial
connections/sources. Credentials in that profile remain references:

```json
{
  "version": 1,
  "endpoint": "http://127.0.0.1:8080/db/v1",
  "data_plane_token_env": "ANTFLY_WRITE_KEY",
  "connections": {},
  "sources": []
}
```

Provision two **different**, random bootstrap secrets of at least 32 bytes in
`ANTFLY_SOURCE_ADMIN_TOKEN` and `ANTFLY_SOURCE_MEDIA_KEY`. Retain the media key and
state directory across restarts. Then run:

```sh
antfly-source serve --config sources.json --state-dir /var/lib/antfly-sources \
  --listen 127.0.0.1:8082 --public-url https://sources.example.com
```

Put the listener behind TLS when accessed remotely. Antfly must be able to reach
`public-url`, and its remote-content security policy must permit that host. Use
one companion per operator/credential trust boundary. Its admin token can manage
all configured sources; it is not an end-user or multi-tenant database token.
Never expose that token or the source administration API to a browser.

Applications can register and rotate credentials without changing process
environment variables or Antfly configuration. `PUT /v1/secrets/{key}` accepts
`{"value":"..."}` over the authenticated control API. Values are encrypted with
AES-GCM in a separate keystore under the private state directory, using a
domain-separated key derived from the bootstrap media key. There is no secret
read endpoint. Profiles, catalogs, media rows, and API responses retain only
references. Keep the bootstrap key separately from the state volume; changing it
requires migrating the keystore and republishing media capabilities.

A runtime S3 connection looks like:

```json
{
  "provider": "s3",
  "secret_refs": {
    "access_key_id": "photos.access",
    "secret_access_key": "photos.secret",
    "session_token": "photos.session"
  }
}
```

Omit `session_token` for permanent keys. S3-compatible services additionally use
`s3_endpoint` and the explicit `s3_insecure` development option. Google Drive uses
`provider: "google_workspace"`, `services: ["drive"]`, and
`secret_refs: {"credentials":"drive.oauth"}`. The referenced value is the same
service-account or authorized-user JSON accepted by the existing factory; OAuth
refresh remains in the Google client. Operator-managed `*_env` references still
work. Do not specify both an env and secret reference for the same credential.
An embedded application can supply `CredentialFactory` with its existing secret
manager instead of using the companion's keystore.

Register a media source with `PUT /v1/sources/{uuid}`:

```json
{
  "id": "11111111-1111-4111-8111-111111111111",
  "alias": "photos",
  "connection": "photos",
  "table": "files",
  "publication": "media",
  "config": {"bucket": "photos", "prefix": "originals/"},
  "permissions": {"public": true, "readers": []}
}
```

Drive media sources use `config: {"resource":"drive","folder_id":"..."}`.
Binary images, PDF, and audio are published without OCR/text extraction. Media
publication supports S3, GCS, and binary Drive objects with positive exact lengths.
Media does not inherit the text importer's 32 MiB limit. Workspace document
exports and unknown-length objects must use text publication. Initial verification
streams original bytes through SHA-256 with a 64 KiB buffer and a 30-minute
per-object deadline; it never buffers the complete object.

Text sources keep their version-1 canonical format. Media rows use version 2,
`kind: "media"`, empty `content`, and a `media` object matching the model-aware
chunk API's source-reference contract:

```json
{
  "type": "source",
  "uri": "https://sources.example.com/v1/media/<id>/<signature>",
  "mime_type": "image/jpeg",
  "version": "\"<strong-etag>\"",
  "version_kind": "strong_etag",
  "length": 12345
}
```

The canonical source URI, provider revision, permissions, and connection identity
remain in the canonical document. Enrichment `field` selectors read top-level
fields; use a nested-value template for this canonical envelope. For document
extraction use `template: "{{content.resource.uri}}"` with the
`document_extraction` asset producer; the gateway supplies MIME type and a strong
versioned URL. For URL-based media embeddings use
`template: "{{remoteMedia url=content.resource.uri}}"`. Model-aware chunking consumes the
`content.resource` source-reference object through a JSON-producing source template.
Do not configure the literal field name `content.resource.uri` as a field selector.
Text-only consumers such as `antfly-grep` should continue selecting text sources.
Publication readiness proves the canonical row is query-visible; downstream
embedding/extraction completion is observed through Antfly's normal index status.

A PDF extraction producer for the canonical media envelope uses:

```json
{
  "name": "document_units_v1",
  "kind": "asset",
  "template": "{{content.resource.uri}}",
  "content_type": "application/json",
  "producer_json": "{\"type\":\"document_extraction\",\"config\":{}}"
}
```

The opt-in `TestSourceMediaArtifactIntegration` covers real PDF fetching,
extraction, chunk generation, full-text querying, and credential-free scoped
purging with foreign-row preservation. Run it against a standalone server whose
remote-content policy permits the loopback fixture:

```sh
ANTFLY_SOURCE_TEST_ENDPOINT=http://127.0.0.1:PORT/db/v1 \
  GOWORK=off GOEXPERIMENT=simd go test -run TestSourceMediaArtifactIntegration
```

Media URLs are **private bearer capabilities**, not public share links. They
identify one source/configuration/object revision and contain no provider
credentials. Every read rechecks the live registry, selected inventory, current
provider authority, source version, and exact byte length. The companion serves
strong content ETags, HTTP conditional requests, `HEAD`, and single/multipart byte
ranges. HEAD performs a provider authorization/version check without downloading
the object (S3 uses a one-byte authorization probe). S3 and GCS keep one provider
response open per contiguous requested range while buffering at most 1 MiB.
Drive fetches bounded windows and passes before/after revision checks before
releasing bytes. There are four active requests and a two-minute request deadline.
Socket write deadlines also release slots when clients stop reading. When embedding
the handler, response-writer middleware must expose `Unwrap() http.ResponseWriter`
for `http.ResponseController` to reach the server connection.
Google streaming responses use the caller's operation deadline for the complete
transfer. The HTTP client's timeout (60 seconds by default) bounds response
headers and each blocked body read, without counting consumer backpressure or
limiting the total duration of a progressing transfer. One timer is reused per
response, with no allocation per read; initial hash verification allows 30 minutes.
The reusable Google `OpenSource` reader also streams original files directly into
application-owned staging. Callers must check both copy and close errors before
accepting staged bytes; native Workspace exports retain bounded acquisition.
A later provider failure truncates an already started response; clients must
require the advertised complete length. Saturation returns `503` and
`Retry-After`.

Provider clients and refreshed token sources are pooled per credential generation
with a 64-entry bound and at most 64 concurrent constructions; rotation/revocation
retires idle transports. Each caller can cancel its wait without cancelling a
shared token source. Shutdown cancels construction before joining workers. Custom
`http.DefaultTransport` wrappers are supported. Generations
track only the secrets referenced by a connection, so unrelated source,
connection, or secret changes do not revoke valid in-flight reads. Membership
checks use signed sorted compact inventories and a bounded 32 MiB LRU, without
reading acquisition journals on each request. Pending inventories are reconciled
once after restart. Signed descriptors survive restart and are pruned after
successful full syncs. Explicit purges remove descriptors and unreferenced digest
receipts before committing deletion completion, including purge-only jobs. Receipts
shared with other sources survive. Completion persists source removal in the live
profile before clearing the purge intent, so a failed earlier profile write cannot
resurrect a deleted source after restart. Concurrent credential revocations are
preserved. Failed runs keep pending/published generations for recovery.

Media publication computes `metadata.sha256` from verified original bytes.
Signed receipts bind that digest to provider URI, version, endpoint and exact
length. Subsequent syncs reuse receipts while rechecking current access and pinned
revision, including resumed Drive selections. Identical signed descriptors are
reused without replacement or fsync. The HTTP
ETag is this original-byte SHA-256. Provider ETags/checksums remain separate.
Catalog bindings advertise media document version 2; text consumers skip media
bindings and continue accepting version-1 text. Older pre-hash media descriptors
are revoked; resync to issue verified descriptors after upgrading.

### Shared control protocol and SDKs

All control requests use `Authorization: Bearer <source-admin-token>` and JSON.
The standalone contract lives in `specs/openapi/sources/api.yaml`; it is deliberately
separate from the joined database OpenAPI and its generated clients.

| Operation | Route |
|---|---|
| Inspect current profile | `GET /v1/profile` |
| Put/revoke a secret | `PUT/DELETE /v1/secrets/{key}` |
| Put/revoke a named connection | `PUT/DELETE /v1/connections/{name}` |
| Put/remove a source | `PUT/DELETE /v1/sources/{uuid}` |
| Start/resume sync | `POST /v1/sync` with `{"restart":false}` |
| Execute queued purges independently | `POST /v1/sync` with `{"purge_only":true}` |
| Inspect latest job after ambiguous submission | `GET /v1/jobs` |
| Poll/cancel a job | `GET/DELETE /v1/jobs/{id}` |

Sync returns `202` with a job receipt and `Location`; it continues if the HTTP
caller disconnects. One full-profile sync runs at a time. Concurrent sync or
reconfiguration returns `409`. Jobs end as `completed`, `failed`, `cancelled`, or
`interrupted`; only the latest receipt is retained. After a process restart, poll
the interrupted receipt and submit sync again to resume durable work. Use
`restart: true` only to discard a failed pinned selection, retaining ownership.
Do not blindly retry an ambiguous POST: inspect the current receipt first.

Job receipts identify their `operation` (`sync` or `purge`) and report
`purged_source_count`. Use `SourcesClient.runPurges()` (TypeScript) or
`run_purges()` (Python/Rust) to complete queued deletion without acquiring
other sources. Purge jobs survive unrelated credential revocation. A full sync
also drains committed purge intents before validating acquisition connections;
its receipt reports completed purges even if subsequent acquisition fails.
One runner-state lock covers purge and sync, preventing concurrent CLI/library
writers from racing database deletion or checkpoint removal.

Connection/secret revocation is allowed during sync, cancels that job, and blocks
future acquisition. Requests already acquiring bytes may finish. Changing connection transport/reference settings or removing/changing a source
also invalidates old capabilities; sync republishes their replacements. Rotating a
secret value behind an unchanged reference uses the new credential on future reads. Missing objects within
an active source are deleted only after an authoritative inventory and successful
publication. Removing an entire source follows the existing non-destructive
runner policy: the next sync retires its catalog binding, preserving owned rows
and state for recovery. To remove owned rows as well, DELETE with `?purge=true`,
then run a purge-only job or start sync. The durable purge queue retires the catalog entry and reconciles
only the source's exclusive key range and journaled legacy IDs. It needs no
provider credentials, survives interruption, and prevents reusing the source ID
until completion. Other sources and foreign rows are preserved. A source UUID's
destination table is immutable; use a new UUID to move destinations. Restored
connections can resume without losing their ledger.

Failed job receipts include a safe `failure` object with `source_id`, `phase`,
`code`, and `retryable`. The same fields are logged without raw provider messages,
URLs or credentials. Inspect this receipt before retrying; failed and interrupted
jobs retain their durable acquisition checkpoint.

The TypeScript SDK exports `SourcesClient`, Python exports `SourcesClient`, and
Rust exports `SourcesClient`/`SourceSyncJob`. Each targets the companion URL and
leaves acquisition/reconciliation in Go. For example, in a backend:

```ts
import { SourcesClient } from "@antfly/sdk";

const sources = new SourcesClient({
  baseUrl: "https://sources.example.com",
  token: process.env.ANTFLY_SOURCE_ADMIN_TOKEN!,
});
await sources.putSecret("photos.access", accessKeyFromYourCredentialFlow);
await sources.putSecret("photos.secret", secretKeyFromYourCredentialFlow);
await sources.putConnection("photos", {
  provider: "s3",
  secret_refs: { access_key_id: "photos.access", secret_access_key: "photos.secret" },
});
await sources.putSource(sourceDefinition);
const job = await sources.sync();
const status = await sources.job(job.id);
```

Python has equivalent snake-case methods (`put_secret`, `put_connection`,
`put_source`, `sync`, `job`, `current_job`, `cancel_job`) on a context-managed client.
Rust exposes the same methods asynchronously. Neither SDK retries sync implicitly
or forwards its operator token through HTTP redirects.

## Common source-management API

The standalone server mounts the shared contract at `/sources/v1`. Colony mounts
it beneath its organization/instance scope. The source of truth is
`specs/openapi/sources/management.yaml`; provider authorization/installation remains
a runtime-specific extension. Existing `/v1` standalone control routes remain
available for compatibility, including whole-profile synchronization.

The common API supports source GET/PUT/retirement and per-source runs, durable
history, idempotent submission, cancellation and explicit purge execution.
`POST /sources/{id}/runs` takes `{ "operation": "sync", "restart": false }` and an
`Idempotency-Key` of up to 128 ASCII letters/digits/underscores/hyphens. A key is
scoped to one source and retained with its receipts. Reusing it with different
input returns a conflict. Resolve lost responses using that key or run history;
do not blindly launch another mutation. Run state and publication completeness
are separate. Standalone execution is serialized.

The optional Go client lives in `sources/management`; TypeScript uses
`@antfly/sdk/sources`, Python `antfly.sources`, and Rust the `sources` feature/module.
Always specify the complete management base URL. The common client does not send
provider secrets to the Antfly database listener.

A source-scoped run retains the full profile/catalog context. It neither retires
sibling bindings nor prunes their media references. History survives restarts;
control pages bound disk scanning and can return an empty page with a next cursor
when sibling runs intervene. Source pages are also bounded to the common client’s
2 MiB response limit; `limit` is an upper bound on item count. Follow cursors until absent. Receipts/idempotency
bindings are retained with service state; deleting that state loses both history
and ingestion ownership and is not routine history cleanup.

Retirement retains rows by default. `DELETE /sources/{id}?purge=true` persists a
purge intent; a run with `operation: purge` executes only that source's deletion
without provider credentials. Other queued purges are left untouched.

Original media uses the Go/Colony proxy as the intended delivery path. Shared
`PublicationConnector`, `VerifyMediaDigest` and `ServeMedia` provide provider/version
acquisition, bounded original-byte hashing and HTTP range/conditional delivery.
The hosting adapter must validate its capability and current source/object/provider
authority before delivery. Standalone persists signed receipts and authority on its
single-writer filesystem; Colony uses protected hosted credentials and PostgreSQL.
This does not require native Drive/email/chat connectors or durable byte staging.
Proxy and provider availability remain dependencies for later processing/retries;
stale historical representations fail rather than silently returning newer bytes.

`MediaContentScope()` on the built-in media connectors identifies their provider
endpoint independently of credentials and source policy. Hosting runtimes can
cache verified byte digests under that namespace, immutable object version and
length; a digest receipt never replaces live access checks or grants delivery.

## Native source records and searchable artifacts

Source runs now publish the consumer-independent [v3 envelope](../../../zig/pkg/antfly-grep/schema/document-v3.json):

```json
{
  "schema_version": 3,
  "source": {"id":"catalog-source-id","provider":"gcs","item_id":"bucket/report.pdf","uri":"gs://bucket/report.pdf","path":"report.pdf","revision":"generation","indexed_at":1793740000},
  "content": {"mime_type":"application/pdf","resource":{"type":"source","uri":"https://sources.example/v1/media/PIN/SIGNATURE","mime_type":"application/pdf","version":"\"SHA256\"","version_kind":"strong_etag","length":1234}},
  "permissions": {"public":false,"readers":["group:researchers"]},
  "metadata": {}
}
```

The illustrative `SHA256` must be the actual lowercase 64-digit content digest.
Text/code/community bodies use `content.text` instead of `content.resource`.
`source.id` identifies the logical catalog source; the Antfly document key and
`source.item_id` identify its individual item. No persisted `grep` wrapper is
required. Catalog/document formats have separate version numbers.

Publication defaults to `native`: S3/GCS binaries are streamed, collaboration
attachments use their original bytes, and Docs/Slides are exported as PDF
with before/after provider revision checks and an acquired representation hash.
Drive identity remains the original document, not the export URL. The proxy
exports on demand and acquires each representation once per HTTP request. It
checks current access, provider revision, size, and the pinned representation
hash before exposing bytes, then serves request-local ranges without a durable
copy or staging bucket. Changed export bytes require resynchronization even when
the provider revision is unchanged. Collaboration delivery checks the current
parent object, attachment membership and pinned parent revision using metadata;
HEAD, permission audits and matching conditional responses acquire no attachment
bodies. Gmail checks the message history revision through its metadata API and
fetches remote attachments by their pinned attachment ID. Inline MIME parts use
a bounded full-message acquisition only when their bytes are requested. A GET
fetches only the selected attachment and checks its pinned byte digest. Deleted
or edited parents fail closed, and an unavailable sibling cannot block delivery.
Canonical parent-text reads likewise reload only text and provider metadata,
validate the pinned parent identity and revision, and acquire no attachment bodies.
Native parent revisions cover canonical text, provider metadata and attachment
descriptors. Attachment-derived Calendar invitation relationships remain in the
published metadata without changing that revision. Prior native acquisition
baselines are rediscovered once to establish this canonical revision.
Exports and collaboration attachments retain
bounded acquisition limits; large S3/GCS originals use streaming. Unsupported
whole-workbook export is rejected. Explicit `publication: text` retains the optional text-extractor path;
it is not required by native indexing. Standalone binary publication requires
`antfly-source serve` (or an embedded revision-enforcing media resolver).

References carry no provider credentials. Media signatures have no clock expiry;
the source service rechecks current selection, credentials, access and revision
before delivery. Keep the service URL and signing key durable, or use an embedded
resolver with equivalent guarantees. Do not replace it with a short-lived signed
provider URL.

Configure native enrichments with `template: "{{content.resource.uri}}"` and
ordinary artifact-backed indexes. A source's `streams` binding declares searchable
content; producer names and index names are application configuration:

```json
"streams": [
  {"name":"source","field":"content.text","purpose":"source","default":true,"boundary":"record"},
  {"name":"extracted","artifact":"units","index":"unit_text","field":"text","purpose":"extraction","default":true,"boundary":"unit","location_field":"provenance"},
  {"name":"summary","artifact":"summary","index":"summary_text","field":"text","purpose":"summary","generated":true,"default":false,"boundary":"record"}
]
```

Transcripts/captions use the same binding contract and native `asset` producers.
Acquisition visibility is distinct from artifact convergence. Grep checks native
index/source readiness; pending or failed producer work is an explicit incomplete
result. Updating stream/producer configuration does not change acquisition identity
or revoke the original byte reference. Scoped LinearMerge still reconciles only
the owning source. Current exposed API authorization must cover both originals
and derived members; catalog filters and local principals grant no access.

### Transition and local smoke

New synchronizations write version 3 records. Re-run a complete source sync to
replace old `grep` rows in its owned key range; keep the same source UUID and
catalog table. Finish or discard an interrupted old acquisition before changing
its publication mode. Existing manual grep tables can keep an explicit
`document_field` adapter. Native bindings use `fields` and `streams` instead.
Colony marks older catalog publications stale until the source is synchronized.
No automatic deletion of old Grepstein projection tables or buckets occurs.

Build the local server and grep in the worktree, then run:

```sh
(cd zig && zig build antfly grep -Doptimize=Debug -Dmetal=false -Dcuda=false -j2)
bash scripts/smoke_native_sources.sh
```

The smoke starts an isolated loopback database, acquires actual source/media
records, waits for native producers and indexes, and searches the native members
with the real grep executable. It covers text synchronization/deletion, PDF pages,
timed audio, image captions, a video audio route, summaries, incomplete extraction,
transient transcription failure, bounded cursor pagination, a 1,000-document
member batch, source-versus-artifact trigram isolation, path/type exclusions and
retained snapshot completion. Model HTTP replies
are deterministic fixtures; this validates the integration, not GPU/model quality.
Private-IP fetching is enabled only in the isolated smoke configuration.


Interrupted inventories from older companion versions are upgraded in place:
local, S3 and GCS relative keys are reconstructed from the pinned item IDs and
matching source configuration before publication. Namespace adoption therefore
preserves row IDs on resume without listing the provider again. Inconsistent
checkpoint identities fail before content publication.

## Versioned search streams and processing

See [Source search streams and native processing](../../../docs/design/source-search-streams.md)
for search_version=2, semantic-only media, complete query compatibility, external
vector admission, generic artifact discovery, standalone processing/search commands,
Colony integration and the model-free local fixture. Legacy TextStream declarations
remain supported; processing configuration does not change acquisition identity.

## Source knowledge

Optional `knowledge` version 1 discovers native extractor outputs, resolver-backed
entities and source-owned graphs alongside SearchStreams. See
[Source knowledge](../../../docs/design/source-knowledge.md) for native builders,
stable namespaces, curation references, temporal/current-state semantics, bounded
queries, the standalone fixture and Colony deployment controls. Knowledge is
fail-closed by default and requires an operator-enforced one-instance/one-audience
boundary; source JSON cannot enable that boundary.

Discovery-only edits reuse a completed publication only when its canonical
acquisition identity (including the row-key namespace) and published revision
match the local publication receipt. JSON object ordering does not change that
identity. The receipt is written after successful inventory and catalog
publication; interrupted publication or older state without a receipt requires a
normal sync before discovery can be refreshed. Refresh never extends the access
lease or relabels an old row-key generation.

For self-hosted GCS, a trusted profile may set `google_adc: true` on a
`gcs` connection to use Application Default Credentials with read-only
storage scope. Leave `credentials_env` and the credentials secret reference
unset for that connection.

For projected PDF pages, set `publication: "text"` on the GCS source and
`page_provenance: true` in its config.
Each object must have `original_pdf_uri`, `original_page`,
`extraction_revision`, and `content_sha256` custom metadata. The source verifies
the text digest, retains the projection location in metadata, and cites the
original PDF URI with its page fragment. Use a generation-pinned original URI
such as `gs://original-bucket/report.pdf?generation=42`. The cited generation
must remain available in the original bucket.
