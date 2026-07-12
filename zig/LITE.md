# Antfly Lite

Antfly Lite is Antfly's single-file storage engine and `.aflite` format. It can
be opened directly by an embedded application or selected by the full
standalone server. It feels like SQLite for Antfly without making `lite` a
deployment topology: `embedded` and `standalone` describe runtime ownership;
`lite`, `local`, and `object` describe durable storage.

The user-facing CLI surface is:

```sh
antfly lite <command>
```

The embedded library and the storage engine use the product name Antfly Lite.
The server topology remains Antfly Standalone.

## Goals

- Provide an embedded Antfly database with no server process and a full
  standalone server backed by the same file.
- Keep the first-use path simple: `antfly lite init app.aflite`, then local
  reads, writes, search, backup, restore, and health checks.
- Preserve Antfly's core feature model: documents, schemas, text search, vector
  search, sparse search, graph edges, enrichments, and retrieval-oriented query
  APIs.
- Make upgrade to normal Antfly explicit and reliable through portable backup
  and restore.
- Keep the embedded API stable enough for language bindings.
- Make `.aflite` the public database format, backed by a Lite-native
  single-file engine instead of exposing a temporary directory-backed user
  format.

## Non-Goals

- Antfly Lite is not a distributed database.
- Antfly Lite does not run Raft, shard placement, cluster metadata heartbeats,
  or multi-node balancing.
- Antfly Lite does not require local inference to be available.
- Antfly Lite uses the normal standalone `/db/v1`, SQL, metadata, inference,
  backup, and restore contracts; it does not duplicate them under a Lite API.
- Antfly Lite should not silently emulate distributed behavior in ways that make
  later promotion surprising.
- Antfly Lite should not include legacy fallback code for pre-release
  `.aflite`, directory-backed, or LSM-container experiments. Unknown versions
  and invalid headers should fail explicitly.

## Implemented Architecture

The implementation now consists of:

- `pkg/antfly-embedded` exposes a standalone embedded package.
- `pkg/antfly/src/embedded/db.zig` wraps the high-level DB surface.
- `pkg/antfly/src/embedded/api.zig` exposes JSON-oriented helpers for batch,
  lookup, scan, search, stats, indexes, enrichments, capabilities, and
  `runUntilIdle`.
- `storage/db/db.zig` already supports open modes such as writer,
  query-readonly, and status-only.
- `storage/lite/native.zig` owns the native revision-2 header, alternating checkpoint roots,
  page allocation, free map, crash recovery, integrity checks, stable snapshots,
  and atomic vacuum replacement. Document commits publish a namespace-head
  directory and per-namespace page links in the same checkpoint, so a cold
  table snapshot walks that table's history rather than the global document
  log. Namespace-head updates are append-only deltas backed by an in-memory
  materialized directory; a full directory snapshot is emitted every 256
  deltas. This makes the normal commit cost proportional to the namespaces
  touched by the transaction instead of every namespace in the database while
  bounding cold-open replay. The checkpoint links the directory delta and
  document pages atomically. Normal commits remain append-only; explicit vacuum
  reclaims superseded pages without putting a reachability walk on the write
  path. Each checkpoint also pins a copy-on-write ordered B+ tree mapping every
  logical document key to its newest document page. Initial loads and vacuum
  build packed trees as bounded streaming operations. Integrity checks validate
  every tree page, separator range, and checkpoint/free-map reachability, then
  prove that the index contains exactly the newest document page for every key
  in history. Missing, stale, duplicate, or cross-key document pointers and
  missing directory or namespace-link metadata are treated as corruption.
- `storage/lite/docstore.zig` provides ordered document transactions, pinned
  snapshots, replay lanes, and prefix-bounded logical namespaces. Point reads
  and ordered seeks traverse the checkpoint's disk-resident B+ tree in
  `O(log N)` pages. A cursor retains one decoded root-to-leaf path, its current
  key, and its current value, so sequential next/previous traversal is
  amortized `O(1)` and cold-scan memory remains bounded by tree height and the
  page cache rather than live-key count or document payload volume. Tombstones
  remain indexed until vacuum and are skipped during iteration. Write cursors
  merge a sorted, latest-write-wins overlay containing only that transaction's
  pending mutations; this provides read-your-writes without materializing the
  durable namespace. Pinned reads use concurrent positional I/O. A
  `std.Io.RwLock` allows normal append-only commits while readers pin roots and
  blocks vacuum before it can reclaim those roots.
- `storage/lite/index_storage.zig` stores Antfly index logical files in the
  native index catalog inside the same `.aflite` file.
- `storage/lite/backend.zig` caches one runtime per logical table/group and
  injects those runtimes through the standalone backend-runtime DB-open hook.
- Standalone metadata is stored in a reserved system namespace in the same
  file. Durable HTTP transaction sessions, including staged writes and
  savepoints, use a second reserved namespace so reopening or copying the
  `.aflite` file retains the complete database state. The existing data, query,
  transaction, inference, SQL, and `/db/v1` implementations are reused rather
  than forked.
- Durable transaction sessions are copy-on-write: the candidate record is
  committed to the Lite namespace before it replaces the in-memory session.
  Failed writes and fsyncs cannot expose unacknowledged staged operations.
  Standalone applies the bounded `transaction_sessions` TTL, count, encoded
  record size, and savepoint policy documented in `STORAGE.md`, preventing
  abandoned sessions from growing the `.aflite` file without limit.

Directory-backed and LSM-container profiles remain internal development and
conformance tools. They are not public `.aflite` formats and invalid or unknown
native headers do not fall back to them.

## Product Shape

Antfly Lite has embedded, CLI, and standalone-server surfaces.

### CLI

The CLI should live under `antfly lite`:

```sh
antfly lite init app.aflite
antfly lite status app.aflite
antfly lite batch app.aflite --file writes.json
antfly lite query app.aflite --file query.json
antfly lite schema set app.aflite --file schema.json
antfly lite schema get app.aflite
antfly lite index create app.aflite --file index.json
antfly lite enrichment create app.aflite --file enrichment.json
antfly lite run-until-idle app.aflite
antfly lite backup app.aflite --out app.afb
antfly lite restore app.afb --out app.aflite
antfly lite export app.aflite --out app.afb
antfly lite import app.aflite --from app.afb
antfly lite check app.aflite
antfly lite compact app.aflite
antfly lite vacuum app.aflite
antfly lite serve app.aflite --addr 127.0.0.1:8080 --config production.json
```

`antfly lite init` should be non-destructive: it creates a new `.aflite` file
and rejects an existing database path. Destructive replacement should stay on
explicit restore/import flows where the source and target are both known.
`antfly lite import <db.aflite> --from <backup.afb>` may import into an existing
empty Lite database. `antfly lite import <db.aflite> --from <source.aflite>`
must be treated as a physical snapshot replacement and require `--replace` when
the target already exists; it should not silently merge one live Lite database
into another.

`antfly lite status` should include a storage block that identifies the live
file format, the selected engine, the primary, replay, and index layouts, the
native format revision, page size, and active checkpoint sequence. That makes the
public native `.aflite` path observable and keeps internal bridge profiles from
being mistaken for the v1 contract.

For native `.aflite`, the public status contract should report
`primary_layout: native_document_pages`,
`replay_layout: native_replay_lanes_in_document_catalog`, and
`index_layout: native_index_catalog_pages`. Any LSM adapter used while the
native index engine is being completed is an implementation detail and must not
appear as the public index layout for native Lite files.

`antfly lite serve` is an artifact-oriented convenience constructor for the
full standalone runtime. It serves the normal `/db/v1` API and is equivalent to
`antfly standalone --storage-engine lite --storage-path <file>`. Lite does not
define a storage-specific HTTP namespace. The convenience command binds only
to loopback hosts. It forwards the complete standalone option surface,
including configuration, authentication, TLS, secrets, inference, and
connections. It owns `--storage-engine`, `--storage-path`, `--host`, and
`--port`; conflicting duplicates fail closed.

Network backup and restore always use named, capability-scoped `external_io`
connections. This includes `file://`, whose URI path is logical and resolved
beneath the filesystem connection's configured root. S3 and GCS connections
have distinct credential shapes and bucket/prefix scopes. Offline Lite
artifact commands may still use explicit local paths or ambient cloud
credentials because they run with the invoking user's filesystem authority.

Restore through `/db/v1` is a durable asynchronous job, not request-duration
work. Admission requires the standalone process's shared asynchronous
backend-runtime lane and its engine-owned durable job store; an unavailable
worker or store returns `503` before any job is created. The accepted response
contains a job ID; status and cooperative
cancellation use `/db/v1/restore/jobs/{job_id}`. Idempotency keys make retries
safe; requests without a key create independent jobs. Restore state lives inside
the `.aflite` file, and completed table boundaries are durably checkpointed and
not repeated after restart. An ambiguous publication/checkpoint interruption
fails closed for operator inspection. Destructive overwrite is not exposed until
table generations can be staged and atomically swapped. Terminal state and
explicit idempotency keys are retained for seven days in a history bounded by
10,000 jobs and 64 KiB per encoded job. Cancellation is checked at safe table
publication boundaries. A standalone process executes at most two restore jobs
concurrently; the remainder stay durably queued inside the `.aflite` file.
`antfly restore` always prints the accepted or terminal job document. Use
`--idempotency-key` for retry-safe submission and `--wait` (optionally
`--wait-timeout <seconds>`) for a terminal exit status. Failed and cancelled
terminal jobs exit nonzero.

An artifact first created through embedded commands has one root database. On
its first standalone start, Antfly atomically adopts that root as the
standalone `default` table: it persists a stable `group-<id>/table-db` alias in
the file before publishing table metadata. The alias deliberately omits the
host data-directory prefix, so moving or restoring the `.aflite` file cannot
orphan its documents or indexes. Embedded root databases use the deterministic
document-identity namespace of that future `default` table from creation, so
adoption is O(1) rather than rewriting every live document; an identity mismatch
fails closed. Subsequent standalone tables use isolated
namespaces in the same artifact. This makes `lite batch` followed by `lite
serve` a genuine interoperability path rather than two unrelated databases.
After that adoption, embedded data commands continue to address the `default`
table through the persisted alias. A file created directly by standalone has
no unambiguous root table, so root-oriented `lite batch`, query, schema, index,
enrichment, import, promote, and compact operations fail closed and direct the
user to `lite serve` plus `/db/v1`. Artifact `status` and the physical `check`,
`vacuum`, and `snapshot` operations remain available.

The equivalent tagged configuration is:

```json
{
  "storage": {
    "engine": "lite",
    "lite": { "path": "./app.aflite", "fsync": true }
  }
}
```

Storage configuration is a tagged union: `engine` is required and exactly the
matching `lite`, `local`, or `object` member is allowed. Lite rejects Raft,
replication, horizontal-sharding, and serverless settings.

### HTTP And Administrative Operations

A standalone process backed by Lite serves the same `/db/v1` API as
directory-backed standalone. `GET /db/v1/status` includes
a safe storage summary with the engine, format, fsync policy, and typed
maintenance capabilities; it does not expose the database path or credentials.

Portable backup and restore remain normal `/db/v1` operations. A physical
`.aflite` copy is a stable snapshot, not the portable archival contract; `.afb`
remains the cross-engine backup format.

Once an artifact has been opened by standalone, the offline `antfly lite
backup` command refuses to emit a misleading root-only archive. Use the
authenticated `/db/v1` backup operation for a portable all-table archive, or
`antfly lite snapshot` for a complete physical copy of the artifact. An
offline physical snapshot includes metadata, every table namespace, indexes,
and durable transaction sessions.

Coordinated maintenance is an authenticated, storage-neutral admin surface:

```text
POST /admin/v1/maintenance/check
POST /admin/v1/maintenance/compact
POST /admin/v1/maintenance/vacuum
GET  /admin/v1/maintenance/jobs/{job_id}
DELETE /admin/v1/maintenance/jobs/{job_id}
```

Normal API authentication and admin RBAC protect these routes when enabled.
For an otherwise unauthenticated standalone server, configure a dedicated
token with `--admin-token-env <ENV_NAME>` and send it using `Authorization:
Bearer ...`; without either mechanism the admin surface fails closed.

POST requests return `202` and a job document. `Idempotency-Key` safely returns
the original job on retries for at least 24 hours within the current server
process. Job IDs are opaque, non-sequential 63-bit values and callers reconcile
storage state after restart before retrying. The bounded history rejects new
work rather than dropping an unexpired key. Jobs execute on the shared
`std.Io` backend-runtime lane; coordinator shutdown fences its owner, requests
cooperative cancellation, and drains outstanding work before releasing the
Lite handle. `DELETE` requests cooperative cancellation; native maintenance
checks the token at safe page and record boundaries, including during shutdown.
Only one maintenance job runs at a time; a
conflicting request returns `409`, and an engine that does not support an
operation returns `422`. Completed jobs are retained in a bounded in-memory
history. Lite reports `online: false`: check, compaction, and vacuum acquire the
exclusive maintenance gate. Readiness becomes false and new database requests
receive `503` while admin status and cancellation remain available. This avoids
unbounded request queues and does not call a stop-the-world rewrite "online".
Checkpoint inspection, index writes, document commits, compaction, and vacuum
share the Lite store mutex and FIFO writer admission gate, so blocked writers
sleep without polling and resume in arrival order. Maintenance cannot
race checkpoint publication or file replacement. Vacuum builds a temporary,
disk-backed LSM live-key index for one logical record class at a time. The
newest record wins, tombstones suppress older values, and a bounded mutable
batch is flushed to sorted runs. It then streams the ordered live references,
values, and replacement pages, so heap use does not scale with the number of
distinct keys. Temporary index directories are removed on success and error.
This also avoids a
second whole-database value snapshot and a whole-file output image in memory;
the replacement is fsynced, atomically renamed, and adopted through its
already-open read/write handle before the parent directory is fsynced. A
post-rename sync error therefore cannot leave the process writing an unlinked
old inode.

All normal Lite opens require working advisory file locks. The writer lease,
reader snapshot lock, stable-snapshot source lock, and vacuum/rewrite lock fail
closed with `FileLocksUnsupported`; Antfly never retries without a lock.
Filesystems and CSI drivers without advisory-lock semantics are unsupported for
writable Lite deployments.

Reinitializing an existing artifact is an atomic generation swap, not an
in-place truncate. Readers already holding the old inode finish against their
pinned generation; readers opened after the rename see the new database.

The Kubernetes operator exposes the same topology/storage separation through
`spec.mode: Standalone` plus `spec.storage.engine: lite`. The optional
`spec.storage.liteFileName` selects a safe `.aflite` basename on the standalone
PVC; the operator owns the absolute mount path and rejects competing raw
`spec.config.storage` values.

The standalone listener holds an advisory lease for its host/port while using
restart-safe address reuse. Cross-thread shutdown only publishes an atomic stop
request and wakes the accept loop; listener and connection teardown stay on the
server thread. Bind/listen failure reaches the owning runtime before readiness,
so a failed listener cannot leave a headless process holding the `.aflite`
writer lock. Standalone metadata updates retain a copy-on-write checkpoint
until the catalog commit is durable, so failed persistence restores the exact
prior in-memory state.

The `antfly lite check`, `compact`, and `vacuum` commands remain useful for
offline files and automation that does not run a server.

### Embedded Library

The library API should be small and boring:

- open/close
- batch writes/deletes
- lookup
- scan
- search/query
- add/drop/list index
- add/drop/list enrichment
- set/get schema
- run maintenance until idle
- status/stats
- backup/export
- restore/import
- integrity check

`libantfly` should be the long-term stable C ABI boundary. The storage-neutral
open surface, ABI evolution rules, and read-only backend contract live in
[`CAPI.md`](CAPI.md). Antfly Lite should not have a separate
independently-versioned ABI; `.aflite` is a storage/open mode and the
`antfly_lite_*` names are convenience entrypoints in the same `libantfly` ABI.

The C ABI should expose a single Lite status JSON call that mirrors
`antfly lite status`: storage identity, DB stats, pending work, and capability
flags. Bindings should not have to reconstruct Lite status by combining several
lower-level calls differently in each language.

The C ABI should also expose a path-level Lite check call, not only a
handle-level check. Bindings need to inspect invalid, truncated, or corrupted
`.aflite` files and receive the same JSON integrity report as `antfly lite
check` without first opening the database successfully.

The embedded Zig API should expose the same status shape for Lite handles, with
the storage identity available as a typed value on the lower-level DB wrapper.
It should also expose a path-level Lite integrity check so Zig users can inspect
invalid `.aflite` files without first opening a handle.

### File Format

Antfly Lite uses `.aflite` as the live database format. Users should
not need to understand a temporary directory-backed layout.

The single-file database should be implemented as a Lite-native backend, not as
a long-term LSM directory packed into one file. The native backend should keep
Antfly's document, index, enrichment, query, backup, and restore semantics, but
map them onto file-local pages or segments directly. That avoids the extra I/O
and coordination introduced by emulating logical files, manifests, renames, and
asynchronous cleanup inside another single-file container.

The v1 production target should therefore be:

```text
Antfly DB and indexes
  -> Lite-native storage engine
    -> .aflite single-file database
```

An LSM-backed `.aflite` container can still be useful as an incremental
implementation bridge because it exercises the existing storage abstraction and
lets the CLI, C ABI, backup, restore, portable-interoperability, and
conformance tests land early. It should not define the long-term v1
architecture. If benchmarks show meaningful I/O and coordination savings from
the Lite-native path, the native backend is the v1 target, not a v2 candidate.

Directory-backed LSM storage should remain available as an internal development,
debug, and conformance-test profile. LSM-container storage should be treated the
same way. Neither should be the public Lite v1 contract.

### Compatibility Policy

Because this is new, unreleased code, native revision 2 does not carry a legacy fallback,
pre-release importer, v0 directory reader, silent LSM-container upgrade path, or
prototype-to-v1 auto-migrator. Prototype files can be recreated from tests or
explicit exports while the format is still pre-release. `.aflite` readers should
accept the documented revision-2 format and reject unknown versions loudly. Recovery
from an older complete checkpoint root inside the same file is crash
recovery, not legacy compatibility; a file with no complete checkpoint should
fail with an explicit integrity error. Compatibility branches should only be
added after a format has shipped and users can reasonably have files that need
preservation.

The implementation consequence is that the production Lite open path should be
small and direct: parse the current header, validate its checkpoint and ordered
index root, recover within the same format if needed, and otherwise return an explicit error. It should not
carry readers for discarded prototype layouts, and tests should assert rejection
of invalid headers, unsupported versions, and bridge-profile files opened through
the default `.aflite` path.

Internal bridge profiles are explicit developer/test engine selections, not
compatibility modes. The default `auto` path should never inspect a failed
native open and then silently retry a bridge or prototype layout.

The extension meanings should stay distinct:

- `.aflite` is a live Antfly Lite single-file database.
- `.afb` is the portable Antfly backup archive.
- `~/.antfly/lite/` may be used for CLI registry data, caches, temporary
  workspaces, and internal development databases, but not as the public database
  format.

## Storage Design

### Lite-Native Single-File Backend

The `.aflite` single-file format should be a database file with a native layout
for embedded Antfly data:

- database header and format version
- checkpoint roots
- catalog pages
- document key/value pages or segments
- copy-on-write ordered document-index pages
- text index files
- dense vector/HBC posting files
- sparse posting files
- graph reverse indexes
- catalog records
- enrichment definitions and state
- free-space map
- integrity metadata
- optional append journal or commit log

The backend should provide Antfly database operations directly:

- point lookup
- ordered scan
- compare-and-set or transaction commit
- index definition reads and writes
- posting-list reads and writes
- vector/HBC reads and writes
- graph edge reads and writes
- enrichment queue/state reads and writes
- snapshot creation for readers, backup, and restore
- page or segment allocation and reclamation

Important correctness rules:

- Atomic publish must survive process crash.
- Readers must not observe a partially committed transaction.
- The backend must support integrity checking.
- The backend must support online backup or a consistent checkpoint.
- Vacuum/compaction should be explicit.

LMDB may still be useful for an LMDB profile, but it should not be the only
Antfly Lite story. LMDB gives an mmap data file plus a lock file and fits a
simple KV shape well. Antfly's richer index stack already has its own LSM and
posting-file needs, so the native backend gives us a more general product while
removing the I/O cost of pretending those structures are separate filesystem
objects.

### Internal LSM Profiles

The durable LSM directory and LSM-container layouts should remain useful
internally:

- exercising existing LSM conformance tests
- comparing native `.aflite` behavior against the current filesystem storage
- debugging corruption or recovery issues
- measuring native backend performance against the bridge implementation

These profiles should be hidden behind developer flags or build steps. They
should not appear in the normal user docs as Lite database formats.

## Concurrency Model

Antfly Lite should match the familiar embedded database model:

- One writer at a time.
- Multiple concurrent readers where backend snapshots support it.
- Cross-process locking for the database path.
- Read-only opens for tooling and inspection.
- Clear `ANTFLY_BUSY` errors when another process or in-process write handle
  owns the writer lock.

The CLI should expose this plainly:

```sh
antfly lite status app.aflite
antfly lite query app.aflite --readonly --file query.json
```

The embedded API should expose open profiles:

- writer
- readonly query
- status only
- hosted/manual maintenance

## Upgrade To Normal Antfly

Upgrade should be backup/restore first.

The durable, user-facing archival contract is the portable Antfly backup format,
not a physical copy of the Lite storage engine. A Lite database should export
the same portable logical content that a normal Antfly backend can restore:

- documents
- schemas
- index definitions
- enrichment definitions
- reusable enrichment artifacts where portable
- dense embeddings
- sparse embeddings
- graph edges
- resolver/promotion artifacts where portable
- table and shard metadata in a single-shard layout

The flow:

```sh
antfly lite backup app.aflite --out app.afb
antfly restore --format portable --input app.afb --table docs \
  --location s3://archive/promotions/app --connection promotion-reader --wait
```

or:

```sh
antfly lite promote app.aflite --target http://cluster:8080 --table docs \
  --connection promotion-reader --location s3://archive/promotions/app
```

`promote` orchestrates portable backup upload plus normal restore. The named
connection is required by the target API and scopes its read access to the
chosen location; the CLI uses the invoking user's separate local or ambient
write authority to stage/upload the portable artifact. Multiple named target
connections may select different buckets, accounts, prefixes, and reader
roles. Promotion waits for terminal success by default, prints the restore job,
and exits nonzero on failure. `--no-wait` returns the accepted job instead. It
should not invent a separate migration protocol until backup/restore proves too
slow for large databases.

The no-wait path prints the response returned by the successful submission; it
does not issue an immediate second GET that could turn a transient routing
failure into an ambiguous CLI error after the job was already accepted.

`promote` and network `restore --input` require an explicit `--location`.
Client-local defaults are unsafe because a remote server resolves filesystem
connections in its own namespace. The location must be writable by the CLI and
readable through the target's named connection; `file://` is appropriate only
for genuinely shared storage, while `s3://` and `gs://` are the normal remote
choices.

Normal Antfly should also be able to restore directly from a `.aflite` live
database file:

```sh
antfly restore --input app.aflite --table docs \
  --location gs://migration-staging/app --connection migration-reader --wait
```

That direct path should not make `.aflite` the backup format. It should open
the `.aflite` database read-only, stream portable logical restore records, and
restore them into normal Antfly. `.afb` remains the stable cross-backend,
archival, streamable backup format. `.aflite` remains a live embedded database.

### Upgrade Semantics

Lite is a single-node, single-shard source. Restore into normal Antfly should:

- create the target table if requested
- restore source documents
- restore schemas and index/enrichment definitions
- rebuild or import indexes according to restore policy
- map the Lite single shard into the cluster's placement model
- start normal Antfly background workers after restore
- report replay/enrichment/index readiness through normal status APIs

Indexes should default to logical rebuild on restore. Physical index restore can
be an optimization later when the source and target backend formats match.

### Downgrade / Extract

The reverse path should also work:

```sh
antfly backup --format portable --table docs --backup-id docs \
  --connection archive-writer --location s3://archive/exports/docs
# Fetch docs.afb from the configured location with the object-store tooling.
antfly lite restore docs.afb --out docs.aflite
```

Network backup locations are server-owned and authorized through named
connections, so `antfly backup` intentionally does not pretend a client-local
`--out` path is visible to the server. For a local Lite database, create the
artifact directly with `antfly lite backup source.aflite --out docs.afb`.

This makes Lite useful for local development, debugging production data slices,
offline demos, and customer support bundles.

## Enrichments And Inference

Antfly Lite should preserve the enrichment model, but inference execution needs
clear modes. Enrichments are part of Antfly's feature set; inference is an
execution dependency that may be local, remote, caller-supplied, or disabled.

### Enrichment Modes

Supported modes:

1. Caller-supplied artifacts.
2. Remote inference provider.
3. Local embedded inference.
4. Manual maintenance.
5. Disabled/deferred enrichment.

#### Caller-Supplied Artifacts

This is the most reliable default. Applications can write documents with
precomputed `_embeddings`, extracted assets, chunk artifacts, graph edges, or
other enrichment outputs. Lite persists and indexes them without needing a model
runtime.

This mode should be the default for small applications and language bindings.

#### Remote Inference Provider

Lite can call a configured Antfly inference service, OpenAI-compatible endpoint,
or other provider through the existing enrichment/provider interfaces.

The CLI should support:

```sh
antfly lite enrichment create app.aflite --file embedding-index.json
antfly lite run-until-idle app.aflite
```

Configuration must be explicit. A local file opened by a library should not
unexpectedly start sending data to a network provider.

#### Local Embedded Inference

Local inference should be optional packaging:

- `antfly lite` base build: database, search, vector indexes, no heavy model
  runtime requirement.
- `antfly lite` full build: bundled or dynamically available inference runtime.
- Application embedding: caller links the inference runtime if wanted.

Local inference is important for demos and offline use, but it should not be
required for the core embedded database.

#### Manual Maintenance

Hosted/manual mode is important for environments such as WASM, mobile, plugins,
or apps that want deterministic control of background work. In this mode, writes
record replay/enrichment debt and the application drives progress:

```zig
try db.runUntilIdle();
```

The CLI equivalent is:

```sh
antfly lite run-until-idle app.aflite
```

#### Disabled Or Deferred Enrichment

Users must be able to open a Lite database without configured inference. In that
case:

- writes still succeed if enrichment outputs are not required synchronously
- pending work is visible in status
- capabilities/status reports `inference_mode`,
  `no_inference_configured_ok`, and whether caller-supplied artifacts, remote
  providers, or a local inference runtime are available
- queries that depend on missing index material return clear readiness/status
  information
- backup includes pending definitions and source documents
- restore into a normal Antfly deployment can resume enrichment

## Feature Coverage

Antfly Lite should aim for feature parity at the API level where the feature is
single-node and local.

### Should Work In Lite

- document writes/deletes
- lookup and scan
- schemas
- text search
- dense vector search
- sparse vector search
- hybrid search
- graph edges and graph query where local-only
- generated enrichments
- caller-supplied embeddings/assets
- local or remote inference-backed enrichment
- TTL cleanup
- local transactions/OCC where supported by the DB layer
- backup/restore/export/import
- integrity check
- compaction/vacuum
- read-only inspection

### Should Be Explicitly Unsupported Or Different

- distributed shard ownership
- Raft replication
- cluster placement
- cross-node joins
- remote shard fanout
- distributed transaction coordination
- server-side autoscaling
- multi-replica or horizontally scaled Kubernetes operator deployments
- cluster heartbeat/status aggregation
- S3/object-storage native serving as the primary Lite file

Some of these can still be simulated for testing, but they should not be
presented as production Lite capabilities. Lite status and capabilities should
advertise these distributed-only features as explicit `false` values so
bindings do not have to infer cluster semantics from missing fields.

## CLI Details

Suggested command groups:

```text
antfly lite init
antfly lite info
antfly lite status
antfly lite check
antfly lite batch
antfly lite lookup
antfly lite scan
antfly lite query
antfly lite index list
antfly lite index create
antfly lite index drop
antfly lite enrichment list
antfly lite enrichment create
antfly lite enrichment drop
antfly lite schema get
antfly lite schema set
antfly lite run-until-idle
antfly lite compact
antfly lite vacuum
antfly lite backup
antfly lite restore
antfly lite promote
antfly lite serve
```

The CLI should accept JSON request files that match the public API contracts.
This keeps Lite compatible with normal Antfly examples, tests, and SDKs.

## Packaging

Packages:

- `antfly` CLI with `antfly lite` subcommands.
- `antfly-embedded` Zig package.
- `libantfly` C ABI artifact, with `.aflite` exposed as an embedded storage
  profile rather than a separate Lite-only ABI.
- Language bindings generated or hand-written over the C ABI.
- Optional full package with embedded inference runtime.

Build profiles:

- `lite-core`: embedded database, indexes, and maintenance CLI, with no
  heavyweight inference runtime.
- `lite-full`: embedded database plus local inference runtime.
- `lite-wasm`: hosted/manual maintenance profile.
- `lite-dev`: debug/status tooling and compatibility experiments.

## Testing

Minimum test matrix:

- open/close/reopen durability
- crash during write
- crash during index update
- crash during commit/checkpoint publish
- reader/writer concurrency
- read-only open while writer exists
- online backup or snapshot while a write transaction is open
- backup from Lite, restore into normal Antfly
- backup from normal Antfly, restore into Lite
- direct restore from `.aflite` into normal Antfly through a portable restore
  stream
- enrichment disabled, then resumed
- caller-supplied embeddings search
- remote inference-backed enrichment
- local inference-backed enrichment where available
- integrity check detects truncated or corrupted database pages, segments, or
  journal data

The most important compatibility test is a round trip:

```text
Lite -> portable backup -> normal Antfly -> portable backup -> Lite
```

The restored documents, schemas, definitions, embeddings, graph edges, and
query-visible results should match within documented index rebuild semantics.

## Implementation Status And Remaining Work

### Complete: Lite-Native Single-File Backend

- Implemented a Lite-native storage backend for embedded and standalone Antfly.
- Add database header, catalog roots, page or segment allocator, free-space map,
  copy-on-write ordered document index, commit/checkpoint publish, crash
  recovery, integrity checks, and streaming vacuum rebuild.
- Preserve Antfly's document ordering, range scans, index definitions,
  enrichment state, vector/HBC artifacts, sparse artifacts, graph artifacts,
  backup, and restore semantics.
- Add `.aflite` as the live single-file database format.
- Run existing DB conformance tests against the native `.aflite` backend.
- Keep filesystem-backed LSM and LSM-container profiles as developer/test-only
  bridge paths.
- Do not add legacy fallback code for pre-release Lite layouts, v0 directories,
  or LSM-container prototypes; reject unknown versions and invalid headers with
  explicit errors while preserving same-format checkpoint recovery.
- Add negative open tests proving that the default `.aflite` path does not fall
  back to bridge profiles or prototype readers.

### Complete: Name, CLI, And Standalone Composition

- Added the `antfly lite` command group and embedded database operations.
- Added full standalone composition through `--storage-engine lite` and
  `--storage-path`, with `lite serve` as an equivalent constructor.
- Added multi-table key/index namespaces and same-file metadata persistence.
- Keep `~/.antfly/lite/` for CLI registry data, caches, temporary workspaces,
  and internal developer databases only.

### Complete: Portable Upgrade Path

- Add `antfly lite backup`.
- Add `antfly lite restore`.
- Treat `antfly lite export` as an alias for backup and `antfly lite import`
  as the inverse restore shape.
- Add `antfly lite promote` as a wrapper around portable backup and normal
  restore.
- Add normal Antfly restore support for `.aflite` input by opening it read-only
  and producing the same portable logical restore stream as `.afb`. The normal
  CLI shape should be:

  ```sh
  antfly restore --input app.aflite --table docs \
    --location s3://migration-staging/app --connection migration-reader
  ```

- Extend portable backup coverage for schema, index definitions, enrichment
  definitions, and portable artifacts that are not yet included.

### Ongoing: Embedded API Hardening

- Define stable `libantfly` C ABI.
- Add ownership/error/result conventions.
- Expose stable error-code names and descriptions for language bindings.
- Provide a buffer free-and-zero helper for generated bindings while retaining
  the raw pointer/length free function.
- Add Go as the first post-Zig/C binding in `go/pkg/antflylite`, backed by the
  stable C ABI and gated C-library smoke tests.
- Freeze the Lite open options and capabilities response.

### Ongoing: Enrichment And Inference Profiles

- Add explicit inference modes.
- Make "no inference configured" a clean status, not an error-prone partial
  setup.
- Expose inference profile fields in Lite status and capabilities so embedded
  users and bindings can branch without probing errors.
- Support caller-supplied artifacts as the default happy path.
- Support remote inference providers.
- Support optional local inference builds.

### Ongoing: Product Polish

- Add docs and examples.
- Add app templates for common embedded use cases.
- Add migration guides.
- Add package publishing for CLI and language bindings.
- Persist any remaining standalone operational catalogs that are durable
  database state in reserved `.aflite` namespaces.
- Add distributed qualification for portable restore and maintenance-job
  automation without changing the storage-neutral API paths.

## Open Questions

- Which enrichment artifacts are portable enough to backup/restore directly,
  and which should always be rebuilt?
- What is the minimum local inference package that is small enough for Lite
  users but useful enough for demos?

## Recommendation

Ship Antfly Lite as `.aflite`, not as a public directory-backed format. This
keeps the product mental model simple: a Lite database is a file, and a portable
backup is an `.afb` archive.

This moves more work into v1 because the Lite-native storage engine must exist
before the public Lite launch. That is the right tradeoff: it keeps the UX clean
and avoids shipping a synthetic LSM container whose extra logical-file churn,
vacuum pressure, and coordination become the public architecture.

The naming recommendation is:

- Live single-file Lite database: `*.aflite`
- Portable backup archive: `*.afb`
- CLI/internal workspace: `~/.antfly/lite/`
