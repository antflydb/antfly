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
- `storage/lite/native.zig` owns the native revision-3 header, alternating checkpoint roots,
  page allocation, free map, crash recovery, integrity checks, stable snapshots,
  and atomic vacuum replacement. Document commits publish a namespace-head
  directory and per-namespace page links in the same checkpoint for mutation
  and integrity bookkeeping. Materialized document snapshots seek the pinned
  live-key index, group record references by physical page, and reuse a record
  reader, so reads scale with live pages rather than overwritten history or
  key-to-page disorder. Output keys are allocated once through the caller's
  allocator and transferred from the cursor; results remain in key order. Namespace heads use a copy-on-write catalog B+ tree, so updates and
  cold writes touch only the requested namespaces and their tree paths. Batches
  collect distinct namespaces, share a sorted tree traversal to resolve their
  heads, and group record references by physical page before decoding them.
  Tracking memory scales with distinct namespaces rather than document count;
  one-namespace batches retain the point-lookup fast path. Tiny
  directories (at most 32 namespaces fitting one page) retain the existing
  inline snapshot encoding without extra tree pages. Their bounded cache retains
  namespace keys across mutations, reserving new ownership before checkpoint
  publication and updating heads without allocation afterward. Promotion and
  rollback invalidate the cache. Larger legacy snapshot/
  delta directories migrate atomically on their next document mutation; new
  large directories and vacuum output build the index directly. Old pinned
  checkpoints retain their original layout. Both layouts use existing v3 page
  kinds; this binary reads packed and unpacked v3 files without an offline
  conversion. Older binaries that lack indexed namespace-directory support
  may reject namespace operations on promoted files. The checkpoint publishes
  namespace heads and document pages atomically. Normal commits remain append-only; explicit vacuum
  reclaims superseded pages without putting a reachability walk on the write
  path. Each checkpoint also pins a copy-on-write ordered B+ tree mapping every
  live logical document key to its newest document page. Deletes remove keys
  through copy-on-write merging and redistribution; tombstones remain in history
  and older pinned index roots remain readable. Initial loads and vacuum
  build packed trees with one unfinished node per level. The builder retains
  encoded record references for long keys and materializes only the last input
  key for order validation. Counting compact pages uses the same builder.
  Integrity checks validate
  every tree page, separator range, and checkpoint/free-map reachability, then
  prove that every live key points to its newest document page. Deleted keys
  may be absent or point to their latest tombstone in existing revision-3 files.
  Missing live keys, stale, duplicate, or cross-key document pointers and
  missing directory or namespace-link metadata are treated as corruption.
- `storage/lite/docstore.zig` provides ordered document transactions, pinned
  snapshots, replay lanes, and prefix-bounded logical namespaces. Point reads
  and ordered seeks traverse the checkpoint's disk-resident B+ tree in
  `O(log N)` pages. Warm point reads binary-search validated immutable page
  views instead of rescanning every slot. Encoded bytes and decoded offsets
  share the page cache's byte budget and CLOCK eviction; active readers pin
  evicted views until release, and their memory remains accounted. Page reuse,
  rollback, and vacuum invalidate residency, and integrity checks bypass views
  as well as encoded pages. Overflow keys use per-reader reusable scratch.
  A cursor retains one decoded root-to-leaf path, its current
  key, and its current value, so sequential next/previous traversal is
  amortized `O(1)` and cold-scan memory remains bounded by tree height and the
  page cache rather than live-key count or document payload volume. Overflow
  keys remain encoded until a comparison or returned entry needs them; resolved
  comparison keys are released immediately. Full integrity checks still resolve
  every key and validate ordering and references. Legacy
  indexed tombstones are skipped during iteration; new deletes prune the active
  index immediately. Key-only cursors share snapshot, prefix, seek, and overlay
  behavior, validating record metadata without loading external values. Replay
  truncation uses these cursors in transactions limited to 512 keys or a 256 KiB
  prefixed-key target (a single key may exceed that target). Each chunk reserves
  the writer from scan through commit, then releases it so other writers can
  proceed. The cutoff is exclusive. Errors propagate; earlier completed chunks
  remain committed and retries safely process the remaining entries. Replay
  readers share one traversal that treats only not-found as end-of-lane; allocation,
  I/O, corruption, malformed-key, and callback errors propagate. Materialized replay
  results reserve list capacity before taking payload ownership and release all
  collected payloads on failure. Write cursors
  merge a sorted, latest-write-wins overlay containing only that transaction's
  pending mutations; this provides read-your-writes without materializing the
  durable namespace. Pinned reads use concurrent positional I/O and retain
  their file generation across vacuum. Short index reads use a generation
  lock during publication; old document readers continue on their original inode.
- `storage/lite/index_storage.zig` stores Antfly index logical files in the
  native index catalog inside the same `.aflite` file.
  Each catalog checkpoint owns an immutable descriptor containing its history
  root and a copy-on-write B+ tree mapping keys to their latest record pages.
  Point reads and misses use bounded tree searches, including after reopening
  and at older pinned checkpoints. Empty keys are indexed, and keys longer
  than 512 bytes use references to their immutable record pages in both leaves
  and separators. This bounds encoded key slots so mixed-size keys always
  admit a split, including maximum-length catalog keys. Vacuum rebuilds those
  references against the new generation; integrity checks prove that the
  referenced records remain reachable in the checkpoint. Updates retain encoded
  key references and resolve only comparison keys, including during splits.
  A transaction-local tree editor shares changed paths across catalog and
  document batches. Sorted batches seal and release completed subtrees, keeping
  the active frontier and adjacent rebalance siblings. Unsorted batches retain
  their touched nodes until finalization. All pages remain private until commit.
  Deleting a catalog key removes it from the current tree using copy-on-write
  merging and redistribution. Historical records and older checkpoint roots
  remain intact, but retired filenames no longer accumulate directory-scan
  work. Directory listing and subtree deletion seek the live catalog tree at a
  path prefix instead of replaying mutation history. Subtree deletion scans one
  immutable root and flushes private batches of at most 64 keys or 16 KiB of
  key bytes (a larger individual key is processed alone). Keys and editor scratch
  are released between batches; all batches publish atomically, and an error
  rolls the whole deletion back. Listing pins a checkpoint
  and holds the generation read lock, allowing ordinary commits to continue.
  In validated namespaces, immediate-file listings seek past each nested
  directory's exclusive prefix bound before reading descendant catalog records.
  Listing work depends on direct files and directory prefixes encountered, not
  nested file count. The unscoped adapter retains its accepted repeated/trailing
  separators and uses a general prefix scan with dirname filtering, so existing
  logical keys keep their listing behavior without normalization or migration.
  Size-limited file reads validate the value length in the pinned catalog record
  before allocating or reading its payload, including external values.
  External catalog values use a 64-way immutable extent tree with byte lengths
  on each child. Appends retain one unfinished node per height, fill the partial
  tail leaf, and seal suffix subtrees once. Existing full subtrees remain
  shared, so large appends no longer rewrite the ancestor path per leaf and
  temporary memory depends on tree height rather than suffix length. Within a
  native transaction, repeated appends share that frontier and partial tail,
  so small calls write the same suffix pages as one combined append.
  Range reads seek directly to the requested extents. Vacuum builds packed
  catalog indexes and extent trees; integrity checks validate both structures.
  Atomic index writes use a fixed 64 KiB buffer and a private staging file.
  Header patches and range checksums operate on the buffered tail and positional
  file I/O. Staging uses a short random sibling basename independent of the
  database name, including when the database basename approaches filesystem
  limits. POSIX staging files are unlinked while open so abort and process
  death reclaim them. Staging holds neither a document writer slot nor a
  generation pin; unrelated commits and vacuum can proceed. Finish streams the
  staged bytes into native extents and publishes one checkpoint under the store
  mutex. This adds a staging I/O pass in exchange for bounded payload heap use;
  it does not eliminate the final copy or its publication lock. I/O failures
  poison the sink, and finish consumes it on success or error.
  Atomic writers carry cache intent through both buffered publication and
  staged imports. Cold sequential writes bypass admission for external payload
  pages, preserving hot reads; catalog records and tree navigation pages remain
  cacheable. Reused page IDs invalidate cached bytes and links even when the
  replacement bypasses admission. The policy belongs to each writer and does
  not disable caching for concurrent readers; subsequent reads can cache the
  cold-written data normally.
  Native external-value writes encode directly into an operation-owned 64 KiB
  page buffer. Staged imports, buffered external values, appends, document chains,
  and vacuum copies stage pages by address and coalesce contiguous runs into
  positional writes. When the buffer fills, it drains the earliest contiguous
  run and retains later pages so late packed-record pages can fill their gaps.
  Fragmented free-page runs flush separately. Value-chain writers retain only
  one next-page ID. A completed tree is flushed before its root can be read or
  published. A failed flush admits none of its requested pages and poisons its batch; abort drops
  pending bytes without an implicit retry. Cache policy is applied per page
  after a successful write.
  Positional page writes extend the file directly, without per-page stat or
  resize calls; data, checkpoint-slot, and active-slot sync barriers remain.
  Revision 2 and other unsupported headers are rejected without mutation;
  there is no automatic upgrade or compatibility reader.
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

A Lite database is provisioned with the default `full_text_index_v0`
full-text index on creation, matching the server's table-create behavior,
regardless of which surface creates it -- the CLI (`antfly lite init` /
`antfly lite create`), the C ABI, and the native Go/Zig `embedded` package all
share the same creation routine, so `antfly lite index create` is only needed
for indexes beyond that default.

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
being mistaken for the format revision 3 contract.

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
cancellation use `/db/v1/restore/jobs/{job_id}`. Retained jobs can be listed
newest-first with `GET /db/v1/restore/jobs`, using cursor pagination and optional
phase/scope filters. Authorization is applied before pagination results are
returned. Idempotency keys make retries
safe; requests without a key create independent jobs. Restore state lives inside
the `.aflite` file, and completed table boundaries are durably checkpointed and
not repeated after restart. Standalone restore is synchronous inside the worker,
so terminal success means the restored table is readable, not merely accepted.
Job status reports published and completed table counts separately; for Lite the
two checkpoints normally advance back-to-back because restoration is local.
It also reports generations whose publication is visible but whose
parent-directory durability is pending. Such a job terminates failed with an
explicit committed/pending result for operator inspection; it is never reported
as rolled back or durably complete. The current table ordinal is checkpointed
before irreversible work so restart reconciliation only adopts the exact backup
identity. Destructive overwrite is not exposed until
table generations can be staged and atomically swapped. Terminal state and
explicit idempotency keys are retained for seven days in a history bounded by
10,000 jobs, 64 MiB total, and 64 KiB per encoded job. Admission reserves room
for progress and terminal state before work begins. Cancellation is checked at
safe table publication boundaries. A standalone process executes at most two
restore jobs concurrently; the remainder stay durably queued inside the
`.aflite` file.
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

Backup and restore remain normal `/db/v1` operations. A physical `.aflite`
copy is a stable snapshot, not the archival contract. `.afb` is the common
bundle envelope; Lite reads and writes its portable logical representation,
while normal Antfly may also package an explicitly native representation. Lite
backup/export emits a self-contained `full` bundle. AFB2 `delta` is an
exact-base repository/export representation: import must receive the named
base-manifest digest and must never guess a base or silently treat the delta as
self-contained.

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
history. Native Lite reports `online: true`: integrity checks pin a header and
file length, while compaction and vacuum copy a pinned generation and catch up
foreground mutations before publication. Readiness stays available during these
jobs. The compatibility bridge still uses the exclusive maintenance gate.
Native checkpoint publication and generation replacement serialize under the
Lite store mutex. Document writers use FIFO admission; vacuum reserves the writer
slot for its short publication window and returns `FileBusy` if bounded catch-up
cannot finish.
Private staged index output enters the store mutex only for final publication.
Vacuum walks the current checkpoint's catalog and document indexes, skips
tombstones, and streams values into replacement pages without a temporary LSM
index or history deduplication. Its key-tree builder and extent builder each
retain one unfinished node per level; payload buffers do not grow with logical
file size. Namespace metadata still scales with namespace count. Integrity
checking continues to validate reachable pages, while compact-layout statistics
use record lengths and ordered keys instead of loading payloads again.
The replacement is fsynced, atomically renamed, and adopted through its
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

Because this is new, unreleased code, native revision 3 does not carry a legacy fallback,
pre-release importer, v0 directory reader, silent LSM-container upgrade path, or
prototype-to-v1 auto-migrator. Prototype files can be recreated from tests or
explicit exports while the format is still pre-release. `.aflite` readers should
accept the documented revision-3 format and reject unknown versions loudly. Recovery
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
- `.afb` is the representation-aware Antfly Backup Bundle. Lite emits and
  consumes the portable logical representation; native bundles are restored by
  normal Antfly.
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
  owns the writer lock, or an optional `busy_timeout_ms` wait for it.
- Serialized threading within a process: one handle may be shared by any
  number of threads, with reads running in parallel and alongside writes. See
  `CAPI.md` "Thread Safety" for the per-call access classes.

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
antfly restore --input app.afb --table docs \
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

Local inference is built in, not optional packaging: every `libantfly`/
`antfly lite` build embeds the standalone inference runtime in-process, the
same as the `antfly` executable (see COMPILATION.md's "C API composition"
section). There is no separate base/full build distinction -- `zig build
capi` always links the inference archive (2026-09-17 product decision: Lite
hosts get local inference without a separate runtime, at the cost of a much
larger shared library).

Opening a Lite handle with the local-runtime-configured flag constructs an
embedded inference provider owned by the handle and reports
`local_inference_runtime: true` and `inference_mode: "local_embedded"`.
Adding an `embeddings` index whose `embedder` (or chunker/extractor producer)
uses `"provider": "antfly"` with no `api_url` runs against that embedded
provider instead of failing or requiring a remote URL -- `antfly lite
run-until-idle app.aflite` drains the resulting enrichment work locally, with
no network calls. Application embedding (see `go/pkg/lite/README.md`
for the Go binding) gets the same embedded behavior automatically by linking
the standard `libantfly` -- no separate library or extra link flags.

Models are still auto-discovered the same way as `antfly inference pull`,
under `~/.antfly/inference/models/`.

**Worker process.** GPU-hosted and driver-backed backends (Metal, CUDA, ONNX,
PJRT) run model construction and, for Metal/CUDA/PJRT, execution itself in a
separate, replaceable child process (`<worker executable> inference
_worker`), not in the host process -- see
`BackendRuntime.requiresProcessIsolation` in
`zig/pkg/inference/src/backends/backends.zig`. This is crash containment, not
an implementation accident: an unabortable driver call or a model load that
corrupts GPU state can only be recovered by killing and respawning the
process that made it, and that must never be the process embedding
`libantfly`. Worker placement is decided per build, not per model: when any
process-isolated backend is compiled in (Metal is on by default on macOS), the
worker starts when a local-runtime handle opens and all local inference runs
there, CPU models included. Only builds without those backends run inference
in-process.

The `antfly` CLI resolves the worker by re-executing itself (`argv[0]` names
the `antfly` binary the user launched, which understands `inference
_worker`). A library host has no such self -- `argv[0]` is the Go test
binary, `examples/dogfood`, or whatever else linked `libantfly` -- so the
runtime resolves the worker executable in this order:

1. `ANTFLY_INFERENCE_WORKER`, an environment variable naming the worker
   executable directly (typically the path to an `antfly` binary).
2. The image this code was loaded from, via `dladdr`: for the statically
   linked `antfly` executable this is itself (unchanged CLI behavior); for a
   shared `libantfly`/`libantfly.dylib`, the runtime looks for a sibling
   `antfly` binary in the same directory.
3. `antfly` on `PATH`.

If none of these resolve, model construction on a process-isolated backend
fails with a clear error naming `ANTFLY_INFERENCE_WORKER`. Set that variable
(or ship an `antfly` binary next to `libantfly`, or put one on `PATH`) when
embedding Lite in a host that is not the `antfly` binary itself and needs
Metal/CUDA/ONNX/PJRT models.

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
`antfly lite index create` adds indexes beyond the default `full_text_index_v0`
full-text index that every creation surface (CLI, C ABI, and embedded)
already provisions, matching the server; it does not need to be run just to
make text search work.

## Packaging

Packages:

- `antfly` CLI with `antfly lite` subcommands.
- `antfly-embedded` Zig package.
- `libantfly` C ABI artifact, with `.aflite` exposed as an embedded storage
  profile rather than a separate Lite-only ABI.
- Language bindings generated or hand-written over the C ABI.
- Optional full package with embedded inference runtime.

Build and test targets:

- `lite`: build and install the Lite-only CLI, `libantfly`, and its C header.
- `lite-test`: run Lite backend, CLI, bindings, examples, and C ABI packaging
  checks, including smoke tests for both the Lite-only and full Antfly CLIs.
- `antfly capi`: build the full Antfly CLI and `libantfly`. Use
  `-Dlite-local-inference-runtime=true` when the embedding supplies a local
  inference runtime and should advertise that capability.
- `wasm`: build and install the embedded database and inference WASM bundle.
  `wasm-test` builds the bundle and runs its Node smoke test.

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
  and producing the portable logical representation used by AFB2. The normal
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
- Add Go as the first post-Zig/C binding in `go/pkg/lite`, backed by the
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


## Native throughput and online compaction

Native v3 now has two explicit signatures: `AFLITE\x03N` for the original
unpacked encoding and `AFLITE\x03P` for packed records. New files use the packed
encoding. Existing unpacked v3 files remain readable and writable without
conversion; explicit vacuum writes a packed replacement. Revision 2 remains
unsupported. Older binaries reject the packed signature before checkpoint
selection, preventing an older reader from silently selecting a pre-packing
fallback checkpoint. A packed file requires a binary supporting this encoding.

Small records in multi-key transactions share immutable record pages. A tagged
reference identifies a physical page and a validated record offset; large
records keep individual pages and external values keep their extent/chain
representation. Checks validate physical checksums, record boundaries, and
checkpoint ownership. Vacuum uses the same packing rules as compact-size
accounting. Record and index writes coalesce in bounded page-write buffers.

Write transactions maintain one ordered pending-key index. Point reads and
cursors use that index directly, and commit emits only each key's final mutation.
Each pending key has one active value. Replacements release unborrowed values
immediately; versions returned by `get` or `getManySorted` remain owned until
transaction teardown. Cursor copies have their own lifetime. Sorted multi-reads visit each relevant index node once,
then order record references by physical page so a packed page is read and
checksummed once for all requested records on it. Result order remains the
caller's key order; missing keys and duplicate requests retain their semantics.
Single and sorted transaction reads share an owned cache of immutable snapshot
hits and misses. Duplicate keys and overlapping batches reuse the same borrowed
payload until transaction teardown; pending writes take precedence without
invalidating previously borrowed values. Retained read memory depends on the
unique read set, rather than the number of read calls.

The page cache uses incremental CLOCK eviction. Incoming payload pages start
cold; reads promote pages and navigation metadata gets additional chances.
Capacity pressure evicts only enough pages to admit the next page. Oversized
pages bypass admission, and reused page IDs invalidate previous contents.
Resource-manager pressure still controls cache memory.

Document commits and index-file mutations enter a synchronous commit queue.
Up to 64 queued requests share one native transaction and one durable checkpoint
publication. The leader hands off after each group, so an unending producer
cannot prevent a completed caller from returning. A failed group fails all its
members; publication failures fence subsequent coordinated writes until reopen.

The native transaction owns a final-key write set across all three roots,
bounded by 1,024 keys and 1 MiB of retained key/inline-value bytes. Each flush
shares page allocation and record packing and edits each touched index once;
the group publishes one free map per flush and one durable checkpoint at commit.
Large values and file imports stream directly to private extents. Append and
rename reuse these extent references, and point/range reads see earlier staged
writes. Writer-side scans explicitly materialize the pending batch before
pinning cursor roots. Pinned reader APIs only consult immutable roots and never
access the mutable write set.
In addition to the key/value staging budget, a transaction retains at most four
append frontiers across the metadata and index catalogs. Each owns a partial
value page, a 64 KiB write buffer, and one bounded extent node per tree height.
Full leaves stream once; commit, spill, and snapshot materialization seal the
frontiers. Active full/range reads and rename seal only the source file they
need, while size reads use its buffered length. Overwrite, delete, and abort
discard obsolete buffers without flushing. Appending to another file when all
four frontier slots are occupied flushes private state without publishing or
ending the transaction. These changes retain
the revision-3 encoding and immutable checkpoint semantics.
An already assembled large batch is consumed synchronously from the caller's
buffers with one index edit, avoiding another owned staging copy. Sorted updates
retain a bounded tree frontier, including deletion-rebalance neighbors, rather
than every touched node. Sorted initial document batches stream each key's
last mutation directly into the bulk index builder, preserving tombstones and
last-write-wins semantics without retaining all index entries. Unsorted batches
retain scratch proportional to their size; the 1,024-key / 1 MiB bounds apply
to mutations retained across calls.
Reaching either staging limit flushes privately without ending the transaction;
abort discards all flushed and pending changes together.

An index append with `sync=false` advances this handle's visible roots while
retaining the last durable on-disk checkpoint slots. A subsequent durable
mutation or explicit sync publishes those accumulated changes with the normal
sync barriers. A separate open observes the last published checkpoint until
that barrier. `no_sync` remains the explicit handle-level durability opt-out.
Callers requiring durable completion must request a barrier and handle its
errors; completion of an asynchronous append is not a durability promise.

Store-level vacuum prepares its replacement from a pinned snapshot while
foreground reads and mutations continue. It captures changed keys across the
document, index, and private metadata catalogs and streams their final values
into the replacement. Catch-up holds at most 65,536 keys / 4 MiB of key bytes
and makes at most eight rounds. If it cannot catch up, it returns `FileBusy`,
discards the unpublished replacement, and leaves foreground commits intact.
Cancellation is checked during copying and catch-up. The final publication
reserves the writer slot and briefly fences checkpoint acquisition and logical
index-file reads while publishing the final header and atomic rename.

Document read transactions own references to their original file generation.
Vacuum can publish without waiting for those transactions; retired descriptors
and caches are released after their last reader. Physical reclamation occurs by
replacing and retiring whole file generations, rather than reusing pages that
might still belong to a reader. Compaction is explicitly requested through the
existing maintenance API; this change does not add an automatic vacuum policy.

The embedded-root adoption probe pins a read generation and inspects only index
and record metadata. It skips the internal `\x02db/` range with an index seek,
ignores tombstones, and stops at the first live user document. It never loads
external document values, so startup memory does not grow with their size.

The server maintenance coordinator advertises native maintenance as online.
Integrity checks pin both the header and observed file length under the mutation
mutex, then validate that snapshot outside it. Later appends do not create false
tail-corruption reports; a tail already present when pinned remains an error.
Compaction releases the namespace registry lock after its initial sync barrier.

Secret metadata enumeration also uses a scope-bounded catalog cursor. It does
not materialize unrelated private metadata or encrypted values from other scopes.

Run `zig build lite-native-benchmark -Doptimize=ReleaseSafe` for the reproducible
transaction-assembly, commit, and sorted-read workloads. Timings are observations;
structural tests enforce page-read, write-call, memory, and correctness bounds.

### Throughput baseline

Measured against `1debc3d03d` with the same benchmark source, Zig 0.16.0,
ReleaseSafe, and the C allocator. Values are medians of three alternating runs
on the same development machine. Each run creates a file, assembles one sorted
batch with a missing-key read before each insert, commits small document values,
and reads all keys in order. The build step compiles the benchmark separately.

| Records | Assembly before / after | Commit before / after | Sorted reads before / after | Page accesses before / after | File bytes before / after |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 1.62 / 0.70 ms | 3.94 / 0.29 ms | 3.06 / 0.23 ms | 3,000 / 24 | 4,136,960 / 110,592 |
| 4,000 | 19.12 / 2.68 ms | 16.29 / 0.91 ms | 13.31 / 0.87 ms | 12,000 / 92 | 16,498,688 / 389,120 |
| 16,000 | 274.66 / 10.85 ms | 65.52 / 3.60 ms | 67.97 / 3.85 ms | 48,000 / 363 | 65,941,504 / 1,499,136 |

This workload uses `no_sync`, warm filesystem caches, and small values. Page
accesses include cache lookups; they are not physical disk I/O counts. Other
builds ran on the host during measurement. Durable fsync throughput, vector
index construction, and sustained concurrent compaction need separate workload
qualification. Timing thresholds are intentionally absent from the tests.

Vacuum catch-up sorts each root's changed keys and applies batches of at most
1,024 keys, with a 1 MiB target for retained inline values and key bytes (at most
one record can cross that target). Each batch shares its index editor and page
allocator. External values stream to the private image before batch publication.
A catch-up round remains one native transaction: failure or cancellation restores
its checkpoint, file length, and report. Rollback also invalidates cached bytes
and links in the discarded tail, preserving the restored checkpoint’s cache;
streamed retry writes can safely reuse those page IDs. The regression workload replays changes
to all three roots and separately bounds heap usage while copying 12 MiB of inline
values, including cancellation after earlier batches have written.

Maintenance snapshots and prepared images share the live store's resource
manager. Their page caches admit only navigation pages; sequential payload scans
use cursor-local storage. Accounting belongs to each handle until it closes,
including private images and retired descriptors after publication. The regression
suite exercises a shared 32 KiB soft / 64 KiB hard page-cache budget and verifies
that cleanup releases both page-cache and link-cache usage.

Document, catalog, vacuum, and sorted multi-read paths share a record-page reader.
It validates each physical page's checksum and packed-record boundaries once per load,
then borrows payload slices until it loads another page. Reverse scans and seeks
use the same validated offsets; returned document and catalog values retain their
existing ownership contracts. Cursor storage is bounded by one physical page and
its record-offset directory.

The benchmark target also includes vacuum catch-up under a 64 KiB shared cache
budget and forward document scans. In the review diagnostic with 4,096 newly
inserted small documents, the catch-up image shrank from 83,275,776 to 507,904 bytes
(the source was 450,560 bytes). Catch-up fell from approximately 121 ms to 18 ms;
16,000-record scans fell from approximately 20 ms to 1.7–2.1 ms. These are local
`no_sync` observations, not latency guarantees. Tests enforce bounded image growth,
cache accounting, heap usage, and physical-page accesses instead of elapsed time.

The grouped-callback regression compares 1,024 small index mutations in 16
transactions of 64 callbacks with the same mutations submitted as explicit
batches. Both now write 89 pages in 48 write calls and produce a 368,640-byte
file. Before transaction staging, individual callbacks wrote 4,962 pages in
2,048 calls and produced a 20,328,448-byte file. These are structural counts,
independent of fsync timing. Regression tests also cover mixed-root groups,
streamed imports and appends, rename ordering, spill/abort, allocation failure,
and adoption probes over large values and excluded internal key ranges.

The append-frontier regression compares 1,024 64-byte appends to a 1 MiB file
inside one transaction with one combined 64 KiB append. Both now write 23 pages
in five write calls and add 94,208 bytes to the file; the individual calls
previously wrote 3,090 pages in 1,027 calls and added 12,656,640 bytes. With 64
small calls, both forms write eight pages in four calls and add 32,768 bytes.
The long-key seek diagnostic uses 16,000 keys of 1,000 bytes with page caching
disabled. Lazy resolution reduces a seek from 298 logical page reads and
313,848 bytes of peak scratch to 17 reads and 14,768 bytes. Permanent regressions
bound page writes and cursor scratch, and cover forward/reverse scans, pinned
roots, complete integrity audits, append visibility barriers, extent boundaries,
private spills, allocation failures, and streaming-write rollback.

A 16,384-file subtree deletion with 128-byte filenames uses 408,723 bytes of
extra peak heap with private batches, down from 9,150,685 bytes when retaining
the entire key set and editor. Logical page reads rise from 2,953 to 4,373 as
each bounded batch reopens its edit path. Sixteen transaction reads of one
256 KiB value retain 262,435 bytes and perform 68 logical reads, compared with
4,194,672 bytes and 1,088 reads before snapshot result sharing. A 1 KiB-limited
read of a 4 MiB index file now rejects with three logical reads and no payload
allocation; previously it allocated the whole payload and performed 1,053 reads.
These measurements disable page caching; regressions enforce memory and I/O
bounds, pinned snapshot semantics, and rollback after private deletion flushes.

Large document and index values share a checksum-checked chunk reader for
linked chains and extent trees. A 64 KiB positional-read window coalesces nearby
value pages. Extent reads coalesce only physically contiguous references within
the requested range; linked chains grow read-ahead while page adjacency holds
and reset it at gaps. Requested pages enter the shared cache according to its
normal or metadata-only policy; unused prefetched pages are never admitted.
Full reads allocate
the returned value once and retain only bounded traversal state. Read windows
remain tied to their pinned checkpoint and never cross its page-count bound.

Follow-up measurements with page caching disabled: 64 blind overwrites of a
256 KiB transaction value retain 262,363 bytes rather than 16,779,704 bytes.
An 8 MiB document read uses three allocations and an index-file read uses two,
down from 4,127 and 4,193 respectively. At 16,384 namespaces, updating one
namespace uses 108,756 bytes of temporary heap and eight page writes rather
than the previous periodic 1,627,634-byte / 102-page snapshot. Ordinary indexed
updates trade the former five-page delta for eight pages to remove snapshot
spikes and whole-directory cold loads. Tiny directories keep their original
page layout. Regression tests bound allocation counts, physical value-read
calls, hot/cold mutation heap, and page writes; timings are not assertions.

Read regressions also bound bytes transferred for values assembled by small
appends, not just the number of calls. A fragmented 1 MiB value now reads
1,081,344 bytes through the value-page reader (including extent metadata,
excluding catalog lookup and the root probe). Repeated small range reads need
no value-page I/O after warming under the normal cache policy; metadata-only
readers retain extent metadata while leaving payloads uncached. Across 200
small document commits, the single-namespace path uses 3,621 allocations.
Retaining the inline directory cache reduces the 32-namespace path from
11,821 to 4,421. Failure sweeps cover directory-loading ownership and
inline-cache preparation, including private publication followed by rollback.

Batched namespace resolution reduces a 16,384-namespace update from 49,348
logical page reads to 449 with caching disabled. Physical grouping keeps packed
record reads bounded after interleaved namespace updates and a cold reopen.
A sorted 65,536-document initial batch in one namespace uses 28,849 bytes of
peak temporary native heap instead of 6,451,968 bytes. At 16,384 and 131,072
documents it uses 22,347 and 28,879 bytes, excluding caller-owned input. Regressions cover read and
heap bounds, repeated mutations in input order, missing namespaces, external
index keys, pinned checkpoints, and allocation-failure rollback in packed and
unpacked v3 files.

Read-only index traversals retain validated page views and slot offsets rather
than copying every inline key. Batch reads, namespace-head resolution, and
cursors reuse buffers by tree depth; overflow keys are resolved lazily into
bounded scratch buffers. Sorted batches advance from the previous separator
and skip larger gaps with a bounded search. Record readers reuse their physical
page buffers. Returned cursor keys and batch values remain independently owned;
failed refills invalidate the active cursor path and permit a fresh seek.

With 16,384 short-key documents and caching disabled, a one-key batch uses 13
allocations instead of 279, a full batch uses 16,396 instead of 33,599, and an
index cursor scan uses 16,391 instead of 33,385. After warming its traversal
buffers, each seek allocates only its returned key. A one-document snapshot
among 16,384 namespaces seeks the pinned document index: three logical reads,
16 allocations, and 14,459 bytes of peak temporary heap, down from 255 reads,
49,881 allocations, and 1,402,790 bytes. The cursor retains bounded traversal
scratch and reads external values from the same checkpoint. Legacy indexed
tombstones remain excluded. With one live key after 16,384 versions, a snapshot
uses two reads and 11 allocations rather than 16,387 reads and 32,779 allocations.
Arbitrary byte prefixes stop at the first nonmatching key. Regressions cover
allocation bounds, dense and sparse overflow reads, packed and unpacked v3
records, pinned roots, malformed pages, and allocation-failure recovery.

Integrity coverage scans the index once and walks history newest first. It stores
hash buckets of record references, resolving every candidate collision with a
complete key comparison, and requires each indexed reference to name the newest
record for its key. Missing live keys, stale references, extra entries, and
legacy tombstone references retain their existing validation rules. History,
namespace-link, and leaf-record audits reuse packed-page readers while checks
continue bypassing the page cache to validate on-disk checksums.

With caching disabled and 16,384 short-key documents, complete integrity checks
use 1,279 logical reads instead of 98,811. With 16,384 distinct namespaces, they
use 2,127 instead of 164,780. Neither workload performs per-record point probes.
These bounds describe the packed, ordered fixtures; overwritten or physically
scattered history can require additional record reads for exact comparisons.

Sorted document updates use 98,923 bytes of peak temporary native heap at 16,384
documents, 209,341 at 65,536, and 210,033 at 131,072, down from 3,274,182,
14,251,272, and 26,493,930 respectively. Caller-owned input is excluded. Per-node
ownership, geometric reclamation of dead arena allocations, and retention of
rebalance neighbors keep update and delete scratch tied to the frontier.
Private pages are flushed before a rebalance rereads them; ordinary frontier
advances preserve packed-record filling and buffered writes. Regressions cover
sparse-write bounds, multi-level deletion, mixed inline/overflow keys, duplicate
mutations, pinned roots, packed/unpacked v3, collision handling, allocation
failures, and rollback after partial writes.

The bounded page-addressed buffer also preserves coalescing across streamed
index nodes and late packed-record pages. For 65,536 sorted document updates,
page-write calls fall from 1,203 to 83 while 1,221 written pages and 411 reads
remain unchanged. For catalog updates, calls fall from 1,223 to 99 with 1,434
written pages unchanged. Regressions cover out-of-order pages, duplicate
staged addresses, partial-run failures without cache admission, sorted initial
ingest under a 512 KiB native heap budget, and pinned snapshots after overwrites.

Document, catalog, namespace, and vacuum record writers share reusable encoding
scratch owned by their page allocator. The bulk builder copies inline leaf keys
into one fixed page-sized slab; parent separators retain independent ownership
before that slab is reused. A sorted 65,536-document ingest now makes 858 native
allocations rather than 131,928, excluding caller-owned input. At 16,384 documents
it makes 238 rather than 33,004. Peak scratch remains bounded by page size and
tree height; packed and unpacked revision-3 layouts are unchanged.

Materialized snapshots retain one temporary reference per indexed result, group
records by physical page, and sort the owned output back into key order. On a
65,536-document fixture inserted out of key order with caching disabled, reads
fall from 65,945 to 1,181 and allocations from 196,640 to 131,120. The ordered
fixture also takes 1,181 reads. Metadata storage scales with the requested live
result set, never its historical versions; legacy indexed tombstones are filtered
before returning. Regressions cover allocation churn, scattered pages, distinct
file/output allocators, pinned external values, mixed tombstones, and exhaustive
allocation-failure cleanup.
