# Antfly deployment and storage engines

Antfly models deployment topology and persistence as independent choices. A
deployment mode answers **which processes run**; a storage engine answers
**where durable state lives**. `lite` is therefore a storage engine, not a
deployment mode.

## Vocabulary

Deployment modes:

| Mode | Runtime ownership |
| --- | --- |
| `embedded` | An application opens Antfly directly; no server is required. |
| `standalone` | One server process owns metadata, data, APIs, and inference. |
| `distributed` | Split roles use replicated coordination and can scale horizontally. |
| `serverless` | Compute is decoupled from object-backed durable state. |

Storage engines:

| Engine | Durable representation |
| --- | --- |
| `lite` | One portable `.aflite` file and one writable owner. |
| `local` | Directory-based local shard and metadata storage. |
| `object` | Object-store-backed durable data. |

Backup representation is orthogonal to these engines. The canonical remote
repository is `refs/` plus immutable `manifests/<sha256>` and
`blobs/sha256/<sha256>` objects. A manifest names either a portable logical or
native physical representation and always carries the complete materialized
inventory for its snapshot. Logical objects map safe paths and roles to a
separate digest-sorted unique blob inventory, so content can be deduplicated
without losing native filenames. Parent links make incremental capture,
accounting, and reachability GC efficient; repository restore pins one immutable
manifest and does not replay a parent chain.

`.afb` is the Antfly Backup Bundle transport envelope. AFB1 remains readable as
the v0.2.0 portable stream. AFB2 identifies native versus portable and full
versus delta in its manifest, stores digest-addressed blobs, and ends with a
digest-to-offset index plus a fixed-size checksummed locator trailer. Full
bundles carry every unique digest once. Delta
bundles carry only digests absent from an exact declared base; native and
portable import both fail closed without that base and rehash resolved content.
A self-contained native AFB2 is therefore a transport for an already validated
native generation, not a new storage engine.

This gives the product names precise meanings:

- **Antfly Lite** is the single-file engine and `.aflite` format.
- **Antfly Standalone** is the all-in-one server topology.
- **Standalone with Lite** is the full standalone server backed by one
  `.aflite` file.
- **Embedded Lite** is an application opening the same file without a server.

## Configuration

Storage is a tagged union. Exactly one member matching `storage.engine` may be
configured.

The Zig runtime consumes JSON configuration validated against the OpenAPI
schema. The selected command supplies the deployment topology; an optional
`deployment_mode` in the file is an assertion and must match the command.

Single-file standalone:

```json
{
  "storage": {
    "engine": "lite",
    "lite": { "path": "./data.antfly.aflite", "fsync": true }
  }
}
```

Directory-backed standalone or distributed deployment:

```json
{
  "storage": {
    "engine": "local",
    "local": { "base_dir": "~/.antfly" }
  }
}
```

The local engine is entirely directory-backed. Object storage is a distinct
engine rather than a per-subsystem override; mixed local/S3 configurations are
rejected so data placement cannot silently diverge from the selected engine.

Object-backed serverless deployment:

```json
{
  "connections": {
    "primary-storage": {
      "kind": "external_io",
      "capabilities": ["storage.primary"],
      "external_io": {
        "protocol": "s3",
        "region": "us-west-2",
        "buckets": ["antfly-data", "antfly-wal"],
        "prefix": "production",
        "bucket_provisioning": "require_existing",
        "credentials": { "source": "web_identity", "role_arn": "arn:aws:iam::123456789012:role/antfly-data", "token_file": "/var/run/secrets/data/token" }
      }
    }
  },
  "storage": {
    "engine": "object",
    "object": {
      "connection": "primary-storage",
      "bucket": "antfly-data",
      "prefix": "production",
      "lanes": { "wal": { "bucket": "antfly-wal", "prefix": "production/wal" } }
    }
  }
}
```

Serverless derives its `artifacts`, `manifests`, `wal`, `progress`, and
`catalog` object prefixes from this root. Each lane may override the connection,
bucket, or prefix, which supports separate durability classes, accounts, and
least-privilege credentials. Referenced connections must be S3 `external_io`
connections with the `storage.primary` capability, and any configured bucket
allowlist is enforced at startup. S3 connections require a non-empty explicit
bucket allowlist; unrestricted bucket access is never inferred from omission.
Low-level URI flags and environment variables are supported only without a
connection-based config; mixing them is rejected because a location override
without an identity override is ambiguous. Lanes that resolve to the same
connection share one object-store client and HTTP connection pool; distinct
credential sources remain isolated.

Primary storage credentials are deliberately independent from
`remote_content.s3`. The former grants Antfly write authority over database
state; the latter grants read authority over user-provided content and may
contain several named credentials selected by bucket. `credentials.source` is
one of `default`, `static`, `profile`, or `web_identity`. The default source uses
the refreshable AWS default credential chain shared with Bedrock: environment
credentials, web identity/IRSA, shared profiles, ECS task credentials, and EC2
instance metadata. Profile and web-identity sources are configured per
connection and therefore support multiple accounts in one process. Expiring
credentials refresh before their safety window. Static keys remain available
through secret references for S3-compatible systems, but should never be
committed plaintext.
Pass `--secret-store-path` (or `ANTFLY_SECRET_STORE_PATH`) when those JSON
values use `${secret:...}`.

External-I/O credential references remain references in the loaded node
configuration. Antfly resolves an owned credential snapshot immediately before
each backup, restore, or connection probe, and keeps it alive for the object
client lifetime. Updating the secret store therefore affects new operations
without restarting the process; in-flight operations continue with the
snapshot they opened with. Probe-cache identities include a digest of the
resolved snapshot, so rotation cannot reuse a stale successful probe. Bucket,
prefix, protocol, and capability authorization remain immutable node config and
cannot be expanded by rotating a credential.

Primary object-storage clients resolve connection references once during
serverless bootstrap because they own long-lived pools and durability state.
Use `default`, `profile`, or `web_identity` for automatically refreshed primary
credentials. Rotating a `static` primary-storage secret requires a controlled
process restart; this does not affect the per-operation rotation behavior of
backup and restore connections.

Object-store clients created by the API borrow the process's shared backend
`std.Io` runtime for S3/GCS requests and credential refresh. Connection probes
retain bounded fanout without constructing a scheduler per connection, and
restore workers do not create a scheduler per job. Embedded and CLI callers
without a backend runtime receive one client-owned fallback. Serverless keeps a
single pool runtime shared by its credential-isolated S3 clients. Google access
token refresh is serialized with a `std.Io.Mutex`, so concurrent GCS operations
reuse one immutable cached token instead of racing refresh and reclamation. A
failed proactive refresh continues serving the still-valid token until its
actual expiry, then fails closed.

Remote artifact traversal is segment-scoped and paginated. Restore lists at
most 1,000 objects at a time, validates every returned key as a strict
descendant of the requested artifact directory, and requires continuation
tokens to advance. There is no 10,000-object restore ceiling, sibling prefixes
cannot bleed into a restore, and page memory remains bounded independently of
backup size. Local file transfer reuses the same `std.Io` runtime instead of
constructing a scheduler for every artifact. Full-object downloads use bounded
8 MiB range reads into an atomic temporary file and fence every range with the
object's initial ETag, preventing both whole-object heap growth and mixed-version
files when an archive is modified concurrently. The filesystem provider keeps
metadata and bytes in one versioned object envelope published by an atomic
rename after syncing the staged file, performs range requests with positional
`std.Io` reads, and serializes mutations with bounded striped advisory locks.
Filesystems without lock support fail closed rather than weakening conditional
writes. Upload staging lives outside the enumerable key namespace, so
concurrent pagination cannot observe partial objects or internal temporary
keys. Staged files retain an exclusive advisory lock for the lifetime of an
upload. At startup, the provider removes only unlocked staging files older than
24 hours; cleanup scans bucket staging directories rather than stored objects,
so crash recovery is bounded by bucket count and abandoned uploads instead of
database size.

Whole-file filesystem restores use a provider-native streaming path: the
object is opened once, SHA-256 is recomputed while copying, and the destination
is renamed into place only after the checksum matches the envelope. This
detects local media corruption without adding whole-object memory or repeated
open/stat overhead. Recursive filesystem restores use a native prefix transfer,
walking the selected archive once instead of rescanning the namespace for every
logical page; S3 and GCS continue using their service-native continuation
tokens. Portable AFB restore validation and import use three positional file
passes—validation, primary records, then derived indexes—rather than loading the
archive into heap memory. Each payload and compressed expansion is capped at
128 MiB, so peak parser memory is independent of archive size and malformed
block lengths fail before allocation. A shared advisory lock and size/mtime
fence pin all passes to one archive generation; unsupported locking and
concurrent rewrites fail closed.

Backup uploads are bounded as well. Filesystem connections stream directly
into their staged object envelope; S3 multipart parts and GCS resumable chunks
start at 16 MiB and grow for very large objects to keep request counts bounded.
Portable export walks a pinned ordered storage cursor and emits bounded AFB
batches directly into a synced temporary file, then atomically publishes it;
neither the logical database nor completed archive is materialized in heap.
Failed sessions are aborted or cancelled, and source size and modification time
are fenced throughout upload so concurrent rewrites fail before completion. The
backup API preserves file paths through upload and download instead of
rematerializing portable archives at the location boundary. Providers without a
streaming upload capability reject files above 64 MiB instead of attempting an
unbounded heap allocation. Filesystem listing retains only the smallest
requested page plus one continuation candidate, so page memory is bounded by
the requested page size even when the archive contains many more objects. Page
sizes are capped at 10,000 keys to keep caller-controlled memory and sorting
work predictable.

Backup catalog listing is cursor-paginated in stable manifest-key order. The
API and CLI default to 100 backups per page and reject limits above 1,000.
Remote locations resume with provider-native object cursors; filesystem
locations scan the directory once while retaining only the smallest `limit +
1` eligible manifest names in a bounded heap. Responses include `next_cursor`
only when another page exists.

Backup IDs are immutable publication keys. Payloads and per-table manifests use
opaque generation-scoped paths; the public table or cluster manifest is created
conditionally as the final commit point. Reusing a published ID returns `409`
without changing the existing backup, and a failed pre-publication attempt can
be retried safely. This prevents restores from observing an old manifest with
partially overwritten payloads. Local manifest publication holds a fail-closed
advisory lock and uses sync-plus-rename; object stores use conditional puts.
Manifest publication and reads are limited to 16 MiB for both filesystem and
object-store locations, so Antfly cannot commit a backup that it will later
reject as oversized. Remote reads request at most the limit plus one sentinel
byte, preventing an oversized or malformed control-plane object from causing an
unbounded client allocation before validation.

Backup and restore select credentials independently from primary storage and
remote-content reads. Network requests must name an `external_io` connection while the
`s3://` location continues to identify the artifact:

```console
antfly backup --backup-id daily --location s3://archive/prod/daily --connection archive-writer
antfly restore --backup-id daily --location s3://archive/prod/daily --connection archive-reader
```

Network backup and restore commands require both `--connection` and
`--location`; there is no client-local filesystem default because a connection
may authorize several buckets or a server-scoped filesystem root. Backup
format defaults to `portable` for both table and cluster backups. Use
`--format native` explicitly when same-engine physical restore speed is more
important than cross-engine portability.

Restore does not accept a format selector. The published manifest and artifact
magic are authoritative, so table restore, input restore, and Lite promotion
all detect `native` versus `portable` rather than accepting an ignored or
contradictory client hint.

Cluster backup is a synchronous aggregate operation and always emits its
per-table JSON result. An HTTP `200` means the aggregate attempt finished, not
that every table succeeded. The CLI exits zero only when the result is
`completed`; `partial`, `failed`, malformed, and internally inconsistent
results exit non-zero so scheduled backups cannot silently accept incomplete
artifacts.

The server requires `backup.write` or `restore.read` respectively and verifies
the object-store bucket and segment-bounded prefix, or resolves a logical
`file:///...` path beneath an administrator-controlled filesystem root. This
supports several buckets, accounts, roles, and read/write trust domains in one
process without promoting storage credentials to process-global environment
variables. Every network backup and restore request requires a named connection;
ambient process credentials are not exposed through the network API. Offline
Lite tooling may still use environment credentials when explicitly requested.

`backup.write` credentials may be genuinely write-only. Normal publication
does not probe the bucket or read an existing manifest; it writes private
generation objects and uses a conditional manifest create as the conflict
check. `HeadBucket` is required only when the connection explicitly enables
bucket provisioning. Listing and restore use a separate `restore.read`
connection and its read credentials. Read paths issue only the required
list/get operations and do not add a bucket-existence probe, preserving
least-privilege object-reader configurations.

When a backend cannot stream a snapshot directly to object storage, Antfly
stages the generation beneath the configured local storage root (or the Lite
file directory), rather than a hard-coded repository cache path. The staging
directory is keyed by the cryptographically random artifact generation and is
created exclusively. Deployments without local staging authority fail closed
instead of writing to an implicit container path.

GCS and filesystem authority are protocol-specific rather than accepting S3
fields that would be silently ignored:

```json
{
  "connections": {
    "gcs-reader": {
      "kind": "external_io",
      "capabilities": ["restore.read"],
      "external_io": {
        "protocol": "gcs",
        "project_id": "prod-search",
        "buckets": ["antfly-archive"],
        "prefix": "production",
        "credentials": {
          "source": "service_account",
          "credentials_path": "/var/run/secrets/gcs-reader.json"
        }
      }
    },
    "local-backups": {
      "kind": "external_io",
      "capabilities": ["backup.write", "restore.read"],
      "external_io": {
        "protocol": "filesystem",
        "root": "/var/lib/antfly/backups"
      }
    }
  }
}
```

Backup identifiers are bounded portable path components. Manifest artifact
paths are validated as relative paths, and filesystem locations cannot escape
their connection root through absolute paths, traversal components, or existing
symlink ancestors.

Restore is a durable asynchronous job. `POST /db/v1/restore` and table restore
verify that both the shared asynchronous backend-runtime lane and a durable
engine, filesystem, or replicated-metadata job store are available, then persist
the job before returning `202`; otherwise admission fails with `503` and creates
no job. Clients poll or cancel
`/db/v1/restore/jobs/{job_id}` and list retained jobs with
`GET /db/v1/restore/jobs`. The list is newest-first, cursor-paginated, and may
be filtered by phase or scope. Authorization is applied before a job enters the
page: table administrators see jobs for their tables and cluster administrators
see cluster jobs. `Idempotency-Key` safely coalesces retries,
while requests without a key create independent jobs. Catalog publication and
replica completion are recorded as separate durable per-table checkpoints.
Publication is not repeated after restart. If leadership
changes before that checkpoint, recovery adopts only an exact, still-active
restore intent for the same backup and location; unrelated existing tables and
an already-cleared ambiguous intent fail closed. In distributed deployments a
job becomes `succeeded` only after every placement replica reports completion,
the catalog clears the table's restore intents, and the completion checkpoint is
durable. Job status exposes separate published and completed table counts so
operators can distinguish accepted work from readable restored data. Progress
also exposes a durability-pending count. A table enters that state when its new
generation is visible but parent-directory durability cannot be confirmed; the
job fails closed with a committed/pending result instead of reporting either a
rollback or durable success. The active table ordinal is checkpointed before
irreversible work, so recovery can reconcile an exact restore identity after a
worker crash without treating an unrelated existing table as resumable.
Progress is stored as a single active ordinal plus disjoint compressed ranges
for durability-pending, published, and completed tables rather than repeated
table-name strings, so its durable footprint remains O(table count). Cluster jobs persist a bounded
terminal summary instead of duplicating every table name: aggregate counts are
complete, up to eight failure details are retained, and a truncation flag is
explicit. A partial cluster restore has phase `failed`, so CLI and automation
cannot mistake partial data for complete success. Destructive overwrite is
intentionally not a restore mode until the catalog supports an atomic
staged-generation swap.
Cancellation is cooperative and best-effort: queued work becomes cancelled,
running work stops at a safe boundary, including while the metadata leader is
waiting for placement replicas, and a successful irreversible publication that
races cancellation remains `succeeded` with `cancel_requested: true` for
auditability. The API never reports restored data as cancelled.
Terminal job state and explicit idempotency keys are retained for seven days;
the history is bounded by 10,000 jobs, 64 MiB total, and 64 KiB per encoded job.
Admission reserves space for progress and the bounded terminal summary and
rejects new work instead of discovering a record-size failure after restoring
data. Job IDs are random
opaque 63-bit values. In distributed deployments, job records, idempotency
fences, published/completed table checkpoints, and the runnable queue are metadata-Raft
state. Durable job records carry an explicit internal format version and fail
closed if a future binary cannot interpret them. Followers may serve locally
applied job state for scalable polling; if a
new job has not applied there yet, they return a retryable `503` with
`Retry-After` instead of a false `404`. The Antfly CLI retries this response
until its wait deadline, so polling works behind non-sticky load balancers.
Creation, cancellation, and execution remain
metadata-leader operations. A leadership-term
change is detected by a backend-runtime maintenance supervisor, without
requiring client traffic. It reloads replicated state, fences the old worker
owner, and returns incomplete attempts to their original FIFO positions with a
higher attempt ID. Mutations are acknowledged only after a Raft read barrier
confirms the proposed value is committed and applied. At most two restore jobs
execute across the control-plane leader. A single nonblocking dispatcher owns
FIFO admission. Concurrent wakeups coalesce into another pass, and leadership
transition pauses admission before draining the old worker owner, preventing
completion callbacks from deadlocking against dispatch or reordering requeued
work. The runnable deque is rebuilt once on
leadership acquisition and consumed incrementally, so completing a job does
not rescan retained terminal history. Expired history is removed in bounded
batches of up to 1,024 keys per metadata-Raft transition rather than one
consensus round per job, avoiding retention cliffs during failover and new-job
admission.
Table restore jobs are visible to administrators of that table; cluster restore
jobs require cluster administration. Cancellation is observed at table-publication
boundaries.

The canonical and convenience forms are equivalent:

```console
antfly standalone --storage-engine lite --storage-path ./data.antfly.aflite
antfly lite serve ./data.antfly.aflite --fsync true --config production.json
```

Both start the normal standalone metadata, data, inference, SQL, and public
HTTP runtime. `antfly lite serve` is only an artifact-oriented constructor; it
atomically creates the file when it does not exist and opens it otherwise. All
other standalone options are forwarded; storage path/engine and listen address
remain owned by the Lite constructor so conflicting duplicates fail closed.
There is no storage-specific HTTP namespace. Clients use the same `/db/v1`
contract regardless of whether standalone storage is `local` or `lite`.

Durable multi-request transaction sessions have bounded, configurable
retention. Defaults are one hour, cleanup every minute, 1,024 sessions, a
16 MiB encoded record per session, and 64 savepoints:

```json
{
  "transaction_sessions": {
    "ttl_seconds": 3600,
    "cleanup_interval_seconds": 60,
    "max_count": 1024,
    "max_record_bytes": 16777216,
    "max_savepoints": 64
  }
}
```

Session changes use persist-before-publish copy-on-write semantics. A failed
allocation, write, or fsync leaves both the durable record and the in-memory
session unchanged. Mutations are serialized by a bounded set of per-session
locks; unrelated sessions do not hold the registry lock while cloning,
renewing leases, encoding, or waiting for durable I/O.

## Validation and safety invariants

- `lite` is valid only with `standalone` or `embedded`.
- `object` is valid only with `serverless` until an explicit distributed object
  storage design is implemented.
- `storage.lite.path` is required and must end in `.aflite`.
- `storage.local`, `storage.lite`, and `storage.object` are mutually exclusive.
- Lite has exactly one writable process per file. A second writer fails closed
  on the file lock; readers must use a stable snapshot or a read-only mode.
- Filesystems without advisory locks are unsupported for normal Lite opens.
  Reader, writer, stable-snapshot, and maintenance paths return
  `FileLocksUnsupported`; they never reopen the file without fencing.
- Raft, shard replication, horizontal scaling, and distributed role settings
  are invalid with Lite.
- A Lite file contains database metadata, documents, indexes, schemas,
  enrichments, and transactional state. Server identity/RBAC, protected
  secret-store files, model files, and extension packages are deployment control-plane
  state and are intentionally external. Moving a complete secured deployment
  requires both the `.aflite` file and its control-plane configuration.
- Lite durability is controlled by `fsync`. High availability is a separate
  WAL/file-replication concern and must not be described as Raft replication.
  `fsync: false` is intended only for disposable or externally protected data;
  acknowledged commits can be lost after an OS or power failure.
- Switching a running deployment between storage engines is a migration, never
  an in-place configuration toggle. Startup must not create an empty target and
  redirect traffic when durable state already exists under another engine.

## Runtime architecture

Lite installs a scoped DB-open provider on the standalone backend runtime.
Logical group paths become cached key and index namespaces inside the file, so
the existing `/db/v1`, SQL, transaction, indexing, and inference code paths are
reused unchanged. Each checkpoint pins both the append-only document history
and a copy-on-write, disk-resident ordered B+ tree whose leaves map full logical
keys to their newest document pages. Point reads and cursor seeks are
`O(log N)` page lookups. Forward and reverse cursors retain one decoded
root-to-leaf path, making sequential next/previous traversal amortized `O(1)`
per key while retaining memory proportional only to tree height; they skip
indexed tombstones and never materialize a table-sized key array. Prefix bounds
prevent cross-table scans. Write cursors merge a sorted overlay bounded by the
transaction's pending mutations, preserving read-your-writes without loading
the durable keyspace. Initial imports and vacuum use
a streaming packed-tree builder, retaining one leaf plus one separator per
output page rather than all live keys. Normal commits copy only the affected
tree path, and the shared bounded page cache serves index and document pages.

Pinned reads use concurrent positional I/O rather than holding the store-wide
mutation mutex. Index segment reads use the same checkpoint and generation
fence, so concurrent queries do not serialize on the mutation mutex and vacuum
cannot reclaim their pages. A `std.Io.RwLock` permits normal append-only commits
while readers pin roots and makes vacuum/rewrite wait before reclaiming pages.
A checkpointed namespace-head directory and per-namespace document links retain
efficient table-history maintenance independently of the global ordered index.
Recreating an existing Lite artifact publishes a complete new inode by atomic
rename, so readers pinned to the prior generation are never exposed to
truncation. Missing namespace
metadata fails closed as file corruption. Hash-indexed namespace runtimes give
expected `O(1)` reuse without storage reinitialization on repeated reader/writer
opens. The standalone metadata
catalog and durable HTTP transaction sessions are stored in reserved system
namespaces in the same file. Metadata is published in memory only with a
durable catalog commit; staged multi-request transactions are reloaded from the
file after restart rather than depending on a directory sidecar.

Storage maintenance uses the engine-neutral authenticated admin surface:
`POST /admin/v1/maintenance/{check,compact,vacuum}` returns an asynchronous job,
`GET /admin/v1/maintenance/jobs/{job_id}` reports progress and results, and
`DELETE /admin/v1/maintenance/jobs/{job_id}` requests cooperative cancellation.
With normal API authentication enabled, admin RBAC protects these routes.
Otherwise they are disabled unless standalone is started with
`--admin-token-env <ENV_NAME>`; callers then send that value as a Bearer token.
The same routes exist for every engine and return `422` when unsupported. The
status capability reports whether maintenance preserves request availability.
Lite maintenance is currently coordinated but exclusive (`online: false`): the
asynchronous job runs through the server's shared `std.Io` backend-runtime lane
with fenced owner shutdown, acquires the file maintenance gate, readiness
becomes false,
and new database requests fail fast with `503` instead of accumulating behind
the maintenance lock. Admin status and cancellation remain available. Shutdown
and explicit cancellation stop native checks and vacuum
at safe page/record boundaries; a replacement file is never published after
cancellation. Idempotency keys are retained for at least 24 hours within a
server process. Job IDs use 63-bit process-seeded opaque values with collision
checks, so identifiers do not expose sequence information and cross-process
aliasing is negligible. After restart,
callers reconcile storage state before retrying. The bounded history rejects
new jobs instead of silently evicting an unexpired key. Vacuum retains one
logical record class at a time rather than holding metadata, index, and
document key sets simultaneously, streams one value at a time into a durable
replacement file, and atomically renames it; it has no artificial distinct-key
ceiling and does not allocate a second database-sized output image. Lite also
keeps `lite check`, `lite compact`, and `lite vacuum` for offline files. Backup
and restore remain portable `/db/v1` operations rather than storage
maintenance.
