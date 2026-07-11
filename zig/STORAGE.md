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

This gives the product names precise meanings:

- **Antfly Lite** is the single-file engine and `.aflite` format.
- **Antfly Standalone** is the all-in-one server topology.
- **Standalone with Lite** is the full standalone server backed by one
  `.aflite` file.
- **Embedded Lite** is an application opening the same file without a server.

## Configuration

Storage is a tagged union. Exactly one member matching `storage.engine` may be
configured.

The examples below use YAML for readability and match the OpenAPI schema. The
current Zig `--config` loader consumes the equivalent JSON representation.

```yaml
# Single-file standalone
deployment_mode: standalone
storage:
  engine: lite
  lite:
    path: ./data.antfly.aflite
    fsync: true
```

```yaml
# Directory-backed standalone or distributed deployment
deployment_mode: standalone
storage:
  engine: local
  local:
    base_dir: ~/.antfly
    data: local
    metadata: local
```

Local-engine backend selection and any S3 connection used by that engine are
nested under `storage.local`. Removed top-level fields such as `storage.data`,
`storage.metadata`, and `storage.s3` are invalid; configuration loaders fail
closed instead of interpreting multiple shapes.

```yaml
# Object-backed serverless deployment
deployment_mode: serverless
storage:
  engine: object
  object:
    provider: s3
    bucket: antfly-data
    prefix: production/
```

The canonical and convenience forms are equivalent:

```console
antfly standalone --storage-engine lite --storage-path ./data.antfly.aflite
antfly lite serve ./data.antfly.aflite --fsync true
```

Both start the normal standalone metadata, data, inference, SQL, and public
HTTP runtime. `antfly lite serve` is only an artifact-oriented constructor; it
atomically creates the file when it does not exist and opens it otherwise.
There is no storage-specific HTTP namespace. Clients use the same `/db/v1`
contract regardless of whether standalone storage is `local` or `lite`.

Durable multi-request transaction sessions have bounded, configurable
retention. Defaults are one hour, cleanup every minute, 1,024 sessions, a
16 MiB encoded record per session, and 64 savepoints:

```yaml
transaction_sessions:
  ttl_seconds: 3600
  cleanup_interval_seconds: 60
  max_count: 1024
  max_record_bytes: 16777216
  max_savepoints: 64
```

Session changes use persist-before-publish copy-on-write semantics. A failed
allocation, write, or fsync leaves both the durable record and the in-memory
session unchanged.

## Validation and safety invariants

- `lite` is valid only with `standalone` or `embedded`.
- `object` is valid only with `serverless` until an explicit distributed object
  storage design is implemented.
- `storage.lite.path` is required and must end in `.aflite`.
- `storage.local`, `storage.lite`, and `storage.object` are mutually exclusive.
- Lite has exactly one writable process per file. A second writer fails closed
  on the file lock; readers must use a stable snapshot or a read-only mode.
- Raft, shard replication, horizontal scaling, and distributed role settings
  are invalid with Lite.
- A Lite file contains metadata, documents, indexes, schemas, enrichments, and
  transactional state. Sidecar files are temporary recovery artifacts, not
  independently required database state.
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
reused unchanged. Prefix-bounded cursors prevent cross-table scans;
per-namespace snapshots bound materialized memory and allow writes to update
only the affected table cache. A checkpointed namespace-head directory and
per-namespace document links make cold materialization proportional to the
selected namespace's history. Missing namespace metadata fails closed as file
corruption. Cached namespace runtimes avoid allocations and storage
reinitialization on repeated reader/writer opens. The standalone metadata
catalog and durable HTTP transaction sessions are stored in reserved system
namespaces in the same file. Metadata is published in memory only with a
durable catalog commit; staged multi-request transactions are reloaded from the
file after restart rather than depending on a directory sidecar.

Storage maintenance uses the engine-neutral authenticated admin surface:
`POST /admin/v1/maintenance/{check,compact,vacuum}` returns an asynchronous job,
and `GET /admin/v1/maintenance/jobs/{job_id}` reports progress and results.
The same routes exist for every engine and return `422` when unsupported. Lite
also keeps `lite check`, `compact`, and `vacuum` for offline files. Backup and
restore remain portable `/db/v1` operations rather than storage maintenance.
