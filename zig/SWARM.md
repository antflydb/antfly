# Swarm Runtime, Providers, and Shard DB Access

This file records the node-level design decisions for metadata, data, local
providers, shard DB access, and DB runtime ownership.

## Decisions

- A node should own one `BackendRuntime` and share it across metadata, data,
  and all DB/store opens on that node.
- A swarm node always owns embedded Antfly inference. It exposes Antfly inference as both the
  direct local inference provider for Antfly enrichment/query code and as the
  public `/ai/v1` compatibility API on the unified server. Local managed
  embeddings must not loop back through the node's public HTTP `/ai/v1` server
  just to call Antfly inference in the same process.
- Metadata should not grow private DB runtimes or private worker pools when it
  probes local shard data.
- Metadata should prefer a typed shard DB adapter for DB-level shard operations
  instead of opening local DBs ad hoc.
- Direct DB opens remain valid as a fallback for bootstrap, tests, restore, and
  cases where no live local data service owns the shard yet.
- Fallback direct opens must borrow the node `BackendRuntime` when one exists.
- The raft `ShardOperationAdapter` is only the transition-action surface. DB
  probes such as median-key lookup and schema readiness live on
  `ShardDbAdapter`.
- Runtime status is the preferred live metadata channel for facts that can be
  cached and published by data servers. `ShardDbAdapter` is for DB/probe facts
  that are not yet available in runtime status, or for fallback/bootstrap
  probes.
- Simulations need an explicit runtime mode rather than accidental fallback
  runtime creation. Deterministic/manual simulations should borrow the shared
  manual runtime and drive visibility through foreground progress hooks. IO-
  backed simulations remain useful when the test specifically validates worker
  scheduling or shutdown semantics.

## Local Antfly inference Provider

The provider contract should distinguish remote Antfly inference from local embedded
Antfly inference:

- **Remote Antfly inference provider:** uses HTTP through `base_url`. This is still the
  right shape when inference is hosted by another process, another node, or an
  external Antfly inference deployment.
- **Local Antfly/Antfly inference provider:** resolves `provider=antfly` or
  `provider=antfly` configs that target the current node's embedded model
  runtime into direct dense embedder, sparse embedder, reranker, generator, and
  chunker implementations. It should call the shared Antfly inference `Node` / pipeline
  APIs directly, reuse loaded model sessions, and never consume public HTTP
  accept/handler capacity.

The direct provider is the production shape for `antfly swarm`. Loopback HTTP is
only for external clients using the Antfly inference-compatible API; it has the wrong
failure mode for background enrichment because a long local embedding request
can occupy the public API path, making status and other public requests look
unavailable even though the process is alive.

The direct provider should also be the place where local inference concurrency
and batching are controlled. Enrichment can then batch documents and chunking
requests without creating a fresh HTTP client per sub-batch or competing with
user traffic for the same listener.

This applies to Antfly inference chunking as well as embeddings. Today the enrichment
chunker routes `.antfly` configs through `chunking.antfly.chunkText(...)`,
which posts to `{api_url}/chunk`. That is correct for remote Antfly inference, but local
embedded swarm should resolve the same config to an in-process chunker instead.
The existing `.antfly` / `.mock` fixed chunkers are already local and do not
need this migration.

## Shard DB Adapter

`ShardDbAdapter` is the metadata-facing facade for local-or-remote shard DB
operations:

- `fetchMedianKey(group_id)` for split planning.
- `schemaIndexReady(group_id, schema_version, read_schema_version)` for local
  schema progress.
- Future extensions can cover group status, split/merge observations, and
  status snapshots where that reduces direct DB opens.

Implementations:

- **Local data adapter:** preferred long-term path. Uses live `DataServer`,
  provisioned storage, DB caches, and the node runtime.
- **Remote adapter:** calls the node that owns the shard through internal APIs.
  Internal group DB probe routes use the `/internal/v1/groups/:id/db/*` prefix;
  transition routes stay under `/shard-ops/*`.
- **Fallback local adapter:** opens the local group DB directly with
  status/read-only options and the shared `BackendRuntime`.

## Direct DB Open Classification

Production metadata/control-loop DB access should fit one of these categories:

- **Live adapter:** a local data server already owns the shard. Metadata should
  use live runtime status or a `DataServer`-backed `ShardDbAdapter`, avoiding a
  second DB open against the same group.
- **Remote adapter:** the owning shard is on another node. Metadata should route
  through internal group APIs instead of opening local replica paths.
- **Fallback/bootstrap:** no live local data service owns the shard yet, or the
  operation is provisioning, restore, repair, tests, or startup catch-up. Direct
  DB opens are valid here, but they must borrow the node `BackendRuntime` when
  one is available.

Current classification:

- `metadata/shard_db_adapter.zig` `FallbackLocalShardDbAdapter`: fallback/
  bootstrap. It opens status/query-readonly DBs and already accepts a shared
  `BackendRuntime`.
- `metadata/service.zig` schema progress refresh: runtime status first, local
  adapter/fallback second. This remains local-only because schema progress
  records are reported per hosting node. Remote schema readiness should be read
  from published `RuntimeGroupStatusReport.indexes`, not by probing a remote DB.
- `metadata/table_provisioner.zig` `reconcileReplicaRootWithOptions`: fallback/
  bootstrap/provisioning writer. It opens local group DBs to create/restore DBs,
  apply schemas, and reconcile indexes; this is not a live probe and should keep
  borrowing the shared runtime through reconcile options.
- `metadata/table_provisioner.zig` `localRangeHasSchemaVersionIndex`: fallback
  only when no `ShardDbAdapter` is supplied.
- `metadata/service.zig` local group-status collection: live-provider first.
  When a live `DataServer` is registered, metadata calls its
  `LocalGroupStatusProvider` instead of opening group DBs directly. The provider
  returns a cached snapshot immediately and refreshes through the data runtime;
  the refresh path prefers runtime-status snapshots over opening another DB
  handle. Direct DB opens remain only for fallback/bootstrap when no live
  provider exists or when a runtime snapshot is not available yet.
- `metadata/server.zig` median-key lookup: routed `HostedShardDbAdapter` first.
  Local routes dynamically prefer the data server's live adapter when
  registered and otherwise fall back to a direct local DB adapter.
- `metadata/sim_harness.zig` median-key lookup: simulation-only direct DB open.
  It should stay explicit about runtime mode and can later reuse a simulation
  `ShardDbAdapter` if the test harness grows one.
- `api/table_reads.zig`, `api/table_writes.zig`, and
  `api/provisioned_storage.zig`: public data-plane/provisioned-storage DB
  ownership surfaces, not metadata control-loop probes.

## Migration Plan

- [x] Add the design record.
- [x] Add a `ShardDbAdapter` facade for metadata DB probes.
- [x] Move metadata table-provisioning and schema-progress fallback DB opens to
      borrow `BackendRuntime`.
- [x] Route metadata-host median-key fallback through `ShardDbAdapter`.
- [x] Add a `DataServer`-backed `ShardDbAdapter` implementation that avoids
      fallback DB opens when a live local runtime-status cache owns the shard.
- [x] Let data servers register local metadata providers from `start()` after
      construction, so the adapter does not point at a moved by-value temporary.
- [x] Keep runtime status to cheap split-eligibility facts such as doc count,
      bytes, and freshness. Compute the split key lazily only after the
      reconciler decides a group needs a split, then cache that result by group
      and storage generation so repeated planning does not reopen or rescan the
      DB.
- [x] Add a remote `ShardDbAdapter` implementation for DB-level probes that
      should be served by a different node. Median-key probes now route through
      `/internal/v1/groups/:id/db/median-key`, and metadata servers install a
      hosted DB adapter by default. Schema readiness stays status-derived, with
      local adapter/fallback probes only for bootstrap or missing runtime
      status.
- [x] Add direct local Antfly inference dense/sparse embedder, reranker, and chunker
      implementations and route embedded swarm `provider=antfly` /
      `provider=antfly` configs to them.
- [x] Always expose the Antfly inference-compatible public `/ai/v1` API from swarm while
      keeping Antfly-managed local enrichment/query paths on the direct provider
      instead of loopback HTTP.
- [x] Add direct local Antfly inference generator implementations for Antfly managed
      query/retrieval paths that need those providers in-process.
- [ ] Add a swarm regression where enrichment is embedding a slow local batch
      while public index status remains reachable and reports active local
      inference progress.
- [x] Make metadata/public HTTP simulations choose an explicit shared runtime
      mode instead of skipping manual runtimes and silently opening per-DB
      fallback runtimes.
- [x] Audit remaining metadata/control-loop direct DB opens and classify each as
      live-adapter, remote-adapter, or fallback/bootstrap.
- [x] Move local group-status collection off direct DB opens when a live data
      server is present. Metadata now uses `LocalGroupStatusProvider`; the live
      data provider serves cached grouped status immediately and its refresh path
      prefers runtime status over opening the group DB.
- [x] Publish `created_at_millis` and `disk_bytes` through runtime status so
      local group-status conversion can use data-runtime-owned facts instead of
      walking the group directory on the metadata collection path.
