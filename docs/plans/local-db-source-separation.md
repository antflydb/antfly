# Local database source separation

This refactor targets main before the Lite/inference licensing and release PR
(#893). Existing source licenses remain unchanged; moved files keep their
original headers. License-header tooling records ELv2 files that moved into
otherwise Apache directories. Package naming, artifact publication, and CLI
behavior changes belong to #893.

## Source owners

- `zig/pkg/antfly-embedded` owns embedded facades, shared schema types, and
  Antfly Inference provider adapters (including Vertex and Bedrock).
- `zig/pkg/inference/src/host` owns inference host/worker implementation and
  portable request/execution contracts.
- `zig/lib/runtime` owns borrowed runtime ABIs, cancellation, cache budgets,
  filesystem helpers, and threaded I/O limits.
- `zig/pkg/antfly-server-api` owns generated server routers and extractors;
  shared generated types live in embedded. Authored schemas stay in
  `specs/openapi`, with the generator importing shared type modules.
- Local API helpers and catalog/index reconciliation contracts remain under
  `zig/pkg/antfly` until the local database owner can move as a complete unit.
  Server replica catalogs, provisioning summaries, and coordination remain
  with the server.

## Database and hot standby contracts

`storage/db` owns apply receipts, durable outbox storage, replication policy
values, effect codecs, and publication sequencing. Borrowed publisher and
write-gate interfaces keep concrete hot standby runtimes outside DB production code.
`storage/hot_standby` supplies the primary, standby, fencing, policy/metrics,
and synchronous wait adapters. These interfaces preserve durable frame formats
and acknowledge writes only after publication/wait and authority revalidation.
Writer-cache identity includes adapter identity and borrowed lock identity.

## Engine boundary

DB executes normalized replicated mutations with one tagged receipt; wire
versions and the eight Raft provenance alternatives stay in ingress adapters.
Ordered entry retry, lifecycle admission, and group/completion-fence snapshot
translation live in `storage/server_db_adapter.zig`. Local receipt writes,
source-pin recovery, snapshot pins, repair proofs, and range finalization retain
their existing store transactions and apply fences.

Native Raft snapshot wire transport lives in `raft/storage/native_snapshot.zig`
and runs in the server Raft storage test suite. The physical DB exposes
`OrderedApplyReceipt`, ordered mutation admission, and local snapshot document
replacement, staging, and repair primitives. Server callers use these generic
primitives through the server adapter;
compatibility method aliases have been removed. Durable term/index bytes,
persisted keys, and error identities remain unchanged.

`storage/db/replication_ingress.zig`
owns envelope decoding and temporary payload allocation. Apply receipts remain
atomic with primary mutations and derived effects. The engine also owns record
and effect formats, durable outbox recovery, and local snapshot maintenance.
The runtime library owns the shared/exclusive mutation barrier. Hot standby
adapters own seed capture, authenticated replica restore coordination, and
remote acknowledgement waits.

Runtime names use `hot_standby_*` or `HotStandby*`; engine contracts use generic
replication names. Existing persisted key bytes, record encodings, error names,
and public C ABI symbols remain compatible. Private maintenance enum values
remain unchanged. The private storage-provider apply symbol is
`antfly_storage_owner_apply_hot_standby_replication_record`; provider and consumer
archives compile against that name together. Server runtime integration tests
live under `storage/hot_standby`, with test-only hooks for white-box engine
assertions. Production DB sources do not import those fixtures or runtimes.

The public C ABI has an independent root (`public_capi_root.zig`) and dependency
facade (`capi_embedded_root.zig`). `capi/db.zig` owns its public exports;
`capi/server_owner.zig` owns private storage-provider operations. Shared local
handle state and the single handle registry live in `capi/handles.zig`. Server
context, transaction recovery, and runtime hooks are opaque handle lifetimes
with cleanup/release callbacks, preserving teardown order without importing
server implementation types into public handles. Integration tests live in
`capi/db_test.zig`, outside the production public source closure.

Borrowed read consistency, routing deadlines, and table read callbacks do not
import server quorum trackers or concrete routing sessions. Server join providers
own those sessions and expose borrowed opaque state through their callbacks.
The browser source profile contains DB and local query owners without native
writer configuration. Portable restore and transaction tracing have local
owners; remote backup-location handling and Raft trace adaptation stay outside
the browser profile. The local inference provider adapter lives in storage;
standalone keeps a compatibility facade over the same implementation.

`zig build embedded-source-boundary-check` follows authored imports from the
embedded root, physical DB, and public C API, excluding test-only owners.
`embedded-native-module-boundary-check` and `embedded-wasm-module-boundary-check`
resolve named imports against each target's actual `Build.Module` import tables.
They retain unknown conditional branches and select only conditions proven by
the target or its generated build options. Native public C API object compilation
is independently available through `zig build embedded-capi-check`.

Feature-disabled early returns exclude later statements only when the return
is unconditional within its enclosing block. An unbraced runtime condition or
loop cannot hide reachable imports. Generated Antfly modules are audited even
when the compiler writes them outside the checkout. The build explicitly marks
dependency-owned modules using their owning builder, and the audit retains
their declared imports back into Antfly sources.

`python3 zig/tools/check_embedded_isolated_build.py` stages the working source
inputs with server coordination and private C API implementations replaced
by unconditional compile-time traps, then compiles the
public C API and complete WASM artifact and checks their module graphs. It runs
in `zig-full / x86_64` on main merges/full validation, not as a new per-PR gate.
The traps satisfy Zig’s cache scans of dormant test imports; any live server
import fails compilation and the module audit independently rejects its owner.

Local index reconciliation has its own result summary; server provisioning
keeps group/root counts separately. Local range observation limits and catalog
route identity are portable contracts. Server catalog command envelopes stay
with server coordination. Backup materialization stays with local storage;
replica-catalog restore admission stays under server Raft storage.

## Transaction and restore ownership

`storage/server_transaction_dispatch.zig` owns participant identity validation,
coordinator selection, and ordered transaction dispatch. Local schema validation
and durable intent/receipt application remain in the storage engine.

`storage/server_transaction_recovery.zig` owns participant fan-out, coordinator
acknowledgements, and replicated metadata cleanup. Its server configuration is
separate from the engine's local maintenance configuration. The engine accepts
an optional factory that creates an owned opaque runtime, binds local intent
resolution and identity hooks, forwards lifecycle operations, and releases the
runtime before its borrowed store and contexts. The server snapshots its
configuration during initialization; the factory context need only survive
initialization, while callback contexts survive the DB. Local maintenance can
run without a participant resolver, preserves distributed pending decisions,
and retains failed local resolutions for a later bounded pass.

The local and server policies instantiate the same
`storage/db/maintenance/transaction_recovery_driver.zig` lifecycle. It owns
bounded scan cursors, clocks, leases, scheduling, pause/resume, statistics, and
worker draining. Only policy validation and the bounded recovery pass differ;
participant callbacks and coordinator decisions remain server-owned. Managed
one-shot recovery uses the runtime's policy dispatch; the unused core-only
one-shot shortcut has been removed.

Foreground writes and recovery share one heap-owned `LocalExecutionState` for
admission, publication, mutable storage settings, visibility statistics, and
synchronization. The stable recovery context retains explicit borrowed core,
executor, runtime and immutable open resources. Each resolution constructs a dedicated
`LocalMutationExecution` receiver with private scratch state; it never creates
a temporary DB wrapper. The receiver shares the local mutation implementation
with foreground execution, fixes synchronization to `.propose`, and exposes no
DB ownership or resident scheduling operations. Provider replacement is fenced
while the receiver borrows providers. Resident retry scheduling remains with
the serving owner.
Recovery joins before shared publication caches or execution state are freed.
Namespace and split shadow changes remain visible through their existing shared
owners and apply fences.

The server restore adapter owns authenticated replica installation entry points.
The DB exposes identity-preserving installation, namespace checks, staging,
validation, and repair without importing the seed coordinator.

Raft command and coordinator recovery fixtures live in
`storage/server_db_integration_test.zig` and run in `antfly-server-db-test` and
the normal unit aggregate. Their white-box hooks are available only in test
builds. Local maintenance regressions remain owned by the storage engine shard;
`antfly-local-transaction-recovery-test` provides a focused run.

## Local mutation, collection, and read owners

`storage/db/local_mutation.zig` owns the shared local preparation and execution
algorithms used by foreground DB writes and synchronous recovery. Its compile-time
resource binding supplies the existing local state, codec, result and runtime
types; it neither imports the owning DB wrapper nor selects a server runtime.
The borrowed `Context` captures stable resource pointers, and each invocation
constructs an `Execution` with private scratch. Prepared-row workers also live
in this owner, including their pinned catalog inputs and shared-region lifetime.
The DB forwards existing entry points and retains ownership, resident scheduling,
caller acknowledgement, and the ordered read/commit admission boundaries.
There is no runtime dispatch or extra allocation from the compile-time binding.

`storage/db/replay_vector_collectors.zig` owns dense/sparse replay collectors,
artifact identity decoding, sparse field reads, and result destruction. Numeric
payloads and artifact keys retain their original borrowed lifetimes; cloned
identities remain owned. Tombstone compaction preserves the allocation length
needed to free the original array and does not turn deleted inputs into upserts.

`storage/db/graph_field_plan.zig` uses one builder for live and pinned-snapshot
field-derived graph writes. It allocates both merged output arrays before
changing either extracted result. Errors leave the original contributions
intact; successful publication transfers element ownership once. The shared
`storage/db/owned_keys.zig` reserves list capacity before cloning a key, so list
growth cannot leak a clone. Allocation sweeps cover both planners, existing
contributions, empty field targets, deduplication, and public extraction with an
actual configured graph index.

`storage/db/relational_read_session.zig` owns retained relational readers,
policy leases, proofs, cancellation/deadline checks and filter destruction.
`storage/db/read_projection.zig` owns artifact projection and transaction/column
materialization from a borrowed local core and pinned read transaction. DB still
acquires the admitted snapshot, validates generations and holds statement/read
fences; projection contexts retain no DB pointer. Document sessions retain their
existing owner in `document_rows.zig`.

The authored and resolved native/WASM boundary checks cover these owners. Their
existing ELv2 headers are preserved until the licensing PR applies the Apache
classification to the full embedded source closure.

## Remaining separation

The physical DB and its complete local source closure must still move into
`antfly-embedded`. This physical move is deferred to keep this refactor from
widening its merge-conflict surface. The local source owner now uses shared APIs directly rather
than server facades. Public C API and private server operation ownership are now separate. Keep
the isolated native/WASM checks passing throughout the physical package move.
The licensing PR applies Apache classification to the local source closure;
this structural PR preserves existing source licenses.

## Review and merge order

Merge this refactor into main first. The licensing PR applies its Apache
boundary, packaging, and release changes on top. Its source moves and DB
refactors should then disappear from its diff against main.
