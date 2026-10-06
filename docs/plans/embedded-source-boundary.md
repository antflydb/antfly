# Physical source boundary for embedded Antfly

Status: the structural extraction PRs, including #953, are merged. The local
DB, native lake readers, file CLI and public C API live in `antfly-embedded`;
model execution lives in `inference`; reusable credentials live in `lib/credentials`.
#893 applies Apache package licensing and independent releases to those owners.
The isolated build removes the whole server package. The historical phases below
record the extraction sequence; current ownership is specified in
[embedded source ownership](../design/embedded-source-ownership.md) and
[LICENSING.md](../../LICENSING.md).


## Intended ownership

- `zig/pkg/antfly-embedded`: embedded engine, local database and API surfaces,
  Antfly inference provider adapters, C ABI, Lite CLI, shared local contracts,
  and their build owner.
- `zig/pkg/inference`: native inference engine, host, worker, and CLI.
- `zig/pkg/antfly-server-api`: Apache generated admin/internal schemas and
  public/metadata/auth route interfaces consumed only by the server; it is not an
  embedded dependency.
- `zig/pkg/antfly-client`: generated public HTTP client code and client SDK.
- `zig/pkg/antfly`: ELv2 server orchestration, distributed coordination,
  network APIs, and its build owner. It may import the embedded engine.
- `zig/lib`: independently reusable Apache libraries.

The public `libantfly` ABI, language package names, CLI names, Lite backup
formats, and WASM behavior remain the same.

## Migration

1. Keep shared generated OpenAPI types in `antfly-embedded`, server-only
   admin/internal output and public, metadata, and auth extractors and routers
   in `antfly-server-api`, the public HTTP client in `antfly-client`, and
   inference output in `inference`. Keep the orchestrating
   code generation in neutral `zig/build_support`; move source-text tests with
   their source owners. Done on the licensing branch for those module families.
2. Move the inference host, worker execution, and transport into `inference`.
   Place Antfly provider adapters, remote capability discovery, and endpoint
   context in `antfly-embedded/src/inference`. Keep provider-neutral execution
   control and request types with `inference`, so the host imports only named
   Apache modules; remove the server-side host facade.
   Shared cancellation, cache budget, runtime ABI, diagnostics, template
   content, sparse embeddings, and public request limits get independent
   Apache owners. Done on the licensing branch.
3. Extract storage and local API owners into `antfly-embedded`, replacing
   relative server-to-engine imports with explicit module imports. Move the
   DB-backed managed embedder and shared configuration/template contracts with
   their embedded owners. Keep backup/restore and the C ABI there. Split the
   Raft and hot-standby integrations out of `storage/db/db.zig` before moving
   the local DB owner, as described below.
4. Extract the Apache raft and metadata contracts needed by the local engine.
   Server raft, placement, and metadata control stay in `antfly`.
5. Give `antfly-embedded` an independent build graph for Lite, C ABI, and
   WASM. Remove the Apache per-file exceptions for `zig/pkg/antfly` once no
   Apache owner remains there.
6. Change the staged-source CI check to remove `zig/pkg/antfly` entirely.
   Build Lite, the C ABI, WASM, and inference from that source tree; also
   build the server from the full tree and run focused backup/restore tests.

Moves must not use duplicated source trees or symlinks to preserve old import
paths. Zig rejects relative imports outside a module root, so server-to-engine
references need declared module boundaries as owners move. The existing
license and source dependency checks continue to guard each intermediate
step; passing those checks alone does not establish the final physical
boundary.

## Raft and metadata split

Classify these files by responsibility, not by their current `raft/` or
`metadata/` directory names:

- The generic Raft implementation stays in `zig/lib/raft`. Replica catalog and
  backup/restore bootstrap remain server-owned: they serve distributed replica
  coordination, not Lite. Read-safety interfaces and typed feature reads used
  by the local engine may move with the embedded storage owner. The generic
  read-state observer lives in `lib/raft`; the thin `raft/catalog.zig` re-export
  is gone.
- Move local catalog cloning, index reconciliation, and restore projections
  with embedded metadata and backup code only when an embedded entry point
  actually consumes them. `metadata/provision_contract.zig` remains with server
  provisioning because its current consumers are server-only.
- Keep metadata authority, reallocation, and routing coordination in the server.
  The embedded SQL and transaction records now consume the pure
  `metadata/catalog_route_contract.zig` identity/fence contract, including its
  incarnation stamp; `metadata/api.zig` re-exports that contract. Durable
  `topology_records.zig` table and range records remain shared with local restore.
- Split `backup_cohort.zig` at its pure plan/checkpoint transition and driver
  interface: the reusable state machine remains Apache, while admission,
  metadata locks, Raft persistence, and scheduling are server-owned.
- Raft hosts, transport, placement planning, metadata authority, control loops,
  and HTTP routes remain in `pkg/antfly`. They consume the embedded storage and
  protocol modules rather than reaching into their source directories.

Move one dependency layer at a time, then build Lite, the C ABI, and WASM from
the Apache-only staged tree and run the server read-gate, metadata, restore,
and standalone suites. The boundary check should reject an embedded import
of `pkg/antfly` and a duplicate copy of any moved source file.

The replica catalog remains in `pkg/antfly/src/raft/storage`: it serves
distributed replica bootstrap and restore, not Lite's C ABI. The provisioning
result contract remains in `pkg/antfly/src/metadata`, the read-state
observer in `lib/raft`, and portable filesystem helpers in `lib/runtime`. The
remaining local metadata and backup implementations still depend on Apache
storage/API files under `pkg/antfly`; those dependencies must move before their
consumers can.

## DB and hot-standby separation

The borrowed replication interfaces, replay ingress, local snapshot hooks, and
server runtime test fixtures are separated in #940. Runtime adapters use
`hot_standby_*` names; local engine contracts use generic replication names.
Existing durable keys, wire formats, error identities, and public C ABI symbols remain
compatible. The authored production source audit follows 634 local sources
without entering server coordination. Public C API and private storage-provider
operations now have separate roots and profiles; the complete physical source
move remains pending. Actual named module checks cover native C API and WASM,
and full-suite CI compiles the Apache products with ELv2 implementations
replaced by unconditional compile-time traps.

Current implementation owners (still under `pkg/antfly` until their storage
dependency layers move):

- `storage/db/apply_receipts.zig` owns Raft and replication receipt persistence helpers
  and replay disposition. Mutations still commit their receipt writes under
  their existing apply fence.
- `storage/db/durable_outbox.zig` owns persisted keys, explicit kind tags,
  mutation identity, framing, and checksums. It imports no HA runtime or wire
  record owner.
- `storage/db/durable_outbox_store.zig` owns bounded pending-page reads,
  rolling-upgrade singleton reads, and exact-key clearing. Publication cannot
  reach arbitrary internal KV keys through this interface.
- `storage/db/replication_contract.zig` owns borrowed publisher and write-admission
  interfaces. DB options hold no concrete primary, standby, or fence-store
  handles. Publishers report live identity rather than copying epoch state.
- `storage/db/replication_policy.zig` owns policy data and enums; the hot standby primary
  reexports the same declarations so callbacks retain one type identity.
- `storage/db/replication_effects.zig` owns portable mutation codecs. The hot standby
  effects adapter reexports those declarations and owns runtime log appends.
- `storage/db/replicated_mutation.zig` owns the borrowed normalized batch and
  tagged receipt. `replication_ingress.zig` decodes envelopes and interprets
  provenance before DB execution.
- `storage/server_db_adapter.zig` owns ordered replay policy and group/fence
  snapshot adaptation. Local mutation/receipt transactions, pin recovery and
  snapshot repair safety remain with DB.
- `capi/db.zig`, `capi/handles.zig`, and `capi_embedded_root.zig` own the public
  embedded API and shared local handle state. `capi/server_owner.zig` owns the
  private server operations; integration tests live in `capi/db_test.zig`.
- `storage/db/commit_integration.zig` owns local publication lock ordering,
  pending-record recovery sequencing, and final authority rechecks. Durability
  waits run after releasing local log and transition locks; acknowledgement
  requires reacquiring the transition fence and checking the pinned authority.
- `storage/hot_standby/db_commit.zig` implements the borrowed publisher using
  the primary log, record matching, durability decisions, and metrics. Server
  integrations explicitly validate this adapter before accessing its concrete
  primary. It receives no DB or store handle.
- `storage/hot_standby/sync_wait.zig` owns progress and session waits. Server
  consumers import it directly; the DB/control public facades no longer expose
  runtime wait implementations. DB integration tests may still use them.

Borrowed runtime state must outlive the DB and its in-flight operations. The
interfaces add no allocation and preserve copied option lifetimes. Existing
Apache headers are preserved on extracted code; this phase does not relicense
server coordination. Portable codec and commit-ordering tests run alongside
existing HA integration regressions.

Replay and snapshot/seed adapters and server integration fixtures are now
outside the physical DB production closure. The remaining work is the physical
move of the complete local source closure into `antfly-embedded`. Portable
replication records, atomic receipt persistence and typed replay execution
remain local engine responsibilities. The staged Apache build contains no
hot standby or other ELv2 server implementations. Their earlier Apache-list
entries are removed now that local contracts no longer depend on them.

The intended dependency direction is server replication adapter -> embedded
storage operations. The local DB must not import HA sessions, primary/standby
runtimes, lease watchdogs, or failover coordination. Extracting these owners
does not make HA orchestration part of the embedded package or change its
license. Preserve existing Apache owners for reusable storage contracts; keep
server orchestration ELv2.

### Embedded storage owner

- One mutation executor serves native writes, committed replay, and restore
  import through distinct typed entry points. A trusted replay entry point
  must not become a client-controlled `bypass_replication_write_gate` flag.
- Storage owns atomic persistence of mutations, applied receipts, and pending
  replication effects. Extract codecs and storage operations for the existing
  durable outbox without changing its keys, framing, checksum, or legacy-record
  recovery. Expose typed pending-effect operations rather than unrestricted
  internal KV writes to adapters.
- Keep Raft entry identities, HA applied LSNs, and native operation identities
  separate. Recheck applied receipts under the mutation lock, and persist
  receipts in the same commit as their corresponding durable effects. Do not
  synthesize Raft identities for native writes.
- Storage owns coherent snapshot capture, staged-generation import, validation,
  and local publication. The seed adapter chooses the replication checkpoint
  and supplies its typed manifest/provenance; local backup remains usable
  without an HA runtime.
- Replace concrete HA handles in DB options with a narrow commit integration:
  write admission, a mutation/capture lease, publication of committed pending
  effects, and completion/acknowledgement. Preserve ownership and close-time
  barriers for borrowed callbacks. Engine code must neither inspect standby
  slots nor drive transport to satisfy acknowledgement.

### Server hot-standby owner

Place the DB adapter with the existing server hot-standby implementation,
rather than creating another engine package:

- `HotStandbyPrimaryProgressSyncWait` and `HotStandbySessionSyncWait` live outside
  DB. Primary progress evaluation, standby selection, bounded polling and
  session replication stay with the hot standby runtime.
- The runtime adapter uses `replication_ingress.applyRecord` to decode portable
  records and call typed engine operations for batch/schema/policy/derived
  effects and applied progress. Restore and online-source completion repair
  remain part of an already-applied retry before acknowledgement.
- The HA owner publishes pending effects into its replication log, matches
  previously appended records during recovery, and drives paged retries. The
  engine deletes an exact pending-effect key only after successful publication;
  unavailable or incompatible HA ownership must not silently drop it.
- Keep promotion/demotion, fencing, synchronous durability policy, replication
  transport, slot management, rejoin, timeline changes, seed orchestration,
  Kubernetes leases, and administrative routes with the server runtime.

### Commit ordering and migration

Preserve the current ordering: validate write authority and serialize the local
commit with promotion fencing; persist mutations and pending effects; publish
the ordered replication record; release the DB apply lock and transition lock
before waiting for remote durability; revalidate authority under the transition
lock before reporting success. Do not hold DB locks across network waits or
reduce this protocol to a best-effort post-commit callback. A missing required
mirror remains an error, and incomplete publication continues to gate writes.

Extract receipts/outbox storage first, then the commit integration and HA wait
adapters, then replay dispatch, then snapshot/seed adapters. Move the local DB
only after its imports no longer pull replication runtimes into the engine.
Use the same storage operations from the Raft adapter, without duplicating
mutation, schema, transaction, or restore logic. Keep recovery scheduling and
adapter shutdown explicit so pending work cannot outlive the DB handle.

Validation must cover crash/reopen between local commit and log publication,
idempotent publication and exact-key deletion, legacy and paged outbox recovery,
duplicate replay with unfinished restore/source-pin work, concurrent replay
receipt validation, slow or unavailable standbys, fencing during a synchronous
wait, coherent seed capture during mutations, and DB close during recovery.
Native Lite/C API/WASM builds must succeed without the server HA source tree.

## OpenAPI ownership

Keep `specs/openapi` as the authored schema tree, organized by API surface.
`openapi.yaml` is the joined **public** server API: database/auth, inference,
and extensions. The `/admin/v1` and `/internal/v1` specifications remain
separate because they have different audiences and security contracts; they
should not be folded into the public SDK/documentation spec.

Generated Zig modules follow their consumers rather than the location of the
authored YAML. Embedded database and provider contracts live under
`pkg/antfly-embedded`, the public HTTP client under `pkg/antfly-client`,
inference API contracts under `pkg/inference`, and server-only admin/internal
modules and public, metadata, and auth extractors and routers under Apache
`pkg/antfly-server-api`, imported by the ELv2 server.
Public, metadata, and auth types remain shared because embedded local API
code uses them; generating them twice would create distinct Zig types. Their
server extractors and routers are generated into `antfly-server-api` with an external types-module
import, so the server and embedded code use the same schema declarations.
The neutral `zig/build_support/openapi.zig` orchestrates one deterministic
generation/check step across all outputs. The boundary check must keep Lite,
C API, and inference free of server-only imports.

## SQL runtime integration

The embedded C API consumes the same SQL compiler, executor, scalar/binding
helpers, and integrity mutation planner as the server. These implementations
are explicit Apache exceptions until the engine source migration completes.
Keep the SQL HTTP and pgwire adapters, metadata decisions, owner routing, and
publication workers in the server. The embedded C API adapter remains a
single-handle adapter and does not acquire a server coordinator dependency.

The shared read callback contract uses local graph wire types and a route
budget from `api/routing_budget.zig`. Its graph route-cache binding is an
opaque borrowed server capability; the engine does not import or dereference
the server cache implementation. A pinned read view similarly borrows an
opaque routing-session handle: the server adapter owns and releases its actual
catalog lease, while embedded integrity planning only uses the bound source. Portable seed validation consumes
`system_catalog/portable_policy_contract.zig`, while server catalog projections
and their Raft/metadata dependencies stay in `system_catalog/projection.zig`.

## Server transaction ownership cleanup

The structural prerequisite (#940) separates transaction participant dispatch
and recovery fan-out into ELv2 `storage/server_transaction_*` owners. Their
configuration and test fixtures are also server-owned. The Apache engine keeps
local maintenance configuration, durable intent/receipt application, identity
hooks, and an owned opaque runtime factory whose store adapter stays engine-owned.
Local recovery needs no server participant resolver and retains failed work for
a later bounded pass. Authenticated replica installation is a server adapter;
identity-preserving staging, validation, and repair remain Apache local primitives.
Raft and coordinator fixtures run in a dedicated server suite; authored engine
sources no longer import their test adapter. See `local-db-source-separation.md`
for lifecycle and test ownership details.


### Shared recovery execution owner

Foreground writes and transaction recovery now share one local execution-state
owner for mutable admission, publication, storage settings, statistics and
synchronization. Recovery holds explicit borrowed capabilities and uses a
synchronous view with private scratch state, replacing the copied DB wrapper.
The generic transaction recovery driver belongs to the Apache local source
closure; it owns scheduling, leases, scan cursors, pause/resume and draining.
Server policy validation, participant fan-out and coordinator decisions remain
in ELv2 server owners. The unused core-only and local free-function recovery
shortcuts have been removed; managed one-shot recovery uses runtime dispatch.

## Combined embedded distribution

The current release shape has two primary products: the ELv2 `antfly` server
archive and the Apache `antfly-embedded` archive. The embedded archive ships
`antfly-lite`, `antfly-inference`, `libantfly`, `include/antfly.h`, the private
worker, runtime files and notices. Python/npm packages consume that archive and
install only the library; Go/Rust consumers link the same
library. Separate source owners do not require separate distribution archives.
Release build contract schema 3 records this shape; schemas 1 and 2 retain their
historical artifact layouts and verification rules.

Independent Apache-only Zig fetching is tracked in
[issue #988](https://github.com/antflydb/antfly/issues/988). The package
currently requires a monorepo checkout; a standalone manifest, dependency
closure, and immutable source artifact remain to be implemented.
