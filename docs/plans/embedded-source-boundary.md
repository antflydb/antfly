# Physical source boundary for embedded Antfly

The Lite, embedded C ABI, and inference products are Apache-2.0, but their
build still reads Apache files from the mixed `zig/pkg/antfly` server package.
The source boundary is complete when the Apache products build with the entire
`zig/pkg/antfly` directory absent, and the server imports the embedded engine
through declared Zig modules.

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
   their embedded owners. Keep backup/restore and the C ABI there.
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
