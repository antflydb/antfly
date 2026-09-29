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
- `zig/pkg/antfly`: ELv2 server orchestration, distributed coordination,
  network APIs, and its build owner. It may import the embedded engine.
- `zig/lib`: independently reusable Apache libraries.

The public `libantfly` ABI, language package names, CLI names, Lite backup
formats, and WASM behavior remain the same.

## Migration

1. Move shared generated OpenAPI types and code generation into
   `antfly-embedded`; move source-text tests with those files. Done on the
   licensing branch.
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

- The generic Raft implementation stays in `zig/lib/raft`. The local replica
  catalog and backup/restore storage, read-safety interfaces, and typed feature
  reads belong with the embedded storage owner. This includes the current
  `raft/storage/{catalog,backup_restore}.zig`, `raft/{read_gate,feature_reads}.zig`,
  and `raft/state_machine/read_state_observer.zig`. Replace the thin
  `raft/catalog.zig` re-export with direct named-module imports.
- Local catalog cloning, index reconciliation, provisioning results, and
  restore projections belong with embedded metadata and backup code. The
  current `metadata/{local_catalog,local_index_reconcile,provision_contract,
  restore_provisioning_contract}.zig` are the starting set. Move their storage,
  API, and managed-embedder dependencies first so the new owners do not import
  back into `pkg/antfly`.
- Topology wire versions, reallocation requests, incarnation IDs, and mutation
  stamps are currently consumed by server coordination, not the Lite or
  inference import graph. Keep them in `pkg/antfly` unless an actual embedded
  consumer appears. `topology_records.zig` is distinct: its durable table and
  range records are read by local restore code.
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
result contract lives in `pkg/antfly-embedded/src/metadata`, the read-state
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

Generated Zig modules should follow their consumers rather than the location
of the authored YAML. Put embedded database and provider contracts under
`pkg/antfly-embedded`, inference API contracts under `pkg/inference`, and
server-only admin, internal, and authority/route modules under `pkg/antfly`.
Move any types genuinely shared between embedded and server into a small
Apache schema owner instead of copying or generating the same type twice.
The code-generation build support currently in
`pkg/antfly-embedded/build/codegen.zig` orchestrates all three products, so
move that build support to a neutral build directory when splitting generated
destinations. Keep one deterministic generation/check step across all outputs
and assert that Lite/C API/inference never imports server-only modules.
