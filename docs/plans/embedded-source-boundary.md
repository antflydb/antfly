# Physical source boundary for embedded Antfly

The Lite, embedded C ABI, and inference products are Apache-2.0, but their
build still reads Apache files from the mixed `zig/pkg/antfly` server package.
The source boundary is complete when the Apache products build with the entire
`zig/pkg/antfly` directory absent, and the server imports the embedded engine
through declared Zig modules.

## Intended ownership

- `zig/pkg/antfly-embedded`: embedded engine, local database and API surfaces,
  C ABI, Lite CLI, shared protocol types, and their build owner.
- `zig/pkg/inference`: inference host, worker, and CLI.
- `zig/pkg/antfly`: ELv2 server orchestration, distributed coordination,
  network APIs, and its build owner. It may import the embedded engine.
- `zig/lib`: independently reusable Apache libraries.

The public `libantfly` ABI, language package names, CLI names, Lite backup
formats, and WASM behavior remain the same.

## Migration

1. Move shared generated OpenAPI types and code generation into
   `antfly-embedded`; move source-text tests with those files. This part is in
   progress on the licensing branch.
2. Extract storage and local API owners into `antfly-embedded`, replacing
   relative server-to-engine imports with explicit module imports. Keep
   backup/restore and the C ABI in the embedded owner.
3. Extract the Apache raft and metadata contracts needed by the local engine.
   Server raft, placement, and metadata control stay in `antfly`.
4. Move the inference host and worker into `inference`; the server retains
   only its provider adapter and worker supervision policy.
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
