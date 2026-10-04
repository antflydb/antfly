# Embedded and server source ownership

The local DB and its dependency closure live in `zig/pkg/antfly-embedded/src/local`.
The server consumes this implementation through the internal `antfly_local_sources`
module. This refactor preserves existing licenses; the Apache/ELv2 licensing and
release changes belong to the separate licensing PR.

## Physical owners

| Owner | Responsibility |
| --- | --- |
| `pkg/antfly-embedded/src/local` | DB, WAL, LSM, indexes, search, graph execution, local transactions, local backups/restore, SQL and decision-function evaluation, Lite, public C API and file CLI |
| `pkg/antfly-embedded/src/inference` | Antfly inference providers and embedding integration |
| `pkg/inference` | Model execution, inference host and native provider exports |
| `pkg/antfly/src` | HTTP handlers, distributed transactions, Raft coordination, cluster metadata, hot standby, server storage-owner adapters and private C API |
| `pkg/antfly-embedded/build` | Local storage profiles, public C API, native provider archives and browser build |
| `build_support/antfly` | Shared module composition, dependency configuration, runtime contracts and test collection |

Portable local transaction receipts and replication records remain with the DB.
Raft application, hot-standby lifecycle, and durability policy remain server features
implemented by adapters. The local ports contain borrowed or captured write admission,
generation pinning, commit requirements, and publication/completion callbacks.
Policy, waits, and telemetry stay in the server adapters; copied captures retain
configuration by value while their pointer and slice targets remain borrowed.
Backup artifact decoding and local restore staging are local operations;
coordinated snapshot publication and cluster restore remain server operations.
Table-drop cleanup fences are shared local contracts. Metadata protocol activation,
membership barriers, and reallocation requests remain server coordination.
Existing shared contracts and generated OpenAPI ownership remain in their existing
embedded/shared-library/server-API packages. Authored YAML remains in `specs/openapi`.

`source_catalog.zig` is an internal bridge for server consumers, not the public
embedding API. `embedded_root.zig`, Lite bindings and the public C header remain
the product entry points. Server adapters can use the local implementation, but
production local code cannot import the server tree. Test-only fixture capabilities
are supplied explicitly through a named module and share the consumer's types.

Files under `src/local/` omit the redundant `local_` prefix. The query execution
contracts live in `api/query_execution_contract.zig`, separate from the public
query contracts in `api/query_contract.zig`. Import aliases can retain `local_`
when they distinguish these implementations from server coordination at a call site.

## Independent products

From `zig/`, build the local products with:

```sh
zig build --build-file embedded.build.zig lite capi-smoke embedded-capi-check -Dmetal=false
zig build --build-file embedded.build.zig wasm-test -Dmetal=false
```

The public library compiles its local C API directly and links native inference
and enrichment compute archives. It does not reuse the server storage archive or
link server runtime entry points. The server compiles the same authored sources
under its storage profile and keeps its own private adapters.

The independent CLI supports file-oriented Lite commands. The server command
wrapper supplies the `lite serve` callback; its behavior remains available through
the server product. The independent executable retains the hidden inference worker
entry point for process isolation. Public executable names and release packaging are unchanged
by this refactor.

The staged-build check removes the entire server package before compiling the
CLI, public C API, native boundary and WASM products:

```sh
python3 tools/check_embedded_isolated_build.py -- -j1
```

This runs in the existing full-suite CI path. Import-boundary audits separately
reject a production local module graph that resolves any server source. PDF
products obtain their font assets from the canonical TypeScript design-system
fonts through a generated module, rather than importing server UI assets.

## Test ownership

Zig collects tests from the main compilation module, not named dependencies.
Moving local implementations behind `antfly_local_sources` therefore requires a
local test partition alongside each server consumer that owns local tests.
`test_partitions.zig` discovers the local roots referenced by the server test
surface, clones its configured module graph, and makes the local catalog the
main compilation module. Late-bound options and fixture type identity are shared
within that graph. Compiler filters are retained. Control-only profiles omit inactive
physical DB imports; generation-publication tests belong to the physical owner,
including the portable lifecycle helpers borrowed by control facades.

Runtime selection and ownership audits see the union of both inventories before
execution. A filter can match either owner, while missing filters, empty runnable
selections and duplicate ownership still fail. Explicitly allowed empty selections
retain their existing behavior. Once the union is validated, an individual owner
may be empty even under `--require-no-skips`; selected tests still cannot skip.
Inventory listing includes both owners and never treats listing as execution.
Calling finalization twice does not duplicate
partitions or inventories. Small real-build fixtures cover these contracts in
`tools/test_local_test_partitions.py`; the normal product suites exercise the
actual local/server fixture graph.
