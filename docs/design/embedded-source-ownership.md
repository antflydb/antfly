# Embedded and server source ownership

The local DB and its dependency closure live in `zig/pkg/antfly-embedded/src/local`.
The server consumes this implementation through the internal `antfly_local_sources`
module. The embedded and shared owners are Apache-2.0; the server coordination owner
remains ELv2. See [LICENSING.md](../../LICENSING.md) for authoritative package
classification and third-party exceptions.

## Physical owners

| Owner | Responsibility |
| --- | --- |
| `pkg/antfly-embedded/src/local` | DB, WAL, LSM, indexes, search, graph execution, local transactions, local backups/restore, SQL and decision-function evaluation, portable lake readers, Lite, public C API and file CLI |
| `pkg/antfly-embedded/src/inference` | Antfly inference providers and embedding integration |
| `pkg/inference` | Model execution, inference host and native provider exports |
| `lib/credentials` | Credential-source identities and native AWS discovery/cache shared by lake, backups and Bedrock |
| `pkg/antfly/src` | HTTP handlers, distributed transactions, Raft coordination, cluster metadata, hot standby, server storage-owner adapters and private C API |
| `build_support/embedded` | Local storage profiles, public C API, native provider archives and browser build |
| `build_support/antfly` | Shared module composition, dependency configuration, runtime contracts and test collection |

Portable local transaction receipts and replication records remain with the DB.
Raft application, hot-standby lifecycle, and durability policy remain server features
implemented by adapters. The local ports contain borrowed or captured write admission,
generation pinning, commit requirements, and publication/completion callbacks.
Policy, waits, and telemetry stay in the server adapters; copied captures retain
configuration by value while their pointer and slice targets remain borrowed.
The local `storage/db/execution_resources.zig` owns the canonical shared execution
state and result types. `local_mutation.zig` composes request preparation,
authoritative commit, and derived materialization without runtime dispatch.
Foreground writes and synchronous recovery share those operations; recovery
borrows stable resources and owns only invocation scratch. DB retains opening,
closing, snapshot admission, durable commit ordering, and resident scheduling.

Server upload retry policy lives in `storage/artifact_upload_recovery.zig`.
`server_coordinated_ttl.zig`, `server_query_visibility.zig`,
`server_document_child_range.zig`, and `server_group_metadata.zig` own routing,
placement, and group coordination. Embedded owns their local observations,
durable outboxes, and borrowed integration ports, without importing these adapters.
Backup artifact decoding and local restore staging are local operations;
coordinated snapshot publication and cluster restore remain server operations.
Table-drop cleanup fences are shared local contracts. Metadata protocol activation,
membership barriers, and reallocation requests remain server coordination.
Native SQL owns its pull stream, typed execution batches, parallel scheduling,
spill operators, and result cursors in `src/local/sql`. Row-source value/identity
contracts and external-table schema bindings belong to the local engine.
Embedded lake querying is an intentional product capability, similar to using
DuckDB against files and object storage. Portable Parquet/Iceberg readers,
inventory discovery and validation, snapshot pinning, delete application,
row identities and continuation checks, bounded caches, parallel scans and SQL
cursors live in the local source owner. Sidecar and row-fragment codecs required
by those readers are portable data operations, not cluster coordination.

The native Zig package exposes these capabilities through `antfly_embedded.lake`.
The public package, C API, file CLI and native reader tests use the same embedded
dependency composer. Consumer regressions import the public package and open a
host-resolved source, scan its SQL cursor and compile SQL. Native boundary checks
include both the public package and C API owners.
Its `sql_cursor` implements the shared SQL catalog cursor contract; callers can
use it from their SQL backend without an HTTP server, Raft group or replica.
`lake.host.OpenOptions` accepts a borrowed resolver for managed credential
references. The resolver is needed only while opening a source; a successful
open transfers the returned object-store owner to the source. Source close
releases that owner. With no resolver, ordinary file/cloud URIs use the portable
object-store implementation. The server resolves node connections and secrets
in `configured_object_store_support.zig`, then supplies that port. It uses a thin
`api/lake_sql_cursor.zig` adapter over the same cursor implementation.

AWS discovery and ref-counted immutable credential snapshots live in
`lib/credentials/src/aws.zig`, independent of any inference provider. Bedrock,
lake readers, backups and server connection validation consume that owner.
Bedrock retains type aliases for compatibility. Cache leases keep credentials
alive across refresh and cache shutdown; absolute deadline/cancellation context
continues across credential discovery and provider dispatch. Browser identity
contracts do not expose native AWS discovery. The extracted implementation is Apache-2.0, along with its shared credentials
owner; server secret resolution and managed-connection policy remain ELv2.

Google authentication already follows this split: `lib/google/src/auth.zig`
owns ADC/service-account discovery, token minting, refresh and caching. Vertex
and GCS choose their scopes and consume that shared owner. API-key adapters
receive resolved keys and format service authorization headers. Antfly secret
references, connection configuration and provider resource lifetimes remain in
their host/integration owners; credential-source identities remain shared in
`lib/credentials`. Extract generic authentication mechanics when another service
needs them, while keeping service scopes, request signing and header conventions
with the appropriate protocol adapter.

Cluster placement, distributed query orchestration, catalog publication,
sidecar build coordination, managed credential policy and HTTP serving remain
server-owned. `ProvisioningProjection` and restore source-artifact provisioning
DTOs also remain server metadata; shared table-record memory helpers belong to
local metadata. Inventory and portable manifest descriptors may be shared even
when they describe externally stored data. Their presence does not grant local
code responsibility for cluster publication.

Parquet and Iceberg readers are implemented; the Lance metadata tag does not
promise a Lance reader. This PR adds a native Zig capability, not a new C ABI,
Python/npm command or stable convenience wrapper. Browser lake scans require a
separate asynchronous host-I/O integration and are not exposed by the current
WASM surface. The existing browser DB and inference source boundary remains
independent of native lake I/O. The SQL refinement benchmark follows the local
execution owner; Lake serving benchmarks remain server compositions.
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
zig build --build-file embedded.build.zig embedded-lake-test embedded-package-test aws-credentials-test -Dmetal=false
```

The public library compiles its local C API directly and links native inference
and enrichment compute archives. It does not reuse the server storage archive or
link server runtime entry points. The server compiles the same authored sources
under its storage profile and keeps its own private adapters.

The independent CLI supports file-oriented Lite commands. `lite serve` is
removed from both CLIs; use `antfly standalone --storage-engine lite
--storage-path app.aflite` for HTTP serving through the ELv2 server. The independent executable retains the hidden inference worker
entry point for process isolation. The Apache `antfly-embedded` archive contains both public CLIs, `libantfly`,
`antfly.h`, the private worker, runtime files and notices. It is the embedded
release product alongside the separate ELv2 server archive. Embedded language packages ship their native library without a worker; public command installation belongs to the CLI distributions.

The staged-build check removes the entire server package before compiling the
CLI, public C API, public Zig package, AWS credentials, native lake and WASM products.
The C ABI conformance runner translates the canonical public header at build
time and executes the shared cases from the embedded package. Both C smoke and
conformance run without any server sources:

```sh
python3 tools/check_embedded_isolated_build.py -- -j1
```

This runs in the existing full-suite CI path. Import-boundary audits separately
reject a production local module graph that resolves any server source. PDF
products embed the reviewed Apache-2.0 Roboto fonts through a generated module,
independently of server UI fonts. The asset manifest validates their byte identities
and upstream notices.

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
Aggregate ownership follows the contract being tested. The coverage/status owner
collects the complete runtime-status contract suite; table-write and graph
consumers borrow it. HTTP transport contracts stay with the HTTP runtime owner,
while SQL execution and session contracts stay with public API parity. Lake's
sidecar selection does not collect general engine or segment contracts. These
exclusions apply only to aggregate runs; focused targets retain their selections.
Calling finalization twice does not duplicate
partitions or inventories. Small real-build fixtures cover these contracts in
`tools/test_local_test_partitions.py`; the normal product suites exercise the
actual local/server fixture graph.

## Licensing boundary

The entire first-party local engine and native lake dependency closure is
Apache-2.0. Server credential adapters, provisioning, Raft, hot standby and cluster
publication remain ELv2. Third-party sources and assets retain their upstream
licenses; package ownership does not replace those notices. The header policy,
source dependency audit and asset manifest enforce this classification.

Future changes must extend the isolated embedded build and tests when they add a
local reader or host integration, rather than importing the server query barrel.
The full suite builds without the entire server package and validates native,
C ABI conformance and browser consumers. License and release packaging checks
validate the canonical notices shipped with each product.

Build composition and test partitioning use Zig 0.17 configuration APIs.
Generated inputs retain LazyPath ownership until make phase; authored imports
inspected during configuration are registered as configure dependencies so the
serialized build graph is invalidated when source ownership changes.
