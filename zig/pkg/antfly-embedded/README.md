# Antfly Embedded for Zig

This Apache-2.0 package provides local databases, SQL and lake readers, and
inference without the Antfly server. Use the versioned
`antfly-embedded-source_<version>.tar.gz` asset from GitHub Releases. Its
`.zig-hash` asset records the Zig package hash; the release checksum ledger
also authenticates the archive bytes.

```sh
zig fetch --save=antfly_embedded https://github.com/antflydb/antfly/releases/download/vVERSION/antfly-embedded-source_VERSION.tar.gz
```

In `build.zig`:

```zig
const dependency = b.dependency("antfly_embedded", .{
    .target = target,
    .optimize = optimize,
});
app.root_module.addImport("antfly-embedded", dependency.module("antfly-embedded"));
app.root_module.addImport("antfly-inference", dependency.module("antfly-inference"));
app.root_module.link_libc = true;
```

The embedded module exports `db`, `api`, object storage, and `lake`. Local
`.aflite` and directory storage and database-free native inference use the
same Apache source owners as the C API. Defaults disable optional accelerator
backends; opt in with `metal`, `cuda`, `pjrt`, or `onnx` dependency options
when their target/runtime prerequisites are available. Supply an absolute
`onnx-root` for an external ONNX Runtime installation. `blas=auto|linked|off`
and `blas-root` control CPU acceleration; `cuda-artifacts` selects the CUDA
bundle format. These options are passed to the shared composition rather
than requiring files to be added to the fetched package cache.

The package also exposes the native `antfly` shared-library artifact
(`libantfly`), `antfly-lite`, and `antfly_wasm`. The browser artifact contains
both embedded database and inference APIs. Use `dependency.artifact(...)`
with `b.addInstallArtifact` to install a selected product. Standalone source
package commands include `zig build lite`, `capi`, `wasm`, and `wasm-test`;
the default builds native Lite and its C API. `zig build inference-wasm`
builds database-free browser inference, with `-Dwasm-memory-model=wasm64`
selecting memory64 (the default is wasm32). `wasm` builds the combined
embedded database/inference wasm32 bundle. Both browser owners reuse their
authored build profiles; they exclude native cloud credentials and server
coordination. Zig 0.17.0 or later is required. Native builds retain the
existing supported Linux and macOS targets; optional accelerator runtimes
must be supplied by the consumer.

The source archive includes the complete first-party Apache composition and
shared runtime sources, generated contracts, pinned dependency manifests,
source provenance, and third-party notices. It contains no ELv2 server or
server test sources. Repository-wide corpora, benchmarks and CI tooling are
excluded; compile-time embedded assets are included individually. Optional
backend artifacts remain available for consumers enabling acceleration.
Third-party dependencies retain their own licenses.
A downloaded package has no dependency on a monorepo checkout. Development
checkouts can use the same package entry point under `zig/pkg/antfly-embedded`.

## Graph metric maintenance

Active writable native databases automatically maintain configured graph metrics in
bounded background ticks. Background metrics refresh after graph updates;
manual metrics build after refresh or rebuild requests. Queued work resumes on
reopen. The default combined runtime uses a fresh incarnation identity and a
durable ownership lease. Read-only, standby, and hidden restore owners retain
their existing background-worker gates. Databases without configured metrics
do not start a graph metric worker or acquire its lease. Adding the first
metric starts maintenance; removing the last metric parks the worker and
releases its lease.

Callers that drive graph maintenance themselves can open with
`.graph_metric_maintenance = .{ .start_background_loop = false }`. The existing
`start_index_workers`, `start_optional_runtimes`, and
`start_optional_runtime_workers` controls also apply. Explicit coordinator and
worker configurations retain their supplied identities and budgets.

## Source organization

The implementation lives directly under `src/`: `storage/` contains the local
DB, `api/` its operations and contracts, `metadata/` local catalog contracts, and
`inference/` provider integration. SQL, search, graph, lake readers and C API
implementations share this Apache owner. `src/root.zig` and the configured
`src/engine/` modules expose the public Zig API; `src/source_catalog.zig` is an
internal bridge for server consumers. See the [ownership design](../../../docs/design/embedded-source-ownership.md)
for the embedded/server boundary.
