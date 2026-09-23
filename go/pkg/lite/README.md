# Antfly Lite Go Binding

`go/pkg/lite` is the first language binding above the stable Zig APIs and
the `libantfly` C ABI. It wraps the Lite open/storage profile in that ABI, so
applications embed a live `.aflite` database directly instead of talking to the
network SDK.

The Go module includes the matching `antfly.h` C ABI header. Applications
still need `libantfly` at build and runtime. From the source tree, build the
C library before running cgo-backed tests:

```sh
cd zig
zig build capi
cd ../go/pkg/lite
go test -tags libantfly ./...
```

Outside the source tree, install an Antfly CLI release package or archive that
contains `include/antfly.h` and `lib/libantfly.*`, then point cgo and
the dynamic loader at that installation when building your app. For example:

```sh
CGO_LDFLAGS="-L/path/to/antfly/lib" \
LD_LIBRARY_PATH="/path/to/antfly/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
go build ./...
```

On macOS use `DYLD_LIBRARY_PATH` instead of `LD_LIBRARY_PATH` when the library
is not already on the loader search path.

### Embedded inference

`libantfly` always links the standalone inference runtime in-process, the
same as the `antfly` executable (see `zig/COMPILATION.md`'s "C API
composition" section and `zig/LITE.md`'s "Local Embedded Inference"
section). Setting `LocalRuntimeConfigured` (below) yields `local_embedded`
behavior -- an embedded model runtime that runs chunker, embedder, and
extractor producers configured with `"provider": "antfly"` and no `api_url`
locally instead of failing or requiring a remote URL -- with the standard
library and no extra link flags. This makes `libantfly` a much larger shared
library than before inference was embedded by default; there is no smaller
inference-free variant to link against instead.

Normal `go test ./...` does not run the C ABI smoke test. The `libantfly`
build tag means "a built `libantfly` is available to link"; it is not
Lite-specific, because the C ABI itself is storage-neutral. Without it, test
binaries would fail at link time, so package consumers and repository-wide
`go test ./...` runs do not need a freshly built `libantfly` unless they are
testing the binding against the source-tree C library.

The open helpers call `ValidateABI` before filling C option structures or
creating handles. Applications can call `ValidateABI` at startup to fail fast
when the loaded `libantfly` ABI version or `antfly_open_options` size
does not match the header used to build the Go binding.

The binding exposes raw JSON methods such as `StatusJSON` and `CapabilitiesJSON`
for parity with the C ABI. It also exposes typed `Status` and `Capabilities`
helpers for stable Lite control fields, including storage identity, inference
mode, caller-supplied artifact support, and distributed-only capability flags.
Use constants such as `InferenceModeCallerSuppliedArtifacts`,
`InferenceModeManualMaintenance`, and `InferenceModeDisabledDeferred` when
branching on inference status or capabilities.
Typed `PendingWorkStats`, `RunUntilIdleStatus`, `Check`, `Compact`, `Vacuum`,
`CopyStableSnapshot`, and `CopyStableSnapshotFile` helpers cover the stable
Lite maintenance reports while keeping the raw JSON methods available.
`ReplayGeneratedEnrichments` recreates generated enrichment work from stored
documents after a manual-maintenance or restore pause.
Use `CheckFile` or `CheckFileJSON` to inspect an invalid, truncated, or
corrupted `.aflite` file without opening a database handle.

Use `Create` for a new native `.aflite` writer database and `Open` for an
existing native `.aflite` writer database. `Open` does not create missing files
or upgrade pre-release Lite layouts; unknown or invalid files fail explicitly.
Every `Create*` variant provisions the default `full_text_index_v0` full-text
index, matching the server's table-create behavior, so `AddIndexJSON` is only
needed for indexes beyond that default.
Use `OpenReadonly` for read-only query handles and `OpenStatusOnly` for
inspection. Use `CreateHosted` for a new hosted/manual-maintenance database and
`OpenHosted` for an existing hosted/manual-maintenance database when the
application will call `RunUntilIdle` itself. Use `RunUntilIdleStatus` when the
application also wants the typed post-drain pending-work readiness document.
Use `CreateWithOptions` and `OpenWithOptions` for advanced settings such as map
size, native-profile TTL cleanup, and explicit inference status reporting. Set
`RemoteProviderConfigured` when the embedding producer is backed by a configured
remote provider so `Status().Inference` reports `remote_provider` instead of the
default caller-supplied/deferred mode. Set `LocalRuntimeConfigured` when the
application requests a local inference runtime; Lite reports `local_embedded`
whenever the loaded build advertises `LocalInferenceRuntime`, which is true
by default for the standard `libantfly` (see "Embedded inference" above).

**Worker executable resolution.** GPU-hosted and driver-backed backends
(Metal, CUDA, ONNX, PJRT) construct and, for Metal/CUDA/PJRT, execute models
in a separate, replaceable worker process rather than inside the Go process
-- crash containment for an unabortable driver call or GPU state corruption
means the process that made the call must be the one that gets killed and
respawned, and that must never be the Go host. Unlike the `antfly` CLI (which
re-execs `argv[0]`, itself), a Go binary linking `libantfly` has no
`antfly`-shaped `argv[0]` to re-exec, so the runtime resolves the worker
executable itself, in order: the `ANTFLY_INFERENCE_WORKER` environment
variable (a path to the worker executable, typically an `antfly` binary);
otherwise an `antfly` binary next to the loaded `libantfly`; otherwise
`antfly` on `PATH`. In a build that includes Metal (the macOS default), CUDA,
ONNX, or PJRT, opening a `LocalRuntimeConfigured` handle spawns the worker, and
all local inference, CPU models included, runs there. If none of these
resolve, the spawn fails with a clear error naming `ANTFLY_INFERENCE_WORKER` --
set it (or place an `antfly` binary next to `libantfly` or on `PATH`) before
opening the handle. See
`zig/LITE.md`'s "Local Embedded Inference" section for the full resolution
order and rationale.

### Concurrency

A `*DB` is safe for concurrent use by multiple goroutines, like `*sql.DB`;
share one handle rather than opening one per goroutine. `libantfly` runs in
serialized threading mode (`ThreadingMode() == ThreadingSerialized`): reads
such as `SearchJSON`, `LookupJSON`, and `ScanJSON` run in parallel with each
other and with writes, `Batch` and transaction calls on one handle queue
instead of failing with `Busy`, and schema or index changes wait for in-flight
calls. `Close` waits for in-flight calls; calls after it return
`InvalidArgument`. See `zig/CAPI.md` "Thread Safety" for the full contract.

Only one writer handle may be open per file at a time, across processes. Set
`OpenOptions.BusyTimeout` to wait for another writer to close instead of
failing immediately with `Busy`, like `sqlite3_busy_timeout`.

Use `BeginTransaction`, `WriteTransaction`, `ResolveTransaction`,
`TransactionStatus`, and `CommitVersion` when an embedded application needs the
local transaction/OCC path exposed by the Antfly C ABI.

`OpenOptions.Storage` selects a `.aflite` file (`StorageLite`, the default)
or a normal Antfly directory (`StorageDirectory`); every method works on
either. `CreateWithOptions` only creates `.aflite` files; open a missing
directory path to create one.

Use `Backup` or `BackupToFile` to write a portable `.afb` archive from any
handle. Use `Restore` or `RestoreFile` to create a new database from one
without publishing a partial target on failure; `RestoreOptions.Storage`
selects a `.aflite` file (the default) or a directory, and a backup of either
kind restores into either kind. `ImportBackup` imports into an empty open
database.
Use `CopyStableSnapshot` or `CopyStableSnapshotFile` when you want a physical
`.aflite` database snapshot rather than a portable `.afb` backup archive.

From the repository’s `zig` directory, `zig build lite` builds the Lite CLI
and `libantfly`. Run `zig build lite-test` for the Lite checks, including the
Go binding tests against the built library.
