# Antfly Lite Go Binding

`go/pkg/antflylite` is the first language binding above the stable Zig APIs and
the `libantfly` C ABI. It wraps the Lite open/storage profile in that ABI, so
applications embed a live `.aflite` database directly instead of talking to the
network SDK.

The Go module includes the matching `antfly.h` C ABI header. Applications
still need `libantfly` at build and runtime. From the source tree, build the
C library before running cgo-backed tests:

```sh
cd zig
zig build capi
cd ../go/pkg/antflylite
go test -tags antflylite_capi ./...
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

Normal `go test ./...` does not run the C ABI smoke test. The
`antflylite_capi` tag is intentional so package consumers do not need a freshly
built `libantfly` unless they are testing the local binding against the
source-tree C library.

The open helpers call `ValidateABI` before filling C option structures or
creating handles. Applications can call `ValidateABI` at startup to fail fast
when the loaded `libantfly` ABI version or `antfly_lite_open_options` size
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

Use `BeginTransaction`, `WriteTransaction`, `ResolveTransaction`,
`TransactionStatus`, and `CommitVersion` when an embedded application needs the
local transaction/OCC path exposed by the Antfly C ABI.

Use `ExportToFile` or `BackupToFile` to write a portable `.afb` archive from an
open Lite handle. Use `RestoreFile`, `Restore`, `RestoreBackupFile`,
`RestoreBackup`, or handle-level `Import` to stage a portable backup into a new
`.aflite` database without publishing a partial target on import failure.
Use `CopyStableSnapshot` or `CopyStableSnapshotFile` when you want a physical
`.aflite` database snapshot rather than a portable `.afb` backup archive.

From the repository’s `zig` directory, `zig build lite` builds the Lite CLI
and `libantfly`. Run `zig build lite-test` for the Lite checks, including the
Go binding tests against the built library.
