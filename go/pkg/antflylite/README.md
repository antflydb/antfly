# Antfly Lite Go Binding

`go/pkg/antflylite` is the first language binding above the stable Zig and C
Antfly Lite APIs. It wraps the C ABI, so applications embed a live `.aflite`
database directly instead of talking to the network SDK.

Build the C library before running cgo-backed tests from the source tree:

```sh
cd zig
zig build lite-capi
cd ../go/pkg/antflylite
go test -tags antflylite_capi ./...
```

Normal `go test ./...` does not run the C ABI smoke test. The
`antflylite_capi` tag is intentional so package consumers do not need a freshly
built `libantflylite` unless they are testing the local binding against the
source-tree C library.

The open helpers call `ValidateABI` before filling C option structures or
creating handles. Applications can call `ValidateABI` at startup to fail fast
when the loaded `libantflylite` ABI version or `antfly_lite_open_options` size
does not match the header used to build the Go binding.

The binding exposes raw JSON methods such as `StatusJSON` and `CapabilitiesJSON`
for parity with the C ABI. It also exposes typed `Status` and `Capabilities`
helpers for stable Lite control fields, including storage identity, inference
mode, caller-supplied artifact support, and distributed-only capability flags.
Use constants such as `InferenceModeCallerSuppliedArtifacts`,
`InferenceModeManualMaintenance`, and `InferenceModeDisabledDeferred` when
branching on inference status or capabilities.
Typed `PendingWorkStats`, `RunUntilIdleStatus`, `Check`, `Vacuum`, and
`CopyStableSnapshot` helpers cover the stable Lite maintenance reports while
keeping the raw JSON methods available. `ReplayGeneratedEnrichments` recreates
generated enrichment work from stored documents after a manual-maintenance or
restore pause.
Use `CheckFile` or `CheckFileJSON` to inspect an invalid, truncated, or
corrupted `.aflite` file without opening a database handle.

Use `Open` for the normal writer profile, `OpenReadonly` for read-only query
handles, `OpenStatusOnly` for inspection, and `OpenHosted` when the application
wants hosted/manual maintenance and will call `RunUntilIdle` itself. Use
`RunUntilIdleStatus` when the application also wants the typed post-drain
pending-work readiness document.
Use `OpenWithOptions` for advanced settings such as map size, native-profile
TTL cleanup, and explicit inference status reporting. Set
`RemoteProviderConfigured` when the embedding producer is backed by a configured
remote provider so `Status().Inference` reports `remote_provider` instead of the
default caller-supplied/deferred mode. Set `LocalRuntimeConfigured` when the
application embeds a local inference runtime and wants status to report
`local_embedded`.

Use `BeginTransaction`, `WriteTransaction`, `ResolveTransaction`,
`TransactionStatus`, and `CommitVersion` when an embedded application needs the
local transaction/OCC path exposed by the Lite C ABI.

Use `ExportToFile` or `BackupToFile` to write a portable `.afb` archive from an
open Lite handle. Use `RestoreFile`, `Restore`, `RestoreBackupFile`,
`RestoreBackup`, or handle-level `Import` to stage a portable backup into a new
`.aflite` database without publishing a partial target on import failure.

The repository-level `zig build lite-core` gate builds `libantflylite` and runs
the Go binding tests against it.
