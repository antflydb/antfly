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

The binding exposes raw JSON methods such as `StatusJSON` and `CapabilitiesJSON`
for parity with the C ABI. It also exposes typed `Status` and `Capabilities`
helpers for stable Lite control fields, including storage identity, inference
mode, caller-supplied artifact support, and distributed-only capability flags.
Use constants such as `InferenceModeCallerSuppliedArtifacts`,
`InferenceModeManualMaintenance`, and `InferenceModeDisabledDeferred` when
branching on inference status or capabilities.
Typed `Check`, `Vacuum`, and `CopyStableSnapshot` helpers cover the stable Lite
maintenance reports while keeping the raw JSON methods available.

Use `Open` for the normal writer profile, `OpenReadonly` for read-only query
handles, `OpenStatusOnly` for inspection, and `OpenHosted` when the application
wants hosted/manual maintenance and will call `RunUntilIdle` itself.
Use `OpenWithOptions` for advanced settings such as map size and native-profile
TTL cleanup.

Use `BackupToFile` to write a portable `.afb` archive from an open Lite handle.
Use `RestoreBackupFile` or `RestoreBackup` to stage a portable backup into a
new `.aflite` database without publishing a partial target on import failure.

The repository-level `zig build lite-core` gate builds `libantflylite` and runs
the Go binding tests against it.
