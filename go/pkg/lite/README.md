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

**In-process execution.** `libantfly` runs inference in-process on every
backend, including Metal, CUDA, and ONNX -- there is no separate worker
process, and `ANTFLY_INFERENCE_WORKER` is not used by `libantfly`. A call
that has already reached a GPU driver cannot be interrupted: a `call_timeout_ms`
deadline or a `Close` only takes effect once the call returns on its own, and
a driver fault terminates the process (unlike the `antfly` server, which runs
models in a separate, restartable worker process it can kill). Size
`InferenceOptions`/`OpenOptions` resource budgets and keep concurrent calls
bounded accordingly. See `zig/LITE.md`'s "Local Embedded Inference" section
for the full rationale.

### Embedded inference without a database

`OpenInference` opens a standalone `*Inference` handle that runs models with
no `DB` attached -- models load on first use and stay cached until `Close`.
Each method sends and receives the JSON of the matching `/ai/v1` route of the
Antfly inference HTTP API (see `specs/openapi/inference/api.yaml`):
`Embed`, `Rerank`, `Chunk`, `Generate`, `GenerateBatch`, `Rewrite`, `Extract`,
`Read` (OCR), `Transcribe`, and `ListModels`. `Generate` always returns a
complete response; a request with `"stream": true` fails with
`InvalidArgument`. Use `GenerateStream` to stream chunks instead.

```go
inf, err := lite.OpenInference(&lite.InferenceOptions{ModelsDir: "/path/to/models"})
if err != nil {
    // Unsupported means this build does not link the inference runtime, or
    // it failed to start.
}
defer inf.Close()

body, err := inf.Chunk([]byte(`{"input":"Ants live in colonies."}`))
```

Like `DB`, an `*Inference` is safe for concurrent use by multiple goroutines,
and `Close` waits for in-flight calls before releasing the handle; calls made
after `Close` return `InvalidArgument`.

On failure, every JSON method still returns the runtime's JSON error body
(`{"error": ..., "message": ...}`) wrapped in an `*InferenceError`, alongside
the mapped stable `ErrorCode` (an HTTP 404, such as a model that is not
installed, maps to `NotFound`; 4xx to `InvalidArgument`; 429/503/504 or an
elapsed call timeout to `Busy`; 501/507, meaning the model does not fit the
configured memory budgets, to `Unsupported`; a `Pull` or `GenerateStream`
callback returning `false`, or its `ctx` becoming done, to `Cancelled`). Use `errors.As` for the API
error code (e.g. `"MODEL_NOT_FOUND"`) and message, or `errors.Is(err, lite.NotFound)`
for the stable code:

```go
_, err := inf.Embed(request)
var infErr *lite.InferenceError
if errors.As(err, &infErr) {
    log.Printf("inference call failed: %s: %s", infErr.API.Code, infErr.API.Message)
}
```

Models are not downloaded automatically. Use `Pull` to fetch a model from the
Hugging Face Hub into the handle's models directory, like
`antfly inference pull`. The optional progress callback runs synchronously on
the calling goroutine as each file starts, every 16 MiB, and as each file
completes:

```go
_, err := inf.Pull(ctx, []byte(`{"model":"owner/name"}`), func(p lite.PullProgress) bool {
    log.Printf("%s: %s %d/%d bytes (cached=%v)", p.Model, p.File, p.BytesDownloaded, p.TotalBytes, p.Cached)
    return true // false stops the pull
})
```

`Pull` and `GenerateStream` both take a `context.Context` (nil is treated as
`context.Background()`) alongside their own callback-return-value mechanism
for stopping early -- the callback returning `false`, or `ctx` being done at
the point a callback fires, both stop the call, and either way the call
returns `lite.Cancelled` (the `ANTFLY_CANCELLED` C ABI code). Each progress
report is a rendezvous: the call waits for the callback to return before
continuing, so cancellation takes effect exactly at the report where the
callback returns `false` (or `ctx` is observed done) -- for `Pull`, that can
be each file start, every 16 MiB, or each file end (`ctx` itself is only
checked when a report fires, not continuously); for `GenerateStream`, each
streamed chunk. For `Pull`, files already fully downloaded before
cancellation stay staged, so pulling the same model again resumes rather than
restarts.

```go
err := inf.GenerateStream(ctx, request, func(chunk []byte) bool {
    // chunk is a "chat.completion.chunk" JSON document; it is only valid
    // during this call, so copy it before returning if you need to keep it.
    return true // false stops generation
})
```

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
