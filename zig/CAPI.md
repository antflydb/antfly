# Antfly C API

`libantfly` is the stable embedded C ABI boundary for Antfly. Storage layouts
are selected by open options; they are not separate ABIs. Language bindings
should target this API once and expose storage-specific conveniences on top.

The public header, `include/antfly.h`, is Apache-2.0 so bindings can vendor
or transcribe it; the Lite bindings built on it are Apache-2.0 too. The
`libantfly` library itself is ELv2 like the rest of the core.

## Naming

- `antfly_*`: library-level calls that take no database handle (ABI version,
  threading mode, errors, options, buffer and result frees, artifact ID
  decoding, restoring a backup to a new path).
- `antfly_db_*`: calls on an `antfly_db *` handle, for any storage kind.
- `antfly_lite_*`: operations on the `.aflite` single-file format itself
  (integrity checks, compaction, vacuum, stable snapshots), plus shortcuts
  that open one with default options.
- `antfly_inference_*`: calls on an `antfly_inference *` handle, which runs
  inference (embed, rerank, chunk, generate, and so on) without a database.

Each operation has one name. ABI version 2 removed the earlier `antfly_lite_*`
duplicates of storage-neutral calls and the separate Lite options struct.

## ABI Contract

- `antfly_abi_version()` returns the ABI version supported by the library.
- Every options struct starts with `abi_size`.
- Callers must initialize options with the matching `*_init` function before
  setting fields.
- Readers of options structs must only read fields fully covered by `abi_size`.
- Reserved fields must be zero when present.
- New fields may be appended to options structs without breaking older callers.
- Handles are opaque `antfly_db *` values and must be closed with
  `antfly_db_close`.
- Returned buffers are owned by the caller and must be released with
  `antfly_buffer_free`.

## Storage-Neutral Open Surface

The primary embedded open surface is storage-neutral:

- `antfly_db_open(path, out_handle)`
- `antfly_db_open_with_options(path, options, out_handle)`
- `antfly_db_create_with_options(path, options, out_handle)`

`antfly_open_options` selects:

- `storage_kind`: `ANTFLY_STORAGE_KIND_DIRECTORY` for a normal Antfly
  directory, or `ANTFLY_STORAGE_KIND_LITE` for a single-file `.aflite`.
- `open_mode`: writer, read-only query, or status-only.
- `profile`: native or hosted/manual maintenance.
- `flags`: `NO_SYNC`, `TTL_CLEANUP`, remote/local inference capability state,
  and generated-enrichment replay.
- storage sizing, TTL cleanup tuning, embedded-inference resource budgets,
  and `busy_timeout_ms`.

Directory storage is the default for the generic open APIs. The
`antfly_lite_open*` / `antfly_lite_create*` shortcuts open a `.aflite` file
with default options; pass `antfly_open_options` with
`ANTFLY_STORAGE_KIND_LITE` for anything else.
`antfly_db_create_with_options` currently provides exclusive create semantics
for `ANTFLY_STORAGE_KIND_LITE` only. Directory storage should use
`antfly_db_open_with_options`, which preserves the existing directory
open-or-create behavior until the directory backend exposes an exclusive create
primitive.

## Backup and Restore

Portable `.afb` backups are storage-neutral:

- `antfly_db_backup` writes a backup of any handle.
- `antfly_db_import_backup` imports one into an empty database of either kind.
- `antfly_restore_backup_json` and `antfly_restore_backup_file_json` create a
  new database at a path, of the storage kind in the passed
  `antfly_open_options` (NULL means directory storage). The database is built
  and indexed beside the destination, then published atomically; `replace`
  swaps out an existing one. A failed or interrupted restore leaves the
  destination holding either the complete old or the complete new database,
  never a partial or missing one. Replacing a directory database that any
  process has open (through libantfly or otherwise), or a Lite file that has
  an open writer, fails with `ANTFLY_BUSY`; so does opening a directory
  database while a restore publishes it. `ANTFLY_OUTCOME_UNKNOWN` means the
  new database was published but its crash durability could not be confirmed.

A backup of either kind restores or imports into either kind.
`antfly_db_status_json` and `antfly_db_capabilities_json` likewise report on
any handle; the status `storage.format` is `"aflite"` or `"directory"`.

## Inference

`antfly_inference_open` starts the embedded inference runtime on its own,
with no database, and returns an `antfly_inference *` handle; close it with
`antfly_inference_close`. `antfly_inference_options` sets the models
directory, the same resource budgets as the `inference_*_budget_mb` open
options, and an optional per-call timeout. NULL options use the defaults.

Each call takes the request JSON and returns the response JSON of the
matching `/ai/v1` route of the inference HTTP API
(`specs/openapi/inference/api.yaml`), dispatched in memory to the same
handlers:

| Call | Route |
|---|---|
| `antfly_inference_embed_json` | `POST /embed` |
| `antfly_inference_rerank_json` | `POST /rerank` |
| `antfly_inference_chunk_json` | `POST /chunk` |
| `antfly_inference_generate_json` | `POST /generate` |
| `antfly_inference_rewrite_json` | `POST /rewrite` |
| `antfly_inference_extract_json` | `POST /extract` |
| `antfly_inference_read_json` | `POST /read` (OCR) |
| `antfly_inference_transcribe_json` | `POST /transcribe` |
| `antfly_inference_list_models_json` | `GET /models` |

- Images and audio go inline in the JSON, as base64 or `data:` URIs.
- Responses are complete; a generate request with `"stream": true` fails
  with `ANTFLY_INVALID_ARGUMENT`.
- The output buffer holds the response body even when a call fails, so a
  failure carries the runtime's JSON error. Free it either way. HTTP 404
  (for example a model that is not installed) maps to `ANTFLY_NOT_FOUND`,
  other 4xx to `ANTFLY_INVALID_ARGUMENT`, 429/503/504 and an elapsed timeout
  to `ANTFLY_BUSY`, and 501 or 507 (the model does not fit the budgets) to
  `ANTFLY_UNSUPPORTED`.
- Models are not downloaded on demand. Install them with
  `antfly inference pull <owner/name>`.
- On backends that isolate model execution in a worker process (Metal, CUDA,
  ONNX), the runtime runs the `antfly` executable named by
  `ANTFLY_INFERENCE_WORKER`, else the one next to libantfly, else the first
  on `PATH`; it must come from the same release. If the runtime cannot
  start, open returns `ANTFLY_UNSUPPORTED`.

Inference handles have the same guarantees as database handles (see Thread
Safety): any thread may call concurrently, close waits for in-flight calls,
and a closed handle, or a database handle passed by mistake, is rejected with
`ANTFLY_INVALID_ARGUMENT`. They live in a separate registry with its own
reserved address range on 64-bit POSIX targets, so there the two kinds can
never alias. Each handle owns its own runtime and loaded models; share one
handle rather than opening several.

## Read-Only Modes

Read-only open modes are part of the storage contract, not just a DB-layer write
guard:

- Lite native files open with read-only file access.
- LSM primary/index backends open physical storage in read-only mode.
- LMDB primary storage opens the LMDB environment read-only and does not create
  missing directories or databases.
- In-memory backends have no physical read-only state, but DB write APIs still
  reject mutations under read-only open modes.

`status_only` should be at least as restrictive as query read-only. It may
avoid starting optional background work where the storage implementation can
support that cleanly.

## Thread Safety

`libantfly` runs in one threading mode, equivalent to SQLite's default
"serialized" mode: any thread may call any function on any handle,
concurrently. `antfly_threading_mode()` reports it as
`ANTFLY_THREADING_SERIALIZED`, like `sqlite3_threadsafe()`, and the Lite
capabilities JSON carries `"threading": "serialized"`.

Unlike a single SQLite connection, one handle is not a single serial queue.
Every export that takes a handle enters through a per-handle guard, and each
export has one of four access classes:

| Class | Exports | Runs concurrently with |
|---|---|---|
| read | lookup, get_raw, scan, search (JSON, dense, text, wire, hits), graph queries, aggregates, stats, schema/index/enrichment listing, status, capabilities, check, pending-work stats, enrichment extract/compute | everything except exclusive calls |
| write | batch, transactions and intent resolution, compact, vacuum, snapshot | reads and maintenance; one write at a time per handle |
| maintain | run-until-idle, generated-enrichment replay, backup, stable snapshot copy | reads and writes; one maintenance call at a time per handle |
| exclusive | set schema, add/delete index or enrichment, import a backup into a handle, range and split changes, shadow index managers, readable lease hook | nothing; waits for in-flight calls |

Concurrent writes on one handle queue behind each other instead of failing
with `ANTFLY_BUSY`. Reads run against pinned storage snapshots while a write
commits. A search stamps the current document identity generation; if a write
commits before the search re-checks it, the search restamps and retries, and
its final attempt briefly holds off writers so it always completes. A read
with a caller-pinned `identity_read_generation` that has gone stale is
rejected with `ANTFLY_INVALID_ARGUMENT` rather than retried.

A handle value is an opaque id for a slot in a process-wide registry, not a
pointer to the database. Registry slots are never freed and each carries a
generation, so every handle value a caller passes is safe to use at any time:

- `antfly_db_close` claims the slot, rejects new calls, waits for every call
  that has already entered (including calls still waiting for a lock), then
  frees the database and advances the slot's generation.
- A call racing close, or made after it, returns `ANTFLY_INVALID_ARGUMENT`
  instead of touching freed memory. This holds even for a thread paused
  before its call reached the handle.
- Concurrent and repeated `antfly_db_close` calls are no-ops after the first.
- A slot reused by a later open gets a new generation, so an old handle value
  can never reach the new database. A slot that has used every generation a
  handle value can encode is retired for the life of the process instead of
  wrapping, so generations never repeat.

On 64-bit POSIX targets a handle value is also a genuine address inside an
inaccessible region the library reserves (no memory is committed), so it is
at least 4096, aligned, and never inside any allocator's heap. Bindings can
therefore keep it in pointer-typed fields that a garbage collector inspects,
such as Go's `unsafe.Pointer`.

This is stronger than `sqlite3_close`, where using a closed connection is
undefined. Bindings may still track their own handle state to report a
closed handle before crossing the ABI.

Across handles and processes the Lite model matches SQLite in WAL mode: one
writer and any number of readers per file. The writer lock is taken when a
writer handle opens and held until it closes. A second writer open fails
with `ANTFLY_BUSY` immediately, or, when `busy_timeout_ms` is set in
`antfly_open_options`, retries with capped
exponential backoff until the timeout elapses, like `sqlite3_busy_timeout`.
Read-only and status-only opens never contend for the writer lock.

Every thread that calls into `libantfly` needs at least
`ANTFLY_MIN_THREAD_STACK_SIZE` (8 MiB) of native stack. The storage engine
keeps sizable buffers on the stack: release builds peak around 2 MiB and
debug builds use more, and a smaller stack crashes inside the engine rather
than returning an error. 8 MiB is the Linux and macOS main-thread default,
but secondary threads are often smaller: macOS pthreads default to 512 KiB
and Rust `std` threads to 2 MiB. How each binding meets the minimum:

| Binding | How it gets the minimum |
|---|---|
| Go | cgo calls run on OS threads that inherit the 8 MiB main-thread stack |
| Python | CPython threads use 8 MiB (Linux) or 16 MiB (macOS) |
| Rust | caller's responsibility; spawn threads with `antfly_lite::MIN_THREAD_STACK_SIZE` |
| TypeScript | configures koffi's call stacks to 8 MiB before loading the library |
| C | size threads with `pthread_attr_setstacksize(&attr, ANTFLY_MIN_THREAD_STACK_SIZE)` |

The Rust binding's tests run at exactly the minimum, so stack growth in the
engine fails them.

Handles must not be carried across `fork()`: close them before forking or
open new ones in the child. The library may run background enrichment and
maintenance work on its own threads for non-hosted handles; hosted handles
leave that work to explicit run-until-idle calls.

## Lite File Operations

The `antfly_lite_*` functions are not a separate Lite ABI. Besides the open
shortcuts, they cover only operations on the `.aflite` format itself:

- Integrity checks (`antfly_lite_check_json`), including path-level checks for
  files that may not open (`antfly_lite_check_file_json`).
- Physical stable snapshots of the file (`antfly_lite_copy_stable_snapshot_json`,
  `antfly_lite_copy_stable_snapshot_file_json`).
- Compaction and vacuum of the file (`antfly_lite_compact_json`,
  `antfly_lite_vacuum_json`).

Everything else, including status, backup, import, restore, drains, and
generated-enrichment replay, is storage-neutral and named `antfly_db_*` or
`antfly_*`.

## Testing Expectations

C ABI changes should have coverage for:

- Header/library size agreement for options structs.
- Prefix-compatible options parsing.
- Unknown flag and non-zero reserved-field rejection.
- Generic directory open, Lite open, create, read-only reopen, and write
  rejection.
- Physical read-only behavior for persistent backends.
- Binding smoke tests that compile against the installed public header.
- Every new export that takes a handle must enter through `enterHandle` with
  the right access class, and a binding test should run it concurrently with
  writes (see `go/pkg/lite/concurrency_cgo_test.go`).
