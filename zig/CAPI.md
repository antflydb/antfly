# Antfly C API

`libantfly` is the stable embedded C ABI boundary for Antfly. Storage layouts
are selected by open options; they are not separate ABIs. Language bindings
should target this API once and expose storage-specific conveniences on top.

## ABI Contract

- `antfly_abi_version()` returns the ABI version supported by the library.
- Every options struct starts with `abi_size`.
- Callers must initialize options with the matching `*_init` function before
  setting fields.
- Readers of options structs must only read fields fully covered by `abi_size`.
- Reserved fields must be zero when present.
- New fields may be appended to options structs without breaking older callers.
- Handles are opaque `void *` values and must be closed with
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
- storage sizing and TTL cleanup tuning fields.

Directory storage is the default for the generic open APIs. Lite-specific
helpers such as `antfly_lite_open_with_options` remain source-compatible
wrappers that set `storage_kind` to Lite and use the same handle model.
`antfly_db_create_with_options` currently provides exclusive create semantics
for `ANTFLY_STORAGE_KIND_LITE` only. Directory storage should use
`antfly_db_open_with_options`, which preserves the existing directory
open-or-create behavior until the directory backend exposes an exclusive create
primitive.

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
| maintain | run-until-idle, generated-enrichment replay, backup/export, stable snapshot copy | reads and writes; one maintenance call at a time per handle |
| exclusive | set schema, add/delete index or enrichment, import/restore into a handle, range and split changes, shadow index managers, readable lease hook | nothing; waits for in-flight calls |

Concurrent writes on one handle queue behind each other instead of failing
with `ANTFLY_BUSY`. Reads run against pinned storage snapshots while a write
commits. A search stamps the current document identity generation; if a write
commits before the search re-checks it, the search restamps and retries, and
its final attempt briefly holds off writers so it always completes. A read
with a caller-pinned `identity_read_generation` that has gone stale is
rejected with `ANTFLY_INVALID_ARGUMENT` rather than retried.

`antfly_db_close` rejects new calls with `ANTFLY_INVALID_ARGUMENT`, waits for
every call that has already entered (including calls still waiting for a
lock), and then frees the handle. Concurrent `antfly_db_close` calls on one
handle are safe. As with `sqlite3_close`, using a handle after
`antfly_db_close` has returned is undefined; bindings should guard their own
handle field (the Go binding holds a read/write mutex around every call).

Across handles and processes the Lite model matches SQLite in WAL mode: one
writer and any number of readers per file. The writer lock is taken when a
writer handle opens and held until it closes. A second writer open fails
with `ANTFLY_BUSY` immediately, or, when `busy_timeout_ms` is set in
`antfly_open_options` or `antfly_lite_open_options`, retries with capped
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

## Lite Compatibility Helpers

The `antfly_lite_*` functions are convenience APIs in `libantfly`, not a
separate Lite ABI. They are appropriate for operations that are inherently Lite
specific:

- Lite status and capability JSON.
- `.aflite` integrity checks, including path-level checks for files that may
  not open successfully.
- Lite backup/export and restore/import helpers.
- Stable snapshot, compact, vacuum, and run-until-idle maintenance.
- Generated-enrichment replay for hosted/manual Lite workflows.

Bindings should prefer the storage-neutral open surface for new generic open
paths, then expose Lite helpers for these Lite-only workflows.

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
