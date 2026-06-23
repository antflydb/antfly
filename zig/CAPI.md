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

## Capabilities

Capabilities are a storage-neutral handle property. Bindings should call
`antfly_db_capabilities_json(handle, out)` after opening any embedded database
and use `antfly_lite_capabilities_json` only as a Lite compatibility helper.
Both functions return the same versioned JSON shape for a Lite handle.

The capability object includes relational and SQL blocks:

```json
{
  "relational": {
    "tables": true,
    "closed_schema_validation": true,
    "local_schema_rewrite_jobs": true,
    "portable_backup": false,
    "transactions": "local"
  },
  "sql": {
    "adapter": true,
    "ddl": true,
    "dml": true,
    "sessions": true,
    "document_tables": "read_only_planned",
    "embedded_exec": true
  }
}
```

`embedded_exec=true` means the embedded/Lite SQL path can execute directly
against local storage instead of requiring a localhost HTTP server.
`portable_backup=false` is intentional until the
portable backup path can materialize relational physical rows as logical rows
and restore them through the typed relational write path.

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
