# antfly-lite

Pure-Python binding for embedded Antfly Lite databases, built on the stable
`libantfly` C ABI. There is no compiled extension and no `cffi` dependency:
the binding is plain `ctypes`, so one wheel serves every platform, and
`ctypes.CDLL` releases the GIL for the duration of each foreign call, which
is what lets Python threads get real parallelism against one handle.

```python
import antfly_lite

with antfly_lite.create("my.aflite") as db:
    db.batch([antfly_lite.WriteIntent(key="doc:1", value=b'{"title":"hi"}')], timestamp=1)
    db.run_until_idle()
    print(db.lookup("doc:1"))
```

`import antfly` refers to the separate `antfly-sdk` package (the network
client). This package's import name is `antfly_lite`.

## Installing libantfly

This package does not build or bundle `libantfly` itself (yet -- future
releases may ship platform wheels with a bundled library). You need a built
or installed copy of the shared library and, at import time, `antfly_lite`
locates it using this order (first match wins):

1. **`ANTFLY_LIBRARY`** environment variable: an explicit path to the shared
   library file. If set but the file does not exist, discovery fails
   without falling back to any other mechanism -- an explicit override that
   points nowhere is treated as a configuration error, not a hint to keep
   guessing.
2. **`ANTFLY_LIB_DIR`** environment variable: a directory containing the
   platform-appropriate library file.
3. A library bundled inside this package at `antfly_lite/_lib/` (reserved
   for future platform-specific wheels; empty in the current pure-Python
   wheel).
4. The **`antfly-cli`** package's bundled `lib/` directory, if `antfly-cli`
   is installed. Antfly CLI release wheels ship `antfly_cli/bin/antfly`,
   `antfly_cli/include/antfly.h`, and `antfly_cli/lib/libantfly.*` together,
   so installing `antfly-cli` alongside `antfly-lite` is a convenient way to
   get a matching library without a separate download.
5. **`zig/zig-out/lib`**, found by walking up from this package's own
   location -- for development checkouts of the `antfly` monorepo.
6. The system dynamic linker's search path, via
   `ctypes.util.find_library("antfly")`.

Platform library names: `libantfly.dylib` (macOS), `libantfly.so` (Linux),
`antfly.dll` (Windows).

Call `antfly_lite.validate_abi()` at startup to fail fast when the loaded
library's ABI version or `antfly_lite_open_options` struct size does not
match the version this binding was written against. `create()`, `open()`,
`open_hosted()`, `create_hosted()`, `check_file()`, and
`copy_stable_snapshot_file()` all validate the ABI automatically before
touching the library.

### Embedded inference worker resolution

GPU-hosted and driver-backed inference backends (Metal, CUDA, ONNX, PJRT)
run in a separate, replaceable worker process rather than inside the Python
process, for crash containment. A Python process linking `libantfly` has no
`antfly`-shaped `argv[0]` to re-exec, so the runtime resolves the worker
executable itself, in order: the `ANTFLY_INFERENCE_WORKER` environment
variable (a path to the worker executable, typically an `antfly` binary);
otherwise an `antfly` binary next to the loaded `libantfly`; otherwise
`antfly` on `PATH`. If none of these resolve, calls into a process-isolated
backend fail with a clear error naming `ANTFLY_INFERENCE_WORKER`. Set it (or
place an `antfly` binary next to `libantfly` or on `PATH`) before opening a
`local_runtime=True` handle that needs Metal/CUDA/ONNX/PJRT models. See
`zig/LITE.md`'s "Local Embedded Inference" section for the full resolution
order and rationale; this mirrors the Go binding's README verbatim.

## JSON conventions

Request parameters accept a `dict`/`list` (serialized with
`json.dumps(..., separators=(",", ":"))`), a `str` (sent as UTF-8 bytes
as-is, for callers that already have a JSON string), or `bytes`/`bytearray`
(sent as-is). Plain identifiers such as keys, index names, and base64
artifact IDs are always `str`/`bytes`, never JSON-encoded.

Every JSON-returning method returns parsed JSON (a `dict`, `list`, or
scalar) by default. Pass `raw=True` to get the raw response bytes instead
(useful to avoid a decode/round-trip, or to inspect a response that failed
to parse). This is the one consistent scheme used everywhere in this
binding -- there is no separate family of `*_json`-suffixed methods, except
where the C ABI itself exposes two genuinely different entry points (for
example `batch()`, which takes typed `WriteIntent`s, versus `batch_json()`,
which takes the public Antfly batch request shape).

The four `*_wire` search methods (`dense_search_wire`, `text_match_wire`,
`text_term_wire`, `text_match_phrase_wire`) exchange a packed binary wire
format, not JSON, and always return raw `bytes`.

## Threading

A `Database` is safe for concurrent use by multiple threads; share one
handle rather than opening one per thread. `libantfly` runs in serialized
threading mode (`antfly_lite.threading_mode() ==
antfly_lite.THREADING_SERIALIZED`): reads such as `search()`, `lookup()`,
and `scan()` run in parallel with each other and with writes, `batch()` and
transaction calls on one handle queue instead of failing with `BusyError`,
and schema or index changes wait for in-flight calls. `close()` waits for
in-flight calls on other threads to finish; calls made after `close()`
raise `InvalidArgumentError`. See `zig/CAPI.md`'s "Thread Safety" section
for the full C ABI contract.

Only one writer handle may be open per file at a time, across processes.
Pass `busy_timeout=<seconds or datetime.timedelta>` to `create()`/`open()`
to wait for another writer to close instead of failing immediately with
`BusyError`, like `sqlite3_busy_timeout`.

## API overview

- **Opening**: `create()`, `open()`, `open_readonly()`, `open_status_only()`,
  `open_hosted()`, `create_hosted()`, `open_with_options()`/
  `create_with_options()` (with an `OpenOptions` dataclass for advanced
  settings: map size, TTL cleanup, inference resource budgets, busy
  timeout).
- **Data**: `batch()`, `batch_json()`, `lookup()`, `get_raw()`, `scan()`,
  `search()`, `stats()`, `aggregate_hits()`, `lookup_artifact()`,
  `get_schema()`/`set_schema()`, `extract_enrichments()`,
  `compute_enrichments()`.
- **Indexes/enrichments**: `list_indexes()`, `add_index()`,
  `delete_index()`, `list_enrichments()`, `add_enrichment()`,
  `delete_enrichment()`.
- **Graph**: `edges()`, `neighbors()`, `traverse_edges()`,
  `execute_graph_queries()`, `find_shortest_path()`,
  `find_k_shortest_paths()`, `match_pattern()`.
- **Transactions**: `begin_transaction()`, `write_transaction()`,
  `resolve_transaction()`, `transaction_status()`, `commit_version()`.
- **Status/maintenance**: `status()`, `capabilities()`,
  `pending_work_stats()`, `run_until_idle()`, `run_until_idle_status()`,
  `check()`, `vacuum()`, `compact()`, `copy_stable_snapshot()`,
  `replay_generated_enrichments()`.
- **Backup/restore**: `backup()`, `export()`, `import_backup()`,
  `import_()`, `backup_to_file()`, `export_to_file()`; module-level
  `restore()`, `restore_backup()`, `restore_file()`,
  `restore_backup_file()`.
- **Module-level, no handle needed**: `check_file()`,
  `copy_stable_snapshot_file()`, `decode_artifact_id()`, `abi_version()`,
  `threading_mode()`, `THREADING_SERIALIZED`, `validate_abi()`.
- **Errors**: `AntflyError` (with `.code` and `.name`, e.g.
  `"ANTFLY_BUSY"`) and one subclass per C ABI error code:
  `InvalidArgumentError`, `NotFoundError`, `VersionConflictError`,
  `IntentConflictError`, `TxnNotFoundError`, `BusyError`,
  `OutcomeUnknownError`, `UnsupportedError`, `StalledError`,
  `InternalError`. Unrecognized codes raise `AntflyError` directly.

`create()` provisions the default `full_text_index_v0` full-text index,
matching the server's table-create behavior, so `add_index()` is only
needed for indexes beyond that default. `open()` does not create missing
files or upgrade pre-release Lite layouts; unknown or invalid files fail
explicitly.

## Development

```sh
cd py/packages/lite
uv sync
uv run ruff format --check .
uv run ruff check .
uv run pyright
ANTFLY_LITE_REQUIRE_LIBRARY=1 uv run pytest -q
```

Tests that need `libantfly` skip cleanly when it cannot be found, unless
`ANTFLY_LITE_REQUIRE_LIBRARY=1` is set, in which case they fail instead.
`tests/test_conformance.py` runs every case in
`zig/pkg/antfly/capi-conformance/cases/*.json` through this public API, the
same declarative cases the Go and Rust bindings run.
