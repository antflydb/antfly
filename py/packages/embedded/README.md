# antfly-embedded

Python binding for embedded Antfly databases and inference, built on the stable
`libantfly` C ABI. The Python code uses plain `ctypes`, without a compiled
extension or `cffi` dependency. Release wheels bundle platform-specific
`libantfly` binaries and the inference worker. `ctypes.CDLL` releases the GIL
for the duration of each foreign call, allowing parallel calls from Python
threads against one handle.

```python
import antfly_embedded

with antfly_embedded.create("my.aflite") as db:
    db.batch([antfly_embedded.WriteIntent(key="doc:1", value=b'{"title":"hi"}')], timestamp=1)
    db.run_until_idle()
    print(db.lookup("doc:1"))
```

`import antfly` refers to the separate `antfly-sdk` package (the network
client). This package's import name is `antfly_embedded`.

## Installing libantfly

Release platform wheels bundle the Apache-2.0 `libantfly` library and
private `antfly-inference-worker` helper. Installing this binding does not add
`antfly-lite` or `antfly-inference` commands. Source checkouts and the
pure-Python development wheel need a built copy of the shared library. At
import time, `antfly_embedded`
locates it using this order (first match wins):

1. **`ANTFLY_LIBRARY`** environment variable: an explicit path to the shared
   library file. If set but the file does not exist, discovery fails
   without falling back to any other mechanism -- an explicit override that
   points nowhere is treated as a configuration error, not a hint to keep
   guessing.
2. **`ANTFLY_LIB_DIR`** environment variable: a directory containing the
   platform-appropriate library file.
3. The wheel's bundled `antfly_embedded/_lib/` directory.
4. **`zig/zig-out/lib`**, found by walking up from this package's own
   location -- for development checkouts of the `antfly` monorepo.
5. The system dynamic linker's search path, via
   `ctypes.util.find_library("antfly")`.

Platform library names: `libantfly.dylib` (macOS), `libantfly.so` (Linux),
`antfly.dll` (Windows).

Call `antfly_embedded.validate_abi()` at startup to fail fast when the loaded
library's ABI version or `antfly_open_options` struct size does not match
the version this binding was written against. `create()`, `open()`,
`open_hosted()`, `create_hosted()`, `check_file()`, and
`copy_stable_snapshot_file()` all validate the ABI automatically before
touching the library.

## Storage kinds

`create()`/`open()`/`create_with_options()`/`open_with_options()` accept a
`storage=` argument (an `antfly_embedded.Storage`):

- `Storage.LITE` (the default): a single-file `.aflite` database.
- `Storage.DIRECTORY`: a normal single-node Antfly directory. Opening a
  missing directory path creates it; `create_with_options()` only has
  exclusive-create semantics for `Storage.LITE`, so creating directory
  storage surfaces the library's own error rather than being special-cased
  by this binding.

A portable backup (`backup()`/`db.backup_to_file()`) works across storage
kinds: `import_backup()` and the module-level `restore()`/`restore_file()`
accept a backup of either kind and can write it into either kind (`restore`
and `restore_file` take the destination's `storage=` too). `Storage.LITE`
destinations still require a `.aflite` path client-side; `Storage.DIRECTORY`
has no suffix requirement.

## Embedded inference (no database)

`antfly_embedded.Inference` runs the same embedded inference runtime as a
`local_runtime=True` Lite handle, but standalone -- no database, no
`.aflite` file:

```python
import antfly_embedded

with antfly_embedded.Inference.open() as inf:
    print(inf.chunk({"input": "Ants live in colonies. Workers gather food."}))
    print(inf.embed({"model": "Qwen/Qwen3-Embedding-0.6B-GGUF", "input": ["hello"]}))
```

`Inference.open()` takes keyword-only options: `models_dir` (defaults to
`$ANTFLY_INFERENCE_MODELS_DIR`, else `~/.antfly/inference/models`),
the same `host_budget_mb`/`backend_budget_mb`/`process_memory_budget_mb`/
`combined_budget_mb`/`kv_budget_mb`/`scratch_budget_mb` resource-budget
overrides as `OpenOptions` (0 means automatic), and `call_timeout_ms` (a
per-call deadline; 0 means none -- see "Inference runs in-process" below for
what it can and cannot interrupt). It raises `UnsupportedError` if this
build does not link the inference runtime or the runtime cannot start.

Each call takes the request JSON and returns the response JSON of the
matching `/ai/v1` route of the Antfly inference HTTP API: `embed()`,
`rerank()`, `chunk()`, `generate()`, `generate_batch()` (up to 128
non-streaming requests per call; per-item failures are reported in the
response, not raised), `rewrite()`, `extract()`, `read()` (OCR),
`transcribe()`, and `list_models()`. Requests accept the same `dict` / `str`
/ `bytes` JSON forms as `Database` methods, and responses honor the same
`raw=True` convention. `generate()` and `generate_batch()` requests always
return complete responses; `"stream": true` raises `InvalidArgumentError`
-- use `generate_stream()` to stream.

On failure, the C API still returns a JSON error body (`{"error": ...,
"message": ...}`); this binding folds it into the raised exception's
message rather than discarding it, using the same `AntflyError` subclasses
as `Database` (a missing model raises `NotFoundError`, for example).

### Streaming generation

`generate_stream(request, on_chunk)` streams a generate call; it sets
`"stream": true` for you (do not set it in `request`). `on_chunk` is called
synchronously on the calling thread for each chunk -- the parsed JSON of a
`chat.completion.chunk` -- as the model produces tokens:

```python
def on_chunk(chunk):
    print(chunk["choices"][0]["delta"].get("content", ""), end="")


with antfly_embedded.Inference.open() as inf:
    inf.generate_stream(
        {"model": "ggml-org/gemma-4-e2b-it-gguf:gguf:Q4_0", "messages": [{"role": "user", "content": "hi"}]},
        on_chunk,
    )
```

A request rejected before generation starts (such as a missing model) fails
like `generate()`, with the runtime's JSON error folded into the exception.
A failure partway through the stream raises `InternalError` with a
`STREAM_FAILED` body. On success `generate_stream()` returns `None` -- there
is no final response body, only the chunks already delivered to `on_chunk`.

### Pulling models

Models are not downloaded automatically. `pull(request, progress=None)`
downloads one from the Hugging Face Hub into the handle's models directory,
like `antfly inference pull` (`request = {"model": "owner/name[:variant]",
...}`); `close()` waits for it. Pass `progress=callable` to receive an
`antfly_embedded.PullProgress` (`model`, `file`, `bytes_downloaded`,
`total_bytes`, `files_done`, `files_total`, `cached`) synchronously on the
calling thread as each file starts, every 16 MiB, and as it completes.
Completed files stay staged across a cancelled or interrupted pull, so
pulling the same model again resumes rather than restarts.

### Cancellation

`pull()`'s `progress` callback and `generate_stream()`'s `on_chunk` callback
can stop the call in progress by returning `False` (returning `None` or
`True`, including implicitly by falling off the end of the function, keeps
going -- existing callbacks that don't return anything keep working
unchanged). A cancelled call raises `antfly_embedded.CancelledError`.
Cancellation only takes effect at the next report -- for `pull()`, each
file's start, every 16 MiB, and its end; for `generate_stream()`, each
chunk -- it does not interrupt work in progress between reports.

If the callback itself raises, this binding captures the exception, cancels
the call the same way, and re-raises the original exception once the
underlying C call returns; it never unwinds across the C ABI boundary, and
it takes priority over whatever error the call itself would otherwise
report.

`Inference` handles have the same threading contract as `Database` (see
"Threading" below): safe for concurrent use by multiple threads, and
`close()` (idempotent, safe to call repeatedly and concurrently) waits for
in-flight calls before releasing the handle. Calls made after `close()`
raise `InvalidArgumentError`. `progress`/`on_chunk` callbacks always run on
the calling thread, never a library-owned thread.

### Inference runs in-process

`libantfly` runs every inference backend -- including Metal, CUDA, and ONNX
-- in the calling process, on every backend, for both `local_runtime=True`
Lite handles and standalone `Inference` handles. There is no separate
worker process; `ANTFLY_INFERENCE_WORKER` is not used.

This is the trade SQLite makes for driver-backed extensions: once a call
reaches the device or driver it cannot be interrupted. `call_timeout_ms`
(and closing a handle) only take effect once the call returns; a call stuck
in the driver blocks close instead of being abandoned; and a GPU driver
fault terminates the process. Calls on CPU backends still stop
cooperatively. See `zig/CAPI.md`'s "Inference In Process" section for the
full rationale.

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
threading mode (`antfly_embedded.threading_mode() ==
antfly_embedded.THREADING_SERIALIZED`): reads such as `search()`, `lookup()`,
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

## Graph edges and index readiness

`batch_json()` accepts `graph_writes` and `graph_deletes` for individual edges.
Each operation names an existing graph index, source document, target document,
and edge type. A write may also contain `weight`, `created_at`, `updated_at`,
and `metadata_json` (a JSON string). These operations preserve the other edges
on the source document. For example:

```python
db.batch_json({"graph_writes": [{
    "index_name": "graph", "source": "node:a", "target": "node:b",
    "edge_type": "KNOWS", "metadata_json": '{"uuid":"1"}',
}]})
db.batch_json({"graph_deletes": [{
    "index_name": "graph", "source": "node:a", "target": "node:b",
    "edge_type": "KNOWS",
}]})
```

For document writes, omitting `_edges` preserves existing edges. Supplying
`_edges.<index>` replaces that document's edge set for the index; an empty
object or empty edge list clears it. `lookup()` returns stored document fields
without `_edges`, so a lookup result cannot be used to replace the full edge
set. Deleting a target document also removes its incident edges.

`status()["stats"]["indexes"]` and `stats()["indexes"]` include
`replay_applied_sequence` and `replay_target_sequence` for each index, along
with `replay_catch_up_required`, `catch_up_active`, and `catch_up_phase`.
These fields can be polled on an open handle to observe index progress.
Check `indexes_available` in the same `stats` object before using the index
list. If it is `false`, the apply lock was busy and that poll omitted the index
inventory; retry until it is `true` or your own deadline expires.

## API overview

- **Opening**: `create()`, `open()`, `open_readonly()`, `open_status_only()`,
  `open_hosted()`, `create_hosted()`, `open_with_options()`/
  `create_with_options()` (with an `OpenOptions` dataclass for advanced
  settings: storage kind, map size, TTL cleanup, inference resource
  budgets, busy timeout).
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
- **Backup/restore**: `backup()`, `import_backup()`, `backup_to_file()`;
  module-level `restore()`, `restore_file()` (both accept `storage=` to
  select the destination kind).
- **Embedded inference (no database)**: `Inference.open()`, `embed()`,
  `rerank()`, `chunk()`, `generate()`, `generate_stream()`,
  `generate_batch()`, `rewrite()`, `extract()`, `read()`, `transcribe()`,
  `list_models()`, `pull()` (with a `PullProgress` dataclass for progress
  callbacks; both `pull()` and `generate_stream()` accept a callback that
  can cancel the call by returning `False`). See "Embedded inference (no
  database)" above.
- **Module-level, no handle needed**: `check_file()`,
  `copy_stable_snapshot_file()`, `decode_artifact_id()`, `abi_version()`,
  `threading_mode()`, `THREADING_SERIALIZED`, `validate_abi()`.
- **Errors**: `AntflyError` (with `.code` and `.name`, e.g.
  `"ANTFLY_BUSY"`) and one subclass per C ABI error code:
  `InvalidArgumentError`, `NotFoundError`, `VersionConflictError`,
  `IntentConflictError`, `TxnNotFoundError`, `BusyError`,
  `OutcomeUnknownError`, `UnsupportedError`, `StalledError`,
  `CancelledError`, `InternalError`. Unrecognized codes raise `AntflyError`
  directly.

`create()` provisions the default `full_text_index_v0` full-text index,
matching the server's table-create behavior, so `add_index()` is only
needed for indexes beyond that default. `open()` does not create missing
files or upgrade pre-release Lite layouts; unknown or invalid files fail
explicitly.

## Development

```sh
cd py/packages/embedded
uv sync
uv run ruff format --check .
uv run ruff check .
uv run pyright
ANTFLY_LITE_REQUIRE_LIBRARY=1 uv run pytest -q
```

Tests that need `libantfly` skip cleanly when it cannot be found, unless
`ANTFLY_LITE_REQUIRE_LIBRARY=1` is set, in which case they fail instead.
`tests/test_conformance.py` runs every case in
`zig/pkg/antfly-embedded/capi-conformance/cases/*.json` through this public API, the
same declarative cases the Go and Rust bindings run.
