# antfly-lite

Safe Rust binding for Antfly Lite, the embedded `libantfly` C ABI. It wraps
the Lite open/storage profile in that ABI, so applications embed a live
`.aflite` database directly instead of talking to the network SDK
(`antfly-sdk`).

This crate mirrors the reference Go binding (`go/pkg/lite`) idiomatically:
[`Database`] is a `Send + Sync` handle safe for concurrent use from any
thread, most operations are exposed as `*_json` methods that take request
bytes and return response bytes (the same wire-level JSON contract shared
with the Antfly server API and the other language bindings), and a
default-on `serde` feature layers typed convenience wrappers (`Status`,
`Capabilities`, `CheckReport`, ...) on top.

## Example

```rust,no_run
use antfly_lite::{Database, OpenOptions, WriteIntent};

fn main() -> Result<(), antfly_lite::Error> {
    let db = Database::create("my.aflite", &OpenOptions::new())?;
    db.batch(&[WriteIntent::put("doc:1", r#"{"title":"hello","body":"world"}"#)], 1)?;
    db.run_until_idle()?;
    let doc = db.lookup_json("doc:1")?;
    let hits = db.search_json(
        r#"{"full_text_search":{"match":{"field":"body","text":"world"}},"limit":5}"#,
    )?;
    println!("{}\n{}", String::from_utf8_lossy(&doc), String::from_utf8_lossy(&hits));
    Ok(())
}
```

Every thread that calls into a `Database` needs at least
`antfly_lite::MIN_THREAD_STACK_SIZE` (8 MiB) of stack. The main thread has
that on Linux and macOS, but Rust's spawned threads default to 2 MiB, so
size them explicitly:

```rust,no_run
# use std::sync::Arc;
# fn run(db: Arc<antfly_lite::Database>) {
std::thread::Builder::new()
    .stack_size(antfly_lite::MIN_THREAD_STACK_SIZE)
    .spawn(move || db.stats_json())
    .unwrap();
# }
```

Async runtimes need the same for the threads that make these blocking
calls, for example Tokio's `thread_stack_size` on the runtime builder.

## Building against `libantfly`

`antfly-lite-sys`'s `build.rs` (see that crate's README) locates the
`libantfly` dylib via `ANTFLY_LIB_DIR`, or `zig/zig-out/lib` inside an
`antfly` source checkout. Enable the `libantfly` Cargo feature to actually
link it:

```sh
cd zig && zig build capi   # or: zig build, for the full antfly CLI
cd ../rs
cargo test -p antfly-lite --features libantfly
```

Outside the source tree, set `ANTFLY_LIB_DIR` to point at an installed
`libantfly`'s `lib` directory.

### Embedded inference

`libantfly` always links the standalone inference runtime in-process, the
same as the `antfly` executable. Setting `local_runtime_configured` on
[`OpenOptions`] yields `local_embedded` behavior: an embedded model runtime
that runs chunker, embedder, and extractor producers configured with
`"provider": "antfly"` and no `api_url` locally instead of failing or
requiring a remote URL.

**Worker executable resolution.** GPU-hosted and driver-backed backends
(Metal, CUDA, ONNX, PJRT) construct and, for Metal/CUDA/PJRT, execute models
in a separate, replaceable worker process rather than inside your process --
crash containment for an unabortable driver call or GPU state corruption
means the process that made the call must be the one that gets killed and
respawned, and that must never be your host process. A Rust binary linking
`libantfly` has no `antfly`-shaped `argv[0]` to re-exec, so the runtime
resolves the worker executable itself, in order: the `ANTFLY_INFERENCE_WORKER`
environment variable (a path to the worker executable, typically an `antfly`
binary); otherwise an `antfly` binary next to the loaded `libantfly`;
otherwise `antfly` on `PATH`. If none of these resolve, calls into a
process-isolated backend fail with a clear error naming
`ANTFLY_INFERENCE_WORKER` -- set it (or place an `antfly` binary next to
`libantfly` or on `PATH`) before opening a `local_runtime_configured` handle
that needs Metal/CUDA/ONNX/PJRT models.

## Thread safety

`Database` is `Send + Sync` and safe for concurrent use from any thread,
like `*sql.DB` in Go: share one handle rather than opening one per thread.
`libantfly` runs in serialized threading mode
(`threading_mode() == THREADING_SERIALIZED`): reads such as `search_json`,
`lookup_json`, and `scan_json` run in parallel with each other and with
writes, `batch` and transaction calls on one handle queue instead of failing
with `Busy`, and schema or index changes wait for in-flight calls.

`close` takes `&self`, not `self` by value, specifically so it can be called
on a `Database` shared across threads (e.g. `Arc<Database>`) without every
other clone having to be dropped first -- calls made after (or racing) close
return `Error::InvalidArgument` rather than touching a freed handle.
`Drop` also closes, for callers who never need to close early. Both are
idempotent and safe to call concurrently or more than once.

Internally, `Database` gates calls through a small hand-rolled
reader/writer lock rather than `std::sync::RwLock`: `std`'s `RwLock` makes
no fairness guarantees, and on platforms like macOS a steady stream of
readers can starve a pending writer indefinitely, which would make `close`
hang under sustained concurrent read load. The internal gate instead blocks
*new* reads once a close is requested (like Go's `sync.RWMutex`), guaranteeing
close completes in bounded time.

Only one writer handle may be open per file at a time, across processes. Set
`OpenOptions::busy_timeout` to wait for another writer to close instead of
failing immediately with `Busy`, like `sqlite3_busy_timeout`.

## API surface

- `Database::open`/`create` (with [`OpenOptions`]), plus `open_default`,
  `create_default`, `open_readonly`, `open_status_only`, `open_hosted`, and
  `create_hosted` convenience constructors.
- Raw `*_json` methods take `impl AsRef<[u8]>` and return `Vec<u8>`, matching
  the C ABI's JSON contract 1:1: `batch_json`, `lookup_json`, `get_raw`,
  `scan_json`, `search_json`, the packed wire search variants, schema/index/
  enrichment administration, graph queries, transactions, and maintenance
  (backup/export/import, check, compact, vacuum, stable snapshots).
- With the default `serde` feature, typed convenience methods
  (`status`, `capabilities`, `check`, `vacuum`, `compact`,
  `copy_stable_snapshot`, `pending_work_stats`, ...) parse those JSON
  payloads into typed structs, returning `TypedResult<T>` (an FFI error or a
  JSON decode error).
- `restore`/`restore_backup`/`restore_file`/`restore_backup_file` stage a
  portable `.afb` backup into a new `.aflite` database; `Database::backup`/
  `export`/`backup_to_file`/`export_to_file` produce one.

See the crate's rustdoc for the full method list.

## Testing

- `cargo test -p antfly-lite` (no feature) runs only pure, non-linking
  tests (`tests/pure.rs`) -- value types, error name/description tables,
  and a compile-time `Database: Send + Sync` assertion. It needs no dylib.
- `cargo test -p antfly-lite --features libantfly` additionally runs:
  - `tests/errors.rs`: cross-checks `Error`'s names/descriptions against the
    live `antfly_error_code_name`/`antfly_error_code_description`, plus ABI
    validation and struct-size checks.
  - `tests/concurrency.rs`: a handle shared by threads doing batch/search/
    scan/lookup/stats/run-until-idle concurrently, close racing in-flight
    calls, and `busy_timeout` behavior.
  - `tests/conformance.rs`: runs every case under
    `zig/pkg/antfly/capi-conformance/cases/*.json` (see that directory's
    README), the same declarative suite every language binding runs.

Some conformance/concurrency cases (full-text search under concurrent write
pressure) need substantially more native stack than a typical fixed-size OS
thread gets by default; these tests run their bodies on an explicitly
large-stack thread rather than relying on the test harness's default.
