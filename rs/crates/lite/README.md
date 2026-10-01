# antfly-lite

Safe Rust binding for Antfly Lite, the embedded `libantfly` C ABI. It wraps
that ABI's `antfly_db` handle, so applications embed a live database
directly instead of talking to the network SDK (`antfly-sdk`) -- either a
single-file `.aflite` database (the default, [`Storage::Lite`]) or a normal
Antfly directory ([`Storage::Directory`]), selected on [`OpenOptions`].

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

**In-process, every backend.** `libantfly` runs every inference backend,
including Metal, CUDA, and ONNX, in the calling process -- there is no
separate worker process, and `ANTFLY_INFERENCE_WORKER` is not used. The
`antfly` server instead runs those backends in a worker process it can kill
and restart, because a GPU driver call or an ONNX model load has no
per-call abort; a library cannot do that to its host program, so
`libantfly` accepts the trade SQLite makes: once a call reaches the device
or driver it runs to completion (`call_timeout_ms`/[`InferenceOptions::call_timeout`],
and closing a handle, take effect only when it returns; calls on CPU
backends still stop cooperatively), and a GPU driver fault terminates the
process. See `zig/CAPI.md`'s "Inference In Process" section for the full
detail.

### Embedded inference without a database

[`Inference`] opens the same embedded inference runtime on its own, with no
database -- `Inference::open`/`open_default` (options: [`InferenceOptions`],
covering models directory, resource budgets, and a per-call timeout) and
`close`. It mirrors `Database`'s handle-safety story (`Send + Sync`, close
waits for in-flight calls, idempotent, safe from any thread).

Each method mirrors one `/ai/v1` route of the inference HTTP API and takes/
returns raw JSON bytes: `embed`, `rerank`, `chunk`, `generate`,
`generate_batch`, `rewrite`, `extract`, `read` (OCR), `transcribe`, and
`list_models`. Unlike `Database`'s `*_json` methods, these return
[`InferenceResult<Vec<u8>>`] on failure: [`InferenceError`] carries both the
mapped [`Error`] and the runtime's JSON error body
(`{"error":...,"message":...}`), since `antfly_inference_*_json` calls are
documented to always leave one behind.

```rust,no_run
use antfly_lite::{Inference, InferenceOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let inference = Inference::open(&InferenceOptions::new())?;
    let chunks = inference.chunk(r#"{"input":"Ants live in colonies. Workers gather food."}"#)?;
    println!("{}", String::from_utf8_lossy(&chunks));
    Ok(())
}
```

**Streaming generation.** `generate` always returns a complete response --
a request with `"stream": true` fails with `Error::InvalidArgument`. Use
`Inference::generate_stream` to stream instead: it sets `"stream": true`
for you and calls `on_chunk` on the calling thread for each
`chat.completion.chunk` JSON chunk as the model produces tokens.

```rust,no_run
# use antfly_lite::{Inference, InferenceOptions};
# fn run(inference: &Inference) -> Result<(), Box<dyn std::error::Error>> {
let mut on_chunk = |chunk: &[u8]| {
    println!("{}", String::from_utf8_lossy(chunk));
    true // keep going; return false to stop generating early
};
inference.generate_stream(
    r#"{"model":"owner/name","messages":[{"role":"user","content":"hi"}]}"#,
    &mut on_chunk,
)?;
# Ok(())
# }
```

`on_chunk` is a rendezvous -- generation waits for each call to return
before producing the next chunk -- so returning `false` always stops
generation at that chunk, and the call fails with `Error::Cancelled`
(wrapped in `InferenceError`). A pre-stream failure (for example a missing
model) fails like `generate`, with the JSON error attached; a mid-stream
failure fails with `Error::Internal` and a JSON body naming
`"STREAM_FAILED"`.

**Pulling models.** `Inference::pull` downloads a model into the handle's
models directory (like `antfly inference pull`), with an optional progress
callback called synchronously on the calling thread as each file starts,
every 16 MiB, and as it completes:

```rust,no_run
# use antfly_lite::{Inference, InferenceOptions, PullProgress};
# fn run(inference: &Inference) -> antfly_lite::InferenceResult<()> {
let mut on_progress = |p: &PullProgress| {
    println!("{}: {}/{} bytes", p.file, p.bytes_downloaded, p.total_bytes);
    true // keep going; return false to cancel the pull
};
inference.pull(r#"{"model":"owner/name"}"#, Some(&mut on_progress))?;
# Ok(())
# }
```

Returning `false` cancels the pull -- the call fails with `Error::Cancelled`
and the model is not installed. The progress callback is a rendezvous (the
download waits for each call to return before continuing), so cancellation
is always honored at the report where `on_progress` returns `false`, even
at the very last report of the last file. Completed files stay staged, so
pulling the same model again resumes rather than restarts. `Inference::close`
waits for a pull (or a stream) in progress; neither is cancelled by `close`
itself.

A panic inside `on_chunk`/`on_progress` is caught and does not unwind
across the C ABI boundary (unwinding through `extern "C"` is undefined
behavior): it cancels the call and is resumed once the underlying FFI call
returns.

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
- `restore`/`restore_file` stage a portable `.afb` backup into a new
  database, of the storage kind `RestoreOptions::storage` selects (a
  backup of either kind restores into either kind); `Database::backup`/
  `backup_to_file` produce one, and `Database::import_backup` imports one
  directly into an empty, already-open handle.
- `Inference::open`/`open_default` (with [`InferenceOptions`]) open an
  embedded inference runtime with no database; `embed`, `rerank`, `chunk`,
  `generate`, `generate_batch`, `rewrite`, `extract`, `read`, `transcribe`,
  `list_models`, and `pull` mirror the `/ai/v1` inference HTTP API 1:1.
  `generate_stream` streams a generate request chunk by chunk, and both it
  and `pull` accept a callback that can cancel the call by returning
  `false`, surfaced as `Error::Cancelled`. See "Embedded inference without a
  database" above.

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
    README), the same declarative suite every language binding runs, plus a
    standalone test that restores a backup into directory storage and
    reopens it.
  - `tests/inference.rs`: `Inference::open`/`open_default`, chunk/embed/
    generate/generate_stream/pull error mapping (including the JSON error
    body carried on `InferenceError` and `Error::Cancelled` from a
    cancelled `generate_stream`/`pull`), use-after-close and double-close,
    plus preconditioned tests that skip rather than fail when unmet:
    embedding with a locally installed `Qwen/Qwen3-Embedding-0.6B-GGUF*`
    model, streaming/cancelling generation with a locally installed
    `ggml-org/gemma-4-e2b-it-gguf*` model, and pulling a real model from
    the network (including cancelling on the first progress report, which
    reliably fails with `Error::Cancelled` and leaves the model uninstalled)
    when `ANTFLY_INFERENCE_PULL_TEST_MODEL` is set.

Some conformance/concurrency cases (full-text search under concurrent write
pressure) need substantially more native stack than a typical fixed-size OS
thread gets by default; these tests run their bodies on an explicitly
large-stack thread rather than relying on the test harness's default.
