# @antfly/lite

Node.js binding for embedded Antfly Lite -- the stable `libantfly` C ABI --
using [koffi](https://koffi.dev) for FFI (prebuilt binaries, no node-gyp).
Applications embed a live `.aflite` database directly in the Node process
instead of talking to the network SDK.

**Node-only.** This package uses koffi to load a native shared library and
does not work in browsers. Antfly Lite also ships a separate WebAssembly
build for browser use; this package is unrelated to that build.

## Installation

```bash
pnpm add @antfly/lite
```

You also need a built `libantfly` shared library at runtime -- see
"Library discovery" below. From the `antfly` monorepo source tree, build it
with `cd zig && zig build lite` (produces `zig/zig-out/lib/libantfly.*`);
this package does not build it for you.

## Quick start

```typescript
import { create, THREADING_SERIALIZED, threadingMode } from "@antfly/lite";

// `await using` calls Symbol.asyncDispose, which closes the handle.
await using db = await create("my.aflite");

await db.batch(
  [{ key: "doc:1", value: { title: "hello", body: "world" } }],
  BigInt(Date.now()) * 1_000_000n // nanoseconds
);

const doc = await db.lookup("doc:1"); // parsed JSON
const raw = await db.lookupRaw("doc:1"); // raw JSON bytes (Buffer)

const hits = await db.search({
  full_text_search: { match: { field: "body", text: "world" } },
  limit: 5,
});

console.log(threadingMode() === THREADING_SERIALIZED); // true
```

Without top-level `await using` support, call `close()` explicitly (ideally
in a `finally` block), or rely on the best-effort `FinalizationRegistry`
cleanup as a last resort.

## Async-first, thread-safe by design

Every operation that touches an open handle is **async** and runs on koffi's
native worker thread pool via `fn.async(...)`, so database work never blocks
the Node event loop and concurrent calls actually run in parallel. Only cheap
metadata calls that never touch a handle -- `abiVersion()`, `threadingMode()`,
`validateAbi()`, `errorCodeName()` / `errorCodeDescription()` -- are
synchronous.

`libantfly` runs in **serialized threading mode** (`antfly_threading_mode()`
== `THREADING_SERIALIZED`, like `sqlite3_threadsafe()`): any thread may call
any function on any handle concurrently. Reads (`lookup`, `search`, `scan`,
`stats`, ...) run in parallel with each other and with writes; writes on one
handle queue behind each other instead of failing with `BusyError`; and
schema/index/enrichment changes wait for in-flight calls. See
`zig/CAPI.md`'s "Thread Safety" section for the full contract. A `Database`
is safe to share across any number of concurrent async operations -- open one
handle per file, not one per call.

### koffi's worker thread pool size

koffi dispatches async calls through Node's native async-work queue, which
runs on **libuv's shared threadpool** (the same pool used by `fs`, `dns`,
`zlib`, and `crypto.pbkdf2`). Its default size is 4 threads
(`UV_THREADPOOL_SIZE`, max 1024). If your application needs more than 4
truly concurrent native calls in flight (across `@antfly/lite` and any other
libuv-threadpool consumer), set `UV_THREADPOOL_SIZE` **before the Node
process starts** (it cannot be changed at runtime):

```bash
UV_THREADPOOL_SIZE=16 node app.js
```

`koffi.config()` additionally controls per-call memory pool sizing
(`async_stack_size`, `async_heap_size`, `resident_async_pools`,
`max_async_calls`) but does not change the number of worker threads; the
defaults are sufficient for this binding since `antfly_buffer` outputs are a
small fixed-size struct (the pointed-to bytes are heap-allocated by
`libantfly`, not stack/heap-marshaled by koffi).

### Close semantics

`db.close()` is idempotent and safe to call concurrently. It:

1. Immediately rejects (with `InvalidArgumentError`) any new call made after
   `close()` has started.
2. Waits for every call this binding has already dispatched (queued on
   koffi's worker pool, or actively executing) to settle.
3. Only then calls the native `antfly_db_close`, and marks the handle closed.

This extra bookkeeping exists because `antfly_db_close` only drains C calls
that have already **entered** the library; a koffi async call can be queued
on the libuv threadpool but not yet dequeued ("entered") when `close()`
starts. Freeing the handle while such a call is still queued would be a
use-after-free once that call finally runs. `@antfly/lite` tracks its own
in-flight promises (comparable to the Go binding's `sync.RWMutex` held around
every call, exclusively in `Close`) so this can never happen.

`await using` (`Symbol.asyncDispose`) calls `close()` for you. A
`FinalizationRegistry` also closes the handle as a best-effort fallback if a
`Database` is garbage-collected without an explicit `close()` -- do not rely
on this for anything but leak mitigation; it is not deterministic.

## Library discovery

`@antfly/lite` resolves the `libantfly` shared library in this order:

1. **`ANTFLY_LIBRARY`** environment variable -- an exact file path. Throws if
   the file does not exist (fails loudly rather than silently falling
   through).
2. **`ANTFLY_LIB_DIR`** environment variable -- a directory expected to
   contain the platform library file (see below). Throws if the file is
   missing from that directory.
3. The installed **`@antfly/cli-<platform>`** package's `lib/` directory
   (the same npm platform package the `@antfly/cli` installer uses),
   resolved via `require.resolve`.
4. **`zig/zig-out/lib/`** found by walking up from this package's own
   directory -- for running against a local `antfly` monorepo source
   checkout.
5. The bare **system loader name**, resolved by the OS's normal shared
   library search path (`LD_LIBRARY_PATH` / `DYLD_LIBRARY_PATH` / `PATH` /
   rpath), matching the Go binding's fallback.

Platform file names: `libantfly.dylib` (macOS), `libantfly.so` (Linux),
`antfly.dll` (Windows).

Call `resolveLibrary()` yourself to see which tier resolved and why, without
loading the library.

### Embedded inference

`libantfly` runs inference **in-process**, in the calling process, on every
backend -- Metal, CUDA, ONNX, and CPU alike -- for both `Database` (with
`localRuntimeConfigured: true`) and the standalone `Inference` handle below.
There is no separate worker process and no `ANTFLY_INFERENCE_WORKER`
variable to configure. This is the trade SQLite makes for extensions that
touch a GPU driver: a call that reaches the device or driver has no per-call
abort, so `callTimeoutMs` and `close()` only take effect once the call
returns, and a driver fault terminates the process. Calls on CPU backends
still stop cooperatively. (The `antfly` server instead runs these backends
in a worker process it can kill and restart; a library cannot do that to its
host program.) See `zig/CAPI.md`'s "Inference In Process" section for the
full rationale.

### `Inference`: embedded inference without a database

`Inference.open(options?)` starts the embedded inference runtime with no
database attached -- models load on first use and stay cached until
`close()`. Each call sends the request JSON and returns the response JSON of
the matching `/ai/v1` route of the Antfly inference HTTP API
(`specs/openapi/inference/api.yaml`):

```typescript
import { Inference } from "@antfly/lite";

const inf = await Inference.open({ modelsDir: "/path/to/models" });
try {
  const { data } = (await inf.chunk({
    input: "Ants live in colonies. Workers gather food.",
  })) as { data: unknown[] };

  const embeddings = await inf.embed({ model: "owner/name", input: ["hello"] });
} finally {
  await inf.close();
}
```

- **Calls**: `embed`, `rerank`, `chunk`, `generate`, `generateBatch`,
  `rewrite`, `extract`, `read` (OCR), `transcribe`, `listModels` -- each with
  a bare form (parsed JSON) and a `...Raw` form (raw `Buffer`), following the
  same convention as `Database`. Binary inputs (images, audio) go inline in
  the request JSON, as base64 or `data:` URIs. Responses are always
  complete: a `generate` request with `"stream": true` rejects with
  `InvalidArgumentError`.
- **Errors**: every failed inference call still gets a JSON error body from
  `libantfly` (`{"error": ..., "message": ...}`); this binding folds it into
  the thrown `AntflyError`'s message and exposes the parsed body as
  `.body`, in addition to the usual `.code`/`.codeName`. A model that is not
  installed rejects with `NotFoundError` (`.body.error === "MODEL_NOT_FOUND"`).
- **`pull(request, onProgress?)`** downloads a model from the Hugging Face
  Hub into the handle's models directory, like `antfly inference pull`:
  `{ model: "owner/name[:variant]", variants?, token?, tasks?, capabilities?,
  projector?, maxArtifactBytes?, maxModelBytes? }` (as request JSON; field
  names follow the HTTP API, e.g. `"model"`). The optional `onProgress`
  callback receives a `PullProgress` for each file as it downloads. Unlike
  every other `Inference`/`Database` call, **`pull()` runs synchronously and
  blocks the Node event loop** for its duration -- this is intentional: koffi
  queues a registered callback's invocation to run on the JS main thread
  "as soon as the event loop has a chance to run" when the originating call
  is async, which could reorder or delay progress reports relative to the
  (already-freed) native data they point to. Calling synchronously keeps the
  whole call on one OS thread, matching the C API's "called on the calling
  thread" guarantee exactly. The call cannot be cancelled, and `close()`
  waits for it.
- **Options**: `InferenceOptions` -- `modelsDir` (defaults to
  `$ANTFLY_INFERENCE_MODELS_DIR`, else `~/.antfly/inference/models`),
  the same `*BudgetMb` resource knobs as `Database`'s `OpenOptions`, and
  `callTimeoutMs` (a deadline per call; 0 means none).
- **Lifecycle**: `close()` is idempotent, safe to call concurrently, and
  waits for in-flight calls; `Inference` implements `AsyncDisposable`
  (`await using`), same as `Database`.

## ABI validation

`validateAbi()` checks that the loaded library's `antfly_abi_version()`
(expected: 2) and `antfly_open_options` struct size match what this binding
was compiled against, throwing `AbiMismatchError` otherwise. It runs
automatically before every open/create call and before `checkFile()`; call
it yourself at startup to fail fast.

## Storage kinds

Every open/create call accepts `storage` (`Storage.Lite`, the default, or
`Storage.Directory`) via `OpenOptions.storage`:

- `Storage.Lite` -- a single-file `.aflite` database (this package's usual
  use case).
- `Storage.Directory` -- a normal single-node Antfly directory. Directory
  storage is created by opening a missing path with `open`/`openWithOptions`;
  `create`/`createWithOptions` only create `.aflite` files.

A portable `.afb` backup (`backup()`) round-trips across storage kinds: it
restores into either kind with the module-level `restore`/`restoreFile`, and
imports into an *empty* database of either kind with `importBackup()`.
`restore(path, backup, { storage, replace })` and
`restoreFile(path, backupPath, { storage, replace })` select the destination
kind with `storage` (default `Storage.Lite`); the `.aflite` suffix is only
required client-side when the destination is `Storage.Lite`.

## JSON conventions

Every request parameter that accepts a JSON body (`request`, `config`,
`schema`, ...) accepts a plain object (JSON.stringify'd), a JSON string, or
raw UTF-8 bytes (`Buffer` / `Uint8Array`).

Every JSON-returning operation has two forms:

- The bare name (e.g. `lookup`, `search`, `status`) returns **parsed JSON**
  (typed as `unknown`, or a specific interface for `Status` /
  `Capabilities` / the maintenance reports).
- The `...Raw` suffix (e.g. `lookupRaw`, `searchRaw`, `statusRaw`) returns
  the **raw JSON bytes** as a `Buffer`, skipping the parse.

The packed **wire** searches (`denseSearchWire`, `textMatchWire`,
`textTermWire`, `textMatchPhraseWire`) are a different, non-JSON binary
format matching the server's wire protocol; they always take and return raw
bytes (`Buffer` / `Uint8Array`), never JSON.

`getRaw(key)` is the one exception to the naming scheme above: it mirrors
`antfly_db_get_raw` / the Go binding's `Raw(key)`, returning the document's
raw stored bytes (not JSON at all, since a document need not be JSON at the
storage layer).

Timestamps and versions that are `uint64_t` in the C ABI (`timestampNs`,
`expectedVersion`, `commitVersion`, `busyTimeoutMs`, ...) accept both
`number` and `bigint`; use `bigint` where precision matters; a nanosecond
Unix timestamp exceeds `Number.MAX_SAFE_INTEGER`, so `commitVersion()`
always returns `bigint`.

## Errors

Every failed call rejects with an `AntflyError` subclass carrying:

- `.code` -- the numeric `antfly_error_code`.
- `.codeName` -- the stable symbolic name, e.g. `"ANTFLY_BUSY"`, matching
  `antfly_error_code_name()`.
- `.message` -- built from `antfly_error_code_description()`.

Subclasses: `InvalidArgumentError` (1), `NotFoundError` (2),
`VersionConflictError` (3), `IntentConflictError` (4), `TxnNotFoundError` (5),
`BusyError` (6), `OutcomeUnknownError` (7, not safe to retry automatically --
publication may have succeeded but crash durability could not be confirmed),
`UnsupportedError` (8, not transient), `StalledError` (9, a bounded drain
like `runUntilIdle` made no progress and gave up), `InternalError` (255).

## API surface

Mirrors `go/pkg/lite`'s surface idiomatically:

- **Open/create**: `create`, `open`, `openReadonly`, `openStatusOnly`,
  `openHosted`, `createHosted`, `openWithOptions`, `createWithOptions`, plus
  `OpenOptions` (including `storage`), `Storage`, `OpenMode`, `Profile`,
  `TxnStatus`, `GraphDirection`, `InferenceMode`, `THREADING_SERIALIZED`.
- **`Database`**: `close()`, `[Symbol.asyncDispose]`; `batch`/`batchJson`;
  `lookup`/`lookupRaw`/`getRaw`; `scan`/`search`/`stats`; `status`,
  `capabilities`, `check`, `pendingWorkStats`, `runUntilIdle` /
  `runUntilIdleStatus`, `replayGeneratedEnrichments`; `getSchema`/
  `setSchema`; `listIndexes`/`addIndex`/`deleteIndex` (returns `boolean`);
  `listEnrichments`/`addEnrichment`/`deleteEnrichment`; graph
  (`edges`/`neighbors`/`traverseEdges`/`executeGraphQueries`/
  `findShortestPath`/`findKShortestPaths`/`matchPattern`); wire searches
  (`denseSearchWire`/`textMatchWire`/`textTermWire`/`textMatchPhraseWire`);
  `aggregateHits`, `lookupArtifact`; `extractEnrichments`/
  `computeEnrichments`; transactions (`beginTransaction`/`writeTransaction`/
  `resolveTransaction`/`transactionStatus`/`commitVersion` -- 16-byte
  transaction ids accepted as a `Uint8Array(16)` or a 32-char hex string);
  `backup`/`importBackup`, `backupToFile`; `compact`/`vacuum`/
  `copyStableSnapshot`.
- **Module-level**: `checkFile`, `restore`/`restoreFile` (both take
  `RestoreOptions` -- `{ storage, replace }`), `copyStableSnapshotFile`,
  `decodeArtifactId`, `abiVersion`, `threadingMode`, `validateAbi`.
- **`Inference`**: `Inference.open(options?)`, `close()`,
  `[Symbol.asyncDispose]`; `embed`/`rerank`/`chunk`/`generate`/
  `generateBatch`/`rewrite`/`extract`/`read`/`transcribe`/`listModels`
  (each with a `...Raw` form); `pull(request, onProgress?)`; plus
  `InferenceOptions`, `PullProgress`, `validateInferenceAbi`. See "Embedded
  inference" above.

See `src/index.ts` for the full export list and `src/types.ts` for typed
`Status` / `Capabilities` / report interfaces.

## Testing

```bash
pnpm run typecheck
pnpm run lint
pnpm run build
pnpm run test                                    # skips native-library tests cleanly if libantfly isn't found
ANTFLY_LITE_REQUIRE_LIBRARY=1 pnpm run test       # fails instead of skipping if libantfly isn't found
ANTFLY_LIBRARY=/nonexistent pnpm run test         # exercises the clean-skip path directly
```

`test/conformance.test.ts` runs every case under
`zig/pkg/antfly/capi-conformance/cases/*.json` through this public API,
mirroring `go/pkg/lite/conformance_cgo_test.go`'s semantics, including the
`storage`-typed opens and the `import_backup`/`restore_open` cross-storage
cases. `test/storage.test.ts` adds coverage beyond conformance: restoring a
`.aflite` backup into directory storage and reopening it. `test/
concurrency.test.ts` covers mixed concurrent operations, `close()` racing
in-flight calls, `busyTimeoutMs` behavior, and that concurrent searches
overlap instead of serializing on the event loop. `test/errors.test.ts` and
`test/discovery.test.ts` include pure tests that need no native library at
all. `test/inference.test.ts` covers the `Inference` handle: open/close,
calls that need no model (`chunk`, `listModels`), a missing-model
`NotFoundError`, `pull({})`/streaming-`generate` `InvalidArgumentError`
cases, an optional real-embedding test gated on a locally installed Qwen
model, and a network-gated `pull()` test (`ANTFLY_INFERENCE_PULL_TEST_MODEL`,
e.g. `sparse-encoder-testing/splade-bert-tiny-nq-onnx`) that exercises
progress callbacks against the Hugging Face Hub.

## License

Apache-2.0
