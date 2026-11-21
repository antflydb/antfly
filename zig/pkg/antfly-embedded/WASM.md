# antfly-embedded WASM

This is the canonical note for the embeddable Antfly WASM/hosted package
surface. It covers the Zig package exports, JS client helper, hosted storage
contracts, remote template rendering, and smoke-test path.

The embedded surface is intended for:

- JavaScript/WASM bindings
- Rust FFI callers
- other direct function-call hosts

It is intentionally narrower than the full Antfly root module surface.

## Package Surface

Entrypoints:

- `pkg/antfly-embedded/src/root.zig`
- `pkg/antfly-embedded/wasm_client.mjs`

Primary exports:

- `db`
- `api`
- `host_environment`
- `object_storage`
- `lsm_backend`
- `storage_backend`
- `db_types`

Current implementation note:

- this package targets
  [root.zig](src/engine/root.zig)
  as the canonical shared embedded module entrypoint
- [embedded_root.zig](src/embedded_root.zig)
  is a compatibility shim

The shared embedded module sits on the normal storage DB engine path:

- [db.zig](src/engine/db.zig)
  wraps [mod.zig](src/storage/db/mod.zig)
- query execution comes from:
  - [search_exec.zig](src/storage/db/query/search_exec.zig)
  - [graph_exec.zig](src/storage/db/query/graph_exec.zig)
  - [projection.zig](src/storage/db/query/projection.zig)
  - [result_shape.zig](src/storage/db/query/result_shape.zig)

## Hosted Entry Points

Preferred open paths for browser/WASM-style hosts:

- `db.DB.openHosted(...)`
- `api.Api.openHosted(...)`

Use those when the host provides `Storage` and drives maintenance explicitly
with `runUntilIdle()`.

Native embedded hosts can also call:

- `embedded.api.Api`
- `embedded.db.DB`

The embedded API currently exposes:

- `batchJson`
- `lookupJson`
- `scanJson`
- `searchJson`
- `capabilitiesJson`
- `statsJson`
- `listIndexesJson`
- `listEnrichmentsJson`
- `openHosted`
- `graphCatalogJson`
- `cypherJson` / `cypherJsonWithDiagnostic`
- `graphWriteStatusJson`

`src/engine/db.zig` has two execution profiles:

- `native`
  - the default shared DB wrapper with native background runtimes
- `hosted`
  - disables enrichment, TTL cleanup, and transaction recovery runtimes
  - uses the manual derived executor so hosts can advance indexing explicitly
    via `runUntilIdle()`

## JS Client Helper

For JS/WASM hosts, use:

- `pkg/antfly-embedded/wasm_client.mjs`

It wraps the exported function-call ABI and provides:

- `put` / `putMany` / `delete`
- `lookup` / `scan` / `search`
- `renderRemoteTemplate`
- `stats`
- `pendingWorkStats`
- `capabilities`
- `listIndexes` / `listEnrichments`
- `addIndex` / `addIndexJson`
- `graphCatalog` / `graphCatalogJson`
- `cypher` / `cypherJson`
- `graphWriteStatus` / `graphWriteStatusJson`

## Cypher

Hosted Zig and WASM use the shared Cypher compiler, typed value format,
immutable graph materialization, postimage planner, and native transaction
ledger. A hosted graph maps labels and relationship indexes to the configured
single table. Native C/Lite can map multiple local tables; distributed graphs
use the authenticated server API.

The `antfly.search` Cypher seed procedure uses authenticated server execution.
Hosted graph calls report `CypherSearchUnavailable` for this procedure. The
ordinary hosted `search` API remains available separately.

```js
api.addIndex({ name: "links", kind: "graph", config_json: "{}" });
api.graphCatalog({
  action: "put", name: "knowledge",
  definition: {
    labels: [{ name: "Document", table: "docs" }],
    relationships: [{ name: "LINKS", source_label: "Document",
      target_label: "Document", index: "links", stored_type: "links" }],
  },
});
const result = api.cypher("knowledge", {
  query: "MATCH (n:Document) RETURN n LIMIT 20",
});
```

Writes require a UUID in `write_request_id`. The JavaScript helper supplies
one with `crypto.randomUUID()` when the caller omits it; direct Zig callers
supply their own. Retain this ID and consult `graphWriteStatus(id)` when a
diagnostic reports an unknown outcome. Reusing an admitted ID rejects the
query rather than executing it again. Errors expose `diagnostic.code`, a
source span, the write outcome, and the transaction ID when available.

Catalog replacements and drops require the current decimal
`expected_revision`. Graph bindings pin table and index identities across
reopen. Raw embedded calls reject protected tables because these entry points
do not carry authenticated principal authority. Hosts drive graph indexing
with `runUntilIdle()` alongside other derived indexes.

## Browser Database Files in OPFS

`HostFileStorage` (`src/storage/host_file_storage.zig`) implements `Storage`
over a small synchronous file-operation table supplied by the host: size,
read, write/append, sync, rename, remove (file or tree), list and a clock. In
the WASM bundle, the table is the `antfly_host_fs_*` env imports. The JS client
fills them from `instantiate*({ fileSystem })`, and `openFileSystem(path)` opens
a hosted database there. `HostFileStorage.MemoryHost` is an in-memory table for
native tests.

`antfly_embedded_opfs_fs.mjs` provides that table on the Origin Private File
System. It must run in a dedicated worker, because synchronous access handles
exist only there:

```js
import { openOpfsFileSystem, withCapacity } from "./antfly_embedded_opfs_fs.mjs";
const dir = await (await navigator.storage.getDirectory()).getDirectoryHandle("my-db", { create: true });
const fs = await openOpfsFileSystem(dir);              // throws OpfsBusy if another tab holds it
const api = await instantiateAntflyEmbeddedApiFromBytes(wasmBytes, { fileSystem: fs });
await withCapacity(fs, () => api.openFileSystem("/db"));  // new or existing database
await withCapacity(fs, () => api.putMany(docs));      // top up free slots before each call
await withCapacity(fs, () => api.runUntilIdle());
api.close();
fs.close();                                            // release the handles
```

Antfly's storage calls are synchronous. OPFS can only open, rename or delete
files asynchronously, so the backend keeps a pool of pre-opened physical "slot"
files and stores the logical namespace inside them, like SQLite's
`opfs-sahpool` VFS:

- **Slot layout.** Each slot has two 2 KiB header records followed by file
  data. A record holds a generation, the logical path and a CRC-32; the newest
  valid record wins, and an empty path means free.
- **Namespace changes.** Create, rename and delete overwrite the slot's older
  record and flush before returning. A torn header write therefore leaves the
  previous record in force, which makes each of those operations
  all-or-nothing.
- **Rename.** The moved slot's record (with a fresh generation) is flushed
  before the replaced destination is freed. If a crash leaves both, the higher
  generation wins on reopen, so `rename_is_atomic` holds.
- **Data durability.** Data becomes durable at `sync`, as with `fsync`.
  Deleted slots are truncated and reused.
- **Capacity.** Slots can only be added asynchronously, between database
  calls, so the free reserve must cover one call's peak file demand. That peak
  is bursty: a first vector index build writes about 128 block files and up to
  about 128 spool files before deleting them, measured at 159 files for 25
  documents and 265 for 2,000. Later calls need about 10.
  - Defaults: 512 slots with 384 kept free. Opening them takes about half a
    second in Chromium.
  - The pool tracks each call's peak and keeps at least twice the largest.
  - `withCapacity(fs, fn)` tops up the reserve before a call.
  - If a call still runs out, it fails with `OpfsPoolExhausted`. Don't retry
    it. The hosted profile has no durable index-repair checkpoint, so an index
    interrupted mid-build stays unusable even after a reopen. Treat the copy
    as a cache: delete it and rebuild from the source of truth.
- **One tab at a time.** Sync access handles are exclusive, so a second tab gets
  `OpfsBusy`. Close the pool to hand it over.

The database is no longer resident in WASM memory; only the engine's caches
are. `wasm_opfs_fs_test.mjs` simulates OPFS with separate durable and volatile
bytes. It checks the contract, unsynced-data loss, interrupted renames, torn
header records, deletion, capacity exhaustion and the exclusive lock. The WASM
smoke test runs a database with vector and hybrid search through the pool and
reopens it in a fresh module.

## Host Storage Contracts

Browser/WASM and other hosted runtimes should not need to emulate a full POSIX
filesystem. They need to provide the storage contracts that the embedded path
uses.

There are two storage layers:

- `Storage`
  - file/path-oriented
  - used by durable LSM internals
  - defined in
    [storage_io.zig](src/storage/lsm_backend/storage_io.zig)
- `ObjectStorage`
  - blob/object-oriented
  - used by serverless/object-backed consumers
  - defined in
    [object_storage.zig](src/storage/object_storage.zig)

To make embedding simpler, one host can expose both through:

- [host_environment.zig](src/storage/host_environment.zig)

Recommended browser mapping:

- `Storage`
  - back with OPFS or another file-like store
  - implement `createDirPath`, `readFileAlloc`, `readFileRangeAlloc`,
    `writeFileAbsolute`, `renameAbsolute`, `deleteFileAbsolute`, `deleteTree`,
    `fileSize` (`file_size` in the vtable), and `nowNs`
- `ObjectStorage`
  - back with IndexedDB, OPFS blobs, or remote object APIs exposed by JS
  - implement `bucketExists`, `makeBucket`, `putObject`, `getObject`,
    `getObjectAttributes`, `statObject`, `deleteObject`, and `listObjects`

For durable atomic publication, provide either a custom `begin_atomic_write`
whose sink implements durable atomic finish, or all three fallback capabilities:
`sync_contents_absolute`, `sync_parent_absolute`, and `rename_is_atomic = true`.
The fallback writes a temporary sibling, syncs its contents, atomically renames
it, then syncs the destination's parent directory. Implementing rename alone
is insufficient: `beginAtomicWrite` returns `DurableAtomicWriteUnsupported`
without these capabilities. A host must only declare atomic rename when its
backing store guarantees it across failures.

These durability capabilities and the required callbacks above are distinct
from optional performance hooks such as range batching, immutable read sources,
and cache advice. Optional hooks can be omitted without weakening durability.

The host may use the same physical backing store for both. The split is
logical, not necessarily operational.

Available building blocks:

- [storage_io.zig](src/storage/lsm_backend/storage_io.zig)
  - `NativeStorage`
  - `MemoryStorage`
  - `HostStorage`
- [object_storage.zig](src/storage/object_storage.zig)
  - shared object/blob facade
  - `HostObjectStorage`
- [host_environment.zig](src/storage/host_environment.zig)
  - bundle exposing both host-facing contracts
- [api.zig](src/engine/api.zig)
  - embedded function-call surface over `DB`
  - intended for JS/WASM bindings that want direct calls instead of a local
    HTTP server
- [db.zig](src/engine/db.zig)
  - includes `openHosted(...)`
- [wasm_root.zig](src/wasm_root.zig)
  - runnable shared embedded WASM example using `Api.openHosted(...)`
  - proves close/reopen persistence over host-provided `Storage`

Typical hosted flow:

1. host creates one shared context
2. host exposes `HostStorage` callbacks for durable LSM
3. host exposes `HostObjectStorage` callbacks for blobs/artifacts
4. app constructs a `HostEnvironment`
5. durable LSM opens with `.storage = host_env.storage.storage()`
6. object/blob consumers use `host_env.object_storage.objectStorage()`
7. app opens with `embedded.db.DB.openHosted(...)` or
   `embedded.api.Api.openHosted(...)`
8. JS/WASM callers can also use the exported FFI shown in
   `src/wasm_root.zig`
9. host drives maintenance with `runUntilIdle()`

## Why Two Storage Interfaces

`Storage` and `ObjectStorage` intentionally remain separate.

`Storage` is better for:

- temp sibling writes plus rename
- small durable engine files
- directory/prefix cleanup

`ObjectStorage` is better for:

- immutable or versioned blobs
- serverless manifests and artifacts
- future external segment/blob families
- remote/cloud-backed transports

Trying to collapse them into one interface would make both worse.

## Remote Template Rendering

Hosted/freestanding template support:

- local template rendering is built in
- local configured chunking for `antfly` / `mock` is built in
- built-in native remote template rendering is not available on freestanding
- remote template helpers like `remoteText` can be provided by the host via
  `remoteTemplateRenderer` when instantiating `wasm_client.mjs`
- `wasm_client.mjs` exports `createGoParityRemoteTemplateRenderer(...)` and
  `formatDotpromptMediaUrl(...)` to help JS hosts mirror Go
  `remoteMedia` / `remotePDF` / `remoteText` behavior

Example:

```js
const remoteTemplateRenderer = createGoParityRemoteTemplateRenderer({
  remoteText({ url }) {
    return `hosted:${url}`;
  },
  remoteMedia({ url, mode }) {
    if (mode === "extract") return `pdf-text:${url}`;
    return formatDotpromptMediaUrl(`hosted:${mode}:${url}`);
  },
});

const api = await instantiateAntflyEmbeddedApiFromBytes(wasmBytes, {
  remoteTemplateRenderer,
});
```

## License

First-party embedded database, JavaScript bindings, and inference WASM sources
are Apache-2.0. Third-party components retain their compatible upstream terms.
The installed bundle contains `LICENSE`, `LICENSING.md`, `THIRD_PARTY_NOTICES.md`,
and complete notices under `LICENSES/third-party`.

## WASM Smoke

This package has a hosted/shared WASM smoke path built on the shared embedded
DB/API surface, not the older portable-only path.

Build and install the bundle from the `zig` directory (without running tests):

- `zig build wasm`

Build the bundle and run its smoke test under Node:

- `zig build wasm-test`

The browser artifact uses ReleaseSafe to avoid LLVM inference miscompilation
and browser limits on unoptimized functions. Distributable modules omit DWARF
debug information by default while retaining runtime safety checks. Use
`-Dwasm-strip=false` for a debugger build. That profile reserves 32 GiB for
compiler admission and needs a corresponding `--maxrss` budget; the stripped
profile retains its 20 GiB reservation.

Artifacts are installed under:

- `zig-out/antfly-wasm/`

The smoke uses:

- hosted embedded profile
- `MemoryStorage`
- manual derived indexing via `runUntilIdle()`
- shared embedded `Api`
- close/reopen on the same host storage to prove text-index persistence
- package client helper from `pkg/antfly-embedded/wasm_client.mjs`
- host-provided remote template rendering for `remoteText`
- a separate WASM fixture for 64-bit counters, compare-exchange, float16 distances,
  secure host entropy, and freestanding clocks

The JavaScript host supplies secure entropy through `crypto.getRandomValues`.
Opening a database fails explicitly if secure entropy is unavailable; it never
substitutes a predictable random generator.

## Validation

- `zig build embedded-test --summary failures` (database/API lifecycle tests)
- `zig build antfly-embedded-test --summary failures` (package compile surface)
- `zig build wasm`
- `zig build wasm-test`

## Current Limits

This note is about the hosted embedded/WASM seam. It does not imply that all of
`antfly-zig` is browser-ready today.

Remaining concerns outside this seam include:

- non-storage native dependencies in other modules
- networking/runtime assumptions outside storage
- browser-specific policy choices for quota, persistence, and concurrency
- full parity between the shared hosted embedded path and every native-only
  subsystem

Adapters live outside core storage logic. File-backed `Storage` on OPFS is
covered above; the next useful one is `HostObjectStorage` backed by IndexedDB
or OPFS blobs for object-backed consumers.
