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
  [root.zig](../antfly/src/embedded/root.zig)
  as the canonical shared embedded module entrypoint
- [embedded_root.zig](../antfly/src/embedded_root.zig)
  is a compatibility shim

The shared embedded module sits on the normal storage DB engine path:

- [db.zig](../antfly/src/embedded/db.zig)
  wraps [mod.zig](../antfly/src/storage/db/mod.zig)
- query execution comes from:
  - [search_exec.zig](../antfly/src/storage/db/query/search_exec.zig)
  - [graph_exec.zig](../antfly/src/storage/db/query/graph_exec.zig)
  - [projection.zig](../antfly/src/storage/db/query/projection.zig)
  - [result_shape.zig](../antfly/src/storage/db/query/result_shape.zig)

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

`embedded/db.zig` has two execution profiles:

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

## Host Storage Contracts

Browser/WASM and other hosted runtimes should not need to emulate a full POSIX
filesystem. They need to provide the storage contracts that the embedded path
uses.

There are two storage layers:

- `Storage`
  - file/path-oriented
  - used by durable LSM internals
  - defined in
    [storage_io.zig](../antfly/src/storage/lsm_backend/storage_io.zig)
- `ObjectStorage`
  - blob/object-oriented
  - used by serverless/object-backed consumers
  - defined in
    [object_storage.zig](../antfly/src/storage/object_storage.zig)

To make embedding simpler, one host can expose both through:

- [host_environment.zig](../antfly/src/storage/host_environment.zig)

Recommended browser mapping:

- `Storage`
  - back with OPFS or another file-like store
  - implement `createDirPath`, `readFileAlloc`, `readFileRangeAlloc`,
    `writeFileAbsolute`, `renameAbsolute`, `deleteFileAbsolute`, `deleteTree`,
    and `nowNs`
- `ObjectStorage`
  - back with IndexedDB, OPFS blobs, or remote object APIs exposed by JS
  - implement `bucketExists`, `makeBucket`, `putObject`, `getObject`,
    `getObjectAttributes`, `statObject`, `deleteObject`, and `listObjects`

The host may use the same physical backing store for both. The split is
logical, not necessarily operational.

Available building blocks:

- [storage_io.zig](../antfly/src/storage/lsm_backend/storage_io.zig)
  - `NativeStorage`
  - `MemoryStorage`
  - `HostStorage`
- [object_storage.zig](../antfly/src/storage/object_storage.zig)
  - shared object/blob facade
  - `HostObjectStorage`
- [host_environment.zig](../antfly/src/storage/host_environment.zig)
  - bundle exposing both host-facing contracts
- [api.zig](../antfly/src/embedded/api.zig)
  - embedded function-call surface over `DB`
  - intended for JS/WASM bindings that want direct calls instead of a local
    HTTP server
- [db.zig](../antfly/src/embedded/db.zig)
  - includes `openHosted(...)`
- [antfly_wasm.zig](../../examples/antfly_wasm.zig)
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
   `examples/antfly_wasm.zig`
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

## WASM Smoke

This package has a hosted/shared WASM smoke path built on the shared embedded
DB/API surface, not the older portable-only path.

Build:

- `zig build antfly-embedded-wasm-smoke`

Run under Node:

- `zig build antfly-embedded-wasm-smoke-run`

Artifacts are installed under:

- `zig-out/antfly-embedded-wasm-smoke/`

The smoke uses:

- hosted embedded profile
- `MemoryStorage`
- manual derived indexing via `runUntilIdle()`
- shared embedded `Api`
- close/reopen on the same host storage to prove text-index persistence
- package client helper from `pkg/antfly-embedded/wasm_client.mjs`
- host-provided remote template rendering for `remoteText`

## Validation

- `zig build antfly-embedded-test --summary failures`
- `zig build antfly-embedded-wasm-smoke`
- `zig build antfly-embedded-wasm-smoke-run`

## Current Limits

This note is about the hosted embedded/WASM seam. It does not imply that all of
`antfly-zig` is browser-ready today.

Remaining concerns outside this seam include:

- non-storage native dependencies in other modules
- networking/runtime assumptions outside storage
- browser-specific policy choices for quota, persistence, and concurrency
- full parity between the shared hosted embedded path and every native-only
  subsystem

The next useful adapter should live outside core storage logic:

- browser JS host
- `HostStorage` backed by OPFS
- `HostObjectStorage` backed by IndexedDB or OPFS blobs
