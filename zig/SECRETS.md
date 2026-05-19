# Antfly Zig Secrets Store

## Goal

Antfly-zig needs a small local secrets store for swarm mode. The store backs the
`/secrets` API, resolves `${secret:key}` references in configuration, and lets
operators keep provider credentials out of ordinary config files.

The current implementation is intentionally simple: one JSON file on local disk
is loaded into memory at process startup, then API writes update that in-memory
map and persist it back to disk atomically. That works for API-managed secrets,
but it does not notice external edits to the secrets file while Antfly is
running.

This document captures the current design and the plan for dynamic reloads and
runtime secret rotation.

## Current Implementation

The core store lives in `zig/pkg/antfly/src/common/secrets.zig`.

`FileStore` owns:

- `alloc`
- `path`
- `entries: StringArrayHashMapUnmanaged(StoredSecret)`

Each stored secret has:

- `value`
- `created_at_ns`
- `updated_at_ns`

The persisted file shape is:

```json
{
  "secrets": [
    {
      "key": "openai.api_key",
      "value": "sk-...",
      "created_at_ns": 123,
      "updated_at_ns": 456
    }
  ]
}
```

The file is currently plaintext JSON. The API never returns secret values, but
the file itself must be protected by filesystem permissions.

## Current Load And Write Flow

Startup:

1. Swarm runtime resolves the store path, normally `<base>/secrets.json`.
2. `FileStore.init(alloc, path)` duplicates the path and calls `load()`.
3. `load()` reads and parses the JSON file if it exists.
4. Parsed entries are copied into `entries`.

Reads:

1. `list()` returns stored secret metadata plus environment-only API key secrets.
2. `getOwned()` returns the stored value if present.
3. If the key is not stored, `getOwned()` maps the key to an environment variable
   and returns the environment value if set.
4. `resolveValueOwned()` resolves `${secret:key}` references through `getOwned()`.
5. `resolveReferenceOwned()` resolves through a `FileStore` when one is supplied,
   or through environment variables only when there is no store.

Writes:

1. `put()` validates the key, updates `entries`, calls `persist()`, and returns
   metadata.
2. `delete()` removes the entry from `entries`, calls `persist()`, and returns
   whether an entry existed.
3. `persist()` serializes all entries, writes a temporary file, then renames it
   over the configured store path.

The `/secrets` API is wired through:

- `zig/pkg/antfly/src/api/http_server.zig`
- `zig/pkg/antfly/src/api/httpx_handler.zig`

In swarm mode those handlers receive `ApiHttpServerConfig.secret_store`. In
multi-node mode the store is absent and secret management writes return 503.

## Environment Fallback

Secret keys map to environment variables by uppercasing and replacing `.`, `-`,
and `:` with `_`.

Examples:

- `openai.api_key` maps to `OPENAI_API_KEY`
- `anthropic.api_key` maps to `ANTHROPIC_API_KEY`
- `aws.secret` maps to `AWS_SECRET`

Environment discovery currently lists variables ending in `_API_KEY` and maps
them back to `*.api_key` secret names. Stored secrets and environment secrets
can both exist; the list API reports that as `configured_both`.

Lookup precedence is:

1. File store entry
2. Environment variable fallback

## Current Limitation

`FileStore.load()` runs only during `FileStore.init()`. After startup, `entries`
is the source of truth.

That means:

- API-managed writes are visible immediately because they update `entries`.
- API-managed writes are persisted to disk.
- External edits to `secrets.json` are not visible until process restart.
- `GET /secrets` also reports the in-memory snapshot, not the externally edited
  file.

There is a second limitation: some runtime credentials are resolved and cached
when config is parsed. Even if the file store learns to reload from disk, values
already copied into long-lived runtime structs will not automatically change.

Known examples:

- `Config.parseFromSliceWithSecrets()` walks JSON config and replaces
  `${secret:key}` strings with the resolved value at parse time.
- Managed embedding config stores OpenAI API keys as concrete `api_key` bytes in
  `ManagedEmbeddingEntry`.
- Foreign DSNs and remote-content credentials may be resolved into config structs
  before runtime use, depending on the path.

## Dynamic File Reload Plan

The first phase is to make `FileStore` notice changes to the secrets file and
reload safely.

### Store Metadata

Add observed file metadata to `FileStore`:

- last observed `mtime`
- last observed size
- optionally inode or equivalent platform file identity
- a store generation counter
- a lock around `entries` and metadata

The generation counter increments only after a successful reload or local write.
Runtime components can use it later to invalidate credential caches.

### Refresh On Demand

Add a method like:

```zig
pub fn refreshIfChanged(self: *FileStore) !bool
```

Expected behavior:

1. `stat()` the secrets file.
2. If the file does not exist:
   - treat that as an empty file only if the previous observed state also allows
     deletion semantics;
   - otherwise decide explicitly whether external deletion clears secrets or is
     ignored.
3. If `mtime`, size, and identity match the observed metadata, return `false`.
4. Read and parse the file into a temporary map.
5. Validate keys while loading.
6. If parsing succeeds, swap the temporary map into `entries`, update observed
   metadata, increment generation, and return `true`.
7. If parsing fails, keep the old in-memory snapshot and return the parse error
   or log and suppress it, depending on caller policy.

Do not mutate the active map until the new file has been fully parsed. A bad
external edit must not destroy the last known good secrets.

### Read Paths

Call `refreshIfChanged()` before operations that need current file contents:

- `list()`
- `getOwned()`
- `resolveValueOwned()`

This avoids a background watcher and keeps the model portable. The cost is one
file metadata lookup per secret read or list operation, with a full JSON parse
only when the file changed.

### Write Paths

`put()` and `delete()` should continue to update the in-memory map first and
persist atomically, but they need conflict handling around external edits.

Recommended flow:

1. Lock the store.
2. Refresh from disk if the file changed since the last observation.
3. Apply the local mutation.
4. Persist using temporary-file plus rename.
5. Stat the final file.
6. Update observed metadata.
7. Increment generation.

Refreshing before mutation prevents a local API write from accidentally
discarding externally added secrets.

### Locking

Current code passes `*FileStore` to API handlers and other runtime paths. Dynamic
reload introduces mutation during reads, so the store needs synchronization.

A simple mutex is sufficient initially:

- lock around refresh, reads from `entries`, local writes, and metadata updates
- return owned copies before unlocking

An rw-lock can be introduced later if secret reads become hot enough to matter.
The JSON file is small, so correctness is more important than read concurrency.

### Failure Policy

The safest default policy is:

- malformed file on refresh: keep old snapshot and return/log an error
- missing file on startup: empty store
- missing file after a previous successful load: treat as an external delete
  only if we deliberately want that operational behavior
- local persist failure: leave the in-memory mutation visible only if callers can
  tolerate disk divergence; otherwise stage mutations in a temporary map and
  commit after persist succeeds

For API writes, a stricter approach is preferable: do not report success unless
the updated state was persisted.

## Runtime Secret Rotation Plan

File reload alone updates only future lookups through `FileStore`. It does not
change values that have already been resolved into config or provider structs.

The second phase is to preserve secret references in runtime configuration for
credential-bearing fields.

### Secret Value Type

Introduce a representation like:

```zig
pub const SecretValue = union(enum) {
    literal: []const u8,
    secret_ref: []const u8,
};
```

Parsing should keep `${secret:key}` as `secret_ref` instead of replacing it with
the concrete value for runtime credentials.

For fields where an ordinary string is still needed, provide:

```zig
pub fn resolveSecretValueOwned(
    alloc: std.mem.Allocator,
    store: ?*FileStore,
    value: SecretValue,
) ![]u8
```

### Managed Embedders

Managed embedders are the first high-value target because provider API keys need
rotation without restart.

Current shape:

- `ManagedEmbeddingEntry.api_key` stores resolved bytes.
- OpenAI request code builds `Authorization: Bearer <key>` from that cached
  value.

Target shape:

- store `api_key: ?SecretValue`
- at request time, resolve the current key from `FileStore`
- build the auth header from the resolved key for that request
- avoid storing the cleartext key longer than necessary

If per-request resolution is too expensive, cache the resolved auth header with
the `FileStore` generation number and refresh it when the generation changes.

### Config Parsing

`Config.parseFromSliceWithSecrets()` currently replaces all `${secret:key}`
strings in a generic JSON tree before typed parsing. That is convenient, but it
eagerly destroys reference identity.

Long term, split config handling into two modes:

1. Eager resolution for legacy fields that must remain plain strings.
2. Reference-preserving parsing for fields that participate in runtime
   credential rotation.

This does not need to be completed globally in one change. Start with explicit
credential fields and migrate more as needed.

### Foreign Sources And Remote Content

Foreign DSNs, CDC replication DSNs, S3 credentials, and remote-content headers
should follow the same reference-preserving model if they need live rotation.

Some of these components may hold pooled clients or long-lived connections.
Changing the secret value is not enough; the owning component may need to:

- close and recreate a connection pool
- rebuild an S3 client
- restart a replication worker
- retry failed auth with the latest generation

Each subsystem should define what "rotation applied" means for its resources.

## API Behavior

The public API should continue to avoid returning secret values.

`GET /secrets` after dynamic reload should:

- refresh from disk before listing
- include externally added keys
- stop listing externally removed keys if deletion semantics are enabled
- report environment overlay status using current environment variables

`PUT /secrets/{key}` should:

- refresh before applying the write
- validate the key
- update or add exactly that key
- persist successfully before returning 200

`DELETE /secrets/{key}` should:

- refresh before applying the delete
- persist successfully before returning 204
- return 404 if the key is absent after refresh

## Testing Plan

Unit tests for `common/secrets.zig`:

1. Initial load reads existing file.
2. `getOwned()` sees an external file edit without restarting the store.
3. `list()` sees an externally added key.
4. External malformed JSON leaves the previous snapshot intact.
5. API-style `put()` after an external edit preserves the external key.
6. API-style `delete()` after an external edit does not resurrect old entries.
7. Environment fallback still works when file entries are absent.
8. File entry still takes precedence over environment fallback.
9. Generation increments after successful reload and local writes.
10. Generation does not increment after failed reload.

API tests:

1. Start an API server with a store.
2. Write `secrets.json` externally.
3. `GET /secrets` reports the new key.
4. Update the file externally.
5. A request path that resolves the secret sees the new value.

Runtime rotation tests:

1. Configure an OpenAI embedder with `${secret:openai.api_key}`.
2. Serve a fake OpenAI endpoint that records Authorization headers.
3. Issue a request and observe the first key.
4. Edit the secrets file.
5. Issue another request without restarting Antfly.
6. Observe the second key.

## Rollout Plan

1. Add reload metadata, mutex, generation, and `refreshIfChanged()` to
   `FileStore`.
2. Call refresh from `list()`, `getOwned()`, and write paths.
3. Add unit coverage for external file edits and malformed edits.
4. Wire API tests around external edits.
5. Add reference-preserving `SecretValue`.
6. Migrate managed embedder API keys to resolve at request time or by generation
   cache.
7. Migrate foreign DSNs and remote-content credentials where live rotation is
   operationally useful.
8. Document operational behavior for malformed files, file deletion, and
   rotation of long-lived clients.

## Open Questions

1. Should external deletion of `secrets.json` clear all in-memory file secrets,
   or should Antfly keep the last known good snapshot?
2. Should malformed external edits surface as API errors on `GET /secrets`, or
   should they only be logged while serving the last known good snapshot?
3. Should the secrets file remain plaintext JSON, or should the Zig store move
   toward encrypted-at-rest parity with the older Go keystore language?
4. Which credential-bearing subsystems require true live rotation versus
   restart-on-change semantics?
5. Should environment variables always be lower priority than file entries, or
   should operators be able to choose env override precedence?
