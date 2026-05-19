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
   - on startup, initialize an empty store;
   - after a previous successful load, keep the last known good snapshot.
3. If `mtime`, size, and identity match the observed metadata, return `false`.
4. Read and parse the file into a temporary map.
5. Validate keys while loading.
6. If parsing succeeds, swap the temporary map into `entries`, update observed
   metadata, increment generation, and return `true`.
7. If parsing fails, keep the old in-memory snapshot and record the reload
   failure for logs and metrics.

Do not mutate the active map until the new file has been fully parsed. A bad
external edit must not destroy the last known good secrets.

A complete, valid replacement file is authoritative for file-backed secrets.
That includes deletions: if the file exists, parses successfully, and no longer
contains a key, the key should be removed from the in-memory file-backed
snapshot. Missing or malformed files do not change the active snapshot.

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

- malformed file on refresh: keep old snapshot and log/metric the error
- missing file on startup: empty store
- missing file after a previous successful load: keep last known good snapshot
- valid replacement file with removed keys: apply the deletion
- local persist failure: leave the in-memory mutation visible only if callers can
  tolerate disk divergence; otherwise stage mutations in a temporary map and
  commit after persist succeeds

For API writes, a stricter approach is preferable: do not report success unless
the updated state was persisted.

This policy is designed for Kubernetes projected files. Projection updates may
briefly expose missing or partial files, but a completed projection should be
treated as authoritative. Operators can remove a key from the projected
`secrets.json` file and Antfly will remove that file-backed key after the valid
replacement is observed.

One important consequence is environment fallback. If a file-backed key is
deleted and an environment variable for the same key exists, lookup will fall
back to the environment value. Deployments should avoid configuring the same
secret through both sources unless that fallback is intentional.

## Kubernetes And Enterprise Integration

The primary enterprise integration path should be Kubernetes-projected secret
files, not direct integrations with every external secrets manager.

A typical deployment can be:

```text
external secrets manager
  -> External Secrets Operator or CSI Secret Store
  -> Kubernetes projected file
  -> Antfly FileStore
```

That keeps Antfly's runtime contract small and lets operators use their existing
secret manager, IAM, auditing, and rotation systems. Antfly only needs to make
the mounted-file behavior robust:

- valid replacement files are authoritative, including key deletion
- missing or malformed files keep last known good values
- reload status is visible through logs and metrics
- secret values are never returned by APIs or written to logs
- runtime consumers can observe changed values without process restart where
  live rotation is supported

Direct integrations with Vault, AWS Secrets Manager, GCP Secret Manager, Azure
Key Vault, and similar systems should be deferred until there is a concrete
customer requirement that cannot be handled by Kubernetes projection or local
file provisioning.

Encrypted-at-rest support inside Antfly is still useful for non-Kubernetes
deployments and simpler VM/bare-metal deployments. It is less urgent for the
Kubernetes enterprise path, where the external manager and Kubernetes secret
projection own most of the secret lifecycle.

The file store should still be designed with a codec boundary so encrypted files
can be added without rewriting store semantics:

```zig
const SecretsCodec = struct {
    decode: fn (alloc: std.mem.Allocator, bytes: []const u8) anyerror!PersistedSecretsFile,
    encode: fn (alloc: std.mem.Allocator, file: PersistedSecretsFile) anyerror![]u8,
};
```

Initial codec:

- plaintext JSON, protected by filesystem permissions

Future codec:

- authenticated encrypted envelope
- key supplied by environment variable, key file, or Kubernetes-mounted key
- ciphertext contains the current inner `PersistedSecretsFile` JSON
- failed authentication is treated like malformed JSON: keep last known good

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
- stop listing externally removed keys after a valid replacement file is loaded
- report environment overlay status using current environment variables
- expose stale reload status through logs and metrics when a malformed or
  missing file forced the store to keep last known good values

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
5. External valid JSON without a previous key deletes that key from memory.
6. Missing file after a successful load leaves the previous snapshot intact.
7. API-style `put()` after an external edit preserves the external key.
8. API-style `delete()` after an external edit does not resurrect old entries.
9. Environment fallback still works when file entries are absent.
10. File entry still takes precedence over environment fallback.
11. Generation increments after successful reload and local writes.
12. Generation does not increment after failed reload.

API tests:

1. Start an API server with a store.
2. Write `secrets.json` externally.
3. `GET /secrets` reports the new key.
4. Update the file externally.
5. A request path that resolves the secret sees the new value.
6. Remove a key from a valid replacement file.
7. `GET /secrets` no longer reports that file-backed key.

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
8. Add logs and metrics for stale reload state.
9. Document operational behavior for malformed files, file deletion, Kubernetes
   projection, and rotation of long-lived clients.

## Initial Decisions

1. A valid replacement file is authoritative for file-backed secrets, including
   deletion.
2. A missing file after a previous successful load keeps the last known good
   snapshot.
3. A malformed file keeps the last known good snapshot and records reload
   failure through logs and metrics.
4. File entries have priority over environment fallback. If a file key is
   deleted and a matching environment variable exists, the environment value is
   used.
5. Kubernetes-projected files are the primary enterprise integration surface.
   Direct external secret manager integrations are deferred.
6. Plaintext JSON remains the initial store format, protected by filesystem
   permissions. Add a codec boundary so encrypted-at-rest support can be added
   later.
7. Start true live rotation with managed embedder API keys. Treat DSNs,
   replication workers, pools, and remote clients as subsystem-specific follow
   ups because they may need resource rebuild semantics.

## Deferred Questions

1. What exact logs and metrics should expose last reload success, last reload
   failure, generation, and stale snapshot state?
2. Should there be an API field for non-secret reload health, or are logs and
   metrics sufficient?
3. What encrypted envelope format and key-source contract should the future
   encrypted codec use?
4. Which long-lived clients should support live rotation versus restart-on-change
   semantics?
