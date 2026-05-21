# Antfly Zig Secrets Store

## Goal

Antfly-zig needs a small local secrets store for swarm mode. The store backs the
`/secrets` API, resolves `${secret:key}` references in configuration, and lets
operators keep provider credentials out of ordinary config files.

The implementation is intentionally simple: one JSON file on local disk is held
as an in-memory snapshot. Reads refresh that snapshot when the file changes, and
API writes refresh first, stage a replacement snapshot, persist it atomically,
then publish the new in-memory state.

This document captures the current design, what is already implemented, and the
remaining plan for runtime secret rotation.

## Status Summary

Implemented:

- `FileStore` refreshes from disk on demand for `list()`, `getOwned()`, and
  write paths.
- Valid replacement files are authoritative, including deleted keys.
- Missing or malformed files keep the last known good snapshot.
- API writes refresh before mutation and only report success after persist.
- Managed OpenAI embedders preserve secret references and resolve the API key at
  request time through the live `FileStore`.
- Provider registry generator/reranker config is parsed from the raw config tree
  so API keys keep `${secret:key}` reference identity.
- Generator API keys resolve immediately before each OpenAI-compatible request.
- Reranker API keys are carried through the runtime options path and resolve
  immediately before each rerank request where provider support exists.
- Foreign DSNs from query requests resolve through the live `FileStore` for each
  request path that has access to the API server secret store.
- CDC runners now carry an optional `FileStore` and resolve replication DSNs
  through it before each snapshot or streaming connection.
- S3 backup locations can open through `openBackupLocationWithSecrets()` and use
  live file-backed AWS credential overrides.
- Remote-content S3 credentials and HTTP header values are preserved as secret
  references in config instead of being eagerly flattened.
- Remote-content helpers resolve configured HTTP headers and S3 credentials at
  fetch time, including managed embedder query-template paths that receive the
  API server `FileStore`.
- `DB.OpenOptions` can carry a `FileStore` and remote-content config, so
  lower-level managed DB opens, generated source-template rendering, and
  enrichment workers can resolve remote-content credentials outside
  API-server-only paths.
- Metadata-service CDC snapshot and streaming coordinators now receive the
  service-owned `FileStore`, so replication DSNs can rotate without restarting
  the metadata service.
- Metadata-service restore planning uses `openBackupLocationWithSecrets()` when
  the concrete service owns a `FileStore`, so S3 restore metadata reads can use
  live secret-backed AWS credentials.
- `GET /status` reports compact, non-secret local secret-store health when a
  store is available: whether Antfly is serving a stale last-known-good snapshot.
- API server tests cover external `secrets.json` additions and deletions being
  reflected by `GET /secrets`.

Still needed:

- Runtime tests for generator/reranker, foreign DSN, CDC DSN, S3 backup, and
  remote-content credential rotation.
- Optional Prometheus metrics for reload state if operators need scrape-based
  alerting in addition to `GET /status`.
- A future encrypted-at-rest codec, if non-Kubernetes deployments need local
  secret-file encryption.

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
2. `list()`, `getOwned()`, `getOwnedWithGeneration()`,
   `resolveValueOwned()`, and `resolveValueWithGenerationOwned()` refresh from
   disk first if the file metadata changed.
3. `getOwned()` returns the stored value if present.
4. If the key is not stored, `getOwned()` maps the key to an environment variable
   and returns the environment value if set.
5. `resolveValueOwned()` resolves `${secret:key}` references through `getOwned()`.
6. `resolveReferenceOwned()` resolves through a `FileStore` when one is supplied,
   or through environment variables only when there is no store.
7. `resolveReferenceWithGenerationOwned()` returns the resolved value, source,
   and cache generation for generation-keyed clients.

Writes:

1. `put()` validates the key, refreshes from disk, stages the mutation,
   persists it, publishes the staged entries, and returns metadata.
2. `delete()` refreshes from disk, stages removal from `entries`, persists it,
   publishes the staged entries, and returns whether an entry existed.
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

The file store now notices external edits, but some runtime credentials are
still resolved and cached when config is parsed. Values already copied into
long-lived runtime structs will not automatically change unless that subsystem
preserves the secret reference and resolves it at use time, or rebuilds the
client/resource when the store generation changes.

Known examples:

- `Config.parseFromSliceWithSecrets()` walks JSON config and replaces
  `${secret:key}` strings with the resolved value at parse time, except for
  credential-bearing registry, termite S3, and remote-content fields that need
  live rotation.
- Managed embedding config now keeps OpenAI API keys as `SecretValue` references
  and resolves at request time.
- Some backup, remote-content, and CDC call sites still need store ownership
  plumbing before every process path can use live file-backed credentials.

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
   failure for warning logs and compact cluster status.

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
- reload status is visible through compact cluster status and warning logs
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

`common/secrets.zig` now provides:

```zig
pub const SecretValue = union(enum) {
    literal: []u8,
    secret_ref: []u8,
    env_var: []u8,
};
```

Parsing should keep `${secret:key}` as `secret_ref` instead of replacing it with
the concrete value for runtime credentials.

`resolveOwned()` returns an owned value at use time. For a `secret_ref`, it reads
through `FileStore.getOwned()`, so external file edits are observed by the next
resolution.

### Managed Embedders

Managed embedders are the first high-value target because provider API keys need
rotation without restart.

Implemented shape:

- store `api_key: ?SecretValue`
- at request time, resolve the current key from `FileStore`
- cache the bearer auth header by `FileStore` generation
- refresh the cached auth header when the generation changes

### Remaining Credential Consumers

We want live rotation for all known credential-bearing subsystems, but each one
needs an explicit ownership model because some hold long-lived clients or
workers.

| Subsystem | Current Status | Needed Semantics |
| --- | --- | --- |
| Managed OpenAI embedders | Implemented with generation-cached bearer auth headers and a fake OpenAI-compatible rotation test. | Continue using the same generation contract for future managed provider clients. |
| Generator providers | Implemented for OpenAI-compatible providers with generation-cached bearer auth headers. | Add fake-provider runtime test; extend the same pattern when additional provider clients are implemented. |
| Reranker providers | Runtime options now carry and resolve `api_key` references before rerank calls. | Add provider-specific tests once non-local reranker clients are implemented. |
| Foreign DSNs | Implemented for API query paths that have the API server `FileStore`. | Add runtime tests and define pool invalidation behavior for any long-lived foreign clients. |
| CDC replication DSNs | Metadata-service snapshot and streaming coordinators pass the service-owned `FileStore` into runners, which resolve before snapshot/stream connections. | Add reconnect/retry tests for active workers after DSN rotation. |
| S3/backup credentials | Implemented for `openBackupLocationWithSecrets()` with AWS override keys, and wired through API/httpx backup handlers plus metadata-service restore planning when the service owns a `FileStore`. | Add rotation tests and define generation-keyed client caching if S3 client rebuild cost becomes visible. |
| Remote-content S3 credentials | Fetch helpers select configured credentials and resolve secret references immediately before S3 fetches. `DB.OpenOptions`, managed DB opens, and enrichment runtimes can carry the `FileStore` and remote-content config outside API-server-only paths. | Add rotation tests. |
| Remote-content HTTP headers | Fetch helpers select configured HTTP credentials and resolve header secret references immediately before outbound HTTP fetches. `DB.OpenOptions`, managed DB opens, and enrichment runtimes can carry the `FileStore` and remote-content config outside API-server-only paths. | Add rotation tests. |

### Reconnect and Cache Contract

Secret-backed clients must be keyed by the generation of the `FileStore`
snapshot used to build them. `FileStore` exposes generation-aware resolution so
callers can resolve the credential and capture the generation under the same
store lock. Literal values and environment-only fallback use generation `0`.

Default behavior is per-operation generation check, followed by cache reuse for
stateful or expensive resources. Auth headers are cached by generation for
OpenAI-compatible embedders and generators. Long-lived DB pools, S3 clients, and
remote-content clients should use the same shape: cache keys must include
non-secret config identity plus the resolved secret generation, never the raw
secret value.

Foreign DSNs currently resolve per request and create short-lived clients. If a
long-lived pool is added later, the pool key should be `(logical source,
non-secret source config, secret_generation)`. New work must use the newest
generation. Checked-out connections may finish their current request, idle
connections from older generations should close immediately, and the old pool
should drain with a short TTL.

CDC workers resolve the DSN before snapshot or stream connection creation and
log the generation used. A stream may finish the current poll with the
credential it started with, but any reconnect, retry, or next snapshot/stream
connection must resolve again. If the resolved generation changed, the worker
rebuilds the source connection and resumes from the persisted checkpoint. If the
new secret is missing or invalid, the worker records a failed/retryable status
and does not keep opening new work with the old credential.

Deleting a key from a valid secrets file is revocation for new work. Malformed
files do not advance generation and keep the last known good snapshot.

Recommended implementation order:

1. Add runtime tests for generator/reranker, foreign DSN, CDC DSN, S3 backup,
   and remote-content credential rotation.
2. Add generation-keyed client caches for S3/backup and remote-content clients.

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
should follow the same reference-preserving model. We expect these to support
live rotation, but the implementation needs subsystem-specific rebuild behavior.

Some of these components may hold pooled clients or long-lived connections.
They follow the generation contract above: new work resolves the current
generation, old checked-out work may finish, and any retry/reconnect must use
the latest generation.

## API Behavior

The public API should continue to avoid returning secret values.

`GET /secrets` after dynamic reload should:

- refresh from disk before listing
- include externally added keys
- stop listing externally removed keys after a valid replacement file is loaded
- report environment overlay status using current environment variables
- expose stale reload status through compact `GET /status` fields when a
  malformed or missing file forced the store to keep last known good values

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

Additional runtime tests:

1. Generator and reranker fake providers observe changed Authorization headers
   after editing `secrets.json`.
2. Remote-content HTTP fetch observes changed secret header values without
   restart.
3. S3/backup client construction uses the new credential generation after file
   rotation.
4. Foreign DSN and CDC workers reconnect or restart when a referenced DSN
   changes.

## Rollout Plan

1. [done] Add reload metadata, mutex, generation, and `refreshIfChanged()` to
   `FileStore`.
2. [done] Call refresh from `list()`, `getOwned()`, and write paths.
3. [done] Add unit coverage for external file edits and malformed edits.
4. [done] Wire API tests around external edits.
5. [done] Add reference-preserving `SecretValue`.
6. [done] Migrate managed embedder API keys to resolve at request time or by generation
   cache.
7. [done] Add managed embedder runtime test with a fake OpenAI-compatible
   endpoint.
8. [done] Migrate generator and reranker provider credential plumbing.
9. [partial] Preserve and resolve remote-content HTTP header references at
   fetch time; rotation tests remain.
10. [partial] Preserve and resolve remote-content S3 references at fetch time;
   rotation tests remain.
11. [partial] Migrate foreign DSNs and CDC DSNs; reconnect tests remain.
12. [done] Add warning logs and compact cluster status for stale reload state.
13. [done] Document operational behavior for malformed files, file deletion, Kubernetes
   projection, and rotation of long-lived clients.

## Initial Decisions

1. A valid replacement file is authoritative for file-backed secrets, including
   deletion.
2. A missing file after a previous successful load keeps the last known good
   snapshot.
3. A malformed file keeps the last known good snapshot and records reload
   failure through warning logs and compact cluster status.
4. File entries have priority over environment fallback. If a file key is
   deleted and a matching environment variable exists, the environment value is
   used.
5. Kubernetes-projected files are the primary enterprise integration surface.
   Direct external secret manager integrations are deferred.
6. Plaintext JSON remains the initial store format, protected by filesystem
   permissions. Add a codec boundary so encrypted-at-rest support can be added
   later.
7. Start true live rotation with managed embedder API keys.
8. Support live rotation for all credential-bearing integrations that Antfly
   owns: generator/reranker providers, remote-content credentials, S3/backup
   credentials, foreign DSNs, and CDC DSNs.
9. Treat DSNs, replication workers, pools, and remote clients as
   subsystem-specific implementations because they may need resource rebuild
   semantics.

## Deferred Questions

1. Should Prometheus metrics mirror the compact `GET /status` secret-store
   state for scrape-based alerting?
2. What encrypted envelope format and key-source contract should the future
   encrypted codec use?
3. Should S3/backup and remote-content clients share a generation-keyed
   credential cache, or should each subsystem own its own cache?
