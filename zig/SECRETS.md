# Antfly Secret Sources and Native Store

## Goal

Antfly resolves `${secret:key}` references through an ordered resolver and backs
the `/secrets` API with an optional Antfly-managed native store. Operators can
keep credentials outside ordinary configuration and combine native overrides,
externally managed sources, and environment fallback.

The current backends are JSON files and the process environment. File snapshots
refresh on changes; native writes persist atomically before publishing new state.
This document describes that implementation and the remaining rotation roadmap.

## Source configuration and ownership

The `secrets` section configures the resolver before ordinary configuration
references are resolved:

```json
{
  "secrets": {
    "native": {
      "name": "native",
      "path": "/var/lib/antfly/secrets.json"
    },
    "sources": [
      {
        "name": "tenant",
        "type": "file",
        "path": "/run/secrets/tenant/secrets.json"
      },
      {
        "name": "platform",
        "type": "file",
        "path": "/run/secrets/platform/secrets.json"
      }
    ],
    "environment": true
  }
}
```

Resolution is deterministic: a native override wins, then the first matching
source in array order, then the environment if enabled. An empty value is a
present value, not a reason to fall through. A source's last-known-good snapshot
retains its precedence when a refresh fails.

- `native` is optional and is the only store the secret API can write. Its name
  defaults to `native`. The name describes Antfly ownership, not storage topology.
  `backend` defaults to `file`, preserving the existing node-local JSON store.
  Explicit `distributed` and `serverless` backends use encrypted persistence;
  their configuration and runtime delivery are described below.
- `sources` defaults to `[]`. Entries require a unique `name`, a `type`, and
  provider-specific settings. Only `type: "file"` is implemented now. These are
  externally managed and read-only to Antfly, regardless of filesystem permissions.
- `environment` defaults to `true`, including when `secrets` is `{}`. It is a
  final fallback, not an entry implicitly inserted into the ordered source array.
  `false` disables environment lookup for `${secret:...}` and environment secret
  discovery. It does not disable unrelated environment configuration, explicit
  provider environment defaults/settings, or cloud SDK workload identity.
- An explicit section without `native` disables secret API writes. In particular,
  `{"secrets":{"environment":false}}` creates an empty resolver that cannot fall
  back to the process environment. No default native file is added.
- Names use ASCII letters, digits, dots, underscores, or hyphens; `environment`
  is reserved. Duplicate names (including the native name), empty paths, unknown
  source types/properties, and secret-reference paths are configuration errors.
  A native path must not also be an external source. Exact duplicate paths are
  rejected; operators must also avoid aliases or symlinks to the same file.
- Paths are literal and relative paths use the process working directory. Keep
  configured logical paths for reads so projected-volume symlink rotation works.
  Native atomic writes follow the current target without replacing the symlink;
  dangling targets fail rather than replacing the link.

Source membership, ordering, names, and environment policy are startup-only.
Restart to change them; the existing file-content rotation behavior stays live.
Standalone, metadata, data, and serverless startup all use this bootstrap parser.

### Compatibility and migration

When `secrets` is absent, preserve existing deployment defaults and
`--secret-store-path` behavior: standalone uses `<base>/secrets.json` by default,
other modes have no file store by default, and explicit legacy paths use the
first file as the writable store and remaining files as fallbacks. Environment
fallback remains enabled. Serverless's legacy `ANTFLY_SECRET_STORE_PATH` also
continues to work.

Do not combine an explicit `secrets` section with legacy secret-store flags (or
serverless's legacy path environment variable); startup rejects the ambiguity.
For projected Kubernetes volumes, migrate every projected file into `sources`.
Add a separate native path only when node-local overrides are desired.

### API and dashboard

Ownership is per source, not a per-key read/write permission. Values remain
write-only through the API:

- `GET /secrets` includes `writable`, indicating whether this server has a native
  write destination. Each effective key includes `source` (winning source name)
  and `managed` (whether a native override exists), alongside existing metadata.
  File paths and secret values are not returned. Legacy file/env status fields
  remain compatible; `configured_both` only considers enabled environment fallback.
- `PUT /secrets/{key}` creates or replaces a native override. It never mutates
  the external source that currently supplies the key. Without native, return 503.
- `DELETE /secrets/{key}` removes only the native override; any external or
  environment value becomes effective immediately. Return 404 when there is no
  native override, even if an external value exists, and 503 without native.
  There are no deletion tombstones that hide external values.
- The dashboard uses the returned capability, displays the winning source, and
  offers deletion only for managed keys. Its delete confirmation explains that
  a fallback may become active. Adding an existing key creates an override.

### Extension boundary

Keep resolution policy separate from source ownership. A future source provider
should supply lookup, metadata, refresh/health, and revision information; only
native storage needs mutation support. The current `FileStore` uses explicit
writable/environment policy and ordered file snapshots, retaining its existing
cross-archive callback boundary. It is not yet a generic remote-provider engine.

Remote source types can extend the tagged `sources` entries without changing
precedence or `${secret:key}` syntax. Define authentication, timeout, caching,
last-known-good, and revision semantics before adding a provider. A distributed
native backend can later implement the same override behavior, but needs explicit
replication and consistency semantics; the `native` name makes no distribution
promise today. Same-open-file metadata/read snapshots remain a separate hardening
step for rotations that occur between the current stat and read operations.

## Intended native storage architecture

The native store is an Antfly-owned logical service. Its persistence follows the
storage backend; `native` does not imply a file or a distributed service. All
implementations share secret identity, encryption, conditional mutation, and
resolution semantics.

| Deployment | Intended persistence | Commit boundary |
| --- | --- | --- |
| Distributed Antfly | Dedicated encrypted metadata records replicated through metadata Raft | Durable quorum commit and application of the conditional mutation |
| Antfly Lite | Reserved encrypted records inside the native `.aflite` file | Atomic durable commit through the existing single-writer machinery |
| Antfly Serverless | One bounded ciphertext collection per scope | Successful conditional replacement using ETag/generation CAS |

The common contracts (`common/secret_contract.zig`), AFSE codec
(`common/secret_record.zig`), and Lite persistence adapter
(`storage/lite/secret_store.zig`) are implemented. Embedding hosts can obtain a
scope-bound adapter through `lite.backend.Handle.secretStore`. Existing runtime
consumers still use `FileStore`: JSON files remain plaintext and are not silently
converted. Distributed/serverless adapters, migration, production key-provider
configuration, RPCs, and runtime resolver integration remain follow-up work.

### Common source and native writer contract

`Source` borrows a provider handle and exposes `resolve`, `listMetadata`, and
`refresh` (returning health). `NativeStore` combines a source with a writer that
exposes `put` and `removeOverride`. Read-only sources have no writer capability.
Callbacks use the existing checked runtime callback boundary to preserve error
semantics across separately compiled archives. Providers own their lifecycle;
they must outlive borrowed handles and all in-flight calls.

- Identity is `(scope, key, revision)`. Scope is a trusted, stable tenant/database
  identity, not a user-provided namespace that bypasses authorization.
- Revisions are source-local unsigned 64-bit counters. Native revisions are
  durable, monotonically increasing per scope; zero means the initial empty
  snapshot and cannot identify a stored value. Every effective mutation,
  including deletion, advances the scope revision; writes assign that revision
  to the entry. Overflow must reject a write, never wrap or reuse a revision.
- `Lookup` returns a scope snapshot revision and an optional value with its own
  entry revision. An absent key is a successful lookup with `value = null`, not
  an I/O error. A present empty byte string is still a winning secret.
- Read options include a minimum source-local snapshot revision. The wrapper
  rejects older snapshots even when they report absence. Providers additionally
  enforce authorization and their declared freshness policy before returning a
  snapshot. Revisions from different sources must never be compared.
- `resolveOrdered` accepts named sources in priority order. It falls through only
  after successful absence; unavailability, corruption, and authorization errors
  propagate. The caller supplies native first, external sources next, and an
  environment adapter last only when enabled. No adapter is implicitly added.
- Writer preconditions are `any`, `absent`, or `exact(entry_revision)`. Backends
  check these inside their commit boundary. Conflicts return `error.Conflict`;
  a preliminary read followed by an unconditional write is insufficient.
- Mutations return the committed scope revision. Removal of an already absent
  override is a no-op with `changed = false`. Internal deletion markers may be
  needed for replication/cache invalidation; they must not hide external fallback.
- Returned values and metadata use the caller's allocator. `SecretBytes.deinit`
  securely zeros its owned allocation before releasing it. This is best-effort
  cleanup, not a guarantee about application copies, swap, or crash dumps.

`ExpectedRevision.check` is shared precondition logic, not a transaction engine.
The common layer validates requests and returned revisions but cannot supply
backend atomicity, tenant authorization, durability, or cache freshness itself.

### Encrypted record format: AFSE v1

The storage-independent codec implements envelope encryption using Zig's
`XChaCha20Poly1305`: a new 32-byte data key and 24-byte nonce are generated with
fallible `Io.randomSecure` on every seal. There is no public caller-selected
nonce API and no fallback to weaker randomness when entropy is unavailable.
The extended nonce construction supports randomized nonces; see the
[libsodium construction documentation](https://libsodium.gitbook.io/doc/secret-key_cryptography/aead/chacha20-poly1305/xchacha20-poly1305_construction).

A caller-supplied `KeyProvider` wraps the data key and returns an opaque wrapped
blob plus a concrete wrapping-key identifier. Its unwrap callback receives the
expected identity, key identifier, and blob and fills a temporary 32-byte key.
A provider must authenticate its wrapping format and enforce allowed key IDs and
scope policy. Provider configuration is trusted bootstrap state; record bytes
cannot select an arbitrary provider or credential source. No production KMS,
raw-key, or OS-keychain provider is introduced in this phase.

The canonical binary format uses unsigned little-endian integers:

| Offset | Field |
| --- | --- |
| 0 | Magic `AFSE` (4 bytes) |
| 4 | Format version `1` (u16) |
| 6 | Algorithm `1` = XChaCha20-Poly1305 (u16) |
| 8 | Entry revision (u64, nonzero) |
| 16 | Scope byte length (u16) |
| 18 | Secret-name byte length (u16) |
| 20 | Wrapping-key identifier byte length (u16) |
| 22 | Wrapped-data-key byte length (u32) |
| 26 | Ciphertext byte length (u32; equals plaintext length) |
| 30 | Nonce (24 bytes) |
| 54 | Scope, secret name, key identifier, wrapped key, in that order |
| Variable | Ciphertext, then 16-byte authentication tag |

The entire prefix through the wrapped key is AEAD associated data. This binds
version, algorithm, lengths, scope, name, revision, nonce, wrapping-key identity,
and wrapped key to the ciphertext. Identities and key IDs are nonempty UTF-8,
without NUL bytes, limited to 1 KiB each; wrapped keys are limited to 64 KiB and
values to 1 MiB. Empty values and arbitrary binary secret values are supported.
The parser rejects unsupported versions/algorithms, oversized lengths,
truncation, and trailing bytes before asking the key provider to unwrap anything.
Names, key identifiers, lengths, and revisions are authenticated but not encrypted.

`decode` returns a borrowed **unauthenticated** framing view. It is not a trusted
metadata or authorization API. `open` requires an expected identity from the
backend's trusted index/current committed revision and rejects substitution before
unwrapping. Authentication failure produces `error.CorruptInput`; unavailable key
providers remain errors rather than triggering source fallback. Temporary data
keys are erased on success and failure, and failed plaintext buffers are erased
and freed. A returned plaintext buffer remains the caller's responsibility.

AEAD does not provide rollback protection by itself. A previously valid record
is still valid at its original revision. Backends must obtain the expected
revision from their authoritative head and enforce read freshness. Restore must
not silently reset counters under an existing scope; it needs an explicit
freshness fence or a new scope incarnation with appropriately re-encrypted data.
Rewrapping changes authenticated bytes, so v1 requires resealing the record; an
in-place wrapped-key replacement is invalid.

### Backend implementation plan

**Distributed:** Add a protected metadata namespace and conditional Raft command
for encrypted secret records. Encrypt before proposing; Raft apply validates and
persists ciphertext deterministically without KMS calls. Because the revision is
authenticated, prepare against a scope snapshot, propose the expected scope
revision and the next revision together, and retry preparation if another writer
wins. Do not assume a future Raft log index before it has been assigned. Public
metadata/query/export paths must not expose these records as ordinary documents.
Authenticated internal RPCs authorize each worker's scope and required secret.
Publish invalidations after commit and periodically reconcile revisions so missed
notifications cannot leave permanent stale caches.

#### Distributed secret delivery to data nodes

Metadata nodes implement the authoritative `NativeStore`; every data node hosts
a read-only remote `Source` backed by internal RPCs. The initial source always
reads through; the bounded memory cache described below is a future optimization. Metadata
Raft replicates durable encrypted AFSE records. Data nodes obtain values needed
by their assigned work and authorized scopes. Cluster-wide secrets may be
available to all nodes, while tenant secrets require tenant-specific access.
Having a node identity alone does not grant access to every scope or key.

The implemented delivery model decrypts on metadata and uses the existing
internal service authentication plus a separate per-consumer credential and exact
key allowlist. Requests and replies are encrypted with XChaCha20-Poly1305, including
through TLS-terminating proxies. See the current implementation below. A future
mTLS identity adapter can replace mounted delivery credentials. Only metadata nodes need KMS/key-provider
access. Data nodes keep resolved values in memory, with no plaintext disk cache;
responses and errors must not put secret values in logs or tracing. Direct AFSE
delivery is a possible later alternative, but would require each recipient to
have authorized unwrap access and separately provisioned key-provider credentials.
Node identity, transport trust, and metadata key-provider credentials come from
bootstrap configuration, not from the native store they unlock.

The intended sequence once watch/cache optimization is added is:

1. Encrypt the mutation and durably commit/apply it through metadata Raft,
   advancing the scope revision and recording the changed entry revision.
2. Publish an invalidation containing scope, key, committed revision, and change
   kind (update or deletion). Notifications contain no secret values.
3. An affected data node resolves the key through an authenticated internal RPC,
   requiring at least the notified scope revision. The service authorizes the
   node's scope/key access before returning a value or authoritative absence.
4. The node publishes the refreshed cache entry and advances the relevant local
   consumer generation. Provider clients, connection pools, and workers apply
   the subsystem-specific rotation rules described later in this document.

The longer-term internal protocol is (watch is not yet implemented):

| Operation | Semantics |
| --- | --- |
| `ResolveSecret(scope, key, min_revision)` | Return an authorized value with its entry revision, or authoritative absence, plus the observed scope revision. |
| `WatchSecrets(scope, after_revision)` | Deliver ordered committed changes/deletions after a scope revision, or explicitly report that replay history is unavailable. |
| `GetSecretRevision(scope)` | Read an authorized current scope revision for periodic reconciliation. |

Authoritative reads need a Raft read barrier or equivalent leader-confirmed
freshness. Merely satisfying a caller's old minimum revision on an isolated
follower must not renew a cache's freshness indefinitely. A node that cannot
serve the requested revision and freshness must wait, forward, or return an
availability error. Watch publication follows commit; reconnect/reconciliation
must recover changes even if a leader fails between commit and notification.

Watch delivery is an optimization, not the sole correctness mechanism. At
startup, on reconnect, and after a watch-history gap, nodes reconcile their
cached keys against authoritative metadata. Periodic revision checks catch
missed notifications. Use a snapshot/cursor handoff or replay from the fetched
snapshot revision so changes during reconciliation are not lost. Receiving a
new scope revision does not by itself mark every cached entry as refreshed;
advance a fully reconciled revision only after processing all changes through
that revision or reconciling the affected cache. Reject stale responses that
would overwrite newer values or deletion knowledge.

Cache both values and authoritative absence with bounded freshness. The
distributed adapter must specify the freshness duration and whether still-valid
cached values may serve during an outage before runtime integration is enabled.
Use monotonic elapsed time for local expiry. Expired entries fail resolution if
metadata cannot revalidate them; unavailability must never cause source fallback.
Once a change notification establishes that a cached answer is outdated, that
answer cannot satisfy reads requiring the notified revision. Evict cached values
when their scope assignment or authorization is withdrawn, and perform best-effort
plaintext cleanup when replacing or removing owned buffers.

A deletion invalidates the native override, including any cached value. Only
after confirming authoritative absence may ordered resolution expose an external
source or the environment. External files and environment variables remain
separately provisioned sources; this protocol distributes native secrets and
does not make node-local fallback configurations consistent automatically.

Mutation success means durable metadata commit, not acknowledgement from every
data node. Return the committed revision and separately expose each node's
observed/reconciled revision and relevant consumer activation status. Do not
claim that a consumer has switched credentials merely because its node fetched
the new value. A caller needing coordinated rollout can wait for the required
nodes/consumers to report readiness without making every write depend on all
nodes being available.

Rotation is not instantaneous revocation: a partitioned node may use an allowed
cached value until its freshness deadline, and an in-flight request may already
hold the old credential. Revoking that credential at the external provider is a
separate operation. Rollout should account for that overlap and for rebuilding
long-lived clients or sessions where changing a cached string is insufficient.

Distributed delivery tests must cover unauthorized scope/key access, node join
and reassignment, leader failure between commit and notification, reconnect and
watch-history gaps, missed/deferred invalidations, out-of-order fetch responses,
negative-cache invalidation on create, deletion revealing fallback, partitioned
freshness expiry, stale follower reads, and consumer activation acknowledgements.

#### Lite persistence

**Lite (implemented):** A reserved metadata catalog namespace stores encrypted
records inside the existing native file. A single catalog batch commits
ciphertext, entry revision, and scope revision through the existing writer lock
and checkpoint durability guarantees. Read-only and `no_sync` handles expose
only a source, because unsynced writes cannot promise durable mutation success.
The embedding application supplies a key-provider callback. Future CLI
integration will use an explicit platform/mounted-key provider. The database never stores its
unwrapped root key. File copies and backups carry ciphertext; key access/recovery
must be provisioned separately. Opening storage must not require resolving an
application secret from that same unopened store.

The embedding API is:

```zig
var secrets = try handle.secretStore(allocator, trusted_scope, key_provider);
defer secrets.deinit();
const source = secrets.source();
if (secrets.nativeStore()) |native_store| {
    const committed = try native_store.writer.put(
        trusted_scope, "provider.token", token_bytes, .absent,
    );
    var value = try source.resolve(allocator, trusted_scope, "provider.token",
        .{ .min_revision = committed.revision });
    defer value.deinit(allocator);
}
```

The handle and key provider must outlive the adapter and all calls. Keep the
adapter at a stable address while its borrowed interfaces exist. The host chooses
authorized scope identities; calls with a different scope fail `Unauthorized`.
The bridge engine does not support this adapter. Key-provider callbacks may do
I/O, but must not reenter a writer or maintenance operation on the same handle:
a mutation retains the writer reservation while releasing the catalog mutex for
wrapping. Readers and index writers can proceed during wrapping.

The private metadata layout is versioned separately from AFSE:

- Prefix: `\x00antfly.secrets.v1/<hex SHA-256(scope)>/`.
- `head`: exactly one little-endian `u64` scope revision. An absent head means
  the initial revision zero. Once written, it survives removal of every entry.
- `entries/<hex SHA-256(key)>`: little-endian `u64` entry revision, `u16` UTF-8
  key length, key bytes, then an AFSE v1 envelope. The index supplies the expected
  revision and identity when opening the envelope; they are not selected from
  unauthenticated AFSE fields. Hashes bound catalog-key size and avoid delimiter
  ambiguities. Index metadata and names are visible, while values stay encrypted.

A successful mutation is durably published. Conditional writes use entry
revisions and serialize across adapters sharing the handle. Deletion of an
absent entry is a no-op; deletion and recreation never reuse a committed revision.
Counter exhaustion fails `Unavailable`. Reads copy the index and record under
the catalog lock, then unwrap outside it. Listings return sorted index metadata
without invoking the key provider; they validate record framing and matching
identities, but do not authenticate ciphertext. Resolving a value authenticates
it and never treats a corrupt record or unavailable key provider as absence.

There is no secret cache. `refresh` reports the handle's current snapshot; it
does not reopen a read-only file or discover writes made through another process.
Minimum-revision reads fail when that snapshot is too old. A catalog publication
failure returns `OutcomeUnknown` and fences all secret adapters on that live
handle, including reads, listings, and refresh. Reopen is required to select a
complete checkpoint before continuing. A new adapter on the same handle does
not clear the fence. Failed key wrapping happens before publication and does
not advance the revision.

Vacuum and stable file snapshots preserve the private catalog and its encrypted
records; ordinary document reads/exports do not include them. Portable import
rejects generation replacement with `LiteImportTargetNotEmpty` if the live target
contains any secret state, including a scope head retained after all entries are
deleted. Embedded imports check under the writer reservation and catalog lock
at publication, so secrets committed during import preparation cannot be lost.
CLI imports without `--replace` check while holding the target file writer lock,
before staging, and retain that lock through publication. Explicit CLI
`--replace` remains a destructive whole-file replacement, including secret state.
For a non-destructive import, use a fresh file instead; deleting secrets does not
erase revision history or make an existing scope pristine. File backups still need separately
provisioned key access. Whole-file rollback has the restore and
freshness limitations described above; restoring a backup is not a monotonic
secret revision update.

#### Serverless persistence

**Serverless:** The initial implementation atomically replaces one bounded
ciphertext collection using conditional publication. It is both the head and
payload; no listing, second object, or garbage collector is needed. If larger
collections later require immutable records and a separate versioned head,
upload records before publishing the head and retain objects needed by readers
and backups. The trusted head/index boundary is essential: AEAD alone cannot
authenticate an absence forged by an untrusted storage administrator.

### Implemented distributed and serverless runtime

The existing resolver facade now delegates native operations to the common
`Source`/`NativeStore` contract. Native values precede ordered read-only file
sources and the default-enabled environment fallback. A native read error fails
resolution; only an authoritative absence allows fallback. Legacy file sources
retain their existing last-known-good policy. Data-node facades have no writer.

A metadata node uses:

```json
{
  "secrets": {
    "native": {
      "backend": "distributed",
      "scope": "production",
      "keyring_path": "/run/antfly/native-keyring.json",
      "grants": [
        {
          "name": "data-1",
          "credential_path": "/run/antfly/delivery/data-1.key",
          "keys": ["openai.api_key", "backup.s3.secret_access_key"]
        }
      ]
    },
    "sources": [],
    "environment": true
  }
}
```

A data node uses the same scope, its own delivery credential, and metadata admin
endpoints. It also needs the existing internal-service bootstrap credentials.
It does not receive a wrapping key or access to ciphertext persistence:

```json
{
  "secrets": {
    "native": {
      "backend": "distributed",
      "scope": "production",
      "reader": {
        "name": "data-1",
        "credential_path": "/run/antfly/delivery/data-1.key",
        "urls": ["https://metadata-1.example", "https://metadata-2.example"]
      }
    }
  }
}
```

Delivery credentials are independently generated random 32-byte keys, represented
as 64 hexadecimal characters in mounted files (a trailing newline is allowed).
Include every secret name the data node resolves, including names intended to
fall back to local files/environment after an authorized native absence.
Provision a distinct key per grant. Merely possessing the shared internal-service
credential does not satisfy a grant. Keep grants and wrapping keys consistent
across metadata replicas. Grant definitions load at startup; credential files
reload for every exchange, allowing atomic replacement and revocation. During a
rollout, a metadata replica with the old credential remains an authorized source
until its mount is updated. External credential revocation remains separate.

`POST /internal/v1/secrets:read` supports resolve, filtered metadata list, and scope
revision refresh. The grant name selects a credential; an authenticated encrypted
request binds the operation, trusted scope, key and minimum revision. A reply is
authenticated against the complete randomized request, preventing replay of an old
value or absence into a new read. Responses contain ciphertext and `Cache-Control:
no-store`; no plaintext disk cache or secret values in diagnostics are introduced.
Each successful resolve crosses a metadata Raft read barrier. An outage fails
closed immediately; there is no source-level positive or negative cache, TTL,
watch history, or missed-invalidation recovery to manage in this first version.
Existing consumer caches must still revalidate their secret source before use;
in-flight operations may retain an already acquired credential.

A serverless deployment uses an independent secret namespace:

```json
{
  "secrets": {
    "native": {
      "backend": "serverless",
      "scope": "production",
      "path": "s3://antfly-control/production/native",
      "keyring_path": "/run/antfly/native-keyring.json"
    }
  }
}
```

`gs://` and `file://` are also supported. S3 uses the same refreshable bootstrap
credential provider as serverless storage, including profiles, web identity and
task/instance roles. Storage access uses independently provisioned workload
identity/environment credentials. The bucket must already exist; `file://` uses
the `native-secrets` bucket directory below its root. Providers must guarantee
linearizable reads and conditional writes; an S3-compatible endpoint with
eventual consistency is not supported. The embedded adapter requires an explicit
`linearizable_cas` capability assertion. No secret lives beneath a table prefix,
so table deletion and table backup cleanup cannot delete secrets. Serverless
workers receive the same resolver as request handlers. Serverless `/secrets`
(also `/db/v1/secrets`) administration requires a separate bearer token of at
least 32 bytes, configured through `ANTFLY_SECRET_ADMIN_TOKEN`, even when other
serverless routes are exposed through a trusted ingress. Terminate public TLS at
that ingress. Without the token, administration is disabled.

The mounted keyring is a JSON object with `active` and `keys`, where each key has
an immutable `id` and a 64-character hexadecimal `key`. For example:

```json
{
  "active": "wrapping-2026-09",
  "keys": [
    {"id": "wrapping-2026-09", "key": "<64 random hexadecimal characters>"}
  ]
}
```

Generate each wrapping key independently (for example `openssl rand -hex 32`),
protect the file with OS permissions, and mount it only on decrypting runtimes.
Antfly never creates or persists a root wrapping key next to encrypted records.
The provider reloads the file for every wrap/unwrap, authenticates scope, key name,
entry revision and key ID, and erases owned key material after use. Rotation adds
a new key and changes `active`; retain old key IDs until their records and backups
have been resealed or retired. Replacing bytes under an existing ID is invalid.
A missing keyring or missing retained key is an availability error.

Both adapters write **AFSC v2** (and can read AFSC v1), a bounded collection of
AFSE envelopes: 4-byte `AFSC` magic, u16 version, u16 scope length, u64 scope
revision, u32 entry count (all little endian), a random 16-byte publication ID,
scope bytes, then u32 length + AFSE bytes for each entry. AFSC v1 omits the
publication ID. Every mutation, including delete, gets a new ID so concurrent
identical operations have distinct commit evidence. Entries are sorted by name;
duplicate names, invalid framing, unexpected scope, and entry revisions above
the collection revision are rejected. The initial limits are 1,024 entries and 8
MiB per scope, retaining the common 1 MiB value limit. An empty collection
retains its revision to prevent delete/recreate ABA.

Distributed publication uses metadata transition tag 60 and a private
`native_secrets_v1` namespace. Encryption happens before proposal. Apply checks
the expected scope revision and atomically publishes ciphertext; a losing CAS is
a deterministic no-op. Followers forward the encrypted proposal through Raft and
wait for the exact collection to become visible behind a read barrier. A
different publication at the proposed revision proves a CAS conflict; a later
revision or timeout yields `OutcomeUnknown`, since it may have superseded our
commit. Raft snapshot/export/import of internal metadata retains this namespace.
Public table enumeration does not expose it. **Upgrade every metadata replica
before enabling the distributed backend**; old binaries cannot apply the new
transition or restore its snapshot projection. Disabling the configuration does
not make persisted state downgrade-compatible.

Serverless stores the collection at
`<prefix>/secrets/v1/<hex SHA-256(scope)>/collection`. One conditional PUT publishes
both head and payload atomically, avoiding a two-object commit and orphan GC.
GET captures the payload and its ETag from the same response. GCS captures the
`x-goog-generation` from that response and publishes with `ifGenerationMatch`
(including generation zero for creation), because JSON API upload CAS does not
use the ETag read-condition header. See [GCS preconditions](https://docs.cloud.google.com/storage/docs/request-preconditions). Missing buckets,
missing ETags, permissions failures and unsupported conditional operations are
errors. Only known precondition failures permit automatic preparation retry;
ambiguous publication returns `OutcomeUnknown` without replaying the mutation.
API PUT responses expose the committed entry revision and never the value.

### Bootstrap, caching, and rollout

Startup becomes two phases: obtain storage access, node identity, and
key-provider access from workload identity, mounted sources, or the host
application; then open native storage and resolve application secrets. Bootstrap
credentials cannot reference the store they unlock. Operational provider
registry, inference API/S3 credentials, inference and web-search connection
credentials, external-I/O credentials, and remote-content references are
retained until use; bootstrap settings still resolve before the native store
opens. The existing `environment` switch controls resolver fallback, not cloud
workload identity or the encryption provider.

API mutation success means durable publication, not that all workers have already
refreshed. Return a committed revision, expose observed revisions, and allow
operations to require a minimum revision. Define bounded cache freshness and
whether still-valid last-known-good values are usable during outages. Expired
caches fail; backend outages must not silently change the winning source. Remove
native override and revoke external credential remain different operations.

Lite persistence, distributed Raft persistence, and serverless conditional
publication are implemented against the common contract. Backend tests cover
recovery, competing conditional writers, delete/recreate without revision reuse,
missed invalidations, stale/partitioned reads, key-provider failure and rotation,
unauthorized scope access, and backup/restore. Migration from existing JSON is an
explicit, verifiable import; never overwrite the sole plaintext source before
the encrypted destination is durably committed and readable.

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
- Integrate the shared encrypted record codec with native backends and trusted
  key providers; existing file-store JSON remains plaintext.

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

1. Runtime bootstraps the explicit `secrets` section, or resolves legacy paths
   and deployment defaults when the section is absent.
2. `FileStore.init(alloc, path)` duplicates the path and calls `load()`.
3. `load()` reads and parses the JSON file if it exists.
4. Parsed entries are copied into `entries`.

Reads:

1. `list()` returns stored secret metadata plus environment-only API key secrets.
2. `list()`, `getOwned()`, `getOwnedWithGeneration()`,
   `resolveValueOwned()`, and `resolveValueWithGenerationOwned()` refresh from
   disk first if the file metadata changed.
3. `getOwned()` returns the stored value if present.
4. If no configured file contains the key, `getOwned()` uses environment fallback
   only when enabled.
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
   over the current native target, preserving any configured symlink.

The `/secrets` API is wired through:

- `zig/pkg/antfly/src/api/http_server.zig`
- `zig/pkg/antfly/src/api/httpx_handler.zig`

The handlers receive `ApiHttpServerConfig.secret_store` where a resolver is
configured. Writes require its native write capability, independently of deployment
mode. A file-backed native store on one node does not update other nodes.

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

1. Native override, if configured
2. First matching external source in configured order
3. Environment variable fallback, enabled by default

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

`put()` and `delete()` stage a replacement map and
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

### Config and secret reconciliation acknowledgement

`config.json` and `secrets.json` have deliberately different acknowledgement
rules:

- A remote-content routing or credential-reference change is complete only
  when `GET /status` reports `runtime_config.hash` equal to the SHA-256 of the
  generated `config.json` (the operator publishes its first 16 characters as
  `antfly.io/config-hash`) and `runtime_config.stale` is false.
- Hot publication is deliberately limited to `remote_content`. Before
  publishing a new full-file hash, Antfly verifies that every startup-only
  field is unchanged and that named credential routing is structurally
  complete. A startup-only change leaves the old hash active and reports a
  stale snapshot until the operator's config-hash rollout starts a process
  that loaded the complete file.
- A value rotation behind an unchanged `${secret:...}` reference is complete
  only after `secret_store.source_generation` equals the opaque, non-secret
  generation embedded by the control plane in `secrets.json`, and
  `secret_store.stale` is false. The generation is random publication metadata,
  not a digest of credential bytes. It does not require a config generation
  change or pod rollout.
- `secret_store.supports_source_generation` advertises the acknowledgement
  protocol independently of the currently loaded file. A reload-capable
  process therefore remains distinguishable from a legacy process while
  migrating a generationless projected Secret.
- `secret_store.source_generation` is JSON `null` when the applied file has no
  control-plane acknowledgement generation, including during legacy upgrades
  and after local secret writes.
- `last_reload_failed`, `reload_successes`, and `reload_failures` distinguish a
  successfully published generation from a last-known-good stale snapshot.

Controllers such as Colony must poll the running Antfly process for these
acknowledgements; writing the Kubernetes ConfigMap or Secret alone is not a
runtime acknowledgement. The operator does not read Secret contents. Its
non-secret pod-template config hash is a compatibility rollout fallback for
older Antfly versions that do not publish hot-reloaded config snapshots.

Remote-content requests perform a cheap file-identity check at most once per
second. Reload I/O and validation are serialized separately from snapshot
publication, so concurrent readers continue using the last-known-good snapshot
without waiting for file reads or parsing. Status reads perform an exact content
check so acknowledgement hashes also detect pathological same-metadata edits.

Encrypted-at-rest support inside Antfly is still useful for non-Kubernetes
deployments and simpler VM/bare-metal deployments. It is less urgent for the
Kubernetes enterprise path, where the external manager and Kubernetes secret
projection own most of the secret lifecycle.

The shared encrypted record codec and key-provider contract are now implemented
as AFSE v1 above. It encrypts individual secret values with authenticated identity
metadata; it does not encrypt the existing `PersistedSecretsFile` JSON wholesale.
The codec has an independently generated libsodium/PyNaCl wire vector and tests
for tampering, truncation, wrong keys, entropy failure, and allocation cleanup.
Lite persistence is available to embedding hosts. Distributed and serverless
adapters are connected to their respective runtimes; standalone rejects these
backend selections (Lite remains an embedding-host adapter). Data nodes use the scoped
read-only delivery protocol described above. Projected files continue to
use the existing JSON format and last-known-good reload behavior. Any future
encrypted-file adapter must enforce an explicit cache freshness policy on failed
authentication rather than treating an unreadable native store as absence.

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
- require a native store and update or add exactly that key there
- persist successfully before returning 200

`DELETE /secrets/{key}` should:

- refresh before applying the delete
- persist successfully before returning 204
- return 404 if the native override is absent after refresh, without touching sources
- reveal the next matching source or environment value after removal

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
9. [done] Preserve and resolve remote-content HTTP header references at fetch
   time.
10. [done] Preserve and resolve remote-content S3 references at fetch time,
    including same-length live rotation coverage.
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
   permissions. AFSE records, Lite/distributed/serverless adapters, and runtime
   integration are implemented; explicit migration of existing JSON stores remains
   pending.
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
2. Which production key providers and key-recovery workflows should be shipped
   first, and what bounded cache freshness policy should native backends use?
3. Should S3/backup and remote-content clients share a generation-keyed
   credential cache, or should each subsystem own its own cache?

## Secret-store qualification

`zig build secrets-test` is a required CI gate. It combines the common resolver
and encrypted record tests, distributed/serverless adapter tests, the Lite host
lifecycle, and the `native-secret-lifecycle` VOPR scenario. Lite coverage uses the
real `.aflite` host API and live resolver, including rotation, unavailable keys,
read-only reopen, stable snapshots, concurrent writers, ambiguous sync fencing,
tamper rejection, and the existing backup/import isolation rules.

The VOPR scenario maintains an independent value/revision model while driving
production Lite, standalone file, filesystem object-store CAS, and encrypted
delivery implementations through deterministic `VoprIo`. Every fault is forced
at least once in the bounded test; seeded histories explore different orderings.
It covers competing/stale writers, lost acknowledgements before and after
publication, key rotation, corruption, replayed delivery responses, denied grants,
partitions, Lite sync failures, and crash/reopen. After an uncertain write the
model permits only the complete old or complete new state; it checks that the
adapter never retries the mutation. Every history is exactly replayed. This is
not a virtual metadata Raft cluster: quorum routing and elections are exercised
by the real-process suite below.

`e2e/antfly/test_secrets.py` starts production standalone, three-node metadata
plus data, and two-worker serverless deployments. A local OpenAI-compatible
provider records the Authorization header used by real background enrichment.
The suite checks native precedence, rotation, restart, deletion revealing file
and environment fallback, key failure, metadata-only API responses, serverless
admin authentication, concurrent writers, follower writes, metadata leader death
and catch-up, read-only data nodes, delivery credential revocation, and metadata
outage. The storage-owner ABI borrows the process resolver before any table opens;
this is necessary for background enrichment to see the same native store as the
public API. A cached bearer header must match the newly resolved value even if
source generations collide.

Run from `zig/`:

```sh
zig build antfly secrets-test vopr-build
uv run --project e2e/antfly pytest -q e2e/antfly/test_secrets.py -m 'not objectstore_integration'
python3 ../scripts/ci/zig_vopr_soak.py --binary zig-out/bin/vopr run \
  --scenario secrets --histories 100 --seed 6195175 \
  --corpus /tmp/secret-corpus --output /tmp/secret-campaign
```

The base E2E suite discovers these local tests. Nightly VOPR runs 1,000 secret
histories per exploration-policy shard and retains replay traces and corpora.
The separate `Zig secret store cloud qualification` workflow runs weekly or on
manual dispatch once repository variable `SECRET_STORE_CLOUD_QUALIFICATION=true`
is set. It runs trusted `main` code using the `secret-store-qualification`
environment. Configure its variables `SECRET_STORE_S3_BUCKET`,
`SECRET_STORE_GCS_BUCKET`, `SECRET_STORE_AWS_REGION`, and secrets
`SECRET_STORE_AWS_ACCESS_KEY_ID`, `SECRET_STORE_AWS_SECRET_ACCESS_KEY`, optional
`SECRET_STORE_AWS_SESSION_TOKEN`, and `SECRET_STORE_GCS_SERVICE_ACCOUNT_JSON`.
Use dedicated test buckets and narrowly scoped credentials. The enabled workflow
fails if settings are absent rather than silently skipping either provider.

For local cloud qualification set `OBJECTSTORE_S3_INTEGRATION=1` and/or
`OBJECTSTORE_GCS_INTEGRATION=1`, the corresponding `OBJECTSTORE_*_TEST_BUCKET`,
and the normal object-store credentials; run the same pytest file with
`-m objectstore_integration`. Each test uses a unique `antfly-secret-e2e/` prefix,
checks concurrent conditional publication from separate workers and restart
visibility, and removes its native overrides. Encrypted empty-head objects may
remain: apply a lifecycle expiration policy to that test prefix. Cloud tests
are skipped in ordinary developer/PR runs; filesystem and HTTP mock coverage
must not be reported as real S3/GCS qualification.
