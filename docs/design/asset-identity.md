# Asset identity across a provider change

Status: implemented (Antfly `checksum_algorithm`, antfly-source
`identity.namespace`); verified with a real local server, MinIO, and
embedder run (see Verification).

## Principle

Identity shouldn't depend on where bytes live. A document that moves to a
new URL, bucket, hostname, or provider is still the same document if its
bytes are unchanged, and the system that owns a document's identity should
say so without needing to know anything about *why* it moved. This task
gives each system exactly one change, and neither is specific to migration:
Antfly gets a way to identify a document by a declared content digest
instead of by its location, and antfly-source gets a way to key a row by a
stable namespace and relative path instead of by the provider that happened
to acquire it. Antfly never has to know a row came from a migration, a
rotated signed URL, or a hand-edited config; antfly-source never has to know
Antfly keeps derived data keyed by document ID. Each side only has to keep
its own promise: antfly-source's job is to not change the ID when the
content didn't change; Antfly's job is to not recompute when the content it
already has a digest for is still correct, no matter how the row claims to
have gotten it this time.

## The problem, and why it recomputes today

Two independent causes force recomputation on a pure relocation, one in each
system.

**Antfly.** `document_extraction`'s skip decision (`metadataFingerprintAlloc`
in `document_extraction.zig`) hashes `source_url` together with `etag`,
`version`, and `last_modified`. All four are properties of *where* the bytes
were fetched from, not of the bytes themselves. Changing any one of them —
exactly what happens when a source moves — produces a different fingerprint,
so extraction reruns: download, parse, OCR/transcription where configured,
chunk, embed. The row's digest, if the config exposes one at all, is never
consulted for identity; it's just one more field among several hashed
together with the location fields that actually change.

**antfly-source.** A row's key (`RecordKey` in `sync.go`) is
`sha256(providerID)`, and `providerID` is the connector's own acquisition
identity (`item.ID`) — a string that embeds the provider's scheme, the
source's connection namespace, and the item's provider-native path (e.g.
`s3://job/<source>/<bucket>/<key>` or `local://job/<source>/<prefix>/...`).
Changing the connection or provider changes this string unconditionally,
even when the object is a byte-for-byte copy with the same relative path.
The new row gets a new key; the sync writer's reconciliation pass then
deletes the old key as absent. From Antfly's perspective this isn't an
update — it's a delete and an unrelated insert, so even a perfect Antfly-side
fix accomplishes nothing: the row it would have recognized never arrives
under the same ID.

Both causes are independent and both have to be fixed for a provider change
to actually keep derived data: Antfly needs a reason to treat relocated
bytes as unchanged, and antfly-source needs to actually present them under
the same ID when it can.

## Antfly: `checksum_algorithm`

`DocumentExtractionConfig` (and its nested `source` field-reference block)
gains `checksum_algorithm` (today, the only value is `sha256`). It declares
that `checksum` (or the row value named by `source.checksum_field`) is a
digest of the original bytes, computed with that algorithm.

When `checksum_algorithm` is set and the row resolves to a well-formed
digest, the metadata fingerprint (now v4) hashes the digest, the filename,
the content type, the native-text policy/quality config, and the extraction
config — and nothing about location: no `source_url`, `etag`,
`last_modified`, or `version`. A stored v3 fingerprint (computed before
`checksum_algorithm` was declared, or before this existed at all) is also
accepted as a match, recomputed from the row's *current* fields and checked
alongside the v4 candidate: if it still matches what's stored, the row's
location and metadata haven't changed since that extraction, exactly as
strictly as v3 ever proved that on its own. This means declaring
`checksum_algorithm` on an already-populated source does not force a
one-time re-extraction of every row; only a row whose location *also*
changed loses its free pass, which is correct, not a regression.

The declared digest is not trusted blindly. The bytes extraction actually
downloads are hashed — using whichever hash the fetch path already computes
for other purposes (`prepared_source.content_sha256` on the async path, a
fresh hash over the already-fully-buffered bytes on the synchronous
precommit path; neither path buffers or hashes anything it wasn't already
going to), never a second read or a second buffer — and compared
case-insensitively against the declared digest. A mismatch fails extraction
with a clearly named error (`DocumentChecksumMismatch`) through the existing
failure-manifest path, rather than silently keeping stale output alive under
a digest that no longer describes what's actually at the source. A declared
digest that isn't a well-formed digest for its algorithm (wrong length,
non-hex characters) is `InvalidChecksumDigest`, a configuration error, never
a silent fall back to location-based identity — a typo in a checksum field
must not be able to pin stale output forever just because it also happens to
disable identity checking.

Without `checksum_algorithm`, behavior is byte-for-byte unchanged, including
for a row that sets a bare `checksum_field` with no algorithm declared: the
v4 path is only reachable once an algorithm is declared, and in every other
case `metadataFingerprintAlloc` is now a thin wrapper that defers to exactly
the old v3 formula.

`checksum_field` (and the sibling `*_field` names) resolved only a top-level
row key before this; a verified digest (`go/pkg/sources/document.go`'s
`metadata["sha256"]`) lives nested under `metadata` in antfly-source's row
envelope, not at the top level. Rather than ask antfly-source to duplicate
that digest into a second, top-level field purely for Antfly's convenience —
which would create exactly the kind of second source of truth this task is
trying to avoid — the row-field lookup (`dupeOptionalStringField`) now walks
a dot-separated path (e.g. `checksum_field: "metadata.sha256"`) through
nested objects when the field name contains one. A field name with no dot is
an unaffected, ordinary top-level lookup, so every config written before
this existed keeps behaving exactly as it did.

## antfly-source: `identity.namespace`

`Source` gains `identity: {namespace: string}`. A row's key is now
`sha256(namespace + "/" + relative_key)` when a namespace is set, where
`relative_key` is a connector-reported key relative to the *source's own
root* — independent of which connection or provider acquired it. With no
namespace, a source keys its rows exactly as it always has
(`RecordKey(sourceID, providerID)`, unconditionally byte-for-byte: this is
not a special case inside a shared formula, it's the literal old function,
untouched, still called by the new one when there's nothing to scope by).

A new source gets an opaque namespace at creation — today, its own ID,
though nothing depends on that choice being stable or portable, only on it
existing and never changing — so nothing is ever keyed before the source
has one, and a later connection or provider change on that source never has
to re-key anything. `identity.namespace` is immutable once set: a PUT that
omits `identity` leaves it exactly as it was (never clears it by omission),
and a PUT that tries to change an already-set namespace is rejected (409).
A PUT that changes connection or config on a source with **no** namespace
yet still re-keys every row, exactly as it always has — nothing before this
field existed separated identity from connection/config — but the response
now says `rekeys: true`. Setting a namespace for the first time on an
already-populated source is accounted the same way: it's the one
unavoidable re-key that adopting this feature on existing data costs, and
it's worth stating plainly rather than hiding it, since a provider change
bundled with that adoption will still delete-and-reinsert that one time.

Connectors expose `relative_key` relative to the source's own configured
root, not the bucket or shared store: S3 and GCS strip their configured
`prefix` from the full object key; local strips its configured `prefix`
from the local store path (and since local storage is already named by
content digest, this makes a local object's relative key the digest-named
path itself, stable across a prefix change too). Providers with no
path-like key of their own — Drive, GitHub, and other collaboration
surfaces — report no relative key; it defaults to the item's own native
provider ID (its `item.ID` with the connector's own ownership prefix
stripped). This is the honest answer for those providers, not a workaround:
moving a Drive file or a GitHub blob to a different *provider* is not a
meaningful operation to begin with (there is nowhere for it to move to that
still means the same thing), so their identity is just their native ID,
exactly as before.

`migrate-local` loses its own identity logic entirely — there wasn't much to
lose, since the right behavior is for it to do nothing special: it sends a
normal `PUT` with the new connection and config, plus `identity` if the
source doesn't have a namespace yet (defaulting it to the source's own ID,
the same default a brand new source gets), then syncs through the ordinary
pipeline. Because `--to-prefix` is required to produce the same
`objects/sha256/<digest>[.ext]` relative layout the local store itself uses
(stripped of each side's own prefix the same way), a source that already
has a namespace is never re-keyed by a provider change migrate-local
performs — rows update in place. Its warning is now conditional and
honest about the two separate things that determine the outcome: whether
*this run* assigned a namespace (the one unavoidable re-key, printed as such
after the run, not guessed beforehand), and — regardless of that — whether
the destination table's own `document_extraction` config declares
`checksum_algorithm` for this source's rows, which `migrate-local` has no
way to see or confirm: it is server/Zig-side configuration, not anything
visible in a source profile. Without it, Antfly still recomputes derived
data for a row whose location metadata changed, even when `migrate-local`
kept that row's ID exactly the same.

## Compatibility

- No `identity.namespace`: `RecordKey` unchanged, byte for byte, for every
  input it previously accepted or rejected (pinned by
  `TestIdentityKeyPreservesLegacyFormulaWithoutANamespace`).
- `identity.namespace` set: the key depends only on namespace and relative
  key, never on connection, provider, or revision metadata — confirmed by
  constructing the *same* namespace+relative-key pair under two different
  providers and checking they key identically
  (`TestIdentityKeyNamespaceFollowsContentNotLocation`).
- The "setting namespace to a source's current provider-ID prefix
  reconstructs that provider ID" property that makes namespace adoption
  legible is a plain string-concatenation identity —
  `TrimSuffix(providerID, "/"+relativeKey) + "/" + relativeKey == providerID`
  — not a claim that the resulting `IdentityKey` equals the legacy
  `RecordKey` (the two formulas hash different strings on purpose: the
  whole provider ID vs. namespace-plus-relative-key). Pinned for each
  path-like provider against a real `Enumerate` call:
  `TestLocalRelativeKeyStripsConfiguredPrefix`,
  `TestS3RelativeKeyStripsConfiguredPrefix`,
  `TestGCSRelativeKeyStripsConfiguredPrefix`.
- `SourceRow`/`SyncWriter.Upsert` recompute and verify a row's key from the
  row's own persisted fields (`source.item_id`, `source.relative_key`) under
  the owning source's namespace, never trusting an externally supplied key
  (`TestSourceRowRecomputesIdentityUnderTheOwningNamespace`).
- The full `PUT` lifecycle — auto-namespace at creation, rejection of a
  namespace change, omission preserving the existing value, and the
  `rekeys` accounting in each case — is exercised end to end against the
  real service handler in `TestServiceSourceIdentityNamespaceLifecycle`.
- `TestMigrateLocalAdoptsNamespaceAndRekeysExactlyOnce` runs the real
  Runner/Publisher pipeline: a source synced once while still on `local`
  (so it predates `identity.namespace`, like a real pre-upgrade deployment)
  is migrated to S3; the legacy key's row is gone, the new key's row is the
  only one present, and both the PUT response and `IdentityNotice` report
  the one-time rekey.
- Zig: `checksum_algorithm` unset keeps `metadataFingerprintAlloc`
  byte-for-byte equal to the old formula
  (`"...ignores config without checksum_algorithm, byte for byte"`); once
  set, the fingerprint is identical across two different declared
  locations and still changes when the digest itself changes
  (`"...v4 fingerprint ignores location once a digest is declared"`); a
  malformed digest is rejected rather than silently falling back
  (`"...rejects a malformed digest rather than falling back to location"`);
  downloaded-byte verification matches, mismatches, and no-ops correctly
  (`"...checksum verification matches, mismatches and no-ops"`); and the
  dotted-path row lookup resolves a source row's nested
  `metadata.sha256`, leaves a plain field name unaffected, and fails closed
  (no match, not an error) on a path that doesn't resolve
  (`"...checksum_field resolves a dotted path into a source row's nested
  metadata"`).

## What else this helps

Nothing about either change is specific to a local-to-S3 migration; both are
general statements about identity. The same digest-based Antfly fingerprint
and the same namespace-based antfly-source key cover, without any further
change: a rotated signed URL or refreshed presigned token (the URL changes,
the bytes don't); a CDN or hostname change in front of the same object; an
S3 bucket rename or a cross-region move; mirroring the same bucket to a
second provider for redundancy; and a provider migration in the other
direction, GCS to S3. Each of these changes exactly the fields this task
removed from Antfly's fingerprint and from antfly-source's key formula, and
nothing else.

## Alternatives rejected

**Copy derived data to the new key instead of avoiding the rekey.** This
was rejected because it has to happen for every artifact kind a source's
documents can have (OCR text, chunks, embeddings, graph edges, and whatever
is added later) and has to be kept in lockstep with each one's own storage
format forever, while the approach here needs no artifact-specific code at
all: it prevents the rekey, so there is nothing to copy.

**Provider-neutral IDs for every source.** Giving every source a
provider-independent ID (rather than an opt-in namespace) looks simpler but
re-keys every row of every existing source immediately, as a one-time
migration with no opt-out, instead of letting only a source that actually
changes provider pay that cost, once, only when it happens.

**A global content-digest cache.** A cache mapping content digests to
already-computed derived artifacts, shared across sources and even across
tables, would also solve "don't recompute what we've already computed" and
would generalize further than this task's scope (for example, to two
unrelated sources that happen to ingest the same file). It's a reasonable
project on its own, but a materially larger one — it needs its own storage,
eviction, and consistency story independent of any one producer or table —
and isn't needed to make a provider change keep what one source already
produced for itself.

**Multiple checksum fields for identity.** Letting `DocumentExtractionConfig`
declare several checksum fields, perhaps with a priority order (prefer a
strong hash, fall back to a weaker one), was considered and rejected.
Identity needs one answer, and several fields can disagree; a priority
order would also change a row's fingerprint the moment a higher-priority
digest is *added* to an already-extracted row, re-extracting identical
bytes for no reason related to the bytes themselves. MD5, S3 ETags, and GCS
CRC32C can't identify the same content across providers anyway (an ETag is
not even always a content hash, and CRC32C is too weak to trust as an
identity, only as a transfer check), so there is no real case where a
source has several checksums that are all trustworthy candidates and must
be reconciled. A source with only a weak checksum simply leaves
`checksum_algorithm` unset and keeps today's location-based behavior; an
ordinary transfer-integrity check some provider happens to expose stays an
ordinary row field, not identity. Moving to a stronger algorithm later is a
new enum value and a config change, which correctly re-extracts once, the
same one-time cost namespace adoption already asks for on the
antfly-source side.

## Out of scope

Signed media references (`service_media_http.go`'s `media_pin`) bind to a
source's connection and config (`sourceMediaBinding`), so any connection or
config change — including the one this task's own migration path makes —
invalidates every outstanding pin immediately (410) rather than waiting for
the next sync or prune pass. That gap predates this task and is unrelated to
it: it exists whether or not a row's ID changes, and whether or not derived
data survives. Loosening it to tolerate a connection change within the same
provider identity is a separate security decision (pins exist specifically
to bind a signed reference to the connection that was authorized to serve
it), not something this task's identity model should casually relax as a
side effect.

A generic field-scoped skip check for *every* producer kind — so that a
provider change never retriggers anything beyond document_extraction's own
decision — is also out of scope. Document extraction's own skip decision
(fixed here) and chunk-sourced embedding's skip decision
(`modelChunkEmbeddingSourceHash`, already content-scoped, unaffected) both
correctly ignore a pure relocation already. A narrower producer-dispatch
path does not: a dense or sparse embedding producer configured to read a
field directly off the raw document row (bypassing `document_extraction`
and chunking entirely) gates on a whole-row content digest
(`capturePrimarySource` in `artifact_publication.zig`) before it ever
reaches the content-scoped fallback (`shouldSkipEmbeddingArtifact`), and
that whole-row digest changes whenever *any* field changes, including
`source.uri`. That gate is shared by many producer kinds beyond embedding,
for correctness properties beyond simple caching (it is also how a producer
notices its row changed underneath it at all), so narrowing it is a
separate change with its own review, not a small adjustment to make here
silently. It does not affect the document-extraction pipeline this task's
proof exercises, which chunks before embedding.

## Verification

Unit tests for every compatibility rule above pass: Go (`go test ./...` in
`go/pkg/sources`, and in `go/pkg/sdk` after `make generate`); Zig (the
`checksum`-filtered subset of `unit-storage-test`, and a full, unfiltered
`antfly-storage-db-enrichment-test` run covering document extraction
broadly). `make -s license-check` passes.

Real end-to-end run, no mocks, against this worktree's own build (not the
`feat-video-codec-matrix` binary used for Go-only work elsewhere, since this
changes Zig): a real `antfly standalone` server (local storage engine) with
a real `document_extraction` → `chunk` → `embedding` pipeline, a real local
embedder (`BAAI/bge-small-en-v1.5`, already-downloaded weights, no network),
a real local MinIO instance, and `antfly-source migrate-local` repointing a
running `antfly-source serve` instance over its own admin API, with `serve`
never stopped.

A source was synced once on a `local` connection (no `identity.namespace`
yet, like a real pre-upgrade deployment); extraction, chunking, and
embedding all ran for real (confirmed by a real match from both full-text
and semantic search). A plain `PUT` then gave it a namespace with no
connection change (`rekeys: true`, as designed): the row's ID changed once,
and extraction/embedding ran again for the new ID, since there was no prior
state to skip against under it -- the one unavoidable cost of adopting this
feature, exactly as documented above. From that point, `migrate-local`
moved the source from the local store to MinIO (the actual destination
objects, read back and hashed, matched; `rekeys: false`, as designed, and
its printed `IdentityNotice` said rows updated in place):

- The extraction-fetch log added for this proof (`document extraction
  fetching source`, in both `enrichment_runtime.zig` and
  `mutation_materialization.zig`) recorded **zero** new fetches across the
  move: 2 before, 2 after.
- `skip_by_hash_count` on the table's `document_vectors` index went from
  0 to 2 across the same move: one skip for `document_extraction`'s own
  fingerprint, one for the chunk-sourced embedding's content hash --
  exactly the two producers this task's checks traced.
- The row's ID was byte-for-byte identical before and after
  (`.../records/91afc9b3...`); only `source.uri` changed, from
  `local:///...` to `s3://asset-identity/...`.
- Full-text search (`"Joseon"`) and semantic search
  (`"Korean dynasties history"`) both still returned that same row,
  unchanged, immediately after the move.

A file's content was then changed in place at the same S3 key (bypassing
antfly-source's own write path, to isolate a pure content change from a
location change) and resynced: the extraction-fetch count advanced to 3,
the extraction generation advanced, and full-text search found the new
text -- a real, measured recompute, under the same row ID throughout.

Finally, the row's declared digest was corrupted directly (a well-formed
but wrong 64-hex-character value) and the artifact reprocessed: extraction
fetched the real bytes, hashed them, and failed with exactly
`last_error_code: "DocumentChecksumMismatch"` / `"declared checksum does
not match the downloaded bytes"` -- not a silent skip, not stale output
kept alive. A separately-tried malformed (wrong-length) digest produced
`InvalidChecksumDigest` instead, confirming the second named failure path
for real as well. Restoring the correct digest and reprocessing converged
cleanly again, with no residual error.
