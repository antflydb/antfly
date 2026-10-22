# Visibility Masks

## Purpose

Antfly query execution has to remove documents that should not be returned:
deleted documents, documents created after a read generation, expired TTL rows,
and stale rows left behind in secondary indexes. The write path now maintains
chunked ordinal visibility in primary storage. Current reads use compact deleted
chunks and summary checks; generation-bound reads use visibility chunks carrying
creation/deletion generations. These paths avoid scanning all identity state
for the normal small-complement case.

The implementation is in
[`doc_identity.zig`](pkg/antfly-embedded/src/storage/db/doc_identity.zig).
The design sections below describe the shared mask model; optional cache and
telemetry extensions are not promises of a fully deployed metrics surface.

This is the same shape used by production search systems:

- Lucene keeps per-segment `liveDocs` bitsets and masks candidates at query
  time.
- LSM engines retain tombstones as indexed state and fold them during
  compaction.
- Vector databases commonly maintain per-segment deleted-doc bitmaps and apply
  them during candidate generation or rerank.

Antfly's natural identity for this is the document ordinal.

## Goals

- Make current-read visibility checks proportional to changed visibility state,
  not total document count.
- Avoid full identity scans on first query after a write.
- Support dense, sparse, full-text, match-all, sort, and aggregation paths with
  one shared visibility contract.
- Keep public APIs based on document IDs while using ordinals internally.
- Keep TTL behavior correct without allowing expired documents to consume top-k
  vector slots.
- Provide operator metrics for mask size, build latency, fallbacks, and cache
  behavior.
- Make visibility data durable and recoverable with normal LSM/WAL semantics.

## Non-Goals

- Replacing per-index document-number mappings in one migration.
- Supporting arbitrary historical MVCC reads beyond the existing
  `identity_read_generation` semantics.
- Exposing ordinals, bitmaps, or visibility generations in the public API.
- Optimizing every rare visibility shape before the common current-read path is
  cheap and observable.

## Current implementation and limits

Visibility chunks cover 65,536 ordinals and are maintained atomically with
identity state. Current reads first try summary/deleted-chunk fast paths;
generation reads use the durable visibility records. Missing or invalid chunks
can be reconstructed from the affected ordinal-state range for the current
read. This read-time reconstruction does not imply a scheduled background repair
or a persisted repaired chunk.

The non-visible set is bounded (65,536 entries by default); overflow returns
an unavailable complement so callers can choose the general include/filter
path. Incomplete identity coverage, invalid manifests, and older stores may
require fallback or explicit repair rather than certification of a fast mask.
Full identity/primary scans still exist for validation and general fallback;
the implementation does not promise their removal from every read shape.
TTL remains time-sensitive and requires its own checks alongside ordinal masks.

## Design Summary

Maintain visibility as chunked ordinal bitmaps.

For each table/identity namespace, store durable visibility chunks keyed by
ordinal range. Each chunk contains enough state to answer current visibility
without scanning identity rows:

- `deleted_ordinals`: ordinals that are currently deleted.
- `created_generation`: compact per-ordinal creation generation data, or a
  generation-ordered create log that can build `created_after(read_generation)`.
- `deleted_generation`: optional per-ordinal delete generation data if exact
  historical delete visibility becomes required.
- `ttl_candidates`: optional chunk-local TTL metadata or pointer into the
  timestamp store. TTL remains a separate time-varying dimension unless it is
  promoted to an indexed expiry mask.

The query planner asks for a `VisibilityMask`:

```zig
const VisibilityMask = struct {
    generation: ?u64,
    mode: enum { current, at_generation },
    include: ?OrdinalBitmap,
    exclude: ?OrdinalBitmap,
    ttl_sensitive: bool,
};
```

For current reads:

```text
exclude = deleted_ordinals
```

For generation reads:

```text
exclude = deleted_ordinals OR created_after(generation)
```

If delete-at-generation semantics become exact MVCC semantics:

```text
exclude = created_after(generation) OR deleted_at_or_before(generation)
```

If a mask is too dense or an executor only accepts positive filters, the planner
can materialize an include bitmap from the same chunk data. That is a planner
choice, not the storage format.

## Storage Layout

Use fixed-size ordinal chunks, for example 65,536 ordinals per chunk. The exact
size should be benchmarked, but powers of two make ordinal-to-chunk mapping
cheap:

```text
chunk_id = ordinal >> 16
offset   = ordinal & 0xffff
```

Suggested records:

```text
visibility/chunk/<namespace>/<chunk_id>/deleted_bitmap
visibility/chunk/<namespace>/<chunk_id>/created_generation_blocks
visibility/chunk/<namespace>/<chunk_id>/deleted_generation_blocks
visibility/chunk/<namespace>/<chunk_id>/stats
visibility/manifest/<namespace>
```

`deleted_bitmap` should use a compressed bitmap representation such as roaring.
Generation data can start with block encoding:

- For chunks where all ordinals share one generation range, store a compact
  chunk summary.
- For mixed chunks, store fixed-width generation arrays or delta-compressed
  runs.
- Promote to roaring bitmaps for `created_after(generation)` only when a query
  actually needs them, then cache the derived bitmap by `(chunk_id, generation)`.

The manifest records:

- schema/version
- chunk size
- highest known ordinal
- visibility generation
- dirty chunk count
- optional aggregate counts: live, deleted, created

## Write Path

All identity mutations update visibility chunks in the same logical batch as
the identity rows.

On insert/upsert:

1. Resolve or allocate the document ordinal.
2. Update the identity ordinal state.
3. Clear the ordinal bit in `deleted_bitmap`.
4. Store/update `created_generation` for the ordinal if it was newly allocated.
5. Mark the chunk dirty in memory.

On delete:

1. Resolve the document ordinal.
2. Update the identity ordinal state with delete generation.
3. Set the ordinal bit in `deleted_bitmap`.
4. Store/update `deleted_generation` only if historical delete semantics need
   it.
5. Mark the chunk dirty in memory.

On compaction:

- Merge chunk deltas into canonical chunk records.
- Drop obsolete generation detail if it is older than the supported read
  generation horizon.
- Keep the current `deleted_bitmap` cheap to load without walking history.

On recovery:

- Replay the WAL through the same identity mutation code.
- Rebuild any dirty in-memory chunk cache from durable chunk records.
- Missing or corrupt chunks can be reconstructed for a read from that ordinal
  range. This is not a guarantee of persisted or background repair; identity
  statistics can report invalid chunks separately.

## Query Path

The planner should request a mask once per search request:

```text
request -> VisibilityProvider.resolve(namespace, generation, ttl_policy)
```

The provider returns a mask handle made from chunk references. It should avoid
materializing one giant bitmap unless the executor needs it. Executors consume
the mask in the representation that fits their native candidate identity.

Dense vector:

- Convert ordinal mask chunks to vector-id mask chunks only for touched index
  partitions.
- Prefer pushing exclusions into candidate generation or rerank.
- If only rerank can apply the mask, overfetch enough candidates to preserve
  total-hit relation semantics.

Sparse vector:

- Use sparse doc-num mappings where doc nums are not ordinals.
- Apply chunked ordinal masks directly only when sparse doc nums are ordinal
  aligned.

Full text:

- Keep using snapshot doc-number mappings.
- Use positive projection when stale index rows require it.
- Do not replace full-text snapshot live-doc semantics with an ordinal exclude
  mask unless the doc-number mapping proves it is equivalent.

Match-all and sorted scans:

- Stream candidates through the mask.
- For pure ordinal scans, skip entire chunks when their mask is all excluded.
- For native doc-value sort, pass the mask into the collector so exactness and
  page bounds are computed after visibility.

Aggregations:

- Accept the same mask handle.
- Avoid expanding masks to document IDs unless an aggregation needs stored
  document data.

TTL:

- TTL is time-varying, so it must not be hidden behind a cached identity-only
  mask.
- Long term, maintain an expiry index keyed by expiration time and ordinal.
- The planner can then add `expired_at(now)` to the visibility mask.
- Until then, TTL tables should continue using the TTL-aware positive live path
  for vector top-k correctness.

## Cache Model

Cache visibility by chunk, not by whole database generation.

In-memory cache entries:

```text
(namespace, chunk_id) -> decoded current chunk
(namespace, chunk_id, generation_bucket) -> derived created_after bitmap
(namespace, chunk_id, ttl_bucket) -> derived expired bitmap
```

Invalidation is chunk-local:

- A write dirties only the chunks for touched ordinals.
- Readers can continue using immutable decoded chunks while the writer installs
  new versions.
- The cache can use generation counters per chunk rather than one global
  generation.

This gives better mixed workload behavior than clearing a global live-doc cache
on every identity write.

## Scalability

Target complexities:

- Current read with few tombstones: O(chunks touched by query), often O(1) for
  match-all planning and O(candidate partitions) for vector search.
- Delete write: O(1) chunk update plus normal WAL/LSM write cost.
- First query after write: no O(total documents) scan.
- Generation read: O(chunks touched plus derived generation mask construction).
- A missing chunk can be reconstructed within its ordinal range; general
  include/validation fallback may still scan identity or primary state.

The important boundary is that no normal query should scan all identity states
just to learn that there is one tombstone.

## Operator UX

Proposed telemetry extensions (not all exported today):

- `antfly_visibility_chunks_total`
- `antfly_visibility_dirty_chunks`
- `antfly_visibility_cache_entries`
- `antfly_visibility_cache_hits_total`
- `antfly_visibility_cache_misses_total`
- `antfly_visibility_mask_build_ns_total`
- `antfly_visibility_mask_builds_total`
- `antfly_visibility_full_scan_fallbacks_total`
- `antfly_visibility_repair_count_total`
- `antfly_visibility_deleted_ordinals`
- `antfly_visibility_mask_bytes`
- `antfly_visibility_overflow_total`

A future compact status summary could be:

```json
{
  "visibility": {
    "chunk_size": 65536,
    "chunks": 128,
    "dirty_chunks": 0,
    "deleted_ordinals": 42,
    "cache_entries": 128,
    "full_scan_fallbacks": 0,
    "last_repair_generation": 0
  }
}
```

This makes the UX practical for operators: if queries get slower, they can see
whether Antfly is hitting masks, rebuilding chunks, or falling back to scans.

## Remaining work

Chunk records, atomic write maintenance, current deleted complements, and
bounded chunk reconstruction are implemented. The chunk size and primary-store
namespace are fixed in the current format; these are no longer rollout questions.

Remaining improvements include broader per-index acceleration, cache tuning,
and operator telemetry. The metrics/status shape above is a proposed extension,
not an inventory of exported Prometheus series. Existing identity statistics
include visibility chunk count, deleted ordinals, mask bytes, and invalid-chunk
repair counts; they do not prove that background repair has completed. TTL
expiry and general fallback must continue to fail or filter correctly when a
compact mask cannot be certified.
