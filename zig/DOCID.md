# Document IDs and Posting IDs

## Purpose

`antfly-zig` currently inherits the same flat-keyspace idea as Go antfly:
document IDs are written directly into LMDB keys, and derived records are built
by concatenating textual delimiters and suffixes.

Examples from the current code:

- primary document: `<docID>`
- embedding: `<docID>:i:<indexName>:e`
- summary: `<docID>:i:<indexName>:s`
- enrichment artifact: `<docID>:e:<type>:<name>:...`
- TTL metadata: `<docID>:t`

This works only as long as document IDs do not collide with the storage format.
It couples correctness to delimiter avoidance and makes arbitrary byte-valued
document IDs unsafe.

This document proposes a document identity design for `antfly-zig` that:

- allows arbitrary document IDs
- preserves lexicographic ordering by raw document ID bytes
- supports efficient prefix scans
- removes delimiter-based parsing from internal storage
- gives query execution a compact internal posting identity for bitmap filters
  and exclusions

## Current Problem

The current storage layout depends on raw user IDs appearing as the leading
portion of many structured keys.

That creates several classes of problems:

- document IDs containing `:i:`, `:e:`, `:t`, `:out:`, or similar markers can
  confuse key parsing
- document IDs containing bytes such as `0x00` or `0xff` interact badly with
  sentinel-based range construction
- metadata and secondary records are distinguished by string suffixes instead of
  explicit record types
- internal code must repeatedly split and parse strings to recover structure

Even when a specific path happens to work today, the format is brittle because
new record types must keep avoiding user-controlled bytes.

## Goals

- Accept arbitrary byte sequences as document IDs.
- Keep LMDB ordering aligned with the raw byte ordering of document IDs.
- Preserve efficient range scans over ordered IDs.
- Preserve efficient "all IDs with prefix P" scans.
- Make primary records and derived records structurally distinct.
- Keep the public API based on raw user document IDs, not encoded IDs.
- Give the backend one canonical internal document identity that all index
  families can share.
- Make query-time filters and exclusions document-set native internally, with
  compact ordinal bitmaps used when they are the right physical representation.

## Non-Goals

- Backward-compatible on-disk keys for old databases.
- A text-readable internal key format.
- Reusing delimiter parsing for new code paths.
- Exposing internal posting IDs in the public API.
- Requiring every index family to migrate to ordinal-backed filtering in a
  single flag-day change.

## Design Summary

Use a binary tuple format:

1. Encode each variable-length component with an order-preserving escape codec.
2. Terminate each encoded component with an unambiguous terminator.
3. Add an explicit record-kind byte after the document ID component.
4. Encode fixed-width numeric fields in big-endian form.

With this structure, LMDB's normal lexicographic ordering gives us the ordering
we want without requiring document IDs to avoid reserved characters.

## Component Codec

Each variable-length component, such as `doc_id`, `index_name`, or `edge_type`,
is encoded as:

- every nonzero byte `b` is emitted as `b`
- byte `0x00` is emitted as `0x00 0xff`
- the component terminator is `0x00 0x00`

This gives three useful properties:

- arbitrary bytes are representable
- the encoded form preserves the ordering of the original byte sequence
- no encoded component contains `0x00 0x00` except as the terminator

### Why Ordering Is Preserved

For two raw byte strings `a` and `b`, compare them byte-by-byte:

- if the first differing byte is nonzero in both strings, the encoded order is
  unchanged
- if one string has `0x00` at the first differing position, it encodes to
  `0x00 0xff`, which sorts after the component terminator `0x00 0x00` but
  before any later nonzero byte at a greater position, matching raw ordering
- if one string is a prefix of the other, its encoding ends with `0x00 0x00`,
  which sorts before the longer string's next encoded content

As a result, lexicographic ordering of encoded components matches lexicographic
ordering of raw components.

## Key Layout

We should stop using "raw doc ID followed by textual suffixes" and instead move
to explicit binary records.

Suggested high-level layout:

```text
primary document
  D | enc(doc_id) | 00 00 | P

ttl timestamp
  D | enc(doc_id) | 00 00 | T

summary
  D | enc(doc_id) | 00 00 | S | enc(index_name) | 00 00

embedding
  D | enc(doc_id) | 00 00 | E | enc(index_name) | 00 00

chunk
  D | enc(doc_id) | 00 00 | C | enc(index_name) | 00 00 | u32be(chunk_id)

enrichment artifact
  D | enc(doc_id) | 00 00 | A | enc(kind) | 00 00 | enc(name) | 00 00 | ...

outgoing edge
  G | enc(source_doc_id) | 00 00 | enc(index_name) | 00 00 | enc(edge_type) | 00 00 | enc(target_doc_id) | 00 00 | O

incoming edge
  G | enc(target_doc_id) | 00 00 | enc(index_name) | 00 00 | enc(edge_type) | 00 00 | enc(source_doc_id) | 00 00 | I
```

Notes:

- `D` and `G` are top-level namespaces, not literal strings.
- `P`, `T`, `S`, `E`, `C`, `A`, `O`, and `I` are single-byte record kinds.
- fixed-width integers should be big-endian so numeric order is preserved
  lexicographically
- internal metadata should remain under a separate reserved prefix outside user
  document namespaces

## Ordering Properties

Within the `D` namespace:

- all records for a document are adjacent
- documents are ordered by raw `doc_id`
- range partitioning by encoded document key remains valid

That means shard ownership and median-key splitting can continue to rely on raw
LMDB key ordering as long as they operate on encoded primary-key boundaries.

## Prefix Scans

Supporting arbitrary IDs is not enough. We also want efficient "scan all
documents whose ID starts with prefix `p`".

### Prefix Scan Range

For document ID prefix `p`, scan:

```text
lower = D | enc_prefix(p)
upper = next_prefix(lower)
```

Where:

- `enc_prefix(p)` is the encoded byte stream for `p` without appending the
  component terminator
- `next_prefix(x)` returns the smallest byte string strictly greater than all
  keys beginning with `x`

This is the standard lexicographic prefix-range pattern.

### Computing `next_prefix`

Pseudo-code:

```text
fn nextPrefix(buf):
  out = copy(buf)
  i = out.len
  while i > 0:
    i -= 1
    if out[i] != 0xff:
      out[i] += 1
      return out[0 .. i + 1]
  return null
```

If `nextPrefix(lower)` returns `null`, the scan is unbounded above.

### Why This Works

Because the component encoding preserves raw lexicographic order and because the
document namespace is a contiguous prefix, every raw document ID that begins
with raw prefix `p` will map into one contiguous encoded key range.

## API Surface

The user-facing DB API should continue to treat document IDs as raw bytes or raw
strings.

Encoding rules should be internal to the storage layer:

- write paths encode user IDs before touching LMDB
- read paths decode IDs when returning results
- scan APIs that expose user IDs decode before returning them
- internal comparisons on user IDs should compare raw IDs, not encoded slices

## Implemented Runtime Identity Notes

The current Zig implementation has a shard-local ordinal identity layer in
addition to the binary key layout work described above. The long-term invariant
is:

```text
ordinal identity is valid only for:
  namespace(table_id, shard_id, range_id) + identity_read_generation
```

Internal `ShardDocSet` / resolved-doc-filter envelopes must carry both values.
Consumers must reject a resolved ordinal set when either the namespace or the
generation differs from the target DB. This is intentionally fail-closed because
ordinal values are compact physical IDs, not public document IDs.

Operationally:

- split cutover must give child ranges their own namespace/generation boundary
  and must invalidate parent-range resolved-filter caches
- merge cutover must mint a new merged-range namespace/generation boundary and
  reject cached child-range ordinal sets
- reassignment must be treated like an ownership identity change, even when the
  byte range is unchanged
- rebuilds that regenerate ordinal mappings must bump the identity generation
  before new ordinal-backed filters are accepted
- cache keys for resolved filters, ordinal bitmaps, and ordinal-to-vector
  projections must include namespace and `identity_read_generation`
- public APIs must keep rejecting internal resolved-doc-filter envelopes

The in-process structured-filter cache now keys shared entries by the full
`Namespace{table_id, shard_id, range_id}` plus generation, not by a compressed
tag. This prevents a stale ordinal set from being reused across two ranges that
would collide under a lossy namespace identifier.

Near the ordinal limit, allocation must fail with `DocOrdinalExhausted` before
wrapping. Ordinal `0` remains reserved as "missing", so `maxInt(u32)` is not an
allocatable live ordinal.

## Canonical Document Identity

The binary key codec fixes how user document IDs are stored. It does not, by
itself, give the query engine a compact internal identity that can be shared by
full-text, dense vector, sparse vector, algebraic, and graph indexes.

Long term, Antfly should treat document identity as a backend primitive:

```text
table / shard / range
  raw doc_id <-> canonical_doc_id <-> doc_ordinal
  doc_ordinal -> visibility / tombstone / generation
  indexes store doc_ordinal in postings, vectors, sparse rows, and graph edges
  query planner exchanges document sets keyed by doc_ordinal
  final projection maps doc_ordinal back to raw doc_id / source document
```

The public API remains document-ID based. `doc_ordinal` and posting bitmaps are
internal physical planning details.

### Identity Layers

Use two related identifiers instead of forcing one identifier to satisfy every
property:

```text
canonical_doc_id = deterministic shard-local identity for a raw doc_id
doc_ordinal      = compact shard-local posting ID used in bitmaps
```

`canonical_doc_id` should be deterministic per shard or range. A typical shape
is:

```text
canonical_doc_id = hash64(table_id, shard_id, raw_doc_id)
```

This is useful for rebuilds, restore validation, and cross-index consistency.
It should not be the primary bitmap key because a hash is sparse and can
collide unless collision handling is added.

`doc_ordinal` should be compact and persisted:

```text
doc_id           -> doc_ordinal
canonical_doc_id -> doc_ordinal
doc_ordinal      -> doc_id / canonical_doc_id
doc_ordinal      -> generation / visibility / tombstone
```

The ordinal should normally be `u32` within a shard or range so Roaring bitmaps
remain efficient. If a shard can exceed the `u32` space, the higher-level
identity should be `(range_id, doc_ordinal)` or the range should split before
that limit becomes operationally risky.

### Why Not Deterministic Ordinals Only

It is tempting to compute:

```text
doc_ordinal = hash32(raw_doc_id)
```

That avoids an allocation table, but it gives up too much:

- collisions need a second mechanism anyway
- hash values are sparse, which weakens bitmap compression
- changing hash policy changes physical identity
- ordering by document ID no longer helps scans or locality

It is also tempting to define ordinals as sorted positions in document-ID order.
That is compact and deterministic, but insertions would shift ordinals unless
the system forbids renumbering, which again requires a persisted allocation
table. Sorted positions are useful inside immutable index segments or offline
snapshot builders, where they can support dense arrays, rank/select-style
iteration, and deterministic rebuild-local ordering. They should not be the
shared live document identity. The shared identity must be stable under ordinary
inserts, deletes, and rebuilds. Sorted-position IDs also should not be stored as
durable vector IDs unless the vector index generation is rebuilt at the same
time, because any resort changes the vector/document mapping.

A sparse stable ID is a different tradeoff. A random or hashed sparse ID can
avoid an allocation table only by accepting collision handling, weaker bitmap
compression, and worse candidate locality. A persisted monotonic `u64` avoids
collision risk and compaction pressure, but it is still an allocation-table ID:
the system still needs `doc_id -> id`, `id -> doc_id`, visibility generations,
namespace validation, restore validation, and cache/index generation boundaries.
Sparse IDs reduce the need to ever reassign IDs; they do not remove the
identity lifecycle.

The better design is:

```text
deterministic canonical identity + persisted compact ordinal
```

This gives stable rebuild semantics without giving up bitmap performance. In
normal operation rebuilds should preserve allocated ordinals. Any operation that
chooses to compact or reassign them must create a new identity/index generation
and rebuild every ordinal-backed artifact before cutover.

Implementation note: the local identity table now persists the canonical
forward row as well as the public-ID and reverse ordinal rows:

```text
raw doc_id        -> doc_ordinal
canonical_doc_id  -> doc_ordinal
doc_ordinal       -> raw doc_id / canonical state
```

The canonical row is maintained by new writes and namespace reassignment, and
validation treats existing canonical rows as authoritative consistency checks
while still allowing older stores that have not backfilled the row yet. The
focused DOCID gate now includes both mixed-version validation coverage for
missing canonical rows and a conflict-allocation guard that rejects a corrupt
canonical row before reserving a new ordinal.

## Document-Set Native Planning

Current internal query paths often resolve filters and exclusions into string
document-ID lists. That works, but it forces every index family to repeatedly
translate public IDs back into its own physical identity.

Introduce an internal document-set abstraction. Persisted compact ordinals are
the canonical internal identity, but query-time sets should remain adaptive:

```zig
const ResolvedDocSet = union(enum) {
    all,
    none,
    doc_keys: []const []const u8,       // tiny sets and compatibility fallback
    ordinals: []const DocOrdinal,       // small resolved ordinal sets
    ordinal_bitmap: RoaringBitmap,      // large dense or reusable sets
};

const ResolvedDocFilter = struct {
    include: ResolvedDocSet,
    exclude: ResolvedDocSet,
};
```

The planner should prefer ordinal-backed forms whenever the shard has
doc-ordinal coverage, but it should not force every tiny set through a bitmap.
`doc_keys` remains useful for explicit small ID filters, point lookups, API
compatibility, and index families that have not yet been converted.

A reasonable normalization policy is:

```text
0 docs                 -> none
small explicit IDs     -> doc_keys or sorted ordinals
small resolved sets    -> sorted ordinals
large dense sets       -> ordinal_bitmap
large sparse sets      -> sorted ordinals until density justifies bitmap cost
reused/composed sets   -> representation chosen from cardinality + density
all visible docs       -> all plus live_docs_bitmap at execution
```

The first thresholds should be empirical. The current implementation keeps
medium sets and large sparse sets as sorted ordinal arrays, and promotes only
large dense sets into roaring bitmaps. Instrumentation should continue deciding
whether a doc-key/ordinal list or a bitmap is cheaper for a specific operator.

Useful bitmap sets:

```text
live_docs_bitmap      documents visible to the read snapshot
deleted_docs_bitmap   tombstoned or hidden documents
include_bitmap        positive filter constraint
exclude_bitmap        exclusions, blocked IDs, must_not filters
candidate_bitmap      index-produced candidate set
```

Query execution should intersect and subtract these bitmaps before expensive
scoring whenever possible. Tiny lists can stay as lists until an operator needs
set algebra, repeated reuse, or bitmap-only execution.

## Query Language and CTE Bindings

Named filter bindings fit naturally with canonical ordinals. The public JSON DSL
can allow reusable document-set expressions:

```json
{
  "with": {
    "visible": {
      "doc_id": {
        "ids": ["..."]
      }
    }
  },
  "query": {
    "bool": {
      "must": [
        { "match": { "body": "renewal" } },
        { "ref": "visible" }
      ]
    }
  }
}
```

The planner compiles this into a named internal binding:

```text
with.visible -> ResolvedDocSet
query        -> expression tree that references visible
```

If the shard has ordinal coverage and the binding is large or reused:

```text
visible_bitmap = bitmap(doc_id -> doc_ordinal)
match(body, "renewal") -> scored stream or candidate bitmap
must(match, ref visible) -> intersect with visible_bitmap
```

If `visible` only contains a handful of explicit IDs, the binding can remain as
`doc_keys` or sorted `ordinals` and be applied through a cheaper point-membership
path.

`ref` should point at the compiled binding, not re-expand the original JSON
each time it appears. This lets a single expensive filter be reused across
multiple query branches, vector searches, sparse searches, full-text clauses,
and exclusions.

The same mechanism handles negative filters:

```json
{
  "with": {
    "blocked": {
      "doc_id": {
        "ids": ["a", "b"]
      }
    }
  },
  "query": {
    "bool": {
      "must": [{ "match": { "body": "renewal" } }],
      "must_not": [{ "ref": "blocked" }]
    }
  }
}
```

Public docs can describe these as `with` bindings or named filters. Internally
they behave like CTEs over document sets.

## Index Integration

All index families should converge on `doc_ordinal` as the common document
identity used for filtering and candidate exchange.

### Full-Text

Full-text already has the closest shape: query filters can produce Roaring
bitmaps of numeric document IDs. The long-term change is to make those numeric
IDs map cleanly to canonical shard-local ordinals.

Possible implementation:

```text
segment_doc_id -> doc_ordinal
segment_live_bitmap projected into doc_ordinal space
full_text_filter(query) -> RoaringBitmap(doc_ordinal)
```

Segment-local IDs can still exist for compact postings. The planner boundary
should exchange canonical ordinals.

### Dense Vector

Dense vector metadata currently has vector IDs and doc-key mappings. That is not
the same as a canonical posting ID.

Long term, primary document-level dense vectors should use a stable vector ID
that is independent from physical ordinals. The dense layer must still persist
explicit mappings so ordinal-backed filters can constrain vector search without
making the vector ID itself rebuild-sensitive:

```text
doc_id -> primary_document_vector_id
vector_id -> doc_ordinal
doc_ordinal -> vector_id(s)
```

Dense search should accept:

```text
include: ?ResolvedDocSet
exclude: ?ResolvedDocSet
```

and apply them during candidate generation or immediately after candidate
retrieval, before expensive reranking or projection. Bitmap-backed sets are best
for broad filters and exclusions; sorted ordinal lists are often cheaper for
small explicit filters. Child/chunk/external vectors can keep their own
physical row IDs, and primary document vectors keep a stable vector ID that is
joined to the current ordinal through persisted membership rows.

### Sparse Vector

Sparse postings should store `doc_ordinal` instead of raw document keys:

```text
term / feature -> [(doc_ordinal, weight)]
```

This gives sparse query filters the same document-set boundary as full-text:
small candidate sets can remain as ordinal lists, while broad term filters can
use bitmaps.

### Algebraic

Algebraic path dictionary postings should move from string doc-key rows to
ordinal postings:

```text
path / value / token -> RoaringBitmap(doc_ordinal)
```

During migration, algebraic filters can continue returning `doc_keys` when
ordinal coverage is missing. Once coverage exists, the preferred result is a
`ResolvedDocSet` that dense, sparse, full-text, and graph planning can all
consume. Small and sparse results may remain as sorted ordinals; large dense or
reusable results can promote to `ordinal_bitmap`.

### Graph

Document-backed graph nodes and edges should reference `doc_ordinal` where the
node is a document identity. Graph-native node IDs can remain separate, but any
edge that is used as a document filter should be able to produce a
`ResolvedDocSet`.

## Visibility, Deletes, and Generations

Ordinals should not be renumbered for ordinary deletes or updates. Correctness
should come from visibility metadata:

```text
doc_ordinal -> created_generation
doc_ordinal -> deleted_generation?
read_snapshot -> live_docs_bitmap
```

Query planning applies:

```text
effective_candidates =
  candidate_bitmap
  intersect live_docs_bitmap
  intersect include_bitmap
  subtract exclude_bitmap
```

Physical compaction may reclaim ordinals later, but only behind a generation or
epoch boundary that makes old indexes and cached bitmaps invalid. Normal query
correctness should not depend on immediate ordinal reuse.

## Shards, Ranges, and Restore

`doc_ordinal` is shard-local or range-local. It should not be globally
meaningful without its shard/range namespace.

For distributed planning:

```text
ShardDocSet = {
  shard_id: ShardId,
  docs: ResolvedDocSet,
}
```

If ranges split or merge, the system has two safe options:

1. preserve existing ordinal namespaces until indexes are rebuilt
2. create a new generation and rebuild ordinal mappings for the new ranges

Backup and restore should persist:

- document key codec version
- canonical document identity policy
- doc-ordinal allocation table
- ordinal generation / epoch
- tombstone and visibility metadata

The deterministic `canonical_doc_id` lets restore tooling validate that rebuilt
ordinal mappings still refer to the same logical documents, even if compact
ordinals are reassigned during an explicit rebuild.

## Planner Roadmap

The migration should be incremental. Do not require every index to switch at
once.

### Phase 1: Safe Document Keys

- Implement the binary document key codec described above.
- Centralize document-key construction and parsing.
- Move primary records, derived records, graph records, TTL records, and dense
  metadata off delimiter-based raw doc-key strings.

### Phase 2: Ordinal Allocation

- Add a backend-owned doc-ordinal allocator per shard/range.
- Persist `doc_id -> doc_ordinal` and `doc_ordinal -> doc_id`.
- Add live/tombstone generation metadata.
- Keep existing query behavior unchanged.

### Phase 3: Internal Document Sets

- Add `ResolvedDocSet`.
- Convert request-level filters and exclusions to use `ResolvedDocSet`
  internally.
- Keep `doc_keys` as a compatibility and tiny-set representation.
- Add sorted ordinal-list and ordinal-bitmap representations.
- Add counters for doc-key list, ordinal-list, bitmap fast path, missing ordinal
  coverage, bitmap promotion, and unsupported filter shapes.

### Phase 4: Algebraic Filter Bitmaps

- Teach algebraic filter resolution to return ordinal-backed `ResolvedDocSet`
  values when possible.
- Preserve the existing doc-key list path as fallback.
- Add direct include/exclude `ResolvedDocSet` plumbing into vector search
  requests.

### Phase 5: Dense and Sparse Consumption

- Teach dense vector search to consume include/exclude ordinal lists and
  bitmaps.
- Teach sparse vector search to consume include/exclude ordinal lists and
  bitmaps.
- Avoid mapping every filter hit through string doc IDs on hot paths.

### Phase 6: Full-Text Projection

- Project full-text segment-local doc IDs into canonical ordinals at planner
  boundaries.
- Let full-text filters and query clauses produce reusable ordinal bitmaps.

### Phase 7: Query `with` Bindings

- Add public JSON DSL support for `with` bindings and `{ "ref": "name" }`.
- Compile each binding once per request.
- Cache binding results for the request lifetime.
- Push binding bitmaps into vector, sparse, full-text, algebraic, and graph
  operators.

### Phase 8: Cross-Index Cleanup

- Move algebraic postings to ordinal-backed rows or bitmap blocks.
- Move sparse postings to ordinal-backed rows.
- Add graph-to-document bitmap production for document-backed graph filters.
- Add health/stat fields for ordinal coverage, fallback rates, stale
  generations, and rebuild requirements.

## Implementation Notes

Status as of 2026-05-19:

- The binary component codec and internal key constructors live in
  `src/storage/internal_keys.zig`. Primary document records, embedding
  artifacts, graph edge artifacts, TTL timestamp records, and replay metadata
  use explicit internal namespaces and record kinds instead of raw
  delimiter-suffixed document IDs. The standalone TTL module no longer exposes
  the legacy textual `:t` suffix; its public timestamp helpers now describe the
  structured internal TTL key path directly. Timestamp read/delete behavior and
  delete-time artifact cleanup now have focused coverage for NUL-containing
  document IDs whose raw IDs are adjacent by prefix, so both TTL and artifact
  cleanup scans are tied to the encoded document component boundary rather than
  a textual prefix. DB-level batch/get/scan and identity-resolution coverage
  now exercises the adversarial document-ID set from this document, including
  the empty ID, `0x00`, `0xff`, and delimiter-shaped byte sequences. Derived
  replay batcher dedupe keys now use length-prefixed tuple components for
  sparse `(index, doc)` and graph `(source, target, edge_type)` maps, so
  embedded NUL bytes in user-controlled IDs cannot collapse independent replay
  mutations into one buffered entry. Transaction session read-snapshot maps use
  the same length-prefixed tuple approach for `(table, doc_id)` keys, avoiding
  NUL-delimiter collisions while preserving the public raw document-ID surface.
  Chunk-generation caches in the DB and enrichment worker now also use
  length-prefixed tuple components for document ID, artifact/source identity,
  and chunking configuration, so embedded separator bytes cannot alias cached
  chunks across distinct documents or fields. The same tuple-key discipline now
  applies to request-local and distributed text-stat grouping maps for
  `(index, field)` and `(aggregation, field)` keys, avoiding separator
  collisions in query planning/support statistics before they feed ordinal-aware
  search and aggregation paths. Dense vector mapping metadata now uses a v2
  encoded-component namespace for `(index, record-kind, id/doc)` rows instead of
  interpolating raw index names into textual metadata prefixes, so index names
  containing delimiter-like bytes cannot collide with metadata record kinds or
  adjacent index-name prefixes during ordinal-member scans and index cleanup.
  Lookup, ordinal-member scan, cleanup, and next-vector-ID reservation paths
  still read legacy textual dense metadata rows, so stores written before the
  v2 key shape do not lose stable vector mappings after upgrade.
  Distributed transaction participant IDs now use an ASCII length-prefixed
  internal table-name component plus group ID for newly generated participants,
  while still parsing legacy participant strings, so recovery
  cannot split a table name that happens to contain the legacy group marker.
- `src/storage/db/doc_identity.zig` adds the long-term identity table
  foundation: deterministic `canonical_doc_id`, persisted
  `doc_id -> doc_ordinal`, `doc_ordinal -> doc_id`, next-ordinal allocation,
  and ordinal state with create/delete generations. It can now materialize the
  live ordinal state rows into a `ResolvedDocSet`, establishing the reusable
  snapshot `live_docs_bitmap` primitive; broad query execution still needs to
  thread this live-doc set through every candidate/intersection path.
- Public-ID-to-doc-set resolution now consults ordinal state: tombstoned
  ordinals are omitted, live ordinals remain eligible for ordinal-list/bitmap
  planning, and mixed missing-coverage fallbacks preserve only live or unknown
  document IDs instead of reintroducing known-deleted documents. Explicit
  include/exclude document-ID filters in DB search plumbing now pass the request
  `identity_read_generation` into this resolver as well, so lowered filter IDs
  use the same snapshot boundary as generated algebraic/text/vector filters.
- The identity table now also exposes generation-aware variants of the same
  resolution and filtering primitives. Callers can resolve public IDs or filter
  existing `ResolvedDocSet` values at a specific identity generation using
  `created_generation`/`deleted_generation` instead of only current live state,
  giving the storage layer the MVCC-ready boundary needed before query snapshots
  are threaded through every operator.
- Internal search requests can now carry an optional `identity_read_generation`.
  Dense, sparse, and text document-filter planning thread that generation through
  the resolved-public-ID and live-filter callbacks, so request-local ordinal
  lists/bitmaps are resolved against the intended identity generation instead of
  implicitly reusing the current live view. Requests that omit the generation
  are stamped at the top-level DB search boundary with the current replay
  sequence, making the existing current-live behavior an explicit request-local
  identity snapshot for all downstream planners. DB preflight/planning stats and
  search-request text-stat collection now use the same stamp-or-reject boundary,
  so planning diagnostics cannot report a non-current identity snapshot as
  executable. Until primary storage and every index can serve historical
  snapshots at arbitrary generations, top-level DB search rejects explicit
  non-current identity generations instead of returning a result whose identity
  filters are historical but primary/index candidates are current. The focused
  DOCID gate now names the storage/search regressions that keep this boundary
  intact: default request stamping, explicit doc-ID filter resolution,
  `ResolvedDocSet` projection, `match_all` candidate ordinal lookup, native
  doc-set-to-ID projection, native live filtering, and full-text ordinal
  projection all receive the request identity generation.
- Local table-read orchestration now carries the DB-stamped
  `identity_read_generation` out of the initial search and reuses it for
  same-response aggregation, rerank, and response shaping work. This prevents a
  table query whose public request omitted the generation from letting follow-up
  local operators restamp against a later identity table view. Focused table-read
  coverage now asserts that a provisioned local query execution returns the
  stamped request alongside the search result. Aggregation code that needs a
  second full-result query now also requires that stamped request before rerun,
  so distributed/remote follow-up aggregation work cannot silently issue a fresh
  unstamped search at a different identity-table view. The storage-level
  named-graph execution boundary now enforces the same rule for manually
  supplied input hit IDs: callers must provide an `identity_read_generation`
  instead of relying on the DB wrapper to restamp public document IDs at
  execution time. C API JSON, packed dense, dense-wire, text-wire, graph, and
  aggregate helpers now thread the identity generation used by the read through
  their owned result helpers and response encoders, so returned hit IDs are not
  paired with a separately sampled post-read generation token. The C API search
  boundary also rejects stale explicit generations before invoking readable
  lease hooks, keeping lease coordination behind the same fail-closed identity
  snapshot validation as storage search. The build now exposes a
  dedicated `capi-test` step and includes it in `unit-test`, so these C API
  identity-generation boundaries are covered by direct C API tests rather than
  only by the shared-library compile. The C API gate names graph generation
  propagation, stale search and aggregation rejection, the packed-dense
  no-ordinal-leak regression, and the binary footer generation-token regression.
- Structured full-text filter doc-set caching keys entries by both filter JSON
  and identity read generation, preventing a cached ordinal list/bitmap resolved
  at one generation from being reused for the same JSON at another generation.
  The cache entry shape also carries an optional full identity namespace for any
  future shared/global doc-set cache: request-local entries continue to use the
  request generation, while shared entries must match both namespace and
  generation before reusing a bitmap/list. The focused DOCID gate now includes
  request-local and shared-key cache regressions so bitmap cache invalidation
  remains tied to the stamped identity snapshot.
- Internal shard query forwarding preserves that identity generation as
  `_identity_read_generation`, and algebraic vector-worker envelopes round-trip
  it through request options before lowering back into a `SearchRequest`.
  Algebraic distributed partial-request envelopes also preserve the internal
  generation field. The shard DB boundary accepts omitted/current stamps and
  rejects stale stamps before exporting materialized distributed partials, and
  multi-group algebraic aggregation planning can use the distributed partial
  fast path for current stamped requests instead of falling back only because
  the identity snapshot is explicit. This keeps distributed fanout,
  vector-worker offload, and algebraic aggregation partial collection from
  silently dropping the snapshot boundary once callers start setting it.
  Historical non-current materialized partial reads still require exact
  tombstone subtraction before they can be enabled. The internal vector-worker
  HTTP route lowers envelopes through the same `SearchRequest` execution path,
  so unsupported non-current identity generations fail closed at the DB boundary
  instead of bypassing snapshot validation. Distributed graph expansion requests
  also encode, parse, and lower the same generation into their per-frontier
  `SearchRequest` values, and the internal graph-expand HTTP route maps the
  resulting unsupported snapshot error to a client-visible 400 instead of an
  internal failure. The focused DOCID gate now includes both internal route
  regressions, so vector-worker and graph-expand identity generation failures
  cannot silently turn into unstamped remote execution. The public table-query
  HTTP adapter rejects those internal
  shard fields, including `_identity_read_generation`, the plain
  `identity_read_generation` spelling used inside internal worker envelopes, and
  native doc-id constraint envelopes, by inspecting top-level JSON fields rather
  than raw body substrings. The query-contract parser uses the same structured
  detection before relaxing schema strictness for internal shard fields or
  public `with` bindings, so request-local identity generation remains an
  internal execution boundary rather than a public query contract while public
  query text can still mention those names literally. The embedded public JSON
  search surface, public table-query execution helpers, public join subquery
  lowering, and public retrieval-agent query runners now call the same public
  parser wrapper before storage search, so non-HTTP, joined-query, and agentic
  public callers cannot pass internal shard DOCID controls through the
  lower-level internal parser.
- Distributed table read fanout now rejects non-null internal
  `resolved_doc_filter` pointers before copying a request across more than one
  group, and hosted single-group reads reject the same pointer before forwarding
  to a remote owner. Until cross-shard `ShardDocSet`/namespace lifecycle exists,
  resolved ordinal filters remain shard-local execution state; cross-group and
  remote requests must exchange public document IDs, native document-ID
  constraints, or other generation-stamped serializable filters instead. The
  distributed result merge boundary now also clears `SearchHit.doc_ordinal`
  metadata when it combines more than one shard/range result set, preserving
  local ordinals for single-result merges but preventing range-local ordinals
  from leaking into merged cross-range hit pages.
  Public table-query HTTP also rejects the internal doc-identity reassignment
  control field at the top level, so reassignment stays limited to internal
  transition/admin routes rather than becoming a public query contract. The
  shared query-contract parser also rejects top-level doc-identity control
  fields even when public `with` bindings require relaxed unknown-field parsing,
  preventing non-HTTP call paths from silently ignoring those controls. The same
  rejection runs before the packed dense-query fast parser, so benchmark-shaped
  embedding requests cannot bypass the public identity-control boundary.
  Public query response encoding, C API JSON search encoding, and the packed C
  dense-search wire response have direct coverage that internal
  `SearchHit.doc_ordinal` metadata is omitted from response shapes, keeping
  ordinals as planner-local state even when result pages are ordinal-complete
  internally.
  Serverless published-search planning applies the same top-level guard for
  public and legacy request shapes, rejecting doc-identity generation,
  reassignment, and native-doc-ID constraint fields instead of ignoring them
  while parsing with relaxed public-request schemas. The focused DOCID gate now
  includes the public table/query-contract guards plus the serverless search-plan
  and graph-plan guards, so the public/internal identity-control boundary is
  exercised outside the broad serverless test bucket.
  Serverless table graph-query detection now runs the public graph guard before
  its relaxed OpenAPI parse too, so graph searches cannot bypass the public
  table-query identity-control rejection by being routed ahead of the shared
  table-query adapter. The lower-level serverless graph endpoint parsers for
  neighbors, traverse, and shortest path use the same guard before their relaxed
  request-shape parsing, so graph-specific public endpoints do not silently
  ignore identity controls either.
- Direct ordinal-producing algebraic filters now run their resolved ordinal
  sets through the same live-state filter before returning a `ResolvedDocSet`,
  preventing stale ordinal posting rows from exposing tombstoned documents.
  The direct algebraic filter bridge also accepts the request
  `identity_read_generation`, so explicit ID filters, bool/ref composition, and
  ordinal posting results are filtered at the same identity snapshot used by the
  rest of the search request instead of implicitly resolving against current
  live state.
- Dense and sparse native constraint derivation now also accepts a live-set
  filtering callback, so explicit resolved ordinal/list/bitmap filters are
  intersected with identity-table live state before they become vector IDs or
  sparse doc numbers. The broad `live_docs_bitmap` candidate intersection is
  now wired through the production dense, sparse, and text paths called out
  below, while remaining layout-gated for sparse chunk/generated search and
  other operators that do not yet have an ordinal-native candidate boundary.
- The broad all-docs live-filter primitive now has a coverage-safe
  materialization path. `liveFilteredDocSetFromStoreAlloc(.all)` scans primary
  documents and returns a live ordinal set only when every primary document has
  a live identity mapping; missing ordinal/state coverage, or a tombstoned state
  for a still-present primary record, preserves `.all` so planning does not
  silently narrow. Native constraint derivation can opt into this all-docs
  live filter without marking stored filters as resolved. Resolved all-document
  exclusions now lower to an empty native candidate set for dense, sparse, and
  text constraint derivation instead of relying on later stored-result
  postprocessing to remove every hit. Non-chunk-backed
  dense, sparse, and full-text production search paths now enable this broad
  live-doc candidate filter. Chunk-backed full-text searches also enable it
  when every segment has document-ordinal sidecar coverage, so parent ordinals
  can be projected directly to chunk segment doc numbers. Dense
  chunk/generated/mixed layouts now enable it through the parent-ordinal
  membership expansion rows: the live primary ordinal set is expanded to all
  live child/external vector IDs before HBC search runs. Sparse
  chunk/generated layouts now enable it through a sparse doc-number expansion
  boundary: live parent ordinals resolve to current chunk artifact keys for
  the sparse index's chunk source, then to the sparse index's physical
  `doc_num`s before sparse scoring runs.
- `DB.batch` writes identity metadata in the same committed store batch as
  primary document updates. The first identity-bearing batch also persists the
  identity namespace record, so default and explicitly configured
  table/shard/range namespaces do not require a separate open-time store write.
  Deletes mark ordinal tombstones instead of removing mappings, preserving
  ordinal stability for existing index generations. Local transaction
  resolution now appends the same identity metadata to the committed
  intent-resolution batch: transaction-created primary documents allocate
  ordinals atomically with the commit, and transaction deletes tombstone those
  ordinals instead of leaving diagnostic coverage gaps. Transaction intent
  writes also fail closed at the exhausted ordinal sentinel before pending
  new-document intents are stored. Coverage includes transaction-created
  identity rows surviving reopen even when the store has no ordinary batch
  replay sequence to seed the read-generation watermark. Explicit DB recovery
  of already-committed orphaned intents now uses the same resolution extra-batch
  hook so crash recovery does not finalize primary document writes without the
  matching identity rows. The optional background transaction-recovery runtime
  receives a DB-owned identity hook context too, so runtime cleanup and explicit
  recovery share the same committed-intent identity side effects. Internal
  namespace reassignment refreshes that runtime hook context before future
  recovery work can append identity rows in the new table/shard/range namespace.
  Background TTL cleanup now uses the same committed delete-batch identity
  side effect, so expired-document reclamation tombstones ordinals instead of
  removing primary records while leaving live identity rows behind. Buffered
  batch identity metadata now also tracks same-batch upsert state locally:
  resurrecting a tombstoned document advances the ordinal state's create
  generation, and an upsert/delete pair for the same document in one committed
  batch leaves the ordinal tombstoned instead of accidentally reviving it from
  stale pre-batch state. DB-owned write paths now cache the committed identity
  visibility summary after the primary store commit succeeds, so immediate
  post-load query planning can prove all-live visibility without cold-reading
  the summary key from the primary LSM. Cold/reopened stores still fall back to
  the persisted summary and then full identity stats when no in-process summary
  is available.
- Portable AFB backup/restore now has a dedicated doc-identity KV batch block.
  Export includes every internal identity-table row, and import restores those
  rows verbatim after validating that the block only targets the identity
  namespace. After all identity batches are imported, restore now validates the
  identity table as a whole: doc-to-ordinal and ordinal-to-doc rows must agree,
  every mapped ordinal must have state, state canonical IDs must recompute from
  the restored table/shard/range namespace and raw document ID, and allocated
  ordinals must remain below the persisted next-ordinal watermark. Ordinal
  state is now self-validating as generation history: canonical IDs must be
  nonzero, delete generations cannot precede create generations, and the shared
  `isVisibleAt(generation)` predicate defines the future snapshot boundary
  where a document is visible after creation and hidden at or after deletion.
  This preserves the table/shard/range namespace, next ordinal, allocation
  mappings, and tombstone/generation state instead of requiring restore to infer
  new ordinals from primary documents, while rejecting corrupted restored identity
  metadata before queries can rely on it. Raw logical snapshot restore now runs
  the same identity-table validation after importing the primary store snapshot,
  so both AFB import and snapshot restore share the canonical-ID consistency
  boundary.
- Portable graph edge backup/restore now exports structured graph edge
  artifacts and imports edge batches back into binary graph artifact keys
  instead of reconstructing delimiter-suffixed edge keys. Existing edge-batch
  values from older standalone graph exports are converted into the graph edge
  artifact payload shape during import, while arbitrary byte-valued source,
  target, index, and edge-type components round-trip through the internal key
  codec.
- `src/storage/db/doc_set.zig` adds the internal `ResolvedDocSet` and
  `ResolvedDocFilter` representations with sorted ordinal lists and
  density-aware Roaring bitmap promotion for large dense sets. The module now
  also provides a deep-clone helper for every owned representation so
  request-local planners can cache and reuse document sets without sharing
  mutable bitmap/list ownership.
  This remains a storage-internal module: `storage/db/mod.zig` no longer
  re-exports `doc_set` or `DocOrdinal`, and the DB helper methods that resolve
  public IDs to `ResolvedDocSet` values are private implementation callbacks
  rather than public API surface. The re-exported Zig `SearchRequest` type no
  longer exposes the storage-internal `ResolvedDocFilter` type directly; its
  resolved-filter execution hook is opaque and only cast back inside storage
  query execution. The public API continues to exchange raw document IDs and
  request JSON only.
- DB search plumbing has an internal planner boundary for resolving public
  document IDs into a `ResolvedDocSet` when ordinal coverage is available,
  while preserving the `doc_keys` fallback for unresolved or mixed coverage.
  Direct algebraic filter lowering now also composes explicit request
  `filter_doc_ids`, `exclude_doc_ids`, and any existing resolved filter into
  that `ResolvedDocFilter` before applying algebraic filter/exclusion JSON, so
  mixed explicit-ID-plus-algebraic requests can stay on the ordinal-backed path
  when the representations are compatible instead of forcing an intermediate
  public document-ID set.
  Dense profiled search also treats the internal resolved-filter hook as a
  native document constraint for published-search eligibility, so resolved
  document-set constraints run under the same apply-lock/snapshot path as
  explicit public-ID filters instead of using the lock-free published fast path.
  Result-shaping fallback filters now also understand the internal
  `ResolvedDocFilter` hook: if a resolved ordinal/list filter reaches the
  stored-pattern postprocessor, it is projected through the DB identity callback
  and intersected/excluded before stored JSON loads, preventing this defensive
  postprocessing path from widening back to public-ID-only filtering. Focused
  coverage now asserts that missing or unsupported ordinal-to-public-ID
  projection fails closed with `UnsupportedQueryRequest` before any stored loads
  or hit filtering run.
- `src/storage/db/algebraic/index.zig` now exposes
  `resolvedDocFilterForFilterJsonAlloc`, which converts algebraic filter JSON
  into `ResolvedDocFilter` values backed by identity-table ordinals when the
  referenced documents have ordinal coverage. DB vector/sparse filter bridging
  now uses this boundary directly for resolvable algebraic filter JSON, so
  ordinal-backed filters can flow into vector constraints without first
  materializing public document IDs. The existing public-doc-ID bridge remains
  the compatibility fallback for binding-heavy or mixed-coverage cases that
  cannot be safely combined as document sets yet.
- Algebraic scalar doc-fact postings and adaptive path-dictionary promotion now
  write ordinal posting rows alongside the existing document-key posting rows
  when identity coverage is available. Scalar term/bool filters and
  path-dictionary term filters can satisfy `ResolvedDocSet` planning directly
  from those ordinal rows when ordinal coverage matches the legacy posting
  rows. Configured-field `exists` filters use doc-fact field ordinal rows when
  field-presence coverage is complete. `terms` filters now union exact-term
  ordinal sets when every term has complete ordinal coverage. Promoted
  path-dictionary range filters (`range`, `numeric_range`, `term_range`, and
  `date_range`) and string label scans (`match`, `prefix`, `wildcard`,
  `regexp`, and `fuzzy`) now use the same ordinal rows when the dictionary
  labels resolve with complete ordinal coverage. Configured-field standard
  `range`, `numeric_range`, `term_range`, `date_range`, and `ip_range` filters
  now scan scalar ordinal rows directly when ordinal coverage matches the
  legacy scalar doc-key rows. Configured-field string scans (`match`, `prefix`,
  `wildcard`, `regexp`, and `fuzzy`) use the same direct ordinal scalar scan,
  and promoted path-dictionary IP string filters can resolve matching
  dictionary labels into ordinal rows without materializing public document IDs.
  Path-fact projection
  now also writes ordinal rows, and `geo_bbox`, `geo_distance`, and
  `geo_shape` filters resolve matching geo points from those ordinal rows when
  coverage matches the legacy public-DOCID projection. Unpromoted schemaless
  `path_lookup` rows now have ordinal companions too, so raw path `term`,
  `terms`, `exists`, string scan, IP string scan, and range filters can resolve
  directly to ordinals before adaptive dictionary promotion exists.
  Dictionary-backed path `exists` filters union complete ordinal rows across
  all ready promoted kinds for that path. Mixed coverage falls back through
  document IDs instead of narrowing silently. Fixed algebraic aggregation
  constraints now have the same `ResolvedDocSet` boundary, and range/histogram
  bucket intersections try ordinal membership first at the request's
  `identity_read_generation` before falling back to public-DOCID intersection
  if any bucket input lacks ordinal coverage. Even without fixed constraints,
  range/histogram buckets now attempt an identity-table live filter for their
  candidate doc IDs, preventing stale algebraic postings from counting
  tombstoned documents when ordinal coverage is available. The distributed
  range/date-range, histogram, and terms cardinality-partial fast paths now
  receive the same `identity_read_generation` and drop scalar/path bucket
  entries or range bucket document IDs whose identity state is tombstoned at
  that generation before emitting partials or nested cardinality child inputs.
  Table-read
  aggregation context now carries that identity generation into algebraic
  aggregation planning, so aggregation-side ordinal filters use the same
  snapshot boundary as search execution; omitted generations are stamped from
  the DB's current derived sequence instead of being left as implicit
  current-live lookups, and explicit non-current generations fail closed before
  aggregation planning. The C API JSON search and aggregate-hits entry points
  now accept and forward the same optional identity generation into aggregation
  context as well, using the same current-sequence stamp when omitted and the
  same fail-closed check when provided, preventing that boundary from being
  dropped if those paths grow algebraic aggregation support. C API JSON search
  and search-hit entry points now perform that stamp-or-reject step before
  readable-lease preparation too, so a stale explicit identity generation fails
  closed before any read-consistency side effect is requested.
  Algebraic join fact maintenance now writes ordinal-backed per-document
  reference rows (`docjf_ord`) next to legacy `docjf` rows and ordinal-backed
  join fact rows (`jf_ord`) next to legacy `jf` rows when identity coverage is
  available. Join update/delete cleanup prefers ordinal document references only
  after verifying they exactly match the legacy public-DOCID references, and
  join fold/derived-join scans prefer `jf_ord` prefixes when ordinal-row
  coverage matches the legacy scan prefix. Legacy `jf` rows remain as the
  mixed-coverage compatibility path.
- Vector query requests can now carry a resolved document filter internally.
  Primary document-level dense vectors use stable deterministic vector IDs, with
  persisted doc/vector/ordinal mapping rows kept as the execution boundary for
  resolved ordinal filters, legacy ordinal-keyed vectors, external vectors, and
  multi-vector cases. Namespace reassignment may rewrite deterministic
  `canonical_doc_id` values, but it must preserve `doc_id -> doc_ordinal` and
  the stable vector ID mapping. Any future operation that compacts or reassigns
  ordinals must build a new identity and index generation and rebuild
  ordinal-backed dense, sparse, text, algebraic, and graph artifacts before
  cutover instead of mutating the mapping under existing indexes. Dense
  constraint derivation consumes sorted ordinals and ordinal bitmaps through the
  mapping table; the only ordinal-as-vector-ID branch is named as a legacy
  compatibility path and requires existing HBC metadata to match the same
  document. Native dense constraint derivation now fails closed if a caller
  provides ordinal constraints without an ordinal-to-vector mapper. Dense write
  batching now prefetches metadata for both stable deterministic IDs and legacy
  ordinal-keyed vector IDs, so stores that still contain ordinal vectors do not
  fall back to per-document metadata probes during replay. DB-level coverage now
  verifies that stable dense vector IDs diverge from identity ordinals,
  conflicting ordinal-keyed metadata is not adopted as the vector ID for a
  different document, legacy ordinal prefetch is warmed in the batch path, dense
  artifact rebuild replays document vectors under stable vector IDs instead of
  ordinal IDs, dense hits still report the identity-table ordinal rather than
  the vector ID, and existing ordinal filters remain valid after the internal
  namespace reassignment wrapper rewrites
  canonical document IDs. The unconditional dense-artifact rebuild helper,
  graph-derived rebuild helper, and all-steps restore repair wrapper are kept
  internal to the DB module; provisioned startup, restore repair, and managed
  catch-up callers use the bounded `IfNeeded` or step-wise maintenance paths
  instead of exposing force rebuild as a public query/write contract.
- Chunk-backed and external dense embeddings now persist parent-document
  ordinal membership rows in addition to the physical vector mapping. A single
  parent `doc_ordinal` can expand to all chunk or external vector IDs for that
  document, so resolved ordinal filters constrain multi-vector dense indexes
  without collapsing to only the primary document vector. Bare ordinal-as-vector
  fallback remains only for older primary-vector layouts whose metadata already
  matches the ordinal ID; child/chunk vectors keep a distinct vector-row ID to
  avoid collisions between multiple vectors for one parent ordinal. Production
  dense search now uses the same expansion for broad all-docs live filtering,
  preventing stale child vectors from surfacing after their parent primary
  document has been deleted.
- Sparse postings now have an ordinal-native compatibility path without using
  the identity ordinal as the physical sparse row ID: DB-owned sparse writes let
  the sparse index allocate its own `doc_num`, and resolved ordinal filters map
  through `doc_ordinal -> doc_id -> sparse doc_num` before sparse scoring.
  Sparse native constraint derivation now fails closed if an ordinal-backed
  filter reaches a sparse executor without that ordinal-to-physical-doc-number
  mapper, instead of falling back to ordinal-as-`doc_num` assumptions.
  Sparse chunk encoding tolerates physical doc numbers arriving out of write
  order, and sparse search can consume resolved ordinal include/exclude sets
  directly without projecting those filters through string document IDs.
  Non-chunk sparse search results now resolve `SearchHit.doc_ordinal` from the
  identity table instead of copying the sparse physical `doc_num`, so downstream
  stored-pattern and graph-handoff paths can reuse ordinal-complete sparse hit
  pages without depending on sparse row-number stability. Chunk-backed sparse
  hits intentionally treat sparse doc numbers as chunk artifact rows;
  chunk-backed sparse search expands parent ordinals to current chunk artifact
  sparse doc numbers before scoring, and generated chunk-backed sparse result
  pages recover the parent ordinal from internal chunk artifact keys at the
  request's identity generation before parent grouping.
- Explicit public `filter_doc_ids` and `exclude_doc_ids` are now resolved
  through the identity table before dense/sparse native constraint derivation
  when the executor is operating in ordinal-native mode. Complete identity
  coverage carries those request constraints as ordinal doc numbers; incomplete
  coverage keeps the existing public-ID fallback so correctness does not depend
  on partial ordinal state. Sidecar-covered full-text search now uses the same
  request-local identity resolution for explicit public include/exclude IDs and
  projects the resolved ordinals directly into snapshot-global text `doc_num`
  clauses; legacy or mixed sidecar snapshots still fall back through public IDs.
- Full-text filter planning now has an ordinal projection boundary for vector
  and sparse searches. When `filter_query_json` or `exclusion_query_json` can
  be satisfied by a full-text index, the matching segment/global text doc IDs
  are projected directly through the full-text document-ordinal sidecar into
  `ResolvedDocSet` ordinals when every segment has sidecar coverage, and fall
  back through stored document IDs only for legacy or mixed-coverage snapshots.
  The full-text postings remain segment-local internally; the cross-index
  planner boundary exchanges canonical ordinals. Structured full-text filter
  projection now keeps a request-local `ResolvedDocSet` cache, so identical
  include/exclude filter JSON clauses reuse cloned ordinal sets within one
  dense/sparse constraint derivation instead of re-running the projection
  boundary. Text search now derives the same native document constraints and
  pushes resolved include and exclude document constraints into the underlying
  full-text bool query before scoring. Public-ID constraints are now projected
  once into full-text snapshot-global numeric doc clauses (`doc_num`) when
  possible, avoiding a repeated stored-ID scan inside each bool clause; final
  result shaping still keeps the doc-id/stored-filter postprocess as a
  compatibility backstop.
- Public hybrid vector queries now route both text clauses and structured
  filter/exclusion clauses to the active read-schema full-text index before
  remote or vector-worker execution. The public dense fast parser is disabled
  for requests that carry `full_text_search`, `filter_query`,
  `exclusion_query`, named bindings, or internal doc-filter wire fields, so
  those clauses cannot be silently dropped by a packed benchmark-style parse.
  Default dynamic schema-less string fields now emit the same exact
  `field.keyword` companion as the no-schema mapper, which lets public
  structured term filters resolve through the full-text postings in provisioned
  swarm tables instead of widening vector search. Dense and sparse native
  constraint derivation also intersects `req.full_text` result doc sets before
  vector scoring, so the public hybrid guardrail's text + metadata + exclusion
  shape constrains HBC candidates rather than relying only on result
  postprocessing. The remaining performance issue observed at 100k documents is
  the cost of building the full-text-derived doc set itself; HBC is constrained
  correctly, but full-text projection still executes a broad scored text query
  to collect matching ordinals.
- Full-text segments now carry an optional document-ordinal sidecar section in
  local stored-document order. Text backfill, incremental projection, and split
  rebuild paths populate or preserve the sidecar when identity-table coverage is
  available, and segment merge rewrites the sidecar in live-document order.
  Text query execution can project `ResolvedDocFilter` ordinal lists/bitmaps
  directly into snapshot-global full-text `doc_num` constraints when every
  nonempty segment has sidecar coverage. The same sidecar-aware projection is
  used for internally collected structured full-text filter doc sets, so
  `filter_query_json` and `exclusion_query_json` that resolve to ordinal-backed
  `ResolvedDocSet` values can avoid public-ID projection inside text search. If
  any segment lacks coverage, or a mixed filter still contains public document
  keys, query planning falls back through the existing public-ID projection path
  instead of widening the filter; mixed-version full-text snapshots with both
  sidecar and legacy segments have explicit coverage for this fallback. The
  focused DOCID gate now includes the mixed-sidecar fallback and unresolved
  ordinal projection fail-closed regressions, along with dense/vector native
  constraint fail-closed coverage when ordinal constraints cannot be mapped to
  physical vector IDs.
  Sidecar-covered chunk text search also uses this projection for broad
  live-doc filtering; non-sidecar chunk text layouts continue to preserve the
  compatibility path.
  The focused DOCID gate now includes the lower-level segment sidecar
  round-trip/merge regression, and forced full-text compaction has DB-level
  coverage that a merged segment keeps the ordinal sidecar in live-document
  order: a resolved ordinal filter continues to match the same document after
  compaction and after reopening the database.
- Public query JSON now accepts a top-level `with` object for named document
  filters and treats `{ "ref": "name" }` as a structured filter clause. The
  query parser normalizes each public binding once into
  `SearchRequest.doc_filter_bindings`; named algebraic filter JSON bindings are
  resolved once per request, later ref filter clauses reuse the compiled set,
  and the result is projected into the same `ResolvedDocFilter` path consumed by
  dense and sparse search. Full-text searches that use named bindings now run
  the same algebraic binding resolution before text execution and carry the
  resolved public document IDs into result shaping, so include/exclude doc-id
  constraints are enforced without requiring stored-document pattern loads.
  Full-text searches with `require_algebraic_filter_resolution` also enter this
  resolver for plain top-level filter/exclusion JSON, so fail-closed algebraic
  filter resolution is no longer limited to named binding forms. Algebraic
  schema lifecycle state now participates in the same fail-closed boundary:
  planners reject rebuild-required capabilities, adaptive materializations are
  marked `rebuild_required` when schema/capability fingerprints drift, and DB
  vector/sparse symbolic filters fail closed instead of reusing stale algebraic
  artifacts. The focused DOCID gate names those planner, adaptive-progress, and
  DB symbolic-filter regressions to cover rolling-upgrade mixed-index states.
- Named graph query planning now resolves document-backed graph hits into a
  request-local `ResolvedDocSet` alongside the public hit list. Graph result
  dependencies still expose public document IDs for compatibility, but the
  graph execution boundary now materializes the canonical ordinal/list/bitmap
  representation needed for graph-backed document filters and later bitmap
  reuse without adding bitmap internals to public graph results. Graph result
  doc-set materialization now receives the enclosing request's
  `identity_read_generation`, keeping dependent graph filters on the same
  request-local identity snapshot boundary as vector, sparse, text, and
  algebraic filters. Manually supplied named graph input sets now also resolve
  their public hit IDs into request-local `ResolvedDocSet` values before graph
  execution, and their materialized hit lists now carry visible
  `doc_ordinal` values when the supplied IDs are visible at the stamped
  generation. DB graph query results now annotate produced search hits with the
  same request-local visible ordinals before dependent graph queries resolve
  those hits into doc sets, so chained graph result sets can avoid public-ID
  re-resolution when the graph hit page is ordinal-complete. Dense result pages
  that feed graph `result_ref` execution now perform the same request-stamped
  live ordinal lookup even when there is no explicit resolved filter, keeping
  `$embeddings_results` graph handoff ordinal-complete for primary dense hits.
  Dense hit pages now also annotate request-generation ordinals when native
  public doc-ID constraints or stored-pattern filter/exclusion JSON requires a
  post-search filtering boundary, letting result shaping consume the same
  ordinal-complete page instead of re-resolving dense hits through public IDs.
  Standalone DB/C API graph execution now snapshots or rejects stale
  `identity_read_generation` values before lease preparation or named-input-set
  resolution, so unbounded `result_ref` selectors use the same complete-set
  guard and ordinal projection path instead of remaining a public-ID-only
  handoff. C API aggregate-hit
  requests also validate explicit
  `identity_read_generation` values before aggregation request materialization
  or stored hit loading, so stale follow-up aggregation requests cannot run
  against a later identity snapshot. C API JSON search responses now return the
  stamped `identity_read_generation`, giving follow-up aggregate-hit requests a
  concrete snapshot token to echo instead of restamping against a later identity
  table view. The lower-level C API search-hit result struct carries the same
  stamped generation for `antfly_db_search_hits_json` and direct text-match
  searches, packed dense C API results carry it as well, and the binary packed
  search wire response appends it as an 8-byte footer after the ID blob.
  Standalone C API graph execution also includes the stamped generation on
  graph result objects, so callers that seed later graph or aggregate work from
  returned hits have the same snapshot token available. Graph execution named
  input sets and aggregate-hit requests with explicit hit IDs now require that
  token instead of silently restamping to the current identity table view.
  Distributed join workers now apply the same rule to group-local follow-up
  pagination and full-result reruns: the first page may establish the result
  shape, but any additional page request must carry an
  `identity_read_generation` so join workers do not restamp page two against a
  later identity-table view.
- Composed search named result sets now use the same internal materialization
  boundary before graph attachment. Full-text, dense, sparse, fused, and
  `$embeddings_results` alias sets still carry public hit IDs for compatibility,
  but they also retain request-local `ResolvedDocSet` pointers when identity
  coverage is available. When every materialized hit carries `doc_ordinal`,
  graph handoff now builds that set directly from hit ordinals instead of
  re-resolving the hit page through public document IDs; mixed pages keep the
  previous identity-table fallback. This prevents composed-search graph handoff
  from being a public-ID-only internal exchange point. Unbounded graph `result_ref`
  selectors now fail closed when an available resolved doc-set cannot be
  projected to graph document keys, instead of silently narrowing to the
  materialized public hit page. They also fail closed when the referenced named
  result reports more total hits than it materialized, preventing page-sized
  full-text, embeddings, or fused result sets from being treated as complete
  document sets. The same total-hit guard is now applied to non-composed base
  search graph attachments; `$full_text_results`, `$fused_results`, and
  `$embeddings_results` carry the base result's real `total_hits`, not just the
  materialized hit-page length, and a saturated base result page is treated as
  potentially incomplete even when post-filtering can only report the visible
  materialized total. Distributed cross-range graph expansion applies the same
  fail-closed rule before resolving unbounded base or prior graph result refs
  into frontier keys, so fanout traversal does not exchange a page-sized public
  DOCID list as though it were a complete document set. Cross-range graph
  `result_ref` resolution now also requires a stamped
  `identity_read_generation`, including limited refs, so page/rank semantics
  still carry an explicit snapshot token instead of silently reusing public hit
  IDs from an unstamped request. Multi-group table-read routing applies that
  guard before running the base shard fanout, and multi-group preflight applies
  the same guard before shard preflight fanout, preventing unstamped graph refs
  from doing cross-shard work only to fail later or fall through to generic
  public-ID fanout. The exported cross-range graph executor now enforces the
  same guard directly, so future callers cannot bypass the table-read wrapper
  and run result-ref fanout without an explicit identity snapshot. Distributed
  graph hydration now carries the same identity
  generation to group-local hydrate requests. Local hydrate responses attach
  request-generation `doc_ordinal` metadata when available, while the
  coordinator clears those ordinals again when a hydrate result combines more
  than one range group, preventing range-local ordinals from escaping as a
  merged cross-range identity. Distributed graph edge reads now carry the same
  identity generation during pattern traversal and validate it at the
  group-local edge-read DB boundary, so edge scans cannot bypass stale
  generation rejection while expand/hydrate paths remain stamped. Cross-range
  graph execution also checks catalog
  merged-group doc-identity health for every current table range before fanout.
  That distributed fanout guard now requires runtime status for each
  participating range instead of treating absent telemetry as ready, and fails
  closed on namespace-conflict, `rebuild_required`, or a runtime
  table/shard/range namespace that no longer matches the catalog range. That
  keeps cross-shard graph expansion from exchanging ordinal-derived result sets
  while any participating range is known to need identity repair or reassignment.
  Cross-range readiness now also treats an active doc-identity reassignment
  signal as not ready, so graph fanout does not exchange ordinal-derived
  document sets while a merge transition is in the middle of rewriting the
  receiver identity namespace.
  The public graph selector helper and serverless public graph adapter now use
  the same guard for public result-set resolvers and can mark saturated search
  pages as incomplete when seeding graph result refs. Limited selectors continue
  to use hit-order materialization because the limit is explicitly rank/page
  semantics.
- The standalone KV graph index no longer uses `DocStore.KeyEncoder`'s
  colon-delimited `:i:`, `:out:`, and `:in:` edge keys for its main and
  reverse edge stores. It now writes structured internal user keys with encoded
  document, index, edge-type, and target components under a distinct
  `graph_index` artifact type, preserving DB-owned graph edge artifact keys and
  their codec values under the existing `graph` artifact namespace. Reverse
  rebuild, split-range pruning, stats scans, and incoming/outgoing edge scans
  use the same structured parser, with direct coverage for document IDs and
  edge types containing delimiter-like bytes, `0x00`, and `0xff`.
- Query-builder graph metadata validation now routes graph JSON through the
  executor parser boundary consistently, and graph-query preflight cleanup
  releases partially built graph query names when parsing fails. This keeps the
  public graph request validation path aligned with the structured graph
  executor while avoiding leaked request-local graph query state on unsupported
  graph shapes.
- `DBStats.doc_identity` exposes the first ordinal-health counters:
  the persisted identity namespace (`table_id`, `shard_id`, `range_id`),
  allocated ordinal count, next ordinal, state-row count, live/tombstoned
  ordinal counts, and diagnostic primary-document coverage gaps. Normal status
  uses bounded next-ordinal metadata; diagnostic stats perform the full
  identity-state scan plus an exact primary-document coverage scan that flags
  missing doc-ordinal mappings, missing ordinal state, and live primary records
  whose ordinal state is still tombstoned. The same stats now expose remaining
  `u32` ordinal capacity and an exhaustion flag, matching the fail-closed
  allocator boundary so large shards can be split or rebuilt before ordinal
  allocation reaches the reserved limit. The identity allocator now has focused
  coverage at the capacity boundary: a batch that would cross the reserved
  sentinel fails with `DocOrdinalExhausted` without committing partial identity
  rows, the final allocatable ordinal can still be assigned, and subsequent
  allocations fail closed once `next_ordinal` reaches the sentinel. The focused
  DOCID gate now names that allocator boundary plus DB stats coverage that a
  store at the sentinel reports zero remaining ordinal capacity, sets both
  `ordinal_capacity_exhausted` and `rebuild_required`, and rejects the next
  document write with
  `DocOrdinalExhausted`. New document writes now check the exhausted ordinal
  sentinel before consistency-specific derived/index wait work, while existing
  document updates that already have ordinals can still proceed. Transaction
  intent writes use the same guard under the DB apply lock before storing
  pending intents, so exhausted shards do not accept new-document transaction
  writes that would later be unable to allocate identity rows at commit.
  Metadata merge planning and public merge-intent validation now also treat
  exhausted ordinal capacity as a DOCID readiness failure, including explicit
  merge-reassignment opt-ins. Split remains the preferred operational response
  for large ranges approaching the `u32` boundary; merge is blocked because it
  would combine exhausted or near-exhausted identity domains.
  Focused coverage exercises that
  fail-closed behavior for `propose`, `write`, `full_text`, `enrichments`,
  `aknn`, and `full_index` sync levels plus direct and request-shaped
  transaction intent writes. The focused DOCID gate now includes both the
  all-sync-level batch exhaustion regression and the transaction-intent
  exhaustion regression. Diagnostic scans also publish min/max
  create and delete generations across ordinal state rows, giving operators a
  concrete stale-generation window for tombstone retention and eventual ordinal
  compaction planning. The same stats derive a `rebuild_required` flag from
  ordinal capacity exhaustion and diagnostic primary-document coverage gaps,
  including primary documents with missing ordinal rows and primary documents
  whose ordinal state is still tombstoned. The focused DOCID gate names that
  diagnostic coverage/tombstone regression, giving operators a single remediation
  signal without treating bounded fast stats as corruption. Stale explicit
  identity-generation requests now increment
  a document-set planning rejection counter at the shared DB snapshot validation
  boundary used by DB search, table-read aggregation helpers, and the C API, so
  unsupported historical snapshot traffic is visible as runtime telemetry rather
  than only as request errors. Runtime-status snapshot cloning now preserves both
  identity stats and document-set planning counters, and the status-preservation
  predicate treats those counters as runtime facts, so distributed/local status
  consumers do not lose namespace, rebuild, or fallback telemetry to synthetic
  placeholder refreshes. Local runtime status now carries the same identity and
  document-set planning telemetry through metadata runtime reports, metadata
  HTTP parsing, raft/store runtime-status record version 4, and remote
  local-status reconstruction, so distributed status views keep the DOCID
  health counters after crossing metadata boundaries.
- `DBStats.doc_set_planning` exposes the first document-set representation
  counters: resolved set count, `doc_keys` fallback count and document total,
  ordinal-list count and document total, ordinal-bitmap count and document
  total, missing ordinal coverage count, bitmap promotion count, unsupported
  filter-shape count, and stale identity-generation rejection count. These
  counters make the Phase 3 fallback/promotion behavior and the Phase 8
  stale-generation boundary visible before every operator is fully
  ordinal-native. The focused DOCID gate now includes the ordinal-bitmap
  promotion regression, so the large-set representation and promotion counter
  remain covered alongside the query execution boundaries. `zig build
  docid-doc-set-bench` now provides a repeatable ReleaseFast benchmark for raw
  sorted `u32` ordinal arrays, direct roaring bitmaps, the current compact
  ordinal-list/bitmap document-set operators, sorted sparse `u64` IDs, and
  public DOCID-key baselines across small, medium, large, dense, and sparse
  layouts. `zig build docid-write-bench` measures insert, update, and delete
  phases across write consistency levels and reports the isolated
  extraction, artifact-cleanup, identity-capacity, identity-metadata,
  derived-payload, and store-write timings from `BatchProfile` alongside
  resulting identity-table stats. `zig build docid-query-bench` now benchmarks
  direct DB query shapes that exercise the real filter bridges: match-all with a
  doc filter, full-text with a doc filter, and sparse-vector search with a doc
  filter. Each shape runs `public_ids` mode, where public document IDs are
  resolved on every query, and `ordinal_docset` mode, where the benchmark
  pre-resolves the request-local ShardDocSet-style ordinal filter at a stamped
  identity generation. Output includes checksums, hit counts, elapsed/average
  nanoseconds, per-shape `docid_query_bench_summary` rows, and doc-set planning
  counter deltas so correctness and projection work are visible together. The
  benchmark now fails correctness mismatches by default; `--allow-mismatch`
  keeps exploratory runs non-fatal, `--max-ordinal-ratio <ratio>` turns the
  ordinal/public timing comparison into an optional performance guard, and
  `--require-public-resolution-delta` asserts that the public-ID path still
  exercises request-time doc-set resolution while the pre-resolved ordinal path
  does not. It also emits a separate `sparse_id_projection` proxy for sorted
  sparse-ID intersection cost instead of pretending sparse native IDs are a
  public DB search mode. A local smoke sample (`docs=1024`, `queries=8`,
  `repeats=4`, `filter_size=128`) matched public-ID checksums for every real
  query shape, avoided 64 per-query doc-set resolutions per shape in ordinal
  mode, and showed ordinal-mode DB time roughly 8-12% lower on that small
  single-process run. A smaller guarded smoke
  (`docs=128`, `queries=3`, `repeats=2`, `filter_size=16`,
  `--max-ordinal-ratio 2.0`, `--require-public-resolution-delta`) passed with
  matching checksums/hit counts, public `resolved_set_delta=12`, ordinal
  `resolved_set_delta=0`, and ordinal/public ratios around 0.84-0.88 across the
  real DB shapes. These benchmarks are now the first pass/fail evidence hooks
  for validating whether the compact ordinal machinery is still earning its
  complexity as sparse-ID alternatives evolve. `scripts/run_docid_query_matrix.sh`
  wraps that benchmark into timestamped evidence runs under
  `bench/results/docid-query-matrix/`, preserving `environment.txt`,
  `commands.txt`, `status.tsv`, per-case stdout/stderr/JSONL, a combined
  `docid-query-matrix-combined.jsonl`, and a summary-only
  `docid-query-matrix-summary.jsonl`. Set `DOCID_QUERY_MATRIX_SMOKE=1` for a
  fast local matrix; the default non-smoke matrix is a bounded developer
  evidence run, and larger release-scale runs should override the
  `DOCID_QUERY_MATRIX_*` case sizes and `DOCID_QUERY_MATRIX_MAX_ORDINAL_RATIO`.
  The scripted cases cover the existing medium baseline, a selective
  small-filter shape, and a broad large-filter shape so future evidence is not
  limited to one favorable filter size. A local smoke matrix passed all three
  cases and produced 9 summary rows: the tiny and selective cases stayed below
  the `1.25` ordinal/public ratio guard, while the broad-filter case showed
  stronger benefit from skipping per-query public-ID resolution (`0.67`, `0.67`,
  and `0.81` ratios for match-all, full-text, and sparse search). A bounded
  default matrix run (`1024`/`2048` docs across the three cases) completed in
  roughly three minutes, matched correctness for all 9 shape/case summaries, and
  produced ordinal/public ratios of about `0.89-0.92` for the medium baseline,
  `0.96-0.98` for selective small filters, and `0.68-0.78` for broad filters.
  An attempted 8k release-scale selective run was intentionally left as an
  override-only profile because that single case ran past 10 minutes locally.
  A direct 100k-doc attempt exposed a degenerate setup path: with per-batch
  `full_index`, only 5k loaded after about 101s and the cost was degrading; with
  `--defer-full-index-load` but giant 10k write batches, the first 10k docs
  still took about 156s before the final full-index wait. The healthier setup is
  deferred indexing with smaller write batches: a 10k-doc run with 1k batches
  loaded in about 13s, then spent about 86s in the one-time `full_index` wait and
  about 17s preparing the resolved filter. The resulting single-query
  ordinal/public ratios were about `0.94`, `0.89`, and `0.96` for match-all,
  full-text, and sparse search. Follow-up profiling with
  `ANTFLY_BENCH_METRICS=1` showed where that time is going: the 10k deferred
  write/load stage spent about 13.6s of 14.0s in primary `store_write_ns`; the
  one-time index wait spent about 4.6s applying full-text and about 89.6s
  applying sparse-vector replay, with about 1.1s of replay-window collection per
  index. The benchmark therefore now has
  `--progress-every <docs>` and `--defer-full-index-load` to make large-load
  setup measurable, but 100k real full-text+sparse query evidence should use
  deferred indexing with bounded batches or a dedicated bulk-load path rather
  than repeated per-batch full-index barriers.
  Sparse-vector replay now has an internal bulk append path wired through
  backend batch options and resource-manager accounting. The path preserves the
  existing sparse on-disk layout but groups postings by term, writes complete
  chunks once, uses larger bulk replay batches, and lets replay/backfill callers
  skip per-doc existence probes when they have already applied deletes or are
  building a fresh index. A bounded 10k-doc DOCID query profile after the change
  still shows primary store load around 13-15s and full-text apply around
  4-4.5s; sparse apply improved from the original ~89.6s profile to roughly
  28-32s locally. That is a meaningful reduction, but not enough to call sparse
  loading solved. The remaining sparse load cost appears to be backend
  write/flush dominated rather than DOCID identity work, so future large-scale
  evidence should profile sparse backend batch commit/flush directly before
  using 100k full DB runs as a pass/fail signal.
  Follow-up sparse write profiling now emits `antfly_bench_sparse_write` rows.
  On the same 10k deferred-index run, sparse apply remained about `28-30s`.
  The profile attributed roughly `12-14s` to forward/reverse sparse row writes,
  about `5.2s` to commit, and only a few hundred milliseconds to grouped
  posting/chunk/meta writes. Sorted sparse artifact reads and a non-namespaced
  erased-batch `appendPut` hook did not materially improve this shape because
  the current LSM batch path still pays per-entry active-memtable mutation cost.
  A direct bulk-state append path now keeps append-only sparse rows in a
  transaction-local sorted-state buffer instead of mutating the active memtable
  per row, with arena-backed entry allocation and safe fallback copying when
  arena-owned entries must move into the normal mutable table. Sparse
  `fwd:`/`rev:`/`inv:` rows now use that append path during bulk replay. On the
  same 10k deferred-index profile, this removed most forward/reverse row-append
  time (`fwd_rev_put_ms` dropped to about `1.6s`) and fixed the earlier
  arena-fallback lifetime crash; however, sparse apply still measured about
  `26.4s` because commit-time WAL/table materialization grew to about `14.2s`.
  Sparse layout v2 now makes that breaking change while the feature is still
  undeployed: sparse keys use typed binary prefixes, bulk replay stores inverted
  postings in a compact segment blob, and bulk doc maps are stored in a compact
  doc-map segment instead of per-document `fwd:`/`rev:` rows. The old chunk-row
  shape remains only as the small incremental delta path, and search reads both
  segment blobs and delta chunks. On the same 10k deferred-index profile,
  sparse commit dropped from about `13-14s` to about `16ms`, sparse apply
  dropped to about `11.5s`, filter preparation dropped from about `13-14s` to
  about `68ms`, and the sparse query shape dropped from about `10.1s` to about
  `3.1s`. The remaining sparse apply time is no longer generic LSM commit; it is
  now dominated by replay artifact decode/grouping and doc-map/postings segment
  encoding. The next lever is to avoid materializing the whole sparse replay
  batch before segment encode, or to stream segment construction directly from
  replay artifacts under backend-runtime/resource-manager budgets.
  Sparse deferred replay now also has a prepared `SparseWrite` path for
  field-backed sparse indexes: replay reads borrowed document bytes with a
  sorted batched store read, extracts sparse vectors directly, and hands the
  prepared writes to the bulk sparse loader instead of first materializing
  `BatchWrite` document-value copies and then reparsing them inside
  `IndexManager`. On the same 10k deferred-index profile, sparse indexing
  itself dropped from roughly `1.4s` to about `67ms`, and total sparse apply
  dropped from roughly `2.7-2.8s` to about `2.1s`. The remaining measured cost
  is now mostly sparse extraction from JSON (`~1.34s`) plus replay document-key
  scan/key construction (`~0.67s`). That points to the next breaking-change
  lever if we need more: persist field-backed sparse vectors, or a compact
  sparse replay artifact, at write time so catch-up does not have to reread and
  reparse full JSON documents.
  Field-backed dense and sparse vectors remain ordinary stored document fields.
  Configured vector indexes still extract those fields during document replay and
  indexing, but they do not turn `field: []` payloads into embedding artifacts or
  strip them from persisted JSON. Explicit precomputed vectors use `_embeddings`,
  which is the artifact upload boundary alongside generated `_chunks` and
  `_edges`; Zig intentionally still rejects `_summaries`. This keeps benchmark
  field-backed vector cases measuring JSON document extraction plus index apply,
  while `_embeddings` cases measure artifact write/read/replay behavior. The
  earlier local experiment that promoted field-backed vectors into artifacts was
  reverted because it changed document round-tripping semantics.
  Sparse embedding artifacts still use a planar compact payload
  (`count | indices[] | values[]`) instead of interleaved `(index,value)` pairs.
  That keeps explicit/generated artifact payloads binary and lets replay borrow
  validated `[]const u32` and `[]const f32` slices directly from batched artifact
  reads when alignment and endian constraints allow it, falling back to allocated
  decode otherwise. Replay keeps the artifact read transaction open while the
  sparse bulk loader consumes borrowed slices. Artifact-key parser and borrowed
  decode optimizations apply to `_embeddings` and derived chunk embeddings; the
  field-backed sparse DOCID benchmark remains on the document-field replay path.
  Follow-up write-path work added `--bulk-load` to the DOCID query benchmark,
  routed benchmark loads through an internal primary-store bulk session, added
  append-oriented docstore bulk batches, batched text-projection ordinal lookup,
  and added a schema-less raw-text projection fast path for documents that do
  not require JSON string unescaping or stored-vector sanitization. Those
  changes did not materially improve the pathological local DOCID setup: a
  10k deferred/full-text+sparse run with `--bulk-load` still spent about
  `86.9s` loading, including about `69.4s` in primary `store_write_ns`, while
  sparse replay stayed around `140ms`. Public swarm guardrail comparisons show
  that this is not representative of the normal public write path: dense 100k
  swarm loading took about `32.4s` to insert and `67.3s` through index
  visibility, and schema-less hybrid 10k swarm loading took about `2.1s` to
  insert and `8.4s` through index visibility. The next DOCID profiling step is
  therefore primary-store/direct-ingest instrumentation for the local benchmark
  path: record whether direct bulk append is used or why it falls back, split
  WAL/sort/table-ingest/mutable-put timings, and compare record counts per
  document against the public guardrail path before treating 100k local DOCID
  profiles as product evidence.
  That instrumentation exposed the local-path bug: append-only primary records
  could direct-ingest, but small non-append metadata records stayed in the
  mutable table and blocked later append batches, forcing the next batch down
  the mutable flush path. The LSM bulk path now drains pending mutable bulk
  records into sorted ingest before declaring append direct-ingest ineligible.
  On a 1k deferred bulk profile this removed append fallback completely and cut
  `store_write_ns` from roughly `488ms` to `48ms`; on the 10k profile, load
  dropped from about `86.9s` to about `22.0s`. The new primary-LSM summary shows
  no flushes, two successful append direct-ingests, about `70k` direct-ingested
  primary entries, and about `7.7s` in sorted ingest/table construction. The
  remaining local DOCID setup cost is now split across extraction (`~4.6s`),
  identity metadata (`~3.2s`), derived artifact construction (`~5.0s`), and
  primary sorted ingest (`~7.7s`), with deferred index wait around `6.3s`
  (`~2.6s` full-text apply, `~78ms` sparse apply, and replay-window collection).
  Follow-up local bulk-load work made the in-memory LSM direct-ingest path take
  ownership of sorted arena-backed states instead of rebuilding table data, kept
  write-only deferred loads on thin replay records even without index workers,
  batched full-text ordinal lookup, and let full-text replay index borrowed store
  values while the read transaction is open instead of materializing a second
  owned write batch. On the same 10k deferred/full-text+sparse `--bulk-load`
  profile, total load dropped to about `10.8s`, `store_write_ns` stayed around
  `0.8s`, primary sorted ingest dropped to about `4ms`, and derived replay
  construction dropped to about `1.0s`. Deferred index wait is now about `6.1s`;
  sparse replay remains about `80ms`, and full-text apply is about `2.1s`, with
  `ANTFLY_BENCH_METRICS` showing the text indexer itself spends about `41ms`
  building the segment and about `705ms` inserting it. The remaining full-text
  wait is mostly replay-window collection plus document collection/read
  overhead, not sparse replay or segment construction.
  Follow-up write-path cleanup now removes more per-document overhead from the
  same measured phases: primary document keys and identity doc-to-ordinal keys
  are allocated at exact size instead of going through temporary array-list
  builders, all-new identity batches pre-reserve their KV write capacity, and
  full-text replay ordinal lookups use the replay arena for transient lookup
  keys instead of per-document long-lived allocator/free cycles. These are
  mechanical hot-path reductions; the last local end-to-end timing run was
  discarded because unrelated desktop load made multiple already-optimized
  phases regress together.
  The first optimization from that evidence specialized
  `ResolvedDocSet` ordinal set algebra: list/list operators now use direct
  sorted-array merge/intersection/difference, bitmap/bitmap operators use
  roaring `orWith`/`andWith`/`andNotWith`, and list/bitmap operators avoid
  flattening a bitmap unless the result representation requires it. This keeps
  the public empty/small/large representation contract while removing the
  previous flatten/sort/rebuild cost from the hot set-algebra path. Path-fact geo
  predicates and unpromoted schemaless path lookup filters now have the same
  guarded ordinal projection, so vector/sparse filter bridging can consume
  complete path and geo matches as document ordinals without first
  materializing public DOCIDs. The write benchmark also exposed that
  document-only deletes were dominated by per-document enrichment-artifact
  prefix scans rather than identity metadata; the storage path now persists an
  artifact-presence marker and keeps a conservative in-memory flag so fresh
  document-only stores skip those scans, while generated-enrichment targets and
  upgraded stores without the marker continue to take the safe cleanup path.
  Public query guardrail profiling then exposed a schema-less exact-filter
  gap: term/terms filters on ordinary string fields could not safely use the
  analyzed text postings, so hybrid vector queries fell back to widening
  through stored documents. Schema-less text extraction now also emits a
  bounded keyword companion using the Elasticsearch-style `.keyword` subfield,
  and structured term filters rewrite to that companion only when the target
  text snapshot actually contains the required postings. Explicit
  `search_as_you_type` schema derivation now follows Elasticsearch-style
  subfield names as well: `field._2gram`, `field._3gram`, and
  `field._index_prefix`, with `._index_prefix` carrying the edge-ngram prefix
  analyzer. Focused storage tests cover schema-less keyword projection,
  schema serialization, explicit/dynamic search-as-you-type variants, and the
  vector/filter ordinal bridge; 1k public query guardrail runs with and without
  schema both kept dense raw hits constrained at `20` rather than widening to
  the full document set.
- The identity table now has a persisted table/shard/range namespace record.
  Existing single-store callers use the compatibility namespace (`0/0/0`),
  while Zig `OpenOptions.identity_namespace` can seed and validate a
  non-default namespace. Canonical document IDs now hash the full namespace,
  including `range_id`, and batch writes fail closed if the stored namespace
  disagrees with the identity namespace used for canonical ID allocation. The
  namespace decoder remains compatible with earlier two-component
  table/shard-only namespace rows by treating the missing range as `0`. Split
  prepare/finalize now preserves the existing identity-table namespace on both
  parent and child shards by copying/restoring identity metadata around
  page-split and streaming split paths. This implements the safe split option
  of preserving ordinal namespaces until indexes are rebuilt. Managed
  provisioned table group opens now derive a fresh-store identity namespace from
  catalog metadata before batch writes, startup catch-up, schema/index
  reconciliation, backup, and local transaction paths allocate ordinals. Those
  write opens now also validate the opened DB's persisted namespace against the
  catalog range namespace before foreground batch, replicated group-local,
  schema-update, cached hosted, sync, and local transaction write boundaries;
  stale existing/cached handles fail closed with `DocIdentityNamespaceMismatch`
  instead of appending documents or transaction intents under the wrong range.
  The public batch facade maps that guard to a 503 identity-unavailable
  response, while internal group-write and shard-operation HTTP routes return
  409 so distributed writers and transition coordinators can treat the stale
  local range as a routing/topology conflict.
  Public table-query HTTP now uses the same identity-unavailable response for
  stale read-side doc-identity namespace failures instead of collapsing them
  into a generic query failure, keeping public behavior fail-closed and
  distinguishable while the operator repairs range identity telemetry.
  Range metadata carries a stable `range_id`; older or zero-valued records fall
  back to `group_id`, while provisioned stores use the catalog document-identity
  domain when it is present. Split finalization preserves the source identity
  namespace on both physical ranges; merge finalization keeps the receiver
  identity namespace until an explicit rebuild or reassignment flow exists. The
  local split transition runtime now derives the destination DB identity
  namespace from the projected destination range when it is visible, otherwise
  from the source range's identity domain before opening the destination DB.
  Fresh split handoff rows therefore preserve source-range ordinals instead of
  allocating a destination-range namespace or falling back to the default
  compatibility namespace. The metadata simulation split runtime now mirrors
  that behavior by preserving source runtime identity telemetry for the
  destination DB, so transition simulation tests do not hide namespace bugs.
  Existing or restored stores keep their persisted namespace through
  `prefer_existing_identity_namespace` instead of rewriting canonical IDs under
  a new namespace. Routing topology epochs
  intentionally remain based on routing shape (`table_id`, `group_id`, and key
  bounds), not `range_id`, because range identity changes do not by themselves
  change request routing. The local identity table now has a rebuild/reassignment primitive
  that validates the current identity rows, rewrites the persisted namespace and
  every ordinal state's `canonical_doc_id` for a new table/shard/range
  namespace, preserves existing ordinals and create/delete generations, and
  validates the result. The DB-local wrapper is intentionally internal: it runs
  the primitive under the DB apply lock and updates the in-memory namespace,
  but it is not exposed through the public query/API surface, and status-only
  DB handles reject reassignment so diagnostic opens remain observe-only. The
  local merge coordinator now has the first transition-scoped caller: receiver
  namespace reassignment is callable only after the persisted
  `allow_doc_identity_reassignment` opt-in is recorded, and focused coverage
  verifies that the operation rewrites the receiver namespace and canonical IDs
  while preserving existing ordinals. `MergeConfig` can now carry a target
  receiver reassignment namespace, so the existing transition-runtime
  opt-in callback records the lifecycle decision and applies the configured
  receiver namespace in the same local merge runtime path. When a target
  receiver namespace is configured, merge progress now fails closed until that
  opt-in has been recorded, preventing accept/bootstrap/catch-up/finalize from
  proceeding under the stale receiver namespace. The data runtime's
  fallback local merge construction now derives that target receiver namespace
  from catalog range metadata, opens existing receivers with
  `prefer_existing_identity_namespace`, passes the catalog-derived target to
  the merge coordinator for the opt-in transition action, and replays donor
  documents under that receiver namespace during catch-up. The focused DOCID
  gate now depends on the data-runtime split and merge fallback tests plus the
  fast metadata simulation smoke tests for split namespace derivation and merge
  reassignment opt-in recording. It also runs the public metadata split/merge
  lifecycle simulations that exercise forwarded public transition requests,
  post-split multi-range read readiness, and post-merge routing over
  namespace-aware DB opens, plus the seeded and expanded metadata VOPR
  campaigns so generated split/merge lifecycle sequencing must publish the
  runtime identity status required by the transition guards. It now also runs
  the public split/merge traffic chaos bucket across delayed transport,
  restarts, and partitions. Those simulations publish refreshed runtime
  doc-identity telemetry from each active replica after split/merge cutover
  before multi-range public queries, matching the production strict read guard:
  catalog routing alone is not enough once ordinal namespaces are range-local.
  Both
  catalog-to-coordinator runtime derivation and transition-simulation callback
  propagation are therefore exercised with the rest of the DOCID boundary
  suite. Metadata state now
  carries runtime document-identity telemetry into merged group status.
  Metadata compares each runtime identity namespace with
  the catalog's expected table/group/range namespace and marks the status
  `rebuild_required` when live ordinal rows belong to a stale namespace.
  Automatic merge planning fails closed when adjacent groups report incompatible
  live ordinal namespaces, conflicting namespace telemetry, catalog/runtime
  namespace drift, `rebuild_required`, or an active doc-identity reassignment.
  That prevents the automatic path from combining two range-local ordinal
  spaces until the explicit reassignment or rebuild lifecycle exists. New
  explicit reassignment merges are also blocked while either side already
  reports an active reassignment, while replay of the existing opted-in merge is
  allowed to continue so the in-flight namespace rewrite can finish. The shared
  internal metadata HTTP split/merge route and generic table workflow enforce
  the active-reassignment guard before delegating to any admin source or
  persisting new desired transitions, so operator requests fail early instead
  of being accepted and later skipped by the reconciler. Those internal HTTP
  routes now return a distinct 409 doc-identity namespace conflict for the guard
  or for delegated source-level readiness failures instead of collapsing them
  into malformed split/merge requests, and the metadata HTTP client preserves
  that conflict as `DocIdentityNamespaceMismatch` for split/merge callers.
  Explicit metadata merge requests now run the same
  compatibility check against the admin snapshot's merged runtime status when
  identity telemetry is available, so manual merge requests cannot bypass the
  DOCID namespace guard. Internal metadata merge requests can explicitly opt
  into the receiver-namespace replay path with
  `allow_doc_identity_reassignment`; that opt-in is carried through desired
  merge transitions, raft apply storage, projected metadata state, reconciler
  planning, executable merge transition actions, and the internal group-write
  HTTP action codec. The raft transition runtime now records that signal on the
  merge runtime before accept/catch-up/finalize actions execute, and the local
  merge coordinator persists the opt-in in its merge state so restart between
  phases does not erase the decision. Merge runtime status reports the persisted
  opt-in as an internal lifecycle fact, the transition-service queue path has
  focused coverage that cloned merge records dispatch the same callback,
  metadata simulations now record the same transition-runtime callback, and
  raft HTTP-host merge simulations now model the callback before accept,
  catch-up, and finalize actions when a queued merge transition carries the
  reassignment opt-in. This keeps the service-lane simulation aligned with the
  metadata and storage guard instead of silently skipping the opt-in side
  effect.
  A real raft HTTP-host merge simulation now also configures a receiver target
  namespace on `MergeCoordinatorRuntime`, carries the reassignment opt-in through
  the queued metadata record, and verifies that the receiver DB reports the
  target table/shard/range namespace after donor replay. The merge runtime
  reassignment callback now fails closed if a transition action carries the
  opt-in but the selected runtime does not implement the callback. The
  multiplexed transition runtime has focused coverage that the same
  reassignment callback dispatches through the donor/receiver group lookup
  before finalize, preventing multi-range service routing from dropping the
  opt-in side effect. The merge coordinator now applies the configured receiver
  identity namespace before persisting the reassignment opt-in, so a failed
  namespace rewrite cannot leave durable merge state claiming that
  reassignment is active. Reopened merge coordinators with an already-persisted
  reassignment opt-in also reapply the configured target namespace at lifecycle
  gates, recovering older or partial durable state before receiver bootstrap,
  catch-up, finalize, or rollback can proceed. This keeps rollback from leaving
  a receiver in an old identity namespace after donor replay has already used a
  persisted reassignment opt-in; the focused data-storage test bucket now
  exercises that rollback recovery path directly. The focused DOCID gate now
  includes the missing-callback fail-closed regression, active-reassignment
  planning guard, stale split-destination namespace guard, configured receiver
  namespace opt-in regression, and persisted rollback recovery regression.
  Metadata merged-group snapshots carry that observed fact for transition
  diagnostics. The generic metadata admin snapshot JSON route also exposes
  those merged-group doc-identity diagnostics, so operators can inspect the
  active reassignment signal and runtime namespace facts without calling the
  private storage-layer rebuild primitive.
  Namespace-conflict and `rebuild_required` telemetry still fail closed.
  Automatic split planning now uses the same source-group guard: if a range's
  runtime doc-identity telemetry reports a namespace conflict or
  `rebuild_required`, or if the source range reports an active doc-identity
  reassignment, the reconciler does not create a fresh automatic split intent
  because the current split lifecycle preserves/copies identity rows rather
  than repairing stale namespaces or compacting exhausted ordinal spaces.
  Internal metadata split request handlers now enforce the same guard for
  operator-requested splits, so manual split requests cannot bypass
  doc-identity namespace-conflict or `rebuild_required` telemetry while the
  split path remains preserve-only. Explicit merge requests now also require
  donor and receiver runtime identity status through metadata HTTP and the
  generic table workflow, so an operator cannot start either a namespace-rewrite
  merge or a preserve-existing merge while one side lacks the telemetry needed
  to evaluate the transition safely. The reconciler applies the same
  missing-status rule before upserting desired reassignment merges, and now
  blocks non-reassignment merge replay when one side is missing status after
  doc-identity telemetry exists elsewhere, so direct desired-state replay cannot
  bypass the higher-level validators. Manual split validation now also requires
  source range runtime identity status, and reconciler split replay treats a
  missing source status as incompatible once runtime identity telemetry is
  otherwise present, keeping preserve-only split transitions from advancing on
  unknown namespace health.
  Reconciler replay also preserves committed split and merge rollback reasons,
  marks already-committed non-terminal splits with
  `doc_identity_namespace_mismatch` when the source range reports stale or
  rebuild-required doc-identity telemetry, and marks already-committed,
  non-terminal merge transitions with the same reason when current runtime
  telemetry shows incompatible donor/receiver ordinal namespaces and the
  reassignment opt-in is absent. Older in-flight transition records therefore
  fail closed instead of advancing past the guard. Merged group status now
  derives an explicit doc-identity lifecycle of `unknown`, `preserving`,
  `reassigning`, `rebuild_required`, or `ready` from catalog identity domains,
  runtime ordinal telemetry, namespace-conflict flags, and active reassignment
  observations. Metadata status summarizes those lifecycle counts, and public
  cluster health reports `rebuild_required` as degraded while treating active
  reassignment as an in-progress healthy state. Mixed-version groups that have
  not emitted identity facts remain `unknown`, so rolling upgrades do not
  pretend a shard is ordinal-ready before runtime telemetry proves it. The
  local merge handoff path now has focused
  coverage that donor documents replayed into a receiver DB allocate identity
  rows in the receiver's table/shard/range namespace; this preserves the
  receiver-side ordinal and canonical-ID contract while the higher-level merge
  lifecycle keeps automatic incompatible live namespaces fail-closed.
  Merge coordinator state now persists the receiver identity reassignment target
  namespace alongside the reassignment opt-in flag, so a restarted merge
  coordinator can recover the exact table/shard/range namespace that must be
  reapplied before bootstrap, catch-up, finalize, or rollback. Merge transition
  status now reports that configured target namespace alongside the active
  reassignment flag, giving metadata observation code a concrete namespace
  signal instead of a boolean-only lifecycle marker. Provisioned read-handle
  caching now also keys query DB handles by the catalog-derived identity
  namespace and opens cached query handles with that namespace, so a range
  reassignment cannot reuse a query handle stamped for the old
  table/shard/range identity while the LSM root generation is unchanged. The
  cache-only lookup path and pending-open coordination use the same namespace
  key, avoiding stale-handle hits and cross-namespace pending-open collapse.
  Uncached provisioned lookup DB opens and local graph-edge read opens now use
  the same catalog-derived namespace, so direct read paths do not silently fall
  back to the compatibility namespace while cached query reads use the managed
  table/range namespace. Provisioned warm status-only opens, hosted local graph
  hydration opens, and the shared no-cache provisioned query-open helper used
  by scans, preflight, text/algebraic stats, and aggregation reruns follow the
  same rule. The primary lookup shortcut now validates a leased write DB against
  the catalog-derived table/shard/range namespace before serving the read, so it
  cannot bypass the namespace-keyed read cache with a stale range handle.
  Query, lookup, and warm-status read DB opens also validate the opened store's
  persisted namespace against the catalog-derived namespace after open and after
  query index-reconcile reopens, so prefer-existing read opens cannot silently
  keep a stale table/shard/range namespace. Hosted graph hydration and local
  graph-edge reads run the same post-open validation before using identity
  generations or graph postings. The shared managed write DB opener now applies
  the same validation after open and after index-reconcile reopens, so create,
  startup catch-up, status-only, cached, and uncached write handles fail closed
  before accepting mutations or publishing runtime state for a stale namespace.
  Split-transition status observation now applies the same expected-namespace
  check to status-only destination handles, so transition control loops cannot
  treat a destination with a stale persisted identity namespace as a healthy
  split participant.
  Regular multi-group table query, scan, and preflight fanout now run the same
  catalog/runtime doc-identity readiness check before shard fanout, so
  missing runtime identity telemetry, namespace conflicts, active reassignment,
  or `rebuild_required` telemetry fail before distributed read planning starts
  rather than only at graph expansion time. The lower-level distributed
  text-stat and algebraic
  aggregation helper fanout paths run the same check before issuing per-group
  reads, keeping aggregation-side follow-up requests from becoming an internal
  bypass around the table-wide readiness gate. Distributed explicit and
  background text-stat follow-up envelopes now also carry
  `_identity_read_generation`; shard collectors reject stale stamped requests
  before reading text index snapshots, so significant-terms aggregation support
  cannot silently recompute field/background statistics against a newer
  identity view than the search page that requested them. Aggregation
  full-result reruns
  and complete-result aggregation contexts now reuse the
  `identity_read_generation` snapped by the first result page when the caller
  did not explicitly stamp the public request, so paging-driven aggregation
  completion and aggregation-side fast-path planning stay on the same identity
  snapshot instead of restamping against current live identity rows. Algebraic exact-cardinality
  reads now also have a generation-aware path for root cardinality and terms
  child-cardinality aggregations, so stale scalar/path postings from documents
  deleted after the request's identity generation do not re-enter aggregation
  results through public-document-ID scans. Date-histogram aggregations with a
  stamped identity generation now use the same live-filtered scalar fact scan
  before building bucket counts and child fold metrics, avoiding stale
  materialized rollup rows when identity tombstones have advanced beyond index
  cleanup. The compatibility constrained-doc-ID path also live-filters its
  fallback result at the stamped generation when it cannot stay entirely on a
  resolved ordinal constraint set. Root algebraic metric and stats aggregations
  with a stamped identity generation now use live identity candidates plus
  doc-ID metric readers instead of trusting materialized rollups that cannot
  subtract identity tombstones after the fact. Single-field terms aggregations
  with fold-metric children now do the same live scalar-fact grouping under a
  stamped generation, and composite/path-fact terms variants that still depend
  on rollup-only reads decline the algebraic fast path until they have an exact
  generation-aware implementation. Explicit and implicit algebraic derived-join
  reads now also have generation-aware execution for local fold scans and
  distributed tensor-program partials: doc-fact, path-fact, and join-fact rows
  are filtered against the requested identity generation so tombstoned
  documents are subtracted from both sides of the join before aggregation.
  Distributed algebraic partial exports and multi-group
  distributed algebraic aggregation planning now preserve
  `_identity_read_generation` through the internal partial request envelope.
  Shard collectors accept omitted/current generation stamps and reject stale
  stamps before reading materialized partials, which keeps the distributed fast
  path on the same identity snapshot as the search page that triggered
  aggregation planning. The focused DOCID gate now names the aggregation-context
  non-current generation rejection, the stale/rebuild-required algebraic
  partial lifecycle rejection, and generation-aware derived-join partial
  subtraction, so materialized partial reads cannot silently bypass the
  fail-closed boundary before exact tombstone subtraction exists.
  Provisioned and hosted distributed algebraic aggregation
  responses now clone result label metadata before parsed aggregation requests
  are released, including nested bucket children, so response metadata does not
  borrow from the internal fanout request lifetime. Historical non-current
  materialized rollup reads still require exact generation-aware subtraction
  before they can be enabled.
- Phases 1-3 now have storage primitives, focused coverage, and initial
  document-set representation/fallback counters. Phase 4 has the
  algebraic resolved-filter boundary plus direct DB vector/sparse consumption
  of resolved algebraic filters, Phase 5 now has sparse ordinal consumption,
  live-filtered resolved ordinal constraints, sparse hit ordinal propagation,
  stable dense vector IDs with ordinal membership mappings, and parent-ordinal
  expansion for chunk/external dense vector IDs. Phase 6 now
  has the first full-text-to-ordinal projection
  boundary for vector filters, request-local reuse for repeated structured
  full-text filters, and direct ordinal-to-full-text-doc-number projection for
  resolved top-level and internally collected structured text filters when
  segment sidecar coverage is complete. Structured full-text filters that match
  zero documents now resolve to an explicit empty document set instead of
  falling through as unresolved, so vector/sparse candidate generation can
  short-circuit empty positive filters before expensive ANN work. Broad
  live-doc candidate filtering now
  has a tested native-constraint primitive and is enabled for non-chunk-backed
  dense, sparse, and full-text searches, sidecar-covered chunk-backed full-text
  searches, and dense chunk/generated searches that can expand live parent
  ordinals through dense membership rows. It is also enabled for chunk-backed
  sparse generated searches by expanding live parent ordinals through chunk
  artifacts into sparse physical doc numbers. Non-chunk sparse searches now use
  the same explicit ordinal-to-sparse-doc-number lookup instead of assuming
  sparse physical `doc_num`s equal identity ordinals, preserving correctness for
  legacy or repaired sparse layouts. The plain `match_all` fallback path now
  derives the same request-local native document constraints and applies
  resolved document-set filters/exclusions before totals and paging, so resolved
  ordinal filters no longer bypass the identity boundary when an all-doc scan is
  used. Match-all primary-document candidates now also carry
  their live identity ordinal when available, letting resolved ordinal filters
  stay as ordinal/doc-number constraints instead of widening through public
  document IDs. The stored-pattern/result-shaping fallback now preserves that
  boundary as well: ordinal/list/bitmap resolved filters are applied directly
  against hit ordinals when every hit carries one, and only mixed or legacy hit
  pages fall back to identity projection through public document IDs. Full-text
  hit construction now copies the segment sidecar `doc_ordinal` into each
  `SearchHit` when available, and chunk-parent result shaping preserves that
  ordinal when grouping chunk hits into parent hits. Sparse hit construction now
  resolves hit ordinals through the request-stamped identity table instead of
  trusting sparse physical `doc_num`s, so legacy or repaired sparse layouts that
  diverge from identity ordinals do not publish incorrect planner metadata.
  Dense hit construction performs the same request-stamped live ordinal lookup
  for resolved-filter pages and graph result-ref source pages. Composed result merging
  now preserves the common `identity_read_generation` when all shard results
  agree, or uses the caller's explicit stamped generation when one was provided,
  while leaving the merged result unstamped if shard generations conflict.
  Graph named-set cloning, and graph named-set fusion preserve hit ordinals, so
  ranked merge/fusion pages and single-set clone paths do not strip the ordinal
  metadata before graph handoff or result shaping. When every source hit has an
  ordinal and public IDs do not conflict with ordinal identity, named-set fusion
  ranks by ordinal-backed internal keys and maps the fused winners back to a
  representative public ID for output. Mixed pages keep the legacy public-ID
  fusion path; conflicting source ordinals for the same public ID clear the
  fused hit ordinal and force the safer identity projection fallback.
  Artifact-ID externalization also preserves hit ordinals
  while converting internal artifact keys to public hit IDs for top-level and
  graph-result hits. That candidate ordinal lookup uses the request's stamped
  `identity_read_generation`, keeping match-all and dense candidate filtering on
  the same request-local visibility boundary as dense, sparse, and text planning.
  Text search's direct resolved-filter-to-segment-doc-number projection now also
  passes that stamped generation into live filtering, so already-resolved
  document-set filters do not silently restamp against the current identity
  table view before full-text scoring. If a text segment lacks ordinal sidecar
  coverage and the executor also cannot project the resolved ordinal filter
  back to public IDs at the stamped generation, text native-constraint
  derivation now fails closed instead of dropping the filter. The shared
  dense/sparse native constraint path has the same representation check: a
  resolved ordinal include or exclusion must either lower to native document
  numbers or project back to public document IDs at the stamped generation, and
  unsupported projections fail closed instead of widening the request. Composed
  structured filters now additionally carry a request-local
  text-doc-number sidecar for the full-text branch when the identity view is
  known to be all-visible, avoiding a
  text-doc-number -> shard-ordinal -> text-doc-number round trip while still
  passing the shard-local `ResolvedDocFilter` to vector and graph consumers.
  Dense primary indexes now also maintain an in-memory ordinal -> dense vector
  ID cache populated only after successful mapping commits and cleared on
  rebuild/reset. The persistent ordinal mapping remains the source of truth for
  cold or reopened indexes, but hot composed filters can project ordinal
  constraints to vector IDs without issuing tens of thousands of mapping reads.
  The full-text scorer also accepts sorted native doc-number include/exclude
  constraints on the request, so common term/match bool queries can stay on the
  fast postings collector instead of compiling broad `doc_num` clauses into the
  query tree. Dense standalone searches without explicit document filters no
  longer pre-materialize a broad all-live-doc vector-ID filter; visibility is
  left to normal result postprocessing unless the caller supplied a real
  document constraint.
  In the 100k `public-query-guardrail --mode handler
  --query-shape hybrid-filter-exclude-project` profile, these changes moved the
  handler path from roughly 570ms before this pass to roughly 55ms while
  preserving the filled `k=20` correctness guardrail.
  Graph
  `result_ref` projection from complete resolved document sets now carries the
  same stamped generation when translating ordinals back to graph document keys,
  and the DB projection helper fails closed if an ordinal is not visible at that
  generation instead of exposing a stale current/live mapping. The shared
  dense/sparse/text native constraint fallback and stored-pattern result-shaping
  fallback now pass the same generation into ordinal-to-public-ID projection, so
  compatibility paths do not silently widen an old request-local document set
  against current live identity rows. The focused DOCID gate now also names the
  no-widening `match_all` path, the sparse mixed-coverage fallback that keeps
  explicit public document IDs when identity coverage is incomplete, and the
  dense/sparse/text handling of resolved all-document exclusions as empty native
  candidate sets. Stored-pattern/result-shaping fallback
  also resolves native public include/exclude doc IDs into request-generation
  `ResolvedDocSet` values and applies them against hit ordinals when every hit
  carries `doc_ordinal`, keeping the last postprocessing filter stage from
  reintroducing public-ID membership checks for ordinal-complete pages. This
  boundary is now covered by the focused `lib-db-result-shape-test` build step,
  which imports the DB result-shaping module explicitly and verifies that
  native public-ID constraints are resolved once, then applied against hit
  ordinals without stored-field loads. The DOCID gate now depends on that step
  and includes the stored-pattern fail-closed regressions for missing or
  unsupported ordinal projection. Remote vector-worker offload now also
  fails closed when handed an in-memory
  `ResolvedDocFilter`; the worker envelope has no cross-process representation
  for shard-local ordinal sets yet, so the guard prevents accidental
  unconstrained vector execution if a caller misses the higher-level
  remote/cross-group rejection. Hosted remote preflight uses the same guard
  before query encoding, preventing planning diagnostics from silently dropping
  a request-local resolved ordinal filter on the way to a remote owner. The
  shared internal JSON query encoder also rejects
  `ResolvedDocFilter` pointers directly, making this fail-closed behavior a
  property of the serialization boundary rather than only of today's callers.
  Remote shard query parsing now restores the caller-supplied
  `identity_read_generation` onto parsed `SearchResult` values for both
  vector-worker and regular query routes, so an internal response does not need
  to expose that generation in public JSON just to let the coordinator preserve
  an explicitly stamped snapshot boundary.
  Cross-range graph execution now requires a stamped identity generation for
  every graph fanout, not only `result_ref` selectors. Provisioned and hosted
  table-query orchestration derive that stamp from the base multi-group search
  result when the caller did not set one explicitly, so graph expand/hydrate
  requests cannot let each range restamp independently mid-query.
  Distributed right-join workers now also carry the snapped
  `identity_read_generation` from the first structured group-local search page
  into follow-up pages, so right-side broadcast/shuffle hit collection and
  unmatched pagination stay on one identity snapshot instead of restamping
  midway through the scan. If the first page cannot report a stamped
  generation, the existing follow-up guard still fails closed before issuing
  page 2. Distributed join right-table group planning now also applies the
  strict doc-identity readiness gate before direct group-local fanout, so
  rebuild-required, namespace-conflict, active-reassignment, missing-status, or
  stale-namespace runtime telemetry cannot bypass regular multi-group read
  validation through join-specific routes. The stateful distributed shuffle
  engine uses the same guard before choosing worker/finalizer groups, covering
  durable shuffle execution and coordinator fallback paths in addition to
  transient broadcast/shuffle right-table fanout. Internal join worker and
  finalizer HTTP routes now map doc-identity namespace mismatches to explicit
  409 conflicts, matching internal write-route behavior and letting
  coordinators observe readiness failures without treating them as generic
  worker crashes. Internal group read routes now apply the same conflict mapping
  for lookup, scan, query/vector-worker, preflight, graph, text-stat, and
  algebraic-partial worker calls, so DOCID readiness failures remain visible at
  every internal HTTP read boundary. The internal API client now preserves those
  conflict bodies as `DocIdentityNamespaceMismatch` for read, join, vector,
  batch, and transaction group calls instead of collapsing them into topology,
  intent, decision, or generic HTTP failures. Public transaction commit routes
  now convert propagated doc-identity namespace failures into structured
  `doc_identity_unavailable` commit conflicts with retry hints rather than
  surfacing an internal server error. Retrieval-agent HTTP and A2A entrypoints
  now also map doc-identity readiness failures to explicit unavailable/failed
  responses instead of treating them as generic retrieval failures. Query-builder
  HTTP entrypoints apply the same unavailable mapping when runtime preflight
  detects a doc-identity namespace mismatch, keeping stale-DOCID telemetry out
  of generic agent failures. Public table batch, query, and query-view handlers
  now preserve doc-identity unavailability as 503 responses at the public HTTP
  boundary instead of collapsing future DOCID readiness failures into generic
  internal query/write failures.
  Distributed right-join worker/result exchange now carries a private
  ordinal-backed identity key for structured local `SearchHit` rows, so
  unmatched-right completion does not treat a second public `_id` alias for an
  already matched ordinal as a distinct unmatched document. The private key is
  stripped when join shells are applied to public query responses.
  Internal shard query forwarding now has a private `_resolved_doc_filter`
  envelope for serialized `ResolvedDocFilter` values. The envelope carries the
  table/shard/range identity namespace, the request identity generation, and an
  adaptive include/exclude set representation (`all`, `none`, public doc keys,
  sorted ordinals, or Roaring bitmap bytes). Public query parsers reject the
  field with the rest of the shard-only DOCID controls, while internal query
  and vector-worker routes can parse it back into an owned request-local
  resolved filter. DB search validates the envelope namespace and generation
  against the opened shard before execution, returning a namespace conflict or
  stale-generation failure instead of widening the filter.
  The same private envelope is now reused by distributed graph expand and graph
  hydrate requests. Graph expansion lowers it into the per-frontier
  `SearchRequest`, while graph hydration validates the namespace/generation and
  applies the filter against hydrated hit ordinals or document keys before
  returning hits. Hosted remote graph forwarding and regular query/preflight
  fanout validate the envelope against every target group's runtime identity
  status before forwarding it, rejecting active reassignment, namespace
  conflict, rebuild-required, missing-status, or stale-namespace targets instead
  of broadcasting one shard-local ordinal set across incompatible shards. Range
  records now carry an optional preserved document-identity domain separate from
  physical `group_id`/`range_id`. Finalized splits stamp both children with the
  source identity domain, finalized merges preserve the receiver identity
  domain, and open/restore/query validation derives the expected DB namespace
  from that catalog domain. Runtime health and ShardDocSet forwarding therefore
  accept same-domain ordinal envelopes after preserve-only splits, while still
  rejecting stale physical namespaces and reassignment/rebuild transitions.
  Metadata HTTP admin snapshot and table-range responses now round-trip those
  preserved identity-domain fields, while legacy range JSON without the fields
  still derives the domain from the physical group/range. Query-shaped internal
  text-stat workers reuse the same private query envelope and preserve
  `_resolved_doc_filter` across parse/encode. Algebraic distributed partials
  now carry the stamped identity generation into tensor-program scans and use
  generation-aware fact-row projection for supported doc-fact, path-fact, and
  derived-join operators. Explicit/background significant-term stats now carry
  the same internal ShardDocSet envelope and apply it while computing
  shard-local foreground/background text statistics, so restricted significant
  term scoring uses the exact ordinal-visible corpus instead of widening to
  public document IDs. ShardDocSet forwarding now also requires
  runtime telemetry with actual ordinal identity rows, not only a catalog range
  identity match, so mixed-version workers that have not proven ordinal support
  fail closed instead of receiving a private field they might ignore. The API
  DOCID gate now includes an explicit internal worker exchange matrix: each
  named worker boundary must either carry ShardDocSet or validate a
  generation-stamped projection before fanout, with zero boundaries allowed to
  remain in the old fail-closed serialization gap. The current matrix is:
  query, vector-worker, preflight, graph expand/hydrate, search-request text
  stats, and explicit/background text stats carry ShardDocSet; graph edge
  reads, graph result refs, aggregation context/reruns, algebraic partials,
  distributed-join right fanout/worker/unmatched-follow-up/finalizer, and
  shuffle worker/finalizer validate generation-stamped projections. The same
  gate covers preserved-range lifecycle cutover for missing, old, and stale
  runtime status reports, plus split/merge/reassignment validation during
  mixed-version transition planning. Metadata lifecycle tests now cover the
  rolling-cutover status surface directly: old nodes that only report group
  health stay `unknown`, preserved split identity domains stay `preserving`,
  and concrete stale namespaces are promoted to `rebuild_required` even when no
  ordinal rows have been observed. Distributed join readiness now validates any
  concrete runtime identity namespace, even on empty shards without ordinal
  rows, and explicitly accepts preserved split identity domains while rejecting
  active reassignment, rebuild-required, and stale-namespace right-table fanout.
  Phase 7 now has public `with` parsing plus the request-local document-set
  binding path for algebraic filters across vector, sparse, and text result
  filtering. The shared document-set module now owns compatible union and
  intersection/difference operations for ordinal-backed and doc-key-backed
  sets, normalizes empty composed doc-key results to `none`, plus filter-level
  intersection that intersects include sets while unioning excludes. The
  algebraic resolved-filter boundary uses those
  operations for `bool` include and `must_not` exclude composition,
  `conjuncts`, `disjuncts`, `match_all`, `match_none`, explicit document-ID
  filters, and `ref` bindings when every binding and child filter can stay in a
  compatible resolved representation. Required `bool.must`/`bool.filter`
  clauses can now compose `ref` bindings that already carry exclude sets
  without widening back to public document IDs, and set-only contexts can use a
  binding's effective include-minus-exclude set when that difference is exactly
  representable. For `all` minus an ordinal-backed exclude set, the algebraic
  set-only path materializes the request generation's visible document ordinals
  before subtracting, keeping the candidate set exact instead of treating a
  finite complement as unrepresentable. Negated or disjunctive child filters
  that require broader complement semantics still fall back through the
  compatibility path.
  Incoming in-memory `ResolvedDocFilter` values that still carry `doc_keys` now
  normalize those keys through the DB identity table before they compose with
  native public-ID include/exclude constraints. Fully covered keys therefore
  stay on the ordinal-backed path; partial/missing coverage keeps the doc-key
  fallback rather than inventing ordinals.
  Algebraic resolved-filter bindings now apply the same identity-table
  normalization before `ref` bindings compose with native ordinal-producing
  filters, so complete doc-key-backed bindings can stay on the ordinal path.
  Partial/missing binding coverage still falls back through the compatibility
  path. Graph `result_ref` selector resolution can now consume a
  named result's `ResolvedDocSet` for unbounded selectors by projecting
  ordinals back to graph node keys through DB identity rows. That applies both
  to composed named result sets and the non-composed base `$full_text_results`,
  `$fused_results`, and `$embeddings_results` aliases, as well as manually
  supplied named graph input sets in standalone graph execution. Limited
  `result_ref` selectors continue to use hit-order materialization so `limit`
  preserves ranked result semantics and avoids unnecessary base doc-set
  projection. Pattern graph result hits now also resolve their binding keys to
  request-generation `doc_ordinal` values when the DB identity table has
  coverage, and non-pattern graph hits built from result nodes now perform the
  same request-generation lookup. Downstream named-set fusion, graph expansion,
  and result-shaping stages can keep ordinal metadata instead of re-resolving
  graph hits by public document ID. Graph result sets now distinguish complete node-backed document sets from
  page-derived hit sets: when a graph query materializes the full `nodes` list
  but only exposes a paged hit list, dependent unbounded `result_ref` selectors
  use the complete node projection to build a `ResolvedDocSet`; incomplete
  page-only sets remain fail-closed. Graph expansion intersection now also
  intersects result pages by `doc_ordinal` when both the base page and all graph
  result pages are ordinal-complete, falling back to public document IDs only
  for mixed pages. Graph expansion union uses the same ordinal-complete guard
  for deduplication, so alias/document-key mismatches do not make the union keep
  two hits for the same canonical document when both pages carry ordinals. Final
  search-hit deduplication now follows the same rule for ordinal-complete hit
  pages, preserving the legacy public-ID dedupe fallback only for mixed pages.
  Single-result query merges and graph named-set cloning now preserve
  `doc_ordinal` metadata, fused named sets preserve source ordinals when they
  agree, and fused hits drop conflicting ordinal metadata when the same public
  ID appears with inconsistent identity rows. This keeps internal graph/fusion
  handoff ordinal-aware while still falling back to projection-safe behavior for
  mixed or corrupt pages.
  The focused DOCID test gate now includes the graph union/intersection,
  ordinal-fusion preservation/conflict, named-set cloning, single-result merge
  preservation, unbounded `result_ref` projection, complete-node doc-set, and
  graph generation-threading cases that cover these boundaries, plus the
  distributed cross-range graph guard that rejects ranges whose doc identity
  status reports rebuild-required before fanout.
  Phase 8 has initial identity health counters, graph-result doc-set
  production, and ordinal rows for algebraic scalar doc-fact/path-dictionary
  term, terms, configured-field exists, range, IP range, configured-field
  string-scan, promoted path-dictionary string-scan/path-exists postings,
  unpromoted schemaless path lookup postings, path-fact geo projection, and
  join fact scan rows. Full-text query execution now has a native numeric
  document-clause surface for projected constraints and can avoid public-ID
  projection for sidecar-covered resolved ordinal filters, including structured
  text filters that collect ordinal-backed document sets before scoring. The
  remaining portable backup path now preserves identity-table rows directly and
  restores graph edge batches into structured graph artifacts rather than colon-delimited
  edge keys; raw logical snapshots already carry identity rows as part of the
  store snapshot. Snapshot restore now validates imported identity rows
  structurally and, for strict restore callers, rejects a restored namespace
  that differs from the requested table/shard/range namespace before deferred
  runtime repair can mark the primary restore complete. Portable AFB import now
  has the same opt-in strict namespace validation for restored identity-table
  rows. Table restore callers now pass the catalog/runtime expected namespace
  into local snapshot restore, so both provisioned table restore and
  metadata-driven table-provisioner restore fail closed before reopening a range
  with stale identity metadata. Provisioned background restore-repair catch-up
  now opens the restored DB with the same catalog-derived namespace and rejects
  stale persisted identity rows before repairing runtime artifacts. Callers
  that explicitly set `prefer_existing_identity_namespace` keep the
  preserve-existing behavior used by transition/reassignment flows. The split
  path now preserves the existing identity namespace on both split outputs,
  avoiding missing ordinal coverage after page-level or streaming split. The
  focused DOCID gate now names the portable AFB identity round-trip, invalid
  canonical-ID rejection, strict snapshot validation, deferred restore namespace
  rejection, explicit restore runtime repair, and incomplete deferred import
  recovery regressions, so restore/rebuild validation coverage is part of the
  DOCID boundary rather than only broad DB test coverage. Restore import marker
  and repair marker mutation helpers are internal to the DB module; managed
  callers only use read/probe helpers plus the incomplete import recovery probe
  when reopening a table. The
  standalone graph edge store is now off delimiter parsing. Operational
  lifecycle coverage now exercises the highest-risk DOCID cutovers directly:
  split finalization preserves the identity namespace on both sides and proves
  exact public doc-ID filters still resolve to the expected shard-local
  ordinals; merge reassignment proves donor and receiver documents are
  searchable through the receiver namespace after opt-in; strict namespace
  reopen fails before reassignment repair and succeeds after repair; generation
  projection hides future ordinal allocations and tombstones deleted ordinals;
  and DB-level capacity tests cover the final allocatable `u32` ordinal before
  rejecting new documents at exhaustion. The remaining DOCID work is broader
  operational scale and upgrade validation rather than a known internal
  public-document-ID exchange path.

## Open Problems

The hard parts are operational rather than syntactic:

- ordinal stability across compaction
- broader split/merge/reassignment chaos coverage under multi-node restarts
- broader snapshot visibility and MVCC integration across public distributed
  query paths
- rebuild validation beyond strict namespace repair/reopen checks
- production shared/global bitmap cache invalidation beyond the structured
  filter cache's namespace-and-generation key primitive
- very large shards that approach `u32` ordinal limits under realistic indexes
- mixed-version indexes during rolling upgrades beyond doc-identity lifecycle
  fail-closed coverage
- whether public query errors should fail closed or fall back when ordinal
  coverage is missing

The design should prefer correctness first: missing ordinal coverage should use
the doc-key fallback or fail closed according to the caller's policy. It should
not silently widen a filter.

## Historical Key-Codec Migration Notes

The original implementation shape below has been replaced by the implemented
`src/storage/internal_keys.zig` module described in the implementation notes
above. The remaining value in this section is the adversarial coverage checklist
for structured internal keys and public document-ID boundaries.

Minimum coverage should include:

- ordering of encoded document IDs matches ordering of raw document IDs
- round-trip encode/decode for arbitrary bytes, including `0x00` and `0xff`
- prefix scans over ASCII IDs
- prefix scans over binary IDs containing `0x00`
- shard range membership based on encoded primary keys
- artifact cleanup for documents with binary IDs
- TTL read/write for arbitrary IDs
- edge key encode/decode with arbitrary source and target IDs

Useful adversarial IDs:

- empty string
- `":"`
- `":i:"`
- `":e:"`
- `":t"`
- `"\x00"`
- `"\x00\x00"`
- `"\xff"`
- `"abc\x00def"`
- `"abc\xffdef"`
- `"abc:"`

## Recommendation

`antfly-zig` should move to a binary, order-preserving key codec for all
document-derived records.

This gives us:

- arbitrary document IDs
- preserved ordering
- efficient prefix scans
- less brittle storage internals

Trying to patch the current delimiter format with more escaping would preserve
the core problem. The right fix is to make storage keys structured and binary.
