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
- Make query-time filters and exclusions bitmap-native internally.

## Non-Goals

- Backward-compatible on-disk keys for old databases.
- A text-readable internal key format.
- Reusing delimiter parsing for new code paths.
- Exposing internal posting IDs in the public API.
- Requiring every index family to migrate to bitmap-native filtering in a
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
  query planner exchanges bitmaps of doc_ordinal
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
table.

The better design is:

```text
deterministic canonical identity + persisted compact ordinal
```

This gives stable rebuild semantics without giving up bitmap performance.

## Bitmap-Native Document Sets

Current internal query paths often resolve filters and exclusions into string
document-ID lists. That works, but it forces every index family to repeatedly
translate public IDs back into its own physical identity.

Introduce an internal document-set abstraction:

```zig
const ResolvedDocSet = union(enum) {
    all,
    none,
    doc_keys: []const []const u8,      // compatibility fallback
    ordinals: RoaringBitmap,          // preferred internal form
};

const ResolvedDocFilter = struct {
    include: ResolvedDocSet,
    exclude: ResolvedDocSet,
};
```

The planner should prefer `ordinals` whenever the shard has doc-ordinal
coverage. `doc_keys` remains as a migration fallback and for index families that
have not yet been converted.

Useful bitmap sets:

```text
live_docs_bitmap      documents visible to the read snapshot
deleted_docs_bitmap   tombstoned or hidden documents
include_bitmap        positive filter constraint
exclude_bitmap        exclusions, blocked IDs, must_not filters
candidate_bitmap      index-produced candidate set
```

Query execution should intersect and subtract these bitmaps before expensive
scoring whenever possible.

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

If the shard has ordinal coverage:

```text
visible_bitmap = bitmap(doc_id -> doc_ordinal)
match(body, "renewal") -> scored stream or candidate bitmap
must(match, ref visible) -> intersect with visible_bitmap
```

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

Long term:

```text
vector_id -> doc_ordinal
doc_ordinal -> vector_id(s)
```

Dense search should accept:

```text
include_bitmap: ?RoaringBitmap(doc_ordinal)
exclude_bitmap: ?RoaringBitmap(doc_ordinal)
```

and apply them during candidate generation or immediately after candidate
retrieval, before expensive reranking or projection. The vector index can keep
its own vector IDs for physical layout, but those IDs should not be the shared
query identity.

### Sparse Vector

Sparse postings should store `doc_ordinal` instead of raw document keys:

```text
term / feature -> [(doc_ordinal, weight)]
```

This makes sparse query filters the same bitmap intersection problem as
full-text.

### Algebraic

Algebraic path dictionary postings should move from string doc-key rows to
ordinal postings:

```text
path / value / token -> RoaringBitmap(doc_ordinal)
```

During migration, algebraic filters can continue returning `doc_keys` when
ordinal coverage is missing. Once coverage exists, the preferred result is a
bitmap that dense, sparse, full-text, and graph planning can all consume.

### Graph

Document-backed graph nodes and edges should reference `doc_ordinal` where the
node is a document identity. Graph-native node IDs can remain separate, but any
edge that is used as a document filter should be able to produce a
`RoaringBitmap(doc_ordinal)`.

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
  ordinals: RoaringBitmap,
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
- Keep `doc_keys` as a compatibility representation.
- Add counters for ordinal fast path, string fallback, missing ordinal coverage,
  and unsupported filter shapes.

### Phase 4: Algebraic Filter Bitmaps

- Teach algebraic filter resolution to return `RoaringBitmap(doc_ordinal)` when
  possible.
- Preserve the existing doc-key list path as fallback.
- Add direct include/exclude bitmap plumbing into vector search requests.

### Phase 5: Dense and Sparse Consumption

- Teach dense vector search to consume include/exclude ordinal bitmaps.
- Teach sparse vector search to consume include/exclude ordinal bitmaps.
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

## Open Problems

The hard parts are operational rather than syntactic:

- ordinal stability across compaction
- range split and merge semantics
- snapshot visibility and MVCC integration
- rebuild and restore validation
- bitmap cache invalidation when generations advance
- very large shards that approach `u32` ordinal limits
- mixed-version indexes during rolling upgrades
- whether public query errors should fail closed or fall back when ordinal
  coverage is missing

The design should prefer correctness first: missing ordinal coverage should use
the doc-key fallback or fail closed according to the caller's policy. It should
not silently widen a filter.

## Implementation Shape

Introduce a dedicated codec module, for example:

- `src/storage/key_codec.zig`

Suggested responsibilities:

- encode a single component
- decode a single component
- build primary document keys
- build TTL keys
- build summary, embedding, chunk, artifact, and edge keys
- parse internal keys back into structured parts where needed
- compute prefix scan bounds for document-ID prefixes

The important rule is that key construction and parsing should be centralized.
Call sites should not manually append delimiters or inspect textual suffixes.

## Migration Plan

### Option A: Flag Day

Introduce the new codec and require fresh databases.

Pros:

- simplest implementation
- avoids dual-format parsing
- easiest to reason about

Cons:

- old on-disk data must be rebuilt or migrated offline

### Option B: Dual Read, New Write

Teach the storage layer to read both old and new key formats but only write the
new one.

Pros:

- softer migration

Cons:

- more code complexity
- scan logic gets harder because both formats must be considered
- deletes and artifact cleanup become easier to get wrong

### Option C: Offline Rewrite Tool

Add a one-shot migration utility that reads an old DB and writes a new DB using
the new codec.

Pros:

- keeps runtime code clean
- explicit operational boundary

Cons:

- requires migration tooling and validation

### Recommended Path

For `antfly-zig`, prefer:

1. implement the new codec
2. switch runtime storage to the new format
3. use fresh databases for development and testing first
4. add an offline rewrite tool later if old data needs to be preserved

This is materially safer than carrying both formats in the main code path.

## Code Areas Affected

The change is broader than `DocStore` alone.

Primary places that must move off delimiter semantics:

- `src/storage/docstore.zig`
- `src/storage/db/db.zig`
- `src/storage/ttl.zig`
- `src/storage/db/catalog/index_manager.zig`
- any enrichment or artifact scan code that currently prefixes on raw `doc_key`
- any graph-key parser that currently searches for `:i:`, `:out:`, or `:in:`

The dense-vector metadata mapping should also stop embedding raw `doc_key`
inside metadata strings and instead use the same encoded component scheme under
its metadata namespace.

## Testing Requirements

Minimum test coverage should include:

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
