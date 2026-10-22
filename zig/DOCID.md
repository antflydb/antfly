# Document IDs and Posting IDs

## Purpose

`antfly-zig` originally inherited the same flat-keyspace idea as Go antfly:
document IDs were written directly into LMDB keys, and derived records were
built by concatenating textual delimiters and suffixes.

Examples from the original layout:

- primary document: `<docID>`
- embedding: `<docID>:i:<indexName>:e`
- summary: `<docID>:i:<indexName>:s`
- enrichment artifact: `<docID>:e:<type>:<name>:...`
- TTL metadata: `<docID>:t`

That layout only worked as long as document IDs did not collide with the
storage format. It coupled correctness to delimiter avoidance and made
arbitrary byte-valued document IDs unsafe.

This document describes the document identity design `antfly-zig` moved to,
which:

- allows arbitrary document IDs
- preserves lexicographic ordering by raw document ID bytes
- supports efficient prefix scans
- removes delimiter-based parsing from internal storage
- gives query execution a compact internal posting identity for bitmap filters
  and exclusions

## Original Problem

The original storage layout depended on raw user IDs appearing as the leading
portion of many structured keys.

That created several classes of problems:

- document IDs containing `:i:`, `:e:`, `:t`, `:out:`, or similar markers could
  confuse key parsing
- document IDs containing bytes such as `0x00` or `0xff` interacted badly with
  sentinel-based range construction
- metadata and secondary records were distinguished by string suffixes instead
  of explicit record types
- internal code had to repeatedly split and parse strings to recover structure

Even where a specific path happened to work, the format was brittle because
new record types had to keep avoiding user-controlled bytes. The binary key
codec described below replaced this layout; see Implementation Notes for the
shipped state.

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

`antfly-zig` moved away from "raw doc ID followed by textual suffixes" to
explicit binary records. Constructors and parsers live in
`pkg/antfly-embedded/src/storage/internal_keys.zig`; the artifact contract
built on them is in [DB.md](DB.md#derived-artifact-contract).

Document-owned records (all under the user namespace byte `0x01`):

```text
primary document
  0x01 | enc(doc_id) | 0x10

ttl timestamp
  0x01 | enc(doc_id) | 0x11

relational row
  0x01 | enc(doc_id) | 0x12

artifact (common prefix)
  0x01 | enc(doc_id) | 0x20 | enc(type) | enc(name) | <record suffix>

embedding / asset / resolution artifact
  0x01 | enc(doc_id) | 0x20 | enc("embedding" | "asset" | "resolution") | enc(name)

chunk
  0x01 | enc(doc_id) | 0x20 | enc("chunk") | enc(name) | 0x30 | u32be(chunk_id)

document unit (asset, or chunk within a unit)
  0x01 | enc(doc_id) | 0x20 | enc("asset") | enc(name) | 0x35 | enc(unit_id)
  0x01 | enc(doc_id) | 0x20 | enc("chunk") | enc(name) | 0x35 | enc(unit_id) | 0x30 | u32be(chunk_id)

derived embedding of another artifact
  <base artifact key> | 0x31 | enc(name)

graph edge (owned by the source document)
  0x01 | enc(doc_id) | 0x20 | enc("graph") | enc(index_name) | 0x32 | enc(edge_type) | enc(target_doc_id) [| relationship suffix]
```

Notes:

- `enc(x)` is the [component codec](#component-codec) and always ends with
  the `00 00` terminator, so `0x01 | enc(doc_id) | 0x10` is
  `01 | <escaped doc_id> | 00 00 | 10`. This matches the
  `user_ns | enc(doc) | 00 00 | 0x20 | ...` notation in DB.md, which writes the
  terminator out.
- `0x10`, `0x11`, `0x12`, and `0x20` are single-byte record kinds after the
  document component; `0x30`, `0x31`, `0x32`, and `0x35` are record suffix
  kinds inside an artifact. Other per-document state (asset state `0x33`,
  graph asset state `0x34`, coverage `0x36`, unit navigation `0x37`/`0x38`,
  and so on) uses further kind bytes defined in `internal_keys.zig`.
- The optional graph relationship suffix is either the encoded logical source
  (when it differs from the owning document) or a versioned extension
  `0x01 | enc(edge_id) | enc(owner_document) | enc(logical_source)`.
  Incoming-edge lookup uses the graph index's incoming directory, not a
  second document-keyed record (see [GRAPH.md](GRAPH.md)).
- Replay/derived-work state (`0x02`) and identity state (`0x03`) are separate
  reserved namespaces outside user document keys.
- Fixed-width integers are big-endian so numeric order is preserved
  lexicographically.

## Ordering Properties

Within the user namespace (`0x01`):

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
lower = 0x01 | enc_prefix(p)
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

The binary key codec fixes how user document IDs are stored. On its own it does
not give the query engine a compact internal identity that can be shared by
full-text, dense vector, sparse vector, algebraic, and graph indexes.

Antfly treats document identity as a backend primitive:

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

The local identity table persists the canonical forward row as well as the
public-ID and reverse ordinal rows:

```text
raw doc_id        -> doc_ordinal
canonical_doc_id  -> doc_ordinal
doc_ordinal       -> raw doc_id / canonical state
```

## Document-Set Native Planning

Current internal query paths often resolve filters and exclusions into string
document-ID lists. That works, but it forces every index family to repeatedly
translate public IDs back into its own physical identity.

Antfly has an internal document-set abstraction for this. Persisted compact
ordinals are the canonical internal identity, but query-time sets remain
adaptive:

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

The planner prefers ordinal-backed forms whenever the shard has doc-ordinal
coverage, but it does not force every tiny set through a bitmap. `doc_keys`
remains useful for explicit small ID filters, point lookups, API compatibility,
and mixed-coverage or legacy-snapshot fallback paths.

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

Query execution intersects and subtracts these bitmaps before expensive scoring
whenever possible. Tiny lists can stay as lists until an operator needs set
algebra, repeated reuse, or bitmap-only execution.

## Query Language and CTE Bindings

Named filter bindings fit naturally with canonical ordinals. The public JSON DSL
allows reusable document-set expressions:

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

`ref` points at the compiled binding rather than re-expanding the original JSON
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

All index families converge on `doc_ordinal` as the common document identity
used for filtering and candidate exchange, with compatibility fallbacks for
legacy or mixed-coverage snapshots. See Implementation Notes for the shipped
detail behind each index family below.

### Full-Text

Full-text query filters produce Roaring bitmaps of numeric document IDs, and
those numeric IDs map to canonical shard-local ordinals through an optional
document-ordinal sidecar carried on each segment:

```text
segment_doc_id -> doc_ordinal
segment_live_bitmap projected into doc_ordinal space
full_text_filter(query) -> RoaringBitmap(doc_ordinal)
```

Segment-local IDs still exist for compact postings. When every relevant
segment has sidecar coverage, the planner boundary exchanges canonical
ordinals directly; segments without coverage (legacy or mixed-version
snapshots) fall back to projection through public document IDs.

### Dense Vector

Dense vector metadata has vector IDs and doc-key mappings, which is not by
itself a canonical posting ID.

Primary document-level dense vectors use a stable vector ID that is
independent from physical ordinals. The dense layer persists explicit mappings
so ordinal-backed filters can constrain vector search without making the
vector ID itself rebuild-sensitive:

```text
doc_id -> primary_document_vector_id
vector_id -> doc_ordinal
doc_ordinal -> vector_id(s)
```

Dense search accepts:

```text
include: ?ResolvedDocSet
exclude: ?ResolvedDocSet
```

and applies them during candidate generation or immediately after candidate
retrieval, before expensive reranking or projection. Bitmap-backed sets are best
for broad filters and exclusions; sorted ordinal lists are often cheaper for
small explicit filters. Child/chunk/external vectors keep their own physical
row IDs, and primary document vectors keep a stable vector ID that is joined to
the current ordinal through persisted membership rows.

### Sparse Vector

Sparse postings keep their own physical `doc_num` rather than storing
`doc_ordinal` directly; resolved ordinal filters map through
`doc_ordinal -> doc_id -> doc_num` before sparse scoring:

```text
term / feature -> [(doc_num, weight)]
```

This gives sparse query filters the same document-set boundary as full-text:
small candidate sets can remain as ordinal lists, while broad term filters can
use bitmaps, without requiring sparse doc numbers to equal identity ordinals.

### Algebraic

Algebraic path dictionary postings have ordinal-backed rows alongside the
existing string doc-key posting rows:

```text
path / value / token -> RoaringBitmap(doc_ordinal), plus existing doc-key rows
```

Algebraic filters return `doc_keys` when ordinal coverage is missing. When
coverage exists, the result is a `ResolvedDocSet` that dense, sparse,
full-text, and graph planning can all consume. Small and sparse results may
remain as sorted ordinals; large dense or reusable results can promote to
`ordinal_bitmap`.

### Graph

Document-backed graph nodes and edges reference `doc_ordinal` where the node is
a document identity: graph query execution materializes a request-local
`ResolvedDocSet` for document-backed graph hits alongside the public hit list.
Graph-native node IDs remain separate, and edges used as document filters can
produce a `ResolvedDocSet`.

## Visibility, Deletes, and Generations

Ordinals are not renumbered for ordinary deletes or updates. Correctness comes
from visibility metadata:

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
correctness does not depend on immediate ordinal reuse.

## Shards, Ranges, and Restore

`doc_ordinal` is shard-local or range-local, and is not globally meaningful
without its shard/range namespace.

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

Split preserves the source identity namespace on both output ranges (option 1).
Merge preserves the receiver's identity namespace by default, with an explicit
opt-in reassignment flow that rewrites the receiver namespace and replays donor
documents into it (option 2).

Backup and restore persist:

- document key codec version
- canonical document identity policy
- doc-ordinal allocation table
- ordinal generation / epoch
- tombstone and visibility metadata

The deterministic `canonical_doc_id` lets restore tooling validate that rebuilt
ordinal mappings still refer to the same logical documents, even if compact
ordinals are reassigned during an explicit rebuild.

## Implemented Components

The migration landed per component rather than as a flag-day change. Index
families that have not converged on ordinals for a given code path keep a
`doc_keys`/public-ID fallback rather than blocking on the rest. Paths below are
relative to `zig/pkg/antfly-embedded/src/storage/`.

| Component | Location | Role |
|-----------|----------|------|
| Safe document keys | `internal_keys.zig` | Binary component codec and every internal key constructor/parser: primary, derived, graph, TTL, replay metadata, dense metadata |
| Ordinal allocation | `db/doc_identity.zig` | Per-shard/range allocator; persists forward, canonical, and reverse rows plus create/delete generations; live-set materialization |
| Internal document sets | `db/doc_set.zig` | `ResolvedDocSet`/`ResolvedDocFilter`, sorted-ordinal and density-promoted Roaring forms, deep clone, union/intersection/difference |
| Algebraic filter bitmaps | `db/algebraic/index.zig` (`resolvedDocFilterForFilterJsonAlloc`) | Lowers algebraic filter JSON to ordinal-backed sets when coverage exists |
| Query `with` bindings | `SearchRequest.doc_filter_bindings` | Each binding is compiled once per request and pushed into dense, sparse, full-text, algebraic, and graph operators |

`doc_set` is storage-internal: `storage/db/mod.zig` does not re-export it or
`DocOrdinal`, the DB helpers that resolve public IDs into sets are private
callbacks, and the re-exported `SearchRequest` carries the resolved filter only
as an opaque hook. The public API exchanges raw document IDs and request JSON
only.

## Implementation Notes

> **Relocated:** The per-change implementation notes (identity generation,
> live-doc filtering, per-index-family ordinal consumption, namespace and
> split/merge lifecycle, and the Phase 1–8 roadmap narrative with its 100k
> timing passes) are preserved verbatim in
> [docs/design/docid/history/implementation-notes.md](../docs/design/docid/history/implementation-notes.md).
> The standing rules from those notes are below.

### Key Codec Discipline

- Primary records, embedding and graph edge artifacts, TTL timestamps, and
  replay metadata use explicit internal namespaces and record kinds. The TTL
  module exposes only the structured TTL key path; TTL and artifact cleanup
  scans are bounded by the encoded document component, not a textual prefix.
- In-memory maps keyed by user-controlled components use length-prefixed tuple
  components, never separator joins: derived replay batcher dedupe (sparse
  `(index, doc)`, graph legacy shorthand `(source, target, edge_type)`;
  current graph identities also retain owner/index and optional explicit edge ID), transaction
  read-snapshot `(table, doc_id)` maps, DB and enrichment-worker
  chunk-generation caches, and text-stat `(index, field)` /
  `(aggregation, field)` grouping.
- Dense vector mapping metadata uses a v2 encoded-component namespace for
  `(index, record-kind, id/doc)` rows. Lookup, ordinal-member scan, cleanup,
  and next-vector-ID reservation still read legacy textual rows so pre-v2 stores
  keep their stable vector mappings.
- New distributed transaction participant IDs use an ASCII length-prefixed
  table-name component plus group ID; legacy participant strings still parse.
- The standalone KV graph index writes structured keys under a distinct
  `graph_index` artifact type (DB-owned edges stay under `graph`). Portable
  graph-edge backup exports structured artifacts and converts older edge-batch
  values on import.

### Identity Generations And Read Snapshots

Every read runs against one `identity_read_generation`. The invariants:

- **Stamp or reject.** A request without a generation is stamped at the
  top-level DB boundary with the current replay sequence. An explicit
  non-current generation is rejected until primary storage and every index can
  serve historical snapshots; returning historical identity filters over current
  candidates is not allowed. The same check guards DB search, preflight and
  planning stats, text-stat collection, table-read aggregation context, graph
  execution, and the C API search, search-hits, and aggregate-hits entry points.
  The C API runs it before readable-lease preparation, so a stale request causes
  no read-consistency side effect. Rejections increment
  `doc_set_planning`'s stale-generation counter.
- **One snapshot per response.** The stamped generation is reused for
  same-response aggregation, rerank, response shaping, full-result aggregation
  reruns, text-stat follow-ups, cross-range graph expand/hydrate/edge reads, and
  distributed join follow-up pages. Cross-range graph fanout requires a stamp on
  every request; table-query orchestration derives it from the base multi-group
  search when the caller omitted it.
- **Follow-ups echo the token.** Requests that carry explicit hit IDs (named
  graph input sets, aggregate-hit requests, join page 2+) must supply the
  generation and fail closed instead of restamping. The C API returns the token
  on JSON search responses, the search-hit result struct, packed dense results,
  graph result objects, and as an 8-byte footer after the ID blob in the binary
  packed search wire response.
- **Non-current fails closed.** Internal routes that lower envelopes into a
  `SearchRequest` (vector worker, graph expand) go through the same DB boundary;
  graph expand maps the unsupported-snapshot error to 400. Distributed algebraic
  partial and text-stat collectors accept omitted/current stamps and reject
  stale ones before reading materialized state.
- **Internal fields are rejected by public parsers.** `_identity_read_generation`,
  the worker-envelope spelling `identity_read_generation`, `_resolved_doc_filter`,
  native doc-ID constraint envelopes, and the doc-identity reassignment control
  are rejected by top-level JSON field inspection (not substring matching), so
  public query text may still mention the names. The guard runs in the public
  table-query HTTP adapter, the query-contract parser (including relaxed parsing
  for `with` bindings), before the packed dense fast parser, in the embedded JSON
  search surface, join subquery lowering, retrieval-agent query runners, and the
  serverless search-plan, table graph-query, and graph endpoint (neighbors,
  traverse, shortest path) parsers.
- **Resolved filters are shard-local.** An in-memory `ResolvedDocFilter` is
  rejected by multi-group fanout, hosted single-group forwarding, remote
  vector-worker offload, hosted remote preflight, and the shared internal JSON
  query encoder. Cross-shard exchange uses the `ShardDocSet` envelope below,
  public IDs, or another generation-stamped serializable filter.
- **Ordinals never leave the shard.** Merging more than one shard/range result
  clears `SearchHit.doc_ordinal`, as does multi-group graph hydration. Public
  query responses, C API JSON, and the packed dense wire omit it. The private
  ordinal-backed key used for distributed right-join unmatched tracking is
  stripped before public responses. Composed result merging keeps the common
  generation when all shards agree (or the caller's explicit stamp), and leaves
  the result unstamped on conflict.
- **Cache keys carry the snapshot.** Request-local structured full-text filter
  caches key by filter JSON plus generation; shared entries additionally key by
  the full `Namespace{table_id, shard_id, range_id}` (never a lossy tag).
- **No widening on projection.** Ordinal-to-public-ID projection runs at the
  stamped generation and fails closed when an ordinal is not visible. Native
  constraint derivation (dense, sparse, text) and the stored-pattern
  result-shaping fallback either lower a resolved filter to native doc numbers,
  project it back at the stamped generation, or fail with
  `UnsupportedQueryRequest`; they never drop it.

### Live-Doc Filtering

- Ordinal-producing filters (algebraic postings, explicit ID filters, bool/ref
  composition) are intersected with identity-table live state at the request
  generation, so stale posting rows cannot expose tombstoned documents. Public-ID
  resolution omits tombstoned ordinals; mixed-coverage fallbacks keep only live
  or unknown IDs.
- `liveFilteredDocSetFromStoreAlloc(.all)` returns a live ordinal set only when
  every primary document has a live identity mapping; otherwise it keeps `.all`
  so planning never silently narrows. Resolved all-document exclusions lower to
  an empty native candidate set.
- The broad live-doc candidate filter covers non-chunk sparse and full-text
  search, sidecar-covered chunk full-text, dense chunk/generated/mixed layouts
  (parent ordinal → live child/external vector IDs via membership rows), and
  sparse chunk/generated layouts (parent ordinal → current chunk artifacts →
  sparse `doc_num`). Dense searches without an explicit document constraint do
  not pre-materialize an all-live vector-ID filter; visibility is left to result
  postprocessing.
- `match_all` derives the same native constraints and applies them before totals
  and paging; its candidates carry live ordinals.
- Structured full-text filters that match nothing resolve to `none`, so vector
  and sparse search short-circuit before ANN work.
- The stored-pattern/result-shaping fallback applies resolved filters (and
  native public include/exclude IDs resolved once at the request generation)
  directly against hit ordinals when every hit carries one, without stored-field
  loads; mixed pages project through the identity table.

### Writes, Backup, And Restore

- `DB.batch` writes identity rows in the same committed batch as the primary
  update; the first identity-bearing batch also persists the namespace record.
  Deletes tombstone ordinals rather than removing mappings. Within one batch,
  resurrecting a tombstoned document advances its create generation and an
  upsert/delete pair leaves it tombstoned.
- Transaction intent resolution, explicit orphaned-intent recovery, the
  background transaction-recovery runtime, and TTL cleanup all append the same
  identity side effects to their committed batches. Namespace reassignment
  refreshes the recovery runtime's hook context.
- After a commit the DB caches the identity visibility summary so post-load
  planning can prove all-live visibility; cold stores fall back to the persisted
  summary, then full identity stats.
- The canonical forward row is maintained by writes and namespace reassignment.
  Validation treats an existing canonical row as authoritative and tolerates
  stores that have not backfilled it; allocation rejects a corrupt canonical row
  before reserving a new ordinal.
- Portable AFB backup has a dedicated doc-identity KV batch block, restored
  verbatim after checking it only targets the identity namespace. Both AFB
  import and raw logical snapshot restore then validate the whole table:
  forward and reverse rows agree, every mapped ordinal has state, canonical IDs
  recompute from the restored namespace and raw ID and are nonzero, delete
  generations do not precede create generations, and every ordinal is below the
  persisted next-ordinal watermark. `isVisibleAt(generation)` (visible from
  creation, hidden at or after deletion) is the shared visibility predicate.
- Strict restore callers pass the expected catalog namespace: provisioned table
  restore, metadata table-provisioner restore, and restore-repair catch-up fail
  closed on a mismatched namespace before reopening the range.
  `prefer_existing_identity_namespace` keeps the persisted namespace for
  transition and reassignment flows. Restore import/repair marker mutation
  helpers, the unconditional dense/graph rebuild helpers, and the all-steps
  repair wrapper are internal to the DB module; managed callers use the bounded
  `IfNeeded` or step-wise maintenance paths.

### Ordinal Use By Index Family

| Family | Physical row ID | Ordinal mapping | Notes and fallback |
|--------|-----------------|-----------------|--------------------|
| Full-text | Segment-local doc number | Optional doc-ordinal sidecar per segment, in stored-document order; written by backfill, incremental projection, split rebuild, and merge (rewritten in live-document order) | With full sidecar coverage, resolved filters project to snapshot-global `doc_num` constraints, passed as sorted native include/exclude lists so term/match bool queries stay on the fast postings collector; hits copy the sidecar ordinal (chunk-parent grouping keeps it). Missing sidecar or mixed public keys fall back to public-ID projection; if that is also impossible, constraint derivation fails closed. |
| Dense | Stable deterministic vector ID for primary vectors; distinct vector-row IDs for chunk/external vectors | Persisted `doc_id → vector_id`, `vector_id → doc_ordinal`, `doc_ordinal → vector_id(s)` rows; parent-ordinal membership rows expand one ordinal to all child vectors; an in-memory ordinal → vector-ID cache populated only after mapping commits and cleared on rebuild | Ordinal-as-vector-ID is a named legacy path accepted only when existing HBC metadata matches the same document; write batching prefetches both forms. Derivation without an ordinal mapper fails closed. Hits report the identity ordinal, not the vector ID. Rebuild replays under stable vector IDs. |
| Sparse | Sparse-index-allocated `doc_num` | `doc_ordinal → doc_id → doc_num`; chunk layouts expand parent ordinal → chunk artifact keys → `doc_num` | Hits resolve `doc_ordinal` through the identity table at the request generation (never copy `doc_num`); chunk hits recover the parent ordinal from chunk artifact keys. Derivation without a mapper fails closed. Chunk encoding tolerates out-of-order doc numbers. |
| Algebraic | Doc-key posting rows | Ordinal rows alongside: scalar doc-fact, path dictionary, unpromoted `path_lookup`, path-fact (geo), join `docjf_ord`/`jf_ord` | Covers `term`, `terms`, `exists`, `range`/`numeric_range`/`term_range`/`date_range`/`ip_range`, string scans (`match`, `prefix`, `wildcard`, `regexp`, `fuzzy`), IP string, and `geo_*` filters. Ordinal rows are used only when their coverage matches the legacy rows; join cleanup uses ordinal references only after verifying they match. Mixed coverage falls back through document IDs. |
| Algebraic aggregations | — | Fixed constraints and range/histogram bucket intersections try ordinal membership first | Under a stamped generation, cardinality, date-histogram, root metric/stats, single-field terms with fold-metric children, derived joins, and distributed range/histogram/terms partials subtract tombstoned documents. Composite and path-fact terms variants that depend on rollup-only reads decline the algebraic fast path. |
| Graph | Graph-native node IDs | Document-backed hits are annotated with request-generation ordinals and materialize a request-local `ResolvedDocSet` | Unbounded `result_ref` selectors consume the complete node projection or resolved set; a page-derived set (materialized fewer than `total_hits`, or a saturated page) fails closed. Limited selectors use hit order. Union, intersection, final dedupe, named-set cloning, and fusion key by ordinal when every hit has one; conflicting ordinals for one public ID clear the ordinal. |

Hybrid requests: the public dense fast parser is disabled for requests carrying
`full_text_search`, `filter_query`, `exclusion_query`, named bindings, or
internal doc-filter fields. Dense and sparse derivation intersect the
`req.full_text` result set before scoring. Default dynamic string fields emit the
same exact `field.keyword` companion as the no-schema mapper so structured term
filters resolve through full-text postings. When the identity view is known
all-visible, composed structured filters carry a request-local text-doc-number
sidecar for the full-text branch. Incoming `doc_keys` sets and `ref` bindings
are normalized through the identity table before composition, so fully covered
keys stay ordinal-backed. Algebraic lifecycle state is part of the fail-closed
boundary: planners reject `rebuild_required` capabilities, and symbolic filters
with `require_algebraic_filter_resolution` never reuse stale artifacts.

### Stats And Counters

`DBStats.doc_identity` reports the persisted namespace (`table_id`, `shard_id`,
`range_id`), allocated count, `next_ordinal`, state-row count, live and
tombstoned counts, remaining `u32` capacity, `ordinal_capacity_exhausted`, and
`rebuild_required`. Normal status uses bounded next-ordinal metadata. Diagnostic
stats add a full state scan and an exact primary-coverage scan (missing
mappings, missing state, live primaries with tombstoned state) and min/max
create/delete generations, which bound the tombstone-retention window.
`rebuild_required` is derived from exhaustion and diagnostic coverage gaps.

`DBStats.doc_set_planning` reports resolved-set count, `doc_keys` fallback
count/total, ordinal-list count/total, ordinal-bitmap count/total, missing
coverage, bitmap promotions, unsupported filter shapes, and stale-generation
rejections.

Both survive runtime-status snapshot cloning and travel through metadata
runtime reports, metadata HTTP, and raft/store runtime-status record version 4.
Metadata derives a per-group doc-identity lifecycle (`unknown`, `preserving`,
`reassigning`, `rebuild_required`, `ready`); groups that have not emitted
identity facts stay `unknown`. Cluster health treats `rebuild_required` as
degraded and active reassignment as healthy-in-progress. The admin snapshot JSON
route exposes merged-group doc-identity diagnostics.

Ordinal exhaustion: ordinal `0` is reserved as "missing" and `maxInt(u32)` is
not allocatable. A batch that would cross the sentinel fails with
`DocOrdinalExhausted` without committing partial rows. New-document writes check
the sentinel before any sync-level wait; updates to documents that already have
ordinals proceed. Transaction intents check under the DB apply lock before
storing pending new-document intents. Split and merge planning and validation
(including reassignment opt-ins) treat exhaustion as a readiness failure.

### Namespaces, Split/Merge, And Readiness

- **Namespace record.** The identity table persists `Namespace{table_id,
  shard_id, range_id}`; canonical IDs hash all three. Single-store callers use
  the compatibility namespace `0/0/0`; `OpenOptions.identity_namespace` seeds or
  validates another. Older two-component rows decode with `range_id = 0`. Batch
  writes fail closed if the stored and allocating namespaces disagree.
- **Catalog domain.** Range metadata carries a stable `range_id` (zero falls
  back to `group_id`) and an optional preserved document-identity domain
  separate from physical `group_id`/`range_id`; legacy range JSON derives the
  domain from the physical group/range. Managed opens derive the expected
  namespace from that domain. Routing topology epochs stay keyed on routing
  shape (`table_id`, `group_id`, key bounds), not `range_id`.
- **Open-time validation.** Every managed write open (create, startup catch-up,
  status-only, cached, uncached, and after index-reconcile reopen) and every
  read open (query, lookup, warm status, graph hydration, graph-edge read, the
  no-cache query helper, and the primary lookup shortcut over a leased write DB)
  validates the persisted namespace against the catalog and fails with
  `DocIdentityNamespaceMismatch`. Read-handle caches and pending-open
  coordination key on the namespace, so a reassignment cannot reuse an old
  handle even when the LSM root generation is unchanged.
- **Error mapping.** Public batch, query, and query-view handlers return 503
  (identity unavailable). Internal group-write, shard-operation, group-read
  (lookup, scan, query, vector-worker, preflight, graph, text-stat,
  algebraic-partial), and join worker/finalizer routes return 409; the internal
  API client and metadata HTTP client decode that back to
  `DocIdentityNamespaceMismatch`. Public transaction commit returns a structured
  `doc_identity_unavailable` conflict with retry hints; retrieval-agent HTTP/A2A
  and query-builder entry points map it to unavailable.
- **Split** preserves the source namespace on both children (page split and
  streaming split copy identity rows). The destination DB namespace comes from
  the projected destination range, else the source identity domain. Finalized
  splits stamp both children with the source domain. Split-transition status
  applies the expected-namespace check to destination handles.
- **Merge** preserves the receiver namespace by default. Reassignment is an
  explicit opt-in, `allow_doc_identity_reassignment`, carried through desired
  transitions, raft apply, projected state, reconciler planning, transition
  actions, and the group-write action codec. `MergeConfig` carries the target
  receiver namespace; the coordinator applies it *before* persisting the opt-in
  (so a failed rewrite cannot leave durable state claiming reassignment),
  persists the target alongside the flag, reapplies it at every lifecycle gate
  (bootstrap, catch-up, finalize, rollback) after restart, and replays donor
  documents under the receiver namespace. A configured target blocks progress
  until the opt-in is recorded, and a runtime without the reassignment callback
  fails closed. Merge status reports the target namespace.
- **Reassignment primitive.** Validates current rows, rewrites the namespace and
  every `canonical_doc_id`, preserves ordinals, generations, and stable vector
  IDs, and revalidates. It runs under the DB apply lock, is internal to the DB
  module, and is rejected on status-only handles.
- **Readiness guards.** A range is not ready for ordinal exchange when runtime
  status is missing, reports a namespace conflict or `rebuild_required`, has a
  runtime namespace that differs from the catalog domain, or has an active
  reassignment. Automatic and manual split/merge planning, reconciler replay of
  desired transitions, multi-group query/scan/preflight fanout, distributed
  text-stat and algebraic helper fanout, cross-range graph execution,
  distributed join right-table planning, and the stateful shuffle engine all
  apply this check before fanout. New reassignment merges are blocked while
  either side is reassigning; replay of an in-flight opted-in merge may continue.
  Reconciler replay marks committed non-terminal transitions with
  `doc_identity_namespace_mismatch` when telemetry turns stale.

### Distributed Exchange: ShardDocSet

Internal shard forwarding carries a serialized `ResolvedDocFilter` in the
private `_resolved_doc_filter` envelope:

- the table/shard/range identity namespace and the request identity generation;
- an adaptive include/exclude representation: `all`, `none`, public doc keys,
  sorted ordinals, or Roaring bitmap bytes.

Public parsers reject the field. Internal query and vector-worker routes parse
it into an owned request-local filter, and DB search validates the namespace
and generation against the opened shard before execution, returning a
namespace conflict or stale-generation failure instead of widening. Graph expand
lowers it into the per-frontier `SearchRequest`; graph hydrate applies it to
hydrated hits. Before forwarding, the coordinator validates every target group's
runtime status against the readiness guard and requires telemetry that proves
actual ordinal rows, so mixed-version workers that might ignore the field fail
closed. Same-domain envelopes are accepted after preserve-only splits. The wire
format tolerates additive fields and rejects missing namespace/generation data
and invalid ordinals.

Internal worker exchange matrix (every boundary must be on one side; none may
remain in the fail-closed serialization gap):

| Mechanism | Boundaries |
|-----------|------------|
| Carries ShardDocSet | query, vector-worker, preflight, graph expand/hydrate, search-request text stats, explicit/background text stats (significant terms) |
| Validates a generation-stamped projection | graph edge reads, graph result refs, aggregation context/reruns, algebraic partials, distributed-join right fanout/worker/unmatched follow-up/finalizer, shuffle worker/finalizer |

Set algebra (`doc_set.zig`, used by the algebraic resolved-filter boundary):

- Union, intersection, and difference are defined for compatible
  ordinal-backed and doc-key-backed sets; empty composed doc-key results
  normalize to `none`. Filter-level intersection intersects includes and unions
  excludes.
- `bool` include / `must_not` exclude, `conjuncts`, `disjuncts`, `match_all`,
  `match_none`, explicit document-ID filters, and `ref` bindings compose
  natively when every child stays in a compatible representation.
- `bool.must`/`bool.filter` may compose `ref` bindings that carry exclude sets;
  set-only contexts use include-minus-exclude when exactly representable.
- `all` minus an ordinal-backed exclude materializes the generation's visible
  ordinals before subtracting.
- Negated or disjunctive children needing broader complement semantics fall
  back through the compatibility path.

### DOCID Query Benchmark CLI And Bulk-Ingest Notes

`antfly-storage-bench` provides `storage_bench query` and `storage_bench write`
for direct DOCID/document-set benchmarking outside the DB catalog path.
`storage_bench query` runs each filtered shape (match-all, full-text,
sparse-vector) in `public_ids` mode (public document IDs resolved on every
query) and `ordinal_docset` mode (request-local ordinal filter pre-resolved at
a stamped identity generation), and fails on correctness mismatches by
default. Guardrail flags: `--allow-mismatch` keeps exploratory runs
non-fatal, `--max-ordinal-ratio <ratio>` turns the ordinal/public timing
comparison into an optional performance guard, and
`--require-public-resolution-delta` asserts that the public-ID path still
exercises request-time doc-set resolution while the pre-resolved ordinal path
does not. `--defer-full-index-load` (with `--progress-every <docs>`) makes
large-load setup measurable by deferring full index load. `--bulk-load` routes
benchmark loads through an internal primary-store bulk session so the
direct-ingest path can be benchmarked directly.

The LSM bulk path drains pending mutable bulk records into sorted ingest
before declaring append direct-ingest ineligible: append-only primary records
could already direct-ingest, but small non-append metadata records staying in
the mutable table used to block later append batches and force them down the
mutable flush path. This keeps `--bulk-load` benchmarks, and any other
primary-store bulk session, on the direct sorted-ingest path instead of
silently falling back to the mutable flush path.

For public-query evidence across full-text, dense-filter, sparse-filter, graph
expansion, algebraic-filter, and hybrid-composed shapes, use
`python3 scripts/run_db_query_matrix.py --suite public` (100k documents by
default). Dense/HBC cost is better isolated with `dense-stack-bench`, because the
public-query guardrail does not surface dense/HBC profile counters for
dense-filter or hybrid-composed requests. For that reason the matrix does not
require symbolic dense profile rows by default; pass
`--public-arg=--require-symbolic-profile` only on a route expected to emit those
profiles. This flag replaces `DOCID_PERF_MATRIX_REQUIRE_SYMBOLIC_PROFILE=1` from
the removed `run_docid_perf_matrix.sh` wrapper
([history](../docs/design/docid/history/implementation-notes.md#key-measurements)).

> **Relocated:** The DOCID query-benchmark diary (267 lines: benchmark
> command shapes, smoke-run numbers, and the write-path optimization narrative
> that led to the fixes above) that previously lived here is preserved
> verbatim in
> [docs/design/docid/history/query-bench-diary.md](../docs/design/docid/history/query-bench-diary.md).

### Test Coverage

There is no single DOCID build step. DOCID boundaries are covered by the
storage DB suites (`antfly-storage-db-test`, `antfly-storage-db-query-test`,
including the result-shaping and capacity tests), the `capi-test` step (part of
`antfly-unit-test`), and the `*_docid_test` modules wired in
`zig/pkg/antfly/build/api_tests.zig`: `api_transactions_docid` (run by
`antfly-api-transaction-commit-test`), `api_table_reads_docid`
(`antfly-api-table-read-test`), `api_public_table_http_docid`
(`antfly-api-public-table-http-test`), `api_table_writes_docid`,
`raft_transition_runtime_docid`, `lib_api_docid`, `lib_serverless_docid`, and
`lib_docid_lifecycle`. Metadata VOPR split/merge lifecycle campaigns and the
optional metadata and LSM chaos suites cover restart, partition, and
compaction hardening.

## Why `u32` Ordinals

The ordinal is intentionally `u32` and shard-local. That keeps native filter
bitmaps, dense/sparse/text sidecars, and internal wire forms compact, and it
matches the 32-bit doc-number model used by common segment/bitmap layouts. A
single physical range should split before the final allocatable ordinal is
exhausted; if we ever need more than roughly 4.29B addressable slots in one
logical index without splitting, the extension point should be a segmented
ordinal space such as `(range/segment identity, u32 local ordinal)`, not a
blanket `u64` ordinal in every hot path.

## Open Problems

The hard parts are operational rather than syntactic:

- production-scale ordinal stability coverage across async/background
  compaction beyond the explicit LSM/full-text chaos gates and deterministic
  cache invalidation fixtures
- production multi-node split/merge/reassignment chaos with sustained
  concurrent query/write pressure beyond deterministic split handoff,
  metadata restart/partition, and public traffic simulations
- broader snapshot visibility and MVCC integration across public distributed
  query paths beyond the focused generation-cutover identity coverage;
  historical non-current materialized partial and rollup reads stay disabled
  until exact generation-aware tombstone subtraction exists
- rebuild validation beyond strict namespace repair/reopen checks
- production shared/global bitmap cache invalidation beyond the structured
  filter cache's namespace-and-generation key primitive and LSM cache
  ownership-prefix invalidation coverage
- very large shards that approach `u32` ordinal limits under production-scale
  data volumes, beyond the final-ordinal simulation and split-before-cap
  invariant
- real old/new binary mixed-version rolling upgrades beyond doc-identity
  lifecycle fail-closed fixtures and internal wire/status compatibility
  fixtures
- the cost of building a full-text-derived document set for hybrid queries:
  vector candidates are constrained correctly, but the projection still runs a
  broad scored text query to collect matching ordinals
- continued audit that public query errors fail closed or use exact doc-key
  fallback when ordinal coverage is missing
- a suspected public-query guardrail harness gap, as of the 2026-05 log: at
  100k, graph-expand direct DB-search measured `~592ms` while the handler path
  measured `~0.4ms`; it needs separate evidence before being treated as a
  product regression
  ([history](../docs/design/docid/history/implementation-notes.md#key-measurements))

The design should prefer correctness first: missing ordinal coverage should use
the doc-key fallback or fail closed according to the caller's policy. It should
not silently widen a filter.

## Key-Codec Test Checklist

Coverage for structured internal keys and public document-ID boundaries
(`internal_keys.zig` plus DB batch/get/scan and identity-resolution tests)
should include:

- ordering of encoded document IDs matches ordering of raw document IDs
- round-trip encode/decode for arbitrary bytes, including `0x00` and `0xff`
- prefix scans over ASCII IDs
- prefix scans over binary IDs containing `0x00`
- shard range membership based on encoded primary keys
- artifact cleanup for documents with binary IDs, including raw IDs that are
  adjacent by prefix
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
