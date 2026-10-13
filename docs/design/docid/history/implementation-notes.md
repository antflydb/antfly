# DOCID Implementation Notes And Phase Roadmap

> Summary of a verbatim log. Full text: [docs/design/docid/history/implementation-notes.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/docid/history/implementation-notes.md). Originally relocated verbatim from `zig/DOCID.md` (lines 749–1496 and 1529–2216 at commit 3ce5aff4ea) on 2026-10-12, after durable decisions were folded into [DOCID.md](../../../../zig/DOCID.md).

These notes tracked the move from raw, delimiter-suffixed public document IDs to structured internal keys plus a per-range identity table (`doc_id <-> doc_ordinal`, deterministic `canonical_doc_id`, create/delete generations), and the threading of ordinal-backed `ResolvedDocSet`s and a request-local `identity_read_generation` through every index family, distributed boundary, and lifecycle transition. Paths such as `src/storage/db/doc_identity.zig` refer to `zig/pkg/antfly-embedded/src/storage/`; "the focused DOCID gate" was never a single build step (see the design doc's Test Coverage section). The log closes by stating that the remaining DOCID work is broader production-scale and real rolling-upgrade validation rather than a known internal public-document-ID exchange path.

## Decisions

**Keys and identity table**
- Internal keys use a binary component codec (`src/storage/internal_keys.zig`) with explicit namespaces and record kinds; the legacy textual TTL `:t` suffix and the standalone KV graph index's colon-delimited `:i:`/`:out:`/`:in:` keys are gone — so NUL/`0xff`/delimiter-shaped IDs and index names cannot collide by prefix.
- Every in-memory tuple key built from user IDs (replay dedupe, transaction read snapshots, chunk caches, text-stat grouping) uses length-prefixed components — embedded NULs could otherwise collapse distinct entries.
- Dense vector metadata moved to a v2 encoded-component namespace; lookup, scan, cleanup, and next-ID paths still read legacy textual rows so upgraded stores keep stable vector mappings. Distributed transaction participant IDs use an ASCII length-prefixed table name while still parsing the legacy form.
- Deletes tombstone ordinals rather than remove mappings, preserving ordinal stability for existing index generations. Identity rows are written in the same committed batch as primary updates (also for transaction resolution, orphaned-intent recovery, background recovery, and TTL cleanup).
- Ordinal state is self-validating generation history: nonzero canonical IDs, delete generation never before create, and a shared `isVisibleAt(generation)` predicate.
- Ordinals are `u32`; the allocator fails closed with `DocOrdinalExhausted` at the reserved sentinel without committing partial rows, checked before derived/index wait work and for transaction intents. Split is the preferred response near the boundary; once exhausted, lifecycle planning fails closed until repair, reassignment, or rebuild.
- Any future operation that compacts or reassigns ordinals must build a new identity and index generation and rebuild ordinal-backed dense, sparse, text, algebraic, and graph artifacts before cutover — never mutate the mapping under existing indexes. Namespace reassignment may rewrite `canonical_doc_id` but must preserve `doc_id -> doc_ordinal` and stable vector IDs.

**Snapshot boundary**
- Requests that omit `identity_read_generation` are stamped at the top-level DB search boundary with the current replay sequence; explicit non-current generations are rejected (and counted) until primary storage and every index can serve historical snapshots — otherwise identity filters would be historical while candidates are current.
- Follow-up work reuses the first page's stamp: aggregation reruns, rerank, response shaping, join follow-up pages, cross-range graph fanout, text-stat follow-ups. Named graph input sets and aggregate-hit requests with explicit hit IDs must carry a token. C API responses (including an 8-byte footer on the packed binary wire) return the stamped generation, and stale stamps are rejected before readable-lease preparation.
- Filter doc-set caches key on filter JSON plus generation (and full identity namespace for any future shared cache).

**Public/internal boundary**
- `doc_set`/`DocOrdinal` are storage-internal; the public API exchanges raw document IDs and request JSON only, and `SearchHit.doc_ordinal` is omitted from all response encodings.
- Public parsers (HTTP table-query, query-contract, embedded JSON, join subqueries, retrieval agents, serverless search and graph endpoints, packed dense fast parser) reject `_identity_read_generation`, `identity_read_generation`, native doc-id constraint envelopes, reassignment controls, and `_resolved_doc_filter`, using top-level JSON field inspection rather than substring matching.
- Doc-identity readiness failures map to 503 identity-unavailable on public routes and 409 on internal routes; the internal client preserves them as `DocIdentityNamespaceMismatch`. Transaction commit returns a structured `doc_identity_unavailable` conflict.

**Index families**
- Dense: primary vectors use stable deterministic vector IDs distinct from ordinals; ordinal filters go through persisted mapping rows, and the ordinal-as-vector-ID branch is a named legacy path requiring matching HBC metadata. Chunk/external vectors persist parent-ordinal membership rows. Missing mapper → fail closed.
- Sparse: the sparse index allocates its own `doc_num`; ordinals map `doc_ordinal -> doc_id -> doc_num`; hits resolve ordinals from the identity table rather than copying `doc_num`.
- Full-text: segments carry an optional ordinal sidecar (preserved through backfill, split, merge); projection to `doc_num` is used only when every segment has coverage, otherwise falls back through public IDs.
- Algebraic: ordinal posting rows (`docjf_ord`, `jf_ord`, scalar/path/geo companions) are written beside legacy rows and used only when coverage matches the legacy rows; mixed coverage falls back through document IDs rather than narrowing.
- Graph: unbounded `result_ref` selectors fail closed if the named result may be incomplete (total hits > materialized, or saturated page) or its doc set cannot be projected; limited selectors keep hit-order semantics.

**Distribution and lifecycle**
- Resolved ordinal sets are shard-local. Cross-group work exchanges either the private `_resolved_doc_filter` (ShardDocSet) envelope — namespace, generation, and an adaptive `all`/`none`/doc keys/sorted ordinals/Roaring set — or generation-stamped projections. Merged multi-range results clear `doc_ordinal`. ShardDocSet forwarding requires runtime telemetry with actual ordinal rows, so unproven mixed-version workers fail closed.
- Split preserves the source identity namespace on both children; merge keeps the receiver namespace unless the persisted `allow_doc_identity_reassignment` opt-in rewrites it. The opt-in is applied before it is persisted and reapplied on reopen at lifecycle gates, including rollback. Range records carry a preserved identity domain separate from physical `group_id`/`range_id`.
- Routing topology epochs stay based on routing shape (`table_id`, `group_id`, key bounds), not `range_id`, because identity changes do not change routing.
- Automatic and manual split/merge, reconciler replay, multi-group reads, joins, shuffle, and graph fanout fail closed on missing runtime identity status, namespace conflict, catalog/runtime drift, `rebuild_required`, or active reassignment. Catalog routing alone is not enough once namespaces are range-local.
- Merged group status derives a lifecycle of `unknown`, `preserving`, `reassigning`, `rebuild_required`, or `ready`; cluster health reports `rebuild_required` as degraded and reassignment as healthy-in-progress; mixed-version groups without telemetry stay `unknown`.
- Read and write DB opens validate the persisted namespace against the catalog-derived namespace (cached handles are keyed by it), except callers that explicitly set `prefer_existing_identity_namespace` for transition/reassignment flows.
- Backup/restore preserves identity rows verbatim (dedicated AFB block) and validates the whole table afterward; strict restore rejects a namespace that differs from the expected one.

## Rejected approaches
- Delimiter-suffixed or NUL-delimited keys/tuples — collide for adversarial IDs (empty, `0x00`, `0xff`, delimiter-shaped).
- Using the identity ordinal as the physical dense vector ID or sparse `doc_num` — breaks for multi-vector documents and legacy/repaired layouts; kept only as named legacy compatibility paths.
- Silently restamping follow-up requests against the current identity table — lets one logical query observe two snapshots.
- Forwarding in-memory `ResolvedDocFilter` pointers across processes — the JSON encoder, vector-worker offload, and hosted preflight reject them; the serialized ShardDocSet envelope replaced this.
- Treating a page-sized hit list as a complete document set for graph `result_ref` or cross-range expansion — rejected via total-hit/saturation guards.
- Trusting materialized algebraic rollups under a stamped generation — they cannot subtract tombstones; stamped metric/stats/terms/date-histogram/derived-join paths use live identity candidates, and variants without an exact generation-aware path decline the fast path. Historical non-current materialized reads remain disabled.
- Pre-materializing a broad all-live-doc vector-ID filter for unfiltered dense searches — dropped; visibility left to result postprocessing.
- Automatically merging ranges with incompatible live ordinal namespaces — blocked until an explicit reassignment or rebuild lifecycle is used.

## Key measurements
100k-document runs (no date or hardware given in the source):

| Measurement | Result |
|---|---|
| `antfly-api-bench --mode handler --query-shape hybrid-filter-exclude-project` | roughly 570ms → roughly 55ms, filled `k=20` guardrail preserved |
| Public matrix (`scripts/run_db_query_matrix.py --suite public`), full-text | `~15.6ms` DB, `~16.3ms` handler |
| dense-filter | `~22ms` handler; direct DB-search `~43-107ms` |
| sparse-filter | `~126-128ms` |
| algebraic-filter | `~469ms` DB, `~196ms` handler |
| hybrid-composed | `~423ms` DB, `~149ms` handler |
| graph-expand (suspected harness gap) | `~592ms` direct DB vs `~0.4ms` handler |
| `dense-stack-bench` (`dims=64`, `k=20`, `queries=2`, `sync_level=full_index`) | pure DB `~9.9ms`, concurrent `~5.5ms`, packed C API `~15.0ms`; resource-manager peaks about `0.65MiB` / `1.68MiB` |

Conclusions drawn there: the remaining hybrid cost at 100k is building the full-text-derived doc set (a broad scored text query), not HBC; the dense-filter direct-DB outlier points at the public-query guardrail's composed filter/DB benchmark path, not HBC cache warming. Dense/HBC profile counters stayed zero in handler/local guardrail runs, so `DOCID_PERF_MATRIX_REQUIRE_SYMBOLIC_PROFILE=1` is set only for routes expected to emit them.

## Phase status at the end of the log
- Phases 1–3: storage primitives, coverage, document-set representation/fallback counters (`DBStats.doc_identity`, `DBStats.doc_set_planning`).
- Phase 4: algebraic resolved-filter boundary consumed by vector/sparse.
- Phase 5: sparse ordinal consumption, stable dense vector IDs, parent-ordinal expansion.
- Phase 6: full-text-to-ordinal projection, sidecar-based `doc_num` projection; zero-match filters resolve to an explicit empty set.
- Phase 7: public `with` bindings and `{ "ref": "name" }` resolved once per request; compatible union/intersection/difference; negated or disjunctive filters needing broader complements still fall back.
- Phase 8: health counters, graph-result doc sets, ordinal rows across algebraic postings, strict restore validation, and lifecycle hardening (split/merge cutover, strict reopen, compaction, near-`u32` pressure, mixed-version ShardDocSet fixtures, repeated ownership moves with pinned leases).
- Internal worker exchange matrix: query, vector-worker, preflight, graph expand/hydrate, search-request text stats, and explicit/background text stats carry ShardDocSet; graph edge reads, graph result refs, aggregation context/reruns, algebraic partials, distributed-join right fanout/worker/unmatched-follow-up/finalizer, and shuffle worker/finalizer validate generation-stamped projections.
