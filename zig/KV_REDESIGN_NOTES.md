# KV redesign — handoff notes (Phase B, KV/durability copy)

Status: NOT STARTED in code. Investigation only. Tree clean at commit 3551602c
(segment blob-write removal, already pushed to claude/antfly-document-store-schema-19xGk).

## Goal
Make typed columns authoritative for the KV/durability copy of relational
documents, eliminating the JSON blob stored in the KV store. Today the segment
stored-doc blob is already column-derived (commit 3551602c), but the KV value
(`db.get`/`getStoreValue`, keyed by doc key) is still the authoritative JSON
blob, read SYNCHRONOUSLY by transforms and vector/dense materialization.

## Key code seams (verified by reading db.zig @ commit 3551602c)
- Write: db.zig ~3376 — `strippedStoredDocumentValueAlloc(cleaned, vector_store_field_names, ...)`
  produces `store_value`; keyed by `internal_keys.documentKeyAlloc(write.key)`;
  appended to `store_writes`. This is the authoritative KV blob (vector fields stripped).
- Read: `DB.get` (db.zig:4223) -> `encodeStoreLookupKeyAlloc` -> `core.getStoreValue`.
  `DB.get` is GENERIC and SCHEMA-AGNOSTIC (also serves artifacts, metadata,
  timestamps, group keys). Consumers of the doc value:
    - `lookup`/`getDocument` (4277) -> `projectLookupStoredBytes`
    - transforms `stageTransform` (db.zig:4000-4016): reads `db.get(transform.key)`
      for read-modify-write, then `transform_mod.resolveDocumentTransform`.
    - vector/dense hit materialization (~db.zig:38939).

## Architectural constraint (the hard part)
The KV store is the SYNCHRONOUS source of truth; search "segments" (typed
columns) are built ASYNC/batched. So a sync transform read right after a write
cannot be served by reconstructing from segments. The doc's "scope decision"
(RELATIONAL.md ~320-335) flags removing the KV blob as a separate architecture
task needing a consistency-model redesign.

## Two candidate designs (decide before coding)
A) SELF-DESCRIBING TYPED-ROW AS THE KV VALUE (recommended, lower risk):
   - Relational KV value becomes a compact self-describing typed-row encoding
     (magic prefix + per-field {path, value_type, is_json, value}), NOT JSON.
     Mirrors the segment relational_manifest approach (no schema lookup needed
     on read).
   - Write: detect relational table, project doc -> typed row -> encode as KV value.
   - Read: at the doc-value seam, detect magic prefix -> reconstruct JSON ->
     return. Keeps `db.get` consumers (transform/vector/lookup) unchanged.
   - Preserves synchronous semantics (row written in same batch). Win: no JSON
     text; typed, smaller. Does NOT achieve true single-copy (KV row + segment
     columns still separate physical copies) but is safe + incremental.
   - Risk: `db.get` is generic; reconstruction must be doc-key-aware (only
     document keys, not artifacts/metadata) AND only relational ones. Use a
     magic-prefix sniff on the value so non-relational/other keys pass through
     untouched. Round-trip fidelity: relational schema is CLOSED
     (additionalProperties:false) so every field is a declared column -> typed
     row fully captures the doc -> transforms round-trip safely.
   - Reuse: `document_mapper` already has the reconstruction
     (`reconstructRelationalDocumentAlloc`) and projection
     (`buildRelationalTypedFields` / schema_capability.projectRelationalRowAlloc).
     Could reuse the segment leaf module `section/relational_manifest.zig`
     encoding for a per-doc row too.

B) SEGMENTS AS THE SINGLE STORE (big, risky, true single-copy):
   - Make the columnar store synchronous (memtable for columns) so sync reads
     hit it; drop KV doc blob entirely. Requires consistency-model redesign of
     the write/index pipeline. Out of scope per doc; high blast radius on a
     40k-line db.zig.

## Recommendation
Implement (A). It is the natural extension of the segment work, testable in
isolation, and avoids gambling the engine's consistency model. Confirm with the
user whether (A)'s "still two physical copies" is acceptable vs. they expected
(B)'s true single-copy — the answer materially changes the work.

## Open items to verify before coding (A)
- Exact location/signature of `core.getStoreValue` and
  `strippedStoredDocumentValueAlloc` / `projectLookupStoredBytes` (greps were
  flaky during this session due to a temp-fs/output-channel issue; re-confirm).
- How the write path knows the table+schema for a doc key (it already computes
  relational columns in batch — thread the same into store_value encoding).
- Whether `getStoreValue` is also used for non-document internal keys that could
  collide with the magic prefix (use a NUL/sentinel prefix unlikely in JSON).
- Vector field stripping interaction: today store_value strips vector fields;
  the typed-row must represent the same stripped doc.
