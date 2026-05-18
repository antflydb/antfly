# Schema

This repo now has a real split between:

- Public schema contract: JSON Schema plus Antfly extensions and dynamic templates
- Versioned runtime schema: compiled execution metadata used by indexing, reopen, and query-time helpers

## Public Contract

The table schema surface is Go-compatible by design.

Document-schema fields may use:

- `x-antfly-types`
- `x-antfly-analyzer`
- `x-antfly-index`
- `x-antfly-include-in-all`
- `patternProperties`
- `additionalProperties`

Table-level dynamic templates may use:

- `match`
- `unmatch`
- `path_match`
- `path_unmatch`
- `match_mapping_type`
- `mapping.type`
- `mapping.analyzer`
- `mapping.index`
- `mapping.store`
- `mapping.include_in_all`
- `mapping.doc_values`

## Runtime Model

The runtime schema is the source of truth for execution.

It is versioned and persisted in [src/storage/schema.zig](/Users/ajroetker/go/src/github.com/antflydb/antfly-zig/src/storage/schema.zig).
Compilation lives in [src/schema/mod.zig](/Users/ajroetker/go/src/github.com/antflydb/antfly-zig/src/schema/mod.zig).

The compiled model currently carries:

- explicit full-text fields
- compiled dynamic rules for `patternProperties`
- compiled dynamic rules for `additionalProperties` schema objects
- open dynamic text subtrees for `additionalProperties: true`
- open dynamic infer-types subtrees for object-scoped
  `x-antfly-dynamic-indexing.mode = "infer_types"`
- resolved dynamic-template selectors and mapping options

When no schema is present, Zig now follows a separate permissive path that is
meant to behave like Go/Bleve dynamic inference by default:

- nested string fields are indexed recursively for full-text search
- nested numeric, boolean, datetime, and geopoint fields are inferred
  recursively for typed queries
- nested objects and arrays are traversed instead of being limited to
  top-level fields

When a schema is present, the compiled runtime schema remains the source of
truth and the explicit `additionalProperties` policy below applies.

## TTL Semantics

TTL is intentionally close to Go, but not identical today.

Shared behavior:

- the runtime schema carries `ttl_duration_ns` and `ttl_field`
- query-time visibility filtering uses stored write-time metadata
- background TTL cleanup deletes expired documents by stored timestamp

Current Zig difference from Go:

- Go's schema contract treats the configured TTL field as required when TTL is enabled
- Go's schema-level TTL parsing expects string timestamps in RFC3339 or RFC3339Nano
- Zig is currently more permissive
  - if the configured `ttl_field` is missing from a write, Zig falls back to the batch/write timestamp
  - Zig accepts integer nanoseconds and numeric strings in addition to RFC3339 strings

This means Zig currently favors operational robustness over strict schema
enforcement for TTL-bearing writes. That is a deliberate documented difference
for now, not an accident.

With a schema present, `additionalProperties` has an explicit policy split:

- `additionalProperties: true` means open text fallback for unknown string fields only
- `additionalProperties: { ...schema... }` means explicit typed dynamic behavior compiled into runtime rules
- `additionalProperties: true` plus
  `x-antfly-dynamic-indexing: { "mode": "infer_types" }` means open dynamic
  inference for unknown nested fields under that object, including text,
  numeric, boolean, datetime, and geopoint detection

## Full-Text Rules

Explicit schema fields can expand into multiple emitted fields.

- primary `text` / `html` keeps the original field name
- `x-antfly-analyzer` applies to that primary explicit field
- `keyword` / `link` variant becomes `field__keyword` when a primary text field exists
- `search_as_you_type` variant becomes `field__2gram`
- `search_as_you_type + keyword` without a primary type auto-adds text semantics
- only the primary field participates in `_all`
- generated `__keyword` and `__2gram` variants keep their fixed analyzers

Dynamic templates do not use schema-style field expansion.
They are single mappings, matching Go's Bleve-template behavior.

That means:

- explicit fields and schema-driven dynamic rules can emit `__keyword` / `__2gram`
- dynamic templates emit a single field at the original path

## Resolution Order

Text indexing currently resolves dynamic fields in this order:

1. explicit compiled field plan
2. compiled dynamic rule from `patternProperties` / `additionalProperties` schema objects
3. dynamic template match
4. schema-present `infer_types` fallback for opted-in open dynamic objects
5. open `additionalProperties: true` text fallback

This ordering is enforced in [src/storage/db/document_mapper.zig](/Users/ajroetker/go/src/github.com/antflydb/antfly-zig/src/storage/db/document_mapper.zig).

Query-time analyzer resolution now uses the same compiled runtime schema for
explicit fields and compiled dynamic rules when a `match` or `match_phrase`
query does not specify an analyzer explicitly.

For dynamic-template fields, especially templates gated by
`match_mapping_type`, the text index now persists observed field-to-analyzer
bindings at index time and reloads them on reopen. That gives query-time
`match` / `match_phrase` exact analyzer inference for stable dynamic field
paths instead of falling back to field-name-only schema heuristics.

The first debug surface for this compiled plan now hangs off the existing admin
table and index detail routes via `?debug=runtime_schema`.

## Related Docs

- [TODO.md](/Users/ajroetker/go/src/github.com/antflydb/antfly-zig/TODO.md)
- [SERVERLESS.md](/Users/ajroetker/go/src/github.com/antflydb/antfly-zig/SERVERLESS.md)
