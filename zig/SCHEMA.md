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

It is versioned and persisted in [go/pkg/antfly/src/storage/schema.zig](/Users/ajroetker/go/pkg/antfly/src/github.com/antflydb/antfly-zig/go/pkg/antfly/src/storage/schema.zig).
Compilation lives in [go/pkg/antfly/src/schema/mod.zig](/Users/ajroetker/go/pkg/antfly/src/github.com/antflydb/antfly-zig/go/pkg/antfly/src/schema/mod.zig).

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

This ordering is enforced in [go/pkg/antfly/src/storage/db/document_mapper.zig](/Users/ajroetker/go/pkg/antfly/src/github.com/antflydb/antfly-zig/go/pkg/antfly/src/storage/db/document_mapper.zig).

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

## Dynamic Templates and the Algebraic Index

Dynamic templates feed the algebraic sidecar as well as the full-text index, so
a template-matched field is promoted into typed group/measure/time docfacts at
ingest time — Elasticsearch-style runtime-adaptive mapping — without a schema
version bump or a reindex.

- `schema/mod.zig` compilation lowers each bounded template
  (`keyword`/`link`/`numeric`/`boolean`/`datetime`) into a capability
  `dynamic_field_rule`. Unbounded templates (`text`/`html`/`search_as_you_type`)
  and templates with no positive selector (`match` / `path_match` /
  `match_mapping_type`) are intentionally NOT promoted — they stay on the
  schemaless path-fact path. This is the cardinality guard that keeps a broad
  `match: "*"` template from exploding group-by cardinality.
- At ingest, `storage/db/algebraic/index.zig` evaluates the rules against every
  observed field not already covered by a static spec, reusing the same
  `globMatch` / `match_mapping_type` semantics as the full-text resolver in
  `storage/schema.zig`. Explicit schema fields always win over templates.
- The dynamic fact identity is the full dotted path (top-level templates have
  path == field name). Numeric templates project group+measure, datetime
  project group+time, keyword/boolean project group.
- At query time the planner resolves a queried field against the same
  `dynamic_field_rules` (`Index.fieldConfig`/`resolveField`), so group-by, sum,
  and term aggregations over template-promoted fields route to the sidecar's
  docfact fold scan. Only rules carrying a name/path selector (`match` /
  `path_match`) resolve a concrete query field; `match_mapping_type`-only rules
  can't be evaluated without a value and so do not resolve at query time.

Template-only updates propagate without a recreate on two levels:

- the durable table `indexes_json` is regenerated on every schema update
  (`api/tables.zig: regenerateAlgebraicIndexesFromSchemaAlloc`), so fresh opens,
  new replicas, and restarts pick up the new rules; and
- live indexes are refreshed in place
  (`DB.reloadAlgebraicSchemaConfigs` → `Index.reloadConfigJson`) so running
  writers apply the new rules immediately. Existing documents are reconciled
  lazily — only new or rewritten documents reflect a changed template until a
  full rebuild — matching the no-reindex contract.

## Related Docs

- [TODO.md](/Users/ajroetker/go/pkg/antfly/src/github.com/antflydb/antfly-zig/TODO.md)
- [SERVERLESS.md](/Users/ajroetker/go/pkg/antfly/src/github.com/antflydb/antfly-zig/SERVERLESS.md)
