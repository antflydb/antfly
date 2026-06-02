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

Relational schemas are stricter. Top-level dynamic templates are rejected in
`storage_mode: "relational"` because the row shape must stay closed. Flexible
document-style indexing belongs behind an explicitly declared `json` column:

```json
{
  "type": "json",
  "schema": {
    "type": "object",
    "properties": {
      "title": {"type": "text"},
      "score": {"type": "numeric"}
    },
    "additionalProperties": true
  },
  "dynamic_templates": {
    "metrics": {
      "path_match": "metrics.*",
      "mapping": {"type": "numeric", "doc_values": true}
    }
  }
}
```

The embedded `schema` and `dynamic_templates` are scoped under the owning column
path. For a column named `attrs`, the example above emits runtime paths such as
`attrs.title`, `attrs.score`, and `attrs.metrics.latency`. Attaching embedded
document config to a scalar or non-`json` relational field is invalid.

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
  `dynamic_field_rule`. A rule requires a name/path selector (`match` /
  `path_match`). Unbounded templates (`text`/`html`/`search_as_you_type`),
  selector-less templates, and `match_mapping_type`-only templates are
  intentionally NOT promoted — they stay on the schemaless path-fact path. This
  is the cardinality guard that keeps a broad `match: "*"` template from
  exploding group-by cardinality, and it keeps ingest and query symmetric: a
  `match_mapping_type`-only rule could be evaluated at ingest (a value is
  present) but never at query time (no value), so it is excluded from both.
  `index.zig:validateConfig` enforces the same selector requirement so a
  hand-authored config cannot reintroduce the asymmetry.
- At ingest, `storage/db/algebraic/index.zig` evaluates the rules against every
  observed field not already covered by a static spec. Both ingest and query use
  a single shared selector evaluator (`dynamicRuleSelectorMatches`, parameterized
  by an optional value) reusing the same `globMatch` / `match_mapping_type`
  semantics as the full-text resolver in `storage/schema.zig`, so the two can
  never drift. Explicit schema fields always win over templates. The FIRST
  selector-matching rule wins (Elasticsearch dynamic-template order): a value that
  does not coerce under that rule's type simply yields no fact (exactly like a
  static typed field given a non-coercible value) — ingest does not fall through
  to a later overlapping rule of a different type.
- The dynamic fact identity is the full dotted path (top-level templates have
  path == field name). Numeric templates project group+measure, datetime
  project group+time, keyword/boolean project group.
- At query time the planner resolves a queried field against the same
  `dynamic_field_rules` (`Index.fieldConfig`/`resolveField`), so group-by, sum,
  and term aggregations over template-promoted fields route to the sidecar's
  docfact fold scan. Resolution requires that all name/path-matching rules AGREE
  on the scalar type: with one matching rule (or several that agree) query reads
  the exact type ingest stored; if overlapping rules disagree, the field is
  ambiguous and query declines (the aggregation falls back to a complete scan)
  rather than reading a type that ingest may have stored differently per
  document.

Template-only updates propagate without a recreate on two levels:

- the durable table `indexes_json` is regenerated on every schema update
  (`api/tables.zig: regenerateAlgebraicIndexesFromSchemaAlloc`), which carries
  forward user-tunable runtime knobs (adaptive policy, planner/result limits,
  and configured HLL cardinalities) so a schema change does not reset tuning or
  drop approximate-cardinality sketches; fresh opens, new replicas, and restarts
  pick up the new rules; and
- live indexes are refreshed and rebuilt in place
  (`DB.reloadAlgebraicSchemaConfigs` → `IndexManager.reloadAlgebraicSchemaConfigs`)
  so running writers apply the new rules after the local sidecar has been
  reprojected from committed base rows.

Capability changes (detected by a differing capability fingerprint) first set
`capability_lifecycle_status: "rebuild_required"` on the algebraic config so a
crash or reopen during migration falls back instead of reading schema-derived
facts that may only cover the post-change subset. The local table-schema apply
path stages that pending algebraic config before durably saving the runtime
schema and the local schema JSON mirror in one transaction, then clears the
algebraic sidecar and replays existing committed base rows through the refreshed
config. Pending rebuild completion is schema-version gated: a writable reopen
only clears `rebuild_required` when the durable runtime schema version matches
the pending algebraic capability version, and schema-versioned capabilities stay
pending if no durable runtime schema has been adopted yet. After a successful
rebuild, the catalog persists `capability_lifecycle_status: "current"`.
Dynamic-template changes also use the narrower `dynamic_rules_backfill_pending`
diagnostic/field guard while the rebuild is pending. Tables created with a
template (rather than updated) take the fingerprint-equality fast path and are
never flagged.

Relational `json` columns use the same shape at the column scope. Each embedded
JSON domain is emitted as a `json_subdocument_domains` entry with the owning
path and a capability fingerprint. Updating that column's embedded `schema` or
column-local `dynamic_templates` marks the changed domain
`lifecycle_status: "rebuild_required"` as a transition state. While pending,
algebraic field resolution withholds static and dynamic facts below that JSON
path, but unrelated top-level relational fields remain eligible for the sidecar.
Local rebuild/backfill reprojects the JSON cell from the committed relational row
and then marks the domain `current`; the schema update does not rewrite
unchanged row values.

Relational base-column changes are different: they change authoritative storage
or secondary scan layout, not just derived interpretation. Schema updates
therefore reject storage-mode switches and relational column-catalog changes
(add/remove/rename/type/nullability/indexed) until an explicit row rewrite or
secondary-index rebuild path is available. Derived-only changes below an
existing `json` column remain valid because the base row still stores the same
JSON cell.

Backup/restore follows that same boundary. Native backups preserve relational
physical rows and secondary scan entries as a snapshot. Portable logical backups
are not currently schema-aware for relational tables; they must either reject
relational physical rows or, in a future implementation, materialize packed rows
through the schema and restore through the relational write path.

## Related Docs

- [TODO.md](/Users/ajroetker/go/pkg/antfly/src/github.com/antflydb/antfly-zig/TODO.md)
- [SERVERLESS.md](/Users/ajroetker/go/pkg/antfly/src/github.com/antflydb/antfly-zig/SERVERLESS.md)
