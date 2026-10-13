# Algebraic Sparse-Token Database Theory

This note sketches a database design where records are represented as sparse
symbolic tokens, query plans compile into algebraic transforms, and aggregations
are evaluated as folds over algebraic structures chosen for each operator.

The core idea is not to force database operations into group theory alone.
Groups are useful for reversible updates, but ordinary database work also needs
monoids, semirings, lattices, vector spaces, and sparse tensor operations.

The serving contract for remote Parquet/Iceberg materializations, exact SQL state
interchange, coverage proofs, and invalidation is described in
[REMOTE_TABLE_SERVING.md](REMOTE_TABLE_SERVING.md). Its implementation status is the
readiness gate for those capabilities.

> **Relocated:** Extended derivations, worked tokenization and temporal-cylinder
> examples, the end-to-end COUNT/MAX maintenance example, and the original
> prototype sketch → [docs/design/algebraic/theory.md](../docs/design/algebraic/theory.md).

## Thesis

```text
database state: sparse vector / formal sum of tuples
query plan:      composition of algebraic transforms
aggregation:     fold into a monoid, group, semiring, or lattice
updates:         delta vectors
```

The query engine's intermediate representation is an algebraic program over
sparse formal vectors. Each operator advertises the algebraic laws the optimizer
may use for incremental execution, factorization, and physical layout selection.
The engine is algebra-polymorphic: each query fragment declares the structure it
actually needs.

## Sparse Tuple Space

- A logical tuple is a basis token, e.g. `basis("users", id=1, country="US")`.
- A table is a sparse formal sum of basis tokens: `Users = 1*t1 + 1*t2 + ...`.
- The physical form is a sparse map `coordinate_id -> coefficient`; each token
  is an implicit basis vector.
- Inserts and deletes are delta vectors: `Users' = Users + (+1*t3 - 1*t1)`.

Coefficient interpretation is part of the semantics:

```text
+1 = fact exists
-1 = deletion delta
n  = multiplicity or count
w  = weight, probability, or score
```

Integer coefficients make insert/delete maintenance group-like (additive
inverses). Natural-number coefficients give multiset semantics. Floating
coefficients represent weights or probabilities and may weaken exact relational
guarantees. Coefficients may also carry provenance or authorization labels.

## Creating Sparse Tokens and Vectors

Tokens come in three layers:

```text
row token:       exact identity           basis("users.row", id=1)
attribute token: queryable atomic facts   basis("users.country", row=1, value="US")
feature token:   derived/index/search     basis("age_bucket=30_39")
```

- A token represents one atomic fact the engine can move, combine, delete, or
  index independently. Opaque row tokens alone are insufficient because
  `GROUP BY`, `WHERE`, `JOIN`, `MIN`, and `MAX` operate on attributes.
- Attribute tokens are the primary representation for relational execution.
- Feature tokens are derived from row or attribute tokens; they never replace
  them.
- A canonical token includes namespace/table, attribute, row or entity
  identity, value, type tag, and optional version or time, encoded as a
  deterministic byte string such as `users.country|row=1|type=string|value=US`.
- The compact coordinate is `hash128(canonical_token)`. A symbol table
  `coordinate_id -> canonical token` is retained for exactness, debugging,
  reversibility, and collision handling.

## Temporal Analytics and Cylinders

A cylinder is a key-space selector crossed with a time region and a measure
algebra:

```text
Cylinder = KeyBasis x TimeRegion x MeasureAlgebra

raw facts:       point/event tokens   (exact history, late data, recomputation)
temporal index:  interval or bucket tokens
rollups:         cylinder tokens      (materialized analytical folds)
```

- Cylinders are a planning and materialization concept; raw point facts stay
  canonical.
- Time regions are regular buckets (minute, hour, day, month) for rollups and
  pruning, or arbitrary intervals for window predicates.
- An arbitrary interval is answered as complete cylinders plus boundary scans,
  optionally with smaller-grain cylinders.
- Each cylinder declares its measure algebra and support state so the optimizer
  knows whether updates are reversible, monotone, approximate, or require
  recomputation. `AVG` is the product algebra `(sum, count)`.
- A new event updates every materialized cylinder covering its key and time;
  invertible cylinders apply inverse deltas for deletes and corrections.
  Non-invertible cylinders (`MAX`, `PERCENTILE`) need retained support state or
  affected-bucket recomputation.

## Queries as Algebraic Operators

Queries compile into maps over sparse formal sums:

```text
filter_p(t)  = t if p(t), 0 otherwise
project(t)   = basis in a target space (e.g. country_age(country, age))
fold_by(key, value, combine)  -- e.g. GROUP BY country COUNT(*):
             basis(users, id, country, age) -> basis(group, country) with value 1,
             combine = +
```

The result of a fold is another sparse vector whose basis token identifies the
group and whose coefficient stores the aggregate: `3*basis("US") + 2*basis("DE")`.

## Aggregates and Algebraic Requirements

```text
COUNT: additive monoid, or additive group when deletes are required
SUM:   additive monoid/group
AVG:   product of (SUM, COUNT)
MIN:   meet-semilattice (identity +infinity)
MAX:   join-semilattice (identity -infinity)
JOIN:  semiring-like multiplication and addition
DISTINCT / PERCENTILE / TOP-K: set, sketch, histogram, or heap support state
```

- Group aggregates (`SUM`, `COUNT`) maintain deletes with inverse deltas.
- Semilattice aggregates (`MIN`, `MAX`) are associative, commutative, and
  idempotent but have no inverse: inserts merge directly
  (`new_max = max(old_max, x)`); a delete of the current extremum requires
  retained support state (ordered multiset, count per value, top-k, heap plus
  tombstones) or recomputation of the affected group.
- The invertibility of each aggregate is visible in the query IR.

## Joins as Contractions

A join is a sparse tensor contraction over the shared key:

```text
Orders[user_id, order_id, amount]
Users[user_id, country]
Join = contraction over user_id -> basis("joined", country, amount)
```

Joins admit the delta (product) rule, which makes them candidates for
incremental execution:

```text
Delta(A join B) = Delta(A) join B + A join Delta(B) + Delta(A) join Delta(B)
```

## Engine Architecture

### Algebraic IR

The compiler lowers SQL, Datalog, or other query languages into `map`, `filter`,
`project`, `join`/`contract`, `group`/`fold`, `union`, and `difference`
operators. Each operator declares the laws it relies on: associative,
commutative, idempotent, has identity, has inverse, distributes over another
operator, monotone. The optimizer reorders, factors, caches, or incrementally
maintains computations only through declared laws.

### Physical Representation

The sparse model compiles to hash maps from basis keys to coefficients,
compressed bitmaps, CSR/COO matrices, tries over tuple keys, factorized join
indexes, columnar segments, and ordered maps for min/max support. Layout is
chosen from sparsity, cardinality, update rate, query shape, and the operators'
algebraic requirements.

### Incremental Maintenance

```text
linear fragments:             Q(D + DeltaD) = Q(D) + Q(DeltaD)
joins:                        product rule above
group aggregate with inverse: apply delta directly
monotone semilattice:         merge new evidence
non-invertible delete:        use support state or recompute affected group
```

Under this model a materialized view is a cached expression result, an index is
a chosen sparse basis layout, incremental update is the expression derivative
over deltas, and query optimization is law-preserving rewrite.

## Antfly Prototype

The algebraic index is a derived sidecar, not a replacement for the core storage
or query path. It reuses document-first writes, the sequence-based derived log,
per-index replay and watermarks, and query-time aggregation fallback. The
module lives under `zig/pkg/antfly-embedded/src/storage/db/algebraic/` (token,
vector, algebra, cylinder, index, planner, plus fact, path, join, law, lexical,
distributed, adaptive, HLL, and planner-control files).

### Derived Target

`algebraic_tokens` is a derived target beside `full_text`, `dense_vector`,
`sparse_vector`, and `graph`:

```text
document write
  -> parse base JSON once
  -> commit base document
  -> append derived record
  -> algebraic worker tokenizes JSON into deltas
  -> algebraic index updates token and cylinder state
```

- Insert, overwrite, and delete deltas derive from the cleaned base document and
  the changed/deleted document keys.
- Query correctness never depends on background execution; rebuild from the
  docstore is the fallback for missed or incompatible algebraic state.

### Tokenization

- Each document yields a row token plus typed field tokens, e.g.
  `field|table=orders|field=customer|doc=100|type=string|value=alice`.
- Coordinates are `hash128(canonical_token)` with a retained symbol table.
- State uses a backend key-value layout, not the embedding sparse index; the
  sparse index is prior art for sorted dimensions, posting chunks, and split
  planning only.

### Schemaless and Late-Typed Facts

Antfly stays schemaless by default; algebraic indexing does not require a
declared schema. The model is two-tiered:

```text
universal schemaless facts:
  fact(doc{}, path{}, kind{}, value{}) -> 1

typed/materialized projections:
  docfact(role{}, field{}, scalar{}, doc{}) -> 1
  postings(field{}, analyzer{}, term{}, doc{}) -> freq/norm/positions
  embedding(doc{}, dim[]) -> float
  sparse_embedding(doc{}, token{}) -> weight
  cylinder(bucket{}, group{}, metric{}) -> value
```

- Every JSON document projects into path/kind/value facts, e.g.
  `fact(doc=o1, path="meta/tier", kind=string, value="gold")`; objects and
  arrays emit `kind=object|array, value=""`.
- The empty object/array value is a structural presence marker for bounded
  `exists` and delete compensation; planners never treat it as a scalar term.
- Schemas, inferred path profiles, query observations, or explicit index configs
  promote selected paths into typed projections.
- Mixed-kind paths (`amount=20`, `amount="20"`) require kind-qualified
  semantics, an explicit coercion policy, a verification path, or fallback. A
  numeric range may use only `kind=number` facts when that matches the public
  query contract.
- For schema-declared `docfact` fields, parseable JSON strings are canonicalized
  through the declared integer/number type; malformed strings are omitted from
  the typed projection and remain available through schemaless path facts and
  fallback.

```text
schemaless ingest creates generic facts
observed path profiles describe possible typed interpretations
declared schemas pin ambiguous paths
adaptive materializations only promote proven-safe typed shapes
fallback remains available for unknown or ambiguous semantics
```

### Lexical Tensor Access Paths

Full-text, algebraic scalar lookup rows, sparse vectors, dense vectors, and
graph layouts are physical access paths under one typed sparse-tensor query IR,
not separate engines with separate semantics. The term dictionary (FST lookup,
range iteration, automaton search) implements label selection for mapped
string dimensions such as `term{}` or `scalar{}`:

```text
term:              slice(postings, field=body, term="cat") -> tensor(doc{})
prefix/term_range: range-select lexicon(field{}, term{})
                   join with postings on term{}; reduce(or, term{}) -> tensor(doc{})
wildcard/regexp:   automaton-select lexicon(field{}, term{}); same join/reduce
```

- The algebraic index does not embed a second search engine. BM25,
  positions, phrase/proximity, highlighting, and analyzer-specific ranking stay
  in the full-text layout; algebraic planning consumes its candidate tensors
  and continues with exact filters, vector pruning, joins, or folds.
- Analyzed text terms and canonical scalar facts are distinct dimensions:
  `postings(field{}, analyzer{}, term{}, doc{})` versus
  `docfact(role{}, field{}, kind{}, scalar{}, doc{})`. They may share FST
  machinery, never meaning.
- A lexical access path is selected only when field, analyzer, kind, and
  coercion rules prove the candidate tensor has the requested semantics.

Dictionary identity is a storage-level ownership contract:

```text
dictionary_identity =
  table or index scope
  field or JSON path
  label kind: analyzed_term | canonical_scalar | sparse_token | graph_label
  analyzer or canonicalization version
  value kind and coercion policy
```

- Exactly one dictionary/postings owner exists per `dictionary_identity`
  (full-text, algebraic path promotion, sparse-vector, or graph); the registry
  row is the shared source of truth and other planners consume the advertised
  access path.
- Two physical owners for the same label space (e.g. a full-text FST and an
  algebraic FST for `body`/default analyzed terms) is a forbidden duplicate and
  a correctness risk.
- Separate dictionaries exist only for distinct semantics; a consumer that
  cannot prove an exact identity match falls back or creates a dictionary for
  its own distinct dimension.

### Materialized Expressions

The sidecar materializes declared tensor expressions, not ad hoc result
families. A materialized expression is identified by a stable semantic
fingerprint of the expression fragment, declared law, dimensions, metadata,
dictionary identity where relevant, and the owning algebraic index:

```text
materialized_expr:<expr_id>:<axis_key> -> law state
minmax:<expr_id>:<axis_key>:<measure_value> -> count-law support
```

- `COUNT`, `SUM`, and `AVG` (`{sum, count}`) rows receive direct law deltas;
  deletes apply the inverse.
- `MIN`/`MAX` result rows hold the current boundary; `minmax` support rows count
  each value. A delete decrements support; when the current extremum's count
  reaches zero the index finds the next supported value or marks the group for
  bounded recomputation.

### Temporal Expressions

The time bucket is another expression axis:
`materialized_expr:<expr_id>:time=<bucket_start>:group=<axis_key> -> law state`.
Supported bucket kinds are hour, day, and month. Arbitrary intervals combine
complete materialized buckets, boundary document scans, and optional
smaller-grain buckets.

### Query Planning

The original routing gate admitted only single-table aggregations with no joins
or graph expansion, a supported filter subset, supported group and measure
fields, a supported aggregate law, and an optional aligned `date_histogram`
bucket. Everything else fell back to the existing search/aggregation path. The
fallback rule still holds, but the gate is now proof-based: see
[Planning And Execution](#planning-and-execution) and
[Planner Ownership](#planner-ownership). The physical row layout is listed in
[Current Implementation](#current-implementation).

> **Relocated:** The prototype bootstrap milestone list, demonstration query
> set, and success criteria are preserved verbatim in
> [docs/design/algebraic/history/prototype-and-capability-log.md](../docs/design/algebraic/history/prototype-and-capability-log.md).

### Risks

The main design risks are:

```text
token cardinality explosion
schema drift and mixed field types
overwrite/delete correctness
late-arriving temporal events
non-invertible aggregate maintenance
planner accidentally choosing the sidecar for unsupported semantics
symbol-table and hash-collision handling
distributed/sharded aggregate merge semantics
```

The mitigation is to keep the index opt-in, schema-constrained, and
planner-gated, and to prove that derived-log replay maintains algebraic
materializations correctly before generalizing the query IR. Additional bounded
query shapes extend the typed IR, materialized-expression IDs, tensor-program
envelopes, and proof-gated access paths only when they preserve exact semantics
and have benchmark evidence; unsupported text, join, vector, graph, and
schemaless shapes stay on explicit fallback paths.

## Current Implementation

The local algebraic sidecar lives under:

```text
zig/pkg/antfly-embedded/src/storage/db/algebraic/
```

It is opt-in through `.algebraic` index configs, maintained through derived
replay, and planner-gated so unsupported or unhealthy shapes fall back to the
existing aggregation path. Base documents remain canonical. Algebraic state is
derived, rebuildable, and scoped to prefixed sidecar keys.

> **Relocated:** The line-per-capability changelog that used to list everything
> implemented is preserved verbatim in
> [docs/design/algebraic/history/prototype-and-capability-log.md](../docs/design/algebraic/history/prototype-and-capability-log.md).
> The grouped summary below is the current reference.

Implemented capability, by area:

- **Encoding and laws.** Canonical tuple/scalar tokens with symbol-id backed
  keys; typed canonicalization for string, integer, number, bool, time, and
  bytes; a law registry for group, monoid, lattice (including set-union), and
  semiring descriptors with explicit semiring product hooks; proof-gated laws
  for aggregate, MIN/MAX support, distributed partial, and graph/path
  capabilities; law-compatible tensor row merge.
- **Facts and materializations.** `docfact`, `pathfact`, `path_lookup`,
  `path_profile`, `joinfact`, and `docjf` rows; direct COUNT/SUM/AVG/MIN/MAX
  and sumsquares tensors with exact MIN/MAX support rows; hour/day/month
  cylinders; overwrite/delete compensation from persisted fact rows.
- **Joins.** Configured composite equi-joins with explicit document roles and
  side gating, side projection limited to required fields, temporal bucket or
  window matching with range-pruned scans, strict materialization matching
  unless the query names the materialization or the join fold is declared safe
  for implicit matching.
- **Planning.** Typed IR for metric, bucket, constraint, and join shapes; root
  metric, terms (including multi-field composite), date_histogram, histogram,
  range, stats, cardinality, constrained rollup, and nested metric routing;
  pathfact predicate constraints; bucket-limit, scan-budget, health, and schema
  lifecycle gating; explicit fallback/rejection for unsupported ranking,
  non-proven traversal, arbitrary joins, unsupported constraints, and
  mixed-kind/coercion cases.
- **Schema.** Schema-derived v2 configs with declared laws and adaptive
  defaults; capability fingerprints and skipped-unbounded-field metadata;
  public `type: "algebraic"` accepted only with `derive_from_schema: true`, with
  internal materialization fields stripped or rejected. Bounded dynamic
  templates (keyword/numeric/boolean/datetime) compile into capability
  `dynamic_field_rules`; accepted template changes advance the schema
  generation, refresh durable and live configs (`Index.reloadConfigJson`), and
  gate acceleration until a generation-local completion marker proves rebuild
  coverage. Unbounded text templates stay on the schemaless path-fact path.
- **Adaptive.** Persisted query-shape observations, recommendations,
  candidates, decision history, and progress rows; deterministic
  materialization IDs from canonical recommendation hashes; engine-owned
  validated lifecycle transitions (observing, recommended, backfilling, ready,
  stale); bounded backfill from `docfact` rows; cost/savings-aware
  dematerialization; schemaless path promotion with dictionary-backed lookup
  rows; recovery from interrupted backfill, schema drift, and dictionary-owner
  conflicts.
- **Lexical.** A semantic dictionary registry that prevents duplicate
  FST/postings publication for equivalent analyzed-term, canonical-scalar,
  sparse-token, and graph-label identities.
- **Bulk ingest.** Fact-only append-only fast path; cursor-capable LSM write
  batches for non-append ingest; ready-adaptive maintenance-plan caching;
  promoted FST rebuilds deferred to session finish; final mutable state
  direct-ingested as a sorted run when direct bulk ingest is enabled and no
  immutable flush is pending.
- **Distributed.** Canonical-token merge keys; tensor-program/access-path
  envelopes for configured aggregates, `docfact`/`pathfact`/derived-join folds,
  and root, terms, range, and histogram cardinality partials; production
  distributed aggregation selects tensor programs centrally before provisioned
  or hosted fanout and falls back to the normal planner when no program can be
  proven; the internal partial protocol rejects named-materialization and
  cardinality-only bodies.
- **Vector and graph.** Native dense pruning (doc IDs → HBC vector IDs) and
  sparse pruning (doc-num filtering during accumulation) for safe symbolic
  constraints; proof-gated `native_doc_id_constraints` and
  `graph_target_constraints` inputs; internal shard-query wire fields for native
  include/exclude candidate sets; required symbolic vector filters fail closed
  on stale lifecycle; distributed graph expand/get-edges carry tensor-program
  envelopes plus access-path proofs.
- **Observability.** Planner estimates, last decision, fallback, lifecycle,
  dictionary ownership, vector symbolic-filter, and graph semiring
  traversal counters in internal status and benchmark JSONL; tensor
  accumulator memory under the `algebraic.tensor_accumulators`
  resource-manager slice.

Planner cost guards are deliberately conservative. `max_result_buckets` bounds
returned algebraic buckets, while `max_planner_scan_rows` bounds sidecar row
scans for rollups, terms, and date histograms. If the sidecar would need to scan
more rows than the configured budget, the planner records an explicit fallback
reason and lets the existing aggregation path answer the request. Nested
cardinality acceleration has an independent `max_cardinality_cache_bytes`
working-set limit (32 MiB by default); exhausting it releases the cache and
falls back instead of allowing query memory to grow with the full corpus.

The current storage model is intentionally direct and debuggable. It is a
schema-derived algebraic sidecar with a first schemaless fact substrate and
shared dictionary registry, while base documents remain the canonical source:

```text
docfact:<doc_key>                   -> explicit typed symbolic fact list
docjf:<doc_key>:<join>:<side>:<fact_key> -> document-local join fact reference
pathfact:<doc_key>                  -> universal schemaless path/kind/value fact list
path_profile:<path>                 -> observed kinds, cardinality, parse/coercion stats, token stats
path_lookup:<path>:<kind>:<value>:<doc_key> -> exact schemaless candidate lookup
observe:<shape>                     -> persisted query-shape observation count/reason/recommendation
materialization-state:<recommendation> -> scannable adaptive lifecycle state for a recommended materialization
adaptive-candidate:<recommendation> -> persisted rank/decision/materialization id for adaptive work
adaptive-decision-history:<recommendation>:<generation> -> persisted adaptive policy decision audit row
adaptive-progress:<recommendation>  -> persisted adaptive backfill/dematerialization progress
materialized_expr:<expr_id>:<axis_key> -> cached law-declared tensor expression result
minmax:<expr_id>:<group>:<value> -> count-law support tensor for delete-safe extrema
joinfact:<join_name>:<side>:<fact_key> -> projected left/right contribution facts
lexicon:<dictionary_identity>:<label> -> owned/shared dictionary metadata for that identity
lexicon_fst:<dictionary_identity>   -> rebuildable FST artifact only for the registry owner of row lexicons
postings:<dictionary_identity>:<label>:<doc_key> -> owned/shared candidate/posting payload for that identity
symbol rows                        -> canonical token to compact id mapping
status rows                        -> health, replay, and instrumentation data
```

Configured direct, temporal, adaptive, schemaless, and join materializations
all read and write through `materialized_expr` rows plus declared support
tensors where a law needs delete-safe support state.
The long-term storage model should make the typed sparse-tensor IR broad enough
that all reusable layouts below are exposed as first-class access paths rather
than aggregate-specific helpers:

```text
docfact/pathfact rows       -> symbolic sparse tensor leaves
lexicon/postings/FST rows   -> reusable lexical tensor access paths
materialized_expr rows      -> cached law-declared tensor expression outputs
vector/sparse/graph layouts -> advertised physical tensor access paths
```

These rows should remain derived and rebuildable. Declared schemas can pin
`path` to typed `field` roles. Without a declared schema, path profiles and query
observations decide which paths are safe to promote into typed projections or
adaptive materializations.

The join implementation is deliberately configured, not a general SQL join
planner. V1 joins are composite equi-joins with optional temporal bucket or
window constraints. Join materializations declare which side supplies measures
and which side supplies group fields, so the engine does not assume one fixed
orders-to-dimensions shape.

## Performance Coverage

Algebraic correctness belongs to the normal DB, API, metadata, and graph suites.
The existing `antfly-unit-test` aggregate retains the dynamic-template and
cardinality-cache checks. Planner ownership is checked by an ordinary DB test.
There are no separate algebraic test targets or matrix scripts.

The shared DB benchmark tools (`storage_bench analytics`, `storage_bench
summary`, and `scripts/run_db_query_matrix.py --suite analytics`) retain
aggregation, adaptive materialization, cold/warm reads, joins, graph traversal,
and no-schema/schema-only/algebraic public-query comparisons. Commands,
defaults, and pass-through options (`--analytics-arg`, `--public-arg`,
`--summary-arg`, `--analytics-docs`, `--public-docs`, `--baseline`) are in
[BENCHMARKS.md](BENCHMARKS.md#db-query-comparisons). Performance limits should
come from representative measurements; tiny verifier fixtures test the
comparison tool and do not establish production baselines.

## Implementation State

This section records what the current prototype already implements. The roadmap
below is intentionally forward-looking and should not repeat these completed
notes or carry design rules that future implementation must preserve.

### Tensor Expression Storage

Configured aggregate state uses the `materialized_expr`/`minmax` rows defined in
[Materialized Expressions](#materialized-expressions). Direct count, sum, avg,
sumsquares, temporal buckets, and support-backed min/max all mutate through
tensor/law helpers.

`materialized_expr` is also the cache contract for configured materializations,
ready adaptive materializations, promoted schemaless path folds, and configured
join folds. Expression fingerprints carry semantic identity separate from the
physical access-path owner, so same-shaped folds over different source fields,
join semantics, or laws cannot collide.

### Fact Substrates

`docfact` is the schema-derived typed fact substrate. `pathfact`, `path_lookup`,
and `path_profile` provide the schemaless JSON-pointer substrate with typed
kind/value facts, mixed-kind/profile signals, and exact candidate lookups.
Deletes and overwrites reverse prior state through stored facts instead of
stored document snapshots.

`joinfact` and `docjf` provide the join substrate. Join facts retain side-local
group, measure, and time facts; `docjf` records per-document references so
updates and deletes can reverse only the affected join contributions.

### Planning And Execution

The current planner can route exact metric, terms, date-histogram, histogram,
range, and date-range folds through algebraic execution when declared laws,
access paths, lifecycle state, and constraints prove the request exact. Exact
`stats` is represented as the component bundle of `avg`, `min`, `max`, and
`sumsquares`; exact cardinality uses canonical distinct-value partials rather
than a sketch.

The typed IR includes tensor dimensions, fragments, physical access-path proofs,
`TensorExpr` fingerprints, and `TensorProgram` envelopes. Materialized terms and
date-histogram executors consume proven multi-output tensor programs for bucket
counts plus child metrics, including multi-field composite terms over configured
group fields. `docfact_rows`, `pathfact_rows`, and `join_fact_rows` can execute
bounded non-materialized folds through the same proof model.
Schemaless `pathfact` bucket folds can also apply proven pathfact predicate
constraints, including typed equality and string-prefix predicates, before
emitting term buckets and child metrics.

Algebraic bucket ordering is part of the public aggregation contract, not an
internal storage detail. Composite terms may use compact canonical tuple or
symbol-id keys on disk, but ties are ordered by the rendered public JSON bucket
key so algebraic, full-text-backed, and document-scan aggregation responses stay
checksum-equivalent across typed scalar encodings and shard-local dictionaries.

Configured and derived joins are deliberately exact-law only. Public
`algebraic_join` requests must carry configured join name, side roles, optional
temporal mode, and law-compatible aggregation semantics. Broader joins remain
out of scope unless side roles, fanout, temporal semantics, and merge laws can be
proved exact.

### Public Query Contract

Public search requests now have a canonical `query` field for structured query
trees. Compatibility request fields are normalized into that tree before
algebraic, full-text, vector, graph, and join planning: `full_text_search`
becomes scoring `bool.must`, `filter_query` becomes non-scoring `bool.filter`,
and `exclusion_query` becomes `bool.must_not`.

Structured filters support JSON-pointer-style `path` aliases beside `field`,
typed scalar `term` values for strings, numbers, booleans, and null, multi-value
`terms`, and `exists`. Query-string syntax remains accepted as a full-text escape
hatch, but it is not the canonical representation for algebraic filters or
typed fact planning.

### Adaptive Materialization

Query observations, path-profile observations, candidate scoring, lazy backfill,
readiness state, and dematerialization are engine-owned. Public status exposes
recommendation and progress health under `/tables/{table}/indexes/{index}` in
`status.algebraic`; there is no manual user-facing materialization lifecycle API.
Generated OpenAPI Zig types model index-status discriminators as typed
single-value enums, so `AlgebraicIndexStats` remains the one compact public
status envelope while generated code rejects mismatched stats variants.

Candidates are ranked with observed demand, estimated scan savings, write cost,
and measured owned sidecar bytes after backfill. Dematerialization removes owned
adaptive tensor/progress/candidate state and leaves the shape stale until future
observations requalify it. Candidate generations and decision-history rows are
persisted so status and benchmark output can explain policy drift, skipped
backfills, ready promotion, and automatic dematerialization decisions.

### Distributed Protocol

Distributed algebraic partials use canonical token semantics rather than
shard-local ids; shard-local symbol ids are physical only. Partial merge keys
include canonical axis, metric or expression identity, and law identity so
distinct laws over the same coordinate do not collide. Coordinator-side scalar
and bucket merges route through tensor row merge helpers.

Internal shard protocols carry access-path, tensor-expression, and tensor-program
envelopes. Shards re-prove owner, layout, fragments, dimensions, law ids,
dictionary identity, expression ids, and program ids before exporting partials
as `PartialEnvelope` rows, and the coordinator's `PartialProtocol` validator
rejects the same mismatches before merging canonical partial rows.
`docfact`, `pathfact`, derived-join, and configured-materialization tensor
programs can return canonical partial rows without requiring a named
materialization request body. Configured aggregate fallback partials are sent as
planner-owned tensor program/access-path envelopes; named materialization lookup
is retained only as an internal storage utility for local sidecar scans and
tests.

Root cardinality, terms-with-nested-cardinality,
range-with-nested-cardinality, and histogram-with-nested-cardinality
distributed partials use planner-owned tensor program envelopes with canonical
distinct-value and bucket/range merge keys.

New distributed shapes extend tensor/access-path envelopes rather than named
aggregate sidecar requests.

### Lexical Access Paths

Ownership follows the `dictionary_identity` rule in
[Lexical Tensor Access Paths](#lexical-tensor-access-paths): one owner
publishes dictionary/postings/FST rows per identity, and algebraic planning
consumes the advertised access path rather than building a duplicate FST.
Adaptive path promotion writes lexicon/posting rows during partial backfill while
the dictionary registry is `building`, then rebuilds the promoted FST once and
marks the registry `ready` only when backfill reaches the end. Ready bulk ingest
continues to coalesce promoted dictionary dirtiness and rebuild each changed
dictionary once per batch.

### Vector And Graph Integration

Supported algebraic `docfact` and `pathfact` filters can be resolved into native
include/exclude doc-id sets before dense or sparse vector traversal. Hosted
vector-worker requests carry raw symbolic filters plus a requirement that shard
execution resolve them algebraically or fail closed.

Graph indexes can opt into bounded provenance-semiring traversal. Eligible
traversal, shortest-path, and simple linear-pattern shapes prove a typed graph
access path and tensor program before using semiring execution. Ambiguous
provenance, weighted/non-deduped traversals, and richer graph patterns remain on
the normal graph executor until exact semantics are proven.

## Implementation Guide

This section is the working implementation contract. It folds the detailed notes
that used to live under short-, medium-, and long-term roadmap bullets into one
place, so the roadmap can stay focused on outcomes, ordering, and exit criteria.
When roadmap work introduces a new architectural invariant, move that invariant
here after the implementation proves it.

### Operating Rules

Algebraic execution remains opt-in, engine-owned, and evidence-driven. Schema
and query observation can derive capability plans, but public APIs must not
expose manual materialization lifecycle controls. Planner selection requires
ready lifecycle state, exact law proofs, matching dictionary identity,
dimensional compatibility, valid merge semantics, and benchmark-backed cost
gates.

The public contract is query capability plus status. External API, status, and
future protocol surfaces should use engine state, tensor expressions, access
paths, cleanup recommendations, and fallback reasons. Internal storage can keep
using `materialized_expr` as the durable tensor-cache row name because that is a
physical implementation detail, not a user-facing lifecycle API.

The stable public query surface is the normalized structured `query` tree
described in [Public Query Contract](#public-query-contract); query strings are
a full-text escape hatch, not the algebraic planning contract.

### Public Status Surface

Default public index status is compact and operator-focused: it answers whether
the algebraic sidecar is enabled, ready, selected recently, falling back,
rebuilding, stale, or blocked, with enough counters to see broad cost and health
trends. The compatibility rule is:

```text
public:   AlgebraicIndexStats
internal: AlgebraicCapabilityStats
internal: AlgebraicAdaptiveCandidateStatus
internal: AlgebraicAdaptiveCandidateDecisionStatus
internal: AlgebraicRuntimeHealth
internal: AlgebraicAdaptiveProgressStatus
```

`AlgebraicIndexStats` matches the index-status shape of the other index
families and is the only public envelope. Its stable fields are durable
concepts, not implementation tables:

```text
enabled/healthy/ready/rebuilding/stale
planner selected/fallback counts and latest fallback reason
schema capability lifecycle status and rebuild reason
adaptive materialization lifecycle counts
active backfill or cleanup progress summary (embedded operation + counters)
owned derived bytes and indexed document count
```

Additions to `AlgebraicIndexStats` are a compatibility-budget decision: prefer
coarse counters and the latest operator-relevant reasons. Candidate rows,
decision history and policy-drift counts, scoring inputs, canonical
recommendation hashes, tensor proof objects, expression ids, dictionary
registries, planner estimates, distributed proof counters, vector-filter
internals, exact error document keys, and adaptive worker sequence cursors stay
in internal state, an explicit diagnostics/admin surface with its own
compatibility expectations, or benchmark/debug JSONL. Keep one positive contract
test for the generated public shape rather than migration-style tests for
unshipped alternatives.

### Coverage And Selection

Every optimized path needs benchmark coverage against scan and full-text
aggregation baselines with query latency, write amplification, owned derived
bytes, update/delete churn, cold and warm reads, join fanout, constrained
predicates, and LSM behavior. Planner defaults stay conservative until the
LSM-backed numbers show a durable win.

Benchmark output should expose planner estimates, selected/fallback decisions,
adaptive candidate status, decision history, policy drift, dictionary registry
state, vector symbolic-pruning counters, graph traversal counters, and partial
merge validation status. A fast query path is not enough; the summary must make
storage and write cost visible.

CI-safe benchmark fixtures prove coverage shape, parser behavior, and regression
plumbing. They are not production performance evidence by themselves. Production
claims require larger repeatable runs with checked-in or archived baseline
summaries, hardware/environment notes, and threshold tolerances chosen from
measured variance rather than the tiny guardrail fixture. The shared DB matrix
records commands, parameters, raw JSONL, and comparison summaries; no
archive-verifier or separate policy executable is required.

Failure-injection coverage should target stale lifecycle state, missing or
conflicting dictionary ownership, adaptive backfill interruption, distributed
partial validation failure, symbolic filter fail-closed behavior, and
dematerialization cleanup.

### Fact And Schema Lifecycle

Schema-derived `docfact` rows are the correctness boundary for declared typed
fields. Schemaless `pathfact`, `path_lookup`, and `path_profile` rows are the
discovery substrate for late-typed promotion. Mixed-kind paths require
kind-qualified plans, explicit coercion policy, or fallback.

Schema lifecycle drift marks affected algebraic capability stale or
rebuild-required. Added compatible fields can start from the change point plus
optional backfill; type, analyzer, coercion, or expression changes require
readiness gating before the planner can select algebraic execution.

### Adaptive Lifecycle

Adaptive materialization is driven by persisted observations, path profiles,
policy scores, readiness state, measured storage/write cost, and decision
history. Policy drift is durable state, so future scoring changes must preserve
enough history to explain why a candidate was skipped, backfilled, promoted,
dematerialized, rebuilt, or left stale.

Automatic de-materialization must be safe and reversible. Owned adaptive tensor,
candidate, progress, and cleanup rows can be removed when workload demand no
longer justifies them. Base documents, fact rows, path profiles, dictionary
ownership, and schema-derived capability metadata remain the source for future
re-materialization.

### Planner Ownership

New query shapes should compile into typed tensor programs from the normalized
query tree, not into one-off execution paths. Current planner-owned tensor
builders cover configured materialized metrics, derived join folds,
schema-derived `docfact` bucket folds including distributed multi-output
histogram/range/date-range envelopes, schemaless `pathfact` bucket folds
including distributed terms/histogram/range/date-range envelopes, dense/sparse
vector-search programs with optional native doc-id constraint inputs, and
provenance-semiring graph traversal/edge programs with optional target
constraints where applicable. Catalog and API code should discover and validate
access paths, but planner-owned code should construct the tensor programs for
eligible vector, graph, aggregate, and join shapes.

Additional query shapes should extend `storage/db/algebraic/planner.zig` rather
than constructing executor-local tensor programs. Fallback cases should move
onto shared tensor execution only when benchmark data shows a durable win over
the existing scan, full-text, vector, or graph path.

Join expansion stays exact-law only: side roles, fanout, temporal semantics, and
merge laws must be configured and provable. Broader SQL-style join planning is
not a goal unless the configured algebraic laws preserve exact public semantics.

### Vector And Graph Execution

Symbolic algebraic constraints are pushed into dense, sparse, hosted vector, and
graph execution only when they can be resolved exactly and fail closed when
required. Vector and graph paths should advertise physical tensor access paths
that the shared planner can prove, rather than special-casing algebraic
internals.

Dense vectors keep ranking/search semantics in the vector executor. Full-text
keeps BM25, phrase/proximity, highlighting, and analyzer-specific ranking in the
lexical executor. Graph traversal keeps non-proven traversal semantics in the
graph executor. Algebraic participates when it can provide exact candidate
tensors, constraints, folds, or semiring programs.

### Runtime Optimization

Physical optimization follows measurement. The key levers are symbol-id row
layout, row locality, batching, bulk join fanout, dictionary/FST sharing, cold
reads, support-row compaction, update/delete churn, and write amplification.
Optimizations must preserve canonical merge semantics across shards even when
local storage uses compact ids. Rules:

- **Compile batch state once.** Bulk maintenance caches decoded ready adaptive
  materialization specs across consecutive batches (used for schemaless path
  promotion, direct adaptive tensors, and ready join tensors) and invalidates
  the cache on any adaptive observation, progress, or lifecycle change.
  Non-invertible laws are still marked stale exactly inside the batch.
- **Coalesce deltas.** Invertible ready adaptive tensor deltas, including
  promoted path-fact count tensors, are coalesced and flushed once per affected
  tensor/expression row before commit.
- **One bulk transaction window.** Non-append bulk ingest opens a
  cursor-capable write batch so overwrite/delete maintenance, configured and
  adaptive joins, and path promotion share the LSM bulk-ingest window. Primary
  writes inside an external DB bulk session use
  `BatchOptions{ .mode = .bulk_ingest }`, taking the same direct sorted-ingest
  path as the sidecars.
- **Diff overwrites.** Overwritten upserts skip the pre-delete, diff old and new
  `docfact` and `path_lookup` rows, and rewrite `docfact`/`pathfact` payloads
  only when the encoded payload changed. Aggregate, adaptive, path-profile, and
  join deltas still run exact old-minus/new-plus maintenance whenever the
  projected dependency changes; unchanged rows and tensors are skipped.
- **No preaggregated append fold.** Configured and adaptive materialized tensors
  stay on the tensor-law mutation path inside bulk batches. The fact-only
  append-only fast path remains for fact ingestion, but preaggregated
  materialization accumulation is guarded off until it has the same production
  correctness evidence.
- **FST rebuilds once.** Standalone bulk batches rebuild each dirty promoted FST
  once before commit; DB bulk sessions carry dirty dictionaries across flushed
  coalescer batches and rebuild each once at session finish, before the primary
  store publishes the final sorted run. Non-bulk writes rebuild immediately.
- **Finish boundary.** External DB bulk finish publishes the primary LSM session
  before forcing managed-index catch-up to `full_index`, so sidecar rows fold
  the final coalesced documents and survive a durable reopen.
- **Bucket preload.** Adaptive constrained terms use lazy point lookups for tiny
  bucket sets and preloaded child metric row maps for larger ones.
- **Resource accounting.** The catalog attaches the shared `ResourceManager` at
  open; coalesced tensor accumulator memory is reported under
  `algebraic.tensor_accumulators` and released when the batch or index closes.
  Algebraic writes use the same DocStore/LSM runtime store as the primary.

Benchmark counters (internal status and benchmark JSONL, rolled into
`performance_evidence_summary` by `storage_bench summary`; never part of the
compact public stats):

- adaptive maintenance: `algebraic_adaptive_maintenance_plan_build_count`,
  `algebraic_adaptive_maintenance_cached_spec_count`,
  `algebraic_adaptive_maintenance_disabled_count`
- LSM: `algebraic_lsm_flushes`, `algebraic_lsm_flush_output_runs`,
  `algebraic_lsm_sorted_ingest_runs`, `algebraic_lsm_sorted_ingest_bytes`,
  `algebraic_lsm_write_pressure_compactions`
- FST: `algebraic_path_dictionary_fst_rebuild_count` (summarized as
  `total_`/`max_path_dictionary_fst_rebuild_count`)
- churn row families: before/after entries and bytes for `materialized_expr`,
  `docfact`, `pathfact`, `path_lookup`, `path_profile`, `joinfact`, `docjf`,
  `minmax`, and `sym`, plus total update time (attribution, not isolated
  per-family timing)
- correctness: `unclassified_algebraic_comparisons` and a per-query
  `correctness_record` flag

Summary guard flags: `--min-lsm-sorted-ingest-runs` (direct ingest stayed
active), `--max-path-dictionary-fst-rebuild-count` (no per-row rebuild
regressions), `--max-lsm-flushes` and `--max-lsm-write-pressure-compactions`
(no fallback to normal flush/compaction), and `--require-performance-evidence`
(unclassified comparisons must be zero). These LSM thresholds certify the
algebraic sidecar contract only, not generic compaction, WAL, block cache, or
read amplification. The DB matrix forwards LSM tuning and bulk-finish options
through `--analytics-arg`, records them in the `dataset_lsm_config` event, and
applies the same bulk flags to the cold/warm read stage. The wide-key benchmark
issues the public multi-field `terms.fields` shape over the same canonical axes
as its materializations, so it is an exact comparison.

Known gap: the ready-spec cache reduces maintenance-plan builds, but
materialized-adaptive LSM churn remains dominated by lower-level sidecar
mutation cost.

> **Relocated:** The dated May 17-18, 2026 churn-benchmark narrative (smoke-run
> timings and counter deltas, 88 lines) that previously lived here is preserved
> verbatim in [docs/design/algebraic/history/churn-benchmarks-2026-05.md](../docs/design/algebraic/history/churn-benchmarks-2026-05.md).
> The durable rules above were folded out of it before the move.

### Law Expansion

New algebraic structures should be added only when they unlock exact planner
capabilities. Semirings, lattices, provenance/path algebras, and domain laws need
declared identity/merge behavior, tensor proof hooks, distributed merge tests,
and targeted benchmarks. They should not be added as unused abstractions.

### Applying This Guide

Roadmap work should update this guide only when it changes an implementation
rule that future work must preserve. Completed behavior belongs in
Implementation State. Temporary sequencing, estimates, and exit criteria belong
in the Roadmap. Code-specific details belong beside the relevant tests or module
docs once they are no longer architectural constraints.

## Approximate cardinality (HyperLogLog)

An index can materialize approximate distinct counts so a `cardinality`
aggregation is answered from a per-group sketch instead of scanning and
deduplicating every value:

```json
{
  "hll_cardinalities": [
    {"name": "customers_by_region", "group_by": ["region"],
     "value_field": "customer", "precision": 14}
  ]
}
```

`group_by` contains the bucket axes, `value_field` names the distinct value,
and `precision` (4–18, default 14) controls sketch size and accuracy. Results
identify whether they are estimates and include the relative error when
applicable:

```json
{"value": 4044, "approximate": true, "relative_error": 0.0081}
{"value": 4096, "approximate": false}
```

The planner uses a matching, current sketch when the query shape permits it and
otherwise falls back to the exact distinct scan. Deletes and overwrites mark
affected groups dirty; the durable maintenance lane rebuilds those sketches in
the background.

Array-valued group and value fields use set semantics. A document contributes
every distinct value to every distinct Cartesian group tuple, while duplicate
array elements do not inflate the sketch. `mode: exact` always bypasses HLL;
`mode: approximate` fails explicitly when no current matching sketch exists;
and `mode: auto` may fall back to an exact scan.

Ingest batches all distinct values from a document into one dense contribution
per group tuple. `max_hll_contributions_per_document` bounds the logical
Cartesian expansion across every configured sketch, while
`max_hll_contribution_bytes_per_document` (default 8 MiB) independently bounds
the dense sketch bytes merged and written. An index may publish at most 64 HLL
materializations, including adaptive promotions. These hard limits keep high
precision or adversarial multi-valued documents from turning a small logical
request into unbounded CPU, memory, or write amplification.

Distributed grouped cardinality exports sketches lazily, one selected shard
bucket at a time, and bounds their aggregate raw size with
`max_distributed_hll_partial_bytes` (default 2 MiB per shard request). Sketch
bytes use base64 on the internal HTTP boundary. If any non-exact child lacks a
fresh sketch or the export budget is reached, the optimized route is rejected
for the whole query so `auto` can fall back to an exact plan without mixing HLL
and exact merge laws; `approximate` reports that no supported route is
available.

Adaptively promoted sketches share `max_auto_materializations_per_index` with
other adaptive materializations. Their initial backfill is durable and resumes
after restart, processing no more than `max_backfill_rows_per_tick` document
facts per maintenance tick. Reads continue using the exact path until the
backfill cursor and any concurrent-mutation repair markers commit as complete.

## Roadmap

The previous short-, medium-, and long-term design rules have been folded into
the Implementation Guide, and completed behavior has been moved into
Implementation State. The active roadmap now tracks production-hardening work
only. When one of these items lands, move the durable facts into the sections
above and remove the temporary roadmap item.

### Production Hardening

1. Establish real performance baselines.
   Use `scripts/run_db_query_matrix.py --suite analytics --profile bounded` for
   LSM analytics, adaptive coverage, cold/warm reads, graph traversal, and public
   schema comparisons. Preserve raw results and environment metadata. Set
   `--baseline` and measured `--summary-arg` limits for latency, memory, storage,
   and churn; pass LSM bulk/tuning options through `--analytics-arg`.

2. Prove owner-suite test health.
   Run `antfly-storage-db-test` and the API/metadata owner
   suites, or `antfly-unit-test` for the aggregate including graph coverage. The DB matrix preserves the
   no-schema/schema-only/algebraic public-query comparisons. Use the normal e2e
   workflow for transport and server coverage.

3. Harden crash, recovery, and rebuild behavior.
   Add failure-injection coverage for interrupted adaptive backfill,
   dematerialization cleanup, corrupt or stale sidecar rows, schema drift during
   ingest, dictionary owner conflicts, and reopen/replay across the LSM backend.

4. Exercise sustained write-path pressure.
   Run long overwrite/delete/churn workloads for aggregate tensors, MIN/MAX
   support rows, temporal buckets, joins, schemaless facts, adaptive cleanup, and
   bulk LSM ingest. Track sidecar bytes, support-row growth, flush counts,
   cached bulk-maintenance plan hits, row-family churn counters, and update
   latency. Keep the cursor-capable bulk write-batch path in the archive so
   regressions in configured/adaptive tensor maintenance show up next to
   fact-only append and direct-write runs.

5. Validate distributed and sharded failure modes.
   Test mixed shard-local symbol ids, canonical merge keys, missing/stale shards,
   aggregate law-family partial merges, rejected tensor envelopes, partial
   validation failures, vector fail-closed routing, and graph traversal
   proof/reject behavior across shard fanout. Positive coverage currently exists
   for canonical same-token/different-local-dictionary merges, aggregate law
   families (count/sum/sumsquares/avg/min/max), distributed stats/date/terms/
   range/histogram/cardinality folds, and derived join partials; archived
   integration runs should still add negative shard-health and stale/
   missing-shard evidence.

6. Bound memory and storage growth.
   Measure and cap symbol registry growth, docfact/pathfact volume, path-profile
   cardinality, dictionary/FST ownership, support-row compaction, accumulator
   memory, and derived sidecar bytes under high-cardinality workloads.

7. Keep the external API capability-oriented.
   Users request query capability and observe readiness/status only; manual
   materialization lifecycle controls, internal expression ids, and
   engine-owned adaptive transitions stay out of the stable API. The status
   rules are in [Public Status Surface](#public-status-surface).

### Non-goals

```text
arbitrary query-time joins
automatic typed materialization of every schema field or schemaless path
automatic materialization of every aggregate combination
manual user-facing materialization lifecycle or explicit materialization definition APIs
best-effort approximate MIN/MAX
using the algebraic sidecar when status indicates incomplete derived state
```
