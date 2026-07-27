# Hyperdimensional Computing in Antfly

- Status: experiment concluded no-go for a stable public index; prototype
  retained for reproducibility
- Target: do not release without new representative evidence that reverses the
  quality/cost decision
- Related design: [ALGEBRAIC.md](ALGEBRAIC.md)

## Implementation Status

This worktree now contains the first engine-owned Phase 1 slice:

- a versioned MAP encoder identity with SHA-256 fingerprints;
- deterministic, domain-separated bipolar atomic symbols;
- typed key/value binding and association-count-normalized structural bundles;
- streaming Rademacher projection with deterministic zero ties;
- projection checksums and fail-closed identity validation;
- explicit vector, projection, and working-memory byte estimates;
- semantic/structural combination into the existing dense `f32` artifact shape;
- canonical HDC configuration and deterministic selected-path JSON projection;
- a public logical `hypervector` index type that lowers to the existing
  `dense_vector` `IndexKind`;
- managed query projection while preserving the provider's source dimensions;
- optional per-index structured query associations composed in the same
  coordinate system as documents;
- document composition before the existing dense artifact and HBC write;
- full encoder fingerprints, including model digest and projection checksum,
  in artifact names, query cache identity, and canonical source invalidation;
- selected-value source hashes that ignore JSON key order and unrelated fields;
- golden compatibility, algebra, drift, invalid-input, translation, and
  enrichment tests;
- a ReleaseFast `zig build hdc-bench` harness for projection latency,
  structural encoding throughput, checksum cost, memory accounting, and a
  synthetic complete-HDC RaBitQ recall/latency/storage smoke measurement;
- a deterministic controlled-composition diagnostic that compares embeddings,
  embeddings plus exact filters, embeddings plus transparent structured score
  fusion, semantic-only HDC, and complete HDC on both present and deliberately
  absent structured combinations;
- a pinned WANDS product-search experiment over 42,994 products, 480 queries,
  and 233,448 human relevance judgments, with real BGE embeddings,
  validation-only weight selection, holdout quality, exact graph-answer
  scoring, and equal RaBitQ candidate budgets;
- a managed backfill-to-retrieval fixture that uses natural-language HDC search
  to select a location and then follows only the exact stored graph edge, closes
  and reopens the database, repeats the query, and verifies resumed HDC
  enrichment for a new document.

This is not release-ready. The public shape and lifecycle semantics are now
explicit, but interrupted-backfill fault injection, HDC-specific resource
attribution, representative workload evidence against ordinary embeddings plus
exact filters, a public HTTP query fixture, and a distributed
fuzzy-seed-to-exact-graph fixture remain required.

| Production capability | State | Required evidence |
| --- | --- | --- |
| Versioned deterministic MAP algebra | implemented | golden and algebra tests |
| Bounded streaming semantic projection | implemented | unit tests and `hdc-bench` |
| Public logical hypervector configuration | implemented experimentally and lowered to dense storage | generated OpenAPI and translation contract tests |
| Canonical document-to-association projection | implemented for selected JSON paths | replay fixtures against stored typed/schema values |
| Managed ingest, backfill, rebuild, and quarantine | ingest/backfill and close/reopen/resumed writes tested through existing dense lifecycle; HDC seed/model drift receives a distinct artifact namespace | interrupted-backfill and config-drift rebuild fault injection |
| Text and structured query composition | implemented | managed local-provider projection, exact-path validation, and public query forwarding tests |
| Dense HBC/RaBitQ retrieval quality | pinned WANDS exact and RaBitQ evaluation finds no quality/cost win over tuned embedding score fusion | HBC-tree confirmation only if a later workload reverses the no-go result |
| Fuzzy seed to exact graph result | managed backfill/retrieval fixture implemented | distributed graph fixture and public HTTP contract |
| Operational status and resource counters | normal index status exposes public hypervector config, semantic config fingerprint, coverage outcomes, replay target, backfill progress/state, cardinality, and disk use | HDC encode/query/resource/fallback counters and pressure tests |

## Product Goal

For users, hypervector retrieval should be a distinct logical capability
without becoming a second physical database subsystem:

> Declare one `hypervector` index, write normal documents, query with text and
> optional structured associations, and let Antfly deterministically derive,
> backfill, rebuild, cache, and search complete hypervectors through the
> existing dense-vector machinery.

Production success means fast fuzzy-to-exact graph queries, predictable memory
and storage, no manual vector materialization, fail-closed coordinate-system
drift, and lifecycle/status behavior identical to other managed derived
indexes. The graph remains exact: HDC only chooses ranked seed nodes.

## Experiment Goal

This experiment must determine whether complete hypervectors provide a
meaningful retrieval advantage over ordinary embeddings plus exact filters or a
simple structured-score baseline, without introducing a new physical index
engine or weakening graph correctness.

Concretely, it must answer:

> Can Antfly combine semantic and structured evidence into one managed
> hypervector that selects better graph entry points while meeting production
> requirements for latency, recall, storage, rebuilds, and a simple user
> experience?

Success requires all of the following:

- measurably better retrieval, or uniquely useful composition, on
  representative workloads;
- exact graph facts, authorization constraints, and filters remaining
  authoritative;
- competitive HBC/RaBitQ performance and bounded resource use;
- deterministic, fail-closed encoder identity and lifecycle behavior;
- a straightforward UX: declare a hypervector index, write normal documents,
  and query with text plus optional structured associations.

The experiment is allowed to reject HDC. If complete hypervectors cannot
outperform ordinary embeddings with exact filtering or transparent structured
score fusion enough to justify their additional dimensions, scoring cost, and
lifecycle surface, Antfly should prefer the simpler existing approach. A
successful implementation of the primitive is not, by itself, a successful
product experiment.

### Current decision

The current experiment is a **no-go for productionizing HDC as a stable public
index**. On the pinned human-labelled WANDS workload, validation-tuned complete
HDC and validation-tuned embedding score fusion are effectively tied on
application quality. The embedding baseline is materially smaller and faster,
and its semantic ANN candidates can be reranked with the structured score
without adding a new physical index.

This does not invalidate the deterministic HDC primitive or its lifecycle
tests. It means the primitive has not earned the public API, dimensions, or
operational cost. The `hypervector` API in this branch must remain experimental
and must not be declared stable based on the GraphCon example, the controlled
diagnostic, or WANDS. A later proposal must reverse this result on a workload
whose composition cannot be reproduced by exact filtering or simple late
fusion.

## User Experience

HDC is exposed as a logical `hypervector` index and operated through the same
managed-index lifecycle as embeddings. Users do not create projection matrices,
write vector fields, choose internal artifact names, or submit
10,000-dimensional query vectors.

### 1. Declare a hypervector index

The table definition gives stored dimensions, encoder configuration, semantic
identity, and structural paths separate meanings:

```json
{
  "location_hdc": {
    "type": "hypervector",
    "dimensions": 10000,
    "encoding": {
      "type": "map",
      "seed": 13
    },
    "semantic": {
      "field": "description",
      "weight": 8,
      "model_digest": "sha256:location-semantics-v1",
      "embedder": {
        "provider": "antfly",
        "model": "location-semantics",
        "dimension": 768
      }
    },
    "structural": {
      "paths": [
        "features",
        "name",
        "region",
        "timezone"
      ]
    }
  }
}
```

`dimensions` always means the stored complete-vector dimension. The provider
dimension belongs to `semantic.embedder` and never changes the meaning of the
stored field. `semantic.weight` must be positive. `semantic.model_digest` is an
immutable provider revision or content digest; mutable aliases must be resolved
before creation so rolling model changes cannot silently reuse a generation.

### 2. Write ordinary documents and graph facts

Documents contain their normal source fields:

```json
{
  "_id": "location:seattle",
  "name": "Seattle",
  "description": "Pacific Northwest city on Puget Sound",
  "region": "pacific_northwest",
  "timezone": "pacific",
  "features": ["waterfront", "mountains", "nature_access"]
}
```

Graph facts remain ordinary exact edges. For example:

```json
{
  "_id": "person:ada",
  "name": "Ada",
  "_edges": {
    "travel_graph": {
      "VISITED": [
        {
          "target": "location:seattle",
          "weight": 1.0
        }
      ]
    }
  }
}
```

After the source write commits, the managed enrichment lifecycle embeds
`description`, projects it into HDC space, binds and bundles the configured
structural fields, persists the complete derived vector, and updates the
existing dense index. The caller does not generate or write `_embeddings` for
this managed HDC index.

### 3. Observe readiness through normal index status

Adding the index to a populated table starts the existing managed backfill.
The normal index status surface reports:

- public hypervector configuration and full encoder fingerprint;
- backfill state and progress;
- produced, pending, skipped, and terminal-failed source counts;
- replay applied and target sequences;
- vector cardinality and disk usage.

There is no separate HDC administration API. Callers that require complete
coverage use the same readiness and coverage policy they use for other managed
derived indexes.

### 4. Search with text, optionally adding structural evidence

The public request uses `semantic_search`; Antfly performs both provider
embedding and deterministic HDC projection:

```http
POST /tables/travel/query
Content-Type: application/json
```

```json
{
  "semantic_search": "persons who visited cities on the Pacific coast",
  "indexes": ["location_hdc"],
  "limit": 10
}
```

The ordinary embedding hits contain ranked location documents and cosine
scores. The client does not select an HDC query mode or know the internal
generation-scoped artifact name.

When the query contains known structured evidence, `hypervector_queries` binds
it into the same coordinate system. The outer key is the index name and every
inner key must exactly match a configured `structural.paths` entry:

```json
{
  "semantic_search": "cities with easy access to nature",
  "indexes": ["location_hdc"],
  "hypervector_queries": {
    "location_hdc": {
      "region": "pacific_northwest",
      "features": ["waterfront", "mountains"]
    }
  },
  "limit": 10
}
```

Unknown paths fail closed. Supplying structured associations for an ordinary
embeddings index is rejected. Text-only queries remain supported, but
graduation requires measured evidence that document structural composition
improves a representative workload over ordinary embeddings plus exact
filters.

### 5. Create graph indexing explicitly, then use fuzzy hits as seeds

A hypervector index never auto-creates a graph index. A graph has independent
edge schema, direction, provenance, authorization, storage, and lifecycle
choices. A quickstart may generate both declarations, but both resources must
remain visible in the table definition.

The same request can hand the ranked embedding results to an existing graph
query:

```json
{
  "semantic_search": "persons who visited cities on the Pacific coast",
  "indexes": ["location_hdc"],
  "limit": 10,
  "graph_searches": {
    "visitors": {
      "type": "neighbors",
      "index_name": "travel_graph",
      "start_nodes": {
        "result_ref": "$embeddings_results",
        "limit": 1
      },
      "params": {
        "edge_types": ["VISITED"],
        "direction": "in",
        "max_depth": 1,
        "include_paths": true
      }
    }
  }
}
```

For the motivating fixture, the embedding hits rank
`location:seattle` first. The relevant graph response shape is:

```json
{
  "graph_results": {
    "visitors": {
      "type": "neighbors",
      "total": 1,
      "nodes": [
        {
          "key": "person:ada",
          "depth": 1,
          "path": ["location:seattle", "person:ada"]
        }
      ]
    }
  }
}
```

The seed and its cosine score are approximate retrieval evidence. The returned
person and path come only from the stored incoming `VISITED` edge. Similarity
never creates or proves a graph fact.

### 6. Let the managed lifecycle handle change

Changing a document's semantic source or selected structural fields recomputes
its complete vector. Deleting a document removes its derived artifact and dense
entry. Closing and reopening the database preserves the encoder configuration,
indexed vectors, graph facts, and enrichment replay position.

Changing an encoder version, canonicalization version, seed, dimension,
structural path, semantic weight, source field, provider, model, immutable model
digest, provider dimension, projection algorithm, or projection checksum
changes the full 256-bit generation identity. Antfly assigns the new coordinate
system a different internal artifact namespace and drives it through the normal
rebuild and coverage lifecycle rather than mixing compatible-looking
dimensions.

### 7. Reject ambiguous or unbounded configurations early

Index creation rejects the legacy bare `embeddings.hdc` field, zero or
non-finite semantic weights, missing immutable model identity, unsupported
encoders, malformed structural paths, and dimensions or logical projection work
above the configured resource limits. These are explicit creation errors, not
silent fallback to a different retrieval mode.

## Decision Summary

Antfly can support the fuzzy-entry-point-to-exact-graph-query described in the
GraphCon 2026 HDC example without adding a new graph engine or putting dense
hypervectors into the algebraic sidecar.

| Question | Decision |
| --- | --- |
| Is `hdc` a field on an embeddings index? | No. The stable public boundary is a logical `type: "hypervector"` index. `hdc` remains an internal encoder/enrichment implementation term. |
| Is it a new physical index engine? | No. It lowers to the existing `.dense_vector` `IndexKind`, HBC/RaBitQ, exact rerank, coverage, rebuild, and status machinery. |
| Does it auto-create a graph index? | No. Graph indexing is a separate explicit resource with independent topology, provenance, authorization, cost, and lifecycle. |
| How do structural fields affect queries? | Only explicit `hypervector_queries[index_name]` associations enter the structural channel. Natural-language text alone does not align with random atomic structural symbols. |

The proposed ownership boundary is:

```text
public hypervector index
  -> managed HDC enrichment produces versioned, rebuildable dense f32 artifacts
  -> existing dense_vector IndexKind and HBC/RaBitQ rank candidate node ids
  -> algebraic planning proves and composes candidate and graph access paths
  -> separately declared graph index traverses exact stored edges
```

More specifically:

1. **HDC encoding is a new enrichment capability.** It owns deterministic
   symbol encoding, semantic projection, binding, bundling, encoder identity,
   and derived-artifact lifecycle.
2. **HDC is its own public logical index.** Its configuration and structured
   query semantics are distinct from ordinary embeddings, so the public API can
   evolve without complicating every embeddings index.
3. **HDC retrieval uses the existing dense-vector index internally.** A
   10,000-dimensional HDC vector is a dense vector for nearest-neighbor
   retrieval. Antfly already supports cosine distance, HBC search, RaBitQ
   quantization, exact reranking, precomputed query vectors, and graph expansion
   from embedding results.
4. **The algebraic layer owns composition, not HDC ranking storage.** It should
   type-check and connect `vector_search(doc, score)` to
   `graph_traverse(src, dst)` while keeping the approximate candidate score
   distinct from exact graph provenance.
5. **The graph remains the source of relational truth.** HDC proposes likely
   entry points. Stored directed edges determine what is known to be true.
   Graph creation is always explicit; HDC never synthesizes a graph resource.
6. **A dedicated hypervector physical index is deferred.** It becomes justified
   only if the existing dense path cannot meet measured cost/recall targets, or
   if Antfly needs server-side HDC operations such as bind, unbind, permutation,
   or online prototype updates.

The HDC representation must not be expanded into one algebraic sidecar row per
coordinate. That would turn every document into roughly 10,000 dense sidecar
facts, defeat the sparse-token layout, and mix approximate ranking state with
the exact symbolic facts on which algebraic law proofs rely.

## Motivation

The motivating GraphCon 2026 query asks:

> Can we find Seattle's visitors without naming Seattle?

The reference flow is:

1. Embed a natural-language description such as "persons who visited cities on
   the Pacific coast."
2. Project the semantic embedding into a 10,000-dimensional MAP hypervector.
3. Search a complete `Location.hv` column containing structural property
   associations plus weighted semantic evidence.
4. Select Seattle as a fuzzy candidate.
5. Follow exact `VISITED` edges to return people connected to Seattle.

Step 3 is approximate ranked retrieval. Step 5 is exact graph evaluation. The
boundary matters: a high HDC score does not prove an edge exists, and a graph
edge must not be invented from vector similarity.

Antfly already exposes this general query composition. A graph selector may use
`$embeddings_results` as its start set, dense queries may carry precomputed
vectors, and the algebraic IR already describes vector and graph physical access
paths. The missing product capability is an HDC encoder with a durable,
versioned coordinate system.

## Scope

This design covers:

- deterministic MAP-style hypervector encoding for structured properties;
- required positive-weight semantic embedding projection into the same HDC
  coordinate system for the initial public query mode;
- complete node hypervectors used for cosine candidate retrieval;
- optional relationship hypervectors keyed by existing edge identity;
- lifecycle, storage, query, distributed, and planner integration;
- an evidence plan for deciding whether the feature should graduate beyond an
  external-vector prototype.

This design does not initially cover:

- replacing ordinary dense semantic embeddings;
- inferring or persisting graph edges merely because two vectors are similar;
- arbitrary path-hypervector materialization;
- exposing a general-purpose HDC programming language in the public API;
- training embedding models or learning the random projection;
- making cosine similarity an exact algebraic law;
- adding online prototype learning before retrieval correctness and storage
  economics are established.

## HDC Model

### Coordinate space

The initial model follows the reference demo's Multiply-Add-Permute (MAP)
representation:

```text
dimension:       d, initially 10,000
atomic values:   {-1, +1}^d
bundle values:   integer or floating-point sums in R^d
similarity:      cosine
binding:         element-wise multiplication
bundling:        element-wise addition
permutation:     a deterministic invertible coordinate permutation
```

Unrelated deterministic atomic vectors should be approximately orthogonal at
large `d`. This property makes it possible to compose multiple associations in
one distributed representation and later rank related representations by
similarity.

The dimension is part of encoder identity, not an implicit global. Ten thousand
is a starting point inherited from the reference implementation, not a
hard-coded database invariant.

### Atomic symbols

An atomic structural symbol is generated from a canonical token:

```text
token_hv("key:region")
token_hv("value:pacific_northwest")
token_hv("predicate:VISITED")
```

The generation function must be deterministic across processes, architectures,
replay, and shards. It must not use a process-randomized language hash.

Conceptually:

```text
seed_material = encoder_seed || canonical_token
token_seed    = stable_cryptographic_hash(seed_material)
token_hv      = deterministic_bipolar_vector(token_seed, d)
```

Canonicalization rules are observable semantics. Field paths, value kinds,
numeric formatting, Unicode normalization, null handling, arrays, objects,
timestamps, and bytes all require stable definitions. The encoder should reuse
Antfly's typed value and path canonicalization where those semantics match
rather than creating an HDC-only interpretation of JSON.

### Binding

A key/value association binds the key and value vectors:

```text
association("region", "pacific_northwest")
  = token_hv("key:region") * token_hv("value:pacific_northwest")
```

For bipolar MAP factors, element-wise multiplication is self-inverse:

```text
(K * V) * K = V
```

This is useful for associative lookup, but it does not by itself retain schema,
direction, or provenance. Those remain explicit metadata.

### Bundling

A node's structural representation bundles its associations:

```text
structural_hv(node)
  = sum(association(field, value) for canonical fields in node)
```

The raw sum should remain unnormalized while it is maintained. Preserving the
sum allows an association to be added or subtracted exactly. At ranking
materialization time, Antfly divides the structural channel by the square root
of its typed leaf-association count before adding semantic evidence. This keeps
documents with more configured values from winning merely because their
structural vector has a larger norm while retaining multiplicity in the raw
accumulator.

When a composite is used as a factor in a later MAP binding operation, it is
projected back to bipolar values. Zero coordinates need a deterministic
context-specific tie breaker so replay cannot choose a different factor.

### Semantic projection

The reference design projects a normalized semantic embedding `x` into HDC
space with a deterministic Rademacher matrix `R`:

```text
x             in R^m
R             in {-1, +1}^{m x d}
semantic_hv   = sign(xR)
```

The projection is a coordinate transform, not a new semantic model. It cannot
add information absent from `x`. Its value is putting semantic and structural
signals into a composable HDC space.

Document and query embedding modes must match. If the underlying embedder uses
different query and document prefixes, the HDC runtime must preserve them.

The projection matrix may be regenerated deterministically, stored as an
artifact, or both. In all cases its checksum is part of encoder identity.
Changing the matrix, seed, dimensions, embedding model, model digest, or
embedding normalization invalidates all derived HDC vectors.

### Complete node vector

The initial complete node vector is:

```text
normalized_structural_hv = raw_structural_hv / sqrt(association_count)
node_hv = normalized_structural_hv + semantic_weight * semantic_hv
```

The semantic weight is configuration and part of encoder identity. The
reference value of `8` is a demo choice, not a universal default. It must be
strictly positive for the public semantic-search mode.

A semantic-only query hypervector can be compared directly with this complete
node vector:

```text
query_hv = sign(embed_query(text) * R)
score    = cosine(query_hv, node_hv)
```

The score is a ranking signal, not a probability or graph fact.

When callers know structural evidence, the public per-index association map is
encoded with the same typed path/value rules:

```text
query_hv = normalize(bundle(query_associations))
         + semantic_weight * sign(embed_query(text) * R)
```

Every query path must exactly match configured structural paths. Atomic
structural coordinates are intentionally independent of natural-language
projection coordinates, so structural evidence only contributes when encoded
as associations; it is not inferred from query text.

### Relationship vector

An optional relationship vector can bind subject, predicate, and object:

```text
edge_hv = bipolar(subject_hv)
        * token_hv("predicate:VISITED")
        * bipolar(object_hv)
```

This is not required for the motivating query. Slide 13 searches node
hypervectors and then traverses ordinary graph edges.

MAP multiplication is commutative. Therefore the hypervector alone does not
preserve edge direction. The graph index and edge record must continue to store
source, target, type, identity, and other exact metadata.

Relationship vectors, if enabled, are derived artifacts keyed by existing edge
identity. They must not create edges and must not replace the graph layout.

## Antfly Ownership Model

| Concern | Owner |
| --- | --- |
| Canonical source document | primary document store |
| Exact typed facts and aggregate materializations | algebraic sidecar |
| Directed source/target/type topology | graph index |
| Semantic embedding generation | embedding enrichment/runtime |
| HDC token, projection, bind, bundle, and permutation semantics | HDC enrichment |
| Authoritative derived node/edge hypervector | versioned HDC artifact |
| Approximate candidate ranking | dense-vector executor initially |
| RaBitQ candidate compression and exact rerank policy | HBC dense index |
| Exact symbolic prefilters | algebraic `docfact`/`pathfact` resolution |
| Vector-to-graph plan construction and proof | algebraic planner/IR |
| User-visible graph result hydration | graph/query execution |

This follows the rule already established in `ALGEBRAIC.md`: the algebraic
planner should consume search-engine candidate tensors rather than embedding a
second ranking engine in the algebraic sidecar.

## Current Antfly Primitives

The initial prototype can be assembled from existing primitives:

- `dense_vector` is already a typed algebraic physical layout.
- `vector_search` already produces `(doc, score)`.
- dense search supports cosine distance and precomputed query vectors.
- dense HBC indexes already support RaBitQ quantization and reranking.
- algebraic symbolic filters can produce native include/exclude document sets
  before dense traversal.
- `graph_traverse` and graph-edge access paths already exist in the algebraic
  IR.
- graph query selectors already accept `$embeddings_results`.
- graph traversal can remain on the normal graph executor for query shapes that
  do not have a proven provenance-semiring plan.

Consequently, phase 0 requires no new physical index kind. It needs an external
encoder, an external dense-vector field, and a query that expands graph results
from the embedding candidate set.

## Proposed Data Flow

### Ingest and replay

```text
primary mutation
  -> commit canonical document and graph facts
  -> derive semantic source text and structural typed facts
  -> embed semantic source text
  -> project semantic embedding into HDC space
  -> bind and bundle structural facts
  -> combine structural and semantic channels
  -> persist versioned HDC artifact
  -> apply vector to the configured dense index
  -> mark artifact/index coverage at the source generation
```

Base documents remain canonical. HDC state is rebuildable. The primary write
must not become unrecoverable because HDC enrichment is unavailable.

The existing asynchronous derived-enrichment pattern is preferred. Tables that
require synchronous HDC coverage may opt into the same readiness and
fail-closed behavior used by other required derived artifacts, but synchronous
remote embedding on the primary mutation path is not the default.

### Query

```text
natural-language query
  -> embed_query
  -> deterministic HDC projection
  -> vector_search(Location.hv, cosine, top-k)
  -> candidate Location ids with scores
  -> graph_traverse(candidate ids, edge_type=VISITED, direction=in)
  -> exact Person/Location paths
  -> hydrate stored documents and expose retrieval provenance
```

The implemented managed path accepts the ordinary public `semantic_search`
field shown in [User Experience](#user-experience). The managed embedder
computes and projects the query vector, and graph search selects
`$embeddings_results`. The lower-level `embeddings` field remains useful for
Phase 0 external-vector experiments and internal tests, but is not part of the
managed HDC user journey.

### Result semantics

The composed execution must distinguish these fields:

```text
seed_doc          fuzzy candidate selected by HDC
seed_score        cosine ranking signal for seed_doc
result_doc        node reached through stored graph edges
graph_provenance  exact edge/path provenance
```

If multiple seeds reach the same result, the engine must not silently reinterpret
cosine ranking as a path law. The safe initial behavior is to return one result
per seed/path. A collapsed result view may use a declared deterministic rule
such as maximum seed score, while retaining the winning seed and path
provenance.

## Encoder Configuration and Identity

An HDC encoder needs a complete durable identity. At minimum:

```text
encoder_kind
encoder_version
vsa_model                         // initially MAP
dimensions
atomic_vector_algorithm
atomic_vector_seed
canonicalization_version
included_paths and excluded_paths
value-kind policy
array/object policy
binding_normalization_version
zero_tie_break_version
permutation_version, if used
semantic_enabled
semantic_source/template_version
embedding_provider and model
embedding_model_digest
embedding_dimensions
embedding_normalization
projection_kind
projection_seed
projection_dimensions
projection_artifact_checksum
semantic_weight
output_precision
```

Every persisted vector artifact and distributed query envelope must identify
the encoder coordinate system. Two 10,000-dimensional vectors are not
compatible merely because their lengths match.

An encoder identity change causes:

1. derived HDC lifecycle to become stale or rebuild-required;
2. old and new vectors to be prevented from sharing one index generation;
3. backfill into a new artifact/index generation;
4. atomic publication only after coverage is ready;
5. cleanup of the superseded generation after readers release it.

This should reuse Antfly's existing artifact identity, coverage generation,
rebuild, and quarantine mechanisms rather than introduce an HDC-only lifecycle.

## Proposed Configuration Surface

Phase 0 uses a normal external embeddings index. Conceptually:

```json
{
  "location_hdc": {
    "type": "embeddings",
    "field": "hv",
    "dimension": 10000,
    "metric": "cosine",
    "external": true,
    "use_quantization": true
  }
}
```

Phase 1 adds an engine-owned logical `hypervector` index. This is the
experimental public schema implemented in this worktree:

```json
{
  "location_hdc": {
    "type": "hypervector",
    "dimensions": 10000,
    "encoding": {
      "type": "map",
      "seed": 13,
      "projection_seed": 13
    },
    "semantic": {
      "field": "description",
      "weight": 8,
      "model_digest": "sha256:location-semantics-v1",
      "embedder": {
        "provider": "antfly",
        "model": "location-semantics",
        "dimension": 768
      }
    },
    "structural": {
      "paths": ["features", "name", "region", "timezone"]
    }
  }
}
```

`dimensions` is always the stored dense-vector dimension. Provider dimensions
belong to `semantic.embedder`. Defaults and structural path order are
canonicalized into the private managed generator config, then the logical index
is lowered to the existing `.dense_vector` kind.

The internal embedding artifact name carries the complete 256-bit
`Identity.fingerprint`: encoder and canonicalization versions, output
dimensions and precision, structural paths, semantic source, provider, model,
caller-pinned immutable model digest, provider dimensions, normalization,
projection algorithm/seed/checksum, and positive semantic weight. The same
fingerprint participates in query-cache identity and selected-input source
hashing. A coordinate-system change therefore writes a new artifact namespace
instead of allowing same-dimensional vectors from different generations to
alias during a rebuild.

Document source hashes include only semantic source text, canonical values at
selected structural paths, and the complete fingerprint. JSON key reordering
and unrelated field changes do not cause re-embedding. Arrays remain
order-independent multisets, matching document composition semantics.

The experimental guardrails cap HDC output at 65,536 dimensions, source
embeddings at 65,536 dimensions, and one logical projection at 134,217,728
matrix coordinates. Projection remains streaming, so its working allocation is
one output `f32` vector, but configurations whose deterministic compute cost
exceeds that limit are rejected before ingest or query execution.

Canonicalization version 1 supports dot-separated object paths. Missing paths
are omitted; scalar null, boolean, integer, number, and string values retain
their JSON type; configured objects recursively contribute typed leaf values;
and arrays are order-independent multisets whose duplicates retain
multiplicity. Literal-dot keys, templates, chunks, sparse/external vectors,
multimodal sources, artifact-backed embeddings, and non-cosine metrics are
rejected in this first slice rather than assigned ambiguous semantics.

The API describes a capability rather than exposing manual materialization
state. The normal index status surface returns the public hypervector config,
the derived semantic configuration fingerprint, generation-scoped
produced/skipped/terminal-failed/pending coverage, replay target and applied
sequence, backfill state/progress, vector cardinality, and disk usage. This
gives users one readiness model shared with ordinary managed embeddings.

HDC-specific encode/query time, projection-coordinate count, rejected-resource
count, artifact bytes, and query fallback counters are not yet attributed
separately. Those should be metrics and status extensions on the dense index,
not a second HDC administration API.

## Storage and Precision

### Authoritative artifact

HDC algebra should compute in `f32` initially. A 10,000-dimensional vector is
40,000 raw bytes before metadata and storage framing.

The complete node vector is generally not bipolar because it contains bundled
structural counts and a weighted semantic term. It therefore cannot be reduced
to one bit per coordinate without losing the authoritative additive state.

The first Antfly implementation should use the existing dense artifact codec
and exact-vector loading/reranking path. `f16` authoritative artifacts may be
evaluated later, but adopting them requires explicit codec, error, replay, and
reranking tests; it should not be assumed solely because the reference demo
uses Arrow `float16`.

### Packed bipolar candidate representation

A strictly bipolar hypervector can encode each coordinate in one bit:

```text
+1 -> 1
-1 -> 0
```

At 10,000 dimensions this reduces one vector from 40,000 bytes of `f32` to
10,000 bits, or 1,250 bytes, before framing: a theoretical 32x reduction.
With this encoding, coordinate-wise bipolar binding is XNOR, disagreement is
XOR, and similarity can use word-sized XOR plus population count. For bipolar
vectors `a` and `b` with dimension `D`:

```text
dot(a, b) = D - 2 * hamming_distance(a, b)
```

This is still a dense representation: every coordinate is present. It is not a
fit for the sparse-vector index, which would have to store almost every
coordinate and its index.

Packed bits cannot directly preserve the current authoritative complete-node
vector. Structural bundling requires counters before thresholding, and
association-count normalization plus the weighted semantic term produces
real-valued magnitudes. Taking only the final sign changes cosine geometry and
loses evidence strength; it is therefore a new retrieval approximation and
coordinate-system contract, not a lossless codec.

A safe experiment is a three-level path:

```text
authoritative complete f32 hypervector
  -> packed sign sketch for inexpensive candidate selection
  -> exact cosine rerank from the authoritative f32 artifact
```

That path must be compared directly with HBC/RaBitQ for recall, latency,
storage, candidate budget, and update cost. If persisted, the bit mapping,
threshold/tie algorithm, packing order, and version must participate in index
identity. Packed bipolar storage should not become a public configuration or a
specialized physical layout until it demonstrates a durable advantage over
RaBitQ on representative complete-HDC distributions.

### Search index

RaBitQ is already present in the HBC dense index. Its compressed representation
is a candidate-search structure, not the authoritative HDC vector. Exact
reranking can load the durable vector according to the configured rerank policy.

The benchmark must measure:

- RaBitQ recall against exact cosine for HDC distributions;
- packed-sign candidate recall and throughput against both exact cosine and
  RaBitQ;
- sensitivity to raw bundle magnitude and semantic weight;
- the cost of exact reranking 10,000-dimensional candidates;
- index, artifact, cache, and working-memory bytes;
- whether random orthogonal transformation changes HDC recall or is redundant
  for already distributed coordinates.

The pinned WANDS run below now measures the candidate-only packed-sign path.
It is deliberately not an authoritative codec: exact reranking still reads the
complete `f32` vector.

### Projection artifact

For a 768-to-10,000 Rademacher projection, an `int8` matrix is approximately
7.68 MB before framing, or about 0.96 MB if packed to one bit per sign. The
initial Zig primitive generates deterministic matrix rows as a stream and keeps
only the 40 KB `f32` output accumulator for 10,000 dimensions. This removes the
matrix allocation from the hot-path working set while retaining a checksum over
the logical matrix. A materialized or SIMD-packed projection artifact remains a
benchmark-driven optimization.

The projection should be:

- addressed by encoder identity;
- checksum-validated at load;
- shared by matching enrichments on a process if materialized;
- accounted through the resource manager;
- bounded in batch working memory;
- immutable for the lifetime of an index generation.

## Update and Delete Semantics

There are two valid maintenance strategies.

### Recompute the complete node

On a document overwrite:

1. canonicalize all included properties;
2. rebuild the structural sum;
3. rebuild semantic text and embedding if it changed;
4. combine the channels;
5. replace the durable artifact and vector-index entry.

This is the safest first implementation. Most nodes have few properties, and
semantic regeneration often dominates cost.

### Incremental structural maintenance

Because raw bundling is additive, structural associations may be compensated:

```text
next_structural = prior_structural
                - removed_associations
                + added_associations
```

This optimization is valid only when the prior raw structural accumulator is
available and encoder identity is unchanged. It does not eliminate semantic
re-embedding when the semantic document changes. It should be added only after
recompute performance is measured.

Document deletion removes the artifact and vector-index entry by the same source
generation used for other derived indexes. Edge deletion independently removes
any optional edge HDC artifact; it never derives graph deletion from vector
arithmetic.

## Algebraic Planner Integration

### Phase 0: existing result-set handoff

The existing request executor runs vector retrieval and exposes
`$embeddings_results`; graph execution resolves that named set into node keys.
This is sufficient to reproduce the motivating flow and should be the first
correctness target.

### Phase 1: typed composed program

The planner should eventually express the composition in one typed program:

```text
input query_hv(dim{})

step 0:
  vector_search(query_hv, layout=location_hdc)
  -> candidates(doc{}, score{})

step 1:
  graph_traverse(
    graph=travel_graph,
    starts=project(doc{}, step0),
    edge_type=VISITED,
    direction=in
  )
  -> paths(seed{}, doc{}, provenance{})

step 2:
  join(paths(seed{}), candidates(doc{}, score{}), seed=doc)
  -> results(seed{}, doc{}, score{}, provenance{})
```

The current IR has `vector_search`, `graph_traverse`, step references, and
`doc`/`score`/`src`/`dst` dimensions. The extension should focus on:

- proving that the graph step consumes the vector step's document axis;
- representing top-k/limit and score ownership in typed metadata;
- retaining seed identity through traversal;
- attaching exact graph provenance without declaring cosine a semiring;
- validating both vector and graph physical owners;
- serializing compatible distributed program envelopes;
- preserving existing fallback for unsupported graph shapes.

No new HDC-specific tensor fragment is needed for candidate retrieval.
`vector_search` is semantically correct when its metadata identifies the HDC
encoder space.

HDC-specific fragments become reasonable only for server-side operations:

```text
hdc_bind
hdc_bundle
hdc_unbind
hdc_permute
hdc_prototype_update
```

Those operations require their own dimensional, encoder-identity, numeric, and
update-law proofs. They should not be added speculatively.

### Exact constraints

Algebraic `docfact` and `pathfact` filters can prune the HDC vector search to an
exact document set before ranking:

```text
exact symbolic filter
  -> native include doc ids
  -> HDC cosine search within candidates
  -> graph traversal from top-k
```

This is preferable to encoding hard security, tenancy, lifecycle, or type
constraints into the hypervector. Required exact constraints must fail closed
if their algebraic lifecycle or distributed envelope is stale.

## Distributed Execution

The coordinator should compute a query hypervector once per encoder identity and
fan it out using the existing dense query protocol. Shards must validate:

- index owner and generation;
- dimensions and metric;
- encoder fingerprint;
- required symbolic constraint envelope;
- identity read generation.

Shard-local top-k results merge by the existing dense ranking contract. The
coordinator then supplies the selected canonical document identities to graph
execution.

If vector and graph layouts are sharded differently, the planner must make the
handoff explicit. A vector hit is a document identity, not a shard-local vector
id. Graph traversal must resolve the canonical identity under the request's
identity generation.

Projection or embedder drift on any shard is a hard incompatibility. A shard
must not silently compute a query vector in a different coordinate system or
mix old and new HDC generations in one result set.

## Correctness and Safety Invariants

1. **Candidate is not fact.** HDC scores only rank where to begin exact work.
2. **Direction stays explicit.** MAP binding is commutative; graph source and
   target are authoritative.
3. **Encoder identity is complete.** Same dimension does not imply compatible
   vectors.
4. **Model drift fails closed.** A changed semantic model or projection requires
   rebuild before generated semantic queries use the index.
5. **Base documents remain canonical.** HDC artifacts are derived and
   rebuildable.
6. **Exact filters stay exact.** Authorization, tenancy, deletion visibility,
   and hard query predicates cannot be approximated by HDC similarity.
7. **Graph paths remain exact.** Similarity never materializes an asserted edge
   without a separate explicit product operation and provenance.
8. **Scores retain provenance.** A propagated seed score identifies the fuzzy
   seed that produced it and is not presented as edge confidence.
9. **Resource use is bounded.** Projection, batch tensors, exact rerank vectors,
   and rebuild state are resource-manager accounted.
10. **Replay is deterministic.** Rebuilding from the same source generation and
    encoder identity produces byte-compatible coordinate semantics and
    tolerance-compatible vectors.

## Evaluation Plan

The first question is not whether HDC can be stored. It can. The question is
whether it provides enough value over ordinary dense semantic retrieval to
justify its additional dimensions, encoding lifecycle, and update cost.

### Baselines

Measure at least:

1. original semantic embedding, exact cosine, then graph expansion;
2. original semantic embedding, existing HBC/RaBitQ, then graph expansion;
3. semantic-only HDC projection, exact cosine, then graph expansion;
4. semantic-only HDC projection, HBC/RaBitQ, then graph expansion;
5. complete structural-plus-semantic node HDC, exact cosine, then graph;
6. complete node HDC, HBC/RaBitQ, then graph;
7. structured exact filter plus original semantic embedding plus graph;
8. original semantic embedding plus transparent structured score fusion plus
   graph;
9. structured exact filter plus complete node HDC plus graph.

The semantic-only HDC case isolates the cost and recall effect of random
projection. The complete-node case measures whether HDC composition changes
entry-point quality. The structured-score baseline is required because exact
filters intentionally have no soft-fallback behavior; comparing HDC only with a
fail-closed filter would incorrectly attribute the general value of soft
evidence to HDC's representation.

### Workloads

Include:

- natural-language descriptions that omit the target entity name;
- partial structured descriptions;
- contradictory structural and semantic evidence;
- near-miss locations;
- varying structural property counts;
- multi-tenant exact filters;
- inserts, overwrites, deletes, and schema/encoder drift;
- graph fanout from one and many fuzzy seeds;
- enough nodes to require approximate search;
- skewed node types and fields absent from some rows.

### Measurements

Retrieval quality:

- Recall@k, MRR, and nDCG for intended entry nodes;
- end-to-end graph-answer precision and recall;
- RaBitQ ANN recall against exact HDC cosine;
- result stability across replay and distributed merge;
- sensitivity to dimension, semantic weight, and field selection.

Cost:

- encode time per document and batch;
- query embedding and projection time;
- vector search, exact rerank, graph traversal, and end-to-end latency;
- authoritative artifact bytes per document;
- quantized index bytes per document;
- projection/cache/working-memory peaks;
- write amplification and rebuild duration;
- overwrite/delete churn cost;
- distributed query bytes for a 10,000-dimensional query vector.

Operational behavior:

- stale/rebuild-required transitions;
- interrupted backfill recovery;
- mixed-generation rejection;
- resource-limit fallback;
- unavailable embedder behavior;
- query and graph fallback counters.

### Pinned reference regression

The motivating repository's corpus at commit
[`de6abda83ee5fa1e46d540fc3ed6c74eecd15b73`](https://github.com/prrao87/hdc-lancedb/tree/de6abda83ee5fa1e46d540fc3ed6c74eecd15b73)
contains three locations, nine discriminative semantic-ranking queries, and
three semantic-to-`VISITED` path checks. With `nomic-embed-text`, the upstream
runner reports 9/9 top-1 semantic queries and 3/3 exact path checks.

That fixture is valuable as a compatibility regression and supplies the
Seattle query shape used by the managed Antfly test. It is not a held-out
benchmark—the upstream README says so explicitly—and cannot establish
production quality or ANN behavior at scale. A release benchmark should consume
the pinned CSVs and provider model digest as an external fixture, then add a
larger independently labeled corpus rather than treating those twelve checks as
a graduation threshold.

### Initial encoder baseline

On 2026-07-26, the ReleaseFast streaming implementation on the local `arm64`
development host produced this smoke baseline:

```text
zig build hdc-bench -Doptimize=ReleaseFast
projection 768 -> 10,000:       2.133 ms/query
projection throughput:          3.60 billion matrix coordinates/second
projection checksum:            0.384 ms
8 structural associations:      44.640 us/document
authoritative vector bytes:      40,000
projection working bytes:        40,000
logical packed matrix bytes:     960,000

synthetic complete-HDC RaBitQ:
  vectors / queries / k:         256 / 20 / 10
  raw approximate recall@10:     0.835
  recall@10 after 40 reranks:    1.000
  exact scoring:                 0.146 ms/query
  quantized scoring:             0.058 ms/query
  exact rerank of 40:            0.025 ms/query
  exact vectors:                 10,240,000 bytes
  quantized set:                 365,632 bytes
  measured compression:          28.0x

candidate-budget sweep:
  candidates:                    10       20       40       80
  reranked recall@10:            0.835    1.000    1.000    1.000
  rerank ms/query:               0.007    0.014    0.025    0.049
```

This is evidence for the primitive, not a production SLO. CI and release
guardrails should use representative production hosts and must add embedding,
dense search, reranking, graph traversal, concurrent ingestion, and resource
pressure. The RaBitQ run uses random provider vectors plus deterministic
structural associations, so it demonstrates that the existing quantizer can
operate on complete HDC distributions; it does not establish application
quality or a safe production candidate budget. The benchmark is parameterized
so dimension, association-count, dataset-size, semantic-weight, query-count,
and `k` sweeps can be recorded without changing code.

### Controlled composition diagnostic

The benchmark also contains a deterministic diagnostic with 262 documents
drawn from 12 semantic topics, 6 regions, and 4 features. Twenty-six of the 288
possible topic/region/feature combinations are deliberately absent. It evaluates:

- 262 hard-match queries with one exact relevant seed and one exact graph
  answer owned by that seed;
- 26 soft-fallback queries where the requested combination is absent and
  same-topic, same-region alternatives are relevant;
- original 768-dimensional semantic vectors;
- those vectors with exact region and feature filters;
- those vectors with a transparent score
  `cosine + 0.25 * region_match + 0.25 * feature_match`;
- semantic-only and complete 10,000-dimensional HDC vectors.

The exact graph traversal is not timed in this diagnostic. Because every
relevant hard-match seed owns one exact answer, hard-match top-1 is also the
controlled graph-answer accuracy; production evidence must still execute the
distributed traversal.

On the 2026-07-26 local `arm64` ReleaseFast run with the public HDC semantic
weight of 8:

| Method | Hard top-1 / MRR / hit@10 | Soft top-1 / MRR / hit@10 | Exact scoring ms/query |
| --- | --- | --- | ---: |
| embedding | 0.046 / 0.162 / 0.485 | 0.154 / 0.352 / 0.923 | 0.017 |
| embedding + exact filter | 1.000 / 1.000 / 1.000 | 0.000 / 0.000 / 0.000 | 0.001 |
| embedding + structured score | 1.000 / 1.000 / 1.000 | 0.462 / 0.671 / 1.000 | 0.018 |
| semantic-only HDC | 0.046 / 0.161 / 0.466 | 0.154 / 0.400 / 0.846 | 0.149 |
| complete HDC, text query | 0.042 / 0.160 / 0.458 | 0.231 / 0.450 / 0.846 | 0.146 |
| complete HDC, structured query | 1.000 / 1.000 / 1.000 | 0.154 / 0.336 / 0.923 | 0.149 |

The HDC semantic-weight sweep exposes a tradeoff rather than a clear win:

| HDC semantic weight | Hard top-1 / MRR | Soft top-1 / MRR |
| ---: | --- | --- |
| 4 | 0.939 / 0.966 | 0.000 / 0.078 |
| 8 | 1.000 / 1.000 | 0.154 / 0.336 |
| 12 | 1.000 / 1.000 | 0.423 / 0.636 |
| 16 | 1.000 / 1.000 | 0.423 / 0.630 |
| 24 | 1.000 / 1.000 | 0.423 / 0.630 |
| 32 | 0.992 / 0.996 | 0.500 / 0.668 |

At weight 32, complete HDC has slightly higher soft top-1 than structured score
fusion, but slightly lower soft MRR, lower hard accuracy, 13 times as many
stored coordinates, and about 8 times the exact scoring time in this scan. At
weight 8, the structured-score baseline dominates complete HDC on soft quality
while tying its hard quality.

This result does not graduate HDC and should not be used to select a production
default. The workload is synthetic, small enough for exact scans, and designed
to test composition mechanics rather than application semantics. Its useful
conclusion is negative: complete HDC has not yet demonstrated unique retrieval
value over a simpler representation. The next experiment must reproduce or
reverse that result on independently labeled, representative data using the
same ANN candidate budgets and end-to-end graph traversal. Hard constraints
must continue to use exact filters regardless of the result.

### Pinned WANDS experiment

The representative external experiment uses the MIT-licensed
[Wayfair ANnotation Dataset](https://github.com/wayfair/WANDS) at revision
`3b74dcf4ba29ab8ff3e6a50b5b09fc627cb882b5`. WANDS contains 42,994 products,
480 real product-search queries, and 233,448 human `Exact`, `Partial`, or
`Irrelevant` judgments. Wayfair designed its cross-referencing process to
increase judgment completeness and make the corpus discriminate between
retrieval systems.

The semantic model is
[BAAI/bge-small-en-v1.5](https://huggingface.co/BAAI/bge-small-en-v1.5) at
revision `5c38ec7c405ec4b44b94cc5a9bb96e735b38267a`. The fixture generator rejects
source or model files whose SHA-256 digests do not match the pinned revisions.
It embeds:

- `product_name + product_description` as the semantic document, truncated to
  128 tokens;
- the published query with BGE's retrieval-query instruction;
- `product_class` and `query_class` as the only structured association;
- the stored category hierarchy as the exact graph answer reached from a
  selected product seed.

The benchmark uses gains `Exact=2`, `Partial=1`, and `Irrelevant=0`. It ranks
the full 42,994-product catalog and treats unjudged pairs as irrelevant. That is
consistent with WANDS's completeness-oriented candidate mining, but remains an
explicit evaluation assumption. Query IDs divisible by five form a 96-query
validation split; the remaining 383 queries with at least one relevant judgment
are an untouched holdout. Hyperparameters are selected only by validation
NDCG@10:

- complete HDC semantic-weight candidates: `4, 8, 12, 16, 24, 32`; selected
  weight: `4`;
- structured-fusion boost candidates:
  `0, .01, .025, .05, .075, .1, .15, .2, .25, .35, .5, .75, 1`; selected
  boost: `.075`.

The 2026-07-26 local `arm64` ReleaseFast exact holdout result is:

| Method | NDCG@10 | MRR@10 | Recall@10 | Relevant top-1 | Graph answer top-1 | Score ms/query |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| embedding | 0.7387 | 0.9139 | 0.0576 | 0.8799 | 0.9426 | 0.94 |
| embedding + exact class filter | 0.7293 | 0.8974 | 0.0542 | 0.8695 | 0.9582 | 0.05 |
| embedding + tuned class score | **0.7688** | 0.9256 | **0.0610** | 0.8982 | **0.9739** | **0.94** |
| complete HDC + class, weight 4 | 0.7643 | **0.9294** | 0.0600 | **0.9060** | **0.9739** | 24.51 |

HDC's small MRR and relevant-top-1 gains do not constitute a Pareto
improvement: tuned fusion has higher NDCG and recall, graph-answer accuracy is
identical, and HDC exact scoring is about 26 times slower. The HDC query also
adds approximately 1.09 ms for deterministic 384-to-10,000 projection after
the common embedding step.

Storage and RaBitQ measurements over all 42,994 products are:

| Representation | Authoritative bytes | RaBitQ bytes | Quantization | Approximate ms/query |
| --- | ---: | ---: | ---: | ---: |
| 384-dimensional embedding | 66,038,784 | 2,753,152 | 13.6 ms | 3.47 |
| 10,000-dimensional complete HDC | 1,719,760,000 | 54,728,368 | 552.1 ms | 7.63 |

At the same 50-candidate RaBitQ budget, a production-shaped baseline first
retrieves semantic candidates and then applies the class boost during exact
rerank:

| Method | NDCG@10 | MRR@10 | Recall@10 | Relevant top-1 | Graph answer top-1 | Exact top-10 recall |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| embedding candidates + structured rerank | **0.7674** | **0.9299** | 0.0597 | **0.9060** | **0.9739** | 0.8825 |
| complete HDC candidates + exact rerank | 0.7643 | 0.9294 | **0.0600** | **0.9060** | **0.9739** | 0.9995 |

HDC preserves its own exact top ten more faithfully, but that does not improve
the labelled application result. The simpler candidate-plus-rerank path is
slightly better on NDCG and MRR, ties top-1 and graph-answer accuracy, uses
about one twentieth of the quantized bytes, and takes less than half the
approximate scoring time in this direct RaBitQ scan. At 100 candidates the
baseline reaches 0.7700 NDCG@10 and 0.9765 graph-answer top-1, so the conclusion
does not depend on a single narrow budget.

The 2026-07-27 follow-up also thresholds each complete HDC coordinate at zero,
packs the signs low-coordinate-first into little-endian `u64` words, selects
candidates by Hamming distance, and exact-reranks from the unchanged complete
`f32` vectors. At 10,000 dimensions the padded sketch occupies 157 words, or
1,256 bytes per product:

| 50-candidate representation | Candidate bytes | Build | Approximate ms/query | Exact top-10 recall | NDCG@10 | Graph answer top-1 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| embedding RaBitQ, then structured rerank | **2,753,152** | 13.8 ms | **3.33** | 0.8825 | **0.7674** | **0.9739** |
| complete HDC RaBitQ | 54,728,368 | **625.9 ms** | 7.69 | **0.9995** | 0.7643 | **0.9739** |
| complete HDC packed bipolar | 54,000,464 | 1,587.6 ms | 5.25 | 0.9582 | 0.7651 | **0.9739** |

Packed bipolar scanning is approximately 32% faster than HDC RaBitQ in this
scalar full-corpus benchmark, but it gives up 4.13 percentage points of exact
top-10 recall at 50 candidates. The packed index is only 1.3% smaller than HDC
RaBitQ because RaBitQ already stores approximately one code bit per
10,000-dimensional coordinate plus per-vector metadata. It remains about 20
times larger and 58% slower to scan than the 384-dimensional embedding
candidate index. Its marginal application-metric differences do not reverse
the no-go: at 100 candidates the tuned baseline reaches 0.7700 NDCG@10 and
0.9765 graph-answer top-1, versus 0.7647 and 0.9739 for packed HDC.

A deterministic paired bootstrap over the 383 holdout queries confirms that
neither HDC candidate representation has demonstrated an application-quality
advantage. At 50 candidates, packed HDC minus the production-shaped baseline
has mean NDCG@10 delta `-0.002342` with 95% interval
`[-0.008816, 0.004046]`; the graph-answer delta and interval are exactly zero.
Across 50, 100, and 200 candidates, every HDC NDCG interval includes zero and
every graph-answer interval is either tied or includes zero on its upper bound.
The experiment therefore provides no statistically significant evidence for
graduating HDC.

#### Dimension and seed robustness

A follow-up selected the representation only by validation NDCG@10. At
coordinate seed 13, semantic weight 4 won at every tested dimension:

| HDC dimensions | Validation NDCG@10 |
| ---: | ---: |
| 2,000 | 0.7143 |
| 4,000 | 0.7326 |
| 8,000 | **0.7349** |
| 10,000 | 0.7332 |

At the selected 8,000 dimensions, coordinate seeds 13, 29, and 47 reached
validation NDCG@10 of 0.7349, **0.7382**, and 0.7378 respectively. Seed 29 was
therefore selected before examining its holdout result. The RaBitQ transform
seed remains independently fixed at 13 so changing HDC coordinates does not
move the baseline candidate index.

The selected 8,000-dimensional, seed-29, weight-4 setting does not reverse the
result. At 50 candidates:

| Method | Candidate bytes | NDCG@10 | MRR@10 | Relevant top-1 | Graph answer top-1 | Approximate ms/query |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| embedding candidates + structured rerank | **2,753,152** | **0.7674** | **0.9299** | **0.9060** | **0.9739** | **3.46** |
| selected HDC RaBitQ | 43,713,904 | 0.7642 | 0.9207 | 0.8903 | **0.9739** | 6.67 |
| selected HDC packed bipolar | 42,994,000 | 0.7618 | 0.9189 | 0.8903 | **0.9739** | 4.61 |

Relative to the baseline, HDC RaBitQ's paired NDCG delta is `-0.003185`
with 95% interval `[-0.010541, 0.003904]`; packed HDC's delta is `-0.005667`
with interval `[-0.012515, 0.000916]`. Both graph-answer intervals span zero.
At 200 candidates packed HDC is significantly worse on NDCG
(`-0.005429`, interval `[-0.010634, -0.000209]`). Dimension and seed tuning
reduces the original HDC resource cost by 20%, but provides no significant
quality advantage and leaves the candidate state about 16 times larger than
the embedding baseline.

The RaBitQ measurement deliberately isolates quantization, candidate selection,
and exact reranking over the entire corpus. It is not an HBC tree-topology
benchmark. Because the application result is already a no-go, building a
production HBC tree for HDC would not change the decision; HBC-tree validation
becomes necessary only if a later representative workload first demonstrates a
quality advantage.

Reproduction:

```text
# Pull WANDS revision 3b74dcf... into /tmp/antfly-hdc-wands.
./zig-out/bin/antfly inference pull \
  hf:BAAI/bge-small-en-v1.5:native \
  --tasks embed \
  --models-dir /tmp/antfly-hdc-models

UV_CACHE_DIR=/tmp/uv-cache uv run --project e2e/inference \
  python tools/prepare_hdc_wands_fixture.py \
  --dataset-dir /tmp/antfly-hdc-wands \
  --model-dir /tmp/antfly-hdc-models/BAAI/bge-small-en-v1.5 \
  --output /tmp/antfly-hdc-wands/wands-bge-small.afhw \
  --batch-size 64 \
  --max-tokens 128 \
  --threads 8

zig build hdc-wands-bench -- \
  --fixture /tmp/antfly-hdc-wands/wands-bge-small.afhw \
  --hdc-dims 10000 \
  --semantic-weights 4 \
  --fusion-weight 0.075 \
  --ann-semantic-weight 4 \
  --ann-candidates 50,100,200

# Validation-selected dimension/coordinate seed, with the ANN seed controlled
# independently:
zig build hdc-wands-bench -- \
  --fixture /tmp/antfly-hdc-wands/wands-bge-small.afhw \
  --hdc-dims 8000 \
  --semantic-weights 4 \
  --fusion-weight 0.075 \
  --seed 29 \
  --ann-seed 13 \
  --ann-semantic-weight 4 \
  --ann-candidates 50,100,200
```

The benchmark implementation and fixture helper are:

- [WANDS quality and RaBitQ benchmark](pkg/antfly/src/bench/hdc_wands_bench.zig)
- [pinned WANDS/BGE fixture generator](tools/prepare_hdc_wands_fixture.py)
- [machine-readable WANDS result](bench/baselines/hdc-wands-bge-small-2026-07-26.json)
- [machine-readable packed-bipolar follow-up](bench/baselines/hdc-wands-packed-bipolar-2026-07-27.json)
- [machine-readable dimension/seed follow-up](bench/baselines/hdc-wands-tuned-representation-2026-07-27.json)

### Graduation criteria

The HDC enrichment may become engine-owned when:

- it has an end-to-end use case where complete-node HDC materially improves
  quality or enables composition unavailable from the baseline;
- exact graph answers remain equivalent for the same selected seeds;
- vector index recall is acceptable under production quantization settings;
- artifact, index, latency, rebuild, and churn costs have explicit guardrails;
- encoder drift and distributed generation checks fail closed;
- the public API can describe the capability without exposing manual
  materialization mechanics.

A dedicated hypervector index layout is considered only when the engine-owned
enrichment passes those criteria and profiling shows the existing dense layout
is the dominant unacceptable cost.

## Implementation Sequence

### Phase 0: external feasibility spike

- Reproduce the reference MAP encoder outside Antfly.
- Store complete node hypervectors through an external embeddings index.
- Query with a packed precomputed vector.
- Expand from `$embeddings_results` through the existing graph query API.
- Add an end-to-end fixture that proves fuzzy seed selection and exact edge
  traversal are separate.
- Run all baselines from the evaluation plan on a bounded dataset.

Exit: a reproducible quality/cost report, not a new public feature.

### Phase 1: engine-owned HDC enrichment

- Add a versioned HDC generator and encoder identity.
- Expose a logical `hypervector` index and lower it to `.dense_vector`.
- Reuse canonical typed/path projection for structural inputs.
- Integrate semantic embedding and projection artifact caching.
- Carry the full coordinate fingerprint in artifacts, query caches, routed
  shard requests, and algebraic vector-worker envelopes; reject mismatches.
- Compose optional exact-path structured query associations in the document
  coordinate system.
- Persist derived HDC artifacts through existing artifact lifecycle machinery.
- Backfill, resume, rebuild, quarantine, and status like other managed
  enrichments.
- Feed existing dense indexes without adding a new physical index kind.

Exit: deterministic rebuild and lifecycle tests plus benchmark guardrails.

### Phase 2: planner-owned composition

- Build a typed vector-candidate-to-graph tensor program.
- Prove both access paths and the document-identity handoff.
- Preserve seed score and exact graph provenance.
- Export/validate the distributed program envelope.
- Retain explicit fallbacks for unsupported graph shapes.

Exit: local and distributed equivalence with the existing result-set handoff.

### Phase 3: optional relationship HDC

- Add only for a demonstrated associative edge/path retrieval use case.
- Key every vector by an existing edge identity.
- Keep source, target, direction, and type in graph storage.
- Avoid all-pairs or arbitrary path materialization.
- Evaluate selected/top-k derived adjacency separately from asserted graph
  edges.

Exit: evidence that relationship HDC adds value beyond node seeding and exact
graph traversal.

### Phase 4: conditional specialized physical layout

Potential optimizations include packed bipolar operands, SIMD bind/unbind,
specialized bundle accumulators, compressed authoritative sums, GPU projection,
and prototype-update storage. Each requires a benchmark showing a durable win
over the dense-vector path and a precise lifecycle contract.

## Alternatives Considered

### Put all HDC coordinates in the algebraic sidecar

Rejected. The sidecar is optimized around sparse symbolic facts and
materialized exact-law tensors. Complete HDC vectors are dense approximate
features. Per-coordinate rows would create extreme row and write amplification,
and ordinary cosine ranking would still require a vector executor.

### Add a specialized HDC physical index immediately

Rejected. The public logical `hypervector` type is useful because its identity
and structured query semantics differ from ordinary embeddings. A new physical
kind is not: existing dense HBC and RaBitQ provide candidate retrieval and
already own lifecycle, distributed execution, status, filtering, and ANN work.
The logical type therefore lowers to `.dense_vector` until profiling proves a
specialized layout is necessary.

### Use only ordinary semantic embeddings

This is the baseline and may be the right answer for slide 13 alone. A random
sign projection does not introduce semantic information. HDC is justified by
its ability to compose structural roles, values, relationships, sequences, or
online memories in one coordinate system, not by dimensional expansion itself.

### Replace graph traversal with S-P-O similarity

Rejected. Similarity is approximate, MAP binding is commutative, and a retrieved
path vector does not have the same truth semantics as a stored directed edge.
HDC may select or rank candidate paths, but exact graph facts remain
authoritative.

### Materialize similarity edges

Deferred and separate from this proposal. A product may explicitly create a
derived top-k similarity graph with provenance and bounded degree, but those
edges are not asserted domain facts. Such a graph needs its own refresh,
staleness, threshold, and user-visible semantics.

## Open Questions

- Which canonical JSON value kinds should participate in structural HDC by
  default?
- Should HDC structural fields be schema-declared only, or may path observation
  recommend them?
- Is one complete node vector preferable to separate structural and semantic
  vector columns with late score fusion?
- What dimension/weight combinations survive realistic property-count skew?
- Should the projection matrix be stored or only regenerated and checksum
  verified?
- Does existing artifact `f32` storage meet scale targets, or is `f16`
  authoritative storage worth the extra error contract?
- How should graph result scoring collapse multiple fuzzy seeds?
- Is query-vector projection best placed at the coordinator, inference service,
  or a reusable managed embedder wrapper?
- Do edge/path hypervectors have a concrete query that cannot be served by node
  HDC plus exact graph execution?
- At what measured scale, if any, does a specialized hypervector layout beat
  existing HBC/RaBitQ enough to justify a new index kind?

## Antfly Code References

- [Algebraic design and search-engine ownership](ALGEBRAIC.md)
- [Algebraic tensor IR](pkg/antfly/src/storage/db/algebraic/ir.zig)
- [Algebraic planner](pkg/antfly/src/storage/db/algebraic/planner.zig)
- [Sparse formal-vector implementation](pkg/antfly/src/storage/db/algebraic/vector.zig)
- [Graph query model and traversal](pkg/antfly/src/graph/query.zig)
- [Graph result-set handoff](pkg/antfly/src/storage/db/query/graph_exec.zig)
- [Dense index catalog and configuration](pkg/antfly/src/storage/db/catalog/index_manager.zig)
- [Managed embedder configuration and query projection](pkg/antfly/src/inference/managed_embedder.zig)
- [Managed enrichment execution](pkg/antfly/src/storage/db/enrichment/enrichment_runtime.zig)
- [HDC MAP encoder and JSON composition](pkg/antfly/src/hdc.zig)
- [HDC microbenchmark](pkg/antfly/src/bench/hdc_bench.zig)
- [Public hypervector index schema](../specs/openapi/antfly/indexes.yaml)
- [HBC storage and RaBitQ integration](pkg/antfly/src/storage/hbc_adapter.zig)
- [Embedding enrichment interface](pkg/antfly/src/storage/db/enrichment/embedder.zig)
- [Derived artifact codec](pkg/antfly/src/storage/db/enrichment/artifact_codec.zig)
- [Public query contract](pkg/antfly/src/api/query_contract.zig)

## External References

### Motivating implementation

- Prashanth Rao, [Multimodal Knowledge Graphs for
  Agents](https://prrao87.github.io/slidev-talks/graphcon-2026/#/13), GraphCon
  2026, slide 13.
- [GraphCon slide
  source](https://github.com/prrao87/slidev-talks/blob/main/talks/graphcon-2026/slides.md#L1684-L1759),
  including the semantic-query-to-exact-graph flow.
- [`prrao87/hdc-lancedb`](https://github.com/prrao87/hdc-lancedb/tree/de6abda83ee5fa1e46d540fc3ed6c74eecd15b73),
  pinned reference code and data.
- [MAP token, binding, bundling, and triple
  encoder](https://github.com/prrao87/hdc-lancedb/blob/de6abda83ee5fa1e46d540fc3ed6c74eecd15b73/src/hdc/core.py).
- [Structural and semantic complete-node
  encoder](https://github.com/prrao87/hdc-lancedb/blob/de6abda83ee5fa1e46d540fc3ed6c74eecd15b73/src/hdc/location.py).
- [Semantic embedding and deterministic random
  projection](https://github.com/prrao87/hdc-lancedb/blob/de6abda83ee5fa1e46d540fc3ed6c74eecd15b73/src/hdc/semantic.py).
- [Node/edge artifact encoding and
  persistence](https://github.com/prrao87/hdc-lancedb/blob/de6abda83ee5fa1e46d540fc3ed6c74eecd15b73/src/hdc/encode.py).
- [HDC candidate search followed by exact graph
  traversal](https://github.com/prrao87/hdc-lancedb/blob/de6abda83ee5fa1e46d540fc3ed6c74eecd15b73/src/hdc/retrieve.py).
- [Storage and compression
  notes](https://github.com/prrao87/hdc-lancedb/blob/de6abda83ee5fa1e46d540fc3ed6c74eecd15b73/COMPRESSION_TRICKS.md).

### HDC and VSA background

- Pentti Kanerva, [Hyperdimensional Computing: An Introduction to Computing in
  Distributed Representation with High-Dimensional Random
  Vectors](https://doi.org/10.1007/s12559-009-9009-8), Cognitive Computation,
  2009.
- Denis Kleyko et al., [Vector Symbolic Architectures as a Computing Framework
  for Emerging Hardware](https://doi.org/10.1109/JPROC.2022.3209104),
  Proceedings of the IEEE, 2022.
- Denis Kleyko et al., [A Survey on Hyperdimensional Computing aka Vector
  Symbolic Architectures, Part I: Models and Data
  Transformations](https://arxiv.org/abs/2111.06077), 2021.
- Mike Heddes et al., [Torchhd: An Open Source Python Library to Support
  Research on Hyperdimensional Computing and Vector Symbolic
  Architectures](https://www.jmlr.org/papers/v24/23-0300.html), Journal of
  Machine Learning Research, 2023.
- [Torchhd documentation](https://torchhd.readthedocs.io/en/stable/).
