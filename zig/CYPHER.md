# Antfly Graph: Cypher compatibility, SQL composition, and exploration UX

Status: production server and embedded implementation, 2026-10-15. Authenticated distributed
queries, native SQL GRAPH_TABLE, coordinated writes/search, and the query
workbench are implemented. The compatibility matrix records supported behavior,
qualification, and explicit limits against the pinned Cypher baseline.
Repository observations were checked against `origin/main` at
`dc4e34c497`.

## Implementation status

The worktree contains an Antfly-owned generated Cypher grammar, a catalog-free
AST and compiler, scope binding, typed graph values, a budgeted execution engine with cached, cancellable full-string regex predicates,
and a native immutable-cut adapter. Graph definitions are persisted through the
system catalog and exposed through generated HTTP catalog routes and SDK types.
The existing JSON pattern engine now follows Cypher relationship uniqueness in
each MATCH scope and accepts explicit zero-hop bounds.

| Surface | Implemented behavior | Qualification and compatibility bounds |
| --- | --- | --- |
| Grammar/compiler | Generated conflict-free grammar; spans; Unicode strings and identifiers; owned AST; named and positional parameters; compile budgets | Full selected-version conformance suite, temporal/spatial types, additional Java regex constructs, legacy integer id() |
| Read execution | MATCH/OPTIONAL MATCH, directional and variable-length paths, zero-hop paths, relationship uniqueness, UNWIND, list and pattern comprehensions/slices, chained comparisons, all/any/none/single, reduce, map projections, correlated read EXISTS, WITH/RETURN, grouping, DISTINCT, ORDER BY/SKIP/LIMIT, UNION, correlated CALL | Streaming/spill, optimized shortest paths, schema type binding, comprehensive expression compatibility |
| Values | Qualified node and relationship identities, paths, lists/maps, three-valued equality and partial comparisons, numeric comparison, Cypher ORDER BY type ordering, tagged JSON | Decimal-string signed integers, typed parameter maps and collision-free envelopes implemented; SQL graph datum encoding implemented; pgwire JSONB entities, prepared parameters, NULLs, and ordinary-table joins qualified |
| Write execution | CREATE/MERGE/SET/REMOVE/DELETE/DETACH DELETE hooks and statement overlays; native graph effects survive transaction prepare/reopen/commit/abort; source range and guard regressions qualified | Production authenticated distributed provider, principal-bound durable receipts, duplicate request-ID rejection, and concurrent MERGE qualified through the 66-check real three-metadata/three-data-node campaign, including owner-node receipt restart recovery. Receipts are replicated in metadata Raft; reads on every metadata node, availability while the original owner is killed, global request-ID rejection after leader failover, and opaque read/snapshot recovery are qualified. Recovery after owner loss while a committed write waits for embedding-index visibility is qualified without replaying the statement. The principal-bound `/transactions/{id}/status` receipt remains readable after table permission revocation and across owner loss, failover, restart, and table removal; it contains no staged rows or execution plans. |
| Search | Typed antfly.search procedure, lossless private owner RPC, bounded top-K merge, score propagation through traversal, and explicit approximation metadata for owner-local scoring | Production preparation, owner-cut ranking with authorized candidate whitelists, work budgets and approximation metadata implemented; full-text HTTP and semantic/hybrid provider transport on one and two owners qualified. Full-text search is also qualified on real replicated owner nodes. Protected full-text tables are qualified, including score invariance after hidden documents are added. Protected semantic/hybrid search and post-commit indexing of a Cypher-created row are qualified on one and two shards. |
| Graph catalog | Durable create/read/list/drop/rename; conditional definition updates; immutable table/index incarnation bindings; scoped table targets; mapping validation; graph resource permissions; generated SDKs | Six native HTTP revision checks qualify stable graph incarnation, monotonic revisions, stale/concurrent update rejection, prepared SQL invalidation, and authorization of newly mapped tables. Owner readiness checks are available in the catalog UI. These revision checks also pass through the real three-metadata/three-data-node campaign, including receipt availability across failover and restart. |
| Native execution | DB.queryCypher over caller-retained immutable table cuts and authority callbacks; physical adjacency paging; node/relationship projection, revocation checkpoints | Signed owner-side row authority, borrowed cohort fences, immutable owner cuts and private authenticated distributed paging implemented; protected-table and real three-metadata/three-data-node integration qualified; public C/Lite catalog, text-query, write, and receipt bindings implemented and qualified; public Zig and WASM catalog/query/write/status interfaces use the portable single-source host and pass native and WASM smoke qualification |
| JSON graph matching | Relationship-simple trails; parallel identities; node revisits; zero hops; conjunctive uniqueness; independent OPTIONAL scope | All deployment/cross-table paths and compatibility fixtures |
| HTTP text queries | POST /graphs/{name}/query, scoped graph/table authorization, immutable owner cuts, EXPLAIN/PROFILE, strict request decoder, exact integer parameters and typed results | Authenticated three-metadata/three-data-node paging and mutations qualified; broader policy revocation and failure qualification remains |
| SQL composition | Generated native GRAPH_TABLE MATCH/COLUMNS syntax; SQL token lowering into shared graph AST; positional parameters; graph-aware resolver contract | Native resolver and typed graph rows share one cut with ordinary SQL scans and multiple graph catalogs; prepared parameters, joins and rename behavior qualified through HTTP. Pgwire entities, prepared parameters, NULLs, and joins qualified through psql. Read-only and serializable transaction composition is qualified through psql. Staged SQL updates, inserts, deletes, joins, field-derived adjacency replacement and NULL removal, rollback, and policy-hidden postimages are qualified through psql. Staged explicit graph effects with their authenticated source-row intents and commits, and Cypher derived-adjacency read-your-writes are also qualified. Broader transaction fault qualification remains. |
| Workbench | Graph selector/editor, exact typed parameters, Run/EXPLAIN/PROFILE/Cancel, SQL composition templates, table/graph/JSON/plan views, node/edge/path inspector, parallel edges/self loops, zoom/pan, bounded neighbor expansion, principal-scoped persisted write receipts, scoped catalog setup, schema property suggestions, text/semantic/hybrid templates, graph permission editing | Thirty-one workbench/catalog/SQL UI tests, application type checking, and production build qualified. Visual browser qualification remains. |

Go, Python, Rust, TypeScript, and Zig generated clients carry the named graph
catalog and query routes. Go exposes `ExecuteGraphQuery` and `GraphWriteStatus`;
Python exposes `client.graphs.execute`, `client.graphs.update`, and `client.graphs.write_status`, with
lossless parameter/value helpers. SDK tests preserve both signed integer
boundaries, user map keys, and unknown/committed write delivery outcomes.
The full Go and Rust SDK suites and all 304 Python SDK tests pass.

Cypher scalar compatibility includes integer and whitespace-trimmed boolean
conversion, Java-compatible finite float text (including suffixes and hexadecimal
exponents), and string/scalar concatenation with list precedence. Inline MATCH
properties can refer to node, relationship, and path aliases introduced in the
same MATCH; predicates wait for their dependencies while retaining early filtering
where possible. Shortest-path selectors run after the complete MATCH scope's
inline predicates resolve, before MATCH WHERE, preserving other patterns' row
multiplicity and all-shortest ties. UNION deduplication compares bound column names independently of
wildcard binding order. Core and native regression cases cover these boundaries,
including SQL GRAPH_TABLE scalar types and durable mutation postimages.

Native Cypher CREATE defaults, generated-column SET through aliases, and REMOVE
SQL NULL postimages are qualified against durable SQL rows. Cypher entity properties
omit SQL NULL and document JSON null fields; `keys(n)` and `properties(n)` do
not expose them, while nulls inside user maps remain data. This behavior is
qualified through native storage and durable SQL normalization. Normalized rows
retain their private preparation marker through the coordinator, owner RPC,
compiled owner, and native storage boundaries.

Native row-policy qualification covers initial schema version zero, filtered
scans and traversal, allowed writes, durable rejection of policy-denied writes,
and SQL GRAPH_TABLE. Six standalone/HA policy publication and restart tests
pass. Native transaction BEGIN only records participant metadata; PREPARE still
requires authenticated row authority. Four additional postimage checks pass on
one and two shards and on a real three-metadata/three-data-node cluster with `USING` narrower than `WITH CHECK`: allowed writes
persist, while RETURN, aliases, nested containers, and subsequent MATCH do not
expose hidden rows. Mutation targets retain opaque identity without granting
read access to their properties. Snapshot-open errors release index and
shard resources before their resource manager closes.

Public C/Lite qualification covers durable local graph definitions, conditional
updates, table/index incarnation binding, exact signed integer parameters,
relationship persistence across reopen, rollback, duplicate request IDs, and
minimal receipts after table removal. Python integration qualifies embedded
SQL GRAPH_TABLE joins, parameters, staged updates/inserts/deletes, and rollback.
All 229 Python database tests and all 106 TypeScript database tests pass against
the rebuilt library. Go and Rust native graph integration tests pass, including
atomic rollback, duplicate admission diagnostics, and retained receipts. The
Go, Python, and TypeScript bindings treat the new symbols as optional when
loading an older ABI-compatible library; unavailable calls report unsupported.
Rust graph calls require a library exporting the new ABI symbols.

Public Zig and WASM use the same durable catalog, immutable materialization,
postimage normalization, mutation planning, and native transaction ledger.
Qualification covers exact integer parameters, persisted relationships,
duplicate admission, rollback before commit, and minimal receipts. Hosted
receipt readiness is conservative against the owner's current publication
target. Materialization rejects wrong-owner and released capture capabilities;
its owned properties survive subsequent document and edge publications.
Allocation-failure checks verify cleanup and explicit memory-budget failures.
Embedded SQL search drains derived publication before capturing the shared
statement cut, including search immediately following a durable SQL commit.
The `antfly.search` Cypher seed procedure requires the authenticated server's
immutable owner-ranking provider. Embedded graph execution reports
`CypherSearchUnavailable` for that procedure; ordinary embedded search APIs
remain available separately.

Cypher uses the operator's graph memory, anchor, edge, and intermediate-state
ceilings, including statement-overlay capture work. Responses are bounded at
32 MiB of escaped JSON. Execution counts the output before a durable write
decision, so an oversized RETURN rolls back instead of committing an undeliverable
result. Final read delivery rechecks graph/table authority after encoding.

Computed lists and maps have the same 64-level depth ceiling as input values.
Query-owned metadata caches structural hashes, expanded traversal work, and
entity presence by immutable allocation, so shared values created by `reduce`
do not expand while being checked or hashed. Comparisons, output copying, and
mutation property validation reserve their work against the intermediate-state
budget before recursive traversal. Identical shared containers compare directly,
including Cypher's unknown result for nested nulls. Entity projection and alias
refresh skip ordinary data subtrees. Exceeding a ceiling returns an error and
rolls back staged writes before any durable decision.

Grouping, projection DISTINCT, DISTINCT aggregates, and UNION deduplication use
hash buckets with exact, budgeted collision comparisons. Numeric hashes agree
across INTEGER/FLOAT equality without collapsing distinct large integers into
the same rounded floating-point bucket; map hashes ignore insertion order, and grouping
still treats nulls as equal. Hash tables preserve the first encountered value
and existing row order. Cancellation is checked during hashing, bucket lookup,
and deduplication.

The fresh native probe at 2,000/4,000/8,000 unique rows reports
32,003/64,003/128,003 work units for grouping and 20,009/40,009/80,009 for
UNION. At 8,000 rows, the same local Debug library probe dropped from roughly
0.52s to 0.07s for grouping and 1.21s to 0.047s for UNION. Timings are diagnostic;
regressions assert bounded linear work rather than machine-dependent latency.

MATCH anchor scans use charged identity sets across graph labels, including
terminal LIMIT scans, preserving first encounter order without prefix rescans.
Mutation execution keeps index-qualified identity maps for pending/applied
deletions and dirty/created overlay entities. Source/target/producer reference
indexes bound detach and producer cleanup to relevant relationships; versioned
references reject retired projections and deduplicate self loops. Node paging
sorts once per structural change and keeps its cache in the retained owner's
allocator. Postimage planning uses these indexes rather than rescanning mutation
or entity arrays, and shares the runtime's work ceiling and cancellation checks
before the durable decision. SET and REMOVE index entity-to-row aliases once
per clause and refresh only affected rows, including aliases nested in paths
and containers. Result ownership includes commit-time arena growth.
Retained capture decodes each provider page in a temporary budgeted arena,
clones authorized entities and cursors, then reclaims the scan and JSON buffers
before the next page. This keeps temporary allocations out of the retained cut.

Query-owned string caches reuse codepoint/trim analysis, casing, reversal, and
numeric conversions for immutable input allocations. Uncached scans reserve
byte work, long Unicode scans checkpoint, and CONTAINS uses a linear KMP scan
with cached needle preprocessing. Large immutable maps similarly index property
names once; small maps use charged bounded scans. Map projections and property
merges avoid quadratic name searches. Regression tests cover bounded work,
cancellation, rollback before commit, and retained page-allocator lifetimes.

All public SDK graph catalog mutation helpers preserve committed-but-pending
visibility outcomes. Go create/update return `GraphCatalogMutationResult` with
exactly one of `Entry` or `Outcome`; Go drop returns an optional outcome. Python
and TypeScript drop likewise return a pending outcome instead of silently
reporting completed visibility. Pending mutations must be observed rather than
replayed.

The default host build keeps storage final links target-scoped, so native
frameworks cannot enter the freestanding WASM artifact. Portable storage uses
platform-wide atomics, local callback dispatch without a native error registry,
and an explicit rejection of filesystem-managed roots. WASM smoke coverage also
checks computed-depth rollback, shared-value equality, and linear grouping/UNION.

Fresh graph replacement reservations persist their contributor-format version.
Their private generations initialize the ordering marker without a primary-store
migration scan, including resume before the first source-page checkpoint. Older
jobs without that version retain the idempotent contributor migration. Lite and
Directory process-exit recovery keep the same bounded-checkpoint deadlines.

The 2026-10-15 follow-up qualifies 115 core Cypher tests, 942 SQL tests
(with three skips), and 314 embedded database tests (with one skip). The
authenticated two-owner campaign passes 49 expression checks and 66 graph
read/write checks. SDK suites pass in Go, Python (308 tests), and TypeScript
(495 tests, one skip); the focused workbench/SQL/catalog UI suite passes 31 tests.

Run focused compiler, native, and WASM qualification with:

```sh
cd zig
zig build lib-cypher-test cypher-grammar-generated-check
zig build capi-cypher-test embedded-cypher-test cypher-native-capture-test
zig build wasm-test -Dwasm-strip=true
zig build antfly-graph-test -- graph.pattern.
```

The production HTTP campaign creates unique fixtures on a disposable authenticated
server and removes only those fixtures:

```sh
ANTFLY_CYPHER_TEST_AUTH=admin:fixture-password python3 scripts/verify_cypher_http.py --url http://127.0.0.1:18591 --shards 2 --writes
```

It qualifies authenticated scans/traversal, OPTIONAL/zero hops, Unicode, both
signed 64-bit parameter boundaries, metadata-only EXPLAIN and authentication
rejection. The 66-check production write campaign additionally checks node/edge persistence, property removal, exact relationship properties, rollback, MERGE contention, duplicate write IDs, principal-isolated commit receipts, and deletion. The native owner campaign additionally verifies that captured graph
reads retain their document and parallel-edge generations after live updates.

The library suite includes failure injection through compilation, traversal,
aggregation, cloning and serialization. Production-facing guarantees must be
verified against native storage and authenticated execution, beyond fixture
providers. Unsupported features fail explicitly rather than returning truncated
or approximate results as complete.

The pinned Neo4j 5.26 read campaign is in
[`lib/cypher/fixtures/neo4j_read.json`](lib/cypher/fixtures/neo4j_read.json).
Replay it through Antfly with the library tests, and against an empty disposable
Neo4j database with `python3 lib/cypher/verify_neo4j.py --url http://127.0.0.1:17474`.
The campaign covers parallel trails, cycles, optional bindings, nulls, grouping,
list scopes, slices, chained and partial comparisons, map projections and Unicode expressions. The current campaign has 95 cases, including one documented extension. Mixed UNION/UNION ALL is explicitly marked
as an Antfly extension because that baseline rejects the mixed form.

Verification in this worktree used Zig 0.17.0. The storage-independent suite
passes the focused compiler/execution tests, and all 95 reference campaign cases qualify against the pinned
Neo4j image (94 compatible cases and one explicitly documented extension).
The generated grammar check and focused graph catalog tests pass. The native
integration verifies retained document visibility, parallel edge multiplicity,
and matching metadata/native index identities. The TypeScript SDK build and
Antfarm typecheck pass. Browser visual verification could not run because the
browser runtime failed during module import; it remains outstanding.
Optional installed-model inference tests encountered model-shape, timeout, and
host memory-admission failures in a broader binding run. The final embedded
database suites exclude those inference tests and pass in full; graph/search
qualification uses controlled provider fixtures and passes independently.

Final production qualification passes all eight tests in
`e2e/antfly/test_cypher_cluster.py` and `e2e/antfly/test_cypher_search.py`.
These include six-node authenticated queries and writes, 66 HTTP checks,
catalog revision changes, protected postimages, receipt failover/restart,
owner loss during embedding visibility, and protected semantic/hybrid search
on one and two shards. The standalone HTTP campaign also passes all 66 checks;
pgwire transaction composition and postimage-policy qualification pass.
The final rebuild also passes schema defaults/generated-column/null
normalization and protected full-text score-invariance checks. Native C/Lite
export validation reports 121 public functions with no internal exports, and
the final hosted WASM smoke includes pre-commit rollback diagnostics.
The workbench/catalog/SQL UI suite passes 31 tests and the production build.

The focused `antfly-row-policy-follower-test` covers reader-drain deferral,
signed follower writes, policy-phase retries at different Raft indexes, replay,
and rejection of changed publication contents. All six tests in
`antfly-standalone-policy-ha-test` and all three transport/codec tests in
`antfly-cypher-transport-test` pass. The library suite covers atomic UNION writes,
including rollback after a later branch fails and write admission when the
first branch is read-only.

Catalog-created graphs capture table IDs and immutable storage names in the
same metadata transaction as graph creation. Logical table renames preserve the
binding; dropping and recreating a table does not retarget the graph. Native
execution rejects a retained cut whose physical table ID differs from the
binding. Relationship mappings also retain the source index incarnation and
configuration fingerprint; metadata readers resolve these in the same transaction,
and native execution rejects mismatched indexes. Unbound legacy graph records require explicit recreation. Mapping
strings are literal default.public names; other scopes use
`{"database":"research","namespace":"papers","table":"docs"}`.

Use `elementId(node_or_relationship)` for an opaque qualified entity ID and
`antfly.documentKey(node)` for Antfly's native string document key. Neo4j's
deprecated [`id()` returns an integer](https://neo4j.com/docs/cypher-manual/5/functions/scalar/#functions-id);
this implementation rejects it with `CypherLegacyIdUnavailable` until a
qualified integer identity provider exists. `length()` accepts paths; `size()`
accepts lists and Unicode strings. These functions keep their Cypher types.
`toLower()` and `toUpper()` use pinned Unicode 16.0.0 default full case mappings,
including expanding mappings and contextual Greek final sigma. They are locale
independent across native and portable execution; server locale does not select
Turkish or Lithuanian tailoring. `trim()`, `ltrim()`, and `rtrim()` follow Neo4j's
Java `Character.isWhitespace` boundaries, preserving nonbreaking spaces.
The tables are reproduced with `lib/cypher/tools/generate_unicode_case.py` from
hash-checked official Unicode inputs; their Unicode license is embedded in the
generated source.

Scalar property access rejects values other than nodes, relationships, maps,
and null, so invalid reads abort a write statement. `toInteger(string)` parses
and bounds-checks decimals exactly before truncating toward zero; valid decimal
values outside signed 64-bit range raise `CypherNumericOverflow`. `avg()` uses a
running floating-point mean to avoid intermediate integer or float sum overflow.
`toString(float)` retains fractional digits and Java 21/Cypher notation thresholds,
including negative zero and subnormal values.

Property values are validated before overlay changes or normalization: graph
handles, even inside maps/lists, raise `CypherInvalidPropertyValue` and roll back.
Self-loop endpoint predicates follow the selected traversal orientation. CREATE
and MERGE reject already-bound relationship or path declarations before storage
admission, including EXPLAIN and empty inputs. Final wildcard column schemas are
bound independently of row production, and cells follow that bound column order.
SQL GRAPH_TABLE preserves the scalar types of string/boolean/numeric functions;
relationship properties remain JSONB until explicitly converted because their
Cypher values have no declared SQL property schema.

## Product direction

Build **Antfly Graph**, a Cypher-compatible graph language with Antfly search
extensions, backed by the same planning and execution services as SQL and JSON.
Users should write SQL when thinking in rows, graph patterns when thinking in
relationships, and combine them when needed.

The PostgreSQL superset goal remains the SQL compatibility contract. It does not
require every graph query to adopt SQL syntax. Provide a standalone graph
language and a native SQL boundary for graph patterns. Preserve PostgreSQL
behavior for existing SQL, including identifiers, operators, parameters, and
keyword categories.

Initially describe the language as **Cypher-compatible with Antfly extensions**.
Claim a Cypher superset only after qualifying an explicitly selected version and
feature baseline. Neo4j Cypher, openCypher, ISO GQL, and SQL/PGQ are related but
different compatibility targets; do not present them as interchangeable.

The initial product promise is:

> Query existing graph indexes with familiar Cypher patterns, compose graph
> results with PostgreSQL SQL, and explore the results visually.

## Existing foundations

The repository already provides much of the graph execution foundation:

- [Graph queries](pkg/antfly-embedded/src/graph/query.zig) expose traversal,
  shortest paths, k-shortest paths, named query dependencies, and pattern matching.
- [Pattern matching](pkg/antfly-embedded/src/graph/pattern.zig) contains conjunctive
  patterns, optional bindings, table-qualified node identities, filters, and work
  budgets.
- [Public graph schemas](../specs/openapi/antfly/indexes.yaml) define canonical
  `graph_queries` operations and their exactness and deployment limitations.
- [SQL grammar](lib/sql/grammar/antfly_sql.y) recognizes graph DDL, while its
  basic `MATCH ... RETURN` productions currently belong to
  `unsupported_statement`. Recognition alone does not establish execution support.
- [Grammar architecture](lib/sql/grammar/GRAMMAR.md) already separates generated
  parsing, catalog-free ASTs, binding, typed plans, and execution services.
- [Public graph admission](pkg/antfly/src/api/public_graph_query.zig) preserves
  canonical request validation and identity rules.

These are reusable foundations, not proof that arbitrary Cypher can execute.
The current structured API has narrower projection and aggregation contracts,
path semantics, and runtime-specific cross-table limitations. Extend the shared
engine where necessary rather than forcing the new language into an unchanged
JSON request shape.

## Inspiration and compatibility targets

| Reference | Adopt | Keep explicit |
| --- | --- | --- |
| Neo4j Cypher | Recognizable patterns, clause pipelines, optional matching, typed graph values, useful visual exploration | Version baseline, path semantics, expression behavior, unsupported features |
| SurrealDB | Multiple query surfaces over the same documents and relationships; easy transitions between search and graph | Antfly's catalog mapping, ownership, PostgreSQL syntax, and transaction guarantees |
| SQL/PGQ | Graph pattern matching as a relation that SQL can compose | Supported subset and exact syntax; no implied full standards compliance |
| PostgreSQL | Relational composition, prepared parameters, SQL type and null contracts | Graph additions must preserve existing SQL behavior |

SurrealDB now documents a GQL surface over its existing records and relations.
Its direct table-to-label mapping is convenient, but Antfly should support an
explicit mapping layer. SurrealQL arrow navigation is useful inspiration for
exploration; adding a second traversal syntax at launch would increase the
learning and compatibility burden.

Neo4j's current `SEARCH` clause also supports search-index integration. Compare
against that syntax before introducing Antfly shorthand with the same name.

## Language surfaces

The server exposes authenticated graph text queries and native SQL composition.
The matrix above records the supported Cypher subset and execution limits.
Standalone clauses also execute through the library or `DB.queryCypher`.

### Standalone graph queries

Select a graph in the request or workbench and submit familiar patterns:

```cypher
MATCH (a:Document)-[r:CITES]->(b:Document)
WHERE a.id = $document_id
RETURN b.id, b.title, r.weight
ORDER BY r.weight DESC
LIMIT 20
```

The graph selection is outside ordinary query text, so a selected graph does not
require a proprietary clause in every query. A future graph-selection statement
may support scripts, with its dialect and session behavior specified separately.

Support named parameters for the graph surface. Keep SQL positional parameters
such as `$1` on the SQL surface. Both bind to typed values and never use string
substitution.

### SQL graph composition

Introduce a `GRAPH_TABLE` relation following the SQL/PGQ direction:

```sql
SELECT g.target_id, g.title, d.published_at
FROM GRAPH_TABLE (
  knowledge
  MATCH (a IS Document)-[r IS CITES]->(b IS Document)
  WHERE a.id = $1
  COLUMNS (
    b.id AS target_id,
    b.title AS title
  )
) AS g
JOIN documents AS d ON d.id = g.target_id
ORDER BY d.published_at DESC;
```

`COLUMNS` defines a typed relational output. SQL can then apply joins, CTEs,
aggregations, windows, sorting, and limits through its normal planner. This
MATCH/COLUMNS grammar is implemented; it is a supported SQL/PGQ subset, not a
claim of complete SQL/PGQ standards conformance.

Use one shared pattern AST and logical representation underneath standalone
queries and `GRAPH_TABLE`. Do not implement composition as an opaque function
that evaluates a Cypher string. Native syntax allows binding, diagnostics,
correlation, cost estimates, and safe optimization across the boundary.

### JSON and transport

Preserve canonical JSON `graph_queries` for programmatic structured requests.
All frontends should lower into shared plans, with explicit semantic modes where
their established contracts differ.

Submit graph text to `POST /db/v1/graphs/knowledge/query`:

```json
{
  "query": "MATCH (a:Document)-[:CITES]->(b:Document) WHERE a.id = $id RETURN b",
  "parameters": {"id": "doc:root"}
}
```

The response exposes columns, tagged rows, graph incarnation and revision,
completion, execution statistics, and optional plan/profile data. Integer values
use decimal strings. SDK graph operations preserve the tagged values without
coercing entity identities or integers through floating-point numbers.

PostgreSQL wire clients should use SQL `GRAPH_TABLE` by default. If a standalone
graph dialect is later supported over pgwire, select it explicitly and define
its connection, prepared statement, and result-type behavior. Do not guess the
language from ambiguous text or silently switch a SQL connection's dialect.

### Durable graph writes

Use the same query endpoint for writes. Supply a unique `write_request_id` when
submitting CREATE, MERGE, SET, REMOVE, DELETE, or DETACH DELETE. The server reserves
that ID durably for its authenticated principal before execution. Reusing it
returns a conflict, including after a metadata-leader failover.

The `commit` result contains the transaction ID and outcome. If delivery fails or
the outcome is unknown, read `GET /db/v1/transactions/{id}/status`; do not replay
the query. This endpoint is principal-bound and exposes only receipt metadata.
It remains readable after table permission revocation or removal. A committed
write can still be waiting for index visibility; report that state separately
from aborted and unknown outcomes. The workbench persists these receipts in the
current principal's workspace and offers status reconciliation.

### Distributed deployment

Metadata and data processes must share the operator-provisioned internal RPC
credential and the separate principal-signing authority. Configure
`ANTFLY_INTERNAL_SERVICE_SECRET`/`ANTFLY_INTERNAL_SERVICE_ISSUER` for internal
transport and `ANTFLY_TRUSTED_PRINCIPAL_SECRET`/
`ANTFLY_TRUSTED_PRINCIPAL_ISSUER` for signed row-policy grants. Row-policy
publication also requires `ANTFLY_SETTING_AUTHORITY_SECRET`/
`ANTFLY_SETTING_AUTHORITY_ISSUER` for setting/catalog authority. Test
credentials in qualification scripts are isolated fixture credentials.
Mixed artifact-source producer campaigns require Raft owners. Standalone
ordered artifact publication remains unavailable on the merged main baseline;
its guard rejects publication proposals instead of fabricating Raft receipts.

Graph admission captures immutable graph/table/index identities and a coherent
owner cut. Execution checks current graph/table permissions and policy grants at
checkpoints and before delivery. A policy-generation change yields a conflict;
a temporarily unavailable owner or leader yields a service-unavailable error.
No path silently downgrades to an unauthenticated read. Policy postimage checks
use SELECT visibility independently of write `WITH CHECK` permissions. Follower
apply finalizes committed serving intents before verifying signed writes. Exact
phase retries at distinct Raft indexes advance their receipts without reopening
a finalized barrier; changed bundles or owner descriptors are rejected.

## Logical graph catalog

Introduce a named graph catalog object that maps logical labels and relationship
types to existing documents, sources, and graph indexes.

| Graph concept | Antfly representation |
| --- | --- |
| Node | Document with a table-qualified identity |
| Label | Declared table or filtered view over a table |
| Node property | Declared stored document field |
| Relationship type | Declared edge type |
| Relationship identity | Stable identity preserving distinct parallel edges |
| Relationship property | Declared edge metadata or property |
| Graph index | Physical access path supporting a logical graph |

For example, `knowledge` could map `Document` to `documents`, `Person` to a
filtered `entities` relation, and `CITES` and `MENTIONS` to existing graph
indexes. Labels must remain distinct from physical table names: a table may hold
multiple entity kinds, and a mapped node may have multiple labels.

A graph definition declares label predicates, relationship endpoint mappings,
and usable index recipes. Property constraints come from the mapped table's
schema. The graph is a logical view over existing storage.
Resolve `[:CITES]` through the selected graph definition rather than guessing
which of several graph indexes on a table the user intended.

Use catalog-owned identities and incarnation fences for graph definitions.
Graph lookup authorizes the definition and each underlying resource. Changes to
label mappings, policies, or index recipes must invalidate incompatible prepared
plans and cursors. DDL should store normalized typed definitions, not raw query
text, following the existing grammar architecture.

Graph catalog entries expose their definition `revision` as a decimal string.
Replace mappings with `PUT /graphs/{name}` and
`{"expected_revision":"1","definition":{"labels":[{"name":"Document","table":"docs"}]}}`.
The metadata transaction compares the expected revision, captures the new table
and index incarnations, and advances the revision while preserving `graph_id`.
A concurrent edit returns `409 CatalogGenerationChanged`; reload explicitly
before saving again. The workbench retains the revision at which editing began,
so a catalog refresh cannot silently overwrite another editor's changes.
Prepared SQL queries require preparation again after a definition changes.
Already admitted reads keep their immutable definition and owner cuts; current
graph and table authority is still rechecked throughout execution.

The catalog's **Check readiness** action opens the authenticated read path with
`MATCH (n) RETURN n LIMIT 0`. This validates retained owner cuts and index
incarnations without returning documents. The status applies to that check;
every query validates readiness again, and changing definitions clears it.

## Semantic contract

`=~` uses full-string matching, ASCII Java case-insensitive flags, Unicode
characters, Java line terminators, and leading `(?i)`, `(?m)`, `(?s)`, and `(?d)`
flags. Ordinary captures, single-digit backreferences, lookarounds without
captures, ASCII shorthand classes, quoting, and absolute anchors are supported.
Patterns are limited to 16 KiB, 4096 capture-engine states, bounded repetitions
through 255, and the statement's shared work/memory budgets. Parsed programs are
cached only within the statement; match scratch space is reused. Named captures,
Unicode case/class flags, scoped flags, class intersections, possessive/atomic
constructs, and captures in lookarounds currently fail explicitly. Invalid
patterns have the distinct `CypherInvalidRegex` diagnostic. These supported
predicates are qualified against the pinned reference and through eight native
HTTP/SQL expression checks, including invalid patterns, work limits, and
composition with PostgreSQL regex predicates.

The matching contract follows [Neo4j string predicates](https://neo4j.com/docs/cypher-manual/25/expressions/predicates/string-operators/)
and the supported portion of [Java Pattern](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/regex/Pattern.html).

`WITH … WHERE` can filter on both input variables and projection aliases when
the projection does not aggregate or use DISTINCT. Only projected variables
remain in scope for the following clause. Alias shadowing is resolved before
filter evaluation. These cases are qualified against the pinned Neo4j 5.26
reference; DISTINCT and aggregate projections reject unprojected variables.

Pattern comprehensions use the same retained owner cuts, row authority,
relationship uniqueness, cancellation and work budgets as MATCH. Their local
node, relationship and path bindings do not escape the expression. Parallel
relationships preserve multiplicity; a null anchor or no match yields an empty
list. Predicates and projections cannot contain aggregate functions.

### Paths and matching

On the inspected `origin/main` baseline, a significant compatibility gap was
variable-length path uniqueness: JSON `GraphMatchEdge` used node-simple paths,
with an exception for explicit cycle closing. Cypher's default match behavior
prevents relationship reuse within its match scope while permitting node
revisits. Cyclic graphs can therefore produce different answers.

Use Cypher semantics as the canonical contract for both graph text and JSON
MATCH. Relationship uniqueness applies within one MATCH group, including all
comma-separated patterns; each OPTIONAL MATCH starts a new relationship scope.
Permit node revisits, retain parallel relationship multiplicity, and support
zero-hop paths. The former JSON node-simple behavior is an intentional contract
change, authorized as part of this design. Explicit exploration modes may add
node-simple or repeatable traversal without changing the MATCH default.

Specify and qualify:

- Relationship reuse scope across patterns and clauses.
- Node-simple, relationship-simple, and any supported repeatable path modes.
- Zero-hop paths, self-loops, cycles, and repeated bindings.
- Variable-length relationship bindings and ordered path values.
- Shortest-path ties, all-shortest results, and weighted path restrictions.
- Undirected matching and parallel-edge multiplicity.

Do not silently cap an unbounded path at the current 64-hop execution limit.
Reject unsupported bounds or require an explicitly bounded exploration
operation. A limit on returned rows is not a proof that traversal work is bounded.

### Rows, values, and clause scope

Return one row per matching binding or path according to the selected language
semantics. Deduplication requires `DISTINCT`; a graph visualization may merge
identical displayed nodes without changing row multiplicity.

Support node, relationship, and path values with stable public identities.
Preserve ordered path elements and distinct parallel relationships. SQL
composition projects schema-bound scalar columns and JSONB graph values. Nodes,
relationships, and paths retain their tagged entity envelopes at that boundary;
ordinary maps remain ordinary JSON objects, including keys named `type` or
`value`. This encoding is covered by native HTTP and pgwire qualification.

Define missing-field behavior separately from SQL NULL and JSON null, then
document the conversion at each frontend boundary. Preserve optional-match null
bindings and the scope of predicates attached to an optional match.

Implement `WITH` as a scoped relational stage, not textual rewriting. Specify
aggregation grouping, list values, property lookup, ordering, and correlated
subquery scope. Share scalar infrastructure only where semantics agree; SQL and
Cypher require separate identifier, parameter, coercion, and expression rules.

### Identity and authorization

Equal document keys in different tables identify different nodes. Use
table-qualified identity internally and preserve source provenance in public
values. Do not expose private storage identities.

Enforce row and field policies during expansion, including intermediate nodes,
not only when hydrating the final results. Hidden nodes or edges must not become
observable through paths, counts, errors, or graph exploration. Bind and authorize
all graph mappings through the existing catalog and policy machinery.

### Exactness, budgets, and visibility

Exact pattern queries return exact results or fail with a specific diagnostic.
Never label a partial aggregate exact. Bounded exploration can return partial
results only through an explicit contract with a visible completion reason.

Retain request deadlines, cancellation, admission limits, and work accounting for
anchors, nodes, edges, edge bytes, intermediate states, and distinct identities.
The editor should explain whether a limit controls output or execution work.

Bind graph and document reads to compatible retained visibility cuts. Report
index readiness and stale coverage explicitly. Independent source cuts must not
be advertised as one atomic cross-table snapshot unless a coordinated mechanism
actually provides that guarantee. Preserve runtime-specific capabilities,
including serverless cross-table restrictions, through binding diagnostics.

## Search followed by graph reasoning

Antfly's distinctive extension should be a clear dataflow:

```text
Search documents → retain scored seed identities → expand graph
                 → filter / aggregate / rerank evidence
```

Start with a table-producing search operator whose outputs can bind graph
variables. For example:

```cypher
CALL antfly.search(
  'documents',
  {query: $question, mode: 'hybrid', limit: 20}
)
YIELD node AS seed, score

MATCH (seed)-[:MENTIONS]->(person:Person)
RETURN person.name, max(score) AS relevance
ORDER BY relevance DESC
LIMIT 10
```

This query deliberately starts from the top-20 search relation. It is different
from matching the complete graph and ranking the result afterward. The existing
canonical JSON `MATCH` contract says top-level retrieval results and filters do
not scope its anchor universe; preserve that behavior and express search-seeded
matching through an explicit dependency.

The seed table must be included in the selected graph mapping, and the search
operator must retain graph-bindable table-qualified identities. Carry retrieval
scores with their seeds; do not automatically assign them to reached nodes.
Aggregation, score propagation, fusion, and reranking require explicit operators.

Explain should expose candidate counts, scoring method, approximation, fan-out,
and any prefilter or postfilter behavior. A graph query over approximate seeds
can match those seeds exactly without becoming an exact global top-K query.
Never move top-K selection across graph expansion unless equivalence is proven.

Consider concise `SEARCH` syntax after this contract settles, comparing it with
current Neo4j behavior. Additional Antfly extensions can expose published graph
metrics, evidence provenance, and freshness requirements through the same typed
planning boundary.

## Workbench UX

Extend the current workbench into one query environment with multiple result
views. Language selection and result presentation are independent.

```text
Catalog                       Query editor
  Tables                      Language: SQL | Graph | JSON
  Graphs                      Graph: knowledge
    knowledge                 Parameters
      Document                Run · Explain
      Person
      CITES                   Results: Table | Graph | JSON | Plan
      MENTIONS
```

### Authoring

- Autocomplete labels, relationship types, properties, and valid endpoint
  connections from the selected graph definition.
- Provide inline syntax, binding, compatibility, and readiness diagnostics with
  source locations.
- Keep parameter input separate from query text and reuse typed SDK binding.
- Offer query starters for neighbors, shared connections, shortest paths,
  missing relationships, and search followed by traversal.
- Add **Explore this document**: select a document and relationship, then generate
  an editable query and visualization.

An initial graph setup flow should list available graph indexes, let users map
labels and endpoint types, preview a small bounded neighborhood, and save a
named graph definition. Display mapping errors and readiness before execution.

### Results and exploration

Default scalar projections to Table. Queries returning nodes, relationships, or
paths enable Graph view. SQL returning supported graph values should also enable
Graph view. Returning scalar IDs alone should not implicitly fetch unrelated
graph data.

Selecting a row highlights its path. Selecting a node shows its document,
source table, labels, and provenance. Selecting an edge shows relationship
identity, type, properties, ownership, and direction. Preserve graph values in
JSON export so users can inspect the actual result independently of layout.

**Explore neighbors** runs a separate bounded query. Visually distinguish those
nodes and edges from the original query result. A local visualization filter must
not appear to modify query semantics. Display separate counts for returned rows,
displayed nodes, and visualization caps; large results may need pagination or a
bounded rendering sample without truncating the logical result silently.

### Explain and status

Explain presents understandable stages: anchor lookup, edge expansion, document
filter, optional expansion, aggregation, and sort. Include estimated and actual
work, chosen indexes, fan-out, visibility cuts, coverage, and budget failures.

Show completion, freshness, and readiness near the result controls. An exact
query failure and an incomplete exploration result must be distinguishable.
Provide actionable diagnostics such as a missing anchor index, unsupported path
mode, graph mapping conflict, or unsupported cross-table deployment capability.

## Compiler and execution architecture

```text
Generated SQL grammar ──────┐
Generated graph grammar ────┼→ catalog binding → typed logical plan
Canonical JSON graph API ───┘                      ↓
                                      shared planning and execution
```

Use a separate generated graph grammar, with shared graph pattern AST types for
the SQL `GRAPH_TABLE` branch. Follow the existing Antfly-owned generator model;
do not introduce post-parse string scans or a durable query-text interpreter.

Parsing remains catalog-free and retains source spans. Binding resolves labels,
tables, graph mappings, property types, policies, and runtime capabilities.
Generated statement dispatch identifies graph statements explicitly. Preserve
PostgreSQL lexical compatibility and keyword categories in the SQL grammar.

Add or extend typed logical operators for anchor relations, edge expansion,
optional expansion, path selection, projection, aggregation, subqueries, and
search-produced anchors. Reuse existing graph primitives when their semantics
match. Support a broader common IR than today's public JSON wire schema.

Planner optimization must preserve path multiplicity, optional-match behavior,
authorization, visibility, and top-K placement. Qualify anchor selection and
reverse traversal against table-qualified routing restrictions. Explain should
make rejected optimizations and unavailable access paths inspectable.

Prepared plans and cursors bind language version, semantic mode, graph/catalog
incarnations, policies, index recipes, parameters, ordering, and retained cuts.
Apply deterministic ordering where a pagination contract requires it; do not
invent an order for unordered Cypher results.

## Mutations and ownership

Ship read-only graph queries first. Add `CREATE`, `MERGE`, updates, and deletion
after defining relationship ownership and transactional behavior.

Distinguish explicit relationships from edges derived through document fields,
enrichment artifacts, or other managed sources. A graph mutation must not edit a
derived edge only to have the next index rebuild recreate it. Define whether
such edits update the source document, create an explicit contribution, or fail.

Cypher exposes legacy artifact assertions with producer-qualified relationship
identities. Different documents asserting the same source/type/target are parallel
managed relationships; multiple artifact contenders from one producer retain that
producer's winning live value. Ordinary graph APIs keep their aggregate adjacency
projection. A legacy direct contender sharing an artifact projection also remains
managed, because tuple-level writes cannot isolate its private contributor state.
Explicit stable-ID relationships can coexist in the same index and remain writable.
Producer-owned facts, artifacts, and field-derived relationships reject direct `SET`, `REMOVE`,
and `DELETE`. Relationship wire values carry their producer identity independently
of the opaque relationship ID. Ownership distinguishes explicit edges, manually
owned document facts, deterministic field projections (`field`), and published
artifact assertions. Updating a producer rebuilds its field projections; unrelated
updates preserve manually owned facts and the last published artifact assertions.
Field projections have a distinct identity from artifact assertions, including
when both share a producer and traversal endpoints.
Postimage read authority applies to producers independently of graph label
membership. Referenced producers are captured under the same snapshot fences,
including nonvertices, with budgeted, deduplicated lookups and indexed identity
resolution. Losing read authority hides their assertions in the statement view.
Online graph index activation installs its own resolver dependencies under stored
destination authorization, preserves sibling definitions, and schedules corpus
backfill outside the DDL barrier.
Contribution selection resumes within an owner under physical work and output
budgets, so fan-in does not require materializing every producer in one page.
Artifact reconciliation chooses the surviving contributor before emitting either
a physical edge write or deletion, keeping ordered effects and their guards unique.
Ordered resolver replay records authored review overrides (including absence)
with exact value/revision guards; these inputs do not require generated receipts.
Deleting a producer withdraws its nonincident assertions immediately
in Cypher and staged SQL `GRAPH_TABLE` views; other producers' assertions survive.
Endpoint deletion still follows the normal `DELETE`/`DETACH DELETE` rules.
Replicated owners order the installed producer catalog and its activation through
reserved control capacity before accepting asynchronous output. Reopens and
reconciliation retry the catalog/activation receipt gap; hidden restore and child
roots stay quiescent. Receiver-local completion and publication events wake the
coalesced status publisher without adding replay debt. Startup reconciliation
retains owners until producer completion and resolver replay settle. Resolver
backfill hints remain schedulable after activation, while the hint-only boundary
rejects bundled producer effects. Accepted publications wake enrichment, resolver,
and promotion consumers at the durable replay boundary; background candidate
reads carry bounded deadlines so owner transitions can make progress. Committed
graph writes prepare missing physical graph indexes on the independent owner
control worker, preserving newer durable schemas and sibling indexes during
historical replay. Document writes and prepared transaction intents prepare the
same pinned graph write plan before deriving field edges. This requires installed
physical namespaces, while query readiness continues to wait for backfill;
ordered apply cannot wait for publications queued behind its own entry.
Read-only status loads the existing physical root identity
and derives direct source names from catalog configuration, retaining completion
proofs after a live owner retires. Resident status requires a current initialized
catalog as well as physical-root, table identity, range, and storage fences;
historical apply owners fall back to a fresh read-only status snapshot. Cached index detail and lifecycle status
refresh incomplete producer observations until native completion is visible,
then resume the cache fast path. Metadata registration, heartbeats, and cached
capability summaries require framed runtime-status transport for producer
completion or pending graph counts, including enclosing store and per-group
codecs; older profiles cannot silently drop these facts. Resolver candidate batches
retain their pinned physical table bindings and bulk reads across the private
compiled callback boundary. Resolution artifacts keep public table names for
curation alongside immutable `storage_table` identities; graph endpoint routing
and mention edges prefer those identities, including after public-name rebinding.
Bulk callback scratch space is bounded while complete candidate sets remain
supported. Ordered transaction resolution publishes durable effects without
waiting for producer output through the same Raft progress driver; the proposing
request enforces its visibility barrier after apply confirmation. Captured embedded
opens defer derived replay to the independent maintenance owner, so large pending
full-text work cannot strand healthy SQL reads or mutations. Background turns
reserve replacement progress before replay, sample one finite cutoff, and retain
remaining debt for later turns under steady writes; explicit idle calls still
prove complete coverage. Native index publication validates captured schema and
producer catalog bytes and the embedded table registry under the writer lease
without reopening the foreground runtime for each mutation. Each bounded graph
replacement page shares one publication lease, then yields to queued foreground
writers. Repair and replacement shadow managers share the
source schema registry so encoded relational rows use the same layouts. Mixed graph repair
projects field contribution provenance from current authoritative rows under the
writer fence directly into the candidate. Repair never mutates Raft-owned source
artifacts outside ordered publication. Contribution reconciliation recovers
missing field assertions from authoritative rows with a target cache per apply
window, preserving linear work for high fan-out fields; replay catches concurrent
mutations before cutover. Resolver catalog hashing accepts the same versioned envelope
as the resolver reader. SQL `GRAPH_TABLE` consumes normalized staged postimages,
including encoded rows, without applying defaults or generated expressions again.

Specify stable relationship IDs, parallel edges, uniqueness, concurrent `MERGE`,
endpoint deletion, detach semantics, policy enforcement, and atomicity. Reuse
existing write and transaction services; do not imply stronger cross-table
transactions or immediate derived-index readiness than the storage layer offers.

## Delivery sequence and qualification

1. **Graph catalog and semantic contract.** Select a versioned Cypher baseline;
   define mappings, identities, paths, policies, visibility, and capabilities.
2. **Read-only graph language.** Implement fixed-hop `MATCH`, filters,
   projections, optional matches, ordering, and limits. Add bounded paths only
   after their selected uniqueness semantics are implemented.
3. **SQL composition and workbench.** Add `GRAPH_TABLE`, typed parameters,
   Table/Graph views, graph setup, and Explain.
4. **Search and richer clause pipelines.** Add explicit search seed relations,
   `WITH`, aggregation, subqueries, and additional path selectors.
5. **Mutations.** Add writes after ownership and transaction semantics are
   specified and qualified.

Publish a compatibility matrix separating accepted syntax, executable behavior,
runtime restrictions, and Antfly extensions. Unsupported syntax needs stable,
specific diagnostics, not silent fallback.

Qualification should cover parser generation and PostgreSQL regression fixtures;
Cypher conformance cases for the selected baseline; differential result checks
for supported pure graph queries; cross-frontend equivalence where semantic modes
agree; and adversarial cases involving cycles, parallel edges, optional nulls,
missing properties, multiplicity, authorization, cross-table identity, and budgets.

Integration qualification should include search seed scope, score preservation,
snapshot/index readiness, cancellation, prepared-plan invalidation, cursor
retention, catalog changes, and deployment capability differences. UX checks
should verify that visual expansion, rendering caps, and local filters never
misrepresent the original query result.

## Implemented contract

- The differential baseline is pinned to Neo4j 5.26. Relationship uniqueness
  applies within each MATCH; OPTIONAL MATCH has an independent scope. The
  compatibility matrix identifies language features outside this baseline.
- Native SQL `GRAPH_TABLE` uses the generated MATCH/COLUMNS grammar and the
  same graph AST, retained cuts, permissions, and transaction overlays as the
  graph frontend. PostgreSQL wire results encode graph entities as JSONB.
- Durable named graphs bind label mappings to table incarnations and
  relationship mappings to source index incarnations. Conditional replacement
  uses a decimal-string revision; prepared SQL invalidates on mapping changes.
- Public values use tagged, collision-free envelopes and decimal-string signed
  integers. Missing entity properties read as Cypher null; nested user nulls
  remain data. SQL NULL does not become a stored Cypher entity property.
- `antfly.search` has explicit modes, indexes, limits, authorized candidate
  sets, score propagation, and approximation metadata. Workbench results expose
  approximate ranking separately from exact graph traversal.
- Authenticated HTTP and generated SDK routes expose graph queries and durable
  write status. SQL HTTP and pgwire support prepared graph composition. Public
  C/Lite entry points use the same compiler, immutable materialization, postimage
  planner, native intents, and durable status semantics; portable Zig/WASM entry
  points are provided by the single-source graph host.
- Production writes use statement overlays and native coordinated transactions.
  All UNION branches participate in one transaction, including a read first
  branch followed by a write. Failures roll back every branch. Write admission
  checks the entire query before resolving permissions or opening owner cuts.
  Retained overlays use the configured intermediate-state ceiling without a
  separate 65,536-entity write limit. Retained owner reads are released once
  postimages and conflict proofs are owned, before distributed prepare/commit.
  Owner range proofs validate the complete cut at prepare; production writes do not materialize a predicate per row.
  CREATE preserves explicit nulls through SQL default evaluation and validates
  requested labels after defaults and generated columns have been normalized.
  CREATE/MERGE reject labels or properties on an already-bound node before
  opening storage; plain bound endpoints remain valid. Statement-owned local
  cuts are reclaimed after their final reader closes, including setup and
  execution failures, so completed queries do not consume retained-cut capacity.
  Published continuation cuts retain their existing expiry lifecycle.
  Deleted entities retain identity for counts and identity functions, but
  property access, properties(), keys(), and labels() raise CypherDeletedEntity.
  Entity results after deletion contain no cached properties. Deletion is
  shared across subqueries and DETACH DELETE aliases. Retained aliases resolve
  active document-derived replacements; a removed contribution has no readable
  cached properties. Observing a missing derived contribution does not record
  an explicit deletion; restoring its source field restores readable aliases
  with the same identity. CREATE/MERGE reject deleted endpoints, and overlays and
  commit plans also validate live endpoints.
  MERGE rejects null node and relationship keys during pattern evaluation and
  rolls back earlier writes in the same statement. Keys referencing nodes in
  the same pattern are checked once those nodes are bound. Relationship aliases resolve
  current overlay postimages through subqueries, paths, lists, and maps; CREATE
  omits null entity properties while preserving nested null data. SQL GRAPH_TABLE
  shares the operator memory, anchor, edge, and intermediate-state ceilings with
  direct Cypher, including transaction overlays and pgwire; no separate 65,536
  entity clamp applies to staged SQL graph reads. Graph budget rejections use
  SQLSTATE 54000 through SQL HTTP and pgwire. SQL retains its separate 256 MiB
  working-memory ceiling even when the graph ceiling is larger.
  `scripts/verify_cypher_mutations.py` qualifies relationship postimages, nested
  null data, MERGE rollback, and DELETE counts on authenticated owner cuts.
  `e2e/antfly/test_cypher_limits.py` exercises all four operator ceilings through
  direct Cypher, SQL HTTP, and pgwire, including staged reads and rollback.
  `scripts/verify_cypher_normalization.py` covers durable SQL parity, CREATE and
  MERGE predicate labels, and deletion of existing versus transient nodes.
  `scripts/verify_cypher_write_limits.py` qualifies production writes above
  65,536 rows and rejection at the requested state ceiling.
- Replicated policy installation remains fail-closed while old readers are
  active. The next signed committed mutation can finalize serving on a follower
  without a coordinator receipt probe; blocked apply defers until readers drain.
- Unknown delivery outcomes require receipt reconciliation and never statement
  replay. Workbench receipts are persisted per principal, and minimal status
  reads continue to work after table permission removal.

## References

- [Neo4j 5 value ordering and comparison](https://neo4j.com/docs/cypher-manual/5/values-and-types/ordering-equality-comparison/).
- [Neo4j patterns and match modes](https://neo4j.com/docs/cypher-manual/current/patterns/).
- [Neo4j SEARCH clause](https://neo4j.com/docs/cypher-manual/current/clauses/search/).
- [SurrealDB GQL and shared data-model mapping](https://surrealdb.com/docs/learn/querying/gql/overview).
- [SurrealDB graph traversal](https://surrealdb.com/docs/learn/data-models/graph/graph-traversal).
- [SurrealDB RELATE statement](https://surrealdb.com/docs/reference/query-language/statements/relate).
- [PGQL specification and GRAPH_TABLE](https://pgql-lang.org/spec/2.0/).
- [PostgreSQL keyword categories](https://www.postgresql.org/docs/current/sql-keywords-appendix.html).
- [Antfly source composition and visibility](../docs/plans/composed-query-sources.md).
- [Antfly query pipeline proposal](../docs/plans/pipelined-query-api.md).

External documentation evolves; pin the chosen language baseline and conformance
fixtures during implementation rather than treating current documentation URLs
as a permanent version contract.
