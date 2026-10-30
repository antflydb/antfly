# Antfly Graph: Cypher compatibility, SQL composition, and exploration UX

Status: production server and embedded implementation, 2026-10-15. Authenticated distributed
queries, native SQL GRAPH_TABLE, coordinated writes/search, and the query
workbench are implemented. The compatibility matrix records supported behavior,
qualification, and explicit limits against the pinned Cypher baseline.
Repository observations were checked against `origin/main` at
`d0614ed7e5`.

## Implementation status

The worktree contains an Antfly-owned generated Cypher grammar, a catalog-free
AST and compiler, scope binding, typed graph values, a budgeted execution engine with cached, cancellable full-string regex predicates,
and a native immutable-cut adapter. Graph definitions are persisted through the
system catalog and exposed through generated HTTP catalog routes and SDK types.
The existing JSON pattern engine now follows Cypher relationship uniqueness in
each MATCH scope and accepts explicit zero-hop bounds.

Execution uses bounded native pages rather than capturing the graph. Equality,
range, and bounded sparse IN constraints on mapped relational properties restrict the
native READY-index planner; authorization, graph membership, and full Cypher
predicates remain residual checks. Index continuations retain their schema and
index identity and reapply the original predicate bounds. Sparse IN sets use
deduplicated equality seeks, with the selected branch retained in the cursor.
Mixed or rounding-sensitive operands stay residual. Without a usable index,
execution keeps a budgeted primary scan.

Retained results and temporary pages share one quota, backed by a reclaiming
allocator even when the host supplies a request arena. Releasing a page frees
its physical buffers instead of merely decrementing a counter while the host
retains the allocation. RPC bodies and decoded reply metadata use temporary
arenas; only returned entities and continuation bytes survive each call.

Single-pattern node and traversal MATCH pipelines, including bound inputs from
earlier clauses, stream through ordinary WITH projections into
bounded top-k or hash aggregation. Grouped count/sum/avg/min/max retain one
accumulator set per group, and aggregate expressions are evaluated after the
fold. Intermediate reductions can feed subsequent clauses. Unordered LIMIT
propagates demand to adjacency pages; completed routes release their temporary
buffers after feeding the consumer. DISTINCT, collect, OPTIONAL and general
multi-pattern joins retain the budgeted materializing path.

Shortest-path frontiers share equal-distance predecessors in a DAG. Each
vertex/branch expands once; qualifying paths are reconstructed lazily with all
ties preserved. Undirected positive relationship-simple cycles retain separate
first-edge branches. A final shortest pattern can use earlier comma-separated
bindings when no unresolved constraints or used relationships require exhaustive
selection. Eligible global count(*) expressions fold endpoint multiplicities
directly over the DAG, including shortest-cycle branches. Materialized path
outputs can still be exponential and remain subject to output/work/memory limits.

Distributed point and endpoint-batch reads use the immutable key ranges captured
with the owner cohort. Keys are partitioned by owning shard; temporary routing
and reply-decode buffers are reclaimed after retaining positive results.
Statement-covered owner validations are classified once when acquiring the
cohort, so expression checkpoints perform one statement check. Independent
owner validators remain on every checkpoint; each owner call still checks its
retained authority. Incoming adjacency still follows source-owner routing. SQL overlays reconcile
staged postimages and exact relationship tombstones when pages are read, so a
node-only GRAPH_TABLE query does not enumerate a staged source's adjacency.

The public regression corpus checks one-anchor indexed property reads, indexed
read-your-writes after a key moves, composite-index and empty-page continuations,
2 MiB local grouped/compound aggregate pipelines, and shared shortest-path
expansion with every result tie preserved. Authenticated HTTP tests compare
2,000 and 10,000 inputs under the same 8 MiB execution budget, including RPC
page/decode overhead. Traversal regressions require a one-edge LIMIT, bounded
grouping/top-k, and a 32,768-route shortest-path count within 2 MiB without
materializing those routes. Shortest-path LIMIT releases reconstructed routes
after feeding its consumer; count overflow is reported only for qualifying
endpoints. SQL overlay tests reject any adjacency callback for a
node-only staged query and qualify explicit/derived edges, rollback and row
policies through HTTP and pgwire.

| Surface | Implemented behavior | Qualification and compatibility bounds |
| --- | --- | --- |
| Grammar/compiler | Generated conflict-free grammar; spans; Unicode strings and identifiers; owned AST; named and positional parameters; compile budgets | Full selected-version conformance suite, temporal/spatial types, additional Java regex constructs, legacy integer id() |
| Read execution | MATCH/OPTIONAL MATCH, directional and variable-length paths, zero-hop paths, relationship uniqueness, UNWIND, list and pattern comprehensions/slices, chained comparisons, all/any/none/single, reduce, map projections, correlated read EXISTS, WITH/RETURN, grouping, DISTINCT, ORDER BY/SKIP/LIMIT, UNION, correlated CALL | General pipeline streaming/spill, schema type binding, comprehensive expression compatibility |
| Values | Qualified node and relationship identities, paths, lists/maps, three-valued equality and partial comparisons, numeric comparison, Cypher ORDER BY type ordering, tagged JSON | Decimal-string signed integers, typed parameter maps and collision-free envelopes implemented; SQL graph datum encoding implemented; pgwire JSONB entities, prepared parameters, NULLs, and ordinary-table joins qualified |
| Write execution | CREATE/MERGE/SET/REMOVE/DELETE/DETACH DELETE hooks and statement overlays; native graph effects survive transaction prepare/reopen/commit/abort; source range and guard regressions qualified | Production authenticated distributed provider, principal-bound durable receipts, duplicate request-ID rejection, and concurrent MERGE qualified through the 66-check real three-metadata/three-data-node campaign, including owner-node receipt restart recovery. Receipts are replicated in metadata Raft; reads on every metadata node, availability while the original owner is killed, global request-ID rejection after leader failover, and opaque read/snapshot recovery are qualified. Recovery after owner loss while a committed write waits for embedding-index visibility is qualified without replaying the statement. The principal-bound `/transactions/{id}/status` receipt remains readable after table permission revocation and across owner loss, failover, restart, and table removal; it contains no staged rows or execution plans. |
| Search | Typed antfly.search procedure, lossless private owner RPC, bounded top-K merge, score propagation through traversal, and explicit approximation metadata for owner-local scoring | Production preparation, owner-cut ranking with native authorized-candidate predicates, work budgets and approximation metadata implemented; full-text HTTP and semantic/hybrid provider transport on one and two owners qualified. Full-text search is also qualified on real replicated owner nodes. Protected full-text tables are qualified, including score invariance after hidden documents are added. Protected semantic/hybrid search and post-commit indexing of a Cypher-created row are qualified on one and two shards. |
| Graph catalog | Durable create/read/list/drop/rename; conditional definition updates; immutable table/index incarnation bindings; scoped table targets; mapping validation; graph resource permissions; generated SDKs | Six native HTTP revision checks qualify stable graph incarnation, monotonic revisions, stale/concurrent update rejection, prepared SQL invalidation, and authorization of newly mapped tables. Owner readiness checks are available in the catalog UI. These revision checks also pass through the real three-metadata/three-data-node campaign, including receipt availability across failover and restart. |
| Native execution | DB.queryCypher over caller-retained immutable table cuts and authority callbacks; physical adjacency paging; node/relationship projection, revocation checkpoints | Signed owner-side row authority, borrowed cohort fences, immutable owner cuts and private authenticated distributed paging implemented; protected-table and real three-metadata/three-data-node integration qualified; public C/Lite catalog, text-query, write, and receipt bindings implemented and qualified; public Zig and WASM catalog/query/write/status interfaces use the portable single-source host and pass native and WASM smoke qualification |
| JSON graph matching | Relationship-simple trails; parallel identities; node revisits; zero hops; conjunctive uniqueness; independent OPTIONAL scope | All deployment/cross-table paths and compatibility fixtures |
| HTTP text queries | POST /graphs/{name}/query, scoped graph/table authorization, immutable owner cuts, EXPLAIN/PROFILE, strict request decoder, exact integer parameters and typed results | Authenticated three-metadata/three-data-node paging and mutations qualified; broader policy revocation and failure qualification remains |
| SQL composition | Generated native GRAPH_TABLE MATCH/COLUMNS syntax; SQL token lowering into shared graph AST; positional parameters; graph-aware resolver contract | Native resolver and typed graph rows share one cut with ordinary SQL scans and multiple graph catalogs; prepared parameters, joins and rename behavior qualified through HTTP. Pgwire entities, prepared parameters, NULLs, and joins qualified through psql. Read-only and serializable transaction composition is qualified through psql. Staged SQL updates, inserts, deletes, joins, field-derived adjacency replacement and NULL removal, rollback, and policy-hidden postimages are qualified through psql. Staged explicit graph effects with their authenticated source dependencies and commits, and Cypher derived-adjacency read-your-writes are also qualified. Broader transaction fault qualification remains. |
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

MATCH equality joins use the same bounded lookup planner for fused reductions
and materialized consumers such as UNWIND and collect. Materialized consumers
retain matching rows rather than Cartesian candidate states. Streaming DISTINCT
aggregates retain per-group unique arguments with Cypher value equality and SQL
NULL exclusion; min/max remain constant-state reducers because duplicates cannot
change their result.
SQL GRAPH_TABLE pushes row-local predicates through inner/cross joins and the
preserved side of outer joins using bound column identity. Derived queries,
nullable join inputs, and observable graph source ordinals retain their semantic
boundaries. Bound duplicate predicates are removed without cross-domain coercion.
Pushdown quotas leave remaining predicates in SQL residual evaluation;
they do not reject otherwise valid statements. A NULL operand in an ordinary SQL
comparison proves the graph scan empty before its producer is opened.

SQL row-local traversal cursors retain an explicit depth-first stack and bounded
adjacency pages across pulls. They preserve relationship uniqueness, repeatable
walks, variable-length edge bindings, forward property constraints, and written
path direction when an endpoint seek reverses execution. Quotas accumulate across
pulls, and closing a suspended cursor releases its pages and route state. Selective
interior anchors and stateful selectors retain the general producer's plan.
Each cursor binds its backend into a stable arena once, preserving transaction
overlays across pulls while authority checkpoints and cumulative quotas remain active.
Overlay scans preserve opaque native continuation tokens and emit dirty postimages
in a separate phase, including inserts, replacements and tombstones.
Equality lookup planning includes the first pattern of a correlated MATCH and
optional producers, whose null extension happens before downstream filtering.
COLLECT stores argument values per group; DISTINCT collection shares typed
argument equality and NULL exclusion with the other streaming reducers.
SQL graph pushdown carries typed Boolean trees and lowers same-column equality
alternatives to indexed membership. Unsupported OR arms decline the whole hint;
SQL remains responsible for residual truth and errors. Direct column lineage
survives transparent derived projections and renaming, with aggregation, windows,
materialization, OFFSET and LIMIT kept as semantic boundaries.

Native Cypher CREATE defaults, generated-column SET through aliases, and REMOVE
SQL NULL postimages are qualified against durable SQL rows. Cypher entity properties
omit SQL NULL and document JSON null fields; `keys(n)` and `properties(n)` do
not expose them, while nulls inside user maps remain data. This behavior is
qualified through native storage and durable SQL normalization. Normalized rows
retain their private preparation marker through the coordinator, owner RPC,
compiled owner, and native storage boundaries.

Relationship-only CREATE/SET/REMOVE/DELETE stages native graph effects without
rewriting unchanged endpoint documents. SQL JSON nulls, SQL NULLs, constraints,
and unrelated enrichment inputs retain their stored representation. Protected
sources are checked against both UPDATE USING and WITH CHECK on the unchanged
typed native row, once per source, and retained by a content/version read guard.
Graph-only prepares reserve source ranges durably, including across reopen, so
removing the document rewrite does not weaken conflict protection. Unit CALL
subqueries propagate their write status through nested calls and UNION arms;
an outer RETURN is optional when the statement writes.
Cypher preparation drains pending local endpoint cleanup in bounded pages before
capture, including on hosted/WASM owners without background workers. Admission
cancellation and deadlines apply between pages. External lake owners reject
graph-only mutations at PREPARE, before a durable transaction decision.

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

Public Zig and WASM use the same durable catalog, immutable storage cuts,
postimage normalization, mutation planning, and native transaction ledger.
Qualification covers exact integer parameters, persisted relationships,
duplicate admission, rollback before commit, and minimal receipts. Hosted
receipt readiness is conservative against the owner's current publication
target. Capture rejects wrong-owner and released capabilities; pinned primary
and adjacency transactions survive subsequent document and edge publications.
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
Pinned storage reads decode each provider page in a temporary budgeted arena
and return authorized entities and cursors. Lazy overlay lookups and expansion
clone only needed entities, then reclaim the storage replies and JSON buffers.
Terminal scan operators also reclaim their page-owned input before advancing.
Native postimage normalization and field extraction likewise reclaim JSON and
schema-expression scratch buffers before the next mutation. Empty relationship
catalogs bypass extraction. Native regressions execute 8,000-row CREATE,
SET/REMOVE through repeated nested aliases, and DELETE within the default
statement memory ceiling.

Query-owned string caches reuse codepoint/trim analysis, casing, reversal, and
numeric conversions for immutable input allocations. Uncached scans reserve
byte work, long Unicode scans checkpoint, and CONTAINS uses a linear KMP scan
with cached needle preprocessing. Large immutable maps similarly index property
names once; small maps use charged bounded scans. Map projections and property
merges avoid quadratic name searches. Regression tests cover bounded work,
cancellation, rollback before commit, and retained page-allocator lifetimes.

The final native 1,000/2,000/4,000/8,000-row DELETE probe reports
35,001/74,001/156,001/328,001 work units, including retained-cut sorting.
The 8,000-row delete dropped from roughly 44s to 4.3s; the cached 400,000-byte
trim/size reduction dropped from 5.5s to 0.06s. SET and REMOVE also scale roughly
linearly through 8,000 rows. These timings are diagnostic, not test thresholds.

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
Top-level node properties `_edges` and `_embeddings` are native ingestion
commands and reject nonnull Cypher assignments with `CypherInvalidPropertyValue`.
Use relationship patterns for graph writes and the native batch API for external
embeddings. These names remain valid inside user maps and on relationships;
assigning null or removing them from a node is allowed.
Relational SQL datetime columns are exposed as signed integer epoch nanoseconds
in graph values, including mutation postimages and MERGE keys. Cypher writes
must use that integer representation; SQL timestamp strings raise
`CypherTypeMismatch` before commit. SQL retains its timestamp string input and
formatted timestamp output. No implicit temporal type conversion happens during
a graph mutation, so repeated MERGE uses the same value before and after storage.
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
Relationship identity is derived from the full stored endpoint/type/edge-ID/
producer tuple, with an additional field-projection discriminator. Native edge
IDs, including ones beginning with `cypher:`, are scoped to that tuple. Updating
metadata never changes identity, and parallel native edges on distinct endpoints
remain distinct. New Cypher relationships use this same identity immediately,
before commit. Treat public `elementId()` values as opaque.
Relationship property updates retain the native creation/update timestamps and
physical weight from the captured storage cut. Nonnegative numeric `weight` properties also
set the native weight; removing or assigning a nonnumeric or negative property preserves the
captured physical weight. These storage attributes travel only on authenticated
owner RPCs and are omitted from public graph values. Existing native TTL origins
are preserved by the storage layer when the same explicit edge is updated.
Algebraic path conversion preserves per-node table qualifiers, including
same-table tags and cross-table endpoints, through native and HTTP path results.
Graph preflight advances bounded receiver-local producer readiness bookkeeping
before waiting for enrichment, so a replica with no optional producer worker can
publish its readiness registry. Retryable enrichment admission failures retain
their typed diagnostic and HTTP 503 status through retained-owner RPCs.
Direct native adjacency reads use the native `full_index` visibility barrier;
Cypher captures drain maintenance before freezing their coherent cut.
Statement-local entity indexes deduplicate producer authority lookups without
enumerating producers during snapshot capture.
Native metadata uses the private Cypher wire envelope only when the entire
object has exactly `source_table`, `target_table`, and `antfly_cypher`, the table
names are strings, and the marker contains exactly version 1 and valid tagged
map properties. That complete shape is reserved for the storage protocol.
Ordinary, partial, malformed, and unsupported-version marker values remain user
metadata, so a user property named `antfly_cypher` does not make an edge unreadable.
Postimage read authority applies to producers independently of graph label
membership. Referenced producers are resolved lazily from the same immutable
storage cut, including nonvertices, with budgeted, deduplicated lookups and
indexed identity resolution. Losing read authority hides their assertions in the statement view.
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
  C/Lite entry points use the same compiler, immutable storage cuts, postimage
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
  Label predicates use indexed structural equality under the statement work
  and cancellation meter, including native capture and routed owner projection.
  Owner RPCs inherit the remaining work ceiling and return spent units to the
  coordinator. Retained entity aliases resolve postimages by identity on access;
  repeated writes to one shared entity do not refresh every referencing row.
  Ordinary property maps remain immutable snapshots.
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
  replay. The workbench classifies each Run/Profile submission with server-side
  EXPLAIN before execution, then persists a write ID when the compiler reports
  effects; comment syntax cannot bypass receipt recovery. Workbench receipts are
  persisted per principal, and minimal status reads continue to work after table
  permission removal. Temporary SQL statement-cut unavailability retains its
  retryable SQLSTATE 53300 identity across runtime archive callbacks.

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

Replicated policy rejections use the same typed row-execution error registry as
participant transport. A denied PREPARE advances its Raft entry and returns a
403 to the coordinator, which durably aborts and exposes `RowPolicyDenied` with
an aborted write outcome. Catalog changes remain typed 409 rejections. These
rejections never turn an already committed resolve into a rejected write.
Normalization reuses validated overlay nodes when the schema has no expression
bindings or numeric modifiers, avoiding JSON round trips for ordinary bulk
updates while retaining normalization for defaults, generated columns, and
numeric constraints.

Retained graph RPC ingress and clients share one explicit diagnostic vocabulary.
Owner-side type/property validation keeps its client error identity and status,
including relational timestamp normalization; it must not masquerade as a
retryable cursor restart. Producer/readiness changes before capture report a
retryable graph snapshot failure. SQL `GRAPH_TABLE` reports these temporary
readiness failures as SQLSTATE `53300`, permitting a bounded retry of the whole
stateless read while preserving transaction and write-receipt rules.

Managed dense-vector configs may name the generator's own embedding output at
both generator and index scope. Ordered publication accepts that exact match;
foreign selectors and mixed source lists remain excluded. This prevents
replicated semantic producers from repeatedly computing outputs that their
publication gate would reject. `antfly-generated-vector-contract-test` covers
selector isolation and native publication planning, with the replicated
owner-loss fixture exercising provider completion and durable receipt recovery.
Provisioning snapshot races and pending Raft capability barriers retain stable
availability errors across runtime archives; availability does not establish
that a preceding mutation was never admitted.

The workbench releases an unresolved write only after an explicit server outcome
or transaction receipt proves its disposition. Permission/conflict reason codes
alone never override an unknown outcome. Receipt inspections also fence their
responses by principal scope and write ID so an older inspection cannot replace
a newer write's recovery state or message.

### Physical execution and bounded retention

Embedded Cypher and SQL `GRAPH_TABLE` retain primary and outgoing/reverse graph
storage transactions under the shared statement cohort, then release every
writer/replay fence before execution. Storage pins contain no vertex or edge
population. Lite read forks borrow their original checkpoint/inode pin through
the erased read family; each fork owns its own decode scratch. Graph pages fork
both captured adjacency reads so page-local decoding does not accumulate in the
storage owner. Catalog, schema, policy and graph identity checks remain live.

Production and embedded write overlays load entities on demand. Scans merge
native pages with staged postimages. Read-only overlays leave untouched rows
page-owned; write scans retain their existing preimages to avoid redundant
point lookups. Mutation and retained-identity lookups own private copies; incident adjacency is loaded
before validating node deletion. Native conflict/range guards still certify
the captured cut at commit. Producer visibility, regenerated field ownership,
relationship postimages, and rollback remain shared across every clause.
Detached Lite promotions validate their source catalog/replacement generation,
then merge destinations and publish the SQL decision on the live coordinator
under one short owner/path fence. Foreground recovery cannot abandon a partially
prepared promotion. Runnable resolver/promotion debt keeps maintenance scheduled
after enrichment is clean; blocked stages do not force a busy idle loop.

Anchor scans visit each physical table once even when multiple logical labels
share it, applying the union of their predicates before binding. Single-node
terminal projections fuse their scan with unordered limits, stable ordered top-k
(`SKIP + LIMIT` heap capacity), or global count/sum/avg/min/max accumulators.
Ordinary unordered projections avoid singleton grouping and unused sorting
records. Page/evaluation arenas are reclaimed between batches; retained heap
entries and min/max values have independent lifetimes. Supported grouped reductions
retain one accumulator per key. DISTINCT retains typed unique keys, including
null and numeric equivalence, rather than every source row. Unordered DISTINCT
propagates `SKIP + LIMIT` demand after deduplication; row-local `WITH DISTINCT`
stages share their key set across producer pages. Collect retains its result payload; correlated pattern expressions retain the
general operator. Row-local WITH and UNWIND form a streaming pipeline into
reducers and LIMIT. UNWIND preserves scalar, list, null and duplicate semantics,
reclaiming each expanded row after its downstream consumer finishes.

Independent all-path patterns feed their bindings directly into the next pattern
without materializing Cartesian products. Ready equality constraints use native
indexes, including properties of previously bound entities. An unindexed inner
node scan can build a statement-local hash lookup only when every backend page
certifies a complete, authorized, unfiltered scan. Its reclaimable cache is capped
at the smaller of 8 MiB and one eighth of the statement memory limit; exceeding
that cache quota releases it and resumes ordinary scans. Other predicates,
relationship uniqueness, null equality and duplicate matches remain residual
Cypher checks. Same-property equality OR trees normalize to indexed membership
only when every arm provides safe candidate evidence; the complete Boolean
expression remains residual. Explicitly required catalog labels contribute their
equality predicates to anchor planning. Unlabeled scans preserve the union of
label memberships instead of conjoining unrelated label predicates.

SQL `GRAPH_TABLE` prunes unused row-local COLUMNS expressions and pushes compatible
scalar property conditions into MATCH, preserving SQL residual comparisons.
Aggregation, DISTINCT, ordering, optional matching and selected paths retain
their required semantic barriers. Observable SQL `_id` values keep source
ordinals, so those scans retain SQL predicates outside MATCH. Tagged graph
continuations replay the same optimized relation with bounded page retention;
legacy ordinal tokens keep their original unfiltered-source interpretation. Ordinary
all-path relations, including comma-separated patterns, expose a pull cursor: each SQL demand reads another bounded graph page
under the same cut and cumulative work/memory ceilings. SQL LIMIT can stop the
source without retaining previous pages. Advisory SQL row goals influence page
size without truncating the relation. Suspended pattern joins retain their current
route, shared bindings, deferred constraints and relationship uniqueness across
fetches. Terminal node scans amortize reads with bounded anchor pages. Each production binding owns its authority
channel, RPC client and staged view for the cursor lifetime.

Path planning compares every vertex before opening an anchor scan. Existing
bindings and document-key seeks take precedence over constant property filters
and label-only scans. Reverse execution restores the original order of named
paths and variable-length relationship lists. Interior anchors traverse the
reversed prefix and feed each match directly into the suffix, retaining shared
relationship uniqueness and restoring the complete path's source order.
`EXISTS` supplies one-row demand
to ordinary terminal projections (and implicit projections on MATCH/WITH bodies),
preserving SKIP, LIMIT 0, DISTINCT and global-aggregation semantics.

Read-only statements derive conservative property requirements and carry them
through pinned native reads, distributed endpoint batches and owner RPCs.
Relational readers project only the required columns, including every catalog
label-predicate field; row policies still inspect the complete typed row before
projection. Returning an entity, dynamic property access, unknown procedures,
writes and staged reconciliation retain complete documents.
Entity aliases forwarded by `WITH`, including renames and wildcard forwarding,
retain their lineage without demanding complete properties. Only subsequent
consumers determine which fields must be hydrated; returning the full entity
still retains its complete document.

Eligible single-pattern shortest paths use breadth-first traversal and cached
endpoint hydration. Bound destinations stop after the winning frontier layer.
All-shortest ties are preserved; undirected positive cycles retain first-edge
branches to avoid incorrectly eliminating valid cycles by
relationship uniqueness. Correlated/multi-pattern matches, path-dependent
constraints and prebound route aliases use the exhaustive selector. Owners
accept bounded, aligned endpoint batches; the coordinator partitions these
by table/owner, preserves missing/hidden values,
and checks overlapping-owner results. Runtime caches deduplicate hydration
without changing parallel-edge multiplicity. Positive lower bounds fetch and
cache authorized adjacency only as the frontier demands it. Repeatable walks
use vertex/lower-bound distance states directly. Uniqueness modes first probe
those states without emitting speculative results, stopping at the winning
bound-target layer (or the first eligible layer for an unfiltered LIMIT 1).
Acyclic captured prefixes admit compressed distance states and shared tied
predecessors. Cyclic prefixes use exact breadth-first route histories, extending
the same adjacency cache only when needed. An unsuccessful walk probe proves
there can be no qualifying unique trail. Mapping ranges
are sought directly during replay, which consumes logical execution work without
charging physical reads again. Their route
enumeration remains combinatorial when uniqueness requires it; neither vertex
compression nor early deduplication may discard valid qualifying trails.

Read-only MATCH/projection pipelines inside write statements use the same
streaming operators. Every write remains a materialization barrier; read caches
are scoped to one pipeline and cannot survive a write. Entity expressions still
refresh through the statement postimage and policy projection. Native bound-pair
overlays pull opaque base pages on demand, merge staged pair postings (including
document-derived replacements) in a separate phase, and reuse complete captures
for later reads. Partial captures do
not drain parallel relationship bags merely to satisfy LIMIT. Hosts without
native pair seeks retain the once-per-adjacency fallback index.

Eligible ALL-path endpoint reductions discover authorized adjacency from sparse
(pattern edge, hop, identity) states. Rejected intermediate node predicates leave
the branches behind those nodes unopened. Finite bounds and a DAG proof permit prefix
compression under trail and node uniqueness. Unbounded segments first discover
the authorized, property-qualified closure and prove its maximum depth with a
topological pass; cyclic segments retain exact traversal. Repeatable walks admit bounded
product states directly. Pattern labels, directions, relationship types,
constant properties and parallel-edge multiplicities remain exact. Edge patterns
sharing a mapping/direction domain reuse one capture; mixed types and directions
use separate cached domains. Trail reductions partition domains by overlapping
physical mapping sets and prove each component acyclic: disjoint relationship
types can revisit a node across segments without reusing a relationship.
Node-simple reductions retain the whole-graph acyclicity proof. Rebound
route aliases, repeated node aliases, path-sensitive constraints and unproven cyclic
trails retain exact enumeration, reusing captured adjacency where applicable.
Intermediate counts saturate above the result range, so only qualifying endpoint
counts can report overflow. SQL GRAPH_TABLE exposes this through the exact-count
cursor hook under the same owner cut, with SQL residuals and ordinal selection
retaining ordinary pulls. SQL aggregates can additionally negotiate a weighted
endpoint cursor before their first pull. Only endpoint-local graph projections
and deterministic SQL inputs qualify. Multiplicities reach SQL reducers through
bag-preserving scan and projection adapters: checked wide integer and exact NUMERIC states, NULLs,
DISTINCT, grouping, filters and spill retain their SQL behavior. Non-DISTINCT floating SUM/AVG,
ordered aggregates, observable source ordinals, route-sensitive expressions and
nontransparent relational operators retain ordinary rows. Scalar repeatability is
checked independently of parallel execution support: CASE, Boolean expressions,
IN lists and pure builtins retain weights and their ordinary lazy evaluation.
External decisions and consumable pattern cursors require ordinary rows. Pinned
statement settings remain stable inputs. The cursor buffers endpoint contributions
for one bounded anchor batch, never the complete result or route bag. It retains
at most 128 validated, independently owned roots under a soft memory allowance.
Anchor reads remain narrow so large records cannot force oversized scan pages.
A declined batch proof replays retained roots through ordinary cursor frames;
those roots and their scan frame remain pinned across partial pulls. Soft seed
admission failure flushes the retained prefix and disables batching.
The negotiation explicitly distinguishes exact multiplicities from presence.
SQL MIN/MAX, Boolean reductions and DISTINCT aggregates request unit endpoint
contributions when every reducer is duplicate-insensitive. Saturated path counts
therefore do not force those consumers back into route enumeration. Mixing in a
bag-sensitive COUNT/SUM/AVG requests exact weights through every scan adapter,
including authenticated owner cursors; unsupported consumers keep ordinary rows.

SQL global aggregates whose arguments depend on different endpoints can negotiate
at most two dependency-specific weighted passes. Each page explicitly identifies
its active aggregate slots; SQL evaluates only those arguments and FILTERs and
updates the existing typed SQL accumulators. Each pass includes the shared WHERE
and its own FILTER dependencies. Both passes retain one cursor, immutable read cut,
authority checks, adjacency cache and cumulative work/memory quotas. Unsupported
or coupled dependencies decline negotiation and preserve ordinary execution.
Private tagged continuations keep partial pulls within one pass; they do not
change the public graph continuation contract.

Derived tables and inlined CTEs preserve these capabilities through repeatable
scalar projections and filters. A bounded relational plan maps each reducer's
required outputs through aliases and computed expressions to physical graph
fields before opening a pass. Predicates remain live in every pass; demanded
computations run in their assigned pass, and unused computations that could fail
retain their error dependencies. Projected rows carry explicit aligned weights
and active aggregate slots through filtering and pagination. A bounded operator
tree concatenates UNION ALL contributions and carries them through cross joins
with a repeatable side relation. Pinned table scans, literal relations and
repeatable aggregate summaries are captured once before graph production.
Graph-derived builds negotiate their own dependency fields and multiplicity
demand; a single build partition covers the union needed by probe consumers.
Both arms retain compressed contributions. Private unsigned weight limbs travel
with each build tuple through memory and spill, and matches multiply the two
weights with the active consumers' exact or capped overflow policy. Builds use the statement's scan and memory quotas and the typed hash join's
spill path. Unobserved side columns permit direct multiplication of contribution
weights. Conditional inner, left/right and full outer joins preserve duplicate
keys, SQL NULLs and ON residuals. Full and side-preserving outer joins track
matched build entries and emit each unmatched build contribution with its
original weight to every aggregate consumer. Membership lowering retains its NULL-aware summaries and
repeatable builds rather than expanding the graph input.
Each unsupported child retains
ordinary production with unit weights, so negotiation never requires advancing a
producer or rolling back another child's capability. A tree with no weighted
producer declines negotiation and retains the ordinary columnar execution path.
Positional set coercions,
NULLs and DISTINCT state stay in the typed SQL machinery. Pure direct
sort keys can be omitted when no pagination observes their order and the consumer
has negotiated duplicate/order-insensitive typed reduction. Other joins, set
operators, analytic windows, ordered/error-producing sort keys and
cardinality-changing stages retain ordinary production. No aggregate expression
is algebraically rewritten.

Materialized CTEs capture complete typed tuples with exact multiplicities in a
shared memory/spill replay. Every reference has an independent reader; aggregate
consumers retain weights, and row consumers expand them only when requested.
Capture evaluates the source once and retains every output field, including
error dependencies, so consumers with different field demands share the same
snapshot and values. Order-observable sources retain ordinary capture when the
aggregate-only proof would discard their ordering. Saturated producer counts
are never stored as exact replay weights.

SQL top-N negotiates endpoint bags only for repeatable predicates, projections
and order programs. The typed heap and spill codec retain one record per
contribution, with a private count clipped to the maximum observable window.
Only the final LIMIT window expands into output rows; OFFSET skips whole bags.
NULL ordering, typed comparisons, expression errors and positional coercions
remain SQL operations. A separate capped-count capability admits saturated graph
counts as lower bounds for finite windows, including through UNION ALL and
bounded side joins. Finite ordered and unordered subquery windows clip those
counts into exact selected multiplicities before downstream consumers see them.
Nested windows retain compact typed contributions in the statement's bounded
memory/spill cursor, and all downstream aggregate slots consume the same selected
bag. Only external row output expands multiplicities. Exact SUM/AVG and COUNT
consumers never receive saturated lower bounds.

Read phases share a bounded, quota-accounted cache of raw adjacency pages across
anchors. Cache keys include mapping, direction, identity and continuation; each
capture still applies its own property residuals and uniqueness proof. Admission
stops when the cache allowance fills, retaining borrowed pages until phase end.
Cache hits retain authorization/cancellation checkpoints and logical work charges.
Writes separate read phases, so neither staged nor durable mutations reuse stale
adjacency. SQL weighted cursors own the same cache for their immutable read cut.

For path patterns whose consumer does not observe the starting node, native
reductions combine at most 128 starting nodes per batch. Pure endpoint
expressions use scope-aware dependency analysis, including CASE, map projections,
slices, comprehensions and reduce; scalar functions explicitly opt into repeatability.
Consumer dependencies participate in traversal orientation: start-only grouping
or reductions can run backward and combine independent roots when endpoint label
and property admissions describe the same anchor population. Equal filter scores
alone cannot justify scanning a different table or label population without cardinality
estimates. Selective point and interior anchors still take precedence; route-observing
consumers keep their ordinary plans. Independent aggregate calls with different endpoint dependencies use at most two
orientation-specific passes over the same read cut and adjacency cache. Each call
retains its own complete accumulator; the original projection, ordering and
pagination run once over the completed states. Grouped or coupled endpoint
consumers keep their ordinary plan; no algebraic SUM rewrite changes NULL,
overflow or arithmetic behavior.
Read-only endpoint predicates separate provably total anchor-local admission
from residual endpoint evaluation. Admission filters roots before collecting a
batch; the residual must observe only the emitted endpoint. The totality proof
covers nullable entity properties and comparisons, preserving eager Boolean
errors by retaining an entire predicate whenever any operand can fail. Range
predicates alone do not override batching orientation. An optional snapshot-bound
index estimate probes at most 32 candidates per population and reports a bound
only after exhausting the index range. Unindexed and broad populations retain
batching; proven selective ranges can select an endpoint instead. Probe work is
charged as planning work without advancing a query scan or binding anchors. Native,
owner RPC and distributed providers share the same authority and read cut; active
write overlays decline estimates. The statement caches bounded estimate keys;
selective point seeks and bound identities retain priority.
A declined proof resumes each original anchor before emitting compressed rows;
soft batch allocation failure flushes retained roots and disables admission.
Unbounded DAG segments first form the exact mandatory minimum-hop frontier,
then sum all remaining path lengths in one topological pass. The same transition
operator applies relationship properties and multiplicities in both phases; no
subtraction from saturated counts can conceal a qualifying overflow. Long bounded ranges, including selective singleton anchors,
can use the same reduction when a bounded closure proof shows every captured
route fits the upper bound; other ranges retain the product-state reducer.
Fixed single-hop segments transfer their frontier directly to the next segment,
applying target-node constraints during insertion. They share the same captured
arc eligibility and uniqueness proof without copying every frontier through an
extra endpoint table.

Connected linear MATCH components combine shared endpoint constraints before
compression, including reversed pattern order and adjacent read-only MATCH
clauses. The compressed producer must prove uniqueness for the complete
component before using merged histories. A declined proof returns to the original
clauses, preserving their independent relationship/node uniqueness scopes.
SQL pull cursors share the same linear component normalization before weight
negotiation, retaining the original MATCH for ordinary execution. Directed cores
with unobserved one-edge leaves also admit local factorization: each junction
counts eligible leaf-edge assignments while excluding its adjacent trunk edge
under relationship-simple semantics. Cached local choices avoid carrying full
route histories; parallel edges remain separate identities. Local assignment
counting uses at most 2^L subset states for L leaves, rather than enumerating
edge permutations. Identical candidate sets use falling factorials; repeatable
leaves multiply independent cardinalities. Factors are cached by pivot and the
excluded edge's eligibility class. The core uniqueness
proof must still pass before any contribution is emitted. Node-simple paths,
observed leaf bindings, mixed directions and unproven cyclic trails retain exact
execution. Repeatable walks permit reuse explicitly. Leaf matching shares work,
authority, adjacency, property and memory limits with the statement.
Intermediate bindings observed by the consumer, optional clause boundaries,
early predicates and path-sensitive constraints retain ordinary execution.
Single-hop bound-pair plans keep their direct seek instead of discovering a
component closure.

Endpoint-local predicates, grouping keys, COUNT/SUM/AVG/MIN/MAX, their
DISTINCT variants, and COLLECT(DISTINCT ...) consume weighted endpoint rows
from those same states.
Scope-aware analysis also carries these bags through repeatable row-local WITH
projections, filters and UNWIND, including alias shadowing and lexical locals.
UNWIND preserves the incoming weight for each emitted list item. Intermediate
DISTINCT deduplicates the projected keys globally and resets each surviving
weight to one, so subsequent SUM/AVG need no exact upstream multiplicity.
Before proving endpoint dependencies, a backwards, scope-aware liveness pass
removes unused WITH bindings only when evaluating them is provably total: literals,
variable reads and nullable node property reads. Dead arithmetic, function calls
and parameter reads remain observable, preserving evaluation errors. DISTINCT
keys remain live even when a later projection discards them. Lexical free-variable
analysis visits each expression directly rather than rescanning it per alias.
OPTIONAL MATCH uses the same compressed ALL-path bags and tracks whether each
input actually matched before its downstream filters. An unmatched input emits
one null-extended row; downstream filtering never creates a replacement row.
This match marker is independent of saturated multiplicities and aggregate passes.
Wildcard DISTINCT includes every binding in its dependency proof; observing
an interior node or route retains exact production. Read-only intermediate
ordering can be omitted before COUNT/MIN/MAX reductions, including scalar
expressions composed from their completed states, when all affected
projections, predicates and sort keys are provably total and no pagination
observes their order. Ordered finite windows retain compressed endpoint bags in
a weighted TopK heap: excess multiplicities are trimmed at the worst key, SKIP
consumes weights, and intermediate WITH windows pass the clipped exact bags to
subsequent projections, filters, UNWIND, wildcard DISTINCT, MATCH, OPTIONAL MATCH
and reductions. Pure correlated EXISTS and pattern-comprehension expressions
run once per weighted binding, including unbounded initial MATCH pipelines
and predicates attached directly to MATCH. OPTIONAL MATCH evaluates these
predicates before deciding whether to emit a null extension,
preserving their own aggregate scopes. Read-only CALL subqueries use weighted
correlated apply: returned child weights multiply the parent contribution,
empty returning bodies suppress the parent, and unit bodies preserve it once.
Nested calls isolate projection metadata, aggregate demand and profiling buffers;
variable conflicts and recursion limits follow ordinary subquery semantics.
Pure CALL queries enter this pipeline from a weighted unit input, so an initial
CALL does not need an earlier MATCH or finite window to retain child bags.
Pure MATCH chains separated by WITH or UNWIND enter weighted execution directly.
Before another MATCH or pure returning CALL, equivalent complete bindings can
be consolidated when a downstream COUNT/MIN/MAX reduction makes their
interleaving unobservable. Counts are combined without changing bag multiplicity; representation-sensitive binding
keys keep integer/float distinctions, including inside containers. Pagination,
ordering, unit calls, effects and order-sensitive outer reductions block this
regrouping. Returning apply always executes each child from a unit bag before
multiplying its returned weights; internal child windows/reducers keep their
own invocation scope.
SUM can also consolidate under a runtime certificate: a private, unpublished
attempt verifies one integer sign per aggregate group, with zero neutral.
Nonpositive groups and nonnegative groups can coexist across aggregate states.
Successful attempts publish owned rows and exact weights. Mixed signs, floating
point or checked arithmetic failures retry ordinary execution with cumulative
work accounting; intermediate overflow and floating-point order stay observable.
Owned, statement-local pattern-expression memoization remains available during
these private attempts. Certification never turns identical captured pattern
expressions into repeated graph traversals, and cached values do not borrow
private evaluation scratch.
Weighted MATCH fuses row-local stages into their actual finite window or
reduction, carrying parent multiplicities through the fold. Small unordered
windows retain demand-driven traversal across subsequent MATCH stages: each
prefix binding runs its continuation before the producer advances, and consumer
completion unwinds the enclosing producers. Pure returning CALL bodies with
row-local projections participate in the same continuation, merging their
returned scope into each parent before resuming the outer consumer. Empty child
bags resume the producer rather than truncating its prefix. Child reducers,
DISTINCT, ordering and windows retain their own stage barriers. Filters and
OPTIONAL null extension remain in their own MATCH scopes; no prefix is truncated
before its continuation qualifies. After 64 unsuccessful routes, an endpoint-local
filtered window builds a lazy suffix reachability memo over node, pattern
segment and hop states. Admissibility follows pure row-local WITH projections,
filters and UNWIND stages, including renamed columns and returning CALL scope
merges. Child producers inherit finite demand from the enclosing continuation;
they do not need an artificial LIMIT attached to their own RETURN.
Physical page demand follows the same continuation chain. A shortest-path
frontier may stop at its first winner only when every enclosing stage preserves
that row; selective, DISTINCT and presence-probe continuations retain the full
winning-frontier search.
Proved pure MATCH and returning CALL continuations can also run as presence
probes in a private query scope. They stop at the first qualifying continuation,
and a bounded memo keys possible/absent results by captured bindings and the
current admission generation. Probes never update the parent's rows, weights,
DISTINCT sets or completed-child cache. Pure child windows, DISTINCT and reducers
can run to completion as private weighted stages before their returned bindings
probe the parent tail. Their scope and pagination remain inside the child;
unsupported or unavailable dependencies stay unknown. Semantic errors remain
observable only when ordinary DFS reaches them.
Consumer progress resets the stall counter; a later stall can still activate pruning.
Activation happens inside the suspended DFS, without restarting or duplicating
previously emitted rows. OPTIONAL retains its separate MATCH success flag:
before the first intrinsic match, only the MATCH predicate can prove absence;
after a match, a proved local continuation can reject remaining suffixes.
Speculative projection/filter errors leave a suffix unknown.
The original DFS continues while pruning impossible suffixes, including when
one late branch qualifies after an exponentially large rejected branch. The
reachability graph ignores trail/simple uniqueness as a conservative superset;
DFS still enforces exact histories and output order. An explicit postorder
stack avoids recursion proportional to graph depth. Unbounded segments require
a finite bound before quotienting. Speculative predicate errors mark a suffix unknown
rather than exposing errors early. Constant residuals, read authority and
statement budgets remain mandatory; unproved shapes retain
the ordinary producer. First successful rows keep lazy traversal.
Path demand analysis lowers scalar consumers before planning read-only statements
and SQL pull cursors. Non-null path counts need no path identity; length, node
and relationship cardinality, segment-list sizes and certified first/last node
accessors use execution-only bindings. WITH aliases carry these bindings, while
public wildcard projections expand from their original visible scope. Private
names cannot collide with user names. Full path values, DISTINCT path identity,
shadowed lexical variables and other unsupported expressions retain their
ordinary semantics and full-value dependencies.
When lengths are observable, traversal products retain total hop counts and
interned observed segment lengths alongside graph identity. Geometry captures
remain shared, and unobserved length dimensions do not enter the state key.
Distance layers cannot be quotiented away when their lengths affect output.
Adaptive presence probes carry the same summary prefix, preserving finite-window
DFS order. Proved shortest-path frontiers can fold endpoint/length bags without
expanding their equivalent route histories; saturated or unproved histories
retain ordinary execution. These bindings also participate in OPTIONAL null
extension, profiling scope, quota accounting and allocation-failure cleanup.

Summary lowering follows correlated EXISTS imports and pattern-comprehension
scopes, including local path shadowing. Total unused imports and projections are
removed before outer dependency analysis. After projection pruning and SQL
column negotiation, execution recomputes private summary demands; discarded hop
slots cannot keep a length product alive or disable a topological reduction.

Direct COUNT/SUM/AVG/MIN/MAX of total and segment lengths use an exact bag
algebra of counts, first moments and extrema for each observed marginal. Edge
transitions increment the appropriate segment and total moments; union merges
counts, moments and extrema. Proven DAG tails fold in topological order,
including bounded ranges whose complete closure fits the bound. Local grouped
reducers consume the algebra directly without expanding length histograms.
Signed SUM overflow remains exact because hop lengths are nonnegative, and
promoted AVG counts use scaled ratios.

Returning CALL exports carry caller demands back to private child projections.
Dead total exports and imports are removed to a fixed point, while every
aggregate grouping key (including constants and empty-input group boundaries),
DISTINCT key, window, sort dependency and potentially failing expression remains
observable. Scalar returning CALL bodies participate in endpoint locality and
length lineage. Materialized child bags and memo replay carry at most four
integer bins per marginal, preserving exact counts, sums and extrema. Coupling
these bins by splitting their arbitrary-precision weights avoids a Cartesian
product. This representation is admitted only when downstream consumers observe
individual marginal statistics or certified linear first-moment consumers; it
never substitutes for an observable path or a joint distribution.

SQL aggregate requests can certify a direct integer COUNT/SUM/AVG/MIN/MAX field.
Transparent typed renames propagate that certificate; filters, computed
expressions, windows, replay and joins clear it unless independently certified.
GRAPH_TABLE negotiates compatible statistic passes with shared geometry and
emits the same integer bins through its typed weighted cursor. Exact AVG/SUM
passes may share bags with MIN/MAX and Boolean reducers; duplicate-insensitive
states and partial merges track presence rather than overflowing a cardinality
counter. Physical contribution ordinals remain independent of exact logical bag
cardinality, including when wide summary bins are followed by unit rows.
Summary negotiation includes dependencies of SQL grouping keys, WHERE and
aggregate FILTER predicates. Providers certify that these fields are constant
within the physical endpoint bag; a length-dependent key or predicate declines
that certificate and retains the exact histogram. Endpoint integer fields are
rejected as summary candidates before allocating a reduction plan, avoiding
duplicate geometry retention for long fixed-hop patterns. The same dependencies follow
typed renames and transparent CTE projections, so a filter cannot regain a
certificate merely by moving into a subquery.

SQL summary negotiation carries a bounded disjunction of certificates. Each
single-input alternative selects one varying integer input and names all
inputs that must be constant within its bag. Joint alternatives prove shared
marginal lineage for all observed non-invariant columns. A scalar program may be affine relative to `hops`
while its offset or coefficient depends on an endpoint column; the graph
producer must prove those invariants before emitting synthetic representatives.
Certificates retain their separate invariant sets through computed CTE
projections, physical-column mappings and positional UNION ALL branches. They
are never combined by assuming that a second observed marginal is constant.
Nonlinear observations, predicate dependencies and computed error observations
invalidate incompatible alternatives. Proof growth is bounded; unsupported
shapes retain exact distribution execution.

Endpoint identity has its own lineage, separate from scalar affinity and opaque
lexical shadows. Transparent WITH aliases, wildcard projections and returning
CALL exports retain the original producer binding for property type admission.
The original expression still performs read-policy projection at evaluation;
rebinding an alias to a map, scalar or another value cannot revive an earlier
entity proof.

After private path-summary lowering, read execution specializes total numeric
length guards for one ALL-path segment into its hop bounds. The same pass runs
for native queries, returning CALL and UNION scopes, and SQL GRAPH_TABLE
cursors. It accepts comparisons and conjunctions (including chained and
reversed comparisons) with checked integer or floating-point literals,
invocation parameters and transparent constant aliases. Mixed numeric bounds
use the runtime's exact integer/number ordering, including fractional and i64
boundaries. A known NULL comparison accepts no rows.
The complete predicate and pattern property expressions must certify:
eager Boolean siblings or deferred pattern properties that can fail,
unproved bound types or projection boundaries, multiple segments and shortest
selectors remain barriers. Contradictory ranges keep ordinary empty/optional
semantics, and invocation-local copies leave cached query trees unchanged.

Native and SQL scalar programs share checked integer affine lineage for unary
signs, addition, subtraction and multiplication by constants. Every intermediate
must remain affine in one marginal, or carry an independent-marginal totality
proof for a first-moment consumer. Representative bags include both extrema,
so the ordinary scalar evaluators still detect intermediate BIGINT overflow,
including when a later operation cancels it. Native SUM also requires a
one-sign argument to preserve its contribution-order overflow semantics; mixed
signs retain the ordinary executor. Nonlinear expressions and joint extrema
remain distribution-sensitive.

Integer parameters specialize native affine coefficients per invocation, and
constant aliases retain their lineage through WITH and returning CALL. SQL
certificates recognize typed integer parameters without embedding their bound
values in cached programs. Endpoint property operands use symbolic affine
proofs: every endpoint is checked for an integer operand before any bag is
published, and ordinary representative evaluation retains read-policy
projection and expression error boundaries. Other operand types retry the
exact distribution executor, preserving NULL and overloaded `+` semantics.

Fixed lists such as `[n,n+1,2*n]` form finite unions of affine branches. They
retain at most four integer representatives per marginal; UNWIND evaluates the
original list for each representative with its exact multiplicity. This avoids
length histograms without inventing branch values or changing duplicate, eager
list evaluation, or intermediate overflow behavior. Conservative sign proofs
allow one-sign SUM consumers; nonlinear and distribution observers still
require exact values.

Fixed UNWIND repetitions retain scalar lineage and exact weights. Path/list
aliases in a fixed list carry their private summaries; full values are removed
only after consumer liveness proves that the alias itself is unobserved.
Local reducers consume affine moments directly through the ordinary row
pipeline; escaping bags retain integer representatives. Every intermediate
arithmetic bound is checked, and current fold weights account for repetitions
coalesced by CALL memoization, so repeated elements multiply every contribution
correctly.
Empty lists, observable full paths, heterogeneous marginal inputs and
length-dependent filters retain their normal semantics.

Invocation-local range proofs follow length aliases through transparent WITH
projections and split conjunctions only when every crossed expression is total.
Known Boolean siblings and equality comparisons on known node properties may
remain as residual filters; arbitrary functions, arithmetic projections, scope
changes, DISTINCT, ordering, pagination and post-OPTIONAL filters remain barriers.
Eager error-producing siblings and pattern-property expressions are never
pruned by a later length guard.

Fixed UNWIND lists also carry endpoint identity and integer property lineage.
All entity branches must have the same producer origin; scalar branches must
carry compatible affine proofs. The original list still evaluates with all duplicates,
NULLs and errors, and opaque rebindings block prior proofs.

SQL joint summary certificates prove that every input to an integer affine
consumer derives from the same producer marginal or a per-bag invariant.
GRAPH_TABLE checks each contributing projection against that private marginal;
columns such as length(p) and 2*length(p) therefore share representatives with
their correlation intact. CTE projections and positional UNION ALL mappings
carry this certificate and its invariant obligations. Nonlinear computations,
filters and grouping keys that observe the marginal decline the certificate
and retain exact distributions. Original expression evaluation on integer
extrema preserves checked intermediate arithmetic.

Independent segment lengths can also feed linear AVG, COUNT and eligible SUM
expressions, such as `avg(size(xs)+size(ys))`. Coupled integer marginal bins
preserve every linear first moment without constructing a Cartesian product.
An interval proof over the statement's path-depth bound must establish that
every intermediate integer operation is total for every possible coupling.
Unknown bounds, multiplication of varying inputs, possible overflow and unproved joint
MIN/MAX or DISTINCT consumers retain exact distributions. Native integer
parameters specialize the proof per invocation; transparent projections,
returning CALL and finite linear UNWIND branches preserve the proof.

SQL first-moment certificates carry request-local bounds checks in addition to
producer lineage obligations. GRAPH_TABLE supplies bounds for each observed
column and independently proves all private marginal dependencies. CTE relays
compose the checks through their original scalar programs; physical-column
and positional UNION ALL mappings translate the bounds into each caller's
names. Integer parameter slots belong to the current invocation, never cached
programs. NULL and other parameter types remain unknown and retain ordinary
execution. Filters, grouping keys and computed error observations cannot lose
their invariant obligations while a proof crosses a projection. Proof failures
return to exact execution before publishing any contribution; they do not
raise speculative arithmetic errors.

Bounded numeric certificates also retain exact binary lattices: multiplication
by `2.0`, division by `2.0`, and invocation-local floating parameters can share
length moments when every conversion and intermediate operation is exact over
the producer's bounds. Lattice numerators must fit binary64's significand and
results must remain finite without subnormal rounding. Arbitrary floating
scales and reciprocals that may round retain exact distributions. SQL preserves
the distinction between integer division, binary floating arithmetic and exact
NUMERIC literals; decimal literals must equal the certified lattice. Varying NUMERIC
division retains exact distributions because its row-dependent display scale
is observable even when the quotient is exact. Original scalar evaluation
still produces the public value and type. Proof construction visits each native
expression once and propagates allocation failures. SQL binary64 AVG transports
exact weighted totals and arbitrary-precision counts through the existing wide
reducer and spill format. Its inputs use exact binary64 decomposition rather
than SQL's rounded float-to-NUMERIC cast, with guard digits at the final float
conversion. Ordinary unweighted states stay compact. Floating SUM retains its
ordered overflow behavior and REAL retains its own rounding rules.

Segment provenance identifies a further shared axis: a linear consumer whose
coefficient is identical for every variable path segment depends only on total
length.
For example, `min(size(xs)+size(ys))` on a two-segment path uses total-length
extrema. This is an algebraic certificate, not an inference from independent
marginal extrema. The executor substitutes a canonical segment vector carrying
all variable hops in the first variable segment and zero in the other variable
segments. Fixed-hop segments retain their actual lengths and contribute constant
offsets to the geometry; whole-path slots still count every hop. Bounded totality
proofs cover all intermediate projections before substitution. Finite UNWIND
choices retain total-axis dependence even when their coefficients or offsets
differ. Their range certificate does not fabricate one shared linear formula;
subsequent arithmetic must remain total over the union of branch bounds. Every
native aggregate in that pass must factor through the same axis. SQL reducers negotiate separate
passes, and certificates compose through CTE projections and positional UNION
ALL mappings. Invariant predicates and grouping observations cannot read a
collapsed segment. Partial segment sums, unequal segment coefficients, unknown
arithmetic and expressions that may fail retain their original distributions.


Certified integer reductions also use a shared path-statistics algebra. Linear
cost directions carry signed extrema directly, so unequal segment coefficients
do not require a joint length distribution. Quadratic arguments retain exact
first and cross moments: advancing an axis updates its square by twice the
first moment plus the count, while composition retains both cross terms.
The physical moment basis contains only demanded monomials and their first-
moment dependencies. Passive geometric axes carry no moment state. Axes with
positively proportional gradient columns across all moment consumers share a
primitive integer weighted-sum axis. Opposite signs stay separate, preserving
signed correlations. This keeps whole-path and weighted-cost squares compact
even with many segments, without changing the original geometry used by extrema,
supports or admission proofs.
Polynomial certification checks every original intermediate against integer
bounds; mixed-sign SUM and integer-to-binary64 conversions outside the exact
range retain ordinary execution.

DISTINCT integer length expressions carry bounded bitsets of reachable costs.
Shifts, unions and composition preserve holes and zero-hop acceptance without
retaining route multiplicities. Every bitset operation charges statement work
and all storage belongs to the existing query memory budget. Current proof
limits are degree two, 32 geometric axes and a 65,537-value support domain;
unproved shapes use the existing executor rather than changing query semantics.

Consumers receive tagged aggregate arguments, never fabricated ordinary graph
rows. First-moment consumers receive at most two integral bins with exact bag
weights; extrema receive their exact value, and DISTINCT receives an owned
bitmap argument. All arguments for one endpoint/pipeline binding travel in one
owned batch, with one tagged entry per reducer and at most two bins per entry.
Grouping resolves the binding once and dispatches its licensed slots directly;
filters remain independent for each reducer. SQL tags and exact promoted bin
weights follow projection and positional UNION mappings. Compatible SQL
polynomial reducers share one traversal and one count/first/cross-moment basis; only cost
directions and DISTINCT supports add traversal state. Physical slots are dense
by unique coefficient direction, independently of the number of moment
consumers. MIN/MAX and shifted DISTINCT arguments share their respective
direction slots; constant offsets are applied only at emission. Merged projections are
replanned together and retain their original orientation and totality proofs.
Native reducers negotiate certificate families independently, so a floating
first-moment consumer cannot force an integer second moment onto a full length
distribution. These passes share the read cut, adjacency cache and quotas.
Native and SQL grouped integer COUNT(DISTINCT) merge sparse aligned bitmap
words directly into group state. SQL pages, CTE projections and positional
UNION ALL adapters own and clone bitmap arguments without enumerating bits.
The integer DISTINCT partial codec uses canonical sorted words (`AGB` version
1), validates signatures and cardinalities, and accepts older scalar-member
checkpoints. Sparse typed spill records preserve aggregate slot ownership
without initializing unrelated reducers; complete packets use one dense frame.
Existing dense checkpoints remain readable, including through partition transfer
and repartition. DISTINCT spilling emits bounded word records; final reduction
externally sorts by aggregate slot and word index, unions overlaps, and counts
bits once.
Hash-table growth is preflighted before retaining new state so memory pressure
uses the existing spill boundary. Sparse words preserve holes and widely separated correlated
offsets without expanding the support into scalar rows; ordinary fallback
contributions deduplicate against the same support. Original grouping keys,
filters and observed arithmetic retain independent invariance/totality
obligations. Global endpoint merging is allowed only when those observations
and row expansion are independent of the endpoint; otherwise statistics stay
with their original binding. Native and SQL execution share the same pinned,
authorized traversal and cycle/uniqueness proofs.

Each reduction scope interns structurally equal, repeatable aggregate calls into
one physical slot, including repeated calls in ordering expressions. Original
AST pointers map directly to that slot; DISTINCT flags and lexical scope remain
part of the identity. Pattern consumers and unknown/nonrepeatable functions do
not license structural sharing. Materialized scopes cache reducers lazily so
CASE/coalesce evaluation retains its error behavior.

Groups retain evaluated grouping keys and bare key-variable bindings rather
than entire source rows. Final expressions use compiled key-slot lookups with
lexical and ORDER BY alias-shadowing guards. A free-variable walk retains only
additional source bindings required behind those guards (including map selector
variables); unproved pattern dependencies conservatively retain their source.
Entity-valued keys always keep their complete graph identity. Forwarded entity
bindings retain only properties proved live in later projections; sibling
aggregate inputs do not become grouping payloads. Demands follow transparent
WITH aliases and merge for aliases sharing a physical key slot. Full-value
escapes, opaque captures, DISTINCT forwarding, writes and opaque authorization
projection callbacks conservatively keep complete observed content. Labels and
relationship physical attributes remain intact. Aggregate-only input payloads
are released with their producer pages.

Ordered DISTINCT with a proved key ordering uses a bounded group window and
collision-checked membership for retained keys only. Evicted keys cannot improve
against the monotone threshold; SKIP and stable ties use the same complete key
and first-seen sequence rules. DISTINCT reductions and unproved ordering shapes
retain their general executor.

General grouped projection constructs its source/alias environment once. Proved finite
SKIP/LIMIT windows use the existing stable TopK selector, with per-group scratch
for projection and ordering and independent ownership for selected rows. Every
group still evaluates its projection and predicates, including losing candidates
and LIMIT 0, so selection cannot suppress arithmetic errors. Unknown or invalid
window expressions retain their original late evaluation order.
Retained sort entries and unbounded grouped projections share immutable
allocations between projected cells and ordering keys, including nested payloads.
Each completed group releases its reducer arenas after its surviving projection
has acquired ownership, including filtered and duplicate groups. Error cleanup
can revisit released states without freeing their payloads twice. The clone memo
belongs to one entry's ownership transfer and never survives source recycling. Allocation identity,
rather than Cypher equality, preserves numeric representations and entity
snapshots; cached subtree heights enforce the same value-depth limit.

Proved HEAD/LAST, literal indices, and bounded front/back slice consumers of
COLLECT retain only their required windows. Compatible demands combine the
largest front and back windows and follow transparent WITH aliases. A weighted
ring of independently owned suffix runs handles arbitrary-precision bag counts
without expansion; eviction promptly releases obsolete payloads. Finalization
removes front/back overlap for short inputs before evaluating the original
consumer expressions. DISTINCT suffixes keep complete equality membership but
own only the demanded payload window, preserving first-seen order. Every input
argument still evaluates,
including after the prefix fills and for an empty prefix. NULL exclusion,
first-seen DISTINCT order, weighted multiplicity and lazy branches retain their
ordinary semantics. Combined cardinality and front/back demands use one private
collection summary, including through transparent WITH aliases. SIZE of a
row-local filtered comprehension consumes per-filter counters instead of retaining
its input collection. Bounded HEAD/LAST, index and slice demands also pass through
row-local mapped or filtered comprehensions. Each consumer evaluates all predicates
and selected projections, including discarded cells, but owns only its output
window; NULL projections remain list elements. Eligible REDUCE consumers use
independently owned sequential accumulator state instead of retaining COLLECT
inputs. Initial expressions must be closed and transitions may reference only
parameters, the element and accumulator. Transitions preserve encounter order,
NULL exclusion and intermediate overflow, including weighted repetitions; arbitrary
folds are never reassociated into commutative aggregates. A flat private carrier
replaces result root containers instead of adding nesting, so forwarding summary
rows preserves public depth limits and entity snapshots. Full collection escapes,
correlated or effectful consumers conservatively retain materialization. Predicate
and projection evaluation preserves NULL exclusion,
counts NULL projections, and records each consumer's first scalar error for its
original evaluation site; lazy branches and earlier aggregate argument failures
retain their error precedence. Resource/cancellation failures propagate immediately.
ANY/ALL/NONE/SINGLE consumers use per-consumer three-valued state in the same
summary. Each predicate stops at its own decisive answer (or first error), while
every original COLLECT argument continues to evaluate. SINGLE caps true
multiplicity at two, including promoted weighted bags; empty inputs and unknown
answers retain ordinary Cypher semantics.

Repeated scalar UNWIND domains can reuse exact sequential reducer snapshots
across fresh groups. A bounded statement-local cache keys complete captured
bindings by allocation identity and representation. SUM/AVG retain their original
floating transitions; DISTINCT support is shared without merging group membership.
Bounded collection snapshots include consumer decisions, three-valued predicate
state, prefix/suffix windows and sequential REDUCE accumulators. Deferred consumer
errors and uninitialized accumulators preserve lazy evaluation. Continued groups
and repeated DISTINCT partitions return to ordinary sequential membership and
transition processing.

For scalar reduction pipelines, bound WITH wildcards become explicit columns
before capture analysis. Pure returning CALL bodies participate through lexical
capture and output-dependency analysis, including nested calls, local WITH
scopes, expanding UNWIND sources, local reductions, DISTINCT, ordering and local
SKIP/LIMIT windows. The complete child domain contributes its original encounter
order and multiplicity; expanded aliases remain dependent even for constant
child sources. Internal variable forwarding can be pruned, while potentially
failing expressions and binding conflicts remain observable. Unused dependent
expressions still execute inside each complete probe domain, preserving their
scalar errors. Independent returned values come from reached execution samples;
dependent branches are never evaluated against placeholder values. Full
collections, effects and unsupported bodies retain their existing execution
paths.

Local reducer dependencies track cardinality as well as argument values:
COUNT(*) consumes preceding filters, expansions and nested child domains even
without a value argument. An ungrouped child reduction resets cardinality to one
before applying its predicate or window. Grouping keys retain their effect on
child multiplicity, including keys whose output columns are later discarded.
Invocation-local variation is tracked separately from caller correlation. A dead
bound-variable key that is constant throughout the child invocation becomes a
NULL grouping slot. Keeping the slot preserves zero rows on empty grouped input;
removing its value dependency allows reducer domains to share across callers.
Primitive bindings and certified entity-free containers qualify. Container
certificates bound every nested read to the structural depth limit; unknown or
excessive depths retain ordinary reads. Type proofs compose list concatenation,
records and nested sequences, with memoized DAG traversal. Original producers
still execute, so constructor, parameter and expression failures retain their
position. Entity reads may invoke backend visibility projection. Varying keys,
uncertified reads and potentially failing expressions keep their original
evaluation.

Structural proofs for imported containers inspect existing metadata only. An
uncached parameter is not traversed merely to plan an unused read, including
when an upstream producer is empty. Real evaluation registers immutable parameter
metadata with the statement owner and shares it across row and child epochs.
A reached read-only CALL may resolve its initial mandatory variable-read prefix,
stopping before fallible expressions or entity authority reads, then normalize
its dead captures. Memo keys use that certified body's actual dependencies;
uncertified later bindings retain their original captures and cannot reuse an
entry whose dependency was removed. Producer and grouping errors stay observable.

Primitive certificates follow parameter lists through forwarding projections and
nested CALL imports. Element validation is lazy: only a proposed dead variable
read requests a shallow primitive-element proof. Successful and failed proofs
are charged and shared once per immutable parameter for that execution. Planning
an ordinary COUNT(*) does not scan an otherwise unobserved parameter list.
Binding-type snapshots are immutable across UNWIND shadowing and child exports;
dummy NULL schema values never supply type evidence. Builtin signatures describe
successful result domains. Arithmetic and argument-dependent builtins compose
lazy primitive proofs, so derived keys can share domains without scanning
unused parameter lists or skipping their original producer evaluations. CASE
joins the successful result domains of its branches. List literals, indexing,
HEAD/LAST, slicing and reversal transfer their element domains through forwarding
aliases. Private collection summaries retain a separate logical input/window
certificate, so lowering cannot mistake a summary header for a user element.
Known map fields carry separate successful result types; property and constant-key
index reads preserve the selected field's certificate without classifying the
whole map as primitive. Comprehensions infer their projection in the element's
lexical scope, including nested records and lists. Entity-valued fields retain
ordinary reads. All producers, predicates and unused map entries still execute.
Captured producer proofs use their original lexical frame and statement ownership;
no certificate borrows a recyclable normalization arena.

An independent pure zero-or-one-row CALL also preserves a compressed axis. Each
child executes with one unit input, so filtering, DISTINCT, windows and global
empty-input reducers retain their invocation-local semantics before the caller's
multiplicity is applied. This composes with exact range and immutable-list
COUNT/MIN/MAX/integer-SUM certificates. The proof checks reads and output names,
including implicit names and nested CALL joins: exporting the axis binding can
conflict even when the child never reads it. Wildcards, correlated children
and effects keep ordinary execution. Pure expanding children also preserve an
unobserved axis for commutative COUNT/MIN/MAX consumers. Each complete child
bag retains local filters, DISTINCT, ordering, windows and empty-input reducers;
its emitted weights multiply the caller weight only after successful binding.
Observed range and immutable-list axes also compose with independent child bags.
An invocation-scoped observation keeps bag copies separate from set support:
COUNT multiplies the emitted child weights, while COUNT DISTINCT observes a
complete unique range once per group and list domains merge their exact support.
Integer SUM first combines the complete child's multiplicity for each group,
then applies the axis's ordered prefix bounds with that multiplier. Replaying
an entire mixed-sign axis per child could hide an intermediate overflow. Existing
floating accumulators retain sequential list replay. Floating SUM/AVG and
order-sensitive consumers otherwise retain sequential contributions.
Integer affine observer arguments share these range certificates. Every
intermediate arithmetic operation must be total over the actual range interval;
cancelled dependencies cannot hide an overflowing intermediate. Each aggregate
retains its own transformed extrema and ordered prefix sum. Reversal and
concatenation compose those summaries in producer order, while constant affine
images have one distinct value. A synchronous, charged proof cache shares source
DAGs without retaining transient AST addresses across normalization frames.

Reached row-local WITH pipelines carry checked integer affine lineage through
renaming, arithmetic and predicates. Each intermediate expression is total over
its own incoming domain, before any following filter narrows that domain.
Comparison restrictions use bounded ordinal searches; modular equalities solve
an exact integer congruence and preserve signed-remainder semantics and source
encounter order. Eager AND and OR operands are independently certified over their
full incoming domain before intersection or union. Compatible OR branches and
interior inequalities retain multiple ordered segments; incompatible overlapping
lattices decline. WHERE overlays projected aliases on the
input scope for ordinary projections; DISTINCT predicates and later stages
receive only the projected scope. Binding indexes
preserve first-name lookup and avoid quadratic proof lookup on wide projections.
The resulting ordered domain feeds the existing aggregate certificates, including
checked SUM prefixes and exact-recurrence AVG; unsupported or rounded cases
retain ordinary replay. Empty domains retain grouping slots and do not evaluate
unreached consumers. Every original logical stage cardinality remains recorded.

The same checked program feeds reductions and ranked windows. Its ordered bag
segments remain separate from unordered DISTINCT support: concatenation retains
copies and reversal changes both segment order and leaf direction. Source proof
work and segment count are bounded, with ordinary replay on a decline. Arithmetic
WITH DISTINCT keeps affine tuples as stable novel support, subtracting prior
ordered runs without materializing members. Overlapping progressions use checked
lattice intersections and bounded segment subtraction; constant tuples retain
the first encounter. COUNT, SUM and extrema can retain symbolic union support
through affine WITH projections and filters when stable subtraction would
exceed its segment cap. COUNT uses exact set cardinality; extrema use checked
bounds. SUM proves the novel signed moments and exact prefix bounds of each
monotone leaf in original source order, preserving intermediate overflow even
when the final total fits. Constant SUM arguments use the unique row count.
DISTINCT argument support remains separate from DISTINCT row identity.
AVG and other order-sensitive consumers still require an encounter-order proof.
A DISTINCT ordered page can select directly from union ranks without expanding
the lattice difference. This requires a certified
unit producer invocation,
so repeated outer rows cannot bypass shared DISTINCT membership. Independent
zero-or-one-row CALLs and singleton UNWINDs extend the program after their values,
binding compatibility and child-local cardinality are checked. Failed probes
decline before publishing results. CALL reducers and windows retain unit-input
semantics, and structural or varying captures keep their ordinary paths.

A certified complete scalar domain also carries WITH ordering and SKIP/LIMIT.
Ordinal slicing and sorting disjoint monotone runs happen before downstream
observers, without enumerating the omitted values. Pagination bounds and every
reached pre-window expression must be checked before publication; unsupported
scopes and fallible expressions retain ordinary execution. For interleaving
lattices, exact bag-rank searches locate a finite page, followed by a stable
merge of just the retained values. DISTINCT pages use exact union cardinalities
for the rank search and advance duplicate cursors together. Stable DISTINCT
pages with constant order keys use novel-prefix measures to skip prior support
and retain the requested source encounters without subtracting every lattice gap.
A checked, injective lattice union whose cardinality fills its complete envelope
is one ordered arithmetic run; interleaved lanes can therefore feed unbounded
or large-page scalar reducers without emitting each represented row. Retained
output and merge work use query budgets rather than a fixed page-size cutoff.
Duplicate bag occurrences remain in source order. Invariant integer AVG arguments use the
same zero-stride observer certificate.

Checked constant modulo periods partition an arithmetic producer into signed
remainder cohorts. Discovery is bounded by remaining query work and memory;
there is no fixed period-64 cutoff. Only remainder signs reached by the source
are considered. Whole-domain sign predicates use checked endpoint bounds,
leaving crossing intervals to charged binary searches. Divisors resolve through checked lexical stage
bindings, including parameters and projected scalar aliases. Unknown outputs
never become fabricated constants. Cohort stages advance in lockstep: DISTINCT
shares membership across cohorts and selects the first surviving source ordinal.
Global ORDER BY/SKIP/LIMIT barriers apply once to the combined bag. Small bags
use point ranking. Large bags with checked constant cohort order keys form
weighted rank buckets, skipping complete buckets arithmetically. Within ties,
selection follows the original source runs and preserves repeated occurrences.
Stable rank-prefix counts select slices in each run and retain its cohort owner;
reducers receive compressed slices rather than one context per retained row.
Common checked affine order keys rank directly across the combined cohort bag.
The reconstructed multiplicity must match the certified bag before publication;
unsupported order keys and scope histories keep ordinary execution.

All cohort programs and observer packets are checked before publishing groups.
Collision-checked hashing collects cohort summaries once per final group;
merge certification is linear in packets and observers rather than pairwise in
retained rows. Ordered point contributions preserve their encounter sequence.
Colliding group keys require a per-observer merge certificate: COUNT and extrema
commute, SUM also needs one sign across the colliding domains, and AVG requires
ordered point contributions or an identical constant sample. DISTINCT collection
arguments certified constant within a cohort consume one value at its first
source encounter; their shared membership and order use ordinary COLLECT
semantics. Point cohorts also support ordinary collection after a global window. Mixed-sign SUM
and unproved floating recurrence keep ordinary replay. Shared DISTINCT, global
windows, and filters retain each original logical stage cardinality. Proof work
is bounded independently of source cardinality.

Ranked group windows may ignore an aggregate rank only when the independent
suffix packet proves identical contributions for every candidate key. Both
aggregate aliases and structurally equal aggregate ORDER BY expressions use
this certificate. A finite independent outer list or range can move inside an
injective key axis while preserving each group's exact contribution sequence.
Checked scope-restoring projections may separate the axes. Bounded derived
outer aliases retain their lexical dependencies and execute in the complete
suffix packet, with the key identity preserved explicitly. Grouping keys and
the inner source must remain independent of those outer dependencies. Sources keep
their captured lexical frame, and WITH siblings evaluate in the same input
scope. The complete suffix proof validates every outer value before pruning
keys. Dependent projections, scope barriers, and failed proofs keep ordinary
execution. Logical stage counts continue to describe the original pipeline.

Total, invariant affine grouping keys can also collapse an unobserved producer
axis through independent fanout. The grouping key is retained and evaluated on a
representative, preserving empty-input grouping behavior and bag multiplicity.

Arithmetic AVG has a separate exact-recurrence certificate. A single ordered
arithmetic progression, including affine images, reversals and consecutive
concatenations, can advance a group's mean without visiting its values. The
proof bounds the complete prefix so integer values, half-integer means,
subtractions and denominators are exactly representable in binary64. Existing
state must match the certified prefix, including its weight representation.
A zero-stride sequence separately certifies the repeated converted binary64
sample, including rounded i64 endpoints. It preserves the exactly representable
weight bound and only extends an existing mean equal to that sample.
Independent zero-or-one-row CALLs preserve this arithmetic axis, including
nested children, filtering, local windows and global empty-input reducers.
Binding conflicts and failed children still run at their original boundary.
Unsupported prefixes, nonconsecutive domains and expanding children retain
ordinary non-DISTINCT contribution order and rounding. This certificate never
replaces a rounded online mean with a generic SUM/COUNT quotient.

DISTINCT integer observers retain exact arithmetic set support per group. Native
range descriptors carry membership through reducer snapshots without allocating
one key per integer. COUNT normalizes compatible source ranges; SUM retains
first-encounter range order. Exact union moments separate negative and positive
contributions, so removing prior members preserves checked SUM prefix bounds,
including overlaps, different strides, reversals and affine constant images.
Ordinary scalar membership remains available alongside compressed ranges, with
numeric equality shared across INTEGER and NUMBER values. A bounded union proof
may decline to charged encounter-order replay without expanding prior support.
Immutable producer support uses a shared DAG with one charged visit per node;
repeated producer branches cannot expand the certificate exponentially.

AVG DISTINCT shares the compressed integer domain and first-encounter DAG. A
nonmutating union proof measures novel support before publishing membership.
Exact moments propose an ordered remainder on the incoming lattice; subset,
disjointness and equal cardinality prove that it is the complete remainder.
Only an exact binary64 recurrence can consume that remainder as a summary.
Fragmented remainders and rounded prefixes replay incoming values without
expanding old compressed support. Bulk scalar replay clears the caller's single
value token so DISTINCT cohorts cannot reuse an ordinal for different values.
Independent expanding children may duplicate these idempotent set contributions.

Pure integer DISTINCT packets can merge into existing groups using their ordered
range encounters, including exact arithmetic AVG state. Floating SUM and
unsupported mixed packet forms preserve ordinary replay. Restored membership
does not recompute aggregate values or change floating representations. All
packet data is cloned before reducer
arenas close; group eviction releases its range support and encounter history.

Bounded COUNT group windows accept pure expanding children using dynamic emitted
weight certificates. Both materialized and streamed child boundaries check the
combined caller/child multiplicity; terminal contributions and cached reducer
packets retain their independent bounds. A failed bound retries ordinary deferred
reduction before publishing results, preserving overflow and middle-stage error
precedence. CALL plan output counts include the logical multiplicity of successful
joins on materialized, memoized and streamed paths, only on the recording pass.

An intermediate ORDER BY can share one sorted inner domain only after proving a
unique outer source, unchanged partition lineage in every DISTINCT and grouping
key, and identical captured inner inputs. Each nonempty partition contributes one
group.
Stable sorts preserve outer source order between equivalent copies of the same
inner domain; this preserves group discovery, unordered final windows and stable
aggregate-order ties without requiring final ordering by every group key. The
proof follows every intermediate projection and sort. Fallible independent
columns are prevalidated across outer bindings before replay: any failure
resumes ordinary stage execution, preserving error precedence across sort
barriers. The complete bounded snapshot must be admitted before any outer group
is published. Repeated outer keys, cross-partition DISTINCT, correlated ordering
and global intermediate windows preserve the global sort. The cache admits at
most the smaller of 4 MiB and one sixteenth of the query memory budget; admission
failure falls back before publishing partial state.

COUNT and certified numeric group windows may reuse a successful complete
domain, including a validated ordered domain, before ranking its group key. Such
a packet certifies
argument evaluation for rejected and LIMIT 0 groups as well as selected groups.
A failed argument probe disables reuse and resumes ordinary row/error order; it
never hides an error behind eviction. Every COUNT-window transition is charged
and may contribute at most `floor(maxInt(i64) / intermediate_states_limit)`
non-NULL values. UNWIND compression checks the product of incoming multiplicity
and domain size without narrowing or overflowing; replay packets validate each
COUNT state against the same bound. A larger or failed domain retries an unpublished read-only
fold without group eviction, preserving exact symbolic COUNT overflow and error
ordering. The failed attempt releases its window arenas and caches; work remains
charged and no stage or result is published before retry. COUNT argument failures also
retry with deferred aggregates, so later middle-stage errors retain precedence
over failures in RETURN arguments for selected, rejected and LIMIT 0 groups. Folds with downstream
observers retain ordinary contribution execution. Physical work counts alone do
not certify logical cardinality. This bound covers repeated and evicted keys
without retaining per-key counts outside the bounded window.

Unpublished read-only folds with no downstream observer also admit key-ranked
MIN/MAX, SUM and AVG windows. Every selected or rejected argument is evaluated.
MIN/MAX certify primitive successful values; numeric reducers certify numeric
types and finite bounds. Integer SUM bounds each weighted contribution by
`floor(maxInt(i64) / intermediate_states_limit)` so forgotten groups cannot hide
prefix overflow. Floating SUM reserves finite headroom across all charged
transitions, and AVG bounds its intermediate mean updates. DISTINCT memberships
belong only to retained group arenas. Failed certificates retry ordinary deferred
reduction before publication, preserving invalid types, structural values,
overflow and later producer errors. Complete reducer packets may be reused only
after checking every argument and bounding each packet's absolute SUM
contributions by their peak and complete emitted cardinality, including compressed
ranges, immutable lists and expanding child bags. Cancellation cannot conceal an
overflowing prefix when keys repeat or are evicted; unsafe packet bounds retry
the ordinary symbolic reduction before publication. Unvalidated packets cannot bypass these checks; effectful
or downstream folds retain their ordinary execution paths.

Monotone arithmetic group keys have an exact rank interval for key-ranked
windows. A read-only single-seed domain with a preserved injective integer
component and a complete independent suffix can allocate reducers only for its
best SKIP+LIMIT keys. Grouping tuples may include further checked affine or
primitive constant components. The first varying ORDER BY component determines
rank direction; all-constant ordering retains source encounter order for ties.
Independent zero-or-one-row CALLs compose with the same suffix packet proof,
including nested calls and empty-input reducers. Checked nonzero integer affine
keys share this rank certificate, with negative slopes reversing rank direction.
Every intermediate key expression must be total over its reached domain; a tuple
with no injective component or
unknown forms declines.
The suffix packet first certifies successful argument evaluation, all
consumer bounds and all producer stages. Selected keys retain source encounter
order; omitted logical rows still contribute to stage counters. Correlated
expressions, joins and uncertified suffixes keep ordinary execution. Checked
projections, DISTINCT and filters in the row-local prefix share the ordered
domain proof. Unit prefixes may include singleton UNWINDs and pure singleton
CALLs; a general outer fanout still declines. Multiple segments use a charged
integer rank boundary, then visit selected keys in original encounter order.
Failed preflight disables its memo instead of
repeating the same failed probe for each key. Unsafe packet cardinalities
request the ordinary no-eviction retry. LIMIT 0 still validates the suffix.

Pure CALL bodies containing only projections and nested singleton CALLs preserve
at most one unit output from one unit input, including filters, local DISTINCT,
windows and global reducers that restore an empty input. That structural
cardinality proof permits COUNT group windows through these bodies. Expanding,
graph-reading and effectful bodies retain their existing execution paths.

Ordinary REDUCE expressions share the collection fold's bounded accumulator
ownership and per-transition scratch lifetime. Each ownership transfer preserves
immutable DAG sharing and transfers root structural metadata into an accumulator
cache that expires with that allocation. Obsolete accumulator values, metadata
and temporary row environments are released after each transition. Closed,
repeatable scalar subexpressions are identified by invocation-local plan IDs and
cached in independently owned statement storage on first reached evaluation.
String analysis/conversion caches also follow proven immutable input allocations
through aliases; dynamic strings remain in recyclable scopes. Folds borrow
already immutable results instead of cloning them on every transition. These
immutable caches survive recycled fold/window/traversal scopes without retaining
transient input addresses; errors remain lazy and prepared executions
never share parameter-dependent values. Writes confine invariant storage to one fold invocation, so entity-dependent
values never cross mutation stages.

Direct HEAD/LAST, SIZE, index and slice consumers of RANGE use a checked arithmetic
sequence descriptor. Range arguments evaluate once in their original order;
NULLs, noninteger arguments, zero steps, descending and empty ranges preserve
normal semantics. Point/cardinality consumers allocate no element list. Slices
retain and charge only selected elements, and ordinary range materialization is
still governed by the work and allocation budgets. Cardinalities outside signed
integer output range report numeric overflow; endpoint access can handle the full
signed integer domain without constructing it.

Bounded pattern-comprehension windows and endpoint-local cardinalities feed
projection sinks directly from the MATCH pipeline. Cardinalities retain exact
weighted endpoint bags; windows retain the ordinary collection ordering proof
and independently owned front/back runs. All matching predicates and required
projections still evaluate. Scalar projection failures are deferred until MATCH
finishes so later matching failures retain precedence; resource and cancellation
failures propagate immediately. Symbolic/path-observing cardinalities and
unsupported nested scopes keep the existing weighted dependency/materialization
fallbacks rather than expanding compressed route bags.

Invocation-local integer parameter bounds specialize front/back retention without
replacing the original bound expressions. Invalid, NULL, missing and unproved
bounds retain their original evaluation behavior. Private annotations and stable
invariant-expression IDs never mutate a prepared/public AST or cross executions.
Full escapes, unknown/unbounded windows, scope barriers and
correlated or effectful consumers conservatively retain the complete list.

Read execution builds collision-checked membership indexes for proved invariant
IN lists at their first reached evaluation site. Immutable parameter lists also
retain their metadata across row scopes and follow transparent aliases by owned
allocation identity. Temporary row/AST addresses never become statement cache
keys. Equality-only keys preserve shared value DAGs and entity identity; wide
strings use reclaimable byte pools. Null-free domains use hash probes, while
nullable structures retain tri-valued comparisons and unknown-result fallback.
Left-hand expressions still evaluate before the RHS; writes and unproved dynamic
inputs retain ordinary evaluation. Index construction and probes share statement
work, allocation and cancellation accounting.

Compatible repeatable DISTINCT arguments also share a collision-checked value
store across different reducers (for example COUNT and COLLECT). Ordinary row
emissions resolve each cohort ordinal once; synthetic statistic bins and moment
inputs bypass that emission cache. Each reducer
has its own compact ordinal membership bitmap, so independently activated
statistic cohorts preserve their first contribution and COLLECT order. Compressed
integer supports remain per consumer and import only that consumer's observed
ordinary values. Materialized scopes cache distinct argument sequences lazily,
without evaluating an unused CASE branch or merging effectful inputs.
Membership that has no full-payload consumer retains compact entity equality
keys: table plus document key for nodes, and physical identity plus index for
relationships. This pruning also applies inside containers and paths. Bounded
collection consumers own their observed prefix payloads separately; an unbounded
COLLECT cohort still shares full values and preserves each consumer's first
representation. Small scalar keys stay in their metadata arena. Large string payloads use
bounded byte slabs with exact allocations for oversized values, preventing
allocator-dependent metadata arena growth from retaining extra payload space.

A scalar COUNT projection ordered directly by grouping keys can retain just
SKIP + LIMIT groups while consuming the input. Its stable max heap owns each
selected group's arena and recycles hash collision links on eviction. COUNT
DISTINCT keeps membership only for selected groups and releases it on eviction.
Rejected rows still evaluate arguments and validate their hash/depth domains.
Recyclable membership values use emission-local metadata; immutable wide arguments
reuse their owner's analysis. Each charged contribution must certify a
logical count no greater than maxInt(i64) divided by the work budget, ruling out
COUNT overflow even for discarded groups. Weighted producers and cached reducer
packets carry exact bounds; an unproved weight retries general aggregation before
publishing results. Argument failures also retry deferred reduction so later
middle-stage errors retain precedence. Aggregate-ranked order and post-group
predicates retain general aggregation. Heap comparison metadata has scratch lifetime so
recycled group allocations cannot enter the statement's identity caches.

Unproved length predicates, length grouping keys, non-extremum DISTINCT
arguments and other distribution-dependent expressions retain exact hop states. Cyclic
trail histories retain ordinary enumeration when uniqueness cannot be proved;
repeatable bounded walks retain the finite hop recurrence.

Intermediate ordered projections negotiate a weighted stage plan. DISTINCT
collapses bindings before sorting and handing the ordered bag to downstream
stages, without materializing the route multiplicity or removing observable
sort keys, windows or errors.
Unordered DISTINCT windows and intermediate WITH DISTINCT stages use the same
demand-triggered proof when their row goal remains unfilled. Each projection
evaluates before checking its already retained keys; probes only read these sets.
Already retained keys are inadmissible; negative suffix
entries remain reusable as keys grow, while positive entries carry a generation
and are reconsidered after DISTINCT or OPTIONAL match-state changes. The proof
does not replace DFS ordering or evaluate a projection error before DFS reaches it.
Repeated product-state work can trigger a bounded DAG depth proof. When a
segment's lower bound is satisfied and its remaining upper bound covers the
proved suffix depth, hop states share one reachability entry per node and
segment. Multiple segments prove their depths incrementally in postorder from
complete captured domains, preserving segment boundaries and residuals without
opening additional readers. Cycles and unknown adjacency at the read horizon
retain exact hop states; an unloaded boundary never becomes an empty suffix.
For unbounded simple paths, a fully captured cyclic domain can use distinct
relationship identities as a finite trail bound, or node identities as a
node-simple bound. Shared domains count physical index/key identities once.
Bounds must prove that the statement's path-depth limit cannot truncate a valid
path; repeatable cyclic walks retain ordinary depth-limited traversal.
Returning CALL bodies inherit proven row goals through unfiltered projections,
capped by their own existing limits; child SKIP and aggregate/sort barriers remain
inside the child scope. An outer
filter, DISTINCT, sort, aggregation or expanding stage prevents this propagation.
Pure returning CALL stages and their finite-window continuations share bounded
child-bag memoization by captured bindings and child row goal. Streaming bodies
retain lazy production; scoped child reducers/windows reuse complete memoized
bags across parent continuations. Keys preserve binding representations and
implicit prebound MATCH aliases. Cache admission can decline
without failing the query; writes, procedures, volatile functions and unit CALL
remain barriers. Cached children own their payloads and preserve parent weights.
A bounded tee owns a streaming child's intrinsic projected rows and relative
weights before merging parent bindings. Only a completed child can publish
its bag; an early outer LIMIT, speculative parent pruning, or tee quota decline
prevents admission. Parent weights and filters remain outside the cached bag.
A completed empty child needs no retained routes. Speculative child rows
conservatively prevent empty-child admission when a parent filter rejects them,
so unrelated parent bindings cannot poison the memo.
Pattern dependency analysis follows query WITH scopes and lexical shadowing;
only captured outer bindings stay live. Unobserved path aliases therefore do
not force route production merely because a later expression contains a pattern.
SIZE over a pure pattern comprehension requests cardinality directly and evaluates
its projection once per complete binding, preserving NULL entries and eager
projection errors without constructing a list of repeated elements. Pure
pattern comprehensions, SIZE comprehensions and EXISTS expressions share a
bounded statement-local memo keyed by expression identity and captured bindings.
It owns cached payloads independently of route scratch, keeps representation and
lexical scope observable, and declines admission without failing the query.
Statement-wide field liveness includes locally bound comprehension entities.
Writes and procedure calls remain execution barriers. Expression evaluation
isolates nested profiling buffers and weighted continuations from retained execution state.
Each subsequent match negotiates a seed-aware weighted child producer. Binding
liveness follows WITH scopes, including wildcard observation, match constraints
and eager expression dependencies. Endpoint-only children retain compressed
counts and multiply them by the parent contribution; observing route or interior
identity keeps exact production. Presence reductions do not require exact path
counts, COUNT retains its overflow sentinel, and numeric folds require exact
weights. Demand analysis visits aggregate calls inside scalar expressions:
MIN/MAX and DISTINCT aggregates require only presence, COUNT needs capped counts,
and non-DISTINCT SUM/AVG/COLLECT need exact multiplicity. Proof-only constant
folding accepts bound parameters and constant arithmetic in finite row goals
without moving invalid-goal errors ahead of their projection. Bounded path counts
saturate at maxInt(u64), strictly above every legal signed SKIP+LIMIT sum, so even
large-offset windows retain sufficient prefix counts. Unordered finite windows
request prefix counts when their proven row goal exceeds remaining route-work
capacity; small goals keep demand-driven first-route production. Finite
consumers clip prefix counts to their requested window. Exact native and SQL
producers use adaptive counts for
frontier additions, branch factors and products, so exceeding u64 does not force
route enumeration. Counts remain inline u64 when possible and promote to
immutable, arena-owned arbitrary-precision counts on overflow. Retained windows
own any promoted producer counts after the producer's scratch arena ends. Unbounded
projections retain those counts and SKIP subtracts exactly; finite windows clip
before any row expansion. COUNT and integer SUM check their result ranges,
including zero sums over promoted counts. Floating SUM uses a wide intermediate,
and AVG normalizes count magnitudes before computing weight ratios, so even
products beyond binary128's exponent range do not create spurious numeric errors.
SQL pages retain compact u64 weights for ordinary consumers and carry promoted
exact counts when needed. Projections, unions, joins and materialized replay
preserve those counts; bounded windows clip before expansion. SUM/AVG promote
reducer state only when compact counts cease to suffice. Exact NUMERIC totals
and counts survive versioned partial interchange and spill, including cancellation
and division of intermediates beyond the public NUMERIC exponent range. Final
values still obey their declared SQL result ranges. Trail scope and
optional null extension remain clause-local. Expansion is reserved for external
rows and consumers such as non-DISTINCT collect that need individual elements.
Saturated path counts remain sufficient for a finite window. Other ordering, aggregation,
writes and non-repeatable expressions remain pipeline barriers.
Endpoint-only RETURN DISTINCT also reduces the path product states directly,
including saturated counts, while preserving the existing uniqueness proofs and
exact cyclic fallback. Unordered DISTINCT with LIMIT retains demand-driven route
production, so a small row goal need not capture a complete closure. DISTINCT
ordering accepts expressions composed of projected
values, including an unaliased property such as `b.score`. Structural references
read the computed projection value even when an alias shadows the source entity;
discarded properties remain inaccessible.
Projection aliases in ORDER BY resolve in the projection scope before dependency
checks. Single fixed-hop reductions retain page-at-a-time streaming, since they
have no shared prefixes to compress. Nullable arguments ignore NULL without discarding their groups. DISTINCT
tracks argument values independently of route multiplicities. Integer weighted
sums use wide intermediates, floating sums preserve finite cancellation, and AVG
combines weighted means. Saturated path counts cannot serve as approximate
numeric weights: exact SUM/AVG producers promote their counts instead of using
an overflow sentinel or enumerating repeated routes. Expressions depending on intermediate entities, paths or
unsupported consumers retain exact route production. The optimizer never substitutes shortest-path semantics for ALL.


SQL pull cursors retain the selective interior anchor, traverse a reversed prefix,
restore source path order and resume the suffix with shared uniqueness and deferred
constraints. Suspended frames pin bounded adjacency and endpoint-cache pages;
endpoint hydration uses aligned, deduplicated native/owner batches, including NULL
for missing or hidden nodes. The exact-count hook retains the original logical
pattern independently of this cursor plan. Outer SQL demand can stop traversal
without materializing either half of the path.

Lazy write overlays maintain transaction-local property candidate indexes for
scalar equality and membership predicates. They use Cypher's typed numeric hashes,
retain collision residuals, and update postings for normalized creations, SET,
REMOVE, SQL postimages and deletion. MERGE probes staged candidates without
rescanning or sorting every previous insertion. Unindexed/range scans retain the
ordered staged phase and opaque native continuation tokens. Bounded scalar
constant folding shares checked runtime arithmetic when constructing seeks;
failed folds remain residuals so overflow and division errors keep their runtime
semantics.

Fixed-edge matches with two bound endpoints seek the complete encoded
(source, index, type, peer) key prefix in native outgoing or incoming storage.
Parallel IDs, contributor selection, TTL, ownership, authorization and physical
continuation budgets retain the ordinary adjacency rules. Counts and pull
queries share this capability through RPC and distributed source owners.
Transaction overlays merge captured native pairs with transaction-local pair
postings, lazy scalar-property indexes and collision residuals. Native
backends without pair seeks populate a retained adjacency domain once under the
same statement cut and physical/work quotas. Later MERGE probes reuse that index;
new relationships and postimage changes maintain it incrementally. Pair indexes
are allocated only when requested, preserve parallel logical IDs, and recheck
producer authority and tombstones. Candidate pages borrow dense postings rather
than copying or sorting a complete match set for every pull.
Mutation normalization and postimage projection decode RPC scaffolding in
reclaimable call-local scratch and retain only canonical entities for the
statement. Projected document hits use native point lookups rather than opening
a scan session per hit.

Document-profile reads decode and authorize full records in reclaimable scratch,
then retain only requested fields before owner serialization. Caller arenas are
peeled before compiled owner boundaries; temporary decoding can release its
quota even when library-local arena vtables have different addresses. Owners
whose native readers enforce full-record policies and whose authority hooks
check identity independently certify storage projection; those document reads
retain query and label-predicate fields before row materialization. Custom
property-inspecting policies keep full decoding. Full-entity consumers retain
full records. The known `antfly.search` procedure participates in
entity lineage and property liveness, including renamed node yields, score-only
results and later MATCH/WITH consumers. Unknown procedures retain conservative
full-entity behavior. Search membership and row-policy checks still precede top-k
selection and property pruning.

`antfly.search` no longer scans the table to enumerate allowed IDs. Unrestricted
table labels without row policies use native indexed search directly. Restricted
membership and active row policies are checked against the retained cut during native candidate selection, before
top-k and pagination, including text fallback and match-all collectors. A bounded
membership cache reuses checks across native phases. Candidate checks consume
the remaining statement anchor/work budgets; there is no fixed 65,536-document
pre-enumeration ceiling. Membership is never implemented by dropping disallowed
hits from an already-selected top-k result.

`tests/test_cypher_performance.py` qualifies both Lite and directory storage:
point reads/writes with a one-anchor ceiling, shared-label counts, bounded-memory
terminal operators, SQL graph composition, shortest paths on a dense acyclic
graph, and lazy incident-edge deletion. Weighted regression cases include
indexed and unindexed root/endpoint predicates, computed derived-table and CTE
projections, SQL ordering/UNION ALL/persisted dimension joins, spillable full
outer joins, NULL-aware membership, shared materialized replay, saturated SQL top-N windows,
selective indexed ranges,
connected multi-pattern and multi-clause components, factorized branches,
independent 100-hop SQL
endpoint reductions, floating DISTINCT SUM/AVG, and finite ordered windows over saturated path bags. Core oracles compare
compressed execution with exact cyclic and acyclic trails, including OPTIONAL
null extension, partial bag pagination, eager Boolean errors, pattern-property
errors, and heterogeneous outer bindings. Core regressions cover computed top-k
values, stable ties, distinct label predicates, batched endpoint multiplicity,
and Lite fork/cursor lifetime after parent close. Native search regressions
exercise predicate labels alongside row policies and a selective posting in a
66,000-document table with a four-anchor budget.


### Finite key domains and terminal ranked packets

Document-key equality, finite `IN` domains, and unions of finite key predicates
share point access planning. Domains deduplicate string keys before lookup;
non-string members cannot match document keys, and residual Cypher predicates
still run after authorized reads and graph membership checks. An unrestricted
`OR` arm keeps ordinary scanning. Backends with aligned `get_many` support read
at most 128 identities per batch, within the remaining anchor budget; singleton
reads retain the normal lookup/cache path. The same backend visibility and policy
contract applies to both forms.

Range windows distinguish ordered sequences from ranked bags. A terminal
COUNT/MIN/MAX reduction can consume rank-selected source runs directly, retaining
duplicates without materializing the sorted page. SUM additionally proves one
sign across the selected domain so reordering cannot hide intermediate integer
overflow. AVG, collection order, grouped output windows, and later pagination
keep the ordered path.

Periodic cohorts partition lexicographic ordering by constant prefixes using a
stable, charged merge sort, then select affine ranks within each prefix bucket.
Global SKIP/LIMIT applies once across these buckets. Packet ordinals include
stable ties from earlier source runs. A packet's first and last stable ranks
certify whether its contributions are contiguous; bounded collection endpoints
may consume constant or checked affine weighted packets only when their merge
preserves encounter order. Failed proofs retain ordinary execution, including eager errors and
floating-point recurrence semantics.

### Qualified identities, certified index order, and affine rank selection

`elementId(n)` equality and finite `IN` predicates share point-domain planning
with document keys. The planner decodes node identities into table-qualified
candidates, intersects them with graph label tables, and deduplicates keys per
table before authorized reads. Invalid identities cannot match. The original
string predicate remains authoritative: alternate JSON formatting does not make
an otherwise unequal element ID compare equal. Writes use the same lookup path.

A readonly single-node match with a finite ordered projection can request a
backend certificate for property order. The native adapter uses a ready index
from the statement's pinned schema generation when its remaining index keys
match the requested properties, directions, null placement, and stable primary-key
ordering. Leading integer, binary-string, and boolean keys may be omitted from
the requested order only when compatible equality conditions are enforced for
every omitted key. A failed coercion or omitted hint declines that proof.
Supported key types are integer, string with binary collation, and boolean.
Partial indexes, expression keys, unproved extra keys, incompatible collations,
and types whose storage order differs from Cypher decline the certificate.
Authorization hooks must explicitly certify that visible scalar values remain
unchanged. Active write overlays decline ordered reads because staged values
may move between index positions.

Residual predicates and label membership run on every authorized candidate;
scanning stops only after SKIP+LIMIT qualifying rows. Fallible projections and
predicates retain eager execution so a window cannot hide an error. Candidate
reads, including rejected rows, consume the anchor budget. Compatible typed conjunctions establish native seek bounds on the ordered
index. Stronger equality access paths on another index are considered before
falling back to the native selective planner and a bounded Cypher top-k.
A single compatible IN domain can form a deduplicated disjoint union of bounded
ordered equality streams; NULL members never open a stream. Mixed operands,
floating domains, multiple independent IN domains, domains above 256 input members
or eight distinct non-NULL partitions, and insufficient initial lookahead budgets
retain ordinary execution.
Unsupported storage operands remain authoritative runtime residuals.

Distributed reads negotiate the same capability over authenticated RPC. For
multiple owners, a metadata-only probe establishes that every owner can provide
the requested order before any records are read. A heap merges one authorized
head per owner, preserving ordinary owner encounter order on equal keys. Opaque
continuations retain owner cursors rather than documents. Speculative heads and
heads reread across page boundaries count against the global anchor budget;
comparison work is charged. Single-owner requests forward bounded pages without
per-record RPC overhead. Catalog changes, revocation, and malformed continuation
responses fail rather than silently switching ordering after results begin.

Periodic range ordering can select ranks across cohorts with different checked
affine keys. Constant prefixes partition the sort; arithmetic rank counts select
source runs without walking skipped rows. Boundary ties compare the complete
ORDER BY tuple and original source position, including repeated source ranges.
Small windows render only their selected points, preserving AVG recurrence.
Larger packets carry stable rank and contiguity proofs for compatible reducers.
Immutable rank provenance survives subsequent filters and small windows, so
interleaved members retain their exact encounter positions;
unsupported or overflowing proofs retain ordinary execution.

Bounded collection endpoints can consume checked affine packets directly when
their encounter order is certified. The accumulator advances the full logical
count while retaining only the requested prefix and suffix. This supports
`head`, `last`, and finite collection slices over large ordered windows without
materializing the collection. DISTINCT, fallible extraction, and interleaved
packets still require their existing stronger proofs or ordinary execution.

Public boundary regressions run on Lite and Directory storage. On a
13,000-document table, qualified identity equality examines one anchor (26
states), a two-identity domain examines two anchors (60 states), and indexed
`ORDER BY n.score DESC LIMIT 5` examines five anchors (144 states). The
1,300,000-value ordered affine collection endpoint and heterogeneous affine
rank cases consume 388 and 722 states respectively, with less than 150 KiB
reported statement memory. Tests enforce a 2,000-state, 1 MiB ceiling for
these range cases rather than relying on elapsed-time thresholds.


### Bounded ordered execution across graph and SQL boundaries

Ordered native pages expose an optional exact continuation after each emitted
item. A zero-row capability probe advertises this support with an empty cursor
array. Authenticated RPC carries the array alongside the node page; legacy
providers retain one-item reads. The final item resumes after the full examined
page, including its rejected tail, while intermediate items resume after that
specific row. Schema/index identity and the statement cut fence every token.

The distributed heap retains one head per owner backed by bounded owner pages.
It primes individual heads before speculative refills, caps refills by remaining
output demand, and uses small reads when the remaining anchor budget is tight.
All speculative and repeated records count against the global scan ceiling.
Continuations advance only for consumed items, so unused prefetched nodes need
not be serialized or retained across calls. Buffers use the execution allocator
and remain bounded by coordinator demand and owner count.
The two-owner regression returns 1,000 rows in 34 RPCs instead of 1,010,
examining 1,016 records including lookahead. With alternating records hidden,
it returns all 512 visible rows in 23 RPCs and accounts for all 1,024 records.

SQL GRAPH_TABLE requests negotiate ordering before pulling rows. Direct typed
scalar projection columns map to the shared Cypher order proof, including exact
null placement. Both embedded and distributed wrappers preserve the resulting
order certificate. The relational source adapter maps a direct graph scan's
internal SQL columns back to graph projection columns before statement capture,
then forwards only the physical cursor's attested order. Existing total-column
lineage carries that proof through transparent derived projections and filters.
Joins, ordered/cardinality-changing derived relations, fallible projections,
and previously captured mutation cursors retain their normal paths.
A SQL executor regression requires five physical rows for an attested top-five
query over 13,000 rows and checks the full-sort fallback when proof is declined.
SQL retains its residuals and applies its normal row-goal
rules; a graph cursor never truncates the relation using an advisory row goal.
Fallible graph projections, incompatible ordering, staged postimages, and
synthetic source ordinals decline the certificate. Ordered replay tokens use a
separate ordinal namespace and must re-establish the same order on resume;
legacy unordered tokens retain their original interpretation.

A later SKIP/LIMIT over a ranked periodic domain slices inherited encounter
order directly. Contiguous packets require a linear metadata pass. Interleaved
packets use monotone rank searches through immutable provenance, including
holes left by filters. Neither path walks the skipped rows. Small selected
windows render only their surviving points, preserving floating AVG recurrence
and collection order. The 1,300,000-value g,x-ranked window followed by
SKIP 500000 LIMIT 5 is covered by a 2,000-state, 1 MiB public regression.

On the 13,000-document public fixture, the selective ordered range examines
five anchors in 213 states, and ordered equality examines one anchor in 45
states on both storage backends. Warm SQL GRAPH_TABLE top-five calls measured
about 0.3 ms on Lite and 4 ms on Directory, versus roughly 73 ms and 33 ms
before order negotiation reached the relational source adapter. These timings
are illustrative; regressions enforce physical work and resource ceilings.

### Order lineage and disjoint ordered domains

Cypher WITH stages carry direct node/property lineage through total one-to-one
projections, including renaming and wildcard forwarding. Every expression and
filter in the crossed stages must be total. Shadowing replaces the old lineage;
computed scalar values never become entity receivers. Intermediate ordering,
DISTINCT, SKIP/LIMIT, aggregation, expansion, and fallible expressions remain
barriers. The original stage executor still evaluates projections and residuals;
lineage only supplies a physical source order certificate.

SQL derives the same property order through its existing identity-column proof,
which checks every intervening projection and predicate, then follows the
attestation back from the physical graph cursor. Demand propagates through those
certified filter frames as a page-prefetch hint rather than a total read ceiling.
The executor's full-sort fallback remains available when any provider declines.

Native IN partitions share the coordinator's budgeted ordered heap, pinned
schema/authority checks, and exact continuation protocol. All primed heads,
rejected rows, repeated reads, and speculative refills consume the global anchor
budget. Local partition ties compare primary identity instead of IN-list order.
Per-item merged cursors let an enclosing authenticated coordinator retain bounded
RPC pages; continuations contain positions rather than cached documents.

Ordered IN planning admits at most eight distinct scalar partitions. A public
13,000-row/256-value/1,000-result regression preserves the ordinary sparse-seek
plan under the default one-million-state budget. This prevents per-row merged
continuation metadata from scaling with a large partition vector. The ordinary
planner still evaluates every IN member and preserves the complete result.

Public top-five regressions on 13,000 documents exercise both Lite and Directory.
The equality-fixed composite prefix reads five anchors and spends 250 states,
versus 6,500 anchors previously. The two-value IN union reads six anchors and
spends 353 states, versus 13,000 anchors previously. Transparent WITH variants
read five anchors with 139–164 states; SQL derived wrappers inherit the same
five-row physical source certificate as direct GRAPH_TABLE queries. Tie-order
checks compare complete node rows against ordinary execution across merge-page
boundaries, and coercion failures retain authoritative predicates.

### Predicate lineage, constant keys, and ordered DISTINCT

The single-node source plan carries physical predicate hints independently of
its final order certificate. Total transparent WITH stages forward entity and
property aliases, invocation constants, equality/range predicates, and bounded
IN or same-property OR domains. Every original predicate remains in its lexical
stage. Unbounded reads and aggregate consumers can use these restrictions too;
fallible final projections still execute over all surviving rows. An unsafe
crossed stage, shadowed receiver, or cardinality barrier declines the proof.

Order negotiation removes invocation constants, equality-fixed scalar keys,
and repeated property keys before matching an index. If no effective sort key
remains, the required order is the ordinary primary encounter order. The native
adapter can attest that order with an empty physical order request when every
key of a ready relational index has an enforced typed equality. This preserves
selective seeks even when the constant key's declared direction differs from
the index direction. Otherwise execution retains the ordinary primary scan.
The existing authenticated RPC and owner merge carry the same capability;
continuations never switch between primary and index cursor formats.

Ordered DISTINCT has two streaming proofs. Projecting the source entity retains
its unique identity in a one-row-per-node pipeline, so no membership set is
needed. Scalar projections can deduplicate adjacent rows when every varying
projected key is ordered and every varying order key is projected. Otherwise,
when all varying order keys are projected, a hash index tracks only the rows in
the retained SKIP+LIMIT heap. Hash entries follow stable heap slots through swaps,
replacement and eviction, and hash collisions use full Cypher row equality.
SKIP and LIMIT count distinct rows. Both proofs require the physical provider's attestation before
selecting the streaming executor. An unavailable certificate, fallible expression,
or unprojected varying sort key retains ordinary grouping and its original
representatives. NULLs and ties use the shared Cypher equality/order rules.

On the 13,000-document public comparison, a selective WITH predicate uses one
anchor and 58–60 states instead of 13,000 anchors and 312,030–390,022 states.
Top-five DISTINCT entity/scalar reads use five anchors and 166–186 states;
previously they read every node. Literal and equality-fixed sort-key cases use
five anchors and 189–333 states, including the query that previously exhausted
the default state budget. A top-five scalar DISTINCT with 128 repeated leading
values reads 132 anchors and uses 4,262 states, retaining only one adjacent key.
The regression suite also exercises NULL runs, both index directions, aliases,
unbounded and aggregate consumers, residual predicates, and authenticated
multi-owner primary-order reads.


### Partially ordered DISTINCT, identity-preserving stages, and index prefixes

Single-node source lineage propagates entity uniqueness through total projection
stages. WITH DISTINCT is redundant when the projected row still contains that
source entity, including renamed entities and wildcard forwarding. Such stages
retain their normal expression evaluation and predicates but need no membership
set. Dropping the entity, expanding rows, grouping, or crossing a cardinality or
fallible-expression barrier retains ordinary DISTINCT execution. Unbounded and
aggregate consumers share the same uniqueness proof.

Native relational indexes may supply the requested ordered key prefix after any
enforced equality-fixed leading keys. A complete index-key match certifies stable
primary ties. Extra trailing keys instead return a coarse ordering certificate;
opaque continuations retain that certificate mode and schema generation. Exact
certificates are considered first. The private owner RPC and coordinator merge
preserve mode and primary encounter provenance; local IN partitions share their
physical owner's encounter domain.

A coarse source consumes the complete boundary key group once. Its finite heap
ranks ties by catalog owner encounter order and primary document key, including
when WITH has removed or renamed the source entity. It may stop only at a strictly
worse key or end of source. Only SKIP+LIMIT rows are retained, independent of the
size of a tied group. Partial-order DISTINCT uses the same bounded heap and hash
index: a duplicate with an earlier primary representative replaces its retained
row, and evicted entries leave the membership index immediately. No documents or
large tied groups are serialized into continuations, and no group replay is
needed. Refills grow to bounded native pages when a tie or duplicate run makes
progress stall; every lookahead and speculative record is charged.

Consumers that require a fully stable, unbounded streaming cursor decline a
coarse-only metadata probe. SQL GRAPH_TABLE can separately negotiate a licensed
finite prefix, described below; an advisory row goal alone cannot truncate its
relation. Neither capability advertises a stronger order than its executor supplies.
Regression coverage includes partial DISTINCT with duplicates, redundant identity
stages, boundary ties ordered differently by trailing index keys, a whole-table
tie under a small memory ceiling, and two authenticated owners whose catalog
encounter order differs from global key order.

Native index pages establish exact range exhaustion before returning a
budget-limited final page. A key-only continuation check distinguishes EOF from
an unconsumed next candidate without hydrating another row or changing the saved
continuation. Thus a whole-table tied group can complete at its exact candidate
ceiling; it does not require an extra record allowance for an empty EOF page.
Byte-limited unconsumed rows retain their previous continuation.

On both public embedded backends with 13,000 documents, partial-order DISTINCT
now reads five anchors and uses 215 states, while a redundant WITH DISTINCT reads
five anchors and uses 164 states. The previous paths read all 13,000 anchors.
A leading duplicate run completes in 77 anchors and 3,813 states. A composite
prefix with a 64-row boundary tie completes in 68 anchors; its full-node query
uses 3,521 states. These bounds include charged lookahead records.

### Ordered offsets, reverse traversal, extrema, and distinct-key seeks

An exact ordered source retires qualifying SKIP rows as they pass through the
normal projection pipeline. Its logical consumed count still includes that
prefix for demand and termination, but retained picks contain only LIMIT rows.
This removes the offset-sized heap and its comparisons, clones, and final sort.
Coarse order retains the boundary heap because primary encounter ties still need
refinement. Partial DISTINCT retains its membership proof and selected window.

Native ordered reads consider exact forward order, coarse forward prefixes, then
reverse traversal. Every varying requested direction and NULL placement must
match the corresponding physical direction after reversal; omitted leading keys
still require enforced equality. Reverse iteration is bounded on both sides and
uses an exclusive continuation. Both native and row cursors fence direction as
well as index/schema identity. Reverse traversal certifies coarse ties because
physical reversal also reverses primary keys; the normal bounded refinement
restores catalog owner and primary encounter order.

A scalar MIN/MAX projection negotiates all of its observers before reading
candidates. Total single-source lineage can carry through the same safe WITH
stages as ordered windows. Each supported observer executes an authorized,
non-NULL ordered window; identical aggregate expressions share one result.
Unsupported observers or fallible expressions retain the ordinary aggregate
fold. Typed NULL tuple bounds complement the authoritative runtime non-NULL
predicate. Empty and all-NULL inputs still produce one NULL aggregate row.
Internal window, multiplicity, and profile state stays scoped to each observer;
the outer physical plan reports index_observers. Subsequent weighted stages receive
the ordinary one-row aggregate result.

For an adjacent scalar DISTINCT over one or more varying physical keys, a
qualifying row confirms that its entire key tuple projects identically. Refills
may then exclude that group using an exclusive typed tuple bound, including NULL
components. Every varying field carries its component of the same boundary;
constant and equality-fixed leading fields are reconstructed from enforced
filters. Mixed directions and reverse traversal use physical tuple comparison.
A rejected candidate never grants this hint. The native reader combines the group boundary
with equality prefixes, original residual bounds, immutable schema generation,
retained authorization, and any non-NULL bound. Opaque owner continuations remain
positions rather than documents. The private RPC encodes a present NULL boundary
separately from an absent hint. Providers that do not support key-group seeks may
ignore the hint and retain normal monotone iteration and duplicate suppression.

The extrema rewrite reserves its additional filter frame before probing. At a
caller's stage/recursion ceiling it declines early, preserving the legal original
aggregate fold and its result. Regression cases exercise both a zero-stage
window allowance and an identity pipeline exactly at that allowance.

On both 13,000-document public backends, SKIP 12000 LIMIT 5 now completes in
180,217 states and 852,376 bytes; the former heap exceeded the default state
ceiling and a 2 MiB memory ceiling. Opposite-direction top-five ordering examines
six anchors and uses 193 states, including tie-boundary lookahead, instead of
13,000 anchors and 247,049 states. A matching-direction scalar MIN or MAX examines
one anchor and uses 40 states instead of 13,000 anchors and 169,001 states. The
12,000-row duplicate-prefix scalar DISTINCT examines nine anchors in 273 states,
down from 12,045 anchors and 384,234 states. Tests also enforce bounded reads for
Unicode strings, booleans, leading/trailing NULL runs, equality-fixed prefixes,
coarse reverse ties, filtered candidates, and repeated extremum observers.


### Constant domains, group-directed observers, and licensed SQL prefixes

An adjacent DISTINCT whose projected keys are all constant or fixed by
source equality needs only an existence witness. It stops after the first
qualifying row, including when LIMIT exceeds one or SKIP removes the sole
result. Empty inputs remain empty. This singleton proof also applies without
ORDER BY. Property results retain the ordinary filtered source's first
representative; literal and proved NULL-result domains may push later total
filters across that source without depending on encounter order. MIN/MAX over a constant or
equality-fixed argument uses the same existence path. Logical singleton status is
independent of the physical keys retained for an indexed access path, so the last
allowed witness does not require a subsequent EOF read. A closed NULL argument
returns NULL without reading candidates after the complete source pipeline passes
the totality proof.
Fallible predicates, projections, and stages retain their normal execution.

Ordered grouped COUNT/MIN/MAX with a finite window first negotiates an adjacent
group-key proof and every observer's domain. Counts and broad, sparse groups use
one ordered source pass. Each complete group is projected and its reducer state
is retired when the next group arrives; only the current group and selected
output rows remain retained. A complete key order avoids both the candidate heap
and the final result sort, and permits stopping at the first unselected group.
A bounded, charged prefix sample compares observed density with the seek cost of
standalone extrema. Dense extrema domains can instead discover selected keys
with tuple seeks and observe their non-NULL boundaries. The sample is replayed
from retained owned pages and never read or charged a second time. The choice is
advisory: every chosen executor retains its original budgets and semantics.

Per-domain observer invocations own a recyclable planning/evaluation arena; only
result values escape to the statement arena. Invocation memoization resets before
arena reuse. Sharing-aware clone contexts preserve compact value graphs while
taking ownership of those results, and immutable literal/parameter grouping keys
need no equality recheck. This bounds observer metadata by one active group,
including broad result windows.

Unbounded ordered DISTINCT uses the same complete-key cursor as finite windows.
A projected row determined entirely by ordered keys and constants is adjacent
across coarse owner ties too when every owner attests exact primitive key
representations (exact_keys). Legacy providers retain tie refinement and preserve
the primary representative, including mixed numeric representations. The
certified path needs only the previous row and an exclusive
tuple bound, without a global membership set or a final sort. Source order and
collector layout are separate certificates: a partial DISTINCT collector that
retains a selection heap still sorts its retained window. Only append-ordered
picks can elide that final sort. A count-only
consumer implements scalar COUNT(DISTINCT key) over the globally merged domain
stream without retaining output rows. Counts are never added across owners that
may share keys. COUNT(DISTINCT constant) needs an existence witness; COUNT(NULL)
returns zero without source reads after proving the complete source pipeline
total. Unsupported or fallible sources retain the ordinary aggregate fold.

The rewrite carries total, single-node lineage through safe identity and
projection stages. It requires grouping keys to be covered by the requested
order, and requires MIN/MAX and DISTINCT COUNT arguments to have a certified
scalar domain. An ORDER BY reference to an aggregate output alias retains the
original fold, including when that alias shadows a grouping key, a WITH binding,
or the source node. Removing observer outputs must never expose an older binding
to the discovery sort. Unsupported aggregates, HAVING, fallible expressions,
incomplete order, and stage ceilings also retain the original fold. Internal
equality parameters are immutable, invocation-scoped, collision-free with user
bindings, and never
enter the public JSON contract. NULL groups use an internal IS NULL filter rather
than equality with NULL. Source authorization, owner snapshots, budgets, and
runtime predicates remain authoritative. The physical plan reports index_groups or ordered_groups for the selected strategy.

SQL scans distinguish an advisory row_goal from an ordered_window capability.
The planner licenses OFFSET+LIMIT only for a finite, row-preserving graph scan
through identity projections with total outer column/literal outputs. A complete
outer SQL predicate can grant the same license when every arm translates exactly
to typed integer, string, or boolean graph comparisons. The certificate is bound
per invocation and consumed atomically by the graph provider; partial conjunct
hints, unsupported arms, approximate numeric domains, or failed translation grant
no source cap. SQL still evaluates its full residual. Inner selection frames,
joins, aggregates, windows, ordinals, scalar-cardinality observers,
fallible projections, and unbounded consumers cannot license a truncated source.
Transaction/lake overlays clear the capability when they clear physical ordering.

A graph cursor first attempts a full stable-order certificate. If that declines,
an explicitly licensed window can use the shared Cypher bounded-order executor
to refine coarse index ties into an exact prefix. The cursor retains that finite
prefix in its owned arena and pages it normally; it does not expose an exact
full-relation count or accept weighted/cardinality plans. Providers may decline
the capability and return their ordinary complete relation. The enclosing SQL
executor still applies its own OFFSET and LIMIT. Ordinary graph cursors and SQL
requests carrying only row_goal continue to return the complete source relation.

On both public embedded storage backends with 13,000 documents:

| Query shape | Original anchors | Final anchors | Original states | Final states |
| --- | ---: | ---: | ---: | ---: |
| Tuple DISTINCT | 12,045 | 9 | 696,383 | 473 |
| Equality-fixed DISTINCT | 13,000 | 1 | 619,994 | 42 |
| Equality-fixed MIN/MAX | 12,000 | 2 | 480,007 | 150 |
| Literal DISTINCT | 13,000 | 1 | 285,990 | 12 |
| Ordered grouped COUNT | 13,000 | 12 | 221,117 | 369 |
| Ordered grouped MAX | 13,000 | 12 | 312,117 | 631 |

A 3,000-group MAX window completes in 4,440,766 bytes on both backends; its
regression enforces a 16 MiB ceiling. Group observer ownership also preserves
shared containers under a 2 MiB native budget. The SQL cursor tests distinguish
advisory goals from licensed prefixes and exercise coarse ties, ordinary full
relations, OFFSET, derived projections, NULL ordering, and source ordinals.

#### Broad domains and complete predicate certificates

The follow-up public fixtures compare the prior group-seek implementation with
ordered folding and domain cursors on both Lite and Directory storage. The
5,000-group fixture has one document per key; the 13,000-document domain fixture
has three repeated scores. Counts below are statement-level charged work, not
unaccounted owner-local reads.

| Query shape | Prior anchors | Current anchors | Prior states | Current states |
| --- | ---: | ---: | ---: | ---: |
| 5,000 grouped COUNT results | 10,000 | 5,000 | 915,228 | 100,046 |
| 5,000 grouped MAX results | default state budget exceeded | 5,000 | default state budget exceeded | 135,141 |
| Unbounded score DISTINCT | 13,000 | 5 | 312,294 | 136 |
| Scalar COUNT(DISTINCT score) | 13,000 | 5 | 272,974 | 171 |
| COUNT(DISTINCT 7) | 13,000 | 1 | 207,994 | 14 |
| COUNT(NULL) | 13,000 | 0 | 65,004 | 5 |
| DISTINCT score under IS NULL | 13,000 | 1 | 189,597 | 24 |

The grouped MAX fixture retains about 5.5 MiB, including its 5,000 output rows,
and completes with an exact 5,000-anchor allowance. Native sparse-domain and rejected-prefix
regressions also require fewer than 100 transport requests for 5,000 source
records, preventing exclusive seeks from becoming one RPC per sparse key or
rejected row.

For `GRAPH_TABLE` with a fully consumed outer `score >= 0` predicate, ascending
order and LIMIT 5, warmed local medians changed from 128.5 ms to 0.30 ms on Lite
and from 113.5 ms to 3.1 ms on Directory. Timings depend on the environment;
complete predicate transfer and bounded source work are the contract. A partial
predicate, unsupported OR arm, inner selection frame, or fallible output retains
the complete source and normal SQL residual evaluation.


#### Composed observers and streaming group retirement

Global COUNT(DISTINCT), MIN and MAX now negotiate one complete observer plan
before reading data. Every argument still needs the normal total ordered-source
proof; an unsupported observer declines the complete rewrite. Identical observer
expressions reuse one result even when their output aliases differ. Execution
reuses the captured ordering certificates, including cursor and representation
flags, so observers do not renegotiate after preflight. Cardinality
continues to traverse the coordinator's global union of authorized keys rather
than adding owner-local counts. COUNT(NULL) remains a zero-read observer.

Ordered DISTINCT uses the same adaptive page sizing for finite and unbounded
consumers, capped by remaining logical demand for finite windows. Sparse and
rejected prefixes grow transport batches; duplicate-heavy pages return to seeks.
The existing representation, tie-order and residual proofs remain mandatory.

A certified complete group-key source can fold both bounded and unbounded
ordered COUNT/MIN/MAX output. Each completed group releases its reducer arena before
the next group is admitted. Groups before SKIP consume their required observers
without cloning discarded output into the statement arena; only emitted rows
remain owned. Ordered admission also avoids unused temporary candidate ranks. Unbounded output still needs memory proportional to its result.

SQL complete-filter certificates also transfer typed direct-column IS NULL and
IS NOT NULL predicates, including within supported AND/OR trees. Null tests bind
a closed NULL operand rather than trying to evaluate a row column at invocation
binding. Numeric/structured domains, partial filters, source ordinals and inner
selection barriers retain their existing fallback behavior and SQL residuals.


The public measurements below use ready indexes and 8,000 source documents.
Memory is charged query memory, including returned rows and the source working
page. Performance fixtures drain pending index work before asserting budgets;
client requests can tighten service ceilings, but cannot raise them.

| Query shape | Before | After |
| --- | ---: | ---: |
| Mixed COUNT(DISTINCT score), MIN, MAX | 8,000 anchors; 311,980 states | 7 anchors; 245 states |
| Duplicate COUNT(DISTINCT score) | 10 anchors | 5 anchors |
| Unbounded ordered grouped COUNT | 20.3–23.9 MB; 369,664 states | 8.08 MB; 160,066 states |
| Unbounded ordered grouped MAX | 20.3–23.9 MB; 425,664 states | 9.03 MB; 216,068 states |
| Grouped SKIP 7900 LIMIT 5 | exceeds a 2 MiB budget | 0.61 MB; 5 rows |

A separate SQL-populated fixture over 50,000 documents returns five grouped
COUNT rows after SKIP 49900 using approximately 1.13 MB on both storage backends,
under a 2 MiB allowance. The native fixture also covers COUNT/MIN/MAX across
that offset under 256 KiB, isolating reducer/output ownership from document
page decoding. Beyond-end and unbounded-offset cases retain their empty/last-row
semantics. Unbounded results still require memory proportional to emitted rows.

Finite sparse DISTINCT now satisfies the same native transport bound as its
unbounded equivalent: fewer than 100 data requests for 5,000 records. Typed SQL
NULL-test certificates are covered by five-row licensed windows over a
13,000-row source, plus nullable AND/OR, derived fallback, and parameterized
public result comparisons. Mixed observers also retain empty-input, all-NULL,
duplicate-alias and fallible-expression behavior.

#### Observer expressions and intermediate domain boundaries

Indexed observer planning follows the scope-local aggregate slots inside scalar
output expressions, rather than requiring a bare aggregate in every output
column. All observer certificates are negotiated before reads. Each aggregate
leaf executes lazily on first demand and shares its result across equivalent
calls. CASE and coalesce retain their original evaluation sites, aliases retain
their ORDER BY scope, and output expressions and sort keys still evaluate before
SKIP/LIMIT discards the single global result. Nonaggregate grouping items,
source-dependent scalar wrappers and unsupported observer arguments retain the
ordinary executor, including its empty-input cardinality and error behavior.

An intermediate DISTINCT can now consume a certified native key domain at that
stage boundary. Its small result feeds the following projection or reduction;
the runtime need not materialize all duplicate source documents first. Only an
adjacent key-domain proof licenses this boundary. Redundant entity DISTINCT,
ordinary row stages and symbolic path/range cohorts retain their existing fused
continuation and multiplicity planning.

Grouped DISTINCT counts can use the same per-domain observer plans as extrema,
including unbounded output. A bounded density sample selects between domain
seeks and one-pass ordered folding. Sampled records remain charged and are
replayed locally. Any ordinary COUNT observer keeps the required full-domain
fold; mixed COUNT/DISTINCT queries must not discard row multiplicity. Per-group
cardinality still observes the global authorized union of keys, never the sum
of owner-local counts. NULL grouping keys, all-NULL arguments and finite/offset
windows use the ordinary grouping and observer semantics.

The Go and TypeScript SDKs refuse automatic graph-query and catalog-write
redirects. The Go generated typed/raw operations apply the same policy as the
wrappers, clear replayable bodies after request editors, and bound request and
response bytes. A borrowed http.Client keeps its original redirect policy;
custom transports must implement GraphNoReplayDoer. Graph query requests retain
the 1 MiB ceiling, catalog commands have a separate 3 MiB ceiling, and graph
responses have a 32 MiB ceiling. TypeScript protects both wrappers and the typed
API through graph-route middleware. Receipt inspection remains a separate read;
uncertain delivery never licenses statement replay.

Measured with ready indexes, eight grouping keys and three score values on both
Lite and Directory. The figures below use 8,000 documents; repeating with 32,000
documents gives the same bounded anchor/state counts after these changes.

| Query shape | Prior anchors / states | Current anchors / states |
| --- | ---: | ---: |
| Wrapped COUNT(DISTINCT), coalesced MIN and MAX−MIN | 8,000 / 311,982 | 7 / 253 |
| Mixed observers followed by LIMIT 1 | 8,000 / 311,981 | 7 / 249 |
| MIN followed by ORDER BY its alias | 8,000 / 104,003 | 1 / 43 |
| Intermediate ordered DISTINCT followed by RETURN | 8,000 / 224,009 | 5 / 125 |
| Intermediate ordered DISTINCT followed by COUNT | 8,000 / 200,164 | 5 / 120 |
| Unbounded grouped COUNT(DISTINCT score) | 8,000 / 359,772 | 85 / 4,071 |
| Bounded grouped COUNT(DISTINCT score), LIMIT 8 | 8,000 / 359,824 | 61 / 3,075 |
| Intermediate tuple DISTINCT followed by grouped COUNT | 8,000 / 344,645 | 47 / 2,332 |

Intermediate ordered DISTINCT retains 49,490 bytes instead of 16,728,822 bytes.
At 32,000 documents the previous wrapped/limited observers and grouped DISTINCT
counts exceed the default state ceiling, and intermediate DISTINCT can exceed
the default memory ceiling. The new public regression exercises these shapes
with a 256-anchor, 2 MiB allowance and checks their exact expected results. A
separate nullable-group fixture compares seeks and full folds across unbounded,
bounded and beyond-end windows, including mixed ordinary/distinct counts.

#### Aggregate output scopes and SQL scalar observers

Grouped index planning now collects scope-local aggregate leaves inside scalar
output expressions. Group-key discovery, observation, HAVING and the output
window are separate phases. A certified adjacent key stream sends each key to
its observer consumer without materializing the full key relation. The consumer
recycles its invocation arena, evaluates output expressions and predicates, and
owns only rows surviving the final SKIP/LIMIT. Ordered folds apply the same
filter-before-window behavior. Certified total observers retain cheap retirement of skipped groups. A scalar
wrapper can keep a bounded group window only when its post-reduction evaluation
is proved total: checked i64 interval arithmetic covers COUNT with safe integer
operators, and COALESCE composes total leaves. Fallible or unknown wrappers and
predicates still evaluate losing groups so a nonzero finite window cannot hide
errors. The existing grouped LIMIT 0 short circuit is preserved.
The count interval uses the full i64 domain for compressed and multi-path
counts. A certified single-input, node-only producer can instead use the
statement anchor ceiling as its logical count bound, allowing safe expressions
such as COUNT(DISTINCT score) + 1 to retain bounded execution.

A following sort can supply the earlier aggregate's group-key order through
transparent alias projections. Computations, filters, DISTINCT, intervening windows and
correlated stages remain barriers to this propagation; the following consumer
still owns its original sort. Aggregate-output predicates remain after
reduction and are never reinterpreted as source predicates. Global indexed
observers apply their output predicate before ORDER BY and pagination, including
empty results and late pagination diagnostics.

SQL aggregate requests can negotiate an explicit scalar MIN, MAX or DISTINCT
domain through GRAPH_TABLE. Direct column lineage and SQL type metadata must
survive every forwarding projection. The graph bridge accepts integer, string
and boolean property domains only, rejects unproved residual conditions and
source ordinals, and obtains a complete native ordering certificate before any
pull. Direct group keys and complete typed comparison/NULL/AND/OR admission
predicates travel with the observer through identity projections. Computed
inputs, numeric/structured coercions and set/join barriers retain ordinary
execution unless their existing independent proofs apply. Every graph projection retained for the SQL consumer still needs a total-expression
proof. Observer negotiation cannot elide errors in demanded computations.

Scalar observers use the resumable node cursor. MIN/MAX consume one authorized
non-NULL boundary; DISTINCT keeps one owned adjacent key, adaptive source pages
and unit SQL multiplicities. No full distinct relation is buffered between the
graph and SQL reducers. Several observers share the statement's immutable read
cut and cumulative anchor/state quotas; owner-local distinct counts are never
summed. SQL keeps its own aggregate reduction, NULL/empty-input semantics,
coercion and final output evaluation.

The eight-group/three-score public-boundary fixture verifies exact outputs on
Lite and Directory after draining both native indexes. Work is independent of
the source length for these selected key domains. Entries below are physical
anchors / intermediate states; both backends report the same counts.

| Query shape | Before, 8,000 documents | After, 8,000 and 64,000 documents |
| --- | --- | --- |
| Grouped COUNT(DISTINCT score) + 0 | 8,000 / 336,296 | 85 / 4,091 |
| Global COUNT(DISTINCT score), output WHERE | 8,000 / 167,985 | 5 / 180 |
| Grouped COUNT(DISTINCT score), following RETURN ORDER BY | 8,000 / 336,320 | 85 / 3,991 |
| Grouped COUNT(DISTINCT score), output WHERE | 8,000 / 336,408 | 85 / 4,015 |

The previous grouped shapes exceed the default state allowance at 32,000
records; the previous global output filter exceeds it at 64,000. The new public
regressions allow 256 anchors and 2 MiB, compare filtered windows with the
ordinary executor, and check failing expressions outside nonzero windows.
A separate 5,000-group regression bounds safe arithmetic, COALESCE and unary
wrappers with the same allowance; native tests cover providers that decline
observer ordering or ignore tuple seeks. Scalar DISTINCT also streams 5,000
keys under a 512 KiB cursor allowance with narrow caller pages.

For SQL over 32,000 documents, warmed median latency (one warmup followed by
four measured executions) now tracks the scalar index domains rather than the
source length:

| SQL outer aggregate over GRAPH_TABLE | Lite before / after | Directory before / after |
| --- | --- | --- |
| COUNT(DISTINCT score), MIN(score), MAX(score) | 214.36 ms / 0.30 ms | 87.14 ms / 3.24 ms |

At 64,000 documents the same outer aggregate measures 0.34 ms on Lite and
3.06 ms on Directory. The separate native graph-bridge regression certifies
all three observer passes before reading and exhausts them under five anchors;
its narrow pages verify unit multiplicities and independent aggregate slots.

Final verification after merging origin/main (`3f8eeafaaf`): 467 native Cypher
tests; 1,080 SQL and 226 catalog-related tests plus four transport checks;
139 system-catalog integration tests; all Go SDK packages; 521 TypeScript SDK
tests (one skipped) and 290 Antfarm tests; and all 1,136 public Python Cypher
and performance tests on the final optimized library. Antfarm builds, the
137-function C API export boundary, generated error checks, formatting and
whitespace checks also pass. The merge preserves both graph and native-memory
SDK/UI contracts and assigns separate metadata floors and command tags to the
new graph/session and resolver capabilities.


#### Accepted group demand and SQL grouped scalar domains

A proved total HAVING predicate can stop after SKIP + LIMIT accepted groups.
Rejected groups do not consume that demand. Integer comparisons, boolean
composition and aliases of proved total counts qualify; fallible or unknown
predicates still inspect all candidate groups. Both adjacent folds and indexed
group discovery apply this rule after reduction. LIMIT 0 retains its existing
short circuit. Count arithmetic uses a tighter bound only for a certified
one-row-per-anchor producer; weighted axes and multiple paths keep the full
logical count range and their checked overflow behavior.

SQL observers carry the argument, direct group-key tuple and complete admission
predicate as one obligation. Forwarding owns and renames the entire obligation,
including every predicate dependency. The graph cursor owns the AST and scalar
type environment used by deferred negotiation. It keeps all demanded total
projection fields for SQL residual checks, while adjacent membership and seeks
use only the certified group/value tuple. SQL retains its own grouping and NULL
semantics, including groups containing only NULL arguments.

Grouped DISTINCT traverses globally ordered group/value domains. Grouped extrema
sample the admitted key stream: sparse groups use one sequential fold; dense
groups seek their next key and one non-NULL value boundary. A boundary provider
that declines before pulling switches the remaining discovery stream to one
fold without replaying previous groups. Every boundary has a recyclable
invariant scope; allocation identities and literal hashes cannot leak into the
next group. Narrow caller pages preserve unit multiplicities.

Grouped GRAPH_TABLE COLUMNS can request intrinsic group-key order when direct
scalar group keys are available. Native execution still certifies the index and
reducer. Synthetic source ordinals, positional resume tokens and unsupported
expressions remain barriers; SQL keeps its own final ordering.


Admission certificates come from SQL's bound typed program, including scalar
AST predicates and its inserted coercions. Closed literal/parameter coercions
bind once per invocation; a failed speculative conversion declines observation
and remains the residual evaluator's responsibility. Row-dependent casts and
fallible predicates remain barriers. Identity frames carry and rename the
entire predicate obligation and its dependencies. Catalog retention uses a
typed clone, preserving the internal representation and ownership of label
predicate values independently of their wire JSON form.


Repeated or overlapping IN predicates over one primitive property intersect
before native ordered partitioning. NULL members create no true-row partition,
and typed hash membership bounds normalization work. The surviving equality
partitions remain disjoint and reuse the pinned read and continuation protocol.
Independent property domains retain their existing cost barrier. This keeps
repeated admission hints from disabling sparse ordered SQL/Cypher observers.


The final public-boundary measurements use 64,000 documents on Lite and
Directory, with native indexes fully published. The grouped fixture has eight
`id` keys and three `score` values, indexed by `(id, score DESC)` and
`(score DESC)`. The bounded-group fixture has one distinct `id` per document.
Every measurement checks its exact result.

| First five ordered groups | Previous result at 64,000 documents | Final anchors / states, both backends |
| --- | --- | --- |
| COUNT(DISTINCT score) + 1 | State budget exceeded | 6 / 192 |
| COUNT(DISTINCT score), aggregate-output WHERE c > 0 | State budget exceeded | 6 / 257 |

The arithmetic and filtered-group plans retain 44,964 and 41,620 bytes respectively.
Their public regressions use a 256-anchor, 2 MiB ceiling, including offset
windows. Overlapping native membership predicates followed by COUNT(DISTINCT),
MIN and MAX use eight anchors, 787 states and 97,388 bytes on both backends.

SQL latency is a warmed median after one warmup and three measured executions:

| SQL aggregate over GRAPH_TABLE, 64,000 documents | Previous result | Lite | Directory |
| --- | --- | --- | --- |
| COUNT(DISTINCT score), MIN, MAX; outer score >= 1 | Operator limit exceeded | 0.36 ms | 1.03 ms |
| Same observers; outer score = 0 OR score = 2 | Operator limit exceeded | 0.65 ms | 2.30 ms |
| Same observers grouped by id | Operator limit exceeded | 2.05 ms | 3.11 ms |
| Grouped observers inside COLUMNS, outer ORDER BY id | Operator limit exceeded | 1.89 ms | 3.12 ms |

Additional 64,000-row checks cover filtering on a different projected field,
filtered groups, derived aliases and invocation parameters. Nullable keys and
arguments, source ordinals, provider declines, losing-group errors, weighted
counts and arithmetic overflow retain their original results and diagnostics.
Index-budget fixtures wait for publication before asserting bounded indexed
work, so background build timing cannot select their ordinary scan fallback.


Final validation after merging origin/main (`60a70f8399`): 469 native Cypher
tests, 1,081 SQL tests, 226 catalog-related tests and four transport checks;
all 1,142 public Python Cypher/performance tests on the final optimized library;
and the six new public regressions plus 66 publication-ready index-budget
fixture cases in separate focused runs. The C API build and its 137-function
export boundary, generated engine-error inventory, Zig/Python formatting and
whitespace checks pass.

#### SQL group windows and independent reducer admission

SQL scalar observers distinguish source admission from each reducer's `FILTER`.
The complete typed source predicate travels through identity frames separately
from the reducer predicate. `COUNT(DISTINCT)`, `MIN` and `MAX` negotiate their own
admitted domains. Filtered grouped reducers additionally share a source-group
presence pass: a group survives even when every reducer rejects all its rows.
Representative contributions are licensed only for these idempotent reducers;
ordinary counts, sums and fallible arguments retain their normal execution.

Grouped reducer domains retain leading and fixed-prefix native seeks. A total
reducer predicate over a complete ordered key tuple keeps non-leading key
conditions as runtime residuals: otherwise those hints can select a different
index or conceal rejected duplicate runs inside a native scan. The consumer
retires each rejected tuple after one witness. Structural constraints stay
pushed, and ordinary selective source filters retain their access-path choice.

A finite SQL window over the complete direct group-key order carries an explicit
observer demand, including each key's direction and `OFFSET + LIMIT`. Once every
reducer has certified its scalar plan, one shared source-domain discovery obtains
the final key of that prefix. Every serial reducer uses the same boundary and
key order, including reducers with different filters. This prevents filtered-out
groups from shifting the requested prefix. NULL keys use native tuple ordering.
Identity frames forward the certificate; joins, sets, HAVING, incomplete key
orders and fallible grouped outputs decline it. Relational barriers and mixed
scalar/statistical reducer families discard only this optional demand, preserving
complete compressed weighted execution. Grouped `GRAPH_TABLE COLUMNS`
projections similarly lower a certified outer group-key window to the shared
native grouped executor. Source ordinals and residual predicates remain barriers.

Closed SQL Boolean predicates bind per invocation before observer negotiation.
TRUE contributes no restriction; FALSE or UNKNOWN admits no input. Partial or
fallible predicate programs never become empty-source certificates. For a total
predicate depending only on an attested ordered key tuple, a rejected witness
also licenses retirement of that entire duplicate-key span. This includes NULL
and range disjunctions without flattening their three-valued Boolean semantics.
Uncertified key domains and predicates reading other properties keep their
ordinary residual scan. Rejection tuples use recyclable storage leased to each
active fold, rather than accumulating one allocation for each rejected row.

Both Antfarm graph views share a memoized linear-time parallel-edge lane pass.
It groups unordered endpoint pairs once, preserving encounter-order lanes,
reverse-edge offsets and self loops across pan, zoom and selection updates.

The 64,000-document public regressions now exercise constant and parameterized
admission, independent aggregate filters, NULL/range disjunctions, and bounded
SQL groups on both Lite and Directory storage. They compare nullable groups,
empty filtered groups and mixed key directions against an exhaustive arithmetic
projection control, and retain errors from fallible source and grouped outputs.
The native NULL/range case additionally runs with a 128-anchor budget. Previously
failing high-cardinality SQL windows now complete with the normal operator limit,
including descending order, offsets and identity wrappers.

After confirming scalar-index publication on the 64,000-document dense-group
fixture (7 global anchors and 85 grouped anchors on each backend), the original
grouped `FILTER` query completed in 6.80 ms on Lite and 11.96 ms on Directory
(median of three warmed measurements). Before this change both exhausted their
operator budgets. `WHERE TRUE` measured 1.39 ms and 3.57 ms respectively, in the
same range as the bare aggregate. The
400-edge lane helper benchmark measured 60.47 ms before and 0.23 ms after, with
identical lane assignments; this measures the helper rather than browser render
time.


Validation after merging `origin/main` at `730999bf2d`: all 69 native build and
check steps pass, including 471 Cypher tests, 1,081 local SQL tests, 226 server
SQL tests, four transport checks and the 137-function C API export boundary.
The rebuilt optimized library passes all 1,148 public Python Cypher/performance
tests and a separate eight-case focused run covering the new regressions and
compressed grouped-path fallback through `UNION ALL`. The shared lane helper's
26 focused UI tests, TypeScript checks, generated error inventory, graph error
contract, formatting and whitespace checks also pass.

The next performance pass extends the bounded SQL group path with a shared,
pull-based scalar group producer. It seeks one complete key at a time, reduces
that key's full domain, and leaves grouped output and `HAVING` evaluation to the
typed SQL evaluator. OFFSET and LIMIT count accepted groups. A range-based
totality certificate admits safe wrappers such as `count(DISTINCT x)+0`,
`coalesce(count(DISTINCT x),0)` and double negation; fallible or unsupported
outputs retain complete execution. The earlier window certificate remains a
fallback for compatible plans, and relational barriers retain compressed
weighted aggregation.
Admission checks the logical SQL types of every group key and reducer field.
JSON and array keys retain ordinary SQL grouping even if a backend can attest
to physical index ordering; that ordering alone does not license SQL grouping.

The producer handles explicit NULL placement by seeking the NULL and non-NULL
phases of each lexicographic key, including mixed directions and permuted keys.
Each seek orders one axis under a fixed earlier prefix, so the same exact
non-NULL bound applies to every key position.
Its DISTINCT projection contains only that axis: physical suffix keys may
reorder peers, but every peer projects the same value. The witness therefore
stops immediately rather than draining a dense peer run for encounter ties.
It retains only the continuation tuple between pulls. Each group's planning
state uses a reclaiming execution epoch with the same memory budget and
cumulative work quotas, so selective HAVING does not accumulate planning caches
for discarded groups. The scan and its pinned read cut outlive finalized group
delivery. Embedded SQL, authenticated HTTP SQL and pgwire validate live authority
before and after each group pull.

Native NULL access hints lower to SQL null-safe equality. This lets the storage
planner seek the exact NULL prefix, including proving an empty partition without
scanning non-NULL entries. The authoritative Cypher predicate is retained. A
64,000-row regression requires zero scanned anchors for an absent NULL group.

IN scan hints now normalize homogeneous primitive domains before applying the
unique-value and fanout limits. Repeated parameter entries no longer consume the
anchor budget. The original parameter and IN evaluator retain their NULL and
three-valued semantics. Invocation-invariant domains are normalized once;
row-dependent domains remain local to their evaluation.

Graph statements derive a conservative set of physical data dependencies from
the compiled AST, including nested patterns, EXISTS and UNION branches. Only
those owners drain indexes and capture immutable data cuts. Catalog resolution,
schema checks and authorization still cover every graph mapping. Unlabeled
nodes, dynamic procedures and writes retain the full data cohort. Constant
queries need no data cut. The embedded and distributed statement adapters accept
partial cohorts only when the host supplies this dependency proof.

The shipped Antfarm bundle is rebuilt from the lane-helper implementation, so
the committed application now includes the linear-time edge-lane calculation.

On the indexed 64,000-row fixture after merging `origin/main` at `27011cf1c3`,
formerly failing SQL query shapes complete with the ordinary operator limits.
Medians of three warmed runs after index
publication are shown below; these measure local embedded execution.

| SQL group window, LIMIT 5 | Lite | Directory |
| --- | ---: | ---: |
| `count(DISTINCT score)+0` | 0.60 ms | 1.15 ms |
| `coalesce(count(DISTINCT score),0)` | 0.55 ms | 1.27 ms |
| `HAVING count(DISTINCT score)>0` | 0.48 ms | 1.11 ms |
| Explicit `NULLS FIRST` | 0.49 ms | 1.40 ms |
| Selective `HAVING min(score)=1`, OFFSET 2 | 2.22 ms | 3.30 ms |

IN inputs containing 2, 256, 258 or 1,000 repeated entries all use four anchors
for the same two-value domain on both backends. The 1,000-entry case measures
0.60 ms on Lite and 1.44 ms on Directory. The native coarse-order regression
also returns both complete groups from 512 duplicate rows within a 128-anchor
budget, scanning fewer than 32 anchors. An empty NULL partition uses zero
anchors on the real storage backends.

With 64 mapped tables, Directory constant and one-anchor point queries measure
0.06 ms and 0.49 ms respectively; Lite measures 23 ms and 22 ms in this run.
Full graph metadata and authorization checks remain proportional to the mapped
owners. Pruning data cuts preserves those checks and their cost.

Hosted query-cut sealing borrows primary and replay admission from its
distributed statement cohort. Those leases remain owned by the cohort through
artifact sealing and are released once, after every participating owner's cut
is captured. Independently admitted cuts still release mutation admission
before sealing. The native regression checks admission during materialization,
two consecutive statement captures and a write after cohort release. This
prevents premature writer admission and the double release that could poison
later hosted reads with persistent `Backpressured` responses.

Final validation after merging `origin/main` at `27011cf1c3`: native Cypher,
local SQL, server SQL and transport suites pass. The final optimized build
passes all 124 steps, including four query-capture ownership/cancellation tests
with zero leaks and the 137-function C API export boundary. The rebuilt library
passes all 1,150 public Python Cypher/performance tests. The authenticated server
campaign passes HTTP graph queries, SQL GRAPH_TABLE, pgwire, transaction
overlays and authorization checks, including the new 64,000-row bounded group
cases. Go SDK race tests, generated Python SDK imports, 30 focused UI tests,
TypeScript checks, full generation, generated error inventory, graph error
contract, formatting and whitespace checks pass. The shipped Antfarm bundle
matches the pinned frontend build.

Finalized scalar-group sources own a retained invocation domain distinct from
their reclaiming per-group planning epochs. Host-supplied
admission expressions, node constraints and reducer predicates are specialized
together without mutating prepared ASTs. Their ownership IDs cannot collide
with an earlier cursor-negotiation domain. Immutable parameter allocations also
key normalized IN hints and equality indexes: plain parameters do not have a
compiled invariant ID. Only proved invocation-owned values survive a group
pull; synthetic key literals and allocation-keyed row metadata remain local.
Shared work callbacks point at the active epoch while it executes, so cache
construction and later probes retain one cumulative statement quota.

Embedded authority checkpoints subscribe to every captured schema registry and
policy gate, including mappings outside the data cut. Each publication brackets
its state change with cohort invalidation; an in-flight publication never
licenses cached validation. An unchanged cohort needs a constant number of
atomic reads, rather than acquiring every mapped owner's schema view on every
execution step. Changed cohorts retain the full lease/schema checks. The read
also checks its earliest wall-clock and monotonic expiries on every checkpoint.
Subscriptions detach before owner leases and read storage are released. The
notification state lives in its owners, so separate storage archives observe
the same publications without relying on duplicate module globals.

Certified group-key HAVING predicates now restrict group discovery before
reduction. Totality facts bind parameters through typed SQL invocation evaluation;
failed speculative conversions decline pruning and preserve ordinary evaluation.
The bridge rebinds invocation constants, extracts safe conjunctions,
and translates a disjunction only when every arm is supported. Constant FALSE
and UNKNOWN license an empty domain. The original typed SQL HAVING remains
authoritative after reduction, and aggregate-dependent terms stay there.
This restriction is attempted only with the total-output proof and a producer
which certifies the complete scalar obligations. Failed negotiation keeps the
original fallback requests, preserving errors in fallible source projections.

Validation after merging `origin/main` at `03136b096d`: the final optimized
library/server build passes all 140 steps and the 137-function C API export
boundary. The native SQL suites pass 1,082 local and 227 server tests; all eight
transport tests pass, with zero leaks. Native Cypher passes 474 tests. The six
authority-publication tests and two C API graph/authority tests also pass with
zero leaks. The rebuilt library passes all 1,150 public Cypher/performance
tests, including the 64,000-group parameterized HAVING and repeated-IN cases.
The authenticated HTTP/GRAPH_TABLE/pgwire transaction and authorization campaign
passes. Release validation passes 225 tests with one skip; generated source and
error inventories, error identities, formatting and whitespace checks pass.

Qualification also observed two memory-quota failures in the existing grouped
path-statistics test during one public run. Both cases pass in isolation and in
the subsequent full suite without code or quota changes; a direct 32-AVG probe
reports about 42.5 MB peak under the unchanged 64 MiB default. This remains a
nonreproduced qualification signal, rather than a claimed memory fix. The
publication module separately compiles and executes a wasm32 single-threaded
roundtrip. The broader WASM module-boundary check remains blocked by the merged
baseline's unresolved `inference_api` import in chunking/types.zig.

All 30 final performance probes pass with result equality checks. Warm medians
(three measured runs after one warm-up) on the same 4,000-document fixture:

| Directory query shape | Before | After |
| --- | ---: | ---: |
| 64 mapped owners, 4,000-anchor scalar reduction | ~74 ms | 20.4 ms |
| Last group selected by key-only HAVING | 154.7 ms | 0.64 ms |
| Equivalent key WHERE | 0.50 ms | 0.63 ms |
| HAVING FALSE | 153.8 ms | 0.57 ms |
| 300 repeated IN entries, OFFSET 999 LIMIT 1 | SQLSTATE 54000 | 119 ms |

The three-entry IN control takes 115 ms, showing the repeated input no longer
incurs normalization per discarded group. Parameterized key HAVING with an
aggregate conjunct takes 0.75 ms. Lite also passes all probes: the same 64-owner
reduction takes 88 ms versus about 127 ms before; key HAVING takes 5.5 ms versus
5.1 ms for WHERE, and FALSE takes 4.4 ms versus 4.1 ms for WHERE FALSE. These
whole-query measurements include owner/catalog startup and storage work; that
startup still validates mapped owners even though repeated authority checkpoints
now use a constant-time cohort check.

SQL finalized scalar groups expose a certified key-only prefix operation. The
SQL host uses it only for total output with no HAVING; HAVING still chooses the
accepted groups before OFFSET. Skipped keys retain their continuation and count
against the same logical row/page and Cypher work quotas, but allocate no output
cells and evaluate no reducers. Embedded and hosted cursor wrappers validate
authority before and after the prefix, and each key retains engine checkpoints.

Ordered seek negotiation also considers the requested OFFSET + LIMIT, reducer
count, bounded source cardinality, available retained bytes and work headroom.
Small windows retain indexed seeks. A deep window may decline streaming for the
ordinary full-pass aggregate when a conservative estimate fits its memory and
work budget. Unknown or capped estimates retain bounded traversal. Costing is
advisory and supplies no pruning certificate; fallible projections and HAVING
continue through the existing proof and fallback boundaries.

Embedded graph catalogs store a small versioned manifest and independently
indexed graph records. Named queries read a format marker and one record, so
unrelated graph definitions no longer inflate query startup. Listing still
materializes the full catalog. Version 1 catalogs remain readable and migrate
atomically on their next mutation. Manifest comparison under the write
transaction rejects a stale predecessor; updates preserve incarnation, while
drop/recreate allocates a fresh identity. Response encoding precedes commit.
Old binaries reject the version 2 manifest, and future format markers reject
reads and writes rather than downgrading the catalog.

Hosted graph checkpoints batch graph/table ACL checks against one live
permission snapshot, including API-key expiry and current owner/key permission
intersection. The admitted request scope remains an additional intersection.
The metadata authority endpoint performs a linearizable lookup of the current
indexed graph name-to-incarnation binding and verifies its record still exists,
without decoding its definition or dependency closure. Standalone queries use
the equivalent indexed in-memory binding under the catalog mutex. Every call
checks the current binding, including after catalog replacement with a reused
revision. Checks still bracket every data RPC: this reduces allocation and
decoding work, and retains both control-plane round trips and immediate
revocation behavior. Custom sources without the probe capability retain full
catalog reads.

Whole-population costing reads the maintained document-identity visibility
summary from the retained primary cut. It is an upper bound: TTL, graph
membership and row policies may hide documents. It never supplies an aggregate
or licenses pruning. Missing summaries retain the seek plan. Indexed filtered
estimates retain their existing bounded path, and all estimator calls install
the statement work meter and bracket storage access with authority checkpoints.

The embedded manifest also maintains the encoded record-byte total. Mutations
update it atomically with the selected record and enforce the existing 32 MiB
whole-catalog ceiling before committing; listing verifies the total. This keeps
the bounded list contract while avoiding unrelated definition reads on ordinary
updates. Regression coverage checks migration/accounting, stale-write rejection,
capacity rejection without publication, drop/recreate identity and future-format
rejection.

Filtered cardinality estimates request at most the native reader's 4,096-row
page limit and project no fields. They count policy-checked index candidates
without constructing graph nodes; graph membership can only reduce that upper
bound. An incomplete page or remaining IN alternatives makes the estimate
unknown, retaining bounded traversal. The same index scan implementation serves
ordinary reads, preserving their existing projection and visibility behavior.

After merging `origin/main` at `9d152a4c78`, the rebuilt optimized library passes
all 50 build steps and the 137-function public export boundary. All 1,154 public
Cypher/performance tests pass, including the 64,000-group repeated-IN cases.
Native Cypher passes 474 tests. The focused catalog migration/accounting test,
three physical metadata binding tests and 19 user-manager tests pass.

Eleven final performance probes pass with result equality checks. Warm medians
(five measured runs after warm-up) on a 4,000-document Directory fixture:

| Query shape | Before this pass | After |
| --- | ---: | ---: |
| OFFSET 999 LIMIT 1, one reducer | 39.9 ms | 8.4 ms |
| OFFSET 999 LIMIT 1, three reducers | 79.1 ms | 20.4 ms |
| OFFSET 2999 LIMIT 1, one reducer | 117.0 ms | 23.8 ms |
| OFFSET 2999 LIMIT 1, three reducers | 233.5 ms | 45.0 ms |
| Named constant query, 256 unrelated graphs | ~1.34 ms | 0.0183 ms |

The low-offset indexed path remains about 1 ms. Named constant queries take
0.0189, 0.0176 and 0.0183 ms with zero, 64 and 256 unrelated graphs respectively.
These local whole-query timings demonstrate the access-path and catalog gains;
they do not measure distributed network latency.

The final native SQL qualification passes all 24 build steps: 1,082 local SQL
tests, 227 server SQL tests and 57 supporting tests, with no failures or leaks.

Remote estimates also respect the owner's existing 128-candidate sample limit,
independently of the planner's requested full-pass bound. The RPC client sends
the smaller demand, rejects oversized replies and treats saturation below the
planner's demand as unknown. This prevents planner hints from becoming owner
budget errors or underestimating the population. Regression coverage exercises
large demand, saturation, authority rejection and malformed replies. The
replicated expression campaign varies its CONTAINS needle per iteration so its
scan-budget assertion remains meaningful with loop-invariant result caching.

The corrected replicated-owner campaign passes end to end (130 seconds),
including its catalog, expression, mutation and policy checks. All 474 native
Cypher tests pass again with the bounded RPC estimator regression.

The final RPC-qualified server passes all 71 optimized build steps. Its
replicated-owner campaign passes (134 seconds including setup/teardown), and the
authenticated standalone property-plan, SQL GRAPH_TABLE and pgwire campaign
passes (78 seconds), including the 64,000-group repeated-IN regression. One
standalone launch stopped at the harness disk-headroom precondition; after
obsolete local archive cleanup, the unchanged server passes the full campaign.

Final integration also merges `origin/main` at `0c753dc705` (Lite recovery and
allocator proofs). Conflict resolution retains both current-catalog validation
and detached allocator verification, and startup replay honors either deferral
option. Post-merge qualification passes all 66 native build steps, 474 Cypher
tests, 624 local Lite tests and 12 companion tests, with five skips and no
failures or leaks; 81 affected Python SDK tests also pass. The full public,
SQL and authenticated server campaigns above were run before this final Lite
merge, rather than being claimed as repeated on the merge commit.

Distributed search now retains top-k candidates in a worst-first heap, with a
dense identity index that avoids tombstone accumulation under eviction. Ranking,
key hashing and final ordering consume statement work; coordinator checkpoints
remain bounded during merging. Result order remains descending score then
ascending document key, and retained duplicate identities still reject overlap.
The isolated 64-owner merge probe takes approximately 4 ms at k=512 and 8 ms at
k=1024, versus 37 ms and 153 ms before; these measure merge CPU rather than
network or owner search time.

Cardinality costing has a separate retained-cut population capability across
native, distributed and RPC backends. Embedded pinned reads retain the summary
with their primary transaction; hosted statement snapshots capture it from the
opened query-cut database and provide it through the native authority adapter.
It reads maintained metadata without sampling rows and sums owner bounds with
overflow/unknown handling. Unfiltered
costing uses it directly. Filtered estimates retain their existing bounded page
limits, falling back to the conservative physical population when sampling is
incomplete. Active write overlays decline the capability. Population evidence
never supplies an aggregate or licenses pruning; missing or excessive bounds
retain bounded traversal.

Hosted graph admission resolves unique database/namespace/table targets in one
catalog epoch, then reuses those bindings for label aliases. Every label still
validates its captured immutable table identity, and each unique target retains
its logical permission checks, schema binding and row-policy proof. The shared
SQL resolver supports batched definition reads; graph execution also reuses the
logical names preserved by graphForExecution instead of parsing the definition
a second time. A focused regression admits 64 aliases over two physical tables
through one metadata resolution and rejects a stale identity on the last alias.

Native Lite manual maintenance also coordinates with already-detached work before
taking its path-writer lease. Waiting releases the owner gate so a publisher can
finish and release its retained cut; the manual drain reacquires the gate before
refreshing and draining. This avoids retaining a competing reader frontier while
a large synchronous drain exhausts retirement admission. Ordinary reads/writes
keep concurrent admission, and external writer contention retains the existing
busy contract. A focused concurrency test covers waiting for multiple detached
publishers and reuse of the idle notification; it runs in the C API unit gate.

After merging `origin/main` at `b0b39178d6`, focused qualification passes 476
native Cypher tests, nine transport/admission tests and the manual-maintenance
concurrency test. Native SQL passes 1,082 local, 227 server and 57 supporting
tests. The corrected library passes all 50 optimized build steps and the
137-function export boundary. All 1,154 public Cypher/performance tests pass
(468 seconds), including the three Lite drains which exposed detached-cohort
contention in the initial run. That run also reproduced the earlier 32-AVG
memory-quota signal; both isolated cases and the final full run pass without
memory-code or quota changes. The admission fix addresses the FileBusy drain
failure, rather than being claimed as a memory-quota fix.

The final hosted-capability qualification repeats all 476 native Cypher tests
and both real-owner/retained-cut tests. The latter captures two documents, adds
a third live document, and proves that the RPC population remains two and
rejects another table. The final production build passes all 88 steps; the
library passes 50 steps and its 137-function export boundary. All 1,154 public
Cypher/performance tests pass again (777 seconds on the shared host), and both
authenticated standalone and replicated-owner campaigns pass (235 seconds,
including setup/teardown).

An authenticated 4,000-document SQL GRAPH_TABLE probe verifies index readiness
with a one-anchor read and compares equal results over five warmed samples.
At offset 2,999, count(DISTINCT score), min(score), max(score) with ORDER BY id
now takes a median 671 ms versus 644 ms for the expression-forced full-pass
control. Before the fix, the corresponding medians were 1,189 ms and 559 ms:
the seek penalty relative to its control falls from roughly 2.1x to 1.04x.
These are shared-host observations, not a controlled absolute speedup claim.
The one-/64-label admission probe is too noisy for a reliable timing claim
(59/94 ms medians, with individual samples up to 276 ms); the focused admission
regression instead proves the constant metadata-call count and per-label stale
identity rejection. Search-merge CPU results above exclude network and owner
search time; their benchmark removes the work cap to measure the complete
merge, while production enforces the newly accounted coordinator work budget.


Ordered membership unions retain one shared position vector plus one update per
emitted row. The private page/RPC contract carries this trace separately from
legacy scalar cursor arrays. Consumers materialize a full continuation only at
the boundary they actually consume, including singleton coordinator envelopes.
Baseline strings belong to the receiving page allocation, never a reclaimed
proof arena; decoding rejects duplicate/out-of-range owners, invalid update
slots, resurrection of a finished stream, and contradictory cursor formats.
This removes the eight-domain cutoff while retaining the existing bounded
membership proof, exact scalar disjointness, residual predicates, tie handling,
and statement work/memory/anchor admission. ORDER-compatible equality-prefix
indexes take precedence over broad sort indexes, with expression-key guards.
Adjacency caches retain only payload/continuation rather than ordered metadata.

Owner setup has bounded I/O waves (at most 16, additionally capped by host I/O
configuration). All read-index admissions finish before capture; the complete
cohort is captured before snapshot forks. A busy or failed capture joins its
wave and releases every successful fence before retrying. Fork failures close
all successful snapshots. Native cancellation stops later dispatches; cleanup
still visits every retained capability. Hosts explicitly mark movable remote
capabilities; retained local fences/snapshots stay on the controller thread.
The statement-owned allocation lock covers allocation operations, not owner
RPCs, and outlives all capabilities allocated through it.

Distributed search groups remote owners into bounded waves with independent
arenas and counters. It reserves disjoint work and candidate ceilings before
dispatch, retaining sequential admission for tight ceilings and local
capabilities. Budget failures report consumed work and candidate progress over
the private RPC. After joining and charging all attempts, an owner constrained
by its reservation can retry once serially using unused capacity. The first
attempt is never refunded. Worker memory admission retains quota-failure
provenance across concurrent allocations; ordinary allocator flags keep their
per-call behavior. Remote graph RPC request/reply buffers use the worker's
allocator; reply bodies transfer when ownership matches, otherwise copy with
the transport's independent ownership respected. Capability allocation remains
separate so cleanup can release it even after query quota exhaustion.

Search merging borrows worker payloads until the wave joins, then promotes only
its surviving top-k into independently reclaimable heap slots. Eviction frees
the old slot, and final output transfers only retained payloads into caller
storage. Payload retention is bounded by active owner pages plus top-k rather
than all intermediate heap replacements. Owner-local scoring still sets the
existing approximate flag for multi-owner search.

Controlled probes inject five milliseconds at each owner boundary. Admission,
capture and fork for 8/16 owners take 18/37 ms versus 149/305 ms before. Search
for 8/16/64 owners takes 6/12/50 ms versus a sequential 49/96/385 ms with equal
results. These isolate scheduling under injected latency, not production
network, storage or scorer performance.

This implementation includes origin/main at 809b5d8be027. The merged private
failure ABI is version 66: existing Cypher failure identities remain stable,
and main's unsupported-exact-sort failure gets a distinct appended identity.
The Go SDK and embedded workbench assets were regenerated from the merged
contracts and frontend sources.

The final public library's 4,096-document membership probe returns identical
top-ten rows for eight alternatives and the same domain plus one absent value.
Both now scan 17 anchors and pass a 64-anchor ceiling; previously the ninth
alternative scanned 2,048 anchors and failed that ceiling. Three warmed samples
give medians of 0.60/0.81 ms for eight/nine alternatives, versus 1.09/8.58 ms
before. Reported memory is 68,336/68,764 bytes versus 99,980/1,119,912 bytes
before. These shared-host timings support the deterministic scan/memory result;
they are not a general production latency guarantee.
The existing 13,000-document, 256-domain top-1,000 regression now also runs
under a 4,096-anchor ceiling and its original 4 MiB memory limit in both
storage modes. It scans 3,040 anchors instead of the former full 13,000-row
fallback; its assertion now enforces the bounded access path.

Qualification of the final library covers all 1,156 public Cypher/performance
cases: the full run passes 1,154 cases and identifies two obsolete full-scan
expectations; both corrected bounded-scan cases pass on rerun. The new compact
membership regression also passes independently in both storage modes.
Native Cypher passes 481/481 tests. The combined SQL, graph transport,
admission and retained-cut qualification succeeds in all 87 build steps,
including 1,309 SQL cases and 10 transport cases. The final shared library
passes all 50 build/export-check steps. Generated Go SDK tests and the merged
workbench frontend build pass as well.
The final production server/library/export build succeeds in all 88 steps.
Authenticated qualification passes all 10 selected cases across the full run
and the property-campaign rerun: replicated-owner execution, all eight search
cases, and the complete property/SQL GRAPH_TABLE campaign. The latter creates
both ordered indexes before its fully indexed 64,000-row ingestion, avoiding
an unrelated late-backfill race. Its nine-/32-alternative queries retain the
strict 64-anchor ceiling and at-most-16-scans assertion. An isolated
authenticated 64,000-row probe returns the same top ten using 12 scans.

Ordered capability probes and initial one-row owner heads now use bounded
remote read waves. The coordinator preserves catalog owner positions when
collecting replies, so sorting, ties and nested continuations retain their
original semantics. Already-finished owners are omitted on resume. Later
refills still follow the winning stream and the consumer's remaining demand.
Cross-owner get-many uses the same wave admission, with per-owner identities
and original output positions; identity validation and overlap rejection run
before accepted values enter the result. Local capabilities stay on the
controller thread, including when routing interleaves local and remote owners.

Read waves have independent arenas and work counters, disjoint candidate
reservations, and coordinator headroom. Tight ceilings and backends without
isolated counters use sequential admission. Every started read joins before
results are combined or scratch is released. Work-limited private ordered
and batched reads return consumed work over RPC; authority is rechecked before
returning a budget failure. Failed reads without candidate progress consume
their entire candidate reservation. A skewed reservation can retry once with
the unused whole-wave capacity; no work or candidate admission is refunded.
Fatal errors, cancellation and quota failures drain the started wave.

Sequential search, local-owner search and the one-owner fallback within a
remote search now reclaim each owner page after promoting only surviving
top-k entries. Owner value accounting also reclaims its transient metadata
cache instead of retaining it in the statement arena. This bounds payload
retention independently of the number of owner pages even when parallel
execution is unavailable or a small quota selects the sequential path.

This integration merges origin/main through 031742c340741b5b4b8f2f49d2f366cbf2c5fe09.
Metadata protocol 63 admits graph definitions and transaction sessions alongside
main's resolver namespace and durable document-job formats. Runtime semantic-detail ABI 2 combines
the document/inference and graph inventories while preserving their common
prefix and testing the new extension boundary explicitly.
The complete main inventory precedes the graph extension. Main's semantic
detail identifiers, behavior codes, and metadata transition identifiers retain
their published values. Golden tests cover the endpoint at 781, document details
at 782 through 825, and the graph extension beginning at 826.
The metadata command frames preserve main's durable job tag 79 and assign
transaction sessions tag 81; main retains resolver advancement tag 80.
Encoder/decoder regressions distinguish the durable frames.

The same injected-five-millisecond probes now measure get-many for 8/16/64
owners at 6/12/57 ms, versus 50/95/390 ms before. Ordered first-page startup
is 12/25/109 ms versus 97/195/778 ms. These isolate scheduling and assert equal
results; they do not measure production storage or network latency. With one
4 KiB candidate per owner and a top-two result, sequential search peak memory
for 8/32/128/512 owners is 41/38/41/42 KB (decimal), versus approximately
79/207/605/2,420 KB before. The 128-owner query now succeeds under a 256 KiB
ceiling in both sequential and parallel modes.

The merged native metadata facade now exposes job lookup, paged scans and
per-owner counts through the compiled kernel projection channel. Checkpoint
and scan buffers transfer as owned opaque bytes, preserving binary values
and avoiding a second JSON encoding. A real compiled-owner regression covers
binary checkpoints, scheduled rows, quota transitions and invalid cursors.
Private failure ABI 68 preserves main's UnsupportedExactSort,
InvalidDocumentCheckpoint and TooManyActiveJobs identities at 838 through 840,
then appends graph identities at 841 through 904. This keeps expected document-job errors
distinct across the kernel boundary rather than classifying them as defects.

Retained-read ingress converts its clock-labelled transport deadline to the
native platform clock before binding an owner route fence. This preserves the
remaining budget across different clock epochs, including Darwin's awake and
monotonic clocks, rather than rejecting fresh graph admission as expired.
The regression uses distinct clock epochs and rejects an expired transport
budget.

SQL also preserves replicated owner-read admission failures as retryable
SQLSTATE 53300/HTTP 503 rather than internal XX000 errors. The diagnostic
regression covers unavailable reads, leadership changes and read-index
timeouts while preserving authorization, cancellation and unknown-write
outcomes. The authenticated verifier can restart explicitly read-only, idle SQL statements on retryable
53300 admission failures or 40001 snapshot restarts; mutation requests never
opt into that retry path.
The six qualification campaigns share a retry guard with three contract tests
covering nonadmission, stateless SQL reads, active transactions, malformed
responses and unknown mutation outcomes. Explicitly expected failures remain
observable by their assertions.
The policy verifier separately retries only explicit write nonadmission;
admitted writes resolve their durable receipts. Its four replicated postimage
checks pass, and a compiled-owner regression verifies that receipt probes
require configured authority. The mutation campaign allows 300 seconds for
32 checks and two independently bounded 60-second index-generation waits.

The preceding fanout milestone passed 484 native Cypher cases, 42 runtime error ABI cases,
nine private failure-identity cases, nine standalone SQL diagnostic cases,
and 1,310 SQL cases across both partitions (24 build/run steps). All 1,156
public Cypher/performance cases pass against the final shared library in one
complete run, including both storage modes and the membership, search,
SQL GRAPH_TABLE and quota regressions. The combined metadata, private
transport, retained-cut and graph-authority qualification passes 100 steps,
including 10 transport cases and three durable metadata cases. Two focused
compiled-owner runs each pass all 56 steps, covering binary job projections,
quota transitions, typed errors and policy verifier configuration. The
execution-budget gate passes all 67 steps and 14 cases. The final shared
library passes all 50 build/export steps, with exactly 137 public functions
and no internal exports; the production server/library/export build passes
all 88 steps. Generated Go SDK tests and the merged workbench build pass.
Authenticated qualification passes all 10 selected cases across the suite
and replicated-owner rerun. The latter completes all six campaigns, durable
receipt reads from every metadata replica, metadata-leader loss, duplicate
request-ID rejection across graphs, and receipt reads after node restart.


## Cohort finalization, selective writes, and SQL graph guards

Graph cut creation separates local native/route checks from quorum admission
validation. Production routed owners opt in to deferred finalization. Each
snapshot remains unavailable to graph operations and ordinary SQL cursors until
its validate operation succeeds while the complete mutation-fenced cohort is
held. This performs one frozen quorum proof per owner, replacing the remote
path's four capture proofs plus final cohort proof. Direct capture remains
validated before returning; custom sources retain explicit fence validation
unless they opt in to the finalization contract.

A reference-counted finalization capsule synchronizes validation with capture
retirement. Releasing the short fence makes unfinalized cuts unusable; snapshots
cannot dereference cancellation controls borrowed from a released capture.
The capsule uses the source's allocator, so freeing an independent short-fence
arena cannot invalidate a retained read or its eventual release.
Successful finalization publishes readiness atomically. Paging keeps the
independent route, native authority, expiry, and cancellation checks. Frozen
proof validation retains the existing fail-fast rule when Raft apply owns its
mutex; no validation waits for an apply blocked by the mutation fence.

The shared dependency planner now recognizes literal search targets, including
scoped table objects, and labeled CREATE, MERGE, SET, and REMOVE operations.
Nested patterns, relationship endpoint mappings, and UNION arms contribute to
the closure. Dynamic procedure targets, unlabeled patterns, and deletion retain
the full cohort. Deletion includes incident relationships, incoming producers,
and DETACH cleanup; native integrity coordination continues to expand physical
participants where schemas require it. Unresolved literal search names retain
runtime diagnostics through the conservative fallback.

HTTP and embedded hosts keep full catalog and permission validation, but activate
range tracking, capture owner cuts, collect conflict proofs, and submit guarded
commit participants only for the proved execution domain. Empty mutation plans
for uncaptured graph tables do not become participants. Read-only dependencies
remain guarded participants even if they produce no mutation. Embedded authority
leases and schema watches continue to cover every catalog mapping.

SQL GRAPH_TABLE uses the same closure, unions dependencies across graph aliases
and ordinary SQL tables, and explicitly opens a partial graph cut. Its binding
retains the complete authorization/schema inventory. Graph admission batches
unique logical tables in one metadata epoch. Page guards check graph incarnation
through the bounded indexed authority probe and verify all logical table IDs,
physical names, and schema/policy revision fences in one resolve-many batch per
graph. Graph aliases share the statement's authority channel; each RPC client
and request carries its own graph identity and revision. Custom catalog sources without
the probe use the original authoritative catalog read.

Every SQL page or optimized scalar/group/count operation has mandatory guards
before and after execution. Internal RPCs share that operation boundary; long
operations still refresh guards, and every checkpoint checks cancellation and
the live role revision before considering a cached guard. This avoids graph JSON
decoding and a separate linearizable metadata read for each table on every RPC,
while changes during an operation prevent its output from being returned.

Write replies use one shared typed flat envelope in HTTP and the embedded C API.
The serializer writes existing result fields and the typed commit receipt
directly, with identical wire representations. The bounded encoder counts exact
escaped bytes, allocates the final buffer once, and serializes into that buffer.
The precommit result-size check and receipt reservation remain before the durable
decision. No result JSON string, generic JSON DOM, or parsed receipt is retained
solely to attach commit metadata.

An equal-output serialization probe over the actual runtime Result and shared
commit envelope averages three runs per size. At 1,000/10,000/50,000 rows,
legacy serialization takes 381/4,942/20,402 microseconds versus 107/1,450/5,077
microseconds for the typed envelope. At 50,000 rows the 1,650,416-byte response
uses 53,439,410 versus 2,475,662 arena bytes (about 95% less). The probe includes
legacy result/receipt parsing and final encoding, asserts identical bytes, and
excludes query execution, commit, storage, and network latency.

Streaming cursor initialization also preserves graph-budget failure provenance.
Its owning wrapper translates failures before destroying the budget, including
failures while copying or planning the initial query. A configured graph memory
ceiling therefore remains SQLSTATE 54000; genuine allocator exhaustion remains
53200. The one-byte initialization regression and an allocation-failure sweep
cover classification and partial-state cleanup.

Statement cleanup qualification checks per-store `*.query-pins` generations.
The artifact inventory's reusable `native-query-links` caches intentionally
survive individual statements and can be created during index setup, before any
Cypher request. Those published caches are outside the statement-cut lifecycle.

The current qualification includes 486 native Cypher cases, 1,310 SQL cases,
50 runtime error ABI cases, and nine private failure-identity cases. Debug
qualification passes 238 local lake/SQL cases, both deferred graph owner cases,
and five focused host cases covering SQL graph joins, joined-table privileges,
exact schema normalization, constrained legacy replay, and admitted directory
ownership. Allocation-failure sweeps disable address-dependent heap remapping;
the legacy replay pressure check uses a cold cache, and directory workspace
limits account for fixed reader structures. The Python SDK's 99 cases, generated
Go SDK tests, and workbench build also pass.

The final stripped ReleaseFast server/library build passes all 88 steps and
the export boundary retains exactly 137 public functions with no internal
exports. All 1,158 embedded Cypher/performance cases pass against that shared
library; its SHA-256 remains unchanged through the final consistency build.
All 19 distributed, authenticated search, operator-limit, and statement-cleanup
cases pass in one complete run, including owner loss during index visibility
and the configured one-byte memory ceiling. Authenticated storage and owner-loss
verifiers share the bounded admission retry guard: only an explicit 503
`write_outcome: not_started` permits mutation replay. Expected failures and
admitted or unknown write outcomes remain observable. Its three contract tests
pass. Qualification retains the production disk-admission floor; obsolete
worktree build artifacts were reclaimed before the final campaign.

## Read operation guards, detached Lite reads, and typed coordinator transport

This integration merges `origin/main` through `7dabd028d6` before the read-path
changes. Public Cypher read execution now shares one authority scope across its
internal RPCs, with mandatory checks before execution, after execution, and after
bounded response encoding. SQL extends the same scope around statement cursor
construction, including eager GRAPH_TABLE execution when streaming cannot open.
The periodic refresh remains bounded to ten milliseconds; cancellation and live
role revisions are checked before considering a cached authority result. Write
execution retains its existing precommit validation boundaries.

Native Lite keeps short reads on the existing admission path. Once immutable
graph cuts are prepared, a read that runs longer than one millisecond releases
the connection mutex, path gate, and file writer lease at its next runtime or
response-encoding checkpoint. Short queries such as RETURN 1 avoid detachment
and its snapshot bookkeeping under contention. The store and file read-only flags are frozen before detachment, so retirement
cannot race read admission by changing them. An immutable root borrow
prevents refresh from reusing that root for a concurrent foreground request.
Instead, the owner installs a replacement root and retains the old root until
its last query, maintenance, or cursor borrower retires. This keeps old table
objects and index contexts alive across concurrent DDL and data publication.
The lifetime borrow is distinct from the immutability borrow: after execution
releases all leases, watches, and cuts, resume permits root reuse when there was
no overlap. Sequential reads therefore do not force a file reopen, and resume restores the
serving root's original read-only flags before allowing further operations.

Before delivering detached output, the C adapter reacquires current access and
checks the graph incarnation/revision, every mapped physical table identity and
schema version, and current raw-read authority for the root and all mappings.
An invalidated result buffer is freed before returning an error. Mutation
execution and directory-backed C handles keep their existing exclusive access.
The native fixture explicitly exercises sequential root reuse and overlapping
root retention across drop-and-recreate; public Python coverage races immutable
Cypher reads with SQL updates through shared and independent connections.

A graph RPC channel may now provide a typed in-process operation. The graph
statement coordinator uses the same owner validation, work meter, projection,
and read-field configuration for typed and serialized operations. Remote owner
calls continue to use the version-one JSON transport. The optional storage-read
callback goes through the existing checked runtime callback ABI; sources without
it retain the serialized fallback. Authenticated search preparation is shared
between both adapters. Projection and observation routing retain their dedicated
owner semantics.

Normalizers may return their input unchanged. Input nodes therefore use the
reply arena, so a typed reply stays valid after the owner function returns.
Owner replies decode directly into canonical transport values, avoiding a
generic JSON DOM for the common type-first node, relationship, list, and property
map envelopes. Structural discriminator keys can borrow the input during parsing;
returned strings and entity data have owned lifetimes. The parser preserves
signed integer precision, unsigned native timestamps, duplicate-field rejection,
and the value-depth bound. Scalar text requires JSON strings, even though the
generic Zig byte-slice parser also accepts JSON byte arrays. Reordered envelopes and path values retain the shared
version-one decoder, including path adjacency validation. The coordinator no
longer serializes and reparses an already-decoded distributed read reply.

An equal-output owner transport probe uses 128 identities with 1 KiB properties
and averages 100 repetitions per mode. One serialized owner hop takes 427 µs;
two serialized hops take 882 µs; a typed coordinator over one serialized leaf
takes 526 µs. All modes assert identical decoded properties and retain 172,176
bytes in the result arena. This isolates codec/coordinator work, excluding
network and storage latency; result-arena capacity does not measure transient
peak allocation.

Typed callback payloads use named request, ordering, filter, and budget-failure
types so independently compiled runtimes agree on the checked ABI contract.
A separate producer-library/consumer-test fixture verifies nested requests,
canonical node replies, budget failures, and transported errors in both Debug
and ReleaseFast builds with matching compilation profiles. Hosted read sources
forward the typed capability through their existing routing wrapper. Wire
serialization preserves nullable fields recursively: a terminal ordered cursor
step carries `after: null`, which must survive coordinator/leaf round trips.

An eligible immutable Lite read retries transient native WouldBlock/FileBusy
failures at the individual read operation for at most 100 ms. Each retry keeps
the same pinned cut and accumulated work/memory charges, and checks cancellation
and authority before sleeping for one millisecond. Exhaustion reports snapshot
unavailability. Mutation statements retain their existing failure behavior.

A local native concurrency probe starts a roughly 18 ms read, waits 2 ms, then
submits RETURN 1. Across 20 repetitions, median short-query latency falls from
13.9 ms to 2.46 ms on the same connection and from 14.0 ms to 2.59 ms through a
second connection to the same file. An independent file measures 0.21 ms.
The cooperative threshold preserves the short-query fast path. A separate probe
runs 1,000 calls per thread three times and measures median rates of about
14,100 queries/second serially and 11,000 with eight contending threads.
These are local microbenchmarks, not
end-to-end service throughput guarantees.

Statistic packet construction uses a reclaimed emission arena for integer bins,
large-count division and reducer argument evaluation. Reducers and escaping
cursor packets copy retained state into their own arenas. A 512-layer graph
with 32 grouped quadratic averages fits a 32 MiB core budget in either encounter
order, including an allocator that cannot grow allocations in place. The local
probe drops from roughly 43–46 MiB to 20–24 MiB without changing results or
work limits; allocation-failure campaigns cover cleanup and packet ownership.
The existing native workload also passes in both storage modes: grouped AVG
reports 22,618,302 bytes for Lite and 21,757,102 bytes for directory storage,
versus the earlier 45,974,146 and 61,136,686 bytes. Both still scan one anchor,
explore 4,088 edges and charge 144,840 intermediate states.

Completed qualification includes 491 core Cypher tests, five native C-adapter
fixtures, the independently compiled callback ABI fixture in Debug and
ReleaseFast, 11 server transport tests, the checkpoint-pinning regression,
and 13 authenticated-search/limit scenarios. The optimized library and CLI
build successfully; the native export boundary retains 137 public functions
without internal exports. The full optimized embedded run passes 1,513 cases
with four skips. All 20 standalone/replicated-owner scenarios pass against the
final optimized CLI, preserving the production disk-admission floor.

Managed inline embedders implicitly consume an artifact with the index's name.
Aggregate readiness retains that source's publication and producer-completion
fences from every expected owner, including when the vector target is empty.
Zero cardinality cannot erase pending source work. A serving snapshot may remain
queryable while this completion work proceeds. Fault-injection setup retries
only definite seed-batch prepare aborts; admitted or unknown mutations retain
their original transaction identity and are never replayed as new writes.
Standalone native owners retain their local producer and coverage protocol.
Only Raft owners install the ordered-publication dispatcher: that dispatcher
requires committed Data-Raft entries and cannot complete native publications.
Empty-index end-to-end checks retain the implicit source and require its
completion proof in both aggregate and per-owner readiness before seeding data.
A fresh local store may lack its cardinality record until the first primary
write. Local completion then uses one key-only seek in the same pinned source
revision to prove emptiness. Any user key keeps a missing counter unknown;
ordered owners continue to require the producer registry. Concurrent counter
creation cannot change an older snapshot's proof.

Native owner capture shares catalog admission with long-running searches. A
separate file-pinning gate excludes completed dense checkpoint handoffs and
shared vector-generation replacement while capture selects manifests and
acquires hardlinks or stable WAL descriptors. Checkpoint builders still run
outside this gate; completed publishers defer without blocking. Capture waits
honor cancellation. A deterministic regression attempts publication between
manifest selection and file pinning, then seals the old manifest after the
new checkpoint has retired its live WAL path.


### Typed local leaves and demand-scoped owner cuts

The production local-owner chain now forwards typed graph calls through the
statement source, retained route wrapper, checked runtime callback boundary and
native snapshot. Native wire calls adapt the same typed operations; observations
keep their separate wire reply contract. Normalize/project replies allocate their
nodes and value arrays in the reply arena. Both transports share route validation
and the frozen-cohort finalization handshake, including before/after authority
and cancellation checks. Remote leaves retain their bounded JSON transport.

Read-only physical planning collects adjacency index names across all patterns,
including reverse/wildcard edges, expression patterns, subqueries and UNION arms.
Each physical table receives its own set. SQL GRAPH_TABLE unions those sets across
all graph relations in the statement. Procedures, writes and transactional SQL
retain conservative complete capture. Primary rows, scalar layouts, catalog,
schema, policies and authoritative out-of-line source payloads remain retained.

A scoped owner cut synchronizes, checks readiness/repair, pins and opens only
selected managed index assets. Its full immutable index catalog remains available
for graph identity validation, so omitting an unused graph projection does not
weaken catalog binding. Primary-only cuts can avoid unrelated producer failures;
selected graph cuts conservatively retain producer-recovery health checks.
Manifest validation binds the exact index selection, preventing a scoped cut
from reopening with broader asset authority. Scoped cuts never advertise a
complete-corpus reuse proof. Ordinary backup and resumable search captures keep
the existing full-generation behavior.

Typed runtime callbacks carry separate reply and reclaimable scratch allocators.
Each native owner constructs its scratch arena locally, so per-document decoders
can recognize and peel its arena vtable even when the caller belongs to another
compiled runtime. Only canonical reply values/cursors are cloned into the caller's
reply arena before owner scratch is released. This preserves payload pruning for
wide schemaless documents without a JSON codec hop. The independent-runtime ABI
fixture repeatedly decodes 8 MiB through a 1 MiB scratch buffer and overwrites that
buffer before checking the reply, guarding both reclamation and reply ownership.


Qualification for typed local leaves and scoped owner cuts: 492 optimized core
Cypher tests pass, together with the independent-runtime callback fixture in
Debug and ReleaseFast. Four native capture/SQL/cleanup fixtures and 12 transport
regressions pass, including the legacy wire allocator guard. The complete native
Python suite passes 1,513 tests with four skips. All 20 public end-to-end tests
pass, including authenticated replicated owners, graph writes and recovery,
SQL GRAPH_TABLE, semantic/hybrid search, operator ceilings and the wide-document
8 MiB memory regression. The final optimized CLI/library build passes all 87
steps; the library exports exactly 137 public C functions and no internal symbols.

### Computed key access and catalog validation epochs

Read-only document-key and element-identity access hints can evaluate bounded
deterministic scalar expressions over existing input bindings, including string
concatenation and scalar conversions. The evaluator is separate from allocation-free constant
folding: normal query memory/work limits and cancellation apply. Candidate-bound
expressions, graph reads, aggregates and unrecognized calls remain residuals.
Invalid scalar hints fall back to ordinary evaluation, preserving errors on
nonempty inputs and their absence on empty inputs. The complete predicate remains
authoritative after every point read. Finite IN/OR domains use the same evaluator.

Native owner snapshots bind every allowed immutable graph once at creation.
Subsequent operations select those validated bindings without reopening or
rebinding index metadata. Live validation deduplicates physical graph indexes and
caches successful identity proofs per graph against the source index manager's
identity and catalog publication generation. Epoch reads synchronize with apply
and catalog publication; validation fences both sides of a cache miss. Catalog
reload and restore attempts invalidate the generation, including failed attempts.
A drop/recreate or configuration change cannot reuse an earlier success. Policy,
deadline and final statement checks remain independent of the metadata cache.


Qualification for computed keys and catalog epochs: all 494 optimized core
Cypher tests pass, including scalar-error timing, NULL and OPTIONAL behavior,
finite IN/OR domains, shared scalar signatures, and entity-bearing input-map
fallbacks. Three native owner fixtures pass, including live index retirement and
same-name recreation. The public authenticated access-path/SQL-overlay campaign
passes with computed 400-key equality, IN and coalesce predicates capped at 400
anchors and 50,000 states. The Go SDK suite and 544 TypeScript SDK tests pass
(one TypeScript skip); the rebuilt Antfarm bundle and all 71 optimized CLI build
steps pass after merging origin/main.

Local API-key probes over 2,000 documents, with seven warmed samples per case,
reduce the two-computed-key workload from 4,000 anchors / 92,026 states to two
anchors / 84 states (33.9 ms before, 14.7 ms after). The 400-key lookup with 64
unused mappings falls from a 115 ms median to 33.9 ms; the equivalent zero-mapping
case measures 35.5 ms. Computed 400-key equality now succeeds with 400 anchors and
15,606 states, rather than exceeding the default state ceiling. These timings
are local diagnostic measurements; the scan/work ceilings are regression-tested
independently of wall-clock latency.

### Computed property domains and statement-local storage roots

Property equality, pattern-map equality and finite property membership now use
bounded, deterministic scalar access hints over existing input bindings. They
share the document-key hint evaluator and preserve the full Cypher predicate as
an authoritative residual. Allocation-free constant folding remains separate.
Candidate-dependent expressions, entity reads, volatile calls and failed scalar
evaluation fall back to ordinary execution. Query work, memory, depth and
cancellation limits still apply to hint construction.

Finite key/property domains also recognize graph-free list comprehensions,
`coalesce`, and `reverse`. Collection eligibility is depth/work/cardinality
bounded before materialization; comprehensions prove their predicate and
projection with the iteration variable shadowing any outer binding. Computed
key domains enter the existing deduplicated, paged `get_many` access path.
Membership and null/error semantics remain those of the ordinary evaluator.

Selected, non-resumable graph reads retain immutable primary, scalar-index and
outgoing/reverse graph roots under the distributed statement capture cohort.
They release the cohort before row execution and fork the captured roots for
independent page readers. These reads do not sync, seal, publish or reopen a
physical query generation. Search/procedures, mutations, serializable SQL range
tracking and explicitly durable consumers retain the physical generation path.
Both implementations share graph catalog guards, deadlines, population bounds,
authorization and typed RPC dispatch.

Pinned relational readers bind captured scalar-index definitions and row-policy
phase/generation/epoch to authenticated typed sessions. Document scans reuse the
native projection/filtering path over the retained transaction, and document
point reads project stored byte spans before constructing requested properties.
TTL uses the statement cohort's wall time. SQL child cursors retain their parent
owner independently, so closing the graph handle cannot invalidate SQL rows.

Graph store ownership has a separate reference-counted lifetime. Catalog removal
moves physical cleanup to the last reader instead of waiting under apply/catalog
locks. Captured roots survive catalog-array relocation; live identity guards
reject drop/recreation before further reads. This prevents a DDL/read lock cycle
while preserving immutable adjacency and writer progress.

Indexed typed-session range bounds resolve against the same captured scalar
catalog as their row reader. A live drop/replacement cannot substitute a later
column type or make an already captured index disappear during deferred cursor
construction.

Retired graph namespaces remain fenced through the backend's final close,
including deferred flushes. Index cleanup yields while a retained reader still
owns that namespace; same-name admission stays behind the existing durable
cleanup tombstone. Canonical pruning and orphan shadow-root collection also
respect retained graph roots. This prevents an old backend from flushing into
a deleted or recreated namespace, while index removal itself does not wait for
query readers under apply/catalog locks.

Authenticated adjacency pages validate the principal lease before and after
reading the captured graph roots. Producer and endpoint visibility still pass
through the ordinary per-entity policy projection. SQL transaction overlays can
normalize supplied postimages and derive their field edges using guarded schema
metadata without reading live documents or adjacency. Failed durable-cut setup
closes the reader before reclaiming its physical generation.

Qualification for computed domains and statement-local roots: all 496 optimized
core Cypher tests pass. Four native fixtures pass with no leaks across all 58
build steps, exercising durable and pinned cuts, staged postimage projection,
captured visibility, child SQL cursor lifetime, scalar catalog replacement and
graph namespace retirement/recreation. The storage compilation ownership audit
passes all 30 tests. The Go SDK suite, 547 TypeScript SDK tests (one skip), 304
Antfarm tests and 15 CI contract tests pass after merging origin/main; the
Antfarm production bundle was rebuilt from the merged source.

The optimized production CLI passes all 71 build steps. The public campaign
passes 18 cases on its first run; the broad property/SQL-overlay case passes
after making index readiness an explicit fixture precondition before the
64,000-row ordered-read workload. Fast statement capture does not drain index
construction jobs. Readiness probes alone tolerate bounded coherent-read
admission contention; ordinary query assertions still fail immediately.

The remaining owner-loss case passed its isolated rerun and four additional
fault injections. Its first campaign run timed out with a durable
`committed_repair_required` receipt instead of reaching `committed`. That
intermittent result remains unexplained; neither the recovery assertion nor its
deadline was relaxed, and the successful reruns do not establish that the
intermittent failure is fixed. All 20 distinct public cases have passed against
the final binary across these runs, including authenticated replicated queries,
graph mutations/recovery, SQL overlays, managed search and operator ceilings.

Local API-key measurements use a standalone server, a relational table with
2,000 rows and an `id` scalar index, three warmups and seven timed requests per
case. Responses are checked against exact sums and explicit anchor/work ceilings.
Before this change a document-key point query measured 15.041 ms median; with
statement-local roots the final binary measured 0.984 ms with the same one anchor
and 30 states. Computed two-key property equality measured 1.123 ms, two anchors
and 102 states; computed two-key document membership measured 1.096 ms, two
anchors and 150 states. Computed 400-key property equality, pattern maps and document membership
used exactly 400 anchors and respectively 19,206, 17,606 and 26,816 states, within
the 50,000-state regression ceiling. Their medians were respectively 18.579,
18.435 and 10.769 ms. Timings are local diagnostic observations,
not latency guarantees; the public regression tests enforce deterministic
results, work/anchor ceilings and bounded memory independently.

### Bounded sparse native seeks and correlated lookup batches

Certified property membership domains retain one deduplicated statement-local
list (up to 4,096 values). The first physical-owner request includes every
predicate hint. Normally it includes the complete domains. If that would exceed
the 1 MiB graph packet ceiling, a bounded planning handoff instead carries each
large certified domain's length, maximum string byte length, and three cost
samples. The client counts encoded bytes without allocating the full packet,
including projection names; the first planning call passes with a 256 KiB scratch
ceiling. No partitioned query rows escape that handoff. The owner compares
usable IN prefixes and the SQL planner's ordinary
fixed-predicate access path before emitting rows. Bounded samples examine at most
three branches and eight records per branch, with at most four indexes per domain
and 32 IN plans. This is bounded approximate costing, not exhaustive optimization.
Fixed equalities may precede the varying IN component in a composite prefix;
partial indexes must independently prove their predicate from fixed conditions.
An unusable IN hint does not discard a selective equality or range access path.
Large integer equality on DOUBLE remains indexed when the integer round-trips
exactly through binary64. Its SQL bound retains binary64 semantics; an inexact
integer remains residual without poisoning other selective conditions.
Empty domains return without scanning, independently of the schema scalar type.

A sampled string domain validates the actual fixed prefix plus a worst-case
NUL-escaped string bound before any indexed emission. Numeric certificates retain
the complete-domain DOUBLE compatibility proof. Unsupported hosts pin one primary
scan rather than treating samples as the domain. This preserves the original
paged transport envelope for large computed string domains.

The continuation pins the captured index, schema and selected domain. Subsequent
physical-owner requests send at most 128 values from that domain, retaining the
global offset and native cursor. A chosen fixed-predicate path may omit unused IN
hints from subsequent owner requests; the runtime retains the authoritative full
Cypher predicate. Coordinator channels leave leaf continuations opaque until
every shard completes. This prevents mixed indexed/primary shards and alternate
access paths from truncating or duplicating results. Native pages consume multiple
equality branches within row/work demand and reuse one captured typed session,
schema, policy and projection plan. Reader arenas reclaim bounds between branches.
Composite suffixes, direction, TTL and catalog generations retain SQL visibility.

The domain certificate describes the complete original list, including integers
outside DOUBLE's exact range. Before first emission, the owner validates every
bound of a complete chosen domain, or its conservative global bound proof after
a sampled handoff. Incompatible or oversized bounds decline that plan
before any rows escape; other usable access paths remain candidates. Primary
fallback stays primary until completion. Unproved lists retain bounded null
removal/deduplication and residual evaluation. Native seek and costing work are
charged; no owner-side domain cache or cross-request lease is introduced.

Native `get_many` now groups the bounded input by physical table and shares its
read authorization, captured transaction, schema and projection plan. Relational
batches use one authenticated typed reader and the SQL reader's sorted storage
multi-read, with per-key TTL and row-policy checks. Reclaimable source scopes
retain one run/index decode per chunk and release buffers before retrying a
smaller batch; large unrequested columns cannot accumulate beside reply storage. Document batches project stored spans before parsing requested content.
Durable query generations capture one transient local read cut per batch at
the statement wall time; embedded Zig and C API hosts forward the same bounded
callback, including the C API's existing authority-retry behavior. Results remain
aligned with the input, including duplicate, missing, hidden and invalid identities. Schema, cancellation, deadline and authority checks surround
each batch; output owns its bytes independently of temporary readers.

Full consumers of `UNWIND ... MATCH` can prefetch deterministic row-bound property
equalities in bounded windows. A conditional indexed probe fetches candidates and
proves exact EOF in one operation, capped at 128 records plus one overflow witness.
Unindexed paths decline without a corpus scan. Each leaf has a 1 MiB probe quota;
the coordinator has a 2 MiB aggregate quota and proves completion across trailing
empty owners. Transport scratch uses the statement budget independently of the
optional cache quota, so owner work is consumed before cache admission can decline. Unsupported hosts retain the estimate/fetch compatibility path.
Incomplete or quota-declined probes are advisory work: owner work remains charged,
but their results supply no bindings or logical anchors. Admission completes only
after the full candidate cache and encounter-order index fit. A quota-owned cache
retains only matching candidates and replays each equality bucket in
native encounter order for the original outer inputs. Duplicate outer bindings,
nonunique properties, OPTIONAL null extension and sequential aggregates retain
their ordinary semantics. The original MATCH predicate remains authoritative.
Unsupported proofs and cache quota exhaustion decline to the ordinary operator,
with performed work still charged. Saturated estimates decline; estimates never
supply query rows or EOF. Empty terminal probes can finish trailing owners at
an exact anchor ceiling without refunding any observed records. Finite row
goals, general graph joins and
nested continuations retain demand-driven or weighted execution.

The graph catalog readiness action now probes `MATCH ()-[r]->() RETURN r LIMIT 0`.
This requires every mapped physical relationship projection during snapshot
capture without enumerating graph rows. A node-only probe intentionally has no
physical relationship dependencies and cannot establish projection readiness.

Qualification for bounded native batches: all 501 optimized core tests pass,
including mixed indexed/primary owners, RPC fallback continuation, membership
boundaries through 4,096 values, global integer precision evidence, duplicate and
OPTIONAL correlated inputs, independent-join eligibility, finite row goals and
empty terminal probes at an exact anchor ceiling. All four native retained-cut
fixtures pass with no leaks across 58 build steps, including durable and pinned
key batches, duplicates/missing identities, captured visibility and scalar-prefix
reuse after a live catalog drop. The storage compilation harness passes all 30
cases; the expanded post-merge harness passes all 31. The Go SDK suite,
547 TypeScript SDK tests (one skip), 304 Antfarm tests
and four new main CI contracts pass. Main is merged through `207cd9ca3b`; the
rebuilt Antfarm assets match the bundled assets byte for byte.

The membership regression uses a 2,000-row indexed table and explicit anchor
ceilings. At 256 distinct values, measured runtime work falls from 82,191 to
16,913 states. The 257- and 400-value cases now visit exactly 257 and 400
anchors, respectively, instead of falling back to 2,000 primary rows. Correlated
equality tests assert bounded bulk reads, zero scalar owner probes, duplicate
outer-input replay and native encounter order. These are deterministic work
improvements. A subsequent warmed public-API measurement of the sorted multi-read
binary produced the following local medians (three warmups, seven samples):

| Query | Earlier median | Batched median |
| --- | ---: | ---: |
| Property membership, 256 values | 33.965 ms | 5.900 ms |
| Property membership, 400 values | 15.424 ms | 8.746 ms |
| Document-key membership, 400 values | 11.941 ms | 7.070 ms |
| Correlated property equality, 400 inputs | 20.055 ms | 11.694 ms |
| Document-key point lookup | 0.984 ms | 0.996 ms |

Before conditional probes, the correlated case charged 38,014 states versus
the earlier 19,206: its bounded index proof and candidate fetch performed
separate reads, while bulk requests replaced individual owner probes. The earlier timing campaign was
noisy under concurrent compilation, so these separate-run medians are diagnostic
observations, not controlled latency guarantees. Exact sums, work ceilings,
128/256/257/400 membership boundaries, incompatible-domain primary fallback,
large-integer precision, wide projections and SQL overlays also pass through the
public API with the sorted multi-read implementation.


On macOS, embedded-only test composition uses the same native final-link path
as the main composition. Relocatable Zig/LLVM objects can lose `N_ALT_ENTRY` on
exported aliases inside merged data aggregates. The native linker restores those
alias flags in an adjacent staged entry object, preserving base-relative private
members while keeping code/data dead-stripping enabled. The compiler object is
unchanged and the staged object remains beside the executable for debug maps.
Six binary-format tests cover public/private aliases, ordering, idempotence, ordinary objects,
malformed input and staging; the corrected embedded gate passes 125 tests.

Post-merge qualification also passes the 71-step CLI build, the 66-step/125-test
embedded gate, 501 core tests, four native graph fixtures without leaks, all 31
storage compilation contracts, 17 build-wrapper tests, 210 PR/CI contracts and
five SDK CI contracts. The temporary source-guard diagnostics are removed from
the implementation.

Follow-up qualification for owner-selected access paths and conditional probes
passes 504 optimized core tests, the independent callback ABI contract, and five
native retained-cut/SQL integration tests across 58 build steps with no leaks.
The new native fixture uses 2,000 rows and competing indexes, verifies selective
one/two-anchor plans in both predicate orders, checks empty/null membership, proves
probe EOF at an exact budget, preserves owner work failures, and resumes all 400
original values after a sampled planning handoff. Wire tests keep 400 long string
values within the existing 1 MiB packet ceiling and pin unsupported-host fallback
to its primary cursor.

Authenticated HTTP follow-up passes all reproduced failures with strict anchor
ceilings: competing bucket-IN/id-EQ scans fall from 2,000 anchors to one; either
ordering of selective id-IN and broad bucket-IN visits two; empty/null domains
visit zero; an unknown IN property preserves the one-anchor id equality path.
The 1,000-input correlated query returns the exact sum 499,500 with 1,000 anchors
and 93,045 states versus the earlier 95,038 states. Its initial local time was
25.125 ms versus the earlier 34.954 ms. The full authenticated property-plan and
SQL-overlay regression, including lossless large-DOUBLE equality, passes in
24.02 seconds.

A subsequent warmed run uses three warmups and seven samples per case:

| Query | Previous batched median | Owner-planned/probe median |
| --- | ---: | ---: |
| Property membership, 256 values | 5.900 ms | 6.156 ms |
| Property membership, 400 values | 8.746 ms | 10.332 ms |
| Document-key membership, 400 values | 7.070 ms | 7.335 ms |
| Correlated property equality, 400 inputs | 11.694 ms | 9.633 ms |
| Document-key point lookup | 0.996 ms | 0.921 ms |

These are diagnostic local observations under background compilation, not a
controlled latency comparison. Membership retains its exact anchor ceilings;
the 256-value case charges 16,914 states (previously 16,913), and the 400-input
correlated case charges 37,221 (previously 38,014). Full-domain bound validation
precedes indexed emission, preserving correctness when a later key is oversized.

The follow-up is merged with upstream main through `b6e576b8e5` in
`1040e6c0a8`. The merge preserves adjacency source touches using upstream
`MutationWriteBuffers` ownership and regenerates the Go OpenAPI client and
dashboard assets from the merged sources. Go SDK tests pass; the dashboard
passes all 304 tests, and the TypeScript SDK passes 547 tests with one
intentional skip. The 29 build/export contracts also pass, including all
21 nested build-memory policy cases and executable/shared-library linkage.
The rebuilt merged CLI passes all 12 strict authenticated HTTP probes and
the complete property-plan/SQL-overlay regression in 26.20 seconds.
Competing predicates retain their exact one/two-anchor ceilings, empty
domains visit zero anchors, and both 1,000-input correlated forms return
499,500; the fused probe form charges 93,045 states.
Merged native qualification succeeds in all 148 build steps: 165 graph
write/recovery cases, five retained-cut/SQL integration cases, 13 transport
cases (including the staged bulk-read contract), the portable embedded host,
and the C API catalog/execution gate. All reported native cases have zero
leaks. The unchanged 504-case core suite and independent callback ABI gate
reuse their successful cached results. The merged CLI build also succeeds.

The next access-path follow-up merges main through `e5aad86397` in
`07695c25b3`. Main's appended runtime detail values remain unchanged; Cypher
additions follow them in both error domains, with explicit numeric contracts.

Sparse owner continuations now budget encoded bytes, including escaped native
cursors, table names and projected fields. Windows contain at most 128 values
but can be smaller for long strings. Unused IN hints become inactive operands
without renumbering the pinned owner's filter ordinals. Large derived domains
use bounded planning samples even below 128 values; hints that cannot fit even
then decline to one pinned primary scan before any rows escape. Typed filter
conversion borrows immutable payloads instead of serializing and reparsing them.
Composite prefix ordinals are copied across callback ownership boundaries.

Native composite access paths combine up to four primitive IN columns in one
mixed-radix prefix plan, capped at 128 combinations and 256 KiB of retained
request hints. Fixed equalities can occur between the varying keys. Costing
compares these paths with the existing scalar and fixed-predicate paths; every
chosen bound is validated before output. Continuations pin schema, index,
filter ordinals, dimension lengths, tuple position and native row position.
Larger products keep the existing bounded single-domain plans and residual
Cypher checks. The regression fixture has only a composite `(tenant,id)` index:
two tenant values and two identifiers previously visited 1,000 anchors for two
results; the new contract permits exactly two anchors across four tuple seeks.

Adjacency reads use retained source-owner ranges. Outgoing traversals route by
the known source identity; pair-restricted incoming traversals route by their
known peer source. Undirected pairs use the union of possible source owners,
and unrestricted incoming traversals still visit every possible source owner.
Legacy capabilities without certified ranges retain their original fan-out.
Continuations skip only owners proved irrelevant at the retained statement cut.

Correlated probes share the bounded owner-read scheduler. Remote waves use
isolated work meters and join before copying results or reclaiming scratch;
local capabilities keep controller-thread affinity. Complete leaves reserve
their exact populations before a skewed incomplete leaf retries against the
remaining global population allowance. Discarded attempts remain charged as
work, never query bindings or logical anchors. Encounter order, trailing empty
EOF proofs, cancellation and the 2 MiB aggregate capture quota are preserved.

The access-path qualification passes all 507 core Cypher tests and the callback
ABI gate, plus 13 transport, five retained-cut/SQL, five local C API, one portable
embedded and 52 independent runtime-boundary cases. Reported native suites have
zero leaks. Focused debug checks cover bounded long-string packets, composite
cursor ownership, source-owner routing and all 12 parallel-probe outcomes. The
merged Go SDK passes its complete test suite. The merge also adds the scoped
`objectstore` dependency to inference source owners: upstream GCS credential
configuration now names that module directly.

The corrected merged CLI and combined native gate succeed in all 147 build
steps. The expanded authenticated property-plan/SQL-overlay regression passes
in 30.73 seconds, including the composite two-anchor ceiling and both derived
long-string membership cases. The replicated-owner regression passes in 286.83
seconds, covering HTTP queries and writes, catalog revisions, expressions,
mutations, normalization and postimage authorization. These runs exercise the
rebuilt executable; they do not rerun or clear the enrichment fault-campaign
caveat below.

Main advanced during qualification and was fetched and merged again through
`c0705f1264` in `5d49f54003`. The browser merge retains both the Cypher module
and upstream's shared inference schema import. All 37 source-boundary and
local-test-ownership contracts pass on this snapshot.
The rebuilt merged CLI again passes both public regressions: the expanded
property-plan/SQL-overlay case in 29.17 seconds and the replicated-owner
qualification in 164.18 seconds.
The combined native gate also succeeds in all 147 steps on this merge,
including the portable embedded host; all reported native cases have zero
leaks. Unchanged core and callback ABI test results are reused from the
successful access-path qualification above.

The distributed enrichment caveat above remains open. Two combined public
campaigns passed 19 cases but the owner-loss case ended with a durable
`committed_repair_required` receipt; all three data owners exhausted retries on
`EnrichmentSourceChanged`. A subsequent campaign again passed the other 19
cases, but failed earlier during fixture seeding with
`transaction_outcome_unknown`, before fault injection. Four isolated owner-loss
runs against the batched binary passed. Those passes do not establish a fix for
the campaign failure. Source-guard instrumentation did not implicate the
producer-input generation comparison; no publication fence, retry-safety rule,
test assertion or deadline was weakened. This is not a completely green public
campaign, and its underlying catalog/enrichment recovery cause still requires
diagnosis.


The SQL staged graph view also forwards `get_many` to its transaction overlay.
Mixed staged, deleted, missing, duplicate and untouched identities retain input
alignment while untouched identities share one bounded owner request.


The frontier/aggregate follow-up merges main through `0d5de07465` in
`f0202d494e`. Upstream Pioneer runtime detail IDs remain 833–835 and the
unmerged Cypher additions follow at 836. Standalone uses upstream's common
trusted-principal resolver, with paired issuer/secret validation.

Source ranges certify document placement, not relationship placement. Only an
explicit retained backend certificate licenses source-owned adjacency routing;
ordinary native and producer-backed indexes conservatively retain all possible
source-table owners. The regression retains an artifact edge with source `a`
and producer `z` across disjoint owner ranges, including batched first pages
and a continuation after a nonempty earlier owner.

Correlated membership prefetch now divides supported population or cache-quota
declines into bounded left-to-right subwindows. Unknown capabilities still
fall back directly, and singleton declines keep the scalar executor. A
127-key/two-matches-per-key regression replaces 127 scalar scans with three
probes; duplicate input order, eager expression evaluation and discarded work
accounting remain intact.

Private adjacency batches carry at most 16 identities and one MiB of admitted
first-page payload. NULL slots mean cache decline, never EOF. Physical scans,
including discarded payloads, remain charged. Native owners enforce per-page
record allowances; distributed batches group identities by immutable owner and
retain original owner encounter order and native continuations. Full-closure
reductions prefetch only already-required bounded or unbounded frontiers, with
memory/work/edge headroom and existing eight-MiB cache admission. Row-goal and
ordinary demand-driven paths retain scalar reads. Oversized relationship data
remains readable through the scalar path. Frontier regressions preserve exact
path counts and cut adjacency RPCs by more than half.

Exact owner-side `COUNT(*)` uses the original single-node Cypher predicate,
parameters and logical label, not SQL admission hints or population estimates.
The same evaluator performs authorization, TTL and field projection at each
retained owner. Direct native execution retains its existing evaluator; owner
partials replace row transport only at coordinators and RPC clients. Only row-local primitive predicates are licensed; optional
matches, DISTINCT, grouping, projection windows, nested patterns, writes and
small-memory invocations retain the existing executor. Partial counts carry
exact examined populations; the coordinator checks and sums them within the
statement anchor limit. Nested evaluator work forwards to the original owner
meter even on failures. Wire tests verify NULL parameters, zero row-scan RPCs,
multiple owners, state/anchor exhaustion and complete callback cleanup.

Final qualification of this merged follow-up succeeds in all 147 build steps,
with all 514 core Cypher tests and the callback ABI gate passing. The selected
native suites pass 13 transport, five retained-cut/SQL, five local C API, one
portable embedded and 52 runtime-boundary cases, with zero reported leaks.
All eight focused debug tests, the complete Go SDK suite and all 37
source-boundary/test-ownership contracts pass. The rebuilt CLI passes the
expanded authenticated property-plan/SQL-overlay regression in 34.33 seconds
and the replicated-owner qualification in 161.72 seconds. The nonunique
127-key public regression counts 254 matches while visiting exactly 254 anchors.
The enrichment owner-loss campaign caveat recorded above
remains open; these gates do not rerun or establish a fix for that campaign.

The owner-wave follow-up merges `origin/main` through `7b9245d250` in
`62bef6bf40`; the generated Go SDK spec is regenerated from the merged OpenAPI
source. Exact count partials and adjacency frontier batches now share the
bounded owner-read scheduler. Remote waves respect configured width, local
capability thread affinity, statement cancellation and retained-cut checks.
Every started call joins before scratch is released. Isolated work reservations
are charged before skewed owners retry; discarded count/edge reservations and
speculative adjacency pages remain accounted. Adjacency results are consumed in
original owner order, preserving first nonempty pages and native continuation
ownership. Payload admission remains one MiB for the returned batch.

Retained physical graph roots now supply a conservative endpoint directory.
Two boundary seeks in each outgoing/reverse root produce inclusive endpoint
envelopes without scanning rows or changing persistent write metadata. Producer
ownership records may widen an envelope; logical endpoints outside primary
shard ranges remain included. The directory is captured during the existing
owner-fork wave and lives exactly as long as that owner's retained capability.
Routing consults these graph envelopes before document-placement certificates.
Unknown/legacy capabilities keep broadcast routing. Empty proofs exclude empty
owners; broad or overlapping envelopes retain false positives. Durable managed
cuts derive the same proof from their immutable physical copies; mutable DBs
return unknown rather than consulting live graph roots.

In the same eight-owner fixture with a 20-ms delay per owner and fanout width
four, count latency drops from 175 to 53 ms and empty adjacency from 176 to
50 ms. These are synthetic transport-latency measurements, not production
throughput claims. Regression coverage includes skewed reservations, work
charging, revocation, controller-thread affinity, speculative edge accounting,
original owner ordering, binary endpoint keys, producer placement and cloned
routing-proof ownership.

Advisory adjacency waves also decline on concurrent scratch quota pressure.
All jobs join and retain work/physical reservations before returning NULL cache
slots, allowing the scalar executor to continue after scratch is released.
An authorization, cancellation or other owner failure takes precedence over a
memory decline. Copy admission follows the same rule. Focused tests combine a
128-KiB scratch quota with four simultaneous 256-KiB attempts, and separately
combine quota failures with a forbidden owner.
