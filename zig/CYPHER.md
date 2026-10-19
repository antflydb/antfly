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
individual marginal statistics; it never substitutes for an observable path.

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

Native and SQL scalar programs share checked integer affine lineage for unary
signs, addition, subtraction and multiplication by constants. Every intermediate
must remain affine in one marginal. Representative bags include both extrema,
so the ordinary scalar evaluators still detect intermediate BIGINT overflow,
including when a later operation cancels it. Native SUM also requires a
one-sign argument to preserve its contribution-order overflow semantics; mixed
signs retain the ordinary executor. Nonlinear and cross-marginal expressions
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
allow one-sign SUM consumers; nonlinear, cross-marginal and distribution
observers still require exact values.

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

Length predicates, length grouping keys, non-extremum DISTINCT arguments
and other distribution-dependent expressions retain exact hop states. Cyclic
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
