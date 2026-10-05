# SQL compiler boundary

This is an executable, bounded subset of SQL extraction, not a claim of parity with the
combined SQL branch. Parsing a statement does not imply the current backend can
execute it: catalog binding and backend capabilities must admit the complete
statement before any mutation.

## Compiled semantic forms

- Tableless and relational-source `SELECT`: expressions/`AS` aliases, `*`, or
  `COUNT(*)`; typed comparisons, `IS [NOT] NULL`, `AND`/`OR`/`NOT`, IN/BETWEEN,
  arithmetic, casts, CASE, registered scalar functions and JSON extraction.
  Ordering supports source columns, aliases, ordinal positions, expressions,
  ASC/DESC and NULLS FIRST/LAST; LIMIT/OFFSET accept nonnegative integers/parameters.
- Inner/outer joins with source aliases, derived tables and nonrecursive CTEs;
  grouping, aggregate FILTER/DISTINCT, HAVING, and bounded aggregate ordering.
- Linear recursive CTEs with seed-typed outputs, delta worklists, UNION ALL or
  typed distinct visited sets. Physical inputs share one statement capture;
  static-side hash indexes are reused across iterations. Work, cancellation
  and retained-memory quotas also bound nonterminating recursion.
- Window ranking, offset/value functions and aggregates with PARTITION BY,
  ORDER BY, peer-aware RANGE, ROWS and GROUPS frames and frame exclusions.
  Query-local WINDOW definitions support checked inheritance and sort sharing.
  Equal sort domains share
  sorting; moving aggregates use bounded indexed state. Window evaluation runs
  after grouping/HAVING and before final ordering/limits under the same budget.
- Equality-correlated EXISTS/NOT EXISTS and scalar subqueries, including direct
  and composed scalar aggregates, lower to grouped hash joins rather than per-row reads.
  All physical tables participate in the same authorized statement capture.
  Scalar subqueries preserve zero-row NULL and SQLSTATE 21000 for multiple rows.
- Scalar IN/NOT IN subqueries use grouped hash membership and per-correlation
  total/non-null evidence, preserving empty-set and SQL NULL semantics without
  duplicating outer rows. Two bounded inner projections share the statement
  capture; execution never opens one native query per outer row.
  Side-local computed equality expressions are compiled hash keys too, so
  expressions such as `o.x + 1 IN (SELECT i.y + 1 ...)` do not degrade to a
  quadratic residual join. Set `ANTFLY_SQL_MEMBERSHIP_BENCHMARK=1` when running
  `zig build sql-test` for the 10,000-row fixture (routine coverage uses 512).
- UNION/INTERSECT/EXCEPT with ALL/distinct multiplicities, INTERSECT precedence,
  parenthesized operands and final ordering/limits. Typed equality keeps JSON
  null distinct from SQL NULL; set operands share the statement memory budget.
- `INSERT INTO ... (columns) VALUES (...) [, ...]` with scalar expressions and
  parameters. All rows are prepared and validated before one atomic mutation.
- Scalar subqueries inside multi-row `INSERT ... VALUES` use a flat generated
  source AST and bound VALUES node, with balanced cross-arm type inference.
  Adjacent literal rows share one typed block; only one nonliteral arm iterator
  is active at a time. Source reads (including self-reads) close before commit;
  assignment-literal coercion and scalar cardinality are preserved. The
  1,000-row parser limit remains subject to the request memory budget.
- `INSERT INTO ... (columns) SELECT ...` with typed source values, assignment
  checks and bounded whole-statement preparation. Source cursors close before
  commit, including self-inserts; source failure cannot publish a partial batch.
- `UPDATE ... SET column = expression [, ...] [WHERE ...]`.
- Joined `UPDATE ... FROM`/explicit JOIN and `DELETE ... USING`/explicit JOIN,
  including CTE sources. Target provenance is retained from the joined snapshot;
  source readers close before native atomic commit. UPDATE rejects multiple
  matches per target with SQLSTATE 21000; DELETE deduplicates target images
  before applying the mutation-row quota. Equality predicates select hash joins.
- Target-only UPDATE predicate and assignment subqueries, and DELETE predicate
  subqueries, use the same decorrelated joined-mutation planner. Their bounded
  scans share one captured statement view before atomic commit; correlated
  scalar cardinality and mutation-row quotas still apply.
- `INSERT ... ON CONFLICT (_id) DO NOTHING` and `DO UPDATE SET ... [WHERE ...]`.
  Assignments bind old-row and `excluded` values once, with native defaults and
  generated values. Skipped rows remain atomic read-set fences, never deletes;
  affected counts and RETURNING omit them. Concurrent arbiter changes fail as
  definite serialization conflicts, without implicit replay. Coordinated
  backends also resolve explicit complete unique-column targets through the
  native tuple codec and generation-bound claim authority, including composite
  keys and native NULL-distinct behavior. Exact claim/absence guards travel with
  the atomic mutation and survive session staging, savepoints, and recovery.
  `ON CONFLICT (columns) WHERE predicate` also infers partial unique arbiters
  when the bounded conjunction of typed column/literal comparisons and NULL
  tests proves the native index predicate. Membership is evaluated from typed
  rows when constructing claims; unsupported inference expressions fail closed.
  Targetless `ON CONFLICT DO NOTHING` checks all supported native immediate
  unique constraints plus `_id`; skipped candidates do not reserve identities
  against later VALUES rows. Targetless UPDATE, unsupported native constraint
  forms, and providers without native arbiter coordination fail closed.
- `DELETE FROM ... [WHERE ...]`.
- Relational and document INSERT/UPDATE/DELETE `RETURNING` projections, expressions and
  wildcard. Native schema-bound normalization supplies defaults/generated
  values; DELETE uses version-fenced preimages. All output preparation occurs
  before commit, and a backend without normalization fails before writing.
  Document UPDATE retains undeclared fields from the pinned primary preimage;
  native digest and schema-epoch predicates protect every mutation. Explicit
  SQL NULL removes a document member while JSON null remains a present value.
- Native table/database/schema/tablespace CREATE/DROP/rename/tablespace DDL,
  schema-version-conditional column/default changes and multicolumn covering
  indexes. Unique declarations await native constraint activation; a durable
  pending receipt is not a successful CREATE acknowledgement.
- `BEGIN`/`START TRANSACTION` isolation/read modes, `COMMIT`, `ROLLBACK`, named
  SAVEPOINT/ROLLBACK TO/RELEASE, backed by durable native session ownership.
- Deferrable UNIQUE and `SET CONSTRAINTS ... IMMEDIATE/DEFERRED`, with durable
  timing overrides and final-overlay native commit validation. IMMEDIATE is
  retroactive and cannot publish its mode change while pending data violates it.
- Native `TRUNCATE [TABLE] ... [RESTART|CONTINUE IDENTITY] [CASCADE|RESTRICT]` admits a
  fresh-generation barrier through shared distributed restore staging. Pending
  or unknown admission returns a reconciliation receipt, not completion. The
  current safe execution boundary requires an FK-closed cohort; untouched-parent
  inverse-witness retirement and graph boundaries are not yet implemented.
  RESTART IDENTITY resets the fresh owner generation; the current SQL catalog
  has no owned sequences or serial columns, so there is no sequence counter to
  restart. DDL inside an explicit transaction remains rejected.
- Dry-run `EXPLAIN` for admitted SELECT/INSERT/UPDATE/DELETE/MERGE binds the
  underlying statement with its normal authority and emits bounded text or
  versioned JSON. `FORMAT JSON`, `VERBOSE`, and `COSTS OFF` are supported;
  `ANALYZE` and requested cost estimates reject until instrumentation and a
  cost model exist. EXPLAIN never opens row cursors or publishes mutations.

Names can be `table`, `namespace.table`, or `database.namespace.table`. Unquoted
identifiers fold ASCII case; quoted identifiers preserve their exact names.
`_id` is reserved for the backend's opaque row identity, not a schema primary
key declaration. INSERT without `_id` uses the shared native secure identity
provider once during preparation; the native expected-absent fence prevents
collisions from overwriting a row. Providers without that capability require an
explicit `_id`. Integer literals are signed 64-bit values parsed exactly.

## Deliberate exclusions

`MERGE` has a bounded compiler AST for ordered MATCHED/NOT MATCHED arms,
conditional UPDATE/DELETE/INSERT/DO NOTHING, source relations, CTEs and
RETURNING. MERGE binds lazy ordered arm programs
and one projected source-preserving candidate join. It executes only when a
durable serializable transaction retains source/target range proofs with the
mutation; API autocommit opens an implicit such transaction. Plain batch
backends and read-committed sessions reject without reading candidates.
RETURNING projects native-prepared target postimages and captured source values
before mutation admission, so expression failures cannot obscure a committed
outcome. Explicit MERGE DEFAULT cells use the native absent-field preparation
path. Small identity-key sources use deduplicated, guarded primary point scans.
Small sources with complete equality on a declared total multicolumn index
use deduplicated, guarded secondary-index probes; a saturated nonunique probe
falls back to the coordinated full join. Partial, expression, and general
residual index lookups and full execution parity remain incomplete.

Ordered ANY/SOME/ALL comparisons use grouped extrema and NULL/count evidence.
LIKE/ILIKE quantifiers use quota-bound distinct pattern sets per correlation
key, with step-limited matching, empty/NULL truth tables and per-pattern NOT.
EXISTS supports one correlated ordered comparison in addition to computed equality
keys; its discarded projections are validated without runtime evaluation or
scan payload dependencies.
Uncorrelated scalar/membership/quantified value relations can contain set
operations, CTEs, grouping/HAVING, windows and ORDER/LIMIT/OFFSET. An independently
bound derived-query boundary preserves those semantics and rejects outer
references. Derived relations also accept positional column alias lists.
Nonrecursive CTEs accept `MATERIALIZED` and `NOT MATERIALIZED`: the former
shares a bounded typed producer, the latter inlines each reference. An unhinted
CTE materializes when referenced more than once and otherwise stays pipelined.

DISTINCT window aggregates remain unsupported. Multi-column
membership, multiple correlated ranges, nested subquery
expressions requiring outer bindings, per-key ORDER/LIMIT, correlated set/group/HAVING/window
subquery forms, lazy CASE/COALESCE subquery
branches, and non-decorrelatable mutation-expression subqueries
remain unsupported. Typed expression and partial conflict arbiters share native
uniqueness authority; deferrable conflict arbiters are deliberately rejected.
Mutual/nonlinear recursion, aggregate/window recursive terms and nullable-side
self joins remain unsupported; recursive output widening requires a seed cast.
Pgwire SQL
PREPARE/EXECUTE/DEALLOCATE uses connection-owned prepared state shared with
Parse/Bind, typed scalar argument evaluation, binding identity checks and pull
execution. HTTP prepare/execute/close exposes bounded durable owner-scoped
resources with identity-fenced rebinding and independent transaction lifetime.
These implemented forms do not constitute the complete SQL extraction parity gate.
Unsupported tails and additional statements are rejected, never
ignored. The existing generated grammar remains a syntax oracle, not a
production semantic parser with implicit conflict resolution.

Whole-shape parameter inference runs before emitting inner programs. Symbolic
column lineage carries constraints through nested derived queries, CTEs, joins,
set operands and INSERT assignment context. This catalog-only pass never reads
rows, reuses resolved read identities, and is skipped when parameter types are
already known. Nonblocking nested queries and CTEs stream through owned pull
cursors; blocking sorts, aggregates, windows and set operands retain bounded
intermediate results.

The execution layer separately controls support for parsed ordering, predicate
forms, DDL, and transactions. Those capabilities must not be advertised merely
because the compiler represents them.

## Ownership and admission

Compiled plans own immutable schema-independent semantic data in one arena.
Lexer buffers and original source/comments are released before compilation
returns. Each execution supplies typed parameter values and its own catalog
binding; there is no textual parameter substitution or durable bound-schema
pointer in the compiled plan.

`describe.zig` provides non-executing semantic binding shared by Describe and
Execute. It resolves and authorizes the referenced table once, returns ordered
output metadata and inferred positional parameter types, preserves unknown
parameter holes, and retains physical table/schema identity for rebinding.
Description never opens a row reader or stages a mutation.
Generated columns remain readable but cannot be explicitly assigned by SQL
INSERT/UPDATE. Their native output-only status is checked during binding, even
for provably empty updates; native recomputation never silently overrides an
accepted SQL assignment. UPDATE does not fetch generated values it will replace
through native recomputation.

String literals assigned to JSON columns are interpreted as JSON input text;
typed JSON parameters remain values. Numeric JSON text is retained losslessly.
Identical JSON literals share one immutable parsed tree in the binding; Execute
reuses that tree rather than parsing the literal again after validation.
Native typed reads and expression results carry separate SQL-null flags through
HTTP and pgwire, including binary portals. Mutations carry explicit JSON-null
field provenance into native validation and row preparation, so a JSON null
value does not become SQL NULL. SQL `NULL` and nested JSON nulls remain supported. JSON literal nesting is admitted before tree allocation and bounded
to 64 container levels.

The default quotas are 1 MiB statement bytes, 16,384 tokens, 8,192 AST nodes,
64 levels of nesting/tree depth, 1,024 positional parameter slots, and 1,000
insert rows. Token admission happens before decoding/allocating the excess
token. Associative boolean chains are balanced so a long flat clause cannot
create a linear-depth binding/evaluation stack.

Run compiler tests from `zig/`:

```sh
zig test --dep sql_parser -Mroot=pkg/antfly/src/sql/compiler.zig -Msql_parser=lib/sql/root.zig
zig test lib/sql/root.zig
```

Execution binds scalar instructions and column ordinals once. Safe conjuncts
remain native predicate/index bounds; residual predicates execute before
OFFSET/LIMIT/counting or mutation staging. Bounded top-K retains at most
OFFSET+LIMIT rows (plus one overflow witness without an explicit LIMIT), owns
only competitive rows, and preserves stable tie ordering. Primary-key ascending
order and simple COUNT retain their native fast paths.

Lake execution requests up to 1,024 typed rows independently of delivery page
size. Projected batches remain pending across bounded delivery pages. Semantic
vector failures recover the successful row prefix and defer the terminal error
until that prefix drains; failed vector workspaces are released before recovery. Joins
batch key expression evaluation and probe admission, while preserving input
order and delaying later-lane errors until consumed. Grouped reductions resolve
group IDs once per batch and update state by aggregate column; global COUNT,
integer SUM and boolean reductions consume contiguous vectors. Join payload
admission is column-major, and join/group hash vectors memoize repeated text
without relying on dictionary IDs across batches.
Retained join/group strings and exact decimals use adaptive dictionaries,
switching to flat owned references for high-cardinality inputs.

Parquet scans evaluate predicate and delete evidence before projected payloads.
A bounded selection mask is shared with delivery when its physical row range
matches, including narrower payload-page slices. Dictionary predicate results
and delete key parts are memoized only for entries referenced by that page;
applicable equality-delete files with identical field sets share key encoding.
Standard offset/column indexes can skip page reads, while Iceberg manifest
bounds resolve through field IDs. Unknown encodings remain conservative.
Identity-only scans generate row references without decoding data pages.
Parsed footers and decoded vectors use immutable, version/credential-scoped
cache leases with bounded eviction. Compressed range bytes also use immutable
leases: native page/header/index decoding borrows pinned bytes, while legacy
owned reads copy outside the cache mutex. Pinned entries cannot be evicted, and
admission declines when pinning leaves insufficient space. Duplicate range reads share one in-flight
request; next-group evidence and next-file metadata use shared bounded
scheduling and join before cursor teardown. Iceberg delete indexes are prepared
once per pinned source and applied directly to column batches. Missing evolved
optional equality-delete fields use SQL NULL; malformed delete files still fail.
Native byte dictionaries retain compact indices through scanning. Decoded pages
pin a single immutable chunk dictionary rather than copying it per page; cache
accounting charges the dictionary once and eviction respects page dependencies.
Eligible scalar programs execute a shared instruction DAG over contiguous
integer, number, boolean and string vectors with separate SQL/JSON null state.
Common subexpressions and column normalization are shared across projections,
group keys and aggregate inputs with the same FILTER selection. Lazy programs
keep scalar evaluation, and FILTER inputs evaluate only accepted rows. Declared
SQL types, mixed numeric comparisons and exceptional arithmetic lanes retain
the scalar coercion and error contracts, including UTF-8 and expression output
size validation at result roots rather than discarded intermediates.

Eligible COUNT, integer SUM and boolean reductions split pinned lake scans into
up to four workers claiming compressed-size-ordered row-group tasks. File
projection/schema plans are built once by the coordinator and borrowed by each
task; a claim references its group directly. Workers own private readers
and bounded local state; deterministic exact merging preserves first-occurrence
group order. Memory pressure falls back to the original pinned serial spilling
scan. Floating-point, DISTINCT and pattern reductions keep ordered execution.
All workers share process-wide scheduling admission. Speculative next-group
warming reserves 4 MiB and releases admission at completion, independently of
its cursor-owned join handle.

Join dynamic filters are sealed and installed on eligible probe scans before
probing or collecting spill partitions. At memory pressure, a non-shared join
transfers its retained build prefix directly to Grace partition sinks and sends
remaining build/probe batches there, without an intermediate chain spool.
Oversized partitions test remaining hash bits before creating a chain fallback;
indistinguishable keys keep bounded chain probing. Complete spill partition jobs evaluate
ordinary ON residuals and matched markers inside the worker. Join and exact
aggregate workers deliver bounded typed blocks through queues with backpressure;
output does not add spill writes. Consumer close stops and joins producers, and
terminal errors follow the successfully produced prefix. Providers requiring
external decision evaluation or pattern-set cursor state retain the coordinated
residual path.

Sort spill uses bounded eight-way merging and releases sealed write buffers.
Each run pins its borrowed merge head until that run advances; output encoding
finishes before advancement, avoiding per-head cloning.
Independent merge compactions share parallel scheduling admission. Complete
homogeneous normalized keys use stable radix passes with original ordinals as
tie breakers; mixed, nullable-layout and truncated keys retain exact comparison.
Sequential sort, join and group runs pack homogeneous columns, null flags and
ordinals into checksummed blocks, with optional Snappy compression. Wide rows
and singleton blocks use compact record framing without additional staging
copies. Blocking result spools use the same sequential typed blocks without a
row-offset directory. External sort delivery discards keys and transfers owned
values directly into the result page. Pgwire reuses its DataRow buffer across a
page and encodes primitive/JSON cells directly into it. HTTP/pgwire response
limits remain enforced. Random-access chains retain individually framed records.
Exact COUNT, integer SUM and boolean aggregates partition updates into bounded
typed reducers; oversized partitions use sorted partial merging. Floating-point,
distinct and pattern aggregates preserve their ordered merge paths. A final
ORDER BY reuses a window sort when its complete physical key and null ordering
match, preserving original-row tie order and expression errors. External window
layouts share one immutable input payload, ordinal permutations, and output
sidecars indexed by original row. Partitions are slices of a permutation rather
than rewritten payload stores.

The deterministic spill tests compare identical inputs and memory budgets:
eight-way sorting and partitioned aggregation both write fewer bytes than their
binary-merge and sorted-update counterparts. Current measurements are recorded
in `bench/baselines/native-lake-batch-block-refinements.json`.
Shared-DAG and blocking result-delivery samples are recorded separately in
`bench/baselines/native-lake-pipeline-refinements.json`. The result fixture writes
417,792 bytes through indexed rows versus 197,824 bytes through typed blocks;
read/write calls drop from 104 to 49. Its median elapsed time is about half.
The longer shared expression chain reduces median elapsed time by about 26%;
short-expression timings overlap and fused result workspace can be larger.
These are local microbenchmarks, not overall query speed guarantees. The independent PyArrow
end-to-end test covers compressed indexed pages, dictionary/plain encodings,
nulls, SQL joins/groups/windows, HTTP, pgwire streaming and cold restart.

SQL sessions admit READ COMMITTED. Repeatable-read and serializable require
explicitly capable read/write providers: coordinated owner snapshots, replicated
tracking activation, durable range observations and owner-fenced atomic prepare.
Changed observations cause serialization failure, never silent snapshot renewal.
Savepoint rollback retains observations already exposed to the client. Unsupported
providers reject stronger BEGIN; TTL-enabled tables reject guarded reads until
clock-driven visibility has a transaction-time contract. Conservative first-byte
buckets may conflict for distinct keys sharing a prefix.
The native row-preparation port shares schema defaults,
stored-generated expressions and validation with commit; it does not publish
any data while staging a session statement.

The standalone ReleaseSafe microbenchmark measures preparation/retained plan
memory, bounded token admission and nested shape binding with inferred versus
explicit parameter types, not SQL execution or storage throughput:

```sh
zig run -O ReleaseSafe --dep sql_parser -Mroot=pkg/antfly/src/sql_bench.zig -Msql_parser=lib/sql/root.zig
```

Additional ownership/admission samples are recorded in
`bench/baselines/native-lake-ownership-refinements.json`. In the local fixture,
column batch join admission takes about 43% of the row-materializing baseline
elapsed time. Three window layouts over 256 rows with 16 KiB payloads write
4,255,232 bytes through shared payloads versus 29,524,992 through payload
rewrites, with compression disabled. These fixtures validate equal results and
alternate execution order; they do not measure total production query latency.
