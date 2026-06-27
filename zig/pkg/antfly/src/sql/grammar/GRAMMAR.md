# Antfly SQL Grammar Migration

Antfly SQL should become PostgreSQL-compatible at the user surface without
embedding PostgreSQL's parser as the engine boundary. PostgreSQL grammar and
CockroachDB grammar behavior are references for compatibility. Antfly owns the
grammar, token model, AST, lowering, diagnostics, and execution semantics.

The production target is:

```text
SQL bytes
  -> Antfly SQL scanner
  -> generated Antfly SQL parser
  -> catalog-free Antfly SQL AST
  -> statement-family classifier
  -> binder
  -> typed logical plan
  -> shared Antfly service
```

The generated parser must not become a second control plane. It recognizes
syntax and builds raw AST nodes with source spans. Catalog lookup, role checks,
storage visibility, derived-index lifecycle, graph metric state, Lite behavior,
and execution all stay in the binder, planner, and shared service layers.

The reusable generator machinery lives under `zig/lib/yacc`, following the same
library-plus-codegen shape as `zig/lib/openapi`. Antfly SQL owns only the input
grammar, migration docs, and checked-in generated output under this directory.
Use `zig build regen-sql-grammar` to refresh checked-in grammar metadata and
`zig build sql-grammar-generated-check` to verify it is current. The checked-in
generated metadata retains the PostgreSQL version/branch/commit/date plus the
referenced PostgreSQL `gram.y`, PostgreSQL `scan.l`, and CockroachDB `sql.y`
URLs from the grammar seed, keeping compatibility provenance visible without
vendoring those grammars.

The first generator implementation builds deterministic SLR parser tables:
grammar parsing, production validation, nullable/FIRST/FOLLOW sets, LR(0)
states, indexed action/goto tables, source-aware syntax diagnostics, and
structured conflict reporting. Generator tests cover URL-bearing reference
metadata so `https://` references are not confused with grammar comments. The
grammar seed uses `%expect` to record the current broad-grammar conflict count;
regeneration fails if the parser table conflict count drifts without an
intentional grammar update, and the checked-in generated metadata records both
actual and expected conflict counts. The
current broad Antfly SQL seed grammar generates deterministic parser metadata
with tracked conflict reporting. Conflict drift now has a structured generator
report path that prints the expected and actual conflict counts plus
representative state/terminal/action conflicts, so broad grammar edits fail
with actionable evidence instead of an opaque count mismatch. The
first runtime integration observes the generated parser on the `ParsedSql` path
but does not require grammar parity
before the existing parser can handle a statement. When the generated parser
accepts a statement, `ParsedSql` retains a source-span-bearing generated raw
node and uses it for the first migrated statement variants. Session,
transaction, prepared statements, and prepared transactions now require
generated parser success;
they also carry the first generated AST payload with source spans and token
ranges for command names, values, prepared-statement arguments, and nested
prepared statements. Generated prepared transaction ASTs retain the command
kind and global transaction identifier token range for `PREPARE TRANSACTION`,
`COMMIT PREPARED`, and `ROLLBACK PREPARED`, and lower through the typed
prepared-transaction DDL plan boundary only when the generated command, keyword
tail, and GID metadata agree. This AST is retained by `ParsedSql`; prepared
statements now have an AST-to-plan conversion path with retained
statement/command span,
command-kind, name, parameter, argument, and nested-statement range validation
plus parity coverage against the existing token-based lowerer, including typed
`PREPARE name(type, ...) AS ...` parameter lists and generated `DEALLOCATE ALL`.
Prepared-statement command heads now require generated parser success at SQL
ingress for `PREPARE`, `EXECUTE`, and `DEALLOCATE`, including malformed
multi-token tails. Parsed-statement classification validates retained
prepared AST kind, statement/command spans, prepared-statement name ranges,
parameter ranges, argument ranges, and inner-statement ranges before
publishing the prepared family.
Session catalog commands now have generated AST-to-plan parity
for the generated-covered `SET`, `SET LOCAL`, `RESET`, `RESET ALL`, `SHOW`,
`SHOW ALL`, and `DISCARD ALL` forms. Generated `SET ... TO ...` and
`SET LOCAL ... TO ...` session values accept comma-separated expression lists
so generated ingress covers PostgreSQL-style `search_path` updates before the
typed session lowerer decides whether a setting is supported.
PostgreSQL-compatible non-`ALL` `DISCARD` forms such as `DISCARD TEMP` and
`DISCARD PLANS` are represented as explicit generated unsupported diagnostics
instead of syntax errors or session-parser probes.
PostgreSQL role session controls such as `SET ROLE` and `RESET ROLE` are also
classified as explicit generated unsupported diagnostics instead of generic
session setting tails.
Session command heads now require generated parser success at SQL ingress, so
malformed multi-token session commands cannot fall back to the legacy session
adapter. Parsed-statement classification validates retained session AST kind,
statement/command spans, setting-name ranges, and value ranges before
publishing the session family.
Transaction boundary commands now dispatch through generated AST-to-plan
lowering for generated-covered `BEGIN`, `COMMIT`, PostgreSQL `END` commit
aliases, and `ROLLBACK`
adapter-noop boundaries, including retained statement/command source-span and
command-kind validation. `START`, `COMMIT`/`END`, and `ROLLBACK` command heads now
require generated parser success at SQL ingress, so malformed transaction
tails cannot fall back to the legacy transaction adapter. Transaction mode and
savepoint controls are also lowered from generated transaction ASTs: `SET TRANSACTION`,
`START TRANSACTION`, `BEGIN ...` mode tails, `SAVEPOINT`, `RELEASE [SAVEPOINT]`,
and `ROLLBACK TO [SAVEPOINT]` retain validated mode/name token ranges before
lowering to the typed transaction-control or savepoint plans. Parsed-statement
classification validates retained transaction AST kind, statement/command
spans, boundary-tail ranges, mode ranges, and savepoint-name ranges before
publishing the transaction family.
Simple DDL has generated-parser corpus coverage but still falls back to the
existing parser when the seed grammar does not yet cover the shape; generated
simple DDL ASTs now carry structured object, option, and behavior fields for
database create/alter/drop, schema create/drop, and extension create/alter/drop catalog plans and lower those
catalog plans directly from generated AST ranges. `CREATE`/`DROP`
database/schema/extension statement heads now require generated parser success
at the `ParsedSql` boundary, so malformed generated-owned catalog DDL fails
closed instead of probing the legacy DDL classifier. Generated-covered
`ALTER DATABASE` and `ALTER EXTENSION` statement heads now use the same strict
generated parser boundary for catalog-setting and extension-update forms.
Generated DDL lowering
also validates the retained statement span, command span, and command-kind
metadata before dispatching to catalog, table, index, or alter-table planning.
Generated AST-to-plan parity also covers seed `CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`, and `DROP INDEX`
forms using the same parser options as the existing lowerer. Generated
`CREATE TABLE` classification now includes `TEMP`/`TEMPORARY` and `UNLOGGED`
relation-lifetime prefixes with retained target-name spans.
Plain view catalog DDL heads, including `CREATE VIEW`, `CREATE OR REPLACE VIEW`,
`ALTER VIEW ... RENAME TO`, and `DROP VIEW`, now parse through generated DDL
ASTs and use a generated runtime boundary before delegating to the existing
typed view catalog planner.
Domain catalog DDL heads, including `CREATE DOMAIN`, `ALTER DOMAIN`, and
`DROP DOMAIN`, now use the same generated DDL boundary with retained object
spans and validated command/tail metadata before delegating to the existing
typed domain catalog planner. Generated `CREATE DOMAIN` ASTs retain the
definition tail starting at `AS`, and domain planning fails closed if that
tail no longer begins at the recorded domain-name boundary.
Sequence catalog DDL heads, including `CREATE SEQUENCE`, `ALTER SEQUENCE`, and
`DROP SEQUENCE`, now use the generated DDL boundary with retained object,
option-tail, and drop-behavior metadata before delegating sequence option
semantics to the existing typed sequence catalog planner.
Enum type catalog DDL heads, including `CREATE TYPE ... AS ENUM`,
`ALTER TYPE ... ADD VALUE`, and `DROP TYPE`, now use the generated DDL
boundary with retained type-name, value-tail, and drop-behavior metadata before
delegating enum value semantics to the existing typed enum catalog planner.
Schema namespace rename DDL, `ALTER SCHEMA ... RENAME TO ...`, now shares the
generated schema namespace boundary with generated `CREATE SCHEMA` and
`DROP SCHEMA`, retaining old/new schema-name metadata before lowering to the
typed namespace catalog plan.
Tablespace catalog DDL heads, including `CREATE TABLESPACE`,
`ALTER TABLESPACE ... RENAME TO ...`, and `DROP TABLESPACE`, now use the
generated DDL boundary with retained tablespace-name and option/rename-tail
metadata before delegating to the existing typed tablespace catalog planner.
Logical-replication catalog DDL heads, including
`CREATE`/`ALTER`/`DROP PUBLICATION` and
`CREATE`/`ALTER`/`DROP SUBSCRIPTION`, now use the generated DDL boundary with
retained object-name and operation-tail metadata before delegating to the
existing typed logical-replication catalog planner.
Materialized-view catalog DDL heads, including `CREATE MATERIALIZED VIEW`,
`DROP MATERIALIZED VIEW`, and `REFRESH MATERIALIZED VIEW`, now use the
generated DDL boundary with retained view-name and population/refresh-tail
metadata before delegating to the existing typed materialized-view catalog
planner. `ALTER MATERIALIZED VIEW` remains an unsupported generated diagnostic
until Antfly has a typed plan for that operation family.
Row-security policy catalog DDL heads, including `CREATE POLICY`,
`ALTER POLICY`, and `DROP POLICY`, now use the generated DDL boundary with
retained policy-name, target-table, operation-tail, `IF EXISTS`, and
drop-behavior metadata before delegating to the existing typed row-security
catalog planner, including routine-backed policy predicates that require
parser-context function bindings.
Routine catalog DDL heads, including `CREATE FUNCTION`,
`CREATE OR REPLACE FUNCTION`, `CREATE PROCEDURE`, `DROP FUNCTION`, and
`DROP PROCEDURE`, now use the generated DDL boundary with retained routine
name, signature-tail, replace, `IF EXISTS`, and cascade metadata before
delegating routine option/body semantics to the existing typed routine catalog
planner.
Role authorization DDL heads, including `CREATE ROLE`, `ALTER ROLE`, and
`DROP ROLE`, now use the generated DDL boundary with retained role-name,
operation-tail, and `IF EXISTS` metadata before delegating role setting
semantics to the existing typed authorization catalog planner. The role
boundary now also requires generated parser success at SQL ingress for
`ROLE`, `USER`, and `GROUP` forms instead of falling back to the legacy
permissive DDL adapter on malformed role catalog statements.
Type-system catalog DDL heads, including `CREATE COLLATION`,
`ALTER COLLATION`, `DROP COLLATION`, `CREATE OPERATOR`, `DROP OPERATOR`,
`CREATE AGGREGATE`, `DROP AGGREGATE`, `CREATE CAST`, and `DROP CAST`, now use
the generated DDL boundary with retained object-name or signature-tail ranges
before delegating option/signature semantics to the existing typed type-system
catalog planner. These supported type-system heads now require generated
parser success at SQL ingress so malformed generated-owned catalog statements
do not fall back to the permissive legacy DDL classifier; unsupported
type-system variants such as operator class/family and aggregate/operator
`ALTER` forms continue through the generated unsupported diagnostic path.
Extended generated-owned catalog DDL heads now follow the same strict ingress
rule for `CREATE`/`ALTER`/`DROP` view, domain, sequence, enum type,
tablespace, publication, subscription, and policy statements, plus materialized
view create/drop/refresh and routine create/drop statements. Malformed
complete-looking catalog statements in those families fail through generated
parser diagnostics instead of falling back to the legacy permissive DDL
classifier. Parsed-statement classification for generated-covered DDL now also
validates the retained generated DDL AST kind, statement span, command span,
object/table ranges, index ranges, and operation-tail ranges before publishing
the DDL family, so corrupted generated DDL payloads fail closed at the SQL
ingress boundary instead of routing into typed catalog planning.
Generated
`CREATE INDEX` ASTs also retain
`UNIQUE`, method, element-list, covering-index `INCLUDE (...)`, options, and
partial-index `WHERE ...` token ranges and generated-first create-index
planning validates those ranges before lowering through the typed DDL planner.
Antfly-derived index methods exposed through PostgreSQL-style `CREATE INDEX
... USING antfly_full_text`/`antfly_aknn`/`antfly_graph`/
`antfly_graph_metric`/`antfly_hybrid`/`antfly_algebraic` now use the same
strict generated parser ingress as ordinary table/index DDL instead of a
derived-index-specific fallback exception.
PostgreSQL-style relation population heads, including `SELECT ... INTO` and
`CREATE [TEMP|TEMPORARY|UNLOGGED] TABLE ... AS SELECT ... [WITH [NO] DATA]`,
now parse and classify through the generated DDL family with retained target
name spans before delegating to the existing relation-population planner.
Incomplete covered DDL clause-boundary shapes for `CREATE TABLE`, lifetime
prefixed `CREATE [TEMP|TEMPORARY|UNLOGGED] TABLE`, `CREATE VIEW`,
`CREATE DOMAIN`, `CREATE SEQUENCE`, `CREATE TYPE`, `CREATE TABLESPACE`,
`CREATE PUBLICATION`, `CREATE SUBSCRIPTION`, `CREATE MATERIALIZED VIEW`,
`CREATE INDEX`, `CREATE ROLE`, `CREATE COLLATION`, `CREATE OPERATOR`,
`CREATE AGGREGATE`, `CREATE CAST`, `ALTER TABLE`, `ALTER SCHEMA`,
`ALTER TABLESPACE`, `ALTER COLLATION`, `CREATE POLICY`, `ALTER PUBLICATION`, `ALTER SUBSCRIPTION`, `ALTER POLICY`,
`ALTER ROLE`, `ALTER VIEW`, `ALTER DOMAIN`, `ALTER SEQUENCE`, `ALTER TYPE`, `DROP TABLE`,
`DROP VIEW`, `DROP DOMAIN`, `DROP SEQUENCE`, `DROP TYPE`, `DROP TABLESPACE`,
`DROP PUBLICATION`, `DROP SUBSCRIPTION`, `DROP POLICY`, `DROP ROLE`,
`DROP COLLATION`, `DROP OPERATOR`, `DROP AGGREGATE`, `DROP CAST`,
`DROP MATERIALIZED VIEW`, and
`REFRESH MATERIALIZED VIEW` now fail closed through the generated parser
instead of falling back to the legacy DDL classifier, while rich DDL syntax
that is not yet generated-covered still falls back to the existing typed DDL
parser.
Simple DML now has generated-parser corpus
coverage, retained generated raw and AST nodes for covered write statements,
structured generated DML ranges for target tables, sources, assignments,
predicates, conflict clauses, returning clauses, values lists, default-values
inserts, and truncate options. PostgreSQL `INSERT ... OVERRIDING
{SYSTEM|USER} VALUE ...` syntax is recognized by the generated grammar as an
explicit unsupported diagnostic until identity-column override semantics are
implemented in the write planner. Supported explicit-column `INSERT ... VALUES`
plans, including `ON CONFLICT` actions and field/all-field/expression
`RETURNING` lists, and `INSERT ... DEFAULT VALUES` plans, including
`ON CONFLICT` actions and returning lists, now lower directly from generated
AST ranges into relational row batches. Supported explicit-column
`INSERT ... SELECT` plans, including `ON CONFLICT` actions and returning lists,
now validate generated source, conflict, and returning ranges and parse the
source `SELECT` body into a retained lightweight generated read-body payload before direct
insert-source lowering, with generated-direct coverage for computed source
projections, expression predicates, ordering, pagination, `RETURNING *`
plus expressions, cross-table sources, conflict-update predicates and
expressions, and non-recursive CTE write prefixes. Generated DML child-read
metadata now also fails closed when the retained read kind disagrees with the
retained `SELECT`/set-operation/relation-source clause shape, so insert-source
and mutation-source wrappers cannot publish impossible child-read families for
later lowerers to rediscover. Parsed-statement classification also validates
generated top-level DML clause layout before publishing a write family:
`INSERT` target/column/value/default/conflict/returning ranges must align with
`INTO`, `VALUES`, `DEFAULT VALUES`, `ON CONFLICT`, and `RETURNING`; `UPDATE`
assignment/source/predicate/returning ranges must align with `SET`, `FROM`,
`WHERE`, and `RETURNING`; `DELETE` source/predicate/returning ranges must align
with `FROM`, `USING`, `WHERE`, and `RETURNING`; `TRUNCATE` extra targets must
remain comma-delimited after the primary target; and `MERGE` source and match
predicate ranges must align with `USING` and `ON`. Single-table point `UPDATE`
and `DELETE` statements with generated
`WHERE` ranges and field/all-field/expression `RETURNING` lists now also lower
directly from generated AST ranges into relational row batches, including
non-recursive CTE write prefixes. Table-wide and
single-table source `UPDATE` and `DELETE` statements without joined
`FROM`/`USING` sources now validate generated AST ranges before direct
mutation-source lowering. Explicit `UPDATE ... FROM` and `DELETE ... USING`
joined mutation-source statements now validate generated target, source,
predicate, and returning ranges before direct joined mutation-source lowering,
retain lightweight generated read-body payloads for relation source bodies,
parse generated child-read wrappers for those relation sources, and
generated-direct parity now covers explicit joined
`UPDATE ... FROM` and `DELETE ... USING` bodies with separate source schemas,
computed source assignments, expression predicates, source-qualified returning
expressions, and lock options, plus simple, correlated, filtered, computed,
row-value, and `EXISTS` semijoin joined mutation-source forms for both
`UPDATE` and `DELETE`, including non-recursive CTE write prefixes. `MERGE`
statements now validate generated target, source, and
`ON`/arm ranges, retain generated read-body payloads for `USING` relation
source bodies, and parse and validate complete generated child-read ASTs for
those sources before direct merge-plan lowering, with generated-direct coverage for
multiple matched/not-matched arms, conditional arms, matched
`DELETE`, matched/not-matched `DO NOTHING`, expression-filtered matched
`UPDATE`, filtered not-matched `INSERT`, `RETURNING`, and non-recursive CTE
write prefixes; recursive CTE insert-source, update, delete, and `MERGE`
forms now retain generated per-CTE body metadata and validate generated CTE and
command ranges, CTE item name/alias/`AS`/materialization/body delimiter
layout, comma-separated item boundaries, and full generated child-read parses
for each recorded CTE body before dispatching to the typed recursive write-plan
variants;
and `TRUNCATE`
lowers directly from generated AST ranges into mutation-source plans. Generated
direct `UPDATE` and `DELETE` point-vs-source selection now uses generated
target and `WHERE` ranges, including target aliases, before reusing the typed
selector semantics, so generated DML lowering no longer rediscovers those
clause boundaries through legacy token classification. Incomplete
migrated DML statements that stop at required generated clause boundaries,
including `INSERT ... ON CONFLICT ... DO` tails and `MERGE` action bodies, now
fail closed through the generated parser instead of falling back to the legacy
write classifier. Other DML shapes still use an initial generated
AST-to-plan wrapper that fails closed if the generated DML family does not
match the existing write classifier before delegating to the current typed DML
lowerer. Deeper DML cutover still requires replacing token-based recursive DML
command-body lowering with complete generated AST-driven lowering beyond
validated generated CTE child-read bodies, broader unsupported-shape diagnostics,
and a later full statement-head promotion once generated coverage matches the
currently supported typed DML surface.
Representative
read queries now have generated-parser corpus coverage, retained generated raw
and AST nodes for covered read statements, top-level generated AST ranges for
covered `SELECT` projections, sources, predicates, grouping, having filters,
window clauses, ordering, pagination, set-operation tails, and CTE prefixes,
owned list item and expression arrays plus first/last expression metadata for
top-level projection, grouping, ordering, and function argument lists, explicit
projection expression/alias token splits for explicit `AS` aliases and
PostgreSQL-style bare aliases, generated projection metadata for PostgreSQL
qualified-star projections such as `d.*`, generated ordering direction,
`USING` operator, and `NULLS` ordering token splits, and first-join generated metadata for
join operator/type, left input, right input, `ON` predicate ranges,
`USING` column-list ranges, and conditionless `CROSS JOIN`/`NATURAL JOIN`
metadata, explicit generated join-tree root/depth metadata for
left-associative generated join nodes, plus simple top-level
comparison expression metadata for covered `WHERE`, `HAVING`, and join
predicates. Generated top-level read validation now verifies statement/command
source spans, clause keyword layout, and generated payload consistency for
projection, `DISTINCT`,
source, `WHERE`, `GROUP BY`, `HAVING`, `WINDOW`, `ORDER BY`, pagination,
and set-operation tails before dispatching to read-family lowerers. Normal
function-call argument
lists are accepted in generated expression grammar, including top-level
projection functions with comma-separated arguments, `*` aggregate arguments,
aggregate `DISTINCT` argument metadata, aggregate-local argument `ORDER BY`
metadata, aggregate `FILTER (WHERE ...)` predicate metadata, and ordered-set
aggregate `WITHIN GROUP (ORDER BY ...)` metadata, and positive `LIKE`,
`ILIKE`, `IN (...)`, and `BETWEEN ... AND ...` predicates are accepted and
classified in generated expression metadata along with their `NOT` negated
forms; `IN` and `NOT IN` predicates over parenthesized read subqueries now
classify the right operand as generated subquery metadata. `ANY`/`ALL`/`SOME`
quantified comparison predicates and quantified `LIKE`/`ILIKE` pattern
predicates over parenthesized expression lists and parenthesized read
subqueries are also accepted and classified with explicit quantifier token
ranges, array-constructor metadata, and subquery token spans.
`EXISTS` and `NOT EXISTS` read-subquery predicates are accepted with explicit
operator, negation, and subquery token spans. Generated subquery expression
metadata now records subquery read kind plus first-query `SELECT`, projection,
source, `WHERE`, and set-operation ranges plus owned projection-list and
`WHERE` predicate expression payloads, plus generated set-operation payloads
for subquery set-operation tails, and subquery-owned `ORDER BY`, `LIMIT`,
`OFFSET`, and `FETCH` tail payloads for later direct subquery planning.
Generated validation rejects unrelated payload fields on generated subquery,
function-call, and `CASE` expression nodes, so those richer expression kinds now
have the same kind-specific payload isolation as binary, prefix, grouped, cast,
extract, array, temporal, and token-range expression nodes.
`IS NULL` and `IS NOT NULL` predicates are accepted and
classified as explicit null-test expression kinds, `IS TRUE`/`IS FALSE`/
`IS UNKNOWN` boolean-test predicates are accepted with their `IS NOT` variants,
and
`IS DISTINCT FROM` / `IS NOT DISTINCT FROM` predicates are accepted with
multi-token operator ranges. Top-level
`AND` and `OR` predicates are classified as logical-expression metadata with
left and right token ranges, owned child expression nodes, and owned
per-condition expression arrays plus chain-level condition-count and
first/last condition spans for top-level logical chains, while
`BETWEEN ... AND ...` remains classified as a range predicate. Prefix `NOT`
predicates are accepted and classified with owned right-side expression nodes.
Parenthesized expression groups carry inner token ranges and owned inner
expression nodes,
and comparison operands can expose unary plus/minus, additive, and
multiplicative child expression-kind summaries, including JSON/path postfix operator summaries,
function-call child summaries, and direct function-call name and argument-list
metadata,
and an initial generated AST-to-plan wrapper that validates those ranges,
rejects malformed structural payloads for covered expression kinds, verifies
child/list expression AST spans against their parent metadata, and fails closed
if the generated read family is incompatible with the existing read classifier.
Simple query, aggregate, join, and lateral reads now validate
generated clause ranges before calling their typed read-plan lowerers directly;
single binary join reads also validate generated join-tree metadata against the
typed join lowerer before producing a join plan, and generated join reads now
fail closed on malformed left-associative tree metadata, first-join
compatibility metadata, and `ON`/`USING` condition payloads. The executable
join contract is intentionally limited to one generated binary inner/left/cross
join until the row-plan API grows N-way, right/full outer-join, and
natural-join semantics;
generated binary `JOIN ... USING (...)` lowers through schema-checked equality
keys, generated `CROSS JOIN` lowers through a cartesian row-engine path, while
generated right/full joins, conditionless natural joins, and generated
join/lateral multi-join ASTs still fail closed before the typed lowerers can
partially parse them;
basic `OVER (PARTITION BY ... ORDER BY ...)` window reads now classify as a
generated window family, inline function-call `OVER` clauses carry generated
name/definition, partition-list, order-list, and frame-tail expression
metadata, minimal named `WINDOW ... AS (PARTITION BY ... ORDER BY ...)`
clauses are accepted and carry owned named-window AST items with name,
definition, partition-list, order-list, and frame-tail ranges, seed
`ROWS`/`RANGE` frame tails are accepted, and window reads dispatch directly
after validating projection/source/window ranges;
plain `DISTINCT` and `DISTINCT ON (...)` reads now carry generated distinct
ranges, `DISTINCT ON` expression-list AST items, and match the production
aggregate/query-family split;
generated set-operation reads now classify as a distinct read family and
validate the left query plus generated set-operation operator, `UNION ALL`,
right-query projection/source, right-projection boundary expressions,
right-predicate payloads, and parent read
result-tail ordering/pagination ranges before calling the set-operation lowerer
directly;
single- and multi-CTE reads now expose generated CTE-list, first-CTE, and
last-CTE name/body ranges plus owned per-CTE name/body item arrays, optional
column-alias lists, and `MATERIALIZED` / `NOT MATERIALIZED` hint metadata;
generated CTE reads now fail closed on malformed list counts, first/last
compatibility fields, column-alias lists, materialization hints, and body
enclosure metadata instead of falling back to classifier parsing after generated
validation rejects a covered CTE. That CTE wrapper validation now runs at the
generated read entry boundary before typed CTE parsing can proceed; each generated CTE item also carries body read-kind and
body clause-span metadata for the first body query, including set-operation
tails, plus owned body projection/group/order lists, predicate expression
metadata, generated body join-tree metadata with first-join compatibility
fields, body pagination expression metadata for `LIMIT`, `OFFSET`, and
`FETCH`, body `antfly.*` table-function source item arrays, and body graph
table-function semantic payloads, and the lowerer validates those body
payloads before dispatch; CTE body sub-parsers now receive a body-local
generated read AST cloned from the generated CTE metadata, so typed body
planning consumes generated body projection, predicate, ordering, pagination,
join, window, set-operation, row-lock, and table-function source ranges
instead of relying only on outer CTE pre-validation. The clone boundary now
revalidates the complete body-local generated read payload, including
projection, group, and order list boundary expressions, so corrupted CTE body
metadata fails closed before the typed body planner can consume token fallback;
recursive CTE reads carry an explicit generated recursive flag, and simple
non-recursive CTE reads dispatch directly when those ranges validate; recursive
CTE reads now validate generated recursive CTE metadata before dispatching to
the typed recursive CTE lowerer; generated pagination
grammar now covers `LIMIT`, `OFFSET`, and `FETCH FIRST`/`FETCH NEXT` query
tails with count expression metadata, and simple query, aggregate, join, and
window pagination use generated range-validated lowering when generated read
metadata is available; numeric and placeholder pagination values now execute
from generated expression ranges for `LIMIT`, `OFFSET`, and counted `FETCH`
forms, while expression-free `LIMIT ALL` and default `FETCH FIRST ROW ONLY`
remain explicit generated cases. Lateral, set-operation, and non-recursive CTE
final-read pagination now use the same generated range validation on their
typed lowerer paths. Generated `ORDER BY` lists now validate generated item,
expression, direction, `USING`, and `NULLS FIRST`/`NULLS LAST` ranges before
typed order planning, and the typed parser must consume the same generated
order-list span. Generated projection, grouping, generic expression, and
ordering lists now recursively validate each owned generated expression
payload before typed lowering can consume the list, including generic
function-call argument, ordered-argument, `WITHIN GROUP`, `FILTER`, and `OVER`
payload ranges. Query, aggregate, window, join, lateral, and CTE body read
entry points now also validate the whole generated read payload before typed
planning starts, so list boundary expressions, optional clause payloads,
pagination expressions, set-operation payloads, Antfly/graph table-function
source item counts and argument ranges, and join metadata cannot drift from
the generated AST while token fallback still exists. Set-operation payload
validation now also fail-closes right-hand `DISTINCT`/`DISTINCT ON`, projection,
source, and `WHERE` metadata so right-arm generated clause ranges cannot drift
independently of the retained set-operation AST. Generated CTE read lowering now derives non-recursive final read-family
dispatch from generated final-select ranges and clause metadata, including
final set-operation reads, instead of re-entering the legacy read classifier;
direct CTE query-plan lowering also preserves generated final set-operation
metadata for same-source CTE set-operation arms and fails closed when retained
set-operation payloads are malformed.
Recursive CTEs still dispatch through the recursive CTE family after validating
their generated recursive flag.
Unsupported read shapes
mostly still fall back, but complete top-level row-locking reads such as
`SELECT ... FOR UPDATE`, `FOR NO KEY UPDATE`, `FOR SHARE`, and
`FOR KEY SHARE`, including `OF`, `NOWAIT`, and `SKIP LOCKED` tails, and CTE
final reads with row-locking tails now parse through the generated read grammar
as normal reads with retained `row_lock_tokens`; relational reads lower those
tails through the existing typed `RowClaimRequest` planner, while document SQL
rejects them with the explicit `DocumentSqlLockingUnsupported` diagnostic. The
generated row-lock boundary is mode-aware so temporal `FOR SYSTEM_TIME` source
clauses remain part of their relation source rather than being misclassified as
lock tails.
Generated CTE body metadata now retains and validates body-level
`body_row_lock_tokens` as well as body-level Antfly and graph table-function
source metadata, and rebases that metadata when a CTE body is cloned into the
direct generated read AST path.
Deeper read cutover still requires full generated
query-body AST payloads for expression-level projections and predicates,
complete multi-join planning and richer join-tree semantics beyond the current
validated left-associative generated join nodes, complete expression AST nodes,
complete per-CTE body AST arrays, recursive CTE planning, aggregates, richer
inline window-expression semantic planning, ordering, remaining pagination cutover, and
direct generated read-plan lowering. Canonical Antfly query table functions
such as `antfly.full_text_search`, `antfly.semantic_search`,
`antfly.vector_search`, `antfly.hybrid_search`, `antfly.graph_traverse`,
`antfly.graph_match`, and `antfly.graph_metric` are now accepted as generated
read sources with named `=`/`=>` arguments, retained source/name/argument token
ranges, owned comma-adjacent named-argument item/name/operator/value ranges, list-based
Antfly function kind metadata across joined sources, graph function subset
metadata, exact named-argument operator validation for `=` and `=>`,
graph-specific semantic argument payloads for table/index selectors,
start/target selectors, pattern returns, metric names, and query text, and
fail-closed semantic/range validation in the generated read lowering boundary.
The generated parser now also treats graph DDL as a distinct graph
statement family and `ParsedSql` retains those generated raw and AST nodes.
Seed and rich `CREATE GRAPH INDEX` statements, including `IF NOT EXISTS`,
`CREATE GRAPH METRIC`, and
`ALTER GRAPH INDEX ... ADD METRIC` statements now have graph-specific generated
AST-to-plan wrappers that validate retained statement/command source spans and
lower to typed index plans instead of only routing through the generic DDL
family, and `ParsedSql` now requires generated-parser
success for `CREATE GRAPH` and `ALTER GRAPH` statement heads instead of falling
back to the legacy DDL classifier on malformed graph DDL. The generated
facade now returns closed statement-family nodes for the covered families and
generated unsupported AST nodes for seed `ANALYZE`, bulk I/O `COPY`,
maintenance `VACUUM`/`REINDEX`, ownership and system administration statements
such as `ALTER INDEX`, `ALTER SYSTEM`, `CREATE/DROP ACCESS METHOD`,
`DROP OWNED`, and `REASSIGN OWNED`, utility/control statements such as
`CLUSTER`, `COMMENT`, `GRANT`/`REVOKE`, `LISTEN`/`NOTIFY`, `LOCK`, `CALL`,
`CHECKPOINT`, `LOAD`, `SECURITY LABEL`, and `UNLISTEN`, plus non-table-function
graph query heads such as Cypher-style `MATCH ... RETURN ...` as explicit
`graph_query_not_planned_by_generated_parser` unsupported diagnostics, plus
common PostgreSQL extension catalog families for conversions, event triggers,
extended statistics,
operator/aggregate ALTER forms, operator class/family objects, and text-search
configuration/dictionary/parser/template objects, plus simple `EXPLAIN` forms
with stable reason metadata. Generated
unsupported AST heads that Antfly already plans through typed catalog/control
planners now route to the parsed DDL family explicitly; generated `EXPLAIN`
routes to the parsed explain family explicitly; and unsupported heads without
typed plans route to terminal parsed unsupported statements. Full production AST construction
remains the next migration boundary for larger DDL, query, DML, and Antfly
extension families.
Unsupported generated diagnostics also cover PostgreSQL materialized-view DDL
entry points that Antfly does not type yet, including
`ALTER MATERIALIZED VIEW`; procedural blocks with `DO`; foreign-table DDL;
foreign-schema imports; unsupported `ALTER FUNCTION`, `ALTER PROCEDURE`,
`ALTER ROUTINE`, `DROP ROUTINE`, language, transform DDL, and PostgreSQL
large-object administration such as `ALTER LARGE OBJECT`; trigger and
rewrite-rule DDL, including `ALTER TRIGGER` and `ALTER RULE`;
foreign-server DDL; access-method DDL; ownership maintenance statements; and
`ALTER INDEX`/`ALTER SYSTEM`. PostgreSQL conversion, event-trigger, extended
statistics, operator/aggregate ALTER forms, operator class/family, and text-search
configuration/dictionary/parser/template DDL now share the same generated
unsupported AST and reason coverage. These common unsupported
PostgreSQL dump/admin shapes now have stable unsupported AST reasons instead of
generic parser fallback. Legacy-supported materialized-view catalog operations
now use generated DDL AST nodes as a validated boundary before delegating to the
typed materialized-view catalog planner: `CREATE MATERIALIZED VIEW`,
`DROP MATERIALIZED VIEW`, and `REFRESH MATERIALIZED VIEW` fail closed if
generated kind, span, object name, tail, cascade flag, or command keywords are
corrupted. Legacy-supported row-security policy catalog operations use
validated generated DDL AST nodes for
`CREATE POLICY`, `ALTER POLICY`, and `DROP POLICY`, including routine-backed
policy predicates that require parser-context function bindings, and fail
closed if generated kind, span, policy name, target table, tail, `IF EXISTS`,
or cascade metadata is corrupted before
delegating to the typed row-security catalog planner. Legacy-supported routine
catalog operations use validated generated DDL AST nodes for `CREATE FUNCTION`,
`CREATE OR REPLACE FUNCTION`, `CREATE PROCEDURE`, `DROP FUNCTION`, and
`DROP PROCEDURE`, and fail closed if generated kind, span, routine name,
signature tail, replace, `IF EXISTS`, or cascade metadata is corrupted before
delegating to the typed routine catalog planner; routine validation also
requires the generated routine-name range to start after the exact
`FUNCTION`/`PROCEDURE` header and the signature/option tail to begin at the
recorded routine-name boundary. Legacy-supported role
authorization operations use validated generated DDL AST nodes for
`CREATE ROLE`, `ALTER ROLE`, and `DROP ROLE`, and fail closed if generated
kind, span, role name, operation tail, or `IF EXISTS` metadata is corrupted
before delegating to the typed authorization catalog planner; role validation
also requires the generated role-name range to start after the exact
role/user/group alias header and any generated operation tail to begin at the
recorded role-name boundary. Legacy-supported
type-system catalog operations use validated generated DDL AST nodes for
`CREATE/ALTER/DROP COLLATION`, `CREATE/DROP OPERATOR`,
`CREATE/DROP AGGREGATE`, and `CREATE/DROP CAST`, while
`ALTER OPERATOR`, `ALTER AGGREGATE`, and
`CREATE/ALTER/DROP OPERATOR CLASS/FAMILY` remain explicit unsupported
generated AST nodes with stable reasons. Supported type-system DDL fails closed
if generated kind, span, object name, signature tail, or `IF EXISTS` metadata is
corrupted before delegating to the typed type-system catalog planner.
Notification channel
commands also use the validated unsupported boundary for `LISTEN`, `NOTIFY`,
and `UNLISTEN` before delegating to typed notification catalog planning.
Authorization and utility commands use it for `GRANT`, `REVOKE`, `COMMENT`,
`CALL`, and `LOCK`; update-policy trigger commands use it for
`CREATE TRIGGER` and `DROP TRIGGER`; and routine trigger catalog commands now
use a typed `trigger_catalog` plan for generated-covered `CREATE TRIGGER` and
`DROP TRIGGER` row-trigger forms before reaching the SQL routine runtime.
Maintenance commands use the same
validated generated unsupported boundary for `VACUUM`, `ANALYZE`, `REINDEX`,
and `CLUSTER` before delegating to typed maintenance planning.
Legacy-supported cursor commands use generated cursor AST nodes for `DECLARE`,
`FETCH`, and `CLOSE`, including validated statement/command source spans and
typed tail token ranges before delegating to typed cursor portal planning.
Those cursor heads now require generated parser success at SQL ingress so
incomplete cursor commands fail closed instead of falling back to the legacy
DDL-like command adapter. Parsed-statement classification validates retained
cursor AST kind, statement/command spans, and tail ranges before publishing
cursor statements into typed cursor portal planning. `MOVE` uses the same
direction/count/name tail parser as `FETCH` but lowers to a distinct cursor
portal plan variant so runtime code can preserve PostgreSQL cursor movement
semantics separately from row-producing fetches.
Savepoint commands use generated transaction AST nodes for `SAVEPOINT`,
`RELEASE [SAVEPOINT]`, and `ROLLBACK TO [SAVEPOINT]` before delegating to typed
savepoint planning. Savepoint heads and rollback-to/release prefixes now pass
through the same generated LR parse gate as the rest of the transaction family
and require generated parser success at SQL ingress, including malformed or
incomplete savepoint-name tails. Bulk I/O commands use the validated
unsupported boundary for `COPY` before
delegating to typed bulk I/O planning. `EXPLAIN` uses the validated
unsupported boundary before delegating to typed explain planning, including
generated option payloads and subject-range validation before the inner
read/write statement is reparsed.
Generated unsupported utility command heads that require a subject, such as
`CALL`, `COPY`, `GRANT`, `LISTEN`, `LOCK`, `MATCH`, `NOTIFY`, `REINDEX`,
`REVOKE`, and `UNLISTEN`, now fail closed through the generated parser when
the statement stops at the command head instead of falling through to legacy
DDL probing.
Generated unsupported nodes now also participate in the parsed statement
boundary: generated-covered unsupported heads that are not intentionally
supported by the existing catalog planner become terminal parsed unsupported
statements and fail closed before legacy DDL probing. Generated unsupported
diagnostics also cover rich PostgreSQL catalog shapes under otherwise supported
heads, including `CREATE DATABASE ...` option tails, `CREATE SCHEMA ...`
authorization/definition tails, and multi-target `DROP TABLE`, `DROP INDEX`,
`DROP EXTENSION`, `DROP VIEW`, `DROP MATERIALIZED VIEW`, `DROP DOMAIN`,
`DROP SEQUENCE`, `DROP TYPE`, `DROP PUBLICATION`, `DROP ROLE`, `DROP COLLATION`,
and `DROP SCHEMA`, so those valid-but-unplanned forms become explicit
unsupported statements rather than generic syntax failures. Generated unsupported
heads that already have typed catalog/runtime support now enter the parsed DDL
family directly and are accepted only through the validated generated
unsupported boundary. Recognized generated-owned unsupported heads now require
retained unsupported AST kind, statement/command span, subject-range, and
`EXPLAIN` option-range validation before publishing unsupported, explain, or
DDL-family parsed statements, and generated parser success at SQL ingress,
including incomplete routine,
transform, text-search, and foreign-schema forms, so grammar regressions cannot
silently re-enter the legacy DDL classifier. Generated trigger DDL uses a tighter subject span that
starts after the `TRIGGER` command keyword, so `CREATE TRIGGER` and
`DROP TRIGGER` cannot reach typed update-policy or routine-trigger planning
with a broad command-tail range; unsupported materialized-view variants such as
`ALTER MATERIALIZED VIEW` are intentionally outside that allowlist until Antfly
has a typed plan for them.

## Compatibility Policy

PostgreSQL compatibility is a behavioral contract, not a source-code dependency
on PostgreSQL parser internals. PostgreSQL's grammar is tightly coupled to C
Bison actions, PostgreSQL parse-node types, catalog assumptions, extension
semantics, and release-specific server behavior. Antfly needs a smaller grammar
that maps directly to Antfly-native plans and fails closed for unsupported
semantics.

CockroachDB is the closer model: it keeps a SQL scanner, owns a generated
dialect grammar, produces its own AST, and uses PostgreSQL-compatible syntax as
a compatibility target. Antfly should follow that shape in Zig instead of
vendoring PostgreSQL grammar files wholesale.

Rules for new grammar work:

- Accept PostgreSQL syntax only when it maps to an Antfly-native typed plan or
  to an explicit unsupported-shape diagnostic.
- Preserve source byte spans for every AST node that can produce a diagnostic.
- Keep parser output catalog-free.
- Do not store raw SQL text as durable metadata, index definitions, graph
  metric configs, role settings, extension state, backup scopes, or job
  payloads.
- Prefer statement-family variants over lowerer probe order.
- Treat Antfly-specific graph, full-text, vector, enrichment, Lite, and
  algebraic-index syntax as first-class grammar branches, not post-parse string
  scans.

## Migration Plan

The migration should be incremental. The current parser stays on the production
path until generated coverage has parity for a statement family.

1. Define the supported Antfly SQL grammar subset in a checked-in grammar file.
   Start with statement families Antfly already executes: session commands,
   transactions, prepared statements, DDL, simple DML, row reads, graph DSL,
   derived indexes, full-text, Lite SQL, and extension/index options.
2. Add a parser compatibility corpus with accepted PostgreSQL-compatible
   examples, accepted Antfly extensions, and intentionally rejected PostgreSQL
   forms.
3. Run the generated parser in shadow tests against the corpus while the
   hand-written parser remains production.
4. For each statement family, prove that generated AST output lowers to the
   same typed plan shape as the current parser.
5. Switch one statement family at a time from hand-written parsing to generated
   parsing.
6. Delete obsolete hand-written parsing branches only after parity tests cover
   diagnostics, AST shape, binding, planning, and SQL/API behavior.

Suggested migration order:

1. Session and control statements: `SET`, `SET LOCAL`, `RESET`,
   `RESET ALL`, `SHOW`, `SHOW ALL`, `DISCARD ALL`, `BEGIN`, `COMMIT`,
   `ROLLBACK`, `PREPARE`, `EXECUTE`, `DEALLOCATE`,
   `PREPARE TRANSACTION`, `COMMIT PREPARED`, `ROLLBACK PREPARED`.
   The generated prepared-statement grammar accepts PostgreSQL-compatible
   `DEALLOCATE PREPARE name` in addition to `DEALLOCATE name` and
   `DEALLOCATE ALL`, and the generated AST records the actual prepared
   statement name span after the optional `PREPARE` keyword. Prepared
   transactions use a separate generated AST family from named prepared
   statements so two-phase-commit GIDs cannot be confused with statement names.
2. DDL: `CREATE DATABASE`, `CREATE SCHEMA`, `CREATE TABLE`, `ALTER TABLE`,
   `DROP`, `CREATE INDEX`, scalar/vector/full-text/graph index forms, graph
   metric declarations, and extension declarations. Simple database, schema,
   table, index, and extension DDL now has generated-parser corpus coverage
   when it matches the seed grammar. Database create/alter/drop, schema
create/drop, and extension create/alter/drop
catalog DDL now has structured generated AST payloads, direct generated
AST-to-plan lowering, and strict generated parsing at the parsed-statement
boundary, generated table/index drops validate generated object,
`IF EXISTS`, and dependency-behavior ranges before typed planning, and generated
create-index metadata covers basic
PostgreSQL-style unique, covering, partial, method, element-list, and option
clauses with generated-first AST-to-plan parity validation. Generated
`ALTER TABLE` ASTs now retain target-table and operation-tail ranges and
generated-first ALTER TABLE planning validates those ranges before lowering
through the typed DDL planner for covered add/drop/rename/validate operations
and generated-owned add-primary-key, add-unique, add-foreign-key, add-check,
and add-period constraint families.
Incomplete covered DDL clause-boundary shapes for create-table, create-index,
alter-table, and drop-table now use generated fail-closed diagnostics instead
of classifier fallback.
Unsupported DDL remains on the existing parser until
   each shape has raw AST parity.
3. Simple DML: `INSERT ... VALUES`, primary-key `UPDATE`, primary-key
   `DELETE`, `RETURNING`, and `ON CONFLICT`. Initial generated-parser coverage
   now retains raw and AST DML nodes for representative `INSERT ... VALUES`,
   `INSERT ... SELECT`, `UPDATE`, `DELETE`, `TRUNCATE`, and `MERGE` statements;
   generated DML ASTs now carry structured body ranges and conflict ranges.
   Explicit-column `INSERT ... VALUES` has direct generated AST-to-plan
   lowerers for supported row batches, including column-list, partial, and
   named constraint conflict targets, conflict actions, and field, all-field,
   and expression `RETURNING` lists; `INSERT ... DEFAULT VALUES` has a direct
   generated AST-to-plan lowerer for default row batches with conflict actions
   and returning lists; supported explicit-column `INSERT ... SELECT` has a
   generated range-validated direct insert-source lowerer for same-table and
   configured cross-table sources, conflict actions, conflict-update
   predicates and expressions, computed source projections, expression
   predicates, ordering, pagination, and returning lists including
   `RETURNING *` plus expressions;
   single-table point `UPDATE` and `DELETE` have direct
   generated AST-to-plan lowerers for generated primary/unique selector ranges
   with field, all-field, and expression returning lists; table-wide and
   single-table source `UPDATE`/`DELETE` without joined sources have generated
   range-validated direct mutation-source lowerers; explicit `UPDATE ... FROM`
   and `DELETE ... USING` have generated range-validated direct joined
   mutation-source lowerers with generated-direct coverage for separate source
   schemas, computed source assignments, expression predicates,
   source-qualified returning expressions, and lock options; simple,
   correlated, filtered, computed, row-value, and `EXISTS` semijoin `UPDATE`
   and `DELETE` mutation sources now have generated-direct parity coverage;
   non-recursive CTE write prefixes parse as generated DML, retain CTE prefix
   metadata, and have generated-direct coverage for insert-source, point
   update/delete, joined update/delete, and merge lowering; `MERGE` has a
   generated range-validated direct merge-plan lowerer with generated-direct coverage for
   multiple matched/not-matched arms, conditional arms, matched `DELETE`,
   matched/not-matched `DO NOTHING`, expression-filtered matched `UPDATE`,
   filtered not-matched `INSERT`, and `RETURNING`; and `TRUNCATE` has a direct
   generated AST-to-plan lowerer for the supported table-list, identity, and
   drop-behavior surface.
   The write-plan lowering context now dispatches through retained generated
   DML ASTs when the generated parser covers the statement, using direct
   generated AST-to-plan lowerers and failing closed when that generated
   AST-to-plan contract rejects the retained AST instead of retrying the
   legacy write classifier.
   Generated-DML-specific coverage now calls the direct generated lowerer
   without retrying the classifier path, so promoted generated DML cases fail
   closed if their AST-to-plan contract regresses.
   Direct DML relation-source paths now parse nested generated child-read ASTs
   and validate them through the shared generated read-AST contract before typed
   write-plan lowering, so `INSERT ... SELECT`, relation-source mutation paths,
   and `MERGE ... USING` wrappers do not depend only on lightweight range
   payload checks. Insert-source and recursive DML CTE retained generated
   child-read metadata now also validate `SELECT`, `FROM`, `WHERE`, and
   set-operation keyword layout plus statement/command source spans before the
   child read is reparsed. Joined mutation and merge relation-source wrappers
   apply the same retained source-span validation before wrapping their source
   body in a generated child read.
   Recursive DML CTE prefixes now reparse each recorded generated CTE body as a
   child read and validate that child through the shared generated read-AST
   contract before dispatching to typed recursive write-plan lowering; selector-based
   recursive `UPDATE`/`DELETE` forms that classify as joined mutation sources
   through their recursive CTE predicates validate the generated command body
   without requiring an explicit generated `FROM`/`USING` relation-source
   payload, and recursive generated DML delegation now fails closed unless the
   typed recursive parser consumes exactly the retained generated statement
   boundary.
   Generated DML AST kind also owns parsed-statement write-family
   classification for generated-covered writes, preserves recursive CTE write
   metadata at the parsed boundary, validates top-level command source spans
   for plain and `WITH`-prefixed writes, checks retained CTE prefix
   count/first/last/body-read compatibility plus CTE item layout and comma
   adjacency, target/source/assignment clause ranges, top-level DML keyword
   layout for `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, and `MERGE`, and required source
   child-read payloads before direct generated
   lowering, validates retained generated child-read `SELECT` source, `WHERE`,
   set-operation clause boundaries, and generated read-kind compatibility for
   insert-source and relation-source writes, and fails closed when retained generated kind metadata is missing,
   internally inconsistent, or disagrees with the legacy classifier instead of
   falling back to legacy write-family classification.
   Incomplete migrated DML clause-boundary shapes for insert, update, delete,
   truncate, `INSERT ... ON CONFLICT ... DO` tails, and `MERGE` action bodies
   now use generated fail-closed diagnostics instead of classifier fallback.
   Generated-covered write statements that end on expression operators, open
   delimiters, or quantified/pattern predicate heads in assignments,
   predicates, conflict predicates, merge join predicates, and returning lists
   also propagate generated syntax diagnostics instead of re-entering legacy
   write parsing.
   Plain DML command heads, `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, and
   `MERGE`, now require generated-parser success at SQL ingress even for
   complete-looking malformed statements, so those heads cannot fall back to
   legacy write-family classification when the generated grammar rejects them.
   Switching the full DML family from generated-parser gating plus typed
   lowerer delegation to complete generated AST-driven lowering still requires
   generated command-body AST-driven lowering for `WITH`-prefixed recursive
   CTE DML beyond
   validated generated CTE child-read bodies and direct recursive write-plan
   dispatch, broader unsupported-shape diagnostics, and full generated
   `WITH` statement-head promotion once generated coverage can distinguish read
   CTEs from write CTEs before successful generated parsing.
4. Read queries: projections, predicates, joins, CTEs, aggregates, windows,
   set operations, lateral, ordering, limits, and document-table sources.
   Initial generated-parser coverage now retains raw and AST read nodes for
   representative projection/filter/order/limit, grouped, join, lateral, and
   non-recursive CTE query shapes; generated read ASTs now carry top-level
   ranges for covered `SELECT` projection, source, `WHERE`, `GROUP BY`,
   `HAVING`, `WINDOW`, `ORDER BY`, `LIMIT`, `OFFSET`, `FETCH`, set-operation,
   and CTE-prefix bodies, plus owned list item and expression arrays for
   top-level projection, grouping, ordering, and function argument lists with
   first/last expression metadata for those lists, explicit projection
   expression/alias token splits for explicit `AS` aliases and
   PostgreSQL-style bare aliases, generated ordering direction, `USING`
   operator, and `NULLS` ordering token splits, and owned join item arrays
   with first-join compatibility metadata for join operator/type, left input,
   right input, `ON` predicate ranges, and `USING` column-list ranges, plus
   explicit join-tree root/depth metadata for generated left-associative join
   nodes, plus simple top-level comparison
   expression metadata for covered `WHERE`, `HAVING`, and join predicates; and
   normal function-call argument lists are accepted by the generated expression
   grammar for covered read projections, including `*` aggregate arguments and
   aggregate `DISTINCT` argument metadata, aggregate-local argument `ORDER BY`
   metadata, aggregate `FILTER (WHERE ...)` predicate metadata, plus ordered-set
   aggregate `WITHIN GROUP (ORDER BY ...)` metadata, and positive `LIKE`/`ILIKE`
   predicates, including PostgreSQL `ESCAPE` tails, `IN (...)`, and
   `BETWEEN ... AND ...` predicates are accepted and classified
   in generated expression metadata along with their `NOT` negated forms.
   `ANY`/`ALL`/`SOME` quantified comparison predicates over parenthesized
   expression lists and parenthesized read subqueries are accepted and
   classified with explicit quantifier token ranges and subquery token spans,
   `EXISTS` and `NOT EXISTS` read-subquery predicates are accepted with
   explicit operator, negation, and subquery token spans, and generated
   subquery expression metadata now carries subquery read kind plus first-query
   `SELECT`, projection, source, `WHERE`, and set-operation ranges plus owned
   projection-list and `WHERE` predicate expression payloads plus generated
   set-operation payloads for subquery set-operation tails,
   and quantified predicates over typed array constructors such as
   `= ANY(ARRAY[...]::text[])` carry generated array-constructor metadata after
   public tokenization normalizes cast suffixes. `IN` and `NOT IN` predicates
   over parenthesized read subqueries classify the right operand as generated
   subquery metadata. PostgreSQL-style quantified
   pattern predicates such as `LIKE ANY(...)`, `LIKE SOME(...)`, `ILIKE ALL(...)`,
   and their `NOT` variants are also accepted and carry the same generated
   quantifier plus right-side array-constructor or read-subquery metadata. Array/range containment
   and overlap predicates using `@>` and `&&` are accepted and classified with
   generated operator ranges and right-side array-constructor metadata.
   PostgreSQL JSON key predicates using `?`, `?|`, and `?&` are accepted and
   classified with generated operator ranges and array-constructor metadata for
   key-list variants. POSIX-style regex predicate operators `~`, `~*`, `!~`,
   and `!~*` are accepted and classified with generated operator ranges.
   String concatenation with `||` is accepted and classified with generated
   operator ranges in projections and predicates.
   `IS NULL` and `IS NOT NULL` predicates, plus PostgreSQL postfix `ISNULL`
   and `NOTNULL`, are accepted and classified as explicit null-test expression
   kinds, `IS TRUE`/`IS FALSE`/`IS UNKNOWN`
   boolean-test predicates are accepted with their `IS NOT` variants, and
   `IS DISTINCT FROM` / `IS NOT DISTINCT FROM` predicates are accepted with
   multi-token operator ranges. Top-level
   `AND` and `OR` predicates are classified as logical-expression metadata with
   left and right token ranges, owned child expression nodes, and owned
   per-condition expression arrays plus chain-level condition-count and
   first/last condition spans for top-level logical chains, while
   `BETWEEN ... AND ...` remains classified as a range predicate with explicit
   generated metadata for PostgreSQL `SYMMETRIC` and `ASYMMETRIC` modifiers
   plus owned lower/upper bound expression payloads.
   Prefix `NOT` predicates are accepted and classified with owned right-side
   expression nodes.
   Parenthesized expression groups carry inner token ranges and owned inner
   expression nodes, and projection, ordering, and comparison operands can
   expose unary plus/minus, additive, and multiplicative child expression-kind
   summaries, including JSON `->`/`->>`
   and JSON path `#>`/`#>>` postfix operator summaries in projections and
   predicates, function-call child summaries, direct function-call
   name and argument-list metadata, PostgreSQL `CAST(expr AS type)` value and
   target-type ranges, including temporal keyword cast targets such as
   `TIMESTAMP` and `TIMESTAMPTZ`, generated coverage for
   public-tokenizer-normalized `expr::type` inputs, generated `INTERVAL '...'` literal value ranges for
   lowerer-backed date/time functions such as `date_bin`, generated `TIMESTAMP
   '...'` / `TIMESTAMPTZ '...'` literal type/value ranges, generated
   `CURRENT_DATE` and `CURRENT_TIMESTAMP(...)` temporal keyword ranges,
   generated `EXTRACT(field FROM expression)` field/source ranges with owned
   source expression payloads, lowerer-backed
   temporal function metadata for `date_part`/`date_trunc`-style projection,
   predicate, and ordering expressions, range-bound helper metadata for
   `lower`/`upper`-style projection, predicate, and ordering expressions, and
   searched `CASE WHEN ... THEN ... ELSE ... END` branch metadata with owned
   per-branch condition/result expression lists. Generated expression
   validation now also scopes retained metadata by expression family, so
   subquery, function-call, array, cast, CASE, boolean-chain, temporal literal,
   `EXTRACT`, and operator-specific payloads fail closed when attached to a
   different generated expression kind.
   The read-plan lowering context now dispatches through retained generated
   read ASTs when the generated parser covers the statement, and generated read
   AST kind owns parsed-statement read-family classification for
   generated-covered reads, failing closed when retained generated kind
   metadata disagrees with the legacy classifier. Generated read
   ASTs now have a generated-first lowering boundary for covered simple query,
   aggregate, join, lateral, window, set-operation, non-recursive CTE, and
   recursive CTE plans, reusing the current typed read lowerers only after
   validating retained generated statement, command, clause, list, expression,
   join-tree, CTE, row-lock, and result-tail metadata. This rejects malformed
   generated range payloads, including owned projection/group/order list item
   metadata and nested generated expression range metadata, and malformed
   structural metadata for covered generated
   expression kinds such as function calls, binary and logical predicates,
   casts, `CASE` expressions, temporal literals, `EXTRACT`, arrays, subqueries,
   and grouped expressions; it also verifies that child expression AST spans
   and owned list expression AST spans match their parent token metadata, and
   expression-owned lists stay inside their owning generated expression clause
   for function arguments, aggregate-local ordering, `WITHIN GROUP` ordering,
   inline window partitions/orderings, array elements, `CASE` branches, boolean
   condition chains, subquery projections, and subquery result-tail ordering
   and pagination expressions; it also verifies clause-owned
   read lists stay inside their generated projection, grouping, ordering, CTE
   body projection/group/order, named-window partition/order, set-operation
   right-projection, and join `USING` column ranges, and that clause-owned
   first/last expression summary payloads match the first/last expression item
   spans in their generated lists. Generated expression and set-operation AST
   builders now construct recursive child/list payloads in place instead of
   relying on large recursive value returns, keeping expanded generated read
   coverage stable as more expression metadata is retained.
   Simple query reads now have
   a direct generated AST-to-query-plan lowering boundary after clause-range
   validation, and aggregate, join, lateral, window, and set-operation reads
   now have direct generated AST-to-read-family dispatch boundaries after
   clause-range validation;
   generated read classification and generated aggregate-read validation now
   recognize aggregate-function projections such as global `COUNT(*)` reads
   and aggregate CTE bodies without requiring `GROUP BY` or `HAVING` side
   effects.
Single binary join reads now validate generated join-tree metadata against
the typed join lowerer before producing a join plan, and generated join
reads reject malformed left-associative tree root/depth/index/child
metadata, first-join compatibility fields, and `ON`/`USING` condition
payloads before invoking typed join lowering. Binary `JOIN ... USING (...)`
now lowers generated column-list metadata into schema-checked equality join
keys. Generated `CROSS JOIN` lowers through an explicit cartesian row-engine
path represented as an inner join with no equality keys; generated
`NATURAL JOIN` still fails closed because it requires schema-derived common
column expansion. Generated `RIGHT` and `FULL` joins remain parsed
PostgreSQL-compatible join ASTs but fail closed at the executable join
contract until storage and API row plans grow those outer-join semantics.
   Basic `OVER (PARTITION BY ... ORDER BY ...)` and named
   `WINDOW ... AS (PARTITION BY ... ORDER BY ...)` reads now classify as
   generated window reads, inline function-call `OVER` clauses carry generated
   name/definition, partition-list, order-list, and frame-tail expression
   metadata, named windows carry generated AST items with name, definition,
   partition-list, order-list, and frame-tail ranges, seed `ROWS`/`RANGE` frame
   tails are accepted in inline and named windows with owned expression
   payloads for bounded frame offsets, CTE body named windows carry the same
   generated metadata, and generated window reads dispatch to the typed window
   lowerer after range validation.
   Plain `DISTINCT` and `DISTINCT ON (...)` reads now carry generated distinct
   ranges, `DISTINCT ON` expression-list AST items for top-level and CTE body
   reads, and preserve the production aggregate/query-family split.
   Generated expression validation now rejects mixed-shape `token_range`,
   temporal literal, and current temporal leaf expressions that carry stray
   operator, child, list, subquery, function, window, cast, case,
   boolean-chain, extract, or predicate metadata.
   Binary/operator expression validation now rejects stray unrelated payloads
   while preserving the legitimate extra metadata for negation, quantified
   predicates, `BETWEEN` bounds, `LIKE` escapes, and boolean condition chains.
   Generated operator validation also checks expression kind against concrete
   operator tokens for comparison, quantified comparison, pattern, membership,
   range, boolean, `EXISTS`, arithmetic, containment, overlap, JSON, regex,
   concatenation, `IS`, and null-safe distinct operator families.
   Prefix, `IS`, and `EXISTS` expression validation now similarly rejects
   unrelated generated payload fields while preserving each shape's required
   child and negation metadata.
   Grouped, cast, extract, array-constructor, and logical-`NOT` expression
   validation now also rejects unrelated payload fields while preserving each
   shape's required child/list metadata.
   Set-operation reads now classify as their own generated read family and
   dispatch to the native set-operation lowerer after validating the generated
   left-query, operator, `UNION ALL`, right-query projection/source, and
   right-predicate payload ranges; generated set-operation result tails now
   retain parent read ordering, `LIMIT`, `OFFSET`, and `FETCH` ranges instead
   of treating those clauses as part of the right query. Single- and multi-CTE
   reads now carry generated CTE-list, first-CTE, last-CTE, owned per-CTE name/body
   ranges, optional column-alias lists, and `MATERIALIZED` / `NOT MATERIALIZED`
   hint metadata; generated CTE reads reject malformed CTE-list counts,
   first/last compatibility fields, column-alias list payloads,
   materialization hint payloads, and parenthesized body spans without
   re-entering classifier fallback; each generated
   CTE item now carries body read-kind and first-query clause-span metadata,
   including set-operation tails with generated operator and right-query
   payloads, plus owned body projection/group/order list payloads, body
   predicate expression payloads, generated body join-tree metadata with
   first-join compatibility fields, and body pagination
   expression payloads for `LIMIT`, `OFFSET`, and `FETCH`, plus body Antfly
   table-function source item arrays and graph table-function semantic
   payloads; generated CTE lowering validates those body payloads and their
   clause keyword layout.
   Parsed generated read classification now fails closed when retained
   generated read metadata is too incomplete to derive a read family, including
   recursive CTEs whose generated final `SELECT` metadata is corrupted, instead
   of falling back to the legacy read classifier. Generated read statements
   with missing or invalid retained AST payloads now become unknown parsed
   statements at the parsed-statement boundary even when the legacy classifier
   can still name the same read family.
   Recursive CTE reads carry an
   explicit recursive flag; and simple non-recursive CTE reads dispatch to the
   typed read lowerer after validating those ranges. Recursive CTE reads now
   validate generated recursive CTE ranges and the recursive flag before
   dispatching to the typed recursive CTE lowerer. Generated CTE read lowering
   now derives non-recursive final read-family dispatch from generated
   final-select ranges and clause metadata, including final set-operation
   reads, instead of re-entering the legacy read classifier.
   Generated pagination coverage now accepts and ranges expression and
   PostgreSQL-compatible `ALL`/`NULL` `LIMIT` tails, `OFFSET` with optional
   `ROW`/`ROWS`, and `FETCH FIRST`/`FETCH NEXT` tails with optional fetch
   counts; generated metadata records limit expressions, the `LIMIT ALL`
   marker, offset expressions, and explicit fetch-count expressions, and
   simple query, simple select-set result tails, aggregate, join, lateral,
   set-operation, window, and non-recursive CTE final-read pagination now use
   generated range-validated lowering when generated read metadata is available.
   Numeric and placeholder pagination values are parsed from generated
   expression ranges instead of the full clause tail in the executable
   pagination helpers. Generated read order clauses now fail closed when the
   generated order-list item, expression, direction, `USING`, or `NULLS`
   ranges disagree with the token stream or with the span consumed by typed
   order planning. Generated projection clauses now validate generated
   item/expression/alias spans before typed select-list planning, and typed
   projection planning must consume the same generated projection span for
   generated simple, aggregate, window, join, and lateral reads. Generated
   `DISTINCT` clauses now fail closed when plain `DISTINCT` spans or
   `DISTINCT ON` item/expression spans disagree with typed query or aggregate
   distinct parsing. Generated
   source clauses now require typed source planning for generated simple,
   aggregate, window, and set-operation right-hand reads to consume the same
   retained generated source span before later clauses are planned. Generated
   aggregate `GROUP BY` clauses now similarly
   validate generated item/expression list spans before typed grouping
   planning, and typed grouping must consume the same generated group-list
   span. Generated read `WHERE` clauses on generated simple, aggregate,
   window, join, and lateral read lowerers now validate the retained generated
   predicate span and require typed predicate planning to consume the same
   clause body; simple generated read, aggregate, window, join, and lateral
   `WHERE` lowering now also threads single-atom generated predicate ASTs into
   the typed atom parsers so quantified `ANY`/`SOME`/`ALL` branches fail
   closed when the retained generated expression kind, comparison operator
   token, or quantifier token does not match the typed parser branch; typed
   expression predicate lowering similarly consumes generated single-atom
   metadata for comparison, null/boolean/distinct `IS` predicates,
   `IN`/`BETWEEN`, regex, and `LIKE`/`ILIKE` predicates; generated
   comparison predicate metadata must now agree with exact `=`, `<>`, `<`,
   `<=`, `>`, or `>=` operator tokens on direct typed-comparison handoffs;
   generated `IS`, `IS NOT`, `IS DISTINCT FROM`, `IS NOT DISTINCT FROM`,
   `ISNULL`, and `NOTNULL` predicate metadata must now agree with exact
   multi-token or postfix operator spans;
   generated
   `IN`/`BETWEEN` predicate metadata must now agree with exact operator,
   optional `NOT`, and optional `SYMMETRIC`/`ASYMMETRIC` token payloads;
   direct generated containment, overlap, and JSON-key predicate metadata
   must now agree with exact `@>`, `&&`, and `?` operator tokens;
   generated regex
   predicate metadata must now agree with the exact `~`/`~*`/`!~`/`!~*`
   operator token, and generated pattern predicate metadata must agree with
   the typed `LIKE`/`ILIKE` operator
   token and optional `ANY`/`SOME`/`ALL` quantifier token, so stale operator
   kinds or quantifier payloads cannot be accepted by the legacy expression
   lowering paths; generated child predicate expressions returned from
   generated boolean `OR`/`AND`/`NOT` groups must now be internally
   payload-valid before typed lowering can consume them, including exact
   retained operator spans; generated scalar `OR`/`NOT` predicate groups now thread the
   same child metadata into scalar comparison, boolean-test, `IN`, and
   quantified-list expansion branches; scalar atom, access predicate, direct
   join `WHERE`/`ON` predicates, and joined expression-predicate lowering now
   consume generated metadata for containment, overlap, JSON key
   existence, JSON extract comparisons, pattern, range, membership, null-test,
   boolean-test, quantified, and comparison operator families; generated logical `OR`/`AND`
   condition-list items are now
   threaded into expression alternative lowering, including inside generated
   logical `NOT` groups, so child predicate metadata is validated while
   boolean expression groups are expanded;
   aggregate `HAVING` clauses apply the same clause-span guard and now thread
   generated predicate metadata into typed output-field, output-expression,
   `OR`, and `NOT` lowering so stale comparison or boolean-test kinds fail
   closed before aggregate plans are accepted.
   Generated named `WINDOW` clauses now validate top-level window item,
   name/definition, partition list, order list, and frame expression spans
   before the typed window lowerer accepts a consumed `WINDOW` tail.
   Generated inline window `OVER` expressions now validate the generated
   `OVER` span, exact `OVER name` or `OVER (...)` token layout,
   named-window reference or inline definition, partition list, order list,
   frame expression spans, and typed window-function argument arity/ranges
   before the typed window-spec lowerer accepts the consumed projection
   expression.
   Generated aggregate projection functions now validate retained function
   name, argument, `DISTINCT`, ordered-argument, `WITHIN GROUP`, and `FILTER`
   predicate spans before the typed aggregate lowerer accepts supported
   aggregate specs, and aggregate/window `FILTER (WHERE ...)` lowering now
   threads retained generated filter predicate metadata into typed scalar,
   expression, boolean-group, and containment filter branches.
   Generated `WHERE` and aggregate `HAVING` predicates now recursively validate
   retained quantified-comparison, `EXISTS`, grouped, subquery, set-operation
   subquery, array-constructor, and boolean condition-chain boundary payloads
   before the typed lowerer accepts the generated clause span, require retained
   predicate expression spans to match their owning `WHERE`/`HAVING` bodies
   across top-level reads, CTE bodies, set-operation right-hand reads, and
   subqueries, require generated `JOIN ... ON` predicate expressions to match
   their retained `ON` bodies, and validate retained subquery `FROM`/`WHERE`
   keyword layout.
   Ordinary select-list lowering now threads generated projection expression
   AST items into the typed projection parser and verifies unambiguous
   projection starts against the retained generated expression kind, so
   malformed generated `CURRENT_DATE`/`CURRENT_TIMESTAMP`, cast, case, concat,
   unary/logical, parenthesized, and function-call projection metadata fails
   closed before fallback projection parsing can accept it; fixed select-list
   function branches now also validate the retained function-name token against
   the typed branch, including accepted aliases such as `jsonb_typeof`;
   aggregate specs now validate the retained function-name token against the
   typed aggregate op before accepting aggregate argument/filter metadata, and
   typed window specs validate the retained function-name token before
   accepting generated `OVER` metadata;
   scalar expression predicate lowering now validates exact-range generated
   function-call metadata against the typed row-expression function kind, and
   exact-range generated JSON key/extract and string-concat operator metadata
   against the typed row-expression operator kind, before accepting retained
   predicate payloads;
   select-list,
   `GROUP BY`, and order-expression item handoffs now also require the
   retained generated expression payload to be internally valid at the lookup
   boundary before the broad start-kind check is accepted, and generated
   order-expression function starts validate the retained function identity for
   case-fold, `md5`, concat, routine/extension, and general row-expression
   branches. Aggregate
   `GROUP BY` lowering now applies the same generated item lookup for simple
   group fields, ordinals, and expression group keys, so corrupted group item
   expression kinds also fail closed during typed group planning. Query,
   aggregate, window, join, and lateral `ORDER BY` lowering now threads
   generated order item expressions into output-name/ordinal and concrete
   order-expression parsing, so corrupted simple order item kinds and
   unambiguous order expression starts fail closed during typed order planning.
   Generated read validation
   rejects pagination payloads that are missing required expressions, attach
   expressions to `LIMIT ALL`, place expression spans outside their owning
   pagination tail, attach `OFFSET` expression spans to anything except the
   exact offset value plus an optional `ROW`/`ROWS` suffix, mismatch
   explicit `FETCH` count spans, or corrupt the fixed
   `FETCH FIRST`/`FETCH NEXT` plus `ROW`/`ROWS ONLY` keyword layout; it also
   validates top-level clause keyword layout and payload consistency for
   projection, plain `DISTINCT` and `DISTINCT ON`, source, `WHERE`, `GROUP BY`,
   `HAVING`, `WINDOW`, `ORDER BY`, pagination, set-operation tails and
   right-hand `FROM`/`WHERE` clauses, CTE-body `SELECT`/source clauses,
   expression-subquery `SELECT` clauses, and generated top-level and CTE-body
   join `ON`/`USING` condition keyword layout before invoking the typed
   read-family lowerers. Optional generated expression fields now treat
   scalar-only shape metadata, such as child-kind tags and branch counts, as
   real retained metadata so stale expression payloads fail closed even when no
   token range is present; optional child expression groups now reject orphan
   kind or child-expression payloads that lack a matching token range before
   lowering.
   Complete generated row-locking reads now retain generated `row_lock_tokens`
   for all PostgreSQL row-lock strengths, target lists, and wait policies, and
   lower through typed relational row-claim planning for supported relational
   reads; document SQL returns the explicit
   `DocumentSqlLockingUnsupported` diagnostic. CTE read bodies now retain
   `body_row_lock_tokens`, validate the `FOR` row-lock boundary, and clone that
   metadata into direct generated read ASTs. CTE read bodies also retain,
   validate, and clone body-level Antfly and graph table-function metadata so
   generated child-read planning sees the same table-function source semantics
   as direct top-level reads. Parsed-statement classification now also
   validates generated Antfly and graph table-function source counts, source
   item ranges, named-argument ranges, graph compatibility fields, and
   required graph semantic payloads for both top-level read sources and CTE
   body sources before publishing a generated read family. Incomplete generated read
   clause-boundary shapes for
   `SELECT`/`WITH`, source clauses, predicates, grouping, having filters,
   incomplete boolean and comparison operator tails, ordering,
   `DISTINCT`/`DISTINCT ON`, set operations, unambiguous pagination result
   tails, row-locking clauses, incomplete named `WINDOW` clause item tails,
   joins, and CTE `AS` bodies now require
   generated parsing and fail closed instead of falling back to the legacy read
   classifier. Read expressions that end on generated-owned operators, open
   function/group delimiters, or quantified/pattern predicate heads such as
   `= ANY`, `LIKE ANY`, `@>`, regex operators, and `||` also propagate the
   generated syntax diagnostic instead of re-entering legacy read parsing.
   Generated read CTE prefixes now validate the exact `WITH`/`WITH RECURSIVE`
   list boundary, each CTE item name and optional column-alias group, required
   `AS`, optional `MATERIALIZED`/`NOT MATERIALIZED`, body parentheses, comma
   separation, and final item boundary before CTE body planning consumes the
   generated metadata.
   Specialized generated expression nodes now validate exact token layout for
   `INTERVAL` literals, typed `TIMESTAMP`/`TIMESTAMPTZ` literals,
   `CURRENT_TIMESTAMP[(precision)]`, `EXTRACT(field FROM source)`, and
   `ARRAY[...]` constructors before read lowering consumes their retained
   expression payloads.
Generated join-tree validation now also requires exact adjacency between
left input, join operator, right input, and `ON`/`USING` condition ranges,
and verifies that retained join operator tokens match the generated join
kind before typed join planning consumes the metadata. Parsed-statement
classification now also validates retained generated join-item arrays,
left-associative tree root/depth/child links, first-join compatibility fields,
and top-level/CTE-body `ON`/`USING` payload consistency before publishing a
generated read family. Read CTE classification now also validates CTE item
layout, comma adjacency, optional column-list payloads, and materialization
keyword metadata before publishing a generated read family. It also validates top-level and CTE-body generated
projection, `DISTINCT ON`, grouping, and ordering list payloads, including
first/last expression summaries, alias spans, direction/`USING` operator
ordering, `NULLS` ordering, and delimiter adjacency before publishing a
generated read family, validates generated set-operation right-query
projection/source/predicate payloads, right-arm projection adjacency after
`SELECT`/`DISTINCT`, and right-arm `FROM`/`WHERE` clause keyword layout, and
requires top-level and CTE-body `WHERE`/`HAVING` generated predicate expression
spans to match their retained clause bodies. Named `WINDOW` clauses now also validate generated window item
boundaries, `name AS (...)` layout, partition/order list payloads, and frame
expression spans before publishing a generated read family. Join classification
now also validates join operator token sequences against generated join kind
metadata and rejects conditionless join metadata unless the generated join kind
is actually conditionless. Generated row-lock clauses now validate the complete
`FOR` strength, optional `OF` target list, and `NOWAIT`/`SKIP LOCKED` wait
policy layout instead of only checking the leading `FOR` token. Read
classification also verifies that top-level and CTE-body clause body ranges are
preceded by the expected clause keywords, including `FROM`, `WHERE`, `GROUP BY`,
`HAVING`, `WINDOW`, `ORDER BY`, `LIMIT`, `OFFSET`, and `FETCH`, before a
generated read family is published.
   Switching reads from fallback to required generated parsing still requires
   broader PostgreSQL-compatible grammar coverage, richer projection,
   grouping, and ordering expression planning semantics beyond the current
   validated owned expression item arrays, full multi-join
   planning/lowering and richer join-tree semantics beyond the current
   validated binary inner/left/cross join nodes with retained `ON`/`USING` or
   conditionless cartesian payload layout, remaining exact join-item
   segment/tail validation and fail-closed right/full plus conditionless
   `NATURAL JOIN` executable-contract coverage, expression AST
   planning/lowering beyond the current recursive
   predicate/operator/subquery-tail metadata and structural checks, broader function
   semantic planning outside the currently range-validated generic, aggregate,
   and window function surfaces, broader boolean expression-tree coverage, quantified and `EXISTS`
   subquery semantic planning/lowering beyond retained generated payload
   validation, remaining specialized expression operator semantics beyond
   exact generated token-layout validation, richer
   inline window-expression semantic planning beyond current generated exact `OVER`
   layout and argument-range validation, broader recursive CTE semantic planning beyond the
   current generated-first lowering boundary and validated CTE prefix/item/body
   clause/list/expression/join/window/pagination/row-lock metadata, and
   unsupported-shape diagnostics.
5. Advanced DML: `INSERT ... SELECT`, `UPDATE ... FROM`, `DELETE ... USING`,
   `TRUNCATE`, and `MERGE`.
6. Antfly extensions: graph traversal DSL, graph metric query surfaces,
   automatic embeddings, full-text ranking, algebraic indexes, enrichment
   clauses, lake/source syntax, and Lite-specific capability checks. Seed and
   rich `CREATE GRAPH INDEX`, `CREATE GRAPH METRIC`, and
   `ALTER GRAPH INDEX ... ADD METRIC` statements now have generated
   graph-family corpus coverage, retained generated AST nodes, and generated
   AST-to-plan wrappers for typed graph index and graph metric index plans.
   Runtime parsing requires generated-parser success for migrated `CREATE GRAPH`
   and `ALTER GRAPH` DDL heads, so incomplete graph DDL fails closed instead of
   falling back to the legacy DDL classifier. Parsed-statement classification
   for generated graph DDL now validates retained graph AST kind and
   statement/command spans before publishing the DDL family, so corrupted graph
   AST payloads fail closed before typed graph catalog planning.
   Canonical `antfly.*` query table-function sources now have generated
   grammar coverage, named-argument coverage, source/name/argument AST ranges,
   owned named-argument item/name/operator/value ranges, list-based Antfly
   function kind metadata across joined sources, graph function subset metadata,
   graph-specific semantic argument payloads for table/index selectors,
   start/target selectors, result-ref selectors, pattern returns, metric names,
   and query text, and fail-closed generated-read validation that rejects
   duplicate named arguments and corrupted or missing required graph payloads
   before typed lowering. Direct relational table-function lowering now accepts
   `antfly.graph_neighbors(...)`, `antfly.graph_shortest_path(...)`,
   `antfly.graph_k_shortest_paths(...)`, and `antfly.graph_metric(...)`
   alongside traversal and match table functions, preserves the
   generated-covered graph-query and metric source shapes through typed
   relation source planning, supports joins between graph-match and
   graph-metric table functions, and materializes direct graph query and graph
   metric rows through the existing graph table-function row contract. The
   generated-covered relation-source lowerer now consumes the retained
   generated Antfly and graph table-function AST payload directly instead of
   wrapping graph table-function tokens in synthetic `SELECT * FROM ...`
   statements for reparsing, and fail-closes when generated source payloads are
   missing or corrupted. CTE bodies now retain the same Antfly and graph
   table-function source metadata, validate it at the CTE boundary, and rebase
   it into the direct generated read AST used by child body planning. Broader
   graph unsupported diagnostics now require generated `MATCH ... RETURN ...`
   unsupported ASTs to retain the graph-query reason and exact subject span
   before parsed-statement classification publishes the terminal unsupported
   statement. Broader graph DSL cutover still requires deeper graph-query
   semantic planning for non-table-function graph syntax and broader
   unsupported-shape diagnostics.

## Generator Performance

The generator and generated parser should be optimized for build speed,
incremental development, runtime latency, and memory locality. SQL parsing is
on request paths, CLI paths, tests, and dashboard REPL paths, so generator
choices should be measured rather than treated as purely build-time details.

Important performance requirements:

- **Small generated surface**: generate only the supported Antfly grammar, not
  the full PostgreSQL grammar. Large unreachable productions slow builds,
  increase binary size, and create unsupported syntax that later phases must
  reject.
- **Deterministic generation**: generated files must be stable across runs so
  reviews and cache keys do not churn.
- **Fast incremental builds**: grammar generation should depend only on grammar
  inputs and generator code. Ordinary SQL lowerer or executor edits should not
  regenerate parser artifacts.
- **Scanner/parser split**: keep lexical scanning separate from grammar
  reduction so tokenization can remain allocation-light and reusable for
  fingerprinting, diagnostics, read-only classification, and query formatting.
- **Compact token ids**: use dense token and nonterminal ids so parse tables are
  cache-friendly and cheap to serialize into generated Zig. Generated metadata
  exposes typed token-id helpers, and the SQL facade maps fixed token kinds
  through those helpers instead of runtime string symbol-table probes. Dynamic
  keyword normalization uses a token-only generated lookup instead of scanning
  all grammar symbols, and generated runtime metadata no longer exposes a
  catch-all symbol-name lookup for parser ingress.
- **Indexed parse tables**: generated action/goto tables carry per-state row
  ranges and use binary search inside the active row instead of scanning the
  full table. Generated action and goto rows elide the redundant state id
  because row membership is already encoded by `action_ranges`/`goto_ranges`;
  `sql-parser-bench` reports the compact entry widths (`action=6`,
  `goto=4`, `range=8`) and the current static table footprint
  (`static_bytes=1081392`). Future compression can move from state-elided row
  ranges to row displacement, packed transition arrays, default reductions, or
  another measured scheme if table size or lookup cost becomes material.
- **Allocation-light AST construction**: allocate AST nodes in an arena owned
  by `ParsedSql`; avoid per-token heap allocation and avoid copying token text
  unless normalized text is required. Generated parser metadata now also emits
  stack-buffer parse and diagnostic entrypoints, and the SQL facade uses fixed
  token-id and parser-state buffers for normal statements with allocator-backed
  fallback only for unusually large statements.
- **Span by offset**: store byte offsets and lengths into the original SQL
  buffer instead of materializing substrings for every identifier, literal, or
  diagnostic target.
- **Keyword metadata once**: classify keywords during scanning or tokenization
  and reuse that metadata for parsing, binding, read-only classification, and
  diagnostics.
- **Error recovery bounds**: cap recovery work after syntax errors. Interactive
  SQL REPL and dashboard use need useful diagnostics, but malformed input must
  not trigger quadratic parser behavior.
- **No runtime grammar loading**: generated parse tables should be compiled into
  the binary. Runtime should not read grammar files or dynamically build parser
  tables.
- **Shadow parser budget**: while the generated parser runs in tests or debug
  validation beside the current parser, keep it behind explicit test/debug
  paths so production requests parse once.

`zig build sql-parser-bench -- <iterations>` runs the first parser-only
microbenchmark over the generated SQL compatibility corpus, including
Antfly-specific table-function reads. It pre-tokenizes the corpus and measures
generated parse throughput, token throughput, latency percentiles, gross
allocated bytes per statement, peak live parser bytes, generated table counts,
generated RHS/state item counts, action/goto row-density statistics, and
generated parse table byte estimates.
Track generated Zig compile time and binary size impact with build-system
metrics around that target. Add larger end-to-end SQL benchmarks only after a
statement family actually switches to the generated parser.

## Parser Shape

The first generated output is a small AST-compatible facade, not a full rewrite
of binding and planning. The active target is:

```text
current SQL fixture
  -> generated parser
  -> raw statement-family facade with source spans
  -> existing binder/lowerer entry point
  -> same typed plan or same unsupported diagnostic
```

This avoids mixing syntax migration with execution changes. If a statement
family currently performs ad hoc token inspection, migrate that inspection into
an AST node before switching the family to the generated parser.

The generated parser facade currently produces source-span-bearing closed
variants for:

- session statement, including a generated AST payload for command, name, and
  value token ranges, plus generated AST-to-plan parity for generated-covered
  session catalog commands
- transaction statement, including a generated AST payload for command spans
  and optional `WORK`/`TRANSACTION` boundary-tail token ranges or transaction
  mode token ranges, plus generated AST-to-plan parity for generated-covered
  transaction boundary commands, PostgreSQL `END` commit aliases, and
  `SET TRANSACTION`, `START TRANSACTION`, and `BEGIN` transaction-mode commands
- prepared statement, including a generated AST payload for command, name,
  argument, and nested-statement token ranges, plus generated AST-to-plan parity
  for typed `PREPARE`, `EXECUTE`, and `DEALLOCATE`
- DDL statement, including generated AST payloads for command spans, object
  names, catalog option fields, drop behavior, and generated AST-to-plan parity
  for database and schema catalog plans plus seed `CREATE TABLE`, `ALTER TABLE`,
  `DROP TABLE`, `DROP INDEX`, materialized-view catalog, logical-replication
  catalog, and row-security policy catalog plans
- DML statement, including generated AST payloads for command spans, target
  tables, source/body ranges, predicates, conflict clauses, returning clauses,
  values lists, default-values inserts, truncate options, generated-first
  write-plan dispatch, direct generated AST-to-plan
  lowerers for supported explicit-column `INSERT ... VALUES`,
  `INSERT ... VALUES ... ON CONFLICT`, `INSERT ... DEFAULT VALUES`, insert
  `RETURNING`, single-table point `UPDATE`/`DELETE` with returning projections,
  and `TRUNCATE`, and an initial AST-to-plan wrapper for the other
  generated-covered write statements
- read statement, including a generated AST payload for command spans and an
  generated-first AST-to-plan dispatch for generated-covered read statements,
  backed by validated wrappers into the current typed read lowerers
- extension/index statement, including a closed generated family for
  `CREATE INDEX`, `DROP INDEX`, `CREATE EXTENSION`, and `DROP EXTENSION` with
  DDL-compatible AST payloads for object names, index target tables, index
  methods, index element lists, covering-index include lists, partial-index
  predicates, index options, extension options, unique-index flags, and drop
  behavior, plus generated-first AST-to-plan parity checks for covered
  create-index clauses
- graph statement, including a generated AST payload for command spans and
  graph-specific AST-to-plan wrappers for seed and rich graph index and graph
  metric DDL;
  canonical `antfly.*` table-function reads are represented on generated read
  ASTs as source-level Antfly function item metadata, with graph function items
  retained as a subset carrying graph-specific semantic argument payloads rather
  than as standalone graph statements; `antfly.graph_metric_rerank` now lowers
  as a relational row-source table function by owning the parsed full-text plus
  graph-metric rerank search request and materializing reranked hits through
  the existing graph table-function row schema
- cursor statement, including generated AST payloads for command spans and
  typed tail token ranges for `DECLARE`, `FETCH`, `MOVE`, and `CLOSE`, plus
  generated-first AST-to-plan lowering into typed cursor portal plans
- prepared transaction statement, including generated AST payloads for command
  spans, action kind, and GID token ranges for `PREPARE TRANSACTION`,
  `COMMIT PREPARED`, and `ROLLBACK PREPARED`, plus generated AST-to-plan
  lowering that fails closed when retained generated metadata disagrees with
  the token stream
- unsupported statement, including generated AST payloads for seed `ANALYZE`,
  `COPY`, `VACUUM`, `REINDEX`, `CLUSTER`, `COMMENT`, `GRANT`, `REVOKE`,
  `LISTEN`, `NOTIFY`, `LOCK`, `CALL`, `CHECKPOINT`, `LOAD`,
  `SECURITY LABEL`, `UNLISTEN`,
  `ALTER MATERIALIZED VIEW`, foreign table,
  foreign data wrapper, foreign schema import, user mapping, language,
  unsupported routine/language/transform DDL, large-object administration,
  rule, server, and trigger DDL
  forms, plus bare, simple, optioned, and `EXPLAIN ANALYZE` forms
  with command spans, subject ranges where present, and stable unsupported
  reason metadata. Parsed SQL now distinguishes
  generated unsupported statements from ordinary DDL for unsupported heads that
  have no typed catalog/control plan, so lowerers fail closed instead of trying
  a generic DDL parse for those shapes. Generated unsupported AST heads that
  already have typed catalog/control plans enter the parsed DDL family and are
  accepted only through the validated generated unsupported boundary; unsupported
  materialized-view variants such as `ALTER MATERIALIZED VIEW` are part of the
  fail-closed path rather than the materialized-view catalog boundary.
  `CREATE TRIGGER` and `DROP TRIGGER` are now split at this boundary: supported
  update-policy triggers lower to existing update-policy plans, supported
  routine row triggers lower to `trigger_catalog`, and richer PostgreSQL trigger
  variants still fail closed with unsupported diagnostics.

Later statement-family cutovers should add closed variants for:

- broader unsupported PostgreSQL-compatible statements with diagnostic reasons,
  beyond the first generated utility/control diagnostic set

Those variants should become the normal dispatch boundary for binder and lowerer
code.

Generated parser diagnostics expose bounded parse diagnostics with token index,
source byte span, actual token text, and expected terminal names through the
generated parser facade. Raw parser state, lookahead symbols, parse tables,
rules, and symbol metadata stay private so the generated parser is a syntax
boundary rather than a second SQL control plane. That is the parse-phase shape
the dashboard REPL, CLI, HTTP SQL endpoint, and corpus tests should use instead
of string-only unsupported reasons.

## Testing And Evidence

Generated grammar work needs evidence at multiple levels:

- Corpus tests for accepted PostgreSQL-compatible syntax. The initial checked
  corpus covers session commands, transaction commands, prepared statements,
  simple database/schema/table DDL, extension/index statements,
  representative DML, and representative read queries. It also covers seed and
  rich graph statements as a distinct generated family. Runtime parsing also
  enforces generated parser success for the session, transaction, and prepared
  statement corpus.
- Corpus tests for accepted Antfly-specific syntax. The generated corpus now
  covers canonical `antfly.full_text_search`, `antfly.semantic_search`,
  `antfly.vector_search`, `antfly.graph_traverse`,
  `antfly.graph_neighbors`, `antfly.graph_shortest_path`,
  `antfly.graph_k_shortest_paths`, `antfly.graph_match`,
  `antfly.graph_metric`, and `antfly.graph_metric_rerank` table-function read
  source syntax with named arguments; executable relational row-source lowering
  covers graph traversal, neighbors, shortest-path, k-shortest-path, match,
  graph metric score, and graph metric rerank search-hit rows.
- Corpus tests for intentionally unsupported PostgreSQL syntax with stable
  diagnostics. Seed `ANALYZE`, bulk I/O `COPY`, maintenance `VACUUM`/`REINDEX`,
  utility/control statements such as `CLUSTER`, `COMMENT`, `GRANT`/`REVOKE`,
  `LISTEN`/`NOTIFY`, `LOCK`, `CALL`, `CHECKPOINT`, `DISCARD`, `LOAD`,
  role session controls,
  `SECURITY LABEL`, `UNLISTEN`, PostgreSQL foreign-data
  declarations for foreign data wrappers, foreign tables, schema imports, servers, and user
  mappings, plus language, unsupported routine/language/transform DDL,
  large-object administration, rule, trigger, conversion, event-trigger, extended
  statistics, operator/aggregate ALTER forms, operator class/family, and text-search
  configuration/dictionary/parser/template DDL, rich catalog option shapes for
  `CREATE DATABASE` and `CREATE SCHEMA`, multi-target `DROP TABLE`, `DROP INDEX`,
  `DROP EXTENSION`, `DROP VIEW`, `DROP MATERIALIZED VIEW`, `DROP DOMAIN`,
  `DROP SEQUENCE`, `DROP TYPE`, `DROP PUBLICATION`, `DROP ROLE`, `DROP COLLATION`,
  and `DROP SCHEMA`, and bare, simple, optioned, and `EXPLAIN ANALYZE` forms now produce generated unsupported AST nodes with
  stable reason metadata, explain-option payloads, and subject ranges where
  available.
- AST shape tests for source spans, identifier normalization, literals,
  placeholders, casts, operators, and nested statements. The first AST shape
  tests cover generated session, transaction, prepared, cursor, DDL, DML, read,
  and graph statement payloads, including top-level generated read-list metadata.
- Plan parity tests showing generated ASTs lower to the same typed plans as the
  current parser for migrated statement families. Session catalog commands,
  transaction boundaries, prepared statements, simple DDL database/schema
  catalog plans plus seed `CREATE TABLE` and `DROP TABLE` plans, and
  extension/index `CREATE INDEX`, `DROP INDEX`, plus extension catalog plans
  have generated AST-to-plan parity tests for their generated-covered forms;
  simple catalog DDL also has generated
  field-level checks for object names, option flags, version strings, drop
  behavior, ALTER TABLE operation tails, fail-closed unsupported clauses, and
  malformed database/schema/extension catalog DDL heads that no longer fall
  back to legacy DDL probing.
  Runtime DDL lowering now dispatches generated session statements, prepared
  statements, prepared transactions, graph DDL, database/schema/extension
  catalog DDL, generated
  `ALTER DATABASE ... SET ...` and `ALTER EXTENSION ... UPDATE` catalog DDL,
  `CREATE TABLE` including serial identity-allocation tables, `DROP TABLE`,
  generated schema namespace rename DDL for `ALTER SCHEMA ... RENAME TO ...`,
  generated tablespace catalog DDL for `CREATE TABLESPACE`, `ALTER TABLESPACE`,
  and `DROP TABLESPACE`,
  generated logical-replication catalog DDL for
  `CREATE`/`ALTER`/`DROP PUBLICATION` and
  `CREATE`/`ALTER`/`DROP SUBSCRIPTION`,
  generated materialized-view catalog DDL for `CREATE MATERIALIZED VIEW`,
  `DROP MATERIALIZED VIEW`, and `REFRESH MATERIALIZED VIEW`,
  generated row-security policy catalog DDL for `CREATE POLICY`,
  `ALTER POLICY`, and `DROP POLICY`,
  generated routine catalog DDL for `CREATE FUNCTION`,
  `CREATE OR REPLACE FUNCTION`, `CREATE PROCEDURE`, `DROP FUNCTION`, and
  `DROP PROCEDURE`,
  generated trigger catalog DDL for supported routine row-trigger
  `CREATE TRIGGER` and `DROP TRIGGER` forms, with update-policy trigger
  compatibility still lowering through the existing update-policy catalog plan,
  generated role authorization DDL for `CREATE ROLE`, `ALTER ROLE`, and
  `DROP ROLE`, including PostgreSQL-compatible `USER` and `GROUP` aliases
  where they lower to the same authorization catalog role plans while
  preserving `USER MAPPING` as an explicit unsupported boundary,
  generated type-system catalog DDL for `CREATE/ALTER/DROP COLLATION`,
  `CREATE/DROP OPERATOR`, `CREATE/DROP AGGREGATE`, and `CREATE/DROP CAST`,
  `DROP INDEX`, generated domain catalog DDL for `CREATE DOMAIN`,
  `ALTER DOMAIN`, and `DROP DOMAIN`, generated sequence catalog DDL for
  `CREATE SEQUENCE`, `ALTER SEQUENCE`, and `DROP SEQUENCE`, generated
  enum type catalog DDL for `CREATE TYPE`, `ALTER TYPE`, and `DROP TYPE`,
  generated
  `CREATE INDEX` including retained partial predicate indexes with field or
  expression predicates, and generated
  `ALTER TABLE ADD/DROP/RENAME COLUMN`, `ADD PRIMARY KEY`, `ADD UNIQUE`,
  `ADD FOREIGN KEY`, `ADD CHECK`, `ADD PERIOD`, plus `VALIDATE CONSTRAINT`
  for single-operation statements and selected comma-separated statements
  where every top-level segment is one of those operation families,
  and `ALTER TABLE ... ENABLE/DISABLE ROW LEVEL SECURITY` through the
  generated AST-to-plan boundary; generated session lowering
  preserves catalog setting/search-path plans and adapter noops while failing
  closed when retained command/name/value metadata is malformed. Generated
  prepared-transaction lowering has parity coverage for `PREPARE TRANSACTION`,
  `COMMIT PREPARED`, and `ROLLBACK PREPARED`, and malformed generated action or
  GID ranges fail closed before reaching typed planning.
  Remaining rich DDL compatibility debt is limited to generated metadata that
  still cannot represent every semantic subshape natively, especially broader
  ALTER subcommands outside the supported runtime DDL operation families.
  Simple DML has generated field-level checks for update and truncate body
  ranges, direct generated AST-to-plan parity for truncate mutation-source
  plans, direct resolver-free generated AST-to-plan coverage for supported
  explicit-column insert-values row batches with column-list, partial, and
  named constraint conflict targets, conflict actions, and returning lists,
  default-values row batches with conflict actions and returning lists, direct
  generated AST-to-plan coverage for supported same-table and cross-table
  insert-select requests with conflict and returning lists plus retained
  generated child-read validation for insert-select source bodies, direct generated
  AST-to-plan coverage for single-table point update/delete batches with
  returning lists, direct generated AST-to-plan coverage for table-wide and
  single-table source update/delete mutation-source requests without joined
  sources, direct generated AST-to-plan coverage for explicit update-from and
  delete-using joined mutation-source requests with generated child-read wrapper validation for
  relation source bodies, direct generated AST-to-plan
  coverage for merge plans with generated child-read wrapper validation for `USING` relation
  source bodies, and non-recursive CTE write prefixes across
  insert-source, point update/delete, joined update/delete, and merge,
  generated-direct validation and dispatch with retained generated per-CTE body
  metadata plus generated child-read validation for recursive CTE insert-source, update, delete, and merge
  write-plan variants,
  generated-first write lowering context
  dispatch, generated DML AST kind now drives parsed-statement write-family
  classification for generated-covered writes with fail-closed classifier
  disagreement checks, fail-closed generated DML dispatch when retained
  generated AST metadata is malformed, and generated-family validation over
  other representative write plans. Read plans have initial
  generated AST-to-plan parity through generated-first read lowering context
  dispatch with the same generated read AST handoff into simple, aggregate,
  join, lateral, window, CTE, and recursive CTE typed lowerers that production
  runtime uses; generated read AST kind now drives parsed-statement read-family
  classification for generated-covered reads with fail-closed classifier
  disagreement checks plus generated pagination payload consistency checks
  before a parsed read family is assigned; generated read classification also
  validates retained statement/command spans, top-level `WITH`/`SELECT`
  boundary metadata, projection/source/join/predicate/group/window/order/result-tail
  ranges, and CTE count/first/last/body range compatibility before publishing
  the parsed read family, and generated-family validation wrappers over
  representative query, aggregate, join, lateral, window, set-operation, and
  non-recursive CTE plans,
  AST-shape coverage for
  generated-ranged multi-CTE and recursive CTE
  prefixes, binary `JOIN ... USING (...)` lowering through schema-checked
  equality keys, single- and multi-join component range/tree coverage with
  fail-closed executable-contract validation for unsupported generated
  join/lateral multi-join ASTs, and simple comparison plus
  positive/negated predicate expression-shape coverage for read predicates,
  including escaped `LIKE`/`ILIKE` pattern metadata and fail-closed
  expression-owned token range containment plus operator/kind token
  consistency validation, including `IS NULL`/boolean-test predicate keyword
  consistency and postfix `ISNULL`/`NOTNULL` shape checks, generated function
  call metadata now fails closed when argument `DISTINCT`/`ORDER BY`, `WITHIN
  GROUP`, or `FILTER (WHERE ...)` clause ranges disagree with their keywords
  and parentheses, generated grouped, `CAST(... AS ...)`, and `CASE
  WHEN ... THEN ... ELSE ... END` expression metadata now fails closed when
  child ranges disagree with clause keywords and enclosure tokens, generated
  projection/group/order alias metadata now fails closed when `AS name` or
  bare alias ranges disagree with the underlying list-item tokens, and
  generated ordering lists now fail closed when typed `ASC`/`DESC`, `USING`
  operator, or `NULLS FIRST`/`LAST` metadata disagrees with the underlying
  order-item tokens. Optional generated expression payload checks now also
  reject scalar-only stale shape metadata, including orphan branch counts and
  child-kind tags. Generated CTE body clause spans now fail closed when
  retained `WHERE`, `GROUP BY`, `HAVING`, `WINDOW`, `ORDER BY`, `LIMIT`,
  `OFFSET`, or `FETCH` payload ranges are not preceded by their matching
  clause keywords.
  Subquery expression tests cover generated
  `ORDER BY`, `LIMIT`, `OFFSET`, and `FETCH` tail payloads plus fail-closed
  malformed subquery tail validation, including recursive checks for retained
  subquery result-tail expression payloads.
  Graph DDL has generated AST-to-plan parity for graph index and graph metric
  index plans, including rich graph index declarations and
  `ALTER GRAPH INDEX ... ADD METRIC`, malformed graph DDL is rejected through
  the generated parser instead of legacy DDL fallback, and generated read AST tests cover canonical
  `antfly.*` table-function source ranges and named-argument item ranges,
  including duplicate argument rejection and joined graph sources, plus
  fail-closed malformed Antfly and graph-source validation at both the
  parsed-statement classification boundary and the executable lowering
  boundary. Plan/lowering tests
  now cover `antfly.graph_metric(...)` and `antfly.graph_metric_rerank(...)`
  as direct relational table-function sources, graph metric joins with graph
  match sources, and query-function transfer of parsed graph metric and graph
  metric rerank queries into owned table-function CTEs.
- SQL/API parity tests showing SQL and native API requests reach the same
  service contracts.
- Fuzz or mutation tests for scanner/parser crash resistance and bounded error
  recovery. A deterministic malformed SQL corpus now exercises generated
  source-aware diagnostics for incomplete read, CTE, DDL, DML, and unsupported
  statement shapes. The default unit-test pass keeps a short seeded deterministic
  scanner/parser mutation loop over accepted statement seeds and random
  SQL-token streams. Longer generated parser fuzzing now lives behind
  `zig build sql-parser-fuzz -- --cases <count> [--seed <seed>]`, mutates the
  full generated compatibility corpus including Antfly-specific reads, first
  requires every declared corpus seed to parse as its expected statement family,
  and then requires every mutated/random case to parse, reject as an unsupported
  token shape, or produce a bounded generated parser diagnostic.
- Parser microbenchmarks for corpus throughput, allocation count, parse-table
  size, generated-code compile time, and binary size. The initial
  `sql-parser-bench` target now reports generated parser corpus throughput,
  latency percentiles, allocation totals, peak live parser bytes, and generated
  table counts plus generated parse-table byte estimates.

The migration is complete only when production SQL ingress no longer depends on
hand-written statement parsing for the migrated families and compatibility debt
is represented as explicit unsupported diagnostics rather than parser probes or
string scans.
