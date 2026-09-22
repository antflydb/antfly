# SQL compiler boundary

This is an executable, bounded subset of S2, not a claim of parity with the
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
- Window ranking, offset/value functions and aggregates with PARTITION BY,
  ORDER BY, peer-aware RANGE and explicit ROWS frames. Equal sort domains share
  sorting; moving aggregates use bounded indexed state. Window evaluation runs
  after grouping/HAVING and before final ordering/limits under the same budget.
- Equality-correlated EXISTS/NOT EXISTS and scalar subqueries, including direct
  scalar aggregates, lower to grouped hash joins rather than per-row reads.
  All physical tables participate in the same authorized statement capture.
  Scalar subqueries preserve zero-row NULL and SQLSTATE 21000 for multiple rows.
- UNION/INTERSECT/EXCEPT with ALL/distinct multiplicities, INTERSECT precedence,
  parenthesized operands and final ordering/limits. Typed equality keeps JSON
  null distinct from SQL NULL; set operands share the statement memory budget.
- `INSERT INTO ... (columns) VALUES (...) [, ...]` with scalar expressions and
  parameters. All rows are prepared and validated before one atomic mutation.
- `INSERT INTO ... (columns) SELECT ...` with typed source values, assignment
  checks and bounded whole-statement preparation. Source cursors close before
  commit, including self-inserts; source failure cannot publish a partial batch.
- `UPDATE ... SET column = expression [, ...] [WHERE ...]`.
- `INSERT ... ON CONFLICT (_id) DO NOTHING` and `DO UPDATE SET ... [WHERE ...]`.
  Assignments bind old-row and `excluded` values once, with native defaults and
  generated values. Skipped rows remain atomic read-set fences, never deletes;
  affected counts and RETURNING omit them. Concurrent arbiter changes fail as
  definite serialization conflicts, without implicit replay. Coordinated
  backends also resolve explicit complete unique-column targets through the
  native tuple codec and generation-bound claim authority, including composite
  keys and native NULL-distinct behavior. Exact claim/absence guards travel with
  the atomic mutation and survive session staging, savepoints, and recovery.
  Omitted targets and providers without native arbiter coordination fail closed.
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

Names can be `table`, `namespace.table`, or `database.namespace.table`. Unquoted
identifiers fold ASCII case; quoted identifiers preserve their exact names.
`_id` is reserved for the backend's opaque row identity, not a schema primary
key declaration. INSERT without `_id` uses the shared native secure identity
provider once during preparation; the native expected-absent fence prevents
collisions from overwriting a row. Providers without that capability require an
explicit `_id`. Integer literals are signed 64-bit values parsed exactly.

## Deliberate exclusions

Named windows, GROUPS/EXCLUDE frames and DISTINCT window aggregates remain
unsupported. Subquery IN/ANY/ALL, non-equality correlation, nested subquery
expressions inside an inner subquery, per-key ORDER/LIMIT, set/group/HAVING/window
subquery forms, complex aggregate expressions, lazy CASE/COALESCE subquery
branches, and mutation-expression subqueries
remain unsupported. EXISTS currently admits literal/field projections; richer
expressions need a validation-only binding domain to avoid evaluating discarded
values. Partial/expression/deferrable conflict arbiters, targetless inference and
SQL-language prepared statements/cursors remain separate capability gates.
These implemented forms do not constitute the complete S2 parity gate.
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
