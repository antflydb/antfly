# SQL Adapter Architecture

This package owns Antfly's SQL adapter. Its job is to accept PostgreSQL-like
SQL syntax and produce Antfly-native typed plans, catalog mutations, and runtime
requests. SQL syntax details should be contained here; storage, API, and
durable plan layers should receive typed Antfly structures rather than raw SQL
or parser-local syntax state.

## Pipeline

The SQL pipeline is intentionally staged:

```text
SQL text
  -> lexer tokens with source spans
  -> adapter-private PostgreSQL AST and generated parser metadata
  -> binder and type checks over schema/catalog metadata
  -> Antfly-native typed plans
  -> durable planning, catalog application, or storage execution
```

Raw SQL and PostgreSQL syntax details should not cross the binder/lowerer
boundary. When a later layer needs evidence for diagnostics, corpus coverage, or
fingerprints, pass typed adapter evidence rather than token slices whenever
possible.

## Package Boundaries

The major files have these responsibilities:

- `token.zig`: token kinds, keyword tags, token spans, and token-local
  predicates.
- `lexer.zig`: SQL lexical handling. Token text should usually reference the
  input buffer; allocate owned text only when decoding requires it.
- `ast.zig`: adapter-private PostgreSQL syntax AST. These nodes are temporary
  and should not be stored durably.
- `parser.zig`: parser cursor helpers, statement dispatch, and grammar
  entrypoints.
- `grammar.zig`: catalog-independent PostgreSQL syntax normalization and
  grammar helpers, such as identifier lists, object-name normalization,
  set-operation grammar, aliases, and tail-clause recognition.
- `value.zig`: SQL literal, parameter, JSON, interval, datetime, and scalar
  value parsing. DDL, DML, and expression lowering should share this boundary
  instead of carrying local literal parsers.
- `binder.zig`: catalog and schema lookup, identifier normalization,
  source-name resolution, output-column inference, and type validation.
- `plan.zig`: adapter-owned plan containers, clone/free helpers, projection
  containers, lowered read/write structures, and ownership utilities.
- `ddl_plan.zig`: DDL and catalog plan containers plus DDL-specific syntax to
  typed-plan lowering.
- `lower_select.zig`, `lower_dml.zig`, and `lower_ddl.zig`: statement-family
  lowering entrypoints. These modules should coordinate parsing, binding, and
  expression helpers, then return typed plans.
- `lower_expr.zig`: shared expression and predicate lowering. This is currently
  broader than the target shape; see the extraction plan below.
- `diagnostics.zig`: span-aware unsupported-shape diagnostics and stable
  required-feature classification.
- `corpus.zig`, `app_parity_*`, and `*_corpus.zig`: SQL/API parity fixtures,
  golden-plan fingerprints, and coverage assertions.

## Import Direction

Prefer a layered dependency direction:

```text
token/lexer/value/string helpers
  <- grammar/parser AST helpers
  <- binder
  <- expression/predicate helpers
  <- statement lowerers
  <- durable planning, executor, catalog application, corpus
```

Cycles make this package hard to reason about. A shared helper should usually
move downward into a smaller leaf file instead of making two high-level modules
import each other. Migration slices should update call sites to import the
narrow owner directly; avoid adding facade aliases for moved helpers.

## Expression Layer

Expressions are shared by SELECT projections, WHERE/HAVING predicates, ORDER BY,
RETURNING, default values, generated columns, check constraints, partial
indexes, conflict actions, row-security predicates, and query fingerprints. The
adapter should keep one definition of:

- SQL function and operator recognition.
- Row-expression lowering into `RelationalRowsExpression`.
- Predicate lowering into scalar, access, JSON, text, array, and expression
  predicate surfaces.
- Expression equality and aggregate-spec equivalence.
- Expression dependency walking.
- Determinism and catalog-safe expression validation.
- Simple predicate disjointness proofs used by set-operation rewrites and
  planning checks.

That shared ownership does not mean every query lowerer belongs in the
expression module. Expression helpers should be reusable leaves. Statement
modules should own statement structure.

## Expression Package

Expression behavior lives under `sql/expr/`. Lowerers import the leaf module
they need directly, while `sql/expr/mod.zig` exists as the package namespace for
public consumers that want the whole expression subsystem. Do not re-export
these helpers through `lower_expr.zig` or old `expr_*` aliases in `mod.zig`.

Current expression modules:

- `expr/token.zig`: expression keyword, function-name, operator, start-token,
  and tail-boundary classification.
- `expr/operator.zig`: operator matching, comparison parsing, JSON extraction
  operator recognition, cast-type parsing, and `IS`/null-test tails.
- `expr/build.zig`: construction helpers for `RelationalRowsExpression`,
  expression projections, datetime/interval row expressions, function
  expressions, and JSON extraction expressions.
- `expr/type.zig` (`expr.typing` from `expr/mod.zig`): expression serialization/fingerprints, expression names,
  determinism, dependency walking, type compatibility, row-expression type
  context, aggregate input validation, and expression-driven catalog
  validators.
- `expr/limits.zig`: shared expression expansion limits.
- `expr/generated.zig`: generated-parser payload and identity validation for
  row expressions, projections, predicates, order keys, windows, and read
  clauses.
- `expr/parse.zig`: row-expression operand-start classification and focused
  row-expression parse helpers.
- `expr/predicate.zig`: expression predicate builders and value-list expansion.
- `expr/disjoint.zig`: simple predicate and expression-condition disjointness.
- `expr/equal.zig`: row-expression, projection, aggregate-spec, and
  unique-expression equality.
- `row_claim.zig`: row-claim clause conversion, generated metadata validation,
  names, and fingerprint labels. This remains outside `expr/` because it owns a
  statement/runtime claim surface, not a general expression helper.

Remaining extraction targets:

- Move remaining row-expression parsing bodies from `lower_expr.zig` into
  `expr/parse.zig` when their dependencies can stay expression-local.
- Move scalar/access predicate lowering and WHERE atom parsing into either
  `expr/predicate.zig` or a focused statement lowerer when catalog ownership is
  clearer.
- Continue shrinking `lower_expr.zig` around SELECT, aggregate, window, join,
  lateral, CTE, and set-operation lowering.

During extraction:

- Move call sites to the new owner in the same slice; do not leave forwarding
  aliases in `lower_expr.zig` or `mod.zig`.
- Move tests with the behavior they cover, not all at once.
- Prefer deleting import cycles over preserving old convenience imports.
- Treat a smaller public API as a deliverable. Helpers that only serve one
  module should become private in that module.
- Keep ownership and free/deinit helpers near the data structures they manage.

## Statement Lowering

Statement-family lowerers should own statement shape:

- SELECT lowerers own projection lists, source clauses, CTEs, set operations,
  joins, lateral sources, aggregates, windows, row locks, pagination, and output
  column derivation.
- DML lowerers own INSERT, UPDATE, DELETE, MERGE, conflict actions, mutation
  source validation, RETURNING, and mutation-specific expression restrictions.
- DDL lowerers own catalog plan construction and DDL-specific validation that is
  not general catalog binding.

These modules should ask the binder for resolved catalog/schema facts and ask
expression/predicate helpers for typed expression surfaces. They should not
duplicate SQL token predicate chains or schema lookup logic.

## Binder Boundary

The binder owns catalog-backed meaning:

- Normalized table, source, alias, CTE, column, period, index, constraint, and
  routine lookup.
- Scope and qualifier resolution.
- Output schema inference for reads, CTEs, joins, lateral sources, aggregates,
  windows, and RETURNING where catalog context matters.
- Type compatibility checks that depend on catalog column metadata.

Grammar can parse names. Binder decides what those names mean.

## Generated Parser Metadata

Generated parser ASTs are validation evidence, not the primary semantic model.
Lowerers may use generated ranges and expression kinds to fail closed when
handwritten parsing and generated metadata disagree. After validation, the
owned output should still be Antfly-native typed plans and expression structs.

Generated metadata validation belongs in narrow modules such as
`expr/generated.zig` or statement-specific generated validators, not scattered
through unrelated lowerers.

## Tests

Keep tests beside the behavior owner:

- Lexer and token tests stay with lexical handling.
- Grammar syntax and normalization tests stay with `grammar.zig`.
- Binder tests cover catalog/schema resolution and type checks.
- Expression tests cover expression parsing, equality, analysis, and predicate
  lowering.
- Statement lowerer tests cover typed plan shapes.
- Integration, corpus, app-parity, and storage execution tests stay beside
  their owning integration surface.

When extracting from `lower_expr.zig`, move the smallest coherent test group
with the extracted behavior. It is acceptable for `lower_expr.zig` facade tests
to remain temporarily while call sites migrate.

## Adding SQL Semantics

When adding support for a new SQL feature:

1. Put pure syntax recognition in `grammar.zig` or a narrow expression-token
   helper.
2. Put literal and parameter parsing in `value.zig`.
3. Put catalog lookup and name/type resolution in `binder.zig`.
4. Put reusable expression semantics in the expression/predicate helper layer.
5. Put statement shape and typed-plan assembly in the relevant lowerer.
6. Add tests at the lowest owning layer plus one typed-plan or integration test
   when the feature crosses module boundaries.

Do not add parser-local wrappers that duplicate binder, value, or expression
logic. If a helper is needed by multiple statement families, extract a narrow
shared module rather than growing a statement lowerer sideways.
