# SQL.md Remaining Work Slices

This breaks the remaining large `SQL.md` work into implementation slices ordered
by dependency and risk. The document-table items map to the "Remaining large
pieces" list in `SQL.md`; the whole-adapter items capture the broader parser,
expression, parity, and compatibility-wrapper work that `SQL.md` still tracks.
Each slice should land with focused planner/runtime tests and, where applicable,
SQL/API parity coverage.

## Document SQL

- [x] **Document SQL diagnostics**
  - [x] Return specific unsupported diagnostics for joins, `OFFSET`/`FETCH`,
        windows, locking tails, projection modifiers, view mappings, writes,
        and unsupported aggregate shapes.
  - [x] Keep generic `UnsupportedSqlShape` only for genuinely uncategorized
        parser/lowering failures.
  - [x] Add focused tests for every diagnostic code.

- [x] **Document aggregate polish**
  - [x] Add aggregate `FILTER` support or a precise unsupported diagnostic.
  - [x] Add multi-key `GROUP BY` diagnostics before broader support.
  - [x] Extend `HAVING` from one scalar comparison to simple boolean
        conjunctions where semantics stay exact.
  - [x] Keep SQL-side processing order explicit: aggregate, `HAVING`,
        aggregate `ORDER BY`, then `LIMIT`.

- [ ] **Broader document expressions**
  - [x] Admit casts only where type proof is available.
  - [x] Add JSON-path function equivalents shared with relational `json` /
        `jsonb` expression handling.
  - [x] Add date, numeric, text, and boolean helper coverage incrementally.
    - [x] Add fail-closed ASCII `lower(path) = 'literal'` text predicate
          coverage over `_id`, capped native-candidate, and bounded-scan
          residual producers.
    - [x] Add fail-closed ASCII `upper(path) = 'literal'` text predicate
          coverage over `_id` and capped native-candidate residual producers
          with the shared exact residual evaluator.
    - [x] Add fail-closed `abs(numeric_path) <op> non_negative_number`
          predicate coverage over `_id`, capped native-candidate, and
          bounded-scan residual producers.
    - [x] Add fail-closed `date_utc(datetime_path) <op> 'YYYY-MM-DD'` UTC day
          predicate coverage over indexed, `_id`, capped native-candidate, and
          bounded-scan producers.
    - [x] Add fail-closed `path IS TRUE|FALSE|NOT TRUE|NOT FALSE` boolean
          predicate coverage over indexed, `_id`, capped native-candidate, and
          bounded-scan producers.
  - [x] Fail closed when an expression cannot be executed by a native producer
        or exact bounded residual evaluation.

- [ ] **Array expansion V2**
  - [x] Add diagnostics for unsupported multiple or nested `UNNEST` shapes.
  - [ ] Extend beyond single-field `UNNEST` only with explicit cardinality and
        boundedness rules.
  - [x] Add array-element predicate support when the producer can prove exact
        semantics.
  - [ ] Add native indexed array-element producer support as a separate access
        path, not as a semantic requirement.

- [ ] **Derived-index function parity**
  - [ ] Lower semantic, vector, hybrid, graph, graph-metric, and rerank SQL
        functions to native derived-index producers.
  - [ ] Check source binding, snapshot/generation, schema fingerprint, and
        freshness before admitting a producer.
  - [ ] Keep each function producer-independent at the SQL semantics layer.
  - [ ] Add parity tests against native document query APIs for each function.

- [ ] **Residual and bounded-scan exactness**
  - [ ] Tighten `DocumentSqlRequiresBoundedScan` reasons and user-facing
        diagnostics.
  - [x] Report row-cap and byte-cap failures distinctly.
  - [x] Maintain a residual predicate coverage matrix.
  - [ ] Add fail-closed tests for partial scans, cap hits, unsupported residual
        predicates, and ordered top-k over bounded input.
    - [x] Cover bounded row-cap and byte-cap failures through table-read source
          execution tests.
    - [x] Cover ordered top-k cap failures over bounded scan and bounded
          full-text producers.
    - [x] Cover unsupported SQL residual expression shapes and unknown or
          malformed runtime residual filter JSON.

- [ ] **Document SQL views**
  - [ ] Add SQL view definitions as stable document-to-SQL schema mappings.
  - [ ] Store path mappings, types, nullability, and dependency metadata in
        catalog state.
  - [ ] Derive virtual schema from view mappings.
  - [ ] Add invalidation or rejection when source schema/index metadata no
        longer satisfies the view contract.

- [ ] **Document SQL/native e2e parity**
  - [ ] Prove `_id` lookup parity.
  - [ ] Prove scalar filter and JSON-path predicate parity.
  - [ ] Prove full-text producer parity.
  - [ ] Prove bounded-scan and residual-filter parity.
  - [ ] Prove aggregate, aggregate `ORDER BY`, `HAVING`, and `UNNEST` parity.

- [ ] **Document writes**
  - [ ] Start only after read path, authorization, row filters, and audit
        semantics are shared with native document writes.
  - [ ] Define explicit `_id` / `_doc` insert semantics.
  - [ ] Define typed JSON patch/update semantics.
  - [ ] Define trigger, constraint, generated-field, and audit ordering.
  - [ ] Lower writes into native document write paths, not relational row
        batches.

## Whole SQL Adapter

- [ ] **Parser and grammar migration**
  - [ ] Migrate one statement family at a time to generated structured AST
        paths.
  - [ ] Add raw AST coverage first, then binder validation, then lowering.
  - [ ] Preserve unsupported-shape diagnostics at each migration step.
  - [ ] Remove family-specific compatibility parsing only after parity evidence
        exists.

- [ ] **Broader expression coverage**
  - [ ] Expand text, JSON, array, regex, datetime, numeric, and query-function
        expression families.
  - [ ] Route checks, generated columns, partial predicates, expression indexes,
        conflict actions, update transforms, aggregate filters, `HAVING`, order
        keys, windows, `RETURNING`, and rewrite `USING` through shared typed
        expression infrastructure.
  - [ ] Keep unsupported expression classifications structural and
        allocation-light.

- [ ] **Structured parity tooling**
  - [ ] Emit parser, binder, plan, and unsupported-shape summaries.
  - [ ] Have fixtures consume structured summaries or coverage bits instead of
        rescanning SQL strings.
  - [x] Add an initial JSON-backed document SQL corpus for residual runtime
        filters and unsupported bounded-residual expression cases.
  - [ ] Split fixture freshness checks from behavioral parity checks.
  - [ ] Add coverage gates for native requirements and unsupported reasons.

- [ ] **Cross-surface parity**
  - [ ] Keep SQL, HTTP, SDK, MCP, A2A, CLI, and native internal paths aligned for
        each shared feature.
  - [ ] Cover DDL, DML, reads, writes, derived-index lifecycle, roles,
        extensions, backups, lakes, and unsupported cases.
  - [ ] Assert that supported SQL plans lower to the same native model as the
        equivalent Antfly-native API call.

- [ ] **Compatibility wrapper removal**
  - [ ] Inventory string-only and legacy compatibility wrappers.
  - [ ] Gate removal on typed parser/binder/plan parity evidence.
  - [ ] Delete one wrapper class at a time.
  - [ ] Keep compatibility wrappers only when they have a named compatibility
        reason and cannot change durable catalog or row state.
