# SQL Adapter Remaining Work Slices

This is the tracking document for remaining SQL adapter work. It lists only
slices that still need implementation, parity proof, or cleanup. Keep design
rationale, architecture decisions, ownership boundaries, and implementation
invariants in `SQL.md`. Completed history that is deducible from code, tests,
fixtures, or git history should stay out of both docs.

Each checkbox should describe a landable artifact: code behavior, diagnostics,
fixture coverage, runtime parity, or a deletion. When a slice lands with those
artifacts, delete its checkbox rather than leaving completed history here.

Feature and plan-parity coverage belongs in named entries in
`zig/pkg/antfly/src/sql/fixtures/sql_api_parity_source_corpus.json`, with new
required buckets added to the coverage manifest when a feature becomes part of
the tracked contract. Runtime result parity that needs seeded rows should live
in executable Zig integration tests, with the JSON corpus naming the SQL shapes,
plan summaries, and coverage buckets. Local Zig tests should otherwise prove
helper behavior such as structured-summary extraction, fixture validators,
inventory validators, and diagnostic classifiers.

## Document SQL

Document-table DDL design lives in `SQL.md`; this tracker only lists remaining
document SQL implementation gates.

- [ ] **Admit document writes through SQL**
  Admit each document-write shape only after it lowers to the typed native
  document request model and has SQL/native parity evidence. Keep
  `ON CONFLICT`, `RETURNING`, source-query projection inserts, broad
  projection updates, broad insert/update/delete, truncate, and merge rejected
  until the specific slice below admits that shape with corpus and runtime
  proof.

  - [ ] Admit indexed-producer document mutations.
        Lower `UPDATE docs SET ... WHERE mapped_field <op> value` and
        `DELETE FROM docs WHERE mapped_field <op> value` through
        `LoweredDocumentProducerMutation` only when the predicate has an exact
        indexed/native producer. Close this with equality, `IN`, range,
        boolean/null, nested mapped-path, version-predicate, no-match,
        stale-version, residual-rejection, corpus-summary, coverage-bucket, and
        runtime-parity tests.
  - [ ] Admit bounded-scan document mutations.
        Lower residual-filtered `UPDATE` and `DELETE` only when the producer
        carries explicit row and byte caps plus a stable residual predicate.
        Close this with cap-enforcement, ordering/limit rejection or exact
        semantics, partial-match, no-match, audit/filter/auth ordering,
        corpus-summary, coverage-bucket, and runtime-parity tests.
  - [ ] Admit joined and source-derived document mutations.
        Lower `UPDATE docs ... FROM ...`, `DELETE FROM docs USING ...`, and
        source-derived assignments only when the join/source has deterministic
        cardinality, deterministic ordering, and an exact native candidate
        producer. Close this with duplicate-source handling, assignment
        expression binding, version predicates, conflict/no-match behavior,
        stable rejection of relational mutation-row fallbacks, corpus summaries,
        required coverage buckets, and runtime parity.
  - [ ] Admit projection-write `ON CONFLICT`.
        Lower `DO NOTHING` and `DO UPDATE` only to native document write
        semantics, with explicit behavior for `_version`, generated fields,
        nested-path assignments, conflict target validation, conflict/no-op row
        reporting, corpus plan summaries, required coverage buckets, and runtime
        parity.
  - [ ] Admit projection-write `RETURNING`.
        Return `_id`, `_doc`, virtual fields, projection aliases, generated
        columns, conflict/no-op rows, and updated version metadata with the same
        shape as the native/API path. Close this with corpus plan summaries,
        required coverage buckets, runtime parity, and stable diagnostics for
        unsupported returning expressions.
  - [ ] Admit source-query projection writes.
        Lower `INSERT INTO docs (...) SELECT ...` only when the source has
        deterministic ordering and bounded cardinality or an explicit native
        producer contract. Prove source ordering, source limits, duplicate-key
        handling, conflict behavior, schema validation, corpus plan summaries,
        required coverage buckets, runtime parity, and stable rejection of
        relational mutation-source plans.
  - [ ] Admit `TRUNCATE docs`.
        Close this with source-corpus fixtures and runtime parity for document
        table-emptying, bounded deletion, audit-required behavior, row-filter
        interaction, conflict/no-match reporting, identity-reset behavior, and
        stable diagnostics for unsupported restart/cascade variants.
  - [ ] Admit `MERGE INTO docs ...`.
        Close this with source-corpus fixtures and runtime parity for document
        match, insert, update, delete, no-op, conflict, audit-required,
        row-filter, stale-filter, generated-field, schema-validation, and
        no-match ordering semantics.

- [ ] **Finish document query and view-mapping hardening**
  Keep the tracking manifests
  `zig/pkg/antfly/src/sql/fixtures/document_sql_bounded_scan_inventory.json` and
  `zig/pkg/antfly/src/sql/fixtures/document_sql_read_expansion_gate.json`. Keep
  those manifests in sync with this section whenever an admitted bounded-scan
  contract is removed or a blocked read-expansion surface is admitted.
  Uninventoried mapped-view range, `IN`, ordered, array/`UNNEST`, and other
  predicate families require an exact indexed/native producer or fail with
  `document_sql_bounded_scan_missing_exact_producer`.

  - [ ] Replace `mapped-view-residual-bounded-scan` with an exact producer.
        Add runtime evidence for row caps, byte caps, residual filtering, limit
        interaction, ordering rejection, and the stable
        `document_sql_bounded_scan_missing_exact_producer` diagnostic. Delete
        the inventory entry only in the same patch that replaces it with an
        exact native/indexed producer and matching runtime parity tests.
  - [ ] Admit `additional-array-unnest-patterns`: add JSON corpus fixtures for
        multiple arrays, nested arrays, aliasing, non-equality predicates, empty
        arrays, missing arrays, and rejected cartesian-expansion shapes; add
        required coverage buckets and executable runtime parity tests proving
        exact native `array_any`/indexed-array behavior before expanding the
        admitted array-unnest contract.
  - [ ] Admit `additional-mapped-fields`: add JSON corpus fixtures for scalar,
        numeric, boolean, text, optional, missing, nested, and multi-field view
        mappings; add required coverage buckets and executable runtime parity
        tests proving exact native/indexed producers, projection shape, residual
        behavior, and stable diagnostics for unmapped or mistyped fields.
  - [ ] Admit `derived-index-producer-types`: add JSON corpus fixtures for every
        document derived-index producer shape SQL can select, including stale,
        missing, partial, ordered, and rebuild-in-progress indexes; add required
        coverage buckets and executable runtime parity tests proving SQL and
        native derived-index reads choose the same producer or emit the same
        rejection.
  - [ ] Admit `document-aggregates`: add JSON corpus fixtures for mapped-field
        `COUNT`, `MIN`/`MAX`, `SUM`/`AVG`, grouped aggregates, `HAVING`, order,
        and limit shapes; add required coverage buckets and executable runtime
        parity tests proving aggregate results, residual filtering, empty input,
        null handling, and rejection of unbounded aggregate scans.
  - [ ] Admit `lateral-view-mapping-joins`: add JSON corpus fixtures for each
        allowed lateral document/view-mapping join shape, join predicate family,
        limit interaction, and unsupported correlated form; add required
        coverage buckets and executable runtime parity tests proving row
        identity, cardinality, residual filtering, and stable diagnostics.

## Whole SQL Adapter

- [ ] **Migrate parser and grammar paths family by family**
  - [ ] Pick one family from
        `zig/pkg/antfly/src/sql/fixtures/sql_parser_migration_table.json` and
        add raw generated-AST fixtures for every currently supported syntax
        shape before binder validation.
  - [ ] Add binder and lowering fixtures for that family in
        `sql_api_parity_source_corpus.json`, including stable diagnostics for
        stale generated metadata and rejected unsupported shapes.
  - [ ] Add API/runtime parity evidence for the selected family and update the
        migration-table removal gate with the exact fixture names that prove it.
  - [ ] Delete one selected compatibility parser path only after raw AST,
        binder, lowering, runtime, and API/native parity evidence all exist.
  - [ ] Repeat the same fixture-backed workflow for the remaining migration
        table families: DDL, DML, query, roles, extensions, backups, lakes,
        functions, and maintenance commands.

- [ ] **Expand shared typed expression coverage**
  - [ ] Publish one shared typed-expression contract for text, JSON, array,
        regex, datetime, numeric, boolean, and query-function expressions.
  - [ ] Route `CHECK` constraints through the shared expression binder,
        lowerer, dependency tracker, and diagnostics path.
  - [ ] Route generated columns through the shared expression path, including
        dependency tracking and write-time evaluation/rejection.
  - [ ] Route partial-index predicates and expression-index elements through
        the shared expression path.
  - [ ] Route `ON CONFLICT` actions and `UPDATE` transforms through the shared
        expression path.
  - [ ] Route aggregate filters, `HAVING`, order keys, windows, and
        `RETURNING` expressions through the shared expression path.
  - [ ] Route `ALTER TABLE ... ALTER COLUMN ... TYPE ... USING` clauses through
        the shared expression path.
  - [ ] Add diagnostics tests proving unsupported expression classifications
        remain structural and allocation-light.

- [ ] **Prove cross-surface parity**
  - [ ] Define a canonical native-equivalence summary for SQL plans and
        native/API requests, covering catalog mutations, row writes, document
        writes, reads, and derived-index lifecycle operations.
  - [ ] Add DDL parity fixtures that compare SQL catalog plans with equivalent
        native/API catalog mutations and pin unsupported-case diagnostics.
  - [ ] Add DML parity fixtures that compare SQL row/document write plans with
        equivalent native/API mutation requests, including authorization,
        row-filter, audit, and no-match behavior where applicable.
  - [ ] Add read/query parity fixtures that compare SQL read plans with
        equivalent native/API query requests, including residual predicates,
        bounded scans, ordering, limits, aggregates, and UNNEST behavior.
  - [ ] Add derived-index lifecycle parity fixtures that compare SQL index DDL
        with equivalent native/API derived-index lifecycle requests, including
        rebuild, drop, stale-index, and native-query parity.
  - [ ] Add roles, extensions, backups, lakes, and maintenance parity fixtures
        that classify each shape as native-equivalent, explicit no-op, or
        unsupported with stable diagnostics.
  - [ ] Gate each supported SQL feature on lowering to the same native model as
        the equivalent Antfly-native API call.

- [ ] **Remove compatibility wrappers**
  - [ ] Pick one wrapper entry from
        `zig/pkg/antfly/src/sql/fixtures/sql_compatibility_wrappers.json` whose
        deletion gate is fully satisfied, then delete that wrapper and its
        fallback call sites in one focused patch.
  - [ ] Add regression tests proving the deleted compatibility path now uses
        the typed parser, binder, plan, runtime, and parity route named by its
        inventory entry.
  - [ ] Update the compatibility-wrapper inventory in the same patch that
        deletes a wrapper, either removing the entry or narrowing it to the
        remaining compatibility contract.
  - [ ] Keep only wrappers with a named compatibility reason that cannot change
        durable catalog, document, or row state.
    - [ ] For any kept compatibility contract, keep its `contract_reason` in the
          validator allowlist and preserve the durable-state proof that it is
          read-only or no-op.
    - [ ] For any remaining migration blocker, keep explicit typed parser,
          binder, plan, runtime, and parity deletion evidence in the inventory.
