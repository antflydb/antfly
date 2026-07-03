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
plan summaries, and coverage buckets. Public SQL endpoint parity that needs a
real `ApiHttpServer` belongs in
`zig/pkg/antfly/src/api/public_sql_endpoint_parity.zig`; keep
`zig/pkg/antfly/src/api/http_server.zig` focused on routing, session, auth, DDL,
and server-mechanics behavior. Local Zig tests should otherwise prove helper
behavior such as structured-summary extraction, fixture validators, inventory
validators, and diagnostic classifiers.

Document SQL read-plan and residual-expression evidence belongs in
`zig/pkg/antfly/src/sql/fixtures/document_sql_corpus.json`. If a document-write
slice needs fixture-backed lowering evidence, extend that fixture with a write
case class in the same patch rather than proving document-only behavior solely
through the SQL/API parity corpus.

## Document SQL

Document-table DDL design lives in `SQL.md`; this tracker only lists remaining
document SQL implementation gates.

- [ ] **Finish document query and view-mapping hardening**
  Keep the tracking manifests
  `zig/pkg/antfly/src/sql/fixtures/document_sql_bounded_scan_inventory.json` and
  `zig/pkg/antfly/src/sql/fixtures/document_sql_read_expansion_gate.json`. Keep
  those manifests in sync with this section whenever an admitted bounded-scan
  contract is removed or a blocked read-expansion surface is admitted.
  The bounded-scan inventory is currently empty. Mapped-view range, `IN`,
  ordered, array/`UNNEST`, scalar residual, and other
  predicate families require an exact indexed/native producer or fail with
  `document_sql_bounded_scan_missing_exact_producer`.

  - [ ] Admit correlated lateral subqueries over mapped document views.
        Land only shapes whose native producer proves branch cardinality, row
        identity, correlation binding, deterministic ordering, limit
        interaction, residual filtering, and relational fallback rejection,
        with executable SQL/native/API parity for result rows and diagnostics.

## Whole SQL Adapter

- [ ] **Migrate parser and grammar paths family by family**
  - [ ] For each family in
        `zig/pkg/antfly/src/sql/fixtures/sql_parser_migration_table.json`,
        expand `sql_generated_ast_migration_fixtures.json` to cover every
        currently supported generated-parser syntax shape before binder
        validation.
  - [ ] For each family, add binder, lowering, runtime, and API/native parity
        fixture names to that family's `removal_evidence`, with each referenced
        source entry present in `sql_api_parity_source_corpus.json`.
  - [ ] Add stable diagnostics for each family's stale generated metadata and
        rejected unsupported shapes, and list those fixture or test names in the
        same `removal_evidence` object.
  - [ ] Delete compatibility parser paths one family at a time only after the
        migration-table evidence validator proves raw AST, binder, lowering,
        runtime, API/native parity, stale metadata, and unsupported-shape
        evidence all exist for the deleted paths.
