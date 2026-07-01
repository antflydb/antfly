# SQL.md Remaining Work Slices

This is the current backlog for the remaining large `SQL.md` work. It omits
completed tracking context and keeps only slices that still need implementation,
parity proof, or cleanup. Each checkbox should describe a landable artifact:
code behavior, diagnostics, fixture coverage, runtime parity, or a deletion.

Feature and plan-parity coverage belongs in named entries in
`zig/pkg/antfly/src/sql/fixtures/sql_api_parity_source_corpus.json`, with new
required buckets added to the coverage manifest when a feature becomes part of
the tracked contract. Runtime result parity that needs seeded rows should live
in executable Zig integration tests, with the JSON corpus naming the SQL shapes,
plan summaries, and coverage buckets. Local Zig tests should otherwise prove
helper behavior such as structured-summary extraction, fixture validators,
inventory validators, and diagnostic classifiers.

## Document SQL

- [ ] **Admit document table DDL through SQL**
  - [ ] Parse canonical document-table DDL in the generated parser:
        `CREATE TABLE ... WITH (antfly.storage_mode = 'document',
        antfly.default_type = ..., ...)` plus one or more
        `DOCUMENT SCHEMA name AS JSON '...'` clauses.
  - [ ] Bind the parsed DDL into a typed catalog request summary containing
        table name, `storage_mode = document`, `default_type`, and normalized
        `document_schemas`; durable schema metadata must not preserve raw SQL
        text.
  - [ ] Add source-corpus success fixtures for the canonical DDL shape and
        required coverage buckets for document-table DDL admission.
  - [ ] Decide and fixture the `CREATE DOCUMENT TABLE` shorthand contract:
        either parsed structural rejection, or sugar for the same typed
        document-table DDL request.
  - [ ] Promote generated-failure placeholders for mixed relational/document
        DDL into parsed diagnostic fixtures once document-table DDL parses:
        relational columns, relational constraints, generated columns, and
        other PostgreSQL column-table semantics on a document table.
  - [ ] Promote generated-failure placeholders for schema validation failures
        into parsed diagnostic fixtures once document-table DDL parses:
        malformed schema JSON, missing or unknown `default_type`, invalid
        Antfly extensions, invalid dynamic templates, and unsupported
        multi-document-type shape.
  - [ ] Add SQL/native catalog parity fixtures proving SQL-created document
        table schemas match equivalent native/API schema mutations.
  - [ ] Remove or rename the old generated-parse-failure tracking entries after
        every formerly unparsed DDL shape has a parsed success or parsed
        diagnostic fixture.

- [ ] **Admit document writes through SQL**
  - [ ] Keep unsupported write fixtures in the source corpus for broad insert,
        update, delete, truncate, and merge shapes until each shape is admitted
        by a named slice below.
  - [ ] Add one shared document-write preflight used by native writes and SQL
        document writes; fixture the rejection order for authorization,
        row-filter, audit-required, conflict, and no-match behavior before any
        write form is admitted.
  - [ ] Admit explicit-key full-document insert/upsert:
        `INSERT INTO docs (_id, _doc) VALUES (...)` lowers to native document
        insert/upsert and has corpus parity for document shape, duplicate key,
        malformed `_doc`, schema validation, generated-field rejection, and
        audit behavior.
  - [ ] Admit generated-id full-document insert only after native document id
        generation policy is shared and fixture the SQL/native `_id` behavior.
  - [ ] Admit exact-key delete:
        `DELETE FROM docs WHERE _id = ...` and `_id IN (...)` lower to native
        document delete and have corpus parity for deleted identity,
        row-filter/audit behavior, no-match behavior, and rejection behavior.
  - [ ] Keep non-identity delete predicates rejected until boundedness,
        row-filter, authorization, audit, and no-match semantics have explicit
        source-corpus diagnostics.
  - [ ] Pick one explicit `_doc` patch/update surface, such as
        `jsonb_set(_doc, ...)` or an Antfly-owned
        `antfly.json_patch(_doc, patch)` helper, and fixture JSON path
        assignment, removal, merge, null handling, arrays, type validation,
        stale-filter behavior, conflict handling, and audit ordering.
  - [ ] Admit the selected `_doc` patch/update surface by lowering to native
        document patch/update requests; keep generated-field updates,
        unsupported JSON patch shapes, and broad projection-field updates as
        stable diagnostics.
  - [ ] Admit projection-column writes only after virtual-schema write
        semantics are proven for field paths, nullability, type validation,
        defaults, generated-field rejection, and `additionalProperties`; admitted
        fixtures must construct JSON documents or native document patches, never
        relational row inserts or mutation-source plans.
  - [ ] Admit `TRUNCATE docs` only after document table-emptying semantics have
        bounded deletion, audit, row-filter, and identity-reset behavior pinned.
  - [ ] Admit `MERGE INTO docs ...` only after document match, insert, update,
        delete, no-op, conflict, audit, and row-filter ordering semantics are
        pinned.

- [ ] **Finish document query and view-mapping hardening**
  The current tracking manifests are
  `zig/pkg/antfly/src/sql/fixtures/document_sql_bounded_scan_inventory.json` and
  `zig/pkg/antfly/src/sql/fixtures/document_sql_read_expansion_gate.json`. Keep
  those manifests in sync with this section whenever an admitted bounded-scan
  contract is removed or a blocked read-expansion surface is admitted.
  The current single-array equality-filtered `UNNEST` shape is proven by
  `api.table_reads.docid document sql view mapping runtime results match native
  document reads`, which requires
  `DocumentSqlCapabilities.indexed_array_element_paths` and asserts the native
  `indexed_query` path.
  Bounded-scan admission is narrowed to the
  `mapped-view-residual-bounded-scan` inventory entry; uninventoried mapped-view
  range, `IN`, ordered, array/`UNNEST`, and other predicate families now require
  an exact indexed/native producer or fail with
  `document_sql_bounded_scan_missing_exact_producer`.

  - [ ] Prove bounded-scan residual behavior is exact before deleting the last
        fallback. The remaining `mapped-view-residual-bounded-scan` contract
        needs runtime evidence for row caps, byte caps, residual filtering,
        limit interaction, ordering rejection, and the stable
        `document_sql_bounded_scan_missing_exact_producer` diagnostic. Delete
        the inventory entry only in the same patch that replaces it with an
        exact native/indexed producer and matching runtime parity test.
  - [ ] Admit `additional-array-unnest-patterns`: add JSON corpus fixtures for
        multiple arrays, nested arrays, aliasing, non-equality predicates, empty
        arrays, missing arrays, and rejected cartesian-expansion shapes; add
        required coverage buckets and executable runtime parity tests proving
        exact native `array_any`/indexed-array behavior before expanding the
        current single-array equality-filtered contract.
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
