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

- [ ] **Admit document writes through SQL**
  Admit each document-write shape only after it lowers to the typed native
  document request model and has SQL/native parity evidence. Keep
  broad source-query projection inserts, broad projection updates, broad
  insert/update/delete, and non-admitted conflict, returning, or merge variants
  rejected until the specific slice below admits that shape with corpus and
  runtime proof.

  - [ ] Admit source-query projection writes.
        Current baseline: same-table `INSERT INTO docs (_id, ...) SELECT _id,
        ... FROM docs WHERE ...` lowers to a native document source-insert
        batch when the source producer is an exact `_id` lookup, ready indexed
        scalar producer, or bounded residual scan with row and byte caps;
        assignments are flat declared projection fields; duplicate source ids
        are rejected; optional source `LIMIT` caps materialized rows;
        `RETURNING` rows may include `_id`, `_doc`, `_version`, and declared
        projection fields; source producers may omit the target `_id` and
        allocate target ids through the native generated document id helper in
        canonical source-id order; and no ordering or conflict action is
        present.
    - [ ] Admit source-query projection writes with `ON CONFLICT` only after
          conflict actions compose with source materialization, duplicate-key
          handling, no-op row reporting, and native write responses.
  - [ ] Harden admitted `MERGE INTO docs ...`.
        Land only merge shapes that can be expressed as deterministic native
        document insert/update/delete requests. Required proof: matched insert,
        update, delete, and no-op branches, branch ordering, duplicate-source
        handling, conflict behavior, audit-required behavior, row-filter and
        stale-filter behavior, generated-field rejection, schema-validation
        failures, no-match ordering semantics, document-write lowering
        fixtures, required SQL/API coverage buckets, and executable runtime
        parity.
        Current baseline: bounded document-table `MERGE` admits matched update,
        matched delete, matched no-op, and not-matched `_id`/`_doc` copy insert
        plans through native document batch materialization.
    - [ ] Add projection not-matched inserts after conflict/create-only
          behavior is proven for non-`_doc` payload construction.
    - [ ] Add `RETURNING` once document merge response rows match native/API
          write response shape.

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
  - [ ] Admit `additional-array-unnest-patterns`: add
        `document_sql_corpus.json` cases for multiple arrays, nested arrays,
        aliasing, non-equality predicates, empty arrays, missing arrays, and
        rejected cartesian-expansion shapes; add required coverage buckets and
        executable runtime parity tests proving exact native
        `array_any`/indexed-array behavior before expanding the admitted
        array-unnest contract.
  - [ ] Admit `additional-mapped-fields`: add `document_sql_corpus.json` cases
        for scalar, numeric, boolean, text, optional, missing, nested, and
        multi-field view mappings; add required coverage buckets and executable
        runtime parity tests proving exact native/indexed producers, projection
        shape, residual behavior, and stable diagnostics for unmapped or
        mistyped fields.
  - [ ] Admit `derived-index-producer-types`: add `document_sql_corpus.json`
        cases for every document derived-index producer shape SQL can select,
        including stale, missing, partial, ordered, and rebuild-in-progress
        indexes; add required coverage buckets and executable runtime parity
        tests proving SQL and native derived-index reads choose the same
        producer or emit the same rejection.
  - [ ] Admit `document-aggregates`: add `document_sql_corpus.json` cases for
        mapped-field `COUNT`, `MIN`/`MAX`, `SUM`/`AVG`, grouped aggregates,
        `HAVING`, order, and limit shapes; add required coverage buckets and
        executable runtime parity tests proving aggregate results, residual
        filtering, empty input, null handling, and rejection of unbounded
        aggregate scans.
  - [ ] Admit `lateral-view-mapping-joins`: add `document_sql_corpus.json`
        cases for each allowed lateral document/view-mapping join shape, join
        predicate family, limit interaction, and unsupported correlated form;
        add required coverage buckets and executable runtime parity tests
        proving row identity, cardinality, residual filtering, and stable
        diagnostics.

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
