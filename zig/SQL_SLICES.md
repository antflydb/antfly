# SQL.md Remaining Work Slices

This is the current backlog for the remaining large `SQL.md` work. It omits
completed tracking context and keeps only slices that still need implementation,
parity proof, or cleanup. Each checkbox should describe a landable artifact:
code behavior, diagnostics, fixture coverage, runtime parity, or a deletion.

## Document SQL

- [ ] **Admit document table DDL through SQL**
  - [ ] Parse `CREATE TABLE ... WITH (antfly.storage_mode = 'document', ...)`
        plus one or more `DOCUMENT SCHEMA name AS JSON '...'` clauses.
  - [ ] Reject document-profile `CREATE TABLE` statements that also use a
        relational column list, relational constraints, generated columns, or
        other PostgreSQL column semantics.
  - [ ] Lower document table DDL to native `TableSchema` catalog mutations with
        `storage_mode = document`, `default_type`, and `document_schemas`;
        store no raw SQL text as durable schema metadata.
  - [ ] Validate document-schema JSON through the shared schema validators,
        including Antfly extensions, dynamic templates, `default_type`
        existence, and multi-document-type shape.
  - [ ] Keep `CREATE DOCUMENT TABLE` out of the canonical grammar for now, or
        accept it only as sugar that lowers to the same typed plan.
  - [ ] Add DDL/native parity fixtures comparing SQL-created document table
        schemas with equivalent native/API schema mutations.
  - [ ] Add stable unsupported-shape diagnostics for malformed JSON, missing
        default type, mixed relational/document syntax, and unsupported document
        schema shorthand.

- [ ] **Admit document writes through SQL**
  - [ ] Add a shared document-write preflight used by native writes and SQL
        document writes.
    - [ ] Match native rejection behavior for unauthorized writes,
          row-filter-denied writes, and audit-required writes before SQL write
          lowering is admitted.
    - [ ] Preserve the native audit and row-filter ordering in SQL write tests.
    - [x] Keep document-table `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, and
          `MERGE` fail-closed with `DocumentSqlWriteUnsupported` until native
          document-write preflight and lowering are shared.
    - [x] Keep writes through document SQL views rejected until view write
          semantics are deliberately admitted.
  - [ ] Admit document-table writes in the required design order:
        full-document insert/upsert, exact `_id` delete, explicit `_doc` JSON
        patch/update expressions, and only then projection-column writes.
  - [ ] Define and implement full-document `INSERT`.
    - [ ] Accept `INSERT INTO docs (_id, _doc) VALUES (...)` with explicit
          document keys and JSON documents.
    - [ ] Accept `INSERT INTO docs (_doc) VALUES (...)` only when the native
          document write policy can generate document ids.
    - [ ] Reject duplicate `_id`, malformed `_doc`, unsupported generated-id
          policy, generated-field writes, and schema-validation failures with
          stable diagnostics.
    - [ ] Lower admitted inserts to the native document insert/upsert path, not
          relational row batches.
    - [ ] Add SQL/native parity tests for inserted document shape, `_id`
          behavior, rejection behavior, and audit behavior.
  - [ ] Define and implement exact document-key `DELETE`.
    - [ ] Accept `DELETE FROM docs WHERE _id = ...` and `_id IN (...)` as the
          first delete forms.
    - [ ] Reject non-identity delete predicates until they lower to exact
          native document query/delete semantics with boundedness, row-filter,
          authorization, audit, and no-match behavior pinned.
    - [ ] Lower admitted deletes to native document delete paths.
    - [ ] Add SQL/native parity tests for deleted document identity,
          row-filter/audit behavior, no-match behavior, and rejection behavior.
  - [ ] Define and implement explicit `_doc` JSON patch/update expressions.
    - [ ] Accept `_doc` updates through proven JSON update expressions such as
          `jsonb_set(_doc, ...)` or Antfly-owned helpers such as
          `antfly.json_patch(_doc, patch)` once they lower to typed native
          document patch requests.
    - [ ] Specify JSON path assignment, removal, merge, null handling, array
          semantics, type validation, stale-filter behavior, conflict handling,
          and audit ordering.
    - [ ] Reject generated-field updates, unsupported JSON patch shapes, and
          broad projection-field updates with stable diagnostics until their
          semantics are pinned.
    - [ ] Lower admitted updates to native document patch/update paths.
    - [ ] Add SQL/native parity tests for patch shape, matched-row behavior,
          no-match behavior, rejection behavior, and audit behavior.
  - [ ] Define projection-column document writes as a later virtual-schema
        layer.
    - [ ] Specify accepted projection-column insert/update forms only after
          virtual schema write semantics prove field paths, nullability, type
          validation, defaults, generated-field rejection, and
          `additionalProperties` behavior.
    - [ ] Lower projection writes by constructing JSON documents or native
          document patches, never relational row inserts or mutation-source
          plans.
    - [ ] Add stable unsupported-shape diagnostics for projection writes before
          this layer is admitted.

## Whole SQL Adapter

- [ ] **Migrate parser and grammar paths family by family**
  - [ ] Add a migration table that names each statement family, its
        compatibility entry point, generated-AST entry point, coverage file,
        and removal gate.
  - [ ] Select the next family and add raw generated-AST fixtures before binder
        validation for every supported syntax shape in that family.
  - [ ] Add binder and lowering fixtures for the selected family, including
        stable unsupported-shape diagnostics for rejected shapes.
  - [ ] Add API/runtime parity evidence for the selected family before marking
        it migrated.
  - [ ] Delete the selected family's compatibility parser path only after raw
        AST, binder, lowering, and parity evidence exist.
  - [ ] Repeat the migration table workflow for DDL, DML, query, roles,
        extensions, backups, lakes, functions, and maintenance commands.

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

- [ ] **Build structured parity tooling**
  - [ ] Emit structured parser, binder, plan, native-requirement, and
        unsupported-shape summaries from SQL fixtures.
  - [ ] Replace SQL-string rescans in fixtures with structured summaries or
        explicit coverage bits.
  - [x] Split fixture freshness checks from behavioral parity checks so stale
        fixture detection does not hide runtime parity failures.
  - [x] Add coverage gates for native requirements and unsupported reasons.
  - [x] Add an app-parity coverage report that lists all missing required
        coverage buckets.
  - [x] Label each missing app-parity coverage bucket with the next broad
        artifact category: parser, binder, lowering, runtime, or native parity.
  - [x] Extend the report from broad artifact categories to feature-owned
        next actions with file paths, fixture names, and test targets.
  - [x] Have app-parity coverage gates consume the structured next-action
        report instead of failing on the first raw coverage bucket.

- [ ] **Prove cross-surface parity**
  - [ ] Define native-model equivalence checks that can compare SQL plans with
        HTTP, SDK, MCP, A2A, CLI, and internal native API requests.
  - [ ] Add DDL parity fixtures that compare SQL catalog plans with equivalent
        native/API catalog mutations.
  - [ ] Add DML parity fixtures that compare SQL row/document write plans with
        equivalent native/API mutation requests.
  - [ ] Add read/query parity fixtures that compare SQL read plans with
        equivalent native/API query requests.
  - [ ] Add derived-index lifecycle parity fixtures that compare SQL index DDL
        with equivalent native/API derived-index lifecycle requests.
  - [ ] Add roles, extensions, backups, lakes, and unsupported-case parity
        fixtures across SQL and native/API surfaces.
  - [ ] Gate supported SQL features on lowering to the same native model as
        the equivalent Antfly-native API call.

- [ ] **Remove compatibility wrappers**
  - [ ] Inventory string-only parser, binder, planner, and runtime
        compatibility wrappers with file paths and owning feature families.
  - [ ] Classify each wrapper as a migration blocker, compatibility contract,
        or removable dead path.
  - [ ] For migration blockers, name the typed parser, binder, plan, runtime,
        and parity evidence required before deletion.
  - [ ] Delete one wrapper class at a time with focused regression tests for
        the removed compatibility path.
  - [ ] Keep only wrappers with a named compatibility reason that cannot change
        durable catalog, document, or row state.
