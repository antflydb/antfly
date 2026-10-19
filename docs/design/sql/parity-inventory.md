# Original SQL extraction parity inventory

Status: release gate blocking. Run
`python3 scripts/check_sql_parity_inventory.py --report` for current
dispositions and per-family blocking counts; this document does not restate
them. The checker reads only the JSON fixtures below, never this page.

The pinned original source contains 1586 cases: 267 explicitly invalid or
unsupported cases and 1319 cases requiring review of their original planner or
runtime contract. These are **not** 1586 promises of working runtime execution.
Original plan fingerprints, endpoint behavior, authorization, diagnostics and
deliberate rejections must be distinguished. Blocking dispositions describe
missing case-by-case evidence, not missing implementations.

The SQL design is in [SQL extraction](extraction.md); regex qualification is in
[the regex parity design](regex-parity.md). Batch-by-batch campaign narratives,
per-case evidence prose and component measurements are in
[history](history/parity-inventory-batches.md).

## Provenance

Source commit: `79644dfa1605e8da0f486d021d1c1393577d6265`.
Source path: `zig/pkg/antfly/src/sql/fixtures/sql_api_parity_source_corpus.json`.
Original source SHA-256: `52b61411fa93be84b523c109eb6f79ea9e2f8a83d4e3639a831f4b8a697892c6`.

`zig/pkg/antfly-embedded/src/sql/fixtures/sql_parity_inventory.json` is an immutable compact
projection preserving source order, exact name/family/SQL/parameters, and the
SHA-256 of every complete original entry. Stable IDs are original one-based
positions, `sql-0001` through `sql-1586`. The entire projection is also checksum
pinned by the audit script. Historical implementation-specific plan fingerprints
are not copied as assertions against the new execution engine; each complete
original entry remains identifiable by its canonical hash (sorted JSON keys,
compact separators, UTF-8 without ASCII escaping).

The matching `sql_parity_dispositions.json` must account for every ID exactly
once. Discovery runs, oracle admission and component tests never change a
disposition; only a reviewed case with a passing gate does.

## Compatibility standard

PostgreSQL is the SQL compatibility standard for this work. Its behavior governs
syntax, type coercion, result types, SQL NULL semantics, ordering, JSON/array
operations and mutations. Historical source plans and SQLite results do not
override that contract. SQLite-backed fixtures are limited regression checks,
not evidence of PostgreSQL compatibility; new campaign completion must be
validated against PostgreSQL and the native engine. An unavailable PostgreSQL
oracle is a validation gap, not permission to substitute SQLite silently.
SQLite cannot adjudicate default NULL ordering or division by zero; those use
explicit native contracts.

Campaigns use a real PostgreSQL 18+ oracle (PostgreSQL 19 is the target).
`generate_sql_postgres_reference.py` starts a disposable private Unix-socket
server, sends the original `$n` SQL unchanged, and records complete values,
labels, PostgreSQL type OIDs and SQL NULL flags. The oracle uses UTF-8, C locale
and UTC. Server-side raw cursors cap fetched read rows without modifying the
source SQL or buffering its entire result. Reads are transactionally read-only;
each mutation has an isolated rolled-back fixture. Time, lock, temporary-file and
output limits bound oracle execution; mutation results share a 16 MiB
wire-payload budget across RETURNING and every post-state table. Assignment-column
triggers distinguish an explicit NULL write from a missing document property.
Source schemas remain intact; an explicit current document schema models the
historical object-valued `metadata: json` shorthand without granting any
index-readiness or cardinality authority. Clock functions such as `now()` need a
dedicated clock contract and cannot become sampled values in goldens. Historical
forms that PostgreSQL rejects (for example `UPDATE ... FOR UPDATE`, DEFAULT
subqueries, output-alias arithmetic in ORDER BY/HAVING) are not rewritten to
inflate coverage; they become tested rejections or supersessions.

Non-unique ordering contracts use independent, bounded PostgreSQL observers for
the complete eligible peer frontier. Validation checks a complete ordered prefix,
allowing arbitrary selection only within genuine peers at the LIMIT boundary.
Skipping better rows, duplicating rows or selecting worse rows fails;
PostgreSQL's arbitrary tie order is not a compatibility rule.

Install PostgreSQL 18 or newer, put its binaries on PATH or set `ANTFLY_PG_BIN`,
then verify selected PostgreSQL goldens with:

```sh
uv run --no-project --with 'psycopg[binary]==3.3.6' python scripts/generate_sql_postgres_reference.py read --check zig/pkg/antfly-embedded/src/sql/fixtures/sql_read_campaign_reference.json
uv run --no-project --with 'psycopg[binary]==3.3.6' python scripts/generate_sql_postgres_reference.py document --check zig/pkg/antfly-embedded/src/sql/fixtures/sql_document_reference.json
uv run --no-project --with 'psycopg[binary]==3.3.6' python scripts/generate_sql_postgres_reference.py typed_array_read --check zig/pkg/antfly-embedded/src/sql/fixtures/sql_typed_array_read_reference.json
uv run --no-project --with 'psycopg[binary]==3.3.6' python scripts/generate_sql_postgres_reference.py set_read --check zig/pkg/antfly-embedded/src/sql/fixtures/sql_set_read_campaign_reference.json
uv run --no-project --with 'psycopg[binary]==3.3.6' python scripts/generate_sql_postgres_reference.py mutation --check zig/pkg/antfly-embedded/src/sql/fixtures/sql_mutation_postgres_reference.json
uv run --no-project --with 'psycopg[binary]==3.3.6' python -m unittest discover -s scripts -p test_generate_sql_postgres_reference.py
```

`--include` can extend a checked golden only when every existing contract still
matches; unknown or duplicate IDs and rejected originals fail closed. Empty
fixtures and unlisted empty reads fail oracle admission; deliberately empty
originals need dedicated bounded input probes.

Older bounded references (not PostgreSQL evidence) remain for selected goldens:

```sh
python3 scripts/generate_sql_mutation_reference.py --check zig/pkg/antfly-embedded/src/sql/fixtures/sql_mutation_reference.json
python3 scripts/generate_sql_parity_read_reference.py --cases zig/pkg/antfly-embedded/src/sql/fixtures/sql_read_reference.json --check
```

Campaign manifests (read, document, typed-array read, set read, LATERAL,
mutation, catalog) describe complete cohorts, not claims that every case works.
Selected goldens must pass the non-discovery native endpoint gate before
receiving completion credit. Discovery modes report compiler gaps without
updating dispositions:

- Catalog: `ANTFLY_SQL_CATALOG_DISCOVERY=1` and
  `zig build sql-test -Dtest-filter='SQL catalog campaign discovery'` from `zig/`
  exercise all original `ddl`/`unsupported_ddl` cases. Admission is not proof of
  authorization, durable publication, populated rewrites or constraint
  activation.
- Mutation: `ANTFLY_SQL_MUTATION_DISCOVERY=1` reports individual compiler gaps
  for the fixed 235-case mutation cohort (INSERT/source, nonjoined
  UPDATE/DELETE/source and joined mutations). Its profiles separate point,
  conflict/index-owner, source, temporal and joined execution contracts.
  Outcomes without goldens are not unsupported-feature classifications.

Mutation evidence must verify complete RETURNING labels/types/NULL provenance,
affected rows and every fixture table's complete post-state, not just counts.
Schema-dependent contracts need their actual owner profile: unique, partial and
expression-index cases run against those declared owners (with duplicate
`23505`, named-index-alias `42704` and wrong-inference-target `42P10` probes),
not against an unconstrained baseline.

The restructuring plan excludes graph and lake SQL integration. Any corpus cases
belonging to those integrations still need explicit scope review. Deferred cases
continue to block this strict original-corpus gate; a publication scope exception
must be reviewed as a change to gate policy, not hidden as a passing test.

### Integration scope confirmed 2026-10-19

The remaining-case implementation includes SQL publications and subscriptions,
outbound publication of Antfly changes, PostgreSQL-style management of WASM
extensions, and qualification of existing tablespace SQL behavior. These are
implementation obligations, not completed capabilities or inventory waivers.

- Subscriptions must reuse existing PostgreSQL CDC ingestion, backfill,
  checkpoints and ordinary mutation enforcement. SQL subscription objects need
  durable database-scoped ownership and publication membership; an upstream
  publication created by a replication source is not an Antfly SQL publication.
- Publications must publish committed Antfly changes through shared durable
  mutation/replication machinery. Consistent snapshot-to-stream handoff,
  transaction ordering, consumer acknowledgements, bounded retention and
  owner-failover recovery require end-to-end evidence. A catalog entry or parser
  node alone does not activate outbound logical replication.
- Extension DDL must adapt the existing package/lifecycle catalog and WASM
  runtime, with database-scoped installation, schema-qualified typed exports,
  ownership, dependencies and durable recovery. PostgreSQL SQL compatibility
  does not imply loading PostgreSQL C extensions. Compatibility-name no-ops do
  not establish that an extension or its functions are installed.
- Tablespaces already have shared catalog and SQL lowering. Qualify lifecycle,
  inherited placement and the actual placement/rebalance consumer rather than
  creating a second tablespace subsystem or claiming relocation from metadata
  assignment alone.

Each integration retains the same PostgreSQL oracle, native public execution,
complete-postimage and recovery requirements as other cohorts. Unsupported
responses and successful metadata-only operations do not close positive runtime
contracts. This scope confirmation does not change the pinned corpus, its
checksums or any case disposition.

## Commands and acceptance policy

```sh
make sql-parity-inventory-check
make sql-parity-evidence-check
python3 scripts/check_sql_parity_inventory.py --family truncate_source
python3 scripts/check_sql_parity_inventory.py --report
python3 scripts/check_sql_parity_inventory.py --family read --report
python3 scripts/check_sql_parity_inventory.py --evidence --family read
python3 scripts/check_sql_parity_inventory.py --evidence --gate sql-compiler-rejections --gate sql-explain-runtime
python3 scripts/check_sql_parity_inventory.py --source /path/to/sql_api_parity_source_corpus.json
python3 -m unittest discover -s scripts -p test_check_sql_parity_inventory.py
make sql-parity-release-check
```

Inventory checking succeeds when provenance, exact ID coverage, dispositions,
and referenced evidence are structurally valid. It reports remaining blockers.
Evidence checking runs referenced gates, including partial evidence on unresolved
cases, without claiming release readiness. `--family` and repeatable `--gate`
select recorded evidence; missing or empty selections fail rather than reporting
success without tests. Family reports distinguish unresolved cases with partial
evidence from cases without recorded evidence and identify original rejection
contracts; neither category is an inferred count of missing features.
Zig 0.17 gates use repeatable `-Dtest-filter=...` compile options. Filters for the
same owner/build options share one build without broadening other selected gates.
`--gate` requires `--evidence`; it cannot narrow the release gate. A family report
used with `--release` still checks every original ID.
Release checking fails while any case is unresolved or deferred. Once all cases
are resolved, it runs each distinct referenced evidence gate and propagates
failure or timeout. Neither target is part of default tests.
For a resolved case, at least one cited Zig test must name its stable case ID
inside that test's section; an ID elsewhere in the file is not evidence.
Additional cited tests may establish supporting storage or publication behavior.

Resolve each case as `implemented`, `rejected`, or `superseded`, with a rationale
describing its original contract and current equivalent. `superseded` means
replacement behavior with tested equivalence, not an unsupported feature waiver.
`rejected` requires preserving an original rejection or explaining and testing
the current equivalent diagnostic. Do not use it to waive original accepted
behavior. Test names alone are not a parity review.

Every resolved entry must carry executable evidence, for example:

```json
{
  "id": "sql-0160",
  "status": "implemented",
  "reason": "Original single-table truncation maps to the native durable emptying barrier.",
  "evidence": [
    {
      "path": "zig/pkg/antfly/src/sql/truncate_test.zig",
      "test": "SQL truncate preserves durable pending receipts",
      "gate": "sql-runtime"
    }
  ]
}
```

This is a schema example, not a claim that this test exists. Add the original
case ID to the evidence source and verify the test exercises that case. Register
the actual gate under the ledger's `gates` object:

```json
{
  "sql-runtime": {
    "command": ["zig", "build", "sql-test", "-Doptimize=safe"],
    "cwd": "zig",
    "timeout_seconds": 600
  }
}
```

Commands are argument vectors, never evaluated by a shell. Gate declarations
are reviewed executable repository configuration. All declared evidence gates
for completed cases execute on a successful release audit. Pure planning cases
need matching compiler/binder checks; endpoint claims need mounted endpoint
checks, native mutation claims need real native storage checks, and original
rejections need diagnostic/no-side-effect checks. Reuse grouped tests only when
they explicitly cover every cited original case.

This inventory audit does not replace the original relational-row release gate,
generated contract checks, distributed fault injection, cancellation/ownership
tests, or workload benchmarks. The old `relational-release-gate` combined
relational rows, SQL/API typed-plan parity, and fixture freshness. Its absence
from current SQL extraction is not repaired by naming this inventory check a full release gate.

## Original family counts

| Family | Cases |
| --- | ---: |
| aggregate | 44 |
| ddl | 384 |
| delete | 10 |
| delete_joined_source | 27 |
| delete_source | 18 |
| document_write | 95 |
| explain | 8 |
| insert | 74 |
| insert_source | 25 |
| invalid_delete | 1 |
| invalid_insert | 4 |
| invalid_read | 5 |
| invalid_update | 2 |
| invalid_update_joined_source | 1 |
| invalid_update_source | 2 |
| join | 25 |
| lateral | 21 |
| merge_mutation | 16 |
| query | 161 |
| query_function | 13 |
| read | 258 |
| recursive_insert_source | 1 |
| relation_population | 8 |
| truncate_source | 7 |
| unsupported | 6 |
| unsupported_ddl | 95 |
| unsupported_insert | 2 |
| unsupported_read | 17 |
| unsupported_write | 132 |
| update | 17 |
| update_joined_source | 41 |
| update_source | 44 |
| window | 22 |


## Remaining activation caveats

- `sql-1412` declares a NOT VALID CHECK in its original setup; PostgreSQL still
  enforces that CHECK on new writes. Success against a baseline without the
  CHECK does not establish parity.
- The unique-selector cases `sql-1509`, `sql-1510`, `sql-1512`, `sql-1514`,
  `sql-1515` and `sql-1517` need their ordinary/partial/expression-predicate
  index owners and bounded native access evidence. Their original setup uses
  VALIDATE CONSTRAINT on index names, which is not a PostgreSQL constraint
  operation. `sql-1509`, `sql-1510`, `sql-1514` and `sql-1515` have PostgreSQL
  DML results and mounted native execution against ready unique-index owners
  (exact equality index spans, primary content digests, no full-table capture),
  but remain unresolved until that setup and public catalog activation are
  adjudicated. `sql-1512` and `sql-1517` still need their own owner profiles.
  A table scan against the baseline is not a substitute for unique-owner
  activation.
- Unique-owner provenance is an OpenAPI-generated `constraint`/`index` identity.
  Table-bound DROP/VALIDATE CONSTRAINT reject index owners; index retirement
  removes its paired uniqueness rule. Unqualified PostgreSQL DROP INDEX still
  needs catalog owner resolution (see
  [namespace relation ownership](../../../zig/SYSTEM_CATALOG.md#namespace-relation-ownership)).
- TRUNCATE (`sql-0160`–`sql-0165`, `sql-1101`) maps to the durable
  empty-generation barrier; SQL-owned sequence counters remain outside the
  current catalog model, and the promoted-standby and asynchronous-artifact
  online-transfer fault matrices are not established.
- Prepared CTE/recursive/MERGE cases (`sql-0005`, `sql-0008`–`sql-0010`) have
  hosted single-owner evidence but stay unresolved pending multi-owner and
  distributed fault evidence.
- Joined/source UPDATE and DELETE, MERGE, lateral and recursive source cases
  require explicit current-engine mapping beyond ordinary DML component tests.
- DDL's 384 entries include session commands, prepared statements, cursors,
  maintenance, locks, and constraints. Review actual original admission/runtime
  behavior before treating all of these as implemented or all as future scope.
- Existing scalar/aggregate/join/window, document DML, DDL, session and protocol
  tests should be mapped to exact IDs. Current passing component suites do not
  automatically resolve source corpus entries. No cases are automatically
  waived by family or by keyword.
