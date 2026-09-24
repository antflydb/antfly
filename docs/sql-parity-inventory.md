# Original SQL extraction parity inventory

The pinned original source contains 1586 cases: 267 explicitly invalid or
unsupported cases and 1319 cases requiring review of their original
planner or runtime contract. These are **not** 1586 promises of working runtime
execution. Original plan fingerprints, endpoint behavior, authorization, diagnostics,
and deliberate rejections must be distinguished.

## Provenance and scope

Source commit: `79644dfa1605e8da0f486d021d1c1393577d6265`.
Source path: `zig/pkg/antfly/src/sql/fixtures/sql_api_parity_source_corpus.json`.
Original source SHA-256: `52b61411fa93be84b523c109eb6f79ea9e2f8a83d4e3639a831f4b8a697892c6`.

`zig/pkg/antfly/src/sql/fixtures/sql_parity_inventory.json` is an immutable compact
projection preserving source order, exact name/family/SQL/parameters, and the
SHA-256 of every complete original entry. Stable IDs are original one-based
positions, `sql-0001` through `sql-1586`. The entire projection is also checksum
pinned by the audit script. Historical implementation-specific plan fingerprints
are not copied as assertions against the new execution engine; each complete
original entry remains identifiable by its canonical hash (sorted JSON keys,
compact separators, UTF-8 without ASCII escaping).

The matching `sql_parity_dispositions.json` must account for every ID exactly once.
The inventory began unresolved; seven TRUNCATE cases now have explicit
supersession rationale and admission plus staged-owner publication evidence.
One session-setting case has mounted pgwire evidence for a deliberate UTF-8-only
replacement behavior.
Eight original invalid-shape cases have exact-SQL compiler rejection tests and
an executable evidence gate. Explicit multi-output scalar/IN subqueries now
fail before mutation binding or native authorization.
`sql-0019` has mounted `/db/v1/sql` execution of its exact computed-order
`LIMIT 5` query over native typed rows. Nine distinct ranked rows verify the
five returned IDs, excluded lower-ranked rows, and exact large-integer output.
`sql-0020`, `sql-1254`, and `sql-1255` execute their exact grouped-read SQL
through the same native-backed endpoint. Their evidence checks expression
grouping, output-alias resolution in `GROUP BY`/`HAVING`, and the aggregate
result across mixed-case source values.
`sql-1221`, `sql-1222`, `sql-1256` through `sql-1264`
execute their exact CTE queries through that endpoint. The fixture distinguishes
JSON source filtering, missing JSON fields, missing status, CTE column aliases,
grouped sums, chained filters, and descending order. Explicit materialization
hints are additionally covered by repeated-reference work-count tests: a
materialized producer is evaluated once, while `NOT MATERIALIZED` remains inline.
`sql-1270` exercises the exact multi-row scalar-subquery cardinality error
through mounted HTTP. `sql-1271` and `sql-1272` execute their exact scalar
projections against a separate native text-ID fixture; an empty lookup
additionally verifies SQL-null provenance. `sql-1275` through `sql-1282`
execute their exact IN/NOT IN/ANY/SOME/ALL queries through the integer-ID
fixture, with complete result-set checks over nullable status and varying
integer amounts. Text-key cases are not coerced through that fixture.
`sql-1273` and `sql-1274` exercise exact EXISTS/NOT EXISTS statements against
a boolean-enabled native row. `sql-1288` through `sql-1292` cover the remaining
orderable quantified comparisons over distinct strings and integer extrema.
`sql-1283` through `sql-1287` and `sql-1293` through `sql-1297` now execute
through mounted HTTP with a quota-accounted distinct pattern-set evaluator.
Component evidence covers wildcard, case-insensitive, empty, NULL, negated
quantifier, correlated grouping and retained-byte behavior; ordered-comparison
MIN/MAX is not used for these predicates.
`sql-1298`, `sql-1299`, and `sql-1306` through `sql-1310` have exact mounted
correlation evidence over repeated and singleton customer groups. The scalar
case verifies SQLSTATE 21000 for a multi-row group; membership and ordered
existence check complete output sets. OR-correlated shapes remain unresolved.
`sql-0048` and `sql-0050` are tested supersessions of the old catalog/admin
model. Their exact statements run through a mounted authenticated pgwire
session and native typed-row table: the first FETCH returns one ordered ID and
FETCH ALL returns the remaining eight without reexecution. The blocking-result
fallback also has quota-failure and post-commit permission-revocation tests.
`sql-0051` through `sql-0063` have exact pgwire command coverage for forward,
shorthand, counted, backward, positional, and all-row FETCH plus named/all
CLOSE. Per-command row counts and scroll positions are asserted against one
retained stream; the old catalog/admin mutation interpretation is superseded.
`sql-0064` and `sql-0065` execute their exact EXPLAIN reads through mounted
HTTP and return text and versioned JSON plans. `sql-0066` explains its exact
INSERT against a native text-key table and verifies no row was inserted.
`sql-0068` explains its exact UPDATE-with-membership-subquery, and `sql-0069`
explains its exact cross-table MERGE through mounted HTTP without opening a
distributed read capture or commit attempt. Ordinary target-only UPDATE and
DELETE subqueries share one decorrelated, snapshot-captured mutation path; a
1,024-row component case bounds reads by the captured inputs. The renderer uses the
same authorized binder as execution, opens no row scan, and cannot mutate
storage; ANALYZE and the remaining EXPLAIN shapes are not counted as resolved.
`sql-0001` and `sql-0034` are tested supersessions: their exact typed-text
PREPARE/EXECUTE sequence runs through an authenticated mounted pgwire session,
returns the three matching native rows, and requires read rather than catalog
admin authority. `sql-0035` and `sql-0036` likewise replace the original
catalog/admin-mutation model with connection-owned named and all-plan
deallocation, with SQLSTATE 26000 after each removed plan is executed.
`sql-0037`, `sql-0039`, `sql-0041`, `sql-0043`, and `sql-0046` are tested
supersessions of catalog/admin session mutations. Their exact public-namespace
and one-millisecond timeout commands run through the pgwire session state
machine; the test checks effective SHOW rows, transaction-local rollback, and
RESET. The exact two-namespace `SET SESSION`/`SET LOCAL` commands now use a
bounded ordered lookup path: pgwire tests cover authorization and transaction
scope, and a native resolver test proves that only a missing table advances to
the next namespace. Exact `app.tenant_id` SET/RESET/RESET ALL/DISCARD commands
also use typed connection-owned overlays. Broader custom-setting semantics
remain case-by-case work.
The remaining unresolved dispositions describe missing case-by-case evidence,
not a claim that every current implementation is missing.
The exact `sql-1410` self-read INSERT now passes through mounted SQL against
native relational storage: RETURNING reports its ID, one row is affected, and
a subsequent typed read verifies the committed status and quantity. Multi-row
VALUES-subquery execution also has typed component and self-capture tests.
The exact `sql-1413` INSERT INTO ONLY a namespace-qualified table also passes
through mounted SQL, with affected count and both RETURNING cells checked.
The catalog has no inheritance, so ONLY resolves the exact same table rather
than silently changing the mutation target.
The exact `sql-0170` SELECT FROM ONLY a namespace-qualified table executes
with its original parameter over native rows; the fixture verifies descending
order, five-row LIMIT, an excluded older match and a newer nonmatch.
The exact `sql-1531` point UPDATE copies a quantity through a same-table scalar
subquery, returns the target ID, and is read back after commit. Component tests
also check the shared capture, scalar cardinality, quota and authorization
boundaries; conflict-assignment scalar subqueries remain a separate gap.
The exact `sql-1532` row-assignment UPDATE also executes through mounted SQL,
returns its target ID, and reads back both assigned cells. The parser expands
explicit ROW and parenthesized tuples into simultaneous column assignments
while rejecting duplicate targets and mismatched arity before any write.
That mounted tuple UPDATE also verifies an untouched nullable datetime column
stays physically absent in the stored row; joined mutation capture carries
field-presence metadata rather than treating a projected missing cell as an
explicit SQL NULL.
The exact `sql-1494` INSERT DEFAULT VALUES executes against a native schema
with defaults for all three returned logical columns. The shared prepared-row
pipeline also has component evidence for per-cell DEFAULT across direct and
captured VALUES sources, explicit SQL NULL, generated columns and row IDs.
The exact `sql-1481` two-row INSERT executes through mounted SQL and checks
both affected rows and ordered RETURNING values. The exact `sql-1484` batch
uses a mounted table with an enforced coordinated UNIQUE(id) owner. Distinct
physical row IDs with the same logical ID reject as SQLSTATE `23505` before
either primary row is applied; a native scan verifies no partial write.
The exact `sql-1496` TIMESTAMPTZ literal executes through mounted SQL against
a native datetime column; RETURNING shows the validated `+01:30` source offset
normalized to UTC. The exact `sql-1495` DEFAULT VALUES conflict statement
uses a seeded native row and active coordinated UNIQUE(id) claim. The owner
selects the existing row, the guarded update commits, and RETURNING plus a
physical read verify the schema-derived default values.
The 14 ordinary MERGE cases have exact-text compiler/binder coverage, and
`sql-0579` additionally has component execution plus a mounted, authorized
two-table commit test that checks distinct owner-route range proofs, denies a
missing source-read grant before scanning, and verifies source conflict and
unknown-outcome handling without automatic replay. A missing source proof
aborts before commit. The exact `sql-0579` text also passes through mounted
`/db/v1/sql` with two affected rows and wire-level conflict/unknown-outcome
diagnostics. Its HTTP prepared-resource path pins both table identities and
executes the original statement. They remain unresolved pending case-by-case
endpoint and real cross-owner failover evidence for their original behavior.
`sql-0585` additionally has mounted endpoint execution of lower/upper source
expressions for both matched and source-only mutation images.
`sql-0581` has mounted endpoint execution of conditional matched and
source-only arms, including all-false predicates with no mutation images.
`sql-0584` has mounted endpoint execution of computed RETURNING over the
matched postimage.
No cases are automatically waived by family or by keyword.

The restructuring plan excludes graph and lake SQL integration. Any corpus cases
belonging to those integrations still need explicit scope review. Deferred cases
continue to block this strict original-corpus gate; a publication scope exception
must be reviewed as a change to gate policy, not hidden as a passing test.

## Commands and acceptance policy

```sh
make sql-parity-inventory-check
make sql-parity-evidence-check
python3 scripts/check_sql_parity_inventory.py --family truncate_source
python3 scripts/check_sql_parity_inventory.py --source /path/to/sql_api_parity_source_corpus.json
python3 -m unittest discover -s scripts -p test_check_sql_parity_inventory.py
make sql-parity-release-check
```

Inventory checking succeeds when provenance, exact ID coverage, dispositions,
and referenced evidence are structurally valid. It reports remaining blockers.
Evidence checking runs the executable gates for resolved cases without claiming
release readiness; use it as dispositions are added. Release checking fails while any case is unresolved or deferred. Once all cases
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
    "command": ["zig", "build", "sql-test", "-Doptimize=ReleaseSafe"],
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

## Concrete remaining reviews

- TRUNCATE: `sql-0160` through `sql-0165` and `sql-1101` are mapped to
  the durable empty-generation barrier. Exact original SQL forms are compiled
  and admitted by the API fixture; the real staged-owner driver tests empty
  publication and recovery. External-parent FK retirement and graph cutover
  remain guarded architecture gaps, not waived by those case dispositions.
- Joined/source UPDATE and DELETE, MERGE, lateral and recursive source cases
  require explicit current-engine mapping beyond ordinary DML component tests.
- DDL's 384 entries include session commands, prepared statements, cursors,
  maintenance, locks, and constraints. Review actual original admission/runtime
  behavior before treating all of these as implemented or all as future scope.
- Existing scalar/aggregate/join/window, document DML, DDL, session and protocol
  tests should be mapped to exact IDs. Current passing component suites do not
  automatically resolve source corpus entries.
