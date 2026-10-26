# SQL parity inventory: batch and campaign narratives

> Summary of a verbatim log. Full text: [docs/design/sql/history/parity-inventory-batches.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/sql/history/parity-inventory-batches.md). Originally relocated verbatim from `docs/sql-parity-inventory.md` (lines 3–27, 35–165, 239–526, 559–1312, 2489–2653) at commit 3ce5aff4ea on 2026-10-12; durable decisions were folded into [SQL parity inventory](../parity-inventory.md).

Narratives of the campaigns that moved original 1,586-case SQL extraction corpus
entries to audited dispositions, plus the shared executor/array/regex
infrastructure built along the way. Counts are as-of-the-log, not current
guarantees. The last recorded audit was **478 implemented / 136 rejected / 73
superseded / 899 unresolved** (an earlier checkpoint recorded 448 / 136 / 73 / 929).

## Standing rules recorded in the log
- **Component evidence never grants disposition credit.** Shared operator, codec, array, regex and fixture work is repeatedly recorded as "no original disposition changes"; credit requires exact-case evidence (unchanged source SQL and parameters) through the mounted native endpoint plus an independent PostgreSQL oracle checking labels, types, complete rows/postimages and SQL NULL provenance.
- **Source SQL is never rewritten to inflate coverage.** Historical `UPDATE ... FOR UPDATE` forms stay unresolved PostgreSQL syntax incompatibilities.
- **Discovery cannot update dispositions.** Unknown/duplicate manifest IDs, unavailable reference shapes and changed results fail the reproducibility gate; `--include` can extend a golden only when every existing contract still matches; a required-ID guard prevents regeneration from silently dropping cases; no reference is regenerated merely to accept a discrepancy.
- **SQLite is not a substitute** for native owner fixtures, and SQLite-specific behavior is not a waiver. The 113 SQLite-backed read cases are separate from two explicit native contracts (default NULL ordering, implicit window labels).
- **Empty results are not evidence.** Empty fixtures and unlisted empty reads fail oracle admission; the generator does not treat an empty mutation as evidence. The three empty originals `sql-0459`, `sql-0516`, `sql-0541` were credited only after dedicated input probes proved the inputs nonempty.
- **Schema-dependent cases need their schema.** `sql-1412` (NOT VALID CHECK) and the unique-selector cases cannot be certified against a baseline lacking the CHECK or index owner; a table scan is not a substitute for unique-owner activation. Uniqueness profiles must include an independent duplicate probe that fails with `23505` and leaves storage unchanged.
- **Clock functions** such as `now()` need a dedicated clock contract and cannot become sampled values in deterministic goldens.
- **No family/keyword waivers**; unresolved dispositions describe missing case-by-case evidence, not missing implementation. The 384 DDL entries (session commands, prepared statements, cursors, maintenance, locks, constraints) must be reviewed individually.

## Disposition corrections (supersessions and rejections)
- `sql-1357`, `sql-1219`, `sql-1373` — superseded, not implemented: PostgreSQL rejects output-alias arithmetic in ORDER BY with SQLSTATE 42703. `sql-1219`/`sql-1373` had been incorrectly credited from SQLite-positive behavior; their success goldens were removed and negative contracts kept.
- `sql-0020`, `sql-1254`, `sql-1255` — PostgreSQL-tested supersessions of the original HAVING output-label extension (42703). Namespace rules adopted: HAVING and compound ORDER BY/GROUP BY expressions use the input namespace; bare GROUP BY labels only when no input column matches; bare ORDER BY labels prefer output; missing input columns 42703, ungrouped existing columns 42803.
- `sql-1109`–`sql-1140` (32 DEFAULT-subquery cases) — PostgreSQL rejects with `0A000`; not features awaiting activation. Request parameters cannot become durable defaults and subqueries are prohibited in CREATE/ALTER defaults.
- Catalog/admin-model supersessions via pgwire: `sql-0001`, `sql-0002`, `sql-0003` (EXECUTE returns a pending durable TRUNCATE receipt, not false synchronous completion), `sql-0004`, `sql-0006`, `sql-0007`, `sql-0034`–`sql-0036`, `sql-0037`/`0039`/`0041`/`0043`/`0046`, `sql-0048`, `sql-0050`, `sql-0051`–`sql-0063`.
- TRUNCATE `sql-0160`–`sql-0165` and `sql-1101` mapped to the durable empty-generation barrier; SQL-owned sequence counters remain outside the catalog model.
- One session-setting case is a deliberate UTF-8-only replacement behavior; eight invalid-shape cases have exact-SQL compiler rejection evidence.
- `sql-1413` / `sql-0170` (`ONLY`): the catalog has no inheritance, so ONLY resolves the same table rather than silently changing target.

## Credited campaigns (exact-case evidence)
- Reads: pagination `sql-0205`–`0207`; typed-array predicates `sql-0220`–`0222`, `0284`, `0302`, `1226`, `1227`, `1340`; LATERAL `sql-1363` and 23 strict-API originals (`sql-0549`, `1217`, `1218`, `1345`–`1365` except `1357`); JSON/regex/string `sql-0195`, `0214`, `0216`, `0218`, `0244`, `0256`; ordered-set `sql-0560`–`0566` (`sql-0561` reconciled via PostgreSQL binary array results); JSONB existence `sql-0197`, `0198`; eleven aggregate cases. The read golden reached 91 exact original contracts.
- Mutations: correlated/tuple UPDATE/DELETE `sql-0600`–`0602`, `0610`–`0612`; regex conflict mutations `sql-1443`, `1471`, `1473`–`1475`; UNIQUE(email) upserts `sql-1394`, `1395`, `1398`–`1400`, `1402`, `1406`, `1407`; partial/expression-index upserts `sql-1455`, `1458`, `1460`, `1461` (same canonical CREATE UNIQUE INDEX executed by PostgreSQL and native DDL; probes for 23505, 42704, 42P10); nine cross-table INSERT/CTE, DO NOTHING and qualified RETURNING contracts.
- Mutation cohort: a fixed 235-case cohort (91 INSERT/source, 76 nonjoined UPDATE/DELETE/source, 68 joined); compiler discovery admitted 104/235; 48 positive PostgreSQL goldens were reproducible. The remaining 187 are explicitly **not** 187 unsupported features (absent arbiters, missing typed profiles, ambiguous SQL, nondeterministic producers, non-exercising inputs).

## Cases explicitly left unresolved
- `sql-0005`, `sql-0008`, `sql-0009`, `sql-0010` — exact hosted pgwire/HTTP evidence against a real Raft-backed owner exists, but multi-owner coordination, failover and distributed fault evidence are missing.
- The 14 ordinary MERGE cases (including `sql-0579`, `0581`, `0584`, `0585`) — pending case-by-case endpoint and real cross-owner failover evidence.
- Unique selectors `sql-1509`, `1510`, `1514`, `1515` — mounted native evidence against index owners, but the original index-as-constraint `VALIDATE CONSTRAINT` setup is not a PostgreSQL operation and public catalog activation needs adjudication; `sql-1512`, `1517` still need owner profiles.
- `sql-1412` (NOT VALID CHECK), OR-correlated subquery shapes, EXPLAIN ANALYZE and remaining EXPLAIN shapes, historical `UPDATE ... FOR UPDATE`.

## Design decisions in shared infrastructure
- **Regex:** SQL uses the native Zig `antfly_regex.captures` interface in `lib/regex`; the vendored C engine and bridge were removed. Programs use explicit caller allocation and work/cancellation budgets; backreferences do not claim linear complexity. Remaining work is tracked in [the regex parity design](../regex-parity.md). Ordinary `~`/`~*`/`!~`/`!~*` lower to the existing prepared ARE session rather than another engine.
- **Typed arrays** are a distinct immutable value layer (`array_value.zig`), never a JSON-list approximation; plain JSON arrays are not SQL arrays. Up to six dimensions, lower bounds and per-element SQL NULL provenance are preserved through ordering, hashing, spill codecs, the PostgreSQL binary codec (`array_binary.zig`), parameter frames (`parameter_frame.zig`) and public HTTP/pgwire results. Constructor-bearing schemas require capability 25 (array-only programs keep capability 24).
- **Unknown-literal constructor coercions** are prepared once in the binder rather than via permissive runtime text casts.
- **One scalar VM invocation owns all work admission;** nested kernels borrow it. Cancellation and quota failures are sticky; real OOM stays distinct from quota failure. This is invocation-local, not a statement-wide budget.
- **Set operations** keep a small hash table and promote to the shared external merge sorter before losing spill headroom, without replaying a delivered prefix.
- **Conditional scalar reads** use compiler-generated masked Apply producers: a branch producer is not opened until demanded, while binding/authorization still cover every branch.
- **Scalar producers** apply a compiler-owned two-row bound; NULL LIMIT stays unbounded and NULL OFFSET is zero; negative bounds use `2201W`/`2201X`.
- **Ordered-set aggregates** (`operators.OrderedAggregate`) share one bounded external-sort pass across compatible requests; ordered-set input collection runs serially, without a partial-merge claim.
- **`BETWEEN SYMMETRIC`** expands into both orientations with three-valued comparisons, deliberately not NULL-discarding LEAST/GREATEST; `IS [NOT] UNKNOWN` reuses null-test opcodes to avoid a new persisted capability.
- **SQL integers** are encoded as decimal strings in HTTP output while JSON-column numbers stay numeric.
- **Unique-owner provenance** is an OpenAPI-generated `constraint`/`index` identity in schema metadata, not inferred from editable index descriptions.
- **Precommit read failures** emit a specific `transaction_precommit_read_unavailable` receipt (retryable), never inferred from a generic 503; `CommitDecisionUnknown` stays unknown.

## Defects found
- Uniqueness metadata retained borrowed nested JSON DOM storage after request cleanup (`parseFromValue`); relational declarations moved to an owned token-stream parse.
- `sql-1219`/`sql-1373` were mis-credited from SQLite-positive alias arithmetic (see corrections above).

## Key measurements
Local fixture/microbenchmark numbers; the source labels none of them production latency or throughput claims.

| Measurement | Result |
|---|---|
| Regex selection campaign (PostgreSQL 18.6 C/UTF8) | 24,575 contracts, zero mismatches; 37 minimized witnesses added |
| Regex component contracts | 1,025 pass natively and twice in freestanding WASM; 4,096→16,384-char subjects within fivefold charged work |
| Set spill load gate | 32,768 rows under a single 2 MiB quota (no-spill comparison exceeds it) |
| Catalog truncation boundary | 6,585 original-source request truncations tested |
| Sparse ARRAY stacking | 65,536-element result in 12,332 bytes, one output allocation |
| Scalar `n + 1`, 10,000 rows | exactly 30,000 work units, zero allocation |
| Constant vs dynamic text-array cast, 10,000 rows (debug) | 0 vs 368 bytes/row; ~6 ms vs 26 ms |
| JSONB changed-spine copy (4,096-element subtree, 500 updates) | 612 bytes vs 344,816 bytes full copy |
| Prepared vs dynamic translate, 50,000 rows (debug) | ~113 ms vs 149 ms |
| Ordered-set forced spill, 512 rows, 4 requests | 207,562 bytes shared vs 830,248 bytes independent |
| Source-aware RETURNING, 1,024 targets / 1,024 sources | one capture, exactly 2,048 input rows |
| Scalar limit vs million-row source | reads exactly two source rows |
| Quantified keyed benchmark | 8,872 / 70,802 checkpoints for 128 / 1,024 targets |

Grouped/window HAVING scale check (request-tracked peaks, not RSS):

| Shape | Input rows | Checkpoints | Peak tracked bytes |
| --- | ---: | ---: | ---: |
| Grouped | 128 | 524 | 8,781,813 |
| Grouped | 1,024 | 3,286 | 8,898,058 |
| Window | 128 | 927 | 4,197,936 |
| Window | 1,024 | 6,538 | 4,369,934 |

## Open gaps named in the log
Exact NUMERIC arrays and physical array storage; dynamic JSONB scalar construction; element-changing durable array casts; array-valued ordered index keys; NUMERIC/json/varchar/timestamp array declarations; datetime binary parameters; exact-decimal arithmetic, volatile producers, identity allocation, array DDL and virtual generated columns; cross-level aggregate ownership lifting; FROM/USING source-value RETURNING in MERGE; arbitrary LIMIT/OFFSET expressions and FETCH WITH TIES; unqualified PostgreSQL `DROP INDEX` owner resolution; PostgreSQL 19, broader collations and durable-expression regex qualification; distributed snapshot/concurrent publication and multi-owner fault evidence throughout.

Regeneration commands recorded in the log:

```sh
uv run --no-project --with 'psycopg[binary]==3.3.6' python scripts/generate_sql_postgres_reference.py mutation --check zig/pkg/antfly-embedded/src/sql/fixtures/sql_mutation_postgres_reference.json
python3 scripts/generate_sql_mutation_reference.py --check zig/pkg/antfly-embedded/src/sql/fixtures/sql_mutation_reference.json
python3 scripts/generate_sql_parity_read_reference.py --cases zig/pkg/antfly-embedded/src/sql/fixtures/sql_read_reference.json --check
```

## SQL branch validation and architecture additions

Current audited dispositions: **479 implemented / 136 rejected / 73 superseded /
898 unresolved**. The unchanged original MD5 read now has independent PostgreSQL
and mounted native HTTP evidence with nonempty, duplicate-preserving results.
Its isolated profile retains case, NULL and UTF-8 witnesses without changing the
existing 91-read golden. The other component improvements below do not change
the 898 unresolved original-case dispositions.

### Bounded text fingerprints and prefix comparisons

The shared scalar VM implements PostgreSQL's text `md5` overload using native
Zig, producing lowercase hexadecimal text with one 32-byte output allocation.
It rejects typed non-text inputs and wrong arities with `42883`, preserves
SQL NULL, admits output space before hashing and charges borrowed input bytes
in cancellation-aware chunks. Prefix comparison uses no scratch allocation and
charges only the candidate prefix rather than scanning the unrelated tail.

The independent PostgreSQL oracle verifies 126 text/JSON contracts. Native tests
cover exact output-space admission, work exhaustion, cancellation, all allocation
failure points and fourfold subject growth under linear work and constant output
space. These are resource-bound checks, not a latency benchmark. Mounted public
SQL verifies scalar values, output types, NULL masks and diagnostics. The separate
`digest_read` campaign qualifies only sql-0257; its `ACTIVE` input is backed by
the pinned mega-branch `lower_dml.zig` MD5 regression. This does not certify the
bytea overload, durable MD5 indexes or broader index activation.

Discovery of ten unchanged JSON-tagged array-parameter reads on PostgreSQL 18.6
returned `cannot cast type jsonb to text[]`. Their inputs and unresolved statuses
are retained. Likewise `ends_with` is not an available PostgreSQL 18 function;
no new SQL extension or completion credit was introduced for that historical
source shape.

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

The matching `sql_parity_dispositions.json` must account for every ID exactly once.
The current branch records 479 implemented, 136 rejected and 73 superseded
cases, with 898 still unresolved. The earlier batches add 77 exact compiler
rejection contracts, 115 mounted native reads, twelve native UPDATE/DELETE
contracts and six independently referenced mutations
contracts; they do not claim complete SQL
activation. The immutable corpus remains 1,586 original cases.

The PostgreSQL-backed campaign adds 103 resolved contracts: 54 reads and 49
document mutations. Of those, 24 obsolete document planner rejections are
superseded by the guarded native mutation path, not reclassified as original
positive contracts. Native execution checks full persisted state as well as
public results. Five recorded gates verify mounted execution, both PostgreSQL
references, oracle safety/ordering contracts and pipeline allocation-fault
regressions. This is a validated batch, not completion of either entire campaign;
At that checkpoint, getting below 800 required at least 213 additional resolved
dispositions; the current inventory report remains the authority for totals.
The restructuring plan excludes graph and lake SQL integration. Any corpus cases
belonging to those integrations still need explicit scope review. Deferred cases
continue to block this strict original-corpus gate; a publication scope exception
must be reviewed as a change to gate policy, not hidden as a passing test.

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

## Concrete remaining reviews

The post-#1048 follow-up sequence and acceptance requirements are tracked in
[SQL catalog lifecycle and original-case qualification](design/sql-catalog-lifecycle-parity.md).
It starts with current-engine catalog/index lifecycle evidence, then reuses
faithful owner fixtures across query and mutation activation. The pools in that
plan are unresolved reviews, not automatically missing implementations.

### Namespace relation ownership publication boundary

`system_catalog/relation_names.zig` now provides an owned, bounded before/after
claim planner and a point-store adapter over the metadata owner's existing
transaction. Tables, access indexes and constraint-owned indexes share a
namespace-qualified name space. Claims retain table identity, exact schema
epoch/digest, publication identity and reserved/active/retiring phase. Keys are
length-delimited UTF-8, preserving quoted names without delimiter ambiguity.
Records have an explicit durable format and reject unknown tags or versions.
`TableCut` derives table, access-index and constraint-owned-index names from
the authoritative schema and namespace binding. Its names-only JSON projection
skips unrelated schema payloads, owns retained names independently of request
bytes, and fences both layout version and the exact schema-byte digest. A
UNIQUE index's paired access/rule declarations produce one index claim; a
named UNIQUE or primary-key constraint produces a constraint-index claim.
Unknown provenance, dangling index-origin rules and duplicate relation names
are rejected. CHECK and FK constraint names do not reserve namespace relations.
Canonical SQL lowering, allocation faults and a 256-KiB unrelated payload
under 16-KiB allocator headroom test this extraction boundary.
The transaction-scoped `Publication` accumulator retains each table's original
before cut and coalesces schema, binding and phase updates into its final after
cut. Current LSM metadata transactions read their pending writes, but that must
not replace the original before cut or publish intermediate relation names.
Repeated producers must present the same original ownership fences; a changed
digest, epoch, phase or publication identity is rejected without losing the
previous pending cut. Independent arenas reclaim superseded proposals, and
aggregate before/after claims and table entries are bounded. Compiling the
whole transaction detects cross-table collisions before registry writes and
permits atomic name swaps without publishing intermediate names.
Validation performs one point lookup per distinct name in the before/after cut; it never
scans unrelated tables. Replaying a stale cut is not accepted merely because
the same table ID and name still exist. Unchanged cuts issue no writes.

`zig build system-catalog-relation-test system-catalog-relation-store-test`
checks collisions, namespaces, ownership phases, epoch fences, allocation
faults, exact encoding and real metadata transaction abort/restart behavior.
It also checks aggregate command limits, coalesced final cuts and atomic swaps.
The storage regression injects failure after deleting an old claim and updating
the schema, then verifies that abort restores both across restart. Independent
PostgreSQL oracle coverage checks table/index namespace collisions, equal index
names in distinct schemas, quoted names, and constraint-owned index retirement.

This is the shared publication mechanism, not completed runtime activation.
Metadata command rollback now has a bounded, first-touch before-image journal
restricted to metadata keys. It restores earlier commands' pending values,
including overwrites, creates and deletes, and merges accepted keys into the
standby effect capture only after acceptance. Matching outcome checkpoints
discard rejected reader notifications and topology deltas. Tests exercise
rejection followed by acceptance and restart, allocation failure before a key
overwrite, capture restoration and strict rejection of user-row keys. This
journal is not a user-row savepoint. Under the durable writer-adoption marker,
standalone and Raft table/schema/catalog commands now derive affected table IDs
from captured physical-table and logical-binding writes. The before-reader
retains each command's original values, while the final cut reads pending
transaction values. Admission validates and publishes ownership with those
same metadata writes; it does not scan unrelated tables. Rejected committed
proposals restore their writes and outcome signals, retaining earlier and later
accepted commands. Registry drift remains a hard error and cannot advance the
checkpoint. Bulk standalone updates coalesce the entire logical/physical edit
before admitting it, permitting atomic index-name swaps. Tests cover public
standalone rejection, Raft batch rejection/restart, corruption fences, bulk
swaps, logical binding rename/move and rejected unbinding without physical
retirement. Strict affected-key classification rejects malformed IDs.

The marker is not installed automatically and is not a serving capability.
Verified bootstrap/migration must establish the authoritative ownership cut
before adopting writers. Released standalone JSON table-create replay now
derives ownership from its final binding/topology cut before committing its
source receipt. A collision leaves table, ranges and receipt unchanged.
Binary replay on an adopted receiver instead verifies the authenticated
sender's complete before/after cut: missing claims, retained retired names,
forged digests, unrelated injected claims, cross-group ownership records and writer-marker
removal fail closed. It never repairs missing sender effects. Rejected effects
publish no reader notifications or replay receipt; a valid retry and restart
retain the exact final cut. Validation point-reads only affected tables/names
and retains the existing bounded streaming decoder and transaction boundary.
Replay's verification-only journal copies before-images only for table,
binding and ownership records, not unrelated status/report payloads. A 256-KiB
unrelated payload is overwritten under 16-KiB journal allocator headroom;
filtered journals explicitly reject command-local rollback, requiring outer
transaction abort on verification failure.
Raft snapshots now retain both writer adoption and the exact relation claims
through the exhaustive durable-projection registry. Export and install verify
the complete cut from borrowed snapshot rows before publication: every expected
claim must match, and equal cardinality excludes injected claims. Binding
identities, physical names, namespace/database ancestry and protected default
identities are verified with point reads. Missing/forged claims, orphan bindings,
malformed markers and adoption downgrade attempts cannot replace local state
or advance its checkpoint. Valid replacement removes stale local claims,
preserves other groups and survives restart. Validation is outside the install
apply lock, releases one table cut at a time and honors snapshot cancellation.
Standalone checkpoint export verifies adoption/ownership before creating the
artifact. Import verifies the complete unpublished root after bounded row
batches and before reporting success to the seed materializer. The materializer
already discards its installing root on failure; failed imports are not reusable
fresh targets. Verification streams adopted groups, physical tables, bindings
and ownership rows with cursors and shares the snapshot table-cut verifier.
It checks orphan registry groups as well as marker groups, rejects missing,
forged and injected claims or orphan bindings, and preserves ordinary document
groups alongside adopted relational groups. A 256-table regression completes
within 16-KiB verifier scratch; no whole-catalog map or user-row scan is added.
Valid multi-group/index checkpoints preserve exact ownership across restart.
`system_catalog/relation_reconciliation.zig` adds an isolated, durable
candidate-job protocol rather than backfilling the active registry in place.
Its binary job state pins a source incarnation/revision and a monotonic
candidate generation. Source preparation is bounded by 64 tables, 8192 claims
and 4 MiB of logical page bytes; its owned collision plan is prepared outside
apply. Apply point-checks the exact prior job state and current source epoch,
then commits candidate claims and the continuation cursor in the caller's
same metadata transaction. Replacement jobs fence stale pages and cannot reuse
an existing candidate range. Physical lexical cursors, not numeric table-ID
ordering, survive restart. A separate complete source pass compares ordered
source fingerprints and verifies candidate owners. A candidate-range pass then
checks cardinality and domain-separated multiset fingerprints, excluding extra
or altered entries without a whole-catalog map. Ready is not a serving state.
`zig build system-catalog-reconciliation-test system-catalog-relation-store-test`
covers malformed state, source changes, missing/forged candidates, cross-page
collisions, job replacement, allocation faults, transaction abort and actual
native-store restart after each page. Preparation against a million-table
generated source processes one 64-table page within 64 KiB scratch. Native
coverage mixes document and relational/index definitions and rereads owners
when a candidate changes after preparation. This is not installed as a
background metadata service yet. Job replacement now atomically creates a
retirement intent for the previous generation. Bounded GC prepares at most
64 candidate entries and checks the retained job high-water mark, exact
retirement identity, published-root identity and all entry before-images in
its caller's write transaction. It cannot delete a published generation, and
deletion failures require transaction abort. An exclusive lexical cursor is
persisted with every delete page; the next page seeks past processed keys,
avoiding repeated traversal of the deleted prefix's LSM tombstones. The last page
removes its retirement intent, while the one retained job per group prevents
ID reuse after cleanup without permanent per-generation tombstones. Native
coverage injects failures during deletions, cursor updates, replacement admission
and final-intent removal; it verifies rollback, resumes the last GC page after
reopening the store and checks that a pinned MVCC reader
can still read a deleted current-key version. Malformed roots, generation
exhaustion and stale GC delivery fail closed. Raft snapshot projections now
retain current jobs, candidate generations, retirement cursors and root
identities. A shared streaming verifier checks strict binary key/value
identities, current-generation fingerprints/cardinality, retirement ancestry,
remaining keys beyond GC cursors and root references without another catalog
map. Snapshot validation borrows its existing row map and runs before the
install apply lock; locked point checks prevent removal/downgrade of the local
job high-water mark, job-epoch reuse and root downgrade. Checkpoint export and
unpublished import use the same verifier, scanning one group at a time and
detecting orphan groups not reachable from a job-only scan. Native regressions
preserve candidates and roots through snapshot/install/checkpoint/restart,
retain a partial GC cursor with only its two remaining entries, reject missing
or forged claims and missing job/retirement records, and reject a correctly
framed incompatible checkpoint on import. A failed installing root remains
unusable as a fresh target. This is recovery consistency for unpublished
reconciliation state, not SQL serving or mutable active-root activation.
Binary-effect replay now verifies bounded changed-key deltas against the
receiver's previously verified cut rather than rescanning every candidate.
It fences job/epoch/root regression and sealed fingerprints, accounts for
candidate insertions, and checks retirement progress with bounded successor
probes. New candidate owners are independently point-derived from authoritative
table schemas and bindings, preventing a forged claim and matching forged job
hash from authenticating each other. Per-effect work is bounded by the source
page's table/claim limits; non-catalog metadata effects do not install a
before-image observer. Native tests reject missing job/claim effects, forged
owners, wrong names and cross-group keys without publishing a receipt or
signals, then retry the valid effect at the same log position and reopen the
store to verify durable idempotence. Pure tests cover immutable seals, GC
cursor skips, incomplete intent removal and allocator failures. This is delta
consistency and active-source membership, not a complete-source adoption or
serving capability proof; pending-generation sources remain unwired.
A separate durable source clock now advances transactionally at native table
write/delete, logical catalog delta and standalone import boundaries after
explicit internal adoption. The physical-table contribution is the exact
table identity, physical name and public schema bytes used by namespace claim
derivation, not the entire mutable table envelope. Description, placement,
read-layout and runtime-progress changes therefore cannot invalidate an
otherwise valid namespace build. Writers hash their already-decoded definition;
binary replay and equal-clock snapshot comparisons use the same identity/schema
projection. Source hashing borrows validated encoded slices without copying
schemas or unrelated fields; a 1-MiB description regression runs with an
allocator that rejects every allocation and checks schema/identity fences.
Metadata-only replay is permitted without a clock increment, while
name/schema changes still require one. A native regression preserves a prepared
page across 16 genuine metadata changes and checks both sides of replay and
snapshot admission, including restart. Exact rewrites and job/GC progress do not
advance it. Untracked document groups remain untracked: ordinary writers must
not silently adopt a protocol before the metadata capability barrier. A pinned
native source epoch combines this clock with the validated cluster incarnation;
absent authority or an untracked source fails closed. Advancing binary job
effects validate against that actual receiver epoch,
not only the producer's internally consistent job fields. Unchanged stale jobs
may remain durable for replacement/GC; their continuation cannot advance.
Snapshots/checkpoints
retain and verify the clock even in groups without jobs. Snapshot installation
and binary replay reject clock removal/regression; tracked source changes
cannot replay without an advancing clock.
Snapshot installation also rejects source changes at an unchanged tracked
revision. Its pinned local comparison streams outside the apply lock with
constant scratch and a locked revision recheck; newer revisions avoid this
local scan. Source-only checkpoint groups are validated without another
candidate-range pass.
Native tests cover stale pages after
renames, unchanged epochs during job progress, pinned MVCC reads, transaction
abort, missing-clock effects, snapshot omission/downgrade and restart. Pure
tests cover zero/malformed values, exhaustion, monotonic replay and no implicit
adoption. Clock hashing is skipped for untracked table writers.
Final candidate verification also rechecks its EOF boundary in the committing
transaction, with one successor seek after the verified tail rather than a
full generation scan. Fault tests inject a late entry into empty, nonempty and
exact-page-size cuts and prove no ready seal is committed; adjacent generations
remain independent. Read-budget assertions require one seek and at most one
successor step for the final boundary check, independent of generation size.
Bounded binary coordinator controls now adopt a tracked source and atomically
start/replace its generation through the ordinary metadata Raft/standalone
command boundary. Adoption requires durable decoder-v31 activation bound to
the exact cluster incarnation and metadata membership. It is idempotent and
does not publish a root or enable SQL serving. Start/replacement uses the actual
transactional source epoch and retained job CAS, creating retirement intent in
the same commit. Losing committed controls are no-ops checked before mutation;
native tests preserve both neighboring commands across a stale proposal and
verify the resulting job, clock and retirement after reopening the store.
The same source-mutation classifier drives writer admission and native
ownership journaling. Once tracking is adopted, even unchanged legacy table
wire shapes require decoder-v31 readiness. Both single and batched proposal
paths recheck the monotonic source floor, leader term and membership before
append; lower-version activation cannot lower a tracked source's floor. A
bounded tracking query crosses the storage-owner interface without importing
physical storage into control-only consumers. The generated control catalog
exports the pure relation contracts. Codec tests cover every truncated frame,
trailing bytes, canonical generations and allocation failures; mixed-peer
admission tests cover initial controls, ordinary tracked source writes and
adoption between preparation and final validation.
`zig build antfly-relation-coordinator-test system-catalog-relation-store-test`
exercises these control/admission paths. Source adoption is not automatic;
native source/candidate/GC page preparation now streams from pinned read
transactions and returns owned bounded plans, without acquiring the apply
mutex. One reusable physical cursor performs sequential reads, rewinds correctly
after admission lookahead, and preserves lexical keys across point projections.
The native 65-table restart/fault regression uses these production preparers
through build, source verification, independent candidate verification and
retirement GC; stale job preparation rejects before source decoding. Native
allocation-fault coverage unwinds pinned readers and owned source projections,
and a real rename rejects both stale preparation and stale page application.
Bounded Raft/standalone intents now advance source/candidate pages and retired
generation GC. Frames carry the exact durable before cut, not leader-supplied
claims or source JSON. Each replica prepares from its own pinned authority,
then revalidates source epoch, job, root, applied log position and selected GC
intent under the apply lock. A moved preparation cut is retried outside that
lock. Each batch retains at most one source/candidate page and one GC page;
duplicates and unobserved future cuts do not implicitly chain through the
batch. Producers must observe committed progress before requesting successors.
Conflicting/stale proposals are checked before mutation and do not poison
neighboring metadata entries. Committed entry framing is decoded once outside
the serialized apply section. Native two-replica traces cover duplicate replay,
future-cut batching, source changes preceding a page, checkpoint-only changes,
six-page 65-table reconciliation, mixed standalone/Raft execution, protected
root admission/revalidation, two-page 65-claim GC and reopen durability.
Source clocks do not advance with page/GC progress; these commands still do not
publish roots or enable SQL name resolution.
The committed-apply outcome registration now releases ownership before
unlocking. Its deferred cleanup cannot clear the next writer's active outcome;
a deterministic ownership-handoff regression covers successful and failing
cleanup paths.
Scheduler observation now crosses the storage-owner boundary as one owned,
fixed-size cut from a pinned native transaction: actual source epoch, current
job, root identity and oldest collectible retirement. It does not decode table
schemas, acquire the apply mutex, or materialize a retirement list. Selection
uses one retirement-prefix seek and at most one successor step to skip the
single protected root, independent of backlog size. Pure read-budget tests
cover 128 retirements, protected-only and adjacent-group cuts, malformed owner
identities and orphan retirements; the native replica/restart trace consumes
this production observation throughout. A compiled-owner regression exercises
the opaque client/JSON projection through adoption, all empty-catalog phases,
retirement, GC and reopen, retaining earlier owned cuts across mutations:
`zig build antfly-storage-owner-test -Dstorage-owner-test-filter='opaque metadata relation reconciliation work'`.
Observation does not adopt tracking, schedule work or publish a serving root.
Permanent source conflicts and per-source-row admission limits now record a
bounded terminal reason in the retained job, without discarding its last
committed phase/cursor/fingerprints or partially adding the rejected page.
Each replica derives the failure from its own pinned source; advance intents
carry no leader-supplied failure claim. Cross-page ownership collisions are
checked before candidate writes. Failure publication uses the same job/epoch
CAS and atomic metadata/outbox commit as successful pages. Resource, I/O,
corruption and stale-generation errors are not reclassified as permanent source
failures. Failed generations cannot advance or have their reason/progress
rewritten; authenticated replay and newer snapshot installation retain that
terminal fence. New generations may reconcile corrected source epochs and
retire failed partial candidates normally. The unpublished job format is now
`AFRC04`, binding owner-slot candidate fingerprints; no compatibility decoder is
added for this PR's unpublished format.
Pure tests verify stale/failing writes, immutable failure replay, strict reason
decoding and partial-candidate fingerprints. Native two-replica tests cover
same-page/cross-page conflicts, duplicate commands with surviving neighbors,
snapshot downgrade rejection, reopen and successful rebuild after a source fix.
The opaque storage-owner regression also reads the durable failure through the
compiled control projection and checks it after reopen.
Leader scheduling now shares both in-process and HTTP metadata control/lifecycle
rounds, including metadata-only nodes. A per-service `std.Io` lane and 250-ms
cadence admit at most one bounded intent per slice. Tracking adoption remains
explicit; tracked sources start missing jobs, replace stale source epochs by
CAS, advance committed cuts and alternate forward work with collectible GC.
Unchanged failed epochs remain stopped. Pending receipts suppress repeated
preparation/append until local Raft apply catches up; term changes, coordinator
restart and ambiguous admission re-observe durable state rather than assuming
that an intent won. No job apply waiter or separate polling thread is created.
Capability refresh has a bounded request context, and exact-term/membership
admission still runs through the ordinary proposal path. Background proposals
release synchronous-waiter compaction proofs immediately so page churn cannot
exhaust the bounded receipt tracker. Pure coordinator tests cover cadence/read
budgets, GC alternation, lane contention, leader loss, overwritten receipts and
ambiguous outcomes. A real metadata/Raft control-round regression covers
explicit adoption, initial reconciliation, coordinator restart, source-epoch
replacement, conflict failure, stopped retries, GC and corrected-source rebuild;
it also verifies no root publication and zero retained proposal receipts.
`zig build antfly-relation-coordinator-test` exercises these paths.
`relation_names.Entry` now models one active owner alongside one reserved
successor, including a restore successor with a different physical table ID.
The active cut remains separate and readable; a pending-only name has no active
owner. Reserve compares the exact predecessor and is idempotent only for the
same complete pending owner. Publish and cancel compare both cuts; ordinary DDL
cannot replace an entry with a pending successor. The unpublished `AFRE01`
codec rejects impossible phases, absent-slot garbage and truncated records.
An owned, bounded `EntryPlan` validates every affected name before writes and
verifies replay without synthesizing missing effects. Its adapter borrows the
caller's metadata transaction and generation-bound keyspace, never opening or
committing a second transaction. Pure tests cover collision/stale publication,
canonical bytes, input ownership and allocation failures. A native regression
checks rollback, committed publication, pinned-reader isolation, stale cancel
and reopen. These are compound-entry mechanics, not publication authority:
producers still must validate the immutable plan and capability/lifecycle cut.
Reconciliation candidates now use canonical compound entries throughout
building, source/candidate verification, snapshot/checkpoint verification,
authenticated delta replay and bounded GC. Fingerprints include the complete
active/reserved bytes and the source's explicit successor table identity.
Candidate active-owner reads never return a pending-only name. Preparation
transfers one arena into its immutable entry plan and omits explicit empty
before-images, retaining the existing 64-KiB scratch regression. A compound
candidate test verifies distinct replacement IDs, pending-only invisibility,
forged successor detection, snapshot fingerprints, allocation faults and GC.
Native replay rejects even correctly fingerprinted pending claims when no
authoritative pending-plan source exists; the new proof is not self-authenticating.
Writer admission and active-root serving remain unfinished. There is no fallback
decoder or newly enabled public lookup.
Independent sources can now contribute an active owner and a restore reservation
to the same candidate name, either within a page or across page boundaries. The
reservation binds its exact predecessor; missing or stale predecessors and
duplicate reservations fail before page mutations. In-page normalization uses
the transferred arena and a bounded key-to-offset map, not a second full cut.
Counts and additive fingerprints track contributed owner slots independently;
source fingerprints additionally bind the complete dependency and contribution
mode. Public source verification tolerates separately proven pending slots,
while the final candidate pass still verifies exact entries. Delta replay permits
only an active-only to active-plus-pending transition with an unchanged active
owner. Pure tests exercise both page shapes, missing/stale dependencies,
duplicate reservations, source dependency tampering, snapshot/delta verification,
allocation faults and the unchanged 64-KiB scratch limit. This is the reconciliation
primitive, not restore activation: native restore source projection, reservation
admission, lifecycle/capability barriers and root serving remain unfinished.
The final pure/native catalog gate passes 236 tests plus 22 linked storage tests
without failures or leaks. Formatting, whitespace, source-catalog integrity and
the original inventory check pass; the 902 unresolved original cases remain
uncredited by this internal protocol work.
Native authority verification now covers changed existing candidate entries as
well as inserts. Adding a reserved owner to an active candidate cannot bypass
the durable publication's exact schema and plan identity by carrying a matching
candidate/job fingerprint. Byte-identical effects skip redundant derivation;
changed entries retain the bounded per-table schema/name cache. A journal-backed
native FK regression reproduces the previous acceptance of a forged pending
schema digest, then checks that an authentic reservation update succeeds and
forged schema or publication identities fail. This closes a replay-admission
gap; it does not activate restore sources or credit original SQL parity cases.
The regression fails against the previous insert-only check and passes with the
fix. The final native catalog gate passes 166 tests plus 22 linked storage tests,
with no failures or leaks; formatting, whitespace, source-catalog and inventory
integrity checks also pass.
Restore namespace inputs now participate in the same transactional source epoch
and equal-clock snapshot/replay proofs as public tables and FK publications.
A names-only projection binds the immutable plan identity, target definitions,
logical namespace/name bindings and exact replacement definitions. Artifact DOMs,
receipt counters, placement and runtime progress are excluded. Canonical active,
physical-name and old/new identity reservation keys are included; snapshot proof
scans seek those key families instead of visiting owner receipt histories.
Reservation admission and final retirement advance the source clock in their
existing metadata transaction. Artifact freeze compares semantic cuts, reusing
the already parsed predecessor plan; unchanged receipt/progress commands do not
run additional namespace projection work. A native projection regression reads
512 KiB of ignored artifact data using 32 KiB of scratch and exercises allocation
faults, identity/name changes and malformed reservation keys. Actual rewrite
admission/cancellation fixtures check clock changes across restart, and artifact
sealing retains both the clock and complete source fingerprint. This is source
fencing, not source-stream activation: source-stream integration, native
candidate ownership derivation, admission/serving barriers and root publication
remain unfinished. No original SQL parity case receives completion credit.
The native active-restore cursor now projects one reserved target per source row
and uses a fixed-width target ordinal within the active-job lexical bookmark.
It caches one owned job projection, honors older cursors after an unadmitted
lookahead crosses a job boundary, and never scans terminal job/receipt histories.
Target projection checks exact active/name/identity reservation pointers, the
absence of a public successor, current predecessor name/schema and catalog
namespace hierarchy. Shared names retain an exact active dependency; old-only
names are omitted from this reservation contribution. Predecessor comparison
borrows row bytes only until the next read, retaining the cached plan's schema
instead of allocating another schema-sized copy. Prepared pages own their names
after the source and transaction close.
Native regressions exercise 70 targets, a second active job, 1,000 malformed
terminal-history records, 64-row paging, cross-job rewind and forged reservation
pointers. Real replacement-restore admission/restart fixtures check both owners
and exhaustive allocation failures with arena resize disabled for deterministic
fault points. The final gate passes 168 native catalog and 22 linked storage
tests with no failures or leaks.
Active restore targets now participate in the ordinary production reconciliation
source stream, after hidden creates and public tables. Native candidate-owner
verification uses the durable source bookmark to distinguish public owners
prepared before a restore contribution from complete entries after it. One owned
projection per affected active plan maps old/new physical IDs to the same target;
verification derives each full target cut once and retains only touched candidate
names across targets. Admission bounds apply to touched candidates and source
targets, not the sum of unrelated cold names in predecessor schemas.
The integration regression exercises 70 replacement targets across public and
restore page boundaries, rejects authentic-but-premature pending owners and
forged publication IDs, and verifies structural, replay, source-epoch and native
ownership proofs on every committed page. Real admitted replacement restores
also run this pipeline after restart. The driver preserves durable job identity
and verifies that a ready private generation does not publish a serving root.
The final gate passes 169 native catalog and 22 linked storage tests; inventory,
source-catalog, formatting and whitespace checks pass. Writer adoption,
activation/serving barriers, root publication and original SQL parity credit
remain unfinished.
A native publication-proof preparer now independently rechecks a ready
generation against all three authoritative source ranges, then scans the
candidate range for exact cardinality and owner fingerprints. It pins one
metadata read transaction, retains one restore projection and one bounded page
at a time, and performs no writes or apply-lock catalog scan. The owned result
binds the exact job, source epoch, root and local applied-log position for an
O(1) write-transaction recheck. Root ancestry/retirement protection is verified
separately; a forged ready source seal cannot authenticate itself through
structurally consistent candidate counts. Same-snapshot owner disagreements
are corruption, not retryable generation races.
Native tests cover 70 replacement targets, source/candidate forgery, wrong-group
roots, every captured-cut fence, actual admitted restore after restart, and
exhaustive allocation failures. The final native gate passes 169 catalog tests
plus 22 linked tests; inventory, source-catalog, formatting and whitespace
checks pass. This prepares independent evidence, not a new serving authority:
the publisher/coordinator still must connect capability and producer-lifecycle
barriers, writer adoption and atomic root publication. No SQL disposition changes.
The unpublished writer registry now uses the same canonical compound-entry
format as reconciliation, without a single-owner fallback decoder. Active reads
return only the active slot; before/after mutation plans compare complete
entries, including every pending schema/publication identity, before the first
write. Retained names still require only one admission point read. Replacement
publication explicitly binds predecessor and successor physical IDs, supports
old-only/shared/new-only names, and rejects partial reservation contributions
where a complete writer cut is required. Replay derives affected IDs from both
owner slots. Snapshot/checkpoint checks compare complete entries, so an
unauthorized pending slot cannot hide behind an unchanged valid public owner.
Pure tests exercise replacement cutover, ordinary update/delete theft, hidden
pending-only reads, stale publication and exhaustive allocation failures. A
native regression preserves a compound reservation across restart, rejects
ordinary rename without advancing revision or changing its physical table,
and rejects a checkpoint reservation with no authoritative producer plan.
The native gate passes 170 catalog tests plus 22 linked tests; pure namespace
and reconciliation tests, inventory, source-catalog, formatting and whitespace
checks pass. This unifies writer mechanics, not automatic adoption: native
producer-plan composition, capability/lifecycle barriers and root serving
remain unfinished. No original SQL parity case receives completion credit.
FK generation and hidden-create producers now compose their durable plans into
the same native writer transaction as physical metadata and registry entries.
Captured publication/work keys select only affected tables; exact predecessor
and successor cuts include pending-only names and complete schema/publication
identity. Ordinary table and binding writers use that same projector, preserving
the existing prohibition on silently dropping a bound logical identity.
At an unchanged tracked source clock, receipt-only writer preparation does not
decode jobs or schemas and performs zero preparer allocations. Untracked groups
and mixed changed batches retain semantic digest comparisons; authenticated
replay independently verifies source-clock integrity before committing, including
the unchanged-clock forged-plan regression.
Snapshot and checkpoint verification now include hidden active-work ranges and
complete FK publication cuts, sharing exact entry verification with public
tables. Hidden/public identity overlap fails closed rather than being counted
twice; terminal history is not scanned as hidden work. Actual FK lifecycle
fixtures explicitly bootstrap writer ownership, verify reservation entries and
active-read invisibility, validate snapshots/checkpoints, and retire hidden names
after cancellation across restart. The bootstrap helper is test-only, not an
automatic migration or serving capability.
The final native gate passes 170 catalog tests plus 22 linked tests; inventory,
source-catalog, formatting and whitespace checks pass. Restore producer-plan
composition, automatic adoption, capability/lifecycle barriers and atomic root
serving remain unfinished. Original SQL dispositions and counts are unchanged.
Restore producers now compose exact active and pending namespace entries in
the same native command transaction, including compound user-job admission.
Before and after sources are independently deduplicated by canonical target;
old/new physical aliases share one owned active-plan projection. The command
builder retains only copied names and typed entries in one transferred arena,
releasing each decoded schema cut immediately. Receipt-only changes at an
unchanged tracked source clock still skip projection. The journal's before
reader derives authority from captured active pointers and immutable plans,
not the newer runtime progress record. Cross-cut plan loading prevents a
tampered physical pointer from hiding a still-active producer reservation.
Snapshot/checkpoint verification shares the full restore-target projector,
including fresh targets with no public table row. It scans active jobs, not
terminal history, rejects overlapping FK reservations, and derives each target
once across predecessor/successor aliases. Snapshot cancellation is checked
between remaining restore targets. Real replacement reservation, artifact
freeze and cancellation fixtures now enable writer ownership, as does the
multi-target fresh publication fixture. These fixtures exercise pending-only
visibility, exact owner promotion, snapshot/checkpoint validation and restart.
Builder tests cover borrowed-name retirement, duplicate cuts and exhaustive
allocation faults. This closes restore producer composition, not automatic
writer adoption, capability/lifecycle barriers, atomic root serving or public
unqualified index resolution. Original SQL dispositions remain unchanged.
Final validation passes 170 native catalog tests, 22 linked storage tests,
6 coordinator tests and the 74 pure namespace/reconciliation tests (cached
unchanged component gates on the final run). Inventory, control-catalog,
formatting and whitespace checks pass. An initial combined compile exhausted
local disk space; after removing only obsolete generated binaries/objects, the
final native and component gates passed separately.
Publication verification now has an owned resumable scan over one pinned
metadata snapshot. Each step verifies at most one bounded source/candidate
page; the existing synchronous helper shares that verifier rather than a
second proof implementation. The handle has a stable address for borrowed
prefixes/transaction references. Source cursors are released on phase handoff;
completion, cancellation and errors close all remaining cursors and release
the snapshot immediately. Completed proof reads are idempotent, while a
canceled/failed scan cannot resume or expose partial evidence. Hosts must still
bound idle scan lifetime, because pinned engine snapshots retain old pages.
The 70-target regression checks yielding after 64 source rows, independent
candidate verification, cancellation in both phases and an intervening commit.
That commit cannot splice newer pages into the old scan; the resulting proof
fails its write-transaction epoch fence. Actual admitted restore fixtures run
exhaustive allocation failures through both borrowed-transaction verification
and the owned snapshot handle. The final native gate passes 170 catalog and
22 linked storage tests without failures/leaks; inventory, control-catalog,
formatting and whitespace checks pass. Coordinator scan scheduling,
capability/lifecycle barriers, automatic writer adoption and atomic root
publication remain unfinished. No original SQL disposition is changed.
Both metadata service variants now schedule the owned publication verifier
through the existing serialized control-round lane. Each active scan consumes
one page budget per round and defers that worker's GC append until verification
finishes. The local completed proof is cached, not treated as serving authority.
Exact ready state, source epoch, root and leader term changes discard old work;
leader loss, missing projection and service teardown release the retained scan.
Shutdown closes the worker before releasing resources, preventing recreation.
Snapshot lifetime is capped at 60 seconds with delayed retry; expiry runs before
leader/status observation, so runtime-lane contention cannot indefinitely pin
an old snapshot. Transient failures back off, while corruption stops retries
for the unchanged cut. Very large/slow catalogs can exceed this preparation
deadline and require further scheduling/renewal work; expiry is not successful
verification. Proofs retain their write-transaction recheck requirement and are
not refreshed merely because unrelated metadata or GC commits changed apply
position. Coordinator tests cover per-step work, completion caching, term/root/
epoch replacement, cancellation, resource/corruption failures, expiry and
shutdown. The real service fixture now waits for matching completed evidence
rather than accepting only a ready job flag. Capability/lifecycle barriers,
writer adoption, atomic root publication and public SQL resolution remain
unfinished; original case dispositions are unchanged.
Final focused validation passes all 10 coordinator tests without failures or
leaks, including the four new preparation/lifetime tests and the real service
fixture. Inventory, control-catalog, formatting and whitespace checks pass.
An earlier final compile exhausted local disk; only obsolete generated test
binaries/objects were removed before the successful final run.
Long scans now renew their physical read snapshot at the preparation deadline
without discarding verified pages. Renewal first opens a fresh transaction and
rechecks the exact original ready job, epoch, root and applied-log position.
Only then are old decoded sources/cursors released and the old snapshot closed;
owned lexical cursors and accumulated verification totals resume on the new
snapshot. Source and candidate phases share the same renewal operation.
Completed evidence never reopens a snapshot. Any renewal failure closes both
transaction resources and invalidates the partial scan; resource/race failures
retain the coordinator's bounded retry backoff. This removes the previous
fixed-duration catalog-size limit for unchanged cuts while retaining physical
snapshot lifetime bounds. It does not rebase progress across unrelated commits
in the same metadata group: the existing applied-index fence remains strict.
The 70-target regression renews between every source/candidate page, verifies
unchanged accumulated state, and rejects both epoch-changing and index-only
commits on renewal. An independently pinned scan still completes its old proof,
which fails current-transaction admission. Actual restore allocation-fault
tests include owned renewal, and coordinator tests cover successful deadline
renewal plus stale/resource failures. Capability/lifecycle barriers, automatic
writer adoption and atomic root publication remain unfinished; no original SQL
case receives completion credit from this preparation work.
The combined final gate passes 170 native catalog tests, 22 linked storage
tests and all 10 coordinator tests without failures or leaks (57/57 build
steps). Inventory, control-catalog, formatting and whitespace checks pass.
The branch also merges main's EmbeddingGemma/decision-response changes through
`d409767dc7`. The sole conflict was the generated Go client's compressed
OpenAPI blob; regenerating from the merged specification preserves both SQL
contracts and the new inference types. The complete Go SDK suite passes with
loopback test servers enabled. The focused SQL decision/INSERT gate passes all
28 contracts across compiler, native runtime, streaming, conditional provider
evaluation, mutation atomicity, allocation faults and request validation.
Post-merge native validation also passes 170 catalog, 22 linked storage and
10 coordinator tests (57/57 build steps), with inventory, control-catalog,
formatting and whitespace checks passing on the merged tree.
Published-generation primitives now use a separate canonical live manifest
(`AFRL01`) carrying the source epoch, sequence and exact owner-slot count/hash.
Publication performs four point reads and two writes for an initial root,
without copying candidate names or walking terminal history. Prepared namespace
plans mutate only affected generation keys and update the manifest in the same
transaction; errors require abort. Active reads hide pending-only reservations,
while full entries and integrity totals retain both active and pending owners.
Exact root/manifest CAS rejects stale writers, and retirement GC remains blocked
until the root swaps. Allocation-fault tests discard every failed transaction.
The shared snapshot verifier now tracks the mutable root independently of a
building/ready replacement candidate. Authenticated replay accounts for changed
live entries by subtraction/addition over the previously verified manifest,
rejects removed manifests, bad epoch/sequence transitions and missing deltas,
and permits root swaps only to an unchanged, previously ready candidate seal.
Source incarnation and revision are fenced. Root replacement followed by GC,
count-preserving corruption and simultaneous root/candidate verification have
component coverage. The pure reconciliation gate passes all 53 tests; the
relation-name gate also succeeds from cache. Main's cold-decode/batch-planning
changes through `755cb12a68` are merged without conflicts.
These are shared storage primitives, not native activation authority. Native
snapshot projection/authoritative live-owner validation, replay ownership for
older live generations, capability/lifecycle barriers, production writer
adoption and atomic root publication still need integration. Public SQL
resolution is not enabled and no original case receives parity credit here.
The final-source native regression gate passes 170 catalog, 22 linked storage
and 10 coordinator tests (57/57 build steps), without failures or leaks.
Inventory, control-catalog, formatting and whitespace checks also pass; the
original inventory remains 475 implemented, 136 rejected, 73 superseded and
902 unresolved.
Native writers now route prepared producer cuts into an installed live
generation, updating its exact manifest in the same metadata transaction and
command journal. They do not copy names at publication or fall back to obsolete
standalone registry entries on a live miss. Native snapshots/checkpoints retain
the manifest and independently authenticate the root's full active/pending
entries against table, FK and restore cuts, including distinct-name coverage;
the count/hash alone is not accepted as ownership authority. Recovery pins one
registry per verification pass rather than reloading the root per table.
Snapshot installation fences live-manifest retention, source incarnation and
revision, and same-generation sequence monotonicity. Old registry bytes can
remain physically present but cannot affect live reads or authenticated writes.
Standby replay derives and checks complete before/after producer cuts for live
generation keys, including deletions and older published roots while a newer
candidate exists. Native coverage exercises pinned reads, schema/index rename,
collision rollback, checkpoint export/import, self-consistent forged hashes/clocks,
and reopen. Eight correctly framed replay fault variants cover missing writes,
deletes/manifests, forged/extra owners, obsolete registry mutations, disabled
writers and root swaps lacking receiver publication authority; rejection leaves
the old root, namespace, receipts and notifications unchanged. The multi-target
restore publication fixture now uses a live root for hidden reservations,
promotion, validation failures and restart.
Native activation remains deliberately incomplete: automatic capability-fenced
writer adoption/root publication, receiver-side staged publication proof and
public unqualified SQL resolution are not enabled. Replay rejects a root
generation switch until that proof/capability protocol exists; it must not
replace independent authority with a ready fingerprint or a full scan under
the serialized apply lock. No original SQL case is credited for this integration.
The final gate passes all 172 native catalog tests, 22 linked storage tests and
10 coordinator tests (63/63 build steps); the 53 reconciliation and 27 name
component tests also succeed from cache. Inventory, control-catalog, formatting
and whitespace checks pass with the original dispositions unchanged.
Replica-local publication preparation now shares a fixed-capacity native pool:
eight evidence slots and at most two active owned scans per store. Each call
advances one bounded page outside the apply mutex; unrelated groups cannot
evict active scans. Completed evidence is reused only at the same applied-log
position, source epoch, ready state and root identity. Admission is nonblocking,
idle snapshots close after sixty seconds, busy snapshots renew without losing
their cursors, and teardown closes resources before the native store. Retryable
failures back off; corrupt immutable cuts stop until invalidation. Service
control rounds use this same pool instead of owning duplicate leader scans.
Fake-host tests cover capacity, inactive-evidence eviction, cut changes, expiry,
contention and allocation failures. The real seventy-target restore fixture
checks bounded page advancement, progress-preserving snapshot renewal, idle
release and completed-proof invalidation after index-only and source commits.
This is preparation machinery, not root activation: decoder/producer capability
barriers, receiver proof consumption and automatic publication remain unfinished.
Public SQL resolution and the original parity dispositions are unchanged.
The final-source gate passes 172 native catalog, 22 linked storage and 14
coordinator/service tests (63/63 build steps), with no failures or leaks; pure
name/reconciliation gates succeed from cache. Inventory integrity, control-catalog,
formatting and whitespace checks pass. No original cases are reclassified.
Metadata preparation now has an explicit retry contract through SnapshotBuilder,
MetadataStateMachine, routed apply and the queued Raft adapter. Native receiver
admission exposes a distinct pending-proof outcome before transaction/mutex
entry; ordinary contention, allocation failures and catalog corruption are not
implicitly retried. A queued MultiRaft fault trace retains later reads of the
blocked group while completing another group's reads, and does not swallow
unexpected failures. Metadata-wrapper coverage checks that three preparation
deferrals emit neither durable-apply completion nor delegated ReadStates.
The control-only metadata facade now shares the native pool through a typed,
fixed-layout ABI for one-page preparation, cancellation, expiry and blocking
teardown. Requests carry canonical State bytes; responses contain only scalar
evidence and an optional canonical Generation. Native snapshots and cursors
never cross the boundary, teardown performs no serialization/allocation, and
the public service retains only a storage-free evidence observation. Pending
preparation has distinct append-only identities in both runtime failure ABIs.
Opaque-owner tests cover malformed/null/version/group/operation requests,
completed-proof reuse, expiry/close, stale generations and error identity.
These additions do not activate publication. Capability/lifecycle barriers,
receiver proof consumption, snapshot-prefix replay amortization and public SQL
resolution still require integration; no original parity case is reclassified.
Final-source validation passes all 97 build steps: 172 native catalog, 428 Raft,
22 linked storage and 15 coordinator/service tests, plus the real cross-archive
owner regression, with no failures or leaks. Runtime error transport passes
25 tests, the storage failure registry passes all three round-trip/uniqueness
tests, and the released storage-status identity fingerprint is unchanged.
Inventory integrity, control-source catalog, formatting and whitespace gates
pass; the original 475/136/73/902 dispositions remain unchanged.
Native metadata snapshot retries now use a replica-local installation receipt
(`AMSI01`): one fixed-size record per group carrying the installed Raft index,
byte length and SHA-256 of the accepted bytes. The receipt and snapshot rows
commit atomically with the applied watermark. Exact retries perform two point
reads and no decoding, catalog scans, row allocation, writes or repeated
notifications; hashing the supplied bytes remains linear in input size and
runs outside the apply lock. The receipt survives later entry commits and
reopen, so a retried snapshot prefix cannot roll back completed log work.
Changed bytes at the installed index, corrupt/future receipts, and stale or
equal-index snapshots without a matching receipt are rejected. Only the latest
receipt is retained, bounding history storage, and it is deliberately excluded
from replicated catalog snapshots. A final locked recheck fences concurrent
apply/installation during off-lock preparation.
Native tests inject every caller-allocator failure during first installation,
verify atomic absence of receipt/watermark/notifications on failure, prove
zero caller allocation on exact retries, and run the actual metadata wrapper
through three deferred preparation rounds followed by one completed entry/read.
They cover later-prefix retention, same-length byte substitution, six receipt
faults, receipt replacement, snapshot exclusion and restart. The opaque-owner
regression repeats these preservation/fork checks across the compiled ABI and
reopen. The earlier self-install fixture now restores into a separate receiver
before replaying its referenced heartbeat, matching actual snapshot transfer.
This completes native snapshot-prefix retry amortization, not root publication
or public SQL activation; the original parity dispositions remain unchanged.
The embedded database-catalog merge retains named/composite conflict targets,
constraint metadata, multi-table routing and staged-transaction conflict reads.
Its SQL C-ABI gate passes 51/51 build steps and 12 tests after array fixtures
were migrated to the actual default-table catalog identity. The snapshot retry
gate before this merge passed 94/94 steps, including 173 native catalog tests,
15 coordinator tests, 22 linked tests and 10 opaque metadata-owner tests.
Embedded-session mutation folding now uses an indexed physical-table/primary-key
tuple rather than a quadratic scan. It preserves first-seen order and original
write predicates while retaining the latest non-predicate postimage. Table ID,
schema version and storage mode are fenced across distinct keys too, closing
the earlier per-key-only generation check. Commit request construction groups
rows in one pass and emits deterministic first-seen table order, without a
full input rescan per table. Transferred buffers have explicit failure cleanup.
Standalone fold tests cover tuple identity, predicates, deletes/resurrection,
generation fences and caller-allocation faults. A reproducible ReleaseFast
microbenchmark (`mutation_fold_bench.zig`, 4,096 inputs, 20 iterations) measured
legacy/indexed fold time of 32.46/0.478 ms for one-table distinct rows,
19.36/0.486 ms across 64 tables, 10.14/0.317 ms for half-repeated rows and
0.171/0.139 ms for 32 hot rows. The distinct one-table indexed path allocated
1.21 MB versus 0.54 MB cumulatively per fold; temporary hash storage is released
before return. These are CPU/allocation microbenchmarks, not end-to-end SQL or
storage throughput. Native session integration passes 51/51 build steps,
including its new request-buffer allocation-fault test and 12 C-ABI SQL tests.
No original parity disposition is changed by this infrastructure improvement.
Exact binary64-to-decimal expansion now formats bounded base-10^9 limbs rather
than repeatedly dividing a 4,096-bit integer. SQL JSON numeric comparisons,
decimal-token hash equivalence, PostgreSQL NUMERIC float coercion and canonical
JSON persistence share the allocation-free kernel; their logical decimal and
budget contracts are unchanged. The independent fixed-integer oracle checks
every finite exponent, four boundary/pattern mantissas and both signs, including
subnormals, signed zero, output-buffer limits and nonfinite rejection. A local
ReleaseFast microbenchmark compares identical output checksums for 64 values
spanning the finite exponent range in alternating execution order: the old wide
formatter takes 1,366–1,419 ms versus 0.183–0.217 ms for decimal limbs across
three samples. These are kernel measurements, not end-to-end query throughput.
The change adds no original-case credit.
The isolated `zig build sql-exact-float-test` gate is also a normal unit-suite
dependency. Its debug oracle checks every exponent and both signs in 860 ms
after replacing repeated per-byte wide parsing with native-sized decimal
chunks; magnitude sharing does not remove any signed codec checks. The release
variant (`-Doptimize=ReleaseFast`) runs the checksum-verified microbenchmark.
All 13 selected canonical-content/hash tests pass, including exact native
`0.1`, signed zero, wide integral floats, subnormals and maximum finite values
through strict decimal-token decoding and byte-stable re-encoding. This is
codec/persistence-boundary evidence, not a claim of new original SQL coverage
or complete distributed restore qualification.
Native relation publication now has a separate v32 command carrying the ready
cut, exact predecessor generation and membership-bound activation, never a
sender-authored publication proof. Replica preparation authenticates source and
candidate pages before the apply lock; pending evidence keeps the existing
per-group Raft continuation unacknowledged. A publishing transaction rechecks
the original applied position, job, source epoch and root, then swaps root/live
manifest and adopts all native writers atomically. Raft checkpoint advancement
is explicitly separated from the initially fenced proof position; earlier
commands in the same batch can make publication a deterministic stale no-op.
Published groups retain the v32 producer floor and reject activation downgrade.
The command codec checks canonical ready states, epoch/incarnation binding,
strict predecessor ordering, truncation and allocation faults. Native tests
exercise 70-table multi-page receiver preparation, pre-v32 rejection, a stale
batched source, first publication, duplicate commands, mutable live writes,
root replacement, membership mismatch and reopen. Native publication passes
47/47 build steps, 175 catalog tests and 22 linked tests. The expanded mixed-v31/
v32 proposal and stale-admission checks pass with all 15 coordinator tests
(50/50 build steps); inventory, formatting and whitespace checks also pass.
Public SQL namespace resolution and its original parity cases remain unfinished.
Standby apply now treats only `CatalogPublicationProofPending` as a cooperative
yield, retaining the unapplied record and its successors behind the durable
applied/safe-read frontier. Unexpected corruption and ordinary apply failures
still propagate. A real receive/progress-WAL regression exercises three deferred
rounds, restart before completion, promotion refusal, subsequent hard failure,
ordered resumption and a second reopen. The standby gate passes 58/58 build
steps, including that regression among 444 hot-standby tests.
A fixed-memory metadata point probe consumes at most one authenticated transport
frame per call, owns its target keys and bounded captures, and skips unrelated
large values without allocating a complete row. Captures are unavailable through
its admission API until both the effect footer and descriptor digest verify.
Three focused tests cover large skipped values, missing/empty/deleted captures,
transport-buffer retirement, split headers/keys/values/footers, duplicate target
keys, capture overflow, private-key exclusion, and inner/outer integrity faults.
This probe now drives progressive standby preflight, but is not publication
authority or a replacement for canonical replay validation. A new generation
requires the receiver's own bounded source/candidate proof, a preexisting ready
job, the exact predecessor root/applied cut, and v32 membership/incarnation
activation. Commit rechecks that cut and activation, enforces the initial live
manifest, and atomically adopts native writers. Replay cannot change the sealed
job/candidates in the same adoption effect or enable a writer marker without
verified adoption. Pending preparation preserves the prior standalone projection.
The receive WAL owns the final record while preparation yields; ordinary
single-frame effects need no extra staging commit. After restart the probe
reconstructs one staged prefix frame per retry without extending staging or
exposing rows. The existing 9 MiB checkpoint/corruption regression now asserts
that bound and retains exact source, timeline, gap and checksum diagnostics.
A real primary-log/standby-WAL test exercises a 70-table publication, restart
during proof preparation, promotion refusal, live producer writes, mirrored
reconciliation, root replacement and another reopen. Six fault cases cover a
missing writer, marker-only adoption, sender candidate mutation, changed
membership, changed source and receiver-local candidate corruption; rejected
effects publish neither rows nor receipts nor listener signals. The expanded
catalog/opaque-owner gate passes 84/84 steps, including 182 catalog tests,
10 opaque metadata-owner tests and 22 linked tests.
Standby application now reads one indexed owned record at a time rather than
materializing two copies of the whole receive backlog on each retry. An 8 KiB
record-buffer budget covers 64 queued 4 KiB records, repeated deferral, one-/
three-record windows, complete ordered drain and cleanup; real OOM remains a
hard failure before callback or progress advancement. That regression passes
with all 448 hot-standby tests. These are bounded-work/allocation proofs, not
production throughput claims. No original corpus disposition changes from
these infrastructure tests.
Automatic coordinator publication now advances explicitly tracked groups through
separate capability-activation, proof-preparation and publication rounds. HTTP
capability observation neither appends nor waits for activation, and does not
populate the reusable durable-ready cache. The worker retains only a term/index
receipt while activation is in flight, then prepares one bounded local proof
page per eligible round. Publication carries its exact predecessor and durable
membership activation; final proposal admission binds that wire activation to
the readiness token rechecked under the runtime lock through append. Ordinary
background intents consume cached/durable capability only, never entering an
activation/apply wait. Collection gets its alternating round even when a ready
replacement is waiting for proof or capability. Already-published generations
cancel their preparation instead of repeatedly proposing the same root.
New regressions cover activation receipt suppression, deferred proof work,
publication codec round trips, stale cuts, member-count/fingerprint changes,
GC fairness, real HTTP probe-only versus durable admission, and real control
rounds that reject a conflicting historical baseline before publication, repair
it, publish, mutate a live source and swap/collect the replacement.
Replication completion now requires applied EOF, not merely transport EOF.
The shared client/runtime completion check yields an unacknowledged durable tail
to the next scheduled round instead of claiming catch-up or spinning. The data
maintenance boundary treats the exact proof-pending outcome as cooperative work,
not transport degradation/backoff. New HTTP primary/standby coverage defers the
middle record three times, verifies that successors and upstream applied
acknowledgements stay behind it, then drains the retained tail in order.
These changes do not enable unqualified SQL index resolution by themselves or
change any original parity dispositions. Verification passed 50/50 coordinator
build steps (18 tests), 14 current worker component tests, all 450 hot-standby
tests, and 93/93 runtime build steps (three selected runtime regressions).
The runtime gate required loopback networking for its existing HTTP listener;
the sandbox-only listener rejection was not counted as a code failure.
Bulk catalog resolution now has an opt-in relation-name contract: at most 256
combined forward, reverse and relation lookups share one immutable transaction.
It pins one published live root and its source incarnation/revision, checks
optional epoch fences, and never falls back to obsolete derived claims or table
scans. Results contain active ownership and the physical table identity, with
query definitions captured and digest-checked only when requested. Reservations
are not lookup results. Missing/unpublished roots cannot attest name absence.
The storage-owner bridge validates peer attestation, result cardinality, active
phase and owner/definition identity; older peers cannot silently turn unsupported
resolution into a miss. Ordinary table requests/responses retain their prior
wire shape. Standalone relation requests use the durable authority as well.
Native coverage includes source-epoch/incarnation fences, root-following schema
updates, obsolete-registry misses, a 16 KiB identity allocation budget despite a
256 KiB table description, malformed peer responses and strict legacy wire
decoding. This remains infrastructure: PostgreSQL DROP INDEX lowering, scoped
authorization and publication bootstrap still need public SQL activation and
unchanged-original/native/oracle evidence before any corpus credit.
The final native relation-store gate passed 50/50 build steps and all 35 tests,
including the new contract round trip and strict legacy decoding. Inventory
integrity remains 475 implemented / 136 rejected / 73 superseded / 902 unresolved.
Standalone qualification is tracked separately; its running build is not
evidence of a completed standalone gate.
The linked standalone catalog gate for the qualified point-read cut completes
all 66 build steps: 172
tests pass and one is skipped. This includes the qualified relation-owner
contract and the production storage-provider linkage; it does not activate SQL
index mutations or replace the required transactional ownership fence.
Name-sensitive replacements now have a native transactional fence. The guarded
wire tag requires decoder capability 33 and carries the exact active owner,
authorized logical table and metadata incarnation. Apply resolves the current
namespace/root/binding in the replacing transaction; stale replicated proposals
are no-ops, while standalone admission reports the conflict without advancing
its revision. An unrelated table's source revision does not conflict. Ordinary
replacements retain their previous wire tag. Guard decoding is capped at 8 KiB,
rejects trailing bytes and uses owned parsing/cloning rather than leaking partial
JSON allocations. All 37 native relation-store tests pass, including rename
races, mixed activation floors, exact wire round trips, every truncation and
exhaustive allocation faults. The proposal-layer mixed-member gate completes
all 50 build steps and all 18 tests. Its probe-only HTTP fixture now asserts
activation at the highest unanimous decoder version rather than pinning an
older publication floor. Public index lowering, scoped SQL authorization/search-path binding,
bootstrap and exact admitted-result handling remain unfinished; this internal
fence receives no original-case credit.
The metadata services now also expose guarded replacement internally. Admission
performs one linearizable, definition-free relation point lookup and compares
the exact active owner, logical/physical binding and incarnation; unrelated
source revisions are intentionally not preconditions. The guard survives into
the proposed native command. Receipt observation retains the existing
non-retryable ambiguity semantics after admission and never tries to resolve
the removed index name after a successful drop. The service point-read and
guarded-receipt gate passes all 20 tests and 50 build steps. The context-aware
follow-up carries request deadlines through linearizable admission, capability
activation and receipt waiting, checks activity before proposal, and maps
post-admission cancellation/deadline failures to outcome-unknown. The final
context-aware cut passes all 20 tests and 50 build steps, including deadline
propagation and before/after-admission failure semantics. Public SQL transport
is not yet wired and no additional original cases are credited.
Guarded native apply also rechecks independently mutable extension ownership
inside the replacing transaction. It streams the owning table's indexed members
and parses changed index metadata once, rather than materializing the full
catalog. Shared admission preserves semantic JSON equality and propagates OOM;
the native gate passes all 38 tests and 50 steps, including extension acquisition
after name admission, replicated deterministic rejection, standalone revision
preservation and exhaustive allocation faults. This adds no corpus credit.
The private catalog envelope now carries guarded replacements under a body-bound
administrative grant. Hosted ingress returns an exact durable mutation stamp;
standalone preallocates its reply and verifies the committed postimage before
updating its projection. Extension ownership rejections remain definitive across
the private HTTP hop, while ambiguous outcomes cannot be reclassified as safe
rejections. Strict old-peer decoding and guard/identity wire tests are included.
The real-service test exposed a missing durable activation step that mock
readiness could not prove: guarded writes now install and observe capability 33
before user admission. Activation proofs retain their exact membership and term
through append under the runtime lock, and durable activation cannot decrease
within an incarnation. Later publication work retains that higher writer floor
while attesting its fixed publication decoder contract. All 23 coordinator tests
and 50 build steps pass for this final cut, including a real replacement receipt,
stale-membership rejection and a subsequent source rebuild. The native store's
38 tests also pass, including delayed lower-version activation rejection. The
standalone private-transport cut separately completes all 66 build steps, with
178 tests passing and one skipped. That gate includes private admission and
missing-capability rejection without durable state changes; it does not prove
standalone publication bootstrap or complete public SQL index activation.
Index DDL now has its own internal relation kind rather than masquerading as
an ALTER TABLE with a client-supplied owner. CREATE retains its owning table;
DROP retains qualified index names, multiple targets, dependency behavior and
CONCURRENTLY intent. The ordinary public path resolves names and definitions
from one immutable relation cut, authorizes the logical owning table before
object-type diagnostics, and carries the exact owner through metadata-local
schema preparation into native guarded CAS. It does not materialize a complete
catalog for an ordinary non-retiring index mutation. Pgwire carries its pinned
search path into that bounded lookup, with same-schema creation and qualified
names taking precedence. Malformed or stale mutation replies stay uncertain,
and schema-version exhaustion is checked before admission. Constraint-backed
indexes require removal through their owning constraint, with actionable
diagnostics. Retirement receipts retain the target epoch rather than the still
active predecessor. Fresh-catalog/standalone publication bootstrap, atomic
multi-owner DROP, online CONCURRENTLY builds and CASCADE dependency retirement
remain unfinished; these contracts are not implemented as blocking aliases or
independently committed loops. Qualification includes all 24 catalog/pgwire
owner-layer tests (47 build steps), 89 focused SQL tests with one skipped
(61 steps), all 23 real/pure coordinator tests, and 178 standalone catalog
tests with one skipped (66 steps). The standalone HTTP fixtures require local
listener permission; the authorized run passes after the sandbox-only bind
failures. A separate PostgreSQL 18 oracle checks same-schema creation, ordered
index shadowing and exact SQLSTATEs. These are foundation and target-semantics
witnesses, not original-case end-to-end coverage. No original-case dispositions
change.
The first standalone gate exposed missing storage-owner cleanup symbols in its
unlinked test composition. Its catalog tests now use a dedicated module linked
to the production ABI providers, without changing unrelated restore roots or
introducing test cleanup stubs; the linked rerun must complete independently.
Relation results now retain the owning table's logical name from the same
transaction as its physical identity and active claim. A published namespace
move or rename cannot silently turn a physical routing name into an authorization
name. Native tests cover qualified index lookup after moving a logical binding
without renaming storage, old-namespace misses, owned wire round trips and the
existing 16 KiB allocation bound. The native gate again passes all 35 tests.
This read contract is not a write-admission fence: SQL index activation still
needs owner/binding preconditions rechecked in the mutation transaction, not
only a preflight schema-version comparison.
The owned table-cut projector can now combine an exact predecessor definition
with a plan-fenced successor definition in expected linear time. It retains
old-only active names, both owners for shared names and pending-only new names,
including different physical IDs, namespace changes and index-kind changes.
It reuses the predecessor's arena instead of copying its names into a third
cut, and bounds the distinct-name union at 8192 rather than rejecting two
full but overlapping schemas. Pure tests cover that boundary, input retirement
and exhaustive allocation failures; reconciliation's compound-candidate test
now uses actual schema-derived projected claims.
Pending ADD/DROP-FK generation plans now participate in native reconciliation.
The pinned source reads the durable child plan alongside its binding and table,
compares the exact predecessor physical name/schema, and projects both cuts
before parsing relation names. This adds one plan point read per source table,
without an extra table read or duplicate predecessor schema parse. Replica-side
effect verification indexes complete projected entries once per affected table,
including pending-only names, rather than trusting candidate fingerprints.
Source epochs and equal-clock snapshot/replay comparisons include plan identity,
child definitions and reservation visibility, but exclude receipt arrays,
revision counters, placement and progress-only phases. Reservations survive
canceling until canceled, and disappear when metadata publishes the child (not
only after installation acknowledgements). Native coverage exercises real FK
begin/receipt transitions, candidate forgery, same-clock plan substitution,
bounded receipt-skipping and scratch reuse/allocation faults.
Hidden FK-bearing CREATE plans also participate through their existing active
work index, not a scan of terminal publication history. The lexical source
cursor consumes that range before public table rows, retaining bounded pages,
exclusive resume and lookahead rewind across the range boundary. Each hidden
cut verifies the exact work/plan ID, namespace hierarchy, absent public table
and binding, and ownership of both logical and physical name reservations.
Its names remain pending-only through `published_hidden` and cancellation;
the final publish/cancel transaction retires the active-work entry and advances
the source epoch. Plan/name/work effects are included in replay and equal-clock
snapshot source comparisons, while receipt progress and support-seal changes
that do not change the child's name/schema cut do not force a rebuild.
Native tests check 64-row cuts and document-table handoff, backward retries,
unchanged preparation allocations after 1000 terminal plans, real hidden
CREATE authority/replay checks, reservation forgery, exhaustive allocation
faults, and cancellation retirement across restart. Projection validates the
plan identity separately from semantic hashing so schema bytes are not hashed
again merely to select the source cut. Restore-plan sources remain unfinished,
as do producer reservation admission and serving/publication capability barriers.
Multi-peer coordinator failover/fault coverage remains to be extended alongside
serving activation. Pending-generation reservation sources,
capability barriers and atomic active-root publication still
precede writer adoption and SQL point resolution. No original SQL case is
credited for this protocol component.
FK publication and restore command envelopes use the same admission boundary,
but pending-generation name reservations, initial adoption/migration and full
distributed publication fault coverage still require
activation work. No public unqualified index DDL is enabled.
Rebuild/verification from authoritative bindings and table definitions, serving
capability barriers, authorized point resolution, and DROP/REINDEX integration
remain required before unqualified index DDL is enabled. The registry must not
become a second independently committed catalog, nor may a table scan replace
the missing point-resolution path. No original case is credited for this
component work; current counts are reported at the top of this inventory.

The data-owner deterministic rejection enum includes PostgreSQL array-subscript
failures alongside the shared SQL expression error contract. The focused
`zig build antfly-sql-expression-apply-contract-test` gate exhaustively verifies
exact round trips for that contract and excludes resource/corruption failures;
these failures must not become indefinitely retried committed entries. This is
runtime contract coverage, not additional original-case completion credit.


## Sequence effects and document generation retirement

Closed `sql-1103` as a tested PostgreSQL supersession of its old document-DDL
rejection. The exact SQL uses the existing asynchronous empty-generation
publication job, with authenticated denial, complete empty/future-write
postimages, a fresh table identity and independently reopened owners. The test
never treats a pending receipt as completed TRUNCATE.

The same native cohort qualifies sequence expression demands, durable HTTP
issued values, pgwire connection-owned state and prepared resources, compact
cache leases, SETVAL/DISCARD semantics, implicit MERGE scope and sorted output
ordering. Lost acknowledgements are observed with their original nonce;
unknown or forged receipts cannot allocate a replacement range. The oracle
compares MERGE assignments as a set because matched-row evaluation order is
not specified, while still requiring distinct predicate/assignment values.

`sql-0318`–`sql-0320` stay unresolved. Their PostgreSQL serial/identity fixtures
now exist, but atomic table/sequence/default publication, shared typed-row
identity enforcement and transactional owned-counter handoff remain the next
whole-case boundary. These component and oracle results do not change those
three dispositions.


## Atomic serial and identity column lifecycles

Closed `sql-0318`, `sql-0319` and `sql-0320` against their unchanged originals
and an independent PostgreSQL 18 oracle. CREATE publishes the physical table,
immutable default binding, stable attribute IDs, sequence descriptor, counter
and ownership edge in one native metadata transaction. The mounted Debug gate
runs all three declarations through complete write postimages, DEFAULT and
explicit NULL handling, OVERRIDING, CHECK failure, rollback, schema rewrite,
CONTINUE/RESTART IDENTITY, real native checkpoint installation and exact
allocation-receipt replay, independently cold owners and owned-sequence cleanup.
Constraint retirement must reach its existing public `ready_to_drop` barrier
before DROP; a pending receipt or schema publication is never completion proof.

SQL and public batch/typed-row preparation share the native default provider.
An identity table writer needs namespace usage and table write authority;
explicit NEXTVAL and serial defaults retain sequence privilege checks. Public
bulk mutations accept explicit ALWAYS values like PostgreSQL COPY, while SQL
INSERT, UPDATE, ON CONFLICT and MERGE enforce ALWAYS restrictions. The provider
pins one normalization view per SQL statement and keeps cached allocation leases
on the existing sequence owner; counter effects remain outside scalar bytecode
and are never repeated during Raft row application or restore validation.

Native checkpoint recovery preserves the sequence incarnation, ownership,
counters and durable outcomes. An activated empty sequence outcome journal is
encoded as head zero, distinct from an absent journal. Snapshot source proofs
separately decode logical JSON bindings and physical binary table records;
independently versioned sequence/view records do not alter the physical source
cut. The catalog-store regression gate passed all 51 tests.

Initial CREATE with both owned sequence intents and foreign keys remains
unsupported by the initial-FK publication owner and is rejected before admission.
Table archives without the metadata counter/incarnation proof also fail closed;
this cohort qualifies full native metadata checkpoints, not counter-aware
cross-database table imports. ALTER ADD COLUMN sequence backfill remains guarded.
These limits do not change unrelated original-case dispositions.

The focused owned-default Debug gate passed both parser and shared-preparation
contracts. Sequence-effects (five tests), document TRUNCATE (one lifecycle),
PostgreSQL fixture verification, public API generation, formatting, generated
Python client consistency, license and Apache-boundary checks passed. The broad
SQL run was stopped during unchanged spill allocation-failure coverage and is
not claimed as a completed full-suite pass. The ledger now records 791
implemented, 137 rejected, 433 superseded and 225 unresolved original cases.


## Declared document aliases through native owners

Closed `sql-0917`, `sql-1082` and `sql-1088` with their unchanged originals and
source-owned schemas. Strict public schema admission now recognizes
`x-antfly-column-name`; duplicate/reserved names, ambiguous cross-type paths and
nonfixed placements reject before metadata publication. Compiled path segments
retain literal dots and slashes. Roots remain directly accessible, while aliases
remain logical SQL columns rather than extra stored document members.

The same bindings serve schema caches, native document scans, typed projection,
search-hit hydration and copy-on-write assignments. Native requests deduplicate
physical roots. Native schema epochs own the declared column vector and its name
directory; retained read sessions borrow both instead of rebuilding the schema
for each scan. Projection preparation uses that directory for selected fields,
including an empty condition projection, so its temporary work no longer scales
with unrelated declared columns. Path resolution performs no per-row
parsing/allocation. Cache
and projection owners clone their path bindings; changed-spine assignment leaves
captured snapshots intact and unwinds partial allocations on failure. Overlapping
ancestor/descendant writes reject before execution. Unknown raw fields sharing an
alias's spelling survive mutation and null normalization.

An independent PostgreSQL updatable JSONB-view oracle qualifies the original
mutations, complete document postimages, absent values and writes after restart.
Mounted metadata/data owners verify actual permissions, mandatory physical row
filters composed with alias predicates, native commit postimages and cold reads
and mutations. Component coverage exercises ordinary and native search-hit
projection, allocation failures, strict public admission and path collisions.
This does not activate generated document fields or indexed full-text reads
through views. Those remain unqualified; legacy index-metadata annotations are
never proof of native readiness, uniqueness or authorization.


The Debug preparation benchmark compared eight one-column projections with
rebuilding declarations per scan versus borrowing the retained epoch layout:

| Declared columns | Rebuilt allocations / bytes | Epoch allocations / bytes |
| --- | --- | --- |
| 64 | 600 / 487,200 | 24 / 2,128 |
| 512 | 4,200 / 3,211,232 | 24 / 2,128 |

These are component allocation totals, excluding epoch construction, not
end-to-end SQL latency or throughput. Both paths verify the requested column;
the scaling gate requires borrowed-layout work to stay bounded as unrelated
schema width grows. The document Debug gate passed 26 contracts with no leaks.

## Stored document producers through native preparation

Migrated the producer contracts for `sql-0910`, `sql-0920`, `sql-1072`,
`sql-1074` and `sql-1079` to public `generated_columns`. Their unchanged original
SQL executes against a real native producer or returns PostgreSQL's `428C9`
generated-column assignment diagnostic. These are recorded as **superseded**,
not exact source-schema implementations: the witness retains the old source
schema and explicitly translates its inert property metadata to the supported
root expression contract, including enforced types and C/ASCII case operations.
No historical metadata is silently promoted into producer authority.

The same compiled immutable expression engine now serves document and relational
rows. Document preparation owns the parsed roots and encoded logical results in
one batch region, reuses validation scratch, and runs before the apply lock.
Extraction, graph/vector preparation, grouping and immediate text consumers reuse
those roots; schema and index publication are fenced before commit. Durable
intents retain their historical schema identity. Artifact-only commands preserve
the base row even with constant producers; SQL virtual and DB-owned directive
names cannot be generated targets. Compiled dependency masks also reject
nonpersisted transport/virtual inputs at admission, so restored rows retain all
producer inputs. Unused declared directives and persisted `_type` remain allowed.
Derived SQL NULL removes its member, while JSON null remains a present datum. SQL roots
and aliases are output-only. Whole-document writes recompute supplied results.
Native and public schemas carry reader capability 38 so older owners cannot
silently omit producers. Ordinary metadata updates reject producer changes before
publication. Document producer schema rewrites remain unsupported and are not
claimed by this cohort.

Portable import uses its existing manifest/staging validation path to verify
stored generated values without repair, including compressed documents. It
rejects forged/missing values and documents streamed before producer authority.
Historical document bytes remain unchanged; later unrelated JSON Schema
constraints govern new writes and are not retroactively applied during restore.
Empty generated declarations retain ordinary document admission and capabilities.
Allocation-failure coverage checks owned preparation and NULL provenance. The
mounted campaign compares PostgreSQL complete postimages, denied writes, atomic
invalid batches, generated-field row filters, independently reopened owners and
cold `UPDATE ... RETURNING` results. A real LSM portable export/import compares
complete restored bytes and resumes generated writes after another reopen.
Restore activation now prepares public validators for both storage modes, matching
cold startup instead of publishing a document layout without its producers.
The native campaign and preceding alias
campaign share the existing integration DAG's compiled owner artifact and run
once each in ordinary CI.

A Debug component comparison prepares the same rows and values either by
normalizing to bytes and reparsing for extraction or by retaining the owned parse:

| Rows | Reparsed allocations / bytes | Shared allocations / bytes |
| --- | --- | --- |
| 32 | 194 / 102,414 | 35 / 95,438 |
| 256 | 1,537 / 816,394 | 260 / 384,626 |

The totals exclude schema compilation and verify both values and complete
allocation reclamation. These measure preparation and extraction allocation
work, not end-to-end query latency. The ledger now records 794 implemented,
137 rejected, 438 superseded and 217 unresolved original cases.


## Typed timestamp bins and transaction dates

Closed the native temporal cohort `sql-0179`, `sql-0264`, `sql-0265`,
`sql-0266`, `sql-1467` and `sql-1477` against PostgreSQL's actual type contracts.
`CURRENT_DATE` is a transaction-stable DATE (OID 1082), including its ordinary
DATE-to-TIMESTAMPTZ conflict assignment. Its value borrows the invocation frame;
prepared plans never capture a wall date. `date_bin` selects the timestamp or
timestamptz overload once, binds exact interval/timestamp parameter identities,
and uses checked signed microsecond arithmetic with floor boundaries, explicit
infinities, strict NULLs and PostgreSQL error ordering. Native typed rows stay on
the direct value path; bound logical JSON timestamp ingress shares the existing
normalization boundary. UTC date/timestamp casts retain typed values and infinities.
Timestamp input accepts validated numeric offsets and bare interval seconds;
fractional input rounds to PostgreSQL microseconds with even half ties and day
carry, while bin arithmetic remains integral.
canonical persisted temporal spelling remains strict.

The independent PostgreSQL matrix covers 650 bin, overload, input and cast
contracts, including BC dates, extreme dates, signed origins, invalid strides,
NULLs and infinities. Debug gates cover ownership under allocation failures,
lazy branches, stable prepared parameter/output descriptors, and repeated
transaction-date evaluation with an allocator that rejects every allocation.
A mounted native campaign executes all six unchanged originals and compares
complete rows, SQL NULLs, output type identities and table postimages. It then
reopens metadata/data/session owners, resumes typed-bin and conflict writes,
and verifies filtered/sorted timestamp-bin reads.

Three originals are successful implementations. Three historical success
contracts are explicitly superseded by verified PostgreSQL diagnostics. The two numeric-epoch bin
statements receive PostgreSQL 42883, and the unqualified conflict timestamp
reference receives PostgreSQL 42702. These are precise invalid SQL contracts,
not unsupported-feature waivers; the campaign separately exercises positive typed
bins and qualified calendar assignments. No legacy epoch-number overload or
alternate statement replaces an original. The current timezone capability is
UTC; arbitrary session timezones and full native storage of every temporal type
remain separate work. Counts are 797 implemented, 137 rejected, 441 superseded,
and 211 unresolved originals.

That batch left a separate catalog gap: durable view outputs in
`sql/view_definition.zig` retain the coarse datetime family without its temporal
kind, and the view output/dependency signatures need that exact domain carried
through publication, replacement, restoration and rebinding. These statement
and conflict gates do not qualify stored views over non-timestamptz temporal
outputs. That requires a complete descriptor/capability migration rather than
relabeling DATE, TIME or TIMESTAMP as TIMESTAMPTZ.


## Exact view domains and immutable document envelopes

Stored view outputs now retain DATE, TIME, TIMETZ, TIMESTAMP and INTERVAL
identities through one shared output-to-column projection, including nested
view preparation and execution. Explicit TIMESTAMPTZ normalizes to the historical
implicit descriptor. New typed definitions use format 2 and protocol capability
71; ordinary format-1 definitions, canonical bytes and domain signatures remain
unchanged. All new view mutations require the coordinated writer floor. Reads
and snapshot validation require the descriptor-specific floor, so historical
records remain restorable while newer descriptors reject downgraded owners.
An older ambiguous non-timestamptz view cannot acquire a missing type by guessing:
its expanded type fails the dependency-domain check and it must be recreated.
Replacement compares exact domains and preserves stable output attribute IDs
only for compatible replacements. Authenticated catalog forwarding retains
precise replacement, dependency and cycle diagnostics only after an exact
not-proposed outcome; unknown and committed responses retain mutation ambiguity
regardless of a matching error body. Source rename and additive schema evolution
retain captured identity and output shape.

Domain signatures serialize their bounded enum/numeric descriptor into a
512-byte stack buffer. A wide dependency test computes 2,048 signatures with
heap allocation disabled. This removes one JSON allocation/free per signature;
it is an allocation result, not an end-to-end latency claim. Frozen historical
hash vectors protect compatibility. Allocation-failure enumeration disables
in-place backing reallocations so heap placement cannot change failure counts.

The original rejection `sql-1073` now has an explicit engine-owned identity
invariant shared by ordinary UPDATE, joined UPDATE, conflict UPDATE and MERGE
UPDATE. It returns 428C9 before durable mutation; INSERT still accepts a document
key and whole-document updates remain supported. The independent PostgreSQL
witness models Antfly's immutable envelope with a generated `_id` column and
runs the unchanged original statement. This is an explicit Antfly envelope
mapping, not a claim that ordinary PostgreSQL user columns named `_id` are
immutable. The mounted witness retains the exact original source schema,
checks denied-reader precedence, compares complete documents and native
versions after each rejected mutation, and reopens the owners before repeating
the rejection and committing a positive whole-document update.

Qualification includes native typed output metadata and PostgreSQL values,
incompatible/compatible replacement, nested rename, source-column additions,
owner reopen, durable metadata snapshot installation, capability downgrade
rejection, reverse-edge corruption, and committed-log loss/duplicate delivery.
This view-domain repair qualifies a catalog invariant; it does not manufacture
additional original case closures or claim native storage of every temporal
type or arbitrary session timezone support.

The ledger now records 797 implemented, 138 rejected, 441 superseded and
210 unresolved originals. Only `sql-1073` changes disposition in this cohort.


## Bounded ordinary ordered aggregates

Ordinary aggregate ORDER BY now binds typed ordering programs alongside its
arguments, preserving direction and NULL placement. Native `string_agg` and
`array_agg` use one bounded external stream per aggregate across all groups,
sharing the query's existing spill manager, cancellation and disk quotas.
Distinctness compares the entire value-argument tuple, including a string
aggregate's delimiter, and retains SQL NULL separately from JSON null.
Scalar arrays retain their declared element identity; arrays of arrays retain
matching dimensions and lower bounds, reject NULL/empty/incompatible inputs,
and reject a seventh dimension with PostgreSQL 54000. These contracts use the
existing native array element domains; they do not claim additional codecs for
every PostgreSQL type or ordered window aggregates.

Sort keys own argument values whenever possible, avoiding a second wide payload
copy. FILTER selects lanes before argument evaluation. Immutable collection
arguments are prepared once with bounded work and retained constants; stable
and volatile expressions are not folded. Native projected columns stay in
column batches for immutable programs. Volatile FILTER/argument interactions
use resettable typed row scratch to preserve per-row invocation order, without
reconstructing JSON. Repeated value argument occurrences remain separate;
ORDER BY references reuse their matching argument. Ordinary reducers and
ordered-set aggregates can share the same grouped query. Native partial-state
and materialized shortcuts reject collection recipes rather than interpreting
their internal count markers as user-visible reducer states. Group keys and
extrema preserve temporal and numeric domains. The shared reducer finalizer
returns consumer-owned values, including numeric transitions and extrema,
which remain valid after reducer destruction.

The PostgreSQL 18 oracle verifies 212 contracts with exact output OIDs, complete
values and SQL NULL flags. Mounted native owners pass all 212 both before and
after restart, with stored-view reads, restricted-reader row filters, cold
mutations and complete table postimages. Eight Debug component tests pass with
no skips or leaks, including forced spill, disk-quota rejection, cancellation,
volatile argument identity, native vector lanes, retained output lifetimes and
allocation-failure enumeration. A discovered unordered mixed-aggregate witness
was plan-dependent even within PostgreSQL: its presorted-aggregate optimization
introduced another input sort. The deterministic witness uses explicit
aggregate ordering; a separate sorted-derived-input contract remains covered.

A 512-row component benchmark with compression disabled compares identical
result hashes while duplicating or sharing wide sort-key payloads. At 128 bytes
per value, written spill bytes fall from 1,007,688 to 345,312 (66%). At 1,024
bytes, they fall from 4,481,179 to 3,737,104 (17%). Preparation and ranking are
excluded; these are spill-byte measurements, not end-to-end latency claims.

Only original `sql-1230` changes disposition. Its unchanged DISTINCT status /
ORDER BY created_at statement is invalid PostgreSQL and receives precise 42P10;
its historical success contract is therefore superseded. The new positive
contracts qualify the feature rather than manufacturing original case IDs.
Counts are 797 implemented, 138 rejected, 442 superseded and 209 unresolved.


Broader Debug qualification passes 63 aggregate and reducer regression tests.
The evidence checker also now isolates compile-time and historical runtime
filter groups in both arrival orders, and its inherited temporal-key range gate
uses a real compile filter. All 21 checker tests pass; the recorded collection
and corrected range gates run together as 25 passing Debug tests. This changes
no additional original disposition.


## Bounded measured EXPLAIN reads

SELECT ANALYZE now uses a typed execution sink that evaluates every lazy cell
and discards its payload in resettable scratch. It measures the complete query
independently of the response row limit, while retaining normal scan, work,
memory, spill, cancellation and authorization admission. The same owned typed
parameter frame serves immutable preparation, execution, cost capture and plan
rendering; sequence effects execute exactly once. Planning never invokes stable
or volatile functions.

PostgreSQL option defaults, Boolean grammar, dependencies and duplicate-option
precedence are qualified against 631 independent contracts. TIMING OFF removes
per-pull clocks independently of SUMMARY. Native plan JSON remains an explicit
Antfly logical schema. Coordinator memory and temporary spill bytes have stated
units; provider I/O/cache and journal/WAL counters remain unknown, rather than
invented zeroes. SETTINGS discloses the authorized settings epoch and effective
execution limits without arbitrary setting values, identities or policy secrets.
Mutation ANALYZE and XML/YAML output remain unsupported.

Normal execution, streamed reads and EXPLAIN share a bounded immutable program
walk using actual request parameters. Provably disabled Apply subplans are not
prepared. Derived aggregate liveness is specialized during immutable binding,
removing unused pure reducers, ordered streams and scan fields while retaining
GROUP BY, HAVING, ordering, live correlated invocation constants (including
empty-input global aggregates) and observable effects. PostgreSQL sequence probes
and native regressions verify that unused volatile aggregates still execute.
The broader regression also exposed and corrected NUMERIC extrema modifiers,
implicit timestamp frame normalization and a legacy JSON-only date_trunc path.
Native UTC truncation now preserves pre-epoch, distant, BC and infinite values;
60 exact PostgreSQL value witnesses qualify it through the public API before and
after a cold owner restart. Unsupported timestamp predicate operands retain the
typed residual rather than becoming SQL NULL.

The no-skips mounted Debug campaign executes both unchanged original statements
(sql-0070 and sql-0071), all 631 option/execution contracts, complete table
postimages, mandatory RLS probes and cold-owner replay. Both originals describe
read execution and runtime option metadata; these closures do not claim complete
provider telemetry. The stale sql-1409 syntax rejection is superseded by the
unchanged PostgreSQL OVERRIDING SYSTEM VALUE write, mounted RETURNING and durable
complete rows. A 2,048-row wide-projection component benchmark reduces tracked
peak retained memory for query execution from 11,774,438 to 430,496 bytes (96.3%). Equal
cardinality and demanded scalar failures are checked. This is a retained-memory
measurement, not a latency or storage-throughput claim.

The ledger records 799 implemented, 137 rejected, 443 superseded and
207 unresolved originals. Only sql-0070, sql-0071 and the obsolete sql-1409
rejection change disposition. Four opt-in component benchmarks remain skipped
in the broad Debug suite; the EXPLAIN memory benchmark and both mounted lifecycle
campaigns execute without skips.

### Document admission and assignment-domain integrity

This cohort closes sql-0909 and sql-0941 against their unchanged originals.
The former is an invalid numeric-property patch; the latter assigns a subfield
of a JSONB column, which PostgreSQL does not treat as a composite. Both receive
specific data/type errors rather than generic unsupported-shape waivers.
The ledger moves from 816 implemented, 137 rejected, 443 superseded and 190
unresolved to 816 implemented, 138 rejected, 444 superseded and 188 unresolved.
No source SQL, parameter, schema domain or inventory expectation changes.

The mounted regression exposed a broader native defect: an ordinary document
schema could bypass preparation when REST trusted the owner's atomic-preparation
capability. All declared document schemas now use the existing pinned preparation
pipeline, with their parsed roots reused by extraction. Transaction preparation
owns normalized logical documents until the journal copies them; every row
validates before intent admission. LSM regressions verify atomic invalid-batch
rejection, absence of intents after a failed prepare, cold prepared-row commit,
and allocation-failure cleanup. Unconstrained native JSON tables retain their
streaming fast path, including scalar/array values and cold intent recovery;
malformed unconstrained values reject before durable intent admission. A streaming
work witness validates 4,096 array elements (8,193 bytes) with no available heap
allocator; deep nesting retains its bounded allocator fallback. Empty and closing
delimiter inputs reject safely. Speculative requests recheck their pinned schema
at admission and after coalescer work, including schemaless/unconstrained epochs.
Durable ordered writes and prepared participants retain their admitted historical
schema through publication and restart; ordinary stale requests cannot select it.
Historical generated-document portable restore retains its existing semantics.

Mutation target binding is separate from read-column qualification. INSERT,
UPDATE (including tuples), ON CONFLICT, and both MERGE assignment forms retain
field selectors through bounded linear parsing. A missing root receives 42703;
a present non-composite root receives 42804. A target alias cannot be stripped
into a writable leaf column. Quoted literal-dot columns and ordinary qualified
reads remain valid. Binding allocates no memory and ordinary INSERT targets
avoid a second schema lookup.

The independent PostgreSQL oracle executes both originals, verifies complete
unchanged postimages and native PostgreSQL indirection diagnostics, and names
Antfly's patch and document-schema validation extensions explicitly. Authenticated
native owners verify physical versions and digests, failed SQL/REST batches,
read-only denial before schema details leak, and metadata/data-owner restart.
These closures do not claim user-defined composite types or obsolete private
view-mapping activation.


### Typed mutation admission and borrowed temporal domains

This cohort closes sql-0201 and sql-0592 against their unchanged originals.
The first requires a BIGINT pgwire parameter, independently checked as OID 20;
TIMESTAMPTZ >= BIGINT has no PostgreSQL operator. The second uses the canonical
read profile's TEXT created_at field, which cannot add an INTERVAL. Both receive
42883 through real authenticated native owners before and after restart, with
complete unchanged images. Context-inferred HTTP parameters are not substituted
for the typed pgwire contract. The ledger moves from 816 implemented, 138 rejected,
444 superseded and 188 unresolved to 816 implemented, 138 rejected, 446 superseded
and 186 unresolved.

Source inspection found different declarations on sql-1403, sql-1468 and sql-1469:
UUID/NUMERIC or UUID/TIMESTAMPTZ, with private-era setup and placeholder values.
Those IDs remain unresolved. Their SQL also supplies explicitly named supplemental
TEXT/BIGINT witnesses; those independent domains earn no source-case credit.
One exposed an incompatible conflict assignment escaping preparation and failing
inside physical encoding. All scalar mutation programs now share assignment-family
admission, and INSERT source mismatches report the same assignment diagnostic.
Typed TEXT cannot silently acquire a numeric domain; an explicit cast still works.

The wider execution audit found precise temporal identity being discarded by
conflict captures, joined/MERGE scopes, RETURNING metadata and mapped batch coercion.
Those boundaries now carry the complete descriptor and use typed coercion. Borrowed
DATE, TIME, TIMETZ, TIMESTAMP, TIMESTAMPTZ and INTERVAL cells retain their identity
through selection/projection with no available heap allocator. Wrong domains
reject without consuming an invalid unselected row. Parameter frames reject
cross-kind substitution, including typed SQL NULLs, while the default timestamptz
alias remains valid.

The PostgreSQL oracle covers 56 assignment combinations. Native component tests
cover those combinations, all 36 temporal frame pairings and both mapped/column
borrowing paths. Mounted cold probes verify valid casts, calendar arithmetic,
target/excluded upserts and mixed temporal joined UPDATE/MERGE RETURNING, including
column OIDs and full postimages. All rejecting cases share one cluster lifecycle;
each still executes and verifies its unchanged images before and after the same
owner restart. The focused gate avoids rerunning unrelated SQL fault campaigns.
Durable DATE/TIME column-layout activation remains the separate typed-value boundary.
