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
