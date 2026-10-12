# SQL extraction: local validation checkpoints and initial slice

> Relocated verbatim from `docs/sql-extraction.md` (lines 1914–2130, 2155–2225, 2281–2336) at commit 3ce5aff4ea on 2026-10-12. Durable decisions were folded into [SQL extraction](../extraction.md) before the move. This is a historical implementation log; it describes work at the time it was performed, and its counts and test totals are not current guarantees.

### Latest local validation

- Joined-mutation/recursion follow-up: 195 SQL tests pass in Debug and
  ReleaseSafe, including allocation failures, target ambiguity, DELETE fanout,
  CTE target shadowing, recursive cycle/null semantics and shared physical
  captures. With `ANTFLY_SQL_JOINED_MUTATION_BENCHMARK=1`, the 4,096-row
  ReleaseSafe joined UPDATE fixture reads 8,192 native rows in one capture,
  performs 32,797 checkpoints, peaks at 9,357,062 query bytes, and takes about
  19–22 ms locally with the production allocator and an explicit 64 MiB budget.
  This is executor work-count/allocation evidence, not distributed write latency.
  Pgwire passes 43 tests; the broader native SQL API run passes 159 tests and
  relational row/schema contracts pass 50 tests. The real staged-worker fault
  fixture passes for Raft and native empty-generation owners with lost replies
  and reopen after begin/validate/publication; targets remain empty, donors
  retain their old data, and no source import or tail is executed. The final
  native transaction suite passes all 101 tests in Debug with no leaks. The
  prepared HTTP/native rerun passes all five selected tests after correcting
  generated-router handler signatures; the full API fault-test build also
  verifies that router integration. Generation, formatting, OpenAPI checks and
  diff whitespace checks pass locally. These checks do not establish full
  repository or CI parity.
- Follow-up session and TRUNCATE checks: the pgwire suite passes 47 tests,
  including scoped `application_name` and complete-statement savepoint parsing.
  The three native TRUNCATE API tests pass, including RESTART IDENTITY admission
  with the current sequence-free catalog. The full graph and external-parent
  FK cutover remains guarded.
- Shape-aware subquery follow-up: 180 Debug and ReleaseSafe tests pass, including quantified
  comparison truth tables, computed correlation keys, composed aggregates,
  independently bound complete value relations, and physical cold-field pruning.
  The 10,000-row ReleaseSafe benchmark with the production allocator measured
  approximately 49 ms for membership (three scans, one capture, 8.43 MB query
  peak) and 26–27 ms for ordered ANY/EXISTS (two scans, one capture, 7.27 MB).
  These synthetic local measurements are not distributed latency claims and
  are not comparable to older timings using the leak-checking allocator.
  Routine tests retain leak detection; the opt-in workload uses the production
  allocator to avoid measuring debug allocation quarantine.
  A final 10,000-row rerun measured 57 ms membership, 30–31 ms ordered
  subqueries, and 20 ms uncorrelated EXISTS. The latter fetched 256 inner rows
  rather than all 10,000, with all 10,000 outer rows preserved; the native
  projection excludes discarded cold fields. These are separate workload
  shapes, not an apples-to-apples speedup comparison.
- Current integration checks: 154 SQL-filtered API tests, 50 public relational
  row/schema contracts, 30 pgwire tests, 382 TypeScript SDK tests (one skipped),
  157 Antfarm tests, 22 Python SQL tests and all Go SDK tests pass. Prepared
  resources have five focused HTTP/native tests and five Zig client tests.
  The final ReleaseSafe native run passes 98 transaction and 10 Lite SQL tests,
  including expression/partial arbitration, activation and retirement, with
  zero leaks. Ordinary column-only constraint fingerprints retain their fast
  path; typed layout/VM construction is lazy for expression/partial shapes.
  `make generate`, `make fmt` and the Zig OpenAPI freshness check pass.
- The original 1,586-case SQL extraction corpus now has a provenance-pinned inventory and
  exhaustive disposition ledger. `make sql-parity-inventory-check` verifies its
  integrity. `make sql-parity-release-check` intentionally blocks until each
  source case has executable evidence or a justified exclusion; creating the
  inventory alone is not parity completion. See [the inventory guide](../parity-inventory.md).
- Seven original TRUNCATE cases now have SQL-neutral IDs, explicit supersession
  rationale, exact-statement admission tests, and real empty-generation owner
  publication/restart evidence. `make sql-parity-evidence-check` runs referenced
  gates before the full corpus is resolved: the focused public API TRUNCATE
  suite passes eight tests without leaks and the staged-owner rewrite/empty-
  generation driver passes two. The remaining 1,381 case dispositions still
  block release.
  The graph-index guard now inspects only selected tables after FK closure,
  so an unrelated graph table neither blocks admission nor incurs index-JSON
  parsing; a graph-indexed CASCADE participant remains guarded. Unknown target
  names fail before the dependency walk parses unrelated table schemas.
- All 21 currently referenced `make sql-parity-evidence-check` gates pass,
  including partial evidence on unresolved original prepared CTE mutations;
  partial evidence does not grant release credit.
  The evidence runner combines compatible Zig test filters for the same target
  into seven build invocations, preserving all 21 gate identities and their
  bounded aggregate timeouts. The grouped run passed all selected tests in
  5m54s on this machine with a cold API test rebuild; this is not a controlled
  before/after speedup measurement.
  The current SQL suite passes 227 tests, including the VALUES-subquery and
  observed-group Top-K admission cases.
  The dry-run EXPLAIN and mutation-subquery follow-up passes exact mounted
  text/JSON cases and a write-permission denial test; its test backend
  rejects any accidental row scan or mutation. `make fmt-check` and the
  parity-checker unit suite also pass.
  The inventory validator now prevents a behavior-required original case from
  being marked `rejected`, or an original rejection from being marked
  `implemented` without a tested `superseded` disposition. This protects the
  release gate from status-only waivers; the 1,381 unresolved cases remain
  blocking, and these focused evidence gates do not prove full distributed
  or workload parity.
- The routed table-read suite passes 81 consumer tests, including an exact
  secondary-index proof across two owners and fail-closed cleanup when one
  owner cannot provide the proof, provides a different span identity, or
  returns a malformed proof tag. Its
  18 implementation tests also pass.
- Top-K replacement now reuses the displaced root's bounded arena. A local
  10,000-row/k=5 all-competitive churn test kept its 1,299-byte allocation
  peak and 44,987 comparisons while dropping Debug elapsed time from about
  766 ms to 8 ms; the ReleaseSafe run took about 0.85 ms. The full SQL suite
  passes 207 tests, including allocation-failure, quota and guarded MERGE
  compiler checks. Joined mutations reuse the once-authorized target schema
  when binding read relations; the shared resolver refuses write/admin calls
  and still resolves other physical sources with read authority.
- Pgwire accepts the negotiated UTF-8 `client_encoding` through connection-owned
  SET/LOCAL/SHOW/RESET and rejects non-UTF-8 changes. The exact original
  `sql-0778` statement has mounted simple/extended wire evidence; the pgwire
  suite passes 54 tests. Eight original cases are superseded, eight original
  invalid-shape cases have exact-SQL rejection evidence. The original
  `sql-0019` computed descending-order `LIMIT 5` query now has mounted HTTP
  execution evidence over seven native typed rows, including exact large-
  integer output. The exact `sql-0020`, `sql-1254`, and `sql-1255` grouped
  reads now have mounted native-backed HTTP evidence for expression grouping,
  output aliases in `GROUP BY`/`HAVING`, and mixed-case aggregate counts.
  Seven exact CTE aggregate/read cases now have mounted native-backed evidence,
  including null-safe JSON/status filters, missing values, CTE column aliases,
  chained filters, and ordered output. Four matching materialization-hint and
  classifier cases now have exact endpoint evidence plus repeated-reference
  work-count coverage. Eighteen exact scalar-subquery, existence and
  quantified/membership cases now have mounted endpoint evidence. Seven
  equality/ordered correlated cases also pass exact mounted output/cardinality
  checks. Ten exact quantified LIKE/ILIKE cases now have mounted endpoint,
  wildcard/NULL truth-table and quota evidence. Five OR-correlated EXISTS cases
  have exact mounted evidence plus nested/NULL/NOT EXISTS unit coverage. Nine
  compound, nested, and CTE-contained subquery cases also have exact mounted
  output evidence. Two exact no-op MERGE cases have guarded mounted-commit
  evidence. One exact matched-DELETE MERGE also has a guarded mounted-commit
  check. Four other exact MERGE cases additionally have case-specific source
  conflict and ambiguous-commit evidence. Source-only INSERT RETURNING has
  the same mounted proof and fault coverage. Expression and grouped predicate
  arms and a read-only CTE-backed MERGE have exact mounted coverage. A linked
  hosted CTE self-MERGE now prepares and executes the exact `sql-0008` body
  through authenticated HTTP, proving real Raft-owner statement-fence capture,
  guarded prepare/commit and row read-back. The linked hosted test also runs
  exact pgwire PREPARE/EXECUTE against that owner, verifies the `MERGE 1`
  completion and typed row, and distinguishes precommit read unavailability
  from an ambiguous write outcome. The same hosted fixture now mounts the exact
  `sql-0005` pgwire cross-table CTE INSERT and verifies `INSERT 0 1` plus typed
  target read-back. The exact `sql-0009` recursive prepared read emits three
  rows from a parent/child worklist, and `sql-0010` updates the two distinct
  targets through the same hosted owner path. Multi-owner and distributed fault
  evidence remain open;
  1,381 cases remain unresolved.

- Follow-up ReleaseSafe SQL suite: 168 tests. Membership benchmark (opt-in
  `ANTFLY_SQL_MEMBERSHIP_BENCHMARK=1`): 10,000 outer and 10,000 inner rows,
  three scans, one coordinated capture, 8.43 MB query peak, about 1.59 seconds.
  The routine fixture is smaller and still crosses multiple native pages. The
  OR-correlated EXISTS path on the same opt-in 10,000-row fixture used three
  scans, one capture, 20,256 native rows (the uncorrelated witness stops after
  its first 256-row page), and a 7.48 MB query peak; the local Debug run took
  about 120 ms with the production allocator. This is bounded by the branch
  count rather than outer-row count.
- Pgwire follow-up: 28 tests; native adapter: seven tests, including typed
  expression arguments, JSON-null provenance, cancellation and credential checks.
- Final SQL-filtered API integration: 31 tests, no leaks; `make generate` and
  `make fmt` passed after integration.
- Follow-up native storage: 92 tests; distributed transactions: 96; Lite SQL: 9.
  New fault tests cover reservation survival across two restarts, atomic counter
  exhaustion, and rejection of equal counters after restore/topology/identity
  changes. These are focused fault tests, not a complete distributed chaos gate.
- Native LSM bookkeeping workload (Debug, 100 batches of 32 common-prefix rows):
  tracking inactive 452 ms versus active 485 ms, about 7.3% overhead in one run.
  This is preliminary, not a statistically rigorous production throughput claim.
- Forward-cursor follow-up: pgwire suite passes 30 tests, including real
  BEGIN/DECLARE/FETCH FORWARD/FETCH ALL/COMMIT protocol lifecycle with bounded
  pages and a failed-fetch/transaction-abort path with retained-stream release;
  parser coverage includes quoted identifiers, FETCH ALL, CLOSE ALL, and
  rejected scroll/hold options. SQL-filtered API integration passes 31 tests
  with no leaks. These focused checks do not replace the full SQL extraction parity and
  distributed-fault gates.
- Partial unique `ON CONFLICT` follow-up: SQL runtime/compiler suite passes 170
  tests; distributed transaction suite passes 97 tests, including stronger
  predicate implication and rows entering/leaving partial-index membership;
  C API suite passes 23 tests. `make fmt` and `git diff --check` pass. This
  covers the bounded simple-predicate shape only, not expression/deferrable
  arbiters or the complete SQL extraction parity gate.

- Expanded SQL runtime/compiler/binder: 156 tests in ReleaseSafe; pgwire:
  25 tests; native SQL pgwire adapter and mounted route: seven tests. Final
  combined SQL-filtered API run: 30 tests; distributed transactions: 96 tests.
- API guarded-session integration covers activation, guard-only commit,
  savepoint observation retention, changed-proof rejection before row fetch,
  and DELETE RETURNING with row digest and range proof in one commit plan.
- DocStore/native transactions: 89 tests, including recovery and range-guard
  races. A 100,000-touch bookkeeping fixture performs one activation probe and
  zero counter writes when inactive, versus two probes and one coalesced counter
  write for an active single bucket. This is not a storage-throughput benchmark.
- Native Lite SQL: nine tests, including document mutation/preimage preservation,
  schema fences, same-TTL stale-write rejection, normalized RETURNING, composite
  arbiters, self-FK cascades and stale claim rejection.
- The synthetic 10,000-row pull fixture retains about 98 KiB; one ReleaseSafe
  run produced its first 73-row page in 0.42 ms and completed in 63 ms. This is
  an in-process executor/allocation measurement, not distributed throughput.
- Window tests cover peer-aware frames, grouping/HAVING phase ordering,
  cancellation, exhaustive allocation failures, all boundaries of filtered
  nullable moving frames, and intermediate-versus-result numeric overflow.
- The ReleaseSafe 10,000-row, 8,193-wide moving SUM fixture uses 2.73 MiB of
  indexed aggregate state and completes in about 2 ms. This measures the
  aggregate operator, excluding scan, binding and network work.
- `make generate` succeeded using `/private/tmp/antfly-sql-generate-cache` after
  the default cache again referenced a missing generator executable. `make fmt`
  succeeded across Zig, Go, Python, TypeScript and Rust.

### Prior checkpoint validation

- SQL runtime/compiler/binder: 122 tests; pgwire: 23 tests.
- Native RETURNING/Lite integration: four tests, including document reads.
- API SQL integration: 128 tests, including document snapshot isolation.
- Native relational/index suite: 122 tests, plus the focused ReleaseSafe
  document snapshot fixture; retained-read registry: three lifecycle tests.
- Linked retained-read owner tests passed, including the native read-provider
  integration target. Document SQL rejects the relational-only stateless
  fallback when a retained reader is unavailable.
- `make generate` succeeded with a separate cache after the default Zig cache
  reported missing generator artifacts; `make fmt` succeeded.
- Go SDK module: `go test ./...` passed, including generated SQL policy tests.
- ReleaseSafe nested-shape binding microbenchmark: roughly 5.24 microseconds
  and 31.4 KiB arena capacity for inferred input versus 2.83 microseconds and
  17 KiB with explicit hints. Both perform zero data reads. This measures
  binding overhead, not distributed query throughput.

## Historical initial slice

- Shared bounded scanner and immutable, owned single-statement compiler;
  parameters are typed nodes rather than SQL substitution. See
  `zig/pkg/antfly/src/sql/COMPILER_SUPPORT.md` for exact grammar coverage.
- Shared non-executing Describe/Execute binder: one authorized catalog lookup,
  positional parameter inference, ordered column metadata, and owned typed JSON
  literals parsed once per binding. Conflicting parameter types fail early.
- Relational SELECT projections, conjunction predicates, COUNT, primary-key
  ordering, LIMIT/OFFSET; version-conditional INSERT with explicit `_id`,
  literal-assignment UPDATE, and DELETE through the native atomic coordinator.
- Exact `_id` predicates lower to native point-key spans, including shard-start
  boundaries. UPDATE reads only preserved columns, relies on the whole-row
  version fence, and shares immutable assignment values across prepared rows.
  SQL writes to generated columns are rejected; native generation remains the
  single owner of their values. NULL comparisons can eliminate scans without
  bypassing column validation or permissions.
- Current-catalog binding and authorization, schema fencing, projection and
  predicate pushdown, cancellation, preallocation memory admission, and bounded
  mutation preparation. Unsupported shapes fail before mutation.
- A shared, bounded, scope-partitioned immutable plan cache is used by both
  HTTP SQL and pgwire. It compiles outside its short publication mutex, deduplicates
  concurrent misses, and only evicts idle leased plans; schema resolution,
  authorization, binding and parameter values remain request-local.
- Schema derivation is cached independently in bounded immutable entries (32 MiB
  maximum, four concurrent builders). Hits reuse compact typed columns without
  reparsing schemas. Catalog identity and authorization are still resolved and
  fenced per request; the cache is not an authorization or routing cache.
- HTTP and pgwire share bounded preparation admission, followed by an owned
  read or write execution permit held through completion. HTTP overloads return
  SQLSTATE 53300 with retry guidance. Compiler positions survive worker dispatch.
- Native SQL scan requests stay typed from the executor to the local storage
  boundary. The relational reader can choose a READY compound or partial index
  for a leading equality prefix and intersected range suffix, retaining the complete predicate as a
  residual check; the primary-key path remains the deterministic fallback.
  Explicit primary-key ordering disables secondary-index selection. Candidate
  eligibility is checked before durable readiness I/O, and readiness checks
  reuse one ownership proof from the pinned transaction.
  A sixteen-candidate shortlist uses at most eight snapshot-local index records
  per candidate for costing; a sole candidate needs no cardinality probe.
  Descending bounds and binary versus folded collations preserve predicate
  semantics. These are bounded actual probes, not persistent cardinality stats.
  The schema, index plan and store transaction are pinned together; compilation,
  readiness reads and cardinality probes run after releasing the shared apply
  lock, so planning does not serialize writers behind its allocations or I/O.
- Native retained readers own the schema/store snapshot, compiled predicates,
  projections and row-level authorization filter. Their typed pages decode
  selected ordinals directly, preserving exact integers and missing/null values
  without serializing and reparsing row JSON. SQL cursors close on completion,
  early LIMIT, error or cancellation; mutation preparation releases the read
  snapshot before entering commit admission.
  Single-local-owner provisioned routing carries the original table/topology
  fence and retains owner/admission leases across pages through the checked
  native archive ABI. Leader loss never downgrades retained admission to stale.
  Catalog checks before and after opening tie the snapshot to its SQL binding;
  they are not repeated for every page. Pgwire rechecks credentials and read
  authorization on each retained page and normalizes deadlines to storage time.
- Generated `/sql` contracts plus Go, TypeScript, Python, and Zig convenience
  APIs. Mutation state and transaction receipts survive both success and error
  responses. The one-shot `antfly sql` command sends separately typed parameters
  and never automatically retries or follows redirects for SQL mutations.
- Standalone PostgreSQL protocol/listener module with authenticated backend
  callbacks, typed parameters, bounded prepared statements and portals, and
  structured `std.Io` cancellation. The native adapter reuses the shared binder,
  authentication/policy machinery and atomic coordinator. Prepared statements
  carry pre-execution identity/schema fences, and portals retain bounded native
  results without copying them again. The optional `pgwire` node configuration
  registers the listener with the production API kernel, enforces authenticated
  and protected transport configuration, and joins connections before teardown;
  see `zig/pkg/antfly/src/pgwire/README.md`.

### Verified initial slice

- SQL compiler/binder/executor: 41 tests; pgwire protocol: 20 tests.
- Native SQL/pgwire API integration: 45 tests, including timestamp precision,
  credential rotation, allocation failures, body limits, and generated columns.
- CLI: 87 tests; Zig SQL SDK: 4 tests, including no-replay behavior through both
  the generated and convenience clients with an unsafe borrowed retry policy.
- HTTP library: 584 passed, 8 skipped; OpenAPI generator: 75 tests.
- Go SDK suite passed; TypeScript SDK: 361 passed, 1 skipped, and typecheck;
  Python client/SQL tests: 72 passed.
- Zig OpenAPI and SQL grammar freshness checks passed; Python generated models
  are current. Repository formatting completed.

These are local focused checks, not a full release or CI run. The repository-wide
license check still reports pre-existing header mismatches outside this slice.

### Verified retained-read and planner follow-up

- SQL executor/compiler/binder: 49 tests; pgwire protocol/lifecycle: 21 tests.
- Relational storage/index suite: 112 tests, including typed projection parity,
  retained snapshot ownership/cancellation and planning outside the apply lock.
- API runtime: 17 tests; physical table reads: 18; linked read consumers: 75.
  These execute multi-page SQL, replacement during admission, credential
  revocation, strict leader-loss rejection and retained-owner cleanup.
- Actual hidden storage-owner ABI: one regression covers checked provider
  acquisition, typed paging, snapshot stability and translated errors.
- Configuration: 48 tests. Go SDK suite, Python SQL (12), TypeScript SQL (12)
  and TypeScript typechecking passed.
- Repository format checking and generated Zig OpenAPI freshness passed.

The default local Zig cache had missing generated artifacts; freshness and
focused builds used separate caches. These checks do not establish remote-owner
or multi-owner statement-snapshot support, nor complete the SQL extraction parity gate.

## Initial measurements

Local Apple Silicon, ReleaseSafe compiler microbenchmark: two 10,000-operation
runs averaged approximately 674–999 ns with 928 bytes of retained arena capacity.
A 100,040-byte commented query retained 398 bytes after lexer scratch release.
Rejecting a 256,000-byte token-heavy input at a 128-token quota took approximately
469–792 ns versus 1.356–1.808 ms to tokenize the complete input. That last comparison is
an admission-work reduction, not an end-to-end query throughput claim.

A synthetic executor regression counts 10,000 rows in 40 pages under a 512 KiB
allocation quota, proving pages are released rather than retained as a relation.
A 64-row UPDATE regression verifies that overwritten values are not requested
from storage, a 2 KiB assignment is owned once rather than 64 times, and peak
request allocation stays below 128 KiB. This is an allocation/projection test,
not a claim about end-to-end storage throughput.

The bounded index-costing fixture has 96 primary rows. An intersected descending
range visits six index records without primary lookups; adding a competing
selective covering index requires seven planning probes and selects a one-record
scan, again without primary lookups. These deterministic work counts test reduced
storage work; they are not an end-to-end latency benchmark.
