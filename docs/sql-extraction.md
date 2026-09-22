# SQL extraction implementation ledger

S2 starts at main `227f2dc39` after catalog #691 and relational #784.
The behavioral reference is `combine-pr-141-143-144` at `79644dfa1`.
This ledger is not a claim that all SQL surfaces are available.

**Status: S2 is in progress, not ready for publication as the complete S2 PR.**
The implementation now includes scalar and aggregate execution, joins and CTEs,
native catalog DDL, durable READ COMMITTED sessions/savepoints, and public SQL
interfaces. It does not yet reproduce the mega branch's complete SQL behavior.

## Integration status

The older initial-slice notes below are historical context, not the current
feature inventory. Current additions include:

- Ordinal-bound scalar programs, three-valued predicates, bounded top-K,
  grouped/global aggregates, HAVING, joins, derived tables and nonrecursive CTEs.
- Expression INSERT/UPDATE with whole-batch validation, exact integer values,
  and explicit SQL-null versus JSON-null provenance through native storage.
- Coordinated capture across local owners and tables, with all capture fences
  released before paging. Alias cursors share capture but not cursor position.
  Remote-owner transport and durable range/phantom protection remain unfinished.
- Durable READ COMMITTED sessions, savepoints, read-your-writes overlays and
  native constraint/default/generated-row normalization. Stronger isolation
  remains rejected rather than silently weakened.
- Native catalog/schema/index/constraint DDL and durable pending/invalid
  receipts; pending activation/rewrite is not reported as synchronous success.
- Generated HTTP clients, authenticated pgwire, a Lite C ABI, interactive CLI,
  and the Antfarm SQL workbench using the same execution contracts.

Set operations and typed INSERT-from-query are implemented with shared memory
admission and whole-statement validation before mutation. Full query-shape
parameter constraints propagate through nested derived queries, CTEs and sets
before programs are emitted. Relational RETURNING uses native normalization
under the schema fence and prepares all output before committing; DELETE
returns the version-fenced preimage. Native tests cover defaults and generated
values rather than reading rows back after the write.

Document reads now have declaration-derived shapes and retained native
transactions with bounded pages, TTL, authorization and typed projections.
Document mutations remain explicitly unsupported: undeclared-field preservation
and native mutation/version integration must land before they are enabled.

Remote retained-read ownership now has bounded admission, incarnation/generation
tokens, sequence-fenced page borrowing, owned cancellation and periodic expiry.
This is internal infrastructure, NOT enabled remote SQL: authenticated RPC
mounting, client transport and distributed capture coordination are unfinished.

Remaining major items include document-table mutations, remote coordinated readers, durable
transactional range protection, conflict actions,
native SQL row identity, windows/correlated subqueries, streaming portals and
the complete parity, fault-injection and workload benchmark gates. Passing
focused component tests is not completion of S2.

### Latest local validation

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

## Architecture

One bounded lexical/semantic compilation produces an immutable, owned syntax
plan. Typed positional parameters bind without rewriting SQL. Binding resolves
only referenced catalog names and pins immutable physical identity and schema;
execution reuses the native read and atomic distributed mutation contracts.
Neither the historical mega-branch storage engine nor its old catalog is copied.

Page arenas are released between reads. Output and mutation admission limits are
independent from SQL LIMIT; exceeding a result cap must fail, not silently
truncate a query. Mutations prepare the entire bounded write set before making
one native atomic commit, with observed row versions and the bound schema epoch.
SQL cannot retry an ambiguous mutation automatically.

Allocation admission applies before allocation to both result arena capacity
and temporary page storage. Page-count limits also bound scans that return no
matches. Exact integer columns never round-trip through a floating-point JSON
representation. Native committed-but-pending/repair-required outcomes and
ambiguous transaction receipts must remain visible in SQL responses.

All public contracts originate in OpenAPI. Protocol-specific PostgreSQL OIDs
belong in pgwire, not the native durable types. Graph and lake SQL adapters remain
outside S2, as specified in the restructuring plan.

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

### Initial read capability limit (superseded for coordinated local owners)

Native retained readers provide an owner-local statement snapshot, not a globally
repeatable cross-owner snapshot. The SQL adapter uses the retained-read capability
only when explicitly supplied by its read provider. For providers without it,
statements that would require a second native page fail with
`SqlStatementSnapshotRequired` before returning results or committing writes.
Retained-reader support must replace this gate before unrestricted SQL scans,
range mutations, cursors, or stronger SQL transaction guarantees are exposed.

## Original S2 work list (see integration status above)

1. Extend retained reads beyond the implemented single-local-owner path with
   remote-owner handles and a coordinated cross-owner read fence. Preserve
   schema/table identity, topology, cancellation and bounded admission.
   Range/phantom protection must be explicit for transactional mutations.
   This requires owner-issued read handles and coordinator cleanup across
   success, timeout, disconnect, topology change and partial acquisition; the
   existing independent scan calls cannot supply a global snapshot. Carry the
   implemented typed pages through that protocol, encoding only at process
   boundaries. A durable owner-validated binding lease can eventually replace
   the current bounded schema cache plus pre/post-open catalog checks.
2. Binder-resolved scalar-expression IR and boolean predicates, ready-index
   selection/costing, bounded sort/top-K, aggregates/windows, joins, CTEs and
   subqueries. Reconcile each supported shape with the mega-branch parity corpus.
3. Document execution; native SQL primary-key policy; expression DML, conflict
   actions/RETURNING, and catalog/schema/index/constraint/tablespace SQL DDL.
   Parsing basic DDL does not mean it is executable today.
4. SQL sessions, transactions/savepoints, streaming cursors and native
   coordinator ownership. PostgreSQL prepared identity fences and per-statement
   reauthorization are implemented, but not retained SQL transaction readers
   or transaction/savepoint state.
   HTTP `session_id` and transaction statements currently fail closed.
5. Pgwire transaction status/session ownership; Lite/C ABI, interactive CLI and
   Antfarm SQL workbench. Production listener configuration and lifecycle are
   implemented; explicit SQL transactions remain rejected.
6. Full source parity and release gates, end-to-end fault/cancellation tests,
   workload benchmarks and observability. Update this ledger before publication.

## Validation gates

- Compiler syntax, ownership, parameter, limits, and hostile-input tests.
- Bound executor correctness, schema/version fences, null/numeric semantics,
  bounded scans, atomic mutations, cancellation, and allocation-failure tests.
- Authenticated HTTP execution and catalog authorization parity tests.
- Protocol simple/extended query, parameters, cancellation, and auth tests.
- SQL DDL, sessions, transactions, prepared statements, and cursors.
- Document execution, joins/aggregates/windows, and source parity corpus.
- Lite/C ABI, CLI/workbench, generated clients and freshness checks.
- Benchmarks distinguishing parse, bind, execution, and retained memory.

The focused tests in this slice do not satisfy the complete S2 release gate.

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
or multi-owner statement-snapshot support, nor complete the S2 parity gate.

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

## Current CLI example

Against an existing relational table with a `name` column:

```sh
antfly sql --database default --namespace public \
  --statement 'SELECT _id, name FROM users WHERE _id = $1' \
  --parameters '["user-42"]'
```

Results remain ordinal arrays, so duplicate column names are not lost. The
`--limit` option is an admission ceiling; put `LIMIT` in SQL when intentionally
requesting a prefix. Transaction/session commands still fail explicitly.
