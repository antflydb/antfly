# SQL extraction implementation ledger

SQL extraction starts at main `227f2dc39` after catalog #691 and relational #784.
The behavioral reference is `combine-pr-141-143-144` at `79644dfa1`.
This ledger is not a claim that all SQL surfaces are available.

**Status: SQL extraction is in progress, not ready as a complete SQL feature.**
The implementation now includes scalar and aggregate execution, joins and CTEs,
native catalog DDL, durable READ COMMITTED sessions/savepoints, and public SQL
interfaces. It does not yet reproduce the mega branch's complete SQL behavior.

### Pre-merge activation work

Standalone ordinary FK publication now uses the durable native owner-control
path, with operation receipts separate from Raft applied watermarks. Catalog,
schema, receipt and fence release are committed together. The linked native
regressions pass initial self-FK publication, ordinary DROP/ADD and reparenting
to two external parent owners, lost source/install replies, cold restart and
canceled hidden-owner retirement (three tests). Two additional native storage
tests pass receipt replay and old-schema pinning through restart. Initial
external-parent CREATE, including MATCH PARTIAL support-index publication, now
passes valid/orphan/protected-parent writes, cold restart and deterministic
cancellation in native standalone. Schema finalization uses an exact durable
node/store/root binding and owner progress, never synthetic Raft placements.
Copied foreign owner bindings do not authorize native publication. This does
not enable that lifecycle on hot-standby deployments without explicit adoption.
The linked native activation suite now passes six tests, including public
external-FK/graph TRUNCATE and cold reopen, plus three repeat runs (18 checks)
without skips or leaks. Restore authority cancellation checks run outside the
metadata mutex; test mutations that require immediate phase-two delivery ask
for write-level acknowledgement rather than the API's proposal-only default.

The same work fixes parent receipt validation for non-first owner ranges and
keeps ordinary owner ACKs off the full-catalog decode path: a revision-checked
point read advances an unchanged projection, while publication or concurrent
catalog changes require the complete immutable snapshot refresh.

Authenticated retirement summaries now use a transactional compressed Patricia
tree, with immutable-value hashes, canonical roots, protected keys and bounded
handoff/GC admission. Eighteen focused tests pass, including native abort,
restart, owner transfer and two-phase GC. In the local Debug benchmark, 10,000
summary point reads took 14.3–16.5 ms. This trades write cost for predictable
reads: at 512 records with insertion batches of 64, measured insert WAL was
741,238 bytes versus 437,920 without the tree (1.69x), and GC WAL was 272,162
versus 46,752 bytes (5.82x). These are small local fixtures, not production
throughput claims or a claim of reduced write amplification.

Standalone and bound-primary hot-standby row-policy publication pass two linked
native tests, including ordered metadata/owner replay and cold reopen. This
does not yet prove the complete promoted-primary failover matrix. The catalog
suite passes 146 tests, and the distributed transaction contract suite passes
102 tests.

Durable idle HTTP connections pass eight focused tests, including authenticated
public handler calls for settings, commit/rollback, DISCARD, prepared-resource
retirement and ownership checks. External-parent and graph TRUNCATE are enabled
through the public route without private test permits. The installed recovery
binary passes 16 tests without skips or leaks (five mounted real-listener
scenarios, two focused contracts and nine import anchors); the SQL boundary
suite passes 69 tests, including rejection of a missing parent admission proof
and unsupported native source/parent receipt authority before job admission.
These results do not establish release readiness for the entire SQL feature.
The TypeScript connection helpers use generated request/response schemas and
bounded, non-retrying SQL transport; the SDK SQL suite passes 30 tests.
The complete TypeScript SDK suite passes 396 tests with one conditional bundle
check skipped.

Direct-field dense/sparse vectors now have bounded, resumable snapshot transport
and ordered tail coverage, including oversized historical base artifacts on
full-text-only tables. The focused vector suite passes 15 tests; its 13 storage
tests also pass five additional runs (65 checks) without skips or leaks.
The same 15 tests pass with production admission enabled and test permits
removed. Ordered-artifact compatibility passes 13 tests, portable backup 39,
retained effects 32, and transaction reservations 49. Graph/enrichment/resolver
asynchronous tails remain unsupported.

The next activation cut is still in progress. Ordered-artifact regressions now
pass 54 tests, including durable stale-publication rejection, ambiguous commit
recovery without replay-sequence reuse, owned preparation outside the apply
lock, binary-safe catalog transport, and attempt-bound immutable source layouts.
The graph transfer contract preserves source bytes for receipt verification and
rebinds physical generations using separately authenticated receiver layouts;
the wider source protocol is not advertised yet. Certified source-copy graph
ownership blocks are included in the transferable source-cut digest; omitting
or changing those bytes changes the certificate. Retained-transfer regressions
pass 28 tests, including source-certificate block binding, graph-bearing pinned
source export/reopen/import, oversized frames,
bounded spool caching, authenticated
cold reads, partial-copy corruption repair and restart at exact row boundaries.
The compact publication upload codec and staging primitives now pass retry,
ordered-age pruning, tombstone and allocation-failure checks. These primitives
are not yet connected to the complete Raft/standby upload/finalize path.
Malformed producer graph effects become durable invalid-output rejections;
resource failures and persisted-catalog errors remain failures, not success.
Actual producer integration, complete baseline/provenance admission, historical
proof transfer/adoption, large-publication runtime transport and the full
graph/enrichment/resolver recovery matrix remain activation gates.
These passing foundations are not evidence that those families are enabled.

Sparse immutable segments now bind postings and document maps to per-document
incarnations, preventing stale scores after stable-ordinal replacement and late
compaction publication. Sidecars are point-addressed, query caches are bounded,
and selected compaction inputs are reserved before allocation. The sparse suite
passes 24 tests, including replacement, restart, compaction races, metadata
retirement, corruption and budget denial. A fixed-capacity preflight cache
avoids charging repeated terms for identical document metadata; collisions
conservatively overcount. These are correctness and bounded-work checks, not a
large-scale throughput benchmark.

Native checkpoint capture now waits for an in-flight immutable-table build and
drains the committed mutable tail before exporting its manifest. Previously a
background flush could make the drain yield, producing a self-contained image
without committed retention metadata. Explicit checkpoint admission no longer
overrides the shared maintenance budget. Three checkpoint/backpressure tests
pass; the deterministic contested-cut export/reopen regression also passes ten
additional runs with no skips or leaks. This is correctness evidence, not a
checkpoint-latency benchmark.

The runtime suites pass 50 native implementation and 173 consumer tests. The
three deterministic production DataServer merge/split histories and the native
retirement regression also pass three repeat runs (12 checks), with no skips
or leaks. These cover UNIQUE/FK ownership, public writes, failover, restart and
exact-root cancellation. Native reconciliation and retirement now preserve
the existing storage owner rather than attaching a second opaque owner; cold
retirement reads use the same filesystem runtime.
Generation-transition rejection returns temporary unavailability without
claiming an aborted transaction or authorizing blind replay.

The final API runtime suite passes 338 tests, including shared native/document
restore, hidden handoff capability guards and generated route-policy coverage.
Both focused public write-unavailability regressions pass. The rebuilt mounted
TRUNCATE recovery binary passes all 16 tests after the hidden-owner routing fix;
these results do not rely on a previously built binary without the capability
checks.

The current inventory check still has 1,586 original cases: 103 implemented,
59 explicitly rejected, 38 superseded, and 1,386 unresolved dispositions.
Unresolved dispositions are not a count of distinct missing features. These
activation fixes do not replace the broader SQL parity/release gate.

## Integration status

The older initial-slice notes below are historical context, not the current
feature inventory. Current additions include:

- Ordinal-bound scalar programs, three-valued predicates, bounded top-K,
  grouped/global aggregates, HAVING, joins, derived tables and nonrecursive CTEs.
- Expression INSERT/UPDATE with whole-batch validation, exact integer values,
  and explicit SQL-null versus JSON-null provenance through native storage.
- Coordinated capture across local and remote owners and tables, with all capture
  fences released before paging. Alias cursors share capture but not position.
  Remote capture uses authenticated internal RPC, bounded owner leases and fresh
  quorum proofs. Durable owner-routed range protection is implemented below;
  deployment capabilities and TTL restrictions are explicit admission boundaries.
- Durable READ COMMITTED sessions, savepoints, read-your-writes overlays and
  native constraint/default/generated-row normalization. Repeatable-read and
  serializable sessions require explicitly capable providers, replicated range
  tracking and owner-fenced atomic prepare; unsupported providers reject BEGIN.
- Native catalog/schema/index/constraint DDL and durable pending/invalid
  receipts; pending activation/rewrite is not reported as synchronous success.
  ALTER TABLE ADD PRIMARY KEY uses the metadata-owned non-null/unique
  fresh-generation rewrite. Mounted SQL verifies authorized pending receipts,
  successful publication, NULL/duplicate failure isolation, and data-owner
  restart persistence; broader schema-rewrite and restore gates remain separate.
  Uncertain restore admission now retains an idempotency key and deterministic
  job handle in the SQL receipt, with a non-success HTTP status and no-replay
  guidance rather than presenting an unconfirmed job as accepted.
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
Document INSERT/UPDATE/DELETE now use native validation and schema fences.
Updates preserve undeclared fields from the full raw preimage; exact-byte row
digests prevent stale writes even when custom TTL timestamps are unchanged.
RETURNING is prepared before commit, including SQL-null provenance.

Remote retained-read ownership now has bounded admission, incarnation/generation
tokens, sequence-fenced page borrowing, owned cancellation and periodic expiry.
Authenticated RPC mounting, client transport and distributed capture coordination
are implemented. Lost capture responses have scoped cleanup, rather than relying
only on lease expiry. Remote typed pages preserve exact integers, null provenance
and mutation preimages; native normalization remains the owner authority.
The bound single-table local source now pins multiple independent retained scan
views inside one short native statement-capture fence, after read-index preflight
and before releasing writers to resume. This supports local CTE/alias reads
without pretending separately opened cursors share a snapshot. That source
rejects guarded range-proof capture until it can supply owner-fenced proofs;
the provisioned distributed source retains its separate coordinated path.

Nonblocking pgwire queries now pull bounded pages, including nested derived
queries and CTEs, with network backpressure and immediate snapshot release on
completion/error. Portal admission and plan leases live with the cursor, not a
request stack. Blocking sorts/aggregates/windows retain bounded materialization.
Prepared identity manifests cover all physical tables in a relation plan.
CTE hints now have execution semantics: explicit `MATERIALIZED` and default
multiply referenced CTEs share one quota-bound typed producer, while
`NOT MATERIALIZED` retains separate inline scans. A repeated-reference test
checks physical scan counts and retained-row behavior; single-reference
unhinted CTEs stay pipelined. Default producer demand propagates backward
through inlined CTEs, so a multiply referenced downstream CTE does not
silently rescan an upstream automatic producer.

Window execution shares partition/order sorts, tracks peer boundaries, and uses
segment trees for moving aggregate frames. Ranking, offsets, value functions,
aggregates and ROWS/numeric RANGE frames preserve typed null/numeric semantics.
Internal aggregate nodes use wider numeric state; only requested SQL results are
range-checked. Omitted INSERT identities use the shared native secure row-ID
generator. Primary-identity ON CONFLICT actions use exact observed row fences.
Conflict-assignment scalar subqueries use a post-owner masked Apply: a direct,
uncorrelated scalar SELECT is evaluated only for rows that actually conflict
and satisfy DO UPDATE WHERE. Its point/range/absence reads use the same pinned
statement cut and commit read set as the owner arbitration. The earlier eager
INSERT-source capture was unsound because an untaken assignment could raise a
scalar error on a nonconflicting insert or a WHERE-false conflict. Conditional
or nested scalar subqueries, owner-correlated reads, and secondary-index
probes remain rejected until their demand masks and index membership proofs
can be preserved. Owner-local LSM snapshots fork delayed primary scans from
a bounded, route-fenced visibility cut. The distributed single-table primary
path now acquires every owner's write/replay fence, rejects unresolved
prepared intents, forks independent retained cursors while all fences are
held, and releases the fences before paging. It binds the schema version and
every owner/absence proof to the same guarded commit; unsupported multi-table
or non-primary delayed shapes remain closed.
Existing conflict-owner point reads now use reclaimed cursor and page scratch
per owner, resetting page scratch after empty progress pages. A 32-owner batch
with three 64 KiB native continuation pages per owner fits a 512 KiB SQL
memory admission budget while retaining all normalized images and
version/claim guards for one atomic commit; page and cursor storage no longer
accumulates with owners or progress pages. Conflict point pages now consume one
batch-wide page quota rather than restarting the allowance for every owner;
captured INSERT-source pages and conflict pages still have separate admission.
Ordinary composite-unique targets now use the native tuple codec, activation
coverage, generation identity and durable compare-claim observations; those
observations survive session merging and savepoints. Lite uses native durable
transaction prepare/commit for single-handle constraint expansion, including
self-referencing foreign-key actions. It rejects out-of-handle dependencies.

Equality-correlated EXISTS/NOT EXISTS and scalar subqueries lower to grouped
hash joins under the same coordinated capture as their outer query. Scalar
cardinality, NULL equality and hidden-column projection are checked explicitly.
Single-column IN/NOT IN subqueries now use bounded grouped membership and NULL
evidence under that same capture. Empty sets, duplicate values, nullable operands
and correlated NULL groups retain three-valued SQL semantics. Computed side-local
join operands bind as hash keys rather than quadratic residual comparisons.
ANY/SOME/ALL comparisons use grouped extrema and null/count evidence, including
empty sets and all six comparison operators. Correlated EXISTS supports one
ordered comparison alongside equality keys using MIN/MAX evidence. Computed
OR-correlated EXISTS predicates distribute into at most eight independently
grouped/ordered witness branches under the same capture; local-only OR filters
stay intact to avoid needless scans. NOT EXISTS negates the combined Boolean
result, including when inner comparisons encounter NULL. Nested local
membership subqueries lower within the independently bound inner child;
lexical aliases shadow the outer query as usual, while qualified references
escaping past that child's scope are rejected. Computed
outer keys and composed scalar aggregates are decorrelated without per-row
remote queries. Discarded EXISTS projections are fully bound but never evaluated
or retained as scan dependencies. Uncorrelated value subqueries preserve complete
set/group/window/CTE/top-K semantics through independently bound derived-query
boundaries. Multiple correlated ranges, correlated set/group/window/top-K forms,
and lazy outer subquery branches remain unsupported.

Quantified LIKE/ILIKE subqueries use a distinct, quota-accounted pattern set
per correlation key rather than the ordered-comparison MIN/MAX shortcut.
The inner source is captured once, while the step-limited scalar matcher folds
ANY/SOME/ALL (including per-pattern NOT forms) with SQL empty-set and NULL
semantics. Empty global aggregates contain no synthetic NULL member; large
sets fail the retained-byte limit. Arbitrary pattern matching still has a
bounded per-outer-row probe cost rather than an index shortcut. Pattern state
is allocated only for this internal aggregate; the ordinary 10,000-row grouped
benchmark retains its 1,912-byte peak after the change. Pattern and operand
parameters infer string independently of the aggregate's JSON result type.

Named WINDOW definitions resolve in query-local scopes before publication of the
immutable AST. Inheritance restrictions and unused definitions are validated
without evaluating discarded expressions. GROUPS frames use indexed peer-group
boundaries; EXCLUDE CURRENT ROW/GROUP/TIES/NO OTHERS produces at most three
intervals, shared by indexed aggregates and constant-interval value selection.

Targetless ON CONFLICT DO NOTHING now coordinates primary identity and every
supported immediate unique arbiter. Only admitted candidates reserve in-batch
claims, so skipped rows cannot suppress later valid rows. Native schema and
generation fences still cover the complete atomic mutation.

Explicit ON CONFLICT targets now infer partial unique indexes through a bounded
typed predicate proof shared with native index membership. The admitted SQL
shape is a conjunction of column/literal comparisons and IS [NOT] NULL; claim
generation checks the predicate against typed old/new rows. Typed expression
keys share the native tuple VM, dependency projections, activation and retirement
machinery. Explicit expression targets use order-independent equality-key
identity, and expression/predicate changes fence the durable claim generation.
Deferrable ON CONFLICT arbiters remain deliberately rejected, matching
[PostgreSQL's immediate-arbiter contract](https://www.postgresql.org/docs/18/sql-insert.html).
Ordinary deferrable UNIQUE declarations now share the native claim authority.
Statement validation honors durable timing overrides; final commit validates
the complete final overlay, including swaps. SET CONSTRAINTS IMMEDIATE validates
retroactively before publishing its mode change. Savepoints and restart preserve
the timing state. Deferrable UNIQUE keys are not eligible FK parent targets.

Joined UPDATE/FROM and DELETE/USING (including explicit joins and CTE sources)
carry target versions and document digests through one statement capture. All
images and RETURNING values are prepared before native atomic commit. Equality
conjuncts expose hash keys; overwritten columns are not loaded. Multiple source
matches for an UPDATE target fail with SQLSTATE 21000 rather than arbitrarily
choosing a row. DELETE deduplicates target images before the mutation quota.

Linear recursive CTEs use a delta worklist, typed UNION DISTINCT visited keys,
one capture of physical sources, and reusable static-side hash indexes. Seed
types determine recursive outputs; explicit casts supply widening. Mutual or
nonlinear recursion, recursive aggregates/windows and nullable-side self joins
remain explicit unsupported shapes. This is not unrestricted recursive SQL.

Pgwire SQL PREPARE/EXECUTE/DEALLOCATE shares the existing bounded wire-protocol
prepared registry and binding-identity fences. Statements survive transaction
commit and release on deallocation/disconnect. EXECUTE evaluates typed scalar
arguments in an empty binding environment, never interpolating SQL or opening
table readers; JSON null remains distinct from SQL NULL. Eligible executions
use the existing backpressured pull stream. SQL-language
DECLARE/FETCH/MOVE/CLOSE cursors retain that same bounded pull stream under the
connection owner. Blocking read shapes that decline pull execution instead
execute once into a quota-bound typed cursor spool before DECLARE publishes
the name; a failed quota check cannot expose a partial cursor. Materialized
FETCH never reexecutes SQL and rechecks current authority and the original
binding in the pinned lookup namespace. DECLARE is transaction-bound, pins its binding identity and
transaction read-your-writes overlay, and stages stronger-isolation range proofs
before releasing transaction admission. FETCH pages are bounded and flushed
before another page is pulled; exhausted, failed, closed, committed and
disconnected cursors release their stream. SCROLL uses a quota-bound lazy typed
spool. WITH HOLD drains under the original transaction before COMMIT and detaches
only after confirmed commit; ambiguous commit never publishes a held cursor.
Held fetches recheck current authority and pinned source identity. These cursors
are connection-owned, not restart-durable. HTTP exposes durable prepare/execute/close resources,
independent of transaction commit, with principal/owner checks, expiry, bounded
admission and immutable binding manifests. Execution reads one resource in a
read-only transaction; create/close/expiry atomically maintain compact admission
metadata. Resources survive restart on the owning node; failover to another
owner is explicitly rejected instead of silently retargeting the statement.

Pgwire SET/LOCAL/SHOW/RESET supports bounded `statement_timeout`, bounded
UTF-8 `application_name`, a single existing `search_path` namespace, and the
immutable negotiated UTF-8 `client_encoding`.
`SET NAMES` uses the same UTF-8-only connection-owned path.
`RESET ALL` resets those connection-owned settings with transaction/savepoint
semantics in simple and extended protocol. Writable dotted `app.*` definitions
now have typed pgwire overlays from the durable catalog, while policy-sensitive
definitions cannot be set by the client. Original case `sql-0045` remains
unresolved until exact mounted parity evidence covers the complete catalog
setting/session behavior.
Outside a transaction, `DISCARD ALL` additionally closes connection-owned
prepared plans, portals and held cursors after the command reply; original
case `sql-0047` also remains unresolved pending full catalog-setting parity.
Settings obey transaction and savepoint
restoration. Lookup namespace is separate from immutable transaction ownership;
prepared statements and held cursors retain their original namespace. Multiple
search-path entries, `$user` expansion and unrelated settings fail explicitly.
All pgwire describe, execute, simple-stream and extended-portal paths classify
connection-owned settings through the same typed command boundary, so a setting
cannot accidentally open a SQL storage cursor.

Dry-run EXPLAIN now binds the inner query or mutation under its real authority
and renders bounded text or versioned JSON from the immutable bound plan. It
does not open a row cursor or execute a mutation. The exact original text and
`FORMAT JSON, VERBOSE, COSTS OFF` read cases pass mounted HTTP. The exact
original INSERT explanation also passes mounted HTTP with a post-request row
count proving no write. The exact UPDATE-with-membership-subquery and cross-table
MERGE explanations pass mounted HTTP with no read capture or distributed commit
attempt. Target-only UPDATE predicate/assignment and DELETE predicate subqueries
now use the same decorrelated, snapshot-captured joined-mutation path, with
linear read work tested at 1,024 rows; mutation-plan
authorization and zero-I/O behavior have component coverage. ANALYZE and
fabricated cost estimates fail explicitly; the remaining original EXPLAIN
forms still need case-specific evidence.

Multi-row `INSERT ... VALUES` scalar subqueries now use the bounded INSERT-source
path, including self-table reads, typed literal coercion, and pre-commit
cardinality errors. The compiler, type inference, and execution binding now
use a flat VALUES source with ordered arms; cross-arm parameter inference uses
a balanced expression tree, not a UNION plan. Adjacent literal-only arms share
one typed row block, and only one nonliteral arm iterator is active at a time.
The 1,000-row test executes within 4 MiB and a 1 MiB admission limit rejects
before mutation. Grouped Top-K now caps its reservation by actual group count,
so a scalar source no longer reserves thousands of unused output slots.
In the same Debug SQL fixture, that INSERT's peak fell from 8.49 MB to
2.54 MB. The 512-row membership fixture fell from about 7.80 MB to
1.51 MB, and the ordered subquery fixture from about 7.47 MB to 1.23 MB;
these are local admission-memory observations, not throughput claims.
The original `sql-1410` self-read INSERT also has exact mounted HTTP evidence:
RETURNING reports its ID, and a subsequent typed read verifies the committed
status and quantity.
Table-reference parsing accepts ONLY across SELECT, INSERT, UPDATE, DELETE,
MERGE and TRUNCATE. With no table inheritance in this catalog it names the
same exact table; original `sql-1413` has mounted INSERT/RETURNING evidence.
Original `sql-0170` also has mounted parameterized SELECT evidence for
filtering, descending order and LIMIT over more than five matching rows.
The exact `sql-1531` point UPDATE also has mounted same-table scalar-source
evidence: it returns the target ID and a subsequent typed read verifies the
committed copied quantity. The joined-mutation component suite checks one
captured relational plan, cardinality, quota and read authorization.
The exact `sql-1532` parenthesized multi-column UPDATE has mounted evidence for
both committed cells. Explicit ROW and parenthesized tuple expressions lower
to the same simultaneous assignment plan; duplicate targets and arity mismatch
fail during compilation. A multi-column UPDATE row assignment now accepts an
explicit-width SELECT inside parentheses. It binds positional outputs through
one guarded mutation capture, checks scalar cardinality before commit, and
preserves the child's ORDER BY/LIMIT and aliases. A materialized typed row
producer supplies all positional assignments through one physical source scan;
SELECT * and correlation from inside the row source to the UPDATE
target remain outside this admitted shape. Scalar subqueries inside an
explicit ROW also remain supported.
UPDATE `DEFAULT`, including inside ROW, omits the old declared cell before
native normalization. A mounted relational test verifies the schema-provided
value in RETURNING alongside an incremented tuple member; relational and
document component tests preserve undeclared fields, keep one commit, and
abort without writes if native default preparation fails. A generated column
accepts only DEFAULT, so native preparation recomputes it; explicit values
remain rejected.
Joined relational mutations now carry a compact physical-presence token
alongside version and digest metadata. Projection alone maps both a missing
cell and SQL NULL to a null result; replacement images use that token to
preserve untouched omissions while explicit assignments retain their values.
A mounted tuple-UPDATE regression widens the table with an absent nullable
datetime and verifies the raw stored row remains sparse after commit, without
loading a full document preimage for every joined mutation.
INSERT DEFAULT VALUES and per-cell VALUES DEFAULT also omit cells for native
normalization, including mixed literal/scalar-subquery rows. The omission mask
keeps DEFAULT distinct from explicit SQL NULL through binding and RETURNING;
generated columns accept only DEFAULT and an omitted or defaulted `_id` uses
the native secure row-ID generator. Exact `sql-1494` has mounted evidence for
three schema-derived logical defaults and one affected row.
Typed `TIMESTAMPTZ '...'` literals use the datetime cast program, so offset
validation and UTC normalization happen before mutation admission. Exact
`sql-1496` has mounted native INSERT/RETURNING evidence. The adjacent
`sql-1495` `DEFAULT VALUES ... ON CONFLICT (id)` case now has mounted evidence
with an active coordinated UNIQUE claim: owner resolution selects the seeded
physical row, native preparation fences the claim, and RETURNING plus a
physical lookup verify the committed default-derived update.

Remaining major items include broader isolation deployment and fault validation,
broader correlated/mutation subqueries and MERGE, unrestricted recursion,
TRUNCATE external-FK generation retirement, graph dependency barriers and owned
sequence support, the full session-setting surface, and the complete parity,
fault-injection and workload benchmark gates. Passing focused component tests
is not completion of SQL extraction.

The mega-branch reference already implements parts of these gaps, but on its
older SQL adapter and native row-source contracts. In particular,
`sql/lower_dml.zig` and `api/sql_adapter_integration.zig` contain a literal-row
INSERT source with per-row scalar subqueries (including correlated and multi-row
cases); `api/table_writes/relational_mutation.zig` exercises recursive CTE
sources for joined mutations and MERGE; and `api/http_server.zig` plus
`api/auth_sql_adapter.zig` implement trusted role-setting hydration and native
row-policy checks. The old TRUNCATE path also parses CASCADE and RESTART
IDENTITY. These are behavioral references and test cases to port selectively,
not compatible modules to copy wholesale: the extraction now uses immutable
bound relations, retained statement capture, owner-fenced commits, and the
current catalog/constraint generations. No legacy adapter fallback should be
introduced to claim parity.

### Session catalog and policy-setting boundary: incomplete

The original corpus includes `app.*` session variables, `current_setting`,
role/database defaults, RLS policies, `RESET ALL`, and `DISCARD ALL`. A pgwire
string map alone would be unsafe: policies must not silently trust a value a
client can change. The target shape uses a versioned setting registry in the
SQL catalog with typed values, role/database defaults, explicit write authority,
and an immutable request/session view. Each statement binds `current_setting`
against that view alongside its schema epoch; native policy evaluation and SQL
scalar programs must receive the same pinned view, including remote readers.
Transactions and savepoints journal setting overlays, and RESET/DISCARD operate
on that typed registry, not a separate pgwire-only map. Prepared plans retain
setting dependency identities while evaluating authorized values at execution.
Publication needs policy tests proving that unprivileged SET cannot widen row
visibility, plus rollback, failover, cross-owner, and plan-invalidation tests.
Until then, supported pgwire-only settings remain connection-scoped and the
original `app.*`/policy/RESET ALL/DISCARD ALL cases remain unresolved.

A typed, scoped setting snapshot/view pins names, identity generations,
role/database defaults, and authorized session overlays. Constant and SQL
scalar binding evaluate `current_setting('literal.name')` from that owner-captured
view, including joined expressions and pull streams. Missing capture, stale
generations, dynamic names, and client overlays on policy-sensitive values fail
closed. Metadata Raft now owns durable setting records, revision-fenced
publication, snapshot/import state, and an administrator-only public mutation
route. The production SQL adapter obtains authenticated scoped snapshots; SQL
SET changes only an authorized session overlay, never the durable registry.
Attached HTTP `/sql` sessions now accept typed dotted-name SET/SET LOCAL,
RESET and SHOW, plus RESET ALL, through the same complete-command parser as
pgwire. RESET ALL atomically clears the active and commit-time durable overlays;
savepoint rollback can restore them. It clears stale identities without a
catalog read, leaving the next statement to capture fresh defaults. The owner
keeps `SET LOCAL name = DEFAULT` transaction-local: it removes only the active
override and preserves the durable commit-time overlay across savepoint
rollback and restart; pgwire's session value resumes after transaction end. The owner
checks principal, lease, catalog generation and policy-sensitive write gates
before a durable overlay mutation; SHOW and `current_setting` read the active
overlay, including after a separately prepared statement is executed in the
session. HTTP PREPARE accepts `session_id` and holds the session execution lease
while it authenticates the principal, hydrates the database/namespace and
durable active setting overlay, describes the statement, and publishes the
immutable prepared resource. Such resources bind the exact session ID and
scoped setting-catalog epoch: execute rehydrates current values under its own
lease, refuses a different or ended session, and rejects a changed catalog.
The focused mounted test covers SET-before-PREPARE, a competing SET admission
probe at the exact describe catalog capture, later SET-before-execute,
authorization/scope denial, epoch change, and cold API-instance rehydration;
the durable-store test covers restart of both resource and attached session.
Pgwire now
holds typed, identity-fenced dotted-name overlays with SET/SET LOCAL/SHOW/RESET,
transaction/savepoint rollback, RESET ALL/DISCARD ALL, and prepared-plan epoch
checks. Pgwire overlays belong to one connection and are not restart-durable.
HTTP now also has durable, principal- and owner-bound idle connections with
typed setting overlays. An active transaction is attached by exact durable ID;
committed non-LOCAL settings carry back to the idle connection, while rollback
restores its prior overlay. HTTP `DISCARD ALL` atomically resets the idle
overlay, advances its generation and retires only that connection's prepared
resources; it refuses active or uncertain transactions. HTTP exposes no
connection-owned SQL cursor resource. A focused in-process public-handler
regression covers open, SET/SHOW, BEGIN/COMMIT/ROLLBACK, DISCARD, prepared
retirement, close and cross-principal denial. Cross-owner failover/security
workload gates remain open. A durable setting registry alone
is not policy parity. Schema-bound policy definitions survive catalog Raft
replay, snapshot/import, and table retirement. SQL CREATE/ALTER/DROP POLICY
edits drafts only; ENABLE/DISABLE requests a separate durable owner
publication. Owner-native reads and writes have signed principal/generation
proofs and fail-closed gates, but unsupported search, restore, and mutation
routes cannot bypass policy enforcement or make the feature generally ready.
The native standalone owner exercises publication, proofless-read denial,
signed reads and restart recovery. A bound-primary hot-standby owner also
exercises ordered catalog/bundle/receipt replay into a cold owner, including
fail-closed reads before the active publication and recovery after reopen.
Promoted-primary public-route and broader fault coverage remain separate gates.

The native policy boundary must be catalog-versioned authority, not a SQL
projection filter. A policy record needs the bound table ID/schema epoch,
command and role scope, USING/WITH CHECK expressions, setting dependency
identities, and a publication generation. Every protected read owner must
receive an authenticated principal and immutable policy/setting view, apply
USING before pagination, and return an owner proof tied to the same read cut.
Mutation preparation must check the old image for UPDATE/DELETE visibility and
the normalized new image for INSERT/UPDATE WITH CHECK inside the guarded commit;
API, pgwire, Lite, remote reads, and non-SQL mutation routes must not be able to
select an unprotected backend. A missing/stale policy view or unsupported owner
must deny the operation. Policy DDL must publish durably before it can authorize
new traffic, and prepared statements must revalidate policy and setting
generations. Until every public route and revocation/failover/restore test is
complete, active policy publication remains guarded by capability checks.

Ordinary document reads obtain the principal-independent policy publication
stamp using authenticated internal-service identity, without requiring separate
publication credentials merely to prove that no policy is active. The client
and metadata server share the read-grant classification. Status reads still
reject absent or invalid service tokens in migration mode; policy snapshots,
installation, work discovery and mutations retain their stronger grants.
An active serving stamp still requires an authenticated principal and an
owner-verified policy proof. Missing credentials never substitute for an
absent policy, and missing publication credentials disable background polling
rather than repeatedly issuing unauthorized requests.

### MERGE mutation lowering: partial

The reference branch admits matched UPDATE/DELETE, NOT MATCHED INSERT,
ordered conditional arms, DO NOTHING, expressions, RETURNING, and CTE sources.
The compiler now owns a bounded MERGE AST (source relation, join condition,
ordered conditional arms, structured expressions and RETURNING). It rejects
invalid matched/action pairs, duplicate assignments, and mismatched INSERT arity.
Explicit MERGE DEFAULT cells are omitted from the candidate image so the
pinned native schema applies defaults during preparation. The candidate binder pins the
authorized target once, binds a source-preserving target/source join, retains version/digest
metadata, and projects only referenced source fields (and only needed target
fields for delete-only plans). It binds typed arm predicates and values, infers
their parameters, and selects the first eligible arm lazily under three-valued
SQL logic. The bounded capture classifier rejects duplicate source actions on
one target before any image preparation. A batch builder prepares
fenced UPDATE/DELETE/INSERT images, generated row IDs, document preimages,
typed JSON-null metadata, and retained-byte limits before native admission.
Classification now resets one row-local predicate arena between candidates;
the selected arm ordinal is the only retained result. An opt-in 10,000-row
`ANTFLY_SQL_MERGE_CLASSIFY_BENCHMARK=1` comparison measured 3.92–4.23 ms with
arena reuse versus 4.24–4.69 ms with one arena per row across three local runs;
both retained 240,048 bytes in the benchmark's parent arena. This is a
classifier microbenchmark, not an end-to-end MERGE throughput claim.
MERGE executes only through a backend that stages the coordinated
target/source range proofs with the mutation in one durable transaction. The
API uses an implicit serializable transaction for autocommit MERGE and the same
proof path in an explicit stronger-isolation session. Read-committed or plain
batch backends reject before opening the candidate read. Unknown decisions
retain a reconciliation ID and are never replayed automatically.
RETURNING binds target and source expressions against the same candidate
capture, prepares native postimages before publication, then projects the
normalized target values alongside unchanged source values. A dedicated
prepared-image commit path stages those exact values without normalizing them
again. Projection errors
abort before commit; a missing native preparation capability fails closed.
Only referenced target fields are projected for DELETE/INSERT RETURNING, while
relational UPDATE retains its complete rewrite image.

Small `target._id = source.key` sources now use a 129-row decision capture followed
by deduplicated primary-key point scans (up to 64 keys per native capture).
Every source and target observation joins the same serializable transaction
read set, including misses; a source with more than 128 rows falls back to the
source-preserving hash join. The decision capture stops as soon as the source
is too large for point probes, rather than materializing it before the fallback
reads it again. The optimized source projection reuses the first binding's
authorized table identities, avoiding a second catalog resolve or a different
schema observation within the same statement. The point path never scans target-only rows and
retains the same ordered-arm and mutation-image builder. The mounted API test
asserts a source-only merge opens one point scan and no full target scan.
The original inventory's 14 non-recursive, non-mutation-producing MERGE SQL
cases now have exact-text compiler and authorized candidate-binder coverage;
the first cross-table case also executes a matched update and source-only
insert through one atomic mutation capture. Its mounted API adapter test now
retains source and target range guards on distinct owner routes in the same
committed transaction under an owned read/write identity, rejects missing
source read authority before opening a scan, and maps a source-side conflict
and an ambiguous commit outcome without automatic replay. A lost source proof
aborts before commit admission. These are adapter
fault fixtures. The original `sql-0579` text also passes through the mounted
`/db/v1/sql` handler with two affected rows; HTTP conflict and ambiguous-outcome
responses preserve their distinct retry guidance, with a reconciliation receipt
on the ambiguous outcome. The durable transaction/savepoint round-trip also
retains a read-only source participant's distinct owner route and exact range
generation after target writes are rolled back. HTTP prepare/execute now admits
MERGE; its durable prepared manifest pins both source and target identities and
the original case executes successfully from that resource. This is not yet real
cross-owner failover parity.
The exact `sql-0585` corpus statement also executes through `/db/v1/sql`,
verifying lower/upper source expressions in matched-update and source-only
insert images within one guarded commit.
The exact `sql-0581` statement exercises matched and source-only conditional
arms through that endpoint, including an all-false execution that commits a
guarded read decision with zero writes.
The exact `sql-0584` statement also returns the matched row's postimage and
computed lower-case status from the mounted endpoint with one guarded commit.

Remaining: CTE mutation sources require an explicit single-statement
dataflow/commit model. The bounded direct full-key index probe now
uses a catalog-pinned index identity, native require-index equality scans,
exact span proofs for misses and matches, source-key deduplication and a
16-row nonunique fanout cap. Sources above 32 rows, saturated fanout, and
non-READY indexes use the one-pass coordinated join. This is a conservative
crossover heuristic, not yet a measured adaptive cost model. Conjunctions of
typed equalities may bind every key of a composite total index in physical
index order; partial and expression ON keys still use the full join. Native `auto_index`
alone cannot provide the probe's serializable guarantee: it may choose a
primary scan when the index is not READY. Native
explicit-index reads fail closed on non-READY state and now capture an exact
index-span proof for full-key equality; prefix/range scans retain all 257
conservative primary buckets. The bounded distributed probe planner consumes
this narrower proof for serializable MERGE.
The native proof must be a tagged index-span observation pinned to the READY
index generation and its encoded equality prefix, not a reused primary bucket.
Prepare must reserve writer keys for both prior and candidate tuple prefixes
before admitting a primary intent; the final forward/reverse index effects
must increment the same span counters atomically with the primary row. A read
captures the span counter and matching entries from one retained snapshot,
including an empty result. Commit validation must check the counter and any
in-flight writer reservation. Composite prefixes and partial-index membership
changes need the same old/new reservation rule. Merely incrementing a counter
when staged index effects are sealed would leave a prepare-to-commit phantom
window; merely comparing a counter would miss pending writers. The distributed
proof wire, savepoint merge, and owner-routed prepare must carry the index
generation/span identity and fail closed on non-READY or changed generations.
The write-side foundation now derives a durable exact-tuple span identity from
each forward-index key and increments its counter in the same DocStore mutation
as the index entry. Native LSM reopen and overflow tests cover persistence and
atomic failure. An unchanged index tuple still advances its span generation
when the authoritative primary row is updated; eliding the forward-key rewrite
must not elide a serializable reader's conflict. The transaction manager now
accepts explicit old/new tuple
reservations, fences index-span counter predicates against pending writers, and
checks ordinary staged forward-index effects against active readers before
publication. The DB prepare path derives old tuples from reverse companions
(including retired generations) and new tuples from canonical AROW under the
pinned index plan, deduplicating reservations before intent admission. READY
publication now waits for unresolved row intents to drain, without making idle
read-only sessions block index readiness. Reservation gathering is capped by
the transaction read-guard budget even with retired-generation churn, and its
typed tuple-key batch reuses one row's buffers across the prepared write set.
The native reader now captures one tagged, exact-tuple proof for a full-key
inclusive equality (including a miss) from the same retained snapshot as its
rows. Other index scans keep the conservative primary-range proof. The
distributed proof transport and savepoint merge retain index identity and
reject changed generations; owner prepare checks the exact counter, pending
writers, current catalog head, READY progress, and maintenance control.
Cross-owner/failover fault coverage remains for the new probe planner.
The SQL scan contract now admits an explicit full-key index equality only
inside a coordinated statement read. Its owner adapter rejects absent or
conservative proofs instead of silently switching access paths. The immutable
SQL schema cache exposes direct total index candidates; partial and expression
indexes remain ineligible until their implication and expression proofs are
bound. The coordinated multi-owner read set now retains mutation digests and
document preimages across its bounded page copies; losing either would defeat
MERGE's native version fence. The planner selects this path only for bounded
sources and a complete direct total-index equality; otherwise it retains the
coordinated full join.
An empty implicit or explicit transaction does not force the index probe into
the session's primary-order overlay; once the session has staged table state,
the planner retains the coordinated full join for secondary-index matches.
Primary-key point reads remain available through the staged-session overlay.
The SQL regression exercises duplicate-source cardinality, a saturated
nonunique probe, non-READY fallback, composite-key order, rejection of an
incomplete composite predicate, and the 32-row source crossover without
committing a truncated candidate set. Native storage also checks that a
compound-key miss conflicts with a concurrent matching insert.
A native LSM microbenchmark rejected a tempting sorted multi-get substitution
for the current 257 primary-bucket proofs. Across 64 full-range captures,
scalar gets took about 8.2 ms versus 12.8 ms batched with sparse counters,
and 18.3 ms versus 23.6 ms with all counters populated. The scalar path stays
in place; reducing proof cardinality and conflict granularity is the needed
architectural win, not wrapping the same 257 keys in a batch call.
Acceptance requires duplicate-match, NULL-ON, ordered-arm, conflict,
concurrent-insert, cancellation/unknown-outcome, and cross-owner fault tests.
Most source corpus MERGE cases remain unresolved pending case-by-case endpoint
and distributed-fault evidence. Exact `sql-0579`, `sql-0581`, `sql-0584`, and
`sql-0585` have mounted success, source-conflict and unknown-outcome tests;
the latter two cases return a reconciliation ID rather than replaying.
Exact `sql-0582` also commits a
version-and-digest-fenced matched DELETE with both source and target proofs.
Exact `sql-0583` and `sql-0589` retain both range proofs in one committed
serializable decision when their ordered DO NOTHING arms emit no writes.
Exact `sql-0590` returns the normalized source-only INSERT postimage from
mounted SQL and retains the same two-range guarded commit, source-conflict and
ambiguous-outcome contracts.
Exact `sql-0587` and `sql-0588` execute expression and grouped OR/NOT arm
predicates on matched and source-only rows through that same guarded path.
The read-only CTE source in exact `sql-0623` also executes through mounted SQL,
retaining its archived-source proof and target proof with the matched update;
its durable prepared resource pins both table identities and rejects a replaced
archived source before capture. Missing source read authority also fails before
capture. This does not admit data-modifying CTE producers.

### TRUNCATE generation retirement: hosted public activation verified

The empty-generation implementation reuses durable restore staging: reserve fresh
table/range identities, prove each new owner has no primary, document-artifact,
identity, integrity-claim or ordered-index records, validate the empty cohort,
fence and drain old owners, and publish the replacement metadata atomically.
It does not copy, export or scan the old table's rows. Admission and resumed
workers require current whole-table administrator authority; row-filtered
credentials cannot authorize truncation. Success requires known publication,
not merely acceptance of the background job.

This activation is for owners with Raft-bound generation-handoff receipt
authority. Native-only owners do not yet have an equivalent seal/install
receipt protocol. Admission now requires an explicit owner identity capability
for every selected source and untouched FK parent; missing/native capability
returns unsupported before a durable job is created. Recovered plans check
hidden destinations, sources and untouched parents while still validating;
an unsupported pre-cutover attempt follows durable cancellation, leaving old
rows and identities intact. A capability mismatch after irreversible cutover
is a distinct recovery error and must retain fences, not authorize cancellation.
Native admission/recovery regressions pass: unsupported pre-cutover jobs reach
terminal cancellation without changing old rows, timestamps or identities.
Hidden target capabilities use a closed, service-authenticated control read,
bound to the exact plan, scope, namespace and empty-generation bootstrap, with
a fresh read-index barrier. The multi-range regression ensures exact-group
control reads use an empty key rather than a nonempty range start. This does
not enable native TRUNCATE or relax public table routing.

RESTART IDENTITY uses that fresh owner generation. The SQL catalog currently
has no owned sequence or serial declaration, and generated row IDs are secure
opaque values, so no sequence counter exists to reset. Once owned sequences
are introduced, their new counter generation must be part of the same staging
plan and publication transaction.

Graph-index TRUNCATE has an owner-verified cutover protocol. Its scoped public
guard and test-only admission permit have been removed, and the rebuilt installed
CI binary has passed the strict-public baseline and fault cases. The old owner
closes graph reader, mutation,
maintenance, and worker-snapshot admission under its catalog/apply locks,
drains schedule pins, and persists a Raft-bound seal whose digest includes the
active source fence, old owner identity, locally verified graph configuration,
fresh generation, and staging-plan digest. The gate is reconstructed from
durable intent before optional graph runtimes start. The coordinator resolves
an uncertain seal reply by read-indexing the owner receipt; metadata requires
that exact digest for every old range before atomic publication. Graph plans
use only the new semantic configuration digest, not legacy raw-JSON matching.
Metadata treats graph `begin_cutover` as irreversible, so a late cancellation
cannot move a sealed owner into an unreopenable cancel phase; pre-cutover
cancellation remains available.

Focused tests cover graph read/apply pinning, worker pin drain, direct graph
write and maintenance rejection, lost seal reply, forged/missing Raft markers
and metadata receipts, duplicate seal replay, late cancellation, and a closed
gate after both pre-seal and post-seal restarts. Mounted public SQL tests require
202 admission without an override, an exact old-owner seal receipt before
publication, a fresh table/index with the old document absent, and cold data
restart. A mounted dependency-closed parent/child CASCADE
cutover now also passes: both new owners expose their Plan-bound durable handoff
receipts via read-indexed public routing before and after a cold data-owner
restart, and old parent/child rows remain absent. Both the baseline and
lost-seal/cold-owner recovery cases pass through the unguarded public route.

Planning and preflight now read a transactional authenticated retirement-set
summary instead of scanning historical tombstones. A compressed binary Patricia
tree binds immutable generation authority; activation, imported handoff records,
and tombstone GC update it in the same transaction as the affected records and
applied marker. Native checkpoints retain both, semantic HA replay reconstructs
both, and portable mapped restore does not import either old-generation authority.
Ordinary batches and prepared transactions cannot edit summary keys. A missing
root is accepted only after one bounded seek proves there are no tombstones;
there is no legacy scan fallback. Live admission scopes remain bounded and
deadline/cancellation checked. Seal retries read only the durable fence, intent,
and seal receipt. GC and handoff page admission conservatively charge the maximum
compressed-path write cost, even where a transaction can coalesce shared nodes.

`antfly-retirement-summary-test` passes 18 tests with no skips or leaks, including
abort/reopen, idempotent owner transfer, missing-root refusal, two-phase GC,
canonical insertion order, branch collapse, snapshot copy and worst-prefix depth.
The native Debug WAL benchmark makes the cost explicit: 64 single-record commits
use 130,124 insert bytes versus 56,000 without the tree (2.32x), and 73,075 GC bytes
versus 7,104 (10.29x). At 512 records in batches of 64, insertion is 741,238 versus
437,920 bytes (1.69x), and GC is 272,162 versus 46,752 (5.82x). Ten thousand summary
point reads took 14.27–16.53 ms locally. This is a bounded-read/progress tradeoff,
not a write-amplification improvement or a general latency guarantee.

The new mounted fault fixtures additionally pause after real parent activation,
parent ACK, or graph seal and cold-reopen the data owners before replaying the
original job. The graph fixture now requires a real public-write edge to be
visible before truncation and reuses both endpoint keys after restart to expose
stale adjacency. The installed `antfly-hosted-truncate-fk-recovery` CI binary
passes 16/16 tests with no skips or leaks: five real mounted workloads, two
focused unit regressions and nine import anchors. The strict-public workloads
include external-parent DROP (28.55 s), TRUNCATE baseline (31.96 s) and fault
recovery (28.62 s), and graph baseline/CASCADE (34.86 s) and fault recovery
(6.69 s), after the scoped capability routing fix. The relational API unit
target also passes 69/69, including both CASCADE admission cases and refusal
of missing parent admission proof or native receipt authority before durable
admission. Earlier permit-based cached repetitions passed
12/12; these are additional soak evidence, not substituted for the installed
strict-public run. The installed strict-public binary also passed three further
repetitions of all four TRUNCATE baseline/fault cases (12/12, no skips or leaks).
Timings are local Debug observations, not latency guarantees.
A separate prebuilt `fk-truncate` CI recovery lane selects these tests with the
existing hard watchdog and retained logs; it does not build native code on the
recovery runner or extend the initial-FK/self-FK lanes.

Incoming-FK CASCADE selection must never truncate an outgoing parent implicitly.
The current implementation includes untouched external parents in the fenced
retirement protocol without replacing their table generations, with mounted
strict-public baseline and fault validation. Untouched parents
retain inverse references containing the old child's constraint generation.
Ignoring a missing generation in the SQL/API
coordinator would be unsafe because native RESTRICT checks, participant commit
validation and referential-action cursors also consume those references.

Existing constraint retirement is correct but row-oriented: it scans children
and joins individual reference detaches to ordinary 2PC. The legacy retirement
worker still admits non-FK generations, including UNIQUE,
with durable page/restart proofs. It deliberately rejects an FK-removing target
at admission: its ready checkpoint does not carry the parent-owner ACK and
generation fence required to publish a changed child FK schema. FK retirement
must use the coordinated publication protocol rather than treating a drained
child claim as that missing cross-owner proof. The generation-level path avoids
copying parent data through this shared lifecycle:

1. Pin and reserve untouched parent definitions/ranges and exact old child FK
   generations in the staging plan. Fence and drain affected parent owners as
   well as old child owners; no schema or ownership change may invalidate this
   participant set.
2. Durably stage generation-specific inverse-reference tombstones on those
   parent owners. Pending tombstones must not change reference visibility.
3. Obtain the irreversible metadata activation decision, then activate only
   the corresponding accepted-generation scopes while retaining parent write
   fences. Publish the new child generation only after the required activation
   receipts are durable; a metadata-authorized parent ACK after publication
   releases those fences. Cancellation may remove pending state before the
   irreversible decision, not merely at any time before publication. Once that
   decision is durable, recovery must complete activation, publication and ACK.
4. Apply the same generation interpretation to native prepare and commit
   validation, RESTRICT/NO ACTION, witness checks, action cursors, and attach
   admission. Old-generation attachments remain forbidden. Existing prepared
   transactions must drain before activation, not be bypassed afterward.
5. Reclaim obsolete reference records with resumable bounded GC. Current
   reference keys hash the child generation with row identity, so they cannot
   be dropped using one generation-prefix delete. Foreground skipping must
   retain explicit work/cancellation budgets and cannot silently truncate a
   live-reference search.

The owner-local pending record for step 2 is checksummed and binds an exact
topology fence, plan digest, child table, and old/successor FK generations.
Restore plans pin untouched external parents and require durable parent fence
receipts before child cutover. The owner re-fetches an irreversible metadata
activation decision before committing a replicated accepted-generation scope;
range handoff carries that scope, and bounded resumable GC skips retired
references. The SQL child-only external-parent TRUNCATE route is publicly active
and verified by the installed recovery binary.
The parent activation step now retains its owner fence across restart, and
generic release/cancel cannot lift it; only a metadata-authorized ACK after
child publication releases admission. This closes the pre-publication orphan
window. The mounted fault fixture has passed cold-owner recovery after actual
activation and ACK, with the original job resumed rather than SQL resubmitted;
the installed strict-public run confirms this without an admission permit.
Ordinary FK schema edits now have a metadata-owned parent/source publication
and ACK protocol, while FK-bearing initial CREATE uses hidden child owners and
publishes the table only after parent and child receipts. Standalone initial
self-referential FK CREATE now uses authenticated native hidden-child owner
receipts and an atomic local catalog publication; a two-range restart fixture
and public valid/orphan-write fixture cover this path. The ordinary FK
publication supervisor selects nonterminal work through a durable fixed-width
active index instead of parsing historical publications on every tick; the
index and phase transition share one metadata transaction and snapshot.
Standalone initial FK
CREATE with external parents and initial MATCH PARTIAL declarations remain
guarded until their owner or atomic support-index paths are proven. Hidden-owner
standby replay has a Raft-bound batch envelope, but seed/promotion fault
coverage remains a release gate.

Hosted external-parent FK DROP is routed from SQL DROP CONSTRAINT or the REST
schema update through that same publication plan: an old-to-null generation
transition fences and drains child owners, obtains staged/activated/ACKed
parent-owner receipts, publishes the child schema at metadata, then installs it
with an exact Raft-bound source receipt. A focused state-machine test covers
DROP ordering across serialized restart. A mounted public SQL ADD/DROP workload
has passed parent-ACK-before-child-publication, valid child insert and blocked
parent delete before DROP, terminal DROP, cold owner restart, and permitted
parent delete afterward. A later repetition stalled on the pre-DROP parent
DELETE at Raft target index 10/applied 9; repeated stable completion remains a
release gate. The native parent-action lookup filters retired
inverse references before planning while scanning only a bounded physical
page; an empty retired-only page still advances its continuation to later live
references. GC currently scans routing-first reference keys, including live
records; high-churn retirement needs a bounded-scale proof or an atomically
maintained generation locator covering attach/detach, GC, restore, and range
handoff. `GenerationRetired` remains a typed pre-decision conflict rather
than poisoning Raft apply. The post-publication generation-GC poll uses a
cursor-capable snapshot read; a point-probe transaction previously returned
`Unsupported` before any GC page could be prepared. A byte-equal child index
catalog is not a valid publication invariant because a schema-version bump stages
`full_text_index_vN`; the plan re-derives the exact successor catalog using the
candidate's fresh index incarnation and rejects unrelated index edits. A
subsequent mounted DROP rerun stalled with a data-Raft apply index one behind
the requested index during pre-DROP parent DELETE; that intermittent failure
is still under exact-entry investigation, so the first pass is not soak proof.
The pure index validator currently lives in `api/tables.zig` and is called by metadata
plan validation; moving the shared index-derivation helpers to a
metadata-neutral schema module remains layering cleanup.
Distributed ordinary self-referential FK ADD/DROP is enabled through the
public SQL API. A plan binds each dual-role child/parent owner once and retains
one Raft-persisted fence through parent stage, activation and ACK. The child
schema, integrity catalog, accepted generation, Raft marker and fence release
are installed atomically after metadata publication. The public contract
remains 202 while this durable publication is pending. The standalone native
owner now supports ordinary FK generation publication and retirement through
the same durable owner metadata controls; focused native regression covers its
activation and replay. This is not a claim of mounted standalone fault soak.

Mounted regressions cover ADD enforcement, rejected parent deletion, DROP
retirement and allowed deletion, cold owner restart, lost owner/metadata
replies at each phase, simultaneous metadata and owner cold restart at parent
ACK, and three-voter owner leadership transfer during ADD and DROP. The
three-voter DROP initially stalled at parent stage because schema-migration
finalization cleared the old read layout and retired its versioned full-text
index after the immutable DROP plan was admitted. Metadata FK table locks now
distinguish generation publication from initial-FK support reservations:
only the latter may finalize its exact schema migration under lock. The
generation command requires an all-voter/learner v19 decoder proof both before
admission and at final Raft append, including a stale-term/membership check.
The corrected three-voter mounted test passes through both ADD and DROP ACK.

Focused storage regressions cover old-transaction drain, duplicate Raft
entries, active dual-role fence and parent ACK across cold reopen, atomic child
install, and standby replay, restart, handoff and promotion. Public writes
refresh an authoritative relational catalog and replan on a proven prepared
generation change; document-table cached writes retain their fast path.
Pre-decision `IntegrityTopologyBusy` is a retryable 409, while ambiguous
writes are never replayed. CI includes the mounted self-FK recovery binaries
so the full activation matrix continues to exercise these paths.

Initial MATCH PARTIAL support-index installation now reserves the parent
descriptor, hidden child identity, locks, and durable work in one metadata
transaction. The hosted path now seals parent support, places a private child
group, and admits a local Raft leader from a paired public/private metadata
cut. Hidden-child topology proposals now carry an exact private compiled-owner
descriptor through Raft instead of resolving an unpublished public table, and
skip the public dense-repair admission probe only for that no-document-write
control. Public CREATE now requires and returns 202 in the mounted fixture,
reaches `published` with durable child provision/release and parent
stage/activate/ACK receipts, and survives a data-owner restart. MATCH PARTIAL
valid inserts, orphan rejection, parent-delete protection, and restart reads
pass. Separate mounted tests pass lost owner/metadata replies at each driven
transition, cold restart of both metadata and data before child release, and
three-voter hidden-child leadership transfer before release with all physical
roots enrolled. The initial-CREATE public guard is removed. The linked metadata
facade now exposes the exact initial group reservation; native boundary and
concrete service/facade capability tests prevent silently emitting ordinary
placement commands for reserved hidden owners.
Standalone cancellation has an exact hidden-owner retirement path and a
checksummed local intent. Its self-FK two-range crash/restart and terminal
cold-root tests pass. Hosted retirement now uses the distinct metadata-owned
authority and offline-store machinery below, including mounted offline-rejoin
fault validation.

#### Hosted retirement activation update

Metadata retains a plan-wide-bounded history of every admitted hidden replica,
including placements removed before cancellation.
Publication retains obsolete replicas for cleanup while excluding current
physical group/node/store slots. Tickets distinguish canceled plans from
published-obsolete roots; neither authorizes unlinking a current published
owner. Paging and ACK recheck the immutable plan and current placement in one
metadata transaction. Re-admission to a retired physical slot waits for its
unlink ACK even when replica labels or root generations change.

Authority is explicit administrator enrollment, not ordinary service
registration. OpenAPI routes and generated clients expose
`POST /store-roots/enroll` and read-only
`POST /store-roots/enrollment-status`. The CLI produces an existing root's
signed proof, submits it, and queries its exact identity. Metadata persists
one immutable verifier for the node/store/root identity. Signed retirement
paging and ACKs bind the cluster, store, root, ticket, and request cut. Hosted
child provision/release receipts carry a root attestation checked against
enrollment, the immutable plan, and current placement. These initial-FK
contracts require protocol v19 on every metadata voter and learner; ordinary
mixed-version store registration retains its older encoding. See
[store-root enrollment](store-root-enrollment.md) for operational details.

The worker verifies an exact cold owner bootstrap and persists a checksummed
local intent before quiescing the owner, Raft, readers, writers, and snapshots.
It can cancel a private owner offline without quorum, rename only the verified
root to trash, fsync, remove only the matching local replica catalog entry, and
retry a signed ACK after restart. Prepared/unlinked intents protect replacement
roots across crashes. Permanent generation fences prevent stale rejoin.
Trash GC is separate from acknowledgement: inode-bound, no-follow traversal
persists its cursor and limits entries, deletes, depth, and elapsed time per
slice. Tests exercise real fsynced files and cold-proof callbacks, with a
separate native LSM cold-cancel/reopen test. The mounted three-node canceled
CREATE test also passes: a provisioned follower goes offline and loses its
placement, the coordinator terminally cancels with injected lost replies, and
the same physical root returns. The real signed-page worker cold-cancels and
unlinks that exact root, signs an ACK, and retains its resurrection fence;
the existing public parent survives and the canceled child stays invisible.
The passing mounted matrix comprises
`antfly-api-hosted-initial-fk-test`,
`antfly-api-hosted-initial-fk-fault-test`,
`antfly-api-hosted-initial-fk-transfer-test`, and both retirement cases in
`antfly-api-hosted-initial-fk-offline-test`. The published-obsolete case removes
an offline follower after its release but before metadata publication, then
rejoins that same root while draining. Its distinct signed ticket retires the
released obsolete root without cancellation; the surviving current child
owner remains released and public, with no retirement ticket. Both offline
cases verify actual root disappearance, acknowledged discovery removal, and
the permanent exact-generation resurrection fence.
PR and main E2E gates build the initial and ordinary/self-FK proof binaries
once with the shared native archives, then run them in two separate recovery
lanes. Each prebuilt binary has a 15-minute hard timeout and retained logs;
recovery runners do not rebuild native code, and these fixtures are not added
to the unit-test aggregate.

Raft membership admission uses the exact native applied/pending/retained
configuration cut. An unapplied merge acquire fences membership; an unapplied
release cannot unfence it, and a retained-log gap fails closed. Forwarded,
batched, and automatic joint-consensus exit proposals share this admission
callback instead of relying only on the administrative HTTP route.

The UUID and Ed25519 seed identify a root lineage, not unique hardware. Native
shard backups and default standby seed capture exclude the outer identity.
An operator's full-root copy can clone that identity and key; enrollment is
not hardware attestation or cloned-disk detection. Replacement roots require
explicit enrollment and placement approval, not trust-on-first-use. Public
initial-CREATE activation has mounted publication, restart, hidden-owner
failover, and canceled-offline retirement evidence, not retirement unit tests
alone.

Acceptance needs crash/lost-ack tests at each fence, publication and activation
boundary, cancellation on both sides of publication, parent mutations and new
child inserts during cutover, stale prepared participants, nullable/MATCH PARTIAL
witnesses, and GC/restart bounds. No disconnected tombstone contract or
coordinator-only filter constitutes completion of this boundary.

### Distributed online merge: ordered artifact authority

Online merge admission now carries an ordered artifact catalog in the same
data-Raft command as donor admission or the receiver's initial acceptance. There
is no separate, unfenced catalog-seal window. Metadata pins immutable exact
bindings for both owners; native apply validates namespace, prior catalog epoch,
source/receiver scope, integrity catalog and topology before it can reconcile
local materialization. Data protocol v14 and metadata protocol v20 now gate the
complete ordered artifact decoder, including typed-vector transfer, before even
an empty first page is admitted. Eligible document owners automatically support
row-derived fulltext and direct-field dense/sparse indexes. Graph, generated
vectors, enrichment and resolver tails remain explicitly ineligible.

All ordered eligible owners capture document-owned base embeddings, including
historical values without an active index and fulltext-only tables. Snapshot
objects preserve exact base-artifact bytes; checksum-bound, restartable chunks
carry large dense/sparse values. Retained vector effects and their Raft marker
commit together; prepared transactions reserve artifact bytes before commit.
Receiver application records both positive and deleted vector effects in the
derived replay journal, so coverage and projection readiness share the durable
commit. Direct-field repair reads primary rows and same-snapshot artifact
fallbacks, excluding artifacts whose document no longer exists.

Sparse replacement preserves stable document ordinals using monotone
incarnations and point-addressable immutable posting/docmap sidecars. Queries
filter stale postings before score accumulation; compaction carries captured
incarnations and safely tolerates later updates. Legacy segments remain
readable. Query caches are bounded, and compaction reserves its source,
incarnation and output working set before cloning source data. Native checkpoint
capture also waits for in-flight immutable flush publication before certifying
the source cut.

Each binding includes an exact physical catalog digest and a separate canonical
logical-definition digest. Cross-owner compatibility ignores insertion order,
absent versus serialized-empty catalogs, JSON object key order and owner-local
coverage generations. Configuration differences still reject admission. Each
owner retains its own exact ordered generation, so a healthy receiver does not
rebuild its indexes merely because the donor has independently assigned
generations. The source/page identity and durable source receipt include both
digests; command validation recomputes the logical proof from the actual catalog.

Replica-local drift does not determine the replicated state-machine decision.
After semantic preflight, native apply persists an exact reconciliation intent
and retries bounded work until the intended catalog is physically ready. The
intent fences unrelated schema, artifact, topology and row-policy publication;
explicit private reconciliation contexts permit only monotonic changes toward
that exact intent. Fulltext materialization uses the existing managed shadow
generation scheduler. Obsolete resolver artifacts use a durable, restartable
128-key cleanup cursor, with producer publication fenced against writes behind
the cursor. Final admission, ordered authority, applied receipt and intent/cursor
removal commit atomically. Failed or repair-unavailable indexes and outstanding
retired-artifact cleanup cannot certify readiness.

Native same-owner snapshots preserve pending reconciliation. Portable export,
backup capture and restore publication reject a pending intent rather than
export a partially reconciled catalog; a new namespace cannot inherit another
owner's ordered authority. Catalog-preview changes before cutover durably cancel
the metadata attempt instead of retrying an obsolete preview indefinitely.

Focused regressions cover stale-command rejection before mutation, invalid
source preflight, explicit-context and low-level catalog mutation rejection,
standby replay, populated fulltext repair, restart during multi-page resolver
cleanup, late producer rejection, backup/restore fences, semantic compatibility
and forged proofs. The mounted three-metadata/three-data-node regression
`test_online_merge_recovers_after_owner_link_outage_and_crash[accept-accept_reply_restart]`
passed against a freshly built Debug CLI. It verifies default online selection
with the normal fulltext index, both immutable artifact bindings, loss of a
successful ordered receiver-accept reply, responder restart, an exact idempotent
retry, and the durable source binding and initial page digest/head after
`begin_copy`. After ordinary completion and source release, all original rows
(including a 2 MiB document) remain publicly readable and the copied fulltext
document appears exactly once. The test uses transport faults, not forced
unfencing. This is one mounted fault case, not a repeated full fault matrix.
Public table metadata assigns a shared index incarnation across ordinary shards;
independent-generation compatibility and a crash during active reconciliation
are native regression cases, not artificially injected mounted catalog drift.

The direct-vector native suite passes 15 tests. Five additional cached receiver
soaks passed (65 tests total), including full-index and default-sync projection
scores, positive-tail journal membership, duplicate chunks, owner restart,
historical sparse values larger than 16 MiB, historical dense values exceeding
65,535 dimensions, and fulltext-only latent artifacts. Debug receiver timings
were 9.30–9.84 s (large/full-index), 5.06–5.43 s (default sync), and 2.27–2.44 s
(fulltext-only). These are correctness fixture timings, not throughput claims.
The sparse compatibility suite passed 23 tests, including legacy replacement,
late-update compaction, reopen, corruption and memory-denial cases. Dedicated
compaction/selective-query performance comparison remains unmeasured.

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
  inventory alone is not parity completion. See [the inventory guide](sql-parity-inventory.md).
- Seven original TRUNCATE cases now have SQL-neutral IDs, explicit supersession
  rationale, exact-statement admission tests, and real empty-generation owner
  publication/restart evidence. `make sql-parity-evidence-check` runs referenced
  gates before the full corpus is resolved: the focused public API TRUNCATE
  suite passes eight tests without leaks and the staged-owner rewrite/empty-
  generation driver passes two. The remaining 1,395 case dispositions still
  block release.
  The graph-index guard now inspects only selected tables after FK closure,
  so an unrelated graph table neither blocks admission nor incurs index-JSON
  parsing; a graph-indexed CASCADE participant remains guarded. Unknown target
  names fail before the dependency walk parses unrelated table schemas.
- All fourteen currently referenced `make sql-parity-evidence-check` gates pass.
  The current SQL suite passes 227 tests, including the VALUES-subquery and
  observed-group Top-K admission cases.
  The dry-run EXPLAIN and mutation-subquery follow-up passes exact mounted
  text/JSON cases and a write-permission denial test; its test backend
  rejects any accidental row scan or mutation. `make fmt-check` and the
  parity-checker unit suite also pass.
  The inventory validator now prevents a behavior-required original case from
  being marked `rejected`, or an original rejection from being marked
  `implemented` without a tested `superseded` disposition. This protects the
  release gate from status-only waivers; the 1,459 unresolved cases remain
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
  arms and a read-only CTE-backed MERGE have exact mounted coverage; 1,459 cases
  remain unresolved.

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
outside SQL extraction, as specified in the restructuring plan.

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

## Original SQL extraction work list (see integration status above)

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
   coordinator ownership. Sessions, transaction/savepoint state, retained
   readers, HTTP `session_id`, prepared identity fences and reauthorization are
   implemented for admitted native providers. Stronger-isolation deployment
   across every provider and its complete fault coverage remain unfinished.
5. Pgwire transaction status/session ownership; Lite/C ABI, interactive CLI and
   Antfarm SQL workbench. Production listener configuration and lifecycle are
   implemented, including explicit transactions on supported native owners.
   Bounded scroll/hold cursors are implemented; the full session-setting surface
   remains incomplete.
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

The focused tests in this slice do not satisfy the complete SQL extraction release gate.

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

## Durable range protection implementation boundary

The implementation now includes replicated activation, native bucket counters,
pending-writer reservations, retained-read/RPC proof export, durable session
observations and owner-fenced distributed prepare. Savepoint rollback keeps read
observations while discarding staged writes. Versioned private prepare envelopes
make older receivers reject guarded requests instead of ignoring their proofs.
The transaction suite passes 96 cases, including allocation-failure coverage for
guard ownership and durable savepoint round trips.

Tracking is inactive by default: inactive native batches perform one activation
probe and no counter writes. Active batches update each touched bucket once.
The initial 257-bucket layout is conservative: common-prefix keys share a bucket,
so it is not an adaptive low-contention interval index. TTL-enabled reads reject
guarded snapshots because clock-driven visibility needs an explicit transaction
time contract. Hosted owners route the idempotent activation command through
each catalog-fenced data-Raft group; a failed partial activation is safe to
retry, but it is not an atomic table-wide epoch switch. Guarded Lite sessions
remain unsupported: the local source does not advertise the capability until
session invalidation across in-place restore and the local owner/commit path
have end-to-end fault coverage.
The acceptance checklist below remains the release gate for the broader shape,
not a claim that every deployment or fault workload has been completed.

Remote retained reads now mount a service-authenticated owner protocol, with
catalog-scoped capabilities, bounded owner leases, sequenced pages, cancellation,
and exact typed-row metadata. Distributed statement capture admits each selected
leader before freezing any participant, then validates fresh quorum observations
against the frozen applied index and leader term. Quorum validation never waits
for Raft apply while holding an apply freeze; contention, a newer committed index,
or a leader/incarnation change aborts the statement. These are statement snapshot
guarantees, not durable serializable transaction protection.

Repeatable-read/serializable admission requires capable read and write providers;
the full deployment contract requires all of the following together:

1. Activate tracking through an explicit replicated catalog capability transition
   under the native apply mutex, after draining/rejecting existing prepared
   writers. A guarded read must reject an inactive database; it must never enable
   tracking through an unreplicated read-side write. This avoids unconditional
   write amplification for document/vector workloads that do not use SQL isolation.
   Persist bounded logical-primary-key bucket generations in the same native
   transaction as primary changes. Instrument `DocStore.Txn` and `BatchTxn`
   put/delete/append paths, covering document and relational rows, FK actions,
   TTL deletion, transaction resolution, and bulk restore. Increment each changed
   bucket once per transaction; never derive predicates from physical LSM runs.
2. Capture bucket-generation tokens from the exact native read transaction and
   export them through retained cursor/RPC pages. Bind tokens to table identity,
   schema, and a durable data incarnation. An empty range must produce tokens too.
3. Extend existing durable exact-value predicates and shared read guards to these
   bucket keys. Pending writers must reserve affected buckets before a concurrent
   reader prepares a shared guard; checking only committed generations permits a
   prepare/commit race. Use a separate shared writer-reservation namespace, so
   unrelated concurrent writers in one conservative bucket do not acquire an
   exclusive bucket intent and serialize unnecessarily. Every ordinary writer
   must check reader guards. Acquire bounded, sorted bucket sets rather than a
   global table mutex.
4. Retain and deduplicate tokens in the transaction session, including savepoint
   rollback, and route their validation/read-guard acquisition through the same
   atomic participant prepare as writes. Preserve original snapshot identity
   across subsequent statements, rather than silently opening a fresh snapshot.
5. Fence restore publication, split/merge, ownership movement, and database reopen
   with durable incarnation rules so counters cannot reset or transfer ambiguously.
   Distributed restore already allocates target table/range identities and carries
   the metadata incarnation; reuse those fences rather than generating divergent
   random epochs on individual replicas. In-place CAPI restoration needs explicit
   native session invalidation before enabling guarded CAPI transactions.
   Recovery must resolve shared guards and pending writer reservations together.

Required acceptance coverage includes empty-range phantoms, pending writer versus
reader-prepare races, FK/TTL mutations, restart/recovery, restore and topology
changes, cancellation and lost prepare responses. Fixed logical buckets trade
bounded metadata/locking cost for conservative conflicts; benchmark write
amplification and false-conflict rate before selecting the bucket granularity.
Adding counters alone would add write cost without providing the missing guarantee.

### Ordered artifact activation gate

The expanded asynchronous-artifact protocol is still **not advertised**. The
current implementation must not be treated as full feature activation. The
following mechanisms are installed behind that gate:

- Compact, owned publication commands and bounded replicated upload chunks.
  Finalization authenticates chunks outside the apply lock, then rechecks the
  manifest root and original begin index in the final transaction. Effects,
  provenance, semantic receipts, terminal transport acknowledgement, quota
  release, and chunk retirement share that transaction. Standby replay carries
  the small upload control, not another copy of the assembled publication.
- Transport acknowledgement storage is a fixed 1,024-entry durable ring, with
  constant point-addressed retirement per decision. Eviction removes only a
  retransmission cache entry; the durable semantic receipt/guard path remains
  authoritative. Active uploads separately retain their count/byte quotas.
- Large dispatch jobs yield after two chunks, retaining one queue admission,
  the original owned command, and their accepted chunk position. Scheduler
  pressure preserves progress; shutdown drains ownership. The compact command
  is not decoded or hashed again for each scheduling slice.
- Producer admission leaves two job slots and 16 MiB for activation controls
  in both the local queue and durable upload store (within their existing
  total limits). The upload root authenticates its control class; assembly
  verifies that class against the decoded command. Restart reconstructs the
  same quota partition, and retirement releases the exact class's reservation.
- Fully staged uploads survive loss of their finalize job. Each chunk updates
  an incarnation-bound, checksummed receipt bitmap in the same transaction as
  its bytes. Recovery examines at most eight manifest/bitmap pairs, never the
  payloads, and queues a 133-byte recovery hint through reserved control capacity.
  It releases the snapshot before dispatch, retains its cursor on refusal, and
  fairly wraps ready uploads at the idle maintenance cadence. Finalization still
  authenticates the complete command; a stale begin-index hint cannot consume a
  pruned/recreated upload. Terminal retirement and pruning also remove the bitmap.
- Incomplete uploads have a bounded idle detector (eight observations, five
  minutes without chunk progress). Its clock chooses proposals only. Ordered
  abandonment compares the incarnation, root and authenticated progress in the
  writer transaction, refusing retirement after a racing chunk or completion.
  Restart and clock regression restart the grace period; ready retries alternate
  with idle retirement so pending coverage cannot strand upload capacity. No
  semantic receipt, terminal success, or producer completion is manufactured.
  Lost producer output still requires the required-stream execution driver to
  regenerate it from the pending exact-input obligation.
- Root-producer discovery now appends immutable-catalog requests to the existing
  replay journal, atomically with a checksummed per-obligation dispatch sequence.
  Scheduling leaves semantic obligations pending. Primary/dependency re-dirtying
  resets dispatch even at the same primary position; stale catalog/input guards
  cannot consume work. Bounded, budgeted pages release their read snapshot before
  admission, keep a fair local cursor, and back off after refusal or a full sweep.
  Reopen tests distinguish durable dispatch from completion. Scoped downstream
  enumeration and completion callbacks still belong to the gate below.
- Pending root streams now have bounded, proof-aware retry sweeps independent of
  transport lifetime. Each receiver verifies the immutable plan's provider
  requirements outside the writer, skips current accepted live/absent output,
  and atomically appends only pending requests with a separate durable retry
  cursor. Accepted pages advance metadata without empty journal records. The
  original dispatch cursor, obligation revision/count and completion evidence
  remain unchanged. Checksummed owner-local round IDs prevent repeat admission
  within a sweep; input/dependency changes invalidate stale prepared retries.
  Per-document pages rotate fairly and preserve their ordinal across passes.
  Persisted scheduling time prevents cold-owner churn from continually resetting
  the five-minute retry grace; wall-clock regression may retry early but grants
  no acceptance. Runtime deadlines use the owner's awake clock. This repairs
  lost root requests even before upload Begin, so no extra transport-to-document
  reverse index or mass dispatch reset is needed. Tests cover lost queues,
  reopen, aborted retry transactions, stale inputs/rounds, bounded accepted
  prefixes, absence, same-byte output replacement with a surviving receipt,
  record corruption and retry backoff. Complete scoped callbacks, producer
  fault recovery and all-member completion remain separate activation gates.
- Authoritative coverage markers/counters are scoped by namespace, producer
  epoch, and catalog digest. Replica-local legacy markers are not scanned into
  distributed authority: each epoch initializes empty counters transactionally
  and credits only ordered publications. Statistics capture the authority and
  counters in one atomic point read, with bounded retry across epoch changes.
  Missing counters in populated authoritative generations remain pending, even when a
  publication would leave its coverage marker unchanged. A pending finalization
  retains its upload for retry rather than creating a terminal acknowledgement.
- Raft baseline discovery prepares owned, bounded pages outside apply (128 keys,
  64 KiB or 2 ms, allowing one oversized key up to the cursor limit). Pages bind
  the observed Raft cut, expected cursor, physical row keys and fixed upper bound.
  Followers apply the selected page instead of scanning their local work records.
  Cursor progress, current-input obligations and the Raft/outbox cut commit
  atomically. Foreground mutations capture behind-cursor and new tail writes;
  the fixed bound prevents continuous inserts from extending the migration.
  Queue admission is not completion, and a missing dispatcher cannot advance it.
- Dirty work is keyed by namespace and authority epoch. The scheduler work-page API
  returns owned pages with exact input revisions and epoch-fenced cursors;
  the snapshot is released before work can be dispatched. Both current-work
  scans and obsolete-epoch reclamation are capped at 128 records, 64 KiB, or
  2 ms (one oversized key may advance). Reclamation rechecks the authority
  before committing and never changes active pending counts or grants seal.
- Strict current-stream verification resolves accepted provenance by receipt
  identity, revalidates its complete logical read-set, and checks every output's
  revision witness without reading large artifact bodies. This is deliberately
  distinct from retry acknowledgement: shared graph outputs can be legitimately
  superseded and need projection reconciliation, not repeated inference.
- Accepted producer proofs now also stage a document-ordered reference in the
  same atomic apply, including publications whose output is absent. Variable
  key construction occurs before the apply lock; cold epoch retirement recovers
  the document from the bounded proof rather than duplicating long document
  keys on the write path. A bounded, two-phase maintenance page verifies old
  references and document entries outside its writer transaction, then retires
  them with reference counts and proof bodies under an authority CAS. This is
  the range-seekable source evidence index
  needed for bounded snapshot export, not itself a portable proof stream or a
  receiver-local adoption certificate. A pinned, document-range proof-reference
  reader now seeks directly to encoded binary lower bounds, pages by entry and
  byte limits, and checks each index against its live source reference and
  proof-body presence. It is candidate enumeration, not portable validation of
  the proof's causal input/output scope. The ordered-artifact target
  passes, including allocation faults, LSM apply, 129-proof paged retirement,
  binary range/resume, corrupted-reference rejection, pinned-reader preservation,
  and current-epoch isolation.
  Accepted proof bodies now use APF3 v3, a compact checksummed binary record of
  the same bounded logical read set, historical output compare-and-swap guards,
  and output digests. Those output guards are kept distinct from causal inputs:
  receiver adoption must compare the imported postimage, not replay the donor's
  pre-publication guard. APF3 removes JSON's
  binary-key expansion so one legal proof fits within the AFB2 block ceiling;
  it does not itself add a portable proof block or confer receiver authority.
  The 206-case ordered-artifact target passes, including forged-field and
  allocation-failure codec tests.
  Certified AFB2 source-copy snapshots now include APF3 proofs once per
  publication digest, plus a compact bitmap naming only output sources whose
  receipt still selects that proof at the immutable cut. Export checks the
  document index and source reference in that pinned cut. The source
  certificate covers the new private block; AFB2 reader capability v5 rejects
  older decoders. One-pass and checkpointed import verify proof framing,
  checksum, source namespace, selected output ordinals and ordering, then
  store the bytes under an inert source-proof prefix, never donor receipt or
  authority keys. Borrowed decode avoids another proof-sized allocation.
  The 208-case ordered-artifact and 28-case retained-transfer targets pass,
  including binary document keys, full source-pin reopen, source certificate
  verification and checkpointed staging import. Receiver-local adoption and
  retained-effect/tail provenance transfer remain open.
  Online merge now reads source proofs from certified AFB2 objects through the
  bounded positional descriptor and carries them in a distinct provenance
  page/chunk payload, not vector/graph effects. The receiver validates APF3
  before apply and commits only inert, source-pin-scoped evidence keys with
  the page receipt. A durable pending bit refuses the final tail certificate
  until receiver-local adoption is implemented; transferred donor bytes do
  not grant acceptance. Tests cover a donor/receiver snapshot, lost page
  replies, a restart between three 1 MiB chunks, wrong-source keys, and the
  finalization barrier. Receiver-local adoption, retained-tail provenance,
  and bounded reclamation of abandoned merge evidence remain open. Replicated
  snapshot publication and final-fence apply now reject a provenance-free
  certificate if producer authority activated after its immutable pin, so a
  late activation cannot turn a previously empty proof stream into an
  apparently complete receiver cut. Import now also checks APF3 source order,
  physical position namespace, artifact guard ownership, output key family,
  duplicate outputs, document ownership and the live scoped-producer guard
  rule before storing candidate bytes;
  a valid checksum alone is not an adoption certificate. Bounded off-lock
  receiver preparation can now recapture every causal primary/artifact input,
  compare its current value and timestamp, and own the receiver's physical
  revisions. It also checks selected output digests, tombstones and donor-range
  ownership; changed inputs or outputs yield a stale candidate rather than
  inheriting donor positions. This remains candidate work until an ordered,
  replayable receiver transaction revalidates and installs local receipts.
  A receiver-side writer-transaction verifier now repeats the exact primary,
  artifact-input, and selected-output checks against local physical revisions,
  including same-byte ABA changes. Candidate preparation also owns the source
  pin, APF3 checksum, donor producer identity and selected-source bitmap after
  release of the transfer buffer. Preparation also requires the certified
  donor binding to match the APF3 epoch/digest and the receiver's ordered
  semantic catalog; writer revalidation repeats the receiver catalog and
  active authority fence. APF3 can encode a receiver-owned adopted origin that
  commits the donor pin, proof checksum and selected subset into a local
  publication digest; no caller installs receipts from it yet. A reusable
  receiver producer plan now validates both exact and semantic catalog
  bindings once per source cut, maps index/graph producer names and kinds to
  receiver-owned physical generations in O(1) per proof, and rejects donor
  generation drift. It also maps enrichment authority epochs and resolver
  definition generations, including default-zero resolver generations, from
  the authenticated catalog pair. Graph effect rebinding and promotion
  identity remain open; mapping is candidate identity, not acceptance.
  The receiver can now construct that plan from the donor catalog durably
  bound to the exact merge attempt at its checkpoint and the current ordered
  receiver catalog. It rejects an attempt change or local catalog drift without
  a donor round trip or per-proof catalog transfer.
  A selected direct-index candidate can now build a receiver-owned APF3 body
  off-lock from remapped local inputs and selected postimages. The body binds
  donor lineage through its adoption digest, drops donor historical output
  preconditions, and refuses graph/non-index adoption; it does not stage
  receipts or grant authority until ordered receiver apply is wired. The
  adopted-proof staging participant now takes selected source ownership from
  APF3 effects rather than fabricated mutation bodies, stages only those
  receipts and document/artifact references, and fences each artifact against
  its actual receiver-local revision. It still requires a certified-evidence
  fence and causal/postimage revalidation in the same ordered transaction;
  graph rebindings remain gated. For an absent selected output with no copied
  revision, adoption staging can now CAS absence and mint a receiver-local
  tombstone revision at its ordered apply position. A lost-response retry is
  accepted only if that revision and proof reference still name the same
  adopted proof; an unrelated same-position write cannot earn a receipt.
  Certified online-merge proof import now stages a 32-byte witness atomically
  with each inert APF3 record. It binds the already-verified APF3 checksum to
  the selected bitmap, avoiding a second multi-megabyte hash; writer
  revalidation CASes only the witness under the apply lock.
  Online artifact-page apply now writes receiver-local artifact revisions in
  the same batch as each transferred afterimage or deletion. Those revisions
  are the exact output witnesses later adoption needs, and replay-only pages
  cannot advance them to a new Raft position. Receiver candidate preparation
  treats a present output without such a revision as stale before constructing
  an adopted proof; absent outputs can still acquire a tombstone revision in
  ordered apply. Selected direct-index preparation now owns the receiver
  candidate, encoded adopted APF3, exact output positions and document
  reference keys as one bundle before apply; releasing the imported proof
  buffer cannot invalidate the prepared transaction inputs.
- Completion verification now reconciles shared graph winner/count outputs
  against their current accepted projection, while keeping private contender
  and stream outputs revision-exact. Replacement proofs and effect lookups
  are cached within the read; large artifact bodies are not reread.
- After baseline discovery, maintenance prepares bounded provenance-validation
  pages outside apply. A typed, binary-safe ordered control advances the exact
  cursor only at its unchanged mutation epoch, and atomically re-dirties stale
  streams using current input revisions. Followers apply the selected page;
  they do not rescan their local state. The control uses reserved upload/queue
  capacity and retains its exact apply identity through standby replay.
  Completing this validation pass does not clear required-stream obligations.
  The seal transaction requires both drained obligations and a completed,
  unchanged validation epoch; an empty work queue cannot bypass this guard.
- Producer input capture binds compiled templates, physical generations, and
  catalog bytes from one pinned write-plan epoch. It no longer deserializes
  the index catalog or borrows live generation numbers for each input row.
- Resolver callbacks inherit accepted upstream causal input sets, including
  neighboring primary rows and artifact dependencies. Sorted source sets are
  merged linearly and existing guard ordinals are remapped once per proof.
  A current accepted stream is checked before candidate/provider execution,
  avoiding repeat inference after lost replies. This is causal inheritance
  within an owner, not cross-owner provenance adoption.
- Resolver and asset providers now share an owned causal-input context. Copy
  and deferred asset callbacks capture the pinned producer definition before
  execution, inherit accepted upstream proofs, and publish through ordered
  transactions instead of local artifact/skip-state writes. Missing inputs
  publish explicit absence; durable receipts suppress repeat inference after
  acceptance. Provider queue admission alone leaves replay pending. Tokens
  retain digests/proofs, not duplicate document/upstream bodies, and their
  retained memory participates in provider batch budgets.
  Shared producer dispatch treats bounded queue pressure and leadership loss
  as pending control state, not terminal inference failure; cancellation and
  malformed-command/allocation failures retain their distinct dispositions.
- Asset publication derives full-text projections and downstream replay from
  the authenticated catalog. Consumer membership is compiled once per
  publication, and shared document coverage uses point reads across sibling
  provenance/revision witnesses rather than treating one deleted asset as a
  deleted document. Old or stale sibling bytes cannot earn authoritative
  coverage, and large output bodies are not reread for that decision.
  Epoch activation now initializes full-text coverage alongside vector/graph
  coverage. Direct and deferred callback regressions exercise real leases,
  Raft acceptance, stale upstream rejection, absence, and duplicate suppression;
  storage tests cover output authorization, shared coverage and reopen.
- Acceptance receipts and source-proof references belong only to output
  owners, including explicit deletion outputs. Read-only dependencies still
  invalidate the complete input digest but cannot certify their own producer
  work. Duplicate detection uses the same bounded output-owner bitset.
- Chunk output-set preparation now owns one canonical encoding shared by
  storage and text projection, with per-row scratch reuse and allocation-fault
  cleanup. Private checksummed manifests describe contiguous ordinals, total
  payload bytes, and a streaming content digest; replacement validation requires
  the complete old-tail retirement. The prepared writer fence uses one manifest
  point read, rejects stale replacements, and treats missing inventory as a
  baseline gap. LSM rollback/reopen tests cover inventory/member atomicity.
  Root chunk callbacks now use ordered publication when an inventory exists:
  they pin the catalog and input, skip already accepted work before chunking,
  and atomically publish members, tail deletions, inventory, receipts, provenance,
  coverage, and projection work. Preparation authenticates member identities and
  parses each payload once outside apply; commit checks the actual old count with
  one manifest point read. Shared text coverage is reconciled once per document
  and consumer rather than once per chunk. Missing inventory remains control-plane
  pending without provider invocation or replay acknowledgement, and now submits
  bounded ordered inventory-reconstruction controls. Unit-scoped callbacks and
  staged generations above atomic command limits remain part of the producer
  activation gate below.
- Unit-scoped chunk receiver preparation now shares the root replacement
  validator while binding an independent unit ordinal space. The scope is the
  canonical parent-unit key; authorization checks its document, configured
  extraction producer, member identities, payload parent fields and complete
  tail retirement. It resolves accepted parent provenance off-lock and requires
  the consumer to inherit every causal input. Final apply checks a fixed-size
  provenance reference alongside the manifest fence; absent acceptance cannot
  be replaced by plausible unit bytes. Unit projections do not grant terminal
  document coverage. A scope-specific accepted-set reader cannot substitute
  root or sibling evidence. This is the receiver boundary, not activation of
  extraction/unit callbacks: the accepted upstream unit inventory, bounded
  scope census, and large staged publication path remain required.
- Unit chunk input capture now binds the immutable extraction template and
  selects its child from the pinned catalog once per capture session. This
  follows the existing extraction-owned execution model without inventing a
  competing top-level worker template. Each captured input owns the payload,
  inherited causal sources, exact unit identity, and previous chunk manifest;
  no snapshot or catalog lease needs to survive provider execution. A missing
  unit with a live primary requires accepted upstream deletion provenance,
  rather than being inferred from raw absence. Scope, allocation-failure,
  snapshot-lifetime and retirement-fence regressions exercise this boundary.
  Unit inventory reconstruction uses the same extraction-owned authorization
  on sender and receiver, including when no standalone child worker template
  exists. Its bounded ordered pages reconstruct only that unit's old set and
  resume across reopen without manufacturing producer acceptance or a root
  manifest. Primary source identity is captured once per pinned input session.
  This is preparation machinery, not completed extraction callback activation:
  accepted unit-set enumeration and staged publication still gate that path.
- An ordered per-unit chunk callback now consumes that owned accepted input,
  reconstructs missing inventory through scoped ordered census controls, and
  releases storage/catalog leases before chunking. It decodes the retained unit
  and its provenance once, preserves page/document offsets and transcript timing,
  and uses the existing chunk payload encoder. Publication atomically replaces
  the complete bounded unit set and retires old tails; accepted retries skip
  chunking, and accepted upstream deletion produces an empty replacement. The
  callback-to-Raft regression supplies upstream acceptance explicitly and covers
  inventory bootstrap, payload provenance, allocation failures, retry suppression,
  and retirement. This is not extraction-worker activation: enumeration of the
  accepted complete unit set, durable cross-unit continuation, and staged output
  sets beyond the atomic command limit remain required before wiring that worker.
- Chunk reconstruction now has a bounded off-lock pager and portable fixed-size
  digest checkpoints for root/unit scopes. Physical chunk writes and tombstones
  advance a deduplicated stream revision even before its manifest exists;
  reconstruction resumes only in the same catalog epoch and unchanged stream.
  The new, unreleased manifest uses a resumable hash chain rather than serialized
  standard-library hash internals. Discovery still creates no inventory or
  producer completion proof. Root callbacks now order reconstruction pages through
  the existing census transport. Each receiver independently recomputes the
  bounded page and checks the exact before/after digest; sender time slicing is
  converted to an observed row limit for deterministic receiver verification.
  The final writer checks root, catalog, predecessor and stream-revision fences,
  and commits the inventory with its applied marker/outbox while deleting the
  temporary checkpoint. Partial pages resume after restart, and selected immutable
  generation heads cannot be mistaken for empty old-layout streams. This is
  receiver-local inventory, not producer acceptance or all-member agreement:
  staged-generation adoption or explicit replica-baseline agreement is still
  required before distributed retirement may depend on it.
  Reconstruction discovery and verification use transient physical scans; no
  provider invocation or query-cache admission is required. LSM tests cover
  multi-page receiver verification, lost-reply replay, rollback, restart,
  same-byte mutation races, forged page claims, all checkpoint-byte corruption,
  allocator failures, and final checkpoint removal. Root callback tests cover
  the missing-inventory-to-accepted-output lifecycle separately, preserving the
  distinction between inventory metadata and producer completion.
- Chunk query projection now walks a cursor in the caller's pinned snapshot
  rather than retaining a second, owned raw-prefix result alongside projected
  JSON. Ordinary stream names use borrowed key views; binary names retain the
  owning decode path. Projection cloning reserves container capacity before
  ownership transfers through a shared owned-JSON helper, and nested special-field
  handoff is allocation-fault safe. Exclusion-path allocation failures propagate
  instead of silently omitting requested exclusions. Chunk projection now uses
  a snapshot-bound logical cursor that merges legacy scopes and generation heads
  in key order. Selected heads suppress the whole old scope, including empty
  outputs; a prefix-successor seek skips obsolete tails without walking members
  or their derived descendants. Root/unit neighbors and binary producer names
  remain distinct. Generation-only streams are discoverable without legacy rows.
  Index consumers, graph consumers, transfer, and recovery still need the same
  head-selection semantics before production generation publication is enabled.
- Sparse ordinal projection now uses the logical chunk cursor, narrowed to the
  selected producer so unrelated generated streams are not decoded. Artifact
  discovery and document identities share one primary read snapshot; doc-number
  resolution reuses one sparse snapshot rather than opening a transaction per
  member. Embedding-source discovery also streams borrowed rows instead of
  retaining a complete physical range. Regressions keep retired unit postings
  alive and verify exact projected membership, duplicate input ordinals, binary
  producer isolation, and allocation-fault cleanup. This does not establish an
  atomic cross-store cut or certify derived embedding freshness; those still
  require the ordered publication/input-proof integration.
- Staged chunk generations now have checksummed scope/input/catalog-bound
  identities, bounded append preparation outside the writer, durable resumable
  progress, byte-verified duplicate pages, and a guarded atomic head switch.
  Snapshot views perform one seek plus sequential member reads, and never use
  legacy tails once a head selects a generation (including an empty output).
  Public artifact point lookup uses the same pinned-head resolver and releases
  decoded identity ownership on absence. LSM tests cover restart, aborted head
  publication, stale head/input rejection, duplicate append, allocation failures,
  divergent legacy tails, and old-reader visibility after member deletion.
  Document-extraction state recovery now enumerates logical unit and child
  chunk keys from one pinned read; a selected (including empty) extraction
  head suppresses obsolete physical units, and selected chunk heads suppress
  their legacy tails. Navigation-key recovery shares that read and avoids
  loading block values. This closes a recovery-reader visibility gap, not the
  ordered producer-publication or provenance-adoption gate.
  Ordered graph planning also reads a selected extraction root through that
  generation and inherits its accepted head proof, not a stale physical root.
  A head replacement invalidates prepared graph commands at Raft apply; the
  graph callback still needs producer-side extraction activation and the
  remaining cross-owner adoption barriers.
  These are storage/read foundations, not production activation: ordered command
  admission, receipts, quotas, producer regeneration, remaining index/graph
  readers, and transfer/adoption must be integrated before publishing heads in
  production. The storage lifecycle now reserves increasing stream incarnations
  atomically with begin and durably fences retirement before removing members.
  Bounded GC pages resume from persisted ordinal progress, reject active heads,
  and remove terminal attempt state. A single checksummed, scope-bound high-water
  mark prevents delayed begins from recreating reclaimed identities without
  keeping permanent per-attempt tombstones. Pinned readers retain old members
  through MVCC. Append/GC budgets include physical key bytes, with a bounded
  one-member escape for large keys. Fault tests cover restart mid-GC, duplicate
  pages, late append/publish/begin, empty abandoned attempts, and incarnation reuse.
  Authenticated retirement policy, admission, recovery scheduling and ordered
  command/standby wiring remain required before activating this lifecycle.
- Staged-generation recovery now has a bounded metadata-only discovery pager
  with checksummed, scope/authority-bound restart cursors. Root and unit scopes
  cannot consume each other's attempts. Pages verify state identities and the
  selected head's complete, non-retiring state, copy no member bodies, and release
  their read snapshot before returning. Incarnation proposals are read-only;
  ordered begin resolves racing proposals and an aborted reservation is reusable.
  Tests cover reopen/resume, catalog drift, binary scopes, malformed cursors,
  proposal races, and allocation-fault cleanup. Discovery must be scheduled
  fairly and wrap because concurrent begins may sort behind a cursor; scan-end
  is neither an admission barrier nor a producer completion certificate. Recovery
  still needs ordered resume/finalize/retire dispatch and producer-input recovery.
- Extraction generations now have a distinct document-owned physical namespace
  while sharing the existing staged append, incarnation reservation, atomic
  head switch, bounded retirement and recovery machinery. Chunk and extraction
  streams with identical document/producer names cannot share clocks, heads or
  recovery pages. Extraction head changes participate in owner obligations and
  their own stream revision; private rows remain non-visible preparation state.
  Namespace and LSM lifecycle tests cover partial append/reopen, duplicate pages,
  empty replacement, retirement and pinned old readers. This is the shared
  storage lifecycle, not an accepted extraction directory: complete unit
  enumeration, ordered authorization and reader/transfer integration
  still gate publication from the extraction worker.
- Extraction generations now have an immutable named-output directory over that
  lifecycle. Names are bound into each checksummed payload envelope and the
  generation digest; name/ordinal metadata and payload progress commit together.
  Duplicate names cannot silently replace previous pages. Selected point lookup
  reads one name entry and one payload; sequential enumeration and retirement
  use ordinal metadata without loading unit bodies. Checksummed resume positions
  bind the generation identity, so a head change cannot reuse an old ordinal.
  Page admission accounts for both directory indexes and payload keys, with a
  bounded single-large-member path. LSM tests cover reopen, duplicate pages and
  names, exact lookup read counts, allocation faults, metadata-only GC, empty
  replacement, and pinned old readers; codec tests reject corruption and cross-
  generation metadata. This is the named storage directory, not worker
  activation: semantic output authorization, accepted head publication,
  ordered upload/finalization, and durable cross-unit consumers still need wiring.
- Unit inputs, resolver inputs, and public artifact point lookup now select the
  extraction directory in their pinned snapshot. Typed unit names avoid
  repeating the document and producer in every directory member. Head absence
  is guarded before initial publication; an existing head forbids fallback to
  obsolete physical rows, including for an empty replacement. Unit chunk
  preparation requires the selected head guard or both head-absence and exact
  physical-unit guards. Receiver certification binds the accepted head proof;
  nonempty child output cannot consume an absent unit. Missing causal proof
  stays pending, including physical absence. Regression coverage includes binary
  unit IDs, allocation faults, cross-producer isolation, public lookup, stale
  head guards, and pinned reads across retirement. This does not authorize
  extraction publication or activate the worker/cross-unit scheduler.
- Authorized unit capture sessions can now enumerate an accepted extraction
  directory in bounded metadata-only pages. A page owns its logical unit keys,
  selected-head guard, and inherited producer proof; it releases every storage
  cursor before returning. Proof identity must match the selected generation's
  input digest and extraction producer. Non-unit metadata consumes the visit
  budget and advances the continuation, while a deferred large member never
  advances it. Empty directories require the same accepted proof. This supports
  cross-unit work discovery without loading all unit bodies or keeping provider
  work inside a snapshot; discovery/end-of-page is not child completion. The
  producer-side discovery/retirement driver and extraction publication
  authorization are still release gates. Unit-head guards are also recognized
  by command-level scoped validation, not just the receiver-specific validator.
- Unit capture sessions also provide bounded retirement discovery over existing
  child scopes. Membership checks use one named-directory metadata lookup,
  never a unit body, so a replacement with no units can still find every old
  child scope. Current units and other child producers are excluded. Returned
  scopes own the same accepted head/provenance as desired-unit pages. Checksummed
  continuation tokens bind the extraction generation and canonical child scope;
  stale heads, malformed cursors, corrupt inventories and cross-child resumes
  fail closed. Visits and copied key bytes are bounded, including a single-large-
  identity progress path. Discovery must wrap/revalidate because concurrent child
  inventories may sort behind a cursor: scan-end is explicitly not an obligation
  discharge or child-completion certificate. Retirement publication and the
  producer-side driver still need integration; receiver reconciliation is below.
- Desired-unit and retirement pages have a shared receiver-side child verifier.
  Each child must have current accepted provenance, retain the parent's causal
  inputs, and guard the exact selected parent head/revision. Retirement also
  requires a zero-count accepted output: an empty raw inventory is insufficient.
  The verifier obtains counts from the already-read manifest or generation spec
  rather than scanning chunk bodies, and returns an observation retaining any
  cross-document dependency fence alongside the page receipt digest. This is a
  prerequisite for durable page advancement, not a document-completion
  certificate.
- Unit-child reconciliation now has a durable receiver registry and bounded
  ordered `reconcile_units` controls. The receiver derives desired/retiring
  pages from its own predecessor, verifies every child before advancing, and
  rechecks root, catalog, observation and predecessor fences in the same writer
  as the applied marker/outbox. A changed owner materialization cut resets the
  prefix; cross-document inputs additionally retain their validation epoch.
  Claims bind the parent/child identity and generation but not a replica's
  physical root; stored records and local closures are root-bound. Last-page
  retries are idempotent, including terminal retries. Closure is for one child
  requirement only and cannot substitute for the parent's or sibling's proof.
  Obsolete root/epoch records use the existing bounded maintenance collector.
  Producer scheduling and the semantic extraction publication path still need
  wiring. Completion-plan consumption uses an independent unit-child witness,
  not the extraction parent's or another sibling's receipt.
  Reconciliation uses a sorted union of child inventories, selected generation
  heads, and historical raw unit-chunk scopes. It deduplicates overlapping
  representations and seeks past each raw scope's entire tail instead of walking
  its chunks. Continuations remain canonical inventory-scope identities, so
  discovery survives removal of one representation without skipping another.
  Physical cursors do not hydrate external chunk payloads; raw discovery inspects
  at most the first stored entry of each scope. Missing inventories or accepted
  receipts remain pending rather than silently certifying raw/head-only scopes.
  Ordered-artifact coverage includes checkpoint restart, terminal lost-reply
  retry through Raft apply, stale-predecessor rejection, allocation failures,
  corrupt/root-rekeyed records, and invalidation by a behind-cursor insertion.
- Completion plans compile extraction-owned chunk children directly from the
  catalog, independently of top-level worker templates. Each child has a stable
  definition-bound requirement and a local parent-template ordinal excluded from
  portable identity. Template/catalog reordering preserves identity; changing
  the child definition invalidates it. Configured extraction children remain
  pending requirements even when their parent has no executable template.
  A compiled name index replaces repeated child-catalog parsing and parent
  searches in unit authorization/reconciliation controls. The completion
  verifier consumes only current root-bound child reconciliation closures;
  parent extraction, projections, native effects and sibling requirements
  remain independent and must still pass their own gates.
- Completion scheduling now follows its durable verified prefix and proposes a
  bounded unit-reconciliation control when the next requirement is a blocked
  extraction child. Proposals own their data and release the read snapshot
  before queue admission. Missing child receipts remain pending; admission does
  not move either checkpoint. Existing maintenance polling rediscovers refused
  submissions and resumes accepted pages without a tight retry loop. This wires
  receiver reconciliation, not the still-gated extraction writer, scoped
  provider discovery/callbacks, or all-member completion barriers.
  Verification-only sessions avoid redundant primary-document capture/hashing;
  producer-input sessions retain that capture. A point-probe regression rejects
  primary document/typed-row body reads while verifying a current child closure.
  Validation: the ordered-artifact target passes 196 tests with zero failures or
  leaks, including proposal rediscovery, durable-prefix selection, missing-child
  refusal, allocation failures, restart, and the primary-body-read prohibition.
- Unit producer discovery now has a separate generation-bound continuation for
  desired children and obsolete-scope empty replacements. It selects only
  missing/stale child receipts, while rejecting stale parent evidence and
  propagating corruption. Cursor identity binds the authority/catalog,
  document, parent and child; checksummed framing rejects phase confusion and
  foreign-child resume. Owned pages retain no read snapshot and provide a
  point-only admission fence. Accepted child writes do not rewind discovery;
  changing the selected parent generation requires rediscovery. Scan exhaustion
  is not completion and subsequent sweeps must wrap for behind-cursor inserts.
  Durable scoped-job admission and actual replay callbacks are still required:
  the current thin worker journal coalesces generated references into document
  keys and rebuilds root requests, so adding unit fields to those references
  alone would silently discard the scoped work identity. Admission must retain
  each child/unit/generation together with cursor advancement atomically.
  Validation: 197 ordered-artifact tests pass with zero failures or leaks,
  including missing-result selection, obsolete-scope retirement, stale-parent
  rejection, wrong-child cursors, allocation failures, and a discovery reader
  that rejects extraction-unit payload reads.
- Scoped unit jobs now have a root-local durable outbox admission layer. It
  atomically stages the discovery cursor and generation-bound unit identities,
  deduplicates exact admissions, rejects stale predecessors, and accounts for
  outstanding job count/bytes across pages and generations. Capacity rejection
  occurs before writes and cannot skip the refused page. Payload preparation,
  hashing and cursor encoding happen before the writer; writer-side guards use
  fixed metadata and bounded point probes. Root/epoch cleanup uses the shared
  bounded registry collector. This is not worker activation: the worker must
  still execute/retire scoped jobs with current-generation receipt checks before
  freeing headroom.
  Validation: 198 ordered-artifact tests pass with zero failures or leaks. New
  coverage checks atomic job/cursor persistence across reopen, quota refusal
  without partial insertion, duplicate admission, stale predecessors, allocation
  failures, binary identities, and root/generation-bound checksums.
- Scoped-job admission now joins the durable document wakeup through the shared
  replay transaction, backlog admission, catalog fence and standby write gates.
  Refusal aborts both jobs and wakeup; metadata-only discovery pages advance
  without empty replay records. Thin journal coalescing no longer needs to carry
  unit identities because the same transaction retains those in the outbox.
  Bounded outbox reads own their returned identities, verify each record once,
  and include older generations so workers can retire obsolete jobs and release
  capacity. Workers must wrap discovery cursors for behind-cursor insertions.
  Automatic scoped callback execution and worker-loop integration remain gated.
  Validation: 198 ordered-artifact tests pass with zero failures or leaks,
  including refusal with no visible replay/job insertion, metadata-only pages
  with no wakeup, reopen with the wakeup and jobs intact, and allocation-fault
  coverage of bounded reads across both current and obsolete generations.
- Scoped-job retirement now requires a current accepted child result, including
  an explicitly empty result for a unit absent from the selected parent, or a
  newly accepted parent proving that the queued generation is obsolete. Missing
  receipts remain pending; callback submission never frees capacity. Parent and
  child verification share the reconciliation verifier. A bounded retirement
  session amortizes catalog binding and parent verification across jobs, while
  each prepared retirement owns point-only writer fences. Job deletion and
  counter release commit atomically without advancing the discovery revision or
  discharging stream obligations. Current counters allow concurrent admission
  and retirement; duplicate retirement does not decrement twice. The DB entry
  point uses the existing authority, snapshot and apply fences. The worker still
  needs to invoke these paths around actual scoped callback execution.
  Validation: 198 ordered-artifact tests pass with zero failures or leaks,
  including accepted-result resolution, stale-parent fencing, unfinished jobs
  retaining capacity, batched obsolete-generation retirement, allocation faults,
  idempotent deletion, exact counter release, and pinned-reader visibility.
- Scoped-job worker turns now own bounded scan pages and persist an independent,
  checksummed, root/catalog-fenced continuation. Failed jobs retain their quota
  but cannot pin every attempt to the first page; exhaustion wraps, and inserts
  behind the continuation are retried on the next sweep. Cursor commits are
  idempotent, stale concurrent commits fail, and receipt retirement/admission
  do not rewind worker progress. The DB entrypoint uses the existing standby,
  snapshot-replay, and apply fences. Generation-bound callback capture rejects
  an obsolete/missing parent before unit-body reads or provider execution;
  stale/already accepted callbacks also avoid deserializing chunker settings.
  Maintenance follows the existing blocked completion prefix and either
  submits a verified control or atomically admits scoped jobs with their wake.
  Discovery resumes its own durable cursor, resetting only on parent replacement
  or sweep exhaustion. The runtime job callback is generation-bound; automatic
  bounded worker execution and receipt retirement still need wiring, including
  fair scheduling across children, route selection, and large staged outputs.
  These scheduling records grant no acceptance or activation evidence.
  Scope-local outbox capacity refusal leaves admission unchanged and lets the
  maintenance document sweep continue. Validation: 199 ordered-artifact tests
  pass with zero failures or leaks, including durable continuation/reopen,
  failed-job wraparound, stale cursor CAS, allocation failures, parent-generation
  reset, blocked-child scheduling, and stale callback rejection.
- Nonempty child outboxes now have a root-local, per-document scheduling
  directory. First-job admission installs its fixed-size entry atomically;
  last-job receipt retirement removes it in the same transaction. Document
  cursors select one child with a bounded seek, without walking catalog nodes
  or unit bodies. Independent child and document continuations can commit
  together, preserving fair retry across failing siblings and wrapping for
  insertions behind the saved key. Live-child counts let the last retirement
  remove document cursor metadata in O(1); a first-admission generation fence
  rejects old cursors after a drained queue is refilled. Cursor commits preserve
  concurrent live-child count changes. Document-key hashing is prepared before
  the apply lock; final counter/directory fences use fixed-size keys and values.
  Root/epoch GC handles both registries.
  The directory itself grants no acceptance evidence; the replay worker and
  receiver-verified retirement paths below consume it.
  Validation: 200 ordered-artifact tests pass with zero failures or leaks,
  including sibling retry fairness, binary document isolation, behind-cursor
  admission, current-key retirement, joint-cursor reopen, concurrent count
  preservation, drained/refilled generation fencing, last-child cleanup,
  directory corruption rejection, and obsolete-root GC.
- The local worker checkpoint can now commit both fair continuations and up to
  128 receiver-verified receipt retirements in one store transaction. A stale
  cursor aborts the whole page without dropping jobs. A duplicate retirement
  returns after an absent-job point read, even if a later parent generation
  has invalidated the old receipt; present jobs still require the current
  catalog, authority, and accepted-result fences. Callback selection and
  execution remain the activation boundary.
- Required-work maintenance now visits one document's scoped outbox after
  native input closure, checks up to eight jobs per child against receiver-local
  accepted or obsolete receipts, and checkpoints retirement with both fair
  cursors. Pending callbacks remain queued. Documents without an outbox use a
  point lookup and never open a physical worker cursor. Job preparation reuses
  the bounded page's owned bytes instead of refetching each record from the
  LSM; the writer still checks exact bytes and current receipt fences. This is
  automatic receipt reclamation, not automatic producer execution or stream
  completion.
- The scoped unit chunk callback now takes its child range and placement from
  the accepted typed unit payload, so its route is independent of the number
  of chunks later produced. Chunk and unit range kinds remain distinct even
  when they share the same range ID. A remote unit route is rejected before
  chunking or publication until the routed child command path is available;
  callers cannot supply an unverified route for a durable job. Dynamic
  child-range inspection and routed remote publication remain open.
- Generated replay wakes now select one child and a bounded four-job page from
  the durable directory. The runtime clones the parent template and releases
  the read snapshot and plan before invoking each generation-fenced callback;
  a stable DB-owned context commits both fair cursors under the ordinary HA,
  snapshot, and apply gates. The replay remains pending while the document has
  any queued jobs, including on a wrap turn, and receiver-verified maintenance
  retires them. Missing dispatch or durable-turn infrastructure also fails
  closed while an outbox exists. A restart fixture checks the document-only
  wake, unavailable hooks, and refusal of the durable turn commit without
  cursor advancement or job loss. After receipt retirement, the fixture
  accepts a fresh typed parent generation, admits its current unit job, and
  verifies that bounded worker turns submit an actual child publish command
  while retaining the job pending receiver acceptance. The receiver then
  applies that command, retries the still-queued job without a duplicate
  submission, and retires it and its document wake only through accepted
  receipt maintenance. This wires local
  callbacks but does not complete remote child placement, large staged
  outputs, all-required stream closure, or the distributed activation fault
  matrix.
- Persisted unit encoding now lives in a shared typed payload contract rather
  than the runtime implementation. Ordered unit chunk callbacks decode through
  that contract once, bind document/producer/unit identity, reject provenance
  overrides of identity/text, conflicting mirrored metadata, and route
  contradictions (a local route has no remote owner; a committed remote route
  has one), and verify the
  current logical unit fingerprint before invoking a provider. Transcript/text
  ranges and document offsets are checked against the unit body. Owned decoded
  slices survive release of the raw input; preparation no longer constructs a
  second merged JSON object. The encoder preserves the existing artifact shape.
  This is the contract needed by extraction receiver preparation, not activation
  of the still-gated extraction writer or its ordered continuation protocol.
- Generation heads are now canonical read-set guards, not generic artifact
  mutation authority. The physical commit hook stamps both the exact head and
  its stream witness in the same native/Raft transaction; private staging rows
  do not invalidate visible inputs. Owned producer observations record head
  absence before first publication and use the selected head as the immutable
  set's provenance witness afterward. Asset callbacks and resolver reads select
  those inputs in one snapshot and inherit the head's accepted causal proof,
  including for empty output sets. Missing producer certification remains pending
  rather than adopting a legacy member receipt. Root asset reads, like unit
  reads, resolve through the selected extraction head; a selected empty set
  cannot fall back to stale physical root bytes. Downstream asset publication
  validates that root membership and the accepted producer proof off-lock,
  then fences the receiver-local proof reference at atomic apply. LSM tests
  cover private append, rollback, head replacement, stale/absent guards,
  downstream acceptance, stale/empty guard rejection, and pinned old observations.
  Ordered staged-generation finalization must still publish that proof together
  with the head, receipts, coverage, and consumer work before these paths activate.
- Accepted asset retries now drive ordered graph consumers before completing,
  including absence and deferred-provider acceptance races. They plan against
  the accepted value and its causal proof in one pinned snapshot, without copying
  the asset through an extra preliminary read or rerunning the provider. Derived
  replay under producer authority consumes committed graph effects rather than
  calling the legacy primary-state materializer. The erased document-store
  adapter forwards snapshot forks while retaining payload/import-reader lifetime.
  Regression coverage includes transitive generation-head guards, queued graph
  rejection and accepted-proof invalidation after a head replacement, graph queue
  refusal, real edge publication/absence retirement, accepted retries, binary
  graph-owner keys, and forked snapshots surviving parent close. Graph keys use
  their own suffix grammar rather than public artifact-ID parsing; nested review
  ownership remains restricted to the resolver source scopes. These
  repairs do not complete staged-generation publication, stream drain/seal,
  receiver adoption, or the distributed activation fault matrix.
- Required-work observations carry durable owner-local revisions in addition to
  primary source positions. Re-marking a dependency advances the revision even
  when the primary is unchanged; completing/removing a work record cannot reuse
  an old identity. Journal admission compares that revision atomically with its
  dispatch marker and replay append. Completion also requires the current local
  revision and authority, after receipt validation in the same writer snapshot.
  These revisions are not portable completion certificates: an ordered driver
  must validate each receiver's current obligations and causal receipts rather
  than copying a leader's local token. The required-stream enumeration and
  ordered drain/seal driver remain open.
- Root-template dispatch is now paged rather than allocating and journaling the
  whole catalog for each document. A borrowed fixed-size page preserves the
  immutable plan's upstream-first order, caps requests and escaped-key byte
  estimates, and permits one bounded oversized request. The next-template cursor
  and last dispatch sequence commit with each journal append. Partial progress
  remains schedulable across restart; dependency changes reset it under a new
  work revision. Dispatch completion still does not discharge an obligation or
  certify dynamically scoped unit/chunk, resolver, graph, or promotion streams.
- Logical chunk cursors support exclusive ordinal resume with direct seeks into
  the selected generation, skipping shadowed legacy tails and preserving unit
  boundaries. Resume keys remain positions, not completion certificates; a
  multi-snapshot consumer must validate its work/input revision. Stored embedding
  input fallback and source-hash checks now resolve selected generations in one
  snapshot, including empty output, rather than reading obsolete physical rows.
  Fallback extraction streams rows without retaining a second full raw scan.
  Ordered materialized dense/sparse callbacks now use selected logical chunks
  and retained vector scopes together; other chunk/unit production paths and
  stream completion are still required before activation.
- Scoped chunk-vector commands now authenticate their logical member and exact
  output, require head and output-precondition guards, inherit the accepted
  upstream proof, and project only to matching immutable-catalog consumers.
  Dense/sparse payloads and dimensions are validated before apply. Input capture
  owns the selected bytes and causal read set without retaining an LSM snapshot
  across provider work; provider identity uses the compiled write-plan template
  rather than reparsing catalog JSON per chunk. Provenance decoding happens
  off-lock under a tracked preparation budget; serialized apply rechecks the
  inherited inputs and a fixed-size receiver-local proof reference. Replacing or
  retiring that reference invalidates preparation even if output bytes match.
  Tests cover allocation failure, shared consumers, uncertified input rejection,
  proof-reference races, and sparse projection through actual Raft apply.
  A member receipt does not grant document-wide completion. Full stream
  reconciliation and drain/seal remain activation gates; this
  receiver-local fence is not a portable provenance-adoption certificate.
- Materialized chunk-vector callbacks prepare bounded dense/sparse provider
  batches with owned causal inputs under tracked preparation memory. The
  snapshot and catalog read fence close before inference or dispatcher calls.
  Accepted retries skip provider invocation; publication and absence cleanup
  use the ordered command path, never the legacy direct artifact writer.
  A merged candidate cursor includes existing vector scopes whose chunk has
  disappeared, so shrinking/empty selected generations retire obsolete vectors.
  It resumes across binary names, generation/root/unit boundaries, deduplicates
  live scopes, and avoids rescanning the physical tail ahead of every selected
  member. Tests exercise allocation failures and actual callback-to-Raft output
  acceptance and absence retirement. Completion remains pending: this scan is
  not a portable exact-input stream certificate. Durable scheduler continuation,
  unit/extraction/promotion paths, and the
  stream-completion/obligation driver still require integration before activation.
- Producer censuses now have a document materialization revision, staged in the
  same physical transaction as primary and visible artifact mutations. New unit
  scopes, vector/graph changes, generation-head switches, and nested resolver
  outputs invalidate the owning document's census; unrelated document writes do
  not. Private generation uploads and aborted head publication do not advance
  visibility. One revision write per affected owner avoids per-stream fan-out.
  Receipt-only chunk-vector passes pin this witness across page snapshots and
  inspect the complete accepted proof without decoding it twice. Foreign-input
  proofs additionally pin the mutation epoch, so neighbor changes cannot bless
  a mixed-input pass. A publishing pass cannot also certify completion. These
  observations are preparation fences, not durable/portable completion records;
  the ordered stream-completion command, required-stream
  closure, receiver adoption and drain/seal integration remain open.
- Accepted-member chunk-vector census pages now persist receiver-local
  checkpoints with their exact input observation, exclusive binary cursor,
  member count and rolling receipt digest. Atomic compare-and-swap rejects
  concurrent/regressing scans; duplicate pages are idempotent, and stale input
  observations permit a fresh census rather than trusting old progress. EOF is
  explicitly enumeration, not stream closure: it neither advances replay nor
  discharges a document obligation. Runtime retries reuse current checkpoints
  without repeating provider work. LSM reopen, corruption, allocation failure,
  stale-page and lost-reply tests exercise the persistence contract. Keys have
  namespace/epoch prefixes; obsolete checkpoints share bounded obligation GC,
  preserving pinned readers and current-epoch state through catalog churn.
  Portable completion commands, provenance-adoption certificates,
  durable scheduler continuation, full stream closure and drain/seal are still
  required; these local checkpoints must not be adopted as remote certificates.
- Local census checkpoints are now bound to the durable physical-root
  incarnation in both their key and checksummed value. The resident DB injects
  this identity into replacement runtimes; provider configuration and imported
  metadata cannot override it. Ordinary reopen preserves useful progress, while
  imported donor keys are outside a fresh root's lookup namespace and re-keyed
  donor bytes fail validation. Unknown physical identity fails closed before
  provider invocation or publication. Cleanup also retires foreign-root records
  in the active catalog epoch by seeking over the current root's entire prefix.
  Tests cover reopen identity, fresh-root metadata copies, forged re-keying,
  absent identity, and old/current-epoch GC with pinned readers. This local
  binding does not replace portable source/provenance adoption or distributed
  completion/capability barriers.
- Chunk-vector censuses now distinguish physical-tail budget yields from EOF.
  Checkpoints retain an inclusive physical scan position independently of the
  last accepted logical member, allowing ignored-only pages to survive restart
  without inventing accepted members or repeatedly inspecting the same tail.
  Accepted-only passes yield after each durable page; physical scans share
  visit, byte and elapsed-time budgets. Binary-key, unrelated-output, expired
  deadline, allocation-failure and LSM-reopen tests cover ordering and resume.
  Publishing-pass scheduling still needs separate time slicing; these changes
  do not make the entire reconciliation bounded.
  Checkpoint encoding is prepared into owned bytes before the writer fence;
  input validation and checkpoint CAS remain in the committing transaction.
  No completion, obligation discharge or capability activation is inferred.
- Logical chunk scans now share the page budget with physical output scans.
  A checksummed merge position retains the next generation ordinal and legacy
  row independently, including progress through empty generation heads and
  ignored legacy rows. Census checkpoints persist that position with CAS
  checks that forbid either merge input from rewinding. Prefetched logical
  members retain their predecessor position when an older output is returned,
  so snapshot-per-result retries neither skip members nor repeatedly traverse
  empty heads. An expired deadline still permits forward progress in both
  inputs. Ordered stream-closure commands and receiver verification remain
  open; a resumable local scan is not a portable completion certificate.
  Regression coverage includes checkpoint reopen without accepted members,
  independent merge-input rewind rejection, allocation failures, and
  snapshot-per-result scans through 64 empty heads and obsolete outputs with
  an operation-count bound to catch repeated empty-head traversal.
- A receiver-side chunk-vector census primitive now recomputes bounded pages
  from the immutable producer plan and current accepted provenance. Claims
  bind the producer/index generation, authority, owner input witness, receipt
  chain, and both cursor positions; verification takes a receiver-supplied
  predecessor rather than trusting a sender's local checkpoint. Deterministic
  visit/byte limits reproduce page boundaries without provider invocation.
  Owner-local progress ignores unrelated mutation epochs; foreign-input proof
  participation pins the epoch. Dense/sparse callback tests cover live and
  empty streams, missing acceptance, forged predecessor/result claims, stale
  inputs and allocation failures. This is read-only preparation; the progress
  registry and command below supply the commit boundary. Upstream stream
  closure, obligation discharge, adoption and distributed fault validation
  remain open. EOF alone
  still cannot certify that an upstream producer has finished creating scopes.
- Receiver-verified pages now have a separate durable progress registry. An
  owned prepared record carries the receiver's expected predecessor; the final
  writer rechecks the input witness, physical-root identity, seal state and
  predecessor CAS before installing it. Aborted writes do not advance the
  registry, lost-reply duplicates are idempotent, and competing or stale pages
  cannot overwrite newer progress. Stale input observations restart the scan.
  Fast paths still check the immutable producer plan, including empty streams.
  Root-bound records authenticate both page claims and progress bytes and share
  bounded old-epoch/foreign-root cleanup with other producer metadata. Tests
  exercise real LSM transactions, abort/retry, competing pages, stale prepared
  work, corruption and allocation failures while preserving replay position
  and pending obligations. This writer primitive is not by itself a stream
  closure command or an activation certificate.
- Census pages now have a bounded binary-safe publication control command.
  Receivers resolve the immutable catalog template, independently verify the
  claimed page outside apply using a tracked working-set allocator, then
  commit progress together with the Raft applied marker and the existing
  standby marker/outbox machinery. Stale source/catalog, pending provenance
  and sealed-source outcomes use the same durable rejection path as other
  producer controls. JSON/compact codec tests bind producer identity, limits
  and page claims; dense/sparse callback tests exercise actual Raft installation,
  duplicate apply and stale-generation/input rejection. Upstream closure,
  obligation discharge, provenance
  adoption and distributed crash/promotion validation are still open.
- Materialized chunk-vector workers now automatically drive bounded verified
  census pages through the publication control dispatcher before attempting
  provider work. Missing acceptance falls back to materialization; current
  enumerated progress avoids repeated scans and dispatch. The worker releases
  its read, plan and ownership fence before enqueueing, and queue refusal does
  not advance durable progress or replay. An accepted prefix can advance even
  when its next member is pending, but that member's cursor is not skipped.
  Tests exercise dense/sparse dispatch, refusal/retry, multi-page continuation,
  accepted-prefix verification, and no reinference after acceptance. Normal
  publication waits no longer log warnings, inflate error counters or inherit
  exponential provider/pipeline backoff. Workers still retain replay debt at
  enumeration: upstream closure and exact obligation discharge remain required.
- Census EOF now requires a current accepted root-chunk manifest, including
  explicit empty output, and inherits that producer's complete causal inputs.
  Missing manifests stay pending and altered manifest bytes fail the accepted
  output digest check. Selected generation heads take precedence and require
  their own accepted proof; they cannot borrow an older manifest's acceptance.
  Dense/sparse regressions cover these failures, unaccepted baseline manifests,
  and the valid empty result. This proves only the root replacement boundary, not
  unit/DAG closure or document completion. Catalog authorization is now bound
  once per pinned census/provider batch and reused across members; per-member
  upstream provenance, source capture and output CAS remain independent.
  An operation-count regression verifies one ordered-catalog read across
  sixteen live/absence captures with independent accepted-proof validation.
- The receiver now persists root-only chunk-vector stream closure in the
  census transaction (AVP2), alongside the Raft/standby control markers. Closure
  is derived locally from the pinned catalog and verified enumeration; no
  sender-supplied bit can grant it. Unit/extraction and neighbor-dependent scope
  sets remain open pending their upstream scope certificates. A closure handle
  owns its document identity and rechecks physical root, causal observation and
  exact registry version in the final writer without scanning or decoding
  provenance there. Dense/sparse tests cover live and empty closure, allocation
  faults, partial/missing progress, root mismatch, stale input and unrelated
  writes. One closed stream still does not discharge document obligations or
  advance replay; it must participate in the all-required-stream verifier below.
- Immutable write plans now compile a catalog-wide completion checklist, not
  just the generated-provider queue. It contains mandatory native/authored
  effects, every provider template, every index projection, and separate
  resolver and promotion requirements. Resolver output, upstream scope kind
  and target table remain explicit. Stable framed identities exclude local
  template ordinals; catalog-bound cursors expose allocation-free pages capped
  at 128 requirements/64 KiB (with single-item progress for larger identities).
  Tests cover all index kinds, empty-provider catalogs, promotion requirements,
  canonical ordering, stale catalog cursors, count/byte pagination and allocator
  failures. A requirement is not acceptance: the completion executor and the
  unresolved producer/scope handlers still have to certify every node before
  clearing document work. Provider ordinals map directly to canonical nodes,
  and stream closure handles bind their requirement ID for that verifier.
  Provider identity hashes use the same exhaustive definition-field selection
  as authorization, excluding only row/replay identity and consumer routing.
  Cursor fingerprints cover both catalog bytes and the complete canonical
  requirement set, so adding a requirement cannot reuse an old ordinal cursor.
  Identical provider definitions shared across consumer routes coalesce into
  one requirement while preserving direct ordinal lookup and separate index
  projection requirements; distinct provider inputs never coalesce.
  Compiled definition lookup also resolves row-specific requests without
  scanning the entire template list; execution authorization still compares
  the canonical template's complete definition and fences the active catalog.
  This checklist does not enable protocol 15.
- Required-stream completion now has a distinct private ordered control. The
  work driver prepares bounded pages and submits them without discharging local
  work on queue admission. Every receiver independently verifies the exact
  before/after page identity against its pinned catalog and causal cut; portable
  fingerprints exclude physical roots and local obligation counters. Receiver
  verification uses the sender's observed requirement count rather than its
  machine-dependent deadline. Final apply checks receiver-local root, catalog,
  work revision, predecessor and witness fences, then atomically commits either
  a prefix checkpoint or exact obligation discharge with checkpoint deletion,
  the Raft applied marker, and standby outbox. No terminal certificate remains
  per document. Missing requirement verifiers still stop progress; all-member
  evidence agreement, repair/retry coordination and drain/seal remain separate
  activation gates. A sender's completed page is not remote acceptance.
  Extraction-owned producer scopes now use a separate receiver-local closure:
  it authorizes the immutable template, verifies the fully published named
  generation and accepted head proof/inputs, then carries exact head bytes,
  the proof reference, and bounded private generation-state CAS witnesses into
  the final writer. Child streams remain separate requirements. Missing heads
  stay pending, and head-byte or private-state mutation invalidates a prepared
  parent witness without rescanning units under apply. Allocation-failure
  coverage exercises witness preparation and release. This does not activate
  the still-gated extraction writer, portable adoption or all-member drain/seal.
  LSM regressions exercise native-only completion on two independent roots with
  different work revisions, receiver evidence missing/present after restart,
  rollback, duplicate delivery, stale inputs, forged claims, allocation failures,
  and actual work-driver submission. A separate indexed-table test keeps work
  pending at an unverified index requirement; the 257-requirement engine test
  checks deterministic bounded page claims, restart and final checkpoint deletion.
- Completion prefixes for indexed catalogs now pin a durable receiver-local
  projection lifecycle epoch. Sidecar creation/replacement, status or generation
  changes, configuration identity changes, watermark regression, same-cut count
  replacement and checkpoint removal revoke old evidence before publication;
  ordinary monotonic watermark advances do not. Primary-metadata resets revoke
  inside their transaction. Failed sidecar publication may leave a conservative
  revocation, never a new sidecar with an old completion prefix. Plans without
  index requirements do not read this fence. Portable page identities exclude
  the local epoch value; each receiver still checks its own fence at commit.
  This supplies revocation, not index durability evidence: actual projection
  witnesses and their publication/recovery wiring remain activation work.
  Checks cover restart of a revoked completion prefix, unchanged document/work
  revisions, portable fingerprints across local epochs, corruption, monotonic
  progress, reset rollback and allocation failure between revocation and sidecar
  replacement. The ordered-artifact target now includes the shared apply-state
  suite, including concurrent sidecar writers and checkpoint-format regression
  tests, rather than testing only the new control path.
- Materialization revisions retain the exact receiver-local replay boundary of
  their owner's visible mutation. The physical transaction observer grants a
  boundary only for a fresh all-effects journal append paired with its advancing
  next-sequence marker. Unjournaled/imported changes and ambiguous journal batches
  retain an explicit absent boundary; they cannot borrow another document's old
  journal tip. Unrelated writes therefore do not continuously move the target
  needed for a document's projection evidence. This field is local evidence,
  not portable producer identity or proof that an index has applied the record.
  The PR-only materialization record has one strict versioned encoding, without
  a legacy decoder. Regression coverage checks namespace/version/truncation,
  exact replay capture, stale journal-key reuse, unrelated writes, abort and
  reopen. Live dense/sparse/graph resets revoke projection completion before
  closing storage; interrupted full-text reset also revokes before clearing.
  Full-text rebuild initiation durably publishes a rebuilding checkpoint and
  restart marker before destructive reset, and publishes clean only after
  backfill and sync. A restart-marker publication fault must preserve the old
  physical index while retaining explicit rebuilding debt. These boundaries do
  not activate index completion: physical projection witnesses and coordinated
  publication/recovery still need their own implementation and release evidence.
- Projection evidence preparation has a nonblocking guarded sidecar snapshot:
  it retains the existing checkpoint-publication lock across decoded checkpoint
  access and final primary-metadata fencing. This prevents reading a new
  revocation epoch together with an old clean sidecar while its replacement is
  in flight. One snapshot supports multiple index lookups; absent entries are
  explicitly absent rather than fabricated clean zero-watermark records. Busy
  publication yields for retry, and allocation/error paths release the lease.
  Independent raw reset or catalog changes still fail the final transaction's
  authority/epoch check. Writer reopen revokes local projection evidence before
  loading physical indexes; read-only/status opens do not mutate this fence.
  Tests cover lease lifetime, busy retry, allocation failures, missing sidecars,
  authority/lifecycle changes and writer versus read-only reopen. The guarded
  snapshot is not itself physical evidence; physical validation and aggregate
  certificates supply the additional checks described below.
- Full-text replay for activated physical owners now publishes a checksummed
  projection seal in its own index metadata before advancing the external
  applied-sequence sidecar, on both direct and batched/coalesced replay paths.
  The seal binds physical root, namespace, immutable coverage generation,
  configuration identity and replay cut. Publication forces index/WAL durability
  before writing the seal; retries do not regress its cut and finish any pending
  seal durability barrier. A different physical identity cannot reuse it.
  Unpublished managers have no serving-root authority. Reset and structural
  segment removal delete the seal in the same index transaction as the data
  change, rather than relying on a later best-effort primary cleanup. Tests
  exercise byte corruption, monotonic retry, wrong-root rejection, reopen,
  retained pre-reset readers, real full-text replay and modeled power loss/reset
  sync failure. These seals establish the full-text physical publication
  boundary only: automatic certificate refresh, startup/transfer adoption and
  other index-kind adapters remain open.
- Full-text physical validation now joins the guarded sidecar to the live
  generation's seal. It rejects missing/nonclean/ahead checkpoints, mismatched
  root/namespace/config/coverage generation, incomplete rebuild markers and
  pending repair/admission. Progressive query serviceability does not override
  corpus-wide repair debt. Catalog and per-index apply ownership are retained
  until evidence publication; try-lock acquisition avoids a lock-order cycle
  with replay publishers. A final primary-transaction check repeats the local
  lifecycle/catalog fence and rejects newly durable managed admission. The
  sidecar lease must outlive this physical guard. LSM replay tests cover wrong
  generations, busy retry, allocation cleanup, new admission and a forged clean
  sidecar ahead of physical data. Split pruning serializes with the index apply
  lane and revokes both primary evidence and the physical seal before changing
  the corpus, including mixed-segment replacement (which is not equivalent to
  ordinary compaction). The guarded generation is the prerequisite for the
  aggregate evidence publication described below.
- Full-text aggregate projection certificates now have a strict key-bound
  encoding and transactional issuance from a retained physical guard. One slot
  per index replaces old roots/epochs instead of accumulating per-row or
  historical certificates; index removal deletes the slot in the same catalog
  transaction. Same-identity publication is monotonic and idempotent. Readers
  reject damaged certificates; a newly validated physical guard can reconstruct
  that derived record without trusting its damaged predecessor. Completion
  can consume the record against a document's exact local replay boundary and
  performs only metadata point checks in the final writer. New source mutations,
  lifecycle changes, root changes, stale authority and insufficient coverage
  invalidate the witness. Missing/unrecorded source boundaries require the
  completed snapshot-adoption proof described below; no global journal tip is
  substituted. The ordered completion verifier now
  supports this full-text requirement alongside native/provider requirements.
  Tests cover certificate corruption/slot substitution, aborted and duplicate
  issuance, unknown requirement identities, wrong roots, lagging coverage,
  reopen invalidation, catalog-drop cleanup and a native-plus-full-text ordered
  completion. Leader discovery and receiver preparation now automatically refresh
  missing/lagging full-text certificates before opening their proof snapshot.
  Refresh shares the completion engine's exact resume state and bounds work to
  the next visit/byte-limited page, avoiding starvation beyond the first 128
  requirements. Valid cached coverage uses only metadata probes; missing
  evidence shares one nonblocking sidecar lease, validates physical generations
  outside the primary writer, and repeats fences at commit. Leader maintenance
  also yields between indexes on its time budget. Busy leases leave work pending;
  catalog/source races retain ordered rejection semantics on receivers. Tests
  cover busy retry, a cached page without any sidecar file, and receiver-local
  reconstruction after deleting the discovery certificate. A two-root LSM
  fixture rejects a copied leader certificate, reconstructs the receiver's own
  evidence during ordered apply, and retries the same entry after a lost reply.
  Broader historical adoption, other index kinds and all-member drain remain
  release gates; completed full-text snapshot adoption is described below.
- Authority activation now captures a strict, checksummed receiver-local replay
  boundary in the same primary transaction. This is the immutable historical
  baseline cut, not a projection certificate. Repeated activation retains the
  original cut even after the journal advances; in-memory reservations cannot
  inflate it. A new epoch validates the previous boundary and rejects a
  regressed durable journal in the same namespace. Corrupt/missing boundary
  evidence for an existing authority is not reconstructed from today's tip.
  Native and ordered baseline enrollment validate this authority-bound record;
  native discovery repeats the boundary fence in its committing transaction.
  The private key is excluded from portable metadata. Codec and LSM tests cover
  byte corruption, abort atomicity, duplicate activation, uncommitted sequence
  reservations, restart, epoch replacement, malformed/regressed replay metadata,
  and an empty new namespace. Historical projection credit still requires a
  complete snapshot/replay adoption proof tied to this cut; replay-only seals
  and clean sidecars do not provide that proof.
- Under active artifact authority, physical input capture now advances a
  receiver-local source-gap epoch in the same transaction as an unjournaled or
  ambiguous materialization. Ordinary
  journaled writes leave it unchanged. Snapshot guards bind this epoch to the
  immutable activation boundary, including the absence of authority, so an
  activation race also invalidates a pre-activation candidate. Durable repair
  checkpoints persist the guard beside the original replay floor. Resumed
  candidates and final shadow activation recheck it; an invalidated candidate
  is discarded only after releasing its physical handles, then rebuilds from a
  fresh snapshot without failure backoff. Older candidate checkpoints without
  a guard must rebuild rather than receive inferred source completeness.
  LSM materialization tests cover journaled progress, unjournaled invalidation,
  abort and restart; checkpoint tests cover guard persistence, and full-text
  repair tests contrast source-gap restart with journal-only catch-up. This
  closes the source-gap fence needed by snapshot/replay adoption, but is not
  by itself historical projection or producer-provenance evidence.
- Completed full-text shadow snapshot/replay builds now persist an adopted
  baseline in the physical generation's seal before publishing its pointer.
  The seal binds the owning root, catalog generation, activation boundary and
  snapshot source-gap guard. Incremental replay preserves this proof but cannot
  create it; physical reset/removal retires it with the seal. Receiver-local
  aggregate certificates carry baseline evidence only while its source guard
  is current and its coverage reaches the immutable activation cut. Untouched
  pre-activation rows and recorded unjournaled rows included in that snapshot
  can now satisfy their full-text requirement using that proof. An unchanged
  source-gap epoch excludes later unjournaled mutations. Final discharge
  rechecks the source guard, exact row revision, local lifecycle, authority
  and certificate in the same primary transaction. Tests exercise both
  journal-only catch-up and a fresh build after an unjournaled source race,
  followed by real ordered baseline enrollment and native-plus-full-text
  completion of an unchanged historical row. A staged source-gap change rejects
  discharge. LSM tests cover same-cut proof strengthening, preservation across
  incremental replay/reopen and retirement on reset; codecs reject mismatched
  authority, missing baseline identity and insufficient coverage. Other index
  kinds, imported producer provenance and all-member coordination remain
  activation work.
- Full-text adoption discovery now feeds the existing durable generation-build
  scheduler on both leader discovery and receiver completion preparation. Clean
  idle generations with absent/corrupt/incompatible physical proof, or historical rows whose
  existing proof lacks a current baseline, request bounded adoption work;
  checkpoint/apply contention and existing repair are not treated as proof loss.
  Admission releases all physical/sidecar leases first, rechecks the exact
  authority and catalog under the structural/apply fence, and admits at most one
  new build per completion page. The revision-tracked scheduler name map
  deduplicates existing jobs in O(1), including paused and terminal jobs, without
  repeatedly scanning durable repair state per document. The dedicated
  `artifact_baseline_adoption` trigger distinguishes this work from corruption
  and operator requests and retains the serving generation while building.
  Imported seals bound to another root, namespace, generation or configuration,
  and seals behind the clean checkpoint, also request reconstruction; a sidecar
  cannot relabel physical evidence. Two-index LSM tests cover a foreign-root
  seal alongside an absent seal, busy discovery without false admission, bounded
  scheduling, duplicate no-ops, query availability, pause/restart persistence,
  ordinary worker execution and final ordered historical completion. This is
  local durable scheduling; all-member completion/retry coordination remains
  an independent activation gate.
- Document-wide dense/sparse provider streams now have receiver-verified
  closure handles alongside the materialized-chunk census handles. Preparation
  resolves current accepted provenance for the exact physical primary row or
  tombstone, including explicit absent output. A surviving transport receipt
  cannot certify a replaced output. Owned handles retain only bounded identity
  and revision fences; their final check uses three metadata point reads for
  document-local inputs, not vector payloads or provenance decoding. Foreign
  dependencies retain the additional mutation-epoch fence. These handles close
  one generated requirement, not native effects, index projections, or the
  complete document obligation; the all-required-stream executor remains open.
  LSM-backed regressions cover dense vectors on typed rows and sparse vectors
  on document rows, live/absent/deleted inputs, same-byte output replacement,
  unrelated writes, physical-root mismatch, allocator failures and reopen.
- Root chunk replacements and singleton asset providers now participate in the
  same all-required-stream verifier. Root completion requires current accepted
  manifest/generation evidence and the producer's latest reference, including
  explicit empty output; reconstructed inventories alone cannot close work.
  Singleton asset completion retains all accepted causal inputs, including
  upstream assets. Handles own their identity/fences, survive database reopen,
  and check only metadata in the final writer. Tests exercise real callbacks,
  live/empty/deleted owners, stale upstreams, manifest reimports, allocation
  failure and closure after restart. Projection, resolver and promotion nodes
  remain independent and cannot be skipped by provider success.
  Scope classification is compiled once into the immutable completion plan:
  singleton assets use document scope, document extraction retains its dynamic
  scope requirement, and upstream-unit chunks retain unit scope. Authorization
  shares exact definition lookup and catalog/owner fences with vector producers,
  eliminating per-row template scans and provider-JSON parsing. Dynamic unit,
  extraction and neighbor scope closure remains pending its own verifier.
  Deletion regressions also fixed graph cleanup's tombstone validation: canonical
  empty ownership roots from the owning graph generation are permitted alongside
  zero visible-count witnesses. Nonempty state, segments, trailing bytes, wrong
  generations and other graph owners remain invalid; checks are fixed-width and
  allocation-free. Normal graph preparation still enforces catalog and exact
  mutation/count preconditions before accepting these cleanup records.
- Receiver-local completion accumulation now checkpoints strict prefixes of
  the complete immutable requirement plan. Pages are bounded by count, bytes
  and time; they retain exact obligation revisions, physical-root identity and
  causal materialization observations. Foreign-input requirements additionally
  pin the shared mutation epoch without penalizing owner-local requirements.
  Commit repeats the fixed-width catalog stamp, obligation/cursor CAS and
  read-only witness fences. A terminal page atomically discharges the exact
  obligation and deletes its checkpoint; a persisted EOF cannot grant credit.
  The checkpoint engine's multi-page fault fixture uses explicit test witnesses,
  while native/document/chunk stream adapters use actual accepted evidence.
  Projection, resolver and promotion handlers and the ordered drain-command
  integration remain open: generated-only success never bypasses those nodes.
  Obsolete epochs and imported root identities use bounded local reclamation.
- Vector ingress now distinguishes explicit authored values from generated
  output with a tagged origin, rather than inferring authorship from a missing
  source hash (multimodal provider output can also lack a hash). Dense/sparse
  envelopes preserve descriptive authored origin; contradictory origin/hash or
  graph envelopes are rejected. Direct-field index replay preserves that origin
  and ordinary portable backups use the raw-artifact path for authored vectors,
  avoiding lossy compact batches. Tests cover payload equivalence, malformed
  envelopes, allocation failures, and exact dense/sparse backup round-trips.
  This metadata is not accepted provenance or a completion certificate. Native
  writer-bound certification and receiver adoption remain required; flagless
  historical or imported vectors are not implicitly upgraded to authored output.
- Ordinary DB ingress and coordinated transaction resolution prepare receiver-local authored-vector acceptance
  for active publication epochs. It deduplicates exact primary/timestamp/vector
  postimages and hashes each once before opening the physical writer. The
  physical input-capture hook observes those bytes and issues acceptance in the
  same transaction as their source/artifact revision stamps; incomplete writes,
  replacement postimages, and retries cannot inherit prior observations. Records
  bind catalog, namespace, epoch, physical root, source position and output
  digest. Reads use revision point checks; raw imported origin flags grant no
  acceptance. Obsolete epochs/roots use the shared bounded reclamation walker.
  Native/Raft tests cover raw imports, missing retry postimages, source changes,
  re-keyed records, allocation failures, and dense/sparse outputs. Historical
  provenance adoption remains a separate requirement. Protocol 15 remains disabled.
  Identical dense projection replay now preserves the ingress artifact instead
  of rewriting it and invalidating its source revision; this also avoids a
  redundant vector WAL write. An LSM-backed DB regression verifies acceptance
  after full projection completion and reopening the same physical root.
- Native/base-vector completion now uses a receiver-local, resumable physical
  prefix census. Each 128-visit/64 KiB/2 ms page checks current authored acceptance
  or generated provenance without point-fetching vector bodies or materializing
  external blobs. Persisted cursors
  bind catalog, physical root, causal revisions and predecessor CAS; a restart
  resumes rather than rescanning the accepted prefix. A tombstoned owner with
  surviving vectors or a raw imported flag remains pending. The completion
  adapter consumes this closure, including native-only catalogs; it does not
  bypass independent projection, resolver or promotion requirements.
  Artifact-only mutations now reopen the owning document's work exactly once,
  preserving its primary input position while advancing its work revision.
  This prevents new scopes from hiding behind an earlier completed obligation.
  Maintenance runs the census even with no provider templates; progress requests
  another bounded turn, while missing evidence backs off. Fault tests cover
  130 dense/sparse vectors, cancellation, allocation failures, cursor races,
  restart, exact discharge and reopening after an uncertified vector appears.
- Current receiver-local authored evidence also closes the corresponding
  document-vector provider requirement and suppresses provider execution.
  Both paths bind the exact source and physical root, not the origin flag.
  Dense/sparse and batched-dense callback regressions require zero inference
  or publication calls for accepted authored vectors. Generated retry checks
  now require current provenance rather than a transport receipt that may
  outlive its output. Closure commit checks only fixed metadata fences and the
  accepted record version. Historical receiver adoption, non-document provider
  scopes and ordered all-required discharge remain separate activation gates.
- Coordinated resolution and ordinary ingress share one borrowed commit
  participant interface through backend type erasure. Attachment must precede
  mutations, retries reset observations, and the final callback can write only
  private metadata after physical source revisions are stamped. Unsupported
  backends reject participation. Callback failure rolls back the decision,
  primary writes and acceptance together; terminal lost-reply retries do not
  replay postimages or re-certify them over newer rows. Native/Raft fault tests
  and document/typed-row transaction API tests cover these boundaries. No
  participant, preparation pointer or callback is persisted as durable work.
  The DB regression reopens after durable intent preparation, resolves from
  recovered intents, waits for projection, and reopens again to verify local
  acceptance on both document and typed-row tables.
- Current-epoch authored acceptance retirement scans metadata only, with
  cold-cache admission and bounded 128-record/64 KiB/2 ms pages. Its durable
  cursor advances across live records, so a live prefix cannot starve stale
  receipts. Preparation releases the reader before the writer; commit repeats
  exact-record, artifact-revision, authority, root and cursor checks. Renewal
  racing cleanup therefore survives, and primary-only changes retain the
  authoring evidence of an unchanged output. Completed sweeps are cached at
  the durable native/Raft mutation clock. Mutations during a sweep force a
  follow-up pass (including keys behind the cursor), while idle maintenance
  performs point reads without reopening the certificate scan. Regression
  coverage includes a 257-certificate live-prefix/churn fixture, aborted
  cleanup, competing cursors, racing renewal, pinned readers, byte limits,
  native/Raft clock invalidation, epoch/root fences and restart. This is local
  reclamation, not historic provenance adoption or native-stream closure.

The ordered-artifact regression target covers upload refusal without false
success, restart/resume, pending-coverage retry without retransmission, accepted
graph provenance/coverage, exact standby control identity, and bounded terminal
retirement. Baseline coverage includes two owners with different local work
records, enqueue refusal, restart, duplicate pages, behind-cursor writes and
continuous tail growth. It also fills the producer-upload quota on both owners
before uploading/finalizing the baseline through reserved control capacity.
Wire tests cover binary row keys, control-class tampering and allocation failures.
Validation regressions cover two-owner replay, mutation racing an empty page,
new-leader discovery, duplicate apply, stale graph projection repairs, binary
repair keys, allocation failures, and standby control identity.
Obsolete-work reclamation tests preserve pinned readers and active counts while
removing a multi-page old epoch; cursors from that epoch cannot drive current work.
Upload recovery tests cover restart with pending coverage, queue refusal,
duplicate hints, stale incarnation fences, and payload-free fair discovery.
Runtime tests saturate producer admission while admitting a finalize hint, and
verify scheduler refusal and shutdown release the exact queue reservations.
These are component guarantees, not deployment activation proof.

Focused checks (from `zig/`, with `-Doptimize=Debug`):

- `zig build antfly-ordered-artifact-test`
- `zig build antfly-retained-transfer-test`
- `zig build antfly-data-runtime-test -- --test-filter 'data runtime ordered artifact upload handoff'`
- `zig build antfly-storage-native-fk-test`
- `zig build antfly-standalone-initial-fk-test` (the six native FK/TRUNCATE
  integration cases require this storage-owner-linked target, not the general
  standalone runtime unit-test target).

Before advertising the expanded protocol, finish and validate:

1. Enumerate and schedule every required producer stream from the immutable
   catalog, including unit/chunk scope and shared-output reconciliation. Clear
   each exact-input obligation only after those streams are complete. Bounded
   receiver-verified completion controls and local work-driver submission are
   wired; complete the missing requirement verifiers and the all-member drain/seal
   coordinator (bounded replicated provenance validation is wired). Replicated baseline scan
   completion alone is not an activation certificate.
2. Complete chunk/unit/document-extraction, neighbor-context asset, and promotion
   producer preparation and actual callbacks (root chunk callbacks with an
   existing or receiver-verified reconstructed inventory, and direct/accepted-upstream asset callbacks are wired),
   including remaining input proofs and absence cleanup. Direct document-vector
   authored certification now bypasses inference; historical/adopted authored
   output still requires receiver-local adoption evidence.
3. Historic provenance carried with retained effects and snapshots, atomic
   receiver-local adoption certificates, and contender/promotion recovery.
4. Complete scoped regeneration through the required-stream driver and all-member capability/catalog
   barriers, followed by the distributed crash, lost-reply, leadership-change,
   and standby-promotion fault matrix.
   Root retries now derive identity from the existing exact-input obligation
   and immutable plan, not from transport staging. Finish regeneration for the
   remaining scoped producers and prove it across incomplete/retired uploads,
   membership changes and standby promotion. A retry is not stream completion
   or all-member evidence, and must not replace those barriers.
