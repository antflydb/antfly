# SQL extraction: integration status, sessions, MERGE, TRUNCATE and online merge log

> Relocated verbatim from `docs/sql-extraction.md` (lines 881–1913) at commit 3ce5aff4ea on 2026-10-12. Durable decisions were folded into [SQL extraction](../extraction.md) before the move. This is a historical implementation log; it describes work at the time it was performed, and its counts and test totals are not current guarantees.

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
sets use external DISTINCT and reusable disk state when spilling is enabled.
Without spill I/O they retain the memory quota. Arbitrary pattern matching still has a
bounded per-outer-row probe cost rather than an index shortcut. Pattern state
is allocated only for this internal aggregate; the ordinary 10,000-row grouped
benchmark stays below 2 KiB of operator state. Pattern and operand
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
UTF-8 `application_name`, a bounded ordered `search_path` of authorized namespaces, and the
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
prepared statements and held cursors retain their original namespace.
`$user` expansion and unrelated settings fail explicitly. Ordered namespace
fallback advances only on exact table absence, not authorization or other errors.
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
owned sequence support, broader retirement/promotion fault coverage, complete
route-wide policy/session validation, and the full parity, fault-injection and
workload benchmark gates. Hosted external-FK generation retirement and graph
dependency barriers, including native standalone receipts, have public activation/recovery evidence; they are
not blanket missing features. Passing focused component tests is not completion
of SQL extraction.

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

### Session catalog and policy-setting boundary: implemented core, incomplete activation

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
The implemented core is described below. Pgwire overlays remain
connection-scoped; broader original `app.*`/policy/RESET ALL/DISCARD ALL
contracts still require exact-case and deployment evidence, not another
independent pgwire setting map.

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

Remaining: data-modifying CTE producers require an explicit single-statement
dataflow/commit model; read-only CTE mutation sources are admitted. The bounded direct full-key index probe now
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

The original hosted activation required Raft-bound generation-handoff receipt
authority. Native standalone owners now provide a distinct durable native
seal/install protocol, with linked external-FK/graph restart coverage. Admission
still requires an explicit owner identity capability
for every selected source and untouched FK parent; missing/unsupported capability
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
not relax public table routing or authorize providers without receipt authority.

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
[store-root enrollment](../../../store-root-enrollment.md) for operational details.

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
