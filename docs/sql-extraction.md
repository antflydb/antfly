# SQL extraction implementation ledger

SQL extraction starts at main `227f2dc39` after catalog #691 and relational #784.
The behavioral reference is `combine-pr-141-143-144` at `79644dfa1`.
This ledger is not a claim that all SQL surfaces are available.

**Status: SQL extraction is in progress, not ready as a complete SQL feature.**
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
  ALTER TABLE ADD PRIMARY KEY remains guarded: its non-null/unique fresh-generation
  rewrite needs stable mounted publication and failure evidence before activation.
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
fail during compilation. A row-valued subselect producing multiple columns is
not yet admitted; scalar subqueries inside an explicit ROW are.
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
`DEFAULT VALUES ... ON CONFLICT (id)` case remains unresolved: its unique
arbiter requires durable constraint activation and coordinated write admission,
which the simple mounted fixture cannot substitute with a direct batch write.

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
route. The production SQL adapter obtains authenticated scoped snapshots; it
does not grant SQL SET authority to mutate the durable registry. Pgwire now
holds typed, identity-fenced dotted-name overlays with SET/SET LOCAL/SHOW/RESET,
transaction/savepoint rollback, RESET ALL/DISCARD ALL, and prepared-plan epoch
checks. Those overlays belong to one connection; they are not restart-durable
or available to HTTP durable sessions. Durable-session overlays and
failover/security workload gates remain open. A durable setting registry alone
is not policy parity. Schema-bound policy definitions survive catalog Raft
replay, snapshot/import, and table retirement. SQL CREATE/ALTER/DROP POLICY
edits drafts only; ENABLE/DISABLE requests a separate durable owner
publication. Owner-native reads and writes have signed principal/generation
proofs and fail-closed gates, but unsupported search, restore, and mutation
routes cannot bypass policy enforcement or make the feature generally ready.
The native standalone owner now exercises publication, proofless-read denial,
signed reads and restart recovery. Owners configured for hot standby reject
protected policy state until catalog/bundle/receipt commits gain an ordered
outbox, replay, seed and promotion proof; this is a deliberate safety gate.

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

### TRUNCATE generation retirement: guarded, incomplete

The empty-generation implementation reuses durable restore staging: reserve fresh
table/range identities, prove each new owner has no primary, document-artifact,
identity, integrity-claim or ordered-index records, validate the empty cohort,
fence and drain old owners, and publish the replacement metadata atomically.
It does not copy, export or scan the old table's rows. Admission and resumed
workers require current whole-table administrator authority; row-filtered
credentials cannot authorize truncation. Success requires known publication,
not merely acceptance of the background job.

RESTART IDENTITY uses that fresh owner generation. The SQL catalog currently
has no owned sequence or serial declaration, and generated row IDs are secure
opaque values, so no sequence counter exists to reset. Once owned sequences
are introduced, their new counter generation must be part of the same staging
plan and publication transaction.

Graph indexes still require a graph cutover proof and are rejected before
admission. Graph-derived state and metrics must be fenced with their old owner
generation and proven empty on the new owner before this guard can be removed.

Incoming-FK CASCADE selection must never truncate an outgoing parent implicitly.
The current safe boundary rejects a selected child whose FK parent is outside
the selected cohort (`SqlTruncateExternalForeignKey`). This is **not complete
TRUNCATE support**: untouched parents retain inverse references containing the
old child's constraint generation. Ignoring a missing generation in the SQL/API
coordinator would be unsafe because native RESTRICT checks, participant commit
validation and referential-action cursors also consume those references.

Existing constraint retirement is correct but row-oriented: it scans children
and joins individual reference detaches to ordinary 2PC. Completing the
generation-level path without copying parent data requires a shared lifecycle:

1. Pin and reserve untouched parent definitions/ranges and exact old child FK
   generations in the staging plan. Fence and drain affected parent owners as
   well as old child owners; no schema or ownership change may invalidate this
   participant set.
2. Durably stage generation-specific inverse-reference tombstones on those
   parent owners. Pending tombstones must not change reference visibility.
3. Publish the new child generation, then activate only the corresponding
   tombstones under verifiable publication authority before releasing parent
   write fences. Before publication, cancellation removes pending state and
   restores admission; after publication, recovery must finish activation.
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
references. The SQL child-only external-parent TRUNCATE route remains guarded.
The parent activation step now retains its owner fence across restart, and
generic release/cancel cannot lift it; only a metadata-authorized ACK after
child publication releases admission. This closes the pre-publication orphan
window but does not replace end-to-end coordinator crash/lost-ACK evidence.
Ordinary FK schema edits now have a metadata-owned parent/source publication
and ACK protocol, while FK-bearing initial CREATE uses hidden child owners and
publishes the table only after parent and child receipts. Standalone initial
self-referential FK CREATE now uses authenticated native hidden-child owner
receipts and an atomic local catalog publication; a two-range restart fixture
and public valid/orphan-write fixture cover this path. Standalone initial FK
CREATE with external parents and initial MATCH PARTIAL declarations remain
guarded until their owner or atomic support-index paths are proven. Hidden-owner
standby replay has a Raft-bound batch envelope, but seed/promotion fault
coverage remains a release gate.

Initial MATCH PARTIAL support-index installation now reserves the parent
descriptor, hidden child identity, locks, and durable work in one metadata
transaction. The hosted path now seals parent support, places a private child
group, and admits a local Raft leader from a paired public/private metadata
cut. Hidden-child topology proposals now carry an exact private compiled-owner
descriptor through Raft instead of resolving an unpublished public table, and
skip the public dense-repair admission probe only for that no-document-write
control. The mounted lifecycle still stalls in `provisioning_child` without a
child receipt after these changes; the private owner/control apply boundary and
rollback fault matrix remain unproven, so initial MATCH PARTIAL CREATE stays
publicly guarded.
Standalone cancellation has an exact hidden-owner retirement
path and a checksummed local intent. Its self-FK two-range crash/restart and
terminal cold-root tests pass, but this does not retire offline hosted replicas:
distributed cancellation still needs metadata-owned per-store/replica work,
placement fencing, and durable ACKs before that route is released.

Hosted cancellation must retain a bounded historical set of every hidden child
replica actually admitted by placement, keyed by group, store incarnation, and
replica incarnation; current placement alone is insufficient after removal or
node loss. The terminal canceled metadata transaction should publish an indexed
group-to-plan/child/range proof and per-replica retirement work, never for a
published child. A returning store pages only its work, verifies its cold AICH
bootstrap and canceled receipt against that immutable proof, fsyncs a local
retirement intent, drains/deletes the exact root and local replica catalog, then
submits an idempotent incarnation-fenced ACK. Metadata may compact the work
only after every recorded replica ACKs, retaining a stale-rejoin fence. Required
fault tests include an offline store, placement removal before cancellation,
crashes before the cancel CAS and between unlink and ACK, wrong bootstrap or
reused group ID, store-incarnation replacement, and published-child exclusion.
The metadata placement CAS now assigns a durable initial-FK root generation,
preserves it across same-owner refresh and publication, and rotates it for a
different node, store, or replica; the local replica catalog persists that
generation before owner publication. This is admission identity groundwork,
not a physical-disk identity or a deletion authorization. If an offline disk
is replaced under the same node/store/replica while its placement remains,
metadata could otherwise reuse the generation. Hosted GC therefore still
requires a persistent store-root UUID proven at registration and bound to
placements, owner receipts, work, and ACKs (or an enforced removal/re-admission
CAS), plus an explicit bootstrap-source protocol for replacement roots. Until
that proof and the offline fault matrix pass, hosted cancellation remains
guarded and no generation-only ACK may compact retirement work.

Acceptance needs crash/lost-ack tests at each fence, publication and activation
boundary, cancellation on both sides of publication, parent mutations and new
child inserts during cutover, stale prepared participants, nullable/MATCH PARTIAL
witnesses, and GC/restart bounds. No disconnected tombstone contract or
coordinator-only filter constitutes completion of this boundary.

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
