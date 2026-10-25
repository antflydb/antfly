# SQL extraction

Status: in progress. SQL is not yet a complete feature; the original-corpus
release gate still blocks (`python3 scripts/check_sql_parity_inventory.py --report`).

SQL extraction started from main `227f2dc39` after catalog #691 and relational
#784. The behavioral reference is `combine-pr-141-143-144` at `79644dfa1`; it
is a source of test cases to port selectively, not code to copy. Current
original-case dispositions live in [the parity inventory](parity-inventory.md);
full native PostgreSQL regex parity is a separate required workstream in
[the regex parity design](regex-parity.md). Asynchronous-artifact transfer,
which TRUNCATE depends on, and distributed online merge are specified in
[ordered artifact publication](../ordered-artifacts.md).

Dated implementation logs, per-case evidence prose, validation runs and local
measurements formerly in this document are in [history/](#history).

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
belong in pgwire, not the native durable types. PostgreSQL is the semantic
authority (PostgreSQL 19 is the target; the oracle currently runs PostgreSQL
18.6). SQL syntax or schemas that disagree with PostgreSQL must not gain
positive parity credit through SQLite emulation or fixture-only authority.

The mega-branch reference implements parts of the remaining gaps on its older
adapter and row-source contracts (`sql/lower_dml.zig`,
`api/sql_adapter_integration.zig`, `api/table_writes/relational_mutation.zig`,
`api/http_server.zig`, `api/auth_sql_adapter.zig`; its TRUNCATE also parsed
CASCADE and RESTART IDENTITY). Port behavior and tests selectively onto the
immutable bound relations, retained statement capture, owner-fenced commits and
current catalog/constraint generations. No legacy adapter fallback may be
introduced to claim parity.

## Capability surface

| Surface | Available shape | Remaining boundary |
| --- | --- | --- |
| Reads and operators | Joins, sets, CTEs, aggregates/windows, typed batch kernels, spill-backed blocking operators and pgwire continuation pages | Exact original-case adjudication; broader correlated/lateral shapes; remote workloads rather than only local kernel benchmarks |
| Mutations | Native/document DML, joined UPDATE/DELETE, read-only CTE sources, MERGE, normalized RETURNING and guarded UNIQUE conflict ownership | Data-modifying CTE dataflow; broader demand-masked/correlated conflict subqueries and index membership proofs; MERGE partial/expression-key probe planning |
| Recursion | Bounded linear delta worklists, typed deduplication and captured physical sources | Mutual/nonlinear recursion, recursive aggregates/windows and nullable-side recursive self joins reject explicitly |
| Sessions and protocols | READ COMMITTED sessions/savepoints, durable HTTP prepared/connection resources, pgwire scroll/hold cursors, typed settings and ordered multi-namespace lookup | Stronger-isolation provider activation/fault coverage, TTL visibility contract, guarded Lite sessions, restart/failover ownership and complete setting/policy parity; HTTP has no connection-owned cursor resource |
| Catalog and retirement | Native schema/index/constraint, serial/identity defaults and owned-sequence DDL; hosted and native standalone external-FK/graph TRUNCATE with durable publication/recovery receipts | Sequence-aware initial FK creation and counter-aware table-archive imports, broader schema rewrite/restore and standby-promotion gates; pending receipts are not synchronous publication |
| Row policies | Versioned settings/policy catalog, owner proofs and guarded publication, including native/standby component coverage | Every public protected route, revocation/restore and promoted-primary fault/security validation; unsupported providers remain closed |
| Typed values | Typed rows, lossless SQL arrays (public envelopes, pgwire OIDs, stored `sql_array` columns), precise builtin widths, exact NUMERIC with precision/scale modifiers | Array DDL beyond declarations, element-changing durable casts, non-NULL SQL array defaults, array-valued ordered keys, temporal domains, non-C collations |
| Lake SQL | Read-only Parquet/Iceberg attachments, snapshot/version pinning, pruning, bounded typed cursors, shared caches and spill | Remote end-to-end performance/recovery evidence; writes, nested/binary inference and native SQL decimal semantics are not claimed |

Do not restate case counts in this document. Use
`python3 scripts/check_sql_parity_inventory.py --report` for current family
counts; `--family read --report` limits reporting only. Use
`--evidence --family read` or `--evidence` with repeated `--gate NAME` to run
recorded evidence without claiming release readiness. Families without recorded
gates, unknown gates and gates outside the selected family fail rather than
reporting an empty successful run. Gate selection is not allowed with
`--release`, and `--release --family read` still validates the entire inventory.
Zig 0.17 evidence commands use repeatable `-Dtest-filter=...` options so native
test owners receive compile-time selection; shared gate filters are grouped into
one build per owner. Blocking dispositions are unadjudicated contracts, not
counts of missing features; original rejection contracts need diagnostic or
nonadmission evidence, not positive functionality.

Start with exact-case evidence on already admitted shapes and implement only
gaps exposed by that comparison. Keep deployment activation gates distinct:
distributed isolation/RLS integrity, owned sequence semantics and shared
artifact-stream certification each need their own fault matrices. Broader
graph/enrichment/resolver provenance, producer completion, adoption, recovery
and all-member capability barriers are shared storage work, not SQL grammar.
Passing focused component tests is not completion of SQL extraction, and no
case receives release credit because an overlapping component test passed.

## Schema format and reader capabilities

Every durable SQL-visible type or expression feature advances the native runtime
schema format (`storage_format_version` in
`zig/pkg/antfly-embedded/src/storage/schema.zig`). Serialization refuses to
downgrade a schema that requires a newer format, and the transactional catalog
refuses to bind a schema whose requirement exceeds the catalog's advertised
reader capability. New schema publication atomically advances the catalog
capability. Older schemas stay readable without a write on open.

| Format / capability | Required when the schema contains |
| ---: | --- |
| 15 | Deployed document/relational catalogs (baseline; readable without upgrade) |
| 16 | A precise SQL builtin descriptor on any physical column (integer widths, float4, UUID, text) |
| 17 (ASCH 17) | A `sql_array` typed-array column with its mandatory element identity |
| 18 | Typed durable expression programs: builtin-width literals/arithmetic/checked casts, assignment-cast defaults, ordered `case_when` |
| 19 | Durable predicate programs: `IN`/`NOT IN` membership and integer remainder (even string-only membership) |
| 20 | Scalar NUMERIC or NUMERIC-array physical columns |
| 21 | Exact-NUMERIC schema-VM programs (defaults, generated columns, CHECK, index/UNIQUE expressions), regardless of output type |
| 22 | Exact NUMERIC validation of a public relational scalar NUMERIC column (bounds, const, enum, multipleOf) |
| 23 | NUMERIC precision/signed-scale modifiers on columns or in expression programs |
| 24 | Array-dependent expression programs, even with scalar or boolean results |
| 25 | Durable `ARRAY[...]` constructor programs |

Protocol versions for other subsystems are separate: online merge requires data
protocol v14 and metadata protocol v20; hosted initial-FK contracts require
metadata protocol v19 on every voter and learner; lake publication requires
metadata decoder capability 22 and reader protocols 23–26 (see
[remote table serving](../../../zig/REMOTE_TABLE_SERVING.md)); the expanded
ordered-artifact protocols 15 and 17 are not advertised.

Runtime-status wire profiles are separate from these topology capabilities:
v19 carries media output counters, and v20 adds source authority and native
commit cuts. Rolling negotiation retains v19 media fields while withholding
source-authority observations until the named v20 profile is ready.

## Statement execution

### Reads and operators

- Ordinal-bound scalar programs, three-valued predicates, bounded top-K,
  grouped/global aggregates, HAVING, joins, derived tables, nonrecursive and
  bounded linear recursive CTEs, set operations and typed INSERT-from-query
  share memory admission and whole-statement validation before mutation.
  Query-shape parameter constraints propagate through derived queries, CTEs and
  sets before programs are emitted.
- Equality-correlated EXISTS/NOT EXISTS, scalar and single-column IN/NOT IN
  subqueries lower to grouped hash joins/membership under the outer query's
  coordinated capture, retaining three-valued semantics for empty sets,
  duplicates and NULLs. ANY/SOME/ALL use grouped extrema and null/count
  evidence; one ordered comparison may accompany equality keys. OR-correlated
  EXISTS distributes into at most eight witness branches. Multiple correlated
  ranges, correlated set/group/window/top-K forms and lazy outer subquery
  branches remain unsupported unless lowered through a demanded LATERAL/Apply
  producer (see the inventory for the Apply-based shapes).
- Row-valued `IN`/`NOT IN` uses `sql/tuple_membership.zig`, a shared-prefix trie
  with typed comparison and a fixed stack bounded by the admitted 256-column
  arity. `(NULL, 1)` is unequal to `(2, 2)` but potentially equal to `(2, 1)`;
  empty inner relations are false for `IN` (including an all-NULL left tuple),
  and `NOT IN` negates only true/false, retaining UNKNOWN. Array keys preserve
  dimensions, bounds, exact integers and element NULLs; JSONB null is a non-NULL
  value. Array equality requires identical element types. A failed build poisons
  the index. EXPLAIN identifies NULL-aware membership. Spill-backed membership
  is not implemented; retained-byte, row and work admission fail closed.
- Quantified LIKE/ILIKE subqueries use a distinct, quota-accounted pattern set
  per correlation key with a step-limited matcher; large sets use external
  DISTINCT when spilling is enabled.
- Named WINDOW definitions resolve in query-local scopes before AST publication.
  GROUPS frames use indexed peer groups; EXCLUDE produces at most three intervals.
  Window execution shares partition/order sorts and segment trees; internal
  aggregate nodes use wider numeric state, and only requested results are
  range-checked.
- Explicit `MATERIALIZED` and default multiply referenced CTEs share one
  quota-bound typed producer; `NOT MATERIALIZED` and single-reference unhinted
  CTEs stay inline/pipelined. Producer demand propagates backward through
  inlined CTEs so a downstream reference does not rescan an upstream producer.
- Linear recursive CTEs use a delta worklist, typed UNION DISTINCT visited keys,
  one capture of physical sources and reusable static-side hash indexes. Seed
  types determine recursive outputs; explicit casts supply widening.
- Mixed and qualified wildcards expand through the pinned visible layout before
  binding, preserving duplicate output labels and quoted identifiers. Expansion
  checks the 1,024-column budget before allocating. Final sort heap capacity is
  bounded by actually materialized rows. Window ORDER BY expands output aliases
  once and rejects ambiguous aliases. Three-part column references
  (`schema.table.column`) are admitted against the pinned
  database/namespace/table scope; aliases hide the original name.
- Table references accept ONLY across SELECT, INSERT, UPDATE, DELETE, MERGE and
  TRUNCATE; without inheritance it names the same exact table.
- EXPLAIN binds under the inner statement's authority and renders bounded text
  or versioned native logical-plan JSON. Planning alone opens no row cursor and
  executes no mutation. SELECT ANALYZE executes once through the normal typed
  operators, preserves authorized volatile effects, drains every projected cell
  and discards result payloads with
  resettable scratch. Result-response row limits do not truncate the measured
  query; scan, memory, spill, cancellation and authorization limits still apply.
  Mutation ANALYZE remains explicitly unsupported.
- EXPLAIN options use PostgreSQL defaults and Boolean grammar. TIMING OFF removes
  per-operator clock reads independently of SUMMARY. COSTS uses the declared
  native estimate model when metadata supports it; absent estimates stay unknown.
  BUFFERS reports coordinator memory and temporary spill bytes, with explicit
  units. Provider storage/cache I/O and journal/WAL counts remain unavailable,
  never synthetic zero. SETTINGS exposes the authorized settings epoch and
  execution limits, without arbitrary setting values or policy secrets. Native
  JSON describes Antfly's logical plan and inclusive pull-boundary measurements;
  it does not impersonate PostgreSQL physical nodes, buffer blocks or WAL units.
- Binding precedes a bounded, request-owned immutable expression pass shared by
  execution, streamed reads and EXPLAIN. Custom plans use the owned typed
  parameter frame. Constant-false subplan demand and unused pure derived outputs
  suppress unreachable evaluation; group keys, HAVING, ordering and side effects
  stay live. Stable/volatile functions are never executed during preparation.

### Exact indexed document predicates

Native `full_text_search(...)` document/view queries use an exact postings
producer. A ranked top-K search followed by residual SQL filtering can
omit valid rows, and primary scans cannot establish mandatory index readiness.
The retained source must pair one primary transaction with a frozen full-text
inventory and deletion cut after its selected derived watermark reaches that
primary cut. Later updates, deletes, compaction and index retirement must not
change cursor membership. The text writer's ordinary snapshot intentionally
shares live deletion masks; retaining it alone does not supply this contract.
Catalog and analysis leases protect index-entry relocation during preparation
and capture; neither lease remains held while paging.

The selector pulls one segment bitmap at a time with bounded memory, explicit
work per pull, deadline/cancellation checks, and empty nonterminal pages. It
retains physical readers through compaction and resolves only selected document
identities. Existing document pages remain responsible for primary visibility,
TTL, mandatory authorization, typed projection and residual SQL evaluation.
Normal and bound view queries must use this producer through the existing
retained routed-read machinery. Unsupported placement or provider capability
must reject before output; it cannot fall back to a capped search or skip a
required index. Catalog generation, attribute/schema and index readiness guards
remain mandatory across prepared execution and cold reopen.

The mounted production campaign closes 15 original indexed-read contracts,
including lateral array expansion and unbounded COUNT, with full result/OID/NULL
comparisons before and after catalog/owner restart and mandatory caller filters.
Its PostgreSQL witness explicitly lowers the Antfly-only implicit-document text
predicate to field-scoped PostgreSQL text matching; it never claims PostgreSQL
implements that extension or executes the lowering on Antfly. Owner admission
requires an exact query digest, including across routed retained-read transport;
older or mixed owners cannot silently supply unfiltered rows.

Only positive WHERE conjuncts over one unambiguous document source are selected.
Row bounds, aggregation, shared physical materialization, outer joins and security
barriers remain guarded. Ranked search, stronger delayed snapshots and staged
mutation overlays are separate capabilities. Ordinary WHERE clauses retain the
allocation-free path through predicate extraction.

### Mutations

- Expression INSERT/UPDATE use whole-batch validation, exact integers and
  explicit SQL-null versus JSON-null provenance. Relational RETURNING uses native
  normalization under the schema fence and prepares all output before commit;
  DELETE returns the version-fenced preimage. Omitted INSERT identities use the
  shared native secure row-ID generator.
- Document reads have declaration-derived shapes and retained native
  transactions with bounded pages, TTL, authorization and typed projections.
  Document UPDATE preserves undeclared fields from the full raw preimage;
  exact-byte row digests prevent stale writes even when custom TTL timestamps
  are unchanged.
- Multi-row `INSERT ... VALUES` uses a flat VALUES source with ordered arms;
  adjacent literal-only arms share a typed row block, and only one nonliteral arm
  iterator is active. Scalar subqueries in VALUES use the bounded INSERT-source
  path, including self-table reads and pre-commit cardinality errors.
- `DEFAULT` (INSERT DEFAULT VALUES, per-cell VALUES DEFAULT, UPDATE SET DEFAULT
  including inside ROW, MERGE and `ON CONFLICT DO UPDATE SET col = DEFAULT`)
  omits the cell so native normalization applies the declared default and
  recomputes generated columns. Generated columns accept only DEFAULT. The
  omission mask keeps DEFAULT distinct from explicit SQL NULL through binding
  and RETURNING.
- Explicit ROW and parenthesized tuple assignments lower to one simultaneous
  assignment plan; duplicate targets and arity mismatch fail during compilation.
  A row-valued subquery assignment binds positional outputs through one guarded
  capture and checks scalar cardinality before commit.
- Joined UPDATE/FROM and DELETE/USING (including explicit joins and read-only
  CTE sources) carry target versions, document digests and a compact
  physical-presence token through one statement capture; untouched omitted
  cells stay physically absent. UPDATE selects one coherent matched source row
  per target (which one is unspecified); MERGE keeps its own cardinality
  contract. Source-aware RETURNING binds the complete authorized join scope and
  evaluates against normalized target images and the same coherent source
  representative; RETURNING errors and cancellation precede publication.
- Typed `TIMESTAMPTZ '...'` literals validate offsets and normalize to UTC before
  mutation admission.

### Conflict arbitration

- Primary-identity ON CONFLICT uses exact observed row fences. Targetless DO
  NOTHING coordinates primary identity and every supported immediate unique
  arbiter; only admitted candidates reserve in-batch claims.
- Explicit targets infer partial unique indexes through a bounded typed
  predicate proof shared with native index membership (conjunctions of
  column/literal comparisons and IS [NOT] NULL). Expression keys share the
  native tuple VM; expression/predicate changes fence the durable claim
  generation. Composite unique targets use the native tuple codec and durable
  compare-claim observations that survive session merging and savepoints.
- `ON CONFLICT ON CONSTRAINT` binds the exact folded or quoted name to one
  durable unique generation; it never widens to equivalent constraints. Absent
  names fail with `42704` and CHECK/FK names with `42809` before proposed-row
  evaluation; deferrable arbiters fail with `55000` after proposed defaults,
  matching [PostgreSQL's immediate-arbiter contract](https://www.postgresql.org/docs/18/sql-insert.html).
- Ordinary deferrable UNIQUE declarations share the native claim authority.
  Commit validates the complete final overlay, including swaps; SET CONSTRAINTS
  IMMEDIATE validates retroactively before publishing its mode change.
  Deferrable UNIQUE keys are not eligible FK parent targets.
- Conflict-assignment scalar subqueries use a post-owner masked Apply: a direct
  uncorrelated scalar SELECT is evaluated only for rows that actually conflict
  and satisfy DO UPDATE WHERE (eager evaluation was unsound). Conditional or
  nested scalar subqueries, owner-correlated reads and secondary-index probes
  remain rejected until their demand masks and index membership proofs exist.
- Conflict owner point reads reuse reclaimed cursor/page scratch per owner and
  consume one batch-wide page quota; captured INSERT-source pages have separate
  admission. Lite uses native durable prepare/commit for single-handle
  constraint expansion and rejects out-of-handle dependencies.

### Expression DDL

SQL CREATE/ALTER lowers immutable scalar defaults and STORED generated columns
into the shared durable expression VM. Binding sees the complete candidate
schema, including forward base columns; generated self/cross references and
request-bound parameters are rejected, and subqueries are prohibited in
defaults. Checked numeric assignment casts retain write-time overflow and
atomic batch rollback. Populated ADD uses the metadata-owned staged rewrite
(projecting only expression inputs) rather than publishing nullable generated
columns without backfill. Failed candidate edits leave the original schema
untouched. ALTER TABLE ADD PRIMARY KEY uses the metadata-owned non-null/unique
fresh-generation rewrite. CHECK declarations (column/operator and explicit
expression forms) compile to one typed plan and do not serialize cells into
ordered index keys; persistent key-size limits apply only to actual index
encoding. Partial-index constant bounds fold through the same schema VM once at
DDL time. Volatile defaults/sequence authority, virtual columns and array DDL
beyond typed declarations remain explicit gaps.

## Typed values

### Rows, results and parameters

Internal relation pages retain owned complete Datums and share one
name-to-ordinal directory per page; primitive column pages remain available for
scalar-only projections. Internal binding retains array-valued outputs; public
binding rejects unsupported wire result types before execution. Nested blocking
queries, scalar owners and window input use one typed sink boundary (shared spill
cursor or bounded no-I/O replay cursor). Sets resolve child nodes before parents
and coerce each arm before comparison; VALUES selects one common type across all
rows. Unknown results become text inside their producer and cannot later infer a
numeric parameter from an outer set or assignment.

Native scalar programs bind precise immutable parameter descriptors (primitive
widths and array element identities) into one quota-admitted statement frame that
decodes inputs once. Pgwire Parse/Bind, SQL PREPARE and durable HTTP prepared
resources carry the same descriptors; resources with missing or inconsistent
contracts must be prepared again. The result boundary encodes SQL integers as
decimal strings in HTTP JSON while JSON-column numbers stay numeric.

### Arrays

- Typed arrays (`array_value.zig`) preserve element widths, up to six dimensions,
  non-default lower bounds and per-element SQL NULL. Ordering and hashing agree
  on bounds, NULLs, signed zero and NaN. Arrays never use JSON-null sort
  prefixes or primitive vector kernels.
- The public result envelope (OpenAPI-generated) represents non-NULL arrays as
  dimensions (`length`, `lower_bound`), flat row-major values and aligned
  SQL-null flags. Element type is a bound-column descriptor; JSON shape never
  chooses a SQL type. Integers use exact canonical decimal strings; finite
  floats are JSON numbers; NaN and infinities use explicit tokens. JSONB null
  elements and SQL NULL elements stay distinct.
- Pgwire advertises the exact PostgreSQL array OID and encodes text and binary
  DataRows from the descriptor. Prepared/cursor shape checks include element
  identity. DataRow admission reserves all remaining cell length headers and
  admits each payload against remaining frame space.
- Stored arrays: relational root properties declare
  `{"type":"sql_array","x-antfly-sql-type":"int64","nullable":true}`; all
  builtin element identities bind to ASCH 17 typed-array columns. Implicit
  JSON-array conversion, nested annotations and document-mode declarations are
  rejected. The compact schema-bound codec uses fixed-width slots or
  variable-width offsets with a NULL bitmap and rank checkpoints every 64 cells.
  Semantic hashing covers elements, shape, bounds and NULLs, not physical layout.
  Strict restore checks canonicality (including JSONB semantics) even under a
  valid checksum; authenticated targeted reads inspect only addressing extents.
  Array ordered/unique/FK index keys remain guarded.
- Array functions follow [PostgreSQL's contracts](https://www.postgresql.org/docs/18/functions-array.html):
  `array_position(s)`, `array_remove`, `array_replace`, append/prepend/cat and
  array `||` use anycompatible element resolution with explicit coercions;
  search/removal use IS NOT DISTINCT FROM; concatenation admits equal or
  adjacent ranks and preserves first-axis lower bounds. `array_to_string`,
  overlap, `string_to_array` and JSONB `?|`/`?&` consume typed `text[]`.
- Durable array expressions: the row VM owns a logical value domain separate
  from ordered-key operands; arrays are pinned canonical bytes plus element
  identity. Identity casts and NUMERIC-array modifier casts are admitted;
  element-changing casts, dynamic constructors beyond `ARRAY[...]`, non-NULL SQL
  array defaults and array-valued ordered keys remain guarded.

### Precise scalar types

SQL binding and runtime schemas share one storage-independent builtin identity
(retaining PostgreSQL scalar/array OIDs). Relational root scalar properties
declare `x-antfly-sql-type` from the generated `SQLBuiltinType` enum; the native
property type must match. Standard `format` annotations do not acquire SQL range
semantics. An immutable compiled column plan normalizes input once before
checks, hashes and index extraction: integer widths are checked without float
conversion, float4 is rounded once to its canonical widened value, UUIDs are
canonicalized. Numeric strings are not admitted by the native JSON write API.
Restore verifies rather than repairs values. Ordinary schema updates cannot
reinterpret a retained column under another SQL domain. Native nonfinite scalar
number storage is unsupported.

### Exact NUMERIC

- The kernel uses canonical base-10000 limbs; display scale is separate from
  identity. All operations share bounded work, allocation admission and
  cancellation; failures are sticky with clean retry. Dense multiplication and
  division are quadratic and explicitly work-bounded. Precision/scale coercion
  rounds half away from zero before overflow checking and supports negative
  scales and scale greater than precision. Float-to-NUMERIC uses PostgreSQL's
  6/15 significant digits with ties-to-even.
- NUMERIC has scalar OID 1700 and array OID 1231. Public result columns return
  decimal strings preserving precision and display scale; special values use
  PostgreSQL tokens. Result descriptors carry an optional precision/signed-scale
  modifier, retained only when every contributing expression has the same
  modifier (`SQLNumericModifier` in OpenAPI; pgwire emits PostgreSQL's typmod).
- Decimal/scientific literals and integers outside i64 keep their exact source
  spelling and bind once to immutable NUMERIC constants. abs, ceil, floor, sign,
  mod, round, trunc and sqrt use exact NUMERIC overloads; integer/real inputs
  select double precision as in PostgreSQL. sqrt clamps display scale to
  0..1000 with at least sixteen significant digits.
- SUM/AVG use grouped i128 base-10000 bucket lanes; aggregate checkpoint
  version 2 binds input element identity. Exact SUM/AVG states are eligible for
  scan workers and hash-spill reducers.
- Ordered keys encode PostgreSQL total order lexicographically (separate ranks
  for −∞, negative, zero, positive, +∞, NaN), omit display scale and are
  prefix-free. Binary wire decoding reports `08P01` for short framing and
  `22P03` for invalid content.
- Storage: the `numeric` physical column type holds canonical PostgreSQL binary
  payloads; unannotated native/document `number` fields keep f64 semantics.
  Public relational columns accept the numeric SQL annotation and
  `x-antfly-sql-numeric-modifier`; assignment coercion runs before constraints,
  indexing and dependent generated expressions. Physical restore verifies
  constrained stored values and never rounds or repairs them. Logical restore
  accepts equivalent display scales but rejects values that assignment would
  change. Generic document predicates reject NUMERIC cells rather than
  comparing binary or rounding through f64.
- Schema expressions (defaults, generated, CHECK, expression/partial indexes)
  carry exact NUMERIC through the durable VM, with one sticky execution context
  for work, cancellation and bytes shared across normalization, defaults,
  generated values, recursive constraints, CHECK and physical restore.
  Overflow in a cast is raised when the selected branch executes.

Remaining typed-value work: temporal domains and datetime binary parameters,
dynamic text casts, nonfinite floating-domain casts, broader numeric functions
(exact power), mixed-domain join-key normalization, native NUMERIC vector
kernels, low-level physical codec/hash shared admission, and the array gaps
above.

## Sessions, settings and row policies

The original corpus includes `app.*` session variables, `current_setting`,
role/database defaults, RLS policies, `RESET ALL` and `DISCARD ALL`. A pgwire
string map alone would be unsafe: policies must not trust a value a client can
change. Settings therefore live in a versioned registry in the SQL catalog with
typed values, role/database defaults, explicit write authority and an immutable
request/session view. Each statement binds `current_setting` against that view
with its schema epoch; native policy evaluation and SQL scalar programs receive
the same pinned view, including remote readers. Transactions and savepoints
journal overlays; RESET/DISCARD operate on the typed registry. Prepared plans
retain setting dependency identities and evaluate authorized values at
execution.

- A typed scoped snapshot pins names, identity generations, defaults and
  authorized session overlays. Missing capture, stale generations, dynamic
  names and client overlays on policy-sensitive values fail closed.
- Metadata Raft owns durable setting records, revision-fenced publication,
  snapshot/import state and an administrator-only mutation route. SQL SET
  changes only an authorized session overlay, never the durable registry.
- HTTP `/sql` sessions accept typed dotted-name SET/SET LOCAL, RESET, SHOW and
  RESET ALL through the same complete-command parser as pgwire. RESET ALL clears
  the active and commit-time durable overlays atomically. `SET LOCAL name =
  DEFAULT` removes only the active override. HTTP PREPARE with `session_id` holds
  the session lease while describing; resources bind the exact session ID and
  scoped setting-catalog epoch, and execute rejects a different/ended session or
  changed catalog.
- HTTP durable idle connections are principal- and owner-bound. Committed
  non-LOCAL settings carry back to the idle connection; rollback restores its
  prior overlay. HTTP `DISCARD ALL` resets the overlay, advances its generation
  and retires only that connection's prepared resources; it refuses active or
  uncertain transactions. HTTP has no connection-owned cursor resource.
- Pgwire SET/LOCAL/SHOW/RESET supports bounded `statement_timeout`, bounded UTF-8
  `application_name`, a bounded ordered `search_path` of authorized namespaces,
  immutable UTF-8 `client_encoding` (`SET NAMES` included) and typed `app.*`
  overlays. Pgwire overlays belong to one connection and are not restart-durable.
  Outside a transaction, `DISCARD ALL` also closes prepared plans, portals and
  held cursors after the reply. `$user` expansion and unrelated settings fail
  explicitly. Namespace fallback advances only on exact table absence, never on
  authorization or other errors. Prepared statements and held cursors keep their
  original lookup namespace. All describe/execute/stream paths classify
  connection-owned settings through one typed command boundary, so a setting can
  never open a storage cursor.
- Pgwire PREPARE/EXECUTE/DEALLOCATE shares the bounded wire prepared registry.
  EXECUTE evaluates typed arguments in an empty binding environment, never
  interpolating SQL. DECLARE/FETCH/MOVE/CLOSE cursors use the bounded pull
  stream; blocking shapes execute once into a quota-bound typed spool before
  DECLARE publishes the name. SCROLL uses a lazy typed spool. WITH HOLD drains
  under the original transaction and detaches only after confirmed commit;
  ambiguous commit never publishes a held cursor. Held fetches recheck current
  authority and pinned source identity.
- HTTP prepared resources are independent of transaction commit, with
  principal/owner checks, expiry, bounded admission and immutable binding
  manifests. They survive restart on the owning node; failover to another owner
  is rejected rather than silently retargeting the statement.

Row policies are catalog-versioned authority, not a SQL projection filter. A
policy record binds table ID/schema epoch, command and role scope, USING/WITH
CHECK expressions, setting dependency identities and a publication generation.
Every protected read owner receives an authenticated principal and immutable
policy/setting view, applies USING before pagination and returns an owner proof
tied to the same read cut. Mutation preparation checks the old image for
UPDATE/DELETE visibility and the normalized new image for WITH CHECK inside the
guarded commit. API, pgwire, Lite, remote reads and non-SQL mutation routes must
not be able to select an unprotected backend; a missing/stale view or
unsupported owner denies. SQL CREATE/ALTER/DROP POLICY edits drafts only;
ENABLE/DISABLE requests a separate durable owner publication, which must commit
before it authorizes traffic. Prepared statements revalidate policy and setting
generations. Ordinary document reads obtain the principal-independent policy
publication stamp with internal-service identity; an active stamp still requires
an authenticated principal and owner-verified proof, and missing credentials
never substitute for an absent policy. Until every public route and
revocation/failover/restore test is complete, active policy publication remains
guarded by capability checks.

## MERGE

The compiler owns a bounded MERGE AST (source relation, join condition, ordered
conditional arms, expressions, RETURNING) and rejects invalid matched/action
pairs, duplicate assignments and INSERT arity mismatches. The candidate binder
pins the authorized target once, binds a source-preserving join, retains
version/digest metadata and projects only referenced fields. Arms are selected
lazily under three-valued logic, one row-local arena reset per candidate.
Duplicate source actions on one target are rejected before image preparation.
Full-scan, point and ordered-index candidates share one bounded typed capture
with independent replay readers; all readers close before writer admission.

MERGE executes only through a backend that stages coordinated target/source
range proofs with the mutation in one durable transaction. Autocommit MERGE uses
an implicit serializable transaction; explicit stronger-isolation sessions use
the same path; read-committed or plain batch backends reject before opening the
candidate read. Unknown decisions return a reconciliation ID and are never
replayed. RETURNING projects normalized target values with unchanged source
values through a prepared-image commit path. HTTP prepare/execute pins both
source and target identities. Source-only inserts carry the native
unique-absence fence. Read-only CTE sources are admitted; data-modifying CTE
producers need an explicit single-statement dataflow/commit model.

Access-path selection:

- `target._id = source.key`: a 129-row decision capture, then deduplicated
  primary-key point scans of up to 64 keys per native capture; sources over 128
  rows fall back to the source-preserving hash join. Every observation,
  including misses, joins the serializable read set.
- Complete direct total-index equality (including composite keys bound in
  physical index order): catalog-pinned index identity, require-index equality
  scans, exact span proofs for matches and misses, source-key deduplication and
  a 16-row nonunique fanout cap. Sources above 32 rows, saturated fanout and
  non-READY indexes use the coordinated join. This is a conservative crossover,
  not a measured cost model. Partial and expression ON keys use the full join.
  Native `auto_index` alone cannot provide the guarantee because it may choose a
  primary scan when the index is not READY. Once a session has staged table
  state, secondary-index matches use the full join.
- Index-span proofs are tagged observations pinned to the READY generation and
  encoded equality prefix, never a reused primary bucket. Prepare reserves
  writer keys for prior and candidate tuple prefixes before admitting a primary
  intent; forward/reverse index effects increment the same span counters
  atomically with the primary row (an unchanged tuple still advances its span
  when the primary row changes). Commit validates the counter and in-flight
  reservations. Prefix/range scans keep the 257 conservative primary buckets.
  A sorted multi-get over those 257 buckets was measured slower than scalar gets
  and rejected; reducing proof cardinality is the needed change.

Acceptance still requires duplicate-match, NULL-ON, ordered-arm, conflict,
concurrent-insert, cancellation/unknown-outcome and cross-owner fault tests.

## TRUNCATE generation retirement

TRUNCATE reuses durable restore staging: reserve fresh table/range identities,
prove each new owner has no primary, document-artifact, identity,
integrity-claim or ordered-index records, validate the empty cohort, fence and
drain old owners, and publish replacement metadata atomically. It never copies,
exports or scans old rows. Admission and resumed workers require current
whole-table administrator authority; row-filtered credentials cannot truncate.
Success requires known publication, not acceptance of the background job; the
public contract returns 202 with a pending receipt.

- Hosted owners use Raft-bound generation-handoff receipts; native standalone
  owners use a distinct durable native seal/install protocol. Admission requires
  an explicit owner identity capability for every selected source and untouched
  FK parent; missing capability returns unsupported before a durable job exists.
  Hidden target capabilities use a closed, service-authenticated control read
  with a fresh read-index barrier. An unsupported pre-cutover attempt follows
  durable cancellation; a capability mismatch after irreversible cutover is a
  recovery error that must retain fences.
- Document RESTART IDENTITY uses a fresh owner generation; opaque document
  IDs do not use SQL sequence counters. Its original SQL is qualified through
  completed publication, key reuse and cold-owner recovery. Owned SQL sequences
  exist separately: serial/identity defaults and transactional counter
  ownership/generation/reset handoff must join the same staging publication;
  this document does not claim that unfinished boundary.
- Graph-index cutover: the old owner closes graph reader, mutation, maintenance
  and worker-snapshot admission under its catalog/apply locks, drains schedule
  pins and persists a Raft-bound seal whose digest binds the active source
  fence, old owner identity, verified graph configuration, fresh generation and
  staging-plan digest. Uncertain seal replies resolve by read-indexing the owner
  receipt. `begin_cutover` is irreversible.
- Incoming-FK CASCADE never truncates an outgoing parent implicitly. Untouched
  external parents join the fenced retirement without replacing their
  generations; their inverse references carry the old child's constraint
  generation, which every consumer must interpret. FK retirement uses the
  coordinated publication protocol, not the row-oriented legacy retirement
  worker (which still serves non-FK generations such as UNIQUE). The protocol:
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
- Planning and preflight read a transactional authenticated retirement-set
  summary (a compressed binary Patricia tree updated with activation, handoff
  import and tombstone GC in one transaction) instead of scanning tombstones. A
  missing root is accepted only after one bounded seek proves there are no
  tombstones. This trades higher write/GC WAL bytes for bounded reads.
- Ordinary FK ADD/DROP and FK-bearing initial CREATE (including MATCH PARTIAL
  support indexes and self-referential FKs, hosted and native standalone) use
  the metadata-owned parent/source publication and ACK protocol; the child
  schema, integrity catalog, accepted generation, Raft marker and fence release
  install atomically. The generation command requires an all-voter/learner v19
  decoder proof at admission and final append. `IntegrityTopologyBusy` before
  decision is a retryable 409; ambiguous writes are never replayed.
- CI runs the prebuilt mounted proof binaries (`fk-truncate`, initial and
  ordinary/self-FK recovery lanes) with a 15-minute hard timeout and retained
  logs, outside the unit-test aggregate.

Known gaps: repeated stable completion of mounted external-parent FK DROP
(an intermittent pre-DROP parent DELETE stall is unresolved); a bounded-scale
proof or atomically maintained generation locator for high-churn reference GC;
moving shared index-derivation helpers out of `api/tables.zig`; hidden-owner
standby seed/promotion fault coverage; acceptance tests for crash/lost-ack at
each fence, cancellation on both sides of publication, concurrent parent
mutations and child inserts, stale prepared participants, nullable/MATCH PARTIAL
witnesses and GC/restart bounds.

### Hosted retirement activation

Metadata retains a plan-wide-bounded history of every admitted hidden replica,
including placements removed before cancellation. Publication retains obsolete
replicas for cleanup while excluding current physical group/node/store slots.
Tickets distinguish canceled plans from published-obsolete roots; neither
authorizes unlinking a current published owner. Paging and ACK recheck the
immutable plan and current placement in one metadata transaction. Re-admission
to a retired physical slot waits for its unlink ACK even when replica labels or
root generations change.

Authority is explicit administrator enrollment, not ordinary service
registration: `POST /store-roots/enroll` and read-only
`POST /store-roots/enrollment-status`; the CLI produces an existing root's signed
proof. Metadata persists one immutable verifier per node/store/root identity.
Signed retirement paging and ACKs bind the cluster, store, root, ticket and
request cut. Hosted child provision/release receipts carry a root attestation
checked against enrollment, the plan and current placement. See
[store-root enrollment](../../store-root-enrollment.md).

The worker verifies an exact cold owner bootstrap and persists a checksummed
local intent before quiescing the owner, Raft, readers, writers and snapshots.
It can cancel a private owner offline without quorum, rename only the verified
root to trash, fsync, remove only the matching local replica catalog entry and
retry a signed ACK after restart. Permanent generation fences prevent stale
rejoin. Trash GC is separate from acknowledgement: inode-bound, no-follow
traversal persists its cursor and limits entries, deletes, depth and elapsed
time per slice.

Raft membership admission uses the exact native applied/pending/retained
configuration cut: an unapplied merge acquire fences membership, an unapplied
release cannot unfence it, and a retained-log gap fails closed. Forwarded,
batched and automatic joint-consensus exit proposals share this callback.

**Security caveat.** The UUID and Ed25519 seed identify a root lineage, not
unique hardware. Native shard backups and default standby seed capture exclude
the outer identity, but an operator's full-root copy can clone that identity
and key. Enrollment is not hardware attestation or cloned-disk detection.
Replacement roots require explicit enrollment and placement approval, not
trust-on-first-use.

## Distributed online merge

> **Moved:** the online-merge ordered-catalog admission, transfer, reconciliation and open-work contract is storage, not SQL; it now lives in [ordered artifact publication](../ordered-artifacts.md#distributed-online-merge). It requires data protocol v14 and metadata protocol v20.

## Durable range protection

The implementation includes replicated activation, native bucket counters,
pending-writer reservations, retained-read/RPC proof export, durable session
observations and owner-fenced distributed prepare. Savepoint rollback keeps read
observations while discarding staged writes. Versioned private prepare envelopes
make older receivers reject guarded requests instead of ignoring their proofs.

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

Remote retained reads mount a service-authenticated owner protocol with
catalog-scoped capabilities, bounded owner leases, sequenced pages, cancellation
and exact typed-row metadata. Distributed statement capture admits each selected
leader before freezing any participant, then validates fresh quorum observations
against the frozen applied index and leader term. Quorum validation never waits
for Raft apply while holding an apply freeze; contention, a newer committed index,
or a leader/incarnation change aborts the statement. These are statement snapshot
guarantees, not durable serializable transaction protection. All capture fences
are released before paging; alias cursors share capture but not position.

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

## External lake SQL

Relational table schemas can attach a read-only Parquet prefix or Iceberg table
with `base_source`. The attachment persists with the native schema and is
resolved through the catalog for typed-row queries and SQL over HTTP or pgwire.
Creation (HTTP, MCP and SQL catalog) can infer `document_schemas` and
`schema_fingerprint` (default `auto`) from lake metadata without decoding rows:

```json
{
  "storage_mode": "relational",
  "base_source": {
    "kind": "external",
    "table_id": "events",
    "format": "parquet",
    "uri": "s3://analytics/events",
    "credentials": {"ref": "analytics-lake", "scope": "events"},
    "snapshot": {"mode": "current"},
    "write_policy": "read_only"
  }
}
```

Schema and identity:

- Parquet inference combines all listed files, marks absent/nullable fields
  optional and rejects incompatible types; it needs a schema-bearing file.
  Iceberg uses field IDs, types and requiredness from metadata (a pinned
  snapshot uses its schema; fingerprint e.g. `iceberg-schema:7`); empty tables
  are supported. Discovered columns persist before catalog publication.
- Supported: flat booleans, integers, floats, UTF-8 strings, timestamps (exact
  signed epoch nanoseconds, offsets and pre-1970 dates) and decimals up to
  precision 38 decoded exactly, with SQL string semantics (casts to float are
  explicit). Nested, binary and unsupported logical types fail explicitly.
  Missing optional columns become SQL NULL; incompatible types and missing
  required columns fail scans; new fields do not silently expand the catalog.
- Credential references use `external_io` connections with `lake_read`
  capability and enforce bucket/prefix scope. Snapshot selectors can pin an
  Iceberg snapshot ID or a Parquet object-version digest. Filesystem sources use
  the object-store layout under its `antfly` bucket.
- Each SQL statement pins source inventories and object versions; repeated
  aliases share the inventory. Physical row identities are opaque versioned
  `lake1:` IDs, stable within a snapshot; clients must preserve them verbatim and
  must not parse or synthesize them. Public row queries apply residual
  predicates before limits; Iceberg position/equality deletes apply before page
  limits and resolve field IDs, not physical column names.

Scan and pruning:

- Cursors pull typed column batches, retaining one file's footer/delete metadata
  and one decoded page per projected column; opening a cursor reads no data
  pages. File metadata is ordered by identity so pagination needs no full sort.
- Integer, string and boolean comparisons prune row groups/files from footer
  min/max statistics; Iceberg partition pruning conservatively projects
  identity, bucket, truncate and temporal predicates. Unknown specs, transforms,
  missing statistics, annotated decimal/timestamp types and unsupported
  comparisons stay residual. Exact unfiltered `COUNT(*)` uses footer row counts
  only with no deletes or cursor constraints.
- Nonrecursive equijoins install composite Bloom/range runtime filters on a
  direct probe scan after the build completes (not on preserved outer sides or
  computed keys); range evidence also prunes files, row groups and partitions.
  The exact hash join and ON residual decide every match.
- Large eligible scans split into ordered range workers with bounded typed
  queues; the consumer preserves source order and delivers valid rows before a
  later worker error. LIMIT/OFFSET, small scans/budgets, external decision
  expressions and scheduler saturation use the serial pipeline.

Limits and configuration:

| Limit | Default |
| --- | --- |
| Shared API range cache | 64 MiB, at most 4,096 entries; keys bind credential ref/scope, endpoint, object version and byte range; unversioned reads bypass it |
| Cursor prefetch | At most four concurrent reads and 32 MiB of requested ranges per lookahead batch; prefetch failures are speculative |
| Examined rows per stream | 100,000,000 |
| Parquet decode | Up to four column decoders; 32 MiB input and decoded budgets per active page set |
| Per-statement scan | 10,000,000 scanned rows, 65,536 scan pages, 256 MiB retained bytes |
| Embedded SQL JSON request | 64 MiB (67,108,864 bytes) including text and parameters; oversize returns `54000` with the numeric limit |
| Preparation / execution working memory | 256 MiB each |
| Embedded SQL sessions | 4,096 staged mutations and 256 MiB staging memory; native transaction intents also use the configured admission limit (128 MiB default) |
| Spill | `sql.runtime.Limits.spill_bytes` (default 1 GiB live quota per statement, `0` disables), `spill_root` (default `/tmp`), at most 64 open spill files per statement |
| Native admission scheduler | At most eight asynchronous tasks and 64 MiB reserved staging/workspace across statements; expression pages of ≥1,024 rows split into at most four stripes |
| Native execution chunks | At most 4,096 rows, independent of response-page size |

Cache hits still check cancellation and deadlines; cached bytes are separate
from each statement's memory budget. Under scheduler saturation, required work
runs inline and speculative prefetch yields; task leases release only after
workers join. Statement memory/disk budgets are separate from the global
allowance.

Spill: blocking sorts, grouped aggregates (including DISTINCT inputs), hash-join
builds and window partitions spill through a shared statement owner. Spill files
are private and unlinked while open; records keep exact numeric tags, SQL/JSON
null distinction and order, with length and checksum validation (Snappy when it
saves space). Sorts merge up to eight run heads directly; spilled joins
hash-partition both sides and split oversized partitions on unused hash bits,
with a disk-chain fallback for inseparable duplicates. Windows spill as column
blocks with a four-block decode cache. Pgwire delivers sorted, grouped and
window results in bounded continuation pages beyond the materialized response
row cap; HTTP JSON response limits remain.

Batch kernels: numeric/boolean expressions use bounded instruction-major
kernels with four-lane exact arithmetic; lazy expressions (CASE, AND/OR) and
unsupported types use the scalar evaluator. The native batch interface is
internal, not Apache Arrow. Sort/top-K comparisons cache 32-byte normalized
prefixes and fall back to the exact comparator for ties and complex values.
Window requirements with compatible ORDER BY prefixes share the strongest
permutation only when consumers are peer-invariant.

Rejections: `EXPLAIN` identifies `Lake Scan` (verbose plans show format and
snapshot selector without opening the source). Attachments reject native write
constraints, relational schema indexes, defaults, generated columns and TTL; SQL
mutations and public batches reject writes; existing native tables cannot
acquire or remove an attachment through a schema update. External scans reject
native serializable range-proof requests, active row policies and row filters.
Public secondary-index cursors and explicit collations are unsupported.

Lake index publication, readers, the persistent `lake_cache`, artifact GC
(`lake_indexes.artifact_gc`), full-text `store_source`, cold-query metrics and
the deployment qualification procedure are specified in
[remote table serving](../../../zig/REMOTE_TABLE_SERVING.md#lake-index-publication-operations).
Further native scan/aggregate contracts are in
[bounded publication reuse](../../../zig/REMOTE_TABLE_SERVING.md#bounded-publication-reuse-and-collection-progress),
[immutable scan plans](../../../zig/REMOTE_TABLE_SERVING.md#immutable-scan-plans-and-aggregate-reduction-trees)
and [keyed contribution state](../../../zig/REMOTE_TABLE_SERVING.md#keyed-contribution-state-grouped-partitions-and-cold-load-admission).

Benchmarks and fixtures (run from `zig/`):

```sh
zig build sql-native-refinement-bench lake-native-refinement-bench -Doptimize=ReleaseFast
ANTFLY_BIN="$PWD/zig-out/bin/antfly" uv run --project e2e/antfly --extra lake pytest e2e/antfly/test_lake_sql.py -n 0
```

Recorded samples are in
[`native-sql-refinements.json`](../../../zig/bench/baselines/native-sql-refinements.json),
[`native-lake-execution-refinements.json`](../../../zig/bench/baselines/native-lake-execution-refinements.json)
and [`native-lake-column-pipeline-refinements.json`](../../../zig/bench/baselines/native-lake-column-pipeline-refinements.json);
they measure local CPU kernels and temporary-file I/O, not remote lake latency.
Opt-in SQL workloads use `ANTFLY_SQL_MERGE_CLASSIFY_BENCHMARK=1`,
`ANTFLY_SQL_JOINED_MUTATION_BENCHMARK=1` and
`ANTFLY_SQL_MEMBERSHIP_BENCHMARK=1`.

## Foundations

- The shared bounded scanner and single-statement compiler produce typed
  parameter nodes, never SQL substitution; grammar coverage is in
  `zig/pkg/antfly-embedded/src/sql/COMPILER_SUPPORT.md`.
- A shared, bounded, scope-partitioned immutable plan cache serves HTTP SQL and
  pgwire; it compiles outside its publication mutex, deduplicates concurrent
  misses and only evicts idle leased plans. Schema derivation is cached
  separately (32 MiB maximum, four concurrent builders). Neither cache is an
  authorization or routing cache: identity and authorization are resolved and
  fenced per request.
- HTTP and pgwire share bounded preparation admission followed by an owned read
  or write permit; HTTP overloads return SQLSTATE `53300` with retry guidance.
- The relational reader can choose a READY compound or partial index for a
  leading equality prefix and intersected range suffix, retaining the complete
  predicate as residual; explicit primary-key ordering disables secondary-index
  selection. A sixteen-candidate shortlist uses at most eight snapshot-local
  index records per candidate for costing (bounded probes, not statistics).
  Planning runs after releasing the shared apply lock.
- Native retained readers own the schema/store snapshot, compiled predicates,
  projections and row-level authorization filter and decode typed pages without
  JSON round trips. Leader loss never downgrades retained admission to stale.
  Pgwire rechecks credentials and read authorization on each retained page.
- Generated `/sql` contracts plus Go, TypeScript, Python and Zig convenience
  APIs; mutation state and transaction receipts survive success and error
  responses. The `antfly sql` command never retries or follows redirects for
  SQL mutations. The optional `pgwire` node configuration registers the
  listener; see `zig/pkg/antfly/src/pgwire/README.md`.

### CLI example

Against an existing relational table with a `name` column:

```sh
antfly sql --database default --namespace public \
  --statement 'SELECT _id, name FROM users WHERE _id = $1' \
  --parameters '["user-42"]'
```

Results remain ordinal arrays, so duplicate column names are not lost. The
`--limit` option is an admission ceiling; put `LIMIT` in SQL when intentionally
requesting a prefix. Transaction/session commands still fail explicitly.

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

Focused gates: `zig build sql-test`, `pgwire-test`, `check-openapi`,
`lake-integration-test`, `sql-numeric-test`, `sql-exact-float-test`,
`antfly-schema-expression-test`, `lib-sql-parser-test`; the original-corpus
gates are `make sql-parity-inventory-check`, `make sql-parity-evidence-check`
and `make sql-parity-release-check` (see [the inventory](parity-inventory.md)).
The focused tests do not satisfy the complete SQL extraction release gate.

Two build-memory gates were over their declared claims as of the October 2026
typed-values log, and neither was claimed fixed: broad optimized SQL compilation
reported a 3.44 GB peak against its 3.22 GB declared RSS claim (a cached rerun
passing does not prove cold compilation fits), and the broad durable-runtime
target reported a 14.46 GB compiler peak against its 13.96 GB reservation. The
isolated `sql-numeric-test` target does not fix either
([history](history/typed-values-log-2026-10.md)).

## Sequence execution

`nextval`, `currval`, `lastval` and `setval` use the existing native catalog
allocator and exact nonce-bound acknowledgement journal. Scalar evaluation
suspends on a lazy demand; the execution owner performs I/O and resumes with
one shared resource budget. Successful volatile siblings are not re-executed.
Statements without sequence calls retain their ordinary binding path.

Literal names capture catalog incarnation and immutable object identity.
Connection state separates issued values from compact, process-local cached
leases. HTTP connections persist issued `currval`/`lastval` before result
publication; pgwire owns those values and allocations for its connection
lifetime. `setval(..., false)` clears the caller's cache without changing its
`currval`; `setval(..., true)` updates that sequence's current value without
selecting a new `lastval` owner. Rollback/savepoints preserve issued values,
DISCARD SEQUENCES has a
separate cache epoch, and cold owners burn unconsumed leases. MERGE's
stateful predicate and selected assignment execute together per row; deferred
ordered outputs also use row evaluation. Other decision expressions keep their
batch path.

`antfly-api-sql-sequence-effects-test` qualifies real HTTP connections, pgwire
prepared resources, implicit MERGE transactions, lost/forged acknowledgement
handling, document TRUNCATE publication and independently reopened owners.
The PostgreSQL reference also records serial/identity lifecycles, but those
three original declarations remain unresolved until their native atomic
publication and shared mutation/default contracts are implemented.

## Open work

- Original-corpus adjudication until `make sql-parity-release-check` passes.
- Data-modifying CTE dataflow; broader correlated/mutation subqueries;
  cross-level aggregate ownership lifting; spill-backed tuple membership.
- MERGE cross-owner/failover fault matrix and partial/expression-key probes.
- Stronger-isolation deployment and fault validation; TTL visibility contract;
  guarded Lite sessions.
- Serial/identity declarations, atomic table/owned-sequence defaults and
  transactional counter ownership/reset handoff; TRUNCATE and FK promoted-standby and
  repeated-soak gates (see TRUNCATE known gaps).
- Complete route-wide policy/session validation, revocation/restore and
  promoted-primary security workloads.
- Typed-value gaps listed under [Exact NUMERIC](#exact-numeric) and
  [Arrays](#arrays); regex parity per [its design](regex-parity.md).
- Ordered-artifact activation (see [its checklist](../ordered-artifacts.md#activation-checklist))
  and the online-merge gaps listed under
  [distributed online merge](../ordered-artifacts.md#distributed-online-merge).
- Remote lake end-to-end performance/recovery evidence; workload benchmarks and
  observability.

## History

- [Capability reconciliation and exact-source adjudication](history/reconciliation-and-adjudication-2026-10.md)
- [External lake SQL and lake publication log](history/lake-integration-log.md)
- [Pre-merge activation work](history/pre-merge-activation-2026-09.md)
- [Integration status, sessions, MERGE, TRUNCATE and online merge log](history/integration-status-log.md)
- [Validation checkpoints and initial slice](history/validation-checkpoints.md)
- [Typed rows, arrays, precise types and exact NUMERIC log](history/typed-values-log-2026-10.md)
- [Regex backend and scalar activation log](history/regex-implementation-log.md)
- [Ordered artifact activation gate log](../ordered-artifacts/history/activation-gate-log.md)
