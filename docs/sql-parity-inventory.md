# Original SQL extraction parity inventory

The pinned original source contains 1586 cases: 267 explicitly invalid or
unsupported cases and 1319 cases requiring review of their original
planner or runtime contract. These are **not** 1586 promises of working runtime
execution. Original plan fingerprints, endpoint behavior, authorization, diagnostics,
and deliberate rejections must be distinguished.

## Provenance and scope

PostgreSQL is the SQL compatibility standard for this work. Its behavior governs
syntax, type coercion, result types, SQL NULL semantics, ordering, JSON/array
operations and mutations. Historical source plans and SQLite results do not
override that contract. SQLite-backed fixtures below are limited regression
checks, not evidence of PostgreSQL compatibility; new campaign completion must
be validated against PostgreSQL and the native engine. An unavailable PostgreSQL
oracle is a validation gap, not permission to substitute SQLite silently.

The next read/document campaigns now use a real PostgreSQL 18+ oracle
(PostgreSQL 19 is the target). `generate_sql_postgres_reference.py` starts a
disposable private Unix-socket server, sends the original `$n` SQL unchanged,
and records complete values, labels, PostgreSQL type OIDs and SQL NULL flags.
The oracle uses UTF-8, C locale and UTC. Server-side raw cursors cap fetched
read rows without modifying the source SQL or buffering its entire result.
Reads are transactionally read-only; each mutation has an isolated rolled-back
fixture. Time, lock, temporary-file and output limits bound oracle execution.
Assignment-column triggers distinguish an explicit NULL write from a missing
document property. Source schemas remain intact; an explicit current document
schema models the historical object-valued `metadata: json` shorthand without
granting any index-readiness or cardinality authority.

Seven non-unique ordering contracts additionally use independent, bounded
PostgreSQL observers for the complete eligible peer frontier. Validation checks
a complete ordered prefix, allowing arbitrary selection only within genuine
peers at the LIMIT boundary. Skipping better rows, duplicating rows or selecting
worse rows fails; PostgreSQL's arbitrary tie order is not a compatibility rule.

Install PostgreSQL 18 or newer, put its binaries on PATH or set `ANTFLY_PG_BIN`,
then verify selected PostgreSQL goldens with:

```sh
uv run --no-project --with 'psycopg[binary]==3.3.6' python scripts/generate_sql_postgres_reference.py read --check zig/pkg/antfly-embedded/src/sql/fixtures/sql_read_campaign_reference.json
uv run --no-project --with 'psycopg[binary]==3.3.6' python scripts/generate_sql_postgres_reference.py document --check zig/pkg/antfly-embedded/src/sql/fixtures/sql_document_reference.json
uv run --no-project --with 'psycopg[binary]==3.3.6' python -m unittest discover -s scripts -p test_generate_sql_postgres_reference.py
```

The fixed 251-case read and 211-case document campaign manifests still describe
the complete cohorts, not a claim that every case works. Oracle admission and
discovery-mode native runs do not change dispositions; selected goldens must
pass the non-discovery native endpoint gate before receiving completion credit.

Catalog discovery separately exercises all 479 original `ddl`/`unsupported_ddl`
cases, including the already-dispositioned negative contracts. The current
compiler admits 74, principally catalog operations, transactions and policy
commands; that is not proof of authorization, durable publication, populated
schema rewrites or constraint activation. Enable per-case diagnostics with
`ANTFLY_SQL_CATALOG_DISCOVERY=1` and run `zig build sql-test
-Dtest-filter='SQL catalog campaign discovery'` from `zig/`.

The catalog boundary now tests 6,585 original-source request truncations, with
bounded diagnostic positions and no missing-token dereferences. Targeted
allocation-fault tests cover incomplete CREATE definitions. CREATE defaults,
ALTER ADD defaults and ALTER SET defaults share admission checks before any
catalog operation: request parameters cannot become durable schema defaults,
and subqueries are prohibited. PostgreSQL independently rejects the exact 32
original DEFAULT-subquery cases (`sql-1109`–`sql-1140`) with SQLSTATE `0A000`;
these historical forms are not PostgreSQL features waiting to be activated.
Their dispositions remain unchanged: discovery and rejection evidence do not
inflate implemented-case counts. Mounted HTTP regression tests verify the
malformed-request and unsupported-default diagnostics with zero catalog calls,
so neither a truncated request nor a request-bound default reaches publication.

Typed arrays have a distinct immutable value layer (`array_value.zig`), not a
JSON-list approximation. It preserves element widths, up to six dimensions,
non-default lower bounds and per-element SQL NULL provenance. Owned values and
prepared membership indexes account for actual allocated capacity and clean up
under allocation faults; indexed containment probes allocate no memory. Index
growth reclaims replaced buffers rather than retaining them in an arena. Array
ordering and hashing agree on bounds, NULLs, signed zero and floating-point NaN.
Strict scalar ANY/ALL comparisons retain three-valued logic, including empty
arrays and multidimensional row-major traversal. The shared
`sql_array_reference.json` fixture checks 18 exact PostgreSQL expressions against
both PostgreSQL and the native value operators. This is component evidence,
**not public SQL activation**. The complete typed cell now flows through shared
physical ordering/hashing, top-K and hash-join ownership, retained-column
equality, recursive distinct keys, window peer comparisons and row/column
spill codecs. Arrays never use JSON-null sort prefixes or primitive vector
kernels; exact comparison or fallback retains their identity. Spill decoders
reject invalid types, noncanonical empty dimensions, nested SQL-array tags and
every truncated array-cell prefix. Encoded record limits, decoded array bounds
and statement resident-memory quotas are independent. Allocation-fault tests
cover retained columns and codecs. Full array parsing/binding, durable
typed-column metadata, public result types and pgwire codecs still need
end-to-end integration. Numeric/temporal element types and non-C collations also remain
outside this layer's current contract. No original
typed-array cases are marked complete on this evidence alone.

The shared PostgreSQL binary-array codec (`array_binary.zig`) verifies element
OIDs against the pinned expected type and retains dimensions, lower bounds,
primitive widths and SQL NULL versus JSONB-null elements. It streams encoding
without per-element scratch arenas, bounds wire size before output, and charges
actual decoded arena capacity independently. Eleven server-produced binary
fixtures cover every currently supported element codec; the PostgreSQL oracle
also accepts the native payloads as binary parameters and checks array equality
and JSONB NULL provenance. Fault injection covers decoding every fixture and
every truncated text-array prefix. Boundary probes preserve PostgreSQL's
advisory NULL flags, nonzero boolean receive bytes and empty-extent
normalization; the core now rejects an exclusive upper bound that cannot fit
int32, with SQLSTATE `54000`. The codec alone does not activate catalog types,
public result descriptors or pgwire array parameters/results.

SQL binding now distinguishes array types from both JSON and unknown NULL,
including element identity. Scalar and multidimensional `ARRAY[...]` constructors feed
strict comparisons, `ANY`/`ALL`/`SOME`, cardinality and dimension/bound queries.
One hundred sixty-four shared PostgreSQL expression contracts run through binding,
native statement execution and the HTTP API, checking exact values and SQL
NULL flags. Constant constructors are prepared once into the immutable
program; the 10,000-row debug benchmark uses zero constructor scratch bytes
versus 688 bytes per row for the parameter-dependent equivalent (about 7 ms
versus 15 ms in one local run, not a production latency claim). Allocation
faults cover both preparation and parameter-dependent evaluation. Wider
integer probes against narrow array cells compare without narrowing overflow.
Explicit builtin array casts retain element widths, dimensions, lower bounds
and SQL NULL provenance, including typed empty constructors. Constant cast
chains are prepared once and require no evaluation allocator. Scalar and
vector integer arithmetic check their inferred widths; real arithmetic uses
the scalar path to preserve float4 rounding. Floating-point casts round ties
to even; JSONB numeric casts round exact decimal tokens away from zero without
an intermediate double. Forty-five shared PostgreSQL SQLSTATE contracts cover
invalid syntax, range overflow, unsupported cast pairs and array operators.
Comparison operators require matching array element identities, while
CASE/COALESCE use common-type promotion.

LIKE/ILIKE ANY/ALL/SOME over typed text arrays use the existing bounded pattern
matcher without materializing a JSON copy or allocating per pattern. Negation
applies to each comparison, not to the combined quantifier result. SQL NULL
arrays differ from empty arrays; NULL elements preserve three-valued logic.
Twenty-two PostgreSQL contracts cover these boundaries, C-locale UTF-8 matching,
escaping and short-circuiting past an invalid later pattern. A 10,000-row debug
probe runs with a zero-byte evaluation allocator (about 7 ms locally, not a
production latency claim). Allocation-fault tests also cover dynamic arrays.

PostgreSQL text-array casts share a bounded two-pass decoder with owned values,
rectangular multidimensional shapes, explicit lower bounds, escaped text and
distinct SQL NULL cells. Nineteen PostgreSQL binary-oracle examples and nineteen
SQLSTATE examples cover all nine builtin element types. Allocation-fault tests
cover decoding and dynamic casts. Constant text casts are prepared once: a
10,000-row debug probe used zero scratch bytes versus 368 bytes per dynamic cast
(about 6 ms versus 26 ms locally, not a production latency claim). Both text and
binary JSONB array inputs share pre-DOM nesting/work admission with scalar JSON
casts. These supplemental contracts grant no original disposition credit.

Nested constructors (including bracket shorthand) evaluate children once and
admit matching dimensions/lower bounds before flattening into one exact typed
cell allocation. Rank is capped at six; all-NULL/empty subarrays canonicalize
to zero dimensions while mixed empty/nonempty shapes fail with `2202E`.
Unknown string literals coerce to the selected scalar/array element identity;
explicitly typed text does not. Explicit outer casts resolve/coerce elements before
independent NULL/text defaults are selected. Invalid mixtures of bracket-list
and scalar grammar are rejected rather than silently widening PostgreSQL syntax.
Small constructors keep child references on the stack; large ones admit scratch
memory against the execution quota.
Allocation-fault and byte/work-limit tests cover dynamic scalar and typed-array
column inputs. A 10,000-row debug probe used zero prepared scratch versus 792
dynamic bytes (about 7 ms versus 21 ms locally, not a production latency claim).

Native scalar programs now bind precise immutable parameter descriptors, with
primitive widths and array element identities. `parameter_frame.zig` owns one
quota-admitted arena for all execution inputs: text/binary codecs decode once,
typed logical inputs clone once, and JSON arrays never masquerade as SQL arrays.
Programs bind a shared frame once after exact descriptor checks; the row loop
borrows prepared cells with no decoding, cloning or descriptor scans. Twenty-four
PostgreSQL PREPARE contracts verify parameter OIDs and values; nine isolated
SQLSTATE contracts cover invalid input, shape and arithmetic overflow. Binary
fixtures and allocation-fault tests cover all nine builtin scalar/array codecs.
A 10,000-row native probe decodes once, uses zero evaluation scratch bytes and
retains a 976-byte frame (about 5 ms locally, not a production latency claim).
Program, codec-owner and frame arenas have stable addresses; managed JSON array
allocator references are rehomed before temporary quota wrappers expire.

Statement-wide binding, public SDK/envelope descriptors and pgwire still need
to adopt the precise frame contract; JSON compatibility ingress remains guarded
against array parameters. Native datetime text inputs normalize to UTC, while
datetime binary parameters remain guarded until their codec is bound.
Array-valued public outputs, public array parameters, catalog storage and overloads converting
whole arrays to text/JSON remain explicit activation gaps. Default decimal
constructors still need an exact NUMERIC array representation; direct narrowing
or text casts of these constructors remain guarded (explicit real/double casts
provide floating-point semantics). Native table integer columns retain their
existing int64 contract; integer literals infer int4/int8 and explicit casts
carry their widths. No original disposition credit is granted by these
supplemental contracts.

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
The current branch records 352 implemented, 136 rejected and 70 superseded
cases, with 1,028 still unresolved. The earlier batches add 77 exact compiler
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
getting below 800 now requires at least 229 additional resolved dispositions.

Eight further original cases (`sql-0220`–`sql-0222`, `sql-0284`, `sql-0302`,
`sql-1226`, `sql-1227` and `sql-1340`) now execute typed array predicates through
reads, aggregate FILTER and a left join. The PostgreSQL read campaign contains
68 exact contracts, with peer-frontier checks for the new non-unique aggregate
and timestamp ordering. The separate LATERAL campaign now activates `sql-1363`
through correlated derived-table binding and a parameterized apply operator with
per-parent ORDER/LIMIT and left-null-extension semantics; scalar pattern support
alone would not activate that relation shape.

Six additional read contracts exercise PostgreSQL text slicing and replacement
through the native endpoint. The text oracle independently verifies 54 UTF-8,
NULL and error contracts, including negative split positions, duplicate
translation characters, SQL-standard substring/position/overlay syntax and
PostgreSQL SQLSTATEs. Borrowed slices avoid output allocations; immutable
constant translation alphabets are prepared once in execution-owned caches,
outside serialized instructions. Allocation-fault tests verify cache ownership
after the parsed AST is released. A local debug 50,000-row translation benchmark
measured about 113 ms prepared versus 149 ms dynamic, with reusable scratch
capacity of 46 versus 336 bytes; these are local microbenchmark observations,
not production latency claims. Typed-array/element-width and temporal profiles
remain separate unresolved work, not JSON approximations or synthetic credit.

Native scalar-statement binding now retains precise parameter descriptors,
including scalar widths and array element identities. Predicates, projections,
ordering, INSERT values and UPDATE assignments share one bounded, execution-owned
parameter frame. Programs check descriptor compatibility once before the row
loop; lazy decision functions retain the existing provider-validation and demand
machinery. PostgreSQL oracle checks cover mutation parameter OIDs and bare-target
ambiguity: declared array types permit either projection order, while an
untyped bare target followed by an incompatible cast retains SQLSTATE 42P08.
Allocation-fault tests cover binding and preparation ownership. These native
contracts do not activate typed array parameters through the public envelope,
session/protocol metadata, aggregate/window/relation binding, or storage; those
boundaries still require migration. No original inventory dispositions change
on the strength of these supplemental component tests.

Statement-invariant relation caches now use bounded replay storage rather than
retaining an allocation per source row without a local spill boundary. Small
inputs stay in memory with actual arena-capacity admission; larger inputs spill
once into typed sequential blocks without a per-row disk directory. Each replay
reader owns its position and decode arena, so interleaved materialized-CTE
references cannot invalidate one another's borrowed rows. Readers lease their
sealed run, prohibiting append until they close; cleanup shares the existing
statement spill quota and cancellation machinery. A 4,096-row materialized
self-join verifies one source capture/read and complete output, independently
checked against PostgreSQL. Parameterized LATERAL apply now binds lexical parent
scopes and executes each inner query with its own LIMIT/OFFSET, null extension,
materialized-CTE cache and recursive worklist lifetime. Statement-invariant
inputs share bounded replay readers and hash builds rather than reopening the
captured source for each parent. Supplemental tests check local-column shadowing,
qualified parent wildcards, nested correlation, CTE visibility, parameter
inference, allocation failures and cancellation cleanup. A captured 256-row child
relation supplies 128 parents with per-parent ordered LIMIT/OFFSET under a linear
checkpoint budget; its output is independently verified against PostgreSQL.
The strict public API campaign additionally executes 23 unchanged originals
(`sql-0549`, `sql-1217`, `sql-1218` and `sql-1345`–`sql-1365`, except `sql-1357`)
against captured native tables. PostgreSQL checks complete results, types,
labels, SQL NULL provenance and the full eligible ordering frontier where LIMIT
can select peers. The fixture includes matched and unmatched parents, nullable
filters, per-parent OFFSET and two separately captured native tables. It is
not evidence of distributed snapshot coordination: both fixture databases stay
immutable for each run. Original `sql-1357` is superseded, not implemented:
PostgreSQL rejects its output-alias arithmetic in ORDER BY with SQLSTATE 42703,
and the mounted native endpoint rejects the exact query before opening a read.

Scalar JSONB existence and containment now operate directly on logical values,
with bounded recursive work and no serialization. Typed-array containment uses
the shared membership index; constant operands prepare immutable indexes once,
while dynamic construction and probes share the statement work budget.
`string_to_array` uses bounded linear-time delimiter matching, preserves empty
fields and SQL NULL elements, and owns its typed result. Thirty additional
PostgreSQL scalar contracts check these operations. Explicit LIKE/ILIKE ESCAPE
supports empty and single-codepoint escape strings without allocating a rewritten
pattern. A local debug containment probe evaluated 10,000 rows in approximately
10 ms with zero row allocations; this is a microbenchmark, not a production
latency claim or independent evidence of public array-column activation.

Window ordering now resolves output labels only when the label is the complete
sort key. Arithmetic, casts, scalar calls and CASE expressions bind to input
columns, even when an output label has the same spelling. Normalization retains
those expression trees instead of recursively cloning and substituting aliases.
PostgreSQL and native regressions verify the differing order of a standalone
shadowing alias versus an input expression, quoted and implicit labels, derived
query boundaries and undefined-column rejection. Existing allocation-fault and
window-slot reuse tests use valid standalone sort labels. These supplemental
checks do not grant original window-campaign completion credit. The exact
originals `sql-1219` and `sql-1373` were incorrectly credited from SQLite-positive
alias arithmetic. They now have PostgreSQL and strict public API SQLSTATE 42703
rejection evidence and are superseded, not implemented. Their old success
goldens are removed; the negative contracts remain mounted and executable.

The mutation fixture verifies complete RETURNING rows and labels, SQL NULL
provenance, affected rows, persisted state and untouched rows. A failed RETURNING
projection must leave physical primary bytes, version and content digest
unchanged. `sql-mutation-projections` also covers wildcard scope, output budgets,
allocation faults and MERGE source/target domains. Three-part qualified columns
in `sql-1519` now execute through the native gate. Point and scalar binding share
validation against the pinned database/namespace/table scope; aliases hide the
original name, and qualified synonyms do not add per-row payload cells.

The next mutation campaign is a fixed, source-owned 235-case cohort, not a
completion claim: 91 INSERT/source cases, 76 nonjoined UPDATE/DELETE/source
cases and 68 joined mutations. Compiler discovery currently admits 104/235;
it never updates dispositions. Set `ANTFLY_SQL_MUTATION_DISCOVERY=1` to report
individual compiler gaps. Its profiles separate point, conflict/index-owner,
source, temporal and joined execution contracts.

The PostgreSQL mutation oracle now covers the full fixed cohort for discovery.
Its explicit profile declares logical primary keys and three separate tables;
every successful case records the server's actual command tag and affected-row
count, complete RETURNING labels/types/SQL NULL flags, and every table's complete
post-state. Single-row libpq streaming bounds retained result rows before a
large RETURNING result can be buffered. The oracle pins Psycopg 3.3.6 and uses a
tested oracle-only cursor adapter to retain the terminal command result which
that version's streaming interface discards. A shared 16 MiB wire-payload budget
bounds retained result volume across RETURNING and every post-state table;
single incoming row buffers and decoded Python object overhead are additional,
not an exact process-RSS guarantee. Quota failures close, cancel and
drain the stream before savepoint rollback; later cases must still see the
original baseline. Schema setup is validated outside per-case discovery, and
generated/default producers require explicit reset machinery rather than being
mistakenly isolated by savepoint rollback.

Forty-eight positive PostgreSQL mutation goldens are reproducible on this
profile. The remaining 187 outcomes are **not** 187 unsupported-feature
classifications: they include absent arbiters, missing typed profiles, ambiguous
historical SQL, nondeterministic producers and non-exercising inputs. Missing
partial/expression indexes and CHECK/FK owners still require their own profiles.
Neither a PostgreSQL golden nor discovery alone changes an original disposition;
the complete native endpoint, storage and owner contracts remain required.

JSONB concatenation now uses the shared typed scalar pipeline: object merges
are shallow with right-hand key precedence, while other operands become a
single concatenated array. SQL NULL remains distinct from JSON null. Only the
outer container is allocated; nested immutable values and keys retain their
evaluation lifetime without serialization or deep copying. Allocation admission
covers actual container capacity, including the object-map index, and work
admission covers copied slots and hashed key bytes. Twenty independent
PostgreSQL contracts and allocation-fault tests cover this overload.

JSONB path replacement now consumes the same typed text-array representation
as scalar array expressions. It resolves the path before allocating and copies
only the changed container spine; unchanged subtrees and keys remain immutable
borrowed values. Missing intermediate parents are not synthesized. The shared
scalar implementation handles default creation, negative/out-of-range array
indexes, SQL NULL propagation and visited-null path errors. Allocation admission
covers actual copied container capacity, and work admission covers path bytes,
copied slots and key hashing. The independent PostgreSQL fixture contains 32
value/provenance contracts and 10 SQLSTATE contracts; native fault injection
checks cleanup without recursively freeing borrowed children. This is not yet
evidence for activating public typed-array parameters or logical conflict owners.
Logical JSON parameters and JSON identity casts also preserve string payloads
without parsing them again. Text-format codecs own parsing at ingress, while
explicit SQL text-to-JSON casts still parse normally. Regression checks include
strings containing JSON-looking text, so `"null"` cannot silently become JSON
null and an ordinary value such as `"pro"` cannot fail JSON syntax validation.
The isolated copy regression uses a 4,096-element untouched subtree and 500
updates: local debug scratch usage is 612 bytes for changed-spine copying versus
344,816 bytes for the full-copy baseline. This measures the copy operation only;
result-boundary validation, encoding and storage still process the output and
are not included in a production throughput or latency claim.

The PostgreSQL native mutation runner resets and reads back every fixture table,
compares complete RETURNING labels/types/SQL NULL provenance and affected rows,
and verifies complete stored values independently of SQL projections. The
endpoint campaign executes 20 original cases, including recursive selectors,
UPDATE FROM, DELETE USING, JSONB concatenation and source-aware RETURNING, over
three independently routed native tables. It does not activate the PostgreSQL
profile's logical primary-key owner in native storage. Key-changing and conflict
cases still need matching constraint-owner fixtures; no original disposition
credit is granted by this integration alone.

Source-aware RETURNING binds prepared mutation images as an internal relation.
Candidate and RETURNING source scans share one captured statement read; target
expressions see prepared defaults/generated values, while subqueries see the
pre-publication source snapshot. Projection, scalar cardinality checks and
allocation admission finish before publication. Empty candidate sets do not
evaluate RETURNING expressions, and the shared reader is released before commit.
Simple scalar RETURNING retains its existing fast path. PostgreSQL contracts
cover prepared-image correlation, self-reads, generated values, INSERT sources
and atomic failure. Native allocation-fault tests check ownership cleanup. A
1,024-target/1,024-source regression checks one capture and exactly 2,048 input
rows read, rather than a source rescan per target. The fixed multi-table test
router validates routing and lifetime, not distributed concurrent snapshot
coordination; constraint-owner activation remains separate unfinished work.

Conditional scalar reads now use compiler-generated masked Apply producers.
CASE, COALESCE and boolean short-circuit operators retain SQL NULL truth rules,
and a producer is not opened until its branch is demanded. Prerequisite values
are materialized once; binding and authorization still cover every branch.
The shared PostgreSQL/native fixture checks 32 result contracts and nine error
contracts, including demanded cardinality failures and invalid names in dead
branches. Mutation tests additionally verify that an unused RETURNING producer
reads no source rows, a demanded failure publishes no mutations, and unused
branches do not bypass source authorization. WHERE is lowered before downstream
producers, with its result retained once for their selected-row demand. Aggregate
FILTER similarly gates its argument producers, without bypassing validation of
unused branches. Predicates without downstream subquery consumers retain their
existing execution path rather than acquiring an unnecessary Apply.

A mixed-demand correlation regression checks 128, 512 and 1,024 target rows:
captured input rows are exactly twice the target count, with 8,023, 31,958 and
63,863 execution checkpoints respectively. It checks cross-size linear growth,
not just physical cursor reads, and allocation-fault injection covers both
demanded and bypassed producers. These are native fixture work counters, not a
production latency claim. Post-group HAVING/output and LIMIT demand, and
distributed concurrent snapshot correctness still need separate work. No
original inventory disposition is changed by these shared-operator fixtures.

```sh
uv run --no-project --with 'psycopg[binary]==3.3.6' python scripts/generate_sql_postgres_reference.py mutation --check zig/pkg/antfly-embedded/src/sql/fixtures/sql_mutation_postgres_reference.json
```

`sql-0571`, `sql-0572`, `sql-0606`, `sql-0607`, `sql-1488` and `sql-1493`
execute exact SQL and parameters against a fresh native table for each case.
The bounded independent reference checks complete RETURNING and final storage
state, not just row counts. Regenerate only selected goldens with:

```sh
python3 scripts/generate_sql_mutation_reference.py --check zig/pkg/antfly-embedded/src/sql/fixtures/sql_mutation_reference.json
```

The generator does not rewrite SQL or treat an empty mutation as evidence.
Locking, temporal, multi-table and conflict-arbiter profiles need native owner
fixtures; SQLite compatibility is not a substitute. Those profiles remain
unfinished, including ordered/locked mutation admission, temporal portions,
typed arrays/regex, constraint/index-owner and distributed fault contracts.

Shared JSON construction/extraction preserves SQL NULL separately from JSON
null. Transport types fill unconstrained execution-time polymorphic parameters
only after SQL constraints converge; value-less Prepare/Describe still requires
a determined type. Materialized and cursor execution share this metadata.
JSON numeric output stays numeric across projection, sorting, windows,
aggregation and RETURNING; SQL bigint retains its lossless wire encoding.
Sparse in-memory sorts grow heap slots with admitted rows under the byte budget,
rather than reserving a full scan ceiling for a tiny nested result. Tests cover
growth, quota exhaustion, allocation faults and exact output.

The shared read reference preserves exact source SQL and logical parameters,
checks complete results and SQL NULL provenance, and runs on a native relational
fixture. Its independent SQLite generator is read-only and work/result bounded;
SQLite-specific behavior is not a waiver for a missing native contract. The 113
SQLite-backed cases are separate from the two explicit native contracts
(default NULL ordering and implicit window labels). Regenerate
or check only explicitly selected golden IDs with:

```sh
python3 scripts/generate_sql_parity_read_reference.py --cases zig/pkg/antfly-embedded/src/sql/fixtures/sql_read_reference.json --check
```

Discovery output cannot update dispositions. Unknown/duplicate manifest IDs,
unavailable reference shapes and changed results fail the reproducibility gate.
Source parameters use the original internal tagged representation; the harness
converts those tags to today's public JSON values without changing types or
rounding integers. This is not a legacy-wire compatibility layer.

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
`sql-0002` is a tested supersession of the catalog/admin PREPARE model: its
exact typed-text INSERT plan runs through an authenticated pgwire session and
the native relational writer. The INSERT command count and a subsequent typed
read verify one committed row; write admission still resolves the current
catalog identity and row-policy state.
`sql-0003` is also a tested supersession: the exact PREPARE runs through an
authenticated mounted pgwire adapter; EXECUTE admits one durable TRUNCATE
generation job and returns its pending receipt, not a false synchronous
completion. The staged restore gate separately covers owner publication.
`sql-0004` is a tested supersession of the old catalog/admin PREPARE model. Its
exact UUID CREATE TABLE runs through authenticated mounted pgwire: PREPARE does
not mutate the catalog, while EXECUTE commits one validated typed schema,
logical binding, physical table, and initial range through production catalog
admission. Metadata reopen and Raft snapshot installation preserve that state.
The shared provisioner then creates the data-group owner from the restored
topology; a UUID row survives an owner restart and reads back canonically.
The fixture applies the admitted metadata transition in process, so this case
does not stand in for multi-node consensus and placement fault coverage.
For `sql-0005`, the exact CTE-backed INSERT body has typed component coverage:
source rows are captured and the cursor is closed before one target mutation,
while a source failure performs no write. A pgwire fixture covers the exact
PREPARE/EXECUTE sequence and defers execution. An authenticated mounted HTTP
prepared execution binds the source and target, captures the read-committed
source snapshot, and admits one target native batch. A linked hosted test now
runs the exact pgwire PREPARE/EXECUTE sequence against real Raft-backed source
and target tables, checks `INSERT 0 1` and the typed target read-back, and
retries only a proven precommit read-unavailable error. Multi-owner and
distributed fault evidence remain missing, so the case remains unresolved.
For `sql-0006` and `sql-0007`, the exact prepared CTE UPDATE and DELETE now
execute through authenticated mounted pgwire and a native relational owner.
Command counts and typed reads prove the update and subsequent deletion;
component fixtures also cover source-failure-before-write and versioned
mutation after the captured self-read closes. These supersede the original
catalog/admin PREPARE interpretation, not distributed failover coverage.
`sql-0008` has exact protocol and typed MERGE component evidence. Authenticated
mounted HTTP preparation defers work, then execution carries a captured self-
read range proof into one guarded native commit; source conflicts, unknown
outcomes, and failed proof acquisition have distinct no-replay behavior. This
fixture mocks proof issuance and commit, so real owner-validated range-proof
commit was still missing there. A linked hosted test now prepares and executes
the exact corpus MERGE body through authenticated HTTP against a real Raft-backed
owner, verifies guarded prepare/commit, and reads back the row. The same linked
hosted test now runs the exact pgwire PREPARE/EXECUTE sequence against that owner,
asserts the `MERGE 1` completion and typed read-back, and retries only a proven
precommit read-unavailable error. Distributed fault evidence remains missing;
the case stays unresolved.
For `sql-0009` and `sql-0010`, the exact prepared recursive CTE read and UPDATE
now run through authenticated hosted pgwire against a Raft-backed relational
owner. A parent and child exercise a nontrivial `UNION ALL` delta: the read
returns the child twice (`SELECT 3` total), while the mutation deduplicates
targets (`UPDATE 2`) and a typed read verifies its result. Multi-owner
coordination, failover and distributed cancellation remain unproven, so both
cases stay unresolved.
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

- TRUNCATE: `sql-0160` through `sql-0165` and `sql-1101` are mapped to
  the durable empty-generation barrier. Exact original SQL forms are compiled
  and admitted by the API fixture; the real staged-owner driver tests empty
  publication and recovery. External-parent FK retirement and graph cutover
  now also pass strict-public mounted baseline/cold-recovery tests in the
  installed `fk-truncate` CI binary, without admission overrides. These broader
  activation proofs do not change the original case dispositions or counts;
  SQL-owned sequence counters remain outside the current catalog model.
  Native-only TRUNCATE owners now use durable native generation-handoff
  receipts; the linked standalone activation suite covers external-parent FK
  and graph publication after restart. This does not establish the complete
  promoted-standby or asynchronous-artifact online-transfer fault matrix.
- Joined/source UPDATE and DELETE, MERGE, lateral and recursive source cases
  require explicit current-engine mapping beyond ordinary DML component tests.
- DDL's 384 entries include session commands, prepared statements, cursors,
  maintenance, locks, and constraints. Review actual original admission/runtime
  behavior before treating all of these as implemented or all as future scope.
- Existing scalar/aggregate/join/window, document DML, DDL, session and protocol
  tests should be mapped to exact IDs. Current passing component suites do not
  automatically resolve source corpus entries.
