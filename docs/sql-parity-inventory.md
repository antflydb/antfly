# Original SQL extraction parity inventory

Current audited dispositions: **475 implemented / 136 rejected / 73 superseded /
902 unresolved**. Nine further cross-table INSERT/CTE, multi-row DO NOTHING and
qualified RETURNING contracts now have unchanged-source mounted native and
PostgreSQL evidence. Every case verifies complete table postimages and genuine
logical primary-key enforcement, rather than just affected counts or parsing.
Four original partial/expression-index upserts now have exact
PostgreSQL and native owner-activation evidence, including complete three-table
postimages, independent admission probes and bounded point-owner resolution.
Eight original non-primary-key upserts now have exact native
and PostgreSQL evidence under activated UNIQUE(email), including independent
duplicate-email rejection, complete three-table postimages and point-oriented
ownership resolution without unbounded source reads or statement capture.
Six original JSON/regex/string reads now have exact
PostgreSQL and mounted native endpoint evidence, backed by shared bounded
scalar kernels. Eleven original aggregate cases now have mounted native
public-HTTP and independent PostgreSQL evidence for escaped patterns, nullable
filters, text lengths and regex aggregate inputs. A twelfth already-implemented
MIN/MAX case gained the same PostgreSQL gate without duplicate credit. Grouped
count observers preserve genuine ties rather than requiring one arbitrary order.
Five original regex conflict mutations (sql-1443, sql-1471,
sql-1473, sql-1474 and sql-1475) have mounted public-HTTP evidence with native
primary-key activation and complete PostgreSQL-backed result/postimage checks.
Regex component tests alone do not resolve other corpus entries. Historical
UPDATE ... FOR UPDATE forms remain unresolved PostgreSQL syntax incompatibilities;
their source SQL has not been rewritten to inflate coverage.

The pinned original source contains 1586 cases: 267 explicitly invalid or
unsupported cases and 1319 cases requiring review of their original
planner or runtime contract. These are **not** 1586 promises of working runtime
execution. Original plan fingerprints, endpoint behavior, authorization, diagnostics,
and deliberate rejections must be distinguished.

## Provenance and scope

### Parsed JSON cardinality, C-collation casing and regex operators

`jsonb_array_length` reads the immutable parsed array's cardinality directly,
without scanning or copying elements. It preserves SQL NULL separately from
JSON null and rejects non-array JSON with PostgreSQL's SQLSTATE. Strict function
resolution rejects text-typed and SQL-array arguments rather than silently
converting them. A zero-capacity allocator verifies 10,000 reads of a
16,384-element parsed array under a 16-unit invocation budget.

`initcap` follows PostgreSQL C-collation word and case semantics, including
digits and UTF-8 word boundaries. It allocates exactly one input-sized output,
charges shared work in bounded chunks, and frees unpublished output on
cancellation. Tests cover exact-size allocation, real OOM versus output
admission, cancellation and allocation-fault sweeps. This is not support for
arbitrary locale-dependent collations.

Ordinary `~`, `~*`, `!~` and `!~*` operators lower to the existing prepared ARE
session. They share pattern caching, work admission and cancellation instead
of introducing another regex engine. Independent PostgreSQL contracts verify
precedence, flags, negation, NULLs, errors and lazy Boolean evaluation; native
projection tests preserve PostgreSQL's unnamed operator label. Boolean VM
results retain their explicit builtin type identity. Regex-array quantifiers
and unrelated temporal/durable-expression gaps remain uncertified.

The oracle verifies 78 text/JSON scalar contracts and 61 regex contracts. The
mounted read golden now contains 91 exact original contracts, including
sql-0195, sql-0214, sql-0216, sql-0218, sql-0244 and sql-0256. The JSON-cardinality
LIMIT case compares the full peer frontier rather than pinning one arbitrary
five-row tie selection. Only these six originals gained disposition credit;
their source SQL, parameters and profile rows are unchanged. Clock functions
such as `now()` require a dedicated clock contract and cannot become sampled
values in deterministic goldens.

### Durable ARRAY constructor architecture

The shared binder records unknown-literal constructor coercions as mandatory,
bounded input-function preparation. Durable lowering consumes those exact
prepared values, rather than introducing permissive runtime text casts or
reparsing literals for each row. Typed NULL acquires its destination domain.
The native `array` opcode evaluates children once and admits zero to 32
arguments under the enclosing expression's work, cancellation and byte limits.
Scalar constructors encode canonical typed cells directly. Nested constructors
stack pinned canonical frames with one output allocation, without a decoded
cell vector or JSON round trip. Shared shape validation preserves PostgreSQL
dimension/lower-bound agreement, empty/NULL subarray behavior and rank limits.

`generate_sql_array_constructor_reference.py` independently verifies 27
PostgreSQL values/types/SQLSTATEs. Native tests compare exact canonical bytes
across query execution, pinned values, JSON ingress and cold AROW rows, and
exercise non-NULL defaults, generated columns and CHECKs. Allocation-fault
sweeps cover lowering and native execution. Sparse stacking emits a
65,536-element result in 12,332 bytes with exactly one output allocation and
no decoded output cells; cancellation after allocation frees the output and
remains sticky. This is structural memory evidence, not a production latency
claim. Constructor-bearing schemas and catalogs require capability 25, even
with no array column and a scalar CHECK result; older array-only programs retain
their independent capability 24 contract. Generated public clients retain
constructor identity and explicit literal NULL values.

Dynamic JSONB scalar construction, element-changing durable array casts, and
array-valued ordered index keys remain guarded. Those architectural and
component proofs did not activate original corpus cases: at that stage the
audit remained 448 implemented / 136 rejected / 73 superseded / 929 unresolved.

### Shared scalar work and cancellation ownership

One scalar VM invocation now owns instruction, JSON, typed-array and exact
arithmetic admission. Nested codecs and comparisons borrow that owner instead
of restarting a work budget or reconciling successful work only after return.
NULL-heavy casts, quantified predicates and pattern sets poll the same request
control. Cancellation and work/output admission failures remain sticky across
subsequent VM entries and nested kernels. Real allocator OOM remains distinct
from a deterministic quota failure. JSON output-size/UTF-8 validation retains
its independent contract; discarded intermediate values are not validated as
outputs. Immutable identical string views compare by identity without scanning
or allocating, while actual byte comparisons are metered.

Array text input carries one optional owner across shape discovery, decoding,
typed validation and owned-allocation admission. Both parsing passes poll
bounded byte progress, including NULL cells and whitespace, and cancellation
after allocation releases the unpublished owned decoder. Regression tests
exercise pre-allocation and post-allocation cancellation, shared exact work
accounting, exhausted nested contexts, real OOM versus quota, and allocation-free
JSON comparison/validation. A zero-capacity allocator verifies ordinary scalar
rows allocate nothing: 10,000 `n + 1` evaluations use exactly 30,000 work units.
The 128/1,024-element cast/comparison runs use 389/3,077 units, checking linear
work rather than claiming a production latency improvement. The independent
`test_generate_sql_array_text_reference.py` oracle verifies all nineteen text
inputs against PostgreSQL's complete binary array representation and all
nineteen error fixtures against their exact SQLSTATEs. Native tests consume
the same fixtures; no reference is regenerated merely to accept a discrepancy.
These are invocation-local guarantees, not a claim of a single statement-wide
budget across independent VM invocations or replacement of regex's separate
pattern-work admission. This infrastructure changes no original dispositions.

### Compatibility standard

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
uv run --no-project --with 'psycopg[binary]==3.3.6' python scripts/generate_sql_postgres_reference.py typed_array_read --check zig/pkg/antfly-embedded/src/sql/fixtures/sql_typed_array_read_reference.json
uv run --no-project --with 'psycopg[binary]==3.3.6' python scripts/generate_sql_postgres_reference.py set_read --check zig/pkg/antfly-embedded/src/sql/fixtures/sql_set_read_campaign_reference.json
uv run --no-project --with 'psycopg[binary]==3.3.6' python -m unittest discover -s scripts -p test_generate_sql_postgres_reference.py
```

The 261-case read and 211-case document campaign manifests still describe
the complete cohorts, not a claim that every case works. Oracle admission and
discovery-mode native runs do not change dispositions; selected goldens must
pass the non-discovery native endpoint gate before receiving completion credit.

The separate fourteen-case typed-array read campaign keeps the original SQL and
parameters unchanged, but declares stored SQL arrays explicitly. Its seed codec
preserves lower bounds, SQL NULL elements and builtin element domains; ordinary
JSON arrays remain JSONB. The mounted endpoint gate compares complete values,
type OIDs, labels and NULL flags against PostgreSQL, including nonempty witnesses.

Catalog discovery separately exercises all 479 original `ddl`/`unsupported_ddl`
cases, including the already-dispositioned negative contracts. The current
compiler admits 76, principally catalog operations, transactions and policy
commands; that is not proof of authorization, durable publication, populated
schema rewrites or constraint activation. Enable per-case diagnostics with
`ANTFLY_SQL_CATALOG_DISCOVERY=1` and run `zig build sql-test
-Dtest-filter='SQL catalog campaign discovery'` from `zig/`.

The separate strict catalog campaign keeps ten original commands unchanged and
routes them through mounted SQL HTTP, production catalog admission and committed
metadata Raft apply. It checks command tags, conditional no-op publication,
resource identity, physical table/range topology, native relational schema,
metadata reopen and snapshot installation. A disposable PostgreSQL server
independently checks the same commands and seven duplicate/missing/nonempty
object diagnostics. The SQL adapter translates resource-neutral catalog errors
to target-specific SQLSTATEs only after handling atomic conditional outcomes;
this adds neither existence preflights nor mutation retries. Lost replies after
commit must remain non-retryable `40003`, with the committed object recoverable.
This is single-owner durability evidence, not a distributed quorum or readiness
fault campaign. It does not activate populated database retirement, CASCADE,
routine/sequence/view catalogs, identity allocation or broader schema rewrites.

Set execution retains a small hash table and streams UNION ALL without creating
temporary files. Actual retained capacity triggers promotion to the shared
external merge sorter before the statement loses spill headroom. Promotion
carries residual right-side multiplicities and already-emitted markers, so it
does not replay a delivered prefix. Replaced hash buffers are reclaimed rather
than retained in an arena. Sorted block leases expose typed keys directly;
only one owned group key and one lookahead lease survive reduction. Row and
column adapters share a memory-aware pull ceiling, and reusable scratch has a
bounded retained capacity.

The shared `sql_set_spill_reference.json` fixture checks complete PostgreSQL
multisets, native streamed values, COUNT and COUNT+SUM for UNION, INTERSECT and
EXCEPT, including ALL multiplicities. The native load gate consumes 32,768
rows under a single 2 MiB quota covering both execution and spill allocations.
Aggregate totals are additional audit checksums, not activation of broader
numeric aggregate result domains.
The no-spill comparison exceeds that quota; spilled execution completes under
it. Tests separately cover zero-file small sets, typed-array lower bounds and
NULL elements across disk blocks, allocation faults, cancellation, disk quota
and group caps. Debug timings are diagnostic measurements, not production
throughput claims. This shared executor evidence is separate from original
case completion. The eighteen-case `set_read` campaign supplies distinct
physical rows with repeated logical IDs, SQL NULLs, and archived/tenant tables
without modifying the baseline read fixture. Its PostgreSQL golden and strict
mounted native HTTP gate compare unchanged original queries, complete bags,
ordering, labels, type OIDs and NULL provenance. The native fixture captures
immutable local inputs; it does not certify distributed snapshot publication.
Contradictory source predicates (sql-0459, sql-0516 and sql-0541) remain
unresolved: the oracle's nonempty-witness admission is not relaxed for them.

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

SQL CREATE/ALTER now bind scalar expression defaults and STORED generated
columns against the complete candidate schema. Defaults cannot capture rows;
generated expressions can reference forward base columns but not themselves or
other generated columns. Numeric assignments retain checked builtin widths and
write-time overflow, including atomic batch rollback. Populated ADD operations
use the existing metadata-owned staged schema rewrite, even for nullable
generated columns. Native LSM reopen, portable restore and cold-column rewrite
tests cover these programs; this component evidence does not change original
case dispositions. Exact-decimal arithmetic, volatile producers, identity
allocation, array DDL and virtual generated columns remain guarded gaps.

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

Independent scalar programs can now borrow one statement frame without
constraining unused parameter slots. Used slots still require exact descriptor
identity, validated once at setup. Type-directed JSON frame preparation accepts
declared array text or lossless envelopes, owns retained payloads, and shares
wire/work admission across all inputs. Plain JSON arrays are not SQL arrays;
JSON strings remain logical strings for JSON descriptors. Allocation-fault,
input-owner retirement and combined-envelope quota regressions cover this
preparation layer; a 10,000-iteration composed-program test uses zero scratch
allocation. The statement-wide integration below now uses this preparation layer.

Statement-wide runtime binding now adopts the precise frame contract. PostgreSQL
wire and public HTTP prepared statements retain precise parameter descriptors.
Native datetime text inputs normalize to UTC, while
datetime binary parameters remain guarded until their codec is bound.
Public array results now retain element identity, bounds and SQL NULL flags
through generated HTTP descriptors and PostgreSQL text/binary result delivery.
Catalog storage and overloads converting
whole arrays to text/JSON remain explicit activation gaps. Default decimal
constructors still need an exact NUMERIC array representation; direct narrowing
or text casts of these constructors remain guarded (explicit real/double casts
provide floating-point semantics). Native table integer columns retain their
existing int64 contract; integer literals infer int4/int8 and explicit casts
carry their widths. No original disposition credit is granted by these
supplemental contracts.

Native relational scans now adapt declared SQL-array envelopes into complete
typed cells without reparsing JSON or cloning nested payloads. Each output page
owns one shared column-name directory; scalar-only projections keep their
existing representation. Explicit presence flags distinguish omitted fields
from present SQL NULLs. Ownership-copy helpers preserve typed arrays, JSON null
provenance and mutation metadata when session overlays retire native pages or
MERGE retains point-read rows. These are read/ownership foundations, not complete
array-column SQL activation. Ordinary INSERT/UPDATE/DELETE now retain complete
typed cells until one owned storage-envelope boundary. INSERT SELECT drains a
bounded lossless result cursor before writer admission, preserving array identity
and preventing self-inserts from observing their own writes. RETURNING binds a
shared image projection once and only decodes requested array cells; DELETE
preimages retain owned arrays. Staged session reads preserve bounds, omitted
fields and JSONB null provenance after native page/cursor retirement. Native
C-ABI sequences, PostgreSQL mutation observers and allocation-fault checks
exercise these contracts. Nullable native API columns must explicitly allow
null in their schema; this work does not weaken native JSON-schema validation.
Ordinary array assignments now infer precise target element identities for
unknown parameters and array string literals. Literals and parameters are
prepared once; repeated evaluation needs no row scratch allocation. Assignment
coercions use the shared bounded cast kernel, distinguishing numeric-array
coercions and builtin-to-text assignments from explicit-only text-to-numeric
casts. INSERT SELECT carries target domains through catalog-only source
inference, retaining bounds and SQL NULL elements at the storage boundary.
Conflict writes now retain declared element identities for old/excluded cells
and captured-output descriptors, including direct deferred typed-array queries.
Replacement images use the same typed storage boundary and preserve omitted fields. Batched
decision evaluation owns old-row payloads once, shares qualified aliases, and
retains only field-presence metadata rather than another complete preimage.
Native conflict sequences, coordinated-read backend contracts, independent
PostgreSQL observers and exhaustive allocation-failure/page-retirement checks
cover these contracts. Standalone conflict subqueries retain the existing
capability rejection until atomic range-read protection is activated. Nested
CASE/COALESCE conflict subqueries still require owner-masked Apply activation;
they are not hoisted into eager INSERT-source evaluation. Joined UPDATE/DELETE
now drain a bounded lossless source cursor and close it before writer admission.
Array assignment domains cross the shared source inference boundary; UPDATE
uses the common owned storage encoder, and DELETE RETURNING retains complete
typed preimages with shared name metadata. Per-row scratch can retire without
invalidating mutation keys, nested document members, bounds or JSONB null flags.
Presence metadata is indexed once per row instead of rescanned per output cell.
Typed captures now expose sealed, independent borrowed replay readers over
their existing bounded resident rows or spill blocks. Resident sorted rows
are retained directly without a second spool or row payload clone. External
sorts seal their selected offset/limit window once into bounded final blocks;
partially consumed sorts reject replay rather than accessing retired rows.
Joined mutations use a borrowed forward pass: finished resident sort rows retire
on advancement, and external sorts never write an unnecessary replay run.
They only own values at storage/preimage preparation. A fail-on-allocation
regression verifies zero per-row allocation for resident borrowed sorts and
checks their exact retirement boundary. Replay reader positions and decoded
blocks are independent; closing the
capture requires readers to have retired first. Rewind, array/null ownership,
sort windows, cancellation, spill cleanup and allocation-failure tests cover
the shared reader. MERGE now shares these typed replay readers across arm
classification, assignment preparation and RETURNING. Full-scan, point and
index candidates retain complete Datums; saturated fast paths retire their
source capture before starting the fallback. Decision pages own their selected
inputs before another spill block can replace borrowed storage. Keys, storage
images and DELETE preimages cross explicit owned boundaries, and all readers
and the candidate capture retire before writer admission.
RETURNING ambiguity is checked against the already-authorized join scope; this
does not activate FROM/USING source-value RETURNING, which still needs captured
source-image execution. MERGE arm binding now preserves source, target and
RETURNING array element identities, uses shared assignment coercions for unknown
array literals, and converges statement-wide parameter constraints across arms.
Binding/evaluator tests cover both UPDATE and INSERT domains, numeric-array
widening, exact bigint literals and parameters, bounds and NULL elements.
Constant and parameter arms use the same owned storage encoder as ordinary
mutations; prepared-image tests verify array envelopes rather than the scalar
JSON placeholder. PostgreSQL independently checks the same assignment and error
domains. MERGE RETURNING keeps source element identities alongside normalized
target images. Presence metadata preserves physically absent columns, and
DELETE preimages retain typed arrays, bounds, JSONB null flags and native
version/digest fences without encoding a redundant storage image. Native
document/relational regressions exercise matched updates, DELETE, source-array
widening, parameters and SQL NULL through single-row pages. Typed point/index
regressions also cover saturation and index-readiness fallback, source/target
RETURNING domains and native capture retirement before commit. Exhaustive
allocation-failure sweeps cover point UPDATE, index DELETE and saturated
fallback preparation. Source-only INSERT keeps source/target array identities
through full-scan, point and index candidates. INSERT uses the native unique
absence fence, and MERGE normalization cannot alter absence or conflict guards.
Checkpoint-by-checkpoint cancellation and row-limit failures must release all
native captures without publishing. Broader distributed fault evidence remains
to be expanded. Optional point-plan binding declines only explicitly unsupported
shapes; cancellation and backend/admission failures propagate instead of
silently choosing a write-capable fallback;
these implementation foundations do not grant original-case parity credit.
SQL DDL and ordered/constraint index keys also still need end-to-end activation
gates.
Original case dispositions remain unchanged.

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
The current branch records 365 implemented, 136 rejected and 73 superseded
cases, with 1,012 still unresolved. The earlier batches add 77 exact compiler
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
getting below 800 now requires at least 213 additional resolved dispositions.

Six exact correlated and tuple-membership UPDATE/DELETE originals
(`sql-0600`–`sql-0602`, `sql-0610`–`sql-0612`) now execute against native typed
storage through mounted HTTP. Independent PostgreSQL results check affected
counts and complete persisted state of all three tables, with duplicate and
SQL NULL witnesses. Logical keys remain unchanged; these contracts do not
claim distributed constraint-owner activation.

Eight further original cases (`sql-0220`–`sql-0222`, `sql-0284`, `sql-0302`,
`sql-1226`, `sql-1227` and `sql-1340`) now execute typed array predicates through
reads, aggregate FILTER and a left join. The PostgreSQL read campaign contains
71 exact contracts, with peer-frontier checks for the new non-unique aggregate
and timestamp ordering. The separate LATERAL campaign now activates `sql-1363`
through correlated derived-table binding and a parameterized apply operator with
per-parent ORDER/LIMIT and left-null-extension semantics; scalar pattern support
alone would not activate that relation shape.

Three original pagination contracts (`sql-0205`–`sql-0207`) now execute unchanged
through mounted HTTP over native typed storage. PostgreSQL verifies all labels,
types, rows and SQL NULL flags; complete peer frontiers permit only unspecified
equal-timestamp order. LIMIT ALL and NULL remain unbounded, NULL OFFSET is zero,
and OFFSET ROWS/FETCH FIRST or NEXT ROWS ONLY normalize to the same bounded
execution plan. Parameterized cursor coverage reads exactly five source rows
for offset two and limit three from a 10,000-row input. Omitted FETCH counts mean
one; arbitrary bound expressions and FETCH WITH TIES remain unimplemented.

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
Allocation-fault tests cover binding and preparation ownership.

One statement-owned invocation now shares precise descriptors across scalar,
aggregate, window, derived relation, CTE, recursive and set-operation binding.
Preparation freezes these contracts and decodes inputs once before execution;
cursor pulls retain that frame without copying the input payload again. Retired
or unprepared frames cannot execute. Direct HTTP execution accepts cast-constrained
array text and lossless envelopes, retaining bounds and SQL NULL flags through
public results. Window navigation promotes numeric array defaults using their
element descriptors rather than JSON placeholders. PostgreSQL oracle checks
cover source-order inference, parameter widths, numeric function result types
and array default promotion; allocation-fault tests cover nested plans and cursor
ownership. A 10,000-iteration shared-program regression performs no evaluation
scratch allocation. Physical array storage and exact NUMERIC arrays remain
activation gaps. No original
inventory dispositions change on the strength of these supplemental contracts.

PostgreSQL wire Parse now carries precise input descriptors, preserving int2/int4/
int8, real/double widths and supported array element OIDs. ParameterDescription
uses inferred descriptors instead of substituting bigint for every integer.
Bind accepts text or binary arrays through the existing bounded typed codecs;
portals own descriptor metadata independently of the prepared statement. Execute,
stream opening and subsequent cursor requests carry the same contract into native
binding and frame preparation. SQL PREPARE shares these descriptors, including
array declarations, and EXECUTE evaluates typed array arguments without JSON
shape inference. NUMERIC, json, varchar and timestamp array declarations remain
guarded where their exact codecs/identities are absent. Oracle tests independently
verify the nine supported builtin array OIDs; wire tests cover inferred and
declared input, both formats, SQL NULL, and prepared-owner retirement. These
boundary contracts do not change the original case counts.

HTTP prepared responses now expose generated SQLParameterDescriptor metadata,
aligned with the coarse compatibility parameter_types list. Durable prepared
resources retain the same widths, element identity and nullability; execution
supplies that immutable contract to binding rather than re-inferring it from
values. Resources with missing or inconsistent contracts are rejected and must
be prepared again. Python and TypeScript SDKs export the generated descriptor;
Go and Rust specifications carry the same contract. Malformed array envelopes
are client input errors, not internal failures. NUMERIC and physical array
storage remain separate unfinished work, with no additional disposition credit.

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
The shared PostgreSQL/native fixture checks 85 result contracts and 44 error
contracts, including demanded cardinality failures and invalid names in dead
branches. Mutation tests additionally verify that an unused RETURNING producer
reads no source rows, a demanded failure publishes no mutations, and unused
branches do not bypass source authorization. WHERE is lowered before downstream
producers, with its result retained once for their selected-row demand. Aggregate
FILTER similarly gates its argument producers, without bypassing validation of
unused branches. Predicates without downstream subquery consumers retain their
existing execution path rather than acquiring an unnecessary Apply.

A mixed-demand correlation regression checks 128, 512 and 1,024 target rows:
captured input rows are exactly twice the target count, with 5,672, 22,584 and
45,118 execution checkpoints respectively. It checks cross-size linear growth,
not just physical cursor reads, and allocation-fault injection covers both
demanded and bypassed producers. These are native fixture work counters, not a
production latency claim.

Scalar producers now preserve the complete child query before applying the
zero/one/multiple-row contract, including correlated ORDER/LIMIT/OFFSET,
explicit grouping and HAVING. Simple equality decorrelation retains its grouped
fast path; a catalog-bound lateral child handles query boundaries and non-keyed
correlation without moving the child's paging or predicates. Source-free
children retain qualified and unqualified outer-column binding. Referenced outer
constants remain available in explicit grouped domains; truly ungrouped inner
columns still produce PostgreSQL SQLSTATE 42803. Native ordered/grouped probes
each read 1,024 input rows under one capture for 512 targets, with 19,808/20,988
checkpoints. Fault injection covers demanded and bypassed ordered producers.
These bounds prove statement-owned reuse, not an indexed strategy for arbitrary
non-equality predicates. Catalog-bound aggregate-level admission rejects
outer-owned aggregates until their enclosing-query lifting is implemented;
PostgreSQL reference tests record the required single outer result, and native
tests reject incorrect per-parent execution before capture. Global aggregates
now retain referenced outer values as typed invocation constants, separate from
grouping keys and aggregate state. Compiler-bound frame/ordinal references carry
the active Apply values into the grouped output domain even when no inner row
exists. HAVING, sorting and external decision outputs use that same domain;
SQL NULL and JSON null retain distinct provenance. No synthetic grouping key,
first-input-row dependency or public parameter slot is introduced. A captured
empty/nonempty-source regression checks two outer parents and two physical
source occurrences: one capture, exactly twice the source row count, and two
output-provider calls. Reused compiled statements and allocation-fault tests
cover parameter inference, nested frames, lexical name shadowing and cleanup.
A spill-backed nested-result-cursor regression verifies the invocation values
outlive buffered input and retain the same empty-input results. Post-group scalar-output
demand and distributed concurrent snapshot correctness still need separate work.
No original inventory
disposition is changed by these shared-operator fixtures.

Scalar producers now apply a compiler-owned two-row result bound after validating
the original LIMIT. Literal/parameter limits, NULL, OFFSET, grouping, ordering,
windows and buffered/streaming relation paths share that bound. Query shape
inference normalizes scalar children before constraining their parameters;
RETURNING's child LIMIT no longer reaches the scalar-function binder as `$scalar`.
NULL LIMIT retains unbounded admission for ordinary result sets instead of
silently truncating at the configured result quota, and NULL OFFSET means zero.
Negative literal and parameter bounds are execution errors, with PostgreSQL's
distinct LIMIT (`2201W`) and OFFSET (`2201X`) diagnostics rather than syntax or
capacity errors. Binding does not reject an undemanded scalar child's negative
literal. These contracts do not activate arbitrary LIMIT/OFFSET expressions.

Probe batches retain their caller's demand, and a known one-row frame builds
against an unestimated input rather than eagerly building that entire source.
A native 4,096-target mutation with a million-row scalar limit reads exactly two
source rows, reports cardinality error, closes one captured read and publishes
no mutations. A direct scalar read against an unestimated million-row source
also reads exactly two rows. Separate PostgreSQL/native regressions check that a later value
error is not exposed after two valid scalar rows. Value programs stay inside
their correlated demand instead of being eagerly evaluated for unused groups;
safe column/literal equality decorrelation retains its grouped fast path.
This is not a claim that sorts, aggregates or windows can skip input required
by their own semantics.

Row-source scalar projections now have a transparent selection stage: sort-required
producers execute before sorting, while independent output producers execute only
for the sorted prefix consumed by OFFSET plus LIMIT. Computed sort aliases retain one materialized value, reused by
the final projection. The stage preserves original column identities, correlation
frames and NULL provenance instead of introducing a user-visible derived scope.
Cold forwarded columns do not become physical scan dependencies; an internal
512-cold-column regression retains the public projection-width limit. Unfiltered
pulls propagate remaining output demand without reducing residual-filter scan
capacity or truncating join builds.

Native counters check 512 sort candidates with only three independent output
invocations for LIMIT 2 OFFSET 1, 512 required sort invocations plus three output
invocations, and twelve WHERE-qualified sort invocations plus three outputs.
All use one captured read. PostgreSQL's projection-before-OFFSET behavior is
retained with and without sorting:
LIMIT 2 OFFSET 1 demands exactly three scalar outputs and source rows, and a
cardinality failure on a skipped row remains observable. These are tested plan
contracts, not a promise of identical error precedence across PostgreSQL optimizer
rewrites or constant folding. Allocation-fault tests cover selection-stage cleanup.
Ordered window output now reuses a compatible window permutation in memory as
well as on disk. It moves only row references, after all window/navigation
specifications finish in the original identity domain. The output frontier
evaluates OFFSET-skipped rows but stops at the explicit LIMIT instead of reading
and evaluating the unused ordered tail. A 512-row native regression checks
exactly one/three projection checkpoints for LIMIT 1 with OFFSET 0/2 and bounded
spill reads; an unbounded query still enforces the response quota. PostgreSQL
contracts retain skipped-row and sort-required arithmetic errors. This is a
final-projection work bound, not a claim that window input or frame computation
can skip rows needed by their semantics. Rewritten ORDER BY expressions also
discard stale qualified field spellings so canonical binding cannot recurse.
Scalar children normalize their compiler-owned two-row bound separately from
the implicit response quota, so ordered window projection reports cardinality
(`21000`) before an unused third projection rather than a result-size (`54000`)
error. Sort-required value failures remain observable. Allocation-fault
enumeration covers the final permutation's ownership across multiple window
specifications. Window input explicitly clears that scalar-result bound: COUNT,
SUM, frame calculations and sort keys must still see all required input rows.

Wildcard ORDER BY ordinals now expand against a catalog-only source view before
selection staging. Its identity cache is retained for executable binding, with
one schema resolution and one physical read capture. Derived/CTE/set-arm/lateral
domains preserve positional order and output labels. Internal sorted prefixes
retain OFFSET rows for projection without spending the public response-row
quota on those skipped rows; scan, retained-memory and spill budgets still apply.
PostgreSQL callback counters and native provider counters verify prefix demand
at offsets 0, 1, 130 and 512. Allocation-fault and mounted endpoint checks cover
successful discarded-tail selection and errors on consumed prefix/sort rows.

Window input subqueries now participate in discovery and typed Apply lowering
through PARTITION BY and window ORDER BY, including compiler-expanded named
windows. Window keys use the WHERE-qualified input domain, not an individual
aggregate FILTER or downstream output demand. Shared expression identities
own one prerequisite value; two functions using one named window evaluate its
key once per qualified row. Native provider counters check 128 input rows and
two WHERE-qualified rows under LIMIT 2, with one catalog resolution and one
captured read. PostgreSQL independently checks partition/order values, running
filtered sums, conditional key demand, masked arguments and cardinality errors
in both sort keys and unused definitions. Native allocation-fault enumeration
covers shared named-window inputs and labels; the mounted endpoint exercises
their public execution path.

Scalar output reads now bind above a typed grouping/window boundary. Aggregates
and window results have one owned slot; grouped source identities survive without
forwarding raw ungrouped rows. A catalog-only dependency pass uses ordinary
lexical binding for child references, including CTEs, shadowing and lateral
grandparents. Window boundaries forward only referenced source columns, not an
entire table layout. Group-expression equality compares bound identities rather
than spelling, so qualified/unqualified expressions and GROUP BY aliases or
ordinals share their proper slot. Missing grouped fields retain PostgreSQL's
42803 diagnostic; unknown fields retain 42703.

HAVING owns the completed-group domain before windows. A HAVING subquery and
window projection use separate group/filter/window phases; output selection
then runs independent scalar children only for the consumed sorted prefix.
Native counters verify 128/1,024 input rows, two output-provider invocations and
one captured scan without reading 1,024 unrelated columns. The scale check
records the following request-tracked peaks (not whole-process RSS or production
throughput claims):

| Shape | Input rows | Checkpoints | Peak tracked bytes |
| --- | ---: | ---: | ---: |
| Grouped | 128 | 524 | 8,781,813 |
| Grouped | 1,024 | 3,286 | 8,898,058 |
| Window | 128 | 927 | 4,197,936 |
| Window | 1,024 | 6,538 | 4,369,934 |

Allocation-fault tests
cover prepared grouped HAVING and paging; cancellation checks every recorded
checkpoint and releases each captured read without publication. PostgreSQL and
mounted HTTP contracts cover grouped/global/window outputs, empty inputs,
conditional demand, grouping diagnostics and sort-prefix cardinality failures.
Cross-level aggregate ownership lifting remains unfinished. These shared
execution regressions do not independently change original-case dispositions.

### Shared streaming ordered-set execution

`operators.OrderedAggregate` provides a statement-owned, bounded external-sort
transition for compatible `mode`, continuous percentile and discrete percentile
requests. A group retains one copy of each ordering payload, not separate row
and key copies. Multiple percentile targets become a bounded, rank-sorted event
directory and share one final streaming pass; mode retains only the current run
and best candidate. Temporary files use the existing spill manager's admission,
cancellation and cleanup machinery. Final values are copied into the caller's
result arena, so no output borrows a mutable spill head.

The operator tests cover ascending/descending ties, multiple requested ranks,
SQL NULL versus logical JSON null, exact discrete integers beyond 2^53, string
ownership, empty-group direct-argument validation, real spilling, cancellation,
disk quotas and allocation faults. Independent PostgreSQL tests check the same
rank/tie/direct-argument contracts. A 512-row forced-spill comparison writes
207,562 bytes for four compatible requests together versus 830,248 bytes for
four independent sorts (4× less I/O in this operator fixture, not an endpoint
latency claim). The immutable AST now parses the seven original WITHIN GROUP
forms, retaining ordered inputs in the ordinary argument dependency tree and
only cold direction/null-order metadata behind a pointer. Typed binding gives
direct percentile arguments their grouped key/constant domain, preserves array
fraction result element types, and assigns compatible input/filter/order slots
to shared-sort classes. It rejects ungrouped direct arguments and aggregate
nesting with 42803 and wrong aggregate clause kinds with 42809. Parser-owned
nesting and allocation limits still cover the ordered expressions.

The execution path now captures one group-key/value stream per compatible
input/FILTER/order domain. It separately sorts compact group summaries, then
merge-zips each summary with its known-size input segments. Finalization drains
the complete segment even for NULL fractions, preserving the next group's
boundary. Direct arguments evaluate in their grouped key/invocation-constant
domain; final values replace internal count slots before HAVING, projection and
final ordering. Row, decision-batch and column scan paths use the same collector.
The existing ordinary parallel aggregate path remains unchanged; ordered-set
input collection currently runs serially, without a false partial-merge claim.

Array fractions share that same rank-event pass and retain dimensions, bounds,
element types and SQL NULLs for internal SQL expressions. Requests additionally
obey statement array/memory admission. Public typed-array result delivery now
uses lossless HTTP envelopes and exact PostgreSQL array OIDs/codecs, rather
than approximating SQL arrays with ordinary JSON arrays.
Backends without an Io execute within the in-memory budget and reject spill
admission before filesystem access. Forced-spill grouped tests verify one
512-value domain, not three duplicate streams, across 128 interleaved groups;
allocation-fault enumeration verifies cleanup. PostgreSQL and native regressions
cover grouped fractions, mixed ordinary/ordered aggregates, null-only requests,
FILTER, empty input, exact discrete bigints and multidimensional fractions.

Seven unchanged originals (`sql-0560`–`sql-0566`) now pass the mounted
HTTP/native-storage gate and the reproducible PostgreSQL golden gate, including
labels, type OIDs, complete values, ordering and SQL NULL flags. The required-ID
guard prevents regeneration from silently removing them. The array-result
original `sql-0561` is independently reconciled using PostgreSQL binary results,
preserving dimensions, lower bounds, exact element OIDs and SQL NULL flags.
The oracle rejects unsupported array element types instead of flattening them.
The read golden now verifies 91 exact original contracts. `--include` can extend
a checked golden only when every existing contract still matches; unknown or
duplicate IDs and rejected originals fail closed. Broader array architecture
tests do not independently grant original-case disposition credit.

Predicate modifiers share the existing typed expression pipeline:
`BETWEEN SYMMETRIC` expands into both bound orientations using SQL
three-valued comparisons, while explicit `ASYMMETRIC` retains the ordinary
orientation. This deliberately does not use NULL-discarding LEAST/GREATEST.
`IS [NOT] UNKNOWN` requires a boolean operand and binds to existing null-test
VM opcodes, avoiding a new persisted policy capability. A shared 28-case fixture
is checked independently against PostgreSQL, including reversed/NULL bounds,
negation, precedence and exact bigints; three invalid operand types must fail.
Scalar and typed-vector tests check SQL NULL provenance, allocation-fault
cleanup and instruction limits. A 10,000-row parameterized probe requires zero
per-row scratch allocations; its debug timing is not a production speed claim.

The result boundary normalizes declared SQL integers without a floating-point
round trip, including exact numeric tokens borrowed from native preparation.
Both materialized and streaming HTTP output encode SQL integers as decimal
strings while leaving JSON-column numbers numeric. Public regressions cover
values beyond JavaScript's exact-integer range and mutation DEFAULT RETURNING.
PostgreSQL stream tests consume the result's typed-cell interface rather than
assuming legacy materialized rows.

JSONB `?|`/`?&` and `jsonb_exists_any`/`jsonb_exists_all` consume owned typed
`text[]` cells rather than treating JSON arrays as SQL arrays. Their top-level
lookup ignores SQL NULL search elements; empty non-NULL sets produce false for
ANY and true for ALL, while a SQL NULL operand stays SQL NULL. Array dimensions
and lower bounds do not change which keys are searched. This follows
[PostgreSQL's existence implementation](https://doxygen.postgresql.org/jsonb__op_8c_source.html).
Lookup shares the instruction work budget and short-circuits without row-local
key copies. Constant constructors are prepared once; typed parameter frames
infer the JSON/text-array signature, own decoded payloads and support zero
scratch allocation over repeated evaluations. A shared PostgreSQL/native/public
fixture checks 22 results plus six exact `42883` signature/arity rejections.
Exact original queries `sql-0197` and `sql-0198` are part of the strict read
campaign; unrelated document full-text queries gain no disposition credit.

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
`sql-0020`, `sql-1254`, and `sql-1255` are PostgreSQL-tested supersessions of
the original HAVING output-label extension, not successful grouped reads.
Their exact unchanged SQL returns SQLSTATE 42703 through the native-backed
endpoint and the PostgreSQL oracle. HAVING and compound ORDER BY / GROUP BY
expressions use the input namespace. Bare GROUP BY labels are available only
when no input column matches; bare ORDER BY labels prefer the output namespace.
Duplicate labels are ambiguous only when their expressions differ. Missing
input columns return 42703, while existing ungrouped input columns return 42803.
The shared fixture covers collisions, equal/different duplicate labels, grouped
source expressions, and undefined labels with the same PostgreSQL/native SQL.
These corrections do not reduce the unresolved count or claim unsupported
features as implemented.
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
Integrity preparation now preserves its no-admission evidence through the
public batch response and SQL/pgwire classification. A transient catalog read
from the read-only preparation or definite-conflict probe emits a specific
`transaction_precommit_read_unavailable` receipt; SQL reports the shared
retryable statement-read diagnostic rather than an ambiguous mutation outcome.
The marker is not inferred from a generic 503 or emitted around commit. Tests
inject a preparation read failure and assert zero commit calls, then inject
`CommitDecisionUnknown` from commit and assert it remains unknown. Receipt
classification rejects contradictory committed statuses or any transaction ID
(including malformed IDs), and remains allocation-free after native admission.
The native public-API gate owns the commit-boundary regression; the hosted CTE
gate includes receipt and public-response regressions without importing a
physical database. This repair does not complete the distributed fault cases.
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
Automatic coordinator publication remains deliberately
unwired until independently prepared standby generation adoption is integrated;
public SQL namespace resolution and its parity cases also remain unfinished.
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

### Remaining query and mutation activation

The primary-key-only mutation campaign must not certify schema-dependent
contracts merely because their statement text executes against its baseline.
In particular, `sql-1412` declares a NOT VALID CHECK in the original setup;
PostgreSQL still enforces that CHECK on new writes. The baseline has no such
CHECK, so successful insertion there does not establish parity. The six
unique-selector cases (`sql-1509`, `sql-1510`, `sql-1512`, `sql-1514`,
`sql-1515`, `sql-1517`) likewise need their ordinary/partial/expression-predicate
index owners and bounded native access evidence. Their original setup also
uses VALIDATE CONSTRAINT on index names, which is not a PostgreSQL constraint
operation. Keep these cases unresolved until the intended PostgreSQL contract
and its activated fixture are independently tested; a table scan against the
baseline is not a substitute for unique-owner activation.

Four selectors (`sql-1509`, `sql-1510`, `sql-1514`, `sql-1515`) now have
independent PostgreSQL DML results and mounted native execution against their
ordinary or status-filtered unique index owners. The native campaign builds
indexes to durable ready state and requires exact equality index spans with
primary content digests, full postimages and no full-table statement capture.
UPDATE normalization may additionally open a one-key, zero-column view pinned
to the schema version; this is not candidate discovery or a digest recheck.
The fixture rejects broad ranges and projected reads on that auxiliary path.
READ COMMITTED scans preserve native index selection for empty buffers,
predicate-only entries and writes to unrelated physical tables. Every matching
entry still validates both schema fences, including stale entries after a
matching write. Actual staged overlays and stronger-isolation guards retain
their protected paths. Multi-scan statement overlays remain conservative.
All four cases remain unresolved because their original
index-as-constraint validation setup and public catalog activation still need
end-to-end adjudication. Expression-predicate and positive-amount selectors
(`sql-1512`, `sql-1517`) still need their own owner profiles and access proof.

JSONB path extraction now shares strict typed binding and immutable traversal
between `jsonb_extract_path` and `jsonb_extract_path_text`. Missing components
and SQL NULL do not skip evaluation of later arguments; JSON null remains a
value for JSONB extraction and becomes SQL NULL only for text extraction.
Reads and path updates share PostgreSQL's signed 32-bit ordinal parser,
including leading whitespace but excluding trailing whitespace/separators.
Text extraction uses the canonical JSONB writer rather than API JSON encoding,
preserving object-key order, separators and decimal scale. PostgreSQL goldens
and native tests cover these distinctions, and repeated point extraction
borrows the original string under a zero-capacity allocator and shared work
budget. These are component contracts, not original-case completion credit:
generated document/index owner profiles are still required before their
historical cases can be activated. The four explicit partial/expression arbiter
profiles and ordinary UNIQUE(email)
upserts now have the explicit owner profile described below.

Eight original UNIQUE(email) mutations (sql-1394, sql-1395, sql-1398, sql-1399,
sql-1400, sql-1402, sql-1406 and sql-1407) execute unchanged against native
logical PK and UNIQUE owners. The profile retains the base seed rows and
supplies the nullable `next_status` column required by that source cohort;
it does not rewrite source statements or change the baseline mutation oracle.
Schema declarations, fixture reset writes and mutations share native integrity
machinery. An independent duplicate-email probe must fail with `23505` and
leave storage unchanged, preventing an unconstrained fixture from receiving
credit. PostgreSQL checks exact RETURNING metadata/NULLs and complete postimages
of every table. Probes use the same streaming row/byte bounds as other oracle
mutations, and wrong codes or unexpectedly accepted probes fail closed. Native
read counters enforce no unbounded source reads or full statement capture for
this cohort. Ambiguous unqualified conflict expressions, temporal profiles,
broader partial-index implication and distributed fault activation remain
separate work; their historical entries have not been reclassified.

Unique-owner provenance is now an OpenAPI-generated `constraint`/`index`
identity, retained in schema metadata rather than inferred from editable index
descriptions. Immutable SQL schema views derive only named constraints in a
linear pass; index-owned keys still participate in native conflict inference.
Table-bound DROP/VALIDATE CONSTRAINT reject index owners, while index retirement
removes its paired uniqueness rule even after a description edit. Native cache
lifetime/allocation-fault tests and independent PostgreSQL contracts cover the
namespace distinction. Named SET CONSTRAINTS excludes index owners before any
native authority read, while genuine deferred constraints retain their native
generation checks. Mounted tests repeat the eight inferred upserts with real
index-owned uniqueness, unchanged expected postimages and no unbounded source
reads/capture; the named index alias must reject without a storage change.
Unqualified PostgreSQL DROP INDEX still needs catalog
owner resolution; the table-bound schema-builder tests do not claim that syntax
is complete.

Four original upserts (sql-1455, sql-1458, sql-1460 and sql-1461) now execute
with their own partial, lower(email), mixed tenant/lower(email), and upper(email)
unique-index declarations. Each profile preserves the base schema and all seed
rows. The same canonical CREATE UNIQUE INDEX SQL is independently executed by
PostgreSQL and compiled through native schema DDL, rather than replacing an
expression or partial index with ordinary UNIQUE(email). Separate probes
require duplicate rejection (23505), named-index-alias rejection (42704), and
wrong inference-target rejection (42P10), without changing native storage.
Complete RETURNING labels/types/NULL provenance and all three table postimages
match PostgreSQL; native owner resolution uses point reads with no unbounded
source reads or statement capture.

This activation exposed borrowed nested JSON operands in uniqueness metadata:
parseFromValue retained predicate/expression DOM storage after request cleanup.
Relational declarations now use an owned token-stream parse, like index and
CHECK definitions. Schema lifetime and exhaustive allocation-fault coverage
verify escaped string predicates, nested expressions and exact numeric operands
after source DOM retirement. This is schema-publication work, not extra parsing
on the per-row execution path. Named partial/expression index aliases and global
index namespace resolution remain uncredited.

Quantified scalar children now retain correlated ORDER/LIMIT/OFFSET,
group/aggregate, window and nested-derived boundaries in a typed Apply
producer. Comparison results are projected once and reduced into separate
decisive and unknown witnesses, preserving empty-set ANY=false/ALL=true and
SQL NULL versus JSON null. Simple keyed children and provably independent
ordered/grouped boundaries retain their grouped hash summary path. A
conservative lexical closure proof leaves unresolved unqualified references
in Apply until catalog binding; generated qualifiers avoid quoted-name
collisions. The six comparison operators share 336 PostgreSQL/native
truth-table results, and operand/LIMIT/OFFSET parameter inference is checked.
Captured native mutation tests check linear work and two physical
input scans at 128 and 1,024 targets, plus allocation-fault and every-checkpoint
cancellation cleanup before publication. PostgreSQL and mounted HTTP contracts
exercise correlated child paging, membership, ordered quantifiers and patterns.
The keyed benchmark uses 8,872/70,802 checkpoints for 128/1,024 targets;
this is a linear operator-work bound, not an indexed guarantee for arbitrary
non-equality predicates or proof of early termination on decisive witnesses.
This does not implement cross-level aggregate ownership lifting or add original
inventory completion credit from component-only tests.

Row-valued UPDATE subqueries now retain an exact compiler-owned output-width
contract until the pinned source layout expands wildcards. Both shape inference
and executable derived binding enforce that contract before capture; an empty
source cannot conceal a column mismatch. Qualified wildcards and mixed
wildcard/scalar projections retain positional assignment, source ordering,
one captured read set and one mutation commit. COUNT(*) has one output despite
its compact AST representation. Independent PostgreSQL tests verify complete
postimages, `42601` width diagnostics, `21000` cardinality diagnostics and
unchanged storage after errors. Native tests additionally cover whole-row
SQL NULL assignment on an empty source, allocation-fault cleanup, and exact
typed-array lower bounds/NULL elements/64-bit payloads. A mounted HTTP
regression verifies a committed wildcard row update, both error SQLSTATEs,
direct native postimages after each error, and sparse nullable-field retention.
Correlated row assignments share one typed multi-output Apply producer rather
than independent scalar readers. The producer owns the first tuple before
checking a second-row witness; zero rows assign SQL NULL to every column and
multiple rows abort before commit. Ordering, paging, nested derived sources,
outer arithmetic and correlated COUNT(*) retain their per-target semantics.
The keyed-correlation scaling regression checks bounded native reads and linear
execution checkpoints at 128 and 1,024 target rows. Allocation-fault tests cover
correlated producer cleanup. No original
inventory case receives completion credit from these component regressions.

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
