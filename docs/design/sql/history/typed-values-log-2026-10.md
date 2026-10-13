# SQL extraction: typed rows, arrays, precise types and exact NUMERIC log (2026-10)

> Relocated verbatim from `docs/sql-extraction.md` (lines 3767–4362, 4395–4706, 4886–5262, 5293–5901, 5930–6667) at commit 3ce5aff4ea on 2026-10-12. Durable decisions were folded into [SQL extraction](../extraction.md) before the move. This is a historical implementation log; it describes work at the time it was performed, and its counts and test totals are not current guarantees.

### PostgreSQL campaign validation (2026-10-06)

PostgreSQL is the SQL semantic authority; PostgreSQL 19 is the target and the
current independent oracle uses an isolated PostgreSQL 18.6 instance. The next
read/document batch resolves 103 original contracts (54 reads, 49 document
mutations), leaving 1,066 unresolved. Twenty-four legacy planner rejections are
explicitly superseded by tested guarded mutation execution. Neither oracle
admission nor an old schema's readiness/cardinality annotations grant completion
or authority.

Native execution now preserves function/CASE labels, admits PostgreSQL postfix
NULL tests and implements bounded Unicode-aware padding, repetition, reversal,
position lookup and bit lengths. A nested join buffering regression now owns
text/JSON before advancing upstream pages; the regression includes allocation
faults. The bounded scratch text workload processes 50,000 rows in approximately
101–116 ms in a debug build with 514 bytes of reusable scratch. This is an
absolute workload measurement, not a before/after speedup claim.

The PostgreSQL oracle validates typed complete results, SQL NULL provenance,
affected rows, untouched document state, assignment presence and valid ordered
LIMIT peer frontiers. Server-side cursors avoid buffering full read results.
All five recorded campaign evidence gates pass. The 251-read and 211-document
cohorts remain incomplete: remaining work needs shape-specific executable
profiles for typed arrays, temporal/regex functions, virtual document metadata,
root replacements, generated fields and constraint/index ownership. SQL syntax
or schemas that disagree with PostgreSQL must not gain positive parity credit
through SQLite emulation or fixture-only authority.

### Typed relation row boundary (2026-10-07)

Internal relation pages now retain owned complete Datums and share one
name-to-ordinal directory per page. They no longer build a JSON object plus
SQL-null and pattern side channels for every row. Array dimensions, lower
bounds, NULL elements, exact bigint values and pattern owners survive scalar,
join and grouped row adapters. Internal coercion borrows already-owned array
values without allocating or converting their JSON placeholder; incompatible
element descriptors and JSON-null substitutes fail closed. Primitive column
pages remain available, while relations containing arrays use the lossless
typed-row path until the column-page codec can represent arrays.

The executable boundary contracts cover scalar, cross-join and grouped reads,
ownership after source mutation, allocation-failure cleanup, SQL/JSON NULL
distinction and the public array-output guard. PostgreSQL independently checks
the same scalar/join/grouped values. This is architectural progress, not an
original-case activation: the inventory remains 358 implemented, 136 rejected,
73 superseded and 1,019 unresolved. Common array element-type coercion across
set arms and the generated public/pgwire array descriptors and codecs still
need completion before the remaining array original cases can receive parity
credit.

### Typed internal query results (2026-10-07)

Internal binding now retains array-valued outputs instead of applying public
wire restrictions at every derived-relation boundary. Public binding still
rejects unsupported wire result types before execution. Nested blocking queries,
scalar owners and window input use one typed sink boundary, with either a shared
spill cursor or a bounded no-I/O replay cursor. Sorted results transfer their
operator ownership rather than materializing another JSON result matrix. Scalar
owners keep the two-row cardinality frontier; array-valued min/max capture keeps
its element descriptor as well as its owned value.

External window ingestion no longer rebinds and routes internal input through
the public JSON stream. It retains one bounded typed page for the small-input
fast path and then writes directly into its window row/column store. Memory
window execution releases its input cursor before calculating windows. Deferred
provider output and ordinary selected rows honor typed sinks as well, so neither
path silently drops array payloads.

Executable PostgreSQL contracts cover CTEs, materialization, ordered subqueries,
same-type UNION ALL, array-valued ordered-set aggregates, windows and scalar
subqueries. The engine exercises memory and spill modes, including array values,
NULL elements and allocation-failure cleanup; the mounted HTTP gate checks the
same seven query shapes, complete wire rows, column types and SQL-null flags.
These contracts do not activate unrelated original cases or publish raw arrays.

### Common set and VALUES row types (2026-10-07)

Set binding now retains complete element descriptors and resolves binary
UNION/INTERSECT/EXCEPT nodes before their parents. Each arm is coerced before
the node compares, hashes or emits it. In particular, an outer float4 widening
cannot collapse two bigint arrays before an inner DISTINCT has run. Numeric
widths, NULL arrays and unknown string literals participate in the same bound
coercion programs; incompatible concrete array types fail with cannot-coerce
rather than silently converting through JSON placeholders.

Standalone VALUES uses the existing bounded flat row-source machinery. VALUES
selects one common type across all rows, not a pairwise set-operation chain.
Literal rows retain their grouped fast path and allocation-free literal type
inspection. Nontrivial coercions use compiled expressions. Both binding and
execution preserve derived/CTE boundaries: unknown results become text inside
their producer and cannot subsequently infer a numeric parameter from an outer
set or assignment. Tests that previously assumed otherwise now use explicit
inner casts, with independent PostgreSQL negatives retaining the original
invalid forms. Bare target-context INSERT parameters remain a separate valid
assignment path.

This advances the internal type architecture, not original-case activation.
Public raw-array and pgwire descriptors/codecs, typed mutation capture and the
broader scalar common-type/overload catalog remain unfinished. The original
inventory remains 358 implemented, 136 rejected, 73 superseded and 1,019
unresolved until exact-source mounted execution earns additional dispositions.

### Lossless array result codec (2026-10-07)

The shared array wire codec now represents non-NULL arrays as dimensions
(`length`, `lower_bound`), flat row-major values and aligned SQL-null flags.
Element type remains a bound-column descriptor; JSON shape never chooses a SQL
type. Integers use exact canonical decimal strings. Finite floating values use
JSON numbers, while NaN and infinities use explicit tokens instead of collapsing
to JSON null. JSONB null elements and SQL NULL elements remain distinct.

Streaming output validates and admits the complete value before destination
writes, without per-cell serialization buffers. Retained byte output has one
exact-sized allocation. Ordinal JSON output materializes directly without a
stringify/parse round trip, and decode validates before retaining cells, clones
payload ownership and can use a stable quota owner accounting arena capacity.
Both ordinary JSON numeric parsing and exact number-token parsing preserve
dimension metadata. The PostgreSQL text/binary fixtures cover element domains,
bounds and NULL provenance; JSONB comparison is semantic because PostgreSQL's
binary JSONB wrapper contains formatted JSON rather than canonical row bytes.
Malformed shape/type/quota cases reject before cell allocation, and allocation
faults exercise retained byte output, JSON materialization and decoded owners.

A debug streaming workload emitted 262,144 cells / 3,439,056 bytes in about
119 ms with zero encoder allocations. This is an absolute local workload, not
a before/after speedup claim. Public activation is still guarded: generated
column/array OpenAPI contracts, final-output integration, PostgreSQL element OID
and result-codec integration, and transport-level original-case proofs are the
next dependencies. This codec alone earns no original-case disposition credit.

### Array result descriptors and streaming text output (2026-10-07)

OpenAPI now defines the bound array element enum, dimensions and lossless
value envelope. Zig, Go, Python and TypeScript models are generated from that
contract; the Rust SDK's input specification is synchronized. Python and
TypeScript export these models through their public SDK entry points. The
Python SQL tests and TypeScript type tests preserve exact integer strings,
non-default bounds and JSON-null versus SQL-null flags.

Final SELECT, grouped/window, sorted/deferred and mutation-returning paths now
retain complete Datums until the result boundary rather than extracting an
array's JSON-null placeholder. The boundary checks element descriptors and
copies retained envelopes into the result owner. A native regression releases
the source array before inspecting nested JSONB output; mismatched element
descriptors and accidental JSON substitution are rejected.

The PostgreSQL text-array encoder streams dimensions and flat cells through
bounded-depth braces. A writer adapter escapes text and JSONB directly without
per-element serialization buffers or encoder allocations. Whole-value domain,
wire-byte and shared validation/emission work admission happen before output.
Tests round-trip all 19 PostgreSQL reference vectors and independently check
exact emitted escaping, bounds, integer extrema, NaN/infinities, empty arrays,
SQL NULL and JSONB null. PostgreSQL independently decodes the exact emitted
vectors. Byte/work quota rejection leaves the destination untouched; actual
writer failures remain writer failures. The JSON-envelope encoder now also
shares validation and emission work admission rather than budgeting each
phase independently.

Public array results remain deliberately guarded until PostgreSQL column/OID
and result-codec integration and mounted HTTP/pgwire execution evidence are
complete. Array parameters and native stored-array columns still need their
typed ingress/storage contracts; this change does not reinterpret document
JSON arrays as SQL arrays. No original inventory case is credited by these
architecture-only changes.

### Public array result activation (2026-10-07)

Array-valued result columns now cross the typed public boundary with explicit
element descriptors. Pgwire describe, retained execution and streaming metadata
retain that descriptor and advertise its exact PostgreSQL array OID. Both text
and binary DataRow encoders use the descriptor rather than guessing from JSON.
NULL arrays use the ordinary outer NULL framing; empty arrays and JSONB-null
elements remain independently representable.

Prepared/cursor shape checks include element identity, not only column names
and coarse `array` type, preventing an element-OID change across execution or
resumed pages. DataRow admission reserves all remaining cell length headers
and admits each payload against actual remaining frame space. Primitive/JSON
cells preflight their encoded size as well. A two-array row regression proves
individually fitting cells cannot publish an oversized combined frame.

Immediate pgwire encoding validates a borrowed envelope view with two flat
cell/axis buffers. Nested JSONB and text payloads remain pinned to the result
owner instead of being cloned again. Allocation-failure tests unwind either
buffer, and payload-pointer tests establish borrowing. Encoder quota checks
bound the actual PostgreSQL payload independently of the differently sized JSON
envelope, so compact binary arrays do not inherit a JSON-metadata frame limit.
Array parameters still reject without a complete typed input descriptor rather
than silently converting an array expression's JSON-null placeholder.

Native contracts exercise constant, empty, NULL, multidimensional/non-default
bound, JSONB, set/VALUES promotion, window and scalar-subquery outputs with and
without execution I/O. Existing array-rejection tests are now positive value
and ownership assertions, retaining allocation-failure coverage. Ordered-set
percentile arrays also assert exact fractional values and SQL-null flags.
Mounted HTTP contracts exercise generated metadata and envelope decoding; the
authenticated native pgwire adapter exercises describe/execute and both output
codecs. Protocol tests independently inspect RowDescription OIDs and DataRow
lengths/payloads in simple-text and extended-binary sessions, including empty
and NULL arrays. PostgreSQL independently proves the producer result types and
logical values. These are architectural contracts, not additional original-case
disposition credit. Typed array ingress, native stored-array columns and original
source-case reconciliation remain unfinished.

### Exact-source SQL array result reconciliation

The PostgreSQL read oracle now requests binary results and independently
decodes supported SQL array OIDs into the public lossless envelope. It retains
rank, axis lengths, lower bounds, decimal-string integer cells, non-finite float
tokens and separate SQL NULL flags, including JSONB null versus SQL NULL.
Rank, byte and element budgets and complete-frame checks reject malformed or
unsupported contracts instead of silently flattening arrays into JSON lists.
Native reference comparisons check exact element OIDs and typed array values,
with regressions rejecting altered bounds and null provenance.

The unchanged grouped multi-percentile original `sql-0561` now passes mounted
HTTP over native typed storage and the independently reproduced PostgreSQL
golden. All 77 pre-existing read contracts remain unchanged; the selected read
golden contains 78 contracts. Golden extension requires a checked baseline and
fails on existing drift, unknown/duplicate IDs or PostgreSQL rejection.
The inventory records 365 implemented, 136 rejected, 73 superseded and 1,012
unresolved cases. General array regressions do not create additional original
case credit, and native stored-array columns remain unfinished.

### NULL-aware tuple membership in captured relational execution

`sql/tuple_membership.zig`, exported through the relational operators, provides
the retained lookup kernel for row-valued `IN` and `NOT IN`. Ordinary equality
hash joins cannot implement the required three-valued logic: `(NULL, 1)` is
definitively unequal to `(2, 2)`, but potentially equal to `(2, 1)`. Empty inner
relations are false for `IN`, including an all-NULL left tuple, and `NOT IN`
negates only true/false, retaining UNKNOWN.

A shared-prefix trie hashes edges by parent and typed cell value, checks hash
collisions with typed comparison, and retains one owned payload per distinct
prefix. Exact non-NULL hits use one lookup per column. Ambiguous NULL probes
visit only compatible prefixes using a fixed stack bounded by the admitted
256-column arity. This avoids per-probe allocation, per-outer-row source scans,
JSON key serialization and exponential precomputed NULL-mask tables. Duplicate
right tuples share their complete path. Array keys preserve dimensions, lower
bounds, exact integer values and element NULL flags; JSONB null is a non-NULL
SQL value. The binder establishes per-position common comparison domains and
checks exact arity before opening sources. Array equality requires identical
element types, unlike numeric array promotion in UNION/VALUES; incompatible
operator signatures produce PostgreSQL's undefined-operator diagnostic.

Input-row, retained-byte and cumulative-work admission and cooperative
checkpoints bound build and ambiguous searches. A failed build poisons the
index so partial prefixes cannot become visible as successful rows. Source
payloads are cloned before their page owner retires. Allocation-fault tests
exercise complete cleanup, and deterministic work tests bound 4,096 exact
two-column probes independently of machine timing. PostgreSQL independently
reproduces 1,740 IN/NOT IN truth pairs over 110 right-hand multisets, including
all one/two-column NULL combinations, duplicates, empty sources and selected
three-column sources. Kernel tests alone do not create original-case credit.

Parenthesized and explicit ROW constructors now lower in membership contexts
to a compiler-owned relational operator, not a serialized JSON scalar. Cold
scan projections retain both build and probe keys. Equality-correlated keys
prefix the retained lookup; NULL correlation keys select an empty inner domain,
not the wildcard NULL semantics of tuple comparison. Invariant builds can be
shared across Apply/recursive iterations. Complex correlated projections and
sort/page/group boundaries instead preserve the original child query inside a
demanded LATERAL producer. Separate true and unknown witnesses summarize its
actual output without evaluating expressions in eliminated correlation groups.
Masked branches keep their existing demand and lexical outer-frame bindings.
Scalar membership's existing grouped fast path remains unchanged.

Six original correlated and tuple-membership mutations (`sql-0600`–`sql-0602`,
`sql-0610`–`sql-0612`) execute unchanged through mounted HTTP. Independent
PostgreSQL golden results verify affected counts and complete persisted state
of all source/target tables, with duplicate and SQL NULL witnesses. Each
statement captures one native read set; logical keys are unchanged and no
distributed constraint-owner activation is inferred from these fixtures.

Fourteen complete native query contracts are independently checked against
PostgreSQL, including query boundaries, masked evaluation, array/JSONB keys,
and correlated NULL witnesses. An allocation-failure campaign exercises the
retained Apply build lifecycle. EXPLAIN identifies NULL-aware membership rather
than calling it an ordinary hash join.

Remaining architecture includes spill-backed membership, finer-grained
correlation dependency pruning and partition reuse, broader row/composite
expressions, consistent arity diagnostics through complex derived projections,
and broader mounted read/pgwire coverage. Retained-byte, row and work admission
currently fail closed; this is not an unbounded fallback or a claim that the
entire distributed SQL architecture is complete.

### Typed array search and replacement execution

`array_position`, `array_positions`, `array_remove` and `array_replace` now
share the typed array boundary and follow [PostgreSQL's array-function
contracts](https://www.postgresql.org/docs/18/functions-array.html). Binding
resolves PostgreSQL's anycompatible
element family separately from exact array-operator identity, inserts explicit
operand coercions, and retains authoritative prepared-input widths. Unknown
literal strings adopt a known element domain; typed incompatible arguments
produce an undefined-function diagnostic rather than an implicit text cast.

Search/removal use IS NOT DISTINCT FROM semantics, including SQL NULL and NaN.
Search results are actual subscripts, not zero-based offsets. Positions results
are one-based int4 arrays; removal preserves the input lower bound unless its
result is empty. Replacement supports all admitted ranks and retains every
dimension and lower bound. One-dimensional-only searches/removal return the
PostgreSQL unsupported-feature diagnostic for multidimensional inputs. A NULL
initial search position is diagnosed for a nonempty one-dimensional array;
NULL and empty arrays return NULL. The start argument requires an int4-compatible
input descriptor rather than an implicit narrowing cast from bigint.

Dynamic lookup is a bounded scan. Constant arrays without a start-position
argument reuse the program-owned typed membership index, including its first
NULL ordinal. Exact-sized positions/removal output vectors use two bounded
passes; replacement uses one pass before typed validation. Result cells borrow
pinned input payloads, and retained operator boundaries continue to clone them
before source retirement. Work and output-byte admission cover execution and
result construction, and allocation-failure tests cover prepared indexes and
transforms. A 10,000-probe debug workload over a retained 16-cell constant array
used zero evaluation scratch bytes and took approximately 10 ms; this is an
absolute measurement, not a before/after speedup claim.

Independent PostgreSQL contracts check complete Boolean semantics, exact array
OIDs, dimensions, bigint payloads, SQL NULL flags and diagnostic codes. Mounted
HTTP checks array envelopes and prepared promotions. These are shared execution
contracts, not original-corpus disposition credit: stored-array schema/read/
mutation activation and the broader array-function catalog remain unfinished.
The original-case inventory remains 365 implemented,
136 rejected, 73 superseded and 1,012 unresolved.

### Shape-aware array concatenation

`array_append`, `array_prepend`, `array_cat` and array-valued `||` overloads
share the compatible-element resolver and explicit typed coercions. Array
operator resolution distinguishes unknown string/NULL operands (array-array
concatenation) from typed scalar operands (append/prepend); text/JSON scalar
concatenation retains its existing path. Prepared-input descriptors retain
their original widths even when the result promotes to a wider element type.

Append/prepend accept empty or one-dimensional arrays and preserve an existing
lower bound. Concatenation admits equal or adjacent ranks: equal-rank operands
must have matching trailing lengths and lower bounds; adjacent-rank operands
must match the complete lower-rank shape to the higher-rank tail. The result
retains the appropriate first-axis lower bound rather than normalizing it to
one. Nonidentity construction allocates one admitted flat element vector and
dimension vector, borrowing pinned payloads until the usual retention boundary.
NULL/empty concatenation identities reuse the existing typed operand without
allocating or copying its vector. Rank/shape errors have PostgreSQL diagnostic
codes, and dimension-growth overflow fails admission before result allocation.

Native contracts cover array/element NULL distinctions, equal/adjacent ranks,
non-default bounds, exact bigint promotion, unknown-string overloads and masked
evaluation. Independent PostgreSQL checks verify these semantics and complete
typed output shapes. Allocation-failure tests cover nested construction and
operator lowering. A prepared identity workload asserts zero evaluation scratch
allocation over 10,000 rows and measured approximately 3.7 ms in a debug build
(an absolute workload measurement, not a before/after speedup claim).
Mounted HTTP contracts check exact envelopes and
cross-width array parameters. This remains shared execution progress, not
additional original-case credit or stored-array activation.

### Declared array schema identity

CREATE TABLE and ALTER TABLE ADD COLUMN now retain the declared builtin element
identity in the immutable SQL AST, using the same type parser as casts. The
descriptor distinguishes all nine admitted array element domains, including
int2/int4/int8 and float4/float8, rather than collapsing an array into JSON or a
coarse numeric column. Declared dimension counts and sizes do not constrain
actual PostgreSQL array values and are not retained as type identity.

Native contracts cover sixteen declarations and aliases, CREATE/ALTER descriptor
equivalence, nullability, malformed dimensions, bounded token admission and
every allocation-failure point. Independent PostgreSQL catalog checks compare
exact OIDs and nullability for both DDL forms and verify that empty arrays are
accepted regardless of declared dimensions. This descriptor work did not itself
activate narrow scalar storage; the complete scalar boundary is described below.

This is schema-boundary groundwork, not stored-array activation or additional
original-case credit. Durable schema metadata, physical array cells, native
read/mutation adaptation and canonical backup/restore validation remain to be
connected. CREATE array schemas still fail before publication; ALTER failure
leaves the original schema unchanged. These guards must only be removed after
the complete typed storage path is verified. The inventory remains 365
implemented, 136 rejected, 73 superseded and 1,012 unresolved.

### Durable precise SQL type contracts

SQL binding and immutable runtime schemas share one storage-independent builtin
identity definition, retaining the existing scalar/array PostgreSQL OIDs.
Runtime schema format 16 adds an optional precise SQL descriptor per physical
column. Canonical schema equality includes it; serializing a declared type into
an older format fails instead of silently dropping the descriptor. The decoder
preflights descriptor tags, complete structure and physical-type compatibility
before ownership transfers. Schemas without precise declarations retain their
existing SQL admission behavior.

Prepared and ordinary row encoding, strict decode/restore, and projected reads
check declared integer widths, canonical float4 widening, UUID spelling and
SQL text encoding without allocating. A physically valid checksum alone does
not admit an out-of-domain value. Existing native limitations on nonfinite
scalar number cells remain explicit; this does not claim stored PostgreSQL
NaN/infinity support. Index-cover fingerprints and source binding retain the
precise descriptor. Ordered tuple and expression fingerprints also bind their
declared domains, and cold tuple/row source binding rejects an unconverted
domain change even when coarse physical types match. Byte-retaining rewrite
plans refuse descriptor changes
until an explicit typed conversion path exists. Full-text projection identity
does not change merely because this SQL metadata is present.

Deployed format-15 document/relational catalogs remain readable without a write
on open. New schema publication atomically advances the catalog capability to
the runtime schema format; a catalog that advertises only format 15 cannot
authorize a schema carrying precise descriptors. Stored array cells and their
complete typed read/mutation/backup activation remain unfinished. No original-case
dispositions are credited for this infrastructure alone.

### Public scalar SQL domains and canonical preparation

Relational root scalar properties can explicitly declare `x-antfly-sql-type`,
using the OpenAPI-generated `SQLBuiltinType` enum. The native property type must
match its SQL domain. SQL CREATE/ADD COLUMN emits this annotation for known
builtins, and catalog loading retains all nine builtin identities and numeric
widths. Python, TypeScript, Go and Zig models are generated from the public
specification. The Rust SDK specification is synchronized as well.

Existing standard `format` annotations do not acquire new SQL range semantics.
Document tables and nested/composed scalar annotations are rejected rather than
silently losing SQL identity during physical-layout derivation. SQL arrays are
not inferred from JSON array properties. Stored-array DDL remains guarded.

An immutable compiled column plan normalizes owned input before checks, hashes
and index extraction. Integer widths are checked without floating-point
conversion; float4 inputs are rounded once into their canonical widened value;
UUID strings are canonicalized. Numeric strings are not implicitly admitted by
the native JSON write API. CREATE/ALTER defaults use the same precise widths,
and an invalid replacement default leaves the original schema intact. Stored
expressions convert into their target domain before dependent expressions run;
restore evaluates the same conversions to verify generated results. Output-only
generated fields do not validate caller-supplied replacement values.

Restore verifies rather than repairs scalar values. JSONB string/key validation
shares the bounded, allocation-free text-domain walk used by typed arrays,
including field-local restore checks. Ordinary schema updates cannot reinterpret
a retained column under another SQL domain; explicit typed conversion remains
required and is not yet implemented. Native nonfinite scalar-number storage is
still unsupported, and this work does not claim complete PostgreSQL JSONB
numeric-domain parity.

Native contracts cover public-schema validation, canonical preparation, generated
dependencies, allocation failures, canonical semantic hashes/row bytes, LSM
reopen and mixed-batch atomicity. PostgreSQL independently verifies the native
scalar fixture's exact float4 value, catalog OIDs, integer/float range errors and
text NUL rejection. The mixed-schema allocation-failure sweep forces the backing
allocator's allocate/copy growth path, so optional in-place arena resizing cannot
change the number of fault points between trials; every growth allocation remains
faulted and leak-checked. These are shared architecture contracts, not new
original-case credits: 365 implemented, 136 rejected, 73 superseded and 1,012
unresolved remain the authoritative inventory.

### Compact schema-bound array codec groundwork

A borrowed, allocation-free directory now addresses flat typed array payloads
using schema-owned element identity, dimensions/lower bounds, a NULL bitmap and
fixed-width slots or variable-width offsets. NULL-heavy primitive arrays use
non-NULL slots with rank checkpoints every 64 cells; lookup remains constant
time. Header-only shape projection reads O(rank) metadata from already
authenticated rows. Untrusted publication requires full structural and canonical
validation, including bounded semantic JSONB checks; shape projection alone is
not a restore gate.

Primitive preparation allocates nothing and encoding makes one exact output
allocation. JSONB canonicalizes each element once during preparation; strict
validation reuses bounded scratch storage and rejects, rather than repairs,
noncanonical input. Allocation-failure tests also exposed and fixed the shared
JSON memory writer's `WriteFailed` mapping: buffer exhaustion now propagates as
`OutOfMemory`, allowing quota and injected-failure handling to remain accurate.

Tests cover eleven PostgreSQL binary fixtures across all nine element types,
multidimensional/nondefault bounds, SQL NULL versus JSONB null, exact bigint and
floating-point representations, 135 dense/compact boundary cohorts, malformed
directories, truncated frames, budgets and exhaustive allocation failures. A
4,096-cell fixture with one eighth NULL occupies 4,372 bytes for boolean, 7,956
for int16 and 29,460 for int64, versus PostgreSQL wire sizes of 19,988, 23,572 and
45,076 bytes respectively. These are codec byte/allocation measurements, not an
end-to-end storage latency claim.

This codec is not yet a published native column format. Native schema capability
activation, row preparation/materialization, semantic hashing, index semantics
and restore integration remain required before stored-array DDL can be enabled.
No original SQL case receives new implementation credit from this groundwork.

### Native typed-array row boundary

Native schemas now distinguish `sql_array` from JSON, blobs and dense vectors.
ASCH 17 binds the mandatory precise element identity to the immutable column.
Format 15/16 catalogs and deployed scalar schemas remain readable without an
upgrade write on open; an older capability cannot authorize an array layout,
and serialization refuses a downgrade that would lose the array contract.
Document-mode layouts and JSON-backed array declarations are rejected.

Native preparation borrows the existing parsed envelope while producing one
canonical flat payload. Packed and cold-column cells retain element identity;
logical reconstruction uses the lossless dimensions/values/SQL-NULL envelope.
Semantic hashing includes typed elements, shape, lower bounds and SQL NULLs,
not offsets, padding, frame flags or the dense/compact representation. Physical
floating zero signs survive storage while their semantic hashes agree.

Strict restore checks complete array canonicality, including JSONB semantics,
even when a noncanonical row has a valid physical checksum. Already admitted,
authenticated rows instead inspect only addressing extents for targeted reads;
shape projection and primitive cell lookup require no flat cell materialization.

Stored-array public schema declarations are active; SQL DDL activation and SQL
scan/mutation adapters remain unfinished. Array ordered/unique/FK index keys
remain explicitly guarded
until typed ordering is implemented; arrays are not silently indexed as blobs.
These native contracts do not change the original-case disposition counts.

Owned JSON numeric values use one exact IEEE-754-to-decimal kernel for ordering,
hashing and canonical persistence, avoiding shortest-print rounding after logical
identity has already been established. Parsed JSONB retains decimal tokens;
this internal consistency contract does not redefine SQL float-to-JSONB casts.
Independent Python decimal and PostgreSQL JSONB checks verify the explicit
numeric fixtures. Native tests also cover subnormal and maximum finite values.

Verification: 406 embedded and 175 hosted SQL tests; 154 embedded and one hosted
native relational-system test; 101 PostgreSQL oracle tests. Native preparation
and reconstruction unwind every injected allocation failure for all eleven
binary array fixtures. The debug trusted-projection fixture performs 10,000
lookups in approximately 3.7 ms for 4,096 cells and 3.6 ms for 65,536 cells,
materializing no cell vector. These are local code-path measurements, not an
end-to-end distributed latency guarantee.

### Parsed SQL-array admission boundary

The envelope validator is shared by owned and borrowed decoding and by
allocation-free validation of an already parsed API value. It reports the same
work and wire-byte admission as materialization, without constructing flat cell
or dimension buffers. Admission results are accounting, not transferable trust
tokens: each consumer still validates its current input.

In-place normalization validates the complete envelope before changing any
cell, then rounds finite float4 values in the existing DOM. Integer decimal
strings, dimensions, lower bounds, SQL NULL flags, JSONB values and nonfinite
float spellings remain unchanged. Preservation mode rejects float4 values that
would round; both failed admission and a late invalid cell leave the input
unchanged. This is the reusable boundary needed for public schema preparation
to make extraction and physical storage agree without extra vectors or JSON
parsing. SQL DDL and SQL scan/mutation activation remain unfinished;
this change does not reclassify any original parity case.

The local debug admission benchmark takes approximately 7 ms for four passes
over 4,096 cells and 126 ms for four passes over 65,536 cells, with no retained
cell vector or allocation in validation. Maximum-cardinality measurement uses
an explicit 8 MiB work budget; the default 1 MiB work limit still rejects this
larger envelope. Cardinality, bytes and work remain independent safeguards.

Verification: 409 embedded and 175 hosted SQL tests, seven targeted native
array/reopen/restore tests, 101 PostgreSQL oracle tests and 20 inventory verifier
tests. The original inventory remains 365 implemented, 136 rejected,
73 superseded and 1,012 unresolved.

### Public typed-array column schemas

Relational root properties can declare `{"type":"sql_array",
"x-antfly-sql-type":"int64","nullable":true}`. All nine builtin element
identities bind to ASCH 17 typed-array columns, never JSON columns. Missing
identity, implicit JSON-array conversion, nested annotations and document-mode
declarations are rejected. Generated `SQLArrayColumnSchema` and `SQLBuiltinType`
contracts are available through public Python, TypeScript and Go SDK exports;
the Rust SDK builds them from its synchronized public specification.

Compiled preparation normalizes the existing parsed envelope before extraction
and physical encoding. Its owned admission path discharges the subsequent
array-domain walk, while ordinary schema constraints still run against the
canonical envelope. No extra flat cell vector or JSON parsing is needed for
schema preparation. Admission is bound in O(1) to the exact immutable parsed
schema owner, version and storage mode; a different compiled plan is rejected
before normalization can mutate input. Public validation and field-local
restore cannot assert
that a value was admitted; restore retains strict physical validation and
canonical float4 verification.

The native public-schema fixture covers batch atomicity, exact bigint strings,
non-default lower bounds, JSONB-null/SQL-NULL element provenance, float4 coercion,
LSM reopen and portable restore with the public schema retained. Schema changes
cannot reinterpret retained array elements. Envelope schema constraints and
exhaustive allocation failures are covered independently. These native storage
and SDK contracts do not activate SQL DDL, SQL execution adapters or array
index keys, and do not change any original parity-case disposition.

Verification: 159 embedded and one hosted native relational-system tests;
409 embedded and 175 hosted SQL tests; 101 PostgreSQL oracle tests;
24 Python and 72 TypeScript SDK tests; Go SDK and generated-client package
tests; TypeScript type checking; Rust SDK compilation; OpenAPI and Python
generation checks. Inventory remains 365 implemented, 136 rejected,
73 superseded and 1,012 unresolved.

> Continued from the `Public typed-array column schemas` section (lines 4395–4706).


### Typed MERGE and shared columnar ownership

MERGE full-scan, identity-point and ordered-index candidates now share a
bounded typed capture with independent replay readers. Classification, lazy
assignment pages and RETURNING retain complete array values and element
identities. Keys, storage images and DELETE preimages become owned only at
their retention boundary; candidate readers close before writer admission.
Source-only inserts carry the native unique-absence fence, and normalization
cannot change absence or conflict guards. Optional strategy binding may decline
an unsupported shape, but cannot swallow cancellation or backend failures.

The remote-lake integration preserves leased columnar result pages alongside
typed replay and borrowed sorted traversal. Resident array leases retain their
cursor without copying payloads; sorted rows cannot retire under a lease or a
sealed replay reader. Typed partial aggregate values use the shared portable
block codec. Plain grouped results retain main's columnar batches, while ordered
aggregates use their bounded collector and invocation constants remain visible
in both paths. Native relation column readers can carry arrays directly rather
than forcing an intermediate per-row adapter.

Partition builds consult live shared-budget headroom so input decode, enclosing
operators and output delivery do not each spend the original statement ceiling.
The unchanged small-budget spill/join workload remains an integration gate.
Allocation-failure, per-checkpoint cancellation, guard tampering, index
saturation/readiness fallback, source-only inserts and resident lease/replay
tests cover these boundaries. This is implementation and component evidence,
not a claim that the remaining original SQL parity cases are complete.

### Stored typed-array reads and text output

Fourteen original cases now have strict native/PostgreSQL evidence. The ledger
contains 379 implemented, 136 rejected, 73 superseded and 998 unresolved cases;
the original 1,586-case inventory is unchanged.

The PostgreSQL oracle has a separate `typed_array_read` profile: ordinary JSON
arrays remain JSONB, while explicitly declared SQL arrays retain element type,
dimensions, non-default lower bounds, whole-value NULLs and element NULLs. The
campaign executes fourteen unchanged original queries against native storage
through HTTP. Scalar fixtures separately cover all nine builtin element domains,
multidimensional string output, JSONB numeric scale and exact bigint values.
Mutation readback for this profile remains guarded until it has an equally
lossless array codec; Python list conversion is not accepted as evidence.

Array overlap uses the shared typed membership index. Either constant operand
can be prepared once; dynamic evaluation indexes the smaller operand. The
10,000-row debug workload completed in approximately 12 ms without hot-loop
allocations. `array_to_string` counts output bytes before retaining its single
exact-sized output allocation. The 32,768-element primitive workload produced
98,303 bytes in approximately 2.8 ms with one allocation. These are local debug
measurements, not production throughput claims.

Column projection and result delivery retain complete Datums rather than their
JSON-null placeholders. Arrays become envelopes only at the public boundary;
fallback wire cells re-enter typed execution using their declared element type.
Leased/native and public-page regressions exercise non-default bounds, NULL
elements, exact bigint values and allocation failures. This does not activate
array DDL, array index keys, or the unfinished broader SQL architecture.

### Joined mutation source-aware RETURNING

Joined UPDATE/DELETE RETURNING now binds against the complete authorized join
scope, sharing wildcard, ambiguity and typed-program binding with MERGE. Its
execution slots are dense: a 128-column unused source tail does not widen a
two-cell RETURNING frame. Only required source cells enter the shared bounded,
spill-capable capture. DELETE preimages likewise retain only required target
fields. Source types remain independent of assignment coercion: copying a
smallint array into a bigint target does not change the source RETURNING type.

Joined selection streams into a target-keyed mutation collector. Match fanout
retains one coherent source representative per target before mutation-row
admission, rather than materializing every joined match or grouping independent
source columns into incompatible representatives. All target images undergo
native preparation, then RETURNING evaluates against the normalized/defaulted/
generated target and captured source. Key, version, digest, absence, conflict and
predicate guards must survive normalization. Typed readers and physical read
owners close before writer admission; errors in RETURNING cannot publish writes.

The existing 1,024-target/source debug workload retains one physical capture and
2,048 native input rows. Its observed peak statement memory decreased from
4,632,337 to 4,432,621 bytes after streaming collection and reclaimable metadata
growth; debug latency remained approximately 120 ms. These local samples are
not a production throughput claim. Component regressions exercise source/target
array domains and NULL provenance, generated/defaulted images, qualified stars,
parameters, 256-fold DELETE fanout under a one-row mutation limit, allocation
faults, cancellation, shared disk capture replay and normalization guard
tampering. Shared unchanged query fixtures are independently verified against
PostgreSQL. Type inference and dependency-pruned rebinding reuse each authorized
source catalog identity, preventing schema-epoch drift within a statement.

UPDATE FROM and DELETE USING select one coherent matched source row per target
before affected-row admission, rather than rejecting PostgreSQL's legal UPDATE
fanout or returning columns from different matches. This does not promise which
matching row is chosen; MERGE retains its independent cardinality contract.
Source-aware RETURNING subqueries use the ordinary relational planner over a
compiler-owned prepared input. Its lexical scope retains the original target
and source qualifiers; dependency analysis prunes that input before capture.
Target fields come from normalized native images and source fields from the
same coherent joined representative, with typed, spill-capable replay. Child
physical scans join the mutation's single statement capture, never a new
post-write snapshot. RETURNING errors and cancellation precede publication.
The shared PostgreSQL oracle verifies nine scalar statements plus typed-array
correlation, including CTE/derived sources, dead branches and empty scalar
results. Native component tests additionally cover every allocation failure and
checkpoint cancellation, normalization guard tampering, generated postimages,
shared parameters, zero-width prepared rows, fanout and disk-backed replay.
With 128 cold source columns, 128/512/1,024 targets read exactly 384/1,536/3,072
physical rows through one capture. Checkpoints were 2,280/8,906/17,739 and peak
statement bytes 4,946,138/7,131,447/8,489,103 in the local debug fixture. These
measure bounded scan/planner work, not production latency or throughput.
The complete SQL build passes 509 local-owner tests and 219 server-owner tests
(three existing benchmark skips), and all 102 PostgreSQL oracle regressions
pass. The pull-stream allocation-fault regression uses the shared stable
allocate/copy/free harness so address-dependent remap decisions cannot change
the enumerated allocation count; injected failures and leak checks remain active.
Original-case evidence is still required: no original corpus disposition is
credited solely for these component fixtures.

### Constraint-equivalent native mutation evidence

Thirteen additional original mutation cases now have mounted native/PostgreSQL
evidence: 426 implemented, 136 rejected, 73 superseded and 951 unresolved, with
the original 1,586-case inventory unchanged. Recursive cases remain unresolved
until their original fixture exercises a nontrivial recursive step.

The native campaign derives logical primary keys from the PostgreSQL profile
through the production DDL builder. Fresh databases bootstrap real enforced
activation; complete owner ranges let admission verify native coverage rather
than accepting a fabricated readiness envelope. Seed and reset writes use the
integrity planner and native atomic transactions, so resetting rows also retires
their unique claims. Duplicate and NULL keys fail with PostgreSQL SQLSTATEs
23505 and 23502, without changing the captured primary rows. Exact-source
mutations verify complete RETURNING results and all three tables' persisted
state. These isolated owner fixtures do not prove distributed read cuts or
multi-owner activation.

The stronger profile exposed a primary-key DDL lifetime bug: inserting required
columns can move the parent schema object's slots. Lowering now borrows the
nested properties map independently, without new allocations. Composite-key
growth and allocation-failure regressions are selected by the SQL test gate.
The complete gate passes 510 local-owner and 219 server-owner tests, with three
existing opt-in benchmark skips; the mounted mutation campaign also passes.

### Durable DDL expression schema identities

Check, expression-index, partial-index and ALTER DEFAULT binding now share a
declared-schema column decoder. It validates catalog structure before access,
handles nullable type unions, and retains builtin scalar widths and SQL-array
element identities. An unrelated typed-array column no longer prevents a
scalar check or expression key from binding. Native schema admission tests
exercise checks, a partial expression index, exact smallint defaults, SQL NULL,
and a cold bigint array with non-default bounds and NULL elements.

Durable numeric programs retain builtin widths on literals, arithmetic and
checked casts, including explicit operand promotions. Narrow integer overflow
and float4 rounding therefore survive schema persistence rather than silently
using int64/float64 semantics. Numeric defaults retain assignment-cast plans:
an out-of-range integer default is accepted at DDL time and raises when used,
without preventing an explicit valid value from being written. These programs
require schema capability 18, including after reopen and portable restoration.
Shared numeric fixtures compare native evaluation with PostgreSQL, using binary
float4 results to avoid shortest-text conversion artifacts.

Array-dependent durable expressions, exact decimal assignment semantics,
temporal operations and special floating-point domains remain architecture
gaps. No original-case dispositions are changed by these component regressions.

Durable conditional programs use an ordered `case_when` node, not eager
evaluation or duplicated boolean rewrites. It evaluates each condition once,
treats UNKNOWN as not TRUE, and evaluates only the selected result (or the
mandatory fallback). SQL lowering supplies a typed NULL fallback when ELSE is
omitted and explicitly promotes numeric CASE and COALESCE results to their
bound common domain. The durable VM admits at most fifteen branches, within
the existing node, depth and per-row byte budgets. Conditional programs also
require schema capability 18, even when their results are nonnumeric.
The shared thirty-two-expression PostgreSQL/native fixture covers searched and
simple CASE, typed NULL fallback, branch-local overflow, unselected division by
zero, mixed-width COALESCE, and float4-to-float8 selection. Fault probes cover
preparation ownership; a 10,000-row local debug sample took approximately 1.97 ms
with a failing allocator proving zero per-row scratch allocations. This is a
component latency sample, not a production throughput claim. Native admission
also exercises conditional checks and expression indexes through atomic batch
failure, LSM reopen and portable restore.

Durable predicate programs now retain bounded `IN`/`NOT IN` membership and
integer remainder. Membership evaluates the probe once, observes candidate
errors even for a NULL probe, and lets an equality witness override earlier
NULL candidates. Numeric operands retain their bound promotions and checked
arithmetic before comparison. Integer remainder preserves the dividend's sign
and returns zero for minInt modulo -1, unlike division overflow. Exact decimal
and floating remainder are not approximated by binary-float durable programs.
Boolean truth tests lower to the existing null-safe comparison operations;
UNKNOWN is never silently treated as FALSE.

Membership and remainder require schema capability 19, including string-only
membership programs without numeric type annotations. Generated public enums
come from OpenAPI. Original inventory dispositions remain unchanged until
exact-source native/PostgreSQL campaigns establish complete case evidence.

The focused `zig build antfly-schema-expression-test` gate executes all twenty
durable declaration tests, including the shared sixty-five-expression fixture
through both query and durable evaluators. PostgreSQL independently checks the
same fixture and the membership/remainder DDL with atomic CHECK failures.
The 165-test relational index system gate covers expression-index publication,
covering projection, LSM reopen, portable restore and capability guards.
A local debug membership/remainder sample evaluated 10,000 rows in about
5.4 ms with a failing allocator proving zero per-row scratch allocations;
this excludes preparation, storage and network work.

Durable text-to-numeric casts and exact decimal operations remain unsupported;
these new probes use exact builtin integer operands rather than claiming those
domains. General partial-index predicates still require richer implication and
durable predicate representation beyond the existing conjunctive contract.

The complete SQL run passed 509 local-owner tests (three opt-in benchmark skips).
Its server binary independently passed all 213 tests. The build invocation still
fails its declared 384 MiB process RSS bound, reporting about 1.04 GB; the
statement admission tests do not establish a bound on the entire test process.
This remains a validation-gate issue, not a green full-build result, and the
process limit has not been raised to hide it. The follow-up below addresses
the diagnostic allocation lifetime behind this observation.

### SQL diagnostic memory ownership

The SQL server owner now uses the same exact-filter runner as extracted source
owners. Anonymous reachability anchors remain compiled rather than treated as
runtime behavior evidence. The complete SQL build passes 509 local-owner tests
and 212 server-owner tests, with three existing opt-in benchmark skips.

Zig 0.17's default debug allocator is a process-lifetime arena. SafeAllocator
and allocation-failure enumeration capture many stacks; temporary DWARF unwind
VM buffers are freed by the unwinder but retained by that arena. The shared
test runner now supplies an independent reclaiming debug allocator: libc when
linked, otherwise the native concurrent allocator or the platform's
single-thread/WASM fallback. Persistent symbol caches remain process-owned;
temporary unwind buffers can be reused. Debug allocation never borrows the
test allocator, whose teardown itself can emit diagnostics. Stack traces,
allocation-failure enumeration and leak checks remain enabled.

A matched local debug run of the unchanged exhaustive grouped-output fault
test reduced maximum RSS from 115,916,800 to 9,748,480 bytes (about 92%).
Elapsed time was approximately 13.5 seconds for both runs, so this is a memory
ownership improvement, not an end-to-end latency claim. Runner regressions
verify that the override is active, repeated stack capture produces frames,
and assertion/leak failures still fail with source diagnostics. Original SQL
inventory dispositions remain unchanged by this infrastructure fix.
The complete 212-test server shard subsequently passed with maximum RSS of
17,252,352 bytes (about 16.5 MiB), below the unchanged 384 MiB build estimate;
its local debug runtime was approximately 123 seconds. All twelve shared
runner selection/progress/diagnostic regressions pass as well.

### Owner-masked conflict defaults

`ON CONFLICT DO UPDATE SET column = DEFAULT` now retains an explicit default
assignment through parsing and binding. The conflict operator omits that field
from the selected replacement image; the shared native preparation pipeline
evaluates its declared default (or implicit SQL NULL) and recomputes generated
columns. It does not substitute NULL for a declared default, eagerly prepare
unselected updates, or reuse the proposed INSERT value. Generated columns accept
DEFAULT but not ordinary assignment. Existing old/excluded expressions retain
simultaneous-assignment semantics.

Dense bound target ordinals replace per-row assignment-name scans in both scalar
and decision-batched conflict execution. Defaults remain behind owner selection
and the DO UPDATE predicate in both paths. Component regressions cover new-row
inserts, false predicates and their fences, generated columns, explicit NULL,
default/provider failures before commit, and exhaustive allocation failures.
A PostgreSQL 18 sequence-backed oracle independently checks evaluation timing,
postimages and error atomicity. These component tests do not credit unrelated
original corpus cases. The expanded mounted mutation campaign has reached the
still-unsupported regexp substring/count case; broader regex and named-constraint
arbiters remain follow-up work, and the corpus dispositions are unchanged.

### Named conflict arbiters

`ON CONFLICT ON CONSTRAINT` now retains the exact folded or quoted constraint
name through the SQL catalog, pgwire authorization wrapper and embedded/public
native adapters. Request-owned catalog descriptors distinguish UNIQUE/primary
constraints from CHECK/FK constraints and unrelated access indexes. The native
integrity planner independently binds the selected name to one durable unique
generation, preserving schema fences, all-owner coverage, staged writes and
guarded owner claims. It does not widen a named target to equivalent or unrelated
unique constraints; final mutation validation still enforces every constraint.

Absent names fail with 42704 and CHECK/FK names with 42809 before proposed row
evaluation. PostgreSQL rejects deferrable arbiters later, after proposed defaults;
the native owner-resolution path preserves that timing and returns 55000 without
publishing writes. PostgreSQL 18 sequence-backed tests verify these distinctions.
Composite named arbitration is exercised through real embedded native storage,
including an equivalent deferrable constraint, a nonselected unique violation,
cold reopen and a raced absence guard. Allocation-fault tests retain exact name
ownership across schema-cache eviction and prohibit partially committed writes.

Mounted original-case probes compare affected counts, RETURNING types/NULLs and
complete three-table native postimages with independently regenerated PostgreSQL
goldens. Regex operations, renamed-constraint DDL, alternate archive layouts and
recursive-arm execution remain separate gaps; generic fixture success does not
prove those original contracts.

Six newly verified original cases (sql-1390, sql-1392, sql-1393, sql-1439,
sql-1463 and sql-1465) bring the inventory to 432 implemented, 136 rejected,
73 superseded and 945 unresolved. The absent-name original sql-1487 remains a
verified rejection, now at catalog binding with 42704 rather than a parser error.

Validation: the complete SQL gate passed 510 local and 226 server tests with
three existing opt-in skips and zero leaks. The selected mounted API gate passed
all three tests, the native embedded gate passed both arbitration/reopen tests,
all 105 PostgreSQL oracle tests passed, and a fresh database reproduced all 48
mutation goldens. Inventory integrity and all 20 checker regressions passed.
The broader in-progress mutation probe still fails at unsupported regex; it is
not included in these green-gate claims or credited as completed coverage.

### Exact NUMERIC kernel foundation

> Heading added on relocation: lines 4886–5262 originally continued under the `Execution-owned PostgreSQL regex scalar activation` heading although they describe the exact NUMERIC kernel, binary codec and ordered keys.

The shared exact NUMERIC foundation now owns canonical base-10000 limbs,
separating numeric identity from display scale without floating-point
intermediates. Parsing, formatting, comparison, canonical hashing, addition,
subtraction, multiplication, rounding and truncation share bounded work,
allocation admission and cancellation. Precision/scale coercion rounds before
checking overflow and supports negative scales and scales exceeding precision.
Exact int2/int4/int8 casts accept asymmetric signed minima and round ties away
from zero; special-value cast errors retain PostgreSQL's SQLSTATE contract.

A reproducible disposable PostgreSQL 18 oracle checks 312 cases, including
values beyond binary64 precision, radix input, scale retention, non-finite
values, coercion carry overflow and integer boundaries. Six additional live
boundary summaries check the full unconstrained digit/scale domain without
checking huge rendered strings into the fixture. Seven native tests include
exhaustive allocation failures, cancellation at every observed checkpoint,
sticky admission failures and clean retries. The focused SQL test gate imports
the same kernel through the local/control source catalogs. The complete kernel
test source also compile-checks for wasm32-wasi; this is not runtime evidence
for that target. Capacity/work arithmetic uses widened integers before bounds
checks so admission cannot overflow on 32-bit hosts.

The debug-build multiplication microbenchmark allocates one output buffer;
64/256/1024 decimal-digit inputs consume 289/4225/66049 work units respectively.
The observed 1024-digit run took approximately 0.53 ms locally, excluding input
parsing. This is not an end-to-end SQL benchmark; dense multiplication remains
quadratic and explicitly work-bounded.

This is a shared-kernel foundation, not activation of the NUMERIC SQL type.
Division/remainder, exact literal binding, generated public type identity,
typed-row/index encoding, wire/spill support and aggregate integration remain
unfinished. No original parity cases are credited for kernel-only evidence:
the ledger remains 448 implemented / 136 rejected / 73 superseded /
929 unresolved.

The next exact NUMERIC increment adds PostgreSQL-compatible division, truncated
integer division and remainder to the shared kernel. A normalized base-10000
long-division loop corrects quotient estimates with bounded reusable scratch;
an exact guard group supplies decimal rounding. Remainder is computed directly,
so a representable result is not rejected because an intermediate quotient
would overflow the stored NUMERIC domain. General remainders retain the
divisor-sized buffer rather than the larger dividend scratch allocation.

One-limb division keeps terminating exponent zeroes implicit; one-limb
remainder reduces virtual zeroes using modular exponentiation. Full-range
1e131071 / 1 and 1e131071 % 7 pass with a 64-unit work budget and a one-limb
allocation limit. The large-exponent remainder modulo 12345 matches PostgreSQL
and retains only two limbs rather than the 32769-limb dividend workspace.

The independent PostgreSQL oracle now reproduces 799 complete result/error
contracts plus ten full-domain summaries and two quotient-overflow checks.
Eleven native tests additionally cover all 9999 one-limb divisors against u128,
quotient-estimate correction/addback, every observed cancellation checkpoint
and exhaustive allocation failures across general, short and remainder paths.
For a 2048-digit numerator and 1024-digit divisor, debug microbenchmarks use
three allocations for integer quotient and two for remainder, independent of
quotient length; observed times are approximately 0.8 ms locally. General
dense division remains quadratic and explicitly work-bounded. Native tests
also compile-check for wasm32-wasi without claiming execution on that target.

Public NUMERIC activation still requires exact literal binding and generated
type identity, typed storage/index/wire/spill contracts and aggregate
integration. The parity ledger is unchanged; kernel-only tests are not credits
for original SQL cases.

The exact NUMERIC binary boundary now validates and streams PostgreSQL's
base-10000 wire representation without decimal formatting or binary64
conversion. A shared indexed-group constructor validates every input group,
including discarded fractional groups, before allocating only the significant
canonical limbs. Decoding applies PostgreSQL's declared-scale truncation before
precision/scale coercion. Short framing reports 08P01; invalid framing, signs,
scales and groups report 22P03. Logical non-finite values remain canonical while
the encoder preserves PostgreSQL's ignored infinity scale-field convention.

A disposable PostgreSQL 18 oracle reproduces 65 sender cases and 47 receiver
cases through actual binary NUMERIC parameters, not a local round-trip model.
Large outputs are checked with length and SHA-256 as well as canonical binary
bytes. Native tests cover exhaustive allocation failures, cancellation at every
observed checkpoint, sticky admission, writer errors and validation before
output. Streaming encode allocates nothing. A 65,535-group receiver retains
one significant limb in one allocation; an all-zero payload allocates nothing.
All sixteen kernel/codec tests pass and compile-check for wasm32-wasi.

This codec is shared integration infrastructure, not a claim that public
NUMERIC parameters, results or columns are activated. Exact literal binding,
generated type identity, typed storage/index/spill, scalar and aggregate
execution, and public transport dispatch still require integration. No parity
ledger credit is taken for kernel/codec-only evidence.

Exact NUMERIC identity keys now encode PostgreSQL total order directly in
lexicographic byte order. Separate ranks cover negative infinity, negative
finite values, zero, positive finite values, positive infinity and NaN. Biased
base-10000 weights and terminated group words order finite values without
decimal expansion; complemented negative payloads reverse magnitude order.
Canonical keys omit display scale, so equivalent values share one unique-key
identity. Components are self-delimiting and prefix-free, allowing multi-column
keys without a length prefix that would change numeric ordering. SQL NULL and
the owning index's type/format identity remain responsibilities of its layout.

Decode rejects noncanonical groups, invalid ranks, missing terminators, domain
overflow and extra bytes before allocating. The shared logical validator is
also used by the PostgreSQL binary encoder. Prefix decode consumes exactly one
component, retains no input ownership and recovers the smallest exact scale;
keys are not a replacement for row values that preserve display metadata.

A fresh PostgreSQL 18 dense-rank oracle supplies 235 independently ordered
values. Native tests check all 55,225 pairs, equivalent scale spellings,
composite-key prefix freedom, byte mutations, every observed cancellation
checkpoint, sticky quotas and exhaustive allocation failures. Streaming encode
allocates nothing; allocated encode and decode each use one buffer. A dense
2,048-digit value uses 1,029 key bytes; full-range powers such as 1e131071 and
1e-16383 retain seven-byte keys and pass within a 64-unit work budget. The debug
2,048-digit encode/decode microbenchmark took approximately 0.09 ms locally,
excluding parsing; this is not an end-to-end index performance measurement.

These keys are shared integration infrastructure, not activation of NUMERIC
indexes or public SQL. Generated type identity, exact literal/scalar binding,
typed row/transport integration and aggregate execution remain unfinished.
The parity ledger is unchanged.

### Exact NUMERIC typed execution and transport integration (2026-10-08)

The shared builtin/array identity now includes NUMERIC with PostgreSQL scalar
OID 1700 and array OID 1231. OpenAPI and generated Go/Python/TypeScript/Zig
contracts carry that identity. Public number columns marked `element_type:
numeric` return decimal strings, preserving precision and display scale; SQL
NULL remains JSON null, and special values use their PostgreSQL text tokens.
This does not activate NUMERIC in native relational schema/index descriptors.

Scalar and array frames, retained typed columns, portable spill blocks,
mapped batches, aggregate extrema partials and result pages retain canonical limbs
rather than interpreting their JSON-null placeholders. Physical array/spill
decoders strictly verify canonical binary representations without re-encoding
into a second buffer; admission errors are not reported as corruption. Exact
scalar casts and arithmetic use the shared kernel. Constant numeric casts are
prepared once and can evaluate without scratch allocations. Small sort keys
encode without allocating; wide values retain the exact comparison fallback.
Primitive vector kernels explicitly fall back to typed scalar execution.

Float-to-NUMERIC conversion uses PostgreSQL's six/fifteen significant decimal
digits and ties-to-even rounding, distinct from NUMERIC's ties-away integer
casts. The bounded IEEE coefficient is expanded exactly before rounding.
Text/binary streaming output avoids per-cell staging buffers. Mixed
NUMERIC/real operator and membership coercions use double precision, while
CASE/COALESCE common domains retain real, matching a fresh PostgreSQL 18 oracle.
The binder records these conversions explicitly rather than guessing from a
JSON payload during comparison.

The 38 focused NUMERIC tests cover exact scalar query results and metadata, array
storage/wire/frame round trips, binary pgwire, JSON/text conversion, immutable
retention, spill ownership, grouped extrema, UNION ALL, bounded output and
exhaustive allocation failures. MIN/MAX preserve complete input type identity.
Python SQL tests pass 25 cases; TypeScript SQL/expression tests pass 52 cases and
SDK typechecking passes; the full TypeScript SDK has 424 passing tests and one
skip. All Go SDK packages pass, including a public exact-decimal transport test.
The SDK expression validator now admits server-defined
CASE, casts, modulo and membership with matching structural arity and numeric
identity checks, rather than rejecting valid generated contracts.

Activation is still incomplete: default decimal/scientific and oversized
integer literals, SQL typmod grammar, exact SUM/AVG accumulators and dedicated
grouped NUMERIC lanes,
mixed-domain join-key normalization, native row/index/catalog integration,
broader numeric functions and native vector lanes need follow-through. No
original inventory case is credited by these infrastructure tests alone.
The ledger remains 448 implemented / 136 rejected / 73 superseded / 929
unresolved, with all 1,586 original source cases intact.

### 2026-10-08: exact NUMERIC SUM/AVG and shared partial reduction

SUM and AVG now use dedicated flat grouped lanes backed by signed i128
base-10000 buckets. Updates defer carries until finalization and allocate only
when their exponent span grows. The bucket bound is 9999 times the non-special
row count; it fits i128 for every legal i64 count. Display scale and NaN and
signed-infinity counts remain separate from finite coefficients. AVG divides
the widened sum before enforcing the public result range, so a partial sum
that would overflow as a final SUM can still cancel or produce a valid AVG.
Final results have an owned stable header and are invalidated after mutation.

The shared aggregate checkpoint is version 2 and binds the exact input element
identity as well as aggregate kind, coarse type and DISTINCT mode. NUMERIC
checkpoints retain bounded signed buckets rather than prematurely finalizing a
decimal sum. Decode validates the entire canonical record before allocating
buckets; decoded state and DISTINCT members never borrow transport bytes.
Spill and worker merges share this codec. Final spilled DISTINCT reducers send
decoded members through the external deduplicator rather than adding a partial
total or retaining a second unbounded membership table.

Exact NUMERIC SUM/AVG states are eligible for pinned scan workers and hash-spill
partition reducers; compensated floating reductions retain their ordered path.
Admission accounts for the complete replacement bucket allocation and the gap
between existing and incoming exponents, including row, worker and checkpoint
handoffs. NUMERIC batch admission currently uses ordered per-row updates to
prove each successive span change before mutation; dedicated flat storage and
allocation-free fixed-span updates remain active. A future vector admission
pass must model combined per-group spans, not merely add input limb sizes.

A live PostgreSQL 18 oracle confirms grouped sums 2996.20/2998.20 and averages
1498.1000000000000000/1499.1000000000000000 for the 2,000-row spill fixture.
Serial and spilled outputs agree, including DISTINCT and exact display scale.
Checkpoint tests cover incomplete records, signature mismatches, decoded
ownership, cached-result invalidation and exhaustive allocation failures.
Kernel tests cover cancellation/quota state preservation, special values and
overflowing sums with representable averages.

The local Debug microbenchmark reduces 10,000 copies of 1.2300 in about
0.6 ms with zero hot allocations and 176 bytes of bucket capacity; repeated
immutable kernel addition takes about 499 ms and 10,000 allocations with the
testing allocator. This measures accumulator allocation/carry overhead, not a
production query speedup or a controlled release benchmark.

Validation: `zig build sql-test pgwire-test check-openapi lake-integration-test`
passes with 567 local SQL tests (three skips), 226 server SQL tests and 161 lake
integration tests, with no failures or leaks. The pgwire/OpenAPI gates and
format/whitespace checks pass. The 46 focused NUMERIC tests pass separately.

Native scalar NUMERIC row/index/catalog descriptors, default decimal literals,
typmod grammar, broader numeric functions, mixed-domain join normalization and
native numeric vector kernels remain incomplete. Unsupported native scalar
NUMERIC descriptors fail closed instead of being stored as floating point.
This infrastructure work does not by itself credit any original inventory case;
the original ledger counts and provenance remain unchanged.

### Exact default literals and PostgreSQL rounding domains

Decimal/scientific literals and integer literals outside signed i64 now retain
their exact source spelling in the owned AST, rather than crossing f64 during
parsing. Binding prepares immutable NUMERIC constants once, preserving display
scale, large integers and public result identity through scalar and set paths.
Small integral literals retain their integer domain. Proven floating-domain
literal coercions, including explicit real/double casts, are resolved once at
bind time so primitive vector kernels do not repeatedly parse decimal text.
This does not convert expressions that belong in the NUMERIC domain: an integer
plus a decimal literal retains exact NUMERIC scalar execution.

NUMERIC abs, ceil, floor, sign, mod, round and trunc use exact typed values.
Two-argument round/trunc resolve the PostgreSQL (numeric, integer) overload,
including negative scales, strict SQL NULLs and unknown string literals; real,
double and explicit bigint scale overloads are rejected. One-argument rounding,
ceiling/floor and sign select double precision for integer/real inputs, as
PostgreSQL does. NUMERIC rounding is half-away-from-zero; double rounding is
ties-to-even. Nested scale evaluation and NUMERIC ANY/ALL share the evaluator's
work budget, including conversion work, instead of borrowing overlapping quotas.
Exact integer array probes use five stack base-10000 groups and allocate no
decimal coefficient. Numeric float parameters use the existing PostgreSQL
significant-digit conversion rather than shortest-string conversion.

Continuous percentile inputs are explicitly converted to double precision;
discrete percentiles/mode keep their source domain and cannot share a narrowed
sort stream with exact integers. Aggregate expression identity now includes
the builtin cast identity, preventing NUMERIC/double reducers from aliasing.

PostgreSQL 18 oracle checks cover rounding ties, negative scales, display scale,
NULLs, special NUMERIC values, double overload identity and SQLSTATE 42883 for
missing overloads. The latest focused gates pass 52 NUMERIC tests and the
ordered-set server regression. Function coercions distinguish unknown strings
from typed text, arrays and dates; explicit user-cast failures keep their own
SQLSTATE. Cast rewrites copy all metadata, including coercion origin. Identical
owned decimal spellings share expression work without merging different scales.
Prepared literal, rounding and exact integer-array tests destroy the parsed AST
and execute 1,000 probes per shape with a zero-capacity allocator. This verifies
ownership and zero hot allocations, not a production latency speedup.

The original inventory remains unchanged: 448 implemented, 136 rejected,
73 superseded and 929 unresolved. Remaining decimal work includes native scalar
catalog/row/index/expression-VM activation, typmods, exact sqrt/power and other numeric
functions, complete assignment/coercion coverage, mixed-domain join keys and
native NUMERIC vector kernels. These tests do not certify those unfinished
boundaries or award new inventory credits.

Native real/double literal defaults parse directly at their declared width and
retain a durable assignment cast; real literals do not round through f64 first.
Native expression lowering explicitly refuses NUMERIC instructions until that
VM has an exact decimal value domain, instead of publishing a lossy program.

Final validation: `zig build sql-test pgwire-test check-openapi lake-integration-test`
passes on the final source, with 573 local SQL tests (three existing skips),
226 server SQL tests and 161 lake integration tests, without failures or leaks.
The 84-test pgwire gate requires permission to bind disposable loopback listeners;
the restricted sandbox otherwise produces EPERM failures in three listener tests.
OpenAPI, formatting, whitespace and original-inventory integrity checks pass.

### Exact square roots and shared scalar cancellation

NUMERIC sqrt now uses an exact integer Newton kernel, not double precision.
The selected PostgreSQL display scale is clamped to 0..1000 with at least
sixteen significant digits and no less than the input scale before clamping.
An extra computed decimal digit proves final half-away rounding. Integer/real
inputs still select the double-precision overload; unknown strings use that
preferred overload, while typed text, boolean and arrays report 42883.
Negative inputs report 2201F, including negative infinity. NUMERIC NaN,
positive infinity, SQL NULL and display scales retain PostgreSQL semantics.

Exponent zeroes remain virtual. Iterations alternate two root buffers and
reset a reusable scratch arena; no per-iteration scratch survives the call.
Compact exact squares retain one coefficient group even near the maximum
exponent. Valid constant roots are statement-owned and need no per-row
allocation. Domain failures are not eagerly adopted into that cache: lazy
CASE/COALESCE branches remain lazy, as verified with PostgreSQL.

The scalar evaluator now shares the backend request cancellation/deadline
callback with ordinary expression execution and all exact numeric contexts,
not just regex operations. Work and output bounds remain independent, and
kernel cancellation preserves its sticky failure and clean retry contract.

The independent PostgreSQL oracle now contains 893 contracts, adding 94
square-root cases without replacing the original 799. Tests cover rounding
boundaries, exponent extremes, randomized 200-digit inputs, allocation-fault
unwinding, every observed cancellation checkpoint, output/work rejection,
clean retry and exact integer bracketing independent of the root algorithm.
Local Debug measurements cover both all-nines and irregular inputs at 64,
256 and 1024 decimal digits. The irregular 1024-digit fixture used roughly
149,000 work units; it is not claimed to fit the default 65,536-step scalar
budget. These microbenchmarks are not production latency comparisons.
The focused ReleaseFast run passes both root tests. Its single-shot 1024-digit
irregular sample took about 274 microseconds with six backing allocations;
the all-nines sample took about 71 microseconds with four. Allocation counts
can vary with arena growth and allocator layout. Neither sample establishes
an end-to-end query speedup or a stable latency bound.

Native scalar catalog/row/index/expression-VM activation, typmods, exact power,
broader numeric functions, assignment/coercion coverage and mixed-domain join
keys remain unfinished. No original inventory disposition is changed here.

Final-source validation passes `zig build sql-test pgwire-test check-openapi
lake-integration-test`: 575 local SQL tests (three existing skips), 226 server
SQL tests and 161 lake integration tests, with no failures or leaks. The
pgwire and OpenAPI gates pass as well. The independent PostgreSQL generator
verifies all 893 oracle contracts; formatting, whitespace and original-case
integrity checks pass. The inventory remains 448 implemented, 136 rejected,
73 superseded and 929 unresolved; the family audit identifies DDL (340 open
contracts) as the largest remaining original-case family.

### Exact NUMERIC native array admission and logical hashing

The common NUMERIC layout boundary now validates borrowed canonical PostgreSQL
binary payloads without allocating coefficients or depending on SQL execution.
It rejects malformed groups, padding, negative zero, hidden fractional digits
and ignored/noncanonical special-value metadata. PostgreSQL parameter input
continues through its deliberately permissive receiver-normalization boundary;
restoration never repairs imported bytes into another physical representation.

Native SQL-array publication/restore validates every NUMERIC payload through
this boundary. Logical array hashes use canonical coefficient identity rather
than the scale-bearing wire header, so equal values such as 1.2 and 1.20 hash
equally while retaining distinct physical bytes and display scales. Shape,
lower bounds, SQL NULL placement and unequal values retain distinct identities.
Owned canonical decoding copies significant coefficients directly rather than
normalizing receiver input and then verifying the normalized result.

Evidence covers all 65 independent PostgreSQL sender fixtures, hash equivalence
with the logical kernel, single-bit mutation agreement with receiver/re-encoding
canonicalization, zero-allocation borrowed checks, allocation faults, sticky
work/cancellation failures, checksum-valid malformed AROW rejection and native
prepared/read/restore hash and byte round trips. A 4,096-element fixture compares
borrowed validation with full owned decoding; these local microbenchmarks are
not end-to-end workload latency measurements.

The native schema identity fixture also explicitly proves scalar NUMERIC cannot
be mislabeled as an f64 column. Native scalar catalog/row/index/expression-VM
activation, typmods, broader functions and mixed-domain join normalization remain
unfinished. Original inventory dispositions are unchanged by this prerequisite.

Validation passes `zig build sql-test pgwire-test check-openapi`: 579 local SQL
tests (three existing skips), 226 server SQL tests, and the pgwire/OpenAPI gates.
The focused native gate passes 12 array/schema/projection contracts. PostgreSQL
18 independently re-verifies all 65 binary senders and 47 receivers. Formatting,
whitespace and original inventory integrity checks pass; dispositions remain
448 implemented, 136 rejected, 73 superseded and 929 unresolved.

Two single-shot ReleaseFast samples of the 98,836-byte, 4,096-element fixture
measured borrowed validation at 24–34 microseconds with zero allocations and
owned decoding at 194–208 microseconds with four backing allocations. These
measure different tasks, not competing complete query plans. The first compile
reported a 3.44 GB peak against a 3.22 GB declared RSS claim; a cached rerun and
both runtime samples pass, but this does not prove cold compilation fits that
resource claim. The final Debug fixture also passes validation and owned decode.

### Shared exact NUMERIC ordered-key boundary

The ordered-key writer and prefix parser now live in a runtime-independent
common layout. Logical SQL coefficients and canonical stored row bytes use the
same ordering implementation. A borrowed stored-value adapter validates input
before output, then streams directly from big-endian coefficients without a
limb copy or intermediate key allocation. Existing ascending bytes are
unchanged. Descending encoding complements the complete self-delimiting
component, including special ranks and terminators. Prefix admission never
interprets the following tuple component and stops at the coefficient budget.

Review also found canonical logical admission did not independently enforce
the request's coefficient cap. An old key quota assertion reused an already
failed context and therefore could not establish that invariant. Canonical
admission now checks the cap before traversal; fresh-context key/binary tests
prove rejection before writer output, with sticky failure and no allocation.

Evidence includes all 65 independent PostgreSQL sender payloads, both ordering
directions, composite suffixes, all 235 independent dense ranks and their
55,225 pairwise comparisons in each direction, malformed payloads, allocation
faults and every observed borrowed cancellation checkpoint. PostgreSQL 18
independently re-verifies all 235 ranks. Inventory dispositions are unchanged.

`zig build sql-numeric-test -Doptimize=ReleaseFast` runs 34 standalone arithmetic,
binary and key contracts without importing the server/runtime graph. Its test
compilation used 526 MB in the local run. For 256 canonical-row-to-key conversions
at 64/256/2,048 decimal digits, borrowed encoding used zero allocations versus
256 coefficient allocations for owned decoding followed by encoding. Local
optimized samples were about 1.3–1.8 times faster for the borrowed codec. These
are codec microbenchmarks, not native-index or complete-query latency claims.
The earlier broad optimized SQL compilation still reported the pre-existing
3.44 GB peak against its 3.22 GB claim; the isolated target does not fix that.

Native scalar NUMERIC schema/catalog capability activation, row-cell identity,
mutation/default/generated/check handling, ordered tuple integration and public
schema/API generation remain unfinished. Unannotated native numeric fields
retain their existing f64 meaning; this shared boundary does not activate or
credit an unsupported exact-NUMERIC schema shape.

Final validation passes `zig build sql-test pgwire-test check-openapi`: 583 local
SQL tests (three existing skips), 226 server SQL tests and the wire/OpenAPI
gates. The focused native schema/array/restore gate passes 12 contracts. Live
PostgreSQL 18 also re-verifies 893 arithmetic contracts, 65 binary senders and
47 receivers. Formatting, whitespace and original inventory integrity checks
pass; dispositions remain 448 implemented, 136 rejected, 73 superseded and
929 unresolved.

### Native exact NUMERIC scalar storage and ordered tuples

Native relational rows now have a distinct `numeric` physical column type bound
to the immutable SQL NUMERIC descriptor. Canonical PostgreSQL binary payloads
remain variable-width ordinal cells, not f64 or per-cell JSON. Schema/catalog
capability 20 gates both scalar NUMERIC and NUMERIC array descriptors; older
capabilities cannot silently adopt them. Unannotated native/document `number`
fields retain their established floating-point semantics.

Prepared rows consume original JSON number lexemes or explicit decimal strings
without an intermediate f64. Canonical row encoding retains display scale and
special values, while semantic hashes use typed logical identity independently
of scale. Trusted projections borrow canonical binary; strict restore validates
all coefficients even when outer AROW framing/checksums are valid. Explicit
SQL NULL and absent cells retain their existing distinct presence metadata.

Composite native index keys use the shared borrowed NUMERIC ordered-key boundary
for row cells and typed bounds. They preserve ASC/DESC and default NULL placement,
delimit document suffixes, and roll back partial tuple output on invalid input.
The native tuple fixture compares all pairs of 235 PostgreSQL dense ranks in
both directions and checks row-built keys against typed bounds. Reserved output
capacity permits tuple construction without allocating coefficient buffers.
The borrowed comparison kernel is also checked against those independent ranks.
Datetime tuple-prefix parsing now consumes its complete 128-bit physical field.

Native preparation matches all 65 PostgreSQL sender payloads byte for byte and
round-trips through logical JSON and restore. Allocation-fault tests cover wide
decimals, specials, NULL and absent values. A cold LSM column projection fixture
retains exact precision, scale and logical hashes across maintenance/reopen with
zero primary-row reads. PostgreSQL 18 independently re-verifies 893 arithmetic
contracts, 65 senders/47 receivers and 235 ordering ranks.

This does not yet activate public native SQL NUMERIC columns. Remaining work
includes generated public schema/API types and normalization, exact typed SQL
read/mutation binding, precision/scale declarations, schema-expression arithmetic
and casts under a shared request work budget, and exact native predicate kernels.
Generic document predicates reject NUMERIC cells rather than comparing binary
as text or rounding through f64. Inventory dispositions are unchanged: the new
physical boundary alone is not evidence that those public SQL cases are complete.

Public NUMERIC arrays now use the canonical generated SQLArrayElementType in
their schema annotation, rather than the narrower scalar SQLBuiltinType enum.
The definition lives in the schema specification and metadata aliases it, keeping
the generated Zig dependency direction acyclic. Python, TypeScript and Go SDK
contracts retain the NUMERIC identity. Native public schema publication, cold
LSM reopen and portable restore retain exact coefficients, scale, dimensions,
NaN and SQL NULL for those arrays.

Validation passes the full SQL, pgwire, OpenAPI and native relational-index gates:
583 local SQL tests (three existing skips), 226 server SQL tests, 171 native local
contracts and one server contract. The additional public NUMERIC-array
reopen/restore contract passes separately. The lake integration gate passes
79 local and 85 server contracts, including datetime tuple-prefix delimiting.
SDK SQL gates pass 34 Python and 35 TypeScript tests; TypeScript typechecking
and all Go SDK package tests also pass. The pinned package-manager test launcher
remained live without starting Vitest; the same pinned Node runtime ran the
installed Vitest entrypoint directly. No parity dispositions changed.

### Exact NUMERIC SQL page projection and mutation images

SQL projection now selects the typed-cell boundary for scalar NUMERIC as well
as arrays. Previously a scalar-only NUMERIC selection could bypass adaptation
or enter generic JSON-number coercion. The native schema-cache mapping retains
the physical NUMERIC column's exact SQL identity. Borrowed native pages share
one page-owned name directory; source lexemes parse directly to owned decimal
coefficients, and an already-rounded f64 is rejected instead of silently accepted.
Unselected ordinary scalar projections retain their existing cheap path.

The owned document decoder also builds typed rows for these selections. It
preserves precision, display scale, missing-versus-present NULL metadata, mixed
JSONB nulls and array bounds; all coefficients and borrowed primitive-array
payloads survive release of the input JSON and source page. Mutation RETURNING
images use that same typed boundary even without an array column present.

Evidence compares all 65 independent PostgreSQL binary sender payloads after
SQL projection, verifies shared page layout, rejects rounded backend cells and
required NULLs, and injects every allocation failure through mixed typed-row
ownership and mutation images. The fault harness disables address-dependent
arena remaps so failure indexes cover deterministic fallback allocations.
This is not public native NUMERIC schema/default/generated/check activation:
those boundaries and a shared schema-expression execution budget remain
unfinished. Original inventory dispositions are unchanged.

Session overlays preserve NUMERIC input before generic number coercion, not
only at the final projection boundary. Exact filter operands bind once per
cursor; their immutable coefficients and every staged-row comparison share
one bounded work context. Probes compare typed logical values, never a NUMERIC
datum's JSON-null placeholder. A scalar-only staged-row regression checks exact
equality across different display scales and retained values after cursor close.
A second fixture probes prepared bounds 1,000 times with no allocation available,
then verifies work exhaustion and rejects a pre-rounded f64 input.
The existing native API owner runs schema-cache and session-overlay contracts;
they are intentionally not pulled into the storage-independent SQL test root.

Validation passes 586 local SQL tests (three existing skips), 226 server SQL
tests, pgwire/OpenAPI gates, and all 12 focused native API schema-cache and
session-overlay contracts, with no failures or leaks. Formatting, whitespace,
generated control-catalog consistency and original inventory integrity checks
pass. The inventory remains 448 implemented, 136 rejected, 73 superseded and
929 unresolved; these internal boundaries do not independently credit public
NUMERIC schema/DDL cases.

### Exact NUMERIC schema execution and reader capability

The immutable schema VM now retains canonical NUMERIC bytes through literals,
column reads, arithmetic, comparisons, membership, lazy conditionals, casts and
generated/default bindings. Exact operations use the existing coefficient
kernel, not f64 or JSON-number conversion. Comparisons borrow binary limbs;
identity casts borrow their input, and negation copies only the canonical
binary payload while retaining display scale. Generated-value restore checks
compare logical NUMERIC values without filling missing outputs or repairing
forged values.

An execution context carries sticky work/cancellation admission across plans.
Numeric temporary arenas are capacity-bounded and charged monotonically along
with retained outputs, including when the caller itself uses an arena. CHECK
expression sets share this context rather than resetting numeric work at each
constraint. Deterministic kernel failures map to the established durable
validation errors, preserving activation diagnostics and transport SQLSTATEs;
cancellation and allocator failures remain distinguishable. PostgreSQL's
nonfinite-to-integer rejection retains SQLSTATE 0A000 through append-only
runtime/storage ABI identities, definite replicated-apply outcomes, C API
unsupported status and remote SQL mutation diagnostics.

Schema format/capability 21 records exact-NUMERIC expression requirements even
when all stored columns and expression outputs are integral or boolean. Public
raw generated/default declarations derive this requirement recursively; the
transactional catalog rejects an older reader capability. Strict framing
validates the added flag and truncated/corrupted records.

The native oracle gate covers 537 PostgreSQL cases: 430 arithmetic/remainder,
72 checked integer casts and 35 ordering cases (also run without an available
allocator). Allocation-fault fixtures cover preparation, scratch ownership,
default/generated dependency ordering, explicit NULL, logically equivalent
display scales and forged/missing restore values. A schema-validator generated
column fixture verifies exact rounding above 2^53 and durable capability
publication.

Public scalar NUMERIC schema annotations and generated expression enums, SQL
lowering/DDL activation, typmods and complete nonfinite native float casts
remain unfinished. The new schema
context does not yet unify defaults, generated expressions and every CHECK
form under one complete request-level quota. No original inventory case is
credited solely for this native execution infrastructure.

Validation: the combined SQL/schema-expression/archive-ABI gate exits zero,
with 586 local SQL tests (three existing skips), 226 server SQL tests, 24
schema-expression tests and seven archive-boundary tests, without failures or
leaks. The native relational gate passes 174 local and one server-owner test;
the focused C API status regression passes. PostgreSQL independently rechecks
all 893 kernel reference contracts. Inventory integrity, formatting,
whitespace and dependency-catalog consistency pass, with 929 cases unresolved.

The broad durable-runtime target also exposes a 14.46 GB compiler peak against
its 13.96 GB reservation and duplicate ownership of a guarded graph replay
raft-batch test. Its compile-time-filtered replicated-apply regression passes
with the new semantic error included in the expected-failure roundtrip audit;
this does not establish that the broad runtime target is green.

### Exact NUMERIC SQL schema-expression lowering

SQL schema expressions now lower exact NUMERIC literals, arithmetic, remainder,
negation, comparisons, CASE, COALESCE, membership and numeric assignment casts
into the bounded native VM. Durable column nodes explicitly record conversions
when scalar binding changes their inferred domain; an integer column is never
merely relabeled as NUMERIC. Unknown numeric input preserves PostgreSQL input
SQLSTATEs before schema validation. Typed NULLs do not choose a floating domain
for a decimal comparison or membership list.

Mixed operator comparisons use float8 when required, including precision-sensitive
integer/real and NUMERIC/real cases; CASE/COALESCE retain their separate real
common-type rules. Same-domain comparisons avoid unnecessary casts. Simple
partial-index column/literal predicates survive lowering, including checked
casts of integer/float literals. Exact NUMERIC index predicate activation remains
guarded rather than converting decimal input through binary float.

Decimal defaults retain their exact source plus their declared assignment cast.
Wide integral rounding and narrowing overflow therefore happen during mutations,
not by prematurely rounding a default through f64. Generated/default programs
using this domain require the previously introduced reader capability 21.
Native publication, omitted-value application and strict verification cover a
wide half-integer default and generated bigint, a real default, and deferred
smallint overflow.

The existing 537 PostgreSQL arithmetic/cast/order contracts now run through both
raw native programs and SQL parsing/binding/lowering. A separate independently
reproducible PostgreSQL 18 fixture covers 62 mixed-row, NULL, special-value,
conditional, membership, cast and input-error contracts. Exhaustive allocation
faults cover SQL preparation, independently owned plan literals, scratch,
successful output and failing execution. A 10,000-row exact comparison sample
requires zero scratch allocations; its Debug local sample is about 3 ms, not a
claim about deployed query latency.

This connects exact NUMERIC SQL expressions to native schema execution; it does
not complete public scalar NUMERIC activation. Public schema/generated expression
enums, typmods, exact schema constraints/index activation, dynamic text casts,
non-finite floating domains and complete shared statement/schema work accounting
remain unfinished. No original
parity case is credited solely for these infrastructure tests; 929 remain
unresolved pending source-owned execution/storage evidence.

Validation passes: 587 local SQL tests (three existing skips), 226 server SQL
tests and 27 schema-expression tests, with no failures or leaks. The independent
62-case PostgreSQL oracle, inventory integrity, Zig formatting, whitespace and
control-catalog consistency checks pass. These gates do not supersede the broad
durable-runtime compiler/ownership failure documented above.

### Immutable exact NUMERIC constraint compilation

The next public-NUMERIC prerequisite is an immutable constraint plan, rather
than converting minimum/maximum/exclusive bounds and multipleOf through f64.
The new internal component owns exact literals in a stable-address, 4 MiB
schema arena. Const and enum compare logical numeric values; enum candidates
are compiled into collision-checked hash buckets, deduplicating equivalent
scales without reparsing JSON during row checks. Finite JSON strings remain
strings, not numeric enum members. PostgreSQL's NUMERIC ordering governs
special values.

A request-owned execution arena reuses bounded scratch across rows, while
preserving sticky work, cancellation and quota failures. Exact remainder checks
use the same numeric kernel. Immutable borrowed bounds/enum comparisons need
no allocations. Repeated tiny-decimal remainder checks retain constant scratch
capacity, and a 10,000-probe comparison regression uses an allocator that rejects
all allocations. Exhaustive allocation failures cover plan compilation, source
JSON destruction, row parsing, and exact remainder evaluation.

The independent disposable PostgreSQL 18 oracle verifies 70 constraint
predicates, including precision above 2^53, bounds outside binary-float range,
scale-equivalent values, special values, and nonnumeric const/enum members.
This component is deliberately not advertised as public NUMERIC activation:
wiring it into schema epochs, composition/type validation, a durable reader
capability, generated public contracts, and end-to-end storage/restore tests
remains required. Existing document/physical-float constraints are unchanged.
No original parity cases are reclassified for this prerequisite.

Validation passes: 587 local SQL tests (three existing skips), 226 server SQL
tests, and 30 schema-expression tests, without failures or leaks. The 70-case
PostgreSQL fixture, inventory integrity, Zig formatting, whitespace, and
control-catalog consistency checks pass. The broad durable-runtime limitation
documented above is not superseded by these focused gates.

### Exact NUMERIC schema validation and durable publication barrier

The immutable constraint plans now belong to parsed schema properties and are
released with their schema epoch, including failed compilation. Recursive row
validation uses one bounded exact execution owner, parses each NUMERIC cell once
across compositions, and restores the active value scope between properties.
Bounds, const, enum and multipleOf run against the typed logical value rather
than f64 or reparsed JSON. Composition type checks distinguish integral NUMERIC
from fractional/special NUMERIC, and never reinterpret a special numeric as a
string. anyOf/oneOf/not/conditional probes preserve allocation, work and
cancellation failures instead of treating them as a nonmatching branch.

The 70 PostgreSQL constraint contracts now exercise the actual field validator
as well as the immutable component. Independent source destruction, exhaustive
allocation failures, exact tiny-decimal composition, sticky cancellation, and a
parse-once work-accounting regression cover the validator boundary. Exact
definition positivity also validates unused definitions without allowing f64
underflow to reject a positive multipleOf or conceal a negative one.

Runtime schema format 22 records a separate exact-NUMERIC-validation capability.
Physical NUMERIC codecs (20) and schema VM programs (21) do not imply support
for public scalar constraint semantics. Full and reduced runtime layouts derive
the new capability from scalar NUMERIC columns. Serialization rejects downgrade
to 21, strict decoding checks the new boolean and tail framing, and transactional
catalog binding rejects a reader/catalog that lacks this capability. The flag
requires a relational public schema with a scalar NUMERIC column; internal
NUMERIC layouts without public validation retain their older semantics.

Public scalar and expression enums remain guarded. Generated contracts,
ingress lexeme preservation, complete indexing admission, and end-to-end SQL
mutation/reopen/backup-restore validation still need activation work before
public NUMERIC can be declared complete. Existing document and physical-float
rules remain unchanged, and no original parity dispositions are reclassified.

Validation passes: 587 local SQL tests (three existing skips), 226 server SQL
tests, 35 schema-expression tests and 177 native relational-owner tests,
without failures or leaks. The 70-case PostgreSQL oracle, inventory integrity,
Zig formatting, whitespace and control-catalog consistency checks pass. The
previously documented broad durable-runtime compiler/ownership limitation
remains separate and is not claimed fixed by these gates.

### Public exact NUMERIC contracts and durable index activation

Public scalar SQL builtin identities and relational expression types now expose
NUMERIC in the source OpenAPI contract and regenerated Zig, Go, Python and
TypeScript clients. Relational root number properties accept the numeric SQL
annotation without changing document or unannotated floating-point semantics.
Finite input retains its exact JSON lexeme or decimal string; NaN and infinities
use strings. Const/enum finite numeric members must remain JSON numbers, not
numeric-looking strings. Typed mutation ingress has an owned-row regression for
precision above 2^53, retained scale, extreme small exponents and special values.

CHECK, expression-index and unique-expression declarations now derive the exact
schema-VM capability independently of their output type. A public integer-only
table with a NUMERIC CHECK still fences pre-capability-21 readers, while scalar
NUMERIC validation independently requires capability 22. Cold-row CHECK
evaluation borrows canonical NUMERIC coefficients. SQL expression-index DDL
retains the exact key domain instead of mislabeling it as binary float. Partial
index literal identity casts retain their exact lexemes; other literal casts
remain guarded pending bounded, PostgreSQL-compatible constant folding.

End-to-end LSM tests publish actual public schemas and SQL-generated defaults,
generated columns, CHECKs and partial covering expression indexes. Mixed invalid
batches publish neither earlier valid rows nor invalid rows. Reopen and portable
restore preserve both capability barriers, exact scale and constraints. Covered
NUMERIC reads explicitly supply the partial-index proof and require zero primary
lookups; the proof guard is not bypassed for the test.

The relational public-API test owner now imports fixtures unconditionally rather
than from a named smoke test excluded by its own compiler filters. This restores
the three FK publication/initial-create tests required by that target's ownership
audit; no filters or audits were removed.

This activates public scalar storage and schema-expression boundaries, not all
remaining SQL NUMERIC work. Public column typmods, non-finite floating-domain
casts, dynamic text casts, broader partial-index constant folding and shared
statement/reducer/schema work admission remain unfinished. Parse-once evidence
applies within validation/composition, not the complete write pipeline. Original
inventory dispositions remain unchanged; infrastructure tests do not establish
the mounted behavior of an unadjudicated source case.

Validation: 587 local SQL tests (three skips), 226 server SQL tests, 37 schema
expression tests, the full public relational-row API owner, and native NUMERIC
storage/index/reopen/restore regressions pass without failures or leaks. The
70-case disposable PostgreSQL constraint oracle, Python SQL tests (35),
TypeScript SQL tests (36), focused Go SDK tests, generated contracts, inventory
integrity, formatting, whitespace and control-catalog consistency are checked.
The broad durable-runtime compiler/ownership limitation remains separate.

### Bounded exact constant bounds for partial relational indexes

Partial-index DDL now evaluates row-independent bounds through the same native
schema-expression compiler and evaluator used by CHECK/default/generated
programs. The previous cast-only literal interpreter is removed. Admitted
constants include exact NUMERIC arithmetic, nested casts, conditional/coalesce
expressions, and the VM's scalar text/boolean vocabulary. Explicit typed NULL
casts retain their target domain instead of inheriting the unknown input type.

One folding scope spans all bounds of a predicate. It limits actual temporary
arena capacity, charges compilation and evaluation work, owns returned scalar
bytes, and preserves sticky quota/cancellation failure. Temporary compiled
plans never escape; only folded values enter the published predicate. This is
DDL preparation, not another per-row or per-query interpretation step. It is
not yet general shared admission for every SQL reducer and schema operation.
The native folding owner supports an injected cancellation checkpoint; the DDL
entry point currently supplies work/byte admission but does not yet forward a
request-wide checkpoint. That transport remains part of the shared-admission
work rather than being claimed complete by the component cancellation test.

The empty compilation environment rejects column references even in lazy arms.
Comparison promotions on the indexed column are not erased to manufacture a
sargable predicate. OR/row-dependent/unsupported-expression cases retain their
guards; broader implication and functional-index matching remain separate work.
No storage-format change or weakening of the partial-index proof is required.

A disposable PostgreSQL 18 oracle verifies 44 bounds across exact NUMERIC,
integer widths, real/double, text, boolean, NULL, special values and SQLSTATEs.
The schema VM has allocation-fault, owned-output, cumulative work, byte-admission
and cancellation regressions. The LSM covering-index/reopen/portable-restore
regression now builds its partial bound from nested numeric/integer casts and
arithmetic, still requiring zero primary lookups and an explicit query proof.
Original parity dispositions are unchanged; these infrastructure contracts do
not reclassify original cases without mounted source-owned execution evidence.

Validation: 589 local SQL tests (three skips), 226 server SQL tests and the full
schema-expression owner (38 local and five server tests), and 179 native
relational-owner tests pass without failures or leaks. The independent 44-case
PostgreSQL oracle, inventory integrity,
formatting, whitespace and control-catalog consistency checks pass.

### NUMERIC precision/scale execution boundary

An immutable dependency-neutral modifier now carries precision and signed scale
through parsed scalar/array casts and bound instructions. The existing exact
kernel performs half-away rounding followed by precision overflow checking;
negative scales and scales greater than precision follow PostgreSQL. Array
coercion preserves bounds, dimensions and NULL elements without mutating inputs.
Valid literal results remain cached. Speculative constant preparation defers
modifier overflow in unreachable CASE branches without swallowing invalid input
syntax or unrelated failures. Aggregate expression identity includes modifiers.

One evaluator budget covers array traversal, exact coercion and output ownership;
even large all-NULL arrays poll cancellation. Dynamic array execution is covered
by exhaustive allocation failures and work/byte limit regressions. A disposable
PostgreSQL 18 oracle independently records 46 values, errors, type OIDs and wire
modifiers. The execution regression compares values, SQLSTATEs, builtin identity
and direct-cast modifier encoding; it does not yet assert public/pgwire metadata.
The shared lexer also accepts strictly separated decimal digits in integer,
fractional and exponent parts, preserving source spelling and malformed-token
rejection. NUMERIC type modifier range failures have SQLSTATE 22023, not XX000.

This is an execution prerequisite, not complete modifier activation. Generated
public descriptors, pgwire result descriptors, durable column enforcement and
schema-expression reader capability fencing remain unfinished. DDL and durable
expression publication explicitly reject modifiers until those contracts can
preserve and enforce them; parsing never silently publishes an unconstrained
column or drops quantization from a durable program. Original inventory
dispositions remain unchanged at 929 unresolved cases.

Validation: `zig build sql-test antfly-schema-expression-test lib-sql-parser-test`
passes 593 local SQL tests (three existing skips), 226 server SQL tests and all
43 schema-expression tests. A final six-test focused rerun covers the subsequent
zero-allocation constant reuse and aggregate modifier-identity regressions. Both
scalar and array constants are evaluated 10,000 times with a zero-capacity row
allocator and reuse the same owned result; this is an allocation/work contract,
not an end-to-end latency benchmark. The independent 46-case PostgreSQL oracle,
inventory integrity, control-catalog consistency, formatting and whitespace
checks pass without changing original case dispositions.

### NUMERIC result identity across SQL, public clients and pgwire

Result descriptors now carry an optional immutable precision/signed-scale pair
for scalar NUMERIC and NUMERIC array elements. Catalog, scalar, aggregate,
window, derived-table, CTE, VALUES, set, mutation and RETURNING bindings preserve
that identity. PostgreSQL common-type rules retain a modifier only when every
contributing expression has the same modifier; unknown NULL arms, arithmetic,
unconstrained casts and ordinary numeric function outputs remove it. NULLIF
preserves its first operand's modifier only when comparison coercion has not
changed the result to a floating-point domain.

The public OpenAPI contract owns SQLNumericModifier, with generated Go, Python,
TypeScript and Zig descriptors. Pgwire emits PostgreSQL's modifier encoding for
scalar and array RowDescription fields and fences prepared/cursor result identity
when precision, scale or modifier presence changes. No per-row schema lookup or
heap owner is added: the descriptor contains two bounded integer values.

The independent disposable PostgreSQL 18 oracle now verifies 56 scalar cases
and 12 query descriptors, including prepared versus optimized result metadata,
negative scales, mixed NULL arms, derived scopes and mixed floating-point NULLIF.
Public client round trips and simple/extended wire-frame tests cover the new
contract. This completes result metadata, not durable modifier activation:
DDL columns and schema-expression publication remain guarded until assignment,
storage/restore validation and reader-capability fencing can enforce modifiers.
The original parity dispositions remain unchanged at 929 unresolved cases.

TypeScript's structural expression validator also admits the already activated
exact NUMERIC literal/cast/arithmetic contract, retains incompatible-domain
rejection, and charges decimal-string bytes against the shared literal budget.
The SDK typecheck and 61 SQL/expression tests pass; Python's 36 SQL tests, Go's
focused SQL transport/descriptor tests and generated-client consistency pass.

Final validation: `zig build sql-test pgwire-test check-openapi` passes 595 local
SQL tests (three existing skips), 226 server SQL tests and the wire/OpenAPI gates.
The focused API owner executes the generated NUMERIC descriptor regression.
The full TypeScript SDK suite passes 433 tests with one skip. Inventory integrity,
control-catalog consistency, Rust-spec synchronization, formatting and whitespace
checks pass. No original unsupported cases were reclassified by these checks.

### Strict NUMERIC modifier storage boundary

The shared exact-row boundary now separates caller-budgeted write coercion from
strict stored-byte verification. Writes parse, round and precision-check through
one execution budget before canonical encoding. Verification borrows canonical
limbs and checks precision, declared display scale and negative-scale divisibility
without allocating, formatting or rounding. A valid unconstrained encoding is
not automatically a valid constrained stored value; restore cannot silently
repair a value that violates its immutable column layout. NaN follows PostgreSQL,
while constrained infinities are rejected. Sticky cancellation/quota failures
remain authoritative even when a later call supplies an invalid modifier.

All 20 modifier cases in the independently reverified 893-case PostgreSQL exact
NUMERIC oracle exercise this boundary. Tests also cover canonical-but-unconstrained
payload rejection, every write/owned-output allocation failure and zero-capacity
verification. All 74 NUMERIC owner tests pass without failures or leaks. A Debug
probe verifies 10,000 rows with zero allocations in approximately 1.3 ms; this is
a component work/allocation measurement, not an end-to-end benchmark.

Durable activation remains incomplete: schema-owned modifier descriptors, public
schema annotation and DDL publication, assignment/default/generated enforcement,
schema-VM modifier instructions, restore integration and capability fencing must
all use this boundary before the publication guards can be removed. The original
parity dispositions remain unchanged.

### Immutable NUMERIC modifier layouts and strict native row admission

Durable schema format 23 retains precision and signed scale for scalar and array
NUMERIC columns, with a separate capability flag covering future modifier-bearing
expression programs even when their output columns are not NUMERIC. Older readers
are fenced; truncated, invalid or unfenced layouts are rejected before allocation.
Ordinary schema updates and historical row projections cannot reinterpret a column
under a different modifier. Cover fingerprints include modifier identity without
changing logical NUMERIC equality keys or prohibiting cross-modifier foreign keys.

Native row admission checks constrained scalar values and every non-NULL array
element. Untrusted array offsets are validated before element access; authenticated
projections retain their bounded shape-only path. Scalar write encoding and semantic
hashing apply the same assignment coercion. Full-text projection witnesses exclude
SQL-only reader capability flags, preserving their independent physical identity.

Validation passes 184 native relational-index tests, one server integration test,
598 local SQL tests (three existing skips), and 226 server SQL tests without failures
or leaks. Inventory integrity remains 929 unresolved. Public modifier activation
is still guarded: normalized postimages, defaults/generated evaluation order,
array assignment, public annotation, and schema-VM casts remain to be completed.

### NUMERIC array assignment before canonical row encoding and hashing

Array preparation now shares parsing and modifier coercion under one work budget
and a bounded unpublished owner. It does not alter the caller's envelope on success
or failure. Dimensions, signed lower bounds and SQL NULL flags remain authoritative;
each non-NULL value is rounded and precision-checked before canonical row encoding.
Logical JSON hashing uses the same constrained array boundary as prepared row hashes.
No physical binary is interpreted as JSON or silently repaired during strict restore.

All 20 PostgreSQL modifier expectations also run through multidimensional arrays
with NULL elements and signed bounds. Tests cover exact work exhaustion, memory
admission, every allocation failure, overflow after rounding, strict native row
validation and canonical reconstruction/re-encoding. The independent PostgreSQL 18
oracle reverified all 893 exact NUMERIC contracts. These component tests are not
end-to-end performance measurements or public durable modifier activation.

The remaining activation work is unchanged apart from array assignment: normalized
postimages, target-domain coercion before dependent defaults/generated expressions,
public schema/DDL annotation, modifier-bearing schema-VM instructions, and integrated
restore/reopen publication coverage must land before removing publication guards.
No original parity case dispositions were changed.

Final validation passes 600 local SQL tests (three existing skips), 226 server SQL
tests, 185 native relational-index tests and their server integration owner without
failures or leaks. The focused native array regression and six API schema-cache tests
also pass. Inventory integrity, control-catalog consistency, formatting and whitespace
checks remain green.

### NUMERIC assignment domains before defaults and generated dependencies

The native expression set now coerces constrained base inputs before dependent
expressions run, and constrains each default/generated result before it becomes
another expression's input. One caller-owned execution carries sticky work,
cancellation and retained/scratch-byte admission across every conversion. An
immutable list of constrained dependency ordinals avoids introducing a schema-width
scan on unconstrained expression evaluation. Already constrained canonical values
are reused without coefficient or output allocation.

Logical restore accepts equivalent display scales but rejects values that would
change under assignment. It never repairs the stored input. Physical restore keeps
the stricter canonical scale/precision boundary. Cold generated verification fences
modifier identity before reading a mismatched historical cell. Generated-plan
fingerprints bind target and referenced-column modifiers, while unrelated column
changes and old unconstrained programs retain their existing identities.

The new independent PostgreSQL 18 oracle verifies 11 real-table assignments,
including omitted defaults, explicit NULL, positive/negative rounding, base and
generated-target overflow, NaN and constrained infinities. PostgreSQL forbids
generated-on-generated declarations; its observer uses equivalent explicit nested
casts to check the native dependency topology, not to credit additional SQL syntax.
Native tests also cover logical-restore forgery, every allocation failure, source
identity fences, sticky cancellation/quota and 10,000 zero-allocation canonical
binding reuses (approximately 1.4 ms in Debug, not an end-to-end latency benchmark).

Public modifier activation remains guarded. The remaining prerequisites are
normalized postimages across all mapped fields, generated public schema/DDL
annotations, modifier-bearing schema-VM casts and integrated reopen/restore
publication coverage. Original parity dispositions remain unchanged.

Final-source validation passes all 46 schema-expression tests, 600 local SQL tests
(three existing skips), 226 server SQL tests, 185 native relational-index tests and
their server integration owner without failures or leaks. All four PostgreSQL value,
modifier, descriptor and assignment observers reverify successfully. Inventory
integrity remains 929 unresolved; control-catalog, formatting and whitespace checks
pass without changing original case classifications.

### Public typed array declarations and SQL DDL binding

Array expression literals now use the generated public `sql_array` enum and
require their exact builtin element identity, including typed NULLs. OpenAPI,
Go, Python, Rust, TypeScript and Zig contracts are synchronized. Public server
prechecks share the typed VM's field and arity grammar, admitting CASE and IN
without relaxing typed compilation. TypeScript bounds ordinal envelopes and
counts wire bytes without allocating a serialized copy; generated SDK round-trip
tests preserve exact integer strings, lower bounds and SQL NULL flags.

Direct CHECKs resolve columns against the pinned physical layout rather than
constructing ordered index keys. This admits logical array comparisons while
keeping array-valued ordered keys guarded. SQL CREATE/ALTER column declarations,
typed NULL defaults, same-identity array column comparisons, CASE, COALESCE and
IN lower through the existing query binder into the durable VM. Unknown NULLs
acquire their element identity from the consumer, not from their value. Generated
NUMERIC arrays retain per-element precision/signed-scale assignment coercion.

Independent PostgreSQL verification covers 36 ordering fixtures across all ten
element domains. Public defaults and direct CHECKs exercise those fixtures on
JSON and cold ordinal rows; SQL DDL tests compile generated NUMERIC arrays into
the public schema validator. Array constructors, element-changing durable casts,
non-NULL SQL array defaults and array-valued ordered keys remain unfinished and
explicitly guarded. Original inventory dispositions are unchanged; this is not
a claim of complete array or SQL parity.

Validation passes 74 local and six server schema-expression tests, 621 local
SQL tests (three existing skips) and 226 server SQL tests, without failures or
leaks. SDK checks pass: Go packages, 291 Python tests, 16 Rust unit tests plus
two integration tests, TypeScript typechecking and 478 tests (one existing skip).
The PostgreSQL ordering oracle and generated OpenAPI/Python checks pass.
Inventory integrity, control-catalog, formatting and whitespace checks pass;
all 929 unresolved original cases retain their dispositions.

### Durable NUMERIC modifier expressions and generated wire contracts

Durable scalar casts now carry validated NUMERIC precision/signed-scale
modifiers in the immutable VM node and semantic fingerprint. Assignment uses
the existing shared execution budget and canonical constrained-byte fast path;
rounding allocates bounded unpublished scratch only when needed. Overflow is
reported when a selected cast executes, not while compiling an unselected lazy
branch. Nested casts retain independent modifiers rather than overwriting the
inner coercion. NULL propagation is unchanged.

SQL lowering preserves the modifier on numeric, integer/float-to-numeric and
typed NULL casts. PostgreSQL-valid numeric lexemes such as `.00994` are carried
as validated exact decimal strings in the public literal contract instead of
being serialized as invalid raw JSON numbers. This does not round through f64.

Capability derivation traverses defaults, generated columns, CHECKs, index keys
and UNIQUE keys for modifier-bearing programs. Both full runtime and reduced
CHECK layouts publish capability 23 even with integer/boolean final results;
the durable schema round-trip retains the requirement. Existing catalog reader
fences continue to reject older capability versions.

The OpenAPI expression contract owns the optional modifier, and Zig, Go, Python,
TypeScript and Rust specification artifacts are regenerated. TypeScript local
admission rejects wrong targets, missing/extra fields, fractional precision or
scale, and out-of-range values before transport. SDK round-trips retain signed
scale and recursive argument ordering.

The durable VM matches all 42 currently lowerable scalar cases from the existing
56-case PostgreSQL modifier observer, including negative scale, scale greater
than precision, nested rounding, float4 conversion, errors, NULL and lazy CASE.
Arrays and broader functions still lacking durable VM support remain guarded;
their SQL-runtime coverage is not claimed as durable-expression activation.
The observer also reverified its 12 query descriptors, and the independent
893-case exact NUMERIC observer reverified successfully.

Native tests cover modifier identity, malformed contracts, every allocation
failure, sticky work/cancellation and 10,000 zero-allocation constrained cast
reuses (about 2.3 ms in Debug, not an end-to-end latency benchmark). The existing
CHECK fault sweep now disables address-dependent arena remaps so every allocation
failure index is deterministic rather than intermittently missed.

Public column-modifier activation remains guarded pending schema annotations,
normalization of all mapped postimage fields under a shared preparation budget,
and integrated reopen/restore publication coverage. Original parity inventory
dispositions remain 448 implemented, 136 rejected, 73 superseded and 929
unresolved; this infrastructure change does not inflate case credit.

Final-source validation passes 50 durable-expression tests, 601 local SQL tests
(three existing skips), 226 server SQL tests, 185 native relational-index tests
and their server integration owner, without failures or leaks. SDK validation
passes 450 TypeScript tests (one existing skip), typecheck, 39 Python SQL tests,
focused Go SQL/relational transport tests, and all 17 Rust SDK unit/integration
tests. Rust validation also exposed and corrected a stale aggregate-recipe
default initializer so the current generated API builds. OpenAPI/Python/Rust
generation checks, control-catalog, inventory, formatting and whitespace checks
pass. The work is committed locally, not pushed.

### Shared SQL postimage preparation and reusable NUMERIC array scratch

Both ordinary and typed-row preparation now carry one expression execution
context through base SQL normalization, default/generated evaluation and derived
normalization. Array metadata admission and NUMERIC cells consume that same
sticky work budget. Expression staging vectors, owned outputs and UUID rewrites
also consume the preparation byte allowance. The post-expression pass visits
only default/generated SQL columns, avoiding a second validation of unrelated
base arrays. Submitted generated fields remain output-only and are ignored by
the pre-expression pass.

NUMERIC JSON assignment supports exact scalar and array precision/signed-scale
normalization without floating point. Array preparation reuses one bounded
scratch arena across cells and publishes replacement values only after every
cell succeeds. Dimensions, signed lower bounds and SQL NULL flags survive
unchanged. Preservation mode rejects values that assignment would change and
never rewrites the input; physical restore still has its stricter canonical-byte
codec checks. Unconstrained public arrays are validated without rewriting their
lexemes. This does not admit finite numeric-looking strings as scalar API numbers.

Tests reuse all 20 PostgreSQL modifier oracle cases for scalar and array JSON
assignment, sweep success/late-overflow allocation failures, check sticky
work/cancellation across scalar/array boundaries, and prove row admission cannot
reset its work quota per array. A Debug validation of 10,000 constrained array
cells uses 70 bytes of scratch, 550,154 work units and about 6.7 ms on this
machine. This bounds scratch by the largest cell; it is not an end-to-end query
benchmark and excludes the already-parsed request DOM. All 54 durable-expression
tests pass without failures or leaks, and the live PostgreSQL oracle revalidates
893 exact-NUMERIC contracts.

Public NUMERIC column modifiers remain guarded. Recursive scalar constraints,
generated-value restore verification and later physical encoding still have
separate contexts; joining those budgets, public column annotations/catalog/DDL
activation and integrated restore/reopen evidence remain required. Inventory
classifications are unchanged at 929 unresolved.

Final-source regression gates pass 601 local SQL tests (three existing skips),
226 server SQL tests, 185 native relational-index tests and their server
integration test, without failures or leaks. Control-catalog, inventory integrity,
formatting and whitespace checks pass. No generated public contracts changed.

### Shared recursive NUMERIC constraints and logical restore verification

Recursive scalar NUMERIC predicates now borrow the preparation context rather
than initializing another work/cancellation allowance. The stable constraint
owner retains its reusable bounded arena, charges owner/peak scratch memory to
the row byte allowance, and restores the caller's allocator and limb limits on
every exit. Composition still parses a scalar once across its predicates.
Ordinary and typed-row preparation pass the existing row context into validation;
standalone validation creates one context for SQL normalization, generated-value
verification and recursive scalar constraints.

Logical generated-value verification also accepts the caller's execution
context. Its unpublished arena is byte-bounded, restores caller allocators on
success and failure, and reconciles arena peak capacity against existing VM
allocation charges rather than counting the same retained output twice. Restore
continues to reject forged generated values and unconstrained assignments; it
does not fill defaults or repair stored values.

All 55 focused expression/constraint tests pass without failures or leaks. Tests
cover borrowed context identity, sticky work/cancellation across constraint and
scalar-normalization boundaries, restoration of allocators, tiny restore quotas,
and deterministic allocation-fault cleanup. Live PostgreSQL oracles revalidate
all 11 default/generated assignment fixtures and 70 constraint predicates. Inventory
classifications remain unchanged at 929 unresolved.

CHECK evaluation, physical encoding and field-local physical restore still need
their complete shared-context integration. Public NUMERIC column annotations,
catalog/DDL activation and integrated publication/reopen/restore evidence remain
unfinished; public column modifiers stay guarded. These remaining requirements
are not implied complete by the new preparation and logical-verification paths.

Final-source regression gates pass 601 local SQL tests (three existing skips),
226 server SQL tests, 185 native relational-index tests and their server
integration test, without failures or leaks. Control-catalog, inventory integrity,
formatting and whitespace checks pass. The changes are committed locally, not
pushed; the unrelated HTTP discovery edit is preserved.

### Shared CHECK admission for expressions and legacy comparisons

Logical row validation now carries the preparation execution context through
CHECK evaluation as well as normalization, generated verification and recursive
NUMERIC predicates. Recursive numeric scratch is released and charged before
CHECK starts, so the latter borrows only the remaining row allowance. Standalone
JSON and ordinal-row CHECK entry points retain convenience wrappers; shared
entry points let callers preserve work/cancellation identity across operations.
Both expression and legacy column CHECKs use bounded unpublished scratch.

Legacy NUMERIC operands parse under the shared exact context. Tuple encoders can
also borrow that context, charging canonical NUMERIC inspection/encoding and
non-numeric input/output scans while retaining independent per-key size limits.
Comparison scans consume the same allowance. Existing unscoped tuple callers
keep their byte representation and per-key limits; this does not change index
semantics or fingerprints. Failed shared tuple encoding restores the caller's
previous output prefix, including a quota failure after part of the tuple was
written. Work/cancellation failures remain errors during constraint activation,
not invalid-row diagnostics or fresh allowances.

All 58 focused tests pass without failures or leaks. New tests cover mixed
legacy NUMERIC/text and expression CHECKs on JSON and ordinal rows, sticky
quota/cancellation, tiny byte allowances, allocation-fault cleanup and the whole
recursive-constraint/CHECK pipeline sharing one budget. Borrowed literal text
comparisons are tested with an allocator rejecting every allocation: exactly
20 bytes of comparison allowance succeeds, 19 fails, and refilling the same
execution does not clear its failure. Activation propagates byte exhaustion
instead of persisting it as a bad-row finding; prior plans may have consumed
the shared allowance independently of the current row. PostgreSQL revalidates
all 235 NUMERIC dense-rank fixtures. Inventory classifications are unchanged at
929 unresolved.

Remaining: physical encoding/field-local restore context integration and public
NUMERIC column annotation/catalog/DDL activation. Legacy column CHECK comparison
domains also still inherit index-key size restrictions; decouple SQL scalar
comparison from persistent key encoding before claiming full PostgreSQL domain
parity. The shared admission work does not discharge that separate limitation.

Validation also passes 601 local SQL tests (three existing skips), 226 server
SQL tests, 186 native relational-index tests and their server integration test,
without failures or leaks. The final borrowed-comparison classification fix is
covered by the 58-test focused rerun and a fresh complete SQL rerun. Inventory,
control-catalog, formatting and whitespace checks pass. Changes are committed
locally only; the unrelated HTTP discovery edit is excluded.

### Shared admission through physical restore validation

Selected-field physical restore now carries one execution identity through
ordinal generated-value verification, expression/legacy CHECKs and recursive
field constraints. Existing convenience entry points create one row allowance;
new shared entry points let the caller retain prior work and cancellation.
Generated verification bounds its vectors and unpublished results before
allocation, while continuing to read only dependency cells. Field materialization
charges selected byte payload traversal and uses bounded, per-field scratch;
the following field borrows the remaining allowance, not a fresh quota.
Standalone property validation also uses bounded scratch. Every scope restores
the caller's allocators and reconciles peak capacity with existing logical
charges, without counting the same nested allocation twice.

A real AROW regression spans exact NUMERIC generated arithmetic, a legacy
CHECK, minimum/multipleOf constraints and physical field materialization. It
measures combined work and rejects a one-unit-short allowance, preserving the
failure after counters are refilled. Tiny byte admission, sticky cancellation,
allocator restoration and all allocation failures are exercised. Physically
canonical but logically forged generated output remains rejected; restore does
not default, round or repair stored values. The existing dependency-only cold
reader regression remains in place.

Residual planning also recognizes the pinned scalar/SQL-array domain checks
already discharged by strict canonical physical validation. Unconstrained SQL
columns no longer require a second decode into JSON merely because they carry
SQL type identity. This avoids decoded-envelope amplification for wide arrays
and preserves room for actual residual predicates. A canonical non-null NUMERIC
array with a signed lower bound and SQL NULL exercises the zero-allocation
path with zero remaining allocation allowance. Separate minimum and maximum
length properties remain selected, so this does not discard residual schema
constraints. JSON/JSONB and other non-scalar domains stay conservative. Physical
validation and public-schema/layout binding remain prerequisites, not optional
consequences of this optimization.

This integrates semantic physical-restore validation, not lower-level physical
codec/hash traversal or all recursive non-NUMERIC validator work accounting.
Those paths still need shared kernel admission. Public NUMERIC column modifier
activation and decoupling legacy CHECK domains from persistent key limits also
remain unfinished. No original inventory disposition is changed.

Final-source validation passes 60 focused schema-expression tests, 601 local
SQL tests (three existing skips), 226 server SQL tests, 186 native relational
index tests and their server integration test, with no failures or leaks.
Inventory remains 448 implemented, 136 rejected, 73 superseded and 929
unresolved. Control-catalog, formatting and whitespace checks pass. Changes
are committed locally only; the unrelated HTTP discovery edit is preserved.

### Public NUMERIC precision and signed-scale column activation

Relational root-column schemas now accept the shared
`x-antfly-sql-numeric-modifier` annotation for scalar NUMERIC and NUMERIC SQL
arrays. Strict parsing rejects malformed modifiers, non-NUMERIC identities and
nested SQL column declarations. The annotation flows through compiled validation,
immutable runtime layouts, SQL catalog descriptors and schema expression binding.
SQL scalar CREATE/ADD COLUMN declarations publish the same annotation instead of
silently dropping precision/scale or rejecting the supported scalar shape.

Assignment coercion uses the existing request-owned bounded NUMERIC execution
context before constraints, indexing and dependent generated expressions. Arrays
retain dimensions, signed lower bounds and SQL NULL flags. Capability version 23
is required even when no default, generated expression or index mentions the
column. Physical restore remains strict: it verifies constrained stored values,
never rounds or repairs them. Residual validation does not rematerialize domains
already discharged by the pinned physical layout.

Public OpenAPI and generated Go, Python, TypeScript and Zig contracts reuse
SQLNumericModifier. The array declaration modifier is genuinely optional: Go's
additional-properties serializer must not emit an invalid zero-valued modifier
for existing unconstrained declarations. Omission and signed-scale round-trip
regressions cover that boundary in Go, Python and TypeScript.

PostgreSQL independently verifies all 11 assignment/default/generated-value
fixtures used by the public-schema regression. The native LSM regression covers
overflow batch atomicity, populated-column reinterpretation rejection, index
lookup after reopen and portable restoration. This is public column activation,
not completion of SQL-array DDL, low-level physical codec/hash shared admission,
or legacy CHECK comparison decoupling from persistent key limits. Original
inventory classifications remain unchanged: 929 cases are still unresolved.

Final-source validation passes 62 schema-expression tests, 601 local SQL tests
(three existing skips), 226 server SQL tests, 187 native relational-index tests
and the server integration test, without failures or leaks. Go SQL/round-trip
tests, 42 Python SQL tests, 81 focused TypeScript tests, TypeScript type-checking
and 17 Rust SDK tests pass. Generated Python/Zig drift checks, OpenAPI checks,
control-catalog integrity, inventory integrity and formatting checks pass.
The large generated Go diff is the embedded compressed OpenAPI payload changing,
not hand-written query code. Changes are committed locally only; the unrelated
HTTP discovery edit is preserved.

### Logical CHECK execution without persistent-key amplification

Column/operator CHECK declarations and explicit immutable expressions now compile
into the same owned typed expression plans. Column binding and collation validation
remain schema-epoch operations; evaluating a CHECK no longer serializes a row cell
into an ordered index key. Persistent ordered-key size limits remain unchanged and
are still enforced by actual index encoding. CHECK logical domains instead use the
shared row execution work, cancellation and retained-byte admission contract.

The CHECK set no longer has separate comparison/expression runtime variants or
per-row key scratch. Dependency projection, JSON admission, cold ordinal admission,
deterministic activation failure handling and plan fingerprints use one path.
Equivalent named column/operator and explicit-expression declarations bind the same
typed plan fingerprint. Schema publication records the resulting CHECK coverage;
no new physical row encoding or index compatibility decoder is introduced.

Borrowed text/blob comparison retains binary chunk scans and ASCII collation while
charging actual inspected chunks against shared work and polling cancellation at
most 256 bytes apart. Repeated comparison cannot receive a fresh CPU quota. NUMERIC
continues using canonical borrowed views and its existing group admission.

AROW blob cells retain API base64 text, while typed operands and literals contain
decoded bytes. The VM now honors that boundary on JSON and cold rows rather than
comparing the two representations. Direct leaf comparisons decode bounded chunks
on the stack without allocating; complete base64 validation continues after an
early unequal byte or a SQL NULL counterpart. Nested programs and generated-value
verification use the same admitted decoder. Generated text/blob equality also
shares cancellable comparison work. Physical blob encoding is unchanged.

Column declarations construct their expression DOM directly instead of serializing
and reparsing large literals. Shared node/literal admission stops compilation as
soon as the CHECK set exceeds its limits, with initialized-plan cleanup intact.

A compact independent PostgreSQL fixture covers six assignments with 600 KiB
zero-filled bytea, text larger than 1 MiB, rejecting values and SQL NULL. Native
tests consume those cases through both CHECK declaration forms on JSON and AROW
ordinal rows. Cold checks succeed with every allocation denied, while actual key
encoding of the escaped blob still reports RelationalIndexKeyTooLarge. Separate
assertions prove cancellation during a long equal prefix, sticky work exhaustion
and preserved activation error semantics. The LSM regression covers batch
atomicity, reopen and portable restore with these wide constrained values.
Additional tests cover base64 chunk/padding boundaries, malformed suffixes hidden
behind early mismatches or NULLs, nested blob COALESCE, and rejection of forged
generated blob values on cold rows.

This removes the legacy column CHECK/key-domain coupling; it does not activate
array DDL, add a durable array expression domain, finish low-level codec/hash
shared admission, or widen native query/index predicate domains. The original
inventory remains 448 implemented, 136 rejected, 73 superseded and 929 unresolved.

Final-source validation passes 60 local and five server schema-expression tests,
601 local SQL tests (three existing skips), 226 server SQL tests, and 188 local
plus one server relational-storage tests, without failures or leaks. The six
independent PostgreSQL assignments, control-catalog compilation, inventory
integrity, Ruff, Zig formatting and whitespace checks also pass. These gates do
not establish completion of the remaining parity inventory or the separate
broad durable-runtime compiler-memory gate.

### Borrowed typed-array comparison boundary

The durable array-expression work now has a canonical borrowed comparison kernel
in `sql/array_comparison.zig`. Codec-authenticated pinned array views preserve the
precise element identity, row-major SQL NULL provenance, dimensions and lower
bounds. This boundary never infers types from JSON and does not itself establish
canonical-byte trust. Untrusted ingestion still crosses strict array validation.

Primitive values share the existing typed element comparator; NUMERIC uses its
canonical borrowed group views. Neither allocates a decoded element vector.
PostgreSQL's element-count, rank, dimension-length and lower-bound tie-breaks are
shared with materialized array comparison. JSONB parses only one element pair into
a reusable independently bounded arena. Shared JSON/array admission can now borrow
the row's work/cancellation context; local limits remain effective and exhaustion
or cancellation remains sticky across later comparisons. Binary text comparison
polls shared work in chunks of at most 256 bytes.

The independently regenerated PostgreSQL fixture contains 36 ordering cases across
all supported element identities: exact wide integers, NUMERIC scales/specials,
floating zero/NaN/infinities, NULL elements, empty arrays, rank/shape/bounds,
binary/C text, UUID and structural JSONB. Tests compare both directions and
reflexivity, cross-check the existing materialized path, deny every primitive
allocation, sweep JSONB allocation faults and verify sticky cancellation/quota
failures. The SQL owner explicitly imports this kernel so its tests are selected;
the generated control-source catalog includes the same source contract.

In a local Debug sample, 100 equal comparisons of a 10,000-element int64 array
took 178 ms without decoded allocations versus 201 ms when decoding one vector
per comparison. That conservative one-vector baseline peaked at 1,320,344 bytes;
comparison of two independently decoded inputs would require two vectors.
For 1,000 JSONB element pairs, scratch peaked at 2,012 bytes and was fully
released afterward. These are local workload samples, not production latency
guarantees or a claim that every query already uses borrowed array comparison.

Temporary JSONB trees borrow unescaped string and numeric tokens from pinned
input using the standard JSON scanner. The generic dynamic JSON parser owns
tokens even when allocation-if-needed is requested, so it cannot provide this
boundary. A 128 KiB string element now compares within 64 KiB of scratch;
tests retain strict syntax, escape and duplicate-key validation and sweep
allocation failures. Owned JSON parsing remains unchanged.

Materialized NUMERIC array validation, ordering and semantic hashing now use a
scoped exact context that charges its enclosing row/program on every work unit,
while retaining the array's local cap and inherited coefficient/input limits.
The parent owns cancellation polling and sticky failures, including exhaustion
of a child cap. A 1,000-limb regression checks all three operations for exact
single-charge accounting, mid-coefficient cancellation, sticky quota failure,
inherited coefficient admission and zero allocations. This checkpoint did not
yet wire scalar NUMERIC ingress or other codec owners; the ingress work below
connects those constructors and parsed array envelopes separately.

This is the comparison prerequisite for the durable typed-array VM, not public
array-expression activation. The VM's value domain, generated/default bindings,
array casts/operations, schema capability/public expression contract, array DDL,
and mounted PostgreSQL parity campaigns remain unfinished. Array DDL stays guarded
until those paths are connected. No original inventory disposition is changed.

Final-source validation passes 608 local and 226 server SQL tests, 60 local and
5 server schema-expression tests, and 188 local plus 1 server native relational
integrity tests: 1,088 passing tests, with three existing SQL skips and no failures
or leaks. The seven focused comparison/admission tests, independently regenerated
36-case PostgreSQL fixture, inventory integrity, generated control catalog,
formatting, Ruff and whitespace checks pass. Dispositions remain 448 implemented,
136 rejected, 73 superseded and 929 unresolved.

### Shared SQL array ingress execution

NUMERIC text and PostgreSQL binary constructors now borrow the caller's exact
execution identity while retaining local work limits and inherited input,
output and coefficient limits. Parsed array-envelope inspection and decoding
can share that identity too. Reported admission work is already charged when a
context is supplied; native row normalization no longer charges it again after
the walk. NUMERIC and other array columns therefore poll cancellation during
envelope admission, and native row preparation preserves cancellation rather
than reclassifying it as invalid user data.

Envelope shape, wire, byte and work-cap failures are sticky in the enclosing
context. Ordinary allocator failures keep their allocator error identity.
Owned NUMERIC modifier decoding remains unpublished and unwinds under allocation
faults. Direct NUMERIC JSON materialization also borrows its enclosing context
and respects inherited output limits.

Regressions cover 2,048-digit text/binary ingress, exact single-charge accounting,
mid-coefficient cancellation, sticky exhaustion and inherited input/coefficient
limits. The existing PostgreSQL array-envelope corpus now checks measured work
against the shared owner's counter. A 1,000-element float4 normalization test
checks cancellation before any rewrite; native numeric/float4 row normalization
checks preserve the original envelope and cancellation identity.

The durable VM value domain, public array-expression contract, array DDL and
mounted parity activation remain unfinished. Storage/binary-array codec owners
still need explicit shared-context integration; this is not a claim that every
array path is time-sliced or that row preparation parses each NUMERIC only once.
No original inventory disposition changes.

Final-source validation passes 610 local and 226 server SQL tests, 61 local and
5 server schema-expression tests, and 188 local plus 1 server native integrity
tests: 1,091 passing tests, three existing SQL skips, no failures or leaks.
All nine focused ingress tests, generated control catalog, inventory integrity,
formatting and whitespace checks pass. The larger typed-array VM activation is
not credited by these ingress checks.

### Logical row values separate from ordered key operands

The durable expression VM now owns a logical row-value domain independent of
the ordered-key operand type. Scalar ingress adapts explicitly; index expression
results project explicitly back into the scalar key domain. Batch key bindings
retain reusable logical input slots while persistent key encoding and its format
remain unchanged. Row rewrites use the same logical expression value type.

The row domain also represents canonical typed arrays as pinned bytes plus their
precise element identity, keeping SQL NULL as a distinct outer tag. It neither
infers types from JSON nor turns an extent check into a canonical trust proof.
Array comparisons reuse the borrowed canonical comparator. VM comparison scratch
is charged against the enclosing execution's remaining byte allowance, including
arena capacity that an outer invocation arena may retain; work and cancellation
remain shared. Primitive comparisons need no decoded vectors or heap allocation.

Public array columns/literals and array-valued generated/default bindings remain
guarded. Their compiler type propagation, JSON/cold-row adapters, public expression
contract, capability fencing and mounted PostgreSQL activation are still required.
This domain separation does not activate array indexes or change inventory credit.

Final-source validation passes 611 local and 226 server SQL tests, 62 local and
5 server schema-expression tests, and 188 local plus 1 server native integrity
tests: 1,093 passing tests, three existing SQL skips, no failures or leaks.
Focused scalar projection and bounded array-comparison tests, allocation-fault
cleanup, generated control catalog, formatting and whitespace checks pass.

### Durable array-expression reader capability

Schema format 24 fences array-dependent expression programs independently of
physical array columns. Declaration analysis detects array operands in CHECK,
index/UNIQUE expressions, defaults and generated programs, using exact column
names and both public wire and JSON expression representations. A scalar or
boolean result does not remove its array execution dependency. Unrelated array
columns and text literals do not over-fence scalar programs.

Older catalog capabilities reject this flag; decoding checks strict booleans
and every truncated frame before allocation. Format-23 flag-free schemas remain
readable, and text-projection fingerprints retain their prior representation.
This is a prerequisite, not public array-expression activation: compiler/DDL
guards remain until typed bindings, encoding and mounted execution are complete.
No original inventory disposition changes.

Final-source capability validation passes 611 local and 226 server SQL tests,
63 local and 5 server schema-expression tests, and 190 local plus 1 server native
integrity tests: 1,096 passing tests, three existing SQL skips, no failures or
leaks. Focused capability, generated control catalog and formatting checks pass.

### Canonical array preparation shares its enclosing execution

Array storage preparation and emission now optionally borrow the enclosing
request's work/cancellation identity. Prepared outputs retain their remaining
local work allowance across writes; neither a new array nor a repeated emission
can refill the parent budget. NUMERIC validation and encoding inherit the
parent's coefficient and output bounds instead of creating fresh cell budgets.
Output clearing and variable payload copies poll in at most 256-byte chunks.

Primitive preparation retains zero scratch allocations and one final output
allocation. JSONB canonical buffers retain their separate bounded scratch cohort;
quota exhaustion poisons the enclosing invocation, while ordinary allocation
failure remains OutOfMemory. No wire-format or logical-value semantics change.
The borrowed parent must outlive Prepared and every write.

Sixteen focused flat-array tests pass, including PostgreSQL binary fixture
round trips, repeated-write accounting, sticky cancellation during clearing and
payload emission, inherited NUMERIC limits, escaping-expanded JSONB scratch
quota exhaustion, and allocation-fault cleanup. Generated control catalog and
immutable inventory checks pass. This does not activate public array programs:
their bounded JSON-to-row adapters and compiler/DDL propagation remain pending.
Shared storage decoding and interruption inside canonical JSONB serialization
also remain separate work; this change does not claim complete time-slicing.

Integrated storage-source validation passes 615 local and 226 server SQL tests,
63 local and 5 server schema-expression tests, and 190 local plus 1 server native
integrity tests: 1,100 passing tests, three existing SQL skips, no failures or
leaks. The subsequent array-adapter source has its own validation below.

### Bounded typed-array JSON-to-row adaptation

The durable row VM has an explicitly typed array-envelope adapter. It decodes
the parsed envelope once, prepares canonical storage under the same shared work
and cancellation identity, and returns owned bytes with their precise element
kind. It does not stringify and reparse JSON or infer integer/NUMERIC identity.
NUMERIC modifiers apply before encoding; SQL NULL remains an outer value tag
and array-cell NULL flags and signed multidimensional bounds are preserved.

Scratch remains unpublished in a quota-bound arena. The final row buffer belongs
to the caller, and its retained-byte charge is added to actual scratch arena
capacity, not hidden behind a maximum of the two. All allocator and numeric
limits are restored on success and failure. Empty arrays can require no scratch;
nonempty arrays must account for scratch separately from retained output.

Public compilation, binding sites and DDL are not activated by this adapter.
They still require precise element-type propagation, generated public contracts,
row transforms and mounted execution evidence. No inventory cases are credited.

Final-source adapter validation passes all 65 local and 5 server schema tests,
with no failures or leaks. All 11 PostgreSQL binary element fixtures pass
canonical-byte identity, input-owner destruction and allocation-failure sweeps.
The 20 existing PostgreSQL NUMERIC modifier cases also run through this adapter,
checking rounding, overflow, SQL NULL flags and multidimensional bounds. Sticky
cancellation, byte exhaustion, allocator restoration, control catalog and
formatting checks pass. The 1,100-test integrated storage result above predates
this adapter; it is not reported as a full current-source gate.

### Shared strict array readers and owned result envelopes

Canonical array directories now expose budget-aware strict opening without a
SQL dependency. Directory/checkpoint/cell scans, text validation and embedded
NUMERIC coefficients charge and poll the caller's work/cancellation identity.
UTF-8 validation retains the standard vectorized validator in bounded chunks
split at codepoint boundaries. Header-only authenticated projection remains
O(rank), and is still not a canonical admission proof.

Storage decoding and strict JSONB verification use that same owner. NUMERIC
child contexts inherit input/coefficient limits; text copies poll at 256-byte
boundaries. Reported decode work has already been charged to the parent. Quota
failures remain sticky, while ordinary backing-allocation failure stays OOM.
Strict primitive and NUMERIC admission still allocate nothing; JSONB reuses
one bounded element region. Canonical JSONB serialization itself still needs
internal sorting/number-emission interruption; this is not claimed complete.

The row VM's canonical-array output path directly materializes the ordinal
JSON envelope instead of stringify/parse. Decode scratch and retained DOM
capacity are summed against the enclosing byte allowance. Managed arrays,
including nested JSONB arrays, are rehomed to the row owner before a temporary
budget leaves scope. This is a region-owned adapter: callers discard the row
region on failure, as with other leaky row DOM materializers.

Final focused validation passes 19 flat-array tests, including all 11 PostgreSQL
element fixtures, allocation-fault sweeps, sticky directory/text-copy
cancellation, inherited NUMERIC bounds, and all 256 byte mutations at a UTF-8
polling boundary plus a distinct UTF-8-valid embedded NUL case. Final-source
schema validation passes 67 local and 5 server tests, no failures or leaks.
Those tests verify result DOM ownership after source-byte destruction, exact
canonical round trips, nested allocator lifetimes and combined byte quotas.
The larger SQL/native integration revalidation remains separately tracked.

Public array-expression compilation, precise branch/result type propagation,
binding/assignment normalization, row rewrites, generated contracts and DDL
activation remain unfinished. No original inventory disposition changes.

### Typed array programs and streaming assignment regions

The durable expression compiler now retains precise array element identity in
literals, columns, conditional branches and plan results. Comparisons and IN
require matching operand identities; generated/default targets require the
exact result identity even for SQL NULL. JSON ingress prepares canonical owned
bytes; pinned values and cold ordinal reads borrow the authenticated row owner.
Historical source projection cannot reinterpret a different element type.

NUMERIC-array assignment checks canonical constrained coefficients without
allocating. When rounding is required, it streams one coefficient at a time
into an unpublished output buffer, preserving dimensions, lower bounds and
NULL flags without a decoded cell vector. Base assignments normalize before
generated dependencies execute. Restore verifies the declared target domain
and logical generated result without repairing stored values or forcing an
equivalent display scale.

Request-region allocation accounting now has an opt-in monotonic footprint.
Frees and shrinks cannot refund bytes that an enclosing arena may retain;
moving replacements reserve their complete allocation. Ordinary reclaiming
owners retain their existing accounting. Execution scratch and output capacity
share the same allowance, including intermediate buffers, and quota/cancellation
failures remain sticky without converting ordinary backing OOM into a quota.

Cold dictionary index batches bind array dependencies as logical row operands,
checking NULL slots before dictionary addressing. Scalar expression keys reuse
the existing ordered codec; array-valued ordered keys remain explicitly guarded.
Explicit row rewrite adapters retain canonical payloads and exact element
identity without a JSON round trip. This does not authorize ordinary restore
to rewrite historical rows.

The 10,000-cell Debug assignment fixture produces 161,270 output bytes using
five backing allocations and 505,224 total charged allocation bytes, with no
flat cell vector. Tracked allocation bytes exactly equal the invocation charge.
The observed approximately 10 ms is a local sample, not a production benchmark.
Existing PostgreSQL oracle fixtures cover 11 binary arrays across nine element
domains, with NUMERIC covered separately by 20 modifier boundary cases and
generated assignment tests. No new parity disposition is inferred from these
internal adapters. Public generated expression contracts, SQL DDL/lowering,
direct array CHECK declarations and mounted activation campaigns remain work.

Validation is recorded as separate receipts, not a green combined invocation:
SQL revalidation passes 620 local and 226 server tests with three existing skips.
Final schema validation passes 72 local and five server tests, including
generated-array assignment and both physical-domain and derivation rejection.
Native validation passes all 192 local tests and its server contract, including
the new NULL dictionary-slot and rewrite-identity regressions. These runs have
no failures or leaks. An earlier combined run failed because the fixture omitted
required catalog flags; its corrected successor also exposed that the strict
codec rejects an invalid target coefficient before a forged row can be built.
The final test asserts that rejection and separately constructs a domain-valid
wrong generated value. No production validation was relaxed to make it pass.
Formatting, whitespace and the storage control-catalog check pass. No original
inventory dispositions or public feature claims are changed by this unit.

### Bounded immutable constant preparation and PostgreSQL lazy demand

Binding now separates mandatory unknown-literal input functions from optional
constant evaluation. Invalid raw array input still fails in an unselected
branch, while errors from typed runtime conversions and NUMERIC modifiers stay
lazy. Disabling optional preparation does not disable input validation or change
which runtime branches are demanded. Genuine backing allocation failures and
internal errors are not hidden as optimization misses.

Constant arrays, exact numbers and membership indexes share bounded work and
allocation cohorts per bound program. Purity is classified in one topological
pass rather than repeatedly walking overlapping subtrees. Successful regions
are adopted without cloning; their arena owners remain heap-stable because
JSONB DOM containers retain allocator pointers. Program moves preserve those
owners, and failed publication unwinds regions and membership indexes. These
are per-program bounds, not statement-wide preparation/cancellation admission.

An independent PostgreSQL 18 oracle verifies 16 values and SQLSTATEs. Native
tests run each case with optional caches both enabled and disabled, exercise
exhaustive allocation failures, enforce preparation/output quotas, and retain
the owner-move regression. A 10,000-row Debug repeated-execution fixture uses
zero evaluation allocations and 936 bytes of constant backing storage (not
total program memory); its approximately 2.25 ms is a local sample, not a
production latency claim. The full SQL gate passes 625 local and 226 server
tests with three existing skips and no failures or leaks.

The schema-expression gate also passes 74 local and six server tests, with no
failures or leaks. Formatting, oracle script lint and the storage control-catalog
check pass. Unrelated HTTP discovery changes are preserved and not included.

Durable dynamic array constructors, element-changing durable casts, non-NULL
SQL array defaults and broader array operations remain unfinished. Original
inventory dispositions are unchanged; cache correctness is not evidence of
activating those cases.

### Durable array identity and streaming NUMERIC modifier casts

The native expression VM, SQL schema lowering and public structural precheck
now admit precisely typed array identity casts and NUMERIC-array modifier
casts. An identity cast borrows canonical pinned input. Modifier casts reuse
the streaming assignment kernel: one coefficient scratch region, preserved
dimensions/lower bounds/NULL slots, sticky shared work and allocation admission,
and no reconstructed flat cell vector or JSON round trip. Already constrained
inputs remain allocation-free. Typed NULL retains its array domain, and nested
casts retain distinct modifiers rather than overwriting an operand's cast.

CASE and COALESCE still execute only selected branches. Overflow is a runtime
error, not a schema-publication failure. Typed compilation continues to reject
element-changing casts and scalar/array reinterpretation. The generated public
specification, Go embedded specification, Python/TypeScript descriptions, Rust
bundled specification and Zig contracts document the same boundary. The large
Go diff is its compressed embedded OpenAPI payload, not a runtime refactor.

Evidence extends all 11 independent binary-array fixtures with identity casts
across pinned values, JSON input and cold ordinal rows. NUMERIC casts use the
existing 20 modifier fixtures, independently reverified within all 893 exact
PostgreSQL NUMERIC contracts. A SQL lowering regression checks query/VM byte
agreement across typed NULL, nested modifiers, unselected overflow, JSON input
and cold rows. Exhaustive allocation faults now exercise compiled cast plans
and their streaming conversion; constrained reuse is checked with a failing
allocator. This is not complete array-constructor/cast activation: dynamic
constructors, element-changing casts and non-NULL SQL array defaults remain
unfinished, and no original inventory case is reclassified by these tests.

The combined SQL/schema/OpenAPI validation passes 626 local SQL tests (three
existing skips), 226 server SQL tests, 74 local schema tests and seven public
server expression tests, without failures or leaks; all 144 build steps pass.
Python generation checks, TypeScript type-checking, Go SDK OpenAPI tests and
Rust specification synchronization also pass. Formatting, whitespace, storage
control-catalog and original inventory integrity checks pass; 929 original
cases remain unresolved.

A final cache audit additionally stops preparation after its allocation cohort
is exhausted. Retrying a smaller candidate with a sticky quota flag could hide
a later genuine backing allocation failure. The separate final-source constant
gate passes all seven tests, including the new exhaustive mixed-size candidate
fault sweep. This follow-up does not change expression demand or parity counts.
