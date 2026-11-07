# Named SQL types

Status: implementation in progress. Originals 0096–0100 are qualified by public,
native storage/cohort and PostgreSQL campaigns. Domain annotations (0150) and
independent retirement (0309) are qualified separately. Domain row execution,
protected-table evolution, enum arrays and retained enum-valued views remain
unfinished; catalog-only tests do not qualify them.

Named types must retain identity independently of their physical scalar family.
An enum is not TEXT with an allowed-values check, and a domain is not a copied
column default/CHECK. PostgreSQL is the SQL contract.

## Catalog and immutable binding

A namespace owns a single named-type name directory for enums and domains.
Every type has a stable catalog ID, scalar/array wire OIDs, and immutable
executable generations. Mutation publishes the definition, reverse dependency
edges and bounded durable outcome atomically using the existing catalog lane.
The first implementation must reject generic writes that bypass these checks.
Prepared and stored definitions retain type IDs and executable generations;
rename changes spelling, while drop/recreate changes identity. Builtin and
implicit table-row type names cannot be shadowed.

Each request pins an immutable type view. Native schemas retain the exact
referenced type definition, not a name to resolve during row execution. A type
change that affects existing columns publishes their new schema contracts
through the existing coordinated schema machinery. All affected owners must
acknowledge the contract before the SQL operation reports completion. Negative
or uncertain acknowledgements preserve the existing recoverable job semantics.

## Enum values and ordered indexes

Enum labels have stable per-type symbol IDs, independent of declaration order.
Input validates UTF-8, PostgreSQL's label-byte bound, duplicate labels and exact
case. SQL NULL remains separate from every label. Decoded values carry type
identity; enum/text conversion is explicit, and two different enum types cannot
compare merely because their labels match.

A label also has a stable dense byte-string order key. ADD VALUE chooses a key
strictly between its neighbors without changing any existing symbol or order
key. This avoids rewriting historical rows and LSM keys on ordinary BEFORE/AFTER
insertions. Comparison, joins, unique/FK keys, range scans and sort use this
same order. A compiled view maps symbol IDs directly and resolves labels with a
sorted lookup directory; row execution neither scans catalog definitions nor
allocates a new registry.

Order keys may grow under adversarial repeated insertion into one gap. Admission
bounds total definition bytes, key bytes and label count before publication;
exhaustion must report a resource-limit error rather than reorder existing keys
or accept an ambiguous representation. Tests must include deep gap insertion,
lexicographic prefix boundaries, stable existing keys and cold reconstruction.

## Domains

Domains retain the named identity plus an exact base type, default, NOT NULL
state and named CHECK programs. Constraint binding uses a typed VALUE input and
retains ordinary SQL NULL/three-valued semantics. Ingress, SQL assignments,
explicit casts, typed-row mutations, COPY and restore share the native validator.
Adding a restrictive contract validates every dependent stored value through the
existing online schema-validation/cohort protocol before activation. Domain
NOT NULL does not erase outer-join NULL semantics. Default changes affect future
assignments, not historical rows.

## Completion requirements

- Public original CREATE/ALTER/DROP enum and domain statements, exact PostgreSQL
  notices/errors and authenticated catalog publication.
- Usable table columns, defaults, assignments, arrays, scalar/array wire identity,
  parameter binding, casts and immutable prepared/stored dependencies.
- Consistent comparison, ordering, aggregates, joins, unique/FK enforcement and
  indexed bounds; no string-order approximation for enums.
- Online dependent-schema activation, drop RESTRICT/CASCADE, bounded ambiguity
  recovery, corruption rejection and independent cold restore/reopen tests.
- PostgreSQL oracle fixtures and strict native public campaigns that execute the
  unchanged original inventory statements and verify complete postimages.

Implementation begins with immutable type identities and stable enum ordering.
Catalog publication alone does not satisfy these requirements or justify
promoting the original cases in the qualification ledger.

## Implementation state

The immutable enum model and value kernel now exist. Labels retain append-only
symbol IDs and dense stable order keys. Prepared atom directories provide
allocation-free input lookup, symbol decoding, comparison and borrowed index
ranks. A physical enum value is exactly four bytes; SQL NULL belongs to the
row's existing bitmap. A durable origin UUID is independent of remappable local
catalog IDs and scalar/array OIDs. Logical hashes retain that origin and symbol;
SQL equality/hash identity also includes the local type. The kernel tests verify
100,000 repeated operations, deep order insertions, remapping, corrupt symbols,
separate type identities and allocation-failure cleanup.

Namespace-owned enum publication uses command version 6 and decoder capability
111 on the existing catalog and bounded outcome journal. Primary records,
name/child indexes and an origin lookup directory commit atomically. Ordinary
catalog writes cannot bypass the type lane. Independent ADD VALUE operations
can serialize against an older captured generation when their immutable type
ID is unchanged; DROP retains an exact generation fence. Conditional skips
retain an explicit no-op outcome. Notice capacity is reserved before submission.
Recovery resolves the current namespace by ID. A retired namespace requires
global administrative authority; reusing its old name cannot redirect a token.

The nine-test native metadata campaign covers cold publication, ordered symbol
insertion, generation/identity preservation, exact duplicate and no-op replay,
name-index rebuilding, validation precedence, namespace rename/reuse/retirement,
and read-only observation of retained decisions. The public catalog campaign
compares original declarations with PostgreSQL, permissions, search paths,
cold metadata and a deliberately lost acknowledgement. This catalog campaign
is separate from complete typed-row qualification.

Native schema epochs now retain a deduplicated, ID-sorted type registry.
ASCH encoding and semantic capability 39 fence these layouts. Schema decoding
owns labels, ranks and principals independently of input buffers; validation
bounds registry bytes and rejects dangling IDs, duplicate origins and incompatible
column descriptors. One prepared atom directory is shared by every referencing
column in an epoch.

Prepared native writes resolve labels through that directory and encode four-byte
symbols in dense or sparse AROW. Projection and reconstruction resolve exact
labels, including empty labels independently of SQL NULL and absent cells.
Strict restore rejects unknown symbols even when the physical checksum is valid.
Logical row hashes include durable type origin and symbol, so local catalog-ID
remapping and additive type evolution preserve content identity.

Ordered tuples use stable escaped rank bytes for row writes, typed bounds,
native vectors and external label vectors. Their definition fingerprints retain
type ID and origin without forcing a rebuild on ADD VALUE. Historical projection
rejects incompatible symbol/rank definitions. Covering indexes retain only the
referenced type definitions, bind type identity in forward-value fingerprints,
and preserve INCLUDE fields through historical source projection. Transform
cells retain identities, and cold page decoding validates symbol membership. Enum operands borrow cached
atoms; shared logical value slots remain 32 bytes. A Debug measurement of 100,000
sparse projected row/key operations used zero allocations after preparation
(80-byte row, 64 schema columns, roughly 0.1 seconds on the development machine).

Schema-pin tests release both producer buffers and the initial epoch owner before
reading retained atoms. Allocation-failure enumeration includes schema decoding,
compiled directories, tuple plans and covering-plan construction.

These native storage tests do not qualify the public SQL feature. Still required
before the original cases receive implementation credit:

1. Bind SQL type-name intents and scalar descriptors to the schema-owned
   definitions. Public JSON schema binding, catalog dependencies and typed-row
   APIs and SQL discovery of those declared native enum columns are implemented.
   Public SQL result/parameter descriptors and scalar pgwire OIDs now retain
   that identity. SQL CREATE TABLE resolves normalized type-name intents through
   one bounded catalog cut, deduplicates executable directories, and serializes
   owned definitions before admission. Public writes and cold reads are tested.
   Named scalar casts now resolve through a preparation-only interface and
   invocation-owned lookup cache. Durable prepared resources retain the original
   lookup plus catalog incarnation, namespace ID, type ID, origin UUID and minimum
   compatible generation. Cold execution succeeds; drop/recreate of the spelling
   rejects the old prepared statement. Catalog syntax alone does not qualify the
   original lifecycle cases.
2. Complete enum expression operations, uniqueness/FK type compatibility and
   distributed integrity qualification over those public schemas.
3. Activate enum arrays and retained enum-valued view outputs; named scalar
   casts now retain immutable preparation identities. Scalar parameter/result descriptors now retain immutable identities,
   prepared witnesses and scalar OIDs; array execution remains unsupported.
4. Activate ADD VALUE across every dependent schema through the existing
   coordinated schema/owner-ACK protocol. Pending or uncertain publication must
   retain a recoverable job handle; it cannot report ready from catalog commit.
5. Add full public row/default/index/restart/restore campaigns, dependent DROP
   RESTRICT/CASCADE, domain base/default/CHECK/NOT NULL validation and activation.

The initial ledger credited 0096–0099 after mounted execution and native
cohort/storage qualification. Dependent enum retirement 0100 is now qualified
by the later staged-restoration campaign. Domain CREATE/ALTER and dependent
CASCADE originals remain unresolved; the separately qualified annotations and
independent retirement do not establish those row-use contracts.

## Public schema and integrity binding

The public TableSchema now exposes generated readonly named-type layouts and
physical `sql_enum` columns referencing an immutable catalog type ID. The
ordinary schema compiler owns a deduplicated registry of executable views;
validators, row preparation, native schemas and cold projection retain that
same identity. Embedded use remains self-contained. Server ingress resolves
references from one bounded metadata read cut, checks existing namespace usage
authority against current scope names, replaces readonly client assertions,
and drops unused snapshots. Constants and JSON payloads do not create type
dependencies. Ordinary schemas retain an allocation-free negative fast path.

REST creation, full replacement, RFC 7396 PATCH postimages, MCP creation, and
the internal SQL schema producer share this binding stage. Public requests
resolve layouts before pure schema parsing and reuse that read cut for FK
admission. Metadata repeats fixed-size executable-layout witnesses using point
lookups, then updates reverse type/table dependencies in the table transaction.
Name-directory reconstruction retains durable reverse edges. Rejected catalog
outcomes remain rejected after their blocking dependency disappears.

FK admission and distributed claim preparation compare named type identity and
stable common symbol history independently of child/parent column names.
Additive epochs preserve existing equality tuples; unrelated enums and forged
rank histories cannot share claims merely because both use four-byte symbols.
This uses the existing native integrity planner and transaction machinery.

Dependent ADD VALUE and DROP CASCADE still require coordinated owner/schema
activation. Their explicit activation barrier remains in place, while DROP
RESTRICT reports dependent objects. Public scalar SQL column/parameter descriptors
and CREATE TABLE names retain immutable type identity. Named casts, array execution
and domain activation remain unfinished. These changes do not
qualify the original enum/domain cases or change the disposition ledger.

Creation now captures compiler-derived reader contracts before the catalog
barrier and carries them through normal and FK-initial admission. Read-only
preflight and serialized apply share executable-layout verification. Active
and retained read schemas contribute to the same reverse dependency set, so
historical rows cannot lose their type while a migration is unfinished.

JSON predicates and ordered bounds resolve labels once to borrowed cached
atoms. Typed tuple encoding remains strict. Text projections discard the row
registry along with relational columns and retain their existing v11/v12
representation; additive type definitions do not alter projection bytes at a
fixed logical schema version.

The final Debug affected gate passes 25 storage/model tests, 10 native metadata
tests, and three public/admission campaigns with no skips, failures or leaks.
The public campaign creates a table through REST using only catalog type IDs,
verifies readonly assertions cannot replace definitions, writes typed values,
rejects invalid labels, applies PATCH, filters by declaration order, and checks
complete projections after an independent cold reopen. Admission tests include
current-scope authorization, deduplicated bulk lookup and allocation-failure
cleanup. Native tests preserve dependencies that exist only in retained read
schemas through name-directory rebuilding and cold metadata recovery.

On the development machine, Debug loops of 100,000 sparse projected row/key
operations and 100,000 JSON-bound preparations took roughly 89 ms and 32 ms,
respectively, with zero allocations after preparation. These microbenchmarks
are separate from end-to-end request throughput and do not qualify SQL scalar
execution or coordinated type activation.


## Bound SQL values and retained execution

SQL datums now carry schema-prepared enum atoms in the existing extension slot,
without growing each ordinary cell. Atoms cache borrowed labels and immutable
ranks once per type epoch. Equality, ordering and hashing retain local type ID,
durable origin and symbol; equivalent additive generations share their existing
symbol/rank history. Common domains select the additive successor when two
inputs reference different compatible generations.

Bound scalar programs support enum comparisons, unknown literal input, explicit
text output, NULL/three-valued operators, IN, CASE, COALESCE, NULLIF, LEAST and
GREATEST. Enum arithmetic, implicit typed-TEXT comparisons, unrelated enums and
conflicting shared parameters fail during binding. Unknown enum literals use
the existing mandatory input-preparation lane, so invalid labels fail before
lazy branch execution and valid constant labels resolve once per program.
Variable-width rank comparison consumes the shared execution work quota. Programs and parameter frames
capture deduplicated owned executable directories. PostgreSQL enum binary input
is a label, not a native four-byte symbol. Serialized descriptors and casts expose
symbolic identity/generation witnesses, never executable process pointers;
unbound deserialization rejects those witnesses until catalog rebinding exists.

Graph hash joins, membership, grouping, top-K, MIN/MAX partial transport and
portable row/column spill retain the same typed atoms. Spill decoding bounds and
validates identity, labels and ranks before copying. Generic JSON, builtin-array
and coarse native-mutation paths reject unsupported enum payloads instead of
interpreting their JSON-null placeholders as SQL NULL.

Retained enum columns intern each distinct (type, origin, symbol) once, own its
label/rank independently of the producer and retain only one tagged pointer per
row plus the NULL bitmap. Enum-only columns allocate no placeholder JSON vector.
A mixed producer widens safely, and primitive physical-vector shortcuts refuse
extension columns. A Debug campaign retaining 8,192 repeated values used 14
allocator calls, including vector growth. This is allocation evidence, not an
end-to-end throughput claim.

The reproducible PostgreSQL 18+ execution oracle contains 248 scalar probes over
three labels and SQL NULL, checking exact values and SQLSTATEs. Native campaigns
also cover disk-backed duplicate join fan-out, producer retirement, additive
common types, malformed/truncated spill, budget failure and exhaustive allocation
failures. Compatibility JSON hashing now agrees with prepared-row hashing for
enum labels, empty labels, NULL and absent fields.

These changes establish the shared enum value/expression execution layer. SQL
column names and scalar casts now bind immutable catalog identities. Enum arrays,
distributed dependent-schema activation, domains and original enum DDL
qualification remain incomplete. No original inventory disposition was
changed for these runtime probes.

## Declared-column SQL and transport activation

SQL discovery prepares one owned enum directory per type in a native schema.
Table reads, projected native column pages, joins, set operations, aggregates,
windows, assignments, conflict updates and RETURNING preserve the executable
domain. Existing installed RLS policies use the same declared-column binder and
pinned native row decoder. Ordinary timestamp pages retain their established
physical normalization. A direct column read has no scalar projection vector;
semantic-error recovery must handle that shape without indexing such a vector.

OpenAPI owns the public `enumeration` result type and optional `named_type`
identity on SQL columns and parameter descriptors. Enum cells contain exact
labels, including empty strings; SQL NULL remains in the NULL bitmap. Generated
clients consume these definitions. PostgreSQL text and binary enum input/output
both carry label bytes; RowDescription and ParameterDescription retain the
catalog scalar OID and four-byte type length.

Durable prepared descriptors store only immutable identity witnesses. Execution
resolves all referenced IDs in one current authorized catalog cut and prepares
shared executable views. Deserialization cannot manufacture an executable view.
Prepared frames and programs compare logical domain contracts, never allocation
addresses. Contextual input inference keeps parameters nullable even when a
referencing table column is NOT NULL; stored-row constraints remain responsible
for rejecting invalid assignments.

Portals and cursor spools capture their own type directories and payloads before
prepared statements or producer leases retire. COPY decodes labels through the
pinned column directory without a per-row enum allocation. Unsupported enum
arrays remain explicit errors. Durable enum-valued view outputs require a
future identity/rebinding contract and fail before publication; an explicit
conversion to TEXT uses the existing durable view contract.

The Debug qualification commands are `sql-test -Dtest-filter='SQL named enum'`,
`pgwire-test`, and `antfly-api-sql-enum-catalog-test`. The public campaign exercises
native typed-row ingestion, SQL updates and RETURNING, ordered/filter/aggregate
reads, prepared parameters and cold restart. It now also exercises SQL named
CREATE TABLE/casts and prepared identity rejection after name reuse. Dependent
type evolution, enum arrays, domains and restore activation remain unqualified;
these tests do not promote an original inventory disposition.

## Persistent enum expressions

The native scalar validator now accepts schema-bound enum literals, explicit
TEXT/enum conversions, column reads, comparisons and conditional results.
Programs own immutable atom directories independently of producer buffers.
Enum literal/cast JSON carries only `named_type_id`; its definition comes from
that schema epoch. Comparison uses stable declaration ranks, and fingerprints
retain type ID/origin and symbol without changing after additive generations.
Unrelated named identities cannot compare merely because labels match.

SQL defaults, generated values and CHECKs lower to this existing native VM.
Native row preparation and cold validation share the same programs. Enum
expressions require semantic capability 40 and ASCH format 40, including when a
program produces a boolean or TEXT. Ordinary enum columns retain their semantic
floor 39; the current physical frame has its independent encoding requirement.
Per-row lookup uses cached atoms and checks the referenced symbol rather than
scanning every label. Schema-owned directory memory has a preparation quota.

The focused Debug gate passes 65 enum/reader/decode tests with no skips or leaks.
This includes native default/generated/CHECK preparation, cold validation,
additive fingerprint stability and allocation-failure cleanup. The mounted
public campaign exercises the same default/constraint path, explicit supplied
values and omitted defaults, before and after restart; all three tests pass.
This work does not qualify dependent ALTER/DROP, enum arrays or domains.


## Dependent type activation

ADD VALUE now uses one bounded immutable cohort on the existing schema-validation
ledger. Admission captures the type incarnation, origin, generation and complete
reverse dependency set at one catalog cut. It takes lifecycle locks for every
participant and records owner preparation once; ACKs retain compact ordinal
records. Discovery carries each physical table ID separately from the cohort's
anchor. The writer checks every current owner/root and exact per-table layout;
different participants may have different columns and schema versions.

The native proof checks append-only symbol/rank preservation and unchanged
column ordinals, generated/check fingerprints, index tuple encodings and
integrity generations. Existing transactions drain before compatible successor
installation. The type and all table descriptors publish atomically after every
owner is installed; release completes the operation. No historical row scan,
physical rewrite or index rebuild is required. The populated LSM test verifies
zero scanned rows, unchanged primary bytes and index identity through cold
recovery. Admission/publication fault tests cover atomic participant locks,
aliases, schemas and type state, including heterogeneous table layouts.

The SQL producer retains its actor-authorized recovery receipt before admission.
A bounded request can return HTTP 202 with DDL PENDING and a pending catalog
receipt. Pending carries no completed generation/catalog revision. Read-only
observation joins the original nonce digest to the exact retained job; another
job for the same type/generation cannot substitute for it. Cold completion
returns the original committed revision even after later operations. Recovery
never resubmits the mutation. Decoder floor 115 fences cohort admission.

The mounted Debug campaign now passes public creation of parent/child enum
columns with UNIQUE/FK enforcement, pending admission, denied receipt access,
predecessor reads, cold completion, successor writes and retained FK rejection.
PostgreSQL witnesses distinguish bare ORDER BY of a TEXT cast's output label
from qualified ORDER BY of the enum input. These are different sort domains.
The same campaign passes conditional ADD notice/no-install checks, dependent DROP
RESTRICT, duplicate UNIQUE rejection, a second positioned generation and exact
recovery of the earlier nonce after that generation. Public execution passes all
four mounted tests; metadata catalog and shared submission each pass thirteen.
The combined Debug snapshot additionally passes 1,596 local SQL tests (four
existing opt-in skips), 243 server SQL tests, both 52-test publication roots and
the populated LSM epoch gate, with no failures or leaks.
The admission helper is shared with ordinary schema validation: report/occupancy
lag retries the same prepared plan only before proposal; uncertainty permits
read-only observation, never replay.

Protected tables remain ineligible: an installed row-policy program binds the
exact schema, so its successor must be prepared and attested under the same
all-owner decision. The common policy guard remains enforced; retaining an old
policy stamp or bypassing that writer is not an activation strategy. Enum arrays
and retained enum-valued views also remain unsupported. Domain SET NOT NULL now
uses executable domain directories, projected validation, write fences and
authenticated recovery through the shared coordinator. Dependent DROP CASCADE
uses staged rewrite machinery to remove columns/artifacts while preserving
other columns and rows, with type retirement in the same publication. Other
restrictive CHECK changes retain explicit guards. Catalog-only mutation does
not qualify dependent evolution.


## Type annotations

Original `sql-0149` is independently qualified. COMMENT ON TYPE uses the shared
resource annotation field and atomic nonce outcomes, including exact observation
after a lost reply. Empty/NULL removes the description; unchanged descriptions
retain a no-op outcome without rewriting the resource. Type IDs, executable
generations and canonical row layout digests do not change. ADD VALUE preserves
the annotation. Decoder floor 114 gates the tagged operation, including removal,
before durable admission. Snapshot responses omit absent descriptions to retain
the ordinary query projection. The public cold campaign passes all three tests;
the durable catalog and decoder suite passes twelve.

Dependency preparation now optionally returns the complete type/table reverse
edge set and table records from one immutable catalog revision. Admission caps
this cut at 128 tables and 16 MiB, rejects excess edges before schema loading,
checks every table's canonical type witness, and computes the shared expected
type digest once. Ordinary casts do not traverse this directory. Native tests
exercise two real tables, an excess-edge rejection and catalog reconstruction.
The read-only cut grants no mutation authority. ADD admission independently
rejoins the current writer's complete dependency/owner cut; dependent DROP and
protected-table evolution still require the additional barriers described above.


Dependent enum retirement now uses the shared staged restore/rewrite machinery
and an atomic type/table catalog publication. The populated public cold campaign
covers parent/child data, namespace authority, PostgreSQL notices and complete
column postimages, identity continuity and exact retained receipt recovery after
name reuse. It does not certify guarded policy/view/artifact cohorts or portable
type-catalog restore. Domain originals 0150 and 0306–0310 remain open.

Cold readiness/installation ACKs retain their logical acceptance while current
reporter/runtime authority can change. Idle workers re-attest through the same
fully verified ACK reducer when the current signed commitment differs. Counts,
logical chains and physical work remain unchanged; terminal records stay idle.
The public restart test forces one old-reporter ACK before reopening stores.

## Domain contract and activation boundary (2026-10-29)

The native definition is now a distinct named-type union alternative. Domains
retain their exact base identity, numeric modifier, owned native default and
named CHECK programs. An immutable bounded registry resolves forward references,
rejects cycles and rebound origins/generations, and prepares the base chain once.
The chain depth is checked independently of DFS recursion depth: already prepared
parents must not permit an arbitrarily deep execution stack. Enum physical
identities remain enum identities when a domain uses an enum base.

SQL declarations lower through the existing scalar binder and native expression
VM. VALUE is one typed `value` slot. Source numeric lexemes remain exact. Domain
constraint names are allocated in declaration order; a later explicit collision
is an error. CHECK execution order is alphabetical, after NOT NULL, rather than
source order. Both details were checked against PostgreSQL 18. NOT VALID checks
still apply to new base-value assignments. Compiled text validation ran 100,000
iterations with allocations disabled; three Debug whole-process probes took
approximately 33–43 ms including process startup. This is not an end-to-end
storage throughput result.

The public layout is generated from OpenAPI for Zig and SDKs. Its domain and
enum alternatives are checked for mutual exclusivity. Native registry ownership
is shared by the generated-layout adapter rather than maintaining a second
executable implementation. Existing enum layout serialization does not gain a
null domain field, so its executable fingerprints remain stable.

Builtin-based domain catalog creation, presentation-only comments and independent
retirement use the existing catalog transaction and retained nonce outcome.
Metadata decoder capability 117 fences the new declaration before interpretation;
the metadata writer independently validates the executable definition. Domain
columns, SQL domain casts, named-base catalog creation and dependent alterations
remain guarded. The generic model or a passing catalog campaign does not qualify
CREATE DOMAIN's complete row-use contract or dependent CASCADE.

The shared row validator now owns column bindings per immutable schema epoch
and borrows executable domain programs from that epoch's existing registry;
CHECK/default programs are not cloned or recompiled for the row plan.
It applies domain defaults before base normalization, gives explicit column
defaults precedence, checks fresh base assignments before generated dependents,
and checks generated outputs as fresh values. Domain-only tables reuse the
parsed JSON document without allocating schema-width staging arrays per row.
Sparse trusted input descriptors distinguish INSERT from PATCH, bind to the
exact destination schema version, and preserve already-domain provenance only
on submitted base inputs. Generated values cannot inherit those witnesses.
Explicit DEFAULT requests are sparse producer instructions: PATCH applies only
requested domain/column defaults and retains unrelated omissions. Raw values
cannot coexist with a DEFAULT instruction, and DEFAULT cannot carry a trusted
source-domain witness. Historical restore retains logical provenance instead of reapplying future
NOT VALID rules. Debug tests cover these shared preparation paths and exhaust
allocation failures during epoch compilation; this is not public activation or
qualification of the four remaining domain originals.

The scalar binder/VM now has a separate logical domain coercion boundary after
ordinary base conversion. It uses the row validator's native programs, captures
a complete owned ancestor registry per program, and retains independent contracts
in prepared parameter frames. Trusted same-domain NULL values remain distinct
from fresh client parameters. Domain checks run before NULL/cache fast paths,
remain lazy in CASE/COALESCE, and cannot be erased by decimal templates or
literal-input validation. Symbolic descriptors cannot execute until rebound;
conflicting definitions under one immutable identity fail closed. Text checks
need no evaluation allocation, and nested checks share the scalar work budget.
PostgreSQL 18 probes verify domain preservation for CASE/COALESCE and first-value/
lag windows, unwrapping for NULLIF/min/max, and generated domain violations before
ON CONFLICT discards an INSERT candidate. Public descriptors, relational metadata
propagation, native/distributed mutation provenance and coordinated activation
remain required; these kernel results do not qualify the four open originals.

### Required work to activate dependent domains

This boundary must be completed as one execution/activation cohort, rather than
lowering a domain to copied column constraints:

1. Retain the named identity separately from the physical base in native schema
   epochs, prepared SQL descriptors, casts and assignment output descriptors.
   Domain NOT NULL must not make an outer-join output statically nonnullable.
   Comparisons and ordinary operators use the base family; domain identity is
   preserved only at the PostgreSQL-defined common-type/coercion boundaries.
2. Carry trusted source-domain provenance from pinned SQL input/output types to
   prepared mutations, with an exact origin/generation witness checked against
   the destination epoch. Inference must come from owned compiled programs or
   native readers. JSON input and client-supplied parameter descriptors cannot
   mint that witness. Use per-expression/per-column metadata rather than widening
   every ordinary row value slot.
3. Enforce base-value coercions through the shared prepared validator for SQL,
   typed-row writes, COPY and other ingress. Preserve PostgreSQL's distinction
   between a fresh coercion from a base value and an already domain-typed value.
   A PostgreSQL 18 probe confirmed that an empty scalar subquery with a NOT NULL
   domain result can supply SQL NULL to a column of that same domain. Therefore
   unconditional domain checks on every complete row would reject valid SQL.
   NOT VALID checks also must not start checking untouched historical cells on
   an unrelated-column update. Column-level NOT NULL remains an independent
   storage invariant.
4. Compile an activation *delta* once per predecessor/successor pair. Scan for a
   newly enabled NOT NULL and newly added/validated CHECKs; skip unchanged rules
   and newly NOT VALID rules. Defaults affect future assignments. The standalone `validated_contract` mode is an invariant probe, not a
   restore API. The prepared `Activation` delta now selects newly restrictive
   rules and retains predecessor/successor identities, but it is not yet
   connected to the distributed job scanner/publication path. A full epoch publication must join all
   dependent tables and named-base definitions, stage the native contracts,
   collect current owner proofs, and atomically publish catalog, table schemas
   and the durable outcome through the existing protocol.
5. Keep physical/canonical restore verification distinct from re-running fresh
   domain coercions. Restore needs the declared immutable domain/base registry
   and provenance/activation semantics; it must not accidentally reject a
   historically admitted domain-typed NULL or bypass table constraints. Reuse
   the existing staged restore machinery and catalog identity remapping.

Originals 0306, 0307, 0308 and 0310 now have public row/default/constraint,
activation and fault/restart evidence for these boundaries. Parser, registry,
generated DTO and catalog tests alone do not establish that qualification.
Enum arrays, protected-table evolution and portable named-type catalog
restoration remain outside the completed scope.


### Prepared activation and row admission components

The native registry now stamps exact identity witnesses on executable views.
`validateCoercion` distinguishes fresh base input from trusted already-domain
SQL input, rejects stale/rebound witnesses, and preserves the PostgreSQL typed
NULL exception. This witness belongs to compiled expression/column metadata;
ordinary value slots remain unchanged. Public input descriptors do not grant
that trust.

The owned `Activation` plan compares pinned predecessor/successor contracts,
selects newly enabled NOT NULL and newly validated/changed CHECK programs in
alphabetical order, and recursively includes changed base-domain rules.
Default-only changes produce no scan work. It does not recheck an unchanged
NOT NULL against historically admitted typed NULLs. Nested ownership and
allocation-failure tests cover the full preparation path.

`domain_row.Plan` prepares domain defaults and validates actual assigned
columns while leaving untouched historical values alone. Its sparse JSON
operations share the existing parsed document/null authority, emit only absent
eligible defaults, and use epoch-bound ordinal provenance. They do not create
schema-width value/mask arrays on each row. Explicit column defaults and
generated expressions retain precedence; the runtime still needs to connect
these components at the shared preparation boundaries.

The physical codec resolves domains to immutable builtin/enum bases without
compiling logical CHECK programs into every covering layout. Repeated column
references share preparation-time physical-base lookups; covering directories
retain transitive named bases, and transforms/hash/FK paths resolve the actual
enum base. A codec test verifies unchanged base row bytes and ancestor retention.
Native schema format and semantic capability 41 admit bounded immutable domain
DAGs, validate exact physical column identities and reject old-reader admission.
Cold decoding rebuilds the domain registry; SQL column metadata owns the registry
past the producing schema lifetime. Public builtin-domain tables use compiler
coercion witnesses, canonical native verification and private versioned handoffs.
Each session owns a shared contract; raw replacements retire older authority.
The native intent boundary consumes the contract before journaling AROW bytes.
FK actions preserve untouched typed values and validate changed inputs. Portable
restore preserves historical typed NULLs and still rejects fresh invalid writes.

SQL casts, inferred domain parameters, retained wire metadata, CTE/derived/set
outputs and domain-preserving windows use the owned compiled contracts. Result
wire OIDs flatten to the physical base as PostgreSQL does; parameter wire OIDs
retain the logical domain. Unknown/base set inputs unwrap domains. Public cast
and prepared-parameter campaigns pass across cold restart. The original numeric
CHECK/text DEFAULT NOT NULL declarations and dependent DROP DOMAIN CASCADE are
qualified through PostgreSQL-backed mounted lifecycle campaigns, native FK
cascades, cold intents and portable restore. ALTER DOMAIN SET NOT NULL now uses
that same durable schema epoch coordinator: exact immutable postimages,
capability-118 admission, fenced/drained owners, bounded projected bitmap scans,
all-owner readiness, atomic type/table publication and durable release. Existing
NULLs cancel without changing the active domain; the original retained operation
reports SQLSTATE 23502 even after cold recovery. Rows and index tuples are not
rewritten. Domain nullability never changes physical column nullability. Other
restrictive CHECK/default evolutions remain guarded. Nested domain declarations
remain guarded until
catalog dependency closure and coordinated child-generation publication exist. Explicit window-default
expressions also still need deferred evaluation; demand-time coercion of fresh
unknown defaults alone does not establish general lazy-argument parity.
