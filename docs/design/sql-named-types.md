# Named SQL types

Status: implementation in progress. No original enum/domain case is qualified
by this design document or by catalog-only tests.

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
   APIs are implemented; SQL catalog column discovery still rejects enum columns until named
   SQL column/result contracts are wired end to end. Bound scalar programs now
   carry exact enum domains through expression evaluation; this is not public
   SQL column activation.
2. Complete enum expression operations, uniqueness/FK type compatibility and
   distributed integrity qualification over those public schemas.
3. Bind named type intents in SQL columns, scalar casts and parameter/result
   descriptors. Resolve by immutable identity, retain prepared witnesses and
   wire actual scalar/array OIDs through the existing public/pgwire interfaces.
4. Activate ADD VALUE across every dependent schema through the existing
   coordinated schema/owner-ACK protocol. Pending or uncertain publication must
   retain a recoverable job handle; it cannot report ready from catalog commit.
5. Add full public row/default/index/restart/restore campaigns, dependent DROP
   RESTRICT/CASCADE, domain base/default/CHECK/NOT NULL validation and activation.

The disposition ledger has not been changed for the enum/domain work. Passing
catalog/model tests alone does not establish these remaining contracts.

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
RESTRICT reports dependent objects. Public SQL column/array descriptors, named cast resolution and domain
activation remain unfinished. These changes do not
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

The reproducible PostgreSQL 18+ execution oracle contains 216 scalar probes over
three labels and SQL NULL, checking exact values and SQLSTATEs. Native campaigns
also cover disk-backed duplicate join fan-out, producer retirement, additive
common types, malformed/truncated spill, budget failure and exhaustive allocation
failures. Compatibility JSON hashing now agrees with prepared-row hashing for
enum labels, empty labels, NULL and absent fields.

These changes establish the shared enum value/expression execution layer. Public SQL type
name/column/result binding, scalar/array OIDs, named casts, distributed dependent
schema activation, domains and original enum DDL qualification remain incomplete.
No original inventory disposition was changed for these runtime probes.
