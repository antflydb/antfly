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

1. Bind SQL enum columns to schema-owned definitions through the public schema
   compiler and immutable catalog dependencies. Current SQL scalar paths reject
   enum columns rather than treating symbols as integers or labels as TEXT.
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
