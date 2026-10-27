# Query-produced relations

`CREATE TABLE AS`, `SELECT INTO` and materialized-view population should use the
same unpublished owner, validation, publication and cancellation machinery as
restore and schema evolution. Query results are new typed values, not archived
rows. They must pass ordinary row normalization rather than historical-restore
validation.

## Implemented boundary

The compiler retains the source query, output-name overrides, persistence mode,
`WITH DATA` intent and materialized-view operation. The request owns one bounded
producer over the existing SQL read graph. It retains one result lease and one
transport page, preserving typed NUMERIC/array descriptors and the distinction
between SQL NULL and JSON null. A public result limit does not truncate a
population; scan, work, memory and spill limits still apply. `WITH NO DATA`
describes the source without opening its row cursor.

The producer preserves the existing read backend, authorization validator,
statement snapshot and RLS/filter path. A fixed-memory fingerprint of physical
source identities and parameter descriptors rejects a changed source epoch
before opening rows. This fingerprint is a definition identity, not an
attestation of a physical data snapshot.

A metadata-issued capture binds the definition, source definitions, destination
schema, principal instance, catalog revision and write stamp. Each destination
has a distinct partition binding. Query captures do not invent backup artifacts
or archived source-table identities.

An owner atomically commits each bounded row page with its delivery cursor and
chain receipt. An exact lost-ACK replay cannot append the rows again. Completion
is a separate, durable seal; unsealed output cannot validate or publish. Native
apply reconstructs the delivery digest from the original write bytes and SQL
NULL provenance before normalization. It rejects changed, missing or reordered
pages and mixed restore/rewrite commands.

The synchronous delivery adapter resolves uncertain responses through exact
read-indexed receipts. Recovery can recognize sealed output without evaluating
the query again. An importing owner, including one with zero rows, does not
prove that evaluation never started. A fresh producer is refused for that
state. Its coordinator must retire incomplete output through cancellation.

One producer routes globally numbered rows through a complete, disjoint owner
range map. Capture-bound row keys do not depend on page boundaries or owner-local
counts. Bounded per-owner buffers coalesce small input pages; encoded JSON stays
encoded until native normalization. Each owner enforces its key range at commit.
All seals bind the same global source count, and their local counts must sum to
that count before the delivery cohort is complete. Cold-reopen tests exercise
lost page and seal replies across two native LSM owners.

The metadata decoder floor is separate from native owner qualification. New
population plans require the metadata-voter capability before reservation and
atomically raise the durable data-owner capability floor with accepted admission.
Incapable stores and destination collisions cannot reserve a job or raise that
floor. Ordinary coordinated operations retain their v2 admission semantics;
ordinary restore plans retain their previous optional-field encoding and digest.

Query-produced generations now have a private purpose in the shared durable
job store. Job/name admission commits atomically. Ordinary query jobs accept
only fresh destinations; materialized refresh additionally accepts an exact,
catalog-owned predecessor through the fenced overwrite lane. The capture carries an immutable producer expiry. A worker
uses read-indexed owner status, never backup import or query evaluation: it waits
for the request's seal, cancels expired incomplete output, and sends sealed
output through ordinary global validation and atomic publication. Sealed output
remains recoverable after producer expiry. Verified EOF releases source cursors,
snapshots and spill resources before waiting for destination publication.

Metadata validates each owner's cursor binding and common source count atomically
with its import receipt. A per-target durable accumulator rejects inconsistent counts,
double-counted replies and incomplete totals before entering validation. This
uses constant work per receipt rather than rescanning the full owner cohort.
Voter capability activation is observed durably before compound admission;
readiness checks alone do not satisfy the apply-side floor.

The public adapter now activates ordinary permanent `CREATE TABLE AS` and
`SELECT INTO` through shared destination planning and compound admission. It
checks namespace/tablespace placement, destination authority and source access.
An admitted capture nonce must match before the request can deliver rows. A
conditional create skips a committed existing relation before source evaluation;
a pending name reservation remains a transition, not a successful conditional
create. Catalog epoch checks protect admission, and immutable database,
namespace and tablespace fingerprints protect publication. Durable rejected jobs
retain cancellation tombstones so delayed requests cannot revive them.

The request returns an owned job receipt if publication remains pending or an
acknowledgement becomes uncertain after sealing starts. The worker rechecks
stored authority and the submitting user's immutable instance before publishing.
The HTTP path observes publication for a bounded interval; pgwire uses its
statement deadline. Population cannot join an explicit SQL transaction yet.

The recovery campaign runs actual SQL producers through metadata Raft and native
LSM owners with lost replies and cold reopen. It checks complete typed postimages
and SQL-null provenance, empty publication, and hidden partial-output
cancellation. The mounted HTTP campaign executes four unchanged original
statements against both document and relational sources, comparing complete rows,
nullable text columns and conditional-repeat behavior with a checked-in,
input-hash-verified PostgreSQL reference. WITH NO DATA and conditional repeats
perform no source reads. These close sql-0364, sql-0365, sql-0366 and sql-0567.

## Catalog-owned materializations

Permanent `CREATE MATERIALIZED VIEW` now uses that same bounded producer and
staged job lane. Its descriptor lives in the existing view catalog, with a
logical view identity and durable source/output attributes. A separate immutable
physical generation owns the rows. Metadata publishes the descriptor, source
attribute bootstrap, dependency edges, backing table and ranges in one
transaction. Reverse physical bindings distinguish tables from materializations
without scanning the catalog. Legacy ordinary-view records retain their bytes.

The admission capture binds the logical owner, principal instance and exact
physical output layout. Metadata decoder v75 and native owner protocol v4 are
required before reservation; rejected mixed-version attempts do not raise the
data floor. Table records preserve backing ownership through binary projections,
cloning and snapshots. The compiled metadata client forwards planning through the
same native method, including its deadline. Native ABI v77 added an append-only
write-rejection status while preserving main's published numeric identities;
v78 qualifies the refresh planning projection.

Reads authorize the logical relation and use the existing SQL read/filter path
over its bound physical generation. They verify that the descriptor still names
that backing. A populated generation may contain zero rows; an unpopulated
`WITH NO DATA` generation instead reports PostgreSQL SQLSTATE 55000. Source
cursors are never opened for that declaration. Both ordinary and transaction
write paths enforce backing immutability under the native apply fence, including
cold reopen; staging import retains its dedicated admission path.

`DROP MATERIALIZED VIEW` checks object kind and reuses the existing dependency
closure and topology retirement transaction. It removes descriptors, dependency
edges, backing tables and placements together. Physical reader ownership and
cleanup continue through the shared native lifetime machinery. Ordinary
`DROP VIEW` and row mutations cannot change a materialization.

The mounted campaign closes sql-0351, sql-0352, sql-0353, sql-0358 and sql-0359
against document and relational sources. The PostgreSQL reference now covers
eleven unchanged originals, plus populated/unpopulated read and mutation
diagnostics. Native metadata tests verify mixed-version rejection, forged owner
and output-layout rejection, publication, strict binary framing, snapshot
recovery and retirement. The broader Debug gates also qualify existing ordinary
views and mixed document/relational restore cohorts.

## Fenced materialized refresh

Ordinary permanent `REFRESH MATERIALIZED VIEW` evaluates the stored definition
through the existing owner/delegation, source-column identity and RLS/filter
machinery. A request-owned preparation factory binds one bounded producer after
the logical view has been resolved. It is released before its arena and is never
serialized into recovery state. Compatible source renames use durable column
identities rather than rebinding by name.

Planning point-reads the expected logical identity and generation, then obtains
its bounded indexed physical range set. It preserves the output schema and
placement declaration while allocating a new physical generation. The admitted
plan binds both predecessor and successor, the unchanged definition and exact
logical/output identities. Refresh requires metadata decoder v76 before
reservation; creation remains v75 and the native owner protocol remains v4.

After producer EOF releases source readers, the shared overwrite worker validates
and seals output, fences the old owners and atomically publishes the new backing.
The logical view, owner, generation, dependencies and output attribute identities
remain stable. `WITH NO DATA` publishes an unpopulated generation without opening
a source cursor. Incomplete production retains the ordinary cancellation and
receipt-recovery rules; workers never repeat query evaluation.

Namespace reconciliation permits only the descriptor's current backing or the
exact successor proved by its immutable admitted plan. A pending successor keeps
the predecessor's logical name readable. Snapshot validation covers that pending
state as well as publication and retirement; unrelated reservations cannot shadow
a view. The mounted native campaign runs lost-reply/reopen recovery across both
document and relational sources, including populated and initially unpopulated
refreshes, a subsequent `WITH NO DATA`, complete PostgreSQL postimages and stable
logical/output identities. This closes sql-0356. The original sql-0357 combines
`CONCURRENTLY` with `WITH NO DATA`: PostgreSQL and the public API agree on 42601
for a populated view and 0A000 for an unpopulated view, with unchanged data and no
producer admission. This negative original does not qualify successful concurrent
refresh.

## Activation still required

- Explicit transaction participation for population and its catalog publication.
- Successful concurrent refresh: activate materialized index DDL, prove an
  eligible ready unique index and finish concurrent-reader isolation. Current
  positive `CONCURRENTLY` attempts report the missing-index prerequisite; this
  is not eligibility checking. Broader MAINTAIN/role-ownership semantics also
  remain open. Nonconcurrent in-flight reads may observe a generation change.
- Materialized index DDL and coherent backup/restore of descriptors and their
  backing generations alongside ordinary tables. Archive restore must not
  fabricate a query population or publish an unbound physical backing.
- Connection-owned TEMP lifetimes and UNLOGGED crash semantics. Neither may be
  silently implemented as an ordinary global durable table.
- Broader public fault campaigns for live authority/RLS revocation, namespace
  changes during population, mixed-version placement and multi-owner cutover.
  The existing recovery tests cover lifecycle components; they do not replace
  these end-to-end qualification cases.

PostgreSQL completion tags distinguish populated output (`SELECT n`) from
`WITH NO DATA` or a conditional no-op (`CREATE TABLE AS` or
`CREATE MATERIALIZED VIEW`). A population returns no DataRows, so its completion
count must come from the destination-row count. Refresh reports
`REFRESH MATERIALIZED VIEW`. The private runtime completion count is excluded
from the public JSON result envelope.
