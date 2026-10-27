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
job store. Job/name admission commits atomically, and query jobs accept only
fresh destinations. The capture carries an immutable producer expiry. A worker
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

The recovery campaign runs actual SQL producers through metadata Raft and native
LSM owners with lost replies and cold reopen. It checks complete typed postimages
and SQL-null provenance for populated output, empty `WITH NO DATA` publication,
and hidden partial-output cancellation. Separate metadata tests qualify compound
admission collisions, unsupported data owners and exact count accumulation
across reopen. These are lifecycle component tests, not mounted public case
closure evidence.

## Activation still required

The public catalog adapter deliberately rejects population commands until the
complete lifecycle is integrated. Parser and component coverage do not close
original parity cases. In particular, the following are unfinished:

- Public destination planning, transaction participation, namespace/tablespace
  and placement checks,
  conditional-create semantics, and connecting the request-owned producer to
  compound admission. A retry must inspect the persisted capture nonce before
  adopting output; it must never attach a new evaluation to an existing job.
- Materialized definitions, bound dependencies and owner authority in the
  existing view catalog; atomic backing-generation publication and retirement;
  unpopulated-read errors; refresh isolation, MAINTAIN checks and concurrent
  refresh prerequisites.
- Connection-owned TEMP lifetimes and UNLOGGED crash semantics. Neither may be
  silently implemented as an ordinary global durable table.
- Mounted public SQL/pgwire campaigns for unchanged originals, complete
  postimages, authority/RLS, publication failures, cancellation, cold recovery
  and mixed document/relational inputs, compared with independent PostgreSQL.

PostgreSQL completion tags distinguish populated output (`SELECT n`) from
`WITH NO DATA` or a conditional no-op (`CREATE TABLE AS` or
`CREATE MATERIALIZED VIEW`). A population returns no DataRows, so its completion
count must come from the destination-row count. Refresh reports
`REFRESH MATERIALIZED VIEW`. The private runtime completion count is excluded
from the public JSON result envelope.
