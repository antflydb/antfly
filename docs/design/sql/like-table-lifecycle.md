# LIKE table creation and ownership lifetime

`CREATE TABLE … LIKE` is a declaration-copy operation on the ordinary table
creation path. It does not copy rows, policies, grants, table comments or foreign
keys. Options select defaults, generated expressions, identities, constraints,
indexes and column/object comments. Sources are authorized and pinned by native
table identity and exact definition; `IF NOT EXISTS` for an existing destination
finishes before resolving a missing source.

SERIAL and identity columns have different ownership contracts. Copying a SERIAL
default borrows the same early-bound counter, with ordinary counter privileges
required at allocation. Copying an identity creates a new independently owned
counter from the source's current configuration, not its running state. The
accepting metadata transaction resolves those effects through guarded source
attributes. It publishes the new table, attribute registry, counters, ownership
edges, reverse default dependencies and copied annotations together. No
post-CREATE mutation adopts or repairs those identities.

A default reference is independent of an OWNED BY edge. Rename and generation
replacement preserve that distinction through explicit attribute handoffs.
TRUNCATE RESTART IDENTITY resets only counters owned by the truncated table;
borrowed counters continue. These paths use the same schema preparation and
publication machinery as ordinary tables.

The reverse default directory makes source-retirement admission proportional to
the source's owned counters and indexed dependencies. Ordinary source DROP must
reject while another table borrows its owned counter, preserving all schemas,
rows, defaults and ownership edges. Dropping the borrower removes its references
and its own identity counter, without resetting or deleting the shared SERIAL
counter. Source DROP can then retire its counters and outgoing FK references
through the existing whole-table publication protocol. A fresh table with the
same name receives a new immutable table identity.

The unchanged PostgreSQL lifetime oracle follows exactly that RESTRICT → borrower
DROP → source DROP sequence. Owning-table CASCADE with surviving borrowers is a
separate contract (`sql-0158`), not a prerequisite of LIKE (`sql-0369`). A prior
implementation checkpoint incorrectly coupled those scopes. Full catalog native
snapshots preserve annotations; portable per-table annotation export remains
separate work.

## Qualification

The strict Debug gate `antfly-api-sql-definition-copy-test` selects both the
original declaration-copy campaign and the coupled ownership-lifetime campaign.
The original campaign executes the immutable `sql-0369` statement and verifies
typed rows, constraints, mixed LIKE sources, comments, permissions, excluded RLS,
SERIAL/identity behavior, attribute renames and TRUNCATE across independent cold
metadata, data and session owners. The second campaign combines identity,
SERIAL, generated values, deferred UNIQUE, CHECK, NOT NULL and an excluded FK;
its source now uses a single public CREATE with counter and FK declarations,
through the [atomic hidden-child publication](initial-fk-counter-publication.md).
The test checks
source RESTRICT before and after cold reopen, unchanged durable SERIAL
state after borrower retirement, source-counter retirement, surviving-parent
writes, and safe source-name reuse. No admitted DROP is replayed: observation
waits for the accepted operation's catalog effect.

`scripts/generate_sql_population_reference.py --persistence --check` independently
checks all nine unchanged persistence originals against PostgreSQL 18+, including
LIKE ownership semantics and clean/crash restart postimages. Native annotation,
creation-source and publication gates supply the adjacent fault, ownership and
accepting-transaction proofs. These are correctness and lifecycle checks, not a
claim about distributed latency or arbitrary fault interleavings.

On 2026-10-31 the pinned Zig 0.17 Debug gate passed both campaigns with zero
skips, failures or leaks, including final native checkpoint validation. The
PostgreSQL 18.6 persistence oracle passed unchanged. This qualifies `sql-0369`;
it does not relabel other unresolved originals or activate owning-table
CASCADE. The subsequent decoder-128 cohort also qualifies combined initial
FK/counter creation, with the same source and lifetime checks.
