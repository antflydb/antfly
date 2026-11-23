# Guarded definition copying during initial FK creation

A FK-bearing `CREATE TABLE (... LIKE source ..., FOREIGN KEY ...)` uses the
existing hidden-child publication. Copying a definition does not require a
second CREATE/ALTER, a second coordinator, or holding the source's schema lock
until every owner acknowledges the child.

The public compiler supplies a bounded set of identity-bound source guards and
the catalog revision from the same capture. It also identifies which source
column comments and index/constraint declaration slots should be copied. These
are trusted compiler intents, not caller-supplied destination attribute IDs or
annotation bytes.

Metadata preparation assigns the destination identity and generated names,
binds reserved self references, validates each current source guard and derives
fresh destination attribute/object identities. It captures annotation postimages
through the same column and object annotation machinery used by ordinary CREATE.
The immutable initial-publication plan contains the source intents and those
postimages. Owner bootstrap receipts therefore attest the plan containing the
copied effects as well as the physical child schema.

Native BEGIN independently checks the source guards and derives the same complete
postimage before its first persistent write. Catalog revision alone is insufficient:
point comments deliberately do not advance it. A changed source note defeats the
postimage comparison. A race between preflight and committed BEGIN becomes a
specific pre-write nonadmission; malformed effects and storage failures still
fail apply. No child/name/counter reservation may escape that failed admission.

After acceptance, copied definitions and notes are owned values. They no longer
need their source table to exist. Owner placement, cold recovery, support sealing,
publication and cancellation never replay the source SQL or reload later source
comments. Support sealing preserves the exact creation intents and all counter
and annotation effects while updating only its existing support witnesses.

Final publication commits the table, ranges, catalog binding, attribute registry,
object directory, copied notes and existing explicit-counter effects together.
Captured notes stay private inside the plan until that transaction. Cancellation
never materializes destination annotations. Subsequent schema changes and backup
use the ordinary annotation registries and stable destination identities.

Portable validation reconstructs the fresh destination registries from the sealed
schema. Annotation text may vary, but attribute IDs, signatures, schema epochs,
object IDs and declaration directories may not. Duplicate or unknown object notes
are rejected. Serialized comparisons reclaim allocations on either outcome.

Decoder cohort 129 protects the new private plan fields at service admission,
native BEGIN/support sealing and checkpoint validation, including retained
terminal plans. Empty fields are omitted from older plan shapes. No public route
or OpenAPI type is added.

## Continuing references remain a separate requirement

A copied SERIAL default retains its original counter. The
[shared counter lease protocol](initial-fk-counter-dependencies.md) now protects
that reference during the hidden interval and converts it atomically to the live
default edge at publication. Cancellation releases the lease without rewinding
allocations. Copied identity configuration uses independently sealed source
inputs and creates a fresh counter; it does not copy running state or retain a
source lifetime dependency. Source table locks are not surrogate counter leases:
direct counter DDL and source-owned retirement use the same resource guard.

This cohort does not close owning-table CASCADE, role-owned initial publication,
portable per-table annotation export or arbitrary multi-node fault qualification.
It does not add original-case credit for an auxiliary combination of features.

## Evidence

The strict `antfly-metadata-initial-copy-test` drives actual native owner provision,
release and cancellation. It tests unchanged metadata after a raced source note,
source retirement while the child is hidden, cold metadata recovery, private
annotation state, final copied notes, allocation-failure rollback and duplicate
publication after counter allocation. Its two campaigns pass in pinned Zig Debug
with zero skips, failures or leaks.

`antfly-api-sql-definition-copy-test` includes authenticated public FK-bearing LIKE
creation, copied primary-index/column notes, default values and FK/UNIQUE/CHECK
violations, followed by independently cold owners. Both authenticated campaigns pass in pinned Zig Debug with zero skips, failures
or leaks. The broader initial-FK gate passes 23 tests plus its placement test,
including rejection of a valid-but-differently-owned support seal. All 1,904 SQL
tests pass, with only the four existing opt-in benchmark skips. The 19 standalone
wire-classification tests pass too.

`scripts/generate_sql_initial_copy_reference.py` independently executes the
PostgreSQL 18 witness in a disposable database. The committed fixture records
invisible precommit state, copied notes after source retirement, generated/default
values, constraint SQLSTATEs and rollback absence. Generation and check both pass.
