# Copied counters during initial foreign-key publication

A hidden FK-bearing LIKE creation uses the ordinary guarded column-counter
factory. It captures the resolved source binding and counter definition for each
copied column, alongside the destination scope and immutable counter postimages.
The metadata accepting writer independently resolves those sources and derives
the same bytes. Neither compiler-supplied IDs nor a portable postimage grants
counter authority.

Identity copies create a fresh owned counter from source configuration. They
copy the configured start, increment, bounds, cache and cycle settings, with the
destination column's integer width. They never copy running values or require
the source table to remain alive after admission.

SERIAL defaults retain a reference to the existing counter. They allocate no
counter ID, name, state or ownership edge. An all-borrowed plan is valid: its
counter postimage contains no newly created sequences. It still consumes the
ordinary table allocator slot and reserves the child through the same native
initial-FK protocol.

## Shared definition and lifetime leases

Each borrowed counter has a resource-indexed lease for the hidden publication.
The key binds the counter, destination table and publication ID. The fixed value
binds the metadata incarnation and immutable counter-effects digest. It uses the
effects digest rather than the full plan digest because parent support sealing
changes the physical publication witnesses. The complete plan-envelope check
keeps the counter effects immutable during that seal.

Multiple publications can share a counter. Borrowed IDs are deduplicated in a
fixed 256-element buffer, so cleanup requires only the accepted plan's bounded
membership. A conflict check performs one prefix seek; it does not enumerate
unrelated tables, user rows or all readers.

A lease freezes descriptor identity/configuration and retirement. Allocations
and value operations remain independent and durable. Admission refuses an
already admitted counter retirement or an owner generation/rewrite which may
handoff that counter. Metadata source-table locks are not used as surrogate
counter leases: direct counter DDL and source-owned retirement must see the same
resource protection.

Native counter DDL, sequence retirement, source-owned retirement, column handoff
and topology drop check the lease before admitting effects. Shared catalog
resource mutation/removal provides a final guard too. Identical descriptor
reindexing and independent presentation metadata remain possible. A source
retirement cannot delete its rows, outgoing references or annotations before
discovering a counter lease.

Final publication writes the child and all live default edges before releasing
its leases in that same metadata transaction. New identity counters and copied
annotations join that transaction as before. Cancellation releases leases
without deleting, resetting or rewinding borrowed counter state. Lost replies
and duplicate terminal commands use the existing publication/revision receipts.

## Streaming recovery closure

Checkpoints and Raft images apply the same verifier. Native iteration streams
catalog rows, counter rows and initial publication rows. It retains one decoded
plan at a time rather than a whole-catalog map.

Every copied-counter publication must be stored under its declared child key,
carry its complete plan digest, and have a supported activation with nonempty
membership and the current metadata incarnation. For every nonterminal plan,
verification proves each exact lease
key/value and its frozen source descriptor. Running values are deliberately not
part of that comparison. A reverse pass validates and counts lease records.
Exact forward membership plus equal reverse cardinality proves there are no
missing, orphaned, wrong-owner or terminal leases, without decoding a plan again
for every edge. Source definitions for identity copies and completed plans do
not become continuing dependencies.

Cohort 129 covers the private copied-source fields and retained plans. Existing
live default directories remain the authority for published borrowers; hidden
leases do not manufacture public table or default rows.

## Default effects and row constraints

SQL binding admits a literal NULL independently of a column's NOT NULL row
constraint. Preparing the statement consumes no defaults. At execution, explicit
NULL remains distinct from an omitted default, including through INSERT VALUES
and INSERT SELECT encoding.

Hosted mutation preparation uses the shared typed candidate/selected pipeline for
counter defaults, just as it does for domain-aware rows. Candidates allocate
missing defaults; selected rows compute generated values and check constraints.
The validator is cached by schema epoch, but each ordinary input still requires
its own candidate preparation. Inputs already prepared for a filtered INSERT
skip that phase once, preserving default effects even when selection rejects a
row. A batch completes one row before executing any default of the next row.
The existing scoped owner view, schema verification and mandatory row filter
remain in force.

This avoids a second manual default loop and the generic physical normalizer's
type error for explicit NULL. Sequence values remain durable when a later row
constraint rejects the insert. This does not claim complete effect-order parity
for UPDATE DEFAULT, MERGE or every source-expression failure.

## Qualification and remaining work

The strict `antfly-metadata-copied-counter-test` passes five Debug campaigns with
zero skips, failures or leaks: all-borrowed cold publication, cancellation with
allocated values, fresh identity publication after source retirement, fresh
identity cancellation/name reuse, and multiple cold lease owners. The real native
campaigns reject missing, orphaned and wrong-effects leases, plus a valid
publication moved under another child key, through checkpoint
verification. They also cover descriptor mutation rejection, live reference
counts, source drop preflight, allocation failures and duplicate publication.
The broader initial-FK suite passes 28 tests plus placement.

The public lifetime campaign combines copied identity/SERIAL defaults, generated
columns, deferred UNIQUE, CHECK, an explicit FK and independent cold owners. Its
current qualification also checks PostgreSQL's failed-insert sequence effects:
PREPARE performs no allocation, separate EXECUTEs have separate effects, and
NOT NULL/CHECK/UNIQUE rejection does not rewind sequence values. Successful
multi-row inserts allocate every row independently with a cached validator. A
three-row insert failing on its second row allocates exactly twice, publishes no
rows and never evaluates its third row; the surviving rows and counter values
are checked after cold restart.

Final pinned-Zig Debug API qualification passes 11 tests across declaration
copying, domain rows, domain session/distributed handoffs, ordinary default
changes and acknowledgement/retry contracts, with zero skips, failures or leaks.
The final native run passes the 28-test initial-FK suite, placement and the strict
five-test copied-counter subset. The SQL suite passes all 1,904 tests with only
its four existing opt-in benchmark skips. `make fmt`, the 21 inventory-checker
tests and license/Apache boundary checks pass. The committed PostgreSQL 18.6
copied-counter fixture regenerates and checks independently.

The independent PostgreSQL witness records both the counter lifetime and the
postimages for owning-table CASCADE. The latter remains an implementation target,
not a claim that Antfly's table CASCADE is activated. No additional immutable
original is credited merely for this combination.
