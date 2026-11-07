# Trigger-function definitions and execution contracts

The original definitions `sql-0405`–`sql-0410` execute unchanged through the
mounted SQL API and the shared routine catalog. This closes six function CREATE cases. Original table-trigger attachments
`sql-0751`–`sql-0753` also have a separate mounted lifecycle qualification below.
PostgreSQL 18 is the independent oracle.

## Catalog and return contract

A trigger function remains a function resource in the ordinary overload
namespace. Its declaration has a separate `returns_trigger` contract rather
than pretending a trigger row is a scalar value or a procedure's void result.
The contract requires PL/pgSQL, no declared arguments, and no scalar result.
CREATE validates procedural syntax without resolving PERFORM's SQL names.
Ordinary SELECT/CALL execution cannot use the trigger invocation ABI.

Create, replace, comment and drop use existing scope authorization, immutable
identity/generation fencing, atomic metadata transactions, durable nonce
outcomes and catalog recovery. Replacement preserves the return contract and
owner. Comments preserve executable generations. No parallel catalog, job
system or retry lane is introduced.

Metadata profile 135 introduced the return contract. Table attachment admission
requires current metadata profile 174 and native data-reader capability 7.
A shared producer/apply selector and a bounded structural envelope projection
enforce the required floor before admission. Opaque body text cannot impersonate a
contract field. False flags are omitted from ordinary encoded definitions.

## Compiled execution

The existing bounded procedural VM executes both procedures and trigger
functions. Supported trigger bodies use whole-row RETURN NEW, RETURN OLD,
RETURN NULL, literal RAISE NOTICE and scalar PERFORM. Other body forms retain
explicit admission guards. Each statement owns its compiled step directory and
its lazily bound scalar programs; no mutable plan is shared across requests.
Each PERFORM binds once on first demand. RETURN leaves unreachable SQL unbound.

An explicit trigger context supplies the event, row/statement level and borrowed
OLD/NEW images. Invalid event/image combinations fail before effects. Return
values retain image identity rather than reconstructing JSON or copying rows.
The 10,000-invocation Debug probe requires zero row-memory allocations. The
mutation owner must pin both images for the invocation's lifetime.

Both invocation forms share the same execution loop, cancellation checkpoints,
scalar/sequence/decision providers, effect budgets and notice delivery. A
trigger body that reaches END without RETURN raises PostgreSQL SQLSTATE 2F005.
The caller owns returned notices in its statement arena. Ordinary SQL calls
receive 0A000; Antfly's existing HTTP mapping represents that SQLSTATE as 501.
Invalid language and declared-argument errors retain specific 42P13 diagnostics.

## Qualification and remaining activation

The PostgreSQL fixture executes all six unchanged CREATEs, checks negative
calls/definitions, and attaches row triggers to observe NEW/OLD/NULL behavior
and first-invocation resolution failures. Adding both audit_log overloads makes
the deferred calls executable without replacing their trigger functions.
Native VM tests exercise those same original bodies, lazy binding, cancellation,
notices, event nulls and the allocation-free return path. Allocation-failure
sweeps cover owned definition decoding, preparation and execution cleanup.

The mounted native campaign checks authorization, replacement, comments, actual
cold metadata recovery, drop/recreate and a lost reply after real mutation
commit. The native metadata campaign checks predecessor capability rejection,
exact request replay, stable identities and cold body generations. Ordinary
function, procedure and prepared-manifest campaigns remain regression gates.

The unchanged table-trigger declarations `sql-0751`–`sql-0753` now pass their
own authenticated mounted SQL campaign and independent PostgreSQL oracle.
CREATE retains the omitted FOR EACH ROW statement timing; REPLACE preserves
attachment identity; DROP retires dispatch. The campaign checks TG_ARGV,
complete postimages, notices, demanded division-by-zero rollback and subsequent
schema publication, then repeats observations after a full native catalog/data
restart. The combined Debug qualification passes all 103 steps without skips or
leaks. Physical table IDs remain opaque; SQL relation OIDs and table/function
reverse edges come from the same durable catalog cut.

Document ingress, FK actions, native population and distributed restore retain
additional fault-campaign requirements. Their implementation must preserve the
statement's schema/catalog cut, RLS/constraint ordering and atomic effect
ownership; the three attachment originals do not certify those broader paths.

## Attachment execution boundary

Table attachments reference immutable routine IDs. Their canonical, bounded
`sql_triggers` schema directory has a table-local generation and monotonic
attachment ID high water. Replacing an attachment preserves its ID and advances
its generation; dropping and recreating a name must not revive an old binding.
Routine bodies stay in the ordinary routine catalog. A statement captures the
current body generation, so function replacement affects later statements while
an admitted statement keeps its executable cut. Moving a function to another
namespace does not change the attachment's immutable routine identity.

The statement manifest carries the real table ID/schema version/schema digest,
attachment generation/digest, metadata incarnation/revision, captured routine
closure and owner-issued statement/transaction clocks. Its canonical digest
identifies data; it is not authorization. The private mutation receiver must
independently authenticate the statement producer and fence its real pinned
schema before any trigger program runs. The typed-row domain handoff does not
provide executable authority and is not reused as a trigger permission token.

The bounded dispatcher has no table read/write, sequence allocation, network or
ambient catalog ports. Scalar helper resolution is confined to the captured
routine closure. Copy-on-write row assignments modify only an unpublished,
owned candidate; the original MVCC images stay unchanged. One statement budget
covers all invocations, rather than granting fresh work credit to every row.
PostgreSQL's name ordering and explicit UPDATE target semantics determine which
attachments fire. Statement phases run once at the coordinator, including for
an empty affected-row set; they do not run independently on each shard.

The mutation preparation order is defaults, BEFORE ROW candidate transformation,
generated columns, final RLS WITH CHECK and constraints, transaction-owned AFTER
ROW/STATEMENT execution, then atomic row/effect commit. An AFTER failure still
rolls back the statement; it must not run as a post-commit callback. USING admission checks the original
row, independently of the transformed candidate. Physical restore/replay must
verify the attachment/schema and routine dependency closure without firing
triggers again. A normal mutation after restore uses the restored attachment
and freshly captured executable authority.

A typed absent NEW/OLD image keeps the table tuple descriptor: known fields read
as SQL NULL, unknown fields still raise 42703, and local assignments can materialize
a record. Statement/AFTER return images are ignored. TG_ARGV is text[] with a
zero lower bound, not a PostgreSQL array with the usual lower bound of one.
Statement WHEN permits scalar conditions but cannot reference row fields; TG_*
variables belong to the function frame, not the WHEN expression namespace.

The three original attachment cases are qualified by the mounted lifecycle
campaign, native reverse-edge/catalog tests and PostgreSQL reference. Broader
all-ingress and distributed restore campaigns remain additional release gates.
The producer continues to enforce actual receiver capabilities. UPDATE OF uses
durable attribute identities and dependency-aware rename/drop; neither column
name hashes nor current physical positions are identity authority.

Closure capture uses the statement's metadata revision and typed frame. It must
preserve the procedural VM's lazy RETURN rule: never resolve a later PERFORM
that cannot execute after an unconditional RETURN. Within a reached scalar
statement, normal SQL binding still resolves field/function names in dead CASE
branches, as PostgreSQL does. Capture and receiver preparation must use the same
bounded scalar binder rather than separate textual function-name extraction.
An unknown-column error crossing the native storage ABI must retain SQLSTATE
42703 through its registered native identity; a transport must not collapse
it to a generic internal error.

### Statement phases across writable CTEs

PostgreSQL 18 coalesces statement trigger phases by physical target relation
and event across one writable-CTE command. Two UPDATE substatements affecting
different rows of the same table produce one BEFORE UPDATE and one AFTER
UPDATE, including when both substatements affect zero rows. They do not create
one statement trigger per CTE or per shard.

The first UPDATE's target-column set controls BEFORE UPDATE OF admission;
the last completed UPDATE's target-column set controls AFTER UPDATE OF. AFTER
phases follow actual substatement completion order: an INSERT reading a writable
UPDATE CTE queues the UPDATE's AFTER phase before its own INSERT phase. An
independent INSERT can complete before an otherwise unused UPDATE CTE.

The independent PostgreSQL witnesses and unchanged SQL are recorded under
`statement_scope` in `sql_trigger_attachment_reference.json`. The executor's
optional logical-step begin/end hooks preserve these boundaries. The coordinator
owns shared per-table/event BEFORE state and replaces queued AFTER state on
substatement completion; shard receivers only verify the resulting prepared rows.
