# Trigger-function definitions and execution contracts

The original definitions `sql-0405`–`sql-0410` execute unchanged through the
mounted SQL API and the shared routine catalog. This closes six CREATE cases,
not the separate table-trigger attachment and mutation-dispatch feature.
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

The unreleased combined protocol profile 135 includes the new return contract;
this does not advance the current protocol or advertise reserved version 136.
A shared producer/apply selector and a bounded structural envelope projection
require that floor before admission. Opaque body text cannot impersonate a
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

Table-trigger declarations (`sql-0751`–`sql-0753`) remain guarded. Completing
that feature requires durable table/function dependencies and owner-bound
mutation dispatch across SQL writes, document ingress, FK actions and restored
schemas. Dispatch must preserve statement-versus-row timing, the statement's
schema/catalog cut, RLS/constraint ordering and atomic effect ownership. The
function-definition qualification does not certify those unfinished paths.
