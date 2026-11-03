# SQL lock ownership

This is an implementation record for explicit SQL locking. The PostgreSQL 18
conflict matrices and lifetime rules are the semantic reference.

Current status (2026-11-03): public HTTP and pgwire advisory execution is active
and qualified against the three unchanged original advisory statements. The
native ledger, durable command-source permits, shared prepared-decision
integration, independent prepared lock home, ordered scalar/type layer,
savepoints and DISCARD share native lifetime authority. Distributed row/table
participant enrollment remains open; the seven original table/row locking cases
are not closed by advisory coverage.
The sections below retain earlier qualification records; the final sections
describe the current implementation.

## Native resource contract

`storage/pessimistic_lock_contract.zig` defines the eight table, four row and two
advisory modes. Resource identity includes the catalog incarnation. The bigint
and two-int advisory key spaces use different domain tags, including for negative
values with identical underlying bits. Aggregate mode counts count distinct
backends; a backend's session refcounts remain separate from its transaction
holdings. Conflict checks subtract only the exact backend identity and fence.

A grant has a fixed 234-byte canonical representation with a checksum. There is
one transaction slot per mode, recording the oldest live subtransaction ordinal.
Savepoint release reparents those ordinals; rollback releases only the aborted
subtree. Session advisory refs survive rollback. `unlock_all` clears session refs
without releasing transaction locks. PREPARE rejects mixed session and transaction
ownership of the same resource, even across different modes, as PostgreSQL does.
Transaction-only grants transfer to an independent prepared backend. Grants on
unrelated session-owned resources remain on the live connection.

## Transaction-local journal

`storage/pessimistic_lock_ledger.zig` runs within the accepting native writer
transaction. Its caller must abort that transaction on **any** error. Registration,
read-only observation, effect admission and prepared handoff require independent
caller-supplied authority checks. They are not optional hooks or client claims.
The eventual native receiver must implement them against actual connection,
transaction and external-decision authority.

Each backend has a monotone effect sequence. A frame binds that sequence, a nonce
and its complete body. The journal retains 64 exact terminal receipts. Replaying a
successful acquisition cannot increment a refcount again. Replaying a failed try
cannot change it into a newly successful acquisition. An expired receipt is never
permission to admit its serial again. Catalog incarnation checks also apply when
the requested owner record is absent.

A blocked acquisition persists its exact frame and a doubly linked resource wait
node. New conflicting arrivals respect the queue. An already held mode remains
reentrant; upgrades precede waiters blocked by the upgrader's existing grant.
Compatible queue admission uses aggregate mode counts without reading every
waiter. The operation-count regression verifies six point reads and no cursor
for that path with eight queued backends.

Cancellation seals the same exact effect. If cancellation wins before the original
RPC arrives, a durable cancelled receipt prevents that late RPC from acquiring.
If acquisition already won, cancellation returns its original grant receipt; it
does not compensate by unlocking a potentially shared session reference.

Advisory deadlock analysis uses the current native writer view. Held-lock edges
and queue-priority edges are distinct. A queue-only cycle is reordered and checked
again, rather than reported as an unavoidable deadlock. A cycle consisting of held
conflicts produces a durable `deadlocked` outcome. It does not silently release
the victim's previously owned session locks. The SQL owner must perform the
appropriate transaction abort and cleanup. A truncated graph yields a resource
limit error, never a false deadlock proof. This single-home graph does not replace
the distributed participant proof protocol required for row/table locks.

An owner is limited to 1,024 resources; a resource to 256 holding backends and 128
waiters. Graph traversal is limited to 64 vertices and queue reordering to 32
attempts, with a cumulative 2,048-probe budget across those attempts. The limits
do not multiply into an unbounded apply-lock loop. The source writer aborts on
planning exhaustion, leaving the attempted
admission absent and earlier waiters intact.

## Resumable lifecycle

Close, rollback, savepoint release, session unlock-all and prepared handoff pin a
control job on the source owner before changing grants. Other effects cannot
interleave on that owner. Transaction rollback, savepoint release and PREPARE use a
separate transaction membership index and compact counter; ending a transaction
with no transaction locks visits no session resources. Close and session unlock-all
use the backend membership index. Each quantum seeks one resource through the
selected index and changes at most two grants, one aggregate and fixed progress
records. The final receipt is published only after the indexed cohort is complete.
A quantum does not scan the global catalog or materialize all held resources.

Prepared handoff reserves an independent incoming owner. Every transferred grant
remains held throughout the source-to-destination change. Partial progress leaves
that owner non-executable until finalization; ordinary effects cannot adopt its
locks. Source and destination ownership, aggregate counts and the source cursor
change in the same native transaction. Closing a retired backend cannot resurrect
its old authority. Bounded terminal-owner reclamation still needs the authoritative
connection-generation/decision retirement floor before these records can be deleted.

## Connection command admission

`api/sql_connection_admission.zig` serializes commands on an authenticated HTTP
logical connection. It uses `std.Io.Mutex` only around a bounded active map, never
while executing or waiting. The key includes the credential principal, so another
credential cannot occupy the owner's execution slot. A fresh request must still
reload and validate the durable connection after admission. This lease does not
confer native lock or transaction authority. A copied old release cannot release
a newer execution. Pgwire already owns its command loop.

HTTP statement execution, prepared resource operations and connection close share
this admission. Overlap is a known pre-execution rejection, reported as SQLSTATE
55006. Sequence effect ownership remains distinct: its cache is not reused as a
connection execution mutex or as durable advisory ownership.

## Qualification and activation boundary

Focused Debug gates:

- `antfly-pessimistic-lock-contract-test`: conflict matrices, fixed codec, reentrance,
  subtransaction lifetime and prepared detach semantics.
- `antfly-pessimistic-lock-ledger-test`: exact effects, cancellation races, queued
  fairness, hard and soft cycles, bounded traversal, handoff and failed staged writes.
- `antfly-pessimistic-lock-native-test`: actual LSM/WAL reopen at every handoff
  quantum, failed native staged writes, immutable prior readers, lost terminal
  receipts, persisted queues/cancellation and cold hard/soft cycle decisions.
- `antfly-sql-connection-admission-test`: bounded execution leases and stale release.
- `antfly-api-sql-connection-admission-test`: mounted public connection commands.

Independent isolated PostgreSQL oracles:

```
ANTFLY_PG_BIN=/opt/homebrew/opt/postgresql@18/bin \
  uv run --no-project --with 'psycopg[binary]==3.3.6' \
  python scripts/check_sql_lock_postgres.py
ANTFLY_PG_BIN=/opt/homebrew/opt/postgresql@18/bin \
  uv run --no-project --with 'psycopg[binary]==3.3.6' \
  python scripts/check_sql_advisory_wait_postgres.py
```

The oracles check actual conflicting connections, MVCC, key-changing row updates,
savepoints, key spaces, reentrance, PREPARE ownership, queued upgrades, soft/hard
cycles and cancellation. They do not qualify Antfly public case dispositions.

Remaining activation work is substantial: replicated metadata commands and native
receiver authority; durable connection/transaction scope admission and expiry
fencing; participant enrollment before row/table grants; ordinary writer and reader
integration; linking locks to the existing external 2PC decision/activation path;
prepared GID/session handoff; scalar ordered effect demand with correct `void` OID
2278 and public descriptors; all public connection/pgwire lifecycle paths; restore
and mixed-decoder capability gates; distributed fault and PostgreSQL case campaigns.
A fixed HTTP idle TTL cannot be reused to release an active transaction's locks.
The existing native 2PC and prepared registry remain the decision owners; the lock
journal must not invent an alternative commit protocol. None of the explicit lock
originals is marked implemented based on these contract and storage tests alone.

The current Debug qualification includes 22 contract/journal tests (six of them
shared contract tests), four real LSM/WAL campaigns, two connection admission
unit tests and one mounted HTTP campaign. All completed without failures, skips
or leaks. The independent PostgreSQL oracles passed 130 lifetime/conflict and 31
queued-wait assertions. Formatting, license/boundary checks and inventory integrity
also pass; inventory remains 898 implemented, 138 rejected, 449 superseded and
101 unresolved originals. These counts describe this qualification, not complete
SQL lock activation. The final native proof audit additionally rejects a validly
checksummed grant copied under another resource's key before admitting a waiter.

## Metadata-owned session commit staging

The existing external transaction registry now distinguishes named prepared GIDs
from private session commits. Both use the same immutable participant manifest,
external owner digest, native voting, all-owner activation barrier, resolution and
retained acknowledgements. Ordinary commits retain their current fast path; no
public locking function or private admission is enabled by this staging work.

Named transactions keep their 64-slot quota, GID index and client-controlled
prepared interval. Private session commits use a separate 128-slot metadata quota,
require an immutable connection reference and reserve no public GID, including the
valid empty named GID. These are metadata admission limits; they do not increase a
data store's existing native participant capacity. Database/table reservation
counters cover both kinds, so schema work cannot race an in-flight private commit.

A private identity freezes its requested visibility level in the owner digest.
A vote-complete private commit remains in the recovery work index and cannot turn
into an expiry abort. The existing recovery controller chooses a commit version
and drives native activation, resolution and retained acknowledgement. An expired
incomplete preparation still aborts. Named prepared transactions continue waiting
for COMMIT PREPARED or ROLLBACK PREPARED.

Private completion receipts remain pending during preparation. Only native
resolution publishes committed/aborted completion; the receipt retains its exact
decision version and sync level after retirement. Named PREPARE consumers reject
private receipts. Default named identity and receipt JSON bytes remain unchanged.
Immutable receipt-node membership and private work membership are not rewritten
on every phase change, avoiding unnecessary metadata LSM versions.

Debug qualification covers named quota/namespace isolation, private admission
capacity, cold pending/terminal receipts, exact named wire compatibility, immutable
decision contracts and the shared controller's lost prepare/commit/activation/
resolution/handoff replies. The metadata gate passes 11 tests and transaction gates
pass 130 tests, without skips, failures or leaks. These are backend staging and
regression gates, not public SQL lock case closures.

Protocol 136 now advertises private session decisions and standalone ownership.
Service and proposal floors match the native receiver, which rejects private
proposals below 136, including late proposals that bypass preflight. Enabled
native admission, cold receipts and ACK replay are qualified. Immutable lock
transaction scope admission, connection/expiry fencing and the public ordered
SQL effect provider remain required before complete SQL locking activation.


## Durable completion and receipt replay qualification

Local terminal commit records can now retain an optional exact decision version.
Records without a supplied version keep their existing JSON bytes. The codec and
clone preserve the full unsigned 64-bit range, including values beyond JSON's
floating-point exact range, and reject zero, fractional, negative and overflowing
versions. Ordinary visibility-status updates retain the original version. Actual
LSM close/reopen and injected failed writes qualify these properties.

The named PREPARE delivery consumer validates the entire receipt contract before
retiring execution inputs. A named receipt cannot carry a commit decision or a
COMMITTED status. Exact prepared/aborted replays do not write another local
session version or renew its lease; metadata delivery ACK remains a separate
retryable step. Ordinary commit and abort recorders both reject externally owned
execution plans. Four cold LSM phases for each named outcome cover malformed
proofs, failed adoption writes, replay during failed writes, failed ACK persistence
and recovery after a successful ACK. The transaction Debug gates now pass 133
tests (44 local and 89 main), without failures, skips or leaks. The mounted public
SQL/pgwire PREPARE gate additionally passes all three Debug tests, including the
complete multi-owner cold recovery campaign.

Private receipt adoption now atomically retains the exact decision before
acknowledging metadata. Recovery preserves the private purpose, decision version
and frozen sync level after execution inputs are retired. Exact adoption and ACK
replays do not write additional local session versions. Debug transaction gates
pass 148 tests, including failed adoption, failed ACK and cold recovery for both
private outcomes. The mounted public PREPARE gate passes all three tests.

Standalone references explicitly carry their realm and retain node zero; clustered
references keep their existing JSON bytes and reject the local sentinel. Native
receipt selection checks the actual catalog realm. The metadata Debug gate passes
13 tests plus its declaration check, including decoder-floor rejection and cold
standalone receipt/ACK replay. A public standalone SQL test additionally passes
through three cold runtime phases: prepared rows remain invisible, commit and
abort resolve independently, and a retired GID can be reused without adopting the
old decision. This test runs in the existing native control-source cohort.

These recovery foundations do not close any SQL lock inventory cases or enable
public advisory functions. Connection scopes and ordered effect integration
remain the next activation boundary.

## Native command scopes and decision-gated cleanup

`storage/pessimistic_lock_authority.zig` now owns the durable execution scope
around the shared journal. Registration binds backend identity and fence,
connection ID, database, principal, deployment realm and lease. A retained
connection binding prevents an old registration from adopting a replacement
backend. The active scope quota is 4,096. Command sequences and immutable
execution nonces distinguish an active retry from a completed command; completed
commands cannot execute effects again. This is native authority machinery, not
yet public advisory-function activation.

The journal has explicit data and metadata homes. Metadata scopes, their work
index, connection bindings and grants all remain inside the accepting metadata
writer's permitted namespace. Four snapshot projections retain these families.
The metadata command rollback journal restores the complete touched-key set
before recording a semantic rejection; allocation or I/O errors still require
aborting the enclosing transaction. Ordinary data-home journal keys remain
separate.

The metadata adapter uses the existing catalog outcome journal and external
transaction registry. It checks realm, catalog incarnation and decoder 137 in
the native receiver, including late proposals. A connection with an attached
scope cannot admit a private commit without the current command and bound
transaction. The external admission and scope seal commit atomically. Named
and private identities and final receipts retain an optional immutable scope
reference; absent references retain their existing encoding.

Expiry fences the command before it can release any lock. The scope receiver's
accepting-writer guard rejects a fenced command even after a full restart.
Renewal changes neither backend identity nor queued effect identity and cannot
revive a fenced scope. Try results, acquisition, observation, cancellation,
unlock-all, rollback and savepoint controls reuse the exact-effect journal.
Internal metadata transport and standalone dispatch carry these operations
through the existing native storage boundary. Read control carries cancellation
and execution deadlines.

Transaction cleanup independently joins the exact existing external decision.
Only complete native resolution, or a fenced unsealed transaction that could
never admit participants, permits retirement. Cleanup follows the transaction
membership index one resource at a time; it preserves independent session
references, including mixed session/transaction holdings on one resource.
Pending release has an immediate work entry, independent of the idle lease
expiry. The completed command serial is a durable witness that remains useful
when later transactions replace the recent release marker.

The existing prepared-transaction recovery path advances one cleanup quantum
before handoff or delivery ACK. Native handoff and ACK admission reject pending
lock cleanup. The final decision receipt cannot be deleted until the matching
scope has durably adopted resolution. Scope adoption and receipt deletion share
the same writer transaction. Local receipt adoption checks the exact scope
reference as well as the existing owner, purpose, version and sync contract.

The same leader-fenced maintenance owner advances expiry and closed-scope work
with a keyset cursor. An unresolved decision does not prevent visiting later
owners. Pending lock work uses bounded continuations without increasing the
existing two-phase-commit recovery cadence: each slice has a 16-quantum and
cooperative 20 ms budget, checked between operations. Each individual operation
also retains its I/O deadline. Empty work returns to the idle schedule. Foreground
wakeups that arrive during a slice are preserved. Scope and prepared-registry
commands advance standalone durability without invalidating unchanged table
projections.

Public activation still requires the durable local connection producer and
registration adoption, private commit selection for every scoped write, native
command guards for metadata side effects such as sequences and DDL, pgwire and
HTTP connection lifecycle integration, and ordered SQL effects with correct
strict-NULL and void OID 2278 behavior. Prepared-owner handoff and distributed
row/table participant enrollment remain separate required integrations. Retired
scope and journal reclamation still requires an authoritative admission and
connection-retirement floor; this implementation retains those fences rather
than guessing when deleting them is safe. No explicit-lock original is credited
from these foundations alone.

The combined Debug qualification passes all 132 build steps: eight concrete
LSM/WAL campaigns; 14 prepared-registry tests and one metadata declaration check;
30 catalog/projection tests; 148 transaction contracts; three public SQL/pgwire
recovery tests; and the public standalone cold-recovery campaign, which also
exercises routed scope cleanup and asserts zero catalog-projection reloads.
Native release tests abort every staged-write boundary, reopen between successful
quanta, preserve session references after transaction cleanup, and reject obsolete
command execution. The existing 22 contract/journal tests also pass. Independent
PostgreSQL 18 oracles pass 130 conflict/lifetime and 31 wait/cancellation checks.
These results qualify the backend and recovery boundary. Inventory remains 912
implemented, 138 rejected, 449 superseded and 87 unresolved originals.


## Native command fences for journaled metadata effects

The shared bounded catalog admission now carries an optional immutable command
fence. SQL producers bind it before calculating compact observation/recovery
hashes; the dispatch boundary verifies that binding rather than silently changing
a prepared identity. The existing body-bound internal service grant covers the
serialized fence. It grants no catalog, table, or role privileges.

The receiving journal first checks its exact retained nonce/digest. A known
result is returned even after its command has completed, been fenced, or survived
a cold reopen. For an absent request still inside the 64-slot admission window,
the same accepting transaction checks catalog incarnation, activated decoder 138,
standalone/cluster realm, backend fence, base principal, database, command sequence,
nonce, executor, and the absence of a queued lock wait or control job. A permanent
fence proves nonexecution only while absence remains authoritative. Outside that
window the result stays unknown. No additional journal, receipt, or coordinator
was introduced.

Sequence counter requests bind the same fixed-width identity into their existing
semantic request digest. Existing successful reservations are observed before
liveness checks, so closing a command cannot allocate a second value or disguise
an evicted reservation as a definite rejection. `nextval`, `setval`, and implicit
column allocation retain the guarded request through lost-ACK recovery. Ordinary
requests retain their prior wire shape and perform no additional scope reads or
scope writes.

The guard uses at most four native point reads. Its qualification adapter exposes
neither a cursor nor a writer. SQL connection context borrows the immutable fence
through one pointer; forwarding copies the identity into the already-owned
mutation. Scope observations and outcome observations are classified as reads,
retain administrative authorization, and receive metadata identity stamps.

Catalog success notices now own a bounded 1024-byte value rather than borrowing
JSON parser or native read scratch. Their wire form remains a string or null.
The shared reader/writer record bound is 8 KiB, allowing worst-case JSON escaping
plus the fixed fence identity. Notice limits are checked while preparing the
catalog result, before applying the logical change.

The audited native campaign covers standalone and cluster realms, local and Raft
apply, queued-wait cancellation, command rollover, stale executors, counter-write
abort boundaries, exact cold replay, bounded journal eviction, substituted
principals/databases/realms, escaped capability markers, body-signature tampering,
and general-allocator decoding/reencoding of maximally escaped notices. The
focused Debug gate and sequence contracts pass all 73 build steps. Broader Debug
qualification passes 140 build steps and 354 tests, including mounted native
sequence/session effects, public SQL/pgwire, standalone recovery, and catalog
regressions. The final observation-identity review gate passes all 70 steps.

This completes the receiving boundary for mutations already using the common
admission journal and for sequence counters. It does not activate public advisory
SQL functions. Legacy catalog/view/table/settings mutation paths without that
journal reject scoped dispatch until they are integrated. Durable local connection
registration/adoption and command production, universal scoped private commits,
HTTP/pgwire lifecycle integration, ordered scalar effects, prepared-owner handoff,
row/table participant enrollment, and retirement-floor GC remain required. An
already admitted asynchronous catalog job must retain its independent durable
publication authority; it must not depend on a finished statement remaining live.
No original SQL parity disposition is changed by these foundations.

The preceding main merge preserves published LEANN data protocol 25 and assigns
SQL index, schema, drop, and external-transaction controls distinct floors 26, 27,
28, and 29. Earlier qualification entries record the pre-merge unpublished floors.

The main integration through `3b712f2243` preserves all 80 published metadata
transition tags, including completion references at tag 81 and completion
capability floors 69, 71, and 78. The combined unpublished SQL profile now
requires capability 154 for command fences (153 for native lock scopes); its
private transition tags occupy 82–95. Earlier qualification entries retain
the development capability numbers used when those checks ran.

## Durable source command producer

`api/sql_command_producer.zig` owns registration, exact control delivery,
command admission and source retirement in the existing durable session store.
Pending control requests are committed before RPC delivery. The source record
binds the native registration, source instance, immutable command/executor
identity, and logical request-body digest. It stores no executable SQL body and
creates no new transaction decision protocol. Providers must prove transaction-
wide shared writer serialization; a facade-local mutex cannot protect a shared
execution CAS.

Native BEGIN acknowledgement produces a ready source command, not permission to
replay SQL. A separate durable ready-to-executing CAS issues one execution
permit. Repeated or stale tickets, substituted bodies, failed local writes, and
another process instance cannot issue a second permit. A native idle observation
can transfer the source instance while staging the next command in one local
CAS; there is no separate adoption WAL commit. Ready and executing statements
cannot be handed off. The native backend and its command sequence remain
unchanged. Adopting a successful native BEGIN from a lost reply does not itself
evaluate the statement. All control delivery continues using the existing
authenticated catalog transport and shared bounded outcome journal. Expired
control admissions can be renewed only from an exact native scope observation;
the immutable command identity never changes.

A different facade is not proof of process death. Nonexpired live commands
remain owned by their original instance, even when another facade shares the
physical session namespace. Explicit closure or expiry first fences native
execution. Cleanup then joins native transaction-resolution authority, releases
transaction membership and advances one close quantum at a time. Lease renewal
is an exact compare-and-renew operation; observing its durable acknowledgement
after the old expiry must not fence the successfully renewed command. No idle
HTTP TTL is interpreted as permission to release an active native transaction's
locks.

The durable work index supports bounded, exclusive keyset pages. The existing
local supervisor processes at most eight source owners per cooperative 20 ms
slice, with independently bounded I/O. It never evaluates SQL and never treats a
changed facade as an expiry proof. Source work remains indexed through uncertain
control replies and cold reopen. HTTP connection GC and DISCARD/close cannot
erase an unretired source owner. After native retirement, connection removal
reclaims its source outbox in the same transaction. Native scope/binding fences
reject delayed registration or backend replacement; the source does not add
another unbounded tombstone family. Orphaned source records can still drive
cleanup after their parent is gone. Removing the actual native replay fences
still requires the authoritative retirement floor. Session control, settings and
sequence-state publication carry the same restrictive command fence when an
execution owner is attached, alongside existing SQL/RLS privileges.

The native campaigns cover both catalog realms, local and Raft apply, three cold
reopen phases, lost requests/replies, exact body retransmission, one-use
permits, shared facades, admission-window eviction, lease renewal, unsealed
transaction release, incremental session-lock cleanup, connection GC, native
work pagination, and actual supervisor/authenticated-source dispatch. Scope-
control RPCs do not mark the public SQL statement as submitted. Idle facade
handoff, stale source CAS, rejected ready/executing takeover, orphan cleanup,
and reclamation through the existing native fence also have explicit campaigns.
A separate LSM/WAL campaign aborts every staged outbox record/index write for
execution, retirement and reclamation.

This is the source control boundary, not activation of public advisory
functions. Public HTTP/pgwire entry points still need to create/pin these owners
around SQL. Scoped DML and transaction completion now select the existing
private decision path described below. Ordered advisory scalars still need their PostgreSQL NULL/void contracts, and legacy
metadata effects, prepared-owner handoff, participant enrollment and retirement-
floor GC remain. No original fixture is reclassified from this qualification
alone.

Debug qualification passes all 138 broader build steps, including native command
fences, mounted sequence/session effects, public SQL/pgwire and standalone cold
prepared recovery. The handoff/reclamation review passes all 71 steps with 19
focused source/connection tests and five metadata-submission tests. The final
late-renewal review passes all 61 steps and 20 source/connection tests, with no
skips, failures or leaks. Scope cancellation remains irreversible even if an
already dispatched renewal wins before its native fence. The existing HTTP
runtime unit owner also selects these native campaigns; they are not E2E tests.
Inventory remains 912 implemented, 138 rejected, 449 superseded and 87
unresolved.

The final lifetime qualification passes all 126 Debug build steps, including 21
source/connection tests, 41 public SQL/pgwire native contracts, seven mounted
sequence/session effects tests, five metadata-submission contracts, and the
standalone cold prepared recovery test. Active command publication may cross the
parent's idle TTL only with its exact unexpired source fence; ordinary expired
connection access stays rejected. Independently proved BEGIN/terminal repairs
remain possible after source retirement, preventing cleanup from being stranded.


### Scoped transaction decisions

The SQL session coordinator selects the existing metadata-authoritative private
`session_commit` purpose whenever the request owns a command fence. Explicit
COMMIT, guarded implicit DML, zero-row commits, validation aborts and ROLLBACK
share that authority. SQL authorization, RLS participant signing, FK/UNIQUE
preparation, immutable route manifests and durability settings still use the
ordinary transaction pipeline. A scoped request never falls back to the legacy
commit coordinator after an external purpose is sealed.

The session persists its exact signed plan, connection, scope, command and
participant digest before metadata admission. A first private admission binds
and seals the transaction in the same native writer, eliminating a separate
binding RPC and its orphan-binding crash window. An existing different binding,
the retained released transaction ID, an obsolete command or an unscoped admission on a
bound connection is rejected. Metadata control deliveries retain the original
admission digest after the client command dies.

Zero-participant transactions still cross the native decision and release
barriers; they do not invent a table participant or require a storage writer.
The same indexed supervisor advances their commit/abort and handoff. Once the
plan is sealed, interrupted delivery is reconciliation debt under that purpose,
not permission to replay SQL. Terminal native receipts are adopted locally
before their delivery ACK can be reclaimed.

An unadmitted purpose can nevertheless own transaction locks acquired earlier.
Its cleanup supplies the original sealed admission to the native lock writer.
That writer independently joins the incarnation, connection, principal, command,
transaction and exact admission journal, verifies both decision record and
receipt absence, and requires a retained rejection or a permanently expired
admission window. Absence inside a live window never permits release. Each
cleanup quantum retains session grants and checks the same proof; a local SQL
abort is published only after the native unsealed binding has been released.
No second decision registry or additional permanent tombstone family is added.

Command completion waits for uncertain connection decisions and bounded native
transaction-lock cleanup. Independent terminal reconciliation may publish while
a source is finishing, because that phase cannot issue another SQL execution
permit. A busy FINISH control cannot fence the backend and discard its session
locks. The existing supervisor performs the terminal reconciliation before
advancing the source again. Normal completion adds only a bounded local
connection-record lookup, not a scan or a copy of the staged row plan.

This integration does not activate public advisory functions. HTTP/pgwire source
pinning, ordered scalar effects with PostgreSQL NULL/void contracts, prepared
owner handoff, row/table participant enrollment and authoritative retirement-floor
GC remain separate requirements. Original parity dispositions are unchanged.


The final qualification passes all **147 Debug build steps** and 84 selected
tests with no skips, failures or leaks. It includes 24 before/after-delivery
scenarios over three native-store lifetimes for admission, prepare, decision,
handoff and ACK; healthy/failed rollback with admitted and rejected purposes;
refusal of release while admission remains possible; cleanup after the original
journal window is evicted; and preservation of session grants and usable idle
connections. The standalone integration commits a scoped row through real native
table participants beside named prepared transactions and verifies both across
cold reopen. Existing sequence/session, public SQL/pgwire, metadata-submission
and command-fence gates also pass. `make generate`, `make fmt`, source-boundary,
license and original-inventory checks pass.

Private commit recovery now separates durable row resolution from the requested
index visibility barrier. The metadata receipt records a known committed decision
while row propagation is pending, then records index visibility progress. Native
participant bitmaps retain successful visibility barriers across retries;
permanent enrichment failures become terminal repair debt. The source adopts
these receipts monotonically and persists the exact terminal version and repair
status before acknowledging the receipt. Transactional locks can be released
after row resolution while asynchronous indexing remains pending; an early
committed receipt alone cannot authorize their release.

Live command scopes hold a durable per-database reference in the same native
transaction as registration. Fencing keeps that reference until bounded cleanup
has retired the scope. Database rename/drop checks the counter in preflight and
again during Raft apply, so a late proposal cannot remove the catalog identity
needed by cleanup. The counter shares the existing scope snapshot projection;
rename/drop performs a point read rather than scanning connection owners.

These are shared activation prerequisites, not additional original-case credits.
Public scoped ownership still requires named prepared-transaction handoff and
consistent HTTP/pgwire command lifetime integration before advisory/table-lock
cases can be credited.


After merging `origin/main` at `24486f5f30`, regeneration resolves the generated
Go client conflict from the combined OpenAPI source. The generated Go API
package compiles. The merged qualification passes all **156 Debug build steps**
and **87 selected tests**, with no skips, failures or leaks, including main's
three agent-generation regressions. The source-boundary and original-inventory
checks also pass on the merged tree; 87 original cases remain unresolved.


The private commit visibility and database-retirement cohort passes **170/170
Debug build steps** and **110 selected tests**, with no skips, failures or leaks.
The audited `antfly-sql-transaction-lifecycle-test` selects all required native
receipt, cold adoption, partial visibility, repair and database-retirement tests
through the transaction discovery root. The normal transaction suite also owns
the new cases. The standalone test requests `full_index` through real native
participants. Source-producer, public SQL/pgwire, sequence/session,
metadata-submission, native lock fault and command-fence gates pass together.
`make generate`, `make fmt`, license, source-boundary and original-inventory
checks pass. The original inventory remains **87 unresolved cases**; these
recovery prerequisites do not independently close a public locking case.


## Named prepared lock home

Named transactions with a command scope enlist the metadata lock home in the
existing prepared registry. After every table participant has durably voted,
the registry enters `locking`; it cannot publish PREPARED or its session receipt
until the native home has finished transferring transaction grants. This frozen
vote does not expire into an abort because its source command died. Recovery uses
the same work index and immutable external owner, never SQL replay or a second
decision registry.

The destination lease is derived from the exact operation and admission digest.
It has a stable backend fence, database and node identity and does not expire.
It can therefore survive the source connection's retirement. Each native
handoff processes at most one indexed transaction resource. Intermediate quanta
write only the native cursor and exact delivery outcome; they do not rewrite the
registry's frozen participant manifest or increment its logical phase revision.
The audited native campaign asserts zero registry writes/deletes for these
quanta. The accepting writer
rechecks the sealed transaction, original scope, source lease and destination
identity on every quantum. Session grants stay with the original backend.
Publishing PREPARED, completing the original command and releasing its bound
transaction happen in the same native transaction as the final transfer.

A resolved decision does not retire its metadata record until the lock home has
released its grants. An abort before handoff releases only the sealed source
transaction. An abort during an incoming transfer completes that exact transfer
before retiring the destination, preserving continuous ownership. Cleanup after
a completed handoff reads the prepared registry and destination directly; it
never borrows a live source command. Final metadata handoff reclaims the retired
non-expiring destination and its sole close receipt in that same native writer.
No per-transaction destination tombstone is added. An old home command cannot
recreate the reclaimed destination once its metadata operation is retired;
reader snapshots pinned before deletion retain their immutable view.
The prepared registry's durable database reference also fences rename and drop
after the originating connection has closed; neither operation may bypass a
frozen prepared owner merely because its live-scope counter reached zero. Every quantum remains independently fenced
by the immutable decision. Mixed session/transaction advisory ownership of one
key is a known preparation rejection. An exact retained rejection can authorize
unadmitted named-transaction cleanup without waiting for connection expiry.

The coordinator permits a bounded eight phase/lock quanta per recovery pass and
rotates through the existing work cursor between passes. Its observed native
postimage determines whether the final quantum completed; a caller cannot infer
that a background job finished from a local phase calculation. Decoder version
155 protects both publication and mutation delivery, including commands which
also carry an older command fence.

Qualification covers twelve native clustered/standalone scenarios: commit,
abort before handoff, abort during transfer, abort after preparation, rejected
mixed-scope admission and source reuse by a newer transaction. They exercise
exact replay, cold reopen between quanta, snapshot contention, source retirement
and failed native writes at each transfer, cleanup and reclamation boundary.
Resolving an older prepared participant preserves the newer transaction's
binding and both session and transaction grants on the original backend.

The combined Debug qualification passed **170/170 steps and 112 selected tests**.
After adding reclamation, manifest-write suppression and the database rename
barrier, the three affected native/standalone gates passed **107/107 steps and
29 tests**. The final source-reuse extension passed **61/61 steps and 19 tests**.
All runs had no skips, failures or leaks. The standalone SQL campaign prepares
real row participants with a transaction advisory grant, retires the source,
reopens, commits the prepared row and reopens again. The independent PostgreSQL
18 oracles pass **134 lifetime/conflict/database-retirement assertions** and
**31 wait/cancellation assertions**. Generation, formatting, license,
source-boundary and original-inventory checks pass.

The public advisory/scalar and HTTP/pgwire source activation work remains open;
no original inventory case is closed by this native handoff alone. Authoritative
retirement-floor reclamation for connection tombstones is still separate work;
the prepared destination does not add a new per-transaction tombstone family.
The original inventory remains **912 implemented, 138 rejected, 449 superseded
and 87 unresolved**.

## Ordered advisory scalar execution and void identity

The shared SQL engine now binds all eleven PostgreSQL advisory builtins, including
both key overloads and the `pg_catalog` namespace. Arguments cross the existing
integer coercion boundary; the two-key overload never silently narrows a BIGINT.
Strict constant NULLs and unselected branches issue no demand. Each executed
occurrence produces an owned, ordered demand; provider I/O remains outside the
scalar VM and immutable prepared programs retain no connection pointers.

Advisory and sequence demands execute in row order across predicates, projections,
VALUES, mutation expressions and nested SQL routines. Each executed nested call
has its own stable suspension frame, keyed by its parent invocation and instruction.
Its receipts cannot overwrite a caller's equal-numbered instruction or cause a
completed effect to execute again after suspension. Frames share the invocation's
work, cancellation and byte budgets. Ordinary nested AI calls retain batch
execution instead of inheriting sequence/advisory serialization. Bound programs
cache their external-effect classification once; checking it during row execution
is constant time. Hand-built internal programs retain the scanning fallback.

The effect owner polls cancellation immediately before provider I/O. An uncertain
provider result is returned to the enclosing owner without replaying the SQL or
issuing its next effect. Advisory replies must have their declared non-NULL boolean
or void identity; malformed replies cannot be memoized as successful results.

`void` is an SQL pseudotype, not an alias for TEXT or JSON NULL. A payload-free
Datum tag preserves it through prepared parameters, retained columns, scalar
clones and both columnar spill versions. Retained columns use a reserved immediate
extension value, without allocating an object for every void cell or growing the
existing compact extension entries. Public OpenAPI/SDK descriptors expose `void`;
pgwire uses OID 2278, type length 4 and zero payload bytes in text and binary results.
SQL NULL still has its separate NULL marker. PostgreSQL text Bind input is ignored
after encoding validation; binary Bind requires an empty payload and rejects
nonempty bytes with SQLSTATE 22P03.

Physical columns and stored view output cannot acquire the pseudotype. Arrays,
comparison operators, grouping, DISTINCT aggregates, set deduplication and sort
keys must reject unsupported void comparison domains before any effect runs.
COALESCE, explicit text output, compatible text concatenation and COUNT retain
PostgreSQL behavior. The independent scalar/void oracle supplements the existing
lock lifetime and wait oracles; it does not substitute for native/public ownership
qualification.

The public HTTP/pgwire durable source lifecycle is still required before closing
`sql-0089`–`sql-0091`; the native provider is qualified in the following section.
This scalar/type cohort earns
no original-case credits. The immutable original inventory remains **912
implemented, 138 rejected, 449 superseded and 87 unresolved**.


The broad Debug run passed all **131/131 build steps**. Its SQL owners passed
**1,979 tests** (1,725 local and 254 server); four opt-in benchmark tests were
skipped. The pgwire, native public result and durable sequence gates also passed,
including **42 native-result tests** and **seven sequence tests**. The final
focused advisory/type gate passes **11 tests**, with no skips, failures or leaks,
including materialized CTEs, recursive UNION ALL, windows, ordered results and
non-executing EXPLAIN. Nested AI calls retain batches of four across two fresh
invocations; cancellation, unknown delivery and malformed receipts stop before
another provider call. Allocation-failure campaigns cover retained cells, spill
versions and public typed-result ownership.

The isolated PostgreSQL oracles pass **177 scalar/void**, **134 lifetime/conflict**
and **31 wait/cancellation** assertions. `make generate`, `make fmt`, generated Go
SDK tests, license/boundary checks, control-catalog checks and original-inventory
integrity pass. Latest `origin/main` at `24486f5f30` is included. These gates qualify
the listed layers, not the still-unfinished public lock activation.

The final affected Debug recheck passed **93/93 steps**, including all **42
native-result** and **seven durable sequence** tests, without skips, failures or
leaks, after the coercion and recursive-type review fixes.


## Native advisory execution and bounded wait traffic

`api/sql_advisory_execution.zig` binds an ordered scalar occurrence to one immutable
native ledger effect. Its frame owns the authenticated connection, execution
instance, durable command ticket, database and backend incarnation. It validates
both the durable executing source and authenticated native scope before issuing
a mutation. Prepared programs retain no execution authority. The production
adapter supplies the provider only when ingress has supplied a command fence.

All eleven advisory functions use the same native graph and receipt journal,
including both key spaces, shared/exclusive modes, reentrance, transaction scope
and bounded session unlock-all. Integer pairs are range checked before delivery.
Transaction effects bind the existing transaction ID, then use the durable
savepoint identity rather than nesting depth. The session registry derives that
identity during existing savepoint clone/decode and mutation work, so hot statement
admission reads it in constant time without scanning the map. Recovery rejects
zero identities and identities outside the existing durable allocation counter.
Failed savepoint persistence leaves the published projection intact. Retained
pgwire reads refresh the projection when resumed. No second subtransaction counter
or persistent format is introduced.

A fresh uncontended effect performs one native observation and one mutation.
The accepting transaction publishes its exact effect receipt with the ordinary
command outcome, eliminating a guaranteed-empty outcome lookup and a follow-up
receipt read. Only uncertain requests use exact prior-outcome observation and
redelivery. An immutable effect can outlive replacement of its bounded journal
admission, and a lost bind reply is reconciled against the bound native scope.
No uncertainty path reruns the SQL occurrence or fabricates compensation unlocks.

Queued acquisitions retain an authenticated resource stamp containing its digest,
generation and revision. An unchanged stamp suppresses another apply and journal
entry; a changed resource is replanned using the actual current graph. Generation
prevents an equal revision on a recreated resource from looking unchanged. Reads
remain bounded polls with cancellation checks and cooperative `std.Io` backoff;
this is not a push notification or long-poll implementation. Control jobs advance
one indexed quantum per mutation and yield cooperatively. Native hard deadlock
receipts report SQLSTATE 40P01 without releasing earlier session grants or
advertising automatic statement replay as safe.

Returned or cancelled SQL bodies can leave a queued acquisition or an unlock-all
job. Durable command completion now cancels the exact abandoned waiter or
finishes one control quantum at a time before completing the native command.
Every cleanup request remains in the existing source outbox and survives a cold
restart or a lost reply. Cleanup preserves earlier session grants and transaction
grants; transaction decisions remain owned by the existing 2PC controller.

Capability 156 protects exact effect replies and resource stamps on native lock
reads and writes. Unrelated catalog outcomes keep their existing field shape.
Gate tests check older capability rejection before native records are created,
then activate monotonically before running current-decoder lock operations.

The native source campaign now covers all overloads with uncertainty on both
sides of acceptance, the real scalar-to-native transaction boundary, hard
cycles, wakeup after release, cold completion of abandoned waiters/control jobs,
and monotone savepoint projection through cold rollback and release. The wait
traffic test proves 64 unchanged polls append zero commands or journal entries;
the uncontended test asserts exactly one read and one mutation with no prior
outcome lookup. These are operation-count checks, not latency benchmark claims.

Public activation still requires durable HTTP/pgwire connection enrollment,
lease renewal tied to live execution and transactions, command ownership across
portal suspension, implicit transaction ownership for transaction-scoped calls,
and distributed row/table participant enrollment and savepoint propagation.
The metadata-home advisory savepoint handoff is implemented below. These unfinished boundaries prevent
closing the original advisory cases; counts remain **912 implemented, 138
rejected, 449 superseded and 87 unresolved**.


After the savepoint and retained-read review fixes, the affected Debug source and
public-result gates passed **70/70 build steps**, including **32 source/controller
and session-recovery tests** and **42 native-result/retained-read tests**, with no
skips, failures or leaks. The earlier combined authority qualification passed
**93/93 steps**, including the command-fence, metadata prepared-transaction and
transaction-lifecycle gates. Generation, formatting, original-inventory integrity,
Apache boundaries and the generated control-catalog check pass. These results do
not qualify the unfinished public source activation or earn original-case credits.

The final post-review transaction/authority recheck passed **102/102 build steps**
and **52 selected test executions**, including nine native LSM campaigns, all 18
metadata prepared-transaction tests and all 16 transaction-lifecycle tests. There
were no skips, failures or leaks. This recheck includes the stricter savepoint
decoder and the derived active-subtransaction projection.


## Atomic savepoint publication before native cleanup

The shared SQL control adapter publishes savepoints through the existing source
owner when a native command fence is present. CREATE preserves the existing
monotone savepoint IDs. RELEASE and ROLLBACK TO prepare the ordinary SQL session
postimage, including staged rows, settings, failure state and read observations.
The local writer compares both the current durable session bytes and the exact
executing source record. It atomically commits that SQL postimage with one
immutable native control intent in the source outbox.

Native control runs after that commit. This order can conservatively retain old
locks across a crash; it cannot release a lock before its SQL rewind is durable.
The worker completes the recorded effect without re-evaluating a savepoint name
or replaying SQL. RELEASE reparents child transaction grants to the remaining
parent ID. ROLLBACK TO removes grants acquired at the target savepoint or in its
descendants, while preserving earlier root grants and session ownership. Each native quantum
uses the existing indexed ledger and publishes independently. Receipts bind to
the full system metadata group ID, incarnation and immutable effect.

An outstanding intent blocks further connection mutations and advisory effects.
Older registry savepoint entry points cannot bypass this gate. Exact source or
session conflicts abort the complete local publication. Failed persistence on
either side of the session/outbox write leaves both old records intact. The
foreground driver can lose its source CAS to the background worker after native
acceptance; it reloads that exact intent and continues control delivery without
repeating SQL publication.

Live retained-read capabilities remain outside durable encoding. Refreshing the
session for publication preserves a live capability only when its transaction,
owner, scope and immutable read characteristics match. Savepoint operations do
not drop that capability, including on failed publication. A cold reopen retains
the durable chosen-snapshot marker without recreating the capability; subsequent
reads keep the existing restart requirement. Snapshot closure still follows the
registry's deferred retirement mechanism.

A live foreground cleanup renews its native lease cooperatively before expiry.
The immutable intent survives replacement of its control admission by renewal.
Cancellation can move the command to finishing while that exact renewal remains
in flight; recovery adopts its proven expiry before completing the saved control.
A renewal does not admit another advisory effect through the unfinished intent.
The source's existing irreversible fencing and decision-gated retirement remain
the abandonment path. This does not implement generic execution/idle-transaction
heartbeats or public connection enrollment.

Qualification uses real LSM/WAL and local/Raft metadata application in both
standalone and clustered realms. It exercises failed local publication, stale
source plans, blocked bypass paths, lease renewal, lost control replies, command
completion and cold reopen between quanta. The shared SQL adapter also exercises
a worker winning the acknowledgement CAS after native acceptance. Independent
mounted transaction-control and isolation gates protect the existing SQL paths.
No original advisory disposition is closed by this internal lifecycle cohort;
public ingress ownership remains required.

The final Debug qualification passed **102/102 build steps**: all **34
source/controller tests**, the mounted original transaction-control campaign,
and all **seven transaction-characteristics/capability tests**. There were no
skips, failures or leaks. The **42 native-result/retained-read tests** also passed
against the reviewed production changes. Socket-based gates ran with local bind
access rather than accepting sandbox skips. Inventory integrity and Apache/control
catalog audits pass; the original dispositions remain **912 implemented, 138
rejected, 449 superseded and 87 unresolved**.


## Shared ingress controller and command-specific liveness

`api/sql_command_execution.zig` provides the protocol-independent enrollment and
command loan used to assemble public ingress ownership. Its native discovery is
a capability-gated point lookup of one database. It returns the database ID,
incarnation, realm and admission head without enumerating scopes, work items or
prepared transactions. Decoder capability 157 protects this selector and reply.
Mixed selectors, malformed database names and realm substitution fail closed.

Enrollment requires the existing logical connection's authenticated owner and
scope, persists the immutable native registration, and assists only registration
or safe control work. A command loan observes native idleness, persists its exact
request digest and ticket, then takes one durable execution permit. A fresh
request cannot adopt a ready/executing body, including byte-identical SQL. Native
control may resume after cold reopen; SQL evaluation never resumes implicitly.

Completion reserves its nonce before taking the permit, so cancellation of the
request or entropy source cannot prevent its local publication. An in-flight
renewal survives transition to finishing even without a savepoint intent. The
worker adopts its exact expiry proof before completing the command.

Cancellation before permit taking has a different disposition: it marks the body
skipped and hands the exact BEGIN/finish sequence to the existing worker. It does
not issue an execution permit or retire the connection's earlier session locks.
Fresh ingress may assist this frozen skip before taking its own new ticket. The
skip marker clears only after native completion is proven. Process-loss recovery
retains the existing irreversible fence and retirement behavior.

Completion publication retries a bounded number of source CAS races using its
reserved nonce. A delayed completion checks native backend identity and the
monotone command sequence; it cannot complete a newer command. Admission binding
also rechecks the durable taken ticket, so a copied old loan cannot occupy a new
request's command slot. The local completion flag uses atomic publication for a
concurrent heartbeat borrower.

The local admission pool now records the exact command bound to an admission
lease. Merely occupying a connection is not evidence that an older command is
live. Copied stale leases cannot bind/release a newer admission, and a loan must
match the lease's connection and authenticated principal. This is liveness proof,
not durable authority or another SQL coordinator.

Lease maintenance now distinguishes idle connections, exact executing commands,
and body-free completion. The source worker renews only its own process instance;
an executing source additionally requires the admission pool's exact command.
Replacing an admission cannot keep the earlier body alive. Idle resources retain
the existing connection TTL and active SQL transaction lease checks. A completed
body can renew while its native cleanup or durable transaction decision remains
pending, preserving session locks rather than treating slow cleanup as a crash.

Renewal observes the native registration, persists its immutable admission, and
delivers that admission in the same maintenance quantum. It does not wait another
directory sweep before extending the native lease. Lost replies and cold reopen
use the retained request and native expiry proof; expired/fenced owners cannot be
revived. Healthy command heartbeat checks make no metadata RPCs. A command loan
exposes the same exact-admission heartbeat operation to ingress owners.

These components still require the HTTP/pgwire adapters, independent live-query
heartbeat scheduling, disconnect retirement and portal-bound command ownership
before public activation. The bounded maintenance sweep alone does not establish
a renewal latency guarantee under slow RPCs. They do not close the original
advisory cases or change the original inventory counts.

Qualification: the combined source/API/transaction Debug run passed **83/83 build
steps**, including **41 source/controller tests**, **42 native-result/API tests**,
and the transaction lifecycle gates. The reviewed source/fence/submission run
passed **92/92 steps**. After the final native-fence change, the source gate passed
**61/61 steps**, with all **41 tests** and no skips, failures or leaks. The server
maintenance campaign covers local and Raft apply, idle and executing owners,
unbound/replacement admissions, foreign process instances, completion, expired
leases and native fencing before local expiry. Native fencing publishes local
recovery immediately rather than repeatedly attempting renewal.

Full formatting, license, Apache-boundary, control-catalog and inventory checks
pass. Main was fetched and merged again at `24486f5f30` (already contained). The
original inventory remains **912 implemented, 138 rejected, 449 superseded and
87 unresolved (64 DDL/catalog)**; public ingress and distributed activation are
still required before crediting those originals.


## Atomic connection reset and native unlock

A scoped `DISCARD ALL` keeps the backend alive. Its accepting source transaction
checks the exact command and observed connection revision, resets settings,
search path and sequence state, increments resource generations, removes only
that connection's prepared resources, and retains an immutable session-unlock
intent. These changes commit together. It does not first unlock native grants
or retire the backend. Other connections' prepared resources remain intact.

Savepoints and DISCARD use the same `ControlPlan`, source CAS validation,
`control_effect` outbox and bounded native cleanup loop. This is a general SQL
metadata/control publication boundary, rather than a second reset coordinator.
The SQL settings adapter allocates its empty result before publication and waits
for the native completion proof before reporting success. A failed source CAS
aborts the whole local transaction; only that proven pre-publication failure
permits replanning. A changed logical connection revision is never overwritten.
In-flight lease renewal is assisted as control before publication.

After publication, cancellation and uncertain native replies leave the exact
unlock intent durable. Command FINISH preserves it, and existing source recovery
completes the indexed unlock job without executing SQL or incrementing reset
generations again. Reentrant session references are all released. The same native
backend can admit its next command and acquire those resources again.

The native campaign injects failures after staging the source, prepared-resource
removal and connection postimage; every aborted transaction preserves the old
resources and metadata. It also covers a competing renewal CAS, cancellation
after reset, lost effect replies, cold reopen between cleanup quanta, backend
reuse and preservation of another connection's resources. Both catalog realms
and local/Raft metadata apply are exercised. The shared settings adapter runs
against the real native source/ledger path, including cancellation after reset.

Debug qualification passes **83/83 build steps**, with **43 source/controller
cases**, **42 native-result/API cases**, and the mounted transaction lifecycle
and **18 transaction contract cases**. All report zero skips, failures and leaks.
Formatting, license, Apache-boundary, control-catalog and inventory checks pass.
This qualifies the scoped reset publication and recovery path; public ingress,
pgwire connection ownership and distributed participant activation remain open.
No original inventory disposition changes: **87 remain unresolved (64 DDL)**.


## Independent upkeep and enrolled HTTP request ownership

An API-owned `std.Io.Threaded` lane now runs eight bounded upkeep workers at a
stable address. Workers claim each source key once per keyset round, with at
most 128 queued identifiers and no duplicate in-flight owner. A blocked source
cannot monopolize the ordinary session/prepared-maintenance queue or the request
executor. Each native control quantum carries a one-second deadline. Shutdown
cancels control contexts and joins every worker before freeing API state. A
partial lane start likewise joins already-started workers before releasing its
I/O implementation. Admission failures preserve ordinary transaction recovery;
a scoped SQL request requires successful upkeep admission before execution.

Successful exact renewal replies immediately publish the renewed local lease
for idle/executing owners without a second native snapshot. Lost replies retain
the immutable renewal request and resume through native observation or the exact
retained receipt. FINISH and metadata-control owners keep their existing outbox
barriers. Expiry and native fencing remain authoritative; upkeep does not replay
SQL or grant a new body permit.

The HTTP SQL worker now pins a command loan for an already-enrolled connection,
including prepared-resource operations. Its body digest streams directly through
a hashing writer. Admission binds an atomic expiry/loss signal to that exact
command; scalar cancellation checks inspect it without I/O. Upkeep only accesses
a borrowed signal under the admission mutex. Release removes it before request
storage is reclaimed, and delayed progress for an old command cannot modify the
next request's signal. Native control does not retain request or credential
storage. The worker restores its original context after finishing execution
owners and publishing command completion.

HTTP close explicitly retires the enrolled native backend through the same
irreversible, decision-gated control state machine before removing its local
connection/source. Cancellation after an accepted advisory grant preserves the
native session reference until explicit unlock/reset/close. A completion-write
failure after execution reports SQLSTATE `40003`, rather than suggesting the
statement can be replayed. Joined result and prepared/read response ownership is
released on diagnostic and encoding paths as well as successful delivery.

The native campaigns demonstrate progress for twenty other owners with one
blocked RPC, an unavailable ordinary maintenance mutex and exhausted request
concurrency. They also prove shutdown cancellation/join and exact-command signal
publication after an old signal is freed. Real HTTP handlers execute bigint and
two-key advisory operations through local/Raft metadata apply, including request
cancellation after an accepted grant, authority-loss cancellation, failed durable
completion, and native retirement/local reclamation on close. Fault injection
exposed and fixed a completed-result reservation leak on the HTTP error path.

Final Debug qualification passes **83/83 build steps**, including all **46
source/controller cases**, **42 API/native-result cases**, and the transaction
lifecycle campaigns, with no skips, failures or leaks. Formatting and license,
Apache-boundary, control-catalog and original-inventory checks pass.

This wires enrolled connections; automatic enrollment of newly opened public
connections still needs the remaining metadata publication guards. Pgwire
connection/portal ownership and row/table participant enrollment also remain open.
Implicit advisory lifetimes are addressed by the following section. The original inventory is unchanged:
**87 unresolved, including 64 DDL/catalog cases**. These remaining activation
requirements are not claimed by the enrolled-connection fixtures.

## Lazy implicit advisory transactions and independent completion

Autocommit evaluation pins a lazy advisory transaction owner on the shared SQL
adapter. The first executed transaction-scoped advisory effect begins one durable
SQL transaction. Later effects, including session effects in the same statement,
share its binding. Plain reads, untaken branches, STRICT null calls and
preparation do not create a SQL transaction. Invocation-time admission includes
catalog-bound routines, operators and casts without guessing their effect shape
from the outer SQL text. The read keeps its original statement snapshot and
clock; starting its advisory scope does not switch its row-read or staging path.
Catalog scalar routine binding now preserves transitive external-effect metadata
and uses the evaluator's existing invocation-owned suspension frames.

Successful implicit completion adopts the durable decision and drains the exact
session's native release/receipt ACK before exposing the result. Error cleanup
uses bounded control with the original accepting-writer fence independently of
request cancellation. Failure of that cleanup retires the connection through the
durable source outbox, rather than exposing an implicit transaction as a new
explicit client transaction. A sealed or unknown decision is never replaced by
another SQL execution or a contradictory rollback.

Command-source completion also delivers decision-gated transaction release
through its own outbox. Each observation stages or delivers one exact quantum;
lost requests/replies resume that admission. This allows the independent source
lane to free a decided connection without the prepared-work scanner. Session
grants survive ordinary transaction release. Native decision validation remains
a prerequisite, and an uncertain admission still blocks command completion.

Sealed execution and a connection's best-effort uncertain marker can become
visible in separate local commits. A fixed 74-byte projection of session owner,
principal and execution state now accompanies every connection-owned session
publication, including lease transfers. Source completion reads that fact and the
connection in one native cut, so a missing uncertain marker cannot invalidate a
still-arriving admission. This does not load staged rows or execution plans.
Unchanged facts do not produce additional LSM writes; connectionless sessions do
not allocate this projection. Session retirement removes it atomically.

Automatic public connection enrollment still requires the remaining metadata
publication guards. Pgwire connection/portal ownership and distributed row/table
participant enrollment remain separate activation requirements. No original case
is reclassified by this component and enrolled-ingress work.

Sequence effects use the current effect attachment independently of row staging.
When `nextval` precedes the first advisory transaction effect, its nontransactional
session postimage is flushed before BEGIN, and the existing sequence owner then
follows the new attachment. When it follows that effect, its owner binds directly
to the lazy transaction. Rollback flushes sequence session state before detaching,
preserving PostgreSQL `currval`/`lastval` behavior after expression errors. Native
fixtures drive the real catalog adoption/publication coordinator before creating
sequences. An interrupted BEGIN without a session projection instead uses the
existing exact connection reconciliation; it cannot be mistaken for a sealed
admission or leave the source blocked on a missing record.

A separate definition-admission gap remains for constant sequence names inside
stored SQL bodies: native body validation needs the caller's authenticated
namespace resolution and exact sequence identity proofs, checked again by the
receiving writer. Dynamic sequence arguments use the existing invocation lookup.
Do not suppress native reference validation to enable constants. String-body
validation proofs must also remain distinct from stored dependency edges, so
DROP/recreate and execution-time lookup retain PostgreSQL's semantics.

Failed cleanup also marks a command-specific retirement requirement on its live
admission signal before attempting local publication. The request finalizer
therefore publishes retirement instead of ordinary FINISH if the first retirement
write fails and storage recovers before finalization. If that retry also fails,
the source remains executing and cannot be adopted by another request. Closing
source maintenance retries removal of unsealed local staging; sealed decisions
remain intact. The fault campaign fails both decision lookup and the first
retirement write, then verifies retirement, detached local state and native cleanup.

Final qualification passes **118/118 Debug build steps**: **49 source/controller
cases**, **42 native-result cases**, **two mounted routine/prepared cases**, the
mounted transaction lifecycle case and **18 transaction lifecycle contracts**,
without skips, failures or leaks. The PostgreSQL 18 oracle passes **221
assertions**, including mixed sequence/advisory ordering and error postimages.
Formatting, license, Apache-boundary, control-catalog and inventory audits pass.
The original inventory remains unchanged at **87 unresolved (64 DDL/catalog)**.


## Prepared ordinary catalog commands

Ordinary catalog DDL uses a private prepare/submit/observe contract, alongside
specialized sequence, type, callable, authorization and native-index commands.
Preparation resolves physical table identities, initial ranges, placement,
constraint names, copied source proofs and column sequence bindings exactly
once. It returns the complete native command, with a catalog incarnation,
bounded journal admission and the originating command fence already attached.
Submission cannot silently attach or replace that fence. The canonical prepared
command digest, rather than a second copy of the original JSON intent, identifies
the retained outcome. Hashing streams canonical JSON into SHA-256 with fixed
scratch and no heap allocation; it also enforces the native command byte limit.
Observation is a compact admission/incarnation/digest
projection; it does not carry schema bytes or grant resubmission authority.

Decoder capability 158 is required at both proposal and receiving native apply.
The command retains its ordinary format version, so creation-source and column
sequence validation keep their existing contracts. Unscoped predecessors omit
the new descriptor entirely. A fresh catalog can prepare against journal head
zero; the catalog incarnation and owner/name proof are captured in one native
read transaction. Preparation performs no logical DDL effect. Cluster preparation
and encoding do not require the catalog mutation mutex; only final readiness
validation and proposal are serialized, and apply waiting occurs after unlock.

The receiving native transaction consults the bounded shared outcome journal
before testing current command liveness. Thus an accepted success or rejection
survives unrelated catalog revisions, executor retirement and restart. A changed
prepared command cannot reuse its nonce. Expired admissions cannot recreate an
object after eviction. Catalog revision disagreement is a definite retained
rejection, rather than an inference from the newest catalog hash. Successful
outcomes retain a fixed-size revision/object identity or removal flag, never an
unbounded resource/schema response. Hash-derived physical table IDs remain bound
by the prepared topology rather than masquerading as catalog-counter identities.

Physical topology admission returns an explicit result. Every refusal precedes
logical catalog/topology effects; once admitted, storage failures abort the transaction containing
both topology/catalog publication and the journal outcome. The caller no longer
recomputes resolver projections after writing topology to infer whether creation
was admitted. Tablespace replacement and topology creation/drop are mutually
exclusive under the existing native envelope validator.

Public SQL automatic connection enrollment is still gated. Ordinary DDL now
joins public recovery receipts and authorization labels; public view definitions
use the shared native journal with or without an enrolled command scope. Remaining
work includes automatic ingress enrollment and coordinated validation, retirement,
generation publication and table lifecycle mutation families. Straight-line
schema definitions and annotations now share the native journal and recovery
receipts with or without an enrolled command fence; presentation-only outcomes preserve schema and catalog epochs. A private native outcome is not evidence that those
complete public features are activated. The original parity inventory remains
unchanged by this command-boundary work. See the
[view command integration](sql-catalog-lifecycle-parity.md#view-definitions-share-native-command-outcomes).


The prepared catalog command Debug gate passes the standalone production startup,
preparation, submission, unrelated-mutation and cold-reopen case; ten native
command-fence contracts; and three decoder contracts. Its lifecycle campaign
covers cluster/standalone realms through both local and Raft apply, arbitrary
physical table IDs, database/namespace/tablespace evolution, stale/changed commands,
ended executors, bounded eviction, aborted transactions and installed snapshots.
Faults after provisional catalog effects include both an injected storage failure
and `CatalogGenerationChanged`: neither may be swallowed as an admission refusal,
and both must roll back catalog effects and the retained outcome together.


Range-bearing create/drop cases additionally verify complete membership removal
and reject an absent primary table with surviving range membership. That state
must not certify a completed drop or erase the remaining logical binding. Run
`zig build antfly-sql-catalog-command-test -Doptimize=Debug` from `zig/` for the
combined native and standalone contract gate. The separate
`antfly-sql-command-producer-test` Debug gate passes all 49 source/controller
regressions after the catalog atomicity change.


## Public advisory case closure

The native public locking Debug gate executes `sql-0089`, `sql-0090` and
`sql-0091` unchanged through independent authenticated HTTP connections. It
checks both key spaces, shared/exclusive conflicts, reference counts, NULL/void
results and warnings, commit/rollback, savepoint rollback, DISCARD, pgwire result
OIDs and disconnect cleanup, and cold logical-connection recovery. Its six tests
also qualify existing prepared transactions and sync-setting lifetimes; there
are no skips or leaks. The PostgreSQL 18 value oracle passes 221 assertions;
the independent conflict and wait/deadlock oracles pass 134 and 31 assertions.

Ingress pins the native metadata group from discovery instead of assuming the
main group. Read-only remote DDL preparation preserves its authenticated body
fence; the accepting mutation still independently checks the live command.
Lock/prepared ownership control preserves bounded relation-publication scans,
since it changes no relation source. The public sync campaign verifies catalog
activation under command traffic without increasing its request deadline.
Transaction control borrows only the exact outer execution lease, and completion
loads durable state using the receiving allocator.

Validation: `zig build antfly-api-sql-locking-test -Doptimize=Debug -j1`,
`zig build antfly-sql-command-producer-test -Doptimize=Debug -j1`, and
`zig build antfly-pessimistic-lock-authority-test -Doptimize=Debug -j1`.
