# SQL lock ownership

This is an implementation record for explicit SQL locking. The PostgreSQL 18
conflict matrices and lifetime rules are the semantic reference. The native
journal described here is not yet activation of the unresolved SQL cases.

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

Protocol 136 is reserved for private admissions, while the advertised protocol
remains 135. The native receiver rejects private proposals below 136, including
late proposals that bypass preflight, without installing a job or reservation.
Changing the advertised protocol also requires matching service/proposal floors
and enabled-protocol proposal/snapshot/restore qualification. Those changes,
private local-session result adoption, immutable lock transaction scope admission,
connection/expiry fencing and the public ordered SQL effect provider remain
required before complete SQL locking activation.


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

Private receipt adoption remains an unapplied proposal. It must atomically retain
the exact private decision
before acknowledging metadata, and the recovery dispatcher must preserve private
purpose across every lost reply. The optional version codec alone does not enable
that path. The separate protocol-136 source proposal also remains unapplied. No
SQL inventory case has been closed by these additional recovery foundations.

Standalone ownership needs explicit qualification as part of public activation:
the HTTP server uses node zero when no session router is configured, while the
existing prepared-session reference validator rejects node zero. A standalone
owner must be fenced by its catalog incarnation and standalone realm rather than
mapped to a fabricated cluster node. Clustered envelopes must still reject the
local sentinel. Both receipt-work selection and cold session decoding need the
same owner semantics before this path is exposed.
