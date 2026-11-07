# SQL notifications

LISTEN, NOTIFY and UNLISTEN use the existing SQL connection, command-source and
transaction-decision owners. Notification intents are not request-side callbacks,
and recovery never evaluates SQL again. The three original notification cases
(sql-0126–sql-0128) are qualified through mounted native, PostgreSQL and recovery
gates. Publication/subscription replication and the `pg_notify()` function are
separate feature surfaces.

## Transaction and delivery boundaries

Each transaction owns a bounded ordered intent list. Savepoints capture its
length; rollback truncates the list and its duplicate-event index together.
Identical channel/payload pairs fold within one transaction, preserving first
occurrence order. Distinct transactions never fold together. LISTEN and UNLISTEN
become effective at commit. A listener receives no new messages while inside a
transaction. Previously offered HTTP pages can still be acknowledged there.
READ ONLY transactions permit notification commands. PREPARE rejects pending
notification or registration effects with PostgreSQL's `0A000` behavior.

The immutable SQL decision includes the exact connection generation, sender PID
and intent list. Capacity admission precedes the irreversible decision. The same
native writer updates reservations, registrations, ordered block headers and
immutable payloads. A block becomes deliverable only after the existing commit
visibility and participant-retirement barrier. A later ready block cannot bypass
an earlier relevant block still waiting at that barrier.

Polling can resume an already persisted source-completion control. It cannot
create a command ticket, grant an execution permit or replay SQL. Connection
close retires the PID binding and registrations through the existing bounded
native close job. DISCARD retains the PID, advances the connection generation
and removes registrations atomically with native command completion.

## Public transport

Pgwire sends NotificationResponse frames at an idle transaction boundary,
including when the client sends no further SQL. The sender PID is reserved in
the native metadata group before BackendKeyData is advertised. A collision can
change the candidate PID only before that advertisement and before user SQL.
An idle socket keeps one structured reader alive across notification timers;
timer ticks do not cancel and restart the reader. Frames are acknowledged only
after a successful flush. An interrupted flush leaves the offered prefix intact.

HTTP uses `POST /sql/connections/{connection_id}/notifications`. It is separate
from SQL execution so a delivery failure cannot make a committed statement look
safe to retry. A page has an opaque 20-character cursor; omitting its ACK replays
the unconsumed prefix. Delayed duplicate ACKs remain harmless after reclamation.
The schema generates the endpoint and public types; Python, TypeScript and Go also
provide bounded, validated polling wrappers. No SQL retry occurs in this path.

Every poll checks the current principal instance, connection owner/generation and
source state. Native apply independently joins the actual backend binding and
database/incarnation proof. Pgwire's compact shared-head cache stores neither
authorization nor message pages. Fresh listeners and listeners with a backlog
always fetch their own page. Idle peers can reuse a recent database revision,
with one bounded refresh owner and an expiration fence. Ready backlog uses a
shorter timer; a visibility barrier retains the idle interval.

Socket liveness is process-bound and distinct from a client-controlled HTTP TTL.
Only the matching wire epoch can renew its local connection record; that renewal
does not change SQL lifecycle revisions or replace the native source lease.
After process replacement, the previous socket epoch enters source recovery.

## Bounds and recovery

The current admission profile limits notification queues to 8 MiB per database
and 64 MiB per metadata group, including outstanding capacity reservations.
These are Antfly resource limits, not PostgreSQL's default queue size. They leave
headroom for the native metadata snapshot format. Admission failure happens
before commit; rollback does not require delivery capacity.

A transaction has at most 128 intents, 64 KiB of logical intent text and
128 KiB of encoded intent data. The encoded limit is checked while staging,
so highly escaped payloads cannot make the common decision envelope too large
to commit or roll back. Existing manifest/envelope limits also apply. Channel
identifiers have PostgreSQL's 63-byte boundary; payloads are UTF-8, exclude NUL
and contain fewer than 8000 bytes. A page has at most 128 events and 64 KiB of
logical channel/payload bytes. Its scan visits at most eight transaction blocks.
Bloom summaries skip definitely irrelevant blocks without decoding their bodies.
No notification commit walks all listeners.

Reclamation scans compact member cursors in bounded keyset slices, then removes
ready blocks in bounded slices. A productive cursor round restarts promptly at
its end; only a complete idle round sleeps. Its objective sleeps when
ACK/readiness epochs have not changed. ACK and readiness publication wake it. Only the active native
metadata leader schedules group reclamation. Header, reservation and aggregate
budget changes share the same accepting writer as the affected blocks.

Queue keys belong to the metadata snapshot projection. Installation validates
canonical records and payloads, checksums, PID forward/reverse ownership,
listener/member edges, cursor bounds, frozen-decision references, contiguous
blocks and exact reservation/byte totals. Persisted reclamation progress cannot
skip a listener that still pins an older prefix. Validation uses reusable block
scratch and compact totals; it does not decode the entire queue at once.

## Qualification

Run native gates from `zig/`, using the pinned compiler:

```sh
zig build antfly-api-sql-notifications-test -Doptimize=Debug -j1
zig build antfly-sql-notification-journal-test -Doptimize=Debug -j1
zig build antfly-sql-notification-storage-test -Doptimize=Debug -j1
zig build antfly-sql-notification-decoder-test -Doptimize=Debug -j1
```

The PostgreSQL 18+ oracle is `scripts/check_sql_notifications_postgres.py`.
The mounted campaign executes sql-0126, sql-0127 and sql-0128 unchanged, then
checks ordered delivery, savepoints, rollback, cold recovery, DISCARD, READ ONLY
and acknowledgement boundaries. Socket tests independently cover idle delivery
and flush failure. The native storage campaign checks aborted writer visibility,
retained readers, WAL reopen, snapshot installation and corrupt checkpoint
rejection. Parser-only or oracle-only results do not close an original case.
