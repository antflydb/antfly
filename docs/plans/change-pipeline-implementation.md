# Change pipeline implementation ledger

This tracks implementation of #1698 and #1639 on `feat/change-pipeline` (draft
PR #1700). Both issues remain open until all required implementation and
qualification are complete. The shared proposed design is in PR #1699 on
`docs/change-pipeline-contracts`, at `docs/design/change-pipeline-contracts.md`.

## Required outcome

Implement destination integration, retained committed history, public changefeeds,
consistent bootstrap and bounded retention, actual pgwire logical replication /
pgoutput, and qualification with a pinned existing PostgreSQL CDC client. Do not
substitute polling SQL for the replication protocol or claim root-local history
alone establishes cross-shard transaction completion.

## Existing prerequisite (71151ee55f)

- Shared source transaction envelope, preserving lake retry identity.
- PostgreSQL polling only exposes complete source transactions.
- CDC progress advances after the whole source transaction applies.
- Unsupported mutation messages fail rather than silently advance progress.
- The PostgreSQL parser now retains update before-images and old-image kind,
  identity-key column flags, and before/after completeness. Key-only NULL
  placeholders are removed from non-key fields; unchanged TOAST remains absent
  rather than becoming SQL NULL. Tuple/schema column-count and trailing-byte
  mismatches fail before granting a checkpoint. Value ownership covers both
  images and malformed tuple error paths.
- Expanded parser checks cover changed keys, full old rows, NULL versus unchanged
  TOAST, delete old-row identity and malformed tuples. The full
  `source-transaction-test` target passes; current log:
  `zig/source-before-images-final-test.log`.
- `zig build source-transaction-test -j2` passes native CDC, parser, envelope,
  retry and real Iceberg row-commit checks.

## In progress: native committed history

New implementation files:

- `storage/committed_frame.zig`: bounded versioned before/after tuple frames,
  namespace/transaction/schema identity, authenticated chunk descriptors and pull
  decoding. Standalone frame corruption/cross-chunk test passes.
- `storage/committed_history.zig`: primary-transaction history, persistent consumer
  epochs/acknowledgments, bounded reclamation, before-image capture, and durable
  reservations for transaction intents. No public activation is exposed yet.
- `storage/transaction_intent_value.zig`: extracted shared intent codec and exact
  normalization size accounting, used by transaction and history reservation.
- `storage/docstore.zig`: captures both writer APIs before overwrite and stages
  history before physical commit, alongside existing source authority/revisions.

New `zig build docstore-history-test -j2` target (also registered in unit coverage).
Four storage/transaction tests and the standalone frame test pass: coalesced
images/rollback, reserved quota/ack/reclaim, consumer epoch fencing and shared
retention, and real transaction-manager prepare/reopen/resolve/idempotent recovery.
The 56-case transaction/reservation suite also passes. Current logs:
`zig/committed-history-final.log` and `zig/committed-history-regressions.log`.

Descriptors index every event ordinal and authenticate each immutable chunk, so
resuming late in a transaction does not scan its row prefix. The reader owns one
reusable chunk buffer; returned tuple/schema fields belong to the caller's page
arena. Consumer removal preserves outstanding prepared credits through resolution;
the last consumer cannot invalidate an already prepared commit.

Capture uses the original transaction ID when resolving durable intents. Native
source clock advancement reuses existing authority advancement or advances once
when history is the only consumer. Raft roots require an observed same-transaction
ordered apply marker. Inactive stores cache absence without copying rows.

Important next checks before exposure:

- Exercise real DB row/relational/SQL, cascades, TTL and rollback/savepoint paths.
- Cross-shard completion must be coordinated from durable transaction decisions;
  individual root frames are not complete global transactions.
- Source movement, split/merge/restore and root incarnation fencing need history
  transport/retention capability qualification and protocol gating.
- Root history is private storage infrastructure. Authentication, live policies,
  expiration, snapshots, slot LSN ordering and public cursors are still separate.

## In progress: durable native source admission

- Private `storage/source_admission.zig` defines source binding, immutable
  batch identity, a monotonic source sequence separate from provider checkpoints,
  canonical participant-effect digests, and bounded durable receipt windows.
  Progress heads and receipt rings are isolated by table/source identity; epoch
  or generation changes cannot implicitly abandon the previous binding.
- Source head, receipts, and ring eviction compile to ordinary native transaction
  intents with an exact source-head predicate. Rows and source progress commit
  together; a receipt is never a best-effort second write.
- Retried committed batches resolve before current-schema validation or transform
  expansion. Pending intent receipts also fence changed payloads before recovery.
  Expired receipts reject old source sequences even when opaque checkpoints cycle.
- Metadata writes are protected from ordinary batches and excluded from row
  preparation and row history; trusted source admission is the only compiler.
- Native engine and runtime error boundaries preserve source/history outcomes.
  All 56 runtime error ABI tests and the bounded receipt/pending-replay test pass.
  The new `zig build source-admission-test -j2` target exercises the bound
  destination through native transaction callbacks. The expanded native run
  passes receipt eviction, acknowledgment, reopen, exact retry, conflicting-payload
  rejection, private-key protection, repeated prepare, and a real prepared
  transaction across reopen. Prepared source retries resolve the existing vote
  rather than expanding transforms again; a conflicting retry cannot abort it.
  Transaction read validation excludes the transaction's own pending intents,
  while retaining version checks and conflicts with other writers.
- `zig build source-admission-test source-admission-codec-test
  docstore-history-test docstore-transaction-reservations-test -j2` passes:
  two bound native integration tests, bounded codec/replay tests, four retained
  history tests, and all 56 transaction/reservation regressions. Final local log:
  `zig/source-admission-final-recovery.log`. `make gc` completed safely.

The source command is currently private. Distributed routing/anchor ownership,
replicated transport, managed connector invocation, and writable-Iceberg adapters
still require integration; this does not advertise those destination capabilities.

## In progress: transaction-wide source projection and destination apply

- `storage/source_effects.zig` owns a bounded source-ordered projection plan.
  Full deletes remove preceding transforms for that key; later transforms retain
  their individual upsert behavior against the deleted base. Limits count all
  input operations, including superseded work, and rejected partial plans cannot
  be submitted. Final effects and their bytes belong to the plan arena.
- Managed streaming compiles every member of a complete provider transaction
  before the first destination mutation, then calls canonical `commitBatch`.
  This is required even for one destination table: ordinary routed table batches
  can fan out sequentially across shards. Unsupported transaction capabilities
  fail before falling back to any non-atomic table writes.
- Routed projection uses historical images for key changes, enter/update/leave
  and deletes. FULL before-images can fill unchanged-TOAST holes in new images.
  Filtering or cleanup requiring unavailable historical fields fails explicitly
  with `ReplicationProjectionIncomplete`; it cannot advance progress by
  treating omitted fields or identity placeholders as historical NULL values.
- Private native source commands are rejected at routed/coordinator/participant
  boundaries until source-head anchor and participant transport are implemented,
  instead of silently discarding their durable admission metadata.
- `zig build source-transaction-test -j2` passes the expanded 18-case
  snapshot/stream/parser suite, source envelope checks, and two planner tests.
  Coverage includes native rollback when a later member is invalid, atomic
  multi-table request shape, old-key removal, route enter/leave, historical TOAST
  reconstruction, missing-capability/incomplete-image rejection before mutation,
  snapshot routes, configured transforms, and work-permit revocation. Current
  log: `zig/source-atomic-routing-qualified-final.log`.

This improves atomic application but does not yet connect managed source progress
to durable admission receipts. Lost acknowledgments can still replay transforms
until the source actor persists sequence/pending identity/accepted timestamp and
uses destination receipt resolution. Native source schema qualification must also
validate that a mapping without FULL old rows uses a stable replica-identity key;
arbitrary non-identity key expressions cannot silently inherit that guarantee.
Writable-Iceberg transformation/admission and accepted-versus-catalog-committed
progress remain separate required adapters.

## In progress: durable source actor control authority

- `metadata/source_control.zig` defines immutable source/provider/destination
  binding, explicit generation, stable pending sequence/batch/checkpoint/time,
  payload and delivery-plan digests, original transaction identity and recovery
  manifest. Catalog ordinal is a locator, not source identity. Identity hashes
  use explicit versioned field projections.
- Accepted and authoritatively committed frontiers remain separate. Bounded
  outstanding receipts retain the correlation while writable-Iceberg catalog
  commit lags WAL acceptance. Native adapters can report an already committed
  receipt atomically. This state never claims that an index has published.
- Reservations retain worst-case encoded receipt credits before destination
  admission. Operation count and encoded-byte limits apply independently; only
  committed-prefix proof releases receipt backlog.
- Catalog/revision/binding CAS fences mutation. Replan requires a not-admitted
  proof and retains the original logical batch, source offset, sequence, payload
  and accepted timestamp. Ambiguous attempts cannot be discarded. Rejected
  attempts can retire only under their original reservation identity; a new
  generation requires complete retirement first.
- Metadata transition tag 83 persists the authority and a constant-time per-table
  work counter. Live work blocks table retirement. Both state and counters are
  included in Raft snapshots; the coordinated decoder floor is version 90.
- Private service helpers use linearizable reads, preflight validation, durable
  proposal/application receipts and exact observed state. Lost acknowledgments
  and failed CAS never grant source progress. Corruption, backpressure and
  conflicts retain their runtime error identities.
- Seven standalone control tests pass, along with real metadata reopen, stale
  catalog fencing, snapshot install, table-drop protection, snapshot projection
  registry and decoder-floor checks. `zig build source-control-test
  source-transaction-test -j2` passes, including all 18 CDC regressions. All 56
  runtime error ABI tests pass. Current logs:
  `zig/source-control-complete-qualification.log` and
  `zig/source-control-error-abi-test.log`.

The managed source actor and destination adapters still need to invoke these
operations. Receipt/fencing references are private adapter outputs, not
self-authorizing public strings: native and writable-Iceberg adapters must prove
their engine-specific outcomes before accepting, committing, replanning or
retiring source work. No source actor/public capability is advertised yet.

## In progress: delivery orchestration and native outcome recovery

- `metadata/source_delivery.zig` now orders capability validation, linearizable
  source-state reads, durable reservation, destination admission, observed
  metadata acceptance and destination coordinator release. A failed release
  blocks the next reservation so its receipt cannot be overwritten while an
  acknowledgment debt remains. Recovery resolves the original pending manifest
  without provider rows when destination admission already happened.
- The actor preserves the original pending timestamp/sequence/plan on retries,
  checks exact batch/checkpoint/predecessor/payload identity, and never orders
  opaque checkpoints. Acceptance and authoritative commit remain independent
  adapter outputs. Unknown destination or metadata outcomes stop progress.
- Private table-write callbacks resolve a native prepared vote using the durable
  command and original coordinator locator, without schema validation or
  transform expansion against newer rows. Immutable table ID and the persisted
  payload fingerprint are verified before using the locator. Coordinator release
  verifies a committed source receipt and remains idempotent after metadata GC.
- The delivery fault test passes with a stateful simulated destination and source
  control service: lost destination response, recovery without input rows, lost
  metadata acceptance acknowledgment, release failure, payload mismatch and
  cyclic checkpoints. This is orchestration qualification, not proof of a
  production native/Iceberg adapter or an existing CDC client.

`zig build source-admission-test source-control-test -j2` passes, including
native prepared-vote recovery through callbacks after reopen/rename, explicit
coordinator metadata collection, receipt-only recovery after collection, and the
metadata persistence/snapshot/decoder checks. Standalone delivery/control tests
also pass (8 tests, no leaks). Logs: `zig/source-delivery-test.log` and
`zig/source-delivery-native-qualification.log`.

`metadata/native_source_delivery.zig` now implements the single-root native
adapter: capability discovery checks persisted native ownership and exact
namespace; preparation repeats the ownership check under the apply fence.
Source identity and scope fence command construction, and the original
coordinator locator survives rename/reopen. Admission validates the actual effect
digest against the stored manifest. A bounded receipt retains command recovery
authority with base64 provider strings, so worst-case JSON escaping cannot exhaust
reserved receipt credits. Durable destination commit is distinct from index
publication. Recovery and release verify persisted engine receipts.

Real database plus metadata actor recovery tests pass: loss of the destination
reply, both stores reopened, current table locator changed, recovery without
source rows, loss of metadata acceptance acknowledgment, a release outage and
changed-payload retry. The counter changes exactly once per accepted batch.
The receipt codec's maximum-length/control-character round trip passes too.
Checkpoint-only commits and unscoped/Raft-root rejection checks pass. The actor
relocates the same immutable source after catalog reordering without resetting
its pending reservation or frontiers. Standalone pending-relocation and real
persisted catalog-reorder checks pass. `zig build source-admission-test
source-control-test source-transaction-test -j2` passes, including all 18 CDC
parser/stream regressions, metadata snapshot/recovery/decoder checks and native
adapter qualification. Logs: `zig/native-source-adapter-final-qualification.log`
and `zig/source-delivery-relocation-test.log`.

Native BEGIN now atomically persists the command fingerprint as a distinct source
purpose (journal v8). Ordinary records retain the existing v6/v7 encoding and
cannot upgrade into source transactions. Source PREPARE requires the original
purpose and freezes its complete final intent set; exact retries preserve the
vote and revision. DB retries of prepared source commands skip transform/default
and identity expansion. Pending COMMIT/ABORT decisions require the same purpose;
commit before a complete prepare is rejected. Source-owned pending records stay
under source-control recovery, rather than ordinary timeout/presume-abort loops.

Purpose-aware native recovery, journal and server recovery qualification pass:
`zig build docstore-transaction-reservations-test source-admission-test
antfly-server-db-test -j2` runs 57 journal tests, 77 server integration/recovery
tests and five native admission/recovery tests with no failures or leaks.
The checks include mixed-purpose BEGIN/PREPARE/decision rejection, immutable
source intent sets, source record reopen/corruption, v6 compatibility, rejection
of commit before prepare, and source-owned timeout recovery while ordinary stale
transactions still abort. Log: `zig/source-purpose-complete-qualification.log`.
Distributed admission still must
carry source purpose and original participant authority through its private RPCs;
current distributed source markers remain rejected. Explicit destination
scope retirement/reseed remains required.

## In progress: proven native replanning

- Native commands distinguish logical batch identity from delivery attempt.
  Attempt zero preserves existing IDs/fingerprints; subsequent attempts keep the
  source sequence, payload, checkpoint/predecessor and accepted timestamp.
- One checksummed 156-byte fence per immutable native table/source closes failed
  attempts independently of journal retention. BEGIN and PREPARE reject delayed
  closed attempts, including after journal cleanup/reopen. A later logical
  source sequence can begin at attempt zero again.
- Native retirement proves no committed receipt or prepared vote exists. It
  atomically aborts and fences an empty source BEGIN, or fences an absent/aborted
  attempt. It cannot certify absence after another attempt committed the logical
  sequence. An ordinary transaction-ID collision is preserved as ordinary work.
- Replanning first resolves original prepared/committed work. Only a verified
  not-admitted outcome can replace delivery authority. The original reservation
  digest makes a lost metadata reply retryable without closing its replacement.
  Both ordinary recovery and replan refresh same-binding catalog ordinals.
- A persistent fence permits immediate cleanup of the aborted source journal
  after intent/outbox/participant debt drains. Ordinary records and ordinary
  retry-window retention keep their previous rules. Cleanup refuses pending or
  mismatched-purpose records and requires the durable fence.
- Final qualification passes: two source-admission codec tests, 58 journal
  tests, five native admission/recovery tests, metadata recovery/snapshot/decoder
  checks, all 18 CDC parser/stream regressions and all 56 runtime error ABI tests.
  The real-store scenario covers an empty source BEGIN, lost replan reply,
  immediate journal pruning, reopen, rejected delayed old calls, catalog reorder
  between reserve/replan, immutable prepared votes, and ordinary ID collisions.
  The counter reflects each accepted logical batch once, preserving original
  sequence/checkpoint/time across replacement plans. Commands:
  `zig build source-admission-test source-control-test source-transaction-test
  source-admission-codec-test docstore-transaction-reservations-test -j2` and
  the standalone runtime error ABI tests. Logs:
  `zig/source-replan-final-qualification.log`,
  `zig/source-attempt-fence-codec-test.log`, and
  `zig/source-attempt-error-abi-test.log`.

Managed runner integration, destination generation retirement/reseed,
distributed native authority and writable-Iceberg adapters remain required;
this private path does not advertise a public ingestion/feed capability.
Managed
runner wiring, distributed source authority and writable-Iceberg adapters remain
required. The callbacks and actor do not yet
advertise a public ingestion/changefeed capability.

## Durable outbound control authority

The private `metadata/changefeed_control.zig` contract now persists immutable
owner/authorization and table-incarnation bindings, root enrollment grants,
consumer epochs, delivery cursors, ACK positions and cleanup obligations through
metadata Raft. Transition tag 84 requires coordinated decoder floor 92. A
checksummed authority counter and feed state are updated atomically and included
in metadata snapshots. Names and index/schema publication do not change a table
incarnation binding.

A bounded opaque cursor ledger reconstructs the position of each retained
cursor. Partial-frame delivery never authorizes whole-frame reclamation. Client
ACK, native reclamation, snapshot ACK, snapshot-pin release and consumer release
remain separate persisted steps. Bootstrap and consumer leases produce explicit
reset-required states; restart requires cleanup and creates a new consumer epoch.
Private enrollment, cut, page and release proofs still require validation by the
runtime adapter. Nonzero digests in this metadata contract alone do not certify
a globally coherent snapshot or authorize data access.

Focused checks cover control transitions, forged and expired cursors, bounded
unacknowledged delivery, bootstrap deadlines, owner rebinding, corrupt state,
lost metadata acknowledgments, rejected proposals, interleaved authority-counter
updates, real metadata-store reopen, snapshot installation and retained cleanup
debt. Runtime root enrollment/read/bootstrap, live authorization, public HTTP/SQL
feeds and pgwire are still outstanding; this private control layer advertises no
public changefeed capability.

## Root consumer fencing and bounded reads

`storage/history_consumer.zig` binds retained consumers to a metadata authority,
immutable definition and monotonic enrollment grant. Cleanup advances a single
checksummed root floor, including when enrollment never arrived or its reply was
lost. Delayed unknown grants at or below that floor cannot recreate pins.
Existing consumers survive unrelated cleanup; refreshed grants preserve their
consumer epoch, start and ACK. Lease renewal remains a separate conditional
operation, and metadata close/reset must precede root release. Expiry alone never
silently deletes a retention obligation.

`DB.nativeHistoryConsumerControl` serializes these transitions under the native
apply owner and persists an empty root's trusted identity during enrollment.
`DB.nativeHistoryPage` returns page-owned authenticated before/after images from
one committed frame, directly seeking the retained event ordinal. The reader
checks encoded event size before allocation, distinguishes a partial frame from
whole-frame completion, and does not ACK on read. These private native methods
reject foreign and Raft-owned roots. Raft command/RPC wiring, coordinated
snapshot pins, feed reconciliation and global transaction completion still need
implementation; a root page alone is not a public changefeed or proof of global
commit completion.

Qualified with `zig build native-history-consumer-test docstore-history-test
changefeed-control-test -j2`: real native DB enrollment before the first write,
reopen, partial-frame paging and replay, byte-budget/expiry rejection, ACK versus
GC separation, page ownership after GC, Raft/foreign-root rejection, fixed-space
retirement-before-enrollment fencing, rolled-back control changes, lost replies
across renewal, and metadata grant refresh preserving read position. The normal
changefeed control and unit targets now include the native integration check.

## Native root adapter and table-source capabilities

The private `TableWriteSource` contract now appends optional native consumer
control and page-read callbacks. Bound sources invoke them through the existing
callback boundary; missing capabilities return an explicit unsupported outcome.
`metadata/native_changefeed_root.zig` binds an adapter to the observed immutable
metadata table incarnation and actual persisted native namespace. The current
adapter exposes local native document roots; object, relational, snapshot and
lifecycle capabilities require their respective implementation/qualification.

The adapter recovers original enrollment epochs/starts across lost replies,
applies a renewed metadata lease to the recovered native consumer, reconstructs
positions from retained opaque cursors, and flushes ACKs from the root's observed
predecessor after a lost native reply. Owner-scoped collection can reclaim a
bounded number of frames even after the final consumer is retired, while the
shared native floor protects other consumers and prepared credits. Moving a
closing/reset feed to another storage owner preserves its retired enrollment
grant rather than creating a grant newer than its close fence.

Integration exercises the table-source callbacks with real native and metadata
stores: commit between enrollment and its lost reply, lease extension, partial
and whole-frame ACK, lost native ACK followed by a newer target, root-release
reply loss, both stores reopening, collection after final release, and rejection
of delayed enrollment after collection. These are root/control recovery checks.
The adapter does not authenticate a public client, project/filter images, certify
a distributed completion or construct a consistent snapshot. Provisioned/hosted
root selection, owner RPCs, feed reconciliation and public transports remain
required before advertising public changefeeds.

## Delivery coordination and exact projection

`metadata/changefeed_delivery.zig` coordinates authorized prepared pages with the
durable receipt ledger. It rechecks live authorization before source work,
before issuing a receipt and before returning data; checks expiry with a fresh
clock; and returns no data on an unknown metadata outcome. A retry reconstructs
the original issued interval and verifies its public payload, provider proof,
event count and advances. Changing page limits cannot silently repartition an
already issued delivery. Full cursor windows reject new work before another
source read, while retained deliveries remain replayable. Client ACK is durable
metadata progress; native ACK/collection remains a separate owner operation.

`common/changefeed_projection.zig` evaluates bounded exact predicates on both
already authorized historical images before projecting fields. It distinguishes
insert/enter/update/leave/delete, treats missing separately from JSON null,
preserves arbitrary decimal precision/exponents with the existing exact numeric
kernel, and masks inaccessible image existence before classifying an operation.
Rows contain only requested literal top-level fields; an empty field selection
means all provider-authorized fields. Updates remain version notifications when
only unselected fields changed. Iterative arena-owned cloning preserves deeply
nested values without recursive stack growth or float conversion.

Focused tests qualify lost apply replies, rejected proposals, forged ACKs,
expired/unknown/misbound cursors, full receipt windows, replay payload mismatch,
policy change/revocation before or after receipt commit, every filtered
transition, hidden images, decimal precision, and deeply nested values. These
components are included in `changefeed-control-test` and the unit target. A
production provider still must authenticate against real current policy,
produce canonical authorized images, certify distributed completion, and bind
its prepare/replay implementation to retained root history. Public HTTP/SQL,
bootstrap, lifecycle categories and pgoutput remain unfinished.

## Transaction scope and private projection continuation

Retained frame headers now record an explicit transaction scope in a previously
reserved byte. Direct root commits are `root_local`; intent-resolution commits
are `coordinated`; legacy headers remain `unknown`. Readers reject unknown
scope tags, and transaction IDs cannot stand in for this completion evidence.
A coordinated frame still requires a retained participant/outcome qualification
before it can produce a completed public transaction.

Private provider continuation is now part of delivery identity, replay proof,
latest position and the ACK checkpoint. Its canonical base64url representation is
bounded to 64 KiB and shares the existing bounded receipt ledger. Nonempty
continuations use version-2 cursor/proof identities; empty-context identities
remain unchanged. Decoder floor 92 gates the writes. State parsing authenticates
the stored JSON representation while admitting optional legacy defaults, without
rebinding old cursor identities; typed validation precedes digest traversal of
untrusted nested values.

The root-local projection continuation codec carries whether earlier pages
exposed an authorized row. It uses at most 819 bytes for all 4096 roots, binds
consumer/definition scope and exact root positions, tolerates owner/grant movement,
and clears completed-frame visibility. An entirely hidden partial transaction
retains explicit private state rather than emitting a visible completion marker.
A legacy partial cursor without that state requires resynchronization. The native
provider still needs to consume these facts; this codec alone does not certify
distributed completion or expose a public feed.

Qualification includes frame scope across native batch/transaction recovery,
legacy envelopes/cursor identity, continuation replay and tampering, ACK/reopen,
Raft snapshot installation with pending continuation/cleanup, maximum root fanout,
and rejection of deeply nested unknown metadata before digest traversal.

## Native document delivery provider

`metadata/native_changefeed_delivery.zig` now joins retained native pages,
authorized historical image projection and durable receipt coordination. It
emits a versioned document envelope with feed-scoped event/transaction/version
identities, immutable table IDs, lossless UTF-8/base64url keys, projected
before/after rows, and explicit page-versus-transaction completion. Feed versions
are not document conditional-write tokens. Mutable table display names and
private source positions are not part of event payloads.

Replay uses the original recorded interval and continuation. A no-data heartbeat
replays as a heartbeat even after a new commit arrives. Byte/event limits cannot
silently repartition an issued delivery. Private visibility continuation permits
a filtered empty final page to complete a previously visible transaction while
entirely filtered transactions emit no public transaction boundary. Payload byte
counts are checked before output allocation; iterative JSON emission preserves
exact numbers and arbitrary nesting without the formatter's fixed-depth stack.

The provider requires a policy implementation that verifies the principal,
replication privilege, live table/root-set incarnation and policy revision, and
authorizes/redacts both historical images. The current source is a single local
JSON document root with explicit `root_local` capture evidence. It rejects
coordinated/legacy transactions, relational tuples, object/lake roots, bootstrap
and lifecycle requests pending their separate qualified implementations. These
are remaining implementation obligations, not substituted completion criteria.

A real native-store plus real metadata-store integration checks lost delivery
reply followed by a live-row update, exact historical replay, field exclusion,
partial-frame completion, filtered leave/enter, completely hidden transactions,
heartbeat replay after a later commit, revocation before cursor inspection, and
rejection of unqualified coordinated completion. Production policy/root routing,
HTTP/SQL/pgwire exposure, distributed outcome proofs and bootstrap remain required.

## Remaining implementation phases

1. Finish root history tests, prepared credit accounting and native integration.
2. Destination-independent admission with durable source checkpoint, replay
   identity/payload fencing and native/Iceberg adapters; managed PostgreSQL routing.
3. Durable feed/slot metadata and root enrollment, coordinated commit completion,
   stable delivery identities and independent per-index readiness events.
4. Consistent durable snapshot/bootstrap cuts, paging/restart, retention pins,
   consumer ack/expiry/backpressure, and source topology transitions.
5. Public HTTP/SQL interfaces and generated specs/SDKs, live authorization,
   before/after projection filtering and revocation/reset semantics.
6. pgwire replication startup, commands, slots/publications/catalog subset,
   CopyBoth/feedback/keepalive/cancel, pgoutput typed messages and stable LSN map.
7. Pinned existing-client snapshot/stream/reconnect/failure qualification, including
   inbound PostgreSQL -> destination commit -> outbound feed with independently
   advancing indexes; record actual versions and unsupported capabilities.

## In progress: PostgreSQL logical wire codec

`pgwire/pgoutput.zig` implements bounded protocol-v1 message encoding for begin,
commit, relation/type, insert/update/delete/truncate, origin and logical messages.
It preserves SQL NULL, empty text, binary and unchanged-TOAST distinctions,
including old tuples. CopyData WAL envelopes and keepalives use network byte
order; strict standby-status decoding treats positions as untrusted claims.
UTF8 text and C-string fields reject embedded NUL or invalid encoding. PostgreSQL
epoch conversion handles negative times and overflow.

The codec uses an explicit LSN type. It neither assigns LSNs nor acknowledges
history. Durable slot mappings must establish which issued, complete transaction
a reported flush position covers before it can affect feed retention. The codec
has no authentication, slots, exported snapshots, replication commands or socket
activation. Protocol negotiation and full existing-client qualification remain
required; this codec alone does not satisfy the pgoutput requirement.

`zig build pgoutput-test -j2` exercises independently written wire fixtures for
all v1 message variants, framing, malformed feedback, bounds and allocation
failure cleanup. This target also runs in the regular unit suite.

## In progress: durable PostgreSQL slot correlation

`metadata/pg_slot.zig` defines a slot-scoped virtual byte ordering domain, distinct
from native frontiers and inbound source positions. Each issued complete
transaction binds its before/after feed cursors, immutable serialization digest,
completion authority, byte length, commit offset, xid, and commit time to one
persisted LSN interval. Replay retains those assignments. Interior restart
positions rewind to Begin; expired positions require resynchronization.

The slot state is embedded in the existing durable changefeed metadata record.
`Mutation.slot.confirm` advances its confirmed LSN and native feed acknowledgement
in one metadata transaction. Feedback beyond issued positions fails; feedback
inside a transaction cannot release that transaction. Ordinary native ACK calls
cannot bypass slot confirmation during streaming. Physical root reclamation is
still separate and follows the shared acknowledged floor.

Slot sessions use durable actor/fence tokens, bounded leases, explicit takeover,
and monotonic observed time. Pending mappings and serialized bytes have quotas;
slow clients receive backpressure, never silent eviction. Slot names are scoped
by immutable database identity and select a deterministic metadata feed ID.
The decoder floor advances to 93 before these state/mutation shapes are admitted.
Legacy envelopes without slot fields remain readable.

These are private authority contracts. Production replication privilege checks,
publication/catalog integration, slot creation/drop/recreation and socket
activation remain unfinished. In particular, the provider must implement bounded
durable committed-transaction spooling and certify complete projected outcomes
before issuing a mapping; a caller-supplied digest is not a substitute. No public
PostgreSQL capability is advertised on the strength of the metadata tests.
Native feed restart rejects a configured slot rather than resetting its LSNs.

Qualification: `zig build changefeed-control-test docstore-history-test -j2`
passes, including the actual metadata-store reopen/snapshot/takeover/lost-reply
case, native consumer/provider recovery, retained history and decoder-floor
checks. Standalone control tests pass 20 cases, including slot lag quotas,
allocation failures, partial-frame rejection and legacy envelope decoding. Logs:
`zig/pg-slot-durable-final-qualification.log`, `zig/pg-slot-control-test.log`.


## In progress: explicit replication permission

The auth OpenAPI contract now defines `replicate` as independent table-export
permission. `read`, `write` and `curator` do not imply it; replication-only grants
do not imply SELECT or mutation access. Existing `admin` grants include it. The
native enum appends the new member to preserve existing ordinal assignments.
API-key grants intersect with current owner authority, including inherited roles
and live revocation. Historical projection and ownership remain additional
requirements, not consequences of possessing the grant.

Signed gateway `operations` and scoped `table_operations` can carry this explicit
grant. Native/public permission conversions, generated clients, and Antfarm's
permission editor carry the same value. `make generate` completed with local Zig
caches and the pinned frontend toolchain, including TypeScript/frontend checks.
Unrelated model-explorer kernel output from that full generator was restored.

This permission alone does not expose a stream. Production changefeed and
logical-replication admission still must require the grant, bind immutable user
and table identities, reauthorize historical row/field policy, and check slot or
publication ownership. Runtime capability/upgrade admission remains necessary
before public replication activation.

Qualification: `zig build antfly-usermgr-test lib-usermgr-abi-test -j2`
passes the native authorization, storage and independent ABI cases. The added
ABI case and focused signed-gateway/public-conversion cases pass with
`zig build antfly-api-auth-test lib-usermgr-abi-test
-Dtest-filter='trusted principal replication grants'
-Dtest-filter='trusted principal table operations' -j2`.
`GOWORK=off GOEXPERIMENT=simd go test ./...` passes in `go/pkg/sdk` with local
HTTP test listeners enabled. Logs: `zig/replication-privilege-usermgr-test.log`,
`zig/replication-privilege-final-qualification.log`,
`go/pkg/sdk/replication-permission-sdk-qualified.log`, and
`change-pipeline-replication-generate-qualified.log`.

## In progress: replication command and connection-owner boundary

`pgwire/replication_commands.zig` parses bounded logical replication commands and
negotiates committed-only text pgoutput v1. Slot creation retains snapshot and
temporary choices for capability-aware backend admission. Known replication
prefixes cannot fall through to SQL on malformed input. Publication names retain
quoted identity; LSN parsing rejects overflow, options reject duplicates, and
unsupported protocol guarantees fail before metadata work.

`Backend.VTable.open_replication_session` is an optional authenticated owner
factory. The protocol refuses logical startup when absent, restricts logical
sessions to simple queries, and sends administration to that separate owner.
Authorization runs before and after work; result shape and boundedness are
checked. Owner resources close before credentials are released. Known committed
or uncertain slot mutations report completion uncertainty without fabricating
SQL transaction reconciliation work.

The production native adapter still has no factory. START requires the separate
stream owner described below; a missing callback fails explicitly. Durable
spooling and native integration remain unfinished. This boundary does not
qualify any existing CDC client.

Qualification: `zig build pgwire-test pgoutput-test -j2` passes with local
listeners permitted. It covers the full existing wire/session/lifecycle suite,
new logical startup/owner/authorization/uncertain-outcome checks, and bounded
parser/codec/allocation-failure tests. Final log:
`zig/replication-session-reviewed-qualification.log`. An initial fixture bug
returned a runtime-created callback table by dangling pointer; static tables
fixed it, and the full suite was rerun successfully.

Slot bootstrap integration must account for CREATE_REPLICATION_SLOT having no
publication selection. The consistent database snapshot/history cut must exist
before START_REPLICATION supplies publication names. Do not discover/activate
only the selected roots at START and thereby lose intervening commits. The
slot's durable publication/projection binding must also fence changed replay
serialization; a different publication list cannot reuse an existing LSN mapping
with different bytes. Database-wide cut ownership and this binding are still
required integration work.

## In progress: duplex CopyBoth transport

`ReplicationSession.open_stream` supplies a connection-owned committed stream.
The coordinator serializes bounded polling, feedback and authorization; a
separate single-message reader runs concurrently. It never cancels/restarts
that reader between polling ticks, which could lose partially consumed frames.
Stream exit joins and drains tasks before releasing payloads/leases/credentials.
Renewal buffers are securely released on success, error and queued cancellation.

Provider calls and writes have bounded deadlines. Only valid, accepted feedback
extends the consumer deadline; keepalives and outgoing data do not. Keepalive
requests and direct replies use PostgreSQL epoch timestamps. CopyDone emits COPY
and START completion boundaries and leaves following query bytes untouched.
Errors after COPY begins disconnect instead of resuming an uncertain input frame.

Polling accepts the exact caller-maintained byte frontier and at most one
bounded complete pgoutput message from retained spooling. The provider's end
cannot regress or authorize acknowledgement of unsent bytes. Feedback is capped
by the sent frontier or a backend-validated reconnect position, including rewind
from an interior LSN to Begin. Sending/closing a stream never acknowledges its
history. Durable slot confirmation still belongs to the metadata authority.

`zig build pgwire-test pgoutput-test -j2` passes the full suite with local
listeners enabled. Live-socket cases cover delivery, feedback, renewal, following
commands, future acknowledgement rejection, revocation before payload,
idle keepalive/timeout, cancellation/shutdown and validated reconnect rewind.
Bounded frontend frame/renewal allocation-failure tests pass as well. Logs:
`zig/copyboth-complete-qualification.log`, `zig/copyboth-lifecycle-qualification.log`
and `zig/replication-input-codec-test.log`.

No production stream factory, durable committed spool or existing-client
qualification is supplied by these transport checks. Subsequent integration
must also advance over irrelevant retained events without inventing visible
empty PostgreSQL transactions/LSN increments or leaking hidden participant
counts. That needs a certified checkpoint-only native progress operation,
separate from delivery/confirmation of serialized PostgreSQL transactions.

## In progress: immutable committed PostgreSQL spool

`metadata/pg_spool.zig` stores complete committed pgoutput transactions as
content-addressed message parts and an authenticated bounded-fanout seek index.
It writes the sealed manifest last. A failed upload poisons the writer; recovery
rebuilds from the original retained input instead of continuing after an unknown
partial write. The manifest binds feed incarnation, definition, publication
projection, original native cursor interval, completion proof, LSN interval,
transaction ID and commit time. Its storage locator also binds the immutable
storage/upload scope. Exact historical rebuilds in a replacement storage scope
preserve the separate serialization identity used by the slot mapping.

The builder retains only an index frontier. Reads authenticate the manifest and
index path and return one complete bounded message at an exact message boundary;
interior cursors, changed bindings and corrupted parts fail explicitly. Limits
are 1 GiB per transaction, 65,536 messages, 16 MiB per message and 256 KiB per
storage part. A transaction larger than 32 MiB builds and seeks within a 64 KiB
independent working-memory budget. Multipart corruption and allocation failures
are covered as well.

`metadata/pg_spool_artifacts.zig` adapts the existing scoped immutable artifact
store, including checksum/size verification, cancellation, upload journaling and
persisted closure witnesses. It requires a trusted immutable storage namespace;
HA callers must additionally require shared storage. Local filesystem tests
supply a fixture namespace and explicitly reject HA qualification. They reopen
the actual filesystem backend and replay exact indexed messages while preserving
closure evidence. `zig build pg-spool-test -j2` runs the codec and filesystem
qualification with local caches (`zig/pg-spool-artifact-qualification.log`).

The format and adapter alone do not authorize publication or protect a live
slot. The durable slot admission/cleanup contract below now supplies metadata
accounting, but the runtime still must drive it around actual storage work.
Production row serialization, coherent database cuts and client qualification
also remain required; the native runtime still has no replication factory.

## In progress: slot spool admission and storage retirement

`pg_slot.State.spool` now retains an upload ledger within the same durable slot
record as its LSN map. Admission freezes the feed incarnation, definition,
projection, native cursor interval, completion proof, exact wire interval and
storage scope before the first put. Each upload has a monotonic ID, scoped nonce
and conservative payload-byte reservation, including message/index/manifest
objects. The journal and filesystem metadata remain the store's separate bounded
maintenance responsibility. One attempt may be uploading at a time; the ledger
has a fixed record bound and a 2 GiB payload quota. Abandoned uploads consume the
same reservation until collected. The first reservation permanently binds the
serialization definition/projection, including after the ledger becomes empty.

Private publication records the verified manifest locator and serialization
identity atomically with its LSN entry. Feedback advances the native cursor only
at complete transaction boundaries and marks corresponding uploads as garbage;
it does not release storage capacity. Cleanup uses the recorded original storage
scope and advances a monotonic prefix floor only after trusted storage confirms
fenced reclamation. Lost reservation/publication/cleanup replies are replayable;
stale sessions and changed payloads/scopes fail. Feed reset/close retires all
uploads, and deletion waits for outstanding spool cleanup as well as native
root/snapshot cleanup. Protocol 94 gates these coordinated metadata operations.

Artifact stores now expose an optional durable scoped-upload retirement barrier.
The filesystem implementation holds shared admission through physical writes and
closure publication, while retirement drains writers under an exclusive barrier
and durably records a per-domain floor in the authoritative payload namespace.
The floor and its locks are independent of replaceable inventory directories;
backups/restores must preserve this authority. The floor survives attempt and
whole-inventory removal, different inventory handles, and backend reopen. Writes at or below it fail, so a paused old owner
cannot recreate a reclaimed upload after its quota was returned. A provider
without this storage guarantee cannot qualify the writable spool adapter.

Runtime integration still must verify the actual sealed manifest and retained
completion authority before proposing private publication; hashes received from
public clients cannot provide either authority. It must also drive bounded
physical cleanup and persist continuations before supplying cleanup evidence.
These metadata/storage contracts do not yet supply that production worker or
first-START publication selection before the first visible transaction.

Qualification includes the actual artifact-store path from reserved admission
through sealed upload, indexed replay after reopen, complete feedback, bounded
seven-entry cleanup pages and quota release. It checks retirement across handles,
blocked-writer cancellation, reopen rejection and corrupt-floor failure. The
metadata recovery case reserves before publication, reopens, publishes atomically,
installs a snapshot on another owner, fences stale feedback, then reopens after
lost confirmation and cleanup replies. Relevant logs are
`zig/pg-spool-storage-fence-qualification.log`, `zig/pg-spool-control-test.log` and
`zig/pg-spool-filesystem-regression.log` and
`zig/pg-spool-metadata-recovery-qualification.log`.

The persisted locator remains immutable. Reconstructing missing immutable objects
in their original unretired scope is compatible with this contract. Replacing a
published locator with a different storage namespace still needs an explicit
fenced repair transition that preserves its serialization/LSNs while charging
both old cleanup debt and the replacement upload. Never silently overwrite the
original locator or release its charge merely because a new store is configured.

The next runtime worker must recover the original observed reservation after an
unknown metadata response before attempting another upload. It must verify the
stored scope, frozen historical schema/projection and completion authority before
building, and verify the actual seal before private publication. Streaming reads
must use the published locator and recheck current session ownership around I/O;
read/send never acknowledges data. Cleanup must select only the first garbage
record, persist bounded provider continuations, durably retire its upload ID,
and prove the attempt is physically empty before returning capacity. Enumeration
success alone is insufficient when a provider reports retained/nonempty scopes.
A live or new upload must never be covered by a cleanup cutoff. The runtime cleanup worker and durable continuation contract below now implement
this path. Production namespace resolution/scheduling remain required; the upload/publication
worker is implemented below; the earlier manual filesystem collector is an isolated
storage qualification fixture.

## In progress: bounded runtime spool cleanup

`metadata/pg_spool_cleanup.zig` drives storage reclamation through the actual
metadata service read/propose/observe contract. Its internal registry must
authorize maintenance and resolve the recorded immutable storage namespace. It
selects only the first irreversible garbage record; live or uploading records
stop the sweep without touching storage. It durably fences the original upload
ID, checkpoints that observation before deleting data, and resumes the exact
provider continuation from slot state. Opaque tokens are bounded, canonically
base64url encoded and internal; only the first garbage record may hold progress.
Protocol 95 gates this persisted checkpoint contract.

Each call admits bounded storage work/deletions, including provider-owned journal
maintenance. Cancellation and storage errors retain the reservation. Provider
continuations are saved only after completed work, and metadata authority is
re-read after I/O. A concurrent collector's newer checkpoint cannot be overwritten.
An unknown proposal response stops further work; the next call observes durable
progress before continuing. An explicitly obsolete provider continuation restarts
private discovery within the same fenced garbage scope; it never changes the
client acknowledgement or upload reservation. Native feed acknowledgement is
unaffected.

Enumeration exhaustion cannot return capacity. Artifact stores must separately
prove that the original scope is fenced and its payload/inventory are gone. The
filesystem implementation checks its authoritative retirement floor and both
attempt directories under the storage barrier. A complete checkpoint records the
internal evidence; a subsequent pass verifies physical absence again before
removing the reservation. The metadata reducer requires the recorded completion
proof. Writable spool providers must support both retirement and absence proof.

`zig build pg-spool-test -j2` qualifies the worker against actual filesystem and
metadata stores. Both reopen between sweep pages. It covers lost fence/page/GC
responses, cancellation after deletion, partial storage failure, concurrent
checkpoint interference, obsolete provider cursor recovery, refusal to release
capacity without absence evidence,
and preservation/readability of a later live transaction. The metadata snapshot
case transfers an opaque cleanup checkpoint to another owner before completing
and replaying reclamation. Pure reducer checks cover stale revisions, canonical
tokens, immutable completion and proof matching. Relevant logs:
`zig/pg-spool-cleanup-runtime-qualification.log`,
`zig/pg-spool-cleanup-control-test.log` and
`zig/pg-spool-cleanup-metadata-qualification.log`.

This is a private worker, not a registered production replication endpoint. The
production owner still must supply namespace credentials, lifecycle scheduling,
shared-storage qualification and wiring of the upload/publication worker below. Coherent cuts,
typed historical relational rows, public endpoints and existing-client
qualification remain part of the full implementation objective.

## In progress: historical spool upload/publication worker

`metadata/pg_spool_upload.zig` stages a certified native delivery, observes its
reservation, then streams one bounded pgoutput message at a time. A producer
must supply the original historical schema/images and retained completion
proof, deterministic preflight wire identity, message count and visible-event
count. Native receipt authority binds that descriptor before any storage scope
is allocated. Message lengths are included in the wire hash, so equal byte
concatenation with different message boundaries cannot change the serving index.
Hidden-only native progress cannot invent an empty PostgreSQL transaction.

The reservation freezes that plan and the original scope. Before opening a
writer iterator, metadata assigns a unique, single-use build token. Only that
observed grant may publish. Recovery of an interrupted/unknown build retires its
attempt and retains its quota; a later charged scope rebuilds the same historical
plan and LSN interval. It does not repeatedly append potentially different
outputs to one reservation. Unknown reservation responses recover the recorded
scope before preparing anything else. Unknown publication responses are resolved
from the durable mapping; they never create another LSN interval. Protocol 96
gates wire identity and single-use build grants. Legacy private reservations
remain readable/cleanable but cannot be resumed by this runtime as strict plans.

During a build the producer's live authorization and session window are checked
around each message. Near expiry the worker observes current metadata to accept
renewal of the same owner, while preserving the iterator's original immutable
definition. A changed owner/build grant or expired lease fails. Iterator buffers
are released between messages and on errors. The worker verifies complete wire
identity/count, the actual sealed manifest and original plan, asks the storage
registry to protect the root, and reobserves ownership before atomic slot
publication. Publishing is not consumer acknowledgement.

Shared storage is required by default. The local filesystem qualification opts
out explicitly; no HA capability or production endpoint is implied. Namespace
resolution must not upload data before admission, and root protection must use
actual provider retention authority, not a client-supplied digest. Production
source verification, namespace credentials and scheduling remain required.

`zig build pg-spool-test -j2` exercises actual metadata/filesystem orchestration,
including wrong wire output, no re-planning before recovery, reservation/build/
publication response loss, repeated reopen, immutable LSN ordering, lease renewal
past the original grant, authorization/stale-owner failure, and charged cleanup
of abandoned attempts while preserving the live spool. The metadata snapshot
case now also transfers strict wire identity and build grants. Relevant logs:
`zig/pg-spool-upload-runtime-qualification.log`,
`zig/pg-spool-upload-control-test.log` and
`zig/pg-spool-upload-metadata-qualification.log`.

The retained stream owner below now reads published spools. Runtime integration
still must implement real historical relational production and coherent bootstrap
cuts and register production source/storage/authorization capabilities. Public APIs,
client qualification and the remaining inbound/destination/lifecycle work are
still part of the full objective.

## In progress: retained stream owner and CopyBoth bridge

`metadata/pg_spool_stream.zig` connects durable slots to published immutable
spools. Opening validates current policy and the requested database/slot scope,
qualifies storage, acquires a fenced connection actor and observes an immutable
publication binding. Publication names are canonical, unique UTF-8 identifiers;
reordering the same set is equivalent, while changing the set or projection
requires resynchronization. The binding precedes strict wire capture, including
when the slot has no visible transactions yet. Protocol 97 gates publication
binding and atomic feedback renewal/confirmation.

Poll seeks one complete message through the recorded namespace and authenticated
manifest. It verifies the original plan, serialization identity and message
count, then rechecks authorization/ownership after storage I/O. The actor retains
no transaction buffer and never reconstructs from current rows. Frontiers are
contiguous; server end cannot regress. An interior reconnect rewinds to Begin
while retaining the validated requested frontier for old feedback. New feedback
is bounded by returned bytes or that reconnect frontier, not arbitrary later
server publication. Polling and outgoing data do not renew consumer ownership.

Valid feedback atomically renews the same actor lease and confirms only complete
mapped transactions, advancing native metadata acknowledgement in that same
write. Partial flushes retain the entire transaction; storage reservations remain
charged through physical cleanup. Unknown confirmation responses are recovered
from durable metadata. Closing releases only the matching connection lease,
never an acknowledgement or the durable slot's history. A private actor finalizer
also recovers ambiguous opens after connection work drains.

`pgwire/retained_stream.zig` implements the typed `ReplicationStream` bridge. Each
returned payload has an explicit release owner; another poll or close cannot
retain an outstanding payload. The host supplies real credentials, metadata,
namespace routing, publication authorization and independent close/recovery
contexts. This bridge does not itself register the native production factory.

`zig build pg-spool-stream-test -j2` runs a real loopback CopyBoth connection over
actual spools and metadata. It checks complete B/I/C delivery and CopyDone without
acknowledgement, publication mismatch, storage qualification, ambiguous-open
lease cleanup, interior replay, future feedback rejection, authorization before
cursor use, partial/full confirmation, lost confirmation replies and charged
post-ACK cleanup debt. Existing upload/cleanup tests continue to pass. Reducer
checks cover canonical binding, pre-capture binding and atomic lease/ACK state.
Relevant logs: `zig/pg-spool-stream-runtime-qualification.log`,
`zig/pg-spool-stream-control-test.log` and
`zig/pg-spool-stream-metadata-qualification.log`.

The loopback client is a transport qualification fixture, not the required
existing PostgreSQL CDC client. Production relational capture, coherent cuts,
credential lineage, namespace registration, public endpoints, inbound/destination
and lifecycle integration, and existing-client qualification remain outstanding.

## In progress: production credential incarnation fencing

Production Basic pgwire credentials now retain the persisted UserManager user
instance ID. Validation checks that incarnation and the existing credential MAC
under the same mutation lock, preserving the MAC format used by existing
destination grants. Catalog identity copies preserve the ID, including physical
table projections. Live single-resource and bulk permission checks intersect the
admitted grants with current permissions and the admitted incarnation under one
lock. The bulk helper also releases its temporary logical-name mapping.

Trusted credential renewal requires the same optional user incarnation as the
existing session. Locally backed trusted identities recheck that incarnation
before returning a cloned identity. External trusted principals retain their
existing issuer authority contract.

The pgwire regression deletes and recreates a username, then reinstalls the exact
old password verifier. Although its legacy credential MAC matches, the retained
credential and both projected permission paths must reject the old incarnation;
a fresh login belongs to the new incarnation. This is a prerequisite for the
production feed owner, not registration of the replication session factory.
Qualification: SQL/catalog DDL and row-filter suites pass, as do UserManager
storage/persistence tests. The independent archive-boundary test checks the
identity-aware MAC and permission methods across separate error domains,
including zero IDs and username recreation. Its deterministic random fixture
now emits distinct identities across calls. Logs:
`zig/identity-lineage-qualification.log`,
`zig/identity-lineage-final-qualification.log` (the archive fixture failure
before that correction), and `zig/identity-lineage-abi-qualification.log`
(corrected archive tests pass).

## In progress: provisioned native history root binding

`ProvisionedTableWriteSource.bindNativeHistoryRoot` supplies an owned, narrowly
scoped `TableWriteSource` for the native history adapter. The general production
writer still routes across many tables and does not pretend to have one native
namespace. The binding copies its table locator and records the exact catalog
table/range namespace. Every callback rechecks that namespace behind the
linearizable routing barrier before and after storage work, holds the existing
group operation guard, and leases the resident writer through the production
cache. No DB pointer outlives that lease. History pages own their bytes after the
lease is released. The binding cannot be used as a general batch writer.
Runtime owner selection supplies an explicit `native_history_owner` capability,
independent of SQL range guards. The provisioning method is appended to the
storage-engine method table and native exports, with descriptor layout checking.

Resident native provisioning atomically persists the exact DB identity and
native authority under the apply/snapshot/replication mutation guards. Existing
bindings take a read-only outcome without another WAL write. A mismatched
namespace, persisted Raft authority or legacy ordered-apply marker cannot be
converted by selecting a standalone serving profile. Restoring hidden/staged
roots and read-only handles retain their existing admission guards.

This initial production owner capability qualifies an unreplicated standalone
native writer with a resident cache. Ordered Raft control, mirrored hot-standby
pins, linked/hosted owner dispatch and coherent snapshot cuts still require their
own completed control paths; this capability rejects those profiles. It does
not register a public endpoint or pgwire factory.

`zig build provisioned-native-history-test -j2` executes the physical writer
suite, not the HTTP discovery root. It enrolls an empty production-cache table,
captures a write, closes and reopens the writer, changes the current row and
replays the original historical image. Table replacement and live serving-profile
changes fence the old binding. The test is also selected by the normal
table-write lifecycle gate. Initial qualification is recorded in
`zig/provisioned-native-history-qualification.log`; the expanded Raft-authority
and legacy-marker checks plus the existing changefeed control suite are tracked
in `zig/provisioned-native-history-final-qualification.log`.
Those expanded checks and the changefeed control suite pass. The final explicit
owner-capability integration passes in
`zig/provisioned-native-history-owner-qualification.log`. The compiled standalone
configuration smoke check also passes in
`zig/provisioned-native-history-native-build.log`; this checks compiled native
exports/runtime wiring, not end-to-end replication startup.

## In progress: atomic ordered root retention outcomes

`DB.applyOrderedHistoryConsumerControl` is the trusted storage primitive for
data-Raft retention controls. Under the apply/snapshot/replication mutation
guards it binds the actual root's persisted Raft authority and stages consumer
effects, a bounded result receipt and the ordinary ordered-entry marker in the
same primary transaction. It creates no document event. Native authority cannot
be converted by invoking this method. Mirrored batch owners remain unsupported
until their explicit durable control envelope is implemented.

Expected reducer rejections are committed outcomes, including invalid admission,
stale consumer/ACK fences, consumer limits and unavailable retention capacity.
Any partially staged reducer writes are aborted before the rejection transaction
is recorded. Corruption, allocation failures, storage failures and wrong durable
owner authority remain errors and cannot advance the ordered marker. This lets
the eventual state-machine adapter advance a rejected log entry without
misreporting success or blocking later ACK/cleanup entries forever.

`storage/history_ordered.zig` defines the authenticated canonical HCO1 receipt:
exact root namespace, term/index, command fingerprint and applied/rejected
result, in a fixed 192-byte record. Only the latest control receipt is retained.
Exact replay returns its original result, even after later row writes, and
rejects a different command under that entry identity. Once a newer control has
replaced it, replay explicitly reports `superseded`; callers must inspect the
live lease and cannot infer success, advance ACKs or discard history from that
result. This avoids an unbounded command/result journal.

The method and result are appended to the native storage-engine ABI. The
production Raft command codec/all-member activation barrier, state-machine
dispatch, owner-routed submission and linearizable inspection/page capabilities
still need integration before any distributed root can advertise this feature.
The standalone native binding continues to reject Raft profiles.

`zig build ordered-history-consumer-test -j2` runs the physical writer suite and
is selected by the ordinary lifecycle gate. Coverage includes rejected admission
followed by successful enrollment, ordered row capture, conflicting payload
replay, future ACK rejection followed by valid ACK, result recovery after writer
reopen, explicit supersession, retirement/re-enrollment fencing and corrupted
receipt rejection. Expanded qualification also checks collection replay after
reopen leaves retained state byte-for-byte unchanged, bounded receipt encoding
and allocation failure. Logs: `zig/ordered-history-consumer-qualification.log`
and `zig/ordered-history-final-qualification.log`.
The expanded physical writer tests, provisioned-native regression and compiled
standalone configuration/native-export smoke check all pass. This qualifies the
storage primitive and ABI, not end-to-end Raft submission or public feeds.

## In progress: dedicated Raft retention envelope

`data/history_control_entry.zig` defines the private retained-history entry
separately from document batches. It carries an exact table/range namespace,
original storage-owner generation/schema/index/range/settings and one typed
consumer command. Explicit protocol and command-version fields are mandatory;
the owner fields cannot disappear into parser defaults. Unknown and duplicate
fields are rejected. Descriptor text, range keys and the total envelope are
bounded, and non-native storage or restore/hidden-child bootstrap authority is
not accepted by this entry type.

At this codec milestone, protocol 30 was reserved while the advertised
maximum remained 29 (the native dispatch milestone below advances it). Encoding requires evidence of an activated protocol at
least 30; this codec's presence does not activate that protocol. The outer
`batch: null` poisons older row-batch parsers. The current ordinary envelope
decoder also explicitly rejects `history_control`, including a forged envelope
with an empty object batch, so it cannot acknowledge retention work as an empty
row mutation while dedicated dispatch is still absent.

`zig build history-control-entry-test -j2` passes 17 tests. It checks all seven
consumer command forms, owner binding, mandatory version/metadata fields,
unknown/duplicate fields, size limits, every allocation-failure point and owned
decode lifetime. The same target reruns the existing row-batch, transaction,
split/merge, binary range and protocol-barrier codec regressions, and belongs to
the normal unit gate. Log:
`zig/history-control-entry-regression-qualification.log`.

Dedicated state-machine/native-owner dispatch, durable all-member activation,
owner-routed submission and linearizable inspection/pages remain the next
integration boundary. No public feed or replication capability is advertised by
this codec milestone.

## In progress: compiled owner retention dispatch

Private owner ABI 92 adds `orderedHistoryControlJson`, with bounded typed
command/outcome JSON and the existing immutable term/index request header.
The native provider invokes the atomic ordered control primitive; reducer
rejections remain typed results, while corruption and entry conflicts retain
their exact stable error identities. The new statuses append to the existing
registry without renumbering published values. Older owner headers are rejected
before dispatch. Writer contention uses the existing committed-entry retry
normalization. Response allocation may fail after commit, so recovery continues
to use the durable command/result receipt rather than inventing a new command.

`ProvisionedKernelOwnerSource.applyPreparedOrderedHistoryControlGroupLocalAtRaftEntry`
uses the same descriptor-pinned owner admission as row replay, without resolving
live catalog state. Production admission is nonblocking on the Raft progress
thread and uses the prepared control lane; direct fixture admission exercises
the same compiled physical owner. Its result type belongs to the shared table
write contract, independently of the intentionally opaque control-side DB type.
No borrowed native result bytes escape the owner lease.

The compiled source qualification applies enrollment and a rejected future ACK,
reopens the physical owner, recovers the exact rejection, rejects a different
command at the same entry, reports supersession explicitly and verifies that
ACK progress stayed unchanged. A catalog which fails any admin or routing read
records zero calls throughout. The test also checks old-header rejection before
handle dispatch. The 15 error-registry tests and two frozen status-number tests
pass. Logs: `zig/ordered-history-owner-final-qualification.log`,
`zig/ordered-history-error-registry-qualification.log`, and
`zig/ordered-history-error-abi-qualification.log`. The existing broad owner
operation/replay regression is tracked in
`zig/ordered-history-owner-regression-qualification.log`.
That existing owner regression passes under ABI 92, including ordinary
operations and non-idempotent row-transform replay at an exact Raft entry.

This completes the compiled owner call, not data-Raft state-machine submission.
Projection gating, dedicated entry dispatch, protocol activation, owner-routed
submission and linearizable history inspection/pages are still outstanding.
At this owner-only milestone the advertised maximum remained 29. Ordinary
entry decode continues to reject retention envelopes; dedicated applying decode
is required.

## In progress: typed applying decoder and projection retention gate

`raft_batch.decodeForApply` returns `OwnedApplyEntry`, a tagged union of a row
batch and a dedicated history control. A history command has no synthetic empty
document batch. The ordinary decoder remains fail-closed for retention entries;
existing callers cannot accidentally acknowledge one through their row path.
Both applying branches reuse one parsed envelope and return independently owned
bytes. Codec tests exercise every allocation-failure point, binary range bounds
and maximum-width owner IDs/generations and grant counters without precision
loss.

The projection decoder now recognizes the applying history variant and requires
a trusted native delegate before appending anything. Its only operation is a
minimum durable protocol guard; it manufactures neither rows nor split deltas.
The new guard is appended to the internal operation union to retain existing tag
values. It checks persisted activation for an isolated command and follows
ordered staged barriers in a larger apply batch, without additional row effects.

The real projection-store test rejects retention before activation and after an
older durable barrier, preserving the exact previous watermark and retained
entry list. A codec input claiming activation is not authority. Independent
guard tests cover absent, zero, older and unsupported floors, positive ordered
activation, and both single-operation and batched checks with no row effects.

`zig build history-control-entry-test history-control-projection-test -j2` passes
22 tests: 17 codec/legacy envelope checks and five projection, snapshot-barrier
and native source-control regressions. These tests are included in the normal
unit/data-storage gates. Log:
`zig/history-control-projection-contract-qualification.log`.

At this projection-only milestone the advertised maximum remained 29. The
following native dispatch milestone completes the applying branch. Submission
and linearizable history reads remain pending; the projection gate alone does
not provide a distributed feed.

## In progress: native Raft history application

The data-Raft decoder ceiling is now 30. The applying state machine handles the
history union explicitly, requires the durable projection barrier, and calls the
compiled native owner with the original descriptor and exact term/index before
publishing shared apply progress. Storage contention retries without advancing
that progress. Deterministic consumer rejections complete their log entry and
retain a typed failure for the proposer; superseded receipts require outcome
recovery rather than reporting success. Corruption remains an apply failure.
The ordinary batch decoder still rejects history entries.

The focused compiled-owner qualification injects a lost reply after durable
native enrollment, verifies the pending shared watermark and proposer result,
then retries the same entry successfully. It also verifies that an invalid ACK
returns its typed conflict while allowing a subsequent valid inspection.
The projection and codec regression targets accompany this qualification.
Log: `zig/history-control-native-apply-qualification.log`.

This is an internal apply capability, not a registered public feed capability.
Owner-routed proposals, linearizable pages and typed result recovery, explicit
all-member activation qualification, and retained-history split/merge transfer
remain required before distributed feeds can use it. The unlinked storage
profile fails closed. Public HTTP/SQL, coherent bootstrap, typed pgoutput and
existing-client qualification remain outstanding.

## In progress: Raft history read cuts and compiled-owner transport

`DB.orderedHistoryPage` accepts only persisted Raft authority and a nonzero
minimum applied index. The authority, ordinary ordered marker, consumer lease
and retained frame are read from one stable transaction. A root behind the cut
returns a retryable read error; a corrupt marker fails closed. Standalone reads
continue to require native authority. This primitive does not manufacture a
ReadIndex proof: the production owner must establish and validate that barrier
and the original route/root identity before exposing a page.

The private `history_page_wire` transport carries binary committed-frame bytes
and bounded page metadata in a checksummed envelope. It binds the returned
namespace, frame sequence, first ordinal, frame count, continuation and caught-up
state to the original request. Its domain-separated checksum also binds every
lease reference field, request timestamp and both budgets, so a valid response
cannot be transplanted to a different grant or request. Decode budgets are
checked before allocation;
returned schemas and tuple images belong to the page. Qualification covers
binary images, partial/complete/empty pages, mutation of every encoded byte,
forged checksummed metadata and every allocation-failure point.

Physical storage qualification checks lagging cuts, zero cuts, standalone/Raft
authority separation, expired leases, historical data surviving close/reopen,
and ACK visibility. Logs: `zig/ordered-history-read-cut-qualification.log`.
At this storage primitive milestone the compiled-owner ABI operation was still
pending; it is implemented in the next milestone below. Production routing
remains pending, so this primitive alone cannot expose a linearizable public feed.

## In progress: compiled-owner history page operation

Private storage-owner ABI 93 adds `orderedHistoryPage`. Its bounded strict JSON
request carries the original private page request and nonzero minimum applied
index; its response is the HPG1 binary page envelope. The physical owner reads
through `DB.orderedHistoryPage`, then encodes the result before releasing the
storage page. The control client decodes into caller-owned page storage before
releasing the ABI response. Old ABI headers and invalid/reserved request fields
fail before storage dispatch.

Four append-only failure identities preserve invalid history admission, page
size limits, cursor conflicts and corrupt ordered markers. These errors must
not collapse into an internal owner failure. The independently compiled error
registry verifies unique status identities and exact round trips.

The real compiled-owner qualification enrolls retention, writes an ordered row,
reads its historical image, rejects a lagging cut/zero cut/insufficient byte
budget/expired lease, then closes and reopens the physical owner and compares
header, images and continuation. Original page bytes survive owner teardown.
Logs: `zig/ordered-history-compiled-page-qualification.log` and
`zig/ordered-history-page-error-registry-qualification.log`.

The production source adapter must still establish leader ReadIndex, validate
the live route/root identity and preserve those fences across owner borrowing.
This ABI operation is private and does not register a public feed or permit
stale fallback when a leader barrier fails.

## In progress: fenced routed history reads

`ProvisionedKernelOwnerSource.orderedHistoryPageRouted` now performs the private
Raft history read through the compiled owner. It requires Raft authority and
frozen-proof support; validates the catalog route and retained namespace;
normalizes the remaining deadline budget to the platform clock; waits for
ReadIndex without stale fallback; and captures/validates leader incarnation,
term and applied cut. The borrowed owner's namespace must match the route.
After fetching the bounded page, it validates leader and route again before
returning bytes. Any final failure releases the page rather than returning it.

Qualification includes an actual compiled-owner enrollment and retained ordered
row, plus injected leader loss and route replacement after page acquisition.
Separate rejection checks prove unsupported barriers, namespace mismatch and
ReadIndex leader loss fail before opening an owner. Log:
`zig/ordered-history-routed-read-qualification.log`.

This provides the native group-local read adapter. Feed root binding, owner-routed
control submission/result recovery, remote transport, topology/history lineage
transfer and production provider registration remain required. This method does
not advertise public feed capability or replace the coherent bootstrap work.

## In progress: read-only retention outcome recovery

`DB.orderedHistoryOutcome` recovers an exact original command result without
reapplying enrollment, ACK, retirement or collection. A typed lookup binds the
command, term/index and a leader read cut at or beyond that entry. It validates
Raft authority, namespace, the ordinary applied marker and the authenticated
latest history receipt in one stable transaction. Original results survive
intervening row writes. Conflicting payloads/terms fail closed; missing or
superseded receipts return an explicit unknown/superseded outcome rather than
success. Lookup never writes a marker or repeats collection. A recovered result
proves original execution, not current lease validity; the owner must still
validate the live definition/grant and permissions before using it.

Compiled-owner ABI 94 exposes this through bounded strict JSON requests and
bounded typed outcome responses, with the same buffer ownership discipline as
the ordered control operation. Qualification covers successful recovery before
and after reopen, payload and term conflicts, lagging cuts, malformed cut bounds,
recovery of deterministic rejection, supersession and old ABI header rejection.
A physical corruption check confirms recovery rejects a damaged receipt. Log:
`zig/ordered-history-read-only-outcome-qualification.log`.

The routed control proposer still needs all-applying-member protocol activation,
original route/descriptor admission, exact proposal outcome waiting and fenced
read-only result recovery. Public feed registration remains pending; this
lookup is private and does not turn an unknown result into permission to ACK.

## In progress: routed exact receipt recovery

`orderedHistoryOutcomeRouted` now recovers receipts through the same fenced
native group-local read path as retained pages. Both operations share one
adapter for route/namespace checks, deadline translation, strict ReadIndex,
leader incarnation/term validation, owner identity and final route validation.
Recovery additionally requires the captured applied cut to cover the original
entry and requested minimum, then passes that captured cut to the read-only
native lookup. It never weakens a lagging proof to a local marker observation.

The compiled-owner qualification recovers an actual enrollment receipt after a
retained row write, and injects final leader loss and route replacement for both
pages and receipt recovery. Both operations reject the invalidated result. A
future receipt cut fails before native result access. Log:
`zig/ordered-history-routed-outcome-qualification.log`.

Owner-routed proposal submission, activation across all applying members and
binding these operations into production feed roots remain pending.

## In progress: retention protocol admission evidence

Data-Raft protocol preflight now distinguishes confirmed older decoders from
unavailable capability/route evidence while preserving the existing activation
version and exact applying-membership fingerprint. A zero/unknown peer version
or incomplete routes are retryable unavailable evidence; a positively observed
older decoder is incompatible. Neither case permits activation.

The protocol-30 qualification rejects a mixed 30/29/30 applying set, reprobes
upgraded processes at the same endpoints, activates only after all three support
30, and rejects unknown capability/incomplete routing separately. The same gate
reruns the native history apply/reply-loss/rejection ordering test. Log:
`zig/ordered-history-protocol-preflight-qualification.log`.

The proposer must still recheck the captured leader term and membership under
proposal ordering, and retain the original accepted entry identity through
outcome recovery. Capability advertisement must reflect the actual native apply
profile. This evidence classification is not a completed control proposer.

## In progress: local leader control submission and exact recovery

`DataServer.submitHistoryControlLocal` now submits dedicated retention entries
through the actual local data-Raft leader. It requires the compiled native owner,
probes every applying peer for protocol 30, acquires routed structural admission,
loads a bounded original owner descriptor and verifies its namespace against the
route. Under proposal ordering it rechecks the captured leader term and exact
membership fingerprint, appends the protocol barrier when needed, then proposes
the control and registers its exact apply waiter before releasing the Raft lock.
Structural admission and activation ownership are released before waiting.
Hosted-group validation and activation-cache insertion share proposal ordering,
so unhosted requests cannot allocate cache entries or race retirement cleanup.

Known accepted entries always retain their original term/index on interrupted
confirmation. A null outcome is unconfirmed, and a superseded receipt cannot
certify success. `recoverHistoryControlLocal` uses bounded read-only recovery
through the fenced adapter; it retries transient read contention without
appending another control. Leader/route replacement and non-retryable failures
remain unconfirmed rather than inventing a successful ACK. Deadline conversion
preserves remaining nanosecond budgets; consensus-lock acquisition and apply
waiting honor cancellation. The original command is never re-proposed by the
recovery operation.

Qualification uses a real in-process single-node Raft log, actual native
projection and compiled owner, a concurrent progress driver and production
ReadIndex/frozen-proof callbacks. It confirms enrollment and typed ACK rejection,
persists protocol activation, interrupts confirmation after acceptance, and
recovers the original receipt while proving the log index did not advance.
The gate also covers apply reply loss, membership/term fences and peer capability
preflight. Logs: `zig/ordered-history-local-submission-qualification.log` and
`zig/ordered-history-local-submission-repeat-qualification.log`.

This is a private local leader path. Remote control transport, serving-profile
capability advertisement, feed root binding, topology/history transfer and
file-backed multi-node recovery qualification remain required. Public feeds,
bootstrap and existing-client qualification are not complete.

## In progress: serving-profile protocol advertisement

The internal capability response now derives its data-Raft ceiling from the
actual serving compiler unit. Compiled native-owner profiles advertise the full
supported decoder/apply version; profiles without native history apply stop at
29 and cannot authorize the protocol-30 retention barrier. This is an internal
protocol capability, not an advertisement of public feeds, bootstrap or lake
row-history support.

The production distributed and API runtime archives select the same native
control profile. Qualification checks both supported/unsupported ceilings and
the actual HTTP JSON response for the selected profile. It also retains the
internal-service-principal boundary for the capability path. The focused
`history-serving-capability-test` belongs to the normal API auth gate; its HTTP
runtime discovery anchor ensures the handler tests execute rather than passing
an empty selection. Logs: `zig/history-serving-protocol-capability-qualification.log`
and `zig/history-serving-capability-http-qualification.log`.

Both implementation issues remain open. Their live requirements still include
native and writable-Iceberg destination integration, public HTTP/SQL feeds,
coherent bootstrap, retained topology/recovery semantics and qualification of
actual pgoutput with a pinned existing CDC client. The private native control
milestones do not replace that scope.

## In progress: private remote control wire contract

`data/history_control_wire.zig` defines a bounded version-1 request/response
contract for node-to-node retention control. Submit and recover are distinct
operations; submit cannot carry a receipt, and recovery must carry the exact
original nonzero term/index. The decoder binds the command namespace to the
already-validated catalog route. Unknown/duplicate fields, missing versions,
unsupported versions and oversized payloads fail closed.

Responses distinguish an unconfirmed result without a receipt, an accepted but
unconfirmed receipt, superseded history and known applied/rejected results.
Known rejection is not successful ACK permission. Recovery replies must echo
the original receipt. Applied results must match the requested consumer/epoch,
include valid lease metadata and sufficient ACK/renew progress, and stay within
the requested reclamation bound. Both codecs return value-owned fixed fields;
no parse-arena bytes escape. Full-width receipt values retain integer precision.

`history-control-entry-test` passes 19 checks, including the existing dedicated
entry/legacy-envelope regressions, strict remote parsing, result binding,
reclamation bounds and every allocation-failure point. Log:
`zig/history-control-wire-qualification.log`.

This is a private codec, not a mounted remote API or an authorization boundary.
The HTTP surface must be specified/generated before registration, require service
authority and catalog fencing, and preserve unknown outcomes without automatic
re-proposal. Remote routing and feed-root registration remain pending.

## In progress: specified private history control ingress

The internal OpenAPI contract now describes the bounded node-to-node
`POST /groups/{group_id}/tables/{table_name}/history/control-v1` operation.
Generated internal types and server declarations accompany the specification.
Submission and exact receipt recovery remain distinct operations; a 200 response
can carry a known rejection or an unconfirmed result and is never itself ACK
permission. These receipts are private Raft coordinates, not public cursors.

A transport-neutral operation validates the immutable catalog fence, path group,
request namespace, version, byte limit, and returned receipt/result binding.
The data runtime registers a callback only for a linked native owner with Raft.
That callback uses the existing local-leader proposal path or read-only exact
receipt lookup. It cannot turn recovery into a fresh proposal. Callback failures
remain ambiguous because HTTP status alone cannot prove proposal rejection.
Native and linked route registrars share the 4096-byte prebuffering limit.

The focused ingress checks cover wrong routes/namespaces before dispatch,
unsupported profiles, known rejections, lost confirmations with original receipt,
invalid callback results, and temporary storage failures. Production runtime
qualification extends the real Raft/native-owner test through this operation
boundary. Signed service authority is required even in legacy migration
mode; unsigned requests fail before invoking the owner. The six focused HTTP
checks (including a mounted loopback route in migration mode) and four real
Raft/native-owner checks pass without leaks. Full
`make generate` also passed. This does not yet register a production feed root, route controls remotely,
implement a remote retained-page API, or satisfy the public feed/client contracts.

## In progress: bounded authenticated history control client

`ApiHttpClient.fetchGroupHistoryControl` preserves the specified private
submit/recover union instead of replaying ordinary batch endpoints. It validates
route/namespace and local cancellation before transport, percent-encodes table
names, signs with internal service authority, and propagates the original
remaining budget (capped at five seconds) and cancellation token. Request and
response bodies remain bounded to 4096 bytes.

The client makes exactly one transport attempt. Any transport failure or
non-200 status returns an unconfirmed result, retaining the original recovery
receipt. Missing/wrong route acknowledgement, malformed or mismatched results,
and any outcome marker also prevent confirmation. A valid reply can preserve a
newly accepted submission receipt without certifying its outcome. Cancellation
after a reply similarly preserves a valid receipt while withholding confirmation.
Known typed rejections are resolved results, never successful ACK permission.

Focused qualification uses a signed request executor to verify the exact
operation, maximum-width receipt coordinates, encoded table path, byte limits,
timeout, cancellation and route fence. It exercises success/rejection, lost
transport, invalid/mismatched replies, missing acknowledgements, contradictory
and empty markers, and every specified HTTP error status. All seven focused
HTTP/ingress/client tests pass without leaks. This is the remote
transport boundary; production root registration, bounded leader routing and
remote retained-page delivery remain required before public feed capability.

## In progress: deadline-bound retained-history leader routing

The data runtime exposes a request-scoped `historyControlPort` for a feed-root
owner. It keeps the HTTP endpoint's callback local-only, so a forwarded control
cannot bounce between receivers. The routing callback preserves the immutable
catalog fence and operation, translates the original remaining budget to the
owned runtime clock, and caps the whole route/lane/transport attempt at five
seconds. A local leader goes through the existing native proposal/recovery path.

Remote discovery captures the compact peer generation under that same budget.
It uses an explicitly observed hosted leader, or the captured peer leader when
there is no hosted observation. A hosted group with no known leader cannot
fall back to a cached leader. The leader must be eligible for that exact
group and have an endpoint. Missing/ineligible/newer conflicting observations
never fall back to an older cached leader, another placement, an unbounded admin
snapshot, or another proposal. Forwarding uses the owned request lane and the
bounded signed client. Original receipt recovery stays a read-only operation.

Local transport-neutral completion now also rechecks cancellation before
returning: a valid receipt survives but confirmation is withheld. This matches
the remote client's post-response behavior. Leader selection and the real Raft
fixture are being qualified through the new port. The fixture also starts the
actual signed HTTP handler and a second caller with no local replica, using a
cached compact peer view whose fallback fetch is forbidden. It recovers the
original native receipt and checks that the live Raft log has not grown. It
also interrupts confirmation after a remotely submitted control is accepted,
then recovers the same receipt without another proposal. Log assertions read
the hosted Raft status, including unstable entries; the fixture's backing
MemoryStorage alone is not evidence of the live log position. All five focused
runtime checks (including this HTTP/native recovery) and seven HTTP/ingress/client
checks pass without leaks. This fixture uses in-memory Raft and on-disk native
storage; it does not qualify WAL-backed multi-node failover. Distributed
failover and production feed-root binding remain separate required qualification.

## In progress: specified remote retained-page delivery

The internal OpenAPI contract now specifies service-only
`POST /groups/{group_id}/tables/{table_name}/history/page-v1`. Full generation
passed. Requests are capped at 4096 bytes and include an explicit version,
consumer/grant reference, private position, timestamp and page limits. The HPG1
response binds those fields to the returned metadata and binary historical
images. Its bound is exactly the fixed 96-byte prefix plus the requested raw
page budget; forwarding must not silently retain the executor's 4 MiB default.

Native and linked registrars share prebuffering limits. The handler requires a
signed service credential even in migration mode and returns owned binary bytes
with `Cache-Control: no-store`. Typed errors distinguish topology changes,
resynchronization, a larger required event budget, unsupported owners, temporary
unavailability and corrupt pages. The client validates error/version/status
binding, binary content type, catalog acknowledgement, checksum and request
binding, and withholds pages after cancellation. Reads never imply ACK.

Control and page routing now share the deadline-bound leader/lane selection
path; HTTP receivers remain local-only. Native pages use the existing frozen
ReadIndex and final leader/route checks. The real Raft fixture commits two
versions of one row, then reads both historical images over signed HTTP from a
caller without a local replica. It replays the first page after reading the
second and checks the explicit too-small-budget error. Ten HTTP/ingress/client
checks and all five focused real Raft/native checks passed without leaks.
Append-only runtime error details preserve invalid page/control requests, invalid
control responses and exact receipt conflicts across independent archives; the
existing canonical corrupt-Raft-receipt identity is preserved. All 58 runtime
error tests pass. The combined gate also passes against this final inventory:
five real Raft/native checks and ten HTTP/ingress/client checks, without leaks.

These are private raw retained pages, not public authorized delivery, typed
PostgreSQL production, coherent bootstrap, global completion certification,
retired-root transfer or WAL-backed multi-node failover qualification. Those
requirements remain mandatory before the full issues can be completed.

## In progress: read-only retained grant observation

A production root must recover an enrollment whose receipt was lost without
blindly proposing enrollment again. `history_consumer.observeGrant` now reads
an immutable namespace/authority/consumer/definition/grant/policy identity and
returns an explicit observation: authority unbound, grant absent above the
retirement floor, grant fenced by retirement, exact active lease, or a different
current grant for the same immutable consumer/definition. The active result
contains the actual epoch, start, ACK, expiry, policy and retained-byte limit.
This is a state observation, not proof of an original command's full payload,
requested expiry/quota or successful completion.

The operation never binds authority, enrolls, renews, acknowledges or writes a
receipt. Namespace/authority/definition/policy conflicts fail closed. A native
lease missing under a valid control record is corruption, and an uncontrolled
legacy consumer cannot be adopted as an absent grant. The DB exposes separate
native and ordered authority entry points; ordered reads require a positive
applied cut checked in the same transaction as authority and lease state.
Both methods are appended to the internal storage-engine interface.

Physical qualification passes: unbound versus absent/retired/different-grant
states, renewal, close/reopen, policy/scope mismatch, unavailable applied cuts,
wrong authority kind, missing native state/consumer, missing control authority,
malformed records, and preservation of ordinary apply and exact command receipts.
The physical gate passes, including standalone observation without a Raft marker
and mutual rejection of authority kinds. All three existing compiled native
consumer boundary checks also pass without leaks. The generated inference error
inventory now includes the retained RPC errors from the preceding boundary
change. The compiled-owner C ABI,
routed observation endpoint, durable root-control recovery and production root
registration remain required; this DB observer alone does not finish them.

## In progress: compiled grant observation and frozen route binding

Storage-owner ABI 95 adds a read-only native/ordered grant query. Bounded JSON
requests carry an explicit wire version, authority kind, immutable grant
identity and required applied cut. Replies bind all those fields with a domain
separated digest and validate active consumer identity/policy, grant equality,
retirement-floor inequalities and scalar bounds. No observation is promoted to
a command receipt. The client copies fixed-size results before freeing the
compiled-owner response. Old ABI requests fail before touching storage. Selecting native authority cannot
provision an unused root's durable document namespace: observation fails closed
until explicit root activation has persisted it.

`ProvisionedKernelOwnerSource.orderedHistoryGrantRouted` shares the existing
ReadIndex, frozen owner proof, current descriptor identity, final leader and
final route checks with pages and exact receipt recovery. The caller cannot
choose a weaker native authority or a zero ordered cut for a Raft root.

Initial compiled tests exposed a missing native status mapping for
`HistoryConsumerConflict`; it now has an explicit append-only status (928)
instead of becoming a generic kernel failure. The routed source checks passed
before that mapping correction. Final qualification covers all observation
states, close/reopen, request binding, full-width integers, allocation failure,
malformed replies, old ABI rejection and both final-fence failure paths. The
physical runner invokes the wire contract assertions explicitly; an imported
helper namespace alone is not test execution. The status-corrected combined
gate passes without leaks: six physical/wire checks, two compiled-owner checks,
and two routed-source checks (plus thirteen discovery/support checks).
Routed HTTP observation,
durable pending-control reconciliation and production root registration are
still required.

## Specified routed HTTP grant observation

The internal API now specifies
`POST /groups/{group_id}/tables/{table_name}/history/grant-v1`; full API/SDK
regeneration passed. The 4096-byte request contains version and immutable grant
identity. Its response binds that identity to the private ordered-read contract
(raft authority and positive lower bound 1); the receiver internally enforces
the stronger current frozen ReadIndex. Neither a missing nor an unbound grant
is a reservation or proof that an in-flight control can never commit.

The shared native/linked route registrar enforces prebuffering limits. The
handler requires a signed service credential even in legacy migration mode and
returns `Cache-Control: no-store`. Typed errors preserve consumer conflicts
separately from topology changes and resynchronization. The bounded client
validates status/code/version, catalog acknowledgement, media type, response
binding and observation inequalities, and withholds results after cancellation.

The data runtime routes grants through the same immutable-fence/deadline/lane
selection as controls and pages; receivers stay local-only. The real native
Raft fixture observes both active and absent grants after an unconfirmed remote
control, then recovers its original receipt and checks the live log index. That
proves observation must not propose another command or overwrite the outcome.
All thirteen focused HTTP/ingress/client tests and all five real native Raft
integration checks pass without leaks. The real integration exposed transient
ReadUnavailable during page admission; page and grant receivers now retry only
transient read admission under one original deadline (at most five seconds),
preserving the route and identity and obtaining a fresh ReadIndex each attempt.
They never submit a control or advance an ACK. Qualification log:
`history-grant-http-retry-test.log`. Durable pending-control ownership
and recovery, production root registration, full public delivery and the rest
of the long-term issue contracts remain required.

## In progress: durable pending root-control ownership

Feed roots now carry an optional bounded exact-request journal. Preparing stores
command bytes, original immutable fence, table locator and their length-framed
SHA-256 identity, together with metadata sequence, ownership epoch and grant.
The owner must validate the typed command/fence before preparing; storing opaque
bytes does not grant permission to dispatch arbitrary commands. A unique actor
must durably transition prepared to dispatching before network submission.
Dispatching survives restart as ambiguous, even if a later grant read is absent.
An accepted response records the original positive term/index; mismatched actors,
stale intent sequences, replacement receipts and repeated begin are rejected.
Only a never-dispatched prepared intent may currently be cancelled.

These transitions use the ordinary revision/epoch-fenced metadata command path.
Decoder protocol 98 prevents older members from discarding the journal. Pending
work prevents root rebind, grant replacement, enrollment-result application,
reclamation-result application and release. Close/reset retain the exact request
for recovery, and final cleanup cannot discard it. The 8 MiB state bound still
applies; each command/fence/table is additionally bounded to 4096/16384/1024 bytes.
Legacy states may omit the optional field; parsing verifies their stored digest.

The next integration must validate and persist terminal results, atomically fold
their metadata effects, and reconcile ambiguous submissions through actual root
state without manufacturing original command receipts. The journal alone does
not yet dispatch production root controls or resolve accepted work, and is not
registered as a public feed capability. Qualification passed:
`zig build changefeed-control-test -j2` (including real metadata snapshot install
and reopen plus existing native root-adapter checks), and the final standalone
metadata suite (31 checks, including legacy omitted journal fields, monotonic
intent stamps and idempotent accepted-receipt recording). Logs:
`root-control-recovery-test.log`, `root-control-metadata-final-test.log`.

## In progress: atomic accepted-control settlement

Metadata decoder protocol 99 adds settlement of an accepted exact receipt with
its corresponding root effect in the same transaction. One bounded last-result
record remains after the pending request is removed, so a lost metadata reply
can be recognized without another native command. Exact retries are no-ops;
changed receipts, actors, request digests and stale intents conflict. Dispatching
or otherwise unconfirmed work cannot use this path. Enrollment preserves the
actual consumer/start, ACK propagation cannot exceed the metadata-authorized
whole-frame cursor, and retirement requires an irrevocably closing/reset feed.
A rejected native command clears pending work without reporting a successful
root effect. Snapshot enrollment still requires its distinct implemented cut.

The private typed outcome boundary decodes the retained original submit request
and catalog fence, verifies immutable feed/namespace/consumer/definition/grant,
checks policy and lease bounds, then validates the response against that command
and its exact accepted receipt. Initial enrollment may not have advanced native
ACKs; known consumers must match the retained epoch/start and authorized ACK
range. Unknown and superseded outcomes are rejected. The settlement proof is an
internal audit binding over feed/epoch/root/intent/request/receipt/outcome; it is
not a public credential or an independent authentication mechanism. The owner
must obtain the response through the authenticated root transport.

Qualification passed: `zig build history-control-entry-test changefeed-control-test
-j2` covers the typed private boundary, all-allocation-failure cleanup, physical
metadata snapshot/install/reopen and idempotent settlement replay, existing
native adapters, and decoder-floor enforcement. The final standalone metadata
suite passes all 32 checks, including exact retries after deletion. Logs:
`root-control-settlement-final-qualification.log` and
`root-control-settlement-final-metadata-test.log`.

The same request-scope checks can run before prepare/dispatch; validation alone
is not permission to dispatch again. Grant refresh preserves an existing
consumer epoch/start, cursor/ACK progress and any already-pinned snapshot.
Initial snapshot enrollment still requires its separate coherent-cut provider;
this native boundary rejects unsupported destination contracts.

Production dispatch, terminal reconciliation of ambiguous/superseded submissions,
root discovery/registration, coherent bootstrap and the complete long-term issue
acceptance scopes remain required.

## In progress: ordered root-control dispatch and recovery

The private root driver connects the durable journal, typed outcome validator,
metadata service and ordered control port. Prepare validates the immutable
request/fence scope and the policy's current root binding before recording it.
A fresh actor and confirmed metadata begin CAS grant one stack invocation
permission for one submit. The owner reloads and rechecks the attempt before
I/O. A recovered dispatching intent remains ambiguous and is never submitted
again; a never-dispatched prepared intent can be cancelled during close/reset.
Accepted intents recover the original command and receipt through the read-only
recovery operation.

Known positive receipts survive in the driver's fixed-size result when metadata
persistence, cancellation or ACK fails. recordAcceptance can attach that exact
attempt under a fresh maintenance request without I/O; stale sequence/actor/body
bindings cannot replace another intent. It recognizes an already-settled matching
receipt as an idempotent completion. Applied and rejected outcomes both settle
atomically; settled does not imply a successful native command or an ACK grant.
The policy must authorize maintenance, validate the current incarnation/capability
and owner binding, generate fresh unpredictable actors, and preserve the original
transport deadline/cancellation. No lock spans transport or metadata I/O.

Nine physical metadata/reopen cases cover confirmed submission, accepted-but-
unconfirmed response, ambiguous transport, lost begin/acceptance/settlement ACK,
superseded recovery, rejection and cancellation after a known native response.
The cancellation case recovers a returned receipt under a fresh maintenance
request when the original request could not persist it. They verify bounded submission counts, no
redispatch after restart, retained exact receipts and persisted root effects.
The actual native-Raft fixture now additionally drives a second consumer through
signed remote HTTP with a deliberately lost metadata acceptance ACK, reopens the
metadata store, recovers the original receipt, and checks the live Raft log index.
The metadata service in this fixture uses its standalone durable projection; this
is not a claim of full metadata-Raft failover qualification. Qualification passed:
`zig build history-control-entry-test history-control-apply-test -j2`, including
all nine driver recovery cases and all five real native-Raft checks, without
leaks. The large HTTP-server and metadata-store test objects live on the heap to
keep the combined native fixture within the main thread stack. Authoritative
log: `root-control-driver-final-test.log`.

This ordered driver is not yet registered as a production feed-root factory.
Ambiguous/superseded reconciliation still needs positive grant-state or fencing
proofs; an absent observation cannot authorize blind resubmission. Standalone
native controls need their qualified observation/receipt path, and discovery,
retired-root ownership, coherent cuts, public feeds, destination integration and
existing-client qualification remain required by the full issues.

## In progress: reconciliation from current grant state

Metadata decoder protocol 100 adds an explicitly distinct current-state
reconciliation record. It has no invented original outcome or receipt. A known
original receipt is retained only when it was already in the pending intent.
Reconciliation atomically folds the authorized metadata effect and removes the
pending attempt, retaining one bounded proof for lost metadata replies. Exact
replays remain no-ops after close/delete; settlement and observation records
cannot both occupy the latest-result slot.

The typed observer validates the original submit request, immutable root scope,
request-bound grant reply, exact grant, consumer epoch/start, policy, quota and
lease bounds. ACK requires observed progress at least to the authorized target;
renewal requires the actual lease to reach its requested extension. A delayed
original CAS cannot reverse those predicates. Enrollment recovers the actual
unchanged epoch/start. Older grants remain unproven; newer grants require
resynchronization. Unbound/absent observations cannot authorize a replay. During
close/reset, absent consumer plus an irrevocable retirement floor at or above
the old grant proves release even if the original control's outcome is unknown.
No-effect bounded maintenance can be abandoned without inventing its work count.

The driver performs a current grant read through the same frozen fence, checks
current policy/root binding before and after I/O, then uses a revision-fenced
metadata mutation. No control submission occurs on this path. It preserves a
semantic reconciliation separately from accepted-command settlement, including
when a late known receipt is presented after reconciliation.

Predicate contracts, allocation-failure cleanup, legacy state parsing and the
existing metadata/native suites pass. Real signed native-Raft qualification also
passes for lost replies, superseded receipts and an absent never-sent grant.
A deliberately lost reconciliation metadata ACK survives physical reopen and
returns the recorded semantic result without a new control proposal. The test
checks native supersession directly; remote HTTP correctly preserves the receipt
while withholding confirmation via its ambiguity marker. Closing cleanup now
allows retirement/collection/reclamation through the prepared driver instead of
cancelling every prepared command, and actual retirement through deletion passes.
Large data-server fixtures join the HTTP and metadata stores on the heap.
Authoritative logs: `root-observation-contract-test.log` and
`root-observation-live-heap-test.log` (all five native-Raft checks, no leaks).
Negative reconciliation still needs a durable retirement-fence attempt when an
old grant is unbound/absent without such a floor; production factory registration,
standalone native outcomes, topology/bootstrap and the complete issue contracts
remain required.

## In progress: durable retirement fencing for unresolved controls

Metadata protocol 101 adds a distinct retirement-fence intent that can displace
an unresolved control only after irreversible close/reset. It keeps the original
root locator and grant, records the displaced request identity and any known
receipt, and assigns a new metadata sequence and fresh dispatch actor. Repeated
fence attempts retain the first displaced identity in constant space rather than
building a chain. Active feeds cannot use this transition, and stale expected
sequences or changed locators conflict.

The fence planner builds and validates an explicit retirement command from the
current closing authority stamp. It is a new cleanup obligation, not a replay
or claimed outcome of the displaced command. The normal driver must acquire a
new confirmed begin before dispatch. Accepted fence results recover by their own
receipt; if a fence reply is lost, a current absent-consumer/retirement-floor
proof can release the root. Completion records retain the bounded displaced
identity, including when completion is semantic rather than an original reply.

Qualification passed: `zig build history-control-entry-test
history-control-apply-test changefeed-control-test -j2`. The real native fixture
exercises a never-sent original enrollment, a never-sent first fence, a distinct
fence whose entire reply is lost, metadata reopen, read-only release
reconciliation, and a delayed original packet rejected by the durable floor.
Predicate/core checks cover active-feed rejection, repeated fencing, stale
mutations and locator binding; the standalone metadata suite passes 33 checks.
Authoritative logs: `root-retirement-fence-qualification.log` (all five native
Raft checks, no leaks), `root-retirement-fence-metadata-test.log`.
Production registration, standalone native outcome qualification, retired-root
handoff/bootstrap, destination/public transports and pinned client qualification
remain required by the full goal.

## In progress: registered retained-root discovery source

Data-server API configuration now installs a retained-root source for linked
routed runtimes. The source bundles concrete routed control/page/grant ports and
the actual catalog source; local-only HTTP receivers remain unchanged. Discovery
captures one authoritative compact routing generation under the original
request budget/cancellation (at most five seconds), never an operational admin
fallback. It resolves immutable selected table IDs, verifies base-incarnation
bindings and native destination contracts, requires complete keyspace coverage,
uses canonical document identity overrides, bounds the vector to 4096 roots and
rejects duplicate namespaces. Names and ranges remain owned by the captured
generation even after publisher retirement. Initial ownership counters belong
to feed metadata; topology hashes are not substituted for them.

This source discovers candidates, not proof of full serving capability. Actual
root admission still checks the compiled owner/protocol. Capture does not grant
public authorization, a consistent snapshot, global transaction completion or
unsupported destination guarantees. validate re-captures current bindings and
requires the same root identity/topology and table locator before/after driver
work. Root-set planning and validation are now exercised by the real routed
native fixture rather than hand-built root plans alone.

Discovery contracts and signed HTTP compatibility pass, including generation
lifetime, recreate-under-same-name rejection, incomplete topology, deadlines and
allocation-failure cleanup. Current incarnation mismatch is also explicitly
checked. Live native qualification passes all five checks with root plans and
before/after binding validation supplied by the configured source. Logs:
`history-root-discovery-test.log`, `history-root-discovery-qualified-test.log`.
Production feed management/authorization, consumer scheduling, retired-root transfer,
standalone native outcomes, bootstrap and the full inbound/outbound contracts
remain required.

## In progress: internal feed metadata capability

`api/changefeed_metadata.zig` defines a callback-bound state capability with
owned bounded state parsing, authority reads, and revision-fenced mutations.
Local metadata status sources register it against the existing linearizable
service. A request facade lets shared control owners use the capability without
fabricating local projections or proposal receipts. Original request deadlines
and cancellation flow through each call; cancellation after a successful write
is an unknown mutation outcome, never evidence that no commit occurred.

Raw state requires the reserved internal maintenance principal. This is not a
public authorization policy or an HTTP endpoint. Remote metadata transport,
public management, live authorization, owner scheduling, and all remaining
bootstrap/producer/destination/client qualification contracts still require
implementation. Physical recovery tests cover a lost mutation acknowledgment,
store reopen, stale retries, and unauthorized reads. Port allocation tests target
response ownership and parser-entry failure. The combined control-entry and
retained-history RPC qualification completed successfully (exit 0), including
27 control-entry tests and 13 HTTP contracts, in
`feed-metadata-port-final-test.log`.

## In progress: private feed metadata HTTP transport

The internal OpenAPI contract now defines POST
`/internal/v1/catalog/changefeed-state-v1` for bounded state reads, authority
reads, and exact revision-fenced writes. API generation completed. Every action
names the immutable metadata group/incarnation; the handler checks that identity
before invoking the capability. Tokenless migration calls are rejected. Local
metadata HTTP sources register the same strong state port as embedded sources.

The signed client uses at-most-once mutation transport and one shared time
budget. A delegated mutation failure, malformed/mismatched reply, or ambiguous
outcome header remains unknown. Strong state reconciliation is required before
another mutation decision. Handler tests cover unsigned rejection, wrong-cluster
rejection before authority access, and positive authority binding. Client tests
cover exact dispatch counts and reply/action validation. Six explicitly
discovered tests passed in `feed-metadata-handler-test.log`; the final stricter
outcome/deadline qualification is in `feed-metadata-transport-final-test.log`.

The data runtime binding is described below. These private operations are not
public feed management, a consumer scheduler, or a coherent bootstrap
implementation. The broader completion requirements remain open.

## In progress: registered remote metadata feed port

The data runtime's `RemoteMetadataSource.statusSource` registers the feed-state
port. It captures a single deadline on the source clock, discovers the immutable
metadata identity under that budget, and uses the private state endpoint for
every state/authority read. Cached metadata heads supply identity only, never
feed state or a substitute for the endpoint's strong-read barrier. Read failover
preserves the original deadline and cancellation. Each mutation selects one peer;
an uncertain result returns to the durable owner without mutation peer failover.
Returned state storage belongs to the caller and passes the same envelope/ID
validation as local state. Maintenance-principal admission remains mandatory.

The focused runtime test covers read failover, exact write dispatch count,
state ownership/validation and expiry. The first run passed in
`feed-metadata-runtime-test.log`. The stronger rerun also checks shrinking
per-request budgets, alongside the transport lost-reply/empty-outcome tests,
in `feed-metadata-runtime-qualified-test.log`; the combined gate completed with
exit 0 (one runtime test and six transport tests, no failures or leaks).
The metadata client now maps its legacy transport ambiguity error to an unknown
feed mutation even when send telemetry is absent.

This registration does not implement a public feed owner, policy binding,
scheduling, coherent bootstrap, typed pgoutput producer or destination adapters.
Those and the remaining full issue acceptance contracts are still required.

## In progress: private distributed transaction completion evidence

`storage/transaction_completion.zig` in the embedded owner defines a bounded,
versioned private manifest and certificate, exported through
`committed_frame.completion`. The manifest binds transaction/admission identity,
original coordinator namespace/owner, commit version, and the exact ordered
participant namespaces, owner fences, locators and effect/guard/checkpoint roles.
Every participant needs a matching positive resolution at a canonical native or
Raft position. A resolution either binds an exact retained effect frame or a
positive zero-effect decision; missing frames or filtered events cannot supply
that decision. Completeness requires the exact frozen member set, including
read-only members. Digests use explicit domains and length framing; validation
hashes the manifest once rather than once per participant. Membership and counts
are private and cannot enter the public authorized projection.

Five direct frame/completion tests passed in
`transaction-completion-frame-test.log`, including corrupted/partial evidence,
stale owners, changed admission/coordinator fences, invalid positions and initial
allocation failure. Source catalog generation completed. The regular build gate
is `transaction-completion-test`, also included in the unit aggregate.

This is the structural contract, not a durable certificate producer or evidence
of distributed feed support. Required integration remains: reserve bounded
completion storage before prepare votes; persist each native frame/zero-effect
resolution atomically; retain the original frozen routing/manifest through
recovery; assemble only after every participant resolves; publish retained
completion evidence to the relevant history owners before releasing coordinator
recovery debt; preserve it through coordinator GC and topology transfer; and have
the authorized delivery owner verify it before exposing completion. Existing
coordinated frames remain rejected by the public-projection provider until those
production invariants and failure/recovery tests are implemented.

## In progress: atomic coordinator completion admission

The frozen pre-prepare `completion.Admission` is separate from the final
`Manifest`, whose commit version is selected only after voting. Its checksummed
envelope binds the original transaction, source/admission digest, coordinator
namespace/fence and exact participant roles/owners/locators. Converting admission
to a final manifest requires a positive committed version.

`TxnManager.initTransactionWithCompletionAdmission` verifies the physical
coordinator namespace and writes that envelope in the existing native BEGIN
batch with participant membership and any caller replay marker. Retries require
the exact frozen plan. Changed plans, legacy BEGIN without the frozen envelope,
and adopting an existing ordinary transaction are rejected. Reads verify the
transaction ID, coordinator role and current physical namespace. Ordinary
committed-record cleanup cannot erase completion admission before the explicit
retention/publication handoff. Fully resolved aborted transactions can retire the
plan before terminal retry-record expiry, preserving the aborted retry fence.

The physical LSM test covers store reopen, repeated BEGIN, durable replay marker,
changed input/namespace, legacy retry/adoption rejection, committed cleanup
protection, and aborted cleanup. Seven history tests and the standalone completion
gate passed in `completion-admission-qualified-test.log`. All 131 broader docstore
and transaction tests passed with no failures or leaks in
`completion-admission-docstore-test.log`.

This is a trusted storage-owner primitive. It is not yet called by production
coordinator admission and does not advertise distributed feed support. The next
integration must activate a compatible Raft protocol, reserve participant
completion credits before prepare votes, capture atomic effect/zero-effect
receipts, and complete the retained certificate handoff before releasing the
committed plan. Until that handoff exists, committed plans deliberately remain
pinned; the new primitive must not be enabled as a production serving capability.

## In progress: coordinator completion capacity credits

The history capture layer observes frozen coordinator admission in the same
native batch as BEGIN and reserves capacity from the existing aggregate history
quota in an initialized history scope, including after the last public consumer
has been removed. The
checksummed per-transaction credit binds the admission digest. Its bound covers
the saved admission, a complete certificate for every participant (including
guard/checkpoint-only members), and metadata/chunk overhead. Credits share the
limit with prepared row effects; quota failure aborts the whole BEGIN batch,
including its transaction record, plan and replay marker. Replay reuses the
original saved plan bytes after identity verification, avoiding repricing or
retry failure after serialization changes.

Completion credits survive row resolution, removal of the last public consumer,
and store reopen. Committed plans remain pinned for certificate handoff. Aborted
plans release capacity only after local intents/outbox debt and every participant
ACK drain. This partial cleanup retains the terminal aborted record until its
ordinary retry cutoff. A first/late history enrollment rejects outstanding
completion admission rather than creating an uncertified cut.

All 132 docstore/transaction tests and the completion build gate passed in
`completion-credit-replay-test.log`; the focused history gate passed eight tests
in `completion-credit-debt-test.log`. Tests cover credits plus prepared row
reservations, reopen, consumer release, unresolved abort debt, pre-expiry abort
credit retirement, atomic full-quota rejection, saved-encoding replay, and an
escaped-payload certificate bound.

This coordinator change was followed by the participant primitives below.
Unprovisioned scopes must not be advertised as supporting prepared completion
evidence. Production protocol activation, distributed participant qualification,
applying-cut/owner validation, zero-effect receipts, complete certificate capture
and publication, recovery/GC handoff and topology transfer remain required.
Public coordinated delivery stays disabled until that integration is qualified.

## In progress: participant completion admission and credit-gated votes

`TxnManager.initParticipantWithCompletionAdmission` persists the original frozen
plan at a non-coordinator participant and rejects an uninitialized history scope.
The existing capture layer reserves the same full completion bound before that
participant can prepare, even when the initialized scope has no public consumers.
Plan reads bind the transaction's coordinator/participant role to its physical
namespace and require membership in the original manifest. Admission now requires
an explicit coordinator member with the matching owner fence. The native recovery
list keeps that original coordinator locator first, independently of canonical
namespace sorting; a non-coordinator stores the original coordinator as its
recovery owner.

Planned PREPARE verifies the saved credit amount and digest under the native
apply/prepare fence. A guard-only member cannot install public row or graph
mutations; checkpoint-only writes remain limited to private metadata. A pending
planned transaction cannot commit before its prepare vote is durable, including
zero-effect participants. Existing ordinary transaction behavior remains covered
by the broader transaction suite.

All 133 docstore/transaction tests and the completion gate passed in
`participant-completion-vote-test.log`. Six direct frame/codec tests passed in
`participant-completion-codec-test.log`. Physical tests cover guard-only votes,
no-consumer credits, rejection before scope initialization, forbidden effects,
commit-before-vote rejection, reopen, original owner ordering, and refusal to
presume abort for a prepared remote participant.

These are trusted storage-owner primitives, not production protocol activation
or distributed feed qualification. The next required boundary is an atomic
per-participant resolution receipt tied to the exact retained frame or a positive
zero-effect decision and the applying native/Raft cut. It must survive retries
without allocating a new identity. The coordinator still needs authenticated
receipt collection, complete certificate assembly/publication, explicit retained
handoff before GC, and ownership/topology recovery. Frozen routed-input/payload
verification and current-owner validation must be integrated at the native
prepare ingress before exposing this capability. Public coordinated delivery
remains disabled until these contracts and failure/recovery checks pass.

## In progress: atomic participant resolution receipts

Planned committed resolutions now stage a private checksummed request binding the
frozen admission, transaction ID, commit version and exact resolved transaction
record. The final history capture stage verifies the actual record through the
shared transaction record codec (prepared, committed and intents resolved),
checks the saved credit, and consumes the request before physical commit. It
persists a bounded checksummed local resolution in the same native transaction
as the final row effects and authenticated descriptor. The effect receipt hashes
that exact descriptor, which authenticates the tuple chunks and header. Request
failure rolls back the row, frame, source-position advance and transaction
decision together.

Zero-effect commits produce a positive resolution at an advanced native source
position or the actual applying Raft term/index; they do not fabricate an empty
row frame. Missing applying markers fail the whole resolution. Retry keeps the
original receipt and commit version even after later applied markers or frames.
Lookup verifies the physical namespace, not merely that the receipt names some
member of the manifest. A retained receipt also prevents reusing its transaction
ID after primary metadata disappearance. The reservation bound now uses worst
case manifest escaping rather than assuming the future certificate serializer
matches saved admission formatting.

All 134 docstore/transaction tests and the completion gate passed in
`completion-resolution-atomic-test.log`. Ten focused history tests plus the
completion gate passed in `completion-resolution-final-test.log`. Physical
checks cover effect descriptor binding, zero-effect source positions, reopen,
retry after superseding markers, full rollback after a staged-frame validation
failure, missing Raft applying cut, and rejection of a well-formed foreign-member
receipt. The Raft-cut fixture supplies native apply markers; it is not a real
networked Raft failover qualification.

The producer and lookup remain trusted storage-owner primitives. There is no
activated distributed receipt RPC or public coordinated feed capability yet.
Required next work includes frozen-input/current-owner checks at ingress,
versioned native/Raft activation, strong routed receipt reads, authenticated
coordinator collection, complete certificate assembly/publication, retained
handoff before coordinator/participant GC, and ownership/topology recovery.
Public projection still rejects coordinated frames until that full integration
and the required failure/recovery tests pass. The full public feed, bootstrap,
destination, pgoutput and existing-client requirements remain outstanding.

## In progress: compiled cut-bound participant receipt reads

`history_resolution_wire.zig` defines bounded private request/reply binding for a
namespace, transaction, original owner fence, manifest digest, committed version
and minimum native/Raft cut. Replies include the actual observed storage cut.
A positive receipt must match all expected identities and cannot be ahead of that
cut; wrong authority domains and inconsistent Raft term/index pairs fail. A null
receipt is unproven snapshot absence, never a zero-effect result or permission
to replay an uncertain mutation. Stored receipt integrity can be validated
independently of primary transaction/coordinator metadata.

`DB.historyResolution` checks the physical identity, persisted namespace,
authority, required applied marker/native sequence and receipt in one read
transaction. Raft callers must establish ReadIndex before invoking it. The
compiled engine and storage-owner client expose this operation under engine
descriptor 39 and owner ABI 96, with strict 4096-byte bounds and owned response
cleanup. This is a private compiled capability; no public HTTP handler was added.

The compiled-owner gate passed in `history-resolution-owner-test.log` using
`history-resolution-owner-test -Dstorage-owner-test-filter='compiled ordered
history grant'`. It checks cut-bound absence, stale-cut rejection, namespace and
authority mismatch, reopen and ABI/malformed-request guards. Ten physical history
tests and the completion gate passed in `history-resolution-snapshot-test.log`,
including receipt lookup in a snapshot with primary transaction metadata absent.
The initial direct wire gate passed four tests in
`history-resolution-wire-test.log`; source catalog generation completed.

The provisioned owner source now routes receipt reads through the same captured
ReadIndex and frozen owner/catalog validation as retained page and grant reads.
It rejects native authority requests on this Raft path and requested minimum
indices ahead of the captured proof before acquiring an owner. The compiled
storage request uses the captured proof's applied index. Only after final owner
and catalog validation succeeds does the route rebind the reply digest to the
original logical request, retaining the actual stronger observed storage cut.

The compiled routed gate passed in `history-resolution-routed-test.log` using
`history-resolution-routed-test -Dstorage-owner-source-test-filter='ordered
history routed pages'`. Its real owner fixture checks cut-bound absence and
original request binding, leader loss and route replacement during final
validation, future-cut rejection, wrong namespace and wrong authority. This is
a routed compiled-owner qualification, not network or failover qualification.

Signed network receipt transport, mutation-side input/owner validation and
protocol activation, authenticated collection, complete certificate publication,
retention/GC handoff and topology recovery still require implementation and
qualification. The direct storage read does not itself establish leadership or
authorize a public caller. Coordinated public feeds remain disabled, and the
broader full issue acceptance requirements remain open.

## In progress: signed completion receipt transport contract

The internal OpenAPI spec now defines service-only POST
`/groups/{group_id}/tables/{table_name}/history/resolution-v1`, with strict
4096-byte request/response bounds, an immutable catalog route fence and required
shared remaining-time header. The request binds the original transaction,
participant namespace, owner fence, manifest digest, committed version and
positive minimum Raft applied index. Native authority is excluded from HTTP.
The response binds that request and exposes the actual qualified storage cut
with either local effects/no-effect evidence or unproven null absence. Neither
null nor one participant's positive evidence authorizes global completion or
replay of an uncertain mutation.

`make generate` completed successfully in
`history-resolution-http-generate.log`, producing the internal route and type
bindings. Unrelated model-explorer generation drift was removed. This is the
specification and generated binding stage only: the signed handler, HTTP client,
DataServer port and real network receipt qualification remain to be implemented.

## In progress: completion receipt transport implementation

The private ingress now validates explicit request version, frozen namespace,
Raft authority and positive minimum applied index before invoking a callback.
It validates the returned receipt digest/cut against the original request after
checking cancellation, so forged or late results cannot escape. The HTTP route
requires signed service authority even during migration, a positive remaining
routing budget and a bounded body. The client sends the catalog fence and
remaining budget, verifies the protocol acknowledgment and content type, rejects
mutation-outcome headers and validates the entire receipt reply.

DataServer now has a resolution port using the existing one-budget known-leader
routing path. Local reads invoke the previously qualified frozen-owner method;
remote reads use the signed bounded client. These changes are not yet qualified:
The first `history-retained-rpc-test` attempt stopped at the test-selection audit
because the newly registered client filter had no test yet. The client test now
covers positive effects, positive no-effect and absent replies, forged binding,
stale cut, missing acknowledgment, wrong content type, typed topology errors,
malformed error status, connection loss and cancellation. The native Raft fixture
now registers the resolution handler and exercises signed network absence with
an actual qualified cut, checking that the read appends no proposal. It does not
yet exercise a positive durable completion receipt across the network.

The combined `history-retained-rpc-test native-history-consumer-test` gate
completed with exit 0 in `history-resolution-network-test.log`: all fifteen
HTTP transport/auth/ingress/client tests and three storage-side native consumer
tests passed. The initial attempt had a new-test syntax error, corrected before
the passing run. The storage-side native gate does not include the DataServer
Raft fixture; its results do not qualify the actual network receipt path.

`history-control-apply-test` completed with exit 0 in
`history-resolution-raft-network-test.log`: all five real DataServer/native
Raft tests passed. The extended local-submission fixture routes a completion
receipt request from a caller with no replica over signed HTTP to the actual
leader, validates the qualified storage cut and original request digest, and
confirms that no proposal was appended. This proves the network absence-read
path, not positive durable receipt collection or global completion.

Positive durable native network receipts, mutation-side plan/input activation,
certificate collection/publication and the broader full issue requirements
still require implementation and qualification.

## In progress: frozen completion BEGIN encoding

The private native batch BEGIN now has an optional exact checksummed completion
admission envelope. It is encoded only with a distinct `begin_completion_v1`
phase. Parsing requires the phase and envelope together, validates the stored
plan checksum and transaction ID, and binds the complete participant list in
coordinator-first recovery order. Parsed envelope bytes have explicit ownership
and cleanup; re-encoding preserves the original string. Public batch ingress
rejects the command. A downgraded legacy BEGIN carrying the envelope is rejected.

The existing storage dispatch explicitly rejects this new form until its durable
completion admission path is implemented, preventing an old BEGIN from silently
ignoring the plan. This is a wire contract stage, not protocol activation.
`completion-begin-wire-test` completed with exit 0 in
`completion-begin-wire-test.log`, including the declared completion BEGIN test
and filtered root discovery; source catalog generation completed. Tests
cover exact envelope preservation, public injection, downgrade, participant
reordering/omission and transaction substitution. Durable native DB dispatch,
current owner/input fences, Raft capability activation, production coordinator
wiring and positive network receipts remain outstanding.

## In progress: native replicated completion BEGIN admission

`DB.beginReplicatedCompletionTransaction` now parses the frozen plan, validates
physical and persisted namespace plus Raft source authority, requires initialized
retained history and enters the native apply lock. It invokes coordinator or
participant completion admission with the actual applied marker in the same
native batch. Same-cut replay additionally checks the durable plan digest;
changed plans cannot obtain confirmation merely by supplying an old applied cut.
The engine descriptor is now 40 for this private applying capability.

This method does not establish leader, catalog route or input admission itself.
The versioned batch form remains rejected by the legacy server dispatcher until
its full mutation-side fencing and applying-member protocol activation are wired.
Restore/hidden scopes remain unsupported on this method until their lifecycle is
qualified. Production coordinators do not yet invoke the method.

The source `ordered-history-consumer-test` suite passed all seven selected
tests in `completion-begin-native-test.log`, including the new DB test for
initialization failure without marker/BEGIN, original envelope persistence,
changed-plan refusal at old and fresh cuts, and exact retry after physical
reopen. The same command's `history-resolution-owner-test` passed with exit 0
against engine descriptor 40, including its compiled owner scope/reopen checks.
Source catalog generation completed.

The DB admission test now additionally submits a valid 512-member frozen plan
whose required reservation exceeds a four-MiB root limit. It checks HistoryFull,
unchanged applied cut and byte-identical retained-history state, and absence of
both BEGIN record and plan. This extension passed under
`ordered-history-consumer-test` with exit 0 in `completion-begin-quota-test.log`
(all seven selected tests passed). Positive receipt network recovery, production
activation and the full issue requirements remain.

## In progress: frozen participant PREPARE input bindings

Completion admission/manifest version 2 now requires a nonzero `input_digest`
for every frozen participant, including guards and checkpoint anchors. Its
versioned hashes bind those digests as well as original owner, namespace, role
and locator. Version 1 preserves its original hash interpretation, rejects an
unhashed input field, and cannot authorize the new prepare-input check. V2's
certificate reservation accounts for the additional authenticated fields.

The input digest uses a separate length-framed namespace-bound domain over the
complete canonical private PREPARE encoding. `completionPrepareInput` uses the
existing codec, preserving effects, guards, schema/policy fields and transaction
identity without looking up current rows. `requireCompletionPrepareInput` also
checks the prepare transaction ID against the plan before comparing its input.
Callers must preserve the original encoding rather than recompile against later
schema or topology. The whole-source admission fingerprint remains distinct.

`transaction-completion-test` passed in `completion-input-binding-test.log`.
The combined `completion-begin-wire-test transaction-completion-test` passed with
exit 0 in `completion-prepare-input-test.log`, including explicit tests for V1
non-reinterpretation, V2 missing/zero inputs, changed participant inputs changing
admission/manifest identity, envelope round-trip and canonical prepare replay,
plus effects/guard/topology substitutions. Source catalog generation completed.

This contract is not yet invoked by the production durable prepare path. The
server dispatcher continues to reject completion BEGIN. Durable prepare input
checking, current owner fencing, all-applying-member activation, coordinator
wiring, positive receipt collection/publication and all remaining full issue
acceptance work remain outstanding.

## In progress: durable v2 PREPARE input enforcement

The native transaction request and mutation-extra batch now carry a trusted
completion-input digest, computed by private server dispatch from the complete
canonical PREPARE request and physical namespace. V2 transaction-manager intent
admission rejects omitted or substituted digests before writing intents; V1
cannot be silently upgraded through this field. Existing role and reserved
completion-capacity checks remain in that path.

DB prepare observes a v2 durable prepare under the apply lock before expanding
transforms/defaults or recompiling row images. An exact pending retry retains
its original prepared bytes. Under Raft, a fresh retry entry advances only its
actual applied marker under the same lock; it does not rewrite intents or emit
a new row event. The typed request boundary changes engine descriptor to 41.

The retry shortcut also verifies the original source-purpose binding. It cannot
use a local request to prepare a Raft-authoritative v2 transaction. Old actual
Raft PREPARE entries remain replayable after commit/retirement through the
existing applied-cut check, without rewriting or advancing committed state.

Completion admission reserves protocol 31 while the advertised ceiling remains
30. Proposal minimum-version selection and the Raft encoder/decoder refuse the
new BEGIN below that floor, preventing a versioned batch from being admitted as
legacy work before complete activation. Production BEGIN dispatch still rejects
it until owner/input ingress and all-applying-member activation are fully wired.

The compiled-owner gate passed against descriptor 41 and all six actual Raft
tests passed in `completion-prepare-enforcement-test.log`, including reserved
activation-floor selection and encoder rejection. That combined command exited
1 because the DB test used an incorrect direct intent lookup key. After inspecting
the actual intent format (transaction prefix + raw ID + colon + logical row key),
the source suite was rerun successfully: all seven selected tests passed with
exit 0 in `completion-prepare-source-test.log`.

The DB test checks missing/wrong input rejection without marker advance,
byte-identical original intents across a fresh retry, physical reopen, commit
resolution, a positive effects receipt at the actual committed cut and replay
of an old PREPARE after commit. `docstore-history-test` also passed all ten
selected tests with exit 0 in `completion-prepare-manager-test.log`; the extended
v2 zero-effect fixture directly verifies missing/wrong input rejection and that
a prepared retry cannot bypass source-purpose binding. Source catalog generation
completed. Production activation, positive network receipt collection/publication
and all broader issue requirements remain outstanding.

## In progress: routed completion owner identity

The private receipt spec now names the implemented
`X-Antfly-Catalog-Route-Deadline-Ms` header. `make generate` passed in
`completion-receipt-header-generate.log`; unrelated model-explorer drift was
removed. This corrects the spec contract without changing runtime behavior.

`completion.RoutedOwner` now defines the current authoritative physical-owner
context: canonical nonzero metadata cluster incarnation and group, immutable
storage/table binding, physical namespace, root generation, data group and
topology epoch. Its separate domain-bound digest is the participant's owner
fence. Root generation zero is a valid initial generation. The binding excludes
leader term and unrelated catalog revision; leadership and current routing must
be validated separately at admission. V2 `Admission.requireRoutedOwner` compares
this context against the exact frozen member and rejects changed ownership.

`transaction-completion-test` passed with exit 0 in
`completion-routed-owner-test.log`, including cluster/table/root/topology/group
changes, namespace substitution and invalid incarnation encodings. This is the
identity contract, not live-owner qualification. The next boundary must construct
it from a frozen current catalog capture and actual acquired owner, carry it in
versioned BEGIN, verify physical identity at native apply, then complete the
all-applying-member activation and production coordinator path. Full public
feed, bootstrap, destination and existing CDC client qualification remain open.

## In progress: carry and apply routed completion owner context

Versioned private BEGIN now carries `completion_owner` for v2 admission. The
codec requires it to match the frozen participant fence and BEGIN topology
epoch, preserves its fixed-size fields through owned decode, and rejects the
context on legacy BEGIN or legacy v1 admission. V1 low-level contracts retain
their previous behavior without this field.

The private native applying method now accepts that owner context and verifies
the v2 plan binding plus the actual opened namespace and `core.root_generation`
before admitting BEGIN or its applied marker. A matching plan/context for a
replacement root still fails against the actual physical DB. The typed engine
boundary is descriptor 42. Metadata cluster/table binding assertions still need
trusted live catalog admission before proposal; the native check alone does
not prove current catalog or leader authority.

The source `ordered-history-consumer-test` passed all seven selected tests,
and the completion BEGIN wire tests passed in `completion-owner-wire-test.log`.
They cover owner round-trip, missing owner, changed root context, topology
mismatch and a forged mutually matching plan/context for a different physical
generation without advancing the applied cut. The combined command's
`history-resolution-owner-test` also passed against descriptor 42, and the
combined command completed with exit 0. Source catalog generation completed.

Production dispatch remains disabled at the reserved protocol floor until live
catalog/owner admission and activation are complete. PREPARE must also carry and
check its current owner context against the frozen member and actual physical
root on every vote/retry; an input digest alone cannot authorize a replacement
owner after restore or movement. This still needs native/request wiring. All
remaining full issue requirements remain outstanding.

## Working constraints

Use this worktree and its `zig/.zig-cache` and `zig/zig-out`; no temporary build
caches. Do not spawn subagents without explicit authorization. Run appropriate
focused tests and `make gc` after long build sessions. Runtime logs are ignored
local artifacts. Keep draft PR scope/status honest as implementation grows.

## Next code boundaries to use

- Native `api/table_write_source.zig` already has `commitTransactionWithId` and
  cancellation-aware variants; `api/table_writes.zig:BoundTableWriteSource` routes
  to durable transactions. A terminal retry currently returns its old outcome
  without comparing a new payload, so a bare hash-derived transaction ID is not
  sufficient source admission. Add durable source-head/checkpoint and payload
  fencing in the same native transaction; do not bolt on a best-effort receipt.
- `storage/transactions.zig:VersionPredicate` supports internal `exact_value`
  guards. Public `storage/db/types.zig:TransactionVersionPredicate` only carries
  document versions. Any source-head exact predicate must be compiled through a
  trusted private source-admission field, not admitted from public document keys.
- Managed PostgreSQL capture is in `metadata/replication_backfill.zig`; its
  transaction iterator groups complete checkpoint runs, but destination apply
  now submits complete groups through canonical transaction commit. Preserve
  this ordered projection when adding durable destination admission. Key changes and
  filtered enter/leave need provider before-images; the current pgoutput update
  parser now retains old tuples and completeness; routed apply still must consume
  those images before claiming that scope.
- Pgwire `backend.zig:Backend.VTable` now has optional authenticated replication
  sessions and CopyBoth streaming. The native runtime still needs to register a
  production owner backed by coherent cuts, typed retained rows and fenced slot
  spooling. HTTP/SQL polling alone cannot satisfy #1639.
- Durable metadata commands/snapshot storage live in
  `metadata/storage/raft_apply_store.zig`; feed/slot records need this authority,
  not process-local maps. Public specs remain under `specs/openapi/antfly` and
  require `make generate` before handler implementation.
- Writable Iceberg admission/drain is `serverless/lake_ingestion.zig`. Its WAL
  retains original source batches, but grouped catalog commits fold mutations.
  Add recoverable committed-history references tied to actual catalog commits;
  accepted WAL entries alone are not committed outbound events. Existing external
  snapshot attachment remains a separately qualified capability.

## Source routing integration constraints

Do not copy one root-local command into every distributed participant unchanged.
A routed transaction needs an explicit, topology-fenced source-head anchor plus
payload-fenced data participants and a durable coordinator outcome. The anchor
owns the global predecessor checkpoint and receipt; individual data groups may
not have seen the preceding source transaction. Per-participant effect digests
must bind the routed effects while the source payload digest binds the entire
original transaction. Resolve recent retries through the anchor before routing
against a newer schema/topology; retain enough original participant authority to
finish recovery after lost responses. Source receipts cannot authorize early
global completion while participant resolution is incomplete.

Source progress also needs a persisted monotonic sequence and stable accepted
timestamp/transaction identity, including checkpoint-only polls. Do not order
opaque checkpoints, encode snapshot/LSN phases into numeric ranges, or advance
destination progress only for polls containing rows. Preserve old key/row and
unchanged-TOAST information from pgoutput; source transforms and route
enter/leave/key changes must use source images with declared completeness.
