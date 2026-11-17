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

## In progress: owner-fenced completion PREPARE and retries

Private PREPARE now has a distinct `prepare_completion_v1` encoding carrying
its routed owner context, including guarded prepares. The codec binds topology
epoch, preserves the context in canonical input/replay, and rejects legacy
phase downgrade. Both BEGIN and PREPARE select the reserved protocol 31 floor;
Raft encoding/decoding remains disabled below it.

Trusted server dispatch carries this owner context to native prepare. Before
materialization or a prepared-retry shortcut, DB checks the actual physical
namespace/root generation and requires an ordered applying entry. The transaction
manager checks that the context names the local member and matches the original
frozen owner fence along with its input digest, credits and source purpose.
Ordinary transactions cannot acquire a completion-owner context without a frozen
plan. Low-level vote admission carries/checks the same owner context. The typed
engine boundary is descriptor 43.

`completion-begin-wire-test ordered-history-consumer-test docstore-history-test
history-resolution-owner-test` completed with exit 0 in
`completion-prepare-owner-test.log`. The seven DB tests, ten lower-level history
tests, explicit wire cases and compiled-owner descriptor-43 gate all passed.
They cover owner omission/replacement without applied-cut advance, full prepared
retry/reopen/commit, lower-level v2 guard votes, canonical guarded replay and
legacy downgrade rejection. Source catalog generation completed.

The real DataServer Raft test now additionally checks that owner-bearing PREPARE
selects reserved protocol 31 and cannot encode below that ceiling.
`history-control-apply-test` passed all six tests with exit 0 in
`completion-prepare-owner-raft-test.log`, including BEGIN and PREPARE floor
selection and encoder rejection. Live catalog admission, activation and
production coordinator wiring remain next;
all full destination/feed/bootstrap/pgoutput/client requirements remain open.

## In progress: authoritative completion owner admission capture

`api/completion_owner_admission.zig` captures one authoritative compact routing
generation and compares cluster incarnation, route, table ID, topology epoch,
physical namespace and table storage binding with the original fence and loaded
owner descriptor. A catalog behind the original revision is unavailable rather
than authority to proceed. Unrelated newer catalog revisions do not enter the
owner digest. The helper rejects hidden/restore descriptors and non-native base
bindings until those lifecycles are qualified; it never uses an admin snapshot.

Its deadline is the minimum of request and original fence budgets, translated
to the catalog clock once. A scoped cancellation pair preserves both tokens,
including fallible checkpoints, during acquisition and validation. No borrowed
capability escapes the function. Descriptor loading has a corresponding bounded
owner-source method.

DataServer now requires a route fence for completion commands and validates the
frozen context against this capture after descriptor loading while the existing
table activity admission lease remains held through proposal. Current leader
and protocol activation are still checked by the existing proposal path. The
protocol ceiling remains 30 and native BEGIN dispatch remains disabled; this
wiring does not yet advertise protocol 31 or public coordinated feeds.

The initial `completion-owner-admission-test history-control-apply-test` passed
with exit 0 in `completion-owner-admission-test.log`. A stronger replacement
storage-binding test then exposed that `cloneRoutingTable` discarded fixed
storage settings/incarnation and private restore fields. The latter omission
is intentional for compact routing, so the fix preserves fixed storage fields
and carries an opaque `routing_storage_binding` digest over the original full
table identity. Full durable records omit the optional field and preserve their
prior JSON shape; raw restore paths remain excluded from routing. Compact
re-cloning preserves an absent binding as absent, and completion admission
rejects projections without this capability rather than manufacturing it.

The stronger capture test passed with exit 0 in
`completion-owner-admission-final-test.log` after this correction, including
replacement table binding, expired fence budget and missing cluster incarnation.
The combined projection/real-Raft verification completed with exit 0 in
`completion-owner-projection-test.log`: the capture/clone cases and all six
actual Raft regressions passed. It adds explicit clone checks for storage
identity, opaque binding preservation, legacy absence, raw-path omission and
default durable JSON shape. Source catalog
generation completed. The existing Raft tests do not yet qualify an admitted
protocol-31 completion transaction. Activation/coordinator integration, positive
network receipts/certificates and every broader full issue requirement remain.

## In progress: keep frozen PREPARE stable through proposal normalization

Before enabling actual completion proposals, review found that the ordinary
Raft admission path materializes a missing timestamp and selects revision
semantics. Doing so after freezing a participant input would change its digest.
Owner-bearing completion PREPARE now requires a positive original timestamp and
explicit `revision_v1` semantics in both its encoder/decoder and proposal
normalizer. These values must be chosen before BEGIN; admission cannot silently
recompile them later. Ordinary legacy transaction behavior is unchanged.

`completion-begin-wire-test ordered-history-consumer-test
history-control-apply-test` completed with exit 0 in
`completion-prepare-stability-test.log`: all six real Raft tests, seven DB tests
and the explicit wire cases passed. Tests cover missing timestamp or deferred
revision rejection and byte-identical
canonical input before/after proposal normalization with a different wall clock,
as well as native prepare/retry/reopen/commit. Protocol 31 remains reserved and
BEGIN dispatch disabled until actual activation/native/network qualification.
All full issue acceptance requirements remain in scope.

## In progress: actual protocol-31 completion apply qualification

The working implementation raises the native serving decoder ceiling to 31,
adds floor checks to both the raw retained fold and native document apply, and
dispatches owner-fenced v2 BEGIN into the atomic native admission method. The
server validates the original complete participant order, actual local locator,
owner group and unsupported mixed/restore inputs before dispatch. API-only
profiles remain below the native history floor. Native owner ABI is now 97 so
pre-activation owner binaries cannot silently share this capability.

The real Raft fixture now captures a current opaque table binding and owner,
freezes PREPARE timestamp/revision/input, submits BEGIN/PREPARE/commit through
the actual proposal path, checks durable activation, and fetches/retries a
positive immutable effects receipt through signed HTTP from a caller with no
replica. This qualifies the private single-root path; it does not advertise a
public distributed completion guarantee.

ABI 97 qualification passed the compiled owner receipt/reopen scope. After
fixing the real-Raft fixture to share its existing storage owner and initialize
its diagnostic transport, `history-control-apply-test history-retained-rpc-test`
finished with exit 0: six real-Raft tests and fifteen signed HTTP tests passed
(`completion-activation-test.log`). The positive path submits BEGIN, PREPARE and
commit through the general proposer and retries the immutable receipt remotely.
Source catalog generation completed before the ABI-only bump.

Completion BEGIN now uses a closed set of allowed request fields. Unrelated
private mutation fields must retain their defaults, including fields added in
future revisions; authorization and schema admission context remain allowed.
The codec and storage dispatch both enforce this constraint. The focused wire
and real-Raft regression run completed with exit 0 in
`completion-begin-closed-set-test.log`: the local wire suite, runtime discovery
suite and all six real-Raft tests passed. The validator checks empty slice
lengths, preserving decoded empty arrays without comparing allocator addresses.
Terminal resolution/recovery owner fencing, mixed-member rejection and original
root retention still need explicit audit/qualification before this activation
stage can be considered complete. A positive single-root happy-path receipt
cannot prove cross-root completion or topology recovery. Complete certificate
publication, production source/coordinator wiring and all public feed/bootstrap/
retention/pgoutput/existing-client requirements remain mandatory.

## In progress: fenced terminal completion resolution

The new private `resolve_completion_v1` phase carries the original routed owner
and participates in protocol-31 preflight/apply-floor checks. Legacy phases
cannot carry this binding or silently downgrade its presence. Proposal admission
revalidates the current compact catalog and owner descriptor. The native core
checks v2 saved-plan membership/fence and physical namespace/root generation in
the same apply-lock scope as terminal mutation. Zero-effect, abort, terminal
retry and materialized row paths carry the same binding. Existing ordinary and
v1 resolution signatures remain available; they cannot resolve a v2 completion
plan without its authority.

The compiled engine descriptor is 44 and owner ABI is 98 for this revision.
Source/control catalog generation passed. New tests reject missing authority
and replacement-root resolution without advancing the applied marker or exposing
rows, then resolve with the original owner and read the receipt. The real Raft
fixture now supplies owner-bound resolution as well. Wire tests cover downgrade,
missing owner and mixed row mutations. `completion-resolution-fence-test.log` finished with exit 0: the wire/local
suites, all seven history-consumer tests, compiled-owner reopen test and six
actual-Raft tests passed. This qualifies owner-bound row resolution/reopen and
its negative authority cases. Explicit v2 zero-effect/abort/fresh-entry terminal
retry cases are covered by the additional qualification below. Certificate publication, original-root
handoff retention and production recovery callers remain outstanding.

### Resolution edge-case qualification

The native consumer fixture now exercises a fresh-entry terminal retry and
compares its receipt with the original, rejects a replacement owner after commit,
resolves a prepared guard/checkpoint participant into an explicit no-effect
receipt at its actual Raft entry, and aborts an unprepared v2 participant without
manufacturing a committed receipt. It also rejects ownerless abort and commit
before prepare without advancing the applied marker. The focused run completed with exit 0
in `completion-resolution-zero-effect-test.log`: all seven history-consumer
cases and the local storage discovery suite passed.

## In progress: assembling qualified participant receipts

`history_resolution_wire.Assembly` owns a validated copy of the frozen plan and
collects cut-qualified read replies in canonical participant order. It checks
request transaction/manifest/version/owner binding before accepting a positive
receipt, treats absence as unproven without clearing prior evidence, deduplicates
identical receipts and rejects conflicts. Encoding requires every participant,
including guard/checkpoint no-effect participants, and validates the resulting
certificate. Wire validation does not authenticate a source: callers must use
the trusted original-owner read port and its catalog/auth/deadline protections.
This collector is not itself durable publication or authorization to expose a
public coordinated frame.

The standalone wire/assembly contract run passed all seven tests in
`completion-certificate-assembly-test.log`. The real-Raft fixture now feeds its
signed positive/retried HTTP receipt into the assembler and parses the complete
certificate. That integration run completed with exit 0 in
`completion-certificate-raft-test.log`: all six real-Raft tests passed.
Durable all-owner publication,
reserved-to-retained accounting, certificate/receipt GC independent of primary
coordinator metadata, original-root handoff retention and production coordinator
integration remain mandatory next work.

### Next publication/accounting invariants

A qualified complete certificate is the input to a private ordered publication,
not a public caller-supplied assertion. Every required owner must durably publish
it under the original physical participant fence and compare its own included
receipt with the immutable locally stored resolution. A checksummed envelope or
structural certificate validation alone does not authenticate remote evidence.
Original-owner read authorization and the operation's original deadline remain
required through collection and publication.

Publication must atomically move the promised reservation into retained bytes
covering all surviving completion material (plan, local receipt, certificate and
fixed record overhead). Exact retries must preserve bytes and accounting;
conflicting publications must fail without advancing the ordered marker.
Primary coordinator metadata GC must not destroy the retained evidence or release
its charge early. Retained certificate/receipt GC needs its own owner-fenced
eligibility and exact accounting transition, including zero-effect participants.
Original-root retirement and topology handoff must preserve the evidence and row
history until their consumers no longer need it. These are pending implementation
requirements, not guarantees supplied by the assembler.

## In progress: atomic native certificate publication

`committed_history.publishCompletion` validates the complete certificate against
the frozen v2 plan, original routed owner and physical namespace/root generation.
It compares the included local resolution with the immutable locally retained
receipt and checks that receipt against the actual persisted Raft apply cut.
The trusted caller must still obtain remote evidence from qualified owner reads;
structural validation does not authenticate a remote producer.

The reducer canonicalizes the certificate and atomically exchanges the exact
CCV1 reservation for a checksummed CRT1 retained-byte ledger covering the plan,
local receipt, certificate, first-publication journal, keys and fixed record
overhead. The checksummed journal is ordered by the actual first publication
Raft index, making zero-effect certificates discoverable without a row frame.
Fresh-entry retries preserve that original position and do not append another
journal record. Exact retries compare
all stored bytes, ledger fields and accounting without charging twice. Missing
credit, conflicting receipt, mismatched owner or corrupt ledger fails before
commit. The original plan remains retained and charged; primary metadata GC is
still conservatively blocked pending the independent retention lifecycle.

`DB.publishReplicatedCompletionCertificate` holds snapshot mutation admission and
the native apply lock, rejects hidden/restore/mirrored scopes, and commits the
publication together with the actual ordered marker. A previously applied entry
cannot create a missing publication or rewrite the marker. Engine descriptor 45
and owner ABI 99 prevent linking this layout with an older owner. Source/control
catalog generation passed. The new native tests check debit/charge equality,
row and positive no-effect certificates, same/fresh-entry retries, reopen,
conflicting local evidence, wrong-root rejection and journal identity/duplicate
checks. The first run found a duplicate test-local declaration; its compiled ABI
scope passed, and the declaration was corrected after the run completed. The
revised source run in `completion-publication-native-test.log` passed all seven
history-consumer cases and local storage discovery after correcting that
fixture's DB pointer argument.

The source native accounting suite passed (seven history-consumer cases plus
local storage discovery). The command is now wired as the isolated private
`publish_completion_v1` transaction phase: it owns its complete certificate bytes,
rejects downgraded/missing-owner/mixed-row forms, and binds transaction ID and
original routed owner. Proposal admission revalidates that owner; native dispatch
uses the actual committed Raft entry. Publication requires its own version-32
all-applying-member activation floor; version-31 completion admission does not
imply publication support. The encoder/decoder ceiling, proposer, native apply
and projection fold share that required-version classification.

The combined wire/native/compiled-owner/real-Raft run in
`completion-publication-raft-test.log` finished with exit 0: all 27 wire/local
cases, seven history-consumer cases, compiled-owner reopen scope and six real-Raft
cases passed. Its real-Raft fixture takes qualified signed HTTP receipts,
assembles the certificate, proposes publication twice and checks the durable
version-32 floor. The protocol preflight negative fixture now exercises both
history control (30 vs 29) and publication (32 vs 31), including upgrading the
incompatible peer and distinguishing unknown capability. That additional
qualification finished with exit 0 in
`completion-publication-mixed-version-test.log`: all six real-Raft cases passed,
including both explicit mixed-version preflight cases. The production coordinator, native/HA completion publication,
independent metadata GC, owner-fenced retained-evidence GC, original-root topology
handoff and all public/source/bootstrap/pgoutput/client requirements remain
outstanding.

## In progress: read-only publication recovery observations

Before enabling independent primary metadata GC, recovery needs a qualified read
of the retained publication. `CompletionPublicationQuery` carries the complete
original routed owner, transaction ID, manifest digest, commit version and
positive minimum applied index. Its versioned request digest binds all those
fields, and the observation validates that binding, actual cut, positive retained
charge and first publication position. A claimed publication beyond the observed
cut is rejected. `DB.historyCompletionPublication` checks the
physical identity and reads one snapshot; it rejects hidden/restore scopes.
The observation returns the actual persisted read cut and the immutable first
publication position, certificate digest and retained charge. Missing publication
is unproven, never permission to replay effects or release retention obligations.

The publication reducer's retained-evidence checks are shared by a compile-time
read-only branch: that branch can be instantiated with a read transaction which
has no mutation methods. It validates the certificate, frozen plan, local receipt,
CRT1 ledger, journal and accounting; it cannot create evidence or advance an apply
marker. An orphaned retained ledger without its certificate is corruption, not
absence. Engine descriptor 46 and owner ABI 100 cover the new native read entry.
Source/control catalog generation passed. The first source native run passed
all seven history-consumer cases and local storage discovery. The expanded run
in `completion-publication-read-test.log` completed with exit 0, including the
compiled-owner ABI-100 scope and all source consumer cases. It covers unproven
absence, lagging cut, wrong physical root, wrong manifest, forged request digest,
future publication position, positive no-effect publication and stable
observations/accounting after reopen. A further source run in
`completion-publication-read-recovery-test.log` passed all seven consumer cases
and local storage discovery after removing the primary transaction record in the
fixture and injecting ledger corruption: retained publication verification and
idempotent replay still work without that record; corruption is rejected without
repair or marker/accounting changes. This is evidence of storage independence,
not qualification of a production GC policy. Routed/HTTP recovery qualification
remains pending.
No primary or retained-evidence GC has been enabled by this read-only primitive.

## In progress: routed publication observation

ABI 101 adds a bounded (4096-byte) JSON publication observation operation with
strict field/duplicate validation and request-bound reply validation. The client,
compiled export and native reducer use the same query/observation types.
`orderedHistoryPublicationRouted` captures one bounded original deadline, checks
the complete original routed owner against authoritative catalog and descriptor,
uses the frozen leader ReadIndex path, and revalidates owner/catalog/proof after
the native read. The quorum frontier and actual persisted native frontier are
separate: the native read enforces the caller's requested native minimum and
returns its real persisted cut and immutable first publication position. The
quorum proof is never relabelled as a native write.

Source/control catalog generation passed. `completion-publication-routed-test.log`
finished with exit 0: the compiled ABI-101 scope and all six real-Raft cases passed.
The compiled fixture checks qualified absence, lagging cut, wrong physical root,
reopen, invalid ABI and malformed input. The actual-Raft fixture reads/retries a
positive publication for its complete certificate after publication through the
general proposer. The additional negative run in
`completion-publication-routed-fence-test.log` finished with exit 0: all six
real-Raft cases passed, including changed owner context, expired original
deadline, fallible scoped expiry despite a false cancellation probe, and injected
final frozen-leader failure after the native read (both proof validations were
observed). HTTP transport and production coordinator integration remain pending.

An additional recovery case needs qualification: protocol barriers currently
advance the group projection watermark while skipping document DB apply. The
native reader correctly refuses to serve a requested cut beyond its persisted
marker. Leadership/no-op/idle-root progress must be tested and implemented with
real durable source authority; a group ReadIndex is not a substitute for a
native persisted cut. This remains part of the full recovery/bootstrap work.

## In progress: signed HTTP publication recovery

The internal OpenAPI contract now defines the service-only
`/groups/{group_id}/tables/{table_name}/history/publication-v1` read, including the
complete original owner, request digest, actual cut, nullable immutable
publication observation and typed errors. Request/reply limits are 4096 bytes;
the relative catalog routing budget is required and bounded to five seconds.
`make generate` completed before handler implementation; generated internal API
bindings are retained, while unrelated model-explorer inventory churn was removed.

Publication query/reply/codec definitions now live in the pure
`history_publication_wire.zig` module; the native history implementation keeps
aliases for its existing entry points. HTTP validation does not import the native
ledger implementation. Direct wire qualification passed all six tests, including
request/owner binding, publication/cut bounds, duplicate fields and byte budgets.

The HTTP client signs one bounded attempt, validates scope/cut/version/response
headers and content type, rejects mutation outcome headers and treats an absent
endpoint as unsupported. The handler requires a service principal even in legacy
migration, enforces the shared budget and body limit, and returns no-store
observations/errors. Runtime routing uses known leader discovery and the existing
frozen original-owner native reader within one deadline. The real-Raft fixture
now retrieves its published certificate observation through signed HTTP from a
caller with no local replica.

The first run in `completion-publication-http-test.log` passed all 17 HTTP
contracts/authentication cases and reached its successful signed-HTTP publication
read, but the later direct late-fence fixture hit transient read admission before
its injected post-read failure. The fixture now uses the production bounded read
retry path for positive observations and one original deadline with reset
per-attempt proof counts for its direct late-fence assertion.

Publication port/client calls also own a combined fence/context scope. Both
original clocks are normalized once; the earlier deadline and both fallible
cancellation scopes survive forwarding. Identical cancellation tokens are
checked once per checkpoint. This closes the inherited generic forwarding gap
where an original route-only budget could be replaced by a fresh context budget.
`completion-publication-http-recovery-test.log` finished with exit 0 after those
changes: all 18 HTTP contract/authentication cases, seven history-consumer cases,
local storage discovery and six actual-Raft cases passed. The signed remote read
returns the same immutable certificate publication as the qualified local read,
and the late-fence fixture observes both proof validations on its final attempt. Its tests include unproven absence, positive publication,
forged request/cut/future publication, missing acknowledgment, wrong content type,
mutation-only headers, typed errors, unsupported endpoint, connection loss and
cancellation, plus service-only route mounting during migration. Production
coordinator integration, idle-root/leadership cut progress, retention/GC and all
public/source/bootstrap/pgoutput/existing-client requirements remain mandatory.

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

## In progress: idle completion recovery and sparse native cuts

The real-Raft idle regression commits a protocol barrier after certificate
publication, waits for group apply, then reads without another document write.
`completion-idle-barrier-regression.log` reproduced a bounded read failure:
forcing the root's minimum index to the group ReadIndex waited for a native
mutation which the no-op entry never performed.

Resolution and publication recovery now retain the caller's native minimum.
The native transaction still checks its persisted marker against that minimum;
the reply still contains the actual native marker. The leader barrier precedes
opening that transaction, and the same frozen leader/quorum proof and complete
owner/catalog fences are validated afterward. `applyReady` publishes group
progress only after each prior native effect has completed, so observing the
barrier establishes freshness without requiring a write for every Raft no-op.
A concurrent commit invalidates the frozen proof and forces bounded recovery.
This does not create evidence for a missing publication or permit resubmission.

`completion-idle-sparse-cut-test.log` completed with exit 0: all six real-Raft
cases passed, including that resolution and publication cuts remain below the
idle barrier. Page and grant reads now require a positive native authority marker
under the same quorum/owner fences; exact control-outcome reads preserve the
caller's minimum, which must cover its actual accepted receipt. The expanded
signed-HTTP page/grant idle regression also checks that these reads append no
Raft entries. `completion-idle-all-reads-test.log` completed with exit 0: all six
real-Raft cases passed, including the expanded signed-HTTP page/grant checks.
Formatting and diff checks passed. Restart, snapshot and leader-election
recovery qualification remain required. Full production coordinator,
retention, bootstrap, public feeds and existing-client qualification remain open.

## In progress: owned coordinator input freeze

`api/completion_plan.zig` freezes the complete canonical private PREPARE payloads
before the first BEGIN. The plan owns payload and locator bytes, full original
routed owners, a v2 admission with per-participant input digests, the common
begin timestamp and the original BEGIN creation timestamp. It preserves
non-emitting guard/checkpoint members, enforces declared effects/guard roles,
checks owner/transaction/topology binding, rejects duplicate locators and
namespaces, and bounds payloads to 16 MiB per participant and 64 MiB total.
These limits bound this coordinator plan; they do not advertise large streaming
transaction support or replace destination retention admission.

Admission members use canonical namespace order. Durable recovery locators use
coordinator-first order followed by canonical followers, as required by the
existing completion BEGIN validator and transaction manager. PREPARE retries
parse the frozen bytes into independently owned requests; they do not borrow
caller input buffers or the plan's arena. Owner capture and table activity
leases remain the invoking coordinator's responsibility; constructing a plan
provides no new authority.

The real-Raft certificate publication fixture now takes its BEGIN and PREPARE
from this builder. An allocation-failure qualification mutates the caller's row
buffer after freezing, checks the original retry payload, validates a
coordinator which sorts last, and keeps decoded retries alive after destroying
the plan. Negative cases cover duplicate participants, changed physical owners,
wrong recovery groups, missing timestamps and legacy document versions.
The allocation-failure case passed in `completion-frozen-plan-final-test.log`;
normal-stack integration validation is recorded below.

This is the input-freeze portion of coordinator integration. Normal destination
routing still uses the legacy participant RPCs and has not been switched to
completion v2. Routed source admission remains rejected until its global
checkpoint anchor, durable recovery, all-owner completion publication and
retention obligations are integrated. No destination capability is expanded by
this builder alone.

The first compiled run passed the allocation-failure case, then exhausted the
large existing integration fixture's macOS stack during HTTP-server setup.
Running the identical binary with a 32 MiB diagnostic stack passed all seven
cases (`completion-frozen-plan-stack-diagnostic.log`). The fixture's large
admission temporaries are now isolated in a non-inlined helper; normal-stack
validation is running in `completion-frozen-plan-normal-stack-test.log`. The
build/test default stack has not been enlarged.

`completion-frozen-plan-split-fixture-test.log` completed with exit 0: all seven
real-Raft cases passed with the default stack, zero skipped/failed/leaked. This
includes the complete signed HTTP recovery path and the owned-plan allocation
failure test. Source/control catalog generation, formatting and diff checks
also passed. The normal-stack fixture setup compile initially found parameter
shadowing; the corrected helper passed this final run.

## In progress: completion-aware participant dispatch

`api/completion_participant.zig` dispatches BEGIN, the exact frozen PREPARE,
owner-bound terminal RESOLVE, and complete-certificate publication through the
existing canonical routed-batch writer. It does not add a parallel phase wire
format or pass completion inputs through legacy participant RPCs which discard
them. `DataServer.completionParticipantPort` binds the actual writer. The port
validates the original metadata incarnation, namespace, group and topology
against the frozen owner, checks publication against the complete frozen
admission, and preserves the earliest caller/route deadline and both
cancellation scopes. It makes one writer invocation and does not retry an
unknown mutation. A scope failure after dispatch remains an unknown outcome.
PREPARE input freeze now requires `.write`: proposal-only acknowledgement cannot
serve as a prepared vote, and acknowledgement mode cannot change after hashing.

The HTTP adapter uses the existing signed `/batch-routed-v1` transport and
original catalog fence, percent-encodes the table path, and caps response bytes
at 4096. It validates JSON success counts and refuses success with a mutation
outcome header, missing/wrong fence acknowledgement, malformed JSON or wrong
content type. The canonical handler explicitly requires service credentials
for completion commands even when legacy unauthenticated migration is enabled.
These commands have no legacy unauthenticated compatibility contract.

The actual-Raft fixture now sends BEGIN/PREPARE/RESOLVE through signed HTTP and
publishes the same certificate through the local participant port and signed
HTTP adapter. Existing qualified native resolution/publication reads still
verify immutable receipts and first publication positions. A focused port
contract checks changed fences, pending terminal rejection, fence-only
cancellation, cancellation after dispatch and exactly one writer invocation on
unknown outcomes. A mounted HTTP test checks unsigned completion rejection in
migration mode and that a signed request reaches the writer boundary.

Source/control catalog generation and formatting/diff checks passed. Combined
real-Raft and HTTP/authentication qualification results are recorded below.

This provides the participant dispatch boundary, not a complete production
coordinator. Normal application/source admission still needs owner capture,
frozen-plan orchestration and durable decision/recovery integration; routed
source acceptance still requires the single global checkpoint anchor. No new
public destination capability is advertised. Retained evidence GC, coherent
bootstrap, public feed scheduling/transports, typed pgoutput production and
existing-client qualification remain required by the unchanged full objective.

The first combined run passed all 19 HTTP/auth cases and all eight real-Raft
assertions, but the fixture leaked its newly allocated reusable HTTP executor.
The fixture now explicitly releases that executor and pool. Authentication was
also qualified on the older `/batch` endpoint, which can decode private batches
when routed forwarding is disabled; completion commands require service
credentials there as well.

`completion-participant-recovery-test.log` finished with exit 0: 19 HTTP/auth
cases and nine actual-Raft cases passed with zero failures, skips or leaks.
Additional fault responses cover missing acknowledgement, malformed/duplicate
JSON, wrong counts/content type, an outcome header on success, oversized body
and a lost response. Every case makes exactly one HTTP attempt and returns an
unknown outcome. Final review added preservation of the upstream executor's
clock authority; the response wrapper forwards its realtime clock and retains
its monotonic Io borrow. A simulated-time token check verifies that signing does
not silently escape to the host wall clock. Final qualification is recorded in
`completion-participant-clock-qualified-test.log`.

`completion-participant-clock-qualified-test.log` completed with exit 0: all
19 HTTP/authentication cases and nine real-Raft cases passed, with zero failures,
skips or leaks. This includes actual signed HTTP BEGIN/PREPARE/RESOLVE and
certificate publication, the local publication port, immutable recovery reads,
clock-preserving signing at simulated time, malformed-response unknown outcomes
and both migration auth boundaries. Formatting and diff checks passed.

## In progress: native admission qualification and current-owner capture

A catalog descriptor alone cannot qualify completion admission: native BEGIN
also requires the actual physical namespace/root generation, writable Raft
authority and initialized committed-history state. The new private
`history_admission_wire.zig` query carries the original owner and positive
minimum native index; its bounded, request-bound reply reports the actual
persisted cut and the retention policy's total ceiling. It does not reserve
capacity, initialize history, create a consumer, cast a vote or authorize a
source checkpoint. No invented transaction or consumer identity is used.

`DB.historyAdmission` checks physical generation and namespace, persisted
identity, Raft authority, history state and the ordered marker in one read
transaction. Closed, read-only, hidden, restore-staged and async-mirrored owners
cannot qualify. Uninitialized history fails explicitly before BEGIN rather than
being inferred from a group projection watermark. The compiled owner operation
uses a 4096-byte strict JSON codec with native ABI 102 and engine descriptor 47.

`ProvisionedKernelOwnerSource.captureCompletionOwnerRouted` captures the current
owner from authoritative metadata and descriptor, requires the caller's frozen
table binding, and qualifies the actual acquired owner under the existing
leader ReadIndex/frozen proof and before/after catalog/owner checks. It retains
one original deadline and both cancellation scopes. Sparse native cuts remain
separate from group progress; a protocol barrier need not invent a native write.

The actual-Raft fixture now uses this qualified owner to freeze its plan before
signed HTTP BEGIN/PREPARE/RESOLVE. It checks changed binding and expired caller
scope, and captures again after an idle barrier, requiring the same owner and
actual native cut. The compiled owner fixture covers an uninitialized root,
positive qualification, retry/reopen, lagging requested cut, wrong physical root
or namespace, malformed query and invalid ABI.

`completion-admission-wire-test.log` completed with exit 0: all seven pure wire
and completion contract tests passed. Source/control catalog generation and
formatting/diff checks passed. Native/real-Raft qualification results are
recorded below. The first attempt stopped
before compilation because pruned object output left a cached configure-script
path; local manifests/build configuration metadata were invalidated before the
rebuild. Current worktree caches and immutable dependency fixtures are preserved.

This adds the native/source capture prerequisite. Remote owner capture still
needs its spec-first signed HTTP interface, and the normal application/source
coordinator is still on the legacy path. Initialization/configuration of a
previously unconfigured history root, global source-head admission, full
coordinator recovery, retention/GC, public feeds, coherent bootstrap and actual
CDC-client qualification remain required by the full objective.

The first native qualification run passed all nine real-Raft cases, but its
compiled-owner negative test found that `ChangefeedCapabilityUnsupported` lacked
an exact private status mapping and became `StorageKernelFailure`. Status 929
was appended for this error without renumbering any published identity. The
exhaustive inverse/uniqueness audit in `completion-admission-status-test.log`
passed all 15 tests. Engine-error and source/control catalog generation passed.

`completion-admission-status-qualified-test.log` completed with exit 0: the
compiled ABI-102 owner case and all nine real-Raft cases passed, with no failures,
skips or leaks. Positive capture, idle sparse cuts, binding/expiry rejection,
uninitialized history, physical scope and minimum-cut rejection, retry/reopen,
malformed input and invalid ABI are qualified by this run. Formatting and diff
checks passed. The result remains an observation of preconditions, not a quota
reservation or a global completion decision.

### Remote current-owner qualification

The private `history/owner-v1` contract captures and qualifies a native retained
history participant across nodes. Its request carries the expected table binding
from the coordinator's original catalog generation; its versioned reply carries
the full physical owner and native admission observation. The observation binds
the owner digest and positive persisted cut, with the configured retention ceiling
explicitly remaining a policy limit rather than an available-capacity promise.
The client rejects a changed metadata incarnation, namespace, topology or table
binding, and rejects a native observation for a different physical root.

The endpoint follows the existing signed service-only retained-history boundary,
including during legacy migration. Strict JSON, 4096-byte bodies, the original
catalog route fence, acknowledgement and one shared relative deadline apply.
DataServer routes only to the known eligible leader and retries local transient
read admission within that deadline. This is a read: it neither reserves quota
nor accepts a transaction nor advances a source checkpoint. Full coordinator
integration and the remaining public feed/bootstrap/retention/CDC qualification
requirements remain outstanding. Test results follow after verification.

`owner-capture-qualified-test.log` completed with exit 0: all 21 signed retained
history RPC/authentication tests and all nine real-Raft cases passed, with zero
failures, skips or leaks. The initial sandboxed run compiled successfully but
could not bind the fixture's loopback listener; the permitted run resolved that
environment restriction. The native observation and all owner identity checks
are validated at the typed port boundary as well as HTTP decoding. A final run
also exercises remote capture before freezing BEGIN/PREPARE/RESOLVE inputs.

`owner-capture-final-test.log` completed with exit 0: all 21 RPC/authentication
cases and nine real-Raft cases passed again, with zero skips, failures or leaks.
The coordinator fixture now rejects a changed expected binding over signed HTTP,
qualifies the owner remotely before freezing the plan, submits canonical
BEGIN/PREPARE/RESOLVE commands and publishes/re-observes the certificate. Remote
qualification also succeeds after an idle protocol barrier without manufacturing
an extra native write. OpenAPI generation, license, formatting and diff checks
passed. This completes the remote qualification prerequisite, not the full
production coordinator, source integration or public changefeed objective.

### Retained original participant owners for coordinator restart

Remote capture exposed a durable recovery gap: admission v2 authenticated each
owner digest but did not retain its full original identity. Resolving locators
against the current catalog after restart cannot reconstruct an old root
generation safely. Admission/manifest v3 therefore require a full `RoutedOwner`
for every member, validate namespace and digest equality, and expose
`recoveryOwner(namespace)` from the retained admission. Both the admission and
complete certificate carry these identities; their domain-separated v3 hashes
bind the same canonical member identity, roles, original input digest and locator.
No catalog refresh can replace a retained owner.

The frozen production plan now emits v3. Legacy v1/v2 digests and serialized
member shapes remain unchanged; legacy recovery cannot invent a missing owner.
Reservation accounting includes every owner's serialized representation in the
certificate, including guard/checkpoint participants with no public row effects.
Private BEGIN, PREPARE and resolution paths continue enforcing original input
and owner bindings for v3 as for v2.

Raft protocol 33 gates admission and certificate v3 before proposal and native
apply. Version selection decodes the JSON envelope header under bounded input
instead of matching text, recognizes escaped field names, rejects duplicate
versions, and preserves allocation errors. Existing v1/v2 admission and
certificate entries retain protocol 31/32 requirements for replay compatibility.
The applying group retains its monotonic activated protocol floor.

`completion-recovery-contract-test.log` passed all six pure completion tests.
`completion-recovery-protocol-test.log` passed both protocol tests, including
allocation failure injection. The broader durable/native/Raft run is pending.
This repairs the retained-input prerequisite; normal coordinator execution,
background certificate recovery/publication and the full source/public-feed
objective remain outstanding.

The first broader run passed the 11 storage and 27 frame cases, but the real
Raft case exposed two native `version == 2` gates: native BEGIN and prepared
status lookup rejected v3. Both gates now apply the same owner/input fencing to
v2 and v3. The focused BEGIN target also proved to contain only discovery tests
for the external codec module. An executable test in its compilation root now
calls private encoding/decoding and validation directly, checks v3 round-trip,
rejects public injection, missing or replaced owners, substituted participant
locators and changed topology. Its result and the corrected native result remain
pending; discovery success alone is not evidence of these rejection contracts.

`completion-recovery-native-test.log` completed with exit 0: 11 committed-history
storage cases, 27 frame cases, six focused BEGIN-root cases (including the new
executable boundary check), and all nine real-Raft cases passed, with no failures,
skips or leaks. The actual native owner now accepts and retains v3 BEGIN,
validates the original input at PREPARE, resolves and publishes a v3 certificate.
The group activates protocol 33 before admission and retains it through the idle
barrier/read qualification. The all-member preflight suite covers incompatible
and unavailable peers at protocol 33 as well as the older retained-history
floors. Source/control catalog generation, formatting and diff checks passed.

### Coherent retained coordinator decision observation

Coordinator recovery needs the authoritative decision and its original admission
from the same native snapshot. The private decision query binds the complete
original coordinator owner, stable transaction ID and positive minimum native
applied index. Its reply binds the request digest, actual persisted cut, canonical
record digest, decision metadata and retained admission v3 with all original
participant owners. Missing admission is unproven absence and cannot authorize
replay, presume-abort, checkpoint progress or retention release.

The native DB validates writable Raft history authority, physical namespace and
root generation, persisted identity and history policy before reading both keys
in one read transaction. The transaction-record decoder supplies the canonical
status, original begin/creation times, commit version, finalization time and
prepared/resolved flags. Aborts retain commit version zero; finalization time is
a separate field. Only a committed, prepared record can yield a committed
manifest. Pending and aborted decisions never yield a complete transaction.

The request is capped at 4096 bytes; the owned reply is capped at the admission
limit plus 4096 bytes. Strict decoding owns all variable data beyond snapshot and
C-response teardown. Native engine descriptor 48 and storage owner ABI 103 add
this read. The source wrapper establishes ReadIndex, checks the original
catalog/physical owner before and after the snapshot, preserves sparse native
cuts, and destroys owned replies if a final fence fails. It never submits writes.

The real-Raft fixture now assembles its complete certificate from the retained
native decision/admission instead of the in-memory caller plan. Compiled-owner
negative tests cover absent admission, a lagging required cut and wrong root
generation. The transaction-manager reopen test checks pending and aborted
record decoding. Pure query/reply and ownership tests in
`completion-decision-wire-test.log` pass all eight cases, including allocation
failure injection. Native/Raft qualification is pending.

Remote signed decision transport, normal coordinator execution/recovery hooks,
all-owner certificate propagation/retention release, source destination wiring
and the remaining public-feed/bootstrap/pgoutput/client work are still required.

`completion-decision-native-test.log` completed with exit 0: all 11 committed
history storage cases, nine real-Raft cases and the compiled ABI-103 owner case
passed, with no failures, skips or leaks. The native decision read returns the
original prepared/resolved committed record and v3 owner identities. The fixture
assembles and publishes its complete certificate from that persisted admission.
Sparse-cut, physical owner and minimum-index checks remain qualified. The eight
pure wire/contract cases also pass. Formatting, license and diff checks passed.

### Signed remote coordinator decision read

The internal `history/decision-v1` endpoint carries the coherent native decision
observation across nodes. The spec was updated and `make generate` completed
before handler implementation. Requests bind the full original coordinator owner
and transaction ID to a catalog route fence and one relative 1–5000 ms budget.
Membership and admission metadata remain signed service-only, including during
legacy unauthenticated migration. Requests are capped at 4096 bytes and replies
at 16 MiB plus 4096 bytes, with that reply limit applied to both the selected
forwarding executor and the client request.

The typed port validates metadata incarnation, group, topology and namespace
before dispatch. DataServer uses known eligible leader routing and bounded local
read-admission retries. Replies require a fence acknowledgement and JSON content
type, reject mutation-outcome headers, and explicitly require the envelope
version. All variable participant data is owned beyond response teardown; forged,
expired or canceled results release that ownership before returning an error.
The HTTP handler serializes the observation value and then destroys its arena.

The real-Raft fixture now requests the retained decision through a coordinator
with no local replica, over signed HTTP, and assembles its certificate from the
returned native admission. New ingress/client tests cover record states, original
owner substitution, missing versions/acks, malformed or oversized responses,
unknown/duplicate request fields, lost transport and cancellation. Qualification
is pending. Production coordinator execution/recovery and all remaining full
objective requirements remain open.

`completion-decision-http-test.log` completed with exit 0: all 23 RPC and
authentication cases and nine real-Raft cases passed, with no failures, skips or
leaks. `completion-decision-http-qualified-test.log` passed the same 32 cases
again with legacy unauthenticated migration enabled on the serving fixture:
an unsigned request to the mounted decision endpoint returns 401, followed by
successful signed remote decision recovery and complete certificate assembly.
OpenAPI generation, formatting, license and diff checks passed. The existing
application/source coordinator and background completion driver still require
integration; neither issue is ready to close.

### Completion recovery from retained coordinator evidence

A bounded recovery pass now consumes the qualified retained coordinator decision,
validates every original participant route against one authoritative catalog
generation before making recovery writes, and uses retained v3 owners throughout.
Pending/absent observations never authorize a write. A positive durable commit
can submit only the same terminal resolution or the complete certificate; this
path cannot reconstruct BEGIN or PREPARE from current rows or routing.

The pass collects qualified immutable resolutions for every member, including
guard-only and checkpoint-only roots. Missing resolution evidence after one
idempotent terminal attempt keeps the pass incomplete. It builds a certificate
only after all positive receipts exist, then observes or publishes the exact
certificate at every original owner. It reports completion only when all owners
return matching publication evidence. Unknown mutation outcomes return immediately;
a subsequent pass rereads the durable decision and reconciles retained receipts
and publication journals instead of replaying an uncertain mutation in a loop.
Completion is neither source ACK nor retention-release authority.

The DataServer exposes the composed recovery source using its existing catalog,
decision/resolution/publication ports and canonical routed Raft writer. The native
Raft fixture invokes recovery over signed remote read/write ports before manual
publication, then repeats it to verify journal reconciliation. A two-member
stateful test covers guard/checkpoint participants, missing resolutions,
publication applied before a lost reply, and non-committed decisions. Build and
qualification results are pending. Background scheduling and application/source
coordinator integration remain required.

The first driver run in `completion-recovery-driver-test.log` passed all ten
real-Raft and recovery cases with zero failures, skips or leaks. The native fixture
published its certificate through recovery before the legacy/manual retry calls.
The retained-terminal submission API takes the exact original decision query and
observation, so it validates the original request digest rather than inventing a
new lookup minimum. It now also supports an authoritative abort: it uses the
record's original finalization time for terminal abort delivery, and cannot
publish a certificate for an aborted decision. A final run verifies propagation
to both original owners while publication counts remain unchanged.

The remaining background hook is concrete: native transaction maintenance calls
`transaction_recovery_source.Source` through the storage-owner callback bridge.
Its existing resolver carries transaction ID, participant locator and terminal
status, but lacks the retained original coordinator owner needed by this driver.
Integration must carry that retained identity from the native admission, reread
its decision through the qualified port, and execute on the elected control
worker without retaining a native lease across Raft apply. It must preserve the
legacy recovery path for transactions without a completion admission. Normal
application/source admission and bounded fanout/recovery qualification remain
outstanding; exposing the DataServer recovery composition alone does not finish
those integrations.

`completion-recovery-terminal-test.log` passed all ten recovery/Raft cases with
exit 0 and zero failures, skips or leaks. The two-member case confirms abort
delivery to both original owners without creating a certificate or advancing
publication counts. A final cancellation guard now runs after the last resolution
read before returning an incomplete result; its regression cancels precisely on
the second, still-missing follower observation and verifies that no publication
is submitted. Final qualification is pending.

`completion-recovery-qualified-test.log` completed with exit 0: all ten
recovery/Raft cases passed again, with no failures, skips or leaks. This includes
the last-read cancellation regression, authoritative abort delivery to both
members, guard/checkpoint completion, unknown publication reconciliation and
native signed-HTTP certificate publication. Formatting, license, engine-error
generation and diff checks passed. The full goal remains incomplete: scheduled
recovery/admission integration, bounded fanout, destination/source wiring,
retention/bootstrap, public feeds and actual CDC-client qualification remain.

### Native maintenance to elected deferred recovery

Storage-owner ABI 104 appends an optional completion-enqueue callback to the
existing transaction-maintenance configuration. The native scan reads the
retained admission, schedules only its original coordinator, and carries the
original owner, transaction/admission identities, locator and expiry cutoff.
Source-purpose reservations carry a zero abort cutoff. Scheduling success is
not participant acknowledgement, transaction completion or retention release.
Completion records bypass both legacy replicated resolution and the local
legacy fallback; unsupported/missing completion hooks keep their debt intact.
Backpressure resumes from the first unscheduled record instead of repeatedly
filling the queue with a completed prefix.

The control queue owns copied locators, deduplicates stable owner/transaction/
admission identities, and bounds active jobs to 32. Inline executors are rejected.
Native enqueue uses a nonblocking Raft leadership check so it cannot wait behind
an apply holding the native owner. A deferred runtime maintenance job holds no
native DB/cache lease. It checks shutdown, hot standby, local coordinator
leadership and the captured term before recovery actions. The standard callback
error boundary preserves failure identity across compilation units. Jobs release
all queue ownership on success, error, leadership loss or shutdown.

The retained recovery entry point validates the scheduled admission digest against
a fresh qualified decision. An expired ordinary pending coordinator can choose
an idempotent abort, but propagates it only after rereading the authoritative
terminal decision; a raced COMMIT is never converted into an abort. Source-owned
pending work is preserved. The elected job then runs terminal recovery over the
same original owners and final publication obligations as the foreground driver.

The first qualification run passed all four focused maintenance/legacy cases and
three compiled storage cases, including a real ABI-104 native background callback
that observes a retained pending admission without resolving it. Its Raft build
caught a status-field typo in the term guard, corrected to `hard.current_term`. The
non-replicated fallback was also explicitly filtered to exclude completion
records. The final run additionally exercises deferred job execution against
actual native Raft, owned bounded queue behavior, expiry and admission mismatch.
Final results are pending. Normal admission, bounded fanout, durable completion
GC/finalization, destinations and the public-feed/CDC scope remain outstanding.

The focused maintenance suite now also proves that a non-replicated legacy
fallback cannot auto-abort retained completion work when the new callback is
absent. Capacity/leadership pressure stops the completion scan at its first
unscheduled record and preserves the previous continuation rather than skipping
that record on every sweep. The native/maintenance cases pass; the Raft test
additionally requires publication by the deferred job itself before any
foreground fallback and rejects a deliberately stale captured leadership term.

`completion-recovery-scheduling-published-test.log` completed with exit 0:
three compiled-storage cases, 11 real-Raft/recovery cases and four focused
maintenance/legacy cases passed, with zero failures, skips or leaks. The native
ABI-104 maintenance worker actually invokes the new callback with the original
retained owner/admission while the transaction remains pending. The deferred
DataServer job itself publishes the native certificate before foreground
recovery is permitted, and a stale captured term is rejected. Queue ownership,
deduplication/capacity, scheduled admission mismatch, ordinary expiry abort,
source-purpose preservation and the filtered local legacy fallback are covered.
Generation, license, formatting and diff checks passed. Normal application/source
admission, bounded fanout and the remaining full issue scope are not complete.

### Normal retained-history transaction admission

Normal provisioned Raft transaction commits now receive a completion coordinator
from DataServer. Before any BEGIN, it uses one authoritative routing session to
qualify every original native owner through the same signed/read-index ports as
recovery, compiles the entire authenticated intent, freezes canonical PREPARE
bytes, and checks the complete reservation against each root's policy ceiling.
All explicitly unsupported roots retain the legacy path; a mixed-capability
transaction fails before BEGIN. Capability qualification is not a reservation of
currently free bytes; native ordered BEGIN remains the capacity authority.

The intent compiler maps every shared intent field at compile time, so adding an
unmapped adjunct fails compilation. Row-policy proof/database/admission time,
range guards, graph effects, integrity commands and relational generation/schema
fences cannot disappear into a reduced completion payload. Root-local source
commands, restore scopes and precomputed digest substitution are explicitly
rejected. The existing legacy fanout uses the same complete intent mapping.

The ordinary admission digest derives from the canonical frozen participant set
and complete inputs, independent of route enumeration and admission wall time.
Coordinator selection uses namespace order. Qualified existing COMMIT resumes
terminal recovery, matching pending state remains outcome-unknown, and changed
input under the same ID is rejected. Each fresh phase makes one mutation attempt;
a failed/unknown BEGIN, PREPARE or coordinator decision leaves retained recovery
ownership and is not replayed within the pass. After a durable coordinator
COMMIT, recovery must positively assemble/publish every participant certificate;
incomplete work is reported as propagation pending, not a fresh commit failure.

Qualification: `normal-completion-admission-qualified-build.log`,
`ZIG_GLOBAL_CACHE_DIR=.zig-cache/global zig build history-control-apply-test -j2`
with loopback networking enabled, passed all 11 tests with no skips or leaks.
The actual native/Raft fixture now exercises normal distributed transaction
assembly, completion publication, identical stable-ID retry with no additional
Raft append, and changed-input rejection with no additional append. Existing
allocation-failure coverage now uses the exhaustive intent compiler and checks
policy/schema/generation adjunct round trips. Canonical derived admission is
stable across reordered inputs and changed wall time; invalid compiler inputs
are rejected. An earlier sandbox-only run could not bind its loopback listener;
the same compiled suite passed when networking was enabled.

This is progress, not completion of #1698/#1639. Non-write visibility levels
currently fail before BEGIN on the new path rather than claim an unsatisfied
index barrier. Hosted metadata writers, standalone/native source admission,
source-global checkpoint participants, automatic root policy activation, bounded
parallel admission/recovery and obligation/retention release remain unfinished,
alongside the previously listed public feed/bootstrap/pgoutput/client scope.

### Completion-aware visibility barriers

The completion coordinator now accepts every existing sync level. BEGIN,
PREPARE, coordinator COMMIT and complete-certificate publication retain their
write-durable contract. `propose` receives that stronger durability; `write`
requires no additional derived barrier. `full_text`, `enrichments` and
`full_index` require a separate barrier on every original participant after
completion publication, under the original aggregate request deadline.

A fresh qualified retained COMMIT, matching the frozen admission and commit
version, authorizes only a terminal-resolution command carrying the requested
sync level. It cannot recreate BEGIN/PREPARE or replay original row/transform
inputs. The existing canonical routed batch transport performs the native
owner's synchronization and preserves retryable visibility and terminal
worker-failure errors. A barrier retry may append an idempotent terminal Raft
command; it does not replace the original resolution or completion publication.
No retention release or source acknowledgement is authorized by this phase.

The normal structured commit result independently preserves propagation debt,
visibility retry debt and visibility repair debt. Both visibility flags survive
when different owners require different remedies. Callers requesting errors get
the existing propagation / visibility-not-satisfied / enrichment-worker-failed
classes in the same precedence as legacy coordination. Post-COMMIT deadline,
transport or barrier errors become committed visibility debt, never a new
ambiguous decision or permission to reapply the transaction.

Qualification: `completion-visibility-qualified-test.log`,
`ZIG_GLOBAL_CACHE_DIR=.zig-cache/global zig build history-control-apply-test -j2`
with loopback networking enabled, passed all 11 cases with no skips or leaks.
The native Raft fixture retries its committed normal transaction at propose,
full-text, enrichment and full-index synchronization, with no pending flags.
Participant tests verify exact requested levels, original owner/terminal version,
empty effect inputs, preservation of worker-failed/retry-in-progress errors, and
rejection of pending/aborted decisions, mismatched fences and invalid levels
before dispatch. The stateful two-owner coordinator fixture preserves concurrent
repair and retry debt, runs both barriers, and on successful retry does not
re-resolve original effects or republish completion. Mixed capability and an
insufficient policy ceiling are rejected before any decision read/mutation;
all explicitly unsupported owners retain the legacy path. The preliminary
12-case run also passed; its direct flag-assignment unit was replaced by the
stronger two-owner coordinator checks above.

The native fixture has no failing derived index; actual index-worker failure,
cross-node failover and the broader public-feed/source/bootstrap/retention/CDC
qualification requirements remain unproven. Hosted metadata writers and
source-global admission are still outstanding. This removes the explicit
non-write-sync rejection recorded in the preceding admission milestone; it does
not complete either issue.

### Metadata-hosted completion transport

The production metadata CDC worker now enables completion admission on its
HostedProvisionedTableWriteSource. A synchronous hosted transport supplies the
same owner, retained-decision, resolution, publication and canonical participant
ports used by the provisioned Raft coordinator. Readers use the existing strict,
bounded signed history RPCs; mutations use the existing signed routed-batch
writer with exactly one attempt. The adapter's stack lifetime ends with the
synchronous commit; retained native state and DataServer maintenance own later
recovery. Physical-only legacy adapters can retain their original callback set.

Endpoint selection requires an explicit known leader and active local/remote
replica eligibility. It resolves that leader's endpoint within the request's
original clock/cancellation budget, then rechecks leadership and eligibility
before dispatch. Unknown/ineligible leadership, a changed leader during lookup,
missing internal service credentials, and cancellation fail conservatively;
there is no placement fallback or mutation rediscovery/replay. An authenticated
unsupported response from every root can still select the coordinator's legacy
path. Missing credentials or leader evidence cannot establish unsupported
history capability.

Qualification: `hosted-completion-admission-test2.log`,
`ZIG_GLOBAL_CACHE_DIR=.zig-cache/global zig build history-control-apply-test -j2`
with loopback networking enabled, passed all 12 cases with no skips or leaks.
The actual native Raft HTTP fixture now uses a replica-less hosted adapter's
TableWriteSource stable-ID commit callback, authenticates every private RPC,
commits its ordinary transaction, and retries with full-index visibility without
propagation/visibility debt. Endpoint tests verify unknown/inactive leaders,
lookup-time leadership changes, missing credentials and cancellation without
arbitrary endpoint fallback; allocated rejected endpoints are released. A first
compile found only a fixture-local shadowed variable, corrected before this
successful run. Formatting, license and diff checks passed.

This connects ordinary hosted commits to retained completion. It does not make
the full CDC source transaction contract complete: source-global admission,
checkpoint/head anchors, root-policy activation and its rollout, retention
obligation release, public feeds, bootstrap, destination-specific integration,
pgoutput factory wiring and existing-client qualification remain outstanding.

### Consumer-independent native root policy

Private history controls now include configure_policy and inspect_policy, with
generated internal API contracts. Configuration binds the physical namespace and
control authority, an exact predecessor revision, a strictly newer revision and
byte ceiling. Exact retries recover the same policy; substituted payloads or
stale predecessors conflict. The fixed checksummed HCP1 policy record is charged
to retained bytes. Resizing cannot undercut retained bytes plus promised credits.
Configuration does not allocate a consumer, increment a consumer epoch, move an
ACK or establish a bootstrap cut.

A configured policy keeps native before/after capture enabled even with no
consumer or prepared credit. Initial activation refuses existing prepared
intents. Configured roots reject legacy BEGIN/PREPARE and pre-v3 completion
admission/preparation, so an old unprepared transaction cannot cross activation
and subsequently create unfenced effects. Abort remains available for preexisting
legacy state. New consumer enrollment must match the configured policy revision;
late enrollment uses the actual current history sequence. Capture remains on
when the last consumer retires.

Typed policy observations have a bounded HCO2 ordered receipt, preserving the
192-byte envelope and strict checksumming/canonical validation. Existing HCO1
receipts and consumer response shapes remain readable. Policy observations
cannot masquerade as consumers or feed ACK/renew effects; the feed-intent
validator explicitly refuses policy commands. Policy controls require data-Raft
protocol 34 at admission and apply across all applying members, while previous
controls retain protocol 30 and original v3 completion retains protocol 33.
Existing command ordinals remain unchanged. The by-value engine command/result
contract changed, requiring storage engine descriptor 49; opaque owner ABI 104
is unchanged because its control boundary still carries bounded JSON bytes.

Automatic table-policy provisioning/rollout, coherent bootstrap pins, native
standalone completion, timed retention and deterministic frame commit time,
completion obligation finalization/release, and full public-feed/source/client
qualification are still outstanding. The current policy configures the byte
ceiling and continuous capture; it does not implement the entire retention or
bootstrap lifecycle.

Qualification: `root-history-policy-abi49-qualified-test.log`,
`ZIG_GLOBAL_CACHE_DIR=.zig-cache/global zig build history-resolution-owner-test history-control-entry-test history-control-apply-test -Dstorage-owner-test-filter='compiled ordered history' -j2`
with loopback networking enabled, passed 58 cases (4 compiled-owner, 13 storage
support, 29 control-entry, 12 Raft), with no skips, failures or leaks. The native
case proves capture before the first consumer (first enrollment has epoch 1 and
start 1), a before/after page after enrollment, continued capture after retirement
(next enrollment has epoch 2 and start 3), policy receipt recovery after reopen,
revision/idempotence conflicts, a ceiling below retained data, stale grant policy
revision rejection, old BEGIN/PREPARE and v2 admission rejection, and positive
owner qualification before any consumer exists. Policy receipts reject forged
reserved bytes, zero revisions and discriminator substitution even with
recomputed checksums. The actual Raft owner admits configure/inspect only after
protocol 34 activation; previous protocol and feed-intent tests remain green.
The predecessor full run passed before the ABI change; the final successful run
rebuilt producer and consumers using engine descriptor 49. The attempted direct
engine import in the control-only test was removed rather than weakening its
compilation boundary. API generation completed, unrelated model-explorer output
was restored, and format/diff checks passed.


### Correct independent authorization and retention revisions

Root retention configuration revisions and feed authorization/projection policy
revisions have separate owners and meanings. The root policy milestone
incorrectly required a new lease's authorization revision to equal the root
retention revision. That check is removed. Native admission still enforces the
root byte ceiling and preserves the lease's exact authorization revision;
re-enrollment cannot substitute a different authorization revision into an
existing lease. Root policy configuration/inspection continues to use its own
predecessor/revision CAS. The two revision spaces must not be coupled by future
policy provisioning or feed enrollment.

Qualification: `root-policy-authorization-revision-test.log`,
`ZIG_GLOBAL_CACHE_DIR=.zig-cache/global zig build history-resolution-owner-test -Dstorage-owner-test-filter='compiled ordered history root policy' -j2`,
passed the compiled native case with no skips, failures or leaks. It now enrolls
a lease at authorization revision 17 while retention configuration is revision
2, rejects changing that existing lease to authorization revision 18, and
observes that retention remains revision 2. Existing capture-before-enrollment,
quota/CAS, reopen and post-retirement capture checks in the same case pass.
This supersedes the preceding milestone's claim that a grant authorization
revision must match the configured retention revision. Automatic provisioning
and the rest of the full issue scope remain outstanding.

### Provision retained capture before hosted CDC transaction admission

The production metadata HTTP CDC worker now enables initial root-policy
provisioning on its hosted transaction writer. Each original native participant
must have a confirmed policy and qualified owner before the coordinator can send
its first BEGIN. Provisioning inspects the root's existing policy and preserves
administrative revision/ceiling choices. An unconfigured or mismatched-authority
inspection is not an absence proof: only an exact initial revision-1/predecessor-0
CAS at the native default ceiling is attempted, and native namespace/authority
checks independently prevent takeover. No resize is authorized by this path.
Unknown inspection/configuration responses defer admission; a later pass starts
with inspection and can observe an installed policy without resubmitting it.

The system retention authority derives from the metadata group and incarnation,
excluding names, schema/index publications, leader terms and feed authorization
revisions. Future public feed factories must use the same system authority.
Existing roots bound to another authority fail closed until explicit handoff;
configuration is not a consumer enrollment, source ACK, or bootstrap cut.
The CDC worker's existing metadata work permit is checked before/after each
provisioning boundary through the callback ABI.

A bounded 64-entry cache belongs to the hosted writer, not native storage or a
consumer lease. Each hit still requires a fresh qualified owner/cut/ceiling and
valid work permit. Its proof binds the complete original owner digest, ceiling
and confirmed native control index; physical-generation or budget changes force
inspection again. Supported policy controls cannot remove activation on the same
physical owner; any future disable contract must invalidate this proof. Cache access uses a nonblocking atomic mutex; a contended lookup falls back to
inspection and a contended update is skipped. Cache locks are not held across
RPCs, and cache contention never blocks admission. This avoids appending policy
inspection entries for every unchanged transaction while keeping ownership and
capacity observations fresh. No new native ABI or RPC is
introduced; the existing signed control and owner protocols are reused.

Qualification: `cdc-root-policy-provisioning-qualified-test.log`,
`ZIG_GLOBAL_CACHE_DIR=.zig-cache/global zig build history-control-apply-test -j2`
with loopback networking, passed all 13 cases without skips, failures or leaks.
The actual hosted commit callback now initializes the policy through signed
HTTP/native Raft before committing and retrying with full-index visibility.
Stateful checks cover existing administrative policies, lost inspection, an
applied configuration with lost reply followed by read reconciliation, conflicting
authority, lagging native owner cut, revoked metadata lease, cache reuse and
invalidation by physical generation/ceiling changes.
`cdc-policy-metadata-worker-test.log`,
`ZIG_GLOBAL_CACHE_DIR=.zig-cache/global zig build antfly-metadata-service-test -j2`,
passed all 137 metadata service/control-loop regressions without skips or leaks.
License, formatting and diff checks passed.

This provisions hosted transactional destinations. It does not finish direct
snapshot batch integration, source-global checkpoint/head anchoring, zero-effect
source transactions, native standalone completion, coherent bootstrap, timed
retention or obligation release, public feeds, writable Iceberg destination
commits, typed pgoutput production wiring, or existing-client qualification.
Those remain required by the unchanged full goal.

Final qualification after the shared-writer cache synchronization change:
`cdc-root-policy-provisioning-final-test.log`,
`ZIG_GLOBAL_CACHE_DIR=.zig-cache/global zig build history-control-apply-test antfly-metadata-service-test -j2`,
passed all 150 cases (13 provisioning/Raft, 137 metadata service), with no skips,
failures or leaks. The added contention check holds the cache unavailable and
verifies that provisioning completes through inspection without blocking.

### Atomic snapshot destination admission

Plain PostgreSQL snapshot rows now use `TableWriteSource.commitBatch`, matching
streamed and routed effects. Missing atomic capability fails closed without a
raw batch fallback. The existing snapshot-window write sync level is preserved
to avoid waiting on enrichment while bulk ingest is paused. Both paths reject
conflicts and all four propagation/visibility debt flags before permitting
checkpoint progress.

Qualification: `snapshot-retained-admission-regression-test.log`,
`ZIG_GLOBAL_CACHE_DIR=.zig-cache/global zig build source-transaction-test -j2`,
passed all 45 cases (3 source effects, 12 envelope dependencies, 30 snapshot,
stream and parser regressions), with no skips, failures or leaks. Coverage
includes real bound destination snapshot writes, exact cutover ownership,
resumption, round fairness, and explicit rejection of each pending-debt flag
and missing atomic capability.

This does not yet supply stable snapshot transaction identities or an atomic
source-global checkpoint/head. A lost outcome can still replay snapshot
upserts. Those contracts and the remaining full goal remain outstanding.

### Retained source ownership in coordinator observations

The private decision record now exports its persisted optional source-purpose
fingerprint. The spec was updated and `make generate` completed before runtime
wiring. Native record decoding retains this field across reopening; zero
fingerprints fail validation. Ordinary replies omit the absent field, preserving
their existing JSON shape. New source-bearing replies fail closed on older
strict decoders rather than silently losing ownership.

Retained recovery uses the fresh record to exclude source-owned pending
transactions from ordinary expiry, even if a stale job supplies an expiry cutoff.
The participant expiry entry point independently rejects source ownership before
any mutation dispatch. Native storage's existing purpose fence remains in place.
This is read-side recovery ownership; terminal participant requests still need
the frozen source admission command and checkpoint-anchor integration.

Qualification: `source-purpose-recovery-test.log` passed all 13 native history
cases with no skips or leaks, including source-purpose persistence across reopen
and strict decision reply round trips. Its Raft test fixture initially failed
compilation because the new request omitted explicit protocol fields; corrected
before qualification. `source-purpose-recovery-qualified-test.log`,
`ZIG_GLOBAL_CACHE_DIR=.zig-cache/global zig build history-control-apply-test -j2`,
passed all 13 Raft/control/recovery cases without skips, failures or leaks. The
stateful recovery fixture checks both coordinator suppression and direct expiry
rejection with unchanged mutation counts. Formatting and diff checks passed.

The full source-global checkpoint and frozen-payload adapter, public feeds,
bootstrap/retention, production pgoutput, and existing-client qualification
remain required. Neither issue is complete.

### Whole-source purpose through retained Raft completion

Source ownership is now a distinct private completion command field, independent
of the single-root `source_admission.Command` that owns a local checkpoint.
`TransactionIntentRequest.completion_source_purpose` is mutually exclusive with
that command and requires a qualified completion owner and a nonzero fingerprint.
Canonical PREPARE encoding includes it in the retained input digest. Frozen plans
require identical purposes at every participant and exactly one declared
checkpoint participant. The declaration does not by itself implement a source
head or supply checkpoint writes.

Private `begin_source_completion_v1`, `prepare_source_completion_v1`, and
`resolve_source_completion_v1` reject omission, zero purposes, ordinary phase
downgrades, and missing owners. Source BEGIN requires terminal retention and a
v3 routed admission. Native BEGIN commits purpose, admission, credits and the
actual applied marker atomically; PREPARE retains it on both ordered branches.
Resolution and retained recovery/visibility retries carry the same purpose.
Existing native purpose fences reject substitution, while ordinary expiry still
cannot abort source-owned pending work. No root-local checkpoint command is
copied to followers.

These formats require all-applying-member Raft floor 35. Ordinary completion
floors remain 31/32/33 and root-policy controls remain 34. Engine descriptor 50
rejects older by-value transaction layouts and supplies the new native source
completion methods. The opaque owner JSON ABI remains 104. No public endpoint
or SDK contract is added.

Qualification so far: `source-completion-protocol-test.log` passed both protocol
classifier cases; `source-completion-purpose-test.log` passed 42 history/support
and Raft envelope cases. `source-completion-purpose-native-test.log` passed all
34 codec/runtime cases, including source phase round trips and downgrade/missing
purpose rejection. `source-completion-purpose-raft-test.log` passed all 13 actual
Raft/control/recovery cases without skips or leaks. The signed HTTP/native Raft
fixture exercises source BEGIN, no-effect PREPARE, COMMIT, decision observations,
repeated completion recovery, and a later full-index barrier. Earlier fixture
failures (shadowed names, metadata type alias, and tagged-envelope assumptions)
were corrected before these successful runs. A two-owner recovery qualification
and existing bound-adapter regression are being checked separately.

This is the retained native/Raft transport prerequisite. Production source
journal integration, durable frozen-input recovery, the source-global checkpoint
head, writable Iceberg admission, and the remaining public-feed/bootstrap/retention
and pgoutput/client requirements remain unfinished under the full active goal.

Final qualification for this change: `source-completion-purpose-two-owner-test.log`
passed all 13 Raft/control/recovery cases. The stateful two-owner fixture requires
the exact source purpose on the missing follower's terminal request, rejects
mixed purposes and multiple checkpoint owners before admission, and verifies
that a second recovery pass adds no resolution or publication mutations.
`source-completion-bound-regression-test.log`,
`ZIG_GLOBAL_CACHE_DIR=.zig-cache/global zig build source-admission-test -j2`,
passed all five bound-adapter cases, including receipt eviction, native/metadata
restart after lost replies, and transform replay protection. Both suites ended
successfully with no skips, failures or leaks. Formatting and diff checks passed.
These results do not claim that the production source worker now uses this
transport or that its source-global checkpoint has been implemented.

### Frozen completion artifact codec

`completion_plan.Plan.encodeFrozen` and `restoreFrozen` define bounded AFP1
artifacts for complete pre-admission inputs. The binary frame preserves the
original v3 admission, namespace-ordered canonical PREPARE bytes, begin timestamp
and creation time. Its domain-separated SHA-256 covers every header/payload
byte except the checksum slot. Restore requires a trusted expected digest and
validates lengths, participant count, original owners, input digests, source
purpose, canonical encoding, timestamps and exact end-of-frame consumption.
The original participant recovery locators are reconstructed without consulting
current routing, schema, rows or source configuration. Output owns its inputs.
The existing 64 MiB aggregate input and 16 MiB admission limits remain intact;
metadata can retain an artifact reference instead of copying row WAL into its
1 MiB reservation manifest. No artifact storage or durable pin is implied by
encoding or decoding. Version-1 private encoders must remain stable; future
incompatible formats require a retained versioned reader rather than replanning.

The focused `completion-frozen-plan-test` target also joins `antfly-unit-test`.
Its dedicated root supplies the normal source-owner imports without requiring
a native-server link. Tests cover deterministic ordering, complete owned row
inputs, corrupted checksum/magic, invalid counts/lengths/timestamps, substituted
payloads even with a recomputed checksum, truncation, trailing bytes, mutated
source purpose, and every allocation failure in encoding and restore.

Qualification: `completion-frozen-plan-owned-test.log` passed both discovery
and artifact tests. `completion-frozen-plan-raft-test.log`,
`ZIG_GLOBAL_CACHE_DIR=.zig-cache/global zig build completion-frozen-plan-test history-control-apply-test -j2`,
passed all 15 cases (2 codec, 13 native/Raft) without skips, failures or leaks.
The real signed HTTP/Raft source fixture now destroys the original plan and
encoded buffer before BEGIN and completes PREPARE, COMMIT, repeated recovery
and a later full-index barrier through the restored owned plan. An initial
wrong-cwd edit caused an empty-test invocation; the edit and dedicated test-root
setup were corrected before successful qualification. Formatting/diff checks pass.

This supplies the artifact contract needed by the distributed source adapter.
Persisting, pinning and reclaiming the artifact, binding its trusted reference
into the source journal, source-global checkpoint integration, production worker
wiring and every other outstanding requirement of the full goal remain required.

### Source artifact reservation, sealing, and reclamation journal

The existing source-control actor now owns bounded frozen-plan artifact debt.
A source reserves one payload (up to the AFP1 envelope bound) before upload,
seals a verified backend witness before destination admission, and keeps the
charge through acceptance and authoritative commit. Retirement makes bytes
eligible for cleanup; only a trusted fenced storage-reclamation proof returns
quota. Monotonic upload tokens and a retained reclamation record prevent old
proofs from releasing a newer allocation. Unsealed abandonment is a distinct
metadata CAS that cannot win after sealing; sealed work requires positive
destination rejection/fencing evidence before replanning.

`source_artifact_control` is backend-neutral. Native backing must reuse native/
Raft durability; it must not require an additional provider WAL or make cloud
artifact storage mandatory for default native ingestion. Existing object-backed
profiles may supply their own verified backend. The metadata journal treats
backend evidence as trusted private adapter input, never a public proof. The
actual storage adapter/physical proof verifier is not supplied by this change.

Modern delivery planning receives the source sequence, next upload token and a
logical input fingerprint. That fingerprint excludes destination manifests and
artifact hashes, avoiding a circular identity when PREPARE carries the source
purpose. Replanning preserves logical ownership while changing physical upload
authority. Legacy reservation identities and JSON shapes remain unchanged when
no artifact is used. Artifact commands require coordinated metadata floor 102;
legacy source commands retain floor 90. Classification parses typed commands,
including escaped field names, and rejects duplicate fields. No native ABI or
public endpoint changes.

The delivery coordinator reserves before calling the storage sealer, observes
durable sealing before admission, and does not admit in the pass that loses a
seal reply. It refreshes source state after release because cleanup changes the
same journal. Unsupported adapters cannot consume an artifact-enabled scope.
Artifact debt contributes to existing retention-work counters, preventing table
retirement through commit until reclamation completes. Snapshot installation
and reopening preserve that debt and upload authority. Reseeding the same
destination preserves its negotiated contract; an explicit fully drained move
to a different destination binding renegotiates capabilities, including Iceberg.

Qualification: `source-artifact-journal-qualified-test.log`,
`ZIG_GLOBAL_CACHE_DIR=.zig-cache/global zig build source-control-test source-admission-test -j2`,
completed successfully with all 41 reported metadata/native-adapter cases passing
without skips or leaks. This covers actual metadata reopen/snapshot behavior,
retention counters, coordinated decoder classification and existing native
receipt/restart/transform regressions. `source-artifact-logical-identity-test.log`
passed all 11 direct control/delivery cases after the final fingerprint helper
change, including lost sealing replies, worker replacement without provider
rows, quota lifecycle, proof replay, replanning and destination migration.
Formatting/diff checks passed. Initial inline JSON writer compilation was fixed
before qualification. GC reclaimed 86.0 GiB from this worktree's inactive build
cache using its documented recent-write override while retaining live-build
protection.

The actual native/object artifact backing, distributed destination adapter,
source-global checkpoint/head and production source-worker integration remain
unfinished. Native completion-obligation release, public feeds, coherent
bootstrap/retention, production pgoutput and existing-client qualification also
remain required by the unchanged full goal. Neither issue is complete.

### Native frozen-plan KV storage primitive

`source_plan_storage.zig` stages frozen AFP1 payloads in the caller's native
transaction. It charges an 8-upload / 256 MiB root quota before accepting chunks,
limits payloads to the existing codec bound, authenticates each 1 MiB chunk and
the whole object, and retains the first seal position on retries. The storage
seal authenticates bytes; semantic restoration still validates the completion
admission and every participant input. Native and Raft position encodings cannot
be interchanged. Root namespace, generation, fence, and storage epoch bind each
upload grant.

Explicit fencing advances a persistent root epoch. Already active uploads can
finish across another upload's retirement, while absent older grants cannot be
restarted. Cleanup deletes at most eight chunks per transaction and releases
quota only after the record and all chunks are removed. Interrupted partial
uploads can be reclaimed without treating absent chunks as corruption. There
is no timer expiry or accumulating per-upload tombstone. Missing quota under an
existing record and corrupted chunks fail closed. The frozen completion codec
now uses the same bounded digest implementation, with compile-time agreement
on its admission and participant bounds.

Qualification: `source-plan-storage-test.log` reports 11 passing pure storage /
completion contract tests. `source-plan-storage-qualified-test.log` completes
`source-plan-storage-test completion-frozen-plan-test` successfully, including
the two existing frozen-plan codec cases. `source-plan-native-storage-test.log`
reports one passing native DocStore case: aborted writes leave no quota, sealed
bytes survive close/reopen, fencing survives another reopen, reclamation releases
quota, and the old grant remains rejected after a final reopen. No skips or
leaks were reported; formatting and diff checks passed.

This is the transactional KV primitive, not the production staging facade.
Native DB/engine and ordered Raft commands, qualified read/fence/GC proof ports,
topology admission hooks, source-delivery adapters, and production worker wiring
remain outstanding. It does not establish a public feed, coherent bootstrap,
production pgoutput, or qualification with an existing CDC client. The full
objective remains active and both issues remain incomplete.

### Native source-plan owner facade and topology debt

Added typed private commands and bounded status/owned-chunk observations around
native artifact staging. `DB.nativeSourcePlanControl` verifies the persisted
namespace, physical generation and native source authority, and advances its
native source clock in the same transaction as the staging effect. Failed
commands abort that clock advance. The first seal retains its original sequence.
`DB.applyOrderedSourcePlanControl` accepts only Raft authority and commits the
artifact effect or a deterministic typed rejection with the ordinary Raft apply
marker. A reducer that rejects after staging writes is aborted before the
rejection is recorded in a fresh transaction. Only the latest command outcome
is retained; superseded outcomes require a qualified live observation rather
than re-proposal. Exact-entry command/term conflicts fail closed.

Observation and chunk reads verify physical identity, persisted authority and
actual read cut in the same snapshot. A caller requesting a native sequence or
Raft index ahead of that snapshot receives temporary unavailability. Chunk bytes
are independently owned and bounded to 1 MiB. Full catalog-fence qualification
remains the responsibility of the trusted serving owner; a supplied digest
alone is insufficient. These methods reject unsupported mirrored-writer,
hidden-child and active-restore profiles instead of silently omitting their
required durability. Native engine descriptor version 51 includes the new
methods; the opaque owner JSON ABI remains unchanged at this stage.

Retained upload quota now contributes to the existing topology-sensitive-work
scan. It inspects quota metadata, including retained physical generations, and
never reads payloads. New upload admission requires an unfenced topology; exact
retries and existing payloads can finish while topology waits for their debt.
Cleanup must drain before the existing snapshot/cutover checks can succeed.

Qualification: `source-plan-control-native-test.log` passes both actual linked
engine cases for native and ordered commands. The subsequent broader
`source-plan-control-qualified-test.log` command completed with exit 0:
`zig build native-source-plan-control-test source-plan-storage-test source-plan-native-storage-test completion-frozen-plan-test source-admission-test -j2`.
It reports both engine cases (including topology-fence admission), five existing
bound-source admission regressions, both frozen-plan codec cases and the native
DocStore recovery case passing, with no skips or leaks. The pure storage target
also completed successfully. Native and Raft seal clocks, rejected-command
rollback, lost-result recovery, command substitution, minimum-cut rejection,
reopen, quota drain and post-reclamation late admission are checked. Formatting
and diff checks passed. An initial compile-time name collision was corrected
before qualification.

Opaque-owner JSON transport, dedicated version-gated Raft dispatch, serving
route qualification, source worker adapters and source-global checkpoint/head
integration remain outstanding. Public feeds, coherent bootstrap/retention,
production pgoutput and qualification with an existing CDC client remain part
of the unchanged full goal. This does not complete either issue.

### Binary-safe opaque-owner source-plan transport

`source_plan_wire.zig` encodes private version-1 staging commands with canonical
base64 chunks, bounded to 2 MiB JSON and 1 MiB decoded binary. Decoding rejects
unknown/duplicate fields, unsupported versions, noncanonical padding, ignored
action fields and payload/hash disagreement. Parsed spec slices and decoded
binary have explicit ownership; arbitrary binary bytes survive input-buffer
release. Encoding keeps the command's semantic fingerprint unchanged. The
codec has no authority to qualify a source or a physical root.

Five additive opaque-owner operations expose native control, ordered control,
qualified observation, sealed chunk read and read-only ordered outcome recovery.
They reuse the existing versioned JSON headers; the opaque owner ABI remains
104 and the native engine descriptor remains 51. Responses own their storage;
chunk responses carry base64 rather than interpreting binary payloads as UTF-8.
The ordered writer uses the actual supplied term/index and its existing atomic
apply-marker path. Recovery does not propose or apply the command again.
Source-plan semantic errors have append-only runtime status identities 930–938,
so malformed commands, expired receipts, backpressure, conflicts and corruption
retain their identities across the boundary. Parse failures are normalized to
the declared malformed-wire error rather than generic kernel failures.

Qualification: `source-plan-wire-qualified-test.log` passes all 14 direct wire /
storage / completion tests, including arbitrary bytes, owned round trips,
canonical encodings, strict decoding and every allocation-failure point.
`source-plan-error-identity-test.log` passes all 16 failure-registry tests,
including exact source-plan error round trips and stable new status numbers.
`source-plan-opaque-owner-qualified-test.log` completes with exit 0 for
`zig build source-plan-wire-test history-resolution-owner-test -Dstorage-owner-test-filter='compiled source plan' -j2`.
The actual compiled-owner case passes upload, early-seal rejection, exact
outcome recovery, seal retry, reopen, qualified observation, binary chunk
readback, fencing, reclamation and late-admission rejection. It also verifies
old header rejection, malformed input on all four generic JSON operations and
expired chunk reads retaining their exact errors. No skips or leaks were
reported. Formatting and diff checks passed.

This qualifies ordered native DB application through the opaque owner, not a
new data-Raft entry or production HTTP route. Dedicated activated Raft dispatch,
serving route qualification, native empty-target authority provisioning, worker
adapters and source-global head/checkpoint integration remain outstanding.
No public endpoint or capability is advertised by this change. The unchanged
full goal still includes destination integration, public feeds, coherent
bootstrap/retention, production pgoutput and an existing CDC client's complete
snapshot/stream/reconnect qualification. Neither issue is complete.

### Activated frozen-owner source-plan Raft dispatch

Added a dedicated `source_plan` data-Raft envelope at protocol floor 36. Its
immutable owner includes metadata incarnation/group, table binding, physical
namespace/generation, group and topology epoch. The staging root fence is that
owner's canonical digest. The entry also carries the physical open descriptor;
namespace/generation mismatch, unsupported storage/restore profiles, duplicate
fields, mixed control envelopes and unknown fields fail closed. Ordinary batch
decoders reject these entries, including controls without row effects.

Both the raw projection and serving apply machine explicitly dispatch source
plans before accessing document batches. The projection requires a native
delegate and the durable activation floor and creates no document mutation or
split delta. Native apply acquires the descriptor-pinned owner without live
metadata IO and uses the actual committed entry term/index. The apply machine
checks the original group and publishes application progress only after native
completion. Lost native replies retry the exact retained entry outcome; known
rejections complete their entries without blocking subsequent valid commands.
Superseded results remain unresolved rather than certifying an operation.
The existing all-peer protocol preflight now covers floor 36 as well.

Qualification exposed an existing opaque-boundary gap: the native projection's
`UnsupportedBatchProtocolVersion` rejection was folded into generic kernel
failure. Added append-only runtime status 939 and exact round-trip coverage.
No engine layout or opaque JSON header changed (engine descriptor 51, owner ABI
104). The maximum understood data-Raft protocol is now 36; older source
completion/history commands retain their own original floors.

`source-plan-raft-qualified-test.log` completes with exit 0 for:
`zig build source-plan-entry-test source-plan-apply-test history-control-entry-test history-control-apply-test -j2`.
The final run had local TCP access for the existing real-Raft/ReadIndex test;
all reported cases passed without skips or leaks. It covers the new codec,
wrong-group rejection, missing native delegate, older durable floor, first
upload's lost reply, positive original-entry recovery, early-seal rejection,
continued chunk/seal progress, exact seal cut, fencing, reclamation and expired
late admission. All 13 existing native history apply cases and 29 history /
ordinary batch codec cases pass. `source-plan-raft-error-test.log` passes all 16
error-registry tests. Formatting and diff checks passed. Initial test-target
linkage and a compile-time decoder issue were corrected before qualification;
the sandboxed discovery run's local-bind failure was resolved by the final run
with local socket access.

Dedicated dispatch does not yet supply the production source-plan proposer,
route-qualified HTTP read/write ports, native empty-target provisioning, worker
adapter, or the source-global head/checkpoint participant. These remain required
alongside destination integration, public feeds, coherent bootstrap/retention,
production pgoutput and qualification with an existing PostgreSQL CDC client.
The full goal and both issues remain incomplete.

### Request-bound source-plan observations and owned chunk reads

Private observation replies now include the exact typed query digest. Chunk
replies include the typed read-request digest, covering the upload, expected
seal, ordinal and minimum cut. This binds even unsealed presence/bitmap replies,
which have no seal proof yet, to their requested upload. Typed opaque-owner
client methods validate these replies and return independently owned binary.
The native owner validates generated observations before serialization.

Observation validation checks physical root equality, authority-specific cut
encoding and the requested minimum native sequence or Raft index. Quota bounds
include the minimum possible charge for its active upload count. Lifecycle
validation rejects unused bitmap bits, impossible presence/absence combinations,
partial un-fenced seals, proof/position mismatch, future seal/fence positions,
Raft term/index contradictions and reclamation without an advanced epoch.
An initial zero native source clock is allowed only without artifact debt and
with the initial storage epoch; it is not a synthetic positive seal cut.
Seal, fence and reclamation evidence are recomputed from the exact spec.

Chunk decoding requires a live matching seal and authenticates owned bytes with
the requested ordinal's hash. It bounds both JSON and decoded binary, verifies
canonical base64, and explicitly compares the decoder's computed output size
before writing. Merely matching encoded length is insufficient: changed padding
can otherwise describe a larger or smaller decoded output of the same encoded
length. Tests cover both cases. Parsed and binary allocations are freed on all
failed decoder paths. Existing ledger hashes and by-value engine layouts are
unchanged (engine descriptor 51, opaque owner header 104); these are private
version-1 source-plan reply envelopes.

Qualification: `source-plan-observation-padding-test.log` passes all 20 direct
wire/storage/completion cases, including upload substitution, root/quota/bitmap
corruption, native-zero distinction, causal native/Raft cuts, proof tampering,
owned bytes, padding changes, and all allocation failure points in both read
reply decoders. `source-plan-observation-qualified-test.log` completes with exit
0 for `source-plan-wire-test history-resolution-owner-test native-source-plan-control-test source-plan-apply-test` with the compiled source-plan owner filter;
the opaque-owner, two native-control cases and Raft case all pass without skips
or leaks. After the final decoder-size guard, `source-plan-observation-final-test.log`
completes `source-plan-wire-test history-resolution-owner-test` with the same
compiled-owner filter successfully. The actual typed observation/chunk methods
are exercised across the compiled owner. Formatting and diff checks pass.

Production HTTP ports, route/leader qualification and the source worker adapter
are still outstanding; this contract supplies the validated read boundary they
need. Native empty-target provisioning, the source-global head/checkpoint,
destination integrations, public feeds, coherent bootstrap/retention, production
pgoutput and existing-client qualification remain required. Neither issue nor
the full goal is complete.

### Authenticated source-plan staging through native Raft

The internal OpenAPI now defines bounded version-1 control, observation and
chunk routes under each physical group/table. Generated bindings accompany the
specification. All three require an internal service principal and an immutable
catalog route fence. Operations check the physical namespace, metadata
incarnation, topology epoch and full source completion owner before accessing
storage. The API-kernel callback configuration ABI is 25; engine descriptor 51,
opaque owner header 104 and data-Raft protocol 36 remain unchanged.

Control submissions use the existing local-leader lifecycle, including term and
membership capture, all-peer protocol preflight and write admission. A dedicated
source-plan entry supplies the storage command. Once accepted, failures preserve
the exact term/index receipt with an unknown outcome. Recovery reads only that
receipt through a fresh ReadIndex and native outcome lookup; it never reproposes
a command or protocol barrier. Replies bind the original typed request digest.
Known rejections remain distinguishable from uncertain outcomes. Observation and
chunk reads revalidate owner, route and frozen read barrier after storage access.
Chunk responses own their bytes and validate the expected seal and ordinal hash.
Cancellation is checked before parsing untrusted bodies; ordinal bounds are
checked before indexing evidence bitmaps.

Qualification: `source-plan-http-native-test.log` completes with exit 0 for
`source-plan-http-test history-control-apply-test source-plan-wire-test`.
All six HTTP contract tests and all 13 native Raft tests pass without skips or
leaks. The actual signed HTTP/native/Raft fixture exercises an accepted reply
loss, recovery without advancing the log, incomplete-seal rejection, chunk
upload, stable repeated seal, owned chunk retrieval and restoration of the exact
AFP1 plan and source purpose. It also rejects a substituted table binding and
verifies fence/reclaim makes the old chunk unreadable. The wire/storage target
also succeeds. Fixture compilation errors were corrected before this final run.

These DataServer callbacks serve local leaders. Remote client routing and the
source delivery worker adapter remain outstanding. Trusted internal callers
must durably reserve the exact input in source metadata before beginning an
upload; these storage routes do not independently look up a metadata source
grant. Native empty-target authority provisioning, the distributed source-global
head/checkpoint, destination integrations, public feeds, coherent bootstrap and
retention, production pgoutput and existing-client qualification also remain
required. Neither issue nor the full goal is complete.

The broader `history-serving-capability-test` also passes all 29 cases without
skips or leaks in `source-plan-http-capability-test.log` (exit 0). Its first
sandboxed run failed only at the two local TCP binds; the final run granted local
socket access. Source-catalog validation, `zig fmt --check` on every changed Zig
file, and `git diff --check` pass.

### Routed source-plan transport

Source-plan control, observation and chunk worker ports now use the same bounded
known-leader capture and forwarding lane as retained-history operations. Routing
requires the original physical namespace, metadata incarnation and topology
owner. It does not fall back to an administrative snapshot or placement scan
when the leader is unknown. HTTP ingress retains local-only callbacks, preventing
forwarding loops. Routed operations intersect caller and route-fence deadlines
and cancellation before discovery or local dispatch.

The signed HTTP client makes one transport attempt per operation. Mutation
ambiguity preserves the original recovery receipt; it never retries submission
or receipt recovery automatically. Control replies require the original request
digest, exact recovery receipt, route acknowledgment, JSON content type and valid
phase evidence. Any unknown-outcome marker, including an empty marker, prevents
confirmation. Observations bind the requested query; owned chunk bytes bind the
read request, live seal, ordinal and chunk hash. Cancellation after chunk decode
frees the decoded allocation. Conflict responses to reads fail closed with
resynchronization required; they do not authorize a replacement upload or a
source checkpoint advance.

Qualification: `source-plan-routed-http-test.log` completes with exit 0 for
`source-plan-http-test history-control-apply-test`. All eight source-plan HTTP
and client tests and all 13 native Raft tests pass without skips or leaks.
Client tests cover signed escaped routes, one bounded attempt, original receipt
preservation, lost and malformed responses, request/receipt substitution,
missing acknowledgments, wrong content types, unknown markers, cancellation,
expired route budgets, corrupt chunk bytes, and resynchronization. The actual
signed HTTP/native/Raft fixture now recovers a lost accepted receipt through a
second node without advancing the log, reads its quota through that node, and
retrieves independently owned sealed bytes through routed chunk reads.

The first run identified fixture header-type and peer-view freshness issues,
which were corrected. A subsequent run lost its cache files during compilation;
the final cold rebuild completed successfully. Formatting and diff checks pass.

This supplies routed transport, not the production source delivery adapter.
The source-global head/checkpoint, native empty-target provisioning, worker and
destination integrations, public feeds, coherent bootstrap/retention, production
pgoutput and existing-client qualification remain required. The full goal and
both implementation issues remain incomplete.

### Metadata-reserved source-plan delivery bridge

`api/source_plan_delivery.zig` supplies the routed artifact-store bridge needed
by source delivery adapters. It verifies the metadata record's lifecycle and
exact artifact specification before using storage. Each transfer pass intersects
the original and route-fence deadlines/cancellation with a bounded five-second
budget. Upload validates whole-object, frozen-plan and chunk commitments before
BEGIN, resumes from the qualified received bitmap, and stops immediately after
an uncertain command. A subsequent pass observes the immutable upload identity;
completed chunks and the first successful seal are reused.

Loading requires a sealed/accepted record and its exact recorded stamp. It
returns independently owned frozen bytes, validates every chunk against the
live seal and ordinal hash, and rechecks whole-object and frozen-plan identity.
No provider rows are needed for this read. Reclamation requires a validated
metadata garbage record with a nonzero retirement proof. A pass fences if
necessary and performs one bounded native reclaim command; a partial result
returns null, keeping the metadata charge until an actual reclamation proof is
available. The caller must load the current journal record under source ownership;
this bridge does not independently query metadata or infer retirement authority.

Qualification: `source-plan-delivery-test.log` completes `source-plan-http-test`
with exit 0, all nine cases passing without skips or leaks. The bridge test drops
replies after BEGIN, chunk and seal independently, verifies immediate stop and
bitmap-based resume without duplicate phase mutations, retrieves retained owned
bytes, rejects an altered specification and expired route, and rejects premature
or malformed retirement. `source-plan-delivery-native-test.log` completes
`history-control-apply-test` with exit 0, all 13 cases passing without skips or
leaks. Its real signed HTTP/native/Raft fixture uses the bridge through a second
node to upload/seal a genuine frozen completion plan, recover and restore it,
and reclaim it. Source catalog, formatting and diff checks pass.

The bridge is not yet wired into the production source journal adapter. The
source-global head/checkpoint and production destination/worker integration
remain required, as do empty-target provisioning, public changefeeds, coherent
bootstrap/retention, production pgoutput and existing-client qualification.
Neither issue nor the full goal is complete.

### Native checkpoint intent and protocol-37 source admission

`storage/source_checkpoint.zig` adds the checkpoint head intent for a qualified
source completion participant. Its binding is the logical source scope, not a
row shard, upload attempt, feed cursor or PostgreSQL LSN. The command retains the
source-purpose fingerprint, original acceptance time, monotonic source sequence,
opaque checkpoint and exact predecessor. Planning returns a protected metadata
write plus an exact-value predicate; it performs no standalone write. Native
PREPARE attaches both to the transaction. Commit resolves the head with the
other intents; abort discards it. The transaction manager permits this metadata
intent only for a source-owned declared checkpoint participant. Ordinary writes
cannot address its protected prefix.

The bounded current-head record uses the checksummed `SCP1` binary format and
retains sequence, purpose, transaction identity, acceptance time and checkpoint.
Checkpoint bytes are never ordered. Exact current retries resolve as already
committed; altered retries conflict, old sequences expire, and gaps or wrong
predecessors fail. This is one current head per binding, not a historical batch
receipt window or a complete source-lifecycle fence.

The trusted intent and canonical private PREPARE now carry the checkpoint
command. Frozen plans bind it into the participant input digest and reconstruct
it from retained AFP1 bytes. A plan containing a head write emits
`begin_source_checkpoint_v1` and `prepare_source_checkpoint_v1`, requiring
all-applying-member data-Raft protocol 37 before BEGIN. BEGIN includes an explicit
checkpoint marker; stripping the new phase tag is rejected. PREPARE requires the
exact command and matching source purpose. Public batch JSON rejects these
private envelopes. Existing source-purpose-only version-35 envelopes remain
readable and retain their previous floor.

Engine descriptor 52 and API-kernel ABI 26 fence changed by-value mutation and
callback layouts. Storage-owner ABI 105 fences the new append-only failure
statuses: checkpoint conflict 940, transaction identity conflict 941 and already
committed 942. The shared failure envelope layout remains ABI 89. Source
validation identities cross the complete native/owner/operation/HTTP stack as
explicit typed rejections; unknown-outcome markers, including empty markers,
still prevent confirmation. Expected deterministic source rejections advance
the applied Raft position so a following explicit abort can proceed.

Qualification: `source-checkpoint-contract-test.log` passes the direct head
contract test, including cyclic opaque positions, exact/altered retries,
corruption and old-sequence fencing. `source-checkpoint-error-boundary-test.log`
passes exact status/envelope round-trips including numeric identities 940–942.
`source-checkpoint-native-test.log` completes
`completion-begin-wire-test history-control-apply-test` with exit 0: 29 codec
cases, six API discovery/codec cases, and all 13 native Raft cases pass without
skips or leaks. The real signed cross-node completion fixture restores a frozen
checkpoint plan, verifies its BEGIN requires version 37, commits the first head,
rejects a conflicting first sequence and a sequence gap, explicitly aborts those
transactions, prepares then aborts a second checkpoint, and successfully commits
a different second checkpoint from the original predecessor. It also exercises
typed source errors and unknown/empty-outcome markers without mutation retries.
Initial qualification identified missing RPC and storage-owner rejection mappings
and a test expectation for `.aborted`; these were corrected before the final run.

Stable checkpoint-authority provisioning/routing, source-scope retirement fences,
historical retry/receipt bounds, production source journal/destination/worker
integration and cross-topology qualification remain outstanding. The native
checkpoint intent does not advertise those capabilities by itself. Public feeds,
coherent bootstrap/retention, production pgoutput and pinned existing-client
qualification also remain required. Neither issue nor the full goal is complete.

Final boundary qualification: `source-checkpoint-abi-test.log` completes
`api-kernel-abi-test history-resolution-owner-test` with the compiled-owner ABI
filter and exit 0. The actual compiled storage-owner ABI case and all eight API
kernel/layout/import cases pass without skips or leaks. The new reusable
`api-kernel-abi-test` target checks incompatible contexts as well as C layouts.
`source-checkpoint-final-generated-check.log` completes source-catalog,
engine-error, C-header and OpenAPI checks successfully. All changed Zig files
pass formatting checks; the diff check passes. Unrelated model-explorer JSON
from generation was restored before committing.

### Durable source checkpoint authority binding

The source journal now binds its checkpoint authority before the first source
reservation. Metadata protocol 103 gates binding and retirement-fence records;
legacy commands retain their earlier floors and legacy state JSON omits the new
field. Binding is immutable for the scope, enables retained-artifact admission,
and cannot be added after pending/accepted work. Reordering source definitions
preserves the binding. A different row-routing choice cannot select a new head.

The stored full owner is the original physical reference. New attempts must
capture a fresh qualified owner and use `requireOwner` to check the same metadata
incarnation, namespace, group and physical root generation. Schema/topology
lease fences may change without moving that physical authority. A changed root
or metadata incarnation requires explicit recovery/migration; the reference is
not provisioning or migration proof.

Delivery passes the bound reference to planning. A new plan's candidate is bound
through metadata CAS, then reloaded and confirmed before reservation, upload or
BEGIN. A lost binding reply stops the pass. Subsequent planning must retain the
binding and artifact contract; a plan that drops it is rejected. Replanning also
checks the existing binding. Adapters must explicitly declare checkpoint
support; legacy adapters cannot silently consume this profile.

Retiring a bound scope leaves checkpoint-fencing work outstanding. Metadata
requires an attested fencing proof for the exact original authority before
reseed and keeps table-retention work counters charged until it is recorded.
The proof is supplied by a qualified internal adapter, like artifact sealing
and reclamation proofs; deterministic metadata apply does not perform native
network reads. Binding apply verifies the metadata group/incarnation and live
source definition. Retired cleanup remains possible after source definition
replacement. A separate incarnation-key buffer preserves the source-state key.

Qualification: `source-authority-qualified-test.log` completes
`source-checkpoint-authority-test source-control-test` with exit 0. Its five
selected suites pass 13, 12, 12, nine and three cases without skips or leaks.
Coverage includes legacy behavior; immutable binding; refreshed owner fences
versus changed physical generation; lost binding replies stopping before upload
and admission; refusal to drop the checkpoint from a later plan; reuse of the
bound owner when a candidate row group changes; explicit protocol-103 decoder
classification; native metadata reopen; foreign-incarnation rejection; and
snapshot restore retaining authority/retirement debt and table-retention work
counters. Both legacy and authority-enabled artifact lifecycles run through
reopen and reclamation. The initial broader API filter was narrowed to avoid
unrelated delivery tests. The decoder-ceiling test was updated for protocol 103.

`source-authority-generated-check.log` passes source-catalog, engine-error and
OpenAPI checks. All changed Zig files pass formatting checks and the diff check
passes. Unrelated model-explorer JSON from generation was restored.

Physical checkpoint provisioning, native source-scope fencing, qualified
migration and production adapter/worker wiring remain outstanding. The metadata
fence record is a required attestation boundary, not an implementation of native
fencing by itself. Historical retry bounds, cross-topology recovery, other
destination integrations, public feeds, coherent bootstrap/retention, production
pgoutput and existing-client qualification remain required. The full goal and
both issues remain incomplete.

### PostgreSQL stream progress independent of derived indexes

Production `StreamingReplicationRunner` now requests `.write` for complete
source transactions, matching the durable snapshot apply level. Provider resume
progress no longer waits on the embedding/index publication frontier. Atomic
commit capability, conflicts, unresolved propagation, and unexpected unresolved
visibility results still fail closed before the status checkpoint advances.
This change does not add stable batch identity, destination checkpoint intents,
or lost-reply recovery to this existing production runner.

Qualification: `ZIG_GLOBAL_CACHE_DIR=.zig-cache/global zig build
source-transaction-test -j2` completed with exit zero
(`source-stream-durable-test.log`). The 30 CDC/provider transaction checks and
three lake native row-commit checks passed with zero skips/failures/leaks, as
did the shared source-envelope and effect-plan checks. The runner recovery
fixture models a permanently unavailable derived index, injects a destination
failure in a later transaction, verifies that the failed transaction's checkpoint
was never persisted, and resumes the complete transaction from the prior durable
checkpoint while index waits remain unavailable. Native distributed destination
admission, stable retry identity, and the full retained-history/pgoutput production
integration remain required under the overall issue contracts.

### Replication owner SQLSTATE preservation

The production pgwire error boundary now maps retained-history and changefeed
capability failures to `0A000`, expired/fenced/conflicting history authority to
`55000`, history capacity limits to `53300`, oversized history to `54000`, and
unknown commit/source outcomes to `40003`. History corruption remains `XX000`.
These mappings preserve the existing distinction between replication owner
uncertainty and SQL transaction completion; they do not authorize replay.

Qualification: the full 151-test `pgwire-test` target completed with exit zero
(`source-pgwire-errors-test.log`). Its wire-level regression injects 17 different
owner failures during CREATE_REPLICATION_SLOT, checks the exact ErrorResponse
SQLSTATE and reason, then executes IDENTIFY_SYSTEM on the same connection. It
verifies one dispatch per command, no successful slot result, no SQL execution,
no SQL completion debt, and owner release. The initial sandboxed run passed 143
checks but denied local socket binds for eight listener/CopyBoth checks; the
unchanged target passed when rerun with loopback listener permission. No external
PostgreSQL CDC client was exercised by this test and production logical
replication owner integration remains outstanding.

### Checkpoint-only lake source transactions

The provider-neutral complete-transaction envelope now permits zero row changes
while retaining batch/source/epoch/checkpoint/key-definition validation. Native
lake row normalization validates the schema/key definition and cancellation even
for such a transaction, and skips empty Parquet validation. Catalog preparation
uses the same source-ownership and exact predecessor guards, publishes WAL
coverage/checkpoint atomically through catalog CAS, and carries prior manifests
without adding data/delete files. The catalog may publish a new metadata snapshot;
this does not certify outbound row events or authorize synthetic pgoutput records.

Production filesystem WAL admission/drain now accepts these complete envelopes,
including exact retries after publication. Grouping has a separate 1,024-record
ceiling so checkpoint-only polls cannot bypass the row limit; existing payload
and row bounds still apply. Stable batch identity remains caller-supplied.

Qualification: `source-transaction-test` passed (30 CDC/provider cases, four lake
module/catalog cases, and shared source/effect checks; zero skips/failures/leaks),
recorded in `source-empty-lake-test.log`. The catalog regression covers an initial
empty poll, subsequent real rows, later empty progress, unchanged retained data
manifests, source conflict, stale predecessor, and an invalid key definition.
`lake-integration-test -Dtest-filter='external lake WAL grouping'
-Dtest-filter='external lake filesystem WAL group commit'` passed all three
selected tests, zero skips/failures/leaks (`source-empty-lake-wal-test.log`).
The real filesystem WAL fixture verifies contiguous zero-row admission, group
publication, no additional data/delete manifests, exact original retry LSNs,
and no remaining drain work. The group-bound fixture leaves the 1,025th empty
transaction for the next pass with its predecessor intact. Production PostgreSQL
worker integration with stable durable source identity/checkpoint authority,
native scope fencing and the remaining full issue contracts are still required.

### Bounded native checkpoint receipt windows

Private checkpoint commands now support an immutable positive `receipt_window`
(up to 65,536 slots). The native transaction predicates on the original head
and atomically resolves both the next head and its sequence slot. Slots are
overwritten in a fixed ring rather than accumulating one key per batch. Exact
historical retries inside the window return SourceAlreadyCommitted; altered
input returns SourcePayloadConflict, and older sequences return
SourceReceiptExpired. Missing/corrupt evidence inside the retained window fails
closed. Changing the window on an existing source binding is rejected.

SCP2 heads/receipts retain a domain-separated digest of the entire checkpoint
command, including its exact predecessor and window, with checksummed bounded
encoding. Probe results are owned before another lookup can reuse read scratch.
SCP1 decoding and protocol-37 replay remain unchanged; zero windows are omitted
from canonical legacy JSON. Private receipt keys use the same protected raw
metadata-intent path and checkpoint-participant authorization as the head.

New begin_source_checkpoint_v2 / prepare_source_checkpoint_v2 tags require
protocol 38. Frozen plans derive and revalidate the receipt profile from their
original PREPARE bytes and set the BEGIN marker for every participant, ensuring
activation precedes admission. Tag downgrade or stripped checkpoint fields are
rejected. The engine descriptor is 53, storage-owner ABI 106, API-kernel ABI 27;
the stable failure envelope is unchanged. The OpenAPI command spec and generated
internal type include the bounded optional window.

Qualification:
- Direct checkpoint contracts: two tests passed, covering bounded key count,
  cyclic positions, historical exact retries, altered predecessors, immutable
  windows, expiry, missing evidence, size limits, and omitted legacy JSON fields.
- Direct data-Raft protocol contracts: three tests passed, covering legacy
  layouts and separate protocol-37/38 BEGIN/PREPARE classification.
- Final completion-begin-wire-test / history-control-apply-test /
  api-kernel-abi-test run exited zero: 29 codec, six API-boundary, eight ABI, and
  13 native Raft checks passed with zero skips/failures/leaks. The actual native
  checkpoint fixture restores a receipt-profile frozen plan, activates protocol
  38 before BEGIN, commits a first sequence, rejects conflicting/gapped input,
  aborts a prepared second sequence, and commits a different second attempt.
- make generate, source-catalog-check, engine-errors-check, openapi-check,
  changed-source zig fmt --check and git diff --check passed. Unrelated generated
  model-explorer artifacts were restored.

Evidence: source-checkpoint-receipts-{contract,protocol}-test.log,
source-checkpoint-receipts-native-final-test.log,
source-checkpoint-receipts-generated-check.log and
source-checkpoint-receipts-generate.log.

This is the native receipt storage/activation contract, not a claim of complete
production source recovery. Qualified read-only receipt observation, physical
checkpoint provisioning, native source-scope retirement/migration and bounded
reclamation of retired bindings, source-worker/destination integration, and
completion/GC ownership remain required. A checkpoint receipt does not by itself
prove every participant's completion or authorize source acknowledgment, artifact
reclamation, public event delivery, or index coverage. The full #1698/#1639
requirements remain active; both issues and draft PR #1700 remain open.

### Qualified read-only checkpoint receipt recovery

`POST /internal/v1/groups/{group_id}/tables/{table}/source-checkpoint/observe-v1`
now exposes private checkpoint observations through the actual native owner.
The service-principal handler and signed client enforce bounded request/reply
sizes (16 KiB / 8 KiB), strict JSON, route acknowledgement, content type and
request identity. The routed DataServer port uses only a known eligible leader
and bounded compact peer discovery. HTTP callbacks are local native callbacks,
so forwarding does not recursively route back through the same endpoint.

Native identity, root generation, persisted Raft authority, actual sparse apply
cut, checkpoint head and retained sequence receipt are observed in one storage
snapshot. The serving owner independently captures/revalidates the full catalog
owner, ReadIndex proof, route, namespace, deadline and cancellation before and
after the native call. Reads do not submit BEGIN/PREPARE, propose control entries,
advance checkpoints, or create retention/ACK authority.

Observations distinguish absent (unproven), committed (local checkpoint receipt)
and expired (outside the retained window). They bind the full request and owner;
SCP2 receipts verify the full command digest, including the original predecessor.
SCP1 retains its original source-purpose/current-head semantics rather than
acquiring a historical receipt window. Missing/corrupt in-window evidence fails
closed. A changed window or input is rejected. A checkpoint receipt alone is not
proof of global participant completion and cannot authorize source ACK, replan,
artifact reclamation, public event delivery, or index publication.

Evidence is a fixed owned bounded value internally, encoded as canonical base64
of checksummed SCP records over JSON. Neither read buffers nor parsed JSON are
borrowed after teardown. Common source owner/route admission moved into
`api/source_owner_scope.zig`, shared with source-plan operations. The engine
descriptor is 54, storage-owner ABI 107, API-kernel ABI 28; no new Raft mutation
format or failure-envelope version was introduced.

Qualification:
- Direct storage contracts: all ten tests passed, including binary evidence
  round trips, exact historical receipts, expiry, changed predecessors/windows,
  missing evidence, corrupted records, wrong owner/request, strict JSON and
  unproven absence (`source-checkpoint-read-contract-test.log`).
- `source-checkpoint-read-test source-plan-http-test history-control-apply-test
  api-kernel-abi-test -j2` exited zero (`source-checkpoint-read-native-test.log`).
  Two storage observation tests, two API ingress/discovery checks, nine existing
  source-plan HTTP regressions, eight API ABI checks and thirteen native Raft
  checks passed with zero skips/failures/leaks.
- The real cross-node native fixture reads the retained sequence-1 receipt after
  sequence 2 commits, checks exact head/receipt identity across repeated reads,
  rejects pre-cancelled reads, and reports a future sequence as absent without
  claiming not-admitted authority. The native assertions live in a separate
  test helper rather than expanding the runtime owner's inline fixture.
- make generate, source-catalog-check, engine-errors-check, openapi-check,
  changed Zig formatting and git diff checks passed. Unrelated generated
  model-explorer data was restored.

Physical checkpoint provisioning, native source-scope fencing, qualified
migration/retired-root recovery, production destination adapter/worker wiring,
completion/GC ownership, and all remaining public feed/bootstrap/pgoutput/client
qualification requirements are still outstanding. These private reads are an
input to that integration, not a replacement for it. Both issues remain open
and PR #1700 remains draft.

### Provider-independent seal recovery and confirmed retained admission

`SourceDelivery` can now recover a qualified native seal after its reply was
lost while metadata still records a reserved upload. An adapter's optional
`recoverArtifact` callback observes the original immutable upload without
provider bytes. The actor then commits and reloads the metadata seal using the
same guarded confirmation path as normal upload. Unknown metadata outcomes stop
the pass before admission, including after a positive native seal observation.
An absent/unsealed upload keeps the reservation and charge without admission.

Adapters exposing `admitRetained` receive only the original pending authority
and request context, never provider payload. The delivery path requires a
matching artifact reservation, sealed metadata phase and non-null stamp before
invoking it, then rechecks request activity. Frozen artifacts are mandatory for
that capability at both initial planning and replanning. Recovery resolves the
original destination outcome first; only a capable adapter with confirmed seal
authority can resume its original retained admission. Legacy adapters preserve
their existing behavior. The callback remains responsible for restoring and
verifying the original frozen plan and resolving ambiguous native phases;
absence alone never grants replay, changed-manifest or reclamation authority.

`SourcePlan.Transfer.recoverSeal` supplies the actual read-only bridge: it
validates the reserved journal grant and original native spec, uses the existing
qualified routed observation, rejects fenced/reclaimed input, and returns only
an observed immutable seal. It does not upload or propose control commands.

Qualification: `source-control-test source-plan-http-test
history-control-apply-test -j2` completed with exit zero
(`source-retained-admission-test.log`). All selected suites (13, 12, 9, 12, 3 and
9 tests) passed with zero skips/failures/leaks. The source recovery regression
runs both legacy-artifact and checkpoint-authority profiles with native reply
loss and metadata reply loss. It verifies no admission while the seal remains
unobserved, no admission after an uncertain metadata confirmation, recovery by
a replacement worker without provider rows, one upload/one admission, and quota
release only after acceptance/retirement. The bridge regression verifies seal
observation adds no control calls. The actual cross-node native fixture also
recovers the seal from a still-reserved journal reference before loading and
restoring the original frozen plan. Formatting and diff checks passed.

This closes the orchestration gap between native seal observation and metadata
confirmation. It does not claim that the full distributed source adapter,
checkpoint provisioning/fencing, completion/GC handoff, PostgreSQL worker wiring,
public feeds/bootstrap/retention, production pgoutput or existing-client
qualification are complete. The full issue contracts remain active and both
issues / draft PR #1700 remain open.

### Native source-scope registry reducers and persisted bounded state

Added the internal source-scope basis/spec/reference/metadata-cut schemas and
regenerated their API types. `storage/source_scope.zig` provides the native
registry reducers and NativeKV store operations: immutable physical-root basis,
stable actor/source domain, generation/binding identity, an 88-byte frozen attempt
reference, one active transaction slot, checkpoint-owner sequence admission,
closed attempt-token floors, active/closing/closed phases, and stable first
closure cuts. Coordinator commit is rejected during closure; a follower can
finish a previously authenticated retained positive coordinator decision.

Each root has a fixed 1024-domain budget and a monotonic metadata floor. Qualified
retirement deletes the closed domain and returns its budget charge in the same
transaction as advancing that floor; an earlier active-actor bind cannot recreate
it afterward. Once enabled, the header's strict policy survives deleting the last
domain. Missing records never grant BEGIN. Replacement generations require the
old generation to be closed and a later metadata cut. Existing global checkpoint
heads seed sequence admission when binding the checkpoint authority.

Header and domain records use bounded, checksummed, domain-separated envelopes
with strict JSON decoding and invariant checks. Store operations require the
caller to abort the enclosing transaction on any failure and atomically persist
registry changes with transaction/effect changes. A metadata cut is explicitly
not self-authenticating: control integration must capture qualified actor state
at that cut before invoking these reducers.

Qualification: direct `zig test` of `storage/source_scope.zig` completed with
exit zero (`source-scope-unit-test.log`): six scope regressions and two imported
checkpoint regressions, zero skips/failures/leaks. Tests cover persistent reload,
close/drain/drop, late admission/bind rejection, immutable closure cuts, follower
commit drainage, generation replacement, domain budget, reference round-trip,
and checksum/type corruption. API generation and source-catalog generation
completed; unrelated generated model-explorer data was restored.

This is registry infrastructure, not completed source fencing: the private
control endpoints, independently qualified metadata state/cut capture, legacy
source drain/migration, scoped BEGIN/PREPARE/resolve fields and transaction-record
persistence still need wiring. No protocol/ABI floor was advanced prematurely.
The full outstanding distributed adapter, public feed/bootstrap/retention,
production pgoutput and existing-client qualification requirements remain active.

### Retained source-scope transaction identity

Native transaction records now have a v9 encoding retaining the full fixed
88-byte scope reference alongside the original source purpose. Legacy v6/v7/v8
encodings remain unchanged. Scoped records require retained terminal decisions;
strict decoding rejects invalid reference fields, flags and unretained records.
BEGIN retries and intent writes require the exact retained reference. Resolution
requires it even on terminal retries, preventing a stale or ordinary worker from
adopting a scoped transaction ID. The scoped first-materialization observation
checks the same identity, and paged transaction summaries preserve it for
recovery. The existing unscoped observation remains a compatibility wrapper.

Qualification: `zig build docstore-history-test -j2` completed with exit zero
(`source-scoped-record-test.log`), including all 14 native history/transaction
regressions and the committed-frame suite. The new real-store regression reopens
a prepared scoped transaction, verifies retained reference bytes, rejects altered
tokens/generations and omitted references on begin/prepare/resolve, then verifies
that abort retains the original reference. Malformed v9 flags, zero identity and
missing retention are rejected. Formatting and diff checks passed.

This installs retained identity only. Production commands still use the existing
wire profile; registry slot mutation and physical-root qualification must join
the same atomic native write batch before scoped commands are enabled. No new
Raft/API ABI profile is enabled by this record-only change.

### Atomic native scope admission, prepare and resolution

The native transaction write batch now enforces registry state for scoped v9
records. Before staging replacement transaction/effect bytes it loads the exact
retained completion admission, verifies v3 original owners and the checkpoint
coordinator role, derives the local/coordinator physical bases and immutable slot
identity, and enters or resolves that slot in the same NativeKV batch. First
PREPARE checks the active slot before making the vote/intents durable; an already
prepared exact retry keeps its first materialized images. Resolution requires an
explicit actual apply cut before releasing the slot. A closing checkpoint
coordinator cannot commit, while abort can atomically drain its original slot.

Fresh legacy source-purpose BEGINs now consult the root's persisted strict policy
and are rejected after scoped admission is enabled. Retained legacy transactions
keep their original retry behavior; enabling the policy still requires the
planned legacy-worker drain/migration. Ordinary application transactions are
unaffected. Terminal scoped retries do not recreate or touch a reclaimed slot.

Qualification: final `zig build --build-file zig/build.zig docstore-history-test
-j2` completed with exit zero (`source-scope-atomic-final-test.log`), all 14 native
regressions passing with zero skips/failures/leaks, plus the committed-frame
build target dependency. The real-store scoped fixture now uses v3 retained
owners, a persisted source registry and history reservation. It verifies failed
unbound/legacy BEGIN leaves no transaction or admission record, retained identity
across reopen, closing coordinator commit rejection with pending record/intents
preserved, missing-cut abort rejection, atomic slot drainage on abort, stable
closure cut/token floor, and closure between BEGIN and first PREPARE leaving no
vote or intents. Formatting and diff checks passed. Two intermediate test-only
compile errors (allocator argument and concrete read-transaction method) were
corrected before this final passing run.

This closes the native batch crash window for registry transitions. Scoped
commands are not enabled yet: private wire tags/activation floors, API/opaque
owner plumbing, independently qualified metadata actor/cut capture, control
endpoints and managed legacy migration remain required. Production source adapter
and the remaining full issue contracts remain unfinished and active.

### Required recovery edge: scoped ABORT before original BEGIN

The existing not-enlisted ABORT no-op is insufficient for scoped attempts. A
participant that has a bound domain but has not installed this attempt's BEGIN
can still receive that delayed BEGIN after the coordinator records ABORT and
private transaction metadata is collected. Its local attempt floor must advance
before the original participant can ACK retirement; absence alone must never
supply a positive abort or not-admitted proof. The original frozen admission,
qualified positive coordinator outcome and original physical owner must authorize
that floor transition, without clearing a newer slot. If the coordinator itself
has never admitted BEGIN, recovery needs an explicitly qualified source-attempt
abort/admission retirement transition rather than assuming absence is ABORT.
Scope control/metadata retirement and the production source adapter must cover
these cases before protocol-39 source admission is enabled for production.

### Routed scoped transaction profile and retained decision identity

Added private `begin_source_scope_v1`, `prepare_source_scope_v1` and
`resolve_source_scope_v1` tags carrying the exact retained scope reference.
Stripping that reference or substituting an older phase is rejected. Scoped BEGIN
requires the checkpoint/receipt profile; the checkpoint PREPARE binds the same
binding and sequence and requires bounded receipts, while row-only participants
carry the reference without inventing a checkpoint command. Public batch parsing
rejects all of these private phases.

Frozen plan construction requires every original participant to carry the same
reference and makes the unique checkpoint participant the coordinator. The
reference is authenticated in the canonical original PREPARE bytes and restored
from the retained artifact without provider rows. BEGIN and terminal propagation
preserve it. The qualified decision-read record now returns the retained
reference (omitted for legacy records), so replacement-worker resolution also
uses the original attempt identity rather than an unscoped command.

Native BEGIN/first-materialization/PREPARE dispatch carries the reference into
v9 transaction records and atomic registry transitions. Ordered resolution
threads it through abort, terminal and materialized commit paths, deriving the
slot-release cut from the actual ordered Raft receipt. Legacy method signatures
retain compatibility wrappers. The source attempt profile requires data-Raft 39
for each of BEGIN/PREPARE/resolve; the pre-existing version-12 restore/source-pin
constant retains its original meaning. Engine descriptor is 55, opaque storage
owner ABI 108 and API-kernel ABI 29 for the changed native layouts/contracts.

Qualification: `completion-frozen-plan-test completion-begin-wire-test
docstore-history-test history-control-apply-test -j2` completed with exit zero
(`source-attempt-native-final-test.log`): suites of 3, 30, 6, 14 and 13 tests,
zero skips/failures/leaks. The linked real-Raft/ReadIndex and signed cross-node
legacy source fixture passed across the new native ABI boundaries. New codec
cases cover checkpoint and row-only PREPARE, all three phase round-trips,
downgrade/stripping/public-parser rejection and mismatched checkpoint identity.
The frozen-plan case covers providerless restore, mixed attempts and incorrect
checkpoint coordinator rejection. Native v9 recovery verifies that decision-read
extraction retains its scope reference. Four direct protocol tests passed,
including protocol-39 classification for every scoped phase. Source catalog,
engine-error, OpenAPI, formatting and diff checks passed. API regeneration
completed and unrelated generated model-explorer data was restored. An initial
sandboxed native run passed 12/13 Raft tests but denied the real loopback bind;
the unchanged cached fixture passed all 13 after enabling local TCP access.
Two new frozen-fixture call signature errors were corrected before the final
passing run.

This qualifies command/retained-identity plumbing and preserves existing native
behavior. It does not qualify production scoped admission: physical registry
control endpoints, independent metadata actor/cut capture, managed legacy drain,
positive coordinator-authorized ABORT-before-BEGIN floors and explicit source
attempt retirement still need implementation and cross-node race tests. The
full distributed source adapter and the public feeds/bootstrap/retention,
production pgoutput and existing-client qualification contracts remain active.

### Bounded coordinator-authorized retirement before local BEGIN

Added `storage/source_scope_retirement.zig` and the internal abort
intent/command/local-receipt schemas. The intent freezes the original local
owner, coordinator query, scope reference, source purpose and admission digest.
The ordered command requires a positive original scoped coordinator ABORT with
resolved intents and verifies the original v3 participant membership and
checkpoint coordinator. Absent/pending/committed coordinator observations and
changed identities never grant retirement. Command validation is structural;
its observation must originate in an independent qualified ReadIndex capture at
the trusted proposal boundary, not an HTTP caller's asserted evidence.

For a root that never installed BEGIN, the native operation atomically advances
one cumulative token floor in its existing charged scope record. It creates no
per-attempt transaction outcome or tombstone, does not infer an absent
coordinator's abort, preserves a newer active slot, and refuses to skip an older
unresolved slot. That floor rejects delayed BEGIN even after reopen. A local
record that does exist must use intent drainage rather than this absent-record
path. Native `TxnManager.retireScopedSourceAttempt` handles both cases, committing
the caller's ordered apply marker with the floor or with local abort/intent
retirement. Its receipt is explicitly local fencing, not global completion or
all-owner ACK. Reply decoding binds both the intent and the exact observed
coordinator record digest.

Completion followers retain the original coordinator locator as their one
persisted recovery obligation. Qualified v3 local resolution now normalizes the
original local participant acknowledgement to that retained obligation after
checking original owner membership and physical namespace. This preserves old
participant-list retry identity and permits local drainage/acknowledgement
without rewriting existing sidecars. Legacy v1 behavior remains unchanged.

Ordinary scoped ABORT with no local transaction now returns unresolved rather
than a successful not-enlisted no-op. A worker cannot acknowledge retirement
until the dedicated positive fencing path completes. Registry operations also
reject records whose charged strict header is missing or inconsistent, including
partial restore; an orphan record cannot silently enable uncharged scope work.

Qualification: direct model tests completed with exit zero
(`source-abort-retirement-unit-test.log`): 22 tests, including six new retirement
regressions and imported scope/checkpoint/decision contracts. Final native
`docstore-history-test -j2` completed with exit zero
(`source-abort-retirement-native-test.log`): all 15 native regressions passed,
zero skips/failures/leaks. The new separate native fixture verifies atomic marker
and floor persistence without a transaction tombstone, reopen and delayed-BEGIN
rejection, a later legitimate attempt, real prepared-intent drainage, and the
qualified follower acknowledgement. The store fixture supplies a synthetic
coordinator observation to isolate reducers; it does not stand in for production
ReadIndex provenance. Source-catalog, engine-error, OpenAPI, formatting and diff
checks passed. API generation completed; a new schema reference and test helper
sentinel-path signature were corrected before final checks, and unrelated
model-explorer generation was restored.

Still required: independently qualified coordinator capture, physical-root
control/native outcome plumbing, routed retirement endpoints, worker/ACK
integration and activation that excludes members retaining the old no-op
behavior. Existing protocol-39 entries must retain replay compatibility; the
new scope-control binding profile must establish the stronger capability before
production admission. The coordinator-before-BEGIN case additionally needs
qualified scope-closure/admission retirement, rather than fabricating ABORT from
absence. No issue is complete and the full goal remains active.

### Durable source closing intent and lifetime scope-fencing debt

Metadata protocol 104 adds `enable_scope_fencing` and `close` source mutations.
Enabling requires a bound native checkpoint authority and drained pending,
outstanding and artifact work. The enabled mode remains charged as source
lifecycle work even while idle, preventing table deletion from bypassing native
scope closure. It is idempotent and requires a scope-capable delivery adapter.

Closing persists independently of retirement. It stops fresh reservations,
replacement plans and new checkpoint-authority binding while preserving exact
pending-grant confirmation, original positive outcome settlement and artifact
cleanup. The closing state remains work until retirement; an enabled scoped
actor must close before retirement, and its authority remains charged until the
checkpoint-fence transition records completion. Reseeding clears the old mode
only after its existing retirement gates succeed. False flags are omitted so
legacy encoded state and the old native/lake contracts remain compatible.

Delivery recovery still observes original outcomes and may confirm a previously
sealed artifact, but known closing state never starts retained admission.
Delivery and replanning reject fresh work before planning when closing is known.
Metadata apply accepts closure as a drain operation after configuration changes,
whereas enabling scoped mode still requires the live source definition. Both new
operations use the coordinated decoder-104 barrier; malformed/unknown source
commands conservatively require that maximum version rather than the older
checkpoint-only floor. Existing valid legacy and artifact/checkpoint operations
retain their previous floors.

Qualification: final `source-control-test -j2` completed with exit zero
(`source-closing-metadata-final-test.log`). Selected source model/delivery tests,
service decoder classification and all 10 metadata storage/discovery regressions
passed with zero reported skips/failures/leaks. The new source model tests cover
closing/reopen, retained exact reservation retry, new-grant/replan rejection,
positive original settlement, idle mode debt and ordered retirement. The delivery
regression covers sealed pending authority with no fresh admission/planning.
The separate real-store fixture verifies idle debt across reopen, closing flags
and work counters across snapshot installation, blocked table deletion, and debt
retention until the recorded checkpoint-fence step. Its native fence proof is a
synthetic model input, not production coordinator provenance. Source-catalog,
engine-error, OpenAPI, formatting and diff checks passed. Intermediate apply
switch and test helper import/snapshot-interface errors were corrected before the
final passing run.

This supplies the metadata lifecycle prerequisite for actual scope controls.
Independently qualified metadata state/cut capture, physical bind/close/drop and
retirement endpoints, the stronger data-Raft activation profile, real native
closure proofs and worker/ACK integration remain required. No native ABI shape
was changed or data-scope capability advertised by this metadata-only step.
The full issue contracts remain active and neither issue / draft PR is complete.

### Applied metadata source-scope capture and absent-scope fencing

`RaftApplyStore.captureSourceScope` now reads the actual durable applied index,
metadata incarnation, activated decoder record, actor table definition and
source-control state from one native read transaction. It requires the caller's
minimum applied index, exact incarnation, valid decoder-104 activation and an
enabled native scope-fencing actor with its original checkpoint authority.
The resulting owned capture derives the immutable native scope spec from stored
state, records active/closing/retired phase and actor revision, copies the
checkpoint table name, and retains pending token/sequence, transaction, purpose,
reservation and distinct delivery/artifact commitments. No parsed state or read
transaction buffer escapes the capture.

Binding additionally requires an active actor and the live source definition;
closing and retirement expose separate gates. `source_scope_capture.readOnService`
establishes the service's qualified linearizable read before accessing the
projected store, rejects metadata-group mismatches before reading, and withholds
and frees the owned capture on cancellation. Captured facts/cuts are explicitly
not caller-manufactured authority or a substitute for the service's ReadIndex
and native control-owner qualification.

Native `Store.closeAbsent` covers a scope whose earlier bind has not arrived.
After independently qualified closing/retired metadata capture, it advances the
root's monotonic metadata floor and strict policy without creating a domain
record or consuming a domain charge. Earlier active-actor bind captures cannot
recreate the absent scope. An existing domain takes its normal close/drain path;
this operation cannot erase it or overwrite a newer entry.

Qualification: final `source-control-test -j2` completed with exit zero
(`source-scope-capture-final-test.log`), including all 11 metadata storage and
discovery cases, the source model/delivery cases and decoder service checks.
The new separate fixture projects actual committed metadata entries, verifies
captured cuts and pending identities, rejects missing activation/future cuts/
wrong incarnation/source, observes closing/retired gates, installs a snapshot
and confirms the same spec, cut and phase. A service wrapper fixture verifies
read-before-capture ordering, pre-read group/cancellation rejection and cleanup
on cancellation after capture. Nine direct scope/checkpoint tests passed
(`source-scope-absent-close-test.log`), including absence fencing, no domain
allocation, queued old-bind rejection, preserving existing domains and monotonic
floor/idempotence. Source-catalog, engine-error, OpenAPI, formatting and diff
checks passed with zero reported skips/failures/leaks.

This closes the same-snapshot metadata provenance gap below the service boundary.
Private transport/routing, physical ordered scope control and native observation,
legacy-source migration qualification, stronger data activation, source worker
integration and coordinator-before-BEGIN closure retirement remain outstanding.
The full issue goals and draft PR remain active and unfinished.

### Qualified lifecycle command construction and bounded scope control reducer

- Added private scope control Intent/Command/Query/Observation contracts, including
  exact original routed owner, immutable actor binding, minimum metadata cut,
  bind/close/drop action, actual local apply cut, bounded header and current slot.
  Internal OpenAPI schemas and generated types describe these shapes without
  advertising an unimplemented endpoint or a new data decoder floor.
- `source_scope_capture.authorizeOnService` independently runs the qualified
  metadata service read, requires the exact stored binding and correct lifecycle
  phase, and puts the actual captured metadata cut in the private command.
  An old minimum index cannot authorize binding an already retired actor. The
  command body is still not self-authenticating metadata evidence.
- The native reducer requires an independently supplied legacy-drain gate before
  first enabling strict admission, including absent-scope closure. No request
  boolean or metadata `pending == null` substitutes for a real worker handover.
  There is deliberately no production gate that asserts drainage unconditionally.
- Closing an existing scope now persists the root metadata floor in the same
  enclosing transaction as the record, matching absent-scope closure. Delayed
  captures cannot allocate another domain below that floor. Cross-metadata-group
  and cross-incarnation checkpoint owners are rejected during binding.
- Drop requires the exact stored Spec and first actual close cut, releases one
  domain charge, and retains strict admission plus the root metadata floor.
  Observation absence grants no outcome/replay/global fencing authority.
- Header and Record invariants are shared by durable decoding and wire observation
  validation; malformed phase/cut/slot combinations cannot pass the wire validator.

Validation: direct scope control tests passed all 18 tests; these tests are also
now included in `zig build source-control-test`. The final combined target exited
0, including real committed metadata projection, snapshot installation, fresh-cut
command authorization, wrong binding/future-cut refusal, cancellation and service
ReadIndex ordering tests (metadata storage 11, service 3 and discovery suites 12,
all zero failed/skipped/leaked). `make generate` completed with the pinned network
accessible toolchain after the sandboxed attempt could not resolve npm DNS.
`source-catalog-check`, `engine-errors-check`, and `openapi-check` exited 0; the
schema fragment tests passed all nine cases. No native ABI or data-Raft protocol
maximum was changed by this reducer/authorization milestone.

Still outstanding: genuine first-bind legacy-worker handover/drain certification,
actual native owner/ordered apply/RPC wiring and durable bounded control receipts,
qualified native observation and coordinator-retirement transport, historical root
cohort registration and lifecycle coverage, destination provisioning and migration,
all-owner fencing/ACK/publication/credit cleanup, and the remaining full-scope
public feeds/bootstrap/retention/pgoutput/existing-client requirements above. The
unit reducer gate is a fixture, not production migration qualification. Issues
#1698/#1639 remain open and PR #1700 remains draft.

### Bounded native scope-control ordered recovery contract

`storage/source_scope_ordered.zig` adds one checksummed latest-control receipt per
physical root. It binds the exact typed command (including captured metadata cut),
original immutable root, actual Raft term/index, and applied observation or narrow
deterministic rejection. Applied results validate the requested bind/close/drop
postcondition and actual cut. Rejection classification deliberately excludes
corrupt storage and unexpected/transient failures. Recovery requests require a
minimum applied index covering the original entry; the eventual serving adapter
must independently establish ReadIndex and physical-owner authority.

Absent and superseded receipts remain explicit unknown evidence. They authorize
neither blind mutation replay nor metadata retirement. A newer control replaces
one fixed receipt key; no per-entry or per-generation journal accumulates. Scope
mutation/rejection, receipt, and ordinary apply marker must be one native commit;
this module does not itself supply the native DB transaction or Raft transport.

Validation: direct ordered-scope tests passed all 20 tests. Cases include exact
rejection recovery, changed-command and changed-term refusal, supersession with a
single retained key, malformed/corrupt receipt refusal, exact success/actual-cut
binding, impossible success postcondition refusal, and cross-root generation
refusal. The final `source-control-test` exited 0 after including these tests;
metadata storage 11 and service 3 plus discovery suites all passed. Internal
OpenAPI generation and `source-catalog-check engine-errors-check openapi-check`
completed successfully, including nine schema-fragment tests. An initial fixture
parameter shadowing error was corrected before the final green run. Native owner,
ordered apply, RPC integration and actual legacy-drain qualification remain the
next required integration work; this is not a completed production feature.

### Native scope observation across the compiled owner boundary

The native DB, compiled storage-engine VTable, opaque storage-owner C ABI and
client now expose `sourceScopeObservation`. The native owner validates physical
namespace/generation and durable Raft authority, then reads actual applied cut,
registry header and requested domain record from one read transaction. The routed
owner wrapper establishes/revalidates leader ReadIndex and complete catalog owner
before and after that read. Engine descriptor is now 56 and storage-owner ABI 109;
API kernel ABI remains 29 and the data decoder maximum remains 39. This read does
not advertise a new mutating scope envelope, a legacy drain gate or standalone
native sequence semantics.

Validation: the final recovered `history-control-apply-test` exited 0, all 13 tests
passed with zero skips/failures/leaks (`source-scope-owner-native-recovered-test.log`).
The real native/Raft fixture observes absence on a legacy root without enabling
strict mode, repeats that qualified read, rejects a changed original root,
withholds a requested future cut and honors cancellation. A bounded fixture retry
handles only transient `ReadUnavailable` from the real ReadIndex try-lock; future
cut refusal is still checked as `StorageReadTemporarilyUnavailable`. The initial
fixture incorrectly selected the forwarding node's nonexistent storage owner;
that was corrected to pass the actual native owner explicitly. An intervening
build lost cached libc/object files; a normal regenerated build restored them
and passed. Existing source-catalog, engine-error and OpenAPI contract checks had
already exited 0 for the native changes.

Still required: actual bound/closing scope observations through ordered controls,
private HTTP observation transport (an unreferenced ingress adapter draft is not
that integration), qualified metadata lifecycle transport and real first-bind
worker handover, original-coordinator retirement transport, all root lifecycle
coverage and the rest of the full issue scope. The above is native read
qualification, not complete destination or changefeed qualification.

### Pinned existing-client runner and independently verified PostgreSQL baseline

Added `zig/e2e/antfly/clients/debezium`: Debezium 3.3.0.Final's unchanged PostgreSQL
connector and pgoutput decoder, a small engine lifecycle/JSONL runner, digest-pinned
Maven/JRE images, and a generic SQL-driven qualification driver. There is no Antfly
connector, SQL/catalog shim or decoder adapter. The driver verifies the connector's
reported runtime version and connector identity, keeps the stock connector's offset
file across restarts, and supports an external endpoint as well as its disposable
PostgreSQL baseline. Baseline PostgreSQL 17.6 is also pinned by image digest. Only
uniquely named test containers/SQL objects are created and removed; the driver uses
an existing Docker network and exposes no baseline host ports. Credential-bearing
retained artifacts are opt-in, permission-restricted and gitignored.

Validation: the digest-pinned image builds successfully. The final baseline run
`debezium-baseline-version-verified-test.log` exited 0 and observed 19 events through
an initial snapshot, exact numeric(38,8) byte representation, boolean/before-image
checks, a two-row transaction with stable source txId/order and END count, primary
key delete/tombstone/new-key records, a graceful reconnect after offline writes,
and a client SIGKILL/reconnect with additional offline writes. Neither reconnect
repeated the initial snapshot. The runtime event metadata reports PostgreSQL
connector version 3.3.0.Final. Initial fixture assumptions were corrected from
cross-row Debezium transaction-string equality to actual stable source.txId and
transaction order/count evidence; PostgreSQL row LSNs differ within a transaction.
A Docker address-pool exhaustion was avoided without pruning other resources by
using the existing bridge and the disposable server's actual container address.
Python syntax checks pass. No test artifacts or credential files are committed.

This proves the harness runs a real pinned existing client. It does NOT qualify
Antfly. The same client still must pass against Antfly's actual public factory,
bootstrap/exported snapshot, slots and bounded retention, typed pgoutput, schema
history, publication, privacy, reconnect, root lifecycle, server/node/HA failures
and inbound-to-Native/Lake-to-outbound pipelines. Those remain mandatory scope;
the PostgreSQL baseline is an independent control, not a replacement for them.

### Authenticated native scope observation RPC

The private `/groups/{group_id}/tables/{table_name}/source-scope/observe-v1` path now
runs through a typed callback port, internal-service authentication, immutable
catalog route qualification, bounded HTTP client/handler, eligible-leader routing,
and the existing native ReadIndex wrapper. Request and reply limits are 8 KiB;
strict reply decoding validates original owner/domain/minimum cut and all durable
registry invariants. Missing route acknowledgement, wrong media type, an outcome
ambiguity marker, corrupt or oversized reply and lost transport all fail without
producing an observation. Reads have no mutation retry/replay authority.

API kernel ABI is now 30 because its configuration includes the new callback;
engine descriptor 56, storage-owner ABI 109 and data decoder maximum 39 are
unchanged. Production data server configuration installs the port only for the
supported linked Raft profile. The internal OpenAPI path/server are regenerated.

Validation: `history-control-apply-test` exited 0, all 16 tests passed with zero
skips/failures/leaks (`source-scope-rpc-native-test.log`). The native fixture now
reads the scope from its forwarding node through the real signed HTTP service and
native owner, repeats it, and tests cancellation. The directly qualified native
read still checks wrong-root and future-cut refusal. Dedicated executed tests
`source scope ingress qualifies owner routes and rejects forged or cancelled
observations` and `source scope HTTP client rejects unqualified forged expired
and ambiguous read replies` cover admission and response failure boundaries and
one transport attempt. Source-catalog, engine-error and OpenAPI checks also exited
0, including all nine schema fragment tests and generated provenance checks.

The metadata lifecycle wire draft remains unreferenced/unverified and is not part
of this RPC milestone. Fresh qualified metadata capture transport, actual mutating
scope controls and original-coordinator retirement RPCs, genuine legacy-worker
handover and all remaining end-to-end issue requirements are still required.

### Bounded metadata lifecycle capture wire contract

`metadata/source_scope_wire.zig` encodes/decodes the independently qualified
capture already implemented in the metadata store/service wrapper. The request
fingerprint binds version, metadata group/incarnation, actor table/source and
minimum applied index. Replies validate that correlation, actual cut, actor
revision, exact source identity and original checkpoint owner basis, bounded
checkpoint table name and every retained pending reference/digest. A retired
actor cannot retain an active pending identity. The two distinct plan digests are
preserved; no input manifest or transaction outcome is reconstructed from them.
Request and reply bounds are 2 KiB and 8 KiB respectively. Decoding copies the only
variable-length field into an independently owned Snapshot, preserving the local
capture ownership/deinit contract after response/parser buffers are released.

Validation: `source-control-test` and the focused
`source-scope-metadata-wire-test --summary all` both exited 0; the focused build
completed all 55 steps. Its two declared regression cases cover strict request
roundtrip/duplicate refusal, decoded-buffer ownership, altered query/cut/source
refusal and pending-generation/retired-phase consistency. The combined target
also passed existing metadata storage/service/capture tests. Source-catalog,
engine-error and OpenAPI checks exited 0, including nine schema-fragment tests.
No native ABI, data decoder or metadata durability version changes are introduced
by this bounded DTO. The actual authenticated metadata endpoint/client and its
fresh service ReadIndex remain next integration work; these bytes do not grant
binding/retirement authority on their own. All full issue requirements remain.

### Fresh authenticated metadata source lifecycle RPC

`/internal/v1/catalog/source-scope-capture-v1` now serves the bounded capture
contract through `api/source_scope_metadata.Port.fromService`. Both production
metadata-service constructors install it. The port requires an internal maintenance
principal, invokes the actual service ReadIndex capture, validates its snapshot and
withholds cancellation. The HTTP handler requires verified service credentials even
in migration mode plus a shared routing budget, checks metadata identity before and
after capture, and returns only a bounded correlated reply with an explicit capture
acknowledgement. The client requires service credentials, shares one five-second
budget across bounded read retries, validates one acknowledgement, media type,
identity/cut correlation and absence of mutation markers, and owns its decoded
Snapshot independently of response buffers. Read transport loss returns unavailable,
not a mutation outcome or substitute authority.

The private endpoint and request/query/pending/snapshot/reply shapes are declared
in internal OpenAPI under its existing `/internal/v1` base. `make generate` completed;
only the owned unrelated model-explorer outputs were restored. No native ABI or
durable protocol version changes: engine 56, storage owner 109, API kernel 30,
metadata decoder 104 and data decoder maximum 39 remain current.

Validation: the final combined `source-control-test
antfly-metadata-sql-sessions-test -j2` exited 0 after code generation
(`source-scope-metadata-rpc-qualified-test.log`). Service tests passed 5/5, including
new client/server rejection cases; actual metadata storage/capture regressions
passed 11/11; real metadata-server tests passed 2/2, all zero skips/failures/leaks.
The real host fixture rejects a wrongly signed token with 401, creates a source
actor through actual Raft/service protocol admission, and uses the signed HTTP
client to observe active, closing and retired states after their corresponding
commits. Each subsequent requested minimum covers the prior capture, and observed
cuts increase. Metadata retirement still leaves physical-fencing work outstanding.
The declared checkpoint-owner basis in this metadata fixture is not a physical data
root qualification. Unit cases additionally cover unsigned requests, identity
changes during capture, mismatched queries, stale cuts, missing/duplicate ACKs,
wrong media type, mutation-marker confusion and exhausted read transport retries.
The actual applied-store fixture also exercises the service port and owned wire
roundtrip. Source-catalog, engine-error and OpenAPI checks exited 0, including all
nine schema fragment tests. Fixture retirement shape and retention predicate compile
errors were corrected before the final green runs.

Still required: consuming this fresh authority in actual ordered native bind/close/
drop/retirement controls, real first-bind legacy-worker handover, complete cohort
root lifecycle coverage and destination integration, plus the original retained
history/public feed/bootstrap/retention/pgoutput and Antfly existing-client gates.
This milestone neither proves physical fencing nor finishes either issue.

### Fresh remote command construction and repeated-binding floor regression

`MetadataHttpClient.authorizeSourceScopeIntent` now obtains its own authenticated
capture, checks the exact source binding and requested active/closing/retired
phase, and constructs the private command with the actual observed metadata cut.
It accepts no caller snapshot. Native physical-owner qualification and genuine
legacy drainage remain independent required checks; metadata authorization also
cannot manufacture a valid physical close cut for drop.

The real metadata Raft/HTTP lifecycle fixture now exercises this constructor:
active bind succeeds, altered binding and active close fail, closing bind fails,
closing close succeeds and retired drop receives only metadata authorization.
`source-control-test antfly-metadata-sql-sessions-test` exited 0 for these changes
(`source-scope-remote-authorize-test.log`), including the actual host/server tests.
Contract checks also passed.

While preparing native integration, a regression test reproduced an existing
Header.bind bug: a repeated active binding with a newer qualified metadata cut
left metadata_floor at 10 when 11 was required. This would also make the bounded
ordered success receipt reject that otherwise valid refresh. The same-spec active
branch now advances the header floor before returning the original record. It
preserves the first binding cut, pending slot, sequence/token state and domain
charge. The regression proves stale allocation is fenced and the refreshed result
can be staged as an exact ordered receipt. It failed before the fix and all 21
standalone tests passed afterward (`source-scope-rebind-green-test.log`). The test
is included in source-control-test, whose final run exited 0; source-catalog,
engine-error and OpenAPI checks also exited 0. No ABI or protocol maximum changes
are introduced by this invariant fix.

Actual native ordered controls/retirement RPC, genuine worker migration and the
remaining full destination/changefeed/bootstrap/retention/pgoutput/client scope
are still mandatory and incomplete. Neither these checks nor the PostgreSQL
control baseline constitute full Antfly end-to-end qualification.

### Atomic bounded native attempt-retirement success receipts

`source_scope_retirement_ordered.zig` adds one bounded, checksummed successful
retirement receipt per physical root (8 KiB maximum). The receipt binds the
original qualified coordinator ABORT record digest, complete retirement intent,
actual local ordered cut, cumulative token floor and original reply disposition.
An exact recovery returns the stored reply without consulting newer live scope
state. A replaced receipt returns superseded; mismatched term/intent/proof at the
same index conflicts. Missing/superseded receipts never infer a transaction
outcome. Corruption and oversize remain errors.

TxnManager now stages this receipt in the same writer as unenlisted fencing, or
as enlisted ABORT resolution, scope-slot completion, participant acknowledgement
and caller-supplied ordered marker writes. Both terminal retry branches use that
same writer, including empty intent cleanup and no-marker already-resolved
retries. The private typed receipt input validates transaction identity, ABORT,
source reference/purpose, owner and actual cut before resolution. No general row
commit participant callback is used to certify control-only or abort effects.
The successful reply is read from the durable receipt after commit instead of
being reconstructed from a later live scope read.

The real NativeKV retirement fixture covers failed receipt validation rolling
back a staged token floor and marker, exact response recovery after reopen,
wrong-term and altered-intent conflicts, enlisted intent cleanup, supersession,
no-marker terminal retry, another reopen and an ABORT whose empty intent cleanup
was not acknowledged. Further checks reject corrupt and oversized receipts.
The combined docstore-history-test/source-control-test run exited 0 in
`source-scope-retirement-receipt-qualified-test.log`; the native history suite
reported 15 passed, zero skipped/failed/leaked. Final corruption checks and
repository contract check results are recorded below after their terminal runs.

This is a trusted low-level native transaction implementation, not exposure of
retirement through DB/engine/opaque-owner/data-Raft/RPC. The fixture's namespace
and source authority are established directly and do not prove production
physical-owner qualification or metadata/coordinator-read provenance. Rejected
ordered retirement results, genuine legacy worker migration, actual controls and
retirement RPC, hidden checkpoint provisioning and cohort/root lifecycle remain
required. Full destination integration, retained/public history, bootstrap and
bounded retention, typed production pgoutput, actual Antfly Debezium qualification
and the original issues' recovery/performance acceptance checks remain open.

Final `docstore-history-test` exited 0, all 15 tests passed with no skips,
failures or leaks (`source-scope-retirement-receipt-final-native-test.log`).
`make source-catalog-check engine-errors-check openapi-check` exited 0
(`source-scope-retirement-receipt-contract-check.log`), including nine schema
fragment tests. No engine/owner/API ABI or data/metadata protocol maximum is
changed by this internal native receipt implementation.

### Physical ordered source-scope DB controls and recovery

The DB now exposes trusted `applyOrderedSourceScopeControl` and
`orderedSourceScopeOutcome`, with compiled-engine descriptor 57. Ordered apply
holds the physical apply fence, snapshot mutation admission and replication
write gate; verifies namespace/root generation and durable Raft authority; and
stages the scope transition, bounded outcome and actual entry marker in one
native writer. Deterministic rejection aborts the reducer's partial writes before
committing its narrow rejection and marker separately. Replay binds the exact
entry and command. Recovery verifies the actual durable applied cut and refuses
future cuts, mismatched terms and physical roots.

First strict activation deliberately remains blocked by SourcePendingWork.
The current production worker can emit ordinary unlabelled commitBatch writes;
an empty source registry or absent retained source transaction is therefore not
a legacy-drain proof. The DB gate cannot open until genuine persisted worker
handover is integrated. Already strict roots use their durable registry without
re-running migration authorization. This is an unfinished migration contract,
not evidence that a production actor can now activate scoped delivery.

A real NativeKV DB regression reopens the root across first-activation rejection,
sets up a strict scope explicitly as a fixture, then exercises repeated binding,
close, exact replay, rejection of a wrong physical close cut, drop, stale bind
rejection, superseded recovery, future read cuts, wrong term and changed root.
The first run completed its behavioral checks but failed leak detection: seven
encoded scope header/record buffers were not released. Scope writers now free
those temporary encodings after the writer copies them, including failed puts.
The dedicated native target and existing source-control suite are being rerun;
terminal evidence follows below. Repository source-catalog, engine-error and
OpenAPI checks passed.

Serving-owner/metadata qualification, all-member data decoder activation and
opaque/Raft/RPC exposure are independent integration requirements. No stronger
data protocol is advertised by these DB methods. Retirement DB exposure and
durable rejection outcomes, genuine migration, hidden checkpoint and cohort
lifecycle, and the complete original destination/public-history/bootstrap/
retention/pgoutput/real-Antfly-client and recovery/performance scope remain open.

The final dedicated DB control regression passed, zero skipped/failed/leaked,
and source-control-test exited 0
(`source-scope-native-ordered-control-green-test.log`). The regression is also
selected by the default data-storage test profile. Contract checks exited 0
(`source-scope-native-ordered-control-contract-check.log`). A real linked native
owner/routed history integration run is in progress to qualify descriptor 57;
its terminal result is recorded below. No owner/API ABI or data/metadata protocol
maximum has been changed.

Issues #1698 and #1639 were re-read from GitHub during this milestone. Both remain
open, with all original acceptance criteria intact, including complete native
and writable-Iceberg destination integration, public retained feeds, coherent
bootstrap/retention and actual pinned-client Antfly pgoutput qualification.
The current native scope controls do not establish those broader requirements.

A second recovery regression reproduced absent-drop refresh failure: after drop
at metadata cut 12, the same retired actor freshly captured at cut 13 returned
SourceCheckpointConflict. An absent strict-root drop now advances the common
metadata floor when necessary, preserving strict mode, other domains and bounded
storage without a per-source tombstone. The first standalone run failed exactly
at that refreshed drop; the fixed run passed all 22 tests
(`source-scope-refreshed-drop-{red,green}-test.log`). The real DB fixture now
includes repeated drop with a newer metadata cut and checks that floor after
reopen. This does not authorize first strict activation on a legacy root.

The first broad linked integration compile failed with LLVM's No space left on
device while emitting its object. Dedicated DB/control tests and the shared
15-test history suite had passed. Normal repository GC completed but reclaimed
no significant space; recent/active caches were protected. The same linked test
selection is being retried with -Dstrip=true and -j1 to omit debug information
and bound concurrent build output, without changing protocol/runtime assertions
or test selection. A lower 5 GiB disposable-cache threshold is also being run
through the repository collector with its active-process/recent-write guards
unchanged. Final results follow after terminal observation.

The final `source-scope-native-control-test source-control-test
 docstore-history-test -j1` invocation exited 0
(`source-scope-native-control-final-regression-test.log`), including the native
refreshed-drop/reopen checks and all 15 history tests, with no failures or leaks.
Final `make source-catalog-check engine-errors-check openapi-check` exited 0
(`source-scope-native-control-final-contract-check.log`), including nine schema
fragment tests. Linked-owner qualification remains pending below; these narrower
passes do not substitute for it or the original issues' end-to-end requirements.

The linked `history-control-apply-test -Dstrip=true -j1` retry exited 0
(`source-scope-native-control-owner-stripped-integration-test.log`): all 19
selected history/source-scope tests passed, zero skipped/failed/leaked. This
qualifies the existing real native-owner, routed and authenticated fixtures with
engine descriptor 57; it does not expose the new control methods through opaque
owner or data-Raft/RPC yet. Debug-symbol stripping changes emitted debug data,
not this run's assertions or selected test surface. The guarded lower-threshold
collector recovered usable disk space (44 GiB observed before its final shared
cache phases), protecting active builds and recent outputs.

### Opaque source-scope control boundary and qualified outcome reads

Owner ABI 110 adds bounded ordered source-scope control and read-only exact
outcome recovery calls. The C boundary checks version/reserved fields, positive
entry coordinates, table handle and payload size before decoding. Mutation uses
the DB's actual ordered entry/receipt path; outcome reads use the bounded
recovery DTO and independently qualified native applied cut. Both serialize
validated results into explicitly owned buffers. The typed client strictly
decodes applied observations against the exact command and entry. Superseded
is a recovery state, never success or authorization to replay effects.

The provisioned owner has a descriptor-pinned trusted apply wrapper and a routed
recovery method. The latter qualifies full catalog ownership and actual ReadIndex
before and after the opaque read, with bounded context/cancellation and future
cut rejection. Wrong-group apply calls fail before owner acquisition/dispatch.
A local recovery command is not a fresh metadata authorization; the real fixture
uses it only to query an absent receipt and does not invent a data-Raft entry.

Standalone scope/model/codec tests passed all 23 checks
(`source-scope-owner-boundary-codec-test.log`). The real linked owner integration
run passed all 19 selected history/source-scope tests, zero skips/failures/leaks
(`source-scope-opaque-owner-qualified-test.log`, -Dstrip=true -j1). New checks
exercise absent-receipt recovery, wrong physical owner, future cut, cancellation,
wrong-group apply guard and actual ABI rejection of old version, reserved fields,
zero entry and oversize payload. The initial compile caught an anonymous versus
named apply-receipt type mismatch at the C boundary; the explicit conversion was
fixed before this successful run. Repository source-catalog, engine-error and
OpenAPI checks passed (`source-scope-opaque-owner-contract-check.log`).

No stronger data protocol or public mutation endpoint is advertised by this ABI
change. Live control mutation qualification still requires an activated dedicated
Raft envelope, actual metadata authorization at proposal, and genuine legacy
worker handover before first strict activation. Native ordered retirement DB/RPC
exposure and durable rejection recovery, hidden checkpoint/cohort lifecycle,
complete Native/Lake destination integration, public history/bootstrap/retention,
typed production pgoutput, actual Antfly existing-client qualification and all
original recovery/performance criteria remain mandatory and incomplete.

### Activated source-scope lifecycle Raft envelope and native dispatch

Version 40 adds a distinct source-scope bind/close/drop envelope. It preserves the
full original source owner and physical storage descriptor, validates their
namespace/root-generation agreement and the bounded metadata-authorized command,
and rejects mixed/unknown payloads. Ordinary row-batch decoding rejects this
control, including when batch is null; it never fabricates an empty row write.
The projection store requires both a native delegate and the persisted v40
barrier. Native apply independently checks group and activated floor before
calling the descriptor-pinned owner ABI. Deterministic outcomes are retained at
the actual entry, and an expired result remains SourceOutcomeUnresolved.

The current advertised maximum becomes 40 only with this decoder, projection and
native handler present. Scoped BEGIN/PREPARE/RESOLVE remain profile 39; legacy
source-pin profile 12 is unchanged. Retirement does not silently inherit the new
lifecycle envelope and is still an explicit outstanding integration requirement.

The focused source-scope envelope and existing history-control entry suites
exited 0 (`source-scope-raft-entry-test.log`). They exercise activation, immutable
owner/descriptor binding, ordinary decoder refusal and duplicate/mixed payload
rejection. Repository source-catalog, engine-error and OpenAPI checks exited 0
(`source-scope-raft-control-contract-check.log`). A real linked native apply
regression is in progress: it extends the existing source-plan fixture to v40
activation and response-loss retry of a SourcePendingWork receipt, without
inventing document history. Its command is trusted test input, not evidence of
production metadata authorization or genuine legacy handover. Terminal results
follow below.

Leader proposal authorization, private lifecycle RPC, genuine old-worker drain,
ordered retirement DB/transport/rejection handling and hidden checkpoint/cohort
lifecycle remain incomplete. So do the original full destination, retained and
public history, bootstrap/retention, typed production pgoutput, actual Antfly
existing-client qualification and recovery/performance requirements. Both issues
remain open and the implementation PR remains draft.

The native state-machine regression passed
(`source-scope-raft-native-retry-test.log`). It proves v40 activation gating,
projection/native ordering, a committed native rejection receipt before an
injected response loss, no shared applied/token completion before retry, exact
retry yielding the original SourcePendingWork, and no document-apply effects.
The helper's initial import/type and local-name compilation errors were fixed
before that passing run.

Leader-local submission now accepts an Intent, not a caller metadata cut. It
obtains fresh signed metadata capture, qualifies exact binding/lifecycle phase,
then uses the existing membership/term-fenced protocol preflight, all-applying-
member activation and routed write admission to submit the dedicated command.
Every known accepted result carries the exact captured Command and actual Raft
receipt, so later read-only recovery preserves its fingerprint. Metadata endpoint
failover is read-only, uses no cached source snapshot and shares one bounded
platform deadline across every attempt. The leader wrapper also shares its
five-second total budget with metadata capture and proposal. Corrupt, denied or
phase-conflicting captures do not authorize another proposal.

The remote authorizer fixture verifies unsupported-endpoint failover, fresh cuts
10 then 11 on repeated calls, exact Intent preservation, closing-source bind
rejection, corrupt correlation rejection and cancellation before transport.
The actual linked Raft fixture also instantiates the new leader submission
wrapper and rejects a wrong-group intent before metadata access or proposal.
These tests do not claim a successful real metadata-host-to-data-leader control
submission or worker handover; that end-to-end qualification remains required.

Final `source-plan-apply-test history-control-apply-test -Dstrip=true -j1` exited
0 (`source-scope-raft-proposal-final-test.log`): the two native apply/authorizer
tests and all 21 linked history/scope tests passed, with no skips/failures/leaks.
An earlier combined run caught a future-cut test racing the read barrier: it saw
ReadUnavailable contention before the qualified future-cut check. The helper
now retries only ReadUnavailable, preserving the exact required future-cut
failure and never treating storage unavailability as success.

Engine descriptor 57, owner ABI 110, API kernel 30 and metadata protocol 104 are
unchanged by the Raft lifecycle/proposal implementation. Data profile 40 covers
these lifecycle controls; attempt identity remains 39 and legacy source pins 12.
Private lifecycle RPC, genuine legacy handover/physical-root cohort lifecycle,
ordered attempt retirement transport/rejection recovery and every remaining
original destination/history/bootstrap/retention/pgoutput/client acceptance
criterion are still incomplete. Neither issue can be closed on this evidence.

Final repository contract checks also exited 0 after the deadline and contention
fixes (`source-scope-raft-proposal-final-contract-check.log`), including all nine
schema fragment tests and generated/provenance checks.

### Correlated private source-scope RPC request/reply contract

`source_scope_http_wire.zig` separates submit Intent from exact read-only Recovery.
The request carries a required nonzero 16-byte request_id for one RPC attempt;
its digest binds that ID, version, operation and complete payload. Submission
accepts no caller metadata cut. Known accepted results carry the exact captured
Command and positive native receipt, even when their outcome remains unknown.
Unknown submit replies retain no invented command/entry, while unknown recovery
replies preserve their exact original command and receipt. Superseded receipts
confirm neither success nor failure. A determinate rejection is not an applied
effect, and applied responses validate the complete action/owner/state/cut.
Request JSON is bounded to 16 KiB and replies to 24 KiB, with strict duplicate and
unknown-field handling and no parsed-buffer ownership escaping the decoder.

`source_scope_control_operations.zig` supplies the callback ABI boundary. It
binds the immutable route and owner before dispatch, intersects deadlines and
cancellation, validates the returned request correlation and semantic evidence,
and preserves known command/receipt authority after late cancellation while
clearing confirmation. Ambiguous callback failure yields an unknown result;
forged replies become unresolved, never accepted success. HTTP adapters still
must authenticate service authority; this operation is not a public endpoint.

The first focused wire/ingress run passed all four selected tests, zero skips,
failures or leaks (`source-scope-control-ingress-test.log`). A subsequent update
adds explicit request IDs and corresponding generated private API schemas;
terminal generation and final validation results follow below. The new schemas
also clarify that an original captured Command in a recovery body grants only
read-only lookup, never fresh-write authorization.

Transport route/handler/client wiring, actual metadata-to-data proposal
qualification, genuine legacy worker handover, retirement transport and cohort/
checkpoint lifecycle remain unfinished. These internal contracts do not complete
any of the original full destination/public-history/bootstrap/retention/typed
pgoutput/real-Antfly-client/recovery/performance acceptance criteria by themselves.
Both issues stay open and the implementation PR stays draft.

Full `make generate` exited 0 (`source-scope-control-rpc-generate.log`). Only the
private internal OpenAPI root/types changed for this contract; unrelated generated
model-explorer artifacts from that run were restored. No endpoint, capability,
engine/owner/API ABI or data/metadata protocol maximum is advertised or changed.
Final `source-scope-control-wire-test -Dstrip=true -j1` exited 0: all four selected
wire/ingress tests passed, zero skipped/failed/leaked
(`source-scope-control-rpc-final-test.log`). Request-ID substitution and all-zero
ID rejection are included. Final source-catalog, engine-error and OpenAPI checks
also exited 0 (`source-scope-control-rpc-contract-check.log`), including nine
schema-fragment tests. Actual authenticated HTTP route/client integration still
must be implemented and qualified before this contract is a usable RPC surface.

### Authenticated lifecycle HTTP transport and routed recovery

The private `/internal/v1/groups/{group}/tables/{table}/source-scope/control-v1`
endpoint now has its schema, strict service-authenticated handler, bounded client,
production API configuration and routed/local DataServer dispatch. API kernel ABI
31 fences the new configuration port. Engine 57, owner ABI 110, data profile 40,
metadata 104 and failure envelope 89 remain unchanged.

HTTP submission sends only the validated Intent. The serving leader uses the
fresh metadata-authorized submission path and returns its exact captured command
with every known accepted entry. Recovery only reads the exact original command
and entry; its independently requested minimum applied index is preserved through
the leader wrapper, rather than reduced to the old entry index. The handler
requires an explicitly authenticated internal-service credential even in legacy
internal mode or when no service secret is configured. Bodies are capped at
16 KiB and encoded responses at 24 KiB. Phase conflicts and pending/unresolved
results retain conservative unknown-write classification where appropriate.

The client makes one transport attempt, validates request ID/correlation, owner,
route acknowledgement (including duplicate rejection), media type, size and
semantic evidence before returning a result. A valid response carrying an
unknown marker or late cancellation clears confirmation while preserving the
captured command/receipt. Lost, malformed, forged, oversized or unacknowledged
replies never cause automatic mutation replay. Unknown recovery keeps only its
original recovery authority.

The real forwarding-node/loopback-service/native-owner fixture exercises the new
recovery RPC and confirms that both the native cut and registry header remain
unchanged. Client tests cover absent credentials before transport, unknown-marker
receipt preservation, forged correlation/cut, missing/duplicate acknowledgement,
size limits and connection loss, with exactly one attempt. Dedicated handler tests
reject missing/invalid service tokens with legacy mode enabled, with and without
a configured secret, and verify conservative phase-error classification.
The first complete linked/handler run passed 27 selected linked tests and both
handler tests (`source-scope-control-http-final-test.log`), zero skips/failures/
leaks. Additional future-minimum qualification then reached the real ReadIndex
wait's Timeout before the outer DeadlineExceeded; the assertion now accepts only
those two bounded timeout forms and fails if an older result is returned.
Final generation and rerun evidence follows below.

This qualifies actual authenticated recovery transport, not a successful real
metadata-host-to-data-leader source activation. Genuine legacy handover remains
mandatory before first strict bind, and metadata-to-data mutating delivery,
retirement DB/RPC/rejection recovery, hidden checkpoint/cohort lifecycle and all
original destination/public-feed/bootstrap/retention/typed-pgoutput/actual-Antfly-
client/performance acceptance criteria remain unfinished. Both issues remain open.

Final `make generate` exited 0 (`source-scope-control-http-final-generate.log`);
unrelated model-explorer artifacts from this run were restored. Final
`history-control-apply-test source-scope-http-auth-test -Dstrip=true -j1` exited
0 (`source-scope-control-http-final-qualified-test.log`): all 27 selected linked
tests and both dedicated handler tests passed, zero skips/failures/leaks. This
includes the leader-wrapper future-minimum regression; returning an older result
fails the test, and only the outer deadline or inner ReadIndex timeout qualifies
as bounded unavailability. Final source-catalog, engine-error and OpenAPI checks
also exited 0 (`source-scope-control-http-final-contract-check.log`), including
nine schema-fragment tests. These checks do not close the remaining full scope.

### Bind destination planning grants to scoped admission identity

Scoped source delivery now supplies the reserved native scope reference to both
initial frozen planning and replacement planning. The reference derives from the
same validated metadata state and checkpoint owner used by lifecycle capture;
it carries the exact source generation, binding, domain, sequence and upload
token. A returned frozen plan must advertise the identical reference and a
retained artifact with the same binding and token before metadata checkpoint
binding or reservation can advance. Missing or substituted identities fail
closed. Legacy unscoped plans keep their null reference.

The shared derivation does not prove physical owner registration, legacy worker
drainage or successful admission. No existing single-root adapter is advertised
as a distributed/scoped destination. Production replication still needs the full
adapter and destination-aware worker integration.

`source-control-test -Dstrip=true -j1 --summary all` exited 0 with all 90 build
steps successful (`source-scope-planning-final-test.log`). Its selected delivery
regression covers derivation against the scope specification, exact artifact
matching, missing scope/artifact, substituted sequence/token and unexpected
scope on an unscoped grant. Existing metadata storage/reopen/capture and HTTP
service tests also passed, with zero reported failures or leaks. This is an
internal adapter-contract change; no persisted format or runtime ABI changes.
The original destination, retained history, public feeds, bootstrap/retention,
pgoutput and actual Antfly client qualification scope remains open.

### Separate distributed plan preparation from admission

The completion coordinator now exposes owned read-only `prepare`,
`restorePrepared` and `commitPrepared` operations. Existing ordinary commit uses
those same operations. This lets source delivery retain the complete original
plan before beginning native admission, rather than invoking the ordinary
commit path while planning. Prepared routes own their table names; the ordinary
commit result keeps its previous borrowed-input name lifetime.

Preparation freezes canonical complete input, captures and validates every
qualified owner, checks reservation capacity, and performs no decision lookup or
participant mutation. A source checkpoint supplies the coordinator role even
when it sorts after effects participants by namespace. Participant arrays stay
in canonical namespace order, with a separate coordinator index used for the
terminal decision. Caller-specified owners are compared with fresh observations
and never silently overwritten. A source-owned transaction without history
capability fails instead of returning the ordinary legacy fallback.

Restoration reads only retained frozen bytes and fresh qualified owner/routing
observations. It never recompiles provider effects or substitutes current owners.
The existing retained-owner route validator is shared with terminal recovery.
Topology/root identity conflicts retain the old authority and fail closed; this
does not implement archived-root routing across structural transitions.

This interface is a prerequisite for the production distributed source adapter;
it does not yet wire PostgreSQL workers through metadata reservation, retained
artifact transfer, hidden checkpoint provisioning or lifecycle handover. Those
and all remaining original issue acceptance criteria remain unfinished.

The split preparation/restore integration run passed all 27 selected linked
tests (`completion-prepared-admission-final-test.log`), zero skips/failures/leaks.
The source-catalog, engine-error and OpenAPI checks also passed
(`completion-prepared-admission-contract-check.log`), including nine schema
fragment tests. A further changed-topology regression initially expected
SourceBindingConflict, but the qualified owner port rejects the mismatched epoch
earlier with TopologyChanged. The assertion now requires that exact boundary
error and checks that no decision lookup or participant write occurred. Final
qualification of that additional regression follows below.

Final `history-control-apply-test -Dstrip=true -j1` exited 0
(`completion-prepared-admission-owner-final-test.log`): all 27 selected linked
tests and six selected local tests passed, zero skips/failures/leaks. This
includes exact topology-drift rejection during restoration, scoped checkpoint
coordinator selection, no-write preparation/restoration, frozen-byte reopening,
explicit prepared admission, owner substitution rejection and refusing source
fallback when every owner lacks retained-history capability. The original
public-feed/bootstrap/retention/pgoutput/client qualification scope is unchanged
and remains unfinished.

### Route source effects through the ordinary transaction planner

Ordinary multi-table commit and scoped source planning now share the same
participant partitioning function. Source planning preserves the ordinary
handling of writes, deletes, transforms, graph effects, predicates, integrity
operations, range guards, schema fences, restore fences and index maintenance.
`prepareSourceCommit` enlists the stable checkpoint root even without effects,
attaches the same source purpose/reference to every participant, freezes the
complete plan with checkpoint coordinator authority, and verifies the freshly
captured coordinator against the durable physical checkpoint reference.
Retired authorities, mismatched bindings/sequences and legacy receipt commands
are rejected before transaction admission. This compiles a plan; metadata
reservation/seal and native lifecycle registration still precede its admission.

The integration fixture now supplies the administrative topology snapshot as
well as its immutable routing generation. The original run correctly failed
because the previous fixture deliberately rejected administrative reads; the
ordinary router requires that snapshot to check structural transitions. The
qualified rerun exercises the shared router with two-root write/delete effects,
checkpoint-only planning, root-generation substitution, sequence substitution
and unchanged decision/write/publication counters. Final results follow below.
This does not yet expose the planner through the production worker destination
callback or implement the remaining original issue scope.

Final `history-control-apply-test -Dstrip=true -j1` exited 0
(`source-routed-preparation-final-test.log`): all 27 selected linked tests and
six selected local tests passed, zero skips/failures/leaks. The sequence mismatch
regression changes the scope reference while keeping a valid checkpoint command;
changing the checkpoint sequence alone without its predecessor correctly fails
earlier with SourceCheckpointConflict. Additional coverage verifies fresh plan
fences with stable physical checkpoint identity, rejects retired authorities and
legacy receipt commands, and requires release of captured topology snapshots
when a checkpoint table or its ranges are absent. The shared topology capture
now releases those snapshots on both successful null-return paths.
Source-catalog, engine-error and OpenAPI checks passed
(`source-routed-preparation-contract-check.log`), including nine schema fragment
tests. No persisted format, protocol or runtime ABI changes are introduced.
Production destination callback/worker wiring and the original remaining
history/public-feed/bootstrap/retention/pgoutput/client qualification scope
remain unfinished; both issues stay open.

### Retained destination plan manifest and native artifact handoff

`source_destination_plan.zig` now owns the frozen source plan bytes and a bounded,
versioned recovery manifest. The manifest pins the original checkpoint authority,
actual captured coordinator, source reference/token/sequence, checkpoint command,
transaction and admission identities, begin/commit versions, native artifact
epoch, full checksum, frozen digest and per-chunk hashes. Construction qualifies
the quota observation against the exact coordinator root; native BEGIN still
owns the atomic capacity decision. Metadata delivery input carries the original
checkpoint authority, the exact scope reference and the matching artifact grant.
Canonical encoded manifest and returned metadata fields must agree.

Recovery strictly parses the metadata-pinned manifest digest, validates the native
artifact grant, loads only a sealed/accepted ledger record through the existing
qualified transfer ports, and verifies every payload chunk and complete frozen
admission. Provider rows, current schema and new routing never reconstruct the
plan. The shared pure owner/fence check is used both at HTTP ingress and when
building the retained transfer. A manifest grants no metadata reservation, native
seal, source lifecycle registration, transaction commitment or source ACK.

Focused `completion-frozen-plan-test -Dstrip=true -j1` passed all four selected
tests (`source-destination-manifest-transfer-test.log`), zero skips/failures/leaks.
The new test uses a multi-chunk real frozen effect, loses the native seal reply,
recovers the seal without mutation, refuses loading before the metadata seal,
and restores the original plan after the original owned plan is destroyed.
It rejects wrong routes, forged quota roots, exhausted capacity, substituted
scope tokens/transaction IDs/checkpoints, changed bytes, mismatched metadata
encodings and unknown manifest fields. Real owner/HTTP qualification follows.

The production source actor/destination callback, legacy handover, lifecycle
cohort registration/retirement, checkpoint root provisioning and all original
public feed/bootstrap/retention/pgoutput/client qualification work remain open.

Real `history-control-apply-test -Dstrip=true -j1` exited 0
(`source-destination-manifest-native-test.log`): all 27 selected linked tests
and six selected local tests passed, zero skips/failures/leaks. The existing
forwarding-node/authenticated HTTP/native owner fixture now constructs and
transfers a scoped destination manifest through production ports, refuses
loading on native seal alone, restores its original admission/checkpoint input
from a locally granted sealed artifact ledger, and verifies that the complete
qualified observation/cut is unchanged by loading. Artifact reclamation is
qualified separately afterward. The fixture's ledger and retirement grants are
local test inputs, not evidence of a real metadata source actor transition or
successful source lifecycle activation/admission.

Source-catalog, engine-error and OpenAPI checks passed
(`source-destination-manifest-contract-check.log`), including nine schema
fragment tests. No existing wire format, Raft profile or ABI changes are needed
for this versioned opaque source manifest. Production destination callback and
worker integration, real lifecycle handover and the full remaining original
acceptance criteria remain open.

### Resume an original retained scoped admission after a lost phase reply

`Coordinator.commitSourcePrepared` now supports completing a source transaction
from its original retained scoped plan. The ordinary prepared-commit path still
refuses a pending transaction. The source path requires scoped checkpoint receipt
support and uses durable write synchronization, independently of derived index
visibility. Before replaying any pending phase it checks the positive retained
coordinator admission digest, original begin/creation times, source purpose and
complete scope reference. Every BEGIN/PREPARE uses the original frozen plan and
captured participant authority. Unknown phase outcomes stop that pass; confirmed
COMMIT uses terminal recovery without replaying admission. ABORT stays a conflict.

The caller must first observe the exact sealed metadata grant and load its native
artifact; this method is an internal coordinator primitive, not a substitute for
those proofs. Native scope fencing remains authoritative at every participant.
Production actor/destination callback wiring must enforce that handoff.

`history-control-apply-test -Dstrip=true -j1` exited 0
(`source-retained-admission-recovery-test.log`): all 27 selected linked tests and
six selected local tests passed, zero skips/failures/leaks. The coordinator-port
regression models a positive pending source coordinator, lost PREPARE reply and
restart with the same frozen plan. It checks immediate stop after uncertainty,
exact pending identity checks before any BEGIN, checkpoint coordinator selection,
full completion on the later pass, no admission replay after COMMIT, refusal of
ABORT and ordinary/unscoped input, and no derived visibility wait. This is modeled
phase recovery inside the linked suite, not qualification of successful real
source activation/admission through the metadata actor and native registry.
Source-catalog, engine-error and OpenAPI checks passed
(`source-retained-admission-recovery-contract-check.log`), including nine schema
fragment tests. All original outstanding full-scope acceptance criteria remain
open; no issue is closed and no protocol or ABI changes are introduced.

### Bind retained admission to one metadata reservation and native checkpoint

`source_destination_admission.zig` now checks the actor's exact reservation and
artifact ledger grant against its bounded retained manifest. It verifies source
binding/generation/domain, reservation identity, original transaction, artifact
specification, source input fingerprint and complete checkpoint command before
using native evidence. Retired artifact grants fail closed. Admission requires
the metadata sealed phase; a native seal alone does not authorize it.

Checkpoint resolution validates a request-bound native observation. Absence is
unproven, expiry is explicit, and only positive original checkpoint evidence
returns fixed owned acceptance proof. That proof is separate from all-owner
completion, source ACK, index visibility and retention release. The admission
path loads the original sealed artifact, freshly qualifies its original owners,
and invokes scoped retained recovery. An unknown mutation response permits only
a checkpoint read in the same pass, never automatic write replay.

`Coordinator.qualifyRetained` takes ownership of an already verified plan on all
paths. Both byte-based restoration and destination admission use it, avoiding a
second full frozen-plan encode/decode while capturing fresh transport authority.
Focused `completion-frozen-plan-test -Dstrip=true -j1` passed all five selected
tests (`source-destination-admission-test.log`), zero skips/failures/leaks. Tests
cover metadata/payload identity substitution, native checkpoint correlation,
unproven absence, seal gating before checkpoint access and positive acceptance
without touching admission/artifact callbacks. Linked qualification follows.

Production actor/adapter wiring, durable release authority and artifact manifest
retention after metadata acceptance, real lifecycle registration/handover and the
full original remaining acceptance criteria are still unfinished.

Final `completion-frozen-plan-test history-control-apply-test -Dstrip=true -j1`
exited 0 (`source-destination-admission-qualified-test.log`): all five focused
tests, 27 selected linked tests and six selected local tests passed, zero
skips/failures/leaks. The real forwarding-node/authenticated HTTP/native owner
fixture now constructs a source-identity-bound local reservation/ledger grant,
resolves its genuinely absent native checkpoint through production ports, and
verifies that the complete qualified native observation remains unchanged. The
fixture still grants metadata state locally and never invokes real source BEGIN;
it does not prove full worker handover or successful source activation/admission.

Source-catalog, engine-error and OpenAPI checks passed
(`source-destination-admission-contract-check.log`), including nine schema
fragment tests. No protocol, persisted format or runtime ABI changes occur in
this step. The original full scope remains active. In particular, production
metadata acceptance must retain sufficient original manifest authority for later
completion/reclamation instead of losing it when the pending reservation clears.
That durability contract, actor/worker wiring and all remaining original issue
acceptance criteria are unfinished.

### Metadata protocol 105 retains recovery manifests after acceptance

Source reservations and replans can now negotiate retained manifest authority.
The persistent artifact record keeps its opaque manifest and plan digest through
reserved, sealed, accepted and garbage phases. Acceptance can clear the pending
reservation without losing cleanup routing. Only proven reclamation removes the
manifest. The source state retains the negotiated mode across subsequent grants
and same-destination reseeding; downgrade attempts and adapters without the
retention capability fail closed. New retained reservation identities use v3;
existing unretained v1/v2 identities and omitted-default JSON remain unchanged.

Metadata topology protocol 105 gates the new command/state decoder contract.
Encoded command inspection recognizes field presence even for explicit false
and escaped field names, so an older decoder cannot be admitted accidentally.
Recovery manifests are capped at 64 KiB, independently of row/frozen payload
capacity, keeping duplicated pending/ledger JSON safely bounded within existing
state limits. Native destination manifests negotiate the mode and admission
requires the ledger's exact retained manifest rather than a transient copy.

The first `source-control-test completion-frozen-plan-test -Dstrip=true -j1`
passed (`source-manifest-retention-test.log`). Expanded source-control, focused
and linked runs also passed (`source-manifest-retention-qualified-test.log`).
Coverage includes original/retained delivery modes with lost native/metadata
seal replies, acceptance, committed cleanup debt, manifest persistence through
store reopen and snapshot installation, final removal on reclamation, scoped
identity checks, downgrade refusal, replanning after fenced reclamation and
protocol classification. The final field-presence qualification follows below.

This supplies durable manifest authority for production cleanup; it does not
implement all-owner release/retirement, legacy worker handover or production
source actor wiring. All original outstanding acceptance criteria remain open.

Final generation exited 0 (`source-manifest-retention-generate.log`); only the
two unrelated model-explorer outputs were restored. Final `source-control-test
completion-frozen-plan-test history-control-apply-test -Dstrip=true -j1` exited
0 (`source-manifest-retention-owner-final-test.log`). All five focused tests,
27 selected linked tests, six selected local tests and the selected metadata
storage/service tests passed, zero reported skips/failures/leaks. This includes
explicit-false/escaped retention field negotiation and grant ownership after
the original metadata manifest buffer is freed. The owned grant duplicates its
bounded recovery bytes into the parsed manifest arena, avoiding a snapshot
lifetime dependency introduced by the retained record field.

Final source-catalog, engine-error and OpenAPI checks passed
(`source-manifest-retention-contract-check.log`), including nine schema fragment
tests. Metadata protocol 105 is the only version change; engine 57, storage owner
ABI 110, API kernel 31, data profile 40 and failure envelope 89 stay unchanged.
All original scope remains active: durable manifest retention is implemented,
while production destination/worker integration, real lifecycle handover and
all-owner release plus the original feeds/bootstrap/retention/pgoutput/actual
Antfly client qualification acceptance criteria remain unfinished.

### Bounded durable source acceptance receipts

`source_destination_receipt.zig` converts positive checkpoint observations into
bounded internal metadata receipts. Each receipt binds its original reservation,
retained manifest digest, complete source reference/token and request-qualified
checkpoint evidence. It references the retained manifest rather than copying
routing/chunk metadata into the 8 KiB receipt. Receipt creation leaves metadata's
committed frontier untouched; acceptance is separate from all-owner completion,
source ACK, index coverage and retention release.

Decoding binds current applied actor state, original checkpoint authority,
artifact specification and retained manifest. It remains usable after acceptance
clears the pending reservation and during garbage cleanup. Exact accepted metadata
reference plus the artifact ledger's cumulative reclamation floor recognizes
completed cleanup after newer attempts own the current manifest or replace the
last-reclaimed identity. A token beyond that proven floor never becomes a
reclaimed result, even in a structurally valid generic metadata row. No native
proof or new owner authority is reconstructed after manifest reclamation.

Actor-facing `resolveDelivery` and `admitDelivery` now return these bounded receipts
from the metadata-bound admission bridge. Closing blocks admission before native
I/O while positive resolution of original pending work remains available. The
whole synchronous operation preserves the earliest request/route deadline and
cancellation through receipt encoding.

Focused tests passed with the maximum 1024-byte checkpoint containing JSON control
characters (`source-destination-receipt-test.log`). The receipt stays within its
8 KiB bound, accepted progress does not become committed progress, and cleanup
recovers after metadata roundtrip, artifact retirement and reclamation. Expanded
runs additionally cover actor closing/draining and replay across newer pending
and reclaimed attempts. Final qualification follows below.

These are internal receipt/admission interfaces; successful production source
activation, destination worker wiring and all-owner release remain unfinished,
as do all original remaining issue acceptance criteria.

Final `completion-frozen-plan-test history-control-apply-test -Dstrip=true -j1`
exited 0 (`source-destination-receipt-floor-final-test.log`): all six focused
tests, 27 selected linked tests and six selected local tests passed, zero
skips/failures/leaks. This includes the cumulative-floor counterexample and
idempotent old cleanup while a new attempt is pending and after that attempt
is reclaimed. Native regression tests remain green; the new receipt/actor
acceptance scenarios use modeled checkpoint evidence and do not prove a real
source worker activation or successful scoped BEGIN through production metadata.

Source-catalog, engine-error and OpenAPI checks passed
(`source-destination-receipt-contract-check.log`), including nine schema fragment
tests. The receipt is a new versioned opaque internal format; existing Raft,
metadata and ABI versions are unchanged. No issue is closed, and the full
original implementation goal remains active.

### Recover source completion against the accepted original admission

Completion recovery now has an explicit source expectation: original admission
digest, complete scope reference, source purpose and begin/commit versions.
`runSource` validates those fields against a fresh retained coordinator read
before routing any terminal or publication mutation. A pending coordinator is
unresolved, ABORT is a conflict, and missing evidence remains unproven. The
ordinary retained-job path is unchanged and never gains source expiry authority.

`source_destination_completion.zig` decodes the exact applied metadata acceptance
receipt, qualifies its original coordinator route and invokes this bound recovery.
Only original participant resolution/publication can produce complete. It never
advances metadata committed progress, frees private transaction/registry state,
returns source ACK or grants retention release. Already reclaimed receipts remain
idempotent without reconstructing native authority. Unaccepted pending work and
forged coordinator fences fail before recovery writes.

The first linked run passed (`source-bound-completion-test.log`). Expanded
qualification links a genuine scoped plan/manifest, modeled positive checkpoint
observation, applied-in-memory metadata acceptance and durable receipt to the
existing two-owner recovery port fixture. It checks identity rejection before
writes, completion at the checkpoint coordinator even when that owner sorts
second, and unchanged metadata committed progress. This is modeled source-flow
qualification; successful production metadata/worker activation is still open.
Final results follow below.

The expanded receipt flow exposed a real ownership bug in frozen manifest
construction: checkpoint strings referenced a temporary parsed batch's nested
arena. Deinitializing that batch can return its backing allocation to the outer
arena; later manifest serialization overwrote the checkpoint bytes. Construction
now copies both checkpoint and predecessor strings into the retained arena before
releasing the parsed batch. The original two-owner case reproduced the corruption
and now verifies the complete original checkpoint command before receipt creation.

A dedicated `source-completion-recovery-test` target selects the actual retained
two-owner test through a compile discovery anchor and an exact runtime filter;
no harness-only/empty selection counts as that qualification. Its final focused
run passed (`source-bound-completion-owned-test.log`). Final
`source-completion-recovery-test completion-frozen-plan-test
history-control-apply-test -Dstrip=true -j1` exited 0
(`source-bound-completion-final-test.log`): the focused source recovery test,
all six frozen/receipt tests, 27 selected linked tests and six selected local
tests passed, zero reported skips/failures/leaks. Pending and missing coordinator
cases also produce no writes. Source-catalog, engine-error and OpenAPI checks
passed (`source-bound-completion-final-contract-check.log`), including nine schema
fragment tests. No protocol or ABI version changes occur in this step.

The full goal remains active. These checks qualify internal receipt-bound
completion and existing native regressions, not successful production source
activation, all-owner private retirement/release or the remaining original
public feeds/bootstrap/retention/pgoutput/actual Antfly client acceptance criteria.


### Ordered source retirement through the compiled storage engine

The DB facade now exposes ordered retirement and exact outcome recovery through
storage engine contract version 58. A positive original coordinator ABORT remains
mandatory. The native writer commits the scope token floor, local enlisted ABORT
resolution when present, retirement outcome and actual apply marker together.
The DB holds the physical owner/apply fence throughout. Narrow deterministic
rejections are persisted with their marker only after the mutation writer aborts;
unresolved evidence, corruption and storage errors remain errors.

The bounded retirement journal uses an explicit version-2 key and checksum domain.
Its command fingerprint includes the entire qualified coordinator observation,
including its actual read cut. A changed observation at the same entry conflicts.
An absent or superseded journal never proves a local or global transaction result.
The previous journal key is not misread as the new envelope.

The actual DB/compiled-engine source-scope fixture now also exercises an
unenlisted retirement, exact retry, restart recovery, changed observation cut,
binding rejection without a changed token floor, future minimum refusal,
supersession and final reopen. Registry binding and the positive remote ABORT
observation are explicit fixture setup, not successful production legacy handover
or an independently executed remote coordinator read.

`source-scope-native-control-test -Dstrip=true -j1` passed its selected native DB
case (`source-retirement-db-test.log`). `docstore-history-test -Dstrip=true -j1`
passed all 15 selected tests with zero skips/failures/leaks
(`source-retirement-history-test.log`), including the changed-read-cut regression
in the real NativeKV reopen helper. Full `make generate` completed successfully
(`source-retirement-db-generate.log`); unrelated generated model explorer outputs
were discarded. Source-catalog, engine-error and OpenAPI checks passed, including
nine schema fragment tests (`source-retirement-db-contract-check.log`). Linked
qualification results are recorded below when complete.

Owner ABI 110, API kernel 31, data protocol 40, metadata protocol 105 and failure
contract 89 are unchanged. Retirement is not yet exposed through the opaque owner
ABI, a data Raft entry or authenticated remote transport. Those proposal boundaries
must obtain fresh independent coordinator evidence; accepting a caller-provided
observation would not meet the contract. All-owner retirement/release and the full
original destination/public feeds/bootstrap/retention/pgoutput/actual Antfly CDC
client acceptance scope remain open. The goal and both issues remain open.

Final `history-control-apply-test -Dstrip=true -j1` exited 0
(`source-retirement-db-linked-test.log`): all 27 selected linked and six selected
local tests passed, zero reported skips/failures/leaks. Guarded repository cache
cleanup completed and preserved active/recent outputs
(`source-retirement-db-gc.log`). The full goal remains active.


### Native owner retirement boundary and qualified exact recovery

Owner ABI 111 adds private ordered source retirement and exact outcome recovery
using the existing versioned JSON/entry headers. The apply boundary rejects old
ABI, reserved fields, missing entry cuts and oversized requests, keeps the decoded
coordinator admission arena alive, and invokes engine 58 with its actual entry.
It is a trusted apply boundary, not caller-supplied coordinator authorization.
No external retirement mutation endpoint or data Raft retirement entry exists yet.

Bounded request codecs retain all dynamic coordinator admission participants and
locator bytes. Response envelopes correlate the full command fingerprint and
actual entry for every outcome, including rejection and supersession. Positive
responses additionally validate the original intent, coordinator record digest
and actual apply cut. Corrupt, oversized, unrelated or malformed responses remain
errors. These values are fixed after decoding; no borrowed dynamic response data
escapes a parsed arena.

Qualified retirement recovery uses the shared native routed read machinery:
current catalog/physical owner admission, leader ReadIndex, frozen barrier
validation before/after the owner call, minimum applied cut, deadline and
cancellation checks. No mutation or current-registry inference is introduced.
Native fixtures exercise absent-receipt supersession, route mismatch, future
minimum refusal, cancellation and ABI guards; their modeled ABORT is used only
for a read-only query. It does not prove production coordinator-read provenance.

Full generation completed (`source-retirement-owner-generate.log`); only the
intended generated source control catalog remains, and unrelated model explorer
outputs were discarded. Contract checks passed
(`source-retirement-owner-contract-check.log`). The selected real NativeKV
retirement helper covers owned query decoding after input release and correlated
success/rejection/supersession response decoding, including changed command read
cut and changed entry term rejection. All 15 retained-history tests passed in
`source-retirement-owner-test.log`; linked results follow when complete.

Engine 58, API kernel 31, data 40, metadata 105 and failure 89 remain unchanged.
The next retirement step still needs a versioned data Raft entry, trusted proposal
construction from a fresh independent coordinator read and authenticated transport.
The broader original goal remains active with both issues open.

The first linked compile caught a fixture assigning a scope cut directly to the
nominally distinct coordinator publication position. Explicit field conversion
fixed it. Final `history-control-apply-test -Dstrip=true -j1` exited 0
(`source-retirement-owner-linked-retry-test.log`): 27 selected linked and six local
tests passed, zero skips/failures/leaks. This includes real opaque owner retirement
recovery and ABI guard calls. Guarded cache cleanup completed
(`source-retirement-owner-gc.log`), preserving active/recent outputs. The private
ordered apply ABI is compiled/exported; successful retirement mutation through a
real versioned Raft entry is still unqualified and remains required next.


### Activated source retirement Raft dispatch

Data protocol 41 adds a distinct source-retirement entry, carrying the complete
original positive coordinator ABORT command and immutable physical owner
snapshot. Ordinary document decoding refuses the envelope. Strict decoding
requires explicit owner/root fields, rejects mixed/duplicate layouts, validates
positive ABORT/admission membership and bounds the command and complete entry.
Encoding requires the activated protocol floor. Source-scope lifecycle v40 and
legacy source-pin protocol 12 retain their separate meanings.

The topology projection delegates retirement without manufacturing document rows,
history or split deltas, requiring durable v41 activation. Native apply validates
the activated projection floor and original group, uses an apply-only descriptor
lease and owner ABI 111/engine 58, and records bounded deterministic rejection as
its actual entry outcome. Superseded receipts remain unresolved. A test-only
lost-reply injection occurs after the native commit and before publishing the
apply outcome; replay must recover the exact durable receipt before advancing
application progress. No new production endpoint or coordinator-read provenance
is implied by this dispatch.

The native fixture extends the existing source-plan/scope lifecycle case from
entry 10 through activation at 11 and retirement at 12. It checks preactivation
refusal in both projection and native apply, a lost native rejection reply,
unchanged document-apply counts, exact rejection recovery and rejection of a
changed coordinator read cut. The missing scope remains missing; first strict
legacy-worker handover is not bypassed. Coordinator ABORT input is modeled for
this trusted apply-path qualification; production proposal evidence remains open.

Full generation completed (`source-retirement-raft-generate.log`), with unrelated
model explorer outputs discarded. Contract checks passed, including schema
fragments (`source-retirement-raft-contract-check.log`). All 15 retained-history
tests passed in `source-retirement-raft-test.log`. Initial qualification also
caught an invalid modeled COMMIT with an ABORT-only zero commit version and a
nested test constant shadowing an outer token. The fixture now uses a valid
COMMIT version before asserting refusal, and distinct test-local names. Final
codec and linked results follow below.

Engine 58, owner ABI 111, API kernel 31, metadata 105 and failure 89 are unchanged.
Production intent-only retirement submission must still obtain a fresh independent
coordinator ReadIndex observation, preserve its owned admission for exact recovery,
and use authenticated transport. All-owner retirement/release and all remaining
original destination/changefeeds/bootstrap/retention/pgoutput/Antfly CDC client
requirements remain open; the full goal remains active.

Final `source-scope-entry-test history-control-apply-test -Dstrip=true -j1`
exited 0 (`source-retirement-raft-retry-test.log`): all three selected codec cases,
28 linked cases and six linked-suite local cases passed, plus 12 local discovery
anchors; zero reported skips/failures/leaks. The retirement native helper runs
inside the existing real source-plan/native-owner apply fixture. Guarded cache
cleanup completed (`source-retirement-raft-gc.log`). This proves activated trusted
apply/retry dispatch, not production coordinator observation provenance or end-to-
end source retirement/release. The full original goal remains active.


### Intent-only local retirement proposal authority

The local retirement submission method accepts an intent and original coordinator
locator, never an observation. Before proposal it verifies the target fence/group
and local leadership, constructs a bounded routing session for the original
coordinator owner, and obtains a fresh decision through the qualified history
decision port. Its original minimum applied index is preserved. The reply must
prove resolved positive ABORT with the exact source scope, purpose, admission,
participant ownership and checkpoint coordinator role; the original retained
coordinator member must name the supplied locator. Missing, pending and committed
evidence cannot authorize retirement. No presume-abort or metadata-progress
inference is added.

The owned command keeps the independently read admission arena alive through
proposal and exact recovery. Accepted submissions retain their actual entry and
optional outcome. An ambiguous submission without a known index returns the exact
owned command with no entry/outcome, rather than discarding its observation or
retrying the mutation. The common native control path now applies protocol-41
membership activation, routed write admission, immutable owner qualification,
proposal authority checks, real apply outcome waiting and read-only exact recovery
to retirement. Coordinator read and proposal share the original bounded deadline
and cancellation context. Exact recovery never appends a control or barrier.

A focused authority fixture uses authoritative catalog generations and the real
decision-port boundary to check fresh cuts on repeated capture, owned admission
locator lifetime, original minimum propagation, absence/pending/COMMIT refusal,
scope/admission mismatch, unresolved ABORT, route mismatch and cancellation before
reads. The native leader fixture submits only an intent for an absent coordinator
transaction and checks that the registry observation/apply cut is unchanged. It
also instantiates the complete production submission path and rejects wrong-group
input before dispatch. Successful positive scoped retirement proposal and end-to-
end source activation remain open; the native fixture does not manufacture ABORT.

Source-catalog, engine-error and OpenAPI checks passed, including schema fragments
(`source-retirement-authority-contract-check.log`). No schema, protocol or ABI
changes occur in this step. Final focused/linked results follow below. External
authenticated retirement transport, all-owner orchestration/release and the entire
remaining original destination/public feed/bootstrap/retention/pgoutput/actual
Antfly client qualification scope remain required. Both issues and the full goal
remain open.

Final `source-retirement-authority-test history-control-apply-test -Dstrip=true
-j1` exited 0 (`source-retirement-authority-test.log`): the focused authority case
and its discovery anchor, 28 linked cases and six linked-suite local cases passed,
plus 12 local discovery anchors; zero reported skips/failures/leaks. An additional
retained coordinator locator mismatch case passed in a final focused run
(`source-retirement-authority-final-test.log`). Guarded cache cleanup completed
(`source-retirement-authority-gc.log`). Successful scoped ABORT submission through
production Raft and authenticated retirement transport remain required, as do all
other outstanding original acceptance criteria. The goal remains active.


### Authenticated retirement submit/recovery transport

The spec now defines `/internal/v1/groups/{group}/tables/{table}/source-retirement/
control-v1`. Submit carries only original retirement intent, original coordinator
locator and a fresh nonzero RPC identity. The leader independently obtains the
coordinator ABORT before protocol-41 proposal. Recovery accepts the exact original
command/entry and minimum applied index and is read-only. An observation supplied
inside a recovery query grants no proposal authorization. Local fencing proves no
global commit, publication, source ACK, retention release or all-owner retirement.

Transport request/reply bounds include the original bounded coordinator admission
(16,798,720 and 16,802,816 bytes respectively). Requests and replies explicitly own
all decoded participant/locator data. Reply validation correlates the complete
request including nonce, original intent and retained coordinator locator, full
command fingerprint and actual entry. A known command may survive an ambiguous
submit without a known index, but no outcome is then permitted. Recovery preserves
its exact command/entry; superseded remains unconfirmed. No parsed arena is freed
while its participant strings remain referenced by an emitted response.

API kernel 32 adds the owned callback port. The HTTP handler requires an explicit
valid internal-service credential even under legacy unauthenticated settings,
checks body limits and route identity, and marks unconfirmed submits unknown.
The routed port uses leader discovery and authenticated bounded forwarding. The
client has exactly one HTTP attempt, requires one matching route acknowledgement
and JSON content type, validates the whole correlated reply and clears confirmation
on unknown markers or late cancellation. Lost, malformed, oversized, mismatched
or unsuccessful replies produce owned unconfirmed responses; recovery preserves
its original command and entry rather than manufacturing a result or resending.

The transport fixture covers owned decoding after input/response buffers are
released, missing credentials before I/O, one-attempt loss handling, wrong nonce,
missing/duplicate acknowledgement, unknown outcome markers, known command without
entry, exact recovery and changed receipt refusal. Submit JSON with an extra
observation is refused. HTTP auth coverage includes missing/invalid credentials
in legacy mode. The actual native forwarding fixture queries an absent exact
retirement receipt, requires superseded, and checks unchanged registry/apply cut.
It uses modeled evidence only for read-only recovery, not source activation.

Full generation completed (`source-retirement-http-generate.log`), retaining the
intended internal OpenAPI artifacts and discarding unrelated model explorer
outputs. Contract checks passed (`source-retirement-http-contract-check.log`).
Initial linked compiles caught a missing shared dispatch enumeration case, an
untyped fixture cut and use of an owned rather than borrowed fake-response header
type; these were corrected before final qualification. Final results follow below.

Storage engine 58, owner ABI 111, data profile 41, metadata 105 and failure 89 are
unchanged. Successful production scoped retirement/activation, all-owner retirement
and source ACK/release, genuine legacy-worker handover, the destination adapter,
public feeds/bootstrap/retention, production pgoutput and actual Antfly existing-
client qualification remain open. The full original goal and both issues remain
open; transport qualification alone cannot satisfy them.

The first executing linked run passed the new client/auth cases but exposed that
the manually constructed native fixture server omitted the retirement callback;
its conservative 501/unknown result correctly failed the expected exact-read test.
Production server configuration already wired the callback. Adding it to the
fixture produced final `history-control-apply-test -Dstrip=true -j1` exit 0
(`source-retirement-http-native-test.log`): all 30 linked tests and six local
integration tests passed, zero reported skips/failures/leaks. Exact retirement
recovery traverses the real authenticated forwarding path, returns superseded and
leaves the native registry/apply cut unchanged. The focused authority case also
passed in the earlier combined run. Guarded cache cleanup completed
(`source-retirement-http-gc.log`). The full original goal remains active.


### Stop new legacy CDC rounds for destination-owned sources

Both production metadata services now establish qualified metadata ReadIndex
before checking whether a legacy snapshot/stream source may start. Standalone's
single catalog owner performs the same native lifecycle-store check under its
catalog lock. A configured destination actor reserves its source ordinal and its
canonical definition: matching work cannot resume through the old unscoped
`commitBatch` path, including when an unchanged definition moves ordinal or an
actor is retired. Unrelated ordinals/definitions remain eligible. This decision
reads persisted actor state, not the transient lifecycle-work counter; zero work
never grants downgrade authority.

The read uses one native snapshot and a strict table/group actor prefix. It
validates state/key ownership, bounds traversal to 256 records and 4 MiB total
value bytes, and fails closed on incomplete or corrupt census. A definitive match
pauses the old source before provider preparation, destination commit or provider
ACK. A bounded/unreadable source census is unproven; the coordinator defers that
source and continues the round rather than granting permission or aborting later
sources. Existing legacy-only test sinks keep their old interface; all production
cluster/standalone owners expose the gate.

Actual native metadata fixtures check initial eligibility, blocking immediately
after configuration, definition matching across ordinal moves, unrelated source
eligibility, persistence through reopen and snapshot, blocking after retirement
with work count zero, record and byte scan caps, and corrupt state refusal. The
real metadata Raft/ReadIndex host checks the service method before configuration,
after configuration and after retirement. The CDC fixture checks that both an
otherwise eligible snapshot and streaming source are paused without checkpoint
movement and that an unproven census defers without destination completion. Its
initial streaming check was below readiness while still in snapshot phase; the
fixture was corrected to be streaming-eligible before testing the gate.

`source-control-test antfly-metadata-sql-sessions-test -Dstrip=true -j1` passed
(`source-legacy-intake-native-test.log`), including the two selected real metadata
host/session cases and all selected source-control/storage cases. The CDC suite
passed all 30 selected cases after the readiness correction
(`source-legacy-intake-coordinator-test.log`); final localized-error and standalone
qualification results follow below. Source-catalog, engine-error and OpenAPI
checks passed (`source-legacy-intake-contract-check.log`). No schema/protocol/ABI
change is introduced.

This is intake ownership, not persisted worker handover. A worker already past
its intake check may still be active. Active-round cancellation/drainage, delayed
unlabelled internal writes, durable producer fencing and old transaction recovery
still require independent qualification. The native first-strict activation gate
remains closed and is not weakened by this change. Production destination adapter,
cohort lifecycle/retirement/release and all remaining original public feeds,
bootstrap/retention, pgoutput and actual Antfly CDC client requirements remain
open. The full goal and both original issues remain open.

Final localized-error `source-transaction-test -Dstrip=true -j1` exited 0
(`source-legacy-intake-final-coordinator-test.log`): all 30 CDC cases and four
selected effect/transaction cases passed, plus 12 discovery anchors. Standalone's
production wrapper now exposes the same native gate under its catalog lock;
`antfly-standalone-cdc-test -Dstrip=true -j1` passed all eight selected cases
(`source-legacy-intake-standalone-test.log`), including native actor configuration
blocking the old source path. Zero reported skips/failures/leaks. Guarded cache
cleanup completed (`source-legacy-intake-gc.log`). These results qualify new-round
intake only; durable old-writer fencing and in-flight handover remain required.
The full original goal remains active.


### Active legacy CDC ownership checks

Legacy runners now revalidate destination-actor ownership at their entry and
mutation/progress boundaries, not only when a round selects a source. Snapshot
query/row/checkpoint/finalization checks also cover sources without an exact
cutover authority id. Exact-cutover claim, provider mutation checks and retirement
completion callbacks use the same actor ownership guard. Streaming checks bracket
provider polling, atomic destination groups and checkpoint publication, including
idle polls. If ownership is transferred or its census is unproven, the runner
stops. Its error path must revalidate before writing legacy failure status, so it
cannot overwrite status after a new actor takes ownership.

Focused lifecycle fixtures transfer ownership after a snapshot destination commit,
after a stream poll and after a stream destination commit. They verify the old
snapshot checkpoint/offset and stream checkpoint remain unchanged, no failure
status is published under transferred ownership, no destination commit occurs
when transfer preceded apply, and a completed destination effect alone grants no
provider frontier. These are modeled boundary interleavings through the production
runner kernel; the qualified production metadata and standalone gate implementations
were exercised in the preceding native host/store tests.

`source-transaction-test -Dstrip=true -j1` exited 0
(`source-legacy-active-worker-test.log`): all 30 selected CDC cases, four selected
transaction/effect cases and 12 discovery anchors passed; zero reported skips,
failures or leaks. Source-catalog, engine-error and OpenAPI checks passed
(`source-legacy-active-worker-contract-check.log`). No schema, protocol or ABI
change occurs here.

These checks do not make metadata read and destination dispatch one atomic fence.
A remote write already sent may still arrive after a transfer, and old binaries
may not perform these checks. Persisted producer/ingress fencing, durable worker
handover and old transaction recovery are still required before first strict
native scope activation can succeed. The existing native activation gate remains
closed. All other outstanding original destination, retained-history/public-feed,
bootstrap/retention, pgoutput and actual Antfly CDC client acceptance requirements
remain open, and the full goal remains active.

The final idle-poll ownership transfer case also passed: no destination commit or
legacy status/checkpoint publication occurs after the poll reports transferred
ownership. Final `source-transaction-test -Dstrip=true -j1` exited 0
(`source-legacy-active-worker-final-test.log`), again with all 30 selected CDC and
four effect/transaction cases passing plus 12 discovery anchors, zero reported
skips/failures/leaks. Guarded cache cleanup completed
(`source-legacy-active-worker-gc.log`). These checks still do not authorize first
strict activation or claim durable old-producer fencing. The full goal remains
active.


### Durable native producer closure and ordinary primary-commit enforcement

A bounded native producer policy records one checksummed, physical-root-bound
closure with its immutable first actual ordered cut. Repeated closure cannot
reopen legacy intake or move the original cut. Missing old labels stay explicitly
unlabelled; they are never inferred to be ordinary from row contents. The private
trusted classifier distinguishes unlabelled, ordinary, legacy CDC and scoped-source
producers. After closure, unlabelled and legacy CDC require SourcePendingWork.
Explicit ordinary/scoped classifications remain subject to their independent
normal mutation/admission validation. Classification is trusted native ingress
input, not a new field accepted from public batch JSON.

Storage engine 59 exposes private ordered producer closure and explicitly
classified committed mutation. Closure validates the actual physical owner and
source authority, holds the native apply fence, and commits the closure and actual
apply marker together. Its returned closure is cumulative state with the original
cut, not an invented exact per-attempt outcome. Replay cannot claim closure at an
entry predating the first fence. The ordinary primary-commit path checks the policy
under the same apply mutex before effects, after recognizing durable ordered
replay. Missing classification defaults to unlabelled. Header corruption or a
physical namespace/generation mismatch fails closed, including for ordinary writes.

Real NativeKV tests cover atomic rollback, reopen, immutable first closure,
changed cut/root refusal, delayed unlabelled/tagged-legacy rejection without
changing the test effect, ordinary classification and corruption/bounds. The real
DB/compiled-engine fixture additionally applies an ordinary row before closure,
closes at entry 12, refuses delayed unlabelled row effects without advancing the
apply marker, admits explicit ordinary effects at 13, refuses tagged legacy,
retains the original closure through entry 14 and reopen, recognizes old ordered
replay before policy rejection, and refuses ordinary effects under corrupted
policy state. Trusted labels/cuts are modeled fixture inputs; they are not new
production coordinator authority.

`docstore-history-test -Dstrip=true -j1` passed all 15 selected tests with zero
skips/failures/leaks (`source-producer-policy-test.log`). Final
`source-scope-native-control-test docstore-history-test -Dstrip=true -j1` exited 0
(`source-producer-policy-native-test.log`): the actual DB/compiled-engine case and
all 15 history cases passed, zero reported skips/failures/leaks. Full generation
completed (`source-producer-policy-generate.log`); the intended source catalog and
error-name artifacts remain, with unrelated model explorer outputs discarded.
Source-catalog, engine-error and OpenAPI checks passed
(`source-producer-policy-contract-check.log`). Final linked results follow below.

Owner ABI 111, API kernel 32, data profile 41, metadata 105 and failure 89 are
unchanged. No public, owner-C-ABI or Raft producer-closure endpoint is exposed yet.
Authenticated producer classification must still be carried end to end without
upgrading old unlabelled internal requests to ordinary. All relevant native
transaction admission/resolution and special mutation paths still require the
same audit/enforcement and old unsafe transaction drainage. Snapshot/restore and
cohort lifecycle qualification also remain required. The first-strict native
activation gate is still closed; this policy alone is never worker-drain proof.
All other original destination/public feeds/bootstrap/retention/pgoutput/actual
Antfly existing-client requirements remain open, with the full goal active.

Final `history-control-apply-test -Dstrip=true -j1` exited 0
(`source-producer-policy-linked-test.log`): all 30 selected linked and six local
integration cases passed, zero reported skips/failures/leaks. Guarded cache
cleanup completed (`source-producer-policy-gc.log`), preserving active/recent
outputs. This qualifies the storage primitive and ordinary primary commit path,
not complete durable producer fencing across transaction admission, every special
mutation path or network ingress. First strict activation remains closed. The
full original goal and both issues remain open.
