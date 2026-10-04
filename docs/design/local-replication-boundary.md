# Local storage and server replication boundary

This refactor precedes the physical local-engine move into `antfly-embedded`
(#953), which precedes the Apache/ELv2 licensing and release changes (#893).
It preserves source locations and licenses. Raft and hot-standby runtime
implementations remain server owned.

## Ownership

| Local storage | Server adapters |
| --- | --- |
| Atomic mutations and replay receipts | Raft entry dispatch and recovery coordination |
| Durable publication outbox and retry ordering | Hot-standby log, slots, transport, and record matching |
| Local apply, publication, and transition locks | Role, promotion, fencing, and standby selection |
| Admission and publication callbacks | Remote durability policy, waits, and telemetry |
| Background-work permission supplied by admission | Whether a particular role may run background mutations |

`storage/db/replication_contract.zig` contains borrowed ports. Its write gate
has borrowed, captured, and generation-pinned forms, without server role tags.
The DB checks admission and asks whether background work is allowed. Captured
admission equality is supplied by its adapter; opaque bytes are never compared
because captures may contain padding or slices.

A publication binding exposes three local requirements: synchronous completion,
durable outbox retention, and preflight admission. The hot-standby adapter maps
its policy to these requirements. Local storage never selects standbys or
interprets acknowledgement policy. Publication and failure telemetry are
callbacks into the adapter. `storage/hot_standby/durability_policy.zig` owns the
server policy types.

## Binding lifetime

Bindings are copied into open options, caches, DB state, and deferred completion
records. Their callback capture therefore stores configuration by value. It
must not borrow a temporary adapter or depend on its own address. Captures may
contain borrowed pointers and slices whose targets outlive every binding copy;
they must not own resources requiring destruction.

`BorrowedCapture` bounds the inline payload at 256 bytes with 16-byte alignment.
Both encoding and decoding enforce these limits at compile time; debug builds also validate the captured type identity. Only the
adapter that installed the callbacks interprets the payload. The hot-standby
factory captures policy, wait context, and telemetry pointers; copying a binding
preserves its configuration snapshot without an allocation or mutable global
policy. Semantic equality belongs to the adapter.

## Commit and recovery ordering

Fail-closed preflight still runs under the publication lock before local commit.
Mutation, replay receipt, and pending outbox writes remain atomic. Publication
happens after local commit even when authority expires, so committed mutations
do not disappear from the replication tail. Acknowledgement waits release the
local apply and transition locks; successful completion reacquires the transition
lock and checks admission again before acknowledging the client.

Ordered transaction APIs use `OrderedApplyReceipt` and `AtOrderedReceipt` names.
Raft entry dispatch remains in server adapters, which translate term/index into
that receipt. The local store retains ordered receipt validation and atomic
persistence; it does not implement consensus.

Existing receipt keys, outbox keys, envelope versions, and binary encodings are
unchanged. Historical `raft` provenance discriminants remain where they are
part of existing serialized source-authority and artifact-position formats.
The runtime error ABI preserves the existing corruption status identity.
Direct local dispatch returns the generic receipt error; foreign runtime
dispatch decodes the released canonical error name. Compatibility tests cover
both paths without renumbering or renaming the wire detail.

## Naming and enforcement

Authored runtime helpers, types, and DataServer configuration use `hot_standby`,
`hotStandby`, or `HotStandby`. Legacy `--ha-*` CLI aliases, serialized field
names, persisted key strings, deprecated OpenAPI aliases and configuration keys,
published ABI declarations and runtime status enum names, and established
server error tags remain compatible. Comments referring to internal symbols
follow the authored names; compatibility tests retain the legacy vocabulary
they exercise.
Raft keeps its own name in server coordination.

The source boundary audit rejects both server imports and server policy fields
in the local DB ports and commit integration. Tests cover copied policy captures,
borrowed counters, policy-to-requirement mapping, admission generations,
background-work permission, lock release during waits, final admission rechecks,
and unchanged durable receipt encodings. Physical package moves and relicensing
belong to the dependent PRs.

New local mutation paths must use the same admission and publication ports.
For example, entity-edge rewrite retains its local mutation/replay/outbox
ordering and checkpoint barrier while using generic publication errors and
completion hooks. The audit rejects legacy hot-standby publisher names in the
local owner, so incoming server changes cannot silently restore that coupling.

`unit-storage-test-audit` checks explicit test ownership before storage unit
compilation. An adapter that acquires tests must be registered in
`storage/test_manifest.zig`, even when focused tests already reach it through
another import. The inventory and disjoint shard checks remain required.

The focused storage gate validates caller filters against the combined local
and server test inventories before dispatching to either owner. A filter may
select just one owner; unknown filters still fail. The server's aggregate
slice remains independent of filters intended for the local root.

## Local maintenance requirements and external upload recovery

Resolver retirement asks the promotion runtime for typed readiness while holding
its catalog activity fence. Diagnostic status strings remain available to users
but do not grant retirement authority. A pending publisher blocks retirement;
a runtime without local publication ownership leaves server reconciliation free
to remove its local resolver.

Storage interprets historical source-authority records as local or ordered
maintenance requirements. The persisted format, legacy ordered receipt fence,
publication namespace, and corruption checks remain unchanged. DB maintenance
uses these requirements without selecting a Raft role.

`storage/artifact_upload_recovery.zig` owns upload polling cadence, idle detection,
fairness, and queue admission cursors. A cheap borrowed dispatcher hook checks
cadence before DB reads its bounded upload inventory. Storage releases the read
snapshot before handing those facts to the owner; the owner releases its own
mutex before queue admission. A refused proposal never advances its cursor.
Explicit retries bypass periodic cadence. Durable exact-incarnation and progress
checks remain in storage and are the only authority to retire upload bytes.

## Producer scheduling, publication recovery, and TTL routing

`storage/db/artifact_producer_scheduler.zig` owns volatile producer polling,
single-flight admission, fairness cursors, and retry rounds. It is shared local
maintenance, including native inference. DB supplies budgeted transactional work;
it retains durable obligations, exact-generation checks, replay-journal append,
and completion receipts. A scheduler cursor advances only after accepted work,
and a restart rediscovers obligations from storage.

`storage/db/publication_outbox_recovery.zig` owns the publication retry driver.
Its borrowed port provides queue submission, probes, clocks, and fenced draining.
The DB owner still drains queued work before destruction. Durable outboxes,
startup publication barriers, and local append/acknowledgement ordering remain in
storage. Retry deadlines never prove delivery or discharge an obligation.

`storage/coordinated_ttl.zig` contains only local expiration observations and their
borrowed callback. `storage/server_coordinated_ttl.zig` binds group routing and owns
the bounded server queue. Stable cache-entry bindings synchronize route refreshes
with callbacks without holding a routing lock across distributed work. C ABI
adapters attach their existing group identity; the wire layout is unchanged.
Storage retains timestamps, content digests, schema guards, and local deletion.

Upload recovery is one optional dispatcher capability containing both cadence and
recovery callbacks. Its integration regression uses the actual server runtime-hook
factory and verifies refused periodic admission, cadence suppression, explicit
retry, and the following maintenance opportunity against reopened upload state.

## Visibility observations and child-range effects

The DB emits `QueryVisibilityEvent` through a borrowed context and callback.
`storage/server_query_visibility.zig` binds table, group, cache, and owner identity
outside storage. The private C owner attaches its handle's existing routing
identity when encoding the unchanged notification ABI. Detachment still waits
for in-flight observations, and installing a hook still rehydrates durable repair
state. An observer that needs local DB access borrows it through its own context.

`storage/db/document_child_range_effects.zig` partitions prepared generated effects
against local manifest snapshots and physical key bounds. A pure, bounded
selection callback supplies a destination; planning does not inspect server role
or placement status. `storage/server_document_child_range.zig` interprets committed
server placement. Delivery adapters retain live routing and transport admission
checks after the local apply fence is released.

`document_child_range_manifest.zig` owns child-range decoding and allocation
cleanup. `document_child_range_outbox.zig` owns intent encoding, staging, and
delivery iteration. DB supplies fenced manifest reads, snapshot scans, and intent
deletion. Local effects and staged intents still commit in the same batch; an
intent is deleted only after successful delivery. The version-one record and
persisted destination remain compatible. Allocation failure cannot transfer only
part of a record's key/value ownership.

## Local recovery and maintenance owners

`storage/db/portable_activation_recovery.zig` owns activation queue admission,
retry jitter, supervisor probes, the running flag, and permanent close state.
Its stable borrowed port invokes DB's fenced catalog activation. The completion
handshake and runtime owner drain protect the DB and callback context through
shutdown, including a supervisor probe claimed before close.

`storage/db/quarantine_recovery.zig` owns index-load retry registration and joining.
DB supplies load-failure observation and its fenced retry operation. A completed
cohort is joined before another cohort can be scheduled; close permanently
rejects new registration. These are embedded self-healing mechanisms and have
no server coordination dependency.

`storage/db/independent_maintenance.zig` owns bounded operation order, retry
suppression, active/idle and source-scan cadence, scheduler registration, and
shutdown. DB supplies local activity and the budgeted, fenced operations. Runtime
awake time controls repair retry deadlines; relational-index activity retains its
existing clock. Activation contention yields the turn without increasing repair
backoff. Other repair errors leave relational maintenance able to progress.

Publication recovery snapshots its diagnostic counter and publishes the next
retry deadline before releasing its single-flight state. A successor admitted by
a rearm callback cannot change the previous failure's report. Producer
single-flight tests advance the clock while a page is active so cadence cannot
hide missing admission protection.

## Remaining local worker owners and server metadata

`storage/server_group_metadata.zig` owns the server's group creation timestamp
key and accessors. DB provides ordinary reads and writes without interpreting
this server metadata. The existing key and decimal encoding remain unchanged;
server status consumers attach their group identity in the adapter.

`storage/db/graph_cleanup_owner.zig` owns scheduler registration, bounded polling
cadence, and a permanent shutdown barrier. DB supplies write eligibility and one
fenced graph cleanup page. A registration borrows the stable DB address until
stop joins its callback outside the admission mutex.

`storage/db/runtime_restart_owner.zig` owns restart admission, coalesced rerun
requests, capped retry delay, and bounded durable-lane resubmission. Enrichment,
text merge, and sparse compaction each have a separate owner. DB supplies desired
state and a start attempt; enrichment runtime replacement remains protected by
its lifecycle mutex, and structural mutations still govern paused runtimes.
The durable owner lane drains before the borrowed context is destroyed.

`storage/db/native_projection_owner.zig` owns wakeups, worker admission, retryable
failure classification, and shutdown/join. Its stable AsyncContext supplies one
publication round. Catalog pins, stable-tip/cardinality checks, snapshot admission,
and durable checkpoint publication remain local storage operations. Shutdown
joins outside the worker admission mutex; a stopped owner cannot be restarted.

`storage/db/index_repair_scheduler.zig` owns the revision projection, exact runnable
heap, fairness cursor, progress waits, and independent summary types. Durable
repair checkpoints remain the authority. DB reconciles committed events under
its scheduler mutex and executes selected fenced work. A revision gap still
invalidates the projection; volatile progress hints cannot authorize publication.

## Cleanup supervision, visibility lifetime, and local batching

`storage/db/cleanup_job_owner.zig` owns repair-shadow and generated-artifact job
admission, notification coalescing, bounded pages, retry delays, and queue yielding.
IndexManager owns the shared admission atomics; the durable owner lane drains
before either IndexManager or the borrowed operation context is destroyed. DB
retains the actual cleanup page, catalog/snapshot/apply fences, durable cursors,
and terminal filesystem finalization. Inline lanes yield after a bounded slice
on errors, contention, and progress; an explicit maintenance request or reopen
rediscovers the durable marker. Restart supervision uses the same inline-yield
rule, keeping desired runtime state available for the next explicit request.

`storage/db/query_visibility.zig` owns the local visibility event contracts and
observer attachment, replay leases, in-flight callbacks, and detachment barrier.
Callbacks run outside the attachment mutex. A replay lease protects the borrowed
observer while DB reconstructs exact repair identity from durable checkpoints;
no borrowed intent strings survive a notification. Existing DB type aliases and
C notification layouts remain compatible. Detachment must be invoked outside
that observer's own callback and joins outstanding callback and replay leases.

`storage/db/source_pin_cleanup_owner.zig` owns fairness turns, retry deadlines,
progress-sensitive backoff, and diagnostics. Source-pin intents, bounded deletion,
and epoch fencing remain in the local storage reconciliation code. Work-unit
counters allow an error after partial progress to retain a short retry delay.

`storage/db/applied_sequence_coalescer.zig` owns watermark batching and owned
index-name memory. DB retains checkpoint serialization and durable publication.
The existing maximum-per-index rule and 100 ms cadence are unchanged; removing
a pending item transfers its key ownership to the caller.

The independent-maintenance shutdown regression waits for the stop critical
section to release its mutex, with a bounded deadline. This distinguishes the
normal flag-before-unlock handoff from a join-under-lock regression without
leaving a callback borrowed past owner destruction.

## Bulk sessions, target tracking, and schema reconciliation

`storage/db/bulk_ingest_session.zig` retains direct-write bulk admission and the
active-session statistic. The unused buffered staging map and recursive flush
path have been removed. Writes and transforms still commit through ordinary
batch execution before session finish. The public bulk-coalescing statistics
layout is preserved; obsolete staging counters remain zero. Scratch mutation
execution has no resident bulk session.

`storage/db/target_advance_tracker.zig` owns process-local maintenance handoffs,
stuck-index observations, warning cooldowns, their owned index names, and a
mutex. Diagnostic snapshots own their names independently of later clears.
Allocation failure rolls back an inserted map reservation. DB still verifies
durable counters, generation identity, coverage, and publication authority before
turning a handoff into a rebuild or repair intent. The abandoned dense-maintenance
cooldown map and urgent-score setting were removed; the live warning cooldown
setting remains supported.

`storage/db/schema_reconcile_owner.zig` owns admission, coalesced publication
reruns, queued execution, synchronous fallback, and permanent stop state. Movable
and inline handles complete on the caller without retaining a callback context.
Queue rejection uses the same caller fallback. DB supplies one reconciliation
pass and keeps schema-version checks and durable building/failed/ready states.
Close stops admission before draining the durable owner lane, which protects the
borrowed DB and reconciliation owner until queued work completes.

Server group-created timestamp persistence and schema-upgrade assertions belong
to the server integration suite. Local relational tests preserve opaque internal
metadata through ordinary storage operations and have no server metadata import.
Enrichment runtime replacement and dense replay session ownership now have local
owners, described below. Their provider leases and durable publication fences
remain part of the embedded engine closure.


## Enrichment bundle and dense replay session ownership

`storage/db/enrichment_runtime_owner.zig` owns the runtime and its append context
as one bundle. Construction adopts moved providers only after runtime init
succeeds; every later failure destroys the bundle exactly once. DB hydrates the
resident security/execution capabilities and supplies callbacks for durable
failure-envelope recovery and replay target selection. Replacement is serialized
by an owner mutex even when transaction recovery is absent. The recovery provider
borrow fence still encloses replacement when that runtime is present.

A replacement is constructed before stopping the old worker. Its durable state
is reloaded after that worker joins, before starting or publishing the replacement.
Failure retains the original bundle and restores both active and pending restart
demand. Paused replacement transfers ownership without starting the new worker.
The existing DB and AsyncContext runtime/context fields are borrowed views;
publication updates them under the lifecycle fence, and only the bundle destroys
those allocations. Close drains background callbacks before bundle teardown.

`storage/db/dense_catch_up_session_owner.zig` owns the token nonce, session map,
owned index names, capture leases, snapshot replay leases, and active tracking.
A failed registration leaves replay admission with the caller. Token/name checks
fence stale finish and retain callbacks, and retaining admission under the map
mutex allows an in-flight transaction to outlive token removal. DB retains the
catalog and incarnation checks, dense-finish admission fence, resource accounting,
native WAL commit, generation publication, and durable lifecycle checkpoints.
Tracking transitions remain serialized by that dense-finish fence; callback drain
precedes session-owner destruction.

The merge-page regression uses direct bulk writes and checks committed copy
cursors and receiver base rows across reopen, without fabricating retired staging
state. Focused maintenance tests include provider replacement, token admission,
allocation rollback, and both owner modules; normal test ownership is preserved.

Constructor failure paths also unwind the runtime's lease adapter and owned
identity before returning an error. Providers remain with the caller until
construction succeeds, and cleanup does not release a pre-existing durable
lease. Allocation-failure and corrupt persisted-status regressions exercise
these ownership boundaries.
