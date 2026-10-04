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
