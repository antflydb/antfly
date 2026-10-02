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

## Naming and enforcement

Internal `ha_`, `ha...`, and declared `...HA...` helpers use `hot_standby`,
`hotStandby`, or `HotStandby`. Legacy `--ha-*` CLI aliases, serialized field
names, persisted key strings, published runtime status enum names, and established server error tags remain compatible.
Raft keeps its own name in server coordination.

The source boundary audit rejects both server imports and server policy fields
in the local DB ports and commit integration. Tests cover copied policy captures,
borrowed counters, policy-to-requirement mapping, admission generations,
background-work permission, lock release during waits, final admission rechecks,
and unchanged durable receipt encodings. Physical package moves and relicensing
belong to the dependent PRs.
