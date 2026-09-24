// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Synchronous borrowed interface to a physically backed replicated completion
//! pool. Merely constructing these structs does not mint backing or authority.
//! A native provider may publish a lease only after resources and restart debt
//! are installed. No Zig allocator or error-set value crosses this boundary.
const failure = @import("runtime_failure_abi");

pub const pool_abi_version: u32 = 1;
pub const max_entries: usize = 256;
pub const max_members: usize = 256;
/// Aggregate entry/proposal bytes in one callback, checked before any copying
/// or decoding. Snapshot bytes are excluded and use separately installed
/// snapshot resources; they must never enter the prepare workspace.
pub const max_check_payload_bytes: usize = 4 * 1024 * 1024;

pub const Bytes = extern struct {
    ptr: ?[*]const u8 = null,
    len: u64 = 0,
};
pub const NodeIds = extern struct {
    ptr: ?[*]const u64 = null,
    len: u64 = 0,
};
pub const Membership = extern struct {
    voters: NodeIds = .{},
    outgoing: NodeIds = .{},
    learners: NodeIds = .{},
    learners_next: NodeIds = .{},
    auto_leave: u8 = 0,
    reserved: [7]u8 = @splat(0),
};
pub const Identity = extern struct {
    version: u32 = pool_abi_version,
    protocol: u32 = 1,
    profile: u32 = 1,
    capacity: u32 = 0,
    group_id: u64 = 0,
    node_id: u64 = 0,
    incarnation: [16]u8 = @splat(0),
    policy_digest: [32]u8 = @splat(0),
    generation: u64 = 0,
};
/// Internal trusted installer output. DATA constructs this only after verifying
/// a metadata installation response; it is never decoded from a write request.
/// Catalog bytes travel separately and must match the advertised digest before
/// native backing is qualified.
pub const InstallBinding = extern struct {
    identity: Identity = .{},
    schema_catalog_digest: [32]u8 = @splat(0),
    table_id: u64 = 0,
    range_id: u64 = 0,
    split_attempt_epoch: u64 = 0,
    expected_definition: [32]u8 = @splat(0),
};

pub const State = extern struct {
    applied_term: u64 = 0,
    applied_term_known: u8 = 0,
    reserved: [7]u8 = @splat(0),
    term: u64 = 0,
    commit_index: u64 = 0,
    applied_index: u64 = 0,
    last_index: u64 = 0,
    leader_id: u64 = 0,
    membership: Membership = .{},
};
pub const EntryKind = enum(u32) { normal = 0, configuration_v1 = 1, configuration_v2 = 2, _ };
pub const Entry = extern struct {
    term: u64 = 0,
    index: u64 = 0,
    kind: EntryKind = .normal,
    reserved: u32 = 0,
    /// Entire canonical envelope or ordinary Raft payload, never a re-encoded
    /// subset. Native validation classifies it before accepting owned debt.
    payload: Bytes = .{},
};
pub const Entries = extern struct { ptr: ?[*]const Entry = null, len: u64 = 0 };
pub const Payloads = extern struct { ptr: ?[*]const Bytes = null, len: u64 = 0 };
pub const ChangeKind = enum(u32) { add_voter = 1, add_learner = 2, remove = 3, _ };
pub const Change = extern struct { kind: ChangeKind, reserved: u32 = 0, node_id: u64 };
pub const Changes = extern struct { ptr: ?[*]const Change = null, len: u64 = 0 };
pub const Transition = enum(u32) { automatic = 0, joint_implicit = 1, joint_explicit = 2, _ };
pub const MessageKind = enum(u32) {
    propose = 1,
    pre_vote = 2,
    pre_vote_response = 3,
    request_vote = 4,
    request_vote_response = 5,
    append_entries = 6,
    append_entries_response = 7,
    heartbeat = 8,
    heartbeat_response = 9,
    snapshot = 10,
    snapshot_response = 11,
    transfer_leader = 12,
    forget_leader = 13,
    timeout_now = 14,
    read_index = 15,
    read_index_response = 16,
    storage_append = 17,
    storage_append_response = 18,
    storage_apply = 19,
    storage_apply_response = 20,
    _,
};
pub const Kind = enum(u32) {
    campaign = 1,
    inbound = 2,
    proposal = 3,
    configuration = 4,
    ready = 5,
    storage_ack = 6,
    snapshot_admission = 7,
    _,
};
pub const Snapshot = extern struct {
    present: u8 = 0,
    reserved: [7]u8 = @splat(0),
    term: u64 = 0,
    index: u64 = 0,
    membership: Membership = .{},
    data: Bytes = .{},
};
pub const Check = extern struct {
    version: u32 = pool_abi_version,
    kind: Kind,
    new_work_allowed: u8 = 0,
    rejected: u8 = 0,
    reserved: [6]u8 = @splat(0),
    state: State,
    entries: Entries = .{},
    committed_entries: Entries = .{},
    proposals: Payloads = .{},
    changes: Changes = .{},
    transition: Transition = .automatic,
    message_kind: MessageKind = .heartbeat,
    from: u64 = 0,
    to: u64 = 0,
    message_term: u64 = 0,
    previous_index: u64 = 0,
    previous_term: u64 = 0,
    message_commit_index: u64 = 0,
    vote: u64 = 0,
    reject_hint: u64 = 0,
    context: Bytes = .{},
    snapshot: Snapshot = .{},
};
pub const ProposalResult = extern struct {
    version: u32 = pool_abi_version,
    reserved: u32 = 0,
    state: State,
    /// Both zero means authoritative local nonacceptance. Nonzero indices
    /// retain accepted ownership even if the proposal caller received an error.
    first_index: u64 = 0,
    last_index: u64 = 0,
    payloads: Payloads = .{},
};
pub const CheckResult = extern struct {
    version: u32 = pool_abi_version,
    has_append_prefix: u8 = 0,
    reserved: [3]u8 = @splat(0),
    append_prefix: u64 = 0,
};
/// Permanent group-wide durable progress, not a transaction receipt. The
/// issuer retains it across transaction retirement and certifies that every
/// predecessor has completed durably. Consumers still match the exact entry.
pub const Progress = extern struct {
    term: u64 = 0,
    index: u64 = 0,
    payload_digest: [32]u8 = @splat(0),
};
pub const max_durable_cells = 4;
pub const DurableCell = extern struct {
    identity: Progress = .{},
    prepared: u8 = 0,
    reserved: [7]u8 = @splat(0),
};
pub const DurableCells = extern struct {
    version: u32 = pool_abi_version,
    count: u32 = 0,
    startup_reconciliation_pending: u8 = 0,
    reserved: [3]u8 = @splat(0),
    cells: [max_durable_cells]DurableCell = @splat(.{}),
};
pub const DurableObservation = extern struct {
    expected: Progress = .{},
    observed_term: u64 = 0,
    observed_digest: [32]u8 = @splat(0),
    present: u8 = 0,
    replaced_in_this_persist: u8 = 0,
    reserved: [6]u8 = @splat(0),
};
pub const ReconcileMode = enum(u32) { startup_complete = 1, persisted_replacement = 2, _ };
pub const DurableLog = extern struct {
    version: u32 = pool_abi_version,
    mode: ReconcileMode,
    compacted_index: u64 = 0,
    compacted_term: u64 = 0,
    last_index: u64 = 0,
    commit_index: u64 = 0,
    count: u32 = 0,
    reserved: u32 = 0,
    observations: [max_durable_cells]DurableObservation = @splat(.{}),
};
/// Independent v2 control proof. The v1 document structs and vtable remain
/// byte-for-byte unchanged; a missing v2 issuer cannot attest a control owner.
pub const control_proof_abi_version: u32 = 2;
pub const max_durable_controls = 4;
pub const ControlDurableOwnerV2 = extern struct {
    identity: Progress = .{},
    slot_index: u32 = 0,
    reserved: u32 = 0,
};
pub const ControlDurableOwnersV2 = extern struct {
    version: u32 = control_proof_abi_version,
    count: u32 = 0,
    reserved: u64 = 0,
    owners: [max_durable_controls]ControlDurableOwnerV2 = @splat(.{}),
};
pub const ControlDurableLogV2 = extern struct {
    version: u32 = control_proof_abi_version,
    mode: ReconcileMode,
    compacted_index: u64 = 0,
    compacted_term: u64 = 0,
    last_index: u64 = 0,
    commit_index: u64 = 0,
    count: u32 = 0,
    reserved: u32 = 0,
    observations: [max_durable_controls]DurableObservation = @splat(.{}),
    /// One atomic native reconciliation also carries the unchanged v1
    /// document proof, closing any gap between two callback invocations.
    document: DurableLog = .{ .mode = .startup_complete },
};
pub const ControlVTableV2 = extern struct {
    release: *const fn (?*anyopaque) callconv(.c) void,
    durable_owners: *const fn (?*anyopaque, *ControlDurableOwnersV2) callconv(.c) failure.Status,
    reconcile_durable: *const fn (?*anyopaque, *const ControlDurableLogV2) callconv(.c) failure.Status,
};
pub const ControlLeaseV2 = extern struct {
    identity: Identity,
    context: ?*anyopaque,
    vtable: *const ControlVTableV2,
};
pub const ControlProviderV2 = extern struct {
    context: ?*anyopaque,
    acquire: *const fn (?*anyopaque, u64, u64, *ControlLeaseV2) callconv(.c) failure.Status,
};
test "workload admission completion pool v1 durable ABI layout remains unchanged" {
    const std = @import("std");
    try std.testing.expectEqual(@as(usize, 240), @sizeOf(DurableCells));
    try std.testing.expectEqual(@as(usize, 16), @offsetOf(DurableCells, "cells"));
    try std.testing.expectEqual(@as(usize, 432), @sizeOf(DurableLog));
    try std.testing.expectEqual(@as(usize, 48), @offsetOf(DurableLog, "observations"));
    try std.testing.expectEqual(@as(usize, 64), @sizeOf(VTable));
    try std.testing.expectEqual(@as(usize, 56), @offsetOf(VTable, "reconcile_durable"));
}
pub const VTable = extern struct {
    /// Inbound checks are nonmutating readiness checks. Ready may acquire
    /// accepted-entry ownership before persistence/ACK. A changed volatile
    /// suffix is not evidence that an old durable suffix has been discarded;
    /// persistence failure must fence until the durable frontier is recovered.
    check: *const fn (?*anyopaque, *const Check, *CheckResult) callconv(.c) failure.Status,
    /// Infallible finalization of bookkeeping reserved by check. No allocation,
    /// capacity acquisition, or issuance of new authority is permitted here.
    proposal_result: *const fn (?*anyopaque, *const ProposalResult) callconv(.c) void,
    /// Releases the runtime reference; accepted durable debt remains owned.
    release: *const fn (?*anyopaque) callconv(.c) void,
    /// Synchronous apply of the exact previously accepted envelope; all bytes
    /// are borrowed and the owner/DB lifetime is pinned by this lease.
    apply_accepted: ?*const fn (?*anyopaque, u64, u64, Bytes) callconv(.c) failure.Status = null,
    /// Allocation-free query from the retained native owner. not_found means
    /// no durable receipt, never permission to infer a volatile frontier.
    progress: ?*const fn (?*anyopaque, *Progress) callconv(.c) failure.Status = null,
    /// Exact accepted ownership, including protected resolutions. Classification
    /// must consult owned state or a matching durable receipt, not payload flags.
    owns_accepted: ?*const fn (?*anyopaque, u64, u64, Bytes, *u8) callconv(.c) failure.Status = null,
    /// Enumerates owned identities without allocating. Only complete persisted
    /// log evidence may retire them; a Ready preview is never sufficient.
    durable_cells: ?*const fn (?*anyopaque, *DurableCells) callconv(.c) failure.Status = null,
    reconcile_durable: ?*const fn (?*anyopaque, *const DurableLog) callconv(.c) failure.Status = null,
};
pub const Lease = extern struct {
    identity: Identity,
    context: ?*anyopaque,
    vtable: *const VTable,
};

pub const Provider = extern struct {
    context: ?*anyopaque,
    /// Acquires a runtime reference to an already installed native group pool.
    /// This cannot open an ordinary DB and then infer authority from flags:
    /// pool/backend restoration must precede trusted DB binding and group join.
    /// Success fully initializes out_lease and transfers one owned reference.
    acquire: *const fn (?*anyopaque, u64, u64, *Lease) callconv(.c) failure.Status,
    /// Success proves installed/restored backing, even when all admitted slots
    /// are occupied. Never return success while fenced or awaiting maintenance.
    attest: ?*const fn (?*anyopaque, u64, u64, *NativeAttestation) callconv(.c) failure.Status = null,
};

pub const NativeAttestation = extern struct {
    identity: Identity = .{},
    accepted_count: u32 = 0,
    prepared_count: u32 = 0,
};

/// DATA adds its actual Raft state while holding the serialized owner. These
/// fixed-value fields safely cross DATA→API; native does not certify arbitrary
/// caller-supplied membership. Nodes concatenate voters/outgoing/learners/next.
pub const Attestation = extern struct {
    version: u32 = pool_abi_version,
    auto_leave: u8 = 0,
    reserved: [3]u8 = @splat(0),
    backing: NativeAttestation = .{},
    term: u64 = 0,
    commit_index: u64 = 0,
    applied_index: u64 = 0,
    last_index: u64 = 0,
    leader_id: u64 = 0,
    member_counts: [4]u32 = @splat(0),
    members: [max_members]u64 = @splat(0),
};
pub const AttestationSource = extern struct {
    context: ?*anyopaque,
    snapshot: *const fn (?*anyopaque, u64, *Attestation) callconv(.c) failure.Status,
};
