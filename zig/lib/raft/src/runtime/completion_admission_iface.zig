// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const core = @import("../core/mod.zig");

/// Runtime-owned prerequisite for mandatory application completion. This is
/// separate from Raft log/transport queue admission. A provider must own real
/// backing before allowing an enabled group's consensus effects; configuration
/// or a peer's decoder-version report alone is never sufficient evidence.
/// All callbacks execute on the serialized Raft owner. They must not recurse
/// into that owner, perform peer RPCs, or wait for a background reservation.
pub const Check = union(enum) {
    campaign,
    inbound: core.Message,
    proposal: []const []const u8,
    configuration: core.ConfChangeV2,
    configuration_v1: core.ConfChange,
    ready: core.Ready,
    storage_ack: core.Message,
    snapshot_admission,
};

pub const ProposalResult = struct {
    payloads: []const []const u8,
    first_index: ?core.types.Index,
    last_index: ?core.types.Index,
};
pub const Progress = struct {
    term: u64,
    index: u64,
    payload_digest: [32]u8,
};
/// Borrowed complete image owned by a qualified durable storage provider.
/// This type is never constructed from RawNode's volatile log or Ready preview.
pub const DurableLog = struct {
    mode: enum { startup_complete, persisted_replacement },
    durability_confirmed: bool,
    compacted_index: u64,
    compacted_term: u64,
    last_index: u64,
    commit_index: u64,
    entries: []const core.Entry,
    replacement_first: u64 = 0,
    replacement_last: u64 = 0,
};

pub const Guard = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Includes unstable entries, committed entries, snapshots and the
        /// full current membership. Denial must happen before persistence,
        /// configuration application or emission of an acknowledgement.
        check: *const fn (*anyopaque, core.Status, Check, bool) anyerror!void,
        /// Optional nonmutating admission for inbound messages. Only Append
        /// may return a shorter entry prefix, allowing committed predecessors
        /// to progress before a dependent prepare can be acknowledged. This
        /// callback performs the normal inbound check as well.
        inbound_prefix: ?*const fn (*anyopaque, core.Status, core.Message, bool) anyerror!usize = null,
        // The final flag permits NEW application obligations. False still
        // permits elections, accepted log apply and resolving existing debt;
        // the backing provider must classify payloads against owned identities.
        /// Called exactly once after an allowed proposal, including errors.
        /// Null indices are authoritative local nonacceptance; any accepted
        /// index retains pending debt regardless of the caller's result. The
        /// check must preallocate this bookkeeping; result cannot fail.
        proposal_result: *const fn (*anyopaque, core.Status, ProposalResult) void,
        /// Releases this runtime reference, not durable transaction debt.
        /// Backing retained by accepted work must survive runtime detach and
        /// be restored before a replacement runtime can allow its effects.
        detach: *const fn (*anyopaque) void,
        /// Applies only an already-owned canonical entry, using the pinned
        /// group owner installed before acknowledgement. No metadata lookup.
        apply_accepted: ?*const fn (*anyopaque, u64, u64, []const u8) anyerror!void = null,
        progress: ?*const fn (*anyopaque) anyerror!?Progress = null,
        owns_accepted: ?*const fn (*anyopaque, u64, u64, []const u8) anyerror!bool = null,
        /// Reports a retained native owner, never a requested policy or a
        /// transient lookup. Legacy guards keep the ordinary projection path.
        has_backing: ?*const fn (*anyopaque) bool = null,
    };

    pub fn check(self: Guard, status: core.Status, event: Check) !void {
        try self.checkAdmission(status, event, true);
    }
    pub fn checkAdmission(self: Guard, status: core.Status, event: Check, new_work_allowed: bool) !void {
        try self.vtable.check(self.ptr, status, event, new_work_allowed);
    }
    pub fn proposalResult(self: Guard, status: core.Status, result: ProposalResult) void {
        self.vtable.proposal_result(self.ptr, status, result);
    }
    pub fn admitInbound(self: Guard, status: core.Status, message: core.Message, new_work_allowed: bool) !usize {
        const count = if (self.vtable.inbound_prefix) |callback|
            try callback(self.ptr, status, message, new_work_allowed)
        else blk: {
            try self.checkAdmission(status, .{ .inbound = message }, new_work_allowed);
            break :blk message.entries.len;
        };
        if (count > message.entries.len or (count != message.entries.len and message.msg_type != .append_entries))
            return error.CompletionAdmissionUnavailable;
        return count;
    }
    pub fn applyAccepted(self: Guard, term: u64, index: u64, canonical: []const u8) !void {
        const apply = self.vtable.apply_accepted orelse return error.CompletionAdmissionUnavailable;
        try apply(self.ptr, term, index, canonical);
    }
    pub fn detach(self: Guard) void {
        self.vtable.detach(self.ptr);
    }
    pub fn progress(self: Guard) !?Progress {
        const callback = self.vtable.progress orelse return null;
        return try callback(self.ptr);
    }
    pub fn ownsAccepted(self: Guard, term: u64, index: u64, payload: []const u8) !bool {
        const callback = self.vtable.owns_accepted orelse return false;
        return try callback(self.ptr, term, index, payload);
    }
    pub fn hasBacking(self: Guard) bool {
        const callback = self.vtable.has_backing orelse return false;
        return callback(self.ptr);
    }
};

pub const Provider = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    pub const VTable = struct {
        /// Called before group construction/transport publication. Returns one
        /// owned reference, also for initially legacy groups: subsequent policy
        /// activation must be checked by the same guard. Attaching a guard is
        /// not a resource attestation or permission to vote.
        attach: *const fn (*anyopaque, core.types.GroupId, core.types.NodeId, core.Storage) anyerror!Guard,
        /// Queries already installed native ownership before descriptor/group
        /// publication. Must not create a pool or infer readiness from config.
        restored_progress: ?*const fn (*anyopaque, u64, u64) anyerror!?Progress = null,
        reconcile_durable: ?*const fn (*anyopaque, u64, u64, DurableLog) anyerror!void = null,
    };
    pub fn attach(self: Provider, group_id: core.types.GroupId, node_id: core.types.NodeId, storage: core.Storage) !Guard {
        return try self.vtable.attach(self.ptr, group_id, node_id, storage);
    }
    pub fn restoredProgress(self: Provider, group_id: u64, node_id: u64) !?Progress {
        const callback = self.vtable.restored_progress orelse return null;
        return try callback(self.ptr, group_id, node_id);
    }
    pub fn reconcileDurable(self: Provider, group_id: u64, node_id: u64, log: DurableLog) !void {
        const callback = self.vtable.reconcile_durable orelse return;
        try callback(self.ptr, group_id, node_id, log);
    }
};
