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
    pub fn detach(self: Guard) void {
        self.vtable.detach(self.ptr);
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
    };
    pub fn attach(self: Provider, group_id: core.types.GroupId, node_id: core.types.NodeId, storage: core.Storage) !Guard {
        return try self.vtable.attach(self.ptr, group_id, node_id, storage);
    }
};
