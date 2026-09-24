// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! DATA-local Raft callbacks around an owned native completion-pool lease.
//! Native errors cross only as stable Status. Normalization borrows payloads
//! synchronously and uses bounded stack arrays, never a request allocator.
const std = @import("std");
const raft = @import("raft_engine");
const core = raft.core;
const guard_iface = raft.runtime.completion_admission_iface;
const abi = @import("kernel_owner_abi").completion_pool;
const error_identity = @import("../storage/kernel_owner_client.zig");

pub const Bridge = struct {
    alloc: std.mem.Allocator,
    lease: abi.Lease,
    storage: ?core.Storage = null,

    /// Ownership transfers only on success. The native issuer must already
    /// have installed actual resources; this validates framing, not backing.
    pub fn create(alloc: std.mem.Allocator, lease: abi.Lease, group_id: u64, node_id: u64) !guard_iface.Guard {
        try validateIdentity(lease.identity, group_id, node_id);
        const self = try alloc.create(Bridge);
        self.* = .{ .alloc = alloc, .lease = lease };
        return .{ .ptr = self, .vtable = &.{ .check = check, .inbound_prefix = admitInbound, .proposal_result = proposalResult, .apply_accepted = applyAccepted, .progress = progress, .owns_accepted = ownsAccepted, .detach = detach } };
    }
    pub fn validateIdentity(id: abi.Identity, group_id: u64, node_id: u64) !void {
        if (id.version != abi.pool_abi_version or id.protocol != 1 or id.profile != 1 or
            id.group_id != group_id or id.node_id != node_id or group_id == 0 or node_id == 0 or
            id.capacity == 0 or id.capacity > abi.max_entries or id.generation == 0 or
            std.mem.allEqual(u8, &id.incarnation, 0)) return error.CompletionAdmissionUnavailable;
    }

    pub fn createWithStorage(alloc: std.mem.Allocator, lease: abi.Lease, group_id: u64, node_id: u64, storage: core.Storage) !guard_iface.Guard {
        const guard = try create(alloc, lease, group_id, node_id);
        const self: *Bridge = @ptrCast(@alignCast(guard.ptr));
        self.storage = storage;
        return guard;
    }

    fn nativeState(self: *Bridge, status: core.Status) abi.State {
        var result = state(status);
        if (status.applied_index == 0) {
            result.applied_term_known = 1;
        } else if (self.storage) |storage| {
            result.applied_term = storage.term(status.applied_index) catch return result;
            result.applied_term_known = 1;
        }
        return result;
    }

    fn check(ptr: *anyopaque, status: core.Status, event: guard_iface.Check, new_work_allowed: bool) !void {
        const self: *Bridge = @ptrCast(@alignCast(ptr));
        const result = try self.invoke(status, event, new_work_allowed);
        if (result.has_append_prefix != 0) return error.CompletionAdmissionUnavailable;
    }

    fn admitInbound(ptr: *anyopaque, status: core.Status, message: core.Message, new_work_allowed: bool) !usize {
        const self: *Bridge = @ptrCast(@alignCast(ptr));
        const result = try self.invoke(status, .{ .inbound = message }, new_work_allowed);
        return if (result.has_append_prefix != 0) @intCast(result.append_prefix) else message.entries.len;
    }

    fn invoke(self: *Bridge, status: core.Status, event: guard_iface.Check, new_work_allowed: bool) !abi.CheckResult {
        if (status.group_id != self.lease.identity.group_id or status.id != self.lease.identity.node_id)
            return error.CompletionAdmissionUnavailable;
        var normalized: Normalized = undefined;
        var request = try normalized.build(status, event, new_work_allowed);
        request.state = self.nativeState(status);
        var result: abi.CheckResult = .{};
        try error_identity.statusToError(self.lease.vtable.check(self.lease.context, &request, &result));
        if (result.version != abi.pool_abi_version or result.has_append_prefix > 1 or
            !std.mem.allEqual(u8, &result.reserved, 0)) return error.CompletionAdmissionUnavailable;
        if (result.has_append_prefix != 0) {
            if (event != .inbound or event.inbound.msg_type != .append_entries or result.append_prefix > event.inbound.entries.len)
                return error.CompletionAdmissionUnavailable;
        }
        return result;
    }

    fn proposalResult(ptr: *anyopaque, status: core.Status, result: guard_iface.ProposalResult) void {
        const self: *Bridge = @ptrCast(@alignCast(ptr));
        // The fallible proposal check has validated this exact borrowed batch.
        // No native call can mutate Raft between its check and finalization.
        std.debug.assert(result.payloads.len <= abi.max_entries);
        var payloads: [abi.max_entries]abi.Bytes = undefined;
        for (result.payloads, 0..) |payload, i| payloads[i] = bytes(payload);
        const request: abi.ProposalResult = .{
            .state = self.nativeState(status),
            .first_index = result.first_index orelse 0,
            .last_index = result.last_index orelse 0,
            .payloads = .{ .ptr = if (result.payloads.len == 0) null else &payloads, .len = result.payloads.len },
        };
        self.lease.vtable.proposal_result(self.lease.context, &request);
    }

    fn applyAccepted(ptr: *anyopaque, term: u64, index: u64, canonical: []const u8) !void {
        const self: *Bridge = @ptrCast(@alignCast(ptr));
        if (canonical.len > abi.max_check_payload_bytes or term == 0 or index == 0) return error.CompletionAdmissionUnavailable;
        const apply = self.lease.vtable.apply_accepted orelse return error.CompletionAdmissionUnavailable;
        try error_identity.statusToError(apply(self.lease.context, term, index, bytes(canonical)));
    }

    fn detach(ptr: *anyopaque) void {
        const self: *Bridge = @ptrCast(@alignCast(ptr));
        self.lease.vtable.release(self.lease.context);
        self.alloc.destroy(self);
    }
    fn progress(ptr: *anyopaque) !?guard_iface.Progress {
        const self: *Bridge = @ptrCast(@alignCast(ptr));
        return try leaseProgress(self.lease);
    }
    pub fn leaseProgress(lease: abi.Lease) !?guard_iface.Progress {
        const callback = lease.vtable.progress orelse return null;
        var value: abi.Progress = .{};
        const status = callback(lease.context, &value);
        if (status == .not_found) return null;
        try error_identity.statusToError(status);
        if (value.term == 0 or value.index == 0) return error.CompletionAdmissionUnavailable;
        return .{ .term = value.term, .index = value.index, .payload_digest = value.payload_digest };
    }
    pub fn reconcileLease(lease: abi.Lease, log: guard_iface.DurableLog) !void {
        const proof = try documentProof(lease, log);
        const reconcile = lease.vtable.reconcile_durable orelse return error.CompletionAdmissionUnavailable;
        try error_identity.statusToError(reconcile(lease.context, &proof));
    }
    fn documentProof(lease: abi.Lease, log: guard_iface.DurableLog) !abi.DurableLog {
        if (!log.durability_confirmed or log.commit_index > log.last_index or log.compacted_index > log.commit_index or
            (log.compacted_index == 0) != (log.compacted_term == 0) or
            (log.replacement_first == 0) != (log.replacement_last == 0) or
            log.replacement_first > log.replacement_last or log.replacement_last > log.last_index)
            return error.CompletionAdmissionUnavailable;
        const enumerate = lease.vtable.durable_cells orelse return error.CompletionAdmissionUnavailable;
        var cells: abi.DurableCells = .{};
        try error_identity.statusToError(enumerate(lease.context, &cells));
        if (cells.version != abi.pool_abi_version or cells.count > abi.max_durable_cells or
            cells.startup_reconciliation_pending > 1 or !std.mem.allEqual(u8, &cells.reserved, 0))
            return error.CompletionAdmissionUnavailable;
        // Even a fresh empty installation must receive the direct-WAL
        // frontier before its owner can qualify startup. Live non-restored
        // cells cannot be reclassified as a process-death recovery image.
        if (log.mode == .startup_complete and cells.startup_reconciliation_pending == 0 and cells.count != 0)
            return error.CompletionAdmissionUnavailable;
        var proof: abi.DurableLog = .{
            .mode = if (log.mode == .startup_complete) .startup_complete else .persisted_replacement,
            .compacted_index = log.compacted_index,
            .compacted_term = log.compacted_term,
            .last_index = log.last_index,
            .commit_index = log.commit_index,
            .count = cells.count,
        };
        for (cells.cells[0..cells.count], 0..) |cell, i| {
            if (cell.prepared > 1 or !std.mem.allEqual(u8, &cell.reserved, 0) or cell.identity.term == 0 or cell.identity.index == 0)
                return error.CompletionAdmissionUnavailable;
            for (cells.cells[0..i]) |prior| if (prior.identity.index == cell.identity.index) return error.CompletionAdmissionUnavailable;
            var observation: abi.DurableObservation = .{ .expected = cell.identity };
            if (cell.identity.index > log.compacted_index and cell.identity.index <= log.last_index) {
                if (log.entries.len == 0) return error.CompletionAdmissionUnavailable;
                const first = log.entries[0].index;
                if (cell.identity.index < first or cell.identity.index - first >= log.entries.len)
                    return error.CompletionAdmissionUnavailable;
                const actual = log.entries[@intCast(cell.identity.index - first)];
                if (actual.index != cell.identity.index or actual.term == 0) return error.CompletionAdmissionUnavailable;
                observation.present = 1;
                observation.observed_term = actual.term;
                std.crypto.hash.sha2.Sha256.hash(actual.data, &observation.observed_digest, .{});
                observation.replaced_in_this_persist = @intFromBool(log.mode == .persisted_replacement and
                    log.replacement_first != 0 and actual.index >= log.replacement_first and actual.index <= log.replacement_last);
            }
            proof.observations[i] = observation;
        }
        return proof;
    }
    /// Separately negotiated v2 proof for retained BEGINs. An absent issuer
    /// cannot be represented as an empty inventory. A compacted BEGIN needs a
    /// native durable receipt, which this stage deliberately does not infer.
    pub fn reconcileControlProviderV2(provider: ?abi.ControlProviderV2, document_lease: abi.Lease, group_id: u64, node_id: u64, log: guard_iface.DurableLog) !void {
        const issuer = provider orelse return error.CompletionAdmissionUnavailable;
        var lease: abi.ControlLeaseV2 = undefined;
        const result = issuer.acquire(issuer.context, group_id, node_id, &lease);
        if (result == .not_found) return error.CompletionAdmissionUnavailable;
        try error_identity.statusToError(result);
        defer lease.vtable.release(lease.context);
        try validateIdentity(lease.identity, group_id, node_id);
        try reconcileControlLeaseV2(lease, document_lease, log);
    }
    pub fn reconcileControlLeaseV2(lease: abi.ControlLeaseV2, document_lease: abi.Lease, log: guard_iface.DurableLog) !void {
        if (!log.durability_confirmed or log.commit_index > log.last_index or log.compacted_index > log.commit_index or
            (log.compacted_index == 0) != (log.compacted_term == 0) or
            (log.replacement_first == 0) != (log.replacement_last == 0) or
            log.replacement_first > log.replacement_last or log.replacement_last > log.last_index)
            return error.CompletionAdmissionUnavailable;
        var owners: abi.ControlDurableOwnersV2 = .{};
        try error_identity.statusToError(lease.vtable.durable_owners(lease.context, &owners));
        if (owners.version != abi.control_proof_abi_version or owners.count > abi.max_durable_controls or owners.reserved != 0)
            return error.CompletionAdmissionUnavailable;
        var proof: abi.ControlDurableLogV2 = .{
            .mode = if (log.mode == .startup_complete) .startup_complete else .persisted_replacement,
            .compacted_index = log.compacted_index,
            .compacted_term = log.compacted_term,
            .last_index = log.last_index,
            .commit_index = log.commit_index,
            .count = owners.count,
            .document = try documentProof(document_lease, log),
        };
        for (owners.owners[0..owners.count], 0..) |owner, i| {
            const identity = owner.identity;
            if (owner.slot_index >= abi.max_durable_controls or owner.reserved != 0 or
                identity.term == 0 or identity.index == 0 or std.mem.allEqual(u8, &identity.payload_digest, 0) or
                identity.index <= log.compacted_index) return error.CompletionAdmissionUnavailable;
            for (owners.owners[0..i]) |prior| if (prior.slot_index == owner.slot_index or
                prior.identity.index == identity.index) return error.CompletionAdmissionUnavailable;
            var observation: abi.DurableObservation = .{ .expected = identity };
            if (identity.index <= log.last_index) {
                if (log.entries.len == 0) return error.CompletionAdmissionUnavailable;
                const first = log.entries[0].index;
                if (identity.index < first or identity.index - first >= log.entries.len)
                    return error.CompletionAdmissionUnavailable;
                const actual = log.entries[@intCast(identity.index - first)];
                if (actual.index != identity.index or actual.term == 0) return error.CompletionAdmissionUnavailable;
                observation.present = 1;
                observation.observed_term = actual.term;
                std.crypto.hash.sha2.Sha256.hash(actual.data, &observation.observed_digest, .{});
                observation.replaced_in_this_persist = @intFromBool(log.mode == .persisted_replacement and
                    log.replacement_first != 0 and actual.index >= log.replacement_first and actual.index <= log.replacement_last);
            }
            proof.observations[i] = observation;
        }
        try error_identity.statusToError(lease.vtable.reconcile_durable(lease.context, &proof));
    }
    fn ownsAccepted(ptr: *anyopaque, term: u64, index: u64, payload: []const u8) !bool {
        const self: *Bridge = @ptrCast(@alignCast(ptr));
        if (payload.len > abi.max_check_payload_bytes or term == 0 or index == 0) return error.CompletionAdmissionUnavailable;
        const callback = self.lease.vtable.owns_accepted orelse return false;
        var owned: u8 = 0;
        try error_identity.statusToError(callback(self.lease.context, term, index, bytes(payload), &owned));
        if (owned > 1) return error.CompletionAdmissionUnavailable;
        return owned == 1;
    }
};

fn bytes(value: []const u8) abi.Bytes {
    return .{ .ptr = if (value.len == 0) null else value.ptr, .len = value.len };
}
fn nodes(value: []const u64) abi.NodeIds {
    return .{ .ptr = if (value.len == 0) null else value.ptr, .len = value.len };
}
fn membership(value: core.types.ConfState) abi.Membership {
    return .{ .voters = nodes(value.voters), .outgoing = nodes(value.voters_outgoing), .learners = nodes(value.learners), .learners_next = nodes(value.learners_next), .auto_leave = @intFromBool(value.auto_leave) };
}
fn validateMembership(value: core.types.ConfState) !void {
    var count: usize = 0;
    for ([_][]const u64{ value.voters, value.voters_outgoing, value.learners, value.learners_next }) |ids| {
        count = std.math.add(usize, count, ids.len) catch return error.CompletionAdmissionUnavailable;
    }
    if (count > abi.max_members) return error.CompletionAdmissionUnavailable;
}
fn state(value: core.Status) abi.State {
    return .{ .term = value.hard.current_term, .commit_index = value.hard.commit_index, .applied_index = value.applied_index, .last_index = value.last_index, .leader_id = value.soft.leader_id orelse 0, .membership = membership(value.conf_state) };
}
fn entryKind(value: core.types.EntryType) abi.EntryKind {
    return switch (value) {
        .normal => .normal,
        .conf_change => .configuration_v1,
        .conf_change_v2 => .configuration_v2,
    };
}
fn changeKind(value: core.types.ConfChangeType) abi.ChangeKind {
    return switch (value) {
        .add_node => .add_voter,
        .add_learner_node => .add_learner,
        .remove_node => .remove,
    };
}
fn messageKind(value: core.message.MessageType) abi.MessageKind {
    return switch (value) {
        inline else => |tag| @field(abi.MessageKind, @tagName(tag)),
    };
}

const Normalized = struct {
    entries: [abi.max_entries]abi.Entry,
    committed: [abi.max_entries]abi.Entry,
    payloads: [abi.max_entries]abi.Bytes,
    changes: [abi.max_members]abi.Change,
    payload_bytes: usize,

    fn charge(self: *Normalized, len: usize) !void {
        self.payload_bytes = std.math.add(usize, self.payload_bytes, len) catch return error.CompletionAdmissionUnavailable;
        if (self.payload_bytes > abi.max_check_payload_bytes) return error.CompletionAdmissionUnavailable;
    }
    fn normalizeEntries(self: *Normalized, source: []const core.Entry, target: *[abi.max_entries]abi.Entry) !abi.Entries {
        if (source.len > target.len) return error.CompletionAdmissionUnavailable;
        for (source, 0..) |entry, i| {
            try self.charge(entry.data.len);
            target[i] = .{ .term = entry.term, .index = entry.index, .kind = entryKind(entry.entry_type), .payload = bytes(entry.data) };
        }
        return .{ .ptr = if (source.len == 0) null else target, .len = source.len };
    }
    fn normalizeSnapshot(value: ?core.types.Snapshot) !abi.Snapshot {
        const snapshot = value orelse return .{};
        try validateMembership(snapshot.metadata.conf_state);
        return .{ .present = 1, .term = snapshot.metadata.term, .index = snapshot.metadata.index, .membership = membership(snapshot.metadata.conf_state), .data = bytes(snapshot.data) };
    }
    fn normalizeMessage(self: *Normalized, request: *abi.Check, message: core.Message) !void {
        request.message_kind = messageKind(message.msg_type);
        request.from = message.from;
        request.to = message.to;
        request.message_term = message.term;
        request.previous_index = message.log_index;
        request.previous_term = message.log_term;
        request.message_commit_index = message.commit_index;
        request.vote = message.vote orelse 0;
        request.reject_hint = message.reject_hint;
        try self.charge(message.context.len);
        request.context = bytes(message.context);
        request.rejected = @intFromBool(message.reject);
        request.entries = try self.normalizeEntries(message.entries, &self.entries);
        request.snapshot = try normalizeSnapshot(message.snapshot);
    }
    fn build(self: *Normalized, status: core.Status, event: guard_iface.Check, new_work_allowed: bool) !abi.Check {
        self.payload_bytes = 0;
        try validateMembership(status.conf_state);
        var request: abi.Check = .{ .kind = .campaign, .state = state(status), .new_work_allowed = @intFromBool(new_work_allowed) };
        switch (event) {
            .campaign => {},
            .snapshot_admission => request.kind = .snapshot_admission,
            .inbound => |message| {
                request.kind = .inbound;
                try self.normalizeMessage(&request, message);
            },
            .storage_ack => |message| {
                request.kind = .storage_ack;
                try self.normalizeMessage(&request, message);
            },
            .proposal => |payloads| {
                request.kind = .proposal;
                if (payloads.len > self.payloads.len) return error.CompletionAdmissionUnavailable;
                for (payloads, 0..) |payload, i| {
                    try self.charge(payload.len);
                    self.payloads[i] = bytes(payload);
                }
                request.proposals = .{ .ptr = if (payloads.len == 0) null else &self.payloads, .len = payloads.len };
            },
            .configuration_v1 => |change| {
                request.kind = .configuration;
                self.changes[0] = .{ .kind = changeKind(change.change_type), .node_id = change.node_id };
                request.changes = .{ .ptr = &self.changes, .len = 1 };
            },
            .configuration => |change| {
                request.kind = .configuration;
                try self.charge(change.context.len);
                request.context = bytes(change.context);
                if (change.changes.len > self.changes.len) return error.CompletionAdmissionUnavailable;
                for (change.changes, 0..) |item, i| self.changes[i] = .{ .kind = changeKind(item.change_type), .node_id = item.node_id };
                request.changes = .{ .ptr = if (change.changes.len == 0) null else &self.changes, .len = change.changes.len };
                request.transition = switch (change.transition) {
                    .auto => .automatic,
                    .joint_explicit => .joint_explicit,
                    .joint_implicit => .joint_implicit,
                };
            },
            .ready => |ready| {
                request.kind = .ready;
                if (ready.entries.len +| ready.committed_entries.len > abi.max_entries) return error.CompletionAdmissionUnavailable;
                request.entries = try self.normalizeEntries(ready.entries, &self.entries);
                request.committed_entries = try self.normalizeEntries(ready.committed_entries, &self.committed);
                request.snapshot = try normalizeSnapshot(ready.snapshot);
            },
        }
        return request;
    }
};

pub const implementation_tests = implementationTests();
fn implementationTests() type {
    if (!(@import("builtin").is_test and !@import("storage_source_options").control_only)) return struct {};
    const Suite = struct {
        test "workload admission data completion bridge borrows bounded frames and owns native lease" {
            const Fake = struct {
                checks: usize = 0,
                results: usize = 0,
                releases: usize = 0,
                reject: bool = true,
                reply: abi.CheckResult = .{},
                payload_ptr: ?[*]const u8 = null,
                observed_borrow: bool = false,
                observed_recovery: bool = false,
                observed_index: u64 = 0,
                owned_reply: u8 = 1,
                durable: abi.Progress = .{ .term = 7, .index = 11, .payload_digest = @splat(3) },
                applies: usize = 0,

                fn check(ptr: ?*anyopaque, request: *const abi.Check, result_out: *abi.CheckResult) callconv(.c) @import("kernel_owner_abi").Status {
                    const self: *@This() = @ptrCast(@alignCast(ptr.?));
                    self.checks += 1;
                    result_out.* = self.reply;
                    if (request.version != abi.pool_abi_version) return .internal;
                    self.observed_recovery = request.new_work_allowed == 0;
                    if (request.entries.len != 0) {
                        self.observed_borrow = request.entries.ptr.?[0].payload.ptr == self.payload_ptr;
                        self.observed_index = request.entries.ptr.?[0].index;
                    }
                    return if (self.reject) .completion_admission_unavailable else .ok;
                }
                fn result(ptr: ?*anyopaque, request: *const abi.ProposalResult) callconv(.c) void {
                    const self: *@This() = @ptrCast(@alignCast(ptr.?));
                    self.results += 1;
                    self.observed_index = request.last_index;
                }
                fn release(ptr: ?*anyopaque) callconv(.c) void {
                    const self: *@This() = @ptrCast(@alignCast(ptr.?));
                    self.releases += 1;
                }
                fn progress(ptr: ?*anyopaque, output: *abi.Progress) callconv(.c) @import("kernel_owner_abi").Status {
                    const self: *@This() = @ptrCast(@alignCast(ptr.?));
                    output.* = self.durable;
                    return .ok;
                }
                fn owns(ptr: ?*anyopaque, term: u64, index: u64, payload: abi.Bytes, output: *u8) callconv(.c) @import("kernel_owner_abi").Status {
                    const self: *@This() = @ptrCast(@alignCast(ptr.?));
                    if (term != 7 or index != 11 or payload.ptr != self.payload_ptr) return .completion_admission_unavailable;
                    output.* = self.owned_reply;
                    return .ok;
                }
                fn apply(ptr: ?*anyopaque, term: u64, index: u64, payload: abi.Bytes) callconv(.c) @import("kernel_owner_abi").Status {
                    const self: *@This() = @ptrCast(@alignCast(ptr.?));
                    if (term != 7 or index != 11 or payload.ptr != self.payload_ptr) return .completion_admission_unavailable;
                    self.applies += 1;
                    return .ok;
                }
            };
            var fake = Fake{};
            const lease: abi.Lease = .{
                .identity = .{ .group_id = 19, .node_id = 2, .capacity = 4, .generation = 1, .incarnation = @splat(9) },
                .context = &fake,
                .vtable = &.{ .check = Fake.check, .proposal_result = Fake.result, .release = Fake.release, .progress = Fake.progress, .owns_accepted = Fake.owns, .apply_accepted = Fake.apply },
            };
            var invalid = lease;
            invalid.identity.version += 1;
            try std.testing.expectError(error.CompletionAdmissionUnavailable, Bridge.create(std.testing.allocator, invalid, 19, 2));
            try std.testing.expectEqual(@as(usize, 0), fake.releases);
            var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
            const guard = try Bridge.create(failing.allocator(), lease, 19, 2);
            var detached = false;
            defer if (!detached) guard.detach();
            const status: core.Status = .{ .id = 2, .group_id = 19, .soft = .{ .role = .follower, .leader_id = 1 }, .hard = .{ .current_term = 7, .commit_index = 10 }, .applied_index = 10, .last_index = 11, .conf_state = .{} };
            var payload = [_]u8{ 3, 4, 5 };
            fake.payload_ptr = &payload;
            try std.testing.expect(try guard.ownsAccepted(7, 11, &payload));
            fake.owned_reply = 2;
            try std.testing.expectError(error.CompletionAdmissionUnavailable, guard.ownsAccepted(7, 11, &payload));
            fake.owned_reply = 0;
            try std.testing.expect(!try guard.ownsAccepted(7, 11, &payload));
            try std.testing.expectEqual(@as(u64, 11), (try guard.progress()).?.index);
            fake.durable.term = 0;
            try std.testing.expectError(error.CompletionAdmissionUnavailable, guard.progress());
            fake.durable.term = 7;
            try guard.applyAccepted(7, 11, &payload);
            try std.testing.expectEqual(@as(usize, 1), fake.applies);
            var entries = [_]core.Entry{.{ .term = 7, .index = 11, .data = &payload }};
            try std.testing.expectError(error.CompletionAdmissionUnavailable, guard.check(status, .{ .ready = .{ .entries = &entries } }));
            fake.reject = false;
            try guard.checkAdmission(status, .{ .ready = .{ .entries = &entries } }, false);
            try std.testing.expect(fake.observed_borrow and fake.observed_recovery);
            try std.testing.expectEqual(@as(u64, 11), fake.observed_index);
            const append: core.Message = .{ .msg_type = .append_entries, .from = 1, .to = 2, .term = 7, .log_index = 10, .log_term = 7, .entries = &entries };
            fake.reply = .{ .has_append_prefix = 1, .append_prefix = 0 };
            try std.testing.expectEqual(@as(usize, 0), try guard.admitInbound(status, append, true));
            try std.testing.expectError(error.CompletionAdmissionUnavailable, guard.check(status, .{ .ready = .{ .entries = &entries } }));
            fake.reply.append_prefix = 2;
            try std.testing.expectError(error.CompletionAdmissionUnavailable, guard.admitInbound(status, append, true));
            fake.reply = .{ .version = 2 };
            try std.testing.expectError(error.CompletionAdmissionUnavailable, guard.admitInbound(status, append, true));
            fake.reply = .{};
            const checks = fake.checks;
            const too_many = [_][]const u8{&payload} ** (abi.max_entries + 1);
            try std.testing.expectError(error.CompletionAdmissionUnavailable, guard.check(status, .{ .proposal = &too_many }));
            const one_mib = try std.testing.allocator.alloc(u8, 1024 * 1024);
            defer std.testing.allocator.free(one_mib);
            try std.testing.expectError(error.CompletionAdmissionUnavailable, guard.check(status, .{ .proposal = &.{ one_mib, one_mib, one_mib, one_mib, one_mib } }));
            try std.testing.expectEqual(checks, fake.checks);
            try guard.check(status, .{ .proposal = &.{&payload} });
            guard.proposalResult(status, .{ .payloads = &.{&payload}, .first_index = 12, .last_index = 12 });
            try std.testing.expectEqual(@as(usize, 1), fake.results);
            try std.testing.expectEqual(@as(u64, 12), fake.observed_index);
            try std.testing.expectEqual(@as(usize, 1), failing.alloc_index);
            guard.detach();
            detached = true;
            try std.testing.expectEqual(@as(usize, 1), fake.releases);
        }

        test "workload admission data completion reconciliation uses only complete durable evidence" {
            const Fake = struct {
                cells: abi.DurableCells = .{},
                calls: usize = 0,
                observed: abi.DurableLog = .{ .mode = .startup_complete },
                fn check(_: ?*anyopaque, _: *const abi.Check, _: *abi.CheckResult) callconv(.c) @import("kernel_owner_abi").Status {
                    return .ok;
                }
                fn result(_: ?*anyopaque, _: *const abi.ProposalResult) callconv(.c) void {}
                fn release(_: ?*anyopaque) callconv(.c) void {}
                fn enumerate(ptr: ?*anyopaque, out: *abi.DurableCells) callconv(.c) @import("kernel_owner_abi").Status {
                    const self: *@This() = @ptrCast(@alignCast(ptr.?));
                    out.* = self.cells;
                    return .ok;
                }
                fn reconcile(ptr: ?*anyopaque, proof: *const abi.DurableLog) callconv(.c) @import("kernel_owner_abi").Status {
                    const self: *@This() = @ptrCast(@alignCast(ptr.?));
                    self.calls += 1;
                    self.observed = proof.*;
                    return .ok;
                }
            };
            var fake = Fake{};
            const lease: abi.Lease = .{ .identity = .{}, .context = &fake, .vtable = &.{ .check = Fake.check, .proposal_result = Fake.result, .release = Fake.release, .durable_cells = Fake.enumerate, .reconcile_durable = Fake.reconcile } };
            var entries = [_]core.Entry{ .{ .index = 6, .term = 2, .data = @constCast("ordinary") }, .{ .index = 7, .term = 3, .data = @constCast("replacement") } };
            var log: guard_iface.DurableLog = .{ .mode = .startup_complete, .durability_confirmed = true, .compacted_index = 5, .compacted_term = 1, .last_index = 7, .commit_index = 6, .entries = &entries };
            // Fresh empty installation still receives the authoritative frontier.
            try Bridge.reconcileLease(lease, log);
            try std.testing.expectEqual(@as(usize, 1), fake.calls);
            // A restored pool still needs reconciliation even when native recovery has
            // already retired every materialized cell.
            fake.cells.startup_reconciliation_pending = 1;
            try Bridge.reconcileLease(lease, log);
            try std.testing.expectEqual(@as(usize, 2), fake.calls);
            fake.cells.count = 2;
            fake.cells.cells[0] = .{ .identity = .{ .term = 2, .index = 7, .payload_digest = @splat(4) } };
            fake.cells.cells[1] = .{ .identity = .{ .term = 2, .index = 8, .payload_digest = @splat(5) } };
            log.mode = .persisted_replacement;
            try Bridge.reconcileLease(lease, log);
            try std.testing.expectEqual(@as(u8, 1), fake.observed.observations[0].present);
            try std.testing.expectEqual(@as(u64, 3), fake.observed.observations[0].observed_term);
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(entries[1].data, &digest, .{});
            try std.testing.expectEqualSlices(u8, &digest, &fake.observed.observations[0].observed_digest);
            // A complete current suffix is not evidence that a queued proposal above
            // that suffix has been canceled, nor that an existing row was replaced.
            try std.testing.expectEqual(@as(u8, 0), fake.observed.observations[0].replaced_in_this_persist);
            try std.testing.expectEqual(@as(u8, 0), fake.observed.observations[1].present);
            log.replacement_first = 7;
            log.replacement_last = 7;
            try Bridge.reconcileLease(lease, log);
            try std.testing.expectEqual(@as(u8, 1), fake.observed.observations[0].replaced_in_this_persist);
            try std.testing.expectEqual(@as(u8, 0), fake.observed.observations[1].replaced_in_this_persist);
            const before = fake.calls;
            log.durability_confirmed = false;
            try std.testing.expectError(error.CompletionAdmissionUnavailable, Bridge.reconcileLease(lease, log));
            log.durability_confirmed = true;
            log.entries = entries[0..1];
            try std.testing.expectError(error.CompletionAdmissionUnavailable, Bridge.reconcileLease(lease, log));
            log.entries = &entries;
            fake.cells.cells[1].identity.index = 7;
            try std.testing.expectError(error.CompletionAdmissionUnavailable, Bridge.reconcileLease(lease, log));
            try std.testing.expectEqual(before, fake.calls);
        }

        test "workload admission retained control v2 observes four exact persisted BEGINs" {
            const Fake = struct {
                owners: abi.ControlDurableOwnersV2 = .{ .count = 4 },
                calls: usize = 0,
                proof: abi.ControlDurableLogV2 = .{ .mode = .startup_complete },
                fn check(_: ?*anyopaque, _: *const abi.Check, _: *abi.CheckResult) callconv(.c) @import("kernel_owner_abi").Status {
                    return .ok;
                }
                fn result(_: ?*anyopaque, _: *const abi.ProposalResult) callconv(.c) void {}
                fn release(_: ?*anyopaque) callconv(.c) void {}
                fn documentCells(_: ?*anyopaque, out: *abi.DurableCells) callconv(.c) @import("kernel_owner_abi").Status {
                    out.* = .{};
                    return .ok;
                }
                fn enumerate(raw: ?*anyopaque, out: *abi.ControlDurableOwnersV2) callconv(.c) @import("kernel_owner_abi").Status {
                    const self: *@This() = @ptrCast(@alignCast(raw.?));
                    out.* = self.owners;
                    return .ok;
                }
                fn reconcile(raw: ?*anyopaque, proof: *const abi.ControlDurableLogV2) callconv(.c) @import("kernel_owner_abi").Status {
                    const self: *@This() = @ptrCast(@alignCast(raw.?));
                    self.calls += 1;
                    self.proof = proof.*;
                    if (proof.document.version != abi.pool_abi_version or proof.document.count != 0 or
                        proof.document.last_index != proof.last_index or proof.document.commit_index != proof.commit_index)
                        return .completion_admission_unavailable;
                    for (proof.observations[0..proof.count]) |observation| {
                        if (observation.present == 0 or observation.observed_term != observation.expected.term or
                            !std.mem.eql(u8, &observation.observed_digest, &observation.expected.payload_digest))
                            return .completion_admission_unavailable;
                    }
                    return .ok;
                }
            };
            var fake = Fake{};
            var entries: [4]core.Entry = undefined;
            for (&entries, 0..) |*entry, i| {
                entry.* = .{ .index = i + 1, .term = 7, .data = @constCast("canonical-BEGIN") };
                var digest: [32]u8 = undefined;
                std.crypto.hash.sha2.Sha256.hash(entry.data, &digest, .{});
                fake.owners.owners[i] = .{ .identity = .{ .term = 7, .index = i + 1, .payload_digest = digest }, .slot_index = @intCast(i) };
            }
            const lease: abi.ControlLeaseV2 = .{ .identity = .{}, .context = &fake, .vtable = &.{ .release = Fake.release, .durable_owners = Fake.enumerate, .reconcile_durable = Fake.reconcile } };
            const document_lease: abi.Lease = .{ .identity = .{}, .context = &fake, .vtable = &.{ .check = Fake.check, .proposal_result = Fake.result, .release = Fake.release, .durable_cells = Fake.documentCells } };
            var log: guard_iface.DurableLog = .{ .mode = .persisted_replacement, .durability_confirmed = true, .compacted_index = 0, .compacted_term = 0, .last_index = 4, .commit_index = 4, .entries = &entries, .replacement_first = 2, .replacement_last = 3 };
            try std.testing.expectError(error.CompletionAdmissionUnavailable, Bridge.reconcileControlProviderV2(null, document_lease, 19, 2, log));
            try Bridge.reconcileControlLeaseV2(lease, document_lease, log);
            try std.testing.expectEqual(@as(usize, 1), fake.calls);
            try std.testing.expectEqual(@as(u8, 0), fake.proof.observations[0].replaced_in_this_persist);
            try std.testing.expectEqual(@as(u8, 1), fake.proof.observations[1].replaced_in_this_persist);
            try std.testing.expectEqual(@as(u8, 1), fake.proof.observations[2].replaced_in_this_persist);
            fake.owners.owners[1].identity.payload_digest = @splat(9);
            try std.testing.expectError(error.CompletionAdmissionUnavailable, Bridge.reconcileControlLeaseV2(lease, document_lease, log));
            fake.owners.owners[1].identity = fake.owners.owners[0].identity;
            try std.testing.expectError(error.CompletionAdmissionUnavailable, Bridge.reconcileControlLeaseV2(lease, document_lease, log));
            fake.owners.owners[1].identity.index = 2;
            log.compacted_index = 1;
            log.compacted_term = 7;
            try std.testing.expectError(error.CompletionAdmissionUnavailable, Bridge.reconcileControlLeaseV2(lease, document_lease, log));
            log.compacted_index = 0;
            log.compacted_term = 0;
            log.durability_confirmed = false;
            try std.testing.expectError(error.CompletionAdmissionUnavailable, Bridge.reconcileControlLeaseV2(lease, document_lease, log));
        }
    };
    return Suite;
}

comptime {
    if (@import("builtin").is_test) _ = implementation_tests;
}
