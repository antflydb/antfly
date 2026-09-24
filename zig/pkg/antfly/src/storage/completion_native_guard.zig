// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Native half of the checked consensus boundary. The outer C owner retains
//! the physical DB through every lease; callbacks use its heap-stable wrapper.
const std = @import("std");
const abi = @import("kernel_owner_abi").completion_pool;
const failure = @import("runtime_failure_abi");
const errors = @import("kernel_error_identity");
const native = @import("lsm_backend.zig");
const codec = @import("lsm_backend/completion_entry.zig");
const completion = @import("lsm_backend/completion_runtime.zig");
const backend_runtime = @import("lsm_backend/runtime.zig");

// Table activation also promises that transaction begin/decision/ack and
// ordinary writes have pre-consensus capacity. The retained prepare/resolve
// lane alone is not sufficient evidence for activating that policy.
const ordinary_mutations_supported = @import("../common/durable_completion_policy.zig").replicated_activation_supported;

pub const State = struct {
    proposals: [4]?completion.AcceptedIdentity = @splat(null),
    /// Only the direct WAL provider's complete persisted suffix proof can set
    /// this. A volatile Ready/last_index or configuration flag never can.
    durable_log_reconciled: bool = false,
};

fn bytes(value: abi.Bytes) ![]const u8 {
    if (value.len == 0) return &.{};
    if (value.len > abi.max_check_payload_bytes or value.ptr == null) return error.InvalidArgument;
    return value.ptr.?[0..@intCast(value.len)];
}
fn items(comptime T: type, ptr: ?[*]const T, len: u64) ![]const T {
    if (len == 0) return &.{};
    if (len > abi.max_entries or ptr == null) return error.InvalidArgument;
    return ptr.?[0..@intCast(len)];
}

pub fn Guard(comptime DB: type) type {
    return struct {
        fn db(raw: ?*anyopaque) *DB {
            return @ptrCast(@alignCast(raw.?));
        }
        fn backend(owner: *DB) !*native.Backend {
            if (owner.durable_completion_authority != .raft_apply) return error.CompletionAdmissionUnavailable;
            return owner.core.primary_store_owner.lsmBackend() orelse error.UnsupportedCompletionBackend;
        }
        fn pool(owner: *DB) !*native.Backend.CompletionPool {
            const value = (try backend(owner)).completion_pool orelse return error.CompletionAdmissionUnavailable;
            if (value.failed or !value.restored or value.publication_owner == null) return error.RecoveryRequired;
            return value;
        }
        pub fn lease(owner: *DB, group: u64, node: u64) !abi.Lease {
            owner.core.lockApply();
            defer owner.core.unlockApply();
            const b = try backend(owner);
            const locked = backend_runtime.lockBackend(native.Backend, b);
            defer backend_runtime.unlockBackend(native.Backend, b, locked);
            const p = try pool(owner);
            if (p.config.identity.group_id != group or p.config.identity.node_id != node) return error.InvalidArgument;
            return .{ .identity = p.config.identity, .context = owner, .vtable = &vtable };
        }
        pub fn controlLeaseV2(owner: *DB, group: u64, node: u64) !abi.ControlLeaseV2 {
            owner.core.lockApply();
            defer owner.core.unlockApply();
            const b = try backend(owner);
            const locked = backend_runtime.lockBackend(native.Backend, b);
            defer backend_runtime.unlockBackend(native.Backend, b, locked);
            const p = try pool(owner);
            if (p.config.identity.group_id != group or p.config.identity.node_id != node) return error.InvalidArgument;
            return .{ .identity = p.config.identity, .context = owner, .vtable = &control_vtable_v2 };
        }
        pub fn attest(owner: *DB, group: u64, node: u64) !abi.NativeAttestation {
            owner.core.lockApply();
            defer owner.core.unlockApply();
            const b = try backend(owner);
            const locked = backend_runtime.lockBackend(native.Backend, b);
            defer backend_runtime.unlockBackend(native.Backend, b, locked);
            const p = try pool(owner);
            if (p.config.identity.group_id != group or p.config.identity.node_id != node) return error.InvalidArgument;
            if (!ordinary_mutations_supported or !p.ready or !owner.async_context.completion_guard.durable_log_reconciled) return error.CompletionAdmissionUnavailable;
            var result: abi.NativeAttestation = .{ .identity = p.config.identity };
            for (p.cells[0..p.cell_count]) |cell| switch (cell.phase) {
                .accepted => result.accepted_count += 1,
                .prepared => result.prepared_count += 1,
                else => {},
            };
            return result;
        }
        fn check(raw: ?*anyopaque, input: *const abi.Check, output: *abi.CheckResult) callconv(.c) failure.Status {
            if (raw == null) return .invalid_argument;
            checkImpl(db(raw), input, output) catch |err| return errors.statusFromError(err);
            return .ok;
        }
        fn checkImpl(owner: *DB, input: *const abi.Check, output: *abi.CheckResult) !void {
            if (input.version != abi.pool_abi_version or input.new_work_allowed > 1) return error.InvalidArgument;
            if (!owner.core.tryLockApplyExclusive()) return error.CompletionReservationBusy;
            defer owner.core.unlockApply();
            const b = try backend(owner);
            const locked = backend_runtime.lockBackend(native.Backend, b);
            defer backend_runtime.unlockBackend(native.Backend, b, locked);
            const p = try pool(owner);
            output.* = .{};
            switch (input.kind) {
                .configuration, .snapshot_admission => return error.PreparedCompletionActive,
                .campaign => {
                    if (!owner.async_context.completion_guard.durable_log_reconciled) return error.CompletionAdmissionUnavailable;
                },
                .inbound => {
                    if (input.snapshot.present != 0) return error.PreparedCompletionActive;
                    if (input.message_kind != .append_entries) return;
                    const entries = try items(abi.Entry, input.entries.ptr, input.entries.len);
                    for (entries, 0..) |entry, i| {
                        if (entry.kind != .normal) return error.PreparedCompletionActive;
                        const payload = try bytes(entry.payload);
                        const requires_predecessor = codec.protocol.looksLike(payload) or
                            (payload.len != 0 and (try resolution(p, payload)) != null);
                        if (requires_predecessor and entry.index != input.state.applied_index +| 1) {
                            output.has_append_prefix = 1;
                            output.append_prefix = i;
                            return;
                        }
                    }
                },
                .proposal => {
                    const payloads = try items(abi.Bytes, input.proposals.ptr, input.proposals.len);
                    if (payloads.len > 4) return error.CompletionAdmissionUnavailable;
                    for (owner.async_context.completion_guard.proposals) |pending| if (pending != null) return error.CompletionReservationBusy;
                    errdefer cancelProposals(owner, b, p);
                    for (payloads, 0..) |payload, i| {
                        const data = try bytes(payload);
                        const index = std.math.add(u64, input.state.last_index, i + 1) catch return error.InvalidArgument;
                        const identity: completion.AcceptedIdentity = .{ .term = input.state.term, .index = index, .digest = codec.protocol.payloadDigest(data) };
                        try accept(owner, b, p, input.state, identity, data, input.new_work_allowed != 0);
                        if (data.len != 0) owner.async_context.completion_guard.proposals[i] = identity;
                    }
                },
                .ready, .storage_ack => {
                    for (try items(abi.Entry, input.entries.ptr, input.entries.len)) |entry| {
                        if (entry.kind != .normal) return error.PreparedCompletionActive;
                        const data = try bytes(entry.payload);
                        try accept(owner, b, p, input.state, .{ .term = entry.term, .index = entry.index, .digest = codec.protocol.payloadDigest(data) }, data, input.new_work_allowed != 0);
                    }
                    // Restart may have a durable resolution in the committed
                    // suffix whose process-local tuple needs restoration. Its
                    // physical capacity was retained by the prepared cell.
                    for (try items(abi.Entry, input.committed_entries.ptr, input.committed_entries.len)) |entry| {
                        if (entry.kind != .normal) return error.PreparedCompletionActive;
                        if (entry.index <= input.state.applied_index) continue;
                        const data = try bytes(entry.payload);
                        try accept(owner, b, p, input.state, .{ .term = entry.term, .index = entry.index, .digest = codec.protocol.payloadDigest(data) }, data, input.new_work_allowed != 0);
                    }
                },
                else => return error.InvalidArgument,
            }
        }
        const Resolution = struct { id: [16]u8, commit: bool, timestamp: u64 };
        fn resolution(p: *native.Backend.CompletionPool, payload: []const u8) !?Resolution {
            if (payload.len == 0 or payload.len > codec.max_wire_bytes) return error.CompletionPlanCapacityExceeded;
            return p.compiler.withCompletion(?Resolution, payload, parseResolution);
        }
        fn parseResolution(payload: []const u8, alloc: std.mem.Allocator) !?Resolution {
            var decoded = try @import("../data/raft_batch.zig").decode(alloc, payload);
            defer decoded.deinit(alloc);
            if (decoded.protocol_barrier_version != null) return null;
            const req = decoded.batch.req;
            const transaction = req.transaction orelse return null;
            if (transaction != .resolve) return null;
            if (req.writes.len != 0 or req.deletes.len != 0 or req.transforms.len != 0 or req.graph_writes.len != 0 or
                req.graph_deletes.len != 0 or req.predicates.len != 0 or req.merge_artifacts.len != 0 or
                req.split_checkpoint != null or req.split_replication != null or req.split_transition != null or
                req.merge_source_transition != null or req.merge_checkpoint != null or req.merge_replication != null)
                return error.UnsupportedCompletionProfile;
            const value = transaction.resolve;
            if (value.status == .pending or value.commit_version == 0) return error.InvalidArgument;
            return .{ .id = value.txn_id, .commit = value.status == .committed, .timestamp = value.commit_version };
        }
        fn accept(_: *DB, b: *native.Backend, p: *native.Backend.CompletionPool, frontier: abi.State, identity: completion.AcceptedIdentity, payload: []const u8, new_work: bool) !void {
            if (try p.ownsAccepted(identity.term, identity.index, payload)) return;
            if (payload.len == 0) return;
            if (codec.protocol.looksLike(payload)) {
                if (!new_work or frontier.applied_term_known == 0) return error.CompletionAdmissionUnavailable;
                _ = try p.accept(b, identity.term, identity.index, frontier.applied_term, frontier.applied_index, payload);
                return;
            }
            if (try resolution(p, payload)) |value| {
                try p.reserveResolution(value.id, identity, value.commit, frontier.applied_index);
                return;
            }
            // Ordinary mutations require their own pre-persistence size and
            // footprint reservation. Never let a post-ACK mutable/WAL ceiling
            // become a permanent apply veto while that integration is pending.
            return error.CompletionAdmissionUnavailable;
        }
        fn cancelProposals(owner: *DB, b: *native.Backend, p: *native.Backend.CompletionPool) void {
            for (&owner.async_context.completion_guard.proposals) |*pending| if (pending.*) |identity| {
                p.cancelUnacceptedProposal(b, identity) catch {
                    p.failed = true;
                    b.fenceFailedBulkWal();
                };
                pending.* = null;
            };
        }
        fn proposalResult(raw: ?*anyopaque, result: *const abi.ProposalResult) callconv(.c) void {
            const owner = db(raw);
            owner.core.lockApply();
            defer owner.core.unlockApply();
            const b = backend(owner) catch return;
            const locked = backend_runtime.lockBackend(native.Backend, b);
            defer backend_runtime.unlockBackend(native.Backend, b, locked);
            const p = pool(owner) catch return;
            if (result.first_index == 0 and result.last_index == 0) {
                cancelProposals(owner, b, p);
                return;
            }
            for (&owner.async_context.completion_guard.proposals, 0..) |*pending, i| if (pending.*) |identity| {
                if (identity.index != result.first_index +| i or identity.index > result.last_index) {
                    p.failed = true;
                    b.fenceFailedBulkWal();
                }
                pending.* = null;
            };
        }
        fn release(_: ?*anyopaque) callconv(.c) void {}
        fn progress(raw: ?*anyopaque, out: *abi.Progress) callconv(.c) failure.Status {
            if (raw == null) return .invalid_argument;
            const owner = db(raw);
            owner.core.lockApply();
            defer owner.core.unlockApply();
            const b = backend(owner) catch |err| return errors.statusFromError(err);
            const locked = backend_runtime.lockBackend(native.Backend, b);
            defer backend_runtime.unlockBackend(native.Backend, b, locked);
            const p = pool(owner) catch |err| return errors.statusFromError(err);
            out.* = p.durableProgress() catch |err| return errors.statusFromError(err);
            return .ok;
        }
        fn owns(raw: ?*anyopaque, term: u64, index: u64, payload: abi.Bytes, out: *u8) callconv(.c) failure.Status {
            if (raw == null) return .invalid_argument;
            const owner = db(raw);
            owner.core.lockApply();
            defer owner.core.unlockApply();
            const b = backend(owner) catch |err| return errors.statusFromError(err);
            const locked = backend_runtime.lockBackend(native.Backend, b);
            defer backend_runtime.unlockBackend(native.Backend, b, locked);
            const p = pool(owner) catch |err| return errors.statusFromError(err);
            const data = bytes(payload) catch |err| return errors.statusFromError(err);
            out.* = @intFromBool(p.ownsAccepted(term, index, data) catch |err| return errors.statusFromError(err));
            return .ok;
        }
        fn apply(raw: ?*anyopaque, term: u64, index: u64, payload: abi.Bytes) callconv(.c) failure.Status {
            if (raw == null) return .invalid_argument;
            const owner = db(raw);
            const data = bytes(payload) catch |err| return errors.statusFromError(err);
            if (!codec.protocol.looksLike(data)) {
                const value = blk: {
                    owner.core.lockApply();
                    defer owner.core.unlockApply();
                    const b = backend(owner) catch |err| return errors.statusFromError(err);
                    const locked = backend_runtime.lockBackend(native.Backend, b);
                    defer backend_runtime.unlockBackend(native.Backend, b, locked);
                    const p = pool(owner) catch |err| return errors.statusFromError(err);
                    if (!(p.ownsAccepted(term, index, data) catch |err| return errors.statusFromError(err))) return .completion_admission_unavailable;
                    const receipt = p.durableProgress() catch null;
                    if (receipt) |existing| if (existing.term == term and existing.index == index and
                        std.mem.eql(u8, &existing.payload_digest, &codec.protocol.payloadDigest(data))) return .ok;
                    break :blk (resolution(p, data) catch |err| return errors.statusFromError(err)) orelse return .completion_admission_unavailable;
                };
                owner.resolveAcceptedCompletion(value.id, value.commit, value.timestamp, .{ .term = term, .index = index }, codec.protocol.payloadDigest(data)) catch |err| return errors.statusFromError(err);
                return .ok;
            }
            owner.applyAcceptedCompletion(term, index, codec.protocol.payloadDigest(data)) catch |err| return errors.statusFromError(err);
            return .ok;
        }
        fn durableCells(raw: ?*anyopaque, out: *abi.DurableCells) callconv(.c) failure.Status {
            if (raw == null) return .invalid_argument;
            const owner = db(raw);
            owner.core.lockApply();
            defer owner.core.unlockApply();
            const b = backend(owner) catch |err| return errors.statusFromError(err);
            const locked = backend_runtime.lockBackend(native.Backend, b);
            defer backend_runtime.unlockBackend(native.Backend, b, locked);
            const p = pool(owner) catch |err| return errors.statusFromError(err);
            var buffer: [4]native.completion_pool_mod.DurableCell = undefined;
            const cells = p.durableCells(&buffer) catch |err| return errors.statusFromError(err);
            out.* = .{ .count = @intCast(cells.len), .startup_reconciliation_pending = @intFromBool(p.startup_reconciliation_pending) };
            for (cells, out.cells[0..cells.len]) |cell, *result| result.* = .{
                .identity = .{ .term = cell.identity.term, .index = cell.identity.index, .payload_digest = cell.identity.digest },
                .prepared = @intFromBool(cell.prepared),
            };
            return .ok;
        }
        fn reconcile(raw: ?*anyopaque, input: *const abi.DurableLog) callconv(.c) failure.Status {
            if (raw == null) return .invalid_argument;
            reconcileImpl(db(raw), input) catch |err| return errors.statusFromError(err);
            return .ok;
        }
        fn reconcileImpl(owner: *DB, input: *const abi.DurableLog) !void {
            owner.core.lockApply();
            defer owner.core.unlockApply();
            const b = try backend(owner);
            const locked = backend_runtime.lockBackend(native.Backend, b);
            defer backend_runtime.unlockBackend(native.Backend, b, locked);
            const p = try pool(owner);
            // A v1 proof contains no retained BEGIN inventory. It must never
            // qualify an owner by silently treating those slots as empty.
            for (p.control_owners) |control| if (control != null) return error.CompletionAdmissionUnavailable;
            try reconcileDocumentLocked(owner, b, p, input);
        }
        fn reconcileDocumentLocked(owner: *DB, b: *native.Backend, p: *native.Backend.CompletionPool, input: *const abi.DurableLog) !void {
            if (input.version != abi.pool_abi_version or input.count > 4 or input.reserved != 0 or
                input.commit_index > input.last_index or input.compacted_index > input.commit_index or
                (input.compacted_index == 0) != (input.compacted_term == 0)) return error.InvalidArgument;
            const mode: @FieldType(native.completion_pool_mod.DurableLog, "mode") = switch (input.mode) {
                .startup_complete => .startup_complete,
                .persisted_replacement => .persisted_replacement,
                else => return error.InvalidArgument,
            };
            var observations: [4]native.completion_pool_mod.DurableObservation = undefined;
            for (input.observations[0..input.count], observations[0..input.count]) |value, *out| {
                if (value.present > 1 or value.replaced_in_this_persist > 1 or !std.mem.allEqual(u8, &value.reserved, 0)) return error.InvalidArgument;
                out.* = .{ .expected = .{ .term = value.expected.term, .index = value.expected.index, .digest = value.expected.payload_digest }, .present = value.present != 0, .observed_term = value.observed_term, .observed_digest = value.observed_digest, .replaced_in_this_persist = value.replaced_in_this_persist != 0 };
            }
            // A fresh pool has no startup debt to retire, but the direct WAL
            // owner still supplies the complete persisted frontier before any
            // campaigning or attestation can become eligible.
            if (mode == .startup_complete and !p.startup_reconciliation_pending) {
                var buffer: [4]native.completion_pool_mod.DurableCell = undefined;
                if ((try p.durableCells(&buffer)).len != 0 or input.count != 0) return error.RecoveryRequired;
            } else try p.reconcileDurableLog(b, .{ .mode = mode, .compacted_index = input.compacted_index, .compacted_term = input.compacted_term, .last_index = input.last_index, .commit_index = input.commit_index, .observations = observations[0..input.count] });
            if (mode == .startup_complete) owner.async_context.completion_guard.durable_log_reconciled = true;
        }
        fn controlDurableOwnersV2(raw: ?*anyopaque, out: *abi.ControlDurableOwnersV2) callconv(.c) failure.Status {
            if (raw == null) return .invalid_argument;
            const owner = db(raw);
            owner.core.lockApply();
            defer owner.core.unlockApply();
            const b = backend(owner) catch |err| return errors.statusFromError(err);
            const locked = backend_runtime.lockBackend(native.Backend, b);
            defer backend_runtime.unlockBackend(native.Backend, b, locked);
            const p = pool(owner) catch |err| return errors.statusFromError(err);
            var buffer: [abi.max_durable_controls]native.completion_pool_mod.DurableControlOwner = undefined;
            const owners = p.durableControlOwners(&buffer) catch |err| return errors.statusFromError(err);
            out.* = .{ .count = @intCast(owners.len) };
            for (owners, out.owners[0..owners.len]) |active, *result| result.* = .{
                .identity = .{ .term = active.identity.term, .index = active.identity.index, .payload_digest = active.identity.digest },
                .slot_index = @intCast(active.slot_index),
            };
            return .ok;
        }
        fn reconcileControlV2(raw: ?*anyopaque, input: *const abi.ControlDurableLogV2) callconv(.c) failure.Status {
            if (raw == null) return .invalid_argument;
            reconcileControlV2Impl(db(raw), input) catch |err| return errors.statusFromError(err);
            return .ok;
        }
        fn reconcileControlV2Impl(owner: *DB, input: *const abi.ControlDurableLogV2) !void {
            if (input.version != abi.control_proof_abi_version or input.count > abi.max_durable_controls or input.reserved != 0 or
                input.commit_index > input.last_index or input.compacted_index > input.commit_index or
                (input.compacted_index == 0) != (input.compacted_term == 0) or
                input.document.mode != input.mode or input.document.compacted_index != input.compacted_index or
                input.document.compacted_term != input.compacted_term or input.document.last_index != input.last_index or
                input.document.commit_index != input.commit_index) return error.InvalidArgument;
            const mode: @FieldType(native.completion_pool_mod.DurableLog, "mode") = switch (input.mode) {
                .startup_complete => .startup_complete,
                .persisted_replacement => .persisted_replacement,
                else => return error.InvalidArgument,
            };
            var observations: [abi.max_durable_controls]native.completion_pool_mod.DurableObservation = undefined;
            for (input.observations[0..input.count], observations[0..input.count]) |value, *out| {
                if (value.expected.term == 0 or value.expected.index == 0 or
                    std.mem.allEqual(u8, &value.expected.payload_digest, 0) or
                    value.present > 1 or value.replaced_in_this_persist > 1 or
                    !std.mem.allEqual(u8, &value.reserved, 0)) return error.InvalidArgument;
                out.* = .{ .expected = .{ .term = value.expected.term, .index = value.expected.index, .digest = value.expected.payload_digest }, .present = value.present != 0, .observed_term = value.observed_term, .observed_digest = value.observed_digest, .replaced_in_this_persist = value.replaced_in_this_persist != 0 };
            }
            owner.core.lockApply();
            defer owner.core.unlockApply();
            const b = try backend(owner);
            const locked = backend_runtime.lockBackend(native.Backend, b);
            defer backend_runtime.unlockBackend(native.Backend, b, locked);
            const p = try pool(owner);
            try p.reconcileControlDurableLog(.{ .mode = mode, .compacted_index = input.compacted_index, .compacted_term = input.compacted_term, .last_index = input.last_index, .commit_index = input.commit_index, .observations = observations[0..input.count] });
            try reconcileDocumentLocked(owner, b, p, &input.document);
        }
        const control_vtable_v2: abi.ControlVTableV2 = .{ .release = release, .durable_owners = controlDurableOwnersV2, .reconcile_durable = reconcileControlV2 };
        const vtable: abi.VTable = .{ .check = check, .proposal_result = proposalResult, .release = release, .apply_accepted = apply, .progress = progress, .owns_accepted = owns, .durable_cells = durableCells, .reconcile_durable = reconcile };
    };
}
