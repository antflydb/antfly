// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Private, authenticated owner I/O for the existing online merge transition.
//! A callback never grants donor authority to a receiver or bypasses read-index.
const std = @import("std");
const operation = @import("operation.zig");
pub const contract = @import("../storage/db/online_merge_io_contract.zig");
pub const Port = struct {
    ptr: *anyopaque,
    execute_fn: *const fn (*anyopaque, std.mem.Allocator, u64, []const u8, contract.Request, operation.RequestContext) anyerror![]u8,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,
    const VTable = struct { execute_fn: *const fn (*anyopaque, std.mem.Allocator, u64, []const u8, contract.Request, operation.RequestContext) anyerror![]u8 };
    const BoundaryAbi = @import("../runtime_callback_abi.zig").Boundary(VTable);
    pub fn execute(self: Port, alloc: std.mem.Allocator, group: u64, table: []const u8, request: contract.Request, context: operation.RequestContext) ![]u8 {
        try context.ensureActive();
        try request.validate();
        if (group != request.ownerGroup()) return error.OnlineSourceScopeChanged;
        return BoundaryAbi.call("execute_fn", self.boundary_dispatch, self.execute_fn, .{ self.ptr, alloc, group, table, request, context });
    }

    /// Non-Raft ownership permits only the rewrite-source protocol. The native
    /// owner supplies its durable clock; this adapter never synthesizes a Raft
    /// term or applied index. Pin the hot-standby generation across the entire
    /// observation so a promotion cannot return another authority's result.
    /// Actual mutations still enter ordinary native batch admission, whose
    /// shared mutation lease and durable mirror own the commit boundary.
    pub fn executeStandaloneRewrite(self: Port, alloc: std.mem.Allocator, group: u64, table: []const u8, request: contract.Request, context: operation.RequestContext, gate: ?@import("../storage/db/ha_contract.zig").WriteGate) ![]u8 {
        try context.ensureActive();
        if (request.scope.fence.role != .rewrite_source) return error.UnsupportedRestoreSource;
        const pinned = if (gate) |value| value.pinned() else null;
        if (pinned) |value| try value.check();
        var native = request;
        if (native.operation == .admission) native.scope.authority = .native else if (native.scope.authority != .native) return error.OnlineSourceScopeChanged;
        const response = try self.execute(alloc, group, table, native, context);
        errdefer alloc.free(response);
        try context.ensureActive();
        if (pinned) |value| try value.check();
        return response;
    }
};

test "online merge private standalone rewrite port pins authority and never fabricates Raft coordinates" {
    const gate_mod = @import("../storage/hot_standby/public_gate_state.zig");
    const Probe = struct {
        calls: usize = 0,
        state: *gate_mod.State,
        change_generation: bool = false,
        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, _: u64, _: []const u8, request: contract.Request, _: operation.RequestContext) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqual(@import("../storage/db/online_source_contract.zig").Authority.native, request.scope.authority);
            try std.testing.expectEqual(@as(u64, 0), request.scope.copy_attempt.donor_term);
            if (self.change_generation) _ = self.state.generation.fetchAdd(1, .acq_rel);
            return alloc.dupe(u8, "native-owner-clock");
        }
    };
    const alloc = std.testing.allocator;
    var state: gate_mod.State = .{};
    var probe: Probe = .{ .state = &state };
    const port: Port = .{ .ptr = &probe, .execute_fn = Probe.execute };
    const gate: @import("../storage/db/ha_contract.zig").WriteGate = .{ .shared = .{ .state = &state } };
    var request: contract.Request = .{
        .scope = .{
            .fence = .{ .transition_id = 1, .attempt = 0, .admission_epoch = 0, .owner_group_id = 2, .peer_group_id = 3, .role = .rewrite_source, .namespace = .{ .table_id = 1, .shard_id = 12, .range_id = 22 }, .catalog_digest = @splat(0) },
            .receiver_namespace = .{ .table_id = 9, .shard_id = 3, .range_id = 3 },
            .consumer_epoch = 0,
            .copy_attempt = .{},
        },
        .operation = .{ .admission = .donor },
    };
    const observed = try port.executeStandaloneRewrite(alloc, 2, "rows", request, .{}, gate);
    defer alloc.free(observed);
    try std.testing.expectEqualStrings("native-owner-clock", observed);
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    try std.testing.expectError(error.OnlineSourceScopeChanged, port.executeStandaloneRewrite(alloc, 3, "rows", request, .{}, gate));
    state.role.store(@intFromEnum(gate_mod.Role.standby), .release);
    try std.testing.expectError(error.HAReadOnlyStandby, port.executeStandaloneRewrite(alloc, 2, "rows", request, .{}, gate));
    state.role.store(@intFromEnum(gate_mod.Role.fenced_primary), .release);
    try std.testing.expectError(error.HAFencedPrimary, port.executeStandaloneRewrite(alloc, 2, "rows", request, .{}, gate));
    state.role.store(@intFromEnum(gate_mod.Role.disabled), .release);
    probe.change_generation = true;
    try std.testing.expectError(error.HAPromotedStandbyRequiresPrimaryOpen, port.executeStandaloneRewrite(alloc, 2, "rows", request, .{}, gate));
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
    probe.change_generation = false;
    request.operation = .{ .status = .donor };
    try std.testing.expectError(error.OnlineSourceScopeChanged, port.executeStandaloneRewrite(alloc, 2, "rows", request, .{}, gate));
    request.scope.fence.role = .merge_source;
    try std.testing.expectError(error.UnsupportedRestoreSource, port.executeStandaloneRewrite(alloc, 2, "rows", request, .{}, gate));
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
}

test "online merge private port fences owners cancellation and deadlines before dispatch" {
    const Probe = struct {
        calls: usize = 0,
        fn execute(ptr: *anyopaque, _: std.mem.Allocator, group: u64, table: []const u8, request: contract.Request, _: operation.RequestContext) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqual(request.ownerGroup(), group);
            try std.testing.expectEqualStrings("rows", table);
            return error.OnlineSourcePinMissing;
        }
    };
    var probe = Probe{};
    const port: Port = .{ .ptr = &probe, .execute_fn = Probe.execute };
    var request: contract.Request = .{
        .scope = .{
            .fence = .{
                .transition_id = 1,
                .attempt = 1,
                .owner_group_id = 2,
                .peer_group_id = 3,
                .role = .merge_source,
                .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 },
                .catalog_digest = @splat(1),
            },
            .receiver_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 },
            .consumer_epoch = 1,
            .copy_attempt = .{ .donor_term = 1, .sequence = 1 },
        },
        .operation = .{ .status = .donor },
    };
    try std.testing.expectError(error.OnlineSourceScopeChanged, port.execute(std.testing.allocator, 3, "rows", request, .{}));
    var cancelled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, port.execute(std.testing.allocator, 2, "rows", request, .{ .cancellation = .fromAtomic(&cancelled) }));
    try std.testing.expectError(error.DeadlineExceeded, port.execute(std.testing.allocator, 2, "rows", request, .{ .deadline_ns = 0 }));
    try std.testing.expectEqual(@as(usize, 0), probe.calls);
    try std.testing.expectError(error.OnlineSourcePinMissing, port.execute(std.testing.allocator, 2, "rows", request, .{}));
    request.operation = .{ .status = .receiver };
    try std.testing.expectError(error.OnlineSourceScopeChanged, port.execute(std.testing.allocator, 2, "rows", request, .{}));
    try std.testing.expectError(error.OnlineSourcePinMissing, port.execute(std.testing.allocator, 3, "rows", request, .{}));
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
}

test "online merge private port preserves source recovery errors through foreign runtime dispatch" {
    const Probe = struct {
        failure: anyerror,
        fn execute(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: contract.Request, _: operation.RequestContext) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.failure;
        }
        fn foreignDispatch(call: *const @import("../runtime_native_abi.zig").CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) @import("../runtime_error_abi.zig").Status {
            return Port.BoundaryAbi.local_dispatch(call, callback, args, output);
        }
    };
    var probe: Probe = .{ .failure = error.OnlineSourcePinMissing };
    const port: Port = .{ .ptr = &probe, .execute_fn = Probe.execute, .boundary_dispatch = Probe.foreignDispatch };
    const request: contract.Request = .{ .scope = .{
        .fence = .{ .admission_epoch = 0, .transition_id = 1, .attempt = 0, .owner_group_id = 2, .peer_group_id = 3, .role = .merge_source, .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .catalog_digest = @splat(0) },
        .receiver_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 },
        .consumer_epoch = 0,
        .copy_attempt = .{},
    }, .operation = .{ .admission = .donor } };
    // A distinct dispatcher forces the stable C-layout transport rather than
    // the local direct-call fast path that hid the production-only regression.
    for ([_]anyerror{
        error.InvalidOnlineSourceCommand,
        error.OnlineSourceCorrupt,
        error.OnlineSourcePinMissing,
        error.OnlineSourcePinPending,
        error.OnlineSourceScopeChanged,
        error.InvalidSourceSnapshot,
        error.SourceSnapshotCorrupt,
        error.SourceSnapshotCutMismatch,
        error.SourceSnapshotIncomplete,
        error.SourceSnapshotTooLarge,
        error.SourceCopyRestoreUnsupported,
        error.InvalidMergePage,
        error.MergeCopyFenced,
        error.MergePageChunkRequired,
        error.MergePageIncomplete,
        error.MergePageSequenceGap,
        error.RetainedEffectsConsumerLimit,
        error.RetainedEffectsCorrupt,
        error.RetainedEffectsCursorMismatch,
        error.RetainedEffectsFull,
        error.RetainedEffectsIdentityRequired,
        error.RetainedEffectsNamespaceMismatch,
        error.BackendRuntimeIoUnavailable,
        error.CorruptRaftAppliedEntry,
        error.UnknownSchemaVersion,
        error.InvalidRetainedEffectsAdmission,
        error.RetainedEffectsFenceMismatch,
        error.RetainedEffectsMixedControl,
        error.RetainedEffectsTransactionFailed,
    }) |failure| {
        probe.failure = failure;
        try std.testing.expectError(failure, port.execute(std.testing.allocator, 2, "rows", request, .{}));
    }
}
