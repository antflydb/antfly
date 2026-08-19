// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Transport-neutral metadata administration read operations.

const std = @import("std");
const operation = @import("../api/operation.zig");
const metadata_api = @import("api.zig");
const metadata_admin = @import("admin.zig");
const metadata_table_manager = @import("table_manager.zig");
const metadata_transition_state = @import("transition_state.zig");
const raft_reconciler = @import("../raft/reconciler.zig");

pub const Source = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        head: *const fn (*anyopaque) anyerror!metadata_api.MetadataHead,
        linearizable_head: ?*const fn (*anyopaque) anyerror!metadata_api.MetadataHead = null,
        linearizable_snapshot: ?*const fn (*anyopaque) anyerror!metadata_api.AdminSnapshot = null,
        status: *const fn (*anyopaque) anyerror!metadata_api.MetadataStatus,
        admin_snapshot: *const fn (*anyopaque) anyerror!metadata_api.AdminSnapshot,
        free_admin_snapshot: *const fn (*anyopaque, *metadata_api.AdminSnapshot) void,
    };

    fn head(self: Source) !metadata_api.MetadataHead {
        return self.vtable.head(self.ptr);
    }

    fn linearizableHead(self: Source) !metadata_api.MetadataHead {
        const fn_ptr = self.vtable.linearizable_head orelse return error.UnsupportedOperation;
        return fn_ptr(self.ptr);
    }

    fn linearizableSnapshot(self: Source) !metadata_api.AdminSnapshot {
        const fn_ptr = self.vtable.linearizable_snapshot orelse return error.UnsupportedOperation;
        return fn_ptr(self.ptr);
    }

    fn status(self: Source) !metadata_api.MetadataStatus {
        return self.vtable.status(self.ptr);
    }

    fn snapshot(self: Source) !metadata_api.AdminSnapshot {
        return self.vtable.admin_snapshot(self.ptr);
    }

    fn freeSnapshot(self: Source, snapshot_value: *metadata_api.AdminSnapshot) void {
        self.vtable.free_admin_snapshot(self.ptr, snapshot_value);
    }
};

pub const ActiveTransitions = struct {
    split: []metadata_transition_state.SplitTransitionRecord,
    merge: []metadata_transition_state.MergeTransitionRecord,

    pub fn deinit(self: *ActiveTransitions, alloc: std.mem.Allocator) void {
        alloc.free(self.split);
        alloc.free(self.merge);
        self.* = undefined;
    }
};

pub const NodeShutdownStoreStatus = struct {
    store_id: u64,
    placement_intent_count: usize = 0,
    group_status_count: usize = 0,
    runtime_group_count: usize = 0,
    local_voter_count: usize = 0,
    local_leader_count: usize = 0,
};

pub const NodeShutdownStatus = struct {
    node_id: u64,
    type: []const u8 = "remove",
    phase: []const u8,
    safe_to_terminate: bool,
    blocked: bool = false,
    blocked_reason: ?[]const u8 = null,
    message: ?[]const u8 = null,
    stores: []const NodeShutdownStoreStatus,
    pending_groups: []const u64,

    pub fn deinit(self: *NodeShutdownStatus, alloc: std.mem.Allocator) void {
        alloc.free(self.stores);
        alloc.free(self.pending_groups);
        self.* = undefined;
    }
};

pub const Operations = struct {
    source: Source,

    pub fn health(_: Operations, request: operation.RequestContext) operation.ApiError!void {
        try request.ensureActive();
    }

    pub fn head(self: Operations, request: operation.RequestContext) !metadata_api.MetadataHead {
        try request.ensureActive();
        return self.source.head();
    }

    pub fn linearizableHead(self: Operations, request: operation.RequestContext) !metadata_api.MetadataHead {
        try request.ensureActive();
        return self.source.linearizableHead();
    }

    pub fn linearizableSnapshot(self: Operations, request: operation.RequestContext) !metadata_api.AdminSnapshot {
        try request.ensureActive();
        return self.source.linearizableSnapshot();
    }

    pub fn status(self: Operations, request: operation.RequestContext) !metadata_api.MetadataStatus {
        try request.ensureActive();
        return self.source.status();
    }

    pub fn snapshot(self: Operations, request: operation.RequestContext) !metadata_api.AdminSnapshot {
        try request.ensureActive();
        return self.source.snapshot();
    }

    pub fn freeSnapshot(self: Operations, snapshot_value: *metadata_api.AdminSnapshot) void {
        self.source.freeSnapshot(snapshot_value);
    }

    pub fn activeTransitions(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
    ) !ActiveTransitions {
        try request.ensureActive();
        var snapshot_value = try self.source.snapshot();
        defer self.source.freeSnapshot(&snapshot_value);
        var active = try metadata_admin.listActiveTransitions(alloc, &snapshot_value);
        defer metadata_admin.freeActiveTransitions(alloc, &active);
        const split = try cloneValues(alloc, metadata_transition_state.SplitTransitionRecord, active.split);
        errdefer alloc.free(split);
        return .{
            .split = split,
            .merge = try cloneValues(alloc, metadata_transition_state.MergeTransitionRecord, active.merge),
        };
    }

    pub fn tableRanges(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        table_id: u64,
    ) ![]metadata_table_manager.RangeRecord {
        try request.ensureActive();
        var snapshot_value = try self.source.snapshot();
        defer self.source.freeSnapshot(&snapshot_value);
        const refs = try metadata_admin.listTableRanges(alloc, &snapshot_value, table_id);
        defer metadata_admin.freeRangeRefs(alloc, refs);
        return cloneValues(alloc, metadata_table_manager.RangeRecord, refs);
    }

    pub fn groupPlacement(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        group_id: u64,
    ) ![]raft_reconciler.PlacementIntent {
        try request.ensureActive();
        var snapshot_value = try self.source.snapshot();
        defer self.source.freeSnapshot(&snapshot_value);
        const refs = try metadata_admin.listGroupPlacement(alloc, &snapshot_value, group_id);
        defer metadata_admin.freePlacementRefs(alloc, refs);
        return cloneValues(alloc, raft_reconciler.PlacementIntent, refs);
    }

    pub fn nodeShutdownStatus(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        node_id: u64,
    ) !NodeShutdownStatus {
        try request.ensureActive();
        if (node_id == 0) return error.InvalidArgument;
        var snapshot_value = try self.source.snapshot();
        defer self.source.freeSnapshot(&snapshot_value);
        return buildNodeShutdownStatus(alloc, &snapshot_value, node_id);
    }
};

fn cloneValues(alloc: std.mem.Allocator, comptime T: type, refs: anytype) ![]T {
    const out = try alloc.alloc(T, refs.len);
    for (refs, 0..) |record, i| out[i] = record.*;
    return out;
}

fn buildNodeShutdownStatus(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    node_id: u64,
) !NodeShutdownStatus {
    var stores = std.ArrayListUnmanaged(NodeShutdownStoreStatus).empty;
    errdefer stores.deinit(alloc);
    var pending_groups = std.ArrayListUnmanaged(u64).empty;
    errdefer pending_groups.deinit(alloc);

    var node_known = false;
    var node_draining = false;
    var node_finalizing = false;
    for (snapshot.nodes) |node| {
        if (node.node_id != node_id) continue;
        node_known = true;
        node_draining = !metadata_table_manager.nodeLifecycleActive(node.lifecycle);
        node_finalizing = metadata_table_manager.nodeLifecycleFinalizing(node.lifecycle);
        break;
    }

    var placement_total: usize = 0;
    for (snapshot.placement_intents) |intent| {
        if (intent.record.local_node_id != node_id) continue;
        placement_total += 1;
        try appendUniqueU64(alloc, &pending_groups, intent.record.group_id);
    }

    var group_status_total: usize = 0;
    var runtime_group_total: usize = 0;
    var local_voter_total: usize = 0;
    var local_leader_total: usize = 0;
    var store_drain_total: usize = 0;
    var insufficient_shard_voters = false;

    for (snapshot.stores) |store| {
        if (store.node_id != node_id) continue;
        if (store.drain_requested) store_drain_total += 1;
        var store_status = NodeShutdownStoreStatus{ .store_id = store.store_id };

        for (snapshot.placement_intents) |intent| {
            if (intent.record.local_node_id != node_id) continue;
            if (intent.store_id != 0 and intent.store_id != store.store_id) continue;
            store_status.placement_intent_count += 1;
        }
        for (store.group_statuses) |group_status| {
            store_status.group_status_count += 1;
            group_status_total += 1;
            try appendUniqueU64(alloc, &pending_groups, group_status.group_id);
            if (group_status.local_voter) {
                store_status.local_voter_count += 1;
                local_voter_total += 1;
                if (group_status.voter_count == 1) insufficient_shard_voters = true;
            }
            if (group_status.local_leader) {
                store_status.local_leader_count += 1;
                local_leader_total += 1;
            }
        }
        for (store.runtime_statuses) |runtime_status| {
            if (!metadata_table_manager.runtimeStatusBelongsToStore(runtime_status, node_id, store.store_id)) continue;
            store_status.runtime_group_count += 1;
            runtime_group_total += 1;
            try appendUniqueU64(alloc, &pending_groups, runtime_status.group_id);
        }
        try stores.append(alloc, store_status);
    }

    const no_termination_debt = placement_total == 0 and
        group_status_total == 0 and runtime_group_total == 0 and
        local_voter_total == 0 and local_leader_total == 0;
    const administratively_draining = node_draining or store_drain_total > 0;
    const node_not_found = !node_known and stores.items.len == 0 and
        placement_total == 0 and group_status_total == 0 and runtime_group_total == 0;
    const blocked_reason: ?[]const u8 = if (administratively_draining and insufficient_shard_voters)
        "InsufficientShardVoters"
    else
        null;
    const blocked = blocked_reason != null;
    const safe_to_terminate = node_not_found or (administratively_draining and no_termination_debt);

    return .{
        .node_id = node_id,
        .phase = if (node_not_found)
            "not_found"
        else if (!administratively_draining)
            "active"
        else if (blocked)
            "blocked"
        else if (safe_to_terminate)
            "complete"
        else if (node_finalizing)
            "finalizing"
        else
            "draining",
        .safe_to_terminate = safe_to_terminate,
        .blocked = blocked,
        .blocked_reason = blocked_reason,
        .message = if (blocked)
            "Node hosts a shard with no other voters; add or restore another voter before scale-down can complete"
        else
            null,
        .stores = try stores.toOwnedSlice(alloc),
        .pending_groups = try pending_groups.toOwnedSlice(alloc),
    };
}

fn appendUniqueU64(alloc: std.mem.Allocator, list: *std.ArrayListUnmanaged(u64), value: u64) !void {
    if (value == 0) return;
    for (list.items) |existing| if (existing == value) return;
    try list.append(alloc, value);
}

test "metadata admin read operations enforce cancellation and own aggregate results" {
    const FakeSource = struct {
        snapshot_calls: usize = 0,
        free_calls: usize = 0,

        fn iface(self: *@This()) Source {
            return .{
                .ptr = self,
                .vtable = &.{
                    .head = head,
                    .status = status,
                    .admin_snapshot = snapshot,
                    .free_admin_snapshot = freeSnapshot,
                },
            };
        }

        fn head(_: *anyopaque) !metadata_api.MetadataHead {
            return .{ .metadata_group_id = 1 };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        fn snapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.snapshot_calls += 1;
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = &.{},
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeSnapshot(ptr: *anyopaque, _: *metadata_api.AdminSnapshot) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.free_calls += 1;
        }
    };

    var source = FakeSource{};
    const operations = Operations{ .source = source.iface() };
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, operations.status(.{
        .cancellation = operation.CancellationToken.fromAtomic(&canceled),
    }));
    try std.testing.expectEqual(@as(usize, 0), source.snapshot_calls);

    var shutdown = try operations.nodeShutdownStatus(std.testing.allocator, .{}, 99);
    defer shutdown.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("not_found", shutdown.phase);
    try std.testing.expect(shutdown.safe_to_terminate);
    try std.testing.expectEqual(@as(usize, 1), source.snapshot_calls);
    try std.testing.expectEqual(@as(usize, 1), source.free_calls);
}

test "metadata shutdown status exposes pending finalization with shared store debt semantics" {
    var runtimes = [_]metadata_table_manager.RuntimeGroupStatusReport{.{
        .group_id = 101,
        .node_id = 9,
        .store_id = 9,
    }};
    var stores = [_]metadata_table_manager.StoreRecord{.{
        .store_id = 9,
        .node_id = 9,
        .drain_requested = true,
        .runtime_statuses = runtimes[0..],
    }};
    var nodes = [_]metadata_table_manager.NodeRecord{.{
        .node_id = 9,
        .lifecycle = metadata_table_manager.node_lifecycle_finalizing,
    }};
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = &.{},
        .ranges = &.{},
        .nodes = nodes[0..],
        .stores = stores[0..],
        .placement_intents = &.{},
        .split_transitions = &.{},
        .merge_transitions = &.{},
    };

    var pending = try buildNodeShutdownStatus(std.testing.allocator, &snapshot, 9);
    defer pending.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("finalizing", pending.phase);
    try std.testing.expect(!pending.safe_to_terminate);
    try std.testing.expectEqual(@as(usize, 1), pending.stores[0].runtime_group_count);

    // Explicitly foreign retained observations are ignored by this same
    // predicate in status, preflight, and Raft apply.
    runtimes[0].store_id = 10;
    var complete = try buildNodeShutdownStatus(std.testing.allocator, &snapshot, 9);
    defer complete.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("complete", complete.phase);
    try std.testing.expect(complete.safe_to_terminate);
    try std.testing.expectEqual(@as(usize, 0), complete.stores[0].runtime_group_count);
}
