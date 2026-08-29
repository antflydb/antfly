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
        linearizable_head: ?*const fn (*anyopaque, operation.RequestContext) anyerror!metadata_api.MetadataHead = null,
        linearizable_snapshot: ?*const fn (*anyopaque, operation.RequestContext) anyerror!metadata_api.AdminSnapshot = null,
        status: *const fn (*anyopaque) anyerror!metadata_api.MetadataStatus,
        admin_snapshot: *const fn (*anyopaque) anyerror!metadata_api.AdminSnapshot,
        free_admin_snapshot: *const fn (*anyopaque, *metadata_api.AdminSnapshot) void,
    };

    fn head(self: Source) !metadata_api.MetadataHead {
        return self.vtable.head(self.ptr);
    }

    fn linearizableHead(self: Source, request: operation.RequestContext) !metadata_api.MetadataHead {
        const fn_ptr = self.vtable.linearizable_head orelse return error.UnsupportedOperation;
        return fn_ptr(self.ptr, request);
    }

    fn linearizableSnapshot(self: Source, request: operation.RequestContext) !metadata_api.AdminSnapshot {
        const fn_ptr = self.vtable.linearizable_snapshot orelse return error.UnsupportedOperation;
        return fn_ptr(self.ptr, request);
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
        freeSplitTransitions(alloc, self.split);
        freeMergeTransitions(alloc, self.merge);
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
        return self.source.linearizableHead(request);
    }

    pub fn linearizableSnapshot(self: Operations, request: operation.RequestContext) !metadata_api.AdminSnapshot {
        try request.ensureActive();
        return self.source.linearizableSnapshot(request);
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
        const split = try cloneSplitTransitionsOwned(alloc, active.split);
        errdefer freeSplitTransitions(alloc, split);
        return .{
            .split = split,
            .merge = try cloneMergeTransitionsOwned(alloc, active.merge),
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
        return cloneRangesOwned(alloc, refs);
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
        return clonePlacementIntentsOwned(alloc, refs);
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

fn cloneSplitTransitionsOwned(
    alloc: std.mem.Allocator,
    refs: []const *const metadata_transition_state.SplitTransitionRecord,
) ![]metadata_transition_state.SplitTransitionRecord {
    const out = try alloc.alloc(metadata_transition_state.SplitTransitionRecord, refs.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |record| metadata_table_manager.freeSplitTransitionRecord(alloc, record);
        alloc.free(out);
    }
    for (refs, 0..) |record, i| {
        out[i] = try metadata_table_manager.cloneSplitTransitionRecord(alloc, record.*);
        initialized += 1;
    }
    return out;
}

fn freeSplitTransitions(alloc: std.mem.Allocator, records: []metadata_transition_state.SplitTransitionRecord) void {
    for (records) |record| metadata_table_manager.freeSplitTransitionRecord(alloc, record);
    alloc.free(records);
}

fn cloneMergeTransitionsOwned(
    alloc: std.mem.Allocator,
    refs: []const *const metadata_transition_state.MergeTransitionRecord,
) ![]metadata_transition_state.MergeTransitionRecord {
    const out = try alloc.alloc(metadata_transition_state.MergeTransitionRecord, refs.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |record| metadata_table_manager.freeMergeTransitionRecord(alloc, record);
        alloc.free(out);
    }
    for (refs, 0..) |record, i| {
        out[i] = try metadata_table_manager.cloneMergeTransitionRecord(alloc, record.*);
        initialized += 1;
    }
    return out;
}

fn freeMergeTransitions(alloc: std.mem.Allocator, records: []metadata_transition_state.MergeTransitionRecord) void {
    for (records) |record| metadata_table_manager.freeMergeTransitionRecord(alloc, record);
    alloc.free(records);
}

fn cloneRangesOwned(
    alloc: std.mem.Allocator,
    refs: []const *const metadata_table_manager.RangeRecord,
) ![]metadata_table_manager.RangeRecord {
    const out = try alloc.alloc(metadata_table_manager.RangeRecord, refs.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(out);
    }
    for (refs, 0..) |record, i| {
        out[i] = try metadata_table_manager.cloneRange(alloc, record.*);
        initialized += 1;
    }
    return out;
}

pub fn freeRanges(alloc: std.mem.Allocator, records: []metadata_table_manager.RangeRecord) void {
    for (records) |record| metadata_table_manager.freeRange(alloc, record);
    alloc.free(records);
}

fn clonePlacementIntentsOwned(
    alloc: std.mem.Allocator,
    refs: []const *const raft_reconciler.PlacementIntent,
) ![]raft_reconciler.PlacementIntent {
    const out = try alloc.alloc(raft_reconciler.PlacementIntent, refs.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |intent| raft_reconciler.freeIntentOwned(alloc, intent);
        alloc.free(out);
    }
    for (refs, 0..) |intent, i| {
        out[i] = try raft_reconciler.cloneIntentOwned(alloc, intent.*);
        initialized += 1;
    }
    return out;
}

pub fn freePlacementIntents(alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
    for (intents) |intent| raft_reconciler.freeIntentOwned(alloc, intent);
    alloc.free(intents);
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

test "metadata admin active transitions own nested snapshot values" {
    const FakeSource = struct {
        const expected_split_key = "split-key";
        const expected_source_range_end = "source-end";
        const expected_split_rollback = "split-retry";
        const expected_split_table_name = "split-docs";
        const expected_split_schema = "{\"split\":true}";
        const expected_split_indexes = "{\"split_index\":{}}";
        const expected_merge_rollback = "merge-retry";
        const expected_merge_table_name = "merge-docs";
        const expected_merge_schema = "{\"merge\":true}";
        const expected_merge_indexes = "{\"merge_index\":{}}";

        split_key: [64]u8 = undefined,
        source_range_end: [64]u8 = undefined,
        split_rollback: [64]u8 = undefined,
        split_table_name: [64]u8 = undefined,
        split_schema: [64]u8 = undefined,
        split_indexes: [64]u8 = undefined,
        merge_rollback: [64]u8 = undefined,
        merge_table_name: [64]u8 = undefined,
        merge_schema: [64]u8 = undefined,
        merge_indexes: [64]u8 = undefined,
        split_transitions: [1]metadata_transition_state.SplitTransitionRecord = undefined,
        merge_transitions: [1]metadata_transition_state.MergeTransitionRecord = undefined,
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
            @memcpy(self.split_key[0..expected_split_key.len], expected_split_key);
            @memcpy(self.source_range_end[0..expected_source_range_end.len], expected_source_range_end);
            @memcpy(self.split_rollback[0..expected_split_rollback.len], expected_split_rollback);
            @memcpy(self.split_table_name[0..expected_split_table_name.len], expected_split_table_name);
            @memcpy(self.split_schema[0..expected_split_schema.len], expected_split_schema);
            @memcpy(self.split_indexes[0..expected_split_indexes.len], expected_split_indexes);
            @memcpy(self.merge_rollback[0..expected_merge_rollback.len], expected_merge_rollback);
            @memcpy(self.merge_table_name[0..expected_merge_table_name.len], expected_merge_table_name);
            @memcpy(self.merge_schema[0..expected_merge_schema.len], expected_merge_schema);
            @memcpy(self.merge_indexes[0..expected_merge_indexes.len], expected_merge_indexes);
            self.split_transitions[0] = .{
                .transition_id = 101,
                .attempt_epoch = 3,
                .source_group_id = 52,
                .destination_group_id = 53,
                .phase = .replay_deltas,
                .split_key = self.split_key[0..expected_split_key.len],
                .source_range_end = self.source_range_end[0..expected_source_range_end.len],
                .rollback_reason = self.split_rollback[0..expected_split_rollback.len],
                .table_contract = .{
                    .table_id = 51,
                    .table_name = self.split_table_name[0..expected_split_table_name.len],
                    .schema_json = self.split_schema[0..expected_split_schema.len],
                    .indexes_json = self.split_indexes[0..expected_split_indexes.len],
                    .source_identity = .{ .shard_id = 52, .range_id = 52 },
                    .target_identity = .{ .shard_id = 52, .range_id = 52 },
                },
            };
            self.merge_transitions[0] = .{
                .transition_id = 201,
                .donor_group_id = 61,
                .receiver_group_id = 62,
                .phase = .finalizing,
                .rollback_reason = self.merge_rollback[0..expected_merge_rollback.len],
                .allow_doc_identity_reassignment = true,
                .table_contract = .{
                    .table_id = 60,
                    .table_name = self.merge_table_name[0..expected_merge_table_name.len],
                    .schema_json = self.merge_schema[0..expected_merge_schema.len],
                    .indexes_json = self.merge_indexes[0..expected_merge_indexes.len],
                    .source_identity = .{ .shard_id = 61, .range_id = 61 },
                    .target_identity = .{ .shard_id = 62, .range_id = 62 },
                },
            };
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = &.{},
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &self.split_transitions,
                .merge_transitions = &self.merge_transitions,
            };
        }

        fn freeSnapshot(ptr: *anyopaque, _: *metadata_api.AdminSnapshot) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.free_calls += 1;
            @memset(&self.split_key, '!');
            @memset(&self.source_range_end, '!');
            @memset(&self.split_rollback, '!');
            @memset(&self.split_table_name, '!');
            @memset(&self.split_schema, '!');
            @memset(&self.split_indexes, '!');
            @memset(&self.merge_rollback, '!');
            @memset(&self.merge_table_name, '!');
            @memset(&self.merge_schema, '!');
            @memset(&self.merge_indexes, '!');
        }
    };

    var source = FakeSource{};
    const operations = Operations{ .source = source.iface() };
    var active = try operations.activeTransitions(std.testing.allocator, .{});
    defer active.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), source.free_calls);
    try std.testing.expectEqual(@as(usize, 1), active.split.len);
    try std.testing.expectEqual(@as(usize, 1), active.merge.len);
    try std.testing.expectEqualStrings(FakeSource.expected_split_key, active.split[0].split_key orelse return error.TestExpectedEqual);
    try std.testing.expectEqualStrings(FakeSource.expected_source_range_end, active.split[0].source_range_end orelse return error.TestExpectedEqual);
    try std.testing.expectEqualStrings(FakeSource.expected_split_rollback, active.split[0].rollback_reason orelse return error.TestExpectedEqual);
    try std.testing.expectEqualStrings(FakeSource.expected_split_table_name, active.split[0].table_contract.table_name);
    try std.testing.expectEqualStrings(FakeSource.expected_split_schema, active.split[0].table_contract.schema_json);
    try std.testing.expectEqualStrings(FakeSource.expected_split_indexes, active.split[0].table_contract.indexes_json);
    try std.testing.expectEqualStrings(FakeSource.expected_merge_rollback, active.merge[0].rollback_reason orelse return error.TestExpectedEqual);
    try std.testing.expectEqualStrings(FakeSource.expected_merge_table_name, active.merge[0].table_contract.table_name);
    try std.testing.expectEqualStrings(FakeSource.expected_merge_schema, active.merge[0].table_contract.schema_json);
    try std.testing.expectEqualStrings(FakeSource.expected_merge_indexes, active.merge[0].table_contract.indexes_json);
}

test "metadata admin table ranges own nested snapshot values" {
    const FakeSource = struct {
        const expected_start_key = "range-a";
        const expected_end_key = "range-z";
        const expected_backup_id = "restore-table-52";
        const expected_artifact_backup_id = "restore-artifact-52";
        const expected_location = "s3://backups/restore-52";
        const expected_snapshot_path = "restore-52/group-52.afb";
        const expected_connection = "backup-store";
        const expected_artifact_sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

        start_key: [64]u8 = undefined,
        end_key: [64]u8 = undefined,
        backup_id: [64]u8 = undefined,
        artifact_backup_id: [64]u8 = undefined,
        location: [64]u8 = undefined,
        snapshot_path: [64]u8 = undefined,
        connection: [64]u8 = undefined,
        artifact_sha256: [64]u8 = undefined,
        ranges: [1]metadata_table_manager.RangeRecord = undefined,
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
            @memcpy(self.start_key[0..expected_start_key.len], expected_start_key);
            @memcpy(self.end_key[0..expected_end_key.len], expected_end_key);
            @memcpy(self.backup_id[0..expected_backup_id.len], expected_backup_id);
            @memcpy(self.artifact_backup_id[0..expected_artifact_backup_id.len], expected_artifact_backup_id);
            @memcpy(self.location[0..expected_location.len], expected_location);
            @memcpy(self.snapshot_path[0..expected_snapshot_path.len], expected_snapshot_path);
            @memcpy(self.connection[0..expected_connection.len], expected_connection);
            @memcpy(self.artifact_sha256[0..expected_artifact_sha256.len], expected_artifact_sha256);
            self.ranges[0] = .{
                .group_id = 52,
                .range_id = 53,
                .table_id = 51,
                .start_key = self.start_key[0..expected_start_key.len],
                .end_key = self.end_key[0..expected_end_key.len],
                .restore_backup_id = self.backup_id[0..expected_backup_id.len],
                .restore_artifact_backup_id = self.artifact_backup_id[0..expected_artifact_backup_id.len],
                .restore_location = self.location[0..expected_location.len],
                .restore_snapshot_path = self.snapshot_path[0..expected_snapshot_path.len],
                .restore_connection = self.connection[0..expected_connection.len],
                .restore_artifact_size_bytes = 1924,
                .restore_artifact_sha256 = self.artifact_sha256[0..expected_artifact_sha256.len],
            };
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = &.{},
                .ranges = &self.ranges,
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeSnapshot(ptr: *anyopaque, _: *metadata_api.AdminSnapshot) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.free_calls += 1;
            @memset(&self.start_key, '!');
            @memset(&self.end_key, '!');
            @memset(&self.backup_id, '!');
            @memset(&self.artifact_backup_id, '!');
            @memset(&self.location, '!');
            @memset(&self.snapshot_path, '!');
            @memset(&self.connection, '!');
            @memset(&self.artifact_sha256, '!');
        }
    };

    var source = FakeSource{};
    const operations = Operations{ .source = source.iface() };
    const ranges = try operations.tableRanges(std.testing.allocator, .{}, 51);
    defer freeRanges(std.testing.allocator, ranges);

    try std.testing.expectEqual(@as(usize, 1), source.free_calls);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
    try std.testing.expectEqualStrings(FakeSource.expected_start_key, ranges[0].start_key);
    try std.testing.expectEqualStrings(FakeSource.expected_end_key, ranges[0].end_key orelse return error.TestExpectedEqual);
    try std.testing.expectEqualStrings(FakeSource.expected_backup_id, ranges[0].restore_backup_id);
    try std.testing.expectEqualStrings(FakeSource.expected_artifact_backup_id, ranges[0].restore_artifact_backup_id);
    try std.testing.expectEqualStrings(FakeSource.expected_location, ranges[0].restore_location);
    try std.testing.expectEqualStrings(FakeSource.expected_snapshot_path, ranges[0].restore_snapshot_path);
    try std.testing.expectEqualStrings(FakeSource.expected_connection, ranges[0].restore_connection);
    try std.testing.expectEqualStrings(FakeSource.expected_artifact_sha256, ranges[0].restore_artifact_sha256);
}

test "metadata admin group placement owns nested snapshot values" {
    const FakeSource = struct {
        const expected_backup_id = "restore-table-52";
        const expected_artifact_backup_id = "restore-artifact-52";
        const expected_location = "s3://backups/restore-52";
        const expected_snapshot_path = "restore-52/group-52.afb";
        const expected_connection = "backup-store";
        const expected_artifact_sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

        backup_id: [64]u8 = undefined,
        artifact_backup_id: [64]u8 = undefined,
        location: [64]u8 = undefined,
        snapshot_path: [64]u8 = undefined,
        connection: [64]u8 = undefined,
        artifact_sha256: [64]u8 = undefined,
        peer_node_ids: [3]u64 = .{ 3, 1, 2 },
        learner_node_ids: [1]u64 = .{4},
        placement_intents: [1]raft_reconciler.PlacementIntent = undefined,
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
            @memcpy(self.backup_id[0..expected_backup_id.len], expected_backup_id);
            @memcpy(self.artifact_backup_id[0..expected_artifact_backup_id.len], expected_artifact_backup_id);
            @memcpy(self.location[0..expected_location.len], expected_location);
            @memcpy(self.snapshot_path[0..expected_snapshot_path.len], expected_snapshot_path);
            @memcpy(self.connection[0..expected_connection.len], expected_connection);
            @memcpy(self.artifact_sha256[0..expected_artifact_sha256.len], expected_artifact_sha256);
            self.peer_node_ids = .{ 3, 1, 2 };
            self.learner_node_ids = .{4};
            self.placement_intents[0] = .{
                .record = .{
                    .group_id = 52,
                    .replica_id = 2,
                    .local_node_id = 1,
                    .metadata_version = 19,
                    .backup_restore_bootstrap = .{
                        .backup_id = self.backup_id[0..expected_backup_id.len],
                        .artifact_backup_id = self.artifact_backup_id[0..expected_artifact_backup_id.len],
                        .location = self.location[0..expected_location.len],
                        .snapshot_path = self.snapshot_path[0..expected_snapshot_path.len],
                        .connection = self.connection[0..expected_connection.len],
                        .artifact_size_bytes = 1924,
                        .artifact_sha256 = self.artifact_sha256[0..expected_artifact_sha256.len],
                    },
                },
                .store_id = 1,
                .peer_node_ids = &self.peer_node_ids,
                .learner_node_ids = &self.learner_node_ids,
            };
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = &.{},
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &self.placement_intents,
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeSnapshot(ptr: *anyopaque, _: *metadata_api.AdminSnapshot) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.free_calls += 1;
            @memset(&self.backup_id, '!');
            @memset(&self.artifact_backup_id, '!');
            @memset(&self.location, '!');
            @memset(&self.snapshot_path, '!');
            @memset(&self.connection, '!');
            @memset(&self.artifact_sha256, '!');
            @memset(&self.peer_node_ids, 99);
            @memset(&self.learner_node_ids, 99);
        }
    };

    var source = FakeSource{};
    const operations = Operations{ .source = source.iface() };
    const placements = try operations.groupPlacement(std.testing.allocator, .{}, 52);
    defer freePlacementIntents(std.testing.allocator, placements);

    try std.testing.expectEqual(@as(usize, 1), source.free_calls);
    try std.testing.expectEqual(@as(usize, 1), placements.len);
    const restore = placements[0].record.backup_restore_bootstrap orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings(FakeSource.expected_backup_id, restore.backup_id);
    try std.testing.expectEqualStrings(FakeSource.expected_artifact_backup_id, restore.artifact_backup_id);
    try std.testing.expectEqualStrings(FakeSource.expected_location, restore.location);
    try std.testing.expectEqualStrings(FakeSource.expected_snapshot_path, restore.snapshot_path);
    try std.testing.expectEqualStrings(FakeSource.expected_connection, restore.connection);
    try std.testing.expectEqualStrings(FakeSource.expected_artifact_sha256, restore.artifact_sha256);
    try std.testing.expectEqualSlices(u64, &.{ 3, 1, 2 }, placements[0].peer_node_ids);
    try std.testing.expectEqualSlices(u64, &.{4}, placements[0].learner_node_ids);
}

test "metadata admin linearizable snapshot propagates request context" {
    const FakeSource = struct {
        linearizable_calls: usize = 0,
        observed_deadline_ns: ?u64 = null,
        observed_request_id: []const u8 = "",

        fn iface(self: *@This()) Source {
            return .{
                .ptr = self,
                .vtable = &.{
                    .head = head,
                    .linearizable_snapshot = linearizableSnapshot,
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

        fn emptySnapshot() metadata_api.AdminSnapshot {
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

        fn snapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return emptySnapshot();
        }

        fn linearizableSnapshot(ptr: *anyopaque, request: operation.RequestContext) !metadata_api.AdminSnapshot {
            try request.ensureActive();
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.linearizable_calls += 1;
            self.observed_deadline_ns = request.deadline_ns;
            self.observed_request_id = request.request_id;
            return emptySnapshot();
        }

        fn freeSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = FakeSource{};
    const operations = Operations{ .source = source.iface() };
    var snapshot_value = try operations.linearizableSnapshot(.{
        .deadline_ns = std.math.maxInt(u64),
        .request_id = "fenced-read-17",
    });
    operations.freeSnapshot(&snapshot_value);

    try std.testing.expectEqual(@as(usize, 1), source.linearizable_calls);
    try std.testing.expectEqual(@as(?u64, std.math.maxInt(u64)), source.observed_deadline_ns);
    try std.testing.expectEqualStrings("fenced-read-17", source.observed_request_id);
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
