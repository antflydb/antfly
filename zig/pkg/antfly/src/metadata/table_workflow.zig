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

const std = @import("std");
const control_loop = @import("control_loop.zig");
const placement_planner = @import("placement_planner.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const table_manager = @import("table_manager.zig");

pub const PlacementReconcileSummary = struct {
    upserts: usize = 0,
    removals: usize = 0,
};

pub const TableWorkflow = struct {
    loop: control_loop.MetadataControlLoop,

    pub fn init(alloc: std.mem.Allocator) TableWorkflow {
        return .{
            .loop = control_loop.MetadataControlLoop.init(alloc),
        };
    }

    pub fn deinit(self: *TableWorkflow) void {
        self.loop.deinit();
        self.* = undefined;
    }

    pub fn controlLoop(self: *TableWorkflow) *control_loop.MetadataControlLoop {
        return &self.loop;
    }

    pub fn setPlacementCandidates(self: *TableWorkflow, candidate_node_ids: []const u64) !void {
        try self.loop.stateRef().setPlacementCandidates(candidate_node_ids);
    }

    pub fn bootstrapDesiredFromCommitted(self: *TableWorkflow, service: anytype) !void {
        try self.loop.stateRef().syncProjected(service);
        try self.loop.stateRef().seedDesiredFromProjected();
    }

    pub fn createTable(
        self: *TableWorkflow,
        service: anytype,
        table: table_manager.TableRecord,
        initial_range: table_manager.RangeRecord,
    ) !control_loop.ReconcileSummary {
        return try self.createTableWithRanges(service, table, &[_]table_manager.RangeRecord{initial_range});
    }

    pub fn createTableWithRanges(
        self: *TableWorkflow,
        service: anytype,
        table: table_manager.TableRecord,
        initial_ranges: []const table_manager.RangeRecord,
    ) !control_loop.ReconcileSummary {
        try self.bootstrapDesiredFromCommitted(service);
        try self.loop.stateRef().tableManager().upsertTable(table);
        for (initial_ranges) |initial_range| {
            try self.loop.stateRef().tableManager().upsertRange(initial_range);
        }
        return try reconcileForService(&self.loop, service);
    }

    pub fn requestSplit(
        self: *TableWorkflow,
        service: anytype,
        intent: table_manager.SplitIntent,
    ) !control_loop.ReconcileSummary {
        try self.bootstrapDesiredFromCommitted(service);
        try self.loop.stateRef().tableManager().requestSplit(intent);
        return try reconcileForService(&self.loop, service);
    }

    pub fn requestMerge(
        self: *TableWorkflow,
        service: anytype,
        intent: table_manager.MergeIntent,
    ) !control_loop.ReconcileSummary {
        try self.bootstrapDesiredFromCommitted(service);
        try self.loop.stateRef().tableManager().requestMerge(intent);
        return try reconcileForService(&self.loop, service);
    }

    pub fn addRange(
        self: *TableWorkflow,
        service: anytype,
        record: table_manager.RangeRecord,
    ) !control_loop.ReconcileSummary {
        try self.bootstrapDesiredFromCommitted(service);
        try self.loop.stateRef().tableManager().upsertRange(record);
        return try reconcileForService(&self.loop, service);
    }

    pub fn dropTable(
        self: *TableWorkflow,
        service: anytype,
        table_id: u64,
    ) !control_loop.ReconcileSummary {
        try self.bootstrapDesiredFromCommitted(service);
        _ = self.loop.stateRef().tableManager().removeTableTopology(table_id);
        return try reconcileForService(&self.loop, service);
    }

    pub fn planLocalPlacementIntents(
        self: *TableWorkflow,
        alloc: std.mem.Allocator,
        local_node_id: u64,
        candidate_node_ids: []const u64,
    ) ![]raft_reconciler.PlacementIntent {
        var planner = placement_planner.PlacementPlanner.init(alloc);
        return try planner.planLocalIntents(self.loop.stateRef().tableManager(), local_node_id, candidate_node_ids);
    }

    pub fn reconcileLocalPlacementIntents(
        self: *TableWorkflow,
        service: anytype,
        alloc: std.mem.Allocator,
        local_node_id: u64,
        candidate_node_ids: []const u64,
    ) !PlacementReconcileSummary {
        const desired = try self.planLocalPlacementIntents(alloc, local_node_id, candidate_node_ids);
        defer {
            for (desired) |intent| if (intent.peer_node_ids.len > 0) alloc.free(intent.peer_node_ids);
            alloc.free(desired);
        }

        const current = try service.listProjectedPlacementIntents(alloc);
        defer service.freeProjectedPlacementIntents(alloc, current);

        var summary: PlacementReconcileSummary = .{};
        for (desired) |intent| {
            const existing = findPlacementIntent(current, intent.record.group_id, intent.record.local_node_id);
            if (existing == null or !placementIntentsEqual(existing.?, intent)) {
                try service.upsertReplicaIntent(intent);
                summary.upserts += 1;
            }
        }
        for (current) |intent| {
            if (intent.record.local_node_id != local_node_id) continue;
            if (findPlacementIntent(desired, intent.record.group_id, local_node_id) == null) {
                try service.removeReplicaIntent(intent.record.group_id, local_node_id);
                summary.removals += 1;
            }
        }
        return summary;
    }
};

fn reconcileForService(loop: *control_loop.MetadataControlLoop, service: anytype) !control_loop.ReconcileSummary {
    const ServiceType = @TypeOf(service);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (@hasDecl(ServiceDeclType, "reconcileOnceEnsuringLease")) {
        return try service.reconcileOnceEnsuringLease(loop);
    }
    if (@hasDecl(ServiceDeclType, "reconcileOnceIfLeaseHeld")) {
        return (try service.reconcileOnceIfLeaseHeld(loop)) orelse error.ReconcileLeaseNotHeld;
    }
    return try loop.reconcileOnce(service);
}

fn findPlacementIntent(
    intents: []const raft_reconciler.PlacementIntent,
    group_id: u64,
    local_node_id: u64,
) ?raft_reconciler.PlacementIntent {
    for (intents) |intent| {
        if (intent.record.group_id == group_id and intent.record.local_node_id == local_node_id) return intent;
    }
    return null;
}

fn placementIntentsEqual(a: raft_reconciler.PlacementIntent, b: raft_reconciler.PlacementIntent) bool {
    return a.record.group_id == b.record.group_id and
        a.record.replica_id == b.record.replica_id and
        a.record.local_node_id == b.record.local_node_id and
        a.record.metadata_version == b.record.metadata_version and
        a.record.bootstrap_mode == b.record.bootstrap_mode and
        optionalBytesEqual(
            if (a.record.snapshot_bootstrap) |record| record.snapshot_id else null,
            if (b.record.snapshot_bootstrap) |record| record.snapshot_id else null,
        ) and
        optionalBytesEqual(
            if (a.record.backup_restore_bootstrap) |record| record.snapshot_path else null,
            if (b.record.backup_restore_bootstrap) |record| record.snapshot_path else null,
        ) and
        std.mem.eql(u64, a.peer_node_ids, b.peer_node_ids);
}

fn optionalBytesEqual(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return std.mem.eql(u8, a.?, b.?);
}

test "table workflow can build desired topology through the control loop seam" {
    const FakeService = struct {
        table_upserts: usize = 0,
        range_upserts: usize = 0,

        pub fn listProjectedTables(_: *@This(), alloc: std.mem.Allocator) ![]table_manager.TableRecord {
            return try alloc.alloc(table_manager.TableRecord, 0);
        }

        pub fn freeProjectedTables(_: *@This(), alloc: std.mem.Allocator, records: []table_manager.TableRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedRanges(_: *@This(), alloc: std.mem.Allocator) ![]table_manager.RangeRecord {
            return try alloc.alloc(table_manager.RangeRecord, 0);
        }

        pub fn freeProjectedRanges(_: *@This(), alloc: std.mem.Allocator, records: []table_manager.RangeRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
            return try alloc.alloc(raft_reconciler.PlacementIntent, 0);
        }

        pub fn freeProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
            alloc.free(intents);
        }

        pub fn listProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator) ![]@import("transition_state.zig").SplitTransitionRecord {
            return try alloc.alloc(@import("transition_state.zig").SplitTransitionRecord, 0);
        }

        pub fn freeProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator, records: []@import("transition_state.zig").SplitTransitionRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator) ![]@import("transition_state.zig").MergeTransitionRecord {
            return try alloc.alloc(@import("transition_state.zig").MergeTransitionRecord, 0);
        }

        pub fn freeProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator, records: []@import("transition_state.zig").MergeTransitionRecord) void {
            alloc.free(records);
        }

        pub fn observeSplitTransition(_: *@This(), _: u64) !?@import("transition_state.zig").SplitObservation {
            return null;
        }

        pub fn observeMergeTransition(_: *@This(), _: u64) !?@import("transition_state.zig").MergeObservation {
            return null;
        }

        pub fn applyReconciliationPlan(self: *@This(), plan: *const @import("reconciler.zig").ReconciliationPlan) !void {
            self.table_upserts += plan.table_upserts.len;
            self.range_upserts += plan.range_upserts.len;
        }
    };

    var workflow = TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    var fake = FakeService{};

    const summary = try workflow.createTable(&fake, .{
        .table_id = 55,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, .{
        .group_id = 5501,
        .table_id = 55,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    try std.testing.expectEqual(@as(usize, 1), summary.table_upserts);
    try std.testing.expectEqual(@as(usize, 1), summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), fake.table_upserts);
    try std.testing.expectEqual(@as(usize, 1), fake.range_upserts);
}

test "table workflow create preserves existing projected topology" {
    const FakeService = struct {
        table_upserts: usize = 0,
        range_upserts: usize = 0,
        table_removals: usize = 0,
        range_removals: usize = 0,

        pub fn listProjectedTables(_: *@This(), alloc: std.mem.Allocator) ![]table_manager.TableRecord {
            const records = try alloc.alloc(table_manager.TableRecord, 1);
            records[0] = .{
                .table_id = 7,
                .name = try alloc.dupe(u8, "docs"),
                .description = try alloc.dupe(u8, ""),
                .schema_json = try alloc.dupe(u8, ""),
                .read_schema_json = try alloc.dupe(u8, ""),
                .indexes_json = try alloc.dupe(u8, ""),
                .replication_sources_json = try alloc.dupe(u8, ""),
                .placement_role = try alloc.dupe(u8, "data"),
            };
            return records;
        }

        pub fn freeProjectedTables(_: *@This(), alloc: std.mem.Allocator, records: []table_manager.TableRecord) void {
            for (records) |record| table_manager.freeTable(alloc, record);
            alloc.free(records);
        }

        pub fn listProjectedRanges(_: *@This(), alloc: std.mem.Allocator) ![]table_manager.RangeRecord {
            const records = try alloc.alloc(table_manager.RangeRecord, 1);
            records[0] = .{
                .group_id = 7001,
                .table_id = 7,
                .start_key = try alloc.dupe(u8, ""),
                .end_key = null,
            };
            return records;
        }

        pub fn freeProjectedRanges(_: *@This(), alloc: std.mem.Allocator, records: []table_manager.RangeRecord) void {
            for (records) |record| table_manager.freeRange(alloc, record);
            alloc.free(records);
        }

        pub fn listProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
            return try alloc.alloc(raft_reconciler.PlacementIntent, 0);
        }

        pub fn freeProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
            alloc.free(intents);
        }

        pub fn listProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator) ![]@import("transition_state.zig").SplitTransitionRecord {
            return try alloc.alloc(@import("transition_state.zig").SplitTransitionRecord, 0);
        }

        pub fn freeProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator, records: []@import("transition_state.zig").SplitTransitionRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator) ![]@import("transition_state.zig").MergeTransitionRecord {
            return try alloc.alloc(@import("transition_state.zig").MergeTransitionRecord, 0);
        }

        pub fn freeProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator, records: []@import("transition_state.zig").MergeTransitionRecord) void {
            alloc.free(records);
        }

        pub fn observeSplitTransition(_: *@This(), _: u64) !?@import("transition_state.zig").SplitObservation {
            return null;
        }

        pub fn observeMergeTransition(_: *@This(), _: u64) !?@import("transition_state.zig").MergeObservation {
            return null;
        }

        pub fn applyReconciliationPlan(self: *@This(), plan: *const @import("reconciler.zig").ReconciliationPlan) !void {
            self.table_upserts += plan.table_upserts.len;
            self.range_upserts += plan.range_upserts.len;
            self.table_removals += plan.table_removals.len;
            self.range_removals += plan.range_removals.len;
        }
    };

    var workflow = TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    var fake = FakeService{};

    _ = try workflow.createTable(&fake, .{
        .table_id = 8,
        .name = "logs",
        .placement_role = "data",
    }, .{
        .group_id = 8001,
        .table_id = 8,
        .start_key = "",
        .end_key = null,
    });

    try std.testing.expectEqual(@as(usize, 1), fake.table_upserts);
    try std.testing.expectEqual(@as(usize, 1), fake.range_upserts);
    try std.testing.expectEqual(@as(usize, 0), fake.table_removals);
    try std.testing.expectEqual(@as(usize, 0), fake.range_removals);
}

test "table workflow can remove a table topology from desired state" {
    const FakeService = struct {
        table_removals: usize = 0,
        range_removals: usize = 0,

        pub fn listProjectedTables(_: *@This(), alloc: std.mem.Allocator) ![]table_manager.TableRecord {
            const out = try alloc.alloc(table_manager.TableRecord, 1);
            out[0] = .{
                .table_id = 9,
                .name = try alloc.dupe(u8, "docs"),
                .placement_role = try alloc.dupe(u8, "data"),
            };
            return out;
        }

        pub fn freeProjectedTables(_: *@This(), alloc: std.mem.Allocator, records: []table_manager.TableRecord) void {
            for (records) |record| {
                alloc.free(record.name);
                alloc.free(record.placement_role);
            }
            alloc.free(records);
        }

        pub fn listProjectedRanges(_: *@This(), alloc: std.mem.Allocator) ![]table_manager.RangeRecord {
            const out = try alloc.alloc(table_manager.RangeRecord, 2);
            out[0] = .{ .group_id = 901, .table_id = 9, .start_key = try alloc.dupe(u8, "doc:a"), .end_key = try alloc.dupe(u8, "doc:m") };
            out[1] = .{ .group_id = 902, .table_id = 9, .start_key = try alloc.dupe(u8, "doc:m"), .end_key = try alloc.dupe(u8, "doc:z") };
            return out;
        }

        pub fn freeProjectedRanges(_: *@This(), alloc: std.mem.Allocator, records: []table_manager.RangeRecord) void {
            for (records) |record| {
                alloc.free(record.start_key);
                if (record.end_key) |end| alloc.free(end);
            }
            alloc.free(records);
        }

        pub fn listProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
            return try alloc.alloc(raft_reconciler.PlacementIntent, 0);
        }

        pub fn freeProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
            alloc.free(intents);
        }

        pub fn listProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator) ![]@import("transition_state.zig").SplitTransitionRecord {
            return try alloc.alloc(@import("transition_state.zig").SplitTransitionRecord, 0);
        }

        pub fn freeProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator, records: []@import("transition_state.zig").SplitTransitionRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator) ![]@import("transition_state.zig").MergeTransitionRecord {
            return try alloc.alloc(@import("transition_state.zig").MergeTransitionRecord, 0);
        }

        pub fn freeProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator, records: []@import("transition_state.zig").MergeTransitionRecord) void {
            alloc.free(records);
        }

        pub fn observeSplitTransition(_: *@This(), _: u64) !?@import("transition_state.zig").SplitObservation {
            return null;
        }

        pub fn observeMergeTransition(_: *@This(), _: u64) !?@import("transition_state.zig").MergeObservation {
            return null;
        }

        pub fn applyReconciliationPlan(self: *@This(), plan: *const @import("reconciler.zig").ReconciliationPlan) !void {
            self.table_removals += plan.table_removals.len;
            self.range_removals += plan.range_removals.len;
        }
    };

    var workflow = TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    var fake = FakeService{};
    try workflow.bootstrapDesiredFromCommitted(&fake);
    const summary = try workflow.dropTable(&fake, 9);
    try std.testing.expectEqual(@as(usize, 1), summary.table_removals);
    try std.testing.expectEqual(@as(usize, 2), summary.range_removals);
    try std.testing.expectEqual(@as(usize, 1), fake.table_removals);
    try std.testing.expectEqual(@as(usize, 2), fake.range_removals);
}

test "table workflow can reconcile projected local placement intents" {
    const FakeService = struct {
        alloc: std.mem.Allocator,
        intents: std.ArrayListUnmanaged(raft_reconciler.PlacementIntent) = .empty,

        fn deinit(self: *@This()) void {
            for (self.intents.items) |intent| if (intent.peer_node_ids.len > 0) self.alloc.free(intent.peer_node_ids);
            self.intents.deinit(self.alloc);
        }

        pub fn listProjectedPlacementIntents(self: *@This(), alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
            const out = try alloc.alloc(raft_reconciler.PlacementIntent, self.intents.items.len);
            errdefer alloc.free(out);
            for (self.intents.items, 0..) |intent, i| {
                out[i] = .{
                    .record = intent.record,
                    .peer_node_ids = if (intent.peer_node_ids.len == 0) &.{} else try alloc.dupe(u64, intent.peer_node_ids),
                };
            }
            return out;
        }

        pub fn freeProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
            for (intents) |intent| if (intent.peer_node_ids.len > 0) alloc.free(intent.peer_node_ids);
            alloc.free(intents);
        }

        fn upsertReplicaIntent(self: *@This(), intent: raft_reconciler.PlacementIntent) !void {
            for (self.intents.items) |*existing| {
                if (existing.record.group_id != intent.record.group_id or existing.record.local_node_id != intent.record.local_node_id) continue;
                if (existing.peer_node_ids.len > 0) self.alloc.free(existing.peer_node_ids);
                existing.* = .{
                    .record = intent.record,
                    .peer_node_ids = if (intent.peer_node_ids.len == 0) &.{} else try self.alloc.dupe(u64, intent.peer_node_ids),
                };
                return;
            }
            try self.intents.append(self.alloc, .{
                .record = intent.record,
                .peer_node_ids = if (intent.peer_node_ids.len == 0) &.{} else try self.alloc.dupe(u64, intent.peer_node_ids),
            });
        }

        fn removeReplicaIntent(self: *@This(), group_id: u64, local_node_id: u64) !void {
            var i: usize = 0;
            while (i < self.intents.items.len) : (i += 1) {
                const intent = self.intents.items[i];
                if (intent.record.group_id != group_id or intent.record.local_node_id != local_node_id) continue;
                if (intent.peer_node_ids.len > 0) self.alloc.free(intent.peer_node_ids);
                _ = self.intents.orderedRemove(i);
                return;
            }
        }
    };

    var workflow = TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    try workflow.controlLoop().stateRef().tableManager().upsertTable(.{
        .table_id = 12,
        .name = "docs",
        .desired_replica_count = 3,
    });
    try workflow.controlLoop().stateRef().tableManager().upsertRange(.{
        .group_id = 1201,
        .table_id = 12,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    var fake = FakeService{ .alloc = std.testing.allocator };
    defer fake.deinit();

    const first = try workflow.reconcileLocalPlacementIntents(&fake, std.testing.allocator, 2, &.{ 1, 2, 3 });
    try std.testing.expectEqual(@as(usize, 1), first.upserts);
    try std.testing.expectEqual(@as(usize, 0), first.removals);
    try std.testing.expectEqual(@as(usize, 1), fake.intents.items.len);

    const second = try workflow.reconcileLocalPlacementIntents(&fake, std.testing.allocator, 2, &.{ 1, 3 });
    try std.testing.expectEqual(@as(usize, 0), second.upserts);
    try std.testing.expectEqual(@as(usize, 1), second.removals);
    try std.testing.expectEqual(@as(usize, 0), fake.intents.items.len);
}
