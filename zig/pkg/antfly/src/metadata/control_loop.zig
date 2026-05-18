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
const raft_reconciler = @import("../raft/reconciler.zig");
const metadata_reconciler = @import("reconciler.zig");
const metadata_state = @import("state.zig");
const metadata_table_manager = @import("table_manager.zig");
const platform_time = @import("../platform/time.zig");
const transition_state = @import("transition_state.zig");

pub const ReconcileSummary = struct {
    placement_upserts: usize = 0,
    repair_placement_groups: usize = 0,
    rebalance_placement_groups: usize = 0,
    table_upserts: usize = 0,
    range_upserts: usize = 0,
    split_upserts: usize = 0,
    merge_upserts: usize = 0,
    placement_removals: usize = 0,
    table_removals: usize = 0,
    range_removals: usize = 0,
    split_removals: usize = 0,
    merge_removals: usize = 0,
    split_steps: usize = 0,
    merge_steps: usize = 0,
};

pub const MetadataControlLoop = struct {
    alloc: std.mem.Allocator,
    state: metadata_state.MetadataState,
    reconciler: metadata_reconciler.Reconciler,

    pub fn init(alloc: std.mem.Allocator) MetadataControlLoop {
        return initWithConfig(alloc, .{});
    }

    pub fn initWithConfig(alloc: std.mem.Allocator, config: metadata_reconciler.Reconciler.Config) MetadataControlLoop {
        return .{
            .alloc = alloc,
            .state = metadata_state.MetadataState.init(alloc),
            .reconciler = metadata_reconciler.Reconciler.initWithConfig(alloc, config),
        };
    }

    pub fn deinit(self: *MetadataControlLoop) void {
        self.state.deinit();
        self.reconciler.deinit();
        self.* = undefined;
    }

    pub fn stateRef(self: *MetadataControlLoop) *metadata_state.MetadataState {
        return &self.state;
    }

    /// Raw reconcile primitive.
    ///
    /// This intentionally bypasses runtime reconcile-lease fencing. Production
    /// callers should go through metadata service `runRound()` or a
    /// service/simulation helper that first checks reconcile lease ownership.
    pub fn reconcileOnce(self: *MetadataControlLoop, service: anytype) !ReconcileSummary {
        try self.state.syncProjected(service);
        return try self.reconcilePrepared(service);
    }

    /// Reconcile using the caller's prepared projected/desired state.
    ///
    /// Callers that seed desired state from projected state should use this to
    /// avoid racing a second projected refresh between seeding and plan
    /// computation.
    pub fn reconcilePrepared(self: *MetadataControlLoop, service: anytype) !ReconcileSummary {
        self.installMedianKeyLookup(service);
        var current = try self.state.captureCurrent(service);
        defer current.deinit(self.alloc);

        try self.applyObservedTransitionOutcomes(&current);

        var plan = try self.reconciler.computePlan(
            self.state.tableManager(),
            self.state.placementCandidates(),
            self.state.placementCandidateInfo(),
            current.current,
        );
        defer plan.deinit(self.alloc);

        const summary: ReconcileSummary = .{
            .placement_upserts = plan.placement_upserts.len,
            .repair_placement_groups = plan.repair_placement_groups,
            .rebalance_placement_groups = plan.rebalance_placement_groups,
            .table_upserts = plan.table_upserts.len,
            .range_upserts = plan.range_upserts.len,
            .split_upserts = plan.split_upserts.len,
            .merge_upserts = plan.merge_upserts.len,
            .placement_removals = plan.placement_removals.len,
            .table_removals = plan.table_removals.len,
            .range_removals = plan.range_removals.len,
            .split_removals = plan.split_removals.len,
            .merge_removals = plan.merge_removals.len,
            .split_steps = plan.split_steps.len,
            .merge_steps = plan.merge_steps.len,
        };
        try service.applyReconciliationPlan(&plan);
        return summary;
    }

    fn installMedianKeyLookup(self: *MetadataControlLoop, service: anytype) void {
        const Service = switch (@typeInfo(@TypeOf(service))) {
            .pointer => |pointer| pointer.child,
            else => @TypeOf(service),
        };
        if (@hasDecl(Service, "medianKeyLookup")) {
            self.reconciler.setMedianKeyLookup(service.medianKeyLookup());
        } else {
            self.reconciler.setMedianKeyLookup(null);
        }
    }

    fn applyObservedTransitionOutcomes(self: *MetadataControlLoop, current: *const metadata_state.CapturedCurrentState) !void {
        for (current.current.split_transitions, current.split_observations) |record, observed| {
            switch (observed.observation.status.phase) {
                .finalized => try self.state.tableManager().applyFinalizedSplit(record),
                .rolled_back => self.state.tableManager().applyRolledBackSplit(record.transition_id),
                else => {},
            }
        }
        for (current.current.merge_transitions, current.merge_observations) |record, observed| {
            switch (observed.observation.receiver.phase) {
                .finalized => try self.state.tableManager().applyFinalizedMerge(record),
                .rolled_back => self.state.tableManager().applyRolledBackMerge(record.transition_id),
                else => {},
            }
        }
    }
};

test "metadata control loop proposes desired transitions through the service seam" {
    const FakeService = struct {
        tables: []const metadata_table_manager.TableRecord,
        ranges: []const metadata_table_manager.RangeRecord,
        placement_upserts: usize = 0,
        split_upserts: usize = 0,
        merge_upserts: usize = 0,

        pub fn listProjectedTables(self: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.TableRecord {
            const out = try alloc.alloc(metadata_table_manager.TableRecord, self.tables.len);
            errdefer alloc.free(out);
            for (self.tables, 0..) |record, i| out[i] = .{
                .table_id = record.table_id,
                .name = try alloc.dupe(u8, record.name),
                .placement_role = try alloc.dupe(u8, record.placement_role),
                .desired_replica_count = record.desired_replica_count,
                .min_ranges = record.min_ranges,
            };
            return out;
        }

        pub fn freeProjectedTables(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.TableRecord) void {
            for (records) |record| {
                alloc.free(record.name);
                alloc.free(record.placement_role);
            }
            alloc.free(records);
        }

        pub fn listProjectedRanges(self: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.RangeRecord {
            const out = try alloc.alloc(metadata_table_manager.RangeRecord, self.ranges.len);
            errdefer alloc.free(out);
            for (self.ranges, 0..) |record, i| out[i] = .{
                .group_id = record.group_id,
                .table_id = record.table_id,
                .start_key = try alloc.dupe(u8, record.start_key),
                .end_key = if (record.end_key) |end| try alloc.dupe(u8, end) else null,
            };
            return out;
        }

        pub fn freeProjectedRanges(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.RangeRecord) void {
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

        pub fn listProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator) ![]transition_state.SplitTransitionRecord {
            return try alloc.alloc(transition_state.SplitTransitionRecord, 0);
        }

        pub fn freeProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator, records: []transition_state.SplitTransitionRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator) ![]transition_state.MergeTransitionRecord {
            return try alloc.alloc(transition_state.MergeTransitionRecord, 0);
        }

        pub fn freeProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator, records: []transition_state.MergeTransitionRecord) void {
            alloc.free(records);
        }

        pub fn observeSplitTransition(_: *@This(), _: u64) !?transition_state.SplitObservation {
            return null;
        }

        pub fn observeMergeTransition(_: *@This(), _: u64) !?transition_state.MergeObservation {
            return null;
        }

        pub fn applyReconciliationPlan(self: *@This(), plan: *const metadata_reconciler.ReconciliationPlan) !void {
            self.placement_upserts += plan.placement_upserts.len;
            self.split_upserts += plan.split_upserts.len;
            self.merge_upserts += plan.merge_upserts.len;
        }
    };
    var loop = MetadataControlLoop.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .max_shards_per_table = 8,
    });
    defer loop.deinit();

    const tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 10, .name = "docs" },
    };
    const ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 101, .table_id = 10, .start_key = "doc:a", .end_key = "doc:m" },
        .{ .group_id = 102, .table_id = 10, .start_key = "doc:m", .end_key = "doc:z" },
    };
    try loop.stateRef().tableManager().replaceTopology(&tables, &ranges);
    try loop.stateRef().tableManager().requestSplit(.{
        .transition_id = 8001,
        .table_id = 10,
        .source_group_id = 101,
        .destination_group_id = 103,
        .split_key = "doc:h",
    });

    var fake = FakeService{
        .tables = &tables,
        .ranges = &ranges,
    };
    const summary = try loop.reconcileOnce(&fake);
    try std.testing.expectEqual(@as(usize, 0), summary.table_upserts);
    try std.testing.expectEqual(@as(usize, 0), summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 0), summary.placement_upserts);
    try std.testing.expectEqual(@as(usize, 1), summary.split_upserts);
    try std.testing.expectEqual(@as(usize, 1), fake.split_upserts);
}

test "metadata control loop plans placement intents from desired topology and candidates" {
    const FakeService = struct {
        tables: []const metadata_table_manager.TableRecord,
        ranges: []const metadata_table_manager.RangeRecord,
        placement_upserts: usize = 0,

        pub fn listProjectedTables(self: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.TableRecord {
            const out = try alloc.alloc(metadata_table_manager.TableRecord, self.tables.len);
            errdefer alloc.free(out);
            for (self.tables, 0..) |record, i| out[i] = .{
                .table_id = record.table_id,
                .name = try alloc.dupe(u8, record.name),
                .placement_role = try alloc.dupe(u8, record.placement_role),
                .desired_replica_count = record.desired_replica_count,
                .min_ranges = record.min_ranges,
            };
            return out;
        }

        pub fn freeProjectedTables(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.TableRecord) void {
            for (records) |record| {
                alloc.free(record.name);
                alloc.free(record.placement_role);
            }
            alloc.free(records);
        }

        pub fn listProjectedRanges(self: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.RangeRecord {
            const out = try alloc.alloc(metadata_table_manager.RangeRecord, self.ranges.len);
            errdefer alloc.free(out);
            for (self.ranges, 0..) |record, i| out[i] = .{
                .group_id = record.group_id,
                .table_id = record.table_id,
                .start_key = try alloc.dupe(u8, record.start_key),
                .end_key = if (record.end_key) |end| try alloc.dupe(u8, end) else null,
            };
            return out;
        }

        pub fn freeProjectedRanges(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.RangeRecord) void {
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

        pub fn listProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator) ![]transition_state.SplitTransitionRecord {
            return try alloc.alloc(transition_state.SplitTransitionRecord, 0);
        }

        pub fn freeProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator, records: []transition_state.SplitTransitionRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator) ![]transition_state.MergeTransitionRecord {
            return try alloc.alloc(transition_state.MergeTransitionRecord, 0);
        }

        pub fn freeProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator, records: []transition_state.MergeTransitionRecord) void {
            alloc.free(records);
        }

        pub fn observeSplitTransition(_: *@This(), _: u64) !?transition_state.SplitObservation {
            return null;
        }

        pub fn observeMergeTransition(_: *@This(), _: u64) !?transition_state.MergeObservation {
            return null;
        }

        pub fn applyReconciliationPlan(self: *@This(), plan: *const metadata_reconciler.ReconciliationPlan) !void {
            self.placement_upserts += plan.placement_upserts.len;
        }
    };

    var loop = MetadataControlLoop.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
    });
    defer loop.deinit();
    try loop.stateRef().setPlacementCandidates(&.{ 1, 2, 3 });

    const tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 11, .name = "docs", .desired_replica_count = 3 },
    };
    const ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 1101, .table_id = 11, .start_key = "doc:a", .end_key = "doc:z" },
    };
    try loop.stateRef().tableManager().replaceTopology(&tables, &ranges);

    var fake = FakeService{ .tables = &tables, .ranges = &ranges };
    const summary = try loop.reconcileOnce(&fake);
    try std.testing.expectEqual(@as(usize, 3), summary.placement_upserts);
    try std.testing.expectEqual(@as(usize, 3), fake.placement_upserts);
}

test "metadata control loop installs service median key lookup for automatic split planning" {
    const FakeService = struct {
        tables: []const metadata_table_manager.TableRecord,
        ranges: []const metadata_table_manager.RangeRecord,
        stores: []const metadata_table_manager.StoreRecord,
        planned_split_key: ?[]u8 = null,

        pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            if (self.planned_split_key) |value| alloc.free(value);
        }

        pub fn listProjectedTables(self: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.TableRecord {
            const out = try alloc.alloc(metadata_table_manager.TableRecord, self.tables.len);
            errdefer alloc.free(out);
            for (self.tables, 0..) |record, i| out[i] = .{
                .table_id = record.table_id,
                .name = try alloc.dupe(u8, record.name),
                .placement_role = try alloc.dupe(u8, record.placement_role),
                .desired_replica_count = record.desired_replica_count,
                .min_ranges = record.min_ranges,
            };
            return out;
        }

        pub fn freeProjectedTables(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.TableRecord) void {
            for (records) |record| {
                alloc.free(record.name);
                alloc.free(record.placement_role);
            }
            alloc.free(records);
        }

        pub fn listProjectedRanges(self: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.RangeRecord {
            const out = try alloc.alloc(metadata_table_manager.RangeRecord, self.ranges.len);
            errdefer alloc.free(out);
            for (self.ranges, 0..) |record, i| out[i] = .{
                .group_id = record.group_id,
                .table_id = record.table_id,
                .start_key = try alloc.dupe(u8, record.start_key),
                .end_key = if (record.end_key) |end| try alloc.dupe(u8, end) else null,
            };
            return out;
        }

        pub fn freeProjectedRanges(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.RangeRecord) void {
            for (records) |record| {
                alloc.free(record.start_key);
                if (record.end_key) |end| alloc.free(end);
            }
            alloc.free(records);
        }

        pub fn listProjectedStores(self: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.StoreRecord {
            const out = try alloc.alloc(metadata_table_manager.StoreRecord, self.stores.len);
            errdefer alloc.free(out);
            for (self.stores, 0..) |record, i| {
                out[i] = try metadata_table_manager.cloneStore(alloc, record);
            }
            return out;
        }

        pub fn freeProjectedStores(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.StoreRecord) void {
            for (records) |record| metadata_table_manager.freeStore(alloc, record);
            alloc.free(records);
        }

        pub fn listProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
            return try alloc.alloc(raft_reconciler.PlacementIntent, 0);
        }

        pub fn freeProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
            alloc.free(intents);
        }

        pub fn listProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator) ![]transition_state.SplitTransitionRecord {
            return try alloc.alloc(transition_state.SplitTransitionRecord, 0);
        }

        pub fn freeProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator, records: []transition_state.SplitTransitionRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator) ![]transition_state.MergeTransitionRecord {
            return try alloc.alloc(transition_state.MergeTransitionRecord, 0);
        }

        pub fn freeProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator, records: []transition_state.MergeTransitionRecord) void {
            alloc.free(records);
        }

        pub fn observeSplitTransition(_: *@This(), _: u64) !?transition_state.SplitObservation {
            return null;
        }

        pub fn observeMergeTransition(_: *@This(), _: u64) !?transition_state.MergeObservation {
            return null;
        }

        pub fn medianKeyLookup(self: *@This()) ?metadata_reconciler.MedianKeyLookup {
            return .{
                .ptr = self,
                .vtable = &.{
                    .fetch_median_key = fetchMedianKey,
                },
            };
        }

        fn fetchMedianKey(_: *anyopaque, alloc: std.mem.Allocator, _: u64) !?[]u8 {
            return try alloc.dupe(u8, "doc:m");
        }

        pub fn applyReconciliationPlan(self: *@This(), plan: *const metadata_reconciler.ReconciliationPlan) !void {
            if (self.planned_split_key) |value| {
                std.testing.allocator.free(value);
                self.planned_split_key = null;
            }
            if (plan.split_upserts.len != 0) {
                self.planned_split_key = try std.testing.allocator.dupe(u8, plan.split_upserts[0].split_key.?);
            }
        }
    };

    var loop = MetadataControlLoop.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
    });
    defer loop.deinit();

    const tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 49, .name = "docs" },
    };
    const ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 4901, .table_id = 49, .start_key = "doc:a", .end_key = "doc:z" },
    };
    const stores = [_]metadata_table_manager.StoreRecord{
        .{
            .store_id = 1,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .failure_domain = "rack-a",
            .live = true,
            .group_statuses = @constCast((&[_]metadata_table_manager.GroupStatusReport{
                .{
                    .group_id = 4901,
                    .doc_count = 200,
                    .disk_bytes = 200,
                    .empty = false,
                    .updated_at_millis = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms)),
                    .local_leader = true,
                },
            })[0..]),
        },
    };
    try loop.stateRef().tableManager().replaceTopology(&tables, &ranges);

    var fake = FakeService{
        .tables = &tables,
        .ranges = &ranges,
        .stores = &stores,
    };
    defer fake.deinit(std.testing.allocator);

    const summary = try loop.reconcileOnce(&fake);
    try std.testing.expectEqual(@as(usize, 1), summary.split_upserts);
    try std.testing.expect(fake.planned_split_key != null);
    try std.testing.expectEqualStrings("doc:m", fake.planned_split_key.?);
}

test "metadata control loop rewrites desired topology after finalized split" {
    const FakeService = struct {
        tables: []const metadata_table_manager.TableRecord,
        ranges: []const metadata_table_manager.RangeRecord,
        split_records: []const transition_state.SplitTransitionRecord,
        range_upserts: usize = 0,
        split_removals: usize = 0,

        pub fn listProjectedTables(self: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.TableRecord {
            const out = try alloc.alloc(metadata_table_manager.TableRecord, self.tables.len);
            errdefer alloc.free(out);
            for (self.tables, 0..) |record, i| out[i] = .{
                .table_id = record.table_id,
                .name = try alloc.dupe(u8, record.name),
                .placement_role = try alloc.dupe(u8, record.placement_role),
                .desired_replica_count = record.desired_replica_count,
                .min_ranges = record.min_ranges,
            };
            return out;
        }

        pub fn freeProjectedTables(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.TableRecord) void {
            for (records) |record| {
                alloc.free(record.name);
                alloc.free(record.placement_role);
            }
            alloc.free(records);
        }

        pub fn listProjectedRanges(self: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.RangeRecord {
            const out = try alloc.alloc(metadata_table_manager.RangeRecord, self.ranges.len);
            errdefer alloc.free(out);
            for (self.ranges, 0..) |record, i| out[i] = .{
                .group_id = record.group_id,
                .table_id = record.table_id,
                .start_key = try alloc.dupe(u8, record.start_key),
                .end_key = if (record.end_key) |end| try alloc.dupe(u8, end) else null,
            };
            return out;
        }

        pub fn freeProjectedRanges(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.RangeRecord) void {
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

        pub fn listProjectedSplitTransitions(self: *@This(), alloc: std.mem.Allocator) ![]transition_state.SplitTransitionRecord {
            const out = try alloc.alloc(transition_state.SplitTransitionRecord, self.split_records.len);
            errdefer alloc.free(out);
            for (self.split_records, 0..) |record, i| {
                out[i] = .{
                    .transition_id = record.transition_id,
                    .source_group_id = record.source_group_id,
                    .destination_group_id = record.destination_group_id,
                    .phase = record.phase,
                    .split_key = if (record.split_key) |value| try alloc.dupe(u8, value) else null,
                    .source_range_end = if (record.source_range_end) |value| try alloc.dupe(u8, value) else null,
                    .rollback_reason = null,
                };
            }
            return out;
        }

        pub fn freeProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator, records: []transition_state.SplitTransitionRecord) void {
            for (records) |record| metadata_table_manager.freeSplitTransitionRecord(alloc, record);
            alloc.free(records);
        }

        pub fn listProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator) ![]transition_state.MergeTransitionRecord {
            return try alloc.alloc(transition_state.MergeTransitionRecord, 0);
        }

        pub fn freeProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator, records: []transition_state.MergeTransitionRecord) void {
            alloc.free(records);
        }

        pub fn observeSplitTransition(_: *@This(), _: u64) !?transition_state.SplitObservation {
            return .{
                .status = .{
                    .phase = .finalized,
                    .source_split_phase = .none,
                    .bootstrapped = true,
                    .replay_required = false,
                    .replay_caught_up = true,
                    .cutover_ready = true,
                    .destination_ready_for_reads = true,
                    .source_delta_sequence = 1,
                    .dest_delta_sequence = 1,
                },
            };
        }

        pub fn observeMergeTransition(_: *@This(), _: u64) !?transition_state.MergeObservation {
            return null;
        }

        pub fn applyReconciliationPlan(self: *@This(), plan: *const metadata_reconciler.ReconciliationPlan) !void {
            self.range_upserts += plan.range_upserts.len;
            self.split_removals += plan.split_removals.len;
        }
    };

    var loop = MetadataControlLoop.init(std.testing.allocator);
    defer loop.deinit();

    const tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 10, .name = "docs" },
    };
    const ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 101, .table_id = 10, .start_key = "doc:a", .end_key = "doc:z" },
    };
    try loop.stateRef().tableManager().replaceTopology(&tables, &ranges);
    try loop.stateRef().tableManager().requestSplit(.{
        .transition_id = 8001,
        .table_id = 10,
        .source_group_id = 101,
        .destination_group_id = 102,
        .split_key = "doc:m",
    });

    const split_records = [_]transition_state.SplitTransitionRecord{
        .{
            .transition_id = 8001,
            .source_group_id = 101,
            .destination_group_id = 102,
            .phase = .finalizing,
            .split_key = "doc:m",
            .source_range_end = "doc:z",
        },
    };
    var fake = FakeService{
        .tables = &tables,
        .ranges = &ranges,
        .split_records = &split_records,
    };

    const summary = try loop.reconcileOnce(&fake);
    try std.testing.expectEqual(@as(usize, 2), summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), summary.split_removals);
    try std.testing.expectEqual(@as(usize, 2), fake.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), fake.split_removals);
}
