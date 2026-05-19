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
const api = @import("api.zig");
const table_manager = @import("table_manager.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const transition_state = @import("transition_state.zig");

pub const ActiveTransitionCounts = struct {
    split: usize = 0,
    merge: usize = 0,
};

pub const ActiveTransitions = struct {
    split: []const *const transition_state.SplitTransitionRecord,
    merge: []const *const transition_state.MergeTransitionRecord,
};

pub fn findTable(snapshot: *const api.AdminSnapshot, table_id: u64) ?*const table_manager.TableRecord {
    for (snapshot.tables) |*record| {
        if (record.table_id == table_id) return record;
    }
    return null;
}

pub fn findRange(snapshot: *const api.AdminSnapshot, group_id: u64) ?*const table_manager.RangeRecord {
    for (snapshot.ranges) |*record| {
        if (record.group_id == group_id) return record;
    }
    return null;
}

pub fn findStore(snapshot: *const api.AdminSnapshot, store_id: u64) ?*const table_manager.StoreRecord {
    for (snapshot.stores) |*record| {
        if (record.store_id == store_id) return record;
    }
    return null;
}

pub fn countActiveTransitions(snapshot: *const api.AdminSnapshot) ActiveTransitionCounts {
    var counts: ActiveTransitionCounts = .{};
    for (snapshot.split_transitions) |record| {
        if (splitTransitionActive(record)) counts.split += 1;
    }
    for (snapshot.merge_transitions) |record| {
        if (mergeTransitionActive(record)) counts.merge += 1;
    }
    return counts;
}

pub fn listTableRanges(
    alloc: std.mem.Allocator,
    snapshot: *const api.AdminSnapshot,
    table_id: u64,
) ![]const *const table_manager.RangeRecord {
    var count: usize = 0;
    for (snapshot.ranges) |record| {
        if (record.table_id == table_id) count += 1;
    }
    const out = try alloc.alloc(*const table_manager.RangeRecord, count);
    var index: usize = 0;
    for (snapshot.ranges) |*record| {
        if (record.table_id != table_id) continue;
        out[index] = record;
        index += 1;
    }
    return out;
}

pub fn listGroupPlacement(
    alloc: std.mem.Allocator,
    snapshot: *const api.AdminSnapshot,
    group_id: u64,
) ![]const *const raft_reconciler.PlacementIntent {
    var count: usize = 0;
    for (snapshot.placement_intents) |intent| {
        if (intent.record.group_id == group_id) count += 1;
    }
    const out = try alloc.alloc(*const raft_reconciler.PlacementIntent, count);
    var index: usize = 0;
    for (snapshot.placement_intents) |*intent| {
        if (intent.record.group_id != group_id) continue;
        out[index] = intent;
        index += 1;
    }
    return out;
}

pub fn listActiveTransitions(
    alloc: std.mem.Allocator,
    snapshot: *const api.AdminSnapshot,
) !ActiveTransitions {
    const counts = countActiveTransitions(snapshot);
    const split = try alloc.alloc(*const transition_state.SplitTransitionRecord, counts.split);
    errdefer alloc.free(split);
    const merge = try alloc.alloc(*const transition_state.MergeTransitionRecord, counts.merge);
    errdefer alloc.free(merge);

    var split_index: usize = 0;
    for (snapshot.split_transitions) |*record| {
        if (!splitTransitionActive(record.*)) continue;
        split[split_index] = record;
        split_index += 1;
    }

    var merge_index: usize = 0;
    for (snapshot.merge_transitions) |*record| {
        if (!mergeTransitionActive(record.*)) continue;
        merge[merge_index] = record;
        merge_index += 1;
    }

    return .{ .split = split, .merge = merge };
}

pub fn freeRangeRefs(alloc: std.mem.Allocator, refs: []const *const table_manager.RangeRecord) void {
    alloc.free(refs);
}

pub fn freePlacementRefs(alloc: std.mem.Allocator, refs: []const *const raft_reconciler.PlacementIntent) void {
    alloc.free(refs);
}

pub fn freeActiveTransitions(alloc: std.mem.Allocator, refs: *ActiveTransitions) void {
    alloc.free(refs.split);
    alloc.free(refs.merge);
    refs.* = undefined;
}

fn splitTransitionActive(record: transition_state.SplitTransitionRecord) bool {
    return switch (record.phase) {
        .finalized, .rolled_back => false,
        else => true,
    };
}

fn mergeTransitionActive(record: transition_state.MergeTransitionRecord) bool {
    return switch (record.phase) {
        .finalized, .rolled_back => false,
        else => true,
    };
}

test "metadata admin helpers filter snapshot state" {
    var tables = [_]table_manager.TableRecord{
        .{ .table_id = 1, .name = "docs", .placement_role = "data" },
        .{ .table_id = 2, .name = "logs", .placement_role = "data" },
    };
    var ranges = [_]table_manager.RangeRecord{
        .{ .group_id = 10, .table_id = 1, .start_key = "doc:a", .end_key = "doc:m" },
        .{ .group_id = 11, .table_id = 1, .start_key = "doc:m", .end_key = "doc:z" },
        .{ .group_id = 20, .table_id = 2, .start_key = "log:a", .end_key = "log:z" },
    };
    var stores = [_]table_manager.StoreRecord{
        .{ .store_id = 7, .node_id = 1, .role = "data" },
    };
    var group10_peers1 = [_]u64{2};
    var group10_peers2 = [_]u64{1};
    var group20_peers = [_]u64{1};
    var placement_intents = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 10, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .peer_node_ids = group10_peers1[0..] },
        .{ .record = .{ .group_id = 10, .replica_id = 2, .local_node_id = 2, .bootstrap_mode = .persisted }, .peer_node_ids = group10_peers2[0..] },
        .{ .record = .{ .group_id = 20, .replica_id = 3, .local_node_id = 3, .bootstrap_mode = .persisted }, .peer_node_ids = group20_peers[0..] },
    };
    var split_transitions = [_]transition_state.SplitTransitionRecord{
        .{ .transition_id = 9001, .source_group_id = 10, .destination_group_id = 12, .phase = .bootstrap_peer },
        .{ .transition_id = 9002, .source_group_id = 11, .destination_group_id = 13, .phase = .finalized },
    };
    var merge_transitions = [_]transition_state.MergeTransitionRecord{
        .{ .transition_id = 9010, .donor_group_id = 20, .receiver_group_id = 10, .phase = .prepare },
        .{ .transition_id = 9011, .donor_group_id = 21, .receiver_group_id = 11, .phase = .rolled_back },
    };
    const snapshot: api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 77, .metrics = .{} },
        .tables = tables[0..],
        .ranges = ranges[0..],
        .stores = stores[0..],
        .placement_intents = placement_intents[0..],
        .split_transitions = split_transitions[0..],
        .merge_transitions = merge_transitions[0..],
    };

    try std.testing.expect(findTable(&snapshot, 1) != null);
    try std.testing.expect(findRange(&snapshot, 11) != null);
    try std.testing.expect(findStore(&snapshot, 7) != null);

    const counts = countActiveTransitions(&snapshot);
    try std.testing.expectEqual(@as(usize, 1), counts.split);
    try std.testing.expectEqual(@as(usize, 1), counts.merge);

    const table_ranges = try listTableRanges(std.testing.allocator, &snapshot, 1);
    defer freeRangeRefs(std.testing.allocator, table_ranges);
    try std.testing.expectEqual(@as(usize, 2), table_ranges.len);
    try std.testing.expectEqual(@as(u64, 10), table_ranges[0].group_id);

    const group_placement = try listGroupPlacement(std.testing.allocator, &snapshot, 10);
    defer freePlacementRefs(std.testing.allocator, group_placement);
    try std.testing.expectEqual(@as(usize, 2), group_placement.len);

    var active = try listActiveTransitions(std.testing.allocator, &snapshot);
    defer freeActiveTransitions(std.testing.allocator, &active);
    try std.testing.expectEqual(@as(usize, 1), active.split.len);
    try std.testing.expectEqual(@as(u64, 9001), active.split[0].transition_id);
    try std.testing.expectEqual(@as(usize, 1), active.merge.len);
    try std.testing.expectEqual(@as(u64, 9010), active.merge[0].transition_id);
}
