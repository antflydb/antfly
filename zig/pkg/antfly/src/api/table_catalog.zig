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
const metadata_admin = @import("../metadata/admin.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_server = @import("../metadata/server.zig");
const metadata_service = @import("../metadata/service.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const metadata_reconciler = @import("../metadata/reconciler.zig");
const platform_clock = @import("../platform/clock.zig");
const platform_time = @import("../platform/time.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const tables_api = @import("tables.zig");

pub const CatalogSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        admin_snapshot: *const fn (ptr: *anyopaque) anyerror!metadata_api.AdminSnapshot,
        free_admin_snapshot: *const fn (ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void,
    };

    pub fn adminSnapshot(self: CatalogSource) !metadata_api.AdminSnapshot {
        return try self.vtable.admin_snapshot(self.ptr);
    }

    pub fn freeAdminSnapshot(self: CatalogSource, snapshot: *metadata_api.AdminSnapshot) void {
        self.vtable.free_admin_snapshot(self.ptr, snapshot);
    }

    pub fn fromMetadataService(svc: *metadata_service.MetadataService) CatalogSource {
        return .{
            .ptr = svc,
            .vtable = &.{
                .admin_snapshot = metadataServiceAdminSnapshot,
                .free_admin_snapshot = metadataServiceFreeAdminSnapshot,
            },
        };
    }

    pub fn fromMetadataHttpService(svc: *metadata_service.MetadataHttpService) CatalogSource {
        return .{
            .ptr = svc,
            .vtable = &.{
                .admin_snapshot = metadataHttpServiceAdminSnapshot,
                .free_admin_snapshot = metadataHttpServiceFreeAdminSnapshot,
            },
        };
    }

    pub fn fromMetadataServer(srv: *metadata_server.MetadataServer) CatalogSource {
        return .{
            .ptr = srv,
            .vtable = &.{
                .admin_snapshot = metadataServerAdminSnapshot,
                .free_admin_snapshot = metadataServerFreeAdminSnapshot,
            },
        };
    }
};

pub fn emptyCatalogSource() CatalogSource {
    return .{
        .ptr = undefined,
        .vtable = &.{
            .admin_snapshot = emptyAdminSnapshot,
            .free_admin_snapshot = emptyFreeAdminSnapshot,
        },
    };
}

fn emptyAdminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
    return .{
        .status = .{
            .metadata_group_id = 0,
            .metrics = .{},
        },
        .tables = &.{},
        .ranges = &.{},
        .stores = &.{},
        .placement_intents = &.{},
        .split_transitions = &.{},
        .merge_transitions = &.{},
    };
}

fn emptyFreeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

pub const TableRangeRef = struct {
    group_id: u64,
    start_key: []const u8,
    end_key: ?[]const u8,
};

pub fn resolveSingleRangeGroup(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
) !?u64 {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return null;
    const ranges = try metadata_admin.listTableRanges(alloc, &snapshot, table.table_id);
    defer metadata_admin.freeRangeRefs(alloc, ranges);
    if (ranges.len == 0) return null;
    if (ranges.len != 1) return error.UnsupportedMultiRangeTable;
    return ranges[0].group_id;
}

pub fn resolveGroupForKey(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    key: []const u8,
) !?u64 {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return null;
    const ranges = try metadata_admin.listTableRanges(alloc, &snapshot, table.table_id);
    defer metadata_admin.freeRangeRefs(alloc, ranges);
    if (ranges.len == 0) return null;
    for (ranges) |range| {
        if (rangeContainsKey(range.*, key)) return range.group_id;
    }
    return null;
}

pub fn topologyEpoch(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
) !u64 {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return 0;
    const ranges = try metadata_admin.listTableRanges(alloc, &snapshot, table.table_id);
    defer metadata_admin.freeRangeRefs(alloc, ranges);

    sortRangeRefs(ranges);
    var hasher = std.hash.Wyhash.init(0);
    hasher.update(table.name);
    hasher.update(std.mem.asBytes(&table.table_id));
    hasher.update(std.mem.asBytes(&@as(u64, @intCast(ranges.len))));
    for (ranges) |range| {
        hasher.update(std.mem.asBytes(&range.group_id));
        hasher.update(range.start_key);
        if (range.end_key) |end_key| {
            hasher.update(&[_]u8{1});
            hasher.update(end_key);
        } else {
            hasher.update(&[_]u8{0});
        }
    }
    return hasher.final();
}

pub fn validateTopologyEpoch(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    expected_epoch: u64,
) !void {
    if (expected_epoch == 0) return;
    const actual_epoch = try topologyEpoch(alloc, catalog, table_name);
    if (actual_epoch != expected_epoch) return error.TopologyChanged;
}

pub fn validateDocIdentityReadyForTable(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
) !void {
    return validateDocIdentityReadyForTableMode(alloc, catalog, table_name, false);
}

pub fn validateDocIdentityReadyForTableStrict(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
) !void {
    return validateDocIdentityReadyForTableMode(alloc, catalog, table_name, true);
}

pub fn validateResolvedDocFilterContextForGroups(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    group_ids: []const u64,
    namespace_table_id: u64,
    namespace_shard_id: u64,
    namespace_range_id: u64,
) !void {
    _ = alloc;
    if (group_ids.len == 0) return;
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
    for (group_ids) |group_id| {
        const range = findRangeForTableGroup(snapshot.ranges, table.table_id, group_id) orelse return error.DocIdentityNamespaceMismatch;
        const status = findMergedGroupStatus(snapshot.merged_group_statuses, group_id) orelse return error.DocIdentityNamespaceMismatch;
        if (status.doc_identity_reassignment_active) return error.DocIdentityNamespaceMismatch;
        if (status.doc_identity_namespace_conflict) return error.DocIdentityNamespaceMismatch;
        if (status.doc_identity.rebuild_required) return error.DocIdentityNamespaceMismatch;
        if (!runtimeDocIdentityCanAcceptNamespace(status.doc_identity, range, namespace_table_id, namespace_shard_id, namespace_range_id)) {
            return error.DocIdentityNamespaceMismatch;
        }
    }
}

fn validateDocIdentityReadyForTableMode(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    require_runtime_status: bool,
) !void {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return;
    const ranges = try metadata_admin.listTableRanges(alloc, &snapshot, table.table_id);
    defer metadata_admin.freeRangeRefs(alloc, ranges);
    for (ranges) |range| {
        const status = findMergedGroupStatus(snapshot.merged_group_statuses, range.group_id) orelse {
            if (require_runtime_status) return error.DocIdentityNamespaceMismatch;
            continue;
        };
        if (status.doc_identity_reassignment_active) return error.DocIdentityNamespaceMismatch;
        if (status.doc_identity_namespace_conflict) return error.DocIdentityNamespaceMismatch;
        if (status.doc_identity.rebuild_required) return error.DocIdentityNamespaceMismatch;
        if (!runtimeDocIdentityMatchesRange(status.doc_identity, range.*)) return error.DocIdentityNamespaceMismatch;
    }
}

pub fn resolveGroupForKeyPinned(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    key: []const u8,
    expected_epoch: u64,
) !?u64 {
    try validateTopologyEpoch(alloc, catalog, table_name, expected_epoch);
    return try resolveGroupForKey(alloc, catalog, table_name, key);
}

pub fn resolveGroupsForSpan(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
) ![]u64 {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return try alloc.alloc(u64, 0);
    const ranges = try metadata_admin.listTableRanges(alloc, &snapshot, table.table_id);
    defer metadata_admin.freeRangeRefs(alloc, ranges);

    sortRangeRefs(ranges);
    var groups = std.ArrayListUnmanaged(u64).empty;
    defer groups.deinit(alloc);
    for (ranges) |range| {
        if (!rangeOverlapsSpan(range.*, from_key, to_key)) continue;
        try groups.append(alloc, range.group_id);
    }
    return try groups.toOwnedSlice(alloc);
}

pub fn resolveGroupsForSpanEventually(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    timeout_ns: u64,
    poll_interval_ms: u64,
) ![]u64 {
    const start_ns = platform_time.monotonicNs();
    while (true) {
        const groups = try resolveGroupsForSpan(alloc, catalog, table_name, from_key, to_key);
        if (groups.len != 0) return groups;
        if (platform_time.monotonicNs() -| start_ns >= timeout_ns) return groups;
        alloc.free(groups);
        platform_clock.Clock.real().sleepMs(poll_interval_ms);
    }
}

fn metadataServiceAdminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
    const svc: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
    return try svc.adminSnapshot();
}

fn metadataServiceFreeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
    const svc: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
    svc.freeAdminSnapshot(snapshot);
}

fn metadataHttpServiceAdminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
    const svc: *metadata_service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    return try svc.adminSnapshot();
}

fn metadataHttpServiceFreeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
    const svc: *metadata_service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    svc.freeAdminSnapshot(snapshot);
}

fn metadataServerAdminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
    const srv: *metadata_server.MetadataServer = @ptrCast(@alignCast(ptr));
    return try srv.adminSnapshot();
}

fn metadataServerFreeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
    const srv: *metadata_server.MetadataServer = @ptrCast(@alignCast(ptr));
    srv.freeAdminSnapshot(snapshot);
}

fn sortRangeRefs(ranges: []const *const metadata_table_manager.RangeRecord) void {
    std.sort.insertion(*const metadata_table_manager.RangeRecord, @constCast(ranges), {}, struct {
        fn lessThan(_: void, a: *const metadata_table_manager.RangeRecord, b: *const metadata_table_manager.RangeRecord) bool {
            return std.mem.order(u8, a.start_key, b.start_key) == .lt;
        }
    }.lessThan);
}

fn rangeContainsKey(range: metadata_table_manager.RangeRecord, key: []const u8) bool {
    if (range.start_key.len > 0 and std.mem.order(u8, key, range.start_key) == .lt) return false;
    if (range.end_key) |end_key| {
        if (end_key.len > 0 and std.mem.order(u8, key, end_key) != .lt) return false;
    }
    return true;
}

fn rangeOverlapsSpan(range: metadata_table_manager.RangeRecord, from_key: []const u8, to_key: []const u8) bool {
    if (to_key.len > 0 and std.mem.order(u8, range.start_key, to_key) != .lt) return false;
    if (range.end_key) |end_key| {
        if (end_key.len > 0 and from_key.len > 0 and std.mem.order(u8, end_key, from_key) != .gt) return false;
    }
    return true;
}

fn findMergedGroupStatus(statuses: []const metadata_reconciler.MergedGroupStatus, group_id: u64) ?metadata_reconciler.MergedGroupStatus {
    for (statuses) |status| {
        if (status.group_id == group_id) return status;
    }
    return null;
}

fn findRangeForTableGroup(
    ranges: []const metadata_table_manager.RangeRecord,
    table_id: u64,
    group_id: u64,
) ?metadata_table_manager.RangeRecord {
    for (ranges) |range| {
        if (range.table_id == table_id and range.group_id == group_id) return range;
    }
    return null;
}

fn runtimeDocIdentityCanAcceptNamespace(
    stats: metadata_table_manager.RuntimeDocIdentityStatusReport,
    range: metadata_table_manager.RangeRecord,
    namespace_table_id: u64,
    namespace_shard_id: u64,
    namespace_range_id: u64,
) bool {
    if (!runtimeDocIdentityHasOrdinalRows(stats)) return false;
    if (!runtimeDocIdentityMatchesRange(stats, range)) return false;
    return stats.namespace_table_id == namespace_table_id and
        stats.namespace_shard_id == namespace_shard_id and
        stats.namespace_range_id == namespace_range_id;
}

fn runtimeDocIdentityMatchesRange(
    stats: metadata_table_manager.RuntimeDocIdentityStatusReport,
    range: metadata_table_manager.RangeRecord,
) bool {
    if (!runtimeDocIdentityHasNamespace(stats)) return true;
    return stats.namespace_table_id == range.table_id and
        stats.namespace_shard_id == metadata_table_manager.rangeDocIdentityShardId(range) and
        stats.namespace_range_id == metadata_table_manager.rangeDocIdentityRangeId(range);
}

fn runtimeDocIdentityHasNamespace(stats: metadata_table_manager.RuntimeDocIdentityStatusReport) bool {
    return stats.namespace_table_id != 0 or
        stats.namespace_shard_id != 0 or
        stats.namespace_range_id != 0;
}

fn runtimeDocIdentityHasOrdinalRows(stats: metadata_table_manager.RuntimeDocIdentityStatusReport) bool {
    return stats.next_ordinal != 1 or
        stats.allocated_ordinals != 0 or
        stats.state_rows != 0 or
        stats.live_ordinals != 0 or
        stats.tombstone_ordinals != 0;
}

test "catalog source resolves a single-range table group" {
    const FakeCatalog = struct {
        fn iface() CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const group_id = (try resolveSingleRangeGroup(std.testing.allocator, FakeCatalog.iface(), "docs")).?;
    try std.testing.expectEqual(@as(u64, 7001), group_id);
}

test "catalog source resolves groups by key and span" {
    const FakeCatalog = struct {
        fn iface() CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    try std.testing.expectEqual(@as(u64, 7001), (try resolveGroupForKey(std.testing.allocator, FakeCatalog.iface(), "docs", "doc:a")).?);
    try std.testing.expectEqual(@as(u64, 7002), (try resolveGroupForKey(std.testing.allocator, FakeCatalog.iface(), "docs", "doc:z")).?);

    const groups = try resolveGroupsForSpan(std.testing.allocator, FakeCatalog.iface(), "docs", "doc:b", "doc:z");
    defer std.testing.allocator.free(groups);
    try std.testing.expectEqual(@as(usize, 2), groups.len);
    try std.testing.expectEqual(@as(u64, 7001), groups[0]);
    try std.testing.expectEqual(@as(u64, 7002), groups[1]);
}

test "catalog doc identity readiness checks table range health" {
    const alloc = std.testing.allocator;

    const TestState = struct {
        statuses: []const metadata_reconciler.MergedGroupStatus = &.{},
    };

    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 7, .name = "docs", .placement_role = "data" },
            .{ .table_id = 8, .name = "other", .placement_role = "data" },
        };
        const ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
            .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
            .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = null },
        };

        fn iface(state: *TestState) CatalogSource {
            return .{
                .ptr = state,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(ranges[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(state.statuses),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var missing_statuses = TestState{};
    try validateDocIdentityReadyForTable(alloc, FakeCatalog.iface(&missing_statuses), "docs");
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateDocIdentityReadyForTableStrict(alloc, FakeCatalog.iface(&missing_statuses), "docs"));

    const healthy = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001, .allocated_ordinals = 1 } },
        .{ .group_id = 7002, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7002, .namespace_range_id = 7002, .allocated_ordinals = 1 } },
        .{ .group_id = 8001, .doc_identity = .{ .rebuild_required = true } },
    };
    var healthy_state = TestState{ .statuses = healthy[0..] };
    try validateDocIdentityReadyForTable(alloc, FakeCatalog.iface(&healthy_state), "docs");
    try validateDocIdentityReadyForTableStrict(alloc, FakeCatalog.iface(&healthy_state), "docs");
    try validateResolvedDocFilterContextForGroups(alloc, FakeCatalog.iface(&healthy_state), "docs", &.{7001}, 7, 7001, 7001);
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateResolvedDocFilterContextForGroups(alloc, FakeCatalog.iface(&healthy_state), "docs", &.{ 7001, 7002 }, 7, 7001, 7001));

    const mixed_version = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001 } },
        .{ .group_id = 7002, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7002, .namespace_range_id = 7002, .allocated_ordinals = 1 } },
    };
    var mixed_state = TestState{ .statuses = mixed_version[0..] };
    try validateDocIdentityReadyForTable(alloc, FakeCatalog.iface(&mixed_state), "docs");
    try validateDocIdentityReadyForTableStrict(alloc, FakeCatalog.iface(&mixed_state), "docs");
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateResolvedDocFilterContextForGroups(alloc, FakeCatalog.iface(&mixed_state), "docs", &.{7001}, 7, 7001, 7001));

    const rebuild_required = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001 },
        .{ .group_id = 7002, .doc_identity = .{ .rebuild_required = true } },
    };
    var rebuild_state = TestState{ .statuses = rebuild_required[0..] };
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateDocIdentityReadyForTable(alloc, FakeCatalog.iface(&rebuild_state), "docs"));

    const namespace_conflict = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001, .doc_identity_namespace_conflict = true },
        .{ .group_id = 7002 },
    };
    var conflict_state = TestState{ .statuses = namespace_conflict[0..] };
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateDocIdentityReadyForTable(alloc, FakeCatalog.iface(&conflict_state), "docs"));

    const reassignment_active = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001, .doc_identity_reassignment_active = true },
        .{ .group_id = 7002 },
    };
    var reassignment_state = TestState{ .statuses = reassignment_active[0..] };
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateDocIdentityReadyForTable(alloc, FakeCatalog.iface(&reassignment_state), "docs"));

    const stale_namespace = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001, .allocated_ordinals = 1 } },
        .{ .group_id = 7002, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001, .allocated_ordinals = 1 } },
    };
    var stale_state = TestState{ .statuses = stale_namespace[0..] };
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateDocIdentityReadyForTable(alloc, FakeCatalog.iface(&stale_state), "docs"));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateResolvedDocFilterContextForGroups(alloc, FakeCatalog.iface(&stale_state), "docs", &.{7002}, 7, 7001, 7001));

    const empty_stale_namespace = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001 } },
        .{ .group_id = 7002, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001 } },
    };
    var empty_stale_state = TestState{ .statuses = empty_stale_namespace[0..] };
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateDocIdentityReadyForTable(alloc, FakeCatalog.iface(&empty_stale_state), "docs"));
}

test "catalog resolved filter validation accepts preserved split identity domains" {
    const TestState = struct {
        statuses: []const metadata_reconciler.MergedGroupStatus = &.{},
    };

    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 7, .name = "docs", .placement_role = "data" },
        };
        const ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 7001, .range_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
            .{
                .group_id = 7002,
                .range_id = 7002,
                .table_id = 7,
                .start_key = "doc:m",
                .end_key = null,
                .doc_identity_shard_id = 7001,
                .doc_identity_range_id = 7001,
            },
        };

        fn iface(state: *TestState) CatalogSource {
            return .{
                .ptr = state,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(ranges[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(state.statuses),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var missing_state = TestState{};
    try validateDocIdentityReadyForTable(std.testing.allocator, FakeCatalog.iface(&missing_state), "docs");
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateDocIdentityReadyForTableStrict(std.testing.allocator, FakeCatalog.iface(&missing_state), "docs"));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateResolvedDocFilterContextForGroups(std.testing.allocator, FakeCatalog.iface(&missing_state), "docs", &.{7002}, 7, 7001, 7001));

    const old_statuses = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001 } },
        .{ .group_id = 7002, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001 } },
    };
    var old_state = TestState{ .statuses = old_statuses[0..] };
    try validateDocIdentityReadyForTableStrict(std.testing.allocator, FakeCatalog.iface(&old_state), "docs");
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateResolvedDocFilterContextForGroups(std.testing.allocator, FakeCatalog.iface(&old_state), "docs", &.{ 7001, 7002 }, 7, 7001, 7001));

    const stale_statuses = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001, .allocated_ordinals = 1 } },
        .{ .group_id = 7002, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7002, .namespace_range_id = 7002, .allocated_ordinals = 1 } },
    };
    var stale_state = TestState{ .statuses = stale_statuses[0..] };
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateDocIdentityReadyForTable(std.testing.allocator, FakeCatalog.iface(&stale_state), "docs"));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, validateResolvedDocFilterContextForGroups(std.testing.allocator, FakeCatalog.iface(&stale_state), "docs", &.{7002}, 7, 7001, 7001));

    const statuses = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 7001, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001, .allocated_ordinals = 1 } },
        .{ .group_id = 7002, .doc_identity = .{ .namespace_table_id = 7, .namespace_shard_id = 7001, .namespace_range_id = 7001, .allocated_ordinals = 1 } },
    };
    var state = TestState{ .statuses = statuses[0..] };
    try validateDocIdentityReadyForTableStrict(std.testing.allocator, FakeCatalog.iface(&state), "docs");
    try validateResolvedDocFilterContextForGroups(std.testing.allocator, FakeCatalog.iface(&state), "docs", &.{ 7001, 7002 }, 7, 7001, 7001);
}
