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
const metadata_admin = @import("../admin.zig");
const metadata_api = @import("snapshot.zig");
const metadata_catalog_lookup = @import("lookup.zig");
const metadata_catalog_source = @import("source.zig");
const metadata_table_manager = @import("../table_manager.zig");
const metadata_transition_state = @import("../transition_state.zig");
const metadata_reconciler = @import("../reconciler.zig");
const platform_clock = @import("../../platform/clock.zig");
const platform_time = @import("../../platform/time.zig");
const raft_reconciler = @import("../../raft/reconciler.zig");
const sql_schema_mutation = @import("../../sql/schema_mutation.zig");
const catalog_resources = @import("resources.zig");
const table_ddl = @import("table_ddl.zig");

pub const CatalogSource = metadata_catalog_source.CatalogSource;
pub const emptyCatalogSource = metadata_catalog_source.emptyCatalogSource;
pub const unavailableCatalogSource = metadata_catalog_source.unavailableCatalogSource;

pub fn nativeTableNameForCatalogTargetAlloc(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    target: catalog_resources.TableTarget,
) ![]u8 {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    _ = metadata_catalog_lookup.findTableByQualifiedName(&snapshot, target.database_name, target.namespace_name, target.table_name) orelse return error.TableNotFound;
    return try catalog_resources.storageTableNameForTargetAlloc(alloc, target);
}

pub fn nativeTableNameForCatalogCreateTargetAlloc(
    alloc: std.mem.Allocator,
    target: catalog_resources.TableTarget,
) ![]u8 {
    return try catalog_resources.storageTableNameForTargetAlloc(alloc, target);
}

pub const TableRangeRef = struct {
    group_id: u64,
    start_key: []const u8,
    end_key: ?[]const u8,
};

pub const ForeignKeyRefOwnerResolution = struct {
    configured: bool,
    topology_epoch: u64 = 0,
    groups: []u64 = &.{},

    pub fn deinit(self: *ForeignKeyRefOwnerResolution, alloc: std.mem.Allocator) void {
        if (self.groups.len > 0) alloc.free(self.groups);
        self.* = undefined;
    }
};

pub const UniqueConstraintOwnerResolution = struct {
    configured: bool,
    topology_epoch: u64 = 0,
    groups: []u64 = &.{},

    pub fn deinit(self: *UniqueConstraintOwnerResolution, alloc: std.mem.Allocator) void {
        if (self.groups.len > 0) alloc.free(self.groups);
        self.* = undefined;
    }
};

pub const TableDocKeyRangePlan = struct {
    group_id: u64,
    start_key: []u8,
    end_key: []u8,
    topology_epoch: u64 = 0,
    inclusive_start: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.start_key.len > 0) alloc.free(self.start_key);
        if (self.end_key.len > 0) alloc.free(self.end_key);
        self.* = undefined;
    }
};

pub fn resolveSingleRangeGroup(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
) !?u64 {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = metadata_catalog_lookup.findTableByName(&snapshot, table_name) orelse return null;
    const ranges = try metadata_admin.listTableRanges(alloc, &snapshot, table.table_id);
    defer metadata_admin.freeRangeRefs(alloc, ranges);
    if (ranges.len == 0) return null;
    if (ranges.len != 1) return error.UnsupportedMultiRangeTable;
    return ranges[0].group_id;
}

pub fn resolveForeignKeyRefOwnerGroups(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    child_table_name: []const u8,
    constraint_name: []const u8,
    parent_table_name: []const u8,
    parent_key: []const u8,
) !ForeignKeyRefOwnerResolution {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const child_table = metadata_catalog_lookup.findTableByName(&snapshot, child_table_name) orelse return .{ .configured = false };
    const parent_table = metadata_catalog_lookup.findTableByName(&snapshot, parent_table_name) orelse return .{ .configured = false };

    var configured = false;
    var groups = std.ArrayListUnmanaged(u64).empty;
    defer groups.deinit(alloc);
    var hasher = std.hash.Wyhash.init(0);
    hasher.update(child_table.name);
    hasher.update(parent_table.name);
    hasher.update(constraint_name);

    for (snapshot.foreign_key_ref_ranges) |range| {
        if (range.child_table_id != child_table.table_id) continue;
        if (range.parent_table_id != parent_table.table_id) continue;
        if (!std.mem.eql(u8, range.constraint_name, constraint_name)) continue;
        configured = true;
        hashForeignKeyRefRange(&hasher, range);
        if (!metadata_table_manager.foreignKeyReferenceRangeRoutable(range)) continue;
        if (!foreignKeyRefRangeContainsParentKey(range, parent_key)) continue;
        if (!containsGroup(groups.items, range.group_id)) try groups.append(alloc, range.group_id);
    }
    if (!configured) return .{ .configured = false };
    return .{
        .configured = true,
        .topology_epoch = hasher.final(),
        .groups = try groups.toOwnedSlice(alloc),
    };
}

pub fn resolveUniqueConstraintOwnerGroups(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    constraint_name: []const u8,
    encoded_value: []const u8,
) !UniqueConstraintOwnerResolution {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = metadata_catalog_lookup.findTableByName(&snapshot, table_name) orelse return .{ .configured = false };

    var configured = false;
    var groups = std.ArrayListUnmanaged(u64).empty;
    defer groups.deinit(alloc);
    var hasher = std.hash.Wyhash.init(0);
    hasher.update(table.name);
    hasher.update(std.mem.asBytes(&table.table_id));
    hasher.update(constraint_name);

    for (snapshot.unique_constraint_ranges) |range| {
        if (range.table_id != table.table_id) continue;
        if (!std.mem.eql(u8, range.constraint_name, constraint_name)) continue;
        configured = true;
        hashUniqueConstraintRange(&hasher, range);
        if (!metadata_table_manager.uniqueConstraintRangeRoutable(range)) continue;
        if (!uniqueConstraintRangeContainsEncodedValue(range, encoded_value)) continue;
        if (!containsGroup(groups.items, range.group_id)) try groups.append(alloc, range.group_id);
    }
    if (!configured) return .{ .configured = false };
    return .{
        .configured = true,
        .topology_epoch = hasher.final(),
        .groups = try groups.toOwnedSlice(alloc),
    };
}

pub fn resolveGroupForKey(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    key: []const u8,
) !?u64 {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = metadata_catalog_lookup.findTableByName(&snapshot, table_name) orelse return null;
    const ranges = try metadata_admin.listTableRanges(alloc, &snapshot, table.table_id);
    defer metadata_admin.freeRangeRefs(alloc, ranges);
    if (ranges.len == 0) return null;
    return resolveGroupForKeyFromRanges(ranges, key);
}

pub fn resolveTableDocKeyRanges(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
) ![]TableDocKeyRangePlan {
    if (from_key.len > 0 and to_key.len > 0 and std.mem.order(u8, from_key, to_key) != .lt) return error.InvalidRangeBounds;

    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = metadata_catalog_lookup.findTableByName(&snapshot, table_name) orelse return try alloc.alloc(TableDocKeyRangePlan, 0);
    const ranges = try metadata_admin.listTableRanges(alloc, &snapshot, table.table_id);
    defer metadata_admin.freeRangeRefs(alloc, ranges);
    sortRangeRefs(ranges);

    const epoch = tableTopologyEpochFromRanges(table.*, ranges);
    var plans = std.ArrayListUnmanaged(TableDocKeyRangePlan).empty;
    errdefer {
        for (plans.items) |*plan| plan.deinit(alloc);
        plans.deinit(alloc);
    }

    for (ranges) |range| {
        if (!rangeOverlapsSpan(range.*, from_key, to_key)) continue;
        const clipped_start = clippedRangeStart(range.start_key, from_key);
        const clipped_end = clippedRangeEnd(range.end_key, to_key);
        if (clipped_start.len > 0 and clipped_end.len > 0 and std.mem.order(u8, clipped_start, clipped_end) != .lt) continue;
        try plans.append(alloc, .{
            .group_id = range.group_id,
            .start_key = try alloc.dupe(u8, clipped_start),
            .end_key = try alloc.dupe(u8, clipped_end),
            .topology_epoch = epoch,
            .inclusive_start = from_key.len == 0 or (range.start_key.len > 0 and std.mem.order(u8, from_key, range.start_key) == .lt),
        });
    }
    return try plans.toOwnedSlice(alloc);
}

pub fn resolveGroupForKeyFromRanges(
    ranges: []const *const metadata_table_manager.RangeRecord,
    key: []const u8,
) ?u64 {
    for (ranges) |range| {
        if (rangeContainsKey(range.*, key)) return range.group_id;
    }
    return null;
}

/// Whether a table with this name currently exists in the catalog. Used by
/// cross-table graph hydration to fail closed (skip) rather than error when a
/// node references a dropped table.
pub fn tableExists(
    catalog: CatalogSource,
    table_name: []const u8,
) !bool {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    return metadata_catalog_lookup.findTableByName(&snapshot, table_name) != null;
}

pub fn topologyEpoch(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
) !u64 {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = metadata_catalog_lookup.findTableByName(&snapshot, table_name) orelse return 0;
    const ranges = try metadata_admin.listTableRanges(alloc, &snapshot, table.table_id);
    defer metadata_admin.freeRangeRefs(alloc, ranges);

    return tableTopologyEpochFromRanges(table.*, ranges);
}

pub fn tableTopologyEpochFromRanges(
    table: metadata_table_manager.TableRecord,
    ranges: []const *const metadata_table_manager.RangeRecord,
) u64 {
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

pub fn tableSchemaJsonAlloc(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
) !?[]u8 {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = metadata_catalog_lookup.findTableByName(&snapshot, table_name) orelse return null;
    if (table.schema_json.len == 0) return null;
    return try alloc.dupe(u8, table.schema_json);
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

pub fn validateGroupTopologyEpoch(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    group_id: u64,
    expected_epoch: u64,
) !void {
    if (expected_epoch == 0) return;
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = metadata_catalog_lookup.findTableByName(&snapshot, table_name) orelse return error.TopologyChanged;
    const ranges = try metadata_admin.listTableRanges(alloc, &snapshot, table.table_id);
    defer metadata_admin.freeRangeRefs(alloc, ranges);
    if (rangeRefsContainGroup(ranges, group_id) and tableTopologyEpochFromRanges(table.*, ranges) == expected_epoch) return;
    if (foreignKeyRefTopologyEpochMatchesGroup(&snapshot, table.*, group_id, expected_epoch)) return;
    if (uniqueConstraintTopologyEpochMatchesGroup(&snapshot, table.*, group_id, expected_epoch)) return;
    return error.TopologyChanged;
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
    const table = metadata_catalog_lookup.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
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
    const table = metadata_catalog_lookup.findTableByName(&snapshot, table_name) orelse return;
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
    const table = metadata_catalog_lookup.findTableByName(&snapshot, table_name) orelse return try alloc.alloc(u64, 0);
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

pub fn promoteUniqueConstraintEnforced(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    constraint_name: []const u8,
) !bool {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = metadata_catalog_lookup.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
    const schema_json = sql_schema_mutation.schemaWithUniqueConstraintValidationStateAlloc(
        alloc,
        table.schema_json,
        constraint_name,
        .enforced,
    ) catch |err| switch (err) {
        error.UniqueConstraintNotFound,
        error.InvalidSchemaUpdateRequest,
        => return false,
        else => return err,
    };
    defer alloc.free(schema_json);
    const updated = try table_ddl.applySchemaUpdateRecord(alloc, table, schema_json);
    defer metadata_table_manager.freeTable(alloc, updated);
    try catalog.compareAndSwapTableSchema(.{
        .table_id = table.table_id,
        .expected_schema_json = table.schema_json,
        .promoted_table = updated,
    });
    return true;
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

fn clippedRangeStart(range_start: []const u8, from_key: []const u8) []const u8 {
    if (from_key.len == 0) return range_start;
    if (range_start.len == 0) return from_key;
    return if (std.mem.order(u8, from_key, range_start) == .gt) from_key else range_start;
}

fn clippedRangeEnd(range_end: ?[]const u8, to_key: []const u8) []const u8 {
    const end = range_end orelse "";
    if (to_key.len == 0) return end;
    if (end.len == 0) return to_key;
    return if (std.mem.order(u8, end, to_key) == .lt) end else to_key;
}

fn rangeRefsContainGroup(ranges: []const *const metadata_table_manager.RangeRecord, group_id: u64) bool {
    for (ranges) |range| {
        if (range.group_id == group_id) return true;
    }
    return false;
}

fn foreignKeyRefRangeContainsParentKey(range: metadata_table_manager.ForeignKeyReferenceRangeRecord, parent_key: []const u8) bool {
    if (range.start_parent_key.len > 0 and std.mem.order(u8, parent_key, range.start_parent_key) == .lt) return false;
    if (range.end_parent_key) |end_key| {
        if (end_key.len > 0 and std.mem.order(u8, parent_key, end_key) != .lt) return false;
    }
    return true;
}

fn uniqueConstraintRangeContainsEncodedValue(range: metadata_table_manager.UniqueConstraintRangeRecord, encoded_value: []const u8) bool {
    if (range.start_encoded_value.len > 0 and std.mem.order(u8, encoded_value, range.start_encoded_value) == .lt) return false;
    if (range.end_encoded_value) |end_value| {
        if (end_value.len > 0 and std.mem.order(u8, encoded_value, end_value) != .lt) return false;
    }
    return true;
}

fn foreignKeyRefTopologyEpochMatchesGroup(
    snapshot: *const metadata_api.AdminSnapshot,
    child_table: metadata_table_manager.TableRecord,
    group_id: u64,
    expected_epoch: u64,
) bool {
    for (snapshot.foreign_key_ref_ranges) |candidate| {
        if (candidate.child_table_id != child_table.table_id) continue;
        if (candidate.group_id != group_id) continue;
        const parent_table = findTableById(snapshot.tables, candidate.parent_table_id) orelse continue;
        var hasher = std.hash.Wyhash.init(0);
        hasher.update(child_table.name);
        hasher.update(parent_table.name);
        hasher.update(candidate.constraint_name);
        for (snapshot.foreign_key_ref_ranges) |range| {
            if (range.child_table_id != child_table.table_id) continue;
            if (range.parent_table_id != candidate.parent_table_id) continue;
            if (!std.mem.eql(u8, range.constraint_name, candidate.constraint_name)) continue;
            hashForeignKeyRefRange(&hasher, range);
        }
        if (hasher.final() == expected_epoch) return true;
    }
    return false;
}

fn uniqueConstraintTopologyEpochMatchesGroup(
    snapshot: *const metadata_api.AdminSnapshot,
    table: metadata_table_manager.TableRecord,
    group_id: u64,
    expected_epoch: u64,
) bool {
    for (snapshot.unique_constraint_ranges) |candidate| {
        if (candidate.table_id != table.table_id) continue;
        if (candidate.group_id != group_id) continue;
        var hasher = std.hash.Wyhash.init(0);
        hasher.update(table.name);
        hasher.update(std.mem.asBytes(&table.table_id));
        hasher.update(candidate.constraint_name);
        for (snapshot.unique_constraint_ranges) |range| {
            if (range.table_id != table.table_id) continue;
            if (!std.mem.eql(u8, range.constraint_name, candidate.constraint_name)) continue;
            hashUniqueConstraintRange(&hasher, range);
        }
        if (hasher.final() == expected_epoch) return true;
    }
    return false;
}

fn findTableById(tables: []const metadata_table_manager.TableRecord, table_id: u64) ?metadata_table_manager.TableRecord {
    for (tables) |table| {
        if (table.table_id == table_id) return table;
    }
    return null;
}

fn hashForeignKeyRefRange(hasher: *std.hash.Wyhash, range: metadata_table_manager.ForeignKeyReferenceRangeRecord) void {
    hasher.update(std.mem.asBytes(&range.child_table_id));
    hasher.update(range.constraint_name);
    hasher.update(std.mem.asBytes(&range.parent_table_id));
    hasher.update(std.mem.asBytes(&range.group_id));
    hasher.update(std.mem.asBytes(&range.topology_epoch));
    hasher.update(range.start_parent_key);
    if (range.end_parent_key) |end_key| {
        hasher.update(&[_]u8{1});
        hasher.update(end_key);
    } else {
        hasher.update(&[_]u8{0});
    }
    hasher.update(range.state);
}

fn hashUniqueConstraintRange(hasher: *std.hash.Wyhash, range: metadata_table_manager.UniqueConstraintRangeRecord) void {
    hasher.update(std.mem.asBytes(&range.table_id));
    hasher.update(range.constraint_name);
    hasher.update(std.mem.asBytes(&range.group_id));
    hasher.update(std.mem.asBytes(&range.topology_epoch));
    hasher.update(range.start_encoded_value);
    if (range.end_encoded_value) |end_value| {
        hasher.update(&[_]u8{1});
        hasher.update(end_value);
    } else {
        hasher.update(&[_]u8{0});
    }
    hasher.update(range.state);
}

fn containsGroup(groups: []const u64, group_id: u64) bool {
    for (groups) |group| {
        if (group == group_id) return true;
    }
    return false;
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

test "catalog source promotes unique constraint with table schema compare and swap" {
    const unvalidated_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"unique_constraints":[{"name":"uniq_email","columns":["email"],"validation_state":"unvalidated"}]}
    ;
    const enforced_schema =
        \\{"version":1,"enforce_types":true,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"unique_constraints":[{"name":"uniq_email","columns":["email"],"validation_state":"enforced"}]}
    ;
    const FakeCatalog = struct {
        calls: usize = 0,
        tables: [1]metadata_table_manager.TableRecord = .{.{
            .table_id = 7,
            .name = "users",
            .schema_json = unvalidated_schema,
            .placement_role = "data",
        }},

        fn iface(self: *@This()) CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .compare_and_swap_table_schema = compareAndSwapTableSchema,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn compareAndSwapTableSchema(
            ptr: *anyopaque,
            request: metadata_table_manager.TableSchemaCompareAndSwapRequest,
        ) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqual(@as(u64, 7), request.table_id);
            try std.testing.expectEqual(@as(u64, 7), request.promoted_table.table_id);
            try std.testing.expectEqualStrings("users", request.promoted_table.name);
            try std.testing.expectEqualStrings(unvalidated_schema, request.expected_schema_json);
            try std.testing.expectEqualStrings(enforced_schema, request.promoted_table.schema_json);
        }
    };

    var fake = FakeCatalog{};
    try std.testing.expect(try promoteUniqueConstraintEnforced(std.testing.allocator, fake.iface(), "users", "uniq_email"));
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
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
    const epoch = try topologyEpoch(std.testing.allocator, FakeCatalog.iface(), "docs");
    try validateGroupTopologyEpoch(std.testing.allocator, FakeCatalog.iface(), "docs", 7001, epoch);
    try std.testing.expectError(error.TopologyChanged, validateGroupTopologyEpoch(std.testing.allocator, FakeCatalog.iface(), "docs", 8001, epoch));
    try std.testing.expectError(error.TopologyChanged, validateGroupTopologyEpoch(std.testing.allocator, FakeCatalog.iface(), "docs", 7001, epoch +% 1));

    const groups = try resolveGroupsForSpan(std.testing.allocator, FakeCatalog.iface(), "docs", "doc:b", "doc:z");
    defer std.testing.allocator.free(groups);
    try std.testing.expectEqual(@as(usize, 2), groups.len);
    try std.testing.expectEqual(@as(u64, 7001), groups[0]);
    try std.testing.expectEqual(@as(u64, 7002), groups[1]);

    const spans = try resolveTableDocKeyRanges(std.testing.allocator, FakeCatalog.iface(), "docs", "doc:b", "doc:z");
    defer {
        for (spans) |*span| span.deinit(std.testing.allocator);
        std.testing.allocator.free(spans);
    }
    try std.testing.expectEqual(@as(usize, 2), spans.len);
    try std.testing.expectEqual(@as(u64, 7001), spans[0].group_id);
    try std.testing.expectEqualStrings("doc:b", spans[0].start_key);
    try std.testing.expectEqualStrings("doc:m", spans[0].end_key);
    try std.testing.expectEqual(epoch, spans[0].topology_epoch);
    try std.testing.expect(!spans[0].inclusive_start);
    try std.testing.expectEqual(@as(u64, 7002), spans[1].group_id);
    try std.testing.expectEqualStrings("doc:m", spans[1].start_key);
    try std.testing.expectEqualStrings("doc:z", spans[1].end_key);
    try std.testing.expectEqual(epoch, spans[1].topology_epoch);
    try std.testing.expect(spans[1].inclusive_start);

    const tail = try resolveTableDocKeyRanges(std.testing.allocator, FakeCatalog.iface(), "docs", "doc:x", "");
    defer {
        for (tail) |*span| span.deinit(std.testing.allocator);
        std.testing.allocator.free(tail);
    }
    try std.testing.expectEqual(@as(usize, 1), tail.len);
    try std.testing.expectEqual(@as(u64, 7002), tail[0].group_id);
    try std.testing.expectEqualStrings("doc:x", tail[0].start_key);
    try std.testing.expectEqualStrings("", tail[0].end_key);
    try std.testing.expect(!tail[0].inclusive_start);

    try std.testing.expectError(error.InvalidRangeBounds, resolveTableDocKeyRanges(std.testing.allocator, FakeCatalog.iface(), "docs", "doc:z", "doc:b"));
}

test "catalog source resolves foreign key ref owner groups" {
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "orders", .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                    .{ .table_id = 9, .name = "accounts", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{
                    .{
                        .child_table_id = 7,
                        .constraint_name = "orders_customer_id_fkey",
                        .parent_table_id = 8,
                        .start_parent_key = "",
                        .end_parent_key = "customer:m",
                        .group_id = 9101,
                        .topology_epoch = 11,
                    },
                    .{
                        .child_table_id = 7,
                        .constraint_name = "orders_customer_id_fkey",
                        .parent_table_id = 8,
                        .start_parent_key = "customer:m",
                        .end_parent_key = null,
                        .group_id = 9102,
                        .topology_epoch = 12,
                    },
                    .{
                        .child_table_id = 7,
                        .constraint_name = "orders_account_id_fkey",
                        .parent_table_id = 9,
                        .start_parent_key = "",
                        .end_parent_key = null,
                        .group_id = 9201,
                        .topology_epoch = 13,
                    },
                    .{
                        .child_table_id = 7,
                        .constraint_name = "orders_region_id_fkey",
                        .parent_table_id = 9,
                        .start_parent_key = "",
                        .end_parent_key = null,
                        .group_id = 9301,
                        .topology_epoch = 14,
                        .state = metadata_table_manager.foreign_key_ref_range_rebuilding,
                    },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var low = try resolveForeignKeyRefOwnerGroups(std.testing.allocator, FakeCatalog.iface(), "orders", "orders_customer_id_fkey", "customers", "customer:a");
    defer low.deinit(std.testing.allocator);
    try std.testing.expect(low.configured);
    try std.testing.expectEqual(@as(usize, 1), low.groups.len);
    try std.testing.expectEqual(@as(u64, 9101), low.groups[0]);
    try std.testing.expect(low.topology_epoch != 0);
    try validateGroupTopologyEpoch(std.testing.allocator, FakeCatalog.iface(), "orders", 9101, low.topology_epoch);
    try validateGroupTopologyEpoch(std.testing.allocator, FakeCatalog.iface(), "orders", 9102, low.topology_epoch);
    try std.testing.expectError(error.TopologyChanged, validateGroupTopologyEpoch(std.testing.allocator, FakeCatalog.iface(), "orders", 9201, low.topology_epoch));
    try std.testing.expectError(error.TopologyChanged, validateGroupTopologyEpoch(std.testing.allocator, FakeCatalog.iface(), "orders", 9101, low.topology_epoch +% 1));

    var high = try resolveForeignKeyRefOwnerGroups(std.testing.allocator, FakeCatalog.iface(), "orders", "orders_customer_id_fkey", "customers", "customer:z");
    defer high.deinit(std.testing.allocator);
    try std.testing.expect(high.configured);
    try std.testing.expectEqual(@as(usize, 1), high.groups.len);
    try std.testing.expectEqual(@as(u64, 9102), high.groups[0]);
    try std.testing.expectEqual(low.topology_epoch, high.topology_epoch);

    var other_constraint = try resolveForeignKeyRefOwnerGroups(std.testing.allocator, FakeCatalog.iface(), "orders", "orders_account_id_fkey", "accounts", "acct:1");
    defer other_constraint.deinit(std.testing.allocator);
    try std.testing.expect(other_constraint.configured);
    try std.testing.expectEqual(@as(usize, 1), other_constraint.groups.len);
    try std.testing.expectEqual(@as(u64, 9201), other_constraint.groups[0]);
    try std.testing.expect(other_constraint.topology_epoch != low.topology_epoch);

    var rebuilding = try resolveForeignKeyRefOwnerGroups(std.testing.allocator, FakeCatalog.iface(), "orders", "orders_region_id_fkey", "accounts", "region:1");
    defer rebuilding.deinit(std.testing.allocator);
    try std.testing.expect(rebuilding.configured);
    try std.testing.expectEqual(@as(usize, 0), rebuilding.groups.len);
    try std.testing.expect(rebuilding.topology_epoch != 0);

    var absent = try resolveForeignKeyRefOwnerGroups(std.testing.allocator, FakeCatalog.iface(), "orders", "missing_fkey", "customers", "customer:a");
    defer absent.deinit(std.testing.allocator);
    try std.testing.expect(!absent.configured);
    try std.testing.expectEqual(@as(usize, 0), absent.groups.len);

    var empty_parent_key = try resolveForeignKeyRefOwnerGroups(std.testing.allocator, FakeCatalog.iface(), "orders", "orders_customer_id_fkey", "customers", "");
    defer empty_parent_key.deinit(std.testing.allocator);
    try std.testing.expect(empty_parent_key.configured);
    try std.testing.expectEqual(@as(usize, 1), empty_parent_key.groups.len);
    try std.testing.expectEqual(@as(u64, 9101), empty_parent_key.groups[0]);
}

test "catalog source resolves unique constraint owner groups" {
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "users", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .unique_constraint_ranges = @constCast((&[_]metadata_table_manager.UniqueConstraintRangeRecord{
                    .{
                        .table_id = 7,
                        .constraint_name = "users_email_key",
                        .start_encoded_value = "",
                        .end_encoded_value = "email:m",
                        .group_id = 7101,
                        .topology_epoch = 21,
                    },
                    .{
                        .table_id = 7,
                        .constraint_name = "users_email_key",
                        .start_encoded_value = "email:m",
                        .end_encoded_value = null,
                        .group_id = 7102,
                        .topology_epoch = 22,
                    },
                    .{
                        .table_id = 7,
                        .constraint_name = "users_username_key",
                        .start_encoded_value = "",
                        .end_encoded_value = null,
                        .group_id = 7201,
                        .topology_epoch = 23,
                    },
                    .{
                        .table_id = 7,
                        .constraint_name = "users_phone_key",
                        .start_encoded_value = "",
                        .end_encoded_value = null,
                        .group_id = 7301,
                        .topology_epoch = 24,
                        .state = metadata_table_manager.unique_constraint_range_rebuilding,
                    },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var low = try resolveUniqueConstraintOwnerGroups(std.testing.allocator, FakeCatalog.iface(), "users", "users_email_key", "email:a");
    defer low.deinit(std.testing.allocator);
    try std.testing.expect(low.configured);
    try std.testing.expectEqual(@as(usize, 1), low.groups.len);
    try std.testing.expectEqual(@as(u64, 7101), low.groups[0]);
    try std.testing.expect(low.topology_epoch != 0);
    try validateGroupTopologyEpoch(std.testing.allocator, FakeCatalog.iface(), "users", 7101, low.topology_epoch);
    try validateGroupTopologyEpoch(std.testing.allocator, FakeCatalog.iface(), "users", 7102, low.topology_epoch);
    try std.testing.expectError(error.TopologyChanged, validateGroupTopologyEpoch(std.testing.allocator, FakeCatalog.iface(), "users", 7201, low.topology_epoch));
    try std.testing.expectError(error.TopologyChanged, validateGroupTopologyEpoch(std.testing.allocator, FakeCatalog.iface(), "users", 7101, low.topology_epoch +% 1));

    var high = try resolveUniqueConstraintOwnerGroups(std.testing.allocator, FakeCatalog.iface(), "users", "users_email_key", "email:z");
    defer high.deinit(std.testing.allocator);
    try std.testing.expect(high.configured);
    try std.testing.expectEqual(@as(usize, 1), high.groups.len);
    try std.testing.expectEqual(@as(u64, 7102), high.groups[0]);
    try std.testing.expectEqual(low.topology_epoch, high.topology_epoch);

    var other_constraint = try resolveUniqueConstraintOwnerGroups(std.testing.allocator, FakeCatalog.iface(), "users", "users_username_key", "username:a");
    defer other_constraint.deinit(std.testing.allocator);
    try std.testing.expect(other_constraint.configured);
    try std.testing.expectEqual(@as(usize, 1), other_constraint.groups.len);
    try std.testing.expectEqual(@as(u64, 7201), other_constraint.groups[0]);
    try std.testing.expect(other_constraint.topology_epoch != low.topology_epoch);

    var rebuilding = try resolveUniqueConstraintOwnerGroups(std.testing.allocator, FakeCatalog.iface(), "users", "users_phone_key", "phone:1");
    defer rebuilding.deinit(std.testing.allocator);
    try std.testing.expect(rebuilding.configured);
    try std.testing.expectEqual(@as(usize, 0), rebuilding.groups.len);
    try std.testing.expect(rebuilding.topology_epoch != 0);

    var absent = try resolveUniqueConstraintOwnerGroups(std.testing.allocator, FakeCatalog.iface(), "users", "missing_key", "email:a");
    defer absent.deinit(std.testing.allocator);
    try std.testing.expect(!absent.configured);
    try std.testing.expectEqual(@as(usize, 0), absent.groups.len);

    var empty_value = try resolveUniqueConstraintOwnerGroups(std.testing.allocator, FakeCatalog.iface(), "users", "users_email_key", "");
    defer empty_value.deinit(std.testing.allocator);
    try std.testing.expect(empty_value.configured);
    try std.testing.expectEqual(@as(usize, 1), empty_value.groups.len);
    try std.testing.expectEqual(@as(u64, 7101), empty_value.groups[0]);
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
