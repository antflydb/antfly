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

const builtin = @import("builtin");
const std = @import("std");
const metadata_admin = @import("../metadata/admin.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_server = @import("../metadata/server.zig");
const metadata_service = @import("../metadata/service.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const metadata_reconciler = @import("../metadata/reconciler.zig");
const platform_clock = @import("antfly_platform").clock;
const platform_time = @import("antfly_platform").time;
const raft_reconciler = @import("../raft/reconciler.zig");
const tables_api = @import("tables.zig");

pub const CatalogSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Snapshot slices and all transitively referenced bytes must remain
        /// valid until the matching `free_admin_snapshot` call returns.
        admin_snapshot: *const fn (ptr: *anyopaque) anyerror!metadata_api.AdminSnapshot,
        free_admin_snapshot: *const fn (ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void,
        /// First-class table/range routing capability. First-party sources must
        /// override the unsupported defaults; test doubles that never route may
        /// retain them without silently falling back to an admin snapshot.
        routing_snapshot: *const fn (ptr: *anyopaque, deadline_ns: ?u64) anyerror!metadata_api.CatalogRoutingSnapshot = unsupportedRoutingSnapshot,
        /// Compact projection captured after a linearizable read barrier. It
        /// is only required to confirm an eventual negative routing result.
        linearizable_routing_snapshot: ?*const fn (ptr: *anyopaque, deadline_ns: ?u64) anyerror!metadata_api.CatalogRoutingSnapshot = null,
        free_routing_snapshot: *const fn (ptr: *anyopaque, snapshot: *metadata_api.CatalogRoutingSnapshot) void = unsupportedFreeRoutingSnapshot,
        /// Production sources must fail closed when either linearizable
        /// publication validator is unavailable.
        requires_linearizable_publication_fence: bool = false,
        /// Compares a compact contract after a Raft linearizable-read barrier.
        validate_publication: ?*const fn (ptr: *anyopaque, contract: metadata_api.CatalogPublicationContract) anyerror!bool = null,
        validate_table_publication: ?*const fn (ptr: *anyopaque, contract: metadata_api.CatalogTablePublicationContract) anyerror!bool = null,
    };

    pub fn adminSnapshot(self: CatalogSource) !metadata_api.AdminSnapshot {
        return try self.vtable.admin_snapshot(self.ptr);
    }

    pub fn freeAdminSnapshot(self: CatalogSource, snapshot: *metadata_api.AdminSnapshot) void {
        self.vtable.free_admin_snapshot(self.ptr, snapshot);
    }

    pub fn routingSnapshot(self: CatalogSource, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
        return try self.vtable.routing_snapshot(self.ptr, deadline_ns);
    }

    pub fn freeRoutingSnapshot(self: CatalogSource, snapshot: *metadata_api.CatalogRoutingSnapshot) void {
        self.vtable.free_routing_snapshot(self.ptr, snapshot);
    }

    pub fn linearizableRoutingSnapshot(self: CatalogSource, deadline_ns: ?u64) !?metadata_api.CatalogRoutingSnapshot {
        const capture = self.vtable.linearizable_routing_snapshot orelse return null;
        return try capture(self.ptr, deadline_ns);
    }

    /// Reports whether this source explicitly implements compact catalog
    /// routing. The unsupported defaults deliberately never fall back to an
    /// administrative snapshot.
    pub fn hasRoutingCapability(self: CatalogSource) bool {
        return self.vtable.routing_snapshot != unsupportedRoutingSnapshot and
            self.vtable.free_routing_snapshot != unsupportedFreeRoutingSnapshot and
            self.vtable.linearizable_routing_snapshot != null;
    }

    pub fn validatePublication(self: CatalogSource, contract: metadata_api.CatalogPublicationContract) !bool {
        const validate = self.vtable.validate_publication orelse return error.CatalogPublicationFenceUnavailable;
        return try validate(self.ptr, contract);
    }

    pub fn validateTablePublication(self: CatalogSource, contract: metadata_api.CatalogTablePublicationContract) !bool {
        const validate = self.vtable.validate_table_publication orelse return error.CatalogPublicationFenceUnavailable;
        return try validate(self.ptr, contract);
    }

    pub fn fromMetadataService(svc: *metadata_service.MetadataService) CatalogSource {
        return .{
            .ptr = svc,
            .vtable = &.{
                .admin_snapshot = metadataServiceAdminSnapshot,
                .free_admin_snapshot = metadataServiceFreeAdminSnapshot,
                .routing_snapshot = metadataServiceRoutingSnapshot,
                .linearizable_routing_snapshot = metadataServiceLinearizableRoutingSnapshot,
                .free_routing_snapshot = metadataServiceFreeRoutingSnapshot,
                .requires_linearizable_publication_fence = true,
                .validate_publication = metadataServiceValidatePublication,
                .validate_table_publication = metadataServiceValidateTablePublication,
            },
        };
    }

    pub fn fromMetadataHttpService(svc: *metadata_service.MetadataHttpService) CatalogSource {
        return .{
            .ptr = svc,
            .vtable = &.{
                .admin_snapshot = metadataHttpServiceAdminSnapshot,
                .free_admin_snapshot = metadataHttpServiceFreeAdminSnapshot,
                .routing_snapshot = metadataHttpServiceRoutingSnapshot,
                .linearizable_routing_snapshot = metadataHttpServiceLinearizableRoutingSnapshot,
                .free_routing_snapshot = metadataHttpServiceFreeRoutingSnapshot,
                .requires_linearizable_publication_fence = true,
                .validate_publication = metadataHttpServiceValidatePublication,
                .validate_table_publication = metadataHttpServiceValidateTablePublication,
            },
        };
    }

    pub fn fromMetadataServer(srv: *metadata_server.MetadataServer) CatalogSource {
        return .{
            .ptr = srv,
            .vtable = &.{
                .admin_snapshot = metadataServerAdminSnapshot,
                .free_admin_snapshot = metadataServerFreeAdminSnapshot,
                .routing_snapshot = metadataServerRoutingSnapshot,
                .linearizable_routing_snapshot = metadataServerLinearizableRoutingSnapshot,
                .free_routing_snapshot = metadataServerFreeRoutingSnapshot,
                .requires_linearizable_publication_fence = true,
                .validate_publication = metadataServerValidatePublication,
                .validate_table_publication = metadataServerValidateTablePublication,
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

fn unsupportedRoutingSnapshot(_: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
    return error.CatalogRoutingUnavailable;
}

fn unsupportedFreeRoutingSnapshot(_: *anyopaque, _: *metadata_api.CatalogRoutingSnapshot) void {
    unreachable;
}

/// Explicit adapter for legacy test doubles whose fixture data is authored as
/// an admin snapshot. Production constructors must provide compact routing
/// callbacks directly and never use this adapter.
pub fn TestAdminRoutingAdapter(
    comptime admin_snapshot: *const fn (*anyopaque) anyerror!metadata_api.AdminSnapshot,
    comptime free_admin_snapshot: *const fn (*anyopaque, *metadata_api.AdminSnapshot) void,
) type {
    if (!builtin.is_test) @compileError("TestAdminRoutingAdapter is test-only");
    return struct {
        pub fn routingSnapshot(ptr: *anyopaque, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
            if (deadline_ns) |deadline| {
                if (platform_time.monotonicNs() >= deadline) return error.CatalogRoutingSnapshotTimeout;
            }
            var admin = try admin_snapshot(ptr);
            defer free_admin_snapshot(ptr, &admin);

            const tables = try std.testing.allocator.alloc(metadata_table_manager.TableRecord, admin.tables.len);
            var table_count: usize = 0;
            errdefer {
                for (tables[0..table_count]) |table| metadata_table_manager.freeTable(std.testing.allocator, table);
                std.testing.allocator.free(tables);
            }
            for (admin.tables, 0..) |table, index| {
                tables[index] = try metadata_table_manager.cloneTable(std.testing.allocator, table);
                table_count = index + 1;
            }

            const ranges = try std.testing.allocator.alloc(metadata_table_manager.RangeRecord, admin.ranges.len);
            var range_count: usize = 0;
            errdefer {
                for (ranges[0..range_count]) |range| metadata_table_manager.freeRange(std.testing.allocator, range);
                std.testing.allocator.free(ranges);
            }
            for (admin.ranges, 0..) |range, index| {
                ranges[index] = try metadata_table_manager.cloneRange(std.testing.allocator, range);
                range_count = index + 1;
            }
            return .{ .tables = tables, .ranges = ranges };
        }

        pub fn freeRoutingSnapshot(_: *anyopaque, snapshot: *metadata_api.CatalogRoutingSnapshot) void {
            for (snapshot.tables) |table| metadata_table_manager.freeTable(std.testing.allocator, table);
            std.testing.allocator.free(snapshot.tables);
            for (snapshot.ranges) |range| metadata_table_manager.freeRange(std.testing.allocator, range);
            std.testing.allocator.free(snapshot.ranges);
            snapshot.* = undefined;
        }
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
    return try resolveSingleRangeGroupUntil(alloc, catalog, table_name, null);
}

pub fn resolveSingleRangeGroupUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    deadline_ns: ?u64,
) !?u64 {
    var result = try resolveRoute(alloc, catalog, table_name, .all_ranges, deadline_ns);
    defer result.deinit(alloc);
    return switch (result) {
        .found => |plan| if (plan.groups.len == 1)
            plan.groups[0].group_id
        else
            error.UnsupportedMultiRangeTable,
        .not_found => null,
        .timed_out => error.CatalogRoutingSnapshotTimeout,
    };
}

pub fn resolveGroupForKey(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    key: []const u8,
) !?u64 {
    return try resolveGroupForKeyUntil(alloc, catalog, table_name, key, null);
}

pub fn resolveGroupForKeyUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    key: []const u8,
    deadline_ns: ?u64,
) !?u64 {
    var result = try resolveRoute(alloc, catalog, table_name, .{ .key = key }, deadline_ns);
    defer result.deinit(alloc);
    return switch (result) {
        .found => |plan| plan.groups[0].group_id,
        .not_found => null,
        .timed_out => error.CatalogRoutingSnapshotTimeout,
    };
}

pub const RoutedGroupSnapshot = struct {
    group_id: ?u64,
    topology_epoch: u64,
};

/// Resolve a key and compute the routing epoch from one catalog snapshot.
/// Callers can perform an external consistency barrier, acquire structural
/// read admission, and then validate the captured epoch without a torn
/// epoch/route pair.
pub fn routedGroupSnapshot(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    key: []const u8,
) !RoutedGroupSnapshot {
    return try routedGroupSnapshotUntil(alloc, catalog, table_name, key, null);
}

pub fn routedGroupSnapshotUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    key: []const u8,
    deadline_ns: ?u64,
) !RoutedGroupSnapshot {
    var result = try resolveRoute(alloc, catalog, table_name, .{ .key = key }, deadline_ns);
    defer result.deinit(alloc);
    return switch (result) {
        .found => |plan| .{
            .group_id = plan.groups[0].group_id,
            .topology_epoch = plan.topology_epoch,
        },
        .not_found => .{ .group_id = null, .topology_epoch = 0 },
        .timed_out => error.CatalogRoutingSnapshotTimeout,
    };
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

/// Owns one catalog snapshot and its sorted table-range projection for the
/// lifetime of transaction routing. This keeps every key in a table pinned to
/// the same topology without taking a catalog snapshot for each operation.
pub const TransactionRoutingSnapshot = struct {
    catalog: CatalogSource,
    snapshot: metadata_api.AdminSnapshot,
    ranges: []const *const metadata_table_manager.RangeRecord,
    topology_epoch: u64,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        metadata_admin.freeRangeRefs(alloc, self.ranges);
        self.catalog.freeAdminSnapshot(&self.snapshot);
        self.* = undefined;
    }

    pub fn resolveGroupForKey(self: *const @This(), key: []const u8) ?u64 {
        return resolveGroupForKeyFromRanges(self.ranges, key);
    }
};

/// Captures and validates the table topology once for a transaction routing
/// pass. The returned range pointers remain valid until `deinit`.
pub fn transactionRoutingSnapshot(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
) !?TransactionRoutingSnapshot {
    var snapshot = try catalog.adminSnapshot();
    errdefer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return null;
    try validateTransactionTopologyStableSnapshot(&snapshot, table.*);

    const ranges = try metadata_admin.listTableRanges(alloc, &snapshot, table.table_id);
    errdefer metadata_admin.freeRangeRefs(alloc, ranges);
    if (ranges.len == 0) return null;
    sortRangeRefs(ranges);

    return .{
        .catalog = catalog,
        .snapshot = snapshot,
        .ranges = ranges,
        .topology_epoch = topologyEpochFromSortedRanges(table.*, ranges),
    };
}

/// Whether a table with this name currently exists in the catalog. Used by
/// cross-table graph hydration to fail closed (skip) rather than error when a
/// node references a dropped table.
pub fn tableExists(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
) !bool {
    return try tableExistsUntil(alloc, catalog, table_name, null);
}

pub fn tableExistsUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    deadline_ns: ?u64,
) !bool {
    var result = try resolveRoute(alloc, catalog, table_name, .table, deadline_ns);
    defer result.deinit(alloc);
    return switch (result) {
        .found => true,
        .not_found => false,
        .timed_out => error.CatalogRoutingSnapshotTimeout,
    };
}

pub fn topologyEpoch(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
) !u64 {
    return try topologyEpochUntil(alloc, catalog, table_name, null);
}

pub fn topologyEpochUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    deadline_ns: ?u64,
) !u64 {
    var result = try resolveRoute(alloc, catalog, table_name, .all_ranges, deadline_ns);
    defer result.deinit(alloc);
    return switch (result) {
        .found => |plan| plan.topology_epoch,
        .not_found => 0,
        .timed_out => error.CatalogRoutingSnapshotTimeout,
    };
}

fn topologyEpochFromSnapshot(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table: metadata_table_manager.TableRecord,
) !u64 {
    const ranges = try metadata_admin.listTableRanges(alloc, snapshot, table.table_id);
    defer metadata_admin.freeRangeRefs(alloc, ranges);

    sortRangeRefs(ranges);
    return topologyEpochFromSortedRanges(table, ranges);
}

fn topologyEpochFromSortedRanges(
    table: metadata_table_manager.TableRecord,
    ranges: []const *const metadata_table_manager.RangeRecord,
) u64 {
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

pub fn transactionTopologyEpoch(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
) !u64 {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return 0;
    try validateTransactionTopologyStableSnapshot(&snapshot, table.*);
    return try topologyEpochFromSnapshot(alloc, &snapshot, table.*);
}

pub fn validateTransactionTopologyEpoch(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    expected_epoch: u64,
) !void {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
    try validateTransactionTopologyStableSnapshot(&snapshot, table.*);
    if (expected_epoch != 0 and (try topologyEpochFromSnapshot(alloc, &snapshot, table.*)) != expected_epoch) return error.TopologyChanged;
}

pub fn validateTopologyEpoch(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    expected_epoch: u64,
) !void {
    return try validateTopologyEpochUntil(alloc, catalog, table_name, expected_epoch, null);
}

pub fn validateTopologyEpochUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    expected_epoch: u64,
    deadline_ns: ?u64,
) !void {
    if (expected_epoch == 0) return;
    const actual_epoch = try topologyEpochUntil(alloc, catalog, table_name, deadline_ns);
    if (actual_epoch != expected_epoch) return error.TopologyChanged;
}

/// Validate an internally captured topology epoch, including the zero epoch
/// used while a table is absent. Public graph requests use zero to mean
/// "unstamped", so `validateTopologyEpoch` intentionally skips it; read
/// admission needs an exact comparison to notice a table created after an
/// absent-table snapshot was routed.
pub fn validatePinnedTopologyEpoch(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    expected_epoch: u64,
) !void {
    return try validatePinnedTopologyEpochUntil(alloc, catalog, table_name, expected_epoch, null);
}

pub fn validatePinnedTopologyEpochUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    expected_epoch: u64,
    deadline_ns: ?u64,
) !void {
    const actual_epoch = try topologyEpochUntil(alloc, catalog, table_name, deadline_ns);
    if (actual_epoch != expected_epoch) return error.TopologyChanged;
}

/// Capture the current table topology epoch only if `group_id` is one of its
/// published ranges. Internal group-local reads use this before a Raft wait so
/// an obsolete group fails fast instead of waiting on a replica that has
/// already left the table.
pub fn groupTopologyEpoch(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    group_id: u64,
) !u64 {
    return try groupTopologyEpochUntil(alloc, catalog, table_name, group_id, null);
}

pub fn groupTopologyEpochUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    group_id: u64,
    deadline_ns: ?u64,
) !u64 {
    var result = try resolveRoute(alloc, catalog, table_name, .{ .group = group_id }, deadline_ns);
    defer result.deinit(alloc);
    return switch (result) {
        .found => |plan| plan.topology_epoch,
        .not_found => error.TopologyChanged,
        .timed_out => error.CatalogRoutingSnapshotTimeout,
    };
}

pub fn validatePinnedGroupTopology(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    group_id: u64,
    expected_epoch: u64,
) !void {
    return try validatePinnedGroupTopologyUntil(alloc, catalog, table_name, group_id, expected_epoch, null);
}

pub fn validatePinnedGroupTopologyUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    group_id: u64,
    expected_epoch: u64,
    deadline_ns: ?u64,
) !void {
    if (try groupTopologyEpochUntil(alloc, catalog, table_name, group_id, deadline_ns) != expected_epoch)
        return error.TopologyChanged;
}

/// Transactions may not straddle a split or merge. The transition record is
/// published before range cutover, so checking it in addition to the range
/// epoch closes the prepare-to-cutover window where durable intents could
/// otherwise be left on the previous owner.
pub fn validateTransactionTopologyStable(
    catalog: CatalogSource,
    table_name: []const u8,
) !void {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
    return validateTransactionTopologyStableSnapshot(&snapshot, table.*);
}

fn validateTransactionTopologyStableSnapshot(
    snapshot: *const metadata_api.AdminSnapshot,
    table: metadata_table_manager.TableRecord,
) !void {
    for (snapshot.split_transitions) |transition| {
        if (!transitionPhaseActive(transition.phase)) continue;
        if (transition.table_contract.table_id == table.table_id or
            std.mem.eql(u8, transition.table_contract.table_name, table.name))
        {
            return error.TopologyChanged;
        }
        if (transition.table_contract.table_id == 0 and
            (rangeGroupBelongsToTable(snapshot.ranges, table.table_id, transition.source_group_id) or
                rangeGroupBelongsToTable(snapshot.ranges, table.table_id, transition.destination_group_id)))
        {
            return error.TopologyChanged;
        }
    }
    for (snapshot.merge_transitions) |transition| {
        if (!transitionPhaseActive(transition.phase)) continue;
        if (transition.table_contract.table_id == table.table_id or
            std.mem.eql(u8, transition.table_contract.table_name, table.name))
        {
            return error.TopologyChanged;
        }
        if (transition.table_contract.table_id == 0 and
            (rangeGroupBelongsToTable(snapshot.ranges, table.table_id, transition.donor_group_id) or
                rangeGroupBelongsToTable(snapshot.ranges, table.table_id, transition.receiver_group_id)))
        {
            return error.TopologyChanged;
        }
    }
}

fn transitionPhaseActive(phase: metadata_transition_state.TransitionPhase) bool {
    return phase != .finalized and phase != .rolled_back;
}

fn rangeGroupBelongsToTable(ranges: []const metadata_table_manager.RangeRecord, table_id: u64, group_id: u64) bool {
    for (ranges) |range| if (range.table_id == table_id and range.group_id == group_id) return true;
    return false;
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
    return try resolveGroupForKeyPinnedUntil(alloc, catalog, table_name, key, expected_epoch, null);
}

pub fn resolveGroupForKeyPinnedUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    key: []const u8,
    expected_epoch: u64,
    deadline_ns: ?u64,
) !?u64 {
    var result = try resolveRoute(alloc, catalog, table_name, .{ .key = key }, deadline_ns);
    defer result.deinit(alloc);
    return switch (result) {
        .found => |plan| blk: {
            if (expected_epoch != 0 and plan.topology_epoch != expected_epoch) return error.TopologyChanged;
            break :blk plan.groups[0].group_id;
        },
        .not_found => null,
        .timed_out => error.CatalogRoutingSnapshotTimeout,
    };
}

fn findTableByName(
    tables: []const metadata_table_manager.TableRecord,
    table_name: []const u8,
) ?*const metadata_table_manager.TableRecord {
    for (tables) |*table| {
        if (std.mem.eql(u8, table.name, table_name)) return table;
    }
    return null;
}

fn listTableRanges(
    alloc: std.mem.Allocator,
    catalog_ranges: []const metadata_table_manager.RangeRecord,
    table_id: u64,
) ![]const *const metadata_table_manager.RangeRecord {
    var count: usize = 0;
    for (catalog_ranges) |range| {
        if (range.table_id == table_id) count += 1;
    }
    const ranges = try alloc.alloc(*const metadata_table_manager.RangeRecord, count);
    var index: usize = 0;
    for (catalog_ranges) |*range| {
        if (range.table_id != table_id) continue;
        ranges[index] = range;
        index += 1;
    }
    return ranges;
}

pub fn resolveGroupsForSpan(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
) ![]u64 {
    return try resolveGroupsForSpanUntil(alloc, catalog, table_name, from_key, to_key, null);
}

pub fn resolveGroupsForSpanUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    deadline_ns: ?u64,
) ![]u64 {
    return switch (try resolveGroupsForSpanWithDeadline(alloc, catalog, table_name, from_key, to_key, deadline_ns)) {
        .found => |plan_value| blk: {
            var plan = plan_value;
            defer plan.deinit(alloc);
            break :blk try plan.groupIdsAlloc(alloc);
        },
        .not_found => try alloc.alloc(u64, 0),
        .timed_out => error.CatalogRoutingSnapshotTimeout,
    };
}

pub fn resolveGroupsForSpanPinnedUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    expected_epoch: u64,
    deadline_ns: ?u64,
) ![]u64 {
    var result = try resolveRoute(alloc, catalog, table_name, .{ .span = .{
        .from_key = from_key,
        .to_key = to_key,
    } }, deadline_ns);
    defer result.deinit(alloc);
    return switch (result) {
        .found => |plan| blk: {
            if (expected_epoch != 0 and plan.topology_epoch != expected_epoch) return error.TopologyChanged;
            break :blk try plan.groupIdsAlloc(alloc);
        },
        .not_found => try alloc.alloc(u64, 0),
        .timed_out => error.CatalogRoutingSnapshotTimeout,
    };
}

pub const CatalogIdentityNamespace = struct {
    table_id: u64,
    shard_id: u64,
    range_id: u64,
};

pub const CatalogGroupRoute = struct {
    group_id: u64,
    range_id: u64,
    identity_namespace: CatalogIdentityNamespace,
};

/// A complete routing decision derived from one immutable catalog projection.
/// Group selection and database identity must never be looked up separately.
pub const CatalogRoutePlan = struct {
    metadata_group_id: u64,
    metadata_incarnation: ?metadata_api.MetadataClusterIncarnation,
    catalog_revision: u64,
    table_id: u64,
    topology_epoch: u64,
    groups: []CatalogGroupRoute,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.groups);
        self.* = undefined;
    }

    pub fn groupIdsAlloc(self: @This(), alloc: std.mem.Allocator) ![]u64 {
        const ids = try alloc.alloc(u64, self.groups.len);
        for (self.groups, ids) |route, *id| id.* = route.group_id;
        return ids;
    }

    pub fn group(self: @This(), group_id: u64) ?CatalogGroupRoute {
        for (self.groups) |route| if (route.group_id == group_id) return route;
        return null;
    }
};

pub const RouteQuery = union(enum) {
    /// Table existence only; a table with no published ranges is still found.
    table,
    /// Select every published range and require at least one.
    all_ranges,
    key: []const u8,
    span: struct {
        from_key: []const u8,
        to_key: []const u8,
    },
    group: u64,
};

pub const RouteResult = union(enum) {
    found: CatalogRoutePlan,
    not_found,
    timed_out,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.* == .found) self.found.deinit(alloc);
        self.* = undefined;
    }
};

pub const ResolveGroupsResult = RouteResult;

/// Resolve one routing predicate from a compact projection. Eventual
/// projections are sufficient for positive routes. Any miss is evaluated
/// again after a compact linearizable barrier before it becomes authoritative.
pub fn resolveRoute(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    query: RouteQuery,
    deadline_ns: ?u64,
) !RouteResult {
    var eventual = catalog.routingSnapshot(deadline_ns) catch |err| switch (err) {
        error.CatalogRoutingSnapshotTimeout => return .timed_out,
        else => return err,
    };
    defer catalog.freeRoutingSnapshot(&eventual);
    if (try routePlanFromSnapshot(alloc, eventual, table_name, query)) |plan| {
        return .{ .found = plan };
    }

    const maybe_authoritative = catalog.linearizableRoutingSnapshot(deadline_ns) catch |err| switch (err) {
        error.CatalogRoutingSnapshotTimeout => return .timed_out,
        else => return err,
    };
    var authoritative = maybe_authoritative orelse return .not_found;
    defer catalog.freeRoutingSnapshot(&authoritative);
    if (try routePlanFromSnapshot(alloc, authoritative, table_name, query)) |plan| {
        return .{ .found = plan };
    }
    return .not_found;
}

fn resolveGroupsForSpanWithDeadline(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    deadline_ns: ?u64,
) !ResolveGroupsResult {
    return try resolveRoute(alloc, catalog, table_name, .{ .span = .{
        .from_key = from_key,
        .to_key = to_key,
    } }, deadline_ns);
}

fn routePlanFromSnapshot(
    alloc: std.mem.Allocator,
    snapshot: metadata_api.CatalogRoutingSnapshot,
    table_name: []const u8,
    query: RouteQuery,
) !?CatalogRoutePlan {
    const table = findTableByName(snapshot.tables, table_name) orelse return null;
    const ranges = try listTableRanges(alloc, snapshot.ranges, table.table_id);
    defer metadata_admin.freeRangeRefs(alloc, ranges);

    sortRangeRefs(ranges);
    var groups = std.ArrayListUnmanaged(CatalogGroupRoute).empty;
    defer groups.deinit(alloc);
    for (ranges) |range| {
        const selected = switch (query) {
            .table => false,
            .all_ranges => true,
            .key => |key| rangeContainsKey(range.*, key),
            .span => |span| rangeOverlapsSpan(range.*, span.from_key, span.to_key),
            .group => |group_id| range.group_id == group_id,
        };
        if (!selected) continue;
        const range_id = metadata_table_manager.rangeDocIdentityRangeId(range.*);
        try groups.append(alloc, .{
            .group_id = range.group_id,
            .range_id = range_id,
            .identity_namespace = .{
                .table_id = table.table_id,
                .shard_id = metadata_table_manager.rangeDocIdentityShardId(range.*),
                .range_id = range_id,
            },
        });
    }
    const requires_route = switch (query) {
        .table => false,
        else => true,
    };
    if (groups.items.len == 0 and requires_route) return null;
    return .{
        .metadata_group_id = snapshot.metadata_group_id,
        .metadata_incarnation = snapshot.metadata_incarnation,
        .catalog_revision = snapshot.catalog_revision,
        .table_id = table.table_id,
        .topology_epoch = topologyEpochFromSortedRanges(table.*, ranges),
        .groups = try groups.toOwnedSlice(alloc),
    };
}

pub const RoutedSpanSnapshot = struct {
    group_ids: []u64,
    topology_epoch: u64,
};

/// Resolve a span and compute the routing epoch from one catalog snapshot.
pub fn routedSpanSnapshot(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
) !RoutedSpanSnapshot {
    return try routedSpanSnapshotUntil(alloc, catalog, table_name, from_key, to_key, null);
}

pub fn routedSpanSnapshotUntil(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    deadline_ns: ?u64,
) !RoutedSpanSnapshot {
    var result = try resolveRoute(alloc, catalog, table_name, .{ .span = .{
        .from_key = from_key,
        .to_key = to_key,
    } }, deadline_ns);
    defer result.deinit(alloc);
    return switch (result) {
        .found => |plan| .{
            .group_ids = try plan.groupIdsAlloc(alloc),
            .topology_epoch = plan.topology_epoch,
        },
        .not_found => .{
            .group_ids = try alloc.alloc(u64, 0),
            .topology_epoch = 0,
        },
        .timed_out => error.CatalogRoutingSnapshotTimeout,
    };
}

pub fn resolveGroupsForSpanEventually(
    alloc: std.mem.Allocator,
    catalog: CatalogSource,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    timeout_ns: u64,
    poll_interval_ms: u64,
) !ResolveGroupsResult {
    const start_ns = platform_time.monotonicNs();
    const deadline_ns = start_ns +| timeout_ns;
    while (true) {
        const result = resolveGroupsForSpanWithDeadline(
            alloc,
            catalog,
            table_name,
            from_key,
            to_key,
            deadline_ns,
        ) catch |err| switch (err) {
            error.CatalogRoutingSnapshotTimeout => return .timed_out,
            else => return err,
        };
        switch (result) {
            .found => return result,
            .timed_out => return result,
            .not_found => if (catalog.hasRoutingCapability()) return result,
        }
        if (platform_time.monotonicNs() >= deadline_ns) return .not_found;
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

fn metadataServiceRoutingSnapshot(ptr: *anyopaque, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
    const svc: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
    return try svc.catalogRoutingSnapshot(deadline_ns);
}

fn metadataServiceLinearizableRoutingSnapshot(ptr: *anyopaque, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
    const svc: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
    return try metadata_service.linearizableCatalogRoutingSnapshot(metadata_service.MetadataService, svc, .{
        .deadline_ns = deadline_ns,
    });
}

fn metadataServiceFreeRoutingSnapshot(ptr: *anyopaque, snapshot: *metadata_api.CatalogRoutingSnapshot) void {
    const svc: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
    svc.freeCatalogRoutingSnapshot(snapshot);
}

fn metadataServiceValidatePublication(ptr: *anyopaque, contract: metadata_api.CatalogPublicationContract) !bool {
    const svc: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
    return try svc.validatePublication(contract);
}

fn metadataServiceValidateTablePublication(ptr: *anyopaque, contract: metadata_api.CatalogTablePublicationContract) !bool {
    const svc: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
    return try svc.validateTablePublication(contract);
}

fn metadataHttpServiceAdminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
    const svc: *metadata_service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    return try svc.adminSnapshot();
}

fn metadataHttpServiceFreeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
    const svc: *metadata_service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    svc.freeAdminSnapshot(snapshot);
}

fn metadataHttpServiceRoutingSnapshot(ptr: *anyopaque, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
    const svc: *metadata_service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    return try svc.catalogRoutingSnapshot(deadline_ns);
}

fn metadataHttpServiceLinearizableRoutingSnapshot(ptr: *anyopaque, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
    const svc: *metadata_service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    return try metadata_service.linearizableCatalogRoutingSnapshot(metadata_service.MetadataHttpService, svc, .{
        .deadline_ns = deadline_ns,
    });
}

fn metadataHttpServiceFreeRoutingSnapshot(ptr: *anyopaque, snapshot: *metadata_api.CatalogRoutingSnapshot) void {
    const svc: *metadata_service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    svc.freeCatalogRoutingSnapshot(snapshot);
}

fn metadataHttpServiceValidatePublication(ptr: *anyopaque, contract: metadata_api.CatalogPublicationContract) !bool {
    const svc: *metadata_service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    return try svc.validatePublication(contract);
}

fn metadataHttpServiceValidateTablePublication(ptr: *anyopaque, contract: metadata_api.CatalogTablePublicationContract) !bool {
    const svc: *metadata_service.MetadataHttpService = @ptrCast(@alignCast(ptr));
    return try svc.validateTablePublication(contract);
}

fn metadataServerAdminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
    const srv: *metadata_server.MetadataServer = @ptrCast(@alignCast(ptr));
    return try srv.adminSnapshot();
}

fn metadataServerFreeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
    const srv: *metadata_server.MetadataServer = @ptrCast(@alignCast(ptr));
    srv.freeAdminSnapshot(snapshot);
}

fn metadataServerRoutingSnapshot(ptr: *anyopaque, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
    const srv: *metadata_server.MetadataServer = @ptrCast(@alignCast(ptr));
    return try srv.svc.catalogRoutingSnapshot(deadline_ns);
}

fn metadataServerLinearizableRoutingSnapshot(ptr: *anyopaque, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
    const srv: *metadata_server.MetadataServer = @ptrCast(@alignCast(ptr));
    return try metadata_service.linearizableCatalogRoutingSnapshot(metadata_service.MetadataHttpService, srv.svc, .{
        .deadline_ns = deadline_ns,
    });
}

fn metadataServerFreeRoutingSnapshot(ptr: *anyopaque, snapshot: *metadata_api.CatalogRoutingSnapshot) void {
    const srv: *metadata_server.MetadataServer = @ptrCast(@alignCast(ptr));
    srv.svc.freeCatalogRoutingSnapshot(snapshot);
}

fn metadataServerValidatePublication(ptr: *anyopaque, contract: metadata_api.CatalogPublicationContract) !bool {
    const srv: *metadata_server.MetadataServer = @ptrCast(@alignCast(ptr));
    return try srv.validatePublication(contract);
}

fn metadataServerValidateTablePublication(ptr: *anyopaque, contract: metadata_api.CatalogTablePublicationContract) !bool {
    const srv: *metadata_server.MetadataServer = @ptrCast(@alignCast(ptr));
    return try srv.validateTablePublication(contract);
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

test "metadata service constructors provide compact routing snapshots" {
    var svc: metadata_service.MetadataService = undefined;
    try std.testing.expect(CatalogSource.fromMetadataService(&svc).hasRoutingCapability());

    var http_svc: metadata_service.MetadataHttpService = undefined;
    try std.testing.expect(CatalogSource.fromMetadataHttpService(&http_svc).hasRoutingCapability());

    var server: metadata_server.MetadataServer = undefined;
    try std.testing.expect(CatalogSource.fromMetadataServer(&server).hasRoutingCapability());
}

test "catalog sources without compact routing fail closed" {
    const source = emptyCatalogSource();
    try std.testing.expect(!source.hasRoutingCapability());
    try std.testing.expectError(error.CatalogRoutingUnavailable, source.routingSnapshot(null));
}

test "transaction topology fence rejects active split transitions" {
    const Source = struct {
        fn snapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{.{
                    .transition_id = 9,
                    .attempt_epoch = 1,
                    .source_group_id = 7001,
                    .destination_group_id = 7002,
                    .table_contract = .{ .table_id = 7, .table_name = "docs" },
                }})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }
        fn free(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };
    const source: CatalogSource = .{ .ptr = undefined, .vtable = &.{
        .admin_snapshot = Source.snapshot,
        .free_admin_snapshot = Source.free,
    } };
    try std.testing.expectError(error.TopologyChanged, validateTransactionTopologyStable(source, "docs"));
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
                    .routing_snapshot = TestAdminRoutingAdapter(adminSnapshot, freeAdminSnapshot).routingSnapshot,
                    .free_routing_snapshot = TestAdminRoutingAdapter(adminSnapshot, freeAdminSnapshot).freeRoutingSnapshot,
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
                    .routing_snapshot = TestAdminRoutingAdapter(adminSnapshot, freeAdminSnapshot).routingSnapshot,
                    .free_routing_snapshot = TestAdminRoutingAdapter(adminSnapshot, freeAdminSnapshot).freeRoutingSnapshot,
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

test "span routing uses compact catalog snapshot when available" {
    const TestState = struct {
        freed: bool = false,
        routing_calls: usize = 0,
    };
    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 7, .name = "docs", .placement_role = "data" },
        };
        const ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
        };

        fn iface(state: *TestState) CatalogSource {
            return .{
                .ptr = state,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .routing_snapshot = routingSnapshot,
                    .free_routing_snapshot = freeRoutingSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.AdminSnapshotUsedForRouting;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn routingSnapshot(ptr: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.routing_calls += 1;
            return .{
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(ranges[0..]),
            };
        }

        fn freeRoutingSnapshot(ptr: *anyopaque, _: *metadata_api.CatalogRoutingSnapshot) void {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.freed = true;
        }
    };

    var state = TestState{};
    var found = try resolveGroupsForSpanWithDeadline(
        std.testing.allocator,
        FakeCatalog.iface(&state),
        "docs",
        "",
        "",
        null,
    );
    defer found.deinit(std.testing.allocator);
    switch (found) {
        .found => |plan| {
            try std.testing.expectEqual(@as(u64, 7), plan.table_id);
            try std.testing.expectEqual(@as(usize, 1), plan.groups.len);
            try std.testing.expectEqual(@as(u64, 7001), plan.groups[0].group_id);
            try std.testing.expectEqual(CatalogIdentityNamespace{
                .table_id = 7,
                .shard_id = 7001,
                .range_id = 7001,
            }, plan.groups[0].identity_namespace);
        },
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expect(state.freed);
    try std.testing.expectEqual(@as(usize, 1), state.routing_calls);

    const not_found = try resolveGroupsForSpanWithDeadline(
        std.testing.allocator,
        FakeCatalog.iface(&state),
        "missing",
        "",
        "",
        null,
    );
    try std.testing.expectEqual(ResolveGroupsResult.not_found, not_found);
}

test "span routing confirms eventual misses with a linearizable compact snapshot" {
    const TestState = struct {
        eventual_calls: usize = 0,
        linearizable_calls: usize = 0,
        frees: usize = 0,
    };
    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 11, .name = "new-table", .placement_role = "data" },
        };
        const ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 11001, .table_id = 11, .range_id = 17, .start_key = "", .end_key = null },
        };

        fn iface(state: *TestState) CatalogSource {
            return .{ .ptr = state, .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
                .routing_snapshot = routingSnapshot,
                .linearizable_routing_snapshot = linearizableRoutingSnapshot,
                .free_routing_snapshot = freeRoutingSnapshot,
            } };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.AdminSnapshotUsedForRouting;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn routingSnapshot(ptr: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.eventual_calls += 1;
            return .{ .catalog_revision = 4, .tables = &.{}, .ranges = &.{} };
        }

        fn linearizableRoutingSnapshot(ptr: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.linearizable_calls += 1;
            return .{
                .catalog_revision = 5,
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(ranges[0..]),
            };
        }

        fn freeRoutingSnapshot(ptr: *anyopaque, _: *metadata_api.CatalogRoutingSnapshot) void {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.frees += 1;
        }
    };

    var state = TestState{};
    var result = try resolveGroupsForSpanWithDeadline(
        std.testing.allocator,
        FakeCatalog.iface(&state),
        "new-table",
        "",
        "",
        null,
    );
    defer result.deinit(std.testing.allocator);
    switch (result) {
        .found => |plan| {
            try std.testing.expectEqual(@as(u64, 5), plan.catalog_revision);
            try std.testing.expectEqual(@as(u64, 11001), plan.groups[0].group_id);
            try std.testing.expectEqual(@as(u64, 17), plan.groups[0].identity_namespace.range_id);
        },
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expectEqual(@as(usize, 1), state.eventual_calls);
    try std.testing.expectEqual(@as(usize, 1), state.linearizable_calls);
    try std.testing.expectEqual(@as(usize, 2), state.frees);
}

test "route resolver confirms a table-present range miss linearly" {
    const TestState = struct {
        eventual_calls: usize = 0,
        linearizable_calls: usize = 0,
        frees: usize = 0,
    };
    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 12, .name = "docs", .placement_role = "data" },
        };
        const eventual_ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 12001, .table_id = 12, .start_key = "", .end_key = "doc:m" },
        };
        const authoritative_ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 12001, .table_id = 12, .start_key = "", .end_key = "doc:m" },
            .{ .group_id = 12002, .table_id = 12, .range_id = 22, .start_key = "doc:m", .end_key = null },
        };

        fn iface(state: *TestState) CatalogSource {
            return .{ .ptr = state, .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
                .routing_snapshot = routingSnapshot,
                .linearizable_routing_snapshot = linearizableRoutingSnapshot,
                .free_routing_snapshot = freeRoutingSnapshot,
            } };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.AdminSnapshotUsedForRouting;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn routingSnapshot(ptr: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.eventual_calls += 1;
            return .{
                .catalog_revision = 8,
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(eventual_ranges[0..]),
            };
        }

        fn linearizableRoutingSnapshot(ptr: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.linearizable_calls += 1;
            return .{
                .catalog_revision = 9,
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(authoritative_ranges[0..]),
            };
        }

        fn freeRoutingSnapshot(ptr: *anyopaque, _: *metadata_api.CatalogRoutingSnapshot) void {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.frees += 1;
        }
    };

    var state = TestState{};
    try std.testing.expectEqual(
        @as(?u64, 12002),
        try resolveGroupForKeyUntil(
            std.testing.allocator,
            FakeCatalog.iface(&state),
            "docs",
            "doc:z",
            platform_time.monotonicNs() + std.time.ns_per_s,
        ),
    );
    try std.testing.expectEqual(@as(usize, 1), state.eventual_calls);
    try std.testing.expectEqual(@as(usize, 1), state.linearizable_calls);
    try std.testing.expectEqual(@as(usize, 2), state.frees);
}

test "eventual span routing distinguishes snapshot timeout" {
    const FakeCatalog = struct {
        fn iface() CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .routing_snapshot = routingSnapshot,
                    .free_routing_snapshot = freeRoutingSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.AdminSnapshotUsedForRouting;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn routingSnapshot(_: *anyopaque, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
            try std.testing.expect(deadline_ns != null);
            return error.CatalogRoutingSnapshotTimeout;
        }

        fn freeRoutingSnapshot(_: *anyopaque, _: *metadata_api.CatalogRoutingSnapshot) void {}
    };

    const result = try resolveGroupsForSpanEventually(
        std.testing.allocator,
        FakeCatalog.iface(),
        "docs",
        "",
        "",
        std.time.ns_per_s,
        1,
    );
    try std.testing.expectEqual(ResolveGroupsResult.timed_out, result);
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
