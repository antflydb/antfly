// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Structural dispatch for private SQL-lock observations only. This does not
//! authorize a control: the native source rejoins canonical metadata authority
//! and the actual native receiver validates its sealed participant record.
const std = @import("std");
const local = @import("antfly_local_sources");
const Fence = local.metadata_catalog_route_contract.CatalogRouteFence;
const Options = local.storage_db_types.LookupOptions;

pub fn validateOptions(opts: Options) !void {
    try local.api_table_read_source.PrivateLookup.requireSqlLockOptions(opts);
}

pub fn requireSameRoute(outer: Fence, control: Fence) !void {
    try outer.validate();
    try control.validate();
    if (outer.route.execution != .native_local or control.route.execution != .native_local or
        outer.metadata_group_id != control.metadata_group_id or !std.meta.eql(outer.metadata_incarnation, control.metadata_incarnation) or
        outer.catalog_revision != control.catalog_revision or outer.topology_epoch != control.topology_epoch or
        outer.table_id != control.table_id or !std.meta.eql(outer.route, control.route) or
        !std.meta.eql(outer.native_participant, control.native_participant)) return error.LockOwnerChanged;
}

/// Recognize a closed private control shape; never treat an arbitrary lock
/// query, a selector mix, or a claimed descriptor as execution authority.
pub fn isPrivateControl(a: std.mem.Allocator, key: []const u8, opts: Options, fence: Fence) !bool {
    if (opts.sql_lock_query_json.len == 0 or fence.route.execution != .native_local) return false;
    if (key.len != 0 or opts.sql_lock_query_json.len > 32768) return error.InvalidLockScope;
    try validateOptions(opts);
    var query = try std.json.parseFromSlice(local.storage_pessimistic_lock_control.Query, a, opts.sql_lock_query_json, .{});
    defer query.deinit();
    try query.value.validate();
    const control = if (query.value.native_control) |value| value.route else if (query.value.retirement_control) |value| value.route else return false;
    try requireSameRoute(fence, control);
    return true;
}

test "SQL relation lifetime private lock read dispatch rejects mixed row selectors and route substitution" {
    const a = std.testing.allocator;
    try validateOptions(.{ .sql_lock_query_json = "control" });
    try std.testing.expectError(error.InvalidLockScope, validateOptions(.{ .sql_lock_query_json = "control", .relational_integrity_catalog = true }));
    try std.testing.expectError(error.InvalidLockScope, validateOptions(.{ .sql_lock_query_json = "control", .fields = &.{"row"} }));
    const p: local.common_topology_records.RelationPersistence.Descriptor = .{ .kind = .unlogged, .owner_node_id = 7, .owner_store_id = 8, .generation_instance = @splat(1), .data_generation = 1 };
    const fence: Fence = .{ .metadata_group_id = 1, .metadata_incarnation = @splat('a'), .catalog_revision = 2, .table_id = 3, .topology_epoch = 4, .route = .{ .group_id = 5, .range_id = 7, .identity_namespace = .{ .table_id = 3, .shard_id = 6, .range_id = 7 }, .execution = .{ .native_local = p } } };
    try requireSameRoute(fence, fence);
    var changed = fence;
    changed.route.execution.native_local.data_generation += 1;
    try std.testing.expectError(error.LockOwnerChanged, requireSameRoute(fence, changed));
    changed = fence;
    changed.catalog_revision += 1;
    try std.testing.expectError(error.LockOwnerChanged, requireSameRoute(fence, changed));
    try std.testing.expectError(error.InvalidLockScope, isPrivateControl(a, "row", .{ .sql_lock_query_json = "{}" }, fence));
    try std.testing.expectError(error.MissingField, isPrivateControl(a, "", .{ .sql_lock_query_json = "{}" }, fence));
}
