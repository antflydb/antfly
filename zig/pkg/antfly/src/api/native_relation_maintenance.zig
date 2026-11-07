// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! A fixed native metadata maintenance operation, never a SQL row capability.
const std = @import("std");
const local = @import("antfly_local_sources");
const operation = local.api_operation;
pub const Request = @import("../metadata/sql_relation_boots.zig").MaintenanceRequest;
pub const Result = enum { enforced, pending, invalid };

pub fn requestFor(table: local.common_topology_records.TableRecord, range: local.common_topology_records.RangeRecord) !Request {
    var request: Request = .{ .owner_group_id = range.group_id, .namespace = .{ .table_id = table.table_id, .shard_id = @import("../metadata/table_manager.zig").rangeDocIdentityShardId(range), .range_id = @import("../metadata/table_manager.zig").rangeDocIdentityRangeId(range) }, .descriptor = table.sql_persistence orelse return error.InvalidSqlRelationPersistence, .schema_digest = undefined, .indexes_digest = undefined };
    std.crypto.hash.Blake3.hash(table.schema_json, &request.schema_digest, .{});
    std.crypto.hash.Blake3.hash(table.indexes_json, &request.indexes_digest, .{});
    try request.validate();
    if (range.table_id != table.table_id) return error.IdentityNamespaceMismatch;
    return request;
}
pub fn requireAuthority(context: operation.RequestContext) !void {
    try context.ensureActive();
    if (!context.setting_admin or !context.schema_validation_authority or !context.fk_generation_publication_authority or context.sql_relation_owner != null or context.sql_relation_owner_minter != null) return error.Forbidden;
}
pub const Port = struct {
    ptr: *anyopaque,
    execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, Request, operation.RequestContext) anyerror!Result,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,
    const VTable = struct { execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, Request, operation.RequestContext) anyerror!Result };
    const BoundaryAbi = local.runtime_callback_abi.Boundary(VTable);
    pub fn execute(self: Port, a: std.mem.Allocator, table: []const u8, request: Request, context: operation.RequestContext) !Result {
        try requireAuthority(context);
        try request.validate();
        return BoundaryAbi.call("execute_fn", self.boundary_dispatch, self.execute_fn, .{ self.ptr, a, table, request, context });
    }
};
