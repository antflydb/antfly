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
const tables_api = @import("tables.zig");

pub const default_database_name = tables_api.default_database_name;
pub const default_namespace_name = tables_api.default_namespace_name;

pub fn databaseResourceNameAlloc(alloc: std.mem.Allocator, database_name: []const u8) ![]u8 {
    if (database_name.len == 0 or std.mem.indexOfScalar(u8, database_name, '.') != null) return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, database_name);
}

pub fn namespaceResourceNameAlloc(
    alloc: std.mem.Allocator,
    database_name: []const u8,
    namespace_name: []const u8,
) ![]u8 {
    if (database_name.len == 0 or namespace_name.len == 0) return error.UnsupportedSqlShape;
    if (std.mem.indexOfScalar(u8, database_name, '.') != null or std.mem.indexOfScalar(u8, namespace_name, '.') != null) return error.UnsupportedSqlShape;
    return try std.fmt.allocPrint(alloc, "{s}.{s}", .{ database_name, namespace_name });
}

pub fn tableResourceNameAlloc(
    alloc: std.mem.Allocator,
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
) ![]u8 {
    if (database_name.len == 0 or namespace_name.len == 0 or table_name.len == 0) return error.UnsupportedSqlShape;
    if (std.mem.indexOfScalar(u8, database_name, '.') != null or
        std.mem.indexOfScalar(u8, namespace_name, '.') != null or
        std.mem.indexOfScalar(u8, table_name, '.') != null)
    {
        return error.UnsupportedSqlShape;
    }
    return try std.fmt.allocPrint(alloc, "{s}.{s}.{s}", .{ database_name, namespace_name, table_name });
}

pub fn defaultPublicTableResourceNameAlloc(alloc: std.mem.Allocator, table_name: []const u8) ![]u8 {
    return try tableResourceNameAlloc(alloc, default_database_name, default_namespace_name, table_name);
}

pub const TableTarget = struct {
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
};

pub fn sqlTableTargetFromObjectName(object_name: []const u8, current_database_name: []const u8) !TableTarget {
    if (object_name.len == 0) return error.UnsupportedSqlShape;
    if (std.mem.indexOfScalar(u8, object_name, '.')) |dot| {
        if (dot == 0) return error.UnsupportedSqlShape;
        const namespace_name = object_name[0..dot];
        const table_name = object_name[dot + 1 ..];
        if (namespace_name.len == 0 or table_name.len == 0) return error.UnsupportedSqlShape;
        if (std.mem.indexOfScalar(u8, table_name, '.') != null) return error.UnsupportedSqlShape;
        return .{
            .database_name = current_database_name,
            .namespace_name = namespace_name,
            .table_name = table_name,
        };
    }
    return .{
        .database_name = current_database_name,
        .namespace_name = default_namespace_name,
        .table_name = object_name,
    };
}

pub fn tableResourceNameFromSqlObjectAlloc(
    alloc: std.mem.Allocator,
    object_name: []const u8,
    current_database_name: []const u8,
) ![]u8 {
    const target = try sqlTableTargetFromObjectName(object_name, current_database_name);
    return try tableResourceNameAlloc(alloc, target.database_name, target.namespace_name, target.table_name);
}

pub fn tableResourceMatches(permission_resource: []const u8, requested_resource: []const u8) bool {
    if (std.mem.eql(u8, permission_resource, requested_resource)) return true;
    if (std.mem.indexOfScalar(u8, requested_resource, '.') == null) {
        return isDefaultPublicQualifiedTableResource(permission_resource, requested_resource);
    }
    if (std.mem.indexOfScalar(u8, permission_resource, '.') == null) {
        return isDefaultPublicQualifiedTableResource(requested_resource, permission_resource);
    }
    return false;
}

fn isDefaultPublicQualifiedTableResource(resource: []const u8, table_name: []const u8) bool {
    if (table_name.len == 0) return false;
    const first_dot = std.mem.indexOfScalar(u8, resource, '.') orelse return false;
    if (!std.mem.eql(u8, resource[0..first_dot], default_database_name)) return false;
    const rest = resource[first_dot + 1 ..];
    const second_dot = std.mem.indexOfScalar(u8, rest, '.') orelse return false;
    if (!std.mem.eql(u8, rest[0..second_dot], default_namespace_name)) return false;
    const qualified_table = rest[second_dot + 1 ..];
    if (std.mem.indexOfScalar(u8, qualified_table, '.') != null) return false;
    return std.mem.eql(u8, qualified_table, table_name);
}

test "catalog resource names and migration table matching" {
    const alloc = std.testing.allocator;

    const qualified = try defaultPublicTableResourceNameAlloc(alloc, "docs");
    defer alloc.free(qualified);
    try std.testing.expectEqualStrings("default.public.docs", qualified);
    try std.testing.expect(tableResourceMatches("default.public.docs", "docs"));
    try std.testing.expect(tableResourceMatches("docs", "default.public.docs"));
    try std.testing.expect(!tableResourceMatches("tenant_ops.analytics.docs", "docs"));
    try std.testing.expect(!tableResourceMatches("docs", "tenant_ops.analytics.docs"));
}
