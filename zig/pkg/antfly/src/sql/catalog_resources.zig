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
const metadata_table_manager = @import("../metadata/table_manager.zig");

pub const default_database_name = metadata_table_manager.default_database_name;
pub const default_namespace_name = metadata_table_manager.default_namespace_name;

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

pub fn tablespaceResourceNameAlloc(alloc: std.mem.Allocator, tablespace_name: []const u8) ![]u8 {
    if (tablespace_name.len == 0 or std.mem.indexOfScalar(u8, tablespace_name, '.') != null) return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, tablespace_name);
}

pub fn defaultPublicTableResourceNameAlloc(alloc: std.mem.Allocator, table_name: []const u8) ![]u8 {
    return try tableResourceNameAlloc(alloc, default_database_name, default_namespace_name, table_name);
}

pub const TableTarget = struct {
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
};

pub const NamespaceTarget = struct {
    database_name: []const u8,
    namespace_name: []const u8,
};

pub const SqlSessionSetting = struct {
    name: []const u8,
    value: []const u8,
};

pub fn namespaceTargetFromOptional(
    database_name: ?[]const u8,
    namespace_name: ?[]const u8,
) NamespaceTarget {
    return .{
        .database_name = database_name orelse default_database_name,
        .namespace_name = namespace_name orelse default_namespace_name,
    };
}

pub fn tableTargetFromOptional(
    database_name: ?[]const u8,
    namespace_name: ?[]const u8,
    table_name: []const u8,
) !TableTarget {
    if (table_name.len == 0) return error.UnsupportedSqlShape;
    const namespace = namespaceTargetFromOptional(database_name, namespace_name);
    return .{
        .database_name = namespace.database_name,
        .namespace_name = namespace.namespace_name,
        .table_name = table_name,
    };
}

pub fn isDefaultPublicNamespace(database_name: []const u8, namespace_name: []const u8) bool {
    return std.mem.eql(u8, database_name, default_database_name) and
        std.mem.eql(u8, namespace_name, default_namespace_name);
}

pub fn namespaceIsDefaultPublic(target: NamespaceTarget) bool {
    return isDefaultPublicNamespace(target.database_name, target.namespace_name);
}

pub fn tableIsDefaultPublic(target: TableTarget) bool {
    return isDefaultPublicNamespace(target.database_name, target.namespace_name);
}

pub fn storageTableNameForTargetAlloc(alloc: std.mem.Allocator, target: TableTarget) ![]u8 {
    if (tableIsDefaultPublic(target)) return try alloc.dupe(u8, target.table_name);
    return try tableResourceNameAlloc(alloc, target.database_name, target.namespace_name, target.table_name);
}

pub fn tableIdStorageResourceNameAlloc(alloc: std.mem.Allocator, table_id: u64) ![]u8 {
    if (table_id == 0) return error.UnsupportedSqlShape;
    return try std.fmt.allocPrint(alloc, "table:{d}", .{table_id});
}

pub fn storageTableNameForRecordAlloc(alloc: std.mem.Allocator, record: metadata_table_manager.TableRecord) ![]u8 {
    return try tableIdStorageResourceNameAlloc(alloc, record.table_id);
}

pub const SqlCatalogSession = struct {
    current_database_name: []const u8 = default_database_name,
    search_path: []const []const u8 = &.{default_namespace_name},
    settings: []const SqlSessionSetting = &.{},

    pub fn default() SqlCatalogSession {
        return .{};
    }

    pub fn primarySearchPathNamespace(self: SqlCatalogSession) []const u8 {
        if (self.search_path.len == 0) return default_namespace_name;
        return self.search_path[0];
    }

    pub fn currentDatabase(self: SqlCatalogSession) []const u8 {
        if (self.current_database_name.len == 0) return default_database_name;
        return self.current_database_name;
    }

    pub fn settingValue(self: SqlCatalogSession, name: []const u8) ?[]const u8 {
        for (self.settings) |setting| {
            if (std.ascii.eqlIgnoreCase(setting.name, name)) return setting.value;
        }
        return null;
    }

    pub fn tableTargetFromObjectName(self: SqlCatalogSession, object_name: []const u8) !TableTarget {
        if (object_name.len == 0) return error.UnsupportedSqlShape;
        if (std.mem.indexOfScalar(u8, object_name, '.')) |dot| {
            if (dot == 0) return error.UnsupportedSqlShape;
            const first_component = object_name[0..dot];
            const rest = object_name[dot + 1 ..];
            if (first_component.len == 0 or rest.len == 0) return error.UnsupportedSqlShape;
            if (std.mem.indexOfScalar(u8, rest, '.')) |second_dot| {
                const namespace_name = rest[0..second_dot];
                const table_name = rest[second_dot + 1 ..];
                if (namespace_name.len == 0 or table_name.len == 0) return error.UnsupportedSqlShape;
                if (std.mem.indexOfScalar(u8, table_name, '.') != null) return error.UnsupportedSqlShape;
                return .{
                    .database_name = first_component,
                    .namespace_name = namespace_name,
                    .table_name = table_name,
                };
            }
            return .{
                .database_name = self.currentDatabase(),
                .namespace_name = first_component,
                .table_name = rest,
            };
        }
        return .{
            .database_name = self.currentDatabase(),
            .namespace_name = self.primarySearchPathNamespace(),
            .table_name = object_name,
        };
    }

    pub fn namespaceTargetFromSchemaName(self: SqlCatalogSession, schema_name: []const u8) !NamespaceTarget {
        if (schema_name.len == 0) return error.UnsupportedSqlShape;
        if (std.mem.indexOfScalar(u8, schema_name, '.')) |dot| {
            if (dot == 0) return error.UnsupportedSqlShape;
            const database_name = schema_name[0..dot];
            const namespace_name = schema_name[dot + 1 ..];
            if (database_name.len == 0 or namespace_name.len == 0) return error.UnsupportedSqlShape;
            if (std.mem.indexOfScalar(u8, namespace_name, '.') != null) return error.UnsupportedSqlShape;
            return .{
                .database_name = database_name,
                .namespace_name = namespace_name,
            };
        }
        return .{
            .database_name = self.currentDatabase(),
            .namespace_name = schema_name,
        };
    }
};

pub fn sqlTableTargetFromObjectNameWithSession(object_name: []const u8, session: SqlCatalogSession) !TableTarget {
    return try session.tableTargetFromObjectName(object_name);
}

pub fn sqlTableTargetFromObjectName(object_name: []const u8, current_database_name: []const u8) !TableTarget {
    return try sqlTableTargetFromObjectNameWithSession(object_name, .{ .current_database_name = current_database_name });
}

pub fn tableResourceNameFromSqlObjectAlloc(
    alloc: std.mem.Allocator,
    object_name: []const u8,
    current_database_name: []const u8,
) ![]u8 {
    const target = try sqlTableTargetFromObjectName(object_name, current_database_name);
    return try tableResourceNameAlloc(alloc, target.database_name, target.namespace_name, target.table_name);
}

pub fn tableResourceNameFromSqlObjectWithSessionAlloc(
    alloc: std.mem.Allocator,
    object_name: []const u8,
    session: SqlCatalogSession,
) ![]u8 {
    const target = try sqlTableTargetFromObjectNameWithSession(object_name, session);
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

    const namespace = namespaceTargetFromOptional("tenant_ops", "analytics");
    try std.testing.expectEqualStrings("tenant_ops", namespace.database_name);
    try std.testing.expectEqualStrings("analytics", namespace.namespace_name);
    try std.testing.expect(!namespaceIsDefaultPublic(namespace));

    const default_target = try tableTargetFromOptional(null, null, "docs");
    try std.testing.expect(tableIsDefaultPublic(default_target));
    try std.testing.expectEqualStrings("default", default_target.database_name);
    try std.testing.expectEqualStrings("public", default_target.namespace_name);
    try std.testing.expectEqualStrings("docs", default_target.table_name);

    const default_storage = try storageTableNameForTargetAlloc(alloc, default_target);
    defer alloc.free(default_storage);
    try std.testing.expectEqualStrings("docs", default_storage);

    const qualified_target = try tableTargetFromOptional("tenant_ops", "analytics", "events");
    const qualified_storage = try storageTableNameForTargetAlloc(alloc, qualified_target);
    defer alloc.free(qualified_storage);
    try std.testing.expectEqualStrings("tenant_ops.analytics.events", qualified_storage);

    const stable_storage = try tableIdStorageResourceNameAlloc(alloc, 42);
    defer alloc.free(stable_storage);
    try std.testing.expectEqualStrings("table:42", stable_storage);

    const record_storage = try storageTableNameForRecordAlloc(alloc, .{
        .table_id = 42,
        .name = "events",
        .database_name = "tenant_ops",
        .namespace_name = "analytics",
    });
    defer alloc.free(record_storage);
    try std.testing.expectEqualStrings("table:42", record_storage);
    try std.testing.expectError(error.UnsupportedSqlShape, tableIdStorageResourceNameAlloc(alloc, 0));
}

test "sql catalog session maps current database and search path" {
    const default_target = try SqlCatalogSession.default().tableTargetFromObjectName("events");
    try std.testing.expectEqualStrings("default", default_target.database_name);
    try std.testing.expectEqualStrings("public", default_target.namespace_name);
    try std.testing.expectEqualStrings("events", default_target.table_name);

    const search_path = [_][]const u8{"analytics"};
    const tenant_session: SqlCatalogSession = .{
        .current_database_name = "tenant_ops",
        .search_path = &search_path,
    };
    const search_path_target = try tenant_session.tableTargetFromObjectName("events");
    try std.testing.expectEqualStrings("tenant_ops", search_path_target.database_name);
    try std.testing.expectEqualStrings("analytics", search_path_target.namespace_name);
    try std.testing.expectEqualStrings("events", search_path_target.table_name);

    const explicit_namespace_target = try tenant_session.tableTargetFromObjectName("public.events");
    try std.testing.expectEqualStrings("tenant_ops", explicit_namespace_target.database_name);
    try std.testing.expectEqualStrings("public", explicit_namespace_target.namespace_name);
    try std.testing.expectEqualStrings("events", explicit_namespace_target.table_name);

    const explicit_database_target = try tenant_session.tableTargetFromObjectName("archive.analytics.events");
    try std.testing.expectEqualStrings("archive", explicit_database_target.database_name);
    try std.testing.expectEqualStrings("analytics", explicit_database_target.namespace_name);
    try std.testing.expectEqualStrings("events", explicit_database_target.table_name);

    try std.testing.expectError(error.UnsupportedSqlShape, tenant_session.tableTargetFromObjectName("archive.analytics.events.extra"));
    try std.testing.expectError(error.UnsupportedSqlShape, tenant_session.tableTargetFromObjectName("archive..events"));
    try std.testing.expectError(error.UnsupportedSqlShape, tenant_session.tableTargetFromObjectName(".analytics.events"));

    const local_schema_namespace = try tenant_session.namespaceTargetFromSchemaName("analytics");
    try std.testing.expectEqualStrings("tenant_ops", local_schema_namespace.database_name);
    try std.testing.expectEqualStrings("analytics", local_schema_namespace.namespace_name);

    const database_qualified_namespace = try tenant_session.namespaceTargetFromSchemaName("tenant_ops.analytics");
    try std.testing.expectEqualStrings("tenant_ops", database_qualified_namespace.database_name);
    try std.testing.expectEqualStrings("analytics", database_qualified_namespace.namespace_name);

    const public_database_namespace = try tenant_session.namespaceTargetFromSchemaName("public.analytics");
    try std.testing.expectEqualStrings("public", public_database_namespace.database_name);
    try std.testing.expectEqualStrings("analytics", public_database_namespace.namespace_name);

    try std.testing.expectError(error.UnsupportedSqlShape, tenant_session.namespaceTargetFromSchemaName("public.analytics.events"));

    const tenant_database_target = try tenant_session.tableTargetFromObjectName("tenant_ops.analytics.events");
    try std.testing.expectEqualStrings("tenant_ops", tenant_database_target.database_name);
    try std.testing.expectEqualStrings("analytics", tenant_database_target.namespace_name);
    try std.testing.expectEqualStrings("events", tenant_database_target.table_name);
}
