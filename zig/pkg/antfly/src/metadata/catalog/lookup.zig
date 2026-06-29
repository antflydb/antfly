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
const metadata_api = @import("snapshot.zig");
const metadata_table_manager = @import("../table_manager.zig");

pub const default_database_name = metadata_table_manager.default_database_name;
pub const default_namespace_name = metadata_table_manager.default_namespace_name;

pub const QualifiedTableNameParts = struct {
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
};

pub fn qualifiedTableNameParts(table_name: []const u8) ?QualifiedTableNameParts {
    const first_dot = std.mem.indexOfScalar(u8, table_name, '.') orelse return null;
    if (first_dot == 0) return null;
    const rest = table_name[first_dot + 1 ..];
    const second_dot_rel = std.mem.indexOfScalar(u8, rest, '.') orelse return null;
    if (second_dot_rel == 0) return null;
    const second_dot = first_dot + 1 + second_dot_rel;
    if (second_dot + 1 >= table_name.len) return null;
    const leaf = table_name[second_dot + 1 ..];
    if (std.mem.indexOfScalar(u8, leaf, '.') != null) return null;
    return .{
        .database_name = table_name[0..first_dot],
        .namespace_name = table_name[first_dot + 1 .. second_dot],
        .table_name = leaf,
    };
}

pub fn findTableByName(snapshot: *const metadata_api.AdminSnapshot, table_name: []const u8) ?*const metadata_table_manager.TableRecord {
    if (qualifiedTableNameParts(table_name)) |parts| {
        if (findTableByQualifiedName(snapshot, parts.database_name, parts.namespace_name, parts.table_name)) |table| return table;
    }
    return findTableByQualifiedName(snapshot, default_database_name, default_namespace_name, table_name);
}

pub fn findTableByQualifiedName(
    snapshot: *const metadata_api.AdminSnapshot,
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
) ?*const metadata_table_manager.TableRecord {
    for (snapshot.tables) |*record| {
        if (tableCatalogIdentityMatches(record.*, database_name, namespace_name, table_name)) return record;
    }
    return null;
}

pub fn tableCatalogIdentityMatches(
    record: metadata_table_manager.TableRecord,
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
) bool {
    return std.mem.eql(u8, record.database_name, database_name) and
        std.mem.eql(u8, record.namespace_name, namespace_name) and
        std.mem.eql(u8, record.name, table_name);
}

pub fn findDatabaseByName(snapshot: *const metadata_api.AdminSnapshot, database_name: []const u8) ?*const metadata_table_manager.DatabaseRecord {
    const database_id = metadata_table_manager.deriveDatabaseId(database_name);
    for (snapshot.databases) |*record| {
        if (record.database_id == database_id and std.mem.eql(u8, record.name, database_name)) return record;
    }
    return null;
}

pub fn findNamespaceByName(
    snapshot: *const metadata_api.AdminSnapshot,
    database_name: []const u8,
    namespace_name: []const u8,
) ?*const metadata_table_manager.NamespaceRecord {
    const database_id = metadata_table_manager.deriveDatabaseId(database_name);
    const namespace_id = metadata_table_manager.deriveNamespaceId(database_id, namespace_name);
    for (snapshot.namespaces) |*record| {
        if (record.namespace_id == namespace_id and record.database_id == database_id and std.mem.eql(u8, record.name, namespace_name)) return record;
    }
    return null;
}

pub fn findTablespaceByName(snapshot: *const metadata_api.AdminSnapshot, tablespace_name: []const u8) ?*const metadata_table_manager.TablespaceRecord {
    const tablespace_id = metadata_table_manager.deriveTablespaceId(tablespace_name);
    for (snapshot.tablespaces) |*record| {
        if (record.tablespace_id == tablespace_id and std.mem.eql(u8, record.name, tablespace_name)) return record;
    }
    return null;
}

pub fn findSequenceByQualifiedName(
    snapshot: *const metadata_api.AdminSnapshot,
    database_name: []const u8,
    namespace_name: []const u8,
    sequence_name: []const u8,
) ?*const metadata_table_manager.SequenceRecord {
    const sequence_id = metadata_table_manager.deriveSequenceId(database_name, namespace_name, sequence_name);
    for (snapshot.sequences) |*record| {
        if (record.sequence_id == sequence_id and
            std.mem.eql(u8, record.database_name, database_name) and
            std.mem.eql(u8, record.namespace_name, namespace_name) and
            std.mem.eql(u8, record.name, sequence_name))
        {
            return record;
        }
    }
    return null;
}

pub fn effectiveTablespaceForTarget(
    snapshot: *const metadata_api.AdminSnapshot,
    database_name: []const u8,
    namespace_name: []const u8,
    explicit_tablespace_name: ?[]const u8,
) ?*const metadata_table_manager.TablespaceRecord {
    if (explicit_tablespace_name) |name| {
        if (name.len > 0) return findTablespaceByName(snapshot, name);
    }
    if (findNamespaceByName(snapshot, database_name, namespace_name)) |namespace| {
        if (namespace.tablespace_name.len > 0) return findTablespaceByName(snapshot, namespace.tablespace_name);
    }
    if (findDatabaseByName(snapshot, database_name)) |database| {
        if (database.tablespace_name.len > 0) return findTablespaceByName(snapshot, database.tablespace_name);
    }
    return null;
}

test "catalog lookup resolves default and qualified table identities" {
    const table = metadata_table_manager.TableRecord{
        .table_id = 7,
        .name = "docs",
        .database_name = "tenant",
        .namespace_name = "analytics",
    };
    var tables = [_]metadata_table_manager.TableRecord{table};
    const snapshot = metadata_api.AdminSnapshot{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = &tables,
    };

    try std.testing.expect(findTableByName(&snapshot, "docs") == null);
    try std.testing.expectEqual(@as(u64, 7), findTableByName(&snapshot, "tenant.analytics.docs").?.table_id);
    try std.testing.expectEqual(@as(u64, 7), findTableByQualifiedName(&snapshot, "tenant", "analytics", "docs").?.table_id);
    try std.testing.expect(qualifiedTableNameParts("tenant.analytics.docs") != null);
    try std.testing.expect(qualifiedTableNameParts("tenant.analytics.docs.extra") == null);
}
