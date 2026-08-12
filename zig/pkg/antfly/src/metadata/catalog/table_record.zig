// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

const std = @import("std");

pub const default_database_name = "default";
pub const default_namespace_name = "public";

pub const TableMigrationState = struct {
    schema_json: []const u8,
    read_schema_json: []const u8,

    pub fn migrating(self: @This()) bool {
        return self.read_schema_json.len > 0;
    }
};

pub const TableIndexCatalog = struct {
    indexes_json: []const u8,
};

pub const TableRecord = struct {
    table_id: u64,
    name: []const u8,
    database_name: []const u8 = default_database_name,
    namespace_name: []const u8 = default_namespace_name,
    description: []const u8 = "",
    schema_json: []const u8 = "",
    read_schema_json: []const u8 = "",
    foreign_key_validation_json: []const u8 = "{}",
    indexes_json: []const u8 = "{}",
    replication_sources_json: []const u8 = "[]",
    placement_role: []const u8 = "data",
    tablespace_name: []const u8 = "",
    restore_backup_id: []const u8 = "",
    restore_location: []const u8 = "",
    desired_replica_count: u16 = 3,
    min_ranges: u32 = 1,
    data_generation: u64 = 0,

    pub fn migrationState(self: *const TableRecord) TableMigrationState {
        return .{
            .schema_json = self.schema_json,
            .read_schema_json = self.read_schema_json,
        };
    }

    pub fn indexCatalog(self: *const TableRecord) TableIndexCatalog {
        return .{ .indexes_json = self.indexes_json };
    }
};

pub fn schemaRewriteGenerationForSchemaJson(schema_json: []const u8) u64 {
    const value = std.hash.Wyhash.hash(0x53434a47, schema_json);
    return if (value == 0) 1 else value;
}

pub fn cloneTable(alloc: std.mem.Allocator, record: TableRecord) !TableRecord {
    var cloned = record;
    cloned.name = try alloc.dupe(u8, record.name);
    errdefer alloc.free(cloned.name);
    cloned.database_name = try alloc.dupe(u8, record.database_name);
    errdefer alloc.free(cloned.database_name);
    cloned.namespace_name = try alloc.dupe(u8, record.namespace_name);
    errdefer alloc.free(cloned.namespace_name);
    cloned.description = try alloc.dupe(u8, record.description);
    errdefer alloc.free(cloned.description);
    cloned.schema_json = try alloc.dupe(u8, record.schema_json);
    errdefer alloc.free(cloned.schema_json);
    cloned.read_schema_json = try alloc.dupe(u8, record.read_schema_json);
    errdefer alloc.free(cloned.read_schema_json);
    cloned.foreign_key_validation_json = try alloc.dupe(u8, record.foreign_key_validation_json);
    errdefer alloc.free(cloned.foreign_key_validation_json);
    cloned.indexes_json = try alloc.dupe(u8, record.indexes_json);
    errdefer alloc.free(cloned.indexes_json);
    cloned.replication_sources_json = try alloc.dupe(u8, record.replication_sources_json);
    errdefer alloc.free(cloned.replication_sources_json);
    cloned.placement_role = try alloc.dupe(u8, record.placement_role);
    errdefer alloc.free(cloned.placement_role);
    cloned.tablespace_name = try alloc.dupe(u8, record.tablespace_name);
    errdefer alloc.free(cloned.tablespace_name);
    cloned.restore_backup_id = try alloc.dupe(u8, record.restore_backup_id);
    errdefer alloc.free(cloned.restore_backup_id);
    cloned.restore_location = try alloc.dupe(u8, record.restore_location);
    return cloned;
}

pub fn freeTable(alloc: std.mem.Allocator, record: TableRecord) void {
    alloc.free(record.name);
    alloc.free(record.database_name);
    alloc.free(record.namespace_name);
    alloc.free(record.description);
    alloc.free(record.schema_json);
    alloc.free(record.read_schema_json);
    alloc.free(record.foreign_key_validation_json);
    alloc.free(record.indexes_json);
    alloc.free(record.replication_sources_json);
    alloc.free(record.placement_role);
    alloc.free(record.tablespace_name);
    alloc.free(record.restore_backup_id);
    alloc.free(record.restore_location);
}

test "table record clone owns every string field" {
    const alloc = std.testing.allocator;
    const record: TableRecord = .{ .table_id = 1, .name = "docs" };
    const cloned = try cloneTable(alloc, record);
    defer freeTable(alloc, cloned);
    try std.testing.expectEqualStrings(record.name, cloned.name);
    try std.testing.expect(cloned.name.ptr != record.name.ptr);
}
