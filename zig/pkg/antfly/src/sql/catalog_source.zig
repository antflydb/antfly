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
const table_record = @import("../metadata/catalog/table_record.zig");

pub const SqlCatalogSnapshot = struct {
    tables: []table_record.TableRecord,
    owner: ?*anyopaque = null,
};

pub const SqlCatalogSource = struct {
    ptr: *anyopaque,
    context: ?*const anyopaque = null,
    vtable: *const VTable,

    pub const VTable = struct {
        snapshot_alloc: *const fn (
            ptr: *anyopaque,
            context: ?*const anyopaque,
            alloc: std.mem.Allocator,
        ) anyerror!SqlCatalogSnapshot,
        free_snapshot: *const fn (
            ptr: *anyopaque,
            context: ?*const anyopaque,
            alloc: std.mem.Allocator,
            snapshot: *SqlCatalogSnapshot,
        ) void,
    };

    pub fn snapshotAlloc(self: @This(), alloc: std.mem.Allocator) !SqlCatalogSnapshot {
        return try self.vtable.snapshot_alloc(self.ptr, self.context, alloc);
    }

    pub fn freeSnapshot(self: @This(), alloc: std.mem.Allocator, snapshot: *SqlCatalogSnapshot) void {
        self.vtable.free_snapshot(self.ptr, self.context, alloc, snapshot);
    }
};

pub fn tableRecordByName(snapshot: *const SqlCatalogSnapshot, table_name: []const u8) ?*const table_record.TableRecord {
    for (snapshot.tables) |*table| {
        if (std.mem.eql(u8, table.name, table_name)) return table;
    }
    return null;
}

pub fn qualifiedTableRecord(
    snapshot: *const SqlCatalogSnapshot,
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
) ?*const table_record.TableRecord {
    for (snapshot.tables) |*table| {
        if (std.mem.eql(u8, table.database_name, database_name) and
            std.mem.eql(u8, table.namespace_name, namespace_name) and
            std.mem.eql(u8, table.name, table_name))
        {
            return table;
        }
    }
    return null;
}

pub fn tableSchemaJsonAlloc(
    alloc: std.mem.Allocator,
    source: SqlCatalogSource,
    table_name: []const u8,
) !?[]u8 {
    var snapshot = try source.snapshotAlloc(alloc);
    defer source.freeSnapshot(alloc, &snapshot);
    const table = tableRecordByName(&snapshot, table_name) orelse return null;
    if (table.schema_json.len == 0) return null;
    return try alloc.dupe(u8, table.schema_json);
}

pub fn qualifiedTableExists(
    alloc: std.mem.Allocator,
    source: SqlCatalogSource,
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
) !bool {
    var snapshot = try source.snapshotAlloc(alloc);
    defer source.freeSnapshot(alloc, &snapshot);
    return qualifiedTableRecord(&snapshot, database_name, namespace_name, table_name) != null;
}

pub fn emptySqlCatalogSource() SqlCatalogSource {
    return .{
        .ptr = undefined,
        .vtable = &.{
            .snapshot_alloc = emptySnapshotAlloc,
            .free_snapshot = freeBorrowedSnapshot,
        },
    };
}

pub fn unavailableSqlCatalogSource() SqlCatalogSource {
    return .{
        .ptr = undefined,
        .vtable = &.{
            .snapshot_alloc = unavailableSnapshotAlloc,
            .free_snapshot = freeBorrowedSnapshot,
        },
    };
}

fn emptySnapshotAlloc(_: *anyopaque, _: ?*const anyopaque, _: std.mem.Allocator) !SqlCatalogSnapshot {
    return .{ .tables = @constCast((&[_]table_record.TableRecord{})[0..]) };
}

fn unavailableSnapshotAlloc(_: *anyopaque, _: ?*const anyopaque, _: std.mem.Allocator) !SqlCatalogSnapshot {
    return error.UnsupportedOperation;
}

fn freeBorrowedSnapshot(_: *anyopaque, _: ?*const anyopaque, _: std.mem.Allocator, snapshot: *SqlCatalogSnapshot) void {
    snapshot.* = undefined;
}

test "sql catalog empty and unavailable sources are explicit" {
    const alloc = std.testing.allocator;
    const empty = emptySqlCatalogSource();
    var snapshot = try empty.snapshotAlloc(alloc);
    defer empty.freeSnapshot(alloc, &snapshot);
    try std.testing.expectEqual(@as(usize, 0), snapshot.tables.len);

    try std.testing.expectError(error.UnsupportedOperation, unavailableSqlCatalogSource().snapshotAlloc(alloc));
}

test "sql catalog lookup helpers use only table records" {
    const alloc = std.testing.allocator;
    const Fake = struct {
        const tables = [_]table_record.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .database_name = "analytics",
            .namespace_name = "tenant",
            .schema_json = "{\"version\":1}",
        }};

        fn source() SqlCatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .snapshot_alloc = snapshotAlloc,
                    .free_snapshot = freeSnapshot,
                },
            };
        }

        fn snapshotAlloc(_: *anyopaque, _: ?*const anyopaque, _: std.mem.Allocator) !SqlCatalogSnapshot {
            return .{ .tables = @constCast(tables[0..]) };
        }

        fn freeSnapshot(_: *anyopaque, _: ?*const anyopaque, _: std.mem.Allocator, snapshot: *SqlCatalogSnapshot) void {
            snapshot.* = undefined;
        }
    };

    const source = Fake.source();
    const schema_json = (try tableSchemaJsonAlloc(alloc, source, "docs")).?;
    defer alloc.free(schema_json);
    try std.testing.expectEqualStrings("{\"version\":1}", schema_json);
    try std.testing.expect(try qualifiedTableExists(alloc, source, "analytics", "tenant", "docs"));
    try std.testing.expect(!try qualifiedTableExists(alloc, source, "analytics", "public", "docs"));
}
