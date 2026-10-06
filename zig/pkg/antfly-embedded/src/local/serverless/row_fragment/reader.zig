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

//! Reader helpers for decoded Antfly-owned row fragment artifacts.

const std = @import("std");
const Allocator = std.mem.Allocator;
const codec = @import("codec.zig");
const row_fragment = @import("types.zig");
const writer = @import("writer.zig");

pub const Reader = struct {
    alloc: Allocator,
    fragment: row_fragment.Fragment,

    pub fn decodeAlloc(alloc: Allocator, bytes: []const u8) !Reader {
        return .{
            .alloc = alloc,
            .fragment = try codec.decodeAlloc(alloc, bytes),
        };
    }

    pub fn deinit(self: *Reader) void {
        self.fragment.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn rowCount(self: Reader) usize {
        return self.fragment.rowCount();
    }

    pub fn findColumnIndex(self: Reader, name: []const u8) ?usize {
        for (self.fragment.columns, 0..) |column_ref, idx| {
            if (std.mem.eql(u8, column_ref.name, name)) return idx;
        }
        return null;
    }

    pub fn column(self: Reader, name: []const u8) ?row_fragment.Column {
        const idx = self.findColumnIndex(name) orelse return null;
        return self.fragment.columns[idx];
    }

    pub fn projectAlloc(self: Reader, names: []const []const u8) !row_fragment.Fragment {
        var projected = try writer.Builder.init(self.alloc, self.fragment.schema_fingerprint);
        errdefer projected.deinit();

        for (self.fragment.row_refs) |row_ref| {
            try projected.appendRowWithOrdinal(row_ref.key, row_ref.ordinal);
        }

        for (names) |name| {
            const column_ref = self.column(name) orelse return error.RowFragmentColumnNotFound;
            try projected.appendColumn(column_ref.name, column_ref.kind, column_ref.values);
        }

        return try projected.finish();
    }

    pub fn singleRowAlloc(self: Reader, row_index: usize, names: []const []const u8) !row_fragment.Fragment {
        if (row_index >= self.fragment.row_refs.len) return error.RowFragmentRowNotFound;

        var projected = try writer.Builder.init(self.alloc, self.fragment.schema_fingerprint);
        errdefer projected.deinit();

        const row_ref = self.fragment.row_refs[row_index];
        try projected.appendRowWithOrdinal(row_ref.key, row_ref.ordinal);

        for (names) |name| {
            const column_ref = self.column(name) orelse return error.RowFragmentColumnNotFound;
            try projected.appendColumn(column_ref.name, column_ref.kind, column_ref.values[row_index .. row_index + 1]);
        }

        return try projected.finish();
    }
};

test "row fragment reader projects columns and rows" {
    const alloc = std.testing.allocator;
    var builder = try writer.Builder.init(alloc, "schema-v1");
    defer builder.deinit();

    try builder.appendRow("row:a");
    try builder.appendRow("row:b");

    const tenants = [_]row_fragment.CellValue{
        .{ .bytes = @constCast("t1") },
        .{ .bytes = @constCast("t2") },
    };
    try builder.appendColumn("tenant", .bytes, &tenants);

    const amounts = [_]row_fragment.CellValue{
        .{ .i64 = 10 },
        .{ .i64 = 20 },
    };
    try builder.appendColumn("amount", .i64, &amounts);

    const encoded = try builder.encodeAlloc();
    defer alloc.free(encoded);

    var reader = try Reader.decodeAlloc(alloc, encoded);
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 2), reader.rowCount());
    try std.testing.expect(reader.findColumnIndex("amount") != null);

    const names = [_][]const u8{"amount"};
    var projected = try reader.projectAlloc(&names);
    defer projected.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), projected.columns.len);
    try std.testing.expectEqualStrings("amount", projected.columns[0].name);

    var single = try reader.singleRowAlloc(1, &names);
    defer single.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), single.rowCount());
    try std.testing.expectEqualStrings("row:b", single.row_refs[0].key);
    try std.testing.expectEqual(@as(i64, 20), single.columns[0].values[0].i64);
}
