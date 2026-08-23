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

//! Builder for Antfly-owned immutable row fragment artifacts.

const std = @import("std");
const Allocator = std.mem.Allocator;
const codec = @import("codec.zig");
const row_fragment = @import("types.zig");

pub const Builder = struct {
    alloc: Allocator,
    schema_fingerprint: []u8,
    row_refs: std.ArrayListUnmanaged(row_fragment.RowRef) = .empty,
    columns: std.ArrayListUnmanaged(row_fragment.Column) = .empty,

    pub fn init(alloc: Allocator, schema_fingerprint: []const u8) !Builder {
        return .{
            .alloc = alloc,
            .schema_fingerprint = try alloc.dupe(u8, schema_fingerprint),
        };
    }

    pub fn deinit(self: *Builder) void {
        if (self.schema_fingerprint.len != 0) self.alloc.free(self.schema_fingerprint);
        for (self.row_refs.items) |*row_ref| row_ref.deinit(self.alloc);
        self.row_refs.deinit(self.alloc);
        for (self.columns.items) |*column| column.deinit(self.alloc);
        self.columns.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn appendRow(self: *Builder, key: []const u8) !void {
        try self.appendRowWithOrdinal(key, self.row_refs.items.len);
    }

    pub fn appendRowWithOrdinal(self: *Builder, key: []const u8, ordinal: u64) !void {
        const owned_key = try self.alloc.dupe(u8, key);
        errdefer self.alloc.free(owned_key);
        try self.row_refs.append(self.alloc, .{
            .key = owned_key,
            .ordinal = ordinal,
        });
    }

    pub fn appendColumn(
        self: *Builder,
        name: []const u8,
        kind: row_fragment.ColumnKind,
        values: []const row_fragment.CellValue,
    ) !void {
        if (values.len != self.row_refs.items.len) return error.InvalidRowFragment;

        const owned_name = try self.alloc.dupe(u8, name);
        var keep_column = false;
        errdefer if (!keep_column) self.alloc.free(owned_name);

        const owned_values = try self.alloc.alloc(row_fragment.CellValue, values.len);
        var initialized_values: usize = 0;
        errdefer if (!keep_column) {
            for (owned_values[0..initialized_values]) |*value| value.deinit(self.alloc);
            self.alloc.free(owned_values);
        };

        for (values, owned_values) |value, *owned_value| {
            owned_value.* = try cloneCell(self.alloc, value);
            initialized_values += 1;
        }

        var column = row_fragment.Column{
            .name = owned_name,
            .kind = kind,
            .values = owned_values,
        };
        try column.validateCellKinds();
        try self.columns.append(self.alloc, column);
        keep_column = true;
    }

    pub fn finish(self: *Builder) !row_fragment.Fragment {
        const row_refs = try self.row_refs.toOwnedSlice(self.alloc);
        errdefer {
            for (row_refs) |*row_ref| row_ref.deinit(self.alloc);
            self.alloc.free(row_refs);
        }

        const columns = try self.columns.toOwnedSlice(self.alloc);
        errdefer {
            for (columns) |*column| column.deinit(self.alloc);
            self.alloc.free(columns);
        }

        const fragment = row_fragment.Fragment{
            .schema_fingerprint = self.schema_fingerprint,
            .row_refs = row_refs,
            .columns = columns,
        };
        try fragment.validate();
        self.schema_fingerprint = &.{};
        self.row_refs = .empty;
        self.columns = .empty;
        return fragment;
    }

    pub fn encodeAlloc(self: *Builder) ![]u8 {
        var fragment = try self.finish();
        defer fragment.deinit(self.alloc);
        return codec.encodeAlloc(self.alloc, fragment);
    }
};

fn cloneCell(alloc: Allocator, value: row_fragment.CellValue) !row_fragment.CellValue {
    return switch (value) {
        .null => .null,
        .bytes => |bytes| .{ .bytes = try alloc.dupe(u8, bytes) },
        .json => |bytes| .{ .json = try alloc.dupe(u8, bytes) },
        .i64 => |int| .{ .i64 = int },
        .f64 => |float| .{ .f64 = float },
        .bool => |boolean| .{ .bool = boolean },
        .vector_f32 => |vector| .{ .vector_f32 = try alloc.dupe(f32, vector) },
    };
}

test "row fragment builder creates encoded fragment" {
    const alloc = std.testing.allocator;
    var builder = try Builder.init(alloc, "schema-v1");
    defer builder.deinit();

    try builder.appendRow("row:a");
    try builder.appendRow("row:b");

    const amounts = [_]row_fragment.CellValue{
        .{ .i64 = 10 },
        .{ .i64 = 20 },
    };
    try builder.appendColumn("amount", .i64, &amounts);

    const encoded = try builder.encodeAlloc();
    defer alloc.free(encoded);

    var decoded = try codec.decodeAlloc(alloc, encoded);
    defer decoded.deinit(alloc);

    try std.testing.expectEqualStrings("schema-v1", decoded.schema_fingerprint);
    try std.testing.expectEqual(@as(usize, 2), decoded.rowCount());
    try std.testing.expectEqual(@as(i64, 20), decoded.columns[0].values[1].i64);
}
