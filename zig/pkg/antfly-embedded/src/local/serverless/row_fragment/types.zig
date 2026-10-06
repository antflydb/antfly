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

//! Antfly-owned immutable row fragment artifact types.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const ColumnKind = enum(u8) {
    bytes = 1,
    json = 2,
    i64 = 3,
    f64 = 4,
    bool = 5,
    vector_f32 = 6,
};

pub const CellValue = union(enum) {
    null,
    bytes: []u8,
    json: []u8,
    i64: i64,
    f64: f64,
    bool: bool,
    vector_f32: []f32,

    pub fn deinit(self: *CellValue, alloc: Allocator) void {
        switch (self.*) {
            .bytes => |value| alloc.free(value),
            .json => |value| alloc.free(value),
            .vector_f32 => |value| alloc.free(value),
            else => {},
        }
        self.* = undefined;
    }
};

pub const RowRef = struct {
    key: []u8,
    ordinal: u64,

    pub fn deinit(self: *RowRef, alloc: Allocator) void {
        alloc.free(self.key);
        self.* = undefined;
    }
};

pub const Column = struct {
    name: []u8,
    kind: ColumnKind,
    values: []CellValue,

    pub fn validateCellKinds(self: Column) !void {
        for (self.values) |value| {
            switch (value) {
                .null => {},
                .bytes => if (self.kind != .bytes) return error.InvalidRowFragment,
                .json => if (self.kind != .json) return error.InvalidRowFragment,
                .i64 => if (self.kind != .i64) return error.InvalidRowFragment,
                .f64 => if (self.kind != .f64) return error.InvalidRowFragment,
                .bool => if (self.kind != .bool) return error.InvalidRowFragment,
                .vector_f32 => if (self.kind != .vector_f32) return error.InvalidRowFragment,
            }
        }
    }

    pub fn deinit(self: *Column, alloc: Allocator) void {
        alloc.free(self.name);
        for (self.values) |*value| value.deinit(alloc);
        alloc.free(self.values);
        self.* = undefined;
    }
};

pub const Fragment = struct {
    schema_fingerprint: []u8,
    row_refs: []RowRef,
    columns: []Column,

    pub fn deinit(self: *Fragment, alloc: Allocator) void {
        alloc.free(self.schema_fingerprint);
        for (self.row_refs) |*row_ref| row_ref.deinit(alloc);
        alloc.free(self.row_refs);
        for (self.columns) |*column| column.deinit(alloc);
        alloc.free(self.columns);
        self.* = undefined;
    }

    pub fn rowCount(self: Fragment) usize {
        return self.row_refs.len;
    }

    pub fn validate(self: Fragment) !void {
        for (self.columns, 0..) |column, idx| {
            if (column.name.len == 0) return error.InvalidRowFragment;
            for (self.columns[0..idx]) |previous| {
                if (std.mem.eql(u8, previous.name, column.name)) return error.InvalidRowFragment;
            }
            if (column.values.len != self.row_refs.len) return error.InvalidRowFragment;
            try column.validateCellKinds();
        }
    }
};

pub fn freeFragment(alloc: Allocator, fragment: *Fragment) void {
    fragment.deinit(alloc);
}

test "row fragment validates column lengths and cell kinds" {
    const alloc = std.testing.allocator;
    var fragment = Fragment{
        .schema_fingerprint = try alloc.dupe(u8, "schema-a"),
        .row_refs = try alloc.alloc(RowRef, 1),
        .columns = try alloc.alloc(Column, 1),
    };
    defer fragment.deinit(alloc);
    fragment.row_refs[0] = .{ .key = try alloc.dupe(u8, "row:a"), .ordinal = 0 };
    fragment.columns[0] = .{
        .name = try alloc.dupe(u8, "amount"),
        .kind = .i64,
        .values = try alloc.alloc(CellValue, 1),
    };
    fragment.columns[0].values[0] = .{ .i64 = 42 };
    try fragment.validate();
}
