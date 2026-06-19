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

//! RowSource adapter for decoded Antfly-owned serverless row fragments.

const std = @import("std");
const Allocator = std.mem.Allocator;
const row_fragment = @import("types.zig");
const rowsource = @import("../../storage/rowsource/types.zig");

pub const FragmentSource = struct {
    snapshot: rowsource.SnapshotRef,
    fragment_id: []const u8,
    fragment: *const row_fragment.Fragment,
    emitted: bool = false,
    materialized: ?MaterializedBatch = null,

    pub fn init(
        snapshot: rowsource.SnapshotRef,
        fragment_id: []const u8,
        fragment: *const row_fragment.Fragment,
    ) FragmentSource {
        return .{
            .snapshot = snapshot,
            .fragment_id = fragment_id,
            .fragment = fragment,
        };
    }

    pub fn rowSource(self: *FragmentSource) rowsource.Source {
        return .{
            .kind = .serverless_fragment,
            .ctx = self,
            .next_batch = nextBatch,
            .deinit_fn = deinitCtx,
        };
    }

    pub fn deinit(self: *FragmentSource, alloc: Allocator) void {
        if (self.materialized) |*batch| batch.deinit(alloc);
        self.materialized = null;
    }

    fn nextBatch(ctx: *anyopaque, alloc: Allocator) !?rowsource.ColumnBatch {
        const self: *FragmentSource = @ptrCast(@alignCast(ctx));
        if (self.emitted) return null;
        self.emitted = true;
        self.materialized = try materializeBatchAlloc(alloc, self.snapshot, self.fragment_id, self.fragment.*);
        return self.materialized.?.batch;
    }

    fn deinitCtx(ctx: *anyopaque, alloc: Allocator) void {
        const self: *FragmentSource = @ptrCast(@alignCast(ctx));
        self.deinit(alloc);
    }
};

pub const MaterializedBatch = struct {
    batch: rowsource.ColumnBatch,

    pub fn deinit(self: *MaterializedBatch, alloc: Allocator) void {
        for (self.batch.columns) |column| {
            if (column.nulls.bytes.len != 0) alloc.free(column.nulls.bytes);
            switch (column.values) {
                .bytes => |values| alloc.free(values),
                .json => |values| alloc.free(values),
                .i64 => |values| alloc.free(values),
                .f64 => |values| alloc.free(values),
                .bool => |values| alloc.free(values),
                .vector_f32 => |values| alloc.free(values),
            }
        }
        alloc.free(self.batch.columns);
        alloc.free(self.batch.row_refs);
        self.* = undefined;
    }
};

pub fn materializeBatchAlloc(
    alloc: Allocator,
    snapshot: rowsource.SnapshotRef,
    fragment_id: []const u8,
    fragment: row_fragment.Fragment,
) !MaterializedBatch {
    try fragment.validate();

    const row_refs = try alloc.alloc(rowsource.RowRef, fragment.row_refs.len);
    errdefer alloc.free(row_refs);
    for (fragment.row_refs, row_refs) |row_ref, *out| {
        out.* = .{ .serverless = .{
            .fragment_id = fragment_id,
            .row_ordinal = row_ref.ordinal,
        } };
    }

    const columns = try alloc.alloc(rowsource.ColumnVector, fragment.columns.len);
    errdefer alloc.free(columns);
    var initialized_columns: usize = 0;
    errdefer {
        for (columns[0..initialized_columns]) |column| freeColumnValues(alloc, column);
    }

    for (fragment.columns, columns) |column, *out| {
        out.* = try materializeColumnAlloc(alloc, column);
        initialized_columns += 1;
    }

    const batch = rowsource.ColumnBatch{
        .snapshot = snapshot,
        .row_refs = row_refs,
        .columns = columns,
    };
    try batch.validate();
    return .{ .batch = batch };
}

fn materializeColumnAlloc(alloc: Allocator, column: row_fragment.Column) !rowsource.ColumnVector {
    const nulls = try materializeNullsAlloc(alloc, column.values);
    errdefer if (nulls.bytes.len != 0) alloc.free(nulls.bytes);

    const values = switch (column.kind) {
        .bytes => rowsource.ColumnValues{ .bytes = try materializeBytesAlloc(alloc, column.values, .bytes) },
        .json => rowsource.ColumnValues{ .json = try materializeBytesAlloc(alloc, column.values, .json) },
        .i64 => rowsource.ColumnValues{ .i64 = try materializeI64Alloc(alloc, column.values) },
        .f64 => rowsource.ColumnValues{ .f64 = try materializeF64Alloc(alloc, column.values) },
        .bool => rowsource.ColumnValues{ .bool = try materializeBoolAlloc(alloc, column.values) },
        .vector_f32 => rowsource.ColumnValues{ .vector_f32 = try materializeVectorF32Alloc(alloc, column.values) },
    };
    errdefer freeColumnValuesOnly(alloc, values);

    return .{
        .name = column.name,
        .values = values,
        .nulls = nulls,
    };
}

fn materializeNullsAlloc(alloc: Allocator, values: []const row_fragment.CellValue) !rowsource.NullBitmap {
    var has_null = false;
    for (values) |value| {
        if (value == .null) {
            has_null = true;
            break;
        }
    }
    if (!has_null) return .{};

    const nulls = try alloc.alloc(u8, values.len);
    for (values, nulls) |value, *out| {
        out.* = if (value == .null) 1 else 0;
    }
    return .{ .bytes = nulls };
}

fn materializeBytesAlloc(
    alloc: Allocator,
    values: []const row_fragment.CellValue,
    comptime tag: row_fragment.ColumnKind,
) ![]const []const u8 {
    const out = try alloc.alloc([]const u8, values.len);
    errdefer alloc.free(out);
    for (values, out) |value, *slot| {
        slot.* = switch (value) {
            .null => "",
            .bytes => |bytes| if (tag == .bytes) bytes else return error.InvalidRowFragment,
            .json => |bytes| if (tag == .json) bytes else return error.InvalidRowFragment,
            else => return error.InvalidRowFragment,
        };
    }
    return out;
}

fn materializeI64Alloc(alloc: Allocator, values: []const row_fragment.CellValue) ![]const i64 {
    const out = try alloc.alloc(i64, values.len);
    errdefer alloc.free(out);
    for (values, out) |value, *slot| {
        slot.* = switch (value) {
            .null => 0,
            .i64 => |int| int,
            else => return error.InvalidRowFragment,
        };
    }
    return out;
}

fn materializeF64Alloc(alloc: Allocator, values: []const row_fragment.CellValue) ![]const f64 {
    const out = try alloc.alloc(f64, values.len);
    errdefer alloc.free(out);
    for (values, out) |value, *slot| {
        slot.* = switch (value) {
            .null => 0,
            .f64 => |float| float,
            else => return error.InvalidRowFragment,
        };
    }
    return out;
}

fn materializeBoolAlloc(alloc: Allocator, values: []const row_fragment.CellValue) ![]const bool {
    const out = try alloc.alloc(bool, values.len);
    errdefer alloc.free(out);
    for (values, out) |value, *slot| {
        slot.* = switch (value) {
            .null => false,
            .bool => |boolean| boolean,
            else => return error.InvalidRowFragment,
        };
    }
    return out;
}

fn materializeVectorF32Alloc(alloc: Allocator, values: []const row_fragment.CellValue) ![]const []const f32 {
    const out = try alloc.alloc([]const f32, values.len);
    errdefer alloc.free(out);
    for (values, out) |value, *slot| {
        slot.* = switch (value) {
            .null => &.{},
            .vector_f32 => |vector| vector,
            else => return error.InvalidRowFragment,
        };
    }
    return out;
}

fn freeColumnValues(alloc: Allocator, column: rowsource.ColumnVector) void {
    if (column.nulls.bytes.len != 0) alloc.free(column.nulls.bytes);
    freeColumnValuesOnly(alloc, column.values);
}

fn freeColumnValuesOnly(alloc: Allocator, values: rowsource.ColumnValues) void {
    switch (values) {
        .bytes => |slice| alloc.free(slice),
        .json => |slice| alloc.free(slice),
        .i64 => |slice| alloc.free(slice),
        .f64 => |slice| alloc.free(slice),
        .bool => |slice| alloc.free(slice),
        .vector_f32 => |slice| alloc.free(slice),
    }
}

test "serverless fragment source produces one typed row batch" {
    const alloc = std.testing.allocator;
    var fragment = row_fragment.Fragment{
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .row_refs = try alloc.alloc(row_fragment.RowRef, 2),
        .columns = try alloc.alloc(row_fragment.Column, 3),
    };
    defer fragment.deinit(alloc);

    fragment.row_refs[0] = .{ .key = try alloc.dupe(u8, "row:a"), .ordinal = 7 };
    fragment.row_refs[1] = .{ .key = try alloc.dupe(u8, "row:b"), .ordinal = 8 };

    fragment.columns[0] = .{
        .name = try alloc.dupe(u8, "amount"),
        .kind = .i64,
        .values = try alloc.alloc(row_fragment.CellValue, 2),
    };
    fragment.columns[0].values[0] = .{ .i64 = 10 };
    fragment.columns[0].values[1] = .{ .i64 = 20 };

    fragment.columns[1] = .{
        .name = try alloc.dupe(u8, "attrs"),
        .kind = .json,
        .values = try alloc.alloc(row_fragment.CellValue, 2),
    };
    fragment.columns[1].values[0] = .{ .json = try alloc.dupe(u8, "{\"tier\":\"pro\"}") };
    fragment.columns[1].values[1] = .null;

    fragment.columns[2] = .{
        .name = try alloc.dupe(u8, "embedding"),
        .kind = .vector_f32,
        .values = try alloc.alloc(row_fragment.CellValue, 2),
    };
    fragment.columns[2].values[0] = .{ .vector_f32 = try alloc.dupe(f32, &.{ 1.0, 0.0 }) };
    fragment.columns[2].values[1] = .{ .vector_f32 = try alloc.dupe(f32, &.{ 0.5, 0.5 }) };

    var source = FragmentSource.init(
        .{ .table_id = "orders", .snapshot_id = "manifest-1" },
        "frag-1",
        &fragment,
    );
    defer source.deinit(alloc);

    const row_source = source.rowSource();
    const batch = (try row_source.next(alloc)).?;
    try std.testing.expectEqual(rowsource.SourceKind.serverless_fragment, row_source.kind);
    try std.testing.expectEqual(@as(usize, 2), batch.rowCount());
    try std.testing.expectEqual(@as(u64, 8), batch.row_refs[1].serverless.row_ordinal);
    try std.testing.expectEqual(@as(i64, 20), batch.columns[0].values.i64[1]);
    try std.testing.expect(batch.columns[1].nulls.isNull(1));
    try std.testing.expectEqual(@as(f32, 0.5), batch.columns[2].values.vector_f32[1][0]);
    try std.testing.expectEqual(@as(f32, 0.5), batch.columns[2].values.vector_f32[1][1]);
    try std.testing.expect((try row_source.next(alloc)) == null);
}
