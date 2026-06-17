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

//! Shared row-source batch types for Antfly-owned and external lake-native
//! execution. These are view types; concrete sources own the backing memory.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const SourceKind = enum {
    relational_store,
    json_materialized,
    serverless_fragment,
    external_parquet,
    external_iceberg,
    external_lance,
};

pub const NextBatchFn = *const fn (ctx: *anyopaque, alloc: Allocator) anyerror!?ColumnBatch;
pub const DeinitFn = *const fn (ctx: *anyopaque, alloc: Allocator) void;

pub const Source = struct {
    kind: SourceKind,
    ctx: *anyopaque,
    next_batch: NextBatchFn,
    deinit_fn: ?DeinitFn = null,

    pub fn next(self: Source, alloc: Allocator) !?ColumnBatch {
        const batch = try self.next_batch(self.ctx, alloc);
        if (batch) |got| try got.validate();
        return batch;
    }

    pub fn deinit(self: Source, alloc: Allocator) void {
        if (self.deinit_fn) |deinit_fn| deinit_fn(self.ctx, alloc);
    }
};

pub const SnapshotRef = struct {
    table_id: []const u8,
    snapshot_id: []const u8,
    generation: u64 = 0,
};

pub const ServerlessRowRef = struct {
    fragment_id: []const u8,
    row_ordinal: u64,
};

pub const ExternalRowRef = struct {
    source_id: []const u8,
    snapshot_id: []const u8,
    file_id: []const u8,
    row_group_ordinal: u32,
    row_ordinal: u64,
};

pub const RowRef = union(enum) {
    relational_key: []const u8,
    serverless: ServerlessRowRef,
    external: ExternalRowRef,
};

pub const ColumnKind = enum(u8) {
    bytes = 1,
    json = 2,
    i64 = 3,
    f64 = 4,
    bool = 5,
    vector_f32 = 6,
};

pub const ColumnValues = union(ColumnKind) {
    bytes: []const []const u8,
    json: []const []const u8,
    i64: []const i64,
    f64: []const f64,
    bool: []const bool,
    vector_f32: []const []const f32,
};

pub const NullBitmap = struct {
    /// One byte per row for now. Zero means present, non-zero means null.
    bytes: []const u8 = &.{},

    pub fn isNull(self: NullBitmap, row: usize) bool {
        return self.bytes.len > row and self.bytes[row] != 0;
    }
};

pub const ColumnVector = struct {
    name: []const u8,
    values: ColumnValues,
    nulls: NullBitmap = .{},

    pub fn kind(self: ColumnVector) ColumnKind {
        return std.meta.activeTag(self.values);
    }

    pub fn rowCount(self: ColumnVector) usize {
        return switch (self.values) {
            .bytes => |values| values.len,
            .json => |values| values.len,
            .i64 => |values| values.len,
            .f64 => |values| values.len,
            .bool => |values| values.len,
            .vector_f32 => |values| values.len,
        };
    }
};

pub const ColumnBatch = struct {
    snapshot: SnapshotRef,
    row_refs: []const RowRef,
    columns: []const ColumnVector,

    pub fn rowCount(self: ColumnBatch) usize {
        return self.row_refs.len;
    }

    pub fn validate(self: ColumnBatch) !void {
        for (self.columns) |column| {
            if (column.rowCount() != self.row_refs.len) return error.RowSourceColumnLengthMismatch;
            if (column.nulls.bytes.len != 0 and column.nulls.bytes.len != self.row_refs.len) {
                return error.RowSourceNullBitmapLengthMismatch;
            }
        }
    }

    pub fn findColumn(self: ColumnBatch, name: []const u8) ?ColumnVector {
        for (self.columns) |column| {
            if (std.mem.eql(u8, column.name, name)) return column;
        }
        return null;
    }
};

test "column batch validates vector lengths and lookup" {
    const row_refs = [_]RowRef{
        .{ .relational_key = "row:a" },
        .{ .relational_key = "row:b" },
    };
    const values = [_]i64{ 10, 20 };
    const columns = [_]ColumnVector{
        .{ .name = "amount", .values = .{ .i64 = &values } },
    };
    const batch = ColumnBatch{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "snap-1", .generation = 1 },
        .row_refs = &row_refs,
        .columns = &columns,
    };
    try batch.validate();
    try std.testing.expectEqual(@as(usize, 2), batch.rowCount());
    try std.testing.expect(batch.findColumn("amount") != null);
    try std.testing.expect(batch.findColumn("missing") == null);
}

test "column batch rejects mismatched column length" {
    const row_refs = [_]RowRef{
        .{ .relational_key = "row:a" },
        .{ .relational_key = "row:b" },
    };
    const values = [_]i64{10};
    const columns = [_]ColumnVector{
        .{ .name = "amount", .values = .{ .i64 = &values } },
    };
    const batch = ColumnBatch{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "snap-1" },
        .row_refs = &row_refs,
        .columns = &columns,
    };
    try std.testing.expectError(error.RowSourceColumnLengthMismatch, batch.validate());
}

test "row source validates batches returned by adapters" {
    const TestSource = struct {
        emitted: bool = false,

        fn next(ctx: *anyopaque, alloc: Allocator) !?ColumnBatch {
            _ = alloc;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (self.emitted) return null;
            self.emitted = true;
            const row_refs = &[_]RowRef{
                .{ .relational_key = "row:a" },
            };
            const values = &[_]bool{true};
            const columns = &[_]ColumnVector{
                .{ .name = "active", .values = .{ .bool = values } },
            };
            return ColumnBatch{
                .snapshot = .{ .table_id = "users", .snapshot_id = "snap-1" },
                .row_refs = row_refs,
                .columns = columns,
            };
        }
    };

    var state = TestSource{};
    const source = Source{
        .kind = .relational_store,
        .ctx = &state,
        .next_batch = TestSource.next,
    };
    const batch = (try source.next(std.testing.allocator)).?;
    try std.testing.expectEqual(@as(usize, 1), batch.rowCount());
    try std.testing.expect((try source.next(std.testing.allocator)) == null);
}
