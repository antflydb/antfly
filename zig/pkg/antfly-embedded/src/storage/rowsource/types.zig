// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

/// A consumed immutable column owner. Views may copy descriptors and names,
/// while this lease keeps their payloads alive across cursor pulls and closure.
pub const ColumnOwner = struct {
    ptr: *anyopaque,
    release_fn: *const fn (*anyopaque) void,
    retained_bytes: usize,
    pub fn release(self: @This()) void {
        self.release_fn(self.ptr);
    }
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
    dictionary_bytes = 7,
    dictionary_i64 = 8,
    dictionary_f64 = 9,

    pub fn logical(self: ColumnKind) ColumnKind {
        return switch (self) {
            .dictionary_bytes => .bytes,
            .dictionary_i64 => .i64,
            .dictionary_f64 => .f64,
            else => self,
        };
    }
};

/// Compact native scan values. IDs are page-local; semantic consumers compare
/// values, never IDs from different dictionaries. Ownership follows the batch.
pub const DictionaryBytes = struct {
    values: []const []const u8,
    indices: []const u32,
    pub fn at(self: DictionaryBytes, row: usize) []const u8 {
        return self.values[self.indices[row]];
    }
    pub fn deinit(self: DictionaryBytes, a: Allocator) void {
        for (self.values) |value| a.free(value);
        a.free(self.values);
        a.free(self.indices);
    }
};

pub fn DictionaryNumeric(comptime T: type) type {
    return struct {
        values: []const T,
        indices: []const u32,
        /// Decoded pages own their indices; dictionary values may be borrowed
        /// from a cursor or retained cache dependency. Batch views are borrowed.
        owned: bool = false,
        pub fn at(self: @This(), row: usize) T {
            return self.values[self.indices[row]];
        }
        pub fn deinit(self: @This(), a: Allocator) void {
            if (self.owned) a.free(self.values);
            a.free(self.indices);
        }
    };
}

pub const ColumnValues = union(ColumnKind) {
    bytes: []const []const u8,
    json: []const []const u8,
    i64: []const i64,
    f64: []const f64,
    bool: []const bool,
    vector_f32: []const []const f32,
    dictionary_bytes: DictionaryBytes,
    dictionary_i64: DictionaryNumeric(i64),
    dictionary_f64: DictionaryNumeric(f64),
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

    pub fn integerAt(self: ColumnVector, row: usize) !i64 {
        return switch (self.values) {
            .i64 => |values| values[row],
            .dictionary_i64 => |values| values.at(row),
            else => error.RowSourceColumnKindMismatch,
        };
    }

    pub fn bytesAt(self: ColumnVector, row: usize) ![]const u8 {
        return switch (self.values) {
            .bytes => |values| values[row],
            .dictionary_bytes => |values| values.values[values.indices[row]],
            else => error.RowSourceColumnKindMismatch,
        };
    }

    /// Dictionary identity is local to this column's retained page. Null slots
    /// do not reference an entry and may contain an arbitrary physical id.
    pub fn dictionaryId(self: ColumnVector, row: usize) !?u32 {
        if (row >= self.rowCount()) return error.RowSourceColumnLengthMismatch;
        if (self.nulls.isNull(row)) return null;
        return switch (self.values) {
            inline .dictionary_bytes, .dictionary_i64, .dictionary_f64 => |dictionary| blk: {
                const id = dictionary.indices[row];
                if (id >= dictionary.values.len) return error.RowSourceDictionaryIndexOutOfBounds;
                break :blk id;
            },
            else => null,
        };
    }

    pub fn rowCount(self: ColumnVector) usize {
        return switch (self.values) {
            .bytes => |values| values.len,
            inline .dictionary_bytes, .dictionary_i64, .dictionary_f64 => |values| values.indices.len,
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
            if (column.values == .dictionary_bytes) {
                const dictionary = column.values.dictionary_bytes;
                for (dictionary.indices, 0..) |id, index| if (!column.nulls.isNull(index) and id >= dictionary.values.len) return error.RowSourceDictionaryIndexOutOfBounds;
            }
            switch (column.values) {
                inline .dictionary_i64, .dictionary_f64 => |dictionary| for (dictionary.indices, 0..) |id, index| {
                    if (!column.nulls.isNull(index) and id >= dictionary.values.len) return error.RowSourceDictionaryIndexOutOfBounds;
                },
                else => {},
            }
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
