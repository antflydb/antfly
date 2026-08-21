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

//! Bounded replay of one projected RowSource scan. Rebuild reconciliation uses
//! this to amortize remote reads and Parquet decoding across every sidecar for
//! the same pinned source snapshot.

const std = @import("std");
const Allocator = std.mem.Allocator;
const rowsource = @import("../../storage/rowsource/types.zig");
const source_binding = @import("../segment/source_binding.zig");
const lake_build_limits = @import("lake_build_limits.zig");

pub const Buffer = struct {
    kind: rowsource.SourceKind,
    batches: []OwnedBatch,

    pub fn captureAlloc(alloc: Allocator, source: rowsource.Source, limits: lake_build_limits.Limits) !Buffer {
        var working_set = try lake_build_limits.WorkingSetAllocator.init(alloc, limits);
        return captureBoundedAlloc(working_set.allocator(), source, limits) catch |err| {
            if (err == error.OutOfMemory and working_set.limit_exceeded) return error.LakeSidecarReplayBudgetExceeded;
            return err;
        };
    }

    fn captureBoundedAlloc(alloc: Allocator, source: rowsource.Source, limits: lake_build_limits.Limits) !Buffer {
        var budget = try lake_build_limits.Budget.init(limits);
        var batches = std.ArrayListUnmanaged(OwnedBatch).empty;
        errdefer {
            for (batches.items) |*batch| batch.deinit(alloc);
            batches.deinit(alloc);
        }
        var replay_bytes: usize = 0;
        while (try source.next(alloc)) |batch| {
            try budget.admitBatch(batch);
            replay_bytes = std.math.add(usize, replay_bytes, lake_build_limits.estimateBatchBytes(batch)) catch
                return error.LakeSidecarReplayBudgetExceeded;
            if (replay_bytes > limits.max_replay_bytes) return error.LakeSidecarReplayBudgetExceeded;
            try batches.ensureUnusedCapacity(alloc, 1);
            batches.appendAssumeCapacity(try OwnedBatch.cloneAlloc(alloc, batch));
        }
        return .{ .kind = source.kind, .batches = try batches.toOwnedSlice(alloc) };
    }

    pub fn deinit(self: *Buffer, alloc: Allocator) void {
        for (self.batches) |*batch| batch.deinit(alloc);
        alloc.free(self.batches);
        self.* = undefined;
    }

    pub fn cursor(self: *const Buffer) Cursor {
        return .{ .buffer = self };
    }
};

pub const Cursor = struct {
    buffer: *const Buffer,
    next_index: usize = 0,

    pub fn rowSource(self: *Cursor) rowsource.Source {
        return .{ .kind = self.buffer.kind, .ctx = self, .next_batch = nextBatch };
    }

    fn nextBatch(ptr: *anyopaque, _: Allocator) !?rowsource.ColumnBatch {
        const self: *Cursor = @ptrCast(@alignCast(ptr));
        if (self.next_index >= self.buffer.batches.len) return null;
        const batch = self.buffer.batches[self.next_index].batch;
        self.next_index += 1;
        return batch;
    }
};

const OwnedBatch = struct {
    batch: rowsource.ColumnBatch,

    fn cloneAlloc(alloc: Allocator, batch: rowsource.ColumnBatch) !OwnedBatch {
        const table_id = try alloc.dupe(u8, batch.snapshot.table_id);
        errdefer alloc.free(table_id);
        const snapshot_id = try alloc.dupe(u8, batch.snapshot.snapshot_id);
        errdefer alloc.free(snapshot_id);

        const row_refs = try alloc.alloc(rowsource.RowRef, batch.row_refs.len);
        errdefer alloc.free(row_refs);
        var initialized_refs: usize = 0;
        errdefer for (row_refs[0..initialized_refs]) |*row_ref| source_binding.freeOwnedRowRef(alloc, row_ref.*);
        for (batch.row_refs, row_refs) |row_ref, *out| {
            out.* = try source_binding.cloneRowRefAlloc(alloc, row_ref);
            initialized_refs += 1;
        }

        const columns = try alloc.alloc(rowsource.ColumnVector, batch.columns.len);
        errdefer alloc.free(columns);
        var initialized_columns: usize = 0;
        errdefer for (columns[0..initialized_columns]) |*column| freeColumn(alloc, column.*);
        for (batch.columns, columns) |column, *out| {
            out.* = try cloneColumnAlloc(alloc, column);
            initialized_columns += 1;
        }

        const owned = OwnedBatch{ .batch = .{
            .snapshot = .{ .table_id = table_id, .snapshot_id = snapshot_id, .generation = batch.snapshot.generation },
            .row_refs = row_refs,
            .columns = columns,
        } };
        try owned.batch.validate();
        return owned;
    }

    fn deinit(self: *OwnedBatch, alloc: Allocator) void {
        alloc.free(@constCast(self.batch.snapshot.table_id));
        alloc.free(@constCast(self.batch.snapshot.snapshot_id));
        for (self.batch.row_refs) |row_ref| source_binding.freeOwnedRowRef(alloc, row_ref);
        alloc.free(@constCast(self.batch.row_refs));
        for (self.batch.columns) |column| freeColumn(alloc, column);
        alloc.free(@constCast(self.batch.columns));
        self.* = undefined;
    }
};

fn cloneColumnAlloc(alloc: Allocator, column: rowsource.ColumnVector) !rowsource.ColumnVector {
    const name = try alloc.dupe(u8, column.name);
    errdefer alloc.free(name);
    const nulls = try alloc.dupe(u8, column.nulls.bytes);
    errdefer alloc.free(nulls);
    return .{
        .name = name,
        .values = try cloneColumnValuesAlloc(alloc, column.values),
        .nulls = .{ .bytes = nulls },
    };
}

fn cloneColumnValuesAlloc(alloc: Allocator, values: rowsource.ColumnValues) !rowsource.ColumnValues {
    return switch (values) {
        .bytes => |items| .{ .bytes = try cloneByteSlicesAlloc(alloc, items) },
        .json => |items| .{ .json = try cloneByteSlicesAlloc(alloc, items) },
        .i64 => |items| .{ .i64 = try alloc.dupe(i64, items) },
        .f64 => |items| .{ .f64 = try alloc.dupe(f64, items) },
        .bool => |items| .{ .bool = try alloc.dupe(bool, items) },
        .vector_f32 => |items| .{ .vector_f32 = try cloneVectorSlicesAlloc(alloc, items) },
    };
}

fn cloneByteSlicesAlloc(alloc: Allocator, items: []const []const u8) ![][]const u8 {
    const out = try alloc.alloc([]const u8, items.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer for (out[0..initialized]) |item| alloc.free(@constCast(item));
    for (items, out) |item, *copy| {
        copy.* = try alloc.dupe(u8, item);
        initialized += 1;
    }
    return out;
}

fn cloneVectorSlicesAlloc(alloc: Allocator, items: []const []const f32) ![][]const f32 {
    const out = try alloc.alloc([]const f32, items.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer for (out[0..initialized]) |item| alloc.free(@constCast(item));
    for (items, out) |item, *copy| {
        copy.* = try alloc.dupe(f32, item);
        initialized += 1;
    }
    return out;
}

fn freeColumn(alloc: Allocator, column: rowsource.ColumnVector) void {
    alloc.free(@constCast(column.name));
    alloc.free(@constCast(column.nulls.bytes));
    switch (column.values) {
        .bytes, .json => |items| {
            for (items) |item| alloc.free(@constCast(item));
            alloc.free(@constCast(items));
        },
        .i64 => |items| alloc.free(@constCast(items)),
        .f64 => |items| alloc.free(@constCast(items)),
        .bool => |items| alloc.free(@constCast(items)),
        .vector_f32 => |items| {
            for (items) |item| alloc.free(@constCast(item));
            alloc.free(@constCast(items));
        },
    }
}

test "bounded replay scans a source once and supports independent cursors" {
    const alloc = std.testing.allocator;
    const refs = [_]rowsource.RowRef{.{ .relational_key = "a" }};
    const values = [_]i64{7};
    const columns = [_]rowsource.ColumnVector{.{ .name = "n", .values = .{ .i64 = &values } }};
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = .{ .table_id = "t", .snapshot_id = "s" },
        .row_refs = &refs,
        .columns = &columns,
    }};
    const TestSource = struct {
        batches: []const rowsource.ColumnBatch,
        index: usize = 0,
        fn next(ptr: *anyopaque, _: Allocator) !?rowsource.ColumnBatch {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.index >= self.batches.len) return null;
            defer self.index += 1;
            return self.batches[self.index];
        }
    };
    var state = TestSource{ .batches = &batches };
    var replay = try Buffer.captureAlloc(alloc, .{ .kind = .relational_store, .ctx = &state, .next_batch = TestSource.next }, .{});
    defer replay.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), state.index);
    var first = replay.cursor();
    var second = replay.cursor();
    try std.testing.expectEqual(@as(i64, 7), (try first.rowSource().next(alloc)).?.columns[0].values.i64[0]);
    try std.testing.expectEqual(@as(i64, 7), (try second.rowSource().next(alloc)).?.columns[0].values.i64[0]);

    var bounded_state = TestSource{ .batches = &batches };
    try std.testing.expectError(error.LakeSidecarReplayBudgetExceeded, Buffer.captureAlloc(
        alloc,
        .{ .kind = .relational_store, .ctx = &bounded_state, .next_batch = TestSource.next },
        .{ .max_replay_bytes = lake_build_limits.estimateBatchBytes(batches[0]) - 1 },
    ));

    var working_set_bounded_state = TestSource{ .batches = &batches };
    try std.testing.expectError(error.LakeSidecarReplayBudgetExceeded, Buffer.captureAlloc(
        alloc,
        .{ .kind = .relational_store, .ctx = &working_set_bounded_state, .next_batch = TestSource.next },
        .{ .max_working_set_bytes = 1 },
    ));

    const AllocationRunner = struct {
        fn run(a: Allocator, test_batches: []const rowsource.ColumnBatch) !void {
            var test_state = TestSource{ .batches = test_batches };
            var test_replay = try Buffer.captureAlloc(
                a,
                .{ .kind = .relational_store, .ctx = &test_state, .next_batch = TestSource.next },
                .{},
            );
            defer test_replay.deinit(a);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, AllocationRunner.run, .{batches[0..]});
}
