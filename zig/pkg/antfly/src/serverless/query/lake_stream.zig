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

//! Pull-based typed lake scans. Only one file's footer metadata, delete refs
//! and one decoded row group are retained. Opening never reads data pages.
const std = @import("std");
const external = @import("../external_source/types.zig");
const parquet = @import("lake_parquet_rowgroup.zig");
const iceberg = @import("lake_iceberg_snapshot.zig");
const serving = @import("lake_serving.zig");
const types = @import("../../storage/rowsource/types.zig");
const identity = @import("../../storage/rowsource/identity.zig");
const source_binding = @import("../segment/source_binding.zig");
const Context = @import("lake_read_context.zig").Context;
const Allocator = std.mem.Allocator;

/// Conservative pruning contract. Unsupported physical comparisons remain
/// residuals; no approximate numeric conversion can exclude matching rows.
pub const Predicate = struct {
    pub const Op = enum { eq, neq, lt, lte, gt, gte };
    column: []const u8,
    op: Op,
    value: union(enum) { integer: i64, bytes: []const u8, boolean: bool },
};
pub const Limits = struct {
    max_examined_rows: u64 = 100_000_000,
    max_decoded_bytes: usize = 32 * 1024 * 1024,
    max_input_bytes: usize = 32 * 1024 * 1024,
    max_row_group_rows: usize = 1_000_000,
};
pub const Stats = struct {
    files_opened: usize = 0,
    files_pruned: usize = 0,
    groups_decoded: usize = 0,
    groups_pruned: usize = 0,
    rows_examined: u64 = 0,
};
pub const Stream = struct {
    alloc: Allocator,
    source: *serving.ServingSource,
    columns: []const []const u8,
    predicates: []const Predicate,
    context: Context,
    limits: Limits,
    stats: Stats = .{},
    files: []usize,
    file_index: usize = 0,
    discovered: ?parquet.DiscoveredObjectRangeRowGroupPlan = null,
    group_index: usize = 0,
    current: ?parquet.OwnedBatch = null,
    deleted: []types.RowRef = &.{},

    pub fn init(alloc: Allocator, source: *serving.ServingSource, columns: []const []const u8, predicates: []const Predicate, context: Context, limits: Limits) !Stream {
        try context.ensureActive();
        try source.inventory.validate();
        const files = try alloc.alloc(usize, source.inventory.files.len);
        for (files, 0..) |*index, i| index.* = i;
        std.mem.sort(usize, files, source.inventory, struct {
            fn less(inventory: external.Inventory, a: usize, b: usize) bool {
                const left = identity.fileDigest(inventory.source_id, inventory.snapshot_id, inventory.files[a].file_id);
                const right = identity.fileDigest(inventory.source_id, inventory.snapshot_id, inventory.files[b].file_id);
                return std.mem.order(u8, &left, &right) == .lt;
            }
        }.less);
        return .{ .alloc = alloc, .source = source, .columns = columns, .predicates = predicates, .context = context, .limits = limits, .files = files };
    }
    pub fn deinit(self: *Stream) void {
        self.clearFile();
        self.alloc.free(self.files);
        self.* = undefined;
    }
    fn clearBatch(self: *Stream) void {
        if (self.current) |*batch| batch.deinit(self.alloc);
        self.current = null;
    }
    fn clearFile(self: *Stream) void {
        self.clearBatch();
        if (self.deleted.len != 0) self.alloc.free(self.deleted);
        self.deleted = &.{};
        if (self.discovered) |*plan| plan.deinit(self.alloc);
        self.discovered = null;
        self.group_index = 0;
    }
    /// The returned vectors remain valid until the next pull or close.
    pub fn next(self: *Stream) !?types.ColumnBatch {
        try self.context.ensureActive();
        self.clearBatch();
        while (true) {
            if (self.discovered) |*plan| {
                while (self.group_index < plan.row_group_plan.row_groups.len) {
                    const input = plan.row_group_plan.row_groups[self.group_index];
                    self.group_index += 1;
                    const group = for (plan.inventory.files[0].row_groups) |candidate| {
                        if (candidate.ordinal == input.row_group_ordinal) break candidate;
                    } else return error.InvalidParquetRowGroupBatch;
                    if (!groupMayMatch(group, self.predicates)) {
                        self.stats.groups_pruned += 1;
                        continue;
                    }
                    if (group.row_count > self.limits.max_examined_rows -| self.stats.rows_examined) return error.LakeRowsScanBudgetExceeded;
                    self.current = parquet.buildSupportedI64RowGroupBatchFromMaybeCachedCoalescedObjectRangeReaderAlloc(self.alloc, self.source.scanner.reader(), self.source.scanner.cache, plan.inventory, input.file_id, input.row_group_ordinal, self.columns, self.source.scanner.coalesce_options, .{
                        .max_rows = self.limits.max_row_group_rows,
                        .max_struct_allocation_bytes = self.limits.max_decoded_bytes,
                        .max_input_bytes = self.limits.max_input_bytes,
                        .max_decoded_bytes = self.limits.max_decoded_bytes,
                    }) catch |err| return serving.normalizeLakeFooterDiscoveryError(err);
                    self.stats.groups_decoded += 1;
                    self.stats.rows_examined += group.row_count;
                    try self.context.ensureActive();
                    return self.current.?.batch;
                }
                self.clearFile();
            }
            if (self.file_index == self.files.len) return null;
            const index = self.files[self.file_index];
            self.file_index += 1;
            const file = self.source.inventory.files[index];
            if (!fileMayMatch(file, self.predicates)) {
                self.stats.files_pruned += 1;
                continue;
            }
            try self.loadFile(index);
        }
    }
    fn loadFile(self: *Stream, index: usize) !void {
        var inventory = self.source.inventory;
        inventory.files = self.source.inventory.files[index..][0..1];
        inventory.deleted_row_groups = &.{};
        self.discovered = if (self.source.scanner.cache) |cache|
            try parquet.discoverSupportedI64ObjectRangeRowGroupsFromCachedFootersAlloc(self.alloc, self.source.scanner.reader(), cache, inventory, self.columns, 64 * 1024)
        else
            try parquet.discoverSupportedI64ObjectRangeRowGroupsFromFootersAlloc(self.alloc, self.source.scanner.reader(), inventory, self.columns, 64 * 1024);
        self.stats.files_opened += 1;
        std.mem.sort(parquet.ObjectRangeRowGroupInput, self.discovered.?.row_group_plan.row_groups, {}, struct {
            fn less(_: void, a: parquet.ObjectRangeRowGroupInput, b: parquet.ObjectRangeRowGroupInput) bool {
                return a.row_group_ordinal < b.row_group_ordinal;
            }
        }.less);
        if (self.source.scanner.iceberg_delete_plan) |delete_plan| {
            self.deleted = try iceberg.readDeleteRowRefsAlloc(self.alloc, .{
                .reader = self.source.scanner.reader(),
                .client = self.source.scanner.object_reader.client,
                .cache = self.source.scanner.cache,
                .data_inventory = self.discovered.?.inventory,
                .delete_plan = delete_plan,
                .coalesce_options = self.source.scanner.coalesce_options,
                .materialization_limits = .{ .max_struct_allocation_bytes = self.limits.max_decoded_bytes, .max_input_bytes = self.limits.max_input_bytes, .max_decoded_bytes = self.limits.max_decoded_bytes },
            });
        }
    }
    pub fn countAll(self: *Stream) !?u64 {
        if (self.file_index != 0 or self.predicates.len != 0) return null;
        var total: u64 = 0;
        for (self.files) |index| {
            try self.context.ensureActive();
            try self.loadFile(index);
            total = std.math.add(u64, total, self.discovered.?.inventory.files[0].row_count) catch return error.LakeRowsScanBudgetExceeded;
            self.clearFile();
        }
        self.file_index = self.files.len;
        return total;
    }
    pub fn isDeleted(self: Stream, ref: types.RowRef) bool {
        var low: usize = 0;
        var high: usize = self.deleted.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            if (iceberg.externalRowRefLessThan({}, self.deleted[mid], ref)) low = mid + 1 else high = mid;
        }
        if (low < self.deleted.len and source_binding.rowRefsEqual(self.deleted[low], ref)) return true;
        if (ref == .external) {
            const ordinal = ref.external.row_ordinal;
            for (self.source.inventory.deleted_row_groups) |vector| if (std.mem.eql(u8, vector.file_id, ref.external.file_id) and vector.row_group_ordinal == ref.external.row_group_ordinal) {
                var begin: usize = 0;
                var end: usize = vector.row_ordinals.len;
                while (begin < end) {
                    const mid = begin + (end - begin) / 2;
                    if (vector.row_ordinals[mid] < ordinal) begin = mid + 1 else end = mid;
                }
                if (begin < vector.row_ordinals.len and vector.row_ordinals[begin] == ordinal) return true;
            };
        }
        return false;
    }
};
fn rangeMayMatch(comptime T: type, min: T, max: T, value: T, op: Predicate.Op) bool {
    const low = if (T == []const u8) std.mem.order(u8, min, value) else std.math.order(min, value);
    const high = if (T == []const u8) std.mem.order(u8, max, value) else std.math.order(max, value);
    return switch (op) {
        .eq => low != .gt and high != .lt,
        .neq => low != .eq or high != .eq,
        .lt => low == .lt,
        .lte => low != .gt,
        .gt => high == .gt,
        .gte => high != .lt,
    };
}
pub fn groupMayMatch(group: external.RowGroup, predicates: []const Predicate) bool {
    for (predicates) |predicate| for (group.column_chunks) |chunk| {
        if (!std.mem.eql(u8, chunk.column_id, predicate.column)) continue;
        // Decimal/timestamp annotations change comparison semantics relative
        // to their physical statistics. Keep those predicates residual.
        if (chunk.logical_type.len != 0) continue;
        const matches = switch (predicate.value) {
            .integer => |value| if (chunk.stats_min_i64) |min| if (chunk.stats_max_i64) |max| rangeMayMatch(i64, min, max, value, predicate.op) else true else true,
            .bytes => |value| if (chunk.stats_min_bytes) |min| if (chunk.stats_max_bytes) |max| rangeMayMatch([]const u8, min, max, value, predicate.op) else true else true,
            .boolean => |value| if (chunk.stats_min_bool) |min| if (chunk.stats_max_bool) |max| rangeMayMatch(u8, @intFromBool(min), @intFromBool(max), @intFromBool(value), predicate.op) else true else true,
        };
        if (!matches) return false;
    };
    return true;
}
pub fn fileMayMatch(file: external.FileEntry, predicates: []const Predicate) bool {
    if (file.row_groups.len == 0) return true;
    for (file.row_groups) |group| if (groupMayMatch(group, predicates)) return true;
    return false;
}

test "external lake pruning preserves exact integers and unknown annotated statistics" {
    var column: external.ColumnChunk = .{ .column_id = @constCast("n"), .file_offset = 4, .compressed_len = 8, .stats_min_i64 = 9007199254740993, .stats_max_i64 = 9007199254740993 };
    var group: external.RowGroup = .{ .ordinal = 0, .row_count = 1, .column_chunks = @as([*]external.ColumnChunk, @ptrCast(&column))[0..1] };
    try std.testing.expect(groupMayMatch(group, &.{.{ .column = "n", .op = .gt, .value = .{ .integer = 9007199254740992 } }}));
    try std.testing.expect(!groupMayMatch(group, &.{.{ .column = "n", .op = .eq, .value = .{ .integer = 9007199254740992 } }}));
    column.stats_max_i64 = null;
    try std.testing.expect(groupMayMatch(group, &.{.{ .column = "n", .op = .eq, .value = .{ .integer = 0 } }}));
    column.stats_max_i64 = 9007199254740993;
    column.logical_type = @constCast("decimal");
    try std.testing.expect(groupMayMatch(group, &.{.{ .column = "n", .op = .eq, .value = .{ .integer = 0 } }}));
    group.column_chunks = &.{};
    try std.testing.expect(groupMayMatch(group, &.{.{ .column = "n", .op = .eq, .value = .{ .integer = 0 } }}));
}
