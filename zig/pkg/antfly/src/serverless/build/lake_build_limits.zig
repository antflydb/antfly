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

//! Total-work admission for lake sidecar builds and their replay buffers.

const std = @import("std");
const rowsource = @import("../../storage/rowsource/types.zig");

pub const Limits = struct {
    max_batches: usize = 100_000,
    max_rows: u64 = 10_000_000,
    max_input_bytes: usize = 512 * 1024 * 1024,
    max_retained_items: usize = 20_000_000,
    max_output_bytes: usize = 512 * 1024 * 1024,
    max_replay_bytes: usize = 512 * 1024 * 1024,
    /// Hard cap for live allocations made while building one sidecar or replay
    /// buffer. This is deliberately separate from logical input/output limits:
    /// tokenization and index construction can amplify compact source batches.
    max_working_set_bytes: usize = 1024 * 1024 * 1024,

    pub fn validate(self: Limits) !void {
        if (self.max_batches == 0 or self.max_rows == 0 or self.max_input_bytes == 0 or
            self.max_retained_items == 0 or self.max_output_bytes == 0 or self.max_replay_bytes == 0 or
            self.max_working_set_bytes == 0)
        {
            return error.InvalidLakeSidecarBuildLimits;
        }
    }
};

/// Allocator wrapper that turns the build working-set limit into allocation
/// admission rather than a post-allocation observation. Callers can distinguish
/// a configured-limit denial from an actual backing-allocator OOM through
/// `limit_exceeded` and preserve a stable user-facing budget error.
pub const WorkingSetAllocator = struct {
    backing: std.mem.Allocator,
    live_bytes: usize = 0,
    max_live_bytes: usize,
    limit_exceeded: bool = false,

    pub fn init(backing: std.mem.Allocator, limits: Limits) !WorkingSetAllocator {
        try limits.validate();
        return .{ .backing = backing, .max_live_bytes = limits.max_working_set_bytes };
    }

    pub fn allocator(self: *WorkingSetAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn permitsGrowth(self: *WorkingSetAllocator, additional_bytes: usize) bool {
        if (additional_bytes <= self.max_live_bytes -| self.live_bytes) return true;
        self.limit_exceeded = true;
        return false;
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *WorkingSetAllocator = @ptrCast(@alignCast(ctx));
        if (!self.permitsGrowth(len)) return null;
        const memory = self.backing.rawAlloc(len, alignment, ret_addr) orelse return null;
        self.live_bytes += len;
        return memory;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *WorkingSetAllocator = @ptrCast(@alignCast(ctx));
        const growth = new_len -| memory.len;
        if (growth > 0 and !self.permitsGrowth(growth)) return false;
        if (!self.backing.rawResize(memory, alignment, new_len, ret_addr)) return false;
        self.live_bytes = self.live_bytes -| memory.len +| new_len;
        return true;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *WorkingSetAllocator = @ptrCast(@alignCast(ctx));
        const growth = new_len -| memory.len;
        if (growth > 0 and !self.permitsGrowth(growth)) return null;
        const remapped = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        self.live_bytes = self.live_bytes -| memory.len +| new_len;
        return remapped;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *WorkingSetAllocator = @ptrCast(@alignCast(ctx));
        self.backing.rawFree(memory, alignment, ret_addr);
        self.live_bytes -|= memory.len;
    }

    const vtable = std.mem.Allocator.VTable{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };
};

pub const Budget = struct {
    limits: Limits,
    batches: usize = 0,
    rows: u64 = 0,
    input_bytes: usize = 0,

    pub fn init(limits: Limits) !Budget {
        try limits.validate();
        return .{ .limits = limits };
    }

    pub fn admitBatch(self: *Budget, batch: rowsource.ColumnBatch) !void {
        self.batches = std.math.add(usize, self.batches, 1) catch return error.LakeSidecarBuildBudgetExceeded;
        if (self.batches > self.limits.max_batches) return error.LakeSidecarBuildBudgetExceeded;
        self.rows = std.math.add(u64, self.rows, batch.rowCount()) catch return error.LakeSidecarBuildBudgetExceeded;
        if (self.rows > self.limits.max_rows) return error.LakeSidecarBuildBudgetExceeded;
        self.input_bytes = std.math.add(usize, self.input_bytes, estimateBatchBytes(batch)) catch
            return error.LakeSidecarBuildBudgetExceeded;
        if (self.input_bytes > self.limits.max_input_bytes) return error.LakeSidecarBuildBudgetExceeded;
    }

    pub fn checkRetainedItems(self: Budget, count: usize) !void {
        if (count > self.limits.max_retained_items) return error.LakeSidecarBuildBudgetExceeded;
    }

    pub fn checkOutputBytes(self: Budget, byte_len: usize) !void {
        if (byte_len > self.limits.max_output_bytes) return error.LakeSidecarBuildBudgetExceeded;
    }
};

pub fn estimateBatchBytes(batch: rowsource.ColumnBatch) usize {
    var total: usize = @sizeOf(rowsource.ColumnBatch);
    total = addOrMax(total, batch.snapshot.table_id.len);
    total = addOrMax(total, batch.snapshot.snapshot_id.len);
    total = addOrMax(total, std.math.mul(usize, batch.row_refs.len, @sizeOf(rowsource.RowRef)) catch return std.math.maxInt(usize));
    total = addOrMax(total, std.math.mul(usize, batch.columns.len, @sizeOf(rowsource.ColumnVector)) catch return std.math.maxInt(usize));
    for (batch.row_refs) |row_ref| {
        const ref_bytes = switch (row_ref) {
            .relational_key => |key| key.len,
            .serverless => |value| value.fragment_id.len,
            .external => |value| addOrMax(addOrMax(value.source_id.len, value.snapshot_id.len), value.file_id.len),
        };
        total = addOrMax(total, ref_bytes);
    }
    for (batch.columns) |column| {
        total = addOrMax(total, addOrMax(column.name.len, column.nulls.bytes.len));
        const value_bytes = switch (column.values) {
            .bytes, .json => |values| blk: {
                var bytes = std.math.mul(usize, values.len, @sizeOf([]const u8)) catch break :blk std.math.maxInt(usize);
                for (values) |value| bytes = std.math.add(usize, bytes, value.len) catch break :blk std.math.maxInt(usize);
                break :blk bytes;
            },
            .i64 => |values| std.math.mul(usize, values.len, @sizeOf(i64)) catch std.math.maxInt(usize),
            .f64 => |values| std.math.mul(usize, values.len, @sizeOf(f64)) catch std.math.maxInt(usize),
            .bool => |values| std.math.mul(usize, values.len, @sizeOf(bool)) catch std.math.maxInt(usize),
            .vector_f32 => |values| blk: {
                var bytes = std.math.mul(usize, values.len, @sizeOf([]const f32)) catch break :blk std.math.maxInt(usize);
                for (values) |value| {
                    const item_bytes = std.math.mul(usize, value.len, @sizeOf(f32)) catch break :blk std.math.maxInt(usize);
                    bytes = std.math.add(usize, bytes, item_bytes) catch break :blk std.math.maxInt(usize);
                }
                break :blk bytes;
            },
        };
        total = addOrMax(total, value_bytes);
    }
    return total;
}

fn addOrMax(left: usize, right: usize) usize {
    return std.math.add(usize, left, right) catch std.math.maxInt(usize);
}

test "lake build budget is cumulative across batches" {
    const refs = [_]rowsource.RowRef{.{ .relational_key = "a" }};
    const values = [_][]const u8{"0123456789"};
    const columns = [_]rowsource.ColumnVector{.{ .name = "body", .values = .{ .bytes = &values } }};
    const batch = rowsource.ColumnBatch{
        .snapshot = .{ .table_id = "t", .snapshot_id = "s" },
        .row_refs = &refs,
        .columns = &columns,
    };
    var budget = try Budget.init(.{ .max_input_bytes = estimateBatchBytes(batch) });
    try budget.admitBatch(batch);
    try std.testing.expectError(error.LakeSidecarBuildBudgetExceeded, budget.admitBatch(batch));
}

test "lake build working-set allocator rejects growth before allocation" {
    const alloc = std.testing.allocator;
    var bounded = try WorkingSetAllocator.init(alloc, .{ .max_working_set_bytes = 8 });
    const bounded_alloc = bounded.allocator();
    const first = try bounded_alloc.alloc(u8, 8);
    defer bounded_alloc.free(first);
    try std.testing.expectError(error.OutOfMemory, bounded_alloc.alloc(u8, 1));
    try std.testing.expect(bounded.limit_exceeded);
    try std.testing.expectEqual(@as(usize, 8), bounded.live_bytes);
}
