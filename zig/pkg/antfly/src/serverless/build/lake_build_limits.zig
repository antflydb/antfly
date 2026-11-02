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

//! Total-work admission for lake sidecar builds and their replay buffers.

const std = @import("std");
const rowsource = @import("antfly_local_sources").storage_rowsource_types;

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
    quota: @import("antfly_platform").allocator.reclamation.Quota,

    pub fn init(backing: std.mem.Allocator, limits: Limits) !WorkingSetAllocator {
        try limits.validate();
        return .{ .quota = .init(backing, limits.max_working_set_bytes) };
    }
    pub fn deinit(self: *WorkingSetAllocator) void {
        self.quota.deinit();
    }
    pub fn allocator(self: *WorkingSetAllocator) std.mem.Allocator {
        return self.quota.allocator();
    }
    pub fn limitExceeded(self: *const WorkingSetAllocator) bool {
        return self.quota.limit_exceeded;
    }
    pub fn liveBytes(self: *const WorkingSetAllocator) usize {
        return self.quota.live_bytes;
    }
    pub fn maxBytes(self: *const WorkingSetAllocator) usize {
        return self.quota.max_live_bytes;
    }
    pub fn headroom(a: std.mem.Allocator) ?usize {
        if (@import("antfly_platform").allocator.reclamation.Quota.owner(a)) |owner| return @min(owner.available(), headroom(owner.backing) orelse return null);
        if (@import("antfly_local_sources").sql_parallel_scheduler.LockedAllocator.parentAllocator(a)) |parent| return headroom(parent);
        if (comptime @import("builtin").is_test) if (a.vtable == std.testing.allocator.vtable) return std.math.maxInt(usize);
        if (@import("antfly_local_sources").sql_memory_budget.optionalLocalHeadroom(a)) |layer| return @min(layer.available, headroom(layer.backing) orelse return null);
        return @import("antfly_local_sources").sql_memory_budget.optionalHeadroom(a);
    }
    /// Optional cache retention may use a known quota above opaque backing.
    /// Admission still uses strict headroom and actual allocation may fail.
    pub fn cacheHeadroom(a: std.mem.Allocator) ?usize {
        if (@import("antfly_platform").allocator.reclamation.Quota.owner(a)) |owner| {
            const available = owner.available();
            return @min(available, headroom(owner.backing) orelse available);
        }
        return headroom(a);
    }
    pub fn peakBytes(self: *const WorkingSetAllocator) usize {
        return self.quota.peak_bytes;
    }
    pub fn largestRejectedGrowth(self: *const WorkingSetAllocator) usize {
        return self.quota.largest_rejected_growth;
    }
    pub fn largestBackingFailure(self: *const WorkingSetAllocator) usize {
        return self.quota.largest_backing_failure;
    }
};

pub const Budget = struct {
    limits: Limits,
    mutex: std.atomic.Mutex = .unlocked,
    batches: usize = 0,
    rows: u64 = 0,
    input_bytes: usize = 0,

    pub fn init(limits: Limits) !Budget {
        try limits.validate();
        return .{ .limits = limits };
    }

    pub fn admitBatch(self: *Budget, batch: rowsource.ColumnBatch) !void {
        // Parallel file reducers share total-work admission, independently
        // of each worker's private spill and allocator lifetime.
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
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
    if (batch.external_span) |span| total = addOrMax(total, span.first.file_id.len);
    for (batch.row_refs) |row_ref| {
        const ref_bytes = switch (row_ref) {
            .relational_key => |key| key.len,
            .serverless => |value| value.fragment_id.len,
            .external => |value| addOrMax(addOrMax(value.source_id.len, value.snapshot_id.len), value.file_id.len),
        };
        total = addOrMax(total, ref_bytes);
    }
    for (batch.columns) |column| {
        total = addOrMax(total, addOrMax(column.name.len, if (column.nulls.bits != null) column.rowCount() else column.nulls.bytes.len));
        const value_bytes = switch (column.values) {
            .decimal128 => |values| std.math.mul(usize, values.values.len, @sizeOf(i128)) catch std.math.maxInt(usize),
            .encoded => std.math.maxInt(usize),
            inline .dictionary_i64, .dictionary_f64 => |values| addOrMax(std.math.mul(usize, values.indices.len, @sizeOf(u32)) catch std.math.maxInt(usize), std.math.mul(usize, values.values.len, 8) catch std.math.maxInt(usize)),
            .dictionary_bytes => |values| blk: {
                var bytes = std.math.mul(usize, values.indices.len, @sizeOf(u32)) catch break :blk std.math.maxInt(usize);
                bytes = addOrMax(bytes, std.math.mul(usize, values.values.len, @sizeOf([]const u8)) catch break :blk std.math.maxInt(usize));
                for (values.values) |value| bytes = addOrMax(bytes, value.len);
                break :blk bytes;
            },
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
    try std.testing.expect(bounded.limitExceeded());
    try std.testing.expect(bounded.largestRejectedGrowth() > 0);
    try std.testing.expectEqual(@as(usize, 8), bounded.peakBytes());
    try std.testing.expectEqual(@as(usize, 8), bounded.liveBytes());
}

test "lake build working-set allocator shares exact accounting across upload workers" {
    var bounded = try WorkingSetAllocator.init(std.testing.allocator, .{ .max_working_set_bytes = 8192 });
    const Worker = struct {
        fn run(allocator: std.mem.Allocator) void {
            for (0..1000) |_| {
                const bytes = allocator.alloc(u8, 1024) catch @panic("concurrent accounting exceeded admitted working set");
                @memset(bytes, 1);
                allocator.free(bytes);
            }
        }
    };
    var threads: [8]std.Thread = undefined;
    var started: usize = 0;
    defer for (threads[0..started]) |thread| thread.join();
    for (&threads) |*thread| {
        thread.* = try std.Thread.spawn(.{}, Worker.run, .{bounded.allocator()});
        started += 1;
    }
    for (threads) |thread| thread.join();
    started = 0;
    try std.testing.expectEqual(@as(usize, 0), bounded.liveBytes());
    try std.testing.expect(!bounded.limitExceeded());
}

test "lake build working-set headroom composes SQL budgets without bypassing opaque quotas" {
    const Memory = @import("antfly_local_sources").sql_memory_budget;
    var build = try WorkingSetAllocator.init(std.testing.allocator, .{ .max_working_set_bytes = 8192 });
    var sql: Memory = .{ .backing = build.allocator(), .limit = 4096 };
    const bytes = try sql.allocator().alloc(u8, 1024);
    defer sql.allocator().free(bytes);
    try std.testing.expectEqual(@as(?usize, 3072), WorkingSetAllocator.headroom(sql.allocator()));
    const outer = try build.allocator().alloc(u8, 5000);
    defer build.allocator().free(outer);
    try std.testing.expectEqual(@as(?usize, 2168), WorkingSetAllocator.headroom(sql.allocator()));
    var buffer: [8192]u8 = undefined;
    var unknown_backing = std.heap.FixedBufferAllocator.init(&buffer);
    var unknown: Memory = .{ .backing = unknown_backing.allocator(), .limit = 4096 };
    try std.testing.expectEqual(@as(?usize, null), WorkingSetAllocator.headroom(unknown.allocator()));
}

test "external lake publication quota enables adaptive spill ownership and preserves backing failure identity" {
    const local = @import("antfly_local_sources");
    const Quota = @import("antfly_platform").allocator.reclamation.Quota;
    var build = try WorkingSetAllocator.init(std.testing.allocator, .{ .max_working_set_bytes = 2 << 20 });
    defer build.deinit();
    var allocations: local.sql_parallel_scheduler.LockedAllocator = .{ .backing = build.allocator() };
    var marker: u8 = 0;
    const Check = struct {
        fn check(_: *anyopaque) !void {}
    };
    var manager: local.sql_spill.Manager = .{ .alloc = allocations.allocator(), .io = std.testing.io, .context = &marker, .checkpoint = Check.check, .async_writes = false };
    defer manager.deinit();
    var pages = try local.sql_spill.JoinPayloads.init(&manager);
    defer pages.close();
    const values = [_]local.sql_scalar.Datum{local.sql_scalar.Datum.json(.{ .string = "publication quota" })};
    const rows = [_][]const local.sql_scalar.Datum{&values};
    const offset = try pages.appendBatch(.{ .rows = &rows });
    try pages.file.seal();
    var reader = local.sql_spill.PayloadReader.init(&pages);
    defer reader.deinit();
    const payload = try reader.read(.{ .offset = offset, .index = 0 });
    defer payload.release();
    try std.testing.expect(reader.cache.registered);
    try std.testing.expectEqual(@as(usize, 1), manager.payload_budget_count);
    try std.testing.expect(Quota.owner(build.allocator()).?.residency.active());
    try std.testing.expect(!build.limitExceeded());
    var region = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer region.deinit();
    var region_build = try WorkingSetAllocator.init(region.allocator(), .{});
    defer region_build.deinit();
    try std.testing.expect(local.sql_memory_budget.reusableHeadroom(region_build.allocator()) == null);
    var denied = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var genuine = try WorkingSetAllocator.init(denied.allocator(), .{});
    defer genuine.deinit();
    try std.testing.expectError(error.OutOfMemory, genuine.allocator().alloc(u8, 8));
    try std.testing.expect(!genuine.limitExceeded());
}

test "optional block retention respects a request cap above an opaque backing allocator" {
    var buffer: [128 * 1024]u8 = undefined;
    var buffer_allocator = std.heap.FixedBufferAllocator.init(&buffer);
    var bounded = try WorkingSetAllocator.init(buffer_allocator.allocator(), .{ .max_working_set_bytes = 128 * 1024 });
    const a = bounded.allocator();
    try std.testing.expectEqual(@as(?usize, null), WorkingSetAllocator.headroom(a));
    try std.testing.expectEqual(@as(?usize, 128 * 1024), WorkingSetAllocator.cacheHeadroom(a));
    const bytes = try a.alloc(u8, 32 * 1024);
    defer a.free(bytes);
    try std.testing.expectEqual(@as(?usize, 96 * 1024), WorkingSetAllocator.cacheHeadroom(a));
    try std.testing.expect(!bounded.limitExceeded());
}
