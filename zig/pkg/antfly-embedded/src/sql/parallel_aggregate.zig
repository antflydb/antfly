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

//! Shared pinned scan work with exact local reducers and spillable partial states.
const std = @import("std");
const runtime = @import("runtime.zig");
const catalog = @import("catalog.zig");
const binding = @import("aggregate_binding.zig");
const operators = @import("operators.zig");
const scheduling = @import("parallel_scheduler.zig");
const Result = struct {
    backing: std.mem.Allocator,
    budget: @import("memory_budget.zig"),
    grouped: ?*operators.Grouped = null,
    fn deinit(self: *Result) void {
        if (self.grouped) |grouped| grouped.deinit();
        std.debug.assert(self.budget.live == 0);
        self.backing.destroy(self);
    }
};
const Work = struct {
    context: runtime.Context,
    bound: *const binding.Bound,
    cursor: catalog.Cursor,
    workspace: usize,
    ready: *std.Io.Event,
    quota: *@import("native_scan_quota.zig").Quota,
    fn run(self: Work) anyerror!*Result {
        defer if (self.cursor.finish_partition) |finish| finish(self.cursor.ptr);
        try self.ready.wait(self.context.backend.execution_io.?);
        const result = self.context.alloc.create(Result) catch return error.ParallelAggregateMemoryExceeded;
        result.* = .{ .backing = self.context.alloc, .budget = .{ .backing = self.context.alloc, .limit = self.workspace } };
        errdefer result.deinit();
        var context = self.context;
        context.alloc = result.budget.allocator();
        context.limits.retained_bytes = self.workspace;
        const local = operators.Grouped.create(context.alloc, self.bound.specs, .{ .groups = context.limits.scan_rows, .bytes = self.workspace / 2, .spill = context.spill }) catch return error.ParallelAggregateMemoryExceeded;
        result.grouped = local;
        if (self.bound.group_count == 0) local.ensureGlobalGroup() catch return error.ParallelAggregateMemoryExceeded;
        while (true) {
            try context.checkpoint();
            var arena = std.heap.ArenaAllocator.init(context.alloc);
            defer arena.deinit();
            context.arena = arena.allocator();
            const page = self.cursor.next_columns.?(self.cursor.ptr, arena.allocator(), context.limits.executionRows()) catch |err| return switch (err) {
                error.OutOfMemory => error.ParallelAggregateMemoryExceeded,
                else => err,
            };
            if (page.selection.len > context.limits.executionRows()) return error.InvalidSqlBackendResponse;
            // Terminal EOF proves exhaustion and spends no logical page credit.
            // Empty nonterminal responses still count, bounding broken cursors.
            if (page.selection.len == 0 and page.after == null) break;
            try self.quota.admit(page.selection.len, page.after == null);
            @import("aggregate_runtime.zig").addColumns(context, self.bound, local, arena.allocator(), page) catch |err| return switch (err) {
                error.OutOfMemory, error.SqlProgramLimitExceeded => error.ParallelAggregateMemoryExceeded,
                else => err,
            };
            if (page.after == null) break;
        }
        return result;
    }
};
fn compatible(program: @import("scalar.zig").Program) bool {
    for (program.instructions) |instruction| switch (instruction.operation) {
        .column, .literal, .parameter, .unary, .cast => {},
        .binary => |binary| switch (binary.op) {
            .add, .subtract, .multiply, .divide, .modulo, .eq, .neq, .lt, .lte, .gt, .gte, .is_distinct, .is_not_distinct => {},
            else => return false,
        },
        else => return false,
    };
    return true;
}
pub fn execute(context: runtime.Context, bound: *const binding.Bound, grouped: *operators.Grouped, scan: *runtime.Context.ScanState, table: catalog.Table, request: catalog.Scan) !bool {
    const io = context.backend.execution_io orelse return false;
    if (context.limits.retained_bytes < 1024 * 1024) return false;
    for (bound.specs) |spec| if (!operators.exactMergeable(spec)) return false;
    if (bound.input.predicate) |program| if (!compatible(program)) return false;
    for (bound.input.projections) |program| if (program) |value| if (!compatible(value)) return false;
    const scheduler = scheduling.global();
    const budget = scheduler.cpuWorkspaceBudget(context.limits.retained_bytes / 2);
    const maximum = scheduler.fanout(4, budget, 1024 * 1024);
    if (maximum < 2) return false;
    var allocator: scheduling.LockedAllocator = .{ .backing = context.alloc };
    const a = if (context.spill) |manager| manager.allocator() else allocator.allocator();
    const cursors = (scan.partitions(context, a, table, request, maximum) catch |err| return switch (err) {
        error.OutOfMemory => false,
        else => err,
    }) orelse return false;
    defer a.free(cursors);
    defer for (cursors) |cursor| cursor.close(cursor.ptr);
    if (cursors.len < 2 or cursors.len > 4) return error.InvalidSqlBackendResponse;
    var tasks: [4]?scheduling.Task(anyerror!*Result) = @splat(null);
    var locals: [4]?*Result = @splat(null);
    // Join before releasing local buffers or pinned child cursors on all exits.
    defer {
        for (&tasks, &locals) |*task, *local| if (task.*) |*pending| {
            local.* = pending.cancel(io) catch null;
            task.* = null;
        };
        for (locals) |local| if (local) |value| value.deinit();
    }
    var quota: @import("native_scan_quota.zig").Quota = .{ .capacity = context.limits.executionRows(), .row_limit = context.limits.scan_rows, .page_limit = context.limits.scan_pages };
    var worker_context = context;
    worker_context.alloc = a;
    var failure: ?anyerror = null;
    var ready: std.Io.Event = .unset;
    // Complete admission before any lane can recursively submit expression or
    // I/O work. One lane always runs on the caller; denied lanes run afterward.
    var work: [4]Work = undefined;
    const workspace = budget / cursors.len;
    for (cursors, 0..) |cursor, index| {
        work[index] = .{ .context = worker_context, .bound = bound, .cursor = cursor, .workspace = workspace, .ready = &ready, .quota = &quota };
        if (index != 0) tasks[index] = scheduler.submit(io, workspace, Work.run, .{work[index]});
    }
    ready.set(io);
    for (work[0..cursors.len], 0..) |item, index| if (tasks[index] == null) {
        locals[index] = item.run() catch |err| blk: {
            failure = failure orelse err;
            break :blk null;
        };
    };
    for (tasks[0..cursors.len], 0..) |*task, index| if (task.*) |*pending| {
        locals[index] = pending.await(io) catch |err| blk: {
            failure = failure orelse err;
            break :blk null;
        };
        task.* = null;
    };
    if (failure) |err| {
        // A pinned parent remains readable. If local hash state cannot fit its
        // shard, reclaim all worker memory before the serial spilling fallback.
        if (err == error.ParallelAggregateMemoryExceeded) return false;
        return err;
    }
    var ordinal: u64 = 0;
    for (locals[0..cursors.len]) |local| {
        const source = local.?.grouped.?;
        try grouped.mergeExact(source, ordinal);
        ordinal = try std.math.add(u64, ordinal, source.rows_seen);
    }
    return true;
}
