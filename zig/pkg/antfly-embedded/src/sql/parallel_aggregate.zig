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
/// Stable allocator/owners survive until the final typed batches are consumed.
const Reduction = struct {
    backing: std.mem.Allocator,
    allocator: scheduling.LockedAllocator,
    results: [4]?*Result = @splat(null),
    groups: [4]*operators.Grouped = undefined,
    fn release(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        for (self.results) |result| if (result) |owned| owned.deinit();
        self.backing.destroy(self);
    }
};
const ReduceWork = struct {
    context: runtime.Context,
    owner: *Reduction,
    sources: []const ?*Result,
    selections: []const [][]usize,
    lane: usize,
    workspace: usize,
    ready: *std.Io.Event,
    fn run(self: @This()) anyerror!*Result {
        try self.ready.wait(self.context.backend.execution_io.?);
        const a = self.owner.allocator.allocator();
        const result = try a.create(Result);
        result.* = .{ .backing = a, .budget = .{ .backing = a, .limit = self.workspace } };
        errdefer result.deinit();
        result.grouped = try operators.Grouped.create(result.budget.allocator(), self.sources[0].?.grouped.?.specs, .{ .groups = self.context.limits.scan_rows, .bytes = self.workspace / 2, .spill = self.context.spill });
        var ordinal: u64 = 0;
        for (self.sources, self.selections) |source, selections| {
            try self.context.checkpoint();
            try result.grouped.?.mergeExactSelected(source.?.grouped.?, ordinal, selections[self.lane]);
            ordinal = try std.math.add(u64, ordinal, source.?.grouped.?.rows_seen);
        }
        return result;
    }
};
fn reduce(context: runtime.Context, grouped: *operators.Grouped, locals: []const ?*Result, workspace: usize) !bool {
    var partials: usize = 0;
    for (locals) |local| {
        const source = local.?.grouped.?;
        // Spilled partial streams already use bounded partition reduction.
        // Retain the existing serial bridge for those streams and tiny groups.
        if (source.external != null) return false;
        partials +|= source.groupCount();
    }
    if (partials < 4096 or grouped.key_count == 0) return false;
    const owner = try context.alloc.create(Reduction);
    owner.* = .{ .backing = context.alloc, .allocator = .{ .backing = if (context.spill) |manager| manager.allocator() else context.alloc } };
    var adopted = false;
    defer if (!adopted) Reduction.release(owner);
    var arena = std.heap.ArenaAllocator.init(owner.allocator.allocator());
    defer arena.deinit();
    const selections = try arena.allocator().alloc([][]usize, locals.len);
    for (locals, selections) |source, *selection| selection.* = try source.?.grouped.?.partitionGroups(arena.allocator(), locals.len);
    const io = context.backend.execution_io.?;
    var tasks: [4]?scheduling.Task(anyerror!*Result) = @splat(null);
    defer for (&tasks, 0..) |*task, lane| if (task.*) |*pending| {
        owner.results[lane] = pending.cancel(io) catch null;
        task.* = null;
    };
    var ready: std.Io.Event = .unset;
    var work: [4]ReduceWork = undefined;
    for (0..locals.len) |lane| {
        work[lane] = .{ .context = context, .owner = owner, .sources = locals, .selections = selections, .lane = lane, .workspace = workspace, .ready = &ready };
        if (lane != 0) tasks[lane] = scheduling.global().submit(io, workspace, ReduceWork.run, .{work[lane]});
    }
    ready.set(io);
    var failure: ?anyerror = null;
    for (work[0..locals.len], 0..) |item, lane| if (tasks[lane] == null) {
        owner.results[lane] = item.run() catch |err| blk: {
            failure = failure orelse err;
            break :blk null;
        };
    };
    for (tasks[0..locals.len], 0..) |*task, lane| if (task.*) |*pending| {
        owner.results[lane] = pending.await(io) catch |err| blk: {
            failure = failure orelse err;
            break :blk null;
        };
        task.* = null;
    };
    if (failure) |err| return switch (err) {
        // Decline before changing the destination; all local partials remain
        // readable for its established serial/spilling reduction.
        error.OutOfMemory, error.SqlProgramLimitExceeded => false,
        else => err,
    };
    var rows: u64 = 0;
    for (locals, 0..) |source, lane| {
        rows = try std.math.add(u64, rows, source.?.grouped.?.rows_seen);
        owner.groups[lane] = owner.results[lane].?.grouped.?;
    }
    try grouped.adoptReducers(.{ .ptr = owner, .groups = owner.groups[0..locals.len], .release = Reduction.release }, rows);
    adopted = true;
    return true;
}
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
    if (bound.group_count != 0) {
        const reduced = reduce(context, grouped, locals[0..cursors.len], workspace) catch |err| switch (err) {
            error.OutOfMemory, error.SqlProgramLimitExceeded => false,
            else => return err,
        };
        if (reduced) return true;
    }
    var ordinal: u64 = 0;
    for (locals[0..cursors.len]) |local| {
        const source = local.?.grouped.?;
        try grouped.mergeExact(source, ordinal);
        ordinal = try std.math.add(u64, ordinal, source.rows_seen);
    }
    return true;
}
