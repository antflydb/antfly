// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Shared pinned scan work with exact local reducers and spillable partial states.
const std = @import("std");
const runtime = @import("runtime.zig");
const catalog = @import("catalog.zig");
const binding = @import("aggregate_binding.zig");
const operators = @import("operators.zig");
const scheduling = @import("parallel_scheduler.zig");
const Work = struct {
    context: runtime.Context,
    bound: *const binding.Bound,
    cursor: catalog.Cursor,
    parts: usize,
    visited: *std.atomic.Value(usize),
    pages: *std.atomic.Value(usize),
    fn run(self: Work) anyerror!*operators.Grouped {
        var context = self.context;
        const local = operators.Grouped.create(context.alloc, self.bound.specs, .{ .groups = context.limits.scan_rows, .bytes = context.limits.retained_bytes / (4 * self.parts), .spill = context.spill }) catch return error.ParallelAggregateMemoryExceeded;
        errdefer local.deinit();
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
            const visited = self.visited.fetchAdd(page.selection.len, .monotonic);
            if (page.selection.len > context.limits.scan_rows -| visited) return error.SqlProgramLimitExceeded;
            if (self.pages.fetchAdd(1, .monotonic) >= context.limits.scan_pages) return error.SqlProgramLimitExceeded;
            @import("aggregate_runtime.zig").addColumns(context, self.bound, local, arena.allocator(), page) catch |err| return switch (err) {
                error.OutOfMemory, error.SqlProgramLimitExceeded => error.ParallelAggregateMemoryExceeded,
                else => err,
            };
            if (page.after == null) break;
        }
        return local;
    }
};
fn compatible(program: @import("scalar.zig").Program) bool {
    for (program.instructions) |instruction| switch (instruction.operation) {
        .column, .literal, .parameter, .unary => {},
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
    for (bound.specs) |spec| if (spec.distinct or !(spec.kind == .count or spec.kind == .bool_and or spec.kind == .bool_or or (spec.kind == .sum and spec.input_type == .integer))) return false;
    if (bound.input.predicate) |program| if (!compatible(program)) return false;
    for (bound.input.projections) |program| if (program) |value| if (!compatible(value)) return false;
    var allocator: scheduling.LockedAllocator = .{ .backing = context.alloc };
    const a = if (context.spill) |manager| manager.allocator() else allocator.allocator();
    const cursors = (scan.partitions(context, a, table, request, 4) catch |err| return switch (err) {
        error.OutOfMemory => false,
        else => err,
    }) orelse return false;
    defer a.free(cursors);
    defer for (cursors) |cursor| cursor.close(cursor.ptr);
    if (cursors.len < 2 or cursors.len > 4) return error.InvalidSqlBackendResponse;
    var tasks: [4]?scheduling.Task(anyerror!*operators.Grouped) = @splat(null);
    var locals: [4]?*operators.Grouped = @splat(null);
    // Join before releasing local buffers or pinned child cursors on all exits.
    defer {
        for (&tasks, &locals) |*task, *local| if (task.*) |*pending| {
            local.* = pending.cancel(io) catch null;
            task.* = null;
        };
        for (locals) |local| if (local) |value| value.deinit();
    }
    var visited: std.atomic.Value(usize) = .init(0);
    var pages: std.atomic.Value(usize) = .init(0);
    var worker_context = context;
    worker_context.alloc = a;
    var failure: ?anyerror = null;
    for (cursors, 0..) |cursor, index| {
        const work: Work = .{ .context = worker_context, .bound = bound, .cursor = cursor, .parts = cursors.len, .visited = &visited, .pages = &pages };
        tasks[index] = scheduling.global().submit(io, context.limits.retained_bytes / cursors.len, Work.run, .{work});
        if (tasks[index] == null) locals[index] = work.run() catch |err| blk: {
            failure = failure orelse err;
            break :blk null;
        };
    }
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
        const source = local.?;
        try grouped.mergeExact(source, ordinal);
        ordinal = try std.math.add(u64, ordinal, source.rows_seen);
    }
    return true;
}
