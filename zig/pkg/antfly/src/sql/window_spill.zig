// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! External window sorts, partition rows, peer directories and segment trees
//! share the statement's spill owner. Each pass preserves original ordinals.
const std = @import("std");
const disk = @import("disk_rows.zig");
const scalar = @import("scalar.zig");
const operators = @import("operators.zig");
const spill = @import("spill.zig");
const window = @import("window_runtime.zig");
const Datum = scalar.Datum;
fn same(left: []const Datum, right: []const Datum, count: usize) !bool {
    for (left[0..count], right[0..count]) |a, b| {
        if (a.sql_null != b.sql_null) return false;
        if (!a.sql_null and (try scalar.compare(a.value, b.value)) != .eq) return false;
    }
    return true;
}
fn appendPage(context: anytype, rows: *disk.Rows, output: @import("runtime.zig").Output, input_columns: anytype) !void {
    var arena = std.heap.ArenaAllocator.init(context.alloc);
    defer arena.deinit();
    for (output.rows, 0..) |row, index| {
        _ = arena.reset(.free_all);
        const values = try arena.allocator().alloc(Datum, rows.width);
        @memset(values, .{});
        for (row, input_columns, values[0..row.len], 0..) |value, column, *out, i| out.* = .{ .value = try @import("describe.zig").coerceAlloc(arena.allocator(), value, column.type), .sql_null = if (output.sql_nulls) |flags| flags[index][i] else value == .null };
        try rows.append(.{ .values = values, .keys = &.{}, .ordinal = rows.len });
    }
}
pub fn execute(context: anytype, statement: @import("ast.zig").Select) !?@import("runtime.zig").Output {
    const bound = context.binding.window.?;
    const manager = context.spill orelse return null;
    const compiled: @import("compiler.zig").Compiled = .{ .arena = undefined, .statement = .{ .select = bound.statement }, .parameter_count = @intCast(context.parameters.len) };
    var limits = context.limits;
    limits.result_rows = limits.scan_rows;
    var backend = context.backend;
    backend.spill_manager = manager;
    const input = (try @import("read_stream.zig").Stream.open(context.alloc, backend, &compiled, context.parameters, limits)) orelse return null;
    var input_owned = true;
    defer if (input_owned) input.close();
    const width = bound.input.columns.len + bound.specs.len;
    var first = try input.next(context.limits.page_rows);
    var first_owned = true;
    defer if (first_owned) first.deinit();
    if (first.exhausted) {
        var arena = std.heap.ArenaAllocator.init(context.alloc);
        defer arena.deinit();
        const cells = try arena.allocator().alloc([]Datum, first.output.rows.len);
        for (first.output.rows, cells, 0..) |row, *values, index| {
            values.* = try arena.allocator().alloc(Datum, width);
            @memset(values.*, .{});
            for (row, bound.input.columns, values.*[0..row.len], 0..) |value, column, *out, i| out.* = .{ .value = try @import("describe.zig").coerceAlloc(arena.allocator(), value, column.type), .sql_null = if (first.output.sql_nulls) |flags| flags[index][i] else value == .null };
        }
        return try window.evaluateCells(context, statement, cells);
    }
    var rows = try disk.Rows.init(context.alloc, manager, width);
    var rows_owned = true;
    defer if (rows_owned) rows.deinit();
    try appendPage(context, &rows, first.output, bound.input.columns);
    first.deinit();
    first_owned = false;
    while (true) {
        var page = try input.next(context.limits.page_rows);
        defer page.deinit();
        try appendPage(context, &rows, page.output, bound.input.columns);
        if (page.exhausted) break;
    }
    input.close();
    input_owned = false;
    const roots = try @import("ordering_reuse.zig").plan(context.arena, bound);
    for (bound.sorts, 0..) |specification, root| {
        if (roots[root] != root) continue;
        var arena = std.heap.ArenaAllocator.init(context.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const orders = try a.alloc(operators.Order, specification.partition.len + specification.order.len);
        @memset(orders[0..specification.partition.len], .{});
        @memcpy(orders[specification.partition.len..], specification.directions);
        var sort = spill.Sort.init(context.alloc, manager, orders, context.limits.retained_bytes / 4);
        defer sort.deinit();
        var scratch = std.heap.ArenaAllocator.init(context.alloc);
        defer scratch.deinit();
        for (0..rows.len) |index| {
            try context.checkpoint();
            _ = scratch.reset(.free_all);
            const row = try rows.row(index);
            const keys = try scratch.allocator().alloc(Datum, orders.len);
            for (specification.partition, keys[0..specification.partition.len]) |column, *key| key.* = row.values[column];
            for (specification.order, keys[specification.partition.len..]) |column, *key| key.* = row.values[column];
            // Sort compact row references; wide payloads stay in their
            // existing row store instead of being copied into every run.
            try sort.add(.{ .values = &.{Datum.json(.{ .integer = @intCast(index) })}, .keys = keys, .ordinal = row.ordinal });
        }
        var next = try disk.Rows.init(context.alloc, manager, width);
        errdefer next.deinit();
        var carry: ?operators.Row = null;
        while (true) {
            const first_row = carry orelse (try sort.next(scratch.allocator())) orelse break;
            carry = null;
            var keys_arena = std.heap.ArenaAllocator.init(context.alloc);
            defer keys_arena.deinit();
            const keys = try keys_arena.allocator().alloc(Datum, specification.partition.len);
            for (first_row.keys[0..keys.len], keys) |value, *key| key.* = try operators.cloneDatum(keys_arena.allocator(), value);
            var partition = try disk.Rows.init(context.alloc, manager, width);
            defer partition.deinit();
            try partition.append(try rows.row(std.math.cast(usize, first_row.values[0].value.integer) orelse return error.InvalidSqlSpill));
            while (true) {
                _ = scratch.reset(.free_all);
                const candidate = (try sort.next(scratch.allocator())) orelse break;
                if (!try same(keys, candidate.keys, keys.len)) {
                    carry = candidate;
                    break;
                }
                try partition.append(try rows.row(std.math.cast(usize, candidate.values[0].value.integer) orelse return error.InvalidSqlSpill));
            }
            try partition.enableColumnUpdates(bound.input.columns.len);
            for (bound.sorts, 0..) |requirement, sort_index| {
                if (roots[sort_index] != root) continue;
                var starts = try disk.Integers.init(manager);
                defer starts.deinit();
                var ends = try disk.Integers.init(manager);
                defer ends.deinit();
                var groups = try disk.Integers.init(manager);
                defer groups.deinit();
                var position: usize = 0;
                while (position < partition.len) {
                    var end = position + 1;
                    while (end < partition.len and try window.equal(&partition, position, end, requirement.order)) : (end += 1) try context.checkpoint();
                    try groups.append(position);
                    for (position..end) |_| {
                        try starts.append(position);
                        try ends.append(end);
                    }
                    position = end;
                }
                try groups.append(partition.len);
                const indices = disk.Identity{ .len = partition.len };
                for (bound.specs, 0..) |spec, column| if (spec.sort == sort_index) try window.evaluate(context, &partition, indices, requirement, spec, bound.input.columns.len + column, &starts, &ends, &groups);
            }
            for (0..partition.len) |index| try next.append(try partition.row(index));
        }
        rows.deinit();
        rows = next;
        rows_owned = true;
    }
    return try window.finishCells(context, statement, &rows);
}
