// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Streaming aggregate execution over the same retained native scan contract
//! as ordinary SELECT. Only grouping keys and aggregate states outlive pages.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const binding = @import("aggregate_binding.zig");
const operators = @import("operators.zig");
const Datum = @import("scalar.zig").Datum;
const Json = std.json.Value;

fn addRow(context: anytype, bound: *const binding.Bound, grouped: *operators.Grouped, alloc: std.mem.Allocator, row: catalog.Row) !void {
    const cells = try bound.input.cells(alloc, row);
    if (!try bound.input.matches(alloc, cells, context.parameters)) return;
    const values = try alloc.alloc(Datum, bound.group_count);
    for (bound.input.projections[0..bound.group_count], values) |program, *value| value.* = try program.?.evaluate(alloc, cells, context.parameters, .{});
    const inputs = try alloc.alloc(Datum, bound.inputs.len);
    for (bound.inputs, bound.filters, inputs) |index, filter, *value| {
        value.* = .{};
        if (filter) |slot| {
            const test_value = try bound.input.projections[slot].?.evaluate(alloc, cells, context.parameters, .{});
            if (test_value.sql_null) continue;
            if (test_value.value != .bool) return error.SqlTypeMismatch;
            if (!test_value.value.bool) continue;
        }
        value.* = if (index) |slot| try bound.input.projections[slot].?.evaluate(alloc, cells, context.parameters, .{}) else Datum.json(.{ .integer = 1 });
    }
    try grouped.add(values, inputs);
}

pub fn execute(context: anytype, statement: ast.Select) !@import("runtime.zig").Output {
    const bound = context.binding.aggregate orelse return error.InvalidSqlBackendResponse;
    const limit = try context.count(statement.limit, context.limits.result_rows);
    const offset = try context.count(statement.offset, 0);
    if (limit > context.limits.result_rows or offset > context.limits.scan_rows) return error.SqlProgramLimitExceeded;
    if (limit == 0) return .{ .columns = context.binding.columns, .command_tag = "SELECT" };
    const grouped = try operators.Grouped.create(context.alloc, bound.specs, .{ .groups = context.limits.scan_rows, .bytes = context.limits.retained_bytes });
    defer grouped.deinit();
    if (bound.group_count == 0) try grouped.ensureGlobalGroup();
    if (context.binding.table) |table| {
        const predicates = try context.conditions(table, statement.predicate);
        const fields = try context.arena.alloc([]const u8, bound.input.required.len);
        var field_count: usize = 0;
        for (bound.input.required) |ordinal| {
            const name = bound.input.columns[ordinal].name;
            if (std.mem.eql(u8, name, "_id")) continue;
            fields[field_count] = (try table.column(name)).path;
            field_count += 1;
        }
        var scan: @TypeOf(context).ScanState = .{};
        defer scan.deinit();
        var after: ?[]const u8 = null;
        defer if (after) |key| context.alloc.free(key);
        var pages: usize = 0;
        var visited: usize = 0;
        while (!predicates.empty) {
            try context.checkpoint();
            pages += 1;
            if (pages > context.limits.scan_pages) return error.SqlProgramLimitExceeded;
            var arena = std.heap.ArenaAllocator.init(context.alloc);
            defer arena.deinit();
            const page = try scan.page(context, arena.allocator(), table, .{ .fields = fields[0..field_count], .primary_key = predicates.primary_key, .conditions = predicates.terms.items, .after = after, .limit = context.limits.page_rows });
            defer page.deinit();
            if (page.rows.len > context.limits.page_rows) return error.InvalidSqlBackendResponse;
            for (page.rows) |row| {
                try context.checkpoint();
                visited += 1;
                if (visited > context.limits.scan_rows) return error.SqlProgramLimitExceeded;
                try addRow(context, bound, grouped, arena.allocator(), row);
            }
            const next = page.after orelse break;
            if (!scan.retained(context)) return error.SqlStatementSnapshotRequired;
            if (after) |previous| if (std.mem.eql(u8, previous, next)) return error.InvalidSqlBackendResponse;
            const owned = try context.alloc.dupe(u8, next);
            if (after) |previous| context.alloc.free(previous);
            after = owned;
        }
    } else {
        var arena = std.heap.ArenaAllocator.init(context.alloc);
        defer arena.deinit();
        try context.checkpoint();
        try addRow(context, bound, grouped, arena.allocator(), .{ .id = "", .version = 0, .value = .{ .object = .empty } });
    }
    const orders = try context.arena.alloc(operators.Order, statement.order_by.len);
    for (statement.order_by, orders) |order, *out| out.* = .{ .descending = order.descending, .nulls_first = order.nulls_first };
    const capacity = std.math.add(usize, offset, limit + @intFromBool(statement.limit == null)) catch return error.SqlProgramLimitExceeded;
    var top = try operators.TopK.init(context.alloc, capacity, orders, context.limits.retained_bytes);
    defer top.deinit();
    for (0..grouped.groupCount()) |index| {
        try context.checkpoint();
        var arena = std.heap.ArenaAllocator.init(context.alloc);
        defer arena.deinit();
        const alloc = arena.allocator();
        const group = try grouped.resultAt(alloc, index);
        const cells = try alloc.alloc(Datum, group.keys.len + group.aggregates.len);
        @memcpy(cells[0..group.keys.len], group.keys);
        @memcpy(cells[group.keys.len..], group.aggregates);
        if (bound.having) |program| {
            const result = try program.evaluate(alloc, cells, context.parameters, .{});
            if (result.sql_null) continue;
            if (result.value != .bool) return error.SqlTypeMismatch;
            if (!result.value.bool) continue;
        }
        const values = try alloc.alloc(Datum, bound.outputs.len);
        for (bound.outputs, values) |program, *value| value.* = try program.evaluate(alloc, cells, context.parameters, .{});
        const keys = try alloc.alloc(Datum, bound.orders.len);
        for (bound.orders, keys) |program, *value| value.* = try program.evaluate(alloc, cells, context.parameters, .{});
        try top.add(.{ .values = values, .keys = keys, .ordinal = group.ordinal });
    }
    const ordered = try top.finish(context.arena);
    const remaining = ordered.len -| offset;
    if (statement.limit == null and remaining > limit) return error.SqlResultTooLarge;
    const selected = ordered[@min(offset, ordered.len)..][0..@min(remaining, limit)];
    const rows = try context.arena.alloc([]const Json, selected.len);
    const nulls = try context.arena.alloc([]const bool, selected.len);
    for (selected, rows, nulls) |row, *output, *null_row| {
        const values = try context.arena.alloc(Json, row.values.len);
        const sql_nulls = try context.arena.alloc(bool, row.values.len);
        for (row.values, values, sql_nulls) |value, *out, *sql_null| {
            out.* = try context.outputValue(value.value);
            sql_null.* = value.sql_null;
        }
        output.* = values;
        null_row.* = sql_nulls;
    }
    return .{ .columns = context.binding.columns, .rows = rows, .sql_nulls = nulls, .command_tag = "SELECT" };
}
