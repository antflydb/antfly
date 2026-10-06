// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Streaming aggregate execution over the same retained native scan contract
//! as ordinary SELECT. Only grouping keys and aggregate states outlive pages.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const binding = @import("aggregate_binding.zig");
const operators = @import("operators.zig");
const scalar = @import("scalar.zig");
const Datum = scalar.Datum;
const Json = std.json.Value;

fn addRow(context: anytype, bound: *const binding.Bound, grouped: *operators.Grouped, alloc: std.mem.Allocator, row: catalog.Row) !void {
    const cells = try bound.input.cells(alloc, row);
    return addCells(context, bound, grouped, alloc, cells);
}

fn addCells(context: anytype, bound: *const binding.Bound, grouped: *operators.Grouped, alloc: std.mem.Allocator, cells: []const Datum) !void {
    if (!try bound.input.matchesWithProvider(alloc, cells, context.parameters, context.backend.decision_provider)) return;
    const values = try alloc.alloc(Datum, bound.group_count);
    for (bound.input.projections[0..bound.group_count], values) |program, *value| value.* = try context.evaluate(alloc, program.?, cells);
    const inputs = try alloc.alloc(Datum, bound.inputs.len);
    for (bound.inputs, bound.filters, inputs) |index, filter, *value| {
        value.* = .{};
        if (filter) |slot| {
            const test_value = try context.evaluate(alloc, bound.input.projections[slot].?, cells);
            if (test_value.sql_null) continue;
            if (test_value.value != .bool) return error.SqlTypeMismatch;
            if (!test_value.value.bool) continue;
        }
        value.* = if (index) |slot| try context.evaluate(alloc, bound.input.projections[slot].?, cells) else Datum.json(.{ .integer = 1 });
    }
    try grouped.add(values, inputs);
}

fn addRows(context: anytype, bound: *const binding.Bound, grouped: *operators.Grouped, alloc: std.mem.Allocator, cells: []const []const Datum) !void {
    const decision = @import("decision_eval.zig");
    const predicates = if (bound.input.predicate) |*program| try decision.evaluateBatch(alloc, context.backend.decision_provider, program, cells, context.parameters) else null;
    var accepted: std.ArrayList([]const Datum) = .empty;
    for (cells, 0..) |row, i| {
        if (predicates) |values| {
            if (values[i].sql_null) continue;
            if (values[i].value != .bool) return error.SqlTypeMismatch;
            if (!values[i].value.bool) continue;
        }
        try accepted.append(alloc, row);
    }
    const keys = try alloc.alloc([]Datum, accepted.items.len);
    const inputs = try alloc.alloc([]Datum, accepted.items.len);
    for (keys, inputs) |*key, *input| {
        key.* = try alloc.alloc(Datum, bound.group_count);
        input.* = try alloc.alloc(Datum, bound.inputs.len);
        @memset(input.*, .{});
    }
    for (bound.input.projections[0..bound.group_count], 0..) |optional, k| {
        const values = try decision.evaluateBatch(alloc, context.backend.decision_provider, &optional.?, accepted.items, context.parameters);
        for (keys, values) |key, value| key[k] = value;
    }
    for (bound.inputs, bound.filters, 0..) |index, filter, k| {
        const filters = if (filter) |slot| try decision.evaluateBatch(alloc, context.backend.decision_provider, &bound.input.projections[slot].?, accepted.items, context.parameters) else null;
        var selected: std.ArrayList([]const Datum) = .empty;
        var positions: std.ArrayList(usize) = .empty;
        for (accepted.items, 0..) |row, i| {
            if (filters) |values| {
                if (values[i].sql_null) continue;
                if (values[i].value != .bool) return error.SqlTypeMismatch;
                if (!values[i].value.bool) continue;
            }
            try selected.append(alloc, row);
            try positions.append(alloc, i);
        }
        if (index) |slot| {
            const values = try decision.evaluateBatch(alloc, context.backend.decision_provider, &bound.input.projections[slot].?, selected.items, context.parameters);
            for (positions.items, values) |i, value| inputs[i][k] = value;
        } else for (positions.items) |i| {
            inputs[i][k] = Datum.json(.{ .integer = 1 });
        }
    }
    if (bound.group_count == 0) try grouped.addGlobalBatch(inputs) else for (keys, inputs) |key, input| try grouped.add(key, input);
}

// Evaluate expressions against selected physical columns. Only unsupported
// scalar instructions construct a temporary input row; retained aggregate
// inputs contain the computed values, never the complete input row matrix.
fn columnValues(context: anytype, bound: *const binding.Bound, a: std.mem.Allocator, page: catalog.ColumnPage, program: *const scalar.Program) ![]const Datum {
    if (try @import("vector_eval.zig").evaluateColumnsScheduled(a, program, page, bound.input.columns, context.parameters, context.backend.execution_io)) |values| return values;
    const values = try a.alloc(Datum, page.selection.len);
    for (values, 0..) |*value, index| value.* = try context.evaluate(a, program.*, try bound.input.columnCells(a, page, index));
    return values;
}
pub fn addColumns(context: anytype, bound: *const binding.Bound, grouped: *operators.Grouped, a: std.mem.Allocator, page: catalog.ColumnPage) !void {
    const predicates = if (bound.input.predicate) |*program| try columnValues(context, bound, a, page, program) else null;
    var selection: std.ArrayList(usize) = .empty;
    for (page.selection, 0..) |physical, index| {
        if (predicates) |values| {
            if (values[index].sql_null) continue;
            if (values[index].value != .bool) return error.SqlTypeMismatch;
            if (!values[index].value.bool) continue;
        }
        try selection.append(a, physical);
    }
    var accepted = page;
    accepted.selection = selection.items;
    // Unfiltered grouped cohorts preserve physical expression vectors all the
    // way into typed state. Filtered cohorts below retain lazy evaluation.
    if (bound.group_count != 0 and for (bound.filters) |filter| {
        if (filter != null) break false;
    } else true) {
        const Batch = @import("execution_batch.zig").Batch;
        var cohort: std.ArrayList(*const scalar.Program) = .empty;
        for (bound.input.projections[0..bound.group_count]) |*optional| try cohort.append(a, &optional.*.?);
        for (bound.inputs) |input| if (input) |slot| {
            try cohort.append(a, &bound.input.projections[slot].?);
        };
        const outputs = try @import("vector_eval.zig").evaluateColumnsEncodedMany(a, cohort.items, accepted, bound.input.columns, context.parameters, context.backend.execution_io);
        const key_batches = try a.alloc(Batch, bound.group_count);
        const input_batches = try a.alloc(Batch, bound.inputs.len);
        for (key_batches, cohort.items[0..key_batches.len], outputs[0..key_batches.len]) |*batch, program, output| {
            batch.* = output orelse .{ .vectors = .{ .values = try a.dupe([]const Datum, &.{try columnValues(context, bound, a, accepted, program)}), .count = accepted.selection.len } };
        }
        var position = key_batches.len;
        for (bound.inputs, input_batches) |input, *batch| {
            if (input != null) {
                batch.* = outputs[position] orelse .{ .vectors = .{ .values = try a.dupe([]const Datum, &.{try columnValues(context, bound, a, accepted, cohort.items[position])}), .count = accepted.selection.len } };
                position += 1;
            } else {
                const indices = try a.alloc(u32, accepted.selection.len);
                @memset(indices, 0);
                batch.* = .{ .dictionary = .{ .values = &.{Datum.json(.{ .integer = 1 })}, .indices = indices } };
            }
        }
        return grouped.addEncodedColumns(key_batches, input_batches, accepted.selection.len);
    }
    const keys = try a.alloc([]const Datum, bound.group_count);
    const inputs = try a.alloc([]Datum, bound.inputs.len);
    for (inputs) |*input| {
        input.* = try a.alloc(Datum, selection.items.len);
        @memset(input.*, .{});
    }
    // Keys and unfiltered inputs share one DAG and column normalization.
    // Filtered inputs form separate cohorts below, preserving lazy errors.
    var programs: std.ArrayList(*const scalar.Program) = .empty;
    for (bound.input.projections[0..bound.group_count]) |*optional| try programs.append(a, &optional.*.?);
    for (bound.inputs, bound.filters) |input, filter| if (filter == null) if (input) |slot| {
        try programs.append(a, &bound.input.projections[slot].?);
    };
    const vectors = try @import("vector_eval.zig").evaluateColumnsManyScheduled(a, programs.items, accepted, bound.input.columns, context.parameters, context.backend.execution_io);
    for (programs.items, vectors) |program, *vector| if (vector.* == null) {
        @constCast(vector).* = try columnValues(context, bound, a, accepted, program);
    };
    for (keys, vectors[0..keys.len]) |*key, vector| key.* = vector.?;
    var unfiltered: usize = keys.len;
    for (bound.inputs, bound.filters, 0..) |input, filter, k| if (filter == null) {
        if (input != null) {
            @memcpy(inputs[k], vectors[unfiltered].?);
            unfiltered += 1;
        } else @memset(inputs[k], Datum.json(.{ .integer = 1 }));
    };
    const completed = try a.alloc(bool, bound.inputs.len);
    @memset(completed, false);
    for (bound.filters, 0..) |filter, k| {
        if (filter == null or completed[k]) continue;
        const filter_program = &bound.input.projections[filter.?].?;
        const filters = try columnValues(context, bound, a, accepted, filter_program);
        var filtered: std.ArrayList(usize) = .empty;
        var positions: std.ArrayList(usize) = .empty;
        for (accepted.selection, filters, 0..) |physical, value, index| {
            if (value.sql_null) continue;
            if (value.value != .bool) return error.SqlTypeMismatch;
            if (!value.value.bool) continue;
            try filtered.append(a, physical);
            try positions.append(a, index);
        }
        var selected = accepted;
        selected.selection = filtered.items;
        var cohort: std.ArrayList(*const scalar.Program) = .empty;
        var slots: std.ArrayList(usize) = .empty;
        for (bound.inputs, bound.filters, 0..) |input, candidate, index| {
            if (candidate == null or completed[index]) continue;
            if (candidate.? != filter.? and !@import("typed_kernel.zig").sameProgram(filter_program, &bound.input.projections[candidate.?].?)) continue;
            completed[index] = true;
            if (input) |slot| {
                try cohort.append(a, &bound.input.projections[slot].?);
                try slots.append(a, index);
            } else for (positions.items) |position| inputs[index][position] = Datum.json(.{ .integer = 1 });
        }
        const outputs = try @import("vector_eval.zig").evaluateColumnsManyScheduled(a, cohort.items, selected, bound.input.columns, context.parameters, context.backend.execution_io);
        for (cohort.items, outputs, slots.items) |program, optional, slot| {
            const values = optional orelse try columnValues(context, bound, a, selected, program);
            for (positions.items, values) |index, value| inputs[slot][index] = value;
        }
    }
    if (bound.group_count == 0) {
        const columns = try a.alloc([]const Datum, inputs.len);
        for (inputs, columns) |input, *column| column.* = input;
        try grouped.addGlobalColumns(columns, accepted.selection.len);
    } else {
        const columns = try a.alloc([]const Datum, inputs.len);
        for (inputs, columns) |input, *column| column.* = input;
        try grouped.addColumns(keys, columns, accepted.selection.len);
    }
}

fn addGroupedDecisionPages(context: anytype, bound: *const binding.Bound, grouped: *operators.Grouped, top: *operators.TopK, projection: @import("decision_eval.zig").SortedProjection) !void {
    const decision = @import("decision_eval.zig");
    var exhausted = false;
    while (!exhausted) {
        try context.checkpoint();
        var arena = std.heap.ArenaAllocator.init(context.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        var cells: std.ArrayList([]const Datum) = .empty;
        var ordinals: std.ArrayList(u64) = .empty;
        var bytes: usize = 0;
        while (cells.items.len < context.limits.page_rows) {
            const group = (try grouped.nextResult(a)) orelse {
                exhausted = true;
                break;
            };
            const row = try a.alloc(Datum, group.keys.len + group.aggregates.len);
            @memcpy(row[0..group.keys.len], group.keys);
            @memcpy(row[group.keys.len..], group.aggregates);
            try cells.append(a, row);
            try ordinals.append(a, group.ordinal);
            for (row) |cell| bytes +|= try operators.datumBytes(cell);
            if (bytes >= context.limits.page_bytes) break;
        }
        const predicates = if (bound.having) |*program| try decision.evaluateBatch(a, context.backend.decision_provider, program, cells.items, context.parameters) else null;
        var accepted: std.ArrayList([]const Datum) = .empty;
        var positions: std.ArrayList(usize) = .empty;
        for (cells.items, 0..) |row, index| {
            if (predicates) |values| {
                if (values[index].sql_null) continue;
                if (values[index].value != .bool) return error.SqlTypeMismatch;
                if (!values[index].value.bool) continue;
            }
            try accepted.append(a, row);
            try positions.append(a, index);
        }
        const accepted_ordinals = try a.alloc(u64, positions.items.len);
        for (positions.items, accepted_ordinals) |index, *ordinal| ordinal.* = ordinals.items[index];
        try projection.add(context, a, top, accepted.items, accepted_ordinals);
    }
}

fn loadMaterialized(context: anytype, bound: *const binding.Bound, grouped: *operators.Grouped, table: catalog.Table) !bool {
    const open = context.backend.vtable.aggregate_partials orelse return false;
    const recipe = (try @import("aggregate_materialization.zig").fromBound(context.arena, table, bound.*)) orelse return false;
    const cursor = (try open(context.backend.ptr, context.alloc, table, recipe)) orelse return false;
    defer cursor.close(cursor.ptr);
    var pages: usize = 0;
    var groups: usize = 0;
    while (true) {
        try context.checkpoint();
        var arena = std.heap.ArenaAllocator.init(context.alloc);
        defer arena.deinit();
        const partials = (try cursor.next(cursor.ptr, arena.allocator(), context.limits.executionRows())) orelse break;
        if (partials.len == 0 or partials.len > context.limits.executionRows()) return error.InvalidSqlBackendResponse;
        pages += 1;
        if (pages > context.limits.scan_pages or partials.len > context.limits.scan_rows -| groups) return error.SqlProgramLimitExceeded;
        groups += partials.len;
        for (partials) |partial| {
            if (partial.keys.len != recipe.keys.len) return error.InvalidSqlBackendResponse;
            try grouped.importPartialMapped(partial.keys, partial.aggregates, partial.aggregate_slots, partial.ordinal);
        }
    }
    return true;
}

pub fn execute(context: anytype, statement: ast.Select) !@import("runtime.zig").Output {
    const bound = context.binding.aggregate orelse return error.InvalidSqlBackendResponse;
    try bound.input.validateDecisions(context.arena, context.parameters, context.backend.decision_provider);
    var external = if (bound.input.predicate) |*program| @import("decision_eval.zig").hasExternal(program) else false;
    for (bound.input.projections) |optional| if (optional) |*program| {
        external = external or @import("decision_eval.zig").hasExternal(program);
    };
    const limit = try context.count(statement.limit, context.limits.result_rows);
    const offset = try context.count(statement.offset, 0);
    if (limit > context.limits.result_rows or offset > context.limits.scan_rows) return error.SqlProgramLimitExceeded;
    if (limit == 0) return .{ .columns = context.binding.columns, .command_tag = "SELECT" };
    const grouped = try operators.Grouped.create(context.alloc, bound.specs, .{ .groups = context.limits.scan_rows, .bytes = context.limits.retained_bytes / 2, .spill = context.spill });
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
        var aggregate_loaded = try loadMaterialized(context, bound, grouped, table);
        const count_only = bound.group_count == 0 and bound.input.predicate == null and predicates.terms.items.len == 0 and predicates.primary_key == null and for (bound.specs, bound.inputs, bound.filters) |spec, input, filter| {
            if (spec.kind != .count or spec.distinct or input != null or filter != null) break false;
        } else true;
        if (!aggregate_loaded and count_only and !predicates.empty) if (try scan.count(context, table, .{ .fields = fields[0..field_count], .limit = context.limits.page_rows })) |count| {
            try grouped.addGlobalCount(count);
            aggregate_loaded = true;
        };
        const parallel = !predicates.empty and !aggregate_loaded and try @import("parallel_aggregate.zig").execute(context, bound, grouped, &scan, table, .{ .fields = fields[0..field_count], .primary_key = predicates.primary_key, .conditions = predicates.terms.items, .limit = context.limits.executionRows() });
        while (!predicates.empty and !aggregate_loaded and !parallel) {
            try context.checkpoint();
            pages += 1;
            if (pages > context.limits.scan_pages) return error.SqlProgramLimitExceeded;
            var arena = std.heap.ArenaAllocator.init(context.alloc);
            defer arena.deinit();
            if (!external) if (try scan.columns(context, arena.allocator(), table, .{ .fields = fields[0..field_count], .primary_key = predicates.primary_key, .conditions = predicates.terms.items, .after = after, .limit = context.limits.executionRows() })) |column_page| {
                if (column_page.selection.len > context.limits.executionRows()) return error.InvalidSqlBackendResponse;
                if (column_page.selection.len > context.limits.scan_rows -| visited) return error.SqlProgramLimitExceeded;
                visited += column_page.selection.len;
                try addColumns(context, bound, grouped, arena.allocator(), column_page);
                const next = column_page.after orelse break;
                if (!scan.retained(context)) return error.SqlStatementSnapshotRequired;
                if (after) |previous| if (std.mem.eql(u8, previous, next)) return error.InvalidSqlBackendResponse;
                const owned = try context.alloc.dupe(u8, next);
                if (after) |previous| context.alloc.free(previous);
                after = owned;
                continue;
            };
            const page = try scan.page(context, arena.allocator(), table, .{ .fields = fields[0..field_count], .primary_key = predicates.primary_key, .conditions = predicates.terms.items, .after = after, .limit = context.limits.page_rows });
            defer page.deinit();
            if (page.rows.len > context.limits.page_rows) return error.InvalidSqlBackendResponse;
            if (visited + page.rows.len > context.limits.scan_rows) return error.SqlProgramLimitExceeded;
            if (external) {
                visited += page.rows.len;
                var first: usize = 0;
                while (first < page.rows.len) {
                    var decision_page = std.heap.ArenaAllocator.init(context.alloc);
                    defer decision_page.deinit();
                    const scratch = decision_page.allocator();
                    const cells = try @import("decision_eval.zig").rowPage(scratch, bound.input, page.rows[first..], context.limits.page_rows, context.limits.page_bytes);
                    try addRows(context, bound, grouped, scratch, cells);
                    first += cells.len;
                }
            } else for (page.rows) |row| {
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
    // Grouping is complete here: allocating for the requested limit when only
    // a few groups exist wastes memory (especially for scalar subqueries in a
    // large INSERT source) without changing which rows can be returned.
    var top = try operators.TopK.initWithSpill(context.alloc, @min(capacity, grouped.groupCount()), orders, context.limits.retained_bytes, context.spill);
    defer top.deinit();
    const decision = @import("decision_eval.zig");
    const external_results = decision.hasExternalPrograms(bound.outputs) or decision.hasExternalPrograms(bound.orders) or
        (if (bound.having) |*program| decision.hasExternal(program) else false);
    if (external_results) {
        const projection = try decision.SortedProjection.init(context.arena, bound.outputs, bound.orders, bound.order_outputs);
        try addGroupedDecisionPages(context, bound, grouped, &top, projection);
        return projection.finish(context, &top, offset, limit, statement.limit == null);
    } else while (true) {
        try context.checkpoint();
        var arena = std.heap.ArenaAllocator.init(context.alloc);
        defer arena.deinit();
        const alloc = arena.allocator();
        const groups = (try grouped.nextResultBatch(alloc, context.limits.executionRows(), false)) orelse break;
        const Cells = struct {
            groups: operators.GroupBatch,
            selection: ?[]const usize = null,
            fn cell(raw: *anyopaque, a: std.mem.Allocator, row: usize, column: usize) anyerror!Datum {
                const reader: *@This() = @ptrCast(@alignCast(raw));
                const index = if (reader.selection) |selected| selected[row] else row;
                return if (column < reader.groups.keys.width()) reader.groups.keys.cell(a, index, column) else reader.groups.aggregates.cell(a, index, column - reader.groups.keys.width());
            }
        };
        var reader: Cells = .{ .groups = groups };
        var batch: @import("execution_batch.zig").Batch = .{ .reader = .{ .ptr = &reader, .read = Cells.cell, .count = groups.ordinals.len, .width = groups.keys.width() + groups.aggregates.width() } };
        const predicates = if (bound.having) |*program| try resultValues(context, alloc, program, batch) else null;
        var selected: std.ArrayList(usize) = .empty;
        for (0..batch.len()) |row| {
            if (predicates) |values| {
                if (values[row].sql_null) continue;
                if (values[row].value != .bool) return error.SqlTypeMismatch;
                if (!values[row].value.bool) continue;
            }
            try selected.append(alloc, row);
        }
        reader.selection = selected.items;
        batch.reader.count = selected.items.len;
        const outputs = try alloc.alloc([]const Datum, bound.outputs.len);
        const ordering = try alloc.alloc([]const Datum, bound.orders.len);
        for (bound.outputs, outputs) |*program, *values| values.* = try resultValues(context, alloc, program, batch);
        for (bound.orders, ordering) |*program, *values| values.* = try resultValues(context, alloc, program, batch);
        // TopK owns admitted rows; reuse these small boundary slices rather
        // than allocating each group's input/output/key row matrix.
        const values = try alloc.alloc(Datum, bound.outputs.len);
        const keys = try alloc.alloc(Datum, bound.orders.len);
        for (selected.items, 0..) |physical, row| {
            for (outputs, values) |column, *value| value.* = column[row];
            for (ordering, keys) |column, *key| key.* = column[row];
            try top.add(.{ .values = values, .keys = keys, .ordinal = groups.ordinals[physical] });
        }
    }
    if (context.sink != null) return context.emitTop(&top, offset, limit, statement.limit == null);
    const ordered = try top.finishPage(context.arena, offset, limit + @intFromBool(statement.limit == null));
    const remaining = ordered.len;
    if (statement.limit == null and remaining > limit) return error.SqlResultTooLarge;
    const selected = ordered[0..@min(remaining, limit)];
    const rows = try context.arena.alloc([]const Json, selected.len);
    const nulls = try context.arena.alloc([]const bool, selected.len);
    const sources = try context.arena.alloc([]const ?*scalar.PatternSet, selected.len);
    for (selected, rows, nulls, sources) |row, *output, *null_row, *pattern_row| {
        const values = try context.arena.alloc(Json, row.values.len);
        const sql_nulls = try context.arena.alloc(bool, row.values.len);
        const patterns = try context.arena.alloc(?*scalar.PatternSet, row.values.len);
        for (row.values, patterns) |value, *pattern| pattern.* = value.patterns;
        pattern_row.* = patterns;
        for (row.values, values, sql_nulls) |value, *out, *sql_null| {
            out.* = try context.outputValue(value.value);
            sql_null.* = value.sql_null;
        }
        output.* = values;
        null_row.* = sql_nulls;
    }
    return .{ .columns = context.binding.columns, .rows = rows, .sql_nulls = nulls, .pattern_sources = sources, .command_tag = "SELECT" };
}

fn resultValues(context: anytype, a: std.mem.Allocator, program: *const scalar.Program, batch: @import("execution_batch.zig").Batch) ![]const Datum {
    if (try @import("vector_eval.zig").evaluateBatch(a, program, batch, context.parameters)) |values| return values;
    const values = try a.alloc(Datum, batch.len());
    for (values, 0..) |*value, row| value.* = try context.evaluate(a, program.*, try batch.row(a, row));
    return values;
}
