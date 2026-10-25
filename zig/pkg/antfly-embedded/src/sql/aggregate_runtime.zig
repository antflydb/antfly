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
const count_one = [_]Datum{Datum.json(.{ .integer = 1 })};

fn addRow(context: anytype, bound: *const binding.Bound, grouped: anytype, alloc: std.mem.Allocator, row: catalog.Row) !void {
    return addWeightedRow(context, bound, grouped, alloc, row, 1);
}
fn addWeightedRow(context: anytype, bound: *const binding.Bound, grouped: anytype, alloc: std.mem.Allocator, row: catalog.Row, weight: u64) !void {
    const cells = try bound.input.cells(alloc, row);
    return addCells(context, bound, grouped, alloc, cells, weight);
}

fn addCells(context: anytype, bound: *const binding.Bound, grouped: anytype, alloc: std.mem.Allocator, cells: []const Datum, weight: u64) !void {
    return addCellsMasked(context, bound, grouped, alloc, cells, catalog.Count.init(weight), null);
}
fn addCellsMasked(context: anytype, bound: *const binding.Bound, grouped: anytype, alloc: std.mem.Allocator, cells: []const Datum, weight: catalog.Count, active: ?[]const usize) !void {
    return addCellsOverridden(context, bound, grouped, alloc, cells, weight, active, null);
}
fn addCellsOverridden(context: anytype, bound: *const binding.Bound, grouped: anytype, alloc: std.mem.Allocator, cells: []const Datum, weight: catalog.Count, active: ?[]const usize, input: ?catalog.AggregateInput) !void {
    if (input) |argument| {
        var licensed = std.StaticBitSet(256).empty;
        for (active orelse return error.InvalidSqlBackendResponse) |slot| {
            if (slot >= bound.inputs.len or slot >= 256) return error.InvalidSqlBackendResponse;
            licensed.set(slot);
        }
        if (argument.batch.len == 0) {
            if (argument.slot >= 256 or !licensed.isSet(argument.slot)) return error.InvalidSqlBackendResponse;
        } else for (argument.batch) |part| {
            if (part.slot >= 256 or !licensed.isSet(part.slot) or !part.valid()) return error.InvalidSqlBackendResponse;
        }
    }
    if (!try bound.input.matchesWithLimits(alloc, cells, context.parameters, context.backend.decision_provider, @import("decision_eval.zig").limitsFor(context.backend))) return;
    const values = try alloc.alloc(Datum, bound.group_count);
    for (bound.input.projections[0..bound.group_count], values) |program, *value| value.* = try context.evaluate(alloc, program.?, cells);
    if (input) |packet| if (packet.batch.len != 0) {
        var selected: std.ArrayList(@import("cypher").statistic.Argument) = .empty;
        for (packet.batch) |argument| {
            if (bound.filters[argument.slot]) |slot| {
                const test_value = try context.evaluate(alloc, bound.input.projections[slot].?, cells);
                if (test_value.sql_null) continue;
                if (test_value.value != .bool) return error.SqlTypeMismatch;
                if (!test_value.value.bool) continue;
            }
            try selected.append(alloc, argument);
        }
        return grouped.addStatistics(values, selected.items, weight);
    };
    const inputs = try alloc.alloc(Datum, bound.inputs.len);
    for (bound.inputs, bound.filters, inputs, 0..) |index, filter, *value, aggregate_index| {
        value.* = .{};
        if (input) |argument| {
            if (argument.slot != aggregate_index) continue;
        } else if (active) |slots| if (std.mem.indexOfScalar(usize, slots, aggregate_index) == null) continue;
        if (filter) |slot| {
            const test_value = try context.evaluate(alloc, bound.input.projections[slot].?, cells);
            if (test_value.sql_null) continue;
            if (test_value.value != .bool) return error.SqlTypeMismatch;
            if (!test_value.value.bool) continue;
        }
        if (input) |argument| if (argument.support) |support| {
            const spec = bound.specs[aggregate_index];
            if (spec.kind != .count or !spec.distinct or spec.input_type != .integer) return error.InvalidSqlBackendResponse;
            return grouped.addSupport(values, aggregate_index, support);
        };
        value.* = if (input) |value_input| Datum.json(.{ .integer = value_input.value }) else if (index) |slot| try context.evaluate(alloc, bound.input.projections[slot].?, cells) else Datum.json(.{ .integer = 1 });
    }
    try grouped.addCount(values, inputs, weight);
}

fn addRows(context: anytype, bound: *const binding.Bound, grouped: anytype, alloc: std.mem.Allocator, cells: []const []const Datum) !void {
    const decision = @import("decision_eval.zig");
    const predicates = if (bound.input.predicate) |*program| try decision.evaluateBatchWithLimits(alloc, context.backend.decision_provider, program, cells, context.parameters, @import("decision_eval.zig").limitsFor(context.backend)) else null;
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
        const values = try decision.evaluateBatchWithLimits(alloc, context.backend.decision_provider, &optional.?, accepted.items, context.parameters, @import("decision_eval.zig").limitsFor(context.backend));
        for (keys, values) |key, value| key[k] = value;
    }
    for (bound.inputs, bound.filters, 0..) |index, filter, k| {
        const filters = if (filter) |slot| try decision.evaluateBatchWithLimits(alloc, context.backend.decision_provider, &bound.input.projections[slot].?, accepted.items, context.parameters, @import("decision_eval.zig").limitsFor(context.backend)) else null;
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
            const values = try decision.evaluateBatchWithLimits(alloc, context.backend.decision_provider, &bound.input.projections[slot].?, selected.items, context.parameters, @import("decision_eval.zig").limitsFor(context.backend));
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
pub fn addColumns(context: anytype, bound: *const binding.Bound, grouped: anytype, a: std.mem.Allocator, page: catalog.ColumnPage) !void {
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
    const Batch = @import("execution_batch.zig").Batch;
    const vector = @import("vector_eval.zig");
    const key_batches = try a.alloc(Batch, bound.group_count);
    const input_batches = try a.alloc(Batch, bound.inputs.len);
    var programs: std.ArrayList(*const scalar.Program) = .empty;
    var slots: std.ArrayList(usize) = .empty;
    for (bound.input.projections[0..bound.group_count]) |*optional| try programs.append(a, &optional.*.?);
    for (bound.inputs, bound.filters, 0..) |input, filter, index| if (filter == null) {
        if (input) |slot| {
            try programs.append(a, &bound.input.projections[slot].?);
            try slots.append(a, index);
        } else input_batches[index] = try constantBatch(a, accepted.selection.len);
    };
    const outputs = try vector.evaluateColumnsEncodedMany(a, programs.items, accepted, bound.input.columns, context.parameters, context.backend.execution_io);
    for (key_batches, outputs[0..key_batches.len], programs.items[0..key_batches.len]) |*batch, output, program| batch.* = output orelse try scalarBatch(context, bound, a, accepted, program);
    for (slots.items, outputs[key_batches.len..], programs.items[key_batches.len..]) |slot, output, program| input_batches[slot] = output orelse try scalarBatch(context, bound, a, accepted, program);
    // Each FILTER cohort evaluates inputs only for accepted physical lanes.
    // Native vectors remain typed through the SQL-NULL mask; unrelated
    // unfiltered aggregates keep their original physical capabilities.
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
        programs.clearRetainingCapacity();
        slots.clearRetainingCapacity();
        for (bound.inputs, bound.filters, 0..) |input, candidate, index| {
            if (candidate == null or completed[index]) continue;
            if (candidate.? != filter.? and !@import("typed_kernel.zig").sameProgram(filter_program, &bound.input.projections[candidate.?].?)) continue;
            completed[index] = true;
            if (input) |slot| {
                try programs.append(a, &bound.input.projections[slot].?);
                try slots.append(a, index);
            } else {
                const source = try a.create(Batch);
                source.* = try constantBatch(a, selected.selection.len);
                input_batches[index] = try source.mask(a, positions.items, accepted.selection.len);
            }
        }
        const encoded = try vector.evaluateColumnsEncodedMany(a, programs.items, selected, bound.input.columns, context.parameters, context.backend.execution_io);
        for (programs.items, encoded, slots.items) |program, output, slot| {
            const source = try a.create(Batch);
            source.* = output orelse try scalarBatch(context, bound, a, selected, program);
            input_batches[slot] = try source.mask(a, positions.items, accepted.selection.len);
        }
    }
    return grouped.addEncodedColumns(key_batches, input_batches, accepted.selection.len);
}

fn constantBatch(a: std.mem.Allocator, count: usize) !@import("execution_batch.zig").Batch {
    const indices = try a.alloc(u32, count);
    @memset(indices, 0);
    return .{ .dictionary = .{ .values = &count_one, .indices = indices } };
}
fn scalarBatch(context: anytype, bound: *const binding.Bound, a: std.mem.Allocator, page: catalog.ColumnPage, program: *const scalar.Program) !@import("execution_batch.zig").Batch {
    return .{ .vectors = .{ .values = try a.dupe([]const Datum, &.{try columnValues(context, bound, a, page, program)}), .count = page.selection.len } };
}

fn addGroupedDecisionPages(context: anytype, bound: *const binding.Bound, grouped: *@import("ordered_grouped.zig").Collector, top: *operators.TopK, projection: @import("decision_eval.zig").SortedProjection, goal: ?usize) !void {
    const decision = @import("decision_eval.zig");
    var exhausted = false;
    var delivered: usize = 0;
    while (!exhausted) {
        try context.checkpoint();
        var arena = std.heap.ArenaAllocator.init(context.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        var cells: std.ArrayList([]const Datum) = .empty;
        var ordinals: std.ArrayList(u64) = .empty;
        var bytes: usize = 0;
        const maximum = if (goal != null and bound.having == null) @min(context.limits.page_rows, goal.? - delivered) else context.limits.page_rows;
        while (cells.items.len < maximum) {
            const group = (try grouped.nextResult(context, a)) orelse {
                exhausted = true;
                break;
            };
            const grouped_width = group.keys.len + group.aggregates.len;
            const row = try a.alloc(Datum, grouped_width + bound.constant_count);
            @memcpy(row[0..group.keys.len], group.keys);
            @memcpy(row[group.keys.len..grouped_width], group.aggregates);
            @memcpy(row[grouped_width..], context.invocation_constants);
            try cells.append(a, row);
            try ordinals.append(a, group.ordinal);
            for (row) |cell| bytes +|= try operators.datumBytes(cell);
            if (bytes >= context.limits.page_bytes) break;
        }
        const predicates = if (bound.having) |*program| try decision.evaluateBatchWithLimits(a, context.backend.decision_provider, program, cells.items, context.parameters, @import("decision_eval.zig").limitsFor(context.backend)) else null;
        var accepted: std.ArrayList([]const Datum) = .empty;
        var positions: std.ArrayList(usize) = .empty;
        for (cells.items, 0..) |row, index| {
            if (predicates) |values| {
                if (values[index].sql_null) continue;
                if (values[index].value != .bool) return error.SqlTypeMismatch;
                if (!values[index].value.bool) continue;
            }
            if (goal) |limit| if (accepted.items.len == limit - delivered) break;
            try accepted.append(a, row);
            try positions.append(a, index);
        }
        const accepted_ordinals = try a.alloc(u64, positions.items.len);
        for (positions.items, accepted_ordinals) |index, *ordinal| ordinal.* = ordinals.items[index];
        try projection.add(context, a, top, accepted.items, accepted_ordinals);
        delivered += accepted.items.len;
        if (goal != null and delivered == goal.?) break;
    }
}

/// Only an exactly typed full key may select a persisted hash bucket. Mixed
/// comparison domains keep residual filtering without speculative pruning.
fn materializedPoint(a: std.mem.Allocator, recipe: @import("aggregate_materialization.zig").Recipe, conditions: []const catalog.Condition) !?[]const Datum {
    if (recipe.keys.len == 0) return null;
    const keys = try a.alloc(Datum, recipe.keys.len);
    for (recipe.keys, keys) |key, *out| {
        const condition = for (conditions) |condition| {
            if (std.mem.eql(u8, key.path, condition.column) and (condition.op == .eq or condition.op == .is_null)) break condition;
        } else return null;
        const value = if (condition.op == .is_null) Datum{} else condition.datum();
        if (!value.sql_null) {
            const valid = switch (key.type) {
                .integer => value.numeric == null and value.value == .integer,
                .string, .datetime => value.numeric == null and value.value == .string,
                .boolean => value.numeric == null and value.value == .bool,
                .number => if (key.element_type == .numeric) value.numeric != null else value.numeric == null and value.value == .float,
                else => false,
            };
            if (!valid) return null;
        }
        out.* = value;
    }
    return keys;
}

fn materializedOrder(a: std.mem.Allocator, bound: *const binding.Bound, recipe: @import("aggregate_materialization.zig").Recipe, requested: []const ast.Order) ![]const catalog.Scan.Order {
    if (requested.len == 0 or requested.len > recipe.keys.len or requested.len != bound.orders.len) return &.{};
    const result = try a.alloc(catalog.Scan.Order, requested.len);
    for (result, requested, bound.orders, 0..) |*order, source, program, index| {
        if (program.instructions.len != 1 or program.root != 0 or program.instructions[0].operation != .column or program.instructions[0].operation.column != index) return &.{};
        order.* = .{ .column = recipe.keys[index].path, .descending = source.descending, .nulls_first = source.nulls_first orelse source.descending };
    }
    return result;
}

fn loadMaterialized(context: anytype, bound: *const binding.Bound, grouped_owner: **operators.Grouped, table: catalog.Table, conditions: []const catalog.Condition, ordering: []const ast.Order, maximum_groups: ?usize) !bool {
    const open = context.backend.vtable.aggregate_partials orelse return false;
    const reuse = (try @import("aggregate_materialization.zig").reuseFromBound(context.arena, table, bound.*)) orelse return false;
    const recipe = reuse.recipe;
    const cursor = (try open(context.backend.ptr, context.alloc, table, recipe)) orelse return false;
    defer cursor.close(cursor.ptr);
    if ((bound.input.predicate == null or bound.group_count != 0) and for (bound.filters) |filter| {
        if (filter != null) break false;
    } else true) {
        if (cursor.adopt_final_groups) |adopt| {
            const Predicate = struct {
                context: @TypeOf(context),
                bound: *const binding.Bound,
                columns: []const u32,
                fn matches(raw: *anyopaque, a: std.mem.Allocator, keys: []const Datum) anyerror!bool {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    if (keys.len != self.columns.len) return error.InvalidSqlBackendResponse;
                    const cells = try a.alloc(Datum, self.bound.input.columns.len);
                    @memset(cells, .{});
                    for (self.columns, keys) |column, key| cells[column] = key;
                    return self.bound.input.matchesWithLimits(a, cells, self.context.parameters, self.context.backend.decision_provider, @import("decision_eval.zig").limitsFor(self.context.backend));
                }
            };
            var request: catalog.FinalGroupRequest = .{ .page_limit = context.limits.scan_pages, .row_limit = context.limits.scan_rows, .batch_rows = context.limits.executionRows(), .selection = .{ .unordered_prefix = ordering.len == 0, .maximum_groups = if (bound.input.predicate == null and bound.having == null) maximum_groups else null, .conditions = conditions, .order = try materializedOrder(context.arena, bound, recipe, ordering), .spill = context.spill, .memory_bytes = context.limits.retained_bytes / 4 } };
            if (bound.input.predicate != null) {
                const predicate = try context.arena.create(Predicate);
                predicate.* = .{ .context = context, .bound = bound, .columns = reuse.key_columns };
                request.predicate = .{ .ptr = predicate, .matches = Predicate.matches };
                request.point = try materializedPoint(context.arena, recipe, conditions);
            }
            if (try adopt(cursor.ptr, grouped_owner.*, request)) return true;
        }
        if (bound.input.predicate == null) if (cursor.take_final_groups) |take| if (try take(cursor.ptr)) |final| {
            grouped_owner.*.deinit();
            grouped_owner.* = final;
            return true;
        };
    }
    const grouped = grouped_owner.*;
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
        var merge_arena = std.heap.ArenaAllocator.init(context.alloc);
        defer merge_arena.deinit();
        for (partials) |partial| {
            _ = merge_arena.reset(.free_all);
            if (partial.keys.len != recipe.keys.len) return error.InvalidSqlBackendResponse;
            const a = merge_arena.allocator();
            const cells = try a.alloc(Datum, bound.input.columns.len);
            @memset(cells, .{});
            for (reuse.key_columns, partial.keys) |column, key| {
                if (column >= cells.len) return error.InvalidSqlBackendResponse;
                cells[column] = key;
            }
            if (!try bound.input.matchesWithLimits(a, cells, context.parameters, context.backend.decision_provider, @import("decision_eval.zig").limitsFor(context.backend))) continue;
            const partial_count = try @import("aggregate_partial.zig").partialCount(partial);
            if (partial.aggregate_slots) |slots| {
                if (slots.len != partial_count) return error.InvalidSqlBackendResponse;
            } else if (partial_count != bound.specs.len) return error.InvalidSqlBackendResponse;
            var typed: ?[]const operators.Aggregate = if (partial.state_reader) |reader| materialize: {
                const states = try a.alloc(operators.Aggregate, reader.count);
                for (states, 0..) |*state, index| {
                    const slot = if (partial.aggregate_slots) |slots| slots[index] else index;
                    if (slot >= bound.specs.len) return error.InvalidSqlBackendResponse;
                    state.* = try @import("aggregate_partial.zig").read(a, partial, index, bound.specs[slot]);
                }
                break :materialize states;
            } else partial.typed_states;
            var adjusted: ?[]operators.Aggregate = null;
            const aggregates: []Datum = if (typed != null) &.{} else try a.dupe(Datum, partial.aggregates);
            for (0..partial_count) |i| {
                const slot = if (partial.aggregate_slots) |slots| slots[i] else i;
                if (slot >= bound.filters.len) return error.InvalidSqlBackendResponse;
                if (bound.filters[slot]) |filter| {
                    const test_value = try context.evaluate(a, bound.input.projections[filter].?, cells);
                    if (!test_value.sql_null and test_value.value != .bool) return error.SqlTypeMismatch;
                    if (test_value.sql_null or !test_value.value.bool) {
                        const spec = bound.specs[slot];
                        var empty = try operators.Aggregate.initTyped(a, spec.kind, spec.input_type, spec.input_element);
                        defer if (typed == null) empty.deinit();
                        empty.distinct = spec.distinct;
                        if (typed) |states| {
                            if (adjusted == null) {
                                adjusted = try a.dupe(operators.Aggregate, states);
                                typed = adjusted.?;
                            }
                            adjusted.?[i] = empty;
                        } else aggregates[i] = try @import("aggregate_partial.zig").cell(a, empty);
                    }
                }
            }
            if (typed) |states| {
                try grouped.importTypedPartialMapped(partial.keys, states, partial.aggregate_slots, partial.ordinal);
            } else try grouped.importPartialMapped(partial.keys, aggregates, partial.aggregate_slots, partial.ordinal);
        }
    }
    return true;
}

fn supportsWeights(bound: *const binding.Bound) bool {
    if (bound.ordered.len != 0) return false;
    for (bound.specs) |spec| {
        if (spec.kind == .pattern_set) return false;
        if (!spec.distinct and (spec.kind == .sum or spec.kind == .avg) and spec.input_type != .integer and spec.input_element != .numeric) {
            // AVG can retain an exact weighted binary64 total. SUM keeps its
            // ordered overflow behavior and REAL keeps its own rounding rules.
            if (spec.kind != .avg or spec.input_type != .number or (spec.input_element != null and spec.input_element != .float64)) return false;
        }
    }
    if (bound.input.predicate) |program| if (!program.repeatable()) return false;
    for (bound.input.projections) |program| if (program) |value| if (!value.repeatable()) return false;
    for (bound.input.required) |ordinal| if (std.mem.eql(u8, bound.input.columns[ordinal].name, "_id")) return false;
    return true;
}

/// Negotiate presence only when every reducer ignores duplicate source rows.
/// A single bag-sensitive reducer keeps exact weights for the whole scan.
fn multiplicity(bound: *const binding.Bound) catalog.Multiplicity {
    for (bound.specs) |spec| {
        if (spec.distinct) continue;
        switch (spec.kind) {
            .min, .max, .bool_and, .bool_or => {},
            .count, .sum, .avg, .pattern_set => return .exact,
        }
    }
    return .presence;
}

const LinearConsumer = struct {
    program: scalar.Program,
    columns: []const scalar.Column,
    integers: []const ?i64,
    scalars: []const ?@import("cypher").linear_numeric.Lattice,
    total_only: bool,
    fn validate(ptr: *const anyopaque, a: std.mem.Allocator, fields: []const @import("cypher").linear_bounds.Field) !bool {
        const self: *const @This() = @ptrCast(@alignCast(ptr));
        if (try self.program.numericProof(a, self.columns, fields, self.scalars)) |proof| return !self.total_only or proof.total();
        return !self.total_only and self.program.linearBounds(self.columns, fields, self.integers) != null;
    }
};
const StatisticConsumer = struct {
    program: scalar.Program,
    columns: []const scalar.Column,
    integers: []const ?i64,
    fn validate(_: *const anyopaque, _: std.mem.Allocator, _: []const @import("cypher").linear_bounds.Field) !bool {
        return false;
    }
    fn proof(ptr: *const anyopaque, a: std.mem.Allocator, fields: []const @import("cypher").linear_bounds.Field) !?@import("cypher").statistic.Form {
        const self: *const @This() = @ptrCast(@alignCast(ptr));
        return self.program.statisticProof(a, self.columns, fields, self.integers);
    }
};
/// Bind the entire typed admission program, including predicates represented
/// as scalar ASTs. A partial scan hint never licenses scalar observation.
fn observerFilter(context: anytype, table: catalog.Table, bound: *const binding.Bound) !?catalog.Scan.GraphFilter {
    const program = bound.input.predicate orelse return null;
    const observers = @import("observer_request.zig");
    const filter = (try observers.fromProgram(context.arena, program, bound.input.columns, context.parameters)) orelse return null;
    const from = try context.arena.alloc([]const u8, bound.input.columns.len);
    const to = try context.arena.alloc([]const u8, bound.input.columns.len);
    for (bound.input.columns, from, to) |column, *source, *target| {
        source.* = column.name;
        target.* = (try table.column(column.name)).path;
    }
    return observers.mapFilter(context.arena, filter, from, to);
}
fn aggregateRequests(a: std.mem.Allocator, bound: *const binding.Bound, table: catalog.Table, conditions: []const catalog.Condition, parameters: []const Json, predicate_complete: bool, exact_filter: ?catalog.Scan.GraphFilter) ![]const catalog.AggregateWeight {
    const integers = try a.alloc(?i64, parameters.len);
    for (parameters, integers) |parameter, *integer| integer.* = if (parameter == .integer) parameter.integer else null;
    const scalars = try a.alloc(?@import("cypher").linear_numeric.Lattice, parameters.len);
    for (parameters, scalars) |parameter, *value| value.* = switch (parameter) {
        .integer => |n| .integer(n),
        .float => |n| @import("cypher").linear_numeric.Lattice.number(n),
        else => null,
    };
    const requests = try a.alloc(catalog.AggregateWeight, bound.specs.len);
    var stable: std.StringHashMapUnmanaged(void) = .empty;
    for (conditions) |condition| try stable.put(a, condition.column, {});
    for (bound.input.projections[0..bound.group_count]) |optional| if (optional) |program| for (program.required_columns) |ordinal| {
        try stable.put(a, (try table.column(bound.input.columns[ordinal].name)).path, {});
    };
    if (bound.input.predicate) |program| for (program.required_columns) |ordinal| {
        try stable.put(a, (try table.column(bound.input.columns[ordinal].name)).path, {});
    };
    for (bound.specs, bound.inputs, bound.filters, requests) |spec, input, filter, *request| {
        var fields: std.StringHashMapUnmanaged(void) = .empty;
        for (conditions) |condition| try fields.put(a, condition.column, {});
        const programs = [_]?scalar.Program{
            bound.input.predicate,
            if (input) |slot| bound.input.projections[slot] else null,
            if (filter) |slot| bound.input.projections[slot] else null,
        };
        for (programs) |optional| if (optional) |program| for (program.required_columns) |ordinal| {
            try fields.put(a, (try table.column(bound.input.columns[ordinal].name)).path, {});
        };
        var names: std.ArrayList([]const u8) = .empty;
        var keys = fields.keyIterator();
        while (keys.next()) |key| try names.append(a, key.*);
        const presence = spec.distinct or spec.kind == .min or spec.kind == .max or spec.kind == .bool_and or spec.kind == .bool_or;
        var invariant = try stable.clone(a);
        if (filter) |slot| if (bound.input.projections[slot]) |program| for (program.required_columns) |ordinal| {
            try invariant.put(a, (try table.column(bound.input.columns[ordinal].name)).path, {});
        };
        var summary: ?[]const u8 = null;
        var summaries: std.ArrayList(catalog.SummaryCertificate) = .empty;
        if (input) |slot| if (bound.input.projections[slot]) |program| {
            if (program.output_type.kind == .integer and program.statisticCandidate() and (spec.kind == .count or spec.kind == .sum or spec.kind == .avg or spec.kind == .min or spec.kind == .max) and (!spec.distinct or spec.kind == .count)) {
                const consumer = try a.create(StatisticConsumer);
                const columns = try a.dupe(scalar.Column, bound.input.columns);
                for (program.required_columns) |ordinal| columns[ordinal].name = (try table.column(columns[ordinal].name)).path;
                consumer.* = .{ .program = program, .columns = columns, .integers = integers };
                var statistic_stable: std.ArrayList([]const u8) = .empty;
                var stable_keys = invariant.keyIterator();
                while (stable_keys.next()) |key| try statistic_stable.append(a, key.*);
                if (program.required_columns.len != 0) try summaries.append(a, .{ .field = columns[program.required_columns[0]].name, .invariants = statistic_stable.items, .joint = true, .linear = .{ .ptr = consumer, .validate = StatisticConsumer.validate, .integers = integers, .statistic = StatisticConsumer.proof, .statistic_kind = if (spec.distinct) .distinct else switch (spec.kind) {
                    .count => .count,
                    .sum => .sum,
                    .avg => .avg,
                    .min => .min,
                    .max => .max,
                    else => unreachable,
                }, .statistic_global = bound.group_count == 0 and invariant.count() == 0 } });
            }
        };
        if (!spec.distinct and
            (spec.kind == .count or spec.kind == .sum or spec.kind == .avg or spec.kind == .min or spec.kind == .max))
        {
            if (input) |slot| if (bound.input.projections[slot]) |program| {
                if (program.required_columns.len != 0 and (program.affineTogether() or program.numericLinearCandidate())) {
                    var stable_fields: std.ArrayList([]const u8) = .empty;
                    var stable_inputs = invariant.keyIterator();
                    while (stable_inputs.next()) |key| try stable_fields.append(a, key.*);
                    var linear: ?@import("cypher").linear_bounds.Check = null;
                    {
                        const consumer = try a.create(LinearConsumer);
                        const columns = try a.dupe(scalar.Column, bound.input.columns);
                        for (program.required_columns) |ordinal| columns[ordinal].name = (try table.column(columns[ordinal].name)).path;
                        consumer.* = .{ .program = program, .columns = columns, .integers = integers, .scalars = scalars, .total_only = spec.kind == .min or spec.kind == .max };
                        linear = .{ .ptr = consumer, .validate = LinearConsumer.validate, .integers = integers, .scalars = scalars, .total_only = consumer.total_only, .numeric_required = !program.affineTogether() };
                    }
                    try summaries.append(a, .{ .field = (try table.column(bound.input.columns[program.required_columns[0]].name)).path, .invariants = stable_fields.items, .joint = true, .linear = linear });
                }
                if (program.affineColumn()) |column| {
                    summary = (try table.column(bound.input.columns[column].name)).path;
                } else if (program.required_columns.len <= 256) {
                    for (program.required_columns) |candidate| {
                        if (!program.affineRelativeTo(candidate)) continue;
                        var dependencies = try invariant.clone(a);
                        for (program.required_columns) |ordinal| if (ordinal != candidate) {
                            try dependencies.put(a, (try table.column(bound.input.columns[ordinal].name)).path, {});
                        };
                        var stable_fields: std.ArrayList([]const u8) = .empty;
                        var stable_inputs = dependencies.keyIterator();
                        while (stable_inputs.next()) |key| try stable_fields.append(a, key.*);
                        try summaries.append(a, .{ .field = (try table.column(bound.input.columns[candidate].name)).path, .invariants = stable_fields.items });
                    }
                }
            };
        }
        var invariants: std.ArrayList([]const u8) = .empty;
        var stable_keys = invariant.keyIterator();
        while (stable_keys.next()) |key| {
            try invariants.append(a, key.*);
            if (!fields.contains(key.*)) try names.append(a, key.*);
        }
        var observer: ?@import("cypher").runtime.NodeCursor.ScalarObserver = null;
        var observer_field: ?[]const u8 = null;
        var observer_keys: std.ArrayList([]const u8) = .empty;
        var direct_keys = true;
        for (bound.input.projections[0..bound.group_count]) |optional| {
            const program = optional orelse {
                direct_keys = false;
                break;
            };
            const root = program.instructions[program.root];
            if (root.operation != .column or root.input_function) {
                direct_keys = false;
                break;
            }
            try observer_keys.append(a, (try table.column(bound.input.columns[root.operation.column].name)).path);
        }
        var reducer_filter: ?catalog.Scan.GraphFilter = null;
        if (filter) |slot| {
            if (bound.input.projections[slot]) |program| {
                const observers = @import("observer_request.zig");
                if (try observers.fromProgram(a, program, bound.input.columns, parameters)) |predicate| {
                    const from = try a.alloc([]const u8, bound.input.columns.len);
                    const to = try a.alloc([]const u8, bound.input.columns.len);
                    for (bound.input.columns, from, to) |column, *source, *target| {
                        source.* = column.name;
                        target.* = (try table.column(column.name)).path;
                    }
                    reducer_filter = try observers.mapFilter(a, predicate, from, to);
                }
            }
        }
        if (direct_keys and predicate_complete and (filter == null or reducer_filter != null)) if (input) |slot| if (bound.input.projections[slot]) |program| {
            const root = program.instructions[program.root];
            if (root.operation == .column and !root.input_function) {
                observer_field = (try table.column(bound.input.columns[root.operation.column].name)).path;
                observer = switch (spec.kind) {
                    .min => .min,
                    .max => .max,
                    .count => if (spec.distinct) .distinct else null,
                    else => null,
                };
            }
        };
        request.* = .{ .observer_reducer_filter = reducer_filter, .observer_field = observer_field, .observer_keys = observer_keys.items, .observer_filter = exact_filter, .observer = observer, .fields = names.items, .multiplicity = if (presence) .presence else .exact, .summary = summary, .invariants = invariants.items, .summaries = summaries.items };
    }
    return requests;
}

pub fn execute(context: anytype, statement: ast.Select) !@import("runtime.zig").Output {
    const bound = context.binding.aggregate orelse return error.InvalidSqlBackendResponse;
    if (context.invocation_constants.len != bound.constant_count) return error.InvalidSqlBackendResponse;
    try bound.input.validateDecisions(context.arena, context.parameters, context.backend.decision_provider);
    var external = if (bound.input.predicate) |*program| @import("decision_eval.zig").hasExternal(program) else false;
    for (bound.input.projections) |optional| if (optional) |*program| {
        external = external or @import("decision_eval.zig").hasExternal(program);
    };
    const limit = statement.capRows(try context.count(statement.limit, context.limits.result_rows));
    const offset = try context.offsetCount(statement.offset);
    if (limit > context.limits.result_rows or offset > context.limits.scan_rows) return error.SqlProgramLimitExceeded;
    if (limit == 0) return .{ .columns = context.binding.columns, .command_tag = "SELECT" };
    var grouped = try operators.Grouped.create(context.alloc, bound.specs, .{ .groups = context.limits.scan_rows, .bytes = context.limits.retained_bytes / 2, .spill = context.spill });
    defer grouped.deinit();
    // Backends without an Io still support bounded in-memory execution. A
    // zero-byte manager rejects spill admission before touching the filesystem.
    var memory_manager: @import("spill.zig").Manager = .{ .alloc = context.alloc, .io = .failing, .context = context.backend.ptr, .checkpoint = context.backend.vtable.checkpoint, .max_bytes = 0, .async_writes = false };
    defer memory_manager.deinit();
    var collector = try @import("ordered_grouped.zig").Collector.init(grouped, bound, context.spill orelse &memory_manager, context.limits.retained_bytes / 2);
    defer collector.deinit();
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
        var native_quota: @import("native_scan_quota.zig").Quota = .{ .capacity = context.limits.executionRows(), .row_limit = context.limits.scan_rows, .page_limit = context.limits.scan_pages };
        var aggregate_loaded = if (!predicates.empty) try loadMaterialized(context, bound, &grouped, table, predicates.terms.items, statement.order_by, if (statement.limit != null) std.math.add(usize, offset, limit) catch return error.SqlProgramLimitExceeded else null) else false;
        collector.grouped = grouped;
        const count_only = bound.group_count == 0 and bound.input.predicate == null and predicates.terms.items.len == 0 and predicates.primary_key == null and for (bound.specs, bound.inputs, bound.filters) |spec, input, filter| {
            if (spec.kind != .count or spec.distinct or input != null or filter != null) break false;
        } else true;
        if (!aggregate_loaded and count_only and !predicates.empty) if (try scan.count(context, table, .{ .fields = fields[0..field_count], .limit = context.limits.page_rows })) |count| {
            try grouped.addGlobalCount(count);
            aggregate_loaded = true;
        };
        const exact_filter = try observerFilter(context, table, bound);
        const observer_admission = bound.input.predicate == null or exact_filter != null;
        const can_weight = !aggregate_loaded and !predicates.empty and !external and supportsWeights(bound);
        const requests: []const catalog.AggregateWeight = if (can_weight) try aggregateRequests(context.arena, bound, table, predicates.terms.items, context.parameters, observer_admission, exact_filter) else &.{};
        if (requests.len != 0 and statement.limit != null and bound.group_count != 0 and bound.having == null and bound.orders.len == bound.group_count and statement.order_by.len == bound.group_count) window: {
            for (requests) |request| if (request.observer == null) break :window;
            for (bound.outputs) |program| {
                const root = program.instructions[program.root];
                if (root.input_function or (root.operation != .column and root.operation != .literal)) break :window;
            }
            const directions = try context.arena.alloc(bool, bound.group_count);
            const keys = try context.arena.alloc([]const u8, bound.group_count);
            var seen = std.AutoHashMapUnmanaged(u32, void){};
            for (bound.orders, statement.order_by, directions, keys) |program, order, *descending, *key| {
                const root = program.instructions[program.root];
                if (root.input_function or root.operation != .column or root.operation.column >= bound.group_count or seen.contains(root.operation.column)) break :window;
                if ((order.nulls_first orelse order.descending) != order.descending) break :window;
                try seen.put(context.arena, root.operation.column, {});
                descending.* = order.descending;
                key.* = requests[0].observer_keys[root.operation.column];
            }
            const maximum = std.math.add(u64, offset, limit) catch return error.SqlProgramLimitExceeded;
            for (@constCast(requests)) |*request| {
                request.observer_keys = keys;
                request.observer_window = .{ .descending = directions, .maximum = maximum };
            }
        }
        const partitioned = can_weight and bound.specs.len > 0 and
            try scan.aggregateWeights(context, table, .{ .fields = fields[0..field_count], .primary_key = predicates.primary_key, .conditions = predicates.terms.items, .limit = context.limits.page_rows }, requests);
        const weighted = partitioned or (can_weight and try scan.weights(context, table, .{ .fields = fields[0..field_count], .primary_key = predicates.primary_key, .conditions = predicates.terms.items, .limit = context.limits.page_rows }, multiplicity(bound)));
        const parallel = !weighted and bound.ordered.len == 0 and !external and !predicates.empty and !aggregate_loaded and try @import("parallel_aggregate.zig").execute(context, bound, grouped, &scan, table, .{ .fields = fields[0..field_count], .primary_key = predicates.primary_key, .conditions = predicates.terms.items, .limit = context.limits.executionRows() });
        while (!predicates.empty and !aggregate_loaded and !parallel) {
            try context.checkpoint();
            var arena = std.heap.ArenaAllocator.init(context.alloc);
            defer arena.deinit();
            if (!external and !weighted) if (try scan.columns(context, arena.allocator(), table, .{ .fields = fields[0..field_count], .primary_key = predicates.primary_key, .conditions = predicates.terms.items, .after = after, .limit = context.limits.executionRows() })) |column_page| {
                if (column_page.selection.len > context.limits.executionRows()) return error.InvalidSqlBackendResponse;
                try native_quota.admit(column_page.selection.len, column_page.after == null);
                try addColumns(context, bound, &collector, arena.allocator(), column_page);
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
            try page.validateWeights();
            pages += 1;
            if (pages > context.limits.scan_pages) return error.SqlProgramLimitExceeded;
            if (page.rows.len > context.limits.page_rows) return error.InvalidSqlBackendResponse;
            if (page.multiplicities) |weights| if (!weighted or weights.len != page.rows.len) return error.InvalidSqlBackendResponse;
            if (page.aggregate_slots) |slots| {
                if (!partitioned or slots.len == 0 or page.multiplicities == null) return error.InvalidSqlBackendResponse;
                for (slots, 0..) |slot, i| {
                    if (slot >= bound.specs.len or std.mem.indexOfScalar(usize, slots[0..i], slot) != null) return error.InvalidSqlBackendResponse;
                }
            } else if (partitioned) return error.InvalidSqlBackendResponse;
            if (visited + page.rows.len > context.limits.scan_rows) return error.SqlProgramLimitExceeded;
            if (external) {
                visited += page.rows.len;
                var first: usize = 0;
                while (first < page.rows.len) {
                    var decision_page = std.heap.ArenaAllocator.init(context.alloc);
                    defer decision_page.deinit();
                    const scratch = decision_page.allocator();
                    const cells = try @import("decision_eval.zig").rowPage(scratch, bound.input, page.rows[first..], context.limits.page_rows, context.limits.page_bytes);
                    try addRows(context, bound, &collector, scratch, cells);
                    first += cells.len;
                }
            } else for (page.rows, 0..) |row, row_index| {
                try context.checkpoint();
                visited += 1;
                if (visited > context.limits.scan_rows) return error.SqlProgramLimitExceeded;
                const cells = try bound.input.cells(arena.allocator(), row);
                try addCellsOverridden(context, bound, &collector, arena.allocator(), cells, page.weight(row_index), page.aggregate_slots, if (page.aggregate_inputs) |inputs| inputs[row_index] else null);
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
        try addRow(context, bound, &collector, arena.allocator(), .{ .id = "", .version = 0, .value = .{ .object = .empty } });
    }
    const orders = try context.arena.alloc(operators.Order, statement.order_by.len);
    for (statement.order_by, orders) |order, *out| out.* = .{ .descending = order.descending, .nulls_first = order.nulls_first };
    const capacity = std.math.add(usize, offset, limit + @intFromBool(statement.limit == null)) catch return error.SqlProgramLimitExceeded;
    // Grouping is complete here: allocating for the requested limit when only
    // a few groups exist wastes memory (especially for scalar subqueries in a
    // large INSERT source) without changing which rows can be returned.
    const decision = @import("decision_eval.zig");
    const external_results = decision.hasExternalPrograms(bound.outputs) or decision.hasExternalPrograms(bound.orders) or
        (if (bound.having) |*program| decision.hasExternal(program) else false);
    const source_ordered = grouped.finalized != null and bound.ordered.len == 0 and (statement.order_by.len == 0 or grouped.finalized.?.ordered);
    const direct_delivery = source_ordered and !external_results;
    var top: ?operators.TopK = if (direct_delivery) null else try operators.TopK.initWithSpill(context.alloc, @min(capacity, grouped.groupCount()), orders, context.limits.retained_bytes, context.spill);
    defer if (top) |*owner| owner.deinit();
    var direct: std.ArrayList(operators.Row) = .empty;
    var direct_emitted: usize = 0;
    if (external_results) {
        const projection = try decision.SortedProjection.init(context.arena, bound.outputs, bound.orders, bound.order_outputs);
        const goal = if (source_ordered and !decision.hasExternalPrograms(bound.orders) and statement.limit != null) capacity else null;
        try addGroupedDecisionPages(context, bound, &collector, &top.?, projection, goal);
        return projection.finish(context, &top.?, offset, limit, statement.limit == null);
    }
    const early_limit = direct_delivery;
    var delivered: usize = 0;
    result_pages: while (true) {
        try context.checkpoint();
        var arena = std.heap.ArenaAllocator.init(context.alloc);
        defer arena.deinit();
        const alloc = arena.allocator();
        const maximum: u32 = @intCast(if (early_limit and bound.having == null) @min(context.limits.executionRows(), capacity - delivered) else context.limits.executionRows());
        const groups: operators.GroupBatch = if (bound.ordered.len == 0)
            (try grouped.nextResultBatch(alloc, maximum, false)) orelse break
        else ordered: {
            const group = (try collector.nextResult(context, alloc)) orelse break;
            const keys = try alloc.alloc([]const Datum, 1);
            keys[0] = group.keys;
            const aggregates = try alloc.alloc([]const Datum, 1);
            aggregates[0] = group.aggregates;
            const ordinals = try alloc.alloc(u64, 1);
            ordinals[0] = group.ordinal;
            break :ordered .{ .keys = .{ .rows = keys }, .aggregates = .{ .rows = aggregates }, .ordinals = ordinals };
        };
        const Cells = struct {
            groups: operators.GroupBatch,
            constants: []const Datum,
            selection: ?[]const usize = null,
            fn cell(raw: *anyopaque, a: std.mem.Allocator, row: usize, column: usize) anyerror!Datum {
                const reader: *@This() = @ptrCast(@alignCast(raw));
                const index = if (reader.selection) |selected| selected[row] else row;
                const width = reader.groups.keys.width() + reader.groups.aggregates.width();
                if (column >= width) return reader.constants[column - width];
                return if (column < reader.groups.keys.width()) reader.groups.keys.cell(a, index, column) else reader.groups.aggregates.cell(a, index, column - reader.groups.keys.width());
            }
        };
        var reader: Cells = .{ .groups = groups, .constants = context.invocation_constants };
        var batch: @import("execution_batch.zig").Batch = .{ .reader = .{ .ptr = &reader, .read = Cells.cell, .count = groups.ordinals.len, .width = groups.keys.width() + groups.aggregates.width() + bound.constant_count } };
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
        if (direct_delivery) {
            const skipped = @min(selected.items.len, offset -| delivered);
            const count = @min(selected.items.len - skipped, (limit + @intFromBool(statement.limit == null)) - direct_emitted);
            delivered += selected.items.len;
            selected.items = selected.items[skipped..][0..count];
            if (statement.limit == null and count > limit - direct_emitted) return error.SqlResultTooLarge;
        }
        reader.selection = selected.items;
        batch.reader.count = selected.items.len;
        const outputs = try alloc.alloc([]const Datum, bound.outputs.len);
        const ordering = try alloc.alloc([]const Datum, if (direct_delivery) 0 else bound.orders.len);
        for (bound.outputs, outputs) |*program, *values| values.* = try resultValues(context, alloc, program, batch);
        if (!direct_delivery) {
            for (bound.orders, ordering) |*program, *values| values.* = try resultValues(context, alloc, program, batch);
        }
        if (direct_delivery) if (context.sink) |sink| if (sink.append_batch) |append| {
            if (selected.items.len != 0) try append(sink.ptr, .{ .vectors = .{ .values = outputs, .count = selected.items.len } });
            direct_emitted += selected.items.len;
            if (delivered >= capacity) break :result_pages;
            continue;
        };
        // TopK owns admitted rows; reuse these small boundary slices rather
        // than allocating each group's input/output/key row matrix.
        const values = try alloc.alloc(Datum, bound.outputs.len);
        const keys = try alloc.alloc(Datum, ordering.len);
        for (selected.items, 0..) |physical, row| {
            for (outputs, values) |column, *value| value.* = column[row];
            for (ordering, keys) |column, *key| key.* = column[row];
            if (direct_delivery) {
                if (context.sink) |sink| try sink.append(sink.ptr, values) else {
                    const owned = try context.arena.alloc(Datum, values.len);
                    for (owned, values) |*out, value| out.* = try operators.cloneDatum(context.arena, value);
                    try direct.append(context.arena, .{ .values = owned, .keys = &.{}, .ordinal = groups.ordinals[physical] });
                }
                direct_emitted += 1;
            } else try top.?.add(.{ .values = values, .keys = keys, .ordinal = groups.ordinals[physical] });
        }
        if (early_limit and delivered >= capacity) break :result_pages;
    }
    if (context.sink != null) return if (direct_delivery) .{ .columns = context.binding.columns, .command_tag = "SELECT" } else context.emitTop(&top.?, offset, limit, statement.limit == null);
    const ordered = if (direct_delivery) direct.items else try top.?.finishPage(context.arena, offset, limit + @intFromBool(statement.limit == null));
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
        for (row.values, values, sql_nulls, bound.outputs) |value, *out, *sql_null, program| {
            out.* = try context.outputDatum(value, program.output_type.kind, program.output_type.element_type);
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
