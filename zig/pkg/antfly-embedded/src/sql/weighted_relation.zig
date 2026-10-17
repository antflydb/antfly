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

//! Explicit bag-preserving relational adapters. Prove the whole pipeline before
//! negotiating its physical cursor; no producer is advanced during planning.
const std = @import("std");
const binding = @import("relation_binding.zig");
const catalog = @import("catalog.zig");
const scalar = @import("scalar.zig");
const describe = @import("describe.zig");
const A = std.mem.Allocator;
const Names = std.StringHashMapUnmanaged(void);

pub const Plan = struct {
    const Stage = struct { node: *const binding.Node, fields: []const []const []const u8 };
    source: *const binding.Node,
    stages: []const Stage,
    physical: []const catalog.AggregateWeight,
    width: usize,

    pub fn init(a: A, root: *const binding.Node, requests: []const catalog.AggregateWeight) !?Plan {
        var node = root;
        var mapped = requests;
        var stages: std.ArrayList(Stage) = .empty;
        var width: usize = 0;
        while (node.operation == .query) {
            if (stages.items.len == 64) return null;
            const query = node.operation.query;
            if (query.binding.aggregate != null or query.binding.window != null or query.statement.count_all or
                query.binding.order_keys.len != 0 or query.statement.limit != null or query.statement.offset != null or
                query.statement.scalar_cardinality_limit or query.constant_refs.len != 0 or
                query.statement.columns.len != node.columns.len or query.binding.columns.len != node.columns.len) return null;
            const bound = query.binding.scalars;
            if (bound.predicate) |p| if (!p.repeatable()) return null;
            for (bound.projections) |optional| if (optional) |p| if (!p.repeatable()) return null;
            var covered: Names = .empty;
            for (mapped) |request| for (request.fields) |field| try covered.put(a, field, {});
            const inputs = try a.alloc(catalog.AggregateWeight, mapped.len);
            const outputs = try a.alloc([]const []const u8, mapped.len);
            for (mapped, inputs, outputs) |request, *input, *output| {
                var wanted: Names = .empty;
                for (request.fields) |field| {
                    if (columnIndex(node, field) == null) return null;
                    try wanted.put(a, field, {});
                }
                var dependencies: Names = .empty;
                if (bound.predicate) |p| try programFields(a, &dependencies, p, bound.columns);
                for (node.columns, query.statement.columns, query.binding.columns, 0..) |column, projection, definition, index| {
                    const program = if (index < bound.projections.len) bound.projections[index] else null;
                    const direct = if (program) |p| blk: {
                        const instruction = p.instructions[p.root];
                        break :blk !instruction.input_function and (instruction.operation == .column or instruction.operation == .literal) and
                            p.output_type.kind == definition.type and p.output_type.element_type == definition.element_type and
                            scalar.NumericModifier.eql(p.output_type.numeric_modifier, definition.numeric_modifier);
                    } else blk: {
                        const source_index = columnIndex(query.source, projection.field) orelse return null;
                        const source = query.source.columns[source_index];
                        break :blk source.type == definition.type and source.element_type == definition.element_type and
                            scalar.NumericModifier.eql(source.numeric_modifier, definition.numeric_modifier);
                    };
                    // A demanded computed output is checked by its assigned
                    // reducer pass. Truly unused computations must still run;
                    // keep their error dependencies in every candidate pass.
                    if (!direct and !covered.contains(column.internal)) try wanted.put(a, column.internal, {});
                    if (!wanted.contains(column.internal)) continue;
                    if (program) |p| {
                        try programFields(a, &dependencies, p, bound.columns);
                    } else {
                        if (columnIndex(query.source, projection.field) == null) return null;
                        try dependencies.put(a, projection.field, {});
                    }
                }
                output.* = try names(a, wanted);
                input.* = .{ .fields = try names(a, dependencies), .multiplicity = request.multiplicity };
            }
            try stages.append(a, .{ .node = node, .fields = outputs });
            width += node.columns.len;
            mapped = inputs;
            node = query.source;
        }
        if (node.operation != .scan) return null;
        const physical = try a.alloc(catalog.AggregateWeight, mapped.len);
        for (mapped, physical) |request, *out| {
            const fields = try a.alloc([]const u8, request.fields.len);
            for (request.fields, fields) |field, *name| {
                const index = columnIndex(node, field) orelse return null;
                name.* = node.operation.scan.source_columns[index];
            }
            out.* = .{ .fields = fields, .multiplicity = request.multiplicity };
        }
        return .{ .source = node, .stages = stages.items, .physical = physical, .width = width + node.columns.len };
    }

    pub fn project(self: Plan, a: A, original_context: anytype, initial: catalog.Row, slots: ?[]const usize) !?catalog.Row {
        var row = initial;
        var remaining = self.stages.len;
        while (remaining != 0) {
            remaining -= 1;
            const stage = self.stages[remaining];
            const query = stage.node.operation.query;
            var context = original_context;
            context.binding = query.binding;
            context.binding.relation = null;
            const bound = query.binding.scalars;
            const cells = try bound.cells(a, row);
            if (!try bound.matchesWithLimits(a, cells, context.parameters, context.backend.decision_provider, @import("decision_eval.zig").limitsFor(context.backend))) return null;
            const values = try a.alloc(scalar.Datum, stage.node.columns.len);
            @memset(values, .{});
            const fields = try a.alloc([]const u8, values.len);
            for (stage.node.columns, query.statement.columns, query.binding.columns, values, fields, 0..) |column, projection, definition, *value, *field, index| {
                field.* = column.internal;
                const active = if (slots) |active_slots| blk: {
                    for (active_slots) |slot| {
                        if (slot >= stage.fields.len) return error.InvalidSqlBackendResponse;
                        if (contains(stage.fields[slot], column.internal)) break :blk true;
                    }
                    break :blk false;
                } else contains(stage.fields[0], column.internal);
                if (!active) continue;
                const program = if (index < bound.projections.len) bound.projections[index] else null;
                const input = if (program) |p| try context.evaluate(a, p, cells) else try row.cell(projection.field);
                value.* = try describe.coerceDatum(a, input, definition.type, definition.element_type);
            }
            row = try catalog.Row.fromDatums(a, row.id, try catalog.Row.TypedLayout.init(a, fields), values);
        }
        return row;
    }
};

fn columnIndex(node: *const binding.Node, name: []const u8) ?usize {
    for (node.columns, 0..) |column, index| if (std.mem.eql(u8, column.internal, name)) return index;
    return null;
}
fn contains(fields: []const []const u8, name: []const u8) bool {
    for (fields) |field| if (std.mem.eql(u8, field, name)) return true;
    return false;
}
fn programFields(a: A, fields: *Names, program: scalar.Program, columns: []const scalar.Column) !void {
    for (program.required_columns) |index| {
        if (index >= columns.len) return error.InvalidSqlBackendResponse;
        try fields.put(a, columns[index].name, {});
    }
}
fn names(a: A, fields: Names) ![]const []const u8 {
    const result = try a.alloc([]const u8, fields.count());
    var keys = fields.keyIterator();
    for (result) |*name| name.* = keys.next().?.*;
    return result;
}
