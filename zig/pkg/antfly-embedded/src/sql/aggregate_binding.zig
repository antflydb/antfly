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

//! Lower grouping to row-input programs and a separate grouped-output program
//! domain. No ungrouped row identity can accidentally escape aggregation.
const std = @import("std");
const ast = @import("ast.zig");
const scalar = @import("scalar.zig");
const catalog = @import("catalog.zig");
const bound_scalars = @import("bound_scalars.zig");
const operators = @import("operators.zig");
const Allocator = std.mem.Allocator;

pub const Bound = struct {
    input: bound_scalars.Bound,
    group_count: usize,
    specs: []const operators.AggregateSpec,
    /// null denotes COUNT(*); otherwise ordinal in input.projections.
    inputs: []const ?usize,
    filters: []const ?usize,
    outputs: []const scalar.Program,
    names: []const []const u8,
    having: ?scalar.Program,
    orders: []const scalar.Program,
    order_outputs: []const ?usize = &.{},
    constant_count: usize = 0,
    ordered: []const Ordered = &.{},
    ordered_class_count: usize = 0,
};

pub const OrderedKind = enum { mode, continuous, discrete };
pub fn orderedKind(name: []const u8) ?OrderedKind {
    if (std.mem.eql(u8, name, "mode")) return .mode;
    if (std.mem.eql(u8, name, "percentile_cont")) return .continuous;
    if (std.mem.eql(u8, name, "percentile_disc")) return .discrete;
    return null;
}
pub fn orderedWithoutClauseError(kind: OrderedKind, arity: usize) anyerror {
    // PostgreSQL first resolves the complete aggregate signature, whose
    // arguments include the ordered input, before requiring WITHIN GROUP.
    return if (arity == (if (kind == .mode) @as(usize, 1) else 2)) error.SqlWrongAggregateKind else error.UndefinedSqlFunction;
}
pub const Ordered = struct {
    aggregate_index: usize,
    input: usize,
    filter: ?usize,
    kind: OrderedKind,
    order: ast.Scalar.Ordering,
    direct: ?scalar.Program,
    sort_class: usize,
};

pub fn arrayExpression(alloc: Allocator, node: *const ast.Scalar, columns: []const scalar.Column, parameters: []const ?ast.ColumnType) !bool {
    if (node.* == .cast and node.cast.type == .array) return true;
    if (node.* == .call and std.mem.eql(u8, node.call.name, "$array")) return true;
    return (try scalar.inferOutput(alloc, node, columns, parameters)).kind == .array;
}

pub fn aggregateKind(name: []const u8) ?operators.Aggregate.Kind {
    if (std.mem.eql(u8, name, "$pattern_set")) return .pattern_set;
    inline for (@typeInfo(operators.Aggregate.Kind).@"enum".field_names, @typeInfo(operators.Aggregate.Kind).@"enum".field_values) |reflected_name, field_value| if (std.mem.eql(u8, name, reflected_name)) return @fromBackingInt(field_value);
    return null;
}
pub fn contains(node: *const ast.Scalar) bool {
    return switch (node.*) {
        .call => |call| blk: {
            if (call.within_group != null or orderedKind(call.name) != null or aggregateKind(call.name) != null) break :blk true;
            for (call.args) |arg| if (contains(arg)) break :blk true;
            if (call.filter) |filter| if (contains(filter)) break :blk true;
            break :blk false;
        },
        .unary => |unary| contains(unary.operand),
        .binary => |binary| contains(binary.left) or contains(binary.right),
        .cast => |cast| contains(cast.operand),
        .case_when => |case| blk: {
            for (case.branches) |branch| if (contains(branch.condition) or contains(branch.value)) break :blk true;
            break :blk if (case.otherwise) |other| contains(other) else false;
        },
        .in_list => |list| blk: {
            if (contains(list.operand)) break :blk true;
            for (list.values) |item| if (contains(item)) break :blk true;
            break :blk false;
        },
        else => false,
    };
}
pub fn accepts(statement: ast.Select) bool {
    if (statement.group_by.len != 0 or statement.having != null) return true;
    for (statement.columns) |projection| if (projection.expression) |node| if (contains(node)) return true;
    for (statement.order_by) |order| if (order.expression) |node| if (contains(node)) return true;
    return false;
}

pub fn same(a: *const ast.Scalar, b: *const ast.Scalar) bool {
    if (std.meta.activeTag(a.*) != std.meta.activeTag(b.*)) return false;
    return switch (a.*) {
        .column => |name| std.mem.eql(u8, name, b.column),
        .literal => |literal| if (std.meta.activeTag(literal) != std.meta.activeTag(b.literal)) false else switch (literal) {
            .string => |value| std.mem.eql(u8, value, b.literal.string),
            inline else => |value, tag| std.meta.eql(value, @field(b.literal, @tagName(tag))),
        },
        .unary => |unary| unary.op == b.unary.op and same(unary.operand, b.unary.operand),
        .binary => |binary| binary.op == b.binary.op and same(binary.left, b.binary.left) and same(binary.right, b.binary.right),
        .cast => |cast| cast.type == b.cast.type and same(cast.operand, b.cast.operand),
        .call => |call| blk: {
            if (!std.mem.eql(u8, call.name, b.call.name) or call.star != b.call.star or call.distinct != b.call.distinct or (call.filter == null) != (b.call.filter == null) or call.args.len != b.call.args.len) break :blk false;
            // Query and window domains cannot lose their metadata through
            // ordinary aggregate-expression deduplication.
            if (call.subquery != null or b.call.subquery != null or call.window != null or b.call.window != null) break :blk a == b;
            if ((call.within_group == null) != (b.call.within_group == null)) break :blk false;
            if (call.within_group) |within| {
                if (within.orders.len != b.call.within_group.?.orders.len) break :blk false;
                for (within.orders, b.call.within_group.?.orders) |left, right| if (!std.meta.eql(left, right)) break :blk false;
            }
            if (call.filter) |filter| if (!same(filter, b.call.filter.?)) break :blk false;
            for (call.args, b.call.args) |left, right| if (!same(left, right)) break :blk false;
            break :blk true;
        },
        .case_when => |case| blk: {
            if (case.branches.len != b.case_when.branches.len or (case.otherwise == null) != (b.case_when.otherwise == null)) break :blk false;
            for (case.branches, b.case_when.branches) |left, right| if (!same(left.condition, right.condition) or !same(left.value, right.value)) break :blk false;
            break :blk if (case.otherwise) |other| same(other, b.case_when.otherwise.?) else true;
        },
        .in_list => |list| blk: {
            if (list.negated != b.in_list.negated or list.values.len != b.in_list.values.len or !same(list.operand, b.in_list.operand)) break :blk false;
            for (list.values, b.in_list.values) |left, right| if (!same(left, right)) break :blk false;
            break :blk true;
        },
    };
}

const Builder = struct {
    alloc: Allocator,
    groups: []const *const ast.Scalar,
    table: ?catalog.Table = null,
    arguments: std.ArrayList(ast.Projection) = .empty,
    aggregates: std.ArrayList(*const ast.Scalar) = .empty,
    inputs: std.ArrayList(?usize) = .empty,
    filters: std.ArrayList(?usize) = .empty,
    inference: bool = false,
    constants: []const []const u8 = &.{},
    ordered: std.ArrayList(struct { index: usize, direct: ?*const ast.Scalar, original_direct: ?*const ast.Scalar }) = .empty,

    fn node(self: *Builder, value: ast.Scalar) !*const ast.Scalar {
        const result = try self.alloc.create(ast.Scalar);
        result.* = value;
        return result;
    }
    fn slot(self: *Builder, index: usize) !*const ast.Scalar {
        return self.node(.{ .column = try std.fmt.allocPrint(self.alloc, "$grouped_{d}", .{index}) });
    }
    fn rewrite(self: *Builder, input: *const ast.Scalar) anyerror!*const ast.Scalar {
        if (!self.inference) for (self.groups, 0..) |group, index| if (same(input, group)) return self.slot(index);
        if (!self.inference and input.* == .column) for (self.constants, 0..) |name, index| {
            if (std.mem.eql(u8, input.column, name))
                return self.node(.{ .column = try std.fmt.allocPrint(self.alloc, "$constant_{d}", .{index}) });
        };
        if (input.* == .call and input.call.within_group != null) {
            const call = input.call;
            const kind = orderedKind(call.name) orelse return if (aggregateKind(call.name) != null) error.SqlWrongAggregateKind else error.UndefinedSqlFunction;
            if (call.window != null) return error.UnsupportedSqlShape;
            const orders = call.within_group.?.orders;
            if (orders.len != 1 or call.args.len != (if (kind == .mode) @as(usize, 1) else 2)) return error.UndefinedSqlFunction;
            if (call.distinct or call.star) return error.InvalidSqlSyntax;
            for (call.args) |argument| if (contains(argument)) return error.SqlGroupingError;
            if (call.filter) |filter| if (contains(filter)) return error.SqlGroupingError;
            const ordered_input = call.args[call.args.len - 1];
            if (self.inference) {
                var value = if (kind == .continuous) try self.node(.{ .cast = .{ .operand = ordered_input, .type = .number } }) else ordered_input;
                // Array fractions preserve an array output shape during the
                // source-domain inference pass too, not only at final bind.
                if (kind != .mode and ((call.args[0].* == .cast and call.args[0].cast.type == .array) or (call.args[0].* == .call and std.mem.eql(u8, call.args[0].call.name, "$array"))))
                    value = try self.node(.{ .call = .{ .name = "$array", .args = try self.alloc.dupe(*const ast.Scalar, &.{value}) } });
                return value;
            }
            for (self.aggregates.items, 0..) |aggregate, index| if (same(input, aggregate)) return self.slot(self.groups.len + index);
            if (self.aggregates.items.len >= 256) return error.SqlProgramLimitExceeded;
            // Direct arguments bind against grouped keys/constants, never an
            // arbitrary input row. Ordered inputs and FILTER stay row-owned.
            const direct = if (kind == .mode) null else try self.rewrite(call.args[0]);
            const index = self.aggregates.items.len;
            try self.aggregates.append(self.alloc, input);
            try self.inputs.append(self.alloc, self.arguments.items.len);
            try self.arguments.append(self.alloc, .{ .expression = ordered_input });
            try self.filters.append(self.alloc, if (call.filter != null) self.arguments.items.len else null);
            if (call.filter) |filter| try self.arguments.append(self.alloc, .{ .expression = filter });
            try self.ordered.append(self.alloc, .{ .index = index, .direct = direct, .original_direct = if (kind == .mode) null else call.args[0] });
            return self.slot(self.groups.len + index);
        }
        if (input.* == .call) if (orderedKind(input.call.name)) |kind| return orderedWithoutClauseError(kind, input.call.args.len);
        if (input.* == .call and aggregateKind(input.call.name) != null) {
            const call = input.call;
            if (call.star and call.distinct) return error.InvalidSqlParameters;
            if ((call.star and (!std.mem.eql(u8, call.name, "count") or call.args.len != 0)) or (!call.star and call.args.len != 1)) return error.InvalidSqlParameters;
            for (call.args) |arg| if (contains(arg)) return error.SqlGroupingError;
            if (call.filter) |filter| if (contains(filter)) return error.SqlGroupingError;
            if (self.inference) return switch (aggregateKind(call.name).?) {
                .count => self.node(.{ .literal = .{ .integer = 0 } }),
                .avg => self.node(.{ .cast = .{ .operand = call.args[0], .type = .number } }),
                .bool_and, .bool_or => self.node(.{ .cast = .{ .operand = call.args[0], .type = .boolean } }),
                .pattern_set => self.node(.{ .cast = .{ .operand = try self.node(.{ .cast = .{ .operand = call.args[0], .type = .string } }), .type = .json } }),
                else => call.args[0],
            };
            for (self.aggregates.items, 0..) |aggregate, index| if (same(input, aggregate)) return self.slot(self.groups.len + index);
            if (self.aggregates.items.len >= 256) return error.SqlProgramLimitExceeded;
            const index = self.aggregates.items.len;
            try self.aggregates.append(self.alloc, input);
            try self.inputs.append(self.alloc, if (call.star) null else self.arguments.items.len);
            if (!call.star) try self.arguments.append(self.alloc, .{ .expression = if (aggregateKind(call.name).? == .pattern_set) try self.node(.{ .cast = .{ .operand = call.args[0], .type = .string } }) else call.args[0] });
            try self.filters.append(self.alloc, if (call.filter != null) self.arguments.items.len else null);
            if (call.filter) |filter| try self.arguments.append(self.alloc, .{ .expression = filter });
            return self.slot(self.groups.len + index);
        }
        return self.node(switch (input.*) {
            .column => if (self.inference) input.* else {
                // Resolve the input namespace before enforcing grouping.
                // Output labels are not visible inside HAVING or compound
                // sort/group expressions (42703), while a real, ungrouped
                // input column is a grouping error (42803).
                const definition = self.table orelse return error.UndefinedColumn;
                _ = try definition.column(input.column);
                return error.SqlGroupingError;
            },
            .literal => input.*,
            .unary => |unary| .{ .unary = .{ .op = unary.op, .operand = try self.rewrite(unary.operand) } },
            .binary => |binary| .{ .binary = .{ .op = binary.op, .left = try self.rewrite(binary.left), .right = try self.rewrite(binary.right) } },
            .cast => |cast| .{ .cast = .{ .type = cast.type, .element_type = cast.element_type, .operand = try self.rewrite(cast.operand) } },
            .call => |call| blk: {
                const args = try self.alloc.alloc(*const ast.Scalar, call.args.len);
                for (call.args, args) |arg, *out| out.* = try self.rewrite(arg);
                break :blk .{ .call = .{ .name = call.name, .args = args, .star = call.star, .distinct = call.distinct, .filter = if (call.filter) |filter| try self.rewrite(filter) else null, .within_group = call.within_group } };
            },
            .case_when => |case| blk: {
                const branches = try self.alloc.alloc(ast.Scalar.Branch, case.branches.len);
                for (case.branches, branches) |branch, *out| out.* = .{ .condition = try self.rewrite(branch.condition), .value = try self.rewrite(branch.value) };
                break :blk .{ .case_when = .{ .branches = branches, .otherwise = if (case.otherwise) |other| try self.rewrite(other) else null } };
            },
            .in_list => |list| blk: {
                const values = try self.alloc.alloc(*const ast.Scalar, list.values.len);
                for (list.values, values) |item, *out| out.* = try self.rewrite(item);
                break :blk .{ .in_list = .{ .operand = try self.rewrite(list.operand), .values = values, .negated = list.negated } };
            },
        });
    }
};

pub fn bind(alloc: Allocator, table: ?catalog.Table, statement: ast.Select, parameters: []?ast.ColumnType) !Bound {
    return bindWithSettings(alloc, table, statement, parameters, null);
}

pub fn bindWithSettings(alloc: Allocator, table: ?catalog.Table, statement: ast.Select, parameters: []?ast.ColumnType, settings: ?*const @import("setting_catalog.zig").View) !Bound {
    if (statement.columns.len == 0) return error.SqlGroupingError;
    var builder: Builder = .{ .alloc = alloc, .groups = statement.group_by, .table = table, .constants = statement.invocation_constants };
    const projection_nodes = try alloc.alloc(*const ast.Scalar, statement.columns.len);
    for (statement.columns, projection_nodes) |projection, *node| node.* = projection.expression orelse try builder.node(.{ .column = projection.field });
    const groups = try alloc.alloc(*const ast.Scalar, statement.group_by.len);
    for (statement.group_by, groups) |group, *out| {
        out.* = group;
        if (group.* == .literal and group.literal == .integer) {
            const index = std.math.cast(usize, group.literal.integer) orelse return error.SqlGroupingError;
            if (index == 0 or index > projection_nodes.len) return error.SqlGroupingError;
            out.* = projection_nodes[index - 1];
        } else if (group.* == .column) {
            const source_exists = if (table) |definition| blk: {
                _ = definition.column(group.column) catch break :blk false;
                break :blk true;
            } else false;
            if (!source_exists) {
                var match: ?*const ast.Scalar = null;
                for (statement.columns, projection_nodes) |projection, node| if (std.mem.eql(u8, projection.alias orelse projection.field, group.column)) {
                    if (match) |previous| if (!same(previous, node)) return error.AmbiguousSqlColumn;
                    match = node;
                };
                if (match) |node| out.* = node;
            }
        }
        if (contains(out.*)) return error.SqlGroupingError;
        try builder.arguments.append(alloc, .{ .expression = out.* });
    }
    builder.groups = groups;
    const outputs = try alloc.alloc(*const ast.Scalar, projection_nodes.len);
    for (projection_nodes, outputs) |node, *out| out.* = try builder.rewrite(node);
    const having = if (statement.having) |node| try builder.rewrite(node) else null;
    const order_nodes = try alloc.alloc(*const ast.Scalar, statement.order_by.len);
    for (statement.order_by, order_nodes) |order, *out| {
        var expression = order.expression;
        if (order.position) |position| {
            if (position == 0 or position > projection_nodes.len) return error.UndefinedColumn;
            expression = projection_nodes[position - 1];
        } else if (expression == null) {
            var found: ?usize = null;
            for (statement.columns, 0..) |projection, index| if (std.mem.eql(u8, projection.alias orelse projection.field, order.field)) {
                if (found) |previous| if (!same(projection_nodes[previous], projection_nodes[index])) return error.AmbiguousSqlColumn;
                found = index;
            };
            expression = if (found) |index| projection_nodes[index] else try builder.node(.{ .column = order.field });
        }
        out.* = try builder.rewrite(expression.?);
    }
    // Infer parameter constraints through aggregate result expressions before
    // binding any row-input program. SELECT $1+1, MAX($1) must not depend on
    // projection order or prematurely freeze MAX's argument as unknown/text.
    builder.inference = true;
    const source_columns = if (table) |definition| blk: {
        const result = try alloc.alloc(scalar.Column, definition.columns.len + 1);
        for (definition.columns, result[0..definition.columns.len]) |column, *out| out.* = .{ .name = column.name, .type = column.type, .element_type = column.element_type, .nullable = column.nullable };
        result[definition.columns.len] = .{ .name = "_id", .type = .string, .nullable = false };
        break :blk result;
    } else &.{};
    const inference_nodes = try alloc.alloc(*const ast.Scalar, projection_nodes.len);
    for (projection_nodes, inference_nodes) |node, *out| out.* = try builder.rewrite(node);
    const inference_having = if (statement.having) |node| try builder.rewrite(node) else null;
    var inference_pass: usize = 0;
    while (true) : (inference_pass += 1) {
        if (inference_pass > parameters.len + 1) return error.ConflictingSqlParameterTypes;
        var changed = false;
        for (inference_nodes) |node| changed = try scalar.inferParameters(alloc, node, source_columns, parameters, null, .{}) or changed;
        for (builder.arguments.items) |argument| changed = try scalar.inferParameters(alloc, argument.expression.?, source_columns, parameters, null, .{}) or changed;
        for (builder.filters.items) |slot| if (slot) |index| {
            changed = try scalar.inferParameters(alloc, builder.arguments.items[index].expression.?, source_columns, parameters, .boolean, .{}) or changed;
        };
        for (builder.ordered.items) |ordered| if (ordered.original_direct) |direct| {
            const array = try arrayExpression(alloc, direct, source_columns, parameters);
            const coerced = try builder.node(.{ .cast = .{ .operand = direct, .type = if (array) .array else .number, .element_type = if (array) .float64 else null } });
            changed = try scalar.inferParameters(alloc, coerced, source_columns, parameters, null, .{}) or changed;
        };
        if (inference_having) |node| changed = try scalar.inferParameters(alloc, node, source_columns, parameters, .boolean, .{}) or changed;
        if (!changed) break;
    }
    var predicate: ast.Predicate = if (statement.predicate) |node| .{ .scalar = try bound_scalars.predicateScalar(alloc, table, node) } else undefined;
    const input = try bound_scalars.bindWithSettings(alloc, table, .{ .select = .{ .table = statement.table, .columns = builder.arguments.items, .predicate = if (statement.predicate != null) &predicate else null, .limit = statement.limit, .offset = statement.offset } }, parameters, settings);
    const grouped_width = groups.len + builder.aggregates.items.len;
    const columns = try alloc.alloc(scalar.Column, grouped_width + builder.constants.len);
    for (columns[0..grouped_width], 0..) |*column, index| column.* = .{ .name = try std.fmt.allocPrint(alloc, "$grouped_{d}", .{index}), .type = .string };
    for (builder.constants, columns[grouped_width..], 0..) |name, *column, index| {
        const definition = try (table orelse return error.InvalidSqlBackendResponse).column(name);
        column.* = .{ .name = try std.fmt.allocPrint(alloc, "$constant_{d}", .{index}), .type = definition.type, .element_type = definition.element_type, .nullable = definition.nullable };
    }
    for (columns[0..groups.len], input.projections[0..groups.len]) |*column, program| {
        column.type = program.?.output_type.kind orelse .string;
        column.element_type = program.?.output_type.element_type;
    }
    const specs = try alloc.alloc(operators.AggregateSpec, builder.aggregates.items.len);
    for (builder.aggregates.items, builder.inputs.items, specs, columns[groups.len..grouped_width]) |node, index, *spec, *column| {
        const kind = if (node.call.within_group != null) operators.Aggregate.Kind.count else aggregateKind(node.call.name).?;
        const input_type = if (index) |slot| input.projections[slot].?.output_type.kind else null;
        if (node.call.within_group != null and orderedKind(node.call.name).? == .continuous) if (input_type) |typed| {
            if (typed != .integer and typed != .number) return error.UndefinedSqlFunction;
        };
        try operators.Aggregate.validate(kind, input_type);
        spec.* = .{ .kind = kind, .input_type = input_type, .distinct = node.call.distinct };
        column.type = switch (kind) {
            .count => .integer,
            .avg => .number,
            .bool_and, .bool_or => .boolean,
            .pattern_set => .json,
            else => input_type orelse .string,
        };
        if (column.type == .array) column.element_type = if (index) |slot| input.projections[slot].?.output_type.element_type else null;
        if (node.call.within_group != null) {
            column.type = if (orderedKind(node.call.name).? == .continuous) .number else input_type orelse .string;
            column.element_type = if (index) |slot| input.projections[slot].?.output_type.element_type else null;
        }
    }
    const ordered_plans = try alloc.alloc(Ordered, builder.ordered.items.len);
    var class_count: usize = 0;
    for (builder.ordered.items, ordered_plans, 0..) |pending, *plan, position| {
        const call = builder.aggregates.items[pending.index].call;
        const kind = orderedKind(call.name).?;
        var direct_program: ?scalar.Program = null;
        if (pending.direct) |direct| {
            const array = try arrayExpression(alloc, direct, columns, parameters);
            const coerced = try builder.node(.{ .cast = .{ .operand = direct, .type = if (array) .array else .number, .element_type = if (array) .float64 else null } });
            _ = try scalar.inferParameters(alloc, coerced, columns, parameters, null, .{});
            direct_program = try scalar.bindWithSettings(alloc, coerced, columns, parameters, .{}, settings);
            if (array) {
                const column = &columns[groups.len + pending.index];
                column.element_type = if (kind == .continuous) .float64 else try scalar.parameterElementType(.{ .kind = column.type });
                column.type = .array;
            }
        }
        const slot = builder.inputs.items[pending.index].?;
        const filter = builder.filters.items[pending.index];
        var class: ?usize = null;
        for (ordered_plans[0..position]) |prior| {
            if (!std.meta.eql(prior.order, call.within_group.?.orders[0])) continue;
            if (!@import("typed_kernel.zig").sameProgram(&input.projections[slot].?, &input.projections[prior.input].?)) continue;
            if ((filter == null) != (prior.filter == null)) continue;
            if (filter) |f| if (!@import("typed_kernel.zig").sameProgram(&input.projections[f].?, &input.projections[prior.filter.?].?)) continue;
            class = prior.sort_class;
            break;
        }
        if (class == null) {
            class = class_count;
            class_count += 1;
        }
        plan.* = .{ .aggregate_index = pending.index, .input = slot, .filter = filter, .kind = kind, .order = call.within_group.?.orders[0], .direct = direct_program, .sort_class = class.? };
    }
    var pass: usize = 0;
    while (true) : (pass += 1) {
        if (pass > parameters.len + 1) return error.ConflictingSqlParameterTypes;
        var changed = false;
        for (outputs) |node| changed = try scalar.inferParameters(alloc, node, columns, parameters, null, .{}) or changed;
        for (order_nodes) |node| changed = try scalar.inferParameters(alloc, node, columns, parameters, null, .{}) or changed;
        if (having) |node| changed = try scalar.inferParameters(alloc, node, columns, parameters, .boolean, .{}) or changed;
        if (!changed) break;
    }
    const programs = try alloc.alloc(scalar.Program, outputs.len);
    const names = try alloc.alloc([]const u8, outputs.len);
    for (outputs, programs, statement.columns, names) |node, *program, projection, *name| {
        program.* = try scalar.bindWithSettings(alloc, node, columns, parameters, .{}, settings);
        name.* = try alloc.dupe(u8, projection.alias orelse if (projection.field.len != 0) projection.field else if (projection.expression.?.* == .call) projection.expression.?.call.name else "?column?");
    }
    const order_outputs = try alloc.alloc(?usize, order_nodes.len);
    for (order_nodes, order_outputs) |node, *slot_| {
        slot_.* = null;
        for (outputs, 0..) |output, index| if (same(node, output)) {
            slot_.* = index;
            break;
        };
    }
    const orders = try alloc.alloc(scalar.Program, order_nodes.len);
    for (order_nodes, orders) |node, *program| program.* = try scalar.bindWithSettings(alloc, node, columns, parameters, .{}, settings);
    const having_program = if (having) |node| try scalar.bindExpectedWithSettings(alloc, node, columns, parameters, .boolean, .{}, settings) else null;
    if (having_program) |program| if (program.output_type.kind != null and program.output_type.kind != .boolean) return error.SqlTypeMismatch;
    for (builder.filters.items) |slot| if (slot) |index| {
        const kind = input.projections[index].?.output_type.kind;
        if (kind != null and kind != .boolean) return error.SqlTypeMismatch;
    };
    return .{ .input = input, .group_count = groups.len, .specs = specs, .inputs = try builder.inputs.toOwnedSlice(alloc), .filters = try builder.filters.toOwnedSlice(alloc), .outputs = programs, .names = names, .having = having_program, .orders = orders, .order_outputs = order_outputs, .constant_count = builder.constants.len, .ordered = ordered_plans, .ordered_class_count = class_count };
}

test "aggregate binding separates row input from grouped expressions and deduplicates aggregates" {
    var compiled = try @import("compiler.zig").compile(std.testing.allocator, "SELECT 1 AS k, sum(2) + count(*) AS total GROUP BY k HAVING sum(2) > 0 ORDER BY total DESC", .{});
    defer compiled.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const bound = try bind(arena.allocator(), null, compiled.statement.select, &.{});
    try std.testing.expectEqual(@as(usize, 1), bound.group_count);
    try std.testing.expectEqual(@as(usize, 2), bound.specs.len);
    try std.testing.expectEqual(@as(usize, 2), bound.input.projections.len);
    try std.testing.expectEqual(@as(usize, 2), bound.outputs.len);
    try std.testing.expectEqual(ast.ColumnType.integer, bound.outputs[1].output_type.kind.?);
    try std.testing.expectEqual(@as(usize, 1), bound.orders.len);
}

test "aggregate binding rejects nested aggregate and ungrouped row references" {
    for ([_][]const u8{ "SELECT sum(count(*))", "SELECT missing, count(*)", "SELECT count()", "SELECT count(*) GROUP BY count(*)" }) |sql| {
        var compiled = try @import("compiler.zig").compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const expected = if (std.mem.eql(u8, sql, "SELECT count()")) error.InvalidSqlParameters else if (std.mem.eql(u8, sql, "SELECT missing, count(*)")) error.UndefinedColumn else error.SqlGroupingError;
        try std.testing.expectError(expected, bind(arena.allocator(), null, compiled.statement.select, &.{}));
    }
}

test "aggregate HAVING binds input expressions while bare sort and group labels resolve outputs" {
    for ([_][]const u8{
        "SELECT lower('OPEN') AS status_key, count(*) AS row_count GROUP BY lower('OPEN') HAVING lower('OPEN') = 'open' ORDER BY status_key",
        "SELECT 1 AS k, count(*) AS row_count GROUP BY k HAVING count(*) > 0 ORDER BY k",
    }) |sql| {
        var compiled = try @import("compiler.zig").compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const bound = try bind(arena.allocator(), null, compiled.statement.select, &.{});
        try std.testing.expect(bound.having != null);
        try std.testing.expectEqual(@as(usize, 1), bound.orders.len);
    }
}

test "aggregate HAVING cannot see even ambiguous output aliases" {
    var compiled = try @import("compiler.zig").compile(std.testing.allocator, "SELECT 1 AS k, 2 AS k, count(*) AS n GROUP BY 1, 2 HAVING k = 1", .{});
    defer compiled.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    try std.testing.expectError(error.UndefinedColumn, bind(arena.allocator(), null, compiled.statement.select, &.{}));
}

test "ordered aggregate binding separates grouped direct arguments and shares only compatible input domains" {
    const table: catalog.Table = .{ .id = 1, .physical_name = "t", .schema_version = 1, .columns = &.{
        .{ .name = "x", .path = "x", .type = .integer },
        .{ .name = "g", .path = "g", .type = .number },
        .{ .name = "flag", .path = "flag", .type = .boolean },
    } };
    var compiled = try @import("compiler.zig").compile(std.testing.allocator, "SELECT percentile_cont($1) WITHIN GROUP (ORDER BY x),percentile_disc(0.5) WITHIN GROUP (ORDER BY x),mode() WITHIN GROUP (ORDER BY x),percentile_cont(0.25) WITHIN GROUP (ORDER BY x) FILTER (WHERE flag),percentile_cont(0.75) WITHIN GROUP (ORDER BY x DESC) FROM t", .{});
    defer compiled.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var parameters = [_]?ast.ColumnType{null};
    const bound = try bind(arena.allocator(), table, compiled.statement.select, &parameters);
    try std.testing.expectEqual(ast.ColumnType.number, parameters[0].?);
    try std.testing.expectEqual(@as(usize, 5), bound.ordered.len);
    try std.testing.expectEqual(@as(usize, 3), bound.ordered_class_count);
    try std.testing.expectEqual(bound.ordered[0].sort_class, bound.ordered[1].sort_class);
    try std.testing.expectEqual(bound.ordered[0].sort_class, bound.ordered[2].sort_class);
    try std.testing.expect(bound.ordered[2].direct == null);
    try std.testing.expect(bound.ordered[3].filter != null);
    try std.testing.expect(bound.ordered[4].order.descending);
    try std.testing.expectEqual(ast.ColumnType.number, bound.outputs[0].output_type.kind.?);
    try std.testing.expectEqual(ast.ColumnType.integer, bound.outputs[1].output_type.kind.?);
    for ([_][]const u8{
        "SELECT percentile_cont(x) WITHIN GROUP (ORDER BY x) FROM t GROUP BY g",
        "SELECT percentile_cont(SUM(x)) WITHIN GROUP (ORDER BY x) FROM t GROUP BY g",
        "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY SUM(x)) FROM t GROUP BY g",
    }) |sql| {
        var invalid = try @import("compiler.zig").compile(std.testing.allocator, sql, .{});
        defer invalid.deinit();
        try std.testing.expectError(error.SqlGroupingError, bind(arena.allocator(), table, invalid.statement.select, &.{}));
    }
    var grouped = try @import("compiler.zig").compile(std.testing.allocator, "SELECT g,percentile_cont(g/10.0) WITHIN GROUP (ORDER BY x) FROM t GROUP BY g", .{});
    defer grouped.deinit();
    const grouped_bound = try bind(arena.allocator(), table, grouped.statement.select, &.{});
    try std.testing.expectEqualSlices(u32, &.{0}, grouped_bound.ordered[0].direct.?.required_columns);
    var array = try @import("compiler.zig").compile(std.testing.allocator, "SELECT percentile_cont(ARRAY[0.25,NULL,0.75]) WITHIN GROUP (ORDER BY x) FROM t", .{});
    defer array.deinit();
    const array_bound = try bind(arena.allocator(), table, array.statement.select, &.{});
    try std.testing.expectEqual(ast.ColumnType.array, array_bound.outputs[0].output_type.kind.?);
    try std.testing.expectEqual(@import("array_value.zig").ElementType.float64, array_bound.outputs[0].output_type.element_type.?);
}
