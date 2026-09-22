// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

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
};

pub fn aggregateKind(name: []const u8) ?operators.Aggregate.Kind {
    inline for (std.meta.fields(operators.Aggregate.Kind)) |field| if (std.mem.eql(u8, name, field.name)) return @enumFromInt(field.value);
    return null;
}
pub fn contains(node: *const ast.Scalar) bool {
    return switch (node.*) {
        .call => |call| blk: {
            if (aggregateKind(call.name) != null) break :blk true;
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
    arguments: std.ArrayList(ast.Projection) = .empty,
    aggregates: std.ArrayList(*const ast.Scalar) = .empty,
    inputs: std.ArrayList(?usize) = .empty,
    filters: std.ArrayList(?usize) = .empty,
    inference: bool = false,

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
                else => call.args[0],
            };
            for (self.aggregates.items, 0..) |aggregate, index| if (same(input, aggregate)) return self.slot(self.groups.len + index);
            if (self.aggregates.items.len >= 256) return error.SqlProgramLimitExceeded;
            const index = self.aggregates.items.len;
            try self.aggregates.append(self.alloc, input);
            try self.inputs.append(self.alloc, if (call.star) null else self.arguments.items.len);
            if (!call.star) try self.arguments.append(self.alloc, .{ .expression = call.args[0] });
            try self.filters.append(self.alloc, if (call.filter != null) self.arguments.items.len else null);
            if (call.filter) |filter| try self.arguments.append(self.alloc, .{ .expression = filter });
            return self.slot(self.groups.len + index);
        }
        return self.node(switch (input.*) {
            .column => if (self.inference) input.* else return error.SqlGroupingError,
            .literal => input.*,
            .unary => |unary| .{ .unary = .{ .op = unary.op, .operand = try self.rewrite(unary.operand) } },
            .binary => |binary| .{ .binary = .{ .op = binary.op, .left = try self.rewrite(binary.left), .right = try self.rewrite(binary.right) } },
            .cast => |cast| .{ .cast = .{ .type = cast.type, .operand = try self.rewrite(cast.operand) } },
            .call => |call| blk: {
                const args = try self.alloc.alloc(*const ast.Scalar, call.args.len);
                for (call.args, args) |arg, *out| out.* = try self.rewrite(arg);
                break :blk .{ .call = .{ .name = call.name, .args = args, .star = call.star, .distinct = call.distinct, .filter = if (call.filter) |filter| try self.rewrite(filter) else null } };
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
    if (statement.columns.len == 0) return error.SqlGroupingError;
    var builder: Builder = .{ .alloc = alloc, .groups = statement.group_by };
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
            if (!source_exists) for (statement.columns, projection_nodes) |projection, node| if (projection.alias) |alias| if (std.mem.eql(u8, alias, group.column)) {
                out.* = node;
                break;
            };
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
                if (found != null) return error.AmbiguousSqlColumn;
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
        for (definition.columns, result[0..definition.columns.len]) |column, *out| out.* = .{ .name = column.name, .type = column.type, .nullable = column.nullable };
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
        if (inference_having) |node| changed = try scalar.inferParameters(alloc, node, source_columns, parameters, .boolean, .{}) or changed;
        if (!changed) break;
    }
    var predicate: ast.Predicate = if (statement.predicate) |node| .{ .scalar = try bound_scalars.predicateScalar(alloc, table, node) } else undefined;
    const input = try bound_scalars.bind(alloc, table, .{ .select = .{ .table = statement.table, .columns = builder.arguments.items, .predicate = if (statement.predicate != null) &predicate else null, .limit = statement.limit, .offset = statement.offset } }, parameters);
    const columns = try alloc.alloc(scalar.Column, groups.len + builder.aggregates.items.len);
    for (columns, 0..) |*column, index| column.* = .{ .name = try std.fmt.allocPrint(alloc, "$grouped_{d}", .{index}), .type = .string };
    for (columns[0..groups.len], input.projections[0..groups.len]) |*column, program| column.type = program.?.output_type.kind orelse .string;
    const specs = try alloc.alloc(operators.AggregateSpec, builder.aggregates.items.len);
    for (builder.aggregates.items, builder.inputs.items, specs, columns[groups.len..]) |node, index, *spec, *column| {
        const kind = aggregateKind(node.call.name).?;
        const input_type = if (index) |slot| input.projections[slot].?.output_type.kind else null;
        _ = try operators.Aggregate.init(alloc, kind, input_type);
        spec.* = .{ .kind = kind, .input_type = input_type, .distinct = node.call.distinct };
        column.type = switch (kind) {
            .count => .integer,
            .avg => .number,
            .bool_and, .bool_or => .boolean,
            else => input_type orelse .string,
        };
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
        program.* = try scalar.bind(alloc, node, columns, parameters, .{});
        name.* = try alloc.dupe(u8, projection.alias orelse if (projection.field.len != 0) projection.field else if (projection.expression.?.* == .call) projection.expression.?.call.name else "?column?");
    }
    const orders = try alloc.alloc(scalar.Program, order_nodes.len);
    for (order_nodes, orders) |node, *program| program.* = try scalar.bind(alloc, node, columns, parameters, .{});
    const having_program = if (having) |node| try scalar.bindExpected(alloc, node, columns, parameters, .boolean, .{}) else null;
    if (having_program) |program| if (program.output_type.kind != null and program.output_type.kind != .boolean) return error.SqlTypeMismatch;
    for (builder.filters.items) |slot| if (slot) |index| {
        const kind = input.projections[index].?.output_type.kind;
        if (kind != null and kind != .boolean) return error.SqlTypeMismatch;
    };
    return .{ .input = input, .group_count = groups.len, .specs = specs, .inputs = try builder.inputs.toOwnedSlice(alloc), .filters = try builder.filters.toOwnedSlice(alloc), .outputs = programs, .names = names, .having = having_program, .orders = orders };
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
        if (std.mem.eql(u8, sql, "SELECT count()")) try std.testing.expectError(error.InvalidSqlParameters, bind(arena.allocator(), null, compiled.statement.select, &.{})) else try std.testing.expectError(error.SqlGroupingError, bind(arena.allocator(), null, compiled.statement.select, &.{}));
    }
}
