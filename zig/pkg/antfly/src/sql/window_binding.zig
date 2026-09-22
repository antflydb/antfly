// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Window input and output are separate typed domains. The input SELECT owns
//! filtering/grouping; window results cannot escape into WHERE or GROUP BY.
const std = @import("std");
const ast = @import("ast.zig");
const scalar = @import("scalar.zig");
const describe = @import("describe.zig");
const catalog = @import("catalog.zig");
const compiler = @import("compiler.zig");
const Allocator = std.mem.Allocator;
pub const Kind = enum { row_number, rank, dense_rank, percent_rank, cume_dist, ntile, lag, lead, first_value, last_value, nth_value, count, sum, avg, min, max, bool_and, bool_or };
pub const Sort = struct { partition: []const usize, order: []const usize, directions: []const @import("operators.zig").Order };
pub const Spec = struct { kind: Kind, arguments: []const usize, filter: ?usize, sort: usize, frame: ?ast.Window.Frame, type: ast.ColumnType, star: bool };
pub const Bound = struct {
    input: *const describe.BoundStatement,
    statement: ast.Select,
    specs: []const Spec,
    sorts: []const Sort,
    outputs: []const scalar.Program,
    orders: []const scalar.Program,
    names: []const []const u8,
};

pub fn contains(node: *const ast.Scalar) bool {
    return switch (node.*) {
        .call => |call| blk: {
            if (call.window != null) break :blk true;
            for (call.args) |arg| if (contains(arg)) break :blk true;
            break :blk if (call.filter) |filter| contains(filter) else false;
        },
        .unary => |part| contains(part.operand),
        .binary => |part| contains(part.left) or contains(part.right),
        .cast => |part| contains(part.operand),
        .case_when => |part| blk: {
            for (part.branches) |branch| if (contains(branch.condition) or contains(branch.value)) break :blk true;
            break :blk if (part.otherwise) |other| contains(other) else false;
        },
        .in_list => |part| blk: {
            if (contains(part.operand)) break :blk true;
            for (part.values) |value| if (contains(value)) break :blk true;
            break :blk false;
        },
        else => false,
    };
}
pub fn accepts(statement: ast.Select) bool {
    for (statement.columns) |projection| if (projection.expression) |node| if (contains(node)) return true;
    for (statement.order_by) |order| if (order.expression) |node| if (contains(node)) return true;
    return false;
}

pub fn validatePlacement(statement: ast.Select) !void {
    for (statement.group_by) |expression| if (contains(expression)) return error.SqlGroupingError;
    if (statement.having) |expression| if (contains(expression)) return error.SqlGroupingError;
    if (statement.predicate) |predicate| try validatePredicate(predicate);
}

fn validatePredicate(predicate: *const ast.Predicate) anyerror!void {
    switch (predicate.*) {
        .scalar => |expression| if (contains(expression)) {
            return error.SqlGroupingError;
        },
        .conjunction, .disjunction => |part| {
            try validatePredicate(part.left);
            try validatePredicate(part.right);
        },
        .negation => |part| try validatePredicate(part),
        else => {},
    }
}
const Pending = struct { node: *const ast.Scalar, arguments: []const usize, filter: ?usize, sort: usize };
const Builder = struct {
    alloc: Allocator,
    inputs: std.ArrayList(ast.Projection) = .empty,
    specs: std.ArrayList(Pending) = .empty,
    sorts: std.ArrayList(Sort) = .empty,
    fn node(self: *Builder, value: ast.Scalar) !*const ast.Scalar {
        const out = try self.alloc.create(ast.Scalar);
        out.* = value;
        return out;
    }
    fn slot(self: *Builder, prefix: []const u8, index: usize) !*const ast.Scalar {
        return self.node(.{ .column = try std.fmt.allocPrint(self.alloc, "${s}_{d}", .{ prefix, index }) });
    }
    fn input(self: *Builder, node_: *const ast.Scalar) !usize {
        if (contains(node_)) return error.SqlGroupingError;
        for (self.inputs.items, 0..) |projection, index| if (@import("aggregate_binding.zig").same(projection.expression.?, node_)) return index;
        const index = self.inputs.items.len;
        try self.inputs.append(self.alloc, .{ .expression = node_, .alias = try std.fmt.allocPrint(self.alloc, "$input_{d}", .{index}) });
        return index;
    }
    fn rewrite(self: *Builder, node_: *const ast.Scalar) anyerror!*const ast.Scalar {
        if (!contains(node_)) return self.slot("input", try self.input(node_));
        if (node_.* == .call and node_.call.window != null) {
            const call = node_.call;
            if (call.distinct) return error.UnsupportedSqlShape;
            _ = std.meta.stringToEnum(Kind, call.name) orelse return error.UndefinedSqlFunction;
            const args = try self.alloc.alloc(usize, call.args.len);
            for (call.args, args) |arg, *out| out.* = try self.input(arg);
            const window = call.window.?;
            const partition = try self.alloc.alloc(usize, window.partition.len);
            for (window.partition, partition) |item, *out| out.* = try self.input(item);
            const order = try self.alloc.alloc(usize, window.order.len);
            const directions = try self.alloc.alloc(@import("operators.zig").Order, window.order.len);
            for (window.order, order, directions) |item, *out, *direction| {
                out.* = try self.input(item.expression orelse try self.node(.{ .column = item.field }));
                direction.* = .{ .descending = item.descending, .nulls_first = item.nulls_first };
            }
            const sort_index = for (self.sorts.items, 0..) |sort, index| {
                if (std.mem.eql(usize, partition, sort.partition) and std.mem.eql(usize, order, sort.order) and directionsEqual(directions, sort.directions)) break index;
            } else blk: {
                const index = self.sorts.items.len;
                try self.sorts.append(self.alloc, .{ .partition = partition, .order = order, .directions = directions });
                break :blk index;
            };
            const index = self.specs.items.len;
            if (index >= 256) return error.SqlProgramLimitExceeded;
            try self.specs.append(self.alloc, .{ .node = node_, .arguments = args, .filter = if (call.filter) |filter| try self.input(filter) else null, .sort = sort_index });
            return self.slot("window", index);
        }
        return self.node(switch (node_.*) {
            .binary => |part| .{ .binary = .{ .op = part.op, .left = try self.rewrite(part.left), .right = try self.rewrite(part.right) } },
            .unary => |part| .{ .unary = .{ .op = part.op, .operand = try self.rewrite(part.operand) } },
            .cast => |part| .{ .cast = .{ .type = part.type, .operand = try self.rewrite(part.operand) } },
            .call => |part| blk: {
                const args = try self.alloc.alloc(*const ast.Scalar, part.args.len);
                for (part.args, args) |arg, *out| out.* = try self.rewrite(arg);
                var copy = part;
                copy.args = args;
                copy.filter = if (part.filter) |filter| try self.rewrite(filter) else null;
                break :blk .{ .call = copy };
            },
            .case_when => |part| blk: {
                const branches = try self.alloc.alloc(ast.Scalar.Branch, part.branches.len);
                for (part.branches, branches) |branch, *out| out.* = .{ .condition = try self.rewrite(branch.condition), .value = try self.rewrite(branch.value) };
                break :blk .{ .case_when = .{ .branches = branches, .otherwise = if (part.otherwise) |other| try self.rewrite(other) else null } };
            },
            .in_list => |part| blk: {
                const values = try self.alloc.alloc(*const ast.Scalar, part.values.len);
                for (part.values, values) |item, *out| out.* = try self.rewrite(item);
                break :blk .{ .in_list = .{ .operand = try self.rewrite(part.operand), .values = values, .negated = part.negated } };
            },
            else => unreachable,
        });
    }
};
fn directionsEqual(left: []const @import("operators.zig").Order, right: []const @import("operators.zig").Order) bool {
    if (left.len != right.len) return false;
    for (left, right) |a, b| if (a.descending != b.descending or a.nulls_first != b.nulls_first) return false;
    return true;
}

pub fn bind(alloc: Allocator, backend: catalog.Backend, compiled: *const compiler.Compiled, hints: []const ?ast.ColumnType) !Bound {
    const statement = compiled.statement.select;
    const inferred = try alloc.alloc(?ast.ColumnType, compiled.parameter_count);
    @memset(inferred, null);
    @memcpy(inferred[0..hints.len], hints);
    // Resolve once even when whole-shape inference and inner binding both need
    // the table: a schema epoch may not change between those phases.
    var adapter: ?@import("relation_binding.zig").ResolveAdapter = null;
    const pinned_backend = if (statement.table) |name| blk: {
        adapter = .{ .backend = backend, .table = try backend.vtable.resolve(backend.ptr, alloc, name, .read) };
        break :blk adapter.?.iface();
    } else backend;
    try @import("relation_binding.zig").inferExpected(alloc, pinned_backend, statement, inferred, &.{});
    var builder: Builder = .{ .alloc = alloc };
    const outputs = try alloc.alloc(*const ast.Scalar, statement.columns.len);
    const originals = try alloc.alloc(*const ast.Scalar, statement.columns.len);
    for (statement.columns, originals, outputs) |projection, *original, *out| {
        original.* = projection.expression orelse try builder.node(.{ .column = projection.field });
        out.* = try builder.rewrite(original.*);
    }
    const orders = try alloc.alloc(*const ast.Scalar, statement.order_by.len);
    for (statement.order_by, orders) |order, *out| {
        var node_ = order.expression;
        if (order.position) |position| {
            if (position == 0 or position > originals.len) return error.UndefinedColumn;
            node_ = originals[position - 1];
        } else if (node_ == null) {
            for (statement.columns, originals) |projection, original| if (std.mem.eql(u8, projection.alias orelse projection.field, order.field)) {
                node_ = original;
                break;
            };
            if (node_ == null) node_ = try builder.node(.{ .column = order.field });
        }
        out.* = try builder.rewrite(node_.?);
    }
    if (builder.inputs.items.len == 0) _ = try builder.input(try builder.node(.{ .literal = .{ .integer = 1 } }));
    var input_statement = statement;
    input_statement.columns = builder.inputs.items;
    input_statement.order_by = &.{};
    input_statement.limit = null;
    input_statement.offset = null;
    var input_compiled = compiled.*;
    input_compiled.statement = .{ .select = input_statement };
    const input = try alloc.create(describe.BoundStatement);
    input.* = try describe.bind(alloc, pinned_backend, &input_compiled, inferred);
    const parameters = @constCast(input.parameter_types);
    const columns = try alloc.alloc(scalar.Column, input.columns.len + builder.specs.items.len);
    for (input.columns, columns[0..input.columns.len]) |column, *out| out.* = .{ .name = column.name, .type = column.type };
    const specs = try alloc.alloc(Spec, builder.specs.items.len);
    for (builder.specs.items, specs, columns[input.columns.len..], 0..) |pending, *spec, *column, index| {
        const call = pending.node.call;
        const kind = std.meta.stringToEnum(Kind, call.name).?;
        const nargs = call.args.len;
        switch (kind) {
            .row_number, .rank, .dense_rank, .percent_rank, .cume_dist => if (nargs != 0 or call.star) return error.InvalidSqlParameters,
            .lag, .lead => if (nargs < 1 or nargs > 3 or call.star) return error.InvalidSqlParameters,
            .nth_value => if (nargs != 2 or call.star) return error.InvalidSqlParameters,
            .count => if ((call.star and nargs != 0) or (!call.star and nargs != 1)) return error.InvalidSqlParameters,
            else => if (nargs != 1 or call.star) return error.InvalidSqlParameters,
        }
        const first_null = nargs != 0 and call.args[0].* == .literal and call.args[0].literal == .null;
        const default_null = nargs == 3 and call.args[2].* == .literal and call.args[2].literal == .null;
        var input_type = if (nargs != 0) input.columns[pending.arguments[0]].type else ast.ColumnType.integer;
        if (first_null) input_type = switch (kind) {
            .sum, .avg => .number,
            .bool_and, .bool_or => .boolean,
            .lag, .lead => if (nargs == 3 and !default_null) input.columns[pending.arguments[2]].type else input_type,
            else => input_type,
        };
        switch (kind) {
            .sum, .avg => if (input_type != .integer and input_type != .number) return error.SqlTypeMismatch,
            .bool_and, .bool_or => if (input_type != .boolean) return error.SqlTypeMismatch,
            .ntile => if (input_type != .integer) return error.SqlTypeMismatch,
            .lag, .lead, .nth_value => {
                if (nargs > 1 and input.columns[pending.arguments[1]].type != .integer) return error.SqlTypeMismatch;
                if (nargs == 3 and !default_null) {
                    const default_type = input.columns[pending.arguments[2]].type;
                    if ((input_type == .integer or input_type == .number) and (default_type == .integer or default_type == .number)) {
                        if (default_type == .number) input_type = .number;
                    } else if (default_type != input_type) return error.SqlTypeMismatch;
                }
            },
            else => {},
        }
        if (call.filter != null and @intFromEnum(kind) < @intFromEnum(Kind.count)) return error.UnsupportedSqlShape;
        const result_type: ast.ColumnType = switch (kind) {
            .row_number, .rank, .dense_rank, .ntile, .count => .integer,
            .avg, .percent_rank, .cume_dist => .number,
            .bool_and, .bool_or => .boolean,
            else => input_type,
        };
        if (pending.filter) |filter| if (input.columns[filter].type != .boolean) return error.SqlTypeMismatch;
        if (call.window.?.frame) |frame| for ([_]ast.Window.Bound{ frame.start, frame.end }) |bound| {
            const value: ?ast.Value = switch (bound) {
                .preceding, .following => |value| value,
                else => null,
            };
            if (value) |offset| if (offset == .parameter) {
                if (offset.parameter == 0 or offset.parameter > parameters.len) return error.InvalidSqlParameters;
                const slot = &parameters[offset.parameter - 1];
                if (slot.*) |existing| {
                    if (existing != .integer) return error.ConflictingSqlParameterTypes;
                } else slot.* = .integer;
            };
        };
        spec.* = .{ .kind = kind, .arguments = pending.arguments, .filter = pending.filter, .sort = pending.sort, .frame = call.window.?.frame, .type = result_type, .star = call.star };
        column.* = .{ .name = try std.fmt.allocPrint(alloc, "$window_{d}", .{index}), .type = result_type };
    }
    const output_programs = try alloc.alloc(scalar.Program, outputs.len);
    const names = try alloc.alloc([]const u8, outputs.len);
    for (outputs, output_programs, statement.columns, names) |node_, *program, projection, *name| {
        program.* = try scalar.bind(alloc, node_, columns, parameters, .{});
        name.* = projection.alias orelse if (projection.field.len != 0) projection.field else if (projection.expression.?.* == .call) projection.expression.?.call.name else "?column?";
    }
    const order_programs = try alloc.alloc(scalar.Program, orders.len);
    for (orders, order_programs) |node_, *program| program.* = try scalar.bind(alloc, node_, columns, parameters, .{});
    return .{ .input = input, .statement = input_statement, .specs = specs, .sorts = builder.sorts.items, .outputs = output_programs, .orders = order_programs, .names = names };
}
