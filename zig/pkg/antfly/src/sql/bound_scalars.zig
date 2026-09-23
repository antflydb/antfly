// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Request-owned expression binding. Compiled programs borrow neither a schema
//! cache lease nor row memory. Only required columns enter the native scan;
//! expression evaluation uses resolved ordinals rather than SQL name lookup.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
pub const scalar = @import("scalar.zig");
const Allocator = std.mem.Allocator;

pub const Bound = struct {
    columns: []const scalar.Column = &.{},
    projections: []const ?scalar.Program = &.{},
    orders: []const ?scalar.Program = &.{},
    assignments: []const ?scalar.Program = &.{},
    insert_rows: []const []const ?scalar.Program = &.{},
    predicate: ?scalar.Program = null,
    required: []const u32 = &.{},

    /// Page-local cells. Required ordinals only are materialized; generated
    /// expression values use the same page arena and cannot retain prior pages.
    pub fn cells(self: Bound, alloc: Allocator, row: catalog.Row) ![]const scalar.Datum {
        if (self.required.len == 0) return &.{};
        const out = try alloc.alloc(scalar.Datum, self.columns.len);
        @memset(out, .{});
        for (self.required) |ordinal| {
            const cell = try row.cell(self.columns[ordinal].name);
            out[ordinal] = .{ .value = try @import("describe.zig").coerce(cell.value, self.columns[ordinal].type), .sql_null = cell.sql_null };
        }
        return out;
    }

    pub fn matches(self: Bound, alloc: Allocator, values: []const scalar.Datum, parameters: []const std.json.Value) !bool {
        const program = self.predicate orelse return true;
        const value = try program.evaluate(alloc, values, parameters, .{});
        if (value.sql_null) return false;
        if (value.value != .bool) return error.SqlTypeMismatch;
        return value.value.bool;
    }
};

pub fn needsResidual(predicate: ?*const ast.Predicate) bool {
    const node = predicate orelse return false;
    return switch (node.*) {
        .comparison => |comparison| (std.mem.eql(u8, comparison.field, "_id") and comparison.op != .eq) or
            (comparison.value == .string and std.mem.eql(u8, std.mem.trim(u8, comparison.value.string, " \t\r\n"), "null")),
        .is_null => false,
        .conjunction => |pair| needsResidual(pair.left) or needsResidual(pair.right),
        .disjunction, .negation, .scalar => true,
    };
}

pub fn predicateScalar(alloc: Allocator, table: ?catalog.Table, predicate: *const ast.Predicate) !*const ast.Scalar {
    var builder: Builder = .{ .alloc = alloc, .table = table, .columns = &.{}, .parameters = &.{} };
    return builder.predicateExpression(predicate);
}

/// allocator is the bounded binding arena. Nested Program arenas allocate from
/// it, so the enclosing binding owns every program and frees them together.
pub fn bind(alloc: Allocator, table: ?catalog.Table, statement: ast.Statement, parameters: []?ast.ColumnType) !Bound {
    if (statement == .insert) return bindInsert(alloc, table orelse return error.UndefinedTable, statement.insert, parameters);
    const needed = switch (statement) {
        .select => |select| blk: {
            if (needsResidual(select.predicate)) break :blk true;
            for (select.columns) |projection| if (projection.expression != null) break :blk true;
            for (select.order_by) |order| if (order.expression != null) break :blk true;
            break :blk false;
        },
        .update => |update| blk: {
            if (needsResidual(update.predicate)) break :blk true;
            for (update.assignments) |assignment| if (assignment.expression != null) break :blk true;
            break :blk false;
        },
        .delete => |delete| needsResidual(delete.predicate),
        else => false,
    };
    if (table != null and !needed) return .{};
    const table_columns: []const catalog.Column = if (table) |definition| definition.columns else &.{};
    const columns = try alloc.alloc(scalar.Column, table_columns.len + @intFromBool(table != null));
    for (table_columns, columns[0..table_columns.len]) |column, *out| out.* = .{ .name = column.name, .type = column.type, .nullable = column.nullable };
    if (table != null) columns[table_columns.len] = .{ .name = "_id", .type = .string, .nullable = false };
    var builder: Builder = .{ .alloc = alloc, .table = table, .columns = columns, .parameters = parameters };
    var out: Bound = .{ .columns = columns };
    const predicate = switch (statement) {
        .select => |select| select.predicate,
        .update => |update| update.predicate,
        .delete => |delete| delete.predicate,
        else => null,
    };
    const predicate_expression = if (predicate) |node| try builder.predicateExpression(node) else null;
    // Solve constraints across the statement before unconstrained projection
    // parameters default to text. Projection order must not change typing.
    var pass: usize = 0;
    while (true) : (pass += 1) {
        if (pass > parameters.len + 1) return error.ConflictingSqlParameterTypes;
        var changed = false;
        if (predicate_expression) |expression| changed = try scalar.inferParameters(alloc, expression, columns, parameters, .boolean, .{}) or changed;
        switch (statement) {
            .select => |select| {
                for (select.columns) |projection| if (projection.expression) |expression| {
                    changed = try scalar.inferParameters(alloc, expression, columns, parameters, null, .{}) or changed;
                };
                for (select.order_by) |order| if (order.expression) |expression| {
                    changed = try scalar.inferParameters(alloc, expression, columns, parameters, null, .{}) or changed;
                };
                for ([_]?ast.Value{ select.limit, select.offset }) |optional| if (optional) |node| {
                    if (node == .parameter) {
                        if (node.parameter == 0 or node.parameter > parameters.len) return error.InvalidSqlParameters;
                        const slot = &parameters[node.parameter - 1];
                        if (slot.*) |kind| {
                            if (kind != .integer) return error.ConflictingSqlParameterTypes;
                        } else {
                            slot.* = .integer;
                            changed = true;
                        }
                    }
                };
            },
            .update => |update| for (update.assignments) |assignment| {
                if (assignment.expression == null and assignment.value != .parameter) continue;
                const column = try (table orelse return error.UndefinedColumn).column(assignment.field);
                const expression = assignment.expression orelse try builder.node(.{ .literal = assignment.value });
                changed = try scalar.inferParameters(alloc, expression, columns, parameters, column.type, .{}) or changed;
            },
            else => {},
        }
        if (!changed) break;
    }
    if (predicate != null and (table == null or needsResidual(predicate))) {
        out.predicate = try builder.program(predicate_expression.?, .boolean);
        if (out.predicate.?.output_type.kind != null and out.predicate.?.output_type.kind != .boolean) return error.SqlTypeMismatch;
    }
    switch (statement) {
        .select => |select| {
            const programs = try alloc.alloc(?scalar.Program, select.columns.len);
            @memset(programs, null);
            for (select.columns, programs) |projection, *program| if (projection.expression) |expression| {
                program.* = try builder.program(expression, null);
            };
            out.projections = programs;
            const orders = try alloc.alloc(?scalar.Program, select.order_by.len);
            @memset(orders, null);
            for (select.order_by, orders) |order, *program| if (order.expression) |expression| {
                program.* = try builder.program(expression, null);
            };
            out.orders = orders;
        },
        .update => |update| {
            const programs = try alloc.alloc(?scalar.Program, update.assignments.len);
            @memset(programs, null);
            for (update.assignments, programs) |assignment, *program| if (assignment.expression) |expression| {
                const column = try (table orelse return error.UndefinedColumn).column(assignment.field);
                program.* = try builder.program(expression, column.type);
                const kind = program.*.?.output_type.kind;
                if (kind != null and kind != column.type and !(kind == .integer and column.type == .number)) return error.SqlTypeMismatch;
            };
            out.assignments = programs;
        },
        else => {},
    }
    out.required = try builder.required.toOwnedSlice(alloc);
    return out;
}

fn bindInsert(alloc: Allocator, table: catalog.Table, statement: ast.Insert, parameters: []?ast.ColumnType) !Bound {
    if (statement.expressions.len == 0) return .{};
    if (statement.expressions.len != statement.rows.len) return error.InvalidSqlParameters;
    if (statement.defaults.len != 0) {
        if (statement.defaults.len != statement.rows.len) return error.InvalidSqlParameters;
        for (statement.defaults) |mask| if (mask.len != statement.columns.len) return error.InvalidSqlParameters;
    }
    var builder: Builder = .{ .alloc = alloc, .table = null, .columns = &.{}, .parameters = parameters };
    var pass: usize = 0;
    while (true) : (pass += 1) {
        if (pass > parameters.len + 1) return error.ConflictingSqlParameterTypes;
        var changed = false;
        for (statement.rows, statement.expressions, 0..) |row, expressions, row_index| {
            if (row.len != statement.columns.len or expressions.len != row.len) return error.InvalidSqlParameters;
            for (row, expressions, statement.columns, 0..) |literal, expression, name, cell_index| {
                if (statement.isDefault(row_index, cell_index)) continue;
                const column = try table.column(name);
                const node = expression orelse try builder.node(.{ .literal = literal });
                changed = try scalar.inferParameters(alloc, node, &.{}, parameters, column.type, .{}) or changed;
            }
        }
        if (!changed) break;
    }
    const rows = try alloc.alloc([]const ?scalar.Program, statement.rows.len);
    for (statement.expressions, rows) |expressions, *row| {
        const programs = try alloc.alloc(?scalar.Program, expressions.len);
        @memset(programs, null);
        for (expressions, statement.columns, programs) |expression, name, *program| if (expression) |node| {
            const column = try table.column(name);
            program.* = try builder.program(node, column.type);
            const kind = program.*.?.output_type.kind;
            if (kind != null and kind != column.type and !(kind == .integer and column.type == .number)) return error.SqlTypeMismatch;
        };
        row.* = programs;
    }
    return .{ .insert_rows = rows };
}

const Builder = struct {
    alloc: Allocator,
    table: ?catalog.Table,
    columns: []const scalar.Column,
    parameters: []?ast.ColumnType,
    required: std.ArrayList(u32) = .empty,
    seen: std.AutoHashMapUnmanaged(u32, void) = .empty,

    fn program(self: *Builder, expression: *const ast.Scalar, expected: ?ast.ColumnType) !scalar.Program {
        const result = try scalar.bindExpected(self.alloc, expression, self.columns, self.parameters, expected, .{});
        if (result.parameter_types.len > self.parameters.len) return error.InvalidSqlParameters;
        for (result.parameter_types, self.parameters[0..result.parameter_types.len]) |inferred, *existing| {
            if (inferred) |kind| {
                if (existing.*) |prior| if (prior != kind) return error.ConflictingSqlParameterTypes;
                existing.* = kind;
            }
        }
        for (result.required_columns) |ordinal| {
            if (!(try self.seen.getOrPut(self.alloc, ordinal)).found_existing) try self.required.append(self.alloc, ordinal);
        }
        return result;
    }

    fn node(self: *Builder, value: ast.Scalar) !*const ast.Scalar {
        const result = try self.alloc.create(ast.Scalar);
        result.* = value;
        return result;
    }

    fn predicateExpression(self: *Builder, predicate: *const ast.Predicate) anyerror!*const ast.Scalar {
        return switch (predicate.*) {
            .scalar => |expression| expression,
            .comparison => |comparison| blk: {
                const column = try (self.table orelse return error.UndefinedColumn).column(comparison.field);
                const literal = try self.node(.{ .literal = comparison.value });
                const right = if (comparison.value == .null) literal else try self.node(.{ .cast = .{ .operand = literal, .type = column.type } });
                break :blk try self.node(.{ .binary = .{
                    .op = switch (comparison.op) {
                        inline else => |tag| @field(ast.Scalar.Binary, @tagName(tag)),
                    },
                    .left = try self.node(.{ .column = comparison.field }),
                    .right = right,
                } });
            },
            .is_null => |check| try self.node(.{ .unary = .{ .op = if (check.negated) .is_not_null else .is_null, .operand = try self.node(.{ .column = check.field }) } }),
            .conjunction, .disjunction => |pair| try self.node(.{ .binary = .{ .op = if (predicate.* == .conjunction) .@"and" else .@"or", .left = try self.predicateExpression(pair.left), .right = try self.predicateExpression(pair.right) } }),
            .negation => |inner| try self.node(.{ .unary = .{ .op = .not, .operand = try self.predicateExpression(inner) } }),
        };
    }
};
