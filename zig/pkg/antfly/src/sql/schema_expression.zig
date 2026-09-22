// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! SQL DDL shares scalar binding/type inference with query execution, then
//! lowers only operations implemented by the durable native expression VM.
const std = @import("std");
const ast = @import("ast.zig");
const scalar = @import("scalar.zig");
const Json = std.json.Value;

fn json(alloc: std.mem.Allocator, input: anytype) !Json {
    return std.json.parseFromSliceLeaky(Json, alloc, try std.json.Stringify.valueAlloc(alloc, input, .{}), .{ .parse_numbers = false });
}

pub fn lower(alloc: std.mem.Allocator, schema: Json, expression: *const ast.Scalar, expected: ?ast.ColumnType) !Json {
    return (try lowerTyped(alloc, schema, expression, expected)).expression;
}

pub fn lowerTyped(alloc: std.mem.Allocator, schema: Json, expression: *const ast.Scalar, expected: ?ast.ColumnType) !struct { expression: Json, type: ast.ColumnType } {
    const default_type = schema.object.get("default_type") orelse return error.InvalidSqlBackendResponse;
    const row = schema.object.get("document_schemas").?.object.get(default_type.string).?.object.get("schema").?;
    const properties = row.object.get("properties").?;
    var columns = std.ArrayList(scalar.Column).empty;
    for (properties.object.keys(), properties.object.values()) |name, property| {
        const wire_type = property.object.get("type").?.string;
        const kind: ast.ColumnType = if (std.mem.eql(u8, wire_type, "keyword")) .string else std.meta.stringToEnum(ast.ColumnType, wire_type) orelse return error.UnsupportedSqlShape;
        try columns.append(alloc, .{ .name = name, .type = kind });
    }
    var program = try scalar.bindExpected(alloc, expression, columns.items, &.{}, expected, .{});
    defer program.deinit();
    if (program.parameter_types.len != 0) return error.InvalidSqlParameters;
    const values = try alloc.alloc(Json, program.instructions.len);
    for (program.instructions, values) |instruction, *out| {
        const kind = instruction.type.kind orelse return error.SqlTypeMismatch;
        out.* = switch (instruction.operation) {
            .literal => |literal| try json(alloc, .{ .op = "literal", .type = @tagName(kind), .value = literal }),
            .column => |ordinal| try json(alloc, .{ .op = "column", .column = columns.items[ordinal].name }),
            .parameter => return error.InvalidSqlParameters,
            .unary => |part| blk: {
                if (part.op == .positive) break :blk values[part.operand];
                const op: []const u8 = switch (part.op) {
                    .negative => "negate",
                    .not => "not",
                    .is_null => "is_null",
                    .is_not_null => "is_not_null",
                    else => return error.UnsupportedSqlShape,
                };
                break :blk try json(alloc, .{ .op = op, .args = &[_]Json{values[part.operand]} });
            },
            .binary => |part| blk: {
                const op: []const u8 = switch (part.op) {
                    .neq => "ne",
                    .add, .subtract, .multiply, .divide, .concat, .eq, .lt, .lte, .gt, .gte, .@"and", .@"or", .is_distinct, .is_not_distinct => @tagName(part.op),
                    else => return error.UnsupportedSqlShape,
                };
                break :blk try json(alloc, .{ .op = op, .args = &[_]Json{ values[part.left], values[part.right] } });
            },
            .call => |part| blk: {
                const op: []const u8 = switch (part.function) {
                    .lower => "lower_ascii",
                    .upper => "upper_ascii",
                    .coalesce => "coalesce",
                    else => return error.UnsupportedSqlShape,
                };
                const args = try alloc.alloc(Json, part.args.len);
                for (args, part.args) |*arg, index| arg.* = values[index];
                break :blk try json(alloc, .{ .op = op, .args = args });
            },
            .cast => |part| if (program.instructions[part.operand].type.kind == part.type) values[part.operand] else return error.UnsupportedSqlShape,
            .case_when, .in_list => return error.UnsupportedSqlShape,
        };
    }
    return .{ .expression = values[program.root], .type = program.output_type.kind orelse return error.SqlTypeMismatch };
}

pub fn lowerIndexPredicate(alloc: std.mem.Allocator, schema: Json, expression: *const ast.Scalar) ![]const Json {
    const native = try lower(alloc, schema, expression, .boolean);
    var predicates = std.ArrayList(Json).empty;
    try collectPredicates(alloc, native, &predicates);
    return predicates.toOwnedSlice(alloc);
}

fn collectPredicates(alloc: std.mem.Allocator, expression: Json, predicates: *std.ArrayList(Json)) anyerror!void {
    const op = expression.object.get("op").?.string;
    const args = expression.object.get("args") orelse return error.UnsupportedSqlShape;
    if (std.mem.eql(u8, op, "and")) {
        for (args.array.items) |arg| try collectPredicates(alloc, arg, predicates);
        return;
    }
    if (predicates.items.len >= 256 or args.array.items.len == 0) return error.SqlLimitExceeded;
    const left = args.array.items[0];
    const column = left.object.get("column") orelse return error.UnsupportedSqlShape;
    if (std.mem.eql(u8, op, "is_null") or std.mem.eql(u8, op, "is_not_null")) {
        try predicates.append(alloc, try json(alloc, .{ .column = column.string, .op = op }));
        return;
    }
    const allowed = for ([_][]const u8{ "eq", "ne", "lt", "lte", "gt", "gte" }) |candidate| {
        if (std.mem.eql(u8, op, candidate)) break true;
    } else false;
    if (!allowed or args.array.items.len != 2) return error.UnsupportedSqlShape;
    const literal = args.array.items[1];
    if (!std.mem.eql(u8, literal.object.get("op").?.string, "literal")) return error.UnsupportedSqlShape;
    try predicates.append(alloc, try json(alloc, .{ .column = column.string, .op = op, .value = literal.object.get("value") orelse .null }));
}
