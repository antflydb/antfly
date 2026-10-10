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

const Lowered = struct { expression: Json, type: ast.ColumnType };

pub fn lowerTyped(alloc: std.mem.Allocator, schema: Json, expression: *const ast.Scalar, expected: ?ast.ColumnType) !Lowered {
    const default_type = schema.object.get("default_type") orelse return error.InvalidSqlBackendResponse;
    const row = schema.object.get("document_schemas").?.object.get(default_type.string).?.object.get("schema").?;
    const properties = row.object.get("properties").?;
    var columns = std.ArrayList(scalar.Column).empty;
    for (properties.object.keys(), properties.object.values()) |name, property| {
        const wire_type = property.object.get("type").?.string;
        const format = property.object.get("format") orelse .null;
        const kind: ast.ColumnType = if (format == .string and std.mem.eql(u8, format.string, "uuid") and
            (std.mem.eql(u8, wire_type, "keyword") or std.mem.eql(u8, wire_type, "string") or std.mem.eql(u8, wire_type, "text")))
            .uuid
        else if (std.mem.eql(u8, wire_type, "keyword")) .string else std.meta.stringToEnum(ast.ColumnType, wire_type) orelse return error.UnsupportedSqlShape;
        try columns.append(alloc, .{ .name = name, .type = kind });
    }
    return lowerColumns(alloc, columns.items, expression, expected);
}

pub fn lowerColumns(alloc: std.mem.Allocator, columns: []const scalar.Column, expression: *const ast.Scalar, expected: ?ast.ColumnType) !Lowered {
    var program = try scalar.bindExpected(alloc, expression, columns, &.{}, expected, .{});
    defer program.deinit();
    if (program.parameter_types.len != 0) return error.InvalidSqlParameters;
    const values = try alloc.alloc(Json, program.instructions.len);
    for (program.instructions, values) |instruction, *out| {
        const kind = instruction.type.kind orelse return error.SqlTypeMismatch;
        out.* = switch (instruction.operation) {
            .literal => |literal| try json(alloc, .{ .op = "literal", .type = if (kind == .uuid) "string" else @tagName(kind), .value = literal }),
            .column => |ordinal| try json(alloc, .{ .op = "column", .column = columns[ordinal].name }),
            .parameter => return error.InvalidSqlParameters,
            .unary => |part| blk: {
                if (part.op == .positive) break :blk values[part.operand];
                if (part.op == .is_true or part.op == .is_false or part.op == .is_not_true or part.op == .is_not_false) {
                    // IS boolean tests never return NULL. Native distinctness
                    // comparisons preserve that contract for nullable inputs.
                    break :blk try json(alloc, .{
                        .op = if (part.op == .is_not_true or part.op == .is_not_false) "is_distinct" else "is_not_distinct",
                        .args = &[_]Json{ values[part.operand], try json(alloc, .{ .op = "literal", .type = "boolean", .value = part.op == .is_true or part.op == .is_not_true }) },
                    });
                }
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
    const lowered = try lowerTyped(alloc, schema, expression, .boolean);
    if (lowered.type != .boolean) return error.SqlTypeMismatch;
    const native = lowered.expression;
    var predicates = std.ArrayList(Json).empty;
    try collectPredicates(alloc, native, false, &predicates);
    return predicates.toOwnedSlice(alloc);
}

// Partial indexes store a conjunction of native predicates. Normalize SQL
// truth tests in this context only: UNKNOWN and FALSE both exclude an index
// row, while negated IS tests must retain their null-safe semantics.
const PredicateOp = enum { eq, ne, lt, lte, gt, gte, is_null, is_not_null, is_distinct, is_not_distinct };

fn inverse(op: PredicateOp) PredicateOp {
    return switch (op) {
        .eq => .ne,
        .ne => .eq,
        .lt => .gte,
        .lte => .gt,
        .gt => .lte,
        .gte => .lt,
        .is_null => .is_not_null,
        .is_not_null => .is_null,
        .is_distinct => .is_not_distinct,
        .is_not_distinct => .is_distinct,
    };
}

fn appendPredicate(alloc: std.mem.Allocator, predicates: *std.ArrayList(Json), predicate: Json) !void {
    if (predicates.items.len >= 256) return error.SqlLimitExceeded;
    try predicates.append(alloc, predicate);
}

fn collectPredicates(alloc: std.mem.Allocator, expression: Json, negated: bool, predicates: *std.ArrayList(Json)) anyerror!void {
    const operation = expression.object.get("op").?.string;
    if (std.mem.eql(u8, operation, "column")) {
        try appendPredicate(alloc, predicates, try json(alloc, .{ .column = expression.object.get("column").?.string, .op = "eq", .value = !negated }));
        return;
    }
    const args = (expression.object.get("args") orelse return error.UnsupportedSqlShape).array.items;
    if (std.mem.eql(u8, operation, "not")) {
        if (args.len != 1) return error.UnsupportedSqlShape;
        return collectPredicates(alloc, args[0], !negated, predicates);
    }
    // De Morgan's law permits NOT (a OR b), whose native form is a
    // conjunction. Disjunctions still require a richer native index format.
    if ((!negated and std.mem.eql(u8, operation, "and")) or (negated and std.mem.eql(u8, operation, "or"))) {
        for (args) |arg| try collectPredicates(alloc, arg, negated, predicates);
        return;
    }
    var op = std.meta.stringToEnum(PredicateOp, operation) orelse return error.UnsupportedSqlShape;
    if (negated) op = inverse(op);
    if (args.len == 0) return error.UnsupportedSqlShape;
    const column = args[0].object.get("column") orelse return error.UnsupportedSqlShape;
    if (op == .is_null or op == .is_not_null) {
        try appendPredicate(alloc, predicates, try json(alloc, .{ .column = column.string, .op = @tagName(op) }));
        return;
    }
    if (args.len != 2) return error.UnsupportedSqlShape;
    const literal = args[1];
    if (!std.mem.eql(u8, literal.object.get("op").?.string, "literal")) return error.UnsupportedSqlShape;
    var value = literal.object.get("value") orelse .null;
    if (value == .bool) {
        // Equality and inequality exclude NULL in index membership. Distinct
        // must retain NULL even for NOT NULL declarations: historical row
        // layouts can lack columns added after those rows were written.
        if (op == .is_not_distinct) op = .eq;
        if (op == .ne) {
            op = .eq;
            value = .{ .bool = !value.bool };
        }
    }
    try appendPredicate(alloc, predicates, try json(alloc, .{ .column = column.string, .op = @tagName(op), .value = value }));
}
