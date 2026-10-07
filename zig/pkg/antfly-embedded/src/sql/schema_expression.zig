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
    const properties = try @import("schema_columns.zig").properties(schema);
    var columns = std.ArrayList(scalar.Column).empty;
    for (properties.object.keys(), properties.object.values()) |name, property| {
        const column = try @import("schema_columns.zig").column(name, property);
        try columns.append(alloc, .{ .name = name, .type = column.type, .element_type = column.element_type });
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
        // The durable expression VM currently has int64/float64 arithmetic,
        // not the query VM's narrower builtin overflow/rounding contracts.
        // Keep shape discovery complete without silently lowering a different
        // operation or manufacturing JSON-null placeholders for typed arrays.
        if (kind == .array) return error.UnsupportedSqlShape;
        if (instruction.operation == .binary or instruction.operation == .unary) {
            const identity = instruction.type.element_type;
            if ((kind == .integer and identity != null and identity != .int64) or
                (kind == .number and identity == .float32)) return error.UnsupportedSqlShape;
        }
        out.* = switch (instruction.operation) {
            .literal => |literal| try json(alloc, .{ .op = "literal", .type = if (kind == .uuid) "string" else @tagName(kind), .value = literal }),
            .column => |ordinal| try json(alloc, .{ .op = "column", .column = columns[ordinal].name }),
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
            .cast => |part| blk: {
                const source = program.instructions[part.operand].type;
                if (source.kind != part.type) return error.UnsupportedSqlShape;
                if (source.element_type != part.element_type) {
                    const from = source.element_type orelse return error.UnsupportedSqlShape;
                    const to = part.element_type orelse return error.UnsupportedSqlShape;
                    const casts = @import("builtin_cast.zig");
                    if (!casts.integral(from) or !casts.integral(to) or try casts.commonNumeric(from, to) != to) return error.UnsupportedSqlShape;
                }
                break :blk values[part.operand];
            },
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

test "SQL schema expressions bind nullable catalog shapes and cold typed arrays" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const schema = try std.json.parseFromSliceLeaky(Json, a,
        \\{"default_type":"row","document_schemas":{"row":{"schema":{"properties":{"n":{"type":["integer","null"],"x-antfly-sql-type":"int16"},"label":{"type":["keyword","null"]},"cold":{"type":["sql_array","null"],"x-antfly-sql-type":"int64"}}}}}}
    , .{});
    var check = try @import("compiler.zig").compileScalar(a, "n > 0 AND lower(label) = 'ready'", .{});
    defer check.deinit();
    const lowered = try lower(a, schema, check.expression, .boolean);
    try std.testing.expectEqualStrings("and", lowered.object.get("op").?.string);
    var index = try @import("compiler.zig").compileScalar(a, "lower(label)", .{});
    defer index.deinit();
    const key = try lowerTyped(a, schema, index.expression, null);
    try std.testing.expectEqual(ast.ColumnType.string, key.type);
    // Binding may discover every declared type, but may not erase an array
    // dependency or narrow arithmetic into the native int64-only VM.
    for ([_][]const u8{ "n + n > 0", "cold IS NULL", "CAST(n AS smallint) + CAST(n AS smallint) > 0" }) |sql| {
        var unsupported = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer unsupported.deinit();
        try std.testing.expectError(error.UnsupportedSqlShape, lower(a, schema, unsupported.expression, .boolean));
    }
}

test "SQL schema expression catalog decoding rejects malformed and conflicting metadata" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var expression = try @import("compiler.zig").compileScalar(a, "n > 0", .{});
    defer expression.deinit();
    for ([_][]const u8{ "null", "{}", "{\"default_type\":1}", "{\"default_type\":\"row\",\"document_schemas\":null}" }) |text| {
        const schema = try std.json.parseFromSliceLeaky(Json, a, text, .{});
        try std.testing.expectError(error.InvalidSqlBackendResponse, lower(a, schema, expression.expression, .boolean));
    }
    for ([_][]const u8{ "null", "{\"type\":3}", "{\"type\":[\"integer\",1]}", "{\"type\":\"sql_array\"}", "{\"type\":\"boolean\",\"x-antfly-sql-type\":\"int16\"}" }) |text| {
        const property = try std.json.parseFromSliceLeaky(Json, a, text, .{});
        try std.testing.expectError(error.InvalidSqlBackendResponse, @import("schema_columns.zig").column("n", property));
    }
}

test "SQL schema expression nullable catalog binding cleans up allocation faults" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const owned = arena.allocator();
            const schema = try std.json.parseFromSliceLeaky(Json, owned,
                \\{"default_type":"row","document_schemas":{"row":{"schema":{"properties":{"n":{"type":["integer","null"],"x-antfly-sql-type":"int16"},"cold":{"type":"sql_array","x-antfly-sql-type":"int64"}}}}}}
            , .{});
            var check = try @import("compiler.zig").compileScalar(owned, "n > 0", .{});
            defer check.deinit();
            _ = try lower(owned, schema, check.expression, .boolean);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}
