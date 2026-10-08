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

fn promoteNumeric(alloc: std.mem.Allocator, value: Json, source: scalar.Type, target: scalar.Type) !Json {
    if (target.kind != .integer and target.kind != .number) return value;
    const identity: @import("array_value.zig").ElementType = target.element_type orelse if (target.kind == .integer) .int64 else .float64;
    const source_identity: @import("array_value.zig").ElementType = source.element_type orelse if (source.kind == .integer) .int64 else .float64;
    if (source.kind == target.kind and source_identity == identity) return value;
    return json(alloc, .{ .op = "cast", .type = @tagName(target.kind.?), .sql_type = @tagName(identity), .args = &[_]Json{value} });
}

fn json(alloc: std.mem.Allocator, input: anytype) !Json {
    return std.json.parseFromSliceLeaky(Json, alloc, try std.json.Stringify.valueAlloc(alloc, input, .{}), .{ .parse_numbers = false });
}

pub fn lower(alloc: std.mem.Allocator, schema: Json, expression: *const ast.Scalar, expected: ?ast.ColumnType) !Json {
    return (try lowerTyped(alloc, schema, expression, expected)).expression;
}

const Lowered = struct { expression: Json, type: ast.ColumnType, element_type: ?@import("array_value.zig").ElementType };

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
        // Arrays still need a native expression value domain. Numeric builtin
        // widths, however, travel with each operation rather than disappearing
        // into its coarse physical integer/number result kind.
        if (kind == .array) return error.UnsupportedSqlShape;
        // Query NUMERIC support must not publish a decimal program into the
        // native expression VM, whose row values still lack decimal limbs.
        if (instruction.type.element_type == .numeric) return error.UnsupportedSqlShape;
        if (kind == .number and instruction.type.element_type == null) switch (instruction.operation) {
            .binary => |part| switch (part.op) {
                .add, .subtract, .multiply, .divide => return error.UnsupportedSqlShape,
                else => {},
            },
            .unary => |part| if (part.op == .negative) return error.UnsupportedSqlShape,
            else => {},
        };
        out.* = switch (instruction.operation) {
            .literal => |literal| try json(alloc, .{ .op = "literal", .type = if (kind == .uuid) "string" else @tagName(kind), .value = literal }),
            .column => |ordinal| try json(alloc, .{ .op = "column", .column = columns[ordinal].name }),
            .parameter => return error.InvalidSqlParameters,
            .unary => |part| blk: {
                if (part.op == .positive) break :blk values[part.operand];
                switch (part.op) {
                    .is_true, .is_not_true, .is_false, .is_not_false => {
                        const literal = try json(alloc, .{ .op = "literal", .type = "boolean", .value = part.op == .is_true or part.op == .is_not_true });
                        break :blk try json(alloc, .{ .op = if (part.op == .is_true or part.op == .is_false) "is_not_distinct" else "is_distinct", .args = &[_]Json{ values[part.operand], literal } });
                    },
                    else => {},
                }
                const op: []const u8 = switch (part.op) {
                    .negative => "negate",
                    .not => "not",
                    .is_null => "is_null",
                    .is_not_null => "is_not_null",
                    .is_unknown => "is_null",
                    .is_not_unknown => "is_not_null",
                    else => return error.UnsupportedSqlShape,
                };
                break :blk try json(alloc, .{ .op = op, .args = &[_]Json{values[part.operand]} });
            },
            .binary => |part| blk: {
                const op: []const u8 = switch (part.op) {
                    .neq => "ne",
                    .add, .subtract, .multiply, .divide, .modulo, .concat, .eq, .lt, .lte, .gt, .gte, .@"and", .@"or", .is_distinct, .is_not_distinct => @tagName(part.op),
                    else => return error.UnsupportedSqlShape,
                };
                var left = values[part.left];
                var right = values[part.right];
                switch (part.op) {
                    .add, .subtract, .multiply, .divide, .modulo => {
                        if (part.op == .modulo and kind != .integer) return error.UnsupportedSqlShape;
                        // The query VM promotes operands at execution time. A
                        // durable program must record that promotion explicitly.
                        const indexes = [_]usize{ part.left, part.right };
                        const operands = [_]*Json{ &left, &right };
                        for (indexes, operands) |index, operand| {
                            operand.* = try promoteNumeric(alloc, operand.*, program.instructions[index].type, instruction.type);
                        }
                    },
                    else => {},
                }
                break :blk try json(alloc, .{ .op = op, .args = &[_]Json{ left, right } });
            },
            .call => |part| blk: {
                const op: []const u8 = switch (part.function) {
                    .lower => "lower_ascii",
                    .upper => "upper_ascii",
                    .coalesce => "coalesce",
                    .mod => "modulo",
                    else => return error.UnsupportedSqlShape,
                };
                const args = try alloc.alloc(Json, part.args.len);
                if (part.function == .mod and kind != .integer) return error.UnsupportedSqlShape;
                for (args, part.args) |*arg, index| arg.* = if (part.function == .coalesce or part.function == .mod)
                    try promoteNumeric(alloc, values[index], program.instructions[index].type, instruction.type)
                else
                    values[index];
                if (part.function == .coalesce and args.len == 1) break :blk args[0];
                var result = try json(alloc, .{ .op = op, .args = args });
                if (part.function == .mod) if (instruction.type.element_type) |identity| try result.object.put(alloc, "sql_type", .{ .string = @tagName(identity) });
                break :blk result;
            },
            .cast => |part| blk: {
                const source = program.instructions[part.operand].type;
                if ((source.kind == .integer or source.kind == .number) and (part.type == .integer or part.type == .number)) {
                    const target_type: @import("array_value.zig").ElementType = part.element_type orelse if (part.type == .integer) .int64 else .float64;
                    break :blk try json(alloc, .{ .op = "cast", .type = @tagName(part.type), .sql_type = @tagName(target_type), .args = &[_]Json{values[part.operand]} });
                }
                if (source.kind != part.type) return error.UnsupportedSqlShape;
                if (source.element_type != part.element_type) return error.UnsupportedSqlShape;
                break :blk values[part.operand];
            },
            .case_when => |part| blk: {
                if (part.branches.len == 0 or part.branches.len > 15) return error.SqlLimitExceeded;
                const args = try alloc.alloc(Json, part.branches.len * 2 + 1);
                for (part.branches, 0..) |branch, i| {
                    args[i * 2] = values[branch.condition];
                    args[i * 2 + 1] = try promoteNumeric(alloc, values[branch.value], program.instructions[branch.value].type, instruction.type);
                }
                args[args.len - 1] = if (part.otherwise) |other|
                    try promoteNumeric(alloc, values[other], program.instructions[other].type, instruction.type)
                else if (kind == .integer or kind == .number)
                    try json(alloc, .{ .op = "literal", .type = @tagName(kind), .sql_type = @tagName(instruction.type.element_type orelse if (kind == .integer) @as(@import("array_value.zig").ElementType, .int64) else .float64), .value = @as(?u8, null) })
                else
                    try json(alloc, .{ .op = "literal", .type = if (kind == .uuid) "string" else @tagName(kind), .value = @as(?u8, null) });
                break :blk try json(alloc, .{ .op = "case_when", .args = args });
            },
            .in_list => |part| blk: {
                if (part.values.len == 0 or part.values.len > 31) return error.SqlLimitExceeded;
                const indexes = try alloc.alloc(u32, part.values.len + 1);
                indexes[0] = part.operand;
                @memcpy(indexes[1..], part.values);
                var common_type = program.instructions[part.operand].type;
                if (common_type.kind == .integer or common_type.kind == .number) {
                    if (common_type.kind == .number and common_type.element_type == null) return error.UnsupportedSqlShape;
                    for (indexes[1..]) |index| {
                        const other = program.instructions[index].type;
                        if (other.kind == .number and other.element_type == null) return error.UnsupportedSqlShape;
                        const identity = try @import("builtin_cast.zig").commonNumeric(common_type.element_type orelse if (common_type.kind == .integer) .int64 else .float64, other.element_type orelse if (other.kind == .integer) .int64 else .float64);
                        common_type = .{ .kind = if (@import("builtin_cast.zig").integral(identity)) .integer else .number, .element_type = identity };
                    }
                }
                const args = try alloc.alloc(Json, indexes.len);
                for (indexes, args) |index, *arg| arg.* = try promoteNumeric(alloc, values[index], program.instructions[index].type, common_type);
                break :blk try json(alloc, .{ .op = if (part.negated) "not_in_list" else "in_list", .args = args });
            },
        };
        const typed_numeric = switch (instruction.operation) {
            .literal => kind == .integer or kind == .number,
            .binary => |part| switch (part.op) {
                .add, .subtract, .multiply, .divide, .modulo => true,
                else => false,
            },
            .unary => |part| part.op == .negative,
            else => false,
        };
        if (typed_numeric) if (instruction.type.element_type) |identity| {
            try out.object.put(alloc, "sql_type", .{ .string = @tagName(identity) });
        };
    }
    return .{ .expression = values[program.root], .type = program.output_type.kind orelse return error.SqlTypeMismatch, .element_type = program.output_type.element_type };
}

/// Persist an assignment program, not its current result. Arithmetic and casts
/// retain write-time failures, while literal input coercion uses the same
/// declared-type admission as existing defaults. Callers provide the complete
/// candidate schema so forward base-column references bind deterministically.
pub fn lowerAssignment(alloc: std.mem.Allocator, schema: Json, expression: *const ast.Scalar, column: ast.Column, generated: bool) !Json {
    if (generated) try rejectGeneratedReferences(schema, column.name, expression);
    if (expression.* == .literal) return @import("ddl_runtime.zig").defaultExpression(alloc, expression.literal, column.type, column.element_type);
    const lowered = (if (generated)
        lowerTyped(alloc, schema, expression, column.type)
    else
        lowerColumns(alloc, &.{}, expression, column.type)) catch |err| switch (err) {
        error.UnknownColumn => return error.UndefinedColumn,
        else => return err,
    };
    if (column.type == .integer or column.type == .number) {
        if (lowered.type != .integer and lowered.type != .number) return error.SqlAssignmentTypeMismatch;
        // Untyped SQL decimal arithmetic requires an exact decimal value
        // domain; never persist binary-float evaluation under that contract.
        if (lowered.type == .number and lowered.element_type == null) return error.UnsupportedSqlShape;
        const target: @import("array_value.zig").ElementType = column.element_type orelse if (column.type == .integer) .int64 else .float64;
        return json(alloc, .{ .op = "cast", .type = @tagName(column.type), .sql_type = @tagName(target), .args = &[_]Json{lowered.expression} });
    }
    if (lowered.type != column.type) return error.SqlAssignmentTypeMismatch;
    return lowered.expression;
}

fn rejectGeneratedReferences(schema: Json, name: []const u8, expression: *const ast.Scalar) anyerror!void {
    switch (expression.*) {
        .column => |reference| {
            if (std.mem.eql(u8, name, reference)) return error.SqlInvalidGenerationExpression;
            if (schema.object.get("generated_columns")) |definitions| {
                if (definitions != .array) return error.InvalidSqlBackendResponse;
                for (definitions.array.items) |definition| {
                    if (definition != .object) return error.InvalidSqlBackendResponse;
                    const generated = definition.object.get("column") orelse return error.InvalidSqlBackendResponse;
                    if (generated != .string) return error.InvalidSqlBackendResponse;
                    if (std.mem.eql(u8, generated.string, reference)) return error.SqlInvalidGenerationExpression;
                }
            }
        },
        .unary => |part| try rejectGeneratedReferences(schema, name, part.operand),
        .binary => |part| {
            try rejectGeneratedReferences(schema, name, part.left);
            try rejectGeneratedReferences(schema, name, part.right);
        },
        .cast => |part| try rejectGeneratedReferences(schema, name, part.operand),
        .call => |part| for (part.args) |arg| try rejectGeneratedReferences(schema, name, arg),
        .case_when => |part| {
            for (part.branches) |branch| {
                try rejectGeneratedReferences(schema, name, branch.condition);
                try rejectGeneratedReferences(schema, name, branch.value);
            }
            if (part.otherwise) |other| try rejectGeneratedReferences(schema, name, other);
        },
        .in_list => |part| {
            try rejectGeneratedReferences(schema, name, part.operand);
            for (part.values) |item| try rejectGeneratedReferences(schema, name, item);
        },
        .literal => {},
    }
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
    for ([_][]const u8{ "n + n > 0", "CAST(n AS smallint) + CAST(n AS smallint) > 0", "+n > 0", "n > -1" }) |sql| {
        var numeric = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer numeric.deinit();
        _ = try lower(a, schema, numeric.expression, .boolean);
    }
    var array = try @import("compiler.zig").compileScalar(a, "cold IS NULL", .{});
    defer array.deinit();
    try std.testing.expectError(error.UnsupportedSqlShape, lower(a, schema, array.expression, .boolean));
}

test "SQL schema conditional expressions enforce durable branch admission" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const columns = [_]scalar.Column{.{ .name = "n", .type = .integer, .element_type = .int16 }};
    const branches: [16][]const u8 = @splat("WHEN n>0 THEN n ");
    for ([_]usize{ 15, 16 }) |count| {
        const sql = try std.mem.concat(a, u8, &.{ "CASE ", try std.mem.join(a, "", branches[0..count]), "ELSE n END" });
        var compiled = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer compiled.deinit();
        if (count == 16) {
            try std.testing.expectError(error.SqlLimitExceeded, lowerColumns(a, &columns, compiled.expression, null));
        } else {
            const lowered = try lowerColumns(a, &columns, compiled.expression, null);
            try std.testing.expectEqual(@as(usize, 31), lowered.expression.object.get("args").?.array.items.len);
        }
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
