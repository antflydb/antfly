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
const ExpressionContext = enum { generation, index_key, index_predicate, column_default };

fn nativeType(kind: ast.ColumnType, identity: ?@import("array_value.zig").ElementType) []const u8 {
    return if (kind == .array) "sql_array" else if (kind == .number and identity == .numeric) "numeric" else if (kind == .uuid) "string" else @tagName(kind);
}

fn nullLiteral(instruction: scalar.Instruction) bool {
    return instruction.operation == .literal and instruction.operation.literal == .null;
}

fn promoteNumeric(alloc: std.mem.Allocator, value: Json, source: scalar.Type, target: scalar.Type) !Json {
    if (value == .object and value.object.get("value") != null and value.object.get("value").? == .null and
        !std.meta.eql(value.object.get("json_null"), @as(?Json, .{ .bool = true })) and
        std.mem.eql(u8, value.object.get("op").?.string, "literal"))
    {
        // SQL NULL adopts the destination domain; it must not become a
        // runtime cast of the unknown literal's placeholder string type.
        const kind = target.kind orelse return error.UnsupportedSqlShape;
        var result = try json(alloc, .{ .op = "literal", .type = nativeType(kind, target.element_type), .value = @as(?u8, null) });
        if (kind == .array or kind == .integer or kind == .number) if (target.element_type) |identity| try result.object.put(alloc, "sql_type", .{ .string = @tagName(identity) });
        return result;
    }
    if (target.kind == .array) {
        _ = target.element_type orelse return error.UnsupportedSqlShape;
        if (source.kind != .array or source.element_type != target.element_type) return error.UnsupportedSqlShape;
        return value;
    }
    if (target.kind != .integer and target.kind != .number) return value;
    const identity: @import("array_value.zig").ElementType = target.element_type orelse if (target.kind == .integer) .int64 else .float64;
    const source_identity: @import("array_value.zig").ElementType = source.element_type orelse if (source.kind == .integer) .int64 else .float64;
    if (source.kind == target.kind and source_identity == identity) return value;
    // Widen an integer literal's declared identity directly. Wrapping every
    // candidate in a cast needlessly doubles durable IN-list node admission.
    const casts = @import("builtin_cast.zig");
    if (source.kind == .integer and target.kind == .integer and casts.integral(source_identity) and casts.integral(identity) and
        @backingInt(source_identity) <= @backingInt(identity) and value == .object and
        std.mem.eql(u8, value.object.get("op").?.string, "literal"))
    {
        var result: Json = .{ .object = .empty };
        for (value.object.keys(), value.object.values()) |key, item| try result.object.put(alloc, key, item);
        try result.object.put(alloc, "sql_type", .{ .string = @tagName(identity) });
        return result;
    }
    return json(alloc, .{ .op = "cast", .type = nativeType(target.kind.?, identity), .sql_type = @tagName(identity), .args = &[_]Json{value} });
}

fn json(alloc: std.mem.Allocator, input: anytype) !Json {
    // Operator arguments already belong to this preparation arena and become
    // immutable once published into their parent. Copy the argument vector,
    // not the complete child trees through a serialize/parse round trip.
    // Literal payloads still take the owning path below; they may borrow a
    // bound program or temporary constant-folding region.
    if (comptime @hasField(@TypeOf(input), "op") and @hasField(@TypeOf(input), "args") and @typeInfo(@TypeOf(input)).@"struct".field_names.len == 2) {
        var object: std.json.ObjectMap = .empty;
        try object.ensureTotalCapacity(alloc, 2);
        var args = std.json.Array.init(alloc);
        try args.appendSlice(input.args);
        inline for (@typeInfo(@TypeOf(input)).@"struct".field_names) |name| {
            const value: Json = if (comptime std.mem.eql(u8, name, "op"))
                .{ .string = try alloc.dupe(u8, input.op) }
            else
                .{ .array = args };
            object.putAssumeCapacityNoClobber(try alloc.dupe(u8, name), value);
        }
        return .{ .object = object };
    }
    return std.json.parseFromSliceLeaky(Json, alloc, try std.json.Stringify.valueAlloc(alloc, input, .{}), .{ .parse_numbers = false });
}

fn inputLiteral(alloc: std.mem.Allocator, result: scalar.Type, value: scalar.Datum) !Json {
    if (value.temporal() != null) return error.UnsupportedSqlShape;
    const kind = result.kind orelse return error.SqlTypeMismatch;
    const identity = result.element_type;
    if (kind == .array) return json(alloc, .{
        .op = "literal",
        .type = "sql_array",
        .sql_type = @tagName(identity orelse return error.SqlTypeMismatch),
        .value = if (value.sql_null) Json.null else try @import("array_wire.zig").toJsonLeaky(alloc, value.array.?.*, .{}),
    });
    if (value.numeric) |number| {
        var context: @import("numeric_value.zig").Context = .{ .alloc = alloc };
        return json(alloc, .{ .op = "literal", .type = "numeric", .sql_type = "numeric", .value = try @import("numeric_value.zig").format(&context, number.*) });
    }
    if (kind == .json) return json(alloc, .{
        .op = "literal",
        .type = "json",
        .value = value.value,
        .json_null = !value.sql_null and value.value == .null,
    });
    if (value.value == .float and !std.math.isFinite(value.value.float)) return error.UnsupportedSqlShape;
    // Untyped numeric constants have a logical domain but no explicit builtin
    // identity. Match the ordinary literal lowering path instead of asserting
    // that constant folding supplied a subtype it never owned.
    if ((kind == .integer or kind == .number) and identity != null) return json(alloc, .{ .op = "literal", .type = nativeType(kind, identity), .sql_type = @tagName(identity.?), .value = value.value });
    return json(alloc, .{ .op = "literal", .type = nativeType(kind, identity), .value = value.value });
}

/// Preserve PostgreSQL input-function SQLSTATEs before handing a durable
/// literal to schema validation, whose malformed-program errors are broader.
pub fn numericLiteral(alloc: std.mem.Allocator, value: Json) !Json {
    if (value != .null) {
        var context: @import("numeric_value.zig").Context = .{ .alloc = alloc };
        var parsed = try @import("numeric_storage.zig").fromJson(&context, value);
        defer parsed.deinit();
    }
    // SQL numeric lexemes (for example .00994) are not necessarily JSON
    // numbers. The public exact-literal contract accepts decimal strings;
    // transport the validated lexeme without a lossy float conversion or
    // emitting invalid JSON from number_string.
    const wire = if (value == .number_string) Json{ .string = value.number_string } else value;
    return json(alloc, .{ .op = "literal", .type = "numeric", .sql_type = "numeric", .value = wire });
}

pub fn lower(alloc: std.mem.Allocator, schema: Json, expression: *const ast.Scalar, expected: ?ast.ColumnType) !Json {
    return (try lowerTyped(alloc, schema, expression, expected)).expression;
}

const Lowered = struct { expression: Json, type: ast.ColumnType, element_type: ?@import("array_value.zig").ElementType };

pub fn lowerTyped(alloc: std.mem.Allocator, schema: Json, expression: *const ast.Scalar, expected: ?ast.ColumnType) !Lowered {
    return lowerTypedPolicy(alloc, schema, expression, expected, null, .{});
}

pub fn lowerIndexKey(alloc: std.mem.Allocator, schema: Json, expression: *const ast.Scalar, limits: @import("immutable_plan.zig").Limits) !Lowered {
    return lowerTypedPolicy(alloc, schema, expression, null, .index_key, limits);
}

fn lowerTypedPolicy(alloc: std.mem.Allocator, schema: Json, expression: *const ast.Scalar, expected: ?ast.ColumnType, immutable: ?ExpressionContext, limits: @import("immutable_plan.zig").Limits) !Lowered {
    const properties = try @import("schema_columns.zig").properties(schema);
    var columns = std.ArrayList(scalar.Column).empty;
    for (properties.object.keys(), properties.object.values()) |name, property| {
        const column = try @import("schema_columns.zig").column(name, property);
        try columns.append(alloc, .{ .name = name, .type = column.type, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier });
    }
    return lowerColumnsPolicy(alloc, columns.items, expression, expected, immutable, limits);
}

pub fn lowerColumns(alloc: std.mem.Allocator, columns: []const scalar.Column, expression: *const ast.Scalar, expected: ?ast.ColumnType) !Lowered {
    return lowerColumnsPolicy(alloc, columns, expression, expected, null, .{});
}

fn lowerColumnsPolicy(alloc: std.mem.Allocator, columns: []const scalar.Column, expression: *const ast.Scalar, expected: ?ast.ColumnType, immutable: ?ExpressionContext, limits: @import("immutable_plan.zig").Limits) !Lowered {
    if (limits.checkpoint) |check| try check(limits.checkpoint_context);
    var program = try scalar.bindExpected(alloc, expression, columns, &.{}, expected, .{});
    defer program.deinit();
    if (program.parameter_types.len != 0) return error.InvalidSqlParameters;
    return lowerProgramPolicy(alloc, &program, columns, immutable, limits);
}

/// Lower a bound SQL subtree into the stored-index key language.
/// Preserve its implicit casts and builtin domains; do not rebind
/// source SQL or evaluate unrelated predicate operands. The caller supplies
/// the same ordinal/type layout, with canonical column names if aliases were
/// bound. The borrowed program and its preparation caches are never mutated.
pub fn lowerIndexInstruction(alloc: std.mem.Allocator, program: *const scalar.Program, root: u32, columns: []const scalar.Column, limits: @import("immutable_plan.zig").Limits) !Lowered {
    if (root >= program.instructions.len) return error.InvalidSqlProgram;
    var selected = program.*;
    selected.root = root;
    // Bound instructions are topological. No dependency can occur after the
    // selected root; unrelated predicate suffixes must not consume key budget.
    selected.instructions = program.instructions[0 .. @as(usize, root) + 1];
    selected.output_type = program.instructions[root].type;
    return lowerProgramPolicy(alloc, &selected, columns, .index_key, limits);
}

/// Prepare each needed bound subtree once, then compare the fixed-width
/// identity against every candidate. No source SQL or declaration is rebound.
/// Canonical column authority belongs to the caller's already-bound scan.
pub fn indexInstructionIdentity(alloc: std.mem.Allocator, program: *const scalar.Program, root: u32, columns: []const scalar.Column, table: @import("../storage/schema.zig").TableSchema, limits: @import("immutable_plan.zig").Limits) !@import("catalog.zig").IndexDefinition.KeyIdentity {
    const lowered = try lowerIndexInstruction(alloc, program, root, columns, limits);
    const key: @import("catalog.zig").IndexDefinition.Key = .{
        .column = "",
        .expression = lowered.expression,
        .result_type = switch (lowered.type) {
            .string, .uuid => .string,
            .integer => .integer,
            .number => if (lowered.element_type == .numeric) .numeric else .number,
            .array => .sql_array,
            .boolean => .boolean,
            .datetime => .datetime,
            .json => .json,
        },
        .collation = null,
        .direction = .asc,
        .nulls = .default,
    };
    const result = try key.bindIdentity(alloc, table);
    if (limits.checkpoint) |check| try check(limits.checkpoint_context);
    return result;
}

fn defaultArgument(plan: *const @import("immutable_plan.zig"), root: u32) !scalar.Datum {
    var index = root;
    var remaining = plan.nodes.len;
    while (remaining != 0) : (remaining -= 1) {
        if (index >= plan.nodes.len) return error.InvalidSqlProgram;
        if (plan.constants[index]) |value| return value;
        index = plan.nodes[index].alias orelse return error.UnsupportedSqlShape;
    }
    return error.InvalidSqlProgram;
}

fn lowerProgramPolicy(alloc: std.mem.Allocator, program: *const scalar.Program, columns: []const scalar.Column, immutable: ?ExpressionContext, limits: @import("immutable_plan.zig").Limits) !Lowered {
    var plan: ?@import("immutable_plan.zig") = if (immutable != null) try .initWithLimits(alloc, program, limits) else null;
    defer if (plan) |*value| value.deinit();
    if (plan) |*value| if (immutable != .column_default and value.nodes[program.root].volatility != .immutable) return switch (immutable.?) {
        .generation => error.SqlMutableGenerationExpression,
        .column_default => unreachable,
        .index_key => error.SqlMutableIndexExpression,
        .index_predicate => error.SqlMutableIndexPredicate,
    };
    const values = try alloc.alloc(Json, program.instructions.len);
    for (program.instructions, values, 0..) |original, *out, instruction_index| {
        if (limits.checkpoint) |check| try check(limits.checkpoint_context);
        var instruction = original;
        if (plan) |*value| {
            const node = value.nodes[instruction_index];
            if (!node.reachable) continue;
            if (original.type.temporal_kind == .interval and immutable == .column_default) {
                if (instruction_index == program.root) return error.UnsupportedSqlShape;
                _ = try defaultArgument(value, @intCast(instruction_index));
                out.* = .null;
                continue;
            }
            if (node.alias) |target| {
                out.* = try promoteNumeric(alloc, values[target], program.instructions[target].type, instruction.type);
                continue;
            }
            if (value.constants[instruction_index]) |known| {
                out.* = try inputLiteral(alloc, instruction.type, known);
                continue;
            }
            instruction.operation = node.operation;
        }
        // Unknown NULL acquires its precise domain from its consumer below.
        const kind = instruction.type.kind orelse if (nullLiteral(instruction)) .string else return error.SqlTypeMismatch;
        // Explicit builtin identities survive the physical result domain.
        // Unsupported array construction/casts remain guarded below rather
        // than disabling column comparisons, branches and null predicates.
        if (kind == .number and instruction.type.element_type == null) switch (instruction.operation) {
            .binary => |part| switch (part.op) {
                .add, .subtract, .multiply, .divide => return error.UnsupportedSqlShape,
                else => {},
            },
            .unary => |part| if (part.op == .negative) return error.UnsupportedSqlShape,
            else => {},
        };
        out.* = switch (instruction.operation) {
            .literal => |literal| if (kind == .array)
                try json(alloc, .{ .op = "literal", .type = "sql_array", .sql_type = @tagName(instruction.type.element_type orelse return error.UnsupportedSqlShape), .value = literal })
            else if (instruction.type.element_type == .numeric)
                try numericLiteral(alloc, literal)
            else if ((kind == .integer or kind == .number) and instruction.type.element_type != null)
                try json(alloc, .{ .op = "literal", .type = nativeType(kind, instruction.type.element_type), .sql_type = @tagName(instruction.type.element_type.?), .value = literal })
            else
                try json(alloc, .{ .op = "literal", .type = nativeType(kind, instruction.type.element_type), .value = literal }),
            .column => |ordinal| blk: {
                if (ordinal >= columns.len or instruction.type.kind != columns[ordinal].type) return error.InvalidSqlProgram;
                if (columns[ordinal].element_type) |identity| if (instruction.type.element_type != identity) return error.InvalidSqlProgram;
                break :blk try promoteNumeric(alloc, try json(alloc, .{ .op = "column", .column = columns[ordinal].name }), .{ .kind = columns[ordinal].type, .element_type = columns[ordinal].element_type }, instruction.type);
            },
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
                        if (part.op == .modulo and kind != .integer and instruction.type.element_type != .numeric) return error.UnsupportedSqlShape;
                        // The query VM promotes operands at execution time. A
                        // durable program must record that promotion explicitly.
                        const indexes = [_]usize{ part.left, part.right };
                        const operands = [_]*Json{ &left, &right };
                        for (indexes, operands) |index, operand| {
                            operand.* = try promoteNumeric(alloc, operand.*, program.instructions[index].type, instruction.type);
                        }
                    },
                    .eq, .neq, .lt, .lte, .gt, .gte, .is_distinct, .is_not_distinct => {
                        var lhs = program.instructions[part.left].type;
                        var rhs = program.instructions[part.right].type;
                        if (nullLiteral(program.instructions[part.left])) lhs = rhs;
                        if (nullLiteral(program.instructions[part.right])) rhs = lhs;
                        if (lhs.kind == .array and rhs.kind == .array) {
                            if (lhs.element_type != rhs.element_type) return error.UnsupportedSqlShape;
                            left = try promoteNumeric(alloc, left, program.instructions[part.left].type, lhs);
                            right = try promoteNumeric(alloc, right, program.instructions[part.right].type, lhs);
                        }
                        if ((lhs.kind == .integer or lhs.kind == .number) and
                            (rhs.kind == .integer or rhs.kind == .number))
                        {
                            const casts = @import("builtin_cast.zig");
                            var identity = try casts.commonNumeric(lhs.element_type orelse if (lhs.kind == .integer) .int64 else .float64, rhs.element_type orelse if (rhs.kind == .integer) .int64 else .float64);
                            // Mixed NUMERIC/real comparison operators select
                            // float8; conditional common types still use real.
                            if (identity == .float32 and lhs.element_type != rhs.element_type) identity = .float64;
                            const common: scalar.Type = .{ .kind = if (casts.integral(identity)) .integer else .number, .element_type = identity };
                            // Same physical integer/float domains already
                            // compare at full precision. Redundant casts also
                            // obscure simple partial-index column/literal keys.
                            const same_domain = (lhs.kind == .integer and rhs.kind == .integer) or
                                (lhs.kind == .number and rhs.kind == .number and lhs.element_type != .numeric and rhs.element_type != .numeric);
                            if (!same_domain) {
                                left = try promoteNumeric(alloc, left, program.instructions[part.left].type, common);
                                right = try promoteNumeric(alloc, right, program.instructions[part.right].type, common);
                            }
                        }
                    },
                    else => {},
                }
                break :blk try json(alloc, .{ .op = op, .args = &[_]Json{ left, right } });
            },
            .call => |part| blk: {
                if (part.function == .gen_random_uuid or part.function == .uuidv7) {
                    if (immutable != .column_default) return error.UnsupportedSqlShape;
                    if (part.function == .gen_random_uuid) break :blk try json(alloc, .{ .op = "uuidv4" });
                    if (part.args.len == 0) break :blk try json(alloc, .{ .op = "uuidv7" });
                    const argument = try defaultArgument(&plan.?, part.args[0]);
                    if (argument.sql_null) break :blk try inputLiteral(alloc, instruction.type, .{});
                    const interval = argument.temporal() orelse return error.SqlTypeMismatch;
                    if (interval.kind() != .interval) return error.SqlTypeMismatch;
                    var text: [@import("temporal_text.zig").output_capacity]u8 = undefined;
                    break :blk try json(alloc, .{ .op = "uuidv7", .shift = try @import("temporal_text.zig").format(interval.*, &text) });
                }
                if (part.function == .@"$array") {
                    const identity = instruction.type.element_type orelse return error.SqlTypeMismatch;
                    const args = try alloc.alloc(Json, part.args.len);
                    for (args, part.args) |*arg, index| {
                        const source = program.instructions[index].type;
                        const target: scalar.Type = .{ .kind = if (source.kind == .array) .array else switch (identity) {
                            .int16, .int32, .int64 => .integer,
                            .float32, .float64, .numeric => .number,
                            .text => .string,
                            .uuid => .uuid,
                            .boolean => .boolean,
                            .jsonb => .json,
                        }, .element_type = identity };
                        arg.* = try promoteNumeric(alloc, values[index], source, target);
                    }
                    break :blk try json(alloc, .{ .op = "array", .sql_type = @tagName(identity), .args = args });
                }
                const op: []const u8 = switch (part.function) {
                    .lower => "lower_ascii",
                    .upper => "upper_ascii",
                    .coalesce => "coalesce",
                    .mod => "modulo",
                    .replace => "replace",
                    .md5 => "md5",
                    inline .jsonb_typeof, .jsonb_array_length, .jsonb_extract_path, .jsonb_extract_path_text => |tag| @tagName(tag),
                    inline .abs, .ceil, .floor, .round, .trunc, .sign, .sqrt => |tag| @tagName(tag),
                    else => return error.UnsupportedSqlShape,
                };
                const args = try alloc.alloc(Json, part.args.len);
                if (part.function == .mod and kind != .integer and instruction.type.element_type != .numeric) return error.UnsupportedSqlShape;
                for (args, part.args, 0..) |*arg, index, position| arg.* = if ((part.function == .round or part.function == .trunc) and position == 1)
                    try promoteNumeric(alloc, values[index], program.instructions[index].type, .{ .kind = .integer, .element_type = .int32 })
                else if (part.function == .coalesce or part.function == .mod or part.function == .abs or part.function == .ceil or part.function == .floor or part.function == .round or part.function == .trunc or part.function == .sign or part.function == .sqrt)
                    try promoteNumeric(alloc, values[index], program.instructions[index].type, instruction.type)
                else
                    values[index];
                if (part.function == .coalesce and args.len == 1) break :blk args[0];
                var result = try json(alloc, .{ .op = op, .args = args });
                if (part.function == .mod or part.function == .abs or part.function == .ceil or part.function == .floor or part.function == .round or part.function == .trunc or part.function == .sign or part.function == .sqrt) if (instruction.type.element_type) |identity| try result.object.put(alloc, "sql_type", .{ .string = @tagName(identity) });
                break :blk result;
            },
            .cast => |part| blk: {
                const source = program.instructions[part.operand].type;
                var lowered: Json = converted: {
                    if (instruction.input_function) break :converted try inputLiteral(alloc, instruction.type, program.constant_inputs.get(@intCast(instruction_index)) orelse return error.InvalidSqlProgram);
                    // NULL retains its explicit target domain without invoking an
                    // input function or borrowing the unknown literal's identity.
                    if (nullLiteral(program.instructions[part.operand])) {
                        var result = try json(alloc, .{ .op = "literal", .type = nativeType(part.type, part.element_type), .value = @as(?u8, null) });
                        if (part.type == .integer or part.type == .number or part.type == .array) if (part.element_type) |identity| {
                            try result.object.put(alloc, "sql_type", .{ .string = @tagName(identity) });
                        };
                        break :converted result;
                    }
                    if ((source.kind == .integer or source.kind == .number) and (part.type == .integer or part.type == .number)) {
                        const target_type: @import("array_value.zig").ElementType = part.element_type orelse if (part.type == .integer) .int64 else .float64;
                        break :converted try json(alloc, .{ .op = "cast", .type = nativeType(part.type, target_type), .sql_type = @tagName(target_type), .args = &[_]Json{values[part.operand]} });
                    }
                    // Validate unknown input with the exact NUMERIC input function;
                    // the durable plan owns its compiled coefficient, never f64.
                    if (part.type == .number and part.element_type == .numeric and source.kind == .string and
                        program.instructions[part.operand].operation == .literal)
                    {
                        break :converted try numericLiteral(alloc, program.instructions[part.operand].operation.literal);
                    }
                    if (source.kind != part.type) return error.UnsupportedSqlShape;
                    if (source.element_type != part.element_type) return error.UnsupportedSqlShape;
                    if (part.type == .array) break :converted try json(alloc, .{ .op = "cast", .type = "sql_array", .sql_type = @tagName(part.element_type.?), .args = &[_]Json{values[part.operand]} });
                    break :converted values[part.operand];
                };
                if (part.numeric_modifier) |modifier| {
                    if (part.element_type != .numeric or (part.type != .number and part.type != .array)) return error.UnsupportedSqlShape;
                    // This branch's numeric cast is newly allocated. Attach
                    // its modifier, never overwrite an operand's inner cast.
                    if (std.mem.eql(u8, lowered.object.get("op").?.string, "cast")) {
                        try lowered.object.put(alloc, "numeric_modifier", try json(alloc, modifier));
                    } else {
                        lowered = try json(alloc, .{ .op = "cast", .type = nativeType(part.type, part.element_type), .sql_type = "numeric", .numeric_modifier = modifier, .args = &[_]Json{lowered} });
                    }
                }
                break :blk lowered;
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
                else if (kind == .array)
                    try json(alloc, .{ .op = "literal", .type = "sql_array", .sql_type = @tagName(instruction.type.element_type orelse return error.UnsupportedSqlShape), .value = @as(?u8, null) })
                else if (kind == .integer or kind == .number)
                    try json(alloc, .{ .op = "literal", .type = nativeType(kind, instruction.type.element_type), .sql_type = @tagName(instruction.type.element_type orelse if (kind == .integer) @as(@import("array_value.zig").ElementType, .int64) else .float64), .value = @as(?u8, null) })
                else
                    try json(alloc, .{ .op = "literal", .type = if (kind == .uuid) "string" else @tagName(kind), .value = @as(?u8, null) });
                break :blk try json(alloc, .{ .op = "case_when", .args = args });
            },
            .in_list => |part| blk: {
                if (part.values.len == 0 or part.values.len >= @import("../schema/relational_expression.zig").max_nodes) return error.SqlLimitExceeded;
                const indexes = try alloc.alloc(u32, part.values.len + 1);
                indexes[0] = part.operand;
                @memcpy(indexes[1..], part.values);
                var common_type = program.instructions[part.operand].type;
                for (indexes) |index| {
                    if (!nullLiteral(program.instructions[index])) {
                        common_type = program.instructions[index].type;
                        break;
                    }
                }
                if (common_type.kind == .integer or common_type.kind == .number) {
                    for (indexes) |index| {
                        if (nullLiteral(program.instructions[index])) continue;
                        const other = program.instructions[index].type;
                        var identity = try @import("builtin_cast.zig").commonNumeric(common_type.element_type orelse if (common_type.kind == .integer) .int64 else .float64, other.element_type orelse if (other.kind == .integer) .int64 else .float64);
                        if (identity == .float32 and common_type.element_type != other.element_type) identity = .float64;
                        common_type = .{ .kind = if (@import("builtin_cast.zig").integral(identity)) .integer else .number, .element_type = identity };
                    }
                }
                if (common_type.kind == null) common_type = .{ .kind = .string, .element_type = .text };
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

test "SQL index key identities share native domains without narrowing or collation guesses" {
    const catalog = @import("catalog.zig");
    const native = @import("../storage/schema.zig");
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const table: catalog.Table = .{ .id = 17, .physical_name = "items", .schema_version = 4, .columns = &.{
        .{ .name = "email", .path = "email", .type = .string, .element_type = .text },
        .{ .name = "amount", .path = "amount", .type = .number, .element_type = .numeric, .numeric_modifier = .{ .precision = 20, .scale = 4 } },
    } };
    const actual: native.TableSchema = .{ .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "email", .path = "email", .column_type = .string, .sql_element_type = .text },
        .{ .name = "amount", .path = "amount", .column_type = .numeric, .sql_element_type = .numeric, .numeric_modifier = .{ .precision = 20, .scale = 4 } },
    } };
    const view = try table.expressionSchemaLeaky(alloc);
    const canonical = [_]scalar.Column{
        .{ .name = "email", .type = .string, .element_type = .text },
        .{ .name = "amount", .type = .number, .element_type = .numeric, .numeric_modifier = .{ .precision = 20, .scale = 4 } },
    };
    var lower_key: catalog.IndexDefinition.Key = .{ .column = "", .expression = try std.json.parseFromSliceLeaky(Json, alloc,
        \\{"op":"lower_ascii","args":[{"op":"column","column":"email"}]}
    , .{}), .result_type = .string, .collation = "C", .direction = .desc, .nulls = .first };
    lower_key.identity = try lower_key.bindIdentity(alloc, actual);
    for ([_][]const u8{ "lower(q.email)=$1 AND 1/0=0", "upper(q.email)=$1" }, 0..) |sql, index| {
        var parsed = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer parsed.deinit();
        var columns = canonical;
        columns[0].name = "q\x00email";
        var program = try scalar.bindExpected(a, parsed.expression, &columns, &.{.string}, .boolean, .{});
        defer program.deinit();
        const comparison = if (index == 0) program.instructions[program.root].operation.binary.left else program.root;
        const root = program.instructions[comparison].operation.binary.left;
        const query = try indexInstructionIdentity(alloc, &program, root, &canonical, view, .{});
        try std.testing.expectEqual(index == 0, try lower_key.matchesEquality(query, "binary"));
        if (index == 0) {
            try std.testing.expect(!try lower_key.matchesEquality(query, "ci"));
            try std.testing.expectError(error.UnsupportedRelationalIndexCollation, lower_key.matchesEquality(query, "unknown"));
            var unbound = lower_key;
            unbound.identity = null;
            try std.testing.expect(!try unbound.matchesEquality(query, null));
        }
    }
    var numeric: catalog.IndexDefinition.Key = .{ .column = "amount", .expression = null, .result_type = null, .collation = null, .direction = .asc, .nulls = .default };
    numeric.identity = try numeric.bindIdentity(alloc, actual);
    for ([_][]const u8{ "amount=CAST(1 AS NUMERIC)", "amount=CAST(1 AS DOUBLE PRECISION)" }, 0..) |sql, index| {
        var parsed = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer parsed.deinit();
        var program = try scalar.bindExpected(a, parsed.expression, &canonical, &.{}, .boolean, .{});
        defer program.deinit();
        const root = program.instructions[program.root].operation.binary.left;
        const query = try indexInstructionIdentity(alloc, &program, root, &canonical, view, .{});
        try std.testing.expectEqual(index == 0, try numeric.matchesEquality(query, null));
    }
    var changed = table;
    const changed_columns = try alloc.dupe(catalog.Column, table.columns);
    changed_columns[1].numeric_modifier = .{ .precision = 19, .scale = 4 };
    changed.columns = changed_columns;
    const changed_identity = try numeric.bindIdentity(alloc, try changed.expressionSchemaLeaky(alloc));
    try std.testing.expect(!try numeric.matchesEquality(changed_identity, null));
    const Faults = struct {
        fn run(allocator: std.mem.Allocator, key: catalog.IndexDefinition.Key, source: native.TableSchema) !void {
            const result = try key.bindIdentity(allocator, source);
            try std.testing.expectEqualDeep(key.identity.?, result);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Faults.run, .{ numeric, actual });
}

test "SQL index key lowering reuses bound instructions without demanding unrelated predicates" {
    const compiler = @import("compiler.zig");
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const canonical = [_]scalar.Column{.{ .name = "email", .type = .string, .element_type = .text }};
    for ([_][]const u8{
        "lower(email)=$1 AND concat(email,$1)='x'",
        "lower(email)=$1 AND 1/0=0",
        "lower(q.email)=$1 AND 1/0=0",
    }, 0..) |statement, position| {
        var parsed = try compiler.compileScalar(a, statement, .{});
        defer parsed.deinit();
        var binding_columns = canonical;
        if (position == 2) binding_columns[0].name = "q\x00email";
        var program = try scalar.bindExpected(a, parsed.expression, &binding_columns, &.{.string}, .boolean, .{});
        defer program.deinit();
        const original_root = program.root;
        const original_type = program.output_type;
        const comparison = program.instructions[program.instructions[program.root].operation.binary.left].operation.binary;
        const key = try lowerIndexInstruction(arena.allocator(), &program, comparison.left, &canonical, .{});
        _ = try lowerIndexInstruction(arena.allocator(), &program, comparison.left, &canonical, .{ .nodes = @as(usize, comparison.left) + 1 });
        const encoded = try std.json.Stringify.valueAlloc(arena.allocator(), key.expression, .{});
        try std.testing.expectEqualStrings(
            \\{"op":"lower_ascii","args":[{"op":"column","column":"email"}]}
        , encoded);
        try std.testing.expectEqual(ast.ColumnType.string, key.type);
        try std.testing.expectEqual(@as(?@import("array_value.zig").ElementType, .text), key.element_type);
        try std.testing.expectEqual(original_root, program.root);
        try std.testing.expectEqualDeep(original_type, program.output_type);
        try std.testing.expectError(error.InvalidSqlProgram, lowerIndexInstruction(arena.allocator(), &program, std.math.maxInt(u32), &canonical, .{}));
        try std.testing.expectError(error.InvalidSqlProgram, lowerIndexInstruction(arena.allocator(), &program, comparison.left, &.{}, .{}));
        try std.testing.expectError(error.InvalidSqlProgram, lowerIndexInstruction(arena.allocator(), &program, comparison.left, &.{.{ .name = "email", .type = .integer, .element_type = .int64 }}, .{}));
        for (program.instructions, 0..) |instruction, index| if (instruction.operation == .parameter) {
            try std.testing.expectError(error.InvalidSqlParameters, lowerIndexInstruction(arena.allocator(), &program, @intCast(index), &canonical, .{}));
        };
        if (position == 0) {
            const stable = program.instructions[program.instructions[program.root].operation.binary.right].operation.binary.left;
            try std.testing.expectError(error.SqlMutableIndexExpression, lowerIndexInstruction(arena.allocator(), &program, stable, &canonical, .{}));
        }
    }
}

test "SQL index key lowering preserves comparison coercions instead of matching a narrower domain" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const columns = [_]scalar.Column{.{ .name = "amount", .type = .number, .element_type = .numeric }};
    var parsed = try @import("compiler.zig").compileScalar(a, "amount=CAST(1 AS DOUBLE PRECISION)", .{});
    defer parsed.deinit();
    var program = try scalar.bindExpected(a, parsed.expression, &columns, &.{}, .boolean, .{});
    defer program.deinit();
    const root = program.instructions[program.root].operation.binary.left;
    const key = try lowerIndexInstruction(arena.allocator(), &program, root, &columns, .{});
    try std.testing.expectEqual(@as(?@import("array_value.zig").ElementType, .float64), key.element_type);
    try std.testing.expectEqualStrings("cast", key.expression.object.get("op").?.string);
    try std.testing.expectEqualStrings("float64", key.expression.object.get("sql_type").?.string);
    try std.testing.expectEqualStrings("amount", key.expression.object.get("args").?.array.items[0].object.get("column").?.string);
}

test "SQL index key lowering unwinds allocation failure without owning the borrowed program" {
    const a = std.testing.allocator;
    var parsed = try @import("compiler.zig").compileScalar(a, "replace(lower(email),'x','y')=$1", .{});
    defer parsed.deinit();
    const columns = [_]scalar.Column{.{ .name = "email", .type = .string, .element_type = .text }};
    var program = try scalar.bindExpected(a, parsed.expression, &columns, &.{.string}, .boolean, .{});
    defer program.deinit();
    const original_root = program.root;
    const original_type = program.output_type;
    const T = struct {
        fn run(alloc: std.mem.Allocator, borrowed: *const scalar.Program, layout: []const scalar.Column) !void {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const root = borrowed.instructions[borrowed.root].operation.binary.left;
            const key = try lowerIndexInstruction(arena.allocator(), borrowed, root, layout, .{});
            try std.testing.expectEqualStrings("replace", key.expression.object.get("op").?.string);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, T.run, .{ &program, &columns });
    try std.testing.expectEqual(original_root, program.root);
    try std.testing.expectEqualDeep(original_type, program.output_type);
    try T.run(a, &program, &columns);
}

test "SQL index key lowering owns literal packets after the borrowed program closes" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const result = blk: {
        var parsed = try @import("compiler.zig").compileScalar(a,
            \\jsonb_extract_path_text(coalesce(metadata,'{"source":1.2300e2}'::jsonb),'source')=$1
        , .{});
        defer parsed.deinit();
        const columns = [_]scalar.Column{.{ .name = "metadata", .type = .json, .element_type = .jsonb }};
        var program = try scalar.bindExpected(a, parsed.expression, &columns, &.{.string}, .boolean, .{});
        defer program.deinit();
        break :blk try lowerIndexInstruction(arena.allocator(), &program, program.instructions[program.root].operation.binary.left, &columns, .{});
    };
    try std.testing.expectEqualStrings("jsonb_extract_path_text", result.expression.object.get("op").?.string);
    const fallback = result.expression.object.get("args").?.array.items[0].object.get("args").?.array.items[1];
    try std.testing.expectEqualStrings("literal", fallback.object.get("op").?.string);
    // Preserve the owned logical token. Physical canonicalization and JSONB
    // display formatting are separate boundaries, not an ownership repair.
    try std.testing.expectEqualStrings("1.2300e2", fallback.object.get("value").?.object.get("source").?.number_string);
    const encoded = try std.json.Stringify.valueAlloc(arena.allocator(), result.expression, .{});
    try std.testing.expect(std.mem.indexOf(u8, encoded, "1.2300e2") != null);
}

test "SQL index key lowering benchmark bounds candidate rebinding overhead" {
    const a = std.testing.allocator;
    const columns = [_]scalar.Column{.{ .name = "email", .type = .string, .element_type = .text }};
    var key_ast = try @import("compiler.zig").compileScalar(a, "replace(lower(email),'old','new')", .{});
    defer key_ast.deinit();
    var predicate = try @import("compiler.zig").compileScalar(a, "replace(lower(email),'old','new')=$1", .{});
    defer predicate.deinit();
    var program = try scalar.bindExpected(a, predicate.expression, &columns, &.{.string}, .boolean, .{});
    defer program.deinit();
    const root = program.instructions[program.root].operation.binary.left;
    const Stats = struct { allocations: usize, bytes: usize, elapsed_ns: i96 };
    var stats: [2]Stats = undefined;
    for (&stats, 0..) |*result, mode| {
        var tracked = std.testing.FailingAllocator.init(a, .{});
        const started = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
        for (0..16) |_| {
            var arena = std.heap.ArenaAllocator.init(tracked.allocator());
            defer arena.deinit();
            const key = if (mode == 0)
                try lowerColumnsPolicy(arena.allocator(), &columns, key_ast.expression, null, .index_key, .{})
            else
                try lowerIndexInstruction(arena.allocator(), &program, root, &columns, .{});
            try std.testing.expectEqualStrings("replace", key.expression.object.get("op").?.string);
        }
        result.* = .{ .allocations = tracked.allocations, .bytes = tracked.allocated_bytes, .elapsed_ns = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - started };
        try std.testing.expectEqual(tracked.allocated_bytes, tracked.freed_bytes);
        std.debug.print("SQL index key preparation: reuse_bound={} keys=16 allocations={} allocated_bytes={} elapsed_ns={}\n", .{ mode != 0, result.allocations, result.bytes, result.elapsed_ns });
    }
    try std.testing.expect(stats[1].allocations <= stats[0].allocations);
    try std.testing.expect(stats[1].bytes < stats[0].bytes);
}

test "SQL index key lowering benchmark shares immutable operator children without copying trees" {
    const a = std.testing.allocator;
    var expected: ?[]u8 = null;
    defer if (expected) |bytes| a.free(bytes);
    var allocated: [2]usize = undefined;
    for (&allocated, 0..) |*bytes, mode| {
        var tracked = std.testing.FailingAllocator.init(a, .{});
        const started = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
        for (0..16) |_| {
            var arena = std.heap.ArenaAllocator.init(tracked.allocator());
            defer arena.deinit();
            const owned = arena.allocator();
            var child = try json(owned, .{ .op = "column", .column = "email" });
            for (0..6) |_| child = try json(owned, .{ .op = "lower_ascii", .args = &[_]Json{child} });
            const input = .{ .op = "replace", .args = &[_]Json{
                child,
                try json(owned, .{ .op = "literal", .type = "string", .value = "old" }),
                try json(owned, .{ .op = "literal", .type = "string", .value = "new" }),
            } };
            const parent = if (mode == 0)
                try std.json.parseFromSliceLeaky(Json, owned, try std.json.Stringify.valueAlloc(owned, input, .{}), .{ .parse_numbers = false })
            else
                try json(owned, input);
            const encoded = try std.json.Stringify.valueAlloc(owned, parent, .{});
            if (expected) |value| try std.testing.expectEqualStrings(value, encoded) else expected = try a.dupe(u8, encoded);
        }
        bytes.* = tracked.allocated_bytes;
        try std.testing.expectEqual(tracked.allocated_bytes, tracked.freed_bytes);
        std.debug.print("SQL index operator construction: share_children={} operators=16 allocations={} allocated_bytes={} elapsed_ns={}\n", .{ mode != 0, tracked.allocations, bytes.*, std.Io.Clock.now(.awake, std.testing.io).nanoseconds - started });
    }
    try std.testing.expect(allocated[1] < allocated[0]);
}

/// DEFAULT retains arithmetic failures until write time. Stored generation
/// first simplifies demanded immutable constants, matching PostgreSQL DDL
/// failure timing, then persists only the live assignment program. Callers
/// provide the complete candidate schema so forward base references bind.
pub fn lowerAssignment(alloc: std.mem.Allocator, schema: Json, expression: *const ast.Scalar, column: ast.Column, generated: bool) !Json {
    return lowerAssignmentWithLimits(alloc, schema, expression, column, generated, .{});
}

pub fn lowerAssignmentWithLimits(alloc: std.mem.Allocator, schema: Json, expression: *const ast.Scalar, column: ast.Column, generated: bool, limits: @import("immutable_plan.zig").Limits) !Json {
    if (limits.checkpoint) |check| try check(limits.checkpoint_context);
    if (generated) try rejectGeneratedReferences(schema, column.name, expression);
    if (expression.* == .literal) return @import("ddl_runtime.zig").defaultExpression(alloc, expression.literal, column.type, column.element_type);
    const lowered = (if (generated)
        lowerTypedPolicy(alloc, schema, expression, column.type, .generation, limits)
    else
        lowerColumnsPolicy(alloc, &.{}, expression, column.type, .column_default, limits)) catch |err| switch (err) {
        error.UnknownColumn => return error.UndefinedColumn,
        else => return err,
    };
    return finishAssignment(alloc, lowered, column);
}

/// USING reads the old value of its own column. Unlike generated columns it
/// has no self-reference cycle, but must still be immutable for tail replay.
pub fn lowerRewriteAssignment(alloc: std.mem.Allocator, schema: Json, expression: *const ast.Scalar, column: ast.Column, limits: @import("immutable_plan.zig").Limits) !Json {
    const lowered = try lowerTypedPolicy(alloc, schema, expression, column.type, .generation, limits);
    var result = try finishAssignment(alloc, lowered, column);
    if (column.numeric_modifier) |modifier| {
        if (column.type != .number or column.element_type != .numeric) return error.UnsupportedSqlShape;
        try result.object.put(alloc, "numeric_modifier", try json(alloc, modifier));
    }
    return result;
}

fn finishAssignment(alloc: std.mem.Allocator, lowered: Lowered, column: ast.Column) !Json {
    if (column.type == .integer or column.type == .number) {
        if (lowered.type != .integer and lowered.type != .number) return error.SqlAssignmentTypeMismatch;
        // Untyped SQL decimal arithmetic requires an exact decimal value
        // domain; never persist binary-float evaluation under that contract.
        if (lowered.type == .number and lowered.element_type == null) return error.UnsupportedSqlShape;
        const target: @import("array_value.zig").ElementType = column.element_type orelse if (column.type == .integer) .int64 else .float64;
        return json(alloc, .{ .op = "cast", .type = nativeType(column.type, target), .sql_type = @tagName(target), .args = &[_]Json{lowered.expression} });
    }
    if (column.type == .string and lowered.type == .uuid) return lowered.expression;
    if (lowered.type != column.type) return error.SqlAssignmentTypeMismatch;
    if (column.type == .array and lowered.element_type != column.element_type) return error.UnsupportedSqlShape;
    return lowered.expression;
}

test "SQL generated immutable planning prunes dead stable calls and preserves PostgreSQL admission" {
    const compiler = @import("compiler.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const schema = try json(a, .{ .default_type = "row", .document_schemas = .{ .row = .{ .schema = .{ .properties = .{ .s = .{ .type = "keyword" }, .t = .{ .type = "datetime" } } } } } });
    for ([_]struct { sql: []const u8, kind: ast.ColumnType }{
        .{ .sql = "concat(s, 'x')", .kind = .string },
        .{ .sql = "concat_ws('-', s, 'x')", .kind = .string },
        .{ .sql = "now()", .kind = .datetime },
        .{ .sql = "CURRENT_TIMESTAMP", .kind = .datetime },
        .{ .sql = "s::timestamptz", .kind = .datetime },
        .{ .sql = "t::text", .kind = .string },
    }) |case| {
        var parsed = try compiler.compileScalar(std.testing.allocator, case.sql, .{});
        defer parsed.deinit();
        try std.testing.expectError(error.SqlMutableGenerationExpression, lowerAssignment(a, schema, parsed.expression, .{ .name = "g", .type = case.kind }, true));
    }
    for ([_][]const u8{
        "CASE WHEN false THEN concat(s, 'x') ELSE 'ok' END",
        "coalesce('ok', concat(s, 'x'))",
        "coalesce(NULL::text, 'ok', concat(s, 'x'))",
    }) |sql| {
        var parsed = try compiler.compileScalar(std.testing.allocator, sql, .{});
        defer parsed.deinit();
        const lowered = try lowerAssignment(a, schema, parsed.expression, .{ .name = "g", .type = .string }, true);
        try std.testing.expectEqualStrings("literal", lowered.object.get("op").?.string);
        try std.testing.expectEqualStrings("ok", lowered.object.get("value").?.string);
    }
    for ([_][]const u8{ "coalesce(s, 'ok', concat(s, 'x'))", "CASE WHEN s IS NULL THEN 'ok' WHEN true THEN 'yes' ELSE concat(s, 'x') END" }) |sql| {
        var parsed = try compiler.compileScalar(std.testing.allocator, sql, .{});
        defer parsed.deinit();
        const lowered = try lowerAssignment(a, schema, parsed.expression, .{ .name = "g", .type = .string }, true);
        const encoded = try std.json.Stringify.valueAlloc(a, lowered, .{});
        try std.testing.expect(std.mem.indexOf(u8, encoded, "concat") == null);
        try std.testing.expect(std.mem.indexOf(u8, encoded, "column") != null);
    }
    var absorbed = try compiler.compileScalar(std.testing.allocator, "s IS NULL OR concat(s, 'x') IS NULL OR true", .{});
    defer absorbed.deinit();
    const folded = try lowerAssignment(a, schema, absorbed.expression, .{ .name = "g", .type = .boolean }, true);
    try std.testing.expectEqual(true, folded.object.get("value").?.bool);
}

test "SQL generated immutable planning matches independent PostgreSQL DDL contracts" {
    const fixture = try std.json.parseFromSlice(struct {
        entries: []const struct { sql: []const u8, sqlstate: ?[]const u8 },
    }, std.testing.allocator, @embedFile("fixtures/sql_generated_expression_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    try std.testing.expectEqual(@as(usize, 42), fixture.value.entries.len);
    for (fixture.value.entries) |entry| {
        var parsed = try @import("compiler.zig").compile(std.testing.allocator, entry.sql, .{});
        defer parsed.deinit();
        const schema = @import("ddl_runtime.zig").createSchemaAlloc(std.testing.allocator, parsed.statement.create_table) catch |err| {
            if (entry.sqlstate) |state| {
                try std.testing.expectEqualStrings(state, @import("errors.zig").describe(err).code);
                continue;
            }
            std.debug.print("generated expression rejected: {s}: {t}\n", .{ entry.sql, err });
            return err;
        };
        defer std.testing.allocator.free(schema);
        try std.testing.expect(entry.sqlstate == null);
    }
}

test "SQL immutable index planning matches independent PostgreSQL admission contracts" {
    const a = std.testing.allocator;
    const fixture = try std.json.parseFromSlice(struct {
        base_sql: []const u8,
        entries: []const struct { sql: []const u8, sqlstate: ?[]const u8 },
    }, a, @embedFile("fixtures/sql_index_planning_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    try std.testing.expectEqual(@as(usize, 24), fixture.value.entries.len);
    var base = try @import("compiler.zig").compile(a, fixture.value.base_sql, .{});
    defer base.deinit();
    const bytes = try @import("ddl_runtime.zig").createSchemaAlloc(a, base.statement.create_table);
    defer a.free(bytes);
    for (fixture.value.entries) |entry| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const owned = arena.allocator();
        var schema = try std.json.parseFromSliceLeaky(Json, owned, bytes, .{ .parse_numbers = false });
        var compiled = try @import("compiler.zig").compile(a, entry.sql, .{});
        defer compiled.deinit();
        _ = @import("schema_ddl.zig").apply(owned, &schema, compiled.statement.catalog_ddl) catch |err| {
            if (entry.sqlstate) |state| {
                try std.testing.expectEqualStrings(state, @import("errors.zig").describe(err).code);
                continue;
            }
            std.debug.print("index planning unexpectedly rejected {s}: {s}\n", .{ entry.sql, @errorName(err) });
            return err;
        };
        try std.testing.expect(entry.sqlstate == null);
        // The generated public wire contract must accept persisted variants.
        const encoded = try std.json.Stringify.valueAlloc(owned, schema, .{});
        var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(a, encoded);
        defer parsed.deinit(a);
        const definitions = (try parsed.relationalIndexDefinitions(owned)).?;
        try std.testing.expectEqual(@as(usize, 1), definitions.len);
    }
}

test "SQL immutable index public declarations reject ambiguous and incomplete membership" {
    const a = std.testing.allocator;
    const base = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"n\":{\"type\":\"integer\"}}}}},\"relational_indexes\":[{\"name\":\"probe_idx\",\"keys\":[{\"column\":\"n\"}],\"where\":[{s}]}]}";
    for ([_][]const u8{
        "{}",
        "{\"column\":\"n\"}",
        "{\"op\":\"eq\"}",
        "{\"column\":\"\",\"op\":\"eq\"}",
        "{\"column\":\"n\",\"op\":\"eq\",\"collation\":\"\"}",
        "{\"constant\":false,\"column\":\"n\"}",
        "{\"constant\":false,\"op\":\"eq\"}",
        "{\"constant\":false,\"value\":0}",
        "{\"constant\":false,\"collation\":\"C\"}",
    }) |condition| {
        const encoded = try std.mem.replaceOwned(u8, a, base, "{s}", condition);
        defer a.free(encoded);
        try std.testing.expectError(error.InvalidSchemaUpdateRequest, @import("../schema/mod.zig").parseValidatedTableSchema(a, encoded));
    }
}

test "SQL generated immutable planning retains binding errors and PostgreSQL constant failure timing" {
    const compiler = @import("compiler.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const schema = try json(a, .{ .default_type = "row", .document_schemas = .{ .row = .{ .schema = .{ .properties = .{ .s = .{ .type = "keyword" } } } } } });
    for ([_][]const u8{ "1 / 0", "CASE WHEN s IS NULL THEN 1 ELSE 1 / 0 END" }) |sql| {
        var parsed = try compiler.compileScalar(std.testing.allocator, sql, .{});
        defer parsed.deinit();
        try std.testing.expectError(error.SqlDivisionByZero, lowerAssignment(a, schema, parsed.expression, .{ .name = "g", .type = .integer }, true));
    }
    for ([_][]const u8{ "CASE WHEN false THEN 1 / 0 ELSE 1 END", "coalesce(1, 1 / 0)" }) |sql| {
        var parsed = try compiler.compileScalar(std.testing.allocator, sql, .{});
        defer parsed.deinit();
        const lowered = try lowerAssignment(a, schema, parsed.expression, .{ .name = "g", .type = .integer }, true);
        try std.testing.expectEqualStrings("cast", lowered.object.get("op").?.string);
        try std.testing.expectEqualStrings("int64", lowered.object.get("sql_type").?.string);
        try std.testing.expectEqualStrings("1", lowered.object.get("args").?.array.items[0].object.get("value").?.number_string);
    }
    var unknown = try compiler.compileScalar(std.testing.allocator, "CASE WHEN false THEN missing ELSE 'ok' END", .{});
    defer unknown.deinit();
    try std.testing.expectError(error.UndefinedColumn, lowerAssignment(a, schema, unknown.expression, .{ .name = "g", .type = .string }, true));
    var self_reference = try compiler.compileScalar(std.testing.allocator, "CASE WHEN false THEN g ELSE 'ok' END", .{});
    defer self_reference.deinit();
    try std.testing.expectError(error.SqlInvalidGenerationExpression, lowerAssignment(a, schema, self_reference.expression, .{ .name = "g", .type = .string }, true));
    var ordinary_default = try compiler.compileScalar(std.testing.allocator, "1 / 0", .{});
    defer ordinary_default.deinit();
    _ = try lowerAssignment(a, schema, ordinary_default.expression, .{ .name = "g", .type = .integer }, false);
    try std.testing.expectEqual(scalar.Volatility.immutable, scalar.Function.to_timestamp.volatility());
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
    return lowerIndexPredicateWithLimits(alloc, schema, expression, .{});
}

pub fn lowerIndexPredicateWithLimits(alloc: std.mem.Allocator, schema: Json, expression: *const ast.Scalar, limits: @import("immutable_plan.zig").Limits) ![]const Json {
    const lowered = try lowerTypedPolicy(alloc, schema, expression, .boolean, .index_predicate, limits);
    if (lowered.type != .boolean) return error.SqlTypeMismatch;
    const native = lowered.expression;
    const expressions = @import("../schema/relational_expression.zig");
    var memory: @import("memory_budget.zig") = .{ .backing = alloc, .limit = expressions.max_allocated_bytes };
    var arena = std.heap.ArenaAllocator.init(memory.allocator());
    defer arena.deinit();
    var bytes: usize = expressions.max_allocated_bytes;
    var execution = expressions.Execution.init(arena.allocator(), &bytes);
    var predicates = std.ArrayList(Json).empty;
    collectPredicates(alloc, native, false, &predicates, &execution) catch |err| {
        if (err == error.OutOfMemory and memory.isExhausted()) return error.SqlProgramLimitExceeded;
        return err;
    };
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

fn collectPredicates(alloc: std.mem.Allocator, expression: Json, negated: bool, predicates: *std.ArrayList(Json), execution: *@import("../schema/relational_expression.zig").Execution) anyerror!void {
    const operation = expression.object.get("op").?.string;
    if (std.mem.eql(u8, operation, "literal")) {
        const value = expression.object.get("value") orelse Json.null;
        if (value != .null and value != .bool) return error.SqlTypeMismatch;
        // NOT UNKNOWN remains UNKNOWN; WHERE admits only TRUE.
        if (value == .null or value.bool == negated)
            try appendPredicate(alloc, predicates, try json(alloc, .{ .constant = false }));
        return;
    }
    if (std.mem.eql(u8, operation, "column")) {
        try appendPredicate(alloc, predicates, try json(alloc, .{ .column = expression.object.get("column").?.string, .op = "eq", .value = !negated }));
        return;
    }
    const args = (expression.object.get("args") orelse return error.UnsupportedSqlShape).array.items;
    if (std.mem.eql(u8, operation, "not")) {
        if (args.len != 1) return error.UnsupportedSqlShape;
        return collectPredicates(alloc, args[0], !negated, predicates, execution);
    }
    // De Morgan's law permits NOT (a OR b), whose native form is a
    // conjunction. Disjunctions still require a richer native index format.
    if ((!negated and std.mem.eql(u8, operation, "and")) or (negated and std.mem.eql(u8, operation, "or"))) {
        for (args) |arg| try collectPredicates(alloc, arg, negated, predicates, execution);
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
    var value = @import("../schema/relational_expression.zig").foldConstantJson(execution, args[1]) catch |err| {
        if (err == error.RelationalIndexColumnNotFound) return error.UnsupportedSqlShape;
        return err;
    };
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

test "SQL durable JSON lowering executes independent PostgreSQL path contracts" {
    const a = std.testing.allocator;
    const native = @import("../schema/relational_expression.zig");
    const Entry = struct { sql: []const u8, value: Json = .null, sql_null: ?bool = null, oid: ?u32 = null, @"error": ?[]const u8 = null };
    var fixture = try std.json.parseFromSlice(struct { entries: []const Entry }, a, @embedFile("fixtures/sql_json_path_reference.json"), .{ .ignore_unknown_fields = true, .parse_numbers = false });
    defer fixture.deinit();
    var count: usize = 0;
    for (fixture.value.entries) |entry| {
        if (entry.@"error" != null) continue;
        errdefer std.debug.print("durable JSON lowering oracle: {s}\n", .{entry.sql});
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const owned = arena.allocator();
        var parsed = try @import("compiler.zig").compileScalar(a, entry.sql, .{});
        defer parsed.deinit();
        const binding = try json(owned, .{ .default_type = "row", .document_schemas = .{ .row = .{ .schema = .{ .properties = .{ .unused = .{ .type = "keyword" } } } } } });
        const lowered = try lowerTyped(owned, binding, parsed.expression, null);
        const kind: native.Kind = switch (entry.oid.?) {
            3802 => .json,
            25 => .string,
            23 => .integer,
            else => return error.UnexpectedJsonFixture,
        };
        var plan = try native.Plan.init(a, .{ .storage_mode = .relational, .requires_public_schema = true, .requires_json_expressions = true }, lowered.expression, kind);
        defer plan.deinit();
        const actual = try plan.evaluate(owned, &.{});
        try std.testing.expectEqual(entry.sql_null orelse (entry.value == .null), actual == .null);
        const result: Json = switch (actual) {
            .null => .null,
            .json => |root| root.value,
            .string => |value| .{ .string = value },
            .integer => |value| .{ .integer = value },
            else => return error.UnexpectedJsonFixture,
        };
        try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(owned, entry.value, .{}), try std.json.Stringify.valueAlloc(owned, result, .{}));
        if (entry.oid == 23) try std.testing.expectEqual(@as(?@import("array_value.zig").ElementType, .int32), plan.result_sql_type);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 25), count);
}

test "SQL durable JSON lowering preserves JSON null through immutable COALESCE and CASE" {
    const a = std.testing.allocator;
    const native = @import("../schema/relational_expression.zig");
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const owned = arena.allocator();
    const binding = try json(owned, .{ .default_type = "row", .document_schemas = .{ .row = .{ .schema = .{ .properties = .{ .unused = .{ .type = "keyword" } } } } } });
    for ([_][]const u8{ "coalesce('null'::jsonb, '{}'::jsonb)", "CASE WHEN true THEN 'null'::jsonb ELSE '{}'::jsonb END", "coalesce(NULL::jsonb, 'null'::jsonb, '{}'::jsonb)" }) |sql| {
        var parsed = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer parsed.deinit();
        const lowered = try lowerAssignment(owned, binding, parsed.expression, .{ .name = "g", .type = .json }, true);
        try std.testing.expectEqualStrings("json", lowered.object.get("type").?.string);
        try std.testing.expect(lowered.object.get("json_null").?.bool);
        var plan = try native.Plan.init(a, .{ .storage_mode = .relational, .requires_public_schema = true, .requires_json_expressions = true }, lowered, .json);
        defer plan.deinit();
        const actual = try plan.evaluate(owned, &.{});
        try std.testing.expect(actual == .json and actual.json.value == .null);
    }
}

test "SQL folded schema literals preserve optional builtin identities" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const plain = try inputLiteral(a, .{ .kind = .integer }, .{ .value = .{ .integer = 1 }, .sql_null = false });
    try std.testing.expectEqualStrings("integer", plain.object.get("type").?.string);
    try std.testing.expect(plain.object.get("sql_type") == null);
    const typed = try inputLiteral(a, .{ .kind = .integer, .element_type = .int32 }, .{ .value = .{ .integer = 1 }, .sql_null = false });
    try std.testing.expectEqualStrings("int32", typed.object.get("sql_type").?.string);
    const number = try inputLiteral(a, .{ .kind = .number }, .{ .value = .{ .float = 1.5 }, .sql_null = false });
    try std.testing.expectEqualStrings("number", number.object.get("type").?.string);
    try std.testing.expect(number.object.get("sql_type") == null);
    try std.testing.expectError(error.SqlTypeMismatch, inputLiteral(a, .{ .kind = .array }, .{}));
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
    var partial = try @import("compiler.zig").compileScalar(a, "n > 0", .{});
    defer partial.deinit();
    const predicates = try lowerIndexPredicate(a, schema, partial.expression);
    try std.testing.expectEqualStrings("n", predicates[0].object.get("column").?.string);
    try std.testing.expectEqualStrings("gt", predicates[0].object.get("op").?.string);
    var index = try @import("compiler.zig").compileScalar(a, "lower(label)", .{});
    defer index.deinit();
    const key = try lowerTyped(a, schema, index.expression, null);
    try std.testing.expectEqual(ast.ColumnType.string, key.type);
    var constrained = try @import("compiler.zig").compileScalar(a, "CAST(n AS numeric(4,2)) > 0", .{});
    defer constrained.deinit();
    const constrained_check = try lower(a, schema, constrained.expression, .boolean);
    const constrained_cast = constrained_check.object.get("args").?.array.items[0];
    try std.testing.expectEqualStrings("cast", constrained_cast.object.get("op").?.string);
    const modifier = constrained_cast.object.get("numeric_modifier").?;
    try std.testing.expectEqualStrings("4", modifier.object.get("precision").?.number_string);
    try std.testing.expectEqualStrings("2", modifier.object.get("scale").?.number_string);
    for ([_][]const u8{ "n + n > 0", "CAST(n AS smallint) + CAST(n AS smallint) > 0", "+n > 0", "n > -1" }) |sql| {
        var numeric = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer numeric.deinit();
        _ = try lower(a, schema, numeric.expression, .boolean);
    }
    var array = try @import("compiler.zig").compileScalar(a, "cold IS NULL", .{});
    defer array.deinit();
    const array_null = try lower(a, schema, array.expression, .boolean);
    try std.testing.expectEqualStrings("is_null", array_null.object.get("op").?.string);
}

test "SQL schema array column comparisons branches and nulls share query and durable execution" {
    var region = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer region.deinit();
    const a = region.allocator();
    const native = @import("../schema/relational_expression.zig");
    const table: @import("../storage/schema.zig").TableSchema = .{ .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "a", .path = "a", .column_type = .sql_array, .sql_element_type = .int32 },
        .{ .name = "b", .path = "b", .column_type = .sql_array, .sql_element_type = .int32 },
        .{ .name = "flag", .path = "flag", .column_type = .boolean },
    } };
    const columns = [_]scalar.Column{
        .{ .name = "a", .type = .array, .element_type = .int32 },
        .{ .name = "b", .type = .array, .element_type = .int32 },
        .{ .name = "flag", .type = .boolean },
    };
    const empty = try @import("array_value.zig").Value.init(.int32, &.{}, &.{}, .{});
    const canonical: native.Value = .{ .sql_array = .{ .element_type = .int32, .bytes = &.{ 1, 0, 0, 0, 0, 0, 0, 0 } } };
    for ([_]struct { sql: []const u8, expected: bool }{
        .{ .sql = "a IS NULL", .expected = false },
        .{ .sql = "a = b", .expected = true },
        .{ .sql = "a >= b", .expected = true },
        .{ .sql = "a IS NOT DISTINCT FROM b", .expected = true },
        .{ .sql = "a IN (b, NULL, a)", .expected = true },
        .{ .sql = "COALESCE(NULL, a) = b", .expected = true },
        .{ .sql = "CASE WHEN flag THEN a ELSE b END = a", .expected = true },
        .{ .sql = "CASE WHEN false THEN a END IS NULL", .expected = true },
        .{ .sql = "CAST(NULL AS integer[]) IS NULL", .expected = true },
    }) |case| {
        var parsed = try @import("compiler.zig").compileScalar(a, case.sql, .{});
        defer parsed.deinit();
        const lowered = try lowerColumns(a, &columns, parsed.expression, .boolean);
        var plan = try native.Plan.init(a, table, lowered.expression, .boolean);
        defer plan.deinit();
        try std.testing.expectEqual(case.expected, (try plan.evaluate(std.testing.failing_allocator, &.{ canonical, canonical, .{ .boolean = true } })).boolean);
        var query = try scalar.bind(a, parsed.expression, &columns, &.{}, .{});
        defer query.deinit();
        const actual = try query.evaluate(a, &.{ scalar.Datum.typedArray(&empty), scalar.Datum.typedArray(&empty), scalar.Datum.json(.{ .bool = true }) }, &.{}, .{});
        try std.testing.expectEqual(case.expected, actual.value.bool);
    }
    for ([_][]const u8{"CAST(a AS bigint[]) IS NULL"}) |sql| {
        var parsed = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer parsed.deinit();
        try std.testing.expectError(error.UnsupportedSqlShape, lowerColumns(a, &columns, parsed.expression, .boolean));
    }
}

test "SQL schema NUMERIC array casts share query and durable execution" {
    var region = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer region.deinit();
    const a = region.allocator();
    const native = @import("../schema/relational_expression.zig");
    const storage = @import("array_storage.zig");
    const codec = @import("../storage/db/algebraic/relational_row_codec.zig");
    const columns = [_]scalar.Column{.{ .name = "a", .type = .array, .element_type = .numeric }};
    const table: @import("../storage/schema.zig").TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "a", .path = "a", .column_type = .sql_array, .sql_element_type = .numeric },
    } };
    var original = try @import("array_text.zig").decode(a, .numeric, "[-3:-1]={12.345,NULL,-12.345}", .{});
    defer original.deinit();
    const canonical = try storage.encodeAlloc(a, original.value, .{});
    const input: native.Value = .{ .sql_array = .{ .element_type = .numeric, .bytes = canonical } };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    const row_bytes = try codec.serializeOrdinal(a, table.version, table.relational_columns, &.{
        .{ .ordinal = 0, .path = "a", .value_type = .bytes_val, .sql_array_element_type = .numeric, .value = .{ .bytes_val = canonical } },
    }, @splat(0));
    const row = try codec.ordinalRowView(row_bytes, table, &layout);
    for ([_][]const u8{
        "CAST(a AS numeric[])",
        "CAST(a AS numeric(4,2)[])",
        "CAST(NULL AS numeric(4,2)[])",
        "COALESCE(CAST(NULL AS numeric(4,2)[]), a)",
        "CAST(CAST(a AS numeric(4,2)[]) AS numeric(4,1)[])",
        "CASE WHEN false THEN CAST(a AS numeric(1,0)[]) ELSE a END",
        "COALESCE(a, CAST(a AS numeric(1,0)[]))",
    }) |sql| {
        var parsed = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer parsed.deinit();
        const lowered = try lowerColumns(a, &columns, parsed.expression, .array);
        var durable = try native.Plan.init(a, table, lowered.expression, .sql_array);
        defer durable.deinit();
        var query = try scalar.bind(a, parsed.expression, &columns, &.{}, .{});
        defer query.deinit();
        const result = try query.evaluate(a, &.{scalar.Datum.typedArray(&original.value)}, &.{}, .{});
        if (result.sql_null) {
            try std.testing.expect((try durable.evaluate(std.testing.failing_allocator, &.{input})) == .null);
            continue;
        }
        const expected = try storage.encodeAlloc(a, result.array.?.*, .{});
        const converted = try durable.evaluate(a, &.{input});
        try std.testing.expectEqualSlices(u8, expected, converted.sql_array.bytes);
        try std.testing.expectEqualSlices(u8, expected, (try durable.evaluateRow(a, row)).sql_array.bytes);
        var document: Json = .{ .object = .empty };
        try document.object.put(a, "a", try @import("array_wire.zig").toJsonLeaky(a, original.value, .{}));
        try std.testing.expectEqualSlices(u8, expected, (try durable.evaluateJson(a, document)).sql_array.bytes);
    }
}

test "SQL schema ARRAY constructors share PostgreSQL values and errors across query pinned JSON and cold rows" {
    var region = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer region.deinit();
    const a = region.allocator();
    const native = @import("../schema/relational_expression.zig");
    const arrays = @import("array_value.zig");
    const storage = @import("array_storage.zig");
    const codec = @import("../storage/db/algebraic/relational_row_codec.zig");
    const Fixture = struct { entries: []const struct { sql: []const u8, text: ?[]const u8 = null, pg_type: ?[]const u8 = null, sqlstate: ?[]const u8 = null } };
    const fixture = try std.json.parseFromSliceLeaky(Fixture, a, @embedFile("fixtures/sql_array_constructor_reference.json"), .{ .ignore_unknown_fields = true });
    const Failure = struct {
        fn check(state: []const u8, result: anytype) !void {
            _ = result catch |err| {
                try std.testing.expectEqualStrings(state, @import("errors.zig").describe(err).code);
                return;
            };
            return error.TestExpectedError;
        }
    };
    const columns = [_]scalar.Column{
        .{ .name = "n", .type = .integer, .element_type = .int32 },
        .{ .name = "t", .type = .string },
        .{ .name = "flag", .type = .boolean },
        .{ .name = "a", .type = .array, .element_type = .int32 },
        .{ .name = "b", .type = .array, .element_type = .int32 },
        .{ .name = "e", .type = .array, .element_type = .int32 },
        .{ .name = "z", .type = .array, .element_type = .int32 },
    };
    const table: @import("../storage/schema.zig").TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "n", .path = "n", .column_type = .integer, .sql_element_type = .int32 },
        .{ .name = "t", .path = "t", .column_type = .string },
        .{ .name = "flag", .path = "flag", .column_type = .boolean },
        .{ .name = "a", .path = "a", .column_type = .sql_array, .sql_element_type = .int32 },
        .{ .name = "b", .path = "b", .column_type = .sql_array, .sql_element_type = .int32 },
        .{ .name = "e", .path = "e", .column_type = .sql_array, .sql_element_type = .int32 },
        .{ .name = "z", .path = "z", .column_type = .sql_array, .sql_element_type = .int32 },
    } };
    var left = try @import("array_text.zig").decode(a, .int32, "[-2:-1]={1,NULL}", .{});
    defer left.deinit();
    var right = try @import("array_text.zig").decode(a, .int32, "[-2:-1]={3,4}", .{});
    defer right.deinit();
    const empty = try arrays.Value.init(.int32, &.{}, &.{}, .{});
    const l = try storage.encodeAlloc(a, left.value, .{});
    const r = try storage.encodeAlloc(a, right.value, .{});
    const e = try storage.encodeAlloc(a, empty, .{});
    const input = [_]native.Value{ .{ .integer = 12 }, .{ .string = "hello" }, .{ .boolean = true }, .{ .sql_array = .{ .element_type = .int32, .bytes = l } }, .{ .sql_array = .{ .element_type = .int32, .bytes = r } }, .{ .sql_array = .{ .element_type = .int32, .bytes = e } }, .null };
    const values = [_]scalar.Datum{ scalar.Datum.json(.{ .integer = 12 }), scalar.Datum.json(.{ .string = "hello" }), scalar.Datum.json(.{ .bool = true }), scalar.Datum.typedArray(&left.value), scalar.Datum.typedArray(&right.value), scalar.Datum.typedArray(&empty), .{} };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    const row_bytes = try codec.serializeOrdinal(a, table.version, table.relational_columns, &.{
        .{ .ordinal = 0, .path = "n", .value_type = .i64_val, .value = .{ .i64_val = 12 } },
        .{ .ordinal = 1, .path = "t", .value_type = .bytes_val, .value = .{ .bytes_val = "hello" } },
        .{ .ordinal = 2, .path = "flag", .value_type = .bool_val, .value = .{ .bool_val = true } },
        .{ .ordinal = 3, .path = "a", .value_type = .bytes_val, .sql_array_element_type = .int32, .value = .{ .bytes_val = l } },
        .{ .ordinal = 4, .path = "b", .value_type = .bytes_val, .sql_array_element_type = .int32, .value = .{ .bytes_val = r } },
        .{ .ordinal = 5, .path = "e", .value_type = .bytes_val, .sql_array_element_type = .int32, .value = .{ .bytes_val = e } },
    }, @splat(0));
    const row = try codec.ordinalRowView(row_bytes, table, &layout);
    var document: Json = .{ .object = .empty };
    try document.object.put(a, "n", .{ .integer = 12 });
    try document.object.put(a, "t", .{ .string = "hello" });
    try document.object.put(a, "flag", .{ .bool = true });
    try document.object.put(a, "a", try @import("array_wire.zig").toJsonLeaky(a, left.value, .{}));
    try document.object.put(a, "b", try @import("array_wire.zig").toJsonLeaky(a, right.value, .{}));
    try document.object.put(a, "e", try @import("array_wire.zig").toJsonLeaky(a, empty, .{}));
    try document.object.put(a, "z", .null);
    for (fixture.entries) |entry| {
        var parsed = try @import("compiler.zig").compileScalar(a, entry.sql, .{});
        defer parsed.deinit();
        var query = scalar.bind(a, parsed.expression, &columns, &.{}, .{}) catch |err| {
            const state = entry.sqlstate orelse return err;
            try std.testing.expectEqualStrings(state, @import("errors.zig").describe(err).code);
            try Failure.check(state, lowerColumns(a, &columns, parsed.expression, .array));
            continue;
        };
        defer query.deinit();
        const lowered = try lowerColumns(a, &columns, parsed.expression, .array);
        var durable = native.Plan.init(a, table, lowered.expression, .sql_array) catch |err| {
            std.debug.print("durable array constructor compile failed: {s}\n", .{entry.sql});
            return err;
        };
        defer durable.deinit();
        if (entry.sqlstate) |state| {
            try Failure.check(state, query.evaluate(a, &values, &.{}, .{}));
            try Failure.check(state, durable.evaluate(a, &input));
            try Failure.check(state, durable.evaluateJson(a, document));
            try Failure.check(state, durable.evaluateRow(a, row));
            continue;
        }
        const kind = query.output_type.element_type.?;
        try std.testing.expectEqualStrings(entry.pg_type.?, switch (kind) {
            .int16 => "smallint[]",
            .int32 => "integer[]",
            .int64 => "bigint[]",
            .float32 => "real[]",
            .float64 => "double precision[]",
            .numeric => "numeric[]",
            .boolean => "boolean[]",
            .text => "text[]",
            .uuid => "uuid[]",
            .jsonb => "jsonb[]",
        });
        var expected_value = try @import("array_text.zig").decode(a, kind, entry.text.?, .{});
        defer expected_value.deinit();
        const expected = try storage.encodeAlloc(a, expected_value.value, .{});
        const evaluated = try query.evaluate(a, &values, &.{}, .{});
        try std.testing.expectEqualSlices(u8, expected, try storage.encodeAlloc(a, evaluated.array.?.*, .{}));
        try std.testing.expectEqualSlices(u8, expected, (try durable.evaluate(a, &input)).sql_array.bytes);
        try std.testing.expectEqualSlices(u8, expected, (try durable.evaluateJson(a, document)).sql_array.bytes);
        try std.testing.expectEqualSlices(u8, expected, (try durable.evaluateRow(a, row)).sql_array.bytes);
    }
}

test "SQL schema ARRAY constructors unwind lowering and durable execution allocation failures" {
    const a = std.testing.allocator;
    const arrays = @import("array_value.zig");
    const storage = @import("array_storage.zig");
    const native = @import("../schema/relational_expression.zig");
    const columns = [_]scalar.Column{.{ .name = "n", .type = .integer, .element_type = .int32 }};
    const table: @import("../storage/schema.zig").TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "n", .path = "n", .column_type = .integer, .sql_element_type = .int32 },
    } };
    const Faults = struct {
        fn run(backing: std.mem.Allocator, expression: *const ast.Scalar, expected: []const u8) !void {
            var region = std.heap.ArenaAllocator.init(backing);
            defer region.deinit();
            const r = region.allocator();
            const lowered = try lowerColumns(r, &columns, expression, .array);
            var durable = try native.Plan.init(backing, table, lowered.expression, .sql_array);
            defer durable.deinit();
            const result = try durable.evaluate(r, &.{.{ .integer = 12 }});
            try std.testing.expectEqualSlices(u8, expected, result.sql_array.bytes);
        }
    };
    for ([_]struct { sql: []const u8, kind: arrays.ElementType, text: []const u8 }{
        .{ .sql = "ARRAY['1',n]", .kind = .int32, .text = "{1,12}" },
        .{ .sql = "ARRAY[CAST(n AS numeric),1.245::numeric,NULL]", .kind = .numeric, .text = "{12,1.245,NULL}" },
        .{ .sql = "ARRAY[ARRAY[n,n+1],ARRAY[n+2,NULL]]", .kind = .int32, .text = "{{12,13},{14,NULL}}" },
    }) |entry| {
        var parsed = try @import("compiler.zig").compileScalar(a, entry.sql, .{});
        defer parsed.deinit();
        var reference = try @import("array_text.zig").decode(a, entry.kind, entry.text, .{});
        defer reference.deinit();
        const expected = try storage.encodeAlloc(a, reference.value, .{});
        defer a.free(expected);
        try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Faults.run, .{ parsed.expression, expected });
    }
}

test "SQL schema comparison lowering preserves simple partial index literals" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const schema = try std.json.parseFromSliceLeaky(Json, a,
        \\{"default_type":"row","document_schemas":{"row":{"schema":{"properties":{"n":{"type":"integer"},"f":{"type":"number"}}}}}}
    , .{});
    for ([_][]const u8{ "n > 1", "f > 1", "f > CAST(1 AS real)", "n > CAST(1 AS smallint)" }) |sql| {
        var compiled = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer compiled.deinit();
        const predicates = try lowerIndexPredicate(a, schema, compiled.expression);
        try std.testing.expectEqual(@as(usize, 1), predicates.len);
        try std.testing.expectEqualStrings("gt", predicates[0].object.get("op").?.string);
        try std.testing.expectEqual(@as(f64, 1), try @import("builtin_cast.zig").floatValue(f64, predicates[0].object.get("value").?));
    }
}

test "SQL schema partial index constant bounds match PostgreSQL domains and errors" {
    const a = std.testing.allocator;
    const Case = struct { identity: []const u8, sql: []const u8, expected: ?[]const u8 = null, @"error": ?[]const u8 = null };
    var fixture = try std.json.parseFromSlice(struct { reference: []const u8, entries: []const Case }, a, @embedFile("fixtures/sql_partial_bound_reference.json"), .{});
    defer fixture.deinit();
    for (fixture.value.entries) |case| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const owned = arena.allocator();
        const identity = std.meta.stringToEnum(@import("array_value.zig").ElementType, case.identity).?;
        const physical: []const u8 = switch (identity) {
            .numeric, .float32, .float64 => "number",
            .int16, .int32, .int64 => "integer",
            .text => "string",
            .boolean => "boolean",
            else => unreachable,
        };
        const schema = try json(owned, .{ .storage_mode = "relational", .default_type = "row", .document_schemas = .{ .row = .{ .schema = .{ .properties = .{ .n = .{ .type = physical, .@"x-antfly-sql-type" = case.identity } } } } } });
        const sql = try std.fmt.allocPrint(owned, "n > ({s})", .{case.sql});
        var compiled = try @import("compiler.zig").compileScalar(owned, sql, .{});
        defer compiled.deinit();
        const predicates = lowerIndexPredicate(owned, schema, compiled.expression) catch |err| {
            if (case.@"error") |expected| {
                try std.testing.expectEqualStrings(expected, @import("errors.zig").describe(err).code);
                continue;
            }
            std.debug.print("Partial bound failed: {s}: {s}\n", .{ sql, @errorName(err) });
            return err;
        };
        if (case.@"error") |expected| {
            std.debug.print("Partial bound expected {s}: {s}\n", .{ expected, sql });
            return error.TestExpectedError;
        }
        try std.testing.expectEqual(@as(usize, 1), predicates.len);
        const value = predicates[0].object.get("value").?;
        const expected = case.expected orelse {
            try std.testing.expect(value == .null);
            continue;
        };
        switch (identity) {
            .float32 => try std.testing.expectEqual(try std.fmt.parseFloat(f32, expected), try @import("builtin_cast.zig").floatValue(f32, value)),
            .float64 => try std.testing.expectEqual(try std.fmt.parseFloat(f64, expected), try @import("builtin_cast.zig").floatValue(f64, value)),
            .boolean => try std.testing.expectEqual(std.mem.eql(u8, expected, "true"), value.bool),
            .numeric => try std.testing.expectEqualStrings(expected, if (value == .string) value.string else value.number_string),
            .text => try std.testing.expectEqualStrings(expected, value.string),
            .int16, .int32, .int64 => try std.testing.expectEqualStrings(expected, value.number_string),
            else => unreachable,
        }
    }
}

test "SQL durable numeric functions lower and execute independent PostgreSQL contracts" {
    const a = std.testing.allocator;
    const native = @import("../schema/relational_expression.zig");
    const storage = @import("../storage/schema.zig");
    const exact = @import("numeric_value.zig");
    const Entry = struct { domain: enum { numeric, float64 }, input: ?[]const u8, sql: []const u8, expected: ?[]const u8, sqlstate: ?[]const u8 };
    const fixture = try std.json.parseFromSlice(struct { entries: []const Entry }, a, @embedFile("fixtures/sql_numeric_functions_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    var exercised: usize = 0;
    for (fixture.value.entries) |entry| {
        // Durable float cells currently admit finite values only. NUMERIC has
        // its own exact special-value representation; do not alias the two.
        const floating: ?f64 = if (entry.domain == .float64 and entry.input != null) try @import("builtin_cast.zig").floatValue(f64, .{ .string = entry.input.? }) else null;
        if (floating) |value| if (!std.math.isFinite(value)) continue;
        exercised += 1;
        errdefer std.debug.print("durable numeric oracle: {s}\n", .{entry.sql});
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const owned = arena.allocator();
        const schema = try json(owned, .{ .storage_mode = "relational", .default_type = "row", .document_schemas = .{ .row = .{ .schema = .{ .properties = .{ .n = .{ .type = "number", .@"x-antfly-sql-type" = if (entry.domain == .numeric) "numeric" else "float64" } } } } } });
        const sql = try std.mem.replaceOwned(u8, owned, entry.sql, "$1", "n");
        var compiled = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer compiled.deinit();
        const lowered = try lower(owned, schema, compiled.expression, null);
        const columns = [_]storage.RelationalColumn{.{ .name = "n", .path = "n", .column_type = if (entry.domain == .numeric) .numeric else .number, .sql_element_type = if (entry.domain == .numeric) .numeric else .float64, .allows_null = true }};
        var plan = try native.Plan.init(a, .{ .relational_columns = &columns }, lowered, if (entry.domain == .numeric) .numeric else .number);
        defer plan.deinit();
        var bytes: usize = native.max_allocated_bytes;
        var execution = native.Execution.init(owned, &bytes);
        const input: native.Value = if (entry.input == null) .null else if (entry.domain == .numeric) try native.numericJson(&execution, .{ .string = entry.input.? }) else .{ .number = floating.? };
        const result = plan.evaluateWithExecution(&execution, &.{input}) catch |err| {
            try std.testing.expectEqualStrings(entry.sqlstate orelse return err, @import("errors.zig").describe(err).code);
            continue;
        };
        try std.testing.expect(entry.sqlstate == null);
        try std.testing.expectEqual(entry.expected == null, result == .null);
        if (result == .null) continue;
        if (entry.domain == .numeric) {
            var output = try @import("numeric_binary.zig").decodeCanonical(&execution.numeric, result.numeric);
            defer output.deinit();
            try std.testing.expectEqualStrings(entry.expected.?, try exact.format(&execution.numeric, output.value));
        } else {
            var hex: [16]u8 = undefined;
            try std.testing.expectEqualStrings(entry.expected.?, try std.fmt.bufPrint(&hex, "{x:0>16}", .{@as(u64, @bitCast(result.number))}));
        }
    }
    try std.testing.expectEqual(@as(usize, 189), exercised);
}

test "SQL schema partial bounds never fold columns or erase comparison promotions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const schema = try std.json.parseFromSliceLeaky(Json, a,
        \\{"default_type":"row","document_schemas":{"row":{"schema":{"properties":{"n":{"type":"number","x-antfly-sql-type":"numeric"}}}}}}
    , .{});
    for ([_][]const u8{
        "n > n+1",
        "CAST(n AS double precision)>1.0",
        "n > CAST(1 AS real)",
        "n > 1.0 OR n < 2.0",
    }) |sql| {
        var compiled = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.UnsupportedSqlShape, lowerIndexPredicate(a, schema, compiled.expression));
    }
    // Demand-aware planning may eliminate an unreachable column operand,
    // but must retain the bound's exact NUMERIC domain and comparison.
    var simplified = try @import("compiler.zig").compileScalar(a, "n > CASE WHEN true THEN 1.0 ELSE n END", .{});
    defer simplified.deinit();
    const predicates = try lowerIndexPredicate(a, schema, simplified.expression);
    try std.testing.expectEqual(@as(usize, 1), predicates.len);
    try std.testing.expectEqualStrings("n", predicates[0].object.get("column").?.string);
    try std.testing.expectEqualStrings("gt", predicates[0].object.get("op").?.string);
    const bound = predicates[0].object.get("value").?;
    try std.testing.expectEqualStrings("1.0", if (bound == .string) bound.string else bound.number_string);
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
