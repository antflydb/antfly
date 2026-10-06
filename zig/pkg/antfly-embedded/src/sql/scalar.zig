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

//! Bound scalar programs. Binding owns constants and resolves names, function
//! identities, column ordinals and parameter types once. Evaluation follows
//! typed instruction indices with bounded work; lazy branches preserve SQL
//! three-valued logic and do not evaluate unreachable errors.
const std = @import("std");
const ast = @import("ast.zig");
const datetime = @import("../datetime.zig");
const json_order = @import("json_order.zig");
const setting_catalog = @import("setting_catalog.zig");
const Allocator = std.mem.Allocator;
const Json = std.json.Value;
pub const PatternSet = struct {
    ptr: *anyopaque,
    count: usize,
    next: *const fn (*anyopaque, Allocator, *u64) anyerror!?Datum,
    close: *const fn (*anyopaque) void,
};
pub const Datum = struct {
    /// A pinned, immutable SQL array. JSON arrays remain in value and never
    /// acquire SQL-array semantics from their payload shape.
    array: ?*const @import("array_value.zig").Value = null,
    patterns: ?*PatternSet = null,
    value: Json = .null,
    sql_null: bool = true,

    pub fn fromJson(value: Json) Datum {
        return .{ .value = value, .sql_null = value == .null };
    }
    pub fn json(value: Json) Datum {
        return .{ .value = value, .sql_null = false };
    }
    pub fn typedArray(value: *const @import("array_value.zig").Value) Datum {
        return .{ .array = value, .sql_null = false };
    }
};
const arrays = @import("array_value.zig");
pub const Type = struct { kind: ?ast.ColumnType = null, nullable: bool = true, element_type: ?arrays.ElementType = null };
pub const Column = struct {
    name: []const u8,
    type: ast.ColumnType,
    nullable: bool = true,
    element_type: ?arrays.ElementType = null,
    /// Authorized alternate spellings share one cell/dependency ordinal.
    /// They are binding metadata, never additional physical row values.
    aliases: []const []const u8 = &.{},
};
pub const BindLimits = struct { nodes: usize = 8192, depth: usize = 64, parameters: usize = 1024 };
const decisions = @import("../functions/decisions.zig");
pub const DecisionDemand = struct { instruction: u32, function: decisions.Function, args: []const Json };
pub const EvalLimits = struct {
    steps: usize = 65_536,
    pattern_steps: usize = 8 * 1024 * 1024,
    depth: usize = 64,
    output_bytes: usize = 1024 * 1024,
    decision_values: ?[]const ?Datum = null,
    decision_demand: ?*?DecisionDemand = null,
};
pub const Function = enum { ai_decide, ai_choice, ai_score, ai_probability, abs, lower, upper, length, octet_length, concat, coalesce, nullif, greatest, least, ceil, floor, round, sqrt, power, mod, substring, trim, ltrim, rtrim, replace, starts_with, date_part, date_trunc, to_timestamp, current_setting, @"$single", @"$pattern_quantified", trunc, sign, to_jsonb, jsonb_build_object, jsonb_extract_path_text, concat_ws, bit_length, strpos, jsonb_typeof, lpad, rpad, repeat, reverse, left, right, split_part, translate, overlay, ascii, chr, @"$array", @"$array_quantified", cardinality, array_ndims, array_length, array_lower, array_upper };

pub const Instruction = struct {
    type: Type,
    operation: union(enum) {
        literal: Json,
        column: u32,
        parameter: u32,
        unary: struct { op: ast.Scalar.Unary, operand: u32 },
        binary: struct { op: ast.Scalar.Binary, left: u32, right: u32 },
        call: struct { function: Function, args: []const u32, setting_identity: ?setting_catalog.Identity = null },
        cast: struct { operand: u32, type: ast.ColumnType },
        case_when: struct { branches: []const Branch, otherwise: ?u32 },
        in_list: struct { operand: u32, values: []const u32, negated: bool },
    },
    const Branch = struct { condition: u32, value: u32 };
};

const TextTranslation = std.AutoHashMapUnmanaged(u21, []const u8);

pub const Program = struct {
    arena: std.heap.ArenaAllocator,
    instructions: []const Instruction,
    root: u32,
    output_type: Type,
    parameter_types: []const ?ast.ColumnType,
    required_columns: []const u32,
    settings: ?*const setting_catalog.View = null,
    /// Derived execution caches are never part of serialized instructions.
    translations: std.AutoHashMapUnmanaged(u32, *const TextTranslation) = .empty,
    constant_arrays: std.AutoHashMapUnmanaged(u32, *const arrays.Value) = .empty,

    fn literalInstruction(self: *const Program, index: u32) bool {
        return switch (self.instructions[index].operation) {
            .literal => true,
            .cast => |cast| self.literalInstruction(cast.operand),
            else => false,
        };
    }

    fn prepareConstantArrays(self: *Program, a: Allocator) !void {
        for (self.instructions, 0..) |instruction, index| {
            if (instruction.operation != .call or instruction.operation.call.function != .@"$array") continue;
            var constant = true;
            for (instruction.operation.call.args) |arg| constant = constant and self.literalInstruction(arg);
            if (!constant) continue;
            const value = try self.evaluateInstruction(a, @intCast(index), &.{});
            try self.constant_arrays.put(a, @intCast(index), value.array orelse return error.InvalidSqlProgram);
        }
    }

    pub fn deinit(self: *Program) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// Cells use the ordinal order supplied to bind(), including unselected
    /// NULL placeholders. Borrowed scalar results remain valid while the
    /// program, cells and parameters live; computed strings use alloc.
    pub fn evaluateInstruction(self: *const Program, alloc: Allocator, index: u32, parameters: []const Json) !Datum {
        var context: Evaluator = .{ .program = self, .alloc = alloc, .cells = &.{}, .parameters = parameters, .limits = .{} };
        return context.runDatum(index, 0);
    }

    pub fn evaluate(self: *const Program, alloc: Allocator, cells: []const Datum, parameters: []const Json, limits: EvalLimits) !Datum {
        var context: Evaluator = .{ .program = self, .alloc = alloc, .cells = cells, .parameters = parameters, .limits = limits };
        const result = try context.runDatum(self.root, 0);
        _ = try context.validateJson(result.value, 0);
        return result;
    }
};

/// Shared result-boundary validation. Intermediates are deliberately not
/// validated: a large or invalid input can be discarded by a lazy expression.
pub fn validateResult(result: Datum, limits: EvalLimits) !void {
    var context: Evaluator = .{ .program = undefined, .alloc = undefined, .cells = &.{}, .parameters = &.{}, .limits = limits };
    _ = try context.validateJson(result.value, 0);
}

pub fn bind(alloc: Allocator, expression: *const ast.Scalar, columns: []const Column, parameter_hints: []const ?ast.ColumnType, limits: BindLimits) !Program {
    return bindExpected(alloc, expression, columns, parameter_hints, null, limits);
}

pub fn bindWithSettings(alloc: Allocator, expression: *const ast.Scalar, columns: []const Column, parameter_hints: []const ?ast.ColumnType, limits: BindLimits, settings: ?*const setting_catalog.View) !Program {
    return bindExpectedWithSettings(alloc, expression, columns, parameter_hints, null, limits, settings);
}

/// Constraint pass for statement-wide inference. Unconstrained parameters
/// remain unknown; callers may repeat over all programs until no type changes,
/// then choose protocol defaults only for positions still unconstrained.
pub fn inferParameters(alloc: Allocator, expression: *const ast.Scalar, columns: []const Column, parameters: []?ast.ColumnType, expected: ?ast.ColumnType, limits: BindLimits) !bool {
    if (limits.parameters > 1024 or parameters.len > limits.parameters) return error.SqlProgramLimitExceeded;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var binder: Binder = .{ .alloc = arena.allocator(), .columns = columns, .limits = limits, .allow_unresolved = true };
    try binder.registerColumns();
    @memcpy(binder.parameters[0..parameters.len], parameters);
    _ = try binder.compile(expression, expected, 0);
    if (binder.parameter_count > parameters.len) return error.InvalidSqlParameters;
    var changed = false;
    for (parameters, binder.parameters[0..parameters.len]) |*output, inferred| if (inferred) |kind| {
        if (output.* == null) changed = true;
        output.* = kind;
    };
    return changed;
}

/// Read a provisional result type without defaulting unresolved parameters or
/// NULL literals. Relation-wide inference uses this before emitting programs.
pub fn inferOutput(alloc: Allocator, expression: *const ast.Scalar, columns: []const Column, parameters: []const ?ast.ColumnType) !Type {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var binder: Binder = .{ .alloc = arena.allocator(), .columns = columns, .limits = .{}, .allow_unresolved = true };
    if (parameters.len > binder.parameters.len) return error.SqlProgramLimitExceeded;
    @memcpy(binder.parameters[0..parameters.len], parameters);
    try binder.registerColumns();
    return binder.infer(expression, 0);
}

pub fn bindExpected(alloc: Allocator, expression: *const ast.Scalar, columns: []const Column, parameter_hints: []const ?ast.ColumnType, expected: ?ast.ColumnType, limits: BindLimits) !Program {
    return bindExpectedWithSettings(alloc, expression, columns, parameter_hints, expected, limits, null);
}

pub fn bindExpectedWithSettings(alloc: Allocator, expression: *const ast.Scalar, columns: []const Column, parameter_hints: []const ?ast.ColumnType, expected: ?ast.ColumnType, limits: BindLimits, settings: ?*const setting_catalog.View) !Program {
    if (limits.parameters > 1024 or parameter_hints.len > limits.parameters or limits.nodes == 0) return error.SqlProgramLimitExceeded;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    var binder: Binder = .{ .alloc = arena.allocator(), .columns = columns, .limits = limits, .settings = settings };
    try binder.registerColumns();
    @memcpy(binder.parameters[0..parameter_hints.len], parameter_hints);
    binder.parameter_count = parameter_hints.len;
    const root = try binder.compile(expression, expected, 0);
    const output_type = binder.instructions.items[root].type;
    const instructions = try binder.instructions.toOwnedSlice(binder.alloc);
    const parameter_types = try binder.alloc.dupe(?ast.ColumnType, binder.parameters[0..binder.parameter_count]);
    const required_columns = try binder.dependencies.toOwnedSlice(binder.alloc);
    var program: Program = .{
        .arena = arena,
        .instructions = instructions,
        .root = root,
        .output_type = output_type,
        .parameter_types = parameter_types,
        .required_columns = required_columns,
        .settings = settings,
        .translations = binder.translations,
    };
    try program.prepareConstantArrays(binder.alloc);
    program.arena = arena;
    return program;
}

fn numeric(kind: ?ast.ColumnType) bool {
    return kind == .integer or kind == .number;
}

test "SQL bound array expressions match PostgreSQL scalar contracts" {
    const Entry = struct { sql: []const u8, value: Json };
    const fixture = try std.json.parseFromSlice(struct { reference: []const u8, entries: []const Entry }, std.testing.allocator, @embedFile("fixtures/sql_array_expression_reference.json"), .{});
    defer fixture.deinit();
    try std.testing.expectEqual(@as(usize, 31), fixture.value.entries.len);
    for (fixture.value.entries) |entry| {
        var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, entry.sql, .{});
        defer compiled.deinit();
        var program = try bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{});
        defer program.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const result = try program.evaluate(arena.allocator(), &.{}, &.{}, .{});
        try std.testing.expectEqual(entry.value == .null, result.sql_null);
        try std.testing.expect(result.array == null);
        try std.testing.expectEqual(std.math.Order.eq, try compare(result.value, entry.value));
    }
}

test "SQL array binding retains element identity NULL provenance and bounded allocation" {
    const Faults = struct {
        fn run(a: Allocator) !void {
            var compiled = try @import("compiler.zig").compileScalar(a, "2 = ANY (ARRAY[$1, 2, NULL])", .{});
            defer compiled.deinit();
            var program = try bind(a, compiled.expression, &.{}, &.{}, .{});
            defer program.deinit();
            try std.testing.expectEqual(ast.ColumnType.integer, program.parameter_types[0].?);
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const result = try program.evaluate(arena.allocator(), &.{}, &.{.{ .integer = 1 }}, .{});
            try std.testing.expect(result.value.bool);
        }
        fn constant(a: Allocator) !void {
            var compiled = try @import("compiler.zig").compileScalar(a, "2 = ANY (ARRAY[1, 2, NULL])", .{});
            defer compiled.deinit();
            var program = try bind(a, compiled.expression, &.{}, &.{}, .{});
            defer program.deinit();
            var no_memory = std.heap.FixedBufferAllocator.init(&.{});
            try std.testing.expect((try program.evaluate(no_memory.allocator(), &.{}, &.{}, .{})).value.bool);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.constant, .{});
    var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, "ARRAY[1, NULL]", .{});
    defer compiled.deinit();
    var program = try bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{});
    defer program.deinit();
    try std.testing.expectEqual(ast.ColumnType.array, program.output_type.kind.?);
    try std.testing.expectEqual(arrays.ElementType.int64, program.output_type.element_type.?);
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, program.evaluate(arena.allocator(), &.{}, &.{}, .{ .output_bytes = 1 }));
    const value = try program.evaluate(arena.allocator(), &.{}, &.{}, .{});
    try std.testing.expect(!value.sql_null);
    try std.testing.expect(value.array.?.elements[1].sql_null);
    const narrow = try arrays.Value.init(.int32, &.{.{ .length = 1 }}, &.{arrays.Element.json(.{ .integer = 1 })}, .{});
    var work: arrays.Budget = .{};
    try std.testing.expectEqual(@as(?bool, false), try narrow.quantified(arrays.Element.json(.{ .integer = 9223372036854775807 }), .eq, .any, &work));
    for ([_][]const u8{ "ARRAY[1] + ARRAY[2]", "lower(ARRAY[1])", "ARRAY[1] = ARRAY['1']", "jsonb_typeof(ARRAY[1])" }) |sql| {
        var invalid = try @import("compiler.zig").compileScalar(std.testing.allocator, sql, .{});
        defer invalid.deinit();
        try std.testing.expectError(error.SqlTypeMismatch, bind(std.testing.allocator, invalid.expression, &.{}, &.{}, .{}));
    }
    for ([_][]const u8{ "to_jsonb(ARRAY[1])", "jsonb_build_object('x', ARRAY[1])", "concat(ARRAY[1])", "concat_ws(',', ARRAY[1])" }) |sql| {
        var unsupported = try @import("compiler.zig").compileScalar(std.testing.allocator, sql, .{});
        defer unsupported.deinit();
        try std.testing.expectError(error.UnsupportedSqlShape, bind(std.testing.allocator, unsupported.expression, &.{}, &.{}, .{}));
    }
    var strict = try @import("compiler.zig").compileScalar(std.testing.allocator, "array_length(a, 1 / 0)", .{});
    defer strict.deinit();
    var strict_program = try bind(std.testing.allocator, strict.expression, &.{.{ .name = "a", .type = .array, .element_type = .int32 }}, &.{}, .{});
    defer strict_program.deinit();
    try std.testing.expectError(error.SqlDivisionByZero, strict_program.evaluate(arena.allocator(), &.{.{}}, &.{}, .{}));
}

test "SQL constant array preparation eliminates hot loop constructor allocation" {
    var prepared = try @import("compiler.zig").compileScalar(std.testing.allocator, "probe = ANY (ARRAY[1,2,3,4,5,6,7,8])", .{});
    defer prepared.deinit();
    var dynamic = try @import("compiler.zig").compileScalar(std.testing.allocator, "probe = ANY (ARRAY[$1,2,3,4,5,6,7,8])", .{});
    defer dynamic.deinit();
    const columns = &.{Column{ .name = "probe", .type = .integer }};
    var cached = try bind(std.testing.allocator, prepared.expression, columns, &.{}, .{});
    defer cached.deinit();
    var uncached = try bind(std.testing.allocator, dynamic.expression, columns, &.{}, .{});
    defer uncached.deinit();
    try std.testing.expectEqual(@as(usize, 1), cached.constant_arrays.count());
    try std.testing.expectEqual(@as(usize, 0), uncached.constant_arrays.count());
    var none = std.heap.FixedBufferAllocator.init(&.{});
    var storage: [4096]u8 = undefined;
    var scratch = std.heap.FixedBufferAllocator.init(&storage);
    var counts: [2]usize = .{ 0, 0 };
    var elapsed: [2]i128 = undefined;
    var dynamic_bytes: usize = 0;
    for ([_]*const Program{ &cached, &uncached }, 0..) |program, mode| {
        const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
        for (0..10000) |i| {
            scratch.reset();
            const result = try program.evaluate(if (mode == 0) none.allocator() else scratch.allocator(), &.{Datum.json(.{ .integer = @intCast(i % 16) })}, &.{.{ .integer = 1 }}, .{});
            counts[mode] += @intFromBool(result.value.bool);
            dynamic_bytes = @max(dynamic_bytes, scratch.end_index);
        }
        elapsed[mode] = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start;
    }
    try std.testing.expectEqual(@as(usize, 5000), counts[0]);
    try std.testing.expectEqual(counts[0], counts[1]);
    std.debug.print("SQL array constructors: rows=10000 prepared_ns={} dynamic_ns={} prepared_scratch_bytes=0 dynamic_scratch_bytes={}\n", .{ elapsed[0], elapsed[1], dynamic_bytes });
}
fn arrayElementType(kind: ast.ColumnType) !arrays.ElementType {
    return switch (kind) {
        .string => .text,
        .uuid => .uuid,
        .integer => .int64,
        .number => .float64,
        .boolean => .boolean,
        .json => .jsonb,
        .datetime, .array => error.UnsupportedSqlShape,
    };
}
fn arrayScalarType(kind: arrays.ElementType) ast.ColumnType {
    return switch (kind) {
        .text => .string,
        .uuid => .uuid,
        .int16, .int32, .int64 => .integer,
        .float32, .float64 => .number,
        .boolean => .boolean,
        .jsonb => .json,
    };
}

fn mixedNumberQuantified(value: arrays.Value, probe: arrays.Element, op: arrays.Comparison, every: bool, work: *arrays.Budget) !?bool {
    var unknown = false;
    for (value.elements) |element| {
        try work.consume(1);
        if (probe.sql_null or element.sql_null) {
            unknown = true;
            continue;
        }
        const accepted = op.accepts(try compare(probe.value, element.value));
        if (accepted != every) return accepted;
    }
    return if (unknown) null else every;
}
fn uuidTextOperand(expression: *const ast.Scalar) bool {
    return expression.* == .literal and expression.literal == .string;
}
fn common(left: Type, right: Type) !Type {
    if (left.kind == .array or right.kind == .array) {
        const chosen = if (left.kind == .array) left else right;
        const other = if (left.kind == .array) right else left;
        if (chosen.element_type == null or (other.kind != null and (other.kind != .array or other.element_type != chosen.element_type))) return error.SqlTypeMismatch;
        return .{ .kind = .array, .element_type = chosen.element_type, .nullable = left.nullable or right.nullable };
    }
    const kind = if (left.kind == null) right.kind else if (right.kind == null) left.kind else if (left.kind == right.kind) left.kind else if ((left.kind == .uuid and right.kind == .string) or (left.kind == .string and right.kind == .uuid)) ast.ColumnType.uuid else if (numeric(left.kind) and numeric(right.kind)) ast.ColumnType.number else return error.SqlTypeMismatch;
    return .{ .kind = kind, .nullable = left.nullable or right.nullable };
}
fn literalType(value: ast.Value) Type {
    return .{ .kind = switch (value) {
        .null, .parameter => null,
        .boolean => .boolean,
        .integer => .integer,
        .number => .number,
        .string => .string,
    }, .nullable = value == .null or value == .parameter };
}
pub fn statementConstant(node: *const ast.Scalar) bool {
    return switch (node.*) {
        .literal => true,
        .cast => |cast| statementConstant(cast.operand),
        else => false,
    };
}

fn functionId(name: []const u8) !Function {
    inline for (@typeInfo(Function).@"enum".field_names, @typeInfo(Function).@"enum".field_values) |reflected_name, field_value| if (std.mem.eql(u8, name, reflected_name)) return @fromBackingInt(field_value);
    if (std.mem.eql(u8, name, "char_length") or std.mem.eql(u8, name, "character_length")) return .length;
    if (std.mem.eql(u8, name, "ceiling")) return .ceil;
    if (std.mem.eql(u8, name, "substr")) return .substring;
    if (std.mem.eql(u8, name, "position")) return .strpos;
    if (std.mem.eql(u8, name, "btrim")) return .trim;
    return error.UnsupportedSqlShape;
}
fn arity(function: Function, count: usize) !void {
    const valid = switch (function) {
        .@"$array" => true,
        .@"$array_quantified" => count == 4,
        .cardinality, .array_ndims => count == 1,
        .array_length, .array_lower, .array_upper => count == 2,
        .ai_decide, .ai_probability => count == 3,
        .ai_choice, .ai_score => count == 4,
        .abs, .lower, .upper, .length, .octet_length, .ceil, .floor, .round, .trunc, .sign, .sqrt, .to_timestamp, .current_setting, .to_jsonb, .bit_length, .jsonb_typeof, .reverse, .ascii, .chr => count == 1,
        .concat_ws => count >= 2,
        .jsonb_build_object => count % 2 == 0,
        .jsonb_extract_path_text => count >= 2,
        .nullif, .power, .mod, .starts_with, .strpos, .repeat, .left, .right, .date_part, .date_trunc, .@"$single" => count == 2,
        .@"$pattern_quantified" => count == 5,
        .substring, .lpad, .rpad => count == 2 or count == 3,
        .replace, .translate, .split_part => count == 3,
        .overlay => count == 3 or count == 4,
        .trim, .ltrim, .rtrim => count == 1 or count == 2,
        .coalesce, .greatest, .least => count > 0,
        .concat => count > 0,
    };
    if (!valid) return error.InvalidSqlParameters;
}

const Binder = struct {
    alloc: Allocator,
    columns: []const Column,
    limits: BindLimits,
    names: std.StringHashMapUnmanaged(u32) = .empty,
    inferred: std.AutoHashMapUnmanaged(*const ast.Scalar, Type) = .empty,
    instructions: std.ArrayList(Instruction) = .empty,
    translations: std.AutoHashMapUnmanaged(u32, *const TextTranslation) = .empty,
    dependencies: std.ArrayList(u32) = .empty,
    parameters: [1024]?ast.ColumnType = @splat(null),
    parameter_count: usize = 0,
    allow_unresolved: bool = false,
    settings: ?*const setting_catalog.View = null,

    fn registerColumns(self: *Binder) !void {
        for (self.columns, 0..) |column, ordinal| {
            if (column.aliases.len > 3) return error.SqlProgramLimitExceeded;
            try self.registerName(column.name, ordinal);
            for (column.aliases) |alias| try self.registerName(alias, ordinal);
        }
    }
    fn registerName(self: *Binder, name: []const u8, ordinal: usize) !void {
        if (self.names.count() >= self.limits.nodes) return error.SqlProgramLimitExceeded;
        const entry = try self.names.getOrPut(self.alloc, name);
        if (entry.found_existing and entry.value_ptr.* != ordinal) return error.AmbiguousSqlColumn;
        entry.value_ptr.* = @intCast(ordinal);
    }

    fn infer(self: *Binder, expression: *const ast.Scalar, depth: usize) anyerror!Type {
        if (depth >= self.limits.depth) return error.SqlProgramLimitExceeded;
        if (self.inferred.get(expression)) |cached| return cached;
        if (self.inferred.count() >= self.limits.nodes) return error.SqlProgramLimitExceeded;
        const result: Type = switch (expression.*) {
            .literal => |value| if (value == .parameter) blk: {
                if (value.parameter == 0 or value.parameter > self.limits.parameters) return error.InvalidSqlParameters;
                break :blk .{ .kind = self.parameters[value.parameter - 1] };
            } else literalType(value),
            .column => |name| blk: {
                const column = self.columns[self.names.get(name) orelse return error.UnknownColumn];
                if (column.type == .array and column.element_type == null) return error.InvalidSqlProgram;
                break :blk .{ .kind = column.type, .nullable = column.nullable, .element_type = column.element_type };
            },
            .cast => |cast| .{ .kind = cast.type, .nullable = (try self.infer(cast.operand, depth + 1)).nullable },
            .unary => |unary| blk: {
                const input = try self.infer(unary.operand, depth + 1);
                switch (unary.op) {
                    .positive, .negative => {
                        if (input.kind != null and !numeric(input.kind)) return error.SqlTypeMismatch;
                        break :blk input;
                    },
                    .not, .is_true, .is_not_true, .is_false, .is_not_false => if (input.kind != null and input.kind != .boolean) return error.SqlTypeMismatch,
                    else => {},
                }
                break :blk .{ .kind = .boolean, .nullable = unary.op == .not and input.nullable };
            },
            .binary => |binary| blk: {
                const left = try self.infer(binary.left, depth + 1);
                const right = try self.infer(binary.right, depth + 1);
                if ((left.kind == .uuid and right.kind == .string and !uuidTextOperand(binary.right)) or
                    (left.kind == .string and right.kind == .uuid and !uuidTextOperand(binary.left))) return error.SqlTypeMismatch;
                if (binary.op == .json_get or binary.op == .json_text) {
                    if (left.kind != null and left.kind != .json) return error.SqlTypeMismatch;
                    if (right.kind != null and right.kind != .string and right.kind != .integer) return error.SqlTypeMismatch;
                    break :blk .{ .kind = if (binary.op == .json_get) .json else .string };
                }
                const merged = try common(left, right);
                switch (binary.op) {
                    .add, .subtract, .multiply, .divide, .modulo => {
                        if (merged.kind != null and !numeric(merged.kind)) return error.SqlTypeMismatch;
                        break :blk merged;
                    },
                    .concat, .like, .ilike => if (merged.kind != null and merged.kind != .string) return error.SqlTypeMismatch,
                    .@"and", .@"or" => if (merged.kind != null and merged.kind != .boolean) return error.SqlTypeMismatch,
                    else => {},
                }
                break :blk .{ .kind = if (binary.op == .concat) .string else .boolean, .nullable = if (binary.op == .is_distinct or binary.op == .is_not_distinct) false else merged.nullable };
            },
            .call => |call| blk: {
                if (call.subquery != null or call.window != null or call.star or call.distinct or call.filter != null) return error.UnsupportedSqlShape;
                if (std.mem.eql(u8, call.name, "$validate")) {
                    if (call.args.len == 0) return error.InvalidSqlParameters;
                    for (call.args) |arg| _ = try self.infer(arg, depth + 1);
                    break :blk .{ .kind = .integer, .nullable = false };
                }
                const function = try functionId(call.name);
                try arity(function, call.args.len);
                if (function == .@"$array") {
                    if (call.args.len == 0) return error.UnknownSqlArrayType;
                    var element: Type = .{};
                    for (call.args) |arg| element = try common(element, try self.infer(arg, depth + 1));
                    if (element.kind == .array) return error.UnsupportedSqlShape;
                    break :blk .{ .kind = .array, .nullable = false, .element_type = try arrayElementType(element.kind orelse .string) };
                }
                if (function == .@"$array_quantified" or function == .cardinality or function == .array_ndims or function == .array_length or function == .array_lower or function == .array_upper) {
                    const array_index: usize = if (function == .@"$array_quantified") 1 else 0;
                    const input = try self.infer(call.args[array_index], depth + 1);
                    if (input.kind != .array or input.element_type == null) return error.SqlTypeMismatch;
                    for (call.args, 0..) |arg, i| {
                        if (i == array_index) continue;
                        const actual = try self.infer(arg, depth + 1);
                        const desired: ast.ColumnType = if (function == .@"$array_quantified") (if (i == 0) arrayScalarType(input.element_type.?) else if (i == 2) .integer else .boolean) else .integer;
                        if (actual.kind != null and actual.kind != desired and !(numeric(actual.kind) and numeric(desired))) return error.SqlTypeMismatch;
                    }
                    break :blk .{ .kind = if (function == .@"$array_quantified") .boolean else .integer, .nullable = true };
                }
                for (call.args) |arg| if ((try self.infer(arg, depth + 1)).kind == .array) {
                    switch (function) {
                        .coalesce, .nullif, .greatest, .least, .@"$single" => {},
                        // PostgreSQL has these array overloads, but they need
                        // lossless array-to-JSON/text conversion, not .value
                        // (which intentionally remains JSON null for arrays).
                        .to_jsonb, .jsonb_build_object, .concat, .concat_ws => return error.UnsupportedSqlShape,
                        else => return error.SqlTypeMismatch,
                    }
                };
                if (decisions.descriptor(@tagName(function))) |desc| {
                    for (call.args, 0..) |arg, i| {
                        const actual = try self.infer(arg, depth + 1);
                        if (i > 0 and !statementConstant(arg)) return error.UnsupportedSqlShape;
                        const schema_arg = (function == .ai_decide and i == 1) or ((function == .ai_choice or function == .ai_score) and i == 2);
                        if (actual.kind != null and actual.kind != .string and !(schema_arg and actual.kind == .json)) return error.SqlTypeMismatch;
                    }
                    break :blk .{ .kind = switch (desc.result) {
                        .json => .json,
                        .string => .string,
                        .number => .number,
                    }, .nullable = true };
                }

                if (function == .current_setting) {
                    const name = call.args[0];
                    if (name.* != .literal or name.literal != .string) return error.UnsupportedSqlShape;
                    break :blk .{ .kind = .string, .nullable = false };
                }
                if (function == .@"$pattern_quantified") {
                    for (call.args, 0..) |arg, index| {
                        const actual = try self.infer(arg, depth + 1);
                        const required: ast.ColumnType = if (index == 0) .string else if (index == 1) .json else .boolean;
                        if (actual.kind != null and actual.kind != required) return error.SqlTypeMismatch;
                    }
                    break :blk .{ .kind = .boolean, .nullable = true };
                }
                var merged: Type = .{};
                switch (function) {
                    .@"$single" => merged = try self.infer(call.args[0], depth + 1),
                    .coalesce, .nullif, .greatest, .least, .abs, .ceil, .floor, .round, .trunc, .sign, .sqrt, .power, .mod => for (call.args) |arg| {
                        merged = try common(merged, try self.infer(arg, depth + 1));
                    },
                    else => for (call.args) |arg| {
                        _ = try self.infer(arg, depth + 1);
                    },
                }
                if (function == .abs or function == .ceil or function == .floor or function == .round or function == .trunc or function == .sign or function == .sqrt or function == .power or function == .mod) {
                    if (merged.kind != null and !numeric(merged.kind)) return error.SqlTypeMismatch;
                }
                break :blk .{ .element_type = merged.element_type, .kind = switch (function) {
                    .to_jsonb, .jsonb_build_object => .json,
                    .length, .octet_length, .bit_length, .strpos, .ascii => .integer,
                    .starts_with, .@"$pattern_quantified" => .boolean,
                    .sqrt, .power, .date_part => .number,
                    .date_trunc, .to_timestamp => .datetime,
                    .coalesce, .nullif, .greatest, .least, .abs, .ceil, .floor, .round, .trunc, .sign, .mod, .@"$single" => merged.kind,
                    else => .string,
                }, .nullable = function != .concat and function != .jsonb_build_object };
            },
            .case_when => |case| blk: {
                var merged: Type = if (case.otherwise) |other| try self.infer(other, depth + 1) else .{};
                for (case.branches) |branch| {
                    const condition = try self.infer(branch.condition, depth + 1);
                    if (condition.kind != null and condition.kind != .boolean) return error.SqlTypeMismatch;
                    merged = try common(merged, try self.infer(branch.value, depth + 1));
                }
                break :blk merged;
            },
            .in_list => |list| blk: {
                var merged = try self.infer(list.operand, depth + 1);
                for (list.values) |item| merged = try common(merged, try self.infer(item, depth + 1));
                break :blk .{ .kind = .boolean, .nullable = merged.nullable };
            },
        };
        try self.inferred.put(self.alloc, expression, result);
        return result;
    }

    fn compile(self: *Binder, expression: *const ast.Scalar, expected: ?ast.ColumnType, depth: usize) anyerror!u32 {
        if (depth >= self.limits.depth or self.instructions.items.len >= self.limits.nodes) return error.SqlProgramLimitExceeded;
        var kind = try self.infer(expression, depth);
        if (kind.kind == null) kind.kind = expected;
        if (expected == .uuid and expression.* == .literal and expression.literal == .string) kind.kind = .uuid;
        var instruction: Instruction = .{ .type = kind, .operation = undefined };
        instruction.operation = switch (expression.*) {
            .literal => |value| if (value == .parameter) blk: {
                if (kind.kind == .array) return error.UnsupportedSqlShape; // Array parameter descriptors are not yet public.
                const resolved = kind.kind;
                if (resolved == null and !self.allow_unresolved) return error.UnknownSqlParameterType;
                const index = value.parameter - 1;
                if (self.parameters[index]) |prior| {
                    if (resolved != null and prior != resolved.?) return error.ConflictingSqlParameterTypes;
                }
                if (resolved != null) self.parameters[index] = resolved;
                self.parameter_count = @max(self.parameter_count, value.parameter);
                break :blk .{ .parameter = index };
            } else .{ .literal = switch (value) {
                .null => .null,
                .boolean => |v| .{ .bool = v },
                .integer => |v| .{ .integer = v },
                .number => |v| .{ .float = v },
                .string => |v| .{ .string = if (kind.kind == .uuid) @import("../common/uuid.zig").canonicalAlloc(self.alloc, v) catch |err| switch (err) {
                    error.InvalidUuid => return error.SqlTypeMismatch,
                    else => return err,
                } else try self.alloc.dupe(u8, v) },
                .parameter => unreachable,
            } },
            .column => |name| blk: {
                const ordinal = self.names.get(name).?;
                if (std.mem.indexOfScalar(u32, self.dependencies.items, ordinal) == null) try self.dependencies.append(self.alloc, ordinal);
                break :blk .{ .column = ordinal };
            },
            .cast => |cast| .{ .cast = .{ .operand = try self.compile(cast.operand, cast.type, depth + 1), .type = cast.type } },
            .unary => |unary| .{ .unary = .{ .op = unary.op, .operand = try self.compile(unary.operand, switch (unary.op) {
                .not, .is_true, .is_not_true, .is_false, .is_not_false => .boolean,
                .positive, .negative => kind.kind,
                else => null,
            }, depth + 1) } },
            .binary => |binary| blk: {
                if (binary.op == .json_get or binary.op == .json_text) break :blk .{ .binary = .{ .op = binary.op, .left = try self.compile(binary.left, .json, depth + 1), .right = try self.compile(binary.right, .string, depth + 1) } };
                const merged = try common(try self.infer(binary.left, depth + 1), try self.infer(binary.right, depth + 1));
                const operand_kind: ?ast.ColumnType = switch (binary.op) {
                    .@"and", .@"or" => .boolean,
                    .concat, .like, .ilike => .string,
                    .add, .subtract, .multiply, .divide, .modulo => merged.kind orelse expected,
                    else => merged.kind,
                };
                break :blk .{ .binary = .{ .op = binary.op, .left = try self.compile(binary.left, operand_kind, depth + 1), .right = try self.compile(binary.right, operand_kind, depth + 1) } };
            },
            .call => |call| blk: {
                if (std.mem.eql(u8, call.name, "$validate")) {
                    // Discarded EXISTS projections still bind names, types,
                    // functions and parameters. Their code and column reads
                    // must not become runtime dependencies of the count.
                    var scratch = std.heap.ArenaAllocator.init(self.alloc);
                    defer scratch.deinit();
                    var validator: Binder = .{
                        .alloc = scratch.allocator(),
                        .columns = self.columns,
                        .limits = self.limits,
                        .names = self.names,
                        .parameters = self.parameters,
                        .parameter_count = self.parameter_count,
                        .allow_unresolved = self.allow_unresolved,
                    };
                    for (call.args) |arg| _ = try validator.compile(arg, null, depth + 1);
                    self.parameters = validator.parameters;
                    self.parameter_count = validator.parameter_count;
                    break :blk .{ .literal = .{ .integer = 1 } };
                }
                const function = try functionId(call.name);
                if (function == .current_setting) {
                    if (self.allow_unresolved) break :blk .{ .literal = .{ .string = "" } };
                    const view = self.settings orelse return error.SettingCatalogUnavailable;
                    if (call.args[0].* != .literal or call.args[0].literal != .string) return error.UnsupportedSqlShape;
                    const resolved = try view.resolve(call.args[0].literal.string);
                    break :blk .{ .call = .{ .function = function, .args = &.{}, .setting_identity = resolved.identity } };
                }
                const args = try self.alloc.alloc(u32, call.args.len);
                for (call.args, args, 0..) |arg, *out, i| {
                    const desired: ?ast.ColumnType = switch (function) {
                        .@"$array" => arrayScalarType(kind.element_type orelse return error.InvalidSqlProgram),
                        .@"$array_quantified" => if (i == 0) (if ((try self.infer(arg, depth + 1)).kind == .number) .number else arrayScalarType((try self.infer(call.args[1], depth + 1)).element_type.?)) else if (i == 1) .array else if (i == 2) .integer else .boolean,
                        .cardinality, .array_ndims, .array_length, .array_lower, .array_upper => if (i == 0) .array else .integer,
                        .ai_decide, .ai_choice, .ai_score, .ai_probability => if ((try self.infer(arg, depth + 1)).kind == .json) .json else .string,
                        .@"$single" => if (i == 0) kind.kind else .integer,
                        .@"$pattern_quantified" => if (i == 0) .string else if (i == 1) .json else .boolean,
                        .lower, .upper, .length, .octet_length, .trim, .ltrim, .rtrim, .replace, .starts_with, .strpos, .bit_length, .reverse, .translate, .ascii => .string,
                        .concat_ws => if (i == 0) .string else null,
                        .jsonb_typeof => .json,
                        .substring, .repeat, .left, .right => if (i == 0) .string else .integer,
                        .split_part => if (i == 2) .integer else .string,
                        .overlay => if (i < 2) .string else .integer,
                        .chr => .integer,
                        .lpad, .rpad => if (i == 1) .integer else .string,
                        .date_part, .date_trunc => if (i == 0) .string else .datetime,
                        .to_timestamp => .number,
                        .concat => null,
                        .to_jsonb, .jsonb_build_object => null,
                        .jsonb_extract_path_text => if (i == 0) .json else .string,
                        else => kind.kind,
                    };
                    const actual = try self.infer(arg, depth + 1);
                    if (desired != null and actual.kind != null and desired != actual.kind and !(desired == .datetime and actual.kind == .string) and !(desired == .uuid and actual.kind == .string and uuidTextOperand(arg)) and !(numeric(desired) and numeric(actual.kind))) return error.SqlTypeMismatch;
                    out.* = try self.compile(arg, desired, depth + 1);
                }
                if (function == .translate) {
                    const from = self.instructions.items[args[1]].operation;
                    const to = self.instructions.items[args[2]].operation;
                    if (from == .literal and from.literal == .string and to == .literal and to.literal == .string) {
                        // Program-owned literal slices outlive the parsed AST.
                        // A constant alphabet is prepared once, not per row.
                        const mapping = try self.alloc.create(TextTranslation);
                        mapping.* = .empty;
                        var source = (std.unicode.Utf8View.init(from.literal.string) catch return error.SqlTypeMismatch).iterator();
                        var target = (std.unicode.Utf8View.init(to.literal.string) catch return error.SqlTypeMismatch).iterator();
                        while (source.nextCodepoint()) |codepoint| {
                            const replacement = target.nextCodepointSlice() orelse "";
                            if (!mapping.contains(codepoint)) try mapping.put(self.alloc, codepoint, replacement);
                        }
                        try self.translations.put(self.alloc, @intCast(self.instructions.items.len), mapping);
                    }
                }
                break :blk .{ .call = .{ .function = function, .args = args } };
            },
            .case_when => |case| blk: {
                const branches = try self.alloc.alloc(Instruction.Branch, case.branches.len);
                for (case.branches, branches) |branch, *out| {
                    if (kind.kind == .uuid and (try self.infer(branch.value, depth + 1)).kind == .string and !uuidTextOperand(branch.value)) return error.SqlTypeMismatch;
                    out.* = .{ .condition = try self.compile(branch.condition, .boolean, depth + 1), .value = try self.compile(branch.value, kind.kind, depth + 1) };
                }
                if (case.otherwise) |other| if (kind.kind == .uuid and (try self.infer(other, depth + 1)).kind == .string and !uuidTextOperand(other)) return error.SqlTypeMismatch;
                break :blk .{ .case_when = .{ .branches = branches, .otherwise = if (case.otherwise) |other| try self.compile(other, kind.kind, depth + 1) else null } };
            },
            .in_list => |list| blk: {
                var merged = try self.infer(list.operand, depth + 1);
                for (list.values) |item| merged = try common(merged, try self.infer(item, depth + 1));
                if (merged.kind == .uuid) {
                    if ((try self.infer(list.operand, depth + 1)).kind == .string and !uuidTextOperand(list.operand)) return error.SqlTypeMismatch;
                    for (list.values) |item| if ((try self.infer(item, depth + 1)).kind == .string and !uuidTextOperand(item)) return error.SqlTypeMismatch;
                }
                const values = try self.alloc.alloc(u32, list.values.len);
                for (list.values, values) |item, *out| out.* = try self.compile(item, merged.kind, depth + 1);
                break :blk .{ .in_list = .{ .operand = try self.compile(list.operand, merged.kind, depth + 1), .values = values, .negated = list.negated } };
            },
        };
        if (self.instructions.items.len >= self.limits.nodes) return error.SqlProgramLimitExceeded;
        const index: u32 = @intCast(self.instructions.items.len);
        try self.instructions.append(self.alloc, instruction);
        return index;
    }
};

const Evaluator = struct {
    program: *const Program,
    alloc: Allocator,
    cells: []const Datum,
    parameters: []const Json,
    limits: EvalLimits,
    steps: usize = 0,
    pattern_steps: usize = 0,
    bytes: usize = 0,

    fn charge(self: *Evaluator, bytes: usize) !void {
        if (bytes > self.limits.output_bytes -| self.bytes) return error.SqlProgramLimitExceeded;
        self.bytes += bytes;
    }

    fn run(self: *Evaluator, index: u32, depth: usize) anyerror!Json {
        const result = try self.runDatum(index, depth);
        if (result.array != null) return error.SqlTypeMismatch;
        return result.value;
    }

    fn runDatum(self: *Evaluator, index: u32, depth: usize) anyerror!Datum {
        if (depth >= self.limits.depth or self.steps >= self.limits.steps) return error.SqlProgramLimitExceeded;
        self.steps += 1;
        const instruction = self.program.instructions[index];
        var result: Datum = switch (instruction.operation) {
            .parameter => |slot| blk: {
                if (slot >= self.parameters.len) return error.InvalidSqlParameters;
                const value = self.parameters[slot];
                if (value == .null) break :blk .{};
                break :blk Datum.json(try self.convert(value, instruction.type.kind.?));
            },
            .column => |ordinal| if (ordinal < self.cells.len) self.cells[ordinal] else return error.InvalidSqlBackendResponse,
            .cast => |cast| blk: {
                const datum = try self.runDatum(cast.operand, depth + 1);
                if (datum.sql_null) break :blk .{};
                if (datum.array != null) return error.SqlTypeMismatch;
                if (cast.type == .string and self.program.instructions[cast.operand].type.kind == .json) break :blk Datum.json(.{ .string = try self.jsonText(datum.value) });
                if (datum.value == .null and cast.type == .string) break :blk Datum.json(.{ .string = "null" });
                if (datum.value == .null and cast.type != .json) return error.SqlTypeMismatch;
                break :blk Datum.json(try self.convert(datum.value, cast.type));
            },
            .unary => |unary| if (unary.op == .is_null or unary.op == .is_not_null) blk: {
                const datum = try self.runDatum(unary.operand, depth + 1);
                break :blk Datum.json(.{ .bool = datum.sql_null == (unary.op == .is_null) });
            } else Datum.fromJson(try self.runLegacy(index, depth)),
            .binary => |binary| if (binary.op == .json_get or binary.op == .json_text) blk: {
                const left = try self.runDatum(binary.left, depth + 1);
                const right = try self.runDatum(binary.right, depth + 1);
                if (left.sql_null or right.sql_null) break :blk .{};
                if (left.array != null or right.array != null) return error.SqlTypeMismatch;
                const value: Json = if (left.value == .object and right.value == .string)
                    left.value.object.get(right.value.string) orelse break :blk .{}
                else if (left.value == .array and right.value == .integer) selected: {
                    const items = left.value.array.items;
                    const slot = if (right.value.integer < 0) @as(i128, @intCast(items.len)) + right.value.integer else right.value.integer;
                    if (slot < 0 or slot >= items.len) break :blk .{};
                    break :selected items[@intCast(slot)];
                } else break :blk .{};
                if (binary.op == .json_get) break :blk Datum.json(value);
                if (value == .null) break :blk .{};
                break :blk Datum.json(.{ .string = if (value == .string) value.string else try self.jsonText(value) });
            } else if (binary.op == .eq or binary.op == .neq or binary.op == .lt or binary.op == .lte or binary.op == .gt or binary.op == .gte or binary.op == .is_distinct or binary.op == .is_not_distinct) blk: {
                const left = try self.runDatum(binary.left, depth + 1);
                const right = try self.runDatum(binary.right, depth + 1);
                if (binary.op == .is_distinct or binary.op == .is_not_distinct) {
                    const equal = if (left.sql_null or right.sql_null) left.sql_null and right.sql_null else (try compareDatums(left, right)) == .eq;
                    break :blk Datum.json(.{ .bool = equal == (binary.op == .is_not_distinct) });
                }
                if (left.sql_null or right.sql_null) break :blk .{};
                break :blk Datum.json(comparison(binary.op, try compareDatums(left, right)));
            } else Datum.fromJson(try self.runLegacy(index, depth)),
            .case_when => |case| blk: {
                for (case.branches) |branch| {
                    const condition = try self.runDatum(branch.condition, depth + 1);
                    if (!condition.sql_null) {
                        if (condition.value != .bool) return error.SqlTypeMismatch;
                        if (condition.value.bool) break :blk try self.runDatum(branch.value, depth + 1);
                    }
                }
                break :blk if (case.otherwise) |other| try self.runDatum(other, depth + 1) else .{};
            },
            .in_list => |list| blk: {
                const operand = try self.runDatum(list.operand, depth + 1);
                if (operand.sql_null) break :blk .{};
                var unknown = false;
                for (list.values) |item| {
                    const value = try self.runDatum(item, depth + 1);
                    if (value.sql_null) {
                        unknown = true;
                        continue;
                    }
                    if ((try compareDatums(operand, value)) == .eq) break :blk Datum.json(.{ .bool = !list.negated });
                }
                break :blk if (unknown) .{} else Datum.json(.{ .bool = list.negated });
            },
            .call => |call| blk: {
                if (decisions.descriptor(@tagName(call.function))) |desc| {
                    if (self.limits.decision_values) |values| if (index < values.len) if (values[index]) |value| break :blk value;
                    const args = try self.alloc.alloc(Json, call.args.len);
                    for (call.args, args) |arg, *out| {
                        const value = try self.runDatum(arg, depth + 1);
                        if (value.sql_null) break :blk .{};
                        out.* = value.value;
                    }
                    if (self.limits.decision_demand) |demand| demand.* = .{ .instruction = index, .function = desc.function, .args = args };
                    return error.DecisionNotEvaluated;
                }
                if (call.function == .current_setting) {
                    const view = self.program.settings orelse return error.SettingCatalogUnavailable;
                    const value = try view.resolveDependency(call.setting_identity orelse return error.InvalidSqlProgram);
                    break :blk Datum.json(.{ .string = switch (value) {
                        .string => |v| v,
                        .boolean => |v| if (v) "true" else "false",
                        .integer => |v| try std.fmt.allocPrint(self.alloc, "{d}", .{v}),
                    } });
                }
                switch (call.function) {
                    .@"$array" => {
                        const kind = instruction.type.element_type orelse return error.InvalidSqlProgram;
                        const cell_bytes = std.math.mul(usize, call.args.len, @sizeOf(arrays.Element)) catch return error.SqlProgramLimitExceeded;
                        try self.charge(cell_bytes + @sizeOf(arrays.Value) + @sizeOf(arrays.Dimension));
                        if (self.program.constant_arrays.get(index)) |prepared| break :blk Datum.typedArray(prepared);
                        const elements = try self.alloc.alloc(arrays.Element, call.args.len);
                        for (call.args, elements) |arg, *out| {
                            const value = try self.runDatum(arg, depth + 1);
                            if (value.array != null) return error.SqlTypeMismatch;
                            out.* = if (value.sql_null) .{} else arrays.Element.json(try self.convert(value.value, arrayScalarType(kind)));
                        }
                        const dimensions = try self.alloc.alloc(arrays.Dimension, 1);
                        dimensions[0] = .{ .length = std.math.cast(u32, elements.len) orelse return error.SqlProgramLimitExceeded };
                        const value = try self.alloc.create(arrays.Value);
                        value.* = try arrays.Value.init(kind, dimensions, elements, .{});
                        break :blk Datum.typedArray(value);
                    },
                    .@"$array_quantified" => {
                        const probe = try self.runDatum(call.args[0], depth + 1);
                        const input = try self.runDatum(call.args[1], depth + 1);
                        const op = try self.run(call.args[2], depth + 1);
                        const every = try self.run(call.args[3], depth + 1);
                        if (input.sql_null) break :blk .{};
                        const array = input.array orelse return error.SqlTypeMismatch;
                        if (probe.array != null or op != .integer or every != .bool) return error.SqlTypeMismatch;
                        const compare_op = std.enums.fromInt(arrays.Comparison, op.integer) orelse return error.InvalidSqlProgram;
                        const mixed_number = probe.value == .float and arrayScalarType(array.element_type) == .integer;
                        const element: arrays.Element = if (probe.sql_null) .{} else arrays.Element.json(try self.convert(probe.value, if (mixed_number) .number else arrayScalarType(array.element_type)));
                        var work: arrays.Budget = .{ .remaining = self.limits.steps -| self.steps };
                        const initial = work.remaining;
                        const accepted = if (mixed_number) try mixedNumberQuantified(array.*, element, compare_op, every.bool, &work) else try array.quantified(element, compare_op, if (every.bool) .all else .any, &work);
                        self.steps += initial - work.remaining;
                        break :blk if (accepted) |value| Datum.json(.{ .bool = value }) else .{};
                    },
                    .cardinality, .array_ndims, .array_length, .array_lower, .array_upper => {
                        const input = try self.runDatum(call.args[0], depth + 1);
                        if (call.function == .cardinality or call.function == .array_ndims) {
                            if (input.sql_null) break :blk .{};
                            const array = input.array orelse return error.SqlTypeMismatch;
                            if (call.function == .cardinality) break :blk Datum.json(.{ .integer = @intCast(array.cardinality()) });
                            break :blk if (array.dimensions.len == 0) .{} else Datum.json(.{ .integer = @intCast(array.dimensions.len) });
                        }
                        // Strict functions evaluate every argument, even when
                        // an earlier argument is NULL (unlike CASE/coalesce).
                        const axis = try self.runDatum(call.args[1], depth + 1);
                        if (input.sql_null or axis.sql_null) break :blk .{};
                        const array = input.array orelse return error.SqlTypeMismatch;
                        if (axis.array != null or axis.value != .integer) return error.SqlTypeMismatch;
                        const dimension = std.math.cast(i32, axis.value.integer) orelse return error.SqlNumericOutOfRange;
                        const answer: ?i64 = switch (call.function) {
                            .array_lower => if (array.lower(dimension)) |value| @as(i64, value) else null,
                            .array_upper => if (array.upper(dimension)) |value| @as(i64, value) else null,
                            .array_length => if (dimension > 0 and dimension <= array.dimensions.len) array.dimensions[@intCast(dimension - 1)].length else null,
                            else => unreachable,
                        };
                        break :blk if (answer) |value| Datum.json(.{ .integer = value }) else .{};
                    },
                    .jsonb_typeof => {
                        const datum = try self.runDatum(call.args[0], depth + 1);
                        if (datum.sql_null) break :blk .{};
                        break :blk Datum.json(.{ .string = switch (datum.value) {
                            .null => "null",
                            .bool => "boolean",
                            .integer, .float, .number_string => "number",
                            .string => "string",
                            .array => "array",
                            .object => "object",
                        } });
                    },
                    .concat_ws => {
                        const separator = try self.runDatum(call.args[0], depth + 1);
                        if (separator.sql_null) break :blk .{};
                        if (separator.value != .string) return error.SqlTypeMismatch;
                        var output: std.ArrayList(u8) = .empty;
                        errdefer output.deinit(self.alloc);
                        var emitted = false;
                        for (call.args[1..]) |arg| {
                            const datum = try self.runDatum(arg, depth + 1);
                            if (datum.sql_null) continue;
                            const string = if (self.program.instructions[arg].type.kind == .json) try self.jsonText(datum.value) else try self.formatText(datum.value);
                            if (emitted) {
                                try self.charge(separator.value.string.len);
                                try output.appendSlice(self.alloc, separator.value.string);
                            }
                            try self.charge(string.len);
                            try output.appendSlice(self.alloc, string);
                            emitted = true;
                        }
                        break :blk Datum.json(.{ .string = try output.toOwnedSlice(self.alloc) });
                    },
                    .to_jsonb => {
                        // JSON scalar null is a value, unlike a SQL NULL cell.
                        break :blk try self.runDatum(call.args[0], depth + 1);
                    },
                    .jsonb_build_object => {
                        var object: std.json.ObjectMap = .empty;
                        errdefer object.deinit(self.alloc);
                        var i: usize = 0;
                        while (i < call.args.len) : (i += 2) {
                            const key = try self.runDatum(call.args[i], depth + 1);
                            if (key.sql_null or key.value == .null or key.value == .object or key.value == .array) return error.InvalidSqlParameters;
                            const name = try self.formatText(key.value);
                            try self.charge(name.len + @sizeOf(Json) + @sizeOf([]const u8));
                            const value = try self.runDatum(call.args[i + 1], depth + 1);
                            try object.put(self.alloc, name, if (value.sql_null) .null else value.value);
                        }
                        break :blk Datum.json(.{ .object = object });
                    },
                    .jsonb_extract_path_text => {
                        var value = try self.runDatum(call.args[0], depth + 1);
                        if (value.sql_null) break :blk .{};
                        for (call.args[1..]) |arg| {
                            const key = try self.runDatum(arg, depth + 1);
                            if (key.sql_null) break :blk .{};
                            if (key.value != .string) return error.SqlTypeMismatch;
                            value.value = switch (value.value) {
                                .object => |object| object.get(key.value.string) orelse break :blk .{},
                                .array => |array| element: {
                                    const ordinal = std.fmt.parseInt(i64, key.value.string, 10) catch break :blk .{};
                                    const count: i64 = @intCast(array.items.len);
                                    const position = if (ordinal < 0) count +| ordinal else ordinal;
                                    if (position < 0 or position >= count) break :blk .{};
                                    break :element array.items[@intCast(position)];
                                },
                                else => break :blk .{},
                            };
                        }
                        if (value.value == .null) break :blk .{};
                        break :blk Datum.json(.{ .string = if (value.value == .string) value.value.string else try self.jsonText(value.value) });
                    },
                    .@"$single" => {
                        const count = try self.runDatum(call.args[1], depth + 1);
                        if (!count.sql_null and (count.value != .integer or count.value.integer > 1)) return error.SqlCardinalityViolation;
                        break :blk try self.runDatum(call.args[0], depth + 1);
                    },
                    .coalesce => {
                        for (call.args) |arg| {
                            const datum = try self.runDatum(arg, depth + 1);
                            if (!datum.sql_null) break :blk datum;
                        }
                        break :blk .{};
                    },
                    .nullif => {
                        const left = try self.runDatum(call.args[0], depth + 1);
                        const right = try self.runDatum(call.args[1], depth + 1);
                        break :blk if (left.sql_null or (!right.sql_null and (try compareDatums(left, right)) == .eq)) .{} else left;
                    },
                    .greatest, .least => {
                        var best: Datum = .{};
                        for (call.args) |arg| {
                            const datum = try self.runDatum(arg, depth + 1);
                            if (datum.sql_null) continue;
                            if (best.sql_null or (try compareDatums(datum, best)) == (if (call.function == .greatest) std.math.Order.gt else .lt)) best = datum;
                        }
                        break :blk best;
                    },
                    .concat => {
                        var output: std.ArrayList(u8) = .empty;
                        errdefer output.deinit(self.alloc);
                        for (call.args) |arg| {
                            const datum = try self.runDatum(arg, depth + 1);
                            if (datum.sql_null) continue;
                            const string = if (self.program.instructions[arg].type.kind == .json) try self.jsonText(datum.value) else try self.formatText(datum.value);
                            try self.charge(string.len);
                            try output.appendSlice(self.alloc, string);
                        }
                        break :blk Datum.json(.{ .string = try output.toOwnedSlice(self.alloc) });
                    },
                    else => break :blk Datum.fromJson(try self.invokeFunction(call.function, call.args, self.program.translations.get(index), depth + 1)),
                }
            },
            else => Datum.fromJson(try self.runLegacy(index, depth)),
        };
        if (!result.sql_null and instruction.type.kind == .number and result.value == .integer) result.value = try finite(@floatFromInt(result.value.integer));
        return result;
    }

    fn runLegacy(self: *Evaluator, index: u32, depth: usize) anyerror!Json {
        const instruction = self.program.instructions[index];
        return switch (instruction.operation) {
            .literal => |value| value,
            .column => |ordinal| if (ordinal < self.cells.len) self.cells[ordinal].value else error.InvalidSqlBackendResponse,
            .parameter => |slot| if (slot < self.parameters.len) try self.convert(self.parameters[slot], instruction.type.kind.?) else error.InvalidSqlParameters,
            .cast => |cast| try self.convert(try self.run(cast.operand, depth + 1), cast.type),
            .unary => |unary| blk: {
                const value = try self.run(unary.operand, depth + 1);
                if (unary.op == .is_null or unary.op == .is_not_null) break :blk .{ .bool = (value == .null) == (unary.op == .is_null) };
                if (unary.op == .is_true or unary.op == .is_not_true or unary.op == .is_false or unary.op == .is_not_false) {
                    const target = unary.op == .is_true or unary.op == .is_not_true;
                    const matches = value == .bool and value.bool == target;
                    break :blk .{ .bool = matches != (unary.op == .is_not_true or unary.op == .is_not_false) };
                }
                if (value == .null) break :blk .null;
                break :blk switch (unary.op) {
                    .not => if (value == .bool) .{ .bool = !value.bool } else error.SqlTypeMismatch,
                    .positive => if (value == .integer or value == .float) value else error.SqlTypeMismatch,
                    .negative => switch (value) {
                        .integer => |v| .{ .integer = std.math.negate(v) catch return error.SqlNumericOutOfRange },
                        .float => |v| finite(-v),
                        else => error.SqlTypeMismatch,
                    },
                    else => unreachable,
                };
            },
            .binary => |binary| blk: {
                const left = try self.run(binary.left, depth + 1);
                if (binary.op == .@"and" and left == .bool and !left.bool) break :blk left;
                if (binary.op == .@"or" and left == .bool and left.bool) break :blk left;
                const right = try self.run(binary.right, depth + 1);
                if (binary.op == .@"and" or binary.op == .@"or") {
                    if ((left != .null and left != .bool) or (right != .null and right != .bool)) return error.SqlTypeMismatch;
                    const decisive = binary.op == .@"or";
                    if ((left == .bool and left.bool == decisive) or (right == .bool and right.bool == decisive)) break :blk .{ .bool = decisive };
                    break :blk if (left == .null or right == .null) .null else .{ .bool = !decisive };
                }
                if (binary.op == .is_distinct or binary.op == .is_not_distinct) {
                    const equal = if (left == .null or right == .null) left == .null and right == .null else (try compare(left, right)) == .eq;
                    break :blk .{ .bool = equal == (binary.op == .is_not_distinct) };
                }
                if (left == .null or right == .null) break :blk .null;
                break :blk switch (binary.op) {
                    .add, .subtract, .multiply, .divide, .modulo => try arithmetic(binary.op, left, right),
                    .concat => try self.concat(&.{ left, right }, false),
                    .like, .ilike => .{ .bool = try self.like(left, right, binary.op == .ilike) },
                    .eq, .neq, .lt, .lte, .gt, .gte => comparison(binary.op, try compare(left, right)),
                    else => unreachable,
                };
            },
            .call => |call| try self.invokeFunction(call.function, call.args, self.program.translations.get(index), depth + 1),
            .case_when => |case| blk: {
                for (case.branches) |branch| {
                    const condition = try self.run(branch.condition, depth + 1);
                    if (condition == .bool and condition.bool) break :blk try self.run(branch.value, depth + 1);
                    if (condition != .bool and condition != .null) return error.SqlTypeMismatch;
                }
                break :blk if (case.otherwise) |other| try self.run(other, depth + 1) else .null;
            },
            .in_list => unreachable,
        };
    }

    fn jsonText(self: *Evaluator, value: Json) ![]const u8 {
        _ = try self.validateJson(value, 0);
        var writer: std.Io.Writer.Allocating = .init(self.alloc);
        errdefer writer.deinit();
        // The execution allocator also carries the statement-wide hard cap.
        // Count before producing output so a large JSON subtree cannot evade
        // this scalar's independent output bound.
        var counter: std.Io.Writer.Discarding = .init(&.{});
        try std.json.Stringify.value(value, .{}, &counter.writer);
        try self.charge(std.math.cast(usize, counter.count) orelse return error.SqlProgramLimitExceeded);
        std.json.Stringify.value(value, .{}, &writer.writer) catch return error.OutOfMemory;
        return writer.toOwnedSlice();
    }

    fn validateJson(self: *Evaluator, value: Json, depth: usize) error{ SqlProgramLimitExceeded, SqlTypeMismatch }!usize {
        if (depth >= self.limits.depth or self.steps >= self.limits.steps) return error.SqlProgramLimitExceeded;
        self.steps += 1;
        var size: usize = 8;
        switch (value) {
            .string => |string| {
                if (!std.unicode.utf8ValidateSlice(string)) return error.SqlTypeMismatch;
                size = string.len;
            },
            .number_string => |string| size = string.len,
            .array => |array| for (array.items) |item| {
                size +|= try self.validateJson(item, depth + 1);
            },
            .object => |object| for (object.keys(), object.values()) |key, item| {
                size +|= key.len +| try self.validateJson(item, depth + 1);
            },
            else => {},
        }
        if (size > self.limits.output_bytes) return error.SqlProgramLimitExceeded;
        return size;
    }

    fn convert(self: *Evaluator, value: Json, kind: ast.ColumnType) anyerror!Json {
        if (value == .null) return .null;
        return switch (kind) {
            .array => error.SqlTypeMismatch,
            .integer => switch (value) {
                .integer => value,
                .float => |v| blk: {
                    const rounded = @round(v);
                    if (!std.math.isFinite(rounded) or rounded < -9223372036854775808.0 or rounded >= 9223372036854775808.0) return error.SqlNumericOutOfRange;
                    break :blk .{ .integer = @intFromFloat(rounded) };
                },
                .string, .number_string => |v| .{ .integer = std.fmt.parseInt(i64, v, 10) catch return error.SqlTypeMismatch },
                .bool => |v| .{ .integer = @intFromBool(v) },
                else => error.SqlTypeMismatch,
            },
            .number => switch (value) {
                .integer => |v| finite(@floatFromInt(v)),
                .float => |v| finite(v),
                .string, .number_string => |v| finite(std.fmt.parseFloat(f64, v) catch return error.SqlTypeMismatch),
                else => error.SqlTypeMismatch,
            },
            .boolean => switch (value) {
                .bool => value,
                .integer => |v| .{ .bool = v != 0 },
                .string => |v| if (std.ascii.eqlIgnoreCase(v, "true") or std.ascii.eqlIgnoreCase(v, "t") or std.ascii.eqlIgnoreCase(v, "yes") or std.mem.eql(u8, v, "1")) .{ .bool = true } else if (std.ascii.eqlIgnoreCase(v, "false") or std.ascii.eqlIgnoreCase(v, "f") or std.ascii.eqlIgnoreCase(v, "no") or std.mem.eql(u8, v, "0")) .{ .bool = false } else error.SqlTypeMismatch,
                else => error.SqlTypeMismatch,
            },
            .string => .{ .string = try self.formatText(value) },
            .uuid => if (value == .string) .{ .string = @import("../common/uuid.zig").canonicalAlloc(self.alloc, value.string) catch |err| switch (err) {
                error.InvalidUuid => return error.SqlTypeMismatch,
                else => return err,
            } } else error.SqlTypeMismatch,
            .datetime => blk: {
                if (value != .string) return error.SqlTypeMismatch;
                const ns = datetime.parseDateTimeToNs(value.string) orelse return error.InvalidSqlDateTime;
                try self.charge(30);
                break :blk .{ .string = try datetime.formatDateTimeNsAlloc(self.alloc, ns) };
            },
            .json => if (value == .string) blk: {
                try self.charge(value.string.len);
                break :blk try std.json.parseFromSliceLeaky(Json, self.alloc, value.string, .{ .parse_numbers = false, .allocate = .alloc_always });
            } else value,
        };
    }

    fn formatText(self: *Evaluator, value: Json) ![]const u8 {
        if (value == .string) return value.string;
        if (value == .number_string) return value.number_string;
        if (value == .bool) return if (value.bool) "true" else "false";
        if (value == .null) return "";
        try self.charge(64);
        return switch (value) {
            .integer => |v| try std.fmt.allocPrint(self.alloc, "{d}", .{v}),
            .float => |v| try std.fmt.allocPrint(self.alloc, "{d}", .{v}),
            else => error.SqlTypeMismatch,
        };
    }

    fn concat(self: *Evaluator, values: []const Json, ignore_null: bool) !Json {
        var result: std.ArrayList(u8) = .empty;
        errdefer result.deinit(self.alloc);
        for (values) |value| {
            if (value == .null) {
                if (ignore_null) continue;
                return .null;
            }
            const text_value = try self.formatText(value);
            try self.charge(text_value.len);
            try result.appendSlice(self.alloc, text_value);
        }
        return .{ .string = try result.toOwnedSlice(self.alloc) };
    }

    fn invokeFunction(self: *Evaluator, function: Function, args: []const u32, translation: ?*const TextTranslation, depth: usize) anyerror!Json {
        if (function == .@"$pattern_quantified") {
            const operand = try self.run(args[0], depth + 1);
            const set_datum = try self.runDatum(args[1], depth + 1);
            const set = set_datum.value;
            const all = try self.run(args[2], depth + 1);
            const insensitive = try self.run(args[3], depth + 1);
            const negated = try self.run(args[4], depth + 1);
            if (all != .bool or insensitive != .bool or negated != .bool) return error.SqlTypeMismatch;
            if (set != .null and set != .array) return error.SqlTypeMismatch;
            const patterns: []const Json = if (set == .null) &.{} else set.array.items;
            if (set_datum.patterns) |source| {
                if (source.count == 0) return .{ .bool = all.bool };
                if (operand == .null) return .null;
                var scratch = std.heap.ArenaAllocator.init(self.alloc);
                defer scratch.deinit();
                var offset: u64 = 0;
                var saw_null = false;
                while (true) {
                    _ = scratch.reset(.free_all);
                    const pattern = (try source.next(source.ptr, scratch.allocator(), &offset)) orelse break;
                    if (self.steps >= self.limits.steps) return error.SqlProgramLimitExceeded;
                    self.steps += 1;
                    if (pattern.sql_null) {
                        saw_null = true;
                        continue;
                    }
                    const matches = (try self.like(operand, pattern.value, insensitive.bool)) != negated.bool;
                    if (matches != all.bool) return .{ .bool = matches };
                }
                return if (saw_null) .null else .{ .bool = all.bool };
            }
            if (patterns.len == 0) return .{ .bool = all.bool };
            if (operand == .null) return .null;
            var saw_null = false;
            for (patterns) |pattern| {
                if (self.steps >= self.limits.steps) return error.SqlProgramLimitExceeded;
                self.steps += 1;
                if (pattern == .null) {
                    saw_null = true;
                    continue;
                }
                const matches = (try self.like(operand, pattern, insensitive.bool)) != negated.bool;
                if (matches != all.bool) return .{ .bool = matches };
            }
            return if (saw_null) .null else .{ .bool = all.bool };
        }
        if (function == .@"$single") {
            const count = try self.run(args[1], depth + 1);
            if (count != .null and (count != .integer or count.integer > 1)) return error.SqlCardinalityViolation;
            return self.run(args[0], depth + 1);
        }
        if (function == .coalesce) {
            for (args) |arg| {
                const value = try self.run(arg, depth);
                if (value != .null) return value;
            }
            return .null;
        }
        if (function == .greatest or function == .least) {
            var best: Json = .null;
            for (args) |arg| {
                const value = try self.run(arg, depth);
                if (value == .null) continue;
                if (best == .null or (try compare(value, best)) == (if (function == .greatest) std.math.Order.gt else .lt)) best = value;
            }
            return best;
        }
        if (function == .concat) {
            var result: std.ArrayList(u8) = .empty;
            errdefer result.deinit(self.alloc);
            for (args) |arg| {
                const value = try self.run(arg, depth);
                if (value == .null) continue;
                const text_value = try self.formatText(value);
                try self.charge(text_value.len);
                try result.appendSlice(self.alloc, text_value);
            }
            return .{ .string = try result.toOwnedSlice(self.alloc) };
        }
        var values: [4]Json = @splat(.null);
        for (args, 0..) |arg, i| values[i] = try self.run(arg, depth);
        if (function == .nullif) return if (values[0] == .null or (values[1] != .null and (try compare(values[0], values[1])) == .eq)) .null else values[0];
        for (values[0..args.len]) |value| if (value == .null) return .null;
        const first = values[0];
        switch (function) {
            .chr => {
                if (first != .integer) return error.SqlTypeMismatch;
                if (first.integer < 0) return error.InvalidSqlParameters;
                const codepoint = std.math.cast(u21, first.integer) orelse return error.InvalidSqlCharacterCode;
                if (codepoint == 0) return error.InvalidSqlCharacterCode;
                var buffer: [4]u8 = undefined;
                const size = std.unicode.utf8Encode(codepoint, &buffer) catch return error.InvalidSqlCharacterCode;
                try self.charge(size);
                return .{ .string = try self.alloc.dupe(u8, buffer[0..size]) };
            },
            .date_part, .date_trunc => {
                if (first != .string or values[1] != .string) return error.SqlTypeMismatch;
                const field = datetime.unit(first.string) orelse return error.InvalidSqlParameters;
                const ns = datetime.parseDateTimeToNs(values[1].string) orelse return error.InvalidSqlDateTime;
                if (function == .date_part) return .{ .float = datetime.part(ns, field) };
                const truncated = datetime.truncate(ns, field) orelse return error.InvalidSqlDateTime;
                try self.charge(30);
                return .{ .string = try datetime.formatDateTimeNsAlloc(self.alloc, truncated) };
            },
            .to_timestamp => {
                const seconds = try asFloat(first);
                const nanos = @round(seconds * std.time.ns_per_s);
                if (!std.math.isFinite(nanos) or nanos < 0 or nanos >= 18446744073709551616.0) return error.InvalidSqlDateTime;
                try self.charge(30);
                return .{ .string = try datetime.formatDateTimeNsAlloc(self.alloc, @intFromFloat(nanos)) };
            },
            .abs => return switch (first) {
                .integer => |v| .{ .integer = if (v < 0) std.math.negate(v) catch return error.SqlNumericOutOfRange else v },
                .float => |v| finite(@abs(v)),
                else => error.SqlTypeMismatch,
            },
            .ceil, .floor, .round, .trunc => return if (first == .integer) first else if (first == .float) finite(switch (function) {
                .ceil => @ceil(first.float),
                .floor => @floor(first.float),
                .trunc => @trunc(first.float),
                else => @round(first.float),
            }) else error.SqlTypeMismatch,
            .sign => return switch (first) {
                .integer => |value| .{ .integer = if (value < 0) -1 else if (value > 0) 1 else 0 },
                .float => |value| if (!std.math.isFinite(value)) error.SqlNumericOutOfRange else finite(if (value < 0) -1 else if (value > 0) 1 else 0),
                else => error.SqlTypeMismatch,
            },
            .sqrt => {
                const v = try asFloat(first);
                if (v < 0) return error.SqlNumericOutOfRange;
                return finite(@sqrt(v));
            },
            .power => return finite(std.math.pow(f64, try asFloat(first), try asFloat(values[1]))),
            .mod => return arithmetic(.modulo, first, values[1]),
            else => {},
        }
        if (first != .string) return error.SqlTypeMismatch;
        const text_value = first.string;
        return switch (function) {
            .left, .right => blk: {
                if (values[1] != .integer) return error.SqlTypeMismatch;
                try self.textWork(text_value.len *| 2);
                const count: i128 = std.unicode.utf8CountCodepoints(text_value) catch return error.SqlTypeMismatch;
                const requested: i128 = values[1].integer;
                // PostgreSQL 18 right(text, INT_MIN) returns the full input;
                // preserve the observed contract without signed negation.
                const length: usize = if (function == .right and requested == std.math.minInt(i32)) @intCast(count) else @intCast(@min(count, @max(0, if (requested < 0) count + requested else requested)));
                const begin: usize = if (function == .left) 0 else @as(usize, @intCast(count)) - length;
                break :blk .{ .string = try textSlice(text_value, begin, begin + length) };
            },
            .ascii => blk: {
                var iterator = (std.unicode.Utf8View.init(text_value) catch return error.SqlTypeMismatch).iterator();
                break :blk .{ .integer = iterator.nextCodepoint() orelse 0 };
            },
            .split_part => blk: {
                if (values[1] != .string or values[2] != .integer) return error.SqlTypeMismatch;
                const field = values[2].integer;
                if (field == 0) return error.InvalidSqlParameters;
                const delimiter = values[1].string;
                if (delimiter.len == 0) break :blk .{ .string = if (field == 1 or field == -1) text_value else "" };
                try self.textWork(text_value.len *| @max(delimiter.len, 1) *| 2);
                var chunks = std.mem.splitSequence(u8, text_value, delimiter);
                var target: i128 = field;
                if (field < 0) {
                    var count: i128 = 0;
                    while (chunks.next() != null) count += 1;
                    target = count + field + 1;
                    chunks.reset();
                }
                if (target <= 0) break :blk .{ .string = "" };
                var index: i128 = 1;
                while (chunks.next()) |chunk| : (index += 1) if (index == target) break :blk .{ .string = chunk };
                break :blk .{ .string = "" };
            },
            .translate => try self.translateText(text_value, values[1], values[2], translation),
            .overlay => blk: {
                if (values[1] != .string or values[2] != .integer or (args.len == 4 and values[3] != .integer)) return error.SqlTypeMismatch;
                const start = values[2].integer;
                if (start <= 0) return error.SqlSubstringError;
                const replacement = values[1].string;
                try self.textWork(text_value.len *| 2 +| replacement.len);
                const count: i128 = std.unicode.utf8CountCodepoints(text_value) catch return error.SqlTypeMismatch;
                const replaced: i128 = if (args.len == 4) values[3].integer else std.unicode.utf8CountCodepoints(replacement) catch return error.SqlTypeMismatch;
                // PostgreSQL permits negative counts: prefix and suffix may
                // overlap. Use wide arithmetic even at the signed extremes.
                const end = @min(count, @max(0, @as(i128, start) - 1 + replaced));
                const prefix = try textSlice(text_value, 0, @intCast(@min(count, start - 1)));
                const suffix = try textSlice(text_value, @intCast(end), @intCast(count));
                const size = std.math.add(usize, prefix.len, replacement.len) catch return error.SqlProgramLimitExceeded;
                const total = std.math.add(usize, size, suffix.len) catch return error.SqlProgramLimitExceeded;
                try self.charge(total);
                const output = try self.alloc.alloc(u8, total);
                @memcpy(output[0..prefix.len], prefix);
                @memcpy(output[prefix.len..size], replacement);
                @memcpy(output[size..], suffix);
                break :blk .{ .string = output };
            },
            .ai_decide, .ai_choice, .ai_score, .ai_probability => error.DecisionNotEvaluated,
            .length => .{ .integer = @intCast(std.unicode.utf8CountCodepoints(text_value) catch return error.SqlTypeMismatch) },
            .octet_length => .{ .integer = @intCast(text_value.len) },
            .bit_length => .{ .integer = std.math.mul(i64, std.math.cast(i64, text_value.len) orelse return error.SqlNumericOutOfRange, 8) catch return error.SqlNumericOutOfRange },
            .repeat => blk: {
                if (values[1] != .integer) return error.SqlTypeMismatch;
                if (values[1].integer <= 0 or text_value.len == 0) break :blk .{ .string = "" };
                const count = std.math.cast(usize, values[1].integer) orelse return error.SqlProgramLimitExceeded;
                const size = std.math.mul(usize, text_value.len, count) catch return error.SqlProgramLimitExceeded;
                try self.charge(size);
                const output = try self.alloc.alloc(u8, size);
                for (0..count) |index| @memcpy(output[index * text_value.len ..][0..text_value.len], text_value);
                break :blk .{ .string = output };
            },
            .reverse => blk: {
                var iterator = (std.unicode.Utf8View.init(text_value) catch return error.SqlTypeMismatch).iterator();
                try self.charge(text_value.len);
                const output = try self.alloc.alloc(u8, text_value.len);
                var offset = text_value.len;
                while (iterator.nextCodepointSlice()) |character| {
                    offset -= character.len;
                    @memcpy(output[offset..][0..character.len], character);
                }
                break :blk .{ .string = output };
            },
            .lpad, .rpad => try self.padText(function == .lpad, text_value, values[1], if (args.len == 3) values[2] else .{ .string = " " }),
            .strpos => blk: {
                if (values[1] != .string) return error.SqlTypeMismatch;
                const position = std.mem.indexOf(u8, text_value, values[1].string) orelse break :blk .{ .integer = 0 };
                break :blk .{ .integer = @intCast((std.unicode.utf8CountCodepoints(text_value[0..position]) catch return error.SqlTypeMismatch) + 1) };
            },
            .lower, .upper => blk: {
                try self.charge(text_value.len);
                const output = try self.alloc.dupe(u8, text_value);
                if (function == .lower) _ = std.ascii.lowerString(output, output) else _ = std.ascii.upperString(output, output);
                break :blk .{ .string = output };
            },
            .starts_with => if (values[1] == .string) .{ .bool = std.mem.startsWith(u8, text_value, values[1].string) } else error.SqlTypeMismatch,
            .trim, .ltrim, .rtrim => blk: {
                const trim_chars = if (args.len == 1) " " else if (values[1] == .string) values[1].string else return error.SqlTypeMismatch;
                var characters: std.AutoHashMapUnmanaged(u21, void) = .empty;
                defer characters.deinit(self.alloc);
                var character_iterator = (std.unicode.Utf8View.init(trim_chars) catch return error.SqlTypeMismatch).iterator();
                while (character_iterator.nextCodepoint()) |codepoint| {
                    if (self.steps >= self.limits.steps) return error.SqlProgramLimitExceeded;
                    self.steps += 1;
                    try characters.put(self.alloc, codepoint, {});
                }
                var iterator = (std.unicode.Utf8View.init(text_value) catch return error.SqlTypeMismatch).iterator();
                var begin: usize = 0;
                var end: usize = 0;
                var leading = true;
                while (iterator.nextCodepointSlice()) |bytes| {
                    if (self.steps >= self.limits.steps) return error.SqlProgramLimitExceeded;
                    self.steps += 1;
                    const trimmed = characters.contains(std.unicode.utf8Decode(bytes) catch return error.SqlTypeMismatch);
                    const after = @intFromPtr(bytes.ptr) - @intFromPtr(text_value.ptr) + bytes.len;
                    if (leading and trimmed) begin = after else leading = false;
                    if (!trimmed) end = after;
                }
                const start = if (function == .rtrim) 0 else begin;
                const stop = if (function == .ltrim) text_value.len else @max(start, end);
                break :blk .{ .string = text_value[start..stop] };
            },
            .substring => blk: {
                if (values[1] != .integer or (args.len == 3 and values[2] != .integer)) return error.SqlTypeMismatch;
                const start = values[1].integer;
                const length = if (args.len == 3) values[2].integer else std.math.maxInt(i64);
                if (length < 0) return error.SqlSubstringError;
                const end = if (args.len == 3) start +| length else std.math.maxInt(i64);
                var iterator = (std.unicode.Utf8View.init(text_value) catch return error.SqlTypeMismatch).iterator();
                var position: i64 = 1;
                var begin_byte: usize = text_value.len;
                var end_byte: usize = text_value.len;
                while (iterator.nextCodepointSlice()) |bytes| : (position += 1) {
                    const offset = @intFromPtr(bytes.ptr) - @intFromPtr(text_value.ptr);
                    if (position >= @max(start, 1) and begin_byte == text_value.len) begin_byte = offset;
                    if (position >= end) {
                        end_byte = offset;
                        break;
                    }
                }
                break :blk .{ .string = text_value[@min(begin_byte, end_byte)..end_byte] };
            },
            .replace => blk: {
                if (values[1] != .string or values[2] != .string) return error.SqlTypeMismatch;
                if (values[1].string.len == 0) break :blk first;
                var output: std.ArrayList(u8) = .empty;
                errdefer output.deinit(self.alloc);
                var chunks = std.mem.splitSequence(u8, text_value, values[1].string);
                var initial = true;
                while (chunks.next()) |chunk| {
                    if (!initial) {
                        try self.charge(values[2].string.len);
                        try output.appendSlice(self.alloc, values[2].string);
                    }
                    initial = false;
                    try self.charge(chunk.len);
                    try output.appendSlice(self.alloc, chunk);
                }
                break :blk .{ .string = try output.toOwnedSlice(self.alloc) };
            },
            else => unreachable,
        };
    }

    fn textWork(self: *Evaluator, bytes: usize) !void {
        if (bytes > self.limits.pattern_steps -| self.pattern_steps) return error.SqlProgramLimitExceeded;
        self.pattern_steps += bytes;
    }

    fn translateText(self: *Evaluator, text: []const u8, from: Json, to: Json, prepared: ?*const TextTranslation) !Json {
        if (from != .string or to != .string) return error.SqlTypeMismatch;
        try self.textWork(text.len *| 2 +| (if (prepared == null) from.string.len +| to.string.len else 0));
        var mapping: std.AutoHashMapUnmanaged(u21, []const u8) = .empty;
        defer mapping.deinit(self.alloc);
        var source = (std.unicode.Utf8View.init(from.string) catch return error.SqlTypeMismatch).iterator();
        var target = (std.unicode.Utf8View.init(to.string) catch return error.SqlTypeMismatch).iterator();
        while (prepared == null) {
            const codepoint = source.nextCodepoint() orelse break;
            const replacement = target.nextCodepointSlice() orelse "";
            if (!mapping.contains(codepoint)) {
                try self.charge(@sizeOf(u21) + @sizeOf([]const u8) + 16);
                try mapping.put(self.alloc, codepoint, replacement);
            }
        }
        const lookup = prepared orelse &mapping;
        var input = (std.unicode.Utf8View.init(text) catch return error.SqlTypeMismatch).iterator();
        var size: usize = 0;
        while (input.nextCodepointSlice()) |bytes| {
            const replacement = lookup.get(std.unicode.utf8Decode(bytes) catch return error.SqlTypeMismatch) orelse bytes;
            size = std.math.add(usize, size, replacement.len) catch return error.SqlProgramLimitExceeded;
        }
        try self.charge(size);
        const output = try self.alloc.alloc(u8, size);
        input.i = 0;
        var offset: usize = 0;
        while (input.nextCodepointSlice()) |bytes| {
            const replacement = lookup.get(std.unicode.utf8Decode(bytes) catch return error.SqlTypeMismatch) orelse bytes;
            @memcpy(output[offset..][0..replacement.len], replacement);
            offset += replacement.len;
        }
        return .{ .string = output };
    }

    fn padText(self: *Evaluator, left: bool, text: []const u8, length: Json, filling: Json) !Json {
        if (length != .integer or filling != .string) return error.SqlTypeMismatch;
        if (length.integer <= 0) return .{ .string = "" };
        const wanted = std.math.cast(usize, length.integer) orelse return error.SqlProgramLimitExceeded;
        const count = std.unicode.utf8CountCodepoints(text) catch return error.SqlTypeMismatch;
        if (wanted <= count) {
            var iterator = (std.unicode.Utf8View.init(text) catch return error.SqlTypeMismatch).iterator();
            for (0..wanted) |_| _ = iterator.nextCodepointSlice();
            return .{ .string = text[0..iterator.i] };
        }
        const fill = filling.string;
        const fill_count = std.unicode.utf8CountCodepoints(fill) catch return error.SqlTypeMismatch;
        if (fill_count == 0) return .{ .string = text };
        const padding = wanted - count;
        const cycles = padding / fill_count;
        var iterator = (std.unicode.Utf8View.init(fill) catch return error.SqlTypeMismatch).iterator();
        for (0..padding % fill_count) |_| _ = iterator.nextCodepointSlice();
        const partial = fill[0..iterator.i];
        const full_bytes = std.math.mul(usize, cycles, fill.len) catch return error.SqlProgramLimitExceeded;
        const pad_bytes = std.math.add(usize, full_bytes, partial.len) catch return error.SqlProgramLimitExceeded;
        const size = std.math.add(usize, pad_bytes, text.len) catch return error.SqlProgramLimitExceeded;
        try self.charge(size);
        const output = try self.alloc.alloc(u8, size);
        const start = if (left) 0 else text.len;
        for (0..cycles) |index| @memcpy(output[start + index * fill.len ..][0..fill.len], fill);
        @memcpy(output[start + full_bytes ..][0..partial.len], partial);
        @memcpy(output[if (left) pad_bytes else 0..][0..text.len], text);
        return .{ .string = output };
    }

    fn like(self: *Evaluator, text_value: Json, pattern: Json, insensitive: bool) !bool {
        if (text_value != .string or pattern != .string) return error.SqlTypeMismatch;
        const text = text_value.string;
        const glob = pattern.string;
        var i: usize = 0;
        var j: usize = 0;
        var star: ?usize = null;
        var restart: usize = 0;
        while (i < text.len) {
            if (self.pattern_steps >= self.limits.pattern_steps) return error.SqlProgramLimitExceeded;
            self.pattern_steps += 1;
            if (j < glob.len and glob[j] == '%') {
                if (j + 1 == glob.len) return true;
                star = j + 1;
                j += 1;
                restart = i;
                continue;
            }
            if (j < glob.len and glob[j] == '_') {
                i += std.unicode.utf8ByteSequenceLength(text[i]) catch return error.SqlTypeMismatch;
                j += 1;
                continue;
            }
            var escaped = false;
            if (j < glob.len and glob[j] == '\\') {
                j += 1;
                escaped = true;
                if (j == glob.len) return error.InvalidSqlParameters;
            }
            if (j < glob.len and (if (insensitive) std.ascii.toLower(text[i]) == std.ascii.toLower(glob[j]) else text[i] == glob[j])) {
                i += 1;
                j += 1;
                continue;
            }
            if (escaped) j -= 1;
            if (star) |next| {
                restart += std.unicode.utf8ByteSequenceLength(text[restart]) catch return error.SqlTypeMismatch;
                i = restart;
                j = next;
            } else return false;
        }
        while (j < glob.len and glob[j] == '%') : (j += 1) {}
        return j == glob.len;
    }
};

fn finite(value: f64) !Json {
    if (!std.math.isFinite(value)) return error.SqlNumericOutOfRange;
    return .{ .float = value };
}
fn asFloat(value: Json) !f64 {
    return switch (value) {
        .integer => |v| @floatFromInt(v),
        .float => |v| if (std.math.isFinite(v)) v else error.SqlNumericOutOfRange,
        else => error.SqlTypeMismatch,
    };
}
pub fn arithmetic(op: ast.Scalar.Binary, left: Json, right: Json) !Json {
    if (left == .integer and right == .integer) {
        const a = left.integer;
        const b = right.integer;
        return .{ .integer = switch (op) {
            .add => std.math.add(i64, a, b) catch return error.SqlNumericOutOfRange,
            .subtract => std.math.sub(i64, a, b) catch return error.SqlNumericOutOfRange,
            .multiply => std.math.mul(i64, a, b) catch return error.SqlNumericOutOfRange,
            .divide => if (b == 0) return error.SqlDivisionByZero else if (a == std.math.minInt(i64) and b == -1) return error.SqlNumericOutOfRange else @divTrunc(a, b),
            .modulo => if (b == 0) return error.SqlDivisionByZero else if (a == std.math.minInt(i64) and b == -1) 0 else @rem(a, b),
            else => unreachable,
        } };
    }
    const a = try asFloat(left);
    const b = try asFloat(right);
    return finite(switch (op) {
        .add => a + b,
        .subtract => a - b,
        .multiply => a * b,
        .divide => if (b == 0) return error.SqlDivisionByZero else a / b,
        .modulo => if (b == 0) return error.SqlDivisionByZero else @rem(a, b),
        else => unreachable,
    });
}
fn compareIntFloat(integer: i64, number: f64) !std.math.Order {
    if (!std.math.isFinite(number)) return error.SqlNumericOutOfRange;
    if (number >= 9223372036854775808.0) return .lt;
    if (number < -9223372036854775808.0) return .gt;
    const truncated: i64 = @intFromFloat(number);
    const order = std.math.order(integer, truncated);
    if (order != .eq) return order;
    return std.math.order(@as(f64, @floatFromInt(truncated)), number);
}
pub fn compare(left: Json, right: Json) !std.math.Order {
    if (left == .array or left == .object or left == .number_string or right == .array or right == .object or right == .number_string) {
        var budget: json_order.Budget = .{};
        return json_order.compare(left, right, &budget, 0);
    }
    if (left == .null or right == .null) return if (left == .null and right == .null) .eq else if (left == .null) .lt else .gt;
    if (left == .integer and right == .integer) return std.math.order(left.integer, right.integer);
    if (left == .integer and right == .float) return compareIntFloat(left.integer, right.float);
    if (left == .float and right == .integer) return (try compareIntFloat(right.integer, left.float)).invert();
    if (left == .float and right == .float) {
        _ = try asFloat(left);
        _ = try asFloat(right);
        return std.math.order(left.float, right.float);
    }
    if (left == .string and right == .string) return std.mem.order(u8, left.string, right.string);
    if (left == .bool and right == .bool) return std.math.order(@intFromBool(left.bool), @intFromBool(right.bool));
    var budget: json_order.Budget = .{};
    return json_order.compare(left, right, &budget, 0);
}

pub fn semanticHash(value: Json) !u64 {
    var budget: json_order.Budget = .{};
    return json_order.hash(value, &budget, 0);
}

/// Physical operators compare the complete typed cell, never its JSON-only
/// payload. NULL ordering here matches array element ordering; row operators
/// apply their explicit NULLS FIRST/LAST before calling this helper.
pub fn compareDatums(left: Datum, right: Datum) anyerror!std.math.Order {
    if (left.sql_null or right.sql_null) return if (left.sql_null == right.sql_null) .eq else if (left.sql_null) .gt else .lt;
    if (left.array) |array| {
        var work: json_order.Budget = .{};
        return array.compare((right.array orelse return error.SqlTypeMismatch).*, &work);
    }
    if (right.array != null) return error.SqlTypeMismatch;
    return compare(left.value, right.value);
}

pub fn semanticHashDatum(value: Datum) anyerror!u64 {
    if (value.sql_null) return 0;
    if (value.array) |array| {
        var work: json_order.Budget = .{};
        return array.semanticHash(&work);
    }
    return semanticHash(value.value);
}
/// Borrow a UTF-8 character interval; callers account for its linear work.
fn textSlice(text: []const u8, begin: usize, end: usize) ![]const u8 {
    var iterator = (std.unicode.Utf8View.init(text) catch return error.SqlTypeMismatch).iterator();
    for (0..begin) |_| _ = iterator.nextCodepointSlice();
    const offset = iterator.i;
    for (begin..end) |_| _ = iterator.nextCodepointSlice();
    return text[offset..iterator.i];
}

pub fn comparison(op: ast.Scalar.Binary, order: std.math.Order) Json {
    return .{ .bool = switch (op) {
        .eq => order == .eq,
        .neq => order != .eq,
        .lt => order == .lt,
        .lte => order != .gt,
        .gt => order == .gt,
        .gte => order != .lt,
        else => unreachable,
    } };
}

test "SQL PostgreSQL text reference preserves Unicode slicing replacement and errors" {
    const fixture = try std.json.parseFromSlice(Json, std.testing.allocator, @embedFile("fixtures/sql_text_reference.json"), .{});
    defer fixture.deinit();
    for (fixture.value.object.get("entries").?.array.items) |case| {
        const sql = case.object.get("sql").?.string;
        var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var program = try bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{});
        defer program.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        if (case.object.get("error")) |code| {
            const expected_error = if (std.mem.eql(u8, code.string, "22011")) error.SqlSubstringError else if (std.mem.eql(u8, code.string, "54000")) error.InvalidSqlCharacterCode else error.InvalidSqlParameters;
            try std.testing.expectError(expected_error, program.evaluate(arena.allocator(), &.{}, &.{}, .{}));
            try std.testing.expectEqualStrings(code.string, @import("errors.zig").describe(expected_error).code);
        } else {
            const expected = case.object.get("value").?;
            const actual = try program.evaluate(arena.allocator(), &.{}, &.{}, .{});
            try std.testing.expectEqual(expected == .null, actual.sql_null);
            try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(arena.allocator(), expected, .{}), try std.json.Stringify.valueAlloc(arena.allocator(), actual.value, .{}));
        }
    }
}

test "SQL UTF8 text transforms bound work allocation and parameter types" {
    const Harness = struct {
        fn prepared(alloc: Allocator) !void {
            var program = blk: {
                var compiled = try @import("compiler.zig").compileScalar(alloc, "translate($1,'aé🍎','xê')", .{});
                defer compiled.deinit();
                break :blk try bind(alloc, compiled.expression, &.{}, &.{}, .{});
            };
            defer program.deinit();
            var buffer: [6]u8 = undefined;
            var output_memory = std.heap.FixedBufferAllocator.init(&buffer);
            const output = try program.evaluate(output_memory.allocator(), &.{}, &.{.{ .string = "aé🍎aé🍎" }}, .{});
            try std.testing.expectEqualStrings("xêxê", output.value.string);
        }

        fn run(alloc: Allocator) !void {
            var compiled = try @import("compiler.zig").compileScalar(alloc, "overlay(translate($1,$2,$3) placing chr(127822) from 2 for 1)", .{});
            defer compiled.deinit();
            var program = try bind(alloc, compiled.expression, &.{}, &.{}, .{});
            defer program.deinit();
            try std.testing.expectEqualSlices(?ast.ColumnType, &.{ .string, .string, .string }, program.parameter_types);
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const parameters: []const Json = &.{ .{ .string = "abcé" }, .{ .string = "acé" }, .{ .string = "xê" } };
            const output = try program.evaluate(arena.allocator(), &.{}, parameters, .{});
            try std.testing.expectEqualStrings("x🍎ê", output.value.string);
            try std.testing.expectError(error.SqlProgramLimitExceeded, program.evaluate(arena.allocator(), &.{}, parameters, .{ .output_bytes = 1 }));
            try std.testing.expectError(error.SqlProgramLimitExceeded, program.evaluate(arena.allocator(), &.{}, parameters, .{ .pattern_steps = 1 }));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.prepared, .{});
    for ([_][]const u8{ "left($1,2)", "right($1,-1)", "split_part($1,'🍎',-2)" }) |sql| {
        var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var program = try bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{});
        defer program.deinit();
        var no_memory = std.heap.FixedBufferAllocator.init(&.{});
        _ = try program.evaluate(no_memory.allocator(), &.{}, &.{.{ .string = "aé🍎z" }}, .{});
    }
}

test "SQL prepared Unicode translation owns literals and avoids per row map allocation" {
    const compiler = @import("compiler.zig");
    var compiled = try compiler.compileScalar(std.testing.allocator, "translate($1,'aé🍎','xê')", .{});
    var program = bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{}) catch |err| {
        compiled.deinit();
        return err;
    };
    compiled.deinit();
    defer program.deinit();
    try std.testing.expect(program.translations.contains(program.root));
    var dynamic_compiled = try compiler.compileScalar(std.testing.allocator, "translate($1,$2,$3)", .{});
    defer dynamic_compiled.deinit();
    var dynamic = try bind(std.testing.allocator, dynamic_compiled.expression, &.{}, &.{}, .{});
    defer dynamic.deinit();
    for ([_]*const Program{ &program, &dynamic }, 0..) |bound, index| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const parameters: []const Json = if (index == 0) &.{.{ .string = "aé🍎aé🍎" }} else &.{ .{ .string = "aé🍎aé🍎" }, .{ .string = "aé🍎" }, .{ .string = "xê" } };
        const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
        for (0..50000) |_| {
            const output = try bound.evaluate(arena.allocator(), &.{}, parameters, .{});
            try std.testing.expectEqualStrings("xêxê", output.value.string);
            try std.testing.expect(arena.reset(.retain_capacity));
        }
        std.debug.print("SQL translate hot loop: prepared={} rows=50000 scratch_bytes={} elapsed_ns={}\n", .{ index == 0, arena.queryCapacity(), std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start });
    }
    var six_bytes: [6]u8 = undefined;
    var exact = std.heap.FixedBufferAllocator.init(&six_bytes);
    const output = try program.evaluate(exact.allocator(), &.{}, &.{.{ .string = "aé🍎aé🍎" }}, .{});
    try std.testing.expectEqualStrings("xêxê", output.value.string);
    try std.testing.expectEqual(@as(usize, 6), exact.end_index);
}

test "SQL scalar bound programs preserve lazy truth exact integers and function semantics" {
    const cases = [_]struct { sql: []const u8, expected: []const u8 }{
        .{ .sql = "1 + 2 * 3", .expected = "7" },
        .{ .sql = "trunc(-7.9)", .expected = "-7" },
        .{ .sql = "trunc(7.9)", .expected = "7" },
        .{ .sql = "trunc(9007199254740993)", .expected = "9007199254740993" },
        .{ .sql = "trunc(NULL)", .expected = "null" },
        .{ .sql = "sign(-9223372036854775808)", .expected = "-1" },
        .{ .sql = "sign(9223372036854775807)", .expected = "1" },
        .{ .sql = "sign(-0.0)", .expected = "0" },
        .{ .sql = "sign(-0.01)", .expected = "-1" },
        .{ .sql = "sign(0.01)", .expected = "1" },
        .{ .sql = "sign(NULL)", .expected = "null" },
        .{ .sql = "-7 / 2", .expected = "-3" },
        .{ .sql = "NULL AND FALSE", .expected = "false" },
        .{ .sql = "NULL OR TRUE", .expected = "true" },
        .{ .sql = "NULL OR FALSE", .expected = "null" },
        .{ .sql = "FALSE AND 1 / 0 = 1", .expected = "false" },
        .{ .sql = "TRUE OR 1 / 0 = 1", .expected = "true" },
        .{ .sql = "coalesce(NULL, 7, 1 / 0)", .expected = "7" },
        .{ .sql = "CASE WHEN TRUE THEN 7 ELSE 1 / 0 END", .expected = "7" },
        .{ .sql = "CASE 2 WHEN 1 THEN 9 WHEN 2 THEN 7 END", .expected = "7" },
        .{ .sql = "9007199254740993 > 9007199254740992.0", .expected = "true" },
        .{ .sql = "NULL IS NOT DISTINCT FROM NULL", .expected = "true" },
        .{ .sql = "CAST('12' AS bigint) + 1", .expected = "13" },
        .{ .sql = "length('é🍎')", .expected = "2" },
        .{ .sql = "substring('aé🍎z', 2, 2)", .expected = "\"é🍎\"" },
        .{ .sql = "trim('éêé', 'é')", .expected = "\"ê\"" },
        .{ .sql = "ltrim('🍎x🍎', '🍎')", .expected = "\"x🍎\"" },
        .{ .sql = "rtrim('🍎x🍎', '🍎')", .expected = "\"🍎x\"" },
        .{ .sql = "trim('éé', 'é')", .expected = "\"\"" },
        .{ .sql = "replace('abcabc','b','XY')", .expected = "\"aXYcaXYc\"" },
        .{ .sql = "'héllo' LIKE 'h_llo'", .expected = "true" },
        .{ .sql = "7 BETWEEN 2 AND 9", .expected = "true" },
        .{ .sql = "7 NOT BETWEEN 2 AND 9", .expected = "false" },
        .{ .sql = "7 IN (1, 7, NULL)", .expected = "true" },
        .{ .sql = "7 NOT IN (1, NULL)", .expected = "null" },
        .{ .sql = "'hello' NOT LIKE 'z%'", .expected = "true" },
        .{ .sql = "('{\"a\":12}'::json ->> 'a')::bigint", .expected = "12" },
        .{ .sql = "'[1,2,3]'::json -> -1", .expected = "3" },
        .{ .sql = "'2024-02-29'::datetime", .expected = "\"2024-02-29T00:00:00.000000000Z\"" },
        .{ .sql = "date_part('year', '2024-02-29'::datetime)", .expected = "2024" },
        .{ .sql = "date_trunc('month', '2024-02-29T13:14:15+01:00'::datetime)", .expected = "\"2024-02-01T00:00:00.000000000Z\"" },
        .{ .sql = "to_timestamp(0)", .expected = "\"1970-01-01T00:00:00.000000000Z\"" },
    };
    for (cases) |case| {
        var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var program = try bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{});
        defer program.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const value = try program.evaluate(arena.allocator(), &.{}, &.{}, .{});
        const encoded = try std.json.Stringify.valueAlloc(arena.allocator(), value.value, .{});
        try std.testing.expectEqualStrings(case.expected, encoded);
    }
}

test "SQL trunc and sign reject invalid types arity and nonfinite numeric inputs" {
    for ([_][]const u8{ "trunc('text')", "sign(TRUE)" }) |sql| {
        var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlTypeMismatch, bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{}));
    }
    for ([_][]const u8{ "trunc(1, 2)", "sign()" }) |sql| {
        var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.InvalidSqlParameters, bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{}));
    }
    var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, "sign($1)", .{});
    defer compiled.deinit();
    var program = try bind(std.testing.allocator, compiled.expression, &.{}, &.{.number}, .{});
    defer program.deinit();
    for ([_]f64{ std.math.inf(f64), std.math.nan(f64) }) |value| {
        try std.testing.expectError(error.SqlNumericOutOfRange, program.evaluate(std.testing.allocator, &.{}, &.{.{ .float = value }}, .{}));
    }
}

test "SQL scalar binding resolves ordinals and parameter types once" {
    var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, "price * $1 + 2", .{});
    defer compiled.deinit();
    var program = try bind(std.testing.allocator, compiled.expression, &.{.{ .name = "price", .type = .integer, .nullable = false }}, &.{}, .{});
    defer program.deinit();
    try std.testing.expectEqual(ast.ColumnType.integer, program.parameter_types[0].?);
    try std.testing.expectEqualSlices(u32, &.{0}, program.required_columns);
    try std.testing.expectEqual(@as(i64, 23), (try program.evaluate(std.testing.allocator, &.{Datum.json(.{ .integer = 7 })}, &.{.{ .integer = 3 }}, .{})).value.integer);
    try std.testing.expectError(error.SqlNumericOutOfRange, program.evaluate(std.testing.allocator, &.{Datum.json(.{ .integer = std.math.maxInt(i64) })}, &.{.{ .integer = 3 }}, .{}));
    try std.testing.expectError(error.SqlProgramLimitExceeded, program.evaluate(std.testing.allocator, &.{Datum.json(.{ .integer = 7 })}, &.{.{ .integer = 3 }}, .{ .steps = 1 }));
}

test "SQL UUID comparison and cast canonicalize typed literals and parameters" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const canonical = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
    var comparison_expr = try @import("compiler.zig").compileScalar(std.testing.allocator, "id = 'A0EEBC999C0B4EF8BB6D6BB9BD380A11'", .{});
    defer comparison_expr.deinit();
    var program = try bind(std.testing.allocator, comparison_expr.expression, &.{.{ .name = "id", .type = .uuid }}, &.{}, .{});
    defer program.deinit();
    const result = try program.evaluate(alloc, &.{Datum.json(.{ .string = canonical })}, &.{}, .{});
    try std.testing.expect(result.value.bool);

    var parameter = try @import("compiler.zig").compileScalar(std.testing.allocator, "id = $1", .{});
    defer parameter.deinit();
    var bound = try bind(std.testing.allocator, parameter.expression, &.{.{ .name = "id", .type = .uuid }}, &.{}, .{});
    defer bound.deinit();
    try std.testing.expectEqual(ast.ColumnType.uuid, bound.parameter_types[0].?);
    try std.testing.expect((try bound.evaluate(alloc, &.{Datum.json(.{ .string = canonical })}, &.{.{ .string = "{A0EEBC999C0B4EF8BB6D6BB9BD380A11}" }}, .{})).value.bool);
    try std.testing.expectError(error.SqlTypeMismatch, bound.evaluate(alloc, &.{Datum.json(.{ .string = canonical })}, &.{.{ .string = "bad" }}, .{}));
    var mismatched = try @import("compiler.zig").compileScalar(std.testing.allocator, "id = label", .{});
    defer mismatched.deinit();
    try std.testing.expectError(error.SqlTypeMismatch, bind(std.testing.allocator, mismatched.expression, &.{ .{ .name = "id", .type = .uuid }, .{ .name = "label", .type = .string } }, &.{}, .{}));
    var mismatched_list = try @import("compiler.zig").compileScalar(std.testing.allocator, "id IN (label)", .{});
    defer mismatched_list.deinit();
    try std.testing.expectError(error.SqlTypeMismatch, bind(std.testing.allocator, mismatched_list.expression, &.{ .{ .name = "id", .type = .uuid }, .{ .name = "label", .type = .string } }, &.{}, .{}));
}

test "SQL discarded EXISTS projection binds parameters without retaining column dependencies" {
    var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, "concat(price / $1, lower(payload), CAST('bad' AS BIGINT))", .{});
    defer compiled.deinit();
    // Lowering constructs this helper in the IR; it is not user SQL syntax.
    const validation: ast.Scalar = .{ .call = .{ .name = "$validate", .args = compiled.expression.call.args } };
    var program = try bind(std.testing.allocator, &validation, &.{ .{ .name = "price", .type = .integer, .nullable = false }, .{ .name = "payload", .type = .string } }, &.{}, .{});
    defer program.deinit();
    try std.testing.expectEqual(ast.ColumnType.integer, program.parameter_types[0].?);
    try std.testing.expectEqual(@as(usize, 0), program.required_columns.len);
    // No row payload is needed, and a zero divisor must never be evaluated.
    try std.testing.expectEqual(@as(i64, 1), (try program.evaluate(std.testing.allocator, &.{}, &.{.{ .integer = 0 }}, .{})).value.integer);
}

test "SQL scalar JSON null remains distinct from SQL NULL through casts and lazy branches" {
    const cases = [_]struct { sql: []const u8, sql_null: bool, expected: []const u8 }{
        .{ .sql = "CAST('null' AS json)", .sql_null = false, .expected = "null" },
        .{ .sql = "CAST(NULL AS json)", .sql_null = true, .expected = "null" },
        .{ .sql = "CAST('null' AS json) IS NULL", .sql_null = false, .expected = "false" },
        .{ .sql = "CAST('null' AS json) IS DISTINCT FROM NULL", .sql_null = false, .expected = "true" },
        .{ .sql = "coalesce(CAST('null' AS json), CAST('{}' AS json))", .sql_null = false, .expected = "null" },
        .{ .sql = "CAST(CAST('null' AS json) AS text)", .sql_null = false, .expected = "\"null\"" },
        .{ .sql = "('{\"a\":null}'::json -> 'a') IS NULL", .sql_null = false, .expected = "false" },
        .{ .sql = "('{\"a\":null}'::json ->> 'a') IS NULL", .sql_null = false, .expected = "true" },
        .{ .sql = "('{\"a\":null}'::json -> 'missing') IS NULL", .sql_null = false, .expected = "true" },
    };
    for (cases) |case| {
        var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var program = try bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{});
        defer program.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const value = try program.evaluate(arena.allocator(), &.{}, &.{}, .{});
        try std.testing.expectEqual(case.sql_null, value.sql_null);
        try std.testing.expectEqualStrings(case.expected, try std.json.Stringify.valueAlloc(arena.allocator(), value.value, .{}));
    }
}

test "SQL scalar parameter constraints remain unknown until shared statement context resolves them" {
    var projected = try @import("compiler.zig").compileScalar(std.testing.allocator, "$1", .{});
    defer projected.deinit();
    var arithmetic_expression = try @import("compiler.zig").compileScalar(std.testing.allocator, "$1 + 2", .{});
    defer arithmetic_expression.deinit();
    var parameters: [1]?ast.ColumnType = .{null};
    try std.testing.expect(!try inferParameters(std.testing.allocator, projected.expression, &.{}, &parameters, null, .{}));
    try std.testing.expectEqual(@as(?ast.ColumnType, null), parameters[0]);
    try std.testing.expect(try inferParameters(std.testing.allocator, arithmetic_expression.expression, &.{}, &parameters, null, .{}));
    var program = try bind(std.testing.allocator, projected.expression, &.{}, &parameters, .{});
    defer program.deinit();
    try std.testing.expectEqual(ast.ColumnType.integer, program.output_type.kind.?);
    try std.testing.expectEqual(@as(i64, 3), (try program.evaluate(std.testing.allocator, &.{}, &.{.{ .integer = 3 }}, .{})).value.integer);
    var chain = try @import("compiler.zig").compileScalar(std.testing.allocator, "$1 = $2 AND $2 > 3", .{});
    defer chain.deinit();
    var chain_parameters: [2]?ast.ColumnType = .{ null, null };
    _ = try inferParameters(std.testing.allocator, chain.expression, &.{}, &chain_parameters, .boolean, .{});
    _ = try inferParameters(std.testing.allocator, chain.expression, &.{}, &chain_parameters, .boolean, .{});
    try std.testing.expectEqualSlices(?ast.ColumnType, &.{ .integer, .integer }, &chain_parameters);
}

test "SQL scalar binding and computed output clean up on allocation failures" {
    const Harness = struct {
        fn run(alloc: Allocator) !void {
            var compiled = try @import("compiler.zig").compileScalar(alloc, "CASE WHEN price > $1 THEN concat(trim('éêé', 'é'), 'null'::json) ELSE 'none' END", .{});
            defer compiled.deinit();
            var program = try bind(alloc, compiled.expression, &.{.{ .name = "price", .type = .integer }}, &.{}, .{});
            defer program.deinit();
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const result = try program.evaluate(arena.allocator(), &.{Datum.json(.{ .integer = 9 })}, &.{.{ .integer = 2 }}, .{});
            try std.testing.expectEqualStrings("ênull", result.value.string);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL scalar bound arithmetic hot loop does not allocate per row" {
    var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, "CASE WHEN price > $1 THEN price * 3 + 7 ELSE 0 END", .{});
    defer compiled.deinit();
    var program = try bind(std.testing.allocator, compiled.expression, &.{.{ .name = "price", .type = .integer }}, &.{}, .{});
    defer program.deinit();
    var no_memory = std.heap.FixedBufferAllocator.init(&.{});
    var checksum: i64 = 0;
    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..100000) |i| {
        const result = try program.evaluate(no_memory.allocator(), &.{Datum.json(.{ .integer = @intCast(i) })}, &.{.{ .integer = 100 }}, .{});
        checksum += result.value.integer;
    }
    const elapsed = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start;
    try std.testing.expectEqual(@as(i64, 15000534143), checksum);
    std.debug.print("SQL scalar hot loop: rows=100000 instructions={} allocated_bytes=0 checksum={} elapsed_ns={}\n", .{ program.instructions.len, checksum, elapsed });
}

test "SQL LIKE admits large ordinary text with an independent bounded work budget" {
    var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, "$1 ILIKE $2", .{});
    defer compiled.deinit();
    var program = try bind(std.testing.allocator, compiled.expression, &.{}, &.{ .string, .string }, .{});
    defer program.deinit();
    const text = try std.testing.allocator.alloc(u8, 64 * 1024);
    defer std.testing.allocator.free(text);
    @memset(text, 'X');
    @memcpy(text[text.len - 6 ..], "needle");
    for ([_][]const u8{ "%NEEDLE", "%NEEDLE%", "%absent%" }, [_]bool{ true, true, false }) |pattern, expected| {
        const actual = try program.evaluate(std.testing.allocator, &.{}, &.{ .{ .string = text }, .{ .string = pattern } }, .{});
        try std.testing.expectEqual(expected, actual.value.bool);
    }
    try std.testing.expectError(error.SqlProgramLimitExceeded, program.evaluate(std.testing.allocator, &.{}, &.{ .{ .string = text }, .{ .string = "%absent%" } }, .{ .pattern_steps = 32 }));
}

test "SQL document and read text primitives preserve Unicode NULLs and bounded outputs" {
    for ([_][]const u8{ "concat_ws(':')", "concat()" }) |sql| {
        var invalid = try @import("compiler.zig").compileScalar(std.testing.allocator, sql, .{});
        defer invalid.deinit();
        try std.testing.expectError(error.InvalidSqlParameters, bind(std.testing.allocator, invalid.expression, &.{}, &.{}, .{}));
    }
    const cases = [_]struct { sql: []const u8, json: []const u8, sql_null: bool = false }{
        .{ .sql = "concat_ws(':','a',NULL,'',3)", .json = "\"a::3\"" },
        .{ .sql = "concat_ws(':',NULL,NULL)", .json = "\"\"" },
        .{ .sql = "concat_ws(NULL,'a')", .json = "null", .sql_null = true },
        .{ .sql = "concat_ws(':',CAST('null' AS json),'a')", .json = "\"null:a\"" },
        .{ .sql = "bit_length('é')", .json = "16" },
        .{ .sql = "lpad('é',4,'🍎x')", .json = "\"🍎x🍎é\"" },
        .{ .sql = "rpad('é',4,'🍎x')", .json = "\"é🍎x🍎\"" },
        .{ .sql = "lpad('é🍎',1,'x')", .json = "\"é\"" },
        .{ .sql = "rpad('é',3,'')", .json = "\"é\"" },
        .{ .sql = "lpad('é',3)", .json = "\"  é\"" },
        .{ .sql = "lpad('é',-1)", .json = "\"\"" },
        .{ .sql = "repeat('é🍎',2)", .json = "\"é🍎é🍎\"" },
        .{ .sql = "repeat('é',-1)", .json = "\"\"" },
        .{ .sql = "repeat('',9223372036854775807)", .json = "\"\"" },
        .{ .sql = "reverse('aé🍎')", .json = "\"🍎éa\"" },
        .{ .sql = "lpad('é',NULL)", .json = "null", .sql_null = true },
        .{ .sql = "NULL ISNULL", .json = "true" },
        .{ .sql = "NULL NOTNULL", .json = "false" },
        .{ .sql = "1 ISNULL", .json = "false" },
        .{ .sql = "NULL ISNULL AND 1 NOTNULL", .json = "true" },
        .{ .sql = "strpos('aé🍎z','🍎')", .json = "3" },
        .{ .sql = "strpos('aé','')", .json = "1" },
        .{ .sql = "strpos('aé','x')", .json = "0" },
        .{ .sql = "jsonb_typeof(CAST('null' AS json))", .json = "\"null\"" },
        .{ .sql = "jsonb_typeof(CAST(NULL AS json))", .json = "null", .sql_null = true },
        .{ .sql = "jsonb_typeof(to_jsonb(9007199254740993))", .json = "\"number\"" },
        .{ .sql = "jsonb_typeof(to_jsonb(true))", .json = "\"boolean\"" },
    };
    for (cases) |case| {
        var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var program = try bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{});
        defer program.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const datum = try program.evaluate(arena.allocator(), &.{}, &.{}, .{});
        try std.testing.expectEqual(case.sql_null, datum.sql_null);
        try std.testing.expectEqualStrings(case.json, try std.json.Stringify.valueAlloc(arena.allocator(), datum.value, .{}));
    }
    var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, "concat_ws(':','long','value')", .{});
    defer compiled.deinit();
    var program = try bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{});
    defer program.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, program.evaluate(arena.allocator(), &.{}, &.{}, .{ .output_bytes = 2 }));
    for ([_][]const u8{ "repeat('ab',9223372036854775807)", "lpad('é',9223372036854775807,'🍎')", "rpad('x',128,'a')", "reverse('abcdef')" }) |sql| {
        var bounded = try @import("compiler.zig").compileScalar(std.testing.allocator, sql, .{});
        defer bounded.deinit();
        var bounded_program = try bind(std.testing.allocator, bounded.expression, &.{}, &.{}, .{});
        defer bounded_program.deinit();
        try std.testing.expectError(error.SqlProgramLimitExceeded, bounded_program.evaluate(arena.allocator(), &.{}, &.{}, .{ .output_bytes = 2 }));
    }
}

test "SQL Unicode text hot loop reuses bounded output scratch" {
    var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, "rpad(reverse($1),128,'é')", .{});
    defer compiled.deinit();
    var program = try bind(std.testing.allocator, compiled.expression, &.{}, &.{.string}, .{});
    defer program.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var checksum: usize = 0;
    const started = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..50000) |_| {
        if (!arena.reset(.retain_capacity)) return error.OutOfMemory;
        const value = try program.evaluate(arena.allocator(), &.{}, &.{.{ .string = "antfly 🍎" }}, .{ .output_bytes = 1024 });
        checksum += value.value.string.len;
    }
    try std.testing.expectEqual(@as(usize, 12550000), checksum);
    try std.testing.expect(arena.queryCapacity() <= 8192);
    std.debug.print("SQL Unicode text: rows=50000 scratch_bytes={d} elapsed_ns={d}\n", .{ arena.queryCapacity(), std.Io.Clock.now(.awake, std.testing.io).nanoseconds - started });
}

test "SQL typed JSON construction and path extraction preserve scalar and NULL provenance" {
    const cases = [_]struct { sql: []const u8, json: []const u8, sql_null: bool = false }{
        .{ .sql = "to_jsonb('wrapped')", .json = "\"wrapped\"" },
        .{ .sql = "to_jsonb(9007199254740993)", .json = "9007199254740993" },
        .{ .sql = "to_jsonb(NULL)", .json = "null", .sql_null = true },
        .{ .sql = "to_jsonb(CAST('null' AS json))", .json = "null" },
        .{ .sql = "jsonb_build_object()", .json = "{}" },
        .{ .sql = "jsonb_build_object('a',1,'b',NULL,'a',2,'json',CAST('null' AS json))", .json = "{\"a\":2,\"b\":null,\"json\":null}" },
        .{ .sql = "jsonb_extract_path_text(CAST('{\"a\":[1,\"last\"]}' AS json),'a','-1')", .json = "\"last\"" },
        .{ .sql = "jsonb_extract_path_text(CAST('{\"a\":null}' AS json),'a')", .json = "null", .sql_null = true },
        .{ .sql = "jsonb_extract_path_text(CAST('{}' AS json),'missing')", .json = "null", .sql_null = true },
        .{ .sql = "jsonb_extract_path_text(CAST('[1]' AS json),'9223372036854775807')", .json = "null", .sql_null = true },
        .{ .sql = "jsonb_extract_path_text(CAST('{\"a\":true}' AS json),'a')", .json = "\"true\"" },
    };
    for (cases) |case| {
        var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var program = try bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{});
        defer program.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const actual = try program.evaluate(arena.allocator(), &.{}, &.{}, .{});
        try std.testing.expectEqual(case.sql_null, actual.sql_null);
        const expected = try std.json.parseFromSlice(Json, arena.allocator(), case.json, .{});
        defer expected.deinit();
        try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(arena.allocator(), expected.value, .{}), try std.json.Stringify.valueAlloc(arena.allocator(), actual.value, .{}));
    }
    var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, "jsonb_build_object(NULL,1)", .{});
    defer compiled.deinit();
    var program = try bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{});
    defer program.deinit();
    try std.testing.expectError(error.InvalidSqlParameters, program.evaluate(std.testing.allocator, &.{}, &.{}, .{}));
    var bounded = try @import("compiler.zig").compileScalar(std.testing.allocator, "jsonb_build_object('large',1)", .{});
    defer bounded.deinit();
    var bounded_program = try bind(std.testing.allocator, bounded.expression, &.{}, &.{}, .{});
    defer bounded_program.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, bounded_program.evaluate(std.testing.allocator, &.{}, &.{}, .{ .output_bytes = 1 }));
}
