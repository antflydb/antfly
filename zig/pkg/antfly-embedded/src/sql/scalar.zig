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
    /// Exact immutable NUMERIC payload, owned by the pinned value region.
    /// JSON null is only a placeholder and is never SQL NULL or numeric data.
    numeric: ?*const @import("numeric_value.zig").Value = null,
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
    pub fn typedNumeric(value: *const @import("numeric_value.zig").Value) Datum {
        return .{ .numeric = value, .sql_null = false };
    }
};

/// Leaky constructors retain limbs and their stable view in the caller's
/// region. Frames/operators own that region; no input bytes remain borrowed.
pub fn numericTextLeaky(a: Allocator, text: []const u8, work: *arrays.Budget) !Datum {
    return numericTextWithModifierLeaky(a, text, null, work);
}

/// Assignment and parsing consume the same caller-owned invocation budget.
/// The caller discards its bounded region on failure, including owned limbs.
pub fn numericTextWithModifierLeaky(a: Allocator, text: []const u8, modifier: ?@import("numeric_value.zig").TypeModifier, work: *arrays.Budget) !Datum {
    const exact = @import("numeric_value.zig");
    var ctx = work.numericContext(a);
    defer work.remaining = @intCast(ctx.remaining);
    var parsed = try exact.parse(&ctx, text);
    errdefer parsed.deinit();
    if (modifier) |constraint| {
        var constrained = try exact.applyTypeModifier(&ctx, parsed.value, constraint);
        errdefer constrained.deinit();
        const value = try a.create(exact.Value);
        value.* = constrained.value;
        parsed.deinit();
        return Datum.typedNumeric(value);
    }
    const value = try a.create(exact.Value);
    value.* = parsed.value;
    return Datum.typedNumeric(value);
}
pub fn numericBinaryLeaky(a: Allocator, bytes: []const u8, work: *arrays.Budget) !Datum {
    const exact = @import("numeric_value.zig");
    var ctx = work.numericContext(a);
    defer work.remaining = @intCast(ctx.remaining);
    var parsed = try @import("numeric_binary.zig").decode(&ctx, bytes, .{});
    errdefer parsed.deinit();
    const value = try a.create(exact.Value);
    value.* = parsed.value;
    return Datum.typedNumeric(value);
}

test "SQL NUMERIC ingress constructors retain shared cancellation quotas and limits" {
    const exact = @import("numeric_value.zig");
    const a = std.testing.allocator;
    const text: [2048]u8 = @splat('1');
    var reference: exact.Context = .{ .alloc = a };
    var parsed = try exact.parse(&reference, &text);
    defer parsed.deinit();
    const bytes = try @import("numeric_binary.zig").encodeAlloc(&reference, parsed.value);
    defer a.free(bytes);
    const Cancel = struct {
        calls: usize = 0,
        fn poll(ptr: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.calls += 1;
            if (self.calls == 2) return error.Canceled;
        }
    };
    for ([_]bool{ false, true }) |binary_input| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const alloc = arena.allocator();
        var parent: exact.Context = .{ .alloc = alloc };
        var work: arrays.Budget = .{ .shared = &parent };
        const parent_before = parent.remaining;
        const local_before = work.remaining;
        const value = if (binary_input) try numericBinaryLeaky(alloc, bytes, &work) else try numericTextLeaky(alloc, &text, &work);
        try std.testing.expectEqual(local_before - work.remaining, parent_before - parent.remaining);
        try std.testing.expectEqual(std.math.Order.eq, try exact.order(&reference, parsed.value, value.numeric.?.*));

        var cancel: Cancel = .{};
        parent = .{ .alloc = alloc, .checkpoint = Cancel.poll, .ptr = &cancel };
        work = .{ .shared = &parent };
        if (binary_input) {
            try std.testing.expectError(error.Canceled, numericBinaryLeaky(alloc, bytes, &work));
        } else try std.testing.expectError(error.Canceled, numericTextLeaky(alloc, &text, &work));
        try std.testing.expectEqual(@as(usize, 2), cancel.calls);
        parent.checkpoint = null;
        work.remaining = 1_048_576;
        try std.testing.expectError(error.Canceled, numericTextLeaky(alloc, "1", &work));

        parent = .{ .alloc = alloc };
        work = .{ .shared = &parent, .remaining = 128 };
        if (binary_input) {
            try std.testing.expectError(error.SqlProgramLimitExceeded, numericBinaryLeaky(alloc, bytes, &work));
        } else try std.testing.expectError(error.SqlProgramLimitExceeded, numericTextLeaky(alloc, &text, &work));
        work.remaining = 1_048_576;
        try std.testing.expectError(error.SqlProgramLimitExceeded, numericTextLeaky(alloc, "1", &work));

        parent = .{ .alloc = alloc, .max_input_bytes = 8 };
        work = .{ .shared = &parent };
        if (binary_input) {
            try std.testing.expectError(error.SqlProgramLimitExceeded, numericBinaryLeaky(alloc, bytes, &work));
        } else try std.testing.expectError(error.SqlProgramLimitExceeded, numericTextLeaky(alloc, &text, &work));
        parent = .{ .alloc = alloc, .max_groups = 1 };
        work = .{ .shared = &parent };
        if (binary_input) {
            try std.testing.expectError(error.SqlProgramLimitExceeded, numericBinaryLeaky(alloc, bytes, &work));
        } else try std.testing.expectError(error.SqlProgramLimitExceeded, numericTextLeaky(alloc, &text, &work));
    }
}

pub const NumericJsonText = struct {
    value: *const @import("numeric_value.zig").Value,
    pub fn jsonStringify(self: @This(), writer: anytype) std.Io.Writer.Error!void {
        var none = std.heap.FixedBufferAllocator.init(&.{});
        var ctx: @import("numeric_value.zig").Context = .{ .alloc = none.allocator() };
        try writer.beginWriteRaw();
        try writer.writer.writeByte('"');
        @import("numeric_value.zig").write(&ctx, self.value.*, writer.writer) catch return error.WriteFailed;
        try writer.writer.writeByte('"');
        writer.endWriteRaw();
    }
};
const arrays = @import("array_value.zig");
const builtin_cast = @import("builtin_cast.zig");
const regex_functions = @import("regex_functions.zig");
pub const NumericModifier = @import("../common/sql_builtin_type.zig").NumericModifier;
pub const Type = struct { kind: ?ast.ColumnType = null, nullable: bool = true, element_type: ?arrays.ElementType = null, numeric_modifier: ?@import("../common/sql_builtin_type.zig").NumericModifier = null };
pub const Column = struct {
    name: []const u8,
    type: ast.ColumnType,
    nullable: bool = true,
    element_type: ?arrays.ElementType = null,
    numeric_modifier: ?@import("../common/sql_builtin_type.zig").NumericModifier = null,
    /// Authorized alternate spellings share one cell/dependency ordinal.
    /// They are binding metadata, never additional physical row values.
    aliases: []const []const u8 = &.{},
};
pub const BindLimits = struct {
    nodes: usize = 8192,
    depth: usize = 64,
    parameters: usize = 1024,
    invocation: ?*@import("parameter_binding.zig").Invocation = null,
    assignment: bool = false,
};
const decisions = @import("../functions/decisions.zig");
pub const DecisionDemand = struct { instruction: u32, function: decisions.Function, args: []const Json };
pub const EvalLimits = struct {
    steps: usize = 65_536,
    pattern_steps: usize = 8 * 1024 * 1024,
    depth: usize = 64,
    output_bytes: usize = 1024 * 1024,
    /// Request cancellation/deadline control shared by ordinary expressions
    /// and bounded exact arithmetic, independently of regex-session control.
    checkpoint: ?*const fn (?*anyopaque) anyerror!void = null,
    checkpoint_context: ?*anyopaque = null,
    decision_values: ?[]const ?Datum = null,
    decision_demand: ?*?DecisionDemand = null,
    regex_session: ?*regex_functions.Session = null,
    regex_execution: ?*@import("regex_execution.zig") = null,
    regex_checkpoint: ?*const fn (?*anyopaque) anyerror!void = null,
    regex_context: ?*anyopaque = null,
};
pub const Function = enum { ai_decide, ai_choice, ai_score, ai_probability, abs, lower, upper, length, octet_length, concat, coalesce, nullif, greatest, least, ceil, floor, round, sqrt, power, mod, substring, trim, ltrim, rtrim, replace, starts_with, date_part, date_trunc, to_timestamp, current_setting, @"$single", @"$pattern_quantified", trunc, sign, to_jsonb, jsonb_build_object, jsonb_extract_path_text, concat_ws, bit_length, strpos, jsonb_typeof, lpad, rpad, repeat, reverse, left, right, split_part, translate, overlay, ascii, chr, @"$array", @"$array_quantified", cardinality, array_ndims, array_length, array_lower, array_upper, @"$array_pattern_quantified", @"$contains", @"$overlaps", array_to_string, jsonb_exists, string_to_array, @"$like_escape", jsonb_set, array_position, array_positions, array_remove, array_replace, array_append, array_prepend, array_cat, jsonb_exists_any, jsonb_exists_all, regexp_like, regexp_count, regexp_instr, regexp_substr, regexp_replace };

fn regexFunction(function: Function) ?regex_functions.Function {
    return switch (function) {
        .regexp_like => .regexp_like,
        .regexp_count => .regexp_count,
        .regexp_instr => .regexp_instr,
        .regexp_substr => .regexp_substr,
        .regexp_replace => .regexp_replace,
        else => null,
    };
}

pub const Instruction = struct {
    type: Type,
    operation: union(enum) {
        literal: Json,
        column: u32,
        parameter: u32,
        unary: struct { op: ast.Scalar.Unary, operand: u32 },
        binary: struct { op: ast.Scalar.Binary, left: u32, right: u32 },
        call: struct { function: Function, args: []const u32, setting_identity: ?setting_catalog.Identity = null },
        cast: struct { operand: u32, type: ast.ColumnType, element_type: ?arrays.ElementType = null, numeric_modifier: ?@import("../common/sql_builtin_type.zig").NumericModifier = null },
        case_when: struct { branches: []const Branch, otherwise: ?u32 },
        in_list: struct { operand: u32, values: []const u32, negated: bool },
    },
    const Branch = struct { condition: u32, value: u32 };
};

const TextTranslation = std.AutoHashMapUnmanaged(u21, []const u8);

pub const Program = struct {
    arena: *std.heap.ArenaAllocator,
    instructions: []const Instruction,
    root: u32,
    output_type: Type,
    parameter_types: []const ?ast.ColumnType,
    /// Authoritative binding identity. The coarse slice above is a derived
    /// compatibility view for statement/protocol paths not yet migrated.
    parameter_descriptors: []const Type,
    required_columns: []const u32,
    settings: ?*const setting_catalog.View = null,
    /// Statement-owned, stable-address contract. Preparation validates all
    /// registered slices before publishing its immutable execution frame.
    invocation: ?*@import("parameter_binding.zig").Invocation = null,
    /// Derived execution caches are never part of serialized instructions.
    translations: std.AutoHashMapUnmanaged(u32, *const TextTranslation) = .empty,
    constant_arrays: std.AutoHashMapUnmanaged(u32, *const arrays.Value) = .empty,
    constant_numerics: std.AutoHashMapUnmanaged(u32, *const @import("numeric_value.zig").Value) = .empty,
    constant_memberships: std.AutoHashMapUnmanaged(u32, *arrays.Membership) = .empty,

    fn literalInstruction(self: *const Program, index: u32) bool {
        return switch (self.instructions[index].operation) {
            .literal => true,
            .cast => |cast| self.literalInstruction(cast.operand),
            .unary => |unary| self.literalInstruction(unary.operand),
            .call => |call| blk: {
                switch (call.function) {
                    .@"$array", .string_to_array, .abs, .ceil, .floor, .round, .trunc, .sign, .mod, .sqrt => {},
                    else => break :blk false,
                }
                for (call.args) |arg| if (!self.literalInstruction(arg)) break :blk false;
                break :blk true;
            },
            else => false,
        };
    }

    fn prepareConstantArrays(self: *Program, a: Allocator) !void {
        for (self.instructions, 0..) |instruction, index| {
            if (instruction.type.kind == .number and instruction.type.element_type == .numeric and self.literalInstruction(@intCast(index))) {
                const value = self.prepareInstruction(a, @intCast(index)) catch |err| switch (err) {
                    // Preparing a pure function is an optimization, not an
                    // execution demand. Unreachable CASE/COALESCE branches
                    // must not acquire a square-root domain error here.
                    error.SqlInvalidPowerArgument, error.NumericModifierNotPreparable => continue,
                    else => return err,
                };
                if (value.numeric) |number| try self.constant_numerics.put(a, @intCast(index), number);
                continue;
            }
            if (instruction.type.kind != .array or !self.literalInstruction(@intCast(index))) continue;
            const value = self.prepareInstruction(a, @intCast(index)) catch |err| switch (err) {
                error.SqlInvalidPowerArgument, error.NumericModifierNotPreparable => continue,
                else => return err,
            };
            if (value.array) |array| try self.constant_arrays.put(a, @intCast(index), array);
        }
        for (self.instructions) |instruction| {
            if (instruction.operation != .call) continue;
            const call = instruction.operation.call;
            if (call.function != .@"$contains" and call.function != .@"$overlaps" and !(call.function == .array_position and call.args.len == 2)) continue;
            const source = if (call.function == .@"$overlaps" and self.constant_arrays.contains(call.args[1])) call.args[1] else call.args[0];
            if (self.constant_memberships.contains(source)) continue;
            const array = self.constant_arrays.get(source) orelse continue;
            const index = try a.create(arrays.Membership);
            index.* = try arrays.Membership.init(a, array.*, .{});
            try self.constant_memberships.put(a, source, index);
        }
    }

    pub fn deinit(self: *Program) void {
        var memberships = self.constant_memberships.valueIterator();
        while (memberships.next()) |index| index.*.deinit();
        const backing = self.arena.child_allocator;
        self.arena.deinit();
        backing.destroy(self.arena);
        self.* = undefined;
    }

    /// Cells use the ordinal order supplied to bind(), including unselected
    /// NULL placeholders. Borrowed scalar results remain valid while the
    /// program, cells and parameters live; computed strings use alloc.
    fn prepareInstruction(self: *const Program, alloc: Allocator, index: u32) !Datum {
        var context: Evaluator = .{ .program = self, .alloc = alloc, .cells = &.{}, .parameters = &.{}, .typed_parameters = self.preparedValues(), .limits = .{}, .constant_preparation = true };
        return context.runDatum(index, 0);
    }

    pub fn evaluateInstruction(self: *const Program, alloc: Allocator, index: u32, parameters: []const Json) !Datum {
        var context: Evaluator = .{ .program = self, .alloc = alloc, .cells = &.{}, .parameters = parameters, .typed_parameters = self.preparedValues(), .limits = .{} };
        return context.runDatum(index, 0);
    }

    pub fn evaluate(self: *const Program, alloc: Allocator, cells: []const Datum, parameters: []const Json, limits: EvalLimits) !Datum {
        var context: Evaluator = .{ .program = self, .alloc = alloc, .cells = cells, .parameters = parameters, .typed_parameters = self.preparedValues(), .limits = limits };
        const result = try context.runDatum(self.root, 0);
        _ = try context.validateJson(result.value, 0);
        return result;
    }

    fn preparedValues(self: *const Program) ?[]const Datum {
        const invocation = self.invocation orelse return null;
        return if (invocation.frame) |frame| frame.values else null;
    }

    /// Bind once per program/execution, not per row. Multiple programs from a
    /// statement share one frame after exact descriptor compatibility checks.
    pub fn bindParameters(self: *const Program, frame: *const @import("parameter_frame.zig").Frame) !PreparedEvaluation {
        if (frame.descriptors.len != frame.values.len or frame.descriptors.len < self.parameter_descriptors.len) return error.InvalidSqlParameters;
        // A statement frame is shared by independently bound programs. Only
        // parameter instructions constrain this program: unused/defaulted
        // descriptor slots must not reject a different program's array type.
        // This validation is execution setup, never row-loop work.
        for (self.instructions) |instruction| {
            if (instruction.operation != .parameter) continue;
            const ordinal = instruction.operation.parameter;
            if (ordinal >= self.parameter_descriptors.len) return error.InvalidSqlParameters;
            const actual = frame.descriptors[ordinal];
            const expected = self.parameter_descriptors[ordinal];
            if (actual.kind != expected.kind or actual.element_type != expected.element_type or actual.nullable != expected.nullable) return error.ConflictingSqlParameterTypes;
        }
        return .{ .program = self, .parameters = frame.values };
    }
};

/// Borrows an immutable program and frame. Both owners outlive execution and
/// returned borrowed values; computed results use the supplied row allocator.
pub const PreparedEvaluation = struct {
    program: *const Program,
    parameters: []const Datum,

    pub fn evaluateInstruction(self: PreparedEvaluation, alloc: Allocator, index: u32) !Datum {
        var context: Evaluator = .{ .program = self.program, .alloc = alloc, .cells = &.{}, .parameters = &.{}, .typed_parameters = self.parameters, .limits = .{} };
        return context.runDatum(index, 0);
    }

    pub fn evaluate(self: PreparedEvaluation, alloc: Allocator, cells: []const Datum, limits: EvalLimits) !Datum {
        var context: Evaluator = .{ .program = self.program, .alloc = alloc, .cells = cells, .parameters = &.{}, .typed_parameters = self.parameters, .limits = limits };
        const result = try context.runDatum(self.program.root, 0);
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
    if (limits.invocation) |invocation| return invocation.infer(alloc, expression, columns, parameters, if (expected) |kind| Type{ .kind = kind } else null, limits);
    if (limits.parameters > 1024 or parameters.len > limits.parameters) return error.SqlProgramLimitExceeded;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var binder: Binder = .{ .alloc = arena.allocator(), .columns = columns, .limits = limits, .allow_unresolved = true };
    try binder.registerColumns();
    for (parameters, binder.parameters[0..parameters.len]) |kind, *descriptor| descriptor.* = .{ .kind = kind };
    _ = try binder.compile(expression, expected, 0);
    if (binder.parameter_count > parameters.len) return error.InvalidSqlParameters;
    var changed = false;
    for (parameters, binder.parameters[0..parameters.len]) |*output, inferred| if (inferred.kind) |kind| {
        if (output.* == null) changed = true;
        output.* = kind;
    };
    return changed;
}

/// Read a provisional result type without defaulting unresolved parameters or
/// NULL literals. Relation-wide inference uses this before emitting programs.
pub fn inferOutput(alloc: Allocator, expression: *const ast.Scalar, columns: []const Column, parameters: []const ?ast.ColumnType) !Type {
    return inferOutputWithInvocation(alloc, expression, columns, parameters, null);
}

pub fn inferOutputWithInvocation(alloc: Allocator, expression: *const ast.Scalar, columns: []const Column, parameters: []const ?ast.ColumnType, invocation: ?*@import("parameter_binding.zig").Invocation) !Type {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var binder: Binder = .{ .alloc = arena.allocator(), .columns = columns, .limits = .{}, .allow_unresolved = true, .typed_parameters = invocation != null };
    if (parameters.len > binder.parameters.len) return error.SqlProgramLimitExceeded;
    for (parameters, binder.parameters[0..parameters.len]) |kind, *descriptor| descriptor.* = .{ .kind = kind };
    if (invocation) |owner| {
        try owner.mergeCoarse(parameters);
        @memcpy(binder.parameters[0..owner.descriptors.len], owner.descriptors);
    }
    try binder.registerColumns();
    return binder.infer(expression, 0);
}

pub fn bindExpected(alloc: Allocator, expression: *const ast.Scalar, columns: []const Column, parameter_hints: []const ?ast.ColumnType, expected: ?ast.ColumnType, limits: BindLimits) !Program {
    return bindExpectedWithSettings(alloc, expression, columns, parameter_hints, expected, limits, null);
}

pub fn bindExpectedWithSettings(alloc: Allocator, expression: *const ast.Scalar, columns: []const Column, parameter_hints: []const ?ast.ColumnType, expected: ?ast.ColumnType, limits: BindLimits, settings: ?*const setting_catalog.View) !Program {
    if (limits.invocation) |invocation| {
        try invocation.mergeCoarse(parameter_hints);
        return bindDescriptors(alloc, expression, columns, invocation.descriptors, if (expected) |kind| Type{ .kind = kind } else null, limits, settings, true);
    }
    if (limits.parameters > 1024 or parameter_hints.len > limits.parameters or limits.nodes == 0) return error.SqlProgramLimitExceeded;
    var descriptors: [1024]Type = undefined;
    for (parameter_hints, descriptors[0..parameter_hints.len]) |kind, *descriptor| descriptor.* = .{ .kind = kind };
    return bindDescriptors(alloc, expression, columns, descriptors[0..parameter_hints.len], if (expected) |kind| Type{ .kind = kind } else null, limits, settings, false);
}

/// Precise native parameter binding; ingress must prepare an owned frame once
/// before row execution. JSON compatibility entry points remain guarded.
pub fn bindTyped(alloc: Allocator, expression: *const ast.Scalar, columns: []const Column, parameter_hints: []const Type, limits: BindLimits) !Program {
    return bindDescriptors(alloc, expression, columns, parameter_hints, null, limits, null, true);
}

pub fn bindTypedExpectedWithSettings(alloc: Allocator, expression: *const ast.Scalar, columns: []const Column, parameter_hints: []const Type, expected: ?Type, limits: BindLimits, settings: ?*const setting_catalog.View) !Program {
    return bindDescriptors(alloc, expression, columns, parameter_hints, expected, limits, settings, true);
}

pub fn inferTypedParameters(alloc: Allocator, expression: *const ast.Scalar, columns: []const Column, parameters: []Type, limits: BindLimits) !bool {
    return inferTypedParametersExpected(alloc, expression, columns, parameters, null, limits);
}

pub fn inferTypedParametersExpected(alloc: Allocator, expression: *const ast.Scalar, columns: []const Column, parameters: []Type, expected: ?Type, limits: BindLimits) !bool {
    if (limits.parameters > 1024 or parameters.len > limits.parameters) return error.SqlProgramLimitExceeded;
    // An expected array *family* is a constraint, not an input descriptor.
    // Element identity may already come from the expression or shared hints.
    // Authoritative input descriptors still require an explicit element type.
    if (expected) |descriptor| if (descriptor.kind != .array or descriptor.element_type != null) try validateParameterType(descriptor);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var binder: Binder = .{ .alloc = arena.allocator(), .columns = columns, .limits = limits, .allow_unresolved = true, .typed_parameters = true };
    try binder.registerColumns();
    for (parameters) |descriptor| try validateParameterType(descriptor);
    for (parameters, binder.parameters[0..parameters.len]) |descriptor, *output| output.* = try normalizeParameterType(descriptor);
    _ = try binder.compileArrayContext(expression, if (expected) |kind| kind.kind else null, if (expected) |kind| kind.element_type else null, 0);
    if (binder.parameter_count > parameters.len) return error.InvalidSqlParameters;
    var changed = false;
    for (parameters, binder.parameters[0..parameters.len]) |*output, inferred| {
        if (output.kind != inferred.kind or output.element_type != inferred.element_type) changed = true;
        output.* = inferred;
    }
    return changed;
}

pub fn validateParameterType(descriptor: Type) !void {
    if (descriptor.numeric_modifier) |modifier| {
        if ((descriptor.kind != .number and descriptor.kind != .array) or descriptor.element_type != .numeric) return error.InvalidSqlParameters;
        modifier.validate() catch return error.InvalidSqlParameters;
    }
    if (descriptor.kind == null) {
        if (descriptor.element_type != null) return error.InvalidSqlParameters;
        return;
    }
    if (descriptor.kind == .array) {
        if (descriptor.element_type == null) return error.InvalidSqlParameters;
    } else if (descriptor.element_type) |element| {
        if (arrayScalarType(element) != descriptor.kind.?) return error.InvalidSqlParameters;
    }
}

pub fn parameterElementType(descriptor: Type) !arrays.ElementType {
    try validateParameterType(descriptor);
    return descriptor.element_type orelse try arrayElementType(descriptor.kind orelse return error.UnknownSqlParameterType);
}

/// Only an unresolved root string literal inherits the target array domain.
/// Explicit text expressions retain their identity and require an SQL cast.
pub fn assignmentExpression(alloc: Allocator, expression: *const ast.Scalar, expected: Type) !*const ast.Scalar {
    if (expected.kind != .array or expression.* != .literal or expression.literal != .string) return expression;
    const cast = try alloc.create(ast.Scalar);
    cast.* = .{ .cast = .{ .operand = expression, .type = .array, .element_type = expected.element_type orelse return error.SqlAssignmentTypeMismatch } };
    return cast;
}

/// Shared assignment coercion for typed source cursors and mutation cells.
/// Identity borrows the source; converted cells belong to alloc, and dimension
/// metadata borrows the pinned source until the caller's ownership boundary.
pub fn assignArray(alloc: Allocator, datum: Datum, target: arrays.ElementType, limits: EvalLimits) !Datum {
    if (datum.value != .null or datum.patterns != null) return error.SqlAssignmentTypeMismatch;
    if (datum.sql_null) {
        if (datum.array != null) return error.SqlAssignmentTypeMismatch;
        return datum;
    }
    const source = datum.array orelse return error.SqlAssignmentTypeMismatch;
    if (!builtin_cast.assignmentAllowed(source.element_type, target)) return error.SqlAssignmentTypeMismatch;
    if (source.element_type == target) return datum;
    // Reuse the exact bounded scalar/array CAST kernel without constructing
    // instructions, serializing JSON, or registering another parameter frame.
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const program: Program = .{ .arena = &arena, .instructions = &.{}, .root = 0, .output_type = .{}, .parameter_types = &.{}, .parameter_descriptors = &.{}, .required_columns = &.{} };
    var evaluator: Evaluator = .{ .program = &program, .alloc = alloc, .cells = &.{}, .parameters = &.{}, .limits = limits };
    return evaluator.castArray(datum, target);
}

fn normalizeParameterType(descriptor: Type) !Type {
    try validateParameterType(descriptor);
    var result = descriptor;
    if (result.kind != null and result.kind != .datetime and result.element_type == null) result.element_type = try parameterElementType(result);
    return result;
}

fn bindDescriptors(alloc: Allocator, expression: *const ast.Scalar, columns: []const Column, parameter_hints: []const Type, expected: ?Type, limits: BindLimits, settings: ?*const setting_catalog.View, typed_parameters: bool) !Program {
    if (limits.parameters > 1024 or parameter_hints.len > limits.parameters or limits.nodes == 0) return error.SqlProgramLimitExceeded;
    if (typed_parameters) for (parameter_hints) |descriptor| try validateParameterType(descriptor);
    if (typed_parameters) if (expected) |descriptor| if (descriptor.kind != .array or descriptor.element_type != null) try validateParameterType(descriptor);
    const arena = try alloc.create(std.heap.ArenaAllocator);
    errdefer alloc.destroy(arena);
    arena.* = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    var binder: Binder = .{ .alloc = arena.allocator(), .columns = columns, .limits = limits, .settings = settings, .typed_parameters = typed_parameters };
    try binder.registerColumns();
    @memcpy(binder.parameters[0..parameter_hints.len], parameter_hints);
    if (typed_parameters) for (binder.parameters[0..parameter_hints.len]) |*descriptor| {
        descriptor.* = try normalizeParameterType(descriptor.*);
    };
    binder.parameter_count = parameter_hints.len;
    const root = try binder.compileArrayContext(expression, if (expected) |kind| kind.kind else null, if (expected) |kind| kind.element_type else null, 0);
    const output_type = binder.instructions.items[root].type;
    const instructions = try binder.instructions.toOwnedSlice(binder.alloc);
    const parameter_descriptors = try binder.alloc.dupe(Type, binder.parameters[0..binder.parameter_count]);
    const parameter_types = try binder.alloc.alloc(?ast.ColumnType, parameter_descriptors.len);
    for (parameter_types, parameter_descriptors) |*kind, descriptor| kind.* = descriptor.kind;
    const required_columns = try binder.dependencies.toOwnedSlice(binder.alloc);
    var program: Program = .{
        .arena = arena,
        .instructions = instructions,
        .root = root,
        .output_type = output_type,
        .parameter_types = parameter_types,
        .parameter_descriptors = parameter_descriptors,
        .required_columns = required_columns,
        .settings = settings,
        .translations = binder.translations,
    };
    try program.prepareConstantArrays(binder.alloc);
    if (limits.invocation) |invocation| try invocation.register(&program);
    program.arena = arena;
    return program;
}

fn numeric(kind: ?ast.ColumnType) bool {
    return kind == .integer or kind == .number;
}

// PostgreSQL resolves array comparison operators on the declared element
// identity; unlike CASE/COALESCE, operator lookup does not promote the arrays.
fn arrayOperator(left: Type, right: Type) !void {
    if (left.kind == .array and right.kind == .array and left.element_type != right.element_type) return error.SqlUndefinedOperator;
}

test "SQL logical JSON parameters and identity casts never parse string payloads again" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "$1", "CAST($1 AS json)", "CAST($1 AS jsonb)" }) |sql| {
        var compiled = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer compiled.deinit();
        var program = try bind(a, compiled.expression, &.{}, &.{.json}, .{});
        defer program.deinit();
        var none = std.heap.FixedBufferAllocator.init(&.{});
        for ([_][]const u8{ "pro", "null", "true", "12", "[1,2]", "{\"x\":1}", "\"quoted\"" }) |text_value| {
            const result = try program.evaluate(none.allocator(), &.{}, &.{.{ .string = text_value }}, .{});
            try std.testing.expect(!result.sql_null);
            try std.testing.expectEqualStrings(text_value, result.value.string);
        }
        try std.testing.expect((try program.evaluate(none.allocator(), &.{}, &.{.null}, .{})).sql_null);
    }
    for ([_][]const u8{ "CAST(j AS json)", "CAST(j AS jsonb)" }) |sql| {
        var compiled = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer compiled.deinit();
        var program = try bind(a, compiled.expression, &.{.{ .name = "j", .type = .json }}, &.{}, .{});
        defer program.deinit();
        var none = std.heap.FixedBufferAllocator.init(&.{});
        const string = try program.evaluate(none.allocator(), &.{Datum.json(.{ .string = "null" })}, &.{}, .{});
        try std.testing.expectEqualStrings("null", string.value.string);
        const json_null = try program.evaluate(none.allocator(), &.{Datum.json(.null)}, &.{}, .{});
        try std.testing.expect(!json_null.sql_null and json_null.value == .null);
    }
}

test "SQL JSONB path replacement matches PostgreSQL values NULLs and errors" {
    const fixture = try std.json.parseFromSlice(struct {
        reference: []const u8,
        entries: []const struct { sql: []const u8, value: Json, sql_null: bool = false },
        errors: []const struct { sql: []const u8, code: []const u8 },
    }, std.testing.allocator, @embedFile("fixtures/sql_json_path_update_reference.json"), .{});
    defer fixture.deinit();
    for (fixture.value.entries) |entry| {
        var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, entry.sql, .{});
        defer compiled.deinit();
        var program = try bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{});
        defer program.deinit();
        try std.testing.expectEqual(ast.ColumnType.json, program.output_type.kind.?);
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const result = try program.evaluate(arena.allocator(), &.{}, &.{}, .{});
        try std.testing.expectEqual(entry.sql_null, result.sql_null);
        try std.testing.expectEqual(std.math.Order.eq, try compare(result.value, entry.value));
    }
    for (fixture.value.errors) |entry| {
        var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, entry.sql, .{});
        defer compiled.deinit();
        var program = bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{}) catch |err| {
            try std.testing.expectEqualStrings(entry.code, @import("errors.zig").describe(err).code);
            continue;
        };
        defer program.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        if (program.evaluate(arena.allocator(), &.{}, &.{}, .{})) |_| return error.ExpectedJsonPathFailure else |err| {
            try std.testing.expectEqualStrings(entry.code, @import("errors.zig").describe(err).code);
        }
    }
}

test "SQL JSONB concatenation matches PostgreSQL typed scalar contracts" {
    const fixture = try std.json.parseFromSlice(struct {
        reference: []const u8,
        entries: []const struct { sql: []const u8, value: Json, sql_null: bool = false },
    }, std.testing.allocator, @embedFile("fixtures/sql_json_concat_reference.json"), .{});
    defer fixture.deinit();
    try std.testing.expectEqual(@as(usize, 20), fixture.value.entries.len);
    for (fixture.value.entries) |entry| {
        var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, entry.sql, .{});
        defer compiled.deinit();
        var program = try bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{});
        defer program.deinit();
        try std.testing.expectEqual(ast.ColumnType.json, program.output_type.kind.?);
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const result = try program.evaluate(arena.allocator(), &.{}, &.{}, .{});
        try std.testing.expectEqual(entry.sql_null, result.sql_null);
        try std.testing.expectEqual(std.math.Order.eq, try compare(result.value, entry.value));
    }
}

test "SQL JSON and typed array containment match PostgreSQL scalar contracts" {
    const fixture = try std.json.parseFromSlice(struct {
        reference: []const u8,
        entries: []const struct { sql: []const u8, value: Json },
    }, std.testing.allocator, @embedFile("fixtures/sql_containment_reference.json"), .{});
    defer fixture.deinit();
    try std.testing.expectEqual(@as(usize, 30), fixture.value.entries.len);
    for (fixture.value.entries) |entry| {
        var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, entry.sql, .{});
        defer compiled.deinit();
        var program = try bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{});
        defer program.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const result = try program.evaluate(arena.allocator(), &.{}, &.{}, .{});
        try std.testing.expectEqual(entry.value == .null, result.sql_null);
        try std.testing.expectEqual(std.math.Order.eq, try compare(result.value, entry.value));
    }
}

test "SQL JSONB existence sets match PostgreSQL typed array semantics" {
    const a = std.testing.allocator;
    const fixture = try std.json.parseFromSlice(struct {
        reference: []const u8,
        entries: []const struct { sql: []const u8, value: Json },
        type_errors: []const []const u8,
    }, a, @embedFile("fixtures/sql_json_exists_reference.json"), .{});
    defer fixture.deinit();
    try std.testing.expectEqual(@as(usize, 22), fixture.value.entries.len);
    for (fixture.value.entries) |entry| {
        var compiled = try @import("compiler.zig").compileScalar(a, entry.sql, .{});
        defer compiled.deinit();
        var program = try bind(a, compiled.expression, &.{}, &.{}, .{});
        defer program.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const result = try program.evaluate(arena.allocator(), &.{}, &.{}, .{});
        try std.testing.expectEqual(entry.value == .null, result.sql_null);
        try std.testing.expectEqual(std.math.Order.eq, try compare(result.value, entry.value));
        try std.testing.expectEqual(ast.ColumnType.boolean, program.output_type.kind.?);
    }
    for (fixture.value.type_errors) |sql| {
        var compiled = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlUndefinedFunction, bind(a, compiled.expression, &.{}, &.{}, .{}));
    }
}

test "SQL JSONB existence sets own prepared keys and bound allocation-free row work" {
    const Faults = struct {
        fn run(a: Allocator) !void {
            var compiled = try @import("compiler.zig").compileScalar(a, "j ?& ARRAY['a','b',NULL]", .{});
            defer compiled.deinit();
            var program = try bind(a, compiled.expression, &.{.{ .name = "j", .type = .json }}, &.{}, .{});
            defer program.deinit();
            try std.testing.expectEqual(@as(usize, 1), program.constant_arrays.count());
            const parsed = try std.json.parseFromSlice(Json, a, "{\"a\":null,\"b\":false}", .{});
            defer parsed.deinit();
            var none = std.heap.FixedBufferAllocator.init(&.{});
            const result = try program.evaluate(none.allocator(), &.{Datum.json(parsed.value)}, &.{}, .{});
            try std.testing.expect(result.value.bool);
            try std.testing.expectError(error.SqlProgramLimitExceeded, program.evaluate(none.allocator(), &.{Datum.json(parsed.value)}, &.{}, .{ .steps = 4 }));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
    try Faults.run(std.testing.allocator);
}

test "SQL JSONB existence parameters infer text arrays and reuse owned frames" {
    const a = std.testing.allocator;
    var compiled = try @import("compiler.zig").compileScalar(a, "$1 ?& $2", .{});
    defer compiled.deinit();
    var program = try bindTyped(a, compiled.expression, &.{}, &.{}, .{});
    defer program.deinit();
    try std.testing.expectEqual(ast.ColumnType.json, program.parameter_descriptors[0].kind.?);
    try std.testing.expectEqual(ast.ColumnType.array, program.parameter_descriptors[1].kind.?);
    try std.testing.expectEqual(arrays.ElementType.text, program.parameter_descriptors[1].element_type.?);
    const json = try std.json.parseFromSlice(Json, a, "{\"a\":null,\"b\":false}", .{});
    defer json.deinit();
    const Frame = @import("parameter_frame.zig").Frame;
    const input = try a.dupe(u8, "[-1:0][2:3]={{a,b},{a,NULL}}");
    defer a.free(input);
    var frame = try Frame.prepare(a, program.parameter_descriptors, &.{ .{ .datum = Datum.json(json.value) }, .{ .text = input } }, .{});
    defer frame.deinit();
    @memset(input, 0);
    const prepared = try program.bindParameters(&frame);
    var none = std.heap.FixedBufferAllocator.init(&.{});
    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..10000) |_| {
        const result = try prepared.evaluate(none.allocator(), &.{}, .{});
        try std.testing.expect(!result.sql_null);
        try std.testing.expect(result.value.bool);
    }
    std.debug.print("SQL JSONB existence parameters: rows=10000 decode_count=1 scratch_bytes=0 elapsed_ns={}\n", .{std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start});
    var null_frame = try Frame.prepare(a, program.parameter_descriptors, &.{ .sql_null, .{ .text = "{}" } }, .{});
    defer null_frame.deinit();
    try std.testing.expect((try (try program.bindParameters(&null_frame)).evaluate(none.allocator(), &.{}, .{})).sql_null);
}

test "SQL typed containment prepares immutable indexes and releases allocation failures" {
    const Faults = struct {
        fn run(a: Allocator) !void {
            var compiled = try @import("compiler.zig").compileScalar(a, "string_to_array($1,$2,$3) @> ARRAY['read']", .{});
            defer compiled.deinit();
            var program = try bind(a, compiled.expression, &.{}, &.{}, .{});
            defer program.deinit();
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const value = try program.evaluate(arena.allocator(), &.{}, &.{ .{ .string = "read,NULL,write" }, .{ .string = "," }, .{ .string = "NULL" } }, .{});
            try std.testing.expectEqual(true, value.value.bool);
            try std.testing.expectError(error.SqlProgramLimitExceeded, program.evaluate(arena.allocator(), &.{}, &.{ .{ .string = "read,NULL,write" }, .{ .string = "," }, .{ .string = "NULL" } }, .{ .steps = 5 }));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
    var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, "string_to_array('read write',' ') @> ARRAY['read']", .{});
    defer compiled.deinit();
    var program = try bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{});
    defer program.deinit();
    try std.testing.expectEqual(@as(usize, 1), program.constant_memberships.count());
    var denied = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..10000) |_| {
        const value = try program.evaluate(denied.allocator(), &.{}, &.{}, .{});
        try std.testing.expect(value.value.bool);
    }
    std.debug.print("SQL prepared containment: rows=10000 elapsed_ns={} row_allocations=0\n", .{std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start});
}

test "SQL predicate modifiers match PostgreSQL values and boolean typing" {
    const alloc = std.testing.allocator;
    const fixture = try std.json.parseFromSlice(struct {
        entries: []const struct { expression: []const u8, expected: Json },
        type_errors: []const []const u8,
    }, alloc, @embedFile("fixtures/sql_predicate_reference.json"), .{});
    defer fixture.deinit();
    try std.testing.expectEqual(@as(usize, 28), fixture.value.entries.len);
    var none = std.heap.FixedBufferAllocator.init(&.{});
    for (fixture.value.entries) |entry| {
        var compiled = try @import("compiler.zig").compileScalar(alloc, entry.expression, .{});
        defer compiled.deinit();
        var program = try bind(alloc, compiled.expression, &.{}, &.{}, .{});
        defer program.deinit();
        const result = try program.evaluate(none.allocator(), &.{}, &.{}, .{});
        try std.testing.expectEqual(entry.expected == .null, result.sql_null);
        try std.testing.expectEqual(std.math.Order.eq, try compare(result.value, entry.expected));
        try std.testing.expectEqual(ast.ColumnType.boolean, program.output_type.kind.?);
    }
    for (fixture.value.type_errors) |expression| {
        var compiled = try @import("compiler.zig").compileScalar(alloc, expression, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlTypeMismatch, bind(alloc, compiled.expression, &.{}, &.{}, .{}));
    }
}

test "SQL predicate modifiers use bounded zero-allocation parameter evaluation and typed vectors" {
    const alloc = std.testing.allocator;
    var expression = try @import("compiler.zig").compileScalar(alloc, "n BETWEEN SYMMETRIC $1 AND $2", .{});
    defer expression.deinit();
    var program = try bind(alloc, expression.expression, &.{.{ .name = "n", .type = .integer }}, &.{ .integer, .integer }, .{});
    defer program.deinit();
    var none = std.heap.FixedBufferAllocator.init(&.{});
    const parameters: []const Json = &.{ .{ .integer = 9 }, .{ .integer = 2 } };
    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    var matched: usize = 0;
    for (0..10000) |i| {
        const result = try program.evaluate(none.allocator(), &.{Datum.json(.{ .integer = @intCast(i % 10) })}, parameters, .{});
        matched += @intFromBool(result.value.bool);
    }
    try std.testing.expectEqual(@as(usize, 8000), matched);
    std.debug.print("SQL symmetric BETWEEN: rows=10000 scratch_bytes=0 elapsed_ns={}\n", .{std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start});
    try std.testing.expectError(error.SqlProgramLimitExceeded, program.evaluate(none.allocator(), &.{Datum.json(.{ .integer = 7 })}, parameters, .{ .steps = 1 }));
    for ([_][]const u8{ "b IS UNKNOWN", "b IS NOT UNKNOWN" }, 0..) |sql, mode| {
        var compiled = try @import("compiler.zig").compileScalar(alloc, sql, .{});
        defer compiled.deinit();
        var boolean = try bind(alloc, compiled.expression, &.{.{ .name = "b", .type = .boolean }}, &.{}, .{});
        defer boolean.deinit();
        try std.testing.expectEqual(if (mode == 0) ast.Scalar.Unary.is_null else .is_not_null, boolean.instructions[boolean.root].operation.unary.op);
        const rows: []const []const Datum = &.{ &.{.{}}, &.{Datum.json(.{ .bool = true })}, &.{Datum.json(.{ .bool = false })} };
        const vector = (try @import("vector_eval.zig").evaluate(alloc, &boolean, rows, &.{})).?;
        defer alloc.free(vector);
        for (rows, vector, 0..) |row, actual, i| {
            const scalar_value = try boolean.evaluate(none.allocator(), row, &.{}, .{});
            try std.testing.expect(!actual.sql_null);
            try std.testing.expectEqual((i == 0) == (mode == 0), actual.value.bool);
            try std.testing.expectEqual(scalar_value.value.bool, actual.value.bool);
        }
    }
}

test "SQL NUMERIC modifiers match PostgreSQL rounding overflow arrays and lazy execution" {
    const a = std.testing.allocator;
    const Entry = struct { sql: []const u8, expected: ?[]const u8 = null, @"error": ?[]const u8 = null, oid: ?u32 = null, typmod: ?i32 = null, prepared_typmod: ?i32 = null };
    const fixture = try std.json.parseFromSlice(struct { reference: []const u8, entries: []const Entry, queries: Json = .null }, a, @embedFile("fixtures/sql_numeric_typmod_reference.json"), .{});
    defer fixture.deinit();
    for (fixture.value.entries) |entry| {
        errdefer std.debug.print("NUMERIC modifier fixture: {s}\n", .{entry.sql});
        var compiled = @import("compiler.zig").compileScalar(a, entry.sql, .{}) catch |err| {
            try std.testing.expectEqualStrings(entry.@"error" orelse return err, @import("errors.zig").describe(err).code);
            continue;
        };
        defer compiled.deinit();
        var program = bind(a, compiled.expression, &.{}, &.{}, .{}) catch |err| {
            try std.testing.expectEqualStrings(entry.@"error" orelse return err, @import("errors.zig").describe(err).code);
            continue;
        };
        defer program.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const result = program.evaluate(arena.allocator(), &.{}, &.{}, .{}) catch |err| {
            try std.testing.expectEqualStrings(entry.@"error" orelse return err, @import("errors.zig").describe(err).code);
            continue;
        };
        try std.testing.expect(entry.@"error" == null);
        try std.testing.expectEqual(arrays.ElementType.numeric, program.output_type.element_type.?);
        try std.testing.expectEqual(entry.prepared_typmod.?, if (program.output_type.numeric_modifier) |m| try m.postgres() else @as(i32, -1));
        try std.testing.expectEqual(@as(u32, if (program.output_type.kind == .array) 1231 else 1700), entry.oid.?);
        if (compiled.expression.* == .cast) {
            const modifier = program.output_type.numeric_modifier;
            try std.testing.expectEqual(entry.typmod.?, if (modifier) |m| try m.postgres() else @as(i32, -1));
        }
        try std.testing.expectEqual(entry.expected == null, result.sql_null);
        if (entry.expected) |expected| {
            if (result.array) |array| {
                var buffer: [16384]u8 = undefined;
                var writer = std.Io.Writer.fixed(&buffer);
                try @import("array_text.zig").encode(array.*, &writer, .{});
                try std.testing.expectEqualStrings(expected, writer.buffered());
            } else {
                var context: @import("numeric_value.zig").Context = .{ .alloc = arena.allocator() };
                try std.testing.expectEqualStrings(expected, try @import("numeric_value.zig").format(&context, result.numeric.?.*));
            }
        }
    }
}

test "SQL NUMERIC modifier dynamic arrays retain bounds and unwind allocation faults" {
    const Faults = struct {
        fn run(a: Allocator, errors: bool) !void {
            var compiled = try @import("compiler.zig").compileScalar(a, "CAST($1 AS numeric(4,2)[])", .{});
            defer compiled.deinit();
            var program = try bind(a, compiled.expression, &.{}, &.{.string}, .{});
            defer program.deinit();
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const result = try program.evaluate(arena.allocator(), &.{}, &.{.{ .string = "[-2:0]={12.345,NULL,-12.345}" }}, .{});
            var buffer: [16384]u8 = undefined;
            var writer = std.Io.Writer.fixed(&buffer);
            try @import("array_text.zig").encode(result.array.?.*, &writer, .{});
            try std.testing.expectEqualStrings("[-2:0]={12.35,NULL,-12.35}", writer.buffered());
            if (!errors) return;
            try std.testing.expectError(error.SqlNumericOutOfRange, program.evaluate(arena.allocator(), &.{}, &.{.{ .string = "{99.995}" }}, .{}));
            try std.testing.expectError(error.SqlProgramLimitExceeded, program.evaluate(arena.allocator(), &.{}, &.{.{ .string = "{12.345}" }}, .{ .steps = 1 }));
            try std.testing.expectError(error.SqlProgramLimitExceeded, program.evaluate(arena.allocator(), &.{}, &.{.{ .string = "{12.345}" }}, .{ .output_bytes = 0 }));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{false});
    try Faults.run(std.testing.allocator, true);
}

test "SQL NUMERIC modifier array work polls cancellation even for null cells" {
    const a = std.testing.allocator;
    var compiled = try @import("compiler.zig").compileScalar(a, "CAST(items AS numeric(4,2)[])", .{});
    defer compiled.deinit();
    var program = try bind(a, compiled.expression, &.{.{ .name = "items", .type = .array, .element_type = .numeric }}, &.{}, .{});
    defer program.deinit();
    const elements: [1024]arrays.Element = @splat(.{});
    var input = try arrays.Value.init(.numeric, &.{.{ .length = elements.len, .lower = -5 }}, &elements, .{});
    const Control = struct {
        calls: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == 2) return error.QueryCanceled;
        }
    };
    var control: Control = .{};
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const cells = [_]Datum{Datum.typedArray(&input)};
    try std.testing.expectError(error.QueryCanceled, program.evaluate(arena.allocator(), &cells, &.{}, .{ .checkpoint = Control.check, .checkpoint_context = &control }));
    try std.testing.expectEqual(@as(usize, 2), control.calls);
    try std.testing.expectError(error.SqlProgramLimitExceeded, program.evaluate(arena.allocator(), &cells, &.{}, .{ .steps = 100 }));
}

test "SQL NUMERIC modifier constants reuse exact results without row allocations" {
    for ([_][]const u8{ "12.345::numeric(4,2)", "ARRAY[12.345,NULL,-12.345]::numeric(4,2)[]" }) |sql| {
        var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var program = try bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{});
        defer program.deinit();
        var buffer: [0]u8 = .{};
        var fixed = std.heap.FixedBufferAllocator.init(&buffer);
        const first = try program.evaluate(fixed.allocator(), &.{}, &.{}, .{});
        for (0..10000) |_| {
            const next = try program.evaluate(fixed.allocator(), &.{}, &.{}, .{});
            try std.testing.expectEqual(first.numeric, next.numeric);
            try std.testing.expectEqual(first.array, next.array);
        }
    }
}

test "SQL bound array expressions match PostgreSQL scalar contracts" {
    const Entry = struct { sql: []const u8, value: Json };
    const fixture = try std.json.parseFromSlice(struct { reference: []const u8, entries: []const Entry }, std.testing.allocator, @embedFile("fixtures/sql_array_expression_reference.json"), .{});
    defer fixture.deinit();
    try std.testing.expectEqual(@as(usize, 164), fixture.value.entries.len);
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

test "SQL multidimensional constructors flatten typed cells and bound dynamic work" {
    const Faults = struct {
        fn run(a: Allocator) !void {
            var compiled = try @import("compiler.zig").compileScalar(a, "ARRAY[[$1,2],[$2,NULL]]::bigint[]", .{});
            defer compiled.deinit();
            var program = try bind(a, compiled.expression, &.{}, &.{ .integer, .integer }, .{});
            defer program.deinit();
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const result = try program.evaluate(arena.allocator(), &.{}, &.{ .{ .integer = 9007199254740993 }, .{ .integer = -9223372036854775807 } }, .{});
            const array = result.array.?;
            try std.testing.expectEqual(arrays.ElementType.int64, array.element_type);
            try std.testing.expectEqual(@as(usize, 2), array.dimensions.len);
            try std.testing.expectEqual(@as(usize, 4), array.elements.len);
            try std.testing.expectEqual(@as(i64, 9007199254740993), array.elements[0].value.integer);
            try std.testing.expectEqual(@as(i64, -9223372036854775807), array.elements[2].value.integer);
            try std.testing.expect(array.elements[3].sql_null);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
    var constant = try @import("compiler.zig").compileScalar(std.testing.allocator, "needle = ANY(ARRAY[[1,2],[3,4]])", .{});
    defer constant.deinit();
    var dynamic = try @import("compiler.zig").compileScalar(std.testing.allocator, "needle = ANY(ARRAY[[$1,2],[$2,4]])", .{});
    defer dynamic.deinit();
    const columns = &.{Column{ .name = "needle", .type = .integer }};
    var cached = try bind(std.testing.allocator, constant.expression, columns, &.{}, .{});
    defer cached.deinit();
    var uncached = try bind(std.testing.allocator, dynamic.expression, columns, &.{ .integer, .integer }, .{});
    defer uncached.deinit();
    var none = std.heap.FixedBufferAllocator.init(&.{});
    var buffer: [4096]u8 = undefined;
    var scratch = std.heap.FixedBufferAllocator.init(&buffer);
    var elapsed: [2]i128 = undefined;
    var counts: [2]usize = .{ 0, 0 };
    var bytes: usize = 0;
    const parameters: []const Json = &.{ .{ .integer = 1 }, .{ .integer = 3 } };
    for ([_]*const Program{ &cached, &uncached }, 0..) |program, mode| {
        const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
        for (0..10000) |row| {
            scratch.reset();
            const result = try program.evaluate(if (mode == 0) none.allocator() else scratch.allocator(), &.{Datum.json(.{ .integer = if (row % 2 == 0) 1 else 9 })}, parameters, .{});
            counts[mode] += @intFromBool(result.value.bool);
            bytes = @max(bytes, scratch.end_index);
        }
        elapsed[mode] = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start;
    }
    try std.testing.expectEqual(@as(usize, 5000), counts[0]);
    try std.testing.expectEqual(counts[0], counts[1]);
    scratch.reset();
    try std.testing.expectError(error.SqlProgramLimitExceeded, uncached.evaluate(scratch.allocator(), &.{Datum.json(.{ .integer = 1 })}, parameters, .{ .output_bytes = 1 }));
    scratch.reset();
    try std.testing.expectError(error.SqlProgramLimitExceeded, uncached.evaluate(scratch.allocator(), &.{Datum.json(.{ .integer = 1 })}, parameters, .{ .steps = 12 }));
    std.debug.print("SQL multidimensional constructors: rows=10000 prepared_ns={} dynamic_ns={} prepared_scratch_bytes=0 dynamic_scratch_bytes={}\n", .{ elapsed[0], elapsed[1], bytes });
}

test "SQL multidimensional column constructors preserve bounds and admit heap scratch" {
    const Faults = struct {
        fn run(a: Allocator) !void {
            var compiled = try @import("compiler.zig").compileScalar(a, "ARRAY[v,v,v,v,v,v,v,v,v,v,v,v,v,v,v,v,v]", .{});
            defer compiled.deinit();
            var program = try bind(a, compiled.expression, &.{.{ .name = "v", .type = .array, .element_type = .text }}, &.{}, .{});
            defer program.deinit();
            const input = try arrays.Value.init(.text, &.{.{ .lower = -1, .length = 1 }}, &.{arrays.Element.json(.{ .string = "é" })}, .{});
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const result = (try program.evaluate(arena.allocator(), &.{Datum.typedArray(&input)}, &.{}, .{})).array.?;
            try std.testing.expectEqual(@as(usize, 17), result.elements.len);
            try std.testing.expectEqual(@as(i32, -1), result.dimensions[1].lower);
            try std.testing.expectEqualStrings("é", result.elements[16].value.string);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
    var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, "ARRAY[a,b]", .{});
    defer compiled.deinit();
    var program = try bind(std.testing.allocator, compiled.expression, &.{ .{ .name = "a", .type = .array, .element_type = .int32 }, .{ .name = "b", .type = .array, .element_type = .int32 } }, &.{}, .{});
    defer program.deinit();
    const cells = &.{arrays.Element.json(.{ .integer = 1 })};
    const lower_zero = try arrays.Value.init(.int32, &.{.{ .lower = 0, .length = 1 }}, cells, .{});
    const lower_one = try arrays.Value.init(.int32, &.{.{ .lower = 1, .length = 1 }}, cells, .{});
    const six = try arrays.Value.init(.int32, &.{ .{ .length = 1 }, .{ .length = 1 }, .{ .length = 1 }, .{ .length = 1 }, .{ .length = 1 }, .{ .length = 1 } }, cells, .{});
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    try std.testing.expectError(error.SqlArraySubscriptError, program.evaluate(arena.allocator(), &.{ Datum.typedArray(&lower_zero), Datum.typedArray(&lower_one) }, &.{}, .{}));
    try std.testing.expectError(error.SqlArraySubscriptError, program.evaluate(arena.allocator(), &.{ Datum{}, Datum.typedArray(&lower_one) }, &.{}, .{}));
    try std.testing.expectError(error.SqlProgramLimitExceeded, program.evaluate(arena.allocator(), &.{ Datum.typedArray(&six), Datum.typedArray(&six) }, &.{}, .{}));
}

test "SQL array casts match PostgreSQL rejection diagnostics" {
    const Entry = struct { sql: []const u8, code: []const u8 };
    const fixture = try std.json.parseFromSlice(struct { reference: []const u8, entries: []const Entry }, std.testing.allocator, @embedFile("fixtures/sql_array_cast_errors.json"), .{});
    defer fixture.deinit();
    try std.testing.expectEqual(@as(usize, 45), fixture.value.entries.len);
    const Check = struct {
        fn evaluate(sql: []const u8) !void {
            var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, sql, .{});
            defer compiled.deinit();
            var program = try bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{});
            defer program.deinit();
            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();
            _ = try program.evaluate(arena.allocator(), &.{}, &.{}, .{});
        }
    };
    for (fixture.value.entries) |entry| {
        Check.evaluate(entry.sql) catch |err| {
            try std.testing.expectEqualStrings(entry.code, @import("errors.zig").describe(err).code);
            continue;
        };
        return error.ExpectedPostgresRejection;
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
        fn constant(backing: Allocator) !void {
            // Arena resize is an optional optimization: make allocation-fault
            // enumeration deterministic by exercising its allocate/copy path.
            var vtable = backing.vtable.*;
            vtable.resize = Allocator.noResize;
            vtable.remap = Allocator.noRemap;
            const a: Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
            var compiled = try @import("compiler.zig").compileScalar(a, "2 = ANY (ARRAY[1, 2, NULL]::smallint[]::bigint[])", .{});
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
    try std.testing.expectEqual(arrays.ElementType.int32, program.output_type.element_type.?);
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
        try std.testing.expectError(if (std.mem.eql(u8, sql, "ARRAY[1] = ARRAY['1']")) error.SqlUndefinedOperator else error.SqlTypeMismatch, bind(std.testing.allocator, invalid.expression, &.{}, &.{}, .{}));
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

test "SQL array pattern quantifiers bound work and avoid hot loop allocations" {
    const Faults = struct {
        fn run(a: Allocator) !void {
            var compiled = try @import("compiler.zig").compileScalar(a, "probe ILIKE ANY(ARRAY[$1, 'open%'])", .{});
            defer compiled.deinit();
            var program = try bind(a, compiled.expression, &.{.{ .name = "probe", .type = .string }}, &.{}, .{});
            defer program.deinit();
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const result = try program.evaluate(arena.allocator(), &.{Datum.json(.{ .string = "READY-value" })}, &.{.{ .string = "ready%" }}, .{});
            try std.testing.expect(result.value.bool);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
    var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, "probe ILIKE ANY(ARRAY['ready%', 'open%'])", .{});
    defer compiled.deinit();
    var program = try bind(std.testing.allocator, compiled.expression, &.{.{ .name = "probe", .type = .string }}, &.{}, .{});
    defer program.deinit();
    try std.testing.expectEqual(@as(usize, 1), program.constant_arrays.count());
    var no_memory = std.heap.FixedBufferAllocator.init(&.{});
    try std.testing.expectError(error.SqlProgramLimitExceeded, program.evaluate(no_memory.allocator(), &.{Datum.json(.{ .string = "READY-value" })}, &.{}, .{ .pattern_steps = 1 }));
    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    var matched: usize = 0;
    for (0..10000) |row| {
        const result = try program.evaluate(no_memory.allocator(), &.{Datum.json(.{ .string = if (row % 2 == 0) "READY-value" else "closed" })}, &.{}, .{});
        matched += @intFromBool(result.value.bool);
    }
    try std.testing.expectEqual(@as(usize, 5000), matched);
    std.debug.print("SQL array pattern quantifiers: rows=10000 scratch_bytes=0 elapsed_ns={}\n", .{std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start});
}

test "SQL text array casts prepare once and bound dynamic decoding allocation" {
    const Faults = struct {
        fn run(a: Allocator) !void {
            var compiled = try @import("compiler.zig").compileScalar(a, "3 = ANY(raw::int4[])", .{});
            defer compiled.deinit();
            var program = try bind(a, compiled.expression, &.{.{ .name = "raw", .type = .string }}, &.{}, .{});
            defer program.deinit();
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            try std.testing.expect((try program.evaluate(arena.allocator(), &.{Datum.json(.{ .string = "{1,2,3,NULL}" })}, &.{}, .{})).value.bool);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
    var constant = try @import("compiler.zig").compileScalar(std.testing.allocator, "probe = ANY('{1,2,3,4}'::integer[])", .{});
    defer constant.deinit();
    var dynamic = try @import("compiler.zig").compileScalar(std.testing.allocator, "probe = ANY(raw::integer[])", .{});
    defer dynamic.deinit();
    const columns = &.{ Column{ .name = "probe", .type = .integer }, Column{ .name = "raw", .type = .string } };
    var cached = try bind(std.testing.allocator, constant.expression, columns, &.{}, .{});
    defer cached.deinit();
    var uncached = try bind(std.testing.allocator, dynamic.expression, columns, &.{}, .{});
    defer uncached.deinit();
    try std.testing.expectEqual(@as(usize, 1), cached.constant_arrays.count());
    var none = std.heap.FixedBufferAllocator.init(&.{});
    var buffer: [4096]u8 = undefined;
    var scratch = std.heap.FixedBufferAllocator.init(&buffer);
    var elapsed: [2]i128 = undefined;
    var counts: [2]usize = .{ 0, 0 };
    var bytes: usize = 0;
    for ([_]*const Program{ &cached, &uncached }, 0..) |program, mode| {
        const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
        for (0..10000) |row| {
            scratch.reset();
            const result = try program.evaluate(if (mode == 0) none.allocator() else scratch.allocator(), &.{ Datum.json(.{ .integer = if (row % 2 == 0) 1 else 9 }), Datum.json(.{ .string = "{1,2,3,4}" }) }, &.{}, .{});
            counts[mode] += @intFromBool(result.value.bool);
            bytes = @max(bytes, scratch.end_index);
        }
        elapsed[mode] = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start;
    }
    try std.testing.expectEqual(@as(usize, 5000), counts[0]);
    try std.testing.expectEqual(counts[0], counts[1]);
    try std.testing.expectError(error.SqlProgramLimitExceeded, uncached.evaluate(scratch.allocator(), &.{ Datum.json(.{ .integer = 1 }), Datum.json(.{ .string = "{1,2,3,4}" }) }, &.{}, .{ .output_bytes = 1 }));
    std.debug.print("SQL text array casts: rows=10000 prepared_ns={} dynamic_ns={} prepared_scratch_bytes=0 dynamic_scratch_bytes={}\n", .{ elapsed[0], elapsed[1], bytes });
}

test "SQL array cast execution preserves bounds and releases allocations on every fault" {
    const Faults = struct {
        fn run(a: Allocator) !void {
            var compiled = try @import("compiler.zig").compileScalar(a, "a::integer[]", .{});
            defer compiled.deinit();
            var program = try bind(a, compiled.expression, &.{.{ .name = "a", .type = .array, .element_type = .jsonb }}, &.{}, .{});
            defer program.deinit();
            const source = try arrays.Value.init(.jsonb, &.{ .{ .length = 2, .lower = -4 }, .{ .length = 2, .lower = 7 } }, &.{ arrays.Element.json(.{ .number_string = "2.5" }), arrays.Element.json(.null), .{}, arrays.Element.json(.{ .number_string = "-2.5" }) }, .{});
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const result = (try program.evaluate(arena.allocator(), &.{Datum.typedArray(&source)}, &.{}, .{})).array.?;
            try std.testing.expectEqualDeep(source.dimensions, result.dimensions);
            try std.testing.expectEqual(arrays.ElementType.int32, result.element_type);
            try std.testing.expectEqual(@as(i64, 3), result.elements[0].value.integer);
            try std.testing.expect(result.elements[1].sql_null);
            try std.testing.expect(result.elements[2].sql_null);
            try std.testing.expectEqual(@as(i64, -3), result.elements[3].value.integer);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
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
        .float32, .float64, .numeric => .number,
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
        if (chosen.element_type == null or (other.kind != null and other.kind != .array)) return error.SqlTypeMismatch;
        var element = chosen.element_type.?;
        if (other.kind == .array and other.element_type != element) {
            const right_element = other.element_type orelse return error.SqlTypeMismatch;
            element = try builtin_cast.commonNumeric(element, right_element);
        }
        return .{ .kind = .array, .element_type = element, .nullable = left.nullable or right.nullable };
    }
    const kind = if (left.kind == null) right.kind else if (right.kind == null) left.kind else if (left.kind == right.kind) left.kind else if ((left.kind == .uuid and right.kind == .string) or (left.kind == .string and right.kind == .uuid)) ast.ColumnType.uuid else if (numeric(left.kind) and numeric(right.kind)) ast.ColumnType.number else return error.SqlTypeMismatch;
    const element: ?arrays.ElementType = if (left.kind == null) right.element_type else if (right.kind == null) left.element_type else if (kind == .integer)
        (if (left.element_type == null or right.element_type == null or left.element_type == .int64 or right.element_type == .int64) .int64 else if (left.element_type == .int32 or right.element_type == .int32) .int32 else .int16)
    else if (kind == .number)
        (if (left.element_type == .float64 or right.element_type == .float64 or (left.kind == .number and left.element_type == null) or (right.kind == .number and right.element_type == null)) .float64 else if (left.element_type == .float32 or right.element_type == .float32) .float32 else if (left.element_type == .numeric or right.element_type == .numeric) .numeric else null)
    else
        left.element_type orelse right.element_type;
    return .{ .kind = kind, .element_type = element, .nullable = left.nullable or right.nullable };
}

/// Typed operator boundaries use the same array conversion and admission as
/// explicit scalar casts. Identity casts borrow; promotions retain SQL NULLs,
/// dimensions and bounds without interpreting the JSON placeholder.
pub fn castArrayDatum(alloc: Allocator, datum: Datum, target: arrays.ElementType, limits: EvalLimits) !Datum {
    var context: Evaluator = .{ .program = undefined, .alloc = alloc, .cells = &.{}, .parameters = &.{}, .limits = limits };
    return context.castArray(datum, target);
}
fn literalType(value: ast.Value) Type {
    return .{ .kind = switch (value) {
        .null, .parameter => null,
        .boolean => .boolean,
        .integer => .integer,
        .number, .numeric => .number,
        .string => .string,
    }, .element_type = if (value == .numeric) .numeric else if (value == .integer) (if (std.math.cast(i32, value.integer) != null) .int32 else .int64) else null, .nullable = value == .null or value == .parameter };
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
fn arrayCompatibleFunction(function: Function) bool {
    return switch (function) {
        .array_position, .array_positions, .array_remove, .array_replace, .array_append, .array_prepend, .array_cat => true,
        else => false,
    };
}
fn arrayArgument(function: Function, index: usize) bool {
    return if (function == .array_cat) true else if (function == .array_prepend) index == 1 else index == 0;
}

test "SQL PostgreSQL array construction preserves ranks bounds NULLs and compatible types" {
    const Case = struct { sql: []const u8 };
    for ([_]Case{
        .{ .sql = "array_append(ARRAY[1,NULL],2) = ARRAY[1,NULL,2]" },
        .{ .sql = "array_prepend(1,'[0:1]={2,3}'::int4[]) = '[0:2]={1,2,3}'::int4[]" },
        .{ .sql = "array_append(NULL::int4[],1) = ARRAY[1]" },
        .{ .sql = "array_prepend(NULL,NULL) = ARRAY[NULL]::text[]" },
        .{ .sql = "array_cat(NULL::int4[],NULL::int4[]) IS NULL" },
        .{ .sql = "array_cat(ARRAY[]::int4[],NULL::int4[]) = ARRAY[]::int4[]" },
        .{ .sql = "array_cat(ARRAY[1],ARRAY[2,3]) = ARRAY[1,2,3]" },
        .{ .sql = "array_cat('[0:0][3:4]={{1,2}}'::int4[],'[9:9][3:4]={{3,4}}'::int4[]) = '[0:1][3:4]={{1,2},{3,4}}'::int4[]" },
        .{ .sql = "array_cat(ARRAY[1,2],'[0:0][1:2]={{3,4}}'::int4[]) = '[0:1][1:2]={{1,2},{3,4}}'::int4[]" },
        .{ .sql = "array_cat('[0:0][1:2]={{1,2}}'::int4[],ARRAY[3,4]) = '[0:1][1:2]={{1,2},{3,4}}'::int4[]" },
        .{ .sql = "array_append(ARRAY[1]::int2[],9007199254740993::int8) = ARRAY[1,9007199254740993]::int8[]" },
        .{ .sql = "array_cat(ARRAY[1]::int2[],ARRAY[9007199254740993]::int8[]) = ARRAY[1,9007199254740993]::int8[]" },
        .{ .sql = "(ARRAY[1] || NULL) = ARRAY[1]" },
        .{ .sql = "(ARRAY[1] || NULL::int4) = ARRAY[1,NULL]" },
        .{ .sql = "(ARRAY[1] || '{2,3}') = ARRAY[1,2,3]" },
        .{ .sql = "(1 || ARRAY[2,3]) = ARRAY[1,2,3]" },
        .{ .sql = "(ARRAY[1,2] || 3) = ARRAY[1,2,3]" },
        .{ .sql = "('x'::text || ARRAY['y']) = ARRAY['x','y']" },
        .{ .sql = "CASE WHEN false THEN cardinality(array_append(ARRAY[[1,2]],3)) = 0 ELSE true END" },
    }) |case| {
        var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var program = try bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{});
        defer program.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const actual = try program.evaluate(arena.allocator(), &.{}, &.{}, .{});
        try std.testing.expect(!actual.sql_null and actual.value == .bool and actual.value.bool);
    }
}

test "SQL PostgreSQL array construction diagnoses incompatible shapes and overloads" {
    for ([_]struct { sql: []const u8, err: anyerror }{
        .{ .sql = "array_append(ARRAY[[1,2]],3)", .err = error.SqlArrayAppendDimensions },
        .{ .sql = "array_cat(ARRAY[[1,2]],ARRAY[[3]])", .err = error.SqlArrayConcatenationDimensions },
        .{ .sql = "array_cat(ARRAY[1],ARRAY[[[2]]])", .err = error.SqlArrayConcatenationDimensions },
        .{ .sql = "array_cat('[0:1]={1,2}'::int4[],ARRAY[[3,4]])", .err = error.SqlArrayConcatenationDimensions },
        .{ .sql = "ARRAY[1,2] || '7'", .err = error.SqlInvalidTextRepresentation },
        .{ .sql = "array_cat(ARRAY[1],ARRAY[true])", .err = error.SqlUndefinedFunction },
        .{ .sql = "array_append('[2147483646:2147483646]={1}'::int4[],2)", .err = error.SqlProgramLimitExceeded },
        .{ .sql = "array_cat('[2147483646:2147483646]={1}'::int4[],ARRAY[2])", .err = error.SqlProgramLimitExceeded },
    }) |case| {
        var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var program = bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{}) catch |err| {
            try std.testing.expectEqual(case.err, err);
            continue;
        };
        defer program.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        try std.testing.expectError(case.err, program.evaluate(arena.allocator(), &.{}, &.{}, .{}));
    }
}

test "SQL PostgreSQL array construction unwinds every allocation failure" {
    const Fixture = struct {
        fn run(a: Allocator) !void {
            var compiled = try @import("compiler.zig").compileScalar(a, "array_prepend('a',array_append(ARRAY['b',NULL],'c')) || ARRAY['d']", .{});
            defer compiled.deinit();
            var program = try bind(a, compiled.expression, &.{}, &.{}, .{});
            defer program.deinit();
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const result = try program.evaluate(arena.allocator(), &.{}, &.{}, .{});
            try std.testing.expectEqual(@as(usize, 5), result.array.?.elements.len);
            try std.testing.expectEqualStrings("a", result.array.?.elements[0].value.string);
            try std.testing.expect(result.array.?.elements[2].sql_null);
            try std.testing.expectError(error.SqlProgramLimitExceeded, program.evaluate(arena.allocator(), &.{}, &.{}, .{ .output_bytes = 1 }));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL PostgreSQL array construction preserves prepared widths and borrows identity results" {
    const a = std.testing.allocator;
    var compiled = try @import("compiler.zig").compileScalar(a, "array_cat($1,$2)", .{});
    defer compiled.deinit();
    const hints: []const Type = &.{ .{ .kind = .array, .element_type = .int16 }, .{ .kind = .array, .element_type = .int64 } };
    var program = try bindTyped(a, compiled.expression, &.{}, hints, .{});
    defer program.deinit();
    try std.testing.expectEqual(arrays.ElementType.int64, program.output_type.element_type.?);
    for (hints, program.parameter_descriptors) |expected, actual| try std.testing.expectEqual(expected.element_type, actual.element_type);
    const Frame = @import("parameter_frame.zig").Frame;
    var frame = try Frame.prepare(a, program.parameter_descriptors, &.{ .{ .text = "[0:1]={1,NULL}" }, .{ .text = "{9007199254740993}" } }, .{});
    defer frame.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const result = try (try program.bindParameters(&frame)).evaluate(arena.allocator(), &.{}, .{});
    try std.testing.expectEqual(@as(i32, 0), result.array.?.dimensions[0].lower);
    try std.testing.expect(result.array.?.elements[1].sql_null);
    try std.testing.expectEqual(@as(i64, 9007199254740993), result.array.?.elements[2].value.integer);

    var identity_sql = try @import("compiler.zig").compileScalar(a, "$1::int4[] || NULL", .{});
    defer identity_sql.deinit();
    var identity = try bindTyped(a, identity_sql.expression, &.{}, &.{}, .{});
    defer identity.deinit();
    var identity_frame = try Frame.prepare(a, identity.parameter_descriptors, &.{.{ .text = "[0:2]={1,NULL,2}" }}, .{});
    defer identity_frame.deinit();
    const prepared = try identity.bindParameters(&identity_frame);
    var none = std.heap.FixedBufferAllocator.init(&.{});
    const started = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..10000) |_| {
        const value = try prepared.evaluate(none.allocator(), &.{}, .{});
        try std.testing.expect(value.array == identity_frame.values[0].array);
    }
    std.debug.print("SQL array concatenation identity: rows=10000 evaluation_scratch_bytes=0 elapsed_ns={}\n", .{std.Io.Clock.now(.awake, std.testing.io).nanoseconds - started});
}

test "SQL PostgreSQL array search and transform preserve typed NULLs bounds and promotion" {
    const Case = struct { sql: []const u8 };
    const Fixture = struct {
        fn run(a: Allocator) !void {
            for ([_]Case{
                .{ .sql = "array_position(ARRAY[1,NULL,2,NULL],NULL) = 2" },
                .{ .sql = "array_position(ARRAY[1,NULL,2,NULL],NULL,3) = 4" },
                .{ .sql = "array_position('[0:2]={4,5,4}'::int4[],4,-9) = 0" },
                .{ .sql = "array_position('[0:2]={4,5,4}'::int4[],4,1) = 2" },
                .{ .sql = "array_position(ARRAY[1,2],3) IS NULL" },
                .{ .sql = "array_position(ARRAY[]::text[],NULL) IS NULL" },
                .{ .sql = "array_position(NULL::int4[],1,NULL) IS NULL" },
                .{ .sql = "array_position(ARRAY[]::int4[],1,NULL) IS NULL" },
                .{ .sql = "array_position(ARRAY[1],1,'1') = 1" },
                .{ .sql = "array_positions(NULL,NULL) IS NULL" },
                .{ .sql = "array_positions('[0:3]={1,NULL,1,NULL}'::int4[],NULL) = ARRAY[1,3]" },
                .{ .sql = "array_positions(ARRAY[1,2],3) = ARRAY[]::int4[]" },
                .{ .sql = "array_positions(NULL::int4[],NULL) IS NULL" },
                .{ .sql = "array_remove('[0:3]={1,NULL,1,2}'::int4[],1) = '[0:1]={NULL,2}'::int4[]" },
                .{ .sql = "array_remove(ARRAY[1,1],1) = ARRAY[]::int4[]" },
                .{ .sql = "array_remove(ARRAY[1,NULL,2],NULL) = ARRAY[1,2]" },
                .{ .sql = "array_replace('[0:1][3:4]={{1,NULL},{1,2}}'::int4[],1,9) = '[0:1][3:4]={{9,NULL},{9,2}}'::int4[]" },
                .{ .sql = "array_replace(ARRAY[1,NULL,2],NULL,9) = ARRAY[1,9,2]" },
                .{ .sql = "array_replace(ARRAY[1,2],1,NULL) = ARRAY[NULL,2]" },
                .{ .sql = "array_replace(ARRAY[1]::int2[],1::int8,9007199254740993::int8) = ARRAY[9007199254740993]::int8[]" },
                .{ .sql = "array_remove(ARRAY['NaN'::float8,1::float8], 'NaN'::float8) = ARRAY[1::float8]" },
                .{ .sql = "array_replace(ARRAY['null'::jsonb,NULL], 'null'::jsonb, '1'::jsonb) = ARRAY['1'::jsonb,NULL]" },
                .{ .sql = "array_remove(ARRAY[1,2], '1') = ARRAY[2]" },
            }) |case| {
                var compiled = try @import("compiler.zig").compileScalar(a, case.sql, .{});
                defer compiled.deinit();
                var program = try bind(a, compiled.expression, &.{}, &.{}, .{});
                defer program.deinit();
                var arena = std.heap.ArenaAllocator.init(a);
                defer arena.deinit();
                const actual = try program.evaluate(arena.allocator(), &.{}, &.{}, .{});
                try std.testing.expect(!actual.sql_null and actual.value == .bool and actual.value.bool);
            }
        }
    };
    try Fixture.run(std.testing.allocator);
}

test "SQL PostgreSQL array search diagnoses dimensions initial position and signatures" {
    for ([_]struct { sql: []const u8, err: anyerror }{
        .{ .sql = "array_position(ARRAY[[1,2]],1)", .err = error.UnsupportedSqlShape },
        .{ .sql = "array_positions(ARRAY[[1,2]],1)", .err = error.UnsupportedSqlShape },
        .{ .sql = "array_remove(ARRAY[[1,2]],1)", .err = error.UnsupportedSqlShape },
        .{ .sql = "array_position(ARRAY[1],1,NULL)", .err = error.SqlNullValueNotAllowed },
        .{ .sql = "array_remove(ARRAY[1],true)", .err = error.SqlUndefinedFunction },
        .{ .sql = "array_position(ARRAY[1],1,1.5::float8)", .err = error.SqlUndefinedFunction },
        .{ .sql = "array_position(ARRAY[1],1,1::bigint)", .err = error.SqlUndefinedFunction },
        .{ .sql = "array_position(ARRAY[[1,2]],1,NULL)", .err = error.UnsupportedSqlShape },
        .{ .sql = "array_positions(ARRAY[1])", .err = error.SqlUndefinedFunction },
    }) |case| {
        var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var program = bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{}) catch |err| {
            try std.testing.expectEqual(case.err, err);
            continue;
        };
        defer program.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        try std.testing.expectError(case.err, program.evaluate(arena.allocator(), &.{}, &.{}, .{}));
    }
}

test "SQL PostgreSQL array search transforms unwind every allocation failure" {
    const Fixture = struct {
        fn run(a: Allocator) !void {
            var compiled = try @import("compiler.zig").compileScalar(a, "array_replace(array_remove(ARRAY['alpha',NULL,'alpha'],NULL), 'alpha','beta') = ARRAY['beta','beta'] AND array_positions(ARRAY[1,2,1],1) = ARRAY[1,3] AND array_position(ARRAY[1,NULL,2],NULL) = 2", .{});
            defer compiled.deinit();
            var program = try bind(a, compiled.expression, &.{}, &.{}, .{});
            defer program.deinit();
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const actual = try program.evaluate(arena.allocator(), &.{}, &.{}, .{});
            try std.testing.expect(!actual.sql_null and actual.value.bool);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL PostgreSQL array search prepared frames keep input widths and bounded hot lookup" {
    const a = std.testing.allocator;
    var compiled = try @import("compiler.zig").compileScalar(a, "array_replace($1,$2,$3)", .{});
    defer compiled.deinit();
    const hints: []const Type = &.{ .{ .kind = .array, .element_type = .int16 }, .{ .kind = .integer, .element_type = .int64 }, .{ .kind = .integer, .element_type = .int64 } };
    var program = try bindTyped(a, compiled.expression, &.{}, hints, .{});
    defer program.deinit();
    try std.testing.expectEqual(arrays.ElementType.int64, program.output_type.element_type.?);
    for (hints, program.parameter_descriptors) |expected, actual| {
        try std.testing.expectEqual(expected.kind, actual.kind);
        try std.testing.expectEqual(expected.element_type, actual.element_type);
    }
    var frame = try @import("parameter_frame.zig").Frame.prepare(a, program.parameter_descriptors, &.{ .{ .text = "[0:2]={1,NULL,2}" }, .{ .text = "1" }, .{ .text = "9007199254740993" } }, .{});
    defer frame.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const prepared = try program.bindParameters(&frame);
    const result = try prepared.evaluate(arena.allocator(), &.{}, .{});
    try std.testing.expectEqual(@as(i32, 0), result.array.?.dimensions[0].lower);
    try std.testing.expectEqual(@as(i64, 9007199254740993), result.array.?.elements[0].value.integer);
    try std.testing.expect(result.array.?.elements[1].sql_null);
    try std.testing.expectError(error.SqlProgramLimitExceeded, prepared.evaluate(arena.allocator(), &.{}, .{ .output_bytes = 1 }));
    try std.testing.expectError(error.SqlProgramLimitExceeded, prepared.evaluate(arena.allocator(), &.{}, .{ .steps = 1 }));

    var lookup_sql = try @import("compiler.zig").compileScalar(a, "array_position(ARRAY[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15], needle)", .{});
    defer lookup_sql.deinit();
    var lookup = try bind(a, lookup_sql.expression, &.{.{ .name = "needle", .type = .integer, .element_type = .int32 }}, &.{}, .{});
    defer lookup.deinit();
    try std.testing.expectEqual(@as(usize, 1), lookup.constant_memberships.count());
    var none = std.heap.FixedBufferAllocator.init(&.{});
    const started = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..10000) |i| {
        const value = try lookup.evaluate(none.allocator(), &.{Datum.json(.{ .integer = @intCast(i % 16) })}, &.{}, .{});
        try std.testing.expectEqual(@as(i64, @intCast(i % 16 + 1)), value.value.integer);
    }
    std.debug.print("SQL array_position retained index: rows=10000 array_cells=16 evaluation_scratch_bytes=0 elapsed_ns={}\n", .{std.Io.Clock.now(.awake, std.testing.io).nanoseconds - started});
}
fn arrayStringScenario(a: Allocator) !void {
    const fixture = try std.json.parseFromSlice(struct { entries: []const struct { expression: []const u8, expected: Json = .null, @"error": ?[]const u8 = null } }, a, @embedFile("fixtures/sql_array_string_reference.json"), .{});
    defer fixture.deinit();
    for (fixture.value.entries) |case| {
        var compiled = try @import("compiler.zig").compileScalar(a, case.expression, .{});
        defer compiled.deinit();
        if (case.@"error") |state| {
            try std.testing.expectEqualStrings("42883", state);
            var unexpected = bind(a, compiled.expression, &.{}, &.{}, .{}) catch |err| switch (err) {
                error.SqlUndefinedOperator => continue,
                else => return err,
            };
            unexpected.deinit();
            return error.TestExpectedError;
        }
        var program = try bind(a, compiled.expression, &.{}, &.{}, .{});
        defer program.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const value = try program.evaluate(arena.allocator(), &.{}, &.{}, .{});
        try std.testing.expectEqual(case.expected == .null, value.sql_null);
        if (case.expected != .null) try std.testing.expectEqualDeep(case.expected, value.value);
    }
}

test "SQL array overlap and string output preserve PostgreSQL builtin element semantics" {
    try arrayStringScenario(std.testing.allocator);
}

test "SQL array overlap and string output unwind every allocation failure" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, arrayStringScenario, .{});
}

test "SQL array string output admits exact bytes and bounded work before retention" {
    const a = std.testing.allocator;
    var compiled = try @import("compiler.zig").compileScalar(a, "array_to_string(a,',','NULL')", .{});
    defer compiled.deinit();
    var program = try bind(a, compiled.expression, &.{.{ .name = "a", .type = .array, .element_type = .text }}, &.{}, .{});
    defer program.deinit();
    const value = try arrays.Value.init(.text, &.{.{ .length = 3, .lower = -1 }}, &.{ Datum.json(.{ .string = "read" }), .{}, Datum.json(.{ .string = "write" }) }, .{});
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, program.evaluate(arena.allocator(), &.{Datum.typedArray(&value)}, &.{}, .{ .output_bytes = 14 }));
    try std.testing.expectError(error.SqlProgramLimitExceeded, program.evaluate(arena.allocator(), &.{Datum.typedArray(&value)}, &.{}, .{ .steps = 4 }));
    const result = try program.evaluate(arena.allocator(), &.{Datum.typedArray(&value)}, &.{}, .{ .output_bytes = 15 });
    try std.testing.expectEqualStrings("read,NULL,write", result.value.string);
}

test "SQL array overlap prepares either constant operand without hot loop allocation" {
    const a = std.testing.allocator;
    var compiled = try @import("compiler.zig").compileScalar(a, "a && ARRAY[1,5,9]", .{});
    defer compiled.deinit();
    var program = try bind(a, compiled.expression, &.{.{ .name = "a", .type = .array, .element_type = .int32 }}, &.{}, .{});
    defer program.deinit();
    try std.testing.expectEqual(@as(usize, 1), program.constant_memberships.count());
    const value = try arrays.Value.init(.int32, &.{.{ .length = 3, .lower = -1 }}, &.{ Datum.json(.{ .integer = 7 }), .{}, Datum.json(.{ .integer = 9 }) }, .{});
    var none = std.heap.FixedBufferAllocator.init(&.{});
    const started = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..10000) |_| {
        const result = try program.evaluate(none.allocator(), &.{Datum.typedArray(&value)}, &.{}, .{});
        try std.testing.expect(result.value.bool);
    }
    std.debug.print("SQL array overlap: rows=10000 constant_cells=3 evaluation_scratch_bytes=0 elapsed_ns={}\n", .{std.Io.Clock.now(.awake, std.testing.io).nanoseconds - started});
}

test "SQL regex cancellation releases the execution lease and permits a clean retry" {
    const a = std.testing.allocator;
    var compiled = try @import("compiler.zig").compileScalar(a, "regexp_replace('abc123','[0-9]+','X','g')", .{});
    defer compiled.deinit();
    var program = try bind(a, compiled.expression, &.{}, &.{}, .{});
    defer program.deinit();
    var owner = @import("regex_execution.zig").init(a, 16 * 1024 * 1024);
    defer owner.deinit();
    const Cancellation = struct {
        canceled: bool = true,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.canceled) return error.QueryCanceled;
        }
    };
    var cancel: Cancellation = .{};
    const limits: EvalLimits = .{ .regex_execution = &owner, .regex_checkpoint = Cancellation.check, .regex_context = &cancel };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    try std.testing.expectError(error.QueryCanceled, program.evaluate(arena.allocator(), &.{}, &.{}, limits));
    try std.testing.expectEqual(@as(usize, 0), owner.snapshot().active);
    cancel.canceled = false;
    const result = try program.evaluate(arena.allocator(), &.{}, &.{}, limits);
    try std.testing.expectEqualStrings("abcX", result.value.string);
    try std.testing.expectEqual(@as(usize, 0), owner.snapshot().active);
}

test "SQL scalar regex functions preserve PostgreSQL values OIDs NULLs and errors" {
    const Golden = struct { format: u32, collation: []const u8, entries: []const struct { id: []const u8, expression: []const u8, value: Json, oid: ?u32, sqlstate: ?[]const u8 } };
    const a = std.testing.allocator;
    const golden = try std.json.parseFromSlice(Golden, a, @embedFile("testdata/regex-postgres.json"), .{});
    defer golden.deinit();
    try std.testing.expectEqual(@as(u32, 1), golden.value.format);
    try std.testing.expectEqualStrings("C", golden.value.collation);
    var session = regex_functions.Session.init(a, .{}, 16 * 1024 * 1024);
    defer session.deinit();
    for (golden.value.entries) |case| {
        errdefer std.debug.print("PostgreSQL scalar regex fixture {s}: {s}\n", .{ case.id, case.expression });
        var compiled = @import("compiler.zig").compileScalar(a, case.expression, .{}) catch |err| {
            try std.testing.expectEqualStrings(case.sqlstate orelse return err, @import("errors.zig").describe(err).code);
            continue;
        };
        defer compiled.deinit();
        var program = bind(a, compiled.expression, &.{}, &.{}, .{}) catch |err| {
            try std.testing.expectEqualStrings(case.sqlstate orelse return err, @import("errors.zig").describe(err).code);
            continue;
        };
        defer program.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const result = program.evaluate(arena.allocator(), &.{}, &.{}, .{ .regex_session = &session }) catch |err| {
            try std.testing.expectEqualStrings(case.sqlstate orelse return err, @import("errors.zig").describe(err).code);
            continue;
        };
        try std.testing.expect(case.sqlstate == null);
        const expected: arrays.ElementType = switch (case.oid.?) {
            16 => .boolean,
            23 => .int32,
            25 => .text,
            else => return error.UnexpectedRegexResultType,
        };
        try std.testing.expectEqual(expected, program.output_type.element_type.?);
        try std.testing.expectEqual(case.value == .null, result.sql_null);
        try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(arena.allocator(), case.value, .{}), try std.json.Stringify.valueAlloc(arena.allocator(), result.value, .{}));
    }
}

fn arity(function: Function, count: usize) !void {
    if (regexFunction(function)) |regex| {
        if (!regex_functions.validArity(regex, count)) return error.SqlUndefinedFunction;
        return;
    }
    const valid = switch (function) {
        .regexp_like, .regexp_count, .regexp_instr, .regexp_substr, .regexp_replace => unreachable,
        .array_position => count == 2 or count == 3,
        .array_positions, .array_remove, .array_append, .array_prepend, .array_cat => count == 2,
        .array_replace => count == 3,
        .@"$like_escape" => count == 4,
        .@"$contains", .@"$overlaps", .jsonb_exists, .jsonb_exists_any, .jsonb_exists_all => count == 2,
        .array_to_string => count == 2 or count == 3,
        .string_to_array => count == 2 or count == 3,
        .jsonb_set => count == 3 or count == 4,
        .@"$array" => true,
        .@"$array_quantified" => count == 4,
        .cardinality, .array_ndims => count == 1,
        .array_length, .array_lower, .array_upper => count == 2,
        .ai_decide, .ai_probability => count == 3,
        .ai_choice, .ai_score => count == 4,
        .abs, .lower, .upper, .length, .octet_length, .ceil, .floor, .sign, .sqrt, .to_timestamp, .current_setting, .to_jsonb, .bit_length, .jsonb_typeof, .reverse, .ascii, .chr => count == 1,
        .round, .trunc => count == 1 or count == 2,
        .concat_ws => count >= 2,
        .jsonb_build_object => count % 2 == 0,
        .jsonb_extract_path_text => count >= 2,
        .nullif, .power, .mod, .starts_with, .strpos, .repeat, .left, .right, .date_part, .date_trunc, .@"$single" => count == 2,
        .@"$pattern_quantified", .@"$array_pattern_quantified" => count == 5,
        .substring, .lpad, .rpad => count == 2 or count == 3,
        .replace, .translate, .split_part => count == 3,
        .overlay => count == 3 or count == 4,
        .trim, .ltrim, .rtrim => count == 1 or count == 2,
        .coalesce, .greatest, .least => count > 0,
        .concat => count > 0,
    };
    if (!valid) return if (arrayCompatibleFunction(function) or function == .jsonb_exists_any or function == .jsonb_exists_all) error.SqlUndefinedFunction else error.InvalidSqlParameters;
}

const Binder = struct {
    alloc: Allocator,
    columns: []const Column,
    limits: BindLimits,
    names: std.StringHashMapUnmanaged(u32) = .empty,
    inferred: std.AutoHashMapUnmanaged(*const ast.Scalar, Type) = .empty,
    explicit_arrays: std.AutoHashMapUnmanaged(*const ast.Scalar, arrays.ElementType) = .empty,
    instructions: std.ArrayList(Instruction) = .empty,
    translations: std.AutoHashMapUnmanaged(u32, *const TextTranslation) = .empty,
    dependencies: std.ArrayList(u32) = .empty,
    parameters: [1024]Type = @splat(.{}),
    typed_parameters: bool = false,
    parameter_count: usize = 0,
    allow_unresolved: bool = false,
    settings: ?*const setting_catalog.View = null,

    fn commonModifier(self: *Binder, args: []const *const ast.Scalar, depth: usize) !?@import("../common/sql_builtin_type.zig").NumericModifier {
        if (args.len == 0) return null;
        const first = (try self.infer(args[0], depth + 1)).numeric_modifier orelse return null;
        for (args[1..]) |arg| {
            if (!@import("../common/sql_builtin_type.zig").NumericModifier.eql(first, (try self.infer(arg, depth + 1)).numeric_modifier)) return null;
        }
        return first;
    }

    /// PostgreSQL anycompatiblearray/anycompatible resolution is distinct
    /// from the exact anyarray identity used by equality operators. Unknown
    /// literal strings adopt the known domain; typed text never does so.
    fn arrayCompatibleElement(self: *Binder, call: anytype, function: Function, depth: usize) anyerror!arrays.ElementType {
        var chosen: ?arrays.ElementType = null;
        for (call.args, 0..) |arg, i| {
            const actual = try self.infer(arg, depth + 1);
            if (function == .array_position and i == 2) {
                const unknown_text = arg.* == .literal and arg.literal == .string;
                if (!unknown_text and actual.kind != null and (actual.kind != .integer or (actual.element_type orelse .int64) == .int64)) return error.SqlUndefinedFunction;
                continue;
            }
            if (actual.kind == null or (arg.* == .literal and arg.literal == .string)) continue;
            if (arrayArgument(function, i) != (actual.kind == .array)) return error.SqlUndefinedFunction;
            const element = actual.element_type orelse if (arrayArgument(function, i)) return error.UnknownSqlArrayType else try arrayElementType(actual.kind.?);
            if (chosen) |prior| {
                if (prior != element) chosen = builtin_cast.commonNumeric(prior, element) catch return error.SqlUndefinedFunction;
            } else chosen = element;
        }
        return chosen orelse .text;
    }

    /// Unknown string/NULL operands choose the array-array overload, as in
    /// PostgreSQL operator resolution. A typed scalar chooses append/prepend.
    /// Keep text and JSON concatenation on their existing scalar path.
    fn arrayConcatenation(self: *Binder, binary: anytype, depth: usize) anyerror!?*const ast.Scalar {
        if (binary.op != .concat) return null;
        const left = try self.infer(binary.left, depth + 1);
        const right = try self.infer(binary.right, depth + 1);
        if (left.kind != .array and right.kind != .array) return null;
        const operand = if (left.kind == .array) binary.right else binary.left;
        const other = if (left.kind == .array) right else left;
        const unknown = other.kind == null or (operand.* == .literal and operand.literal == .string);
        const name = if ((left.kind == .array and right.kind == .array) or unknown) "array_cat" else if (left.kind == .array) "array_append" else "array_prepend";
        const node = try self.alloc.create(ast.Scalar);
        node.* = .{ .call = .{ .name = name, .args = try self.alloc.dupe(*const ast.Scalar, &.{ binary.left, binary.right }) } };
        return node;
    }

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
                const descriptor = self.parameters[value.parameter - 1];
                if (descriptor.kind == .array and !self.typed_parameters) return error.UnsupportedSqlShape;
                if (descriptor.kind == .array and descriptor.element_type == null) return error.UnknownSqlParameterType;
                break :blk descriptor;
            } else literalType(value),
            .column => |name| blk: {
                const column = self.columns[self.names.get(name) orelse return error.UnknownColumn];
                if (column.numeric_modifier) |modifier| {
                    if ((column.type != .number and column.type != .array) or column.element_type != .numeric) return error.InvalidSqlProgram;
                    try modifier.validate();
                }
                if (column.type == .array and column.element_type == null) return error.InvalidSqlProgram;
                break :blk .{ .kind = column.type, .nullable = column.nullable, .element_type = column.element_type orelse (if (column.type == .integer) arrays.ElementType.int64 else if (column.type == .number) arrays.ElementType.float64 else null), .numeric_modifier = column.numeric_modifier };
            },
            .cast => |cast| blk: {
                if (cast.type == .array and cast.element_type == null) return error.InvalidSqlProgram;
                if (cast.numeric_modifier) |modifier| {
                    if ((cast.type != .number and cast.type != .array) or cast.element_type != .numeric) return error.InvalidSqlProgram;
                    try modifier.validate();
                }
                if (cast.type == .array) try self.typeArrayConstructors(cast.operand, cast.element_type.?, depth + 1);
                const source = try self.infer(cast.operand, depth + 1);
                if (cast.coercion == .function and cast.type == .number and source.kind != null) {
                    // An explicit text/date/boolean cast is not an implicit
                    // numeric-function overload. Only unknown string literals
                    // can acquire that argument type during resolution.
                    const unknown = cast.operand.* == .literal and cast.operand.literal == .string;
                    if (!unknown and !numeric(source.kind)) return error.SqlUndefinedFunction;
                }
                if (cast.type == .array and source.kind != null and source.kind != .array and source.kind != .string) return error.SqlTypeMismatch;
                if (cast.type == .array and source.kind == .array and !builtin_cast.allowed(source.element_type.?, cast.element_type.?)) return error.SqlCannotCoerce;
                if (cast.type != .array and cast.element_type != null and source.kind != null and source.kind != .array and source.kind != .datetime) {
                    const source_element = source.element_type orelse try arrayElementType(source.kind.?);
                    if (!builtin_cast.allowed(source_element, cast.element_type.?)) return if (cast.coercion == .function) error.SqlUndefinedFunction else error.SqlCannotCoerce;
                }
                // Default decimal constructors need an exact NUMERIC array
                // representation before narrowing to integers. An explicit
                // real/double array cast supplies floating-point semantics.
                if (cast.type == .array and source.kind == .array and builtin_cast.floating(source.element_type.?) and (builtin_cast.integral(cast.element_type.?) or cast.element_type == .text) and cast.operand.* != .cast and cast.operand.* != .column) return error.UnsupportedSqlShape;
                if (cast.type != .array and source.kind == .array) return error.UnsupportedSqlShape;
                break :blk .{ .kind = cast.type, .element_type = cast.element_type, .numeric_modifier = cast.numeric_modifier, .nullable = source.nullable };
            },
            .unary => |unary| blk: {
                const input = try self.infer(unary.operand, depth + 1);
                switch (unary.op) {
                    .positive, .negative => {
                        if (input.kind != null and !numeric(input.kind)) return error.SqlTypeMismatch;
                        var output = input;
                        output.numeric_modifier = null;
                        break :blk output;
                    },
                    .not, .is_true, .is_not_true, .is_false, .is_not_false, .is_unknown, .is_not_unknown => if (input.kind != null and input.kind != .boolean) return error.SqlTypeMismatch,
                    else => {},
                }
                break :blk .{ .kind = .boolean, .nullable = unary.op == .not and input.nullable };
            },
            .binary => |binary| blk: {
                if (try self.arrayConcatenation(binary, depth)) |call| break :blk try self.infer(call, depth + 1);
                const left = try self.infer(binary.left, depth + 1);
                const right = try self.infer(binary.right, depth + 1);
                try arrayOperator(left, right);
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
                        var result_type = merged;
                        if (merged.kind == .number and (left.element_type == .float32 or right.element_type == .float32) and !(left.element_type == .float32 and right.element_type == .float32)) result_type.element_type = .float64;
                        break :blk result_type;
                    },
                    .concat => {
                        if (merged.kind != null and merged.kind != .string and merged.kind != .json) return error.SqlTypeMismatch;
                        break :blk .{ .kind = merged.kind orelse .string, .nullable = merged.nullable };
                    },
                    .like, .ilike => if (merged.kind != null and merged.kind != .string) return error.SqlTypeMismatch,
                    .@"and", .@"or" => if (merged.kind != null and merged.kind != .boolean) return error.SqlTypeMismatch,
                    else => {},
                }
                break :blk .{ .kind = if (binary.op == .concat) .string else .boolean, .nullable = if (binary.op == .is_distinct or binary.op == .is_not_distinct) false else merged.nullable };
            },
            .call => |call| blk: {
                if (call.subquery != null or call.window != null or call.star or call.distinct or call.filter != null or call.within_group != null) return error.UnsupportedSqlShape;
                if (std.mem.eql(u8, call.name, "$validate")) {
                    if (call.args.len == 0) return error.InvalidSqlParameters;
                    for (call.args) |arg| _ = try self.infer(arg, depth + 1);
                    break :blk .{ .kind = .integer, .nullable = false };
                }
                const function = try functionId(call.name);
                try arity(function, call.args.len);
                if (arrayCompatibleFunction(function)) {
                    const element = try self.arrayCompatibleElement(call, function, depth);
                    break :blk .{ .kind = if (function == .array_position) .integer else .array, .element_type = if (function == .array_position or function == .array_positions) .int32 else element, .nullable = true };
                }
                if (function == .@"$like_escape") {
                    for (call.args, 0..) |arg, i| {
                        const actual = try self.infer(arg, depth + 1);
                        if (actual.kind != null and actual.kind != (if (i == 3) ast.ColumnType.boolean else .string)) return error.SqlTypeMismatch;
                    }
                    break :blk .{ .kind = .boolean, .nullable = true };
                }
                if (function == .string_to_array) {
                    for (call.args) |arg| {
                        const actual = try self.infer(arg, depth + 1);
                        if (actual.kind != null and actual.kind != .string) return error.SqlTypeMismatch;
                    }
                    break :blk .{ .kind = .array, .element_type = .text, .nullable = true };
                }
                if (function == .array_to_string) {
                    const input = try self.infer(call.args[0], depth + 1);
                    if (input.kind != null and (input.kind != .array or input.element_type == null)) return error.SqlTypeMismatch;
                    for (call.args[1..]) |arg| {
                        const actual = try self.infer(arg, depth + 1);
                        if (actual.kind != null and actual.kind != .string) return error.SqlTypeMismatch;
                    }
                    break :blk .{ .kind = .string, .nullable = true };
                }
                if (function == .@"$contains" or function == .@"$overlaps" or function == .jsonb_exists) {
                    const left = try self.infer(call.args[0], depth + 1);
                    const right = try self.infer(call.args[1], depth + 1);
                    if (function == .jsonb_exists) {
                        if ((left.kind != null and left.kind != .json) or (right.kind != null and right.kind != .string)) return error.SqlTypeMismatch;
                    } else {
                        const merged = try common(left, right);
                        if (merged.kind != .array and (function == .@"$overlaps" or merged.kind != .json)) return error.SqlTypeMismatch;
                        if (merged.kind == .array) try arrayOperator(left, right);
                    }
                    break :blk .{ .kind = .boolean, .nullable = left.nullable or right.nullable };
                }
                if (function == .@"$array") {
                    if (call.args.len == 0) return error.UnknownSqlArrayType;
                    var element: Type = .{};
                    var unknown_text = false;
                    for (call.args) |arg| {
                        const actual = try self.infer(arg, depth + 1);
                        // Unadorned string literals have PostgreSQL's unknown
                        // identity, unlike an explicitly typed text value.
                        if (arg.* == .literal and arg.literal == .string) {
                            unknown_text = true;
                            continue;
                        }
                        element = common(element, actual) catch |err| return if (err == error.SqlTypeMismatch) (if (element.kind == .array and actual.kind == .array) error.SqlCannotCoerce else error.SqlArrayConstructorTypeMismatch) else err;
                    }
                    if (element.kind == null and unknown_text) element.kind = .string;
                    if (element.kind == .array) break :blk .{ .kind = .array, .nullable = false, .element_type = element.element_type, .numeric_modifier = try self.commonModifier(call.args, depth) };
                    var kind = try arrayElementType(element.kind orelse .string);
                    if (element.kind == .number and element.element_type != null) kind = element.element_type.?;
                    if (element.kind == .integer) {
                        kind = .int16;
                        for (call.args) |arg| {
                            if (arg.* == .literal and arg.literal == .string) continue;
                            const actual = try self.infer(arg, depth + 1);
                            if (actual.kind == null) continue;
                            const width: arrays.ElementType = if (arg.* == .literal and arg.literal == .integer) (if (std.math.cast(i32, arg.literal.integer) != null) .int32 else .int64) else actual.element_type orelse .int64;
                            if (width == .int64 or (width == .int32 and kind == .int16)) kind = width;
                        }
                    }
                    break :blk .{ .kind = .array, .nullable = false, .element_type = kind, .numeric_modifier = try self.commonModifier(call.args, depth) };
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
                if (function == .jsonb_exists_any or function == .jsonb_exists_all) {
                    var nullable = false;
                    for (call.args, 0..) |arg, i| {
                        const actual = try self.infer(arg, depth + 1);
                        const desired: ast.ColumnType = if (i == 0) .json else .array;
                        if (actual.kind != null and actual.kind != desired and !(arg.* == .literal and arg.literal == .string)) return error.SqlUndefinedFunction;
                        if (i == 1 and actual.kind == .array and actual.element_type != .text) return error.SqlUndefinedFunction;
                        nullable = nullable or actual.nullable;
                    }
                    break :blk .{ .kind = .boolean, .nullable = nullable };
                }
                if (function == .jsonb_set) {
                    for (call.args, 0..) |arg, i| {
                        const actual = try self.infer(arg, depth + 1);
                        const desired: ast.ColumnType = if (i == 1) .array else if (i == 3) .boolean else .json;
                        if (actual.kind != null and actual.kind != desired and !(i < 3 and arg.* == .literal and arg.literal == .string)) return error.SqlTypeMismatch;
                        if (i == 1 and actual.kind == .array and actual.element_type != .text) return error.SqlTypeMismatch;
                    }
                    break :blk .{ .kind = .json, .nullable = true };
                }
                for (call.args) |arg| if ((try self.infer(arg, depth + 1)).kind == .array) {
                    switch (function) {
                        .coalesce, .nullif, .greatest, .least, .@"$single", .@"$array_pattern_quantified" => {},
                        // PostgreSQL has these array overloads, but they need
                        // lossless array-to-JSON/text conversion, not .value
                        // (which intentionally remains JSON null for arrays).
                        .to_jsonb, .jsonb_build_object, .concat, .concat_ws => return error.UnsupportedSqlShape,
                        .sqrt => return error.SqlUndefinedFunction,
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
                if (function == .@"$pattern_quantified" or function == .@"$array_pattern_quantified") {
                    for (call.args, 0..) |arg, index| {
                        const actual = try self.infer(arg, depth + 1);
                        const required: ast.ColumnType = if (index == 0) .string else if (index == 1) (if (function == .@"$array_pattern_quantified") .array else .json) else .boolean;
                        if (function == .@"$array_pattern_quantified" and index == 1 and actual.kind == .array) {
                            if (actual.element_type != .text) return error.SqlUndefinedOperator;
                            continue;
                        }
                        if (actual.kind != null and actual.kind != required) return error.SqlTypeMismatch;
                    }
                    break :blk .{ .kind = .boolean, .nullable = true };
                }
                var merged: Type = .{};
                if ((function == .round or function == .trunc) and call.args.len == 2) {
                    const value = try self.infer(call.args[0], depth + 1);
                    const scale = try self.infer(call.args[1], depth + 1);
                    const unknown_value = call.args[0].* == .literal and call.args[0].literal == .string;
                    const unknown_scale = call.args[1].* == .literal and call.args[1].literal == .string;
                    if (value.kind != null and !unknown_value and (!numeric(value.kind) or (value.element_type != null and builtin_cast.floating(value.element_type.?)))) return error.SqlUndefinedFunction;
                    if (scale.kind != null and !unknown_scale and (scale.kind != .integer or (scale.element_type != null and scale.element_type != .int16 and scale.element_type != .int32))) return error.SqlUndefinedFunction;
                    break :blk .{ .kind = .number, .element_type = .numeric, .nullable = value.nullable or scale.nullable };
                }
                if (regexFunction(function)) |regex| {
                    const replace_start = function == .regexp_replace and (call.args.len > 4 or (call.args.len == 4 and (try self.infer(call.args[3], depth + 1)).kind == .integer));
                    var nullable = function == .regexp_substr;
                    for (call.args, 0..) |arg, i| {
                        const actual = try self.infer(arg, depth + 1);
                        const desired: ast.ColumnType = if (regex_functions.argument(regex, call.args.len, i, replace_start) == .integer) .integer else .string;
                        const unknown = arg.* == .literal and arg.literal == .string;
                        if (actual.kind != null and actual.kind != desired and !unknown) return error.SqlUndefinedFunction;
                        if (desired == .integer and actual.kind == .integer) {
                            if (arg.* == .literal and arg.literal == .integer) {
                                if (std.math.cast(i32, arg.literal.integer) == null) return error.SqlUndefinedFunction;
                            } else if (actual.element_type != null and actual.element_type != .int16 and actual.element_type != .int32) return error.SqlUndefinedFunction;
                        }
                        nullable = nullable or actual.nullable;
                    }
                    break :blk .{ .kind = switch (regex) {
                        .regexp_like => .boolean,
                        .regexp_count, .regexp_instr => .integer,
                        else => .string,
                    }, .element_type = switch (regex) {
                        .regexp_like => .boolean,
                        .regexp_count, .regexp_instr => .int32,
                        else => .text,
                    }, .nullable = nullable };
                }
                if (function == .nullif) try arrayOperator(try self.infer(call.args[0], depth + 1), try self.infer(call.args[1], depth + 1));
                switch (function) {
                    .@"$single" => merged = try self.infer(call.args[0], depth + 1),
                    .coalesce, .nullif, .greatest, .least, .abs, .ceil, .floor, .round, .trunc, .sign, .sqrt, .power, .mod => for (call.args) |arg| {
                        merged = try common(merged, try self.infer(arg, depth + 1));
                    },
                    else => for (call.args) |arg| {
                        _ = try self.infer(arg, depth + 1);
                    },
                }
                // These constructs select their own result type before an
                // enclosing expression can supply a coercion context.
                if (merged.kind == null) switch (function) {
                    .coalesce, .nullif, .greatest, .least => merged.kind = .string,
                    else => {},
                };
                if (function == .sqrt and merged.kind != null and !numeric(merged.kind)) {
                    const arg = call.args[0];
                    if (arg.* != .literal or arg.literal != .string) return error.SqlUndefinedFunction;
                    merged = .{ .kind = .number, .element_type = .float64 };
                }
                if (function == .abs or function == .ceil or function == .floor or function == .round or function == .trunc or function == .sign or function == .sqrt or function == .power or function == .mod) {
                    if (merged.kind != null and !numeric(merged.kind)) return error.SqlTypeMismatch;
                }
                switch (function) {
                    .ceil, .floor, .round, .trunc, .sign, .sqrt => if (merged.element_type != .numeric) {
                        // PostgreSQL has NUMERIC and double-precision overloads,
                        // not integer or real overloads, for these functions.
                        merged.kind = .number;
                        merged.element_type = .float64;
                    },
                    else => {},
                }
                break :blk .{ .element_type = switch (function) {
                    .power, .date_part => .float64,
                    .sqrt => merged.element_type,
                    .length, .octet_length, .bit_length, .strpos, .ascii => .int32,
                    .starts_with => .boolean,
                    .to_jsonb, .jsonb_build_object => .jsonb,
                    .date_trunc, .to_timestamp => null,
                    .coalesce, .nullif, .greatest, .least, .abs, .ceil, .floor, .round, .trunc, .sign, .mod, .@"$single" => merged.element_type,
                    else => .text,
                }, .kind = switch (function) {
                    .to_jsonb, .jsonb_build_object => .json,
                    .length, .octet_length, .bit_length, .strpos, .ascii => .integer,
                    .starts_with, .@"$pattern_quantified" => .boolean,
                    .sqrt, .power, .date_part => .number,
                    .date_trunc, .to_timestamp => .datetime,
                    .coalesce, .nullif, .greatest, .least, .abs, .ceil, .floor, .round, .trunc, .sign, .mod, .@"$single" => merged.kind,
                    else => .string,
                }, .numeric_modifier = switch (function) {
                    .coalesce, .greatest, .least => try self.commonModifier(call.args, depth),
                    .nullif, .@"$single" => if (merged.element_type == .numeric)
                        (try self.infer(call.args[0], depth + 1)).numeric_modifier
                    else
                        null,
                    else => null,
                }, .nullable = function != .concat and function != .jsonb_build_object };
            },
            .case_when => |case| blk: {
                var merged: Type = if (case.otherwise) |other| try self.infer(other, depth + 1) else .{};
                var modifier = merged.numeric_modifier;
                for (case.branches) |branch| {
                    const condition = try self.infer(branch.condition, depth + 1);
                    if (condition.kind != null and condition.kind != .boolean) return error.SqlTypeMismatch;
                    const actual = try self.infer(branch.value, depth + 1);
                    if (!@import("../common/sql_builtin_type.zig").NumericModifier.eql(modifier, actual.numeric_modifier)) modifier = null;
                    merged = try common(merged, actual);
                }
                if (merged.kind == null) merged.kind = .string;
                merged.numeric_modifier = modifier;
                break :blk merged;
            },
            .in_list => |list| blk: {
                var merged = try self.infer(list.operand, depth + 1);
                for (list.values) |item| {
                    const item_type = try self.infer(item, depth + 1);
                    try arrayOperator(merged, item_type);
                    merged = try common(merged, item_type);
                }
                break :blk .{ .kind = .boolean, .nullable = merged.nullable };
            },
        };
        try self.inferred.put(self.alloc, expression, result);
        return result;
    }

    /// An explicit constructor cast resolves/coerces its elements directly,
    /// before independent child defaults (notably NULL/text) are selected.
    fn typeArrayConstructors(self: *Binder, expression: *const ast.Scalar, element: arrays.ElementType, depth: usize) anyerror!void {
        if (depth >= self.limits.depth) return error.SqlProgramLimitExceeded;
        if (expression.* != .call or !std.mem.eql(u8, expression.call.name, "$array")) return;
        if (self.inferred.count() >= self.limits.nodes) return error.SqlProgramLimitExceeded;
        for (expression.call.args) |arg| {
            try self.typeArrayConstructors(arg, element, depth + 1);
            const actual = try self.infer(arg, depth + 1);
            if (actual.kind) |kind| {
                const source = actual.element_type orelse try arrayElementType(kind);
                if (!builtin_cast.allowed(source, element)) return error.SqlCannotCoerce;
                if (kind == .number and actual.element_type == null and (builtin_cast.integral(element) or element == .text)) return error.UnsupportedSqlShape;
            }
        }
        try self.explicit_arrays.put(self.alloc, expression, element);
        try self.inferred.put(self.alloc, expression, .{ .kind = .array, .nullable = false, .element_type = element });
    }

    fn compile(self: *Binder, expression: *const ast.Scalar, expected: ?ast.ColumnType, depth: usize) anyerror!u32 {
        return self.compileArrayContext(expression, expected, null, depth);
    }

    fn arrayChildren(self: *const Binder, args: []const *const ast.Scalar) bool {
        for (args) |arg| if (self.inferred.get(arg)) |kind| {
            if (kind.kind == .array) return true;
        };
        return false;
    }

    fn compileArrayContext(self: *Binder, expression: *const ast.Scalar, expected: ?ast.ColumnType, array_element: ?arrays.ElementType, depth: usize) anyerror!u32 {
        if (depth >= self.limits.depth or self.instructions.items.len >= self.limits.nodes) return error.SqlProgramLimitExceeded;
        if (expression.* == .cast and expression.cast.type == .number and expression.cast.element_type != null and builtin_cast.floating(expression.cast.element_type.?)) {
            const operand = expression.cast.operand;
            if (operand.* == .literal and operand.literal == .numeric)
                return self.compileArrayContext(operand, .number, expression.cast.element_type, depth + 1);
        }
        const empty_constructor = expression.* == .call and std.mem.eql(u8, expression.call.name, "$array") and expression.call.args.len == 0;
        if (depth == 0 and empty_constructor and self.limits.assignment) return error.UnknownSqlArrayType;
        var kind = if (empty_constructor and array_element != null) Type{ .kind = .array, .element_type = array_element, .nullable = false } else try self.infer(expression, depth);
        if (expected == .number and array_element == .numeric and (kind.kind == .integer or (expression.* == .literal and expression.literal == .string))) kind = .{ .kind = .number, .element_type = .numeric, .nullable = kind.nullable };
        if (expression.* == .literal and expression.literal == .parameter) kind = self.parameters[expression.literal.parameter - 1];
        if (kind.kind == null) kind.kind = expected;
        if (self.typed_parameters and expression.* == .literal and expression.literal == .parameter and self.parameters[expression.literal.parameter - 1].kind == null) kind.element_type = array_element;
        if (kind.kind == .array and kind.element_type == null) kind.element_type = array_element;
        if (expected == .uuid and expression.* == .literal and expression.literal == .string) kind.kind = .uuid;
        var instruction: Instruction = .{ .type = kind, .operation = undefined };
        if (expression.* == .literal and expression.literal == .numeric and expected == .number and array_element != null and builtin_cast.floating(array_element.?)) {
            // Resolve a proven floating-domain constant once, retaining vector
            // eligibility without converting any NUMERIC-domain expression.
            var work: arrays.Budget = .{};
            const value = try numericTextLeaky(self.alloc, expression.literal.numeric, &work);
            var evaluator: Evaluator = .{ .program = undefined, .alloc = self.alloc, .cells = &.{}, .parameters = &.{}, .limits = .{} };
            const converted = try evaluator.castDatumBuiltin(value, .numeric, array_element.?);
            instruction.type = .{ .kind = .number, .element_type = array_element, .nullable = false };
            instruction.operation = .{ .literal = converted.value };
            try self.instructions.append(self.alloc, instruction);
            return @intCast(self.instructions.items.len - 1);
        }
        instruction.operation = switch (expression.*) {
            .literal => |value| if (value == .parameter) blk: {
                if (kind.kind == .array and !self.typed_parameters) return error.UnsupportedSqlShape;
                if (kind.kind == .array and kind.element_type == null) return error.UnknownSqlParameterType;
                if (self.typed_parameters) kind = try normalizeParameterType(kind);
                instruction.type = kind;
                const resolved = kind.kind;
                if (resolved == null and !self.allow_unresolved) return error.UnknownSqlParameterType;
                const index = value.parameter - 1;
                if (self.parameters[index].kind) |prior| {
                    if (resolved != null and prior != resolved.?) return error.ConflictingSqlParameterTypes;
                }
                if (resolved != null) self.parameters[index] = kind;
                self.parameter_count = @max(self.parameter_count, value.parameter);
                break :blk .{ .parameter = index };
            } else .{ .literal = switch (value) {
                .null => .null,
                .boolean => |v| .{ .bool = v },
                .integer => |v| .{ .integer = v },
                .number => |v| .{ .float = v },
                .numeric => |v| .{ .number_string = try self.alloc.dupe(u8, v) },
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
            .cast => |cast| .{ .cast = .{ .operand = if (cast.type == .array) try self.compileArrayContext(cast.operand, if (self.typed_parameters and cast.operand.* == .literal and cast.operand.literal == .parameter and self.parameters[cast.operand.literal.parameter - 1].kind == null) .array else null, cast.element_type, depth + 1) else try self.compileArrayContext(cast.operand, cast.type, if (self.typed_parameters) cast.element_type else null, depth + 1), .type = cast.type, .element_type = cast.element_type, .numeric_modifier = cast.numeric_modifier } },
            // UNKNOWN is a boolean-only spelling of a null test. Validate its
            // operand first, then emit existing VM opcodes so persisted policy
            // DAGs do not require a new runtime capability for this syntax.
            .unary => |unary| .{ .unary = .{ .op = switch (unary.op) {
                .is_unknown => .is_null,
                .is_not_unknown => .is_not_null,
                else => unary.op,
            }, .operand = try self.compile(unary.operand, switch (unary.op) {
                .not, .is_true, .is_not_true, .is_false, .is_not_false, .is_unknown, .is_not_unknown => .boolean,
                .positive, .negative => kind.kind,
                else => null,
            }, depth + 1) } },
            .binary => |binary| blk: {
                if (try self.arrayConcatenation(binary, depth)) |call| return self.compileArrayContext(call, expected, array_element, depth + 1);
                if (binary.op == .json_get or binary.op == .json_text) break :blk .{ .binary = .{ .op = binary.op, .left = try self.compile(binary.left, .json, depth + 1), .right = try self.compile(binary.right, .string, depth + 1) } };
                const left_type = try self.infer(binary.left, depth + 1);
                const right_type = try self.infer(binary.right, depth + 1);
                var merged = try common(left_type, right_type);
                // PostgreSQL's mixed NUMERIC/real operators use float8;
                // CASE/COALESCE and array common types still select real.
                if (merged.kind == .number and merged.element_type == .float32 and
                    (left_type.element_type == .numeric or right_type.element_type == .numeric)) merged.element_type = .float64;
                const operand_kind: ?ast.ColumnType = switch (binary.op) {
                    .@"and", .@"or" => .boolean,
                    .concat => merged.kind orelse .string,
                    .like, .ilike => .string,
                    .add, .subtract, .multiply, .divide, .modulo => merged.kind orelse expected,
                    else => merged.kind,
                };
                break :blk .{ .binary = .{ .op = binary.op, .left = try self.compileArrayContext(binary.left, operand_kind, merged.element_type, depth + 1), .right = try self.compileArrayContext(binary.right, operand_kind, merged.element_type, depth + 1) } };
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
                        .typed_parameters = self.typed_parameters,
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
                if ((function == .round or function == .trunc) and call.args.len == 2) {
                    for (call.args, args, 0..) |arg, *out, i| {
                        const target: arrays.ElementType = if (i == 0) .numeric else .int32;
                        const desired: ast.ColumnType = if (i == 0) .number else .integer;
                        const coercion = try self.alloc.create(ast.Scalar);
                        coercion.* = .{ .cast = .{ .operand = arg, .type = desired, .element_type = target } };
                        out.* = try self.compileArrayContext(coercion, desired, target, depth + 1);
                    }
                    break :blk .{ .call = .{ .function = function, .args = args } };
                }
                if (regexFunction(function)) |regex| {
                    const replace_start = function == .regexp_replace and (call.args.len > 4 or (call.args.len == 4 and (try self.infer(call.args[3], depth + 1)).kind == .integer));
                    for (call.args, args, 0..) |arg, *out, i| {
                        const integer = regex_functions.argument(regex, call.args.len, i, replace_start) == .integer;
                        const desired: ast.ColumnType = if (integer) .integer else .string;
                        const element: arrays.ElementType = if (integer) .int32 else .text;
                        if (integer and arg.* == .literal and arg.literal == .string) {
                            const coercion = try self.alloc.create(ast.Scalar);
                            coercion.* = .{ .cast = .{ .operand = arg, .type = .integer, .element_type = .int32 } };
                            out.* = try self.compileArrayContext(coercion, desired, element, depth + 1);
                        } else out.* = try self.compileArrayContext(arg, desired, element, depth + 1);
                    }
                    break :blk .{ .call = .{ .function = function, .args = args } };
                }
                if (kind.element_type == .float64) switch (function) {
                    .ceil, .floor, .round, .trunc, .sign, .sqrt => {
                        const coercion = try self.alloc.create(ast.Scalar);
                        coercion.* = .{ .cast = .{ .operand = call.args[0], .type = .number, .element_type = .float64 } };
                        args[0] = try self.compileArrayContext(coercion, .number, .float64, depth + 1);
                        break :blk .{ .call = .{ .function = function, .args = args } };
                    },
                    else => {},
                };
                if (arrayCompatibleFunction(function)) {
                    const element = try self.arrayCompatibleElement(call, function, depth);
                    for (call.args, args, 0..) |arg, *out, i| {
                        const start = function == .array_position and i == 2;
                        const target: arrays.ElementType = if (start) .int32 else element;
                        const desired: ast.ColumnType = if (arrayArgument(function, i)) .array else arrayScalarType(target);
                        const coercion = try self.alloc.create(ast.Scalar);
                        coercion.* = .{ .cast = .{ .operand = arg, .type = desired, .element_type = target } };
                        out.* = try self.compileArrayContext(coercion, desired, target, depth + 1);
                    }
                    break :blk .{ .call = .{ .function = function, .args = args } };
                }
                const nested_constructor = function == .@"$array" and self.arrayChildren(call.args);
                for (call.args, args, 0..) |arg, *out, i| {
                    const desired: ?ast.ColumnType = switch (function) {
                        .@"$like_escape" => if (i == 3) .boolean else .string,
                        .string_to_array => .string,
                        .jsonb_exists => if (i == 0) .json else .string,
                        .jsonb_exists_any, .jsonb_exists_all => if (i == 0) .json else .array,
                        .jsonb_set => if (i == 1) .array else if (i == 3) .boolean else .json,
                        .@"$contains", .@"$overlaps" => (try common(try self.infer(call.args[0], depth + 1), try self.infer(call.args[1], depth + 1))).kind,
                        .array_to_string => if (i == 0) .array else .string,
                        .@"$array" => if (nested_constructor) .array else arrayScalarType(kind.element_type orelse return error.InvalidSqlProgram),
                        .@"$array_quantified" => if (i == 0) (if ((try self.infer(arg, depth + 1)).kind == .number) .number else arrayScalarType((try self.infer(call.args[1], depth + 1)).element_type.?)) else if (i == 1) .array else if (i == 2) .integer else .boolean,
                        .cardinality, .array_ndims, .array_length, .array_lower, .array_upper => if (i == 0) .array else .integer,
                        .ai_decide, .ai_choice, .ai_score, .ai_probability => if ((try self.infer(arg, depth + 1)).kind == .json) .json else .string,
                        .@"$single" => if (i == 0) kind.kind else .integer,
                        .@"$pattern_quantified" => if (i == 0) .string else if (i == 1) .json else .boolean,
                        .@"$array_pattern_quantified" => if (i == 0) .string else if (i == 1) .array else .boolean,
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
                    if (((function == .jsonb_set and i < 3) or function == .jsonb_exists_any or function == .jsonb_exists_all) and arg.* == .literal and arg.literal == .string) {
                        const coercion = try self.alloc.create(ast.Scalar);
                        coercion.* = .{ .cast = .{ .operand = arg, .type = desired.?, .element_type = if (i == 1) .text else null } };
                        out.* = try self.compileArrayContext(coercion, desired, if (i == 1) .text else null, depth + 1);
                        continue;
                    }
                    if (function == .@"$array" and self.explicit_arrays.contains(expression) and actual.kind != null) {
                        const target = kind.element_type orelse return error.InvalidSqlProgram;
                        const coercion = try self.alloc.create(ast.Scalar);
                        coercion.* = .{ .cast = .{ .operand = arg, .type = if (nested_constructor) .array else arrayScalarType(target), .element_type = target } };
                        out.* = try self.compileArrayContext(coercion, desired, target, depth + 1);
                        continue;
                    }
                    if (function == .@"$array" and arg.* == .literal and arg.literal == .string) {
                        if (nested_constructor) {
                            const coercion = try self.alloc.create(ast.Scalar);
                            coercion.* = .{ .cast = .{ .operand = arg, .type = .array, .element_type = kind.element_type } };
                            out.* = try self.compileArrayContext(coercion, .array, kind.element_type, depth + 1);
                        } else out.* = try self.compileArrayContext(arg, desired, kind.element_type, depth + 1);
                        continue;
                    }
                    if (desired != null and actual.kind != null and desired != actual.kind and !(desired == .datetime and actual.kind == .string) and !(desired == .uuid and actual.kind == .string and uuidTextOperand(arg)) and !(numeric(desired) and numeric(actual.kind))) return error.SqlTypeMismatch;
                    const element_context: ?arrays.ElementType = if (!self.typed_parameters) (if (kind.kind == .array) kind.element_type else null) else switch (function) {
                        .jsonb_exists_any, .jsonb_exists_all => if (i == 1) .text else null,
                        .jsonb_set => if (i == 1) .text else null,
                        .@"$contains", .@"$overlaps" => (try common(try self.infer(call.args[0], depth + 1), try self.infer(call.args[1], depth + 1))).element_type,
                        .array_to_string => if (i == 0) (try self.infer(arg, depth + 1)).element_type else .text,
                        .@"$array_quantified" => if (i < 2) (try self.infer(call.args[1], depth + 1)).element_type else if (i == 2) .int32 else .boolean,
                        .@"$array_pattern_quantified" => if (i < 2) .text else .boolean,
                        .cardinality, .array_ndims, .array_length, .array_lower, .array_upper => if (i == 0) (try self.infer(arg, depth + 1)).element_type else .int32,
                        else => if (kind.kind == .array or desired == kind.kind) kind.element_type else null,
                    };
                    out.* = try self.compileArrayContext(arg, desired, element_context, depth + 1);
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
                    out.* = .{ .condition = try self.compile(branch.condition, .boolean, depth + 1), .value = try self.compileArrayContext(branch.value, kind.kind, kind.element_type, depth + 1) };
                }
                if (case.otherwise) |other| if (kind.kind == .uuid and (try self.infer(other, depth + 1)).kind == .string and !uuidTextOperand(other)) return error.SqlTypeMismatch;
                break :blk .{ .case_when = .{ .branches = branches, .otherwise = if (case.otherwise) |other| try self.compileArrayContext(other, kind.kind, kind.element_type, depth + 1) else null } };
            },
            .in_list => |list| blk: {
                var merged = try self.infer(list.operand, depth + 1);
                for (list.values) |item| {
                    const item_type = try self.infer(item, depth + 1);
                    const numeric_operand = merged.element_type == .numeric or item_type.element_type == .numeric;
                    merged = try common(merged, item_type);
                    if (numeric_operand and merged.kind == .number and merged.element_type == .float32) merged.element_type = .float64;
                }
                if (merged.kind == .uuid) {
                    if ((try self.infer(list.operand, depth + 1)).kind == .string and !uuidTextOperand(list.operand)) return error.SqlTypeMismatch;
                    for (list.values) |item| if ((try self.infer(item, depth + 1)).kind == .string and !uuidTextOperand(item)) return error.SqlTypeMismatch;
                }
                const values = try self.alloc.alloc(u32, list.values.len);
                for (list.values, values) |item, *out| out.* = try self.compileArrayContext(item, merged.kind, merged.element_type, depth + 1);
                break :blk .{ .in_list = .{ .operand = try self.compileArrayContext(list.operand, merged.kind, merged.element_type, depth + 1), .values = values, .negated = list.negated } };
            },
        };
        if (self.instructions.items.len >= self.limits.nodes) return error.SqlProgramLimitExceeded;
        const index: u32 = @intCast(self.instructions.items.len);
        try self.instructions.append(self.alloc, instruction);
        if (kind.element_type == .numeric and expected == .number and
            (array_element == .float32 or array_element == .float64))
        {
            if (self.instructions.items.len >= self.limits.nodes) return error.SqlProgramLimitExceeded;
            const promoted: u32 = @intCast(self.instructions.items.len);
            try self.instructions.append(self.alloc, .{
                .type = .{ .kind = .number, .element_type = array_element, .nullable = kind.nullable },
                .operation = .{ .cast = .{ .operand = index, .type = .number, .element_type = array_element } },
            });
            return promoted;
        }
        if (expected == .array and array_element != null and kind.kind == .array and kind.element_type != array_element) {
            if (depth == 0 and self.limits.assignment and !builtin_cast.assignmentAllowed(kind.element_type orelse return error.SqlAssignmentTypeMismatch, array_element.?)) return error.SqlAssignmentTypeMismatch;
            if (self.instructions.items.len >= self.limits.nodes) return error.SqlProgramLimitExceeded;
            const promoted: u32 = @intCast(self.instructions.items.len);
            try self.instructions.append(self.alloc, .{ .type = .{ .kind = .array, .element_type = array_element, .nullable = kind.nullable }, .operation = .{ .cast = .{ .operand = index, .type = .array, .element_type = array_element } } });
            return promoted;
        }
        return index;
    }
};

test "SQL array assignment casts match the PostgreSQL builtin matrix without relaxing explicit CAST" {
    const names = [_][]const u8{ "smallint", "integer", "bigint", "real", "double precision", "boolean", "text", "uuid", "jsonb" };
    const kinds = [_]arrays.ElementType{ .int16, .int32, .int64, .float32, .float64, .boolean, .text, .uuid, .jsonb };
    for (kinds, names, 0..) |source, name, source_index| {
        const text = try std.fmt.allocPrint(std.testing.allocator, "SELECT NULL::{s}[]", .{name});
        defer std.testing.allocator.free(text);
        var compiled = try @import("compiler.zig").compile(std.testing.allocator, text, .{});
        defer compiled.deinit();
        const expression = compiled.statement.select.columns[0].expression.?;
        for (kinds, 0..) |target, target_index| {
            const allowed = source_index == target_index or target_index == 6 or (source_index < 5 and target_index < 5);
            try std.testing.expectEqual(allowed, builtin_cast.assignmentAllowed(source, target));
            const expected: Type = .{ .kind = .array, .element_type = target };
            if (allowed) {
                var program = try bindTypedExpectedWithSettings(std.testing.allocator, expression, &.{}, &.{}, expected, .{ .assignment = true }, null);
                defer program.deinit();
                try std.testing.expectEqual(target, program.output_type.element_type.?);
            } else try std.testing.expectError(error.SqlAssignmentTypeMismatch, bindTypedExpectedWithSettings(std.testing.allocator, expression, &.{}, &.{}, expected, .{ .assignment = true }, null));
        }
    }
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var empty = try @import("compiler.zig").compile(std.testing.allocator, "SELECT ARRAY[]", .{});
    defer empty.deinit();
    try std.testing.expectError(error.UnknownSqlArrayType, bindTypedExpectedWithSettings(std.testing.allocator, empty.statement.select.columns[0].expression.?, &.{}, &.{}, .{ .kind = .array, .element_type = .int64 }, .{ .assignment = true }, null));
    var input = try @import("array_text.zig").decode(std.testing.allocator, .int64, "[-3:-2]={32768,NULL}", .{});
    defer input.deinit();
    try std.testing.expectError(error.SqlNumericOutOfRange, assignArray(arena.allocator(), Datum.typedArray(&input.value), .int16, .{}));
    const text = try assignArray(arena.allocator(), Datum.typedArray(&input.value), .text, .{});
    try std.testing.expectEqual(@as(i32, -3), text.array.?.dimensions[0].lower);
    try std.testing.expectEqualStrings("32768", text.array.?.elements[0].value.string);
    try std.testing.expect(text.array.?.elements[1].sql_null);
    try std.testing.expectError(error.SqlAssignmentTypeMismatch, assignArray(arena.allocator(), text, .int64, .{}));
    try std.testing.expectError(error.SqlProgramLimitExceeded, assignArray(arena.allocator(), Datum.typedArray(&input.value), .text, .{ .steps = 0 }));
}

const Evaluator = struct {
    program: *const Program,
    alloc: Allocator,
    cells: []const Datum,
    parameters: []const Json,
    typed_parameters: ?[]const Datum = null,
    limits: EvalLimits,
    steps: usize = 0,
    pattern_steps: usize = 0,
    bytes: usize = 0,
    // Constant caching is speculative: modifier overflow in an unselected
    // branch must remain an execution-time error, not a binding-time error.
    constant_preparation: bool = false,

    fn charge(self: *Evaluator, bytes: usize) !void {
        if (bytes > self.limits.output_bytes -| self.bytes) return error.SqlProgramLimitExceeded;
        self.bytes += bytes;
    }

    fn run(self: *Evaluator, index: u32, depth: usize) anyerror!Json {
        const result = try self.runDatum(index, depth);
        if (result.array != null or result.numeric != null) return error.SqlTypeMismatch;
        return result.value;
    }

    fn runDatum(self: *Evaluator, index: u32, depth: usize) anyerror!Datum {
        if (depth >= self.limits.depth or self.steps >= self.limits.steps) return error.SqlProgramLimitExceeded;
        if (self.steps % 256 == 0) if (self.limits.checkpoint) |poll| try poll(self.limits.checkpoint_context);
        self.steps += 1;
        const instruction = self.program.instructions[index];
        if (self.program.constant_numerics.get(index)) |number| return Datum.typedNumeric(number);
        var result: Datum = switch (instruction.operation) {
            .parameter => |slot| blk: {
                if (self.typed_parameters) |parameters| {
                    if (slot >= parameters.len) return error.InvalidSqlParameters;
                    break :blk parameters[slot];
                }
                if (self.program.invocation != null) return error.InvalidSqlParameters;
                if (instruction.type.kind == .array) return error.UnsupportedSqlShape;
                if (slot >= self.parameters.len) return error.InvalidSqlParameters;
                const value = self.parameters[slot];
                if (value == .null) break :blk .{};
                // Compatibility ingress supplies decoded logical JSON values,
                // not PostgreSQL text-format bytes. A JSON string stays a
                // string even when its contents happen to be valid JSON text.
                if (instruction.type.kind == .json) break :blk Datum.json(value);
                break :blk Datum.json(try self.convert(value, instruction.type.kind.?));
            },
            .column => |ordinal| if (ordinal < self.cells.len) self.cells[ordinal] else return error.InvalidSqlBackendResponse,
            .cast => |cast| blk: {
                if (cast.type == .array) if (self.program.constant_arrays.get(index)) |prepared| {
                    try self.charge(@sizeOf(arrays.Value) + prepared.elements.len * @sizeOf(arrays.Element) + prepared.dimensions.len * @sizeOf(arrays.Dimension));
                    break :blk Datum.typedArray(prepared);
                };
                const datum = try self.runDatum(cast.operand, depth + 1);
                if (datum.sql_null) break :blk .{};
                if (cast.type == .array) {
                    if (datum.array == null and datum.value == .string) {
                        const decoded = try @import("array_text.zig").decodeLeaky(self.alloc, cast.element_type orelse return error.InvalidSqlProgram, datum.value.string, .{ .values = .{ .bytes = self.limits.output_bytes -| self.bytes, .work = self.limits.steps -| self.steps } });
                        self.steps += decoded.work;
                        try self.charge(decoded.allocated_bytes + @sizeOf(arrays.Value));
                        const value = try self.alloc.create(arrays.Value);
                        value.* = decoded.value;
                        const converted = Datum.typedArray(value);
                        break :blk if (cast.numeric_modifier) |modifier| try self.constrainNumeric(converted, modifier) else converted;
                    }
                    const converted = try self.castArray(datum, cast.element_type orelse return error.InvalidSqlProgram);
                    break :blk if (cast.numeric_modifier) |modifier| try self.constrainNumeric(converted, modifier) else converted;
                }
                if (datum.array != null) return error.SqlTypeMismatch;
                if (cast.element_type) |target| {
                    const source = self.program.instructions[cast.operand].type;
                    const source_element = source.element_type orelse (if (source.kind == null or source.kind == .datetime or source.kind == .number) null else try arrayElementType(source.kind.?));
                    if (source_element == .jsonb and datum.value == .null and target != .text and target != .jsonb) break :blk .{};
                    const converted = try self.castDatumBuiltin(datum, source_element, target);
                    break :blk if (cast.numeric_modifier) |modifier| try self.constrainNumeric(converted, modifier) else converted;
                }
                if (datum.numeric != null) break :blk try self.castDatumBuiltin(datum, .numeric, try arrayElementType(cast.type));
                if (cast.type == .string and self.program.instructions[cast.operand].type.kind == .json) break :blk Datum.json(.{ .string = try self.jsonText(datum.value) });
                if (cast.type == .json and self.program.instructions[cast.operand].type.kind == .json) break :blk datum;
                if (datum.value == .null and cast.type == .string) break :blk Datum.json(.{ .string = "null" });
                if (datum.value == .null and cast.type != .json) return error.SqlTypeMismatch;
                break :blk Datum.json(try self.convert(datum.value, cast.type));
            },
            .unary => |unary| if (unary.op == .is_null or unary.op == .is_not_null or unary.op == .is_unknown or unary.op == .is_not_unknown) blk: {
                const datum = try self.runDatum(unary.operand, depth + 1);
                break :blk Datum.json(.{ .bool = datum.sql_null == (unary.op == .is_null or unary.op == .is_unknown) });
            } else if (instruction.type.element_type == .numeric) blk: {
                const operand = try self.runDatum(unary.operand, depth + 1);
                if (operand.sql_null) break :blk .{};
                const exact = try self.castDatumBuiltin(operand, self.program.instructions[unary.operand].type.element_type, .numeric);
                if (unary.op == .positive) break :blk exact;
                if (unary.op != .negative) return error.SqlTypeMismatch;
                const number = try self.alloc.create(@import("numeric_value.zig").Value);
                number.* = exact.numeric.?.*;
                switch (number.kind) {
                    .positive_infinity => number.kind = .negative_infinity,
                    .negative_infinity => number.kind = .positive_infinity,
                    .finite => if (!number.isZero()) {
                        number.negative = !number.negative;
                    },
                    .nan => {},
                }
                try self.charge(@sizeOf(@import("numeric_value.zig").Value));
                break :blk Datum.typedNumeric(number);
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
            } else if (binary.op == .add or binary.op == .subtract or binary.op == .multiply or binary.op == .divide or binary.op == .modulo) blk: {
                var left = try self.runDatum(binary.left, depth + 1);
                var right = try self.runDatum(binary.right, depth + 1);
                if (left.sql_null or right.sql_null) break :blk .{};
                if (instruction.type.element_type == .numeric) {
                    left = try self.castDatumBuiltin(left, self.program.instructions[binary.left].type.element_type, .numeric);
                    right = try self.castDatumBuiltin(right, self.program.instructions[binary.right].type.element_type, .numeric);
                    break :blk try self.numericArithmetic(binary.op, left, right);
                }
                if (left.numeric != null) left = try self.castDatumBuiltin(left, .numeric, instruction.type.element_type orelse .float64);
                if (right.numeric != null) right = try self.castDatumBuiltin(right, .numeric, instruction.type.element_type orelse .float64);
                if (left.value == .null or right.value == .null) break :blk .{};
                break :blk Datum.json(try arithmetic(binary.op, left.value, right.value));
            } else if (binary.op == .concat and instruction.type.kind == .json) blk: {
                const left = try self.runDatum(binary.left, depth + 1);
                const right = try self.runDatum(binary.right, depth + 1);
                if (left.sql_null or right.sql_null) break :blk .{};
                if (left.array != null or right.array != null) return error.SqlTypeMismatch;
                var work: @import("json_order.zig").Budget = .{ .remaining = self.limits.steps -| self.steps };
                const before = work.remaining;
                const joined = try @import("json_concat.zig").concat(self.alloc, left.value, right.value, self.limits.output_bytes -| self.bytes, &work);
                self.steps += before - work.remaining;
                try self.charge(joined.allocated_bytes);
                break :blk Datum.json(joined.value);
            } else if (binary.op == .eq or binary.op == .neq or binary.op == .lt or binary.op == .lte or binary.op == .gt or binary.op == .gte or binary.op == .is_distinct or binary.op == .is_not_distinct) blk: {
                var left = try self.runDatum(binary.left, depth + 1);
                var right = try self.runDatum(binary.right, depth + 1);
                // Numeric/real operators resolve in double precision, while
                // NUMERIC/integer comparisons retain exact integer identity.
                if ((left.numeric != null and right.value == .float) or (right.numeric != null and left.value == .float)) {
                    if (left.numeric != null) left = try self.castDatumBuiltin(left, .numeric, .float64);
                    if (right.numeric != null) right = try self.castDatumBuiltin(right, .numeric, .float64);
                }
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
                var unknown = operand.sql_null;
                for (list.values) |item| {
                    const value = try self.runDatum(item, depth + 1);
                    if (operand.sql_null or value.sql_null) {
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
                    .array_append, .array_prepend, .array_cat => {
                        const left = try self.runDatum(call.args[0], depth + 1);
                        const right = try self.runDatum(call.args[1], depth + 1);
                        if (call.function == .array_cat) {
                            if (left.sql_null) break :blk right;
                            if (right.sql_null) break :blk left;
                            if (left.array.?.elements.len == 0) break :blk right;
                            if (right.array.?.elements.len == 0) break :blk left;
                        }
                        var work: arrays.Budget = .{ .remaining = self.limits.steps -| self.steps };
                        const before = work.remaining;
                        const limits: arrays.Limits = .{ .bytes = self.limits.output_bytes -| self.bytes };
                        const prepend = call.function == .array_prepend;
                        const input = if (prepend) right else left;
                        const empty: arrays.Value = .{ .element_type = instruction.type.element_type.?, .dimensions = &.{}, .elements = &.{} };
                        const output = if (call.function == .array_cat) try left.array.?.concatenate(self.alloc, right.array.?.*, limits, &work) else try (input.array orelse &empty).append(self.alloc, if (prepend) left else right, prepend, limits, &work);
                        self.steps += before - work.remaining;
                        try self.charge(@sizeOf(arrays.Value) + output.dimensions.len * @sizeOf(arrays.Dimension) + output.elements.len * @sizeOf(arrays.Element));
                        const owned = try self.alloc.create(arrays.Value);
                        owned.* = output;
                        break :blk Datum.typedArray(owned);
                    },
                    .array_position, .array_positions, .array_remove, .array_replace => {
                        // These functions are deliberately non-strict in the
                        // searched value/replacement: NULL is a matchable cell.
                        const input = try self.runDatum(call.args[0], depth + 1);
                        const needle = try self.runDatum(call.args[1], depth + 1);
                        const third = if (call.args.len == 3) try self.runDatum(call.args[2], depth + 1) else Datum{};
                        if (input.sql_null) break :blk .{};
                        const array = input.array orelse return error.SqlTypeMismatch;
                        var work: arrays.Budget = .{ .remaining = self.limits.steps -| self.steps };
                        const before = work.remaining;
                        if (call.function == .array_position) {
                            if (array.dimensions.len > 1) return error.UnsupportedSqlShape;
                            if (array.elements.len == 0) break :blk .{};
                            if (call.args.len == 3 and third.sql_null) return error.SqlNullValueNotAllowed;
                            const start = if (call.args.len == 3) std.math.cast(i32, third.value.integer) orelse return error.SqlNumericOutOfRange else null;
                            const answer = if (call.args.len == 2 and self.program.constant_memberships.contains(call.args[0])) try self.program.constant_memberships.get(call.args[0]).?.firstPosition(needle, &work) else try array.position(needle, start, &work);
                            self.steps += before - work.remaining;
                            break :blk if (answer) |value| Datum.json(.{ .integer = value }) else .{};
                        }
                        const limits: arrays.Limits = .{ .bytes = self.limits.output_bytes -| self.bytes };
                        const output = if (call.function == .array_positions) try array.positions(self.alloc, needle, limits, &work) else try array.transform(self.alloc, needle, if (call.function == .array_remove) null else third, limits, &work);
                        self.steps += before - work.remaining;
                        try self.charge(@sizeOf(arrays.Value) + output.dimensions.len * @sizeOf(arrays.Dimension) + output.elements.len * @sizeOf(arrays.Element));
                        const owned = try self.alloc.create(arrays.Value);
                        owned.* = output;
                        break :blk Datum.typedArray(owned);
                    },
                    .@"$like_escape" => {
                        const input = try self.runDatum(call.args[0], depth + 1);
                        const pattern = try self.runDatum(call.args[1], depth + 1);
                        const escape = try self.runDatum(call.args[2], depth + 1);
                        const insensitive = try self.runDatum(call.args[3], depth + 1);
                        if (input.sql_null or pattern.sql_null or escape.sql_null or insensitive.sql_null) break :blk .{};
                        if (escape.value != .string or insensitive.value != .bool) return error.SqlTypeMismatch;
                        var points = (std.unicode.Utf8View.init(escape.value.string) catch return error.SqlInvalidEscapeString).iterator();
                        _ = points.nextCodepoint();
                        if (points.nextCodepoint() != null) return error.SqlInvalidEscapeString;
                        break :blk Datum.json(.{ .bool = try self.likeWithEscape(input.value, pattern.value, insensitive.value.bool, escape.value.string) });
                    },
                    .string_to_array => {
                        if (self.program.constant_arrays.get(index)) |prepared| break :blk Datum.typedArray(prepared);
                        const input = try self.runDatum(call.args[0], depth + 1);
                        const delimiter = try self.runDatum(call.args[1], depth + 1);
                        const null_text = if (call.args.len == 3) try self.runDatum(call.args[2], depth + 1) else Datum{};
                        if (input.sql_null) break :blk .{};
                        if (input.value != .string or (!delimiter.sql_null and delimiter.value != .string) or (!null_text.sql_null and null_text.value != .string)) return error.SqlTypeMismatch;
                        var work: arrays.Budget = .{ .remaining = self.limits.steps -| self.steps };
                        const initial = work.remaining;
                        const output = try @import("text_array.zig").split(self.alloc, input.value.string, if (delimiter.sql_null) null else delimiter.value.string, if (null_text.sql_null) null else null_text.value.string, self.limits.output_bytes -| self.bytes, &work);
                        self.steps += initial - work.remaining;
                        try self.charge(output.bytes);
                        break :blk Datum.typedArray(output.value);
                    },
                    .array_to_string => {
                        const input = try self.runDatum(call.args[0], depth + 1);
                        const delimiter = try self.runDatum(call.args[1], depth + 1);
                        const null_text = if (call.args.len == 3) try self.runDatum(call.args[2], depth + 1) else Datum{};
                        if (input.sql_null or delimiter.sql_null) break :blk .{};
                        if (delimiter.value != .string or (!null_text.sql_null and null_text.value != .string)) return error.SqlTypeMismatch;
                        var work: arrays.Budget = .{ .remaining = self.limits.steps -| self.steps };
                        const initial = work.remaining;
                        const text = try @import("text_array.zig").join(self.alloc, input.array orelse return error.SqlTypeMismatch, delimiter.value.string, if (null_text.sql_null) null else null_text.value.string, self.limits.output_bytes -| self.bytes, &work);
                        self.steps += initial - work.remaining;
                        try self.charge(text.len);
                        break :blk Datum.json(.{ .string = text });
                    },
                    .@"$contains", .@"$overlaps", .jsonb_exists, .jsonb_exists_any, .jsonb_exists_all => {
                        const left = try self.runDatum(call.args[0], depth + 1);
                        const right = try self.runDatum(call.args[1], depth + 1);
                        if (left.sql_null or right.sql_null) break :blk .{};
                        var work: arrays.Budget = .{ .remaining = self.limits.steps -| self.steps };
                        const initial = work.remaining;
                        const accepted = if (call.function == .jsonb_exists) exists: {
                            if (left.array != null or right.value != .string) return error.SqlTypeMismatch;
                            break :exists try @import("json_containment.zig").exists(left.value, right.value.string, &work);
                        } else if (call.function == .jsonb_exists_any or call.function == .jsonb_exists_all) exists: {
                            if (left.array != null) return error.SqlTypeMismatch;
                            break :exists try @import("json_containment.zig").existsKeys(left.value, (right.array orelse return error.SqlTypeMismatch).*, call.function == .jsonb_exists_all, &work);
                        } else if (left.array) |array| contains: {
                            const other = right.array orelse return error.SqlTypeMismatch;
                            if (self.program.constant_memberships.get(call.args[0])) |prepared| break :contains if (call.function == .@"$overlaps") try prepared.overlaps(other.*, &work) else try prepared.contains(other.*, &work);
                            if (call.function == .@"$overlaps") if (self.program.constant_memberships.get(call.args[1])) |prepared| break :contains try prepared.overlaps(array.*, &work);
                            const swapped = call.function == .@"$overlaps" and other.elements.len < array.elements.len;
                            var membership = try arrays.Membership.initWithBudget(self.alloc, if (swapped) other.* else array.*, .{ .bytes = self.limits.output_bytes -| self.bytes }, &work);
                            defer membership.deinit();
                            try self.charge(membership.budget.live + @sizeOf(arrays.Membership));
                            break :contains if (call.function == .@"$overlaps") try membership.overlaps(if (swapped) array.* else other.*, &work) else try membership.contains(other.*, &work);
                        } else try @import("json_containment.zig").contains(left.value, right.value, &work, 0);
                        self.steps += initial - work.remaining;
                        break :blk Datum.json(.{ .bool = accepted });
                    },
                    .@"$array" => {
                        const kind = instruction.type.element_type orelse return error.InvalidSqlProgram;
                        if (self.program.constant_arrays.get(index)) |prepared| {
                            try self.charge(@sizeOf(arrays.Value) + prepared.elements.len * @sizeOf(arrays.Element) + prepared.dimensions.len * @sizeOf(arrays.Dimension));
                            break :blk Datum.typedArray(prepared);
                        }
                        for (call.args) |arg| if (self.program.instructions[arg].type.kind == .array) {
                            break :blk try self.nestedArray(call.args, kind, depth + 1);
                        };
                        const cell_bytes = std.math.mul(usize, call.args.len, @sizeOf(arrays.Element)) catch return error.SqlProgramLimitExceeded;
                        try self.charge(cell_bytes + @sizeOf(arrays.Value) + @sizeOf(arrays.Dimension));
                        const elements = try self.alloc.alloc(arrays.Element, call.args.len);
                        for (call.args, elements) |arg, *out| {
                            const value = try self.runDatum(arg, depth + 1);
                            if (value.array != null) return error.SqlTypeMismatch;
                            out.* = if (value.sql_null) .{} else try self.castDatumBuiltin(value, self.program.instructions[arg].type.element_type, kind);
                        }
                        const dimensions = try self.alloc.alloc(arrays.Dimension, @intFromBool(elements.len != 0));
                        if (elements.len != 0) dimensions[0] = .{ .length = std.math.cast(u32, elements.len) orelse return error.SqlProgramLimitExceeded };
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
                        const exact_number = probe.numeric != null or array.element_type == .numeric;
                        const element: arrays.Element = if (exact_number) probe else if (probe.sql_null) .{} else arrays.Element.json(try self.convert(probe.value, if (mixed_number) .number else arrayScalarType(array.element_type)));
                        const accepted = if (exact_number) try self.numericQuantified(probe, array.*, compare_op, every.bool) else ordinary: {
                            var work: arrays.Budget = .{ .remaining = self.limits.steps -| self.steps };
                            const initial = work.remaining;
                            defer self.steps += initial - work.remaining;
                            break :ordinary if (mixed_number) try mixedNumberQuantified(array.*, element, compare_op, every.bool, &work) else try array.quantified(element, compare_op, if (every.bool) .all else .any, &work);
                        };
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
                    .jsonb_set => {
                        // Strict SQL functions evaluate all arguments before
                        // NULL propagation, unlike CASE and COALESCE.
                        const target = try self.runDatum(call.args[0], depth + 1);
                        const path = try self.runDatum(call.args[1], depth + 1);
                        const replacement = try self.runDatum(call.args[2], depth + 1);
                        const create = if (call.args.len == 4) try self.runDatum(call.args[3], depth + 1) else Datum.json(.{ .bool = true });
                        if (target.sql_null or path.sql_null or replacement.sql_null or create.sql_null) break :blk .{};
                        if (target.array != null or replacement.array != null or create.array != null or create.value != .bool) return error.SqlTypeMismatch;
                        var work: @import("json_order.zig").Budget = .{ .remaining = self.limits.steps -| self.steps };
                        const before = work.remaining;
                        const changed = try @import("json_path_update.zig").set(self.alloc, target.value, path.array orelse return error.SqlTypeMismatch, replacement.value, create.value.bool, self.limits.output_bytes -| self.bytes, &work);
                        self.steps += before - work.remaining;
                        try self.charge(changed.allocated_bytes);
                        break :blk Datum.json(changed.value);
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
                            const string = if (self.program.instructions[arg].type.kind == .json) try self.jsonText(datum.value) else try self.formatText(if (datum.numeric != null) try self.numericJson(datum) else datum.value);
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
                        const input = try self.runDatum(call.args[0], depth + 1);
                        break :blk if (input.numeric != null) Datum.json(try self.numericJson(input)) else input;
                    },
                    .jsonb_build_object => {
                        var object: std.json.ObjectMap = .empty;
                        errdefer object.deinit(self.alloc);
                        var i: usize = 0;
                        while (i < call.args.len) : (i += 2) {
                            var key = try self.runDatum(call.args[i], depth + 1);
                            if (key.numeric != null) key = Datum.json(try self.numericJson(key));
                            if (key.sql_null or key.value == .null or key.value == .object or key.value == .array) return error.InvalidSqlParameters;
                            const name = try self.formatText(key.value);
                            try self.charge(name.len + @sizeOf(Json) + @sizeOf([]const u8));
                            const value = try self.runDatum(call.args[i + 1], depth + 1);
                            try object.put(self.alloc, name, if (value.sql_null) .null else if (value.numeric != null) try self.numericJson(value) else value.value);
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
                            const string = if (self.program.instructions[arg].type.kind == .json) try self.jsonText(datum.value) else try self.formatText(if (datum.numeric != null) try self.numericJson(datum) else datum.value);
                            try self.charge(string.len);
                            try output.appendSlice(self.alloc, string);
                        }
                        break :blk Datum.json(.{ .string = try output.toOwnedSlice(self.alloc) });
                    },
                    .abs, .ceil, .floor, .round, .trunc, .sign, .mod, .sqrt => break :blk if (instruction.type.element_type == .numeric) try self.numericFunction(call.function, call.args, depth + 1) else Datum.fromJson(try self.invokeFunction(call.function, call.args, self.program.translations.get(index), depth + 1)),
                    else => break :blk Datum.fromJson(try self.invokeFunction(call.function, call.args, self.program.translations.get(index), depth + 1)),
                }
            },
            else => Datum.fromJson(try self.runLegacy(index, depth)),
        };
        if (!result.sql_null and instruction.type.kind == .number and instruction.type.element_type == .numeric) result = try self.castDatumBuiltin(result, null, .numeric) else if (!result.sql_null and instruction.type.kind == .number and result.value == .integer) result.value = try finite(@floatFromInt(result.value.integer));
        if (!result.sql_null and result.value == .integer) if (instruction.type.element_type) |kind| if (builtin_cast.integral(kind)) {
            _ = try builtin_cast.checkedInteger(result.value.integer, kind);
        };
        if (!result.sql_null and result.value == .float and instruction.type.element_type == .float32) result.value = .{ .float = try builtin_cast.floatValue(f32, result.value) };
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
                if (unary.op == .is_null or unary.op == .is_not_null or unary.op == .is_unknown or unary.op == .is_not_unknown) break :blk .{ .bool = (value == .null) == (unary.op == .is_null or unary.op == .is_unknown) };
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
                var work: json_order.Budget = .{ .remaining = self.limits.steps -| self.steps };
                const initial = work.remaining;
                const parsed = try json_order.parseTextLeaky(self.alloc, value.string, &work);
                self.steps += initial - work.remaining;
                break :blk parsed;
            } else value,
        };
    }

    /// Evaluate children once, admit a rectangular layout, then flatten into
    /// one exact cell allocation. Payloads borrow the enclosing row/program
    /// region; no JSON reconstruction or repeated evaluation is involved.
    fn nestedArray(self: *Evaluator, args: []const u32, kind: arrays.ElementType, depth: usize) !Datum {
        var local: [16]Datum = undefined;
        const children = if (args.len <= local.len) local[0..args.len] else blk: {
            try self.charge(std.math.mul(usize, args.len, @sizeOf(Datum)) catch return error.SqlProgramLimitExceeded);
            break :blk try self.alloc.alloc(Datum, args.len);
        };
        var dimensions: []const arrays.Dimension = &.{};
        var seen = false;
        var count: usize = 0;
        for (args, children) |arg, *child| {
            child.* = try self.runDatum(arg, depth);
            const shape: []const arrays.Dimension = if (child.sql_null) &.{} else (child.array orelse return error.SqlTypeMismatch).dimensions;
            if (seen) {
                if (shape.len != dimensions.len) return error.SqlArraySubscriptError;
                for (shape, dimensions) |actual, expected| if (actual.length != expected.length or actual.lower != expected.lower) return error.SqlArraySubscriptError;
            } else {
                seen = true;
                dimensions = shape;
            }
            if (child.array) |array| count = std.math.add(usize, count, array.elements.len) catch return error.SqlProgramLimitExceeded;
        }
        if (dimensions.len == 6 or count > std.math.maxInt(i32)) return error.SqlProgramLimitExceeded;
        const rank = if (count == 0) 0 else dimensions.len + 1;
        const cell_bytes = std.math.mul(usize, count, @sizeOf(arrays.Element)) catch return error.SqlProgramLimitExceeded;
        try self.charge(cell_bytes + rank * @sizeOf(arrays.Dimension) + @sizeOf(arrays.Value));
        const cells = try self.alloc.alloc(arrays.Element, count);
        var at: usize = 0;
        for (children) |child| if (child.array) |array| {
            for (array.elements) |element| {
                if (self.steps >= self.limits.steps) return error.SqlProgramLimitExceeded;
                self.steps += 1;
                cells[at] = if (element.sql_null) .{} else try self.castDatumBuiltin(element, array.element_type, kind);
                at += 1;
            }
        };
        const shape = try self.alloc.alloc(arrays.Dimension, rank);
        if (rank != 0) {
            shape[0] = .{ .length = std.math.cast(u32, args.len) orelse return error.SqlProgramLimitExceeded };
            @memcpy(shape[1..], dimensions);
        }
        const value = try self.alloc.create(arrays.Value);
        var work: arrays.Budget = .{ .remaining = self.limits.steps -| self.steps };
        const initial = work.remaining;
        value.* = try arrays.Value.initWithBudget(kind, shape, cells, .{}, &work);
        self.steps += initial - work.remaining;
        return Datum.typedArray(value);
    }

    fn castArray(self: *Evaluator, datum: Datum, target: arrays.ElementType) !Datum {
        if (datum.sql_null) return datum;
        const source = datum.array orelse return error.UnsupportedSqlShape;
        if (source.element_type == target) return datum;
        if (!builtin_cast.allowed(source.element_type, target)) return error.SqlCannotCoerce;
        try self.charge(@sizeOf(arrays.Value) + source.elements.len * @sizeOf(arrays.Element));
        const cells = try self.alloc.alloc(arrays.Element, source.elements.len);
        for (source.elements, cells) |element, *cell| {
            if (self.steps >= self.limits.steps) return error.SqlProgramLimitExceeded;
            self.steps += 1;
            cell.* = if (element.sql_null or (source.element_type == .jsonb and element.value == .null and target != .text and target != .jsonb)) .{} else try self.castDatumBuiltin(element, source.element_type, target);
        }
        const value = try self.alloc.create(arrays.Value);
        value.* = try arrays.Value.init(target, source.dimensions, cells, .{});
        return Datum.typedArray(value);
    }

    fn constrainNumeric(self: *Evaluator, datum: Datum, modifier: @import("../common/sql_builtin_type.zig").NumericModifier) anyerror!Datum {
        try modifier.validate();
        if (datum.sql_null) return datum;
        if (datum.array) |source| {
            if (source.element_type != .numeric) return error.InvalidSqlProgram;
            try self.charge(@sizeOf(arrays.Value) + source.elements.len * @sizeOf(arrays.Element));
            const cells = try self.alloc.alloc(arrays.Element, source.elements.len);
            for (source.elements, cells) |element, *cell| {
                if (self.steps >= self.limits.steps) return error.SqlProgramLimitExceeded;
                if (self.steps % 256 == 0) if (self.limits.checkpoint) |poll| try poll(self.limits.checkpoint_context);
                self.steps += 1;
                cell.* = try self.constrainNumeric(element, modifier);
            }
            var work: arrays.Budget = .{ .remaining = self.limits.steps -| self.steps };
            const before = work.remaining;
            defer self.steps += before - work.remaining;
            const result = try arrays.Value.initWithBudget(.numeric, source.dimensions, cells, .{}, &work);
            const owner = try self.alloc.create(arrays.Value);
            owner.* = result;
            return Datum.typedArray(owner);
        }
        const exact = @import("numeric_value.zig");
        const input = datum.numeric orelse return error.InvalidSqlProgram;
        var context = self.numericContext();
        const before = context.remaining;
        defer self.steps += @intCast(before - context.remaining);
        var result = exact.applyTypeModifier(&context, input.*, modifier) catch |err| switch (err) {
            error.InvalidSqlNumber => return if (self.constant_preparation) error.NumericModifierNotPreparable else error.SqlNumericOutOfRange,
            else => return err,
        };
        errdefer result.deinit();
        try self.charge(@sizeOf(exact.Value) + result.allocation.len * @sizeOf(u16));
        const owner = try self.alloc.create(exact.Value);
        owner.* = result.value;
        return Datum.typedNumeric(owner);
    }

    fn numericContext(self: *Evaluator) @import("numeric_value.zig").Context {
        return .{
            .alloc = self.alloc,
            .remaining = self.limits.steps -| self.steps,
            .max_output_bytes = self.limits.output_bytes -| self.bytes,
            .max_groups = (self.limits.output_bytes -| self.bytes) / 2,
            .checkpoint = self.limits.checkpoint,
            .ptr = self.limits.checkpoint_context,
        };
    }

    fn numericJson(self: *Evaluator, datum: Datum) !Json {
        const exact = @import("numeric_value.zig");
        const number = datum.numeric orelse return error.SqlTypeMismatch;
        var context = self.numericContext();
        const before = context.remaining;
        defer self.steps += @intCast(before - context.remaining);
        const text = try exact.format(&context, number.*);
        try self.charge(text.len);
        return if (number.kind == .finite) .{ .number_string = text } else .{ .string = text };
    }

    fn numericFunction(self: *Evaluator, function: Function, args: []const u32, depth: usize) !Datum {
        const exact = @import("numeric_value.zig");
        const first = try self.runDatum(args[0], depth);
        if (first.sql_null) return .{};
        const input = try self.castDatumBuiltin(first, self.program.instructions[args[0]].type.element_type, .numeric);
        if (function == .mod) {
            const second = try self.runDatum(args[1], depth);
            if (second.sql_null) return .{};
            return self.numericArithmetic(.modulo, input, try self.castDatumBuiltin(second, self.program.instructions[args[1]].type.element_type, .numeric));
        }
        const value = input.numeric.?.*;
        if (function == .abs or function == .sign) {
            try self.charge(@sizeOf(exact.Value));
            const result = try self.alloc.create(exact.Value);
            if (function == .abs) {
                result.* = value;
                result.negative = false;
                if (result.kind == .negative_infinity) result.kind = .positive_infinity;
            } else result.* = if (value.kind == .nan) .{ .kind = .nan } else if (value.isZero()) .{} else .{ .digits = &.{1}, .negative = value.negative or value.kind == .negative_infinity };
            return Datum.typedNumeric(result);
        }
        const scale: i32 = if (args.len == 2) selected: {
            const requested = try self.runDatum(args[1], depth);
            if (requested.sql_null) return .{};
            if (requested.value != .integer) return error.SqlTypeMismatch;
            break :selected std.math.cast(i32, requested.value.integer) orelse return error.SqlNumericOutOfRange;
        } else 0;
        // Evaluate both arguments before borrowing the remaining work budget.
        // Nested scale expressions must not receive a second copy of it.
        var ctx = self.numericContext();
        const before = ctx.remaining;
        defer self.steps += @intCast(before - ctx.remaining);
        var result = if (function == .sqrt) try exact.squareRoot(&ctx, value) else try exact.quantize(&ctx, value, scale, if (function == .round) .half_away else .truncate);
        errdefer result.deinit();
        if (function == .ceil or function == .floor) {
            const order = try exact.order(&ctx, value, result.value);
            if ((function == .ceil and order == .gt) or (function == .floor and order == .lt)) {
                const one: exact.Value = .{ .digits = &.{1} };
                const adjusted = if (function == .ceil) try exact.add(&ctx, result.value, one) else try exact.subtract(&ctx, result.value, one);
                result.deinit();
                result = adjusted;
            }
        }
        try self.charge(@sizeOf(exact.Value) + result.allocation.len * 2);
        const owned = try self.alloc.create(exact.Value);
        owned.* = result.value;
        return Datum.typedNumeric(owned);
    }

    fn numericQuantified(self: *Evaluator, probe: Datum, array: arrays.Value, op: arrays.Comparison, every: bool) !?bool {
        const exact = @import("numeric_value.zig");
        const floating = builtin_cast.floating(array.element_type) or probe.value == .float;
        const needle = if (!probe.sql_null and floating) try self.castDatumBuiltin(probe, null, .float64) else probe;
        var unknown = false;
        for (array.elements) |element| {
            if (self.steps >= self.limits.steps) return error.SqlProgramLimitExceeded;
            self.steps += 1;
            if (needle.sql_null or element.sql_null) {
                unknown = true;
                continue;
            }
            const order = if (floating) float: {
                const right = try self.castDatumBuiltin(element, array.element_type, .float64);
                break :float try compare(needle.value, right.value);
            } else logical: {
                var left_storage: [5]u16 = undefined;
                var right_storage: [5]u16 = undefined;
                const left = if (needle.numeric) |value| value.* else if (needle.value == .integer) exact.integerView(needle.value.integer, &left_storage) else return error.SqlTypeMismatch;
                const right = if (element.numeric) |value| value.* else if (element.value == .integer) exact.integerView(element.value.integer, &right_storage) else return error.SqlTypeMismatch;
                var context = self.numericContext();
                const before = context.remaining;
                defer self.steps += @intCast(before - context.remaining);
                break :logical try exact.order(&context, left, right);
            };
            const accepted = op.accepts(order);
            if (accepted != every) return accepted;
        }
        return if (unknown) null else every;
    }

    fn numericArithmetic(self: *Evaluator, operation: ast.Scalar.Binary, left: Datum, right: Datum) !Datum {
        const exact = @import("numeric_value.zig");
        var context = self.numericContext();
        const before = context.remaining;
        defer self.steps += @intCast(before - context.remaining);
        var result = switch (operation) {
            .add => try exact.add(&context, left.numeric.?.*, right.numeric.?.*),
            .subtract => try exact.subtract(&context, left.numeric.?.*, right.numeric.?.*),
            .multiply => try exact.multiply(&context, left.numeric.?.*, right.numeric.?.*),
            .divide => try exact.divide(&context, left.numeric.?.*, right.numeric.?.*),
            .modulo => try exact.remainder(&context, left.numeric.?.*, right.numeric.?.*),
            else => unreachable,
        };
        errdefer result.deinit();
        try self.charge(@sizeOf(exact.Value) + result.allocation.len * 2);
        const value = try self.alloc.create(exact.Value);
        value.* = result.value;
        return Datum.typedNumeric(value);
    }

    fn castDatumBuiltin(self: *Evaluator, datum: Datum, source: ?arrays.ElementType, target: arrays.ElementType) !Datum {
        if (datum.sql_null) return .{};
        if (datum.numeric != null and target == .numeric) return datum;
        const exact = @import("numeric_value.zig");
        var ctx = self.numericContext();
        const before = ctx.remaining;
        defer self.steps += @intCast(before - ctx.remaining);
        if (target == .numeric) {
            var buffer: [20]u8 = undefined;
            var owned = switch (datum.value) {
                .integer => |integer| try exact.parse(&ctx, try std.fmt.bufPrint(&buffer, "{d}", .{integer})),
                .string => |text| if (source == .jsonb) return error.SqlTypeMismatch else try exact.parse(&ctx, text),
                .number_string => |text| try exact.parse(&ctx, text),
                .float => |number| try exact.fromFloat(&ctx, number, source == .float32),
                else => return error.SqlTypeMismatch,
            };
            errdefer owned.deinit();
            try self.charge(@sizeOf(exact.Value) + owned.allocation.len * 2);
            const value = try self.alloc.create(exact.Value);
            value.* = owned.value;
            return Datum.typedNumeric(value);
        }
        if (datum.numeric) |number| {
            if (builtin_cast.integral(target)) return Datum.json(.{ .integer = switch (target) {
                .int16 => try exact.toInteger(i16, &ctx, number.*),
                .int32 => try exact.toInteger(i32, &ctx, number.*),
                else => try exact.toInteger(i64, &ctx, number.*),
            } });
            if (target != .text and target != .float32 and target != .float64) return error.SqlCannotCoerce;
            const text = try exact.format(&ctx, number.*);
            if (target == .text) {
                try self.charge(text.len);
                return Datum.json(.{ .string = text });
            }
            defer self.alloc.free(text);
            return Datum.json(.{ .float = if (target == .float32) try builtin_cast.floatValue(f32, .{ .string = text }) else try builtin_cast.floatValue(f64, .{ .string = text }) });
        }
        return Datum.json(try self.castBuiltin(datum.value, source, target));
    }

    fn castBuiltin(self: *Evaluator, value: Json, source: ?arrays.ElementType, target: arrays.ElementType) !Json {
        if (value == .string or value == .number_string) {
            const length = if (value == .string) value.string.len else value.number_string.len;
            if (length > self.limits.steps -| self.steps) return error.SqlProgramLimitExceeded;
            self.steps += length;
        }
        if (source == .jsonb and target != .jsonb and target != .text) {
            if (value == .null or value == .array or value == .object or value == .string) return error.SqlTypeMismatch;
            if (target == .boolean and value != .bool) return error.SqlTypeMismatch;
        }
        return switch (target) {
            .numeric => error.SqlTypeMismatch, // Exact values use castDatumBuiltin.
            .int16, .int32, .int64 => .{ .integer = try builtin_cast.checkedInteger(switch (value) {
                .integer => |integer| integer,
                .float => |number| blk: {
                    if (source == .jsonb or source == null) {
                        var buffer: [347]u8 = undefined;
                        const text = try std.fmt.float.render(&buffer, number, .{ .mode = .decimal });
                        var work: arrays.Budget = .{ .remaining = self.limits.steps -| self.steps };
                        const initial = work.remaining;
                        const integer = try json_order.roundedInteger(text, &work);
                        self.steps += initial - work.remaining;
                        break :blk try builtin_cast.checkedInteger(integer, target);
                    }
                    break :blk try builtin_cast.floatingInteger(number, target);
                },
                .number_string => |text| blk: {
                    var work: arrays.Budget = .{ .remaining = self.limits.steps -| self.steps };
                    const initial = work.remaining;
                    const result = try json_order.roundedInteger(text, &work);
                    self.steps += initial - work.remaining;
                    break :blk result;
                },
                .string => |text| try builtin_cast.integerText(text, target),
                .bool => |boolean| @intFromBool(boolean),
                else => return error.SqlTypeMismatch,
            }, target) },
            .float32 => .{ .float = try builtin_cast.floatValue(f32, value) },
            .float64 => .{ .float = try builtin_cast.floatValue(f64, value) },
            .boolean => .{ .bool = switch (value) {
                .bool => |boolean| boolean,
                .integer => |integer| integer != 0,
                .string => |text| try builtin_cast.booleanText(text),
                else => return error.SqlTypeMismatch,
            } },
            .text => blk: {
                if (source == .jsonb) break :blk .{ .string = try self.jsonText(value) };
                if (value == .float and source != null) {
                    var buffer: [64]u8 = undefined;
                    const text = if (source == .float32) try builtin_cast.floatText(f32, @floatCast(value.float), &buffer) else try builtin_cast.floatText(f64, value.float, &buffer);
                    try self.charge(text.len);
                    break :blk .{ .string = try self.alloc.dupe(u8, text) };
                }
                break :blk .{ .string = try self.formatText(value) };
            },
            .jsonb => if (source == .jsonb) value else try self.convert(value, .json),
            .uuid => try self.convert(value, .uuid),
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
        if (function == .@"$pattern_quantified" or function == .@"$array_pattern_quantified") {
            const operand = try self.run(args[0], depth + 1);
            const set_datum = try self.runDatum(args[1], depth + 1);
            const set = set_datum.value;
            const all = try self.run(args[2], depth + 1);
            const insensitive = try self.run(args[3], depth + 1);
            const negated = try self.run(args[4], depth + 1);
            if (all != .bool or insensitive != .bool or negated != .bool) return error.SqlTypeMismatch;
            if (function == .@"$array_pattern_quantified") {
                if (set_datum.sql_null) return .null;
                const array = set_datum.array orelse return error.InvalidSqlProgram;
                if (array.element_type != .text) return error.SqlUndefinedOperator;
                if (array.elements.len == 0) return .{ .bool = all.bool };
                if (operand == .null) return .null;
                var unknown = false;
                for (array.elements) |pattern| {
                    if (self.steps >= self.limits.steps) return error.SqlProgramLimitExceeded;
                    self.steps += 1;
                    if (pattern.sql_null) {
                        unknown = true;
                        continue;
                    }
                    const matches = (try self.like(operand, pattern.value, insensitive.bool)) != negated.bool;
                    if (matches != all.bool) return .{ .bool = matches };
                }
                return if (unknown) .null else .{ .bool = all.bool };
            }
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
        var values: [7]Json = @splat(.null);
        for (args, 0..) |arg, i| values[i] = try self.run(arg, depth);
        if (function == .nullif) return if (values[0] == .null or (values[1] != .null and (try compare(values[0], values[1])) == .eq)) .null else values[0];
        for (values[0..args.len]) |value| if (value == .null) return .null;
        if (regexFunction(function)) |regex| {
            const lease = if (self.limits.regex_execution) |owner| try owner.acquire() else null;
            defer if (lease) |owned| owned.deinit();
            var fallback: regex_functions.Session = undefined;
            const session = if (lease) |owned| owned.session() else self.limits.regex_session orelse blk: {
                fallback = regex_functions.Session.init(self.alloc, .{}, 16 * 1024 * 1024);
                break :blk &fallback;
            };
            defer if (lease == null and self.limits.regex_session == null) fallback.deinit();
            var budget: regex_functions.Budget = .{ .remaining = self.limits.pattern_steps -| self.pattern_steps, .checkpoint = self.limits.regex_checkpoint, .ptr = self.limits.regex_context, .checkpoint_interval = 256 };
            const initial = budget.remaining;
            defer self.pattern_steps += initial - budget.remaining;
            const value = regex_functions.evaluate(self.alloc, session, regex, values[0..args.len], self.limits.output_bytes -| self.bytes, &budget) catch |err|
                return if (self.limits.regex_execution) |owner| owner.mapError(err) else err;
            if (value == .string) try self.charge(value.string.len);
            return value;
        }
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
                else => roundTiesEven(first.float),
            }) else error.SqlTypeMismatch,
            .sign => return switch (first) {
                .integer => |value| .{ .integer = if (value < 0) -1 else if (value > 0) 1 else 0 },
                .float => |value| if (!std.math.isFinite(value)) error.SqlNumericOutOfRange else finite(if (value < 0) -1 else if (value > 0) 1 else 0),
                else => error.SqlTypeMismatch,
            },
            .sqrt => {
                const v = if (first == .float) first.float else try asFloat(first);
                if (v < 0) return error.SqlInvalidPowerArgument;
                if (std.math.isNan(v) or std.math.isPositiveInf(v)) return .{ .float = v };
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
        return self.likeWithEscape(text_value, pattern, insensitive, "\\");
    }

    fn likeWithEscape(self: *Evaluator, text_value: Json, pattern: Json, insensitive: bool, escape: []const u8) !bool {
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
            const is_escape = escape.len != 0 and j < glob.len and std.mem.startsWith(u8, glob[j..], escape);
            if (!is_escape and j < glob.len and glob[j] == '%') {
                if (j + 1 == glob.len) return true;
                star = j + 1;
                j += 1;
                restart = i;
                continue;
            }
            if (!is_escape and j < glob.len and glob[j] == '_') {
                i += std.unicode.utf8ByteSequenceLength(text[i]) catch return error.SqlTypeMismatch;
                j += 1;
                continue;
            }
            var escaped = false;
            if (is_escape) {
                j += escape.len;
                escaped = true;
                if (j == glob.len) return error.SqlInvalidEscapeSequence;
            }
            if (j < glob.len and (if (insensitive) std.ascii.toLower(text[i]) == std.ascii.toLower(glob[j]) else text[i] == glob[j])) {
                i += 1;
                j += 1;
                continue;
            }
            if (escaped) j -= escape.len;
            if (star) |next| {
                restart += std.unicode.utf8ByteSequenceLength(text[restart]) catch return error.SqlTypeMismatch;
                i = restart;
                j = next;
            } else return false;
        }
        while (j < glob.len and glob[j] == '%' and !(escape.len != 0 and std.mem.startsWith(u8, glob[j..], escape))) : (j += 1) {}
        return j == glob.len;
    }
};

fn roundTiesEven(value: f64) f64 {
    const magnitude = @abs(value);
    const integral = @floor(magnitude);
    const fraction = magnitude - integral;
    const rounded = if (fraction > 0.5 or (fraction == 0.5 and @mod(integral, 2) != 0)) integral + 1 else integral;
    return std.math.copysign(rounded, value);
}

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
    if (left.numeric) |number| {
        var none = std.heap.FixedBufferAllocator.init(&.{});
        var ctx: @import("numeric_value.zig").Context = .{ .alloc = none.allocator() };
        return @import("numeric_value.zig").order(&ctx, number.*, (right.numeric orelse return error.SqlTypeMismatch).*);
    }
    if (right.numeric != null) return error.SqlTypeMismatch;
    if (left.array) |array| {
        var work: json_order.Budget = .{};
        return array.compare((right.array orelse return error.SqlTypeMismatch).*, &work);
    }
    if (right.array != null) return error.SqlTypeMismatch;
    return compare(left.value, right.value);
}

pub fn semanticHashDatum(value: Datum) anyerror!u64 {
    if (value.sql_null) return 0;
    if (value.numeric) |number| {
        var none = std.heap.FixedBufferAllocator.init(&.{});
        var ctx: @import("numeric_value.zig").Context = .{ .alloc = none.allocator() };
        var hash = std.hash.Wyhash.init(0);
        try @import("numeric_value.zig").hash(&ctx, number.*, &hash);
        return hash.final();
    }
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

test "SQL NUMERIC JSON and text conversions never expose the typed placeholder" {
    const a = std.testing.allocator;
    const cases = [_]struct { sql: []const u8, expected: []const u8 }{
        .{ .sql = "to_jsonb('9007199254740993.1200'::numeric)", .expected = "9007199254740993.1200" },
        .{ .sql = "jsonb_build_object('n','9007199254740993.1200'::numeric)", .expected = "{\"n\":9007199254740993.1200}" },
        .{ .sql = "to_jsonb('Infinity'::numeric)", .expected = "\"Infinity\"" },
        .{ .sql = "concat('n=', '9007199254740993.1200'::numeric)", .expected = "\"n=9007199254740993.1200\"" },
        .{ .sql = "concat_ws(':','n','9007199254740993.1200'::numeric)", .expected = "\"n:9007199254740993.1200\"" },
        .{ .sql = "'32767.4'::numeric::smallint", .expected = "32767" },
    };
    for (cases) |case| {
        var compiled = try @import("compiler.zig").compileScalar(a, case.sql, .{});
        defer compiled.deinit();
        var program = try bindTyped(a, compiled.expression, &.{}, &.{}, .{});
        defer program.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const output = try program.evaluate(arena.allocator(), &.{}, &.{}, .{});
        try std.testing.expect(output.numeric == null and !output.sql_null);
        const encoded = try std.json.Stringify.valueAlloc(arena.allocator(), output.value, .{});
        try std.testing.expectEqualStrings(case.expected, encoded);
    }
}

test "SQL NUMERIC mixed comparisons and common expressions match PostgreSQL promotion" {
    const Harness = struct {
        fn run(a: Allocator) !void {
            const cases = [_]struct { sql: []const u8, expected: bool }{
                .{ .sql = "'2.0000001'::numeric = '2'::real", .expected = false },
                .{ .sql = "'9007199254740993'::numeric = '9007199254740992'::double precision", .expected = true },
                .{ .sql = "'2.0000001'::numeric = 2", .expected = false },
                .{ .sql = "'2.0000001'::numeric IN ('2'::real)", .expected = false },
                .{ .sql = "'2.0000001'::numeric IN ('2'::real,'3'::real)", .expected = false },
                .{ .sql = "CASE WHEN true THEN '2.0000001'::numeric ELSE '2'::real END = '2'::real", .expected = true },
                .{ .sql = "coalesce('2.0000001'::numeric,'2'::real) = '2'::real", .expected = true },
            };
            for (cases) |case| {
                var compiled = try @import("compiler.zig").compileScalar(a, case.sql, .{});
                defer compiled.deinit();
                var program = try bindTyped(a, compiled.expression, &.{}, &.{}, .{});
                defer program.deinit();
                var arena = std.heap.ArenaAllocator.init(a);
                defer arena.deinit();
                const output = try program.evaluate(arena.allocator(), &.{}, &.{}, .{});
                try std.testing.expect(!output.sql_null and output.value == .bool);
                try std.testing.expectEqual(case.expected, output.value.bool);
            }
        }
    };
    try Harness.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL NUMERIC scalar operators retain exact values and scale" {
    const a = std.testing.allocator;
    const cases = [_]struct { sql: []const u8, expected: []const u8 }{
        .{ .sql = "'9007199254740993.1200'::numeric + 1", .expected = "9007199254740994.1200" },
        .{ .sql = "'9007199254740993.1200'::numeric - 1", .expected = "9007199254740992.1200" },
        .{ .sql = "'1'::numeric / '3'::numeric", .expected = "0.33333333333333333333" },
        .{ .sql = "'1.20'::numeric * '2.30'::numeric", .expected = "2.7600" },
        .{ .sql = "'-5.50'::numeric % '2'::numeric", .expected = "-1.50" },
        .{ .sql = "-'9007199254740993.1200'::numeric", .expected = "-9007199254740993.1200" },
    };
    for (cases) |case| {
        var compiled = try @import("compiler.zig").compileScalar(a, case.sql, .{});
        defer compiled.deinit();
        var program = try bindTyped(a, compiled.expression, &.{}, &.{}, .{});
        defer program.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const output = try program.evaluate(arena.allocator(), &.{}, &.{}, .{});
        try std.testing.expect(!output.sql_null and output.numeric != null);
        var context: @import("numeric_value.zig").Context = .{ .alloc = arena.allocator() };
        try std.testing.expectEqualStrings(case.expected, try @import("numeric_value.zig").format(&context, output.numeric.?.*));
    }
}

test "SQL NUMERIC square root retains domain diagnostics strict nulls and request cancellation" {
    const a = std.testing.allocator;
    const Harness = struct {
        fn run(sql: []const u8) !void {
            var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, sql, .{});
            defer compiled.deinit();
            var program = try bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{});
            defer program.deinit();
            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();
            _ = try program.evaluate(arena.allocator(), &.{}, &.{}, .{});
        }
    };
    for ([_]struct { sql: []const u8, err: anyerror, code: []const u8 }{
        .{ .sql = "sqrt(-1.0)", .err = error.SqlInvalidPowerArgument, .code = "2201F" },
        .{ .sql = "sqrt('-Infinity'::numeric)", .err = error.SqlInvalidPowerArgument, .code = "2201F" },
        .{ .sql = "sqrt(-1)", .err = error.SqlInvalidPowerArgument, .code = "2201F" },
        .{ .sql = "sqrt('1'::text)", .err = error.SqlUndefinedFunction, .code = "42883" },
        .{ .sql = "sqrt(true)", .err = error.SqlUndefinedFunction, .code = "42883" },
        .{ .sql = "sqrt(ARRAY[1])", .err = error.SqlUndefinedFunction, .code = "42883" },
        .{ .sql = "sqrt('bad')", .err = error.SqlInvalidTextRepresentation, .code = "22P02" },
    }) |case| {
        try std.testing.expectError(case.err, Harness.run(case.sql));
        try std.testing.expectEqualStrings(case.code, @import("errors.zig").describe(case.err).code);
    }
    for ([_][]const u8{ "sqrt(NULL)", "sqrt(NULL::numeric)" }) |sql| try Harness.run(sql);
    for ([_][]const u8{
        "CASE WHEN false THEN ARRAY[sqrt(-1.0)] ELSE ARRAY[2.0] END",
        "COALESCE(ARRAY[2.0], ARRAY[sqrt(-1.0)])",
    }) |sql| try Harness.run(sql);
    var compiled = try @import("compiler.zig").compileScalar(a, "sqrt(n)", .{});
    defer compiled.deinit();
    var program = try bind(a, compiled.expression, &.{.{ .name = "n", .type = .number, .element_type = .numeric }}, &.{}, .{});
    defer program.deinit();
    const exact = @import("numeric_value.zig");
    var context: exact.Context = .{ .alloc = a };
    var number = try exact.parse(&context, "2");
    defer number.deinit();
    const Control = struct {
        calls: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == 2) return error.QueryCanceled;
        }
    };
    var control: Control = .{};
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const cells = [_]Datum{Datum.typedNumeric(&number.value)};
    try std.testing.expectError(error.QueryCanceled, program.evaluate(arena.allocator(), &cells, &.{}, .{ .checkpoint = Control.check, .checkpoint_context = &control }));
    try std.testing.expectEqual(@as(usize, 2), control.calls);
    try std.testing.expectError(error.SqlProgramLimitExceeded, program.evaluate(arena.allocator(), &cells, &.{}, .{ .steps = 20 }));
    try std.testing.expectError(error.SqlProgramLimitExceeded, program.evaluate(arena.allocator(), &cells, &.{}, .{ .output_bytes = 2 }));
    const result = try program.evaluate(arena.allocator(), &cells, &.{}, .{});
    const text = try exact.format(&context, result.numeric.?.*);
    defer a.free(text);
    try std.testing.expectEqualStrings("1.414213562373095", text);
}

test "SQL NUMERIC prepared literals rounding and integer array probes require no hot allocation" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "9007199254740993.1200",
        "round(1.255, 2)",
        "sqrt(2.0)",
        "abs(-1.2300)",
        "9007199254740993.0 = ANY(ARRAY[9007199254740992,9007199254740993])",
        "-9223372036854775808.0 = ANY(ARRAY[-9223372036854775808])",
    }) |sql| {
        var compiled = try @import("compiler.zig").compileScalar(a, sql, .{});
        var program = bind(a, compiled.expression, &.{}, &.{}, .{}) catch |err| {
            compiled.deinit();
            return err;
        };
        defer program.deinit();
        compiled.deinit();
        var none = std.heap.FixedBufferAllocator.init(&.{});
        for (0..1000) |_| {
            const result = try program.evaluate(none.allocator(), &.{}, &.{}, .{});
            try std.testing.expect(!result.sql_null);
            if (result.numeric == null) try std.testing.expectEqual(true, result.value.bool);
        }
    }
}

test "SQL scalar bound programs preserve lazy truth exact integers and function semantics" {
    const cases = [_]struct { sql: []const u8, expected: []const u8 }{
        .{ .sql = "1 + 2 * 3", .expected = "7" },
        .{ .sql = "trunc(-7.9)", .expected = "-7" },
        .{ .sql = "trunc(7.9)", .expected = "7" },
        .{ .sql = "trunc(9007199254740993)", .expected = "9007199254740992" },
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
        .{ .sql = "'bot_agent' LIKE 'bot!_%' ESCAPE '!'", .expected = "true" },
        .{ .sql = "'bot_agent' NOT LIKE 'bot!_%' ESCAPE '!'", .expected = "false" },
        .{ .sql = "'BOT_agent' ILIKE 'bot!_%' ESCAPE '!'", .expected = "true" },
        .{ .sql = "'a_b' LIKE 'aé_b' ESCAPE 'é'", .expected = "true" },
        .{ .sql = "'a%b' LIKE 'a%%b' ESCAPE '%'", .expected = "true" },
        .{ .sql = "'a_b' LIKE 'a__b' ESCAPE '_'", .expected = "true" },
        .{ .sql = "'a!xb' LIKE 'a!_b' ESCAPE ''", .expected = "true" },
        .{ .sql = "'a' LIKE 'a' ESCAPE NULL", .expected = "null" },
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
        var exact_context: @import("numeric_value.zig").Context = .{ .alloc = arena.allocator() };
        const logical: Json = if (value.numeric) |number| .{ .number_string = try @import("numeric_value.zig").format(&exact_context, number.*) } else value.value;
        const encoded = try std.json.Stringify.valueAlloc(arena.allocator(), logical, .{});
        try std.testing.expectEqualStrings(case.expected, encoded);
    }
}

test "SQL trunc and sign reject invalid types arity and nonfinite numeric inputs" {
    for ([_][]const u8{ "trunc('text')", "sign(TRUE)" }) |sql| {
        var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlTypeMismatch, bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{}));
    }
    for ([_][]const u8{ "trunc(1, 2, 3)", "sign()" }) |sql| {
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
