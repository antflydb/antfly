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

//! Optional access preparation over an authorized bound predicate. No reader,
//! readiness proof, filter removal or source-SQL rebinding belongs here.
const std = @import("std");
const scalar = @import("scalar.zig");
const catalog = @import("catalog.zig");
const native = @import("../storage/relational_index.zig");
const Budget = @import("memory_budget.zig");
const Allocator = std.mem.Allocator;
pub const Control = struct {
    include_conditions: bool = false,
    null_extended_columns: []const bool = &.{},
    ptr: ?*anyopaque = null,
    checkpoint: ?*const fn (?*anyopaque) anyerror!void = null,
    clock_values: ?*const @import("statement_clock.zig").Frame = null,
};
const State = struct {
    control: Control,
    failure: ?anyerror = null,
    fn poll(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        if (self.control.checkpoint) |check| check(self.control.ptr) catch |err| {
            self.failure = err;
            return err;
        };
    }
    fn optional(self: State, err: anyerror, budget: *Budget) !void {
        if (self.failure) |failure| return failure;
        if (err == error.OutOfMemory) {
            if (budget.isExhausted()) return;
            return err;
        }
        switch (err) {
            error.SqlMutableIndexExpression, error.RelationalIndexColumnNotFound, error.InvalidRelationalExpression, error.InvalidRelationalExpressionType, error.RelationalExpressionBudgetExceeded => return,
            else => {},
        }
        const code = @import("errors.zig").describe(err).code;
        if (std.mem.startsWith(u8, code, "22") or std.mem.eql(u8, code, "0A000") or std.mem.eql(u8, code, "54000")) return;
        return err;
    }
};

fn child(flags: []const bool, index: u32, before: usize) !bool {
    if (index >= before or index >= flags.len) return error.InvalidSqlProgram;
    return flags[index];
}

fn classify(a: Allocator, program: *const scalar.Program) ![]const bool {
    const flags = try a.alloc(bool, program.instructions.len);
    for (program.instructions, flags, 0..) |instruction, *flag, index| flag.* = switch (instruction.operation) {
        .literal, .parameter => true,
        .column => false,
        .unary => |part| try child(flags, part.operand, index),
        .cast => |part| try child(flags, part.operand, index),
        .binary => |part| (try child(flags, part.left, index)) and (try child(flags, part.right, index)),
        .call => |part| pure: {
            if (part.function.volatility() != .immutable) break :pure false;
            for (part.args) |arg| if (!try child(flags, arg, index)) break :pure false;
            break :pure true;
        },
        .case_when => |part| pure: {
            for (part.branches) |branch| if (!try child(flags, branch.condition, index) or !try child(flags, branch.value, index)) break :pure false;
            break :pure if (part.otherwise) |otherwise| try child(flags, otherwise, index) else true;
        },
        .in_list => |part| pure: {
            if (!try child(flags, part.operand, index)) break :pure false;
            for (part.values) |value| if (!try child(flags, value, index)) break :pure false;
            break :pure true;
        },
    };
    return flags;
}

pub const Prepared = struct {
    equalities: []const native.ExpressionEqualityHint = &.{},
    conditions: []const catalog.Condition = &.{},
    primary_key: ?[]const u8 = null,
};

pub fn canonicalElement(kind: @import("ast.zig").ColumnType, element: ?@import("array_value.zig").ElementType) ?@import("array_value.zig").ElementType {
    return scalar.columnElementType(kind, element) orelse switch (kind) {
        .string => .text,
        .boolean => .boolean,
        .json => .jsonb,
        .uuid => .uuid,
        else => null,
    };
}

fn nullableDependencies(a: Allocator, program: *const scalar.Program, columns: []const bool) ![]const bool {
    if (columns.len == 0) return &.{};
    const flags = try a.alloc(bool, program.instructions.len);
    for (program.instructions, flags, 0..) |instruction, *flag, index| flag.* = switch (instruction.operation) {
        .literal, .parameter => false,
        .column => |ordinal| if (ordinal < columns.len) columns[ordinal] else return error.InvalidSqlProgram,
        .unary => |part| try child(flags, part.operand, index),
        .cast => |part| try child(flags, part.operand, index),
        .binary => |part| (try child(flags, part.left, index)) or (try child(flags, part.right, index)),
        .call => |part| any: {
            for (part.args) |arg| if (try child(flags, arg, index)) break :any true;
            break :any false;
        },
        .case_when => |part| any: {
            for (part.branches) |branch| if (try child(flags, branch.condition, index) or try child(flags, branch.value, index)) break :any true;
            break :any if (part.otherwise) |otherwise| try child(flags, otherwise, index) else false;
        },
        .in_list => |part| any: {
            if (try child(flags, part.operand, index)) break :any true;
            for (part.values) |value| if (try child(flags, value, index)) break :any true;
            break :any false;
        },
    };
    return flags;
}

const Preparation = struct {
    output: Allocator,
    backing: Allocator,
    table: catalog.Table,
    columns: []const scalar.Column,
    schema: @import("../storage/schema.zig").TableSchema,
    program: *const scalar.Program,
    parameters: []const std.json.Value,
    constants: []const bool,
    nullable: []const bool,
    state: State,
    hints: std.ArrayList(native.ExpressionEqualityHint) = .empty,
    conditions: std.ArrayList(catalog.Condition) = .empty,
    primary_key: ?[]const u8 = null,

    fn limits(self: *@This()) scalar.EvalLimits {
        return .{ .output_bytes = @import("../storage/db/relational_index_limits.zig").max_stored_key_bytes, .clock_values = self.state.control.clock_values, .checkpoint = State.poll, .checkpoint_context = &self.state, .regex_checkpoint = State.poll, .regex_context = &self.state };
    }
    fn rejectsNull(self: *@This(), a: Allocator, root: u32, key_root: u32) !bool {
        if (self.nullable.len == 0 or !self.nullable[key_root]) return true;
        const cells = try a.alloc(scalar.Datum, self.columns.len);
        @memset(cells, .{});
        var selected = self.program.*;
        selected.root = root;
        selected.output_type = self.program.instructions[root].type;
        const result = try selected.evaluate(a, cells, self.parameters, self.limits());
        return result.sql_null or (result.value == .bool and !result.value.bool);
    }
    fn directColumn(self: *@This(), root: u32) ?catalog.Column {
        const instruction = self.program.instructions[root];
        if (instruction.operation != .column or instruction.operation.column >= self.columns.len) return null;
        const name = self.columns[instruction.operation.column].name;
        if (name.len == 0) return null;
        const column = self.table.column(name) catch return null;
        if (column.type != instruction.type.kind or canonicalElement(column.type, column.element_type) != canonicalElement(column.type, instruction.type.element_type) or !scalar.NumericModifier.eql(column.numeric_modifier, instruction.type.numeric_modifier)) return null;
        return switch (column.type) {
            .integer, .number, .boolean => column,
            .string => if (column.element_type == null or column.element_type == .text) column else null,
            else => null,
        };
    }

    fn key(self: *@This(), root: u32, key_root: u32, value_root: u32, op: catalog.Condition.Op) !void {
        if (key_root >= self.constants.len or value_root >= self.constants.len) return error.InvalidSqlProgram;
        if (self.constants[key_root] or !self.constants[value_root]) return;
        const direct = if (self.state.control.include_conditions and self.conditions.items.len < 256) self.directColumn(key_root) else null;
        if (direct) |column| if (std.mem.eql(u8, column.name, "_id") and (op != .eq or self.primary_key != null)) return;
        if (direct == null and (op != .eq or self.hints.items.len == 256)) return;
        try State.poll(&self.state);
        var budget: Budget = .{ .backing = self.backing, .limit = temporaryLimit(self.backing) };
        var scratch = std.heap.ArenaAllocator.init(budget.allocator());
        defer scratch.deinit();
        const a = scratch.allocator();
        var identity: ?native.ExpressionKeyIdentity = null;
        if (direct == null) {
            identity = @import("schema_expression.zig").indexInstructionIdentity(a, self.program, key_root, self.columns, self.schema, .{ .bytes = budget.limit, .steps = 65_536, .checkpoint = State.poll, .checkpoint_context = &self.state }) catch |err| {
                try self.state.optional(err, &budget);
                return;
            };
            const matches = found: {
                for (self.table.index_definitions) |definition| for (definition.keys) |candidate| {
                    if (candidate.expression != null and try candidate.matchesEquality(identity.?, null)) break :found true;
                };
                break :found false;
            };
            if (!matches) return;
        }
        const type_ = self.program.instructions[key_root].type;
        const source_type = self.program.instructions[value_root].type;
        if (type_.kind != source_type.kind or type_.kind == null or canonicalElement(type_.kind.?, type_.element_type) != canonicalElement(source_type.kind.?, source_type.element_type)) {
            const unknown_text_literal = type_.kind == .string and source_type.kind == .string and source_type.element_type == null and self.program.instructions[value_root].operation == .literal;
            const casts = @import("builtin_cast.zig");
            const integral_widening = type_.kind == .integer and source_type.kind == .integer and type_.element_type != null and source_type.element_type != null and casts.integral(type_.element_type.?) and casts.integral(source_type.element_type.?) and (try casts.commonNumeric(type_.element_type.?, source_type.element_type.?)) == type_.element_type.?;
            if (!unknown_text_literal and !integral_widening and !(type_.kind == .number and type_.element_type == .numeric and source_type.kind == .integer)) return;
        }
        if (!(self.rejectsNull(a, root, key_root) catch |err| {
            try self.state.optional(err, &budget);
            return;
        })) return;
        const value = self.program.evaluateInstructionWithLimits(a, value_root, self.parameters, self.limits()) catch |err| {
            try self.state.optional(err, &budget);
            return;
        };
        if (value.sql_null) return;
        const converted = @import("describe.zig").assignDatum(a, value, source_type.element_type, type_.kind.?, type_.element_type, self.limits()) catch |err| {
            try self.state.optional(err, &budget);
            return;
        };
        const wire = if (converted.numeric) |number| numeric: {
            var work: @import("numeric_value.zig").Context = .{ .alloc = a, .max_output_bytes = self.limits().output_bytes, .checkpoint = State.poll, .ptr = &self.state };
            break :numeric std.json.Value{ .string = @import("numeric_value.zig").format(&work, number.*) catch |err| {
                try self.state.optional(err, &budget);
                return;
            } };
        } else converted.value;
        if (wire == .string and wire.string.len > self.limits().output_bytes) return;
        if (wire == .float and !std.math.isFinite(wire.float)) return;
        if (converted.temporal() != null or converted.array != null or converted.patterns() != null) return;
        try State.poll(&self.state);
        const owned = try @import("../storage/typed_json.zig").clone(self.output, wire);
        if (direct) |column| {
            if (std.mem.eql(u8, column.name, "_id")) {
                if (owned != .string or owned.string.len == 0 or !std.unicode.utf8ValidateSlice(owned.string)) return;
                self.primary_key = owned.string;
            } else try self.conditions.append(self.output, .{ .column = column.name, .op = op, .value = owned });
        } else {
            try self.hints.append(self.output, .{ .identity = identity.?, .value = owned });
        }
    }

    fn walk(self: *@This(), root: u32, depth: usize) !void {
        if (depth >= 128 or root >= self.program.instructions.len) return error.InvalidSqlProgram;
        try State.poll(&self.state);
        const instruction = self.program.instructions[root];
        if (instruction.operation == .column and instruction.type.kind == .boolean and self.state.control.include_conditions and self.conditions.items.len < 256) {
            if (self.directColumn(root)) |column| try self.conditions.append(self.output, .{ .column = column.name, .op = .eq, .value = .{ .bool = true } });
            return;
        }
        if (instruction.operation == .unary and self.state.control.include_conditions and self.conditions.items.len < 256) {
            const part = instruction.operation.unary;
            if ((part.op == .not or part.op == .is_true or part.op == .is_false) and self.program.instructions[part.operand].type.kind == .boolean) {
                if (self.directColumn(part.operand)) |column| try self.conditions.append(self.output, .{ .column = column.name, .op = .eq, .value = .{ .bool = part.op == .is_true } });
                return;
            }
            if (part.op != .is_null and part.op != .is_not_null) return;
            if (self.directColumn(part.operand)) |column| {
                if (std.mem.eql(u8, column.name, "_id")) return;
                if (part.op == .is_null and self.nullable.len != 0 and self.nullable[part.operand]) return;
                try self.conditions.append(self.output, .{ .column = column.name, .op = if (part.op == .is_null) .is_null else .is_not_null });
            }
            return;
        }
        if (instruction.operation != .binary) return;
        const binary = instruction.operation.binary;
        if (binary.op == .@"and") {
            try self.walk(binary.left, depth + 1);
            return self.walk(binary.right, depth + 1);
        }
        const op: catalog.Condition.Op = switch (binary.op) {
            .eq => .eq,
            .neq => .neq,
            .lt => .lt,
            .lte => .lte,
            .gt => .gt,
            .gte => .gte,
            else => return,
        };
        try self.key(root, binary.left, binary.right, op);
        const reverse: catalog.Condition.Op = switch (op) {
            .lt => .gt,
            .lte => .gte,
            .gt => .lt,
            .gte => .lte,
            else => op,
        };
        try self.key(root, binary.right, binary.left, reverse);
    }
};

/// Caller supplies an authorized canonical ordinal layout and keeps the entire
/// SQL residual. This prepares access facts, never readiness or range proofs.
pub fn prepareAccess(output: Allocator, temporary: Allocator, table: catalog.Table, program: *const scalar.Program, columns: []const scalar.Column, parameters: []const std.json.Value, control: Control) !Prepared {
    if (table.storage_mode != .relational or table.external_base_source != null) return .{};
    if (control.null_extended_columns.len != 0 and control.null_extended_columns.len != columns.len) return error.InvalidSqlProgram;
    var has_expression = false;
    for (table.index_definitions) |definition| for (definition.keys) |key_| {
        if (key_.expression != null) has_expression = true;
    };
    if (!has_expression and !control.include_conditions) return .{};
    var state: State = .{ .control = control };
    try State.poll(&state);
    var memory: Budget = .{ .backing = temporary, .limit = temporaryLimit(temporary) };
    var scratch = std.heap.ArenaAllocator.init(memory.allocator());
    defer scratch.deinit();
    const schema = table.expressionSchemaLeaky(scratch.allocator()) catch |err| {
        try state.optional(err, &memory);
        return .{};
    };
    const constants = classify(scratch.allocator(), program) catch |err| {
        try state.optional(err, &memory);
        return .{};
    };
    const nullable = nullableDependencies(scratch.allocator(), program, control.null_extended_columns) catch |err| {
        try state.optional(err, &memory);
        return .{};
    };
    var preparation: Preparation = .{ .output = output, .backing = temporary, .table = table, .columns = columns, .schema = schema, .program = program, .parameters = parameters, .constants = constants, .nullable = nullable, .state = state };
    try preparation.walk(program.root, 0);
    return .{ .equalities = try preparation.hints.toOwnedSlice(output), .conditions = try preparation.conditions.toOwnedSlice(output), .primary_key = preparation.primary_key };
}

pub fn prepareEqualities(output: Allocator, temporary: Allocator, table: catalog.Table, program: *const scalar.Program, columns: []const scalar.Column, parameters: []const std.json.Value, control: Control) ![]const native.ExpressionEqualityHint {
    return (try prepareAccess(output, temporary, table, program, columns, parameters, control)).equalities;
}

fn temporaryLimit(backing: Allocator) usize {
    return @min(1024 * 1024, if (Budget.headroom(backing)) |available| available / 4 else 1024 * 1024);
}

test "SQL expression access prepares immutable equalities without evaluating unrelated errors" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const owned = arena.allocator();
    const columns = [_]scalar.Column{.{ .name = "email", .type = .string, .element_type = .text }};
    var table: catalog.Table = .{ .id = 1, .physical_name = "items", .schema_version = 1, .columns = &.{.{ .name = "email", .path = "email", .type = .string, .element_type = .text }} };
    var key_: catalog.IndexDefinition.Key = .{ .column = "", .expression = try std.json.parseFromSliceLeaky(std.json.Value, owned,
        \\{"op":"lower_ascii","args":[{"op":"column","column":"email"}]}
    , .{}), .result_type = .string, .collation = "C", .direction = .desc, .nulls = .first };
    key_.identity = try key_.bindIdentity(owned, try table.expressionSchemaLeaky(owned));
    const keys = [_]catalog.IndexDefinition.Key{key_};
    table.index_definitions = &.{.{ .name = "email_lower", .keys = &keys, .include_columns = &.{}, .where = &.{} }};
    for ([_][]const u8{
        "lower(email) = 'owner' AND 1/0 = 0",
        "'owner' = lower(email)",
        "lower(email) = upper('owner')",
        "upper(email) = 'owner'",
        "lower(email) = CAST(1/0 AS TEXT)",
        "lower(email) = CAST(now() AS TEXT)",
        "lower(email) = 'owner' OR lower(email) = 'other'",
    }, [_]usize{ 1, 1, 1, 0, 0, 0, 0 }, 0..) |sql, count, index| {
        var parsed = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer parsed.deinit();
        var program = try scalar.bindExpected(a, parsed.expression, &columns, &.{}, .boolean, .{});
        defer program.deinit();
        const root = program.root;
        const instructions = program.instructions.len;
        const hints = try prepareEqualities(owned, a, table, &program, &columns, &.{}, .{});
        try std.testing.expectEqual(count, hints.len);
        if (count != 0) {
            try std.testing.expectEqualStrings(if (index == 2) "OWNER" else "owner", hints[0].value.string);
            try std.testing.expectEqualDeep(key_.identity.?, hints[0].identity);
            try std.testing.expect(!hints[0].fold_ascii);
        }
        try std.testing.expectEqual(root, program.root);
        try std.testing.expectEqual(instructions, program.instructions.len);
    }
    var parsed = try @import("compiler.zig").compileScalar(a, "lower(email) = 'owner'", .{});
    defer parsed.deinit();
    var program = try scalar.bindExpected(a, parsed.expression, &columns, &.{}, .boolean, .{});
    defer program.deinit();
    var tiny: Budget = .{ .backing = a, .limit = 256 };
    var no_output = std.heap.FixedBufferAllocator.init(&.{});
    try std.testing.expectEqual(@as(usize, 0), (try prepareEqualities(no_output.allocator(), tiny.allocator(), table, &program, &columns, &.{}, .{})).len);
    try std.testing.expect(!tiny.isExhausted());
    try std.testing.expectEqual(@as(usize, 0), tiny.live);
    const Faults = struct {
        fn run(backing: Allocator, input: catalog.Table, predicate: *const scalar.Program, bound_columns: []const scalar.Column) !void {
            // Force every growth through an allocation cut: in-place resize
            // availability otherwise depends on the debug heap's address.
            var vtable = backing.vtable.*;
            vtable.resize = Allocator.noResize;
            vtable.remap = Allocator.noRemap;
            const deterministic: Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
            var result = std.heap.ArenaAllocator.init(deterministic);
            defer result.deinit();
            const hints = try prepareEqualities(result.allocator(), deterministic, input, predicate, bound_columns, &.{}, .{});
            try std.testing.expectEqual(@as(usize, 1), hints.len);
        }
        fn cancel(_: ?*anyopaque) !void {
            return error.SqlQueryCanceled;
        }
    };
    try std.testing.expectError(error.SqlQueryCanceled, prepareEqualities(owned, a, table, &program, &columns, &.{}, .{ .checkpoint = Faults.cancel }));
    try std.testing.checkAllAllocationFailures(a, Faults.run, .{ table, &program, @as([]const scalar.Column, &columns) });
}
