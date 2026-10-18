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

//! Bounded instruction-major expression kernels. Lazy or unsupported programs
//! fall back intact to scalar evaluation; unreachable errors stay unreachable.
const std = @import("std");
const scalar = @import("scalar.zig");
const Datum = scalar.Datum;
const Binary = @import("ast.zig").Scalar.Binary;

pub fn evaluate(a: std.mem.Allocator, program: *const scalar.Program, rows: []const []const Datum, parameters: []const std.json.Value) !?[]const Datum {
    return evaluateInput(a, program, RowInput{ .rows = rows, .count = rows.len }, parameters);
}
pub fn evaluateColumns(a: std.mem.Allocator, program: *const scalar.Program, page: @import("catalog.zig").ColumnPage, columns: []const scalar.Column, parameters: []const std.json.Value) anyerror!?[]const Datum {
    return evaluateInput(a, program, ColumnInput{ .page = page, .columns = columns, .count = page.selection.len }, parameters);
}
/// Evaluate only referenced dictionary entries. Programs with lazy evaluation or external effects keep the existing fused scalar/vector path.
/// Unreferenced entries must never raise errors or consume expression work.
pub fn evaluateDictionaryColumns(a: std.mem.Allocator, program: *const scalar.Program, page: @import("catalog.zig").ColumnPage, columns: []const scalar.Column, parameters: []const std.json.Value) !?@import("execution_batch.zig").Batch {
    if (!@import("typed_kernel.zig").supported(program) or program.required_columns.len == 0 or page.selection.len < 4) return null;
    var selection = (try dictionarySelection(a, program.required_columns, page, columns)) orelse return null;
    defer selection.deinit(a);
    var compact = page;
    compact.selection = selection.representatives.items;
    const values = (try evaluateColumns(a, program, compact, columns, parameters)) orelse return null;
    const indices = selection.indices;
    selection.indices = &.{};
    return .{ .dictionary = .{ .values = values, .indices = indices } };
}
const DictionarySelection = struct {
    indices: []u32,
    representatives: std.ArrayList(usize),
    fn deinit(self: *DictionarySelection, a: std.mem.Allocator) void {
        a.free(self.indices);
        self.representatives.deinit(a);
    }
};
fn dictionarySelection(a: std.mem.Allocator, ordinals: []const u32, page: @import("catalog.zig").ColumnPage, columns: []const scalar.Column) !?DictionarySelection {
    if (ordinals.len == 1) return dictionarySelectionSingle(a, ordinals[0], page, columns);
    if (ordinals.len == 0 or page.selection.len < 4) return null;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const owned = arena.allocator();
    const definitions = try owned.alloc(scalar.Column, ordinals.len);
    for (ordinals, definitions) |ordinal, *definition| {
        if (ordinal >= columns.len) return error.InvalidSqlBackendResponse;
        definition.* = columns[ordinal];
    }
    const source: @import("execution_batch.zig").Batch = .{ .columns = .{ .page = page, .definitions = definitions } };
    // Full tuples are compared after hashing. Page-local IDs are identities,
    // never semantic values, and NULL has its own identity in each column.
    const tuple = try owned.alloc(u64, ordinals.len);
    var ids: std.StringHashMapUnmanaged(u32) = .empty;
    defer ids.deinit(a);
    var representatives: std.ArrayList(usize) = .empty;
    var transferred = false;
    defer if (!transferred) representatives.deinit(a);
    const indices = try a.alloc(u32, page.selection.len);
    defer if (!transferred) a.free(indices);
    for (page.selection, indices, 0..) |physical, *index, row| {
        for (tuple, 0..) |*id, column| id.* = (try source.dictionaryIdentity(row, column)) orelse return null;
        const bytes = std.mem.sliceAsBytes(tuple);
        if (ids.get(bytes)) |id| {
            index.* = id;
        } else {
            if (representatives.items.len >= page.selection.len / 2) return null;
            const key = try owned.dupe(u8, bytes);
            const id: u32 = @intCast(representatives.items.len);
            try ids.put(a, key, id);
            try representatives.append(a, physical);
            index.* = id;
        }
    }
    transferred = true;
    return .{ .indices = indices, .representatives = representatives };
}
fn dictionarySelectionSingle(a: std.mem.Allocator, ordinal: u32, page: @import("catalog.zig").ColumnPage, columns: []const scalar.Column) !?DictionarySelection {
    if (page.selection.len < 4) return null;
    if (ordinal >= columns.len) return error.InvalidSqlBackendResponse;
    const source = if (page.native == null) page.batch.findColumn(columns[ordinal].name) else null;
    const native_ordinal = if (page.native) |native| for (native.names, 0..) |name, index| {
        if (std.mem.eql(u8, name, columns[ordinal].name)) break index;
    } else return null else null;
    if (source) |column| switch (column.values) {
        .dictionary_bytes, .dictionary_i64, .dictionary_f64 => {},
        else => return null,
    } else if (native_ordinal == null) return null;
    if (page.native) |native| {
        const physical = page.selection[0];
        if (physical >= native.values.len()) return error.InvalidSqlBackendResponse;
        if ((try native.values.dictionaryIdentity(physical, native_ordinal.?)) == null) return null;
    }
    var ids: std.AutoHashMapUnmanaged(u64, u32) = .empty;
    defer ids.deinit(a);
    var representatives: std.ArrayList(usize) = .empty;
    var owned_representatives = true;
    defer if (owned_representatives) representatives.deinit(a);
    const indices = try a.alloc(u32, page.selection.len);
    var owned = true;
    defer if (owned) a.free(indices);
    for (page.selection, indices) |physical, *index| {
        if (page.native) |native| if (physical >= native.values.len()) return error.InvalidSqlBackendResponse;
        const id: u64 = if (source) |column|
            if (try column.dictionaryId(physical)) |entry| @as(u64, entry) + 1 else 0
        else
            (try page.native.?.values.dictionaryIdentity(physical, native_ordinal.?)) orelse return null;
        const slot = try ids.getOrPut(a, id);
        if (!slot.found_existing) {
            slot.value_ptr.* = @intCast(representatives.items.len);
            try representatives.append(a, physical);
        }
        index.* = slot.value_ptr.*;
    }
    if (representatives.items.len > page.selection.len / 2) return null;
    owned = false;
    owned_representatives = false;
    return .{ .indices = indices, .representatives = representatives };
}

fn constantOperand(program: *const scalar.Program, index: u32, depth: usize) bool {
    if (depth == 32 or index >= program.instructions.len) return false;
    return switch (program.instructions[index].operation) {
        .literal, .parameter => true,
        .cast => |cast| constantOperand(program, cast.operand, depth + 1),
        .unary => |part| constantOperand(program, part.operand, depth + 1),
        else => false,
    };
}
fn decimalOperand(program: *const scalar.Program, index_: u32) ?u32 {
    var index = index_;
    for (0..32) |_| switch (program.instructions[index].operation) {
        .column => |ordinal| return ordinal,
        .cast => |cast| {
            if (cast.type != .number or cast.element_type != .numeric or cast.numeric_modifier != null) return null;
            index = cast.operand;
        },
        else => return null,
    };
    return null;
}
fn decimalComparison(a: std.mem.Allocator, program: *const scalar.Program, page: @import("catalog.zig").ColumnPage, columns: []const scalar.Column, parameters: []const std.json.Value) !?@import("execution_batch.zig").Batch {
    if (page.selection.len == 0) return null;
    const comparison_binary = switch (program.instructions[program.root].operation) {
        .binary => |part| part,
        else => return null,
    };
    var op: @import("catalog.zig").Condition.Op = switch (comparison_binary.op) {
        .eq => .eq,
        .neq => .neq,
        .lt => .lt,
        .lte => .lte,
        .gt => .gt,
        .gte => .gte,
        else => return null,
    };
    var ordinal = decimalOperand(program, comparison_binary.left);
    var constant = comparison_binary.right;
    if (ordinal == null or !constantOperand(program, constant, 0)) {
        ordinal = decimalOperand(program, comparison_binary.right);
        constant = comparison_binary.left;
        if (ordinal == null or !constantOperand(program, constant, 0)) return null;
        op = switch (op) {
            .lt => .gt,
            .lte => .gte,
            .gt => .lt,
            .gte => .lte,
            else => op,
        };
    }
    if (ordinal.? >= columns.len) return error.InvalidSqlBackendResponse;
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const sa = scratch.allocator();
    const Batch = @import("execution_batch.zig").Batch;
    const source: Batch = .{ .columns = .{ .page = page, .definitions = columns } };
    const decimal = (try source.decimalColumnSelected(sa, ordinal.?, null)) orelse return null;
    const view = decimal.physical;
    for (0..view.len()) |row| {
        if (!view.vector.nulls.isNull(view.rowAt(row))) break;
    } else return null;
    var literal_program = program.*;
    literal_program.root = constant;
    const literal = try literal_program.evaluate(sa, &.{}, parameters, .{});
    const numeric = @import("numeric_value.zig");
    var integer: numeric.ScaledInteger = undefined;
    const number: ?numeric.Value = if (literal.sql_null) null else if (literal.numeric) |value| value.* else if (literal.value == .integer) number: {
        integer = try numeric.ScaledInteger.init(literal.value.integer, 0);
        break :number integer.view();
    } else return null;
    const coefficients = view.vector.values.decimal128;
    const maximum = try @import("../common/decimal128.zig").coefficientLimit(coefficients.precision, coefficients.scale);
    const constant_coefficient = if (number) |value| (if (decimal.scales.len == 0) numeric.coefficientAtScale(value, @intCast(coefficients.scale)) else null) else null;
    const values = try a.alloc(bool, view.len());
    errdefer a.free(values);
    const nulls = try a.alloc(u8, view.len());
    errdefer a.free(nulls);
    var context: numeric.Context = .{ .alloc = sa };
    for (values, nulls, 0..) |*value, *flag, lane| {
        const row = view.rowAt(lane);
        flag.* = @intFromBool(number == null or view.vector.nulls.isNull(row));
        value.* = false;
        if (flag.* != 0) continue;
        if (@abs(coefficients.values[row]) >= maximum) return error.InvalidDecimal128;
        const scale = try decimal.scaleAt(lane);
        if (scale > coefficients.precision) return error.InvalidDecimal128;
        const fixed_constant = constant_coefficient orelse (if (number) |value_| numeric.coefficientAtScale(value_, scale) else null);
        const order = if (fixed_constant) |fixed| std.math.order(coefficients.values[row], fixed) else general: {
            const fixed = try numeric.ScaledInteger.init(coefficients.values[row], scale);
            break :general try numeric.order(&context, fixed.view(), number.?);
        };
        value.* = switch (op) {
            .eq => order == .eq,
            .neq => order != .eq,
            .lt => order == .lt,
            .lte => order != .gt,
            .gt => order == .gt,
            .gte => order != .lt,
            else => unreachable,
        };
    }
    return try Batch.fromBoolean(a, values, nulls);
}

/// Bounded instruction-major decimal arithmetic. Coefficients and SQL NULL
/// remain vectors across projection, hashing and reduction. Unsupported scale,
/// specials, rounding and overflow decline the complete expression intact.
const DecimalArithmetic = struct {
    const Vector = struct {
        values: []i128,
        nulls: []u8,
        scale: u16,
        scales: []const u16 = &.{},
        fn value(self: @This(), row: usize) i128 {
            return self.values[if (self.values.len == 1) 0 else row];
        }
        fn flag(self: @This(), row: usize) u8 {
            return self.nulls[if (self.nulls.len == 1) 0 else row];
        }
        fn scaleAt(self: @This(), row: usize) u16 {
            return if (self.scales.len == 0) self.scale else self.scales[row];
        }
    };
    const Entry = struct { program: *const scalar.Program, index: u32, vector: Vector };
    cache: *std.ArrayList(Entry),

    a: std.mem.Allocator,
    program: *const scalar.Program,
    source: @import("execution_batch.zig").Batch,
    parameters: []const std.json.Value,
    remaining: usize = 64,
    fn allocate(self: *@This(), scale: u16) !Vector {
        const values = try self.a.alloc(i128, self.source.len());
        const nulls = try self.a.alloc(u8, self.source.len());
        @memset(values, 0);
        @memset(nulls, 0);
        return .{ .values = values, .nulls = nulls, .scale = scale };
    }
    fn scaled(value: i128, from: u16, to: u16) ?i128 {
        if (to < from or to - from > 38) return null;
        const powers = comptime power: {
            var values: [39]i128 = undefined;
            values[0] = 1;
            for (1..values.len) |i| values[i] = values[i - 1] * 10;
            break :power values;
        };
        return std.math.mul(i128, value, powers[to - from]) catch null;
    }
    fn quotient(left: i128, right: i128, exponent: i32) ?i128 {
        if (right == 0) return null;
        var numerator: u256 = @abs(left);
        var denominator: u256 = @abs(right);
        const powers = comptime power: {
            var values: [77]u256 = undefined;
            values[0] = 1;
            for (1..values.len) |i| values[i] = values[i - 1] * 10;
            break :power values;
        };
        const shift = @abs(exponent);
        if (shift >= powers.len) return null;
        if (exponent >= 0) numerator = std.math.mul(u256, numerator, powers[shift]) catch return null else denominator = std.math.mul(u256, denominator, powers[shift]) catch return null;
        var coefficient = numerator / denominator;
        const remainder = numerator % denominator;
        if (remainder >= denominator - remainder) coefficient = std.math.add(u256, coefficient, 1) catch return null;
        const magnitude = std.math.cast(i128, coefficient) orelse return null;
        return if ((left < 0) != (right < 0)) -magnitude else magnitude;
    }
    fn run(self: *@This(), index: u32, depth: usize) anyerror!?Vector {
        if (depth == 32 or self.remaining == 0) return null;
        for (self.cache.items) |entry| if (same(self.program, index, entry.program, entry.index, 0)) return entry.vector;
        const result = (try self.runUncached(index, depth)) orelse return null;
        if (self.cache.items.len < 512) try self.cache.append(self.a, .{ .program = self.program, .index = index, .vector = result });
        return result;
    }
    fn same(l: *const scalar.Program, li: u32, r: *const scalar.Program, ri: u32, depth: usize) bool {
        if (depth == 32 or l.invocation != r.invocation or l.settings != r.settings) return false;
        const left = l.instructions[li];
        const right = r.instructions[ri];
        if (!std.meta.eql(left.type, right.type) or std.meta.activeTag(left.operation) != std.meta.activeTag(right.operation)) return false;
        return switch (left.operation) {
            .column => |v| v == right.operation.column,
            .parameter => |v| v == right.operation.parameter,
            .literal => |v| blk: {
                const other = right.operation.literal;
                if (std.meta.activeTag(v) != std.meta.activeTag(other)) break :blk false;
                break :blk switch (v) {
                    .null => true,
                    .integer => |n| n == other.integer,
                    .float => |n| @as(u64, @bitCast(n)) == @as(u64, @bitCast(other.float)),
                    .bool => |n| n == other.bool,
                    .string => |v_| std.mem.eql(u8, v_, other.string),
                    .number_string => |v_| std.mem.eql(u8, v_, other.number_string),
                    else => false,
                };
            },
            .unary => |v| v.op == right.operation.unary.op and same(l, v.operand, r, right.operation.unary.operand, depth + 1),
            .binary => |v| v.op == right.operation.binary.op and same(l, v.left, r, right.operation.binary.left, depth + 1) and same(l, v.right, r, right.operation.binary.right, depth + 1),
            .cast => |v| v.type == right.operation.cast.type and v.element_type == right.operation.cast.element_type and std.meta.eql(v.numeric_modifier, right.operation.cast.numeric_modifier) and same(l, v.operand, r, right.operation.cast.operand, depth + 1),
            else => false,
        };
    }
    fn runUncached(self: *@This(), index: u32, depth: usize) anyerror!?Vector {
        if (depth == 32 or self.remaining == 0) return null;
        self.remaining -= 1;
        const instruction = self.program.instructions[index];
        const number = @import("numeric_value.zig");
        if (constantOperand(self.program, index, 0)) {
            var constant = self.program.*;
            constant.root = index;
            const datum = try constant.evaluate(self.a, &.{}, self.parameters, .{});
            const scale: u16 = if (datum.numeric) |value| value.scale else 0;
            if (scale > 38) return null;
            const value = if (datum.sql_null) @as(i128, 0) else if (datum.numeric) |value| number.coefficientAtScale(value.*, scale) orelse return null else if (datum.value == .integer) @as(i128, datum.value.integer) else return null;
            const result: Vector = .{ .values = try self.a.dupe(i128, &.{value}), .nulls = try self.a.dupe(u8, &.{@intFromBool(datum.sql_null)}), .scale = scale };
            return result;
        }
        switch (instruction.operation) {
            .column => |ordinal| {
                if (try self.source.decimalColumnSelected(self.a, ordinal, null)) |decimal| {
                    const coefficients = decimal.physical.vector.values.decimal128;
                    const maximum = try @import("../common/decimal128.zig").coefficientLimit(coefficients.precision, coefficients.scale);
                    var result = try self.allocate(@intCast(coefficients.scale));
                    var scales: []u16 = &.{};
                    if (decimal.scales.len != 0) scales = try self.a.alloc(u16, self.source.len());
                    result.scales = scales;
                    for (result.values, result.nulls, 0..) |*value, *flag, row| {
                        const physical = decimal.physical.rowAt(row);
                        flag.* = @intFromBool(decimal.physical.vector.nulls.isNull(physical));
                        if (scales.len != 0) scales[row] = 0;
                        if (flag.* != 0) continue;
                        const scale = try decimal.scaleAt(row);
                        if (scale > coefficients.precision) return error.InvalidDecimal128;
                        if (scales.len != 0) scales[row] = scale;
                        value.* = coefficients.values[physical];
                        if (@abs(value.*) >= maximum) return error.InvalidDecimal128;
                    }
                    return result;
                }
                const physical = (try self.source.physicalColumn(self.a, ordinal)) orelse return null;
                if (physical.kind != .integer) return null;
                const result = try self.allocate(0);
                for (result.values, result.nulls, 0..) |*value, *flag, row| {
                    const datum = try physical.cell(row);
                    flag.* = @intFromBool(datum.sql_null);
                    if (!datum.sql_null) value.* = datum.value.integer;
                }
                return result;
            },
            .cast => |cast| {
                if (cast.type != .number or cast.element_type != .numeric or cast.numeric_modifier != null) return null;
                return self.run(cast.operand, depth + 1);
            },
            .unary => |part| {
                if (instruction.type.element_type != .numeric or (part.op != .positive and part.op != .negative)) return null;
                const operand = (try self.run(part.operand, depth + 1)) orelse return null;
                if (part.op == .positive) return operand;
                var result = try self.allocate(operand.scale);
                result.scales = operand.scales;
                for (result.values, result.nulls, 0..) |*value, *flag, row| {
                    flag.* = operand.flag(row);
                    if (flag.* == 0) value.* = std.math.negate(operand.value(row)) catch return null;
                }
                return result;
            },
            .binary => |part| {
                if (instruction.type.element_type != .numeric) return null;
                switch (part.op) {
                    .add, .subtract, .multiply, .divide, .modulo => {},
                    else => return null,
                }
                const left = (try self.run(part.left, depth + 1)) orelse return null;
                const right = (try self.run(part.right, depth + 1)) orelse return null;
                // Scale groups share a coefficient vector. Per-lane scales retain
                // PostgreSQL display semantics without scalar replay of the cohort.
                var scales: []u16 = &.{};
                if (part.op == .divide or left.scales.len != 0 or right.scales.len != 0) scales = try self.a.alloc(u16, self.source.len());
                const uniform = if (part.op == .multiply) left.scale + right.scale else @max(left.scale, right.scale);
                var result = try self.allocate(@min(38, uniform));
                result.scales = scales;
                var selected: ?u16 = null;
                var mixed = false;
                for (result.values, result.nulls, 0..) |*value, *flag, row| {
                    flag.* = @intFromBool(left.flag(row) != 0 or right.flag(row) != 0);
                    if (scales.len != 0) scales[row] = 0;
                    if (flag.* != 0) continue;
                    const ls = left.scaleAt(row);
                    const rs = right.scaleAt(row);
                    var scale = if (part.op == .multiply) ls + rs else @max(ls, rs);
                    const l_ = left.value(row);
                    const r_ = right.value(row);
                    if (part.op == .divide) {
                        if (r_ == 0) return error.SqlDivisionByZero;
                        const lf = try number.ScaledInteger.init(l_, ls);
                        const rf = try number.ScaledInteger.init(r_, rs);
                        scale = number.divisionScale(lf.view(), rf.view());
                    }
                    if (scale > 38) return null;
                    if (selected != null and selected.? != scale) mixed = true;
                    selected = scale;
                    if (scales.len != 0) scales[row] = scale;
                    const maximum = try @import("../common/decimal128.zig").coefficientLimit(38, scale);
                    const l = if (part.op == .multiply or part.op == .divide) l_ else scaled(l_, ls, scale) orelse return null;
                    const r = if (part.op == .multiply or part.op == .divide) r_ else scaled(r_, rs, scale) orelse return null;
                    value.* = switch (part.op) {
                        .add => std.math.add(i128, l, r) catch return null,
                        .subtract => std.math.sub(i128, l, r) catch return null,
                        .multiply => std.math.mul(i128, l, r) catch return null,
                        .divide => quotient(l, r, @as(i32, scale) + rs - ls) orelse return null,
                        .modulo => if (r == 0) return error.SqlDivisionByZero else if (l == std.math.minInt(i128) and r == -1) 0 else @rem(l, r),
                        else => unreachable,
                    };
                    if (@abs(value.*) >= maximum) return null;
                }
                result.scale = selected orelse 0;
                if (!mixed) result.scales = &.{};
                return result;
            },
            else => return null,
        }
    }
};
const DecimalWorkspace = struct {
    budget: @import("memory_budget.zig"),
    arena: std.heap.ArenaAllocator = undefined,
    cache: std.ArrayList(DecimalArithmetic.Entry) = .empty,
    disabled: bool = false,
    fn init(self: *@This(), a: std.mem.Allocator) void {
        self.* = .{ .budget = .{ .backing = a, .limit = @min(8 * 1024 * 1024, (@import("memory_budget.zig").headroom(a) orelse (32 * 1024 * 1024)) / 4) } };
        self.arena = .init(self.budget.allocator());
    }
};
fn decimalArithmetic(a: std.mem.Allocator, program: *const scalar.Program, page: @import("catalog.zig").ColumnPage, columns: []const scalar.Column, parameters: []const std.json.Value) !?@import("execution_batch.zig").Batch {
    var workspace: DecimalWorkspace = undefined;
    workspace.init(a);
    defer workspace.arena.deinit();
    return decimalArithmeticWith(&workspace, a, program, page, columns, parameters);
}
fn decimalArithmeticResult(workspace: *DecimalWorkspace, program: *const scalar.Program, page: @import("catalog.zig").ColumnPage, columns: []const scalar.Column, parameters: []const std.json.Value) !?DecimalArithmetic.Vector {
    if (program.output_type.element_type != .numeric or page.selection.len == 0 or program.required_columns.len == 0) return null;
    // No eager constant subtree may cross an empty/NULL input boundary.
    // All-NULL source cohorts keep the scalar evaluator's existing laziness.
    if (workspace.disabled) return null;
    const sa = workspace.arena.allocator();
    const Batch = @import("execution_batch.zig").Batch;
    const source: Batch = .{ .columns = .{ .page = page, .definitions = columns } };
    var has_decimal = false;
    for (program.required_columns) |ordinal| if (try source.decimalColumnSelected(sa, ordinal, null)) |decimal| {
        var present = false;
        for (0..decimal.physical.len()) |row| if (!decimal.physical.vector.nulls.isNull(decimal.physical.rowAt(row))) {
            present = true;
            break;
        };
        if (!present) return null;
        has_decimal = true;
    };
    if (!has_decimal) return null;
    var kernel: DecimalArithmetic = .{ .a = sa, .program = program, .source = source, .parameters = parameters, .cache = &workspace.cache };
    return kernel.run(program.root, 0);
}
fn decimalArithmeticWith(workspace: *DecimalWorkspace, a: std.mem.Allocator, program: *const scalar.Program, page: @import("catalog.zig").ColumnPage, columns: []const scalar.Column, parameters: []const std.json.Value) !?@import("execution_batch.zig").Batch {
    const Batch = @import("execution_batch.zig").Batch;
    const result = (decimalArithmeticResult(workspace, program, page, columns, parameters) catch |err| {
        if (err != error.OutOfMemory or !workspace.budget.isLocallyExhausted()) return err;
        workspace.disabled = true;
        workspace.cache = .empty;
        _ = workspace.arena.reset(.free_all);
        return null;
    }) orelse return null;
    const values = try a.alloc(i128, page.selection.len);
    for (values, 0..) |*value, row| value.* = result.value(row);
    errdefer a.free(values);
    const nulls = try a.alloc(u8, page.selection.len);
    for (nulls, 0..) |*flag, row| flag.* = result.flag(row);
    errdefer a.free(nulls);
    const scales = try a.dupe(u16, result.scales);
    errdefer a.free(scales);
    return try Batch.fromDecimalScales(a, values, nulls, result.scale, scales);
}

/// Pure decimal forests execute one bounded register frame per lane. Sources
/// and constants are shared; only final projections own full coefficient vectors.
const FusedDecimal = struct {
    const Lane = struct { coefficient: i128 = 0, scale: u16 = 0, null: bool = false, valid: bool = true };
    const Node = struct {
        program: *const scalar.Program,
        index: u32,
        operation: union(enum) {
            constant: Lane,
            decimal: @import("execution_batch.zig").DecimalColumn,
            integer: @import("execution_batch.zig").PhysicalColumn,
            sign: struct { child: u16, negative: bool },
            binary: struct { left: u16, right: u16, op: Binary },
            alias: u16,
        },
    };
    nodes: std.ArrayList(Node) = .empty,
    roots: []?u16,
    a: std.mem.Allocator,
    source: @import("execution_batch.zig").Batch,
    parameters: []const std.json.Value,
    fn add(self: *@This(), program: *const scalar.Program, index: u32, depth: usize) anyerror!?u16 {
        if (depth >= 32 or self.nodes.items.len >= 512) return null;
        for (self.nodes.items, 0..) |node, slot| if (DecimalArithmetic.same(program, index, node.program, node.index, 0)) return @intCast(slot);
        const instruction = program.instructions[index];
        const Operation = @FieldType(Node, "operation");
        const operation: Operation = if (constantOperand(program, index, 0)) blk: {
            var constant = program.*;
            constant.root = index;
            const datum = try constant.evaluate(self.a, &.{}, self.parameters, .{});
            if (datum.sql_null) break :blk .{ .constant = .{ .null = true } };
            const scale: u16 = if (datum.numeric) |value| value.scale else 0;
            if (scale > 38) return null;
            const coefficient = if (datum.numeric) |value| @import("numeric_value.zig").coefficientAtScale(value.*, scale) orelse return null else if (datum.value == .integer) @as(i128, datum.value.integer) else return null;
            @import("../common/decimal128.zig").validateCoefficient(coefficient, 38, scale) catch return null;
            break :blk .{ .constant = .{ .coefficient = coefficient, .scale = scale } };
        } else switch (instruction.operation) {
            .column => |ordinal| blk: {
                if (try self.source.decimalColumnSelected(self.a, ordinal, null)) |decimal| break :blk .{ .decimal = decimal };
                const physical = (try self.source.physicalColumn(self.a, ordinal)) orelse return null;
                if (physical.kind != .integer) return null;
                try physical.validate();
                break :blk .{ .integer = physical };
            },
            .cast => |cast| blk: {
                if (cast.type != .number or cast.element_type != .numeric or cast.numeric_modifier != null) return null;
                break :blk .{ .alias = (try self.add(program, cast.operand, depth + 1)) orelse return null };
            },
            .unary => |part| blk: {
                if (instruction.type.element_type != .numeric or (part.op != .positive and part.op != .negative)) return null;
                break :blk .{ .sign = .{ .child = (try self.add(program, part.operand, depth + 1)) orelse return null, .negative = part.op == .negative } };
            },
            .binary => |part| blk: {
                if (instruction.type.element_type != .numeric) return null;
                switch (part.op) {
                    .add, .subtract, .multiply, .divide, .modulo => {},
                    else => return null,
                }
                const left = (try self.add(program, part.left, depth + 1)) orelse return null;
                const right = (try self.add(program, part.right, depth + 1)) orelse return null;
                break :blk .{ .binary = .{ .left = left, .right = right, .op = part.op } };
            },
            else => return null,
        };
        if (self.nodes.items.len >= 512) return null;
        const slot: u16 = @intCast(self.nodes.items.len);
        try self.nodes.append(self.a, .{ .program = program, .index = index, .operation = operation });
        return slot;
    }
    fn prepare(self: *@This(), programs: []const *const scalar.Program, visited: []const bool) !void {
        @memset(self.roots, null);
        for (programs, visited, self.roots) |program, done, *root| {
            if (done or program.output_type.element_type != .numeric or program.required_columns.len == 0 or program.output_type.numeric_modifier != null) continue;
            const begin = self.nodes.items.len;
            root.* = try self.add(program, program.root, 0);
            if (root.* == null) self.nodes.shrinkRetainingCapacity(begin);
        }
    }
    fn binary(left: Lane, right: Lane, op: Binary) !Lane {
        if (!left.valid or !right.valid) return .{ .valid = false };
        if (left.null or right.null) return .{ .null = true };
        var scale = if (op == .multiply) left.scale + right.scale else @max(left.scale, right.scale);
        if (op == .divide) {
            if (right.coefficient == 0) return error.SqlDivisionByZero;
            const numeric = @import("numeric_value.zig");
            const l = try numeric.ScaledInteger.init(left.coefficient, left.scale);
            const r = try numeric.ScaledInteger.init(right.coefficient, right.scale);
            scale = numeric.divisionScale(l.view(), r.view());
        }
        if (scale > 38) return .{ .valid = false };
        const l = if (op == .multiply or op == .divide) left.coefficient else DecimalArithmetic.scaled(left.coefficient, left.scale, scale) orelse return .{ .valid = false };
        const r = if (op == .multiply or op == .divide) right.coefficient else DecimalArithmetic.scaled(right.coefficient, right.scale, scale) orelse return .{ .valid = false };
        const coefficient = switch (op) {
            .add => std.math.add(i128, l, r) catch return .{ .valid = false },
            .subtract => std.math.sub(i128, l, r) catch return .{ .valid = false },
            .multiply => std.math.mul(i128, l, r) catch return .{ .valid = false },
            .divide => DecimalArithmetic.quotient(l, r, @as(i32, scale) + right.scale - left.scale) orelse return .{ .valid = false },
            .modulo => if (r == 0) return error.SqlDivisionByZero else @rem(l, r),
            else => unreachable,
        };
        if (@abs(coefficient) >= 100000000000000000000000000000000000000) return .{ .valid = false };
        return .{ .coefficient = coefficient, .scale = scale };
    }
    fn evaluate(self: *@This(), row: usize, registers: []Lane) !void {
        for (self.nodes.items, registers) |node, *out| out.* = switch (node.operation) {
            .constant => |value| value,
            .decimal => |decimal| blk: {
                const physical = decimal.physical.rowAt(row);
                if (decimal.physical.vector.nulls.isNull(physical)) break :blk .{ .null = true };
                const values = decimal.physical.vector.values.decimal128;
                const scale = try decimal.scaleAt(row);
                try @import("../common/decimal128.zig").validateCoefficient(values.values[physical], values.precision, scale);
                break :blk .{ .coefficient = values.values[physical], .scale = scale };
            },
            .integer => |physical| blk: {
                const value = try physical.cell(row);
                break :blk if (value.sql_null) .{ .null = true } else .{ .coefficient = value.value.integer };
            },
            .alias => |slot| registers[slot],
            .sign => |part| blk: {
                var value = registers[part.child];
                if (part.negative and value.valid and !value.null) value.coefficient = -value.coefficient;
                break :blk value;
            },
            .binary => |part| try FusedDecimal.binary(registers[part.left], registers[part.right], part.op),
        };
    }
};
fn fusedDecimals(a: std.mem.Allocator, programs: []const *const scalar.Program, page: @import("catalog.zig").ColumnPage, columns: []const scalar.Column, parameters: []const std.json.Value, outputs: []?@import("execution_batch.zig").Batch, visited: []bool) !void {
    if (page.selection.len == 0 or programs.len > 512) return;
    var workspace: DecimalWorkspace = undefined;
    workspace.init(a);
    defer workspace.arena.deinit();
    const sa = workspace.arena.allocator();
    var roots: [512]?u16 = undefined;
    var plan: FusedDecimal = .{ .a = sa, .roots = roots[0..programs.len], .source = .{ .columns = .{ .page = page, .definitions = columns } }, .parameters = parameters };
    plan.prepare(programs, visited) catch |err| {
        if (err == error.OutOfMemory and workspace.budget.isLocallyExhausted()) return;
        return err;
    };
    if (for (plan.roots) |root| {
        if (root != null) break false;
    } else true) return;
    const Output = struct {
        values: []i128,
        nulls: []u8,
        scales: []u16,
        failed: bool = false,
        first: ?u16 = null,
        mixed: bool = false,
        fn free(self: @This(), alloc: std.mem.Allocator) void {
            alloc.free(self.values);
            alloc.free(self.nulls);
            alloc.free(self.scales);
        }
    };
    var results: [512]?Output = @splat(null);
    defer for (results[0..programs.len]) |result| if (result) |value| value.free(a);
    for (plan.roots, results[0..programs.len]) |root, *result| {
        if (root == null) continue;
        const values = try a.alloc(i128, page.selection.len);
        errdefer a.free(values);
        const nulls = try a.alloc(u8, page.selection.len);
        errdefer a.free(nulls);
        const scales = try a.alloc(u16, page.selection.len);
        result.* = .{ .values = values, .nulls = nulls, .scales = scales };
    }
    var registers: [512]FusedDecimal.Lane = undefined;
    for (0..page.selection.len) |row| {
        try plan.evaluate(row, registers[0..plan.nodes.items.len]);
        for (plan.roots, results[0..programs.len]) |root, *result| if (root) |slot| {
            const value = registers[slot];
            const out = &result.*.?;
            if (!value.valid) out.failed = true;
            out.values[row] = value.coefficient;
            out.nulls[row] = @intFromBool(value.null);
            out.scales[row] = value.scale;
            if (!value.null and value.valid) {
                if (out.first) |scale| if (scale != value.scale) {
                    out.mixed = true;
                };
                out.first = value.scale;
            }
        };
    }
    for (results[0..programs.len], outputs, visited) |*result, *output, *done| if (result.*) |*value| {
        if (value.failed) continue;
        if (!value.mixed) {
            a.free(value.scales);
            value.scales = &.{};
        }
        output.* = try @import("execution_batch.zig").Batch.fromDecimalScales(a, value.values, value.nulls, value.first orelse 0, value.scales);
        result.* = null;
        done.* = true;
    };
}

/// Retain expression dictionaries across projection instead of expanding them
/// into per-row Datum vectors. Ordinary expressions still share fused gathers.
pub fn evaluateColumnsEncodedMany(a: std.mem.Allocator, programs: []const *const scalar.Program, page: @import("catalog.zig").ColumnPage, columns: []const scalar.Column, parameters: []const std.json.Value, io: ?std.Io) ![]const ?@import("execution_batch.zig").Batch {
    const Batch = @import("execution_batch.zig").Batch;
    const outputs = try a.alloc(?Batch, programs.len);
    @memset(outputs, null);
    errdefer freeEncodedMany(a, outputs);
    var fallback: std.ArrayList(*const scalar.Program) = .empty;
    defer fallback.deinit(a);
    var positions: std.ArrayList(usize) = .empty;
    defer positions.deinit(a);
    const visited = try a.alloc(bool, programs.len);
    defer a.free(visited);
    @memset(visited, false);
    // Identity expressions borrow column views, including exact arrays,
    // NUMERIC and JSON-null values. Scalar consumers normalize at access;
    // physical consumers validate only their final selected primitive lanes.
    for (programs, 0..) |program, index| {
        if (program.root != 0 or program.instructions.len != 1) continue;
        const ordinal = switch (program.instructions[0].operation) {
            .column => |column| column,
            else => continue,
        };
        if (ordinal >= columns.len) return error.InvalidSqlBackendResponse;
        const definitions = try a.dupe(scalar.Column, &.{columns[ordinal]});
        var published = false;
        defer if (!published) a.free(definitions);
        const physical: Batch = .{ .columns = .{ .page = page, .definitions = definitions } };
        outputs[index] = physical;
        visited[index] = true;
        published = true;
    }
    try fusedDecimals(a, programs, page, columns, parameters, outputs, visited);
    var decimal_workspace: DecimalWorkspace = undefined;
    decimal_workspace.init(a);
    defer decimal_workspace.arena.deinit();
    for (programs, 0..) |program, index| {
        if (visited[index]) continue;
        if (try decimalArithmeticWith(&decimal_workspace, a, program, page, columns, parameters)) |native_arithmetic| {
            outputs[index] = native_arithmetic;
            visited[index] = true;
            continue;
        }
        if (try decimalComparison(a, program, page, columns, parameters)) |native_comparison| {
            outputs[index] = native_comparison;
            visited[index] = true;
        }
    }
    var group: std.ArrayList(*const scalar.Program) = .empty;
    defer group.deinit(a);
    var group_positions: std.ArrayList(usize) = .empty;
    defer group_positions.deinit(a);
    for (programs, 0..) |program, index| {
        if (visited[index]) continue;
        if (!@import("typed_kernel.zig").supported(program) or program.required_columns.len == 0) continue;
        const ordinals = program.required_columns;
        var selection = (try dictionarySelection(a, ordinals, page, columns)) orelse continue;
        defer selection.deinit(a);
        group.clearRetainingCapacity();
        group_positions.clearRetainingCapacity();
        for (programs[index..], index..) |candidate, position| {
            if (!visited[position] and @import("typed_kernel.zig").supported(candidate) and std.mem.eql(u32, candidate.required_columns, ordinals)) {
                try group.append(a, candidate);
                try group_positions.append(a, position);
                visited[position] = true;
            }
        }
        var compact = page;
        compact.selection = selection.representatives.items;
        // Gather the dictionary IDs once, then share typed expression nodes
        // across projections over the same referenced input tuples.
        const vectors = try evaluateColumnsMany(a, group.items, compact, columns, parameters);
        defer {
            for (vectors) |values| if (values) |vector| a.free(vector);
            a.free(vectors);
        }
        for (vectors, group_positions.items, 0..) |optional, position, offset| if (optional) |values| {
            const indices = try a.dupe(u32, selection.indices);
            outputs[position] = .{ .dictionary = .{ .values = values, .indices = indices } };
            @constCast(vectors)[offset] = null;
        };
    }
    group.clearRetainingCapacity();
    group_positions.clearRetainingCapacity();
    for (programs, 0..) |program, index| if (outputs[index] == null and @import("typed_kernel.zig").supported(program)) {
        try group.append(a, program);
        try group_positions.append(a, index);
    };
    if (try evaluateEncodedScheduled(a, group.items, page, columns, parameters, io)) |encoded| {
        defer a.free(encoded);
        // No fallible operations between ownership transfer and publication.
        for (encoded, group_positions.items) |vector, index| if (vector) |owned| {
            outputs[index] = owned.batch();
        };
    }
    for (programs, 0..) |program, index| if (outputs[index] == null) {
        try fallback.append(a, program);
        try positions.append(a, index);
    };
    const evaluated = try evaluateColumnsManyScheduled(a, fallback.items, page, columns, parameters, io);
    defer {
        for (evaluated) |vector| if (vector) |values| a.free(values);
        a.free(evaluated);
    }
    for (evaluated, positions.items, 0..) |optional, index, position| if (optional) |values| {
        const vectors = try a.alloc([]const Datum, 1);
        vectors[0] = values;
        outputs[index] = .{ .vectors = .{ .values = vectors, .count = values.len } };
        @constCast(evaluated)[position] = null;
    };
    return outputs;
}

fn evaluateEncodedPart(a: std.mem.Allocator, programs: []const *const scalar.Program, page: @import("catalog.zig").ColumnPage, columns: []const scalar.Column, parameters: []const std.json.Value) anyerror!?[]const ?*@import("typed_kernel.zig").Encoded {
    return @import("typed_kernel.zig").evaluateEncoded(a, programs, ColumnInput{ .page = page, .columns = columns, .count = page.selection.len }, parameters);
}
fn evaluateEncodedScheduled(a: std.mem.Allocator, programs: []const *const scalar.Program, page: @import("catalog.zig").ColumnPage, columns: []const scalar.Column, parameters: []const std.json.Value, io: ?std.Io) !?[]const ?*@import("typed_kernel.zig").Encoded {
    const kernel = @import("typed_kernel.zig");
    var work: usize = 0;
    for (programs) |program| work +|= program.instructions.len;
    const lanes = scheduledLanes(page.selection.len, work);
    if (io == null or lanes == 1) return evaluateEncodedPart(a, programs, page, columns, parameters);
    const scheduling = @import("parallel_scheduler.zig");
    var locked: scheduling.LockedAllocator = .{ .backing = a };
    const alloc = locked.allocator();
    var tasks: [4]?scheduling.Task(anyerror!?[]const ?*kernel.Encoded) = @splat(null);
    var outputs: [4]?[]const ?*kernel.Encoded = @splat(null);
    defer for (outputs) |output| if (output) |vectors| {
        for (vectors) |vector| if (vector) |owned| kernel.Encoded.destroy(owned, alloc);
        alloc.free(vectors);
    };
    var failure: ?anyerror = null;
    for (0..lanes) |lane| {
        var part = page;
        part.selection = page.selection[lane * page.selection.len / lanes .. (lane + 1) * page.selection.len / lanes];
        tasks[lane] = scheduling.global().submit(io.?, (work +| columns.len +| 1) *| part.selection.len *| @sizeOf(Datum), evaluateEncodedPart, .{ alloc, programs, part, columns, parameters });
        if (tasks[lane] == null) outputs[lane] = evaluateEncodedPart(alloc, programs, part, columns, parameters) catch |err| blk: {
            failure = failure orelse err;
            break :blk null;
        };
    }
    // Join every owner before freeing either successful or failed lanes.
    for (tasks[0..lanes], 0..) |*task, lane| if (task.*) |*active| {
        outputs[lane] = active.await(io.?) catch |err| blk: {
            failure = failure orelse err;
            break :blk null;
        };
        task.* = null;
    };
    if (failure) |err| return err;
    for (outputs[0..lanes]) |output| if (output == null) return null;
    const joined = try a.alloc(?*kernel.Encoded, programs.len);
    @memset(joined, null);
    errdefer {
        for (joined) |vector| if (vector) |owned| kernel.Encoded.destroy(owned, a);
        a.free(joined);
    }
    for (joined, 0..) |*output, column| {
        var parts: [4]*const kernel.Encoded = undefined;
        for (outputs[0..lanes], parts[0..lanes]) |lane, *part| part.* = lane.?[column] orelse return error.InvalidSqlBackendResponse;
        output.* = try kernel.Encoded.concat(a, parts[0..lanes]);
    }
    return joined;
}

pub fn freeEncodedMany(a: std.mem.Allocator, outputs: []const ?@import("execution_batch.zig").Batch) void {
    for (outputs) |optional| if (optional) |batch| switch (batch) {
        .dictionary => |encoded| {
            a.free(encoded.values);
            a.free(encoded.indices);
        },
        .vectors => |vectors| {
            for (vectors.values) |values| a.free(values);
            a.free(vectors.values);
        },
        .columns => |physical| a.free(physical.definitions),
        .reader => |reader| if (reader.free_owned) |free| free(reader.ptr, a),
        else => unreachable,
    };
    a.free(outputs);
}

pub fn evaluateColumnsScheduled(a: std.mem.Allocator, program: *const scalar.Program, page: @import("catalog.zig").ColumnPage, columns: []const scalar.Column, parameters: []const std.json.Value, io: ?std.Io) !?[]const Datum {
    const count = scheduledLanes(page.selection.len, program.instructions.len);
    if (io == null or count == 1) return evaluateColumns(a, program, page, columns, parameters);
    const scheduling = @import("parallel_scheduler.zig");
    var allocator: scheduling.LockedAllocator = .{ .backing = a };
    const worker_alloc = allocator.allocator();
    var pending: [4]?scheduling.Task(anyerror!?[]const Datum) = @splat(null);
    var outputs: [4]?[]const Datum = @splat(null);
    defer for (&pending) |*task| if (task.*) |*active| {
        _ = active.cancel(io.?) catch {};
    };
    defer for (outputs) |output| if (output) |values| worker_alloc.free(values);
    var failure: ?anyerror = null;
    for (0..count) |lane| {
        var part = page;
        part.selection = page.selection[lane * page.selection.len / count .. (lane + 1) * page.selection.len / count];
        const reservation = (program.instructions.len +| 1) *| part.selection.len *| @sizeOf(Datum);
        pending[lane] = scheduling.global().submit(io.?, reservation, evaluateColumns, .{ worker_alloc, program, part, columns, parameters });
        if (pending[lane] == null) outputs[lane] = evaluateColumns(worker_alloc, program, part, columns, parameters) catch |err| blk: {
            failure = failure orelse err;
            break :blk null;
        };
    }
    for (pending[0..count], 0..) |*task, lane| if (task.*) |*active| {
        outputs[lane] = active.await(io.?) catch |err| blk: {
            failure = failure orelse err;
            break :blk null;
        };
        task.* = null;
    };
    if (failure) |err| return err;
    for (outputs[0..count]) |output| if (output == null) return null;
    const result = try a.alloc(Datum, page.selection.len);
    for (outputs[0..count], 0..) |output, lane| @memcpy(result[lane * page.selection.len / count .. (lane + 1) * page.selection.len / count], output.?);
    return result;
}
/// Parallel work must have multiple useful lanes and enough expression work
/// to amortize task admission, joining and result assembly.
fn scheduledLanes(rows: usize, instructions: usize) usize {
    if (rows < 2048 or rows *| instructions < 8192) return 1;
    return @min(@as(usize, 4), rows / 1024);
}
/// Fuse column normalization across a set of expressions. Unsupported/lazy
/// programs decline before touching their inputs, preserving short-circuiting.
pub fn evaluateColumnsMany(a: std.mem.Allocator, programs: []const *const scalar.Program, page: @import("catalog.zig").ColumnPage, columns: []const scalar.Column, parameters: []const std.json.Value) anyerror![]const ?[]const Datum {
    const memo = try a.alloc(?[]const Datum, columns.len);
    defer a.free(memo);
    @memset(memo, null);
    defer for (memo) |values| if (values) |vector| a.free(vector);
    const outputs = try a.alloc(?[]const Datum, programs.len);
    errdefer a.free(outputs);
    @memset(outputs, null);
    errdefer for (outputs) |values| if (values) |vector| a.free(vector);
    const input = MemoInput{ .base = .{ .page = page, .columns = columns, .count = page.selection.len }, .memo = memo, .count = page.selection.len };
    var supported: std.ArrayList(*const scalar.Program) = .empty;
    defer supported.deinit(a);
    var positions: std.ArrayList(usize) = .empty;
    defer positions.deinit(a);
    for (programs, 0..) |program, index| if (@import("typed_kernel.zig").supported(program)) {
        try supported.append(a, program);
        try positions.append(a, index);
    };
    if (try @import("typed_kernel.zig").evaluate(a, supported.items, input, parameters)) |vectors| {
        defer a.free(vectors);
        for (vectors, positions.items) |vector, index| outputs[index] = vector;
    } else for (programs, outputs) |program, *output| output.* = try evaluateInput(a, program, input, parameters);
    return outputs;
}
pub fn evaluateColumnsManyScheduled(a: std.mem.Allocator, programs: []const *const scalar.Program, page: @import("catalog.zig").ColumnPage, columns: []const scalar.Column, parameters: []const std.json.Value, io: ?std.Io) ![]const ?[]const Datum {
    var work: usize = 0;
    for (programs) |program| work +|= program.instructions.len;
    const lanes = scheduledLanes(page.selection.len, work);
    if (io == null or lanes == 1) return evaluateColumnsMany(a, programs, page, columns, parameters);
    const scheduler = @import("parallel_scheduler.zig");
    var locked: scheduler.LockedAllocator = .{ .backing = a };
    const alloc = locked.allocator();
    var tasks: [4]?scheduler.Task(anyerror![]const ?[]const Datum) = @splat(null);
    var outputs: [4]?[]const ?[]const Datum = @splat(null);
    defer for (&tasks) |*task| if (task.*) |*active| {
        _ = active.cancel(io.?) catch &.{};
    };
    defer for (outputs) |output| if (output) |vectors| {
        for (vectors) |vector| if (vector) |values| alloc.free(values);
        alloc.free(vectors);
    };
    var failure: ?anyerror = null;
    for (0..lanes) |lane| {
        var part = page;
        part.selection = page.selection[lane * page.selection.len / lanes .. (lane + 1) * page.selection.len / lanes];
        tasks[lane] = scheduler.global().submit(io.?, (work +| columns.len +| 1) *| part.selection.len *| @sizeOf(Datum), evaluateColumnsMany, .{ alloc, programs, part, columns, parameters });
        if (tasks[lane] == null) outputs[lane] = evaluateColumnsMany(alloc, programs, part, columns, parameters) catch |err| blk: {
            failure = failure orelse err;
            break :blk null;
        };
    }
    for (tasks[0..lanes], 0..) |*task, lane| if (task.*) |*active| {
        const result = active.await(io.?);
        task.* = null;
        outputs[lane] = result catch |err| blk: {
            failure = failure orelse err;
            break :blk null;
        };
    };
    if (failure) |err| return err;
    const joined = try a.alloc(?[]const Datum, programs.len);
    @memset(joined, null);
    errdefer {
        for (joined) |vector| if (vector) |values| a.free(values);
        a.free(joined);
    }
    for (joined, 0..) |*vector, column| {
        const supported = for (outputs[0..lanes]) |output| {
            if (output.?[column] == null) break false;
        } else true;
        if (!supported) continue;
        const values = try a.alloc(Datum, page.selection.len);
        vector.* = values;
        for (outputs[0..lanes], 0..) |output, lane| @memcpy(values[lane * page.selection.len / lanes .. (lane + 1) * page.selection.len / lanes], output.?[column].?);
    }
    return joined;
}
const MemoInput = struct {
    base: ColumnInput,
    memo: []?[]const Datum,
    count: usize,
    pub fn fillTyped(self: MemoInput, a: std.mem.Allocator, ordinal: u32, target: anytype) !bool {
        return self.base.fillTyped(a, ordinal, target);
    }
    pub fn fillColumn(self: MemoInput, a: std.mem.Allocator, ordinal: u32, output: []Datum) !void {
        if (ordinal >= self.memo.len) return error.InvalidSqlBackendResponse;
        if (self.memo[ordinal] == null) {
            const values = try a.alloc(Datum, self.count);
            errdefer a.free(values);
            try self.base.fillColumn(a, ordinal, values);
            self.memo[ordinal] = values;
        }
        @memcpy(output, self.memo[ordinal].?);
    }
    pub fn cell(self: MemoInput, a: std.mem.Allocator, index: usize, ordinal: u32) !Datum {
        return self.base.cell(a, index, ordinal);
    }
};
pub fn evaluateBatch(a: std.mem.Allocator, program: *const scalar.Program, batch: @import("execution_batch.zig").Batch, parameters: []const std.json.Value) !?[]const Datum {
    return evaluateInput(a, program, BatchInput{ .batch = batch, .count = batch.len() }, parameters);
}
const BatchInput = struct {
    batch: @import("execution_batch.zig").Batch,
    count: usize,
    pub fn cell(self: BatchInput, a: std.mem.Allocator, index: usize, ordinal: u32) !Datum {
        return self.batch.cell(a, index, ordinal);
    }
    pub fn fillTyped(self: BatchInput, a: std.mem.Allocator, ordinal: u32, target: anytype) !bool {
        const physical = (try self.batch.physicalColumn(a, ordinal)) orelse return false;
        return target.fillPhysical(physical);
    }
    pub fn fillColumn(self: BatchInput, a: std.mem.Allocator, ordinal: u32, output: []Datum) !void {
        for (output, 0..) |*value, index| value.* = try self.cell(a, index, ordinal);
    }
};
const RowInput = struct {
    rows: []const []const Datum,
    count: usize,
    pub fn cell(self: RowInput, _: std.mem.Allocator, index: usize, ordinal: u32) !Datum {
        if (ordinal >= self.rows[index].len) return error.InvalidSqlBackendResponse;
        return self.rows[index][ordinal];
    }
};
const ColumnInput = struct {
    page: @import("catalog.zig").ColumnPage,
    columns: []const scalar.Column,
    count: usize,
    pub fn fillTyped(self: ColumnInput, a: std.mem.Allocator, ordinal: u32, target: anytype) !bool {
        if (ordinal >= self.columns.len) return error.InvalidSqlBackendResponse;
        const definition = self.columns[ordinal];
        if (definition.element_type == .numeric) return false;
        const batch: @import("execution_batch.zig").Batch = .{ .columns = .{ .page = self.page, .definitions = &.{definition} } };
        if (try batch.physicalColumn(a, 0)) |physical| return target.fillPhysical(physical);
        // Row producers and JSON-null columns keep exact scalar semantics.
        return false;
    }
    pub fn fillColumn(self: ColumnInput, a: std.mem.Allocator, ordinal: u32, output: []Datum) !void {
        if (ordinal >= self.columns.len) return error.InvalidSqlBackendResponse;
        const definition = self.columns[ordinal];
        const column = self.page.batch.findColumn(definition.name) orelse {
            for (output, 0..) |*value, index| value.* = try self.cell(a, index, ordinal);
            return;
        };
        if (column.values == .decimal128 or column.values == .encoded) {
            for (output, 0..) |*value, row| value.* = try self.cell(a, row, ordinal);
            return;
        }
        if (column.values == .dictionary_bytes) {
            const dictionary = column.values.dictionary_bytes;
            const normalized = try a.alloc(?Datum, dictionary.values.len);
            defer a.free(normalized);
            @memset(normalized, null);
            for (self.page.selection, output) |index, *value| {
                if (index >= self.page.batch.rowCount()) return error.InvalidSqlBackendResponse;
                if (column.nulls.isNull(index)) {
                    value.* = .{};
                    continue;
                }
                const id = dictionary.indices[index];
                if (id >= normalized.len) return error.InvalidSqlBackendResponse;
                if (normalized[id] == null) normalized[id] = try @import("describe.zig").coerceDatum(a, Datum.json(.{ .string = dictionary.values[id] }), definition.type, definition.element_type);
                value.* = normalized[id].?;
            }
            return;
        }
        for (self.page.selection, output) |index, *value| {
            if (index >= self.page.batch.rowCount()) return error.InvalidSqlBackendResponse;
            if (column.nulls.isNull(index)) {
                value.* = .{};
                continue;
            }
            const raw: std.json.Value = switch (column.values) {
                .dictionary_i64 => |values| .{ .integer = values.at(index) },
                .dictionary_f64 => |values| .{ .float = values.at(index) },
                .i64 => |values| .{ .integer = values[index] },
                .f64 => |values| .{ .float = values[index] },
                .bool => |values| .{ .bool = values[index] },
                .bytes => |values| .{ .string = values[index] },
                .json => |values| try std.json.parseFromSliceLeaky(std.json.Value, a, values[index], .{ .parse_numbers = false }),
                else => return error.UnsupportedSqlExecution,
            };
            value.* = try @import("describe.zig").coerceDatum(a, Datum.json(raw), definition.type, definition.element_type);
        }
    }
    pub fn cell(self: ColumnInput, a: std.mem.Allocator, index: usize, ordinal: u32) !Datum {
        if (ordinal >= self.columns.len) return error.InvalidSqlBackendResponse;
        const value = try self.page.typedCell(a, index, self.columns[ordinal].name, self.columns[ordinal].element_type == .numeric);
        return @import("describe.zig").coerceDatum(a, value, self.columns[ordinal].type, self.columns[ordinal].element_type);
    }
};
fn evaluateInput(a: std.mem.Allocator, program: *const scalar.Program, inputs: anytype, parameters: []const std.json.Value) !?[]const Datum {
    const vectors = (try @import("typed_kernel.zig").evaluate(a, &.{program}, inputs, parameters)) orelse return null;
    defer a.free(vectors);
    return vectors[0];
}
pub fn unary(op: @import("ast.zig").Scalar.Unary, value: Datum) !Datum {
    if (op == .is_null or op == .is_not_null or op == .is_unknown or op == .is_not_unknown) return Datum.json(.{ .bool = value.sql_null == (op == .is_null or op == .is_unknown) });
    if (value.array != null or value.numeric != null) return error.SqlTypeMismatch;
    if (op == .is_true or op == .is_not_true or op == .is_false or op == .is_not_false) {
        const target = op == .is_true or op == .is_not_true;
        const matches = value.value == .bool and value.value.bool == target;
        return Datum.json(.{ .bool = matches != (op == .is_not_true or op == .is_not_false) });
    }
    if (value.value == .null) return .{};
    return switch (op) {
        .positive => if (value.value == .integer or value.value == .float) value else error.SqlTypeMismatch,
        .negative => switch (value.value) {
            .integer => |v| Datum.json(.{ .integer = std.math.negate(v) catch return error.SqlNumericOutOfRange }),
            .float => |v| Datum.json(.{ .float = -v }),
            else => error.SqlTypeMismatch,
        },
        .not => if (value.value == .bool) Datum.json(.{ .bool = !value.value.bool }) else error.SqlTypeMismatch,
        else => unreachable,
    };
}
fn comparison(op: Binary) bool {
    return switch (op) {
        .eq, .neq, .lt, .lte, .gt, .gte => true,
        else => false,
    };
}
pub fn binary(op: Binary, left: Datum, right: Datum) !Datum {
    if (op == .is_distinct or op == .is_not_distinct) {
        const equal = if (left.sql_null or right.sql_null) left.sql_null and right.sql_null else (try scalar.compareDatums(left, right)) == .eq;
        return Datum.json(.{ .bool = equal == (op == .is_not_distinct) });
    }
    if (left.sql_null or right.sql_null) return .{};
    if (comparison(op)) return Datum.json(scalar.comparison(op, try scalar.compareDatums(left, right)));
    if (left.array != null or right.array != null or left.numeric != null or right.numeric != null) return error.SqlTypeMismatch;
    // Arithmetic shares the scalar overflow/division/finite-number contract.
    // JSON null is a value for comparison, but remains null in arithmetic.
    if (left.value == .null or right.value == .null) return .{};
    return Datum.fromJson(try scalar.arithmetic(op, left.value, right.value));
}

test "SQL vector kernels match scalar exact integers nulls arithmetic and lazy fallbacks" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const cells = [_][]const Datum{
        &.{Datum.json(.{ .integer = 9007199254740993 })}, &.{Datum.json(.{ .integer = 9007199254740992 })},
        &.{Datum.json(.{ .integer = 1 })},                &.{Datum.json(.{ .integer = 2 })},
        &.{.{}},                                          &.{Datum.json(.null)},
    };
    for ([_][]const u8{ "n > 9007199254740992", "n IS NULL", "n IS DISTINCT FROM NULL", "n + 1", "n * 2", "n / 2", "n % 2", "+n" }) |sql| {
        var compiled = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer compiled.deinit();
        var program = try scalar.bind(a, compiled.expression, &.{.{ .name = "n", .type = .integer }}, &.{}, .{});
        defer program.deinit();
        const vector = (try evaluate(alloc, &program, &cells, &.{})).?;
        for (cells, vector) |row, value| {
            const expected = try program.evaluate(alloc, row, &.{}, .{});
            try std.testing.expectEqual(expected.sql_null, value.sql_null);
            try std.testing.expectEqualDeep(expected.value, value.value);
        }
    }
    var lazy = try @import("compiler.zig").compileScalar(a, "CASE WHEN TRUE THEN 7 ELSE 1 / 0 END", .{});
    defer lazy.deinit();
    var program = try scalar.bind(a, lazy.expression, &.{}, &.{}, .{});
    defer program.deinit();
    try std.testing.expect((try evaluate(alloc, &program, &.{&.{}}, &.{})) == null);
    try std.testing.expectEqual(@as(i64, 7), (try program.evaluate(alloc, &.{}, &.{}, .{})).value.integer);
}

test "SQL review column kernels honor declared integer coercion" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var compiled = try @import("compiler.zig").compileScalar(a, "n + 1", .{});
    defer compiled.deinit();
    const columns = [_]scalar.Column{.{ .name = "n", .type = .integer }};
    var program = try scalar.bind(a, compiled.expression, &columns, &.{}, .{});
    defer program.deinit();
    const page: @import("catalog.zig").ColumnPage = .{ .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &.{.{ .relational_key = "r" }}, .columns = &.{.{ .name = "n", .values = .{ .bytes = &.{"41"} } }} }, .selection = &.{0} };
    const raw = try page.cell(arena.allocator(), 0, "n");
    const coerced = try @import("describe.zig").coerceAlloc(arena.allocator(), raw.value, .integer);
    const expected = try program.evaluate(arena.allocator(), &.{Datum.json(coerced)}, &.{}, .{});
    try std.testing.expectEqual(@as(i64, 42), expected.value.integer);
    const actual = (try evaluateColumns(arena.allocator(), &program, page, &columns, &.{})).?;
    try std.testing.expectEqualDeep(expected, actual[0]);
}

test "SQL vector arithmetic preserves scalar overflow and mixed numeric semantics" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    for ([_][]const u8{ "n + 1", "n * 2", "n / -1" }) |sql| {
        var compiled = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer compiled.deinit();
        var program = try scalar.bind(a, compiled.expression, &.{.{ .name = "n", .type = .integer }}, &.{}, .{});
        defer program.deinit();
        const edge: i64 = if (std.mem.eql(u8, sql, "n / -1")) std.math.minInt(i64) else std.math.maxInt(i64);
        const row = [_]Datum{Datum.json(.{ .integer = edge })};
        const cells = [_][]const Datum{ &row, &row, &row, &row };
        try std.testing.expectError(error.SqlNumericOutOfRange, program.evaluate(arena.allocator(), cells[0], &.{}, .{}));
        try std.testing.expectError(error.SqlNumericOutOfRange, evaluate(arena.allocator(), &program, &cells, &.{}));
    }
    const cells = [_][]const Datum{ &.{Datum.json(.{ .integer = 9007199254740993 })}, &.{Datum.json(.{ .float = 2.5 })}, &.{.{}}, &.{Datum.json(.null)} };
    for ([_][]const u8{ "n + 0.5", "n > 9007199254740992", "n IS NOT DISTINCT FROM NULL" }) |sql| {
        var compiled = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer compiled.deinit();
        var program = try scalar.bind(a, compiled.expression, &.{.{ .name = "n", .type = .number }}, &.{}, .{});
        defer program.deinit();
        const vector = (try evaluate(arena.allocator(), &program, &cells, &.{})).?;
        for (cells, vector) |row, value| try std.testing.expectEqualDeep(try program.evaluate(arena.allocator(), row, &.{}, .{}), value);
    }
}

test "SQL vector live workspace supports long expressions floats strings and boolean unary kernels" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const cells = [_][]const Datum{ &.{Datum.json(.{ .float = 2.5 })}, &.{Datum.json(.{ .float = -0.0 })}, &.{Datum.json(.{ .float = 9 })}, &.{Datum.json(.{ .float = -4 })}, &.{.{}}, &.{Datum.json(.null)} };
    for ([_][]const u8{ "n + 1.5", "n * 0.5", "n / 2.0", "n >= 0.0", "-n", "(n > 0.0) IS NOT TRUE", "NOT (n > 0.0)" }) |sql| {
        var compiled = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer compiled.deinit();
        var program = try scalar.bind(a, compiled.expression, &.{.{ .name = "n", .type = .number }}, &.{}, .{});
        defer program.deinit();
        const result = (try evaluate(arena.allocator(), &program, &cells, &.{})).?;
        for (cells, result) |row, actual| try std.testing.expectEqualDeep(try program.evaluate(arena.allocator(), row, &.{}, .{}), actual);
    }
    var text = try @import("compiler.zig").compileScalar(a, "s < 'beta'", .{});
    defer text.deinit();
    var text_program = try scalar.bind(a, text.expression, &.{.{ .name = "s", .type = .string }}, &.{}, .{});
    defer text_program.deinit();
    const strings = [_][]const Datum{ &.{Datum.json(.{ .string = "alpha" })}, &.{Datum.json(.{ .string = "beta" })}, &.{.{}}, &.{Datum.json(.null)} };
    const result = (try evaluate(arena.allocator(), &text_program, &strings, &.{})).?;
    for (strings, result) |row, actual| try std.testing.expectEqualDeep(try text_program.evaluate(arena.allocator(), row, &.{}, .{}), actual);
    var expression: std.ArrayList(u8) = .empty;
    defer expression.deinit(a);
    try expression.appendSlice(a, "n");
    for (0..40) |_| try expression.appendSlice(a, " + 1");
    var compiled = try @import("compiler.zig").compileScalar(a, expression.items, .{});
    defer compiled.deinit();
    var program = try scalar.bind(a, compiled.expression, &.{.{ .name = "n", .type = .integer }}, &.{}, .{});
    defer program.deinit();
    const rows: [1024][]const Datum = @splat(&.{Datum.json(.{ .integer = 2 })});
    var budget: @import("memory_budget.zig") = .{ .backing = a, .limit = 256 * 1024 };
    const values = (try evaluate(budget.allocator(), &program, &rows, &.{})).?;
    defer budget.allocator().free(values);
    for (values) |value| try std.testing.expectEqual(@as(i64, 42), value.value.integer);
    try std.testing.expect(budget.peak <= 256 * 1024);
    try std.testing.expectEqual(values.len * @sizeOf(Datum), budget.live);
}

test "SQL direct column kernels preserve physical selection and SQL nulls" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const types = @import("../storage/rowsource/types.zig");
    const refs: [6]types.RowRef = @splat(.{ .relational_key = "r" });
    const numbers = [_]i64{ 4, 9007199254740993, -3, 7, 0, 11 };
    const nulls = [_]u8{ 0, 0, 0, 1, 0, 0 };
    const columns = [_]types.ColumnVector{.{ .name = "n", .values = .{ .i64 = &numbers }, .nulls = .{ .bytes = &nulls } }};
    const page = @import("catalog.zig").ColumnPage{ .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &refs, .columns = &columns }, .selection = &.{ 5, 3, 1, 0, 1 } };
    for ([_][]const u8{ "n * 2", "n > 9007199254740992", "n + 0.5::double precision", "n IS NOT DISTINCT FROM NULL" }) |sql| {
        var compiled = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer compiled.deinit();
        const bound_columns = [_]scalar.Column{.{ .name = "n", .type = .integer }};
        var program = try scalar.bind(a, compiled.expression, &bound_columns, &.{}, .{});
        defer program.deinit();
        const result = (try evaluateColumns(arena.allocator(), &program, page, &bound_columns, &.{})).?;
        for (page.selection, result) |index, actual| {
            const input = if (nulls[index] != 0) Datum{} else Datum.json(.{ .integer = numbers[index] });
            try std.testing.expectEqualDeep(try program.evaluate(arena.allocator(), &.{input}, &.{}, .{}), actual);
        }
    }
}

test "SQL NUMERIC integer decimal expressions retain exact scalar fallback instead of float kernels" {
    const a = std.testing.allocator;
    var compiled = try @import("compiler.zig").compileScalar(a, "n + 0.5", .{});
    defer compiled.deinit();
    var program = try scalar.bind(a, compiled.expression, &.{.{ .name = "n", .type = .integer }}, &.{}, .{});
    defer program.deinit();
    try std.testing.expectEqual(@import("array_value.zig").ElementType.numeric, program.output_type.element_type.?);
    const input: Datum = Datum.json(.{ .integer = 9007199254740993 });
    const cells = [_][]const Datum{ &.{input}, &.{input}, &.{input}, &.{input} };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    try std.testing.expect((try evaluate(arena.allocator(), &program, &cells, &.{})) == null);
    const result = try program.evaluate(arena.allocator(), &.{input}, &.{}, .{});
    var context: @import("numeric_value.zig").Context = .{ .alloc = arena.allocator() };
    try std.testing.expectEqualStrings("9007199254740993.5", try @import("numeric_value.zig").format(&context, result.numeric.?.*));
}

test "SQL shared scheduled column kernels match scalar values over permuted nullable pages" {
    const a = std.testing.allocator;
    var compiled = try @import("compiler.zig").compileScalar(a, "(n + 2) * 3", .{});
    defer compiled.deinit();
    const definitions = [_]scalar.Column{.{ .name = "n", .type = .integer }};
    var program = try scalar.bind(a, compiled.expression, &definitions, &.{}, .{});
    defer program.deinit();
    const count = 2048;
    const values = try a.alloc(i64, count);
    defer a.free(values);
    const nulls = try a.alloc(u8, count);
    defer a.free(nulls);
    const selection = try a.alloc(usize, count);
    defer a.free(selection);
    for (values, nulls, selection, 0..) |*value, *is_null, *index, i| {
        value.* = @as(i64, @intCast(i)) - 1024;
        is_null.* = @intFromBool(i % 17 == 0);
        index.* = count - i - 1;
    }
    const page: @import("catalog.zig").ColumnPage = .{ .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &.{}, .columns = &.{.{ .name = "n", .nulls = .{ .bytes = nulls }, .values = .{ .i64 = values } }} }, .selection = selection };
    // ColumnBatch.rowCount is defined by row_refs; identities are not decoded.
    var complete = page;
    const refs = try a.alloc(@import("../storage/rowsource/types.zig").RowRef, count);
    defer a.free(refs);
    complete.batch.row_refs = refs;
    const scheduled = (try evaluateColumnsScheduled(a, &program, complete, &definitions, &.{}, std.testing.io)).?;
    defer a.free(scheduled);
    const fused = try evaluateColumnsManyScheduled(a, &.{ &program, &program }, complete, &definitions, &.{}, std.testing.io);
    defer {
        for (fused) |result| if (result) |vector| a.free(vector);
        a.free(fused);
    }
    for (fused) |result| try std.testing.expectEqualDeep(scheduled, result.?);
    for (scheduled, selection) |actual, physical| {
        const source: Datum = if (nulls[physical] != 0) .{} else Datum.json(.{ .integer = values[physical] });
        const expected = try program.evaluate(a, &.{source}, &.{}, .{});
        try std.testing.expectEqualDeep(expected, actual);
    }
}

test "SQL dictionary kernels normalize selected values once and preserve missing nulls" {
    const a = std.testing.allocator;
    var compiled = try @import("compiler.zig").compileScalar(a, "n + 1", .{});
    defer compiled.deinit();
    const definitions = [_]scalar.Column{.{ .name = "n", .type = .integer }};
    var program = try scalar.bind(a, compiled.expression, &definitions, &.{}, .{});
    defer program.deinit();
    const page: @import("catalog.zig").ColumnPage = .{ .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &.{ .{ .relational_key = "a" }, .{ .relational_key = "b" }, .{ .relational_key = "c" } }, .columns = &.{.{ .name = "n", .nulls = .{ .bytes = &.{ 0, 0, 1 } }, .values = .{ .dictionary_bytes = .{ .values = &.{ "41", "invalid-unselected" }, .indices = &.{ 0, 0, 0 } } } }} }, .selection = &.{ 2, 1, 0 } };
    const values = (try evaluateColumns(a, &program, page, &definitions, &.{})).?;
    defer a.free(values);
    try std.testing.expect(values[0].sql_null);
    try std.testing.expectEqual(@as(i64, 42), values[1].value.integer);
    try std.testing.expectEqual(@as(i64, 42), values[2].value.integer);
}

test "SQL adaptive scheduling keeps small lanes inline and fused columns lazy" {
    try std.testing.expectEqual(@as(usize, 1), scheduledLanes(1024, 128));
    try std.testing.expectEqual(@as(usize, 1), scheduledLanes(4096, 1));
    try std.testing.expectEqual(@as(usize, 2), scheduledLanes(2048, 5));
    try std.testing.expectEqual(@as(usize, 4), scheduledLanes(4096, 5));
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const definitions = [_]scalar.Column{.{ .name = "n", .type = .integer }};
    var first = try @import("compiler.zig").compileScalar(a, "n + 1", .{});
    defer first.deinit();
    var second = try @import("compiler.zig").compileScalar(a, "CASE WHEN false THEN n / 0 ELSE 7 END", .{});
    defer second.deinit();
    var p1 = try scalar.bind(a, first.expression, &definitions, &.{}, .{});
    defer p1.deinit();
    var p2 = try scalar.bind(a, second.expression, &definitions, &.{}, .{});
    defer p2.deinit();
    const page: @import("catalog.zig").ColumnPage = .{ .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &.{ .{ .relational_key = "a" }, .{ .relational_key = "b" } }, .columns = &.{.{ .name = "n", .values = .{ .i64 = &.{ 2, 5 } } }} }, .selection = &.{ 1, 0 } };
    const outputs = try evaluateColumnsMany(a, &.{ &p1, &p2 }, page, &definitions, &.{});
    try std.testing.expectEqual(@as(i64, 6), outputs[0].?[0].value.integer);
    try std.testing.expectEqual(@as(i64, 3), outputs[0].?[1].value.integer);
    try std.testing.expect(outputs[1] == null);
}

test "SQL typed DAGs load shared columns once and reuse common subexpressions" {
    const a = std.testing.allocator;
    const definitions = [_]scalar.Column{.{ .name = "n", .type = .integer }};
    var c1 = try @import("compiler.zig").compileScalar(a, "(n + 1) * 3", .{});
    defer c1.deinit();
    var c2 = try @import("compiler.zig").compileScalar(a, "(n + 1) * 5", .{});
    defer c2.deinit();
    var p1 = try scalar.bind(a, c1.expression, &definitions, &.{}, .{});
    defer p1.deinit();
    var p2 = try scalar.bind(a, c2.expression, &definitions, &.{}, .{});
    defer p2.deinit();
    const Counting = struct {
        count: usize,
        calls: *usize,
        pub fn fillColumn(self: @This(), _: std.mem.Allocator, ordinal: u32, values: []Datum) !void {
            try std.testing.expectEqual(@as(u32, 0), ordinal);
            self.calls.* += 1;
            for (values, 0..) |*value, row| value.* = if (row % 17 == 0) .{} else Datum.json(.{ .integer = @intCast(row) });
        }
    };
    var calls: usize = 0;
    const results = (try @import("typed_kernel.zig").evaluate(a, &.{ &p1, &p2, &p1 }, Counting{ .count = 2048, .calls = &calls }, &.{})).?;
    defer {
        for (results) |vector| a.free(vector);
        a.free(results);
    }
    try std.testing.expectEqual(@as(usize, 1), calls);
    try std.testing.expectEqualDeep(results[0], results[2]);
    for (results[0], results[1], 0..) |left, right, row| {
        if (row % 17 == 0) {
            try std.testing.expect(left.sql_null and right.sql_null);
        } else {
            try std.testing.expectEqual(@as(i64, @intCast((row + 1) * 3)), left.value.integer);
            try std.testing.expectEqual(@as(i64, @intCast((row + 1) * 5)), right.value.integer);
        }
    }
}

test "SQL native typed columns preserve finite-number coercion and selected nulls" {
    const a = std.testing.allocator;
    var compiled = try @import("compiler.zig").compileScalar(a, "n", .{});
    defer compiled.deinit();
    const definitions = [_]scalar.Column{.{ .name = "n", .type = .number }};
    var program = try scalar.bind(a, compiled.expression, &definitions, &.{}, .{});
    defer program.deinit();
    const types = @import("../storage/rowsource/types.zig");
    const refs: [4]types.RowRef = @splat(.{ .relational_key = "r" });
    const values = [_]f64{ std.math.nan(f64), std.math.inf(f64), -std.math.inf(f64), 4.0 };
    const page: @import("catalog.zig").ColumnPage = .{ .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &refs, .columns = &.{.{ .name = "n", .values = .{ .f64 = &values }, .nulls = .{ .bytes = &.{ 1, 0, 0, 0 } } }} }, .selection = &.{ 0, 3 } };
    const output = (try evaluateColumns(a, &program, page, &definitions, &.{})).?;
    defer a.free(output);
    try std.testing.expect(output[0].sql_null);
    try std.testing.expectEqual(@as(f64, 4.0), output[1].value.float);
    for ([_]usize{ 0, 1, 2 }) |selected| {
        var invalid = page;
        invalid.selection = &.{selected};
        const columns = [_]types.ColumnVector{.{ .name = "n", .values = .{ .f64 = &values } }};
        invalid.batch.columns = &columns;
        try std.testing.expectError(error.SqlTypeMismatch, evaluateColumns(a, &program, invalid, &definitions, &.{}));
    }
}

test "SQL vector root text validation matches scalar and ignores discarded intermediates" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const wide = try alloc.alloc(u8, (scalar.EvalLimits{}).output_bytes + 1);
    @memset(wide, 'x');
    for ([_][]const u8{ "s", "s = s" }) |sql| {
        var compiled = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer compiled.deinit();
        var program = try scalar.bind(a, compiled.expression, &.{.{ .name = "s", .type = .string }}, &.{}, .{});
        defer program.deinit();
        for ([_][]const u8{ "\xff", wide }) |text| {
            const rows = [_][]const Datum{&.{Datum.json(.{ .string = text })}};
            if (std.mem.eql(u8, sql, "s")) {
                const err = if (text.len == 1) error.SqlTypeMismatch else error.SqlProgramLimitExceeded;
                try std.testing.expectError(err, program.evaluate(alloc, rows[0], &.{}, .{}));
                try std.testing.expectError(err, evaluate(alloc, &program, &rows, &.{}));
                const types = @import("../storage/rowsource/types.zig");
                const page: @import("catalog.zig").ColumnPage = .{ .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &.{.{ .relational_key = "r" }}, .columns = &.{types.ColumnVector{ .name = "s", .values = .{ .dictionary_bytes = .{ .values = &.{text}, .indices = &.{0} } } }} }, .selection = &.{0} };
                try std.testing.expectError(err, evaluateColumns(alloc, &program, page, &.{.{ .name = "s", .type = .string }}, &.{}));
            } else {
                try std.testing.expect((try program.evaluate(alloc, rows[0], &.{}, .{})).value.bool);
                try std.testing.expect((try evaluate(alloc, &program, &rows, &.{})).?[0].value.bool);
            }
        }
    }
}

test "SQL numeric dictionary kernels preserve exact values nulls and selection order" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const definitions = [_]scalar.Column{ .{ .name = "n", .type = .integer }, .{ .name = "f", .type = .number } };
    const page: @import("catalog.zig").ColumnPage = .{ .batch = .{
        .snapshot = .{ .table_id = "t", .snapshot_id = "s" },
        .row_refs = &.{ .{ .relational_key = "a" }, .{ .relational_key = "b" }, .{ .relational_key = "c" }, .{ .relational_key = "d" } },
        .columns = &.{
            .{ .name = "n", .values = .{ .dictionary_i64 = .{ .values = &.{ 9007199254740993, -7 }, .indices = &.{ 0, 1, 99, 0 } } }, .nulls = .{ .bytes = &.{ 0, 0, 1, 0 } } },
            .{ .name = "f", .values = .{ .dictionary_f64 = .{ .values = &.{ -0.0, 2.5 }, .indices = &.{ 1, 0, 99, 1 } } }, .nulls = .{ .bytes = &.{ 0, 0, 1, 0 } } },
        },
    }, .selection = &.{ 3, 2, 0, 1 } };
    try page.batch.validate();
    for ([_][]const u8{ "n + 1", "n > 9007199254740992", "n IS NULL", "f * 2.0", "f IS NULL" }) |sql| {
        var compiled = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer compiled.deinit();
        var program = try scalar.bind(a, compiled.expression, &definitions, &.{}, .{});
        defer program.deinit();
        const actual = (try evaluateColumns(arena.allocator(), &program, page, &definitions, &.{})).?;
        for (0..page.selection.len) |row| {
            var cells: [2]Datum = undefined;
            for (definitions, &cells) |definition, *value| {
                const cell = try page.cell(arena.allocator(), row, definition.name);
                value.* = .{ .value = cell.value, .sql_null = cell.sql_null };
            }
            try std.testing.expectEqualDeep(try program.evaluate(arena.allocator(), &cells, &.{}, .{}), actual[row]);
        }
    }
}

fn dictionaryExpressionScenario(a: std.mem.Allocator) !void {
    const definitions = [_]scalar.Column{.{ .name = "n", .type = .integer }};
    const refs: [12]@import("../storage/rowsource/types.zig").RowRef = @splat(.{ .relational_key = "row" });
    const page: @import("catalog.zig").ColumnPage = .{ .batch = .{
        .snapshot = .{ .table_id = "t", .snapshot_id = "s" },
        .row_refs = &refs,
        .columns = &.{.{ .name = "n", .values = .{ .dictionary_i64 = .{ .values = &.{ 3, 4, 0 }, .indices = &.{ 0, 1, 99, 0, 1, 99, 0, 1, 99, 0, 1, 99 } } }, .nulls = .{ .bytes = &.{ 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1 } } }},
    }, .selection = &.{ 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 } };
    var compiled = try @import("compiler.zig").compileScalar(a, "12 / n", .{});
    defer compiled.deinit();
    var program = try scalar.bind(a, compiled.expression, &definitions, &.{}, .{});
    defer program.deinit();
    const outputs = try evaluateColumnsEncodedMany(a, &.{&program}, page, &definitions, &.{}, null);
    defer freeEncodedMany(a, outputs);
    const batch = outputs[0].?;
    try std.testing.expect(batch == .dictionary);
    // Never evaluate the unused zero dictionary entry. NULL is evaluated once.
    try std.testing.expectEqual(@as(usize, 3), batch.dictionary.values.len);
    for (0..12) |row| {
        const input = try page.cell(a, row, "n");
        const expected = try program.evaluate(a, &.{.{ .value = input.value, .sql_null = input.sql_null }}, &.{}, .{});
        try std.testing.expectEqualDeep(expected, try batch.cell(a, row, 0));
    }
}

test "SQL dictionary expressions retain unique results and unwind every failure" {
    try dictionaryExpressionScenario(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, dictionaryExpressionScenario, .{});
}

test "SQL retained numeric dictionaries reuse expression results after relational ownership" {
    const a = std.testing.allocator;
    var store = @import("typed_store.zig").Store.init(a);
    defer store.deinit();
    for (0..1024) |index| _ = try store.append(&.{if (index % 3 == 0) Datum{} else Datum.json(.{ .integer = @intCast(index % 3) })});
    const retained: @import("execution_batch.zig").Batch = .{ .retained = .{ .store = &store, .begin = 17, .count = 12 } };
    const definitions = [_]scalar.Column{.{ .name = "n", .type = .integer }};
    const page: @import("catalog.zig").ColumnPage = .{ .native = .{ .values = &retained, .names = &.{"n"} }, .selection = &.{ 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 } };
    var compiled = try @import("compiler.zig").compileScalar(a, "12 / n", .{});
    defer compiled.deinit();
    var program = try scalar.bind(a, compiled.expression, &definitions, &.{}, .{});
    defer program.deinit();
    const outputs = try evaluateColumnsEncodedMany(a, &.{&program}, page, &definitions, &.{}, null);
    defer freeEncodedMany(a, outputs);
    const batch = outputs[0].?;
    try std.testing.expect(batch == .dictionary);
    try std.testing.expectEqual(@as(usize, 3), batch.dictionary.values.len);
    for (0..12) |row| {
        const input = try page.cell(a, row, "n");
        const expected = try program.evaluate(a, &.{.{ .value = input.value, .sql_null = input.sql_null }}, &.{}, .{});
        try std.testing.expectEqualDeep(expected, try batch.cell(a, row, 0));
    }
}

fn fusedDictionaryScenario(a: std.mem.Allocator) !void {
    const columns = [_]scalar.Column{.{ .name = "n", .type = .integer }};
    const page: @import("catalog.zig").ColumnPage = .{ .batch = .{
        .snapshot = .{ .table_id = "t", .snapshot_id = "s" },
        .row_refs = &.{ .{ .relational_key = "a" }, .{ .relational_key = "b" }, .{ .relational_key = "c" }, .{ .relational_key = "d" }, .{ .relational_key = "e" }, .{ .relational_key = "f" } },
        .columns = &.{.{ .name = "n", .values = .{ .dictionary_i64 = .{ .values = &.{ 3, 4, 0 }, .indices = &.{ 0, 1, 0, 1, 0, 1 } } } }},
    }, .selection = &.{ 5, 4, 3, 2, 1, 0 } };
    var first = try @import("compiler.zig").compileScalar(a, "12 / n + 1", .{});
    defer first.deinit();
    var second = try @import("compiler.zig").compileScalar(a, "12 / n * 2", .{});
    defer second.deinit();
    var left = try scalar.bind(a, first.expression, &columns, &.{}, .{});
    defer left.deinit();
    var right = try scalar.bind(a, second.expression, &columns, &.{}, .{});
    defer right.deinit();
    const programs = [_]*const scalar.Program{ &left, &right };
    const outputs = try evaluateColumnsEncodedMany(a, &programs, page, &columns, &.{}, null);
    defer freeEncodedMany(a, outputs);
    for (outputs, programs) |optional, program| {
        const batch = optional.?;
        try std.testing.expectEqual(@as(usize, 2), batch.dictionary.values.len);
        for (0..page.selection.len) |row| {
            const source = try page.cell(a, row, "n");
            const expected = try program.evaluate(a, &.{.{ .value = source.value, .sql_null = source.sql_null }}, &.{}, .{});
            try std.testing.expectEqualDeep(expected, try batch.cell(a, row, 0));
        }
    }
}
test "SQL fused dictionary projections share referenced inputs and release all allocation failures" {
    try fusedDictionaryScenario(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, fusedDictionaryScenario, .{});
}

fn dictionaryTupleScenario(a: std.mem.Allocator) !void {
    const definitions = [_]scalar.Column{ .{ .name = "n", .type = .integer }, .{ .name = "m", .type = .integer } };
    const refs: [12]@import("../storage/rowsource/types.zig").RowRef = @splat(.{ .relational_key = "row" });
    const page: @import("catalog.zig").ColumnPage = .{ .batch = .{
        .snapshot = .{ .table_id = "t", .snapshot_id = "s" },
        .row_refs = &refs,
        .columns = &.{
            .{ .name = "n", .values = .{ .dictionary_i64 = .{ .values = &.{ 3, 4, 0 }, .indices = &.{ 0, 1, 99, 0, 1, 99, 0, 1, 99, 0, 1, 99 } } }, .nulls = .{ .bytes = &.{ 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1 } } },
            .{ .name = "m", .values = .{ .dictionary_i64 = .{ .values = &.{ 1, 2, 0 }, .indices = &.{ 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0 } } } },
        },
    }, .selection = &.{ 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 } };
    var first = try @import("compiler.zig").compileScalar(a, "12 / (n + m)", .{});
    defer first.deinit();
    var second = try @import("compiler.zig").compileScalar(a, "12 / (n + m) + 1", .{});
    defer second.deinit();
    var p = try scalar.bind(a, first.expression, &definitions, &.{}, .{});
    defer p.deinit();
    var q = try scalar.bind(a, second.expression, &definitions, &.{}, .{});
    defer q.deinit();
    const outputs = try evaluateColumnsEncodedMany(a, &.{ &p, &q }, page, &definitions, &.{}, null);
    defer freeEncodedMany(a, outputs);
    for (outputs, [_]*const scalar.Program{ &p, &q }) |optional, program| {
        const encoded = optional.?;
        try std.testing.expect(encoded == .dictionary);
        try std.testing.expectEqual(@as(usize, 3), encoded.dictionary.values.len);
        for (0..page.selection.len) |row| {
            var cells: [2]Datum = undefined;
            for (definitions, &cells) |definition, *value| {
                const cell = try page.cell(a, row, definition.name);
                value.* = .{ .value = cell.value, .sql_null = cell.sql_null };
            }
            try std.testing.expectEqualDeep(try program.evaluate(a, &cells, &.{}, .{}), try encoded.cell(a, row, 0));
        }
    }
}
test "SQL multi-input dictionary kernels share tuples and preserve NULL and selected errors" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, dictionaryTupleScenario, .{});
}

test "external lake NUMERIC column evaluation preserves physical and native exact operands" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const columns = [_]scalar.Column{.{ .name = "n", .type = .number, .element_type = .numeric }};
    const physicals = [_]@import("../storage/rowsource/types.zig").ColumnValues{
        .{ .i64 = &.{ 9007199254740993, 9007199254740992 } },
        .{ .bytes = &.{ "9007199254740993.125", "9007199254740993.124" } },
        .{ .dictionary_bytes = .{ .values = &.{ "9007199254740993.125", "9007199254740993.124" }, .indices = &.{ 0, 1 } } },
    };
    for (physicals) |physical| {
        const page: @import("catalog.zig").ColumnPage = .{ .batch = .{ .snapshot = .{ .table_id = "numeric", .snapshot_id = "one" }, .row_refs = &.{ .{ .relational_key = "one" }, .{ .relational_key = "two" } }, .columns = &.{.{ .name = "n", .values = physical }} }, .selection = &.{ 1, 0 } };
        const input: ColumnInput = .{ .page = page, .columns = &columns, .count = 2 };
        var values: [2]Datum = undefined;
        try input.fillColumn(ca, 0, &values);
        for (values, 0..) |value, i| {
            try std.testing.expect(value.numeric != null);
            try std.testing.expectEqual(std.math.Order.eq, try scalar.compareDatums(value, try input.cell(ca, i, 0)));
        }
        try std.testing.expectEqual(std.math.Order.lt, try scalar.compareDatums(values[0], values[1]));
    }
    var work: @import("array_value.zig").Budget = .{};
    const number = try scalar.numericTextLeaky(ca, "9007199254740993.125", &work);
    const batch: @import("execution_batch.zig").Batch = .{ .rows = &.{&.{number}} };
    const native: ColumnInput = .{ .page = .{ .native = .{ .values = &batch, .names = &.{"n"} }, .selection = &.{0} }, .columns = &columns, .count = 1 };
    try std.testing.expect((try native.cell(ca, 0, 0)).numeric == number.numeric);
}

fn physicalExpressionScenario(a: std.mem.Allocator) !void {
    const definitions = [_]scalar.Column{.{ .name = "n", .type = .integer }};
    const refs: [4]@import("../storage/rowsource/types.zig").RowRef = @splat(.{ .relational_key = "r" });
    const page: @import("catalog.zig").ColumnPage = .{ .batch = .{
        .snapshot = .{ .table_id = "t", .snapshot_id = "s" },
        .row_refs = &refs,
        .columns = &.{.{ .name = "n", .values = .{ .dictionary_i64 = .{ .values = &.{ 7, -3 }, .indices = &.{ 0, 1, 99, 0 } } }, .nulls = .{ .bytes = &.{ 0, 0, 1, 0 } } }},
    }, .selection = &.{ 3, 2, 1, 0 } };
    var compiled = try @import("compiler.zig").compileScalar(a, "n", .{});
    defer compiled.deinit();
    var program = try scalar.bind(a, compiled.expression, &definitions, &.{}, .{});
    defer program.deinit();
    const outputs = try evaluateColumnsEncodedMany(a, &.{&program}, page, &definitions, &.{}, null);
    defer freeEncodedMany(a, outputs);
    try std.testing.expect(outputs[0].? == .columns);
    const physical = (try outputs[0].?.physicalColumn(a, 0)).?;
    try std.testing.expect(physical.vector.values.dictionary_i64.indices.ptr == page.batch.columns[0].values.dictionary_i64.indices.ptr);
    var state = @import("aggregate_state.zig").Column.init(.{ .kind = .sum, .input_type = .integer });
    defer state.clear(a);
    try state.append(a);
    try state.updateEncoded(a, &.{ 0, 0, 0, 0 }, outputs[0].?);
    try std.testing.expectEqual(@as(i64, 11), (try state.finish(0)).value.integer);
}
test "SQL physical identity expressions reach aggregate kernels without vector expansion" {
    try physicalExpressionScenario(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, physicalExpressionScenario, .{});
}

fn computedPhysicalScenario(a: std.mem.Allocator) !void {
    const definitions = [_]scalar.Column{.{ .name = "n", .type = .integer, .element_type = .int32 }};
    const refs: [3]@import("../storage/rowsource/types.zig").RowRef = @splat(.{ .relational_key = "r" });
    const page: @import("catalog.zig").ColumnPage = .{ .batch = .{
        .snapshot = .{ .table_id = "t", .snapshot_id = "s" },
        .row_refs = &refs,
        .columns = &.{.{ .name = "n", .values = .{ .i64 = &.{ 2, 3, std.math.maxInt(i64) } }, .nulls = .{ .bytes = &.{ 0, 0, 1 } } }},
    }, .selection = &.{ 0, 2, 1 } };
    var compiled = try @import("compiler.zig").compileScalar(a, "n + 1", .{});
    defer compiled.deinit();
    var program = try scalar.bind(a, compiled.expression, &definitions, &.{}, .{});
    defer program.deinit();
    const outputs = try evaluateColumnsEncodedMany(a, &.{&program}, page, &definitions, &.{}, null);
    defer freeEncodedMany(a, outputs);
    try std.testing.expect(outputs[0].? == .reader);
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const physical = (try outputs[0].?.physicalColumn(scratch.allocator(), 0)).?;
    try physical.validate();
    try std.testing.expectEqual(@as(i64, 3), physical.vector.values.i64[0]);
    try std.testing.expect(physical.vector.nulls.isNull(1));
    const masked = try outputs[0].?.mask(scratch.allocator(), &.{ 1, 3, 4 }, 6);
    try std.testing.expect(try masked.physicalColumn(scratch.allocator(), 0) == null);
    const cohort = masked.reader.cohort.?;
    const native = (try cohort.source.physicalColumn(scratch.allocator(), 0)).?;
    try native.validate();
    try std.testing.expectEqualSlices(usize, &.{ 1, 3, 4 }, cohort.positions);
    for (0..6) |index| try std.testing.expectEqual(index != 1 and index != 4, (try masked.cell(a, index, 0)).sql_null);
    var state = @import("aggregate_state.zig").Column.init(.{ .kind = .sum, .input_type = .integer });
    defer state.clear(a);
    try state.append(a);
    try state.updateEncoded(a, &.{ 0, 0, 0, 0, 0, 0 }, masked);
    try std.testing.expectEqual(@as(i64, 7), (try state.finish(0)).value.integer);
}
test "SQL physical computed outputs and FILTER masks retain native lanes under allocation faults" {
    try computedPhysicalScenario(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, computedPhysicalScenario, .{});
}

test "SQL physical computed vectors share parallel scheduling and typed delivery" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const definitions = [_]scalar.Column{.{ .name = "n", .type = .integer }};
    const refs = try ca.alloc(@import("../storage/rowsource/types.zig").RowRef, 4096);
    @memset(refs, .{ .relational_key = "r" });
    const integers = try ca.alloc(i64, refs.len);
    const selection = try ca.alloc(usize, refs.len);
    for (integers, selection, 0..) |*value, *index, i| {
        value.* = @intCast(i);
        index.* = i;
    }
    const page: @import("catalog.zig").ColumnPage = .{ .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = refs, .columns = &.{.{ .name = "n", .values = .{ .i64 = integers } }} }, .selection = selection };
    var compiled = try @import("compiler.zig").compileScalar(a, "n + n + n", .{});
    defer compiled.deinit();
    var program = try scalar.bind(a, compiled.expression, &definitions, &.{}, .{});
    defer program.deinit();
    const outputs = try evaluateColumnsEncodedMany(a, &.{&program}, page, &definitions, &.{}, std.testing.io);
    defer freeEncodedMany(a, outputs);
    const physical = (try outputs[0].?.physicalColumn(ca, 0)).?;
    try physical.validate();
    try std.testing.expectEqual(refs.len, physical.len());
    for (integers, physical.vector.values.i64) |input, output| try std.testing.expectEqual(input * 3, output);
}

fn readerGatherScenario(a: std.mem.Allocator) !void {
    const Batch = @import("execution_batch.zig").Batch;
    const Physical = @import("execution_batch.zig").PhysicalColumn;
    const Reader = struct {
        fn cell(_: *anyopaque, _: std.mem.Allocator, _: usize, _: usize) !Datum {
            return error.InvalidSqlBackendResponse;
        }
        fn physical(_: *anyopaque, alloc: std.mem.Allocator, _: usize, selection: ?[]const usize) !?Physical {
            const source: Physical = .{ .vector = .{ .name = "n", .values = .{ .dictionary_i64 = .{ .values = &.{ 2, 7, std.math.maxInt(i64) }, .indices = &.{ 0, 2, 1 } } }, .nulls = .{ .bytes = &.{ 0, 0, 1 } } }, .selection = &.{ 0, 1, 2 }, .kind = .integer };
            return if (selection) |rows| try source.select(alloc, rows) else source;
        }
    };
    var raw: u8 = 0;
    const source: Batch = .{ .reader = .{ .ptr = &raw, .read = Reader.cell, .read_physical_selected = Reader.physical, .count = 3, .width = 1 } };
    const definitions = [_]scalar.Column{.{ .name = "n", .type = .integer, .element_type = .int32 }};
    const page: @import("catalog.zig").ColumnPage = .{ .native = .{ .values = &source, .names = &.{"n"} }, .selection = &.{ 2, 0 } };
    var compiled = try @import("compiler.zig").compileScalar(a, "n+1", .{});
    defer compiled.deinit();
    var program = try scalar.bind(a, compiled.expression, &definitions, &.{}, .{});
    defer program.deinit();
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const alloc = scratch.allocator();
    const outputs = try evaluateColumnsEncodedMany(alloc, &.{&program}, page, &definitions, &.{}, null);
    try std.testing.expect((try outputs[0].?.cell(alloc, 0, 0)).sql_null);
    try std.testing.expectEqual(@as(i64, 3), (try outputs[0].?.cell(alloc, 1, 0)).value.integer);
    var bad = page;
    bad.selection = &.{1};
    try std.testing.expectError(error.SqlNumericOutOfRange, evaluateColumnsEncodedMany(alloc, &.{&program}, bad, &definitions, &.{}, null));
}
test "SQL physical readers gather selected dictionary lanes without scalar reads" {
    try readerGatherScenario(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, readerGatherScenario, .{});
}

test "SQL physical string comparisons retain temporary payloads through numeric gathers" {
    const a = std.testing.allocator;
    const Physical = @import("execution_batch.zig").PhysicalColumn;
    const Reader = struct {
        fn cell(_: *anyopaque, _: std.mem.Allocator, _: usize, _: usize) !Datum {
            return error.InvalidSqlBackendResponse;
        }
        fn physical(_: *anyopaque, alloc: std.mem.Allocator, column: usize, selection: ?[]const usize) !?Physical {
            var view: Physical = if (column == 0) .{ .vector = .{ .name = "n", .values = .{ .i64 = &.{ 1, 0 } } }, .selection = &.{ 0, 1 }, .kind = .integer } else blk: {
                const strings = try alloc.alloc([]const u8, 2);
                strings[0] = try alloc.dupe(u8, "alpha");
                strings[1] = try alloc.dupe(u8, "omega");
                break :blk .{ .vector = .{ .name = "txt", .values = .{ .bytes = strings } }, .selection = &.{ 0, 1 }, .kind = .string };
            };
            if (selection) |rows| view = try view.select(alloc, rows);
            return view;
        }
    };
    var raw: u8 = 0;
    const source: @import("execution_batch.zig").Batch = .{ .reader = .{ .ptr = &raw, .read = Reader.cell, .read_physical_selected = Reader.physical, .width = 2, .count = 2 } };
    const definitions = [_]scalar.Column{ .{ .name = "n", .type = .integer }, .{ .name = "txt", .type = .string } };
    const page: @import("catalog.zig").ColumnPage = .{ .native = .{ .values = &source, .names = &.{ "n", "txt" } }, .selection = &.{ 0, 1 } };
    var compiled = try @import("compiler.zig").compileScalar(a, "(txt='alpha') = (n>0)", .{});
    defer compiled.deinit();
    var program = try scalar.bind(a, compiled.expression, &definitions, &.{}, .{});
    defer program.deinit();
    const outputs = try evaluateColumnsEncodedMany(a, &.{&program}, page, &definitions, &.{}, null);
    defer freeEncodedMany(a, outputs);
    for (0..2) |row| try std.testing.expect((try outputs[0].?.cell(a, row, 0)).value.bool);
}

test "lake SQL fixed decimal string columns fill comparison kernels with null and duplicate selections" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var compiled = try @import("compiler.zig").compileScalar(a, "n = '1.2300'", .{});
    defer compiled.deinit();
    const columns = [_]scalar.Column{.{ .name = "n", .type = .string }};
    var program = try scalar.bind(a, compiled.expression, &columns, &.{}, .{});
    defer program.deinit();
    const page: @import("catalog.zig").ColumnPage = .{ .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &.{ .{ .relational_key = "a" }, .{ .relational_key = "b" } }, .columns = &.{.{ .name = "n", .values = .{ .decimal128 = .{ .values = &.{ 12300, -100 }, .precision = 5, .scale = 4 } }, .nulls = .{ .bytes = &.{ 0, 1 } } }} }, .selection = &.{ 0, 1, 0 } };
    const result = (try evaluateColumns(ca, &program, page, &columns, &.{})).?;
    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expect(result[0].value.bool);
    try std.testing.expect(result[1].sql_null);
    try std.testing.expect(result[2].value.bool);
}

fn decimalComparisonScenario(a: std.mem.Allocator) !void {
    const definitions = [_]scalar.Column{.{ .name = "n", .type = .number, .element_type = .numeric }};
    const page: @import("catalog.zig").ColumnPage = .{ .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &.{ .{ .relational_key = "a" }, .{ .relational_key = "b" }, .{ .relational_key = "c" } }, .columns = &.{.{ .name = "n", .values = .{ .decimal128 = .{ .values = &.{ 12300, 12301, std.math.minInt(i128) }, .precision = 38, .scale = 4 } }, .nulls = .{ .bytes = &.{ 0, 0, 1 } } }} }, .selection = &.{ 1, 0, 2, 1, 0 } };
    for ([_][]const u8{ "n=1.2300", "n<1.23005", "1.2300<n", "n<>CAST('NaN' AS NUMERIC)", "n<10000000000000000000000000000000000000000000000" }) |text| {
        var compiled = try @import("compiler.zig").compileScalar(a, text, .{});
        defer compiled.deinit();
        var program = try scalar.bind(a, compiled.expression, &definitions, &.{}, .{});
        defer program.deinit();
        const outputs = try evaluateColumnsEncodedMany(a, &.{&program}, page, &definitions, &.{}, null);
        defer freeEncodedMany(a, outputs);
        const output = outputs[0] orelse return error.TestExpectedNativeDecimalComparison;
        try std.testing.expect(output == .reader);
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        for (0..page.selection.len) |row| {
            const cell = try page.typedCell(scratch.allocator(), row, "n", true);
            const expected = try program.evaluate(scratch.allocator(), &.{cell}, &.{}, .{});
            try std.testing.expectEqualDeep(expected, try output.cell(scratch.allocator(), row, 0));
        }
    }
}
test "lake SQL coefficient comparison vectors preserve selected NULL lanes exact fractions and specials with allocation safety" {
    try decimalComparisonScenario(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, decimalComparisonScenario, .{});
}

fn decimalArithmeticScenario(a: std.mem.Allocator) !void {
    const definitions = [_]scalar.Column{ .{ .name = "n", .type = .number, .element_type = .numeric }, .{ .name = "m", .type = .number, .element_type = .numeric }, .{ .name = "i", .type = .integer } };
    const page: @import("catalog.zig").ColumnPage = .{ .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &.{ .{ .relational_key = "a" }, .{ .relational_key = "b" }, .{ .relational_key = "c" } }, .columns = &.{
        .{ .name = "n", .values = .{ .decimal128 = .{ .values = &.{ 12300, -12301, std.math.minInt(i128) }, .precision = 38, .scale = 4 } }, .nulls = .{ .bytes = &.{ 0, 0, 1 } } },
        .{ .name = "m", .values = .{ .decimal128 = .{ .values = &.{ -20, 31, 55 }, .precision = 4, .scale = 2 } } },
        .{ .name = "i", .values = .{ .i64 = &.{ 3, -2, 1 } } },
    } }, .selection = &.{ 1, 0, 2, 1 } };
    for ([_][]const u8{ "n+1.25", "n-m", "-(n*m)+n", "n*i", "n % m", "n / 3.00", "(n+1.25)*(m-2.00)" }) |text| {
        var compiled = try @import("compiler.zig").compileScalar(a, text, .{});
        defer compiled.deinit();
        var program = try scalar.bind(a, compiled.expression, &definitions, &.{}, .{});
        defer program.deinit();
        const outputs = try evaluateColumnsEncodedMany(a, &.{&program}, page, &definitions, &.{}, null);
        defer freeEncodedMany(a, outputs);
        const output = outputs[0] orelse return error.TestUnexpectedResult;
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const sa = scratch.allocator();
        try std.testing.expect(try output.decimalColumnSelected(sa, 0, null) != null);
        for (0..page.selection.len) |row| {
            const inputs = [_]Datum{ try page.typedCell(sa, row, "n", true), try page.typedCell(sa, row, "m", true), try page.typedCell(sa, row, "i", false) };
            const expected = try program.evaluate(sa, &inputs, &.{}, .{});
            const actual = try output.cell(sa, row, 0);
            try std.testing.expectEqual(expected.sql_null, actual.sql_null);
            if (expected.sql_null) continue;
            var context: @import("numeric_value.zig").Context = .{ .alloc = sa };
            try std.testing.expectEqualStrings(try @import("numeric_value.zig").format(&context, expected.numeric.?.*), try @import("numeric_value.zig").format(&context, actual.numeric.?.*));
        }
    }
    for ([_][]const u8{ "n*99999999999999999999999999999999999999", "n+CAST('NaN' AS NUMERIC)", "n/0.00000000000000000000000000000000000001" }) |text| {
        var compiled = try @import("compiler.zig").compileScalar(a, text, .{});
        defer compiled.deinit();
        var program = try scalar.bind(a, compiled.expression, &definitions, &.{}, .{});
        defer program.deinit();
        try std.testing.expect(try decimalArithmetic(a, &program, page, &definitions, &.{}) == null);
    }
}
test "lake SQL coefficient arithmetic retains exact scales nulls nested expressions and overflow fallback" {
    try decimalArithmeticScenario(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, decimalArithmeticScenario, .{});
}

fn mixedDecimalScenario(a: std.mem.Allocator) !void {
    const definitions = [_]scalar.Column{ .{ .name = "n", .type = .number, .element_type = .numeric }, .{ .name = "m", .type = .number, .element_type = .numeric } };
    const page: @import("catalog.zig").ColumnPage = .{ .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &.{ .{ .relational_key = "a" }, .{ .relational_key = "b" }, .{ .relational_key = "c" } }, .columns = &.{ .{ .name = "n", .values = .{ .decimal128 = .{ .values = &.{ 12300, 1230000000, 0 }, .precision = 38, .scale = 4 } }, .nulls = .{ .bytes = &.{ 0, 0, 1 } } }, .{ .name = "m", .values = .{ .decimal128 = .{ .values = &.{ 300, 300, 300 }, .precision = 38, .scale = 2 } } } } }, .selection = &.{ 0, 1, 2, 0 } };
    for ([_][]const u8{ "n/m", "(n/m)+m", "-(n/m)*2.00" }) |text| {
        var compiled = try @import("compiler.zig").compileScalar(a, text, .{});
        defer compiled.deinit();
        var program = try scalar.bind(a, compiled.expression, &definitions, &.{}, .{});
        defer program.deinit();
        const output = (try decimalArithmetic(a, &program, page, &definitions, &.{})) orelse return error.TestUnexpectedResult;
        defer output.reader.free_owned.?(output.reader.ptr, a);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const sa = arena.allocator();
        for (0..page.selection.len) |row| {
            const inputs = [_]Datum{ try page.typedCell(sa, row, "n", true), try page.typedCell(sa, row, "m", true) };
            const expected = try program.evaluate(sa, &inputs, &.{}, .{});
            const actual = try output.cell(sa, row, 0);
            try std.testing.expectEqual(expected.sql_null, actual.sql_null);
            if (expected.sql_null) continue;
            var context: @import("numeric_value.zig").Context = .{ .alloc = sa };
            try std.testing.expectEqualStrings(try @import("numeric_value.zig").format(&context, expected.numeric.?.*), try @import("numeric_value.zig").format(&context, actual.numeric.?.*));
        }
    }
    var c1 = try @import("compiler.zig").compileScalar(a, "n+m", .{});
    defer c1.deinit();
    var c2 = try @import("compiler.zig").compileScalar(a, "(n+m)*(n+m)", .{});
    defer c2.deinit();
    var p1 = try scalar.bind(a, c1.expression, &definitions, &.{}, .{});
    defer p1.deinit();
    var p2 = try scalar.bind(a, c2.expression, &definitions, &.{}, .{});
    defer p2.deinit();
    var workspace: DecimalWorkspace = undefined;
    workspace.init(a);
    defer workspace.arena.deinit();
    const first = (try decimalArithmeticWith(&workspace, a, &p1, page, &definitions, &.{})).?;
    defer first.reader.free_owned.?(first.reader.ptr, a);
    const count = workspace.cache.items.len;
    const second = (try decimalArithmeticWith(&workspace, a, &p2, page, &definitions, &.{})).?;
    defer second.reader.free_owned.?(second.reader.ptr, a);
    try std.testing.expect(workspace.cache.items.len <= count + 2);
    var denied: DecimalWorkspace = undefined;
    denied.init(a);
    defer denied.arena.deinit();
    denied.budget.limit = 1;
    try std.testing.expect(try decimalArithmeticWith(&denied, a, &p1, page, &definitions, &.{}) == null);
    try std.testing.expect(denied.disabled);
}
test "lake SQL fused decimal forest preserves mixed division display scales and declines optional workspace limits" {
    try mixedDecimalScenario(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, mixedDecimalScenario, .{});
}

test "lake SQL fused decimal loops fit final vector admission without intermediate vectors" {
    try fusedDecimalScenario(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, fusedDecimalScenario, .{});
}
fn fusedDecimalScenario(a: std.mem.Allocator) !void {
    var compiled = try @import("compiler.zig").compileScalar(a, "((n+m)*(n-m)+(n+m)*(n-m))/m", .{});
    defer compiled.deinit();
    const columns = [_]scalar.Column{ .{ .name = "n", .type = .number, .element_type = .numeric }, .{ .name = "m", .type = .number, .element_type = .numeric } };
    var program = try scalar.bind(a, compiled.expression, &columns, &.{}, .{});
    defer program.deinit();
    const n = try a.alloc(i128, 1024);
    defer a.free(n);
    const m = try a.alloc(i128, 1024);
    defer a.free(m);
    const selected = try a.alloc(usize, 1024);
    defer a.free(selected);
    for (n, m, selected, 0..) |*left, *right, *row, i| {
        left.* = @as(i128, @intCast(i % 101)) - 50;
        right.* = 3;
        row.* = i;
    }
    const page: @import("catalog.zig").ColumnPage = .{ .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .external_span = .{ .first = .{ .source_id = "t", .snapshot_id = "s", .file_id = "f", .row_group_ordinal = 0, .row_ordinal = 0 }, .count = 1024 }, .columns = &.{ .{ .name = "n", .values = .{ .decimal128 = .{ .values = n, .precision = 38, .scale = 0 } } }, .{ .name = "m", .values = .{ .decimal128 = .{ .values = m, .precision = 38, .scale = 0 } } } } }, .selection = selected };
    var budget: @import("memory_budget.zig") = .{ .backing = a, .limit = 64 * 1024 };
    var outputs = [_]?@import("execution_batch.zig").Batch{null};
    var visited = [_]bool{false};
    try fusedDecimals(budget.allocator(), &.{&program}, page, &columns, &.{}, &outputs, &visited);
    try std.testing.expect(visited[0]);
    defer outputs[0].?.reader.free_owned.?(outputs[0].?.reader.ptr, budget.allocator());
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    for (0..1024) |row| {
        _ = scratch.reset(.retain_capacity);
        const sa = scratch.allocator();
        const expected = try program.evaluate(sa, &.{ try page.typedCell(sa, row, "n", true), try page.typedCell(sa, row, "m", true) }, &.{}, .{});
        const actual = try outputs[0].?.cell(sa, row, 0);
        var context: @import("numeric_value.zig").Context = .{ .alloc = sa };
        try std.testing.expectEqualStrings(try @import("numeric_value.zig").format(&context, expected.numeric.?.*), try @import("numeric_value.zig").format(&context, actual.numeric.?.*));
    }
}
