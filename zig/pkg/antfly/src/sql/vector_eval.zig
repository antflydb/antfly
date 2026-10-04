// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Bounded instruction-major expression kernels. Lazy or unsupported programs
//! fall back intact to scalar evaluation; unreachable errors stay unreachable.
const std = @import("std");
const scalar = @import("scalar.zig");
const Datum = scalar.Datum;
const Binary = @import("ast.zig").Scalar.Binary;

pub fn evaluate(a: std.mem.Allocator, program: *const scalar.Program, rows: []const []const Datum, parameters: []const std.json.Value) !?[]const Datum {
    return evaluateInput(a, program, RowInput{ .rows = rows, .count = rows.len }, parameters);
}
pub fn evaluateColumns(a: std.mem.Allocator, program: *const scalar.Program, page: @import("catalog.zig").ColumnPage, columns: []const scalar.Column, parameters: []const std.json.Value) !?[]const Datum {
    return evaluateInput(a, program, ColumnInput{ .page = page, .columns = columns, .count = page.selection.len }, parameters);
}
const RowInput = struct {
    rows: []const []const Datum,
    count: usize,
    fn cell(self: RowInput, _: std.mem.Allocator, index: usize, ordinal: u32) !Datum {
        if (ordinal >= self.rows[index].len) return error.InvalidSqlBackendResponse;
        return self.rows[index][ordinal];
    }
};
const ColumnInput = struct {
    page: @import("catalog.zig").ColumnPage,
    columns: []const scalar.Column,
    count: usize,
    fn cell(self: ColumnInput, a: std.mem.Allocator, index: usize, ordinal: u32) !Datum {
        if (ordinal >= self.columns.len) return error.InvalidSqlBackendResponse;
        const value = try self.page.cell(a, index, self.columns[ordinal].name);
        return .{ .value = value.value, .sql_null = value.sql_null };
    }
};
fn evaluateInput(a: std.mem.Allocator, program: *const scalar.Program, inputs: anytype, parameters: []const std.json.Value) !?[]const Datum {
    // Kernel workspace is page-local and capped independently of SQL's budget.
    if (program.instructions.len > 256 or inputs.count > 4096 or program.instructions.len == 0) return null;
    if (program.root >= program.instructions.len) return error.InvalidSqlBackendResponse;
    for (program.instructions) |instruction| switch (instruction.operation) {
        .literal => |value| switch (value) {
            .null, .bool, .integer, .float, .string => {},
            else => return null,
        },
        .column => |ordinal| {
            if (instruction.type.kind != .integer and instruction.type.kind != .number and instruction.type.kind != .boolean and instruction.type.kind != .string) return null;
            for (0..inputs.count) |index| {
                switch ((try inputs.cell(a, index, ordinal)).value) {
                    .null, .bool, .integer, .float, .string => {},
                    else => return null,
                }
            }
        },
        .parameter => if (instruction.type.kind != .integer and instruction.type.kind != .number and instruction.type.kind != .boolean and instruction.type.kind != .string) return null,
        .unary => {},
        .binary => |b| switch (b.op) {
            .add, .subtract, .multiply, .divide, .modulo, .eq, .neq, .lt, .lte, .gt, .gte, .is_distinct, .is_not_distinct => {},
            else => return null,
        },
        else => return null,
    };
    var uses: [256]usize = @splat(0);
    for (program.instructions, 0..) |instruction, index| {
        var children: [2]u32 = undefined;
        for (operands(instruction, &children)) |child| {
            if (child >= index) return error.InvalidSqlBackendResponse;
            uses[child] += 1;
        }
    }
    uses[program.root] += 1;
    var pending = uses;
    var live: usize = 0;
    var peak: usize = 0;
    for (program.instructions, 0..) |instruction, index| {
        live += 1;
        peak = @max(peak, live);
        var children: [2]u32 = undefined;
        for (operands(instruction, &children)) |child| {
            pending[child] -= 1;
            if (pending[child] == 0) live -= 1;
        }
        if (pending[index] == 0) live -= 1;
    }
    if (peak *| inputs.count > 32768) return null;
    const output = try a.alloc(Datum, inputs.count);
    errdefer a.free(output);
    if (inputs.count == 0) return output;
    const scratch = try a.alloc(Datum, peak * inputs.count);
    var slots: [256]usize = undefined;
    var available: [256]usize = undefined;
    for (available[0..peak], 0..) |*slot, index| slot.* = index;
    var available_len = peak;
    defer a.free(scratch);
    for (program.instructions, 0..) |instruction, index| {
        available_len -= 1;
        slots[index] = available[available_len];
        const target = scratch[slots[index] * inputs.count ..][0..inputs.count];
        switch (instruction.operation) {
            .literal => |v| @memset(target, Datum.fromJson(v)),
            .parameter => @memset(target, try program.evaluateInstruction(a, @intCast(index), parameters)),
            .column => |ordinal| for (target, 0..) |*out, row_index| {
                out.* = try inputs.cell(a, row_index, ordinal);
            },
            .unary => |u| {
                if (u.operand >= index) return error.InvalidSqlBackendResponse;
                const input = scratch[slots[u.operand] * inputs.count ..][0..inputs.count];
                for (target, input) |*out, value| out.* = try unary(u.op, value);
            },
            .binary => |b| {
                if (b.left >= index or b.right >= index) return error.InvalidSqlBackendResponse;
                const left = scratch[slots[b.left] * inputs.count ..][0..inputs.count];
                const right = scratch[slots[b.right] * inputs.count ..][0..inputs.count];
                var begin: usize = 0;
                while (begin < inputs.count) {
                    // Exact signed integer arithmetic/comparisons use four SIMD lanes.
                    // Mixed numeric kinds use scalar.compare without float casts.
                    if (begin + 4 <= inputs.count and (comparison(b.op) or b.op == .add or b.op == .subtract or b.op == .multiply)) {
                        var lhs_values: [4]i64 = @splat(0);
                        var rhs_values: [4]i64 = @splat(0);
                        var integers = true;
                        for (0..4) |lane| {
                            const l = left[begin + lane];
                            const r = right[begin + lane];
                            if (l.value != .integer or r.value != .integer or l.sql_null or r.sql_null) {
                                integers = false;
                                break;
                            }
                            lhs_values[lane] = l.value.integer;
                            rhs_values[lane] = r.value.integer;
                        }
                        if (integers) {
                            const lhs: @Vector(4, i64) = lhs_values;
                            const rhs: @Vector(4, i64) = rhs_values;
                            if (!comparison(b.op)) {
                                const result = switch (b.op) {
                                    .add => @addWithOverflow(lhs, rhs),
                                    .subtract => @subWithOverflow(lhs, rhs),
                                    .multiply => @mulWithOverflow(lhs, rhs),
                                    else => unreachable,
                                };
                                if (@reduce(.Or, result[1] != @as(@Vector(4, u1), @splat(0)))) return error.SqlNumericOutOfRange;
                                const values: [4]i64 = result[0];
                                for (0..4) |lane| target[begin + lane] = Datum.json(.{ .integer = values[lane] });
                                begin += 4;
                                continue;
                            }
                            const mask: [4]bool = switch (b.op) {
                                .eq => lhs == rhs,
                                .neq => lhs != rhs,
                                .lt => lhs < rhs,
                                .lte => lhs <= rhs,
                                .gt => lhs > rhs,
                                .gte => lhs >= rhs,
                                else => unreachable,
                            };
                            for (0..4) |lane| target[begin + lane] = Datum.json(.{ .bool = mask[lane] });
                            begin += 4;
                            continue;
                        }
                    }
                    if (begin + 4 <= inputs.count and (comparison(b.op) or b.op == .add or b.op == .subtract or b.op == .multiply or b.op == .divide)) {
                        var lhs_values: [4]f64 = undefined;
                        var rhs_values: [4]f64 = undefined;
                        var numbers = true;
                        for (0..4) |lane| {
                            const l = left[begin + lane];
                            const r = right[begin + lane];
                            if (l.sql_null or r.sql_null or l.value != .float or r.value != .float or !std.math.isFinite(l.value.float) or !std.math.isFinite(r.value.float)) {
                                numbers = false;
                                break;
                            }
                            lhs_values[lane] = l.value.float;
                            rhs_values[lane] = r.value.float;
                        }
                        if (numbers) {
                            const lhs: @Vector(4, f64) = lhs_values;
                            const rhs: @Vector(4, f64) = rhs_values;
                            if (comparison(b.op)) {
                                const mask: [4]bool = switch (b.op) {
                                    .eq => lhs == rhs,
                                    .neq => lhs != rhs,
                                    .lt => lhs < rhs,
                                    .lte => lhs <= rhs,
                                    .gt => lhs > rhs,
                                    .gte => lhs >= rhs,
                                    else => unreachable,
                                };
                                for (0..4) |lane| target[begin + lane] = Datum.json(.{ .bool = mask[lane] });
                            } else {
                                // Keep lane order for errors, including an overflow
                                // before a later zero divisor. No reassociation/FMA.
                                if (b.op == .divide and @reduce(.Or, rhs == @as(@Vector(4, f64), @splat(0)))) {
                                    for (0..4) |lane| target[begin + lane] = try binary(b.op, left[begin + lane], right[begin + lane]);
                                } else {
                                    const result: [4]f64 = switch (b.op) {
                                        .add => lhs + rhs,
                                        .subtract => lhs - rhs,
                                        .multiply => lhs * rhs,
                                        .divide => lhs / rhs,
                                        else => unreachable,
                                    };
                                    for (result, 0..) |value, lane| {
                                        if (!std.math.isFinite(value)) return error.SqlNumericOutOfRange;
                                        target[begin + lane] = Datum.json(.{ .float = value });
                                    }
                                }
                            }
                            begin += 4;
                            continue;
                        }
                    }
                    target[begin] = try binary(b.op, left[begin], right[begin]);
                    begin += 1;
                }
            },
            else => unreachable,
        }
        // Binding may widen an integer expression to NUMBER. Apply the same
        // per-instruction conversion as the scalar evaluator, before consumers.
        if (instruction.type.kind == .number) for (target) |*value| {
            if (!value.sql_null and value.value == .integer) {
                const converted: f64 = @floatFromInt(value.value.integer);
                value.value = .{ .float = converted };
            }
        };
        var children: [2]u32 = undefined;
        for (operands(instruction, &children)) |child| {
            uses[child] -= 1;
            if (uses[child] == 0) {
                available[available_len] = slots[child];
                available_len += 1;
            }
        }
        if (uses[index] == 0) {
            available[available_len] = slots[index];
            available_len += 1;
        }
    }
    @memcpy(output, scratch[slots[program.root] * inputs.count ..][0..inputs.count]);
    return output;
}
fn operands(instruction: scalar.Instruction, storage: *[2]u32) []const u32 {
    return switch (instruction.operation) {
        .unary => |u| blk: {
            storage[0] = u.operand;
            break :blk storage[0..1];
        },
        .binary => |b| blk: {
            storage.* = .{ b.left, b.right };
            break :blk storage;
        },
        else => &.{},
    };
}
fn unary(op: @import("ast.zig").Scalar.Unary, value: Datum) !Datum {
    if (op == .is_null or op == .is_not_null) return Datum.json(.{ .bool = value.sql_null == (op == .is_null) });
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
            .float => |v| if (std.math.isFinite(v)) Datum.json(.{ .float = -v }) else error.SqlNumericOutOfRange,
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
fn binary(op: Binary, left: Datum, right: Datum) !Datum {
    if (op == .is_distinct or op == .is_not_distinct) {
        const equal = if (left.sql_null or right.sql_null) left.sql_null and right.sql_null else (try scalar.compare(left.value, right.value)) == .eq;
        return Datum.json(.{ .bool = equal == (op == .is_not_distinct) });
    }
    if (left.sql_null or right.sql_null) return .{};
    if (comparison(op)) return Datum.json(scalar.comparison(op, try scalar.compare(left.value, right.value)));
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
    const rows = [_][]const Datum{&.{Datum.json(.{ .integer = 2 })}} ** 1024;
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
    const refs = [_]types.RowRef{.{ .relational_key = "r" }} ** 6;
    const numbers = [_]i64{ 4, 9007199254740993, -3, 7, 0, 11 };
    const nulls = [_]u8{ 0, 0, 0, 1, 0, 0 };
    const columns = [_]types.ColumnVector{.{ .name = "n", .values = .{ .i64 = &numbers }, .nulls = .{ .bytes = &nulls } }};
    const page = @import("catalog.zig").ColumnPage{ .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &refs, .columns = &columns }, .selection = &.{ 5, 3, 1, 0, 1 } };
    for ([_][]const u8{ "n * 2", "n > 9007199254740992", "n + 0.5", "n IS NOT DISTINCT FROM NULL" }) |sql| {
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
