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
    // Kernel workspace is page-local and capped independently of SQL's budget.
    if (program.instructions.len > 256 or rows.len > 4096 or program.instructions.len *| rows.len > 32768) return null;
    if (program.root >= program.instructions.len) return error.InvalidSqlBackendResponse;
    for (program.instructions) |instruction| switch (instruction.operation) {
        .literal => |value| switch (value) {
            .null, .bool, .integer, .float => {},
            else => return null,
        },
        .column => |ordinal| {
            if (instruction.type.kind != .integer and instruction.type.kind != .number and instruction.type.kind != .boolean) return null;
            for (rows) |row| {
                if (ordinal >= row.len) return error.InvalidSqlBackendResponse;
                switch (row[ordinal].value) {
                    .null, .bool, .integer, .float => {},
                    else => return null,
                }
            }
        },
        .parameter => if (instruction.type.kind != .integer and instruction.type.kind != .number and instruction.type.kind != .boolean) return null,
        .unary => |u| if (u.op != .is_null and u.op != .is_not_null and u.op != .positive) return null,
        .binary => |b| switch (b.op) {
            .add, .subtract, .multiply, .divide, .modulo, .eq, .neq, .lt, .lte, .gt, .gte, .is_distinct, .is_not_distinct => {},
            else => return null,
        },
        else => return null,
    };
    const output = try a.alloc(Datum, rows.len);
    if (rows.len == 0) return output;
    const scratch = try a.alloc(Datum, program.instructions.len * rows.len);
    defer a.free(scratch);
    for (program.instructions, 0..) |instruction, index| {
        const target = scratch[index * rows.len ..][0..rows.len];
        switch (instruction.operation) {
            .literal => |v| @memset(target, Datum.fromJson(v)),
            .parameter => @memset(target, try program.evaluateInstruction(a, @intCast(index), parameters)),
            .column => |ordinal| for (target, rows) |*out, row| {
                if (ordinal >= row.len) return error.InvalidSqlBackendResponse;
                out.* = row[ordinal];
            },
            .unary => |u| {
                if (u.operand >= index) return error.InvalidSqlBackendResponse;
                const input = scratch[u.operand * rows.len ..][0..rows.len];
                for (target, input) |*out, value| out.* = if (u.op == .positive) (if (value.value == .null) Datum{} else if (value.value == .integer or value.value == .float) value else return error.SqlTypeMismatch) else Datum.json(.{ .bool = value.sql_null == (u.op == .is_null) });
            },
            .binary => |b| {
                if (b.left >= index or b.right >= index) return error.InvalidSqlBackendResponse;
                const left = scratch[b.left * rows.len ..][0..rows.len];
                const right = scratch[b.right * rows.len ..][0..rows.len];
                var begin: usize = 0;
                while (begin < rows.len) {
                    // Exact signed integer arithmetic/comparisons use four SIMD lanes.
                    // Mixed numeric kinds use scalar.compare without float casts.
                    if (begin + 4 <= rows.len and (comparison(b.op) or b.op == .add or b.op == .subtract or b.op == .multiply)) {
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
    }
    @memcpy(output, scratch[program.root * rows.len ..][0..rows.len]);
    return output;
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
