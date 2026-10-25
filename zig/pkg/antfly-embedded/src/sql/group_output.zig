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

//! Totality certificates for stopping an ordered, fully reduced group source.
//! Ordinary typed SQL evaluation remains authoritative for every returned row.
const std = @import("std");
const scalar = @import("scalar.zig");
const binding = @import("aggregate_binding.zig");
const Fact = struct {
    safe: bool = false,
    low: i128 = std.math.minInt(i64),
    high: i128 = std.math.maxInt(i64),
};
fn integerRange(t: scalar.Type) Fact {
    return switch (t.element_type orelse .int64) {
        .int16 => .{ .safe = true, .low = std.math.minInt(i16), .high = std.math.maxInt(i16) },
        .int32 => .{ .safe = true, .low = std.math.minInt(i32), .high = std.math.maxInt(i32) },
        else => .{ .safe = true },
    };
}
fn datum(value: scalar.Datum) Fact {
    if (value.sql_null) return .{ .safe = true };
    if (value.numeric != null or value.array != null or value.patterns != null) return .{};
    return switch (value.value) {
        .integer => |n| .{ .safe = true, .low = n, .high = n },
        .null, .string, .bool => .{ .safe = true },
        else => .{},
    };
}
fn simple(t: scalar.Type) bool {
    return t.kind == .integer or t.kind == .string or t.kind == .boolean;
}
fn fact(program: scalar.Program, index: u32, columns: []const Fact, parameters: []const Fact, remaining: *usize) Fact {
    if (remaining.* == 0 or index >= program.instructions.len) return .{};
    remaining.* -= 1;
    const instruction = program.instructions[index];
    if (instruction.input_function) return .{};
    return switch (instruction.operation) {
        .column => |slot| if (slot < columns.len) columns[slot] else .{},
        .literal => |value| datum(scalar.Datum.fromJson(value)),
        .parameter => if (index < parameters.len) parameters[index] else .{},
        .cast => |cast| blk: {
            const input = fact(program, cast.operand, columns, parameters, remaining);
            if (!input.safe) break :blk .{};
            const prior = program.instructions[cast.operand].type;
            if (prior.kind != instruction.type.kind or !simple(prior)) break :blk .{};
            if (prior.kind == .integer) {
                const range = integerRange(instruction.type);
                if (input.low < range.low or input.high > range.high) break :blk .{};
            }
            break :blk input;
        },
        .unary => |unary| blk: {
            var input = fact(program, unary.operand, columns, parameters, remaining);
            if (!input.safe) break :blk .{};
            switch (unary.op) {
                .is_null, .is_not_null => break :blk .{ .safe = true },
                .not, .is_true, .is_not_true, .is_false, .is_not_false, .is_unknown, .is_not_unknown => break :blk .{ .safe = program.instructions[unary.operand].type.kind == .boolean },
                .positive => {},
                .negative => {
                    if (instruction.type.kind != .integer) break :blk .{};
                    input = .{ .safe = true, .low = -input.high, .high = -input.low };
                },
            }
            const range = integerRange(instruction.type);
            break :blk if (instruction.type.kind == .integer and input.low >= range.low and input.high <= range.high) input else .{};
        },
        .binary => |binary| blk: {
            const left = fact(program, binary.left, columns, parameters, remaining);
            const right = fact(program, binary.right, columns, parameters, remaining);
            if (!left.safe or !right.safe) break :blk .{};
            const lt = program.instructions[binary.left].type;
            const rt = program.instructions[binary.right].type;
            switch (binary.op) {
                .eq, .neq, .lt, .lte, .gt, .gte => break :blk .{ .safe = simple(lt) and lt.kind == rt.kind },
                .@"and", .@"or" => break :blk .{ .safe = lt.kind == .boolean and rt.kind == .boolean },
                .add, .subtract, .multiply => {},
                else => break :blk .{},
            }
            if (lt.kind != .integer or rt.kind != .integer or instruction.type.kind != .integer) break :blk .{};
            const result: Fact = switch (binary.op) {
                .add => .{ .safe = true, .low = left.low + right.low, .high = left.high + right.high },
                .subtract => .{ .safe = true, .low = left.low - right.high, .high = left.high - right.low },
                .multiply => multiplied: {
                    const products = [_]i128{ left.low * right.low, left.low * right.high, left.high * right.low, left.high * right.high };
                    break :multiplied .{ .safe = true, .low = std.mem.min(i128, &products), .high = std.mem.max(i128, &products) };
                },
                else => unreachable,
            };
            const range = integerRange(instruction.type);
            break :blk if (result.low >= range.low and result.high <= range.high) result else .{};
        },
        .call => |call| blk: {
            if (call.function != .coalesce and call.function != .greatest and call.function != .least and call.function != .nullif) break :blk .{};
            var result: ?Fact = null;
            for (call.args) |arg| {
                const input = fact(program, arg, columns, parameters, remaining);
                if (!input.safe or !simple(program.instructions[arg].type)) break :blk .{};
                if (result) |previous| result = .{ .safe = true, .low = @min(previous.low, input.low), .high = @max(previous.high, input.high) } else result = input;
            }
            break :blk result orelse .{};
        },
        else => .{},
    };
}
fn totalProgram(a: std.mem.Allocator, program: scalar.Program, columns: []const Fact, parameters: []const std.json.Value) !bool {
    // Parameter coercions are invocation-specific. Reuse typed evaluation, and
    // decline failed speculative conversions without demanding their errors.
    const facts = try a.alloc(Fact, program.instructions.len);
    @memset(facts, .{});
    for (program.instructions, 0..) |instruction, index| if (instruction.operation == .parameter) {
        const value = program.evaluateInstruction(a, @intCast(index), parameters) catch |err| {
            if (err == error.InvalidSqlParameters or err == error.UnsupportedSqlShape or err == error.SqlProgramLimitExceeded or std.mem.startsWith(u8, @import("errors.zig").describe(err).code, "22")) return false;
            return err;
        };
        facts[index] = datum(value);
    };
    var remaining: usize = 1024;
    return fact(program, program.root, columns, facts, &remaining).safe;
}
pub fn total(a: std.mem.Allocator, bound: *const binding.Bound, constants: []const scalar.Datum, parameters: []const std.json.Value) !bool {
    const columns = try a.alloc(Fact, bound.group_count + bound.specs.len + constants.len);
    for (bound.input.projections[0..bound.group_count], columns[0..bound.group_count]) |program, *out| out.* = if (program) |p| if (p.output_type.kind == .integer) integerRange(p.output_type) else .{ .safe = true } else .{};
    for (bound.specs, columns[bound.group_count..][0..bound.specs.len]) |spec, *out| out.* = if (spec.kind == .count) .{ .safe = true, .low = 0 } else .{ .safe = true };
    for (constants, columns[bound.group_count + bound.specs.len ..]) |value, *out| out.* = datum(value);
    for (bound.outputs) |program| {
        if (!try totalProgram(a, program, columns, parameters)) return false;
    }
    if (bound.having) |program| {
        if (!try totalProgram(a, program, columns, parameters)) return false;
    }
    return true;
}

/// An additional source restriction, never a replacement for SQL HAVING.
/// Only called after total output/reducer negotiation licenses skipped groups.
pub fn predicate(a: std.mem.Allocator, bound: *const binding.Bound, table: @import("catalog.zig").Table, parameters: []const std.json.Value, constants: []const scalar.Datum) !?@import("catalog.zig").Scan.GraphFilter {
    var program = bound.having orelse return null;
    const columns = try a.alloc(scalar.Column, bound.group_count);
    for (bound.input.projections[0..bound.group_count], columns) |optional, *column| {
        const key = optional orelse return null;
        const root = key.instructions[key.root];
        if (root.input_function or root.operation != .column) return null;
        column.* = bound.input.columns[root.operation.column];
        column.name = (try table.column(column.name)).path;
    }
    const instructions = try a.dupe(scalar.Instruction, program.instructions);
    program.instructions = instructions;
    for (instructions) |*instruction| if (instruction.operation == .column) {
        const slot = instruction.operation.column;
        const start = bound.group_count + bound.specs.len;
        if (slot < start) continue;
        if (slot - start >= constants.len) return null;
        const value = constants[slot - start];
        if (value.numeric != null or value.array != null or value.patterns != null) return null;
        instruction.operation = .{ .literal = if (value.sql_null) .null else value.value };
    };
    var remaining: usize = 1024;
    return conjuncts(a, program, columns, parameters, program.root, &remaining);
}
fn conjuncts(a: std.mem.Allocator, program: scalar.Program, columns: []const scalar.Column, parameters: []const std.json.Value, index: u32, remaining: *usize) anyerror!?@import("catalog.zig").Scan.GraphFilter {
    if (remaining.* == 0 or index >= program.instructions.len) return null;
    remaining.* -= 1;
    const operation = program.instructions[index].operation;
    if (operation == .binary and operation.binary.op == .@"and") {
        const left = try conjuncts(a, program, columns, parameters, operation.binary.left, remaining);
        const right = try conjuncts(a, program, columns, parameters, operation.binary.right, remaining);
        if (left == null) return right;
        if (right == null) return left;
        return .{ .all = try a.dupe(@import("catalog.zig").Scan.GraphFilter, &.{ left.?, right.? }) };
    }
    var fragment = program;
    fragment.root = index;
    return @import("observer_request.zig").fromProgram(a, fragment, columns, parameters);
}
