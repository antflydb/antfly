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

//! Instruction DAGs over tag-free typed payloads and independent SQL/JSON null
//! states. Only result vectors cross back into Datum; lazy programs decline.
const std = @import("std");
const scalar = @import("scalar.zig");
const ast = @import("ast.zig");
const Datum = scalar.Datum;
const A = std.mem.Allocator;
const Kind = enum { none, integer, number, boolean, string };
const State = enum(u8) { value, sql_null, json_null };
const Vector = struct {
    kind: Kind,
    payload: []u64,
    states: []State,
    fn integers(self: Vector) []i64 {
        return @as([*]i64, @ptrCast(self.payload.ptr))[0..self.states.len];
    }
    fn numbers(self: Vector) []f64 {
        return @as([*]f64, @ptrCast(self.payload.ptr))[0..self.states.len];
    }
    fn booleans(self: Vector) []bool {
        return @as([*]bool, @ptrCast(self.payload.ptr))[0..self.states.len];
    }
    fn strings(self: Vector) [][]const u8 {
        return @as([*][]const u8, @ptrCast(self.payload.ptr))[0..self.states.len];
    }
    fn get(self: Vector, row: usize) Datum {
        return switch (self.states[row]) {
            .sql_null => .{},
            .json_null => Datum.json(.null),
            .value => Datum.json(switch (self.kind) {
                .integer => .{ .integer = self.integers()[row] },
                .number => .{ .float = self.numbers()[row] },
                .boolean => .{ .bool = self.booleans()[row] },
                .string => .{ .string = self.strings()[row] },
                .none => .null,
            }),
        };
    }
    pub fn set(self: Vector, row: usize, value: Datum) !void {
        if (value.patterns != null) return error.UnsupportedTypedKernel;
        self.states[row] = if (value.sql_null) .sql_null else if (value.value == .null) .json_null else .value;
        if (self.states[row] != .value) return;
        switch (self.kind) {
            .integer => if (value.value == .integer) {
                self.integers()[row] = value.value.integer;
            } else return error.UnsupportedTypedKernel,
            .number => switch (value.value) {
                .integer => |n| self.numbers()[row] = @floatFromInt(n),
                .float => |n| self.numbers()[row] = n,
                else => return error.UnsupportedTypedKernel,
            },
            .boolean => if (value.value == .bool) {
                self.booleans()[row] = value.value.bool;
            } else return error.UnsupportedTypedKernel,
            .string => if (value.value == .string) {
                self.strings()[row] = value.value.string;
            } else return error.UnsupportedTypedKernel,
            .none => return error.UnsupportedTypedKernel,
        }
    }
};
fn kind(type_: scalar.Type) ?Kind {
    return if (type_.kind) |k| switch (k) {
        .integer => .integer,
        .number => .number,
        .boolean => .boolean,
        .string => .string,
        else => null,
    } else .none;
}
pub fn supported(program: *const scalar.Program) bool {
    if (program.instructions.len == 0 or program.instructions.len > 256 or program.root >= program.instructions.len) return false;
    for (program.instructions) |instruction| {
        if (kind(instruction.type) == null) return false;
        switch (instruction.operation) {
            .literal => |v| switch (v) {
                .null, .integer, .float, .bool, .string => {},
                else => return false,
            },
            .column, .parameter, .unary => {},
            .binary => |b| switch (b.op) {
                .add, .subtract, .multiply, .divide, .modulo, .eq, .neq, .lt, .lte, .gt, .gte, .is_distinct, .is_not_distinct => {},
                else => return false,
            },
            else => return false,
        }
    }
    return true;
}
fn equal(a: scalar.Instruction, b: scalar.Instruction) bool {
    if (!std.meta.eql(a.type, b.type) or std.meta.activeTag(a.operation) != std.meta.activeTag(b.operation)) return false;
    return switch (a.operation) {
        .column => |v| v == b.operation.column,
        .parameter => |v| v == b.operation.parameter,
        .unary => |v| std.meta.eql(v, b.operation.unary),
        .binary => |v| std.meta.eql(v, b.operation.binary),
        .literal => |v| blk: {
            const other = b.operation.literal;
            if (std.meta.activeTag(v) != std.meta.activeTag(other)) break :blk false;
            break :blk switch (v) {
                .null => true,
                .integer => |n| n == other.integer,
                .float => |n| @as(u64, @bitCast(n)) == @as(u64, @bitCast(other.float)),
                .bool => |n| n == other.bool,
                .string => |n| std.mem.eql(u8, n, other.string),
                else => false,
            };
        },
        else => false,
    };
}
pub fn sameProgram(left: *const scalar.Program, right: *const scalar.Program) bool {
    if (!supported(left) or !supported(right) or left.root != right.root or left.instructions.len != right.instructions.len) return false;
    for (left.instructions, right.instructions) |l, r| if (!equal(l, r)) return false;
    return true;
}
fn children(instruction: scalar.Instruction, storage: *[2]u32) []const u32 {
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
pub fn evaluate(a: A, programs: []const *const scalar.Program, input: anytype, parameters: []const std.json.Value) !?[]const []const Datum {
    return evaluateImpl(a, programs, input, parameters) catch |err| switch (err) {
        error.UnsupportedTypedKernel => null,
        else => return err,
    };
}
fn evaluateImpl(a: A, programs: []const *const scalar.Program, input: anytype, parameters: []const std.json.Value) !?[]const []const Datum {
    if (input.count > 4096 or programs.len > 256) return null;
    var instructions: [512]scalar.Instruction = undefined;
    var len: usize = 0;
    var roots: [256]u32 = undefined;
    // Canonical operand indices make identical subexpressions reusable across
    // projections and aggregates, independently of program-local numbering.
    for (programs, 0..) |program, p| {
        if (!supported(program)) return null;
        var remap: [256]u32 = undefined;
        for (program.instructions, 0..) |original, i| {
            var instruction = original;
            switch (instruction.operation) {
                .unary => |*u| {
                    if (u.operand >= i) return error.InvalidSqlBackendResponse;
                    u.operand = remap[u.operand];
                },
                .binary => |*b| {
                    if (b.left >= i or b.right >= i) return error.InvalidSqlBackendResponse;
                    b.left = remap[b.left];
                    b.right = remap[b.right];
                },
                else => {},
            }
            remap[i] = for (instructions[0..len], 0..) |prior, index| {
                if (equal(prior, instruction)) break @as(u32, @intCast(index));
            } else blk: {
                if (len == instructions.len) return null;
                instructions[len] = instruction;
                len += 1;
                break :blk @intCast(len - 1);
            };
        }
        roots[p] = remap[program.root];
    }
    var uses: [512]usize = @splat(0);
    for (instructions[0..len]) |instruction| {
        var buf: [2]u32 = undefined;
        for (children(instruction, &buf)) |child| uses[child] += 1;
    }
    for (roots[0..programs.len]) |root| uses[root] += 1;
    var pending = uses;
    var live: usize = 0;
    var peak: usize = 0;
    for (instructions[0..len], 0..) |instruction, i| {
        live += 1;
        peak = @max(peak, live);
        var buf: [2]u32 = undefined;
        for (children(instruction, &buf)) |child| {
            pending[child] -= 1;
            if (pending[child] == 0) live -= 1;
        }
        for (roots[0..programs.len]) |root| if (root == i) {
            pending[i] -= 1;
        };
        if (pending[i] == 0) live -= 1;
    }
    if (peak *| input.count > 32768) return null;
    const outputs = try a.alloc([]const Datum, programs.len);
    @memset(outputs, &.{});
    errdefer {
        for (outputs) |values| a.free(values);
        a.free(outputs);
    }
    for (outputs) |*output| output.* = try a.alloc(Datum, input.count);
    const words: usize = for (instructions[0..len]) |instruction| {
        if (kind(instruction.type) == .string) break 2;
    } else 1;
    const payload = try a.alloc(u64, peak * input.count * words);
    defer a.free(payload);
    const states = try a.alloc(State, peak * input.count);
    defer a.free(states);
    var vectors: [512]Vector = undefined;
    var free: [512]usize = undefined;
    for (free[0..peak], 0..) |*slot, i| slot.* = i;
    var free_len = peak;
    var column_scratch: ?[]Datum = null;
    defer if (column_scratch) |values| a.free(values);
    var slots: [512]usize = undefined;
    for (instructions[0..len], 0..) |instruction, i| {
        free_len -= 1;
        const slot = free[free_len];
        slots[i] = slot;
        const target: Vector = .{ .kind = kind(instruction.type).?, .payload = payload[slot * input.count * words ..][0 .. input.count * words], .states = states[slot * input.count ..][0..input.count] };
        vectors[i] = target;
        switch (instruction.operation) {
            .literal => |v| for (0..input.count) |row| try target.set(row, Datum.fromJson(v)),
            .parameter => |ordinal| {
                // Parameter coercion uses its originating program's contract.
                const value = blk: {
                    for (programs) |program| for (program.instructions, 0..) |original, index| {
                        if (original.operation == .parameter and original.operation.parameter == ordinal and std.meta.eql(original.type, instruction.type)) break :blk try program.evaluateInstruction(a, @intCast(index), parameters);
                    };
                    return error.InvalidSqlBackendResponse;
                };
                for (0..input.count) |row| try target.set(row, value);
            },
            .column => |ordinal| {
                const filled = if (comptime @hasDecl(@TypeOf(input), "fillTyped")) try input.fillTyped(ordinal, target) else false;
                if (filled) {} else if (comptime @hasDecl(@TypeOf(input), "fillColumn")) {
                    if (column_scratch == null) column_scratch = try a.alloc(Datum, input.count);
                    try input.fillColumn(a, ordinal, column_scratch.?);
                    for (column_scratch.?, 0..) |value, row| try target.set(row, value);
                } else for (0..input.count) |row| try target.set(row, try input.cell(a, row, ordinal));
            },
            .unary => |u| for (0..input.count) |row| try target.set(row, try @import("vector_eval.zig").unary(u.op, vectors[u.operand].get(row))),
            .binary => |b| try binary(target, b.op, vectors[b.left], vectors[b.right]),
            else => unreachable,
        }
        // Materialize roots when ready, so delivery vectors do not pin live
        // kernel slots while later expressions reuse the shared DAG.
        for (roots[0..programs.len], outputs) |root, output| if (root == i) {
            for (@constCast(output), 0..) |*value, row| {
                value.* = target.get(row);
                try scalar.validateResult(value.*, .{});
            }
            uses[i] -= 1;
        };
        var buf: [2]u32 = undefined;
        for (children(instruction, &buf)) |child| {
            uses[child] -= 1;
            if (uses[child] == 0) {
                free[free_len] = slots[child];
                free_len += 1;
            }
        }
        if (uses[i] == 0) {
            free[free_len] = slot;
            free_len += 1;
        }
    }
    return outputs;
}
fn binary(out: Vector, op: ast.Scalar.Binary, left: Vector, right: Vector) !void {
    const compare = switch (op) {
        .eq, .neq, .lt, .lte, .gt, .gte => true,
        else => false,
    };
    var row: usize = 0;
    while (row < out.states.len) {
        const dense = row + 4 <= out.states.len and for (left.states[row..][0..4], right.states[row..][0..4]) |l, r| {
            if (l != .value or r != .value) break false;
        } else true;
        if (dense and left.kind == .integer and right.kind == .integer and (compare or op == .add or op == .subtract or op == .multiply)) {
            const l: @Vector(4, i64) = left.integers()[row..][0..4].*;
            const r: @Vector(4, i64) = right.integers()[row..][0..4].*;
            @memset(out.states[row..][0..4], .value);
            if (compare) {
                out.booleans()[row..][0..4].* = switch (op) {
                    .eq => l == r,
                    .neq => l != r,
                    .lt => l < r,
                    .lte => l <= r,
                    .gt => l > r,
                    .gte => l >= r,
                    else => unreachable,
                };
            } else {
                const result = switch (op) {
                    .add => @addWithOverflow(l, r),
                    .subtract => @subWithOverflow(l, r),
                    .multiply => @mulWithOverflow(l, r),
                    else => unreachable,
                };
                if (@reduce(.Or, result[1] != @as(@Vector(4, u1), @splat(0)))) return error.SqlNumericOutOfRange;
                if (out.kind == .number) {
                    const widened: @Vector(4, f64) = @floatFromInt(result[0]);
                    out.numbers()[row..][0..4].* = widened;
                } else out.integers()[row..][0..4].* = result[0];
            }
            row += 4;
            continue;
        }
        if (dense and left.kind == .number and right.kind == .number and (compare or op == .add or op == .subtract or op == .multiply or op == .divide)) {
            const lhs = left.numbers()[row..][0..4];
            const rhs = right.numbers()[row..][0..4];
            const finite = for (lhs, rhs) |l, r| {
                if (!std.math.isFinite(l) or !std.math.isFinite(r) or (op == .divide and r == 0)) break false;
            } else true;
            if (finite) {
                const l: @Vector(4, f64) = lhs.*;
                const r: @Vector(4, f64) = rhs.*;
                @memset(out.states[row..][0..4], .value);
                if (compare) out.booleans()[row..][0..4].* = switch (op) {
                    .eq => l == r,
                    .neq => l != r,
                    .lt => l < r,
                    .lte => l <= r,
                    .gt => l > r,
                    .gte => l >= r,
                    else => unreachable,
                } else {
                    const values: [4]f64 = switch (op) {
                        .add => l + r,
                        .subtract => l - r,
                        .multiply => l * r,
                        .divide => l / r,
                        else => unreachable,
                    };
                    for (values) |value| if (!std.math.isFinite(value)) return error.SqlNumericOutOfRange;
                    out.numbers()[row..][0..4].* = values;
                }
                row += 4;
                continue;
            }
        }
        // Nulls, mixed numeric kinds and exceptional lanes share scalar rules.
        try out.set(row, try @import("vector_eval.zig").binary(op, left.get(row), right.get(row)));
        row += 1;
    }
}
