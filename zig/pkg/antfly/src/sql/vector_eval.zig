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
pub fn evaluateColumns(a: std.mem.Allocator, program: *const scalar.Program, page: @import("catalog.zig").ColumnPage, columns: []const scalar.Column, parameters: []const std.json.Value) anyerror!?[]const Datum {
    return evaluateInput(a, program, ColumnInput{ .page = page, .columns = columns, .count = page.selection.len }, parameters);
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
    for (programs, outputs) |program, *output| output.* = try evaluateInput(a, program, input, parameters);
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
    fn fillColumn(self: MemoInput, a: std.mem.Allocator, ordinal: u32, output: []Datum) !void {
        if (ordinal >= self.memo.len) return error.InvalidSqlBackendResponse;
        if (self.memo[ordinal] == null) {
            const values = try a.alloc(Datum, self.count);
            errdefer a.free(values);
            try self.base.fillColumn(a, ordinal, values);
            self.memo[ordinal] = values;
        }
        @memcpy(output, self.memo[ordinal].?);
    }
    fn cell(self: MemoInput, a: std.mem.Allocator, index: usize, ordinal: u32) !Datum {
        return self.base.cell(a, index, ordinal);
    }
};
pub fn evaluateBatch(a: std.mem.Allocator, program: *const scalar.Program, batch: @import("execution_batch.zig").Batch, parameters: []const std.json.Value) !?[]const Datum {
    return evaluateInput(a, program, BatchInput{ .batch = batch, .count = batch.len() }, parameters);
}
const BatchInput = struct {
    batch: @import("execution_batch.zig").Batch,
    count: usize,
    fn cell(self: BatchInput, a: std.mem.Allocator, index: usize, ordinal: u32) !Datum {
        return self.batch.cell(a, index, ordinal);
    }
    fn fillColumn(self: BatchInput, a: std.mem.Allocator, ordinal: u32, output: []Datum) !void {
        for (output, 0..) |*value, index| value.* = try self.cell(a, index, ordinal);
    }
};
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
    fn fillColumn(self: ColumnInput, a: std.mem.Allocator, ordinal: u32, output: []Datum) !void {
        if (ordinal >= self.columns.len) return error.InvalidSqlBackendResponse;
        const definition = self.columns[ordinal];
        const column = self.page.batch.findColumn(definition.name) orelse {
            for (output, 0..) |*value, index| value.* = try self.cell(a, index, ordinal);
            return;
        };
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
                if (normalized[id] == null) normalized[id] = Datum.json(try @import("describe.zig").coerceAlloc(a, .{ .string = dictionary.values[id] }, definition.type));
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
                .i64 => |values| .{ .integer = values[index] },
                .f64 => |values| .{ .float = values[index] },
                .bool => |values| .{ .bool = values[index] },
                .bytes => |values| .{ .string = values[index] },
                .json => |values| try std.json.parseFromSliceLeaky(std.json.Value, a, values[index], .{ .parse_numbers = false }),
                else => return error.UnsupportedSqlExecution,
            };
            value.* = Datum.json(try @import("describe.zig").coerceAlloc(a, raw, definition.type));
        }
    }
    fn cell(self: ColumnInput, a: std.mem.Allocator, index: usize, ordinal: u32) !Datum {
        if (ordinal >= self.columns.len) return error.InvalidSqlBackendResponse;
        const value = try self.page.cell(a, index, self.columns[ordinal].name);
        return .{ .value = try @import("describe.zig").coerceAlloc(a, value.value, self.columns[ordinal].type), .sql_null = value.sql_null };
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
            if (comptime !@hasDecl(@TypeOf(inputs), "fillColumn")) for (0..inputs.count) |index| {
                switch ((try inputs.cell(a, index, ordinal)).value) {
                    .null, .bool, .integer, .float, .string => {},
                    else => return null,
                }
            };
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
            .column => |ordinal| {
                if (comptime @hasDecl(@TypeOf(inputs), "fillColumn")) try inputs.fillColumn(a, ordinal, target) else {
                    for (target, 0..) |*out, row_index| {
                        out.* = try inputs.cell(a, row_index, ordinal);
                    }
                }
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
