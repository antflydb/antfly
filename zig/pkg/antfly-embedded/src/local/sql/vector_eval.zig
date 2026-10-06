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
    pub fn fillTyped(self: MemoInput, ordinal: u32, target: anytype) !bool {
        return self.base.fillTyped(ordinal, target);
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
    pub fn fillTyped(self: ColumnInput, ordinal: u32, target: anytype) !bool {
        if (ordinal >= self.columns.len) return error.InvalidSqlBackendResponse;
        const definition = self.columns[ordinal];
        const column = self.page.batch.findColumn(definition.name) orelse return false;
        const exact = switch (column.values) {
            .i64 => definition.type == .integer or definition.type == .number,
            .f64 => definition.type == .number,
            .bool => definition.type == .boolean,
            .bytes, .dictionary_bytes => definition.type == .string,
            else => false,
        };
        if (!exact) return false;
        for (self.page.selection, 0..) |physical, row| {
            if (physical >= self.page.batch.rowCount()) return error.InvalidSqlBackendResponse;
            if (column.nulls.isNull(physical)) {
                try target.set(row, .{});
                continue;
            }
            const value: std.json.Value = switch (column.values) {
                .i64 => |v| .{ .integer = v[physical] },
                .f64 => |v| blk: {
                    if (!std.math.isFinite(v[physical])) return error.SqlTypeMismatch;
                    break :blk .{ .float = v[physical] };
                },
                .bool => |v| .{ .bool = v[physical] },
                .bytes => |v| .{ .string = v[physical] },
                .dictionary_bytes => |v| blk: {
                    if (v.indices[physical] >= v.values.len) return error.InvalidSqlBackendResponse;
                    break :blk .{ .string = v.values[v.indices[physical]] };
                },
                else => unreachable,
            };
            try target.set(row, Datum.json(value));
        }
        return true;
    }
    pub fn fillColumn(self: ColumnInput, a: std.mem.Allocator, ordinal: u32, output: []Datum) !void {
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
    pub fn cell(self: ColumnInput, a: std.mem.Allocator, index: usize, ordinal: u32) !Datum {
        if (ordinal >= self.columns.len) return error.InvalidSqlBackendResponse;
        const value = try self.page.cell(a, index, self.columns[ordinal].name);
        return .{ .value = try @import("describe.zig").coerceAlloc(a, value.value, self.columns[ordinal].type), .sql_null = value.sql_null };
    }
};
fn evaluateInput(a: std.mem.Allocator, program: *const scalar.Program, inputs: anytype, parameters: []const std.json.Value) !?[]const Datum {
    const vectors = (try @import("typed_kernel.zig").evaluate(a, &.{program}, inputs, parameters)) orelse return null;
    defer a.free(vectors);
    return vectors[0];
}
pub fn unary(op: @import("ast.zig").Scalar.Unary, value: Datum) !Datum {
    if (op == .is_null or op == .is_not_null) return Datum.json(.{ .bool = value.sql_null == (op == .is_null) });
    if (value.array != null) return error.SqlTypeMismatch;
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
pub fn binary(op: Binary, left: Datum, right: Datum) !Datum {
    if (op == .is_distinct or op == .is_not_distinct) {
        const equal = if (left.sql_null or right.sql_null) left.sql_null and right.sql_null else (try scalar.compareDatums(left, right)) == .eq;
        return Datum.json(.{ .bool = equal == (op == .is_not_distinct) });
    }
    if (left.sql_null or right.sql_null) return .{};
    if (comparison(op)) return Datum.json(scalar.comparison(op, try scalar.compareDatums(left, right)));
    if (left.array != null or right.array != null) return error.SqlTypeMismatch;
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
    const wide = try alloc.alloc(u8, 1024 * 1024 + 1);
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
