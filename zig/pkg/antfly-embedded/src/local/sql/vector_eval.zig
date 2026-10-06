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
/// Evaluate only referenced dictionary entries. Programs with multiple inputs,
/// lazy evaluation or external effects keep the existing fused scalar/vector path.
/// Unreferenced entries must never raise errors or consume expression work.
pub fn evaluateDictionaryColumns(a: std.mem.Allocator, program: *const scalar.Program, page: @import("catalog.zig").ColumnPage, columns: []const scalar.Column, parameters: []const std.json.Value) !?@import("execution_batch.zig").Batch {
    if (!@import("typed_kernel.zig").supported(program) or program.required_columns.len != 1 or page.selection.len < 4) return null;
    var selection = (try dictionarySelection(a, program.required_columns[0], page, columns)) orelse return null;
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
fn dictionarySelection(a: std.mem.Allocator, ordinal: u32, page: @import("catalog.zig").ColumnPage, columns: []const scalar.Column) !?DictionarySelection {
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
    var group: std.ArrayList(*const scalar.Program) = .empty;
    defer group.deinit(a);
    var group_positions: std.ArrayList(usize) = .empty;
    defer group_positions.deinit(a);
    for (programs, 0..) |program, index| {
        if (visited[index]) continue;
        if (!@import("typed_kernel.zig").supported(program) or program.required_columns.len != 1) continue;
        const ordinal = program.required_columns[0];
        var selection = (try dictionarySelection(a, ordinal, page, columns)) orelse continue;
        defer selection.deinit(a);
        group.clearRetainingCapacity();
        group_positions.clearRetainingCapacity();
        for (programs[index..], index..) |candidate, position| {
            if (!visited[position] and @import("typed_kernel.zig").supported(candidate) and candidate.required_columns.len == 1 and candidate.required_columns[0] == ordinal) {
                try group.append(a, candidate);
                try group_positions.append(a, position);
                visited[position] = true;
            }
        }
        var compact = page;
        compact.selection = selection.representatives.items;
        // Gather the dictionary IDs once, then share typed expression nodes
        // across every projection over this input's referenced entries.
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
        if (self.page.native) |native| {
            const index = for (native.names, 0..) |name, i| {
                if (std.mem.eql(u8, name, definition.name)) break i;
            } else return false;
            // These batch representations read cells without allocating or
            // coercing payloads. Other readers use the ordinary allocator path.
            switch (native.values.*) {
                .retained, .rows, .vectors, .dictionary => {},
                else => return false,
            }
            for (self.page.selection, 0..) |physical, row| {
                const value = try native.values.cell(std.heap.page_allocator, physical, index);
                if (value.patterns != null) return false;
                if (!value.sql_null) {
                    const exact = switch (value.value) {
                        .integer => definition.type == .integer or definition.type == .number,
                        .float => definition.type == .number,
                        .bool => definition.type == .boolean,
                        .string => definition.type == .string,
                        else => false,
                    };
                    if (!exact) return false;
                    if (value.value == .float and !std.math.isFinite(value.value.float)) return error.SqlTypeMismatch;
                }
                try target.set(row, value);
            }
            return true;
        }
        const column = self.page.batch.findColumn(definition.name) orelse return false;
        const exact = switch (column.values) {
            .i64, .dictionary_i64 => definition.type == .integer or definition.type == .number,
            .f64, .dictionary_f64 => definition.type == .number,
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
                .dictionary_i64 => |v| .{ .integer = v.at(physical) },
                .dictionary_f64 => |v| blk: {
                    if (!std.math.isFinite(v.at(physical))) return error.SqlTypeMismatch;
                    break :blk .{ .float = v.at(physical) };
                },
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
                .dictionary_i64 => |values| .{ .integer = values.at(index) },
                .dictionary_f64 => |values| .{ .float = values.at(index) },
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
    try std.testing.checkAllAllocationFailures(std.testing.allocator, fusedDictionaryScenario, .{});
}
