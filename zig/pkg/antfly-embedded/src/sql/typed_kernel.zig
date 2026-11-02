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
    /// Gather validated native payloads directly into the kernel workspace.
    /// Selection and SQL NULL stay separate from primitive lanes; no Datum
    /// array or JSON tags are constructed for physical readers.
    pub fn fillPhysical(self: Vector, physical: @import("execution_batch.zig").PhysicalColumn) !bool {
        if (physical.len() != self.states.len) return error.InvalidSqlBackendResponse;
        const compatible = switch (physical.vector.values) {
            .i64, .dictionary_i64 => self.kind == .integer or self.kind == .number,
            .f64, .dictionary_f64 => self.kind == .number,
            .bool => self.kind == .boolean,
            .bytes, .dictionary_bytes => self.kind == .string,
            else => false,
        };
        if (!compatible) return false;
        try physical.validate();
        for (0..physical.len()) |row| {
            const source = physical.rowAt(row);
            self.states[row] = if (physical.vector.nulls.isNull(source)) .sql_null else .value;
            if (self.states[row] != .value) continue;
            switch (physical.vector.values) {
                inline .i64, .dictionary_i64 => |values| {
                    const value = if (comptime @TypeOf(values) == []const i64) values[source] else values.at(source);
                    if (self.kind == .integer) self.integers()[row] = value else self.numbers()[row] = @floatFromInt(value);
                },
                inline .f64, .dictionary_f64 => |values| self.numbers()[row] = if (comptime @TypeOf(values) == []const f64) values[source] else values.at(source),
                .bool => |values| self.booleans()[row] = values[source],
                .bytes => |values| self.strings()[row] = values[source],
                .dictionary_bytes => |values| self.strings()[row] = values.at(source),
                else => unreachable,
            }
        }
        return true;
    }
    pub fn set(self: Vector, row: usize, value: Datum) !void {
        if (value.patterns != null or value.array != null or value.numeric != null) return error.UnsupportedTypedKernel;
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
/// Owned delivery vectors keep computed primitive results typed across
/// operator boundaries. JSON NULL retains a distinct state and declines the
/// SQL-null-only physical trait, while scalar readers still see exact cells.
pub const Encoded = struct {
    vector: Vector,
    nulls: []u8,
    type: scalar.Type,
    json_null: bool = false,
    fn allocate(a: A, kind_: Kind, count: usize, type_: scalar.Type) !*Encoded {
        const self = try a.create(Encoded);
        errdefer a.destroy(self);
        const payload = try a.alloc(u64, count * (if (kind_ == .string) @as(usize, 2) else 1));
        errdefer a.free(payload);
        @memset(payload, 0);
        const states = try a.alloc(State, count);
        errdefer a.free(states);
        const nulls = try a.alloc(u8, count);
        errdefer a.free(nulls);
        self.* = .{ .vector = .{ .kind = kind_, .payload = payload, .states = states }, .nulls = nulls, .type = type_ };
        return self;
    }
    fn copy(self: *Encoded, source: Vector, begin: usize) void {
        for (source.states, 0..) |state, offset| {
            const row = begin + offset;
            self.vector.states[row] = state;
            self.nulls[row] = @intFromBool(state == .sql_null);
            self.json_null = self.json_null or state == .json_null;
            if (state != .value) continue;
            switch (source.kind) {
                .integer => self.vector.integers()[row] = source.integers()[offset],
                .number => self.vector.numbers()[row] = source.numbers()[offset],
                .boolean => self.vector.booleans()[row] = source.booleans()[offset],
                .string => self.vector.strings()[row] = source.strings()[offset],
                .none => unreachable,
            }
        }
    }
    fn create(a: A, source: Vector, type_: scalar.Type) !*Encoded {
        const self = try allocate(a, source.kind, source.states.len, type_);
        self.copy(source, 0);
        return self;
    }
    pub fn concat(a: A, parts: []const *const Encoded) !*Encoded {
        if (parts.len == 0) return error.InvalidSqlBackendResponse;
        var count: usize = 0;
        for (parts) |part| {
            if (part.vector.kind != parts[0].vector.kind or !std.meta.eql(part.type, parts[0].type)) return error.InvalidSqlBackendResponse;
            count = try std.math.add(usize, count, part.vector.states.len);
        }
        const self = try allocate(a, parts[0].vector.kind, count, parts[0].type);
        var begin: usize = 0;
        for (parts) |part| {
            self.copy(part.vector, begin);
            begin += part.vector.states.len;
        }
        return self;
    }
    fn read(raw: *anyopaque, _: A, row: usize, column: usize) !Datum {
        const self: *Encoded = @ptrCast(@alignCast(raw));
        if (column != 0 or row >= self.vector.states.len) return error.InvalidSqlBackendResponse;
        return self.vector.get(row);
    }
    fn physical(raw: *anyopaque, _: A, column: usize) !?@import("execution_batch.zig").PhysicalColumn {
        const self: *Encoded = @ptrCast(@alignCast(raw));
        if (column != 0) return error.InvalidSqlBackendResponse;
        if (self.json_null or self.type.kind == null or self.vector.kind == .none) return null;
        return .{ .vector = .{ .name = "", .nulls = .{ .bytes = self.nulls }, .values = switch (self.vector.kind) {
            .integer => .{ .i64 = self.vector.integers() },
            .number => .{ .f64 = self.vector.numbers() },
            .boolean => .{ .bool = self.vector.booleans() },
            .string => .{ .bytes = self.vector.strings() },
            .none => unreachable,
        } }, .range = .{ .count = self.vector.states.len }, .kind = self.type.kind.?, .element = self.type.element_type };
    }
    pub fn destroy(raw: *anyopaque, a: A) void {
        const self: *Encoded = @ptrCast(@alignCast(raw));
        a.free(self.vector.payload);
        a.free(self.vector.states);
        a.free(self.nulls);
        a.destroy(self);
    }
    pub fn batch(self: *Encoded) @import("execution_batch.zig").Batch {
        return .{ .reader = .{ .ptr = self, .read = read, .read_physical = physical, .free_owned = destroy, .count = self.vector.states.len, .width = 1 } };
    }
};
fn Delivery(comptime encoded: bool) type {
    return if (encoded) ?*Encoded else []const Datum;
}
pub fn evaluateEncoded(a: A, programs: []const *const scalar.Program, input: anytype, parameters: []const std.json.Value) !?[]const ?*Encoded {
    return evaluateImpl(true, a, programs, input, parameters) catch |err| switch (err) {
        error.UnsupportedTypedKernel => null,
        else => return err,
    };
}
fn kind(type_: scalar.Type) ?Kind {
    if (type_.element_type == .numeric) return null; // Decimal limbs retain exact scalar evaluation.
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
/// Prepared single-program numeric island. Preparation may allocate/coerce and
/// call the input provider; run never does. The caller retains this scratch and
/// its original operation owner through result handoff and physical retirement.
/// Fixed work counts are not a measured non-yielding-time qualification.
pub const PreparedNumeric = struct {
    pub const max_instructions = 64;
    pub const max_rows = 64;
    pub const max_instruction_cells = max_instructions * max_rows;
    pub const max_cell_steps = 2 * max_instruction_cells + max_rows;
    /// One instruction plus its integer-width check, or final root output.
    pub const max_step_cell_steps = 2 * max_rows;
    pub const max_steps = max_instructions + 1;
    pub const scratch_bytes = @sizeOf(PreparedNumeric);
    allocator: A,
    program: *const scalar.Program,
    instruction_count: usize,
    root: usize,
    instructions: [max_instructions]scalar.Instruction = undefined,
    payload: [max_instructions][max_rows]u64 = undefined,
    states: [max_instructions][max_rows]State = undefined,
    output: [max_rows]Datum = undefined,
    count: usize = 0,
    next_instruction: usize = 0,
    phase: enum { empty, prepared, running, complete, failed } = .empty,

    pub fn eligible(program: *const scalar.Program) !bool {
        if (!supported(program) or program.instructions.len > max_instructions) return false;
        for (program.instructions, 0..) |instruction, index| {
            if (kind(instruction.type) == .string) return false;
            if (instruction.operation == .literal) switch (instruction.operation.literal) {
                .null, .integer, .float, .bool => {},
                else => return false,
            };
            var operands: [2]u32 = undefined;
            for (children(instruction, &operands)) |operand| if (operand >= index) return error.InvalidSqlBackendResponse;
        }
        return true;
    }
    pub fn init(a: A, program: *const scalar.Program) !?*PreparedNumeric {
        if (!try eligible(program)) return null;
        const self = try a.create(PreparedNumeric);
        self.* = .{ .allocator = a, .program = program, .instruction_count = program.instructions.len, .root = program.root };
        @memcpy(self.instructions[0..program.instructions.len], program.instructions);
        return self;
    }
    pub fn deinit(self: *PreparedNumeric) void {
        self.allocator.destroy(self);
    }
    fn vector(self: *PreparedNumeric, index: usize) Vector {
        return .{ .kind = kind(self.instructions[index].type).?, .payload = self.payload[index][0..self.count], .states = self.states[index][0..self.count] };
    }
    /// Call only outside a CPU-loan/bounded interval. A failed prepare exposes
    /// no partial chunk. Inputs become owned numeric values in fixed scratch.
    pub fn prepare(self: *PreparedNumeric, input: anytype, parameters: []const std.json.Value) !void {
        return self.prepareWithAllocator(self.allocator, input, parameters);
    }
    /// Fixed stage storage and preparation/coercion storage may have distinct
    /// owners. The preparation allocator retains its original lifetime; values
    /// are copied into the fixed numeric payload before the math stage runs.
    pub fn prepareWithAllocator(self: *PreparedNumeric, preparation: A, input: anytype, parameters: []const std.json.Value) !void {
        self.phase = .empty;
        self.next_instruction = 0;
        if (input.count > max_rows) return error.InvalidPreparedNumericRows;
        self.count = input.count;
        for (self.instructions[0..self.instruction_count], 0..) |instruction, index| {
            const target = self.vector(index);
            switch (instruction.operation) {
                .literal => |value| for (0..self.count) |row| try target.set(row, Datum.fromJson(value)),
                .parameter => {
                    const value = try self.program.evaluateInstruction(preparation, @intCast(index), parameters);
                    for (0..self.count) |row| try target.set(row, value);
                },
                .column => |ordinal| {
                    const filled = if (comptime @hasDecl(@TypeOf(input), "fillTyped")) try input.fillTyped(preparation, ordinal, target) else false;
                    if (!filled) for (0..self.count) |row| try target.set(row, try input.cell(preparation, row, ordinal));
                },
                .unary, .binary => {},
                else => unreachable,
            }
        }
        self.phase = .prepared;
    }
    /// Drain the same resumable math state without scheduling callbacks. Callers
    /// needing execution boundaries use step() and retain this owner between
    /// calls. This convenience method does not qualify an entire chunk for loans.
    pub fn run(self: *PreparedNumeric) ![]const Datum {
        while (true) if (try self.step()) |output| return output;
    }
    /// Copy the finite result of a joined private clone back to its original
    /// coordinator. No program, allocator or input pointer changes owner.
    pub fn acceptCompleted(self: *PreparedNumeric, other: *const PreparedNumeric) !void {
        if (self.phase != .prepared or other.phase != .complete or self.count != other.count or
            self.root != other.root or self.instruction_count != other.instruction_count)
            return error.InvalidRunnableTransition;
        @memcpy(self.payload[self.root][0..self.count], other.payload[other.root][0..other.count]);
        @memcpy(self.states[self.root][0..self.count], other.states[other.root][0..other.count]);
        @memcpy(self.output[0..self.count], other.output[0..other.count]);
        self.phase = .complete;
    }
    /// Keep normal columnar delivery instead of expanding a prepared kernel
    /// into a page of Datum objects merely to schedule its arithmetic.
    pub fn allocateOutput(self: *const PreparedNumeric, a: A, rows: usize) !*Encoded {
        return Encoded.allocate(a, kind(self.instructions[self.root].type).?, rows, self.instructions[self.root].type);
    }
    pub fn copyOutput(self: *PreparedNumeric, output: *Encoded, begin: usize) !void {
        if (self.phase != .complete or begin > output.vector.states.len or self.count > output.vector.states.len - begin or
            !std.meta.eql(self.instructions[self.root].type, output.type)) return error.InvalidRunnableTransition;
        output.copy(self.vector(self.root), begin);
    }
    /// One fixed instruction (at most 64 evaluations plus 64 width checks), or
    /// 64 final root visits. No allocation, provider, coercion, callback or IO.
    /// Null retains a private continuation; only completion exposes output.
    /// The returned slice borrows this owner and expires at the next prepare.
    pub fn step(self: *PreparedNumeric) !?[]const Datum {
        if (self.phase != .prepared and self.phase != .running) return error.InvalidRunnableTransition;
        self.phase = .failed;
        if (self.next_instruction < self.instruction_count) {
            const index = self.next_instruction;
            const instruction = self.instructions[index];
            const target = self.vector(index);
            switch (instruction.operation) {
                .literal, .parameter, .column => {},
                .unary => |u| for (0..self.count) |row| try target.set(row, try @import("vector_eval.zig").unary(u.op, self.vector(u.operand).get(row))),
                .binary => |v| try binary(target, v.op, self.vector(v.left), self.vector(v.right)),
                else => unreachable,
            }
            if (target.kind == .integer and instruction.operation != .literal) if (instruction.type.element_type) |element| {
                if (element == .int16 or element == .int32) for (target.integers(), target.states) |value, state| {
                    if (state == .value) _ = try @import("builtin_cast.zig").checkedInteger(value, element);
                };
            };
            self.next_instruction += 1;
            self.phase = .running;
            return null;
        }
        const root = self.vector(self.root);
        for (self.output[0..self.count], 0..) |*value, row| {
            value.* = root.get(row);
            try scalar.validateResult(value.*, .{});
        }
        self.phase = .complete;
        return self.output[0..self.count];
    }
};

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
    return evaluateImpl(false, a, programs, input, parameters) catch |err| switch (err) {
        error.UnsupportedTypedKernel => null,
        else => return err,
    };
}

test "SQL typed kernels preserve int4 overflow and float4 node rounding" {
    const Input = struct {
        count: usize = 2,
        pub fn cell(_: @This(), _: A, _: usize, _: u32) !Datum {
            return error.UnexpectedColumnAccess;
        }
    };
    for ([_][]const u8{ "2147483646 + 1", "2147483647 + 1" }, 0..) |sql, index| {
        var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var program = try scalar.bind(std.testing.allocator, compiled.expression, &.{}, &.{}, .{});
        defer program.deinit();
        try std.testing.expect(supported(&program));
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        if (index == 1) {
            try std.testing.expectError(error.SqlNumericOutOfRange, evaluate(arena.allocator(), &.{&program}, Input{}, &.{}));
            try std.testing.expectError(error.SqlNumericOutOfRange, program.evaluate(arena.allocator(), &.{}, &.{}, .{}));
        } else {
            const result = (try evaluate(arena.allocator(), &.{&program}, Input{}, &.{})).?;
            for (result[0]) |value| try std.testing.expectEqual(@as(i64, 2147483647), value.value.integer);
        }
    }
    var compiled = try @import("compiler.zig").compileScalar(std.testing.allocator, "x + x", .{});
    defer compiled.deinit();
    var program = try scalar.bind(std.testing.allocator, compiled.expression, &.{.{ .name = "x", .type = .number, .element_type = .float32 }}, &.{}, .{});
    defer program.deinit();
    try std.testing.expect(supported(&program));
}

fn evaluateImpl(comptime encoded: bool, a: A, programs: []const *const scalar.Program, input: anytype, parameters: []const std.json.Value) !?[]const Delivery(encoded) {
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
    const outputs = try a.alloc(Delivery(encoded), programs.len);
    @memset(outputs, if (encoded) null else &.{});
    errdefer {
        for (outputs) |values| {
            if (encoded) {
                if (values) |owned| Encoded.destroy(owned, a);
            } else a.free(values);
        }
        a.free(outputs);
    }
    if (!encoded) for (outputs) |*output| {
        output.* = try a.alloc(Datum, input.count);
    };
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
    // Physical adapters may allocate selections, null masks or normalized
    // lanes. Reclaim those independently of delivery vectors. Primitive
    // gathers copy every lane and reuse one column's workspace; string
    // comparison inputs remain alive until their final consuming node.
    var physical_scratch = std.heap.ArenaAllocator.init(a);
    defer physical_scratch.deinit();
    var string_scratch = std.heap.ArenaAllocator.init(a);
    defer string_scratch.deinit();
    const string_output = for (roots[0..programs.len]) |root| {
        if (kind(instructions[root].type) == .string) break true;
    } else false;
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
                _ = physical_scratch.reset(.retain_capacity);
                // Escaping strings use the established scalar column ownership
                // contract. Identity encoded outputs already borrow columns.
                const filled = if (comptime @hasDecl(@TypeOf(input), "fillTyped")) if (target.kind == .string and string_output) false else try input.fillTyped(if (target.kind == .string) string_scratch.allocator() else physical_scratch.allocator(), ordinal, target) else false;
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
        if (target.kind == .integer and instruction.operation != .literal) if (instruction.type.element_type) |element| {
            if (element == .int16 or element == .int32) for (target.integers(), target.states) |value, state| {
                if (state == .value) _ = try @import("builtin_cast.zig").checkedInteger(value, element);
            };
        };
        if (target.kind == .number and instruction.type.element_type == .float32) {
            for (target.numbers(), target.states) |*value, state| if (state == .value) {
                value.* = try @import("builtin_cast.zig").floatValue(f32, .{ .float = value.* });
            };
        }
        // Materialize roots when ready, so delivery vectors do not pin live
        // kernel slots while later expressions reuse the shared DAG.
        for (roots[0..programs.len], outputs) |root, *output| if (root == i) {
            if (encoded) {
                // Validate using the same scalar boundary; no Datum delivery
                // array is allocated or retained for primitive consumers.
                for (0..input.count) |row| try scalar.validateResult(target.get(row), .{});
                output.* = try Encoded.create(a, target, instruction.type);
            } else for (@constCast(output.*), 0..) |*value, row| {
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

test "SQL typed kernels round REAL inputs and intermediate nodes with scalar errors" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var compiled = try @import("compiler.zig").compileScalar(a, "(x + x) - x", .{});
    defer compiled.deinit();
    var program = try scalar.bind(a, compiled.expression, &.{.{ .name = "x", .type = .number, .element_type = .float32 }}, &.{}, .{});
    defer program.deinit();
    const Input = struct {
        count: usize = 4,
        value: f64,
        pub fn cell(self: @This(), _: A, _: usize, _: u32) !Datum {
            return Datum.json(.{ .float = self.value });
        }
    };
    for ([_]f64{ 16777217, -16777217, 0, -0.0 }) |value| {
        const result = (try evaluateEncoded(ca, &.{&program}, Input{ .value = value }, &.{})).?;
        const expected = try program.evaluate(ca, &.{try @import("describe.zig").coerceDatum(ca, Datum.json(.{ .float = value }), .number, .float32)}, &.{}, .{});
        const batch = result[0].?.batch();
        for (0..batch.len()) |row| try std.testing.expectEqualDeep(expected, try batch.cell(ca, row, 0));
    }
    for ([_]f64{ 1e-50, std.math.floatMax(f32) }) |value| {
        try std.testing.expectError(error.SqlNumericOutOfRange, evaluateEncoded(ca, &.{&program}, Input{ .value = value }, &.{}));
    }
}

const PreparedTestInput = struct {
    count: usize,
    rows: []const []const Datum,
    calls: *usize,
    pub fn cell(self: @This(), _: A, row: usize, ordinal: u32) !Datum {
        self.calls.* += 1;
        return self.rows[row][ordinal];
    }
};

test "SQL prepared numeric scratch preserves exact arithmetic nulls and comparisons without evaluation allocation" {
    const a = std.testing.allocator;
    var cells: [64][1]Datum = undefined;
    var rows: [64][]const Datum = undefined;
    for (&cells, &rows, 0..) |*cell, *row, i| {
        cell[0] = if (i % 16 < 12) Datum.json(.{ .integer = if (i % 2 == 0) 9007199254740993 else -3 }) else if (i % 2 == 0) .{} else Datum.json(.null);
        row.* = cell;
    }
    for ([_][]const u8{ "n * 2 + 1", "n / 2", "n % 2", "n > 9007199254740992", "n IS NULL", "n IS DISTINCT FROM NULL", "-n" }) |text| {
        var compiled = try @import("compiler.zig").compileScalar(a, text, .{});
        defer compiled.deinit();
        var program = try scalar.bind(a, compiled.expression, &.{.{ .name = "n", .type = .integer }}, &.{}, .{});
        defer program.deinit();
        var allocations = std.testing.FailingAllocator.init(a, .{});
        const stage = (try PreparedNumeric.init(allocations.allocator(), &program)).?;
        defer stage.deinit();
        var calls: usize = 0;
        try stage.prepare(PreparedTestInput{ .count = rows.len, .rows = &rows, .calls = &calls }, &.{});
        const prepared_calls = calls;
        const prepared_allocations = allocations.alloc_index;
        allocations.fail_index = prepared_allocations;
        const original = cells[0][0];
        cells[0][0] = Datum.json(.{ .string = "input is no longer numeric" });
        const result = try stage.run();
        cells[0][0] = original;
        try std.testing.expectEqual(prepared_calls, calls);
        try std.testing.expectEqual(prepared_allocations, allocations.alloc_index);
        for (rows, result) |row, actual| try std.testing.expectEqualDeep(try program.evaluate(a, row, &.{}, .{}), actual);
        try std.testing.expectError(error.InvalidRunnableTransition, stage.run());
    }
}

test "SQL prepared numeric failed preparation invalidates previous chunk and bounds rows before input access" {
    const a = std.testing.allocator;
    var compiled = try @import("compiler.zig").compileScalar(a, "n + 1", .{});
    defer compiled.deinit();
    var program = try scalar.bind(a, compiled.expression, &.{.{ .name = "n", .type = .integer }}, &.{}, .{});
    defer program.deinit();
    const stage = (try PreparedNumeric.init(a, &program)).?;
    defer stage.deinit();
    var calls: usize = 0;
    const good = PreparedTestInput{ .count = 1, .rows = &.{&.{Datum.json(.{ .integer = 41 })}}, .calls = &calls };
    try stage.prepare(good, &.{});
    try std.testing.expectEqual(@as(i64, 42), (try stage.run())[0].value.integer);
    var too_many = good;
    too_many.count = 65;
    try std.testing.expectError(error.InvalidPreparedNumericRows, stage.prepare(too_many, &.{}));
    try std.testing.expectEqual(@as(usize, 1), calls);
    try std.testing.expectError(error.InvalidRunnableTransition, stage.run());
    try std.testing.expectError(error.UnsupportedTypedKernel, stage.prepare(PreparedTestInput{ .count = 1, .rows = &.{&.{Datum.json(.{ .string = "unqualified" })}}, .calls = &calls }, &.{}));
    try std.testing.expectError(error.InvalidRunnableTransition, stage.run());
    try stage.prepare(good, &.{});
    try std.testing.expectEqual(@as(i64, 42), (try stage.run())[0].value.integer);
    var empty = good;
    empty.count = 0;
    try stage.prepare(empty, &.{});
    try std.testing.expectEqual(@as(usize, 0), (try stage.run()).len);
}

test "SQL prepared numeric rejects strings lazy programs and allocation failure before publication" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "'unbounded string' = 'unbounded string'", "CASE WHEN TRUE THEN 7 ELSE 1 / 0 END" }) |text| {
        var compiled = try @import("compiler.zig").compileScalar(a, text, .{});
        defer compiled.deinit();
        var program = try scalar.bind(a, compiled.expression, &.{}, &.{}, .{});
        defer program.deinit();
        var unavailable = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        try std.testing.expect((try PreparedNumeric.init(unavailable.allocator(), &program)) == null);
        try std.testing.expectEqual(@as(usize, 0), unavailable.alloc_index);
    }
    var compiled = try @import("compiler.zig").compileScalar(a, "1 + 2", .{});
    defer compiled.deinit();
    var program = try scalar.bind(a, compiled.expression, &.{}, &.{}, .{});
    defer program.deinit();
    var unavailable = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, PreparedNumeric.init(unavailable.allocator(), &program));
}

test "SQL prepared numeric arithmetic errors retain scalar identities and never publish partial output" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "2147483647 + 1", "1 / 0", "-(-9223372036854775807 - 1)" }) |text| {
        var compiled = try @import("compiler.zig").compileScalar(a, text, .{});
        defer compiled.deinit();
        var program = try scalar.bind(a, compiled.expression, &.{}, &.{}, .{});
        defer program.deinit();
        const stage = (try PreparedNumeric.init(a, &program)).?;
        defer stage.deinit();
        var calls: usize = 0;
        try stage.prepare(PreparedTestInput{ .count = 1, .rows = &.{&.{}}, .calls = &calls }, &.{});
        const expected = program.evaluate(a, &.{}, &.{}, .{}) catch |err| {
            try std.testing.expectError(err, stage.run());
            try std.testing.expectError(error.InvalidRunnableTransition, stage.run());
            continue;
        };
        _ = expected;
        return error.ExpectedScalarNumericFailure;
    }
}

test "SQL prepared numeric coerces parameters before evaluation and preserves floating and boolean semantics" {
    const a = std.testing.allocator;
    var compiled = try @import("compiler.zig").compileScalar(a, "n + $1", .{});
    defer compiled.deinit();
    var program = try scalar.bind(a, compiled.expression, &.{.{ .name = "n", .type = .number, .element_type = .float64 }}, &.{}, .{});
    defer program.deinit();
    var allocations = std.testing.FailingAllocator.init(a, .{});
    const stage = (try PreparedNumeric.init(allocations.allocator(), &program)).?;
    defer stage.deinit();
    var calls: usize = 0;
    const input = PreparedTestInput{ .count = 1, .rows = &.{&.{Datum.json(.{ .float = 1.25 })}}, .calls = &calls };
    try stage.prepare(input, &.{.{ .float = 2.5 }});
    allocations.fail_index = allocations.alloc_index;
    try std.testing.expectEqualDeep(try program.evaluate(a, input.rows[0], &.{.{ .float = 2.5 }}, .{}), (try stage.run())[0]);
    var boolean_compiled = try @import("compiler.zig").compileScalar(a, "NOT n", .{});
    defer boolean_compiled.deinit();
    var boolean = try scalar.bind(a, boolean_compiled.expression, &.{.{ .name = "n", .type = .boolean }}, &.{}, .{});
    defer boolean.deinit();
    const boolean_stage = (try PreparedNumeric.init(a, &boolean)).?;
    defer boolean_stage.deinit();
    try boolean_stage.prepare(PreparedTestInput{ .count = 1, .rows = &.{&.{Datum.json(.{ .bool = true })}}, .calls = &calls }, &.{});
    try std.testing.expectEqual(false, (try boolean_stage.run())[0].value.bool);
    try std.testing.expectEqual(@as(usize, 8256), PreparedNumeric.max_cell_steps);
    try std.testing.expect(PreparedNumeric.scratch_bytes < 128 * 1024);
}

test "SQL prepared numeric retains bulk typed column preparation without per cell provider lookup" {
    const a = std.testing.allocator;
    const Input = struct {
        count: usize = 4,
        calls: *usize,
        pub fn fillTyped(self: @This(), _: std.mem.Allocator, ordinal: u32, target: anytype) !bool {
            if (ordinal != 0) return error.InvalidSqlBackendResponse;
            self.calls.* += 1;
            for (0..self.count) |row| try target.set(row, Datum.json(.{ .integer = @intCast(row + 40) }));
            return true;
        }
        pub fn cell(_: @This(), _: A, _: usize, _: u32) !Datum {
            return error.UnexpectedPerCellProviderLookup;
        }
    };
    var compiled = try @import("compiler.zig").compileScalar(a, "n + 1", .{});
    defer compiled.deinit();
    var program = try scalar.bind(a, compiled.expression, &.{.{ .name = "n", .type = .integer }}, &.{}, .{});
    defer program.deinit();
    const stage = (try PreparedNumeric.init(a, &program)).?;
    defer stage.deinit();
    var calls: usize = 0;
    try stage.prepare(Input{ .calls = &calls }, &.{});
    const values = try stage.run();
    try std.testing.expectEqual(@as(usize, 1), calls);
    for (values, 0..) |value, row| try std.testing.expectEqual(@as(i64, @intCast(row + 41)), value.value.integer);
}

test "SQL prepared numeric steps retain a full fixed continuation without provider or allocation access" {
    const a = std.testing.allocator;
    var compiled = try @import("compiler.zig").compileScalar(a, "n", .{});
    defer compiled.deinit();
    var base = try scalar.bind(a, compiled.expression, &.{.{ .name = "n", .type = .integer, .element_type = .int16 }}, &.{}, .{});
    defer base.deinit();
    var instructions: [PreparedNumeric.max_instructions]scalar.Instruction = undefined;
    instructions[0] = .{ .type = base.output_type, .operation = .{ .column = 0 } };
    for (instructions[1..], 1..) |*instruction, index| instruction.* = .{ .type = base.output_type, .operation = .{ .binary = .{ .op = .add, .left = @intCast(index - 1), .right = 0 } } };
    var expanded = base;
    expanded.instructions = &instructions;
    expanded.root = instructions.len - 1;
    var allocations = std.testing.FailingAllocator.init(a, .{});
    const stage = (try PreparedNumeric.init(allocations.allocator(), &expanded)).?;
    defer stage.deinit();
    var cells: [64][1]Datum = undefined;
    var rows: [64][]const Datum = undefined;
    for (&cells, &rows, 0..) |*cell, *row, index| {
        cell[0] = Datum.json(.{ .integer = @intCast(index + 1) });
        row.* = cell;
    }
    var calls: usize = 0;
    try stage.prepare(PreparedTestInput{ .count = rows.len, .rows = &rows, .calls = &calls }, &.{});
    const prepared_calls = calls;
    const prepared_allocations = allocations.alloc_index;
    allocations.fail_index = prepared_allocations;
    for (&cells) |*cell| cell[0] = Datum.json(.{ .string = "provider storage changed after preparation" });
    for (0..instructions.len) |index| {
        try std.testing.expect((try stage.step()) == null);
        try std.testing.expectEqual(index + 1, stage.next_instruction);
        try std.testing.expectEqual(prepared_calls, calls);
        try std.testing.expectEqual(prepared_allocations, allocations.alloc_index);
    }
    const result = (try stage.step()).?;
    for (result, 0..) |value, index| try std.testing.expectEqual(@as(i64, @intCast(64 * (index + 1))), value.value.integer);
    try std.testing.expectEqual(@as(usize, 128), PreparedNumeric.max_step_cell_steps);
    try std.testing.expectEqual(@as(usize, 65), PreparedNumeric.max_steps);
    try std.testing.expectEqual(prepared_calls, calls);
    try std.testing.expectEqual(prepared_allocations, allocations.alloc_index);
    try std.testing.expectError(error.InvalidRunnableTransition, stage.step());
}

test "SQL prepared numeric step failure and reprepare never publish an old continuation" {
    const a = std.testing.allocator;
    var compiled = try @import("compiler.zig").compileScalar(a, "n", .{});
    defer compiled.deinit();
    var base = try scalar.bind(a, compiled.expression, &.{.{ .name = "n", .type = .integer, .element_type = .int16 }}, &.{}, .{});
    defer base.deinit();
    const instructions = [_]scalar.Instruction{
        .{ .type = base.output_type, .operation = .{ .column = 0 } },
        .{ .type = base.output_type, .operation = .{ .literal = .{ .integer = 1 } } },
        .{ .type = base.output_type, .operation = .{ .binary = .{ .op = .add, .left = 0, .right = 1 } } },
    };
    var expanded = base;
    expanded.instructions = &instructions;
    expanded.root = 2;
    const stage = (try PreparedNumeric.init(a, &expanded)).?;
    defer stage.deinit();
    var calls: usize = 0;
    try stage.prepare(PreparedTestInput{ .count = 1, .rows = &.{&.{Datum.json(.{ .integer = 32767 })}}, .calls = &calls }, &.{});
    try std.testing.expect((try stage.step()) == null);
    try std.testing.expect((try stage.step()) == null);
    try std.testing.expectError(error.SqlNumericOutOfRange, stage.step());
    try std.testing.expectError(error.InvalidRunnableTransition, stage.step());
    try std.testing.expectError(error.InvalidRunnableTransition, stage.run());
    const replacement = PreparedTestInput{ .count = 1, .rows = &.{&.{Datum.json(.{ .integer = 41 })}}, .calls = &calls };
    try stage.prepare(replacement, &.{});
    try std.testing.expect((try stage.step()) == null);
    // Discard a suspended partial computation and start an independent chunk.
    try stage.prepare(replacement, &.{});
    try std.testing.expectEqual(@as(usize, 0), stage.next_instruction);
    try std.testing.expectEqual(@as(i64, 42), (try stage.run())[0].value.integer);
}
