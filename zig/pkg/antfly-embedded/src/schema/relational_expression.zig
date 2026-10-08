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

//! Immutable bounded scalar bytecode. Names, types, literals and operations
//! determine identity; schema epochs and physical ordinals do not.
const std = @import("std");
const schema = @import("../storage/schema.zig");
const checks = @import("relational_checks.zig");
const codec = @import("../storage/db/algebraic/relational_row_codec.zig");
pub const Value = @import("../storage/db/relational_index_keys.zig").Value;
pub const Kind = schema.RelationalColumnType;
const Allocator = std.mem.Allocator;
const Numeric = @import("../common/sql_builtin_type.zig").Type;
const casts = @import("../sql/builtin_cast.zig");
const exact = @import("../sql/numeric_value.zig");
const binary = @import("../sql/numeric_binary.zig");
pub const max_nodes = 128;
pub const max_depth = 16;
pub const max_output_bytes = 1024 * 1024;
pub const max_allocated_bytes = 4 * max_output_bytes;

/// One invocation can span several immutable plans. Work/cancellation remain
/// sticky across nodes and bindings, independently of retained-byte admission.
pub const Execution = struct {
    alloc: Allocator,
    bytes: *usize,
    numeric: exact.Context,

    pub fn init(alloc: Allocator, bytes: *usize) Execution {
        return .{ .alloc = alloc, .bytes = bytes, .numeric = .{ .alloc = alloc } };
    }

    pub fn charge(self: *Execution, work: u64) !void {
        self.numeric.charge(work) catch |err| return executionFailure(err);
    }

    pub fn limit(self: *Execution) anyerror {
        return executionFailure(self.numeric.limit());
    }
};

/// Temporarily own bounded unpublished results while retaining the caller's
/// work/cancellation identity. Reconcile capacity against existing VM charges
/// rather than charging the same retained allocation twice.
pub const ExecutionScratch = struct {
    execution: *Execution,
    memory: @import("../sql/memory_budget.zig"),
    arena: std.heap.ArenaAllocator,
    saved_alloc: Allocator,
    saved_numeric_alloc: Allocator,
    initial_bytes: usize,

    pub fn init(self: *ExecutionScratch, execution: *Execution) void {
        self.* = .{
            .execution = execution,
            .memory = .{ .backing = execution.alloc, .limit = execution.bytes.* },
            .arena = undefined,
            .saved_alloc = execution.alloc,
            .saved_numeric_alloc = execution.numeric.alloc,
            .initial_bytes = execution.bytes.*,
        };
        self.arena = .init(self.memory.allocator());
        execution.alloc = self.arena.allocator();
        execution.numeric.alloc = execution.alloc;
    }

    pub fn deinit(self: *ExecutionScratch) void {
        self.arena.deinit();
        const charged = self.initial_bytes -| self.execution.bytes.*;
        self.execution.bytes.* = self.initial_bytes -| @max(charged, self.memory.peak);
        self.execution.alloc = self.saved_alloc;
        self.execution.numeric.alloc = self.saved_numeric_alloc;
    }

    pub fn failure(self: *ExecutionScratch, err: anyerror) anyerror {
        // Borrowed comparisons can exhaust logical byte admission without
        // allocating. Keep their failure as sticky as allocator admission.
        if (err == error.RelationalExpressionBudgetExceeded or (err == error.OutOfMemory and self.memory.isExhausted())) return self.execution.limit();
        return executionFailure(err);
    }
};

// Preserve the durable validation/transport contract. Numeric kernel errors
// must not turn deterministic bad rows into retryable activation failures.
fn executionFailure(err: anyerror) anyerror {
    return switch (err) {
        error.SqlDivisionByZero => error.RelationalExpressionDivisionByZero,
        error.InvalidSqlNumber, error.SqlNumericOutOfRange => error.RelationalExpressionOverflow,
        error.SqlInvalidTextRepresentation => error.InvalidRelationalExpressionInput,
        error.SqlProgramLimitExceeded => error.RelationalExpressionBudgetExceeded,
        else => err,
    };
}

/// Numeric scratch is an unpublished bounded region. Charge arena capacity,
/// not only result bytes: a caller arena may not reclaim out-of-order frees.
const NumericScratch = struct {
    execution: *Execution,
    memory: @import("../sql/memory_budget.zig"),
    arena: std.heap.ArenaAllocator = undefined,
    saved_alloc: Allocator,
    saved_output: usize,
    saved_groups: usize,

    fn init(self: *NumericScratch, execution: *Execution) void {
        self.* = .{
            .execution = execution,
            .memory = .{ .backing = execution.alloc, .limit = execution.bytes.* },
            .saved_alloc = execution.numeric.alloc,
            .saved_output = execution.numeric.max_output_bytes,
            .saved_groups = execution.numeric.max_groups,
        };
        self.arena = .init(self.memory.allocator());
        execution.numeric.alloc = self.arena.allocator();
        execution.numeric.max_output_bytes = @min(self.saved_output, @min(max_output_bytes, execution.bytes.*));
        execution.numeric.max_groups = @min(self.saved_groups, execution.bytes.* / 2);
    }

    fn deinit(self: *NumericScratch) void {
        self.arena.deinit();
        self.execution.bytes.* -|= self.memory.peak;
        self.execution.numeric.alloc = self.saved_alloc;
        self.execution.numeric.max_output_bytes = self.saved_output;
        self.execution.numeric.max_groups = self.saved_groups;
    }

    fn encode(self: *NumericScratch, value: exact.Value) !Value {
        const ctx = &self.execution.numeric;
        const size = try binary.encodedSize(ctx, value);
        if (size > self.execution.bytes.* -| self.memory.peak) return error.RelationalExpressionBudgetExceeded;
        const output = try allocateOutput(self.execution.alloc, size, self.execution.bytes);
        errdefer self.execution.alloc.free(output);
        var writer: std.Io.Writer = .fixed(output);
        try binary.encode(ctx, value, &writer);
        return .{ .numeric = output };
    }

    fn json(self: *NumericScratch, value: exact.Value) !std.json.Value {
        const text = try exact.format(&self.execution.numeric, value);
        if (text.len > self.execution.bytes.* -| self.memory.peak) return error.RelationalExpressionBudgetExceeded;
        const output = try allocateOutput(self.execution.alloc, text.len, self.execution.bytes);
        @memcpy(output, text);
        return if (value.kind == .finite) .{ .number_string = output } else .{ .string = output };
    }

    fn failure(self: *NumericScratch, err: anyerror) anyerror {
        if (err == error.RelationalExpressionBudgetExceeded or (err == error.OutOfMemory and self.memory.isExhausted())) return self.execution.limit();
        return executionFailure(err);
    }
};

pub fn numericJson(execution: *Execution, input: std.json.Value) !Value {
    try execution.charge(0);
    if (input == .null) return .null;
    var scratch: NumericScratch = undefined;
    scratch.init(execution);
    defer scratch.deinit();
    const parsed = @import("../sql/numeric_storage.zig").fromJson(&execution.numeric, input) catch |err| return scratch.failure(err);
    return scratch.encode(parsed.value) catch |err| return scratch.failure(err);
}

/// Assignment and logical restore share exact parsing, work and cancellation.
/// Preservation validates the target domain without repairing logical values;
/// the physical row codec separately enforces canonical bytes.
pub fn normalizeNumericJson(execution: *Execution, input: std.json.Value, modifier: ?exact.TypeModifier, preserve: bool) !std.json.Value {
    execution.numeric.charge(0) catch |err| return executionFailure(err);
    if (input == .null) return input;
    var scratch: NumericScratch = undefined;
    scratch.init(execution);
    defer scratch.deinit();
    return normalizeNumericJsonInner(&scratch, input, modifier, preserve) catch |err| return scratch.failure(err);
}

fn normalizeNumericJsonInner(scratch: *NumericScratch, input: std.json.Value, modifier: ?exact.TypeModifier, preserve: bool) !std.json.Value {
    const ctx = &scratch.execution.numeric;
    const parsed = try @import("../sql/numeric_storage.zig").fromJson(ctx, input);
    const constrained = if (modifier) |target| try exact.applyTypeModifier(ctx, parsed.value, target) else parsed;
    if (preserve) {
        if (modifier != null and try exact.order(ctx, parsed.value, constrained.value) != .eq) return error.InvalidRelationalGeneratedValue;
        return input;
    }
    return scratch.json(constrained.value);
}

/// NUMERIC array cells borrow one reusable scratch arena and one sticky work
/// budget. No cell gets a fresh quota. Publish replacement values only after
/// complete admission, preserving dimensions, SQL NULLs and signed bounds.
pub fn normalizeNumericArrayJson(execution: *Execution, input: *std.json.Value, modifier: ?exact.TypeModifier, preserve: bool) !void {
    execution.numeric.charge(0) catch |err| return executionFailure(err);
    const wire = @import("../sql/array_wire.zig");
    // Borrowed input has its existing wire/domain bounds; only actual scratch
    // and owned output consume the preparation allocation allowance.
    const inspected = wire.inspectNumericEnvelope(input.*, .{ .values = .{
        .work = @intCast(@min(execution.numeric.remaining, std.math.maxInt(usize))),
    } }) catch |err| {
        return if (err == error.SqlProgramLimitExceeded) executionFailure(execution.numeric.limit()) else err;
    };
    execution.numeric.charge(inspected.admission.work) catch |err| return executionFailure(err);
    const alloc = execution.alloc;
    const replacement: ?[]std.json.Value = if (preserve) null else blk: {
        const size = std.math.mul(usize, inspected.values.len, @sizeOf(std.json.Value)) catch return executionFailure(execution.numeric.limit());
        if (size > execution.bytes.*) return executionFailure(execution.numeric.limit());
        const cells = try alloc.alloc(std.json.Value, inspected.values.len);
        execution.bytes.* -= size;
        break :blk cells;
    };
    var completed: usize = 0;
    errdefer if (replacement) |cells| {
        for (cells[0..completed]) |cell| if (cell == .string) alloc.free(cell.string);
        alloc.free(cells);
    };
    var scratch: NumericScratch = undefined;
    scratch.init(execution);
    defer scratch.deinit();
    for (inspected.values, inspected.nulls, 0..) |raw, flag, index| {
        execution.numeric.charge(1) catch |err| return scratch.failure(err);
        if (flag.bool) {
            if (replacement) |cells| cells[index] = .null;
        } else {
            _ = scratch.arena.reset(.retain_capacity);
            // Output admission reduces the capacity available to later cells.
            scratch.memory.limit = execution.bytes.*;
            const value = normalizeNumericJsonInner(&scratch, raw, modifier, preserve) catch |err| return scratch.failure(err);
            if (replacement) |cells| cells[index] = .{ .string = if (value == .number_string) value.number_string else value.string };
        }
        completed += 1;
    }
    if (replacement) |cells| input.object.getPtr("values").?.* = .{ .array = .fromOwnedSlice(alloc, cells) };
}

/// Already constrained canonical values are reused without decoding limbs or
/// allocating output. Logical restore may preserve equivalent display scales,
/// but never a value changed by assignment; physical restore is stricter.
fn normalizeNumericBinding(execution: *Execution, bytes: []const u8, modifier: exact.TypeModifier, preserve: bool) !Value {
    const view = binary.layout.View.openWithBudget(bytes, .{ .bytes = execution.numeric.max_input_bytes, .groups = execution.numeric.max_groups }, &execution.numeric) catch |err| return executionFailure(err);
    if (view.verifyModifier(modifier, &execution.numeric)) |_| {
        return .{ .numeric = bytes };
    } else |err| if (err != error.InvalidSqlBinaryRepresentation) return executionFailure(err);
    var scratch: NumericScratch = undefined;
    scratch.init(execution);
    defer scratch.deinit();
    const parsed = binary.decodeCanonical(&execution.numeric, bytes) catch |err| return scratch.failure(err);
    const constrained = exact.applyTypeModifier(&execution.numeric, parsed.value, modifier) catch |err| return scratch.failure(err);
    if (preserve) {
        if ((exact.order(&execution.numeric, parsed.value, constrained.value) catch |err| return scratch.failure(err)) != .eq)
            return error.InvalidRelationalGeneratedValue;
        return .{ .numeric = bytes };
    }
    return scratch.encode(constrained.value) catch |err| return scratch.failure(err);
}

fn numericJsonOutput(execution: *Execution, bytes: []const u8) !std.json.Value {
    var scratch: NumericScratch = undefined;
    scratch.init(execution);
    defer scratch.deinit();
    const parsed = binary.decodeCanonical(&execution.numeric, bytes) catch |err| return scratch.failure(err);
    return scratch.json(parsed.value) catch |err| return scratch.failure(err);
}

const Op = enum { literal, column, add, subtract, multiply, divide, modulo, negate, concat, coalesce, lower_ascii, upper_ascii, eq, ne, gt, gte, lt, lte, is_null, is_not_null, is_distinct, is_not_distinct, @"and", @"or", not, cast, case_when, in_list, not_in_list };
const Node = struct {
    op: Op,
    kind: Kind,
    children: []const u16 = &.{},
    ordinal: u32 = 0,
    column_name: []const u8 = "",
    fold_ascii: bool = false,
    literal: Value = .null,
    sql_type: ?Numeric = null,
    numeric_modifier: ?exact.TypeModifier = null,
};

pub const Plan = struct {
    arena: std.heap.ArenaAllocator,
    nodes: []const Node,
    dependencies: []const u32,
    fingerprint: [32]u8,
    result_kind: Kind,
    literal_bytes: usize,

    pub fn init(alloc: Allocator, table: schema.TableSchema, expression: std.json.Value, expected: Kind) !Plan {
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        var bytes: usize = max_allocated_bytes;
        var execution = Execution.init(arena.allocator(), &bytes);
        var compiler: Compiler = .{ .alloc = arena.allocator(), .table = table, .execution = &execution };
        compiler.hash.update("antfly immutable scalar expression v1");
        const root = try compiler.compile(expression, 0);
        if (compiler.nodes.items[root].kind != expected) return error.InvalidRelationalExpressionType;
        var fingerprint: [32]u8 = undefined;
        compiler.hash.final(&fingerprint);
        return .{ .arena = arena, .nodes = compiler.nodes.items, .dependencies = compiler.dependencies.items, .fingerprint = fingerprint, .result_kind = expected, .literal_bytes = compiler.literal_bytes };
    }

    pub fn deinit(self: *Plan) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// Results borrow input values or allocate from the caller's row arena.
    /// Coalesce is lazy, so an unselected expression cannot raise an error.
    pub fn evaluate(self: *const Plan, alloc: Allocator, values: []const Value) !Value {
        var budget: usize = max_allocated_bytes;
        return self.evaluateWithBudget(alloc, values, &budget);
    }

    pub fn evaluateWithBudget(self: *const Plan, alloc: Allocator, values: []const Value, budget: *usize) !Value {
        var execution = Execution.init(alloc, budget);
        return self.evaluateWithExecution(&execution, values);
    }

    pub fn evaluateWithExecution(self: *const Plan, execution: *Execution, values: []const Value) !Value {
        return self.evaluateNode(execution, .{ .values = values }, @intCast(self.nodes.len - 1)) catch |err| return executionFailure(err);
    }

    pub fn evaluateJson(self: *const Plan, alloc: Allocator, document: std.json.Value) !Value {
        var budget: usize = max_allocated_bytes;
        return self.evaluateJsonWithBudget(alloc, document, &budget);
    }

    pub fn evaluateJsonWithBudget(self: *const Plan, alloc: Allocator, document: std.json.Value, budget: *usize) !Value {
        var execution = Execution.init(alloc, budget);
        return self.evaluateJsonWithExecution(&execution, document);
    }

    pub fn evaluateJsonWithExecution(self: *const Plan, execution: *Execution, document: std.json.Value) !Value {
        if (document != .object) return error.InvalidRelationalExpressionInput;
        return self.evaluateNode(execution, .{ .json = document }, @intCast(self.nodes.len - 1)) catch |err| return executionFailure(err);
    }

    pub fn evaluateRow(self: *const Plan, alloc: Allocator, row: codec.OrdinalRowView) !Value {
        var budget: usize = max_allocated_bytes;
        return self.evaluateRowWithBudget(alloc, row, &budget);
    }

    pub fn evaluateRowWithBudget(self: *const Plan, alloc: Allocator, row: codec.OrdinalRowView, budget: *usize) !Value {
        var execution = Execution.init(alloc, budget);
        return self.evaluateRowWithExecution(&execution, row);
    }

    pub fn evaluateRowWithExecution(self: *const Plan, execution: *Execution, row: codec.OrdinalRowView) !Value {
        return self.evaluateNode(execution, .{ .row = row }, @intCast(self.nodes.len - 1)) catch |err| return executionFailure(err);
    }

    /// Caller must fence the exact immutable source layout used to compile this
    /// plan. TuplePlan establishes that proof once per scan, including history.
    pub fn evaluateBoundRowWithBudget(self: *const Plan, alloc: Allocator, row: codec.OrdinalRowView, budget: *usize) !Value {
        var execution = Execution.init(alloc, budget);
        return self.evaluateBoundRowWithExecution(&execution, row);
    }

    pub fn evaluateBoundRowWithExecution(self: *const Plan, execution: *Execution, row: codec.OrdinalRowView) !Value {
        return self.evaluateNode(execution, .{ .bound_row = row }, @intCast(self.nodes.len - 1)) catch |err| return executionFailure(err);
    }

    const Source = union(enum) { values: []const Value, json: std.json.Value, row: codec.OrdinalRowView, bound_row: codec.OrdinalRowView };

    fn evaluateNode(self: *const Plan, execution: *Execution, source: Source, index: u16) anyerror!Value {
        try execution.numeric.charge(1);
        const alloc = execution.alloc;
        const budget = execution.bytes;
        const node = self.nodes[index];
        if (node.op == .literal) return node.literal;
        if (node.op == .column) {
            const value: Value = switch (source) {
                .values => |values| if (node.ordinal < values.len) values[node.ordinal] else return error.InvalidRelationalExpressionInput,
                .json => |json| blk: {
                    const input = json.object.get(node.column_name) orelse .null;
                    if (node.kind == .blob and input == .string) {
                        const size = std.base64.standard.Decoder.calcSizeForSlice(input.string) catch return error.InvalidRelationalExpressionInput;
                        if (size > budget.*) return error.RelationalExpressionBudgetExceeded;
                        budget.* -= size;
                    }
                    break :blk if (node.kind == .numeric) try numericJson(execution, input) else try checks.valueFromJson(alloc, node.kind, input, false);
                },
                .row, .bound_row => |row| blk: {
                    const ordinal = if (source == .bound_row) node.ordinal else row.ordinalForName(node.column_name) orelse break :blk .null;
                    if (ordinal >= row.table_schema.relational_columns.len) return error.RelationalIndexColumnTypeMismatch;
                    if (row.table_schema.relational_columns[ordinal].column_type != node.kind) return error.RelationalIndexColumnTypeMismatch;
                    const cell = (try row.findCell(ordinal)) orelse break :blk .null;
                    if (cell.is_null) break :blk .null;
                    break :blk switch (node.kind) {
                        .string => .{ .string = cell.value.bytes_val },
                        .blob => .{ .blob = cell.value.bytes_val },
                        .integer => .{ .integer = cell.value.i64_val },
                        .number => .{ .number = cell.value.f64_val },
                        .numeric => .{ .numeric = cell.value.bytes_val },
                        .boolean => .{ .boolean = cell.value.bool_val },
                        .datetime => .{ .datetime = cell.value.u64_val },
                        else => return error.InvalidRelationalExpressionType,
                    };
                },
            };
            if (value != .null and !valueHasKind(value, node.kind)) return error.InvalidRelationalExpressionInput;
            if (value == .number and !std.math.isFinite(value.number)) return error.InvalidRelationalExpressionInput;
            return value;
        }
        if (node.op == .case_when) {
            var i: usize = 0;
            while (i + 1 < node.children.len) : (i += 2) {
                const condition = try self.evaluateNode(execution, source, node.children[i]);
                if (condition != .null and condition.boolean)
                    return self.evaluateNode(execution, source, node.children[i + 1]);
            }
            return self.evaluateNode(execution, source, node.children[node.children.len - 1]);
        }
        if (node.op == .coalesce) {
            for (node.children) |child| {
                const value = try self.evaluateNode(execution, source, child);
                if (value != .null) return value;
            }
            return .null;
        }
        if (node.op == .@"and" or node.op == .@"or") {
            var unknown = false;
            for (node.children) |child| {
                const value = try self.evaluateNode(execution, source, child);
                if (value == .null) {
                    unknown = true;
                    continue;
                }
                if (value.boolean == (node.op == .@"or")) return value;
            }
            return if (unknown) .null else .{ .boolean = node.op == .@"and" };
        }
        if (node.op == .is_null or node.op == .is_not_null) {
            const value = try self.evaluateNode(execution, source, node.children[0]);
            return .{ .boolean = (value == .null) == (node.op == .is_null) };
        }
        if (node.op == .in_list or node.op == .not_in_list) {
            // The probe is evaluated once. NULL candidates do not terminate
            // the search: a later equality wins over UNKNOWN.
            const probe = try self.evaluateNode(execution, source, node.children[0]);
            var unknown = probe == .null;
            for (node.children[1..]) |child| {
                const candidate = try self.evaluateNode(execution, source, child);
                if (probe == .null or candidate == .null) {
                    unknown = true;
                    continue;
                }
                const bytes: usize = switch (probe) {
                    .string => |value| @min(value.len, candidate.string.len),
                    .blob => |value| @min(value.len, candidate.blob.len),
                    else => 0,
                };
                if (bytes > budget.* / 2) return error.RelationalExpressionBudgetExceeded;
                budget.* -= bytes * 2;
                if (try valueOrderWithContext(probe, candidate, false, &execution.numeric) == .eq) return .{ .boolean = node.op == .in_list };
            }
            return if (unknown) .null else .{ .boolean = node.op == .not_in_list };
        }
        if (isComparison(node.op)) {
            const left = try self.evaluateNode(execution, source, node.children[0]);
            const right = try self.evaluateNode(execution, source, node.children[1]);
            if (left == .null or right == .null) return switch (node.op) {
                .is_distinct => .{ .boolean = (left == .null) != (right == .null) },
                .is_not_distinct => .{ .boolean = (left == .null) == (right == .null) },
                else => .null,
            };
            // Borrowed values need no allocation, but repeatedly comparing a
            // wide value still consumes CPU. Charge the maximum operand bytes
            // inspected against the same per-row budget as allocated outputs.
            const compared_bytes: usize = switch (left) {
                .string => |bytes| @min(bytes.len, right.string.len),
                .blob => |bytes| @min(bytes.len, right.blob.len),
                else => 0,
            };
            if (compared_bytes > budget.* / 2) return error.RelationalExpressionBudgetExceeded;
            budget.* -= compared_bytes * 2;
            const order = try valueOrderWithContext(left, right, node.fold_ascii, &execution.numeric);
            return .{ .boolean = switch (node.op) {
                .eq, .is_not_distinct => order == .eq,
                .ne, .is_distinct => order != .eq,
                .gt => order == .gt,
                .gte => order != .lt,
                .lt => order == .lt,
                .lte => order != .gt,
                else => unreachable,
            } };
        }
        var operands: [32]Value = undefined;
        for (node.children, 0..) |child, i| {
            operands[i] = try self.evaluateNode(execution, source, child);
            if (operands[i] == .null) return .null;
        }
        const a = operands[0];
        if (a == .numeric or node.kind == .numeric) return numericOperation(execution, node, operands[0..node.children.len], self.nodes[node.children[0]]);
        switch (node.op) {
            .not => return .{ .boolean = !a.boolean },
            .cast => return numericCast(a, node.sql_type.?),
            .negate => return switch (a) {
                .integer => |v| numericCast(.{ .integer = std.math.sub(i64, 0, v) catch return error.RelationalExpressionOverflow }, node.sql_type orelse .int64),
                .number => |v| numericCast(.{ .number = -v }, node.sql_type orelse .float64),
                else => unreachable,
            },
            .add, .subtract, .multiply, .divide, .modulo => {
                const b = operands[1];
                if (node.kind == .integer) {
                    const result = switch (node.op) {
                        .add => std.math.add(i64, a.integer, b.integer),
                        .subtract => std.math.sub(i64, a.integer, b.integer),
                        .multiply => std.math.mul(i64, a.integer, b.integer),
                        .divide => blk: {
                            if (b.integer == 0) return error.RelationalExpressionDivisionByZero;
                            if (a.integer == std.math.minInt(i64) and b.integer == -1) return error.RelationalExpressionOverflow;
                            break :blk @divTrunc(a.integer, b.integer);
                        },
                        .modulo => blk: {
                            if (b.integer == 0) return error.RelationalExpressionDivisionByZero;
                            // Unlike division, minInt % -1 is exactly zero.
                            break :blk if (b.integer == -1) 0 else @rem(a.integer, b.integer);
                        },
                        else => unreachable,
                    } catch return error.RelationalExpressionOverflow;
                    return numericCast(.{ .integer = result }, node.sql_type orelse .int64);
                }
                if (node.op == .divide and b.number == 0) return error.RelationalExpressionDivisionByZero;
                const result = switch (node.op) {
                    .add => a.number + b.number,
                    .subtract => a.number - b.number,
                    .multiply => a.number * b.number,
                    .divide => a.number / b.number,
                    else => unreachable,
                };
                if (node.sql_type == .float32) {
                    // A float4 operation rounds its result once, independently
                    // of subsequent promotion to a wider expression domain.
                    const rounded = try numericCast(.{ .number = result }, .float32);
                    if (rounded.number == 0 and a.number != 0 and b.number != 0 and (node.op == .multiply or node.op == .divide)) return error.RelationalExpressionOverflow;
                    return rounded;
                }
                return finite(result);
            },
            .concat => {
                var size: usize = 0;
                for (operands[0..node.children.len]) |value| size = std.math.add(usize, size, value.string.len) catch return error.RelationalExpressionBudgetExceeded;
                const output = try allocateOutput(alloc, size, budget);
                var offset: usize = 0;
                for (operands[0..node.children.len]) |value| {
                    @memcpy(output[offset..][0..value.string.len], value.string);
                    offset += value.string.len;
                }
                return .{ .string = output };
            },
            .lower_ascii, .upper_ascii => {
                const output = try allocateOutput(alloc, a.string.len, budget);
                for (a.string, output) |byte, *out| out.* = if (node.op == .lower_ascii) std.ascii.toLower(byte) else std.ascii.toUpper(byte);
                return .{ .string = output };
            },
            else => unreachable,
        }
    }
};

fn numericOperand(ctx: *exact.Context, value: Value, real: bool) !exact.Value {
    return switch (value) {
        .numeric => |bytes| (try binary.decodeCanonical(ctx, bytes)).value,
        .integer => |integer| (try @import("../sql/numeric_storage.zig").fromJson(ctx, .{ .integer = integer })).value,
        .number => |number| (try exact.fromFloat(ctx, number, real)).value,
        else => error.InvalidRelationalExpressionInput,
    };
}

fn numericOperation(execution: *Execution, node: Node, operands: []const Value, source: Node) !Value {
    const input = operands[0];
    if (node.op == .cast and input == .numeric) if (node.numeric_modifier) |modifier|
        return normalizeNumericBinding(execution, input.numeric, modifier, false);
    if (input == .numeric and (node.op == .negate or (node.op == .cast and node.sql_type == .numeric))) {
        const View = @import("../common/sql_numeric_layout.zig").View;
        const view = try View.openWithBudget(input.numeric, .{}, &execution.numeric);
        if (node.op == .cast or view.kind == .nan or (view.kind == .finite and view.count == 0)) return input;
        try execution.numeric.charge(input.numeric.len);
        const bytes = try allocateOutput(execution.alloc, input.numeric.len, execution.bytes);
        @memcpy(bytes, input.numeric);
        const sign: u16 = switch (view.kind) {
            .finite => if (view.negative) 0 else 0x4000,
            .positive_infinity => 0xf000,
            .negative_infinity => 0xd000,
            .nan => unreachable,
        };
        std.mem.writeInt(u16, bytes[4..6], sign, .big);
        return .{ .numeric = bytes };
    }
    var scratch: NumericScratch = undefined;
    scratch.init(execution);
    defer scratch.deinit();
    return numericOperationInner(&scratch, node, operands, source) catch |err| return scratch.failure(err);
}

fn numericOperationInner(scratch: *NumericScratch, node: Node, operands: []const Value, source: Node) !Value {
    const ctx = &scratch.execution.numeric;
    const a = try numericOperand(ctx, operands[0], source.sql_type == .float32);
    if (node.op == .cast) {
        const target = node.sql_type.?;
        if (target == .numeric) {
            if (node.numeric_modifier) |modifier| {
                const constrained = try exact.applyTypeModifier(ctx, a, modifier);
                return scratch.encode(constrained.value);
            }
            return scratch.encode(a);
        }
        if (casts.integral(target)) {
            const integer = try exact.toInteger(i64, ctx, a);
            return numericCast(.{ .integer = integer }, target);
        }
        const text = try exact.format(ctx, a);
        return finite(if (target == .float32)
            try casts.floatValue(f32, .{ .number_string = text })
        else if (target == .float64)
            try casts.floatValue(f64, .{ .number_string = text })
        else
            return error.InvalidRelationalExpressionType);
    }
    const b = try numericOperand(ctx, operands[1], false);
    const result = try switch (node.op) {
        .add => exact.add(ctx, a, b),
        .subtract => exact.subtract(ctx, a, b),
        .multiply => exact.multiply(ctx, a, b),
        .divide => exact.divide(ctx, a, b),
        .modulo => exact.remainder(ctx, a, b),
        else => return error.InvalidRelationalExpressionType,
    };
    return scratch.encode(result.value);
}

fn numericCast(value: Value, target: Numeric) !Value {
    if (value == .null) return .null;
    if (casts.integral(target)) return .{ .integer = switch (value) {
        .integer => |v| casts.checkedInteger(v, target) catch return error.RelationalExpressionOverflow,
        .number => |v| casts.floatingInteger(v, target) catch return error.RelationalExpressionOverflow,
        else => return error.InvalidRelationalExpressionInput,
    } };
    const raw: std.json.Value = switch (value) {
        .integer => |v| .{ .integer = v },
        .number => |v| .{ .float = v },
        else => return error.InvalidRelationalExpressionInput,
    };
    return if (target == .float32)
        .{ .number = casts.floatValue(f32, raw) catch return error.RelationalExpressionOverflow }
    else if (target == .float64)
        finite(casts.floatValue(f64, raw) catch return error.RelationalExpressionOverflow)
    else
        error.InvalidRelationalExpressionType;
}

fn allocateOutput(alloc: Allocator, size: usize, budget: *usize) ![]u8 {
    if (size > max_output_bytes or size > budget.*) return error.RelationalExpressionBudgetExceeded;
    budget.* -= size;
    return alloc.alloc(u8, size);
}

fn isComparison(op: Op) bool {
    return switch (op) {
        .eq, .ne, .gt, .gte, .lt, .lte, .is_distinct, .is_not_distinct => true,
        else => false,
    };
}

fn valueOrder(a: Value, b: Value, fold_ascii: bool) !std.math.Order {
    var context: exact.Context = .{ .alloc = std.heap.page_allocator };
    return valueOrderWithContext(a, b, fold_ascii, &context);
}

fn valueOrderWithContext(a: Value, b: Value, fold_ascii: bool, context: *exact.Context) !std.math.Order {
    return switch (a) {
        .null => unreachable,
        .string => |left| blk: {
            if (!fold_ascii) break :blk std.mem.order(u8, left, b.string);
            for (left[0..@min(left.len, b.string.len)], b.string[0..@min(left.len, b.string.len)]) |x, y| {
                const order = std.math.order(std.ascii.toLower(x), std.ascii.toLower(y));
                if (order != .eq) break :blk order;
            }
            break :blk std.math.order(left.len, b.string.len);
        },
        .blob => |value| std.mem.order(u8, value, b.blob),
        .numeric => |bytes| blk: {
            const View = @import("../common/sql_numeric_layout.zig").View;
            const left = try View.openWithBudget(bytes, .{}, context);
            const right = try View.openWithBudget(b.numeric, .{}, context);
            break :blk try left.order(right, context);
        },
        .boolean => |value| std.math.order(@intFromBool(value), @intFromBool(b.boolean)),
        .integer => |value| std.math.order(value, b.integer),
        .number => |value| std.math.order(value, b.number),
        .datetime => |value| std.math.order(value, b.datetime),
    };
}

fn finite(value: f64) !Value {
    if (!std.math.isFinite(value)) return error.RelationalExpressionOverflow;
    return .{ .number = if (value == 0) 0 else value };
}

fn valueHasKind(value: Value, kind: Kind) bool {
    return switch (value) {
        .null => true,
        inline else => |_, tag| std.mem.eql(u8, @tagName(tag), @tagName(kind)),
    };
}

const Compiler = struct {
    alloc: Allocator,
    execution: *Execution,
    table: schema.TableSchema,
    nodes: std.ArrayList(Node) = .empty,
    dependencies: std.ArrayList(u32) = .empty,
    hash: std.crypto.hash.Blake3 = std.crypto.hash.Blake3.init(.{}),
    visited: usize = 0,
    literal_bytes: usize = 0,

    fn frame(self: *Compiler, bytes: []const u8) void {
        var size: [8]u8 = undefined;
        std.mem.writeInt(u64, &size, bytes.len, .little);
        self.hash.update(&size);
        self.hash.update(bytes);
    }

    fn compile(self: *Compiler, input: std.json.Value, depth: usize) anyerror!u16 {
        self.visited += 1;
        if (depth >= max_depth or self.visited > max_nodes) return error.RelationalExpressionBudgetExceeded;
        if (input != .object) return error.InvalidRelationalExpression;
        const op_value = input.object.get("op") orelse return error.InvalidRelationalExpression;
        if (op_value != .string) return error.InvalidRelationalExpression;
        const op = std.meta.stringToEnum(Op, op_value.string) orelse return error.InvalidRelationalExpression;
        var fields = input.object.iterator();
        while (fields.next()) |field| {
            const name = field.key_ptr.*;
            if (std.mem.eql(u8, name, "op")) continue;
            const allowed = switch (op) {
                .literal => std.mem.eql(u8, name, "type") or std.mem.eql(u8, name, "value") or std.mem.eql(u8, name, "sql_type"),
                .column => std.mem.eql(u8, name, "column"),
                .cast => std.mem.eql(u8, name, "args") or std.mem.eql(u8, name, "type") or std.mem.eql(u8, name, "sql_type") or std.mem.eql(u8, name, "numeric_modifier"),
                .add, .subtract, .multiply, .divide, .modulo, .negate => std.mem.eql(u8, name, "args") or std.mem.eql(u8, name, "sql_type"),
                else => std.mem.eql(u8, name, "args") or (isComparison(op) and std.mem.eql(u8, name, "collation")),
            };
            if (!allowed) return error.InvalidRelationalExpression;
        }
        self.frame(op_value.string);
        var node: Node = .{ .op = op, .kind = undefined };
        switch (op) {
            .literal => {
                const kind = input.object.get("type") orelse return error.InvalidRelationalExpression;
                if (kind != .string) return error.InvalidRelationalExpression;
                node.kind = std.meta.stringToEnum(Kind, kind.string) orelse return error.InvalidRelationalExpression;
                const value = input.object.get("value") orelse .null;
                switch (node.kind) {
                    .string, .blob, .boolean, .datetime, .integer, .number, .numeric => {},
                    .json => if (value != .null) return error.InvalidRelationalExpressionType,
                    else => return error.InvalidRelationalExpressionType,
                }
                if (node.kind == .blob and value == .string and value.string.len > std.base64.standard.Encoder.calcSize(max_output_bytes)) return error.RelationalExpressionBudgetExceeded;
                node.literal = (if (node.kind == .numeric) numericJson(self.execution, value) else checks.valueFromJson(self.alloc, node.kind, value, true)) catch |err| switch (err) {
                    error.OutOfMemory => return err,
                    error.SqlProgramLimitExceeded, error.RelationalExpressionBudgetExceeded => return err,
                    else => return error.InvalidRelationalExpressionType,
                };
                switch (node.literal) {
                    .string => |bytes| {
                        if (bytes.len > max_output_bytes) return error.RelationalExpressionBudgetExceeded;
                        node.literal = .{ .string = try self.alloc.dupe(u8, bytes) };
                    },
                    .blob => |bytes| if (bytes.len > max_output_bytes) return error.RelationalExpressionBudgetExceeded,
                    .number => |value_number| node.literal = try finite(value_number),
                    else => {},
                }
                self.frame(@tagName(node.kind));
                self.literal_bytes += switch (node.literal) {
                    .string, .blob, .numeric => |bytes| bytes.len,
                    .datetime => 16,
                    else => 8,
                };
                if (self.literal_bytes > max_allocated_bytes) return error.RelationalExpressionBudgetExceeded;
                self.frame(@tagName(node.literal));
                var bytes: [8]u8 = undefined;
                switch (node.literal) {
                    .null => {},
                    .string, .blob, .numeric => |value_bytes| self.frame(value_bytes),
                    .boolean => |boolean| self.hash.update(&.{@intFromBool(boolean)}),
                    .integer => |integer| {
                        std.mem.writeInt(i64, &bytes, integer, .little);
                        self.hash.update(&bytes);
                    },
                    .datetime => |datetime| {
                        var signed: [16]u8 = undefined;
                        std.mem.writeInt(i128, &signed, datetime, .little);
                        self.hash.update(&signed);
                    },
                    .number => |number| {
                        std.mem.writeInt(u64, &bytes, @bitCast(number), .little);
                        self.hash.update(&bytes);
                    },
                }
            },
            .column => {
                const name = input.object.get("column") orelse return error.InvalidRelationalExpression;
                if (name != .string) return error.InvalidRelationalExpression;
                node.column_name = try self.alloc.dupe(u8, name.string);
                node.ordinal = for (self.table.relational_columns, 0..) |column, ordinal| {
                    if (std.mem.eql(u8, column.name, name.string)) break @intCast(ordinal);
                } else return error.RelationalIndexColumnNotFound;
                node.kind = self.table.relational_columns[node.ordinal].column_type;
                node.sql_type = self.table.relational_columns[node.ordinal].sql_element_type;
                if (node.kind == .numeric and node.sql_type != .numeric) return error.InvalidRelationalExpressionType;
                switch (node.kind) {
                    .string, .blob, .boolean, .datetime, .integer, .number, .numeric => {},
                    else => return error.InvalidRelationalExpressionType,
                }
                if (std.mem.indexOfScalar(u32, self.dependencies.items, node.ordinal) == null) try self.dependencies.append(self.alloc, node.ordinal);
                self.frame(name.string);
                self.frame(@tagName(node.kind));
                if (self.table.relational_columns[node.ordinal].sql_element_type) |kind| {
                    self.frame("precise SQL builtin type");
                    self.frame(@tagName(kind));
                }
                if (self.table.relational_columns[node.ordinal].numeric_modifier) |modifier| {
                    self.frame("SQL NUMERIC column modifier v1");
                    var identity: [4]u8 = undefined;
                    std.mem.writeInt(u16, identity[0..2], modifier.precision, .little);
                    std.mem.writeInt(i16, identity[2..4], modifier.scale, .little);
                    self.frame(&identity);
                }
            },
            else => {
                const args = input.object.get("args") orelse return error.InvalidRelationalExpression;
                if (args != .array) return error.InvalidRelationalExpression;
                const length = args.array.items.len;
                const valid = switch (op) {
                    .negate, .lower_ascii, .upper_ascii, .not, .is_null, .is_not_null, .cast => length == 1,
                    .concat, .coalesce, .@"and", .@"or" => length >= 2 and length <= 32,
                    .in_list, .not_in_list => length >= 2 and length <= 32,
                    .case_when => length >= 3 and length <= 31 and length % 2 == 1,
                    else => length == 2,
                };
                if (!valid) return error.InvalidRelationalExpression;
                self.hash.update(&.{@intCast(length)});
                const children = try self.alloc.alloc(u16, length);
                for (args.array.items, children) |arg, *child| child.* = try self.compile(arg, depth + 1);
                node.children = children;
                node.kind = self.nodes.items[children[if (op == .case_when) children.len - 1 else 0]].kind;
                if (op == .case_when) {
                    for (children[0 .. children.len - 1], 0..) |child, i| {
                        if (self.nodes.items[child].kind != (if (i % 2 == 0) Kind.boolean else node.kind)) return error.InvalidRelationalExpressionType;
                    }
                } else for (children[1..]) |child| if (self.nodes.items[child].kind != node.kind) return error.InvalidRelationalExpressionType;
                if (isComparison(op)) {
                    if (input.object.get("collation")) |collation| {
                        if (collation != .string or node.kind != .string) return error.UnsupportedRelationalIndexCollation;
                        if (std.ascii.eqlIgnoreCase(collation.string, "ci") or std.ascii.eqlIgnoreCase(collation.string, "case_insensitive") or std.ascii.eqlIgnoreCase(collation.string, "antfly.case_insensitive")) {
                            node.fold_ascii = true;
                        } else if (!std.ascii.eqlIgnoreCase(collation.string, "C") and !std.ascii.eqlIgnoreCase(collation.string, "POSIX") and !std.ascii.eqlIgnoreCase(collation.string, "binary")) return error.UnsupportedRelationalIndexCollation;
                    }
                    self.hash.update(&.{@intFromBool(node.fold_ascii)});
                    node.kind = .boolean;
                }
                switch (op) {
                    .cast => {
                        if (node.kind != .integer and node.kind != .number and node.kind != .numeric) return error.InvalidRelationalExpressionType;
                        const kind = input.object.get("type") orelse return error.InvalidRelationalExpression;
                        if (kind != .string) return error.InvalidRelationalExpression;
                        node.kind = std.meta.stringToEnum(Kind, kind.string) orelse return error.InvalidRelationalExpressionType;
                        if (input.object.get("sql_type") == null) return error.InvalidRelationalExpression;
                    },
                    .add, .subtract, .multiply, .divide, .negate => if (node.kind != .integer and node.kind != .number and node.kind != .numeric) return error.InvalidRelationalExpressionType,
                    // PostgreSQL has no float4/float8 remainder operator.
                    .modulo => if (node.kind != .integer and node.kind != .numeric) return error.InvalidRelationalExpressionType,
                    .concat, .lower_ascii, .upper_ascii => if (node.kind != .string) return error.InvalidRelationalExpressionType,
                    .coalesce, .case_when => {
                        if (node.kind == .integer or node.kind == .number or node.kind == .numeric) {
                            const identity = numericIdentity(self.nodes.items[children[children.len - 1]]);
                            const same = for (children, 0..) |child, i| {
                                if (op == .case_when and i % 2 == 0 and i + 1 < children.len) continue;
                                if (numericIdentity(self.nodes.items[child]) != identity) break false;
                            } else true;
                            if (same) node.sql_type = identity;
                        }
                    },
                    .@"and", .@"or", .not => if (node.kind != .boolean) return error.InvalidRelationalExpressionType,
                    .is_null, .is_not_null => node.kind = .boolean,
                    .in_list, .not_in_list => node.kind = .boolean,
                    .eq, .ne, .gt, .gte, .lt, .lte, .is_distinct, .is_not_distinct => {},
                    else => unreachable,
                }
            },
        }
        if (input.object.get("sql_type")) |identity| {
            if (identity != .string) return error.InvalidRelationalExpressionType;
            const typed = std.meta.stringToEnum(Numeric, identity.string) orelse return error.InvalidRelationalExpressionType;
            if ((node.kind != .integer or !casts.integral(typed)) and (node.kind != .number or !casts.floating(typed)) and (node.kind != .numeric or typed != .numeric)) return error.InvalidRelationalExpressionType;
            node.sql_type = typed;
            if (op == .literal and node.kind != .numeric) node.literal = numericCast(node.literal, typed) catch return error.InvalidRelationalExpressionType;
            // Do not change fingerprints of historical unannotated programs.
            // Typed operations/casts have a distinct, immutable semantic key.
            self.frame("SQL numeric expression identity v1");
            self.frame(@tagName(typed));
            if (op == .add or op == .subtract or op == .multiply or op == .divide or op == .modulo or op == .negate) {
                for (node.children) |child| if (numericIdentity(self.nodes.items[child]) != typed) return error.InvalidRelationalExpressionType;
            }
        }
        if (input.object.get("numeric_modifier")) |constraint| {
            if (node.op != .cast or node.kind != .numeric or node.sql_type != .numeric) return error.InvalidRelationalExpressionType;
            const modifier = @import("../sql/numeric_storage.zig").modifierFromJson(constraint) catch return error.InvalidRelationalExpressionType;
            node.numeric_modifier = modifier;
            self.frame("SQL NUMERIC cast modifier v1");
            var identity: [4]u8 = undefined;
            std.mem.writeInt(u16, identity[0..2], modifier.precision, .little);
            std.mem.writeInt(i16, identity[2..4], modifier.scale, .little);
            self.frame(&identity);
        }
        const index: u16 = @intCast(self.nodes.items.len);
        try self.nodes.append(self.alloc, node);
        return index;
    }
};

fn numericIdentity(node: Node) Numeric {
    return node.sql_type orelse if (node.kind == .integer) .int64 else if (node.kind == .numeric) .numeric else .float64;
}

test "relational declarations exact NUMERIC constraints own literals and bound repeated row scratch" {
    const constraints = @import("numeric_constraints.zig");
    const a = std.testing.allocator;
    const Run = struct {
        fn run(alloc: Allocator) !void {
            const plan = blk: {
                var json = try std.json.parseFromSlice(std.json.Value, alloc, "{\"minimum\":9007199254740993.25,\"exclusiveMaximum\":9007199254740993.26,\"multipleOf\":0.0001,\"enum\":[9007199254740993.2500,9007199254740993.25,\"9007199254740993.25\"]}", .{ .parse_numbers = false });
                defer json.deinit();
                break :blk (try constraints.Plan.create(alloc, json.value.object)).?;
            };
            defer plan.deinit();
            try std.testing.expectEqual(@as(usize, 1), plan.enumeration.count());
            var execution: constraints.Execution = undefined;
            execution.init(alloc);
            defer execution.deinit();
            try execution.validateJson(plan, .{ .number_string = "9007199254740993.250000" });
            execution.validateJson(plan, .{ .number_string = "9007199254740993.2499" }) catch |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqual(error.InvalidBatchRequest, err);
                return;
            };
            return error.TestExpectedError;
        }
    };
    try Run.run(a);
    try std.testing.checkAllAllocationFailures(a, Run.run, .{});

    var json = try std.json.parseFromSlice(std.json.Value, a, "{\"minimum\":1e-1000,\"maximum\":1e1000,\"multipleOf\":1e-1000}", .{ .parse_numbers = false });
    defer json.deinit();
    const plan = (try constraints.Plan.create(a, json.value.object)).?;
    defer plan.deinit();
    var execution: constraints.Execution = undefined;
    execution.init(a);
    defer execution.deinit();
    try execution.validateJson(plan, .{ .number_string = "1e-999" });
    const peak = execution.memory.peak;
    for (0..100) |_| try execution.validateJson(plan, .{ .number_string = "1e-999" });
    try std.testing.expectEqual(peak, execution.memory.peak);
    try std.testing.expectError(error.InvalidBatchRequest, execution.validateJson(plan, .{ .number_string = "1e-1001" }));
    execution.context.remaining = 0;
    try std.testing.expectError(error.SqlProgramLimitExceeded, execution.validateJson(plan, .{ .integer = 1 }));
    execution.context.remaining = 10000;
    try std.testing.expectError(error.SqlProgramLimitExceeded, execution.validateJson(plan, .{ .integer = 1 }));
}

test "relational declarations exact NUMERIC constraint predicates match PostgreSQL" {
    const constraints = @import("numeric_constraints.zig");
    const a = std.testing.allocator;
    const Case = struct { schema: []const u8, value: []const u8, expected: bool };
    const Fixture = struct { reference: []const u8, entries: []const Case };
    var fixture = try std.json.parseFromSlice(Fixture, a, @embedFile("../sql/fixtures/sql_numeric_constraints_reference.json"), .{});
    defer fixture.deinit();
    for (fixture.value.entries) |case| {
        var definition = try std.json.parseFromSlice(std.json.Value, a, case.schema, .{ .parse_numbers = false });
        defer definition.deinit();
        const plan = (try constraints.Plan.create(a, definition.value.object)).?;
        defer plan.deinit();
        var execution: constraints.Execution = undefined;
        execution.init(a);
        defer execution.deinit();
        execution.validateJson(plan, .{ .number_string = case.value }) catch |err| {
            if (err != error.InvalidBatchRequest or case.expected) {
                std.debug.print("NUMERIC constraint: schema={s} value={s} expected={} error={s}\n", .{ case.schema, case.value, case.expected, @errorName(err) });
                return err;
            }
            continue;
        };
        try std.testing.expect(case.expected);
    }
}

test "relational declarations NUMERIC constraints borrow enum comparisons and preserve cancellation admission" {
    const constraints = @import("numeric_constraints.zig");
    const a = std.testing.allocator;
    var definition = try std.json.parseFromSlice(std.json.Value, a, "{\"minimum\":9007199254740993.25,\"enum\":[9007199254740993.25000,\"NaN\"]}", .{ .parse_numbers = false });
    defer definition.deinit();
    const plan = (try constraints.Plan.create(a, definition.value.object)).?;
    defer plan.deinit();
    var parse_context: exact.Context = .{ .alloc = a };
    var parsed = try exact.parse(&parse_context, "9007199254740993.25000000");
    defer parsed.deinit();
    var borrowed: exact.Context = .{ .alloc = std.testing.failing_allocator };
    for (0..10000) |_| try plan.validate(&borrowed, parsed.value);

    var execution: constraints.Execution = undefined;
    execution.init(a);
    defer execution.deinit();
    const Cancel = struct {
        fn check(_: ?*anyopaque) anyerror!void {
            return error.Canceled;
        }
    };
    execution.context.checkpoint = Cancel.check;
    try std.testing.expectError(error.Canceled, execution.validateJson(plan, .{ .integer = 1 }));
    execution.context.checkpoint = null;
    try std.testing.expectError(error.Canceled, execution.validateJson(plan, .{ .integer = 1 }));

    var limited: constraints.Execution = undefined;
    limited.init(a);
    defer limited.deinit();
    limited.memory.limit = 0;
    try std.testing.expectError(error.SqlProgramLimitExceeded, limited.validateJson(plan, .{ .integer = 1 }));
    limited.memory.limit = constraints.max_bytes;
    try std.testing.expectError(error.SqlProgramLimitExceeded, limited.validateJson(plan, .{ .integer = 1 }));
    for ([_][]const u8{ "{\"multipleOf\":0}", "{\"multipleOf\":-0.001}", "{\"minimum\":\"1\"}", "{\"minimum\":1e131072}" }) |invalid| {
        var json = try std.json.parseFromSlice(std.json.Value, a, invalid, .{ .parse_numbers = false });
        defer json.deinit();
        try std.testing.expectError(error.InvalidSchemaUpdateRequest, constraints.Plan.create(a, json.value.object));
    }
}

test "relational declarations exact NUMERIC SQL lowering and native programs match PostgreSQL" {
    const a = std.testing.allocator;
    const Case = struct {
        op: []const u8,
        left: []const u8,
        right: ?[]const u8 = null,
        expected: ?std.json.Value = null,
        @"error": ?[]const u8 = null,
    };
    const fixture = try std.json.parseFromSlice(struct { entries: []const Case }, a, @embedFile("../sql/fixtures/sql_exact_numeric_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    const table: schema.TableSchema = .{ .storage_mode = .relational };
    var exercised: usize = 0;
    for (fixture.value.entries) |entry| {
        const op: []const u8 = if (std.mem.eql(u8, entry.op, "remainder")) "modulo" else entry.op;
        const integer_cast = std.mem.eql(u8, op, "int16") or std.mem.eql(u8, op, "int32") or std.mem.eql(u8, op, "int64");
        const ordering = std.mem.eql(u8, op, "order");
        if (!std.mem.eql(u8, op, "add") and !std.mem.eql(u8, op, "subtract") and
            !std.mem.eql(u8, op, "multiply") and !std.mem.eql(u8, op, "divide") and
            !std.mem.eql(u8, op, "modulo") and !integer_cast and !ordering) continue;
        const Literal = struct { op: []const u8 = "literal", type: []const u8 = "numeric", value: []const u8 };
        const text = if (integer_cast) try std.json.Stringify.valueAlloc(a, .{
            .op = "cast",
            .type = "integer",
            .sql_type = op,
            .args = [1]Literal{.{ .value = entry.left }},
        }, .{}) else try std.json.Stringify.valueAlloc(a, .{
            .op = if (ordering) "lt" else op,
            .args = [2]Literal{ .{ .value = entry.left }, .{ .value = entry.right.? } },
        }, .{});
        defer a.free(text);
        const parsed = try std.json.parseFromSlice(std.json.Value, a, text, .{});
        defer parsed.deinit();
        var plan = try Plan.init(a, table, parsed.value, if (integer_cast) .integer else if (ordering) .boolean else .numeric);
        defer plan.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const owned = arena.allocator();
        const token: []const u8 = if (std.mem.eql(u8, op, "add")) "+" else if (std.mem.eql(u8, op, "subtract")) "-" else if (std.mem.eql(u8, op, "multiply")) "*" else if (std.mem.eql(u8, op, "divide")) "/" else if (ordering) "<" else "%";
        const sql = if (integer_cast)
            try std.fmt.allocPrint(owned, "CAST(CAST('{s}' AS numeric) AS {s})", .{ entry.left, if (std.mem.eql(u8, op, "int16")) "smallint" else if (std.mem.eql(u8, op, "int32")) "integer" else "bigint" })
        else
            try std.fmt.allocPrint(owned, "CAST('{s}' AS numeric) {s} CAST('{s}' AS numeric)", .{ entry.left, token, entry.right.? });
        var compiled = try @import("../sql/compiler.zig").compileScalar(owned, sql, .{});
        defer compiled.deinit();
        const lowered = try @import("../sql/schema_expression.zig").lowerColumns(owned, &.{}, compiled.expression, null);
        var sql_plan = try Plan.init(a, table, lowered.expression, plan.result_kind);
        defer sql_plan.deinit();
        for ([_]*Plan{ &plan, &sql_plan }) |candidate| {
            if (entry.@"error") |state| {
                const failure: anyerror = if (std.mem.eql(u8, state, "22012")) error.RelationalExpressionDivisionByZero else if (std.mem.eql(u8, state, "22003")) error.RelationalExpressionOverflow else if (std.mem.eql(u8, state, "0A000")) error.SqlFeatureNotSupported else return error.TestUnexpectedSqlstate;
                try std.testing.expectError(failure, candidate.evaluate(owned, &.{}));
            } else {
                const result = try candidate.evaluate(owned, &.{});
                if (integer_cast) {
                    const wanted = if (entry.expected.? == .integer) entry.expected.?.integer else try std.fmt.parseInt(i64, entry.expected.?.string, 10);
                    try std.testing.expectEqual(wanted, result.integer);
                } else if (ordering) {
                    var unavailable = std.heap.FixedBufferAllocator.init(&.{});
                    const borrowed = try candidate.evaluate(unavailable.allocator(), &.{});
                    try std.testing.expectEqual(entry.expected.?.integer < 0, borrowed.boolean);
                } else {
                    var context: exact.Context = .{ .alloc = arena.allocator() };
                    const value = try binary.decodeCanonical(&context, result.numeric);
                    const actual = try exact.format(&context, value.value);
                    try std.testing.expectEqualStrings(entry.expected.?.string, actual);
                }
            }
        }
        exercised += 1;
    }
    try std.testing.expectEqual(@as(usize, 537), exercised);
}

test "relational declarations NUMERIC modifier SQL lowering matches PostgreSQL and retains lazy overflow" {
    const a = std.testing.allocator;
    const Case = struct { sql: []const u8, expected: ?[]const u8 = null, @"error": ?[]const u8 = null };
    const fixture = try std.json.parseFromSlice(struct { entries: []const Case }, a, @embedFile("../sql/fixtures/sql_numeric_typmod_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    const Run = struct {
        fn evaluate(alloc: Allocator, sql: []const u8) !std.json.Value {
            var compiled = try @import("../sql/compiler.zig").compileScalar(alloc, sql, .{});
            defer compiled.deinit();
            const lowered = try @import("../sql/schema_expression.zig").lowerColumns(alloc, &.{}, compiled.expression, null);
            var plan = try Plan.init(alloc, .{ .storage_mode = .relational }, lowered.expression, .numeric);
            defer plan.deinit();
            const result = try plan.evaluate(alloc, &.{});
            if (result == .null) return .null;
            return @import("../sql/numeric_storage.zig").jsonValueAlloc(alloc, result.numeric);
        }
    };
    var tested: usize = 0;
    for (fixture.value.entries) |case| {
        // Arrays and these functions still lack a durable VM value/opcode.
        // Their SQL runtime coverage is not durable-expression activation.
        var unsupported = false;
        for ([_][]const u8{ "ARRAY", "[]", "GREATEST", "NULLIF", "abs(", "round(" }) |token| {
            if (std.mem.indexOf(u8, case.sql, token) != null) unsupported = true;
        }
        if (unsupported) continue;
        tested += 1;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const actual = Run.evaluate(arena.allocator(), case.sql) catch |err| {
            const state = case.@"error" orelse {
                std.debug.print("Durable NUMERIC modifier failed: {s}: {s}\n", .{ case.sql, @errorName(err) });
                return err;
            };
            try std.testing.expectEqualStrings(state, @import("../sql/errors.zig").describe(err).code);
            continue;
        };
        try std.testing.expect(case.@"error" == null);
        if (case.expected) |expected| {
            try std.testing.expectEqualStrings(expected, if (actual == .number_string) actual.number_string else actual.string);
        } else try std.testing.expect(actual == .null);
    }
    try std.testing.expectEqual(@as(usize, 42), tested);
}

test "relational declarations NUMERIC modifier casts bind fingerprints and reject malformed contracts" {
    const a = std.testing.allocator;
    var first: ?[32]u8 = null;
    for ([_][]const u8{ "{\"precision\":4,\"scale\":2}", "{\"precision\":4,\"scale\":1}" }) |modifier| {
        const text = try std.fmt.allocPrint(a, "{{\"op\":\"cast\",\"type\":\"numeric\",\"sql_type\":\"numeric\",\"numeric_modifier\":{s},\"args\":[{{\"op\":\"literal\",\"type\":\"numeric\",\"value\":\"1.245\"}}]}}", .{modifier});
        defer a.free(text);
        var parsed = try std.json.parseFromSlice(std.json.Value, a, text, .{});
        defer parsed.deinit();
        var plan = try Plan.init(a, .{ .storage_mode = .relational }, parsed.value, .numeric);
        defer plan.deinit();
        if (first) |fingerprint| try std.testing.expect(!std.mem.eql(u8, &fingerprint, &plan.fingerprint)) else first = plan.fingerprint;
    }
    for ([_][]const u8{
        "{\"op\":\"cast\",\"type\":\"integer\",\"sql_type\":\"int32\",\"numeric_modifier\":{\"precision\":4,\"scale\":2},\"args\":[{\"op\":\"literal\",\"type\":\"numeric\",\"value\":\"1\"}]}",
        "{\"op\":\"cast\",\"type\":\"numeric\",\"sql_type\":\"numeric\",\"numeric_modifier\":null,\"args\":[{\"op\":\"literal\",\"type\":\"numeric\",\"value\":\"1\"}]}",
    }) |text| {
        var parsed = try std.json.parseFromSlice(std.json.Value, a, text, .{});
        defer parsed.deinit();
        try std.testing.expectError(error.InvalidRelationalExpressionType, Plan.init(a, .{ .storage_mode = .relational }, parsed.value, if (parsed.value.object.get("type").?.string[0] == 'i') .integer else .numeric));
    }
}

test "relational declarations NUMERIC modifier casts own rounding scratch and reuse constrained bytes" {
    const a = std.testing.allocator;
    const Run = struct {
        fn run(alloc: Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            var compiled = try @import("../sql/compiler.zig").compileScalar(alloc, "CAST(CAST(1.245 AS numeric(4,2)) AS numeric(3,1))", .{});
            defer compiled.deinit();
            const lowered = try @import("../sql/schema_expression.zig").lowerColumns(arena.allocator(), &.{}, compiled.expression, null);
            var plan = try Plan.init(alloc, .{ .storage_mode = .relational }, lowered.expression, .numeric);
            defer plan.deinit();
            const value = try plan.evaluate(arena.allocator(), &.{});
            const text = try @import("../sql/numeric_storage.zig").jsonValueAlloc(arena.allocator(), value.numeric);
            try std.testing.expectEqualStrings("1.3", text.number_string);
        }
        fn canceled(_: ?*anyopaque) anyerror!void {
            return error.Canceled;
        }
    };
    try Run.run(a);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{});
    var parsed = try std.json.parseFromSlice(std.json.Value, a,
        \\{"op":"cast","type":"numeric","sql_type":"numeric","numeric_modifier":{"precision":4,"scale":2},"args":[{"op":"literal","type":"numeric","value":"1.25"}]}
    , .{});
    defer parsed.deinit();
    var plan = try Plan.init(a, .{ .storage_mode = .relational }, parsed.value, .numeric);
    defer plan.deinit();
    var unavailable = std.heap.FixedBufferAllocator.init(&.{});
    var bytes: usize = max_allocated_bytes;
    var execution = Execution.init(unavailable.allocator(), &bytes);
    const literal = plan.nodes[0].literal.numeric;
    const started = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..10000) |_| {
        const value = try plan.evaluateWithExecution(&execution, &.{});
        try std.testing.expectEqual(literal.ptr, value.numeric.ptr);
    }
    std.debug.print("NUMERIC modifier casts: rows=10000 allocated_bytes=0 elapsed_ns={}\n", .{std.Io.Clock.now(.awake, std.testing.io).nanoseconds - started});
    try std.testing.expectEqual(max_allocated_bytes, bytes);
    execution.numeric.remaining = 0;
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, plan.evaluateWithExecution(&execution, &.{}));
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, plan.evaluateWithExecution(&execution, &.{}));
    execution = Execution.init(unavailable.allocator(), &bytes);
    execution.numeric.checkpoint = Run.canceled;
    try std.testing.expectError(error.Canceled, plan.evaluateWithExecution(&execution, &.{}));
    execution.numeric.checkpoint = null;
    try std.testing.expectError(error.Canceled, plan.evaluateWithExecution(&execution, &.{}));
}

test "relational declarations mixed NUMERIC SQL row programs match PostgreSQL" {
    const a = std.testing.allocator;
    const columns = [_]@import("../sql/scalar.zig").Column{
        .{ .name = "n", .type = .number, .element_type = .numeric },
        .{ .name = "i", .type = .integer, .element_type = .int64 },
        .{ .name = "f", .type = .number, .element_type = .float64 },
    };
    const table: schema.TableSchema = .{ .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "n", .path = "n", .column_type = .numeric, .sql_element_type = .numeric },
        .{ .name = "i", .path = "i", .column_type = .integer, .sql_element_type = .int64 },
        .{ .name = "f", .path = "f", .column_type = .number, .sql_element_type = .float64 },
    } };
    const Case = struct { sql: []const u8, n: ?[]const u8, expected: ?[]const u8 = null, @"error": ?[]const u8 = null };
    const fixture = try std.json.parseFromSlice(struct { entries: []const Case }, a, @embedFile("../sql/fixtures/sql_schema_numeric_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    for (fixture.value.entries) |entry| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const owned = arena.allocator();
        var compiled = try @import("../sql/compiler.zig").compileScalar(owned, entry.sql, .{});
        defer compiled.deinit();
        const lowered = @import("../sql/schema_expression.zig").lowerColumns(owned, &columns, compiled.expression, null) catch |err| {
            if (entry.@"error") |state| {
                try std.testing.expectEqualStrings(state, @import("../sql/errors.zig").describe(err).code);
                continue;
            }
            return err;
        };
        const kind: Kind = if (lowered.element_type == .numeric) .numeric else switch (lowered.type) {
            .integer => .integer,
            .number => .number,
            .boolean => .boolean,
            else => unreachable,
        };
        var plan = Plan.init(a, table, lowered.expression, kind) catch |err| {
            std.debug.print("NUMERIC schema lowering failed SQL={s}\n", .{entry.sql});
            return err;
        };
        defer plan.deinit();
        var execution_bytes: usize = max_allocated_bytes;
        var execution = Execution.init(owned, &execution_bytes);
        const values = [_]Value{ if (entry.n) |n| try numericJson(&execution, .{ .string = n }) else .null, .{ .integer = 2 }, .{ .number = 1.25 } };
        if (entry.@"error") |state| {
            try std.testing.expectError(if (std.mem.eql(u8, state, "22003")) error.RelationalExpressionOverflow else if (std.mem.eql(u8, state, "22012")) error.RelationalExpressionDivisionByZero else return error.TestUnexpectedSqlstate, plan.evaluate(owned, &values));
            continue;
        }
        const result = try plan.evaluate(owned, &values);
        if (entry.expected) |wanted| switch (result) {
            .numeric => |bytes| {
                var context: exact.Context = .{ .alloc = owned };
                const decoded = try binary.decodeCanonical(&context, bytes);
                try std.testing.expectEqualStrings(wanted, try exact.format(&context, decoded.value));
            },
            .integer => |value| try std.testing.expectEqual(try std.fmt.parseInt(i64, wanted, 10), value),
            .number => |value| try std.testing.expectEqual(if (lowered.element_type == .float32) @as(f64, try std.fmt.parseFloat(f32, wanted)) else try std.fmt.parseFloat(f64, wanted), value),
            .boolean => |value| try std.testing.expectEqualStrings(wanted, if (value) "true" else "false"),
            else => return error.TestUnexpectedResult,
        } else try std.testing.expect(result == .null);
    }
    try std.testing.expectEqual(@as(usize, 62), fixture.value.entries.len);
}

test "relational declarations SQL NUMERIC lowering owns plans through allocation faults and compares without scratch" {
    const Fixture = struct {
        fn run(alloc: Allocator) !void {
            const columns = [_]@import("../sql/scalar.zig").Column{
                .{ .name = "n", .type = .number, .element_type = .numeric },
                .{ .name = "i", .type = .integer, .element_type = .int64 },
            };
            const table: schema.TableSchema = .{ .storage_mode = .relational, .relational_columns = &.{
                .{ .name = "n", .path = "n", .column_type = .numeric, .sql_element_type = .numeric },
                .{ .name = "i", .path = "i", .column_type = .integer, .sql_element_type = .int64 },
            } };
            var plan = blk: {
                var temporary = std.heap.ArenaAllocator.init(alloc);
                defer temporary.deinit();
                const a = temporary.allocator();
                const text = "CASE WHEN n>i THEN coalesce(n+1.25,i) ELSE 1/(i-i) END";
                var compiled = try @import("../sql/compiler.zig").compileScalar(a, text, .{});
                defer compiled.deinit();
                const lowered = try @import("../sql/schema_expression.zig").lowerColumns(a, &columns, compiled.expression, null);
                break :blk try Plan.init(alloc, table, lowered.expression, .numeric);
            };
            defer plan.deinit();
            // Plan owns literals after the input JSON/SQL arena is destroyed.
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const a = arena.allocator();
            var bytes: usize = max_allocated_bytes;
            var execution = Execution.init(a, &bytes);
            const source = try numericJson(&execution, .{ .string = "9007199254740993.2500" });
            const result = try plan.evaluate(a, &.{ source, .{ .integer = 2 } });
            const output = try numericJsonOutput(&execution, result.numeric);
            try std.testing.expectEqualStrings("9007199254740994.5000", output.number_string);
            if (plan.evaluate(a, &.{ .null, .{ .integer = 2 } })) |_| {
                return error.TestExpectedError;
            } else |err| switch (err) {
                error.RelationalExpressionDivisionByZero => {},
                else => return err,
            }
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const columns = [_]@import("../sql/scalar.zig").Column{.{ .name = "n", .type = .number, .element_type = .numeric }};
    const table: schema.TableSchema = .{ .storage_mode = .relational, .relational_columns = &.{.{ .name = "n", .path = "n", .column_type = .numeric, .sql_element_type = .numeric }} };
    var compiled = try @import("../sql/compiler.zig").compileScalar(a, "n >= 0.0000", .{});
    defer compiled.deinit();
    const lowered = try @import("../sql/schema_expression.zig").lowerColumns(a, &columns, compiled.expression, null);
    var plan = try Plan.init(std.testing.allocator, table, lowered.expression, .boolean);
    defer plan.deinit();
    var bytes: usize = max_allocated_bytes;
    var execution = Execution.init(a, &bytes);
    const value = try numericJson(&execution, .{ .string = "9007199254740993.2500" });
    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..10000) |_| try std.testing.expect((try plan.evaluate(std.testing.failing_allocator, &.{value})).boolean);
    std.debug.print("SQL durable NUMERIC comparison: rows=10000 scratch_bytes=0 elapsed_ns={}\n", .{std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start});
}

test "relational declarations NUMERIC SQL defaults preserve assignment failures and reader capability" {
    const a = std.testing.allocator;
    var compiled = try @import("../sql/compiler.zig").compile(a, "CREATE TABLE exact_defaults (n bigint DEFAULT 9007199254740993.5, f real DEFAULT 0.100000001490116119384765625, g bigint GENERATED ALWAYS AS (CAST(9007199254740993.5 AS bigint)) STORED)", .{});
    defer compiled.deinit();
    const source = try @import("../sql/ddl_runtime.zig").createSchemaAlloc(a, compiled.statement.create_table);
    defer a.free(source);
    var validator = try @import("mod.zig").CompiledTableValidator.init(a, source);
    defer validator.deinit(a);
    const expressions = validator.execution.expressions.?;
    try std.testing.expect(expressions.table.requires_exact_numeric_expressions);
    var document = try std.json.parseFromSlice(std.json.Value, a, "{}", .{});
    defer document.deinit();
    try expressions.applyJson(document.arena.allocator(), &document.value);
    try std.testing.expectEqual(@as(i64, 9007199254740994), document.value.object.get("n").?.integer);
    try std.testing.expectEqual(@as(i64, 9007199254740994), document.value.object.get("g").?.integer);
    try std.testing.expectEqual(@as(f64, @as(f32, 0.1)), document.value.object.get("f").?.float);
    try expressions.verifyJson(a, document.value);
    var overflow = try @import("../sql/compiler.zig").compile(a, "CREATE TABLE overflow_defaults (n smallint DEFAULT 32767.5)", .{});
    defer overflow.deinit();
    const overflow_source = try @import("../sql/ddl_runtime.zig").createSchemaAlloc(a, overflow.statement.create_table);
    defer a.free(overflow_source);
    var overflow_validator = try @import("mod.zig").CompiledTableValidator.init(a, overflow_source);
    defer overflow_validator.deinit(a);
    var missing = try std.json.parseFromSlice(std.json.Value, a, "{}", .{});
    defer missing.deinit();
    try std.testing.expectError(error.RelationalExpressionOverflow, overflow_validator.execution.expressions.?.applyJson(missing.arena.allocator(), &missing.value));
}

test "relational declarations NUMERIC schema execution owns scratch and shares sticky admission" {
    const a = std.testing.allocator;
    const parsed = try std.json.parseFromSlice(std.json.Value, a,
        \\{"op":"multiply","args":[{"op":"literal","type":"numeric","value":"9007199254740993.0001"},{"op":"literal","type":"numeric","value":"3.000"}]}
    , .{});
    defer parsed.deinit();
    const Run = struct {
        fn run(alloc: Allocator, expression: std.json.Value) !void {
            var plan = try Plan.init(alloc, .{ .storage_mode = .relational }, expression, .numeric);
            defer plan.deinit();
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            var bytes: usize = max_allocated_bytes;
            var execution = Execution.init(arena.allocator(), &bytes);
            const result = try plan.evaluateWithExecution(&execution, &.{});
            const text = try numericJsonOutput(&execution, result.numeric);
            try std.testing.expectEqualStrings("27021597764222979.0003000", text.number_string);
            try std.testing.expect(bytes < max_allocated_bytes);
            try std.testing.expectEqual(arena.allocator().ptr, execution.numeric.alloc.ptr);
            execution.numeric.remaining = 0;
            try std.testing.expectError(error.RelationalExpressionBudgetExceeded, plan.evaluateWithExecution(&execution, &.{}));
            execution.numeric.remaining = 1000;
            try std.testing.expectError(error.RelationalExpressionBudgetExceeded, plan.evaluateWithExecution(&execution, &.{}));
            var tiny: usize = 1;
            var bounded = Execution.init(arena.allocator(), &tiny);
            try std.testing.expectError(error.RelationalExpressionBudgetExceeded, plan.evaluateWithExecution(&bounded, &.{}));
            const Poll = struct {
                fn canceled(_: ?*anyopaque) anyerror!void {
                    return error.Canceled;
                }
            };
            var canceled = Execution.init(arena.allocator(), &bytes);
            canceled.numeric.checkpoint = Poll.canceled;
            try std.testing.expectError(error.Canceled, plan.evaluateWithExecution(&canceled, &.{}));
            canceled.numeric.checkpoint = null;
            try std.testing.expectError(error.Canceled, plan.evaluateWithExecution(&canceled, &.{}));
        }
    };
    try Run.run(a, parsed.value);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{parsed.value});
}

test "relational declarations NUMERIC defaults generated topology and strict restoration" {
    const a = std.testing.allocator;
    const source: schema.TableSchema = .{ .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "base", .path = "base", .column_type = .numeric, .sql_element_type = .numeric },
        .{ .name = "sum", .path = "sum", .column_type = .numeric, .sql_element_type = .numeric },
        .{ .name = "negative", .path = "negative", .column_type = .numeric, .sql_element_type = .numeric },
    } };
    const encoded = try schema.serializeSchema(a, source);
    defer a.free(encoded);
    const declarations = try std.json.parseFromSlice(std.json.Value, a,
        \\{"defaults":[{"column":"base","expression":{"op":"literal","type":"numeric","value":"9007199254740993.0001"}}],"generated":[{"column":"negative","expression":{"op":"negate","args":[{"op":"column","column":"sum"}]}},{"column":"sum","expression":{"op":"add","args":[{"op":"column","column":"base"},{"op":"literal","type":"numeric","value":"0.0002"}]}}]}
    , .{});
    defer declarations.deinit();
    const Run = struct {
        fn run(alloc: Allocator, wire: []const u8, defs: std.json.Value) !void {
            const table = try schema.deserializeSchema(alloc, wire);
            const set = Set.createOwned(alloc, table, defs.object.get("defaults").?, defs.object.get("generated").?) catch |err| {
                schema.freeSchema(alloc, table);
                return err;
            };
            defer set.deinit();
            var document = try std.json.parseFromSlice(std.json.Value, alloc, "{}", .{});
            defer document.deinit();
            try set.applyJson(document.arena.allocator(), &document.value);
            try std.testing.expectEqualStrings("9007199254740993.0001", document.value.object.get("base").?.number_string);
            try std.testing.expectEqualStrings("9007199254740993.0003", document.value.object.get("sum").?.number_string);
            try std.testing.expectEqualStrings("-9007199254740993.0003", document.value.object.get("negative").?.number_string);
            try set.verifyJson(alloc, document.value);
            // Restore accepts logically equivalent display scales but never
            // repairs missing or forged generated values.
            try document.value.object.put(document.arena.allocator(), "sum", .{ .number_string = "9007199254740993.000300" });
            try set.verifyJson(alloc, document.value);
            try document.value.object.put(document.arena.allocator(), "sum", .{ .number_string = "9007199254740993.0004" });
            try invalidGenerated(alloc, set, document.value);
            _ = document.value.object.swapRemove("sum");
            try invalidGenerated(alloc, set, document.value);
            var explicit_null = try std.json.parseFromSlice(std.json.Value, alloc, "{\"base\":null}", .{});
            defer explicit_null.deinit();
            try set.applyJson(explicit_null.arena.allocator(), &explicit_null.value);
            try std.testing.expectEqual(std.json.Value.null, explicit_null.value.object.get("sum").?);
            try std.testing.expectEqual(std.json.Value.null, explicit_null.value.object.get("negative").?);
            try set.verifyJson(alloc, explicit_null.value);
        }
        fn invalidGenerated(alloc: Allocator, set: *const Set, document: std.json.Value) !void {
            if (set.verifyJson(alloc, document)) |_| {
                return error.TestExpectedError;
            } else |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqual(error.InvalidRelationalGeneratedValue, err);
            }
        }
    };
    try Run.run(a, encoded, declarations.value);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{ encoded, declarations.value });
}

test "relational declarations NUMERIC target modifiers precede dependent expressions with PostgreSQL oracle" {
    const a = std.testing.allocator;
    const source: schema.TableSchema = .{ .storage_mode = .relational, .requires_public_schema = true, .requires_numeric_modifiers = true, .relational_columns = &.{
        .{ .name = "base", .path = "base", .column_type = .numeric, .sql_element_type = .numeric, .numeric_modifier = .{ .precision = 4, .scale = 2 } },
        .{ .name = "narrow", .path = "narrow", .column_type = .numeric, .sql_element_type = .numeric, .numeric_modifier = .{ .precision = 3, .scale = 1 } },
        .{ .name = "total", .path = "total", .column_type = .numeric, .sql_element_type = .numeric, .numeric_modifier = .{ .precision = 5, .scale = 2 } },
    } };
    const wire = try schema.serializeSchema(a, source);
    defer a.free(wire);
    const declarations = try std.json.parseFromSlice(std.json.Value, a,
        \\{"defaults":[{"column":"base","expression":{"op":"literal","type":"numeric","value":"1.245"}}],"generated":[{"column":"total","expression":{"op":"add","args":[{"op":"column","column":"base"},{"op":"column","column":"narrow"}]}},{"column":"narrow","expression":{"op":"column","column":"base"}}]}
    , .{});
    defer declarations.deinit();
    const Entry = struct { use_default: bool = false, input: ?[]const u8 = null, expected: ?[]const ?[]const u8 = null, @"error": ?[]const u8 = null };
    const fixture = try std.json.parseFromSlice(struct { entries: []const Entry }, a, @embedFile("../sql/fixtures/sql_numeric_assignment_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    const Run = struct {
        fn run(alloc: Allocator, encoded: []const u8, defs: std.json.Value, cases: []const Entry) !void {
            const table = try schema.deserializeSchema(alloc, encoded);
            const set = Set.createOwned(alloc, table, defs.object.get("defaults").?, defs.object.get("generated").?) catch |err| {
                schema.freeSchema(alloc, table);
                return err;
            };
            defer set.deinit();
            for (cases) |entry| {
                var document = try std.json.parseFromSlice(std.json.Value, alloc, "{}", .{});
                defer document.deinit();
                if (!entry.use_default) try document.value.object.put(document.arena.allocator(), "base", if (entry.input) |text| .{ .string = text } else .null);
                if (entry.@"error") |code| {
                    try std.testing.expectEqualStrings("22003", code);
                    if (set.applyJson(document.arena.allocator(), &document.value)) |_| return error.TestExpectedError else |err| {
                        if (err == error.OutOfMemory) return err;
                        try std.testing.expectEqual(error.RelationalExpressionOverflow, err);
                    }
                    try std.testing.expect(!document.value.object.contains("total"));
                    continue;
                }
                try set.applyJson(document.arena.allocator(), &document.value);
                for ([_][]const u8{ "base", "narrow", "total" }, entry.expected.?) |name, expected| {
                    const value = document.value.object.get(name).?;
                    if (expected) |text| try std.testing.expectEqualStrings(text, if (value == .number_string) value.number_string else value.string) else try std.testing.expectEqual(std.json.Value.null, value);
                }
                try set.verifyJson(alloc, document.value);
                if (entry.use_default) {
                    var verification_bytes: usize = max_allocated_bytes;
                    var verification = Execution.init(alloc, &verification_bytes);
                    try set.verifyJsonWithExecution(&verification, document.value);
                    try std.testing.expect(verification.numeric.remaining < 8 * 1024 * 1024);
                    try std.testing.expect(verification_bytes < max_allocated_bytes);
                    try std.testing.expectEqual(alloc.ptr, verification.alloc.ptr);
                    try std.testing.expectEqual(alloc.vtable, verification.alloc.vtable);
                    verification.numeric.remaining = 0;
                    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, set.verifyJsonWithExecution(&verification, document.value));
                    verification.numeric.remaining = 8 * 1024 * 1024;
                    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, set.verifyJsonWithExecution(&verification, document.value));
                    verification_bytes = 1;
                    verification = Execution.init(alloc, &verification_bytes);
                    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, set.verifyJsonWithExecution(&verification, document.value));
                    // Equivalent logical display scales are not repairs.
                    try document.value.object.put(document.arena.allocator(), "base", .{ .number_string = "1.2500" });
                    try set.verifyJson(alloc, document.value);
                    // A consistent-looking generated value cannot authorize
                    // an unconstrained base assignment during restoration.
                    try document.value.object.put(document.arena.allocator(), "base", .{ .number_string = "1.245" });
                    if (set.verifyJson(alloc, document.value)) |_| return error.TestExpectedError else |err| {
                        if (err == error.OutOfMemory) return err;
                        try std.testing.expectEqual(error.InvalidRelationalGeneratedValue, err);
                    }
                }
            }
        }
    };
    try Run.run(a, wire, declarations.value, fixture.value.entries);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{ wire, declarations.value, fixture.value.entries[0..1] });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{ wire, declarations.value, fixture.value.entries[6..7] });
}

test "relational declarations NUMERIC JSON assignments share PostgreSQL scalar and array semantics" {
    const a = std.testing.allocator;
    const Entry = struct { op: []const u8, left: []const u8, precision: u16 = 0, scale: i16 = 0, expected: ?std.json.Value = null, @"error": ?[]const u8 = null };
    const fixture = try std.json.parseFromSlice(struct { entries: []const Entry }, a, @embedFile("../sql/fixtures/sql_exact_numeric_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    var tested: usize = 0;
    for (fixture.value.entries) |entry| {
        if (!std.mem.eql(u8, entry.op, "typmod")) continue;
        tested += 1;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const alloc = arena.allocator();
        const json = try std.json.Stringify.valueAlloc(alloc, .{
            .dimensions = .{ .{ .length = 2, .lower_bound = -3 }, .{ .length = 2, .lower_bound = 7 } },
            .values = .{ entry.left, null, entry.left, null },
            .sql_nulls = .{ false, true, false, true },
        }, .{});
        var document = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
        defer document.deinit();
        const original = document.value.object.get("values").?.array.items;
        var budget: usize = max_allocated_bytes;
        var execution = Execution.init(alloc, &budget);
        const modifier: exact.TypeModifier = .{ .precision = entry.precision, .scale = entry.scale };
        if (entry.@"error") |code| {
            const expected = if (std.mem.eql(u8, code, "22023")) error.SqlInvalidParameterValue else error.RelationalExpressionOverflow;
            try std.testing.expectError(expected, normalizeNumericJson(&execution, .{ .string = entry.left }, modifier, false));
            try std.testing.expectError(expected, normalizeNumericArrayJson(&execution, &document.value, modifier, false));
            try std.testing.expectEqual(original.ptr, document.value.object.get("values").?.array.items.ptr);
            continue;
        }
        const scalar = try normalizeNumericJson(&execution, .{ .string = entry.left }, modifier, false);
        try std.testing.expectEqualStrings(entry.expected.?.string, if (scalar == .number_string) scalar.number_string else scalar.string);
        try normalizeNumericArrayJson(&execution, &document.value, modifier, false);
        const cells = document.value.object.get("values").?.array.items;
        try std.testing.expectEqualStrings(entry.expected.?.string, cells[0].string);
        try std.testing.expectEqualStrings(entry.expected.?.string, cells[2].string);
        try std.testing.expectEqual(std.json.Value.null, cells[1]);
        try std.testing.expectEqual(std.json.Value.null, cells[3]);
        try std.testing.expectEqual(@as(i64, -3), document.value.object.get("dimensions").?.array.items[0].object.get("lower_bound").?.integer);
        try std.testing.expectEqual(@as(i64, 7), document.value.object.get("dimensions").?.array.items[1].object.get("lower_bound").?.integer);
        try normalizeNumericArrayJson(&execution, &document.value, modifier, true);
        try std.testing.expectEqual(cells.ptr, document.value.object.get("values").?.array.items.ptr);
    }
    try std.testing.expectEqual(@as(usize, 20), tested);
}

test "relational declarations NUMERIC JSON array preparation is atomic bounded and cancellation aware" {
    const a = std.testing.allocator;
    const text =
        \\{"dimensions":[{"length":4,"lower_bound":-3}],"values":["1.245",null,"2.5",null],"sql_nulls":[false,true,false,true]}
    ;
    const modifier: exact.TypeModifier = .{ .precision = 4, .scale = 2 };
    const Run = struct {
        fn run(alloc: Allocator, source: []const u8, target: exact.TypeModifier, overflow: bool) !void {
            var document = try std.json.parseFromSlice(std.json.Value, alloc, source, .{});
            defer document.deinit();
            const owned = document.arena.allocator();
            const original = document.value.object.get("values").?.array.items;
            if (overflow) original[2] = .{ .string = "999.995" };
            var budget: usize = max_allocated_bytes;
            var execution = Execution.init(owned, &budget);
            if (overflow) {
                if (normalizeNumericArrayJson(&execution, &document.value, target, false)) |_| return error.TestExpectedError else |err| {
                    if (err == error.OutOfMemory) return err;
                    try std.testing.expectEqual(error.RelationalExpressionOverflow, err);
                }
                try std.testing.expectEqual(original.ptr, document.value.object.get("values").?.array.items.ptr);
                try std.testing.expectEqualStrings("1.245", original[0].string);
            } else {
                try normalizeNumericArrayJson(&execution, &document.value, target, false);
                const cells = document.value.object.get("values").?.array.items;
                try std.testing.expectEqualStrings("1.25", cells[0].string);
                try std.testing.expectEqualStrings("2.50", cells[2].string);
                try std.testing.expect(budget < max_allocated_bytes);
            }
        }
    };
    try Run.run(a, text, modifier, false);
    try Run.run(a, text, modifier, true);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{ text, modifier, false });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{ text, modifier, true });
    var document = try std.json.parseFromSlice(std.json.Value, a, text, .{});
    defer document.deinit();
    const original = document.value.object.get("values").?.array.items;
    var budget: usize = max_allocated_bytes;
    var execution = Execution.init(a, &budget);
    try normalizeNumericArrayJson(&execution, &document.value, null, true);
    const used = 8 * 1024 * 1024 - execution.numeric.remaining;
    try std.testing.expectEqual(original.ptr, document.value.object.get("values").?.array.items.ptr);
    try std.testing.expectError(error.InvalidRelationalGeneratedValue, normalizeNumericArrayJson(&execution, &document.value, modifier, true));
    execution = Execution.init(a, &budget);
    execution.numeric.remaining = used - 1;
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, normalizeNumericArrayJson(&execution, &document.value, null, true));
    execution.numeric.remaining = 8 * 1024 * 1024;
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, normalizeNumericJson(&execution, .{ .integer = 1 }, null, true));
    const Poll = struct {
        fn canceled(_: ?*anyopaque) anyerror!void {
            return error.Canceled;
        }
    };
    execution = Execution.init(a, &budget);
    execution.numeric.checkpoint = Poll.canceled;
    try std.testing.expectError(error.Canceled, normalizeNumericArrayJson(&execution, &document.value, null, true));
    execution.numeric.checkpoint = null;
    try std.testing.expectError(error.Canceled, normalizeNumericJson(&execution, .{ .integer = 1 }, null, true));
    budget = 1;
    execution = Execution.init(a, &budget);
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, normalizeNumericArrayJson(&execution, &document.value, null, true));
    budget = max_allocated_bytes;
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, normalizeNumericJson(&execution, .{ .integer = 1 }, null, true));
}

test "relational declarations NUMERIC JSON array validation reuses bounded scratch across ten thousand cells" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const values = try alloc.alloc([]const u8, 10000);
    @memset(values, "1.25");
    const nulls = try alloc.alloc(bool, values.len);
    @memset(nulls, false);
    const json = try std.json.Stringify.valueAlloc(alloc, .{
        .dimensions = .{.{ .length = values.len, .lower_bound = -100 }},
        .values = values,
        .sql_nulls = nulls,
    }, .{});
    var document = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
    defer document.deinit();
    var budget: usize = max_allocated_bytes;
    var execution = Execution.init(alloc, &budget);
    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    try normalizeNumericArrayJson(&execution, &document.value, .{ .precision = 4, .scale = 2 }, true);
    // Retained scratch is bounded by the largest cell, not the cell count.
    try std.testing.expect(max_allocated_bytes - budget < 4096);
    std.debug.print("NUMERIC JSON array admission: cells=10000 scratch_bytes={} work={} elapsed_ns={}\n", .{ max_allocated_bytes - budget, 8 * 1024 * 1024 - execution.numeric.remaining, std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start });
}

test "relational declarations NUMERIC constrained bindings reuse canonical bytes with sticky admission" {
    const a = std.testing.allocator;
    const bytes = try @import("../sql/numeric_storage.zig").encodeJsonAlloc(a, .{ .string = "1.25" });
    defer a.free(bytes);
    var none = std.heap.FixedBufferAllocator.init(&.{});
    var remaining: usize = max_allocated_bytes;
    var scope = Execution.init(none.allocator(), &remaining);
    const modifier: exact.TypeModifier = .{ .precision = 4, .scale = 2 };
    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..10000) |_| {
        const result = try normalizeNumericBinding(&scope, bytes, modifier, false);
        try std.testing.expectEqual(bytes.ptr, result.numeric.ptr);
    }
    try std.testing.expectEqual(max_allocated_bytes, remaining);
    std.debug.print("NUMERIC binding reuse: rows=10000 allocated_bytes=0 elapsed_ns={}\n", .{std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start});
    scope.numeric.remaining = 0;
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, normalizeNumericBinding(&scope, bytes, modifier, false));
    scope.numeric.remaining = 1000;
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, normalizeNumericBinding(&scope, bytes, modifier, false));
    scope = Execution.init(none.allocator(), &remaining);
    const Poll = struct {
        fn canceled(_: ?*anyopaque) anyerror!void {
            return error.Canceled;
        }
    };
    scope.numeric.checkpoint = Poll.canceled;
    try std.testing.expectError(error.Canceled, normalizeNumericBinding(&scope, bytes, modifier, false));
    scope.numeric.checkpoint = null;
    try std.testing.expectError(error.Canceled, normalizeNumericBinding(&scope, bytes, modifier, false));
}

test "relational declarations NUMERIC generated identity binds target and dependency modifiers and fences cold rows" {
    const a = std.testing.allocator;
    var columns = [_]schema.RelationalColumn{
        .{ .name = "base", .path = "base", .column_type = .numeric, .sql_element_type = .numeric, .numeric_modifier = .{ .precision = 4, .scale = 2 } },
        .{ .name = "derived", .path = "derived", .column_type = .numeric, .sql_element_type = .numeric, .numeric_modifier = .{ .precision = 4, .scale = 2 } },
        .{ .name = "cold", .path = "cold", .column_type = .numeric, .sql_element_type = .numeric, .numeric_modifier = .{ .precision = 4, .scale = 2 } },
    };
    const table: schema.TableSchema = .{ .storage_mode = .relational, .requires_public_schema = true, .requires_numeric_modifiers = true, .relational_columns = &columns };
    const defs = try std.json.parseFromSlice(std.json.Value, a,
        \\[{"column":"derived","expression":{"op":"column","column":"base"}}]
    , .{});
    defer defs.deinit();
    const Run = struct {
        fn identity(alloc: Allocator, source: schema.TableSchema, declarations: std.json.Value) ![32]u8 {
            const encoded = try schema.serializeSchema(alloc, source);
            defer alloc.free(encoded);
            const owned = try schema.deserializeSchema(alloc, encoded);
            const set = Set.createOwned(alloc, owned, .null, declarations) catch |err| {
                schema.freeSchema(alloc, owned);
                return err;
            };
            defer set.deinit();
            const Row = struct {
                table_schema: schema.TableSchema,
                reads: *usize,
                pub fn ordinalForName(_: @This(), name: []const u8) ?u32 {
                    return if (std.mem.eql(u8, name, "base")) 0 else 1;
                }
                pub fn findCell(self: @This(), _: u32) !?codec.Cell {
                    self.reads.* += 1;
                    return null;
                }
            };
            var historical_columns = [_]schema.RelationalColumn{ source.relational_columns[0], source.relational_columns[1] };
            historical_columns[0].numeric_modifier.?.scale += 1;
            var historical = source;
            historical.relational_columns = &historical_columns;
            var reads: usize = 0;
            try std.testing.expectEqualSlices(u32, &.{ 0, 1 }, set.modifier_ordinals);
            try std.testing.expectError(error.InvalidRelationalGeneratedValue, set.verifyRow(alloc, Row{ .table_schema = historical, .reads = &reads }));
            try std.testing.expectEqual(@as(usize, 0), reads);
            return generatedFingerprint(set);
        }
    };
    const original = try Run.identity(a, table, defs.value);
    columns[0].numeric_modifier.?.scale = 1;
    const dependency = try Run.identity(a, table, defs.value);
    try std.testing.expect(!std.mem.eql(u8, &original, &dependency));
    columns[0].numeric_modifier.?.scale = 2;
    columns[1].numeric_modifier.?.scale = 1;
    const target = try Run.identity(a, table, defs.value);
    try std.testing.expect(!std.mem.eql(u8, &original, &target));
    columns[1].numeric_modifier.?.scale = 2;
    columns[2].numeric_modifier.?.scale = 1;
    const unrelated = try Run.identity(a, table, defs.value);
    try std.testing.expectEqualSlices(u8, &original, &unrelated);
}

test "relational declarations SQL numeric programs retain narrow domains and checked casts" {
    const alloc = std.testing.allocator;
    const ast = @import("../sql/ast.zig");
    const columns = [_]@import("../sql/scalar.zig").Column{
        .{ .name = "n", .type = .integer, .element_type = .int16 },
        .{ .name = "i", .type = .integer, .element_type = .int32 },
        .{ .name = "b", .type = .integer, .element_type = .int64 },
        .{ .name = "f", .type = .number, .element_type = .float32 },
        .{ .name = "d", .type = .number, .element_type = .float64 },
    };
    const table: schema.TableSchema = .{ .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "n", .path = "n", .column_type = .integer, .sql_element_type = .int16 },
        .{ .name = "i", .path = "i", .column_type = .integer, .sql_element_type = .int32 },
        .{ .name = "b", .path = "b", .column_type = .integer, .sql_element_type = .int64 },
        .{ .name = "f", .path = "f", .column_type = .number, .sql_element_type = .float32 },
        .{ .name = "d", .path = "d", .column_type = .number, .sql_element_type = .float64 },
    } };
    const Case = struct { sql: []const u8, n: ?i64 = 1, i: i64 = 1, d: f64 = 2.5, expected: std.json.Value = .null, @"error": ?[]const u8 = null };
    const fixture = try std.json.parseFromSlice(struct { reference: []const u8, entries: []const Case }, alloc, @embedFile("../sql/fixtures/sql_numeric_expression_reference.json"), .{});
    defer fixture.deinit();
    for (fixture.value.entries) |case| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        var compiled = try @import("../sql/compiler.zig").compileScalar(a, case.sql, .{});
        defer compiled.deinit();
        const lowered = try @import("../sql/schema_expression.zig").lowerColumns(a, &columns, compiled.expression, null);
        const expected: Kind = switch (lowered.type) {
            ast.ColumnType.integer => .integer,
            .number => .number,
            .boolean => .boolean,
            .string => .string,
            else => unreachable,
        };
        var plan = try Plan.init(alloc, table, lowered.expression, expected);
        defer plan.deinit();
        const cells = [_]Value{ if (case.n) |n| .{ .integer = n } else .null, .{ .integer = case.i }, .{ .integer = 9007199254740993 }, .{ .number = 16777216 }, .{ .number = case.d } };
        var query = try @import("../sql/scalar.zig").bind(a, compiled.expression, &columns, &.{}, .{});
        defer query.deinit();
        const Datum = @import("../sql/scalar.zig").Datum;
        const inputs = [_]Datum{ if (case.n) |n| Datum.json(.{ .integer = n }) else .{}, Datum.json(.{ .integer = case.i }), Datum.json(.{ .integer = 9007199254740993 }), Datum.json(.{ .float = 16777216 }), Datum.json(.{ .float = case.d }) };
        if (case.@"error") |state| {
            const failure = if (std.mem.eql(u8, state, "22003")) error.RelationalExpressionOverflow else if (std.mem.eql(u8, state, "22012")) error.RelationalExpressionDivisionByZero else return error.TestUnexpectedSqlstate;
            try std.testing.expectError(failure, plan.evaluate(alloc, &cells));
            try std.testing.expectError(if (std.mem.eql(u8, state, "22003")) error.SqlNumericOutOfRange else error.SqlDivisionByZero, query.evaluate(a, &inputs, &.{}, .{}));
        } else {
            const wanted = try checks.valueFromJson(a, expected, case.expected, true);
            try std.testing.expect(try valuesEqual(wanted, try plan.evaluate(alloc, &cells)));
            const actual = try query.evaluate(a, &inputs, &.{}, .{});
            try std.testing.expectEqual(case.expected == .null, actual.sql_null);
            try std.testing.expect(try valuesEqual(wanted, try checks.valueFromJson(a, expected, actual.value, true)));
        }
    }
}

test "relational declarations SQL numeric programs conditional allocation faults and zero scratch evaluation" {
    const Fixture = struct {
        fn run(alloc: Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const a = arena.allocator();
            var compiled = try @import("../sql/compiler.zig").compileScalar(a, "CASE WHEN n>0 THEN CAST(n AS integer) ELSE 1/(i-i) END", .{});
            defer compiled.deinit();
            const columns = [_]@import("../sql/scalar.zig").Column{
                .{ .name = "n", .type = .integer, .element_type = .int16 },
                .{ .name = "i", .type = .integer, .element_type = .int32 },
            };
            const table: schema.TableSchema = .{ .storage_mode = .relational, .relational_columns = &.{
                .{ .name = "n", .path = "n", .column_type = .integer, .sql_element_type = .int16 },
                .{ .name = "i", .path = "i", .column_type = .integer, .sql_element_type = .int32 },
            } };
            const lowered = try @import("../sql/schema_expression.zig").lowerColumns(a, &columns, compiled.expression, null);
            var plan = try Plan.init(alloc, table, lowered.expression, .integer);
            defer plan.deinit();
            const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
            var total: i64 = 0;
            for (0..10000) |i| {
                total += (try plan.evaluate(std.testing.failing_allocator, &.{ .{ .integer = @intCast(i + 1) }, .{ .integer = 1 } })).integer;
            }
            try std.testing.expectEqual(@as(i64, 50005000), total);
            try std.testing.expectError(error.RelationalExpressionDivisionByZero, plan.evaluate(std.testing.failing_allocator, &.{ .null, .{ .integer = 1 } }));
            // Fault probes reach this point only after all preparation succeeds.
            std.debug.print("SQL durable CASE: rows=10000 scratch_bytes=0 elapsed_ns={}\n", .{std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start});
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "relational declarations SQL membership preparation faults and zero scratch evaluation" {
    const Fixture = struct {
        fn run(alloc: Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const a = arena.allocator();
            var compiled = try @import("../sql/compiler.zig").compileScalar(a, "(n % 3) IN (NULL,CAST(-1 AS smallint),1) AND (n>0) IS NOT UNKNOWN", .{});
            defer compiled.deinit();
            const columns = [_]@import("../sql/scalar.zig").Column{.{ .name = "n", .type = .integer, .element_type = .int16 }};
            const table: schema.TableSchema = .{ .storage_mode = .relational, .relational_columns = &.{.{ .name = "n", .path = "n", .column_type = .integer, .sql_element_type = .int16 }} };
            const lowered = try @import("../sql/schema_expression.zig").lowerColumns(a, &columns, compiled.expression, null);
            var plan = try Plan.init(alloc, table, lowered.expression, .boolean);
            defer plan.deinit();
            const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
            for (0..10000) |_| try std.testing.expect((try plan.evaluate(std.testing.failing_allocator, &.{.{ .integer = -7 }})).boolean);
            try std.testing.expect(!(try plan.evaluate(std.testing.failing_allocator, &.{.null})).boolean);
            std.debug.print("SQL durable membership: rows=10000 scratch_bytes=0 elapsed_ns={}\n", .{std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start});
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "relational declarations SQL numeric programs fence nonnumeric conditional generated columns" {
    const alloc = std.testing.allocator;
    var validator = try @import("mod.zig").CompiledTableValidator.init(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","column_defaults":[{"column":"x","expression":{"op":"literal","type":"string","value":"ready"}}],"generated_columns":[{"column":"y","expression":{"op":"case_when","args":[{"op":"is_null","args":[{"op":"column","column":"x"}]},{"op":"literal","type":"string","value":"missing"},{"op":"column","column":"x"}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":["keyword","null"]},"y":{"type":"keyword"}},"required":["y"],"additionalProperties":false}}}}
    );
    defer validator.deinit(alloc);
    const expressions = validator.execution.expressions.?;
    try std.testing.expect(expressions.table.requires_typed_expressions);
    const runtime = try @import("mod.zig").deriveRuntimeTableSchema(alloc, validator.schema);
    defer schema.freeSchema(alloc, runtime);
    try std.testing.expect(runtime.requires_typed_expressions);
    var missing = try std.json.parseFromSlice(std.json.Value, alloc, "{}", .{});
    defer missing.deinit();
    try expressions.applyJson(missing.arena.allocator(), &missing.value);
    try std.testing.expectEqualStrings("ready", missing.value.object.get("y").?.string);
    try expressions.verifyJson(alloc, missing.value);
    var explicit = try std.json.parseFromSlice(std.json.Value, alloc, "{\"x\":null}", .{});
    defer explicit.deinit();
    try expressions.applyJson(explicit.arena.allocator(), &explicit.value);
    try std.testing.expectEqualStrings("missing", explicit.value.object.get("y").?.string);
    try expressions.verifyJson(alloc, explicit.value);
    try explicit.value.object.put(explicit.arena.allocator(), "y", .{ .string = "forged" });
    try std.testing.expectError(error.InvalidRelationalGeneratedValue, expressions.verifyJson(alloc, explicit.value));
}

test "relational declarations SQL numeric programs reject malformed conditional domains" {
    const alloc = std.testing.allocator;
    for ([_][]const u8{
        \\{"op":"case_when","args":[{"op":"literal","type":"integer","value":1},{"op":"literal","type":"integer","value":1},{"op":"literal","type":"integer","value":0}]}
        ,
        \\{"op":"case_when","args":[{"op":"literal","type":"boolean","value":true},{"op":"literal","type":"integer","value":1},{"op":"literal","type":"string","value":"bad"}]}
        ,
    }) |text| {
        const parsed = try std.json.parseFromSlice(std.json.Value, alloc, text, .{});
        defer parsed.deinit();
        try std.testing.expectError(error.InvalidRelationalExpressionType, Plan.init(alloc, .{}, parsed.value, .integer));
    }
    const even = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"op":"case_when","args":[{"op":"literal","type":"boolean","value":true},{"op":"literal","type":"integer","value":1}]}
    , .{});
    defer even.deinit();
    try std.testing.expectError(error.InvalidRelationalExpression, Plan.init(alloc, .{}, even.value, .integer));
}

/// Immutable schema-owned defaults and generated-column dependency graph.
/// Defaults cannot reference columns. Generated columns execute in dependency
/// order, irrespective of declaration order. Callers enforce input policy.
pub const Set = struct {
    alloc: Allocator,
    table: schema.TableSchema,
    bindings: []Binding,
    order: []usize,
    read_columns: []bool,
    generated_columns: []bool,
    modifier_ordinals: []const u32,
    pub const Binding = struct { ordinal: u32, generated: bool, plan: Plan };

    pub fn createOwned(alloc: Allocator, table: schema.TableSchema, defaults: std.json.Value, generated: std.json.Value) !*Set {
        const default_entries = try entries(defaults);
        const generated_entries = try entries(generated);
        if (default_entries.len + generated_entries.len > 256) return error.RelationalExpressionBudgetExceeded;
        const bindings = try alloc.alloc(Binding, default_entries.len + generated_entries.len);
        errdefer alloc.free(bindings);
        var initialized: usize = 0;
        var total_nodes: usize = 0;
        var total_literal_bytes: usize = 0;
        errdefer for (bindings[0..initialized]) |*binding| binding.plan.deinit();
        for ([_][]const std.json.Value{ default_entries, generated_entries }, 0..) |declarations, mode| {
            for (declarations) |entry| {
                if (entry != .object or entry.object.count() != 2) return error.InvalidRelationalExpression;
                const name = entry.object.get("column") orelse return error.InvalidRelationalExpression;
                if (name != .string) return error.InvalidRelationalExpression;
                const ordinal: u32 = for (table.relational_columns, 0..) |column, i| {
                    if (std.mem.eql(u8, column.name, name.string)) break @intCast(i);
                } else return error.RelationalIndexColumnNotFound;
                for (bindings[0..initialized]) |binding| if (binding.ordinal == ordinal) return error.InvalidRelationalExpression;
                var plan = try Plan.init(alloc, table, entry.object.get("expression") orelse return error.InvalidRelationalExpression, table.relational_columns[ordinal].column_type);
                errdefer plan.deinit();
                total_nodes += plan.nodes.len;
                total_literal_bytes += plan.literal_bytes;
                if (total_nodes > 4096 or total_literal_bytes > max_allocated_bytes) return error.RelationalExpressionBudgetExceeded;
                if (mode == 0 and plan.dependencies.len != 0) return error.InvalidRelationalExpression;
                bindings[initialized] = .{ .ordinal = ordinal, .generated = mode != 0, .plan = plan };
                initialized += 1;
            }
        }
        const order = try alloc.alloc(usize, bindings.len);
        errdefer alloc.free(order);
        const read_columns = try alloc.alloc(bool, table.relational_columns.len);
        errdefer alloc.free(read_columns);
        @memset(read_columns, false);
        const generated_columns = try alloc.alloc(bool, table.relational_columns.len);
        errdefer alloc.free(generated_columns);
        @memset(generated_columns, false);
        for (bindings) |binding| {
            for (binding.plan.dependencies) |ordinal| read_columns[ordinal] = true;
            if (binding.generated) {
                read_columns[binding.ordinal] = true;
                generated_columns[binding.ordinal] = true;
            }
        }
        var states = @as([256]u2, @splat(0));
        var written: usize = 0;
        for (bindings, 0..) |_, i| try visit(bindings, order, &states, &written, i);
        var constrained: std.ArrayList(u32) = .empty;
        defer constrained.deinit(alloc);
        for (table.relational_columns, read_columns, 0..) |column, read, ordinal| {
            if (read and column.numeric_modifier != null) try constrained.append(alloc, @intCast(ordinal));
        }
        const modifier_ordinals = try constrained.toOwnedSlice(alloc);
        errdefer alloc.free(modifier_ordinals);
        const set = try alloc.create(Set);
        set.* = .{ .alloc = alloc, .table = table, .bindings = bindings, .order = order, .read_columns = read_columns, .generated_columns = generated_columns, .modifier_ordinals = modifier_ordinals };
        return set;
    }

    fn visit(bindings: []const Binding, order: []usize, states: *[256]u2, written: *usize, index: usize) anyerror!void {
        if (states[index] == 2) return;
        if (states[index] == 1) return error.RelationalGeneratedColumnCycle;
        states[index] = 1;
        for (bindings[index].plan.dependencies) |dependency| for (bindings, 0..) |binding, i| {
            if (binding.ordinal == dependency) try visit(bindings, order, states, written, i);
        };
        states[index] = 2;
        order[written.*] = index;
        written.* += 1;
    }

    pub fn deinit(self: *Set) void {
        self.alloc.free(self.modifier_ordinals);
        for (self.bindings) |*binding| binding.plan.deinit();
        self.alloc.free(self.bindings);
        self.alloc.free(self.order);
        self.alloc.free(self.read_columns);
        self.alloc.free(self.generated_columns);
        schema.freeSchema(self.alloc, self.table);
        self.alloc.destroy(self);
    }

    pub const DefaultsPolicy = enum(u8) { preserve_absence, apply_to_absent };

    pub fn applyValues(self: *const Set, alloc: Allocator, values: []Value, present: []bool) !void {
        return self.applyValuesWithPolicy(alloc, values, present, .apply_to_absent);
    }

    /// Rewrite programs bind this policy durably. Ordinary restore must never
    /// call this function: it verifies historical results without computing.
    pub fn applyValuesWithPolicy(self: *const Set, alloc: Allocator, values: []Value, present: []bool, defaults: DefaultsPolicy) !void {
        return self.applyValuesWithDefaultMask(alloc, values, present, defaults, null);
    }

    pub fn applyValuesWithDefaultMask(self: *const Set, alloc: Allocator, values: []Value, present: []bool, defaults: DefaultsPolicy, default_mask: ?[]const bool) !void {
        var budget: usize = max_allocated_bytes;
        var execution = Execution.init(alloc, &budget);
        return self.applyValuesWithExecution(&execution, values, present, defaults, default_mask);
    }

    pub fn applyValuesWithExecution(self: *const Set, execution: *Execution, values: []Value, present: []bool, defaults: DefaultsPolicy, default_mask: ?[]const bool) !void {
        if (values.len != self.table.relational_columns.len or present.len != values.len or (default_mask != null and default_mask.?.len != values.len)) return error.InvalidRelationalExpressionInput;
        // Base assignments must cross the target domain before any generated
        // expression reads them. Ignore submitted generated fields and keep
        // work proportional to the immutable dependency mask.
        for (self.modifier_ordinals) |ordinal| {
            if (!present[ordinal] or self.generated_columns[ordinal]) continue;
            values[ordinal] = try self.normalizeBinding(execution, ordinal, values[ordinal]);
        }
        for (self.order) |index| {
            const binding = &self.bindings[index];
            if (!binding.generated and (present[binding.ordinal] or defaults == .preserve_absence or (default_mask != null and !default_mask.?[binding.ordinal]))) continue;
            values[binding.ordinal] = try self.normalizeBinding(execution, binding.ordinal, try binding.plan.evaluateWithExecution(execution, values));
            present[binding.ordinal] = true;
        }
    }

    /// Mutates a request-owned DOM without reparsing it. Generated fields are
    /// output-only: any submitted value is overwritten deterministically.
    pub fn applyJson(self: *const Set, alloc: Allocator, document: *std.json.Value) !void {
        var budget: usize = max_allocated_bytes;
        var execution = Execution.init(alloc, &budget);
        return self.applyJsonWithExecution(&execution, document);
    }

    pub fn applyJsonWithExecution(self: *const Set, execution: *Execution, document: *std.json.Value) !void {
        execution.numeric.charge(0) catch |err| return executionFailure(err);
        if (document.* != .object) return error.InvalidBatchRequest;
        const alloc = execution.alloc;
        const count = self.table.relational_columns.len;
        execution.numeric.charge(count) catch |err| return executionFailure(err);
        const staging_bytes = std.math.mul(usize, count, @sizeOf(Value) + @sizeOf(bool)) catch return executionFailure(execution.numeric.limit());
        if (staging_bytes > execution.bytes.*) return executionFailure(execution.numeric.limit());
        const values = try alloc.alloc(Value, self.table.relational_columns.len);
        defer alloc.free(values);
        const present = try alloc.alloc(bool, values.len);
        defer alloc.free(present);
        execution.bytes.* -= staging_bytes;
        try self.readValues(execution, document.*, values, present, true);
        try self.applyValuesWithExecution(execution, values, present, .apply_to_absent, null);
        for (self.modifier_ordinals) |ordinal| {
            if (self.generated_columns[ordinal]) continue;
            const column = self.table.relational_columns[ordinal];
            if (document.object.getPtr(column.name)) |cell| if (values[ordinal] == .numeric) {
                cell.* = try numericJsonOutput(execution, values[ordinal].numeric);
            };
        }
        for (self.bindings) |binding| {
            const name = self.table.relational_columns[binding.ordinal].name;
            if (!binding.generated and document.object.contains(name)) continue;
            const value = values[binding.ordinal];
            const key = try allocateOutput(alloc, name.len, execution.bytes);
            @memcpy(key, name);
            try document.object.put(alloc, key, try boundedValueToJson(execution, value));
        }
    }

    /// Restore never fills defaults or repairs forged generated values. It
    /// verifies the stored canonical logical result in dependency order.
    pub fn verifyJson(self: *const Set, alloc: Allocator, document: std.json.Value) !void {
        var budget: usize = max_allocated_bytes;
        var execution = Execution.init(alloc, &budget);
        return self.verifyJsonWithExecution(&execution, document);
    }

    pub fn verifyJsonWithExecution(self: *const Set, execution: *Execution, document: std.json.Value) !void {
        try execution.charge(0);
        var scratch: ExecutionScratch = undefined;
        scratch.init(execution);
        defer scratch.deinit();
        self.verifyJsonInner(execution, document) catch |err| return scratch.failure(err);
    }

    fn verifyJsonInner(self: *const Set, execution: *Execution, document: std.json.Value) !void {
        const has_generated = for (self.bindings) |binding| {
            if (binding.generated) break true;
        } else false;
        if (!has_generated) return;
        if (document != .object) return error.InvalidBatchRequest;
        try execution.charge(self.table.relational_columns.len);
        const values = try execution.alloc.alloc(Value, self.table.relational_columns.len);
        const present = try execution.alloc.alloc(bool, values.len);
        try self.readValues(execution, document, values, present, false);
        try self.verifyValues(execution, values, present);
    }

    /// Cold restore verifies generated semantics directly from ordinal cells.
    /// The caller has already verified physical canonical bytes and checksum.
    /// Unrelated JSON, vector and blob payloads are never materialized.
    pub fn verifyRow(self: *const Set, alloc: Allocator, row: anytype) !void {
        var budget: usize = max_allocated_bytes;
        var execution = Execution.init(alloc, &budget);
        return self.verifyRowWithExecution(&execution, row);
    }

    pub fn verifyRowWithExecution(self: *const Set, execution: *Execution, row: anytype) !void {
        try execution.charge(0);
        var scratch: ExecutionScratch = undefined;
        scratch.init(execution);
        defer scratch.deinit();
        self.verifyRowInner(execution, row) catch |err| return scratch.failure(err);
    }

    fn verifyRowInner(self: *const Set, execution: *Execution, row: anytype) !void {
        const has_generated = for (self.bindings) |binding| {
            if (binding.generated) break true;
        } else false;
        if (!has_generated) return;
        try execution.charge(self.table.relational_columns.len);
        const values = try execution.alloc.alloc(Value, self.table.relational_columns.len);
        const present = try execution.alloc.alloc(bool, values.len);
        @memset(values, .null);
        @memset(present, false);
        for (self.table.relational_columns, self.read_columns, 0..) |column, read, ordinal| {
            if (!read) continue;
            const physical = row.ordinalForName(column.name) orelse continue;
            if (row.table_schema.relational_columns[physical].column_type != column.column_type or
                row.table_schema.relational_columns[physical].sql_element_type != column.sql_element_type or
                !exact.TypeModifier.eql(row.table_schema.relational_columns[physical].numeric_modifier, column.numeric_modifier)) return error.InvalidRelationalGeneratedValue;
            const cell = (try row.findCell(physical)) orelse continue;
            present[ordinal] = true;
            if (cell.is_null) continue;
            values[ordinal] = switch (column.column_type) {
                .string => .{ .string = cell.value.bytes_val },
                .blob => .{ .blob = cell.value.bytes_val },
                .integer => .{ .integer = cell.value.i64_val },
                .number => .{ .number = cell.value.f64_val },
                .numeric => .{ .numeric = cell.value.bytes_val },
                .boolean => .{ .boolean = cell.value.bool_val },
                .datetime => .{ .datetime = cell.value.u64_val },
                else => return error.InvalidRelationalGeneratedValue,
            };
        }
        try self.verifyValues(execution, values, present);
    }

    fn verifyValues(self: *const Set, execution: *Execution, values: []const Value, present: []const bool) !void {
        for (self.modifier_ordinals) |ordinal| {
            if (!present[ordinal] or values[ordinal] == .null) continue;
            if (values[ordinal] != .numeric) return error.InvalidRelationalGeneratedValue;
            _ = try normalizeNumericBinding(execution, values[ordinal].numeric, self.table.relational_columns[ordinal].numeric_modifier.?, true);
        }
        for (self.order) |index| {
            const binding = &self.bindings[index];
            if (!binding.generated) continue;
            if (!present[binding.ordinal]) return error.InvalidRelationalGeneratedValue;
            const expected = try self.normalizeBinding(execution, binding.ordinal, try binding.plan.evaluateWithExecution(execution, values));
            const actual = values[binding.ordinal];
            const equal = if (expected == .numeric and actual == .numeric)
                (valueOrderWithContext(expected, actual, false, &execution.numeric) catch |err| return executionFailure(err)) == .eq
            else
                try valuesEqual(expected, actual);
            if (!equal) return error.InvalidRelationalGeneratedValue;
        }
    }

    /// Stored expressions obey the target domain before dependent expressions
    /// execute. Restore evaluates the identical conversion, never raw float8
    /// arithmetic against an already-rounded float4 physical value.
    fn normalizeBinding(self: *const Set, execution: *Execution, ordinal: usize, value: Value) !Value {
        if (value == .null) return value;
        const column = self.table.relational_columns[ordinal];
        if (column.numeric_modifier) |modifier| {
            if (value != .numeric) return error.InvalidRelationalExpressionInput;
            return normalizeNumericBinding(execution, value.numeric, modifier, false);
        }
        const kind = column.sql_element_type orelse return value;
        return switch (kind) {
            .int16, .int32, .int64 => .{ .integer = casts.checkedInteger(value.integer, kind) catch return error.InvalidRelationalExpressionInput },
            .float32 => .{ .number = casts.floatValue(f32, .{ .float = value.number }) catch return error.InvalidRelationalExpressionInput },
            .uuid => blk: {
                const uuid = @import("../common/uuid.zig");
                const canonical = uuid.format(uuid.parse(value.string) catch return error.InvalidRelationalExpressionInput);
                if (std.mem.eql(u8, value.string, &canonical)) break :blk value;
                break :blk .{ .string = try execution.alloc.dupe(u8, &canonical) };
            },
            else => value,
        };
    }

    fn readValues(self: *const Set, execution: *Execution, document: std.json.Value, values: []Value, present: []bool, ignore_generated: bool) !void {
        for (self.table.relational_columns, values, present, 0..) |column, *value, *exists, ordinal| {
            const input = document.object.get(column.name);
            exists.* = input != null;
            value.* = .null;
            // Wide unrelated blobs/JSON/vectors never enter expression
            // decoding. The immutable mask is compiled once per schema.
            if (!self.read_columns[ordinal] or (ignore_generated and self.generated_columns[ordinal])) continue;
            if (input) |scalar| switch (column.column_type) {
                .numeric => value.* = try numericJson(execution, scalar),
                .string, .blob, .boolean, .datetime, .integer, .number => value.* = try checks.valueFromJson(execution.alloc, column.column_type, scalar, false),
                else => {},
            };
        }
    }
};

fn valuesEqual(a: Value, b: Value) !bool {
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .null => true,
        .string => |bytes| std.mem.eql(u8, bytes, b.string),
        .blob => |bytes| std.mem.eql(u8, bytes, b.blob),
        .numeric => try valueOrder(a, b, false) == .eq,
        .integer => |value| value == b.integer,
        .number => |value| value == b.number,
        .boolean => |value| value == b.boolean,
        .datetime => |value| value == b.datetime,
    };
}

/// Compile and execute a row-independent program without another evaluator or
/// type-promotion implementation. Compilation, evaluation and output share one
/// caller-owned work/byte budget. All temporary plan storage dies on return.
pub fn foldConstantJson(execution: *Execution, expression: std.json.Value) !std.json.Value {
    return foldConstantJsonImpl(execution, expression) catch |err| {
        const failure = executionFailure(err);
        if (failure == error.RelationalExpressionBudgetExceeded) execution.numeric.failure = error.SqlProgramLimitExceeded;
        return failure;
    };
}

fn foldConstantJsonImpl(execution: *Execution, expression: std.json.Value) !std.json.Value {
    try execution.numeric.charge(1);
    var memory: @import("../sql/memory_budget.zig") = .{ .backing = execution.alloc, .limit = execution.bytes.* };
    var arena = std.heap.ArenaAllocator.init(memory.allocator());
    defer arena.deinit();
    const plan: Plan = compile: {
        var compile_bytes = execution.bytes.*;
        var compilation = execution.*;
        compilation.alloc = arena.allocator();
        compilation.bytes = &compile_bytes;
        compilation.numeric.alloc = compilation.alloc;
        defer {
            const numeric_alloc = execution.numeric.alloc;
            execution.numeric = compilation.numeric;
            execution.numeric.alloc = numeric_alloc;
            // Charge real arena capacity, including failed compilation. This
            // is monotonic even when an outer arena cannot reclaim frees.
            execution.bytes.* -|= memory.peak;
        }
        var compiler: Compiler = .{ .alloc = compilation.alloc, .table = .{ .storage_mode = .relational }, .execution = &compilation };
        compiler.hash.update("antfly immutable scalar expression v1");
        const root = compiler.compile(expression, 0) catch |err| {
            if (err == error.OutOfMemory and memory.isExhausted()) return error.RelationalExpressionBudgetExceeded;
            return err;
        };
        try compilation.numeric.charge(compiler.visited);
        // The empty binding environment rejects even a column in a lazy arm.
        std.debug.assert(compiler.dependencies.items.len == 0);
        var fingerprint: [32]u8 = undefined;
        compiler.hash.final(&fingerprint);
        break :compile .{ .arena = arena, .nodes = compiler.nodes.items, .dependencies = &.{}, .fingerprint = fingerprint, .result_kind = compiler.nodes.items[root].kind, .literal_bytes = compiler.literal_bytes };
    };
    const result = try plan.evaluateWithExecution(execution, &.{});
    return boundedValueToJson(execution, result);
}

test "relational declarations constant folding owns output and shares sticky admission" {
    const a = std.testing.allocator;
    var literal = try std.json.parseFromSlice(std.json.Value, a,
        \\{"op":"literal","type":"string","value":"survives compilation"}
    , .{});
    var bytes: usize = max_allocated_bytes;
    var execution = Execution.init(a, &bytes);
    const output = try foldConstantJson(&execution, literal.value);
    defer a.free(output.string);
    literal.deinit();
    try std.testing.expectEqualStrings("survives compilation", output.string);
    try std.testing.expect(bytes < max_allocated_bytes);

    var numeric = try std.json.parseFromSlice(std.json.Value, a,
        \\{"op":"cast","type":"integer","sql_type":"int64","args":[{"op":"literal","type":"numeric","value":"9007199254740993.5"}]}
    , .{});
    defer numeric.deinit();
    const Run = struct {
        fn run(alloc: Allocator, expression: std.json.Value) !void {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            var remaining: usize = max_allocated_bytes;
            var scope = Execution.init(arena.allocator(), &remaining);
            const before = scope.numeric.remaining;
            for (0..2) |_| {
                const value = try foldConstantJson(&scope, expression);
                try std.testing.expectEqual(@as(i64, 9007199254740994), value.integer);
            }
            const decimal = try foldConstantJson(&scope, expression.object.get("args").?.array.items[0]);
            try std.testing.expectEqualStrings("9007199254740993.5", decimal.number_string);
            const text = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(),
                \\{"op":"concat","args":[{"op":"literal","type":"string","value":"left"},{"op":"literal","type":"string","value":"right"}]}
            , .{});
            const joined = try foldConstantJson(&scope, text);
            try std.testing.expectEqualStrings("leftright", joined.string);
            try std.testing.expect(scope.numeric.remaining < before);
            scope.numeric.remaining = 0;
            try std.testing.expectError(error.RelationalExpressionBudgetExceeded, foldConstantJson(&scope, expression));
            scope.numeric.remaining = before;
            try std.testing.expectError(error.RelationalExpressionBudgetExceeded, foldConstantJson(&scope, expression));
        }
    };
    try Run.run(a, numeric.value);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{numeric.value});

    var tiny: usize = 1;
    var limited = Execution.init(a, &tiny);
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, foldConstantJson(&limited, numeric.value));
    tiny = max_allocated_bytes;
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, foldConstantJson(&limited, numeric.value));

    const Poll = struct {
        fn canceled(_: ?*anyopaque) anyerror!void {
            return error.Canceled;
        }
    };
    var canceled = Execution.init(a, &tiny);
    canceled.numeric.checkpoint = Poll.canceled;
    try std.testing.expectError(error.Canceled, foldConstantJson(&canceled, numeric.value));
    canceled.numeric.checkpoint = null;
    try std.testing.expectError(error.Canceled, foldConstantJson(&canceled, numeric.value));
}

fn boundedValueToJson(execution: *Execution, value: Value) !std.json.Value {
    return switch (value) {
        .numeric => |bytes| numericJsonOutput(execution, bytes),
        .string => |bytes| blk: {
            const output = try allocateOutput(execution.alloc, bytes.len, execution.bytes);
            @memcpy(output, bytes);
            break :blk .{ .string = output };
        },
        .blob => |bytes| blk: {
            const output = try allocateOutput(execution.alloc, std.base64.standard.Encoder.calcSize(bytes.len), execution.bytes);
            break :blk .{ .string = std.base64.standard.Encoder.encode(output, bytes) };
        },
        .datetime => |datetime| blk: {
            var buffer: [40]u8 = undefined;
            const text = try std.fmt.bufPrint(&buffer, "{d}", .{datetime});
            const output = try allocateOutput(execution.alloc, text.len, execution.bytes);
            @memcpy(output, text);
            break :blk .{ .number_string = output };
        },
        else => valueToJson(execution.alloc, value),
    };
}

fn valueToJson(alloc: Allocator, value: Value) !std.json.Value {
    return switch (value) {
        .null => .null,
        .string => |bytes| .{ .string = try alloc.dupe(u8, bytes) },
        .blob => |bytes| blk: {
            const encoded = try alloc.alloc(u8, std.base64.standard.Encoder.calcSize(bytes.len));
            break :blk .{ .string = std.base64.standard.Encoder.encode(encoded, bytes) };
        },
        .integer => |integer| .{ .integer = integer },
        .number => |number| .{ .float = number },
        .numeric => |bytes| @import("../sql/numeric_storage.zig").jsonValueAlloc(alloc, bytes),
        .boolean => |boolean| .{ .bool = boolean },
        .datetime => |datetime| .{ .number_string = try std.fmt.allocPrint(alloc, "{d}", .{datetime}) },
    };
}

fn entries(value: std.json.Value) ![]const std.json.Value {
    if (value == .null) return &.{};
    if (value != .array or value.array.items.len > 256) return error.InvalidRelationalExpression;
    return value.array.items;
}

/// Identity for online schema admission. Adding, removing, or changing STORED
/// generated semantics requires a row rewrite; declaration reordering does not.
pub fn generatedFingerprint(set: ?*const Set) [32]u8 {
    const Entry = struct { name: []const u8, fingerprint: [32]u8, modifier: ?exact.TypeModifier };
    var entries_buffer: [256]Entry = undefined;
    var count: usize = 0;
    if (set) |expressions| for (expressions.bindings) |binding| {
        if (!binding.generated) continue;
        const column = expressions.table.relational_columns[binding.ordinal];
        entries_buffer[count] = .{ .name = column.name, .fingerprint = binding.plan.fingerprint, .modifier = column.numeric_modifier };
        count += 1;
    };
    const selected = entries_buffer[0..count];
    std.mem.sort(Entry, selected, {}, struct {
        fn less(_: void, a: Entry, b: Entry) bool {
            return std.mem.lessThan(u8, a.name, b.name);
        }
    }.less);
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly stored generated columns v1");
    for (selected) |entry| {
        var size: [8]u8 = undefined;
        std.mem.writeInt(u64, &size, entry.name.len, .little);
        state.update(&size);
        state.update(entry.name);
        state.update(&entry.fingerprint);
        if (entry.modifier) |modifier| {
            state.update("SQL NUMERIC generated target modifier v1\x00");
            var identity: [4]u8 = undefined;
            std.mem.writeInt(u16, identity[0..2], modifier.precision, .little);
            std.mem.writeInt(i16, identity[2..4], modifier.scale, .little);
            state.update(&identity);
        }
    }
    var result: [32]u8 = undefined;
    state.final(&result);
    return result;
}

/// Metadata has no globally fenced proof that every owner is empty. Until a
/// distributed rewrite exists, public ALTER preserves stored-generated
/// semantics even when an unfenced row-count observation appears empty.
pub fn validateSchemaUpdate(alloc: Allocator, previous_json: []const u8, next_json: []const u8) !void {
    const previous = try generatedSchemaIdentity(alloc, previous_json);
    const next = try generatedSchemaIdentity(alloc, next_json);
    if (!std.mem.eql(u8, &previous, &next)) return error.GeneratedColumnRewriteRequired;
}

fn generatedSchemaIdentity(alloc: Allocator, json: []const u8) ![32]u8 {
    if (json.len == 0) return generatedFingerprint(null);
    const raw = try std.json.parseFromSlice(std.json.Value, alloc, json, .{ .parse_numbers = false });
    defer raw.deinit();
    if (raw.value != .object) return error.InvalidSchemaUpdateRequest;
    const declarations = raw.value.object.get("generated_columns") orelse return generatedFingerprint(null);
    if (declarations == .array and declarations.array.items.len == 0) return generatedFingerprint(null);
    var parsed = try @import("table_schema_impl.zig").parseSchema(alloc, json);
    defer parsed.deinit(alloc);
    const runtime = try @import("mod.zig").deriveRelationalCheckLayout(alloc, parsed);
    errdefer schema.freeSchema(alloc, runtime);
    const set = try Set.createOwned(alloc, runtime, .null, parsed.generated_columns.?.value);
    defer set.deinit();
    return generatedFingerprint(set);
}

test "relational declarations boolean expressions three valued truth tables and lazy failures" {
    const alloc = std.testing.allocator;
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{ .{ .name = "a", .path = "a", .column_type = .boolean }, .{ .name = "b", .path = "b", .column_type = .boolean } } };
    const values = [_]Value{ .null, .{ .boolean = false }, .{ .boolean = true } };
    for ([_][]const u8{ "and", "or" }) |op| {
        const json = try std.fmt.allocPrint(alloc, "{{\"op\":\"{s}\",\"args\":[{{\"op\":\"column\",\"column\":\"a\"}},{{\"op\":\"column\",\"column\":\"b\"}}]}}", .{op});
        defer alloc.free(json);
        const parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
        defer parsed.deinit();
        var plan = try Plan.init(alloc, table, parsed.value, .boolean);
        defer plan.deinit();
        for (values) |a| for (values) |b| {
            const decisive = std.mem.eql(u8, op, "or");
            const expected: Value = if ((a != .null and a.boolean == decisive) or (b != .null and b.boolean == decisive)) .{ .boolean = decisive } else if (a == .null or b == .null) .null else .{ .boolean = !decisive };
            try std.testing.expectEqualDeep(expected, try plan.evaluate(alloc, &.{ a, b }));
        };
    }
    for ([_]struct { op: []const u8, left: bool }{ .{ .op = "and", .left = false }, .{ .op = "or", .left = true } }) |case| {
        const json = try std.fmt.allocPrint(alloc, "{{\"op\":\"{s}\",\"args\":[{{\"op\":\"literal\",\"type\":\"boolean\",\"value\":{s}}},{{\"op\":\"eq\",\"args\":[{{\"op\":\"divide\",\"args\":[{{\"op\":\"literal\",\"type\":\"integer\",\"value\":1}},{{\"op\":\"literal\",\"type\":\"integer\",\"value\":0}}]}},{{\"op\":\"literal\",\"type\":\"integer\",\"value\":0}}]}}]}}", .{ case.op, if (case.left) "true" else "false" });
        defer alloc.free(json);
        const parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
        defer parsed.deinit();
        var plan = try Plan.init(alloc, table, parsed.value, .boolean);
        defer plan.deinit();
        try std.testing.expectEqual(case.left, (try plan.evaluate(alloc, &.{})).boolean);
    }
    const negated = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"op":"not","args":[{"op":"column","column":"a"}]}
    , .{});
    defer negated.deinit();
    var not_plan = try Plan.init(alloc, table, negated.value, .boolean);
    defer not_plan.deinit();
    for (values) |a| try std.testing.expectEqualDeep(if (a == .null) Value.null else Value{ .boolean = !a.boolean }, try not_plan.evaluate(alloc, &.{a}));
}

test "relational declarations expression comparisons exact integer null distinctness and normalized collation" {
    const alloc = std.testing.allocator;
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational };
    for ([_]struct { json: []const u8, expected: Value }{
        .{ .json = "{\"op\":\"gt\",\"args\":[{\"op\":\"literal\",\"type\":\"integer\",\"value\":9007199254740993},{\"op\":\"literal\",\"type\":\"integer\",\"value\":9007199254740992}]}", .expected = .{ .boolean = true } },
        .{ .json = "{\"op\":\"eq\",\"args\":[{\"op\":\"literal\",\"type\":\"integer\"},{\"op\":\"literal\",\"type\":\"integer\"}]}", .expected = .null },
        .{ .json = "{\"op\":\"is_distinct\",\"args\":[{\"op\":\"literal\",\"type\":\"integer\"},{\"op\":\"literal\",\"type\":\"integer\",\"value\":0}]}", .expected = .{ .boolean = true } },
        .{ .json = "{\"op\":\"is_not_distinct\",\"args\":[{\"op\":\"literal\",\"type\":\"integer\"},{\"op\":\"literal\",\"type\":\"integer\"}]}", .expected = .{ .boolean = true } },
        .{ .json = "{\"op\":\"is_null\",\"args\":[{\"op\":\"literal\",\"type\":\"integer\"}]}", .expected = .{ .boolean = true } },
        .{ .json = "{\"op\":\"is_not_null\",\"args\":[{\"op\":\"literal\",\"type\":\"integer\",\"value\":0}]}", .expected = .{ .boolean = true } },
        .{ .json = "{\"op\":\"lt\",\"args\":[{\"op\":\"literal\",\"type\":\"blob\",\"value\":\"AA==\"},{\"op\":\"literal\",\"type\":\"blob\",\"value\":\"AQ==\"}]}", .expected = .{ .boolean = true } },
        .{ .json = "{\"op\":\"eq\",\"args\":[{\"op\":\"literal\",\"type\":\"number\",\"value\":-0.0},{\"op\":\"literal\",\"type\":\"number\",\"value\":0.0}]}", .expected = .{ .boolean = true } },
    }) |case| {
        const parsed = try std.json.parseFromSlice(std.json.Value, alloc, case.json, .{ .parse_numbers = false });
        defer parsed.deinit();
        var plan = try Plan.init(alloc, table, parsed.value, .boolean);
        defer plan.deinit();
        try std.testing.expectEqualDeep(case.expected, try plan.evaluate(alloc, &.{}));
    }
    var identity: ?[32]u8 = null;
    for ([_][]const u8{ "ci", "case_insensitive", "ANTFLY.CASE_INSENSITIVE" }) |collation| {
        const json = try std.fmt.allocPrint(alloc, "{{\"op\":\"eq\",\"collation\":\"{s}\",\"args\":[{{\"op\":\"literal\",\"type\":\"string\",\"value\":\"AbC\"}},{{\"op\":\"literal\",\"type\":\"string\",\"value\":\"abc\"}}]}}", .{collation});
        defer alloc.free(json);
        const parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
        defer parsed.deinit();
        var plan = try Plan.init(alloc, table, parsed.value, .boolean);
        defer plan.deinit();
        try std.testing.expect((try plan.evaluate(alloc, &.{})).boolean);
        if (identity) |previous| try std.testing.expectEqualSlices(u8, &previous, &plan.fingerprint);
        identity = plan.fingerprint;
    }
}

test "relational declarations comparison work shares the row byte budget even for borrowed values" {
    const alloc = std.testing.allocator;
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational };
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"op":"eq","args":[{"op":"literal","type":"string","value":"0123456789"},{"op":"literal","type":"string","value":"0123456789"}]}
    , .{});
    defer parsed.deinit();
    var plan = try Plan.init(alloc, table, parsed.value, .boolean);
    defer plan.deinit();
    const empty: std.json.Value = .{ .object = .empty };
    var budget: usize = 19;
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, plan.evaluateJsonWithBudget(alloc, empty, &budget));
    budget = 20;
    try std.testing.expect((try plan.evaluateJsonWithBudget(alloc, empty, &budget)).boolean);
    try std.testing.expectEqual(@as(usize, 0), budget);
}

test "relational declarations CHECK expression dependency projection deterministic activation failure and strict writes" {
    const alloc = std.testing.allocator;
    const impl = @import("table_schema_impl.zig");
    var parsed = try impl.parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","checks":[{"name":"ratio","expression":{"op":"gte","args":[{"op":"divide","args":[{"op":"column","column":"x"},{"op":"column","column":"y"}]},{"op":"literal","type":"integer","value":0}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":["integer","null"]},"y":{"type":["integer","null"]},"unrelated":{"type":"string"}},"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);
    var compiled = try impl.CompiledValidationPlan.init(alloc, parsed);
    defer compiled.deinit(alloc);
    const set = compiled.checks.?;
    try std.testing.expectEqual(@as(usize, 2), set.dependency_fields.len);
    for (set.dependency_fields) |field| try std.testing.expect(!std.mem.eql(u8, field, "unrelated"));
    for ([_]struct { json: []const u8, bad: bool }{ .{ .json = "{\"x\":10,\"y\":2}", .bad = false }, .{ .json = "{\"x\":-10,\"y\":2}", .bad = true }, .{ .json = "{\"x\":null,\"y\":2}", .bad = false }, .{ .json = "{}", .bad = false } }) |case| {
        const row = try std.json.parseFromSlice(std.json.Value, alloc, case.json, .{ .parse_numbers = false });
        defer row.deinit();
        try std.testing.expectEqual(case.bad, (try set.firstViolationJson(alloc, row.value)) != null);
    }
    const invalid = try std.json.parseFromSlice(std.json.Value, alloc, "{\"x\":1,\"y\":0}", .{});
    defer invalid.deinit();
    try std.testing.expectError(error.RelationalExpressionDivisionByZero, set.firstViolationJson(alloc, invalid.value));
    const failure = (try set.firstFailureJson(alloc, invalid.value)).?;
    try std.testing.expectEqual(@as(usize, 0), failure.index);
    try std.testing.expectEqual(error.RelationalExpressionDivisionByZero, failure.reason);
}

test "relational declarations CHECK expression rejects ambiguous shapes nonboolean results and rounded integer literals" {
    const alloc = std.testing.allocator;
    const impl = @import("table_schema_impl.zig");
    for ([_][]const u8{
        "{\"name\":\"bad\",\"column\":\"x\",\"op\":\"eq\",\"expression\":{\"op\":\"literal\",\"type\":\"boolean\",\"value\":true}}",
        "{\"name\":\"bad\",\"expression\":{\"op\":\"literal\",\"type\":\"integer\",\"value\":1}}",
        "{\"name\":\"bad\",\"expression\":{\"op\":\"eq\",\"args\":[{\"op\":\"column\",\"column\":\"x\"},{\"op\":\"literal\",\"type\":\"integer\",\"value\":1.00000000000000001}]}}",
        "{\"name\":\"bad\",\"expression\":{\"op\":\"and\",\"args\":[{\"op\":\"literal\",\"type\":\"integer\",\"value\":1},{\"op\":\"literal\",\"type\":\"boolean\",\"value\":true}]}}",
        "{\"name\":\"bad\",\"expression\":{\"op\":\"eq\",\"collation\":\"ci\",\"args\":[{\"op\":\"column\",\"column\":\"x\"},{\"op\":\"literal\",\"type\":\"integer\",\"value\":1}]}}",
    }) |definition| {
        const json = try std.fmt.allocPrint(alloc, "{{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"checks\":[{s}],\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"x\":{{\"type\":\"integer\"}}}},\"additionalProperties\":false}}}}}}}}", .{definition});
        defer alloc.free(json);
        try std.testing.expectError(error.InvalidSchemaUpdateRequest, impl.parseSchemaUpdateRequest(alloc, json));
    }
}

fn checkExpressionAllocationFailure(alloc: Allocator) !void {
    const impl = @import("table_schema_impl.zig");
    var parsed = try impl.parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","checks":[{"name":"valid","expression":{"op":"or","args":[{"op":"is_null","args":[{"op":"column","column":"x"}]},{"op":"gt","args":[{"op":"column","column":"x"},{"op":"literal","type":"integer","value":0}]}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":["integer","null"]}},"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);
    var compiled = try impl.CompiledValidationPlan.init(alloc, parsed);
    defer compiled.deinit(alloc);
    const row = try std.json.parseFromSlice(std.json.Value, alloc, "{\"x\":2}", .{});
    defer row.deinit();
    try std.testing.expectEqual(@as(?usize, null), try compiled.checks.?.firstViolationJson(alloc, row.value));
}

test "relational declarations mixed CHECKs share JSON and ordinal row quotas with sticky activation failures" {
    const Run = struct {
        fn run(alloc: Allocator) !void {
            const impl = @import("table_schema_impl.zig");
            var table = try impl.parseSchema(alloc,
                \\{"version":1,"storage_mode":"relational","default_type":"row","checks":[{"name":"positive","column":"n","op":"gte","value":0},{"name":"label","column":"s","op":"eq","value":"keep"},{"name":"small","expression":{"op":"lt","args":[{"op":"column","column":"n"},{"op":"literal","type":"numeric","value":"10"}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":["number","null"],"x-antfly-sql-type":"numeric"},"s":{"type":"string"}},"additionalProperties":false}}}}
            );
            defer table.deinit(alloc);
            var compiled = try impl.CompiledValidationPlan.init(alloc, table);
            defer compiled.deinit(alloc);
            const set = compiled.checks.?;
            const json = try std.json.parseFromSlice(std.json.Value, alloc, "{\"n\":1.25,\"s\":\"keep\"}", .{ .parse_numbers = false });
            defer json.deinit();
            const number = try @import("../sql/numeric_storage.zig").encodeJsonAlloc(alloc, json.value.object.get("n").?);
            defer alloc.free(number);
            var cells: [2]codec.Cell = undefined;
            const n = set.layout.ordinalForName(set.table.relational_columns, "n").?;
            const s = set.layout.ordinalForName(set.table.relational_columns, "s").?;
            cells[n] = .{ .ordinal = @intCast(n), .path = "n", .value_type = .bytes_val, .is_numeric = true, .value = .{ .bytes_val = number } };
            cells[s] = .{ .ordinal = @intCast(s), .path = "s", .value_type = .bytes_val, .value = .{ .bytes_val = "keep" } };
            const bytes = try codec.serializeOrdinal(alloc, set.table.version, set.table.relational_columns, &cells, @splat(0));
            defer alloc.free(bytes);
            const row = try codec.ordinalRowView(bytes, set.table, &set.layout);
            for ([_]bool{ false, true }) |cold| {
                var budget: usize = max_allocated_bytes;
                var execution = Execution.init(alloc, &budget);
                const result = if (cold) try set.firstViolationRowWithExecution(&execution, row) else try set.firstViolationJsonWithExecution(&execution, json.value);
                try std.testing.expectEqual(@as(?usize, null), result);
                const used = 8 * 1024 * 1024 - execution.numeric.remaining;
                try std.testing.expect(used > 0 and budget < max_allocated_bytes);
                try std.testing.expectEqual(alloc.ptr, execution.alloc.ptr);
                try std.testing.expectEqual(alloc.vtable, execution.alloc.vtable);
                execution = Execution.init(alloc, &budget);
                execution.numeric.remaining = used - 1;
                const limited = if (cold) set.firstViolationRowWithExecution(&execution, row) else set.firstViolationJsonWithExecution(&execution, json.value);
                if (limited) |_| return error.TestExpectedError else |err| {
                    if (err == error.OutOfMemory) return err;
                    try std.testing.expectEqual(error.RelationalExpressionBudgetExceeded, err);
                }
                execution.numeric.remaining = 8 * 1024 * 1024;
                const activation = if (cold) set.firstFailureRowWithExecution(&execution, row) else set.firstFailureJsonWithExecution(&execution, json.value);
                try std.testing.expectError(error.RelationalExpressionBudgetExceeded, activation);
                budget = 1;
                execution = Execution.init(alloc, &budget);
                const tiny = if (cold) set.firstViolationRowWithExecution(&execution, row) else set.firstViolationJsonWithExecution(&execution, json.value);
                try std.testing.expectError(error.RelationalExpressionBudgetExceeded, tiny);
                const Cancel = struct {
                    fn poll(_: ?*anyopaque) anyerror!void {
                        return error.Canceled;
                    }
                };
                budget = max_allocated_bytes;
                execution = Execution.init(alloc, &budget);
                execution.numeric.checkpoint = Cancel.poll;
                const canceled = if (cold) set.firstFailureRowWithExecution(&execution, row) else set.firstFailureJsonWithExecution(&execution, json.value);
                try std.testing.expectError(error.Canceled, canceled);
                execution.numeric.checkpoint = null;
                try std.testing.expectError(error.Canceled, execution.charge(0));
            }
        }
    };
    try Run.run(std.testing.allocator);
    var stable = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{});
}

test "relational declarations borrowed CHECK byte admission remains sticky without allocations" {
    const alloc = std.testing.allocator;
    const impl = @import("table_schema_impl.zig");
    var table = try impl.parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","checks":[{"name":"equal","expression":{"op":"eq","args":[{"op":"literal","type":"string","value":"0123456789"},{"op":"literal","type":"string","value":"0123456789"}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"s":{"type":"string"}},"additionalProperties":false}}}}
    );
    defer table.deinit(alloc);
    var compiled = try impl.CompiledValidationPlan.init(alloc, table);
    defer compiled.deinit(alloc);
    const input: std.json.Value = .{ .object = .empty };
    // Both operands are immutable plan literals. There is no runtime arena
    // allocation whose failure could accidentally establish stickiness.
    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    var budget: usize = 20;
    var execution = Execution.init(failing.allocator(), &budget);
    try std.testing.expectEqual(@as(?usize, null), try compiled.checks.?.firstViolationJsonWithExecution(&execution, input));
    try std.testing.expectEqual(@as(usize, 0), budget);
    budget = 19;
    execution = Execution.init(failing.allocator(), &budget);
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, compiled.checks.?.firstFailureJsonWithExecution(&execution, input));
    budget = max_allocated_bytes;
    execution.numeric.remaining = 8 * 1024 * 1024;
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, compiled.checks.?.firstViolationJsonWithExecution(&execution, input));
    try std.testing.expectEqual(@as(usize, 0), failing.allocations);
}

test "relational declarations CHECK expressions release every allocation failure" {
    // Arena remaps depend on surrounding addresses and can change allocation
    // counts between fault indexes. Exercise the allocating fallback with a
    // deterministic backing owner instead of intermittently skipping indexes.
    var stable = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), checkExpressionAllocationFailure, .{});
}

test "relational declarations scalar expressions checked arithmetic lazy null and ordinal independent identity" {
    const alloc = std.testing.allocator;
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{ .{ .name = "x", .path = "x", .column_type = .integer }, .{ .name = "y", .path = "y", .column_type = .integer } } };
    const expression = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"op":"add","args":[{"op":"column","column":"x"},{"op":"literal","type":"integer","value":"1"}]}
    , .{});
    defer expression.deinit();
    var plan = try Plan.init(alloc, table, expression.value, .integer);
    defer plan.deinit();
    try std.testing.expectEqual(@as(i64, 42), (try plan.evaluate(alloc, &.{ .{ .integer = 41 }, .null })).integer);
    try std.testing.expectEqual(Value.null, try plan.evaluate(alloc, &.{ .null, .null }));
    try std.testing.expectError(error.RelationalExpressionOverflow, plan.evaluate(alloc, &.{ .{ .integer = std.math.maxInt(i64) }, .null }));
    var reordered = table;
    reordered.version = 22;
    reordered.relational_columns = &.{ table.relational_columns[1], table.relational_columns[0] };
    var second = try Plan.init(alloc, reordered, expression.value, .integer);
    defer second.deinit();
    try std.testing.expectEqualSlices(u8, &plan.fingerprint, &second.fingerprint);
    const lazy = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"op":"coalesce","args":[{"op":"literal","type":"integer","value":7},{"op":"divide","args":[{"op":"literal","type":"integer","value":1},{"op":"literal","type":"integer","value":0}]}]}
    , .{});
    defer lazy.deinit();
    var lazy_plan = try Plan.init(alloc, table, lazy.value, .integer);
    defer lazy_plan.deinit();
    try std.testing.expectEqual(@as(i64, 7), (try lazy_plan.evaluate(alloc, &.{})).integer);
}

test "relational declarations generated graph defaults explicit null and strict restore" {
    const alloc = std.testing.allocator;
    const impl = @import("table_schema_impl.zig");
    var table = try impl.parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","column_defaults":[{"column":"x","expression":{"op":"literal","type":"integer","value":"4"}}],"generated_columns":[{"column":"z","expression":{"op":"add","args":[{"op":"column","column":"y"},{"op":"literal","type":"integer","value":2}]}},{"column":"y","expression":{"op":"multiply","args":[{"op":"column","column":"x"},{"op":"literal","type":"integer","value":3}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":["integer","null"]},"y":{"type":["integer","null"]},"z":{"type":["integer","null"]}},"required":["x","y","z"],"additionalProperties":false}}}}
    );
    defer table.deinit(alloc);
    var compiled = try impl.CompiledValidationPlan.init(alloc, table);
    defer compiled.deinit(alloc);
    const set = compiled.expressions.?;
    var empty = try std.json.parseFromSlice(std.json.Value, alloc, "{}", .{});
    defer empty.deinit();
    try set.applyJson(empty.arena.allocator(), &empty.value);
    try std.testing.expectEqual(@as(i64, 4), empty.value.object.get("x").?.integer);
    try std.testing.expectEqual(@as(i64, 12), empty.value.object.get("y").?.integer);
    try std.testing.expectEqual(@as(i64, 14), empty.value.object.get("z").?.integer);
    try set.verifyJson(alloc, empty.value);
    try empty.value.object.put(empty.arena.allocator(), "z", .{ .integer = 99 });
    try std.testing.expectError(error.InvalidRelationalGeneratedValue, set.verifyJson(alloc, empty.value));
    var explicit_null = try std.json.parseFromSlice(std.json.Value, alloc, "{\"x\":null,\"y\":\"ignored\"}", .{});
    defer explicit_null.deinit();
    try set.applyJson(explicit_null.arena.allocator(), &explicit_null.value);
    try std.testing.expectEqual(std.json.Value.null, explicit_null.value.object.get("x").?);
    try std.testing.expectEqual(std.json.Value.null, explicit_null.value.object.get("y").?);
    try std.testing.expectEqual(std.json.Value.null, explicit_null.value.object.get("z").?);
    try impl.validateDocumentJson(alloc, table, "{}");
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, impl.parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","generated_columns":[{"column":"x","expression":{"op":"column","column":"y"}},{"column":"y","expression":{"op":"column","column":"x"}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"integer"},"y":{"type":"integer"}},"additionalProperties":false}}}}
    ));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, impl.parseSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","column_defaults":[{"column":"x","expression":{"op":"column","column":"x"}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"integer"}},"additionalProperties":false}}}}
    ));
}

test "relational declarations scalar expression rejects mixed types unknown fields and bounded allocation" {
    const alloc = std.testing.allocator;
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{.{ .name = "x", .path = "x", .column_type = .string }} };
    inline for (.{
        "{\"op\":\"now\"}",
        "{\"op\":\"column\",\"column\":\"x\",\"args\":[]}",
        "{\"op\":\"add\",\"args\":[{\"op\":\"literal\",\"type\":\"integer\",\"value\":1},{\"op\":\"literal\",\"type\":\"number\",\"value\":1}]}",
    }) |json| {
        const parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
        defer parsed.deinit();
        if (Plan.init(alloc, table, parsed.value, .integer)) |good| {
            var unexpected = good;
            unexpected.deinit();
            return error.TestUnexpectedResult;
        } else |_| {}
    }
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"op":"concat","args":[{"op":"column","column":"x"},{"op":"column","column":"x"}]}
    , .{});
    defer parsed.deinit();
    var plan = try Plan.init(alloc, table, parsed.value, .string);
    defer plan.deinit();
    const bytes = try alloc.alloc(u8, max_output_bytes);
    defer alloc.free(bytes);
    @memset(bytes, 'a');
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, plan.evaluate(alloc, &.{.{ .string = bytes }}));
}

test "relational declarations scalar compiler depth and node budgets are enforced before evaluation" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const parsed = try std.json.parseFromSlice(std.json.Value, owned, "{\"op\":\"literal\",\"type\":\"integer\",\"value\":1}", .{});
    var value = parsed.value;
    for (0..max_depth) |_| {
        var args = std.array_list.Managed(std.json.Value).init(owned);
        try args.append(value);
        var object = std.json.ObjectMap.empty;
        try object.put(owned, "op", .{ .string = "negate" });
        try object.put(owned, "args", .{ .array = args });
        value = .{ .object = object };
    }
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational };
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, Plan.init(alloc, table, value, .integer));
    var inner_args = std.array_list.Managed(std.json.Value).init(owned);
    for (0..32) |_| try inner_args.append(parsed.value);
    var inner = std.json.ObjectMap.empty;
    try inner.put(owned, "op", .{ .string = "coalesce" });
    try inner.put(owned, "args", .{ .array = inner_args });
    var outer_args = std.array_list.Managed(std.json.Value).init(owned);
    for (0..4) |_| try outer_args.append(.{ .object = inner });
    var outer = std.json.ObjectMap.empty;
    try outer.put(owned, "op", .{ .string = "coalesce" });
    try outer.put(owned, "args", .{ .array = outer_args });
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, Plan.init(alloc, table, .{ .object = outer }, .integer));
}

test "relational declarations generated FK assignment actions cannot overwrite derived child columns" {
    const alloc = std.testing.allocator;
    const impl = @import("table_schema_impl.zig");
    const json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","generated_columns":[{"column":"child","expression":{"op":"column","column":"source"}}],"foreign_keys":[{"name":"parent","child_columns":["child"],"parent_table":"parents","parent_columns":["id"],"on_update":"cascade"}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"source":{"type":"integer"},"child":{"type":["integer","null"]}},"additionalProperties":false}}}}
    ;
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, impl.parseSchema(alloc, json));
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
    defer parsed.deinit();
    const foreign_key = &parsed.value.object.getPtr("foreign_keys").?.array.items[0];
    try foreign_key.object.put(parsed.arena.allocator(), "on_update", .{ .string = "no_action" });
    try foreign_key.object.put(parsed.arena.allocator(), "on_delete", .{ .string = "set_null" });
    const deletion = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    defer alloc.free(deletion);
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, impl.parseSchema(alloc, deletion));
}

test "relational declarations metadata generated update admission precedes catalog publication" {
    const alloc = std.testing.allocator;
    const json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","column_defaults":[{"column":"x","expression":{"op":"literal","type":"integer","value":"2"}}],"generated_columns":[{"column":"y","expression":{"op":"column","column":"x"}},{"column":"z","expression":{"op":"column","column":"y"}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"integer"},"y":{"type":"integer"},"z":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const manager = @import("antfly_server_test_sources").local_test_sources.metadata_table_manager;
    const tables = @import("antfly_server_test_sources").local_test_sources.api_tables;
    const table: manager.TableRecord = .{ .table_id = 7, .name = "rows", .schema_json = json, .indexes_json = "{}" };
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{ .parse_numbers = false });
    defer parsed.deinit();
    const arena = parsed.arena.allocator();
    const declarations = parsed.value.object.getPtr("generated_columns").?.array.items;
    std.mem.swap(std.json.Value, &declarations[0], &declarations[1]);
    const default_value = parsed.value.object.getPtr("column_defaults").?.array.items[0].object.getPtr("expression").?.object.getPtr("value").?;
    default_value.* = .{ .string = "4" };
    const reordered = try std.json.Stringify.valueAlloc(arena, parsed.value, .{});
    const updated = try tables.applySchemaUpdateRecord(alloc, &table, reordered);
    defer manager.freeTable(alloc, updated);
    const y_expression = declarations[1].object.getPtr("expression").?;
    y_expression.* = (try std.json.parseFromSlice(std.json.Value, arena, "{\"op\":\"literal\",\"type\":\"integer\",\"value\":99}", .{})).value;
    const changed = try std.json.Stringify.valueAlloc(arena, parsed.value, .{});
    try std.testing.expectError(error.GeneratedColumnRewriteRequired, tables.applySchemaUpdateRecord(alloc, &table, changed));
    try std.testing.expectEqualStrings(json, table.schema_json);
    _ = parsed.value.object.orderedRemove("generated_columns");
    const removed = try std.json.Stringify.valueAlloc(arena, parsed.value, .{});
    try std.testing.expectError(error.GeneratedColumnRewriteRequired, tables.applySchemaUpdateRecord(alloc, &table, removed));
    const plain: manager.TableRecord = .{ .table_id = 7, .name = "rows", .schema_json = removed, .indexes_json = "{}" };
    try std.testing.expectError(error.GeneratedColumnRewriteRequired, tables.applySchemaUpdateRecord(alloc, &plain, json));
}

test "relational declarations omitted scalar literal value is typed NULL for generated SDKs" {
    const alloc = std.testing.allocator;
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational };
    const missing = try std.json.parseFromSlice(std.json.Value, alloc, "{\"op\":\"literal\",\"type\":\"integer\"}", .{});
    defer missing.deinit();
    const explicit = try std.json.parseFromSlice(std.json.Value, alloc, "{\"op\":\"literal\",\"type\":\"integer\",\"value\":null}", .{});
    defer explicit.deinit();
    var left = try Plan.init(alloc, table, missing.value, .integer);
    defer left.deinit();
    var right = try Plan.init(alloc, table, explicit.value, .integer);
    defer right.deinit();
    try std.testing.expectEqual(Value.null, try left.evaluate(alloc, &.{}));
    try std.testing.expectEqualSlices(u8, &left.fingerprint, &right.fingerprint);
}

test "relational declarations public NUMERIC annotation validates scope and constrained arrays" {
    const a = std.testing.allocator;
    const public_schema = @import("mod.zig");
    for ([_][]const u8{
        "{\"type\":\"number\",\"x-antfly-sql-numeric-modifier\":{\"precision\":4,\"scale\":2}}",
        "{\"type\":\"integer\",\"x-antfly-sql-type\":\"integer\",\"x-antfly-sql-numeric-modifier\":{\"precision\":4,\"scale\":2}}",
        "{\"type\":\"string\",\"x-antfly-sql-numeric-modifier\":{\"precision\":4,\"scale\":2}}",
        "{\"type\":\"object\",\"properties\":{\"nested\":{\"type\":\"number\",\"x-antfly-sql-type\":\"numeric\",\"x-antfly-sql-numeric-modifier\":{\"precision\":4,\"scale\":2}}}}",
    }) |property| {
        const json = try std.fmt.allocPrint(a, "{{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"n\":{s}}},\"additionalProperties\":false}}}}}}}}", .{property});
        defer a.free(json);
        try std.testing.expectError(error.InvalidSchemaUpdateRequest, public_schema.CompiledTableValidator.init(a, json));
    }
    for ([_][]const u8{
        "{\"precision\":0,\"scale\":2}",
        "{\"precision\":4,\"scale\":1001}",
        "{\"precision\":\"4\",\"scale\":2}",
        "{\"precision\":4}",
        "{\"precision\":4,\"scale\":2,\"unknown\":0}",
        "null",
    }) |modifier| {
        const json = try std.fmt.allocPrint(a, "{{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"n\":{{\"type\":\"number\",\"x-antfly-sql-type\":\"numeric\",\"x-antfly-sql-numeric-modifier\":{s}}}}},\"additionalProperties\":false}}}}}}}}", .{modifier});
        defer a.free(json);
        try std.testing.expectError(error.InvalidSchemaUpdateRequest, public_schema.CompiledTableValidator.init(a, json));
    }
    var validator = try public_schema.CompiledTableValidator.init(a,
        \\{"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":"sql_array","x-antfly-sql-type":"numeric","x-antfly-sql-numeric-modifier":{"precision":4,"scale":2}}},"additionalProperties":false}}}}
    );
    defer validator.deinit(a);
    var document = try std.json.parseFromSlice(std.json.Value, a,
        \\{"a":{"dimensions":[{"length":3,"lower_bound":-7}],"values":["1.245","NaN",null],"sql_nulls":[false,false,true]}}
    , .{});
    defer document.deinit();
    try std.testing.expectError(error.InvalidBatchRequest, validator.validateValue(a, &document.value));
    try validator.prepareValue(document.arena.allocator(), a, &document.value);
    try std.testing.expectEqualStrings("1.25", document.value.object.get("a").?.object.get("values").?.array.items[0].string);
    try std.testing.expectEqual(@as(i64, -7), document.value.object.get("a").?.object.get("dimensions").?.array.items[0].object.get("lower_bound").?.integer);
    try validator.validateValue(a, &document.value);
    const runtime = try public_schema.deriveRuntimeTableSchema(a, validator.schema);
    defer schema.freeSchema(a, runtime);
    try std.testing.expect(runtime.requires_numeric_modifiers);
    try std.testing.expectEqual(@as(u16, 4), runtime.relational_columns[0].numeric_modifier.?.precision);
    try std.testing.expectEqual(@as(usize, 0), validator.restore.properties.len);
}

test "relational declarations public NUMERIC modifiers preserve PostgreSQL assignment ordering and epochs" {
    const a = std.testing.allocator;
    const Entry = struct { use_default: bool = false, input: ?[]const u8 = null, expected: ?[]const ?[]const u8 = null, @"error": ?[]const u8 = null };
    const fixture = try std.json.parseFromSlice(struct { entries: []const Entry }, a, @embedFile("../sql/fixtures/sql_numeric_assignment_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    const Run = struct {
        fn run(alloc: Allocator, cases: []const Entry) !void {
            const public_schema = @import("mod.zig");
            var validator = try public_schema.CompiledTableValidator.init(alloc,
                \\{"version":1,"storage_mode":"relational","default_type":"row","column_defaults":[{"column":"base","expression":{"op":"literal","type":"numeric","value":"1.245"}}],"generated_columns":[{"column":"narrow","expression":{"op":"column","column":"base"}},{"column":"total","expression":{"op":"add","args":[{"op":"column","column":"base"},{"op":"column","column":"narrow"}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"base":{"type":["number","null"],"x-antfly-sql-type":"numeric","x-antfly-sql-numeric-modifier":{"precision":4,"scale":2}},"narrow":{"type":["number","null"],"x-antfly-sql-type":"numeric","x-antfly-sql-numeric-modifier":{"precision":3,"scale":1}},"total":{"type":["number","null"],"x-antfly-sql-type":"numeric","x-antfly-sql-numeric-modifier":{"precision":5,"scale":2}}},"additionalProperties":false}}}}
            );
            defer validator.deinit(alloc);
            const runtime = try public_schema.deriveRuntimeTableSchema(alloc, validator.schema);
            defer schema.freeSchema(alloc, runtime);
            try std.testing.expect(runtime.requires_numeric_modifiers);
            const old_catalog: @import("../storage/db/table_catalog.zig").Catalog = .{ .mode_initialized = true, .storage_mode = .relational, .active_schema_version = runtime.version, .schema_format_version = 22 };
            try std.testing.expectError(error.UnsupportedTableCapabilityVersion, old_catalog.validateForSchema(runtime));
            for (cases) |entry| {
                var document = try std.json.parseFromSlice(std.json.Value, alloc, "{}", .{});
                defer document.deinit();
                if (!entry.use_default) try document.value.object.put(document.arena.allocator(), "base", if (entry.input) |text| .{ .string = text } else .null);
                const result = validator.prepareValue(document.arena.allocator(), alloc, &document.value);
                if (entry.@"error" != null) {
                    if (result) |_| return error.TestExpectedError else |err| {
                        if (err == error.OutOfMemory) return err;
                        try std.testing.expect(err == error.InvalidBatchRequest or err == error.RelationalExpressionOverflow);
                    }
                    continue;
                }
                try result;
                for ([_][]const u8{ "base", "narrow", "total" }, entry.expected.?) |name, expected| {
                    const value = document.value.object.get(name).?;
                    if (expected) |text| try std.testing.expectEqualStrings(text, if (value == .number_string) value.number_string else value.string) else try std.testing.expectEqual(std.json.Value.null, value);
                }
                try validator.validateValue(alloc, &document.value);
            }
        }
    };
    try Run.run(a, fixture.value.entries);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{fixture.value.entries});
}

test "relational declarations physical restore discharges unconstrained SQL domains without materialization" {
    const a = std.testing.allocator;
    const public_schema = @import("mod.zig");
    var validator = try public_schema.CompiledTableValidator.init(a,
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"t":{"type":"string","x-antfly-sql-type":"text"},"i":{"type":"integer","x-antfly-sql-type":"int16"},"f":{"type":"number","x-antfly-sql-type":"float32"},"n":{"type":"number","x-antfly-sql-type":"numeric"},"a":{"type":"sql_array","x-antfly-sql-type":"numeric"}},"additionalProperties":false}}}}
    );
    defer validator.deinit(a);
    try std.testing.expect(!validator.restore.full_root);
    try std.testing.expectEqual(@as(usize, 0), validator.restore.properties.len);
    const table = try public_schema.deriveRuntimeTableSchema(a, validator.schema);
    defer schema.freeSchema(a, table);
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    var context: exact.Context = .{ .alloc = a };
    var parsed = try exact.parse(&context, "1.25");
    defer parsed.deinit();
    const arrays = @import("../sql/array_value.zig");
    const elements = [_]arrays.Element{ arrays.Element.typedNumeric(&parsed.value), .{} };
    const array = try arrays.Value.init(.numeric, &.{.{ .length = 2, .lower = -3 }}, &elements, .{});
    const payload = try @import("../sql/array_storage.zig").encodeAlloc(a, array, .{});
    defer a.free(payload);
    const ordinal = layout.ordinalForName(table.relational_columns, "a").?;
    const cell: codec.Cell = .{ .ordinal = @intCast(ordinal), .path = "a", .value_type = .bytes_val, .sql_array_element_type = .numeric, .value = .{ .bytes_val = payload } };
    const bytes = try codec.serializeOrdinal(a, table.version, table.relational_columns, &.{cell}, @splat(0));
    defer a.free(bytes);
    try codec.validateOrdinalWithLayout(bytes, table, &layout);
    const row = try codec.ordinalRowView(bytes, table, &layout);
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var budget: usize = 0;
    var execution = Execution.init(failing.allocator(), &budget);
    try validator.validateRelationalRestoreFieldsWithExecution(&execution, row);
    try std.testing.expectEqual(@as(usize, 0), failing.allocations);
    var constrained = try public_schema.CompiledTableValidator.init(a,
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"number","x-antfly-sql-type":"numeric","minimum":1},"t":{"type":"string","x-antfly-sql-type":"text","maxLength":10}},"additionalProperties":false}}}}
    );
    defer constrained.deinit(a);
    try std.testing.expectEqual(@as(usize, 2), constrained.restore.properties.len);
}

test "relational declarations physical restore shares generated CHECK and field constraint admission" {
    const Run = struct {
        fn run(alloc: Allocator) !void {
            var validator = try @import("mod.zig").CompiledTableValidator.init(alloc,
                \\{"version":1,"storage_mode":"relational","default_type":"row","generated_columns":[{"column":"g","expression":{"op":"add","args":[{"op":"column","column":"n"},{"op":"literal","type":"numeric","value":"1"}]}}],"checks":[{"name":"positive","column":"n","op":"gte","value":0}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"number","x-antfly-sql-type":"numeric","minimum":1,"multipleOf":0.25},"g":{"type":"number","x-antfly-sql-type":"numeric"},"payload":{"type":"blob"}},"additionalProperties":false}}}}
            );
            defer validator.deinit(alloc);
            try std.testing.expect(!validator.restore.full_root);
            const table = validator.execution.expressions.?.table;
            var layout = try codec.PhysicalLayout.init(alloc, table);
            defer layout.deinit();
            const n = layout.ordinalForName(table.relational_columns, "n").?;
            const g = layout.ordinalForName(table.relational_columns, "g").?;
            const number = try @import("../sql/numeric_storage.zig").encodeJsonAlloc(alloc, .{ .string = "1.25" });
            defer alloc.free(number);
            const generated = try @import("../sql/numeric_storage.zig").encodeJsonAlloc(alloc, .{ .string = "2.25" });
            defer alloc.free(generated);
            var cells: [2]codec.Cell = .{
                .{ .ordinal = @intCast(n), .path = "n", .value_type = .bytes_val, .is_numeric = true, .value = .{ .bytes_val = number } },
                .{ .ordinal = @intCast(g), .path = "g", .value_type = .bytes_val, .is_numeric = true, .value = .{ .bytes_val = generated } },
            };
            if (n > g) std.mem.swap(codec.Cell, &cells[0], &cells[1]);
            const bytes = try codec.serializeOrdinal(alloc, table.version, table.relational_columns, &cells, @splat(0));
            defer alloc.free(bytes);
            const row = try codec.ordinalRowView(bytes, table, &layout);
            var budget: usize = max_allocated_bytes;
            var execution = Execution.init(alloc, &budget);
            try validator.validateRelationalRestoreFieldsWithExecution(&execution, row);
            const used = 8 * 1024 * 1024 - execution.numeric.remaining;
            try std.testing.expect(used > 1 and budget < max_allocated_bytes);
            try std.testing.expectEqual(alloc.ptr, execution.alloc.ptr);
            try std.testing.expectEqual(alloc.vtable, execution.alloc.vtable);
            budget = max_allocated_bytes;
            execution = Execution.init(alloc, &budget);
            execution.numeric.remaining = used - 1;
            const limited = validator.validateRelationalRestoreFieldsWithExecution(&execution, row);
            if (limited) |_| return error.TestExpectedError else |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqual(error.RelationalExpressionBudgetExceeded, err);
            }
            execution.numeric.remaining = 8 * 1024 * 1024;
            try std.testing.expectError(error.RelationalExpressionBudgetExceeded, validator.validateRelationalRestoreFieldsWithExecution(&execution, row));
            budget = 1;
            execution = Execution.init(alloc, &budget);
            try std.testing.expectError(error.RelationalExpressionBudgetExceeded, validator.validateRelationalRestoreFieldsWithExecution(&execution, row));
            const Cancel = struct {
                fn poll(_: ?*anyopaque) anyerror!void {
                    return error.Canceled;
                }
            };
            budget = max_allocated_bytes;
            execution = Execution.init(alloc, &budget);
            execution.numeric.checkpoint = Cancel.poll;
            try std.testing.expectError(error.Canceled, validator.validateRelationalRestoreFieldsWithExecution(&execution, row));
            execution.numeric.checkpoint = null;
            try std.testing.expectError(error.Canceled, validator.validateRelationalRestoreFieldsWithExecution(&execution, row));
            // Still reject physically canonical but logically forged output.
            const generated_index: usize = if (n > g) 0 else 1;
            cells[generated_index].value.bytes_val = number;
            const forged = try codec.serializeOrdinal(alloc, table.version, table.relational_columns, &cells, @splat(0));
            defer alloc.free(forged);
            const rejected = validator.validateRelationalRestoreFields(alloc, try codec.ordinalRowView(forged, table, &layout));
            if (rejected) |_| return error.TestExpectedError else |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqual(error.InvalidRelationalGeneratedValue, err);
            }
        }
    };
    try Run.run(std.testing.allocator);
    var stable = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{});
}

test "relational declarations cold generated verification reads dependency cells only and rejects forged output" {
    const alloc = std.testing.allocator;
    var validator = try @import("mod.zig").CompiledTableValidator.init(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","generated_columns":[{"column":"y","expression":{"op":"add","args":[{"op":"column","column":"x"},{"op":"literal","type":"integer","value":1}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"integer"},"y":{"type":"integer"},"payload":{"type":"blob"}},"additionalProperties":false}}}}
    );
    defer validator.deinit(alloc);
    try std.testing.expect(!validator.restore.full_root);
    const FakeRow = struct {
        table_schema: schema.TableSchema,
        reads: usize = 0,
        generated: i64 = 3,
        missing: bool = false,
        pub fn ordinalForName(self: *@This(), name: []const u8) ?usize {
            for (self.table_schema.relational_columns, 0..) |column, i| if (std.mem.eql(u8, column.name, name)) return i;
            return null;
        }
        pub fn findCell(self: *@This(), ordinal: usize) !?@import("../storage/db/algebraic/relational_row_codec.zig").Cell {
            const name = self.table_schema.relational_columns[ordinal].name;
            if (std.mem.eql(u8, name, "payload")) return error.TestUnexpectedWidePayloadRead;
            self.reads += 1;
            const generated = std.mem.eql(u8, name, "y");
            if (generated and self.missing) return null;
            return .{ .ordinal = @intCast(ordinal), .path = name, .value_type = .i64_val, .value = .{ .i64_val = if (generated) self.generated else 2 } };
        }
    };
    var row: FakeRow = .{ .table_schema = validator.execution.expressions.?.table };
    try validator.execution.expressions.?.verifyRow(alloc, &row);
    try std.testing.expectEqual(@as(usize, 2), row.reads);
    row.generated = 99;
    try std.testing.expectError(error.InvalidRelationalGeneratedValue, validator.execution.expressions.?.verifyRow(alloc, &row));
    row.missing = true;
    try std.testing.expectError(error.InvalidRelationalGeneratedValue, validator.execution.expressions.?.verifyRow(alloc, &row));
}
