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

//! Pointer-free scalar fragment instructions. Execution-only caches, settings
//! captures and external decision providers never cross the worker boundary.
const std = @import("std");
const scalar = @import("antfly_local_sources").sql_scalar;
const A = std.mem.Allocator;
pub const Program = struct {
    version: u16 = 1,
    instructions: []const scalar.Instruction,
    root: u32,
    output: scalar.Type,
    parameters: []const std.json.Value = &.{},
    pub fn validate(self: Program, width: usize) !void {
        if (self.version != 1 or self.instructions.len == 0 or self.instructions.len > 4096 or self.root >= self.instructions.len or self.parameters.len > 256) return error.InvalidSqlDistributedPlan;
        for (self.instructions, 0..) |instruction, index| switch (instruction.operation) {
            .literal => {},
            .column => |ordinal| if (ordinal >= width) return error.InvalidSqlDistributedPlan,
            .parameter => |ordinal| if (ordinal >= self.parameters.len) return error.InvalidSqlDistributedPlan,
            .unary => |operation| if (operation.operand >= index) return error.InvalidSqlDistributedPlan,
            .binary => |operation| if (operation.left >= index or operation.right >= index) return error.InvalidSqlDistributedPlan,
            .cast => |operation| if (operation.operand >= index) return error.InvalidSqlDistributedPlan,
            .case_when => |operation| {
                if (operation.otherwise) |ordinal| if (ordinal >= index) return error.InvalidSqlDistributedPlan;
                for (operation.branches) |branch| if (branch.condition >= index or branch.value >= index) return error.InvalidSqlDistributedPlan;
            },
            .in_list => |operation| {
                if (operation.operand >= index) return error.InvalidSqlDistributedPlan;
                for (operation.values) |ordinal| if (ordinal >= index) return error.InvalidSqlDistributedPlan;
            },
            .call => |operation| {
                if (operation.setting_identity != null) return error.SqlDistributedJoinUnsupported;
                switch (operation.function) {
                    .ai_decide, .ai_choice, .ai_score, .ai_probability, .current_setting, .@"$single", .@"$pattern_quantified", .@"$array_pattern_quantified" => return error.SqlDistributedJoinUnsupported,
                    else => {},
                }
                for (operation.args) |ordinal| if (ordinal >= index) return error.InvalidSqlDistributedPlan;
            },
        };
        if (!std.meta.eql(self.instructions[self.root].type, self.output)) return error.InvalidSqlDistributedPlan;
    }
    pub fn matches(self: Program, a: A, cells: []const scalar.Datum) !bool {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const program: scalar.Program = .{ .arena = &arena, .instructions = self.instructions, .root = self.root, .output_type = self.output, .parameter_types = &.{}, .parameter_descriptors = &.{}, .required_columns = &.{} };
        const value = try program.evaluate(arena.allocator(), cells, self.parameters, .{});
        if (value.sql_null) return false;
        if (value.value != .bool) return error.InvalidSqlDistributedPlan;
        return value.value.bool;
    }
};

const Context = @import("antfly_local_sources").serverless_query_lake_read_context.Context;
const Batch = @import("antfly_local_sources").sql_execution_batch.Batch;
const Numeric = @import("antfly_local_sources").sql_numeric_value;
const RegexExecution = @import("antfly_local_sources").sql_regex_execution;
/// One task owns predicate caches and quota-controlled regex lanes. Row scratch
/// retains capacity but is reset between evaluations; it never owns output rows.
pub const Task = struct {
    pub const Kind = enum { left, right, condition };
    left: ?Prepared = null,
    right: ?Prepared = null,
    condition: ?Prepared = null,
    regex: RegexExecution,
    pub fn init(a: A, context: Context, memory: u64, left: ?Program, right: ?Program, condition: ?Program) !Task {
        var result: Task = .{ .regex = RegexExecution.init(a, @intCast(@min(@as(u64, 16 * 1024 * 1024), memory / 2))) };
        errdefer result.deinit();
        const constants: usize = @intCast(@min(@as(u64, 4 * 1024 * 1024), memory / 32));
        if (left) |ir| result.left = try Prepared.init(a, ir, context, constants);
        if (right) |ir| result.right = try Prepared.init(a, ir, context, constants);
        if (condition) |ir| result.condition = try Prepared.init(a, ir, context, constants);
        return result;
    }
    pub fn deinit(self: *Task) void {
        if (self.left) |*value| value.deinit();
        if (self.right) |*value| value.deinit();
        if (self.condition) |*value| value.deinit();
        self.regex.deinit();
    }
    pub fn matches(self: *Task, kind: Kind, cells: []const scalar.Datum) !bool {
        const slot = switch (kind) {
            .left => &self.left,
            .right => &self.right,
            .condition => &self.condition,
        };
        return if (slot.*) |*prepared| prepared.matches(cells, &self.regex) else true;
    }
    /// A probe row can match many build rows. Keep its demanded values once,
    /// separately from the per-match right side and expression temporaries.
    pub fn beginProbe(self: *Task) void {
        if (self.condition) |*prepared| prepared.beginProbe();
    }
    pub fn endProbe(self: *Task) void {
        if (self.condition) |*prepared| prepared.endProbe();
    }
    /// The VM loads only executed column references, including short circuits.
    /// Borrowed pages remain pinned until this predicate invocation returns.
    pub fn matchesBatch(self: *Task, kind: Kind, left: Batch, row: usize, right: ?Batch) !bool {
        const slot = switch (kind) {
            .left => &self.left,
            .right => &self.right,
            .condition => &self.condition,
        };
        return if (slot.*) |*prepared| prepared.matchesBatch(left, row, right, &self.regex) else true;
    }
};
pub const Prepared = struct {
    const Memo = struct {
        epoch: u64 = 0,
        probe_epoch: u64 = 0,
        value: scalar.Datum = undefined,
        fn get(self: *const Memo, prepared: *const Prepared, probe: bool) ?scalar.Datum {
            return if (if (probe) self.probe_epoch == prepared.probe_epoch else self.epoch == prepared.epoch) self.value else null;
        }
        fn put(self: *Memo, prepared: *const Prepared, probe: bool, value: scalar.Datum) scalar.Datum {
            self.value = value;
            self.epoch = if (probe) 0 else prepared.epoch;
            self.probe_epoch = if (probe) prepared.probe_epoch else 0;
            return value;
        }
    };
    const Cell = struct {
        raw: Memo = .{},
        identity: Memo = .{},
        missing: Memo = .{},
        descriptor: scalar.Type = .{},
        coefficient: Numeric.ScaledInteger = undefined,
        number: Numeric.Value = undefined,
    };
    a: A,
    cells: []Cell = &.{},
    slots: []u16 = &.{},
    epoch: u64 = 0,
    probe_epoch: u64 = 0,
    reuse_left: bool = false,
    program: scalar.Program,
    parameters: []const std.json.Value,
    scratch: std.heap.ArenaAllocator,
    left_scratch: std.heap.ArenaAllocator,
    context: Context,
    fn init(a: A, ir: Program, context: Context, constants: usize) !Prepared {
        try context.ensureActive();
        var result: Prepared = .{ .a = a, .program = try scalar.Program.fromTypedInstructions(a, ir.instructions, ir.root, ir.output, .{ .constant_bytes = constants }), .parameters = ir.parameters, .scratch = .init(a), .left_scratch = .init(a), .context = context };
        errdefer result.deinit();
        var width: usize = 0;
        for (result.program.required_columns) |column| {
            if (column >= 2048) return error.InvalidSqlDistributedPlan;
            width = @max(width, column + 1);
        }
        result.slots = try a.alloc(u16, width);
        @memset(result.slots, std.math.maxInt(u16));
        result.cells = try a.alloc(Cell, result.program.required_columns.len);
        @memset(result.cells, .{});
        for (result.program.required_columns, 0..) |column, index| result.slots[column] = @intCast(index);
        for (result.program.instructions) |instruction| if (instruction.operation == .column) {
            result.cells[result.slots[instruction.operation.column]].descriptor = instruction.type;
        };
        return result;
    }
    fn deinit(self: *Prepared) void {
        self.a.free(self.cells);
        self.a.free(self.slots);
        self.scratch.deinit();
        self.left_scratch.deinit();
        self.program.deinit();
    }
    fn checkpoint(raw: ?*anyopaque) !void {
        const context: *const Context = @ptrCast(@alignCast(raw.?));
        try context.ensureActive();
    }
    pub fn evalLimits(self: *Prepared, session: *RegexExecution) scalar.EvalLimits {
        return .{ .regex_execution = session, .regex_checkpoint = checkpoint, .regex_context = &self.context, .checkpoint = checkpoint, .checkpoint_context = &self.context };
    }
    fn beginProbe(self: *Prepared) void {
        std.debug.assert(!self.reuse_left);
        self.probe_epoch +%= 1;
        if (self.probe_epoch == 0) {
            for (self.cells) |*cell| {
                cell.raw.probe_epoch = 0;
                cell.identity.probe_epoch = 0;
                cell.missing.probe_epoch = 0;
            }
            self.probe_epoch = 1;
        }
        self.reuse_left = true;
    }
    fn endProbe(self: *Prepared) void {
        self.reuse_left = false;
        _ = self.left_scratch.reset(.{ .retain_with_limit = 64 * 1024 });
    }
    const Input = struct {
        prepared: *Prepared,
        left: Batch,
        row: usize,
        right: ?Batch,
        limits: scalar.EvalLimits,
        fn read(raw: *anyopaque, a: A, ordinal: u32, demand: scalar.ColumnRead) anyerror!scalar.Datum {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (ordinal >= self.prepared.slots.len) return error.InvalidSqlBackendResponse;
            const slot = self.prepared.slots[ordinal];
            if (slot == std.math.maxInt(u16)) return error.InvalidSqlBackendResponse;
            const cell = &self.prepared.cells[slot];
            const memo = switch (demand) {
                .value => &cell.raw,
                .identity => &cell.identity,
                .missing => &cell.missing,
            };
            const left = ordinal < self.left.width();
            const probe = left and self.prepared.reuse_left;
            if (memo.get(self.prepared, probe)) |value| return value;
            const payload = if (probe) self.prepared.left_scratch.allocator() else a;
            const source: ?Batch = if (left) self.left else self.right;
            const column = if (left) ordinal else ordinal - self.left.width();
            const row = if (left) self.row else 0;
            if (demand == .missing) if (source) |batch| {
                // An authenticated encoded payload proves presence without DOM
                // materialization. Keep this fact separate from raw-value memo.
                if (try batch.frozenJson(row, column) != null or try batch.encodedJson(row, column) != null)
                    return memo.put(self.prepared, probe, scalar.Datum.json(.{ .bool = false }));
            };
            const value: scalar.Datum = if (demand == .value) value: {
                const batch = source orelse break :value .{};
                break :value if (try batch.decimalColumnSelected(payload, column, &.{row})) |decimal|
                    try decimal.cell(0, &cell.coefficient, &cell.number)
                else
                    try batch.cell(payload, row, column);
            } else derived: {
                // Normalization may construct a large logical array/number.
                // Retain only its opaque token across duplicate probe matches.
                var result = try self.prepared.program.readColumnValue(a, try read(raw, a, ordinal, .value), cell.descriptor, demand, self.limits);
                if (probe and demand == .identity and !result.sql_null)
                    result.value.string = try payload.dupe(u8, result.value.string);
                break :derived result;
            };
            return memo.put(self.prepared, probe, value);
        }
    };
    fn matchesBatch(self: *Prepared, left: Batch, row: usize, right: ?Batch, regex: *RegexExecution) !bool {
        try self.context.ensureActive();
        _ = self.scratch.reset(.{ .retain_with_limit = 64 * 1024 });
        // Reset is constant-time even for wide, short-circuited predicates.
        self.epoch +%= 1;
        if (self.epoch == 0) {
            for (self.cells) |*cell| {
                cell.raw.epoch = 0;
                cell.identity.epoch = 0;
                cell.missing.epoch = 0;
            }
            self.epoch = 1;
        }
        var limits = self.evalLimits(regex);
        var budget: scalar.EvaluationBudget = .{ .work = .{ .alloc = self.scratch.allocator(), .remaining = limits.steps, .checkpoint = limits.checkpoint, .ptr = limits.checkpoint_context } };
        limits.shared_budget = &budget;
        var input: Input = .{ .prepared = self, .left = left, .row = row, .right = right, .limits = limits };
        limits.column_loader = .{ .context = &input, .read = Input.read };
        const value = try self.program.evaluate(self.scratch.allocator(), &.{}, self.parameters, limits);
        if (value.sql_null) return false;
        if (value.value != .bool) return error.InvalidSqlDistributedPlan;
        return value.value.bool;
    }
    fn matches(self: *Prepared, cells: []const scalar.Datum, session: *RegexExecution) !bool {
        try self.context.ensureActive();
        _ = self.scratch.reset(.retain_capacity);
        const value = try self.program.evaluate(self.scratch.allocator(), cells, self.parameters, self.evalLimits(session));
        if (value.sql_null) return false;
        if (value.value != .bool) return error.InvalidSqlDistributedPlan;
        return value.value.bool;
    }
};

test "distributed SQL scalar fragments validate DAGs and retain SQL null predicate semantics" {
    const a = std.testing.allocator;
    const t: scalar.Type = .{ .kind = .boolean };
    var program: Program = .{ .instructions = &.{.{ .type = t, .operation = .{ .column = 0 } }}, .root = 0, .output = t };
    try program.validate(1);
    try std.testing.expect(!try program.matches(a, &.{.{}}));
    try std.testing.expect(try program.matches(a, &.{scalar.Datum.json(.{ .bool = true })}));
    program.instructions = &.{.{ .type = t, .operation = .{ .unary = .{ .op = .not, .operand = 0 } } }};
    try std.testing.expectError(error.InvalidSqlDistributedPlan, program.validate(1));
}

test "distributed SQL prepared predicates reuse regex state and bound warm allocations" {
    const a = std.testing.allocator;
    const instructions: []const scalar.Instruction = &.{
        .{ .type = .{ .kind = .string }, .operation = .{ .column = 0 } },
        .{ .type = .{ .kind = .string }, .operation = .{ .literal = .{ .string = "^(foo|bar|baz)+$" } } },
        .{ .type = .{ .kind = .boolean }, .operation = .{ .call = .{ .function = .regexp_like, .args = &.{ 0, 1 } } } },
    };
    const ir: Program = .{ .instructions = instructions, .root = 2, .output = .{ .kind = .boolean } };
    try ir.validate(1);
    var counted = std.testing.FailingAllocator.init(a, .{});
    var task = try Task.init(counted.allocator(), .{}, 128 * 1024 * 1024, null, null, ir);
    defer task.deinit();
    const cells = [_]scalar.Datum{scalar.Datum.json(.{ .string = "foobarbazfoo" })};
    try std.testing.expect(try task.matches(.condition, &cells));
    const warm_bytes = counted.allocated_bytes;
    const started = @import("antfly_platform").time.monotonicNs();
    for (0..999) |_| try std.testing.expect(try task.matches(.condition, &cells));
    try std.testing.expectEqual(@as(u64, 1), task.regex.snapshot().compilations);
    try std.testing.expectEqual(@as(u64, 999), task.regex.snapshot().hits);
    try std.testing.expect(counted.allocated_bytes - warm_bytes < 64 * 1024);
    std.debug.print("prepared predicate: warm_rows=999 allocations={d} total_bytes={d} warm_extra_bytes={d} elapsed_ns={d}\n", .{ counted.allocations, counted.allocated_bytes, counted.allocated_bytes - warm_bytes, @import("antfly_platform").time.monotonicNs() - started });
    const Cancel = struct {
        checks: usize = 0,
        fn canceled(raw: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            return self.checks >= 2;
        }
    };
    var cancel: Cancel = .{};
    task.condition.?.context.cancellation = .{ .ptr = &cancel, .is_cancelled_fn = Cancel.canceled };
    try std.testing.expectError(error.Canceled, task.matches(.condition, &cells));
    try std.testing.expect(cancel.checks >= 2);
}

test "distributed SQL batch predicates load demanded columns once and short circuit unused payloads" {
    const a = std.testing.allocator;
    const Reader = struct {
        reads: usize = 0,
        fn cell(raw: *anyopaque, _: A, _: usize, column: usize) anyerror!scalar.Datum {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (column != 0) return error.UnreferencedPayloadRead;
            self.reads += 1;
            return scalar.Datum.json(.{ .bool = false });
        }
    };
    var reader: Reader = .{};
    const batch: Batch = .{ .reader = .{ .ptr = &reader, .read = Reader.cell, .count = 2, .width = 3 } };
    var compiled = try @import("antfly_local_sources").sql_compiler.compileScalar(a, "flag AND ignored", .{});
    defer compiled.deinit();
    var bound = try scalar.bind(a, compiled.expression, &.{ .{ .name = "flag", .type = .boolean }, .{ .name = "ignored", .type = .boolean }, .{ .name = "payload", .type = .json } }, &.{}, .{});
    defer bound.deinit();
    const ir: Program = .{ .instructions = bound.instructions, .root = bound.root, .output = bound.output_type };
    var task = try Task.init(a, .{}, 16 * 1024 * 1024, ir, null, null);
    defer task.deinit();
    try std.testing.expect(!try task.matchesBatch(.left, batch, 0, null));
    try std.testing.expect(!try task.matchesBatch(.left, batch, 1, null));
    try std.testing.expectEqual(@as(usize, 2), reader.reads);
    // Repeated references use one invocation-local memo, including lazy NUMERIC.
    var repeated = try @import("antfly_local_sources").sql_compiler.compileScalar(a, "flag OR flag", .{});
    defer repeated.deinit();
    var repeat_bound = try scalar.bind(a, repeated.expression, &.{.{ .name = "flag", .type = .boolean }}, &.{}, .{});
    defer repeat_bound.deinit();
    const repeat_ir: Program = .{ .instructions = repeat_bound.instructions, .root = repeat_bound.root, .output = repeat_bound.output_type };
    var repeated_task = try Task.init(a, .{}, 16 * 1024 * 1024, repeat_ir, null, null);
    defer repeated_task.deinit();
    reader.reads = 0;
    try std.testing.expect(!try repeated_task.matchesBatch(.left, batch, 0, null));
    try std.testing.expectEqual(@as(usize, 1), reader.reads);
}

test "distributed SQL joined numeric predicates borrow decimal lanes and reset invocation memos" {
    const a = std.testing.allocator;
    const Reader = struct {
        values: [1]i128 = .{12300},
        reads: usize = 0,
        fn cell(_: *anyopaque, _: A, _: usize, _: usize) anyerror!scalar.Datum {
            return error.UnexpectedScalarRead;
        }
        fn decimal(raw: *anyopaque, alloc: A, ordinal: usize, selected: ?[]const usize) anyerror!?@import("antfly_local_sources").sql_execution_batch.DecimalColumn {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (ordinal != 0) return null;
            self.reads += 1;
            const view: @import("antfly_local_sources").sql_execution_batch.PhysicalColumn = .{ .kind = .number, .element = .numeric, .range = .{ .count = 1 }, .vector = .{ .name = "n", .values = .{ .decimal128 = .{ .values = &self.values, .scale = 4, .precision = 38 } } } };
            return .{ .physical = if (selected) |rows| try view.select(alloc, rows) else view };
        }
        fn batch(self: *@This()) Batch {
            return .{ .reader = .{ .ptr = self, .read = cell, .read_decimal_selected = decimal, .width = 2, .count = 1 } };
        }
    };
    var left: Reader = .{};
    var right: Reader = .{};
    const ir: Program = .{ .instructions = &.{
        .{ .type = .{ .kind = .number, .element_type = .numeric }, .operation = .{ .column = 0 } },
        .{ .type = .{ .kind = .number, .element_type = .numeric }, .operation = .{ .column = 2 } },
        .{ .type = .{ .kind = .boolean }, .operation = .{ .binary = .{ .op = .eq, .left = 0, .right = 1 } } },
    }, .root = 2, .output = .{ .kind = .boolean } };
    var task = try Task.init(a, .{}, 16 * 1024 * 1024, null, null, ir);
    defer task.deinit();
    try std.testing.expectEqualSlices(u32, &.{ 0, 2 }, task.condition.?.program.required_columns);
    try std.testing.expect(try task.matchesBatch(.condition, left.batch(), 0, right.batch()));
    try std.testing.expect(!try task.matchesBatch(.condition, left.batch(), 0, null));
    left.values[0] = 12301;
    try std.testing.expect(!try task.matchesBatch(.condition, left.batch(), 0, right.batch()));
    try std.testing.expectEqual(@as(usize, 3), left.reads);
    try std.testing.expectEqual(@as(usize, 2), right.reads);
}

test "distributed SQL sparse predicate preparation and evaluation unwind allocation failures" {
    const Scenario = struct {
        fn run(a: A) !void {
            const ir: Program = .{ .instructions = &.{
                .{ .type = .{ .kind = .boolean }, .operation = .{ .column = 3 } },
                .{ .type = .{ .kind = .string }, .operation = .{ .call = .{ .function = .value_identity, .args = &.{0} } } },
                .{ .type = .{ .kind = .boolean }, .operation = .{ .unary = .{ .op = .is_not_null, .operand = 1 } } },
            }, .root = 2, .output = .{ .kind = .boolean } };
            var task = try Task.init(a, .{}, 16 * 1024 * 1024, null, null, ir);
            defer task.deinit();
            const batch: Batch = .{ .rows = &.{&.{ .{}, .{}, .{}, scalar.Datum.json(.{ .bool = true }) }} };
            task.beginProbe();
            defer task.endProbe();
            try std.testing.expect(try task.matchesBatch(.condition, batch, 0, null));
        }
    };
    try Scenario.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Scenario.run, .{});
}

test "distributed SQL residual probes reuse demanded left values across duplicate matches" {
    const a = std.testing.allocator;
    const Reader = struct {
        value: bool = false,
        reads: usize = 0,
        fn cell(raw: *anyopaque, _: A, _: usize, _: usize) anyerror!scalar.Datum {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            return scalar.Datum.json(.{ .bool = self.value });
        }
        fn batch(self: *@This()) Batch {
            return .{ .reader = .{ .ptr = self, .read = cell, .width = 1, .count = 1 } };
        }
    };
    var left: Reader = .{};
    var right: Reader = .{};
    const ir: Program = .{ .instructions = &.{
        .{ .type = .{ .kind = .boolean }, .operation = .{ .column = 0 } },
        .{ .type = .{ .kind = .boolean }, .operation = .{ .column = 1 } },
        .{ .type = .{ .kind = .boolean }, .operation = .{ .binary = .{ .op = .eq, .left = 0, .right = 1 } } },
    }, .root = 2, .output = .{ .kind = .boolean } };
    var task = try Task.init(a, .{}, 16 * 1024 * 1024, null, null, ir);
    defer task.deinit();
    {
        task.beginProbe();
        defer task.endProbe();
        for (0..100) |_| try std.testing.expect(try task.matchesBatch(.condition, left.batch(), 0, right.batch()));
        try std.testing.expectEqual(@as(usize, 1), left.reads);
        try std.testing.expectEqual(@as(usize, 100), right.reads);
    }
    left.value = true;
    task.beginProbe();
    defer task.endProbe();
    try std.testing.expect(!try task.matchesBatch(.condition, left.batch(), 0, right.batch()));
    try std.testing.expectEqual(@as(usize, 2), left.reads);
    try std.testing.expectEqual(@as(usize, 101), right.reads);
}

test "distributed SQL column read modes preserve scalar semantics for nulls missing numerics arrays and floats" {
    const a = std.testing.allocator;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const sa = region.allocator();
    var numeric_context: Numeric.Context = .{ .alloc = sa };
    const number = try Numeric.parse(&numeric_context, "123.4500");
    const special = try Numeric.parse(&numeric_context, "NaN");
    const arrays = @import("antfly_local_sources").sql_array_value;
    const array = try arrays.Value.init(.int64, &.{.{ .length = 2, .lower = -3 }}, &.{ scalar.Datum.json(.{ .integer = 7 }), .{} }, .{});
    const Case = struct { value: scalar.Datum, type_: scalar.Type };
    const cases = [_]Case{
        .{ .value = scalar.Datum.json(.{ .bool = true }), .type_ = .{ .kind = .boolean } },
        .{ .value = scalar.Datum.json(.{ .bool = false }), .type_ = .{ .kind = .boolean } },
        .{ .value = .{}, .type_ = .{ .kind = .boolean } },
        .{ .value = .{ .missing = true }, .type_ = .{ .kind = .json } },
        .{ .value = scalar.Datum.json(.null), .type_ = .{ .kind = .json } },
        .{ .value = scalar.Datum.json(try std.json.parseFromSliceLeaky(std.json.Value, sa, "{\"v\":[1,null,true]}", .{})), .type_ = .{ .kind = .json } },
        .{ .value = scalar.Datum.json(.{ .string = "quote\" and π" }), .type_ = .{ .kind = .string } },
        .{ .value = scalar.Datum.typedNumeric(&number.value), .type_ = .{ .kind = .number, .element_type = .numeric } },
        .{ .value = scalar.Datum.typedNumeric(&special.value), .type_ = .{ .kind = .number, .element_type = .numeric } },
        .{ .value = scalar.Datum.typedArray(&array), .type_ = .{ .kind = .array, .element_type = .int64 } },
        .{ .value = scalar.Datum.json(.{ .float = 0.1 }), .type_ = .{ .kind = .number, .element_type = .float32 } },
        .{ .value = scalar.Datum.json(.{ .float = std.math.inf(f64) }), .type_ = .{ .kind = .number, .element_type = .float64 } },
    };
    for (cases) |case| for ([_][]const u8{
        "is_missing(v)",
        "value_identity(v) IS NULL",
        "value_identity(v) = value_identity(v)",
        "is_missing(v) OR value_identity(v) IS NOT NULL",
    }) |sql| {
        var compiled = try @import("antfly_local_sources").sql_compiler.compileScalar(a, sql, .{});
        defer compiled.deinit();
        var bound = try scalar.bind(a, compiled.expression, &.{.{ .name = "v", .type = case.type_.kind.?, .element_type = case.type_.element_type }}, &.{}, .{});
        defer bound.deinit();
        const ir: Program = .{ .instructions = bound.instructions, .root = bound.root, .output = bound.output_type };
        try ir.validate(1);
        var task = try Task.init(a, .{}, 16 * 1024 * 1024, ir, null, null);
        defer task.deinit();
        const cells = [_]scalar.Datum{case.value};
        const expected = try task.matches(.left, &cells);
        const actual = try task.matchesBatch(.left, .{ .rows = &.{&cells} }, 0, null);
        try std.testing.expectEqual(expected, actual);
    };
}

test "distributed SQL derived reads keep raw memo separate and reuse identities across probe matches" {
    const a = std.testing.allocator;
    const Reader = struct {
        reads: usize = 0,
        fn cell(raw: *anyopaque, _: A, _: usize, _: usize) anyerror!scalar.Datum {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            return scalar.Datum.json(.{ .bool = true });
        }
    };
    var reader: Reader = .{};
    var compiled = try @import("antfly_local_sources").sql_compiler.compileScalar(a, "NOT is_missing(v) AND value_identity(v) = value_identity(TRUE) AND v", .{});
    defer compiled.deinit();
    var bound = try scalar.bind(a, compiled.expression, &.{.{ .name = "v", .type = .boolean }}, &.{}, .{});
    defer bound.deinit();
    const ir: Program = .{ .instructions = bound.instructions, .root = bound.root, .output = bound.output_type };
    var task = try Task.init(a, .{}, 16 * 1024 * 1024, null, null, ir);
    defer task.deinit();
    const batch: Batch = .{ .reader = .{ .ptr = &reader, .read = Reader.cell, .count = 1, .width = 1 } };
    task.beginProbe();
    defer task.endProbe();
    for (0..100) |_| try std.testing.expect(try task.matchesBatch(.condition, batch, 0, null));
    try std.testing.expectEqual(@as(usize, 1), reader.reads);
    const memo = &task.condition.?.cells[0];
    try std.testing.expect(memo.raw.value.value.bool);
    try std.testing.expect(!memo.missing.value.value.bool);
    try std.testing.expect(memo.identity.value.value == .string);
}

test "distributed SQL missing presence reads encoded JSON without materializing payloads" {
    const a = std.testing.allocator;
    const Reader = struct {
        fn cell(_: *anyopaque, _: A, _: usize, _: usize) anyerror!scalar.Datum {
            return error.UnexpectedPayloadRead;
        }
        fn json(_: *anyopaque, _: usize, _: usize) anyerror!?[]const u8 {
            return "{\"present\":null}";
        }
    };
    var marker: u8 = 0;
    const batch: Batch = .{ .reader = .{ .ptr = &marker, .read = Reader.cell, .read_json = Reader.json, .count = 1, .width = 1 } };
    const ir: Program = .{ .instructions = &.{
        .{ .type = .{ .kind = .json }, .operation = .{ .column = 0 } },
        .{ .type = .{ .kind = .boolean }, .operation = .{ .call = .{ .function = .is_missing, .args = &.{0} } } },
    }, .root = 1, .output = .{ .kind = .boolean } };
    var task = try Task.init(a, .{}, 16 * 1024 * 1024, ir, null, null);
    defer task.deinit();
    try std.testing.expect(!try task.matchesBatch(.left, batch, 0, null));
}

test "distributed SQL parameter ordinals match compiler slots and reject every out of range boundary" {
    const a = std.testing.allocator;
    var compiled = try @import("antfly_local_sources").sql_compiler.compileScalar(a, "v = $1", .{});
    defer compiled.deinit();
    var bound = try scalar.bind(a, compiled.expression, &.{.{ .name = "v", .type = .boolean }}, &.{.boolean}, .{});
    defer bound.deinit();
    var ir: Program = .{ .instructions = bound.instructions, .root = bound.root, .output = bound.output_type, .parameters = &.{.{ .bool = true }} };
    try ir.validate(1);
    var task = try Task.init(a, .{}, 16 * 1024 * 1024, ir, null, null);
    defer task.deinit();
    try std.testing.expect(try task.matchesBatch(.left, .{ .rows = &.{&.{scalar.Datum.json(.{ .bool = true })}} }, 0, null));
    ir.parameters = &.{};
    try std.testing.expectError(error.InvalidSqlDistributedPlan, ir.validate(1));
    const parameters: [256]std.json.Value = @splat(.{ .bool = true });
    for ([_]u32{ 0, 1, 255, 256, std.math.maxInt(u32) }) |slot| {
        const instruction: scalar.Instruction = .{ .type = .{ .kind = .boolean }, .operation = .{ .parameter = slot } };
        const program: Program = .{ .instructions = &.{instruction}, .root = 0, .output = .{ .kind = .boolean }, .parameters = &parameters };
        if (slot < parameters.len) try program.validate(0) else try std.testing.expectError(error.InvalidSqlDistributedPlan, program.validate(0));
    }
}

test "distributed SQL identity memo retains only the token after large array normalization" {
    const a = std.testing.allocator;
    const local = @import("antfly_local_sources");
    const elements = try a.alloc(scalar.Datum, 10000);
    defer a.free(elements);
    @memset(elements, scalar.Datum.json(.{ .integer = 7 }));
    const array = try local.sql_array_value.Value.init(.int64, &.{.{ .length = 10000, .lower = -2 }}, elements, .{});
    var compiled = try local.sql_compiler.compileScalar(a, "value_identity(v) = value_identity(v)", .{});
    defer compiled.deinit();
    var bound = try scalar.bind(a, compiled.expression, &.{.{ .name = "v", .type = .array, .element_type = .int64 }}, &.{}, .{});
    defer bound.deinit();
    const ir: Program = .{ .instructions = bound.instructions, .root = bound.root, .output = bound.output_type };
    var budget: local.sql_memory_budget = .{ .backing = a, .limit = 4 * 1024 * 1024 };
    defer std.debug.assert(budget.live == 0);
    var task = try Task.init(budget.allocator(), .{}, 16 * 1024 * 1024, null, null, ir);
    defer task.deinit();
    const cells = [_]scalar.Datum{scalar.Datum.typedArray(&array)};
    const batch: Batch = .{ .rows = &.{&cells} };
    task.beginProbe();
    defer task.endProbe();
    for (0..5) |_| try std.testing.expect(try task.matchesBatch(.condition, batch, 0, null));
    try std.testing.expect(budget.live < 128 * 1024);
}
