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
            .parameter => |ordinal| if (ordinal == 0 or ordinal > self.parameters.len) return error.InvalidSqlDistributedPlan,
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
    const Cell = struct {
        epoch: u64 = 0,
        probe_epoch: u64 = 0,
        value: scalar.Datum = undefined,
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
            for (self.cells) |*cell| cell.probe_epoch = 0;
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
        fn read(raw: *anyopaque, a: A, ordinal: u32, _: scalar.ColumnRead) anyerror!scalar.Datum {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (ordinal >= self.prepared.slots.len) return error.InvalidSqlBackendResponse;
            const slot = self.prepared.slots[ordinal];
            if (slot == std.math.maxInt(u16)) return error.InvalidSqlBackendResponse;
            const cached = &self.prepared.cells[slot];
            const left = ordinal < self.left.width();
            const probe_value = left and self.prepared.reuse_left;
            if (probe_value) {
                if (cached.probe_epoch == self.prepared.probe_epoch) return cached.value;
            } else if (cached.epoch == self.prepared.epoch) return cached.value;
            const payload = if (probe_value) self.prepared.left_scratch.allocator() else a;
            const source = if (left) self.left else self.right orelse {
                cached.value = .{};
                cached.epoch = self.prepared.epoch;
                return .{};
            };
            const column = if (left) ordinal else ordinal - self.left.width();
            const row = if (left) self.row else 0;
            cached.value = if (try source.decimalColumnSelected(payload, column, &.{row})) |decimal|
                try decimal.cell(0, &cached.coefficient, &cached.number)
            else
                try source.cell(payload, row, column);
            if (probe_value) {
                cached.probe_epoch = self.prepared.probe_epoch;
                cached.epoch = 0;
            } else {
                cached.epoch = self.prepared.epoch;
                cached.probe_epoch = 0;
            }
            return cached.value;
        }
    };
    fn matchesBatch(self: *Prepared, left: Batch, row: usize, right: ?Batch, regex: *RegexExecution) !bool {
        try self.context.ensureActive();
        _ = self.scratch.reset(.{ .retain_with_limit = 64 * 1024 });
        // Reset is constant-time even for wide, short-circuited predicates.
        self.epoch +%= 1;
        if (self.epoch == 0) {
            for (self.cells) |*cell| cell.epoch = 0;
            self.epoch = 1;
        }
        var input: Input = .{ .prepared = self, .left = left, .row = row, .right = right };
        var limits = self.evalLimits(regex);
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
            const ir: Program = .{ .instructions = &.{.{ .type = .{ .kind = .boolean }, .operation = .{ .column = 3 } }}, .root = 0, .output = .{ .kind = .boolean } };
            var task = try Task.init(a, .{}, 16 * 1024 * 1024, ir, null, null);
            defer task.deinit();
            const batch: Batch = .{ .rows = &.{&.{ .{}, .{}, .{}, scalar.Datum.json(.{ .bool = true }) }} };
            try std.testing.expect(try task.matchesBatch(.left, batch, 0, null));
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
