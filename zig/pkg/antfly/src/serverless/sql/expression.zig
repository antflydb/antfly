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
const Regex = @import("antfly_sql_regex");
/// One task owns all predicate caches and one serial regex lane. Row scratch
/// retains capacity but is reset between evaluations; it never owns output rows.
pub const Task = struct {
    pub const Kind = enum { left, right, condition };
    left: ?Prepared = null,
    right: ?Prepared = null,
    condition: ?Prepared = null,
    regex: Regex.Session,
    pub fn init(a: A, context: Context, memory: u64, left: ?Program, right: ?Program, condition: ?Program) !Task {
        var result: Task = .{ .regex = Regex.Session.init(a, .{ .heap_bytes = @intCast(@min(@as(u64, 8 * 1024 * 1024), memory / 4)) }, @intCast(@min(@as(u64, 16 * 1024 * 1024), memory / 2))) };
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
};
pub const Prepared = struct {
    program: scalar.Program,
    parameters: []const std.json.Value,
    scratch: std.heap.ArenaAllocator,
    context: Context,
    fn init(a: A, ir: Program, context: Context, constants: usize) !Prepared {
        try context.ensureActive();
        return .{ .program = try scalar.Program.fromTypedInstructions(a, ir.instructions, ir.root, ir.output, .{ .constant_bytes = constants }), .parameters = ir.parameters, .scratch = .init(a), .context = context };
    }
    fn deinit(self: *Prepared) void {
        self.scratch.deinit();
        self.program.deinit();
    }
    fn checkpoint(raw: ?*anyopaque) !void {
        const context: *const Context = @ptrCast(@alignCast(raw.?));
        try context.ensureActive();
    }
    fn matches(self: *Prepared, cells: []const scalar.Datum, session: *Regex.Session) !bool {
        try self.context.ensureActive();
        _ = self.scratch.reset(.retain_capacity);
        const value = try self.program.evaluate(self.scratch.allocator(), cells, self.parameters, .{ .regex_session = session, .regex_checkpoint = checkpoint, .regex_context = &self.context, .checkpoint = checkpoint, .checkpoint_context = &self.context });
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
    try std.testing.expectEqual(@as(u64, 1), task.regex.compilations);
    try std.testing.expectEqual(@as(u64, 999), task.regex.hits);
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
