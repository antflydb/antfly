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

//! Demand-aware immutable schema planning. Binding/type/input validation must
//! precede this pass. Dead branches are removed before mutable-function checks;
//! stable/volatile functions are never executed to decide admissibility.
const std = @import("std");
const scalar = @import("scalar.zig");
const Plan = @This();
pub const Limits = struct {
    nodes: usize = 8192,
    depth: usize = 64,
    bytes: usize = @import("resource_limits.zig").preparation_bytes,
    steps: usize = 1_048_576,
    checkpoint: ?*const fn (?*anyopaque) anyerror!void = null,
    checkpoint_context: ?*anyopaque = null,
};
pub const Node = struct {
    operation: @FieldType(scalar.Instruction, "operation"),
    alias: ?u32 = null,
    volatility: scalar.Volatility = .immutable,
    visited: bool = false,
    reachable: bool = false,
};
/// Ordinary defaults retain deterministic row failures for INSERT/UPDATE.
/// Generated/index expressions still require successful immutable planning.
pub const Policy = enum { strict, defer_row_errors };

const State = struct {
    memory: @import("memory_budget.zig"),
    arena: std.heap.ArenaAllocator,
};
state: *State,
program: *const scalar.Program,
nodes: []Node,
constants: []?scalar.Datum,
evaluator: scalar.Program.ConstantEvaluation,
limits: Limits,
policy: Policy,

pub fn init(a: std.mem.Allocator, program: *const scalar.Program) !Plan {
    return initWithLimits(a, program, .{});
}

pub fn initWithLimits(a: std.mem.Allocator, program: *const scalar.Program, limits: Limits) !Plan {
    return initWithPolicy(a, program, limits, .strict);
}

pub fn initWithPolicy(a: std.mem.Allocator, program: *const scalar.Program, limits: Limits, policy: Policy) !Plan {
    if (program.instructions.len > limits.nodes) return error.SqlProgramLimitExceeded;
    const state = try a.create(State);
    state.memory = .{ .backing = a, .limit = limits.bytes };
    state.arena = std.heap.ArenaAllocator.init(state.memory.allocator());
    errdefer {
        state.arena.deinit();
        a.destroy(state);
    }
    const alloc = state.arena.allocator();
    const nodes = alloc.alloc(Node, program.instructions.len) catch |err| return if (err == error.OutOfMemory and state.memory.isExhausted()) error.SqlProgramLimitExceeded else err;
    const constants = alloc.alloc(?scalar.Datum, program.instructions.len) catch |err| return if (err == error.OutOfMemory and state.memory.isExhausted()) error.SqlProgramLimitExceeded else err;
    @memset(constants, null);
    for (nodes, program.instructions) |*node, instruction| node.* = .{ .operation = instruction.operation };
    var result: Plan = .{ .state = state, .program = program, .nodes = nodes, .constants = constants, .limits = limits, .policy = policy, .evaluator = .init(program, alloc, constants, .{ .steps = limits.steps, .output_bytes = limits.bytes, .checkpoint = limits.checkpoint, .checkpoint_context = limits.checkpoint_context }) };
    result.visit(program.root, 0) catch |err| return if (err == error.OutOfMemory and state.memory.isExhausted()) error.SqlProgramLimitExceeded else err;
    try result.mark(program.root, 0);
    return result;
}

pub fn deinit(self: *Plan) void {
    const a = self.state.memory.backing;
    self.state.arena.deinit();
    std.debug.assert(self.state.memory.live == 0);
    a.destroy(self.state);
    self.* = undefined;
}

fn merge(a: scalar.Volatility, b: scalar.Volatility) scalar.Volatility {
    return if (@backingInt(a) > @backingInt(b)) a else b;
}

fn child(self: *Plan, parent: u32, index: u32, depth: usize) !void {
    // Bound instructions are a topological DAG, never an external dependency.
    if (index >= parent) return error.InvalidSqlProgram;
    try self.visit(index, depth + 1);
    self.nodes[parent].volatility = merge(self.nodes[parent].volatility, self.nodes[index].volatility);
}

fn alias(self: *Plan, index: u32, target: u32) void {
    self.nodes[index].alias = target;
    self.nodes[index].volatility = self.nodes[target].volatility;
    self.constants[index] = self.constants[target];
}

fn boolean(self: *Plan, index: u32) ?bool {
    const value = self.constants[index] orelse return null;
    return if (!value.sql_null and value.value == .bool) value.value.bool else null;
}

fn visit(self: *Plan, index: u32, depth: usize) anyerror!void {
    if (self.limits.checkpoint) |check| try check(self.limits.checkpoint_context);
    if (index >= self.nodes.len) return error.InvalidSqlProgram;
    if (depth >= self.limits.depth) return error.SqlProgramLimitExceeded;
    if (self.nodes[index].visited) return;
    self.nodes[index].visited = true;
    var constant = true;
    switch (self.nodes[index].operation) {
        .literal => {},
        .column, .parameter => constant = false,
        .unary => |part| {
            try self.child(index, part.operand, depth);
            constant = self.constants[part.operand] != null;
        },
        .cast => |part| {
            try self.child(index, part.operand, depth);
            const source = self.program.instructions[part.operand].type.kind;
            // Literal input functions were resolved during binding. Row
            // text↔timestamptz conversion depends on formatting/session state.
            if (!self.program.instructions[index].input_function and ((part.type == .datetime and source == .string) or (part.type == .string and source == .datetime)))
                self.nodes[index].volatility = merge(self.nodes[index].volatility, .stable);
            constant = self.constants[part.operand] != null;
        },
        .binary => |part| {
            try self.child(index, part.left, depth);
            if (part.op == .@"and" or part.op == .@"or") {
                const absorbing = part.op == .@"or";
                if (self.boolean(part.left)) |value| if (value == absorbing) {
                    self.alias(index, part.left);
                    return;
                };
                try self.child(index, part.right, depth);
                if (self.boolean(part.right)) |value| {
                    self.alias(index, if (value == absorbing) part.right else part.left);
                    return;
                }
                if (self.boolean(part.left) != null) {
                    self.alias(index, part.right);
                    return;
                }
            } else try self.child(index, part.right, depth);
            constant = self.constants[part.left] != null and self.constants[part.right] != null;
        },
        .call => |part| {
            self.nodes[index].volatility = part.function.volatility();
            var args: std.ArrayList(u32) = .empty;
            for (part.args) |arg| {
                try self.child(index, arg, depth);
                const known = self.constants[arg];
                if (part.function == .coalesce and known != null and known.?.sql_null) continue;
                if (part.function == .coalesce) try args.append(self.state.arena.allocator(), arg);
                constant = constant and known != null;
                if (part.function == .coalesce and known != null and !known.?.sql_null) break;
            }
            if (part.function == .coalesce) {
                if (args.items.len == 1) {
                    self.alias(index, args.items[0]);
                    return;
                }
                if (args.items.len == 0) {
                    self.constants[index] = .{};
                    return;
                }
                self.nodes[index].operation.call.args = args.items;
            }
        },
        .case_when => |part| {
            var branches: std.ArrayList(@TypeOf(part.branches[0])) = .empty;
            var otherwise = part.otherwise;
            for (part.branches) |branch| {
                try self.child(index, branch.condition, depth);
                if (self.constants[branch.condition]) |condition| {
                    if (condition.sql_null or self.boolean(branch.condition) == false) continue;
                    try self.child(index, branch.value, depth);
                    otherwise = branch.value;
                    break;
                }
                try self.child(index, branch.value, depth);
                try branches.append(self.state.arena.allocator(), branch);
            }
            if (otherwise) |other| try self.child(index, other, depth);
            if (branches.items.len == 0) {
                if (otherwise) |other| self.alias(index, other) else self.constants[index] = .{};
                return;
            }
            self.nodes[index].operation.case_when = .{ .branches = branches.items, .otherwise = otherwise };
            constant = false;
        },
        .in_list => |part| {
            try self.child(index, part.operand, depth);
            constant = self.constants[part.operand] != null;
            for (part.values) |value| {
                try self.child(index, value, depth);
                constant = constant and self.constants[value] != null;
            }
        },
    }
    if (constant and self.nodes[index].volatility == .immutable) self.constants[index] = self.evaluator.evaluate(index) catch |err| {
        // Keep the failing operation in the durable default program. Quota,
        // cancellation, corruption and allocation errors must still abort.
        if (self.policy == .defer_row_errors and @import("errors.zig").isRowEvaluationError(err)) return;
        return err;
    };
}

fn mark(self: *Plan, index: u32, depth: usize) anyerror!void {
    if (self.limits.checkpoint) |check| try check(self.limits.checkpoint_context);
    if (depth >= self.limits.depth) return error.SqlProgramLimitExceeded;
    if (self.nodes[index].reachable) return;
    self.nodes[index].reachable = true;
    if (self.nodes[index].alias) |target| return self.mark(target, depth + 1);
    if (self.constants[index] != null) return;
    switch (self.nodes[index].operation) {
        .literal, .column, .parameter => {},
        .unary => |part| try self.mark(part.operand, depth + 1),
        .cast => |part| try self.mark(part.operand, depth + 1),
        .binary => |part| {
            try self.mark(part.left, depth + 1);
            try self.mark(part.right, depth + 1);
        },
        .call => |part| for (part.args) |arg| try self.mark(arg, depth + 1),
        .case_when => |part| {
            for (part.branches) |branch| {
                try self.mark(branch.condition, depth + 1);
                try self.mark(branch.value, depth + 1);
            }
            if (part.otherwise) |other| try self.mark(other, depth + 1);
        },
        .in_list => |part| {
            try self.mark(part.operand, depth + 1);
            for (part.values) |value| try self.mark(value, depth + 1);
        },
    }
}

test "SQL immutable schema planning shares linear fold work and bounds preparation" {
    const a = std.testing.allocator;
    for ([_]usize{ 128, 512, 2048 }) |leaves| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const instructions = try arena.allocator().alloc(scalar.Instruction, leaves * 2 - 1);
        const kind: scalar.Type = .{ .kind = .integer, .element_type = .int64 };
        for (instructions[0..leaves]) |*instruction| instruction.* = .{ .type = kind, .operation = .{ .literal = .{ .integer = 1 } } };
        var first: usize = 0;
        var count = leaves;
        var next = leaves;
        while (count > 1) {
            const parent_first = next;
            for (0..count / 2) |i| {
                instructions[next] = .{ .type = kind, .operation = .{ .binary = .{ .op = .add, .left = @intCast(first + i * 2), .right = @intCast(first + i * 2 + 1) } } };
                next += 1;
            }
            first = parent_first;
            count /= 2;
        }
        const program: scalar.Program = .{ .arena = &arena, .instructions = instructions, .root = @intCast(next - 1), .output_type = kind, .parameter_types = &.{}, .parameter_descriptors = &.{}, .required_columns = &.{} };
        const start = std.Io.Clock.now(.awake, std.testing.io);
        var plan = try init(a, &program);
        defer plan.deinit();
        try std.testing.expectEqual(@as(i64, @intCast(leaves)), plan.constants[program.root].?.value.integer);
        try std.testing.expectEqual(leaves * 4 - 3, plan.evaluator.usedSteps());
        var reachable: usize = 0;
        for (plan.nodes) |node| reachable += @intFromBool(node.reachable);
        try std.testing.expectEqual(@as(usize, 1), reachable);
        std.debug.print("SQL immutable schema planning: nodes={} fold_steps={} peak_bytes={} elapsed_ns={}\n", .{ instructions.len, plan.evaluator.usedSteps(), plan.state.memory.peak, std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start.nanoseconds });
        try std.testing.expectError(error.SqlProgramLimitExceeded, initWithLimits(a, &program, .{ .nodes = instructions.len - 1 }));
        try std.testing.expectError(error.SqlProgramLimitExceeded, initWithLimits(a, &program, .{ .bytes = 1 }));
        try std.testing.expectError(error.SqlProgramLimitExceeded, initWithLimits(a, &program, .{ .steps = 3 }));
        try std.testing.expectError(error.SqlProgramLimitExceeded, initWithLimits(a, &program, .{ .depth = 1 }));
    }
}

test "SQL immutable schema planning unwinds allocation cuts without sampling stable values" {
    const Faults = struct {
        fn run(a: std.mem.Allocator) !void {
            var parsed = try @import("compiler.zig").compileScalar(a, "CASE WHEN s IS NULL THEN 'ok' WHEN true THEN 'yes' ELSE concat(s, 'x') END", .{});
            defer parsed.deinit();
            var program = try scalar.bind(a, parsed.expression, &.{.{ .name = "s", .type = .string }}, &.{}, .{});
            defer program.deinit();
            var plan = try init(a, &program);
            defer plan.deinit();
            try std.testing.expectEqual(scalar.Volatility.immutable, plan.nodes[program.root].volatility);
            for (program.instructions, plan.nodes) |instruction, node| {
                if (instruction.operation == .call and instruction.operation.call.function == .concat) try std.testing.expect(!node.visited and !node.reachable);
            }
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
}

test "SQL immutable schema planning cancels every phase and rejects malformed dependencies" {
    const Control = struct {
        calls: usize = 0,
        cancel_at: usize = std.math.maxInt(usize),
        fn check(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            const current = self.calls;
            self.calls += 1;
            if (current == self.cancel_at) return error.Cancelled;
        }
    };
    const a = std.testing.allocator;
    var compiled = try @import("compiler.zig").compileScalar(a, "CASE WHEN s IS NULL THEN lower('OK') ELSE lower(s) END", .{});
    defer compiled.deinit();
    var program = try scalar.bind(a, compiled.expression, &.{.{ .name = "s", .type = .string }}, &.{}, .{});
    defer program.deinit();
    var control: Control = .{};
    var plan = try initWithLimits(a, &program, .{ .checkpoint = Control.check, .checkpoint_context = &control });
    plan.deinit();
    const calls = control.calls;
    try std.testing.expect(calls > program.instructions.len);
    for (0..calls) |cut| {
        control = .{ .cancel_at = cut };
        try std.testing.expectError(error.Cancelled, initWithLimits(a, &program, .{ .checkpoint = Control.check, .checkpoint_context = &control }));
        try std.testing.expectEqual(cut + 1, control.calls);
    }
    var malformed = program;
    malformed.root = @intCast(program.instructions.len);
    try std.testing.expectError(error.InvalidSqlProgram, init(a, &malformed));
    const instructions = try a.dupe(scalar.Instruction, program.instructions);
    defer a.free(instructions);
    instructions[program.root].operation = .{ .unary = .{ .op = .not, .operand = program.root } };
    malformed = program;
    malformed.instructions = instructions;
    try std.testing.expectError(error.InvalidSqlProgram, init(a, &malformed));
}

test "SQL default planning defers row failures but preserves resource and cancellation failures" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const kind: scalar.Type = .{ .kind = .integer, .element_type = .int64 };
    const instructions = [_]scalar.Instruction{
        .{ .type = kind, .operation = .{ .literal = .{ .integer = 1 } } },
        .{ .type = kind, .operation = .{ .literal = .{ .integer = 0 } } },
        .{ .type = kind, .operation = .{ .binary = .{ .op = .divide, .left = 0, .right = 1 } } },
    };
    const program: scalar.Program = .{ .arena = &arena, .instructions = &instructions, .root = 2, .output_type = kind, .parameter_types = &.{}, .parameter_descriptors = &.{}, .required_columns = &.{} };
    try std.testing.expectError(error.SqlDivisionByZero, init(a, &program));
    {
        var plan = try initWithPolicy(a, &program, .{}, .defer_row_errors);
        defer plan.deinit();
        try std.testing.expect(plan.constants[program.root] == null);
        try std.testing.expect(plan.nodes[program.root].reachable);
    }
    try std.testing.expectError(error.SqlProgramLimitExceeded, initWithPolicy(a, &program, .{ .steps = 1 }, .defer_row_errors));
    const canceled = struct {
        fn check(_: ?*anyopaque) anyerror!void {
            return error.Canceled;
        }
    }.check;
    try std.testing.expectError(error.Canceled, initWithPolicy(a, &program, .{ .checkpoint = canceled }, .defer_row_errors));
    const Scenario = struct {
        fn run(alloc: std.mem.Allocator, value: *const scalar.Program) !void {
            var plan = try initWithPolicy(alloc, value, .{}, .defer_row_errors);
            defer plan.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(a, Scenario.run, .{&program});
}
