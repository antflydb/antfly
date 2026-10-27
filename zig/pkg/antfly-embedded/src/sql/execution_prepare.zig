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

//! Request-owned immutable expression validation. The same bounded walk serves
//! EXPLAIN and execution before native scan admission. It never mutates bound
//! programs, evaluates stable/volatile functions or consumes producer cursors.
const std = @import("std");
const scalar = @import("scalar.zig");
const catalog = @import("catalog.zig");
const describe = @import("describe.zig");
const relation = @import("relation_binding.zig");
const immutable = @import("immutable_plan.zig");
const A = std.mem.Allocator;
const Key = struct { instructions: usize, len: usize, root: u32, invocation: usize, settings: usize };
const State = struct {
    a: A,
    backend: catalog.Backend,
    programs: std.AutoHashMapUnmanaged(Key, void) = .empty,
    nodes: std.AutoHashMapUnmanaged(*const relation.Node, void) = .empty,
    steps: usize = 1_048_576,
    visited: usize = 0,
    fn check(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        try self.backend.vtable.checkpoint(self.backend.ptr);
    }
    fn program(self: *@This(), value: *const scalar.Program) !void {
        // Identity programs cannot contain a fallible immutable operation.
        if (value.instructions.len == 1) switch (value.instructions[0].operation) {
            .column, .parameter, .literal => return,
            else => {},
        };
        const key: Key = .{ .instructions = @intFromPtr(value.instructions.ptr), .len = value.instructions.len, .root = value.root, .invocation = if (value.invocation) |p| @intFromPtr(p) else 0, .settings = if (value.settings) |p| @intFromPtr(p) else 0 };
        if (self.programs.contains(key)) return;
        if (self.programs.count() == 8192) return error.SqlProgramLimitExceeded;
        try self.programs.put(self.a, key, {});
        if (value.instructions.len > self.steps) return error.SqlProgramLimitExceeded;
        self.steps -= value.instructions.len;
        var buffer: [8192]u8 = undefined;
        var scratch: std.heap.BufferFirstAllocator = .init(&buffer, self.a);
        const values: ?[]const scalar.Datum = if (value.invocation) |invocation| if (invocation.frame) |frame| frame.values else null else null;
        var plan = try immutable.initWithLimits(scratch.allocator(), value, .{ .steps = self.steps, .parameters = values, .checkpoint = check, .checkpoint_context = self });
        defer plan.deinit();
        self.steps -|= plan.evaluator.usedSteps();
    }
    /// A statically disabled Apply has no executable right subplan. Binding
    /// has already checked types, names and authorization for both branches.
    fn disabled(self: *@This(), value: *const scalar.Program) !bool {
        if (value.instructions.len > self.steps) return error.SqlProgramLimitExceeded;
        self.steps -= value.instructions.len;
        var buffer: [8192]u8 = undefined;
        var scratch: std.heap.BufferFirstAllocator = .init(&buffer, self.a);
        const parameters: ?[]const scalar.Datum = if (value.invocation) |invocation| if (invocation.frame) |frame| frame.values else null else null;
        var plan = try immutable.initWithLimits(scratch.allocator(), value, .{ .steps = self.steps, .parameters = parameters, .checkpoint = check, .checkpoint_context = self });
        defer plan.deinit();
        self.steps -|= plan.evaluator.usedSteps();
        const known = plan.constants[value.root] orelse return false;
        return known.sql_null or (known.value == .bool and !known.value.bool);
    }
    fn list(self: *@This(), values: []const scalar.Program) !void {
        for (values) |*value| try self.program(value);
    }
    fn scalars(self: *@This(), value: @import("bound_scalars.zig").Bound, needed: ?[]const bool) !void {
        if (value.predicate) |*predicate| try self.program(predicate);
        for (value.projections, 0..) |optional, index| if (optional) |*projection| {
            if (needed) |mask| if (index < mask.len and !mask[index]) continue;
            try self.program(projection);
        };
        for (value.orders) |optional| if (optional) |*ordering| try self.program(ordering);
    }
    fn bound(self: *@This(), value: describe.BoundStatement, needed: ?[]const bool, depth: usize) anyerror!void {
        if (depth > 64 or self.visited == 16384) return error.SqlProgramLimitExceeded;
        self.visited += 1;
        try check(self);
        try self.scalars(value.scalars, if (value.aggregate != null or value.window != null) null else needed);
        if (value.aggregate) |aggregate| {
            try self.scalars(aggregate.input, null);
            try self.list(aggregate.outputs);
            try self.list(aggregate.orders);
            if (aggregate.having) |*having| try self.program(having);
            for (aggregate.ordered) |ordered| if (ordered.direct) |*direct| try self.program(direct);
        }
        if (value.window) |window| {
            try self.bound(window.input.*, null, depth + 1);
            try self.list(window.outputs);
            try self.list(window.orders);
        }
        if (value.relation) |input| try self.node(input.root, depth + 1);
    }
    fn node(self: *@This(), value: *const relation.Node, depth: usize) anyerror!void {
        if (self.nodes.contains(value)) return;
        if (depth > 64 or self.nodes.count() == 8192) return error.SqlProgramLimitExceeded;
        try self.nodes.put(self.a, value, {});
        try check(self);
        switch (value.operation) {
            .query => |query| {
                try self.bound(query.binding, query.output_needed, depth + 1);
                try self.node(query.source, depth + 1);
            },
            .join => |join| {
                if (join.condition) |*condition| try self.program(condition);
                try self.list(join.left_keys);
                try self.list(join.right_keys);
                try self.node(join.left, depth + 1);
                try self.node(join.right, depth + 1);
            },
            .apply => |apply| {
                try self.node(apply.left, depth + 1);
                if (apply.demand) |*demand| if (try self.disabled(demand)) return;
                if (apply.condition) |*condition| try self.program(condition);
                try self.node(apply.right, depth + 1);
            },
            .set => |set| {
                try self.node(set.left, depth + 1);
                try self.node(set.right, depth + 1);
            },
            .recursive => |recursive| {
                try self.node(recursive.seed, depth + 1);
                try self.node(recursive.step, depth + 1);
            },
            .materialized_ref => |input| try self.node(input, depth + 1),
            .values => |arms| for (arms) |arm| try self.node(arm, depth + 1),
            .unnest => |unnest| {
                try self.list(unnest.arrays);
                try self.node(unnest.source, depth + 1);
            },
            .singleton, .scan, .recursive_ref, .outer_ref, .literal_rows, .prepared_rows => {},
        }
    }
};
pub fn validate(a: A, backend: catalog.Backend, bound: describe.BoundStatement) !void {
    var state: State = .{ .a = a, .backend = backend };
    defer state.programs.deinit(a);
    defer state.nodes.deinit(a);
    try state.bound(bound, null, 0);
}
