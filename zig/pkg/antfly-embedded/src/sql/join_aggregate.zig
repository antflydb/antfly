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

//! Exact partial aggregation before inner equijoins. Both inputs group by
//! join keys and the requested grouping keys. Multiplicity crosses the join
//! as COUNT state, avoiding expansion of duplicate input rows. Floating SUM,
//! AVG, DISTINCT, residual ON predicates and outer joins retain row execution.
const std = @import("std");
const A = std.mem.Allocator;
const binding = @import("relation_binding.zig");
const catalog = @import("catalog.zig");
const materialization = @import("aggregate_materialization.zig");
const operators = @import("operators.zig");
const scalar = @import("scalar.zig");
const Datum = scalar.Datum;
const partial = @import("aggregate_partial.zig");

pub const Plan = struct {
    const OutputKey = struct { side: u1, index: usize };
    const Input = struct { side: u1, column: u32 };
    recipe: materialization.Recipe,
    join_count: usize,
    keys: [2][]const u32,
    /// Every query key maps to one side's grouped key vector.
    output_keys: []const OutputKey,
    inputs: []const ?Input,

    pub fn prove(a: A, node: *const binding.Node, recipe: materialization.Recipe) !?Plan {
        if (node.operation != .join) return null;
        const join = node.operation.join;
        if (join.kind != .inner or join.correlation or join.membership != null or join.left_keys.len == 0 or join.left_keys.len != join.right_keys.len) return null;
        const condition = join.condition orelse return null;
        for (join.left_keys, join.right_keys) |left, right| {
            if (left.instructions.len != 1 or right.instructions.len != 1 or left.instructions[0].operation != .column or right.instructions[0].operation != .column) return null;
        }
        if (!equijoin(condition, condition.root, join.left_keys, join.right_keys, join.left.columns.len, 0)) return null;
        for (recipe.inputs) |input| {
            if (input.spec.distinct) return null;
            switch (input.spec.kind) {
                .count, .min, .max, .bool_and, .bool_or => {},
                .sum => if (input.spec.input_type != .integer) return null,
                else => return null,
            }
        }
        var keys: [2]std.ArrayList(u32) = .{ .empty, .empty };
        defer for (&keys) |*list| list.deinit(a);
        for (join.left_keys, join.right_keys) |left, right| {
            if (left.instructions.len != 1 or right.instructions.len != 1 or left.instructions[0].operation != .column or right.instructions[0].operation != .column) return null;
            try keys[0].append(a, left.instructions[0].operation.column);
            try keys[1].append(a, right.instructions[0].operation.column);
        }
        const outputs = try a.alloc(OutputKey, recipe.keys.len);
        errdefer a.free(outputs);
        const width = join.left.columns.len;
        for (recipe.keys, outputs) |key, *output| {
            const column = findColumn(node, key.path) orelse {
                a.free(outputs);
                return null;
            };
            const side: u1 = @intFromBool(column >= width);
            const ordinal: u32 = @intCast(if (side == 0) column else column - width);
            const index = std.mem.indexOfScalar(u32, keys[side].items, ordinal) orelse append: {
                try keys[side].append(a, ordinal);
                break :append keys[side].items.len - 1;
            };
            output.* = .{ .side = side, .index = index };
        }
        const inputs = try a.alloc(?Input, recipe.inputs.len);
        errdefer a.free(inputs);
        for (recipe.inputs, inputs) |input, *out| {
            out.* = null;
            if (input.column) |key| {
                const column = findColumn(node, key.path) orelse {
                    a.free(outputs);
                    a.free(inputs);
                    return null;
                };
                out.* = .{ .side = @intFromBool(column >= width), .column = @intCast(if (column < width) column else column - width) };
            }
        }
        const left = try keys[0].toOwnedSlice(a);
        errdefer a.free(left);
        return .{ .recipe = recipe, .join_count = join.left_keys.len, .keys = .{ left, try keys[1].toOwnedSlice(a) }, .output_keys = outputs, .inputs = inputs };
    }
    fn equijoin(program: scalar.Program, index: u32, left: []const scalar.Program, right: []const scalar.Program, width: usize, depth: usize) bool {
        if (depth >= 128 or index >= program.instructions.len) return false;
        const operation = program.instructions[index].operation;
        if (operation != .binary) return false;
        const binary = operation.binary;
        if (binary.op == .@"and") return equijoin(program, binary.left, left, right, width, depth + 1) and equijoin(program, binary.right, left, right, width, depth + 1);
        if (binary.op != .eq or binary.left >= program.instructions.len or binary.right >= program.instructions.len) return false;
        const l = program.instructions[binary.left].operation;
        const r = program.instructions[binary.right].operation;
        if (l != .column or r != .column) return false;
        for (left, right) |lk, rk| {
            const lc = lk.instructions[0].operation.column;
            const rc = width + rk.instructions[0].operation.column;
            if ((l.column == lc and r.column == rc) or (r.column == lc and l.column == rc)) return true;
        }
        return false;
    }
    fn findColumn(node: *const binding.Node, path: []const u8) ?usize {
        for (node.columns, 0..) |column, i| if (std.mem.eql(u8, path, column.internal)) return i;
        return null;
    }
};

/// Source and engine are statement-owned. The cursor owns every retained
/// reducer, spill file and probe buffer, and never retains a request page.
pub fn open(a: A, engine: anytype, iterator: anytype, plan: Plan) !catalog.AggregatePartialCursor {
    const Engine = @TypeOf(engine);
    const Iterator = @TypeOf(iterator);
    const State = struct {
        a: A,
        engine: Engine,
        iterator: Iterator,
        plan: Plan,
        arena: std.heap.ArenaAllocator,
        probe_arena: std.heap.ArenaAllocator,
        probe_groups: ?*operators.Grouped = null,
        build_side: u1 = 1,
        build: ?*operators.HashJoin = null,
        current: ?operators.GroupResult = null,
        probe: ?operators.HashJoin.Probe = null,
        ordinal: u64 = 0,
        ready: bool = false,
        done: bool = false,
        fn from(raw: *anyopaque) *@This() {
            return @ptrCast(@alignCast(raw));
        }
        fn close(raw: *anyopaque) void {
            const self = from(raw);
            if (self.probe_groups) |groups| groups.deinit();
            if (self.build) |build| build.deinit();
            self.probe_arena.deinit();
            self.arena.deinit();
            self.a.destroy(self);
        }
        fn collect(self: *@This(), side: u1) !*operators.Grouped {
            const context = self.engine.context;
            const owned = self.arena.allocator();
            const specs = try owned.alloc(operators.AggregateSpec, self.plan.inputs.len + 1);
            specs[0] = .{ .kind = .count };
            for (self.plan.inputs, self.plan.recipe.inputs, specs[1..]) |input, recipe, *spec| {
                spec.* = if (input != null and input.?.side == side) recipe.spec else .{ .kind = .count };
            }
            const grouped = try operators.Grouped.create(self.a, specs, .{ .groups = context.limits.scan_rows, .bytes = context.limits.retained_bytes / 4, .spill = context.spill });
            errdefer grouped.deinit();
            const child = if ((side == 0) != self.iterator.flipped_join) self.iterator.left.? else self.iterator.right.?;
            var scratch = std.heap.ArenaAllocator.init(self.a);
            defer scratch.deinit();
            while (true) {
                _ = scratch.reset(.free_all);
                const temporary = scratch.allocator();
                try self.engine.checkpoint();
                const row = try child.next(temporary) orelse break;
                const keys = try temporary.alloc(Datum, self.plan.keys[side].len);
                for (self.plan.keys[side], keys) |column, *key| key.* = row[column];
                const inputs = try temporary.alloc(Datum, specs.len);
                @memset(inputs, Datum.json(.{ .integer = 1 }));
                for (self.plan.inputs, inputs[1..]) |input, *value| if (input) |source| {
                    if (source.side == side) value.* = row[source.column];
                };
                try grouped.add(keys, inputs);
            }
            return grouped;
        }
        fn prepare(self: *@This()) !void {
            // Match the row executor's cost-based orientation and install its
            // bounded scan filter before pulling any probe rows.
            self.build_side = if (self.iterator.flipped_join) 0 else 1;
            const join = self.iterator.node.operation.join;
            try self.iterator.prepareScanFilter(if (self.build_side == 0) join.right_keys else join.left_keys);
            const groups = try self.collect(self.build_side);
            defer groups.deinit();
            const context = self.engine.context;
            self.build = try operators.HashJoin.create(self.a, .{ .rows = context.limits.scan_rows, .bytes = context.limits.retained_bytes / 4, .spill = context.spill });
            var scratch = std.heap.ArenaAllocator.init(self.a);
            defer scratch.deinit();
            while (true) {
                _ = scratch.reset(.free_all);
                try self.engine.checkpoint();
                const group = try groups.nextPartialResult(scratch.allocator()) orelse break;
                // The encoded states and grouping keys are copied into typed
                // build columns; neither source reducer nor scratch survives.
                const values = try scratch.allocator().alloc(Datum, group.keys.len + group.aggregates.len);
                @memcpy(values[0..group.keys.len], group.keys);
                @memcpy(values[group.keys.len..], group.aggregates);
                const keys = group.keys[0..self.plan.join_count];
                try self.build.?.add(values, keys);
                if (self.iterator.scan_filter) |filter| try filter.add(keys);
            }
            if (self.iterator.scan_filter) |filter| {
                filter.sealed = true;
                const scan = self.iterator.left.?.node.operation.scan;
                const cursor = self.engine.cursors[scan.index];
                _ = try cursor.set_dynamic_filter.?(cursor.ptr, filter);
            }
            self.probe_groups = try self.collect(1 - self.build_side);
            self.ready = true;
        }
        fn next(raw: *anyopaque, out: A, maximum: u32) anyerror!?[]const operators.GroupResult {
            const self = from(raw);
            if (maximum == 0) return error.InvalidSqlBackendResponse;
            if (self.done) return null;
            if (!self.ready) try self.prepare();
            var results: std.ArrayList(operators.GroupResult) = .empty;
            while (results.items.len < maximum) {
                try self.engine.checkpoint();
                if (self.probe) |*probe| if (try probe.next()) |match| {
                    const built = try match.materializeValues(out);
                    const current = self.current.?;
                    const build_keys = built[0..self.plan.keys[self.build_side].len];
                    const build_states = built[self.plan.keys[self.build_side].len..];
                    const left_keys = if (self.build_side == 0) build_keys else current.keys;
                    const right_keys = if (self.build_side == 1) build_keys else current.keys;
                    const left_states = if (self.build_side == 0) build_states else current.aggregates;
                    const right_states = if (self.build_side == 1) build_states else current.aggregates;
                    var left_count = try partial.decode(out, left_states[0], .{ .kind = .count });
                    defer left_count.deinit();
                    var right_count = try partial.decode(out, right_states[0], .{ .kind = .count });
                    defer right_count.deinit();
                    const keys = try out.alloc(Datum, self.plan.output_keys.len);
                    for (self.plan.output_keys, keys) |key, *value| value.* = try operators.cloneDatum(out, if (key.side == 0) left_keys[key.index] else right_keys[key.index]);
                    const values = try out.alloc(Datum, self.plan.inputs.len);
                    for (self.plan.inputs, self.plan.recipe.inputs, values, 0..) |input, recipe, *value, slot| {
                        var state = if (input) |source| try partial.decode(out, if (source.side == 0) left_states[slot + 1] else right_states[slot + 1], recipe.spec) else try operators.Aggregate.init(out, .count, null);
                        defer state.deinit();
                        if (input) |source| {
                            const multiple = if (source.side == 0) right_count.count else left_count.count;
                            state.count = std.math.mul(u64, state.count, multiple) catch return error.SqlNumericOutOfRange;
                            if (state.kind == .sum) state.integer_sum = std.math.mul(i128, state.integer_sum, multiple) catch return error.SqlNumericOutOfRange;
                        } else state.count = std.math.mul(u64, left_count.count, right_count.count) catch return error.SqlNumericOutOfRange;
                        value.* = try partial.cell(out, state);
                    }
                    try results.append(out, .{ .keys = keys, .aggregates = values, .ordinal = self.ordinal });
                    self.ordinal = std.math.add(u64, self.ordinal, 1) catch return error.SqlNumericOutOfRange;
                    continue;
                };
                self.probe = null;
                _ = self.probe_arena.reset(.free_all);
                self.current = try self.probe_groups.?.nextPartialResult(self.probe_arena.allocator());
                if (self.current == null) {
                    self.done = true;
                    break;
                }
                self.probe = try self.build.?.probe(self.current.?.keys[0..self.plan.join_count]);
            }
            return if (results.items.len == 0) null else try results.toOwnedSlice(out);
        }
    };
    const state = try a.create(State);
    state.* = .{ .a = a, .engine = engine, .iterator = iterator, .plan = plan, .arena = .init(a), .probe_arena = .init(a) };
    return .{ .ptr = state, .next = State.next, .close = State.close };
}
