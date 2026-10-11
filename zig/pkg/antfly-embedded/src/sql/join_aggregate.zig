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
const Batch = @import("execution_batch.zig").Batch;

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
                .sum => if (input.spec.input_type != .integer or input.spec.input_element == .numeric) return null,
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
        partitions: ?*@import("partition_join.zig").Join = null,
        current_values: []const Datum = &.{},
        state_offsets: [2][]usize = .{ &.{}, &.{} },
        side_inputs: [2][]usize = .{ &.{}, &.{} },
        input_slots: [2][]usize = .{ &.{}, &.{} },
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
            if (self.partitions) |partitions| partitions.close();
            self.probe_arena.deinit();
            self.arena.deinit();
            self.a.destroy(self);
        }
        fn collect(self: *@This(), side: u1) !*operators.Grouped {
            const context = self.engine.context;
            const owned = self.arena.allocator();
            const specs = try owned.alloc(operators.AggregateSpec, self.side_inputs[side].len + 1);
            specs[0] = .{ .kind = .count };
            for (self.side_inputs[side], specs[1..]) |input, *spec| spec.* = self.plan.recipe.inputs[input].spec;
            const grouped = try operators.Grouped.create(self.a, specs, .{ .groups = context.limits.scan_rows, .bytes = context.limits.retained_bytes / 4, .spill = context.spill });
            errdefer grouped.deinit();
            const child = if ((side == 0) != self.iterator.flipped_join) self.iterator.left.? else self.iterator.right.?;
            var scratch = std.heap.ArenaAllocator.init(self.a);
            defer scratch.deinit();
            while (true) {
                _ = scratch.reset(.free_all);
                const temporary = scratch.allocator();
                try self.engine.checkpoint();
                const batch = try child.nextBatch(temporary, @min(context.limits.executionRows(), @max(@as(usize, 1), context.limits.retained_bytes / (16 * 1024 + child.node.columns.len * @sizeOf(Datum) * 16))), null);
                if (batch.len() == 0) break;
                const source = try temporary.create(Batch);
                source.* = batch;
                const keys = try temporary.alloc(Batch, self.plan.keys[side].len);
                for (self.plan.keys[side], keys) |column, *key| key.* = try source.project(temporary, column);
                const ones = try temporary.alloc(u32, batch.len());
                @memset(ones, 0);
                const one = try temporary.alloc(Datum, 1);
                one[0] = Datum.json(.{ .integer = 1 });
                const inputs = try temporary.alloc(Batch, specs.len);
                @memset(inputs, .{ .dictionary = .{ .values = one, .indices = ones } });
                for (self.side_inputs[side], inputs[1..]) |input, *value| value.* = try source.project(temporary, self.plan.inputs[input].?.column);
                try grouped.addEncodedColumns(keys, inputs, batch.len());
            }
            return grouped;
        }
        fn prepare(self: *@This()) !void {
            // Match the row executor's cost-based orientation and install its
            // bounded scan filter before pulling any probe rows.
            self.build_side = if (self.iterator.flipped_join) 0 else 1;
            for (0..2) |side_index| {
                const side: u1 = @intCast(side_index);
                const owned = self.arena.allocator();
                self.input_slots[side] = try owned.alloc(usize, self.plan.inputs.len);
                @memset(self.input_slots[side], std.math.maxInt(usize));
                var needed: std.ArrayList(usize) = .empty;
                for (self.plan.inputs, 0..) |input, index| if (input) |origin| {
                    if (origin.side == side) {
                        self.input_slots[side][index] = needed.items.len + 1;
                        try needed.append(owned, index);
                    }
                };
                self.side_inputs[side] = try needed.toOwnedSlice(owned);
                self.state_offsets[side] = try owned.alloc(usize, self.side_inputs[side].len + 1);
                var offset = self.plan.keys[side].len;
                for (self.state_offsets[side], 0..) |*entry, slot| {
                    entry.* = offset;
                    const spec = self.stateSpec(side, slot);
                    offset += if (spec.kind == .count) @as(usize, 1) else if (spec.kind == .sum) 3 else 2;
                }
            }
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
                const group = try groups.nextTypedPartialResult(scratch.allocator()) orelse break;
                const values = try self.pack(scratch.allocator(), group);
                const keys = group.keys[0..self.plan.join_count];
                if (self.partitions) |owner| {
                    try owner.add(true, values, keys, @intCast(group.ordinal));
                } else {
                    const admitted = try self.build.?.addBatchUntilFull(scratch.allocator(), .{ .rows = &.{values} }, &.{keys});
                    if (admitted == 0) {
                        const manager = context.spill orelse return error.SqlProgramLimitExceeded;
                        const owner = try @import("partition_join.zig").Join.create(self.a, manager, context.limits.retained_bytes / 4, context.limits.scan_rows, 0, false, false);
                        self.partitions = owner;
                        var position: usize = 0;
                        while (try self.build.?.unmatched(&position)) |match| {
                            const stored = try match.materializeValues(scratch.allocator());
                            try owner.add(true, stored, stored[0..self.plan.join_count], position - 1);
                        }
                        self.build.?.deinit();
                        self.build = null;
                        try owner.add(true, values, keys, @intCast(group.ordinal));
                    }
                }
                if (self.iterator.scan_filter) |filter| try filter.add(keys);
            }
            if (self.iterator.scan_filter) |filter| {
                filter.sealed = true;
                const scan = self.iterator.left.?.node.operation.scan;
                const cursor = self.engine.cursors[scan.index];
                _ = try cursor.set_dynamic_filter.?(cursor.ptr, filter);
            }
            self.probe_groups = try self.collect(1 - self.build_side);
            if (self.partitions) |owner| {
                while (true) {
                    _ = scratch.reset(.free_all);
                    const group = try self.probe_groups.?.nextTypedPartialResult(scratch.allocator()) orelse break;
                    try owner.add(false, try self.pack(scratch.allocator(), group), group.keys[0..self.plan.join_count], @intCast(group.ordinal));
                }
                self.probe_groups.?.deinit();
                self.probe_groups = null;
            }
            self.ready = true;
        }
        fn pack(_: *@This(), a_: A, group: operators.GroupResult) ![]const Datum {
            const states = group.typed_states orelse return error.InvalidSqlBackendResponse;
            var count = group.keys.len;
            for (states) |state| count += if (state.kind == .count) @as(usize, 1) else if (state.kind == .sum) 3 else 2;
            const values = try a_.alloc(Datum, count);
            @memcpy(values[0..group.keys.len], group.keys);
            var offset = group.keys.len;
            for (states) |state| {
                values[offset] = Datum.json(.{ .integer = @bitCast(state.count) });
                offset += 1;
                switch (state.kind) {
                    .count => {},
                    .sum => {
                        const sum: u128 = @bitCast(state.integer_sum);
                        values[offset] = Datum.json(.{ .integer = @bitCast(@as(u64, @truncate(sum))) });
                        values[offset + 1] = Datum.json(.{ .integer = @bitCast(@as(u64, @truncate(sum >> 64))) });
                        offset += 2;
                    },
                    .min, .max => {
                        values[offset] = if (state.selected) |selected| selected.row.values[0] else .{};
                        offset += 1;
                    },
                    .bool_and, .bool_or => {
                        values[offset] = Datum.json(.{ .bool = state.boolean });
                        offset += 1;
                    },
                    else => return error.InvalidSqlBackendResponse,
                }
            }
            return values;
        }
        fn stateSpec(self: *@This(), side: u1, slot: usize) operators.AggregateSpec {
            return if (slot != 0) self.plan.recipe.inputs[self.side_inputs[side][slot - 1]].spec else .{ .kind = .count };
        }
        fn countAt(values: []const Datum, offset: usize) !u64 {
            if (offset >= values.len or values[offset].sql_null or values[offset].value != .integer) return error.InvalidSqlSpill;
            return @bitCast(values[offset].value.integer);
        }
        fn stateAt(self: *@This(), a_: A, side: u1, slot: usize, values: []const Datum) !operators.Aggregate {
            const spec = self.stateSpec(side, slot);
            var state = try operators.Aggregate.initTyped(a_, spec.kind, spec.input_type, spec.input_element);
            var offset = self.state_offsets[side][slot];
            const count = try countAt(values, offset);
            offset += 1;
            switch (spec.kind) {
                .count => {},
                .sum => {
                    const low = try countAt(values, offset);
                    const high = try countAt(values, offset + 1);
                    state.integer_sum = @bitCast(@as(u128, low) | (@as(u128, high) << 64));
                },
                .min, .max => {
                    if (offset >= values.len) return error.InvalidSqlSpill;
                    if (count != 0) try state.update(values[offset]);
                },
                .bool_and, .bool_or => {
                    if (offset >= values.len or values[offset].sql_null or values[offset].value != .bool) return error.InvalidSqlSpill;
                    state.boolean = values[offset].value.bool;
                },
                else => return error.InvalidSqlSpill,
            }
            state.count = count;
            return state;
        }
        fn combine(self: *@This(), out: A, built: []const Datum, probed: []const Datum) !operators.GroupResult {
            const left = if (self.build_side == 0) built else probed;
            const right = if (self.build_side == 1) built else probed;
            const left_count = try countAt(left, self.plan.keys[0].len);
            const right_count = try countAt(right, self.plan.keys[1].len);
            const keys = try out.alloc(Datum, self.plan.output_keys.len);
            for (self.plan.output_keys, keys) |key, *value| value.* = try operators.cloneDatum(out, if (key.side == 0) left[key.index] else right[key.index]);
            const states = try out.alloc(operators.Aggregate, self.plan.inputs.len);
            for (self.plan.inputs, states, 0..) |input, *state, slot| {
                state.* = if (input) |source| try self.stateAt(out, source.side, self.input_slots[source.side][slot], if (source.side == 0) left else right) else try operators.Aggregate.init(out, .count, null);
                if (input) |source| {
                    const multiple = if (source.side == 0) right_count else left_count;
                    state.count = std.math.mul(u64, state.count, multiple) catch return error.SqlNumericOutOfRange;
                    if (state.kind == .sum) state.integer_sum = std.math.mul(i128, state.integer_sum, multiple) catch return error.SqlNumericOutOfRange;
                } else state.count = std.math.mul(u64, left_count, right_count) catch return error.SqlNumericOutOfRange;
            }
            const result: operators.GroupResult = .{ .keys = keys, .aggregates = &.{}, .typed_states = states, .ordinal = self.ordinal };
            self.ordinal = std.math.add(u64, self.ordinal, 1) catch return error.SqlNumericOutOfRange;
            return result;
        }
        fn next(raw: *anyopaque, out: A, maximum: u32) anyerror!?[]const operators.GroupResult {
            const self = from(raw);
            if (maximum == 0) return error.InvalidSqlBackendResponse;
            if (self.done) return null;
            if (!self.ready) try self.prepare();
            var results: std.ArrayList(operators.GroupResult) = .empty;
            while (results.items.len < maximum) {
                try self.engine.checkpoint();
                if (self.partitions) |owner| {
                    const pair = try owner.next() orelse {
                        self.done = true;
                        break;
                    };
                    try results.append(out, try self.combine(out, pair.right.?, pair.left.?));
                    continue;
                }
                if (self.probe) |*probe| if (try probe.next()) |match| {
                    const built = try match.materializeValues(out);
                    try results.append(out, try self.combine(out, built, self.current_values));
                    continue;
                };
                self.probe = null;
                _ = self.probe_arena.reset(.free_all);
                self.current = try self.probe_groups.?.nextTypedPartialResult(self.probe_arena.allocator());
                if (self.current == null) {
                    self.done = true;
                    break;
                }
                self.current_values = try self.pack(self.probe_arena.allocator(), self.current.?);
                self.probe = try self.build.?.probe(self.current.?.keys[0..self.plan.join_count]);
            }
            return if (results.items.len == 0) null else try results.toOwnedSlice(out);
        }
    };
    const state = try a.create(State);
    state.* = .{ .a = a, .engine = engine, .iterator = iterator, .plan = plan, .arena = .init(a), .probe_arena = .init(a) };
    return .{ .ptr = state, .next = State.next, .close = State.close };
}
