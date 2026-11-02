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
        if (!equijoin(condition, condition.root, join.left_keys, join.right_keys, join.left.columns.len, join.output_order, 0)) return null;
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
            const logical = findColumn(node, key.path) orelse {
                a.free(outputs);
                return null;
            };
            const column = if (join.output_order) |order| order[logical] else logical;
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
                const logical = findColumn(node, key.path) orelse {
                    a.free(outputs);
                    a.free(inputs);
                    return null;
                };
                const column = if (join.output_order) |order| order[logical] else logical;
                out.* = .{ .side = @intFromBool(column >= width), .column = @intCast(if (column < width) column else column - width) };
            }
        }
        const left = try keys[0].toOwnedSlice(a);
        errdefer a.free(left);
        return .{ .recipe = recipe, .join_count = join.left_keys.len, .keys = .{ left, try keys[1].toOwnedSlice(a) }, .output_keys = outputs, .inputs = inputs };
    }
    /// When every join key survives in the output, each compact matched pair
    /// already identifies a final group. Another hash reduction cannot help.
    fn canReducePairs(self: Plan) bool {
        for (0..self.join_count) |index| {
            const present = for (self.output_keys) |key| {
                if (key.index == index) break true;
            } else false;
            if (!present) return true;
        }
        return false;
    }
    fn equijoin(program: scalar.Program, index: u32, left: []const scalar.Program, right: []const scalar.Program, width: usize, output_order: ?[]const usize, depth: usize) bool {
        if (depth >= 128 or index >= program.instructions.len) return false;
        const operation = program.instructions[index].operation;
        if (operation != .binary) return false;
        const binary = operation.binary;
        if (binary.op == .@"and") return equijoin(program, binary.left, left, right, width, output_order, depth + 1) and equijoin(program, binary.right, left, right, width, output_order, depth + 1);
        if (binary.op != .eq or binary.left >= program.instructions.len or binary.right >= program.instructions.len) return false;
        const l = program.instructions[binary.left].operation;
        const r = program.instructions[binary.right].operation;
        if (l != .column or r != .column) return false;
        const logical_left = if (output_order) |order| order[l.column] else l.column;
        const logical_right = if (output_order) |order| order[r.column] else r.column;
        for (left, right) |lk, rk| {
            const lc = lk.instructions[0].operation.column;
            const rc = width + rk.instructions[0].operation.column;
            if ((logical_left == lc and logical_right == rc) or (logical_right == lc and logical_left == rc)) return true;
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
        probe_groups: ?*Collected = null,
        build_side: u1 = 1,
        build: ?*operators.HashJoin = null,
        partitions: ?*@import("partition_join.zig").Join = null,
        reduced: ?*operators.Grouped = null,
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
            if (self.reduced) |groups| groups.deinit();
            self.probe_arena.deinit();
            self.arena.deinit();
            self.a.destroy(self);
        }
        /// Key partitions own disjoint groups. The join consumes their typed
        /// states directly instead of copying every key into a serial reducer.
        /// Keep the locked allocator in this stable owner until all lanes close.
        const Collected = struct {
            backing: A,
            locked: @import("parallel_scheduler.zig").LockedAllocator,
            groups: [4]?*operators.Grouped = @splat(null),
            position: usize = 0,
            fn deinit(self: *@This()) void {
                for (self.groups) |group| if (group) |owned| owned.deinit();
                self.backing.destroy(self);
            }
            fn nextTypedPartialResult(self: *@This(), a_: A) !?operators.GroupResult {
                while (self.position < self.groups.len) {
                    if (self.groups[self.position]) |group| if (try group.nextTypedPartialResult(a_)) |partial| return partial;
                    self.position += 1;
                }
                return null;
            }
            fn groupCount(self: *const @This()) usize {
                var count: usize = 0;
                for (self.groups) |group| if (group) |owned| {
                    count +|= owned.groupCount();
                };
                return count;
            }
        };
        fn collectLane(group: *operators.Grouped, keys: []const Batch, inputs: []const Batch, selection: []const usize, hashes: []const ?u64, ordinal: u64) anyerror!void {
            var scratch = std.heap.ArenaAllocator.init(group.backing);
            defer scratch.deinit();
            const lane_alloc = scratch.allocator();
            const local_keys = try lane_alloc.alloc(Batch, keys.len);
            const local_inputs = try lane_alloc.alloc(Batch, inputs.len);
            for (keys, local_keys) |*column, *out| out.* = try column.select(lane_alloc, selection);
            for (inputs, local_inputs) |*column, *out| out.* = try column.select(lane_alloc, selection);
            const ordinals = try lane_alloc.alloc(u64, selection.len);
            const selected_hashes = try lane_alloc.alloc(?u64, selection.len);
            for (selection, ordinals, selected_hashes) |index, *out, *hash| {
                out.* = try std.math.add(u64, ordinal, index);
                hash.* = hashes[index];
            }
            try group.addEncodedColumnsIdentified(local_keys, local_inputs, selection.len, .{ .ordinals = ordinals, .hashes = selected_hashes });
        }
        fn parallelReducers(specs: []const operators.AggregateSpec) bool {
            for (specs) |spec| {
                if (spec.distinct or spec.kind == .pattern_set) return false;
                // Preserve ordered floating SUM/AVG behavior. Exact integer
                // and NUMERIC states can be merged without rounding changes.
                if ((spec.kind == .sum or spec.kind == .avg) and spec.input_type != .integer and spec.input_element != .numeric) return false;
                // Equal NUMERIC values can carry different display scales;
                // floating extrema can carry signed zero. Preserve their
                // ordered representative until typed states retain tie ordinals.
                if (spec.kind == .min or spec.kind == .max) switch (spec.input_type orelse return false) {
                    .integer, .string, .boolean, .datetime => {},
                    else => return false,
                };
            }
            return true;
        }
        fn collect(self: *@This(), side: u1) !*Collected {
            const context = self.engine.context;
            const owned = self.arena.allocator();
            const specs = try owned.alloc(operators.AggregateSpec, self.side_inputs[side].len + 1);
            specs[0] = .{ .kind = .count };
            for (self.side_inputs[side], specs[1..]) |input, *spec| spec.* = self.plan.recipe.inputs[input].spec;
            const collected = try self.a.create(Collected);
            collected.* = .{ .backing = self.a, .locked = .{ .backing = self.a } };
            errdefer collected.deinit();
            const grouped = try operators.Grouped.create(collected.locked.allocator(), specs, .{ .groups = context.limits.scan_rows, .bytes = context.limits.retained_bytes / 4, .spill = context.spill, .coordinator_scope = context.backend.coordinator_scope, .control = if (context.backend.scalar_control) |owner| .{ .ptr = owner.ptr, .checkpoint = owner.checkpoint } else null });
            collected.groups[0] = grouped;
            const child = if ((side == 0) != self.iterator.flipped_join) self.iterator.left.? else self.iterator.right.?;
            var scratch = std.heap.ArenaAllocator.init(self.a);
            defer scratch.deinit();
            const scheduler = @import("parallel_scheduler.zig");
            const lanes = &collected.groups;
            var lane_count: usize = 0;
            var parallel_keys = true;
            for (self.plan.keys[side]) |column| switch (child.node.columns[column].type) {
                .integer, .string, .boolean, .datetime => {},
                else => parallel_keys = false,
            };
            var ordinal: u64 = 0;
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
                const io = context.backend.execution_io;
                if (ordinal == 0 and lane_count == 0 and io != null and context.backend.scalar_control == null and context.backend.coordinator_scope == null and batch.len() >= 256 and parallel_keys and parallelReducers(specs)) {
                    const requested = @min(4, scheduler.global().fanout(@min(4, batch.len() / 128), context.limits.retained_bytes / 4, 1024 * 1024));
                    if (requested > 1) {
                        // Choose fixed ownership before consuming the first page.
                        for (lanes[1..requested]) |*lane| lane.* = try operators.Grouped.create(collected.locked.allocator(), specs, .{ .groups = context.limits.scan_rows, .bytes = context.limits.retained_bytes / 4 / requested, .spill = context.spill, .coordinator_scope = context.backend.coordinator_scope, .control = if (context.backend.scalar_control) |owner| .{ .ptr = owner.ptr, .checkpoint = owner.checkpoint } else null });
                        grouped.limits.bytes = context.limits.retained_bytes / 4 / requested;
                        grouped.budget.limit = grouped.limits.bytes;
                        lane_count = requested;
                    }
                }
                if (lane_count == 0) {
                    try grouped.addEncodedColumns(keys, inputs, batch.len());
                } else {
                    // Hash once and scatter stable row selections. Equal SQL
                    // keys always reach one owner, across every input page.
                    const hashes = try @import("batch_hash.zig").encodedColumns(temporary, keys, batch.len(), true);
                    var counts: [4]usize = @splat(0);
                    for (hashes) |hash| counts[@intCast(hash.? % lane_count)] += 1;
                    const selected = try temporary.alloc(usize, batch.len());
                    var starts: [5]usize = @splat(0);
                    for (0..lane_count) |lane| starts[lane + 1] = starts[lane] + counts[lane];
                    var positions = starts;
                    for (hashes, 0..) |hash, index| {
                        const lane: usize = @intCast(hash.? % lane_count);
                        selected[positions[lane]] = index;
                        positions[lane] += 1;
                    }
                    var tasks: [3]?scheduler.Task(anyerror!void) = @splat(null);
                    // Join every lane while the borrowed input page is alive,
                    // including submission failures and canceled/error paths.
                    defer for (&tasks) |*slot| if (slot.*) |*task| {
                        task.cancel(io.?) catch {};
                    };
                    for (1..lane_count) |lane| {
                        const selection = selected[starts[lane]..starts[lane + 1]];
                        tasks[lane - 1] = scheduler.global().submit(io.?, lanes[lane].?.limits.bytes, collectLane, .{ lanes[lane].?, keys, inputs, selection, hashes, ordinal });
                        if (tasks[lane - 1] == null) try collectLane(lanes[lane].?, keys, inputs, selection, hashes, ordinal);
                    }
                    try collectLane(lanes[0].?, keys, inputs, selected[0..starts[1]], hashes, ordinal);
                    for (&tasks) |*slot| if (slot.*) |*task| {
                        var owned_task = task.*;
                        slot.* = null;
                        try owned_task.await(io.?);
                    };
                }
                ordinal += batch.len();
            }
            return collected;
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
            self.build = try operators.HashJoin.create(self.a, .{ .rows = context.limits.scan_rows, .bytes = context.limits.retained_bytes / 4, .spill = context.spill, .execution = if (context.backend.coordinator_scope) |scope| scope.execution else null, .control = if (context.backend.scalar_control) |owner| .{ .ptr = owner.ptr, .checkpoint = owner.checkpoint } else null });
            var scratch = std.heap.ArenaAllocator.init(self.a);
            defer scratch.deinit();
            var packed_bytes: u64 = 0;
            var packed_groups: u64 = 0;
            while (true) {
                _ = scratch.reset(.free_all);
                try self.engine.checkpoint();
                const group = try groups.nextTypedPartialResult(scratch.allocator()) orelse break;
                const values = try self.pack(scratch.allocator(), group);
                const keys = group.keys[0..self.plan.join_count];
                packed_bytes +|= try packedBytes(values);
                packed_groups += 1;
                if (self.partitions) |owner| {
                    try owner.add(true, values, keys, @intCast(group.ordinal));
                } else {
                    const admitted = try self.build.?.addBatchUntilFull(scratch.allocator(), .{ .rows = &.{values} }, &.{keys});
                    if (admitted == 0) {
                        const manager = context.spill orelse return error.SqlProgramLimitExceeded;
                        // Exact cardinality for resident groups; spilled groups
                        // expose an input-row upper bound. Observed compact
                        // widths include variable-sized keys and MIN/MAX values.
                        const estimated_bytes = (packed_bytes / packed_groups + 1) *| @as(u64, @intCast(groups.groupCount()));
                        const owner = try @import("partition_join.zig").Join.create(self.a, manager, context.limits.retained_bytes / 4, context.limits.scan_rows, estimated_bytes, false, false);
                        self.partitions = owner;
                        var transfer = std.heap.ArenaAllocator.init(self.a);
                        defer transfer.deinit();
                        var position: usize = 0;
                        while (try self.build.?.unmatched(&position)) |match| {
                            _ = transfer.reset(.free_all);
                            const stored = try match.materializeValues(transfer.allocator());
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
        const Owner = @This();
        const Compact = struct {
            owner: *Owner,
            sides: [2][]const Datum,
            fn read(raw: *const anyopaque, a_: A, slot: usize) !operators.Aggregate {
                const self: *const Compact = @ptrCast(@alignCast(raw));
                const owner = self.owner;
                if (slot >= owner.plan.inputs.len) return error.InvalidSqlBackendResponse;
                const left_count = try countAt(self.sides[0], owner.plan.keys[0].len);
                const right_count = try countAt(self.sides[1], owner.plan.keys[1].len);
                if (owner.plan.inputs[slot]) |source| {
                    var state = try owner.stateAt(a_, source.side, owner.input_slots[source.side][slot], self.sides[source.side]);
                    const multiple = if (source.side == 0) right_count else left_count;
                    state.count = std.math.mul(u64, state.count, multiple) catch return error.SqlNumericOutOfRange;
                    if (state.kind == .sum) state.integer_sum = std.math.mul(i128, state.integer_sum, multiple) catch return error.SqlNumericOutOfRange;
                    return state;
                }
                var state = try operators.Aggregate.init(a_, .count, null);
                state.count = std.math.mul(u64, left_count, right_count) catch return error.SqlNumericOutOfRange;
                return state;
            }
        };
        fn packedBytes(values: []const Datum) !usize {
            var bytes: usize = 0;
            for (values) |value| bytes +|= try operators.datumBytes(value);
            return bytes;
        }
        fn clonePacked(out: A, values: []const Datum) ![]const Datum {
            const owned = try out.alloc(Datum, values.len);
            for (values, owned) |value, *cell| cell.* = try operators.cloneDatum(out, value);
            return owned;
        }
        fn combine(self: *@This(), out: A, built: []const Datum, probed: []const Datum) !operators.GroupResult {
            const compact = try out.create(Compact);
            compact.* = .{ .owner = self, .sides = undefined };
            compact.sides[self.build_side] = try clonePacked(out, built);
            compact.sides[1 - self.build_side] = try clonePacked(out, probed);
            const keys = try out.alloc(Datum, self.plan.output_keys.len);
            for (self.plan.output_keys, keys) |key, *value| value.* = compact.sides[key.side][key.index];
            const result: operators.GroupResult = .{ .keys = keys, .aggregates = &.{}, .state_reader = .{ .ptr = compact, .count = self.plan.inputs.len, .read = Compact.read }, .ordinal = self.ordinal };
            self.ordinal = std.math.add(u64, self.ordinal, 1) catch return error.SqlNumericOutOfRange;
            return result;
        }
        /// Consume borrowed resident or partition pairs into typed group columns.
        /// No pair page, packed payload clone or interchange cell crosses the
        /// join/group boundary. The reducer owns its bounded spill fallback.
        fn reducePairs(self: *@This()) !void {
            const context = self.engine.context;
            const specs = try self.arena.allocator().alloc(operators.AggregateSpec, self.plan.recipe.inputs.len);
            for (specs, self.plan.recipe.inputs) |*spec, input| spec.* = input.spec;
            self.reduced = try operators.Grouped.create(self.a, specs, .{ .groups = context.limits.scan_rows, .bytes = context.limits.retained_bytes / 4, .spill = context.spill, .coordinator_scope = context.backend.coordinator_scope, .control = if (context.backend.scalar_control) |owner| .{ .ptr = owner.ptr, .checkpoint = owner.checkpoint } else null });
            var scratch = std.heap.ArenaAllocator.init(self.a);
            defer scratch.deinit();
            while (true) {
                _ = scratch.reset(.free_all);
                try self.engine.checkpoint();
                const a_ = scratch.allocator();
                var compact: Compact = .{ .owner = self, .sides = undefined };
                if (self.partitions) |owner| {
                    const pair = try owner.next() orelse break;
                    compact.sides[self.build_side] = pair.right.?;
                    compact.sides[1 - self.build_side] = pair.left.?;
                } else {
                    const built = try self.nextResidentPair(a_) orelse break;
                    compact.sides[self.build_side] = built;
                    compact.sides[1 - self.build_side] = self.current_values;
                }
                const keys = try a_.alloc(Datum, self.plan.output_keys.len);
                for (self.plan.output_keys, keys) |key, *value| value.* = compact.sides[key.side][key.index];
                const states = try a_.alloc(operators.Aggregate, specs.len);
                for (states, 0..) |*state, slot| state.* = try Compact.read(&compact, a_, slot);
                try self.reduced.?.importTypedPartialMapped(keys, states, null, self.ordinal);
                self.ordinal = std.math.add(u64, self.ordinal, 1) catch return error.SqlNumericOutOfRange;
            }
            if (self.partitions) |owner| owner.close();
            self.partitions = null;
            // Reduction owns all surviving keys and typed state. Release the
            // input groups before the upper aggregate starts delivery/merging.
            self.probe = null;
            self.current = null;
            self.current_values = &.{};
            _ = self.probe_arena.reset(.free_all);
            if (self.build) |build| build.deinit();
            self.build = null;
            if (self.probe_groups) |groups| groups.deinit();
            self.probe_groups = null;
        }
        // Pair values are borrowed until the next pull. The typed reducer
        // consumes them immediately, so resident joins need no output clones.
        fn nextResidentPair(self: *@This(), scratch: A) !?[]const Datum {
            while (true) {
                if (self.probe) |*probe| if (try probe.next()) |match| return try match.materializeValues(scratch);
                self.probe = null;
                _ = self.probe_arena.reset(.free_all);
                self.current = try self.probe_groups.?.nextTypedPartialResult(self.probe_arena.allocator());
                if (self.current == null) return null;
                self.current_values = try self.pack(self.probe_arena.allocator(), self.current.?);
                self.probe = try self.build.?.probe(self.current.?.keys[0..self.plan.join_count]);
            }
        }
        fn takeFinalGroups(raw: *anyopaque) anyerror!?*operators.Grouped {
            const self = from(raw);
            if (self.ready or self.done) return error.InvalidSqlBackendResponse;
            if (!self.plan.canReducePairs()) return null;
            try self.prepare();
            try self.reducePairs();
            const result = self.reduced.?;
            if (self.plan.recipe.keys.len == 0) try result.ensureGlobalGroup();
            self.reduced = null;
            self.done = true;
            return result;
        }
        fn next(raw: *anyopaque, out: A, maximum: u32) anyerror!?[]const operators.GroupResult {
            const self = from(raw);
            if (maximum == 0) return error.InvalidSqlBackendResponse;
            if (self.done) return null;
            if (!self.ready) try self.prepare();
            if (self.reduced == null and self.plan.canReducePairs()) try self.reducePairs();
            var results: std.ArrayList(operators.GroupResult) = .empty;
            const budget = @max(1, @min(self.engine.context.limits.page_bytes, self.engine.context.limits.retained_bytes / 16));
            var bytes: usize = 0;
            var scratch = std.heap.ArenaAllocator.init(self.a);
            defer scratch.deinit();
            // At most one compact pair may cross the byte boundary, preserving
            // progress for a single large key without retaining a second page.
            while (results.items.len < maximum and bytes < budget) {
                _ = scratch.reset(.free_all);
                try self.engine.checkpoint();
                if (self.partitions) |owner| {
                    const pair = try owner.next() orelse {
                        self.done = true;
                        break;
                    };
                    bytes +|= @sizeOf(Compact) + @sizeOf(operators.GroupResult) + self.plan.output_keys.len * @sizeOf(Datum) + try packedBytes(pair.right.?) + try packedBytes(pair.left.?);
                    try results.append(out, try self.combine(out, pair.right.?, pair.left.?));
                    continue;
                }
                if (self.reduced) |groups| {
                    const result = try groups.nextTypedPartialResult(out) orelse {
                        self.done = true;
                        break;
                    };
                    bytes +|= @sizeOf(operators.GroupResult) + try packedBytes(result.keys);
                    for (result.typed_states.?) |state| {
                        bytes +|= @sizeOf(operators.Aggregate);
                        if (state.selected) |selected| bytes +|= try operators.datumBytes(selected.row.values[0]);
                    }
                    try results.append(out, result);
                    continue;
                }
                if (self.probe) |*probe| if (try probe.next()) |match| {
                    const built = try match.materializeValues(scratch.allocator());
                    bytes +|= @sizeOf(Compact) + @sizeOf(operators.GroupResult) + self.plan.output_keys.len * @sizeOf(Datum) + try packedBytes(built) + try packedBytes(self.current_values);
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
    return .{ .ptr = state, .next = State.next, .close = State.close, .take_final_groups = State.takeFinalGroups };
}
