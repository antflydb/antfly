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

//! Partition exact typed states before reducing; use ordered merging for
//! order-sensitive aggregates and partitions that exceed bounded admission.
const std = @import("std");
const operators = @import("operators.zig");
const scalar = @import("scalar.zig");
const spill = @import("spill.zig");
const Datum = scalar.Datum;
const Allocator = std.mem.Allocator;
pub const Grouped = struct {
    const Reduction = struct {
        child: Grouped,
        exact: bool,
        output: ?*@import("parallel_output.zig").Pipe = null,
        task: ?@import("parallel_scheduler.zig").Task(anyerror!void) = null,
        child_closed: bool = false,
        fn run(self: *Reduction) anyerror!void {
            defer {
                self.child.deinit();
                self.child_closed = true;
            }
            var failure: ?anyerror = null;
            self.reduceInto(self.output.?) catch |err| {
                failure = err;
            };
            self.output.?.finish(failure);
        }
        fn reduceInto(self: *Reduction, output: *@import("parallel_output.zig").Pipe) !void {
            var arena = std.heap.ArenaAllocator.init(self.child.a);
            defer arena.deinit();
            while (true) {
                _ = arena.reset(.retain_capacity);
                const result = (try self.child.nextImpl(arena.allocator(), self.exact)) orelse break;
                try output.append(.{ .keys = result.keys, .values = result.aggregates, .ordinal = result.ordinal });
            }
        }
        fn close(self: *Reduction) void {
            if (self.output) |pipe| pipe.stop();
            if (self.task) |*task| {
                task.cancel(self.child.sort.manager.io) catch {};
                self.task = null;
            }
            const a = self.child.a;
            if (self.output) |pipe| pipe.close();
            if (!self.child_closed) self.child.deinit();
            a.destroy(self);
        }
    };
    a: Allocator,
    sort: spill.Sort,
    specs: []const operators.AggregateSpec,
    state_arena: std.heap.ArenaAllocator,
    read_arena: std.heap.ArenaAllocator,
    pending: ?operators.Row = null,
    output_count: usize = 0,
    partitioned: bool = false,
    partitions: [8]?spill.Sequential = @splat(null),
    partition_index: usize = 0,
    local: ?*operators.Grouped = null,
    fallback: ?*Grouped = null,
    bytes: usize,
    parallel: bool = true,
    reductions: [2]?*Reduction = @splat(null),
    next_lane: usize = 0,
    reductions_started: usize = 0,
    pub fn init(a: Allocator, manager: *spill.Manager, specs: []const operators.AggregateSpec, key_count: usize, bytes: usize) !Grouped {
        const backing = manager.allocator();
        _ = a;
        const orders = try backing.alloc(operators.Order, key_count);
        @memset(orders, .{});
        return .{ .a = backing, .sort = spill.Sort.init(backing, manager, orders, bytes / 4), .specs = specs, .state_arena = std.heap.ArenaAllocator.init(backing), .read_arena = std.heap.ArenaAllocator.init(backing), .bytes = bytes, .partitioned = bytes >= 64 * 1024 and for (specs) |spec| {
            if (spec.distinct or !(spec.kind == .count or spec.kind == .bool_and or spec.kind == .bool_or or (spec.kind == .sum and spec.input_type == .integer))) break false;
        } else true };
    }
    pub fn deinit(self: *Grouped) void {
        for (&self.reductions) |*slot| if (slot.*) |job| {
            job.close();
            slot.* = null;
        };
        for (&self.partitions) |*file| if (file.*) |*open| open.close();
        if (self.local) |local| local.deinit();
        if (self.fallback) |fallback| {
            fallback.deinit();
            self.a.destroy(fallback);
        }
        self.a.free(self.sort.orders);
        self.sort.deinit();
        self.state_arena.deinit();
        self.read_arena.deinit();
    }
    pub fn partial(self: *Grouped, keys: []const Datum, states: []const operators.Aggregate, ordinal: u64) !void {
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        const values = try a.alloc(Datum, 1 + states.len * 7);
        values[0] = Datum.json(.{ .bool = true });
        @memcpy(values[1..], try @import("aggregate_partial.zig").encode(a, states));
        try self.append(.{ .keys = keys, .values = values, .ordinal = ordinal });
        for (states, 0..) |state, slot| if (state.distinct) {
            for (state.distinct_values.items) |entry| {
                try self.append(.{ .keys = keys, .values = &.{ Datum.json(.{ .integer = @intCast(slot) }), entry.row.row.values[0] }, .ordinal = ordinal });
            }
            if (state.kind == .pattern_set and state.patterns.?.has_null) try self.append(.{ .keys = keys, .values = &.{ Datum.json(.{ .integer = @intCast(slot) }), .{} }, .ordinal = ordinal });
        };
    }
    pub fn appendPartial(self: *Grouped, keys: []const Datum, cells: []const Datum, ordinal: u64) !void {
        if (cells.len != self.specs.len * 7) return error.InvalidSqlSpill;
        const values = try self.a.alloc(Datum, cells.len + 1);
        defer self.a.free(values);
        values[0] = Datum.json(.{ .bool = true });
        @memcpy(values[1..], cells);
        try self.append(.{ .keys = keys, .values = values, .ordinal = ordinal });
    }
    pub fn add(self: *Grouped, keys: []const Datum, inputs: []const Datum, ordinal: u64) !void {
        const values = try self.a.alloc(Datum, inputs.len + 1);
        defer self.a.free(values);
        values[0] = Datum.json(.{ .bool = false });
        @memcpy(values[1..], inputs);
        try self.append(.{ .keys = keys, .values = values, .ordinal = ordinal });
    }
    fn append(self: *Grouped, row: operators.Row) !void {
        if (!self.partitioned) return self.sort.add(row);
        var hash = std.hash.Wyhash.init(0);
        for (row.keys) |key| {
            const value = if (key.sql_null) 0 else try scalar.semanticHash(key.value);
            var bytes: [9]u8 = undefined;
            bytes[0] = @intFromBool(key.sql_null);
            std.mem.writeInt(u64, bytes[1..9], value, .little);
            hash.update(&bytes);
        }
        const index = hash.final() % self.partitions.len;
        if (self.partitions[index] == null) {
            self.partitions[index] = try spill.Sequential.init(self.sort.manager, @min(4096, self.bytes / 128));
            self.partitions[index].?.buffer_bytes = 512;
        }
        _ = try self.partitions[index].?.append(row, spill.none);
    }
    fn nativeInputs(self: *Grouped, values: []const Datum) bool {
        if (values.len != self.specs.len) return false;
        for (values, self.specs) |value, spec| {
            if (value.sql_null or spec.kind == .count) continue;
            if (spec.kind == .sum and value.value != .integer) return false;
            if ((spec.kind == .bool_and or spec.kind == .bool_or) and value.value != .bool) return false;
        }
        return true;
    }
    fn startReductions(self: *Grouped, exact: bool) !void {
        for (&self.reductions) |*slot| {
            if (slot.* != null) continue;
            while (self.partition_index < self.partitions.len) {
                const index = self.partition_index;
                self.partition_index += 1;
                if (self.partitions[index] == null) continue;
                try self.partitions[index].?.seal();
                const job = try self.a.create(Reduction);
                job.* = .{ .child = Grouped.init(self.a, self.sort.manager, self.specs, self.sort.orders.len, self.bytes / 2) catch |err| {
                    self.a.destroy(job);
                    return err;
                }, .exact = exact };
                errdefer job.close();
                job.child.parallel = false;
                job.child.partitioned = true;
                job.child.sort.parallel_runs = false;
                job.child.partitions[0] = self.partitions[index];
                self.partitions[index] = null;
                job.output = try @import("parallel_output.zig").Pipe.create(self.sort.manager, self.bytes / 128);
                job.task = @import("parallel_scheduler.zig").global().submit(self.sort.manager.io, self.bytes / 2, Reduction.run, .{job});
                if (job.task == null) {
                    job.output.?.close();
                    job.output = null;
                } else self.reductions_started += 1;
                slot.* = job;
                break;
            }
        }
    }
    fn nextParallel(self: *Grouped, out: Allocator, exact: bool) !?operators.GroupResult {
        while (true) {
            try self.startReductions(exact);
            const lane = if (self.reductions[self.next_lane] != null) self.next_lane else 1 - self.next_lane;
            const job = self.reductions[lane] orelse return null;
            if (job.exact != exact) return error.InvalidSqlBackendResponse;
            if (job.output) |pipe| {
                if (try pipe.nextOwned(out)) |row| {
                    self.output_count += 1;
                    return .{ .keys = row.keys, .aggregates = row.values, .ordinal = row.ordinal };
                }
                if (job.task) |*task| {
                    const result = task.await(self.sort.manager.io);
                    job.task = null;
                    try result;
                }
                if (pipe.terminal_error) |err| return err;
            } else if (try job.child.nextImpl(out, exact)) |result| {
                self.output_count += 1;
                return result;
            }
            job.close();
            self.reductions[lane] = null;
            self.next_lane = 1 - lane;
        }
    }
    fn nextPartition(self: *Grouped, out: Allocator, exact: bool) anyerror!?operators.GroupResult {
        while (true) {
            if (self.local) |local| {
                if (try (if (exact) local.nextPartialResult(out) else local.nextResult(out))) |result| {
                    self.output_count += 1;
                    return result;
                }
                local.deinit();
                self.local = null;
            }
            if (self.fallback) |fallback| {
                if (try fallback.nextImpl(out, exact)) |result| {
                    self.output_count += 1;
                    return result;
                }
                fallback.deinit();
                self.a.destroy(fallback);
                self.fallback = null;
            }
            if (self.partition_index == self.partitions.len) return null;
            const index = self.partition_index;
            self.partition_index += 1;
            const file = if (self.partitions[index]) |*open| open else continue;
            try file.seal();
            self.local = try operators.Grouped.create(self.a, self.specs, .{ .bytes = self.bytes / 2, .groups = std.math.maxInt(usize) });
            var offset: u64 = 0;
            var scratch = std.heap.ArenaAllocator.init(self.a);
            defer scratch.deinit();
            while (offset < file.size) {
                try self.sort.manager.check();
                _ = scratch.reset(.retain_capacity);
                const row = try file.read(scratch.allocator(), offset);
                offset = row.following;
                if (row.row.values.len == 0 or row.row.values[0].value != .bool) return error.InvalidSqlSpill;
                const partial_state = row.row.values[0].value.bool;
                if (self.fallback == null and ((!partial_state and !self.nativeInputs(row.row.values[1..])) or !try self.local.?.canRetain(row.row.keys, if (partial_state) &.{} else row.row.values[1..]))) {
                    const fallback = try self.a.create(Grouped);
                    errdefer self.a.destroy(fallback);
                    fallback.* = try Grouped.init(self.a, self.sort.manager, self.specs, self.sort.orders.len, self.bytes);
                    fallback.partitioned = false;
                    fallback.sort.parallel_runs = self.parallel;
                    errdefer fallback.deinit();
                    try self.local.?.exportPartial(fallback);
                    self.local.?.deinit();
                    self.local = null;
                    self.fallback = fallback;
                }
                if (self.fallback) |fallback| {
                    try fallback.append(row.row);
                } else if (partial_state) {
                    try self.local.?.importPartial(row.row.keys, row.row.values[1..], row.row.ordinal);
                } else {
                    try self.local.?.addOrdered(row.row.keys, row.row.values[1..], row.row.ordinal);
                }
            }
            file.close();
            self.partitions[index] = null;
        }
    }
    fn same(left: []const Datum, right: []const Datum) !bool {
        if (left.len != right.len) return error.InvalidSqlSpill;
        for (left, right) |a, b| if (a.sql_null != b.sql_null or (!a.sql_null and (try scalar.compare(a.value, b.value)) != .eq)) return false;
        return true;
    }
    pub fn next(self: *Grouped, out: Allocator) !?operators.GroupResult {
        return self.nextImpl(out, false);
    }
    pub fn nextPartial(self: *Grouped, out: Allocator) !?operators.GroupResult {
        return self.nextImpl(out, true);
    }
    fn nextImpl(self: *Grouped, out: Allocator, exact: bool) anyerror!?operators.GroupResult {
        if (self.partitioned) return if (self.parallel and self.bytes >= 512 * 1024) self.nextParallel(out, exact) else self.nextPartition(out, exact);
        _ = self.state_arena.reset(.free_all);
        const a = self.state_arena.allocator();
        var row = self.pending orelse (try self.sort.next(self.read_arena.allocator())) orelse return null;
        self.pending = null;
        const keys = try a.alloc(Datum, row.keys.len);
        for (row.keys, keys) |key, *copy| copy.* = try operators.cloneDatum(a, key);
        var ordinal = row.ordinal;
        const states = try a.alloc(operators.Aggregate, self.specs.len);
        var initialized: usize = 0;
        defer for (states[0..initialized]) |*state| state.deinit();
        for (states, self.specs) |*state, spec| {
            state.* = try operators.Aggregate.init(self.a, spec.kind, spec.input_type);
            initialized += 1;
        }
        const pattern_sets = try a.alloc(?*@import("pattern_spill.zig").Set, states.len);
        for (self.specs, pattern_sets) |spec, *set| set.* = if (spec.kind == .pattern_set) try @import("pattern_spill.zig").Set.create(self.sort.manager) else null;
        var distinct = spill.Sort.init(self.a, self.sort.manager, &.{ .{}, .{} }, self.sort.memory_bytes);
        defer distinct.deinit();
        var distinct_ordinal: u64 = 0;
        while (true) {
            ordinal = @min(ordinal, row.ordinal);
            if (row.values.len == 0) return error.InvalidSqlSpill;
            if (row.values[0].value == .integer) {
                if (row.values.len != 2) return error.InvalidSqlSpill;
                const slot = std.math.cast(usize, row.values[0].value.integer) orelse return error.InvalidSqlSpill;
                if (slot >= states.len) return error.InvalidSqlSpill;
                try distinct.add(.{ .keys = &.{ row.values[0], row.values[1] }, .values = &.{row.values[1]}, .ordinal = distinct_ordinal });
                distinct_ordinal += 1;
            } else if (row.values[0].value != .bool) return error.InvalidSqlSpill else if (row.values[0].value.bool) {
                if (row.values.len != 1 + states.len * 7) return error.InvalidSqlSpill;
                for (states, 0..) |*state, i| try merge(state, row.values[1 + i * 7 ..][0..7]);
            } else {
                if (row.values.len != states.len + 1) return error.InvalidSqlSpill;
                for (states, row.values[1..], self.specs, 0..) |*state, input, spec, slot| {
                    if (spec.distinct) {
                        if (!input.sql_null or spec.kind == .pattern_set) {
                            try distinct.add(.{ .keys = &.{ Datum.json(.{ .integer = @intCast(slot) }), input }, .values = &.{input}, .ordinal = distinct_ordinal });
                            distinct_ordinal += 1;
                        }
                    } else try state.update(input);
                }
            }
            _ = self.read_arena.reset(.free_all);
            row = (try self.sort.next(self.read_arena.allocator())) orelse break;
            if (!try same(keys, row.keys)) {
                self.pending = row;
                break;
            }
        }
        if (distinct_ordinal != 0) {
            var previous = std.heap.ArenaAllocator.init(self.a);
            defer previous.deinit();
            var current = std.heap.ArenaAllocator.init(self.a);
            defer current.deinit();
            var last: ?[]const Datum = null;
            while (true) {
                _ = current.reset(.free_all);
                const record = (try distinct.next(current.allocator())) orelse break;
                if (last) |prior| if (try same(prior, record.keys)) continue;
                const slot = std.math.cast(usize, record.keys[0].value.integer) orelse return error.InvalidSqlSpill;
                if (slot >= states.len) return error.InvalidSqlSpill;
                if (pattern_sets[slot]) |set| try set.append(record.values[0]) else try states[slot].update(record.values[0]);
                _ = previous.reset(.free_all);
                const copy = try previous.allocator().alloc(Datum, 2);
                for (record.keys, copy) |v, *cell| cell.* = try operators.cloneDatum(previous.allocator(), v);
                last = copy;
            }
        }
        const output_keys = try out.alloc(Datum, keys.len);
        for (keys, output_keys) |key, *copy| copy.* = try operators.cloneDatum(out, key);
        const aggregates = if (exact) try @import("aggregate_partial.zig").encode(out, states) else try out.alloc(Datum, states.len);
        if (!exact) for (states, @constCast(aggregates), pattern_sets) |*state, *copy, set| {
            copy.* = if (set) |patterns| .{ .patterns = &patterns.interface, .sql_null = false } else try operators.cloneDatum(out, try state.finish());
        };
        self.output_count += 1;
        return .{ .keys = output_keys, .aggregates = aggregates, .ordinal = ordinal };
    }
    pub fn merge(state: *operators.Aggregate, cells: []const Datum) !void {
        const count = std.math.cast(u64, cells[0].value.integer) orelse return error.InvalidSqlSpill;
        if (count == 0) return;
        const total = std.math.add(u64, state.count, count) catch return error.SqlNumericOutOfRange;
        if (total > std.math.maxInt(i64)) return error.SqlNumericOutOfRange;
        switch (state.kind) {
            .count => {},
            .sum => if (state.input_type == .integer) {
                const sum = try std.fmt.parseInt(i128, cells[1].value.number_string, 10);
                state.integer_sum = std.math.add(i128, state.integer_sum, sum) catch return error.SqlNumericOutOfRange;
            } else {
                const adjusted = cells[2].value.float - state.compensation;
                const sum = state.number_sum + adjusted;
                if (!std.math.isFinite(sum)) return error.SqlNumericOutOfRange;
                state.compensation = (sum - state.number_sum) - adjusted;
                state.number_sum = sum;
            },
            .avg => {
                const incoming = cells[4].value.float;
                const fraction = @as(f64, @floatFromInt(count)) / @as(f64, @floatFromInt(total));
                const delta = incoming - state.mean;
                state.mean = if (state.count == 0) incoming else if (std.math.isFinite(delta)) state.mean + delta * fraction else state.mean * (1 - fraction) + incoming * fraction;
                if (!std.math.isFinite(state.mean)) return error.SqlNumericOutOfRange;
            },
            .bool_and => state.boolean = state.boolean and cells[5].value.bool,
            .bool_or => state.boolean = state.boolean or cells[5].value.bool,
            .min, .max => {
                try state.update(cells[6]);
            },
            .pattern_set => return error.UnsupportedSqlExecution,
        }
        state.count = total;
    }
};

test "SQL partitioned typed aggregation reduces repeated updates and preserves ordinals" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var written: [2]u64 = undefined;
    for ([_]bool{ false, true }, 0..) |partitioned, run| {
        var budget: @import("memory_budget.zig") = .{ .backing = std.heap.page_allocator, .limit = 512 * 1024 };
        const a = budget.allocator();
        {
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .async_writes = false, .compression = .none };
            defer manager.deinit();
            var group = try Grouped.init(a, &manager, &.{ .{ .kind = .count }, .{ .kind = .sum, .input_type = .integer } }, 1, 128 * 1024);
            group.partitioned = partitioned;
            defer group.deinit();
            for (0..8192) |index| {
                const key = Datum.json(.{ .integer = @intCast(index % 128) });
                try group.add(&.{key}, &.{ Datum.json(.{ .integer = 1 }), Datum.json(.{ .integer = @intCast(index % 7) }) }, index);
            }
            var seen: [128]bool = @splat(false);
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            while (true) {
                _ = scratch.reset(.retain_capacity);
                const result = (try group.next(scratch.allocator())) orelse break;
                const key: usize = @intCast(result.keys[0].value.integer);
                try std.testing.expect(!seen[key]);
                seen[key] = true;
                var sum: i64 = 0;
                for (0..64) |index| sum += @intCast((key + index * 128) % 7);
                try std.testing.expectEqual(@as(i64, 64), result.aggregates[0].value.integer);
                try std.testing.expectEqual(sum, result.aggregates[1].value.integer);
                try std.testing.expectEqual(@as(u64, key), result.ordinal);
            }
            for (seen) |found| try std.testing.expect(found);
            written[run] = manager.written_bytes;
        }
        try std.testing.expectEqual(@as(usize, 0), budget.live);
    }
    try std.testing.expect(written[1] < written[0] / 2);
    std.debug.print("SQL aggregate spill bytes: sorted={d} partitioned={d}\n", .{ written[0], written[1] });
}

test "SQL partitioned integer aggregation preserves invalid input errors" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    const a = std.testing.allocator;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .async_writes = false };
    defer manager.deinit();
    var group = try Grouped.init(a, &manager, &.{.{ .kind = .sum, .input_type = .integer }}, 1, 128 * 1024);
    defer group.deinit();
    const key = Datum.json(.{ .integer = 7 });
    try group.add(&.{key}, &.{Datum.json(.{ .integer = 1 })}, 3);
    try group.add(&.{key}, &.{Datum.json(.{ .float = 2.5 })}, 4);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    try std.testing.expectError(error.SqlTypeMismatch, group.next(arena.allocator()));
}

test "SQL full parallel partition reductions preserve exact states and close early" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    for ([_]bool{ false, true }) |exact| {
        var dummy: u8 = 0;
        var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
        defer manager.deinit();
        {
            var grouped = try Grouped.init(a, &manager, &.{ .{ .kind = .count }, .{ .kind = .sum, .input_type = .integer } }, 1, 512 * 1024);
            defer grouped.deinit();
            for (0..4096) |i| {
                const key = Datum.json(.{ .integer = @intCast(i % 128) });
                try grouped.add(&.{key}, &.{ Datum.json(.{ .integer = 1 }), Datum.json(.{ .integer = 3 }) }, i);
            }
            for (&grouped.partitions) |*file| if (file.*) |*open| try open.seal();
            const written_before = manager.written_bytes;
            var seen: [128]bool = @splat(false);
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            while (true) {
                _ = arena.reset(.retain_capacity);
                const row = (try (if (exact) grouped.nextPartial(arena.allocator()) else grouped.next(arena.allocator()))) orelse break;
                const index: usize = @intCast(row.keys[0].value.integer);
                try std.testing.expect(!seen[index]);
                seen[index] = true;
                try std.testing.expectEqual(@as(u64, index), row.ordinal);
                if (exact) {
                    var sum = try operators.Aggregate.init(a, .sum, .integer);
                    defer sum.deinit();
                    try Grouped.merge(&sum, row.aggregates[7..][0..7]);
                    try std.testing.expectEqual(@as(i64, 96), (try sum.finish()).value.integer);
                } else {
                    try std.testing.expectEqual(@as(i64, 32), row.aggregates[0].value.integer);
                    try std.testing.expectEqual(@as(i64, 96), row.aggregates[1].value.integer);
                }
            }
            for (seen) |present| try std.testing.expect(present);
            try std.testing.expect(grouped.reductions_started > 1);
            try std.testing.expectEqual(written_before, manager.written_bytes);
        }
        try std.testing.expectEqual(@as(usize, 0), manager.files);
        // Closing after one output cancels/drains the other admitted reducer.
        {
            var grouped = try Grouped.init(a, &manager, &.{.{ .kind = .count }}, 1, 512 * 1024);
            defer grouped.deinit();
            for (0..512) |i| try grouped.add(&.{Datum.json(.{ .integer = @intCast(i) })}, &.{Datum.json(.{ .integer = 1 })}, i);
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            try std.testing.expect((try grouped.next(arena.allocator())) != null);
        }
        try std.testing.expectEqual(@as(usize, 0), manager.files);
    }
}

test "SQL parallel aggregate keys remain owned after output blocks and workers close" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    var output = std.heap.ArenaAllocator.init(a);
    defer output.deinit();
    var names: std.ArrayList([]const u8) = .empty;
    defer names.deinit(a);
    {
        var grouped = try Grouped.init(a, &manager, &.{.{ .kind = .count }}, 1, 512 * 1024);
        defer grouped.deinit();
        for (0..512) |i| {
            const name = try std.fmt.allocPrint(a, "group-{d:0>4}", .{i});
            defer a.free(name);
            try grouped.add(&.{Datum.json(.{ .string = name })}, &.{Datum.json(.{ .integer = 1 })}, i);
        }
        while (try grouped.next(output.allocator())) |row| {
            try names.append(a, row.keys[0].value.string);
            try std.testing.expectEqual(@as(i64, 1), row.aggregates[0].value.integer);
        }
    }
    try std.testing.expectEqual(@as(usize, 512), names.items.len);
    var seen: [512]bool = @splat(false);
    for (names.items) |name| {
        try std.testing.expect(std.mem.startsWith(u8, name, "group-"));
        const index = try std.fmt.parseInt(usize, name[6..], 10);
        try std.testing.expect(!seen[index]);
        seen[index] = true;
    }
    for (seen) |present| try std.testing.expect(present);
}
